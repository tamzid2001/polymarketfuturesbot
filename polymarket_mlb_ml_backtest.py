"""Leakage-safe Polymarket US MLB moneyline research and dry-run ML pipeline.

This module deliberately separates three things that are often conflated:

* historical **directional** prediction from pre-game information;
* authenticated historical-price availability; and
* executable-price trading P&L.

It never replaces a missing pre-game observation with a post-game, closing, or
settlement price.  It also never treats a trade-price candle as an executable
ask.  Consequently a report can contain valid directional results while the
trading section correctly reports zero executable trades.

Data sources:
* Polymarket US public gateway: resolved MLB events, markets, and settlement.
* Polymarket US authenticated reporting API: historical trade statistics.
* MLB Stats API: schedule, final scores, and strictly prior team statistics.

The report endpoint is authenticated with the same Ed25519 API credentials
used by the existing runner.  The client records HTTP/authentication failures
as exclusion reasons instead of silently changing data sources.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import os
import statistics
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlencode
from urllib.request import Request, urlopen


LOG = logging.getLogger("polymarket_mlb_ml_backtest")
GATEWAY_BASE = "https://gateway.polymarket.us"
MLB_STATS_BASE = "https://statsapi.mlb.com/api/v1"
POLYMARKET_LEAGUE_TAG_ID = 4
HORIZONS_HOURS = (24, 6, 1)
EPSILON = 1e-6

MARKET_FEATURES = (
    "market_implied_home",
    "market_distance_from_half",
    "favorite_strength",
    "price_momentum_1h",
    "price_momentum_6h",
    "price_momentum_24h",
    "price_volatility_24h",
    "snapshot_age_hours",
    "candle_volume",
    "candle_notional",
)
TEAM_FEATURES = (
    "home_win_pct_10",
    "away_win_pct_10",
    "home_run_diff_10",
    "away_run_diff_10",
    "home_runs_for_10",
    "away_runs_for_10",
    "home_runs_against_10",
    "away_runs_against_10",
    "home_rest_days",
    "away_rest_days",
    "home_elo",
    "away_elo",
    "elo_difference",
)
ALL_FEATURES = MARKET_FEATURES + TEAM_FEATURES


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )


def now_utc() -> datetime:
    return datetime.now(UTC)


def iso(value: datetime | None = None) -> str:
    return (value or now_utc()).astimezone(UTC).isoformat().replace("+00:00", "Z")


def parse_time(value: Any) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def field(obj: Any, *names: str) -> Any:
    for name in names:
        if isinstance(obj, dict) and obj.get(name) is not None:
            return obj[name]
        value = getattr(obj, name, None)
        if value is not None:
            return value
    return None


def canonical_team(value: str | None) -> str:
    """Normalise documented MLB/Polymarket full team names for a strict join."""
    if not value:
        return ""
    aliases = {
        "athletics": "athletics",
        "oakland athletics": "athletics",
        "los angeles angels": "los angeles angels",
        "los angeles angels of anaheim": "los angeles angels",
        "arizona diamondbacks": "arizona diamondbacks",
    }
    cleaned = " ".join(str(value).lower().replace(".", "").split())
    return aliases.get(cleaned, cleaned)


def atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    temporary.replace(path)


def read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return default


def write_csv(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    materialised = list(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    keys: list[str] = []
    for row in materialised:
        for key in row:
            if key not in keys:
                keys.append(key)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys or ["empty"])
        writer.writeheader()
        for row in materialised:
            writer.writerow({key: _csv_value(row.get(key)) for key in keys})


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _csv_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (list, dict)):
        return json.dumps(value, sort_keys=True)
    return value


@dataclass(frozen=True)
class Paths:
    root: Path

    @property
    def raw(self) -> Path:
        return self.root / "data" / "raw" / "polymarket_mlb"

    @property
    def processed(self) -> Path:
        return self.root / "data" / "processed" / "polymarket_mlb"

    @property
    def features(self) -> Path:
        return self.root / "data" / "features" / "polymarket_mlb"

    @property
    def models(self) -> Path:
        return self.root / "models" / "trained" / "polymarket_mlb"

    @property
    def reports(self) -> Path:
        return self.root / "reports" / "polymarket_mlb"

    @property
    def logs(self) -> Path:
        return self.root / "logs"

    @property
    def events_manifest(self) -> Path:
        return self.raw / "events_manifest.json"

    @property
    def canonical_games(self) -> Path:
        return self.processed / "canonical_games.csv"

    @property
    def exclusions(self) -> Path:
        return self.reports / "excluded_markets.csv"

    @property
    def dataset_summary(self) -> Path:
        return self.reports / "dataset_summary.json"


class HttpJson:
    """Small retrying JSON client with raw-response caching owned by caller."""

    def __init__(self, timeout: float = 30.0, retries: int = 4) -> None:
        self.timeout = timeout
        self.retries = retries

    def get(self, url: str) -> Any:
        error: Exception | None = None
        for attempt in range(self.retries):
            try:
                request = Request(url, headers={
                    "Accept": "application/json",
                    "User-Agent": "polymarket-mlb-ml-backtest/1.0",
                })
                with urlopen(request, timeout=self.timeout) as response:  # noqa: S310 - fixed HTTPS sources
                    return json.loads(response.read().decode("utf-8"))
            except Exception as exc:  # noqa: BLE001
                error = exc
                if attempt + 1 < self.retries:
                    time.sleep(min(8.0, 0.5 * (2 ** attempt)))
        raise RuntimeError(f"GET failed after {self.retries} attempts: {url}: {error}")


def is_full_game_moneyline(market: dict[str, Any]) -> bool:
    legacy_type = str(market.get("sportsMarketType") or "")
    v2_type = str(market.get("sportsMarketTypeV2") or "")
    full_game_legacy = legacy_type in {"baseball_team_full_game_winner", "baseball_team_full_game_moneyline", "moneyline"}
    # When both schemas are present they must agree.  A first-five market can
    # legitimately have a broad V2 MONEYLINE type, but it is not full-game.
    valid_type = full_game_legacy if legacy_type else v2_type == "SPORTS_MARKET_TYPE_MONEYLINE"
    if v2_type and v2_type != "SPORTS_MARKET_TYPE_MONEYLINE":
        valid_type = False
    text = " ".join(str(market.get(key) or "").lower() for key in ("slug", "question", "title", "description"))
    derivative_markers = ("first 5", "first-five", "first five", "f5", "innings", "run line", "spread", "total", "over/under", "prop", "series", "futures")
    return valid_type and str(market.get("marketType") or "moneyline").lower() == "moneyline" and not any(marker in text for marker in derivative_markers)


def extract_sides(market: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]] | None:
    """Return (home, away), retaining long/short polarity for settlement labels."""
    sides = market.get("marketSides")
    if not isinstance(sides, list) or len(sides) != 2:
        return None
    parsed: dict[str, dict[str, Any]] = {}
    for side in sides:
        if not isinstance(side, dict):
            return None
        team = side.get("team") if isinstance(side.get("team"), dict) else {}
        role = str(side.get("ordering") or team.get("ordering") or "").lower()
        team_name = side.get("description") or team.get("name") or side.get("displayName")
        if role not in {"home", "away"} or not isinstance(team_name, str) or not team_name.strip():
            return None
        if role in parsed:
            return None
        parsed[role] = {
            "role": role,
            "team": team_name.strip(),
            "team_id": side.get("teamId") or team.get("id"),
            "long": bool(side.get("long")),
            "side_id": side.get("id"),
            "identifier": side.get("identifier"),
        }
    return (parsed["home"], parsed["away"]) if set(parsed) == {"home", "away"} else None


def market_outcome_from_settlement(home: dict[str, Any], settlement: float) -> int | None:
    """Polymarket settlement is the LONG payoff; return 1 when the home team won."""
    if settlement not in {0.0, 1.0}:
        return None
    long_won = settlement == 1.0
    return int(long_won == bool(home["long"]))


class PolymarketHistory:
    """Official public metadata plus optional authenticated reporting data."""

    def __init__(self, paths: Paths, *, key_id: str | None, secret_key: str | None) -> None:
        self.paths = paths
        self.http = HttpJson()
        self.key_id = key_id
        self.secret_key = secret_key
        self._auth_client: Any | None = None
        self._price_scales: dict[str, float] | None = None
        self._report_error: str | None = None

    def closed_events(self, *, refresh: bool = False, page_size: int = 1000) -> list[dict[str, Any]]:
        cached = read_json(self.paths.events_manifest, None)
        if isinstance(cached, dict) and isinstance(cached.get("events"), list) and not refresh:
            return [event for event in cached["events"] if isinstance(event, dict)]
        events: list[dict[str, Any]] = []
        offset = 0
        while True:
            query = urlencode({"limit": page_size, "offset": offset, "closed": "true", "tagIds": POLYMARKET_LEAGUE_TAG_ID})
            payload = self.http.get(f"{GATEWAY_BASE}/v1/events?{query}")
            page = payload.get("events") if isinstance(payload, dict) else None
            if not isinstance(page, list):
                raise RuntimeError("Polymarket events response did not contain an events list")
            raw_path = self.paths.raw / "events" / f"closed_mlb_offset_{offset:06d}.json"
            atomic_json(raw_path, {"retrieved_at": iso(), "request": query, "response": payload})
            events.extend(item for item in page if isinstance(item, dict))
            if len(page) < page_size:
                break
            offset += len(page)
        atomic_json(self.paths.events_manifest, {
            "retrieved_at": iso(), "endpoint": "/v1/events", "parameters": {"closed": True, "tagIds": 4},
            "events": events,
        })
        return events

    def settlement(self, market_slug: str) -> tuple[float | None, str | None]:
        path = self.paths.raw / "settlements" / f"{market_slug}.json"
        cached = read_json(path, None)
        if not isinstance(cached, dict):
            try:
                payload = self.http.get(f"{GATEWAY_BASE}/v1/markets/{market_slug}/settlement")
            except Exception as exc:  # noqa: BLE001
                return None, f"settlement_request_failed:{exc}"
            cached = {"retrieved_at": iso(), "response": payload}
            atomic_json(path, cached)
        response = cached.get("response") if isinstance(cached.get("response"), dict) else {}
        value = finite(response.get("settlement"))
        return value, None if value in {0.0, 1.0} else "ambiguous_or_missing_settlement"

    def _client(self) -> Any | None:
        if self._auth_client is not None:
            return self._auth_client
        if not self.key_id or not self.secret_key:
            self._report_error = "historical_reporting_credentials_unavailable"
            return None
        try:
            from polymarket_us import PolymarketUS
        except ImportError:
            self._report_error = "polymarket_us_sdk_not_installed"
            return None
        self._auth_client = PolymarketUS(key_id=self.key_id, secret_key=self.secret_key)
        return self._auth_client

    def _instrument_scales(self) -> dict[str, float]:
        """Read the documented priceScale from authenticated reference data.

        No raw report value above $1 is used until its instrument supplies a
        price scale.  This is deliberately stricter than guessing 100 or 1000.
        """
        if self._price_scales is not None:
            return self._price_scales
        client = self._client()
        if client is None:
            self._price_scales = {}
            return self._price_scales
        cache = self.paths.raw / "reference" / "mlb_instruments.json"
        cached = read_json(cache, None)
        if not isinstance(cached, dict):
            try:
                # The public documentation supports event_series filtering and
                # page tokens. Retain every response for schema auditing.
                pages: list[dict[str, Any]] = []
                token: str | None = None
                while True:
                    request: dict[str, Any] = {"event_series": "mlb"}
                    if token:
                        request["page_token"] = token
                    response = client.post("/v1/refdata/instruments", body=request, authenticated=True)
                    if not isinstance(response, dict):
                        raise RuntimeError("reference-data response was not an object")
                    pages.append(response)
                    token = response.get("nextPageToken") if isinstance(response.get("nextPageToken"), str) else None
                    if response.get("eof") is True or not token:
                        break
                cached = {"retrieved_at": iso(), "pages": pages}
                atomic_json(cache, cached)
            except Exception as exc:  # noqa: BLE001
                self._report_error = f"reference_data_unavailable:{exc}"
                self._price_scales = {}
                return self._price_scales
        scales: dict[str, float] = {}
        for page in cached.get("pages", []):
            for instrument in page.get("instruments", []) if isinstance(page, dict) else []:
                if not isinstance(instrument, dict):
                    continue
                symbol = instrument.get("symbol")
                scale = finite(instrument.get("priceScale") or instrument.get("price_scale"))
                if isinstance(symbol, str) and scale and scale > 0:
                    scales[symbol] = scale
        self._price_scales = scales
        return scales

    def candles(self, symbol: str, start: datetime, end: datetime) -> tuple[list[dict[str, Any]] | None, str | None]:
        """Fetch 1h trade statistics; no BBO/ask is claimed by this endpoint."""
        cache = self.paths.raw / "trade_stats" / f"{symbol}.json"
        cached = read_json(cache, None)
        if isinstance(cached, dict) and isinstance(cached.get("candles"), list):
            return cached["candles"], cached.get("error")
        client = self._client()
        if client is None:
            return None, self._report_error or "historical_reporting_credentials_unavailable"
        try:
            response = client.post("/v1beta1/report/trades/stats", body={
                "symbol": symbol, "start_time": iso(start), "end_time": iso(end), "interval": "1h",
            }, authenticated=True)
        except Exception as exc:  # noqa: BLE001
            reason = f"historical_report_request_failed:{exc}"
            atomic_json(cache, {"retrieved_at": iso(), "error": reason, "candles": []})
            return None, reason
        raw_stats = response.get("stats") if isinstance(response, dict) else None
        if not isinstance(raw_stats, list):
            reason = "historical_report_schema_missing_stats"
            atomic_json(cache, {"retrieved_at": iso(), "response": response, "error": reason, "candles": []})
            return None, reason
        scales = self._instrument_scales()
        scale = scales.get(symbol)
        candles: list[dict[str, Any]] = []
        for item in raw_stats:
            if not isinstance(item, dict):
                continue
            stamp = parse_time(item.get("interval_end") or item.get("intervalEnd"))
            raw_close = finite(item.get("close"))
            raw_open = finite(item.get("open"))
            raw_high = finite(item.get("high"))
            raw_low = finite(item.get("low"))
            # Values already expressed in dollars are safe. Integer-priced
            # report values require the documented instrument scale.
            divisor = 1.0 if raw_close is not None and 0 <= raw_close <= 1 else scale
            if not stamp or divisor is None or divisor <= 0:
                continue
            values = [raw_open, raw_high, raw_low, raw_close]
            if any(value is None for value in values):
                continue
            converted = [float(value) / divisor for value in values if value is not None]
            if any(value < 0 or value > 1 for value in converted):
                continue
            candles.append({
                "interval_start": item.get("interval_start") or item.get("intervalStart"),
                "interval_end": iso(stamp), "open": converted[0], "high": converted[1],
                "low": converted[2], "close": converted[3], "volume": finite(item.get("volume")),
                "notional": finite(item.get("notional")), "price_scale": divisor,
            })
        reason = None if candles else ("historical_prices_missing_or_price_scale_unavailable" if scale is None else "historical_prices_missing")
        atomic_json(cache, {"retrieved_at": iso(), "request": {"symbol": symbol, "start": iso(start), "end": iso(end)},
                            "response": response, "candles": candles, "error": reason})
        return candles or None, reason


class MlbStats:
    def __init__(self, paths: Paths) -> None:
        self.paths = paths
        self.http = HttpJson()

    def schedule(self, start: datetime, end: datetime, *, refresh: bool = False) -> list[dict[str, Any]]:
        cache = self.paths.raw / "mlb_stats" / f"schedule_{start.date()}_{end.date()}.json"
        payload = read_json(cache, None) if not refresh else None
        if not isinstance(payload, dict):
            query = urlencode({
                "sportId": 1, "startDate": start.date().isoformat(), "endDate": end.date().isoformat(),
                "hydrate": "linescore,team",
            })
            payload = self.http.get(f"{MLB_STATS_BASE}/schedule?{query}")
            atomic_json(cache, {"retrieved_at": iso(), "response": payload})
        response = payload.get("response") if isinstance(payload.get("response"), dict) else payload
        games: list[dict[str, Any]] = []
        for day in response.get("dates", []) if isinstance(response, dict) else []:
            for game in day.get("games", []) if isinstance(day, dict) else []:
                if isinstance(game, dict):
                    games.append(game)
        return games


def final_mlb_game(game: dict[str, Any]) -> dict[str, Any] | None:
    status = game.get("status") if isinstance(game.get("status"), dict) else {}
    if str(status.get("abstractGameState") or "").lower() != "final":
        return None
    start = parse_time(game.get("gameDate"))
    teams = game.get("teams") if isinstance(game.get("teams"), dict) else {}
    home = teams.get("home") if isinstance(teams.get("home"), dict) else {}
    away = teams.get("away") if isinstance(teams.get("away"), dict) else {}
    home_team = home.get("team") if isinstance(home.get("team"), dict) else {}
    away_team = away.get("team") if isinstance(away.get("team"), dict) else {}
    home_score, away_score = finite(home.get("score")), finite(away.get("score"))
    if not start or not home_team.get("name") or not away_team.get("name") or home_score is None or away_score is None or home_score == away_score:
        return None
    return {
        "game_pk": str(game.get("gamePk")), "scheduled_start": iso(start),
        "home_team": str(home_team["name"]), "away_team": str(away_team["name"]),
        "home_score": int(home_score), "away_score": int(away_score),
        "home_won": int(home_score > away_score),
        "status": str(status.get("detailedState") or status.get("abstractGameState")),
    }


def select_mlb_game(candidates: list[dict[str, Any]], home: str, away: str, start: datetime) -> tuple[dict[str, Any] | None, str | None]:
    matching = [
        game for game in candidates
        if canonical_team(game.get("home_team")) == canonical_team(home)
        and canonical_team(game.get("away_team")) == canonical_team(away)
        and (parse_time(game.get("scheduled_start")) and abs((parse_time(game["scheduled_start"]) - start).total_seconds()) <= 6 * 3600)
    ]
    if len(matching) == 1:
        return matching[0], None
    return None, "mlb_game_join_missing" if not matching else "mlb_game_join_ambiguous"


def nonfinal_mlb_reason(schedule_games: list[dict[str, Any]], home: str, away: str, start: datetime) -> str | None:
    """Classify a scheduled but non-final counterpart instead of hiding it as a join miss."""
    matches: list[dict[str, Any]] = []
    for game in schedule_games:
        teams = game.get("teams") if isinstance(game.get("teams"), dict) else {}
        home_data = teams.get("home") if isinstance(teams.get("home"), dict) else {}
        away_data = teams.get("away") if isinstance(teams.get("away"), dict) else {}
        home_team = home_data.get("team") if isinstance(home_data.get("team"), dict) else {}
        away_team = away_data.get("team") if isinstance(away_data.get("team"), dict) else {}
        game_start = parse_time(game.get("gameDate"))
        if (
            canonical_team(home_team.get("name")) == canonical_team(home)
            and canonical_team(away_team.get("name")) == canonical_team(away)
            and game_start is not None
            and abs((game_start - start).total_seconds()) <= 6 * 3600
        ):
            matches.append(game)
    if not matches:
        return None
    detail = " ".join(str((game.get("status") or {}).get(key) or "") for game in matches for key in ("abstractGameState", "detailedState", "codedGameState")).lower()
    if "cancel" in detail:
        return "mlb_game_canceled"
    if "postpon" in detail:
        return "mlb_game_postponed"
    if "suspend" in detail:
        return "mlb_game_suspended"
    return "mlb_game_unresolved_or_nonfinal"


def rolling_team_features(final_games: list[dict[str, Any]]) -> dict[str, dict[str, float | None]]:
    """Compute every feature before applying results from that start-time batch."""
    ordered = sorted(final_games, key=lambda row: parse_time(row.get("scheduled_start")) or datetime.min.replace(tzinfo=UTC))
    history: dict[str, list[dict[str, Any]]] = defaultdict(list)
    elo: dict[str, float] = defaultdict(lambda: 1500.0)
    output: dict[str, dict[str, float | None]] = {}
    index = 0
    while index < len(ordered):
        stamp = parse_time(ordered[index].get("scheduled_start"))
        batch: list[dict[str, Any]] = []
        while index < len(ordered) and parse_time(ordered[index].get("scheduled_start")) == stamp:
            batch.append(ordered[index])
            index += 1
        for game in batch:
            home, away = canonical_team(game["home_team"]), canonical_team(game["away_team"])
            home_prior, away_prior = history[home][-10:], history[away][-10:]
            def aggregate(prior: list[dict[str, Any]], key: str) -> float | None:
                return statistics.mean(item[key] for item in prior) if prior else None
            def rest(prior: list[dict[str, Any]]) -> float | None:
                if not prior or stamp is None:
                    return None
                previous = parse_time(prior[-1]["scheduled_start"])
                return (stamp - previous).total_seconds() / 86400 if previous else None
            output[game["game_pk"]] = {
                "home_win_pct_10": aggregate(home_prior, "won"), "away_win_pct_10": aggregate(away_prior, "won"),
                "home_run_diff_10": aggregate(home_prior, "run_diff"), "away_run_diff_10": aggregate(away_prior, "run_diff"),
                "home_runs_for_10": aggregate(home_prior, "runs_for"), "away_runs_for_10": aggregate(away_prior, "runs_for"),
                "home_runs_against_10": aggregate(home_prior, "runs_against"), "away_runs_against_10": aggregate(away_prior, "runs_against"),
                "home_rest_days": rest(home_prior), "away_rest_days": rest(away_prior),
                "home_elo": elo[home], "away_elo": elo[away], "elo_difference": elo[home] - elo[away],
            }
        for game in batch:
            home, away = canonical_team(game["home_team"]), canonical_team(game["away_team"])
            home_won = int(game["home_won"])
            expected_home = 1 / (1 + 10 ** ((elo[away] - elo[home]) / 400))
            change = 20 * (home_won - expected_home)
            elo[home] += change
            elo[away] -= change
            history[home].append({"scheduled_start": game["scheduled_start"], "won": home_won,
                                  "runs_for": game["home_score"], "runs_against": game["away_score"],
                                  "run_diff": game["home_score"] - game["away_score"]})
            history[away].append({"scheduled_start": game["scheduled_start"], "won": 1 - home_won,
                                  "runs_for": game["away_score"], "runs_against": game["home_score"],
                                  "run_diff": game["away_score"] - game["home_score"]})
    return output


def last_candle_at_or_before(candles: list[dict[str, Any]], cutoff: datetime) -> dict[str, Any] | None:
    valid = [candle for candle in candles if (stamp := parse_time(candle.get("interval_end"))) and stamp <= cutoff]
    return max(valid, key=lambda candle: parse_time(candle["interval_end"]) or datetime.min.replace(tzinfo=UTC), default=None)


def observed_price(candles: list[dict[str, Any]], cutoff: datetime) -> float | None:
    candle = last_candle_at_or_before(candles, cutoff)
    return finite(candle.get("close")) if candle else None


def snapshot_features(candles: list[dict[str, Any]], cutoff: datetime, home_is_long: bool) -> tuple[dict[str, Any] | None, str | None]:
    current = last_candle_at_or_before(candles, cutoff)
    if current is None:
        return None, "no_historical_observation_at_or_before_cutoff"
    long_price = finite(current.get("close"))
    stamp = parse_time(current.get("interval_end"))
    if long_price is None or not 0 < long_price < 1 or stamp is None:
        return None, "invalid_historical_price"
    home_price = long_price if home_is_long else 1.0 - long_price
    previous_1 = observed_price(candles, cutoff - timedelta(hours=1))
    previous_6 = observed_price(candles, cutoff - timedelta(hours=6))
    previous_24 = observed_price(candles, cutoff - timedelta(hours=24))
    if not home_is_long:
        previous_1 = None if previous_1 is None else 1.0 - previous_1
        previous_6 = None if previous_6 is None else 1.0 - previous_6
        previous_24 = None if previous_24 is None else 1.0 - previous_24
    recent = [
        (candle_time, finite(candle.get("close"))) for candle in candles
        if (candle_time := parse_time(candle.get("interval_end"))) and cutoff - timedelta(hours=24) <= candle_time <= cutoff
    ]
    series = [(1 - price) if not home_is_long and price is not None else price for _time, price in recent if price is not None]
    volatility = statistics.pstdev(series) if len(series) >= 2 else None
    return {
        "snapshot_timestamp": iso(stamp), "snapshot_age_hours": (cutoff - stamp).total_seconds() / 3600,
        "market_implied_home": home_price, "market_implied_away": 1 - home_price,
        "market_distance_from_half": abs(home_price - .5), "favorite_strength": abs(2 * home_price - 1),
        "price_momentum_1h": None if previous_1 is None else home_price - previous_1,
        "price_momentum_6h": None if previous_6 is None else home_price - previous_6,
        "price_momentum_24h": None if previous_24 is None else home_price - previous_24,
        "price_volatility_24h": volatility, "candle_volume": finite(current.get("volume")),
        "candle_notional": finite(current.get("notional")),
        # Trades/candles are not order-book asks. Keeping these explicit nulls
        # makes the simulator reject them rather than pretend they are fills.
        "home_executable_ask": None, "away_executable_ask": None,
        "historical_book_available": False,
    }, None


def collect_history(paths: Paths, *, refresh: bool = False, max_games: int | None = None) -> dict[str, Any]:
    """Download/cache authoritative event, settlement, schedule, and trade data."""
    key_id, secret = os.getenv("POLYMARKET_PUBLIC_KEY"), os.getenv("POLYMARKET_SECRET_KEY")
    poly = PolymarketHistory(paths, key_id=key_id, secret_key=secret)
    exclusions: list[dict[str, Any]] = []
    selected: list[dict[str, Any]] = []
    events = poly.closed_events(refresh=refresh)
    for event in events:
        markets = event.get("markets") if isinstance(event.get("markets"), list) else []
        for market in markets:
            if not isinstance(market, dict):
                continue
            context = {"event_id": event.get("id"), "event_slug": event.get("slug"), "market_id": market.get("id"), "market_slug": market.get("slug")}
            if not is_full_game_moneyline(market):
                exclusions.append({**context, "stage": "market_filter", "reason": "not_full_game_mlb_moneyline"})
                continue
            sides = extract_sides(market)
            start = parse_time(market.get("gameStartTime") or event.get("startTime"))
            if not event.get("closed") or not event.get("ended"):
                exclusions.append({**context, "stage": "market_filter", "reason": "event_not_finished"})
            elif not sides:
                exclusions.append({**context, "stage": "market_filter", "reason": "malformed_or_nonbinary_market_sides"})
            elif not start:
                exclusions.append({**context, "stage": "market_filter", "reason": "missing_scheduled_start"})
            elif not isinstance(market.get("slug"), str):
                exclusions.append({**context, "stage": "market_filter", "reason": "missing_market_slug"})
            else:
                home, away = sides
                selected.append({
                    **context, "market_created_at": market.get("createdAt"), "scheduled_start": iso(start),
                    "event_finished_at": event.get("finishedTimestamp"), "event_game_id": event.get("gameId"),
                    "home_team": home["team"], "away_team": away["team"], "home_is_long": home["long"],
                    "home_side_id": home["side_id"], "away_side_id": away["side_id"],
                    "volume": market.get("volume") or event.get("volume"), "liquidity": market.get("liquidity") or event.get("liquidity"),
                })
    selected.sort(key=lambda row: row["scheduled_start"])
    if max_games is not None:
        selected = selected[:max_games]
    if not selected:
        write_csv(paths.exclusions, exclusions)
        summary = {"generated_at": iso(), "events_discovered": len(events), "candidate_markets": 0, "eligible_games": 0,
                   "excluded": len(exclusions), "historical_reporting_error": poly._report_error}
        atomic_json(paths.dataset_summary, summary)
        return summary
    starts = [parse_time(row["scheduled_start"]) for row in selected]
    schedule = MlbStats(paths).schedule(min(item for item in starts if item) - timedelta(days=3), max(item for item in starts if item) + timedelta(days=3), refresh=refresh)
    final_games = [result for game in schedule if (result := final_mlb_game(game))]
    team_features = rolling_team_features(final_games)
    seen_game_keys: set[str] = set()
    canonical: list[dict[str, Any]] = []
    for candidate in selected:
        start = parse_time(candidate["scheduled_start"])
        assert start is not None
        game, join_error = select_mlb_game(final_games, candidate["home_team"], candidate["away_team"], start)
        if game is None:
            exclusions.append({**candidate, "stage": "join", "reason": nonfinal_mlb_reason(schedule, candidate["home_team"], candidate["away_team"], start) or join_error})
            continue
        duplicate_key = game["game_pk"]
        if duplicate_key in seen_game_keys:
            exclusions.append({**candidate, "stage": "deduplicate", "reason": "duplicate_or_relisted_market", "game_pk": duplicate_key})
            continue
        settlement, settlement_error = poly.settlement(str(candidate["market_slug"]))
        if settlement_error:
            exclusions.append({**candidate, "stage": "settlement", "reason": settlement_error, "game_pk": duplicate_key})
            continue
        market_home_won = market_outcome_from_settlement({"long": candidate["home_is_long"]}, float(settlement))
        if market_home_won is None or market_home_won != game["home_won"]:
            exclusions.append({**candidate, "stage": "settlement", "reason": "market_settlement_disagrees_with_official_final", "game_pk": duplicate_key})
            continue
        candles, candle_error = poly.candles(str(candidate["market_slug"]), parse_time(candidate.get("market_created_at")) or start - timedelta(days=7), start)
        if candles is None:
            exclusions.append({**candidate, "stage": "historical_prices", "reason": candle_error or "historical_prices_unavailable", "game_pk": duplicate_key})
            continue
        base = {
            **candidate, **game, **team_features.get(duplicate_key, {}), "market_settlement": settlement,
            "home_target": market_home_won, "market_candle_count": len(candles),
        }
        created = parse_time(candidate.get("market_created_at"))
        valid_horizons = 0
        for horizon in HORIZONS_HOURS:
            cutoff = start - timedelta(hours=horizon)
            if created and created > cutoff:
                exclusions.append({**candidate, "stage": "snapshot", "horizon_hours": horizon, "reason": "market_created_after_snapshot_cutoff", "game_pk": duplicate_key})
                continue
            snapshot, error = snapshot_features(candles, cutoff, bool(candidate["home_is_long"]))
            if snapshot is None:
                exclusions.append({**candidate, "stage": "snapshot", "horizon_hours": horizon, "reason": error, "game_pk": duplicate_key})
                continue
            canonical.append({**base, **snapshot, "horizon_hours": horizon, "feature_cutoff": iso(cutoff), "feature_quality_flags": "trade_candle_no_historical_bbo"})
            valid_horizons += 1
        if valid_horizons:
            seen_game_keys.add(duplicate_key)
    write_csv(paths.canonical_games, canonical)
    write_csv(paths.exclusions, exclusions)
    summary = {
        "generated_at": iso(), "events_discovered": len(events), "candidate_moneyline_markets": len(selected),
        "official_final_games_available": len(final_games), "eligible_game_horizon_rows": len(canonical),
        "eligible_unique_games": len(seen_game_keys), "excluded_records": len(exclusions),
        "historical_reporting_error": poly._report_error,
        "price_source": "authenticated Polymarket US /v1beta1/report/trades/stats; trade candles only, no historical BBO",
        "execution_note": "Historical trade candles are not executable asks; trading simulation will report no executable fills unless historical bid/ask data is supplied.",
    }
    atomic_json(paths.dataset_summary, summary)
    return summary


def numeric(row: dict[str, Any], key: str) -> float | None:
    return finite(row.get(key))


def to_feature_rows(paths: Paths) -> dict[int, list[dict[str, Any]]]:
    rows = read_csv(paths.canonical_games)
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        horizon = int(finite(row.get("horizon_hours")) or 0)
        start = parse_time(row.get("scheduled_start"))
        target = finite(row.get("home_target"))
        market = numeric(row, "market_implied_home")
        if horizon not in HORIZONS_HOURS or start is None or target not in {0.0, 1.0} or market is None:
            continue
        prepared: dict[str, Any] = dict(row)
        prepared["scheduled_start"] = iso(start)
        prepared["home_target"] = int(target)
        for feature in ALL_FEATURES:
            prepared[feature] = numeric(row, feature)
        grouped[horizon].append(prepared)
    for horizon in grouped:
        grouped[horizon].sort(key=lambda row: row["scheduled_start"])
        write_csv(paths.features / f"features_{horizon}h.csv", grouped[horizon])
    return grouped


def wilson_interval(successes: int, total: int) -> tuple[float | None, float | None]:
    if total <= 0:
        return None, None
    z = 1.959963984540054
    p = successes / total
    denominator = 1 + z * z / total
    centre = (p + z * z / (2 * total)) / denominator
    radius = z * math.sqrt((p * (1 - p) + z * z / (4 * total)) / total) / denominator
    return centre - radius, centre + radius


def ece(y_true: list[int], probabilities: list[float], buckets: int = 10) -> tuple[float | None, list[dict[str, Any]]]:
    if not y_true:
        return None, []
    rows: list[dict[str, Any]] = []
    total = len(y_true)
    value = 0.0
    for index in range(buckets):
        lower, upper = index / buckets, (index + 1) / buckets
        positions = [position for position, probability in enumerate(probabilities) if lower <= probability < upper or (index == buckets - 1 and probability == 1)]
        if not positions:
            continue
        observed = sum(y_true[position] for position in positions) / len(positions)
        predicted = sum(probabilities[position] for position in positions) / len(positions)
        value += len(positions) / total * abs(observed - predicted)
        rows.append({"bucket": f"{lower:.1f}-{upper:.1f}", "count": len(positions), "mean_predicted": predicted, "observed_rate": observed})
    return value, rows


def metrics(y_true: list[int], probabilities: list[float]) -> dict[str, Any]:
    if not y_true:
        return {"games": 0}
    try:
        from sklearn.metrics import accuracy_score, balanced_accuracy_score, brier_score_loss, f1_score, log_loss, precision_score, recall_score, roc_auc_score
    except ImportError as exc:  # pragma: no cover - exercised only without optional requirements
        raise RuntimeError("Install requirements_mlb_ml_backtest.txt before training") from exc
    clipped = [min(1 - EPSILON, max(EPSILON, value)) for value in probabilities]
    predicted = [int(value >= .5) for value in probabilities]
    tp = sum(actual == 1 and call == 1 for actual, call in zip(y_true, predicted, strict=True))
    tn = sum(actual == 0 and call == 0 for actual, call in zip(y_true, predicted, strict=True))
    fp = sum(actual == 0 and call == 1 for actual, call in zip(y_true, predicted, strict=True))
    fn = sum(actual == 1 and call == 0 for actual, call in zip(y_true, predicted, strict=True))
    score_ece, calibration = ece(y_true, probabilities)
    low, high = wilson_interval(sum(actual == call for actual, call in zip(y_true, predicted, strict=True)), len(y_true))
    return {
        "games": len(y_true), "accuracy": accuracy_score(y_true, predicted), "accuracy_ci_low": low, "accuracy_ci_high": high,
        "balanced_accuracy": balanced_accuracy_score(y_true, predicted), "precision": precision_score(y_true, predicted, zero_division=0),
        "recall": recall_score(y_true, predicted, zero_division=0), "f1": f1_score(y_true, predicted, zero_division=0),
        "roc_auc": roc_auc_score(y_true, probabilities) if len(set(y_true)) == 2 else None,
        "log_loss": log_loss(y_true, clipped, labels=[0, 1]), "brier_score": brier_score_loss(y_true, probabilities),
        "calibration_error": score_ece, "home_prediction_frequency": sum(predicted) / len(predicted),
        "confusion_matrix": {"tn": tn, "fp": fp, "fn": fn, "tp": tp}, "calibration": calibration,
    }


def feature_matrix(rows: list[dict[str, Any]], columns: tuple[str, ...]) -> list[list[float | None]]:
    return [[numeric(row, column) for column in columns] for row in rows]


def make_estimator(name: str) -> Any:
    try:
        from sklearn.ensemble import HistGradientBoostingClassifier
        from sklearn.impute import SimpleImputer
        from sklearn.linear_model import LogisticRegression
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import StandardScaler
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("Install requirements_mlb_ml_backtest.txt before training") from exc
    if name in {"price_only_logistic", "market_feature_logistic", "market_plus_team_logistic"}:
        return Pipeline([
            ("imputer", SimpleImputer(strategy="median")), ("scale", StandardScaler()),
            ("model", LogisticRegression(C=0.5, max_iter=2000, random_state=7)),
        ])
    if name == "gradient_boosting":
        return Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("model", HistGradientBoostingClassifier(max_iter=150, max_leaf_nodes=7, l2_regularization=1.0, random_state=7)),
        ])
    raise ValueError(f"Unknown model {name}")


def columns_for(name: str) -> tuple[str, ...]:
    if name == "price_only_logistic":
        return ("market_implied_home",)
    if name == "market_feature_logistic":
        return MARKET_FEATURES
    if name in {"market_plus_team_logistic", "gradient_boosting"}:
        return ALL_FEATURES
    raise ValueError(name)


def calibrated_predictions(name: str, train: list[dict[str, Any]], test: list[dict[str, Any]]) -> tuple[list[float], dict[str, Any]]:
    """Fit preprocessing/model/calibration only within the supplied train period."""
    try:
        from sklearn.isotonic import IsotonicRegression
        from sklearn.linear_model import LogisticRegression
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("Install requirements_mlb_ml_backtest.txt before training") from exc
    if len(train) < 60 or len(set(int(row["home_target"]) for row in train)) < 2:
        raise ValueError("insufficient_chronological_training_rows")
    columns = columns_for(name)
    calibration_count = max(25, int(len(train) * .2))
    calibration_count = min(calibration_count, max(0, len(train) - 40))
    base_train, calibration = train[:-calibration_count], train[-calibration_count:]
    if len(base_train) < 40 or len(set(int(row["home_target"]) for row in base_train)) < 2:
        raise ValueError("insufficient_precalibration_training_rows")
    estimator = make_estimator(name)
    estimator.fit(feature_matrix(base_train, columns), [int(row["home_target"]) for row in base_train])
    raw_calibration = list(estimator.predict_proba(feature_matrix(calibration, columns))[:, 1])
    y_calibration = [int(row["home_target"]) for row in calibration]
    calibration_method = "identity"
    calibrator: Any | None = None
    if len(calibration) >= 100 and len(set(y_calibration)) == 2:
        calibrator = IsotonicRegression(out_of_bounds="clip").fit(raw_calibration, y_calibration)
        calibration_method = "isotonic"
    elif len(calibration) >= 25 and len(set(y_calibration)) == 2:
        calibrator = LogisticRegression(C=1.0, max_iter=1000, random_state=7).fit([[value] for value in raw_calibration], y_calibration)
        calibration_method = "platt"
    raw_test = list(estimator.predict_proba(feature_matrix(test, columns))[:, 1])
    if calibration_method == "isotonic":
        probabilities = [float(value) for value in calibrator.predict(raw_test)]
    elif calibration_method == "platt":
        probabilities = list(calibrator.predict_proba([[value] for value in raw_test])[:, 1])
    else:
        probabilities = raw_test
    return probabilities, {"model": name, "features": list(columns), "calibration": calibration_method,
                           "base_train_rows": len(base_train), "calibration_rows": len(calibration)}


def train_model_artifact(paths: Paths, horizon: int, rows: list[dict[str, Any]], name: str) -> Path:
    """Save a model only after validation chose its specification.

    This artifact is trained after the evaluation report, on all currently
    eligible rows. It is labelled *deployment-only* and is never used to score
    the untouched final holdout that selected the specification.
    """
    try:
        import joblib
        from sklearn.isotonic import IsotonicRegression
        from sklearn.linear_model import LogisticRegression
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("Install requirements_mlb_ml_backtest.txt before training") from exc
    if name not in {"price_only_logistic", "market_feature_logistic", "gradient_boosting", "market_plus_team_logistic"}:
        raise ValueError("cannot create a learned-model artifact for a baseline")
    columns = columns_for(name)
    calibration_count = max(25, int(len(rows) * .2))
    calibration_count = min(calibration_count, max(0, len(rows) - 40))
    base_train, calibration = rows[:-calibration_count], rows[-calibration_count:]
    if len(base_train) < 40 or len(set(int(row["home_target"]) for row in base_train)) < 2:
        raise ValueError("insufficient rows for model artifact")
    estimator = make_estimator(name)
    estimator.fit(feature_matrix(base_train, columns), [int(row["home_target"]) for row in base_train])
    raw_calibration = list(estimator.predict_proba(feature_matrix(calibration, columns))[:, 1])
    y_calibration = [int(row["home_target"]) for row in calibration]
    calibration_method = "identity"
    calibrator: Any | None = None
    if len(calibration) >= 100 and len(set(y_calibration)) == 2:
        calibrator = IsotonicRegression(out_of_bounds="clip").fit(raw_calibration, y_calibration)
        calibration_method = "isotonic"
    elif len(calibration) >= 25 and len(set(y_calibration)) == 2:
        calibrator = LogisticRegression(C=1.0, max_iter=1000, random_state=7).fit([[value] for value in raw_calibration], y_calibration)
        calibration_method = "platt"
    artifact = {
        "schema": "polymarket_mlb_market_and_prior_team_features_v1", "created_at": iso(), "horizon_hours": horizon,
        "model": name, "features": list(columns), "estimator": estimator, "calibrator": calibrator,
        "calibration": calibration_method, "base_train_rows": len(base_train), "calibration_rows": len(calibration),
        "deployment_note": "Trained after evaluation on all eligible historical rows; not used to calculate the final holdout metrics.",
    }
    destination = paths.models / f"polymarket_mlb_{horizon}h_{name}.joblib"
    destination.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(artifact, destination)
    atomic_json(destination.with_suffix(".metadata.json"), {key: value for key, value in artifact.items() if key not in {"estimator", "calibrator"}})
    return destination


def baseline_probabilities(name: str, train: list[dict[str, Any]], test: list[dict[str, Any]]) -> list[float]:
    if name == "market_implied_probability":
        return [float(row["market_implied_home"]) for row in test]
    if name == "always_home":
        return [1 - EPSILON] * len(test)
    if name == "market_favorite":
        return [1 - EPSILON if float(row["market_implied_home"]) >= .5 else EPSILON for row in test]
    if name == "historical_home_rate":
        rate = sum(int(row["home_target"]) for row in train) / len(train)
        return [rate] * len(test)
    raise ValueError(name)


def chronological_splits(rows: list[dict[str, Any]]) -> tuple[list[tuple[str, list[dict[str, Any]], list[dict[str, Any]]]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Development walk-forward folds plus untouched 20% final tail."""
    ordered = sorted(rows, key=lambda row: row["scheduled_start"])
    holdout_size = max(30, math.ceil(len(ordered) * .2))
    if len(ordered) < 150 or len(ordered) - holdout_size < 80:
        return [], ordered, []
    development, holdout = ordered[:-holdout_size], ordered[-holdout_size:]
    initial = max(60, len(development) // 2)
    test_size = max(30, (len(development) - initial) // 3)
    folds: list[tuple[str, list[dict[str, Any]], list[dict[str, Any]]]] = []
    cursor = initial
    fold = 1
    while cursor < len(development):
        test = development[cursor:min(len(development), cursor + test_size)]
        if len(test) < 20:
            break
        train = development[:cursor]
        if train[-1]["scheduled_start"] >= test[0]["scheduled_start"]:
            raise AssertionError("chronological split ordering failure")
        folds.append((f"fold_{fold}", train, test))
        cursor += len(test)
        fold += 1
    return folds, development, holdout


def longest_streak(values: list[bool], outcome: bool) -> int:
    longest = current = 0
    for value in values:
        current = current + 1 if value == outcome else 0
        longest = max(longest, current)
    return longest


def simulate_trading(predictions: list[dict[str, Any]], *, threshold: float, fee_rate: float, slippage: float, capital_cap: float, quantity: float) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Uses historical asks only; a candle close can never be treated as an ask."""
    open_positions: list[tuple[datetime, float]] = []
    realised: list[float] = []
    trades: list[dict[str, Any]] = []
    attempted = unavailable = 0
    for row in sorted(predictions, key=lambda item: item["scheduled_start"]):
        probability = float(row["probability_home"])
        buy_home = probability >= .5
        model_probability = probability if buy_home else 1 - probability
        ask_key = "home_executable_ask" if buy_home else "away_executable_ask"
        ask = finite(row.get(ask_key))
        attempted += 1
        if ask is None:
            unavailable += 1
            continue
        entry = ask + slippage
        net_edge = model_probability - entry - fee_rate
        if entry <= 0 or entry >= 1 or net_edge <= threshold:
            continue
        start = parse_time(row["scheduled_start"])
        settlement = parse_time(row.get("event_finished_at")) or start
        assert start is not None
        open_positions = [(end, committed) for end, committed in open_positions if end > start]
        committed = entry * quantity
        if sum(amount for _end, amount in open_positions) + committed > capital_cap + 1e-9:
            continue
        actual_home = int(row["home_target"])
        won = actual_home == int(buy_home)
        pnl = (1.0 if won else 0.0) * quantity - entry * quantity - fee_rate * quantity
        realised.append(pnl)
        open_positions.append((settlement, committed))
        trades.append({**row, "side": "home" if buy_home else "away", "entry_ask": ask, "entry_with_slippage": entry,
                       "estimated_edge": model_probability - ask, "net_estimated_edge": net_edge, "won": won, "pnl": pnl,
                       "capital_committed": committed})
    gross_profit = sum(value for value in realised if value > 0)
    gross_loss = -sum(value for value in realised if value < 0)
    curve, peak, max_drawdown = 0.0, 0.0, 0.0
    for value in realised:
        curve += value
        peak = max(peak, curve)
        max_drawdown = max(max_drawdown, peak - curve)
    summary = {
        "threshold": threshold, "attempted_trades": attempted, "unavailable_historical_ask": unavailable,
        "executable_trades": len(trades), "net_pnl": sum(realised), "fees": fee_rate * quantity * len(trades),
        "slippage": slippage * quantity * len(trades), "return_on_capital": (sum(realised) / sum(float(t["capital_committed"]) for t in trades)) if trades else None,
        "average_return_per_trade": statistics.mean(realised) if realised else None,
        "win_rate": sum(bool(item["won"]) for item in trades) / len(trades) if trades else None,
        "average_entry_price": statistics.mean(float(item["entry_ask"]) for item in trades) if trades else None,
        "average_predicted_probability": statistics.mean(float(item["probability_home"]) for item in trades) if trades else None,
        "average_estimated_edge": statistics.mean(float(item["estimated_edge"]) for item in trades) if trades else None,
        "profit_factor": gross_profit / gross_loss if gross_loss else None, "maximum_drawdown": max_drawdown,
        "longest_winning_streak": longest_streak([bool(item["won"]) for item in trades], True),
        "longest_losing_streak": longest_streak([bool(item["won"]) for item in trades], False),
        "sharpe_ratio_per_trade": (statistics.mean(realised) / statistics.stdev(realised) * math.sqrt(len(realised))) if len(realised) >= 2 and statistics.stdev(realised) > 0 else None,
        "methodology_note": "Historical Polymarket trade candles contain no executable bid/ask. No trade is simulated without a recorded historical ask and available liquidity.",
    }
    return summary, trades


def evaluate_horizon(paths: Paths, horizon: int, rows: list[dict[str, Any]], *, fee_rate: float, slippage: float, capital_cap: float, quantity: float) -> dict[str, Any]:
    folds, development, holdout = chronological_splits(rows)
    names = ("always_home", "market_favorite", "market_implied_probability", "historical_home_rate", "price_only_logistic", "market_feature_logistic", "gradient_boosting", "market_plus_team_logistic")
    fold_metrics: list[dict[str, Any]] = []
    validation_scores: dict[str, list[float]] = defaultdict(list)
    for label, train, test in folds:
        for name in names:
            try:
                probability = baseline_probabilities(name, train, test) if name in {"always_home", "market_favorite", "market_implied_probability", "historical_home_rate"} else calibrated_predictions(name, train, test)[0]
                value = metrics([int(row["home_target"]) for row in test], probability)
                value.update({"horizon_hours": horizon, "fold": label, "model": name,
                              "train_start": train[0]["scheduled_start"], "train_end": train[-1]["scheduled_start"],
                              "test_start": test[0]["scheduled_start"], "test_end": test[-1]["scheduled_start"], "train_games": len(train)})
                fold_metrics.append(value)
                if value.get("log_loss") is not None and name not in {"always_home", "market_favorite"}:
                    validation_scores[name].append(float(value["log_loss"]))
            except (ValueError, RuntimeError) as exc:
                fold_metrics.append({"horizon_hours": horizon, "fold": label, "model": name, "error": str(exc), "train_games": len(train), "test_games": len(test)})
    write_csv(paths.reports / f"fold_metrics_{horizon}h.csv", fold_metrics)
    if not holdout:
        return {"horizon_hours": horizon, "rows": len(rows), "status": "insufficient_for_untouched_holdout", "fold_metrics": fold_metrics}
    eligible_models = {name: statistics.mean(scores) for name, scores in validation_scores.items() if scores}
    selected = min(eligible_models, key=eligible_models.get) if eligible_models else None
    learned_names = {"price_only_logistic", "market_feature_logistic", "gradient_boosting", "market_plus_team_logistic"}
    selected_learned = min((name for name in eligible_models if name in learned_names), key=eligible_models.get, default=None)
    holdout_predictions: list[dict[str, Any]] = []
    comparison: list[dict[str, Any]] = []
    for name in names:
        try:
            probability = baseline_probabilities(name, development, holdout) if name in {"always_home", "market_favorite", "market_implied_probability", "historical_home_rate"} else calibrated_predictions(name, development, holdout)[0]
            metric = metrics([int(row["home_target"]) for row in holdout], probability)
            metric.update({"horizon_hours": horizon, "model": name, "final_holdout_games": len(holdout), "selected_by_validation": name == selected})
            rows_for_model = [{**row, "model": name, "probability_home": prob, "predicted_home": int(prob >= .5), "actual_home": int(row["home_target"]), "validation_fold": "final_holdout"} for row, prob in zip(holdout, probability, strict=True)]
            thresholds = [0, .02, .05, .08, .10]
            trade_summaries: list[dict[str, Any]] = []
            for threshold in thresholds:
                result, trades = simulate_trading(rows_for_model, threshold=threshold, fee_rate=fee_rate, slippage=slippage, capital_cap=capital_cap, quantity=quantity)
                trade_summaries.append(result)
                if name == selected:
                    write_csv(paths.reports / f"trades_{horizon}h_threshold_{int(threshold * 100):02d}c.csv", trades)
            metric["trading_results"] = trade_summaries
            comparison.append(metric)
            holdout_predictions.extend(rows_for_model)
        except (ValueError, RuntimeError) as exc:
            comparison.append({"horizon_hours": horizon, "model": name, "error": str(exc), "final_holdout_games": len(holdout)})
    write_csv(paths.reports / f"holdout_predictions_{horizon}h.csv", holdout_predictions)
    write_csv(paths.reports / f"model_comparison_{horizon}h.csv", comparison)
    selected_row = next((row for row in comparison if row.get("model") == selected), None)
    if selected_row:
        atomic_json(paths.reports / f"calibration_{horizon}h.json", {"model": selected, "calibration": selected_row.get("calibration")})
    artifact_path: str | None = None
    if selected_learned:
        try:
            artifact_path = str(train_model_artifact(paths, horizon, rows, selected_learned))
        except (ValueError, RuntimeError) as exc:
            LOG.warning("MODEL ARTIFACT SKIPPED | horizon=%sh model=%s: %s", horizon, selected_learned, exc)
    return {"horizon_hours": horizon, "rows": len(rows), "development_games": len(development), "final_holdout_games": len(holdout),
            "selected_validation_model": selected, "selected_learned_model": selected_learned, "model_artifact": artifact_path,
            "validation_mean_log_loss": eligible_models, "comparison": comparison, "fold_metrics": fold_metrics}


def render_report(summary: dict[str, Any]) -> str:
    lines = ["# Polymarket MLB ML backtest", "", "## Data integrity", ""]
    dataset = summary.get("dataset", {})
    for key in ("events_discovered", "candidate_moneyline_markets", "official_final_games_available", "eligible_game_horizon_rows", "eligible_unique_games", "excluded_records", "historical_reporting_error"):
        lines.append(f"- {key.replace('_', ' ')}: {dataset.get(key)}")
    lines.extend(["", "## Out-of-sample results", ""])
    for horizon in summary.get("horizons", []):
        lines.append(f"### {horizon['horizon_hours']} hours before first pitch")
        if horizon.get("status"):
            lines.append(f"- {horizon['status']}")
            continue
        lines.append(f"- development / final holdout games: {horizon.get('development_games')} / {horizon.get('final_holdout_games')}")
        lines.append(f"- validation-selected model: {horizon.get('selected_validation_model')}")
        for result in horizon.get("comparison", []):
            if result.get("error"):
                lines.append(f"- {result.get('model')}: unavailable ({result['error']})")
            else:
                lines.append(f"- {result['model']}: accuracy={result.get('accuracy'):.4f}, log_loss={result.get('log_loss'):.4f}, brier={result.get('brier_score'):.4f}, roc_auc={result.get('roc_auc')}")
        lines.append("")
    lines.extend(["## Trading result", "", "Historical OHLC/trade-candle data is not an executable ask. The simulator therefore records no P&L or fill unless a historical ask and sufficient liquidity were captured. This is intentional, not a no-cost midpoint assumption.", ""])
    return "\n".join(lines)


def run_backtest(paths: Paths, *, fee_rate: float, slippage: float, capital_cap: float, quantity: float) -> dict[str, Any]:
    grouped = to_feature_rows(paths)
    outcomes = [evaluate_horizon(paths, horizon, grouped.get(horizon, []), fee_rate=fee_rate, slippage=slippage, capital_cap=capital_cap, quantity=quantity) for horizon in HORIZONS_HOURS]
    summary = {"generated_at": iso(), "dataset": read_json(paths.dataset_summary, {}), "horizons": outcomes,
               "methodology": {"split": "chronological expanding walk-forward plus untouched latest 20% holdout", "random_split": False,
                               "midpoint_execution": False, "fee_rate": fee_rate, "slippage": slippage, "position_size": quantity, "capital_cap": capital_cap}}
    atomic_json(paths.reports / "performance_summary.json", summary)
    (paths.reports / "backtest_report.md").parent.mkdir(parents=True, exist_ok=True)
    (paths.reports / "backtest_report.md").write_text(render_report(summary), encoding="utf-8")
    return summary


def print_terminal_summary(summary: dict[str, Any]) -> None:
    dataset = summary.get("dataset", {})
    print("POLYMARKET MLB ML BACKTEST")
    print(f"Historical games discovered: {dataset.get('candidate_moneyline_markets', 0)}")
    print(f"Eligible games: {dataset.get('eligible_unique_games', 0)}")
    print(f"Excluded records: {dataset.get('excluded_records', 0)}")
    for result in summary.get("horizons", []):
        print(f"\nHorizon: {result['horizon_hours']}h")
        if result.get("status"):
            print(f"Final holdout: unavailable ({result['status']})")
            continue
        print(f"Best validation model: {result.get('selected_validation_model')}")
        print(f"Final holdout games: {result.get('final_holdout_games')}")
        selected = next((row for row in result.get("comparison", []) if row.get("selected_by_validation")), None)
        market = next((row for row in result.get("comparison", []) if row.get("model") == "market_implied_probability"), None)
        if selected and not selected.get("error"):
            print(f"Holdout accuracy: {selected.get('accuracy'):.2%}")
            print(f"Holdout log loss / Brier: {selected.get('log_loss'):.4f} / {selected.get('brier_score'):.4f}")
        if market and not market.get("error"):
            print(f"Market-implied accuracy: {market.get('accuracy'):.2%}")
        print("Executable trades: 0 unless recorded historical asks and liquidity are present (no midpoint fills assumed).")
    print("Conclusion: no model is described as market-beating or profitable without a valid untouched holdout and historical executable prices.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("collect-history", "build-dataset", "train", "backtest", "report", "run-all", "dry-run", "live-monitor"))
    parser.add_argument("--root", type=Path, default=Path("."), help="Project root containing data/, reports/, and models/.")
    parser.add_argument("--refresh", action="store_true", help="Refresh cached public/API responses.")
    parser.add_argument("--max-games", type=int, help="Bound collection for a schema smoke test; omit for all discovered markets.")
    parser.add_argument("--fee-rate", type=float, default=0.0, help="Per-contract modeled fee. Defaults to 0 only because no historical fee schedule is supplied.")
    parser.add_argument("--slippage", type=float, default=0.0, help="Additional per-contract entry slippage, applied only to an actual historical ask.")
    parser.add_argument("--capital-cap", type=float, default=1000.0)
    parser.add_argument("--quantity", type=float, default=1.0)
    return parser


def main(args: argparse.Namespace) -> int:
    paths = Paths(args.root.resolve())
    if args.fee_rate < 0 or args.slippage < 0 or args.capital_cap <= 0 or args.quantity <= 0:
        raise SystemExit("fee-rate/slippage must be non-negative; capital-cap/quantity must be positive")
    if args.mode in {"collect-history", "run-all"}:
        summary = collect_history(paths, refresh=args.refresh, max_games=args.max_games)
        print(json.dumps(summary, indent=2, sort_keys=True))
    if args.mode in {"build-dataset", "train", "backtest", "report", "run-all"}:
        summary = run_backtest(paths, fee_rate=args.fee_rate, slippage=args.slippage, capital_cap=args.capital_cap, quantity=args.quantity)
        print_terminal_summary(summary)
    if args.mode in {"dry-run", "live-monitor"}:
        print("MLB live integration is intentionally dry-run-only until a separately reviewed model artifact and live-trading approval exist.")
    return 0


if __name__ == "__main__":
    configure_logging()
    raise SystemExit(main(build_parser().parse_args()))
