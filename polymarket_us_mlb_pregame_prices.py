#!/usr/bin/env python3
"""Download minute-level pregame prices for an upcoming Polymarket US MLB team.

The public sports feed supplies the schedule and the two full-game moneyline
sides.  The Polymarket US web application's public price-history RPC supplies
timestamped long/short prices.  This module requests one-minute fidelity,
selects the last observed price in each UTC minute, and carries the previous
close through minutes with no new observation.  It never creates a value
before the first exchange observation or after the game starts.

The CSV contract is deliberately small and stable::

    item_id,datetime,price

``item_id`` is the Polymarket US market-side ID for the selected MLB team.
Pregame prices are dollar probabilities in the inclusive range 0.01 through
0.99. Explicit ``full`` history retains genuine terminal 0 and 1 prices too.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import struct
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Iterable, Sequence

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


SPORTS_EVENTS_URL = "https://gateway.polymarket.us/v2/leagues/mlb/events"
CLOSED_EVENTS_URL = "https://gateway.polymarket.us/v1/events"
MARKET_BY_SLUG_URL = "https://gateway.polymarket.us/v1/market/slug"
PRICE_HISTORY_URL = (
    "https://gateway.polymarket.us/"
    "gateway.price_history.v1.PriceHistoryService/GetPriceHistory"
)
USER_AGENT = "polymarketfuturesbot-mlb-pregame-history/1.0"
PRICE_QUANTUM = Decimal("0.0001")
MIN_PRICE = Decimal("0.01")
MAX_PRICE = Decimal("0.99")


class DownloadError(RuntimeError):
    """Raised when genuine Polymarket data cannot satisfy the request."""


@dataclass(frozen=True)
class TeamSide:
    item_id: str
    team: str
    position: str


@dataclass(frozen=True)
class MoneylineMarket:
    event_ticker: str
    event_title: str
    market_id: str
    market_slug: str
    created_at: datetime
    game_start: datetime
    sides: tuple[TeamSide, TeamSide]


@dataclass(frozen=True)
class PricePoint:
    timestamp: int
    long_price: Decimal
    short_price: Decimal


@dataclass(frozen=True)
class MinuteRow:
    item_id: str
    datetime: str
    price: str


def parse_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def utc_iso(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def build_session() -> requests.Session:
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        status=4,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "POST"}),
        # Bound command latency. A read-only retry still backs off, but an
        # unexpectedly large Retry-After cannot suspend a one-shot export for
        # minutes without returning control to the caller.
        respect_retry_after_header=False,
    )
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def discover_moneylines(
    payload: dict[str, Any],
    as_of: datetime,
    *,
    game_scope: str = "upcoming",
) -> list[MoneylineMarket]:
    """Return valid two-team MLB full-game moneylines for one time scope."""
    if game_scope not in {"upcoming", "previous", "any"}:
        raise ValueError("game_scope must be upcoming, previous, or any")
    markets: list[MoneylineMarket] = []
    events = payload.get("events")
    if not isinstance(events, list):
        raise DownloadError("Polymarket sports response has no events list")
    for event in events:
        if not isinstance(event, dict):
            continue
        event_markets = event.get("markets")
        if not isinstance(event_markets, list):
            continue
        for market in event_markets:
            if not isinstance(market, dict):
                continue
            if market.get("sportsMarketType") not in {
                "baseball_team_full_game_winner",
                "baseball_team_full_game_moneyline",
                "moneyline",
            }:
                continue
            game_start = parse_timestamp(market.get("gameStartTime") or event.get("startTime"))
            created_at = parse_timestamp(market.get("createdAt") or event.get("createdAt"))
            if game_start is None or created_at is None:
                continue
            closed = market.get("closed") is True or str(market.get("status") or "") in {
                "MARKET_STATUS_CLOSED",
                "MARKET_STATUS_RESOLVED",
            }
            if game_scope == "upcoming" and (game_start <= as_of or closed or market.get("active") is False):
                continue
            if game_scope == "previous" and (game_start >= as_of or not closed):
                continue
            raw_sides = market.get("marketSides")
            if not isinstance(raw_sides, list) or len(raw_sides) != 2:
                continue
            sides: list[TeamSide] = []
            for raw_side in raw_sides:
                if not isinstance(raw_side, dict):
                    break
                team_payload = raw_side.get("team")
                team = team_payload.get("name") if isinstance(team_payload, dict) else None
                item_id = raw_side.get("id")
                if not isinstance(team, str) or not team.strip() or item_id is None:
                    break
                sides.append(TeamSide(
                    item_id=str(item_id),
                    team=team.strip(),
                    position="long" if raw_side.get("long") is True else "short",
                ))
            if len(sides) != 2 or {side.position for side in sides} != {"long", "short"}:
                continue
            market_slug = market.get("slug")
            market_id = market.get("id")
            if not isinstance(market_slug, str) or market_id is None:
                continue
            markets.append(MoneylineMarket(
                event_ticker=str(event.get("ticker") or event.get("slug") or ""),
                event_title=str(event.get("title") or ""),
                market_id=str(market_id),
                market_slug=market_slug,
                created_at=created_at,
                game_start=game_start,
                sides=(sides[0], sides[1]),
            ))
    if game_scope == "any":
        return sorted(
            markets,
            key=lambda item: (
                0 if item.game_start >= as_of else 1,
                abs((item.game_start - as_of).total_seconds()),
                item.market_slug,
            ),
        )
    return sorted(markets, key=lambda item: (item.game_start, item.market_slug), reverse=game_scope == "previous")


def discover_upcoming_moneylines(payload: dict[str, Any], as_of: datetime) -> list[MoneylineMarket]:
    """Compatibility wrapper for callers that only need upcoming games."""
    return discover_moneylines(payload, as_of, game_scope="upcoming")


def fetch_event_payload(
    session: requests.Session,
    *,
    game_scope: str,
    timeout: float,
    requested_market: str | None = None,
) -> dict[str, Any]:
    """Load upcoming and/or all paginated closed MLB events."""
    if requested_market:
        response = session.get(f"{MARKET_BY_SLUG_URL}/{requested_market}", timeout=timeout)
        response.raise_for_status()
        payload = response.json()
        market = payload.get("market") if isinstance(payload, dict) else None
        if not isinstance(market, dict):
            raise DownloadError(f"market lookup returned no market for {requested_market}")
        return {"events": [{
            "ticker": requested_market.removeprefix("aec-"),
            "title": str(market.get("title") or market.get("question") or requested_market),
            "startTime": market.get("gameStartTime"),
            "createdAt": market.get("createdAt"),
            "markets": [market],
        }]}
    events: list[dict[str, Any]] = []
    if game_scope in {"upcoming", "any"}:
        response = session.get(
            SPORTS_EVENTS_URL,
            params={"limit": 1000, "active": "true", "closed": "false"},
            timeout=timeout,
        )
        response.raise_for_status()
        payload = response.json()
        upcoming = payload.get("events") if isinstance(payload, dict) else None
        if not isinstance(upcoming, list):
            raise DownloadError("Polymarket upcoming sports response has no events list")
        events.extend(event for event in upcoming if isinstance(event, dict))
    if game_scope in {"previous", "any"}:
        page_size = 250
        offset = 0
        while True:
            response = session.get(
                CLOSED_EVENTS_URL,
                params={"limit": page_size, "offset": offset, "closed": "true", "tagIds": 4},
                timeout=timeout,
            )
            response.raise_for_status()
            payload = response.json()
            page = payload.get("events") if isinstance(payload, dict) else None
            if not isinstance(page, list):
                raise DownloadError("Polymarket closed-events response has no events list")
            events.extend(event for event in page if isinstance(event, dict))
            if len(page) < page_size:
                break
            offset += len(page)
            if offset > 10_000:
                raise DownloadError("closed-event pagination exceeded the safety limit")
    return {"events": events}


def _encode_varint(value: int) -> bytes:
    if value < 0:
        raise ValueError("varints must be non-negative")
    encoded = bytearray()
    while value > 0x7F:
        encoded.append((value & 0x7F) | 0x80)
        value >>= 7
    encoded.append(value)
    return bytes(encoded)


def _string_field(number: int, value: str) -> bytes:
    raw = value.encode("utf-8")
    return _encode_varint((number << 3) | 2) + _encode_varint(len(raw)) + raw


def _integer_field(number: int, value: int) -> bytes:
    return _encode_varint(number << 3) + _encode_varint(value)


def _message_field(number: int, value: bytes) -> bytes:
    return _encode_varint((number << 3) | 2) + _encode_varint(len(value)) + value


def encode_price_history_request(
    symbol: str,
    start: datetime,
    end: datetime,
    fidelity_minutes: int = 1,
) -> bytes:
    """Encode the public gateway request without depending on generated protos.

    The official web RPC schema uses field 1 for the market symbol, field 2 for
    a nested [start, end] Unix-second interval, and field 4 for fidelity in
    minutes.  Encoding the small message locally keeps the downloader portable.
    """
    if not symbol:
        raise ValueError("symbol is required")
    if start.tzinfo is None or end.tzinfo is None or end <= start:
        raise ValueError("start/end must be timezone-aware and increasing")
    if fidelity_minutes < 1:
        raise ValueError("fidelity_minutes must be positive")
    interval = _integer_field(1, int(start.timestamp())) + _integer_field(2, int(end.timestamp()))
    return (
        _string_field(1, symbol)
        + _message_field(2, interval)
        + _integer_field(4, fidelity_minutes)
    )


def _read_varint(payload: bytes, offset: int) -> tuple[int, int]:
    result = 0
    shift = 0
    while offset < len(payload):
        value = payload[offset]
        offset += 1
        result |= (value & 0x7F) << shift
        if value < 0x80:
            return result, offset
        shift += 7
        if shift >= 70:
            break
    raise DownloadError("malformed protobuf varint in price-history response")


def _wire_fields(payload: bytes) -> Iterable[tuple[int, int, Any]]:
    offset = 0
    while offset < len(payload):
        tag, offset = _read_varint(payload, offset)
        field_number, wire_type = tag >> 3, tag & 7
        if field_number == 0:
            raise DownloadError("invalid protobuf field zero in price-history response")
        if wire_type == 0:
            value, offset = _read_varint(payload, offset)
        elif wire_type == 1:
            if offset + 8 > len(payload):
                raise DownloadError("truncated fixed64 price-history field")
            value = payload[offset : offset + 8]
            offset += 8
        elif wire_type == 2:
            length, offset = _read_varint(payload, offset)
            if offset + length > len(payload):
                raise DownloadError("truncated length-delimited price-history field")
            value = payload[offset : offset + length]
            offset += length
        elif wire_type == 5:
            if offset + 4 > len(payload):
                raise DownloadError("truncated fixed32 price-history field")
            value = payload[offset : offset + 4]
            offset += 4
        else:
            raise DownloadError(f"unsupported protobuf wire type {wire_type}")
        yield field_number, wire_type, value


def _float_decimal(raw: bytes) -> Decimal:
    value = struct.unpack("<f", raw)[0]
    return Decimal(str(value)).quantize(PRICE_QUANTUM, rounding=ROUND_HALF_UP)


def decode_price_history_response(payload: bytes) -> list[PricePoint]:
    """Decode repeated timestamp/long/short price-history points."""
    points: list[PricePoint] = []
    for field_number, wire_type, raw_point in _wire_fields(payload):
        if field_number != 1 or wire_type != 2:
            continue
        values: dict[int, tuple[int, Any]] = {}
        for point_field, point_wire, value in _wire_fields(raw_point):
            values[point_field] = (point_wire, value)
        if not all(number in values for number in (1, 2, 3)):
            continue
        timestamp_wire, timestamp = values[1]
        long_wire, long_raw = values[2]
        short_wire, short_raw = values[3]
        if timestamp_wire != 0 or long_wire != 5 or short_wire != 5:
            continue
        long_price = _float_decimal(long_raw)
        short_price = _float_decimal(short_raw)
        if not (Decimal("0") <= long_price <= Decimal("1")):
            continue
        if not (Decimal("0") <= short_price <= Decimal("1")):
            continue
        points.append(PricePoint(int(timestamp), long_price, short_price))
    points.sort(key=lambda point: point.timestamp)
    return points


def fetch_price_history(
    session: requests.Session,
    market: MoneylineMarket,
    end: datetime,
    *,
    fidelity_minutes: int = 1,
    timeout: float = 30.0,
) -> list[PricePoint]:
    request_body = encode_price_history_request(
        market.market_slug,
        market.created_at,
        end,
        fidelity_minutes,
    )
    response = session.post(
        PRICE_HISTORY_URL,
        headers={"Content-Type": "application/proto", "Accept": "application/proto"},
        data=request_body,
        timeout=timeout,
    )
    if response.status_code != 200:
        detail = response.text[:300] if "json" in response.headers.get("Content-Type", "") else ""
        raise DownloadError(f"price history HTTP {response.status_code}: {detail}")
    points = decode_price_history_response(response.content)
    if not points:
        raise DownloadError(f"no price history returned for {market.market_slug}")
    return points


def select_team(market: MoneylineMarket, requested_team: str | None = None) -> TeamSide:
    if requested_team:
        needle = requested_team.casefold().strip()
        matches = [side for side in market.sides if needle in side.team.casefold()]
        if len(matches) != 1:
            names = ", ".join(side.team for side in market.sides)
            raise DownloadError(f"team {requested_team!r} did not uniquely match: {names}")
        return matches[0]
    return next(side for side in market.sides if side.position == "long")


def build_minute_rows(
    points: Sequence[PricePoint],
    side: TeamSide,
    market: MoneylineMarket,
    end: datetime,
    *,
    pregame_only: bool = True,
) -> tuple[list[MinuteRow], int]:
    """Build complete UTC-minute closes and return rows plus observed minutes.

    A missing minute is forward-filled from the last genuine observation, as
    recommended by Polymarket's candlestick data guide.  The final incomplete
    UTC minute is excluded.
    """
    hard_end = min(end, market.game_start) if pregame_only else end
    last_complete_minute = int(hard_end.timestamp()) // 60 * 60 - 60
    if not pregame_only and points:
        # A completed contract may have no observations after settlement. Do
        # not extend its terminal price through every minute up to today's run.
        last_observed_minute = max(point.timestamp for point in points) // 60 * 60
        last_complete_minute = min(last_complete_minute, last_observed_minute)
    closes: dict[int, Decimal] = {}
    for point in sorted(points, key=lambda value: value.timestamp):
        if point.timestamp < int(market.created_at.timestamp()):
            continue
        if pregame_only and point.timestamp >= int(market.game_start.timestamp()):
            continue
        minute = point.timestamp // 60 * 60
        if minute > last_complete_minute:
            continue
        price = point.long_price if side.position == "long" else point.short_price
        lower, upper = (MIN_PRICE, MAX_PRICE) if pregame_only else (Decimal("0"), Decimal("1"))
        if lower <= price <= upper:
            closes[minute] = price
    if not closes:
        return [], 0
    observed_minutes = len(closes)
    first_minute = min(closes)
    rows: list[MinuteRow] = []
    previous: Decimal | None = None
    for minute in range(first_minute, last_complete_minute + 1, 60):
        if minute in closes:
            previous = closes[minute]
        if previous is None:
            continue
        rows.append(MinuteRow(
            item_id=side.item_id,
            datetime=datetime.fromtimestamp(minute, UTC).isoformat().replace("+00:00", "Z"),
            price=format(previous, ".4f"),
        ))
    return rows, observed_minutes


def _atomic_write_csv(path: Path, rows: Sequence[MinuteRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=("item_id", "datetime", "price"),
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(asdict(row) for row in rows)
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(path)


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(path)


def download(
    *,
    output: Path | None = None,
    metadata_output: Path | None = None,
    min_rows: int = 500,
    requested_market: str | None = None,
    requested_team: str | None = None,
    game_scope: str = "upcoming",
    history_window: str = "pregame",
    as_of: datetime | None = None,
    timeout: float = 30.0,
) -> dict[str, Any]:
    as_of = (as_of or datetime.now(UTC)).astimezone(UTC)
    if min_rows < 1:
        raise ValueError("min_rows must be positive")
    if game_scope not in {"upcoming", "previous", "any"}:
        raise ValueError("game_scope must be upcoming, previous, or any")
    if history_window not in {"pregame", "full"}:
        raise ValueError("history_window must be pregame or full")
    session = build_session()
    try:
        payload = fetch_event_payload(
            session,
            game_scope=game_scope,
            timeout=timeout,
            requested_market=requested_market,
        )
        markets = discover_moneylines(payload, as_of, game_scope=game_scope)
        if requested_market:
            markets = [market for market in markets if market.market_slug == requested_market]
        if not markets:
            raise DownloadError("no matching upcoming open MLB full-game moneyline found")

        failures: list[str] = []
        selected: tuple[MoneylineMarket, TeamSide, list[PricePoint], list[MinuteRow], int] | None = None
        for market in markets:
            try:
                side = select_team(market, requested_team)
                history_end = min(as_of, market.game_start) if history_window == "pregame" else as_of
                points = fetch_price_history(session, market, history_end, timeout=timeout)
                rows, observed_minutes = build_minute_rows(
                    points,
                    side,
                    market,
                    as_of,
                    pregame_only=history_window == "pregame",
                )
            except (DownloadError, requests.RequestException) as exc:
                failures.append(f"{market.market_slug}: {exc}")
                continue
            if len(rows) >= min_rows:
                selected = market, side, points, rows, observed_minutes
                break
            failures.append(f"{market.market_slug}: only {len(rows)} complete minute rows")
        if selected is None:
            detail = "; ".join(failures[:8])
            raise DownloadError(f"no upcoming MLB market supplied at least {min_rows} minute rows ({detail})")

        market, side, points, rows, observed_minutes = selected
        if output is None:
            output = (
                Path("data/processed/polymarket_us_mlb")
                / f"{market.market_slug}_{side.item_id}_{history_window}_1m.csv"
            )
        if metadata_output is None:
            metadata_output = output.with_suffix(".metadata.json")
        _atomic_write_csv(output, rows)
        game_start_iso = utc_iso(market.game_start)
        before_start_rows = sum(row.datetime < game_start_iso for row in rows)
        metadata = {
            "schema": "polymarket_us_mlb_pregame_minute_prices_v1",
            "retrieved_at": utc_iso(as_of),
            "source": {
                "sports_events_url": SPORTS_EVENTS_URL,
                "closed_events_url": CLOSED_EVENTS_URL,
                "price_history_url": PRICE_HISTORY_URL,
                "fidelity_minutes": 1,
                "price_definition": f"{side.position}_price",
                "minute_aggregation": "last exchange observation in UTC minute; previous close carried through gaps",
                "price_bounds": "0.01..0.99" if history_window == "pregame" else "0..1 including terminal prices",
            },
            "game_scope": game_scope,
            "history_window": history_window,
            "event_ticker": market.event_ticker,
            "event_title": market.event_title,
            "market_id": market.market_id,
            "market_slug": market.market_slug,
            "market_created_at": utc_iso(market.created_at),
            "game_start": utc_iso(market.game_start),
            "team": side.team,
            "position": side.position,
            "item_id": side.item_id,
            "raw_price_points": len(points),
            "observed_minutes": observed_minutes,
            "forward_filled_minutes": len(rows) - observed_minutes,
            "output_rows": len(rows),
            "first_datetime": rows[0].datetime,
            "last_datetime": rows[-1].datetime,
            "minimum_requested_rows": min_rows,
            "strictly_pregame": history_window == "pregame",
            "pregame_rows": before_start_rows,
            "post_start_rows": len(rows) - before_start_rows,
            "output_csv": str(output),
        }
        _atomic_write_json(metadata_output, metadata)
        return metadata
    finally:
        session.close()


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        help="CSV destination; default is a unique market/team/window filename",
    )
    parser.add_argument(
        "--metadata-output",
        type=Path,
        help="JSON provenance destination; default is beside the CSV",
    )
    parser.add_argument("--min-rows", type=int, default=500)
    parser.add_argument("--market-slug", help="Require one exact moneyline market slug within the selected scope")
    parser.add_argument("--team", help="Case-insensitive unique team-name substring")
    parser.add_argument(
        "--game-scope",
        choices=("upcoming", "previous", "any"),
        default="upcoming",
        help="Choose the earliest upcoming game, latest completed game, or search both",
    )
    parser.add_argument(
        "--history-window",
        choices=("pregame", "full"),
        default="pregame",
        help="Pregame stops before first pitch; full includes in-game/terminal observations",
    )
    parser.add_argument("--timeout", type=float, default=30.0)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        metadata = download(
            output=args.output,
            metadata_output=args.metadata_output,
            min_rows=args.min_rows,
            requested_market=args.market_slug,
            requested_team=args.team,
            game_scope=args.game_scope,
            history_window=args.history_window,
            timeout=args.timeout,
        )
    except (DownloadError, requests.RequestException, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    label = "PREMARKET" if metadata["history_window"] == "pregame" else "FULL MARKET"
    print(
        f"POLYMARKET US MLB {label} HISTORY | "
        f"game={metadata['event_title']} team={metadata['team']} "
        f"rows={metadata['output_rows']} observed={metadata['observed_minutes']} "
        f"forward_filled={metadata['forward_filled_minutes']} "
        f"range={metadata['first_datetime']}..{metadata['last_datetime']} "
        f"csv={metadata['output_csv']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
