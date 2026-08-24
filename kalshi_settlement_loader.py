"""Load and cache settled KXBTC15M markets from Kalshi's public API.

The public historical response contains the final settlement, but not an
intramarket book or trade path.  This module deliberately persists only that
observable settlement layer.  Execution is modeled separately by
``execution_path_model``.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from strategy_core import sticky_directional_prediction


LOG = logging.getLogger(__name__)
SERIES_TICKER = "KXBTC15M"
PUBLIC_ENDPOINTS = (
    ("https://external-api.kalshi.com/trade-api/v2/markets", {"status": "settled"}),
    # The current endpoint has historically retained the complete series, but
    # use the archive endpoint too because availability/retention is an API
    # concern rather than a modeling assumption.
    ("https://external-api.kalshi.com/trade-api/v2/historical/markets", {}),
)
DEFAULT_CACHE = Path("data/raw/kalshi_kxbtc15m_settlements.json")
DEFAULT_SIGNALS = Path("historical_signals.parquet")


def parse_timestamp(value: Any) -> datetime | None:
    """Parse Kalshi ISO timestamps without substituting a clock time."""

    if value in {None, ""}:
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=UTC)
    text = str(value).strip()
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def timestamp_text(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class SettlementMarket:
    ticker: str
    open_time: datetime
    close_time: datetime
    settlement_time: datetime
    result: str
    source: str

    @classmethod
    def from_api(cls, record: dict[str, Any], source: str) -> "SettlementMarket | None":
        ticker = str(record.get("ticker") or "")
        result = str(record.get("result") or record.get("market_result") or "").lower()
        open_time = parse_timestamp(record.get("open_time"))
        close_time = parse_timestamp(record.get("close_time") or record.get("expected_expiration_time"))
        settlement_time = parse_timestamp(record.get("settlement_ts") or record.get("settlement_time"))
        if not (
            ticker.startswith(SERIES_TICKER + "-")
            and result in {"yes", "no"}
            and open_time is not None
            and close_time is not None
            and settlement_time is not None
        ):
            return None
        return cls(ticker, open_time, close_time, settlement_time, result, source)

    def to_cache(self) -> dict[str, str]:
        payload = asdict(self)
        for name in ("open_time", "close_time", "settlement_time"):
            payload[name] = timestamp_text(getattr(self, name))
        return payload

    @classmethod
    def from_cache(cls, value: dict[str, Any]) -> "SettlementMarket":
        return cls(
            ticker=str(value["ticker"]),
            open_time=parse_timestamp(value["open_time"]),  # type: ignore[arg-type]
            close_time=parse_timestamp(value["close_time"]),  # type: ignore[arg-type]
            settlement_time=parse_timestamp(value["settlement_time"]),  # type: ignore[arg-type]
            result=str(value["result"]).lower(),
            source=str(value.get("source") or "cache"),
        )


@dataclass(frozen=True)
class HistoricalSignal:
    market_index: int
    ticker: str
    open_time: datetime
    close_time: datetime
    settlement_time: datetime
    actual_result: str
    source_ticker: str
    source_close_time: datetime
    source_settlement_time: datetime
    source_result: str
    decision_time: datetime
    predicted_side: str
    directional_win: bool
    signal_mode: str = "inverse_latest_settlement"
    source_outcome_mode: str = "official_settlement"

    def to_row(self) -> dict[str, Any]:
        payload = asdict(self)
        for name in (
            "open_time", "close_time", "settlement_time", "source_close_time",
            "source_settlement_time", "decision_time",
        ):
            payload[name] = timestamp_text(getattr(self, name))
        return payload


class KalshiSettlementLoader:
    """A paginated public-settlement client with a durable local cache."""

    def __init__(self, cache_path: Path = DEFAULT_CACHE, timeout_seconds: int = 30) -> None:
        self.cache_path = cache_path
        self.timeout_seconds = timeout_seconds

    def _get_json(self, url: str, params: dict[str, str]) -> dict[str, Any]:
        request = Request(
            f"{url}?{urlencode(params)}",
            headers={"Accept": "application/json", "User-Agent": "kalshi-hybrid-backtest/1.0"},
        )
        with urlopen(request, timeout=self.timeout_seconds) as response:  # noqa: S310 - fixed Kalshi URLs above
            payload = json.loads(response.read().decode("utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("Kalshi API returned a non-object response")
        return payload

    def _fetch_endpoint(self, url: str, extra_params: dict[str, str]) -> list[SettlementMarket]:
        cursor: str | None = None
        seen_cursors: set[str] = set()
        records: list[SettlementMarket] = []
        while True:
            params = {"series_ticker": SERIES_TICKER, "limit": "1000", **extra_params}
            if cursor:
                params["cursor"] = cursor
            payload = self._get_json(url, params)
            rows = payload.get("markets")
            if not isinstance(rows, list):
                raise ValueError(f"Kalshi response from {url} has no markets list")
            source = "kalshi_public_historical" if "/historical/" in url else "kalshi_public_current"
            records.extend(
                market for item in rows if isinstance(item, dict)
                if (market := SettlementMarket.from_api(item, source)) is not None
            )
            next_cursor = payload.get("cursor")
            if not next_cursor:
                break
            cursor = str(next_cursor)
            if cursor in seen_cursors:
                raise RuntimeError(f"Kalshi pagination cursor repeated for {url}")
            seen_cursors.add(cursor)
            # Be polite to a public endpoint while still making a complete
            # cache in one run.  A normal 20k-market download is ~20 pages.
            time.sleep(0.05)
        LOG.info("downloaded %d valid settlements from %s", len(records), url)
        return records

    def refresh(self) -> list[SettlementMarket]:
        by_ticker: dict[str, SettlementMarket] = {}
        endpoint_errors: list[str] = []
        for url, params in PUBLIC_ENDPOINTS:
            try:
                for record in self._fetch_endpoint(url, params):
                    previous = by_ticker.get(record.ticker)
                    # Prefer a newer explicit settlement timestamp if an API
                    # rollover exposes the same ticker from both endpoints.
                    if previous is None or record.settlement_time > previous.settlement_time:
                        by_ticker[record.ticker] = record
            except (HTTPError, URLError, TimeoutError, ValueError) as exc:
                endpoint_errors.append(f"{url}: {exc}")
                LOG.warning("settlement endpoint unavailable: %s", endpoint_errors[-1])
        if not by_ticker:
            raise RuntimeError("No settled KXBTC15M markets downloaded; " + "; ".join(endpoint_errors))
        markets = sorted(by_ticker.values(), key=lambda item: (item.open_time, item.ticker))
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.cache_path.write_text(
            json.dumps({
                "series_ticker": SERIES_TICKER,
                "downloaded_at": timestamp_text(datetime.now(tz=UTC)),
                "markets": [market.to_cache() for market in markets],
            }, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        return markets

    def load(self, refresh: bool = False) -> list[SettlementMarket]:
        if refresh or not self.cache_path.exists():
            return self.refresh()
        payload = json.loads(self.cache_path.read_text(encoding="utf-8"))
        rows = payload.get("markets") if isinstance(payload, dict) else None
        if not isinstance(rows, list):
            raise ValueError(f"invalid settlement cache: {self.cache_path}")
        records = [SettlementMarket.from_cache(row) for row in rows if isinstance(row, dict)]
        if not records:
            raise ValueError(f"empty settlement cache: {self.cache_path}")
        return sorted(records, key=lambda item: (item.open_time, item.ticker))


def reconstruct_signals(
    markets: Iterable[SettlementMarket], decision_delay_seconds: int = 45,
    signal_mode: str = "inverse_latest_settlement",
) -> tuple[list[HistoricalSignal], dict[str, Any]]:
    """Build fixed-settlement directional signals without redrawing results.

    ``inverse_latest_settlement`` preserves the original audit: at each
    target's ``open + 45s`` it uses the most recently officially settled
    prior market.  ``sticky_until_directional_win`` is the v8 shadow rule:
    it treats the immediately preceding market's *actual final settlement* as
    the proxy for the realtime >=99c provisional outcome available at the
    boundary.  That proxy is clearly labelled, rather than pretending the
    delayed historical settlement timestamp was available at market open.

    In both modes the target settlement is immutable historical data.  It is
    never sampled, simulated, or modified by execution results.
    """

    if signal_mode not in {"inverse_latest_settlement", "sticky_until_directional_win"}:
        raise ValueError("signal_mode must be inverse_latest_settlement or sticky_until_directional_win")

    ordered = sorted(markets, key=lambda item: (item.open_time, item.ticker))
    settled = sorted(ordered, key=lambda item: (item.settlement_time, item.ticker))
    available: list[SettlementMarket] = []
    next_settlement = 0
    signals: list[HistoricalSignal] = []
    missing_source = 0
    invalid_order = 0

    active_side: str | None = None
    for target_index, target in enumerate(ordered):
        if signal_mode == "sticky_until_directional_win":
            # At a live boundary the prior window's final quote determines
            # direction before the official endpoint responds.  Historical
            # settlement data cannot prove that quote, so this uses the
            # eventually confirmed result only as a transparent proxy.
            if target_index == 0:
                missing_source += 1
                continue
            source = ordered[target_index - 1]
            if source.close_time > target.open_time:
                invalid_order += 1
                continue
            decision_time = target.open_time
            predicted, _transition = sticky_directional_prediction(active_side, source.result)
            active_side = predicted
            signals.append(HistoricalSignal(
                market_index=len(signals), ticker=target.ticker, open_time=target.open_time,
                close_time=target.close_time, settlement_time=target.settlement_time,
                actual_result=target.result, source_ticker=source.ticker,
                source_close_time=source.close_time, source_settlement_time=source.settlement_time,
                source_result=source.result, decision_time=decision_time, predicted_side=predicted,
                directional_win=(target.result == predicted), signal_mode=signal_mode,
                source_outcome_mode="immediate_previous_actual_settlement_proxy",
            ))
            continue
        decision_time = target.open_time + timedelta(seconds=decision_delay_seconds)
        while next_settlement < len(settled) and settled[next_settlement].settlement_time <= decision_time:
            source = settled[next_settlement]
            if source.close_time <= target.open_time and source.ticker != target.ticker:
                available.append(source)
            next_settlement += 1
        if not available:
            missing_source += 1
            continue
        # Settlement times are sorted; in a timestamp tie, ticker ordering is
        # deterministic.  The last qualifying source is therefore causal.
        source = available[-1]
        if source.open_time >= target.open_time:
            invalid_order += 1
            continue
        predicted = "no" if source.result == "yes" else "yes"
        signals.append(HistoricalSignal(
            market_index=len(signals), ticker=target.ticker, open_time=target.open_time,
            close_time=target.close_time, settlement_time=target.settlement_time,
            actual_result=target.result, source_ticker=source.ticker,
            source_close_time=source.close_time, source_settlement_time=source.settlement_time,
            source_result=source.result, decision_time=decision_time, predicted_side=predicted,
            directional_win=(target.result == predicted), signal_mode=signal_mode,
            source_outcome_mode="official_settlement",
        ))
    return signals, {
        "total_settled_markets": len(ordered),
        "eligible_predictions": len(signals),
        "missing_causal_source": missing_source,
        "invalid_source_order": invalid_order,
        "signal_mode": signal_mode,
        "source_outcome_mode": (
            "immediate_previous_actual_settlement_proxy"
            if signal_mode == "sticky_until_directional_win" else "official_settlement"
        ),
        "first_settled_market_timestamp": timestamp_text(ordered[0].open_time) if ordered else None,
        "last_settled_market_timestamp": timestamp_text(ordered[-1].open_time) if ordered else None,
    }


def signal_summary(signals: Iterable[HistoricalSignal], metadata: dict[str, int]) -> dict[str, Any]:
    values = list(signals)
    wins = sum(signal.directional_win for signal in values)
    losses = len(values) - wins
    return {
        **metadata,
        "directional_wins": wins,
        "directional_losses": losses,
        "directional_win_rate": wins / len(values) if values else None,
        "first_market_timestamp": metadata.get("first_settled_market_timestamp"),
        "last_market_timestamp": metadata.get("last_settled_market_timestamp"),
        "first_eligible_signal_timestamp": timestamp_text(values[0].open_time) if values else None,
        "last_eligible_signal_timestamp": timestamp_text(values[-1].open_time) if values else None,
    }


def write_signals_parquet(signals: Iterable[HistoricalSignal], path: Path = DEFAULT_SIGNALS) -> None:
    """Write the requested parquet artifact, requiring a real parquet engine."""

    try:
        import pandas as pd
    except ImportError as exc:  # pragma: no cover - dependency documented for the runner
        raise RuntimeError("pandas and pyarrow are required to write historical_signals.parquet") from exc
    rows = [signal.to_row() for signal in signals]
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cache", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--signals", type=Path, default=DEFAULT_SIGNALS)
    parser.add_argument("--refresh", action="store_true", help="redownload instead of using the local cache")
    parser.add_argument("--decision-delay-seconds", type=int, default=45)
    parser.add_argument(
        "--signal-mode", choices=("inverse_latest_settlement", "sticky_until_directional_win"),
        default="inverse_latest_settlement",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    markets = KalshiSettlementLoader(args.cache).load(refresh=args.refresh)
    signals, metadata = reconstruct_signals(markets, args.decision_delay_seconds, args.signal_mode)
    write_signals_parquet(signals, args.signals)
    print(json.dumps(signal_summary(signals, metadata), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
