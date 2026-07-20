"""Incrementally refresh the Prophet-free KXBTC15M ML feature ledger.

This utility downloads newly settled KXBTC15M markets and the corresponding
Coinbase BTC-USD one-minute candles, then appends only the 16 raw ML feature
columns used by the live classifier.  It never imports, trains, or records a
Prophet forecast.  Existing historical rows are normalized to the same
ML-only schema; legacy Prophet-named CSV columns are intentionally discarded.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import numpy as np
import pandas as pd

from kalshi_ml_features import FEATURE_SCHEMA, ML_ONLY_FEATURE_COLUMNS, feature_values


LOG = logging.getLogger("kalshi_ml_feature_ledger_refresh")
KALSHI_BASE_URL = "https://external-api.kalshi.com/trade-api/v2"
COINBASE_BASE_URL = "https://api.exchange.coinbase.com"
SERIES_TICKER = "KXBTC15M"
OUTCOMES = {"yes": 1, "no": 0}
CORE_COLUMNS = [
    "ticker", "source", "market_open", "forecast_at", "forecast_data_end", "settlement_ts",
    "strike", "expiration_value", "actual_outcome", "actual_yes",
]


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )


def timestamp(value: Any) -> pd.Timestamp | None:
    if value in (None, ""):
        return None
    try:
        result = pd.Timestamp(value)
    except (TypeError, ValueError):
        return None
    return result.tz_localize("UTC") if result.tzinfo is None else result.tz_convert("UTC")


def iso(value: pd.Timestamp | None) -> str:
    return "" if value is None else value.isoformat().replace("+00:00", "Z")


def number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def request_json(url: str, params: dict[str, Any], retries: int = 4) -> Any:
    query = urlencode({key: value for key, value in params.items() if value not in (None, "")})
    target = f"{url}?{query}" if query else url
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            request = Request(target, headers={"Accept": "application/json", "User-Agent": "kalshi-ml-ledger/1.0"})
            with urlopen(request, timeout=30) as response:  # noqa: S310 - fixed public APIs
                return json.loads(response.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt + 1 < retries:
                time.sleep(min(8.0, 0.5 * (2 ** attempt)))
    raise RuntimeError(f"Request failed after {retries} attempts: {target}: {last_error}")


def market_pages(path: str, source: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    cursor = ""
    seen: set[str] = set()
    markets: list[dict[str, Any]] = []
    while True:
        payload = request_json(
            f"{KALSHI_BASE_URL}{path}",
            {"series_ticker": SERIES_TICKER, "limit": 1000, "cursor": cursor, **params},
        )
        page = payload.get("markets") if isinstance(payload, dict) else None
        if not isinstance(page, list):
            raise RuntimeError(f"Kalshi {path} response did not contain a markets list")
        for market in page:
            if isinstance(market, dict) and str(market.get("ticker") or "").startswith(SERIES_TICKER + "-"):
                copied = dict(market)
                copied["_source"] = source
                markets.append(copied)
        cursor = str(payload.get("cursor") or "")
        if not cursor:
            break
        if cursor in seen:
            raise RuntimeError(f"Kalshi {path} repeated a cursor")
        seen.add(cursor)
    return markets


def settled_markets() -> list[dict[str, Any]]:
    raw = market_pages("/markets", "current", {"status": "settled"})
    raw += market_pages("/historical/markets", "historical", {})
    deduplicated: dict[str, dict[str, Any]] = {}
    for market in raw:
        ticker = str(market.get("ticker") or "")
        if ticker:
            deduplicated.setdefault(ticker, market)
    normalized: list[dict[str, Any]] = []
    for market in deduplicated.values():
        result = str(market.get("result") or "").lower()
        open_at = timestamp(market.get("open_time"))
        settled_at = timestamp(market.get("settlement_ts"))
        strike = number(market.get("floor_strike"))
        if result not in OUTCOMES or open_at is None or settled_at is None or strike is None:
            continue
        normalized.append({
            "ticker": str(market.get("ticker") or ""),
            "source": str(market.get("_source") or ""),
            "result": result,
            "open_at": open_at,
            "settled_at": settled_at,
            "strike": strike,
            "expiration_value": number(market.get("expiration_value")),
        })
    normalized.sort(key=lambda market: (market["open_at"], market["ticker"]))
    LOG.info("Loaded %d eligible settled KXBTC15M markets.", len(normalized))
    return normalized


def coinbase_candles(start: pd.Timestamp, end: pd.Timestamp, pause_seconds: float) -> tuple[pd.DataFrame, list[dict[str, str]]]:
    """Fetch a contiguous, non-interpolated one-minute BTC-USD range."""
    start, end = start.floor("min"), end.floor("min")
    records: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + pd.Timedelta(minutes=299), end)
        try:
            payload = request_json(
                f"{COINBASE_BASE_URL}/products/BTC-USD/candles",
                {
                    "granularity": 60,
                    "start": cursor.isoformat().replace("+00:00", "Z"),
                    "end": chunk_end.isoformat().replace("+00:00", "Z"),
                },
            )
            if not isinstance(payload, list):
                raise RuntimeError(f"Coinbase returned {type(payload).__name__}, not a list")
            for candle in payload:
                if not isinstance(candle, list) or len(candle) < 5:
                    continue
                at = pd.Timestamp(float(candle[0]), unit="s", tz="UTC").floor("min")
                close = number(candle[4])
                if close is not None and close > 0:
                    records.append({"ds": at, "close": close})
        except RuntimeError as exc:
            failures.append({"start": iso(cursor), "end": iso(chunk_end), "reason": str(exc)})
            LOG.warning("Coinbase candles failed %s through %s: %s", cursor, chunk_end, exc)
        cursor = chunk_end + pd.Timedelta(minutes=1)
        if pause_seconds:
            time.sleep(pause_seconds)
    if not records:
        return pd.DataFrame(columns=["ds", "close"]), failures
    frame = pd.DataFrame(records)
    frame["ds"] = pd.to_datetime(frame["ds"], utc=True)
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    return (
        frame.dropna().drop_duplicates("ds", keep="last").sort_values("ds").reset_index(drop=True),
        failures,
    )


def exact_window(candles: pd.DataFrame, end: pd.Timestamp) -> pd.DataFrame | None:
    start = end - pd.Timedelta(minutes=60)
    window = candles[(candles["ds"] >= start) & (candles["ds"] <= end)].copy()
    expected = pd.date_range(start=start, end=end, periods=61, tz="UTC")
    if len(window) != 61 or not pd.DatetimeIndex(window["ds"]).equals(expected):
        return None
    return window.reset_index(drop=True)


def read_existing(path: Path) -> pd.DataFrame:
    rows = pd.read_csv(path)
    required = {"ticker", "forecast_at", "settlement_ts", "actual_yes"} | set(ML_ONLY_FEATURE_COLUMNS)
    missing = required - set(rows.columns)
    if missing:
        raise ValueError(f"Existing ledger is missing columns: {', '.join(sorted(missing))}")
    output = pd.DataFrame()
    for column in CORE_COLUMNS:
        output[column] = rows[column] if column in rows else ""
    for column in ML_ONLY_FEATURE_COLUMNS:
        output[column] = pd.to_numeric(rows[column], errors="coerce")
    output["forecast_at"] = pd.to_datetime(output["forecast_at"], utc=True, errors="coerce", format="mixed")
    output["settlement_ts"] = pd.to_datetime(output["settlement_ts"], utc=True, errors="coerce", format="mixed")
    output["actual_yes"] = pd.to_numeric(output["actual_yes"], errors="coerce")
    output = output[
        output["ticker"].notna()
        & output["forecast_at"].notna()
        & output["settlement_ts"].notna()
        & output["actual_yes"].isin([0, 1])
        & output[ML_ONLY_FEATURE_COLUMNS].notna().all(axis=1)
    ].copy()
    output["forecast_at"] = output["forecast_at"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    output["settlement_ts"] = output["settlement_ts"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return output.drop_duplicates("ticker", keep="last")


def refresh(existing: pd.DataFrame, markets: list[dict[str, Any]], pause_seconds: float) -> tuple[pd.DataFrame, dict[str, Any]]:
    known_tickers = set(existing["ticker"].astype(str))
    latest_existing_forecast = pd.to_datetime(existing["forecast_at"], utc=True, errors="coerce", format="mixed").max()
    # This is deliberately an append-only refresher.  A legacy artifact can
    # contain old gaps caused by its former Prophet replay; re-fetching all of
    # them every day would be slow and would not add newly settled information.
    # Historical repairs belong in an explicit full-ledger rebuild.
    additions = [
        market for market in markets
        if market["ticker"] not in known_tickers
        and (pd.isna(latest_existing_forecast) or (market["open_at"] - pd.Timedelta(seconds=120)) > latest_existing_forecast)
    ]
    report: dict[str, Any] = {
        "feature_schema": FEATURE_SCHEMA,
        "uses_prophet": False,
        "existing_rows": int(len(existing)),
        "latest_existing_forecast": iso(latest_existing_forecast),
        "eligible_markets": int(len(markets)),
        "candidate_new_markets": int(len(additions)),
        "added_rows": 0,
        "skipped_markets": [],
    }
    if not additions:
        return existing, report
    first_forecast = additions[0]["open_at"] - pd.Timedelta(seconds=120)
    last_forecast = additions[-1]["open_at"] - pd.Timedelta(seconds=120)
    candles, failures = coinbase_candles(
        first_forecast.floor("min") - pd.Timedelta(minutes=61),
        last_forecast.floor("min") - pd.Timedelta(minutes=1),
        pause_seconds,
    )
    report["coinbase_failures"] = failures
    report["candle_range"] = {"start": iso(first_forecast), "end": iso(last_forecast)}
    settled_stream = sorted((market["settled_at"], OUTCOMES[market["result"]]) for market in markets)
    known_outcomes: list[int] = []
    cursor = 0
    rows: list[dict[str, Any]] = []
    for market in additions:
        forecast_at = market["open_at"] - pd.Timedelta(seconds=120)
        while cursor < len(settled_stream) and settled_stream[cursor][0] < forecast_at:
            known_outcomes.append(settled_stream[cursor][1])
            cursor += 1
        data_end = forecast_at.floor("min") - pd.Timedelta(minutes=1)
        window = exact_window(candles, data_end)
        if window is None:
            report["skipped_markets"].append({"ticker": market["ticker"], "reason": "missing_completed_candles"})
            continue
        row = {
            "ticker": market["ticker"],
            "source": market["source"],
            "market_open": iso(market["open_at"]),
            "forecast_at": iso(forecast_at),
            "forecast_data_end": iso(data_end),
            "settlement_ts": iso(market["settled_at"]),
            "strike": market["strike"],
            "expiration_value": market["expiration_value"],
            "actual_outcome": market["result"],
            "actual_yes": OUTCOMES[market["result"]],
        }
        row.update(feature_values(window, market["strike"], list(known_outcomes), market["open_at"]))
        rows.append(row)
    combined = pd.concat([existing, pd.DataFrame(rows)], ignore_index=True)
    combined = combined.drop_duplicates("ticker", keep="last")
    forecast_times = pd.to_datetime(combined["forecast_at"], utc=True, errors="coerce")
    combined = combined.assign(_forecast=forecast_times).sort_values("_forecast", kind="stable").drop(columns="_forecast")
    report["added_rows"] = int(len(rows))
    report["total_rows"] = int(len(combined))
    return combined[CORE_COLUMNS + ML_ONLY_FEATURE_COLUMNS], report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True, help="Prior ML-only or legacy feature ledger.")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--request-pause-seconds", type=float, default=0.10)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.request_pause_seconds < 0:
        raise SystemExit("--request-pause-seconds cannot be negative")
    existing = read_existing(args.input)
    refreshed, report = refresh(existing, settled_markets(), args.request_pause_seconds)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    refreshed.to_csv(args.output, index=False)
    args.report.write_text(json.dumps({
        **report,
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "output": str(args.output),
    }, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    LOG.info("ML-only ledger refresh complete: added=%d total=%d skipped=%d", report["added_rows"], report.get("total_rows", len(refreshed)), len(report["skipped_markets"]))
    return 0


if __name__ == "__main__":
    configure_logging()
    raise SystemExit(main())
