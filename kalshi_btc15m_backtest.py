"""Historical KXBTC15M outcome export and leakage-safe strategy backtest.

This script never creates orders and does not need Kalshi credentials.  It
downloads every settled KXBTC15M market exposed by Kalshi's current and
archived public endpoints, then recreates the live Prophet decision with BTC
one-minute candles available before the two-minute pre-open forecast time.

The machine-learning result is an expanding-window logistic regression.  It
uses only information available at the prediction time:

* lagged *settled* Kalshi outcomes (the immediately preceding contract is not
  available two minutes before the next market opens, so it is excluded until
  its settlement timestamp),
* the historical strike, and
* Coinbase BTC-USD one-minute price features plus the Prophet forecast.

The output is deliberately classification-only.  Historical market outcomes
do not establish the executable opening fill price of a market order, so a
dollar P&L calculation would be fabricated.  The CSV rows retain enough data
to add an execution-price model later.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

import kalshibtc15minupordown as live_bot


LOG = logging.getLogger("kalshi_btc15m_backtest")

KALSHI_BASE_URL = "https://external-api.kalshi.com/trade-api/v2"
COINBASE_BASE_URL = "https://api.exchange.coinbase.com"
SERIES_TICKER = "KXBTC15M"
COINBASE_PRODUCT = "BTC-USD"
MINUTE = pd.Timedelta(minutes=1)
OUTCOME_YES = "yes"
OUTCOME_NO = "no"
OUTCOME_VALUES = {OUTCOME_YES: 1, OUTCOME_NO: 0}

FEATURE_COLUMNS = [
    "spot_vs_strike_bps",
    "return_1m_bps",
    "return_5m_bps",
    "return_15m_bps",
    "return_60m_bps",
    "vol_15m_bps",
    "vol_60m_bps",
    "range_15m_bps",
    "prophet_p50_vs_strike_bps",
    "prophet_p50_vs_spot_bps",
    "prophet_interval_bps",
    "lag_outcome_1",
    "lag_outcome_2",
    "lag_outcome_4",
    "lag_outcome_8",
    "known_yes_rate_8",
    "known_outcome_count",
    "hour_sin",
    "hour_cos",
]


def configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    logging.getLogger("cmdstanpy").setLevel(logging.WARNING)
    logging.getLogger("prophet").setLevel(logging.WARNING)


def parse_timestamp(value: Any) -> Optional[pd.Timestamp]:
    """Return a UTC minute-compatible timestamp, or None for missing values."""
    if value in (None, ""):
        return None
    try:
        ts = pd.Timestamp(value)
    except (TypeError, ValueError):
        return None
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def iso_timestamp(value: Any) -> str:
    ts = parse_timestamp(value)
    return ts.isoformat().replace("+00:00", "Z") if ts is not None else ""


def as_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def http_json(url: str, params: dict[str, Any], retries: int = 4,
              timeout_s: int = 30) -> Any:
    """Fetch public JSON with bounded retries and useful HTTP error context."""
    query = urlencode({key: value for key, value in params.items()
                       if value not in (None, "")})
    request_url = f"{url}?{query}" if query else url
    last_error: Optional[Exception] = None
    for attempt in range(retries):
        try:
            request = Request(request_url, headers={
                "Accept": "application/json",
                "User-Agent": "kalshi-btc15m-backtest/1.0",
            })
            with urlopen(request, timeout=timeout_s) as response:  # noqa: S310 - fixed public APIs
                return json.loads(response.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt + 1 == retries:
                break
            wait_s = min(8.0, 0.5 * (2 ** attempt))
            LOG.warning("Request failed (%s); retrying in %.1fs: %s",
                        type(exc).__name__, wait_s, request_url)
            time.sleep(wait_s)
    raise RuntimeError(f"Request failed after {retries} attempts: {request_url}: {last_error}")


def fetch_market_pages(path: str, source: str, extra_params: dict[str, Any]) -> list[dict]:
    """Read every cursor page from one Kalshi market collection endpoint."""
    cursor = ""
    seen_cursors: set[str] = set()
    markets: list[dict] = []
    page_count = 0
    while True:
        params = {
            "series_ticker": SERIES_TICKER,
            "limit": 1000,
            **extra_params,
        }
        if cursor:
            params["cursor"] = cursor
        payload = http_json(f"{KALSHI_BASE_URL}{path}", params)
        page = payload.get("markets") if isinstance(payload, dict) else None
        if not isinstance(page, list):
            raise RuntimeError(f"Kalshi {path} response has no markets list")
        for market in page:
            if isinstance(market, dict) and str(market.get("ticker", "")).startswith(SERIES_TICKER + "-"):
                copied = dict(market)
                copied["_source"] = source
                markets.append(copied)
        page_count += 1
        next_cursor = str(payload.get("cursor") or "")
        LOG.info("Kalshi %s page %d: %d KXBTC15M markets", path, page_count, len(markets))
        if not next_cursor:
            break
        if next_cursor in seen_cursors:
            raise RuntimeError(f"Kalshi {path} repeated cursor after page {page_count}")
        seen_cursors.add(next_cursor)
        cursor = next_cursor
    return markets


def market_export_row(market: dict) -> dict[str, Any]:
    """Normalize fields that are stable across current and archived endpoints."""
    result = str(market.get("result") or "").lower()
    return {
        "ticker": str(market.get("ticker") or ""),
        "source": str(market.get("_source") or ""),
        "status": str(market.get("status") or ""),
        "result": result,
        "open_time": iso_timestamp(market.get("open_time")),
        "close_time": iso_timestamp(market.get("close_time")),
        "settlement_ts": iso_timestamp(market.get("settlement_ts")),
        "expected_expiration_time": iso_timestamp(market.get("expected_expiration_time")),
        "floor_strike": as_float(market.get("floor_strike")),
        "expiration_value": as_float(market.get("expiration_value")),
        "strike_type": str(market.get("strike_type") or ""),
        "market_type": str(market.get("market_type") or ""),
        "title": str(market.get("title") or ""),
        "yes_sub_title": str(market.get("yes_sub_title") or ""),
        "volume_fp": as_float(market.get("volume_fp")),
        "open_interest_fp": as_float(market.get("open_interest_fp")),
    }


def fetch_all_closed_markets() -> list[dict[str, Any]]:
    """Fetch current then archived closed markets, deduplicate, sort by open time."""
    current = fetch_market_pages("/markets", "current", {"status": "settled"})
    archived = fetch_market_pages("/historical/markets", "historical", {})
    by_ticker: dict[str, dict[str, Any]] = {}
    for market in current + archived:
        ticker = str(market.get("ticker") or "")
        if ticker:
            by_ticker.setdefault(ticker, market)
    rows = [market_export_row(market) for market in by_ticker.values()]
    rows.sort(key=lambda row: (row["open_time"], row["ticker"]))
    LOG.info("Fetched %d unique closed KXBTC15M markets (%d current, %d archived)",
             len(rows), len(current), len(archived))
    return rows


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(value, handle, indent=2, sort_keys=True, default=str)
        handle.write("\n")


def fetch_coinbase_candles(start: pd.Timestamp, end: pd.Timestamp,
                           request_pause_s: float) -> tuple[pd.DataFrame, list[dict[str, str]]]:
    """Download a contiguous BTC-USD one-minute archive in 300-minute requests.

    Coinbase returns at most 300 candles per request.  Chunks are intentionally
    non-overlapping so every absent minute remains visible as a gap; no prices
    are interpolated or forward-filled.
    """
    start = start.floor("min")
    end = end.floor("min")
    if start > end:
        return pd.DataFrame(columns=["ds", "close"]), []

    candles: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    cursor = start
    request_count = 0
    while cursor <= end:
        chunk_end = min(cursor + pd.Timedelta(minutes=299), end)
        params = {
            "granularity": 60,
            "start": cursor.isoformat().replace("+00:00", "Z"),
            "end": chunk_end.isoformat().replace("+00:00", "Z"),
        }
        try:
            payload = http_json(
                f"{COINBASE_BASE_URL}/products/{COINBASE_PRODUCT}/candles", params)
            if not isinstance(payload, list):
                raise RuntimeError(f"Coinbase returned {type(payload).__name__}, not a candle list")
            for candle in payload:
                if not isinstance(candle, list) or len(candle) < 5:
                    continue
                # Coinbase returns Unix seconds.  ``pd.Timestamp(number)``
                # interprets a bare number as nanoseconds, which would turn
                # modern candles into 1970 timestamps.
                timestamp = pd.Timestamp(float(candle[0]), unit="s", tz="UTC")
                close = as_float(candle[4])
                if timestamp is not None and close is not None and close > 0:
                    candles.append({"ds": timestamp.floor("min"), "close": close})
        except RuntimeError as exc:
            failures.append({
                "start": iso_timestamp(cursor),
                "end": iso_timestamp(chunk_end),
                "reason": str(exc),
            })
            LOG.error("Candle request failed for %s through %s: %s", cursor, chunk_end, exc)
        request_count += 1
        if request_count % 25 == 0:
            LOG.info("Coinbase candle requests: %d; through %s", request_count, chunk_end)
        cursor = chunk_end + MINUTE
        if request_pause_s:
            time.sleep(request_pause_s)

    if not candles:
        return pd.DataFrame(columns=["ds", "close"]), failures
    frame = pd.DataFrame(candles)
    frame["ds"] = pd.to_datetime(frame["ds"], utc=True)
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    frame = (frame.dropna()
             .drop_duplicates(subset=["ds"], keep="last")
             .sort_values("ds")
             .reset_index(drop=True))
    LOG.info("Loaded %d BTC-USD one-minute candles from Coinbase (%s to %s)",
             len(frame), frame["ds"].iloc[0], frame["ds"].iloc[-1])
    return frame, failures


def eligible_markets(markets: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return markets whose result, strike, and timing support a fair replay."""
    result: list[dict[str, Any]] = []
    for market in markets:
        if market["result"] not in OUTCOME_VALUES:
            continue
        if market["floor_strike"] is None:
            continue
        if parse_timestamp(market["open_time"]) is None:
            continue
        result.append(market)
    return result


def known_outcomes_before(markets: Iterable[dict[str, Any]], forecast_at: pd.Timestamp) -> list[int]:
    """Outcomes settled before a forecast time, in settlement-time order.

    This is the guard against the common backtest error of feeding the prior
    15-minute market's result into a forecast made before that market settled.
    """
    settled: list[tuple[pd.Timestamp, int]] = []
    for market in markets:
        settlement = parse_timestamp(market.get("settlement_ts"))
        outcome = OUTCOME_VALUES.get(str(market.get("result") or "").lower())
        if settlement is not None and outcome is not None and settlement <= forecast_at:
            settled.append((settlement, outcome))
    settled.sort(key=lambda item: item[0])
    return [outcome for _, outcome in settled]


def settled_outcome_stream(markets: Iterable[dict[str, Any]]) -> list[tuple[pd.Timestamp, int]]:
    """Pre-sort settlement outcomes once for the chronological replay loop."""
    stream: list[tuple[pd.Timestamp, int]] = []
    for market in markets:
        settlement = parse_timestamp(market.get("settlement_ts"))
        outcome = OUTCOME_VALUES.get(str(market.get("result") or "").lower())
        if settlement is not None and outcome is not None:
            stream.append((settlement, outcome))
    stream.sort(key=lambda item: item[0])
    return stream


def exact_candle_window(candles: pd.DataFrame, end: pd.Timestamp,
                        history_minutes: int) -> tuple[Optional[pd.DataFrame], str]:
    """Return exactly N consecutive completed candles ending at ``end``."""
    if candles.empty:
        return None, "no_candles"
    start = end - pd.Timedelta(minutes=history_minutes - 1)
    window = candles[(candles["ds"] >= start) & (candles["ds"] <= end)].copy()
    expected = pd.date_range(start=start, end=end, freq="min", tz="UTC")
    if len(window) != history_minutes:
        return None, f"expected_{history_minutes}_candles_found_{len(window)}"
    observed = pd.DatetimeIndex(window["ds"])
    if not observed.equals(expected):
        return None, "missing_or_duplicate_minute"
    return window.reset_index(drop=True), "ok"


def bps_change(end_value: float, start_value: float) -> float:
    return (end_value / start_value - 1.0) * 10_000.0


def feature_values(window: pd.DataFrame, strike: float, forecast: dict[str, float],
                   known_outcomes: list[int], market_open: pd.Timestamp) -> dict[str, float]:
    """Build only pre-open numeric features for one historical forecast."""
    close = window["close"].astype(float).to_numpy()
    spot = float(close[-1])
    returns = np.diff(np.log(close)) * 10_000.0
    recent_15 = close[-15:]
    latest_8 = known_outcomes[-8:]
    lag = lambda count: float(known_outcomes[-count]) if len(known_outcomes) >= count else 0.5
    minutes = market_open.hour * 60 + market_open.minute
    radians = 2.0 * math.pi * minutes / (24.0 * 60.0)
    p10, p50, p90 = (float(forecast[key]) for key in ("p10", "p50", "p90"))
    return {
        "spot_vs_strike_bps": bps_change(spot, strike),
        "return_1m_bps": bps_change(spot, float(close[-2])),
        "return_5m_bps": bps_change(spot, float(close[-6])),
        "return_15m_bps": bps_change(spot, float(close[-16])),
        "return_60m_bps": bps_change(spot, float(close[-61])),
        "vol_15m_bps": float(np.std(returns[-15:], ddof=0)),
        "vol_60m_bps": float(np.std(returns[-60:], ddof=0)),
        "range_15m_bps": bps_change(float(np.max(recent_15)), float(np.min(recent_15))),
        "prophet_p50_vs_strike_bps": bps_change(p50, strike),
        "prophet_p50_vs_spot_bps": bps_change(p50, spot),
        "prophet_interval_bps": bps_change(p90, p10),
        "lag_outcome_1": lag(1),
        "lag_outcome_2": lag(2),
        "lag_outcome_4": lag(4),
        "lag_outcome_8": lag(8),
        "known_yes_rate_8": float(np.mean(latest_8)) if latest_8 else 0.5,
        "known_outcome_count": float(len(known_outcomes)),
        "hour_sin": math.sin(radians),
        "hour_cos": math.cos(radians),
    }


def run_prophet_backtest(markets: list[dict[str, Any]], candles: pd.DataFrame,
                         history_minutes: int, forecast_minutes: int,
                         preopen_lead_seconds: float,
                         uncertainty_samples: int,
                         metrics_every: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Recreate the live Prophet side decision for each eligible market."""
    previous_uncertainty = live_bot.UNCERTAINTY_SAMPLES
    live_bot.UNCERTAINTY_SAMPLES = uncertainty_samples
    rows: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    valid = eligible_markets(markets)
    settlement_stream = settled_outcome_stream(markets)
    known_outcomes: list[int] = []
    settlement_cursor = 0
    last_reported_rows = 0
    try:
        for index, market in enumerate(valid, start=1):
            market_open = parse_timestamp(market["open_time"])
            assert market_open is not None
            forecast_at = market_open - pd.Timedelta(seconds=preopen_lead_seconds)
            while (settlement_cursor < len(settlement_stream)
                   and settlement_stream[settlement_cursor][0] <= forecast_at):
                known_outcomes.append(settlement_stream[settlement_cursor][1])
                settlement_cursor += 1
            # The live yfinance call at hh:mm:43 has the completed hh:mm:42
            # candle as its latest observation, not the in-progress hh:mm:43 bar.
            data_end = forecast_at.floor("min") - MINUTE
            window, window_reason = exact_candle_window(candles, data_end, history_minutes)
            if window is None:
                skipped.append({
                    "ticker": market["ticker"], "reason": window_reason,
                    "market_open": iso_timestamp(market_open),
                    "forecast_at": iso_timestamp(forecast_at),
                    "data_end": iso_timestamp(data_end),
                })
                continue
            forecast = live_bot.run_prophet_forecast(window, periods=forecast_minutes)
            if forecast is None:
                skipped.append({
                    "ticker": market["ticker"], "reason": "prophet_failed",
                    "market_open": iso_timestamp(market_open),
                    "forecast_at": iso_timestamp(forecast_at),
                    "data_end": iso_timestamp(data_end),
                })
                continue
            side, decision = live_bot.decide_side_from_forecast(market["floor_strike"], forecast)
            if side is None:
                skipped.append({
                    "ticker": market["ticker"], "reason": "prophet_equal_to_strike",
                    "market_open": iso_timestamp(market_open),
                    "forecast_at": iso_timestamp(forecast_at),
                    "data_end": iso_timestamp(data_end),
                })
                continue
            # Copy because the next market will append more settled outcomes.
            known = list(known_outcomes)
            actual_yes = OUTCOME_VALUES[market["result"]]
            row = {
                "ticker": market["ticker"],
                "source": market["source"],
                "market_open": iso_timestamp(market_open),
                "forecast_at": iso_timestamp(forecast_at),
                "forecast_data_end": iso_timestamp(data_end),
                "settlement_ts": market["settlement_ts"],
                "strike": market["floor_strike"],
                "expiration_value": market["expiration_value"],
                "actual_outcome": market["result"],
                "actual_yes": actual_yes,
                "prophet_p10": float(forecast["p10"]),
                "prophet_p50": float(forecast["p50"]),
                "prophet_p90": float(forecast["p90"]),
                "prophet_side": side,
                "prophet_decision": decision,
                "prophet_correct": int(side == market["result"]),
                "known_outcome_count": len(known),
            }
            row.update(feature_values(window, float(market["floor_strike"]), forecast, known, market_open))
            rows.append(row)
            if (len(rows) - last_reported_rows >= metrics_every
                    or index == len(valid)):
                metrics = prophet_summary(rows)
                LOG.info(
                    "PROPHET RUNNING METRICS | processed %d/%d | forecasts %d | "
                    "correct %d | accuracy %.2f%% | actual YES %.2f%% | predicted YES %.2f%% | "
                    "P50 MAE $%.2f | skipped %d",
                    index, len(valid), metrics["predictions"], metrics["correct"],
                    100.0 * metrics["accuracy"], 100.0 * metrics["actual_yes_rate"],
                    100.0 * metrics["predicted_yes_rate"],
                    float(metrics.get("p50_mae_usd") or 0.0), len(skipped),
                )
                last_reported_rows = len(rows)
    finally:
        live_bot.UNCERTAINTY_SAMPLES = previous_uncertainty
    return rows, skipped


def binary_metrics(actual: list[int], predicted: list[int],
                   probabilities: Optional[list[float]] = None) -> dict[str, Any]:
    if not actual:
        return {"predictions": 0}
    true_positive = sum(a == 1 and p == 1 for a, p in zip(actual, predicted))
    true_negative = sum(a == 0 and p == 0 for a, p in zip(actual, predicted))
    false_positive = sum(a == 0 and p == 1 for a, p in zip(actual, predicted))
    false_negative = sum(a == 1 and p == 0 for a, p in zip(actual, predicted))
    total = len(actual)
    accuracy = (true_positive + true_negative) / total
    yes_precision = true_positive / (true_positive + false_positive) if true_positive + false_positive else None
    yes_recall = true_positive / (true_positive + false_negative) if true_positive + false_negative else None
    no_recall = true_negative / (true_negative + false_positive) if true_negative + false_positive else None
    summary: dict[str, Any] = {
        "predictions": total,
        "correct": true_positive + true_negative,
        "incorrect": false_positive + false_negative,
        "accuracy": accuracy,
        "accuracy_95pct_wilson_approx": [
            max(0.0, accuracy - 1.96 * math.sqrt(accuracy * (1.0 - accuracy) / total)),
            min(1.0, accuracy + 1.96 * math.sqrt(accuracy * (1.0 - accuracy) / total)),
        ],
        "actual_yes_rate": sum(actual) / total,
        "predicted_yes_rate": sum(predicted) / total,
        "yes_precision": yes_precision,
        "yes_recall": yes_recall,
        "no_recall": no_recall,
        "confusion_matrix": {
            "true_positive": true_positive,
            "true_negative": true_negative,
            "false_positive": false_positive,
            "false_negative": false_negative,
        },
    }
    if probabilities is not None:
        clipped = [min(1.0 - 1e-12, max(1e-12, p)) for p in probabilities]
        summary["brier_score"] = float(np.mean([(p - a) ** 2 for p, a in zip(clipped, actual)]))
        summary["log_loss"] = float(-np.mean([
            a * math.log(p) + (1 - a) * math.log(1 - p)
            for a, p in zip(actual, clipped)
        ]))
    return summary


def add_walk_forward_ml(rows: list[dict[str, Any]], min_train_rows: int,
                        retrain_every: int, metrics_every: int) -> dict[str, Any]:
    """Add chronologically out-of-sample ML predictions to already-built rows."""
    model = None
    last_fit_index = -retrain_every
    evaluated_actual: list[int] = []
    evaluated_predicted: list[int] = []
    evaluated_probabilities: list[float] = []
    settlement_order = sorted(
        (settlement, index)
        for index, row in enumerate(rows)
        if (settlement := parse_timestamp(row["settlement_ts"])) is not None
    )
    training_indices: list[int] = []
    settlement_cursor = 0

    for index, row in enumerate(rows):
        forecast_at = parse_timestamp(row["forecast_at"])
        assert forecast_at is not None
        while (settlement_cursor < len(settlement_order)
               and settlement_order[settlement_cursor][0] <= forecast_at):
            settled_index = settlement_order[settlement_cursor][1]
            # A market cannot settle before its own forecast, but retain the
            # guard so an anomalous timestamp can never leak a future row.
            if settled_index < index:
                training_indices.append(settled_index)
            settlement_cursor += 1
        row["ml_train_rows"] = len(training_indices)
        if (len(training_indices) < min_train_rows
                or len({rows[train_index]["actual_yes"] for train_index in training_indices}) < 2):
            row["ml_probability_yes"] = None
            row["ml_side"] = ""
            row["ml_correct"] = None
            continue
        if model is None or index - last_fit_index >= retrain_every:
            x_train = np.asarray([
                [float(rows[train_index][name]) for name in FEATURE_COLUMNS]
                for train_index in training_indices
            ], dtype=float)
            y_train = np.asarray([
                int(rows[train_index]["actual_yes"]) for train_index in training_indices
            ], dtype=int)
            model = make_pipeline(
                StandardScaler(),
                LogisticRegression(C=0.25, max_iter=1000, class_weight="balanced", random_state=0),
            )
            model.fit(x_train, y_train)
            last_fit_index = index
        x_test = np.asarray([[float(row[name]) for name in FEATURE_COLUMNS]], dtype=float)
        probability_yes = float(model.predict_proba(x_test)[0][1])
        predicted_yes = int(probability_yes >= 0.5)
        actual_yes = int(row["actual_yes"])
        row["ml_probability_yes"] = probability_yes
        row["ml_side"] = OUTCOME_YES if predicted_yes else OUTCOME_NO
        row["ml_correct"] = int(predicted_yes == actual_yes)
        evaluated_actual.append(actual_yes)
        evaluated_predicted.append(predicted_yes)
        evaluated_probabilities.append(probability_yes)
        if len(evaluated_actual) % metrics_every == 0:
            metrics = binary_metrics(evaluated_actual, evaluated_predicted, evaluated_probabilities)
            LOG.info(
                "ML RUNNING METRICS | row %d/%d | predictions %d | correct %d | "
                "accuracy %.2f%% | Brier %.4f | log loss %.4f | train rows %d",
                index + 1, len(rows), metrics["predictions"], metrics["correct"],
                100.0 * metrics["accuracy"], float(metrics["brier_score"]),
                float(metrics["log_loss"]), len(training_indices),
            )

    summary = binary_metrics(evaluated_actual, evaluated_predicted, evaluated_probabilities)
    summary.update({
        "method": "expanding_window_logistic_regression",
        "feature_columns": FEATURE_COLUMNS,
        "minimum_training_rows": min_train_rows,
        "retrain_every_markets": retrain_every,
        "lookahead_guard": "Each fit includes only markets whose settlement timestamp was no later than the current forecast timestamp.",
    })
    if summary["predictions"] and summary["predictions"] % metrics_every:
        LOG.info(
            "ML RUNNING METRICS | complete | predictions %d | correct %d | "
            "accuracy %.2f%% | Brier %.4f | log loss %.4f",
            summary["predictions"], summary["correct"], 100.0 * summary["accuracy"],
            float(summary["brier_score"]), float(summary["log_loss"]),
        )
    return summary


def prophet_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    actual = [int(row["actual_yes"]) for row in rows]
    predicted = [int(row["prophet_side"] == OUTCOME_YES) for row in rows]
    summary = binary_metrics(actual, predicted)
    errors = [
        float(row["prophet_p50"]) - float(row["expiration_value"])
        for row in rows if row.get("expiration_value") is not None
    ]
    if errors:
        summary.update({
            "settlement_price_samples": len(errors),
            "p50_mae_usd": float(np.mean(np.abs(errors))),
            "p50_rmse_usd": float(np.sqrt(np.mean(np.square(errors)))),
            "p50_mean_error_usd": float(np.mean(errors)),
        })
    return summary


def summary_markdown(summary: dict[str, Any]) -> str:
    prophet = summary["prophet"]
    ml = summary["machine_learning"]
    coverage = summary["coverage"]

    def pct(value: Any) -> str:
        return "n/a" if value is None else f"{100.0 * float(value):.2f}%"

    def score(block: dict[str, Any]) -> str:
        if not block.get("predictions"):
            return "No out-of-sample predictions were produced."
        return (f"{block['correct']}/{block['predictions']} correct "
                f"({pct(block['accuracy'])}); actual YES {pct(block['actual_yes_rate'])}, "
                f"predicted YES {pct(block['predicted_yes_rate'])}.")

    lines = [
        "# KXBTC15M Historical Backtest",
        "",
        "## Coverage",
        "",
        f"- Closed KXBTC15M markets exported: {coverage['closed_markets_exported']}",
        f"- Markets eligible for forecast replay: {coverage['eligible_markets']}",
        f"- Prophet forecasts evaluated: {coverage['prophet_rows']}",
        f"- Skipped forecast windows: {coverage['skipped_windows']}",
        f"- Coinbase 1-minute candles loaded: {coverage['candles_loaded']}",
        f"- Candle request failures: {coverage['candle_request_failures']}",
        "",
        "## Prophet Signal",
        "",
        f"- {score(prophet)}",
        f"- P50 settlement-price MAE: {prophet.get('p50_mae_usd', 'n/a')}",
        "",
        "## Walk-Forward ML",
        "",
        f"- {score(ml)}",
        f"- Brier score: {ml.get('brier_score', 'n/a')}",
        f"- Training policy: {ml['lookahead_guard']}",
        "",
        "## Important Limits",
        "",
        "- This is a directional-outcome backtest, not a P&L backtest. It does not invent fills, spread, slippage, or exchange fees.",
        "- BTC candles are Coinbase BTC-USD. Kalshi's settlement source can differ, so this is not an exact replay of the live Yahoo/CF Benchmarks data path.",
        "- A result above 50% directional accuracy is not evidence of profitability until it exceeds the executable Kalshi price and costs out of sample.",
        "",
    ]
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("backtest_output"))
    parser.add_argument(
        "--market-limit", type=int, default=0,
        help="Replay only the latest N eligible markets (0 means all). The closed-market CSV is always complete.",
    )
    parser.add_argument("--history-minutes", type=int, default=500)
    parser.add_argument("--forecast-minutes", type=int, default=17)
    parser.add_argument("--preopen-lead-seconds", type=float, default=120.0)
    parser.add_argument("--prophet-uncertainty-samples", type=int, default=1000)
    parser.add_argument("--min-train-rows", type=int, default=200)
    parser.add_argument("--retrain-every", type=int, default=16)
    parser.add_argument("--metrics-every", type=int, default=100,
                        help="Print running Prophet and ML metrics after this many predictions.")
    parser.add_argument("--request-pause-seconds", type=float, default=0.08)
    parser.add_argument("--markets-only", action="store_true",
                        help="Only export the complete closed-market ledger; do not fetch candles or fit models.")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    configure_logging(args.verbose)
    if args.market_limit < 0 or args.history_minutes < 61 or args.forecast_minutes < 1:
        raise SystemExit("market-limit must be >= 0, history-minutes >= 61, and forecast-minutes >= 1")
    if (args.min_train_rows < 1 or args.retrain_every < 1 or args.metrics_every < 1
            or args.request_pause_seconds < 0):
        raise SystemExit("min-train-rows/retrain-every/metrics-every must be positive and request pause cannot be negative")

    all_markets = fetch_all_closed_markets()
    output_dir: Path = args.output_dir
    write_csv(output_dir / "closed_kxbtc15m_markets.csv", all_markets)
    LOG.info("Wrote complete closed-market ledger: %s", output_dir / "closed_kxbtc15m_markets.csv")

    eligible = eligible_markets(all_markets)
    replay_markets = eligible[-args.market_limit:] if args.market_limit else eligible
    base_summary: dict[str, Any] = {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "strategy": {
            "series_ticker": SERIES_TICKER,
            "history_minutes": args.history_minutes,
            "forecast_minutes": args.forecast_minutes,
            "preopen_lead_seconds": args.preopen_lead_seconds,
            "prophet_uncertainty_samples": args.prophet_uncertainty_samples,
            "data_source": "Coinbase BTC-USD one-minute candles",
        },
        "coverage": {
            "closed_markets_exported": len(all_markets),
            "eligible_markets": len(eligible),
            "replay_markets_requested": len(replay_markets),
        },
        "pnl_note": "Not calculated. Historical result data does not supply the strategy's executable opening fill, spread, slippage, or fees.",
    }
    if args.markets_only:
        base_summary["coverage"].update({
            "prophet_rows": 0, "skipped_windows": 0, "candles_loaded": 0,
            "candle_request_failures": 0,
        })
        base_summary["prophet"] = {"predictions": 0}
        base_summary["machine_learning"] = {"predictions": 0, "lookahead_guard": "Not run."}
        write_json(output_dir / "summary.json", base_summary)
        (output_dir / "summary.md").write_text(summary_markdown(base_summary), encoding="utf-8")
        return 0
    if not replay_markets:
        raise RuntimeError("No eligible KXBTC15M markets were returned by Kalshi")

    first_open = parse_timestamp(replay_markets[0]["open_time"])
    last_open = parse_timestamp(replay_markets[-1]["open_time"])
    assert first_open is not None and last_open is not None
    candle_start = (first_open - pd.Timedelta(seconds=args.preopen_lead_seconds)
                    - pd.Timedelta(minutes=args.history_minutes)).floor("min")
    candle_end = (last_open - pd.Timedelta(seconds=args.preopen_lead_seconds) - MINUTE).floor("min")
    candles, request_failures = fetch_coinbase_candles(
        candle_start, candle_end, args.request_pause_seconds)
    rows, skipped = run_prophet_backtest(
        all_markets, candles, args.history_minutes, args.forecast_minutes,
        args.preopen_lead_seconds, args.prophet_uncertainty_samples, args.metrics_every)
    # The full ledger remains exported, but a requested test limit only evaluates
    # the latest N rows.  This lets CI smoke-test a small period without changing
    # the historical archive output.
    if args.market_limit:
        selected = {market["ticker"] for market in replay_markets}
        rows = [row for row in rows if row["ticker"] in selected]
        skipped = [row for row in skipped if row["ticker"] in selected]
    ml_summary = add_walk_forward_ml(
        rows, args.min_train_rows, args.retrain_every, args.metrics_every)
    prophet_metrics = prophet_summary(rows)
    base_summary["coverage"].update({
        "prophet_rows": len(rows),
        "skipped_windows": len(skipped),
        "candles_loaded": len(candles),
        "candle_request_failures": len(request_failures),
        "candle_range_requested": {
            "start": iso_timestamp(candle_start),
            "end": iso_timestamp(candle_end),
        },
    })
    base_summary["prophet"] = prophet_metrics
    base_summary["machine_learning"] = ml_summary
    base_summary["candle_request_failures"] = request_failures

    write_csv(output_dir / "prophet_ml_backtest_rows.csv", rows)
    write_csv(output_dir / "skipped_markets.csv", skipped)
    write_json(output_dir / "summary.json", base_summary)
    (output_dir / "summary.md").write_text(summary_markdown(base_summary), encoding="utf-8")
    LOG.info("Backtest complete: %d Prophet rows, %d ML rows, %d skipped", len(rows),
             ml_summary.get("predictions", 0), len(skipped))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
