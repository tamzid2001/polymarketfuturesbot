"""Offline unit checks for the historical KXBTC15M backtest helpers."""

from __future__ import annotations

from datetime import timedelta

import numpy as np
import pandas as pd

import kalshi_btc15m_backtest as backtest


def market(ticker: str, open_at: pd.Timestamp, result: str) -> dict:
    return {
        "ticker": ticker,
        "result": result,
        "open_time": backtest.iso_timestamp(open_at),
        "settlement_ts": backtest.iso_timestamp(open_at + pd.Timedelta(minutes=15, seconds=1)),
        "floor_strike": 100.0,
        "expiration_value": 101.0 if result == "yes" else 99.0,
        "source": "test",
    }


def test_known_outcomes_exclude_unsettled_prior_market() -> None:
    start = pd.Timestamp("2026-01-01T00:00:00Z")
    markets = [
        market("KXBTC15M-A", start, "yes"),
        market("KXBTC15M-B", start + pd.Timedelta(minutes=15), "no"),
        market("KXBTC15M-C", start + pd.Timedelta(minutes=30), "yes"),
    ]
    forecast_at = start + pd.Timedelta(minutes=28)
    assert backtest.known_outcomes_before(markets, forecast_at) == [1]


def test_exact_candle_window_rejects_gaps() -> None:
    end = pd.Timestamp("2026-01-01T12:00:00Z")
    timestamps = pd.date_range(end=end, periods=500, freq="min", tz="UTC")
    candles = pd.DataFrame({"ds": timestamps, "close": np.linspace(100.0, 110.0, 500)})
    valid, reason = backtest.exact_candle_window(candles, end, 500)
    assert reason == "ok"
    assert valid is not None and len(valid) == 500
    gapped = candles.drop(index=250).reset_index(drop=True)
    missing, reason = backtest.exact_candle_window(gapped, end, 500)
    assert missing is None
    assert reason.startswith("expected_500_candles_found_")


def test_metrics_and_feature_vector_are_stable() -> None:
    metrics = backtest.binary_metrics([1, 0, 1, 0], [1, 0, 0, 0], [0.8, 0.2, 0.4, 0.1])
    assert metrics["correct"] == 3
    assert metrics["predictions"] == 4
    assert 0.0 <= metrics["brier_score"] <= 1.0

    end = pd.Timestamp("2026-01-01T12:00:00Z")
    window = pd.DataFrame({
        "ds": pd.date_range(end=end, periods=500, freq="min", tz="UTC"),
        "close": np.linspace(100.0, 110.0, 500),
    })
    values = backtest.feature_values(
        window, 105.0, {"p10": 104.0, "p50": 106.0, "p90": 108.0},
        [1, 0, 1, 1], end + timedelta(minutes=3),
    )
    assert set(backtest.FEATURE_COLUMNS) == set(values)
    assert all(np.isfinite(float(values[column])) for column in backtest.FEATURE_COLUMNS)


if __name__ == "__main__":
    test_known_outcomes_exclude_unsettled_prior_market()
    test_exact_candle_window_rejects_gaps()
    test_metrics_and_feature_vector_are_stable()
    print("PASS: kalshi historical backtest helper tests")
