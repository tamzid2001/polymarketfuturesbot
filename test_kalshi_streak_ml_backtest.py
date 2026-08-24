"""Focused offline checks for the streak ML backtest feature and split logic."""

from __future__ import annotations

import pandas as pd

import kalshi_streak_ml_backtest as backtest


def test_prepare_signal_uses_prior_results_and_future_labels() -> None:
    raw = pd.DataFrame({
        "market_open": pd.date_range("2026-01-01", periods=12, freq="15min", tz="UTC"),
        "prophet_correct": [1, 0, 0, 1, 1, 1, 0, 0, 0, 1, 1, 0],
        "ml_correct": [1] * 12,
        "prophet_side": ["yes"] * 12,
        "ml_side": ["yes"] * 12,
        "return_1m_bps": list(range(12)),
        "source": ["historical"] * 12,
    })
    prepared = backtest.prepare_signal_data(raw, "prophet")
    data = prepared.frame
    assert data.loc[0, "prior_losing_streak"] == 0
    assert data.loc[2, "prior_losing_streak"] == 1
    assert data.loc[1, "next_3_all_loss"] == 0
    assert data.loc[6, "next_3_all_loss"] == 1
    assert "prophet_correct" not in prepared.numeric_features


def test_chronological_splits_are_strictly_future() -> None:
    splits = backtest.chronological_splits(1000, min_train=400, test_blocks=3)
    assert len(splits) == 3
    assert splits[0][0] >= 400
    assert all(train_end < test_end for train_end, test_end in splits)
    assert all(splits[index][1] <= splits[index + 1][0] for index in range(len(splits) - 1))


if __name__ == "__main__":
    test_prepare_signal_uses_prior_results_and_future_labels()
    test_chronological_splits_are_strictly_future()
    print("PASS: streak ML backtest tests")
