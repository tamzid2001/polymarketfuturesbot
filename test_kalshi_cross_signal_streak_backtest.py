"""Checks for cross-signal ML streak to Prophet outcome alignment."""

from __future__ import annotations

import pandas as pd

import kalshi_cross_signal_streak_backtest as backtest


def test_fourth_market_after_three_ml_losses_scores_prophet() -> None:
    market_open = pd.date_range("2026-01-01", periods=7, freq="15min", tz="UTC")
    raw = pd.DataFrame({
        "market_open": market_open,
        "settlement_ts": market_open + pd.Timedelta(minutes=1),
        "ml_correct": [1, 0, 0, 0, 1, 1, 0],
        "prophet_correct": [0, 1, 0, 1, 1, 0, 1],
        "prophet_side": ["yes"] * 7,
        "ml_side": ["yes"] * 7,
    })
    overlap = backtest.prepare_overlap(raw)
    selected = overlap[overlap["prior_ml_losing_streak"].eq(3)]
    assert selected["overlap_trade_number"].tolist() == [5]
    assert selected["prophet_win"].tolist() == [1]
    summary = backtest.performance(selected, overlap[overlap["prior_ml_losing_streak"].ne(3)])
    assert summary["prophet_win_rate"] == 1.0
    assert summary["longest_winning_streak_in_selected_trades"] == 1


def test_unsettled_ml_outcomes_do_not_select_a_future_market() -> None:
    market_open = pd.date_range("2026-01-01", periods=7, freq="15min", tz="UTC")
    raw = pd.DataFrame({
        "market_open": market_open,
        "settlement_ts": market_open + pd.Timedelta(minutes=45),
        "ml_correct": [1, 0, 0, 0, 1, 1, 0],
        "prophet_correct": [0, 1, 0, 1, 1, 0, 1],
        "prophet_side": ["yes"] * 7,
        "ml_side": ["yes"] * 7,
    })
    overlap = backtest.prepare_overlap(raw)

    assert overlap[overlap["prior_ml_losing_streak"].eq(3)].empty


if __name__ == "__main__":
    test_fourth_market_after_three_ml_losses_scores_prophet()
    test_unsettled_ml_outcomes_do_not_select_a_future_market()
    print("PASS: cross-signal streak backtest tests")
