"""Focused checks for the counter-trend, three-loss simulation."""

from __future__ import annotations

import numpy as np
import pandas as pd

import kalshi_contrarian_streak_backtest as backtest
from kalshi_streak_ml_backtest import prepare_signal_data


def raw_frame() -> pd.DataFrame:
    return pd.DataFrame({
        "market_open": pd.date_range("2026-01-01", periods=10, freq="15min", tz="UTC"),
        "prophet_correct": [1, 0, 0, 0, 1, 0, 1, 1, 0, 0],
        "ml_correct": [1] * 10,
        "prophet_side": ["yes"] * 10,
        "ml_side": ["yes"] * 10,
        "source": ["historical"] * 10,
    })


def test_three_loss_campaign_inverts_exactly_three_non_overlapping_rows() -> None:
    prepared = prepare_signal_data(raw_frame(), "prophet")
    probabilities = pd.DataFrame({
        "trade_number": prepared.frame["trade_number"],
        "probability_next_3_all_loss": np.nan,
        "probability_next_3_all_win": np.nan,
    })
    result = backtest.simulate_policy(prepared, probabilities, "always_after_3_losses", 0.5)
    assert len(result.campaigns) == 1
    assert result.campaigns.loc[0, "trigger_trade_number"] == 5
    changed = result.ledger[result.ledger["trade_side_changed"]]
    assert changed["trade_number"].tolist() == [5, 6, 7]
    assert changed["strategy_win"].tolist() == [0, 1, 0]
    assert result.performance["campaigns_with_at_least_one_opposite_win"] == 1


def test_probability_gate_requires_out_of_sample_50_percent_loss_call() -> None:
    prepared = prepare_signal_data(raw_frame(), "prophet")
    probabilities = pd.DataFrame({
        "trade_number": prepared.frame["trade_number"],
        "probability_next_3_all_loss": [np.nan, np.nan, np.nan, np.nan, 0.51, np.nan, np.nan, np.nan, np.nan, np.nan],
        "probability_next_3_all_win": [np.nan, np.nan, np.nan, np.nan, 0.11, np.nan, np.nan, np.nan, np.nan, np.nan],
    })
    result = backtest.simulate_policy(prepared, probabilities, "calibrated_50pct_gate", 0.5)
    assert len(result.campaigns) == 1
    assert result.campaigns.loc[0, "decision_reason"] == "all_original_losses_probability"


if __name__ == "__main__":
    test_three_loss_campaign_inverts_exactly_three_non_overlapping_rows()
    test_probability_gate_requires_out_of_sample_50_percent_loss_call()
    print("PASS: contrarian streak backtest tests")
