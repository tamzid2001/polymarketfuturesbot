"""Offline tests for the ledger analysis normalization and statistics."""

from __future__ import annotations

import pandas as pd

import kalshi_ledger_analysis as analysis


def test_artifact_normalization_and_streaks() -> None:
    raw = pd.DataFrame({
        "ticker": ["KXBTC15M-A", "KXBTC15M-B", "KXETH15M-C"],
        "market_open": ["2026-01-01T00:00:00Z", "2026-01-01T00:15:00Z", "2026-01-01T00:30:00Z"],
        "actual_outcome": ["yes", "no", "yes"],
        "prophet_side": ["yes", "yes", "yes"],
        "prophet_correct": [1, 0, 1],
        "ml_side": ["yes", "no", "yes"],
        "ml_correct": [1, 1, 1],
        "strike": [100, 101, 102],
        "expiration_value": [101, 100, 103],
        "vol_15m_bps": [10, 11, 12],
    })
    ledger, info = analysis.normalize_artifact(raw, analysis.Path("fixture.csv"), "prophet")
    assert info.pnl_available is False
    assert ledger["result"].tolist() == ["WIN", "LOSS", "WIN"]
    assert ledger["market"].tolist() == ["BTC", "BTC", "ETH"]
    assert ledger["feature_volatility"].tolist() == [10, 11, 12]
    assert analysis.streaks(ledger["win"])["longest_loss"] == 1


def test_real_pnl_performance_and_streak_conditionals() -> None:
    raw = pd.DataFrame({
        "date/time": pd.date_range("2026-01-01", periods=12, freq="15min", tz="UTC"),
        "market": ["BTC"] * 12,
        "side": ["YES"] * 12,
        "profit_loss": [1, -1, -1, -1, 1, 1, -1, 1, 1, -1, 1, 1],
        "result": ["WIN", "LOSS", "LOSS", "LOSS", "WIN", "WIN", "LOSS", "WIN", "WIN", "LOSS", "WIN", "WIN"],
    })
    ledger, info = analysis.normalize_ledger(raw, analysis.Path("fixture.csv"))
    summary = analysis.performance_summary(ledger, 1000)
    assert info.pnl_available is True
    assert summary["monetary_performance"]["available"] is True
    assert summary["total_trades"] == 12
    conditional = analysis.streak_conditionals(ledger)
    assert int(conditional.loc[conditional["after_at_least_losses"].eq(3), "opportunities"].iloc[0]) == 1


def test_prophet_series_keeps_original_trade_numbers_after_rolling_windows() -> None:
    raw = pd.DataFrame({
        "date/time": pd.date_range("2026-01-01", periods=55, freq="15min", tz="UTC"),
        "market": ["BTC"] * 55,
        "side": ["YES"] * 55,
        "result": ["WIN", "LOSS"] * 27 + ["WIN"],
    })
    ledger, _ = analysis.normalize_ledger(raw, analysis.Path("fixture.csv"))
    rolling = analysis.prophet_series(analysis.add_time_series(ledger, 1000))["rolling_50_balance"]
    assert rolling["trade_number"].iloc[0] == 50
    assert rolling["trade_number"].iloc[-1] == 55


if __name__ == "__main__":
    test_artifact_normalization_and_streaks()
    test_real_pnl_performance_and_streak_conditionals()
    test_prophet_series_keeps_original_trade_numbers_after_rolling_windows()
    print("PASS: ledger analysis tests")
