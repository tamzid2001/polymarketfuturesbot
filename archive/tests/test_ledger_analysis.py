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


def test_rolling_regime_uses_the_pretrade_balance() -> None:
    raw = pd.DataFrame({
        "date/time": pd.date_range("2026-01-01", periods=51, freq="15min", tz="UTC"),
        "market": ["BTC"] * 51,
        "side": ["YES"] * 51,
        "result": ["WIN"] * 26 + ["LOSS"] * 25,
    })
    ledger, _ = analysis.normalize_ledger(raw, analysis.Path("fixture.csv"))
    series = analysis.add_time_series(ledger, 1000)
    # Before trade 51, the preceding 50 trades have a +2 balance.  Trade 51
    # itself is a loss and brings its own rolling window to zero.
    assert series.loc[50, "rolling_50_balance"] == 0
    assert series.loc[50, "rolling_50_regime"] == "hot"


def test_high_loss_streak_walkforward_uses_future_blocks_only() -> None:
    raw = pd.DataFrame({
        "date/time": pd.date_range("2026-01-01", periods=400, freq="15min", tz="UTC"),
        "market": ["BTC"] * 400,
        "side": ["YES"] * 400,
        "result": (["WIN"] * 5 + ["LOSS"] * 5) * 40,
    })
    ledger, _ = analysis.normalize_ledger(raw, analysis.Path("fixture.csv"))
    details, summary, events, buckets = analysis.loss_streak_percentile_walkforward(ledger, block_size=100)
    assert len(details) == 6  # Three non-overlapping future blocks for P90 and P99.
    assert set(details["percentile"]) == {"P90", "P99"}
    assert set(summary["percentile"]) == {"P90", "P99"}
    assert details["train_trades"].tolist() == [100, 100, 200, 200, 300, 300]
    assert details["selected_trades"].gt(0).all()
    assert not events.empty
    assert not buckets.empty
    assert set(events["percentile"]).issubset({"P90", "P99"})
    assert "P90" in set(events["percentile"])


def test_conclusion_rejects_accuracy_gain_with_worse_probability_quality() -> None:
    performance = {
        "win_rate_vs_50pct_pvalue": 0.9,
        "win_rate": 0.5,
        "monetary_performance": {"available": False},
    }
    models = {
        "available": True,
        "models": {
            "random_forest": {
                "accuracy_improvement": 0.01,
                "brier_improvement": -0.001,
                "mcnemar_exact_pvalue": 0.01,
            },
        },
    }
    result = analysis.conclusion(performance, pd.DataFrame(), models)
    assert result["streak_predictive_value"].startswith("No ML model")


if __name__ == "__main__":
    test_artifact_normalization_and_streaks()
    test_real_pnl_performance_and_streak_conditionals()
    test_prophet_series_keeps_original_trade_numbers_after_rolling_windows()
    test_rolling_regime_uses_the_pretrade_balance()
    test_high_loss_streak_walkforward_uses_future_blocks_only()
    test_conclusion_rejects_accuracy_gain_with_worse_probability_quality()
    print("PASS: ledger analysis tests")
