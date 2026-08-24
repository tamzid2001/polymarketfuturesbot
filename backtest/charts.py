"""Static PNG and optional Plotly reports for the walk-forward result."""

from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from .config import BacktestConfig
from .equity import drawdown_series


def _finish(path: Path, title: str, ylabel: str = "Balance ($)") -> None:
    plt.title(title)
    plt.xlabel("UTC timestamp")
    plt.ylabel(ylabel)
    plt.xticks(rotation=35, ha="right")
    plt.grid(alpha=0.2)
    plt.legend(loc="best")
    plt.tight_layout()
    plt.savefig(path, dpi=220, bbox_inches="tight")
    plt.close()


def _markers(log: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    return log[log["entry_signal_after_trade"]], log[log["exit_signal_after_trade"]]


def create_charts(log: pd.DataFrame, calibration: pd.DataFrame, output_dir: str | Path, config: BacktestConfig) -> None:
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    entry, exit_ = _markers(log)
    x = log["ds"]
    forecast = log[log["p50"].notna()]

    plt.figure(figsize=(14, 7))
    plt.plot(x, log["always_on_equity"], label="Always-on equity", linewidth=1.8)
    plt.plot(x, log["filtered_equity_after"], label="P10/P90 filtered equity", linewidth=2.2, linestyle="--")
    plt.axhline(float(log.iloc[0]["balance_before_first_trade"]), color="black", linestyle=":", label="Opening balance")
    plt.scatter(entry["ds"], entry["filtered_equity_after"], marker="^", s=60, color="green", label="Entry signal")
    plt.scatter(exit_["ds"], exit_["filtered_equity_after"], marker="v", s=60, color="red", label="Exit signal")
    _finish(root / "equity_comparison.png", "Walk-forward equity comparison")

    plt.figure(figsize=(14, 7))
    plt.plot(x, log["shadow_equity_after"], label="Shadow equity", color="black", linewidth=1.8)
    plt.plot(forecast["ds"], forecast["p10"], "--", label="P10")
    plt.plot(forecast["ds"], forecast["p50"], label="P50")
    plt.plot(forecast["ds"], forecast["p90"], ":", label="P90")
    plt.fill_between(forecast["ds"], forecast["p10"], forecast["p90"], alpha=.18, label="P10-P90")
    plt.scatter(entry["ds"], entry["shadow_equity_after"], marker="^", s=60, color="green", label="Entry signal")
    plt.scatter(exit_["ds"], exit_["shadow_equity_after"], marker="v", s=60, color="red", label="Exit signal")
    _finish(root / "shadow_equity_with_bands.png", "One-step-ahead shadow equity forecast bands")

    plt.figure(figsize=(14, 7))
    plt.plot(x, log["shadow_equity_after"], label="Actual shadow equity", color="black", linewidth=1.8)
    plt.fill_between(forecast["ds"], forecast["p01"], forecast["p99"], alpha=.10, label="P01-P99")
    plt.fill_between(forecast["ds"], forecast["p10"], forecast["p90"], alpha=.16, label="P10-P90")
    plt.fill_between(forecast["ds"], forecast["p25"], forecast["p75"], alpha=.24, label="P25-P75")
    plt.plot(forecast["ds"], forecast["p50"], label="P50")
    _finish(root / "all_quantile_bands.png", "All one-step-ahead predictive quantile bands")

    always_dd, _ = drawdown_series(log["always_on_equity"])
    filtered_dd, _ = drawdown_series(log["filtered_equity_after"])
    plt.figure(figsize=(14, 6))
    plt.plot(x, always_dd, label="Always-on drawdown")
    plt.plot(x, filtered_dd, label="Filtered drawdown")
    _finish(root / "drawdown_comparison.png", "Drawdown comparison", "Drawdown from peak ($)")

    selected = log["selected_trade_pnl"].where(log["bot_active_for_trade"])
    rolling_mean = selected.rolling(config.rolling_window, min_periods=max(3, config.rolling_window // 4)).mean()
    rolling_win = (selected > 0).rolling(config.rolling_window, min_periods=max(3, config.rolling_window // 4)).mean()
    rolling_gain = selected.clip(lower=0).rolling(config.rolling_window, min_periods=3).sum()
    rolling_loss = (-selected.clip(upper=0)).rolling(config.rolling_window, min_periods=3).sum()
    rolling_pf = rolling_gain / rolling_loss.replace(0, np.nan)
    fig, axes = plt.subplots(3, 1, figsize=(14, 10), sharex=True)
    axes[0].plot(x, rolling_mean, label="Rolling selected-trade mean")
    axes[1].plot(x, rolling_win, label="Rolling win rate")
    axes[2].plot(x, rolling_pf, label="Rolling profit factor")
    for axis in axes:
        axis.grid(alpha=.2); axis.legend(loc="best")
    axes[2].set_xlabel("UTC timestamp")
    fig.suptitle(f"Rolling performance ({config.rolling_window}-trade window)")
    fig.tight_layout()
    fig.savefig(root / "rolling_performance.png", dpi=220, bbox_inches="tight")
    plt.close(fig)

    plt.figure(figsize=(12, 6))
    bins = min(30, max(8, int(np.sqrt(len(log)))))
    plt.hist(log["trade_pnl"], bins=bins, alpha=.55, label="Always-on opportunities")
    plt.hist(log.loc[log["bot_active_for_trade"], "selected_trade_pnl"], bins=bins, alpha=.55, label="Filtered selected trades")
    _finish(root / "trade_pnl_distribution.png", "Trade P/L distributions", "Trade P/L ($)")

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    axes[0].plot(forecast["ds"], forecast["forecast_error_p50"], label="P50 error")
    axes[0].axhline(0, color="black", linewidth=1)
    axes[0].legend(); axes[0].grid(alpha=.2); axes[0].tick_params(axis="x", rotation=35)
    axes[1].hist(forecast["forecast_error_p50"].dropna(), bins=bins)
    axes[1].set_title("Histogram of P50 errors")
    fig.suptitle("Forecast errors")
    fig.tight_layout()
    fig.savefig(root / "forecast_errors.png", dpi=220, bbox_inches="tight")
    plt.close(fig)

    plt.figure(figsize=(14, 6))
    plt.plot(x, log["shadow_equity_after"], color="black", label="Shadow equity")
    active = log["bot_active_for_trade"].to_numpy(bool)
    start = None
    for i, value in enumerate(active):
        if value and start is None: start = i
        if start is not None and (not value or i == len(active) - 1):
            end = i if not value else i + 1
            plt.axvspan(x.iloc[start], x.iloc[end - 1], color="green", alpha=.16, label="Bot active" if start == np.flatnonzero(active)[0] else None)
            start = None
    _finish(root / "regime_windows.png", "Regime windows: shaded periods are active")

    plt.figure(figsize=(14, 6))
    plt.plot(x, log["skipped_trade_pnl"].cumsum(), label="Cumulative skipped-trade P/L")
    plt.axhline(0, color="black", linewidth=1)
    _finish(root / "cumulative_skipped_pnl.png", "Cumulative P/L avoided by the filter", "Cumulative skipped P/L ($)")

    overall = calibration[calibration.get("group_type", pd.Series(dtype=str)) == "overall"] if not calibration.empty else pd.DataFrame()
    observed = overall.iloc[0] if not overall.empty else pd.Series(dtype=float)
    expected = np.array([.98, .80, .50])
    actual = np.array([
        observed.get("p01_p99_coverage", np.nan),
        observed.get("p10_p90_coverage", np.nan),
        observed.get("p25_p75_coverage", np.nan),
    ])
    plt.figure(figsize=(7, 6))
    plt.plot([0, 1], [0, 1], "k--", label="Perfect calibration")
    plt.scatter(expected, actual, s=70, label="Observed coverage")
    for exp, obs, label in zip(expected, actual, ["P01-P99", "P10-P90", "P25-P75"], strict=True):
        plt.annotate(label, (exp, obs))
    plt.xlim(0, 1); plt.ylim(0, 1); plt.xlabel("Expected coverage"); plt.ylabel("Observed coverage")
    plt.title("Forecast interval calibration"); plt.grid(alpha=.2); plt.legend(); plt.tight_layout()
    plt.savefig(root / "calibration_chart.png", dpi=220, bbox_inches="tight")
    plt.close()

    try:
        import plotly.graph_objects as go

        fig = go.Figure()
        fig.add_scatter(x=x, y=log["shadow_equity_after"], name="Shadow equity")
        fig.add_scatter(x=forecast["ds"], y=forecast["p90"], line={"width": 0}, showlegend=False)
        fig.add_scatter(x=forecast["ds"], y=forecast["p10"], fill="tonexty", fillcolor="rgba(70,130,180,.18)", line={"width": 0}, name="P10-P90")
        fig.add_scatter(x=x, y=log["filtered_equity_after"], name="Filtered equity")
        fig.update_layout(title="Kalshi walk-forward equity backtest", template="plotly_white", hovermode="x unified")
        fig.write_html(root / "walk_forward_interactive.html", include_plotlyjs="cdn")
    except ImportError:
        pass
