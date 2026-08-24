"""Regime-window reconstruction from next-trade state transitions."""

from __future__ import annotations

import numpy as np
import pandas as pd

from .equity import drawdown_series, profit_factor


def _forecast_percentile(row: pd.Series) -> float:
    """Piecewise-linear percentile of actual equity within forecast quantiles."""

    values = row[["p01", "p10", "p25", "p50", "p75", "p90", "p99"]].to_numpy(float)
    if not np.isfinite(values).all():
        return np.nan
    probabilities = np.array([0.01, 0.10, 0.25, 0.50, 0.75, 0.90, 0.99])
    return float(np.interp(float(row["shadow_equity_after"]), values, probabilities, left=0.0, right=1.0))


def regime_windows(log: pd.DataFrame) -> pd.DataFrame:
    """Summarize each period opened by an entry signal and closed by an exit."""

    rows: list[dict[str, object]] = []
    entry_position: int | None = None
    for position, (_, row) in enumerate(log.iterrows()):
        if bool(row["entry_signal_after_trade"]):
            entry_position = position
        if bool(row["exit_signal_after_trade"]) and entry_position is not None:
            rows.append(_window_record(log, entry_position, position, "closed"))
            entry_position = None
    if entry_position is not None:
        rows.append(_window_record(log, entry_position, len(log) - 1, "open_at_end"))
    result = pd.DataFrame(rows)
    if not result.empty:
        result.insert(0, "window_number", np.arange(1, len(result) + 1))
    return result


def _window_record(log: pd.DataFrame, entry_position: int, end_position: int, status: str) -> dict[str, object]:
    # The entry signal is after the entry row.  Its first possible selected
    # trade is therefore exactly the following row.
    selected = log.iloc[entry_position + 1 : end_position + 1].copy()
    selected = selected[selected["bot_active_for_trade"]]
    entry_row = log.iloc[entry_position]
    end_row = log.iloc[end_position]
    if selected.empty:
        return {
            "entry_signal_timestamp": entry_row["ds"],
            "first_traded_timestamp": pd.NaT,
            "exit_signal_timestamp": end_row["ds"] if status == "closed" else pd.NaT,
            "last_traded_timestamp": pd.NaT,
            "status": status,
            "number_of_selected_trades": 0,
            "wins": 0, "losses": 0, "win_rate": np.nan,
            "gross_profit": 0.0, "gross_loss": 0.0, "net_pnl": 0.0,
            "average_pnl": np.nan, "profit_factor": np.nan,
            "maximum_window_drawdown": 0.0,
            "starting_filtered_balance": float(entry_row["filtered_equity_after"]),
            "ending_filtered_balance": float(entry_row["filtered_equity_after"]),
            "duration": pd.Timedelta(0),
            "lowest_shadow_equity_percentile_reached": np.nan,
            "highest_shadow_equity_percentile_reached": np.nan,
        }
    pnl = selected["trade_pnl"].astype(float)
    local_equity = float(selected["filtered_equity_before"].iloc[0]) + pnl.cumsum()
    dd, _ = drawdown_series(local_equity)
    percentiles = selected.apply(_forecast_percentile, axis=1)
    return {
        "entry_signal_timestamp": entry_row["ds"],
        "first_traded_timestamp": selected["ds"].iloc[0],
        "exit_signal_timestamp": end_row["ds"] if status == "closed" else pd.NaT,
        "last_traded_timestamp": selected["ds"].iloc[-1],
        "status": status,
        "number_of_selected_trades": int(len(selected)),
        "wins": int((pnl > 0).sum()),
        "losses": int((pnl < 0).sum()),
        "win_rate": float((pnl > 0).mean()),
        "gross_profit": float(pnl[pnl > 0].sum()),
        "gross_loss": float(-pnl[pnl < 0].sum()),
        "net_pnl": float(pnl.sum()),
        "average_pnl": float(pnl.mean()),
        "profit_factor": profit_factor(pnl),
        "maximum_window_drawdown": float(-dd.min()),
        "starting_filtered_balance": float(selected["filtered_equity_before"].iloc[0]),
        "ending_filtered_balance": float(selected["filtered_equity_after"].iloc[-1]),
        "duration": selected["ds"].iloc[-1] - selected["ds"].iloc[0],
        "lowest_shadow_equity_percentile_reached": float(percentiles.min()),
        "highest_shadow_equity_percentile_reached": float(percentiles.max()),
    }
