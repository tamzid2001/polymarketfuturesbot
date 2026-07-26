"""Equity accounting and performance measures used by every strategy."""

from __future__ import annotations

from collections.abc import Iterable

import numpy as np
import pandas as pd


def drawdown_series(equity: pd.Series) -> tuple[pd.Series, pd.Series]:
    peak = equity.cummax()
    dollars = equity - peak
    percent = dollars / peak.replace(0, np.nan)
    return dollars, percent


def longest_streak(values: Iterable[float], positive: bool) -> int:
    longest = current = 0
    for value in values:
        match = value > 0 if positive else value < 0
        if match:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest


def profit_factor(pnl: pd.Series) -> float:
    gain = float(pnl[pnl > 0].sum())
    loss = float(-pnl[pnl < 0].sum())
    if loss == 0:
        return float("inf") if gain > 0 else np.nan
    return gain / loss


def downside_deviation(pnl: pd.Series) -> float:
    if pnl.empty:
        return np.nan
    return float(np.sqrt(np.mean(np.minimum(pnl.to_numpy(dtype=float), 0.0) ** 2)))


def performance_summary(
    frame: pd.DataFrame,
    strategy: str,
    pnl_column: str,
    active_column: str,
    starting_balance: float,
    entry_column: str | None = None,
    exit_column: str | None = None,
    completed_windows: int = 0,
    final_regime_open: bool = False,
    exploratory: bool = False,
) -> dict[str, object]:
    """Calculate the requested opportunity-level statistics on ``frame``.

    The caller passes only true walk-forward evaluation rows.  The evaluation
    path is anchored at the *actual absolute balance immediately before the
    first evaluation trade*; it is not normalized or rebased to $100.
    """

    pnl_all = frame[pnl_column].astype(float)
    active = frame[active_column].astype(bool)
    selected = pnl_all[active]
    wins = selected[selected > 0]
    losses = selected[selected < 0]
    oos_equity = starting_balance + pnl_all.cumsum()
    dd_dollars, dd_percent = drawdown_series(oos_equity)
    std = float(selected.std(ddof=1)) if len(selected) > 1 else np.nan
    mean = float(selected.mean()) if len(selected) else np.nan
    down_dev = downside_deviation(selected)
    max_dd = float(-dd_dollars.min()) if len(dd_dollars) else np.nan
    exposure = float(active.mean()) if len(active) else np.nan
    net = float(pnl_all.sum())
    return {
        "strategy": strategy,
        "evaluation_scope": "true_walk_forward_after_initial_training",
        "evaluation_opening_balance": float(starting_balance),
        "exploratory": exploratory,
        "total_opportunities": int(len(frame)),
        "trades_taken": int(active.sum()),
        "trades_skipped": int((~active).sum()),
        "time_in_market_pct": exposure,
        "wins": int(len(wins)),
        "losses": int(len(losses)),
        "win_rate": float(len(wins) / len(selected)) if len(selected) else np.nan,
        "gross_profit": float(wins.sum()),
        "gross_loss": float(-losses.sum()),
        "net_pnl": net,
        "ending_balance": float(oos_equity.iloc[-1]) if len(oos_equity) else starting_balance,
        "average_pnl_per_selected_trade": mean,
        "average_pnl_per_total_opportunity": float(pnl_all.mean()) if len(pnl_all) else np.nan,
        "median_pnl": float(selected.median()) if len(selected) else np.nan,
        "standard_deviation": std,
        "downside_deviation": down_dev,
        "profit_factor": profit_factor(selected),
        "maximum_drawdown_dollars": max_dd,
        "maximum_drawdown_pct": float(-dd_percent.min()) if len(dd_percent) else np.nan,
        "longest_win_streak": longest_streak(selected, positive=True),
        "longest_loss_streak": longest_streak(selected, positive=False),
        "sharpe_like_per_trade": mean / std if std and np.isfinite(std) and std > 0 else np.nan,
        "sortino_like_per_trade": mean / down_dev if down_dev and np.isfinite(down_dev) and down_dev > 0 else np.nan,
        "recovery_factor": net / max_dd if max_dd and np.isfinite(max_dd) and max_dd > 0 else np.nan,
        "exposure_adjusted_return": net / exposure if exposure and np.isfinite(exposure) and exposure > 0 else np.nan,
        "pnl_of_skipped_trades": float(frame.loc[~active, "trade_pnl"].sum()),
        "number_of_entry_signals": int(frame[entry_column].sum()) if entry_column else 0,
        "number_of_exit_signals": int(frame[exit_column].sum()) if exit_column else 0,
        "number_of_completed_regime_windows": int(completed_windows),
        "final_regime_remains_open": bool(final_regime_open),
    }
