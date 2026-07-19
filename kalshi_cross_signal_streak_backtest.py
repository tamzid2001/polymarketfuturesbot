"""Test whether an ML loss streak predicts the next overlapping Prophet trade.

The source artifact contains both signals' outcome correctness for the same
historical Kalshi markets.  This backtest uses only ML results from markets
that settled before the current market.  On the fourth market following three
consecutive ML losses, it takes the Prophet signal's normal side and measures
whether that Prophet trade won.  It never changes a direction or places an
order.

``exactly_3_prior_ml_losses`` is the requested fourth-trade test.  The
``at_least_3_prior_ml_losses`` row is a sensitivity check for longer runs.
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy.stats import binomtest, fisher_exact

from kalshi_streak_ml_backtest import configure_logging, read_artifact, write_csv, write_json


LOG = logging.getLogger("kalshi_cross_signal_streak_backtest")


def prior_losing_streak(wins: pd.Series) -> pd.Series:
    values: list[int] = []
    running = 0
    for won in wins.astype(int):
        values.append(running)
        running = 0 if won else running + 1
    return pd.Series(values, index=wins.index, dtype="int64")


def longest_streaks(wins: pd.Series) -> tuple[int, int]:
    current_win = current_loss = longest_win = longest_loss = 0
    for won in wins.astype(bool):
        if won:
            current_win += 1
            current_loss = 0
        else:
            current_loss += 1
            current_win = 0
        longest_win = max(longest_win, current_win)
        longest_loss = max(longest_loss, current_loss)
    return longest_win, longest_loss


def prepare_overlap(raw: pd.DataFrame) -> pd.DataFrame:
    data = raw.copy()
    data["timestamp"] = pd.to_datetime(data["market_open"], utc=True, errors="coerce")
    data["ml_win"] = pd.to_numeric(data["ml_correct"], errors="coerce")
    data["prophet_win"] = pd.to_numeric(data["prophet_correct"], errors="coerce")
    ml_history = data[data["timestamp"].notna() & data["ml_win"].isin([0, 1])].copy()
    ml_history = ml_history.sort_values("timestamp", kind="stable")
    if ml_history["timestamp"].duplicated().any():
        raise ValueError("ML history has duplicate market_open timestamps; cannot safely align signals")
    ml_history["prior_ml_losing_streak"] = prior_losing_streak(ml_history["ml_win"])
    prophet = data[data["timestamp"].notna() & data["prophet_win"].isin([0, 1])].copy()
    prophet = prophet.sort_values("timestamp", kind="stable")
    if prophet["timestamp"].duplicated().any():
        raise ValueError("Prophet history has duplicate market_open timestamps; cannot safely align signals")
    overlap = prophet.merge(
        ml_history[["timestamp", "ml_win", "prior_ml_losing_streak"]],
        on="timestamp", how="inner", validate="one_to_one",
    )
    overlap = overlap.sort_values("timestamp", kind="stable").reset_index(drop=True)
    overlap["overlap_trade_number"] = np.arange(1, len(overlap) + 1)
    overlap["prophet_side"] = overlap["prophet_side"].astype(str).str.upper()
    return overlap


def performance(selected: pd.DataFrame, other: pd.DataFrame) -> dict[str, Any]:
    wins = selected["prophet_win"].astype(int)
    other_wins = other["prophet_win"].astype(int)
    longest_win, longest_loss = longest_streaks(wins)
    pvalue_50 = float(binomtest(int(wins.sum()), len(wins), p=0.5).pvalue) if len(wins) else None
    table = [[int(wins.sum()), int(len(wins) - wins.sum())],
             [int(other_wins.sum()), int(len(other_wins) - other_wins.sum())]]
    fisher_pvalue = float(fisher_exact(table).pvalue) if len(wins) and len(other_wins) else None
    return {
        "selected_trades": int(len(wins)),
        "prophet_wins": int(wins.sum()),
        "prophet_losses": int(len(wins) - wins.sum()),
        "prophet_win_rate": float(wins.mean()) if len(wins) else None,
        "other_prophet_trades": int(len(other_wins)),
        "other_prophet_win_rate": float(other_wins.mean()) if len(other_wins) else None,
        "win_rate_difference_vs_other": float(wins.mean() - other_wins.mean()) if len(wins) and len(other_wins) else None,
        "win_rate_vs_50pct_pvalue": pvalue_50,
        "fisher_pvalue_vs_other_prophet_trades": fisher_pvalue,
        "longest_winning_streak_in_selected_trades": longest_win,
        "longest_losing_streak_in_selected_trades": longest_loss,
    }


def run(raw: pd.DataFrame, output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    overlap = prepare_overlap(raw)
    write_csv(output_dir / "overlapping_prophet_ml_ledger.csv", overlap)
    report: dict[str, Any] = {
        "overlapping_markets": int(len(overlap)),
        "method": (
            "ML prior loss streak is computed before the market opens; Prophet's original YES/NO side is scored "
            "on the same market-open timestamp."
        ),
        "outcome_limit": "The artifact has directional correctness, not executable prices, fills, fees, or P&L.",
        "conditions": {},
    }
    for name, mask in {
        "exactly_3_prior_ml_losses": overlap["prior_ml_losing_streak"].eq(3),
        "at_least_3_prior_ml_losses": overlap["prior_ml_losing_streak"].ge(3),
    }.items():
        selected = overlap[mask].copy()
        selected["selection_condition"] = name
        other = overlap[~mask].copy()
        write_csv(output_dir / f"{name}_prophet_trades.csv", selected)
        report["conditions"][name] = performance(selected, other)
        LOG.info(
            "%s: Prophet %d/%d wins (%.2f%%) after ML loss streak; other overlap %.2f%%",
            name, int(selected["prophet_win"].sum()), len(selected),
            selected["prophet_win"].mean() * 100 if len(selected) else 0,
            other["prophet_win"].mean() * 100 if len(other) else 0,
        )
    write_json(output_dir / "cross_signal_streak_summary.json", report)
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=Path("cross_signal_streak_backtest_output"))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    configure_logging()
    raw, source = read_artifact(args.input)
    report = run(raw, args.output_dir)
    write_json(args.output_dir / "cross_signal_streak_backtest_index.json", {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": str(source),
        "report": report,
    })
    LOG.info("Cross-signal streak backtest complete: %s", args.output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
