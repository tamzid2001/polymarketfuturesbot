"""Chronological counter-trend backtest after three original signal losses.

This script consumes the outcome-only ``prophet_ml_backtest_rows.csv`` artifact.
It is intentionally an outcome-direction test, not a P&L simulation: an
opposite YES/NO trade wins exactly when the original recorded signal loses.

For each of the Prophet and ML signals it reports two policies:

* ``always_after_3_losses``: invert the next three contracts after every
  non-overlapping three-loss trigger in the original signal history.
* ``calibrated_50pct_gate``: do so only when a strictly out-of-sample,
  calibrated model assigns at least 50% probability that the current and next
  two *original* signals will all lose.  A 50% all-win forecast explicitly
  keeps the original side.  No prediction means no inversion.

The original signal history, rather than counterfactual outcomes, determines
triggers.  That is the information a live implementation would have before it
opens the first contract in a three-trade campaign.  Campaigns never overlap.
"""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from kalshi_streak_ml_backtest import (
    PreparedData,
    chronological_splits,
    configure_logging,
    prepare_signal_data,
    read_artifact,
    write_csv,
    write_json,
)


LOG = logging.getLogger("kalshi_contrarian_streak_backtest")
POLICIES = ("always_after_3_losses", "calibrated_50pct_gate")


@dataclass(frozen=True)
class CampaignResult:
    ledger: pd.DataFrame
    campaigns: pd.DataFrame
    performance: dict[str, Any]


def make_preprocessor(prepared: PreparedData) -> ColumnTransformer:
    return ColumnTransformer([
        ("numeric", Pipeline([
            ("imputer", SimpleImputer(strategy="median")),
            ("scale", StandardScaler()),
        ]), prepared.numeric_features),
        ("categorical", Pipeline([
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("onehot", OneHotEncoder(handle_unknown="ignore")),
        ]), prepared.categorical_features),
    ])


def platt_scale(raw_probability: np.ndarray, actual: np.ndarray,
                test_probability: np.ndarray, fallback: float) -> np.ndarray:
    """Calibrate a model using a later historical holdout without test leakage."""
    if len(np.unique(actual)) < 2 or np.unique(raw_probability).size < 2:
        return np.full(len(test_probability), fallback, dtype=float)
    calibrator = LogisticRegression(C=1_000_000, max_iter=1000, random_state=0)
    calibrator.fit(raw_probability.reshape(-1, 1), actual)
    return calibrator.predict_proba(test_probability.reshape(-1, 1))[:, 1]


def chronological_probabilities(prepared: PreparedData, min_train: int,
                                test_blocks: int) -> pd.DataFrame:
    """Return calibrated, later-only probabilities of all-win/all-loss windows."""
    data = prepared.frame.dropna(subset=["next_3_all_win", "next_3_all_loss"]).reset_index(drop=True)
    targets = ("next_3_all_win", "next_3_all_loss")
    result = data[["trade_number", "timestamp", "side", "prior_losing_streak"]].copy()
    for target in targets:
        result[f"probability_{target}"] = np.nan
        result[f"baseline_{target}"] = np.nan
    splits = chronological_splits(len(data), min_train, test_blocks)
    if not splits:
        return result
    features = data[prepared.numeric_features + prepared.categorical_features]
    for block, (train_end, test_end) in enumerate(splits, start=1):
        # Reserve the most recent 20% of the past for calibration.  It remains
        # strictly before this block's unseen test rows.
        calibration_rows = max(250, int(train_end * 0.20))
        model_end = train_end - calibration_rows
        if model_end < 200:
            continue
        LOG.info("calibrated probability block %d/%d: fit=%d calibrate=%d test=%d",
                 block, len(splits), model_end, calibration_rows, test_end - train_end)
        for target in targets:
            labels = data[target].astype(int).to_numpy()
            model = Pipeline([
                ("preprocess", make_preprocessor(prepared)),
                ("model", LogisticRegression(max_iter=2000, random_state=0)),
            ])
            model.fit(features.iloc[:model_end], labels[:model_end])
            calibration_probability = model.predict_proba(features.iloc[model_end:train_end])[:, 1]
            test_probability = model.predict_proba(features.iloc[train_end:test_end])[:, 1]
            baseline = float(np.mean(labels[:train_end]))
            calibrated = platt_scale(
                calibration_probability, labels[model_end:train_end], test_probability, baseline)
            result.loc[train_end:test_end - 1, f"probability_{target}"] = calibrated
            result.loc[train_end:test_end - 1, f"baseline_{target}"] = baseline
    return result


def streaks(wins: pd.Series) -> tuple[int, int]:
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


def performance_summary(ledger: pd.DataFrame, campaigns: pd.DataFrame) -> dict[str, Any]:
    wins = ledger["strategy_win"].astype(int)
    longest_win, longest_loss = streaks(wins)
    counter_rows = ledger[ledger["trade_side_changed"]]
    return {
        "all_trades": int(len(ledger)),
        "wins": int(wins.sum()),
        "losses": int(len(wins) - wins.sum()),
        "win_rate": float(wins.mean()) if len(wins) else None,
        "longest_winning_streak": longest_win,
        "longest_losing_streak": longest_loss,
        "opposite_contract_trades": int(len(counter_rows)),
        "opposite_contract_wins": int(counter_rows["strategy_win"].sum()),
        "opposite_contract_losses": int(len(counter_rows) - counter_rows["strategy_win"].sum()),
        "opposite_contract_win_rate": float(counter_rows["strategy_win"].mean()) if len(counter_rows) else None,
        "campaigns": int(len(campaigns)),
        "campaigns_with_at_least_one_opposite_win": (
            int(campaigns["has_opposite_win"].sum()) if len(campaigns) else 0
        ),
        "campaign_at_least_one_opposite_win_rate": (
            float(campaigns["has_opposite_win"].mean()) if len(campaigns) else None
        ),
        "campaigns_all_three_opposite_win": (
            int(campaigns["all_three_opposite_win"].sum()) if len(campaigns) else 0
        ),
    }


def opposite_side(side: str) -> str:
    return "NO" if str(side).upper() == "YES" else "YES"


def should_invert(policy: str, probability_loss: float, probability_win: float,
                  threshold: float) -> tuple[bool, str]:
    if policy == "always_after_3_losses":
        return True, "three_original_losses"
    if not np.isfinite(probability_loss) or not np.isfinite(probability_win):
        return False, "no_out_of_sample_probability"
    if probability_loss >= threshold and probability_loss >= probability_win:
        return True, "all_original_losses_probability"
    if probability_win >= threshold and probability_win > probability_loss:
        return False, "all_original_wins_probability_keep_original"
    return False, "below_50pct_probability_threshold"


def simulate_policy(prepared: PreparedData, probabilities: pd.DataFrame, policy: str,
                    threshold: float) -> CampaignResult:
    data = prepared.frame.copy().reset_index(drop=True)
    probability_by_trade = probabilities.set_index("trade_number")
    ledger = data[["trade_number", "timestamp", "side", "trade_win", "prior_losing_streak"]].copy()
    ledger["original_win"] = ledger.pop("trade_win").astype(int)
    ledger["strategy_side"] = ledger["side"].astype(str).str.upper()
    ledger["strategy_win"] = ledger["original_win"]
    ledger["trade_side_changed"] = False
    ledger["campaign_number"] = np.nan
    ledger["campaign_reason"] = "original_signal"
    records: list[dict[str, Any]] = []
    campaign_number = 0
    index = 0
    while index + 2 < len(ledger):
        row = ledger.iloc[index]
        if int(row["prior_losing_streak"]) < 3:
            index += 1
            continue
        probability_row = probability_by_trade.reindex([int(row["trade_number"])])
        probability_loss = float(probability_row["probability_next_3_all_loss"].iloc[0])
        probability_win = float(probability_row["probability_next_3_all_win"].iloc[0])
        invert, reason = should_invert(policy, probability_loss, probability_win, threshold)
        if not invert:
            index += 1
            continue
        campaign_number += 1
        campaign_indexes = list(range(index, index + 3))
        ledger.loc[campaign_indexes, "strategy_side"] = ledger.loc[campaign_indexes, "side"].map(opposite_side)
        ledger.loc[campaign_indexes, "strategy_win"] = 1 - ledger.loc[campaign_indexes, "original_win"]
        ledger.loc[campaign_indexes, "trade_side_changed"] = True
        ledger.loc[campaign_indexes, "campaign_number"] = campaign_number
        ledger.loc[campaign_indexes, "campaign_reason"] = reason
        counter_wins = ledger.loc[campaign_indexes, "strategy_win"].astype(int)
        records.append({
            "campaign_number": campaign_number,
            "trigger_trade_number": int(row["trade_number"]),
            "trigger_timestamp": row["timestamp"],
            "trigger_original_losing_streak": int(row["prior_losing_streak"]),
            "probability_next_3_all_original_losses": probability_loss,
            "probability_next_3_all_original_wins": probability_win,
            "decision_reason": reason,
            "opposite_trade_numbers": ",".join(str(int(ledger.loc[item, "trade_number"])) for item in campaign_indexes),
            "opposite_wins": int(counter_wins.sum()),
            "opposite_losses": int(3 - counter_wins.sum()),
            "has_opposite_win": bool(counter_wins.any()),
            "all_three_opposite_win": bool(counter_wins.all()),
        })
        # A live strategy cannot initiate a second three-trade campaign while
        # the first campaign is still open, so campaigns never overlap.
        index += 3
    campaigns = pd.DataFrame(records)
    return CampaignResult(ledger, campaigns, performance_summary(ledger, campaigns))


def run_signal(raw: pd.DataFrame, signal: str, output_dir: Path, min_train: int,
               test_blocks: int, threshold: float) -> dict[str, Any]:
    prepared = prepare_signal_data(raw, signal)
    probabilities = chronological_probabilities(prepared, min_train, test_blocks)
    write_csv(output_dir / "calibrated_next_three_probabilities.csv", probabilities)
    report: dict[str, Any] = {
        "signal": signal,
        "trades": int(len(prepared.frame)),
        "trigger": "At least three consecutive losses in the original signal immediately before a trade.",
        "campaign": "Invert the current and next two contracts; campaigns do not overlap.",
        "probability_gate": (
            f"Calibrated out-of-sample P(current and next two original signals all lose) >= {threshold:.0%}; "
            "an all-win probability >= threshold preserves the original side."
        ),
        "outcome_limit": "Direction correctness only. Entry prices, fees, fills, and P&L are not in the source artifact.",
        "policies": {},
    }
    performances: list[dict[str, Any]] = []
    for policy in POLICIES:
        result = simulate_policy(prepared, probabilities, policy, threshold)
        write_csv(output_dir / f"{policy}_full_hybrid_ledger.csv", result.ledger)
        write_csv(output_dir / f"{policy}_campaigns.csv", result.campaigns)
        report["policies"][policy] = result.performance
        performances.append({"policy": policy, **result.performance})
    write_csv(output_dir / "contrarian_performance_summary.csv", pd.DataFrame(performances))
    write_json(output_dir / "contrarian_summary.json", report)
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=Path("contrarian_streak_backtest_output"))
    parser.add_argument("--signal", choices=("all", "prophet", "ml"), default="all")
    parser.add_argument("--min-train-trades", type=int, default=2000)
    parser.add_argument("--test-blocks", type=int, default=5)
    parser.add_argument("--probability-threshold", type=float, default=0.50)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.min_train_trades < 200 or args.test_blocks < 2 or not 0 < args.probability_threshold < 1:
        raise SystemExit("min-train-trades must be >= 200; test-blocks >= 2; probability-threshold must be in (0, 1)")
    configure_logging()
    raw, source = read_artifact(args.input)
    signals = ("prophet", "ml") if args.signal == "all" else (args.signal,)
    reports = {
        signal: run_signal(raw, signal, args.output_dir / signal, args.min_train_trades,
                           args.test_blocks, args.probability_threshold)
        for signal in signals
    }
    write_json(args.output_dir / "contrarian_streak_backtest_index.json", {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": str(source),
        "reports": reports,
    })
    LOG.info("Contrarian streak backtest complete: %s", args.output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
