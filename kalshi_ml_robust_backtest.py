"""Leakage-resistant model research for KXBTC15M ML inference.

This program evaluates a deliberately small, pre-defined set of tabular
classifiers using four chronological blocks:

1. base-model training,
2. probability calibration,
3. model and confidence-gate selection, and
4. a final untouched test.

Every preceding block is restricted to outcomes settled before the following
block begins.  That is a stricter guard than simply sorting by forecast time:
the label of a 15-minute contract is not usable until Kalshi has settled it.

The deep candidate is a compact, two-hidden-layer MLP.  It is appropriate for
the row-based feature ledger available here; an LSTM/DeepLOB needs a sequence
of order-book snapshots, which this historical artifact does not contain.

Results are directional correctness only.  The input does not contain the
historical executable Kalshi asks, order-book depth, fee, or fill data needed
for an execution-price/P&L backtest.
"""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from itertools import groupby
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd
from scipy.stats import binomtest
from sklearn.ensemble import ExtraTreesClassifier, HistGradientBoostingClassifier
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss
from sklearn.neural_network import MLPClassifier
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from kalshi_ml_features import FEATURE_SCHEMA, ML_ONLY_FEATURE_COLUMNS


MINIMUM_ROWS = 1_000


@dataclass(frozen=True)
class Candidate:
    """A base classifier plus its probability-calibration method."""

    name: str
    calibration: str
    factory: Callable[[], Any]


def load_rows(path: Path) -> pd.DataFrame:
    """Read feature-complete historical rows in prediction-time order."""
    rows = pd.read_csv(path)
    required = set(ML_ONLY_FEATURE_COLUMNS) | {"actual_yes", "forecast_at", "settlement_ts"}
    missing = required - set(rows.columns)
    if missing:
        raise ValueError(f"Input is missing required columns: {', '.join(sorted(missing))}")
    rows = rows.copy()
    rows["forecast_timestamp"] = pd.to_datetime(rows["forecast_at"], utc=True, errors="coerce")
    rows["settlement_timestamp"] = pd.to_datetime(rows["settlement_ts"], utc=True, errors="coerce")
    rows["actual_yes"] = pd.to_numeric(rows["actual_yes"], errors="coerce")
    for name in ML_ONLY_FEATURE_COLUMNS:
        rows[name] = pd.to_numeric(rows[name], errors="coerce")
    rows = rows[
        rows["forecast_timestamp"].notna()
        & rows["settlement_timestamp"].notna()
        & rows["actual_yes"].isin([0, 1])
        & rows[ML_ONLY_FEATURE_COLUMNS].notna().all(axis=1)
    ].sort_values("forecast_timestamp", kind="stable").reset_index(drop=True)
    if len(rows) < MINIMUM_ROWS:
        raise ValueError(f"At least {MINIMUM_ROWS:,} usable rows are required")
    return rows


def chronological_blocks(
    rows: pd.DataFrame,
    calibration_fraction: float,
    selection_fraction: float,
    test_fraction: float,
) -> tuple[dict[str, pd.DataFrame], dict[str, str]]:
    """Split rows into four time blocks and embargo labels across boundaries."""
    total = len(rows)
    train_end = int(total * (1.0 - calibration_fraction - selection_fraction - test_fraction))
    calibration_end = int(total * (1.0 - selection_fraction - test_fraction))
    selection_end = int(total * (1.0 - test_fraction))
    if train_end < MINIMUM_ROWS:
        raise ValueError("The requested splits leave fewer than 1,000 base-training rows")

    calibration_start = rows.loc[train_end, "forecast_timestamp"]
    selection_start = rows.loc[calibration_end, "forecast_timestamp"]
    test_start = rows.loc[selection_end, "forecast_timestamp"]
    train = rows.iloc[:train_end]
    calibration = rows.iloc[train_end:calibration_end]
    selection = rows.iloc[calibration_end:selection_end]
    test = rows.iloc[selection_end:]

    # At each next-block boundary, throw away labels that were not available yet.
    train = train[train["settlement_timestamp"] < calibration_start].copy()
    calibration = calibration[calibration["settlement_timestamp"] < selection_start].copy()
    selection = selection[selection["settlement_timestamp"] < test_start].copy()
    blocks = {
        "train": train,
        "calibration": calibration,
        "selection": selection,
        "test": test.copy(),
    }
    if min(len(block) for block in blocks.values()) < 100:
        raise ValueError("Chronological blocks are too small after the settlement embargo")
    return blocks, {
        "train_end_before": calibration_start.isoformat(),
        "calibration_end_before": selection_start.isoformat(),
        "selection_end_before": test_start.isoformat(),
        "untouched_test_start": test_start.isoformat(),
    }


def base_candidates() -> list[Candidate]:
    """Keep the candidate set small enough to limit data-mining risk."""
    classifiers: list[tuple[str, Callable[[], Any]]] = [
        (
            "logistic_regression",
            lambda: make_pipeline(
                StandardScaler(),
                LogisticRegression(C=0.25, max_iter=2_000, class_weight="balanced", random_state=0),
            ),
        ),
        (
            "hist_gradient_boosting",
            lambda: HistGradientBoostingClassifier(
                max_iter=150,
                learning_rate=0.05,
                max_leaf_nodes=8,
                min_samples_leaf=100,
                l2_regularization=10.0,
                random_state=0,
            ),
        ),
        (
            "extra_trees",
            lambda: ExtraTreesClassifier(
                n_estimators=500,
                max_depth=8,
                min_samples_leaf=75,
                class_weight="balanced",
                n_jobs=-1,
                random_state=0,
            ),
        ),
        (
            "deep_mlp_32x16",
            lambda: make_pipeline(
                StandardScaler(),
                MLPClassifier(
                    hidden_layer_sizes=(32, 16),
                    activation="relu",
                    solver="adam",
                    alpha=0.1,
                    learning_rate_init=0.001,
                    batch_size=256,
                    max_iter=500,
                    early_stopping=True,
                    validation_fraction=0.15,
                    n_iter_no_change=25,
                    random_state=0,
                ),
            ),
        ),
    ]
    return [
        Candidate(name=f"{name}_{calibration}", calibration=calibration, factory=factory)
        for name, factory in classifiers
        for calibration in ("raw", "isotonic")
    ]


def fit_calibrator(method: str, probabilities: np.ndarray, actual: np.ndarray) -> Any | None:
    if method == "raw":
        return None
    if method == "isotonic":
        calibrator = IsotonicRegression(y_min=0.0, y_max=1.0, out_of_bounds="clip")
        calibrator.fit(probabilities, actual)
        return calibrator
    raise ValueError(f"Unsupported calibration method: {method}")


def apply_calibrator(calibrator: Any | None, probabilities: np.ndarray) -> np.ndarray:
    calibrated = probabilities if calibrator is None else calibrator.predict(probabilities)
    return np.asarray(calibrated, dtype=float)


def probability_metrics(actual: np.ndarray, probabilities: np.ndarray) -> dict[str, float]:
    clipped = np.clip(probabilities, 1e-6, 1.0 - 1e-6)
    return {
        "brier_score": float(brier_score_loss(actual, clipped)),
        "log_loss": float(log_loss(actual, clipped, labels=[0, 1])),
    }


def confidence(probabilities: np.ndarray) -> np.ndarray:
    return np.maximum(probabilities, 1.0 - probabilities)


def streaks(wins: np.ndarray) -> dict[str, int]:
    groups = [(result, len(list(group))) for result, group in groupby(wins.astype(int).tolist())]
    return {
        "longest_win": max((length for result, length in groups if result), default=0),
        "longest_loss": max((length for result, length in groups if not result), default=0),
    }


def wilson_lower_bound(wins: int, total: int, z: float = 1.96) -> float:
    """Conservative lower 95% bound for the selected directional hit rate."""
    if total == 0:
        return 0.0
    rate = wins / total
    denominator = 1.0 + z * z / total
    centre = rate + z * z / (2.0 * total)
    spread = z * math.sqrt(rate * (1.0 - rate) / total + z * z / (4.0 * total * total))
    return (centre - spread) / denominator


def selected_metrics(actual: np.ndarray, probabilities: np.ndarray, threshold: float) -> dict[str, Any]:
    picks = confidence(probabilities) >= threshold
    selected_actual = actual[picks]
    selected_predictions = (probabilities[picks] >= 0.5).astype(int)
    wins = (selected_predictions == selected_actual).astype(int)
    trade_count = len(wins)
    win_count = int(wins.sum())
    return {
        "trades": trade_count,
        "coverage": float(np.mean(picks)),
        "wins": win_count,
        "losses": int(trade_count - win_count),
        "win_rate": float(wins.mean()) if trade_count else None,
        "wilson_lower_95": wilson_lower_bound(win_count, trade_count),
        "win_rate_vs_50pct_pvalue": (
            float(binomtest(win_count, trade_count, p=0.5).pvalue) if trade_count else None
        ),
        "streaks": streaks(wins),
    }


def choose_threshold(
    actual: np.ndarray,
    probabilities: np.ndarray,
    thresholds: list[float],
    minimum_coverage: float,
) -> tuple[float, list[dict[str, Any]]]:
    records = [{"threshold": value, **selected_metrics(actual, probabilities, value)} for value in thresholds]
    eligible = [record for record in records if record["coverage"] >= minimum_coverage]
    if not eligible:
        raise ValueError("No confidence gate meets the requested minimum coverage")
    # A lower confidence bound avoids selecting a sparse gate merely because of a lucky few trades.
    selected = max(
        eligible,
        key=lambda record: (
            float(record["wilson_lower_95"]),
            float(record["coverage"]),
            float(record["win_rate"]),
        ),
    )
    return float(selected["threshold"]), records


def candidate_probabilities(
    candidate: Candidate,
    blocks: dict[str, pd.DataFrame],
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Fit only on the early block; fit probability calibration only on the next block."""
    train = blocks["train"]
    calibration = blocks["calibration"]
    selection = blocks["selection"]
    test = blocks["test"]
    model = candidate.factory()
    model.fit(train[ML_ONLY_FEATURE_COLUMNS].to_numpy(dtype=float), train["actual_yes"].to_numpy(dtype=int))
    calibration_raw = model.predict_proba(calibration[ML_ONLY_FEATURE_COLUMNS].to_numpy(dtype=float))[:, 1]
    calibrator = fit_calibrator(candidate.calibration, calibration_raw, calibration["actual_yes"].to_numpy(dtype=int))
    selection_raw = model.predict_proba(selection[ML_ONLY_FEATURE_COLUMNS].to_numpy(dtype=float))[:, 1]
    test_raw = model.predict_proba(test[ML_ONLY_FEATURE_COLUMNS].to_numpy(dtype=float))[:, 1]
    return (
        apply_calibrator(calibrator, calibration_raw),
        apply_calibrator(calibrator, selection_raw),
        apply_calibrator(calibrator, test_raw),
    )


def rolling_diagnostics(
    rows: pd.DataFrame,
    candidate: Candidate,
    threshold: float,
    period_days: int,
) -> list[dict[str, Any]]:
    """Causal fixed-model monthly diagnostics; these do not choose the model or gate."""
    origin = rows["forecast_timestamp"].min().floor("D")
    latest = rows["forecast_timestamp"].max()
    reports: list[dict[str, Any]] = []
    start = origin + pd.Timedelta(days=period_days)
    while start + pd.Timedelta(days=period_days) <= latest:
        end = start + pd.Timedelta(days=period_days)
        test = rows[(rows["forecast_timestamp"] >= start) & (rows["forecast_timestamp"] < end)]
        historical = rows[
            (rows["forecast_timestamp"] < start) & (rows["settlement_timestamp"] < start)
        ]
        if len(historical) < MINIMUM_ROWS or len(test) < 100:
            start = end
            continue
        if candidate.calibration == "raw":
            base_train = historical
            calibration = None
        else:
            calibration_start = start - pd.Timedelta(days=period_days)
            base_train = historical[historical["forecast_timestamp"] < calibration_start]
            calibration = historical[historical["forecast_timestamp"] >= calibration_start]
            if len(base_train) < MINIMUM_ROWS or len(calibration) < 100:
                start = end
                continue
        model = candidate.factory()
        model.fit(base_train[ML_ONLY_FEATURE_COLUMNS].to_numpy(dtype=float), base_train["actual_yes"].to_numpy(dtype=int))
        if calibration is None:
            calibrator = None
        else:
            calibration_raw = model.predict_proba(calibration[ML_ONLY_FEATURE_COLUMNS].to_numpy(dtype=float))[:, 1]
            calibrator = fit_calibrator(
                candidate.calibration,
                calibration_raw,
                calibration["actual_yes"].to_numpy(dtype=int),
            )
        probabilities = apply_calibrator(
            calibrator,
            model.predict_proba(test[ML_ONLY_FEATURE_COLUMNS].to_numpy(dtype=float))[:, 1],
        )
        reports.append({
            "start": start.isoformat(),
            "end": end.isoformat(),
            "base_training_rows": int(len(base_train)),
            "test_rows": int(len(test)),
            "probability_metrics": probability_metrics(test["actual_yes"].to_numpy(dtype=int), probabilities),
            "selected_metrics": selected_metrics(
                test["actual_yes"].to_numpy(dtype=int), probabilities, threshold
            ),
        })
        start = end
    return reports


def run(
    rows: pd.DataFrame,
    calibration_fraction: float,
    selection_fraction: float,
    test_fraction: float,
    thresholds: list[float],
    minimum_coverage: float,
    rolling_period_days: int,
) -> dict[str, Any]:
    blocks, timing = chronological_blocks(rows, calibration_fraction, selection_fraction, test_fraction)
    selection_actual = blocks["selection"]["actual_yes"].to_numpy(dtype=int)
    test_actual = blocks["test"]["actual_yes"].to_numpy(dtype=int)
    reports: dict[str, dict[str, Any]] = {}
    probabilities: dict[str, tuple[np.ndarray, np.ndarray]] = {}
    for candidate in base_candidates():
        _, selection_probability, test_probability = candidate_probabilities(candidate, blocks)
        probabilities[candidate.name] = (selection_probability, test_probability)
        reports[candidate.name] = {
            "calibration": candidate.calibration,
            "selection_probability_metrics": probability_metrics(selection_actual, selection_probability),
            # This is included for transparent audit only.  It is never used for selection.
            "untouched_test_probability_metrics": probability_metrics(test_actual, test_probability),
        }

    selected_name = min(
        reports,
        key=lambda name: reports[name]["selection_probability_metrics"]["brier_score"],
    )
    selected_candidate = next(candidate for candidate in base_candidates() if candidate.name == selected_name)
    selected_selection_probability, selected_test_probability = probabilities[selected_name]
    threshold, threshold_records = choose_threshold(
        selection_actual, selected_selection_probability, thresholds, minimum_coverage
    )
    rolling = rolling_diagnostics(rows, selected_candidate, threshold, rolling_period_days)
    positive_periods = sum(
        report["selected_metrics"]["win_rate"] is not None
        and report["selected_metrics"]["win_rate"] > 0.5
        for report in rolling
    )
    return {
        "feature_schema": FEATURE_SCHEMA,
        "method": (
            "Four chronological blocks: base training, probability calibration, model/gate selection, "
            "and a final untouched test. Every labeled block is embargoed by settlement timestamp."
        ),
        "outcome_limit": (
            "Directional correctness only. The historical artifact lacks executable Kalshi entry prices, "
            "order-book depth, fees, slippage, and fills, so these results are not P&L."
        ),
        "candidate_set": (
            "Regularized logistic regression, histogram gradient boosting, extra trees, and a two-hidden-layer "
            "tabular MLP; each is tested raw and with isotonic calibration."
        ),
        "rows": {name: int(len(block)) for name, block in blocks.items()},
        "timing": timing,
        "candidate_models": reports,
        "selected_model": selected_name,
        "selected_threshold": threshold,
        "threshold_selection_on_selection_block": threshold_records,
        "selected_model_untouched_test": {
            "probability_metrics": probability_metrics(test_actual, selected_test_probability),
            "selected_metrics": selected_metrics(test_actual, selected_test_probability, threshold),
        },
        "rolling_period_diagnostics": {
            "period_days": rolling_period_days,
            "selection_rule": (
                "Causal refits with the model and threshold fixed after development-block selection. "
                "This is a descriptive stability check: earlier periods precede selection and are not "
                "independent confirmation."
            ),
            "periods": rolling,
            "positive_periods": positive_periods,
            "period_count": len(rolling),
        },
    }


def parse_thresholds(value: str) -> list[float]:
    thresholds = [float(item.strip()) for item in value.split(",") if item.strip()]
    if not thresholds or any(threshold < 0.5 or threshold > 1.0 for threshold in thresholds):
        raise argparse.ArgumentTypeError("thresholds must be comma-separated values from 0.5 through 1.0")
    return sorted(set(thresholds))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=Path("ml_robust_backtest_summary.json"))
    parser.add_argument("--calibration-fraction", type=float, default=0.15)
    parser.add_argument("--selection-fraction", type=float, default=0.15)
    parser.add_argument("--test-fraction", type=float, default=0.15)
    parser.add_argument(
        "--thresholds",
        type=parse_thresholds,
        default=parse_thresholds("0.50,0.52,0.54,0.55,0.575,0.60"),
    )
    parser.add_argument("--minimum-coverage", type=float, default=0.05)
    parser.add_argument("--rolling-period-days", type=int, default=30)
    args = parser.parse_args()
    for name in ("calibration_fraction", "selection_fraction", "test_fraction"):
        if not 0.05 <= getattr(args, name) < 0.3:
            parser.error(f"{name} must be from 0.05 through 0.3")
    if args.calibration_fraction + args.selection_fraction + args.test_fraction >= 0.5:
        parser.error("calibration, selection, and test fractions must leave at least half for training")
    if not 0.01 <= args.minimum_coverage <= 1.0:
        parser.error("minimum coverage must be from 0.01 through 1.0")
    if args.rolling_period_days < 7:
        parser.error("rolling-period-days must be at least seven")
    return args


def main() -> int:
    args = parse_args()
    report = run(
        load_rows(args.input),
        args.calibration_fraction,
        args.selection_fraction,
        args.test_fraction,
        args.thresholds,
        args.minimum_coverage,
        args.rolling_period_days,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
