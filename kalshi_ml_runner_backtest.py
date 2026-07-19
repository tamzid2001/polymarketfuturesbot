"""Chronological holdout backtest for the standalone KXBTC15M ML runner.

The source rows are already feature-safe at their forecast times. This script
adds a second protection against model-selection leakage: it uses an early
training period, a later calibration/selection period, and a final untouched
test period. Candidate selection and confidence-threshold selection never see
the final test outcomes.

It reports directional correctness only. The historical artifact has no
executable entry prices, fills, or fees, so this is not a P&L backtest.
"""

from __future__ import annotations

import argparse
import json
import math
from itertools import groupby
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from scipy.stats import binomtest
from sklearn.ensemble import ExtraTreesClassifier
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from kalshi_btc15m_backtest import FEATURE_COLUMNS


def load_rows(path: Path) -> pd.DataFrame:
    rows = pd.read_csv(path)
    required = set(FEATURE_COLUMNS) | {
        "actual_yes", "forecast_at", "settlement_ts", "ml_probability_yes",
    }
    missing = required - set(rows.columns)
    if missing:
        raise ValueError(f"Input is missing required columns: {', '.join(sorted(missing))}")
    rows = rows.copy()
    rows["forecast_timestamp"] = pd.to_datetime(rows["forecast_at"], utc=True, errors="coerce")
    rows["settlement_timestamp"] = pd.to_datetime(rows["settlement_ts"], utc=True, errors="coerce")
    rows["actual_yes"] = pd.to_numeric(rows["actual_yes"], errors="coerce")
    rows["ml_probability_yes"] = pd.to_numeric(rows["ml_probability_yes"], errors="coerce")
    for name in FEATURE_COLUMNS:
        rows[name] = pd.to_numeric(rows[name], errors="coerce")
    rows = rows[
        rows["forecast_timestamp"].notna()
        & rows["settlement_timestamp"].notna()
        & rows["actual_yes"].isin([0, 1])
        & rows[FEATURE_COLUMNS].notna().all(axis=1)
    ].sort_values("forecast_timestamp", kind="stable").reset_index(drop=True)
    if len(rows) < 1_000:
        raise ValueError("At least 1,000 usable rows are required for a holdout backtest")
    return rows


def chronological_splits(
    rows: pd.DataFrame,
    calibration_fraction: float,
    test_fraction: float,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, str]]:
    total = len(rows)
    calibration_start_index = int(total * (1.0 - calibration_fraction - test_fraction))
    test_start_index = int(total * (1.0 - test_fraction))
    calibration_start = rows.loc[calibration_start_index, "forecast_timestamp"]
    test_start = rows.loc[test_start_index, "forecast_timestamp"]

    # A model fit at each boundary can only use labels already settled then.
    train = rows.iloc[:calibration_start_index]
    train = train[train["settlement_timestamp"] < calibration_start].copy()
    calibration = rows.iloc[calibration_start_index:test_start_index]
    calibration = calibration[calibration["settlement_timestamp"] < test_start].copy()
    test = rows.iloc[test_start_index:].copy()
    if min(len(train), len(calibration), len(test)) < 100:
        raise ValueError("Chronological split left an insufficient train/calibration/test block")
    return train, calibration, test, {
        "train_end_before": calibration_start.isoformat(),
        "calibration_end_before": test_start.isoformat(),
        "test_start": test_start.isoformat(),
    }


def candidates() -> dict[str, Any]:
    return {
        "logistic_regression": make_pipeline(
            StandardScaler(),
            LogisticRegression(C=0.25, max_iter=2_000, class_weight="balanced", random_state=0),
        ),
        "extra_trees": ExtraTreesClassifier(
            n_estimators=400,
            max_depth=8,
            min_samples_leaf=75,
            class_weight="balanced",
            n_jobs=-1,
            random_state=0,
        ),
    }


def calibrated_probabilities(
    model: Any,
    train: pd.DataFrame,
    calibration: pd.DataFrame,
    test: pd.DataFrame,
) -> tuple[np.ndarray, np.ndarray]:
    x_train = train[FEATURE_COLUMNS].to_numpy(dtype=float)
    y_train = train["actual_yes"].to_numpy(dtype=int)
    x_calibration = calibration[FEATURE_COLUMNS].to_numpy(dtype=float)
    y_calibration = calibration["actual_yes"].to_numpy(dtype=int)
    x_test = test[FEATURE_COLUMNS].to_numpy(dtype=float)
    model.fit(x_train, y_train)
    calibration_raw = model.predict_proba(x_calibration)[:, 1]
    test_raw = model.predict_proba(x_test)[:, 1]
    calibrator = IsotonicRegression(y_min=0.0, y_max=1.0, out_of_bounds="clip")
    calibrator.fit(calibration_raw, y_calibration)
    return calibrator.predict(calibration_raw), calibrator.predict(test_raw)


def confidence(probabilities: np.ndarray) -> np.ndarray:
    return np.maximum(probabilities, 1.0 - probabilities)


def streaks(wins: np.ndarray) -> dict[str, int]:
    groups = [(result, len(list(group))) for result, group in groupby(wins.astype(int).tolist())]
    return {
        "longest_win": max((length for result, length in groups if result), default=0),
        "longest_loss": max((length for result, length in groups if not result), default=0),
    }


def selected_metrics(actual: np.ndarray, probabilities: np.ndarray, threshold: float) -> dict[str, Any]:
    picks = confidence(probabilities) >= threshold
    selected_actual = actual[picks]
    selected_predictions = (probabilities[picks] >= 0.5).astype(int)
    wins = (selected_predictions == selected_actual).astype(int)
    trade_count = len(wins)
    return {
        "trades": trade_count,
        "coverage": float(np.mean(picks)),
        "wins": int(wins.sum()),
        "losses": int(trade_count - wins.sum()),
        "win_rate": float(wins.mean()) if trade_count else None,
        "win_rate_vs_50pct_pvalue": (
            float(binomtest(int(wins.sum()), trade_count, p=0.5).pvalue) if trade_count else None
        ),
        "streaks": streaks(wins),
    }


def probability_metrics(actual: np.ndarray, probabilities: np.ndarray) -> dict[str, float]:
    clipped = np.clip(probabilities, 1e-6, 1.0 - 1e-6)
    return {
        "brier_score": float(brier_score_loss(actual, clipped)),
        "log_loss": float(log_loss(actual, clipped, labels=[0, 1])),
    }


def choose_threshold(
    actual: np.ndarray,
    probabilities: np.ndarray,
    thresholds: list[float],
    minimum_coverage: float,
) -> tuple[float, list[dict[str, Any]]]:
    records: list[dict[str, Any]] = []
    for threshold in thresholds:
        metrics = selected_metrics(actual, probabilities, threshold)
        records.append({"threshold": threshold, **metrics})
    eligible = [record for record in records if record["coverage"] >= minimum_coverage]
    if not eligible:
        raise ValueError("No confidence threshold meets the requested minimum calibration coverage")
    selected = max(eligible, key=lambda record: (record["win_rate"], record["coverage"]))
    return float(selected["threshold"]), records


def historical_ml_baseline(test: pd.DataFrame, threshold: float) -> dict[str, Any] | None:
    probabilities = test["ml_probability_yes"].to_numpy(dtype=float)
    valid = np.isfinite(probabilities)
    if not valid.any():
        return None
    actual = test.loc[valid, "actual_yes"].to_numpy(dtype=int)
    probabilities = probabilities[valid]
    return {
        "probability_metrics": probability_metrics(actual, probabilities),
        "selected_metrics": selected_metrics(actual, probabilities, threshold),
    }


def run(
    rows: pd.DataFrame,
    calibration_fraction: float,
    test_fraction: float,
    thresholds: list[float],
    minimum_coverage: float,
) -> dict[str, Any]:
    train, calibration, test, timing = chronological_splits(rows, calibration_fraction, test_fraction)
    actual_calibration = calibration["actual_yes"].to_numpy(dtype=int)
    actual_test = test["actual_yes"].to_numpy(dtype=int)
    candidate_reports: dict[str, dict[str, Any]] = {}
    candidate_probabilities: dict[str, tuple[np.ndarray, np.ndarray]] = {}

    for name, model in candidates().items():
        calibration_probabilities, test_probabilities = calibrated_probabilities(
            model, train, calibration, test
        )
        candidate_probabilities[name] = (calibration_probabilities, test_probabilities)
        candidate_reports[name] = {
            "calibration_probability_metrics": probability_metrics(
                actual_calibration, calibration_probabilities
            ),
        }

    selected_model = min(
        candidate_reports,
        key=lambda name: candidate_reports[name]["calibration_probability_metrics"]["brier_score"],
    )
    calibration_probabilities, test_probabilities = candidate_probabilities[selected_model]
    threshold, threshold_records = choose_threshold(
        actual_calibration, calibration_probabilities, thresholds, minimum_coverage
    )
    for name, (_, candidate_test_probabilities) in candidate_probabilities.items():
        candidate_reports[name]["untouched_test_probability_metrics"] = probability_metrics(
            actual_test, candidate_test_probabilities
        )

    return {
        "method": (
            "Early train / later calibration / final untouched chronological test. "
            "Calibration uses isotonic regression; model and confidence threshold are selected "
            "only from the calibration block."
        ),
        "outcome_limit": (
            "Directional correctness only; historical rows have no executable opening prices, fills, fees, or P&L."
        ),
        "rows": {"train": len(train), "calibration": len(calibration), "test": len(test)},
        "timing": timing,
        "candidate_models": candidate_reports,
        "selected_model": selected_model,
        "threshold_selection_on_calibration": threshold_records,
        "selected_threshold": threshold,
        "selected_model_untouched_test": {
            "probability_metrics": probability_metrics(actual_test, test_probabilities),
            "selected_metrics": selected_metrics(actual_test, test_probabilities, threshold),
        },
        "historical_walk_forward_ml_baseline_on_untouched_test": historical_ml_baseline(test, threshold),
    }


def parse_thresholds(value: str) -> list[float]:
    thresholds = [float(item.strip()) for item in value.split(",") if item.strip()]
    if not thresholds or any(threshold < 0.5 or threshold > 1.0 for threshold in thresholds):
        raise argparse.ArgumentTypeError("thresholds must be comma-separated values from 0.5 through 1.0")
    return sorted(set(thresholds))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=Path("ml_runner_backtest_summary.json"))
    parser.add_argument("--calibration-fraction", type=float, default=0.15)
    parser.add_argument("--test-fraction", type=float, default=0.15)
    parser.add_argument("--thresholds", type=parse_thresholds, default=parse_thresholds("0.50,0.55,0.60,0.65"))
    parser.add_argument("--minimum-coverage", type=float, default=0.20)
    args = parser.parse_args()
    if not (0.05 <= args.calibration_fraction < 0.4 and 0.05 <= args.test_fraction < 0.4):
        parser.error("calibration/test fractions must each be from 0.05 through 0.4")
    if args.calibration_fraction + args.test_fraction >= 0.5:
        parser.error("calibration and test fractions must leave at least half the rows for training")
    if not 0.01 <= args.minimum_coverage <= 1.0:
        parser.error("minimum coverage must be from 0.01 through 1.0")
    return args


def main() -> int:
    args = parse_args()
    report = run(
        load_rows(args.input),
        args.calibration_fraction,
        args.test_fraction,
        args.thresholds,
        args.minimum_coverage,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
