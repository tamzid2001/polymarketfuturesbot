"""Evaluate Prophet-free KXBTC15M ML retraining cadences without look-ahead.

The deployed model is a calibrated logistic-regression classifier.  This
research program holds that architecture, the 16 raw ML-only features, and
the calibration policy fixed while changing only how often the expanding model
is refit.  For each 6-hour, 12-hour, daily, 3-day, weekly, 14-day, and fixed
baseline schedule, a fit at time ``t`` can use only labels whose markets had
already settled before ``t``.

Cadences are selected using the earlier chronological development period.  The
most recent final period is never used to select a cadence.  It is reported as
an untouched comparison only.  This is directional research: the source
ledger lacks executable order-book prices, fills, fees, and slippage, so it is
not a P&L or production-deployment backtest.

No Prophet value is read, trained, or used by this module.  The historical
CSV keeps a legacy filename but this program requires exactly the ML-only
feature schema in :mod:`kalshi_ml_features`.
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
from sklearn.metrics import brier_score_loss, log_loss

from kalshi_ml_features import FEATURE_SCHEMA, ML_ONLY_FEATURE_COLUMNS
from kalshi_ml_model_train import CALIBRATION_TYPES, MODEL_TYPES, train_calibrated


MIN_BASE_ROWS = 1_000
DEFAULT_CADENCES = ("static", "6h", "12h", "1d", "3d", "7d", "14d")


def load_rows(path: Path) -> pd.DataFrame:
    """Load only causal ML-only features and time-order the prediction ledger."""
    rows = pd.read_csv(path)
    required = set(ML_ONLY_FEATURE_COLUMNS) | {"actual_yes", "forecast_at", "settlement_ts"}
    missing = required - set(rows.columns)
    if missing:
        raise ValueError(f"Input is missing required columns: {', '.join(sorted(missing))}")
    rows = rows.copy()
    rows["forecast_timestamp"] = pd.to_datetime(rows["forecast_at"], utc=True, errors="coerce", format="mixed")
    rows["settlement_timestamp"] = pd.to_datetime(rows["settlement_ts"], utc=True, errors="coerce", format="mixed")
    rows["actual_yes"] = pd.to_numeric(rows["actual_yes"], errors="coerce")
    for name in ML_ONLY_FEATURE_COLUMNS:
        rows[name] = pd.to_numeric(rows[name], errors="coerce")
    rows = rows[
        rows["forecast_timestamp"].notna()
        & rows["settlement_timestamp"].notna()
        & rows["actual_yes"].isin([0, 1])
        & rows[ML_ONLY_FEATURE_COLUMNS].notna().all(axis=1)
    ].sort_values("forecast_timestamp", kind="stable").reset_index(drop=True)
    if len(rows) < 1_500:
        raise ValueError("At least 1,500 usable rows are required for the cadence study")
    return rows


def required_history_rows(calibration: str, calibration_fraction: float) -> int:
    """Minimum settled rows required by the exact production train function."""
    if calibration == "raw":
        return MIN_BASE_ROWS
    return max(
        MIN_BASE_ROWS + 100,
        math.ceil(MIN_BASE_ROWS / (1.0 - calibration_fraction)),
    )


def confidence(probabilities: np.ndarray) -> np.ndarray:
    return np.maximum(probabilities, 1.0 - probabilities)


def streaks(wins: np.ndarray) -> dict[str, int]:
    groups = [(result, len(list(group))) for result, group in groupby(wins.astype(int).tolist())]
    return {
        "longest_win": max((length for result, length in groups if result), default=0),
        "longest_loss": max((length for result, length in groups if not result), default=0),
    }


def directional_metrics(actual: np.ndarray, probabilities: np.ndarray, gate: float) -> dict[str, Any]:
    valid = np.isfinite(probabilities)
    actual = actual[valid]
    probabilities = probabilities[valid]
    picked = confidence(probabilities) >= gate
    chosen_actual = actual[picked]
    chosen_probability = probabilities[picked]
    predictions = (chosen_probability >= 0.5).astype(int)
    wins = (predictions == chosen_actual).astype(int)
    trades = int(len(wins))
    win_count = int(wins.sum())
    return {
        "trades": trades,
        "coverage": float(np.mean(picked)) if len(picked) else 0.0,
        "wins": win_count,
        "losses": int(trades - win_count),
        "win_rate": float(wins.mean()) if trades else None,
        "win_rate_vs_50pct_pvalue": float(binomtest(win_count, trades, p=0.5).pvalue) if trades else None,
        "streaks": streaks(wins),
    }


def probability_metrics(actual: np.ndarray, probabilities: np.ndarray) -> dict[str, float]:
    valid = np.isfinite(probabilities)
    actual = actual[valid]
    probabilities = np.clip(probabilities[valid], 1e-6, 1.0 - 1e-6)
    return {
        "brier_score": float(brier_score_loss(actual, probabilities)),
        "log_loss": float(log_loss(actual, probabilities, labels=[0, 1])),
    }


def cadence_bucket(timestamp: pd.Timestamp, cadence: str) -> pd.Timestamp:
    """Return the UTC retraining boundary that governs one prediction."""
    return timestamp.floor({"1d": "1D", "3d": "3D", "7d": "7D", "14d": "14D"}.get(cadence, cadence))


def fit_model(
    settled_rows: pd.DataFrame,
    settlement_values: np.ndarray,
    cutoff: pd.Timestamp,
    model_type: str,
    calibration: str,
    calibration_fraction: float,
) -> tuple[Any, int] | None:
    """Fit exactly the production model from labels settled strictly before cutoff."""
    end = int(np.searchsorted(settlement_values, cutoff.to_datetime64(), side="left"))
    needed = required_history_rows(calibration, calibration_fraction)
    if end < needed:
        return None
    historical = settled_rows.iloc[:end].copy()
    # ``settled_rows`` is already ordered by settlement timestamp.  The
    # production trainer reserves its trailing chronological rows for the
    # isotonic mapping, so this call retains the live training semantics.
    model, _metadata = train_calibrated(
        historical, model_type, calibration, calibration_fraction,
    )
    return model, end


def simulate_cadence(
    rows: pd.DataFrame,
    cadence: str,
    model_type: str,
    calibration: str,
    calibration_fraction: float,
) -> tuple[np.ndarray, list[dict[str, Any]]]:
    """Generate strictly causal probabilities for one expanding retrain rule."""
    probabilities = np.full(len(rows), np.nan, dtype=float)
    settled_rows = rows.sort_values("settlement_timestamp", kind="stable").reset_index(drop=True)
    settlement_values = settled_rows["settlement_timestamp"].to_numpy(dtype="datetime64[ns]")
    x_all = rows[ML_ONLY_FEATURE_COLUMNS].to_numpy(dtype=float)
    fits: list[dict[str, Any]] = []

    if cadence == "static":
        for index, cutoff in enumerate(rows["forecast_timestamp"]):
            result = fit_model(
                settled_rows, settlement_values, cutoff, model_type, calibration, calibration_fraction,
            )
            if result is None:
                continue
            model, train_rows = result
            probabilities[index:] = model.predict_proba(x_all[index:])[:, 1]
            fits.append({
                "trained_at": cutoff.isoformat(), "settled_training_rows": train_rows,
                "prediction_rows": int(len(rows) - index),
            })
            return probabilities, fits
        return probabilities, fits

    buckets = rows["forecast_timestamp"].map(lambda value: cadence_bucket(value, cadence))
    model: Any | None = None
    for boundary, positions in rows.groupby(buckets, sort=True).groups.items():
        indices = np.asarray(list(positions), dtype=int)
        result = fit_model(
            settled_rows, settlement_values, pd.Timestamp(boundary),
            model_type, calibration, calibration_fraction,
        )
        if result is not None:
            model, train_rows = result
            fits.append({
                "trained_at": pd.Timestamp(boundary).isoformat(), "settled_training_rows": train_rows,
                "prediction_rows": int(len(indices)),
            })
        if model is not None:
            probabilities[indices] = model.predict_proba(x_all[indices])[:, 1]
    return probabilities, fits


def paired_comparison(actual: np.ndarray, candidate: np.ndarray, baseline: np.ndarray) -> dict[str, Any]:
    """Exact paired sign test on disagreements in directional correctness."""
    valid = np.isfinite(candidate) & np.isfinite(baseline)
    actual = actual[valid]
    candidate_correct = (candidate[valid] >= 0.5).astype(int) == actual
    baseline_correct = (baseline[valid] >= 0.5).astype(int) == actual
    candidate_only = int(np.sum(candidate_correct & ~baseline_correct))
    baseline_only = int(np.sum(~candidate_correct & baseline_correct))
    disagreements = candidate_only + baseline_only
    return {
        "common_predictions": int(len(actual)),
        "candidate_only_correct": candidate_only,
        "baseline_only_correct": baseline_only,
        "net_correct_difference": candidate_only - baseline_only,
        "two_sided_pvalue": (
            float(binomtest(candidate_only, disagreements, p=0.5).pvalue) if disagreements else 1.0
        ),
    }


def window_reports(
    rows: pd.DataFrame,
    probabilities_by_cadence: dict[str, np.ndarray],
    test_mask: np.ndarray,
    gate: float,
) -> list[dict[str, Any]]:
    """Report final-period calendar months so one aggregate cannot hide drift."""
    test_rows = rows.loc[test_mask, ["forecast_timestamp", "actual_yes"]].copy()
    test_rows["calendar_month"] = test_rows["forecast_timestamp"].dt.tz_localize(None).dt.to_period("M").astype(str)
    reports: list[dict[str, Any]] = []
    for month, source_indices in test_rows.groupby("calendar_month", sort=True).groups.items():
        indices = np.asarray(list(source_indices), dtype=int)
        actual = rows.loc[indices, "actual_yes"].to_numpy(dtype=int)
        reports.append({
            "month": month,
            "rows": int(len(indices)),
            "cadences": {
                cadence: directional_metrics(actual, probabilities[indices], gate)
                for cadence, probabilities in probabilities_by_cadence.items()
            },
        })
    return reports


def run(
    rows: pd.DataFrame,
    cadences: tuple[str, ...],
    model_type: str,
    calibration: str,
    calibration_fraction: float,
    final_test_fraction: float,
    gates: tuple[float, ...],
) -> dict[str, Any]:
    """Select a cadence on development data and report the untouched final period."""
    probability_by_cadence: dict[str, np.ndarray] = {}
    fits_by_cadence: dict[str, list[dict[str, Any]]] = {}
    for cadence in cadences:
        print(f"[cadence] simulating {cadence}", flush=True)
        probability_by_cadence[cadence], fits_by_cadence[cadence] = simulate_cadence(
            rows, cadence, model_type, calibration, calibration_fraction,
        )

    final_start_index = int(len(rows) * (1.0 - final_test_fraction))
    final_start = rows.loc[final_start_index, "forecast_timestamp"]
    final_mask = (rows["forecast_timestamp"] >= final_start).to_numpy(dtype=bool)
    common_predictions = np.logical_and.reduce([
        np.isfinite(probabilities) for probabilities in probability_by_cadence.values()
    ])
    selection_mask = common_predictions & ~final_mask
    if int(selection_mask.sum()) < 1_000 or int(final_mask.sum()) < 500:
        raise ValueError("Chronological selection/final periods are too small after the training warm-up")

    actual = rows["actual_yes"].to_numpy(dtype=int)
    reports: dict[str, dict[str, Any]] = {}
    for cadence, probabilities in probability_by_cadence.items():
        selection_actual = actual[selection_mask]
        selection_probability = probabilities[selection_mask]
        final_actual = actual[final_mask]
        final_probability = probabilities[final_mask]
        reports[cadence] = {
            "retrain_count": int(len(fits_by_cadence[cadence])),
            "first_fit": fits_by_cadence[cadence][0] if fits_by_cadence[cadence] else None,
            "last_fit": fits_by_cadence[cadence][-1] if fits_by_cadence[cadence] else None,
            "selection": {
                "probability_metrics": probability_metrics(selection_actual, selection_probability),
                "directional_by_gate": {
                    f"{gate:.3f}": directional_metrics(selection_actual, selection_probability, gate)
                    for gate in gates
                },
            },
            "untouched_final": {
                "probability_metrics": probability_metrics(final_actual, final_probability),
                "directional_by_gate": {
                    f"{gate:.3f}": directional_metrics(final_actual, final_probability, gate)
                    for gate in gates
                },
            },
        }

    # Pre-declared selection: full-coverage directional accuracy, then Brier
    # score, then the less-frequent schedule.  The final holdout never affects
    # this choice.
    full_gate_key = f"{min(gates):.3f}"
    selected_cadence = max(
        cadences,
        key=lambda cadence: (
            float(reports[cadence]["selection"]["directional_by_gate"][full_gate_key]["win_rate"] or 0.0),
            -float(reports[cadence]["selection"]["probability_metrics"]["brier_score"]),
            -len(fits_by_cadence[cadence]),
        ),
    )
    baseline_cadence = "1d" if "1d" in cadences else cadences[0]
    final_actual = actual[final_mask]
    selection_actual = actual[selection_mask]
    selection_comparisons = {
        cadence: paired_comparison(
            selection_actual, probabilities[selection_mask], probability_by_cadence[baseline_cadence][selection_mask],
        )
        for cadence, probabilities in probability_by_cadence.items()
        if cadence != baseline_cadence
    }
    final_comparisons = {
        cadence: paired_comparison(final_actual, probabilities[final_mask], probability_by_cadence[baseline_cadence][final_mask])
        for cadence, probabilities in probability_by_cadence.items()
        if cadence != baseline_cadence
    }
    selected_vs_daily = (
        None if selected_cadence == baseline_cadence
        else final_comparisons[selected_cadence]
    )

    return {
        "feature_schema": FEATURE_SCHEMA,
        "method": (
            "Expanding chronological walk-forward retraining. At every retrain boundary, "
            "only labels settled strictly before that boundary enter base training or isotonic calibration. "
            "Cadence is selected on the earlier development period; final period is untouched."
        ),
        "outcome_limit": (
            "Directional and probability metrics only. Historical rows do not contain executable Kalshi prices, "
            "fills, fees, or slippage, so this cannot establish P&L or live profitability."
        ),
        "model": {
            "model_type": model_type,
            "calibration": calibration,
            "calibration_fraction": calibration_fraction,
            "uses_prophet": False,
            "feature_columns": ML_ONLY_FEATURE_COLUMNS,
        },
        "cadences": list(cadences),
        "gates": list(gates),
        "rows": {
            "total": int(len(rows)),
            "selection_common": int(selection_mask.sum()),
            "untouched_final": int(final_mask.sum()),
        },
        "timing": {
            "first_forecast": rows["forecast_timestamp"].min().isoformat(),
            "final_test_start": final_start.isoformat(),
            "last_forecast": rows["forecast_timestamp"].max().isoformat(),
        },
        "selection_rule": (
            f"Highest development full-coverage ({min(gates):.3f}) directional win rate; then lower Brier score; "
            "then fewer refits."
        ),
        "development_selected_cadence": selected_cadence,
        "baseline_cadence": baseline_cadence,
        "selection_paired_comparisons_vs_baseline": selection_comparisons,
        "final_paired_comparisons_vs_baseline": final_comparisons,
        "selected_vs_baseline_final": selected_vs_daily,
        "cadence_reports": reports,
        "untouched_final_months": window_reports(rows, probability_by_cadence, final_mask, min(gates)),
    }


def parse_cadences(value: str) -> tuple[str, ...]:
    values = tuple(item.strip().lower() for item in value.split(",") if item.strip())
    if not values:
        raise argparse.ArgumentTypeError("At least one cadence is required")
    unsupported = [value for value in values if value != "static" and value not in DEFAULT_CADENCES]
    if unsupported:
        raise argparse.ArgumentTypeError(
            f"Unsupported cadence(s): {', '.join(unsupported)}. Supported: {', '.join(DEFAULT_CADENCES)}"
        )
    if len(set(values)) != len(values):
        raise argparse.ArgumentTypeError("Cadences must be unique")
    return values


def parse_gates(value: str) -> tuple[float, ...]:
    values = tuple(sorted({float(item.strip()) for item in value.split(",") if item.strip()}))
    if not values or any(value < 0.5 or value > 1.0 for value in values):
        raise argparse.ArgumentTypeError("Gates must be comma-separated values from 0.50 through 1.00")
    return values


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=Path("ml_retraining_cadence_backtest.json"))
    parser.add_argument("--cadences", type=parse_cadences, default=DEFAULT_CADENCES)
    parser.add_argument("--model-type", choices=MODEL_TYPES, default="logistic_regression")
    parser.add_argument("--calibration", choices=CALIBRATION_TYPES, default="isotonic")
    parser.add_argument("--calibration-fraction", type=float, default=0.15)
    parser.add_argument("--final-test-fraction", type=float, default=0.20)
    parser.add_argument("--gates", type=parse_gates, default=(0.50, 0.55, 0.60))
    args = parser.parse_args()
    if not 0.01 <= args.calibration_fraction < 0.5:
        parser.error("--calibration-fraction must be in [0.01, 0.5)")
    if not 0.10 <= args.final_test_fraction <= 0.35:
        parser.error("--final-test-fraction must be from 0.10 through 0.35")
    return args


def main() -> int:
    args = parse_args()
    report = run(
        load_rows(args.input), args.cadences, args.model_type, args.calibration,
        args.calibration_fraction, args.final_test_fraction, args.gates,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({
        "development_selected_cadence": report["development_selected_cadence"],
        "baseline_cadence": report["baseline_cadence"],
        "selected_vs_baseline_final": report["selected_vs_baseline_final"],
        "final_full_coverage": {
            cadence: details["untouched_final"]["directional_by_gate"]["0.500"]
            for cadence, details in report["cadence_reports"].items()
        },
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
