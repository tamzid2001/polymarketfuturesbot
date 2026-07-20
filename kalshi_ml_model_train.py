"""Train and serialize the ML pipeline used by ``kalshi_ml_inference_live.py``.

The input may be the historical ``prophet_ml_backtest_rows.csv`` artifact, but
the production schema deliberately selects only candle, strike, clock, and
previously-settled-outcome fields. It does not use the artifact's Prophet
columns. Only labels settled before the requested cutoff are included.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from kalshi_ml_features import FEATURE_SCHEMA, ML_ONLY_FEATURE_COLUMNS


MODEL_FORMAT_VERSION = 3
MODEL_TYPES = ("hist_gradient_boosting", "logistic_regression")
CALIBRATION_TYPES = ("raw", "isotonic")


class IsotonicCalibratedClassifier:
    """Serializable binary classifier that calibrates its base probability."""

    def __init__(self, base_model: Any, calibrator: IsotonicRegression) -> None:
        self.base_model = base_model
        self.calibrator = calibrator

    def predict_proba(self, values: Any) -> np.ndarray:
        raw_probability = self.base_model.predict_proba(values)[:, 1]
        probability = np.clip(self.calibrator.predict(raw_probability), 0.0, 1.0)
        return np.column_stack((1.0 - probability, probability))


def read_training_rows(path: Path, as_of: pd.Timestamp | None) -> pd.DataFrame:
    rows = pd.read_csv(path)
    required = set(ML_ONLY_FEATURE_COLUMNS) | {"actual_yes", "settlement_ts"}
    missing = required - set(rows.columns)
    if missing:
        raise ValueError(f"Input is missing required columns: {', '.join(sorted(missing))}")
    rows = rows.copy()
    rows["settlement_timestamp"] = pd.to_datetime(rows["settlement_ts"], utc=True, errors="coerce")
    rows["actual_yes"] = pd.to_numeric(rows["actual_yes"], errors="coerce")
    for name in ML_ONLY_FEATURE_COLUMNS:
        rows[name] = pd.to_numeric(rows[name], errors="coerce")
    valid = (
        rows["settlement_timestamp"].notna()
        & rows["actual_yes"].isin([0, 1])
        & rows[ML_ONLY_FEATURE_COLUMNS].notna().all(axis=1)
    )
    if as_of is not None:
        valid &= rows["settlement_timestamp"] < as_of
    rows = rows.loc[valid].sort_values("settlement_timestamp", kind="stable").reset_index(drop=True)
    if len(rows) < 1_000:
        raise ValueError("At least 1,000 settled, feature-complete rows are required")
    if rows["actual_yes"].nunique() < 2:
        raise ValueError("Training rows contain only one outcome class")
    return rows


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def train(rows: pd.DataFrame, model_type: str) -> Any:
    if model_type == "hist_gradient_boosting":
        model = HistGradientBoostingClassifier(
            max_iter=150,
            learning_rate=0.05,
            max_leaf_nodes=8,
            min_samples_leaf=100,
            l2_regularization=10.0,
            random_state=0,
        )
    elif model_type == "logistic_regression":
        model = make_pipeline(
            StandardScaler(),
            LogisticRegression(C=0.25, max_iter=2_000, class_weight="balanced", random_state=0),
        )
    else:
        raise ValueError(f"Unsupported model type: {model_type}")
    model.fit(rows[ML_ONLY_FEATURE_COLUMNS].to_numpy(dtype=float), rows["actual_yes"].to_numpy(dtype=int))
    return model


def train_calibrated(
    rows: pd.DataFrame, model_type: str, calibration: str, calibration_fraction: float,
) -> tuple[Any, dict[str, Any]]:
    """Fit a strictly chronological production classifier and calibrator.

    The latest trailing labels are reserved for isotonic calibration, so no
    calibration outcome is used to fit the base classifier.  This preserves a
    meaningful historical probability mapping at deployment time.
    """
    if calibration == "raw":
        return train(rows, model_type), {
            "calibration": "raw", "base_training_rows": int(len(rows)), "calibration_rows": 0,
        }
    if calibration != "isotonic":
        raise ValueError(f"Unsupported calibration method: {calibration}")
    calibration_rows = max(100, int(round(len(rows) * calibration_fraction)))
    base_rows = len(rows) - calibration_rows
    if base_rows < 1_000 or calibration_rows < 100:
        raise ValueError("Not enough chronological rows for base training plus isotonic calibration")
    base = rows.iloc[:base_rows].copy()
    calibration_rows_frame = rows.iloc[base_rows:].copy()
    if calibration_rows_frame["actual_yes"].nunique() < 2:
        raise ValueError("Chronological calibration rows contain only one outcome class")
    base_model = train(base, model_type)
    raw_probability = base_model.predict_proba(
        calibration_rows_frame[ML_ONLY_FEATURE_COLUMNS].to_numpy(dtype=float)
    )[:, 1]
    calibrator = IsotonicRegression(y_min=0.0, y_max=1.0, out_of_bounds="clip")
    calibrator.fit(raw_probability, calibration_rows_frame["actual_yes"].to_numpy(dtype=int))
    return IsotonicCalibratedClassifier(base_model, calibrator), {
        "calibration": "isotonic",
        "base_training_rows": int(len(base)),
        "calibration_rows": int(len(calibration_rows_frame)),
        "base_training_cutoff": base["settlement_timestamp"].max().isoformat(),
        "calibration_start": calibration_rows_frame["settlement_timestamp"].min().isoformat(),
        "calibration_cutoff": calibration_rows_frame["settlement_timestamp"].max().isoformat(),
        "calibration_fraction": calibration_fraction,
    }


def serialize(
    input_path: Path,
    rows: pd.DataFrame,
    model_path: Path,
    metadata_path: Path,
    model_type: str,
    calibration: str,
    calibration_fraction: float,
) -> dict[str, Any]:
    model_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    trained_at = datetime.now(tz=timezone.utc).isoformat()
    model, calibration_metadata = train_calibrated(rows, model_type, calibration, calibration_fraction)
    metadata: dict[str, Any] = {
        "format_version": MODEL_FORMAT_VERSION,
        "model_type": model_type,
        "feature_columns": ML_ONLY_FEATURE_COLUMNS,
        "feature_schema": FEATURE_SCHEMA,
        "trained_at": trained_at,
        "training_rows": int(len(rows)),
        "training_actual_yes_rate": float(rows["actual_yes"].mean()),
        "settlement_cutoff": rows["settlement_timestamp"].max().isoformat(),
        "source_filename": input_path.name,
        "source_sha256": sha256(input_path),
        **calibration_metadata,
    }
    payload = {
        "format_version": MODEL_FORMAT_VERSION,
        "feature_columns": ML_ONLY_FEATURE_COLUMNS,
        "model": model,
        "metadata": metadata,
    }
    joblib.dump(payload, model_path, compress=3)
    metadata_path.write_text(json.dumps(metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return metadata


def parse_as_of(value: str) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    return timestamp.tz_localize("UTC") if timestamp.tzinfo is None else timestamp.tz_convert("UTC")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--model-output", type=Path, required=True)
    parser.add_argument("--metadata-output", type=Path, required=True)
    parser.add_argument(
        "--model-type",
        choices=MODEL_TYPES,
        default="logistic_regression",
        help="Classifier to serialize; logistic regression is the calibrated production default.",
    )
    parser.add_argument(
        "--calibration", choices=CALIBRATION_TYPES, default="isotonic",
        help="Probability calibration method. Isotonic reserves the latest chronological rows for calibration.",
    )
    parser.add_argument(
        "--calibration-fraction", type=float, default=0.15,
        help="Trailing chronological fraction reserved for isotonic calibration (default 0.15).",
    )
    parser.add_argument(
        "--as-of",
        type=parse_as_of,
        help="Strict settlement cutoff in ISO-8601 UTC; omit to use every settled row in the input.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not 0.01 <= args.calibration_fraction < 0.5:
        raise ValueError("--calibration-fraction must be in [0.01, 0.5)")
    rows = read_training_rows(args.input, args.as_of)
    metadata = serialize(
        args.input, rows, args.model_output, args.metadata_output, args.model_type,
        args.calibration, args.calibration_fraction,
    )
    print(json.dumps(metadata, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
