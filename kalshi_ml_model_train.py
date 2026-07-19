"""Train and serialize the ML pipeline used by ``kalshi_ml_inference_live.py``.

The input must be a historical ``prophet_ml_backtest_rows.csv``. Only labels
settled before the requested cutoff are included, so the saved model records
exactly what information was available when it was trained.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from kalshi_btc15m_backtest import FEATURE_COLUMNS


MODEL_FORMAT_VERSION = 1


def read_training_rows(path: Path, as_of: pd.Timestamp | None) -> pd.DataFrame:
    rows = pd.read_csv(path)
    required = set(FEATURE_COLUMNS) | {"actual_yes", "settlement_ts"}
    missing = required - set(rows.columns)
    if missing:
        raise ValueError(f"Input is missing required columns: {', '.join(sorted(missing))}")
    rows = rows.copy()
    rows["settlement_timestamp"] = pd.to_datetime(rows["settlement_ts"], utc=True, errors="coerce")
    rows["actual_yes"] = pd.to_numeric(rows["actual_yes"], errors="coerce")
    for name in FEATURE_COLUMNS:
        rows[name] = pd.to_numeric(rows[name], errors="coerce")
    valid = (
        rows["settlement_timestamp"].notna()
        & rows["actual_yes"].isin([0, 1])
        & rows[FEATURE_COLUMNS].notna().all(axis=1)
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


def train(rows: pd.DataFrame) -> Any:
    model = make_pipeline(
        StandardScaler(),
        LogisticRegression(C=0.25, max_iter=2_000, class_weight="balanced", random_state=0),
    )
    model.fit(rows[FEATURE_COLUMNS].to_numpy(dtype=float), rows["actual_yes"].to_numpy(dtype=int))
    return model


def serialize(
    input_path: Path,
    rows: pd.DataFrame,
    model_path: Path,
    metadata_path: Path,
) -> dict[str, Any]:
    model_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    trained_at = datetime.now(tz=timezone.utc).isoformat()
    metadata: dict[str, Any] = {
        "format_version": MODEL_FORMAT_VERSION,
        "model_type": "standard_scaler_logistic_regression",
        "feature_columns": FEATURE_COLUMNS,
        "trained_at": trained_at,
        "training_rows": int(len(rows)),
        "training_actual_yes_rate": float(rows["actual_yes"].mean()),
        "settlement_cutoff": rows["settlement_timestamp"].max().isoformat(),
        "source_filename": input_path.name,
        "source_sha256": sha256(input_path),
    }
    payload = {
        "format_version": MODEL_FORMAT_VERSION,
        "feature_columns": FEATURE_COLUMNS,
        "model": train(rows),
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
        "--as-of",
        type=parse_as_of,
        help="Strict settlement cutoff in ISO-8601 UTC; omit to use every settled row in the input.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rows = read_training_rows(args.input, args.as_of)
    metadata = serialize(args.input, rows, args.model_output, args.metadata_output)
    print(json.dumps(metadata, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
