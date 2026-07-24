"""Leakage-safe ML backtest for trade outcomes and three-trade streaks.

The script consumes ``prophet_ml_backtest_rows.csv`` from the historical
Kalshi artifact.  It never places an order.  Features are limited to values
available before a market opens; labels are the current trade result and the
next three observed results.  Every score is made on later, chronological
blocks that were not used to fit that score.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import ExtraTreesClassifier, RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    brier_score_loss,
    f1_score,
    log_loss,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.neighbors import KNeighborsClassifier
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.tree import DecisionTreeClassifier

try:
    from xgboost import XGBClassifier
except Exception:
    XGBClassifier = None


LOG = logging.getLogger("kalshi_streak_ml_backtest")
EASTERN = ZoneInfo("America/New_York")
TARGETS = ("trade_win", "next_3_all_win", "next_3_all_loss")


@dataclass(frozen=True)
class PreparedData:
    frame: pd.DataFrame
    numeric_features: list[str]
    categorical_features: list[str]


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    logging.getLogger("xgboost").setLevel(logging.WARNING)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def write_csv(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


def read_artifact(path: Path) -> tuple[pd.DataFrame, Path]:
    path = path.expanduser()
    if path.is_dir():
        candidate = path / "prophet_ml_backtest_rows.csv"
        if not candidate.exists():
            raise ValueError(f"{path} does not contain prophet_ml_backtest_rows.csv")
        path = candidate
    frame = pd.read_csv(path)
    required = {"market_open", "prophet_correct", "ml_correct"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"Artifact is missing required columns: {', '.join(sorted(missing))}")
    return frame, path


def numeric_column(frame: pd.DataFrame, name: str) -> pd.Series:
    if name not in frame:
        return pd.Series(np.nan, index=frame.index, dtype="float64")
    return pd.to_numeric(frame[name], errors="coerce")


def prior_streaks(wins: pd.Series) -> tuple[pd.Series, pd.Series]:
    prior_wins: list[int] = []
    prior_losses: list[int] = []
    running_wins = running_losses = 0
    for won in wins.astype(bool):
        prior_wins.append(running_wins)
        prior_losses.append(running_losses)
        if won:
            running_wins += 1
            running_losses = 0
        else:
            running_losses += 1
            running_wins = 0
    return pd.Series(prior_wins, index=wins.index), pd.Series(prior_losses, index=wins.index)


def prepare_signal_data(raw: pd.DataFrame, signal: str) -> PreparedData:
    correct_column = f"{signal}_correct"
    side_column = f"{signal}_side"
    if correct_column not in raw or side_column not in raw:
        raise ValueError(f"Artifact has no {signal} signal columns")
    data = raw.copy()
    data["timestamp"] = pd.to_datetime(data["market_open"], utc=True, errors="coerce")
    data["trade_win"] = numeric_column(data, correct_column)
    data = data[data["timestamp"].notna() & data["trade_win"].isin([0, 1])].copy()
    data = data.sort_values("timestamp", kind="stable").reset_index(drop=True)
    data["trade_number"] = np.arange(1, len(data) + 1)
    wins = data["trade_win"].astype(int)
    prior_wins, prior_losses = prior_streaks(wins)
    data["prior_winning_streak"] = prior_wins
    data["prior_losing_streak"] = prior_losses
    prior = wins.shift(1).astype(float)
    for window in (10, 50, 100):
        data[f"prior_rolling_{window}_win_rate"] = prior.rolling(window, min_periods=window).mean()
    for lag in range(1, 11):
        data[f"prior_result_{lag}"] = wins.shift(lag)
    future_count = wins.astype(float) + wins.shift(-1) + wins.shift(-2)
    data["next_3_all_win"] = np.where(future_count.notna(), (future_count.eq(3)).astype(int), np.nan)
    data["next_3_all_loss"] = np.where(future_count.notna(), (future_count.eq(0)).astype(int), np.nan)
    local = data["timestamp"].dt.tz_convert(EASTERN)
    minutes = local.dt.hour * 60 + local.dt.minute
    data["time_sin"] = np.sin(2 * math.pi * minutes / (24 * 60))
    data["time_cos"] = np.cos(2 * math.pi * minutes / (24 * 60))
    data["weekday"] = local.dt.dayofweek.astype(str)
    data["side"] = data[side_column].astype(str).str.upper()
    data["source"] = data["source"].astype(str) if "source" in data else "unknown"
    if signal == "ml" and "ml_probability_yes" in data:
        probability_yes = numeric_column(data, "ml_probability_yes")
        data["signal_confidence"] = np.maximum(probability_yes, 1 - probability_yes)
    else:
        data["signal_confidence"] = np.nan
    source_numeric = [
        "prophet_p10", "prophet_p50", "prophet_p90", "prophet_interval_bps",
        "prophet_p50_vs_spot_bps", "prophet_p50_vs_strike_bps", "spot_vs_strike_bps",
        "return_1m_bps", "return_5m_bps", "return_15m_bps", "return_60m_bps",
        "range_15m_bps", "vol_15m_bps", "vol_60m_bps", "known_outcome_count",
        "known_yes_rate_8", "lag_outcome_1", "lag_outcome_2", "lag_outcome_4", "lag_outcome_8",
    ]
    for name in source_numeric:
        data[name] = numeric_column(data, name)
    numeric_features = [
        "prior_winning_streak", "prior_losing_streak", "prior_rolling_10_win_rate",
        "prior_rolling_50_win_rate", "prior_rolling_100_win_rate", "time_sin", "time_cos",
        "signal_confidence", *[f"prior_result_{lag}" for lag in range(1, 11)], *source_numeric,
    ]
    numeric_features = [name for name in numeric_features if data[name].notna().any()]
    return PreparedData(data, numeric_features, ["side", "weekday", "source"])


def chronological_splits(rows: int, min_train: int, test_blocks: int) -> list[tuple[int, int]]:
    if rows <= min_train + test_blocks:
        return []
    test_size = max(100, (rows - min_train) // test_blocks)
    first_train_end = rows - test_size * test_blocks
    if first_train_end < min_train:
        first_train_end = min_train
    splits = []
    for block in range(test_blocks):
        train_end = first_train_end + block * test_size
        test_end = min(rows, train_end + test_size)
        if test_end > train_end:
            splits.append((train_end, test_end))
    return splits


def build_estimator(name: str, positive_rate: float, train_rows: int) -> Any:
    if name == "logistic_regression":
        return LogisticRegression(max_iter=2000, class_weight="balanced", random_state=0)
    if name == "knn":
        return KNeighborsClassifier(n_neighbors=min(101, max(5, train_rows // 20)), weights="distance", n_jobs=-1)
    if name == "decision_tree":
        return DecisionTreeClassifier(max_depth=5, min_samples_leaf=75, class_weight="balanced", random_state=0)
    if name == "random_forest":
        return RandomForestClassifier(
            n_estimators=200, max_depth=8, min_samples_leaf=50, class_weight="balanced_subsample",
            n_jobs=-1, random_state=0,
        )
    if name == "extra_trees":
        return ExtraTreesClassifier(
            n_estimators=200, max_depth=8, min_samples_leaf=50, class_weight="balanced",
            n_jobs=-1, random_state=0,
        )
    if name == "xgboost" and XGBClassifier is not None:
        scale = (1 - positive_rate) / positive_rate if 0 < positive_rate < 1 else 1.0
        return XGBClassifier(
            n_estimators=200, max_depth=3, learning_rate=0.03, subsample=0.8, colsample_bytree=0.8,
            scale_pos_weight=scale, eval_metric="logloss", n_jobs=-1, random_state=0,
        )
    raise ValueError(f"Unsupported estimator: {name}")


def metric_row(model: str, target: str, actual: np.ndarray, probabilities: np.ndarray,
               baseline_probabilities: np.ndarray) -> dict[str, Any]:
    predicted = (probabilities >= 0.5).astype(int)
    baseline_predicted = (baseline_probabilities >= 0.5).astype(int)
    top_count = max(1, math.ceil(len(actual) * 0.10))
    top_indices = np.argsort(probabilities)[-top_count:]
    return {
        "target": target,
        "model": model,
        "predictions": int(len(actual)),
        "positive_rate": float(np.mean(actual)),
        "accuracy": float(accuracy_score(actual, predicted)),
        "baseline_accuracy": float(accuracy_score(actual, baseline_predicted)),
        "accuracy_improvement": float(accuracy_score(actual, predicted) - accuracy_score(actual, baseline_predicted)),
        "precision": float(precision_score(actual, predicted, zero_division=0)),
        "recall": float(recall_score(actual, predicted, zero_division=0)),
        "f1": float(f1_score(actual, predicted, zero_division=0)),
        "average_precision": float(average_precision_score(actual, probabilities)),
        "roc_auc": float(roc_auc_score(actual, probabilities)) if len(np.unique(actual)) == 2 else None,
        "brier_score": float(brier_score_loss(actual, probabilities)),
        "baseline_brier_score": float(brier_score_loss(actual, baseline_probabilities)),
        "brier_improvement": float(brier_score_loss(actual, baseline_probabilities) - brier_score_loss(actual, probabilities)),
        "log_loss": float(log_loss(actual, np.clip(probabilities, 1e-9, 1 - 1e-9))),
        "baseline_log_loss": float(log_loss(actual, np.clip(baseline_probabilities, 1e-9, 1 - 1e-9))),
        "top_decile_precision": float(np.mean(actual[top_indices])),
        "top_decile_size": top_count,
    }


def evaluate_target(prepared: PreparedData, target: str, min_train: int,
                    test_blocks: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    data = prepared.frame[prepared.frame[target].notna()].reset_index(drop=True)
    target_values = data[target].astype(int).to_numpy()
    splits = chronological_splits(len(data), min_train, test_blocks)
    if len(splits) < 2 or len(np.unique(target_values)) < 2:
        return pd.DataFrame(), pd.DataFrame()
    features = data[prepared.numeric_features + prepared.categorical_features]
    numeric_transformer = Pipeline([("imputer", SimpleImputer(strategy="median")), ("scale", StandardScaler())])
    categorical_transformer = Pipeline([
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("onehot", OneHotEncoder(handle_unknown="ignore")),
    ])
    preprocessor = ColumnTransformer([
        ("numeric", numeric_transformer, prepared.numeric_features),
        ("categorical", categorical_transformer, prepared.categorical_features),
    ])
    model_names = ["logistic_regression", "knn", "decision_tree", "random_forest", "extra_trees"]
    if XGBClassifier is not None:
        model_names.append("xgboost")
    probabilities = {name: np.full(len(data), np.nan) for name in model_names}
    baselines = np.full(len(data), np.nan)
    for block, (train_end, test_end) in enumerate(splits, start=1):
        x_train, x_test = features.iloc[:train_end], features.iloc[train_end:test_end]
        y_train = target_values[:train_end]
        if len(np.unique(y_train)) < 2:
            continue
        LOG.info("%s block %d/%d: train=%d test=%d positive=%.2f%%", target, block, len(splits), train_end,
                 test_end - train_end, np.mean(y_train) * 100)
        baselines[train_end:test_end] = float(np.mean(y_train))
        for name in model_names:
            estimator = build_estimator(name, float(np.mean(y_train)), train_end)
            pipeline = Pipeline([("preprocess", preprocessor), ("model", estimator)])
            pipeline.fit(x_train, y_train)
            probabilities[name][train_end:test_end] = pipeline.predict_proba(x_test)[:, 1]
    mask = np.isfinite(baselines)
    if not mask.any():
        return pd.DataFrame(), pd.DataFrame()
    rows: list[dict[str, Any]] = []
    prediction_frames: list[pd.DataFrame] = []
    for name, values in probabilities.items():
        valid = mask & np.isfinite(values)
        if not valid.any():
            continue
        rows.append(metric_row(name, target, target_values[valid], values[valid], baselines[valid]))
        prediction = data.loc[valid, ["trade_number", "timestamp", "side", "prior_winning_streak", "prior_losing_streak"]].copy()
        prediction["target"] = target
        prediction["model"] = name
        prediction["actual_positive"] = target_values[valid]
        prediction["probability_positive"] = values[valid]
        prediction["predicted_positive"] = (values[valid] >= 0.5).astype(int)
        prediction["baseline_probability"] = baselines[valid]
        prediction_frames.append(prediction)
    valid_models = [name for name, values in probabilities.items() if np.isfinite(values[mask]).all()]
    if valid_models:
        ensemble = np.mean([probabilities[name][mask] for name in valid_models], axis=0)
        rows.append(metric_row("soft_voting_ensemble", target, target_values[mask], ensemble, baselines[mask]))
        prediction = data.loc[mask, ["trade_number", "timestamp", "side", "prior_winning_streak", "prior_losing_streak"]].copy()
        prediction["target"] = target
        prediction["model"] = "soft_voting_ensemble"
        prediction["actual_positive"] = target_values[mask]
        prediction["probability_positive"] = ensemble
        prediction["predicted_positive"] = (ensemble >= 0.5).astype(int)
        prediction["baseline_probability"] = baselines[mask]
        prediction_frames.append(prediction)
    return pd.DataFrame(rows), pd.concat(prediction_frames, ignore_index=True) if prediction_frames else pd.DataFrame()


def run_signal(raw: pd.DataFrame, signal: str, output_dir: Path, min_train: int,
               test_blocks: int) -> dict[str, Any]:
    prepared = prepare_signal_data(raw, signal)
    metrics: list[pd.DataFrame] = []
    predictions: list[pd.DataFrame] = []
    for target in TARGETS:
        LOG.info("%s: evaluating %s", signal, target)
        target_metrics, target_predictions = evaluate_target(prepared, target, min_train, test_blocks)
        if not target_metrics.empty:
            metrics.append(target_metrics)
        if not target_predictions.empty:
            predictions.append(target_predictions)
    metric_frame = pd.concat(metrics, ignore_index=True) if metrics else pd.DataFrame()
    prediction_frame = pd.concat(predictions, ignore_index=True) if predictions else pd.DataFrame()
    write_csv(output_dir / "streak_ml_metrics.csv", metric_frame)
    write_csv(output_dir / "streak_ml_predictions.csv", prediction_frame)
    summary = {
        "signal": signal,
        "trades": len(prepared.frame),
        "evaluation": "Expanding chronological train blocks; later blocks are never used to fit their predictions.",
        "targets": {
            "trade_win": "Whether the current trade wins.",
            "next_3_all_win": "Whether the current and next two trades all win.",
            "next_3_all_loss": "Whether the current and next two trades all lose.",
        },
        "feature_columns": prepared.numeric_features + prepared.categorical_features,
        "models": sorted(metric_frame["model"].unique().tolist()) if not metric_frame.empty else [],
        "metrics": metric_frame.to_dict(orient="records"),
    }
    write_json(output_dir / "streak_ml_summary.json", summary)
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=Path("streak_ml_backtest_output"))
    parser.add_argument("--signal", choices=("all", "prophet", "ml"), default="all")
    parser.add_argument("--min-train-trades", type=int, default=2000)
    parser.add_argument("--test-blocks", type=int, default=5)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.min_train_trades < 200 or args.test_blocks < 2:
        raise SystemExit("min-train-trades must be >= 200 and test-blocks must be >= 2")
    configure_logging()
    raw, source = read_artifact(args.input)
    signals = ("prophet", "ml") if args.signal == "all" else (args.signal,)
    reports = {}
    for signal in signals:
        reports[signal] = run_signal(raw, signal, args.output_dir / signal,
                                     args.min_train_trades, args.test_blocks)
    write_json(args.output_dir / "streak_ml_backtest_index.json", {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": str(source),
        "reports": reports,
    })
    LOG.info("Streak ML backtest complete: %s", args.output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
