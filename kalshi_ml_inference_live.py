"""Standalone ML-side runner for live KXBTC15M markets.

The trade side comes only from the ML model's ``probability_yes``: YES at or
above 0.5, otherwise NO. It has no Prophet-side fallback and no loss-streak
switch. The historical model's feature schema includes forecast-derived
features, which this runner recreates only as model inputs so its inference is
compatible with the validated walk-forward ML backtest.

Use a current ``prophet_ml_backtest_rows.csv`` as ``--training-csv``. The
runner trains only on rows whose outcomes settled before the pre-open inference
time. It is inference-only by default; a real order requires all of:

* ``DRY_RUN=false``;
* ``--submit``; and
* ``--allow-live``.

This code does not establish profitability. It uses a conservative confidence
and entry-price gate because directional accuracy alone ignores fills, spread,
fees, and slippage.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import joblib
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

import kalshibtc15minupordown as kalshi
from kalshi_btc15m_backtest import FEATURE_COLUMNS, feature_values


LOG = logging.getLogger("kalshi_ml_inference_live")
DEFAULT_TRAINING_CSV = os.getenv("ML_TRAINING_CSV", "prophet_ml_backtest_rows.csv")
DEFAULT_MODEL_PATH = os.getenv("ML_MODEL_PATH", "")
DEFAULT_STATE_FILE = os.getenv("ML_INFERENCE_STATE_FILE", "ml_inference_live_state.json")
MIN_TRAIN_ROWS = int(os.getenv("ML_MIN_TRAIN_ROWS", "1000"))
# This is a score gate selected by the robust historical test. It is not a
# substitute for the executable-price gate below, which remains mandatory.
MIN_CONFIDENCE = float(os.getenv("ML_MIN_CONFIDENCE", "0.52"))
MODEL_TYPE = os.getenv("ML_MODEL_TYPE", "hist_gradient_boosting")
MAX_ENTRY_PRICE = float(os.getenv("ML_MAX_ENTRY_PRICE", "0.50"))
MIN_EDGE = float(os.getenv("ML_MIN_EDGE", "0.03"))
PREOPEN_LEAD_S = float(os.getenv("ML_PREOPEN_LEAD_S", "120"))


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    for noisy in ("aiohttp", "asyncio", "cmdstanpy", "prophet", "yfinance"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def load_training_rows(path: Path, as_of: pd.Timestamp) -> pd.DataFrame:
    """Load fully settled historical rows available at the pre-open time."""
    if not path.exists():
        raise FileNotFoundError(
            f"Missing ML training artifact {path}. Set ML_TRAINING_CSV or pass "
            "--training-csv with prophet_ml_backtest_rows.csv."
        )
    rows = pd.read_csv(path)
    required = set(FEATURE_COLUMNS) | {"actual_yes", "settlement_ts"}
    missing = required - set(rows.columns)
    if missing:
        raise ValueError(f"Training artifact missing columns: {', '.join(sorted(missing))}")
    rows = rows.copy()
    rows["settlement_timestamp"] = pd.to_datetime(rows["settlement_ts"], utc=True, errors="coerce")
    rows["actual_yes"] = pd.to_numeric(rows["actual_yes"], errors="coerce")
    for name in FEATURE_COLUMNS:
        rows[name] = pd.to_numeric(rows[name], errors="coerce")
    rows = rows[
        rows["settlement_timestamp"].notna()
        & (rows["settlement_timestamp"] < as_of)
        & rows["actual_yes"].isin([0, 1])
        & rows[FEATURE_COLUMNS].notna().all(axis=1)
    ].sort_values("settlement_timestamp", kind="stable").reset_index(drop=True)
    if len(rows) < MIN_TRAIN_ROWS:
        raise ValueError(
            f"Only {len(rows)} settled rows before {as_of.isoformat()} "
            f"(< ML_MIN_TRAIN_ROWS={MIN_TRAIN_ROWS})"
        )
    if rows["actual_yes"].nunique() < 2:
        raise ValueError("Training rows contain only one outcome class")
    return rows


def train_model(rows: pd.DataFrame):
    if MODEL_TYPE == "hist_gradient_boosting":
        model = HistGradientBoostingClassifier(
            max_iter=150,
            learning_rate=0.05,
            max_leaf_nodes=8,
            min_samples_leaf=100,
            l2_regularization=10.0,
            random_state=0,
        )
    elif MODEL_TYPE == "logistic_regression":
        model = make_pipeline(
            StandardScaler(),
            LogisticRegression(C=0.25, max_iter=1000, class_weight="balanced", random_state=0),
        )
    else:
        raise ValueError(
            "ML_MODEL_TYPE must be 'hist_gradient_boosting' or 'logistic_regression'"
        )
    model.fit(rows[FEATURE_COLUMNS].to_numpy(dtype=float), rows["actual_yes"].to_numpy(dtype=int))
    return model


def load_saved_model(path: Path):
    """Load a model produced by kalshi_ml_model_train.py and verify its schema."""
    payload = joblib.load(path)
    if not isinstance(payload, dict) or payload.get("feature_columns") != FEATURE_COLUMNS:
        raise ValueError(f"Saved model {path} does not match the current ML feature schema")
    model = payload.get("model")
    if model is None or not hasattr(model, "predict_proba"):
        raise ValueError(f"Saved model {path} has no probability classifier")
    metadata = payload.get("metadata") or {}
    LOG.info("Loaded saved ML model %s (%s rows; settlement cutoff %s).",
             path, metadata.get("training_rows", "?"), metadata.get("settlement_cutoff", "?"))
    return model


def next_open_timestamp(ticker: str) -> pd.Timestamp:
    parsed = kalshi.parse_ticker(ticker)
    if not parsed or not parsed.get("settle_et"):
        raise ValueError(f"Cannot parse ticker {ticker}")
    settle = pd.Timestamp(parsed["settle_et"])
    return (settle - pd.Timedelta(minutes=15)).tz_convert("UTC")


def known_outcomes(rows: pd.DataFrame) -> list[int]:
    return rows["actual_yes"].astype(int).tolist()


def executable_position_price(raw_market: Any, side: str) -> float | None:
    """Return the best available ask cost for a YES or NO position."""
    fields = (
        ("yes_ask_dollars", "yes_ask") if side == "yes" else ("no_ask_dollars", "no_ask")
    )
    for field in fields:
        price = kalshi._to_dollars(getattr(raw_market, field, None))
        if price is not None and 0.01 <= float(price) <= 0.99:
            return float(price)
    if side == "no":
        yes_price = kalshi._to_dollars(getattr(raw_market, "last_price_dollars", None))
        if yes_price is None:
            yes_price = kalshi._to_dollars(getattr(raw_market, "last_price", None))
        if yes_price is not None and 0.01 <= float(yes_price) <= 0.99:
            return round(1.0 - float(yes_price), 4)
    return None


def order_fields(side: str, position_price: float) -> tuple[Any, str]:
    if side == "yes":
        return kalshi.BookSide.BID, f"{position_price:.4f}"
    return kalshi.BookSide.ASK, f"{1.0 - position_price:.4f}"


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"submitted_tickers": {}}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        LOG.warning("Cannot read %s; using a new ML inference state.", path)
        return {"submitted_tickers": {}}
    return payload if isinstance(payload, dict) else {"submitted_tickers": {}}


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")


async def wait_for_preopen() -> tuple[str, str]:
    while True:
        current_ticker, next_ticker = kalshi.current_and_next_tickers()
        seconds_to_open = kalshi.seconds_until_ticker_settle(current_ticker)
        if seconds_to_open is None:
            await asyncio.sleep(1)
        elif seconds_to_open <= PREOPEN_LEAD_S:
            return current_ticker, next_ticker
        else:
            await asyncio.sleep(min(30.0, seconds_to_open - PREOPEN_LEAD_S))


async def build_preopen_signal(
    training_csv: Path,
    target_ticker: str,
    model_path: Path | None,
) -> dict[str, Any] | None:
    """Freeze historical labels, candles, and ML model before the target opens."""
    as_of = pd.Timestamp.now(tz="UTC")
    rows = load_training_rows(training_csv, as_of)
    loop = asyncio.get_running_loop()
    candles = await loop.run_in_executor(None, kalshi.fetch_btc_1m)
    valid, reason = kalshi.validate_data(candles)
    if not valid:
        LOG.warning("ML input data failed validation for %s: %s", target_ticker, reason)
        return None
    forecast = await loop.run_in_executor(
        None, kalshi.run_prophet_forecast, candles, kalshi.FORECAST_MINUTES
    )
    if forecast is None:
        LOG.warning("Feature forecast failed for %s.", target_ticker)
        return None
    return {
        "target_ticker": target_ticker,
        "as_of": as_of,
        "rows": rows,
        "candles": candles,
        "forecast": forecast,
        "model": load_saved_model(model_path) if model_path is not None else train_model(rows),
    }


async def resolve_target_market(rest: kalshi.KalshiREST, target_ticker: str) -> dict[str, Any] | None:
    while True:
        market = await kalshi.resolve_active_market(rest)
        if market and market.get("ticker") == target_ticker and market.get("target") is not None:
            return market
        since_open = kalshi.seconds_since_ticker_open(target_ticker)
        if since_open is None or since_open > kalshi.OPEN_TRADE_GRACE_S:
            return None
        await asyncio.sleep(1)


async def score_and_maybe_submit(
    rest: kalshi.KalshiREST,
    cached: dict[str, Any],
    submit: bool,
    state_path: Path,
) -> None:
    target_ticker = str(cached["target_ticker"])
    current_ticker, _ = kalshi.current_and_next_tickers()
    seconds_to_open = kalshi.seconds_until_ticker_settle(current_ticker)
    if seconds_to_open is not None and seconds_to_open > 0:
        await asyncio.sleep(seconds_to_open)

    market = await resolve_target_market(rest, target_ticker)
    if market is None or not kalshi.is_within_open_trade_grace(
        kalshi.seconds_since_ticker_open(target_ticker)
    ):
        LOG.warning("Target %s was not live within the permitted entry window.", target_ticker)
        return

    features = feature_values(
        cached["candles"],
        float(market["target"]),
        cached["forecast"],
        known_outcomes(cached["rows"]),
        next_open_timestamp(target_ticker),
    )
    vector = np.asarray([[float(features[name]) for name in FEATURE_COLUMNS]], dtype=float)
    probability_yes = float(cached["model"].predict_proba(vector)[0][1])
    side = "yes" if probability_yes >= 0.5 else "no"
    confidence = probability_yes if side == "yes" else 1.0 - probability_yes
    price = executable_position_price(market["raw_market"], side)

    LOG.info(
        "ML SIGNAL | ticker=%s side=%s p_yes=%.4f confidence=%.4f train_rows=%d strike=%.2f",
        target_ticker, side.upper(), probability_yes, confidence, len(cached["rows"]), float(market["target"]),
    )
    if confidence < MIN_CONFIDENCE:
        LOG.info("Confidence %.4f < %.4f — SKIP.", confidence, MIN_CONFIDENCE)
        return
    if price is None:
        LOG.warning("No executable %s ask price for %s — SKIP.", side.upper(), target_ticker)
        return
    max_allowed = min(MAX_ENTRY_PRICE, confidence - MIN_EDGE)
    if price > max_allowed:
        LOG.info("Price $%.4f exceeds $%.4f confidence/price gate — SKIP.", price, max_allowed)
        return

    state = load_state(state_path)
    submitted = state.setdefault("submitted_tickers", {})
    if target_ticker in submitted:
        LOG.info("Already submitted ML inference for %s — SKIP duplicate.", target_ticker)
        return
    if not submit:
        LOG.info("ML signal passed all gates; inference-only mode, no order submitted.")
        return

    book_side, order_price = order_fields(side, price)
    count = kalshi.bet_count()
    response, filled = await kalshi._submit(
        rest,
        ticker=target_ticker,
        side=book_side,
        price=order_price,
        count=count,
        reduce_only=False,
        tag=f"ML {side.upper()} p_yes={probability_yes:.4f}",
    )
    submitted[target_ticker] = {
        "at": datetime.now(tz=timezone.utc).isoformat(),
        "side": side,
        "probability_yes": round(probability_yes, 6),
        "confidence": round(confidence, 6),
        "position_price": round(price, 4),
        "count": count,
        "filled": bool(filled),
        "dry_run": kalshi.DRY_RUN,
        "order_id": getattr(response, "order_id", None) if response is not None else None,
    }
    save_state(state_path, state)
    LOG.info("ML order %s for %s.", "filled" if filled else "not filled", target_ticker)


async def run(args: argparse.Namespace) -> None:
    if args.submit and not kalshi.DRY_RUN and not args.allow_live:
        raise SystemExit("Refusing a real order: add --allow-live with --submit.")
    rest = kalshi.KalshiREST()
    try:
        for _ in range(args.windows):
            _, target_ticker = await wait_for_preopen()
            model_path = args.model_path.expanduser() if args.model_path is not None else None
            cached = await build_preopen_signal(args.training_csv.expanduser(), target_ticker, model_path)
            if cached is not None:
                await score_and_maybe_submit(rest, cached, args.submit, args.state_file.expanduser())
            if args.windows > 1:
                await asyncio.sleep(1)
    finally:
        await rest.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--training-csv", type=Path, default=Path(DEFAULT_TRAINING_CSV))
    parser.add_argument(
        "--model-path",
        type=Path,
        default=Path(DEFAULT_MODEL_PATH) if DEFAULT_MODEL_PATH else None,
        help="Optional joblib model from kalshi_ml_model_train.py; omit to train at pre-open.",
    )
    parser.add_argument("--state-file", type=Path, default=Path(DEFAULT_STATE_FILE))
    parser.add_argument("--windows", type=int, default=1)
    parser.add_argument("--submit", action="store_true", help="Submit a gated order; default is infer only.")
    parser.add_argument("--allow-live", action="store_true", help="Required with --submit when DRY_RUN=false.")
    args = parser.parse_args()
    if args.windows < 1:
        parser.error("--windows must be at least 1")
    if not 0.5 <= MIN_CONFIDENCE <= 1.0:
        parser.error("ML_MIN_CONFIDENCE must be between 0.5 and 1.0")
    if not (0.01 <= MAX_ENTRY_PRICE <= 0.99 and 0.0 <= MIN_EDGE < 1.0):
        parser.error("ML_MAX_ENTRY_PRICE or ML_MIN_EDGE is outside its valid range")
    return args


def main() -> None:
    configure_logging()
    asyncio.run(run(parse_args()))


if __name__ == "__main__":
    main()
