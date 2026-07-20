"""Standalone ML-side runner for live KXBTC15M markets.

The trade side comes only from the ML model's ``probability_yes``: YES at or
above 0.5, otherwise NO. It has no Prophet-side fallback, no Prophet-derived
feature, and no loss-streak switch.

Use a current ML-only feature ledger as ``--training-csv``. The runner trains
only on rows whose outcomes settled before the pre-open inference time. It is
inference-only by default; a real order requires all of:

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
import sys
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
from kalshi_ml_features import FEATURE_SCHEMA, ML_ONLY_FEATURE_COLUMNS, feature_values
from kalshi_ml_calibration import IsotonicCalibratedClassifier


LOG = logging.getLogger("kalshi_ml_inference_live")
DEFAULT_TRAINING_CSV = os.getenv("ML_TRAINING_CSV", "kxbtc15m_ml_only_feature_ledger.csv")
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
PREOPEN_DATA_TIMEOUT_S = float(os.getenv("ML_PREOPEN_DATA_TIMEOUT_S", "45"))
LEDGER_FORMAT_VERSION = 1


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
            "--training-csv with an ML-only feature ledger."
        )
    rows = pd.read_csv(path)
    required = set(ML_ONLY_FEATURE_COLUMNS) | {"actual_yes", "settlement_ts"}
    missing = required - set(rows.columns)
    if missing:
        raise ValueError(f"Training artifact missing columns: {', '.join(sorted(missing))}")
    rows = rows.copy()
    rows["settlement_timestamp"] = pd.to_datetime(rows["settlement_ts"], utc=True, errors="coerce", format="mixed")
    rows["actual_yes"] = pd.to_numeric(rows["actual_yes"], errors="coerce")
    for name in ML_ONLY_FEATURE_COLUMNS:
        rows[name] = pd.to_numeric(rows[name], errors="coerce")
    rows = rows[
        rows["settlement_timestamp"].notna()
        & (rows["settlement_timestamp"] < as_of)
        & rows["actual_yes"].isin([0, 1])
        & rows[ML_ONLY_FEATURE_COLUMNS].notna().all(axis=1)
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
    model.fit(rows[ML_ONLY_FEATURE_COLUMNS].to_numpy(dtype=float), rows["actual_yes"].to_numpy(dtype=int))
    return model


def load_saved_model(path: Path):
    """Load a model produced by kalshi_ml_model_train.py and verify its schema."""
    # Models created before the stable calibration module was introduced
    # pickle this wrapper as ``__main__.IsotonicCalibratedClassifier``.  Give
    # Python's current entry-point module that exact compatibility attribute
    # before unpickling, so the active production artifact loads safely. New
    # models use kalshi_ml_calibration.IsotonicCalibratedClassifier instead.
    main_module = sys.modules.get("__main__")
    if main_module is not None:
        setattr(main_module, "IsotonicCalibratedClassifier", IsotonicCalibratedClassifier)
    payload = joblib.load(path)
    if not isinstance(payload, dict) or payload.get("feature_columns") != ML_ONLY_FEATURE_COLUMNS:
        raise ValueError(f"Saved model {path} does not match the current ML feature schema")
    model = payload.get("model")
    if model is None or not hasattr(model, "predict_proba"):
        raise ValueError(f"Saved model {path} has no probability classifier")
    metadata = payload.get("metadata") or {}
    if metadata.get("feature_schema") != FEATURE_SCHEMA:
        raise ValueError(f"Saved model {path} is not the required Prophet-free ML-only schema")
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
    default = {
        "format_version": LEDGER_FORMAT_VERSION,
        "signals": {},
        "submitted_tickers": {},
    }
    if not path.exists():
        return default
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        LOG.warning("Cannot read %s; using a new ML inference state.", path)
        return default
    if not isinstance(payload, dict):
        return default
    payload.setdefault("format_version", LEDGER_FORMAT_VERSION)
    payload.setdefault("signals", {})
    payload.setdefault("submitted_tickers", {})
    return payload


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def settled_outcome(raw_market: Any) -> str | None:
    """Return Kalshi's final YES/NO result, if this market has settled."""
    value = getattr(raw_market, "result", None)
    text = str(getattr(value, "value", value) or "").strip().lower()
    if text in {"yes", "no"}:
        return text
    return None


async def reconcile_pending_signals(rest: kalshi.KalshiREST, state_path: Path) -> int:
    """Settle prior submitted and skipped signals without creating an order.

    A skip is evaluated as a counterfactual at its observed executable price:
    it records whether that specific contract would have won or lost before
    fees. This is observational evidence about the gate, not an estimate of
    executable P&L or proof that every skipped trade should have been taken.
    """
    state = load_state(state_path)
    signals = state.setdefault("signals", {})
    changed = 0
    for ticker, signal in signals.items():
        if not isinstance(signal, dict) or signal.get("settled_at"):
            continue
        market = await rest.get_market(str(ticker))
        if market is None:
            continue
        outcome = settled_outcome(market)
        if outcome is None:
            continue
        side = str(signal.get("side") or "").lower()
        price = signal.get("position_price")
        count = signal.get("count")
        signal["settled_at"] = datetime.now(tz=timezone.utc).isoformat()
        signal["actual_outcome"] = outcome
        if side in {"yes", "no"} and isinstance(price, (int, float)):
            correct = side == outcome
            gross_per_contract = (1.0 - float(price)) if correct else -float(price)
            signal["counterfactual_correct"] = correct
            signal["counterfactual_gross_per_contract"] = round(gross_per_contract, 6)
            if isinstance(count, (int, float)):
                signal["counterfactual_gross_total"] = round(gross_per_contract * float(count), 6)
            if str(signal.get("decision") or "").startswith("skipped"):
                signal["skip_observation"] = "avoided_realized_loss" if not correct else "foregone_realized_profit"
        changed += 1
        LOG.info(
            "ML LEDGER SETTLED | ticker=%s decision=%s outcome=%s counterfactual_correct=%s",
            ticker, signal.get("decision"), outcome, signal.get("counterfactual_correct"),
        )
    if changed:
        save_state(state_path, state)
    return changed


def record_signal(
    state: dict[str, Any],
    *,
    ticker: str,
    side: str,
    probability_yes: float,
    confidence: float,
    price: float | None,
    max_allowed: float | None,
    count: float,
    decision: str,
    reason: str,
) -> None:
    """Persist both submitted and skipped decisions for later settlement scoring."""
    signals = state.setdefault("signals", {})
    signals[ticker] = {
        "recorded_at": datetime.now(tz=timezone.utc).isoformat(),
        "side": side,
        "probability_yes": round(probability_yes, 6),
        "confidence": round(confidence, 6),
        "position_price": round(price, 4) if price is not None else None,
        "max_allowed_price": round(max_allowed, 4) if max_allowed is not None else None,
        "count": count,
        "decision": decision,
        "reason": reason,
        "dry_run": kalshi.DRY_RUN,
    }


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
    *,
    as_of: pd.Timestamp | None = None,
) -> dict[str, Any] | None:
    """Freeze historical labels, candles, and ML model at one causal instant.

    ``as_of`` is normally the instant this pre-open job starts.  The execution
    runner may also reconstruct a just-opened market after an Actions handoff;
    in that case it supplies the market-open timestamp and this function uses
    only candles and settled labels available strictly before that timestamp.
    """
    if as_of is None:
        as_of = pd.Timestamp.now(tz="UTC")
    else:
        as_of = pd.Timestamp(as_of)
        as_of = as_of.tz_localize("UTC") if as_of.tzinfo is None else as_of.tz_convert("UTC")
    rows = load_training_rows(training_csv, as_of)
    loop = asyncio.get_running_loop()
    try:
        candles = await asyncio.wait_for(
            loop.run_in_executor(None, kalshi.fetch_btc_1m), timeout=PREOPEN_DATA_TIMEOUT_S,
        )
    except TimeoutError:
        LOG.warning(
            "ML INPUT FAILED | %s BTC candle fetch exceeded %.0fs; no frozen signal.",
            target_ticker, PREOPEN_DATA_TIMEOUT_S,
        )
        return None
    valid, reason = kalshi.validate_data(candles)
    if not valid:
        LOG.warning("ML input data failed validation for %s: %s", target_ticker, reason)
        return None
    # A fetch can complete after the target opens.  Preserve the causal
    # snapshot by discarding every candle at or after the frozen timestamp.
    # This also lets a new Actions worker reconstruct a current decision from
    # the exact pre-open history instead of falling back to a non-ML side.
    candles = candles.copy()
    candles["ds"] = pd.to_datetime(candles["ds"], utc=True, errors="coerce")
    candles = candles[candles["ds"] < as_of].reset_index(drop=True)
    if len(candles) < 61:
        LOG.warning(
            "ML INPUT FAILED | %s has only %d candles strictly before frozen as_of=%s.",
            target_ticker, len(candles), as_of.isoformat(),
        )
        return None
    model = load_saved_model(model_path) if model_path is not None else train_model(rows)
    LOG.info(
        "ML INPUT READY | %s schema=%s rows=%d candles=%d as_of=%s; no Prophet or forecast input.",
        target_ticker, FEATURE_SCHEMA, len(rows), len(candles), as_of.isoformat(),
    )
    return {
        "target_ticker": target_ticker,
        "as_of": as_of,
        "rows": rows,
        "candles": candles,
        "model": model,
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
        cached["candles"], float(market["target"]), known_outcomes(cached["rows"]),
        next_open_timestamp(target_ticker),
    )
    vector = np.asarray([[float(features[name]) for name in ML_ONLY_FEATURE_COLUMNS]], dtype=float)
    probability_yes = float(cached["model"].predict_proba(vector)[0][1])
    side = "yes" if probability_yes >= 0.5 else "no"
    confidence = probability_yes if side == "yes" else 1.0 - probability_yes
    price = executable_position_price(market["raw_market"], side)
    count = kalshi.bet_count()

    LOG.info(
        "ML SIGNAL | ticker=%s side=%s p_yes=%.4f confidence=%.4f train_rows=%d strike=%.2f",
        target_ticker, side.upper(), probability_yes, confidence, len(cached["rows"]), float(market["target"]),
    )
    if confidence < MIN_CONFIDENCE:
        state = load_state(state_path)
        record_signal(
            state, ticker=target_ticker, side=side, probability_yes=probability_yes,
            confidence=confidence, price=price, max_allowed=None, count=count,
            decision="skipped_confidence", reason="score_below_minimum",
        )
        save_state(state_path, state)
        LOG.info("Confidence %.4f < %.4f — SKIP.", confidence, MIN_CONFIDENCE)
        return
    if price is None:
        state = load_state(state_path)
        record_signal(
            state, ticker=target_ticker, side=side, probability_yes=probability_yes,
            confidence=confidence, price=None, max_allowed=None, count=count,
            decision="skipped_price_unavailable", reason="no_executable_position_price",
        )
        save_state(state_path, state)
        LOG.warning("No executable %s ask price for %s — SKIP.", side.upper(), target_ticker)
        return
    max_allowed = min(MAX_ENTRY_PRICE, confidence - MIN_EDGE)
    if price > max_allowed:
        state = load_state(state_path)
        record_signal(
            state, ticker=target_ticker, side=side, probability_yes=probability_yes,
            confidence=confidence, price=price, max_allowed=max_allowed, count=count,
            decision="skipped_price", reason="executable_price_exceeds_model_edge_gate",
        )
        save_state(state_path, state)
        LOG.info("Price $%.4f exceeds $%.4f confidence/price gate — SKIP.", price, max_allowed)
        return

    state = load_state(state_path)
    submitted = state.setdefault("submitted_tickers", {})
    if target_ticker in submitted:
        signal = state.setdefault("signals", {}).setdefault(target_ticker, {})
        signal.setdefault("duplicate_attempts", []).append({
            "at": datetime.now(tz=timezone.utc).isoformat(),
            "side": side,
            "probability_yes": round(probability_yes, 6),
            "confidence": round(confidence, 6),
            "position_price": round(price, 4),
            "reason": "ticker_already_submitted_in_ledger",
        })
        save_state(state_path, state)
        LOG.info("Already submitted ML inference for %s — SKIP duplicate.", target_ticker)
        return
    if not submit:
        record_signal(
            state, ticker=target_ticker, side=side, probability_yes=probability_yes,
            confidence=confidence, price=price, max_allowed=max_allowed, count=count,
            decision="qualified_inference_only", reason="submission_not_requested",
        )
        save_state(state_path, state)
        LOG.info("ML signal passed all gates; inference-only mode, no order submitted.")
        return

    book_side, order_price = order_fields(side, price)
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
    record_signal(
        state, ticker=target_ticker, side=side, probability_yes=probability_yes,
        confidence=confidence, price=price, max_allowed=max_allowed, count=count,
        decision="submitted" if response is not None else "submission_failed",
        reason="order_submitted" if response is not None else "order_not_accepted",
    )
    save_state(state_path, state)
    LOG.info("ML order %s for %s.", "filled" if filled else "not filled", target_ticker)


async def run(args: argparse.Namespace) -> None:
    if args.submit and not kalshi.DRY_RUN and not args.allow_live:
        raise SystemExit("Refusing a real order: add --allow-live with --submit.")
    rest = kalshi.KalshiREST()
    try:
        reconciled = await reconcile_pending_signals(rest, args.state_file.expanduser())
        if reconciled:
            LOG.info("Reconciled %d prior ML ledger signal(s).", reconciled)
        if args.reconcile_only:
            return
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
    parser.add_argument(
        "--reconcile-only",
        action="store_true",
        help="Settle prior signal-ledger entries without evaluating or submitting a new market.",
    )
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
