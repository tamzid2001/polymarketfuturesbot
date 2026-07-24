"""Prophet-free, pre-open feature schema for KXBTC15M ML inference.

These features are deliberately limited to BTC candles available before the
market opens, the Kalshi strike, previously *settled* contract outcomes, and
the scheduled market-open time.  In particular, this module does not fit or
consume Prophet (or any other price forecast).
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd


ML_ONLY_FEATURE_COLUMNS = [
    "spot_vs_strike_bps",
    "return_1m_bps",
    "return_5m_bps",
    "return_15m_bps",
    "return_60m_bps",
    "vol_15m_bps",
    "vol_60m_bps",
    "range_15m_bps",
    "lag_outcome_1",
    "lag_outcome_2",
    "lag_outcome_4",
    "lag_outcome_8",
    "known_yes_rate_8",
    "known_outcome_count",
    "hour_sin",
    "hour_cos",
]
FEATURE_SCHEMA = "ml_only_raw_candles_settled_outcomes_v1"


def bps_change(end_value: float, start_value: float) -> float:
    """Return the proportional change in basis points."""
    return (end_value / start_value - 1.0) * 10_000.0


def feature_values(
    window: pd.DataFrame,
    strike: float,
    known_outcomes: list[int],
    market_open: pd.Timestamp,
) -> dict[str, float]:
    """Build the ML-only feature vector from data known before market open."""
    if len(window) < 61:
        raise ValueError("At least 61 one-minute candles are required")
    if not math.isfinite(float(strike)) or float(strike) <= 0.0:
        raise ValueError("strike must be a positive finite number")

    close = window["close"].astype(float).to_numpy()
    if not np.isfinite(close).all() or np.any(close <= 0.0):
        raise ValueError("candle close values must be positive and finite")
    spot = float(close[-1])
    returns = np.diff(np.log(close)) * 10_000.0
    recent_15 = close[-15:]
    latest_8 = known_outcomes[-8:]

    def lag(count: int) -> float:
        return float(known_outcomes[-count]) if len(known_outcomes) >= count else 0.5

    market_open = pd.Timestamp(market_open)
    if market_open.tzinfo is None:
        market_open = market_open.tz_localize("UTC")
    else:
        market_open = market_open.tz_convert("UTC")
    minutes = market_open.hour * 60 + market_open.minute
    radians = 2.0 * math.pi * minutes / (24.0 * 60.0)

    values = {
        "spot_vs_strike_bps": bps_change(spot, float(strike)),
        "return_1m_bps": bps_change(spot, float(close[-2])),
        "return_5m_bps": bps_change(spot, float(close[-6])),
        "return_15m_bps": bps_change(spot, float(close[-16])),
        "return_60m_bps": bps_change(spot, float(close[-61])),
        "vol_15m_bps": float(np.std(returns[-15:], ddof=0)),
        "vol_60m_bps": float(np.std(returns[-60:], ddof=0)),
        "range_15m_bps": bps_change(float(np.max(recent_15)), float(np.min(recent_15))),
        "lag_outcome_1": lag(1),
        "lag_outcome_2": lag(2),
        "lag_outcome_4": lag(4),
        "lag_outcome_8": lag(8),
        "known_yes_rate_8": float(np.mean(latest_8)) if latest_8 else 0.5,
        "known_outcome_count": float(len(known_outcomes)),
        "hour_sin": math.sin(radians),
        "hour_cos": math.cos(radians),
    }
    if set(values) != set(ML_ONLY_FEATURE_COLUMNS):
        raise AssertionError("ML-only feature schema and values diverged")
    return values
