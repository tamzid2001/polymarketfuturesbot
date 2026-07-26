"""Input parsing with explicit UTC handling and conservative deduplication."""

from __future__ import annotations

import hashlib
import re
from pathlib import Path

import numpy as np
import pandas as pd

KALSHI_TICKER_RE = re.compile(r"KXBTC15M-(\d{2})([A-Z]{3})(\d{2})(\d{2})(\d{2})-(\d{2})")
RETURN_CANDIDATES = (
    "Total return ($)",
    "Total return",
    "Total Return ($)",
    "Total Return",
)


def _utc_naive(values: pd.Series) -> pd.Series:
    """Parse timestamps as UTC, then remove only the timezone metadata."""

    return pd.to_datetime(values, errors="coerce", utc=True).dt.tz_convert(None)


def parse_kalshi_ticker_timestamp(tickers: pd.Series) -> pd.Series:
    """Parse KXBTC15M ticker timestamps as UTC clock time.

    For example ``KXBTC15M-26JUL221300-00`` becomes
    ``2026-07-22 13:00:00``.  The final group is seconds, not a timezone.
    """

    parts = tickers.astype(str).str.extract(KALSHI_TICKER_RE)
    parts.columns = ["year", "month", "day", "hour", "minute", "second"]
    text = parts.agg("".join, axis=1)
    return pd.to_datetime(text, format="%y%b%d%H%M%S", errors="coerce", utc=True).dt.tz_convert(None)


def _numeric_dollars(values: pd.Series) -> pd.Series:
    return pd.to_numeric(
        values.astype(str).str.replace(r"[^0-9.\-]", "", regex=True),
        errors="coerce",
    )


def _ensure_unique_timestamps(frame: pd.DataFrame, source: str) -> None:
    duplicates = frame.loc[frame["ds"].duplicated(keep=False), ["ds", "ticker"]]
    if not duplicates.empty:
        preview = duplicates.head(10).to_dict("records")
        raise ValueError(
            f"{source} has duplicate trade timestamps after preprocessing. "
            f"They are not silently collapsed because that can hide distinct trades: {preview}"
        )


def load_kalshi_positions(path: str | Path, starting_balance: float) -> pd.DataFrame:
    """Read original Kalshi closed positions and return chronological trade P/L.

    Only byte-for-byte-equivalent economic records are removed.  Two rows that
    share a ticker but differ in cost, P/L, position, side, or update time are
    retained and subsequently rejected as ambiguous duplicate timestamps.
    """

    raw = pd.read_csv(path)
    raw.columns = raw.columns.str.strip()
    if "Ticker" not in raw:
        raise KeyError(f"Missing 'Ticker'; available columns: {raw.columns.tolist()}")
    return_column = next((name for name in RETURN_CANDIDATES if name in raw.columns), None)
    if return_column is None:
        raise KeyError(
            "Could not find realized return column; available columns: "
            f"{raw.columns.tolist()}"
        )

    # Do not infer that rows sharing a ticker are duplicates: partial fills and
    # later corrections may share it.  Only an identical full source record is
    # unquestionably safe to remove.
    raw = raw.drop_duplicates(keep="last").copy()
    raw["ds"] = parse_kalshi_ticker_timestamp(raw["Ticker"])
    raw["trade_pnl"] = _numeric_dollars(raw[return_column])
    clean = raw.loc[raw["ds"].notna() & raw["trade_pnl"].notna()].copy()
    if clean.empty:
        raise ValueError("No valid Kalshi records remained after ticker/P&L parsing")

    result = pd.DataFrame(
        {
            "ds": clean["ds"],
            "ticker": clean["Ticker"].astype(str),
            "trade_pnl": clean["trade_pnl"].astype(float),
        }
    ).sort_values("ds", kind="stable").reset_index(drop=True)
    _ensure_unique_timestamps(result, "Kalshi input")
    result["shadow_equity_after"] = starting_balance + result["trade_pnl"].cumsum()
    return result


def load_equity_curve(path: str | Path, starting_balance: float) -> pd.DataFrame:
    """Read a ``ds,y`` equity curve and recover realized per-trade P/L."""

    raw = pd.read_csv(path)
    raw.columns = raw.columns.str.strip()
    required = {"ds", "y"}
    if not required.issubset(raw.columns):
        raise KeyError(f"Equity CSV requires {required}; available columns: {raw.columns.tolist()}")
    raw["ds"] = _utc_naive(raw["ds"])
    raw["y"] = pd.to_numeric(raw["y"], errors="coerce")
    clean = raw.dropna(subset=["ds", "y"]).sort_values("ds", kind="stable").copy()
    clean = clean.drop_duplicates(subset=["ds", "y"], keep="last").reset_index(drop=True)
    if clean.empty:
        raise ValueError("No valid rows remained in the equity CSV")
    if clean["ds"].duplicated().any():
        raise ValueError("Equity CSV has conflicting duplicate timestamps and cannot be safely replayed")

    # A common exported format begins with an explicit $100 starting row.  It
    # is an observation, not a trade, so do not turn it into a zero-P/L trade.
    if np.isclose(float(clean.iloc[0]["y"]), starting_balance):
        clean = clean.iloc[1:].copy()
    if clean.empty:
        raise ValueError("Equity CSV contains only the starting-balance row")

    pnl = clean["y"].diff()
    pnl.iloc[0] = clean.iloc[0]["y"] - starting_balance
    result = pd.DataFrame(
        {
            "ds": clean["ds"].to_numpy(),
            "ticker": clean.get("ticker", pd.Series([pd.NA] * len(clean))).to_numpy(),
            "trade_pnl": pnl.astype(float).to_numpy(),
            "shadow_equity_after": clean["y"].astype(float).to_numpy(),
        }
    )
    _ensure_unique_timestamps(result, "equity input")
    return result.reset_index(drop=True)


def load_input(path: str | Path, input_format: str, starting_balance: float) -> pd.DataFrame:
    if input_format == "kalshi":
        result = load_kalshi_positions(path, starting_balance)
    elif input_format == "equity":
        result = load_equity_curve(path, starting_balance)
    else:
        raise ValueError("input_format must be 'kalshi' or 'equity'")
    if (result["shadow_equity_after"] <= 0).any():
        raise ValueError("All shadow balances must be positive for the default log transform")
    result.insert(0, "trade_index", np.arange(len(result), dtype=int))
    return result


def data_signature(trades: pd.DataFrame) -> str:
    """Stable input identity used to keep cached sensitivity forecasts honest."""

    data = trades[["ds", "trade_pnl", "shadow_equity_after"]].to_csv(index=False).encode("utf-8")
    return hashlib.sha256(data).hexdigest()[:16]
