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


def _require_verified_starting_balance(starting_balance: float | None, source: str) -> float:
    """Return an explicitly supplied opening balance.

    A realized-P/L export has no account-balance snapshots.  Reconstructing an
    *absolute* balance curve therefore requires a verified balance immediately
    before its first included trade; silently substituting $100 creates a P/L
    index, not equity.
    """

    if starting_balance is None:
        raise ValueError(
            "Cannot construct an absolute balance curve because the account "
            f"balance before the first {source} trade is unknown. Supply "
            "--starting-balance with a verified historical balance."
        )
    if not np.isfinite(starting_balance) or starting_balance <= 0:
        raise ValueError("starting_balance must be a positive verified dollar balance")
    return float(starting_balance)


def load_kalshi_positions(path: str | Path, starting_balance: float | None) -> pd.DataFrame:
    """Read original Kalshi closed positions and return chronological trade P/L.

    Only byte-for-byte-equivalent economic records are removed.  Two rows that
    share a ticker but differ in cost, P/L, position, side, or update time are
    retained and subsequently rejected as ambiguous duplicate timestamps.
    """

    opening_balance = _require_verified_starting_balance(starting_balance, "Kalshi")
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
    result["shadow_equity_after"] = opening_balance + result["trade_pnl"].cumsum()
    result["balance_before_first_trade"] = opening_balance
    result["balance_source"] = "reconstructed_from_verified_starting_balance"
    return result


def load_equity_curve(path: str | Path, starting_balance: float | None) -> pd.DataFrame:
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

    opening_balance = _require_verified_starting_balance(starting_balance, "equity-curve")
    # Preserve the absolute values exactly as supplied.  A rolling lookback
    # may later select a subset of these rows, but it must never turn them
    # into a $100 + cumulative-P/L index.  If the first point is an explicit
    # opening-balance snapshot it is not an opportunity and is removed only
    # when the user supplied that exact verified balance.
    if np.isclose(float(clean.iloc[0]["y"]), opening_balance):
        clean = clean.iloc[1:].copy()
    if clean.empty:
        raise ValueError("Equity CSV contains only the opening-balance snapshot")

    pnl = clean["y"].diff()
    pnl.iloc[0] = clean.iloc[0]["y"] - opening_balance
    result = pd.DataFrame(
        {
            "ds": clean["ds"].to_numpy(),
            "ticker": clean.get("ticker", pd.Series([pd.NA] * len(clean))).to_numpy(),
            "trade_pnl": pnl.astype(float).to_numpy(),
            "shadow_equity_after": clean["y"].astype(float).to_numpy(),
            "balance_before_first_trade": opening_balance,
            "balance_source": "absolute_equity_csv",
        }
    )
    _ensure_unique_timestamps(result, "equity input")
    return result.reset_index(drop=True)


def load_input(path: str | Path, input_format: str, starting_balance: float | None) -> pd.DataFrame:
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


def most_recent_trades(trades: pd.DataFrame, max_trades: int, starting_balance: float | None = None) -> pd.DataFrame:
    """Keep the newest bounded trade universe without rebasing balances.

    ``max_trades`` controls only which complete observations Prophet sees. It
    never changes the supplied absolute balance values.  The retained window's
    opening balance is the actual balance immediately before its first trade.
    """

    if max_trades < 1:
        raise ValueError("max_trades must be positive")
    result = trades.tail(max_trades).copy().reset_index(drop=True)
    result["trade_index"] = np.arange(len(result), dtype=int)
    opening_balance = float(result.iloc[0]["shadow_equity_after"] - result.iloc[0]["trade_pnl"])
    result["balance_before_first_trade"] = opening_balance
    if (result["shadow_equity_after"] <= 0).any():
        raise ValueError("The selected recent-trade shadow equity is non-positive")
    return result


def data_signature(trades: pd.DataFrame) -> str:
    """Stable input identity used to keep cached sensitivity forecasts honest."""

    data = trades[["ds", "trade_pnl", "shadow_equity_after"]].to_csv(index=False).encode("utf-8")
    return hashlib.sha256(data).hexdigest()[:16]
