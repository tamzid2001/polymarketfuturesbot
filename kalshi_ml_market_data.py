"""ML-only KXBTC15M clock, strike, and Coinbase candle helpers.

This module is deliberately independent of the legacy forecast runner.  The
live ML-side bot uses only these public market-data utilities plus its stored
classifier; it does not import, initialize, or call a forecasting model.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

import pandas as pd


ET = ZoneInfo("America/New_York")
SERIES_TICKER = "KXBTC15M"
MIN_CANDLES = 61
MAX_STALE_SECONDS = 600.0
_MONTHS = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}
_TICKER_RE = re.compile(
    r"^(?P<series>[A-Z0-9]+)-(?P<yy>\d{2})(?P<mon>[A-Z]{3})(?P<dd>\d{2})"
    r"(?P<hhmm>\d{4})-(?P<suffix>\d{2})$"
)


def build_ticker(series: str, settle_et: datetime) -> str:
    return (
        f"{series}-{settle_et.strftime('%y')}{settle_et.strftime('%b').upper()}"
        f"{settle_et.strftime('%d')}{settle_et.strftime('%H%M')}-{settle_et.strftime('%M')}"
    )


def parse_ticker(ticker: str) -> dict[str, Any] | None:
    match = _TICKER_RE.match(ticker)
    if match is None or match.group("mon") not in _MONTHS:
        return None
    hhmm = match.group("hhmm")
    settle_et = datetime(
        2000 + int(match.group("yy")), _MONTHS[match.group("mon")], int(match.group("dd")),
        int(hhmm[:2]), int(hhmm[2:]), tzinfo=ET,
    )
    suffix = match.group("suffix")
    return {
        "series": match.group("series"),
        "settle_et": settle_et,
        "suffix": suffix,
        "market_type": "relative" if suffix == "00" else "absolute",
    }


def current_and_next_tickers(series: str = SERIES_TICKER) -> tuple[str, str]:
    now_et = datetime.now(tz=ET)
    slot_minute = (now_et.minute // 15) * 15
    current_open = now_et.replace(minute=slot_minute, second=0, microsecond=0)
    current_settle = current_open + timedelta(minutes=15)
    return build_ticker(series, current_settle), build_ticker(series, current_settle + timedelta(minutes=15))


def seconds_until_ticker_settle(ticker: str) -> float | None:
    parsed = parse_ticker(ticker)
    if parsed is None:
        return None
    return (parsed["settle_et"] - datetime.now(tz=ET)).total_seconds()


def to_dollars(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if isinstance(value, str) and "." in value:
        return number
    if number.is_integer() and 1 <= number <= 100:
        return number / 100.0
    return number


def extract_target(market: Any) -> float | None:
    """Extract Kalshi's KXBTC15M strike without a forecast-runner import."""
    for name in ("floor_strike", "cap_strike", "functional_strike"):
        value = getattr(market, name, None)
        try:
            target = float(value)
        except (TypeError, ValueError):
            continue
        if target > 0:
            return target
    for name in ("yes_sub_title", "no_sub_title"):
        match = re.search(r"\$([0-9,]+(?:\.\d+)?)", str(getattr(market, name, "") or ""))
        if match is not None:
            return float(match.group(1).replace(",", ""))
    return None


def fetch_btc_1m() -> pd.DataFrame | None:
    """Fetch recent BTC-USD one-minute Coinbase candles for ML features."""
    end = pd.Timestamp.now(tz="UTC").floor("min")
    start = end - pd.Timedelta(minutes=100)
    query = urlencode({
        "granularity": 60,
        "start": start.isoformat().replace("+00:00", "Z"),
        "end": end.isoformat().replace("+00:00", "Z"),
    })
    request = Request(
        f"https://api.exchange.coinbase.com/products/BTC-USD/candles?{query}",
        headers={"Accept": "application/json", "User-Agent": "kalshi-ml-live/1.0"},
    )
    try:
        with urlopen(request, timeout=20) as response:  # noqa: S310 - fixed public Coinbase endpoint
            payload = json.loads(response.read().decode("utf-8"))
    except Exception:  # noqa: BLE001 - caller emits the contextual ML failure log
        return None
    if not isinstance(payload, list):
        return None
    rows: list[dict[str, Any]] = []
    for candle in payload:
        if not isinstance(candle, list) or len(candle) < 5:
            continue
        try:
            rows.append({"ds": pd.Timestamp(float(candle[0]), unit="s", tz="UTC"), "close": float(candle[4])})
        except (TypeError, ValueError):
            continue
    if not rows:
        return None
    return (
        pd.DataFrame(rows).dropna().drop_duplicates("ds", keep="last")
        .sort_values("ds").reset_index(drop=True)
    )


def validate_data(frame: pd.DataFrame | None) -> tuple[bool, str]:
    """Require a continuous, fresh one-minute history sufficient for the ML features."""
    if frame is None or len(frame) < MIN_CANDLES:
        return False, f"only {0 if frame is None else len(frame)} candles (<{MIN_CANDLES})"
    data = frame.copy()
    data["ds"] = pd.to_datetime(data["ds"], utc=True, errors="coerce")
    data["close"] = pd.to_numeric(data["close"], errors="coerce")
    data = data.dropna().sort_values("ds").drop_duplicates("ds", keep="last").reset_index(drop=True)
    if len(data) < MIN_CANDLES:
        return False, "insufficient valid one-minute candles"
    diffs = data["ds"].diff().dropna().dt.total_seconds()
    if not len(diffs) or float(diffs.median()) != 60.0 or bool((diffs != 60.0).any()):
        return False, "one-minute candle history has a gap"
    stale_seconds = (pd.Timestamp.now(tz="UTC") - data["ds"].iloc[-1]).total_seconds()
    if stale_seconds > MAX_STALE_SECONDS:
        return False, f"data stale: newest candle {stale_seconds:.0f}s old"
    return True, "ok"
