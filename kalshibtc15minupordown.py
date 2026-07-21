"""
kalshibtc15minupordown.py
─────────────────────────────────────────────────────────────────────────────
BTC 15-min Kalshi market algo-trader  (fully ASYNC) — PROPHET forecast strategy.

STRATEGY  (Prophet 15-minute BTC forecast)
──────────────────────────────────────────
  Two minutes before each new Kalshi KXBTC15M window opens (for example, at
  xx:43 for the xx:45 market open) the bot:

    1. Builds the next KXBTC15M ticker and pre-computes the BTC settlement
       forecast for that upcoming market.
    2. Downloads the latest 500 one-minute BTC/USD candles (Yahoo Finance,
       symbol BTC-USD) and validates them (1-min spacing, fresh/not stale,
       required 15-min boundary timestamps present). Bad/stale data → SKIP.
    3. Fits Facebook Prophet on log(close) and forecasts to the upcoming
       market's settlement. The cached forecast always uses 17 one-minute
       timesteps forward. The settlement median (yhat, back-transformed with
       exp) is the "p50".
    4. As soon as the new market is live, detects its live strike
       (floor_strike) and locks ONE side for the full window:
            p50 forecast  >  live strike   →  BUY YES  (UP)
            p50 forecast  <  live strike   →  BUY NO   (DOWN)
    5. Immediately pre-posts four same-side, market-close-expiring GTC limits
       at the fixed economic costs $0.40, $0.30, $0.20, and $0.10.  The
       opposite side is never submitted, and the four rungs use the same fixed
       BET_AMOUNT_SHARES count.  Remaining orders are canceled at close.
    6. Records BTC fills and lets positions ride to settlement. There is no
       ETH contract, hedge, loss progression, multiplier, or take-profit path.

  Exactly one BTC entry per 15-minute window (never re-enters the same BTC
  contract). Prophet also yields the 80% confidence interval (p10/p90) around
  p50; the forecast-time BTC close and live strike percentile positions within
  it are logged/stored.

DATA SOURCES
────────────
  • Yahoo Finance (yfinance) → 1-minute BTC-USD OHLC history (24/7)
  • Kalshi market WebSocket  → live ticker/trade for the active contract
  • Kalshi REST (kalshi-python-async, V2) → market metadata (strike/result),
                               balance, GTC limit orders

TICKER FORMAT  (US EASTERN time, auto-DST — NOT UTC)
─────────────────────────────────────────────────────
  Pattern : {SERIES}-{YY}{MON}{DD}{HHMM}-{MM}
  Example : KXBTC15M-26JUN271145-45   (settles 11:45 ET)

KALSHI ASYNC SDK NOTES (kalshi-python-async ≥ 3.22, Python ≥ 3.13)
──────────────────────────────────────────────────────────────────
  • Auth   : config.api_key_id + config.private_key_pem → KalshiClient(config)
  • Balance: await PortfolioApi(client).get_balance()
  • Market : await MarketApi(client).get_market(ticker)  (.market.floor_strike/.result)
  • Order  : await OrdersApi(client).create_order_v2(... BookSide.BID|ASK ...)
             BID → buy YES; ASK → buy NO (sell YES).

CREDENTIALS / SETTINGS (env vars)
─────────────────────────────────
    KALSHI_API_KEY_ID / KALSHI_PEM_PATH (or KALSHI_PRIVATE_KEY content)
    KALSHI_DEMO            "true" for sandbox (default false)
    DRY_RUN               "true" (default) — log orders, do not submit
    BET_AMOUNT_SHARES     BTC contracts (shares) per each of the four fixed
                          same-side ladder rungs, fractional at 0.01 granularity
                          (default 1; not dollars)
    HISTORY_MINUTES       1-min candles pulled per forecast (default 500)
    FORECAST_MINUTES      fixed Prophet forecast horizon in minutes (default 17)
    PREOPEN_FORECAST_LEAD_S
                          seconds before the next window opens to pre-compute
                          its settlement forecast (default 120)
    OPEN_TRADE_GRACE_S    max seconds after a window opens to start the fresh
                          locked-side ladder (default 45; never start late)
    UNCERTAINTY_SAMPLES   Prophet uncertainty samples (default 1000)
    DATA_MAX_STALE_S      max age of newest candle before SKIP (default 600)
    YF_PERIOD             yfinance download period (default "2d")
    RUNTIME_LIMIT_MIN     clean-exit after N minutes (default 345 = 5h45m)
    REPORT_INTERVAL_S     performance/portfolio report cadence (default 30)
    TRADE_HISTORY_FILE    BTC-only ladder journal (default prophet_btc_only_trade_history.json)
    TRADED_TICKERS_FILE   BTC-only per-window dedupe store
                          (default prophet_btc_only_traded_market_tickers.json)
    KALSHI_WS_VERBOSE     "true" — log every Kalshi WS message
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import aiohttp
import numpy as np
import pandas as pd
import yfinance as yf
from prophet import Prophet

from kalshi_python_async import (
    BookSide,
    Configuration,
    CreateOrderV2Request,
    EventsApi,
    KalshiAuth,
    KalshiClient,
    MarketApi,
    OrdersApi,
    PortfolioApi,
    SelfTradePreventionType,
)

# Kalshi KXBTC15M tickers are denominated in US EASTERN time (auto-DST), NOT UTC.
ET = ZoneInfo("America/New_York")

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
KALSHI_API_KEY_ID = os.getenv("KALSHI_API_KEY_ID", "")
KALSHI_PEM_PATH   = os.getenv("KALSHI_PEM_PATH",   "kalshi_private_key.pem")
KALSHI_DEMO       = os.getenv("KALSHI_DEMO", "false").lower() in ("1", "true", "yes")
DRY_RUN           = os.getenv("DRY_RUN", "true").lower() in ("1", "true", "yes")
# The inverse Prophet experiment is deliberately confined to DRY_RUN. It
# creates no Kalshi order and is not a switch for live opposite-side trading.
INVERSE_PROPHET_SHADOW_ENABLED = (
    DRY_RUN and os.getenv("INVERSE_PROPHET_SHADOW_ENABLED", "true").lower()
    in ("1", "true", "yes")
)
# The selector is a third, independently paper-filled ladder during a dry run.
# It freezes its side before a market opens from *previously settled* paired
# Prophet/inverse outcomes.  It never looks at the market it is about to trade.
# In a deliberately-confirmed live workflow it instead supplies the one actual
# locked side; the inverse shadow remains paper-only in every mode.
PROPHET_SELECTOR_ENABLED = os.getenv("PROPHET_SELECTOR_ENABLED", "true").lower() in (
    "1", "true", "yes")
PROPHET_SELECTOR_WINDOWS = (3, 5, 7, 10, 25, 50)
# Explicit deployment bootstrap: the first selector market is inverse even if
# a pre-existing baseline ledger is present.  Later markets use the frozen
# trailing-window vote; the override is itself recorded in that first snapshot.
PROPHET_SELECTOR_START_INVERSE = os.getenv("PROPHET_SELECTOR_START_INVERSE", "true").lower() in (
    "1", "true", "yes")
PROPHET_SELECTOR_HISTORY_FILE = os.getenv(
    "PROPHET_SELECTOR_HISTORY_FILE", "prophet_btc_selector_history.json")
PROPHET_SELECTOR_REPORT_FILE = os.getenv(
    "PROPHET_SELECTOR_REPORT_FILE", "prophet_btc_selector_report.json")

# ── Bet sizing ──────────────────────────────────────────────────────────────
# Buy exactly this many contracts (shares) per ladder rung, independent of price.
# Kalshi's fixed-point API supports FRACTIONAL contract counts at 0.01
# granularity (count_fp, sent as a 0–2 decimal string), so 0.01 — the exchange
# minimum — is a valid order. See docs.kalshi.com fixed_point_migration.
BET_AMOUNT_SHARES = float(os.getenv("BET_AMOUNT_SHARES", "1"))
LADDER_LEVELS = (0.40, 0.30, 0.20, 0.10)

# ── Prophet / data settings ───────────────────────────────────────────────────
HISTORY_MINUTES     = int(float(os.getenv("HISTORY_MINUTES", "500")))
FORECAST_MINUTES    = int(float(os.getenv("FORECAST_MINUTES", "17")))
UNCERTAINTY_SAMPLES = int(float(os.getenv("UNCERTAINTY_SAMPLES", "1000")))
DATA_MAX_STALE_S    = float(os.getenv("DATA_MAX_STALE_S", "600"))   # newest candle age
YF_PERIOD           = os.getenv("YF_PERIOD", "2d")
PREOPEN_FORECAST_LEAD_S = float(os.getenv("PREOPEN_FORECAST_LEAD_S", "120"))
OPEN_TRADE_GRACE_S      = float(os.getenv("OPEN_TRADE_GRACE_S", "45"))

RUNTIME_LIMIT_MIN = float(os.getenv("RUNTIME_LIMIT_MIN", "345"))
REPORT_INTERVAL_S = float(os.getenv("REPORT_INTERVAL_S", "30"))    # report cadence
POLL_INTERVAL_S   = float(os.getenv("POLL_INTERVAL_S", "5"))       # window-watch cadence
SETTLE_CHECK_S    = float(os.getenv("SETTLE_CHECK_S", "2"))        # settlement poll cadence
STRIKE_RETRIES    = int(float(os.getenv("STRIKE_RETRIES", "8")))   # strike-resolution retries
KALSHI_WS_VERBOSE = os.getenv("KALSHI_WS_VERBOSE", "false").lower() in ("1", "true", "yes")
# A dry fill may use only a recently received top-of-book quote.  A short
# default intentionally errs on the side of leaving a paper rung unfilled when
# the WebSocket has gone quiet or disconnected.
DRY_QUOTE_MAX_AGE_S = max(0.1, float(os.getenv("DRY_QUOTE_MAX_AGE_S", "3")))

TRADE_HISTORY_FILE  = os.getenv("TRADE_HISTORY_FILE", "prophet_btc_only_trade_history.json")
TRADED_TICKERS_FILE = os.getenv("TRADED_TICKERS_FILE", "prophet_btc_only_traded_market_tickers.json")
INVERSE_PROPHET_SHADOW_HISTORY_FILE = os.getenv(
    "INVERSE_PROPHET_SHADOW_HISTORY_FILE", "prophet_btc_inverse_shadow_history.json")
INVERSE_PROPHET_SHADOW_REPORT_FILE = os.getenv(
    "INVERSE_PROPHET_SHADOW_REPORT_FILE", "prophet_btc_inverse_shadow_report.json")

ORDER_TIF      = "good_till_canceled"    # resting limit through market close
SERIES_TICKER  = "KXBTC15M"
YF_SYMBOL      = os.getenv("BTC_YF_SYMBOL", "BTC-USD")

KALSHI_BASE_URL = (
    "https://demo-api.kalshi.co/trade-api/v2"
    if KALSHI_DEMO
    else "https://api.elections.kalshi.com/trade-api/v2"
)
KALSHI_WS_URL = os.getenv(
    "KALSHI_WS_URL",
    "wss://demo-api.kalshi.co/trade-api/ws/v2"
    if KALSHI_DEMO
    else "wss://api.elections.kalshi.com/trade-api/ws/v2",
)

# Quantile band labels ↔ fractions — the 80% confidence interval around p50.
_QMAP = [("p10", 0.10), ("p50", 0.50), ("p90", 0.90)]


def bet_count(bet_shares: Optional[float] = None) -> float:
    """Contract count for one BTC ladder rung, independent of economic price.
    Kalshi's fixed-point API accepts fractional
    counts at 0.01 granularity, so the count is floored to 0.01 steps and
    clamped to the 0.01 exchange minimum."""
    bet = BET_AMOUNT_SHARES if bet_shares is None else float(bet_shares)
    return max(0.01, math.floor(bet * 100 + 1e-6) / 100.0)


# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
for _noisy in ("aiohttp", "asyncio", "cmdstanpy", "prophet", "yfinance"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)
log = logging.getLogger("kalshi_btc_bot")


# ─────────────────────────────────────────────────────────────────────────────
# Shared state
# ─────────────────────────────────────────────────────────────────────────────
# Live Kalshi WS quotes per ticker (main-loop only).
kalshi_quotes: dict = {}

# Windows handled this run (in-memory) — prevents re-running within a run even
# when a window is skipped for bad data. Persisted actual trades live in the
# PerformanceTracker's traded-tickers store (survives restarts).
handled_windows: set = set()

# Pre-open Prophet forecasts keyed by the ticker that will become live next.
# Example: at xx:44, cache the forecast for the xx:45-open / xx:00-settle ticker;
# at xx:45, compare that cached p50 with the newly-live market strike.
preopen_forecasts: dict = {}

# Running tallies (main-loop only)
trades_placed: int = 0      # orders that reached the exchange (live)
buys_placed:   int = 0
closes_placed: int = 0
fills_count:   int = 0      # orders that actually filled (fill_count > 0)
start_balance: Optional[float] = None   # Kalshi cash balance at startup


# ─────────────────────────────────────────────────────────────────────────────
# Ticker helpers (US Eastern time)
# ─────────────────────────────────────────────────────────────────────────────
_MONTHS = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY":  5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}
_TICKER_RE = re.compile(
    r"^(?P<series>[A-Z0-9]+)"
    r"-(?P<yy>\d{2})(?P<mon>[A-Z]{3})(?P<dd>\d{2})(?P<hhmm>\d{4})"
    r"-(?P<suffix>\d{2})$"
)


def build_ticker(series: str, settle_et: datetime) -> str:
    return (f"{series}-{settle_et.strftime('%y')}{settle_et.strftime('%b').upper()}"
            f"{settle_et.strftime('%d')}{settle_et.strftime('%H%M')}-"
            f"{settle_et.strftime('%M')}")


def parse_ticker(ticker: str) -> Optional[dict]:
    m = _TICKER_RE.match(ticker)
    if not m:
        return None
    mon_num = _MONTHS.get(m.group("mon"))
    if mon_num is None:
        return None
    hhmm = m.group("hhmm")
    settle = datetime(2000 + int(m.group("yy")), mon_num, int(m.group("dd")),
                      int(hhmm[:2]), int(hhmm[2:]), tzinfo=ET)
    suffix = m.group("suffix")
    return {"series": m.group("series"), "settle_et": settle, "suffix": suffix,
            "market_type": "relative" if suffix == "00" else "absolute"}


def current_and_next_tickers(series: str = SERIES_TICKER) -> tuple:
    """Current & next 15-min KXBTC15M tickers, in US EASTERN time."""
    now_et     = datetime.now(tz=ET)
    slot_min   = (now_et.minute // 15) * 15
    current_dt = now_et.replace(minute=slot_min, second=0, microsecond=0)
    current_settle = current_dt + timedelta(minutes=15)
    next_settle    = current_settle + timedelta(minutes=15)
    return build_ticker(series, current_settle), build_ticker(series, next_settle)


def seconds_until_ticker_settle(ticker: str) -> Optional[float]:
    p = parse_ticker(ticker)
    if not p or not p.get("settle_et"):
        return None
    return (p["settle_et"] - datetime.now(tz=ET)).total_seconds()


def seconds_since_ticker_open(ticker: str) -> Optional[float]:
    p = parse_ticker(ticker)
    if not p or not p.get("settle_et"):
        return None
    open_et = p["settle_et"] - timedelta(minutes=15)
    return (datetime.now(tz=ET) - open_et).total_seconds()


def is_within_open_trade_grace(seconds_since_open: Optional[float]) -> bool:
    """True only during the configured first seconds of a live window."""
    return (seconds_since_open is not None
            and 0.0 <= seconds_since_open <= OPEN_TRADE_GRACE_S)


def log_next_ticker_prediction() -> str:
    _, nxt = current_and_next_tickers()
    p = parse_ticker(nxt)
    log.info("⏭  NEXT 15-MIN UP/DOWN TICKER PREDICTION: %s  (settles %s ET, type=%s)",
             nxt, p["settle_et"].strftime("%H:%M") if p else "?",
             p["market_type"] if p else "?")
    return nxt


# ─────────────────────────────────────────────────────────────────────────────
# BTC 1-minute history (Yahoo Finance, 24/7) + validation
# ─────────────────────────────────────────────────────────────────────────────
def fetch_btc_1m() -> Optional[pd.DataFrame]:
    """Download the latest 1-minute BTC/USD candles (blocking — run in executor).

    Returns a DataFrame with tz-aware UTC 'ds' and float 'close', trimmed to the
    last HISTORY_MINUTES rows, or None on failure/empty data.
    """
    try:
        raw = yf.download(YF_SYMBOL, period=YF_PERIOD, interval="1m",
                          progress=False, auto_adjust=False)
    except Exception as exc:  # noqa: BLE001
        log.error("yfinance download failed: %s", exc)
        return None
    if raw is None or len(raw) == 0:
        log.warning("yfinance returned no data for %s", YF_SYMBOL)
        return None

    # yfinance can return MultiIndex columns; flatten if needed.
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = raw.columns.get_level_values(0)
    raw = raw.dropna()
    if "Close" not in raw.columns:
        log.warning("'Close' not in yfinance columns: %s", list(raw.columns))
        return None

    df = raw.reset_index()
    tcol = next((c for c in ("Datetime", "Date", "index") if c in df.columns),
                df.columns[0])
    df = df[[tcol, "Close"]].rename(columns={tcol: "ds", "Close": "close"})
    df["ds"] = pd.to_datetime(df["ds"], utc=True, errors="coerce")
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna().sort_values("ds").reset_index(drop=True)
    if len(df) > HISTORY_MINUTES:
        df = df.tail(HISTORY_MINUTES).reset_index(drop=True)
    return df


def validate_data(df: Optional[pd.DataFrame]) -> tuple:
    """Verify the candles are usable. Returns (ok: bool, reason: str).

    Checks: enough rows, ~1-minute spacing, freshness (not stale), and that the
    required 15-min boundary timestamps (…:00/:15/:30/:45) are all present.
    BTC trades 24/7, so no weekday/session assumptions are made.
    """
    if df is None or len(df) < HISTORY_MINUTES:
        return False, f"only {0 if df is None else len(df)} candles (<{HISTORY_MINUTES})"

    diffs = df["ds"].diff().dropna().dt.total_seconds()
    med = float(diffs.median()) if len(diffs) else 0.0
    if not (55.0 <= med <= 65.0):
        return False, f"candle spacing median {med:.0f}s ≠ 60s (not clean 1-minute data)"

    now = pd.Timestamp.now(tz="UTC")
    last = df["ds"].iloc[-1]
    stale = (now - last).total_seconds()
    if stale > DATA_MAX_STALE_S:
        return False, f"data stale: newest candle {stale:.0f}s old (> {DATA_MAX_STALE_S:.0f}s)"

    # Required 15-minute boundary timestamps within the covered range.
    minute_set = set(df["ds"].dt.floor("min"))
    first_b = df["ds"].iloc[0].ceil("15min")
    last_b = df["ds"].iloc[-1].floor("15min")
    missing = []
    b = first_b
    while b <= last_b:
        if b not in minute_set:
            missing.append(b)
        b += pd.Timedelta(minutes=15)
    if missing:
        eg = missing[0].strftime("%Y-%m-%d %H:%M UTC")
        return False, f"{len(missing)} missing 15-min boundary candle(s) (e.g. {eg})"

    return True, "ok"


# ─────────────────────────────────────────────────────────────────────────────
# Prophet forecast engine (CPU-blocking — run in an executor)
# ─────────────────────────────────────────────────────────────────────────────
def run_prophet_forecast(df: pd.DataFrame, periods: Optional[int] = None) -> Optional[dict]:
    """Fit Prophet on log(close) and forecast `periods` one-minute steps ahead.

    One fit, one predict at interval_width=0.80 — the 80% confidence interval.
    Returns the horizon-end bands back-transformed to USD:
      {p10, p50, p90}.  p50 == exp(yhat); p10/p90 == exp(yhat_lower/upper).
    Blocking; call via loop.run_in_executor.
    """
    try:
        horizon = max(1, int(periods if periods is not None else FORECAST_MINUTES))
        d = pd.DataFrame({
            "ds": df["ds"].dt.tz_localize(None),          # Prophet wants tz-naive
            "y":  np.log(df["close"].astype(float)),      # log transform
        })
        model = Prophet(
            daily_seasonality=False,
            weekly_seasonality=False,
            yearly_seasonality=False,
            interval_width=0.80,                          # 80% CI → p10 / p90
            uncertainty_samples=UNCERTAINTY_SAMPLES,
        )
        model.fit(d)
        future = model.make_future_dataframe(
            periods=horizon, freq="min", include_history=False)
        row = model.predict(future).iloc[-1]              # minute-N (horizon end)
        return {
            "p10": float(np.exp(row["yhat_lower"])),
            "p50": float(np.exp(row["yhat"])),
            "p90": float(np.exp(row["yhat_upper"])),
        }
    except Exception as exc:  # noqa: BLE001
        log.error("Prophet forecast failed: %s", exc)
        return None


def percentile_of_price(price: float, bands: dict) -> float:
    """Interpolated percentile rank (10–90) of `price` within the 80% CI bands."""
    prices = [bands[k] for k, _ in _QMAP]
    qs     = [q for _, q in _QMAP]
    if price <= prices[0]:
        return qs[0] * 100.0
    if price >= prices[-1]:
        return qs[-1] * 100.0
    return float(np.interp(price, prices, qs)) * 100.0


def decide_side_from_forecast(strike: float, forecast: dict) -> tuple[Optional[str], str]:
    """Return the Kalshi side from forecast settlement p50 versus live strike."""
    p50 = float(forecast["p50"])
    strike = float(strike)
    if p50 > strike:
        return "yes", "BUY YES"
    if p50 < strike:
        return "no", "BUY NO"
    return None, "SKIP"


async def prepare_forecast_for_ticker(ticker: str,
                                      settle_et: Optional[datetime],
                                      reason: str) -> Optional[dict]:
    """Fetch data and cache the settlement forecast for a specific ticker."""
    if ticker in preopen_forecasts:
        return preopen_forecasts[ticker]

    loop = asyncio.get_event_loop()
    df = await loop.run_in_executor(None, fetch_btc_1m)
    ok, data_reason = validate_data(df)
    if not ok:
        log.warning("BTC data check failed for %s forecast %s: %s — no cached forecast.",
                    reason, ticker, data_reason)
        return None

    # This is intentionally fixed: forecasting begins two minutes before the
    # next open and always projects 17 one-minute steps ahead for that window.
    horizon = max(1, FORECAST_MINUTES)
    forecast = await loop.run_in_executor(None, run_prophet_forecast, df, horizon)
    if forecast is None:
        log.warning("No valid Prophet forecast for %s forecast %s.", reason, ticker)
        return None

    btc_close = float(df["close"].iloc[-1])
    rec = {
        "ticker": ticker,
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
        "source_reason": reason,
        "settle_et": settle_et.isoformat() if settle_et else "",
        "horizon_minutes": horizon,
        "forecast": forecast,
        "btc_close": btc_close,
        "btc_quantile_position": percentile_of_price(btc_close, forecast),
        "data_start": df["ds"].iloc[0],
        "data_end": df["ds"].iloc[-1],
        "candles": len(df),
    }
    preopen_forecasts[ticker] = rec
    log.info("PRE-OPEN FORECAST cached for %s (%s): %d candles %s → %s, "
             "horizon=%d one-minute steps, BTC close=$%.2f, "
             "P10=$%.2f P50=$%.2f P90=$%.2f",
             ticker, reason, len(df), rec["data_start"], rec["data_end"],
             horizon, btc_close, forecast["p10"], forecast["p50"], forecast["p90"])
    return rec


async def maybe_prepare_next_window_forecast(ct: str, nt: str) -> None:
    """Two minutes before a window opens, cache its 17-step forecast."""
    if nt in preopen_forecasts:
        return
    seconds_to_open = seconds_until_ticker_settle(ct)
    if seconds_to_open is None:
        return
    if not (0 < seconds_to_open <= PREOPEN_FORECAST_LEAD_S):
        return
    parsed = parse_ticker(nt) or {}
    settle_et = parsed.get("settle_et")
    log.info("Pre-open window reached: %.0fs until %s opens — forecasting %s now.",
             seconds_to_open, nt, nt)
    await prepare_forecast_for_ticker(
        nt, settle_et, f"{seconds_to_open:.0f}s before market open")


# ─────────────────────────────────────────────────────────────────────────────
# Kalshi market WebSocket (aiohttp, async, RSA-signed)
# ─────────────────────────────────────────────────────────────────────────────
def _to_dollars(val) -> Optional[float]:
    if val is None:
        return None
    try:
        f = float(val)
    except (TypeError, ValueError):
        return None
    if isinstance(val, str) and "." in val:
        return f
    if f.is_integer() and 1 <= f <= 100:   # legacy integer cents
        return f / 100.0
    return f


def _to_contract_count(val) -> Optional[float]:
    """Parse a fixed-point contract count without applying price conversion."""
    if val is None:
        return None
    try:
        count = float(val)
    except (TypeError, ValueError):
        return None
    return count if count >= 0.0 else None


class KalshiMarketWS:
    """Async Kalshi market-data subscriber over aiohttp."""

    def __init__(self, auth: KalshiAuth, url: str = KALSHI_WS_URL):
        self.auth = auth
        self.url = url
        self.path = urlparse(url).path or "/trade-api/ws/v2"
        self.desired: tuple = ()
        self.subscribed: tuple = ()
        self.connected = False
        self.msg_count = 0
        self._cmd_id = 0

    def set_tickers(self, tickers: tuple) -> None:
        self.desired = tuple(t for t in tickers if t)

    @staticmethod
    def _invalidate_quotes(tickers: tuple) -> None:
        """Prevent a pre-disconnect book from qualifying a dry simulated fill."""
        for ticker in tickers:
            kalshi_quotes.pop(ticker, None)

    async def run(self) -> None:
        while True:
            try:
                headers = self.auth.create_auth_headers("GET", self.path)
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(self.url, headers=headers,
                                                   heartbeat=10) as ws:
                        self.connected = True
                        log.info("Kalshi WS: connected (%s)", self.url)
                        await self._session_loop(ws)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.error("Kalshi WS error: %s", exc)
            self.connected = False
            self._invalidate_quotes(self.subscribed)
            self.subscribed = ()
            log.info("Kalshi WS: reconnecting in 5s …")
            await asyncio.sleep(5)

    async def _subscribe(self, ws, tickers: tuple) -> None:
        # Subscription changes and reconnects require a fresh top-of-book
        # snapshot before a dry simulator can use it.
        self._invalidate_quotes(tickers)
        self._cmd_id += 1
        await ws.send_json({
            "id": self._cmd_id, "cmd": "subscribe",
            "params": {"channels": ["ticker", "trade"],
                       "market_tickers": list(tickers)},
        })
        self.subscribed = tickers
        log.info("Kalshi WS: subscribed ticker/trade → %s", ", ".join(tickers))

    async def _session_loop(self, ws) -> None:
        if self.desired:
            await self._subscribe(ws, self.desired)
        while True:
            if self.desired and self.desired != self.subscribed:
                await self._subscribe(ws, self.desired)
            try:
                msg = await ws.receive(timeout=1.0)
            except asyncio.TimeoutError:
                continue
            if msg.type == aiohttp.WSMsgType.TEXT:
                self._handle(msg.data)
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING,
                              aiohttp.WSMsgType.ERROR):
                break

    def _handle(self, raw: str) -> None:
        try:
            data = json.loads(raw)
        except (ValueError, TypeError):
            return
        self.msg_count += 1
        mtype = data.get("type")
        if mtype in ("subscribed", "ok"):
            if KALSHI_WS_VERBOSE:
                log.info("Kalshi WS ack: %s", data.get("msg"))
            return
        if mtype == "error":
            log.error("Kalshi WS server error: %s", data.get("msg"))
            return
        msg = data.get("msg") or {}
        ticker = msg.get("market_ticker") or msg.get("ticker")
        if not ticker:
            return
        if mtype == "ticker":
            yes_bid = _to_dollars(msg.get("yes_bid_dollars", msg.get("yes_bid")))
            yes_ask = _to_dollars(msg.get("yes_ask_dollars", msg.get("yes_ask")))
            yes_bid_size = _to_contract_count(msg.get(
                "yes_bid_size_fp", msg.get("yes_bid_size", msg.get(
                    "bid_size_fp", msg.get("bid_size")))))
            yes_ask_size = _to_contract_count(msg.get(
                "yes_ask_size_fp", msg.get("yes_ask_size", msg.get(
                    "ask_size_fp", msg.get("ask_size")))))
            last    = _to_dollars(msg.get("last_price_dollars",
                                  msg.get("price", msg.get("last_price"))))
            q = kalshi_quotes.setdefault(ticker, {})
            received_at = datetime.now(tz=timezone.utc)
            if yes_bid is not None: q["yes_bid"] = yes_bid
            if yes_ask is not None: q["yes_ask"] = yes_ask
            if yes_bid_size is not None: q["yes_bid_size"] = yes_bid_size
            if yes_ask_size is not None: q["yes_ask_size"] = yes_ask_size
            if last    is not None: q["last"]    = last
            # A partial ticker update may carry only a last trade or one side
            # of the book.  It must not refresh a prior complete book snapshot
            # for dry-fill purposes.
            if None not in (yes_bid, yes_ask, yes_bid_size, yes_ask_size):
                q["book_received_at"] = received_at
                q["book_source_time"] = msg.get("time")
                q["book_source_ts_ms"] = msg.get("ts_ms", msg.get("ts"))
                q["book_sequence"] = int(q.get("book_sequence", 0)) + 1
            if KALSHI_WS_VERBOSE:
                log.info("Kalshi WS ticker %s  yes_bid=%s x %.2f yes_ask=%s x %.2f last=%s",
                         ticker, q.get("yes_bid"), q.get("yes_bid_size", 0.0),
                         q.get("yes_ask"), q.get("yes_ask_size", 0.0), q.get("last"))
        elif mtype == "trade":
            last = _to_dollars(msg.get("yes_price_dollars",
                               msg.get("yes_price", msg.get("price"))))
            if last is not None:
                q = kalshi_quotes.setdefault(ticker, {})
                q["last"] = last
                q["last_received_at"] = datetime.now(tz=timezone.utc)
            if KALSHI_WS_VERBOSE:
                log.info("Kalshi WS trade  %s  yes_price=%s count=%s",
                         ticker, last, msg.get("count"))


def get_kalshi_quote(ticker: str) -> Optional[dict]:
    q = kalshi_quotes.get(ticker)
    return dict(q) if q else None


def get_active_yes_price(market: dict) -> Optional[float]:
    """Best estimate of the contract's YES price (dollars): WS last/mid, else REST."""
    q = get_kalshi_quote(market["ticker"])
    if q:
        if q.get("last") is not None:
            return q["last"]
        b, a = q.get("yes_bid"), q.get("yes_ask")
        if b is not None and a is not None:
            return (b + a) / 2
        if a is not None:
            return a
        if b is not None:
            return b
    raw = market.get("raw_market")
    if raw is not None:
        for f in ("last_price_dollars", "yes_ask_dollars", "yes_bid_dollars",
                  "previous_price_dollars"):
            v = _to_dollars(getattr(raw, f, None))
            if v is not None:
                return v
    return None


def position_price_from_yes(side: str, yes_price: float) -> float:
    """Convert a YES-term market price into the selected side's position price."""
    yes = float(yes_price)
    return yes if str(side).lower() == "yes" else round(1.0 - yes, 4)


def fresh_executable_dry_quote(
    ticker: str,
    side: str,
    required_count: float,
    *,
    now: Optional[datetime] = None,
) -> tuple[Optional[dict], str]:
    """Return fresh, executable top-of-book evidence for one dry-rung fill.

    A YES buy can take only the YES ask.  A NO buy is a YES ask, so it can
    take only the YES bid and its economic NO cost is ``1 - yes_bid``.  Last
    trade and midpoint data are intentionally excluded: neither proves that a
    resting limit was executable.  Top-of-book size is necessary evidence but
    still cannot prove queue position or an actual exchange execution.
    """
    q = get_kalshi_quote(ticker)
    if not q:
        return None, "no_book_quote"
    received_at = q.get("book_received_at")
    if not isinstance(received_at, datetime):
        return None, "missing_book_timestamp"
    reference_time = now or datetime.now(tz=timezone.utc)
    age_seconds = (reference_time - received_at).total_seconds()
    if age_seconds < -0.5 or age_seconds > DRY_QUOTE_MAX_AGE_S:
        return None, "stale_book_quote"

    # Prices were converted from the WebSocket's dollar fields on ingestion.
    # Do not run them through _to_dollars again: a valid stored value of 1.0
    # must remain $1.00 rather than being interpreted as legacy one cent.
    yes_bid = _to_contract_count(q.get("yes_bid"))
    yes_ask = _to_contract_count(q.get("yes_ask"))
    yes_bid_size = _to_contract_count(q.get("yes_bid_size"))
    yes_ask_size = _to_contract_count(q.get("yes_ask_size"))
    if None in (yes_bid, yes_ask, yes_bid_size, yes_ask_size):
        return None, "incomplete_top_of_book"
    if not (0.0 <= yes_bid <= yes_ask <= 1.0):
        return None, "invalid_top_of_book"

    required = float(required_count)
    if required < 0.01 - 1e-9:
        return None, "invalid_required_count"
    normalized_side = str(side).lower()
    if normalized_side == "yes":
        executable_yes_price = yes_ask
        displayed_depth = yes_ask_size
        executable_field = "yes_ask"
    elif normalized_side == "no":
        executable_yes_price = yes_bid
        displayed_depth = yes_bid_size
        executable_field = "yes_bid"
    else:
        return None, "invalid_position_side"
    if displayed_depth + 1e-9 < required:
        return None, "insufficient_top_of_book_depth"

    received_at_iso = received_at.isoformat()
    return {
        "quote_id": f"{ticker}:{q.get('book_sequence', 0)}:{received_at_iso}",
        "side": normalized_side,
        "economic_price": position_price_from_yes(normalized_side, executable_yes_price),
        "executable_yes_price": round(executable_yes_price, 4),
        "executable_field": executable_field,
        "displayed_depth": round(displayed_depth, 2),
        "required_count": round(required, 2),
        "yes_bid": round(yes_bid, 4),
        "yes_bid_size": round(yes_bid_size, 2),
        "yes_ask": round(yes_ask, 4),
        "yes_ask_size": round(yes_ask_size, 2),
        "quote_received_at": received_at_iso,
        "quote_source_time": q.get("book_source_time"),
        "quote_source_ts_ms": q.get("book_source_ts_ms"),
        "quote_age_seconds": round(max(0.0, age_seconds), 3),
    }, "executable_top_of_book"


# ─────────────────────────────────────────────────────────────────────────────
# Kalshi async REST wrapper
# ─────────────────────────────────────────────────────────────────────────────
def load_pem() -> str:
    if KALSHI_PEM_PATH and os.path.exists(KALSHI_PEM_PATH):
        with open(KALSHI_PEM_PATH, "r") as fh:
            return fh.read()
    env_pem = os.getenv("KALSHI_PRIVATE_KEY")
    if env_pem:
        return env_pem
    raise FileNotFoundError(
        f"No Kalshi PEM at {KALSHI_PEM_PATH!r} and KALSHI_PRIVATE_KEY unset")


class KalshiREST:
    """Async wrapper over the kalshi-python-async V2 Api classes."""

    def __init__(self):
        pem = load_pem()
        # Official auth pattern: set credentials on Configuration BEFORE building
        # the client (KalshiClient.__init__ builds its KalshiAuth from these).
        # client.set_kalshi_auth() is broken in the 3.22 build, so we avoid it.
        config = Configuration(host=KALSHI_BASE_URL)
        config.api_key_id = KALSHI_API_KEY_ID
        config.private_key_pem = pem
        self.client = KalshiClient(config)
        self.auth = KalshiAuth(KALSHI_API_KEY_ID, pem)   # reused for WS handshake
        self.portfolio = PortfolioApi(self.client)
        self.markets   = MarketApi(self.client)
        self.events    = EventsApi(self.client)
        self.orders    = OrdersApi(self.client)
        log.info("Kalshi async client built  demo=%s  base=%s",
                 KALSHI_DEMO, KALSHI_BASE_URL)

    async def close(self) -> None:
        try:
            await self.client.close()
        except Exception:  # noqa: BLE001
            pass

    async def get_balance_dollars(self) -> Optional[float]:
        resp = await self.portfolio.get_balance()
        bd = getattr(resp, "balance_dollars", None)
        if bd is not None:
            try:
                return float(bd)
            except (TypeError, ValueError):
                pass
        cents = getattr(resp, "balance", None)
        return (cents / 100.0) if cents is not None else None

    async def get_market(self, ticker: str):
        try:
            resp = await self.markets.get_market(ticker)
            return getattr(resp, "market", None)
        except Exception as exc:  # noqa: BLE001
            log.warning("get_market(%s) failed: %s", ticker, exc)
            return None

    async def get_positions(self) -> list:
        """Return the account's market positions (MarketPosition objects)."""
        try:
            resp = await self.portfolio.get_positions(limit=200)
            return getattr(resp, "market_positions", None) or []
        except Exception as exc:  # noqa: BLE001
            log.warning("get_positions failed: %s", exc)
            return []

    async def get_open_series_markets(self, series: str = SERIES_TICKER) -> list:
        try:
            resp = await self.events.get_events(
                series_ticker=series, status="open",
                with_nested_markets=True, limit=5)
            out = []
            for ev in (getattr(resp, "events", None) or []):
                out.extend(getattr(ev, "markets", None) or [])
            return out
        except Exception as exc:  # noqa: BLE001
            log.error("get_events fallback failed for %s: %s", series, exc)
            return []


def _field(obj, *names):
    for n in names:
        v = getattr(obj, n, None)
        if v is not None:
            return v
    return None


def is_market_live(market) -> bool:
    """Kalshi only accepts new orders while a market status is ``active``."""
    return str(_field(market, "status") or "").strip().lower() == "active"


def _extract_target(market) -> Optional[float]:
    """Target/strike price = floor_strike (min value for YES); fall back to subtitle."""
    val = _field(market, "floor_strike", "cap_strike", "functional_strike")
    if val is not None:
        try:
            f = float(val)
            if f > 0:
                return f
        except (TypeError, ValueError):
            pass
    for tf in ("yes_sub_title", "no_sub_title"):
        m = re.search(r"\$([0-9,]+(?:\.\d+)?)", str(_field(market, tf) or ""))
        if m:
            return float(m.group(1).replace(",", ""))
    return None


async def resolve_active_market(rest: KalshiREST,
                                series: str = SERIES_TICKER) -> Optional[dict]:
    """Resolve the current open market for a 15-minute series."""
    ct, nt = current_and_next_tickers(series)
    parsed = parse_ticker(ct) or {}
    market = await rest.get_market(ct)
    if market is None:
        log.warning("Direct lookup of %s failed – trying events query", ct)
        markets = await rest.get_open_series_markets(series)
        if markets:
            market = markets[0]
            ct = _field(market, "ticker") or ct
            parsed = parse_ticker(ct) or parsed
    if market is None:
        log.info("No open %s market found", series)
        return None
    if not is_market_live(market):
        log.info("Market %s is not live yet (status=%s); waiting for active.",
                 ct, _field(market, "status"))
        return None
    return {"ticker": ct, "next_ticker": nt,
            "market_type": parsed.get("market_type", "?"),
            "settle_et": parsed.get("settle_et"),
            "target": _extract_target(market),
            "raw_market": market}


# ─────────────────────────────────────────────────────────────────────────────
# Orders (BTC entries are marketable IOC; ETH hedge orders rest until settlement)
# ─────────────────────────────────────────────────────────────────────────────
async def _submit(rest: KalshiREST, *, ticker, side: BookSide, price: str,
                  count: float, reduce_only: bool, tag: str,
                  tif: str = ORDER_TIF,
                  expiration_time: Optional[int] = None) -> tuple:
    """Submit an order (fractional count OK, 0.01 granularity).
    Returns (resp, filled)."""
    global trades_placed, buys_placed, closes_placed, fills_count
    order_id = str(uuid.uuid4())
    unit_cost = _f(price, 0.0) if side == BookSide.BID else 1.0 - _f(price, 0.0)
    log.info("ORDER %s  %s  side=%s price=%s count=%.2f (~$%.2f) reduce_only=%s "
             "tif=%s expires=%s ticker=%s id=%s",
             "[DRY-RUN]" if DRY_RUN else "[LIVE]", tag, side.value, price, count,
             float(count) * unit_cost, reduce_only, tif, expiration_time, ticker, order_id)
    if DRY_RUN:
        log.info("DRY_RUN active — order NOT submitted.")
        return None, True                       # simulate a fill / resting order
    try:
        # The async create_order_v2 builds CreateOrderV2Request(**kwargs) internally,
        # so the order fields are passed DIRECTLY as kwargs (not wrapped).
        kwargs = dict(
            ticker=ticker, side=side, count=f"{float(count):.2f}", price=price,
            time_in_force=tif, client_order_id=order_id,
            self_trade_prevention_type=SelfTradePreventionType.TAKER_AT_CROSS,
            reduce_only=reduce_only)
        if expiration_time is not None:
            kwargs["expiration_time"] = int(expiration_time)
        resp = await rest.orders.create_order_v2(**kwargs)
        try:
            fc = float(getattr(resp, "fill_count", 0) or 0)
        except (TypeError, ValueError):
            fc = 0.0
        log.info("ORDER RESULT: order_id=%s fill_count=%s remaining=%s avg_price=%s",
                 getattr(resp, "order_id", "?"), getattr(resp, "fill_count", "?"),
                 getattr(resp, "remaining_count", "?"),
                 getattr(resp, "average_fill_price", "?"))
        trades_placed += 1
        if reduce_only:
            closes_placed += 1
        else:
            buys_placed += 1
        filled = fc > 0
        if filled:
            fills_count += 1
        elif tif == ORDER_TIF:
            log.warning("Order did NOT fill (IOC) — book too thin at price %s. "
                        "Kalshi min price is $0.01, so a side priced below the best "
                        "opposite quote cannot cross.", price)
        else:
            log.info("Limit order did not fill immediately at price %s.", price)
        return resp, filled
    except Exception as exc:  # noqa: BLE001
        body = getattr(exc, "body", None)
        log.error("create_order_v2 failed: %s%s", exc,
                  f"  raw_body={body}" if body else "")
        return None, False


# ─────────────────────────────────────────────────────────────────────────────
# Performance tracking  (trade journal, stats, equity/drawdown)
# ─────────────────────────────────────────────────────────────────────────────
class PerformanceTracker:
    """Persists trade history + per-window dedupe store, computes statistics."""

    def __init__(self, history_path: str, traded_path: str):
        self.history_path = history_path
        self.traded_path = traded_path
        self.trades: list = []          # list of trade records
        self.traded_tickers: dict = {}  # {ticker: {side,time,btc_price,p50}}
        self._load()

    def _load(self) -> None:
        if os.path.exists(self.history_path):
            try:
                with open(self.history_path, "r") as fh:
                    self.trades = json.load(fh) or []
            except Exception as exc:  # noqa: BLE001
                log.warning("Could not read %s: %s", self.history_path, exc)
        if os.path.exists(self.traded_path):
            try:
                with open(self.traded_path, "r") as fh:
                    self.traded_tickers = json.load(fh) or {}
            except Exception as exc:  # noqa: BLE001
                log.warning("Could not read %s: %s", self.traded_path, exc)

    def _save_history(self) -> None:
        try:
            with open(self.history_path, "w") as fh:
                json.dump(self.trades, fh, indent=2, default=str)
        except Exception as exc:  # noqa: BLE001
            log.warning("Could not write %s: %s", self.history_path, exc)

    def _save_traded(self) -> None:
        try:
            with open(self.traded_path, "w") as fh:
                json.dump(self.traded_tickers, fh, indent=2, default=str)
        except Exception as exc:  # noqa: BLE001
            log.warning("Could not write %s: %s", self.traded_path, exc)

    def already_traded(self, ticker: str) -> bool:
        return ticker in self.traded_tickers

    def record_open(self, rec: dict) -> None:
        """Record a freshly-opened (pending) trade + its dedupe entry."""
        self.trades.append(rec)
        self.traded_tickers[rec["ticker"]] = {
            "side": rec["side"], "time": rec["timestamp"],
            "btc_price": rec.get("btc_entry"), "p50": rec.get("p50_prediction"),
            "trade_kind": rec.get("trade_kind", "BTC_PRIMARY"),
        }
        self._save_history()
        self._save_traded()

    def find_pending(self) -> list:
        return [t for t in self.trades if t.get("result") == "pending"]

    def save(self) -> None:
        """Persist in-place record mutations from monitor tasks."""
        self._save_history()

    def settle(self, rec: dict, result: str, pnl: float) -> None:
        rec["result"] = result
        rec["profit_loss"] = round(float(pnl), 4)
        self._save_history()

    @staticmethod
    def _streak_metrics(records: list) -> dict:
        """Return current and longest win/loss streaks for settled records."""
        longest_win = longest_loss = 0
        current_win = current_loss = 0
        for rec in records:
            if rec["result"] == "WIN":
                current_win += 1
                current_loss = 0
                longest_win = max(longest_win, current_win)
            else:
                current_loss += 1
                current_win = 0
                longest_loss = max(longest_loss, current_loss)

        current_streak = 0
        current_kind = None
        for rec in reversed(records):
            if current_kind is None:
                current_kind = rec["result"]
                current_streak = 1
            elif rec["result"] == current_kind:
                current_streak += 1
            else:
                break
        return {
            "current_streak": current_streak,
            "current_kind": current_kind,
            "longest_win": longest_win,
            "longest_loss": longest_loss,
        }

    def stats(self) -> dict:
        settled = [t for t in self.trades if t.get("result") in ("WIN", "LOSS")]
        total = len(settled)
        wins = sum(1 for t in settled if t["result"] == "WIN")
        losses = total - wins
        win_rate = (wins / total * 100.0) if total else 0.0
        pnls = [float(t.get("profit_loss", 0.0)) for t in settled]
        total_return = sum(pnls)
        avg_return = (total_return / total) if total else 0.0
        wins_pnl = [p for p in pnls if p > 0]
        loss_pnl = [p for p in pnls if p < 0]
        largest_win = max(wins_pnl) if wins_pnl else 0.0
        largest_loss = min(loss_pnl) if loss_pnl else 0.0

        streaks = self._streak_metrics(settled)

        # equity curve + max drawdown (starting balance 0)
        eq = peak = max_dd = 0.0
        for p in pnls:
            eq += p
            peak = max(peak, eq)
            max_dd = max(max_dd, peak - eq)

        return {
            "total": total, "wins": wins, "losses": losses, "win_rate": win_rate,
            "total_return": total_return, "avg_return": avg_return,
            "largest_win": largest_win, "largest_loss": largest_loss,
            **streaks,
            "max_drawdown": max_dd,
            "last": settled[-1] if settled else None,
        }


def inverse_shadow_rung_performance(records: list[dict]) -> dict[str, dict]:
    """Summarize paper quote hits for the Prophet inverse shadow only."""
    stats = {
        f"{level:.2f}": {
            "rung_price": level, "paper_orders": 0, "quote_hits": 0,
            "paper_contracts": 0.0, "filled_contracts": 0.0,
            "winning_hits": 0, "losing_hits": 0, "net_profit": 0.0,
        }
        for level in LADDER_LEVELS
    }
    for rec in records:
        market_result = str(rec.get("market_result") or "").lower()
        side = str(rec.get("side") or "").lower()
        for rung in rec.get("rungs", []):
            level = round(_f(rung.get("economic_price")), 2)
            item = stats.get(f"{level:.2f}")
            if item is None:
                continue
            count = _f(rung.get("count"), 0.0)
            fill = _f(rung.get("fill_count"), 0.0)
            item["paper_orders"] += 1
            item["paper_contracts"] += count
            if fill <= 0.005:
                continue
            entry = _f(rung.get("fill_economic_price"), level)
            pnl = (fill - fill * entry) if side == market_result else -fill * entry
            item["quote_hits"] += 1
            item["filled_contracts"] += fill
            item["net_profit"] += pnl
            if pnl > 0.0:
                item["winning_hits"] += 1
            elif pnl < 0.0:
                item["losing_hits"] += 1
    for item in stats.values():
        item["paper_contracts"] = round(item["paper_contracts"], 2)
        item["filled_contracts"] = round(item["filled_contracts"], 2)
        item["net_profit"] = round(item["net_profit"], 6)
    return stats


def inverse_shadow_performance(records: list[dict]) -> dict:
    """Detailed independent summary; quote hits are never exchange fills."""
    directional = [
        rec for rec in records
        if str(rec.get("market_result") or "").lower() in ("yes", "no")
    ]
    filled = [rec for rec in records if rec.get("result") in ("WIN", "LOSS")]
    wins = sum(rec.get("result") == "WIN" for rec in filled)
    pnls = [_f(rec.get("profit_loss")) for rec in filled]
    costs = [
        sum(_f(rung.get("fill_count")) * _f(rung.get("fill_economic_price"), _f(rung.get("economic_price")))
            for rung in rec.get("rungs", []))
        for rec in filled
    ]
    directional_wins = sum(
        str(rec.get("side") or "").lower() == str(rec.get("market_result") or "").lower()
        for rec in directional
    )
    equity = peak = max_drawdown = 0.0
    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)
    gross_win = sum(pnl for pnl in pnls if pnl > 0.0)
    gross_loss = -sum(pnl for pnl in pnls if pnl < 0.0)
    streaks = PerformanceTracker._streak_metrics(filled)
    return {
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "strategy": "inverse_prophet_executable_quote_shadow_v1",
        "mode": "paper_only_no_exchange_orders",
        "fill_rule": "YES buy: yes_ask <= rung; NO buy: 1 - yes_bid <= rung; fresh complete top-of-book and displayed depth >= rung quantity",
        "quote_max_age_seconds": DRY_QUOTE_MAX_AGE_S,
        "fee_treatment": "excluded_no_exchange_fill",
        "limitations": [
            "A quote hit is a paper fill, not a Kalshi exchange fill.",
            "Queue position, quote cancellation, hidden liquidity, and fees are not modeled.",
            "P&L uses the pre-posted rung limit rather than favorable price improvement.",
        ],
        "shadow_signals_started": len(records),
        "active_shadow_markets": sum(rec.get("result") == "pending" for rec in records),
        "settled_signal_markets": len(directional),
        "unfilled_shadow_markets": sum(rec.get("result") == "UNFILLED" for rec in records),
        "filled_market_trades": len(filled),
        "directional_wins": directional_wins,
        "directional_losses": len(directional) - directional_wins,
        "directional_win_rate": round(directional_wins / len(directional), 6) if directional else None,
        "winning_trades": wins,
        "losing_trades": len(filled) - wins,
        "win_rate": round(wins / len(filled), 6) if filled else None,
        "total_simulated_cost": round(sum(costs), 6),
        "net_profit": round(sum(pnls), 6),
        "return_on_simulated_capital": round(sum(pnls) / sum(costs), 6) if sum(costs) else None,
        "average_profit_per_filled_market": round(sum(pnls) / len(pnls), 6) if pnls else None,
        "largest_winning_trade": round(max(pnls, default=0.0), 6),
        "largest_losing_trade": round(min(pnls, default=0.0), 6),
        "profit_factor": round(gross_win / gross_loss, 6) if gross_loss else None,
        "maximum_drawdown": round(max_drawdown, 6),
        "longest_winning_streak": streaks["longest_win"],
        "longest_losing_streak": streaks["longest_loss"],
        "rung_performance": inverse_shadow_rung_performance(records),
    }


class InverseProphetShadowTracker:
    """Separate durable ledger for paper-only inverse Prophet ladders."""

    def __init__(self, history_path: str, report_path: str):
        self.history_path = history_path
        self.report_path = report_path
        self.trades: list[dict] = []
        self._load()

    def _load(self) -> None:
        if not os.path.exists(self.history_path):
            return
        try:
            with open(self.history_path, "r", encoding="utf-8") as fh:
                payload = json.load(fh)
            self.trades = payload if isinstance(payload, list) else []
        except Exception as exc:  # noqa: BLE001
            log.warning("Could not read inverse Prophet shadow ledger %s: %s", self.history_path, exc)

    def already_shadowed(self, ticker: str) -> bool:
        return any(str(rec.get("ticker")) == ticker for rec in self.trades)

    def find_pending(self) -> list[dict]:
        return [rec for rec in self.trades if rec.get("result") == "pending"]

    def record_open(self, rec: dict) -> bool:
        if self.already_shadowed(str(rec.get("ticker") or "")):
            return False
        self.trades.append(rec)
        self.save()
        return True

    def settle(self, rec: dict, result: str, pnl: float) -> None:
        rec["result"] = result
        rec["profit_loss"] = round(float(pnl), 4)
        self.save()

    def save(self) -> None:
        try:
            with open(self.history_path, "w", encoding="utf-8") as fh:
                json.dump(self.trades, fh, indent=2, default=str)
            with open(self.report_path, "w", encoding="utf-8") as fh:
                json.dump(inverse_shadow_performance(self.trades), fh, indent=2, default=str)
        except Exception as exc:  # noqa: BLE001
            log.warning("Could not write inverse Prophet shadow ledger/report: %s", exc)


class ProphetSelectorTracker(InverseProphetShadowTracker):
    """Durable paper ledger for the pre-open Prophet side selector.

    This intentionally reuses the same quote-fill accounting as the inverse
    shadow, but its side is whichever side was selected before that market
    opened.  It is a separate counterfactual portfolio, never an extra order.
    """

    def save(self) -> None:
        try:
            with open(self.history_path, "w", encoding="utf-8") as fh:
                json.dump(self.trades, fh, indent=2, default=str)
            with open(self.report_path, "w", encoding="utf-8") as fh:
                json.dump(prophet_selector_performance(self.trades), fh, indent=2,
                          default=str)
        except Exception as exc:  # noqa: BLE001
            log.warning("Could not write Prophet selector ledger/report: %s", exc)


# Module-level ledgers (created here so all coroutines share them).
tracker = PerformanceTracker(TRADE_HISTORY_FILE, TRADED_TICKERS_FILE)
inverse_shadow_tracker = InverseProphetShadowTracker(
    INVERSE_PROPHET_SHADOW_HISTORY_FILE, INVERSE_PROPHET_SHADOW_REPORT_FILE)
selector_tracker = ProphetSelectorTracker(
    PROPHET_SELECTOR_HISTORY_FILE, PROPHET_SELECTOR_REPORT_FILE)


def paired_prophet_directional_records() -> list[dict]:
    """Return chronological settled Prophet-versus-inverse directional pairs.

    A paired record contains the original frozen Prophet side and the settled
    market outcome.  It is independent of whether either paper ladder filled,
    so comparing normal and inverse win rates cannot be distorted by a fill
    on only one side.  Existing inverse-shadow records seed the history;
    selector records replace them for subsequent dry markets, and live
    selector records in the primary ledger extend it once explicitly enabled.
    """
    by_ticker: dict[str, dict] = {}

    def add(records: list[dict]) -> None:
        for rec in records:
            ticker = str(rec.get("ticker") or "")
            source_side = str(rec.get("source_prophet_side") or "").lower()
            result = str(rec.get("market_result") or "").lower()
            if (not ticker or source_side not in ("yes", "no")
                    or result not in ("yes", "no")):
                continue
            by_ticker[ticker] = {
                "ticker": ticker,
                "source_prophet_side": source_side,
                "market_result": result,
                "sort_time": str(rec.get("settle_et") or rec.get("timestamp") or ticker),
            }

    # The order gives the newer selector/live record priority for a ticker
    # while retaining the older inverse-only history as an initial sample.
    add(inverse_shadow_tracker.trades)
    add(selector_tracker.trades)
    add([rec for rec in tracker.trades if rec.get("selector_applied")])
    return sorted(by_ticker.values(), key=lambda rec: (rec["sort_time"], rec["ticker"]))


def prophet_selector_window_decisions(pairs: list[dict]) -> dict[str, dict]:
    """Evaluate each requested trailing window; ties and no history start inverse."""
    decisions: dict[str, dict] = {}
    for window in PROPHET_SELECTOR_WINDOWS:
        sample = pairs[-window:]
        sample_size = len(sample)
        normal_wins = sum(rec["source_prophet_side"] == rec["market_result"] for rec in sample)
        inverse_wins = sample_size - normal_wins
        if normal_wins > inverse_wins:
            leader = "normal"
        else:
            # An exact tie is deliberately deterministic: begin/stay inverse
            # rather than inventing a mid-stream preference for normal.
            leader = "inverse"
        decisions[str(window)] = {
            "window": window,
            "sample_size": sample_size,
            "normal_wins": normal_wins,
            "normal_losses": sample_size - normal_wins,
            "normal_win_rate": round(normal_wins / sample_size, 6) if sample_size else None,
            "inverse_wins": inverse_wins,
            "inverse_losses": normal_wins,
            "inverse_win_rate": round(inverse_wins / sample_size, 6) if sample_size else None,
            "leader": leader,
            "tied": normal_wins == inverse_wins,
        }
    return decisions


def prophet_selector_decision(source_prophet_side: str) -> Optional[dict]:
    """Freeze the next selector side from only fully settled prior signals."""
    source_side = str(source_prophet_side).lower()
    inverse_side = opposite_position_side(source_side)
    if source_side not in ("yes", "no") or inverse_side is None:
        return None
    pairs = paired_prophet_directional_records()
    windows = prophet_selector_window_decisions(pairs)
    normal_votes = sum(item["leader"] == "normal" for item in windows.values())
    inverse_votes = len(windows) - normal_votes
    bootstrap_inverse = PROPHET_SELECTOR_START_INVERSE and not selector_tracker.trades
    selected_mode = "inverse" if bootstrap_inverse else (
        "normal" if normal_votes > inverse_votes else "inverse")
    selected_side = source_side if selected_mode == "normal" else inverse_side
    return {
        "source_prophet_side": source_side.upper(),
        "selected_side": selected_side.upper(),
        "selected_mode": selected_mode,
        "paired_signals_available": len(pairs),
        "normal_votes": normal_votes,
        "inverse_votes": inverse_votes,
        "tie_break": "inverse" if normal_votes == inverse_votes else None,
        "bootstrap_inverse": bootstrap_inverse,
        "windows": windows,
        "frozen_at": datetime.now(tz=timezone.utc).isoformat(),
    }


def prophet_selector_performance(records: list[dict]) -> dict:
    """Detailed paper P&L and the current side decision for the selector."""
    report = inverse_shadow_performance(records)
    pairs = paired_prophet_directional_records()
    windows = prophet_selector_window_decisions(pairs)
    normal_selected = sum(rec.get("selector_mode") == "normal" for rec in records)
    inverse_selected = sum(rec.get("selector_mode") == "inverse" for rec in records)
    report.update({
        "strategy": "prophet_trailing_win_rate_side_selector_v1",
        "mode": "paper_only_no_exchange_orders" if DRY_RUN else "live_selector_execution_ledger",
        "selection_rule": (
            "Before each market, each trailing window (3,5,7,10,25,50) votes for "
            "the higher directional win-rate side on prior paired settled signals; "
            "the majority wins and ties/no history select inverse."
        ),
        "selection_starts_with": "inverse",
        "selection_counts": {"normal": normal_selected, "inverse": inverse_selected},
        "paired_signals_available": len(pairs),
        "window_monitor": windows,
    })
    return report


def ledger_for_record(rec: dict):
    if rec.get("trade_kind") == "BTC_PROPHET_INVERSE_SHADOW":
        return inverse_shadow_tracker
    if rec.get("trade_kind") == "BTC_PROPHET_WIN_RATE_SELECTOR":
        return selector_tracker
    return tracker


def print_performance() -> None:
    s = tracker.stats()
    if s["current_kind"] == "WIN":
        streak_s = f"{s['current_streak']} wins"
    elif s["current_kind"] == "LOSS":
        streak_s = f"{s['current_streak']} losses"
    else:
        streak_s = "none"

    last = s["last"]
    if last:
        qp = last.get("btc_quantile_position")
        qp_s = f"{qp:.0f}th percentile" if isinstance(qp, (int, float)) else "n/a"
        last_block = (
            f"║  Last Trade\n"
            f"║    Market       : {last.get('ticker')}\n"
            f"║    Side         : {last.get('side')}\n"
            f"║    BTC Entry    : ${_f(last.get('btc_entry')):,.2f}\n"
            f"║    Kalshi Strike: ${_f(last.get('strike')):,.2f}\n"
            f"║    Prophet P50  : ${_f(last.get('p50_prediction')):,.2f}\n"
            f"║    BTC Position : {qp_s}\n"
            f"║    Exit Via     : {last.get('exit_method') or 'settlement'}\n"
            f"║    Result       : {last.get('result')}  (P&L ${_f(last.get('profit_loss')):+,.2f})\n"
        )
    else:
        last_block = "║  Last Trade     : (none settled yet)\n"

    log.info(
        "\n"
        "╔═══ BTC KALSHI PROPHET PERFORMANCE ═════════════════════════\n"
        f"║  Trades         : {s['total']}\n"
        f"║  Wins           : {s['wins']}\n"
        f"║  Losses         : {s['losses']}\n"
        f"║  Win Rate       : {s['win_rate']:.1f}%\n"
        f"║  Total Return   : ${s['total_return']:+,.2f}\n"
        f"║  Average Return : ${s['avg_return']:+,.2f}\n"
        f"║  Largest Win    : ${s['largest_win']:+,.2f}\n"
        f"║  Largest Loss   : ${s['largest_loss']:+,.2f}\n"
        f"║  Current Streak : {streak_s}\n"
        f"║  Longest Win    : {s['longest_win']}\n"
        f"║  Longest Loss   : {s['longest_loss']}\n"
        f"║  Max Drawdown   : ${-s['max_drawdown']:,.2f}\n"
        f"{last_block}"
        "╚════════════════════════════════════════════════════════════"
    )


def print_inverse_prophet_shadow_performance() -> None:
    """Log the inverse experiment independently from Prophet's primary P&L."""
    report = inverse_shadow_performance(inverse_shadow_tracker.trades)
    directional_rate = (
        "n/a" if report["directional_win_rate"] is None
        else f"{100 * report['directional_win_rate']:.1f}%"
    )
    roi = (
        "n/a" if report["return_on_simulated_capital"] is None
        else f"{100 * report['return_on_simulated_capital']:.1f}%"
    )
    log.info(
        "\n"
        "╔═══ BTC PROPHET INVERSE SHADOW — PAPER ONLY ════════════════\n"
        f"║  Signals Started : {report['shadow_signals_started']}\n"
        f"║  Active / Settled: {report['active_shadow_markets']} / {report['settled_signal_markets']}\n"
        f"║  Directional W/L : {report['directional_wins']} / {report['directional_losses']}\n"
        f"║  Directional Rate: {directional_rate}\n"
        f"║  Quote-filled    : {report['filled_market_trades']} markets; unfilled {report['unfilled_shadow_markets']}\n"
        f"║  Simulated P&L   : ${report['net_profit']:+,.4f}  (fees excluded)\n"
        f"║  Simulated ROI   : {roi}\n"
        f"║  Max Drawdown    : ${-report['maximum_drawdown']:,.4f}\n"
        "║  Fill evidence   : fresh complete top-of-book + displayed depth only\n"
        "╚════════════════════════════════════════════════════════════"
    )
    for level, rung in report["rung_performance"].items():
        log.info(
            "INVERSE PROPHET SHADOW RUNG | %sc paper_orders=%d quote_hits=%d contracts=%.2f "
            "winners=%d losers=%d net=$%+.4f",
            level, rung["paper_orders"], rung["quote_hits"], rung["filled_contracts"],
            rung["winning_hits"], rung["losing_hits"], rung["net_profit"],
        )


def print_prophet_selector_performance() -> None:
    """Print the selector separately from both raw Prophet paper ledgers."""
    report = prophet_selector_performance(selector_tracker.trades)
    headline_mode = "PAPER ONLY" if DRY_RUN else "HISTORICAL PAPER BASELINE"
    fill_note = (
        "Side is frozen before open; no exchange order in paper mode"
        if DRY_RUN else "Paper baseline only; current live primary uses the frozen selector side"
    )
    roi = (
        "n/a" if report["return_on_simulated_capital"] is None
        else f"{100 * report['return_on_simulated_capital']:.1f}%"
    )
    rate = (
        "n/a" if report["directional_win_rate"] is None
        else f"{100 * report['directional_win_rate']:.1f}%"
    )
    next_source_side = "YES"
    next_decision = prophet_selector_decision(next_source_side)
    next_mode = (next_decision or {}).get("selected_mode", "inverse").upper()
    votes = (
        f"N{(next_decision or {}).get('normal_votes', 0)}"
        f"/I{(next_decision or {}).get('inverse_votes', len(PROPHET_SELECTOR_WINDOWS))}"
    )
    selected = report["selection_counts"]
    log.info(
        "\n"
        f"╔═══ BTC PROPHET WIN-RATE SELECTOR — {headline_mode} ════════════\n"
        f"║  Selector starts : INVERSE; windows {','.join(map(str, PROPHET_SELECTOR_WINDOWS))}\n"
        f"║  Current vote    : {next_mode}  ({votes}; {report['paired_signals_available']} paired settled signals)\n"
        f"║  Frozen choices  : normal {selected['normal']} / inverse {selected['inverse']}\n"
        f"║  Active / Settled: {report['active_shadow_markets']} / {report['settled_signal_markets']}\n"
        f"║  Directional W/L : {report['directional_wins']} / {report['directional_losses']}  ({rate})\n"
        f"║  Quote-filled    : {report['filled_market_trades']} markets; unfilled {report['unfilled_shadow_markets']}\n"
        f"║  Simulated P&L   : ${report['net_profit']:+,.4f}  (fees excluded)\n"
        f"║  Simulated ROI   : {roi}\n"
        f"║  Max Drawdown    : ${-report['maximum_drawdown']:,.4f}\n"
        f"║  {fill_note}\n"
        "╚════════════════════════════════════════════════════════════"
    )
    for window, item in report["window_monitor"].items():
        normal_rate = "n/a" if item["normal_win_rate"] is None else f"{100 * item['normal_win_rate']:.1f}%"
        inverse_rate = "n/a" if item["inverse_win_rate"] is None else f"{100 * item['inverse_win_rate']:.1f}%"
        log.info(
            "PROPHET SELECTOR WINDOW | trailing=%s paired=%d normal=%d/%d (%s) "
            "inverse=%d/%d (%s) leader=%s%s",
            window, item["sample_size"], item["normal_wins"], item["sample_size"], normal_rate,
            item["inverse_wins"], item["sample_size"], inverse_rate,
            item["leader"].upper(), " tie→INVERSE" if item["tied"] else "",
        )
    for level, rung in report["rung_performance"].items():
        log.info(
            "PROPHET SELECTOR RUNG | %sc paper_orders=%d quote_hits=%d contracts=%.2f "
            "winners=%d losers=%d net=$%+.4f",
            level, rung["paper_orders"], rung["quote_hits"], rung["filled_contracts"],
            rung["winning_hits"], rung["losing_hits"], rung["net_profit"],
        )


# ─────────────────────────────────────────────────────────────────────────────
# Portfolio report  (Kalshi balance / positions)
# ─────────────────────────────────────────────────────────────────────────────
def _f(v, default=0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


async def report_portfolio(rest: KalshiREST) -> None:
    """Fetch & print Kalshi balance, recent positions, and P&L, then the
    Prophet-strategy performance report."""
    bal = await rest.get_balance_dollars()
    positions = await rest.get_positions()

    realized_total = 0.0
    fees_total = 0.0
    lines = []
    for mp in positions:
        pf  = _f(getattr(mp, "position_fp", 0))
        rp  = _f(getattr(mp, "realized_pnl_dollars", 0))
        exp = _f(getattr(mp, "market_exposure_dollars", 0))
        fee = _f(getattr(mp, "fees_paid_dollars", 0))
        realized_total += rp
        fees_total += fee
        if pf != 0 or rp != 0:
            tk = getattr(mp, "ticker", "?")
            state = "OPEN" if pf != 0 else "settled"
            side = "YES" if pf > 0 else ("NO" if pf < 0 else "—")
            lines.append(f"│   {tk}  {state}  pos={pf:+g} {side}  "
                         f"exposure=${exp:,.2f}  realized=${rp:,.2f}")

    net = (bal - start_balance) if (bal is not None and start_balance is not None) else None
    net_s = f"${net:,.2f}" if net is not None else "n/a"
    bal_s = f"${bal:,.2f}" if bal is not None else "n/a"
    start_s = f"${start_balance:,.2f}" if start_balance is not None else "n/a"
    body = "\n".join(lines) if lines else "│   (none yet)"
    log.info(
        "\n"
        "╔═══ PORTFOLIO ══════════════════════════════════════════════\n"
        f"║  Balance        : {bal_s}   (start {start_s})\n"
        f"║  Net P&L (cash) : {net_s}   ← balance change since bot start\n"
        f"║  Realized P&L   : ${realized_total:,.2f}   Fees: ${fees_total:,.2f}\n"
        f"║  Orders placed  : {trades_placed}  (buys {buys_placed}, closes {closes_placed}, "
        f"fills {fills_count})\n"
        "║  Recent positions (Kalshi):\n"
        f"{body}\n"
        "╚════════════════════════════════════════════════════════════"
    )
    print_performance()
    print_active_ladder_status()
    if INVERSE_PROPHET_SHADOW_ENABLED:
        print_inverse_prophet_shadow_performance()
        print_active_inverse_prophet_shadow_status()
    if PROPHET_SELECTOR_ENABLED:
        print_prophet_selector_performance()
        print_active_prophet_selector_status()


async def portfolio_reporter(rest: KalshiREST) -> None:
    while True:
        await asyncio.sleep(REPORT_INTERVAL_S)
        try:
            await report_portfolio(rest)
        except Exception as exc:  # noqa: BLE001
            log.warning("portfolio report failed: %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# Settlement checker  (resolve pending BTC ladder → win/loss + P&L)
# ─────────────────────────────────────────────────────────────────────────────
async def _settle_record_if_ready(rest: KalshiREST, rec: dict) -> bool:
    """Settle one closed record if Kalshi has published its result yet."""
    if rec.get("result") != "pending":
        return True
    try:
        settle = datetime.fromisoformat(rec["settle_et"])
    except Exception:  # noqa: BLE001
        return False
    if settle.tzinfo is None:
        settle = settle.replace(tzinfo=ET)
    if datetime.now(tz=timezone.utc) < settle.astimezone(timezone.utc):
        return False

    await _refresh_ladder_fills(rest, rec)
    await _cancel_open_ladder_orders(rest, rec)
    market = await rest.get_market(rec["ticker"])
    if market is None:
        return False
    result = _field(market, "result")
    if not result:
        return False
    result = str(result).lower()
    if result not in ("yes", "no"):
        return False

    # Another task can settle while this coroutine awaits get_market.
    if rec.get("result") != "pending":
        return True
    ledger = ledger_for_record(rec)
    rec["market_result"] = result
    win = (result == str(rec["side"]).lower())
    filled_rungs = [r for r in rec.get("rungs", []) if _f(r.get("fill_count"), 0.0) > 0.0]
    count = sum(_f(r.get("fill_count"), bet_count()) for r in filled_rungs)
    cost = sum(
        _f(r.get("fill_count"), bet_count()) * _f(
            r.get("fill_economic_price"), _f(r.get("economic_price"), 0.0))
        for r in filled_rungs)
    if count <= 0.005:
        rec["result"] = "UNFILLED"
        rec["profit_loss"] = 0.0
        rec["exit_method"] = "market_closed_without_fill"
        ledger.save()
        kind = (
            "INVERSE PROPHET SHADOW" if rec.get("trade_kind") == "BTC_PROPHET_INVERSE_SHADOW"
            else "PROPHET SELECTOR" if rec.get("trade_kind") == "BTC_PROPHET_WIN_RATE_SELECTOR"
            else "PRIMARY"
        )
        log.info("%s UNFILLED | %s %s ladder had no filled contracts; excluded from win/loss P&L.",
                 kind,
                 rec["ticker"], rec["side"])
        return True
    pnl = (count - cost) if win else -cost
    rec["count"] = round(count, 2)
    rec["entry_price"] = round(cost / count, 4) if count > 0 else 0.0
    rec["exit_method"] = "settlement"

    outcome = "WIN" if pnl > 0 else "LOSS"
    ledger.settle(rec, outcome, pnl)
    kind = (
        "INVERSE PROPHET SHADOW" if rec.get("trade_kind") == "BTC_PROPHET_INVERSE_SHADOW"
        else "PROPHET SELECTOR" if rec.get("trade_kind") == "BTC_PROPHET_WIN_RATE_SELECTOR"
        else "PRIMARY"
    )
    log.info("%s SETTLED | %s result=%s side=%s → %s simulated_P&L=$%+.4f%s",
             kind,
             rec["ticker"], result.upper(), rec["side"], outcome, pnl,
             " (not an exchange fill; fees excluded)" if rec.get("trade_kind") in (
                 "BTC_PROPHET_INVERSE_SHADOW", "BTC_PROPHET_WIN_RATE_SELECTOR") else "")
    return True


async def settlement_checker(rest: KalshiREST) -> None:
    """Poll pending trades whose window has settled and finalize their result.

    Win/loss uses the Kalshi market `result` field ("yes"/"no"). P&L per contract:
        win  →  (1 - entry_price) * count
        loss →  -entry_price * count
    Each record holds just one locked YES/NO BTC ladder. P&L is calculated from
    the actual filled same-side rungs; unfilled rungs have no settlement P&L.
    """
    while True:
        await asyncio.sleep(SETTLE_CHECK_S)
        pending = tracker.find_pending()
        if DRY_RUN:
            pending += inverse_shadow_tracker.find_pending()
            if PROPHET_SELECTOR_ENABLED:
                pending += selector_tracker.find_pending()
        if not pending:
            continue
        for rec in pending:
            try:
                _simulate_dry_ladder_fills(rec)
                await _settle_record_if_ready(rest, rec)
            except Exception as exc:  # noqa: BLE001
                log.warning("Settlement check failed for %s: %s", rec.get("ticker"), exc)


# ─────────────────────────────────────────────────────────────────────────────
# Locked-side BTC GTC ladder
# ─────────────────────────────────────────────────────────────────────────────
def _ladder_order_terms(side: str, economic_price: float) -> tuple[BookSide, str]:
    """Translate a YES/NO contract cost into Kalshi's YES-book order terms."""
    if side == "yes":
        return BookSide.BID, f"{economic_price:.2f}"
    # A NO buy at cost c is a YES ask at 1-c.  This is still a NO position,
    # not an opposite-side hedge or a reversal.
    return BookSide.ASK, f"{1.0 - economic_price:.2f}"


def _market_close_epoch(settle_et: Optional[datetime]) -> Optional[int]:
    if settle_et is None:
        return None
    return int(settle_et.astimezone(timezone.utc).timestamp())


async def _refresh_ladder_fills(rest: KalshiREST, rec: dict) -> None:
    """Refresh actual fills for the four orders; missing lookup is non-fatal."""
    if DRY_RUN:
        return
    changed = False
    for rung in rec.get("rungs", []):
        order_id = rung.get("order_id")
        if not order_id:
            continue
        try:
            response = await rest.orders.get_order(order_id)
            order = getattr(response, "order", None)
            if order is None:
                continue
            fill = _f(_field(order, "fill_count_fp", "fill_count"), 0.0)
            average_yes = _to_dollars(
                _field(order, "average_fill_price", "average_fill_price_dollars"))
            if abs(fill - _f(rung.get("fill_count"), 0.0)) >= 0.005:
                rung["fill_count"] = round(fill, 2)
                rung["status"] = str(_field(order, "status") or "unknown")
                if average_yes is not None:
                    rung["fill_economic_price"] = round(
                        average_yes if rec["side"].lower() == "yes" else 1.0 - average_yes,
                        4)
                changed = True
        except Exception as exc:  # noqa: BLE001
            log.warning("Ladder order lookup failed for %s: %s", order_id, exc)
    if changed:
        ledger_for_record(rec).save()


def _simulate_dry_ladder_fills(rec: dict) -> None:
    """Conservatively paper-fill only fresh, depth-supported executable quotes.

    This remains a simulation, not an exchange-fill claim: top-of-book depth
    does not include the order's queue position or hidden/changed liquidity.
    A single quote's displayed depth is consumed across the ladder so the same
    evidence cannot manufacture multiple simulated fills on later polls.
    """
    if not DRY_RUN or rec.get("result") != "pending":
        return
    unfilled = [
        rung for rung in rec.get("rungs", [])
        if _f(rung.get("fill_count"), 0.0) < _f(rung.get("count"), 0.0) - 0.005
    ]
    if not unfilled:
        return
    minimum_count = min(_f(rung.get("count"), bet_count()) for rung in unfilled)
    quote, quote_state = fresh_executable_dry_quote(
        rec["ticker"], rec["side"], minimum_count)
    if quote is None:
        rec["last_dry_quote_state"] = quote_state
        return

    consumed_depth = rec.setdefault("dry_quote_depth_consumed", {})
    quote_id = quote["quote_id"]
    used_depth = _f(consumed_depth.get(quote_id), 0.0)
    remaining_depth = max(0.0, quote["displayed_depth"] - used_depth)
    changed = False
    # Highest economic cost has matching priority for the same selected side.
    # A 40c rung therefore consumes available crossing liquidity before a 30c
    # rung may use the same top-of-book evidence.
    for rung in sorted(unfilled, key=lambda item: _f(item.get("economic_price"), 0.0), reverse=True):
        level = _f(rung.get("economic_price"), 0.0)
        count = _f(rung.get("count"), bet_count())
        if quote["economic_price"] <= level + 1e-9 and remaining_depth + 1e-9 >= count:
            rung["fill_count"] = round(count, 2)
            # These GTC rungs were pre-posted.  A later crossing order executes
            # them at their resting limit, not with assumed price improvement.
            rung["fill_economic_price"] = round(level, 4)
            rung["status"] = "simulated_executable_quote_hit"
            rung["simulated_at"] = datetime.now(tz=timezone.utc).isoformat()
            rung["simulation_quote"] = dict(quote)
            remaining_depth -= count
            consumed_depth[quote_id] = round(quote["displayed_depth"] - remaining_depth, 2)
            log.info(
                "%s | %s locked_%s %s=$%.4f depth=%.2f "
                "age=%.3fs reached $%.2f; paper fill %.2f shares at limit "
                "(not an exchange fill).",
                "INVERSE PROPHET SHADOW RUNG HIT" if rec.get("trade_kind") == "BTC_PROPHET_INVERSE_SHADOW"
                else "PROPHET SELECTOR RUNG HIT" if rec.get("trade_kind") == "BTC_PROPHET_WIN_RATE_SELECTOR"
                else "DRY EXECUTABLE RUNG HIT",
                rec["ticker"], rec["side"], quote["executable_field"],
                quote["executable_yes_price"], quote["displayed_depth"],
                quote["quote_age_seconds"], level, count)
            changed = True
    if changed:
        rec["last_dry_quote_state"] = quote_state
        ledger_for_record(rec).save()


def print_active_ladder_status() -> None:
    """Log this runner's ticker, locked side, and every rung's paper state."""
    pending = tracker.find_pending()
    if not pending:
        log.info("DRY LADDER STATUS | no active Prophet ladder; waiting for the next frozen forecast.")
        return
    for rec in pending:
        unfilled = [
            rung for rung in rec.get("rungs", [])
            if _f(rung.get("fill_count"), 0.0) < _f(rung.get("count"), 0.0) - 0.005
        ]
        required_count = min(
            (_f(rung.get("count"), bet_count()) for rung in unfilled),
            default=bet_count())
        quote, quote_state = fresh_executable_dry_quote(
            rec["ticker"], rec["side"], required_count)
        rungs = ", ".join(
            f"${_f(r.get('economic_price')):.2f}:{r.get('status')}"
            for r in rec.get("rungs", [])) or "none"
        log.info("DRY LADDER STATUS | ticker=%s ws_channels=ticker,trade locked_side=%s "
                 "executable_side_price=%s quote_state=%s quote_age_s=%s rungs=[%s]",
                 rec["ticker"], rec["side"],
                 f"${quote['economic_price']:.4f}" if quote is not None else "unavailable",
                 quote_state,
                 f"{quote['quote_age_seconds']:.3f}" if quote is not None else "unavailable",
                 rungs)


def print_active_inverse_prophet_shadow_status() -> None:
    """Log the opposite-side paper ladder without confusing it with primary fills."""
    pending = inverse_shadow_tracker.find_pending()
    if not pending:
        log.info("INVERSE PROPHET SHADOW STATUS | no active paper inverse ladder.")
        return
    for rec in pending:
        unfilled = [
            rung for rung in rec.get("rungs", [])
            if _f(rung.get("fill_count"), 0.0) < _f(rung.get("count"), 0.0) - 0.005
        ]
        required_count = min(
            (_f(rung.get("count"), bet_count()) for rung in unfilled), default=bet_count())
        quote, quote_state = fresh_executable_dry_quote(rec["ticker"], rec["side"], required_count)
        rungs = ", ".join(
            f"${_f(rung.get('economic_price')):.2f}:{rung.get('status')}"
            for rung in rec.get("rungs", [])) or "none"
        log.info(
            "INVERSE PROPHET SHADOW STATUS | ticker=%s source_prophet_side=%s shadow_side=%s "
            "executable_side_price=%s quote_state=%s quote_age_s=%s rungs=[%s] no exchange order.",
            rec["ticker"], rec.get("source_prophet_side", "?"), rec["side"],
            f"${quote['economic_price']:.4f}" if quote is not None else "unavailable",
            quote_state,
            f"{quote['quote_age_seconds']:.3f}" if quote is not None else "unavailable",
            rungs,
        )


def print_active_prophet_selector_status() -> None:
    """Log the selector's frozen paper ladder without implying an order exists."""
    pending = selector_tracker.find_pending()
    if not pending:
        log.info("PROPHET SELECTOR STATUS | no active selector paper ladder.")
        return
    for rec in pending:
        unfilled = [
            rung for rung in rec.get("rungs", [])
            if _f(rung.get("fill_count"), 0.0) < _f(rung.get("count"), 0.0) - 0.005
        ]
        required_count = min(
            (_f(rung.get("count"), bet_count()) for rung in unfilled), default=bet_count())
        quote, quote_state = fresh_executable_dry_quote(rec["ticker"], rec["side"], required_count)
        rungs = ", ".join(
            f"${_f(rung.get('economic_price')):.2f}:{rung.get('status')}"
            for rung in rec.get("rungs", [])) or "none"
        snapshot = rec.get("selector_snapshot") or {}
        log.info(
            "PROPHET SELECTOR STATUS | ticker=%s source_prophet_side=%s selected_side=%s "
            "mode=%s votes=N%s/I%s executable_side_price=%s quote_state=%s quote_age_s=%s "
            "rungs=[%s] no exchange order.",
            rec["ticker"], rec.get("source_prophet_side", "?"), rec["side"],
            rec.get("selector_mode", "inverse").upper(), snapshot.get("normal_votes", 0),
            snapshot.get("inverse_votes", len(PROPHET_SELECTOR_WINDOWS)),
            f"${quote['economic_price']:.4f}" if quote is not None else "unavailable",
            quote_state,
            f"{quote['quote_age_seconds']:.3f}" if quote is not None else "unavailable",
            rungs,
        )


async def _cancel_open_ladder_orders(rest: KalshiREST, rec: dict) -> None:
    """Cancel only this market's remaining GTC orders after close."""
    if rec.get("ladder_cancel_attempted"):
        return
    rec["ladder_cancel_attempted"] = True
    for rung in rec.get("rungs", []):
        if _f(rung.get("fill_count"), 0.0) >= _f(rung.get("count"), 0.0) - 0.005:
            continue
        order_id = rung.get("order_id")
        if not order_id or DRY_RUN:
            continue
        try:
            await rest.orders.cancel_order_v2(order_id)
            rung["cancel_requested_at"] = datetime.now(tz=timezone.utc).isoformat()
            log.info("GTC LADDER CANCEL | %s %s @ $%.2f id=%s",
                     rec["ticker"], rec["side"], _f(rung.get("economic_price")), order_id)
        except Exception as exc:  # noqa: BLE001
            # It is normal for an already-filled/expired order to reject cancel.
            log.info("GTC ladder cancel skipped for %s: %s", order_id, exc)
    ledger_for_record(rec).save()


def opposite_position_side(side: str) -> Optional[str]:
    normalized = str(side).lower()
    if normalized == "yes":
        return "no"
    if normalized == "no":
        return "yes"
    return None


def create_inverse_prophet_shadow(primary: dict) -> Optional[dict]:
    """Paper-post the opposite frozen Prophet side without an order request.

    The primary record already contains the causal pre-open forecast, strike,
    and locked side. Reusing that immutable snapshot prevents the shadow from
    creating a late or price-selected opposite signal.
    """
    if not INVERSE_PROPHET_SHADOW_ENABLED:
        return None
    ticker = str(primary.get("ticker") or "")
    source_side = str(primary.get("side") or "").lower()
    shadow_side = opposite_position_side(source_side)
    if not ticker or shadow_side is None or inverse_shadow_tracker.already_shadowed(ticker):
        return None
    count = _f(primary.get("bet_amount_shares"), bet_count())
    shadow = {
        "ticker": ticker,
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "settle_et": primary.get("settle_et", ""),
        "source_prophet_side": source_side.upper(),
        "side": shadow_side.upper(),
        "decision_basis": "paper_only_inverse_of_frozen_prophet_side",
        "source_decision_basis": primary.get("decision_basis"),
        "btc_entry": primary.get("btc_entry"),
        "strike": primary.get("strike"),
        "p50_prediction": primary.get("p50_prediction"),
        "forecast_horizon_minutes": primary.get("forecast_horizon_minutes"),
        "forecast_created_at": primary.get("forecast_created_at"),
        "forecast_data_end": primary.get("forecast_data_end"),
        "forecast_bands": primary.get("forecast_bands"),
        "trade_kind": "BTC_PROPHET_INVERSE_SHADOW",
        "mode": "paper_only_no_exchange_orders",
        "bet_amount_shares": round(count, 2),
        "ladder_levels": list(LADDER_LEVELS),
        "rungs": [
            {
                "economic_price": level,
                "count": round(count, 2),
                "fill_count": 0.0,
                "fill_economic_price": None,
                "status": "simulated_resting",
                "order_id": None,
                "time_in_force": "paper_only_no_order",
            }
            for level in LADDER_LEVELS
        ],
        "paper_posted_rungs": len(LADDER_LEVELS),
        "count": 0.0,
        "entry_price": 0.0,
        "order_submitted": "none — paper-only inverse shadow",
        "exit_method": "pending",
        "dry_run": True,
        "result": "pending",
        "profit_loss": 0.0,
    }
    if inverse_shadow_tracker.record_open(shadow):
        log.info(
            "INVERSE PROPHET SHADOW STARTED | %s source_prophet=%s shadow=%s rungs=$0.40/$0.30/$0.20/$0.10 "
            "qty=%.2f; paper only, no exchange order.",
            ticker, source_side.upper(), shadow_side.upper(), count,
        )
        return shadow
    return None


def create_prophet_selector_shadow(primary: dict, selection: Optional[dict]) -> Optional[dict]:
    """Paper-post the selector's pre-open-frozen side as a third ledger.

    The normal and inverse paper ladders remain available as baselines.  This
    ladder is the strategy that would have traded only the side selected by
    the previous settled windows.  It must stay paper-only here: a dry runner
    may never create an additional Kalshi order merely to evaluate a selector.
    """
    if not (DRY_RUN and PROPHET_SELECTOR_ENABLED and selection):
        return None
    ticker = str(primary.get("ticker") or "")
    source_side = str(selection.get("source_prophet_side") or "").lower()
    selected_side = str(selection.get("selected_side") or "").lower()
    if (not ticker or source_side not in ("yes", "no") or selected_side not in ("yes", "no")
            or selector_tracker.already_shadowed(ticker)):
        return None
    count = _f(primary.get("bet_amount_shares"), bet_count())
    selector = {
        "ticker": ticker,
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "settle_et": primary.get("settle_et", ""),
        "source_prophet_side": source_side.upper(),
        "side": selected_side.upper(),
        "selector_mode": selection.get("selected_mode", "inverse"),
        "selector_snapshot": selection,
        "decision_basis": "trailing_win_rate_prophet_side_selector_frozen_before_open",
        "source_decision_basis": primary.get("decision_basis"),
        "btc_entry": primary.get("btc_entry"),
        "strike": primary.get("strike"),
        "p50_prediction": primary.get("p50_prediction"),
        "forecast_horizon_minutes": primary.get("forecast_horizon_minutes"),
        "forecast_created_at": primary.get("forecast_created_at"),
        "forecast_data_end": primary.get("forecast_data_end"),
        "forecast_bands": primary.get("forecast_bands"),
        "trade_kind": "BTC_PROPHET_WIN_RATE_SELECTOR",
        "mode": "paper_only_no_exchange_orders",
        "bet_amount_shares": round(count, 2),
        "ladder_levels": list(LADDER_LEVELS),
        "rungs": [
            {
                "economic_price": level,
                "count": round(count, 2),
                "fill_count": 0.0,
                "fill_economic_price": None,
                "status": "simulated_resting",
                "order_id": None,
                "time_in_force": "paper_only_no_order",
            }
            for level in LADDER_LEVELS
        ],
        "paper_posted_rungs": len(LADDER_LEVELS),
        "count": 0.0,
        "entry_price": 0.0,
        "order_submitted": "none — paper-only Prophet selector",
        "exit_method": "pending",
        "dry_run": True,
        "result": "pending",
        "profit_loss": 0.0,
    }
    if selector_tracker.record_open(selector):
        log.info(
            "PROPHET SELECTOR STARTED | %s source_prophet=%s selected=%s mode=%s "
            "votes=N%s/I%s paired=%s rungs=$0.40/$0.30/$0.20/$0.10 qty=%.2f; "
            "paper only, no exchange order.",
            ticker, source_side.upper(), selected_side.upper(),
            str(selection.get("selected_mode", "inverse")).upper(),
            selection.get("normal_votes", 0), selection.get("inverse_votes", 0),
            selection.get("paired_signals_available", 0), count,
        )
        return selector
    return None


async def execute_locked_ladder(rest: KalshiREST, ct: str) -> None:
    """Lock the Prophet side once and pre-post exactly four same-side GTC buys."""
    market = None
    for _ in range(STRIKE_RETRIES):
        market = await resolve_active_market(rest)
        if market and market.get("target") is not None:
            break
        await asyncio.sleep(2)
    if market is None or market.get("target") is None:
        log.warning("No live BTC market/strike for %s; will not post a ladder.", ct)
        return

    ct = market["ticker"]
    age = seconds_since_ticker_open(ct)
    if not is_within_open_trade_grace(age):
        log.info("LADDER SKIPPED | %s discovered %.1fs after open (grace %.1fs).",
                 ct, age if age is not None else -1, OPEN_TRADE_GRACE_S)
        handled_windows.add(ct)
        return
    if tracker.already_traded(ct):
        handled_windows.add(ct)
        return

    forecast_rec = preopen_forecasts.get(ct)
    if not forecast_rec:
        log.warning("No cached Prophet forecast for %s; no side is locked and no order is sent.", ct)
        handled_windows.add(ct)
        return
    forecast = forecast_rec["forecast"]
    strike = float(market["target"])
    source_side, source_decision = decide_side_from_forecast(strike, forecast)
    if source_side is None:
        log.info("Prophet P50 equals strike for %s; no side is locked.", ct)
        handled_windows.add(ct)
        return
    selection = prophet_selector_decision(source_side) if PROPHET_SELECTOR_ENABLED else None
    selector_applied = bool(not DRY_RUN and selection is not None)
    side = str(selection["selected_side"]).lower() if selector_applied else source_side
    decision = source_decision
    if selector_applied:
        decision = (
            f"{source_decision}; live selector={selection['selected_mode'].upper()} "
            f"from N{selection['normal_votes']}/I{selection['inverse_votes']} trailing-window votes"
        )
    settle_et = market.get("settle_et")
    expiry = _market_close_epoch(settle_et)
    if expiry is None:
        log.error("LADDER BLOCKED | %s has no parsed market close; refusing GTC orders.", ct)
        handled_windows.add(ct)
        return

    log.info("SIDE LOCKED | %s %s by %s (%s; p50=$%.2f strike=$%.2f). "
             "%s only $0.40/$0.30/$0.20/$0.10 GTC buys; no opposite-side order exists.",
             ct, side.upper(), "Prophet selector" if selector_applied else "Prophet", decision,
             forecast["p50"], strike,
             "Paper-posting" if DRY_RUN else "Posting")
    rungs = []
    for level in LADDER_LEVELS:
        book_side, api_price = _ladder_order_terms(side, level)
        log.info("GTC LADDER LIMIT | %s %s economic=$%.2f api_yes=%s qty=%.2f expires=%d",
                 ct, side.upper(), level, api_price, bet_count(), expiry)
        response, _ = await _submit(
            rest, ticker=ct, side=book_side, price=api_price, count=bet_count(),
            reduce_only=False, tag=f"PROPHET {side.upper()} RUNG ${level:.2f}",
            tif=ORDER_TIF, expiration_time=expiry)
        accepted = DRY_RUN or response is not None
        # Dry runs begin as resting paper orders.  They become simulated quote
        # hits later only when the observed selected-side price reaches a rung.
        fill_count = 0.0 if DRY_RUN else _f(getattr(response, "fill_count", 0.0), 0.0)
        average_yes = _to_dollars(getattr(response, "average_fill_price", None))
        fill_cost = (level if average_yes is None
                     else (average_yes if side == "yes" else 1.0 - average_yes))
        rungs.append({
            "economic_price": level, "api_yes_price": api_price,
            "count": bet_count(), "fill_count": round(fill_count, 2),
            "fill_economic_price": round(fill_cost, 4),
            "order_id": getattr(response, "order_id", None) if response is not None else None,
            "status": "simulated_resting" if (accepted and DRY_RUN) else (
                "accepted" if accepted else "submit_failed"),
            "time_in_force": ORDER_TIF, "expiration_time": expiry,
        })

    handled_windows.add(ct)
    accepted_rungs = sum(1 for rung in rungs if rung["status"] == "accepted")
    rec = {
        "ticker": ct,
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "settle_et": settle_et.isoformat() if settle_et else "",
        "source_prophet_side": source_side.upper(),
        "side": side.upper(),
        "decision_basis": (
            "trailing_win_rate_prophet_selector_live_locked_side" if selector_applied
            else "prophet_p50_vs_live_strike_locked_side"
        ),
        "source_decision_basis": "prophet_p50_vs_live_strike_locked_side",
        "selector_applied": selector_applied,
        "selector_mode": selection.get("selected_mode") if selector_applied else "normal_baseline",
        "selector_snapshot": selection,
        "btc_entry": round(float(forecast_rec["btc_close"]), 2),
        "strike": round(strike, 2), "p50_prediction": round(float(forecast["p50"]), 2),
        "forecast_horizon_minutes": int(forecast_rec["horizon_minutes"]),
        "forecast_created_at": forecast_rec.get("created_at"),
        "forecast_data_end": str(forecast_rec["data_end"]),
        "forecast_bands": {k: round(forecast[k], 2) for k, _ in _QMAP},
        "trade_kind": "BTC_PROPHET_LOCKED_LADDER", "bet_amount_shares": bet_count(),
        "ladder_levels": list(LADDER_LEVELS), "rungs": rungs,
        "ladder_preposted_complete": accepted_rungs == len(LADDER_LEVELS),
        "count": round(sum(_f(r["fill_count"]) for r in rungs), 2),
        "entry_price": 0.0, "order_submitted": f"{accepted_rungs}/{len(LADDER_LEVELS)} accepted",
        "exit_method": "pending", "dry_run": DRY_RUN,
        "result": "pending", "profit_loss": 0.0,
    }
    tracker.record_open(rec)
    # This must follow the primary record creation so it inherits exactly the
    # frozen Prophet decision. It never calls _submit or creates an order ID.
    create_inverse_prophet_shadow(rec)
    create_prophet_selector_shadow(rec, selection)
    log.info("%s | %s %s accepted=%d/%d. The side remains locked through settlement.",
             "GTC LADDER PAPER-POSTED" if DRY_RUN else "GTC LADDER POSTED",
             ct, side.upper(), accepted_rungs, len(LADDER_LEVELS))


# ─────────────────────────────────────────────────────────────────────────────
# ETH hedge limit order  (submitted immediately; monitor confirms fills)
# ─────────────────────────────────────────────────────────────────────────────
def _eth_hedge_expiration_epoch(rec: dict) -> Optional[int]:
    """Expire a resting ETH hedge at the paired BTC/ETH market settlement."""
    try:
        settle = datetime.fromisoformat(rec["settle_et"])
        if settle.tzinfo is None:
            settle = settle.replace(tzinfo=ET)
        return int(settle.astimezone(timezone.utc).timestamp())
    except Exception:  # noqa: BLE001
        return None


def _record_eth_hedge_fill(rec: dict, hedge: dict, total_filled: float,
                           average_yes: Optional[float] = None) -> None:
    """Record only newly filled ETH contracts; preserve partial-fill accuracy."""
    previously_recorded = _f(hedge.get("recorded_fill_count"), 0.0)
    new_fill = max(0.0, float(total_filled) - previously_recorded)
    if new_fill < 0.005:
        return

    side = str(hedge["side"]).lower()
    entry = _f(hedge.get("target_entry_price"), 0.0)
    if average_yes is not None and 0.01 <= average_yes <= 0.99:
        entry = position_price_from_yes(side, average_yes)
    filled_at = datetime.now(tz=timezone.utc).isoformat()
    hedge_rec = {
        "ticker": hedge["ticker"],
        "timestamp": filled_at,
        "settle_et": rec.get("settle_et", ""),
        "side": side.upper(),
        "entry_price": round(entry, 4),
        "btc_entry": rec.get("btc_entry"),
        "strike": None,
        "p50_prediction": rec.get("p50_prediction"),
        "btc_quantile_position": rec.get("btc_quantile_position"),
        "count": round(new_fill, 2),
        "bet_amount_shares": round(new_fill, 2),
        "loss_streak": rec.get("loss_streak", 0),
        "bet_multiplier": rec.get("bet_multiplier", 1.0),
        "trade_kind": "ETH_HEDGE",
        "linked_btc_ticker": rec["ticker"],
        "decision_basis": "immediate_opposite_eth_limit_after_btc_loss",
        "order_submitted": "success",
        "exit_method": "pending",
        "dry_run": DRY_RUN,
        "result": "pending",
        "profit_loss": 0.0,
    }
    tracker.record_open(hedge_rec)
    hedge["recorded_fill_count"] = round(total_filled, 2)
    hedge["filled_at"] = filled_at
    hedge["entry_price"] = round(entry, 4)
    log.info("ETH HEDGE FILL %s %s entry=$%.2f new=%.2f total=%.2f linked_btc=%s",
             hedge["ticker"], side.upper(), entry, new_fill, total_filled,
             rec["ticker"])


async def _submit_eth_hedge_limit(rest: KalshiREST, rec: dict) -> None:
    """Place the paired ETH resting limit immediately after the BTC fill."""
    hedge = rec.get("eth_hedge")
    if not isinstance(hedge, dict) or hedge.get("status") != "pending_submission":
        return
    ticker = hedge["ticker"]
    side = str(hedge["side"]).lower()
    count = _f(hedge.get("count"), _f(rec.get("count"), ARBITRAGE_SHARES))
    book_side = BookSide.BID if side == "yes" else BookSide.ASK
    api_price = hedge["api_price"]
    expiration_time = _eth_hedge_expiration_epoch(rec)
    log.info("ETH hedge submit now: %s %s limit=%s count=%.2f expires=%s "
             "(BTC %s entry=$%.2f, paired max=$%.2f)",
             ticker, side.upper(), api_price, count, expiration_time,
             rec["ticker"], _f(rec.get("entry_price")), ARBITRAGE_MAX_PAIR_COST)
    resp, filled = await _submit(
        rest, ticker=ticker, side=book_side, price=api_price, count=count,
        reduce_only=False, tag=f"ETH HEDGE BUY {side.upper()}",
        tif=ETH_HEDGE_TIF, expiration_time=expiration_time)
    hedge["submitted_at"] = datetime.now(tz=timezone.utc).isoformat()
    hedge["order_id"] = getattr(resp, "order_id", None) if resp is not None else None
    hedge["expiration_time"] = expiration_time
    hedge["time_in_force"] = ETH_HEDGE_TIF
    hedge["recorded_fill_count"] = 0.0
    if resp is None and not DRY_RUN:
        hedge["status"] = "submit_failed"
        tracker.save()
        return

    total_filled = count if DRY_RUN and filled else _f(getattr(resp, "fill_count"), 0.0)
    avg_yes = _to_dollars(getattr(resp, "average_fill_price", None)) if resp else None
    _record_eth_hedge_fill(rec, hedge, total_filled, avg_yes)
    if total_filled >= count - 0.005:
        hedge["status"] = "filled"
    elif total_filled > 0.0:
        hedge["status"] = "partially_filled"
    else:
        hedge["status"] = "open"
    tracker.save()


async def _monitor_eth_hedge(rest: KalshiREST, rec: dict) -> None:
    """Confirm fills on the ETH limit already submitted at the BTC opening."""
    hedge = rec.get("eth_hedge")
    if not isinstance(hedge, dict) or hedge.get("status") not in ("open", "partially_filled"):
        return
    order_id = hedge.get("order_id")
    if not order_id:
        return
    response = await rest.orders.get_order(order_id)
    order = getattr(response, "order", None)
    if order is None:
        return
    total_filled = _f(getattr(order, "fill_count_fp", 0.0))
    count = _f(hedge.get("count"), _f(rec.get("count"), ARBITRAGE_SHARES))
    _record_eth_hedge_fill(rec, hedge, total_filled)
    if total_filled >= count - 0.005:
        hedge["status"] = "filled"
        log.info("ETH hedge limit fully filled for %s.", rec["ticker"])
    elif total_filled > 0.0:
        hedge["status"] = "partially_filled"
    tracker.save()


async def eth_hedge_monitor(rest: KalshiREST) -> None:
    """Track fills on ETH limits submitted immediately at paired BTC entry."""
    log.info("ETH hedge monitor started — poll every %.0fs for submitted limits.",
             ETH_HEDGE_POLL_S)
    while True:
        await asyncio.sleep(ETH_HEDGE_POLL_S)
        now = datetime.now(tz=timezone.utc)
        for rec in tracker.find_pending():
            if rec.get("trade_kind", "BTC_PRIMARY") != "BTC_PRIMARY":
                continue
            hedge = rec.get("eth_hedge")
            if not isinstance(hedge, dict) or hedge.get("status") not in ("open", "partially_filled"):
                continue
            try:
                settle = datetime.fromisoformat(rec["settle_et"])
                if settle.tzinfo is None:
                    settle = settle.replace(tzinfo=ET)
                if now >= settle:
                    hedge["status"] = "expired"
                    hedge["expired_at"] = now.isoformat()
                    tracker.save()
                    continue
            except Exception:  # noqa: BLE001
                continue
            try:
                await _monitor_eth_hedge(rest, rec)
            except Exception as exc:  # noqa: BLE001
                log.warning("ETH hedge monitor failed for %s: %s",
                            rec.get("ticker"), exc)


def _build_eth_hedge(rec: dict) -> dict:
    """Build the opposite ETH limit that keeps the full paired cost <= $0.90."""
    entry_price = _f(rec.get("entry_price"), 0.0)
    count = _f(rec.get("count"), ARBITRAGE_SHARES)
    target_price = eth_hedge_target_price(entry_price)
    if target_price is None:
        log.info("ETH hedge skipped: BTC %s entry=$%.2f leaves no ETH price >= $0.01 "
                 "while keeping paired cost <= $%.2f.",
                 rec["side"], entry_price, ARBITRAGE_MAX_PAIR_COST)
        return {
            "status": "skipped",
            "reason": "btc_entry_too_high_for_pair_cost_cap",
            "btc_entry_price": round(entry_price, 4),
            "max_pair_cost": ARBITRAGE_MAX_PAIR_COST,
        }

    try:
        settle = datetime.fromisoformat(rec["settle_et"])
        if settle.tzinfo is None:
            settle = settle.replace(tzinfo=ET)
        eth_ticker = build_ticker(ETH_SERIES_TICKER, settle)
    except Exception:  # noqa: BLE001
        eth_ticker = rec["ticker"].replace(SERIES_TICKER, ETH_SERIES_TICKER, 1)
    eth_side, _, eth_api_price = eth_hedge_order(rec["side"], target_price)
    hedge = {
        "status": "pending_submission",
        "ticker": eth_ticker,
        "side": eth_side.upper(),
        "target_entry_price": round(target_price, 2),
        "api_price": eth_api_price,
        "count": count,
        "btc_side": rec["side"],
        "btc_entry_price": round(entry_price, 4),
        "discount_usd": ARBITRAGE_DISCOUNT_USD,
        "max_pair_cost": ARBITRAGE_MAX_PAIR_COST,
    }
    log.info("ETH HEDGE READY : BTC %s entry=$%.2f → submit %s %s limit <= $%.2f "
             "(api price %s), count %.2f; paired max cost $%.2f.",
             rec["side"], entry_price, eth_ticker, eth_side.upper(), target_price,
             eth_api_price, count, ARBITRAGE_MAX_PAIR_COST)
    return hedge


async def _reconcile_deferred_entry(rest: KalshiREST, rec: dict) -> None:
    """Top up an immediate base entry once its preceding BTC loss is published."""
    deferred = rec.get("deferred_hedge")
    if not isinstance(deferred, dict) or deferred.get("status") != "awaiting_btc_result":
        return
    prior_ticker = deferred.get("prior_btc_ticker")
    prior = next((t for t in tracker.trades if t.get("ticker") == prior_ticker), None)
    if not prior or prior.get("result") == "pending":
        return
    if prior.get("result") != "LOSS":
        deferred["status"] = "not_needed"
        deferred["prior_result"] = prior.get("result")
        tracker.save()
        return

    # Reproduce next_eth_hedge_state() for the known prior loss without being
    # obscured by this newer, still-pending base entry.
    prior_hedge = prior.get("eth_hedge")
    prior_fills = (_f(prior_hedge.get("recorded_fill_count"), 0.0)
                   if isinstance(prior_hedge, dict) else 0.0)
    multiplier = 1.0
    if bool(prior.get("arbitrage_active")) and prior_fills < 0.005:
        multiplier = max(1.0, _f(prior.get("bet_multiplier"), 1.0)) * LOSS_MULTIPLIER
    desired_count = bet_count(ARBITRAGE_SHARES * multiplier)
    base_count = _f(rec.get("count"), BET_AMOUNT_SHARES)
    top_up_count = bet_count(desired_count - base_count) if desired_count > base_count else 0.0
    deferred["prior_result"] = "LOSS"
    deferred["desired_count"] = desired_count
    deferred["multiplier"] = multiplier
    if top_up_count <= 0.0:
        deferred["status"] = "already_sized"
        tracker.save()
        return

    deferred["status"] = "reconciling"
    tracker.save()
    market = await rest.get_market(rec["ticker"])
    if market is None or not is_market_live(market):
        deferred["status"] = "top_up_unavailable"
        tracker.save()
        log.error("Deferred BTC loss for %s arrived after %s was no longer active; "
                  "the original %.2f-share entry remains open.",
                  prior_ticker, rec["ticker"], base_count)
        return

    side = str(rec["side"]).lower()
    book_side = BookSide.BID if side == "yes" else BookSide.ASK
    price = YES_BUY_PRICE if side == "yes" else NO_BUY_PRICE
    log.info("BTC LOSS RECONCILE : %s lost → top up %s %s by %.2f shares "
             "(%.2f → %.2f) and place matched ETH limit.",
             prior_ticker, rec["ticker"], side.upper(), top_up_count,
             base_count, desired_count)
    resp, filled = await _submit(
        rest, ticker=rec["ticker"], side=book_side, price=price, count=top_up_count,
        reduce_only=False, tag=f"BTC LOSS TOP-UP {side.upper()}")
    if not filled:
        deferred["status"] = "top_up_unfilled"
        tracker.save()
        return

    added_count = top_up_count if DRY_RUN else _f(getattr(resp, "fill_count"), 0.0)
    if added_count < 0.005:
        deferred["status"] = "top_up_unfilled"
        tracker.save()
        return
    avg_yes = _to_dollars(getattr(resp, "average_fill_price", None)) if resp else None
    added_entry = (_f(rec.get("entry_price"), 0.5) if avg_yes is None
                   else position_price_from_yes(side, avg_yes))
    total_count = round(base_count + added_count, 2)
    blended_entry = ((base_count * _f(rec.get("entry_price"), 0.5)
                      + added_count * added_entry) / total_count)
    rec["count"] = total_count
    rec["bet_amount_shares"] = total_count
    rec["entry_price"] = round(blended_entry, 4)
    rec["loss_streak"] = max(1, int(_f(prior.get("loss_streak"), 0)) + 1)
    rec["bet_multiplier"] = round(multiplier, 4)
    rec["hedge_trigger_reason"] = "deferred_btc_loss_reconciled"
    rec["arbitrage_active"] = True
    rec["btc_top_up"] = {
        "count": round(added_count, 2), "entry_price": round(added_entry, 4),
        "order_id": getattr(resp, "order_id", None) if resp is not None else None,
        "at": datetime.now(tz=timezone.utc).isoformat(),
    }
    deferred["status"] = "reconciled" if total_count >= desired_count - 0.005 else "partially_reconciled"
    rec["eth_hedge"] = _build_eth_hedge(rec)
    tracker.save()
    if rec["eth_hedge"].get("status") == "pending_submission":
        await _submit_eth_hedge_limit(rest, rec)


async def _reconcile_entries_waiting_on_btc_result(rest: KalshiREST, settled_btc: dict) -> None:
    """Reconcile each immediate entry that was waiting on this BTC result."""
    for rec in list(tracker.find_pending()):
        deferred = rec.get("deferred_hedge")
        if (isinstance(deferred, dict)
                and deferred.get("prior_btc_ticker") == settled_btc.get("ticker")):
            await _reconcile_deferred_entry(rest, rec)


# ─────────────────────────────────────────────────────────────────────────────
# Trade execution for a single 15-minute window
# ─────────────────────────────────────────────────────────────────────────────
async def execute_window_trade(rest: KalshiREST, ct: str, nt: str) -> None:
    """Compatibility entry point for the locked-side BTC-only GTC ladder."""
    del nt
    await execute_locked_ladder(rest, ct)
    return

    """Legacy unreachable execution path retained temporarily for source diff context."""

    # 1) Resolve the active market + strike (retry — a just-opened market can be
    #    missing floor_strike for a few seconds).
    market = None
    for _ in range(STRIKE_RETRIES):
        market = await resolve_active_market(rest)
        if market and market.get("target") is not None:
            break
        await asyncio.sleep(2)
    if market is None or market.get("target") is None:
        log.warning("No live market / strike for %s yet — retrying while the "
                    "new-market entry window remains open.", ct)
        return

    ct = market["ticker"]                       # authoritative ticker from Kalshi
    strike = float(market["target"])
    seconds_since_open = seconds_since_ticker_open(ct)
    if not is_within_open_trade_grace(seconds_since_open):
        log.info("Market %s is %.1fs old after strike resolution; skip late "
                 "entry and wait for the next opening.",
                 ct, seconds_since_open if seconds_since_open is not None else -1)
        handled_windows.add(ct)
        return
    if tracker.already_traded(ct):
        log.info("Window %s already traded — skip (one order per window).", ct)
        handled_windows.add(ct)
        return

    # 2) The opening order must use the pre-open cache. Never fit Prophet in the
    #    order path: a cache miss means no immediate decision, so skip safely.
    forecast_rec = preopen_forecasts.get(ct)
    if forecast_rec:
        log.info("Using cached pre-open forecast for %s created at %s.",
                 ct, forecast_rec.get("created_at"))
    else:
        log.warning("No pre-open forecast cached for %s — SKIP window rather "
                    "than delay the opening order with a live forecast.", ct)
        handled_windows.add(ct)
        return

    forecast = forecast_rec["forecast"]
    p50 = forecast["p50"]
    btc_close = float(forecast_rec["btc_close"])
    quantile = float(forecast_rec["btc_quantile_position"])
    strike_quantile = percentile_of_price(strike, forecast)
    data_start = forecast_rec["data_start"]
    data_end = forecast_rec["data_end"]
    horizon = int(forecast_rec["horizon_minutes"])
    candle_count = int(forecast_rec["candles"])

    # 3) Decision: trade the newly-live contract by comparing its live strike to
    #    the settlement forecast p50.
    side, decision = decide_side_from_forecast(strike, forecast)
    if side is None:
        log.info("Forecast P50 == live strike for %s — no directional edge, SKIP.",
                 ct)
        handled_windows.add(ct)
        return

    # Comparisons.
    btc_vs_strike = "ABOVE" if btc_close > strike else "BELOW"
    btc_vs_p50    = "ABOVE" if btc_close > p50 else "BELOW"
    forecast_vs_strike = "P50 ABOVE strike" if p50 > strike else "P50 BELOW strike"

    # Entry cost per contract (for sizing + P&L accounting).
    yes_p = get_active_yes_price(market)
    if side == "yes":
        entry_price = yes_p if yes_p is not None else 0.5
    else:
        entry_price = (1.0 - yes_p) if yes_p is not None else 0.5

    # The normal BTC-only path is always BET_AMOUNT_SHARES. Settlement runs in
    # the background and this snapshot never waits for it; only results already
    # booked before the open can affect this immediate order's hedge sizing.
    deferred_prior = latest_closed_pending_btc()
    loss_streak = tracker.current_loss_streak()
    hedge_state = tracker.next_eth_hedge_state()
    hedge_active = bool(hedge_state["active"])
    multiplier = float(hedge_state["multiplier"])
    count = bet_count(
        ARBITRAGE_SHARES * multiplier if hedge_active else BET_AMOUNT_SHARES)
    if hedge_active:
        log.info("ETH hedge protocol active after %s: %.2f arb shares × %.6g "
                 "= %.2f per leg / %.2f paired shares (%s).",
                 hedge_state.get("previous_ticker"), ARBITRAGE_SHARES,
                 multiplier, count, count * 2, hedge_state["reason"])
    elif deferred_prior is not None:
        log.info("Prior BTC %s is closed but unresolved: submit the live %.2f-share "
                 "base now; reconcile this same record to the loss pair if needed.",
                 deferred_prior["ticker"], count)
    est_cost = count * entry_price

    settle_et = market.get("settle_et")
    settle_s = settle_et.strftime("%Y-%m-%d %H:%M ET") if settle_et else "?"

    # 5) Full trade-submission log block.
    log.info(
        "\n"
        "==============================\n"
        "NEW KALSHI BTC 15M TRADE\n"
        "==============================\n"
        f"Market            : {ct}\n"
        f"Settlement        : {settle_s}\n"
        f"Forecast Source   : {forecast_rec.get('source_reason', 'pre-open cache')}\n"
        f"Historical Data   : {candle_count} candles loaded\n"
        f"Data Range        : {data_start} → {data_end}\n"
        f"Latest Candle     : {data_end}\n"
        f"BTC Forecast Close: ${btc_close:,.2f}\n"
        f"Live Kalshi Strike: ${strike:,.2f}\n"
        f"Prophet Forecast ({horizon} one-minute steps P50): ${p50:,.2f}\n"
        f"Forecast Bands (80% CI):\n"
        f"    P10: ${forecast['p10']:,.2f}\n"
        f"    P50: ${forecast['p50']:,.2f}\n"
        f"    P90: ${forecast['p90']:,.2f}\n"
        f"BTC Forecast Close Quantile : {quantile:.0f} percentile\n"
        f"Live Strike Quantile        : {strike_quantile:.0f} percentile\n"
        f"BTC Forecast Close vs Strike: {btc_vs_strike}\n"
        f"BTC Forecast Close vs P50   : {btc_vs_p50}\n"
        f"Forecast vs Strike          : {forecast_vs_strike}\n"
        f"Decision          : {decision}\n"
        f"BTC Loss Streak   : {loss_streak}\n"
        f"ETH Hedge         : {'armed after BTC fill' if hedge_active else 'inactive'} "
        f"(arb base {ARBITRAGE_SHARES:.2f}, multiplier ×{multiplier:.6g}; "
        f"{hedge_state['reason']})\n"
        f"Bet Size          : {count:.2f} contract(s) "
        f"@ ~${entry_price:.2f} = ~${est_cost:.4f}"
    )

    # 6) Submit exactly ONE entry order for this window. Recheck after the
    #    non-blocking decision work so no slow metadata response becomes a late
    #    entry.
    if not is_within_open_trade_grace(seconds_since_ticker_open(ct)):
        log.info("Opening order window expired for %s before submission — skip.", ct)
        handled_windows.add(ct)
        return
    enum  = BookSide.BID if side == "yes" else BookSide.ASK
    price = YES_BUY_PRICE if side == "yes" else NO_BUY_PRICE
    resp, filled = await _submit(rest, ticker=ct, side=enum, price=price, count=count,
                                 reduce_only=False, tag=f"BUY {side.upper()}")

    log.info("Order Submitted   : %s", "success" if filled else "failure")
    log.info("==============================")

    # 7) Record (only when a position actually opened).
    handled_windows.add(ct)
    if not filled:
        log.warning("BUY %s not filled for %s — no position, not recorded.",
                    side.upper(), ct)
        return

    # Prefer the ACTUAL average fill price (YES terms in the API response) for
    # hedge math and P&L accounting; fall back to the quote estimate.
    avg_yes = _to_dollars(getattr(resp, "average_fill_price", None))
    if avg_yes is not None and 0.01 <= avg_yes <= 0.99:
        entry_price = avg_yes if side == "yes" else round(1.0 - avg_yes, 4)

    deferred_hedge = None
    if not hedge_active and deferred_prior is not None:
        deferred_hedge = {
            "status": "awaiting_btc_result",
            "prior_btc_ticker": deferred_prior["ticker"],
            "base_count": count,
            "created_at": datetime.now(tz=timezone.utc).isoformat(),
        }

    rec = {
        "ticker": ct,
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "settle_et": settle_et.isoformat() if settle_et else "",
        "side": side.upper(),
        "entry_price": round(float(entry_price), 4),
        "btc_entry": round(btc_close, 2),
        "strike": round(strike, 2),
        "p50_prediction": round(p50, 2),
        "btc_quantile_position": round(quantile, 2),
        "strike_quantile_position": round(strike_quantile, 2),
        "decision_basis": "forecast_p50_vs_live_strike",
        "forecast_horizon_minutes": horizon,
        "forecast_created_at": forecast_rec.get("created_at"),
        "forecast_data_end": str(data_end),
        "forecast_bands": {k: round(forecast[k], 2) for k, _ in _QMAP},
        "count": count,
        "bet_amount_shares": count,
        "loss_streak": loss_streak,
        "bet_multiplier": round(multiplier, 4),
        "hedge_trigger_reason": hedge_state["reason"],
        "trade_kind": "BTC_PRIMARY",
        "arbitrage_active": hedge_active,
        "eth_hedge": None,
        "deferred_hedge": deferred_hedge,
        "order_submitted": "success" if filled else "failure",
        "exit_method": "pending",
        "dry_run": DRY_RUN,
        "result": "pending",
        "profit_loss": 0.0,
    }
    if hedge_active:
        rec["eth_hedge"] = _build_eth_hedge(rec)
    tracker.record_open(rec)
    if isinstance(rec["eth_hedge"], dict) and rec["eth_hedge"].get("status") == "pending_submission":
        await _submit_eth_hedge_limit(rest, rec)
    if isinstance(deferred_hedge, dict):
        await _reconcile_deferred_entry(rest, rec)


# ─────────────────────────────────────────────────────────────────────────────
# Strategy loop  (pre-open forecast; one entry as the new market opens)
# ─────────────────────────────────────────────────────────────────────────────
async def strategy_loop(rest: KalshiREST, market_ws: KalshiMarketWS,
                        started_at: float) -> None:
    log.info("Prophet strategy loop started — pre-open forecast, one locked-side "
             "BTC GTC ladder per fresh 15-min window.")
    while True:
        if (time.time() - started_at) / 60.0 >= RUNTIME_LIMIT_MIN:
            log.info("Runtime limit (%.0f min) reached — clean exit. "
                     "Open positions settle automatically.", RUNTIME_LIMIT_MIN)
            return

        ct, nt = current_and_next_tickers()
        market_ws.set_tickers((ct, nt))
        await maybe_prepare_next_window_forecast(ct, nt)

        # Trade this window exactly once (skip if handled this run or already
        # traded in a prior run / restart).
        if ct not in handled_windows and not tracker.already_traded(ct):
            seconds_since_open = seconds_since_ticker_open(ct)
            if not is_within_open_trade_grace(seconds_since_open):
                log.info("Window %s opened %.0fs ago (> %.0fs grace) — skip late "
                         "ladder and wait to pre-forecast the next market.",
                         ct, seconds_since_open if seconds_since_open is not None else -1,
                         OPEN_TRADE_GRACE_S)
                handled_windows.add(ct)
                await asyncio.sleep(POLL_INTERVAL_S)
                continue
            log_next_ticker_prediction()
            try:
                await execute_window_trade(rest, ct, nt)
            except Exception as exc:  # noqa: BLE001
                log.error("execute_window_trade(%s) failed: %s", ct, exc)
                handled_windows.add(ct)   # don't hammer a broken window all run

        await asyncio.sleep(POLL_INTERVAL_S)


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────
async def main_async() -> None:
    ct, nt = current_and_next_tickers()
    log.info("=" * 68)
    log.info("  BTC Kalshi 15-min algo-trader (ASYNC) — PROPHET strategy")
    log.info("  Now (UTC)       : %s", datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    log.info("  Current window  : %s", ct)
    log.info("  Kalshi REST/WS  : %s", KALSHI_BASE_URL)
    log.info("  Demo / DRY_RUN  : %s / %s", KALSHI_DEMO, DRY_RUN)
    log.info("  BTC rung size   : %.2f contracts/rung  (shares, fractional "
             "at 0.01 granularity — NOT dollars)", BET_AMOUNT_SHARES)
    log.info("  Locked ladder   : exactly $0.40/$0.30/$0.20/$0.10 on the one "
             "Prophet-selected BTC side; no ETH, hedge, or loss multiplier.")
    log.info("  Data / horizon  : %d 1-min candles → fixed %d-min forecast",
             HISTORY_MINUTES, FORECAST_MINUTES)
    log.info("  Pre-open timing : forecast %.0fs before next open; ladder-start grace %.0fs",
             PREOPEN_FORECAST_LEAD_S, OPEN_TRADE_GRACE_S)
    log.info("  Uncertainty     : %d samples", UNCERTAINTY_SAMPLES)
    log.info("  Runtime limit   : %.0f min", RUNTIME_LIMIT_MIN)
    log.info("=" * 68)
    log_next_ticker_prediction()

    if not KALSHI_API_KEY_ID:
        raise SystemExit("Missing credentials — set KALSHI_API_KEY_ID (and "
                         "KALSHI_PEM_PATH or KALSHI_PRIVATE_KEY)")

    global start_balance
    rest = KalshiREST()
    try:
        bal = await rest.get_balance_dollars()
        start_balance = bal if bal is not None else 0.0
        log.info("Kalshi auth OK – balance: $%.2f", start_balance)
    except Exception as exc:
        log.error("Kalshi auth failed: %s", exc)
        await rest.close()
        raise

    await report_portfolio(rest)   # initial portfolio + performance snapshot

    market_ws = KalshiMarketWS(rest.auth)
    btc_tickers = current_and_next_tickers()
    market_ws.set_tickers(btc_tickers)
    ws_task  = asyncio.create_task(market_ws.run(), name="kalshi-ws")
    set_task = asyncio.create_task(settlement_checker(rest), name="settlement-checker")
    rpt_task = asyncio.create_task(portfolio_reporter(rest), name="portfolio-reporter")
    try:
        await strategy_loop(rest, market_ws, started_at=time.time())
    finally:
        for t in (ws_task, set_task, rpt_task):
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
        await report_portfolio(rest)   # final report before exit
        await rest.close()


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
