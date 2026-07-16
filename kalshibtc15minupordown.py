"""
kalshibtc15minupordown.py
─────────────────────────────────────────────────────────────────────────────
BTC 15-min Kalshi market algo-trader  (fully ASYNC) — PROPHET forecast strategy.

STRATEGY  (Prophet 15-minute BTC forecast)
──────────────────────────────────────────
  At the start of every Kalshi KXBTC15M 15-minute window the bot:

    1. Detects the new active KXBTC15M market and its strike (floor_strike).
    2. Downloads the latest 500 one-minute BTC/USD candles (Yahoo Finance,
       symbol BTC-USD) and validates them (1-min spacing, fresh/not stale,
       required 15-min boundary timestamps present). Bad/stale data → SKIP.
    3. Fits Facebook Prophet on log(close) and forecasts 15 minutes ahead.
       The minute-15 median (yhat, back-transformed with exp) is the "p50".
    4. Decides ONE trade for the window:
            current BTC close  <  p50   →  BUY YES  (UP)
            current BTC close  >  p50   →  BUY NO   (DOWN)
    5. Records the trade and, after settlement, computes win/loss + P&L.

  Exactly one order per 15-minute window (never re-enters the same contract).
  Prophet also yields p01/p10/p25/p50/p75/p90/p99 bands; the current BTC price's
  percentile position within that forecast distribution is logged and stored.

DATA SOURCES
────────────
  • Yahoo Finance (yfinance) → 1-minute BTC-USD OHLC history (24/7)
  • Kalshi market WebSocket  → live ticker/trade for the active contract
  • Kalshi REST (kalshi-python-async, V2) → market metadata (strike/result),
                               balance, MARKET orders

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
    BET_AMOUNT_USD        notional $ per buy (default 1 → 1 contract)
    HISTORY_MINUTES       1-min candles pulled per forecast (default 500)
    FORECAST_MINUTES      forecast horizon in minutes (default 15)
    UNCERTAINTY_SAMPLES   Prophet uncertainty samples (default 1000)
    DATA_MAX_STALE_S      max age of newest candle before SKIP (default 600)
    YF_PERIOD             yfinance download period (default "2d")
    RUNTIME_LIMIT_MIN     clean-exit after N minutes (default 345 = 5h45m)
    REPORT_INTERVAL_S     performance/portfolio report cadence (default 30)
    TRADE_HISTORY_FILE    settled-trade journal (default trade_history.json)
    TRADED_TICKERS_FILE   per-window dedupe store (default traded_market_tickers.json)
    KALSHI_WS_VERBOSE     "true" — log every Kalshi WS message
"""

from __future__ import annotations

import asyncio
import json
import logging
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

# ── Bet sizing ──────────────────────────────────────────────────────────────
# Initial bet amount in USD notional per buy. Each Kalshi contract settles to
# $1, so $1 of notional == 1 contract.  Change here (or via env) to scale up.
BET_AMOUNT_USD    = float(os.getenv("BET_AMOUNT_USD", "1"))

# ── Prophet / data settings ───────────────────────────────────────────────────
HISTORY_MINUTES     = int(float(os.getenv("HISTORY_MINUTES", "500")))
FORECAST_MINUTES    = int(float(os.getenv("FORECAST_MINUTES", "15")))
UNCERTAINTY_SAMPLES = int(float(os.getenv("UNCERTAINTY_SAMPLES", "1000")))
DATA_MAX_STALE_S    = float(os.getenv("DATA_MAX_STALE_S", "600"))   # newest candle age
YF_PERIOD           = os.getenv("YF_PERIOD", "2d")

RUNTIME_LIMIT_MIN = float(os.getenv("RUNTIME_LIMIT_MIN", "345"))
REPORT_INTERVAL_S = float(os.getenv("REPORT_INTERVAL_S", "30"))    # report cadence
POLL_INTERVAL_S   = float(os.getenv("POLL_INTERVAL_S", "5"))       # window-watch cadence
SETTLE_CHECK_S    = float(os.getenv("SETTLE_CHECK_S", "20"))       # settlement poll cadence
STRIKE_RETRIES    = int(float(os.getenv("STRIKE_RETRIES", "8")))   # strike-resolution retries
KALSHI_WS_VERBOSE = os.getenv("KALSHI_WS_VERBOSE", "false").lower() in ("1", "true", "yes")

TRADE_HISTORY_FILE  = os.getenv("TRADE_HISTORY_FILE", "trade_history.json")
TRADED_TICKERS_FILE = os.getenv("TRADED_TICKERS_FILE", "traded_market_tickers.json")

ORDER_TIF      = "immediate_or_cancel"   # marketable IOC == market order
SERIES_TICKER  = "KXBTC15M"
YF_SYMBOL      = os.getenv("BTC_YF_SYMBOL", "BTC-USD")

# Marketable buy prices (ensure the IOC crosses so a position actually opens).
YES_BUY_PRICE  = "0.99"    # buy YES (BID) at the top of the book
NO_BUY_PRICE   = "0.01"    # buy NO (ASK / sell YES) at the bottom of the book

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

# Quantile band labels ↔ fractions (0.01 … 0.99), in ascending order.
_QMAP = [("p01", 0.01), ("p10", 0.10), ("p25", 0.25), ("p50", 0.50),
         ("p75", 0.75), ("p90", 0.90), ("p99", 0.99)]


def bet_count() -> int:
    """Number of contracts for one buy ($1 notional == 1 contract)."""
    return max(1, int(round(BET_AMOUNT_USD)))


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
def run_prophet_forecast(df: pd.DataFrame) -> Optional[dict]:
    """Fit Prophet on log(close) and forecast FORECAST_MINUTES ahead.

    Returns the minute-N (horizon end) quantile bands back-transformed to USD:
      {p01, p10, p25, p50, p75, p90, p99}.  p50 == exp(yhat).
    Blocking; call via loop.run_in_executor.
    """
    try:
        d = pd.DataFrame({
            "ds": df["ds"].dt.tz_localize(None),          # Prophet wants tz-naive
            "y":  np.log(df["close"].astype(float)),      # log transform
        })
        model = Prophet(
            daily_seasonality=False,
            weekly_seasonality=False,
            yearly_seasonality=False,
            interval_width=0.80,                          # overridden per band below
            uncertainty_samples=UNCERTAINTY_SAMPLES,
        )
        model.fit(d)
        future = model.make_future_dataframe(
            periods=FORECAST_MINUTES, freq="min", include_history=False)

        def band(iw: float) -> tuple:
            # Re-run predict at a given interval_width; yhat is unchanged by iw.
            model.interval_width = iw
            row = model.predict(future).iloc[-1]          # minute-N (horizon end)
            return (float(row["yhat"]), float(row["yhat_lower"]),
                    float(row["yhat_upper"]))

        yhat, l80, u80 = band(0.80)   # p10 / p90
        _,    l50, u50 = band(0.50)   # p25 / p75
        _,    l98, u98 = band(0.98)   # p01 / p99
        exp = np.exp
        return {
            "p01": float(exp(l98)), "p10": float(exp(l80)), "p25": float(exp(l50)),
            "p50": float(exp(yhat)), "p75": float(exp(u50)), "p90": float(exp(u80)),
            "p99": float(exp(u98)),
        }
    except Exception as exc:  # noqa: BLE001
        log.error("Prophet forecast failed: %s", exc)
        return None


def percentile_of_price(price: float, bands: dict) -> float:
    """Interpolated percentile rank (1–99) of `price` within the forecast bands."""
    prices = [bands[k] for k, _ in _QMAP]
    qs     = [q for _, q in _QMAP]
    if price <= prices[0]:
        return qs[0] * 100.0
    if price >= prices[-1]:
        return qs[-1] * 100.0
    return float(np.interp(price, prices, qs)) * 100.0


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
            self.subscribed = ()
            log.info("Kalshi WS: reconnecting in 5s …")
            await asyncio.sleep(5)

    async def _subscribe(self, ws, tickers: tuple) -> None:
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
            last    = _to_dollars(msg.get("last_price_dollars",
                                  msg.get("price", msg.get("last_price"))))
            q = kalshi_quotes.setdefault(ticker, {})
            if yes_bid is not None: q["yes_bid"] = yes_bid
            if yes_ask is not None: q["yes_ask"] = yes_ask
            if last    is not None: q["last"]    = last
            q["ts"] = datetime.now(tz=timezone.utc)
            if KALSHI_WS_VERBOSE:
                log.info("Kalshi WS ticker %s  yes_bid=%s yes_ask=%s last=%s",
                         ticker, q.get("yes_bid"), q.get("yes_ask"), q.get("last"))
        elif mtype == "trade":
            last = _to_dollars(msg.get("yes_price_dollars",
                               msg.get("yes_price", msg.get("price"))))
            if last is not None:
                q = kalshi_quotes.setdefault(ticker, {})
                q["last"] = last
                q["ts"]   = datetime.now(tz=timezone.utc)
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

    async def get_open_series_markets(self) -> list:
        try:
            resp = await self.events.get_events(
                series_ticker=SERIES_TICKER, status="open",
                with_nested_markets=True, limit=5)
            out = []
            for ev in (getattr(resp, "events", None) or []):
                out.extend(getattr(ev, "markets", None) or [])
            return out
        except Exception as exc:  # noqa: BLE001
            log.error("get_events fallback failed: %s", exc)
            return []


def _field(obj, *names):
    for n in names:
        v = getattr(obj, n, None)
        if v is not None:
            return v
    return None


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


async def resolve_active_market(rest: KalshiREST) -> Optional[dict]:
    """Resolve the current open KXBTC15M market and its target (floor_strike)."""
    ct, nt = current_and_next_tickers()
    parsed = parse_ticker(ct) or {}
    market = await rest.get_market(ct)
    if market is None:
        log.warning("Direct lookup of %s failed – trying events query", ct)
        markets = await rest.get_open_series_markets()
        if markets:
            market = markets[0]
            ct = _field(market, "ticker") or ct
            parsed = parse_ticker(ct) or parsed
    if market is None:
        log.info("No open KXBTC15M market found")
        return None
    return {"ticker": ct, "next_ticker": nt,
            "market_type": parsed.get("market_type", "?"),
            "settle_et": parsed.get("settle_et"),
            "target": _extract_target(market),
            "raw_market": market}


# ─────────────────────────────────────────────────────────────────────────────
# Orders (MARKET = marketable IOC)
# ─────────────────────────────────────────────────────────────────────────────
async def _submit(rest: KalshiREST, *, ticker, side: BookSide, price: str,
                  count: int, reduce_only: bool, tag: str) -> tuple:
    """Submit a marketable IOC order. Returns (resp, filled)."""
    global trades_placed, buys_placed, closes_placed, fills_count
    order_id = str(uuid.uuid4())
    log.info("ORDER %s  %s  side=%s price=%s count=%d (~$%.0f) reduce_only=%s "
             "ticker=%s id=%s",
             "[DRY-RUN]" if DRY_RUN else "[LIVE]", tag, side.value, price, count,
             BET_AMOUNT_USD, reduce_only, ticker, order_id)
    if DRY_RUN:
        log.info("DRY_RUN active — order NOT submitted.")
        return None, True                       # simulate a fill
    try:
        # The async create_order_v2 builds CreateOrderV2Request(**kwargs) internally,
        # so the order fields are passed DIRECTLY as kwargs (not wrapped).
        resp = await rest.orders.create_order_v2(
            ticker=ticker, side=side, count=f"{float(count):.2f}", price=price,
            time_in_force=ORDER_TIF, client_order_id=order_id,
            self_trade_prevention_type=SelfTradePreventionType.TAKER_AT_CROSS,
            reduce_only=reduce_only)
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
        else:
            log.warning("Order did NOT fill (IOC) — book too thin at price %s. "
                        "Kalshi min price is $0.01, so a side priced below the best "
                        "opposite quote cannot cross.", price)
        return resp, filled
    except Exception as exc:  # noqa: BLE001
        log.error("create_order_v2 failed: %s", exc)
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
            "btc_price": rec["btc_entry"], "p50": rec["p50_prediction"],
        }
        self._save_history()
        self._save_traded()

    def find_pending(self) -> list:
        return [t for t in self.trades if t.get("result") == "pending"]

    def settle(self, rec: dict, result: str, pnl: float) -> None:
        rec["result"] = result
        rec["profit_loss"] = round(float(pnl), 4)
        self._save_history()

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

        # streaks (chronological order = append order)
        longest_win = longest_loss = 0
        cw = cl = 0
        for t in settled:
            if t["result"] == "WIN":
                cw += 1; cl = 0; longest_win = max(longest_win, cw)
            else:
                cl += 1; cw = 0; longest_loss = max(longest_loss, cl)
        current_streak = 0
        current_kind = None
        for t in reversed(settled):
            if current_kind is None:
                current_kind = t["result"]; current_streak = 1
            elif t["result"] == current_kind:
                current_streak += 1
            else:
                break

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
            "current_streak": current_streak, "current_kind": current_kind,
            "longest_win": longest_win, "longest_loss": longest_loss,
            "max_drawdown": max_dd,
            "last": settled[-1] if settled else None,
        }


# Module-level tracker (created here so all coroutines share one instance).
tracker = PerformanceTracker(TRADE_HISTORY_FILE, TRADED_TICKERS_FILE)


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


async def portfolio_reporter(rest: KalshiREST) -> None:
    while True:
        await asyncio.sleep(REPORT_INTERVAL_S)
        try:
            await report_portfolio(rest)
        except Exception as exc:  # noqa: BLE001
            log.warning("portfolio report failed: %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# Settlement checker  (resolve pending trades → win/loss + P&L)
# ─────────────────────────────────────────────────────────────────────────────
async def settlement_checker(rest: KalshiREST) -> None:
    """Poll pending trades whose window has settled and finalize their result.

    Win/loss uses the Kalshi market `result` field ("yes"/"no"). P&L per contract:
        win  →  (1 - entry_price) * count
        loss →  -entry_price * count
    """
    while True:
        await asyncio.sleep(SETTLE_CHECK_S)
        pending = tracker.find_pending()
        if not pending:
            continue
        now = datetime.now(tz=timezone.utc)
        for rec in pending:
            try:
                settle = datetime.fromisoformat(rec["settle_et"])
            except Exception:  # noqa: BLE001
                continue
            if settle.tzinfo is None:
                settle = settle.replace(tzinfo=ET)
            if (now - settle).total_seconds() < 5:
                continue   # window not yet closed

            market = await rest.get_market(rec["ticker"])
            if market is None:
                continue
            result = _field(market, "result")
            if not result:
                continue   # not settled yet (Kalshi can lag a few seconds)
            result = str(result).lower()
            if result not in ("yes", "no"):
                continue

            win = (result == str(rec["side"]).lower())
            entry = _f(rec.get("entry_price"), 0.5)
            count = int(rec.get("count", bet_count()))
            pnl = (1.0 - entry) * count if win else -entry * count
            tracker.settle(rec, "WIN" if win else "LOSS", pnl)
            log.info("SETTLED %s  result=%s  our side=%s → %s  P&L $%+.2f",
                     rec["ticker"], result.upper(), rec["side"],
                     "WIN" if win else "LOSS", pnl)


# ─────────────────────────────────────────────────────────────────────────────
# Trade execution for a single 15-minute window
# ─────────────────────────────────────────────────────────────────────────────
async def execute_window_trade(rest: KalshiREST, ct: str, nt: str) -> None:
    """Run the Prophet forecast and place ONE order for the given window."""
    loop = asyncio.get_event_loop()

    # 1) Resolve the active market + strike (retry — a just-opened market can be
    #    missing floor_strike for a few seconds).
    market = None
    for _ in range(STRIKE_RETRIES):
        market = await resolve_active_market(rest)
        if market and market.get("target") is not None:
            break
        await asyncio.sleep(2)
    if market is None or market.get("target") is None:
        log.warning("No open market / strike for %s — SKIP window (no order).", ct)
        handled_windows.add(ct)
        return

    ct = market["ticker"]                       # authoritative ticker from Kalshi
    strike = float(market["target"])
    if tracker.already_traded(ct):
        log.info("Window %s already traded — skip (one order per window).", ct)
        handled_windows.add(ct)
        return

    # 2) Download + validate 500 minutes of 1-minute BTC history.
    df = await loop.run_in_executor(None, fetch_btc_1m)
    ok, reason = validate_data(df)
    if not ok:
        log.warning("BTC data check failed for %s: %s — SKIP window (no order).",
                    ct, reason)
        handled_windows.add(ct)
        return
    btc_close = float(df["close"].iloc[-1])
    data_start = df["ds"].iloc[0]
    data_end = df["ds"].iloc[-1]

    # 3) Prophet forecast (CPU-blocking → executor).
    forecast = await loop.run_in_executor(None, run_prophet_forecast, df)
    if forecast is None:
        log.warning("No valid forecast for %s — SKIP window (no order).", ct)
        handled_windows.add(ct)
        return
    p50 = forecast["p50"]
    quantile = percentile_of_price(btc_close, forecast)

    # 4) Decision.
    if btc_close < p50:
        side, decision = "yes", "BUY YES"
    elif btc_close > p50:
        side, decision = "no", "BUY NO"
    else:
        log.info("BTC close == P50 for %s — no directional edge, SKIP.", ct)
        handled_windows.add(ct)
        return

    # Comparisons.
    btc_vs_strike = "ABOVE" if btc_close > strike else "BELOW"
    btc_vs_p50    = "ABOVE" if btc_close > p50 else "BELOW"
    strike_direction = "P50 ABOVE strike" if strike < p50 else "P50 BELOW strike"

    # Entry cost per contract (for P&L accounting).
    yes_p = get_active_yes_price(market)
    if side == "yes":
        entry_price = yes_p if yes_p is not None else 0.5
    else:
        entry_price = (1.0 - yes_p) if yes_p is not None else 0.5

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
        f"Historical Data   : {len(df)} candles loaded\n"
        f"Data Range        : {data_start} → {data_end}\n"
        f"Latest Candle     : {data_end}\n"
        f"BTC Current Close : ${btc_close:,.2f}\n"
        f"Kalshi Strike     : ${strike:,.2f}\n"
        f"Prophet Forecast (minute {FORECAST_MINUTES} P50): ${p50:,.2f}\n"
        f"Forecast Bands    :\n"
        f"    P01: ${forecast['p01']:,.2f}\n"
        f"    P10: ${forecast['p10']:,.2f}\n"
        f"    P25: ${forecast['p25']:,.2f}\n"
        f"    P50: ${forecast['p50']:,.2f}\n"
        f"    P75: ${forecast['p75']:,.2f}\n"
        f"    P90: ${forecast['p90']:,.2f}\n"
        f"    P99: ${forecast['p99']:,.2f}\n"
        f"Current BTC Quantile : {quantile:.0f} percentile\n"
        f"BTC vs Strike     : {btc_vs_strike}\n"
        f"BTC vs P50        : {btc_vs_p50}\n"
        f"Strike vs P50     : {strike_direction}\n"
        f"Decision          : {decision}\n"
        f"Bet Size          : ${BET_AMOUNT_USD:.0f} ({bet_count()} contract(s))"
    )

    # 6) Submit exactly ONE order for this window.
    enum  = BookSide.BID if side == "yes" else BookSide.ASK
    price = YES_BUY_PRICE if side == "yes" else NO_BUY_PRICE
    count = bet_count()
    _, filled = await _submit(rest, ticker=ct, side=enum, price=price, count=count,
                              reduce_only=False, tag=f"BUY {side.upper()}")

    log.info("Order Submitted   : %s", "success" if filled else "failure")
    log.info("==============================")

    # 7) Record (only when a position actually opened).
    handled_windows.add(ct)
    if not filled:
        log.warning("BUY %s not filled for %s — no position, not recorded.",
                    side.upper(), ct)
        return

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
        "forecast_bands": {k: round(forecast[k], 2) for k, _ in _QMAP},
        "count": count,
        "order_submitted": "success" if filled else "failure",
        "dry_run": DRY_RUN,
        "result": "pending",
        "profit_loss": 0.0,
    }
    tracker.record_open(rec)


# ─────────────────────────────────────────────────────────────────────────────
# Strategy loop  (once per 15-minute window; Prophet forecast decides YES/NO)
# ─────────────────────────────────────────────────────────────────────────────
async def strategy_loop(rest: KalshiREST, market_ws: KalshiMarketWS,
                        started_at: float) -> None:
    log.info("Prophet strategy loop started — one forecast/order per 15-min window.")
    while True:
        if (time.time() - started_at) / 60.0 >= RUNTIME_LIMIT_MIN:
            log.info("Runtime limit (%.0f min) reached — clean exit. "
                     "Open positions settle automatically.", RUNTIME_LIMIT_MIN)
            return

        ct, nt = current_and_next_tickers()
        market_ws.set_tickers((ct, nt))

        # Trade this window exactly once (skip if handled this run or already
        # traded in a prior run / restart).
        if ct not in handled_windows and not tracker.already_traded(ct):
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
    log.info("  Bet amount      : $%.2f  (%d contract(s)/buy)", BET_AMOUNT_USD, bet_count())
    log.info("  Data / horizon  : %d 1-min candles → forecast %d min",
             HISTORY_MINUTES, FORECAST_MINUTES)
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
    market_ws.set_tickers(current_and_next_tickers())
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
