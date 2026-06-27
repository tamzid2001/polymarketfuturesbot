"""
kalshibtc15minupordown.py
─────────────────────────────────────────────────────────────────────────────
BTC 15-min Kalshi market algo-trader  (fully ASYNC).

DATA SOURCES
────────────
  • Alpaca CryptoDataStream      → real-time BTC/USD trades (WS, own thread)
  • Kalshi market WebSocket      → real-time ticker / trade for the active
                                   KXBTC15M contract (aiohttp, RSA-signed)
  • Kalshi REST (kalshi-python-async, V2) → market metadata, balance,
                                   positions, MARKET orders

ASYNC ARCHITECTURE
──────────────────
  asyncio main loop runs:
    • kalshi_ws task        — live contract quotes
    • strategy loop task    — decisions + order placement
  Alpaca's CryptoDataStream runs in its own thread (it owns an event loop)
  and writes BTC price into shared state guarded by a threading.Lock.

TICKER FORMAT  (verified from live Kalshi pages)
─────────────────────────────────────────────────
  Pattern : {SERIES}-{YY}{MON}{DD}{HHMM}-{MM}
  Example : KXBTC15M-26JUN271145-45   (settles 11:45 US EASTERN time)
  IMPORTANT: HHMM/DD are US EASTERN time (auto-DST), NOT UTC.
  Suffix = zero-padded minute of settlement:
    :00 → "-00" RELATIVE up/down (ref = previous window close)
    :15/:30/:45 → ABSOLUTE price (ref = floor_strike)

STRATEGY
────────
1. Alpaca BTC/USD trades → rolling 1-min bars (pace decisions to 1/min).
2. Resolve the active KXBTC15M market (async REST) + reference price.
3. Predict & log the NEXT 15-min up/down ticker every cycle.
4. delta = live BTC − reference. Gate: |delta| > PRICE_DELTA_GATE and no
   open position in this window.  delta>0 → BUY YES, delta<0 → BUY NO.
5. Orders are MARKET (marketable IOC) — buy to open, sell/close to exit.
   Positions are closed before settlement (CLOSE_BEFORE_SETTLE_S) or on an
   opposite signal.
6. DRY_RUN (default ON): orders are logged but NOT submitted.

KALSHI ASYNC SDK NOTES (kalshi-python-async ≥ 3.22, needs Python ≥ 3.13)
────────────────────────────────────────────────────────────────────────
  • Auth     : config.api_key_id + config.private_key_pem → KalshiClient(config)
               (RSA-PSS signing; do NOT use the broken client.set_kalshi_auth)
  • Balance  : await PortfolioApi(client).get_balance()
  • Market   : await MarketApi(client).get_market(ticker)
  • Events   : await EventsApi(client).get_events(series_ticker=, status=, ...)
  • Positions: await PortfolioApi(client).get_positions(ticker=)
  • Order V2 : await OrdersApi(client).create_order_v2(create_order_v2_request=
                 CreateOrderV2Request(ticker, side=BookSide.BID|ASK,
                   count="5.00", price="0.99", time_in_force="immediate_or_cancel",
                   self_trade_prevention_type=..., reduce_only=<bool>))
    Single-book: side=BID → buy YES, side=ASK → buy NO (sell YES).
    A marketable IOC at the price cap (0.99 / 0.01) behaves as a market order.
    Closing uses reduce_only=True on the opposite side.
  • Prod REST: https://api.elections.kalshi.com/trade-api/v2
  • Prod WS  : wss://api.elections.kalshi.com/trade-api/ws/v2

CREDENTIALS (env vars)
──────────────────────
    ALPACA_API_KEY / ALPACA_API_SECRET    Alpaca key id / secret
    KALSHI_API_KEY_ID                     Kalshi API key id (UUID)
    KALSHI_PEM_PATH                       RSA private-key .pem path  (or…)
    KALSHI_PRIVATE_KEY                    …the PEM content directly
    KALSHI_DEMO            "true" for sandbox (default false)
    DRY_RUN               "true" (default) — log orders, do not submit
    RUNTIME_LIMIT_MIN     clean-exit after N minutes (default 345 = 5h45m)
    CLOSE_BEFORE_SETTLE_S close open positions this many secs before settle (30)
    KALSHI_WS_VERBOSE     "true" — log every WS message (QA uses this)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import threading
import time
import uuid
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

# Kalshi KXBTC15M tickers are denominated in US EASTERN time (ET, auto-DST),
# NOT UTC. e.g. KXBTC15M-26JUN271145-45 settles 11:45 ET.
ET = ZoneInfo("America/New_York")

import aiohttp

from alpaca.data.live import CryptoDataStream

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

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
ALPACA_API_KEY    = os.getenv("ALPACA_API_KEY",    "")
ALPACA_API_SECRET = os.getenv("ALPACA_API_SECRET", "")
KALSHI_API_KEY_ID = os.getenv("KALSHI_API_KEY_ID", "")
KALSHI_PEM_PATH   = os.getenv("KALSHI_PEM_PATH",   "kalshi_private_key.pem")
KALSHI_DEMO       = os.getenv("KALSHI_DEMO", "false").lower() in ("1", "true", "yes")
DRY_RUN           = os.getenv("DRY_RUN", "true").lower() in ("1", "true", "yes")
RUNTIME_LIMIT_MIN = float(os.getenv("RUNTIME_LIMIT_MIN", "345"))
CLOSE_BEFORE_SETTLE_S = float(os.getenv("CLOSE_BEFORE_SETTLE_S", "30"))
KALSHI_WS_VERBOSE = os.getenv("KALSHI_WS_VERBOSE", "false").lower() in ("1", "true", "yes")

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

BTC_SYMBOL       = "BTC/USD"
HISTORY_BARS     = 60     # 60 × 1-min bars = 1 hour (paces the decision cycle)
PRICE_DELTA_GATE = 10.0   # |real_price − reference| must exceed $10 to trade
ORDER_CONTRACTS  = 5      # contracts per signal
ORDER_TIF        = "immediate_or_cancel"   # marketable IOC == market order
SERIES_TICKER    = "KXBTC15M"

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
for _noisy in ("aiohttp", "asyncio"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)
log = logging.getLogger("kalshi_btc_bot")


# ─────────────────────────────────────────────────────────────────────────────
# Shared state
# ─────────────────────────────────────────────────────────────────────────────
minute_bars: deque                       = deque(maxlen=HISTORY_BARS)
_current_bar: Optional[dict]             = None
_current_bar_minute: Optional[datetime]  = None
_bar_lock = threading.Lock()

latest_btc_price: float                  = 0.0
latest_btc_ts:    Optional[datetime]     = None
_price_lock = threading.Lock()

# Live Kalshi WS quotes per ticker (main-loop only): {ticker: {yes_bid,yes_ask,last,ts}}
kalshi_quotes: dict = {}

# Open positions (main-loop only): {ticker: {"side": "yes"|"no", "count": int}}
positions: dict = {}

prev_window_close: Optional[float]  = None
prev_window_ticker: Optional[str]   = None


# ─────────────────────────────────────────────────────────────────────────────
# Ticker helpers (deterministic)
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
    """Build the ticker from a settlement datetime expressed in ET wall-clock."""
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
    # HHMM/DD are US Eastern time → build an ET-aware settlement datetime.
    settle = datetime(2000 + int(m.group("yy")), mon_num, int(m.group("dd")),
                      int(hhmm[:2]), int(hhmm[2:]), tzinfo=ET)
    suffix = m.group("suffix")
    return {"series": m.group("series"), "settle_et": settle, "suffix": suffix,
            "market_type": "relative" if suffix == "00" else "absolute"}


def current_and_next_tickers(series: str = SERIES_TICKER) -> tuple:
    """Current & next 15-min KXBTC15M tickers, computed in US EASTERN time."""
    now_et     = datetime.now(tz=ET)
    slot_min   = (now_et.minute // 15) * 15
    current_dt = now_et.replace(minute=slot_min, second=0, microsecond=0)
    current_settle = current_dt + timedelta(minutes=15)
    next_settle    = current_settle + timedelta(minutes=15)
    return build_ticker(series, current_settle), build_ticker(series, next_settle)


def log_next_ticker_prediction() -> str:
    """Predict, print and return the NEXT 15-min up/down market ticker."""
    _, nxt = current_and_next_tickers()
    p = parse_ticker(nxt)
    log.info("⏭  NEXT 15-MIN UP/DOWN TICKER PREDICTION: %s  (settles %s ET, type=%s)",
             nxt,
             p["settle_et"].strftime("%H:%M") if p else "?",
             p["market_type"] if p else "?")
    return nxt


# ─────────────────────────────────────────────────────────────────────────────
# Alpaca WebSocket (own thread)
# ─────────────────────────────────────────────────────────────────────────────
def _minute_bucket(ts: datetime) -> datetime:
    return ts.replace(second=0, microsecond=0, tzinfo=timezone.utc)


async def on_trade(trade) -> None:
    global _current_bar, _current_bar_minute, latest_btc_price, latest_btc_ts
    price = float(trade.price)
    size  = float(trade.size)
    ts: datetime = trade.timestamp
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)

    with _price_lock:
        latest_btc_price = price
        latest_btc_ts    = ts

    bucket = _minute_bucket(ts)
    with _bar_lock:
        if _current_bar_minute is None or bucket != _current_bar_minute:
            if _current_bar is not None:
                minute_bars.append(_current_bar.copy())
            _current_bar_minute = bucket
            _current_bar = {"ds": bucket, "open": price, "high": price,
                            "low": price, "close": price, "volume": size}
        else:
            _current_bar["high"]    = max(_current_bar["high"], price)
            _current_bar["low"]     = min(_current_bar["low"],  price)
            _current_bar["close"]   = price
            _current_bar["volume"] += size


def run_alpaca_stream() -> None:
    while True:
        try:
            log.info("Alpaca WS: connecting, subscribing to %s …", BTC_SYMBOL)
            stream = CryptoDataStream(ALPACA_API_KEY, ALPACA_API_SECRET)
            stream.subscribe_trades(on_trade, BTC_SYMBOL)
            stream.run()
        except Exception as exc:  # noqa: BLE001
            log.error("Alpaca WS error: %s — reconnecting in 5s", exc)
            time.sleep(5)


def read_btc_price() -> tuple:
    with _price_lock:
        return latest_btc_price, latest_btc_ts


# ─────────────────────────────────────────────────────────────────────────────
# Kalshi market WebSocket (aiohttp, async, RSA-signed)
# ─────────────────────────────────────────────────────────────────────────────
def _to_dollars(val) -> Optional[float]:
    """Best-effort convert a Kalshi price field to USD dollars."""
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
        # Official auth pattern: set credentials on the Configuration BEFORE
        # constructing the client. KalshiClient.__init__ builds its internal
        # KalshiAuth from these. (The client.set_kalshi_auth() helper is broken
        # in the 3.22 build — NameError on KalshiAuth — so we avoid it.)
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

    async def get_position_count(self, ticker: str) -> Optional[float]:
        """Return signed position size (positive=long YES, negative=long NO)."""
        try:
            resp = await self.portfolio.get_positions(ticker=ticker)
            for mp in (getattr(resp, "market_positions", None) or []):
                if getattr(mp, "ticker", None) == ticker:
                    pf = getattr(mp, "position_fp", None)
                    return float(pf) if pf is not None else None
        except Exception as exc:  # noqa: BLE001
            log.warning("get_positions(%s) failed: %s", ticker, exc)
        return None


def _field(obj, *names):
    for n in names:
        v = getattr(obj, n, None)
        if v is not None:
            return v
    return None


async def resolve_active_market(rest: KalshiREST) -> Optional[dict]:
    global prev_window_close, prev_window_ticker
    current_ticker, next_ticker = current_and_next_tickers()
    parsed = parse_ticker(current_ticker)
    if parsed is None:
        log.error("Cannot parse constructed ticker %s", current_ticker)
        return None

    market_type = parsed["market_type"]
    settle_et   = parsed["settle_et"]
    suffix      = parsed["suffix"]

    market = await rest.get_market(current_ticker)
    if market is None:
        log.warning("Direct lookup of %s failed – trying events query", current_ticker)
        markets = await rest.get_open_series_markets()
        if markets:
            market = markets[0]
            current_ticker = _field(market, "ticker") or current_ticker
            parsed = parse_ticker(current_ticker) or parsed
            market_type = parsed["market_type"]
            settle_et   = parsed["settle_et"]
            suffix      = parsed["suffix"]
    if market is None:
        log.info("No open KXBTC15M market found")
        return None

    reference_price: Optional[float] = None
    if market_type == "absolute":
        strike = _field(market, "floor_strike", "cap_strike", "functional_strike")
        if strike is not None:
            try:
                reference_price = float(strike)
            except (TypeError, ValueError):
                reference_price = None
        if reference_price is None or reference_price <= 0:
            for tf in ("yes_sub_title", "no_sub_title"):
                m = re.search(r"\$([0-9,]+(?:\.\d+)?)", str(_field(market, tf) or ""))
                if m:
                    reference_price = float(m.group(1).replace(",", ""))
                    break
        if reference_price is None:
            log.error("Cannot find strike for absolute market %s", current_ticker)
            return None
    else:
        if prev_window_close is not None:
            reference_price = prev_window_close
        else:
            prev_ticker = build_ticker(SERIES_TICKER, settle_et - timedelta(minutes=15))
            prev = await rest.get_market(prev_ticker)
            if prev is not None:
                strike = _field(prev, "floor_strike", "functional_strike")
                if strike is not None:
                    try:
                        reference_price = float(strike)
                    except (TypeError, ValueError):
                        reference_price = None
            if reference_price is None:
                reference_price, _ = read_btc_price()
                log.warning("Relative ref unavailable – using live Alpaca $%.2f",
                            reference_price)

    return {"ticker": current_ticker, "next_ticker": next_ticker,
            "market_type": market_type, "suffix": suffix,
            "reference_price": reference_price, "settle_et": settle_et,
            "raw_market": market}


# ─────────────────────────────────────────────────────────────────────────────
# Orders (MARKET = marketable IOC)
# ─────────────────────────────────────────────────────────────────────────────
async def _submit(rest: KalshiREST, *, ticker, side: BookSide, price: str,
                  count: int, reduce_only: bool, tag: str):
    order_id = str(uuid.uuid4())
    log.info("ORDER %s  %s  side=%s price=%s count=%d reduce_only=%s ticker=%s id=%s",
             "[DRY-RUN]" if DRY_RUN else "[LIVE]", tag, side.value, price, count,
             reduce_only, ticker, order_id)
    if DRY_RUN:
        log.info("DRY_RUN active — order NOT submitted.")
        return None
    try:
        req = CreateOrderV2Request(
            ticker=ticker, side=side, count=f"{float(count):.2f}", price=price,
            time_in_force=ORDER_TIF, client_order_id=order_id,
            self_trade_prevention_type=SelfTradePreventionType.TAKER_AT_CROSS,
            reduce_only=reduce_only)
        resp = await rest.orders.create_order_v2(create_order_v2_request=req)
        log.info("ORDER RESULT: order_id=%s fill_count=%s remaining=%s avg_price=%s",
                 getattr(resp, "order_id", "?"), getattr(resp, "fill_count", "?"),
                 getattr(resp, "remaining_count", "?"),
                 getattr(resp, "average_fill_price", "?"))
        return resp
    except Exception as exc:  # noqa: BLE001
        log.error("create_order_v2 failed: %s", exc)
        return None


async def market_buy(rest: KalshiREST, ticker: str, buy_side: str,
                     count: int = ORDER_CONTRACTS):
    """Open a position with a marketable IOC (market) order.
    buy YES → BID@0.99 ; buy NO → ASK@0.01."""
    side  = BookSide.BID if buy_side == "yes" else BookSide.ASK
    price = "0.99" if buy_side == "yes" else "0.01"
    resp = await _submit(rest, ticker=ticker, side=side, price=price, count=count,
                         reduce_only=False, tag=f"MARKET BUY {buy_side.upper()}")
    if DRY_RUN or resp is not None:
        positions[ticker] = {"side": buy_side, "count": count}
    return resp


async def market_close(rest: KalshiREST, ticker: str):
    """Close an open position with a reduce-only marketable IOC (market) order.
    long YES → SELL YES (ASK@0.01) ; long NO → BUY YES (BID@0.99)."""
    pos = positions.get(ticker)
    if not pos:
        return None
    if pos["side"] == "yes":
        side, price = BookSide.ASK, "0.01"      # sell YES to close long YES
    else:
        side, price = BookSide.BID, "0.99"      # buy YES to close long NO
    resp = await _submit(rest, ticker=ticker, side=side, price=price,
                         count=pos["count"], reduce_only=True,
                         tag=f"MARKET CLOSE {pos['side'].upper()}")
    if DRY_RUN or resp is not None:
        positions.pop(ticker, None)
    return resp


# ─────────────────────────────────────────────────────────────────────────────
# Snapshot logging
# ─────────────────────────────────────────────────────────────────────────────
def log_snapshot(btc_price, btc_ts, market, delta) -> None:
    now_utc = datetime.now(tz=timezone.utc)
    ts_str  = (btc_ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z") if btc_ts else "no tick"
    settle  = market["settle_et"]
    secs    = (settle - now_utc).total_seconds()
    tleft   = f"{secs/60:.1f} min" if secs > 0 else "EXPIRED"
    ref_lbl = "floor_strike" if market["market_type"] == "absolute" else "prev close"
    q       = get_kalshi_quote(market["ticker"])
    ws_line = (f"yes_bid={q.get('yes_bid','?')} yes_ask={q.get('yes_ask','?')} "
               f"last={q.get('last','?')}") if q else "no WS quote yet"
    held    = positions.get(market["ticker"])
    pos_line = f"{held['side'].upper()} x{held['count']}" if held else "flat"
    log.info(
        "\n"
        "┌─── Snapshot ───────────────────────────────────────────────\n"
        "│  Cycle UTC   : %s\n"
        "│  Alpaca BTC  : $%,.2f  (tick %s)\n"
        "│  Kalshi ref  : $%,.2f  (%s)\n"
        "│  Kalshi WS   : %s\n"
        "│  Market      : %s  type=%s\n"
        "│  Next pred.  : %s\n"
        "│  Settle      : %s ET  (%s)\n"
        "│  Position    : %s\n"
        "│  Delta       : %s$%,.2f  [gate=$%.0f %s]\n"
        "└────────────────────────────────────────────────────────────",
        now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"), btc_price, ts_str,
        market["reference_price"], ref_lbl, ws_line,
        market["ticker"], market["market_type"], market["next_ticker"],
        settle.strftime("%H:%M"), tleft, pos_line,
        ("▲" if delta > 0 else "▼" if delta < 0 else "="),
        abs(delta), PRICE_DELTA_GATE, ("✓" if abs(delta) >= PRICE_DELTA_GATE else "✗"),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Strategy loop
# ─────────────────────────────────────────────────────────────────────────────
async def strategy_loop(rest: KalshiREST, market_ws: KalshiMarketWS,
                        started_at: float) -> None:
    global prev_window_close, prev_window_ticker
    log.info("Strategy loop started – waiting for bars …")
    last_bar_count = 0
    last_window_ticker: Optional[str] = None

    while True:
        await asyncio.sleep(5)
        if (time.time() - started_at) / 60.0 >= RUNTIME_LIMIT_MIN:
            log.info("Runtime limit (%.0f min) reached — clean exit.", RUNTIME_LIMIT_MIN)
            return

        ct, nt = current_and_next_tickers()
        market_ws.set_tickers((ct, nt))

        with _bar_lock:
            n = len(minute_bars)
        if n == last_bar_count or n < 5:
            continue
        last_bar_count = n
        log.info("── New bar (buffer %d/%d) ─────────────────────", n, HISTORY_BARS)
        log_next_ticker_prediction()

        btc_price, btc_ts = read_btc_price()
        if btc_price == 0.0:
            log.warning("No Alpaca tick yet – skipping")
            continue

        market = await resolve_active_market(rest)
        if market is None:
            continue
        market_ws.set_tickers((market["ticker"], market["next_ticker"]))

        # window rollover → close any stale position from the prior window
        if last_window_ticker is not None and market["ticker"] != last_window_ticker:
            prev_window_close  = btc_price
            prev_window_ticker = last_window_ticker
            log.info("Window rolled %s → %s  cached close=$%.2f",
                     last_window_ticker, market["ticker"], prev_window_close)
            if last_window_ticker in positions:
                log.info("Closing stale position in %s before settlement", last_window_ticker)
                await market_close(rest, last_window_ticker)
        last_window_ticker = market["ticker"]

        ticker = market["ticker"]
        ref    = market["reference_price"]
        delta  = btc_price - ref
        log_snapshot(btc_price, btc_ts, market, delta)

        secs_left = (market["settle_et"] - datetime.now(tz=timezone.utc)).total_seconds()

        # close current position near settlement
        if ticker in positions and 0 < secs_left < CLOSE_BEFORE_SETTLE_S:
            log.info("Near settlement (%.0fs) — closing %s", secs_left, ticker)
            await market_close(rest, ticker)
            continue

        if abs(delta) < PRICE_DELTA_GATE:
            log.info("GATE MISS: |delta|=$%.2f < $%.0f", abs(delta), PRICE_DELTA_GATE)
            continue

        want_side = "yes" if delta > 0 else "no"
        held = positions.get(ticker)
        if held:
            if held["side"] != want_side:
                log.info("Opposite signal — closing %s %s then re-evaluating",
                         ticker, held["side"].upper())
                await market_close(rest, ticker)
            else:
                log.info("GATE MISS: already holding %s in %s", want_side.upper(), ticker)
            continue

        direction = "UP (BUY YES)" if delta > 0 else "DOWN (BUY NO)"
        log.info("✦ SIGNAL %s  delta=$%.2f ref=$%.2f btc=$%.2f",
                 direction, delta, ref, btc_price)
        await market_buy(rest, ticker, want_side, ORDER_CONTRACTS)


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────
async def main_async() -> None:
    now_utc = datetime.now(tz=timezone.utc)
    ct, nt  = current_and_next_tickers()
    log.info("=" * 68)
    log.info("  BTC Kalshi 15-min algo-trader (ASYNC)")
    log.info("  Now (UTC)       : %s", now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"))
    log.info("  Current window  : %s", ct)
    log.info("  Kalshi REST     : %s", KALSHI_BASE_URL)
    log.info("  Kalshi WS       : %s", KALSHI_WS_URL)
    log.info("  Demo / DRY_RUN  : %s / %s", KALSHI_DEMO, DRY_RUN)
    log.info("  Delta gate      : $%.0f   contracts/trade: %d", PRICE_DELTA_GATE, ORDER_CONTRACTS)
    log.info("  Runtime limit   : %.0f min", RUNTIME_LIMIT_MIN)
    log.info("=" * 68)
    log_next_ticker_prediction()

    if not (ALPACA_API_KEY and ALPACA_API_SECRET and KALSHI_API_KEY_ID):
        raise SystemExit("Missing credentials — set ALPACA_API_KEY, "
                         "ALPACA_API_SECRET, KALSHI_API_KEY_ID")

    rest = KalshiREST()
    try:
        bal = await rest.get_balance_dollars()
        log.info("Kalshi auth OK – balance: $%.2f", bal if bal is not None else -1)
    except Exception as exc:
        log.error("Kalshi auth failed: %s", exc)
        await rest.close()
        raise

    threading.Thread(target=run_alpaca_stream, daemon=True, name="alpaca-ws").start()

    market_ws = KalshiMarketWS(rest.auth)
    market_ws.set_tickers(current_and_next_tickers())
    ws_task = asyncio.create_task(market_ws.run(), name="kalshi-ws")
    try:
        await strategy_loop(rest, market_ws, started_at=time.time())
    finally:
        ws_task.cancel()
        try:
            await ws_task
        except asyncio.CancelledError:
            pass
        await rest.close()


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
