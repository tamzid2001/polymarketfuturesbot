"""
kalshibtc15minupordown.py
─────────────────────────────────────────────────────────────────────────────
BTC 15-min Kalshi market algo-trader  (fully ASYNC).

NOW PRICE (rolling 60-second simple average)
────────────────────────────────────────────
  Kalshi BTC 15-min markets resolve on CF Benchmarks' BRTI — the simple
  average of the 60 one-second index values before settlement. Kalshi's trade
  API exposes the TARGET (floor_strike, == the previous window's 60-s average)
  but not a live BTC spot price, so we derive a "NOW price" from Alpaca:

    • Sample Alpaca's live BTC/USD price once PER SECOND.
    • NOW price = simple average of the last 60 one-second samples.
    • On each new 15-min window we ANCHOR the average to the Kalshi target
      (floor_strike) and then blend in each new second's price — keeping the
      rolling 60-s average aligned with the official BRTI value.
    • The NOW price is printed every second.

SIGNAL  (momentum vs the rolling average)
─────────────────────────────────────────
    delta = live_price − NOW_price
      delta ≥ +PRICE_DELTA_GATE ($10)  → BUY UP   (YES)
      delta ≤ −PRICE_DELTA_GATE        → BUY DOWN (NO)
    Only ONE position (UP or DOWN) is open at any time; flipping closes the
    other side first. Orders are MARKET (marketable IOC). Bet size is
    BET_AMOUNT_USD per buy (each contract = $1 notional).

DATA SOURCES
────────────
  • Alpaca CryptoDataStream  → real-time BTC/USD trades (WS, own thread)
  • Kalshi market WebSocket  → live ticker/trade for the active contract
  • Kalshi REST (kalshi-python-async, V2) → market metadata (target), balance,
                               MARKET orders

TICKER FORMAT  (US EASTERN time, auto-DST — NOT UTC)
─────────────────────────────────────────────────────
  Pattern : {SERIES}-{YY}{MON}{DD}{HHMM}-{MM}
  Example : KXBTC15M-26JUN271145-45   (settles 11:45 ET)
  Suffix = zero-padded minute of settlement (00/15/30/45).

KALSHI ASYNC SDK NOTES (kalshi-python-async ≥ 3.22, Python ≥ 3.13)
──────────────────────────────────────────────────────────────────
  • Auth   : config.api_key_id + config.private_key_pem → KalshiClient(config)
             (do NOT use the broken client.set_kalshi_auth)
  • Balance: await PortfolioApi(client).get_balance()
  • Market : await MarketApi(client).get_market(ticker)   (.market.floor_strike)
  • Order  : await OrdersApi(client).create_order_v2(create_order_v2_request=
               CreateOrderV2Request(ticker, side=BookSide.BID|ASK, count="1.00",
                 price="0.99", time_in_force="immediate_or_cancel",
                 self_trade_prevention_type=..., reduce_only=<bool>))
             BID → buy YES; ASK → buy NO (sell YES). reduce_only closes.

CREDENTIALS / SETTINGS (env vars)
─────────────────────────────────
    ALPACA_API_KEY / ALPACA_API_SECRET
    KALSHI_API_KEY_ID / KALSHI_PEM_PATH (or KALSHI_PRIVATE_KEY content)
    KALSHI_DEMO            "true" for sandbox (default false)
    DRY_RUN               "true" (default) — log orders, do not submit
    BET_AMOUNT_USD        notional $ per buy (default 1 → 1 contract)
    PRICE_DELTA_GATE      $ momentum gate (default 10)
    NOW_WINDOW_S          rolling-average window seconds (default 60)
    PRINT_NOW_PRICE       "true" (default) — print NOW price every second
    RUNTIME_LIMIT_MIN     clean-exit after N minutes (default 345 = 5h45m)
    CLOSE_BEFORE_SETTLE_S close position this many secs before settle (30)
    KALSHI_WS_VERBOSE     "true" — log every Kalshi WS message
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

import aiohttp

from alpaca.data.live import CryptoDataStream
from alpaca.data.historical import CryptoHistoricalDataClient
from alpaca.data.requests import CryptoLatestTradeRequest

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
ALPACA_API_KEY    = os.getenv("ALPACA_API_KEY",    "")
ALPACA_API_SECRET = os.getenv("ALPACA_API_SECRET", "")
KALSHI_API_KEY_ID = os.getenv("KALSHI_API_KEY_ID", "")
KALSHI_PEM_PATH   = os.getenv("KALSHI_PEM_PATH",   "kalshi_private_key.pem")
KALSHI_DEMO       = os.getenv("KALSHI_DEMO", "false").lower() in ("1", "true", "yes")
DRY_RUN           = os.getenv("DRY_RUN", "true").lower() in ("1", "true", "yes")

# ── Bet sizing ──────────────────────────────────────────────────────────────
# Initial bet amount in USD notional per buy. Each Kalshi contract settles to
# $1, so $1 of notional == 1 contract.  Change here (or via env) to scale up.
BET_AMOUNT_USD    = float(os.getenv("BET_AMOUNT_USD", "1"))

PRICE_DELTA_GATE  = float(os.getenv("PRICE_DELTA_GATE", "10"))
# Near-the-money guard: only trade when the contract's YES price is in this band,
# so both sides are liquid (skips deep-OTM "lottery ticket" contracts that can't
# be exited because Kalshi's min price is $0.01).
NTM_MIN           = float(os.getenv("NEAR_THE_MONEY_MIN", "0.05"))
NTM_MAX           = float(os.getenv("NEAR_THE_MONEY_MAX", "0.95"))
NOW_WINDOW_S      = int(float(os.getenv("NOW_WINDOW_S", "60")))
PRINT_NOW_PRICE   = os.getenv("PRINT_NOW_PRICE", "true").lower() in ("1", "true", "yes")
PRINT_SPOT        = os.getenv("PRINT_SPOT", "true").lower() in ("1", "true", "yes")
RUNTIME_LIMIT_MIN = float(os.getenv("RUNTIME_LIMIT_MIN", "345"))
CLOSE_BEFORE_SETTLE_S = float(os.getenv("CLOSE_BEFORE_SETTLE_S", "30"))
REPORT_INTERVAL_S = float(os.getenv("REPORT_INTERVAL_S", "30"))   # portfolio report cadence
KALSHI_WS_VERBOSE = os.getenv("KALSHI_WS_VERBOSE", "false").lower() in ("1", "true", "yes")

ORDER_TIF      = "immediate_or_cancel"   # marketable IOC == market order
SERIES_TICKER  = "KXBTC15M"
BTC_SYMBOL     = "BTC/USD"
MIN_SAMPLES    = 3                       # need a few samples before trading

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
for _noisy in ("aiohttp", "asyncio"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)
log = logging.getLogger("kalshi_btc_bot")


# ─────────────────────────────────────────────────────────────────────────────
# Shared state
# ─────────────────────────────────────────────────────────────────────────────
latest_btc_price:  float              = 0.0
latest_btc_ts:     Optional[datetime] = None
last_price_source: str                = "?"     # "WS" | "REST"
_price_lock = threading.Lock()

# Rolling NOW-price (per-second samples → simple average). Main-loop only.
price_samples: deque = deque(maxlen=NOW_WINDOW_S)
now_price: float     = 0.0

# Live Kalshi WS quotes per ticker (main-loop only).
kalshi_quotes: dict = {}

# Single open position (main-loop only): {"ticker","side","count"} or None.
open_position: Optional[dict] = None

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
# Alpaca price feed:  WebSocket (own thread, sub-second) + per-second REST poll.
# Both write the latest spot to shared state; the freshest write wins, so the
# bot always has a real-time price even if the WebSocket goes quiet.
# ─────────────────────────────────────────────────────────────────────────────
def _set_price(price: float, source: str, ts: Optional[datetime] = None) -> None:
    global latest_btc_price, latest_btc_ts, last_price_source
    with _price_lock:
        latest_btc_price  = price
        latest_btc_ts     = ts or datetime.now(tz=timezone.utc)
        last_price_source = source


def read_btc_price() -> tuple:
    with _price_lock:
        return latest_btc_price, latest_btc_ts


def read_btc_full() -> tuple:
    with _price_lock:
        return latest_btc_price, latest_btc_ts, last_price_source


# ── WebSocket (real-time trades) ─────────────────────────────────────────────
async def on_trade(trade) -> None:
    ts: datetime = trade.timestamp
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    _set_price(float(trade.price), "WS", ts)


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


# ── REST spot (per-second fallback / cadence guarantee) ──────────────────────
_rest_client: Optional[CryptoHistoricalDataClient] = None


def fetch_btc_spot_rest() -> Optional[float]:
    """Blocking Alpaca REST call for the latest BTC/USD trade price."""
    global _rest_client
    try:
        if _rest_client is None:
            _rest_client = CryptoHistoricalDataClient(ALPACA_API_KEY, ALPACA_API_SECRET)
        resp = _rest_client.get_crypto_latest_trade(
            CryptoLatestTradeRequest(symbol_or_symbols=BTC_SYMBOL))
        return float(resp[BTC_SYMBOL].price)
    except Exception as exc:  # noqa: BLE001
        log.debug("Alpaca REST spot fetch failed: %s", exc)
        return None


def anchor_now_price(target: Optional[float]) -> None:
    """Reset the rolling buffer and seed it with the Kalshi target so the
    60-s average re-anchors to the official BRTI value at each new window."""
    global now_price
    price_samples.clear()
    if target is not None and target > 0:
        price_samples.append(target)
        now_price = target
        log.info(f"Anchored NOW price (60s avg) to target ${target:,.2f}")


async def btc_second_loop() -> None:
    """Every second: poll Alpaca REST for the spot price (always, alongside the
    WebSocket), update the rolling 60-second NOW average from the MOST RECENT
    price (WS or REST), and print both. Buy/close logic reads this same state."""
    global now_price
    loop = asyncio.get_event_loop()
    while True:
        t0 = loop.time()
        # 1) REST spot every second (runs in a thread so the loop stays free)
        rprice = await loop.run_in_executor(None, fetch_btc_spot_rest)
        if rprice and rprice > 0:
            _set_price(rprice, "REST")
            if PRINT_SPOT:
                log.info(f"Alpaca REST spot (1s): ${rprice:,.2f}")
        # 2) rolling average from the most-recent price (WS may be fresher)
        price, _, source = read_btc_full()
        if price > 0:
            price_samples.append(price)
            now_price = sum(price_samples) / len(price_samples)
            if PRINT_NOW_PRICE:
                log.info(f"NOW 60s-avg=${now_price:,.2f} | spot=${price:,.2f} ({source}) | "
                         f"Δ(spot−now)={price - now_price:+.2f} | "
                         f"samples={len(price_samples)}/{NOW_WINDOW_S}")
        # 3) keep a ~1-second cadence
        await asyncio.sleep(max(0.0, 1.0 - (loop.time() - t0)))


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
    """Target price = floor_strike (min value for YES); fall back to subtitle."""
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
# Orders (MARKET = marketable IOC) — single open position
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
        req = CreateOrderV2Request(
            ticker=ticker, side=side, count=f"{float(count):.2f}", price=price,
            time_in_force=ORDER_TIF, client_order_id=order_id,
            self_trade_prevention_type=SelfTradePreventionType.TAKER_AT_CROSS,
            reduce_only=reduce_only)
        resp = await rest.orders.create_order_v2(create_order_v2_request=req)
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


async def close_position(rest: KalshiREST) -> bool:
    """Close the single open position (reduce-only marketable order). Returns filled."""
    global open_position
    if not open_position:
        return True
    if open_position["side"] == "yes":
        side, price = BookSide.ASK, "0.01"      # sell YES → close long YES
    else:
        side, price = BookSide.BID, "0.99"      # buy YES  → close long NO
    _, filled = await _submit(rest, ticker=open_position["ticker"], side=side, price=price,
                              count=open_position["count"], reduce_only=True,
                              tag=f"MARKET CLOSE {open_position['side'].upper()}")
    if filled:
        open_position = None
    else:
        log.warning("CLOSE did not fill — position still open, will retry.")
    return filled


async def open_market(rest: KalshiREST, ticker: str, side: str):
    """Open a single position; closes the opposite first (one position rule)."""
    global open_position
    if open_position:
        if open_position["ticker"] == ticker and open_position["side"] == side:
            return                              # already in the desired position
        if not await close_position(rest):      # flip → must close the other side first
            log.warning("Could not close existing position to flip — skipping new entry.")
            return
    enum  = BookSide.BID if side == "yes" else BookSide.ASK
    price = "0.99" if side == "yes" else "0.01"
    count = bet_count()
    _, filled = await _submit(rest, ticker=ticker, side=enum, price=price, count=count,
                              reduce_only=False, tag=f"MARKET BUY {side.upper()}")
    if filled:
        open_position = {"ticker": ticker, "side": side, "count": count}
    else:
        log.warning("BUY %s not filled — no position opened.", side.upper())


# ─────────────────────────────────────────────────────────────────────────────
# Snapshot logging
# ─────────────────────────────────────────────────────────────────────────────
def log_snapshot(live, market, delta) -> None:
    now_utc = datetime.now(tz=timezone.utc)
    settle  = market.get("settle_et")
    tleft   = "?"
    if settle:
        secs = (settle - now_utc).total_seconds()
        tleft = f"{secs/60:.1f} min" if secs > 0 else "EXPIRED"
    q = get_kalshi_quote(market["ticker"])
    ws_line = (f"yes_bid={q.get('yes_bid','?')} yes_ask={q.get('yes_ask','?')} "
               f"last={q.get('last','?')}") if q else "no WS quote yet"
    pos = (f"{open_position['side'].upper()} x{open_position['count']} "
           f"({open_position['ticker']})") if open_position else "flat"
    tgt = market.get("target")
    tgt_s = f"${tgt:,.2f}" if tgt is not None else "n/a"
    gate_ok = "✓" if abs(delta) >= PRICE_DELTA_GATE else "·"
    settle_s = settle.strftime("%H:%M") if settle else "?"
    yp = get_active_yes_price(market)
    if yp is not None:
        ntm = "✓ tradable" if (NTM_MIN <= yp <= NTM_MAX) else "✗ deep-OTM (skip)"
        contract_s = f"${yp:.3f}  [near-money {NTM_MIN:.2f}–{NTM_MAX:.2f}: {ntm}]"
    else:
        contract_s = "n/a"
    log.info(
        "\n"
        "┌─── Snapshot ───────────────────────────────────────────────\n"
        f"│  Live BTC    : ${live:,.2f}\n"
        f"│  NOW 60s-avg : ${now_price:,.2f}   (Δ live−now = {delta:+,.2f}, "
        f"gate ${PRICE_DELTA_GATE:.0f} {gate_ok})\n"
        f"│  Kalshi tgt  : {tgt_s}\n"
        f"│  Contract YES: {contract_s}\n"
        f"│  Kalshi WS   : {ws_line}\n"
        f"│  Market      : {market['ticker']}  (settle {settle_s} ET, {tleft})\n"
        f"│  Next pred.  : {market['next_ticker']}\n"
        f"│  Position    : {pos}\n"
        "└────────────────────────────────────────────────────────────"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Portfolio report  (recent positions, P&L, trades placed)
# ─────────────────────────────────────────────────────────────────────────────
def _f(v, default=0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


async def report_portfolio(rest: KalshiREST) -> None:
    """Fetch & print balance, recent positions, total P&L and trade counts."""
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
    body = "\n".join(lines) if lines else "│   (none yet)"
    log.info(
        "\n"
        "╔═══ PORTFOLIO ══════════════════════════════════════════════\n"
        f"║  Balance        : {bal_s}   (start ${start_balance:,.2f})\n"
        f"║  Net P&L (cash) : {net_s}   ← balance change since bot start\n"
        f"║  Realized P&L   : ${realized_total:,.2f}   Fees: ${fees_total:,.2f}\n"
        f"║  Trades placed  : {trades_placed}  (buys {buys_placed}, closes {closes_placed}, "
        f"fills {fills_count})\n"
        f"║  Bot position   : "
        f"{(open_position['side'].upper()+' x'+str(open_position['count'])+' '+open_position['ticker']) if open_position else 'flat'}\n"
        "║  Recent positions (Kalshi):\n"
        f"{body}\n"
        "╚════════════════════════════════════════════════════════════"
    )


async def portfolio_reporter(rest: KalshiREST) -> None:
    while True:
        await asyncio.sleep(REPORT_INTERVAL_S)
        try:
            await report_portfolio(rest)
        except Exception as exc:  # noqa: BLE001
            log.warning("portfolio report failed: %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# Strategy loop  (decision = live price vs rolling 60-s NOW price)
# ─────────────────────────────────────────────────────────────────────────────
async def strategy_loop(rest: KalshiREST, market_ws: KalshiMarketWS,
                        started_at: float) -> None:
    log.info("Strategy loop started …")
    cached_ticker: Optional[str] = None
    anchored_ticker: Optional[str] = None
    market: Optional[dict] = None

    while True:
        await asyncio.sleep(2)
        if (time.time() - started_at) / 60.0 >= RUNTIME_LIMIT_MIN:
            log.info("Runtime limit (%.0f min) reached — clean exit.", RUNTIME_LIMIT_MIN)
            if open_position:
                await close_position(rest)
            return

        ct, nt = current_and_next_tickers()
        market_ws.set_tickers((ct, nt))

        # New 15-min window → close stale position, resolve the new market
        if ct != cached_ticker:
            if open_position and open_position["ticker"] == cached_ticker:
                log.info("Window rolled → closing stale position in %s", cached_ticker)
                await close_position(rest)
            log_next_ticker_prediction()
            market = await resolve_active_market(rest)
            cached_ticker = ct
        # Keep re-resolving until we have the market AND its target. A just-opened
        # market often has no floor_strike for a few seconds → target=None; refetch.
        elif market is None or market["ticker"] != ct or market.get("target") is None:
            market = await resolve_active_market(rest)

        if market is None:
            continue

        # Anchor the rolling NOW price to the official target once it's available
        # for this window (the target == the previous window's 60-s BRTI average).
        if market.get("target") is not None and anchored_ticker != market["ticker"]:
            anchor_now_price(market["target"])
            anchored_ticker = market["ticker"]

        live, _ = read_btc_price()
        if live <= 0 or len(price_samples) < MIN_SAMPLES:
            continue

        delta = live - now_price
        log_snapshot(live, market, delta)

        # close before settlement
        if open_position and market.get("settle_et"):
            secs_left = (market["settle_et"] - datetime.now(tz=timezone.utc)).total_seconds()
            if 0 < secs_left < CLOSE_BEFORE_SETTLE_S:
                log.info("Near settlement (%.0fs) — closing position", secs_left)
                await close_position(rest)
                continue

        # momentum decision vs the rolling NOW price
        if delta >= PRICE_DELTA_GATE:
            desired = "yes"      # UP move
        elif delta <= -PRICE_DELTA_GATE:
            desired = "no"       # DOWN move
        else:
            continue             # inside the band → no action

        if open_position and open_position["side"] == desired and \
                open_position["ticker"] == ct:
            continue             # already positioned correctly

        # Near-the-money guard: skip deep-OTM contracts (illiquid / can't exit)
        yes_p = get_active_yes_price(market)
        if yes_p is None:
            log.info("GATE: no contract price yet for %s — skip entry", ct)
            continue
        if not (NTM_MIN <= yes_p <= NTM_MAX):
            log.info("GATE: contract YES=$%.3f outside near-the-money [$%.2f, $%.2f] "
                     "— skip lottery-ticket trade", yes_p, NTM_MIN, NTM_MAX)
            continue

        direction = "UP (BUY YES)" if desired == "yes" else "DOWN (BUY NO)"
        log.info("✦ SIGNAL %s  live=$%.2f now=$%.2f Δ=%+.2f  contractYES=$%.3f",
                 direction, live, now_price, delta, yes_p)
        await open_market(rest, ct, desired)


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────
async def main_async() -> None:
    ct, nt = current_and_next_tickers()
    log.info("=" * 68)
    log.info("  BTC Kalshi 15-min algo-trader (ASYNC)")
    log.info("  Now (UTC)       : %s", datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
    log.info("  Current window  : %s", ct)
    log.info("  Kalshi REST/WS  : %s", KALSHI_BASE_URL)
    log.info("  Demo / DRY_RUN  : %s / %s", KALSHI_DEMO, DRY_RUN)
    log.info("  Bet amount      : $%.2f  (%d contract(s)/buy)", BET_AMOUNT_USD, bet_count())
    log.info("  Delta gate      : $%.0f   NOW window: %ds", PRICE_DELTA_GATE, NOW_WINDOW_S)
    log.info("  Runtime limit   : %.0f min", RUNTIME_LIMIT_MIN)
    log.info("=" * 68)
    log_next_ticker_prediction()

    if not (ALPACA_API_KEY and ALPACA_API_SECRET and KALSHI_API_KEY_ID):
        raise SystemExit("Missing credentials — set ALPACA_API_KEY, "
                         "ALPACA_API_SECRET, KALSHI_API_KEY_ID")

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

    await report_portfolio(rest)   # initial portfolio snapshot

    threading.Thread(target=run_alpaca_stream, daemon=True, name="alpaca-ws").start()

    market_ws = KalshiMarketWS(rest.auth)
    market_ws.set_tickers(current_and_next_tickers())
    ws_task  = asyncio.create_task(market_ws.run(), name="kalshi-ws")
    smp_task = asyncio.create_task(btc_second_loop(), name="btc-1s-feed")
    rpt_task = asyncio.create_task(portfolio_reporter(rest), name="portfolio-reporter")
    try:
        await strategy_loop(rest, market_ws, started_at=time.time())
    finally:
        for t in (ws_task, smp_task, rpt_task):
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
