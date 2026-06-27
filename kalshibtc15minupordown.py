"""
kalshibtc15minupordown.py
─────────────────────────────────────────────────────────────────────────────
BTC 15-min Kalshi market algo-trader.

DATA SOURCES
────────────
  • Alpaca CryptoDataStream  → real-time BTC/USD trades (WebSocket) → 1-min bars
  • Kalshi market WebSocket  → real-time ticker / trade for the active KXBTC15M
                               contract (yes_bid / yes_ask / last price)
  • Kalshi REST (v2 SDK)     → market metadata (floor_strike), balance, orders

TICKER FORMAT  (verified from live Kalshi pages)
─────────────────────────────────────────────────
  Pattern : {SERIES}-{YY}{MON}{DD}{HHMM}-{MM}
  Example : KXBTC15M-26JUN270045-45

  Suffix rule (zero-padded minute of the settlement time):
    :00 → "-00"  RELATIVE up/down market (ref = previous window close)
    :15 → "-15"  ABSOLUTE price market   (ref = floor_strike)
    :30 → "-30"  ABSOLUTE price market
    :45 → "-45"  ABSOLUTE price market

STRATEGY
────────
1. Stream BTC/USD trades via Alpaca → rolling 60 × 1-min OHLCV bars (these
   pace the decision cycle to once per completed minute).
2. Resolve the active KXBTC15M market (REST) and its reference price.
3. Compare live Alpaca price vs reference; log a timestamped snapshot that
   also shows the live Kalshi WS quote for the contract.
4. Decision gate:  |delta| > PRICE_DELTA_GATE  AND  no open position in this
   window.  delta>0 → BUY YES, delta<0 → BUY NO.
5. DRY_RUN (default ON): every order is logged but NOT submitted, so the
   strategy can be validated against live data before risking real money.

KALSHI SDK NOTES (kalshi-python-sync ≥ 3.22, V2 fixed-point API)
────────────────────────────────────────────────────────────────
  • Auth        : client.set_kalshi_auth(key_id, pem_content)  (RSA-PSS signing)
  • Balance     : PortfolioApi(client).get_balance()
  • Market      : MarketApi(client).get_market(ticker) / .get_markets(...)
  • Events      : EventsApi(client).get_events(series_ticker=, status="open", ...)
  • Order (V2)  : OrdersApi(client).create_order_v2(create_order_v2_request=
                    CreateOrderV2Request(ticker, side=BookSide.bid|ask,
                                         count="5.00", price="0.99", ...))
    Single-book model:  side=bid → buy YES,  side=ask → buy NO (sell YES).
    count/price are fixed-point DOLLAR strings, not integer cents.
  • Production REST: https://api.elections.kalshi.com/trade-api/v2
  • Production WS  : wss://api.elections.kalshi.com/trade-api/ws/v2

CREDENTIALS (env vars)
──────────────────────
    ALPACA_API_KEY          Alpaca key id
    ALPACA_API_SECRET       Alpaca secret
    KALSHI_API_KEY_ID       Kalshi API key id (UUID)
    KALSHI_PEM_PATH         Path to RSA private-key .pem  (or…)
    KALSHI_PRIVATE_KEY      …the PEM content directly (used if no path file)
    KALSHI_DEMO             "true" for sandbox (default: false)
    DRY_RUN                 "true" (default) = log orders, do not submit
    RUNTIME_LIMIT_MIN       clean-exit after N minutes (default 345 = 5h45m)
"""

from __future__ import annotations

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

from alpaca.data.live import CryptoDataStream

from kalshi_python_sync import (
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

import websocket  # websocket-client

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
HISTORY_BARS     = 60     # 60 × 1-min bars = 1 hour (paces decision cycle)
PRICE_DELTA_GATE = 10.0   # |real_price − reference| must exceed $10 to trade
ORDER_CONTRACTS  = 5      # contracts per signal
SERIES_TICKER    = "KXBTC15M"

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
for _noisy in ("websocket",):
    logging.getLogger(_noisy).setLevel(logging.WARNING)
log = logging.getLogger("kalshi_btc_bot")


# ─────────────────────────────────────────────────────────────────────────────
# Shared state
# ─────────────────────────────────────────────────────────────────────────────
minute_bars: deque = deque(maxlen=HISTORY_BARS)
_current_bar: Optional[dict]            = None
_current_bar_minute: Optional[datetime] = None
_bar_lock = threading.Lock()

latest_btc_price: float            = 0.0
latest_btc_ts:    Optional[datetime] = None
_price_lock = threading.Lock()

# Live Kalshi WS quotes per ticker: {ticker: {"yes_bid":.., "yes_ask":.., "last":.., "ts":..}}
kalshi_quotes: dict = {}
_quote_lock = threading.Lock()

# Previous window's settlement reference (used for relative -00 markets)
prev_window_close: Optional[float] = None
prev_window_ticker: Optional[str]  = None

positions_held: set = set()


# ─────────────────────────────────────────────────────────────────────────────
# Ticker helpers  (fully deterministic — no opaque suffix needed)
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


def build_ticker(series: str, settle_utc: datetime) -> str:
    """Construct the full KXBTC15M ticker for a settlement UTC datetime."""
    yy     = settle_utc.strftime("%y")
    mon    = settle_utc.strftime("%b").upper()
    dd     = settle_utc.strftime("%d")
    hhmm   = settle_utc.strftime("%H%M")
    suffix = settle_utc.strftime("%M")
    return f"{series}-{yy}{mon}{dd}{hhmm}-{suffix}"


def parse_ticker(ticker: str) -> Optional[dict]:
    """Parse a KXBTC15M ticker into components (series, settle_utc, suffix, market_type)."""
    m = _TICKER_RE.match(ticker)
    if not m:
        return None
    mon_num = _MONTHS.get(m.group("mon"))
    if mon_num is None:
        return None
    hhmm   = m.group("hhmm")
    settle = datetime(
        2000 + int(m.group("yy")), mon_num, int(m.group("dd")),
        int(hhmm[:2]), int(hhmm[2:]),
        tzinfo=timezone.utc,
    )
    suffix = m.group("suffix")
    return {
        "series":      m.group("series"),
        "settle_utc":  settle,
        "suffix":      suffix,
        "market_type": "relative" if suffix == "00" else "absolute",
    }


def current_and_next_tickers(series: str = SERIES_TICKER) -> tuple:
    """Return (current_window_ticker, next_window_ticker) based on UTC now."""
    now        = datetime.now(tz=timezone.utc)
    slot_min   = (now.minute // 15) * 15
    current_dt = now.replace(minute=slot_min, second=0, microsecond=0)
    next_dt    = current_dt + timedelta(minutes=15)
    current_settle = current_dt + timedelta(minutes=15)
    next_settle    = next_dt    + timedelta(minutes=15)
    return (
        build_ticker(series, current_settle),
        build_ticker(series, next_settle),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Alpaca WebSocket  (real-time BTC/USD)
# ─────────────────────────────────────────────────────────────────────────────
def _minute_bucket(ts: datetime) -> datetime:
    return ts.replace(second=0, microsecond=0, tzinfo=timezone.utc)


async def on_trade(trade) -> None:
    global _current_bar, _current_bar_minute, latest_btc_price, latest_btc_ts

    price: float = float(trade.price)
    size:  float = float(trade.size)

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
            _current_bar = {
                "ds": bucket, "open": price, "high": price,
                "low": price, "close": price, "volume": size,
            }
        else:
            _current_bar["high"]    = max(_current_bar["high"], price)
            _current_bar["low"]     = min(_current_bar["low"],  price)
            _current_bar["close"]   = price
            _current_bar["volume"] += size


def run_alpaca_stream() -> None:
    """Run the Alpaca crypto stream; auto-restart on disconnect."""
    while True:
        try:
            log.info("Alpaca WS: connecting, subscribing to %s …", BTC_SYMBOL)
            stream = CryptoDataStream(ALPACA_API_KEY, ALPACA_API_SECRET)
            stream.subscribe_trades(on_trade, BTC_SYMBOL)
            stream.run()
        except Exception as exc:  # noqa: BLE001
            log.error("Alpaca WS error: %s — reconnecting in 5s", exc)
            time.sleep(5)


# ─────────────────────────────────────────────────────────────────────────────
# Kalshi market WebSocket  (real-time contract quotes)
# ─────────────────────────────────────────────────────────────────────────────
def _to_dollars(val) -> Optional[float]:
    """Best-effort convert a Kalshi price field to USD dollars.

    Post-fixed-point fields are dollar strings/floats already; legacy integer
    cent fields are 0-100. Heuristic: an int/whole number > 1 and <= 100 is
    treated as cents.
    """
    if val is None:
        return None
    try:
        f = float(val)
    except (TypeError, ValueError):
        return None
    if isinstance(val, str) and "." in val:
        return f
    # whole number 1..100 → cents
    if f.is_integer() and 1 <= f <= 100:
        return f / 100.0
    return f


class KalshiMarketWS:
    """Threaded Kalshi market-data WebSocket subscriber.

    Authenticates the handshake with RSA-PSS signed headers, then keeps a live
    subscription to the ``ticker`` and ``trade`` channels for the current and
    next 15-min contracts. Updates the shared ``kalshi_quotes`` dict.
    """

    def __init__(self, auth: KalshiAuth, url: str = KALSHI_WS_URL):
        self.auth = auth
        self.url = url
        self.ws: Optional[websocket.WebSocketApp] = None
        self._cmd_id = 0
        self._connected = threading.Event()
        self._subscribed: tuple = ()
        self._lock = threading.Lock()

    # ── public API ──────────────────────────────────────────────────────────
    def start(self) -> None:
        threading.Thread(target=self._run_forever, daemon=True,
                         name="kalshi-ws").start()

    def ensure_subscribed(self, tickers: tuple) -> None:
        """(Re)subscribe to the given tickers if the set changed."""
        tickers = tuple(t for t in tickers if t)
        if not tickers or tickers == self._subscribed:
            return
        if not self._connected.is_set():
            self._subscribed = tickers  # will subscribe on (re)connect
            return
        self._send_subscribe(tickers)

    # ── internals ───────────────────────────────────────────────────────────
    def _next_id(self) -> int:
        with self._lock:
            self._cmd_id += 1
            return self._cmd_id

    def _send_subscribe(self, tickers: tuple) -> None:
        msg = {
            "id": self._next_id(),
            "cmd": "subscribe",
            "params": {
                "channels": ["ticker", "trade"],
                "market_tickers": list(tickers),
            },
        }
        try:
            self.ws.send(json.dumps(msg))
            self._subscribed = tickers
            log.info("Kalshi WS: subscribed ticker/trade → %s", ", ".join(tickers))
        except Exception as exc:  # noqa: BLE001
            log.error("Kalshi WS subscribe failed: %s", exc)

    def _run_forever(self) -> None:
        path = urlparse(self.url).path or "/trade-api/ws/v2"
        while True:
            try:
                headers = self.auth.create_auth_headers("GET", path)
                header_list = [f"{k}: {v}" for k, v in headers.items()]
                self.ws = websocket.WebSocketApp(
                    self.url,
                    header=header_list,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self.ws.run_forever(ping_interval=10, ping_timeout=5)
            except Exception as exc:  # noqa: BLE001
                log.error("Kalshi WS crashed: %s", exc)
            self._connected.clear()
            log.info("Kalshi WS: reconnecting in 5s …")
            time.sleep(5)

    def _on_open(self, ws) -> None:
        self._connected.set()
        log.info("Kalshi WS: connected (%s)", self.url)
        if self._subscribed:
            self._send_subscribe(self._subscribed)

    def _on_error(self, ws, error) -> None:
        log.error("Kalshi WS error: %s", error)

    def _on_close(self, ws, code, reason) -> None:
        self._connected.clear()
        log.info("Kalshi WS: closed (code=%s reason=%s)", code, reason)

    def _on_message(self, ws, message) -> None:
        try:
            data = json.loads(message)
        except (ValueError, TypeError):
            return
        mtype = data.get("type")
        if mtype in ("subscribed", "ok"):
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
            last    = _to_dollars(msg.get("last_price_dollars", msg.get("price",
                                          msg.get("last_price"))))
            with _quote_lock:
                q = kalshi_quotes.setdefault(ticker, {})
                if yes_bid is not None: q["yes_bid"] = yes_bid
                if yes_ask is not None: q["yes_ask"] = yes_ask
                if last    is not None: q["last"]    = last
                q["ts"] = datetime.now(tz=timezone.utc)
        elif mtype == "trade":
            last = _to_dollars(msg.get("yes_price_dollars", msg.get("yes_price",
                                       msg.get("price"))))
            if last is not None:
                with _quote_lock:
                    q = kalshi_quotes.setdefault(ticker, {})
                    q["last"] = last
                    q["ts"]   = datetime.now(tz=timezone.utc)


def get_kalshi_quote(ticker: str) -> Optional[dict]:
    with _quote_lock:
        q = kalshi_quotes.get(ticker)
        return dict(q) if q else None


# ─────────────────────────────────────────────────────────────────────────────
# Kalshi REST helpers (V2 SDK)
# ─────────────────────────────────────────────────────────────────────────────
def load_pem() -> str:
    """Return the RSA private-key PEM content from file path or env var."""
    if KALSHI_PEM_PATH and os.path.exists(KALSHI_PEM_PATH):
        with open(KALSHI_PEM_PATH, "r") as fh:
            return fh.read()
    env_pem = os.getenv("KALSHI_PRIVATE_KEY")
    if env_pem:
        return env_pem
    raise FileNotFoundError(
        f"No Kalshi PEM at {KALSHI_PEM_PATH!r} and KALSHI_PRIVATE_KEY unset"
    )


class KalshiREST:
    """Thin wrapper over the kalshi-python-sync V2 Api classes."""

    def __init__(self):
        pem = load_pem()
        config = Configuration(host=KALSHI_BASE_URL)
        self.client = KalshiClient(config)
        self.client.set_kalshi_auth(KALSHI_API_KEY_ID, pem)
        self.auth = KalshiAuth(KALSHI_API_KEY_ID, pem)  # reused for WS handshake
        self.portfolio = PortfolioApi(self.client)
        self.markets   = MarketApi(self.client)
        self.events    = EventsApi(self.client)
        self.orders    = OrdersApi(self.client)
        log.info("Kalshi client built  demo=%s  base=%s", KALSHI_DEMO, KALSHI_BASE_URL)

    def get_balance_dollars(self) -> Optional[float]:
        resp = self.portfolio.get_balance()
        # GetBalanceResponse: balance (cents int) + balance_dollars
        bd = getattr(resp, "balance_dollars", None)
        if bd is not None:
            try:
                return float(bd)
            except (TypeError, ValueError):
                pass
        cents = getattr(resp, "balance", None)
        return (cents / 100.0) if cents is not None else None

    def get_market(self, ticker: str) -> Optional[object]:
        try:
            resp = self.markets.get_market(ticker)
            return getattr(resp, "market", None)
        except Exception as exc:  # noqa: BLE001
            log.warning("get_market(%s) failed: %s", ticker, exc)
            return None

    def get_open_series_markets(self) -> list:
        try:
            resp = self.events.get_events(
                series_ticker=SERIES_TICKER,
                status="open",
                with_nested_markets=True,
                limit=5,
            )
            out = []
            for ev in (getattr(resp, "events", None) or []):
                out.extend(getattr(ev, "markets", None) or [])
            return out
        except Exception as exc:  # noqa: BLE001
            log.error("get_events fallback failed: %s", exc)
            return []


def _market_field(market: object, *names):
    """Return the first non-None attribute among names from a Market model."""
    for n in names:
        v = getattr(market, n, None)
        if v is not None:
            return v
    return None


def resolve_active_market(rest: KalshiREST) -> Optional[dict]:
    """Resolve the active KXBTC15M market and its reference price."""
    global prev_window_close, prev_window_ticker

    current_ticker, next_ticker = current_and_next_tickers()
    parsed = parse_ticker(current_ticker)
    if parsed is None:
        log.error("Cannot parse constructed ticker %s", current_ticker)
        return None

    market_type = parsed["market_type"]
    settle_utc  = parsed["settle_utc"]
    suffix      = parsed["suffix"]

    market = rest.get_market(current_ticker)
    if market is None:
        log.warning("Direct lookup of %s failed – trying events query", current_ticker)
        markets = rest.get_open_series_markets()
        if markets:
            market = markets[0]
            current_ticker = _market_field(market, "ticker") or current_ticker
            parsed = parse_ticker(current_ticker) or parsed
            market_type = parsed["market_type"]
            settle_utc  = parsed["settle_utc"]
            suffix      = parsed["suffix"]

    if market is None:
        log.info("No open KXBTC15M market found")
        return None

    reference_price: Optional[float] = None

    if market_type == "absolute":
        strike = _market_field(market, "floor_strike", "cap_strike", "functional_strike")
        if strike is not None:
            try:
                reference_price = float(strike)
            except (TypeError, ValueError):
                reference_price = None
        if reference_price is None or reference_price <= 0:
            # parse "$60,309.79" from the subtitle
            for tf in ("yes_sub_title", "no_sub_title"):
                text = _market_field(market, tf) or ""
                m = re.search(r"\$([0-9,]+(?:\.\d+)?)", str(text))
                if m:
                    reference_price = float(m.group(1).replace(",", ""))
                    break
        if reference_price is None:
            log.error("Cannot find strike for absolute market %s", current_ticker)
            return None
    else:
        # relative -00 market: ref = previous window close
        if prev_window_close is not None:
            reference_price = prev_window_close
            log.info("Relative market – cached prev close $%.2f (%s)",
                     reference_price, prev_window_ticker)
        else:
            prev_ticker = build_ticker(SERIES_TICKER, settle_utc - timedelta(minutes=15))
            prev = rest.get_market(prev_ticker)
            if prev is not None:
                strike = _market_field(prev, "floor_strike", "functional_strike")
                if strike is not None:
                    try:
                        reference_price = float(strike)
                    except (TypeError, ValueError):
                        reference_price = None
            if reference_price is None:
                with _price_lock:
                    reference_price = latest_btc_price
                log.warning("Relative ref unavailable – using live Alpaca $%.2f",
                            reference_price)

    return {
        "ticker":          current_ticker,
        "next_ticker":     next_ticker,
        "market_type":     market_type,
        "suffix":          suffix,
        "reference_price": reference_price,
        "settle_utc":      settle_utc,
        "raw_market":      market,
    }


def place_kalshi_order(
    rest:      KalshiREST,
    ticker:    str,
    buy_side:  str,    # "yes" | "no"
    contracts: int = ORDER_CONTRACTS,
) -> Optional[object]:
    """Submit a marketable fill-or-kill V2 order.

    Single-book mapping:
        buy YES (bet UP)   → side=bid, price="0.99" (pay up to 99¢ for YES)
        buy NO  (bet DOWN) → side=ask, price="0.01" (sell YES low = long NO)
    """
    side       = BookSide.BID if buy_side == "yes" else BookSide.ASK
    price_str  = "0.99" if buy_side == "yes" else "0.01"
    count_str  = f"{float(contracts):.2f}"
    order_id   = str(uuid.uuid4())

    log.info(
        "ORDER %s  buy=%s  side=%s  price=%s  count=%s  ticker=%s  id=%s",
        "[DRY-RUN]" if DRY_RUN else "[LIVE]",
        buy_side.upper(), side.value, price_str, count_str, ticker, order_id,
    )

    if DRY_RUN:
        log.info("DRY_RUN active — order NOT submitted.")
        return None

    try:
        req = CreateOrderV2Request(
            ticker          = ticker,
            side            = side,
            count           = count_str,
            price           = price_str,
            time_in_force   = "fill_or_kill",
            client_order_id = order_id,
            self_trade_prevention_type = SelfTradePreventionType.TAKER_AT_CROSS,
        )
        resp = rest.orders.create_order_v2(create_order_v2_request=req)
        log.info(
            "ORDER RESULT: order_id=%s  fill_count=%s  remaining=%s  avg_price=%s",
            getattr(resp, "order_id", "?"),
            getattr(resp, "fill_count", "?"),
            getattr(resp, "remaining_count", "?"),
            getattr(resp, "average_fill_price", "?"),
        )
        return resp
    except Exception as exc:  # noqa: BLE001
        log.error("create_order_v2 failed: %s", exc)
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Timestamped price-delta snapshot
# ─────────────────────────────────────────────────────────────────────────────
def log_price_delta_snapshot(alpaca_price, alpaca_ts, market, delta) -> None:
    now_utc = datetime.now(tz=timezone.utc)
    alpaca_ts_str = (
        alpaca_ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        if alpaca_ts else "no tick yet"
    )
    settle_utc = market["settle_utc"]
    secs_left  = (settle_utc - now_utc).total_seconds()
    time_left  = f"{secs_left/60:.1f} min" if secs_left > 0 else "EXPIRED"
    mtype      = market["market_type"]
    ref_label  = "floor_strike (fixed)" if mtype == "absolute" else "prev close (relative)"

    q = get_kalshi_quote(market["ticker"])
    if q:
        ws_line = (f"yes_bid={q.get('yes_bid','?')}  yes_ask={q.get('yes_ask','?')}  "
                   f"last={q.get('last','?')}")
    else:
        ws_line = "no WS quote yet"

    gate_price_ok = abs(delta) >= PRICE_DELTA_GATE

    log.info(
        "\n"
        "┌─── Price Snapshot ──────────────────────────────────────────\n"
        "│  Cycle UTC         : %s\n"
        "│  Alpaca BTC/USD    : $%,.2f   (tick %s)\n"
        "│  Kalshi reference  : $%,.2f   (%s)\n"
        "│  Kalshi WS quote   : %s\n"
        "│  Market            : %s  type=%s  next=%s\n"
        "│  Settle at         : %s UTC  (%s remaining)\n"
        "│  Delta (Alp−Kal)   : %s$%,.2f   [gate=$%.0f %s]\n"
        "└──────────────────────────────────────────────────────────────",
        now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        alpaca_price, alpaca_ts_str,
        market["reference_price"], ref_label,
        ws_line,
        market["ticker"], mtype, market["next_ticker"],
        settle_utc.strftime("%H:%M"), time_left,
        ("▲" if delta > 0 else "▼" if delta < 0 else "="),
        abs(delta), PRICE_DELTA_GATE, ("✓" if gate_price_ok else "✗"),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Strategy loop
# ─────────────────────────────────────────────────────────────────────────────
def strategy_loop(rest: KalshiREST, market_ws: KalshiMarketWS, started_at: float) -> None:
    global prev_window_close, prev_window_ticker

    log.info("Strategy loop started – waiting for bars …")
    last_bar_count = 0
    last_window_ticker: Optional[str] = None

    while True:
        time.sleep(5)

        if (time.time() - started_at) / 60.0 >= RUNTIME_LIMIT_MIN:
            log.info("Runtime limit (%.0f min) reached — clean exit.", RUNTIME_LIMIT_MIN)
            return

        # keep the WS subscribed to the live window even before bars accumulate
        ct, nt = current_and_next_tickers()
        market_ws.ensure_subscribed((ct, nt))

        with _bar_lock:
            bars_snapshot = list(minute_bars)
        n = len(bars_snapshot)
        if n == last_bar_count or n < 5:
            continue
        last_bar_count = n
        log.info("── New bar (buffer %d/%d) ─────────────────────", n, HISTORY_BARS)

        with _price_lock:
            btc_price = latest_btc_price
            btc_ts    = latest_btc_ts
        if btc_price == 0.0:
            log.warning("No Alpaca tick yet – skipping")
            continue

        market = resolve_active_market(rest)
        if market is None:
            log.info("No active market resolved – skipping")
            continue

        market_ws.ensure_subscribed((market["ticker"], market["next_ticker"]))

        if last_window_ticker is not None and market["ticker"] != last_window_ticker:
            prev_window_close  = btc_price
            prev_window_ticker = last_window_ticker
            log.info("Window rolled %s → %s  cached close=$%.2f",
                     last_window_ticker, market["ticker"], prev_window_close)
        last_window_ticker = market["ticker"]

        delta = btc_price - market["reference_price"]
        log_price_delta_snapshot(btc_price, btc_ts, market, delta)

        abs_delta = abs(delta)
        ref       = market["reference_price"]
        ticker    = market["ticker"]

        if abs_delta < PRICE_DELTA_GATE:
            log.info("GATE MISS: |delta|=$%.2f < $%.0f", abs_delta, PRICE_DELTA_GATE)
            continue
        if ticker in positions_held:
            log.info("GATE MISS: already in %s", ticker)
            continue

        buy_side  = "yes" if delta > 0 else "no"
        direction = "UP (BUY YES)" if delta > 0 else "DOWN (BUY NO)"
        log.info("✦ SIGNAL: %s  delta=$%.2f  ref=$%.2f  btc=$%.2f",
                 direction, delta, ref, btc_price)

        resp = place_kalshi_order(rest, ticker, buy_side, ORDER_CONTRACTS)
        if DRY_RUN:
            positions_held.add(ticker)  # avoid repeat dry-run signals per window
        elif resp is not None:
            positions_held.add(ticker)
            log.info("✔ Position attempt recorded for %s side=%s", ticker, buy_side)


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    now_utc = datetime.now(tz=timezone.utc)
    ct, nt  = current_and_next_tickers()
    ct_p    = parse_ticker(ct)
    nt_p    = parse_ticker(nt)

    log.info("=" * 68)
    log.info("  BTC Kalshi 15-min algo-trader")
    log.info("  Now (UTC)       : %s", now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"))
    log.info("  Current window  : %s  type=%s  settle=%s UTC", ct,
             ct_p["market_type"] if ct_p else "?",
             ct_p["settle_utc"].strftime("%H:%M") if ct_p else "?")
    log.info("  Next window     : %s  type=%s", nt, nt_p["market_type"] if nt_p else "?")
    log.info("  Alpaca stream   : %s", BTC_SYMBOL)
    log.info("  Kalshi REST     : %s", KALSHI_BASE_URL)
    log.info("  Kalshi WS       : %s", KALSHI_WS_URL)
    log.info("  Demo            : %s", KALSHI_DEMO)
    log.info("  DRY_RUN         : %s", DRY_RUN)
    log.info("  Delta gate      : $%.0f   contracts/trade: %d",
             PRICE_DELTA_GATE, ORDER_CONTRACTS)
    log.info("  Runtime limit   : %.0f min", RUNTIME_LIMIT_MIN)
    log.info("=" * 68)

    if not (ALPACA_API_KEY and ALPACA_API_SECRET and KALSHI_API_KEY_ID):
        raise SystemExit("Missing credentials — set ALPACA_API_KEY, "
                         "ALPACA_API_SECRET, KALSHI_API_KEY_ID")

    rest = KalshiREST()

    try:
        bal = rest.get_balance_dollars()
        log.info("Kalshi auth OK – balance: $%.2f", bal if bal is not None else -1)
    except Exception as exc:
        log.error("Kalshi auth failed: %s", exc)
        raise

    # Alpaca BTC stream
    threading.Thread(target=run_alpaca_stream, daemon=True, name="alpaca-ws").start()

    # Kalshi market WS
    market_ws = KalshiMarketWS(rest.auth)
    market_ws.ensure_subscribed(current_and_next_tickers())
    market_ws.start()

    strategy_loop(rest, market_ws, started_at=time.time())


if __name__ == "__main__":
    main()
