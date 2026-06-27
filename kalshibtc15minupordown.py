"""
kalshi_btc_15m_bot.py
─────────────────────────────────────────────────────────────────────────────
BTC 15-min Kalshi market algo-trader.

TICKER FORMAT  (verified from live Kalshi pages)
─────────────────────────────────────────────────
  Pattern : {SERIES}-{YY}{MON}{DD}{HHMM}-{MM}
  Example : KXBTC15M-26JUN270045-45

  Field       Value   Meaning
  ─────────── ─────── ──────────────────────────────────────────────────────
  KXBTC15M    series  15-min BTC up/down
  26          YY      year 2026
  JUN         MON     month (3-letter uppercase)
  27          DD      day (zero-padded)
  0045        HHMM    settlement time UTC in 24-hr HHMM  → 00:45 UTC
  45          MM      minute of the settlement hour (== HHMM[2:])
                      This is DETERMINISTIC and derivable from the datetime:
                        :00 windows → suffix "00"
                        :15 windows → suffix "15"
                        :30 windows → suffix "30"
                        :45 windows → suffix "45"

  Next-window example:
    KXBTC15M-26JUN270045-45  →  next  KXBTC15M-26JUN270100-00
    KXBTC15M-26JUN270100-00  →  next  KXBTC15M-26JUN270115-15

TWO MARKET TYPES (critical!)
─────────────────────────────
  Suffix -45 / -30 / -15  →  ABSOLUTE PRICE market
      "Resolves YES if BRTI ≥ $60,309.79 at 00:45 UTC"
      floor_strike is a fixed USD value set at market open.
      Strategy: compare real BTC price vs the fixed strike.

  Suffix -00              →  RELATIVE UP/DOWN market
      "Resolves YES if BRTI at 01:00 ≥ BRTI at 00:45"
      No fixed strike at open — the reference price is the PREVIOUS
      window's settlement value, known only at close.
      Target price shows "TBD" until settlement.
      Strategy: compare real BTC price vs the PREVIOUS window's close.

  The bot handles both types:
    • For absolute markets  → use floor_strike from the API.
    • For relative markets  → use the previous window's last known close
      (fetched via GET /markets/{prev_ticker} → result_value, or fall
      back to the most recent Alpaca tick as the reference price).

STRATEGY
────────
1. Stream BTC/USD real-time trades via Alpaca WebSocket → 1-min OHLCV bars
   (rolling 60-bar / 1-hour buffer).
2. Every completed 1-min bar:
   – Get real-time Alpaca BTC price.
   – Fetch the active KXBTC15M market from Kalshi REST API.
   – Determine market type (absolute vs relative) from the suffix.
   – Resolve the effective reference price accordingly.
3. Log a TIMESTAMPED snapshot every cycle:
   Alpaca real-time price vs Kalshi reference price + signed delta.
4. Decision gate:
   – |delta| > PRICE_DELTA_GATE ($10 default)
   – No existing position in this 15-min window
   Then: delta > 0 → BUY YES; delta < 0 → BUY NO.

SETTLEMENT
──────────
  Resolves on the 60-second BRTI average in the final minute before the
  stated UTC time.  Source: CF Benchmarks Real-Time Index.

DEPENDENCIES
────────────
    pip install alpaca-py kalshi-python-sync

CREDENTIALS  (env vars or edit CONFIG below)
────────────────────────────────────────────
    ALPACA_API_KEY       Alpaca API key ID
    ALPACA_API_SECRET    Alpaca secret key
    KALSHI_API_KEY_ID    Kalshi key ID  (Account & Security → API Keys)
    KALSHI_PEM_PATH      Path to Kalshi RSA private key (.pem)
    KALSHI_DEMO          "true" for sandbox (default: false)

KALSHI API NOTES (post-March-2026 fixed-point migration)
──────────────────────────────────────────────────────────
  • yes_price in create_order → dollar string "0.55", NOT integer cents.
  • Production URL: https://api.elections.kalshi.com/trade-api/v2
  • Demo URL:       https://demo-api.kalshi.co/trade-api/v2
"""

from __future__ import annotations

import logging
import os
import re
import threading
import time
import uuid
from collections import deque
from datetime import datetime, timedelta, timezone

from alpaca.data.live import CryptoDataStream
from kalshi_python_sync import Configuration, KalshiClient

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
ALPACA_API_KEY    = os.getenv("ALPACA_API_KEY",    "YOUR_ALPACA_KEY")
ALPACA_API_SECRET = os.getenv("ALPACA_API_SECRET", "YOUR_ALPACA_SECRET")
KALSHI_API_KEY_ID = os.getenv("KALSHI_API_KEY_ID", "YOUR_KALSHI_KEY_ID")
KALSHI_PEM_PATH   = os.getenv("KALSHI_PEM_PATH",   "kalshi_private_key.pem")
KALSHI_DEMO       = os.getenv("KALSHI_DEMO", "false").lower() in ("1", "true", "yes")

KALSHI_BASE_URL = (
    "https://demo-api.kalshi.co/trade-api/v2"
    if KALSHI_DEMO
    else "https://api.elections.kalshi.com/trade-api/v2"
)

BTC_SYMBOL       = "BTC/USD"
HISTORY_BARS     = 60     # 60 × 1-min bars = 1 hour (kept for bar-close trigger)
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
log = logging.getLogger("kalshi_btc_bot")


# ─────────────────────────────────────────────────────────────────────────────
# Shared state
# ─────────────────────────────────────────────────────────────────────────────
minute_bars: deque[dict]          = deque(maxlen=HISTORY_BARS)
_current_bar: dict | None         = None
_current_bar_minute: datetime | None = None
_bar_lock = threading.Lock()

latest_btc_price: float           = 0.0
latest_btc_ts:    datetime | None = None
_price_lock = threading.Lock()

# Previous window's settlement reference (used for relative -00 markets)
prev_window_close:  float | None  = None
prev_window_ticker: str | None    = None

positions_held: set[str] = set()


# ─────────────────────────────────────────────────────────────────────────────
# Ticker helpers  (fully deterministic)
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
    """
    Construct the full KXBTC15M ticker for a given settlement UTC datetime.

    Suffix = zero-padded minute of the settlement time:
        :00 → "00",  :15 → "15",  :30 → "30",  :45 → "45"
    """
    yy     = settle_utc.strftime("%y")
    mon    = settle_utc.strftime("%b").upper()
    dd     = settle_utc.strftime("%d")
    hhmm   = settle_utc.strftime("%H%M")
    suffix = settle_utc.strftime("%M")
    return f"{series}-{yy}{mon}{dd}{hhmm}-{suffix}"


def parse_ticker(ticker: str) -> dict | None:
    """
    Parse a KXBTC15M ticker into its components.
    Returns dict: series, settle_utc, suffix, market_type.

    market_type:
        "absolute"  → suffix in {"15","30","45"}  (fixed strike)
        "relative"  → suffix == "00"              (up/down vs previous close)
    """
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


def current_and_next_tickers(series: str = SERIES_TICKER) -> tuple[str, str]:
    """
    Return (current_window_ticker, next_window_ticker) based on UTC now.
    Settlement time = start of window + 15 min.
    """
    now       = datetime.now(tz=timezone.utc)
    slot_min  = (now.minute // 15) * 15
    curr_open = now.replace(minute=slot_min, second=0, microsecond=0)
    next_open = curr_open + timedelta(minutes=15)

    return (
        build_ticker(series, curr_open + timedelta(minutes=15)),
        build_ticker(series, next_open + timedelta(minutes=15)),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Alpaca WebSocket
# ─────────────────────────────────────────────────────────────────────────────

def _minute_bucket(ts: datetime) -> datetime:
    return ts.replace(second=0, microsecond=0, tzinfo=timezone.utc)


async def on_trade(trade) -> None:
    global _current_bar, _current_bar_minute, latest_btc_price, latest_btc_ts

    price: float = float(trade.price)
    size:  float = float(trade.size)
    ts:    datetime = trade.timestamp
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
                log.debug(
                    "Bar closed %s  O=%.2f H=%.2f L=%.2f C=%.2f V=%.6f",
                    _current_bar_minute,
                    _current_bar["open"], _current_bar["high"],
                    _current_bar["low"],  _current_bar["close"],
                    _current_bar["volume"],
                )
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
    log.info("Alpaca WebSocket: connecting, subscribing to %s …", BTC_SYMBOL)
    stream = CryptoDataStream(ALPACA_API_KEY, ALPACA_API_SECRET)
    stream.subscribe_trades(on_trade, BTC_SYMBOL)
    stream.run()


# ─────────────────────────────────────────────────────────────────────────────
# Kalshi helpers
# ─────────────────────────────────────────────────────────────────────────────

def build_kalshi_client() -> KalshiClient:
    config = Configuration(host=KALSHI_BASE_URL)
    with open(KALSHI_PEM_PATH, "r") as fh:
        config.private_key_pem = fh.read()
    config.api_key_id = KALSHI_API_KEY_ID
    log.info("Kalshi client built  demo=%s", KALSHI_DEMO)
    return KalshiClient(config)


def get_market_detail(client: KalshiClient, ticker: str) -> dict | None:
    """Fetch a single market record from GET /markets/{ticker}."""
    try:
        resp = client.get(f"/markets/{ticker}")
        return resp.json().get("market")
    except Exception as exc:
        log.error("GET /markets/%s failed: %s", ticker, exc)
        return None


def get_active_btc15m_market(client: KalshiClient) -> dict | None:
    """
    Determine the active KXBTC15M market and resolve all parameters.

    Returns a dict:
        ticker          str    full market ticker
        next_ticker     str    next window's full ticker
        market_type     str    "absolute" | "relative"
        suffix          str    "00"|"15"|"30"|"45"
        reference_price float  the price to compare real BTC against:
                               – absolute: floor_strike from market record
                               – relative: previous window's close
        settle_utc      datetime UTC settlement time
        raw_market      dict   full API market record

    Returns None if the market cannot be resolved.
    """
    current_ticker, next_ticker = current_and_next_tickers()
    parsed = parse_ticker(current_ticker)
    if parsed is None:
        log.error("Cannot parse constructed ticker %s", current_ticker)
        return None

    market_type = parsed["market_type"]
    settle_utc  = parsed["settle_utc"]
    suffix      = parsed["suffix"]

    log.info(
        "Window: %s  type=%s  settle=%s UTC  next=%s",
        current_ticker, market_type,
        settle_utc.strftime("%H:%M"), next_ticker,
    )

    # Fetch live market record — try deterministic ticker first
    raw = get_market_detail(client, current_ticker)

    # Fallback to events list if direct lookup misses
    if raw is None:
        log.warning("Direct lookup of %s failed – trying events list", current_ticker)
        try:
            resp = client.get(
                "/events",
                params={
                    "series_ticker":       SERIES_TICKER,
                    "status":              "open",
                    "with_nested_markets": True,
                    "limit":               3,
                },
            )
            events = resp.json().get("events", [])
            if events:
                markets = events[0].get("markets", [])
                if markets:
                    raw = markets[0]
                    current_ticker = raw["ticker"]
                    parsed         = parse_ticker(current_ticker) or parsed
                    market_type    = parsed["market_type"]
                    settle_utc     = parsed["settle_utc"]
                    suffix         = parsed["suffix"]
        except Exception as exc:
            log.error("Events fallback failed: %s", exc)
            return None

    if raw is None:
        log.warning("No open KXBTC15M market found")
        return None

    # ── Resolve reference price by market type ────────────────────────────
    reference_price: float | None = None

    if market_type == "absolute":
        for field in ("floor_strike", "floor_strike_fp", "result_value"):
            val = raw.get(field)
            if val is not None:
                try:
                    reference_price = float(val)
                    if reference_price > 0:
                        break
                except (ValueError, TypeError):
                    pass
        # Fallback: parse from subtitle
        if reference_price is None:
            for text_field in ("yes_sub_title", "no_sub_title"):
                text = raw.get(text_field, "")
                m = re.search(r"\$([0-9,]+(?:\.\d+)?)", text)
                if m:
                    try:
                        reference_price = float(m.group(1).replace(",", ""))
                        break
                    except ValueError:
                        pass
        if reference_price is None:
            log.error(
                "Cannot find floor_strike for absolute market %s  raw=%s",
                current_ticker,
                {k: raw.get(k) for k in
                 ["floor_strike", "floor_strike_fp", "result_value",
                  "yes_sub_title", "no_sub_title"]},
            )
            return None

    else:  # relative
        if prev_window_close is not None:
            reference_price = prev_window_close
            log.info(
                "Relative market – using cached prev close $%.2f from %s",
                reference_price, prev_window_ticker,
            )
        else:
            prev_ticker = build_ticker(SERIES_TICKER, settle_utc - timedelta(minutes=15))
            log.info("Relative market – fetching prev window: %s", prev_ticker)
            prev_raw = get_market_detail(client, prev_ticker)
            if prev_raw is not None:
                for field in ("result_value", "floor_strike", "floor_strike_fp"):
                    val = prev_raw.get(field)
                    if val is not None:
                        try:
                            reference_price = float(val)
                            if reference_price > 0:
                                break
                        except (ValueError, TypeError):
                            pass
            if reference_price is None:
                with _price_lock:
                    reference_price = latest_btc_price
                log.warning(
                    "Cannot get prev window close – using live Alpaca "
                    "price $%.2f as relative reference", reference_price,
                )

    return {
        "ticker":          current_ticker,
        "next_ticker":     next_ticker,
        "market_type":     market_type,
        "suffix":          suffix,
        "reference_price": reference_price,
        "settle_utc":      settle_utc,
        "raw_market":      raw,
    }


def place_kalshi_order(
    client:    KalshiClient,
    ticker:    str,
    side:      str,
    action:    str,
    contracts: int = ORDER_CONTRACTS,
) -> object | None:
    """
    Fill-or-kill limit order.
    yes_price is a dollar string per post-2026 Kalshi API.
    YES ceiling = "0.99"; NO ceiling expressed as yes_price = "0.01".
    """
    order_id      = str(uuid.uuid4())
    yes_price_str = "0.99" if side == "yes" else "0.01"

    log.info(
        "ORDER: %s %s  ticker=%s  qty=%d  yes_price=%s  id=%s",
        action.upper(), side.upper(), ticker, contracts, yes_price_str, order_id,
    )
    try:
        result = client.create_order(
            ticker          = ticker,
            action          = action,
            side            = side,
            type            = "limit",
            time_in_force   = "fill_or_kill",
            yes_price       = yes_price_str,
            count           = contracts,
            client_order_id = order_id,
        )
        order  = getattr(result, "order", result)
        status = getattr(order, "status", "?")
        filled = getattr(order, "filled_count_fp",
                         getattr(order, "filled_count", "?"))
        log.info("ORDER RESULT: status=%s  filled=%s", status, filled)
        return order
    except Exception as exc:
        log.error("Kalshi create_order failed: %s", exc)
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Timestamped price-delta snapshot
# ─────────────────────────────────────────────────────────────────────────────

def log_price_delta_snapshot(
    alpaca_price: float,
    alpaca_ts:    datetime | None,
    market:       dict,
    delta:        float,
) -> None:
    """
    Log a rich timestamped snapshot every cycle showing the live spread
    between the Alpaca real-time BTC price and the Kalshi reference price.
    """
    now_utc    = datetime.now(tz=timezone.utc)
    settle_utc = market["settle_utc"]
    secs_left  = (settle_utc - now_utc).total_seconds()
    time_left  = f"{secs_left / 60:.1f} min" if secs_left > 0 else "EXPIRED"

    alpaca_ts_str = (
        alpaca_ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        if alpaca_ts else "no tick yet"
    )

    mtype     = market["market_type"]
    ref_label = (
        "floor_strike (fixed)"
        if mtype == "absolute"
        else "prev window close (relative)"
    )
    gate_ok = abs(delta) >= PRICE_DELTA_GATE

    log.info(
        "\n"
        "┌─── Price Snapshot ──────────────────────────────────────────\n"
        "│  Cycle UTC         : %s\n"
        "│\n"
        "│  Alpaca BTC/USD    : $%,.2f\n"
        "│  Tick timestamp    : %s\n"
        "│\n"
        "│  Kalshi reference  : $%,.2f  (%s)\n"
        "│  Market type       : %s  (suffix -%s)\n"
        "│  Market ticker     : %s\n"
        "│  Next window       : %s\n"
        "│  Settle at         : %s UTC  (%s remaining)\n"
        "│\n"
        "│  Delta (Alp−Kal)   : %s$%,.2f  [gate=$%.0f  %s]\n"
        "└──────────────────────────────────────────────────────────────",
        now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        alpaca_price,
        alpaca_ts_str,
        market["reference_price"], ref_label,
        mtype, market["suffix"],
        market["ticker"],
        market["next_ticker"],
        settle_utc.strftime("%H:%M"), time_left,
        ("▲" if delta > 0 else "▼" if delta < 0 else "="),
        abs(delta), PRICE_DELTA_GATE,
        ("✓ GATE MET" if gate_ok else "✗ below gate"),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Strategy loop
# ─────────────────────────────────────────────────────────────────────────────

def strategy_loop(client: KalshiClient) -> None:
    global prev_window_close, prev_window_ticker

    log.info("Strategy loop started – waiting for bars …")
    last_bar_count     = 0
    last_window_ticker: str | None = None

    while True:
        time.sleep(5)

        with _bar_lock:
            n = len(minute_bars)

        if n == last_bar_count or n < 1:
            continue
        last_bar_count = n
        log.info("── New bar (buffer %d/%d) ──────────────────────────────", n, HISTORY_BARS)

        # ── 1. Alpaca real-time price ─────────────────────────────────────
        with _price_lock:
            btc_price = latest_btc_price
            btc_ts    = latest_btc_ts
        if btc_price == 0.0:
            log.warning("No Alpaca tick yet – skipping")
            continue

        # ── 2. Resolve active Kalshi market ───────────────────────────────
        market = get_active_btc15m_market(client)
        if market is None:
            log.info("No active market resolved – skipping")
            continue

        # Cache close on window rollover (for next relative market)
        if last_window_ticker is not None and market["ticker"] != last_window_ticker:
            prev_window_close  = btc_price
            prev_window_ticker = last_window_ticker
            log.info(
                "Window rolled %s → %s  cached close=$%.2f",
                last_window_ticker, market["ticker"], prev_window_close,
            )
        last_window_ticker = market["ticker"]

        # ── 3. Delta + timestamped snapshot (always) ──────────────────────
        delta = btc_price - market["reference_price"]
        log_price_delta_snapshot(
            alpaca_price = btc_price,
            alpaca_ts    = btc_ts,
            market       = market,
            delta        = delta,
        )

        # ── 4. Decision gate ──────────────────────────────────────────────
        ticker    = market["ticker"]
        abs_delta = abs(delta)

        if abs_delta < PRICE_DELTA_GATE:
            log.info("GATE MISS: |delta|=$%.2f < $%.0f", abs_delta, PRICE_DELTA_GATE)
            continue
        if ticker in positions_held:
            log.info("GATE MISS: already in %s", ticker)
            continue

        # ── 5. Signal → order ────────────────────────────────────────────
        side      = "yes" if delta > 0 else "no"
        direction = "UP (BUY YES)" if delta > 0 else "DOWN (BUY NO)"
        log.info(
            "✦ SIGNAL: %s  delta=$%.2f  ref=$%.2f  btc=$%.2f",
            direction, delta, market["reference_price"], btc_price,
        )

        order = place_kalshi_order(client, ticker, side, "buy", ORDER_CONTRACTS)
        if order is not None:
            status = getattr(order, "status", "unknown")
            if status in ("executed", "resting"):
                positions_held.add(ticker)
                log.info("✔ Position in %s  side=%s  status=%s", ticker, side, status)
            else:
                log.warning("Unexpected order status: %s", status)


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
    log.info("")
    log.info("  TICKER FORMAT: {SERIES}-{YY}{MON}{DD}{HHMM}-{MM}")
    log.info("  Suffix = zero-padded minute of settlement time")
    log.info("    :00 → -00  (relative up/down market, TBD strike)")
    log.info("    :15 → -15  (absolute price market, fixed strike)")
    log.info("    :30 → -30  (absolute price market, fixed strike)")
    log.info("    :45 → -45  (absolute price market, fixed strike)")
    log.info("")
    log.info("  Now (UTC)       : %s", now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"))
    log.info(
        "  Current window  : %s  type=%s  settle=%s UTC",
        ct,
        ct_p["market_type"] if ct_p else "?",
        ct_p["settle_utc"].strftime("%H:%M") if ct_p else "?",
    )
    log.info(
        "  Next window     : %s  type=%s  settle=%s UTC",
        nt,
        nt_p["market_type"] if nt_p else "?",
        nt_p["settle_utc"].strftime("%H:%M") if nt_p else "?",
    )
    log.info("")
    log.info("  Alpaca stream   : %s", BTC_SYMBOL)
    log.info("  Kalshi demo     : %s", KALSHI_DEMO)
    log.info("  Delta gate      : $%.0f", PRICE_DELTA_GATE)
    log.info("  Contracts/trade : %d", ORDER_CONTRACTS)
    log.info("=" * 68)

    kalshi = build_kalshi_client()

    try:
        bal_resp  = kalshi.get_balance()
        bal_cents = getattr(bal_resp, "balance", None)
        if bal_cents is not None:
            log.info("Kalshi auth OK – balance: $%.2f", bal_cents / 100)
    except Exception as exc:
        log.error("Kalshi auth failed: %s", exc)
        raise

    ws_thread = threading.Thread(
        target=run_alpaca_stream, daemon=True, name="alpaca-ws",
    )
    ws_thread.start()

    strategy_loop(kalshi)


if __name__ == "__main__":
    main()
