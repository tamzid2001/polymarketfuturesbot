"""
test_kalshi_bot.py
─────────────────────────────────────────────────────────────────────────────
Pre-production QA suite for the Kalshi BTC 15-min bot.

Runs end-to-end against LIVE credentials but NEVER submits a real order.
Exits non-zero if any CRITICAL check fails, so CI goes red on a real problem.

Checks
──────
  1.  Secrets / env present
  2.  RSA private key (PEM) loads as an RSA key
  3.  All third-party dependencies import
  4.  Ticker build/parse round-trip (offline, deterministic)
  5.  Alpaca REST auth + live BTC/USD price
  6.  Alpaca real-time trade WebSocket delivers a tick
  7.  Kalshi REST auth + account balance
  8.  Kalshi active market resolves (with reference price)
  9.  Kalshi market WebSocket connects (RSA-signed handshake) + subscribes
  10. V2 order request builds for BUY YES and BUY NO (validation only)
  11. place_kalshi_order under DRY_RUN submits nothing

Run:  python test_kalshi_bot.py
"""

from __future__ import annotations

import os
import sys
import time
import threading
import traceback
from datetime import datetime, timedelta, timezone

# ── result accumulation ──────────────────────────────────────────────────────
RESULTS: list = []   # (name, status, detail, critical)
PASS, FAIL, WARN, SKIP = "PASS", "FAIL", "WARN", "SKIP"


def record(name, status, detail="", critical=True):
    RESULTS.append((name, status, detail, critical))
    icon = {"PASS": "✓", "FAIL": "✗", "WARN": "!", "SKIP": "·"}[status]
    print(f"  [{icon}] {name}: {status}" + (f" — {detail}" if detail else ""), flush=True)


def section(title):
    print(f"\n=== {title} ===", flush=True)


# ─────────────────────────────────────────────────────────────────────────────
# 1. Secrets / env
# ─────────────────────────────────────────────────────────────────────────────
def check_secrets():
    section("1. Secrets / environment")
    required = ["ALPACA_API_KEY", "ALPACA_API_SECRET", "KALSHI_API_KEY_ID"]
    for var in required:
        val = os.getenv(var, "")
        if val and val.strip():
            record(f"env {var}", PASS, f"len={len(val)}")
        else:
            record(f"env {var}", FAIL, "missing/empty")
    pem_path = os.getenv("KALSHI_PEM_PATH", "kalshi_private_key.pem")
    if (pem_path and os.path.exists(pem_path)) or os.getenv("KALSHI_PRIVATE_KEY"):
        src = pem_path if os.path.exists(pem_path) else "KALSHI_PRIVATE_KEY env"
        record("Kalshi PEM source", PASS, src)
    else:
        record("Kalshi PEM source", FAIL, "no PEM file and no KALSHI_PRIVATE_KEY")


# ─────────────────────────────────────────────────────────────────────────────
# 2. PEM validity
# ─────────────────────────────────────────────────────────────────────────────
def check_pem():
    section("2. RSA private key")
    try:
        from cryptography.hazmat.primitives.serialization import load_pem_private_key
        from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
        from cryptography.hazmat.backends import default_backend

        pem_path = os.getenv("KALSHI_PEM_PATH", "kalshi_private_key.pem")
        if os.path.exists(pem_path):
            pem = open(pem_path, "rb").read()
        else:
            pem = os.getenv("KALSHI_PRIVATE_KEY", "").encode()
        key = load_pem_private_key(pem, password=None, backend=default_backend())
        if isinstance(key, RSAPrivateKey):
            record("PEM is RSA private key", PASS, f"{key.key_size}-bit")
        else:
            record("PEM is RSA private key", FAIL, f"got {type(key).__name__}")
    except Exception as exc:  # noqa: BLE001
        record("PEM is RSA private key", FAIL, str(exc))


# ─────────────────────────────────────────────────────────────────────────────
# 3. Dependencies import
# ─────────────────────────────────────────────────────────────────────────────
def check_imports():
    section("3. Dependency imports")
    for mod in ("alpaca.data.live", "kalshi_python_sync", "websocket",
                "cryptography"):
        try:
            __import__(mod)
            record(f"import {mod}", PASS)
        except Exception as exc:  # noqa: BLE001
            record(f"import {mod}", FAIL, str(exc))


# ─────────────────────────────────────────────────────────────────────────────
# 4. Ticker round-trip (offline)
# ─────────────────────────────────────────────────────────────────────────────
def check_tickers(bot):
    section("4. Ticker build/parse (offline)")
    try:
        dt = datetime(2026, 6, 27, 0, 45, tzinfo=timezone.utc)
        t = bot.build_ticker("KXBTC15M", dt)
        ok = t == "KXBTC15M-26JUN270045-45"
        record("build_ticker", PASS if ok else FAIL, t)

        p = bot.parse_ticker(t)
        ok2 = (p and p["settle_utc"] == dt and p["market_type"] == "absolute"
               and p["suffix"] == "45")
        record("parse_ticker round-trip", PASS if ok2 else FAIL, str(p))

        rel = bot.parse_ticker("KXBTC15M-26JUN270100-00")
        record("relative market detection", PASS if rel and rel["market_type"] == "relative"
               else FAIL, rel["market_type"] if rel else "None")

        ct, nt = bot.current_and_next_tickers()
        cok = bot.parse_ticker(ct) is not None and bot.parse_ticker(nt) is not None
        record("current_and_next_tickers", PASS if cok else FAIL, f"{ct} / {nt}")
    except Exception as exc:  # noqa: BLE001
        record("ticker helpers", FAIL, str(exc))


# ─────────────────────────────────────────────────────────────────────────────
# 5. Alpaca REST price
# ─────────────────────────────────────────────────────────────────────────────
def check_alpaca_rest():
    section("5. Alpaca REST auth + live price")
    try:
        from alpaca.data.historical import CryptoHistoricalDataClient
        from alpaca.data.requests import CryptoLatestTradeRequest
        client = CryptoHistoricalDataClient(os.getenv("ALPACA_API_KEY"),
                                            os.getenv("ALPACA_API_SECRET"))
        req = CryptoLatestTradeRequest(symbol_or_symbols="BTC/USD")
        resp = client.get_crypto_latest_trade(req)
        trade = resp["BTC/USD"]
        price = float(trade.price)
        if price > 0:
            record("Alpaca REST BTC/USD", PASS, f"${price:,.2f}")
        else:
            record("Alpaca REST BTC/USD", FAIL, "price <= 0")
    except Exception as exc:  # noqa: BLE001
        record("Alpaca REST BTC/USD", FAIL, str(exc))


# ─────────────────────────────────────────────────────────────────────────────
# 6. Alpaca WebSocket tick
# ─────────────────────────────────────────────────────────────────────────────
def check_alpaca_ws():
    section("6. Alpaca real-time trade WebSocket")
    got = {"tick": None}

    async def _on_trade(trade):
        got["tick"] = float(trade.price)

    stream = None
    try:
        from alpaca.data.live import CryptoDataStream
        stream = CryptoDataStream(os.getenv("ALPACA_API_KEY"),
                                  os.getenv("ALPACA_API_SECRET"))
        stream.subscribe_trades(_on_trade, "BTC/USD")
        th = threading.Thread(target=stream.run, daemon=True)
        th.start()
        deadline = time.time() + 25
        while time.time() < deadline and got["tick"] is None:
            time.sleep(0.5)
        if got["tick"]:
            record("Alpaca WS tick", PASS, f"${got['tick']:,.2f}")
        else:
            record("Alpaca WS tick", WARN, "no tick in 25s (market quiet?)",
                   critical=False)
    except Exception as exc:  # noqa: BLE001
        record("Alpaca WS tick", FAIL, str(exc))
    finally:
        try:
            if stream is not None:
                stream.stop()
        except Exception:  # noqa: BLE001
            pass


# ─────────────────────────────────────────────────────────────────────────────
# 7-9. Kalshi REST + market + WS
# ─────────────────────────────────────────────────────────────────────────────
def check_kalshi(bot):
    section("7. Kalshi REST auth + balance")
    rest = None
    try:
        rest = bot.KalshiREST()
        bal = rest.get_balance_dollars()
        if bal is not None and bal >= 0:
            record("Kalshi balance", PASS, f"${bal:,.2f}")
        else:
            record("Kalshi balance", FAIL, f"got {bal}")
    except Exception as exc:  # noqa: BLE001
        record("Kalshi REST auth", FAIL, str(exc))
        return None

    section("8. Kalshi active market resolution")
    try:
        market = bot.resolve_active_market(rest)
        if market and market.get("reference_price") is not None:
            record("resolve_active_market", PASS,
                   f"{market['ticker']} type={market['market_type']} "
                   f"ref=${market['reference_price']:,.2f}")
        else:
            record("resolve_active_market", WARN,
                   "no open market / no reference yet", critical=False)
    except Exception as exc:  # noqa: BLE001
        record("resolve_active_market", FAIL, str(exc))

    section("9. Kalshi market WebSocket (RSA-signed handshake)")
    try:
        ws = bot.KalshiMarketWS(rest.auth)
        ct, nt = bot.current_and_next_tickers()
        ws.ensure_subscribed((ct, nt))
        ws.start()
        deadline = time.time() + 15
        while time.time() < deadline and not ws._connected.is_set():
            time.sleep(0.5)
        if ws._connected.is_set():
            # give it a moment to receive quotes
            time.sleep(4)
            nq = len(bot.kalshi_quotes)
            record("Kalshi WS connect+auth", PASS,
                   f"connected; {nq} quote(s) received")
        else:
            record("Kalshi WS connect+auth", FAIL,
                   "did not connect in 15s (auth/url?)")
    except Exception as exc:  # noqa: BLE001
        record("Kalshi WS connect+auth", FAIL, str(exc))
    return rest


# ─────────────────────────────────────────────────────────────────────────────
# 10-11. Order build + DRY_RUN safety
# ─────────────────────────────────────────────────────────────────────────────
def check_orders(bot, rest):
    section("10. V2 order request build (no submit)")
    try:
        from kalshi_python_sync import (CreateOrderV2Request, BookSide,
                                        SelfTradePreventionType)
        import uuid
        for buy_side, exp_side, exp_price in (("yes", BookSide.BID, "0.99"),
                                              ("no",  BookSide.ASK, "0.01")):
            side  = BookSide.BID if buy_side == "yes" else BookSide.ASK
            price = "0.99" if buy_side == "yes" else "0.01"
            req = CreateOrderV2Request(
                ticker="KXBTC15M-26JUN270045-45", side=side,
                count="5.00", price=price, time_in_force="fill_or_kill",
                client_order_id=str(uuid.uuid4()),
                self_trade_prevention_type=SelfTradePreventionType.TAKER_AT_CROSS)
            ok = (req.side == exp_side and req.price == exp_price)
            record(f"build order BUY {buy_side.upper()}", PASS if ok else FAIL,
                   f"side={req.side.value} price={req.price}")
    except Exception as exc:  # noqa: BLE001
        record("V2 order build", FAIL, str(exc))

    section("11. DRY_RUN submits nothing")
    try:
        prev = bot.DRY_RUN
        bot.DRY_RUN = True
        resp = bot.place_kalshi_order(rest, "KXBTC15M-26JUN270045-45", "yes", 1)
        bot.DRY_RUN = prev
        if resp is None:
            record("DRY_RUN no-submit", PASS, "returned None (nothing sent)")
        else:
            record("DRY_RUN no-submit", FAIL, "order object returned!")
    except Exception as exc:  # noqa: BLE001
        record("DRY_RUN no-submit", FAIL, str(exc))


# ─────────────────────────────────────────────────────────────────────────────
def main():
    print("=" * 70)
    print("  KALSHI BTC 15-MIN BOT — PRE-PRODUCTION QA SUITE")
    print(f"  {datetime.now(tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}")
    print("  NOTE: no real orders are ever submitted by this suite.")
    print("=" * 70)

    check_secrets()
    check_pem()
    check_imports()

    # Import the bot module (force DRY_RUN so import-time config is safe)
    os.environ.setdefault("DRY_RUN", "true")
    bot = None
    try:
        import kalshibtc15minupordown as bot
        record("import bot module", PASS, critical=True)
    except Exception as exc:  # noqa: BLE001
        section("Bot module import")
        record("import bot module", FAIL, str(exc))
        traceback.print_exc()

    if bot is not None:
        check_tickers(bot)
        check_alpaca_rest()
        check_alpaca_ws()
        rest = check_kalshi(bot)
        if rest is not None:
            check_orders(bot, rest)
        else:
            section("10-11. Orders")
            record("order checks", SKIP, "Kalshi REST unavailable", critical=False)

    # ── summary ──────────────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    counts = {PASS: 0, FAIL: 0, WARN: 0, SKIP: 0}
    crit_fail = 0
    for name, status, detail, critical in RESULTS:
        counts[status] += 1
        if status == FAIL and critical:
            crit_fail += 1
    print(f"  SUMMARY: {counts[PASS]} pass · {counts[FAIL]} fail · "
          f"{counts[WARN]} warn · {counts[SKIP]} skip")
    if crit_fail:
        print(f"  RESULT : ✗ {crit_fail} CRITICAL failure(s) — NOT production-ready")
    else:
        print("  RESULT : ✓ all critical checks passed")
        if counts[WARN]:
            print("           (review warnings above)")
    print("=" * 70)
    sys.exit(1 if crit_fail else 0)


if __name__ == "__main__":
    main()
