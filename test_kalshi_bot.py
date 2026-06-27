"""
test_kalshi_bot.py
─────────────────────────────────────────────────────────────────────────────
Pre-production QA suite for the ASYNC Kalshi BTC 15-min bot.

Runs end-to-end against LIVE credentials but NEVER submits a real order.
Exits non-zero if any CRITICAL check fails, so CI goes red on a real problem.

It prints LIVE INFO with substantial logging:
  • live BTC/USD price (Alpaca REST + WS tick)
  • full live Kalshi BTC 15-min up/down market snapshot (bids/asks/last/strike)
  • a live stream of Kalshi market WebSocket messages (verbose)
  • the predicted NEXT 15-min up/down ticker

Checks
──────
  1.  Secrets / env present
  2.  RSA private key (PEM) loads as RSA
  3.  Dependencies import
  4.  Ticker build/parse round-trip + NEXT-ticker prediction
  5.  Alpaca REST auth + live BTC/USD price
  6.  Alpaca real-time trade WebSocket delivers a tick
  7.  Kalshi REST auth + balance              (async)
  8.  Kalshi active market resolves + LIVE MARKET SNAPSHOT  (async)
  9.  Kalshi market WebSocket live stream (RSA handshake, verbose)  (async)
  10. V2 MARKET order build: BUY YES / BUY NO / CLOSE (validation only)
  11. market_buy / market_close under DRY_RUN submit nothing

Run:  python test_kalshi_bot.py
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
import threading
import traceback
from datetime import datetime, timezone

RESULTS: list = []
PASS, FAIL, WARN, SKIP = "PASS", "FAIL", "WARN", "SKIP"


def record(name, status, detail="", critical=True):
    RESULTS.append((name, status, detail, critical))
    icon = {"PASS": "✓", "FAIL": "✗", "WARN": "!", "SKIP": "·"}[status]
    print(f"  [{icon}] {name}: {status}" + (f" — {detail}" if detail else ""), flush=True)


def section(title):
    print(f"\n=== {title} ===", flush=True)


# 1 ───────────────────────────────────────────────────────────────────────────
def check_secrets():
    section("1. Secrets / environment")
    for var in ("ALPACA_API_KEY", "ALPACA_API_SECRET", "KALSHI_API_KEY_ID"):
        val = os.getenv(var, "")
        record(f"env {var}", PASS if val.strip() else FAIL,
               f"len={len(val)}" if val.strip() else "missing/empty")
    pem_path = os.getenv("KALSHI_PEM_PATH", "kalshi_private_key.pem")
    if (pem_path and os.path.exists(pem_path)) or os.getenv("KALSHI_PRIVATE_KEY"):
        record("Kalshi PEM source", PASS,
               pem_path if os.path.exists(pem_path) else "KALSHI_PRIVATE_KEY env")
    else:
        record("Kalshi PEM source", FAIL, "no PEM file and no KALSHI_PRIVATE_KEY")


# 2 ───────────────────────────────────────────────────────────────────────────
def check_pem():
    section("2. RSA private key")
    try:
        from cryptography.hazmat.primitives.serialization import load_pem_private_key
        from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
        from cryptography.hazmat.backends import default_backend
        pem_path = os.getenv("KALSHI_PEM_PATH", "kalshi_private_key.pem")
        pem = (open(pem_path, "rb").read() if os.path.exists(pem_path)
               else os.getenv("KALSHI_PRIVATE_KEY", "").encode())
        key = load_pem_private_key(pem, password=None, backend=default_backend())
        record("PEM is RSA private key", PASS if isinstance(key, RSAPrivateKey) else FAIL,
               f"{key.key_size}-bit" if isinstance(key, RSAPrivateKey) else type(key).__name__)
    except Exception as exc:  # noqa: BLE001
        record("PEM is RSA private key", FAIL, str(exc))


# 3 ───────────────────────────────────────────────────────────────────────────
def check_imports():
    section("3. Dependency imports")
    for mod in ("alpaca.data.live", "kalshi_python_async", "aiohttp", "cryptography"):
        try:
            __import__(mod)
            record(f"import {mod}", PASS)
        except Exception as exc:  # noqa: BLE001
            record(f"import {mod}", FAIL, str(exc))


# 4 ───────────────────────────────────────────────────────────────────────────
def check_tickers(bot):
    section("4. Ticker build/parse + NEXT-ticker prediction")
    try:
        dt = datetime(2026, 6, 27, 0, 45, tzinfo=timezone.utc)
        t = bot.build_ticker("KXBTC15M", dt)
        record("build_ticker", PASS if t == "KXBTC15M-26JUN270045-45" else FAIL, t)
        p = bot.parse_ticker(t)
        record("parse_ticker round-trip",
               PASS if (p and p["settle_utc"] == dt and p["market_type"] == "absolute") else FAIL,
               str(p))
        rel = bot.parse_ticker("KXBTC15M-26JUN270100-00")
        record("relative detection",
               PASS if rel and rel["market_type"] == "relative" else FAIL,
               rel["market_type"] if rel else "None")
        nxt = bot.log_next_ticker_prediction()  # prints prediction
        record("NEXT-ticker prediction", PASS if bot.parse_ticker(nxt) else FAIL, nxt)
    except Exception as exc:  # noqa: BLE001
        record("ticker helpers", FAIL, str(exc))


# 5 ───────────────────────────────────────────────────────────────────────────
def check_alpaca_rest():
    section("5. Alpaca REST auth + live BTC/USD price")
    try:
        from alpaca.data.historical import CryptoHistoricalDataClient
        from alpaca.data.requests import CryptoLatestTradeRequest
        client = CryptoHistoricalDataClient(os.getenv("ALPACA_API_KEY"),
                                            os.getenv("ALPACA_API_SECRET"))
        resp = client.get_crypto_latest_trade(
            CryptoLatestTradeRequest(symbol_or_symbols="BTC/USD"))
        price = float(resp["BTC/USD"].price)
        print(f"      LIVE BTC/USD (Alpaca REST): ${price:,.2f}", flush=True)
        record("Alpaca REST BTC/USD", PASS if price > 0 else FAIL, f"${price:,.2f}")
    except Exception as exc:  # noqa: BLE001
        record("Alpaca REST BTC/USD", FAIL, str(exc))


# 6 ───────────────────────────────────────────────────────────────────────────
def check_alpaca_ws():
    section("6. Alpaca real-time trade WebSocket")
    got = {"tick": None}

    async def _on_trade(trade):
        got["tick"] = float(trade.price)

    stream = None
    try:
        from alpaca.data.live import CryptoDataStream
        stream = CryptoDataStream(os.getenv("ALPACA_API_KEY"), os.getenv("ALPACA_API_SECRET"))
        stream.subscribe_trades(_on_trade, "BTC/USD")
        threading.Thread(target=stream.run, daemon=True).start()
        deadline = time.time() + 25
        while time.time() < deadline and got["tick"] is None:
            time.sleep(0.5)
        if got["tick"]:
            print(f"      LIVE BTC/USD (Alpaca WS tick): ${got['tick']:,.2f}", flush=True)
            record("Alpaca WS tick", PASS, f"${got['tick']:,.2f}")
        else:
            record("Alpaca WS tick", WARN, "no tick in 25s", critical=False)
    except Exception as exc:  # noqa: BLE001
        record("Alpaca WS tick", FAIL, str(exc))
    finally:
        try:
            if stream is not None:
                stream.stop()
        except Exception:  # noqa: BLE001
            pass


# 7-9 (async) ──────────────────────────────────────────────────────────────────
def _print_market_snapshot(market: dict):
    raw = market["raw_market"]
    fields = ["ticker", "status", "yes_sub_title", "no_sub_title",
              "yes_bid_dollars", "yes_ask_dollars", "no_bid_dollars", "no_ask_dollars",
              "last_price_dollars", "previous_price_dollars",
              "floor_strike", "cap_strike", "strike_type",
              "volume", "open_interest", "liquidity_dollars"]
    print("      ┌─ LIVE KALSHI BTC 15-MIN MARKET ──────────────────────", flush=True)
    print(f"      │ resolved type : {market['market_type']}  "
          f"ref=${market['reference_price']:,.2f}", flush=True)
    print(f"      │ settle (UTC)  : {market['settle_utc'].strftime('%Y-%m-%dT%H:%MZ')}", flush=True)
    print(f"      │ next ticker   : {market['next_ticker']}", flush=True)
    for f in fields:
        v = getattr(raw, f, None)
        if v is not None:
            print(f"      │ {f:<22}: {v}", flush=True)
    print("      └───────────────────────────────────────────────────────", flush=True)


async def check_kalshi_async(bot):
    section("7. Kalshi REST auth + balance (async)")
    rest = None
    try:
        rest = bot.KalshiREST()
        bal = await rest.get_balance_dollars()
        record("Kalshi balance", PASS if (bal is not None and bal >= 0) else FAIL,
               f"${bal:,.2f}" if bal is not None else "None")
    except Exception as exc:  # noqa: BLE001
        record("Kalshi REST auth", FAIL, str(exc))
        traceback.print_exc()
        return None

    section("8. Kalshi active market + LIVE SNAPSHOT (async)")
    market = None
    try:
        market = await bot.resolve_active_market(rest)
        if market and market.get("reference_price") is not None:
            _print_market_snapshot(market)
            record("resolve_active_market", PASS,
                   f"{market['ticker']} ref=${market['reference_price']:,.2f}")
        else:
            record("resolve_active_market", WARN, "no open market yet", critical=False)
    except Exception as exc:  # noqa: BLE001
        record("resolve_active_market", FAIL, str(exc))

    section("9. Kalshi market WebSocket LIVE STREAM (verbose, async)")
    try:
        ws = bot.KalshiMarketWS(rest.auth)
        ct, nt = bot.current_and_next_tickers()
        ws.set_tickers((ct, nt))
        task = asyncio.create_task(ws.run())
        # let it connect + stream live messages for ~12s (verbose logging on)
        for _ in range(24):
            await asyncio.sleep(0.5)
            if ws.connected and ws.msg_count >= 1:
                pass
        await asyncio.sleep(8)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        if ws.connected or ws.msg_count > 0:
            record("Kalshi WS live stream", PASS,
                   f"connected={ws.connected}, {ws.msg_count} msg(s), "
                   f"{len(bot.kalshi_quotes)} quote(s)")
        else:
            record("Kalshi WS live stream", FAIL, "no connection / no messages (auth/url?)")
    except Exception as exc:  # noqa: BLE001
        record("Kalshi WS live stream", FAIL, str(exc))

    return rest


# 10-11 ────────────────────────────────────────────────────────────────────────
async def check_orders_async(bot, rest):
    section("10. V2 MARKET order build: BUY YES / BUY NO / CLOSE (no submit)")
    try:
        from kalshi_python_async import (CreateOrderV2Request, BookSide,
                                         SelfTradePreventionType)
        import uuid
        cases = [
            ("BUY YES",   BookSide.BID, "0.99", False),
            ("BUY NO",    BookSide.ASK, "0.01", False),
            ("CLOSE YES", BookSide.ASK, "0.01", True),
            ("CLOSE NO",  BookSide.BID, "0.99", True),
        ]
        for label, side, price, reduce_only in cases:
            req = CreateOrderV2Request(
                ticker="KXBTC15M-26JUN270045-45", side=side, count="5.00", price=price,
                time_in_force="immediate_or_cancel", client_order_id=str(uuid.uuid4()),
                self_trade_prevention_type=SelfTradePreventionType.TAKER_AT_CROSS,
                reduce_only=reduce_only)
            ok = (req.side == side and req.price == price and req.reduce_only == reduce_only)
            record(f"build {label}", PASS if ok else FAIL,
                   f"side={req.side.value} price={req.price} reduce_only={req.reduce_only}")
    except Exception as exc:  # noqa: BLE001
        record("V2 order build", FAIL, str(exc))

    section("11. DRY_RUN market_buy / market_close submit nothing")
    try:
        class _FakeOrders:
            async def create_order_v2(self, **k):
                raise AssertionError("DRY_RUN must not submit")
        prev = bot.DRY_RUN
        bot.DRY_RUN = True
        rest.orders = _FakeOrders()
        tk = "KXBTC15M-26JUN270045-45"
        r1 = await bot.market_buy(rest, tk, "yes", 1)
        r2 = await bot.market_close(rest, tk)
        bot.DRY_RUN = prev
        ok = (r1 is None and r2 is None and tk not in bot.positions)
        record("DRY_RUN no-submit", PASS if ok else FAIL,
               "buy+close returned None, position cleared" if ok else "submitted something!")
    except Exception as exc:  # noqa: BLE001
        record("DRY_RUN no-submit", FAIL, str(exc))


async def _run_async(bot):
    rest = await check_kalshi_async(bot)
    if rest is not None:
        await check_orders_async(bot, rest)
        await rest.close()
    else:
        section("10-11. Orders")
        record("order checks", SKIP, "Kalshi REST unavailable", critical=False)


# ──────────────────────────────────────────────────────────────────────────────
def main():
    # verbose WS logging + dry-run must be set before importing the bot module
    os.environ.setdefault("KALSHI_WS_VERBOSE", "true")
    os.environ.setdefault("DRY_RUN", "true")

    print("=" * 70)
    print("  KALSHI BTC 15-MIN BOT — PRE-PRODUCTION QA (ASYNC)")
    print(f"  {datetime.now(tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}")
    print("  NOTE: no real orders are ever submitted by this suite.")
    print("=" * 70)

    check_secrets()
    check_pem()
    check_imports()

    bot = None
    try:
        import kalshibtc15minupordown as bot
        record("import bot module", PASS)
    except Exception as exc:  # noqa: BLE001
        section("Bot module import")
        record("import bot module", FAIL, str(exc))
        traceback.print_exc()

    if bot is not None:
        check_tickers(bot)
        check_alpaca_rest()
        check_alpaca_ws()
        try:
            asyncio.run(_run_async(bot))
        except Exception as exc:  # noqa: BLE001
            record("async checks", FAIL, str(exc))
            traceback.print_exc()

    print("\n" + "=" * 70)
    counts = {PASS: 0, FAIL: 0, WARN: 0, SKIP: 0}
    crit_fail = 0
    for _name, status, _detail, critical in RESULTS:
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
