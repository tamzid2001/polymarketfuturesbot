"""
test_kalshi_bot.py
─────────────────────────────────────────────────────────────────────────────
Pre-production QA suite for the ASYNC Kalshi BTC 15-min bot.

Runs end-to-end against LIVE credentials but NEVER submits a real order.
Exits non-zero if any CRITICAL check fails.

Prints LIVE INFO with substantial logging:
  • live BTC/USD price (Alpaca REST + WS tick)
  • the rolling 60-second NOW price, printed EVERY SECOND
  • the current & next 15-min ET tickers + full live Kalshi market snapshot
  • a live verbose stream of Kalshi market WebSocket messages

Checks
──────
  1  Secrets / env present
  2  RSA private key (PEM) loads as RSA
  3  Dependencies import
  4  Ticker build/parse (US Eastern) + current/next markets
  5  Alpaca REST auth + live BTC/USD price
  6  Alpaca real-time trade WebSocket tick
  7  Rolling 60-second NOW price printed every second
  8  Kalshi REST auth + balance                (async)
  9  Kalshi active market + target + LIVE SNAPSHOT  (async)
  10 Kalshi market WebSocket live stream (verbose) (async)
  11 Bet sizing + V2 MARKET order build (BUY/SELL/CLOSE, no submit)
  12 DRY_RUN open/close submit nothing + one-position rule
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
import threading
import traceback
from datetime import datetime, timedelta, timezone

RESULTS: list = []
PASS, FAIL, WARN, SKIP = "PASS", "FAIL", "WARN", "SKIP"


def record(name, status, detail="", critical=True):
    RESULTS.append((name, status, detail, critical))
    icon = {"PASS": "✓", "FAIL": "✗", "WARN": "!", "SKIP": "·"}[status]
    print(f"  [{icon}] {name}: {status}" + (f" — {detail}" if detail else ""), flush=True)


def section(title):
    print(f"\n=== {title} ===", flush=True)


# 1 ────────────────────────────────────────────────────────────────────────────
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


# 2 ────────────────────────────────────────────────────────────────────────────
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


# 3 ────────────────────────────────────────────────────────────────────────────
def check_imports():
    section("3. Dependency imports")
    for mod in ("alpaca.data.live", "kalshi_python_async", "aiohttp", "cryptography"):
        try:
            __import__(mod)
            record(f"import {mod}", PASS)
        except Exception as exc:  # noqa: BLE001
            record(f"import {mod}", FAIL, str(exc))


# 4 ────────────────────────────────────────────────────────────────────────────
def check_tickers(bot):
    section("4. Ticker build/parse (US Eastern) + current/next markets")
    try:
        dt_et = datetime(2026, 6, 27, 11, 45, tzinfo=bot.ET)
        t = bot.build_ticker("KXBTC15M", dt_et)
        record("build_ticker (ET)", PASS if t == "KXBTC15M-26JUN271145-45" else FAIL, t)
        p = bot.parse_ticker(t)
        record("parse_ticker round-trip",
               PASS if (p and p["settle_et"] == dt_et) else FAIL, str(p))
        now_et = datetime.now(tz=bot.ET)
        slot = (now_et.minute // 15) * 15
        exp = bot.build_ticker("KXBTC15M",
                               now_et.replace(minute=slot, second=0, microsecond=0)
                               + timedelta(minutes=15))
        ct, nt = bot.current_and_next_tickers()
        print(f"      CURRENT open market (ET {now_et.strftime('%H:%M')}): {ct}", flush=True)
        print(f"      NEXT market                       : {nt}", flush=True)
        record("current ticker matches ET clock",
               PASS if ct == exp else FAIL, f"{ct} (expected {exp})")
    except Exception as exc:  # noqa: BLE001
        record("ticker helpers", FAIL, str(exc))


# 5 ────────────────────────────────────────────────────────────────────────────
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


# 6 ────────────────────────────────────────────────────────────────────────────
def check_alpaca_ws(bot):
    """Start the bot's Alpaca WS (feeds price state); fall back to REST if quiet."""
    section("6. Alpaca price feed (WebSocket + REST fallback)")
    try:
        from alpaca.data.live import CryptoDataStream
        stream = CryptoDataStream(os.getenv("ALPACA_API_KEY"), os.getenv("ALPACA_API_SECRET"))
        stream.subscribe_trades(bot.on_trade, "BTC/USD")   # populates bot price state
        threading.Thread(target=stream.run, daemon=True).start()
        deadline = time.time() + 12
        while time.time() < deadline:
            p, _, src = bot.read_btc_full()
            if p > 0 and src == "WS":
                print(f"      LIVE BTC/USD (WS tick): ${p:,.2f}", flush=True)
                record("Alpaca price feed (WS)", PASS, f"${p:,.2f}")
                return
            time.sleep(0.5)
        # WS quiet → REST fallback is the DESIGNED behavior, not a failure
        rp = bot.fetch_btc_spot_rest()
        if rp and rp > 0:
            bot._set_price(rp, "REST")
            print(f"      WS quiet — REST fallback BTC/USD: ${rp:,.2f}", flush=True)
            record("Alpaca price feed (REST fallback)", PASS, f"${rp:,.2f}")
        else:
            record("Alpaca price feed", FAIL, "neither WS nor REST returned a price")
    except Exception as exc:  # noqa: BLE001
        record("Alpaca price feed", FAIL, str(exc))


# 7 (async) ─────────────────────────────────────────────────────────────────────
async def check_now_price(bot):
    section("7. Per-second BTC spot (REST) + rolling 60s NOW price (every second)")
    try:
        bot.PRINT_SPOT = True
        bot.PRINT_NOW_PRICE = True
        # demo the target-anchor: seed with a sample target, then blend live ticks
        live, _ = bot.read_btc_price()
        if live > 0:
            bot.anchor_now_price(round(live, 2))
        task = asyncio.create_task(bot.btc_second_loop())   # REST every second + NOW avg
        await asyncio.sleep(22)               # ~22 one-second prints
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        if bot.now_price > 0 and len(bot.price_samples) >= bot.MIN_SAMPLES:
            record("per-second spot + rolling NOW price", PASS,
                   f"now=${bot.now_price:,.2f} over {len(bot.price_samples)} samples")
        else:
            record("per-second spot + rolling NOW price", FAIL,
                   "no price from WS or REST")
    except Exception as exc:  # noqa: BLE001
        record("per-second spot + rolling NOW price", FAIL, str(exc))


# 8-10 (async) ──────────────────────────────────────────────────────────────────
def _print_market_snapshot(market: dict):
    raw = market["raw_market"]
    tgt_s = "${:,.2f}".format(market["target"]) if market.get("target") is not None else "n/a"
    print("      ┌─ LIVE KALSHI BTC 15-MIN MARKET ──────────────────────", flush=True)
    print(f"      │ ticker        : {market['ticker']}", flush=True)
    print(f"      │ target (strike): {tgt_s}", flush=True)
    if market.get("settle_et"):
        print(f"      │ settle (ET)   : {market['settle_et'].strftime('%Y-%m-%dT%H:%M %Z')}",
              flush=True)
    print(f"      │ next ticker   : {market['next_ticker']}", flush=True)
    for f in ("status", "yes_sub_title", "no_sub_title", "yes_bid_dollars",
              "yes_ask_dollars", "no_bid_dollars", "no_ask_dollars",
              "last_price_dollars", "floor_strike", "volume", "open_interest"):
        v = getattr(raw, f, None)
        if v is not None:
            print(f"      │ {f:<18}: {v}", flush=True)
    print("      └───────────────────────────────────────────────────────", flush=True)


async def check_kalshi_async(bot):
    section("8. Kalshi REST auth + balance (async)")
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

    section("9. Kalshi active market + target + LIVE SNAPSHOT (async)")
    try:
        market = await bot.resolve_active_market(rest)
        if market:
            _print_market_snapshot(market)
            tgt_s = ("${:,.2f}".format(market["target"])
                     if market.get("target") is not None else "n/a")
            record("resolve_active_market", PASS, f"{market['ticker']} target={tgt_s}")
        else:
            record("resolve_active_market", WARN, "no open market yet", critical=False)
    except Exception as exc:  # noqa: BLE001
        record("resolve_active_market", FAIL, str(exc))

    section("10. Kalshi market WebSocket LIVE STREAM (verbose, async)")
    try:
        ws = bot.KalshiMarketWS(rest.auth)
        ws.set_tickers(bot.current_and_next_tickers())
        task = asyncio.create_task(ws.run())
        await asyncio.sleep(12)
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
            record("Kalshi WS live stream", FAIL, "no connection / messages (auth/url?)")
    except Exception as exc:  # noqa: BLE001
        record("Kalshi WS live stream", FAIL, str(exc))
    return rest


# 11-12 ─────────────────────────────────────────────────────────────────────────
async def check_orders_async(bot, rest):
    section("11. Bet sizing + V2 MARKET order build (no submit)")
    try:
        from kalshi_python_async import (CreateOrderV2Request, BookSide,
                                         SelfTradePreventionType)
        import uuid
        record("BET_AMOUNT_USD", PASS, f"${bot.BET_AMOUNT_USD:.2f} → {bot.bet_count()} contract(s)")
        cases = [("BUY YES", BookSide.BID, "0.99", False),
                 ("BUY NO",  BookSide.ASK, "0.01", False),
                 ("CLOSE YES", BookSide.ASK, "0.01", True),
                 ("CLOSE NO",  BookSide.BID, "0.99", True)]
        for label, side, price, ro in cases:
            req = CreateOrderV2Request(
                ticker="KXBTC15M-26JUN271145-45", side=side,
                count=f"{float(bot.bet_count()):.2f}", price=price,
                time_in_force="immediate_or_cancel", client_order_id=str(uuid.uuid4()),
                self_trade_prevention_type=SelfTradePreventionType.TAKER_AT_CROSS,
                reduce_only=ro)
            ok = (req.side == side and req.price == price and req.reduce_only == ro)
            record(f"build {label}", PASS if ok else FAIL,
                   f"side={req.side.value} price={req.price} count={req.count} reduce_only={req.reduce_only}")
    except Exception as exc:  # noqa: BLE001
        record("V2 order build", FAIL, str(exc))

    section("12. DRY_RUN open/close + one-position rule (no submit)")
    try:
        class _FakeOrders:
            async def create_order_v2(self, **k):
                raise AssertionError("DRY_RUN must not submit")
        prev = bot.DRY_RUN
        bot.DRY_RUN = True
        rest.orders = _FakeOrders()
        bot.open_position = None
        tk = bot.current_and_next_tickers()[0]
        await bot.open_market(rest, tk, "yes")
        s1 = bot.open_position and bot.open_position["side"] == "yes"
        # flip → should close YES and open NO; still exactly one position
        await bot.open_market(rest, tk, "no")
        s2 = bot.open_position and bot.open_position["side"] == "no"
        await bot.close_position(rest)
        s3 = bot.open_position is None
        bot.DRY_RUN = prev
        record("DRY_RUN open/close + flip", PASS if (s1 and s2 and s3) else FAIL,
               "open YES → flip NO → close, one position throughout"
               if (s1 and s2 and s3) else "state mismatch")
    except Exception as exc:  # noqa: BLE001
        record("DRY_RUN open/close", FAIL, str(exc))


async def _run_async(bot):
    await check_now_price(bot)
    rest = await check_kalshi_async(bot)
    if rest is not None:
        await check_orders_async(bot, rest)
        await rest.close()
    else:
        section("11-12. Orders")
        record("order checks", SKIP, "Kalshi REST unavailable", critical=False)


# ──────────────────────────────────────────────────────────────────────────────
def main():
    os.environ.setdefault("KALSHI_WS_VERBOSE", "true")
    os.environ.setdefault("PRINT_NOW_PRICE", "true")
    os.environ.setdefault("PRINT_SPOT", "true")
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
        check_alpaca_ws(bot)
        try:
            asyncio.run(_run_async(bot))
        except Exception as exc:  # noqa: BLE001
            record("async checks", FAIL, str(exc))
            traceback.print_exc()

    print("\n" + "=" * 70)
    counts = {PASS: 0, FAIL: 0, WARN: 0, SKIP: 0}
    crit_fail = 0
    for _n, status, _d, critical in RESULTS:
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
