"""
test_kalshi_bot.py
─────────────────────────────────────────────────────────────────────────────
Pre-production QA suite for the ASYNC Kalshi BTC 15-min PROPHET bot.

Runs end-to-end against LIVE credentials but NEVER submits a real order
(DRY_RUN is force-overridden to True inside the bot module, regardless of env).
Exits non-zero if any CRITICAL check fails.

Prints LIVE INFO with substantial logging:
  • latest 1-minute BTC-USD candles from Yahoo Finance + validation verdict
  • a real Prophet fit + 15-minute forecast with the 80% CI (p10/p50/p90)
  • the current & next 15-min ET tickers + full live Kalshi market snapshot
  • a live sample of Kalshi market WebSocket messages

Checks
──────
  1  Secrets / env present (Kalshi only — Alpaca no longer used)
  2  RSA private key (PEM) loads as RSA
  3  Dependencies import (kalshi sdk, aiohttp, prophet, yfinance, pandas, numpy)
  4  Bot module imports; DRY_RUN force-override engaged
  5  Ticker build/parse (US Eastern) + current/next markets
  6  yfinance BTC-USD 1-minute history download + data validation
  7  Prophet forecast: 80% CI sane (p10 < p50 < p90, p50 near spot), timing
  8  Quantile interpolation (percentile_of_price)
  9  Kalshi REST auth + balance                       (async)
  10 Kalshi active market + strike + LIVE SNAPSHOT    (async)
  11 Kalshi market WebSocket live stream              (async, non-critical)
  12 Bet sizing + V2 MARKET order build + DRY-RUN submit (nothing sent)
  13 PerformanceTracker round-trip (record → dedupe → settle → stats → reload)
"""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
import time
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
    val = os.getenv("KALSHI_API_KEY_ID", "")
    record("env KALSHI_API_KEY_ID", PASS if val.strip() else FAIL,
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
        pem_path = os.getenv("KALSHI_PEM_PATH", "kalshi_private_key.pem")
        pem = (open(pem_path, "rb").read() if os.path.exists(pem_path)
               else os.getenv("KALSHI_PRIVATE_KEY", "").encode())
        key = load_pem_private_key(pem, password=None)
        record("PEM is RSA private key", PASS if isinstance(key, RSAPrivateKey) else FAIL,
               f"{key.key_size}-bit" if isinstance(key, RSAPrivateKey) else type(key).__name__)
    except Exception as exc:  # noqa: BLE001
        record("PEM is RSA private key", FAIL, str(exc))


# 3 ────────────────────────────────────────────────────────────────────────────
def check_imports():
    section("3. Dependency imports")
    for mod in ("kalshi_python_async", "aiohttp", "cryptography",
                "prophet", "yfinance", "pandas", "numpy"):
        try:
            __import__(mod)
            record(f"import {mod}", PASS)
        except Exception as exc:  # noqa: BLE001
            record(f"import {mod}", FAIL, str(exc))


# 5 ────────────────────────────────────────────────────────────────────────────
def check_tickers(bot):
    section("5. Ticker build/parse (US Eastern) + current/next markets")
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


# 6 ────────────────────────────────────────────────────────────────────────────
def check_btc_history(bot):
    """Download + validate real 1-minute BTC data. Returns df or None."""
    section("6. yfinance BTC-USD 1-minute history + validation")
    try:
        t0 = time.time()
        df = bot.fetch_btc_1m()
        dt = time.time() - t0
        if df is None or len(df) == 0:
            record("yfinance BTC-USD download", FAIL, "no data returned")
            return None
        age = (datetime.now(tz=timezone.utc) - df["ds"].iloc[-1]).total_seconds()
        print(f"      candles      : {len(df)}", flush=True)
        print(f"      range        : {df['ds'].iloc[0]} → {df['ds'].iloc[-1]}", flush=True)
        print(f"      latest close : ${float(df['close'].iloc[-1]):,.2f} "
              f"(age {age:.0f}s)", flush=True)
        record("yfinance BTC-USD download", PASS,
               f"{len(df)} 1-min candles in {dt:.1f}s")
        ok, reason = bot.validate_data(df)
        record("validate_data (rows/spacing/fresh/15-min boundaries)",
               PASS if ok else FAIL, reason)
        return df if ok else None
    except Exception as exc:  # noqa: BLE001
        record("BTC history", FAIL, str(exc))
        traceback.print_exc()
        return None


# 7 ────────────────────────────────────────────────────────────────────────────
def check_prophet(bot, df):
    """Fit Prophet on the real data. Returns the 80% CI bands or None."""
    section("7. Prophet 15-minute forecast (80% confidence interval)")
    if df is None:
        record("Prophet forecast", SKIP, "no validated BTC data")
        return None
    try:
        t0 = time.time()
        fc = bot.run_prophet_forecast(df)
        dt = time.time() - t0
        if fc is None:
            record("Prophet forecast", FAIL, "returned None")
            return None
        keys_ok = set(fc.keys()) == {"p10", "p50", "p90"}
        record("returns 80% CI only (p10/p50/p90)", PASS if keys_ok else FAIL,
               f"keys={sorted(fc.keys())}")
        print(f"      P10 : ${fc['p10']:,.2f}", flush=True)
        print(f"      P50 : ${fc['p50']:,.2f}", flush=True)
        print(f"      P90 : ${fc['p90']:,.2f}", flush=True)
        order_ok = fc["p10"] < fc["p50"] < fc["p90"]
        record("band ordering p10 < p50 < p90", PASS if order_ok else FAIL,
               f"CI width ${fc['p90'] - fc['p10']:,.2f}")
        spot = float(df["close"].iloc[-1])
        drift = abs(fc["p50"] - spot) / spot
        record("p50 sanity (within 5% of spot)", PASS if drift < 0.05 else FAIL,
               f"p50 ${fc['p50']:,.2f} vs spot ${spot:,.2f} ({drift:.2%})")
        record("fit+predict time", PASS if dt < 120 else WARN, f"{dt:.1f}s",
               critical=False)
        return fc if (keys_ok and order_ok) else None
    except Exception as exc:  # noqa: BLE001
        record("Prophet forecast", FAIL, str(exc))
        traceback.print_exc()
        return None


# 8 ────────────────────────────────────────────────────────────────────────────
def check_quantile(bot, df, bands):
    section("8. Quantile interpolation (percentile_of_price)")
    try:
        b = bands or {"p10": 100.0, "p50": 110.0, "p90": 120.0}
        q_lo  = bot.percentile_of_price(b["p10"] - 1, b)
        q_mid = bot.percentile_of_price(b["p50"], b)
        q_hi  = bot.percentile_of_price(b["p90"] + 1, b)
        ok = (q_lo == 10.0 and abs(q_mid - 50.0) < 1e-9 and q_hi == 90.0)
        record("clamp + interp at p10/p50/p90", PASS if ok else FAIL,
               f"below→{q_lo:.0f} · at p50→{q_mid:.0f} · above→{q_hi:.0f}")
        mid = (b["p50"] + b["p90"]) / 2
        q = bot.percentile_of_price(mid, b)
        record("monotonic between bands", PASS if 50.0 < q < 90.0 else FAIL,
               f"halfway p50→p90 lands at {q:.1f} pct")
        if df is not None and bands is not None:
            spot = float(df["close"].iloc[-1])
            print(f"      LIVE: BTC ${spot:,.2f} is at the "
                  f"{bot.percentile_of_price(spot, bands):.0f}th percentile "
                  f"of the 15-min forecast", flush=True)
    except Exception as exc:  # noqa: BLE001
        record("quantile interpolation", FAIL, str(exc))


# 9-12 (async) ──────────────────────────────────────────────────────────────────
def _print_market_snapshot(market: dict):
    raw = market["raw_market"]
    tgt_s = "${:,.2f}".format(market["target"]) if market.get("target") is not None else "n/a"
    print("      ┌─ LIVE KALSHI BTC 15-MIN MARKET ──────────────────────", flush=True)
    print(f"      │ ticker         : {market['ticker']}", flush=True)
    print(f"      │ strike (target): {tgt_s}", flush=True)
    if market.get("settle_et"):
        print(f"      │ settle (ET)    : {market['settle_et'].strftime('%Y-%m-%dT%H:%M %Z')}",
              flush=True)
    print(f"      │ next ticker    : {market['next_ticker']}", flush=True)
    for f in ("status", "yes_sub_title", "no_sub_title", "yes_bid_dollars",
              "yes_ask_dollars", "no_bid_dollars", "no_ask_dollars",
              "last_price_dollars", "floor_strike", "volume", "open_interest"):
        v = getattr(raw, f, None)
        if v is not None:
            print(f"      │ {f:<18}: {v}", flush=True)
    print("      └───────────────────────────────────────────────────────", flush=True)


async def check_kalshi_async(bot):
    section("9. Kalshi REST auth + balance (async)")
    rest = None
    try:
        rest = bot.KalshiREST()
        bal = await rest.get_balance_dollars()
        record("Kalshi balance", PASS if (bal is not None and bal >= 0) else FAIL,
               f"${bal:,.2f}" if bal is not None else "None")
        bot.start_balance = bal if bal is not None else 0.0
        await bot.report_portfolio(rest)   # same report production prints
        record("portfolio + performance report", PASS, "printed above")
    except Exception as exc:  # noqa: BLE001
        record("Kalshi REST auth", FAIL, str(exc))
        traceback.print_exc()
        return None

    section("10. Kalshi active market + strike + LIVE SNAPSHOT (async)")
    try:
        market = None
        for _ in range(3):
            market = await bot.resolve_active_market(rest)
            if market and market.get("target") is not None:
                break
            await asyncio.sleep(2)
        if market:
            _print_market_snapshot(market)
            tgt = market.get("target")
            record("resolve_active_market", PASS, market["ticker"])
            record("strike (floor_strike)", PASS if tgt is not None else WARN,
                   f"${tgt:,.2f}" if tgt is not None else
                   "missing — fresh windows can lag a few seconds",
                   critical=False)
        else:
            record("resolve_active_market", FAIL, "no open KXBTC15M market found")
    except Exception as exc:  # noqa: BLE001
        record("resolve_active_market", FAIL, str(exc))

    section("11. Kalshi market WebSocket LIVE STREAM (async, ~15s)")
    try:
        ws = bot.KalshiMarketWS(rest.auth)
        ws.set_tickers(bot.current_and_next_tickers())
        task = asyncio.create_task(ws.run())
        t0 = time.time()
        while time.time() - t0 < 15 and ws.msg_count == 0:
            await asyncio.sleep(0.5)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        if ws.connected or ws.msg_count > 0:
            record("Kalshi WS live stream", PASS,
                   f"connected={ws.connected}, {ws.msg_count} msg(s), "
                   f"{len(bot.kalshi_quotes)} quote(s)", critical=False)
            for tk, q in list(bot.kalshi_quotes.items())[:2]:
                print(f"      WS quote {tk}: {q}", flush=True)
        else:
            record("Kalshi WS live stream", WARN,
                   "no connection/messages in 15s — bot trades via REST regardless",
                   critical=False)
    except Exception as exc:  # noqa: BLE001
        record("Kalshi WS live stream", WARN, str(exc), critical=False)
    return rest


async def check_orders_async(bot, rest):
    section("12. Bet sizing + V2 order build + DRY-RUN submit (nothing sent)")
    try:
        from kalshi_python_async import (CreateOrderV2Request, BookSide,
                                         SelfTradePreventionType)
        import uuid
        record("BET_AMOUNT_USD", PASS,
               f"${bot.BET_AMOUNT_USD:.2f} → {bot.bet_count()} contract(s)")
        cases = [("BUY YES", BookSide.BID, bot.YES_BUY_PRICE),
                 ("BUY NO",  BookSide.ASK, bot.NO_BUY_PRICE)]
        for label, side, price in cases:
            req = CreateOrderV2Request(
                ticker="KXBTC15M-26JUN271145-45", side=side,
                count=f"{float(bot.bet_count()):.2f}", price=price,
                time_in_force="immediate_or_cancel", client_order_id=str(uuid.uuid4()),
                self_trade_prevention_type=SelfTradePreventionType.TAKER_AT_CROSS,
                reduce_only=False)
            ok = (req.side == side and req.price == price)
            record(f"build {label}", PASS if ok else FAIL,
                   f"side={req.side.value} price={req.price} count={req.count}")

        # DRY-RUN submit through the bot's real order path — must not hit the API.
        class _FakeOrders:
            async def create_order_v2(self, **k):
                raise AssertionError("DRY_RUN must not submit")
        real_orders = rest.orders
        rest.orders = _FakeOrders()
        assert bot.DRY_RUN is True, "DRY_RUN override lost!"
        ct, _ = bot.current_and_next_tickers()
        _, filled_yes = await bot._submit(rest, ticker=ct, side=BookSide.BID,
                                          price=bot.YES_BUY_PRICE, count=1,
                                          reduce_only=False, tag="QA DRY-RUN BUY YES")
        _, filled_no = await bot._submit(rest, ticker=ct, side=BookSide.ASK,
                                         price=bot.NO_BUY_PRICE, count=1,
                                         reduce_only=False, tag="QA DRY-RUN BUY NO")
        rest.orders = real_orders
        record("DRY-RUN submit (YES & NO paths)",
               PASS if (filled_yes and filled_no) else FAIL,
               "simulated fills; zero orders reached the exchange")
    except Exception as exc:  # noqa: BLE001
        record("order build / DRY-RUN submit", FAIL, str(exc))
        traceback.print_exc()


# 13 ────────────────────────────────────────────────────────────────────────────
def check_tracker(bot):
    section("13. PerformanceTracker round-trip (temp files)")
    try:
        with tempfile.TemporaryDirectory() as tmp:
            hp, tp = os.path.join(tmp, "hist.json"), os.path.join(tmp, "traded.json")
            tr = bot.PerformanceTracker(hp, tp)
            rec = {"ticker": "KXBTC15M-QA-TEST1", "timestamp": "2026-01-01T00:00:00Z",
                   "settle_et": "", "side": "YES", "entry_price": 0.40,
                   "btc_entry": 100000.0, "strike": 99990.0,
                   "p50_prediction": 100050.0, "btc_quantile_position": 42.0,
                   "count": 1, "result": "pending", "profit_loss": 0.0}
            tr.record_open(rec)
            record("record_open + one-order-per-window dedupe",
                   PASS if tr.already_traded("KXBTC15M-QA-TEST1") else FAIL)
            record("pending trade discoverable",
                   PASS if len(tr.find_pending()) == 1 else FAIL)
            tr.settle(rec, "WIN", (1.0 - 0.40) * 1)          # +0.60
            rec2 = dict(rec, ticker="KXBTC15M-QA-TEST2", entry_price=0.55,
                        result="pending")
            tr.record_open(rec2)
            tr.settle(rec2, "LOSS", -0.55)                    # -0.55
            s = tr.stats()
            ok = (s["total"] == 2 and s["wins"] == 1 and s["losses"] == 1
                  and abs(s["total_return"] - 0.05) < 1e-9
                  and s["current_kind"] == "LOSS" and s["current_streak"] == 1
                  and s["longest_win"] == 1 and s["longest_loss"] == 1
                  and abs(s["max_drawdown"] - 0.55) < 1e-9)
            record("stats: win-rate/streaks/equity/max-drawdown",
                   PASS if ok else FAIL,
                   f"total={s['total']} wr={s['win_rate']:.0f}% "
                   f"ret=${s['total_return']:+.2f} maxDD=${s['max_drawdown']:.2f}")
            tr2 = bot.PerformanceTracker(hp, tp)              # restart simulation
            record("state survives reload (restart-safe)",
                   PASS if (len(tr2.trades) == 2
                            and tr2.already_traded("KXBTC15M-QA-TEST1")) else FAIL,
                   f"{len(tr2.trades)} trades reloaded from disk")
    except Exception as exc:  # noqa: BLE001
        record("PerformanceTracker", FAIL, str(exc))
        traceback.print_exc()


async def _run_async(bot):
    rest = await check_kalshi_async(bot)
    if rest is not None:
        await check_orders_async(bot, rest)
        await rest.close()
    else:
        section("12. Orders")
        record("order checks", SKIP, "Kalshi REST unavailable", critical=False)


# ──────────────────────────────────────────────────────────────────────────────
def main():
    os.environ["DRY_RUN"] = "true"          # belt: module reads env at import
    os.environ.setdefault("KALSHI_WS_VERBOSE", "true")

    print("=" * 70)
    print("  KALSHI BTC 15-MIN PROPHET BOT — PRE-PRODUCTION QA (ASYNC)")
    print(f"  {datetime.now(tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}")
    print("  NOTE: no real orders are ever submitted by this suite.")
    print("=" * 70)

    check_secrets()
    check_pem()
    check_imports()

    section("4. Bot module import + DRY_RUN override")
    bot = None
    try:
        import kalshibtc15minupordown as bot
        bot.DRY_RUN = True                  # braces: _submit() reads this at call time
        record("import bot module", PASS)
        record("DRY_RUN force-override", PASS if bot.DRY_RUN else FAIL,
               "bot.DRY_RUN=True — no order can reach the exchange")
    except Exception as exc:  # noqa: BLE001
        record("import bot module", FAIL, str(exc))
        traceback.print_exc()

    if bot is not None:
        check_tickers(bot)
        df = check_btc_history(bot)
        bands = check_prophet(bot, df)
        check_quantile(bot, df, bands)
        try:
            asyncio.run(_run_async(bot))
        except Exception as exc:  # noqa: BLE001
            record("async checks", FAIL, str(exc))
            traceback.print_exc()
        check_tracker(bot)

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
        print("  RESULT : ✓ all critical checks passed — safe to launch")
        if counts[WARN]:
            print("           (review warnings above)")
    print("=" * 70)
    sys.exit(1 if crit_fail else 0)


if __name__ == "__main__":
    main()
