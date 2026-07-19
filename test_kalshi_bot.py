"""
test_kalshi_bot.py
─────────────────────────────────────────────────────────────────────────────
Pre-production QA suite for the ASYNC Kalshi BTC 15-min PROPHET bot.

Runs end-to-end against LIVE credentials but NEVER submits a real order
(DRY_RUN is force-overridden to True inside the bot module, regardless of env).
Exits non-zero if any CRITICAL check fails.

Prints LIVE INFO with substantial logging:
  • latest 1-minute BTC-USD candles from Yahoo Finance + validation verdict
  • a real Prophet fit + settlement forecast with the 80% CI (p10/p50/p90)
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
  14 BTC/ETH hedge: target-price math + fill monitor + immediate reconciliation (DRY-RUN)
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

        class _Market:
            def __init__(self, status):
                self.status = status

        opening_gate_ok = (
            bot.is_market_live(_Market("active"))
            and not bot.is_market_live(_Market("initialized"))
            and bot.is_within_open_trade_grace(0.0)
            and bot.is_within_open_trade_grace(bot.OPEN_TRADE_GRACE_S)
            and not bot.is_within_open_trade_grace(bot.OPEN_TRADE_GRACE_S + 0.01)
            and bot.PREOPEN_FORECAST_LEAD_S == 120
            and bot.FORECAST_MINUTES == 17
        )
        record("entry gate requires active market + 2-min pre-open 17-step cache",
               PASS if opening_gate_ok else FAIL,
               f"active + first {bot.OPEN_TRADE_GRACE_S:.0f}s; "
               f"cache {bot.PREOPEN_FORECAST_LEAD_S:.0f}s / {bot.FORECAST_MINUTES} steps")
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
    section("7. Prophet settlement forecast (80% confidence interval)")
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
                  f"of the settlement forecast", flush=True)
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
        record("BET_AMOUNT_SHARES", PASS,
               f"{bot.BET_AMOUNT_SHARES:.2f} contracts/order (shares — NOT "
               f"dollars — fractional count_fp)")
        # Share sizing: buy exactly the share amount regardless of price,
        # floored to 0.01-contract granularity, clamped to the 0.01 minimum.
        import math as _m
        b = bot.BET_AMOUNT_SHARES
        base_ok = abs(bot.bet_count()
                      - max(0.01, _m.floor(b * 100 + 1e-6) / 100.0)) < 1e-9
        sizing_ok = (base_ok
                     and abs(bot.bet_count(0.056) - 0.05) < 1e-9  # floored to 0.01 steps
                     and abs(bot.bet_count(0.004) - 0.01) < 1e-9  # clamped to the minimum
                     and abs(bot.bet_count(0.01 * 2 ** 3) - 0.08) < 1e-9)  # generic share math
        record("bet_count (fixed share sizing, price-independent)",
               PASS if sizing_ok else FAIL,
               "base %.2f → %.2f · 0.056 → %.2f · 0.004 → %.2f · "
               "0.01×2³ → %.2f" % (b, bot.bet_count(), bot.bet_count(0.056),
                                   bot.bet_count(0.004),
                                   bot.bet_count(0.01 * 2 ** 3)))
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

        side_yes, decision_yes = bot.decide_side_from_forecast(
            100000.0, {"p50": 100010.0})
        side_no, decision_no = bot.decide_side_from_forecast(
            100000.0, {"p50": 99990.0})
        side_skip, decision_skip = bot.decide_side_from_forecast(
            100000.0, {"p50": 100000.0})
        side_ok = (side_yes == "yes" and decision_yes == "BUY YES"
                   and side_no == "no" and decision_no == "BUY NO"
                   and side_skip is None and decision_skip == "SKIP")
        record("side decision uses forecast p50 vs live strike",
               PASS if side_ok else FAIL,
               f"above→{decision_yes}, below→{decision_no}, equal→{decision_skip}")

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
                  and s["btc_total"] == 2 and s["btc_wins"] == 1
                  and s["btc_losses"] == 1 and s["btc_current_kind"] == "LOSS"
                  and s["btc_current_streak"] == 1
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


# 14 ────────────────────────────────────────────────────────────────────────────
def check_eth_hedge(bot):
    section("14. BTC/ETH hedge: price math + monitor trigger (DRY-RUN)")
    try:
        from kalshi_python_async import BookSide
        target = bot.eth_hedge_target_price(0.60)
        record("BTC $0.60 entry → ETH opposite target $0.30",
               PASS if abs(target - 0.30) < 1e-9 else FAIL,
               f"target={target}")

        side, book_side, api_price = bot.eth_hedge_order("yes", target)
        record("BTC YES → ETH NO ASK at YES price $0.70",
               PASS if (side, book_side, api_price) == ("no", BookSide.ASK, "0.70") else FAIL,
               f"side={side} book={book_side.value} api={api_price}")

        side2, book_side2, api_price2 = bot.eth_hedge_order("no", target)
        record("BTC NO → ETH YES BID at $0.30",
               PASS if (side2, book_side2, api_price2) == ("yes", BookSide.BID, "0.30") else FAIL,
               f"side={side2} book={book_side2.value} api={api_price2}")

        skip = bot.eth_hedge_target_price(0.90)
        record("hedge skips when BTC entry leaves no <=$0.90 pair room",
               PASS if skip is None else FAIL, f"target={skip}")

        def primary(ticker, result, *, arbitrage_active=False,
                    bet_multiplier=1.0, hedge_status=None, hedge_filled_count=0.0):
            hedge = (None if hedge_status is None else {
                "status": hedge_status, "recorded_fill_count": hedge_filled_count})
            return {
                "ticker": ticker,
                "trade_kind": "BTC_PRIMARY",
                "result": result,
                "arbitrage_active": arbitrage_active,
                "bet_multiplier": bet_multiplier,
                "eth_hedge": hedge,
            }

        def next_state_for(records):
            with tempfile.TemporaryDirectory() as tmp:
                tr = bot.PerformanceTracker(
                    os.path.join(tmp, "h.json"), os.path.join(tmp, "t.json"))
                tr.trades.extend(records)
                return tr.next_eth_hedge_state()

        first_loss = next_state_for([
            primary("KXBTC15M-QA-FIRST-LOSS", "LOSS"),
        ])
        unfilled_loss = next_state_for([
            primary("KXBTC15M-QA-UNFILLED", "LOSS", arbitrage_active=True,
                    bet_multiplier=1.0, hedge_status="expired"),
        ])
        partial_loss = next_state_for([
            primary("KXBTC15M-QA-PARTIAL", "LOSS", arbitrage_active=True,
                    bet_multiplier=1.0, hedge_status="partially_filled",
                    hedge_filled_count=1.0),
        ])
        filled_loss = next_state_for([
            primary("KXBTC15M-QA-FILLED", "LOSS", arbitrage_active=True,
                    bet_multiplier=bot.LOSS_MULTIPLIER, hedge_status="filled"),
        ])
        win_reset = next_state_for([
            primary("KXBTC15M-QA-WIN", "WIN", arbitrage_active=True,
                    bet_multiplier=bot.LOSS_MULTIPLIER, hedge_status="expired"),
        ])
        first_pair = bot.bet_count(bot.ARBITRAGE_SHARES * first_loss["multiplier"])
        multiplied_pair = bot.bet_count(
            bot.ARBITRAGE_SHARES * unfilled_loss["multiplier"])
        lifecycle_ok = (
            first_loss["active"]
            and first_loss["multiplier"] == 1.0
            and abs(first_pair - 10.0) < 1e-9
            and unfilled_loss["active"]
            and abs(unfilled_loss["multiplier"] - bot.LOSS_MULTIPLIER) < 1e-9
            and partial_loss["multiplier"] == 1.0
            and abs(multiplied_pair - 20.0) < 1e-9
            and filled_loss["active"]
            and filled_loss["multiplier"] == 1.0
            and not win_reset["active"]
            and win_reset["multiplier"] == 1.0
        )
        record("hedge sizing: only BTC loss + zero ETH fill → 20 + 20",
               PASS if lifecycle_ok else FAIL,
               "first pair=%.2f+%.2f, escalated pair=%.2f+%.2f; partial/full ETH fill resets to base"
               % (first_pair, first_pair, multiplied_pair, multiplied_pair))

        with tempfile.TemporaryDirectory() as tmp:
            from types import SimpleNamespace

            saved_tracker, saved_dry = bot.tracker, bot.DRY_RUN
            bot.tracker = bot.PerformanceTracker(
                os.path.join(tmp, "h.json"), os.path.join(tmp, "t.json"))
            bot.DRY_RUN = True
            rec = {"ticker": "KXBTC15M-QA-HEDGE", "side": "YES", "entry_price": 0.60,
                   "count": 10.0, "trade_kind": "BTC_PRIMARY",
                   "eth_hedge": {
                       "status": "pending_submission", "ticker": "KXETH15M-QA-HEDGE",
                       "side": "NO", "target_entry_price": 0.30,
                       "api_price": "0.70", "count": 10.0,
                   },
                   "exit_method": "pending", "result": "pending",
                   "settle_et": "2099-01-01T00:00:00+00:00",
                   "timestamp": "qa", "btc_entry": 0.0, "p50_prediction": 0.0}
            bot.tracker.trades.append(rec)
            asyncio.run(bot._submit_eth_hedge_limit(None, rec))
            hedge_records = [t for t in bot.tracker.trades
                             if t.get("trade_kind") == "ETH_HEDGE"]
            submitted = (rec["eth_hedge"]["status"] == "filled"
                         and rec["eth_hedge"]["time_in_force"] == "good_till_canceled"
                         and len(hedge_records) == 1
                         and hedge_records[0]["side"] == "NO"
                         and abs(hedge_records[0]["entry_price"] - 0.30) < 1e-9)

            bot.DRY_RUN = False
            monitor_rec = {"ticker": "KXBTC15M-QA-HEDGE-MONITOR", "side": "YES",
                           "entry_price": 0.60, "count": 10.0,
                           "trade_kind": "BTC_PRIMARY",
                           "eth_hedge": {
                               "status": "open", "order_id": "qa-order",
                               "ticker": "KXETH15M-QA-HEDGE-MONITOR", "side": "NO",
                               "target_entry_price": 0.30, "api_price": "0.70",
                               "count": 10.0, "recorded_fill_count": 0.0,
                           },
                           "exit_method": "pending", "result": "pending",
                           "settle_et": "2099-01-01T00:00:00+00:00",
                           "timestamp": "qa", "btc_entry": 0.0,
                           "p50_prediction": 0.0}

            class _FakeOrders:
                async def get_order(self, order_id):
                    assert order_id == "qa-order"
                    return SimpleNamespace(order=SimpleNamespace(fill_count_fp="10.00"))

            asyncio.run(bot._monitor_eth_hedge(SimpleNamespace(orders=_FakeOrders()), monitor_rec))
            monitored = (monitor_rec["eth_hedge"]["status"] == "filled"
                         and len([t for t in bot.tracker.trades
                                  if t.get("linked_btc_ticker") == monitor_rec["ticker"]]) == 1)
            s = bot.tracker.stats()
            leg_ok = s["kinds"]["ETH_HEDGE"] == 0   # hedge is pending until settlement
            bot.tracker, bot.DRY_RUN = saved_tracker, saved_dry
        record("ETH limit submits immediately after BTC fill (DRY-RUN)",
               PASS if submitted else FAIL,
               f"hedge_status={rec['eth_hedge']['status']}")
        record("ETH limit monitor records a later full fill",
               PASS if monitored else FAIL,
               f"hedge_status={monitor_rec['eth_hedge']['status']}")
        record("stats() keeps un-settled hedge out of settled leg counts",
               PASS if leg_ok else FAIL,
               f"kinds={s['kinds']}")
    except Exception as exc:  # noqa: BLE001
        record("ETH hedge checks", FAIL, str(exc))
        traceback.print_exc()


def check_deferred_loss_reconciliation(bot):
    """A late BTC loss must top an already-live 2-share entry up to 10+10."""
    section("15. Immediate BTC-loss reconciliation")
    try:
        from types import SimpleNamespace

        with tempfile.TemporaryDirectory() as tmp:
            saved_tracker, saved_dry = bot.tracker, bot.DRY_RUN
            bot.tracker = bot.PerformanceTracker(
                os.path.join(tmp, "h.json"), os.path.join(tmp, "t.json"))
            bot.DRY_RUN = True
            prior = {
                "ticker": "KXBTC15M-QA-PRIOR-LOSS",
                "timestamp": "qa",
                "settle_et": (datetime.now(tz=timezone.utc) - timedelta(seconds=1)).isoformat(),
                "side": "NO", "entry_price": 0.50, "count": 2.0,
                "trade_kind": "BTC_PRIMARY", "arbitrage_active": False,
                "eth_hedge": None, "loss_streak": 0,
                "result": "pending", "profit_loss": 0.0,
            }
            current = {
                "ticker": "KXBTC15M-QA-CURRENT", "timestamp": "qa",
                "settle_et": (datetime.now(tz=timezone.utc) + timedelta(minutes=10)).isoformat(),
                "side": "NO", "entry_price": 0.50, "count": 2.0,
                "bet_amount_shares": 2.0, "trade_kind": "BTC_PRIMARY",
                "arbitrage_active": False, "eth_hedge": None,
                "deferred_hedge": {
                    "status": "awaiting_btc_result",
                    "prior_btc_ticker": prior["ticker"], "base_count": 2.0,
                },
                "result": "pending", "profit_loss": 0.0,
                "btc_entry": 0.0, "p50_prediction": 0.0,
            }
            bot.tracker.record_open(prior)
            bot.tracker.record_open(current)

            class _ReconcileRest:
                async def get_market(self, ticker):
                    if ticker == prior["ticker"]:
                        return SimpleNamespace(result="yes")
                    assert ticker == current["ticker"]
                    return SimpleNamespace(status="active")

            settled = asyncio.run(bot._settle_record_if_ready(_ReconcileRest(), prior))
            eth_records = [t for t in bot.tracker.trades
                           if t.get("trade_kind") == "ETH_HEDGE"]
            reconciled = (settled and prior["result"] == "LOSS"
                          and current["count"] == 10.0
                          and current["arbitrage_active"]
                          and current["deferred_hedge"]["status"] == "reconciled"
                          and isinstance(current["eth_hedge"], dict)
                          and current["eth_hedge"]["count"] == 10.0
                          and len(eth_records) == 1
                          and eth_records[0]["count"] == 10.0)
            bot.tracker, bot.DRY_RUN = saved_tracker, saved_dry

        record("late BTC loss tops live base entry to 10 BTC + 10 ETH",
               PASS if reconciled else FAIL,
               f"prior={prior['result']} BTC={current['count']:.2f} "
               f"deferred={current['deferred_hedge']['status']}")
    except Exception as exc:  # noqa: BLE001
        record("immediate BTC-loss reconciliation", FAIL, str(exc))
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
        check_eth_hedge(bot)
        check_deferred_loss_reconciliation(bot)

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
