"""
Polymarket US Futures — automated scalping bot.

Runs as a continuous loop (every 15 min) for up to 5h 45m, then
self-triggers the next GitHub Actions workflow run so the chain never
breaks. Telegram notifications are fully optional.
"""

import math
import os
import sys
import time
import json
from datetime import datetime, date
import zoneinfo
import requests
from polymarket_us import PolymarketUS, APIError

# ==============================================================
# CONFIGURATION  — change these to tune strategy behaviour
# ==============================================================
TAKE_PROFIT_MULTIPLIER  = 2.0   # close when bid >= N × avg_entry
BUYBACK_AMOUNT_USD      = 1.00  # USD allocated per re-entry order
BUYBACK_STD_DEVS        = 1     # std-dev window for buyback trigger
BUYBACK_STD_DEV_PCT     = 0.10  # one std dev = this % of avg_entry
LOOP_INTERVAL_SECONDS   = 900   # 15 minutes between health checks
RUNTIME_LIMIT_SECONDS   = 20700 # 5 h 45 min — exit before 6 h GH limit
# ==============================================================

STATE_FILE       = "state.json"
TELEGRAM_CHAT_ID = "@moneyballpredictions"
EST              = zoneinfo.ZoneInfo("America/New_York")

EMPTY_STATE = {
    "pending_buybacks":   [],
    "balance_snapshots":  {},
    "today_closed":       [],
    "daily_report_sent":  "",
}


# ------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------

def log(msg: str) -> None:
    ts = datetime.now(EST).strftime("%Y-%m-%d %H:%M:%S EST")
    print(f"[{ts}] {msg}", flush=True)


# ------------------------------------------------------------------
# Optional Telegram Notifier
# ------------------------------------------------------------------

class Notifier:
    """
    Wraps Telegram messaging. Silently no-ops when credentials are absent.
    Enabled automatically when TELEGRAM_KEY env-var is present.
    """

    def __init__(self, token: str | None, chat_id: str) -> None:
        self.token   = token
        self.chat_id = chat_id
        self.enabled = bool(token)

    def send(self, text: str) -> None:
        if not self.enabled:
            return
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        try:
            resp = requests.post(
                url,
                json={"chat_id": self.chat_id, "text": text, "parse_mode": "Markdown"},
                timeout=10,
            )
            resp.raise_for_status()
            log(f"INFO  Telegram sent (msg_id={resp.json()['result']['message_id']})")
        except Exception as exc:
            log(f"WARN  Telegram send failed (non-fatal): {exc}")


# ------------------------------------------------------------------
# State persistence
# ------------------------------------------------------------------

def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as fh:
                data = json.load(fh)
            # Back-fill any missing keys from older state files
            for k, v in EMPTY_STATE.items():
                data.setdefault(k, type(v)())
            log("INFO  State loaded from disk.")
            return data
        except Exception as exc:
            log(f"WARN  State file unreadable ({exc}) — starting fresh.")
    return {k: type(v)() for k, v in EMPTY_STATE.items()}


def save_state(state: dict) -> None:
    with open(STATE_FILE, "w") as fh:
        json.dump(state, fh, indent=4)
    log("INFO  State persisted to disk.")


# ------------------------------------------------------------------
# Polymarket API helpers
# ------------------------------------------------------------------

def fetch_balance(client: PolymarketUS) -> dict:
    """Return first UserBalance dict or {} on failure."""
    try:
        resp     = client.account.balances()
        balances = resp.get("balances", []) if isinstance(resp, dict) else []
        if not balances:
            log("WARN  account.balances() returned empty list.")
            return {}
        b = balances[0]
        log(
            f"INFO  Balance — cash=${b.get('currentBalance', 0):.2f}  "
            f"buyingPower=${b.get('buyingPower', 0):.2f}  "
            f"notional=${b.get('assetNotional', 0):.2f}  "
            f"openOrders=${b.get('openOrders', 0):.2f}"
        )
        return b
    except APIError as exc:
        log(f"ERROR fetch_balance API error {exc.status_code}: {exc.message}")
        return {}
    except Exception as exc:
        log(f"ERROR fetch_balance unexpected: {exc}")
        return {}


def fetch_bbo(client: PolymarketUS, slug: str) -> tuple[float | None, float | None]:
    """Return (best_bid, best_ask) floats or (None, None) on failure."""
    try:
        bbo = client.markets.bbo(slug)
        if not isinstance(bbo, dict):
            log(f"WARN  BBO for {slug} — unexpected type {type(bbo)}")
            return None, None
        bid = float((bbo.get("bestBid") or {}).get("value", 0) or 0)
        ask = float((bbo.get("bestAsk") or {}).get("value", 0) or 0)
        log(
            f"INFO  BBO {slug} — "
            f"bid=${bid:.4f} (depth={bbo.get('bidDepth', '?')})  "
            f"ask=${ask:.4f} (depth={bbo.get('askDepth', '?')})  "
            f"lastTrade=${float((bbo.get('lastTradePx') or {}).get('value', 0) or 0):.4f}"
        )
        return bid, ask
    except APIError as exc:
        log(f"ERROR BBO {slug} API error {exc.status_code}: {exc.message}")
        return None, None
    except Exception as exc:
        log(f"ERROR BBO {slug} unexpected: {exc}")
        return None, None


# ------------------------------------------------------------------
# Balance snapshotting (daily, for 10 AM report)
# ------------------------------------------------------------------

def snapshot_balance_today(state: dict, balance: dict) -> None:
    today = date.today().isoformat()
    snaps = state.setdefault("balance_snapshots", {})
    if balance and today not in snaps:
        snaps[today] = {
            "currentBalance": balance.get("currentBalance", 0),
            "buyingPower":    balance.get("buyingPower",    0),
            "assetNotional":  balance.get("assetNotional",  0),
        }
        log(f"INFO  Balance snapshot saved for {today}.")
    # Rolling 7-day window
    while len(snaps) > 7:
        del snaps[sorted(snaps)[0]]


# ------------------------------------------------------------------
# Daily 10 AM report
# ------------------------------------------------------------------

def send_daily_report(notifier: Notifier, state: dict) -> None:
    today = date.today().isoformat()
    if state.get("daily_report_sent") == today:
        log("INFO  Daily report already sent today — skipping.")
        return

    snaps      = state.get("balance_snapshots", {})
    days       = sorted(snaps)
    today_snap = snaps.get(today, {})
    yest_snap  = snaps.get(days[-2], {}) if len(days) >= 2 and days[-1] == today else {}

    today_bal = today_snap.get("currentBalance", 0)
    yest_bal  = yest_snap.get("currentBalance",  0)
    delta     = today_bal - yest_bal
    delta_pct = (delta / yest_bal * 100) if yest_bal else 0
    arrow     = "📈" if delta >= 0 else "📉"

    closed       = state.get("today_closed", [])
    total_profit = sum(p.get("profit", 0) for p in closed)

    lines = [f"🌅 *Polymarket Daily Report — {today}*\n",
             "💰 *Balance*",
             f"  Yesterday: `${yest_bal:.2f}`" if yest_bal else "  Yesterday: _no snapshot_",
             f"  Today:     `${today_bal:.2f}`",
             f"  Change:    {arrow} `${delta:+.2f}` ({delta_pct:+.1f}%)\n"]

    if closed:
        lines.append(f"✅ *Closed Positions ({len(closed)} trades | total P&L: `${total_profit:+.2f}`)*")
        for p in closed:
            e = "🟢" if p.get("profit", 0) >= 0 else "🔴"
            lines.append(
                f"{e} `{p.get('slug', '?')}` — {p.get('outcome', '')}\n"
                f"   Qty {p.get('qty')} | Entry `${p.get('avg_price', 0):.4f}` "
                f"→ Exit `${p.get('exit_price', 0):.4f}` | P&L `${p.get('profit', 0):+.2f}`"
            )
    else:
        lines.append("_No positions closed today._")

    if notifier.enabled:
        notifier.send("\n".join(lines))
    else:
        log("INFO  Daily report (Telegram disabled — printing to log only):")
        for line in lines:
            log(f"       {line}")

    state["daily_report_sent"] = today
    state["today_closed"]      = []
    log("INFO  Daily report complete; today_closed list reset.")


# ------------------------------------------------------------------
# Health check (every loop iteration)
# ------------------------------------------------------------------

def run_health_check(
    client:    PolymarketUS,
    notifier:  Notifier,
    state:     dict,
    elapsed_s: float,
    loop_num:  int,
) -> None:
    remaining_min = (RUNTIME_LIMIT_SECONDS - elapsed_s) / 60
    log("")
    log("=" * 64)
    log(f"HEALTH CHECK #{loop_num}  |  elapsed={elapsed_s/60:.1f} min  |  remaining={remaining_min:.1f} min")
    log(f"  Telegram notifications : {'ENABLED' if notifier.enabled else 'DISABLED (TELEGRAM_KEY not set)'}")
    log(f"  Take-profit threshold  : {TAKE_PROFIT_MULTIPLIER}× avg entry")
    log(f"  Buyback window         : avg_entry ± {BUYBACK_STD_DEVS} std dev  ({BUYBACK_STD_DEV_PCT*100:.0f}% per std dev)")
    log(f"  Buyback amount         : ${BUYBACK_AMOUNT_USD:.2f} USD per re-entry")
    pending = [b for b in state.get("pending_buybacks", []) if not b.get("processed")]
    log(f"  Pending buybacks       : {len(pending)}")
    log(f"  Today closed           : {len(state.get('today_closed', []))} position(s)")
    log(f"  Runtime rollover at    : 5 h 45 min")

    # Verify API reachability
    try:
        client.account.balances()
        log("  API connectivity       : OK")
    except Exception as exc:
        log(f"  API connectivity       : WARN — {exc}")

    log("=" * 64)
    log("")


# ------------------------------------------------------------------
# Position evaluation (take-profit)
# ------------------------------------------------------------------

def evaluate_positions(
    client:   PolymarketUS,
    notifier: Notifier,
    state:    dict,
) -> None:
    log("INFO  Fetching portfolio positions...")
    try:
        pos_resp = client.portfolio.positions()
    except APIError as exc:
        log(f"ERROR portfolio.positions() {exc.status_code}: {exc.message}")
        return
    except Exception as exc:
        log(f"ERROR portfolio.positions(): {exc}")
        return

    positions = pos_resp.get("positions", {}) if isinstance(pos_resp, dict) else {}
    log(f"INFO  Total open positions found: {len(positions)}")

    mlb_seen = 0
    for token_id, pos in positions.items():
        meta       = pos.get("marketMetadata", {}) or {}
        m_slug     = meta.get("slug",      "") or ""
        event_slug = meta.get("eventSlug", "") or ""
        outcome    = meta.get("outcome",   "") or m_slug
        title      = meta.get("title",     "") or m_slug

        combined = (event_slug + " " + m_slug).lower()
        if not any(kw in combined for kw in ["mlb", "world-series"]):
            log(f"SKIP  token={token_id[:12]}… not an MLB market — skipping.")
            continue

        mlb_seen += 1
        qty = float(pos.get("netPosition", "0") or "0")
        if qty == 0:
            log(f"SKIP  {title} ({outcome}) — zero net position.")
            continue

        cost_val  = float((pos.get("cost",      {}) or {}).get("value", "0") or "0")
        avg_price = cost_val / qty if qty != 0 else 0

        log(f"INFO  Evaluating [{title}] outcome='{outcome}'  qty={qty}  avg_entry=${avg_price:.4f}  token={token_id[:12]}…")

        bid, ask = fetch_bbo(client, m_slug)
        if bid is None:
            log(f"WARN  {m_slug} — BBO unavailable, cannot evaluate this cycle.")
            continue

        threshold = avg_price * TAKE_PROFIT_MULTIPLIER
        decision  = "TRIGGER ✓" if bid >= threshold else f"hold (need ${threshold:.4f})"
        log(
            f"INFO  Take-profit check: bid=${bid:.4f}  threshold=${threshold:.4f} "
            f"({TAKE_PROFIT_MULTIPLIER}× ${avg_price:.4f}) → {decision}"
        )

        if avg_price <= 0 or bid < threshold:
            continue

        # ---- Close entire position ----
        intent       = "ORDER_INTENT_SELL_LONG" if qty > 0 else "ORDER_INTENT_SELL_SHORT"
        qty_to_close = abs(int(qty))
        fractional   = abs(qty) - qty_to_close
        if fractional > 0:
            log(f"WARN  {m_slug} — fractional qty {fractional:.4f} cannot be closed; closing {qty_to_close} integer shares.")
        profit = (bid - avg_price) * qty_to_close

        log(f"INFO  TAKE PROFIT: sell {qty_to_close}x {m_slug} @ bid=${bid:.4f}  est_profit=${profit:.2f}")

        try:
            order    = client.orders.create({
                "marketSlug": m_slug,
                "intent":     intent,
                "type":       "ORDER_TYPE_LIMIT",
                "price":      {"value": f"{bid:.4f}", "currency": "USD"},
                "quantity":   qty_to_close,
                "tif":        "TIME_IN_FORCE_GOOD_TILL_CANCEL",
            })
            order_id = (order.get("id", "?") if isinstance(order, dict) else "?")
            log(f"INFO  Order placed: id={order_id}  sell {qty_to_close}x {m_slug} @ ${bid:.4f}")

            state.setdefault("today_closed", []).append({
                "slug":       m_slug,
                "outcome":    outcome,
                "qty":        qty_to_close,
                "avg_price":  round(avg_price, 4),
                "exit_price": round(bid, 4),
                "profit":     round(profit, 2),
                "time":       datetime.now(EST).isoformat(),
            })

            state.setdefault("pending_buybacks", []).append({
                "market_slug":     m_slug,
                "intent":          "ORDER_INTENT_BUY_LONG" if qty > 0 else "ORDER_INTENT_BUY_SHORT",
                "avg_entry_price": round(avg_price, 6),
                "qty_sold":        qty_to_close,
                "sell_price":      round(bid, 6),
                "sell_time":       datetime.now(EST).isoformat(),
                "processed":       False,
            })

            notifier.send(
                f"🚨 *Take-Profit Executed!*\n\n"
                f"• *Market:* `{m_slug}`\n"
                f"• *Outcome:* {outcome}\n"
                f"• *Qty Closed:* {qty_to_close} (100% of position)\n"
                f"• *Entry:* `${avg_price:.4f}` → *Exit:* `${bid:.4f}` ({TAKE_PROFIT_MULTIPLIER}× trigger)\n"
                f"• *Estimated Profit:* `${profit:+.2f} USD`\n"
                f"• *Order ID:* `{order_id}`"
            )

        except APIError as exc:
            log(f"ERROR Order placement {m_slug} — {exc.status_code}: {exc.message}")
        except Exception as exc:
            log(f"ERROR Order placement {m_slug}: {exc}")

    log(f"INFO  Position scan complete — {mlb_seen} MLB position(s) evaluated.")


# ------------------------------------------------------------------
# Buyback logic (price-based, replaces time-based 5 AM approach)
# ------------------------------------------------------------------

def _buyback_zone(avg_entry: float) -> tuple[float, float]:
    """
    Return (lower, upper) price bounds for the buyback trigger.
    Zone = avg_entry ± BUYBACK_STD_DEVS × (avg_entry × BUYBACK_STD_DEV_PCT)
    """
    std_dev = avg_entry * BUYBACK_STD_DEV_PCT
    half    = BUYBACK_STD_DEVS * std_dev
    return round(avg_entry - half, 6), round(avg_entry + half, 6)


def process_buybacks(
    client:   PolymarketUS,
    notifier: Notifier,
    state:    dict,
) -> None:
    """
    Re-enter queued positions when the market price returns within
    BUYBACK_STD_DEVS standard deviations of the original average entry price.
    Runs on every health-check cycle (no time-window restriction).
    """
    pending = [b for b in state.get("pending_buybacks", []) if not b.get("processed")]
    if not pending:
        log("INFO  Buyback check — no pending buybacks.")
        return

    log(f"INFO  Buyback check — evaluating {len(pending)} pending buyback(s).")

    for buyback in pending:
        m_slug     = buyback.get("market_slug", "")
        intent     = buyback.get("intent", "ORDER_INTENT_BUY_LONG")
        avg_entry  = float(buyback.get("avg_entry_price", 0) or 0)
        qty_sold   = int(buyback.get("qty_sold", 0) or 0)

        if avg_entry <= 0:
            log(f"WARN  Buyback for {m_slug} — no avg_entry_price recorded (legacy entry). "
                f"Falling back to ${BUYBACK_AMOUNT_USD:.2f} USD allocation.")

        lower, upper = _buyback_zone(avg_entry) if avg_entry > 0 else (0.0, 9999.0)
        log(
            f"INFO  Buyback [{m_slug}] — original_entry=${avg_entry:.4f}  "
            f"zone=[${lower:.4f}, ${upper:.4f}]  ({BUYBACK_STD_DEVS} std dev)"
        )

        bid, ask = fetch_bbo(client, m_slug)
        if bid is None or bid <= 0:
            log(f"WARN  Buyback {m_slug} — no valid bid; will retry next cycle.")
            continue

        in_zone = lower <= bid <= upper
        log(
            f"INFO  Buyback decision: bid=${bid:.4f} {'IN' if in_zone else 'OUTSIDE'} "
            f"zone=[${lower:.4f}, ${upper:.4f}] → {'EXECUTE' if in_zone else 'skip (price not back to entry zone)'}"
        )

        if not in_zone:
            continue

        # Compute qty: prefer restoring the same number of shares sold; fall back to USD amount
        if qty_sold > 0 and avg_entry > 0:
            buy_qty = qty_sold
            alloc_usd = round(buy_qty * bid, 2)
            log(f"INFO  Buyback qty={buy_qty} (same as sold) @ bid=${bid:.4f} ≈ ${alloc_usd:.2f} USD")
        else:
            buy_qty   = max(1, math.floor(BUYBACK_AMOUNT_USD / bid))
            alloc_usd = BUYBACK_AMOUNT_USD
            log(f"INFO  Buyback qty={buy_qty} (${alloc_usd:.2f} allocation) @ bid=${bid:.4f}")

        try:
            order    = client.orders.create({
                "marketSlug": m_slug,
                "intent":     intent,
                "type":       "ORDER_TYPE_LIMIT",
                "price":      {"value": f"{bid:.4f}", "currency": "USD"},
                "quantity":   buy_qty,
                "tif":        "TIME_IN_FORCE_GOOD_TILL_CANCEL",
            })
            order_id = (order.get("id", "?") if isinstance(order, dict) else "?")
            log(f"INFO  Buyback order placed: id={order_id}  buy {buy_qty}x {m_slug} @ ${bid:.4f}")
            buyback["processed"] = True

            notifier.send(
                f"🔄 *Automated Buyback Complete*\n\n"
                f"• *Market:* `{m_slug}`\n"
                f"• *Trigger:* price returned within {BUYBACK_STD_DEVS} std dev of original entry `${avg_entry:.4f}`\n"
                f"• *Qty:* {buy_qty} @ `${bid:.4f}` (~${alloc_usd:.2f} USD)\n"
                f"• *Order ID:* `{order_id}`"
            )

        except APIError as exc:
            log(f"ERROR Buyback order {m_slug} — {exc.status_code}: {exc.message}")
        except Exception as exc:
            log(f"ERROR Buyback order {m_slug}: {exc}")

    state["pending_buybacks"] = [b for b in state["pending_buybacks"] if not b.get("processed")]
    still_pending = len(state["pending_buybacks"])
    log(f"INFO  Buyback cycle complete — {still_pending} still pending for next cycle.")


# ------------------------------------------------------------------
# Workflow rollover (self-trigger before 6 h runner termination)
# ------------------------------------------------------------------

def trigger_workflow_handoff() -> bool:
    """
    Dispatch a new workflow_dispatch run so the chain never breaks.
    Requires GH_PAT secret (with 'workflow' scope) passed as GH_PAT env var.
    Falls back gracefully — daily cron at 00:07 UTC provides a safety net.
    """
    gh_pat = os.environ.get("GH_PAT") or os.environ.get("GH_TOKEN")
    repo   = os.environ.get("GITHUB_REPOSITORY", "tamzid2001/polymarketfuturesbot")

    if not gh_pat:
        log(
            "WARN  Workflow handoff skipped — GH_PAT secret not set. "
            "The daily cron at 00:07 UTC will restart the chain automatically."
        )
        return False

    url     = f"https://api.github.com/repos/{repo}/actions/workflows/polymarket_monitor.yml/dispatches"
    headers = {
        "Authorization":        f"Bearer {gh_pat}",
        "Accept":               "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    log(f"INFO  Workflow handoff: triggering next run on repo={repo} ...")
    try:
        resp = requests.post(url, headers=headers, json={"ref": "main"}, timeout=15)
        if resp.status_code == 204:
            log("INFO  Workflow handoff SUCCESS — next run queued.")
            return True
        log(f"ERROR Workflow handoff HTTP {resp.status_code}: {resp.text[:200]}")
        return False
    except Exception as exc:
        log(f"ERROR Workflow handoff request failed: {exc}")
        return False


# ------------------------------------------------------------------
# Main entry point
# ------------------------------------------------------------------

def main() -> None:
    start_time = time.monotonic()

    log("=" * 64)
    log("STARTUP  Polymarket Futures Bot")
    log(f"         Python {sys.version.split()[0]}")
    log(f"         PID   {os.getpid()}")
    log(f"         CWD   {os.getcwd()}")

    # Required credentials
    api_key    = os.environ.get("POLYMARKET_PUBLIC_KEY")
    secret_key = os.environ.get("POLYMARKET_SECRET_KEY")

    if not api_key or not secret_key:
        log("ERROR  POLYMARKET_PUBLIC_KEY and/or POLYMARKET_SECRET_KEY not set — cannot continue.")
        sys.exit(1)

    # Optional Telegram
    tg_token = os.environ.get("TELEGRAM_KEY")
    notifier = Notifier(tg_token, TELEGRAM_CHAT_ID)
    if notifier.enabled:
        log("INFO   Telegram notifications: ENABLED (TELEGRAM_KEY found)")
    else:
        log("INFO   Telegram notifications: DISABLED (TELEGRAM_KEY not set — running silently)")

    log(f"CONFIG  TAKE_PROFIT_MULTIPLIER = {TAKE_PROFIT_MULTIPLIER}×")
    log(f"CONFIG  BUYBACK_AMOUNT_USD     = ${BUYBACK_AMOUNT_USD:.2f}")
    log(f"CONFIG  BUYBACK_STD_DEVS       = {BUYBACK_STD_DEVS}  ({BUYBACK_STD_DEV_PCT*100:.0f}% per dev)")
    log(f"CONFIG  LOOP_INTERVAL          = {LOOP_INTERVAL_SECONDS}s ({LOOP_INTERVAL_SECONDS//60} min)")
    log(f"CONFIG  RUNTIME_LIMIT          = {RUNTIME_LIMIT_SECONDS}s ({RUNTIME_LIMIT_SECONDS//60} min)")
    log("=" * 64)

    client = PolymarketUS(key_id=api_key, secret_key=secret_key)
    state  = load_state()

    try:
        loop_num = 0
        while True:
            elapsed_s   = time.monotonic() - start_time
            remaining_s = RUNTIME_LIMIT_SECONDS - elapsed_s

            # ---- Rollover check ----
            if remaining_s <= 0:
                log(f"INFO  Runtime limit reached ({RUNTIME_LIMIT_SECONDS // 60} min). Initiating handoff...")
                save_state(state)
                trigger_workflow_handoff()
                log("INFO  Handoff complete. Exiting cleanly.")
                break

            loop_num += 1
            iter_start = time.monotonic()

            run_health_check(client, notifier, state, elapsed_s, loop_num)

            # Balance snapshot (once/day)
            balance = fetch_balance(client)
            snapshot_balance_today(state, balance)

            # 10 AM EST daily report
            now_est = datetime.now(EST)
            if now_est.hour == 10 and now_est.minute < 15:
                log("INFO  10 AM window — sending daily report.")
                send_daily_report(notifier, state)

            # Core trading logic
            evaluate_positions(client, notifier, state)
            process_buybacks(client, notifier, state)

            save_state(state)

            # ---- Sleep for remainder of 15-min interval ----
            iter_elapsed = time.monotonic() - iter_start
            sleep_s      = max(0.0, LOOP_INTERVAL_SECONDS - iter_elapsed)
            # Don't sleep past the rollover deadline
            sleep_s      = min(sleep_s, RUNTIME_LIMIT_SECONDS - (time.monotonic() - start_time))

            if sleep_s > 1:
                log(
                    f"INFO  Iteration {loop_num} took {iter_elapsed:.1f}s. "
                    f"Sleeping {sleep_s:.0f}s until next health check. "
                    f"Runtime remaining: {remaining_s/60:.1f} min."
                )
                time.sleep(sleep_s)

    except KeyboardInterrupt:
        log("INFO  KeyboardInterrupt received — shutting down gracefully.")
    except Exception as exc:
        log(f"ERROR  Unhandled exception in main loop: {exc}")
        raise
    finally:
        client.close()
        save_state(state)
        log("END    Client closed. State saved. Bot execution complete.")
        log("=" * 64)


if __name__ == "__main__":
    main()
