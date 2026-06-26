import os
import json
import sys
from datetime import datetime, date
import zoneinfo
import requests
from polymarket_us import PolymarketUS, APIError

STATE_FILE = "state.json"
TELEGRAM_CHAT_ID = "@moneyballpredictions"
EST = zoneinfo.ZoneInfo("America/New_York")

# ------------------------------------------------------------------
# Take-profit threshold: close when current price >= this multiple
# of average entry (2.0 = 100% gain, 1.5 = 50% gain, etc.)
# ------------------------------------------------------------------
TAKE_PROFIT_MULTIPLIER = 2.0


def log(msg):
    ts = datetime.now(EST).strftime("%Y-%m-%d %H:%M:%S EST")
    print(f"[{ts}] {msg}")


def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            log(f"WARN  State file unreadable ({e}), starting fresh.")
    return {"pending_buybacks": [], "balance_snapshots": {}, "today_closed": [], "daily_report_sent": ""}


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=4)


def send_telegram(token, text):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        resp = requests.post(
            url,
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"},
            timeout=10,
        )
        resp.raise_for_status()
        log(f"INFO  Telegram sent (msg_id={resp.json()['result']['message_id']})")
    except Exception as e:
        log(f"ERROR Telegram send failed: {e}")


def fetch_balance(client):
    """Returns the first UserBalance dict or {} on failure."""
    try:
        resp = client.account.balances()
        balances = resp.get("balances", []) if isinstance(resp, dict) else []
        if not balances:
            log("WARN  account.balances() returned empty list.")
            return {}
        b = balances[0]
        log(
            f"INFO  Balance — cash=${b.get('currentBalance', 0):.2f} "
            f"buyingPower=${b.get('buyingPower', 0):.2f} "
            f"notional=${b.get('assetNotional', 0):.2f}"
        )
        return b
    except APIError as e:
        log(f"ERROR fetch_balance API error {e.status_code}: {e.message}")
        return {}


def fetch_bbo_price(client, m_slug):
    """Returns (best_bid_float, best_ask_float) or (None, None) on failure."""
    try:
        bbo = client.markets.bbo(m_slug)
        if not isinstance(bbo, dict):
            log(f"WARN  BBO for {m_slug} returned unexpected type: {type(bbo)}")
            return None, None
        best_bid_raw = bbo.get("bestBid") or {}
        best_ask_raw = bbo.get("bestAsk") or {}
        bid = float(best_bid_raw.get("value", 0) or 0)
        ask = float(best_ask_raw.get("value", 0) or 0)
        log(f"INFO  BBO {m_slug} — bid=${bid:.4f} ask=${ask:.4f} depth_bid={bbo.get('bidDepth')} depth_ask={bbo.get('askDepth')}")
        return bid, ask
    except APIError as e:
        log(f"ERROR BBO fetch for {m_slug} API error {e.status_code}: {e.message}")
        return None, None
    except Exception as e:
        log(f"ERROR BBO fetch for {m_slug}: {e}")
        return None, None


def snapshot_balance_today(state, balance):
    """Store today's balance once per calendar day."""
    today_str = date.today().isoformat()
    if balance and today_str not in state.get("balance_snapshots", {}):
        state.setdefault("balance_snapshots", {})[today_str] = {
            "currentBalance": balance.get("currentBalance", 0),
            "buyingPower": balance.get("buyingPower", 0),
            "assetNotional": balance.get("assetNotional", 0),
        }
        log(f"INFO  Saved balance snapshot for {today_str}.")
    # Keep only the last 7 days
    snapshots = state.get("balance_snapshots", {})
    if len(snapshots) > 7:
        oldest = sorted(snapshots.keys())[0]
        del snapshots[oldest]


def send_daily_report(tg_token, state):
    """Send 10 AM EST balance comparison + closed-position summary."""
    today_str = date.today().isoformat()

    if state.get("daily_report_sent") == today_str:
        log("INFO  Daily report already sent today, skipping.")
        return

    snapshots = state.get("balance_snapshots", {})
    sorted_days = sorted(snapshots.keys())

    today_snap = snapshots.get(today_str, {})
    yesterday_snap = {}
    if len(sorted_days) >= 2:
        yesterday_key = sorted_days[-2] if sorted_days[-1] == today_str else sorted_days[-1]
        yesterday_snap = snapshots.get(yesterday_key, {})

    today_bal = today_snap.get("currentBalance", 0)
    yest_bal = yesterday_snap.get("currentBalance", 0)
    delta = today_bal - yest_bal
    delta_pct = (delta / yest_bal * 100) if yest_bal else 0
    direction = "📈" if delta >= 0 else "📉"

    closed = state.get("today_closed", [])
    total_profit = sum(p.get("profit", 0) for p in closed)

    lines = [
        f"🌅 *Polymarket Daily Report — {today_str}*\n",
        f"💰 *Balance*",
        f"  Yesterday: `${yest_bal:.2f}`" if yest_bal else "  Yesterday: _no snapshot_",
        f"  Today:     `${today_bal:.2f}`",
        f"  Change:    {direction} `${delta:+.2f}` ({delta_pct:+.1f}%)\n",
    ]

    if closed:
        lines.append(f"✅ *Positions Closed Today ({len(closed)} trades, total P&L: `${total_profit:+.2f}`)*")
        for p in closed:
            emoji = "🟢" if p.get("profit", 0) >= 0 else "🔴"
            lines.append(
                f"{emoji} `{p.get('slug', '?')}` — {p.get('outcome', '')}\n"
                f"   Qty {p.get('qty')} | Entry `${p.get('avg_price', 0):.4f}` → Exit `${p.get('exit_price', 0):.4f}` | P&L `${p.get('profit', 0):+.2f}`"
            )
    else:
        lines.append("_No positions closed today._")

    send_telegram(tg_token, "\n".join(lines))
    state["daily_report_sent"] = today_str
    state["today_closed"] = []  # reset after report
    log("INFO  Daily report sent and today_closed list cleared.")


def evaluate_positions(client, tg_token, state):
    """Scan all MLB positions and close any that hit the take-profit threshold."""
    log("INFO  Fetching portfolio positions...")
    try:
        pos_resp = client.portfolio.positions()
    except APIError as e:
        log(f"ERROR portfolio.positions() API error {e.status_code}: {e.message}")
        return
    except Exception as e:
        log(f"ERROR portfolio.positions() failed: {e}")
        return

    positions_dict = pos_resp.get("positions", {}) if isinstance(pos_resp, dict) else {}
    log(f"INFO  Total open positions: {len(positions_dict)}")

    mlb_count = 0
    for token_id, pos in positions_dict.items():
        meta = pos.get("marketMetadata", {}) or {}
        m_slug = meta.get("slug", "") or ""
        event_slug = meta.get("eventSlug", "") or ""
        outcome = meta.get("outcome", "") or m_slug
        title = meta.get("title", "") or m_slug

        is_mlb = any(kw in (event_slug + m_slug).lower() for kw in ["mlb", "world-series"])
        if not is_mlb:
            continue

        mlb_count += 1
        net_pos_str = pos.get("netPosition", "0") or "0"
        qty = float(net_pos_str)
        if qty == 0:
            log(f"SKIP  {title} ({outcome}) — zero net position.")
            continue

        cost = pos.get("cost", {}) or {}
        cost_val = float(cost.get("value", "0") or "0")
        avg_price = cost_val / qty if qty != 0 else 0

        log(f"INFO  Evaluating: [{title}] outcome='{outcome}' qty={qty} avg_entry=${avg_price:.4f}")

        # Fetch live BBO for accurate current price (not stale cashValue)
        best_bid, best_ask = fetch_bbo_price(client, m_slug)
        if best_bid is None:
            log(f"WARN  Cannot evaluate {m_slug} — BBO unavailable, skipping.")
            continue

        threshold = avg_price * TAKE_PROFIT_MULTIPLIER
        log(
            f"INFO  Take-profit check: best_bid=${best_bid:.4f} vs threshold=${threshold:.4f} "
            f"({TAKE_PROFIT_MULTIPLIER}× avg ${avg_price:.4f}) — "
            f"{'TRIGGER' if best_bid >= threshold else 'hold'}"
        )

        if avg_price > 0 and best_bid >= threshold:
            intent = "ORDER_INTENT_SELL_LONG" if qty > 0 else "ORDER_INTENT_SELL_SHORT"
            qty_to_close = abs(int(qty))
            profit = (best_bid - avg_price) * qty_to_close
            log(f"INFO  TAKE PROFIT → {m_slug} | qty={qty_to_close} | bid=${best_bid:.4f} | est_profit=${profit:.2f}")

            try:
                order = client.orders.create({
                    "marketSlug": m_slug,
                    "intent": intent,
                    "type": "ORDER_TYPE_LIMIT",
                    "price": {"value": f"{best_bid:.4f}", "currency": "USD"},
                    "quantity": qty_to_close,
                    "tif": "TIME_IN_FORCE_GOOD_TILL_CANCEL",
                })
                order_id = order.get("id", "?") if isinstance(order, dict) else "?"
                log(f"INFO  Order placed: id={order_id} sell {qty_to_close}x {m_slug} @ ${best_bid:.4f}")

                state.setdefault("today_closed", []).append({
                    "slug": m_slug,
                    "outcome": outcome,
                    "qty": qty_to_close,
                    "avg_price": round(avg_price, 4),
                    "exit_price": round(best_bid, 4),
                    "profit": round(profit, 2),
                    "time": datetime.now(EST).isoformat(),
                })

                state.setdefault("pending_buybacks", []).append({
                    "market_slug": m_slug,
                    "intent": "ORDER_INTENT_BUY_LONG" if qty > 0 else "ORDER_INTENT_BUY_SHORT",
                    "processed": False,
                })

                send_telegram(
                    tg_token,
                    f"🚨 *Polymarket Take-Profit Executed!*\n\n"
                    f"• *Market:* `{m_slug}`\n"
                    f"• *Outcome:* {outcome}\n"
                    f"• *Qty Closed:* {qty_to_close}\n"
                    f"• *Entry:* `${avg_price:.4f}` → *Exit:* `${best_bid:.4f}` ({TAKE_PROFIT_MULTIPLIER}× trigger)\n"
                    f"• *Estimated Profit:* `${profit:+.2f} USD`\n"
                    f"• *Order ID:* `{order_id}`",
                )
            except APIError as e:
                log(f"ERROR Order placement failed for {m_slug} — {e.status_code}: {e.message}")
            except Exception as e:
                log(f"ERROR Order placement failed for {m_slug}: {e}")

    log(f"INFO  MLB position scan complete. {mlb_count} MLB position(s) evaluated.")


def process_buybacks(client, tg_token, state, now_est):
    """5:00–5:15 AM EST: re-enter any queued positions at $1.00 USD each."""
    if not (now_est.hour == 5 and now_est.minute < 15):
        return

    pending = [b for b in state.get("pending_buybacks", []) if not b.get("processed")]
    if not pending:
        log("INFO  Buyback window active — no pending buybacks.")
        return

    log(f"INFO  Buyback window active — processing {len(pending)} queued buyback(s).")

    for buyback in pending:
        m_slug = buyback["market_slug"]
        intent = buyback["intent"]
        log(f"INFO  Processing buyback: {m_slug} intent={intent}")

        best_bid, best_ask = fetch_bbo_price(client, m_slug)

        if best_bid is None or best_bid <= 0:
            log(f"WARN  Buyback skipped for {m_slug} — no valid bid price.")
            continue

        buy_qty = max(1, int(1.0 / best_bid))
        log(f"INFO  Buyback qty={buy_qty} @ bid=${best_bid:.4f} (~$1.00 USD allocation)")

        try:
            order = client.orders.create({
                "marketSlug": m_slug,
                "intent": intent,
                "type": "ORDER_TYPE_LIMIT",
                "price": {"value": f"{best_bid:.4f}", "currency": "USD"},
                "quantity": buy_qty,
                "tif": "TIME_IN_FORCE_GOOD_TILL_CANCEL",
            })
            order_id = order.get("id", "?") if isinstance(order, dict) else "?"
            log(f"INFO  Buyback order placed: id={order_id}")
            buyback["processed"] = True

            send_telegram(
                tg_token,
                f"🔄 *Polymarket Automated Buyback Complete*\n\n"
                f"• *Market:* `{m_slug}`\n"
                f"• *Allocation:* `$1.00 USD` (~{buy_qty} shares @ `${best_bid:.4f}`)\n"
                f"• *Order ID:* `{order_id}`",
            )
        except APIError as e:
            log(f"ERROR Buyback order failed for {m_slug} — {e.status_code}: {e.message}")
        except Exception as e:
            log(f"ERROR Buyback order failed for {m_slug}: {e}")

    state["pending_buybacks"] = [b for b in state["pending_buybacks"] if not b.get("processed")]
    log(f"INFO  Buybacks complete. {len(state['pending_buybacks'])} still pending.")


def main():
    log("=" * 60)
    log("START Polymarket Bot execution")

    api_key = os.environ.get("POLYMARKET_PUBLIC_KEY")
    secret_key = os.environ.get("POLYMARKET_SECRET_KEY")
    tg_token = os.environ.get("TELEGRAM_KEY")

    if not api_key or not secret_key or not tg_token:
        log("ERROR Missing required environment variables. Exiting.")
        sys.exit(1)

    client = PolymarketUS(key_id=api_key, secret_key=secret_key)
    state = load_state()
    now_est = datetime.now(EST)
    log(f"INFO  Local time (EST): {now_est.strftime('%Y-%m-%d %H:%M:%S %Z')}")

    try:
        # Snapshot balance once per day
        balance = fetch_balance(client)
        snapshot_balance_today(state, balance)

        # 10 AM EST — send daily report
        if now_est.hour == 10 and now_est.minute < 15:
            log("INFO  10 AM window — sending daily report.")
            send_daily_report(tg_token, state)

        # Evaluate all MLB positions for take-profit
        evaluate_positions(client, tg_token, state)

        # 5 AM EST — process scheduled buybacks
        process_buybacks(client, tg_token, state, now_est)

    except Exception as e:
        log(f"ERROR Unhandled exception in main loop: {e}")
        raise
    finally:
        client.close()
        save_state(state)
        log("END   State saved. Bot execution complete.")
        log("=" * 60)


if __name__ == "__main__":
    main()
