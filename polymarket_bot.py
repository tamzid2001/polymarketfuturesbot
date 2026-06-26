import os
import json
import sys
from datetime import datetime
import zoneinfo
import requests
from polymarket_us import PolymarketUS

STATE_FILE = "state.json"
TELEGRAM_CHAT_ID = "@moneyballpredictions"

def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except Exception:
            return {"pending_buybacks": []}
    return {"pending_buybacks": []}

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=4)

def send_telegram(token, text):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"}
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
    except Exception as e:
        print(f"Telegram Alert Failed: {e}")

def main():
    api_key = os.environ.get("POLYMARKET_PUBLIC_KEY")
    secret_key = os.environ.get("POLYMARKET_SECRET_KEY")
    tg_token = os.environ.get("TELEGRAM_KEY")

    if not api_key or not secret_key or not tg_token:
        print("Error: Missing critical environment configuration variables.")
        sys.exit(1)

    client = PolymarketUS(key_id=api_key, secret_key=secret_key)
    state = load_state()

    # 1. Evaluate Portfolio & Take Profit
    try:
        pos_resp = client.portfolio.positions()
        # SDK returns {"positions": dict[str, UserPosition], "nextCursor": str, "eof": bool}
        positions_dict = pos_resp.get("positions", {}) if isinstance(pos_resp, dict) else {}
    except Exception as e:
        print(f"Failed to fetch portfolio positions: {e}")
        positions_dict = {}

    for token_id, pos in positions_dict.items():
        meta = pos.get("marketMetadata", {}) or {}
        m_slug = meta.get("slug", "") or ""
        event_slug = meta.get("eventSlug", "") or ""
        outcome = meta.get("outcome", "") or ""

        if not m_slug:
            continue

        # Target MLB World Series Futures Explicitly
        is_mlb = "mlb" in event_slug.lower() or "world-series" in event_slug.lower() \
               or "mlb" in m_slug.lower() or "world-series" in m_slug.lower()
        if not is_mlb:
            continue

        net_pos_str = pos.get("netPosition", "0") or "0"
        cost = pos.get("cost", {}) or {}
        cash_value = pos.get("cashValue", {}) or {}

        qty = float(net_pos_str)
        cost_val = float(cost.get("value", "0") or "0")
        cash_val = float(cash_value.get("value", "0") or "0")

        if qty == 0:
            continue

        avg_price = cost_val / qty
        cur_price = cash_val / qty

        # Identify if position value has appreciated > 100%
        if avg_price > 0 and cur_price >= 2 * avg_price:
            profit = cash_val - cost_val
            print(f"Trigger condition met for {m_slug} ({outcome}). Closing position.")

            # Long positions (positive net) use SELL_LONG; short (negative) use SELL_SHORT
            intent = "ORDER_INTENT_SELL_LONG" if qty > 0 else "ORDER_INTENT_SELL_SHORT"
            qty_to_close = abs(int(qty))

            try:
                client.orders.create({
                    "marketSlug": m_slug,
                    "intent": intent,
                    "type": "ORDER_TYPE_LIMIT",
                    "price": {"value": f"{cur_price:.4f}", "currency": "USD"},
                    "quantity": qty_to_close,
                    "tif": "TIME_IN_FORCE_GOOD_TILL_CANCEL",
                })

                msg = (
                    f"🚨 *Polymarket Position Profit Taken!*\n\n"
                    f"• *Market:* `{m_slug}`\n"
                    f"• *Outcome:* {outcome}\n"
                    f"• *Closed Qty:* {qty_to_close}\n"
                    f"• *Avg Entry:* `${avg_price:.4f}` → *Exit:* `${cur_price:.4f}`\n"
                    f"• *Net Profit:* `${profit:.2f} USD`"
                )
                send_telegram(tg_token, msg)

                state["pending_buybacks"].append({
                    "market_slug": m_slug,
                    "intent": "ORDER_INTENT_BUY_LONG" if qty > 0 else "ORDER_INTENT_BUY_SHORT",
                    "processed": False,
                })
            except Exception as order_err:
                print(f"Order routing failed for {m_slug}: {order_err}")

    # 2. Time-Window Validation: 5:00 AM EST Scheduled Re-entries
    est_tz = zoneinfo.ZoneInfo("America/New_York")
    now_est = datetime.now(est_tz)

    if now_est.hour == 5 and now_est.minute < 15:
        print("Checking active scheduled buybacks...")
        for buyback in state.get("pending_buybacks", []):
            if buyback.get("processed"):
                continue

            m_slug = buyback["market_slug"]
            intent = buyback["intent"]

            try:
                # Resolve current best bid for limit price
                bbo = client.markets.bbo(m_slug)
                cur_price = 0.50
                if isinstance(bbo, dict):
                    bid = bbo.get("bid") or bbo.get("bbo", {}).get("bid")
                    if bid:
                        cur_price = float(bid.get("price", 0.50) if isinstance(bid, dict) else bid)

                buy_qty = max(1, int(1.0 / cur_price))

                client.orders.create({
                    "marketSlug": m_slug,
                    "intent": intent,
                    "type": "ORDER_TYPE_LIMIT",
                    "price": {"value": f"{cur_price:.4f}", "currency": "USD"},
                    "quantity": buy_qty,
                    "tif": "TIME_IN_FORCE_GOOD_TILL_CANCEL",
                })

                buy_msg = (
                    f"🔄 *Polymarket Automated Buyback Complete*\n\n"
                    f"• *Market:* `{m_slug}`\n"
                    f"• *Allocation:* `$1.00 USD` (~{buy_qty} shares @ `${cur_price:.4f}`)"
                )
                send_telegram(tg_token, buy_msg)
                buyback["processed"] = True

            except Exception as buy_err:
                print(f"Buyback failed for {m_slug}: {buy_err}")

        state["pending_buybacks"] = [b for b in state["pending_buybacks"] if not b.get("processed")]

    save_state(state)
    client.close()

if __name__ == "__main__":
    main()
