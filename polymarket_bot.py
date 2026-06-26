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
    # Load Repository Secrets
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
        positions = client.portfolio.positions()
    except Exception as e:
        print(f"Failed to fetch portfolio positions: {e}")
        positions = []
        
    for pos in positions:
        # Safe structural parsing across SDK object schemas
        m_slug = getattr(pos, 'market_slug', None) or getattr(pos, 'marketSlug', None) or (pos.get('marketSlug') if isinstance(pos, dict) else None) or (pos.get('market_slug') if isinstance(pos, dict) else None)
        asset = getattr(pos, 'asset', None) or (pos.get('asset') if isinstance(pos, dict) else None)
        qty = getattr(pos, 'quantity', None) or getattr(pos, 'size', None) or (pos.get('quantity') if isinstance(pos, dict) else None) or (pos.get('size') if isinstance(pos, dict) else None)
        avg_price = getattr(pos, 'avgPrice', None) or getattr(pos, 'avg_price', None) or (pos.get('avgPrice') if isinstance(pos, dict) else None) or (pos.get('avg_price') if isinstance(pos, dict) else None)
        cur_price = getattr(pos, 'currentPrice', None) or getattr(pos, 'current_price', None) or (pos.get('currentPrice') if isinstance(pos, dict) else None) or (pos.get('current_price') if isinstance(pos, dict) else None)
        
        if not m_slug or not asset or qty is None or not avg_price or not cur_price:
            continue
            
        # Target MLB World Series Futures Explicitly
        if "mlb" in m_slug.lower() or "world-series" in m_slug.lower():
            qty, avg_price, cur_price = float(qty), float(avg_price), float(cur_price)
            
            # Identify if position value has appreciated > 100%
            if avg_price > 0 and (cur_price >= 2 * avg_price):
                profit = (cur_price - avg_price) * qty
                print(f"Trigger condition met for {m_slug}. Closing positions.")
                
                intent = "ORDER_INTENT_SELL_LONG" if "LONG" in str(asset).upper() else "ORDER_INTENT_SELL_SHORT"
                
                try:
                    client.orders.create({
                        "marketSlug": m_slug,
                        "intent": intent,
                        "type": "ORDER_TYPE_LIMIT",
                        "price": {"value": f"{cur_price:.2f}", "currency": "USD"},
                        "quantity": int(qty),
                        "tif": "TIME_IN_FORCE_GOOD_TILL_CANCEL",
                    })
                    
                    msg = f"🚨 *Polymarket Position Profit Taken!*\n\n• *Market:* `{m_slug}`\n• *Closed Position:* {qty} {asset}\n• *Net Profit Realized:* `${profit:.2f} USD`"
                    send_telegram(tg_token, msg)
                    
                    # Queue the closed asset for next scheduled morning buyback
                    state["pending_buybacks"].append({
                        "market_slug": m_slug,
                        "intent": "ORDER_INTENT_BUY_LONG" if "LONG" in str(asset).upper() else "ORDER_INTENT_BUY_SHORT",
                        "processed": False
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
                # Resolve current pricing context
                book = client.markets.book(m_slug)
                cur_price = 0.50 # Fallback default
                if book and hasattr(book, 'bids') and len(book.bids) > 0:
                    cur_price = float(book.bids[0].price)
                
                # Compute fractional conversion into exact integer shares for $1.00 USD size
                buy_qty = max(1, int(1.0 / cur_price))
                
                client.orders.create({
                    "marketSlug": m_slug,
                    "intent": intent,
                    "type": "ORDER_TYPE_LIMIT",
                    "price": {"value": f"{cur_price:.2f}", "currency": "USD"},
                    "quantity": buy_qty,
                    "tif": "TIME_IN_FORCE_GOOD_TILL_CANCEL",
                })
                
                buy_msg = f"🔄 *Polymarket Automated Buyback Complete*\n\n• *Market:* `{m_slug}`\n• *Allocation:* `$1.00 USD` (~{buy_qty} shares @ `${cur_price:.2f}`)"
                send_telegram(tg_token, buy_msg)
                buyback["processed"] = True
                
            except Exception as buy_err:
                print(f"Buyback failed for {m_slug}: {buy_err}")
        
        # Clean state history
        state["pending_buybacks"] = [b for b in state["pending_buybacks"] if not b.get("processed")]
        
    save_state(state)

if __name__ == "__main__":
    main()
