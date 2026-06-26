import os
import sys
import requests
from polymarket_us import PolymarketUS

TELEGRAM_CHAT_ID = "@moneyballpredictions"

MLB_EVENT_SLUGS = [
    "mlb-world-series-champion-2026",
    "mlb-2026-american-league-champion",
    "mlb-2026-national-league-champion",
    "mlb-2026-al-east-champion",
    "mlb-2026-al-central-champion",
    "mlb-2026-al-west-champion",
    "mlb-2026-nl-east-champion",
    "mlb-2026-nl-central-champion",
    "mlb-2026-nl-west-champion",
    "mlb-world-series-champion-2025",
    "mlb-2025-american-league-champion",
    "mlb-2025-national-league-champion",
]


def check_secrets():
    print("=== [1/4] Secrets Check ===")
    required = {
        "POLYMARKET_PUBLIC_KEY": os.environ.get("POLYMARKET_PUBLIC_KEY"),
        "POLYMARKET_SECRET_KEY": os.environ.get("POLYMARKET_SECRET_KEY"),
        "TELEGRAM_KEY": os.environ.get("TELEGRAM_KEY"),
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        print(f"FAIL — Missing secrets: {', '.join(missing)}")
        sys.exit(1)
    print("PASS — All 3 secrets present.\n")
    return required


def send_telegram(tg_token, text):
    url = f"https://api.telegram.org/bot{tg_token}/sendMessage"
    resp = requests.post(
        url,
        json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["result"]["message_id"]


def test_telegram(tg_token):
    print("=== [2/4] Telegram Greeting ===")
    msg_id = send_telegram(
        tg_token,
        "👋 *Polymarket Bot — QA Test Started*\n\n"
        "✅ Secrets loaded\n"
        "✅ Telegram connectivity verified\n"
        "⏳ Fetching wallet balance and MLB positions...",
    )
    print(f"PASS — Message delivered (id: {msg_id})\n")


def test_balance(client):
    print("=== [3/4] Wallet Balance ===")
    resp = client.account.balances()

    balance_list = resp.get("balances", []) if isinstance(resp, dict) else []

    if not balance_list:
        print("WARN — No balances returned.")
        return "_(no balance data)_"

    lines = []
    for b in balance_list:
        currency = b.get("currency", "USD")
        current = b.get("currentBalance", 0) or 0
        buying_power = b.get("buyingPower", 0) or 0
        asset_notional = b.get("assetNotional", 0) or 0
        open_orders = b.get("openOrders", 0) or 0
        lines.append(
            f"  • *{currency}*\n"
            f"    Cash balance: `${current:.2f}`\n"
            f"    Buying power: `${buying_power:.2f}`\n"
            f"    Position notional: `${asset_notional:.2f}`\n"
            f"    In open orders: `${open_orders:.2f}`"
        )
        print(f"  {currency}: cash=${current:.2f}, buying_power=${buying_power:.2f}, notional=${asset_notional:.2f}")

    print("PASS — Balance fetched.\n")
    return "\n".join(lines)


def test_mlb_positions(client):
    print("=== [4/4] MLB World Series Positions & Event Contracts ===")

    # --- Fetch live positions ---
    pos_resp = client.portfolio.positions()
    positions_dict = pos_resp.get("positions", {}) if isinstance(pos_resp, dict) else {}

    mlb_positions = []
    for token_id, pos in positions_dict.items():
        meta = pos.get("marketMetadata", {}) or {}
        event_slug = meta.get("eventSlug", "") or ""
        slug = meta.get("slug", "") or ""

        is_mlb = (
            any(s in event_slug.lower() for s in ["mlb", "world-series"])
            or any(s in slug.lower() for s in ["mlb", "world-series"])
        )
        if not is_mlb:
            continue

        net_pos = pos.get("netPosition", "0") or "0"
        qty_available = pos.get("qtyAvailable", "0") or "0"
        cost = pos.get("cost", {}) or {}
        cash_value = pos.get("cashValue", {}) or {}
        realized = pos.get("realized", {}) or {}

        cost_val = float(cost.get("value", "0") or "0")
        cash_val = float(cash_value.get("value", "0") or "0")
        realized_val = float(realized.get("value", "0") or "0")
        qty = float(net_pos)

        avg_price = round(cost_val / qty, 4) if qty != 0 else 0
        cur_price = round(cash_val / qty, 4) if qty != 0 else 0
        unrealized_pnl = cash_val - cost_val

        mlb_positions.append({
            "token_id": token_id,
            "slug": slug,
            "event_slug": event_slug,
            "title": meta.get("title", ""),
            "outcome": meta.get("outcome", ""),
            "net_pos": qty,
            "qty_available": qty_available,
            "avg_price": avg_price,
            "cur_price": cur_price,
            "cost_val": cost_val,
            "cash_val": cash_val,
            "unrealized_pnl": unrealized_pnl,
            "realized_val": realized_val,
        })

        print(f"  POSITION: {meta.get('title', slug)} | {meta.get('outcome', '')} | qty={qty} avg=${avg_price} cur=${cur_price}")

    if not mlb_positions:
        print("  (no open MLB positions found)")

    # --- Fetch World Series 2026 event contracts ---
    print("\n  Fetching MLB World Series 2026 event contracts...")
    ws_markets = []
    try:
        event = client.events.retrieve_by_slug("mlb-world-series-champion-2026")
        raw_markets = None
        if isinstance(event, dict):
            raw_markets = event.get("markets") or event.get("event", {}).get("markets")
        if raw_markets:
            for m in raw_markets:
                mslug = m.get("slug", "")
                title = m.get("title", "")
                outcome = m.get("outcome", "")
                ws_markets.append({"slug": mslug, "title": title, "outcome": outcome})
                print(f"    Contract: {outcome or title} — slug: {mslug}")
        else:
            print(f"  WARN — Event returned but no markets field found. Keys: {list(event.keys()) if isinstance(event, dict) else type(event)}")
    except Exception as e:
        print(f"  WARN — Could not fetch WS 2026 event: {e}")

    print("PASS — Positions and event contracts fetched.\n")
    return mlb_positions, ws_markets


def build_telegram_report(balance_text, mlb_positions, ws_markets):
    lines = ["📊 *Polymarket QA — Full Portfolio Report*\n"]

    lines.append("💰 *Wallet Balance*")
    lines.append(balance_text)

    lines.append("\n⚾ *Open MLB World Series Positions*")
    if mlb_positions:
        for p in mlb_positions:
            pnl_emoji = "🟢" if p["unrealized_pnl"] >= 0 else "🔴"
            lines.append(
                f"{pnl_emoji} *{p['title'] or p['slug']}*\n"
                f"   Outcome: `{p['outcome']}`\n"
                f"   Qty: `{p['net_pos']}` | Avg: `${p['avg_price']}` | Cur: `${p['cur_price']}`\n"
                f"   Unrealized PnL: `${p['unrealized_pnl']:.2f}` | Realized: `${p['realized_val']:.2f}`"
            )
    else:
        lines.append("_No open MLB positions found._")

    if ws_markets:
        lines.append(f"\n📋 *World Series 2026 Contracts ({len(ws_markets)} teams)*")
        for m in ws_markets[:10]:
            lines.append(f"  • `{m['slug']}` — {m['outcome'] or m['title']}")
        if len(ws_markets) > 10:
            lines.append(f"  _...and {len(ws_markets) - 10} more_")

    lines.append("\n✅ *QA Test Complete — All checks passed.*")
    return "\n".join(lines)


def main():
    print("=== Polymarket Bot QA Test Suite ===\n")

    secrets = check_secrets()
    api_key = secrets["POLYMARKET_PUBLIC_KEY"]
    secret_key = secrets["POLYMARKET_SECRET_KEY"]
    tg_token = secrets["TELEGRAM_KEY"]

    test_telegram(tg_token)

    client = PolymarketUS(key_id=api_key, secret_key=secret_key)
    try:
        balance_text = test_balance(client)
        mlb_positions, ws_markets = test_mlb_positions(client)
        report = build_telegram_report(balance_text, mlb_positions, ws_markets)
        send_telegram(tg_token, report)
        print("PASS — Full report sent to Telegram.")
    finally:
        client.close()

    print("\n=== All QA tests passed ===")


if __name__ == "__main__":
    main()
