"""
QA test suite — validates secrets, API connectivity, wallet balance,
and MLB position data. Telegram is optional: if TELEGRAM_KEY is absent
the test still passes; Telegram steps are simply skipped.
"""

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


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def send_telegram(tg_token: str, text: str) -> int:
    url  = f"https://api.telegram.org/bot{tg_token}/sendMessage"
    resp = requests.post(
        url,
        json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["result"]["message_id"]


# ------------------------------------------------------------------
# Test steps
# ------------------------------------------------------------------

def check_secrets() -> dict:
    print("=== [1] Secrets Check ===")
    required = {
        "POLYMARKET_PUBLIC_KEY": os.environ.get("POLYMARKET_PUBLIC_KEY"),
        "POLYMARKET_SECRET_KEY": os.environ.get("POLYMARKET_SECRET_KEY"),
    }
    optional = {
        "TELEGRAM_KEY": os.environ.get("TELEGRAM_KEY"),
    }

    missing_required = [k for k, v in required.items() if not v]
    if missing_required:
        print(f"FAIL — Missing required secrets: {', '.join(missing_required)}")
        sys.exit(1)

    print("PASS — Polymarket credentials present.")

    tg = optional["TELEGRAM_KEY"]
    if tg:
        print("INFO — TELEGRAM_KEY present: Telegram notifications will be tested.")
    else:
        print("INFO — TELEGRAM_KEY absent: Telegram steps will be skipped (non-fatal).")

    print()
    return {**required, **optional}


def test_telegram(tg_token: str | None) -> None:
    print("=== [2] Telegram Connectivity ===")
    if not tg_token:
        print("SKIP — TELEGRAM_KEY not set. Skipping Telegram test.\n")
        return

    msg_id = send_telegram(
        tg_token,
        "👋 *Polymarket Bot — QA Test Started*\n\n"
        "✅ Polymarket credentials loaded\n"
        "✅ Telegram connectivity verified\n"
        "⏳ Fetching wallet balance and MLB positions...",
    )
    print(f"PASS — Telegram message delivered (id: {msg_id})\n")


def test_balance(client: PolymarketUS) -> str:
    print("=== [3] Wallet Balance ===")
    resp         = client.account.balances()
    balance_list = resp.get("balances", []) if isinstance(resp, dict) else []

    if not balance_list:
        print("WARN — No balances returned.")
        return "_(no balance data)_"

    lines = []
    for b in balance_list:
        currency      = b.get("currency",       "USD")
        current       = b.get("currentBalance", 0) or 0
        buying_power  = b.get("buyingPower",    0) or 0
        asset_notional= b.get("assetNotional",  0) or 0
        open_orders   = b.get("openOrders",     0) or 0
        lines.append(
            f"  • *{currency}*\n"
            f"    Cash balance:      `${current:.2f}`\n"
            f"    Buying power:      `${buying_power:.2f}`\n"
            f"    Position notional: `${asset_notional:.2f}`\n"
            f"    In open orders:    `${open_orders:.2f}`"
        )
        print(f"  {currency}: cash=${current:.2f}  buyingPower=${buying_power:.2f}  notional=${asset_notional:.2f}")

    print("PASS — Balance fetched.\n")
    return "\n".join(lines)


def test_mlb_positions(client: PolymarketUS) -> tuple[list, list]:
    print("=== [4] MLB Positions & World Series Contracts ===")

    pos_resp       = client.portfolio.positions()
    positions_dict = pos_resp.get("positions", {}) if isinstance(pos_resp, dict) else {}

    mlb_positions = []
    for token_id, pos in positions_dict.items():
        meta       = pos.get("marketMetadata", {}) or {}
        event_slug = meta.get("eventSlug",     "") or ""
        slug       = meta.get("slug",          "") or ""
        combined   = (event_slug + " " + slug).lower()

        if not any(kw in combined for kw in ["mlb", "world-series"]):
            continue

        qty       = float(pos.get("netPosition",  "0") or "0")
        cost_val  = float((pos.get("cost",      {}) or {}).get("value", "0") or "0")
        cash_val  = float((pos.get("cashValue", {}) or {}).get("value", "0") or "0")
        real_val  = float((pos.get("realized",  {}) or {}).get("value", "0") or "0")
        qty_avail = pos.get("qtyAvailable", "0") or "0"

        avg_price     = round(cost_val / qty, 4) if qty else 0
        cur_price     = round(cash_val / qty, 4) if qty else 0
        unrealized    = cash_val - cost_val

        mlb_positions.append({
            "token_id":      token_id,
            "slug":          slug,
            "event_slug":    event_slug,
            "title":         meta.get("title",   ""),
            "outcome":       meta.get("outcome", ""),
            "net_pos":       qty,
            "qty_available": qty_avail,
            "avg_price":     avg_price,
            "cur_price":     cur_price,
            "cost_val":      cost_val,
            "cash_val":      cash_val,
            "unrealized":    unrealized,
            "realized":      real_val,
        })
        print(
            f"  POSITION: {meta.get('title', slug)} | {meta.get('outcome', '')} | "
            f"qty={qty}  avg=${avg_price}  cur=${cur_price}  unrealized=${unrealized:.2f}"
        )

    if not mlb_positions:
        print("  (no open MLB positions found)")

    # Fetch World Series event contracts
    # retrieve_by_slug → {"event": {"markets": [{"slug", "outcome", ...}]}}
    print("\n  Fetching World Series event contracts (2025 + 2026)...")
    ws_markets = []
    for es in ["mlb-world-series-champion-2026", "mlb-world-series-champion-2025"]:
        try:
            event_resp = client.events.retrieve_by_slug(es)
            if not isinstance(event_resp, dict):
                print(f"  WARN — Unexpected type for {es}: {type(event_resp)}")
                continue
            event_obj   = event_resp.get("event", {}) or {}
            raw_markets = event_obj.get("markets", []) or []
            if not raw_markets:
                print(f"  INFO — {es}: no markets found (may be inactive/settled).")
                continue
            print(f"  {es}: {len(raw_markets)} team contract(s)")
            for m in raw_markets:
                mslug     = m.get("slug",      "")
                outcome   = m.get("outcome",   "") or m.get("title", "")
                active    = m.get("active",    False)
                liquidity = m.get("liquidity", 0)
                ws_markets.append({"slug": mslug, "outcome": outcome, "active": active, "liquidity": liquidity})
                print(f"    {'✓' if active else '✗'} {outcome:<32} slug: {mslug}  liq=${liquidity:.0f}")
        except Exception as exc:
            print(f"  WARN — Could not fetch {es}: {exc}")

    print("PASS — MLB positions and event contracts fetched.\n")
    return mlb_positions, ws_markets


def build_report(balance_text: str, mlb_positions: list, ws_markets: list) -> str:
    lines = ["📊 *Polymarket QA — Full Portfolio Report*\n",
             "💰 *Wallet Balance*", balance_text,
             "\n⚾ *Open MLB Positions*"]

    if mlb_positions:
        for p in mlb_positions:
            emoji = "🟢" if p["unrealized"] >= 0 else "🔴"
            lines.append(
                f"{emoji} *{p['title'] or p['slug']}*\n"
                f"   Outcome: `{p['outcome']}`\n"
                f"   Qty: `{p['net_pos']}` | Avg: `${p['avg_price']}` | Cur: `${p['cur_price']}`\n"
                f"   Unrealized P&L: `${p['unrealized']:.2f}` | Realized: `${p['realized']:.2f}`"
            )
    else:
        lines.append("_No open MLB positions._")

    if ws_markets:
        active = [m for m in ws_markets if m["active"]]
        lines.append(f"\n📋 *World Series Contracts ({len(ws_markets)} total, {len(active)} active)*")
        for m in ws_markets[:12]:
            lines.append(f"  {'✓' if m['active'] else '✗'} `{m['slug']}` — {m['outcome']}")
        if len(ws_markets) > 12:
            lines.append(f"  _...and {len(ws_markets) - 12} more_")

    lines.append("\n✅ *QA Test Complete.*")
    return "\n".join(lines)


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main() -> None:
    print("=" * 56)
    print("=== Polymarket Bot QA Test Suite ===")
    print("=" * 56 + "\n")

    secrets    = check_secrets()
    api_key    = secrets["POLYMARKET_PUBLIC_KEY"]
    secret_key = secrets["POLYMARKET_SECRET_KEY"]
    tg_token   = secrets.get("TELEGRAM_KEY")

    test_telegram(tg_token)

    client = PolymarketUS(key_id=api_key, secret_key=secret_key)
    try:
        balance_text             = test_balance(client)
        mlb_positions, ws_markets = test_mlb_positions(client)

        report = build_report(balance_text, mlb_positions, ws_markets)

        if tg_token:
            send_telegram(tg_token, report)
            print("PASS — Full report sent to Telegram.")
        else:
            print("INFO — Telegram not configured. Report (log only):")
            print(report)
    finally:
        client.close()

    print("\n=== All QA tests passed ===")


if __name__ == "__main__":
    main()
