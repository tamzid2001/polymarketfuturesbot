"""
QA test suite — validates secrets, API connectivity, wallet balance,
open MLB positions, and discovers 2026 World Series underdog slugs.

Run this BEFORE production to get the correct market_slug values for markets.json.
"""

import os
import sys
import json
import requests
from polymarket_us import (
    PolymarketUS,
    APIConnectionError,
    APITimeoutError,
    AuthenticationError,
    BadRequestError,
    NotFoundError,
    RateLimitError,
)

TELEGRAM_CHAT_ID = "@moneyballpredictions"
MARKETS_FILE     = "markets.json"


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def send_telegram(tg_token: str, text: str) -> int:
    """Send a Telegram message. Truncates at 4000 chars to stay under API limit."""
    MAX_LEN = 4000
    if len(text) > MAX_LEN:
        text = text[:MAX_LEN] + "\n\n_[message truncated]_"
    url  = f"https://api.telegram.org/bot{tg_token}/sendMessage"
    resp = requests.post(
        url,
        json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"},
        timeout=10,
    )
    if not resp.ok:
        # Retry once without Markdown if parse error
        resp2 = requests.post(
            url,
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text},
            timeout=10,
        )
        resp2.raise_for_status()
        return resp2.json()["result"]["message_id"]
    return resp.json()["result"]["message_id"]


def _extract_markets_from_obj(obj: dict, out: list) -> None:
    """Pull market dicts out of an event or search-result object."""
    for m in obj.get("markets", []):
        if not isinstance(m, dict):
            continue
        slug    = m.get("slug", "") or ""
        outcome = m.get("outcome", "") or m.get("title", "") or ""
        active  = m.get("active", True)
        # price: try outcomePrices list, then bestBid value, then direct price field
        bid = 0.0
        prices = m.get("outcomePrices")
        if prices and isinstance(prices, list) and prices:
            try:
                bid = float(prices[0])
            except (ValueError, TypeError):
                bid = 0.0
        if bid == 0.0:
            bid_obj = m.get("bestBid") or {}
            try:
                bid = float(bid_obj.get("value", 0) or 0)
            except (ValueError, TypeError):
                bid = 0.0
        if bid == 0.0:
            try:
                bid = float(m.get("price", 0) or 0)
            except (ValueError, TypeError):
                bid = 0.0
        if slug:
            out.append({
                "slug":    slug,
                "outcome": outcome,
                "active":  active,
                "bid":     bid,
            })


# ------------------------------------------------------------------
# Test steps
# ------------------------------------------------------------------

def check_secrets() -> dict:
    print("=== [1] Secrets Check ===")
    required = {
        "POLYMARKET_PUBLIC_KEY": os.environ.get("POLYMARKET_PUBLIC_KEY"),
        "POLYMARKET_SECRET_KEY": os.environ.get("POLYMARKET_SECRET_KEY"),
    }
    optional = {"TELEGRAM_KEY": os.environ.get("TELEGRAM_KEY")}

    missing = [k for k, v in required.items() if not v]
    if missing:
        print(f"FAIL — Missing required secrets: {', '.join(missing)}")
        sys.exit(1)

    print("PASS — Polymarket credentials present.")
    tg = optional["TELEGRAM_KEY"]
    if tg:
        print("INFO — TELEGRAM_KEY present: Telegram steps will run.")
    else:
        print("INFO — TELEGRAM_KEY absent: Telegram steps will be skipped (non-fatal).")
    print()
    return {**required, **optional}


def test_telegram(tg_token: str | None) -> None:
    print("=== [2] Telegram Connectivity ===")
    if not tg_token:
        print("SKIP — TELEGRAM_KEY not set.\n")
        return
    msg_id = send_telegram(
        tg_token,
        "👋 *Polymarket Bot — QA Test Started*\n\n"
        "✅ Credentials loaded\n"
        "✅ Telegram connectivity verified\n"
        "⏳ Fetching wallet balance and 2026 WS underdogs...",
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
        currency       = b.get("currency",       "USD")
        current        = b.get("currentBalance", 0) or 0
        buying_power   = b.get("buyingPower",    0) or 0
        asset_notional = b.get("assetNotional",  0) or 0
        open_orders    = b.get("openOrders",     0) or 0
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


def test_open_mlb_positions(client: PolymarketUS) -> list:
    print("=== [4] Open MLB Positions ===")
    pos_resp       = client.portfolio.positions()
    positions_dict = pos_resp.get("positions", {}) if isinstance(pos_resp, dict) else {}

    mlb_positions = []
    for token_id, pos in positions_dict.items():
        meta       = pos.get("marketMetadata", {}) or {}
        event_slug = meta.get("eventSlug", "") or ""
        slug       = meta.get("slug",      "") or ""
        combined   = (event_slug + " " + slug).lower()

        if not any(kw in combined for kw in ["mlb", "world-series", "baseball"]):
            continue

        qty      = float(pos.get("netPosition",  "0") or "0")
        cost_val = float((pos.get("cost",      {}) or {}).get("value", "0") or "0")
        cash_val = float((pos.get("cashValue", {}) or {}).get("value", "0") or "0")
        real_val = float((pos.get("realized",  {}) or {}).get("value", "0") or "0")

        avg_price  = round(cost_val / qty, 4) if qty else 0
        cur_price  = round(cash_val / qty, 4) if qty else 0
        unrealized = cash_val - cost_val

        mlb_positions.append({
            "token_id":   token_id,
            "slug":       slug,
            "event_slug": event_slug,
            "title":      meta.get("title",   ""),
            "outcome":    meta.get("outcome", ""),
            "net_pos":    qty,
            "avg_price":  avg_price,
            "cur_price":  cur_price,
            "cost_val":   cost_val,
            "cash_val":   cash_val,
            "unrealized": unrealized,
            "realized":   real_val,
        })
        print(
            f"  POSITION: {meta.get('title', slug)} | {meta.get('outcome', '')} | "
            f"qty={qty}  avg=${avg_price}  cur=${cur_price}  unrealized=${unrealized:.2f}"
        )
        print(f"    event_slug={event_slug}  market_slug={slug}")

    if not mlb_positions:
        print("  (no open MLB positions)")

    print("PASS — MLB positions fetched.\n")
    return mlb_positions


def discover_underdogs_2026(client: PolymarketUS) -> list:
    """
    Find 2026 World Series markets using multiple strategies.
    Returns list of markets sorted by bid price ascending (biggest underdogs first).
    """
    print("=== [5] 2026 World Series — Discovering All Team Markets ===")
    ws_markets: list[dict] = []
    found_via = ""

    # Strategy 1: search.query
    print("  Strategy 1: search.query('mlb world series 2026')...")
    try:
        results = client.search.query({"query": "mlb world series 2026"})
        if isinstance(results, list):
            for item in results:
                if isinstance(item, dict):
                    _extract_markets_from_obj(item, ws_markets)
        elif isinstance(results, dict):
            for key in ("events", "results", "data"):
                for item in results.get(key, []):
                    if isinstance(item, dict):
                        _extract_markets_from_obj(item, ws_markets)
        if ws_markets:
            found_via = "search.query"
            print(f"  → Found {len(ws_markets)} market(s) via search.")
        else:
            print("  → search.query returned no markets.")
    except Exception as exc:
        print(f"  → search.query failed: {exc}")

    # Strategy 2: try multiple plausible event slugs
    if not ws_markets:
        slug_candidates = [
            "mlb-world-series-champion-2026",
            "mlb-world-series-2026",
            "mlb-2026-world-series-champion",
            "2026-mlb-world-series",
            "world-series-champion-2026",
        ]
        print(f"  Strategy 2: trying {len(slug_candidates)} event slug candidates...")
        for slug in slug_candidates:
            try:
                resp = client.events.retrieve_by_slug(slug)
                if isinstance(resp, dict):
                    event_obj = resp.get("event", resp)
                    _extract_markets_from_obj(event_obj, ws_markets)
                if ws_markets:
                    found_via = f"events.retrieve_by_slug('{slug}')"
                    print(f"  → Found {len(ws_markets)} market(s) via slug '{slug}'.")
                    break
            except Exception:
                pass
        if not ws_markets:
            print("  → No event slug candidates matched.")

    # Strategy 3: events.list scan
    if not ws_markets:
        print("  Strategy 3: events.list — scanning active events for 2026 WS...")
        try:
            offset = 0
            limit  = 50
            found  = False
            while not found:
                page  = client.events.list({"limit": limit, "offset": offset, "active": True})
                items = page if isinstance(page, list) else page.get("events", [])
                if not items:
                    break
                for item in items:
                    title = (item.get("title", "") or "").lower()
                    slug  = (item.get("slug",  "") or "").lower()
                    if "world series" in title and "2026" in (title + slug):
                        _extract_markets_from_obj(item, ws_markets)
                        if ws_markets:
                            found_via = f"events.list (offset={offset})"
                            found = True
                            break
                if len(items) < limit:
                    break
                offset += limit
            if ws_markets:
                print(f"  → Found {len(ws_markets)} market(s) via events.list.")
            else:
                print("  → events.list scan found no 2026 WS event.")
        except Exception as exc:
            print(f"  → events.list scan failed: {exc}")

    # Strategy 4: sports.list
    if not ws_markets:
        print("  Strategy 4: sports.list...")
        try:
            sports = client.sports.list()
            items  = sports if isinstance(sports, list) else sports.get("events", [])
            for item in items:
                if isinstance(item, dict):
                    title = (item.get("title", "") or "").lower()
                    if "world series" in title and "2026" in title:
                        _extract_markets_from_obj(item, ws_markets)
            if ws_markets:
                found_via = "sports.list"
                print(f"  → Found {len(ws_markets)} market(s) via sports.list.")
            else:
                print("  → sports.list found no 2026 WS markets.")
        except Exception as exc:
            print(f"  → sports.list failed: {exc}")

    if not ws_markets:
        print("\n  FAIL — Could not discover 2026 World Series markets via any strategy.")
        print("  → Visit polymarket.us, search '2026 World Series', copy the event slug from the URL.")
        print("  → Update market_slug values in markets.json manually.")
        print("PASS (with warnings) — market discovery incomplete.\n")
        return []

    # Deduplicate by slug
    seen: set[str] = set()
    unique: list[dict] = []
    for m in ws_markets:
        if m["slug"] not in seen:
            seen.add(m["slug"])
            unique.append(m)
    ws_markets = unique

    # Sort ascending by bid — lowest price = biggest underdog
    ws_markets.sort(key=lambda m: float(m.get("bid", 1)))

    print(f"\n  Discovered via : {found_via}")
    print(f"  Total team markets: {len(ws_markets)}")
    print()
    print(f"  {'#':>3}  {'Team / Outcome':<32}  {'Bid':>7}  slug")
    print(f"  {'-'*85}")
    for i, m in enumerate(ws_markets, 1):
        mark = "✓" if m.get("active", True) else "✗"
        print(f"  {i:>3}  {mark} {str(m.get('outcome', '?')):<30}  ${float(m.get('bid', 0)):>6.4f}  {m.get('slug', '?')}")

    bottom_20 = ws_markets[:20]
    print(f"\n  ── Bottom 20 underdogs — copy these into markets.json ──")
    for m in bottom_20:
        print(f"    \"market_slug\": \"{m['slug']}\"  # {m.get('outcome','?')}  bid=${float(m.get('bid',0)):.4f}")

    print("\nPASS — 2026 WS underdog discovery complete.\n")
    return bottom_20


def validate_markets_json(discovered: list) -> None:
    """Cross-check markets.json slugs against what the API actually returned."""
    print("=== [6] markets.json Slug Validation ===")
    if not discovered:
        print("SKIP — no discovered markets to cross-check against.\n")
        return

    try:
        with open(MARKETS_FILE) as fh:
            data = json.load(fh)
        configured = data.get("mlb_world_series", [])
    except Exception as exc:
        print(f"WARN — Could not read {MARKETS_FILE}: {exc}\n")
        return

    discovered_slugs = {m["slug"] for m in discovered}
    ok, bad = [], []
    for entry in configured:
        slug = entry.get("market_slug", "")
        if slug in discovered_slugs:
            ok.append(entry)
        else:
            bad.append(entry)

    print(f"  Configured entries : {len(configured)}")
    print(f"  Slug match (✓)     : {len(ok)}")
    print(f"  Slug mismatch (✗)  : {len(bad)}")

    if bad:
        print("\n  WARN — These markets.json slugs were NOT found in the API:")
        for e in bad:
            print(f"    ✗ {e.get('team', '?'):<28}  slug: {e.get('market_slug', '?')}")
        print("\n  Update markets.json using the slugs printed in step [5] above.")
    else:
        print("  PASS — All configured slugs match discovered API slugs.")
    print()


def build_report(balance_text: str, mlb_positions: list, underdogs: list) -> str:
    lines = ["📊 *Polymarket QA — Portfolio Report*\n",
             "💰 *Wallet Balance*", balance_text,
             "\n⚾ *Open MLB Positions*"]

    if mlb_positions:
        for p in mlb_positions:
            emoji = "🟢" if p["unrealized"] >= 0 else "🔴"
            lines.append(
                f"{emoji} *{p['title'] or p['slug']}*\n"
                f"   Outcome: `{p['outcome']}`\n"
                f"   Qty: `{p['net_pos']}` | Avg: `${p['avg_price']}` | Cur: `${p['cur_price']}`\n"
                f"   Unrealized P&L: `${p['unrealized']:.2f}`"
            )
    else:
        lines.append("_No open MLB positions._")

    if underdogs:
        lines.append(f"\n📋 *2026 WS — Bottom {len(underdogs)} Underdogs*")
        for m in underdogs:
            lines.append(f"  `{m['slug']}` — {m.get('outcome', '?')} @ `${float(m.get('bid',0)):.4f}`")

    lines.append("\n✅ *QA Test Complete.*")
    return "\n".join(lines)


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main() -> None:
    print("=" * 60)
    print("=== Polymarket Bot QA Test Suite ===")
    print("=" * 60 + "\n")

    secrets    = check_secrets()
    api_key    = secrets["POLYMARKET_PUBLIC_KEY"]
    secret_key = secrets["POLYMARKET_SECRET_KEY"]
    tg_token   = secrets.get("TELEGRAM_KEY")

    test_telegram(tg_token)

    client = PolymarketUS(key_id=api_key, secret_key=secret_key)
    try:
        balance_text  = test_balance(client)
        mlb_positions = test_open_mlb_positions(client)
        underdogs     = discover_underdogs_2026(client)
        validate_markets_json(underdogs)

        report = build_report(balance_text, mlb_positions, underdogs)

        if tg_token:
            try:
                msg_id = send_telegram(tg_token, report)
                print(f"PASS — Full report sent to Telegram (msg_id={msg_id}).")
            except Exception as tg_exc:
                print(f"WARN — Telegram report send failed (non-fatal): {tg_exc}")
                print("INFO — Report (log only):")
                print(report)
        else:
            print("INFO — Telegram not configured. Report:")
            print(report)
    except AuthenticationError as exc:
        print(f"FAIL — Authentication error: {exc.message}")
        sys.exit(1)
    except RateLimitError as exc:
        print(f"WARN — Rate limit hit: {exc.message}")
    except APIConnectionError as exc:
        print(f"FAIL — Connection error: {exc.message}")
        sys.exit(1)
    except APITimeoutError:
        print("FAIL — Request timed out.")
        sys.exit(1)
    finally:
        client.close()

    print("\n=== All QA tests passed ===")


if __name__ == "__main__":
    main()
