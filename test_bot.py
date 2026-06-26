import os
import sys
import json
import requests
from polymarket_us import PolymarketUS

TELEGRAM_CHAT_ID = "@moneyballpredictions"

def check_secrets():
    required = {
        "POLYMARKET_PUBLIC_KEY": os.environ.get("POLYMARKET_PUBLIC_KEY"),
        "POLYMARKET_SECRET_KEY": os.environ.get("POLYMARKET_SECRET_KEY"),
        "TELEGRAM_KEY": os.environ.get("TELEGRAM_KEY"),
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        print(f"FAIL — Missing secrets: {', '.join(missing)}")
        sys.exit(1)
    print("PASS — All required secrets are present.")
    return required

def test_telegram(tg_token):
    print("\n--- Telegram Greeting Test ---")
    url = f"https://api.telegram.org/bot{tg_token}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": (
            "👋 *Polymarket Bot — QA Test Passed!*\n\n"
            "✅ Secrets loaded\n"
            "✅ Telegram connectivity verified\n"
            "✅ Polymarket wallet balance check running next..."
        ),
        "parse_mode": "Markdown",
    }
    resp = requests.post(url, json=payload, timeout=10)
    resp.raise_for_status()
    print(f"PASS — Telegram message delivered (message_id: {resp.json()['result']['message_id']})")

def test_portfolio_balance(api_key, secret_key, tg_token):
    print("\n--- Polymarket US Wallet Balance Test ---")
    client = PolymarketUS(key_id=api_key, secret_key=secret_key)

    balances = client.account.balances()
    print(f"Raw balances response: {balances}")

    # Normalise: SDK may return an object or a list
    if isinstance(balances, list):
        balance_lines = []
        total_usd = 0.0
        for b in balances:
            asset = getattr(b, 'asset', None) or (b.get('asset') if isinstance(b, dict) else 'UNKNOWN')
            amount = getattr(b, 'amount', None) or getattr(b, 'balance', None) or (b.get('amount') or b.get('balance') if isinstance(b, dict) else None)
            if amount is not None:
                try:
                    total_usd += float(amount)
                except ValueError:
                    pass
                balance_lines.append(f"  • {asset}: {amount}")
        balance_summary = "\n".join(balance_lines) if balance_lines else "  (no balances returned)"
        total_line = f"${total_usd:.2f} USD" if balance_lines else "N/A"
    else:
        # Single object
        amount = getattr(balances, 'amount', None) or getattr(balances, 'balance', None) or (balances.get('amount') or balances.get('balance') if isinstance(balances, dict) else None)
        balance_summary = f"  • Balance: {amount}"
        total_line = f"${float(amount):.2f} USD" if amount else "N/A"

    print(f"PASS — Wallet balance fetched:\n{balance_summary}")

    # Send balance report to Telegram
    url = f"https://api.telegram.org/bot{tg_token}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": (
            f"📊 *Polymarket Wallet Balance Report*\n\n"
            f"{balance_summary}\n\n"
            f"💰 *Total:* `{total_line}`"
        ),
        "parse_mode": "Markdown",
    }
    resp = requests.post(url, json=payload, timeout=10)
    resp.raise_for_status()
    print(f"PASS — Balance report sent to Telegram.")

def main():
    print("=== Polymarket Bot QA Test Suite ===\n")

    secrets = check_secrets()
    api_key = secrets["POLYMARKET_PUBLIC_KEY"]
    secret_key = secrets["POLYMARKET_SECRET_KEY"]
    tg_token = secrets["TELEGRAM_KEY"]

    test_telegram(tg_token)
    test_portfolio_balance(api_key, secret_key, tg_token)

    print("\n=== All QA tests passed ===")

if __name__ == "__main__":
    main()
