# Polymarket Futures Bot

Automated scalping strategy and execution bot for Polymarket US Futures markets (MLB World Series focus).

---

## What it does

- **Take profit**: Monitors open positions every 15 minutes. When any MLB/World Series position doubles in value (current price ≥ 2× average buy price), it sells and queues a buyback.
- **Scheduled re-entry**: At 5:00–5:15 AM EST, it automatically re-buys queued positions with a $1.00 USD allocation at current bid price.
- **Telegram alerts**: Sends trade notifications to a Telegram channel on every sell and buyback.

---

## Setup

### 1. Fork or clone the repository

**Fork (recommended — lets you run your own GitHub Actions):**

1. Click **Fork** at the top-right of this page on GitHub.
2. In your fork, go to **Settings → Secrets and variables → Actions**.

**Clone (local development):**

```bash
git clone https://github.com/YOUR_USERNAME/polymarketfuturesbot.git
cd polymarketfuturesbot
pip install polymarket-us requests
```

---

### 2. Add your secrets

In your forked repo go to **Settings → Secrets and variables → Actions → New repository secret** and add the following three secrets:

| Secret name | Where to get it |
|---|---|
| `POLYMARKET_PUBLIC_KEY` | Your Polymarket US API key ID |
| `POLYMARKET_SECRET_KEY` | Your Polymarket US API secret key |
| `TELEGRAM_KEY` | Your Telegram bot token (from [@BotFather](https://t.me/BotFather)) |

#### Getting a Polymarket US API key

1. Log in to [Polymarket US](https://polymarket.com).
2. Go to **Account → API Keys** and generate a new key pair.
3. Copy the **Key ID** → `POLYMARKET_PUBLIC_KEY`
4. Copy the **Secret Key** → `POLYMARKET_SECRET_KEY`

#### Getting a Telegram bot token

1. Open Telegram and message [@BotFather](https://t.me/BotFather).
2. Send `/newbot` and follow the prompts.
3. Copy the token BotFather gives you → `TELEGRAM_KEY`
4. Make sure your bot is a member of `@moneyballpredictions` (or update `TELEGRAM_CHAT_ID` in `polymarket_bot.py` to your own channel).

---

### 3. Verify your setup with the QA test

Before the scheduled bot runs, confirm everything works:

1. Go to the **Actions** tab in your fork.
2. Select **"Polymarket QA — Secrets, Telegram & Wallet Balance"**.
3. Click **Run workflow → Run workflow**.

The test will:
- ✅ Validate all three secrets are present
- ✅ Send a greeting message to your Telegram channel
- ✅ Fetch your Polymarket US wallet balance and post it to Telegram

If the workflow passes, your secrets and connectivity are confirmed and the main bot is ready to go.

---

### 4. Enable the scheduled bot

The main workflow (`.github/workflows/polymarket_monitor.yml`) runs every 15 minutes automatically once you push to `main`. GitHub Actions must be enabled on your fork:

1. Go to **Actions** tab.
2. If prompted, click **"I understand my workflows, go ahead and enable them"**.

---

## Running locally

```bash
export POLYMARKET_PUBLIC_KEY="your_key_id"
export POLYMARKET_SECRET_KEY="your_secret_key"
export TELEGRAM_KEY="your_telegram_token"

# Run the main bot
python polymarket_bot.py

# Run the QA test suite
python test_bot.py
```

---

## File structure

```
polymarketfuturesbot/
├── polymarket_bot.py                        # Main execution bot
├── test_bot.py                              # QA test: secrets, Telegram, wallet balance
├── state.json                               # Auto-managed buyback queue (committed by CI)
└── .github/
    └── workflows/
        ├── polymarket_monitor.yml           # Scheduled bot (every 15 min)
        └── qa_test.yml                      # Manual QA test (workflow_dispatch only)
```

---

## Environment variables

| Variable | Required | Description |
|---|---|---|
| `POLYMARKET_PUBLIC_KEY` | Yes | Polymarket US API key ID |
| `POLYMARKET_SECRET_KEY` | Yes | Polymarket US API secret key |
| `TELEGRAM_KEY` | Yes | Telegram bot token |
