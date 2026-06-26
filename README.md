# Polymarket Futures Bot

Automated take-profit and re-entry bot for Polymarket US Futures markets (MLB World Series focus).

The bot runs continuously inside GitHub Actions as a long-lived loop (up to 5 h 45 min), then self-triggers the next run for uninterrupted 24/7 operation. It monitors open MLB positions every 15 minutes and closes any that have doubled in value, then re-enters automatically when the price returns to the original entry level.

---

## How it works

| Step | What happens |
|---|---|
| Every 15 min | Health check: verify API, log balance, scan positions |
| Take-profit | If `current_bid ≥ 2× avg_entry` → close entire position, queue buyback |
| Buyback | When price returns within 1 std dev of original entry → re-buy |
| 10 AM EST | Telegram daily report (balance change + all closed P&Ls) |
| 5 h 45 min | Self-trigger next GitHub Actions run, persist state, exit cleanly |

---

## Setup

### 1. Fork the repository

Click **Fork** at the top-right of this page. Your fork is where GitHub Actions will run.

### 2. Set your secrets

Go to **Settings → Secrets and variables → Actions → New repository secret**.

> Secrets must be added to the **`Polymarket US Futures` environment** (Settings → Environments → Polymarket US Futures), not the repository-level secrets tab.

#### Required (bot will not start without these)

| Secret | Description |
|---|---|
| `POLYMARKET_PUBLIC_KEY` | Your Polymarket US API key ID (UUID) |
| `POLYMARKET_SECRET_KEY` | Your Polymarket US API secret key (Ed25519, base64) |

Get these from [polymarket.us/developer](https://polymarket.us/developer) after completing identity verification in the Polymarket US app.

#### Optional — Telegram notifications

If these are absent, the bot runs silently (logs only). No error is raised.

| Secret | Description |
|---|---|
| `TELEGRAM_KEY` | Telegram bot token from [@BotFather](https://t.me/BotFather) |

The bot auto-detects whether `TELEGRAM_KEY` is set at startup and prints:
```
INFO   Telegram notifications: ENABLED (TELEGRAM_KEY found)
```
or
```
INFO   Telegram notifications: DISABLED (TELEGRAM_KEY not set — running silently)
```

No manual flag or config change is needed.

#### Optional — workflow self-trigger (recommended for 24/7 operation)

| Secret | Description |
|---|---|
| `GH_PAT` | GitHub Personal Access Token with `workflow` scope |

Without `GH_PAT`: the daily cron at `00:07 UTC` restarts the bot every 24 hours.
With `GH_PAT`: the bot self-triggers a new run at 5 h 45 min, keeping the chain unbroken indefinitely.

To create a PAT: GitHub → Settings → Developer settings → Personal access tokens → Fine-grained tokens → select `workflow` permission for this repository.

#### Example: minimal configuration (trading only, no notifications)

```
POLYMARKET_PUBLIC_KEY = <your key id>
POLYMARKET_SECRET_KEY = <your secret key>
```

#### Example: full configuration

```
POLYMARKET_PUBLIC_KEY = <your key id>
POLYMARKET_SECRET_KEY = <your secret key>
TELEGRAM_KEY          = <your telegram bot token>
GH_PAT                = <your github PAT>
```

---

### 3. Verify setup with the QA test

Before the live bot runs, confirm everything works:

1. Go to **Actions** tab → **"Polymarket QA — Secrets, Telegram & Wallet Balance"**
2. Click **Run workflow → Run workflow**

The test will:
- Verify Polymarket credentials and fetch your wallet balance
- If `TELEGRAM_KEY` is set: send a greeting + full portfolio report to Telegram
- If `TELEGRAM_KEY` is absent: print the report to the workflow log (still passes)
- List all MLB World Series team contracts (2025 + 2026)
- Show your open positions with unrealized P&L

---

### 4. Enable the scheduled bot

Go to the **Actions** tab and enable workflows if prompted. The bot starts automatically:

- **Daily cron** (`00:07 UTC`): safety-net restart
- **Self-trigger**: at 5 h 45 min the bot dispatches the next run itself (requires `GH_PAT`)

To start immediately: Actions → **"Polymarket Portfolio Execution Engine"** → **Run workflow**.

---

## Configuration

All strategy parameters are constants at the top of `polymarket_bot.py`:

| Constant | Default | Description |
|---|---|---|
| `TAKE_PROFIT_MULTIPLIER` | `2.0` | Close position when bid ≥ N× avg entry price |
| `BUYBACK_AMOUNT_USD` | `1.00` | USD allocated per re-entry order (fallback when qty unknown) |
| `BUYBACK_STD_DEVS` | `1` | Std-dev width for buyback trigger zone |
| `BUYBACK_STD_DEV_PCT` | `0.10` | One std dev = this fraction of avg entry (10% → ±10% zone at 1 dev) |
| `LOOP_INTERVAL_SECONDS` | `900` | Seconds between health checks (15 min) |
| `RUNTIME_LIMIT_SECONDS` | `20700` | Bot exits and self-triggers after this many seconds (5 h 45 min) |

---

## Buyback logic

When a position is sold at take-profit, the bot records the original average entry price and queued quantity. On every subsequent health check it fetches a live BBO and compares:

```
std_dev = avg_entry_price × BUYBACK_STD_DEV_PCT
zone    = [avg_entry_price − BUYBACK_STD_DEVS × std_dev,
           avg_entry_price + BUYBACK_STD_DEVS × std_dev]

→ execute buyback if current_bid is inside zone
```

Example with `avg_entry = $0.10`, 1 std dev, 10% width:
- Zone: `[$0.09, $0.11]`
- Buyback fires the moment the price trades back into this range

The previous time-based 5 AM buyback window has been removed.

---

## Workflow continuity

GitHub Actions runners terminate after 6 hours. The bot handles this gracefully:

1. At **5 h 45 min** it calls the GitHub API to dispatch a new `workflow_dispatch` run (requires `GH_PAT`)
2. The `concurrency: group` setting ensures at most one run is active or queued at any time
3. If `GH_PAT` is absent, the daily `00:07 UTC` cron provides an automatic restart
4. `state.json` is committed after every run, so no trading state is lost across restarts

---

## Running locally

```bash
git clone https://github.com/YOUR_USERNAME/polymarketfuturesbot.git
cd polymarketfuturesbot
pip install -r requirements.txt

# Minimal (no Telegram)
export POLYMARKET_PUBLIC_KEY="your_key_id"
export POLYMARKET_SECRET_KEY="your_secret_key"

# Full
export POLYMARKET_PUBLIC_KEY="your_key_id"
export POLYMARKET_SECRET_KEY="your_secret_key"
export TELEGRAM_KEY="your_telegram_token"
export GH_PAT="your_github_pat"

# Run the bot (loops forever, Ctrl+C to stop)
python polymarket_bot.py

# Run the QA test once
python test_bot.py
```

---

## File structure

```
polymarketfuturesbot/
├── polymarket_bot.py           # Main bot: health checks, take-profit, buyback, handoff
├── test_bot.py                 # QA test: credentials, balance, MLB positions
├── state.json                  # Persisted state: buybacks, balance history, closed positions
├── requirements.txt            # pip dependencies
└── .github/
    └── workflows/
        ├── polymarket_monitor.yml   # Continuous bot (5h45m loop, daily cron)
        └── qa_test.yml              # Manual QA test (workflow_dispatch only)
```

---

## Environment variables reference

| Variable | Required | Description |
|---|---|---|
| `POLYMARKET_PUBLIC_KEY` | **Yes** | Polymarket US API key ID |
| `POLYMARKET_SECRET_KEY` | **Yes** | Polymarket US API secret key |
| `TELEGRAM_KEY` | No | Telegram bot token — enables notifications if set |
| `GH_PAT` | No | GitHub PAT (`workflow` scope) — enables 24/7 self-triggering |
