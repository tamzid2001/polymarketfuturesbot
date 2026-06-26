# Polymarket Futures Bot

Async WebSocket-driven take-profit and re-entry bot for Polymarket US Futures markets (MLB World Series 2026 focus).

The bot connects to Polymarket's WebSocket feeds on startup, reacts to every price update in real time, and runs inside GitHub Actions for up to 5 h 45 min before self-triggering the next run for uninterrupted 24/7 operation.

---

## How it works

| Event | What happens |
|---|---|
| Startup | Fetch live portfolio; open $1 positions in every `is_underdog: true` market not already held |
| Price update (WS) | Check each tracked market's bid against take-profit threshold and buyback zone |
| Take-profit | `bid ≥ 3× avg_entry` → close 100% of position, queue a buyback |
| Buyback | `bid` returns within ±1 std dev of original entry price → re-enter same quantity |
| Every 60 s | Persist `state.json` to disk |
| Every 15 min | Log status: balance, open positions, pending buybacks, runtime remaining |
| 10 AM EST | Telegram daily report (balance Δ + all closed P&Ls) |
| 5 h 45 min | Self-trigger next GitHub Actions run, save state, exit cleanly |

---

## Setup

### 1. Fork the repository

Click **Fork** at the top-right of this page. Your fork is where GitHub Actions will run.

### 2. Set your secrets

Go to **Settings → Environments → Polymarket US Futures → Add secret**.

> All secrets must be in the **`Polymarket US Futures` environment**, not the repository-level secrets tab.

#### Required

| Secret | Description |
|---|---|
| `POLYMARKET_PUBLIC_KEY` | Your Polymarket US API key ID (UUID) |
| `POLYMARKET_SECRET_KEY` | Your Polymarket US API secret key (Ed25519, base64) |

Get these from [polymarket.us/developer](https://polymarket.us/developer) after completing identity verification in the Polymarket US app.

#### Optional — Telegram notifications

| Secret | Description |
|---|---|
| `TELEGRAM_KEY` | Telegram bot token from [@BotFather](https://t.me/BotFather) |

If absent the bot runs silently (logs only). The bot auto-detects the key at startup:
```
INFO   Telegram notifications: ENABLED (TELEGRAM_KEY found)
INFO   Telegram notifications: DISABLED (TELEGRAM_KEY not set — running silently)
```

#### Optional — workflow self-trigger (recommended for 24/7 operation)

| Secret | Description |
|---|---|
| `GH_PAT` | GitHub Personal Access Token with `workflow` scope |

Without `GH_PAT`: the daily cron at `00:07 UTC` restarts the bot every 24 hours.
With `GH_PAT`: the bot self-triggers a new run at 5 h 45 min, keeping the chain unbroken indefinitely.

To create a PAT: GitHub → Settings → Developer settings → Personal access tokens → Fine-grained tokens → select `workflow` permission for this repository.

---

### 3. Verify setup with the QA test

Before running the production bot:

1. Go to **Actions → "Polymarket QA — Secrets, Telegram & Wallet Balance"**
2. Click **Run workflow**

The test runs 6 steps:
1. Confirms credentials are present
2. Sends a Telegram greeting (if `TELEGRAM_KEY` is set)
3. Fetches and logs your wallet balance
4. Lists all open MLB positions with P&L
5. Discovers all 30 2026 World Series team markets via `search.query`, sorted by price — prints exact slugs
6. Cross-checks `markets.json` entries against live API slugs (PASS / WARN)

If step 6 shows mismatches, copy the slugs from step 5 into `markets.json` and re-run.

---

### 4. Configure `markets.json`

`markets.json` is the single source of truth for which markets the bot tracks and enters.

```json
{
  "settings": {
    "initial_deployment_usd": 1.00
  },
  "mlb_world_series": [
    {
      "team": "Colorado Rockies",
      "event_slug": "mlb-champ-2026-09-27",
      "market_slug": "tec-mlb-champ-2026-09-27-col",
      "is_underdog": true,
      "slug_verified": true,
      "max_deployment_usd": 1.00
    }
  ]
}
```

| Field | Description |
|---|---|
| `event_slug` | The Polymarket event identifier (parent of all team contracts) |
| `market_slug` | The specific team contract slug — must match the API exactly |
| `is_underdog` | `true` → bot opens a $1 position at startup if not already held. `false` → bot monitors the market but **never opens a position** |
| `slug_verified` | `true` once you confirm the slug matches what the QA test step [5] returns |
| `max_deployment_usd` | Capital for the initial entry order. Overrides `settings.initial_deployment_usd` |

**The bot will never open a position for any market where `is_underdog` is `false`.** Those entries exist only so you can add them to the WS subscription for monitoring, or flip the flag later without editing code.

---

### 5. Start the production bot

Actions → **"Polymarket Portfolio Execution Engine"** → **Run workflow**.

The bot also starts automatically:
- **Daily cron** (`00:07 UTC`): safety-net restart
- **Self-trigger**: at 5 h 45 min, if `GH_PAT` is configured

---

## Strategy configuration

All tunable constants are at the top of `polymarket_bot.py`:

| Constant | Default | Description |
|---|---|---|
| `TAKE_PROFIT_MULTIPLIER` | `3.0` | Close when `bid ≥ N × avg_entry` |
| `BUYBACK_AMOUNT_USD` | `1.00` | USD per re-entry when original quantity is unknown |
| `BUYBACK_STD_DEVS` | `1` | Std-dev half-width of the buyback trigger zone |
| `BUYBACK_STD_DEV_PCT` | `0.10` | Fallback std dev = 10% of entry price (used when < 3 trades in history) |
| `PRICE_HISTORY_WINDOW` | `30` | Rolling trade count used to compute live std dev |
| `RUNTIME_LIMIT_SECONDS` | `20700` | 5 h 45 min — bot exits and self-triggers before 6 h GitHub runner limit |
| `STATUS_LOG_INTERVAL_S` | `900` | Log status every 15 min |
| `STATE_SAVE_INTERVAL_S` | `60` | Persist `state.json` every 60 s |

---

## Take-profit logic

```
threshold = avg_entry_price × TAKE_PROFIT_MULTIPLIER   (default: 3×)

if current_bid >= threshold:
    close_position(market_slug)   # closes 100% via orders.close_position()
    queue_buyback(avg_entry, qty_sold)
```

---

## Buyback logic

When a position is sold at take-profit, the bot records the original average entry price and the closing std dev. On every subsequent price update it checks:

```
std_dev = rolling population std dev of last 30 trades (from WS trade events)
        = avg_entry × BUYBACK_STD_DEV_PCT   if fewer than 3 trades in history

zone = [avg_entry − BUYBACK_STD_DEVS × std_dev,
        avg_entry + BUYBACK_STD_DEVS × std_dev]

if current_bid is inside zone:
    re-enter same quantity at current bid
```

Example with `avg_entry = $0.01`, 1 std dev, 10% fallback:
- Zone: `[$0.009, $0.011]`
- Buyback fires the moment the bid returns into this range

---

## Workflow continuity

GitHub Actions runners terminate after 6 hours. The bot handles this gracefully:

1. At **5 h 45 min** it calls the GitHub API to dispatch a new `workflow_dispatch` run (requires `GH_PAT`)
2. The `concurrency: group: polymarket-bot-singleton` setting ensures at most one run is active at a time
3. If `GH_PAT` is absent, the daily `00:07 UTC` cron provides an automatic restart
4. `state.json` is committed to the repo after every run — pending buybacks and balance history survive restarts

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

# Run the bot (WebSocket loop, Ctrl+C to stop)
python polymarket_bot.py

# Run the QA test once
python test_bot.py
```

---

## File structure

```
polymarketfuturesbot/
├── polymarket_bot.py           # Async WebSocket bot: take-profit, buyback, WS handlers, handoff
├── test_bot.py                 # QA test: credentials, balance, positions, slug discovery
├── markets.json                # Source of truth: which markets to track and enter
├── state.json                  # Persisted state: pending buybacks, balance history, closed P&Ls
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
