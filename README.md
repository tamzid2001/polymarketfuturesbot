# Polymarket Futures Bot + Kalshi BTC Prophet Bot

Two independent async trading bots that run 24/7 inside GitHub Actions:

1. **Polymarket Futures Bot** (`polymarket_bot.py`) — WebSocket-driven take-profit and re-entry bot for Polymarket US Futures markets (MLB World Series 2026 focus).
2. **Kalshi BTC 15-Min Prophet Bot** (`kalshibtc15minupordown.py`) — forecasts BTC price with Facebook Prophet, trades Kalshi's 15-minute BTC up/down contracts, and take-profits each position via a live P&L monitor once it is up by the bet amount. [Jump to docs ↓](#kalshi-btc-15-minute-prophet-bot)

Both run inside GitHub Actions for up to 5 h 45 min per job before self-triggering the next run for uninterrupted 24/7 operation.

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
├── polymarket_bot.py           # Polymarket bot: take-profit, buyback, WS handlers, handoff
├── test_bot.py                 # Polymarket QA: credentials, balance, positions, slugs
├── markets.json                # Polymarket source of truth: markets to track/enter
├── state.json                  # Polymarket persisted state
├── requirements.txt            # Polymarket pip dependencies
├── kalshibtc15minupordown.py   # Kalshi bot: Prophet 15-min BTC forecast strategy
├── test_kalshi_bot.py          # Kalshi QA: data, forecast, auth, order build (no orders)
├── requirements_kalshi.txt     # Kalshi pip dependencies (prophet, yfinance, ...)
├── trade_history.json          # Kalshi trade journal (committed back by the workflow)
├── traded_market_tickers.json  # Kalshi one-order-per-window dedupe store
└── .github/
    └── workflows/
        ├── polymarket_monitor.yml   # Polymarket continuous bot (5h45m loop, daily cron)
        ├── qa_test.yml              # Polymarket manual QA
        ├── kalshi_monitor.yml       # Kalshi continuous bot (5h45m loop, 6h cron)
        └── kalshi_qa.yml            # Kalshi manual QA (workflow_dispatch only)
```

---

## Environment variables reference (Polymarket)

| Variable | Required | Description |
|---|---|---|
| `POLYMARKET_PUBLIC_KEY` | **Yes** | Polymarket US API key ID |
| `POLYMARKET_SECRET_KEY` | **Yes** | Polymarket US API secret key |
| `TELEGRAM_KEY` | No | Telegram bot token — enables notifications if set |
| `GH_PAT` | No | GitHub PAT (`workflow` scope) — enables 24/7 self-triggering |

---
---

# Kalshi BTC 15-Minute Prophet Bot

`kalshibtc15minupordown.py` — fully async. Forecasts BTC 15 minutes ahead with
**Facebook Prophet** and trades Kalshi's `KXBTC15M` up/down contracts, exactly
**one entry per 15-minute window**, each managed by a live P&L monitor that
**takes profit once the position is up by the bet amount** (reduce-only IOC
limit at the locking price) or lets it ride to settlement.

> The previous version of this bot used an Alpaca price feed and a momentum
> signal (delta vs a rolling 60-second average). That strategy — and the Alpaca
> dependency — has been fully removed.

## Strategy

At the start of every 15-minute Kalshi window:

1. **Detect** the new active `KXBTC15M` market and its strike (`floor_strike`).
2. **Download** the latest **500 one-minute BTC-USD candles** from Yahoo Finance
   (BTC trades 24/7 — no weekday assumptions).
3. **Validate** the data: ≥500 rows, clean 1-minute spacing, not stale, and all
   `:00/:15/:30/:45` boundary candles present. Any failure → log a warning and
   **skip the window (no order)**.
4. **Forecast**: fit Prophet on `log(close)` (daily/weekly/yearly seasonality
   off, uncertainty sampling on) and predict 15 minutes ahead at
   `interval_width=0.80` — the **80% confidence interval**:

   | Band | Meaning |
   |---|---|
   | `p10` | lower bound of the 80% CI (`exp(yhat_lower)`) |
   | `p50` | median forecast (`exp(yhat)`) |
   | `p90` | upper bound of the 80% CI (`exp(yhat_upper)`) |

5. **Decide** (one order, never re-entered — deduped via
   `traded_market_tickers.json`, which survives restarts):

   ```
   current BTC close < p50   →  BUY YES  (UP)
   current BTC close > p50   →  BUY NO   (DOWN)
   ```

6. **Take-profit** — after the entry fills, a position monitor polls the open
   position's unrealized P&L from live WebSocket quotes every
   `POSITION_POLL_S` (5 s). The moment gains cross `TP_PROFIT_USD` (default:
   the bet amount — a $1 bet exits when the position is up $1), it fires a
   **reduce-only IOC limit** at the exit price that locks the gain
   (`entry + target/count` per contract). Kalshi only accepts `reduce_only`
   on IOC orders (GTC+reduce_only → `400 invalid_order`), so the exit never
   rests on the book — the monitor is the trigger: the market has already
   crossed the exit price when the order is sent, so it fills at
   exit-or-better; a miss cancels harmlessly and the monitor re-fires next
   tick. Partial fills accumulate and the remainder is re-fired. Positions
   closed **manually in the Kalshi app** are detected (live position check
   before every exit order) and booked at Kalshi's reported realized P&L.
   If the target is never reached, the position rides to settlement.
7. **Log everything**: BTC close vs strike vs p50, the 80% CI bands, the
   interpolated percentile of the current price within the forecast
   distribution, and a live P&L line for the open position every 5 s.
8. **Settle**: a background task polls settled markets and records WIN/LOSS +
   P&L into `trade_history.json`. Contracts the take-profit already closed
   realize at their exit price; only the unfilled remainder settles at the
   market result.

## Performance tracking

Every portfolio report (every 30 s) prints the full stats block from
`trade_history.json`: total trades, wins/losses, win rate, total/average
return, largest win/loss, current + longest win/loss streaks, and max drawdown
from the equity curve — plus an **exit breakdown** showing how many positions
were closed by the **take-profit limit** vs held to **settlement** (and, when
they occur, partial take-profits and manual/external closes), each with its
cumulative P&L. Every trade records its `exit_method`
(`take_profit` / `settlement` / `take_profit_partial+settlement` /
`closed_externally`), and the Last Trade panel shows which path closed it.
Both JSON state files are **committed back to the repo by the workflow after
every run**, so statistics accumulate across the 5 h 45 m restart chain.

## Setup

Secrets live in the **`Kalshi` environment** (Settings → Environments → Kalshi):

| Secret | Description |
|---|---|
| `KALSHI_PROD_API_KEY` | Kalshi API key ID |
| `KALSHI_PRIVATE_KEY` | RSA private key (PEM contents) |
| `GH_PAT` | GitHub PAT (`workflow` scope) for the 5 h 45 m self-trigger |

Environment **variables** (not secrets) tune behavior:

| Variable | Default | Description |
|---|---|---|
| `DRY_RUN` | `true` | **Live-money switch.** `false` → real orders |
| `BET_AMOUNT_USD` | `1` | Dollars spent per order — fractional contracts at 0.01 granularity (`count = $ / price`, e.g. $1 at a $0.40 price buys 2.50 contracts) |
| `TP_PROFIT_USD` | `= BET_AMOUNT_USD` | **Take-profit target** in dollars — close the position once unrealized gains cross this amount (reduce-only IOC limit at the locking price) |
| `POSITION_POLL_S` | `5` | Open-position P&L monitor cadence (seconds) |
| `HISTORY_MINUTES` | `500` | 1-minute candles fed to Prophet |
| `FORECAST_MINUTES` | `15` | Forecast horizon |
| `UNCERTAINTY_SAMPLES` | `1000` | Prophet uncertainty samples (80% CI) |

**One-run overrides** — the **Run workflow** dialog on `kalshi_monitor.yml`
accepts three optional inputs that override the variables **for that dispatch
only**: `dry_run`, `bet_amount_usd`, `tp_profit_usd`. The 5 h 45 m handoff
re-dispatches with no inputs, so the chain reverts to the variables above.

## QA before launch

Actions → **"Kalshi QA — Secrets, BTC Data, Prophet Forecast, Kalshi WS & Order
Build"** → Run workflow.

The suite runs 14 checks against live credentials — Kalshi auth/balance, a real
yfinance download + validation, a real Prophet fit with band sanity checks,
order construction, the tracker round-trip, and the take-profit exit-price
math + P&L-monitor trigger (including the stats exit breakdown) — and
**force-overrides DRY_RUN in-process so it can never submit an order**. Exit
code is non-zero on any critical failure.

## Workflow continuity (Kalshi)

`kalshi_monitor.yml` mirrors the Polymarket chain:

1. Bot exits cleanly at **5 h 45 min** (`RUNTIME_LIMIT_MIN=345`).
2. **Persist State** commits `trade_history.json` + `traded_market_tickers.json`
   back to `main` (`[skip ci]`) — runs even if the bot crashed.
3. **Re-trigger** dispatches the next run via `GH_PAT`; the fresh checkout
   already contains the persisted state, so a restart mid-window can never
   double-trade (the dedupe store blocks it) and pending trades are settled by
   the next run.
4. The `kalshi-bot-singleton` concurrency group keeps at most one run active.
5. If a run fails, the every-6-hours cron (`11 */6 * * *`) restarts the chain.

## Running locally

```bash
pip install -r requirements_kalshi.txt
export KALSHI_API_KEY_ID="your_key_id"
export KALSHI_PEM_PATH="kalshi_private_key.pem"
export DRY_RUN=true            # flip to false only when you mean it

python test_kalshi_bot.py      # QA first — never submits an order
python kalshibtc15minupordown.py
```
