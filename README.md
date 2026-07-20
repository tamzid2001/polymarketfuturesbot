# Polymarket Futures Bot + Kalshi BTC Bot

The repository contains the original futures and Prophet runners, plus two separately controlled mechanical average-down strategies:

1. **Polymarket Futures Bot** (`polymarket_bot.py`) — WebSocket-driven take-profit and re-entry bot for Polymarket US Futures markets (MLB World Series 2026 focus).
2. **Kalshi BTC 15-Min Prophet Bot** (`kalshibtc15minupordown.py`) — pre-forecasts BTC two minutes before each new Kalshi market opens with a fixed 17-minute horizon, then compares the cached p50 with the live strike immediately at the open. [Jump to docs ↓](#kalshi-btc-15-minute-prophet-bot)
3. **Kalshi BTC 15-Min Mechanical Average-Down Bot** (`kalshi_btc15m_average_down.py`) — continuous KXBTC15M-only runner with no forecast or ML. It starts a persisted two-sided watcher at each new market opening, waits as long as needed for one side to reach 40¢ or lower, then holds only that side's 40¢/30¢/20¢/10¢ economic ladder to settlement. [Jump to docs ↓](#kalshi-btc-15-minute-mechanical-average-down-runner)
4. **Polymarket US MLB Mechanical Average-Down Bot** (`polymarket_mlb_average_down.py`) — manual-only, disabled-by-default runner for same-day MLB full-game moneylines. It takes no baseball prediction: it snapshots both team costs, waits for the first team to trade 10¢ below its own snapshot, then buys that side and posts only lower 10¢ rungs.

The two continuous bots run inside GitHub Actions for up to 5 h 45 min per job before self-triggering the next run for uninterrupted 24/7 operation.

The mechanical MLB runner is intentionally excluded from that automatic schedule: it starts only from its own manual workflow and requires an explicit live-order confirmation.

---

## Kalshi BTC 15-minute mechanical average-down runner

Use **Actions → “Kalshi BTC 15m Mechanical Average Down” → Run workflow**. It is live by default, uses the persisted contract quantity (default **1 contract per rung**), and runs for 5 h 45 min before handing its state to the next runner. It monitors only `KXBTC15M`; it does not use ML, Prophet, a forecast, indicators, or an opposite-side hedge.

### Exact Kalshi lifecycle

1. **Start one watcher at a fresh opening.** When a KXBTC15M market opens, the runner persists a `watching` record for that ticker. A short start grace absorbs normal discovery/handoff delay, but it is not an entry deadline. If a runner first discovers an already-old market, it skips that market rather than starting a late watcher. A saved watcher continues through the next Actions handoff.
2. **Watch both executable asks for the whole market.** The watcher keeps reading the YES and NO asks until one reaches `≤ $0.40` or the market closes. It may therefore wait much longer than 15 seconds. If neither side reaches 40¢, it submits no order and records a no-signal market.
3. **Choose exactly one side, once.** The first qualifying update selects one side. If both qualify in the same update, the lower ask wins; an exact tie chooses YES. The watcher immediately stops; the bot does **not** submit both sides first, so there is no opposite-side order to cancel.
4. **Fill locks the side.** Only an actual initial fill locks the market. A zero-fill protected immediate order is recorded as unfilled and releases the capital/market slot. Once YES is locked, NO is never submitted for that ticker; once NO is locked, YES is never submitted.
5. **Place the lower same-side ladder.** After the initial fill, it posts only strictly lower limits on the locked side. A 40¢ fill produces 30¢, 20¢, and 10¢ rungs; a 39¢ fill produces 30¢, 20¢, and 10¢; a 10¢ fill creates no lower rung. It never averages up, reverses, or hedges. The default cap is four contracts per market: one contract at each rung.
6. **Hold to settlement.** Unfilled lower limits have the market close as their expiry and are explicitly canceled when the market closes. Filled contracts remain through settlement; then the runner records payout, fees, net P&L, streaks, drawdown, and per-rung results. A closed prior market cannot block the next fresh watcher.

Key persisted settings in `kalshi_btc15m_average_down_config.json` are `initial_position_size` (contracts per rung), `max_total_capital`, `max_active_markets`, and `watch_start_grace_seconds` (default 45; only for starting a watcher at a fresh open).

---

## Mechanical MLB average-down runner

Use **Actions → “Polymarket US MLB Mechanical Average Down” → Run workflow**. Its default is a dry run; real orders require setting `live_trading` to true in that specific manual dispatch.

For every eligible MLB full-game moneyline starting that day in New York time, the runner records the first executable home and away costs. A snapshot of `$0.80` for one team and `$0.20` for the other produces entry limits of `$0.70` and `$0.10`, respectively. It excludes first-five markets, totals, spreads, props, and futures.

### Exact MLB lifecycle

1. **Snapshot both teams.** Before the game starts, record the executable home and away outcome costs. The entry target for each is exactly 10¢ below its own snapshot.
2. **Wait for the first trigger.** The runner polls the executable asks. Whichever team first reaches its target is selected; if both are first observed in the same poll, the larger discount wins, with home used only as the final tie-breaker. It does not submit an order for the other team.
3. **Fill locks the outcome.** A filled IOC limit locks that home or away outcome. The other team is abandoned: there is no hedge, flip, reverse, prediction, or ML filter.
4. **Average only down.** The actual initial fill becomes the starting point. The runner submits only lower same-team limits, in 10¢ steps, subject to the configured contract and capital caps. It never creates a rung at or above the actual fill.
5. **At game start, stop orders.** It cancels every unfilled entry/ladder order and will not place another one. Filled contracts are not actively sold: they remain for the exchange's game settlement. The report is an entry-and-ladder audit, not a realized-P&L exit strategy.

Example: home 80¢ / away 20¢ at baseline gives home `≤70¢` and away `≤10¢` triggers. If home reaches 70¢ first and fills, only home can receive lower 60¢/50¢/... rungs; away receives no order.

Polymarket's API price is always the LONG/YES price, so buying the short/NO team at a 10¢ outcome cost is correctly submitted as a 90¢ API price. The runner stores both prices in its state and logs them on every submission.

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
├── kalshi_btc15m_backtest.py   # Read-only historical KXBTC15M Prophet + ML backtest
├── test_kalshi_backtest.py     # Offline tests for the historical backtest helpers
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

`kalshibtc15minupordown.py` — fully async. Two minutes before each new
`KXBTC15M` market opens (for example, `xx:43` before the `xx:45` open), it
creates a fixed **17-step** BTC forecast with **Facebook Prophet**. As soon as
the market opens, it compares the forecast p50 with the live Kalshi strike and
places exactly **one BTC entry per 15-minute window**, sized in **shares**
(`BET_AMOUNT_SHARES`, default 2 contracts — NOT dollars). A settled BTC-primary
loss activates the next BTC entry's BTC/ETH hedge protocol: paired shares start
at `ARBITRAGE_SHARES` (default 10). They scale by `LOSS_MULTIPLIER` only when
the preceding paired BTC trade also lost **and** zero ETH shares filled; a BTC
win resets the escalation. Immediately after the BTC fill, the bot
submits a resting opposite-side limit in the matching `KXETH15M` market, with
an expiry at settlement, that keeps BTC entry + ETH hedge at 90 cents or less.
Positions ride to settlement; there is no take-profit monitor.

> The previous version of this bot used an Alpaca price feed and a momentum
> signal (delta vs a rolling 60-second average). That strategy — and the Alpaca
> dependency — has been fully removed.

## Strategy

For every 15-minute Kalshi window:

1. **Pre-compute** the upcoming market forecast two minutes before the opening
   (`PREOPEN_FORECAST_LEAD_S`, default 120 seconds).
2. **Download** the latest **500 one-minute BTC-USD candles** from Yahoo Finance
   (BTC trades 24/7 — no weekday assumptions).
3. **Validate** the data: ≥500 rows, clean 1-minute spacing, not stale, and all
   `:00/:15/:30/:45` boundary candles present. Any failure → log a warning and
   **skip the window (no order)**.
4. **Forecast to settlement**: fit Prophet on `log(close)` (daily/weekly/yearly
   seasonality off, uncertainty sampling on) and predict exactly **17**
   one-minute timesteps forward from the cached forecast point. The forecast
   uses `interval_width=0.80` —
   the **80% confidence interval**:

   | Band | Meaning |
   |---|---|
   | `p10` | lower bound of the 80% CI (`exp(yhat_lower)`) |
   | `p50` | median forecast (`exp(yhat)`) |
   | `p90` | upper bound of the 80% CI (`exp(yhat_upper)`) |

5. **At market open**, resolve the newly-live `KXBTC15M` market and its strike
   (`floor_strike`), then immediately use the cached forecast to **decide**.
   A missing cache skips the market rather than running a slow live forecast
   (one order, never re-entered — deduped via
   `traded_market_tickers.json`, which survives restarts):

   ```
   forecast p50 > live strike   →  BUY YES  (UP)
   forecast p50 < live strike   →  BUY NO   (DOWN)
   ```

   If the preceding BTC market's result arrives just after the opening, the
   bot does not delay or skip the live entry. It reconciles that same BTC
   record as soon as the loss is published, topping it up to the required pair
   size and then submitting the matched ETH limit.

6. **ETH hedge after a settled BTC loss** — the next BTC entry uses the
   `ARBITRAGE_SHARES` base of 10 contracts. It multiplies that paired amount
   only when the prior paired BTC bet lost and zero ETH shares filled. A BTC
   profit clears escalation, and normal BTC-only bets always stay at
   `BET_AMOUNT_SHARES`. Immediately after BTC fills, the bot submits a
   settlement-expiring limit in the matching `KXETH15M` ticker for the
   opposite ETH side, at a price that keeps paired cost at or below `$0.90`.
   Example: BTC YES fills at `$0.60`; ETH NO target is
   `1 - 0.60 - 0.10 = $0.30`. For BTC NO, the bot submits ETH YES the same way.
7. **Log everything**: forecast-time BTC close vs strike vs p50, p50 vs the
   live strike, the 80% CI bands, the interpolated percentile of both the
   forecast-time close and the live strike within the forecast distribution,
   and ETH hedge submission/fill lines when hedge mode is active.
8. **Settle**: a background task polls settled markets and records WIN/LOSS +
   P&L into `trade_history.json`. BTC-primary and ETH-hedge fills settle as
   independent records.

## Performance tracking

Every portfolio report (every 30 s) prints the full stats block from
`trade_history.json`: total trades, wins/losses, win rate, total/average
return, largest win/loss, current + longest win/loss streaks, and max drawdown
from the equity curve. It also prints a separate **BTC-primary** win/loss and
streak block, so ETH hedge outcomes cannot obscure the BTC sequence controlling
the next pair size, plus a leg breakdown with BTC-primary vs ETH-hedge P&L. Every trade records its
`trade_kind` (`BTC_PRIMARY` / `ETH_HEDGE`) and settlement result.
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
| `BET_AMOUNT_SHARES` | `2` | BTC-only contracts (**shares — NOT dollars**) bought per order; this base is never multiplied |
| `ARBITRAGE_SHARES` | `10` | Base paired BTC/ETH hedge contracts used after a settled BTC loss |
| `LOSS_MULTIPLIER` | `2` | Multiplies `ARBITRAGE_SHARES` only after a paired BTC loss where zero ETH shares filled |
| `ETH_HEDGE_POLL_S` | `5` | ETH order-fill reconciliation cadence (seconds) |
| `HISTORY_MINUTES` | `500` | 1-minute candles fed to Prophet |
| `FORECAST_MINUTES` | `17` | Fixed Prophet horizon in one-minute timesteps for every cached forecast |
| `PREOPEN_FORECAST_LEAD_S` | `120` | Seconds before the next market opens to pre-compute its 17-step forecast |
| `OPEN_TRADE_GRACE_S` | `15` | Max seconds after a market opens to place the entry. Every manual, scheduled, and handoff run skips an older market and waits for the next live opening. |
| `SETTLE_CHECK_S` | `2` | Settlement polling cadence (seconds), independent of the opening order path |
| `UNCERTAINTY_SAMPLES` | `1000` | Prophet uncertainty samples (80% CI) |

**One-run overrides** — the **Run workflow** dialog on `kalshi_monitor.yml`
accepts four optional inputs: `dry_run`, `bet_amount_shares`,
`arbitrage_shares`, `loss_multiplier`. The 5 h 45 m handoff re-dispatches with
the effective values so manual overrides persist through the chained runs;
scheduled fallback runs use the GitHub environment variables/defaults above.

## QA before launch

Actions → **"Kalshi QA — Secrets, BTC Data, Prophet Forecast, Kalshi WS & Order
Build"** → Run workflow.

The suite runs checks against live credentials — Kalshi auth/balance, a real
yfinance download + validation, a real Prophet fit with band sanity checks,
order construction, the tracker round-trip, and the ETH hedge price/side math —
and **force-overrides DRY_RUN in-process so it can never submit an order**.
Exit code is non-zero on any critical failure.

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
   A restart may begin mid-window, but the bot only enters a market with Kalshi
   status `active` during its first `OPEN_TRADE_GRACE_S` seconds; otherwise it
   pre-forecasts and waits for the next opening.

## Running locally

```bash
pip install -r requirements_kalshi.txt
export KALSHI_API_KEY_ID="your_key_id"
export KALSHI_PEM_PATH="kalshi_private_key.pem"
export DRY_RUN=true            # flip to false only when you mean it

python test_kalshi_bot.py      # QA first — never submits an order
python kalshibtc15minupordown.py
```

### Historical KXBTC15M Backtest

`kalshi_btc15m_backtest.py` is separate from the live bot and never creates
orders. It paginates Kalshi's current and archived settled `KXBTC15M` markets,
writes every closed market and its outcome to
`backtest_output/closed_kxbtc15m_markets.csv`, then replays the live
two-minute-pre-open / 500-candle / 17-minute Prophet decision.

It also evaluates an expanding-window logistic-regression classifier using
only data that existed at each forecast time: BTC one-minute price features,
the Prophet output, and earlier *settled* Kalshi outcomes. The previous market
is excluded until its settlement timestamp, which prevents future-outcome
leakage. The report is directional accuracy and calibration only; it does not
claim dollar P&L because historical result records do not provide executable
opening fills, spreads, or fees.

```bash
pip install -r requirements_kalshi.txt
python test_kalshi_backtest.py
python kalshi_btc15m_backtest.py --output-dir backtest_output
```

The manual **Kalshi BTC 15-min Historical Backtest** GitHub Action runs the
same test and full replay, then uploads these artifacts:

- `closed_kxbtc15m_markets.csv`
- `prophet_ml_backtest_rows.csv`
- `skipped_markets.csv`
- `summary.json` and `summary.md`

It also prints running Prophet and walk-forward ML metrics to the Actions log
every 100 predictions by default, including the current win/loss streak and
the longest win/loss streak. Set the workflow's `metrics_every` input to
change that cadence.

The final summary also ranks directional win/loss rate by Eastern-Time hour,
weekday, weekday-hour, and 15-minute market-open slot. It excludes groups with
fewer than 100 observations by default; use `time_group_min_samples` to set a
different threshold.

### Ledger Statistical Analysis

`kalshi_ledger_analysis.py` analyzes either a real ledger with the columns
`trade_number`, date/time, market, side, entry/exit price, `profit_loss`, and
WIN/LOSS result, or a downloaded Kalshi backtest artifact. It produces separate
reports for Prophet and ML signals, including chronological ML tests, rolling
time series, streak conditional tests, Prophet cutoff tests, Monte Carlo, and
per-market/per-side summaries. It also writes P90/P99 prior-loss-streak
walk-forward tests: each 100-trade training prefix selects the high-loss state
and scores the following non-overlapping 100 trades. Every selected loss run is
exported with start/end time, elapsed duration, and trade-count buckets.

```bash
pip install -r requirements_kalshi_ledger_analysis.txt
python test_ledger_analysis.py
python kalshi_ledger_analysis.py \
  --input ~/Desktop/kalshi-btc15m-backtest-29698207476 \
  --signal all \
  --output-dir ledger_analysis_output
```

The **Kalshi Ledger Statistical Analysis** GitHub Action downloads a specified
backtest artifact and uploads the full analysis as an artifact. It treats a
backtest outcome-only artifact as directional research: monetary P&L, Kelly,
and dollar Monte Carlo remain unavailable until a ledger includes actual fills
and realized `profit_loss`.

### Streak ML Backtest

`kalshi_streak_ml_backtest.py` evaluates whether pre-trade features can predict
the current trade result, an all-win next-three-trade run, or an all-loss
next-three-trade run. It uses expanding chronological test blocks and reports
accuracy, precision, recall, F1, ROC-AUC, PR-AUC, Brier score, log loss, and
top-decile precision for Logistic Regression, KNN, Decision Tree, Random
Forest, Extra Trees, XGBoost when available, and an equal-weight soft-voting
probability ensemble. The **Kalshi Streak ML Backtest** GitHub Action runs this
against a specified historical artifact and uploads its predictions and metrics.
