# Polymarket Futures Bot + Kalshi BTC Bot

The repository contains the original futures and Prophet runners, plus two separately controlled mechanical average-down strategies:

1. **Polymarket Futures Bot** (`polymarket_bot.py`) — WebSocket-driven take-profit and re-entry bot for Polymarket US Futures markets (MLB World Series 2026 focus).
2. **Kalshi BTC 15-Min Prophet BTC-Only Ladder** (`kalshibtc15minupordown.py`, alias `kalshi_btc15m_prophet_btc_only.py`) — pre-forecasts BTC two minutes before each new Kalshi market opens, locks one forecast-selected side, and pre-posts that side's fixed 40¢/30¢/20¢/10¢ GTC ladder. It contains no ETH contract, hedge, multiplier, or loss-progression path. [Jump to docs ↓](#kalshi-btc-15-minute-prophet-bot)
3. **Kalshi BTC 15-Min ML-Side Mechanical Average-Down Bot** (`kalshi_btc15m_average_down.py`) — continuous KXBTC15M-only runner. A stored ML inference selects YES or NO before opening; when the market opens, it immediately posts that side's 40¢/30¢/20¢/10¢ economic GTC ladder through settlement. [Jump to docs ↓](#kalshi-btc-15-minute-mechanical-average-down-runner)
4. **Polymarket US MLB Average-Down Bot** (`polymarket_mlb_average_down.py`) — continuous dry-monitoring runner for same-day MLB full-game moneylines. Its default `mechanical` mode takes no baseball prediction: it snapshots both team costs, waits for the first team to trade 10¢ below its own snapshot, and records the resulting mechanical ladder audit. An explicitly configured `ml_side_average_down` mode is available only after the separate leakage-safe research pipeline produces a versioned model artifact; it freezes one ML-selected team and never substitutes or reverses to the other team. Scheduled runs cannot place orders; a separate manual switch permits a one-off live run.

The continuous runners use GitHub Actions for up to 5 h 45 min per job before self-triggering the next run. The Kalshi ML-side runner also checkpoints material execution state, recovers owned exchange orders at startup, and has a five-minute watchdog for interrupted runs. The Polymarket MLB mechanical runner is now a 24/7 **dry-monitoring** chain: scheduled and handoff jobs are explicitly dry, while live trading remains a separate manual choice.

---

## Kalshi BTC 15-minute mechanical average-down runner

Use **Actions → “Kalshi BTC 15m ML-Side Average Down” → Run workflow**. It uses the persisted contract quantity (default **0.01 contract per rung**) and monitors only `KXBTC15M`. Its separate inverse ML shadow uses **1.00 paper contract per rung** for readable counterfactual P&L; that value is stored independently and cannot change, reserve capital for, or submit a primary live order. The ML-only runner accepts only the schema `ml_only_raw_candles_settled_outcomes_v1`: BTC candle returns/volatility/range, strike distance, clock, and previously settled outcomes. It does not fit, call, or consume Prophet or another price forecast. It rejects a saved model whose schema is not exactly ML-only; there is no fallback model or forecast path.

For an isolated non-trading monitor, use **Actions → “Kalshi BTC Hold-Gate Trailing Paper Reports”**. It runs normal and inverse studies for both ML and Prophet with `DRY_RUN=true`, separate state/history files, and no primary ladder. Each study freezes its model side before open and continuously records actual fresh YES and NO bid/ask prices and displayed depth. It fills only the frozen selected side at 1×40¢/2×30¢/3×20¢/4×10¢, then compares 17 independent paper holds: trails that arm after +1¢ through +9¢, then +10¢/+20¢/…/+80¢ over that study’s actual average filled entry. Every variant has an **absolute 5¢ selected-side stop**—it does not subtract 5¢ from the average. Once a variant arms, its 10¢ trail stays armed through settlement unless it exits on a later retracement. It never assumes a stop fill. Its default 5h45m run queues the next paper-only handoff, so the monitor continues 24/7. Material changes are checkpointed to `paper_reports/` on `main` while the Action is still running and uploaded as final artifacts. It never touches the live trader’s state, orders, or concurrency group.

It also runs a separate **ML ladder scalp range shadow** at one paper share per rung. This alternative fills only against a fresh displayed executable ask and tracks the actual average filled entry of the 40¢/30¢/20¢/10¢ rungs. Rather than stopping at a fixed 1¢ exit, it leaves the paper position through settlement and records the maximum later fresh executable bid with enough displayed depth for the whole position, plus whether **1¢, 2¢, 3¢, 5¢, or 10¢ above average filled entry** was available. It is research-only: it never creates, cancels, reduces, or otherwise changes a live Kalshi order. These are gross quote opportunities—not executable profit claims—because fees, queue position, latency, cancellations, hidden liquidity, and price impact remain excluded.

The ML and Prophet Actions run four independent weighted paper studies: normal ML, inverse ML, normal Prophet, and inverse Prophet. Each locks its side before the market opens: normal uses the prediction and inverse uses its exact opposite; later quotes cannot switch either side. Each ladder buys **1 contract at 40¢, 2 at 30¢, 3 at 20¢, and 4 at 10¢** (10 maximum), using the actual filled average as its reference. For each model/side, the ledger compares 17 trailing-arm gates: +1¢ through +9¢, then +10¢/+20¢/…/+80¢. It records both YES and NO top-of-book prices, exits only at an observed full-depth selected-side bid at or below **5¢**, and otherwise arms a 10¢ retracement trail once the gate is reached; that trail remains armed until exit or settlement. These are separate paper-only ledgers: they cannot submit, close, cancel, reserve, or change a live order. Every compact report includes an ordered P&L/equity time series, actual book evidence, entry fills, exit evidence, current streak, and longest winning/losing streak.

### Deployed model, coverage, and evidence

The live runner resolves its artifact from [`kalshi_ml_model_registry.json`](kalshi_ml_model_registry.json) at every 5h45m handoff. The active model is a regularized logistic regression with isotonic probability calibration, trained and published by the ML-only daily retraining workflow. Its schema lock prevents a model with any non-ML-only feature from being substituted. The runner records the artifact ID, training cutoff, model `p_yes`, confidence, and selected side with every ML-backed market record.

When a retrain replaces the active artifact, the registry retains the predecessor artifact. The runner using the new model downloads both and scores them on each **same frozen pre-open feature vector**. Its state/report records both probabilities and sides, YES→NO/NO→YES changes, agreement rate, and—once settled—the directional result for each model. It also starts a separate 1-share, paper-only 40¢/30¢/20¢/10¢ ladder for **both** the retained predecessor and new model. Each requires the same fresh executable top-of-book and displayed-depth rule, records its own quote evidence, and reports each model’s settled directional result, paper P&L, and new-minus-old difference. These are independent hypotheticals: no transition shadow submits, modifies, reserves capital for, or competes with a live order; fees, queue priority, cancellations, and hidden liquidity remain excluded. This is prospective transition monitoring, not a promotion test or executable-P&L claim.

The `confidence >= 0.50` gate has **100% valid-model-direction coverage**: every valid binary-model score has a YES or NO direction at that threshold. For a freshly discovered, active market with sufficient capital, the runner immediately attempts all four GTC limits on that one ML-selected side. This is still not 100% fill coverage: each rung may remain unfilled, partially fill, be rejected, or be canceled at market close.

The earlier 52.42% / 18,986-score result used a Prophet-feature schema and is **not evidence for this ML-only model**. In an earlier ML-only final untouched 2,879-market test, the 50% gate scored 51.27% (full coverage; p=0.18): this was not statistically significant. The newest daily-retrain artifact uses a separate chronological test (2,895 markets): its pre-selected 55% confidence subset was 234/423 = 55.32% (p=0.032). That is statistically significant at the conventional 5% level for that one directional test, but it is not an executable-P&L result and is not the live 50% coverage policy. The production runner deliberately uses `confidence >= 0.50` so every valid score produces a side; its live P&L and 50%-gate directional performance must be measured prospectively with fills and fees.

### Exact ML-only features

The classifier receives exactly these 16 values, all fixed using information available before the market opens:

| Group | Features | Meaning |
| --- | --- | --- |
| Strike and momentum | `spot_vs_strike_bps`, `return_1m_bps`, `return_5m_bps`, `return_15m_bps`, `return_60m_bps` | Latest BTC spot relative to Kalshi's strike, plus BTC returns over 1, 5, 15, and 60 minutes, in basis points. |
| Volatility and range | `vol_15m_bps`, `vol_60m_bps`, `range_15m_bps` | Standard deviation of one-minute log returns over 15/60 minutes and the 15-minute high-to-low range. |
| Settled-outcome history | `lag_outcome_1`, `lag_outcome_2`, `lag_outcome_4`, `lag_outcome_8`, `known_yes_rate_8`, `known_outcome_count` | Only earlier **settled** KXBTC15M YES/NO outcomes: four lags, the last-eight YES rate, and the number of known settled rows. Missing early lags use a neutral 0.5 value. |
| Time of day | `hour_sin`, `hour_cos` | UTC market-open time encoded cyclically, so times near midnight remain close together. |

The feature builder requires at least 61 continuous one-minute BTC candles. It uses no Kalshi quote or order-book, Prophet output, future candle, un-settled outcome, or price forecast as a model input.

### What the Action logs

At startup the runner prints `ML MODEL`, `ML VALIDATION`, and `ML EXECUTION POLICY` lines with the exact artifact, calibration method, training rows/cutoff, schema, and active gate. Before each market it prints `ML INPUT READY` to confirm no forecast input was used, then `ML SIDE READY` (`p_yes`, confidence, selected side). A candle fetch is capped at 45 seconds and an unfinished pre-open task logs `ML SIDE FAILED` at the open; neither condition can fall back to Prophet, stale inference, or price-side selection. The runner then logs `SIDE LOCKED`, four `GTC LADDER LIMIT` submissions, order IDs/fills, exchange-position guards, settlement, rung P&L, the equal-share `ML LADDER SCALP RANGE`, and separate normal/inverse `WEIGHTED TRAILING` reports. The weighted reports show each held 40¢/33.33¢/26.67¢/20¢ average-cost state, maximum excursion, all requested target-opportunity rates, the paper trailing-stop result, current W/L streak, and longest W/L streak; they remain paper-only and exclude fees, queue priority, cancellation, latency, hidden liquidity, and partial-fill risk.

### ML-only retraining

**“Kalshi BTC 15m ML-Only Daily Retrain”** runs daily at 00:32 UTC and can also be started manually. It downloads the active ledger, appends only newly settled, feature-complete BTC 15-minute rows, reruns chronological validation, and stores the ledger, trained logistic/isotonic model, cadence audit, and validation reports in one 90-day artifact. Scheduled runs publish the validated artifact to the registry; a manual run stores a candidate unless `publish_model` is selected. A live runner never changes model mid-market: it adopts the newly published artifact only when its next 5h45m job starts.

Daily is the operational cadence, not six-hour retraining. The ML-only expanding-window cadence audit compared static, 6-hour, 12-hour, daily, 3-day, weekly, and 14-day refits with each fit restricted to outcomes settled before its prediction boundary. The final untouched 3,860-market, 50%-gate results were:

| Refit cadence | Correct calls | Directional rate | Exact binomial p vs. 50% |
| --- | ---: | ---: | ---: |
| Every 6 hours | 1,981 / 3,860 | 51.32% | 0.104 |
| Every 12 hours | 1,992 / 3,860 | 51.61% | 0.0477* |
| **Daily** | **1,989 / 3,860** | **51.53%** | **0.0597** |
| Weekly | 1,976 / 3,860 | 51.19% | 0.143 |
| Static | 1,927 / 3,860 | 49.92% | not significant |

The 12-hour run made only three more correct calls than daily. Its paired comparison with daily has p=0.784, so there is no evidence that its slight lead is a real cadence advantage rather than ordinary sampling noise. Six-hour retraining also adds cost and operational churn without an observed improvement. Daily is therefore the conservative operational default.

### What “statistically significant” means here

For a directional result, the null hypothesis is that a model has a 50% chance of choosing the correct YES/NO side. The exact binomial p-value is the chance of seeing a result at least this far from 50% if that null hypothesis were true. The conventional `p < 0.05` threshold is called *statistically significant*; it is a screening convention, not a guarantee of a trading edge.

The 12-hour result is nominally below 0.05 when compared with a coin flip alone (`p=0.0477`, marked `*`), but it was one of several cadence/gate experiments and did **not** significantly outperform daily (`paired p=0.784`). It is therefore not sound evidence to replace daily with 12-hour retraining. Daily's 51.53% is just above the threshold (`p=0.0597`), so it is **not** statistically significant by that convention. “Not significant” does not prove that daily is useless or exactly 50%; it means this test does not provide strong enough evidence to rule out random variation. Conversely, a significant directional result does not prove profitability: executable entry price, depth, partial fills, fees, slippage, correlated losses, and the 40/30/20/10 averaging rule are outside these directional tests.

### Exact Kalshi lifecycle

1. **Freeze one ML side before opening.** During the two-minute pre-open window, the runner obtains a bounded BTC candle snapshot and builds only raw-candle, strike, clock, and prior-settled-outcome features. It evaluates the schema-locked calibrated model and records `p_yes`, confidence, selected YES/NO side, exact model run, and training cutoff. The inclusive `≥50%` gate gives every valid binary-model direction coverage; if inference is late or unavailable, it submits no order. There is no mechanical, Prophet, forecast, stale-score, or price-side fallback.
2. **Start only at a fresh opening.** When that KXBTC15M market opens, the runner persists a `watching` record. A 45-second start grace absorbs normal discovery/handoff delay; it is not a price-entry window. If a runner first discovers an already-old market, it skips that market rather than sending a late ladder. A saved watcher continues through the next Actions handoff.
3. **Lock the frozen ML side before sending any order.** Once a valid ML YES/NO side is ready, the other side is never considered. The exchange-position guard must confirm a compatible, within-cap position and the account must cover the complete ladder principal plus configured fee reserve.
4. **Immediately pre-post four GTC limits.** For that one locked side, the runner sends exactly one GTC buy at each fixed economic price: **40¢, 30¢, 20¢, and 10¢**. Every order has the market's explicit close timestamp as its expiry. It sends no opposite-side order, does not reverse, and has no quote-trigger wait.
5. **Fill behavior is intentional.** If the selected side is already cheap enough that one or more limits cross the book, those GTCs may fill immediately at their limit or a better available price. Otherwise they provide liquidity and rest in Kalshi's book. A partial fill remains attached to that same rung; an unfilled rung remains resting until close, rejection, or explicit cancellation.
6. **Hold to settlement and clear the live ladder.** At market close the runner stops new orders and explicitly cancels all remaining GTC rungs. Filled contracts remain through settlement; then the runner records payout, fees, net P&L, streaks, drawdown, and per-rung results. The separate paper scalp shadow may instead record a qualifying bid exit, but it cannot affect this live lifecycle. A closed prior market cannot block the next fresh watcher.

### Pre-posted GTC ladder semantics

The runner uses Kalshi `good_till_canceled` with an **explicit market-close expiry**. It therefore does not rely on a GTC order surviving into the next 15-minute market: the bot also cancels any remaining owned orders after the market closes.

Pre-posting is intentional. If the frozen ML side opens around 20¢, the 40¢, 30¢, and 20¢ GTC buys can all be marketable and may fill immediately; that is the specified fixed 40/30/20/10 ladder, not a reversal or an accidental duplicate. The complete four-rung principal is reserved before any order is submitted, and the exchange-position guard rejects a ticker with an incompatible side, unexpected position, or position above the configured four-rung cap.

Key persisted settings in `kalshi_btc15m_average_down_config.json` are `initial_position_size` (contracts per rung), `max_total_capital`, `max_active_markets`, and `watch_start_grace_seconds` (default 45; only for starting a watcher at a fresh open). When you change only `initial_position_size` in **Run workflow**, the full ladder scales automatically: `10` shares per rung becomes a `40`-contract per-market ceiling and a `$10` principal cap (before fees). Those values persist into later handoffs. You can still supply explicit caps in the same form if you intentionally want a stricter limit.

### 24/7 handoff, recovery, and Kalshi pauses

The normal chain is: run for 5 h 45 min → persist state/config/report → queue the next live run. Only one live ML runner may execute or queue at once. Existing market-close-expiring GTC rungs stay on Kalshi during the short handoff; the successor restores the active ticker's frozen ML side and reconciles the original orders instead of sending a second ladder.

In addition to the end-of-run commit, the runner publishes a durable checkpoint after a **material** event: watcher creation, frozen ML side, accepted/rejected ladder change, fill/cancel status change, or settlement. High-frequency quote and lookup timestamps do not create commits. On every live startup it asks Kalshi for resting orders with this bot's deterministic client IDs, attaches known 40¢/30¢/20¢/10¢ rungs to the ledger, and checks the exchange position. A mixed-side, malformed, or position-mismatched recovery is quarantined: that ticker receives no new order.

The **“Kalshi BTC 15m Live Trader Watchdog”** workflow runs every five minutes. It dispatches a replacement only when there is no active or queued live runner and allows ten minutes for a normally successful self-handoff to appear. This is a recovery safeguard, not a second trader.

Kalshi has a scheduled trading pause every Thursday from **3:00–5:00 AM ET**. During it, the runner sends no new ladder; existing GTC orders are left unchanged (Kalshi's default is to keep them resting) and its WebSocket reconnects after a disconnect. It does not turn a pause into a late entry: if trading resumes after a new market's 45-second opening grace, that market is skipped and the next fresh market starts normally. An unscheduled API pause response uses the same fail-safe behavior. See [Kalshi maintenance and pauses](https://docs.kalshi.com/getting_started/maintenance_and_pauses).

---

## Mechanical MLB average-down runner

The **“Polymarket US MLB Mechanical Average Down”** workflow now runs 24/7 dry monitoring: it runs for 5 h 45 min, saves its state and report, then queues the next dry run. The six-hour schedule is a recovery path if a handoff fails. Scheduled and handoff runs hard-code `LIVE_TRADING=false` and cannot submit an order.

Use **Actions → “Polymarket US MLB Mechanical Average Down” → Run workflow** for a manual run. Its default is dry. Setting `live_trading` to true is an explicit **one-off** live run; it does not self-handoff, so it cannot silently become continuous live trading.

For every eligible MLB full-game moneyline starting that day in New York time, the runner records the first executable home and away costs. A snapshot of `$0.80` for one team and `$0.20` for the other produces entry limits of `$0.70` and `$0.10`, respectively. It excludes first-five markets, totals, spreads, props, and futures.

### Exact MLB lifecycle

1. **Snapshot both teams.** Before the game starts, record the executable home and away outcome costs. The entry target for each is exactly 10¢ below its own snapshot.
2. **Wait for the first trigger.** The runner polls the executable asks. Whichever team first reaches its target is selected; if both are first observed in the same poll, the larger discount wins, with home used only as the final tie-breaker. It does not submit an order for the other team.
3. **Fill locks the outcome.** A filled IOC limit locks that home or away outcome. The other team is abandoned: there is no hedge, flip, reverse, prediction, or ML filter.
4. **Average only down.** The actual initial fill becomes the starting point. The runner submits only lower same-team limits, in 10¢ steps, subject to the configured contract and capital caps. It never creates a rung at or above the actual fill.
5. **At game start, stop orders.** It cancels every unfilled entry/ladder order and will not place another one. Filled contracts are not actively sold: they remain for the exchange's game settlement. The report is an entry-and-ladder audit, not a realized-P&L exit strategy.

Example: home 80¢ / away 20¢ at baseline gives home `≤70¢` and away `≤10¢` triggers. If home reaches 70¢ first and fills, only home can receive lower 60¢/50¢/... rungs; away receives no order.

Polymarket's API price is always the LONG/YES price, so buying the short/NO team at a 10¢ outcome cost is correctly submitted as a 90¢ API price. The runner stores both prices in its state and logs them on every submission.

### Historical MLB ML backtest and optional ML-side mode

`polymarket_mlb_ml_backtest.py` is a distinct, read-only research pipeline. It uses the official Polymarket US public gateway for completed MLB events/settlements, the current Polymarket Exchange reporting endpoint for historical trade statistics, and the MLB Stats API for final game records and strictly prior team features. It does not use the 15 observed dry-run games as an ML sample.

The reporting service currently requires `POLYMARKET_REPORT_JWT`, an Auth0 bearer token with the `read:reports` scope. This is intentionally separate from the live runner's Ed25519 API credentials. The collector uses the current `POST https://api.prod.polymarketexchange.com/v1/report/trades/stats` camelCase schema; it records the legacy `/v1beta1` snake_case documentation/schema discrepancy in `dataset_summary.json` rather than silently mixing formats. If this token or the required scope is absent, the run reports the exact access failure and produces no ML accuracy or P&L claim.

Run it locally after installing its isolated dependencies:

```bash
pip install -r requirements_mlb_ml_backtest.txt
python polymarket_mlb_ml_backtest.py run-all --root . --refresh
```

Or use **Actions → “Polymarket US MLB Historical ML Backtest”**. That workflow is read-only: it has no order call, no live-trading input, and uploads raw API responses, exclusions, feature rows, predictions, and reports as a 90-day artifact.

The backtest separately evaluates the 24-hour, 6-hour, and 1-hour pre-game cutoffs. It accepts only full-game two-team moneylines, checks Polymarket settlement against an MLB final, records every exclusion, and only uses a candle whose timestamp is at or before the cutoff. Its chronological expanding folds and latest 20% untouched holdout never use a random split. Training preprocessing and calibration are fitted only on their preceding chronological data.

It compares market implied probability, market favorite, always-home, historical home rate, price-only logistic regression, market-feature logistic regression, gradient boosting, and a market-plus-prior-team model. Reported prediction metrics include accuracy, confidence interval, log loss, Brier score, ROC-AUC, calibration error, and confusion matrix. A higher accuracy alone is **not** a market edge: the report compares every model to the market baseline on the same untouched games.

Historical trade candles are not executable bid/ask quotes. The trading simulator therefore never converts a candle close or midpoint into a fill. It reports a no-trade result unless historical ask price, liquidity, fee assumption, and position-capacity information are present. This means a directional result may be valid while P&L remains unavailable; it is not evidence of profitability.

If the scoped reporting token is not available, run the separately labelled public-data fallback instead:

```bash
python polymarket_mlb_ml_backtest.py team-only --root . --team-start 2024-03-01
```

It uses only completed MLB Stats API games and strictly prior rolling team/Elo features. It has chronological folds and a final untouched holdout, but it has **no Polymarket price comparison, no market-edge conclusion, and no trading simulation**. Its report explicitly includes an exact paired comparison with the always-home baseline so a small accuracy increase is not misrepresented as a meaningful improvement.

The optional `ml_side_average_down` runner mode remains disabled by default:

```json
{
  "strategy_mode": "mechanical",
  "ml_model_path": "",
  "ml_min_confidence": 0.5
}
```

To use a successfully validated artifact in a future **dry run**, set `strategy_mode` to `ml_side_average_down` and point `ml_model_path` at that artifact. The runner loads no Prophet model and has no fallback side: once it stores the ML-selected home or away outcome, only that outcome can trigger the initial 10¢ discount order and all later rungs stay on that same team through settlement. Missing/unreadable artifacts, unavailable features, or failed inference place no order.

When that ML dry mode is enabled, `ml_inverse_shadow_enabled` (default `true`) also creates a separate, paper-only shadow for the *opposite* team. It mirrors the inverse team's own baseline, 10¢ trigger, and lower rungs, but never calls the order API or changes the ML-selected ladder. Each shadow hit records the fresh BBO-derived outcome ask and timestamp used; because this BBO endpoint supplies no displayed depth, a hit is explicitly a quote observation—not an exchange fill or executable-P&L claim. After settlement, the report separately shows inverse directional accuracy and clearly labelled illustrative quote-hit results. The default mechanical workflow has no model artifact and therefore creates no ML or inverse-shadow selections.

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
| `POLYMARKET_REPORT_JWT` | Auth0 bearer token with `read:reports`, required only for the read-only MLB historical trade-statistics backtest |

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
├── kalshibtc15minupordown.py   # Kalshi bot: Prophet BTC-only locked GTC ladder
├── kalshi_btc15m_prophet_btc_only.py # Explicit BTC-only entry-point alias
├── test_kalshi_bot.py          # Static safety checks: locked side, GTC rungs, no active ETH/multiplier path
├── kalshi_btc15m_backtest.py   # Read-only historical KXBTC15M Prophet + ML backtest
├── test_kalshi_backtest.py     # Offline tests for the historical backtest helpers
├── requirements_kalshi.txt     # Kalshi pip dependencies (prophet, yfinance, ...)
├── prophet_btc_only_trade_history.json          # Prophet BTC-only ladder journal, when created
├── prophet_btc_only_traded_market_tickers.json  # Prophet BTC-only dedupe store, when created
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

`kalshibtc15minupordown.py` — fully async. The explicit
`kalshi_btc15m_prophet_btc_only.py` alias has the same behavior. Two minutes
before each new `KXBTC15M` market opens, it creates a fixed **17-step** BTC
forecast with **Facebook Prophet**. At the fresh market open it compares the
forecast p50 with the live strike, locks exactly one BTC side, then immediately
pre-posts four market-close-expiring GTC limits at **40¢, 30¢, 20¢, and 10¢
economic cost** on that one side. Every rung uses `BET_AMOUNT_SHARES` (default
**1 contract**, not dollars). In paper mode it also keeps independent inverse,
selector, and normal-side scalp paper portfolios. It has no ETH contract,
hedge, multiplier, or loss-progression rule. The primary ladder rides to
settlement; the scalp portfolio is a separate paper-only average-entry exit
audit and cannot affect a primary order.

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
   (one locked side, never reversed — deduped via
   `prophet_btc_only_traded_market_tickers.json`, which survives restarts):

   ```
   forecast p50 > live strike   →  BUY YES  (UP)
   forecast p50 < live strike   →  BUY NO   (DOWN)
   ```

   The default paper workflow keeps this as the normal Prophet baseline and
   additionally freezes a selector side before each market. The selector
   compares only prior paired settled outcomes in trailing **3, 5, 7, 10, 25,
   and 50** signal windows. Each window votes for its higher directional
   win-rate side; the majority wins. The first newly deployed selector market,
   and any tied vote, choose **inverse**; later markets use the frozen vote and
   never change side after the market opens.


6. **Pre-post the locked ladder** — submit one `good_till_canceled` order at
   each fixed economic cost `$0.40`, `$0.30`, `$0.20`, and `$0.10`, each with
   the market's explicit close as expiry. YES uses the matching YES bid. NO is
   represented by an equivalent YES ask (`NO 40¢` appears as `YES sell 60¢` in
   Kalshi's dashboard); economically it is still the locked NO contract, not a
   hedge or reversal.
7. **Log everything**: forecast-time BTC close vs strike vs p50, the locked
   side, all four GTC order IDs/fills, and the fixed rung costs. The current
   market has no ETH-related log or order.
8. **Settle**: at close the runner explicitly cancels remaining owned GTC
   orders, refreshes the four fill counts, and records WIN/LOSS plus P&L only
   for filled BTC contracts. A fully unfilled ladder is recorded as
   `UNFILLED`, not a loss.

## Performance tracking

Every portfolio report (every 30 s) prints the normal BTC-only stats block, the
independent inverse paper report, a **BTC PROPHET LADDER SCALP RANGE** report,
separate normal/inverse **BTC PROPHET WEIGHTED TRAILING** reports, and a
**BTC PROPHET WIN-RATE SELECTOR** report. The range report
requires a fresh complete executable ask for each paper entry and a fresh bid
with depth for the entire paper position before counting its full later
maximum favorable excursion or a 1¢/2¢/3¢/5¢/10¢ target opportunity; it breaks
out the 40¢/35¢/30¢/25¢ average-entry profiles. Each weighted report locks its
normal or inverse Prophet side before open, uses 1×40¢/2×30¢/3×20¢/4×10¢, tracks
1¢–10¢ and 20¢–60¢ gross opportunities, and exits its paper position only after
a full-depth 10¢ trailing retracement. The selector report includes frozen normal/inverse choices,
directional and executable-quote paper P&L, drawdown, per-rung P&L, and one
line for every requested window showing normal/inverse W/L, win rate, and the
current leader. Its durable files are `prophet_btc_selector_history.json` and
`prophet_btc_selector_report.json`.

The inverse and selector report JSON files now include a durable
`pnl_time_series` for every settled paper market. Each point records the
ticker/time, source and selected side, result, filled contracts, entry cost,
gross $1-per-winning-contract settlement payout, net P&L, cumulative cost and
payout, cumulative P&L/ROI, drawdown, and the cash flow of each filled rung.
The Action log prints newly settled selector points once (up to the most recent
eight after a restart), rather than repeatedly printing an ambiguous gross
payout as though it were profit.

Selector comparisons use paired settled outcomes, not unrelated all-time
ledgers. Its paper fills require fresh complete top-of-book and displayed-depth
evidence, but exclude queue position, quote cancellation, hidden liquidity, and
fees; they are not exchange-fill claims.

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
| `BET_AMOUNT_SHARES` | `1` | BTC contracts per each of the four fixed rungs (**shares — NOT dollars**) |
| `PROPHET_SELECTOR_ENABLED` | `true` | Enables the paired trailing 3/5/7/10/25/50 win-rate selector. In paper mode it is a third paper ladder; in confirmed live mode it supplies the one actual locked side. |
| `PROPHET_SELECTOR_START_INVERSE` | `true` | Forces the first selector market after deployment to inverse, then hands control to the frozen trailing-window vote. |
| `PROPHET_SELECTOR_TIME_SERIES_LOG_ROWS` | `8` | Number of recent selector cash-flow points printed after a runner restart; the report JSON always retains the full series. |
| `PROPHET_LADDER_SCALP_SHADOW_ENABLED` | `true` in paper mode | Enables the separate normal-Prophet paper scalp audit; it has no live-order path. |
| `PROPHET_LADDER_SCALP_SHADOW_POSITION_SIZE` | `1` | Paper contracts per scalp rung, independent of `BET_AMOUNT_SHARES`. |
| `PROPHET_LADDER_SCALP_SHADOW_PROFIT_TARGET` | `0.01` | Retained for compatibility; the current range observer reports 1¢/2¢/3¢/5¢/10¢ gross opportunities rather than selecting this exit. |
| `PROPHET_WEIGHTED_TRAILING_SHADOW_ENABLED` | `true` in paper mode | Enables the independent locked-side normal and inverse 1/2/3/4 weighted trailing studies. |
| `PROPHET_WEIGHTED_TRAILING_STOP_PER_CONTRACT` | `0.10` | Full-depth trailing retracement used to close an entire weighted paper position at the observed bid. |
| `PROPHET_WEIGHTED_TRAILING_ACTIVATION_GAINS` | `0.01…0.09, 0.10…0.80` | Independent paper hold-gates above each model side's weighted average entry. Each gate arms its own 10¢ trailing stop, which remains armed through settlement. |
| `HISTORY_MINUTES` | `500` | 1-minute candles fed to Prophet |
| `FORECAST_MINUTES` | `17` | Fixed Prophet horizon in one-minute timesteps for every cached forecast |
| `PREOPEN_FORECAST_LEAD_S` | `120` | Seconds before the next market opens to pre-compute its 17-step forecast |
| `OPEN_TRADE_GRACE_S` | `45` | Max seconds after a market opens to start the locked GTC ladder. An older market is skipped rather than receiving late orders. |
| `SETTLE_CHECK_S` | `2` | Settlement polling cadence (seconds), independent of the opening order path |
| `UNCERTAINTY_SAMPLES` | `1000` | Prophet uncertainty samples (80% CI) |

**One-run overrides** — the workflow exposes the mode, share count, and
selector state paths. The code deliberately ignores legacy hedge or multiplier
environment variables. There is no scheduled Prophet workflow configured.

## QA before launch

Actions → **"Kalshi QA — Secrets, BTC Data, Prophet Forecast, Kalshi WS & Order
Build"** → Run workflow.

The suite runs checks against live credentials — Kalshi auth/balance, a real
yfinance download + validation, a real Prophet fit with band sanity checks,
and static verification that the active path locks one side and posts the four
GTC rungs — and **force-overrides DRY_RUN in-process so it can never submit an
order**.
Exit code is non-zero on any critical failure.

## Workflow safety (Kalshi Prophet ladder)

The old hedged Prophet execution workflow was removed so it cannot overlap the
ML-side BTC runner. This BTC-only ladder has **no scheduled workflow** and a
dedicated **“Kalshi Prophet BTC-only GTC Ladder — Selector”** workflow. It
defaults to `execution_mode=paper`, has its own concurrency group, and
self-handoffs every 5 h 45 min while persisting the normal, inverse, scalp, and
selector ledgers as audit artifacts. A `simulated_executable_quote_hit` means
the observed selected-side quote reached a rung with a fresh complete
top-of-book and displayed depth; it is not an exchange fill or queue claim.

To deliberately enable live selector execution, run the workflow manually with
`execution_mode=live` and type `LIVE_PROPHET_SELECTOR` in
`live_confirmation`. The workflow then sets `DRY_RUN=false` and submits only
the selector side frozen before the market opens—never both normal and inverse.
The inverse shadow remains disabled in live mode. The mode and confirmation
carry to the next 5 h 45 min handoff, so cancel the run or restart in `paper`
mode to return to paper trading.

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

It also evaluates the historical legacy schema and produces a feature ledger.
The production ML-only trainer selects only BTC one-minute raw features,
strike distance, clock, and earlier *settled* Kalshi outcomes from that ledger;
it ignores every Prophet column. The previous market is excluded until its
settlement timestamp, which prevents future-outcome leakage. The report is
directional accuracy and calibration only; it does not claim dollar P&L because
historical result records do not provide executable opening fills, spreads, or
fees.

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

### Prophet normal/inverse selector replay

**“Kalshi Prophet Selector Historical Backtest”** is a separate manual Action
that replays a completed `prophet_ml_backtest_rows.csv` artifact (by default,
the stored run `29698207476`). It does not refit Prophet, fetch prices, submit
an order, or change the running selector. At every historical forecast it sees
only outcomes whose recorded settlement time had already occurred, evaluates
fixed normal and inverse sides, each 3/5/7/10/25/50 trailing window, and the
six-window majority vote. A window/fixed-side choice is made from the first
80% of signals and evaluated only on the final chronological 20% holdout.

The Action uploads the full per-signal decision audit, JSON, and Markdown
report. It reports directional accuracy only: the stored artifact has no
executable quote, fill, queue, spread, slippage, or fee data, so it cannot
approve a live configuration or estimate dollar P&L.

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
