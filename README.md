# KXBTC15M hybrid Kalshi strategy

This repository’s active Kalshi path is a KXBTC15M **sticky-direction, live-capable strategy** with a shared historical-replay and execution state engine. Every startup and heartbeat states `LIVE`, `DRY_RUN`, or `RECONCILE_ONLY`; a research result is never labeled as a live fill.

> **Historical Kalshi settlement replay with empirically calibrated Monte Carlo execution-path simulation.**

It is not a pure Monte Carlo backtest. Historical KXBTC15M settlement outcomes and their timestamps are fixed; only intramarket facts unavailable from the public settlement API—resting-order fills, adverse-path depth, stop activation, and exit execution—are simulated.

The active GitHub Actions worker is [`kalshi_live_trader.py`](kalshi_live_trader.py). Retired Prophet, equity-regime, loss-skip, and ladder code paths are retained only as retired material and are not called by the hybrid live, watchdog, controlled-restart, audit, or emergency-cancel workflows.

## Read this before interpreting a result

| Evidence class | What is known | What is not claimed |
| --- | --- | --- |
| Historical settlements | Final YES/NO, market timestamps, and the causal directional sequence | Intramarket bids/asks, maker fills, stop touches, or slippage |
| Observed operational calibration | The supplied executed/zero-fill and rung cohorts | That an old 40¢ ladder cohort is a direct measurement of a 49¢ or 50¢ maker-fill rate |
| Monte Carlo replay | Distribution of hypothetical execution over the same fixed settlements | A prediction of a different historical settlement sequence or exact past execution events |
| Live shadow ledger | Current exchange observations, conservative fill evidence, and realized shadow accounting | Real-money P&L or proof that shadow behavior will persist |

All dollar results below are gross unless explicitly marked otherwise. Fees, live queue position, partial fills, cancellations, latency, and stop slippage can reduce or eliminate the modeled edge.

## Current live/shadow configuration

[`selected_live_strategy.json`](selected_live_strategy.json) is the canonical base configuration. The active contract is `kxbtc15m-delayed-band-live-v13` / schema `13`. It promotes the observed delayed-entry cohort into the shared shadow/live execution engine; it is not an auxiliary analytics order. Python and GitHub Actions independently assert the version, entry band, GTC/post-only entry type, 2.50× sizing, and direct 51¢ protective-exit contract before execution. v12 and earlier checkpoints use different paths and a different runtime branch, so an old worker cannot reinterpret them as v13 state.

| Setting | Current value | Notes |
| --- | ---: | --- |
| Series | `KXBTC15M` | Discovered from Kalshi market metadata, not ticker arithmetic |
| Market discovery | **Bounded previous/current/upcoming window, 1-second poll** | Uses Kalshi `min_close_ts`/`max_close_ts` metadata to preload the real API successor before open without scanning far-future markets |
| Direction observation window | **Final 5 seconds** | Configurable to 15 seconds; only a ≥99¢ executable bid observed for exactly one side inside this ending-market window can supply the next direction |
| Direction rule | **Sticky until directional win** | Seed inverse to the first prior result; hold the same side after a wrong prediction; flip only after that side settles correctly |
| Starting permanent base | **1.00 share default** | Configurable for a brand-new state; two-decimal `Decimal`, `ROUND_HALF_UP` |
| Cohort gate | **Opening ask <53¢; first fresh ask at/after 60s ≥53¢** | Complete pre-open subscription coverage is required; missing/partial opening evidence fails closed |
| Entry order | **Post-only GTC limit 1¢ below the first qualifying ask** | The derived limit must be 52–57¢; a first qualifying limit above 57¢ is terminally filtered and never chased later |
| Entry lifetime | **Until filled or market close** | No strategy-time expiry; a resting remainder is cancelled only at market close or when confirmed cancellation is required to protect filled exposure |
| Canonical workflow lane | **`delayed_53_57_exit_51`** | The only lane maintained by the production watchdog; real orders remain gated off by default |
| Protective exit | **Executable bid ≤51¢** | Latches before entry cancellation, then sends reduce-only IOC orders at the current executable bid until the authoritative position is flat |
| Maker exit | **Disabled** | v13 never submits the former resting 52¢ exit |
| Partial exit | **Authoritative residual retry** | Each acknowledged IOC is reconciled; only the remaining position is submitted again, so the bot cannot reverse the position |
| Recovery multiplier | **2.50×** | Advances after every filled closed trade while cumulative cycle P&L remains negative |
| Recovery exponent ceiling | **Disabled (`0`)** | Sizing remains uncapped by exponent; the effective base-linked position cap still applies |
| First base threshold | **$350.00** | Realized net P&L only |
| Threshold growth | **2.50×** | Geometric after each permanent-base step |
| Base increment | **+0.50 share** | Supports +0.25, +0.50, and +1.00 |
| Position cap | **100 contracts per permanent-base share by default** | Effective cap is 100 at base 1.00, 150 at base 1.50, and 200 at base 2.00; set `max_cap_per_base_share=0` to use a fixed `max_share_cap` instead |
| Shadow balance | **$1,000.00** | Isolated from the live account state |
| Real-money mode | **User-controlled; source defaults are shadow-safe** | Actual runtime mode is logged/checkpointed. Live requires `KALSHI_SHADOW_ONLY=false`, `KALSHI_LIVE_ENABLED=true`, and an explicit workflow `live_enabled=true` request |

### Base-linked or fixed maximum share size

The reviewed production profile enables **`max_cap_per_base_share=100.00`**.
The effective cap is `permanent_base × 100.00`, rounded to two decimals. Thus a
base increase from 1.00 to 2.00 raises the cap from 100 to 200 without changing
the 2.50× recovery multiplier. `max_share_cap` is the fixed-cap value used when
base linking is explicitly disabled. Enter `max_cap_per_base_share=0` to disable
linking; blank inputs preserve the last successfully checkpointed values.

At base 1.00 and recovery multiplier 2.50, the quantities are calculated from
`base × 2.50 ** exponent`, rounded HALF_UP to 0.01, then limited to the hard cap:

| Recovery exponent | Base 1.00 / cap 100 | Base 1.50 / cap 150 | Base 2.00 / cap 200 |
| ---: | ---: | ---: | ---: |
| 0 | 1.00 | 1.50 | 2.00 |
| 1 | 2.50 | 3.75 | 5.00 |
| 2 | 6.25 | 9.38 | 12.50 |
| 3 | 15.63 | 23.44 | 31.25 |
| 4 | 39.06 | 58.59 | 78.13 |
| 5 | 97.66 | 146.48 | 195.31 |
| 6+ (while unrecovered) | 100.00 | 150.00 | 200.00 |

The cap changes only with the permanent base; a recovery exponent never changes
it. Recovery exponent advances after every filled completed trade while total
recovery-cycle net P&L is negative, including an individual profitable trade
that has not recovered the deficit. Only cycle P&L >= 0 resets to base. Zero
fills change neither state. Funding checks and loss breakers are unchanged; a
larger cap does not supply additional buying power or guarantee recovery.

The two inputs map to `--max-position` and
`--max-position-per-base-share`, persisted as Decimal strings in the chosen
configuration, cycle parameters, and each signal's configuration snapshot. Each market also stores
`effective_position_cap` and `effective_position_cap_after`; heartbeats print
`cap`. Both historical reference replay and live/shadow execution use the same
effective-cap calculation. Optimizer exports explicitly preserve fixed versus
base-linked semantics instead of silently substituting 100. Historical results
are not reclassified.

**Blank means preserve**, including on watchdog restarts and normal handoffs. An
existing configuration keeps both cap fields; publishing the code does not
reinterpret an active order or negative recovery cycle. The narrowly reviewed
legacy-state migration enables 100 contracts per base share only for a fresh
cycle; frozen records keep their creation parameters. A requested cap change is
accepted only with no outstanding order/position.
No live/shadow state is reset. The next worker checks out `main` and restores only
durable data/configuration, never an older runner source file.

### Frozen observed-ledger selection evidence

The original v12 choice was based on the supplied frozen replay of the delayed cohort with a maximum filled entry of 57¢, the ask-minus-1¢ entry, 51/52/50 hybrid stop, 1.00 starting share, 2.50× recovery, and a 100-share initial cap. This table is archived selection evidence; it is **not** a backtest of the new v13 direct 51¢ exit:

| Metric | Frozen replay |
| --- | ---: |
| Resolved trades | 135 |
| Directional wins / losses | 73 / 62 |
| Directional win rate | **54.07%** |
| False-stopped eventual winners | 52 |
| Realized profitable / losing trades | 21 / 114 |
| Equal-size EV | **+3.61¢ per share** |
| Path-dependent 2.50× P&L from a $1,000 reference balance | **+$353.21** |
| Reference return | **+35.32%** |
| Maximum observed drawdown | **$50.77** |
| Quantity-weighted average entry / average stopped loss / fee-adjusted break-even WR | **Not retained in the frozen summary** |

Those last three fields cannot be reconstructed accurately from aggregate counts, so this README does not invent them. They are calculated prospectively from actual filled quantity, actual average entry, realized net P&L, and fees in the durable ledger and printed for all trades plus rolling 20- and 50-trade windows.

These are observed-ledger/replay results, not live fills and not a guaranteed return. The positive 2.50× result is nonlinear and highly sequence-dependent; 52 of 73 directional winners were stopped before settlement. Fees, queue position, partial fills, and live stop slippage were not established by those headline figures. A $150 deposit exceeds the reported $50.77 historical drawdown and can fund the initial 100-share cap at the 57¢ ceiling before fees, but that does **not** prove $150 survival outside the 135-trade sample or fund every later base-linked cap. The worker therefore retains funding checks, the effective cap, continuous exchange reconciliation, and persistent loss circuit breakers.

### Delayed maker entry and direct protective exit

Every second, the worker requests a bounded KXBTC15M close-time window and maintains WebSocket subscriptions for the predecessor, current market, and API-provided successor. During the final five seconds (15 is supported), exactly one side must show a fresh executable bid ≥99¢ to supply the next sticky-direction transition. Neither side, both sides, stale evidence, or an unavailable predecessor fails closed.

At open, the pre-subscribed WebSocket freezes the earliest fresh **price-only** selected-side executable ask. This opening value is an eligibility fact, not the order price: it must be below 53¢. Beginning at 60 seconds after open, the first fresh complete selected-side ask at or above 53¢ becomes decisive. The worker freezes `entry_limit_cents = qualifying_ask_cents - 1`. Limits of 52–57¢ are eligible. If the first qualifying ask implies 58¢ or more, the market becomes `ENTRY_FILTERED`; a later cheaper quote cannot revive it and zero/filtered markets never advance recovery.

The worker submits one deterministic GTC/post-only buy for the selected side. `maker_order_time_in_force=good_till_canceled`, `entry_order_lifetime=until_filled_or_market_close`, and `entry_timeout_seconds=0` are fail-closed contracts. The order rests until fully filled, market close, or confirmed cancellation required by a stop. Live mode uses Kalshi orders and fills as authoritative. Shadow mode requires a post-submission public trade at or below the buy limit; a displayed touch or submission is never called a fill. Partial fills create only the observed exposure and all P&L/recovery accounting uses that quantity.

At an executable selected-side bid ≤51¢, v13 **durably latches the
protective-exit obligation**, cancels and confirms any unfilled entry remainder,
re-reads the authoritative position, and sends a reduce-only IOC at the current
executable bid. There is no 52¢ maker-exit phase. The latch is written before
the cancellation request, so a cancellation 404/timeout, price rebound,
process restart, or worker handoff cannot return the trade to a normal
hold-to-settlement path. If the IOC fills only partly, the next pass refreshes
orders and the authoritative position and sends another deterministic,
price-protected reduce-only IOC for only the residual.

Live-stop safety contract 3 adds a durable exit-order intent **before** each POST and latches the 51¢ protective trigger before the entry-cancel request. A lost response is recovered using the exact client order ID, not retried blindly with a new ID. An empty lookup does not prove rejection. Definitive HTTP 400/404 entry rejections are terminal for that market and do not halt later markets. An ambiguous response starts a continuously reconciled entry interlock: the process and WebSocket remain alive, the exact client ID/order/fills/position are checked every reconciliation interval, a discovered order or position is adopted, and a no-order case becomes zero-fill only after market close proves it cannot create later exposure.

Once triggered at ≤51¢, the direct exit remains latched through price recovery
and process restarts. An IOC may be partially filled; the adapter then re-reads the
authoritative position and submits a new price-protected reduce-only IOC for
only the residual. It does **not** turn the unfilled IOC remainder into GTC,
because a stale resting exit can later oversell after another retry. IOC
fills/fees are refreshed before sizing any residual retry. If a latched exit is
still non-flat at market close, the ledger records a critical protective-exit
incident and refuses to disguise it as an ordinary settlement; if settlement
ultimately flattens it, the classification remains
`PROTECTIVE_EXIT_FAILURE_*` or `HARD_STOP_EXIT_FAILURE_*`.

The 51¢ trigger is not a guaranteed 51¢ execution. No client can guarantee an
exit when the exchange is unavailable, the market is paused/closed, the book
gaps below the limit, or no buyer exists. The worker instead makes the strongest
safe effort available without risking a reverse position: durable intent,
ticker-scoped V2 order and fill reconciliation, authoritative position reads,
reduce-only exits, and bounded residual IOC retries. These paths are covered by
offline fault-injection tests in `tests/test_live_stop_safety.py`; passing those
tests is not evidence of successful real exchange fills.

`ENTRY_FILTERED` is a strategy decision, not an exchange rejection. For example, a qualifying ask of 60¢ implies a 59¢ limit, exceeding the 57¢ ceiling. The heartbeat now retains that observed ask, derived limit and filter reason even when no order was submitted. Separate `analytics_only=true` fills do not affect the executable strategy balance. Live activation still requires the user-controlled gates; this safety patch does not turn on real-money trading.

### User-controlled live switch

Deployment does **not** enable real-money orders. After reviewing the strategy and reconciliation, the account owner can explicitly enable it:

1. Repository **Settings → Secrets and variables → Actions → Variables**: set `KALSHI_LIVE_ENABLED=true` and `KALSHI_SHADOW_ONLY=false`. Keep `KALSHI_MAINTENANCE_MODE=false`. These are permission gates; on a shadow deployment they do not start live trading by themselves.
2. **Actions → Kalshi KXBTC15M Direct 51c Live → Run workflow**, branch `main`: check **LIVE SWITCH** (`live_enabled`), leave **READ-ONLY CHECK** (`reconcile_only`) unchecked.
3. For the frozen defaults, initial shares = `1.00`, scaling multiplier = `2.50`, cap per base share = `100.00`, profit threshold = `350.00`, shares added = `0.50`, direct exit = `51`. Blank inputs preserve the durable configuration, not necessarily the original defaults. Do not request sizing changes during active exposure/recovery.
4. Verify the worker logs **`MODE=LIVE`**, a successful authenticated reconciliation, then accepted exchange order IDs and actual fills. `DRY_RUN`, `RECONCILE_ONLY`, `ENTRY_FILTERED` and `analytics_only=true` are not proof of a real trade. A blocked live request now fails instead of silently becoming shadow.

If a shadow worker is still running, the singleton queues a normal live dispatch; it does not interrupt it. To transition sooner, use **Controlled Restart — Kalshi KXBTC15M Hybrid** with its current `source_run_id` and `target_live=true`. This refuses handoff unless the checkpoint is flat and the market is within the safe 1–14 minute window. Never force-cancel a live worker managing an order or position. The watchdog resumes the last explicitly selected trading mode and uses current `main`; read-only audits do not change that selection. To revoke live permission, restore `KALSHI_SHADOW_ONLY=true` and `KALSHI_LIVE_ENABLED=false`; changing repository variables does not instantly change an already running process, so existing exposure still needs its manager.

The live account uses actual exchange cash, not the $1,000 shadow balance. The delayed strategy still requires an opening ask **below 53¢**, then a first qualifying ask **≥53¢ at/after 60 seconds**, with **ask−1¢ ≤57¢**. Thus the limit band is **52–57¢**, not an unconditional purchase of every market opening above 53¢. Starting with $120 is not a guarantee of recovery or solvency. Exchange rejection, cancellation delay and stop slippage remain possible. The adapter records V2 `average_fee_paid × actual fill_count` when total-fee fields are absent; explicit maker/taker fee totals take precedence.

Durable mode evidence is stored under `execution_context` in each state checkpoint and included in every new JSONL audit event: `mode` (`LIVE`, `DRY_RUN`, or `RECONCILE_ONLY`), `state_namespace`, `real_order_submission_enabled`, `workflow_run_id`, `source_commit`, and `stop_safety_contract`. A checkpoint stamped as live cannot be loaded as shadow or vice versa. Read-only reconciliation retains the live namespace but records order permission as false. The runtime branch is `runtime-state-kxbtc15m-delayed-v13`; live/shadow state and audit filenames remain separate. Legacy unstamped events remain legacy evidence, not retroactively labeled live trades.

Every signal also maintains independent analytics for 40, 41, …, 49¢. It records executable-ask touches separately from conservative simulated fills. At official settlement, it records winner capture and missed-winner rates, the minimum selected-side ask, eventual-winner maximum drawdown, and whether a stopped position would later have won. A stopped record remains observed until official settlement; verification never changes already-realized recovery P&L.

The ledger persists the opening ask, qualifying ask, derived limit, requested notional, exchange/client order IDs, actual fill quantities/prices/fees, stop timestamps, residual exposure, settlement, drawdown, and false-stop status. The heartbeat prints `delayed_entry_status`, `delayed_opening_ask`, `delayed_trigger_ask`, and `delayed_limit`. The corrected timestamp parser accepts ISO time, epoch seconds, and epoch milliseconds; this prevents persisted delayed fills from silently bypassing stop analytics after restart.

### Workflow inputs and checkpoint persistence (September 8 repair)

**Blank numeric input means keep the last successfully checkpointed value**;
it does not mean zero and does not reset to a default on each five-hour run.
With no prior runtime configuration, reviewed source defaults apply. Enter a
value to request a change. Once validated against durable state, configuration
is atomically saved and included in the runtime snapshot. The next worker and
watchdog recovery restore that configuration, even though the dispatch form
shows blank fields. A failed validation or failed remote checkpoint is **not**
confirmation that a requested change has persisted remotely.

| Input | Meaning |
| --- | --- |
| `fresh_state_reset` | One-run, default-off live reset. Only after local and authoritative exchange-flat checks, starts base/recovery/P&L/fees/metrics fresh while retaining the append-only audit ledger. It is never forwarded to the next worker. |
| `initial_shares` | Initial base for a brand-new strategy state; does not overwrite an existing permanent base. |
| `scaling_multiplier` | Recovery multiplier and geometric threshold-growth multiplier. `2.5` and `2.50` mean the same value. |
| `max_share_cap` | Fixed absolute share ceiling used when base-linked capping is disabled. |
| `max_cap_per_base_share` | Contracts of cap per permanent-base share; default 100. Set 0 to disable linking and use `max_share_cap`. |
| `profit_threshold` | First scaling threshold for new state; does not erase an existing accumulated profit/next threshold. |
| `shares_added_after_profit_threshold` | Permanent base increment after realized net profit crosses the current threshold. |
| `max_stop_loss_cents` | Direct protective-exit trigger, 10–51¢; the current default is 51¢ and there is no maker-exit phase. Blank preserves the saved value. |

Changes are refused while an order/position remains unresolved. An existing
negative recovery cycle retains its saved sizing parameters until recovered;
new settings do not retroactively resize it or reset P&L. Live and shadow keep
separate state/ledgers, but this canonical lane's chosen configuration file is
shared. Mode switches therefore still pass state/configuration reconciliation.
The `live_enabled` and `reconcile_only` checkboxes are separate per-dispatch
controls, **not numeric defaults**: live still requires both repository gates,
and reconciliation-only sends no orders. The watchdog retains the guarded
previous mode; changing a numeric input cannot itself activate live trading.
The `fresh_state_reset` checkbox is deliberately non-persistent: it applies to
one explicitly live dispatch, records the previous counters in the audit
ledger, and the automatic five-hour successor starts normally with reset off.

Startup logs print `CONFIG SAVED LOCALLY` with the selected numeric settings
and configuration hash. Confirm the remote checkpoint step also succeeds.
Offline tests now run **before** restoring operator configuration; this avoids
testing mutable live settings against fixed research defaults. Runtime commits
provide their own Git author/committer identity, including on failure paths.
Source code still comes from `main`; runtime restore permits only state,
configuration and ledger paths and verifies the source SHA/code are unchanged.
GitHub scheduling/network interruptions can still delay workers—this is
restart-safe orchestration, not a guarantee of uninterrupted 24/7 execution.

### Exchange-specific funding and order health

Kalshi balances are allocated by exchange shard. A positive aggregate account
balance does **not** establish that a crypto market can be funded. Before a
live entry, the adapter reads the market's authoritative `exchange_index` and
queries the balance for that exchange only. Unavailable metadata or funding
fails closed; it never silently substitutes aggregate cash or transfers funds.
The signal's `entry_funding` snapshot records exchange index, available cash,
required notional and read status in the durable state/audit ledger.
See [Kalshi's exchange-sharding documentation](https://docs.kalshi.com/getting_started/exchange_sharding).

Entry and reduce-only exit requests use the exchange index verified from market
metadata. If that per-market cache is empty after restart, exits/cancellations
require ticker-based auto-routing (`exchange_index=-1`); no balance lookup is
needed to reduce risk. Cancellation always includes `market_ticker`, so an order
ID alone cannot silently route to exchange 0. The routing cache is bounded and
never hard-codes a shard from the ticker's spelling. The short
`ORDER HEALTH` line separates recorded attempts, exchange acknowledgments,
definitive rejections and uncertain submissions, with the persisted breaker
reason. Analytical threshold hits are not order acknowledgments. Raw SDK
exception bodies/headers are not retained for entry or cancellation failures.

Credential rotation does not alter an existing worker's environment. Changing
funds/credentials is not evidence that an ambiguous order disappeared. The
running worker nevertheless stays online and continuously resolves a narrow
entry-submission interlock from exact orders, fills and positions; no manual
reset is needed after that evidence is conclusive. This code has no automatic
fund transfer mechanism.

An independent operator-run [Codespaces shard admin tool](kalshi_shard_admin.py)
supports authenticated read-only checks, an exact-amount transfer to an explicit
exchange shard, a separately confirmed full available source-shard transfer, and
a separately confirmed 100% recurring allocation. Transfer POSTs use integer
centicents and the current V2 `intra_exchange_instance_transfer` request schema.
It never places orders, clears the bot's breaker or restarts a worker.
See the [exact setup, confirmation and recovery instructions](docs/kalshi_codespaces_shard_funding.md).

After funding, the separate [operator-run order smoke test](docs/kalshi_order_smoke_test.md)
can check a shard-2 market and WebSocket read-only. An explicitly confirmed
`--execute --workers-paused` run sends one 1¢/one-contract post-only test order,
then cancels/reconciles it with a 30-second server expiry as backup. It never
resets the strategy breaker or restarts a worker. A test order can fill; actual
exposure requires operator review, not an assumed successful cancellation.

Every explicitly enabled **LIVE** Actions worker also runs the narrower
`kalshi_startup_order_check.py` before the strategy process starts. It discovers
the current KXBTC15M market, requires its API-reported `exchange_index=2`, reads
the balance for shard 2, verifies key write scope and a flat shard, then chooses
one side from fresh two-sided WebSocket quotes. It writes and remotely publishes
a deterministic create intent before sending exactly one 1.00-share, 1¢,
post-only probe. The strategy starts only after the exact create ACK, exact
cancel ACK, zero remaining quantity, zero fills and a flat position are all
reconciled. The same GitHub run ID cannot create a second probe after a retry.
Shadow and reconciliation-only runs never execute this check.

This startup probe is intentionally not sent when an unrelated strategy
loss/accounting breaker, open order, position, stale quote, wrong shard,
insufficient shard balance, missing write scope, old unresolved probe or
checkpoint failure exists. Those conditions pause new exposure while the
worker/restart path keeps reconciling—they are not permission to try unrelated
orders. A persisted `maker_entry_submission_unknown` or legacy
`maker_entry_submission_rejected` interlock is automatically released before
the probe only after V2 order/fill history, the closed source market, the
shard-wide open-order scan and the shard-wide position scan jointly prove that
the exact durable client-order intent is terminal, unfilled and flat. The old
intent is retained and annotated rather than deleted. A successful 1¢
create/cancel validates that specific API path at that moment; it cannot
guarantee later 52–57¢ entries and 50¢ stops. A 1¢ post-only order can still
fill and incur fees; any fill stops startup for manual position review and is
kept separate from strategy P&L.

The live REST/SDK adapter uses Kalshi's current V2 hosts:
`https://external-api.kalshi.com/trade-api/v2` in production and
`https://external-api.demo.kalshi.co/trade-api/v2` in demo. Request signing is
performed over the timestamp in milliseconds plus the HTTP method plus the
full `/trade-api/v2/...` path without query parameters. Create Order V2 uses
fixed-point string `count`/`price`, the single YES-book `bid`/`ask` convention,
`good_till_canceled`, deterministic `client_order_id`, `post_only`, and the
market's API-reported `exchange_index` (or `-1` ticker auto-routing after a
restart). Cancellation is accepted only for the exact returned order ID and is
then checked against order, fill and position reads.

### Sticky signal transition

The v13 signal has no loss-skip rule and is independent of execution. For each new market, the worker freezes the immediately preceding market’s realtime provisional outcome, later checks it against official settlement, and records the transition in both state and audit ledger:

```text
fresh state + previous YES  -> enter NO
entered NO + current settles YES -> enter NO again  (directional loss: hold)
entered NO + current settles NO  -> enter YES next  (directional win: flip)
entered YES + current settles NO -> enter YES again (directional loss: hold)
entered YES + current settles YES -> enter NO next  (directional win: flip)
```

Entry fills, zero fills, hybrid exits, recovery P&L, and permanent-base scaling never change that directional side. Only the completed market result relative to the prior selected side does. A provisional/official mismatch is preserved as an audit discrepancy; it never rewrites an already-submitted entry.

### Durable namespace and archived comparisons

The canonical worker, watchdog, and controlled restart now target only the v13 delayed-band lane:

| Lane | Durable state | Append-only audit ledger | Runtime ref |
| --- | --- | --- | --- |
| Canonical v13 shadow | `data/kalshi_shadow_delayed_band_v13_state.json` | `data/kalshi_shadow_delayed_band_v13_audit.jsonl` | `runtime-state-kxbtc15m-delayed-v13` |
| Canonical v13 live | `data/kalshi_live_delayed_band_v13_state.json` | `data/kalshi_live_delayed_band_v13_audit.jsonl` | `runtime-state-kxbtc15m-delayed-v13` |

All earlier state, IOC, opening-entry, and stop-comparison files remain forensic evidence only. They are not restored by the v13 workflow. A fresh v13 live state starts at exactly 1.00 share; its recovery state is not copied from v12 or earlier experiments.

## September 8 multi-series directional replay

The original open+45-second, available-official-settlement algorithm has now been
replayed across **13 other active 15-minute crypto/commodity series**, with BTC
as a control. Snapshot cutoff: **2026-09-08 01:27:59 UTC**. Both current and
historical API pages are cached, deduplicated, and restricted to that cutoff.

- [Full results, confidence intervals and streaks](reports/multiseries_20260908/backtest_summary.md)
- [Summary CSV, including coverage dates and recent-window statistics](reports/multiseries_20260908/directional_summary.csv)
- [Monthly results](reports/multiseries_20260908/monthly_directional_results.csv)
- [Separate opening-boundary proxy results](reports/multiseries_20260908/boundary_proxy_summary.csv)
- [Reproducible data/code ZIP](reports/multiseries_20260908/reproducible_settlement_replay.zip)
- [Source-line inventory](reports/multiseries_20260908/source_inventory.md)

Across BTC and the other 13 series: **164,330 unique settled markets / 164,306
eligible predictions**. The original first 20,778 BTC predictions reproduce
**10,751 wins / 10,027 losses** exactly. The refreshed full BTC sample is
13,002 / 12,260 over 25,262 signals (51.4686%). Zcash has the highest directional
rate in this snapshot (52.2918% over 6,567 eligible signals); this is not a ranking
of executable net profitability.

### All 13 non-BTC markets: directional results

The 13-series descriptive total is **139,063 settled markets**, **139,044 eligible predictions**,
**71,304 wins / 67,740 losses**, and **51.2816% WR**. The 19 excluded records
had no eligible causal signal; there is no loss-based skipping. This pooled WR
is descriptive, not 139,044 independent cross-asset trials. No pooled p-value or
combined cross-asset streak is claimed.

| Market / series | Eligible | Directional W / L | WR | 95% Wilson CI | Two-sided binomial p | Bonferroni p (13 tests) | Max W / L streak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| BNB (`KXBNB15M`) | 16,287 | 8,365 / 7,922 | 51.36% | 50.59%–52.13% | 0.000533 | 0.006929 | 12 / 14 |
| Copper (`KXCOPPER15M`) | 730 | 366 / 364 | 50.14% | 46.52%–53.75% | 0.970479 | 1.000000 | 10 / 8 |
| Dogecoin (`KXDOGE15M`) | 16,286 | 8,448 / 7,838 | 51.87% | 51.11%–52.64% | 1.818e-6 | 2.364e-5 | 13 / 11 |
| Ethereum (`KXETH15M`) | 25,249 | 12,987 / 12,262 | 51.44% | 50.82%–52.05% | 5.198e-6 | 6.757e-5 | 12 / 14 |
| Gold (`KXGOLD15M`) | 2,595 | 1,291 / 1,304 | 49.75% | 47.83%–51.67% | 0.813775 | 1.000000 | 12 / 9 |
| Hyperliquid (`KXHYPE15M`) | 16,286 | 8,275 / 8,011 | 50.81% | 50.04%–51.58% | 0.039313 | 0.511070 | 13 / 11 |
| Natural gas (`KXNATGAS15M`) | 730 | 366 / 364 | 50.14% | 46.52%–53.75% | 0.970479 | 1.000000 | 15 / 7 |
| NEAR (`KXNEAR15M`) | 6,567 | 3,283 / 3,284 | 49.99% | 48.78%–51.20% | 1.000000 | 1.000000 | 14 / 10 |
| Silver (`KXSILVER15M`) | 2,595 | 1,331 / 1,264 | 51.29% | 49.37%–53.21% | 0.195099 | 1.000000 | 10 / 8 |
| Solana (`KXSOL15M`) | 22,888 | 11,826 / 11,062 | 51.67% | 51.02%–52.32% | 4.563e-7 | 5.932e-6 | 14 / 11 |
| WTI crude (`KXWTI15M`) | 2,595 | 1,350 / 1,245 | 52.02% | 50.10%–53.94% | 0.041173 | 0.535254 | 15 / 10 |
| XRP (`KXXRP15M`) | 19,669 | 9,982 / 9,687 | 50.75% | 50.05%–51.45% | 0.036052 | 0.468673 | 13 / 11 |
| Zcash (`KXZEC15M`) | 6,567 | 3,434 / 3,133 | 52.29% | 51.08%–53.50% | 0.000213 | 0.002774 | 14 / 10 |

**Binomial test:** H0 is a directional win probability of 50%, versus a two-sided
alternative. With `n` signals and `w` wins, the exact probability is
`min(1, 2 * sum(comb(n,k), k=0..min(w,n-w)) / 2**n)`. The reported log-space
calculation was independently checked using exact integer binomial coefficients
for all 14 series, including BTC. See the [binomial-test definition](https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.binomtest.html).

**BNB, DOGE, ETH, SOL and ZEC** retain p<0.05 after Bonferroni correction across
these 13 primary tests. HYPE, XRP and WTI pass the uncorrected 0.05 threshold
but not the correction. BTC is the pre-existing control, excluded from that family:
**13,002 / 12,260**, **51.4686% WR**, **50.85%–52.08% CI**,
**p=3.124691e-6**, and **14 / 11** maximum W/L streak.

The tests and individual Wilson intervals assume independent, identically
distributed Bernoulli trials. Serial dependence, earlier strategy selection,
cross-asset correlation and execution costs limit interpretation. Bonferroni
addresses these 13 simultaneous comparisons; it does not fix invalid
within-series independence or prove a tradable edge. A p-value is not the
probability that the null is true. Beating 50% does not imply beating actual
entry prices plus fees.

### Coverage and stability

All rows end with the **2026-09-08 01:00 UTC market open**. First opens below
are UTC; historical gaps and commodity trading sessions are retained.
Halves divide eligible signals chronologically, not equal calendar durations.
Recent windows use the last 1,000 signals, or the whole history if shorter.
Current streaks are at the frozen cutoff—not the present LIVE worker.

| Series | First market open (UTC) | First-half WR | Second-half WR | Recent n | Recent W / L | Recent WR | Current streak |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| KXBNB15M | 2026-03-07 00:15 | 51.19% | 51.53% | 1,000 | 501 / 499 | 50.10% | 3W |
| KXCOPPER15M | 2026-08-27 20:30 | 53.42% | 46.85% | 730 | 366 / 364 | 50.14% | 1W |
| KXDOGE15M | 2026-03-18 20:00 | 51.66% | 52.08% | 1,000 | 519 / 481 | 51.90% | 1L |
| KXETH15M | 2025-12-10 21:45 | 51.05% | 51.83% | 1,000 | 516 / 484 | 51.60% | 2L |
| KXGOLD15M | 2026-07-31 18:00 | 49.19% | 50.31% | 1,000 | 502 / 498 | 50.20% | 1L |
| KXHYPE15M | 2026-03-18 20:00 | 50.42% | 51.20% | 1,000 | 518 / 482 | 51.80% | 1W |
| KXNATGAS15M | 2026-08-27 20:30 | 49.04% | 51.23% | 730 | 366 / 364 | 50.14% | 4L |
| KXNEAR15M | 2026-06-30 17:15 | 50.84% | 49.15% | 1,000 | 494 / 506 | 49.40% | 1W |
| KXSILVER15M | 2026-07-31 18:00 | 51.04% | 51.54% | 1,000 | 514 / 486 | 51.40% | 3L |
| KXSOL15M | 2026-01-09 00:30 | 52.28% | 51.06% | 1,000 | 509 / 491 | 50.90% | 2L |
| KXWTI15M | 2026-07-31 18:00 | 51.50% | 52.54% | 1,000 | 528 / 472 | 52.80% | 3W |
| KXXRP15M | 2026-02-11 05:00 | 51.31% | 50.19% | 1,000 | 527 / 473 | 52.70% | 2L |
| KXZEC15M | 2026-06-30 17:15 | 52.54% | 52.04% | 1,000 | 516 / 484 | 51.60% | 2L |

ZEC has the highest full-sample WR (52.29%), while SOL has the smallest nominal
p-value. Neither is necessarily the most profitable executable strategy.
Copper and natural gas each have only 730 signals. The latest BTC 1,000 are
499 wins / 501 losses (49.90%), compared with its 51.47% full-history WR.

### Relevant source-code size

Counted for this reporting update with cloc 2.06: executable/source lines only,
excluding comments, blank lines, datasets, documentation, dependencies and Git history.

| Scope | Files | Code lines |
| --- | ---: | ---: |
| Non-archived source, excluding tests/workflows | 32 | 20,999 |
| Non-archived tests | 21 | 5,827 |
| Current workflow definitions | 11 | 1,056 |
| **Relevant non-archived total** | 64 | **27,882** |
| Archived source/tests/workflows (separate) | 55 | 16,508 |
| **All counted source including archive** | 119 | **44,390** |

Non-archived does not mean every module or workflow is currently running.
Detailed file-by-file counts and the exact command are in the linked inventory.
These counts measure repository size, not enterprise valuation or profitability.

Run a new public-data snapshot with `python kalshi_multiseries_backtest.py`.
For an exact offline replay, extract the ZIP into an empty folder and run
`python kalshi_multiseries_backtest.py --output reports --cache cache --offline`.
The script uses only public reads and the standard library. Each signal's source
ticker, official result-availability timestamp, decision time and actual target
settlement are saved. Sticky-after-loss/flip-after-win is checked against the
original inverse-source rule. Boundary-proxy rows are kept separate because
eventual settlement alone cannot prove a >=99c quote was available at open.

**This is a directional-only historical settlement test.** It does not backtest
the delayed 53–57¢ entry filter, maker fills, hybrid stops, 2.50× dollar returns,
or fees. Those require separate execution evidence. All 31 result CSVs reproduced
byte-for-byte from the cached settlements; raw cache, expanded signals and code
hashes are preserved in the ZIP instead of duplicating every expanded file in Git.

## Reconstructed historical directional results — prior inverse baseline

The following snapshot was regenerated from Kalshi’s public settlement endpoints on **2026-08-08** using the prior `inverse_latest_settlement` rule. The cache is intentionally ignored by Git because it is downloaded source data; the exact retrieval commands are below. It is a reproducibility baseline, **not** the v13 delayed-entry/direct-exit expected value.

| Metric | Current public-history replay |
| --- | ---: |
| Settled KXBTC15M markets | 22,411 |
| Eligible causal signals | 22,406 |
| Directional wins / losses | 11,575 / 10,831 |
| Directional win rate | **51.6603%** |
| First settled market open | 2025-12-10 21:45:00 UTC |
| First eligible signal | 2025-12-10 23:00:00 UTC |
| Last market / signal in this snapshot | 2026-08-08 22:30:00 UTC |
| Markets without an earlier published causal settlement | 5 |

The original 20,778-signal reference is reproduced exactly as the first 20,778 current eligible signals:

| Reference horizon | Signals | Wins / losses | Directional WR |
| --- | ---: | ---: | ---: |
| Earlier reported reference | 20,778 | 10,751 / 10,027 | **51.7422%** |
| Current replay, same first 20,778 signals | 20,778 | 10,751 / 10,027 | **51.7422%** |
| New extension in the current snapshot | 1,628 | 824 / 804 | 50.6143% |

The difference from the earlier headline is therefore additional public history, not a random redraw or a changed directional rule. For this baseline, the source signal is the most recently settled earlier market published by the target market’s `open + 45 seconds`; YES maps to predicted NO and NO maps to predicted YES. No two-loss/two-market skip exists in either strategy version.

Live signal timing is intentionally faster: it freezes a provisional prior outcome from the final fresh executable 99¢ bid before the boundary, produces the v13 sticky transition at the next market’s open, and later verifies it against official settlement. The historical optimizer’s `--signal-mode sticky_until_directional_win` rebuild uses the actual previous settlement as an explicitly labelled **provisional-outcome proxy**; it does not claim that the delayed public endpoint was available at the boundary. The historical API does not contain that final quote stream, so the proxy and the live provisional-quote mechanism are distinct evidence paths; their agreement must be measured in shadow rather than assumed.

## Execution calibration

The supplied operational data is used only for the missing execution-path layer. The `loss_skipped=133` diagnostic group is deliberately excluded from fill/path calibration and does not cause market skipping.

| Observed cohort | Directional wins | Directional losses | Directional WR |
| --- | ---: | ---: | ---: |
| Eligible live signals | 653 | 639 | 50.54% |
| Old ladder executed / 40¢-region cohort | 139 | 209 | 39.94% |
| Old ladder zero-fill / no-40¢ cohort | 318 | 221 | 59.00% |

The joint adverse 40¢-region probabilities conditioned on eventual historical direction are:

| Fixed calibration target | Probability |
| --- | ---: |
| `P(40¢ region | eventual directional win)` | 139 / (139 + 318) = **30.4158%** |
| `P(40¢ region | eventual directional loss)` | 209 / (209 + 221) = **48.6047%** |
| Base 49¢ maker-participation scenario, win side | **85.00%** |
| Base 49¢ maker-participation scenario, loss side | **85.00%** |

The last two rows are deliberately separate scenario assumptions. The 40¢-region sample cannot identify resting 49¢ participation, and it says even less about the newer 50¢ entry. The conservative, base, optimistic, and full-participation-reference scenarios make this uncertainty explicit.

Older rung evidence supplies conditional depth shape. The loss-side counters were not perfectly nested, so the implementation projects them to a monotonic hierarchy before sampling; a simulated 10¢ reach always implies 20¢, 30¢, and 40¢ reaches.

| Rung reached | Observed winners | Observed losers | Observed directional WR |
| --- | ---: | ---: | ---: |
| 40¢ | 59 | 113 | 34.30% |
| 30¢ | 39 | 111 | 26.00% |
| 20¢ | 23 | 113 | 16.91% |
| 10¢ | 10 | 113 | 8.13% |

`calibration.py` writes observed-versus-simulated errors for both the joint 40¢ cohorts and the conditional rung WRs. The automated regression test requires the simulator to reproduce these targets approximately while preserving path nesting.

### Archived v12 delayed-cohort checkpoint (September 9, 2026)

This is a separate, small operational-research sample from the durable delayed tracker. It is not the frozen 135-trade selection and not a claim of live execution. The 33 resolved analytics records include qualifying limits above the production 57¢ ceiling, so they measure the tracker rather than the exact executable 52–57¢ band.

| Metric | Current delayed tracker |
| --- | ---: |
| Resolved / directional W-L / WR | 33 / 19-14 / **57.58%** |
| Quantity-weighted average modeled entry | **56.94¢** |
| Gross no-stop P&L / EV per fill | **+$1.755 / +5.32¢** |
| Gross 51/52/50 hybrid P&L / EV per fill | **+$0.215 / +0.65¢** |
| Hybrid realized positive / negative / flat | 6 / 25 / 2 |
| Average gross positive / average gross loss per share | **50.00¢ / 11.14¢** |
| Payoff-implied gross break-even realized WR | **18.22%** |

The independent ladder lanes below use their own fixed quantities and conservative tracked fill evidence. “Win rate” is the eventual selected-direction settlement rate among modeled filled records; it is not the realized stop-exit win rate.

| Analytics limit | Modeled fills | Directional W/L | Directional WR |
| ---: | ---: | ---: | ---: |
| 50¢ | 26 | 13 / 13 | **50.00%** |
| 40¢ | 22 | 9 / 13 | **40.91%** |
| 30¢ | 18 | 5 / 13 | **27.78%** |
| 20¢ | 15 | 2 / 13 | **13.33%** |
| 10¢ | 15 | 2 / 13 | **13.33%** |

These checkpoint figures will change as the append-only ledger grows. Live heartbeats now derive `LIVE PERFORMANCE` from actual completed filled trades only: count-based realized W/L, fee-adjusted realized WR, average net win/loss per actual share, payoff-implied break-even WR, WR-minus-break-even edge, quantity-weighted average entry, directional W/L, total net P&L, and total fees for all history and the last 20 trades.

The current base-case calibration check used 100,000 replications with seed `42`; errors below are simulated minus observed and are percentage points.

| Calibration statistic | Observed | Simulated | Error (pp) |
| --- | ---: | ---: | ---: |
| 40¢-region rate, eventual win | 30.4158% | 30.4077% | -0.0080 |
| 40¢-region rate, eventual loss | 48.6047% | 48.5939% | -0.0107 |
| 40¢-region directional WR | 39.9425% | 39.9415% | -0.0010 |
| No-40¢-region directional WR | 58.9981% | 58.9959% | -0.0023 |
| 40¢ rung directional WR | 34.3023% | 34.3023% | 0.0000 |
| 30¢ rung directional WR | 26.0000% | 25.7718% | -0.2282 |
| 20¢ rung directional WR | 16.9118% | 16.9986% | +0.0869 |
| 10¢ rung directional WR | 8.1301% | 8.1690% | +0.0390 |

## Static expected value — prior inverse baseline only

The following is a **one-share, fixed-size, no-fee, 40¢-stop calculation** using the current 22,406 *prior inverse* fixed directional outcomes and the base 49¢ execution scenario. It does not include recovery sizing, permanent-base scaling, the 100-share cap, funding failures, slippage, the new dynamic entry, or calibration uncertainty. It must not be read as v13 delayed-entry/direct-exit EV.

For entry price `e`, stop `s`, win rate `pW`, fill probability `f`, and joint 40¢-region probabilities `rW`/`rL`, the gross EV per eligible signal is:

```text
pW * ((f - rW) * (1 - e) + rW * (s - e))
+ (1 - pW) * ((f - rL) * (-e) + rL * (s - e))
```

| Mechanical price sensitivity | EV / eligible signal | EV / expected filled share | Gross / 1,000 eligible signals |
| --- | ---: | ---: | ---: |
| 49¢ entry, 40¢ stop | **+$0.02232** | **+$0.02625** | **+$22.32** |
| 50¢ entry, 40¢ stop | **+$0.01382** | **+$0.01625** | **+$13.82** |

The 50¢ row changes only payout math while holding the **49¢** base-fill/path scenario fixed. It is a sensitivity calculation, not a calibrated 50¢ maker-fill forecast. Neither fixed-price row is an expected-value claim for v13: the historical API cannot tell whether a delayed dynamic maker order filled or whether a direct 51¢ exit completed. The durable ledger therefore measures actual/simulated entry price, touch versus fill evidence, fees, partial exits, protective-exit mechanism, and later official outcome. At either fixed price, one cent of fee per filled share would reduce the per-eligible-signal figure by approximately $0.00850 under the 85% participation assumption, before any slippage. A positive static EV is not a capital guarantee: nonlinear recovery sizing can still create drawdowns, cap hits, and funding failures.

## Prior reconstruction comparisons

These are the earlier 50,000-execution-path / 1,500-market reconstruction results preserved for comparison. They used the corrected 1.00-share start and two-decimal sizing, but they are not a substitute for a full current-history run and must not be combined with the dynamic live entry rule. Dollar P&L and bankroll values below are model distributions, not exchange results.

### 1.11× recovery comparison: 40¢ stop, 49¢ entry

| First base threshold | Permanent base step | Median P&L | $100 completion | Approx. P95 bankroll | Approx. P99 bankroll |
| ---: | ---: | ---: | ---: | ---: | ---: |
| $100 | +1.00 | +$351.84 | 54.29% | $603 | $1,023 |
| $100 | +0.50 | +$275.74 | 57.94% | $566 | $990 |
| $100 | +0.25 | +$241.42 | 59.92% | $547 | $975 |
| $125 | +1.00 | +$295.23 | 56.91% | $574 | $995 |
| $125 | +0.50 | +$249.97 | 59.35% | $552 | $978 |
| $125 | +0.25 | +$231.20 | **60.55%** | **$540** | **$968** |

Within that archived 1.11× reference only, $100/+1.00 had the highest median P&L and $125/+0.25 had the highest $100 completion / lowest quoted capital requirement. It models a different fixed-price execution process and is not the source of the active v13 parameters.

### Representative lower-multiplier trade-off

| First threshold | Multiplier | Median P&L | P5 P&L | $100 completion |
| ---: | ---: | ---: | ---: | ---: |
| $50 | 1.01× | +$76.46 | +$40.64 | **98.48%** |
| $100 | 1.01× | +$63.48 | +$41.91 | **98.72%** |
| $125 | 1.01× | +$63.48 | +$41.91 | **98.72%** |
| $50 | 1.02× | +$115.95 | +$70.10 | 92.12% |
| $50 | 1.03× | +$164.22 | +$99.08 | 84.50% |
| $125 | 1.05× | +$127.91 | +$103.29 | 80.58% |
| $125 | 1.07× | +$183.46 | +$129.71 | 71.22% |
| $125 | 1.09× | +$233.94 | +$162.78 | 62.82% |
| $125 | 1.11× | about +$295 | about +$186 | about 57% with +1.00 base step |

The intended reading is a risk trade-off, not a claim that higher recovery is better. These 1.01× results are retained as an older fixed-price comparison and do not validate the active 2.50× delayed-entry strategy.

### Earlier inverse-baseline stop comparison

An earlier static inverse reconstruction ranked 40¢ first, followed closely by 10¢, then staged 40/30/20/10, 20¢, and 30¢. Its per-share gross estimates were 2.68¢, 2.64¢, 2.53¢, 2.44¢, and 2.37¢ respectively. This is descriptive archived evidence for a different strategy. The production workflow accepts only the v13 delayed 53–57¢ lane; archived optimizer output cannot activate retired Actions.

## Full reproducible backtest

Use Python 3.13 and the pinned research requirements. The commands create an ignored cache and a self-contained output directory; no live secrets are needed.

```bash
python3.13 -m venv .venv
.venv/bin/pip install -r requirements_kalshi_hybrid_backtest.txt

# 1. Download/cache actual Kalshi settlement outcomes and reconstruct the
#    prior inverse baseline. Use --signal-mode sticky_until_directional_win
#    below for the historical fixed-settlement proxy replay.
.venv/bin/python kalshi_settlement_loader.py --refresh \
  --cache data/raw/kalshi_kxbtc15m_settlements.json \
  --signals historical_signals.parquet

# 2. Validate the calibration layer alone (100,000 simulated calibration draws).
.venv/bin/python calibration.py \
  --output outputs/kalshi_hybrid_backtest/calibration_report.csv \
  --replications 100000 --seed 42

# 3. Full sticky-direction 49¢ historical screen, stop finalists, 100,000-rep final
#    runs, walk-forward, stress tests, and plots. The historical proxy is
#    labelled in every result; it does not invent intramarket quote history.
.venv/bin/python optimizer.py \
  --output-dir outputs/kalshi_hybrid_backtest/base_49c \
  --entry-price .49 --signal-mode sticky_until_directional_win --execution-scenario base_case \
  --coarse-simulations 5000 --final-simulations 100000 \
  --walkforward-simulations 500 --finalists 15 --seed 42

# 4. Optional 50¢ fixed-price sensitivity. This reuses the 49¢ path
#    calibration; it does not model the live dynamic maker/hybrid-stop rule.
.venv/bin/python optimizer.py \
  --output-dir outputs/kalshi_hybrid_backtest/sensitivity_50c \
  --entry-price .50 --signal-mode sticky_until_directional_win --execution-scenario base_case \
  --coarse-simulations 5000 --final-simulations 100000 \
  --walkforward-simulations 500 --finalists 15 --seed 42

# 5. Optional prior inverse baseline / exact 20,778-signal reconciliation.
.venv/bin/python optimizer.py \
  --output-dir outputs/kalshi_hybrid_backtest/prior_inverse_reference \
  --entry-price .49 --signal-mode inverse_latest_settlement \
  --coarse-simulations 5000 --final-simulations 100000 \
  --walkforward-simulations 500 --finalists 15 \
  --reconciliation-simulations 50000 --seed 42
```

Each output directory contains the full machine-readable result set:

| Artifact | Contents |
| --- | --- |
| `calibration_report.csv` | Observed vs simulated 40¢-cohort and rung calibration errors |
| `optimization_results.csv` | All 363 primary 40¢-stop configurations |
| `pareto_frontier.csv` | Median P&L, P5 P&L, $100 survival, P95 bankroll, P95 drawdown frontier |
| `stop_optimization_results.csv` | No-stop and 40¢/30¢/20¢/10¢ finalist comparisons, including final-depth reruns |
| `walkforward_results.csv` | Chronological 60% train / 20% validation / untouched 20% test replay |
| `stress_test_results.csv` | Fill adverse-selection, depth, slippage, entry-price, and fee stresses |
| `execution_scenario_sensitivity.csv` | Conservative/base/optimistic/full-participation execution scenarios |
| `regime_analysis.csv` | Monthly, half-sample, and rolling 250/500/1,000-market replays |
| `reconciliation_comparison.csv` | Explicit prior-style 1.11× reference runs over fixed actual settlement prefixes (inverse baseline only) |
| `funding_failures_reference.csv` | First prescribed-position funding failures in the Decimal reference replay |
| `plots/` | Calibration, equity, drawdown, bankroll, parameter, stop, Pareto, and walk-forward charts |
| `optimization_summary.md` | Human-readable rankings and explicit notice that settlement-only rows cannot be promoted to the delayed live profile |

The optimizer uses common random numbers for competing configurations, keeps every actual directional settlement fixed, and applies the same `strategy_core.py` recovery/base transitions as the live worker. It reports P&L, drawdown, bankroll, cap-binding, recovery-cycle, fill, zero-fill, and stop distributions. Calibration uncertainty can be added with `--calibration-uncertainty-draws N`. Because the settlement API cannot reproduce the v13 quote-timed 52–57¢ entry cohort, generic optimizer rows are no longer automatically exported as live configuration; only a row explicitly tagged `execution_profile=delayed_53_57_exit_51` can use the guarded export function.

## Production behavior and persistence

- The live engine and historical replay share the recovery/base-sizing transitions. A filled trade updates realized net P&L; a zero fill is exactly $0 and changes neither the recovery exponent nor permanent base.
- Recovery exponent increases after **every filled closed trade** while cumulative recovery-cycle P&L remains negative. It resets only when that cumulative amount reaches at least $0.
- `max_recovery_exponent=0` is the explicit disabled sentinel. The shared shadow/live engine does not stop the 2.50× sequence at an arbitrary exponent; the effective base-linked position limit, funding check, recovery-loss breaker, and daily-loss breaker remain active.
- Permanent-base steps use realized net P&L only. No unrealized value, cancelled order, or zero fill can scale the base.
- Startup reconciles Kalshi balance, open managed orders, positions, fills, and settlements before any entry. During runtime, an ambiguous entry response leaves the worker and risk management online while only new exposure is interlocked. Exact client-order, fill and position checks repeat every reconciliation interval; known orders/positions are adopted and closed-market no-order cases automatically resume later entries. Kalshi remains authoritative.
- Client order IDs are deterministic, partial fills use actual quantities, exits are reduce-only where supported, and the same market cannot be counted twice after restart.
- The worker discovers a bounded previous/current/upcoming market window every second using `min_close_ts`/`max_close_ts`, subscribes the API-provided successor before open, and keeps the ending market subscribed for final 99¢ executable-bid inference. The first complete opening ask establishes eligibility; the first fresh qualifying ask at/after 60 seconds freezes one deterministic ask-minus-1¢ GTC order. There is no IOC entry fallback and later quotes cannot move the limit.
- The direct protective exit defaults to 51¢. The obligation is persisted before entry cancellation; it cannot be unlatched by a cancel error, quote rebound, or restart. No 52¢ maker exit is submitted. A partial IOC is followed by a fresh authoritative position read and another reduce-only IOC for only the residual—never a stale GTC conversion. Actual entry and exit fills, quantities, fees, and residual exposure drive accounting.
- Shadow and live state are isolated at `data/kalshi_shadow_delayed_band_v13_*` and `data/kalshi_live_delayed_band_v13_*`. The v13 shadow lane starts at $1,000 and the configured initial base (1.00 share by default), and tracks realized P&L, peak equity, maximum drawdown, entry filtering, false stops, and timing.
- Every audit JSONL record is appended, flushed, and `fsync`ed before the worker resumes order/position management. Its companion strategy state is atomically written and `fsync`ed immediately after every audit event; therefore a state transition, fill observation, protective-exit event, funding failure, settlement, reconciliation result, and handoff is checkpointed locally while the worker is running—not merely at its end. Remote checkpoints are coalesced every 30 seconds and force-update one parentless `runtime-state-kxbtc15m-delayed-v13` snapshot with an exact lease. Snapshot schema v2 deterministically gzip-compresses each durable file in independent 8 MiB source chunks, verifies every compressed and uncompressed digest/size on restore, and reuses unchanged append-only ledger chunks. This prevents GitHub's 100 MiB single-blob limit from breaking a handoff as the ledger grows. The branch contains only allow-listed KXBTC15M payload chunks plus its manifest; it cannot accumulate ordinary code history or inherit older strategy state.
- Each market ledger record includes the immutable first price-only opening reference, its completeness status, separately timed first displayed-depth book, derived limit, exchange/client order IDs, partial fills, and maker/taker status where exposed. Timing includes exchange-price lag, worker-observation lag, depth-after-price lag, market-open-to-submission, market-open-to-first-fill, submission-to-first-fill, entry completion, first-fill-to-trigger, trigger-to-maker submission, and trigger-to-observed-flat position. Heartbeats use separate bounded `ORDER HEALTH`, `LIVE ACCOUNT`, `HEARTBEAT`, `ENTRY STATUS`, `LIVE PERFORMANCE`, `STOP STATUS`, and `RESEARCH COHORT` lines so GitHub does not truncate safety-critical fields. `LIVE PERFORMANCE` includes actual fees and rolling payoff-implied break-even rates. Five-minute tables print every 40–49¢ level, winner capture/misses, drawdown buckets, and hybrid-stop outcomes. They also print, separately for every hypothetical stop from 40¢ through 49¢, the number/rate of frozen initial prices at or below that stop, plus exact-price and actual configured safety-rejection counts. Those are no-entry diagnostics, not the retired directional loss-skip rule and not ordinary GTC zero-fills.
- A five-hour worker checkpoints and queues its successor only in the middle 13 minutes of a market—from one minute after open through one minute before close. One concurrency group serializes the strategy, and the watchdog is mode-preserving and v13-lane-only; it cannot restore v12 or convert shadow into live.
- Workflow-dispatch parameter overrides are validated and written back to `selected_live_strategy.json` before execution, then included in the material-event and end-of-run checkpoints. A change is accepted only while exchange/order state is flat. If recovery P&L is negative, its saved multiplier/base parameters remain authoritative until that recovery cycle resets; the new settings then govern the fresh cycle. Any non-approved config-hash difference still fails closed. The watchdog is the sole five-minute scheduler; the long worker has no independent cron, preventing redundant five-hour jobs from accumulating behind the singleton concurrency group.

### GitHub Actions inputs

The production worker presents nine manual inputs. Blank strategy values preserve the version already stored in `runtime-state-kxbtc15m-delayed-v13`, so watchdog and five-hour handoffs cannot overwrite a deliberate setting with an old default.

| Input | Meaning |
| --- | --- |
| `live_enabled` | Requests live execution, but only when both repository safety gates also permit it; default `false` |
| `reconcile_only` | Reconcile authoritative Kalshi state without opening exposure |
| `initial_shares` | Two-decimal starting base for a brand-new state; current default `1.00` |
| `scaling_multiplier` | Sets both recovery sizing and geometric profit-threshold growth |
| `max_share_cap` | Fixed absolute maximum quantity used only when base-linked capping is disabled; blank preserves it |
| `max_cap_per_base_share` | Cap per permanent-base share; default 100, so base 1 → cap 100 and base 2 → cap 200; 0 selects fixed-cap mode; blank preserves |
| `profit_threshold` | First realized-net-profit threshold for a permanent base increase |
| `shares_added_after_profit_threshold` | Two-decimal permanent base increment after each threshold crossing |
| `max_stop_loss_cents` | Direct protective-exit trigger from 10 through 51; current default 51; no maker-exit phase |

The controlled-restart workflow exposes only `source_run_id` and `target_live`. Run duration, sticky-direction lane, GTC order lifetime, quote timing, 40–49¢ analytics, checkpoint cadence, maximum position, and all other safety limits remain canonical configuration rather than routine UI knobs.

`KALSHI_SHADOW_ONLY=true` hard-forces `MODE=DRY_RUN`; `KALSHI_SHADOW_ONLY=false` does not independently enable live trading. To switch deliberately, set `KALSHI_SHADOW_ONLY=false` and `KALSHI_LIVE_ENABLED=true`, then run the controlled-restart workflow with `target_live=true` while the named source lane is flat. The handoff refuses boundary timing or persisted exposure, dispatches the current `main`, preserves state, and the replacement reconciles before creating risk. Reversing either repository gate disables live placement again. Credentials are referenced only by the names `KALSHI_PROD_API_KEY` and `KALSHI_PRIVATE_KEY`; they are never written to state, logs, artifacts, source, or README.

## Tests and operational commands

```bash
# Shared-core, replay, path, reconciliation, legacy hybrid regression, v13 delayed-band, and live safety suite.
PYTHONPATH=. .venv/bin/python -m unittest -v \
  tests.test_strategy_core tests.test_live_execution tests.test_maker_hybrid_v11 tests.test_delayed_band_v12 tests.test_reconciliation \
  tests.test_recovery_sizing tests.test_execution_path_model tests.test_historical_replay \
  tests.test_order_smoke_test tests.test_startup_order_check

# Canonical v13 shadow run (isolated $1,000 state, never real orders).
KALSHI_API_KEY_ID=... KALSHI_PEM_PATH=kalshi_private_key.pem \
  .venv/bin/python kalshi_live_trader.py --config selected_live_strategy.json \
  --state-file data/kalshi_shadow_delayed_band_v13_state.json \
  --audit-ledger data/kalshi_shadow_delayed_band_v13_audit.jsonl \
  --shadow-profile delayed_53_57_exit_51 --stop-price 0.51 --trading-mode shadow --dry-run --run-seconds 120

# Read-only reconciliation; it never creates an entry.
KALSHI_API_KEY_ID=... KALSHI_PEM_PATH=kalshi_private_key.pem \
  .venv/bin/python kalshi_live_trader.py --config selected_live_strategy.json \
  --state-file data/kalshi_live_delayed_band_v13_state.json --trading-mode live --reconcile-only
```

The test suite covers fixed outcomes, no loss-skip behavior, sticky hold/flip transitions, Decimal sizing, strict zero-fill invariants, recovery/base transitions, caps, funding checks, startup reconciliation, deterministic idempotency, provisional outcome timing, immutable ask-minus-one entry, no-fill/full/partial/cancelled maker entries, all 40–49¢ levels, winner drawdowns, touch-versus-fill separation, the direct 51¢ exit, partial-IOC residual retries, legacy hybrid regressions, duplicate ticks, restart with a pending exit, post-exit settlement analytics, shadow/live state parity, workflow anti-regression assertions, and the hard shadow-only live gate.

## Remaining risks

The public settlement API cannot prove historical execution paths. The dynamic maker-entry fill model does not know queue priority; shadow public trade-through evidence is conservative but is still not an exchange fill. A displayed bid at the 51¢ trigger may disappear or gap before an IOC reaches the exchange. Fee schedules, liquidity, stale/disconnected data, exit slippage, API behavior, market rules, and a changed directional regime may turn the modeled result negative. The fixed-price EV tables and the 135-trade selected cohort are not guarantees. Treat the backtest and shadow analytics as reproducible risk studies, not assurances of profitability or capital safety.
