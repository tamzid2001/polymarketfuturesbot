# KXBTC15M hybrid Kalshi strategy

This repository’s active Kalshi path is a KXBTC15M **sticky-direction shadow strategy** with a shared historical-replay and live state engine. The research method is:

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

[`selected_live_strategy.json`](selected_live_strategy.json) is the canonical base configuration. The active contract is `kxbtc15m-delayed-band-live-v12` / schema `12`. It promotes the observed delayed-entry cohort into the shared shadow/live execution engine; it is not an auxiliary analytics order. Python and GitHub Actions independently assert the version, entry band, GTC/post-only order type, 2.50× sizing, and 51/52/50 stop hierarchy before execution. v11 and earlier checkpoints use different paths and a different runtime branch, so an old worker cannot reinterpret them as v12 state.

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
| Canonical workflow lane | **`delayed_53_57_stop_50`** | The only lane maintained by the production watchdog; real orders remain gated off by default |
| Hybrid trigger | **Executable bid ≤51¢** | Cancels and confirms any unfilled entry remainder, then starts the maker exit |
| Hybrid maker exit | **52¢ GTC reduce-only sale** | A position is not treated as exited until actual fills exist |
| Hybrid hard stop | **Executable bid ≤50¢** | Confirms maker cancellation/fills, then IOC-exits only authoritative residual exposure |
| Recovery multiplier | **2.50×** | Advances after every filled closed trade while cumulative cycle P&L remains negative |
| Recovery exponent ceiling | **Disabled (`0`)** | Sizing remains uncapped by exponent; the independently configured fixed position cap still applies |
| First base threshold | **$350.00** | Realized net P&L only |
| Threshold growth | **2.50×** | Geometric after each permanent-base step |
| Base increment | **+0.50 share** | Supports +0.25, +0.50, and +1.00 |
| Position cap | **100.00 fixed by default; configurable hard cap** | Actions input `max_share_cap` sets a constant maximum quantity; blank preserves the durable value |
| Shadow balance | **$1,000.00** | Isolated from the live account state |
| Real-money mode | **User-controlled; source defaults are shadow-safe** | Actual runtime mode is logged/checkpointed. Live requires `KALSHI_SHADOW_ONLY=false`, `KALSHI_LIVE_ENABLED=true`, and an explicit workflow `live_enabled=true` request |

### Configurable fixed maximum share size

The optional Actions input **`max_share_cap`** is a **fixed maximum number of
shares per market**, not a multiplier. Enter `100`, `200`, or another positive
finite value with at most two decimal places, no smaller than the configured
starting base. Leaving it blank preserves the saved cap (100.00 by default).

At base 1.00 and recovery multiplier 2.50, the quantities are calculated from
`base × 2.50 ** exponent`, rounded HALF_UP to 0.01, then limited to the hard cap:

| Recovery exponent | Cap 100 | Cap 200 |
| ---: | ---: | ---: |
| 0 | 1.00 | 1.00 |
| 1 | 2.50 | 2.50 |
| 2 | 6.25 | 6.25 |
| 3 | 15.63 | 15.63 |
| 4 | 39.06 | 39.06 |
| 5 | 97.66 | 97.66 |
| 6+ (while unrecovered) | 100.00 | 200.00 |

The cap **does not increase when permanent base increases**. Recovery exponent
advances after every filled completed trade while total recovery-cycle net P&L
is negative, including an individual profitable trade that has not recovered
the deficit. Only cycle P&L >= 0 resets to base. Zero fills change neither state.
Funding checks and loss breakers are unchanged; a larger cap does not supply
additional buying power or guarantee recovery.

The input maps directly to `--max-position`, persisted as a Decimal
string `max_position` in the chosen configuration, cycle parameters,
and each signal's configuration snapshot. Each market also stores
`effective_position_cap` and `effective_position_cap_after`; heartbeats print
`cap`. Both historical reference replay and live/shadow execution use the same
fixed cap calculation. Optimizer exports preserve the selected cap instead of
silently substituting 100. Historical results are not reclassified.

**Blank means preserve**, including on watchdog restarts and normal handoffs. An
existing configuration keeps its `max_position`;
publishing the code does not silently expand active live risk. A cap change
is accepted only with no outstanding order/position. A negative recovery cycle
continues using its saved old parameters; the new cap applies to a fresh cycle.
No live/shadow state is reset. The next worker checks out `main` and restores only
durable data/configuration, never an older runner source file.

### Frozen observed-ledger selection evidence

The v12 choice is based on the supplied frozen replay of the delayed cohort with a maximum filled entry of 57¢, the ask-minus-1¢ entry, 51/52/50 hybrid stop, 1.00 starting share, 2.50× recovery, and 100-share cap:

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

These are observed-ledger/replay results, not live fills and not a guaranteed return. The positive 2.50× result is nonlinear and highly sequence-dependent; 52 of 73 directional winners were stopped before settlement. Fees, queue position, partial fills, and live stop slippage were not established by those headline figures. A $150 deposit exceeds the reported $50.77 historical drawdown and can fund the configured maximum 100-share order at the 57¢ ceiling before fees, but that does **not** prove $150 survival outside the 135-trade sample. The worker therefore retains funding checks, the 100-share cap, reconciliation, and persistent loss circuit breakers.

### Delayed maker entry and hybrid stop

Every second, the worker requests a bounded KXBTC15M close-time window and maintains WebSocket subscriptions for the predecessor, current market, and API-provided successor. During the final five seconds (15 is supported), exactly one side must show a fresh executable bid ≥99¢ to supply the next sticky-direction transition. Neither side, both sides, stale evidence, or an unavailable predecessor fails closed.

At open, the pre-subscribed WebSocket freezes the earliest fresh **price-only** selected-side executable ask. This opening value is an eligibility fact, not the order price: it must be below 53¢. Beginning at 60 seconds after open, the first fresh complete selected-side ask at or above 53¢ becomes decisive. The worker freezes `entry_limit_cents = qualifying_ask_cents - 1`. Limits of 52–57¢ are eligible. If the first qualifying ask implies 58¢ or more, the market becomes `ENTRY_FILTERED`; a later cheaper quote cannot revive it and zero/filtered markets never advance recovery.

The worker submits one deterministic GTC/post-only buy for the selected side. `maker_order_time_in_force=good_till_canceled`, `entry_order_lifetime=until_filled_or_market_close`, and `entry_timeout_seconds=0` are fail-closed contracts. The order rests until fully filled, market close, or confirmed cancellation required by a stop. Live mode uses Kalshi orders and fills as authoritative. Shadow mode requires a post-submission public trade at or below the buy limit; a displayed touch or submission is never called a fill. Partial fills create only the observed exposure and all P&L/recovery accounting uses that quantity.

For the default 50¢ hard stop, an executable selected-side bid ≤51¢ first cancels and confirms any unfilled entry remainder and starts a post-only/reduce-only maker sale at 52¢. If bid reaches ≤50¢ before that maker exit completely fills, the worker confirms the maker cancellation, reconciles its fills, and sends a reduce-only IOC only for the remaining position. Unknown cancellation or position state blocks new exposure. A partial maker exit can never cause the IOC to sell the original full quantity again.

Live-stop safety contract 1 adds a durable exit-order intent **before** each POST. A lost response is recovered using the exact client order ID, not retried blindly with a new ID. An empty lookup does not prove rejection. Definitive HTTP rejections are recorded separately from uncertain timeouts/server failures; failed maker attempts cannot leave a fictitious uncanceled order blocking the hard stop. A price gap straight to ≤50¢ bypasses the maker attempt and starts the hard exit in the same management pass, after entry cancellation. Once triggered, the hard stop remains latched through price recovery and process restarts. IOC fills/fees are refreshed before sizing any residual retry. The trigger price is not a guaranteed execution price: gaps, unavailable liquidity, exchange pauses, disconnects and unknown cancellations can delay or worsen an exit. These paths are covered by offline fault-injection tests in `tests/test_live_stop_safety.py`; passing those tests is not evidence of successful real exchange fills.

`ENTRY_FILTERED` is a strategy decision, not an exchange rejection. For example, a qualifying ask of 60¢ implies a 59¢ limit, exceeding the 57¢ ceiling. The heartbeat now retains that observed ask, derived limit and filter reason even when no order was submitted. Separate `analytics_only=true` fills do not affect the executable strategy balance. Live activation still requires the user-controlled gates; this safety patch does not turn on real-money trading.

### User-controlled live switch

Deployment does **not** enable real-money orders. After reviewing the strategy and reconciliation, the account owner can explicitly enable it:

1. Repository **Settings → Secrets and variables → Actions → Variables**: set `KALSHI_LIVE_ENABLED=true` and `KALSHI_SHADOW_ONLY=false`. Keep `KALSHI_MAINTENANCE_MODE=false`. These are permission gates; on a shadow deployment they do not start live trading by themselves.
2. **Actions → Kalshi KXBTC15M Hybrid Live → Run workflow**, branch `main`: check **LIVE SWITCH** (`live_enabled`), leave **READ-ONLY CHECK** (`reconcile_only`) unchecked.
3. For the frozen defaults, initial shares = `1.00`, scaling multiplier = `2.50`, profit threshold = `350.00`, shares added = `0.50`, hard stop = `50`. Blank inputs preserve the durable configuration, not necessarily the original defaults. Do not request sizing changes during active exposure/recovery.
4. Verify the worker logs **`MODE=LIVE`**, a successful authenticated reconciliation, then accepted exchange order IDs and actual fills. `DRY_RUN`, `RECONCILE_ONLY`, `ENTRY_FILTERED` and `analytics_only=true` are not proof of a real trade. A blocked live request now fails instead of silently becoming shadow.

If a shadow worker is still running, the singleton queues a normal live dispatch; it does not interrupt it. To transition sooner, use **Controlled Restart — Kalshi KXBTC15M Hybrid** with its current `source_run_id` and `target_live=true`. This refuses handoff unless the checkpoint is flat and the market is within the safe 1–14 minute window. Never force-cancel a live worker managing an order or position. The watchdog resumes the last explicitly selected trading mode and uses current `main`; read-only audits do not change that selection. To revoke live permission, restore `KALSHI_SHADOW_ONLY=true` and `KALSHI_LIVE_ENABLED=false`; changing repository variables does not instantly change an already running process, so existing exposure still needs its manager.

The live account uses actual exchange cash, not the $1,000 shadow balance. The delayed strategy still requires an opening ask **below 53¢**, then a first qualifying ask **≥53¢ at/after 60 seconds**, with **ask−1¢ ≤57¢**. Thus the limit band is **52–57¢**, not an unconditional purchase of every market opening above 53¢. Starting with $120 is not a guarantee of recovery or solvency. Exchange rejection, cancellation delay and stop slippage remain possible. The adapter records V2 `average_fee_paid × actual fill_count` when total-fee fields are absent; explicit maker/taker fee totals take precedence.

Durable mode evidence is stored under `execution_context` in each state checkpoint and included in every new JSONL audit event: `mode` (`LIVE`, `DRY_RUN`, or `RECONCILE_ONLY`), `state_namespace`, `real_order_submission_enabled`, `workflow_run_id`, `source_commit`, and `stop_safety_contract`. A checkpoint stamped as live cannot be loaded as shadow or vice versa. Read-only reconciliation retains the live namespace but records order permission as false. The runtime branch remains `runtime-state-kxbtc15m-delayed-v12`; live/shadow state and audit filenames remain separate. Legacy unstamped events remain legacy evidence, not retroactively labeled live trades.

Every signal also maintains independent analytics for 40, 41, …, 49¢. It records executable-ask touches separately from conservative simulated fills. At official settlement, it records winner capture and missed-winner rates, the minimum selected-side ask, eventual-winner maximum drawdown, and whether a stopped position would later have won. A stopped record remains observed until official settlement; verification never changes already-realized recovery P&L.

The ledger persists the opening ask, qualifying ask, derived limit, requested notional, exchange/client order IDs, actual fill quantities/prices/fees, stop timestamps, residual exposure, settlement, drawdown, and false-stop status. The heartbeat prints `delayed_entry_status`, `delayed_opening_ask`, `delayed_trigger_ask`, and `delayed_limit`. The corrected timestamp parser accepts ISO time, epoch seconds, and epoch milliseconds; this prevents persisted delayed fills from silently bypassing stop analytics after restart.

### Sticky signal transition

The v12 signal has no loss-skip rule and is independent of execution. For each new market, the worker freezes the immediately preceding market’s realtime provisional outcome, later checks it against official settlement, and records the transition in both state and audit ledger:

```text
fresh state + previous YES  -> enter NO
entered NO + current settles YES -> enter NO again  (directional loss: hold)
entered NO + current settles NO  -> enter YES next  (directional win: flip)
entered YES + current settles NO -> enter YES again (directional loss: hold)
entered YES + current settles YES -> enter NO next  (directional win: flip)
```

Entry fills, zero fills, hybrid exits, recovery P&L, and permanent-base scaling never change that directional side. Only the completed market result relative to the prior selected side does. A provisional/official mismatch is preserved as an audit discrepancy; it never rewrites an already-submitted entry.

### Durable namespace and archived comparisons

The canonical worker, watchdog, and controlled restart now target only the v12 delayed-band lane:

| Lane | Durable state | Append-only audit ledger | Runtime ref |
| --- | --- | --- | --- |
| Canonical v12 shadow | `data/kalshi_shadow_delayed_band_v12_state.json` | `data/kalshi_shadow_delayed_band_v12_audit.jsonl` | `runtime-state-kxbtc15m-delayed-v12` |
| Canonical v12 live | `data/kalshi_live_delayed_band_v12_state.json` | `data/kalshi_live_delayed_band_v12_audit.jsonl` | `runtime-state-kxbtc15m-delayed-v12` |

All earlier state, IOC, opening-entry, and stop-comparison files remain forensic evidence only. They are not restored by the v12 workflow. A fresh v12 live state starts at exactly 1.00 share; its recovery state is not copied from the v11 shadow experiment.

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

The following snapshot was regenerated from Kalshi’s public settlement endpoints on **2026-08-08** using the prior `inverse_latest_settlement` rule. The cache is intentionally ignored by Git because it is downloaded source data; the exact retrieval commands are below. It is a reproducibility baseline, **not** the v12 delayed-entry/hybrid-stop expected value.

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

Live signal timing is intentionally faster: it freezes a provisional prior outcome from the final fresh executable 99¢ bid before the boundary, produces the v12 sticky transition at the next market’s open, and later verifies it against official settlement. The historical optimizer’s `--signal-mode sticky_until_directional_win` rebuild uses the actual previous settlement as an explicitly labelled **provisional-outcome proxy**; it does not claim that the delayed public endpoint was available at the boundary. The historical API does not contain that final quote stream, so the proxy and the live provisional-quote mechanism are distinct evidence paths; their agreement must be measured in shadow rather than assumed.

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

The following is a **one-share, fixed-size, no-fee, 40¢-stop calculation** using the current 22,406 *prior inverse* fixed directional outcomes and the base 49¢ execution scenario. It does not include recovery sizing, permanent-base scaling, the 100-share cap, funding failures, slippage, the new dynamic entry, or calibration uncertainty. It must not be read as v12 delayed-entry/hybrid-stop EV.

For entry price `e`, stop `s`, win rate `pW`, fill probability `f`, and joint 40¢-region probabilities `rW`/`rL`, the gross EV per eligible signal is:

```text
pW * ((f - rW) * (1 - e) + rW * (s - e))
+ (1 - pW) * ((f - rL) * (-e) + rL * (s - e))
```

| Mechanical price sensitivity | EV / eligible signal | EV / expected filled share | Gross / 1,000 eligible signals |
| --- | ---: | ---: | ---: |
| 49¢ entry, 40¢ stop | **+$0.02232** | **+$0.02625** | **+$22.32** |
| 50¢ entry, 40¢ stop | **+$0.01382** | **+$0.01625** | **+$13.82** |

The 50¢ row changes only payout math while holding the **49¢** base-fill/path scenario fixed. It is a sensitivity calculation, not a calibrated 50¢ maker-fill forecast. Neither fixed-price row is an expected-value claim for v12: the historical API cannot tell whether a delayed dynamic maker order filled or whether the 51/52/50 hybrid exit completed. The v12 ledger therefore measures actual/simulated entry price, touch versus fill evidence, fees, partial exits, stop mechanism, and later official outcome. At either fixed price, one cent of fee per filled share would reduce the per-eligible-signal figure by approximately $0.00850 under the 85% participation assumption, before any slippage. A positive static EV is not a capital guarantee: nonlinear recovery sizing can still create drawdowns, cap hits, and funding failures.

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

Within that archived 1.11× reference only, $100/+1.00 had the highest median P&L and $125/+0.25 had the highest $100 completion / lowest quoted capital requirement. It models a different fixed-price execution process and is not the source of the active v12 parameters.

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

An earlier static inverse reconstruction ranked 40¢ first, followed closely by 10¢, then staged 40/30/20/10, 20¢, and 30¢. Its per-share gross estimates were 2.68¢, 2.64¢, 2.53¢, 2.44¢, and 2.37¢ respectively. This is descriptive archived evidence for a different strategy. The production workflow accepts only the v12 delayed 53–57¢ lane; archived optimizer output cannot activate retired Actions.

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

The optimizer uses common random numbers for competing configurations, keeps every actual directional settlement fixed, and applies the same `strategy_core.py` recovery/base transitions as the live worker. It reports P&L, drawdown, bankroll, cap-binding, recovery-cycle, fill, zero-fill, and stop distributions. Calibration uncertainty can be added with `--calibration-uncertainty-draws N`. Because the settlement API cannot reproduce the v12 quote-timed 52–57¢ entry cohort, generic optimizer rows are no longer automatically exported as live configuration; only a row explicitly tagged `execution_profile=delayed_53_57_stop_50` can use the guarded export function.

## Production behavior and persistence

- The live engine and historical replay share the recovery/base-sizing transitions. A filled trade updates realized net P&L; a zero fill is exactly $0 and changes neither the recovery exponent nor permanent base.
- Recovery exponent increases after **every filled closed trade** while cumulative recovery-cycle P&L remains negative. It resets only when that cumulative amount reaches at least $0.
- `max_recovery_exponent=0` is the explicit disabled sentinel. The shared shadow/live engine does not stop the 2.50× sequence at an arbitrary exponent; the independent 100-share position limit, funding check, recovery-loss breaker, and daily-loss breaker remain active.
- Permanent-base steps use realized net P&L only. No unrealized value, cancelled order, or zero fill can scale the base.
- Startup reconciles Kalshi balance, open managed orders, positions, fills, and settlements before any entry. Unknown or ambiguous ownership fails closed; Kalshi is authoritative.
- Client order IDs are deterministic, partial fills use actual quantities, exits are reduce-only where supported, and the same market cannot be counted twice after restart.
- The worker discovers a bounded previous/current/upcoming market window every second using `min_close_ts`/`max_close_ts`, subscribes the API-provided successor before open, and keeps the ending market subscribed for final 99¢ executable-bid inference. The first complete opening ask establishes eligibility; the first fresh qualifying ask at/after 60 seconds freezes one deterministic ask-minus-1¢ GTC order. There is no IOC entry fallback and later quotes cannot move the limit.
- The hybrid stop defaults to 51¢ trigger / 52¢ maker sale / 50¢ hard-stop threshold. The one hard-stop workflow input moves all three together; entry price never shifts them. Actual entry and exit fills, quantities, fees, and residual exposure drive accounting.
- Shadow and live state are isolated at `data/kalshi_shadow_delayed_band_v12_*` and `data/kalshi_live_delayed_band_v12_*`. The v12 shadow lane starts at $1,000 and the configured initial base (1.00 share by default), and tracks realized P&L, peak equity, maximum drawdown, entry filtering, false stops, and timing.
- Every audit JSONL record is appended, flushed, and `fsync`ed before the worker resumes order/position management. Its companion strategy state is atomically written and `fsync`ed immediately after every audit event; therefore a state transition, fill observation, stop event, funding failure, settlement, reconciliation result, and handoff is checkpointed locally while the worker is running—not merely at its end. Remote checkpoints are coalesced every 30 seconds and force-update one parentless `runtime-state-kxbtc15m-delayed-v12` snapshot with an exact lease. Snapshot schema v2 deterministically gzip-compresses each durable file in independent 8 MiB source chunks, verifies every compressed and uncompressed digest/size on restore, and reuses unchanged append-only ledger chunks. This prevents GitHub's 100 MiB single-blob limit from breaking a handoff as the ledger grows. The branch contains only allow-listed KXBTC15M payload chunks plus its manifest; it cannot accumulate ordinary code history or inherit older strategy state.
- Each market ledger record includes the immutable first price-only opening reference, its completeness status, separately timed first displayed-depth book, derived limit, exchange/client order IDs, partial fills, and maker/taker status where exposed. Timing includes exchange-price lag, worker-observation lag, depth-after-price lag, market-open-to-submission, market-open-to-first-fill, submission-to-first-fill, entry completion, first-fill-to-trigger, trigger-to-maker submission, and trigger-to-observed-flat position. Heartbeats report `first_price_quote_lag`, `first_depth_quote_lag`, and `opening_price_coverage` separately; delayed or missing price history is never relabeled as a complete opening quote. Five-minute tables print every 40–49¢ level, winner capture/misses, drawdown buckets, and hybrid-stop outcomes. They also print, separately for every hypothetical stop from 40¢ through 49¢, the number/rate of frozen initial prices at or below that stop, plus exact-price and actual configured safety-rejection counts. Those are no-entry diagnostics, not the retired directional loss-skip rule and not ordinary GTC zero-fills.
- A five-hour worker checkpoints and queues its successor only in the middle 13 minutes of a market—from one minute after open through one minute before close. One concurrency group serializes the strategy, and the watchdog is mode-preserving and v12-lane-only; it cannot restore v11 or convert shadow into live.
- Workflow-dispatch parameter overrides are validated and written back to `selected_live_strategy.json` before execution, then included in the material-event and end-of-run checkpoints. A change is accepted only while exchange/order state is flat. If recovery P&L is negative, its saved multiplier/base parameters remain authoritative until that recovery cycle resets; the new settings then govern the fresh cycle. Any non-approved config-hash difference still fails closed. The watchdog is the sole five-minute scheduler; the long worker has no independent cron, preventing redundant five-hour jobs from accumulating behind the singleton concurrency group.

### GitHub Actions inputs

The production worker presents only seven manual inputs. Blank strategy values preserve the version already stored in `runtime-state-kxbtc15m-delayed-v12`, so watchdog and five-hour handoffs cannot overwrite a deliberate setting with an old default.

| Input | Meaning |
| --- | --- |
| `live_enabled` | Requests live execution, but only when both repository safety gates also permit it; default `false` |
| `reconcile_only` | Reconcile authoritative Kalshi state without opening exposure |
| `initial_shares` | Two-decimal starting base for a brand-new state; current default `1.00` |
| `scaling_multiplier` | Sets both recovery sizing and geometric profit-threshold growth |
| `profit_threshold` | First realized-net-profit threshold for a permanent base increase |
| `shares_added_after_profit_threshold` | Two-decimal permanent base increment after each threshold crossing |
| `max_stop_loss_cents` | Hard-stop price `H` from 10 through 50; trigger is `H+1` and maker exit is `H+2`; current default 50 |

The controlled-restart workflow exposes only `source_run_id` and `target_live`. Run duration, sticky-direction lane, GTC order lifetime, quote timing, 40–49¢ analytics, checkpoint cadence, maximum position, and all other safety limits remain canonical configuration rather than routine UI knobs.

`KALSHI_SHADOW_ONLY=true` is the current repository setting and hard-forces `MODE=DRY_RUN` in both workflow and Python code. To switch deliberately, set `KALSHI_SHADOW_ONLY=false` and `KALSHI_LIVE_ENABLED=true`, then run the controlled-restart workflow with `target_live=true` while the named source lane is flat. The handoff refuses boundary timing or persisted exposure, dispatches the current `main`, preserves state, and the replacement reconciles before creating risk. Reversing either repository gate disables live placement again. Credentials are referenced only by the names `KALSHI_PROD_API_KEY` and `KALSHI_PRIVATE_KEY`; they are never written to state, logs, artifacts, source, or README.

## Tests and operational commands

```bash
# Shared-core, replay, path, reconciliation, v11 regression, v12 delayed-band, and live safety suite.
PYTHONPATH=. .venv/bin/python -m unittest -v \
  tests.test_strategy_core tests.test_live_execution tests.test_maker_hybrid_v11 tests.test_delayed_band_v12 tests.test_reconciliation \
  tests.test_recovery_sizing tests.test_execution_path_model tests.test_historical_replay

# Canonical v12 shadow run (isolated $1,000 state, never real orders).
KALSHI_API_KEY_ID=... KALSHI_PEM_PATH=kalshi_private_key.pem \
  .venv/bin/python kalshi_live_trader.py --config selected_live_strategy.json \
  --state-file data/kalshi_shadow_delayed_band_v12_state.json \
  --audit-ledger data/kalshi_shadow_delayed_band_v12_audit.jsonl \
  --shadow-profile delayed_53_57_stop_50 --stop-price 0.50 --trading-mode shadow --dry-run --run-seconds 120

# Read-only reconciliation; it never creates an entry.
KALSHI_API_KEY_ID=... KALSHI_PEM_PATH=kalshi_private_key.pem \
  .venv/bin/python kalshi_live_trader.py --config selected_live_strategy.json \
  --state-file data/kalshi_live_delayed_band_v12_state.json --trading-mode live --reconcile-only
```

The test suite covers fixed outcomes, no loss-skip behavior, sticky hold/flip transitions, Decimal sizing, strict zero-fill invariants, recovery/base transitions, caps, funding checks, startup reconciliation, deterministic idempotency, provisional outcome timing, immutable ask-minus-one entry, no-fill/full/partial/cancelled maker orders, all 40–49¢ levels, winner drawdowns, touch-versus-fill separation, full maker exits, hard-stop fallback, partial-maker residual exits, duplicate ticks, restart with a pending maker exit, post-stop settlement analytics, shadow/live state parity, workflow anti-regression assertions, and the hard shadow-only live gate.

## Remaining risks

The public settlement API cannot prove historical execution paths. The dynamic maker fill model does not know queue priority; shadow public trade-through evidence is conservative but is still not an exchange fill. A displayed bid that could execute a maker exit may disappear before a live fill. Fee schedules, liquidity, stale/disconnected data, stop slippage, API behavior, market rules, and a changed directional regime may turn the modeled result negative. The fixed-price EV tables and the 135-trade selected cohort are not guarantees. Treat the backtest and shadow analytics as reproducible risk studies, not assurances of profitability or capital safety.
