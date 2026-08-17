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

[`selected_live_strategy.json`](selected_live_strategy.json) is the canonical base configuration used by the worker. It is versioned as `kxbtc15m-hybrid-live-v9` / schema `9`; this is a hard compatibility boundary for sticky directional state, immediate protected-IOC entry, fixed-profile stops, and durable audit telemetry. A v8 configuration or checkpoint is rejected before the worker can submit an order. Optimizer exports use the same schema, so a future action cannot silently reinterpret the retired maker strategy.

| Setting | Current value | Notes |
| --- | ---: | --- |
| Series | `KXBTC15M` | Discovered from Kalshi market metadata, not ticker arithmetic |
| Direction rule | **Sticky until directional win** | Seed inverse to the first prior result; hold the same side after a wrong prediction; flip only after that side settles correctly |
| Starting permanent base | **1.00 share** | Two-decimal `Decimal`, `ROUND_HALF_UP` |
| Entry | **Immediate protected IOC** | Submitted on the selected side at the fresh executable ask; this is Kalshi’s bounded market-order equivalent, never a resting maker order |
| Fresh-book wait | **Up to 60 seconds** | Wait only for a current executable book; a stale/missing book becomes a missed signal rather than an invented fill |
| Opening telemetry | **500 records** | Complete opening books remain audit/calibration evidence; they do not determine v9 order price |
| Price guard | **Strictly above the profile floor** | At/below 40¢ (or the selected 30/20/10¢ test floor) records no entry; it does not enter into an immediate stop |
| Shadow stop profiles | **40¢ / 30¢ / 20¢ / 10¢** | Each profile has separate state, recovery cycle, drawdown, ledger, configuration hash, workflow lane, and watchdog recovery |
| Default stop floor | **40¢ executable bid** | `sticky_stop_40` remains the reference profile; the other floors are evidence-gathering shadows |
| Stop | **Fixed profile floor** | A 54¢ entry in the 40¢ profile still exits only at an executable bid of **≤40¢** |
| Recovery multiplier | **1.01×** | The selection favors $1,000-survivability, not maximum modeled P&L |
| First base threshold | **$350.00** | Realized net P&L only |
| Threshold growth | **1.01×** | Geometric after each permanent-base step |
| Base increment | **+0.50 share** | Supports +0.25, +0.50, and +1.00 |
| Position cap | **100.00 shares** | Cap is applied after two-decimal sizing |
| Shadow balance | **$1,000.00** | Isolated from the live account state |
| Real-money mode | **Hard-disabled** | `KALSHI_SHADOW_ONLY=true` forces dry run even if a workflow requests live mode |

The selected configuration’s basis is recorded verbatim in its JSON: highest modeled $1,000 survival followed by lower P99 bankroll under the earlier base **49¢ contrarian** calibration. That ranking is not an expected-value claim for v9: sticky-direction signals, immediate market-equivalent execution, and fixed-floor stops require their own settlement replay and shadow evidence. `entry_price=0.49` remains a historical/replay reference, not a live ceiling or live order price.

### Immediate market-IOC entry and fixed-floor stop

At market open, the WebSocket worker uses the first **fresh complete top-of-book** observation for the selected side and submits exactly one immediate-or-cancel order at that selected-side executable ask. This is the safe exchange equivalent of a market buy: it either fills immediately at no worse than the observed ask or leaves no resting order. There is no post-only/GTC maker order, no three-second price-discovery gate, and no maker-cancellation/fallback path in the active v9 configuration.

The worker retains up to 500 opening quote observations for evidence. Each includes exchange/receive timestamps, quote ID/age, YES/NO bid/ask/depth, selected-side executable bid/ask/depth, and the submitted IOC quote. Shadow fills are explicitly labelled `fresh_displayed_top_of_book_ioc` and are limited by displayed depth; they are a hypothetical shadow fill, not a claim that Kalshi filled an exchange order.

An IOC is not submitted if the fresh selected-side ask is at or below the selected fixed stop floor. A 40¢ profile therefore does not create a position at 40¢ or less. The exit trigger uses the fresh selected-side executable **bid**: `bid <= fixed_stop_floor` submits a reduce-only IOC only for confirmed actual exposure. Actual entry price feeds P&L only; it never moves the v9 stop. All submissions, fills, no-entry outcomes, stop triggers, exits, worker-observed fill/stop latency, realized P&L, balance, and max drawdown are append-only audit facts.

Every market record reports actual entry composition. For v9 the expected nonzero category is `market_ioc`; legacy `maker_limit`/`mixed` categories remain readable only for archived v8 evidence. Cumulative method counters are rebuilt idempotently from per-market fill records across workflow handoffs.

### Sticky signal transition

The v9 signal has no loss-skip rule and is independent of execution. For each new market, the worker freezes the immediately preceding market’s realtime provisional outcome, later checks it against official settlement, and records the transition in both state and audit ledger:

```text
fresh state + previous YES  -> enter NO
entered NO + current settles YES -> enter NO again  (directional loss: hold)
entered NO + current settles NO  -> enter YES next  (directional win: flip)
entered YES + current settles NO -> enter YES again (directional loss: hold)
entered YES + current settles YES -> enter NO next  (directional win: flip)
```

Maker fills, IOC fills, zero fills, stops, recovery P&L, and permanent-base scaling never change that directional side. Only the completed market result relative to the prior selected side does. A provisional/official mismatch is preserved as an audit discrepancy; it never rewrites an already-submitted entry.

### Isolated stop experiments

Workflow input `shadow_profile` selects one of `sticky_stop_40`, `sticky_stop_30`, `sticky_stop_20`, or `sticky_stop_10`. A profile’s stop price is enforced by validation and cannot be silently overridden. Shadow files are independent:

| Profile | Fixed floor | Durable state | Append-only audit ledger |
| --- | ---: | --- | --- |
| `sticky_stop_40` | 40¢ | `data/kalshi_shadow_market_ioc_sticky_stop_40_state.json` | `data/kalshi_shadow_market_ioc_sticky_stop_40_audit.jsonl` |
| `sticky_stop_30` | 30¢ | `data/kalshi_shadow_market_ioc_sticky_stop_30_state.json` | `data/kalshi_shadow_market_ioc_sticky_stop_30_audit.jsonl` |
| `sticky_stop_20` | 20¢ | `data/kalshi_shadow_market_ioc_sticky_stop_20_state.json` | `data/kalshi_shadow_market_ioc_sticky_stop_20_audit.jsonl` |
| `sticky_stop_10` | 10¢ | `data/kalshi_shadow_market_ioc_sticky_stop_10_state.json` | `data/kalshi_shadow_market_ioc_sticky_stop_10_audit.jsonl` |

The v8 ledgers are preserved as retired diagnostic history. v9 starts in this separate namespace at a $1,000 shadow balance and 1.00 permanent base, so its recovery/P&L metrics cannot be contaminated by the known v8 synthetic-order cancellation defect. The watchdog evaluates each lane independently.

## Reconstructed historical directional results — prior inverse baseline

The following snapshot was regenerated from Kalshi’s public settlement endpoints on **2026-08-08** using the prior `inverse_latest_settlement` rule. The cache is intentionally ignored by Git because it is downloaded source data; the exact retrieval commands are below. It is a reproducibility baseline, **not** the v9 sticky-direction/IOC expected value.

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

Live signal timing is intentionally faster: it freezes a provisional prior outcome from the final fresh executable 99¢ bid before the boundary, produces the v9 sticky transition at the next market’s open, and later verifies it against official settlement. The historical optimizer’s `--signal-mode sticky_until_directional_win` rebuild uses the actual previous settlement as an explicitly labelled **provisional-outcome proxy**; it does not claim that the delayed public endpoint was available at the boundary. The historical API does not contain that final quote stream, so the proxy and the live provisional-quote mechanism are distinct evidence paths; their agreement must be measured in shadow rather than assumed.

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

The following is a **one-share, fixed-size, no-fee, 40¢-stop calculation** using the current 22,406 *prior inverse* fixed directional outcomes and the base 49¢ execution scenario. It does not include recovery sizing, permanent-base scaling, the 100-share cap, funding failures, slippage, or calibration uncertainty. It must not be read as v9 sticky-direction/IOC EV.

For entry price `e`, stop `s`, win rate `pW`, fill probability `f`, and joint 40¢-region probabilities `rW`/`rL`, the gross EV per eligible signal is:

```text
pW * ((f - rW) * (1 - e) + rW * (s - e))
+ (1 - pW) * ((f - rL) * (-e) + rL * (s - e))
```

| Mechanical price sensitivity | EV / eligible signal | EV / expected filled share | Gross / 1,000 eligible signals |
| --- | ---: | ---: | ---: |
| 49¢ entry, 40¢ stop | **+$0.02232** | **+$0.02625** | **+$22.32** |
| 50¢ entry, 40¢ stop | **+$0.01382** | **+$0.01625** | **+$13.82** |

The 50¢ row changes only payout math while holding the **49¢** base-fill/path scenario fixed. It is a sensitivity calculation, not a calibrated 50¢ maker-fill forecast. Neither fixed-price row is an expected-value claim for the v9 fresh-book IOC rule or its fixed stop floor: the historical rung model only identifies 40¢/30¢/20¢/10¢ touches, not actual IOC fills or stop slippage. The worker therefore records actual average entries, fixed stop floors, post-entry minimum executable bids, stop exits, and later official outcomes for stopped positions before that execution is assigned an EV. At either fixed price, one cent of fee per filled share would reduce the per-eligible-signal figure by approximately $0.00850 under the 85% participation assumption, before any slippage. A positive static EV is not a capital guarantee: nonlinear recovery sizing can still create drawdowns, cap hits, and funding failures.

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

Within that 1.11× reference only, $100/+1.00 had the highest median P&L and $125/+0.25 had the highest $100 completion / lowest quoted capital requirement. Neither is the selected $1,000-survivability candidate; 1.11× is materially more aggressive than 1.01×.

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

The intended reading is a risk trade-off, not a claim that higher recovery is better. The 1.01× candidate sacrifices modeled upside for far greater low-bankroll completion in the supplied reconstruction.

### Earlier inverse-baseline stop comparison

An earlier static inverse reconstruction ranked 40¢ first, followed closely by 10¢, then staged 40/30/20/10, 20¢, and 30¢. Its per-share gross estimates were 2.68¢, 2.64¢, 2.53¢, 2.44¢, and 2.37¢ respectively. This ranking is descriptive only and does not select a v9 sticky profile. The current implementation reruns all fixed stops on the new fixed sticky settlement sequence and reports stop results in `stop_optimization_results.csv`; v9 IOC shadow evidence is collected in the four independent ledgers above.

## Full reproducible backtest

Use Python 3.13 and the pinned research requirements. The commands create an ignored cache and a self-contained output directory; no live secrets are needed.

```bash
python3.13 -m venv .venv
.venv/bin/pip install -r requirements_kalshi_hybrid_backtest.txt

# 1. Download/cache actual Kalshi settlement outcomes and reconstruct the
#    prior inverse baseline. Use --signal-mode sticky_until_directional_win
#    below for the active v9 fixed-settlement proxy replay.
.venv/bin/python kalshi_settlement_loader.py --refresh \
  --cache data/raw/kalshi_kxbtc15m_settlements.json \
  --signals historical_signals.parquet

# 2. Validate the calibration layer alone (100,000 simulated calibration draws).
.venv/bin/python calibration.py \
  --output outputs/kalshi_hybrid_backtest/calibration_report.csv \
  --replications 100000 --seed 42

# 3. Full v9 sticky-direction 49¢ screen, stop finalists, 100,000-rep final
#    runs, walk-forward, stress tests, and plots. The historical proxy is
#    labelled in every result; it does not invent intramarket quote history.
.venv/bin/python optimizer.py \
  --output-dir outputs/kalshi_hybrid_backtest/base_49c \
  --entry-price .49 --signal-mode sticky_until_directional_win --execution-scenario base_case \
  --coarse-simulations 5000 --final-simulations 100000 \
  --walkforward-simulations 500 --finalists 15 --seed 42

# 4. Optional 50¢ fixed-price sensitivity. This reuses the 49¢ path
#    calibration; it does not model the live fresh-book IOC execution rule.
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
| `optimization_summary.md` | Human-readable rankings and the exported live configuration provenance |

The optimizer uses common random numbers for competing configurations, keeps every actual directional settlement fixed, and applies the same `strategy_core.py` state transitions as the live worker. It reports P&L, drawdown, bankroll, cap-binding, recovery-cycle, fill, zero-fill, and stop distributions. Calibration uncertainty can be added with `--calibration-uncertainty-draws N`.

## Production behavior and persistence

- The live engine and historical replay share the recovery/base-sizing transitions. A filled trade updates realized net P&L; a zero fill is exactly $0 and changes neither the recovery exponent nor permanent base.
- Recovery exponent increases after **every filled closed trade** while cumulative recovery-cycle P&L remains negative. It resets only when that cumulative amount reaches at least $0.
- Permanent-base steps use realized net P&L only. No unrealized value, cancelled order, or zero fill can scale the base.
- Startup reconciles Kalshi balance, open managed orders, positions, fills, and settlements before any entry. Unknown or ambiguous ownership fails closed; Kalshi is authoritative.
- Client order IDs are deterministic, partial fills use actual quantities, exits are reduce-only where supported, and the same market cannot be counted twice after restart.
- The worker retains fresh complete selected-side books through the opening minute as evidence, but v9 submits its single price-protected IOC as soon as a fresh executable ask exists. It never posts a maker order, waits for a maximum-opening-price sample, or revives the retired maker fallback.
- The stop is the fixed selected profile floor: for the reference profile, a fresh executable selected-side bid at **≤40¢** submits a reduce-only IOC only for confirmed filled exposure. An entry above 50¢ does not raise that trigger; an entry at/below the floor is rejected before exposure is created.
- State and append-only audit ledgers are separate for each `data/kalshi_shadow_market_ioc_*` profile and for `data/kalshi_live_market_ioc_*`. Every v9 shadow profile starts at $1,000 and 1.00 share, tracks realized P&L, peak equity, and max drawdown, and cannot mutate another profile or a live strategy state.
- Every audit JSONL record is appended, flushed, and `fsync`ed before the worker resumes order/position management. Its companion strategy state is atomically written and `fsync`ed immediately after every audit event; therefore a state transition, fill observation, stop event, funding failure, settlement, reconciliation result, and handoff is checkpointed while the worker is running—not merely at its end. GitHub state commits are additionally coalesced at the configured `durable_checkpoint_interval_seconds` (default: 5 seconds); a pending material event is retried by the ordinary worker checkpoints once that interval expires, without publishing every quote update.
- Each market ledger record includes opening quote evidence plus observed execution timing: first-fresh-book lag, market-open-to-IOC submission, market-open-to-first-fill, submission-to-first-fill, entry completion, first-fill-to-stop-trigger, stop-trigger-to-first-exit submission, and stop-trigger-to-observed-flat-position. These are explicitly **worker-observed** timestamps; they do not claim unavailable matching-engine fill times. GitHub Actions heartbeats report active state, balance, cumulative shadow P&L, max drawdown, completed/stop/settlement counts, IOC composition, and entry/stop latency medians. The durable `execution_timing_metrics` state provides count/mean/median/P95/max summaries rebuilt from those per-market records across restarts.
- A five-hour worker checkpoints and queues its successor only in the middle 13 minutes of a market—from one minute after open through one minute before close. The watchdog is serialized **per profile** and mode-preserving; it cannot convert a shadow worker into a live worker.

`KALSHI_SHADOW_ONLY=true` is the persistent current repository variable and hard-forces `MODE=DRY_RUN` in both workflow and Python code. Even setting `KALSHI_LIVE_ENABLED=true` and supplying live inputs cannot place a real order until that hard lock is deliberately removed. Credentials are referenced only by the names `KALSHI_PROD_API_KEY` and `KALSHI_PRIVATE_KEY`; they are never written to state, logs, artifacts, source, or README.

## Tests and operational commands

```bash
# Shared-core, replay, path, reconciliation, and live-execution safety suite.
PYTHONPATH=. .venv/bin/python -m unittest -v \
  tests.test_strategy_core tests.test_live_execution tests.test_reconciliation \
  tests.test_recovery_sizing tests.test_execution_path_model tests.test_historical_replay

# Dry-run 30¢ profile (uses its own isolated $1,000 shadow state).
KALSHI_API_KEY_ID=... KALSHI_PEM_PATH=kalshi_private_key.pem \
  .venv/bin/python kalshi_live_trader.py --config selected_live_strategy.json \
  --state-file data/kalshi_shadow_market_ioc_sticky_stop_30_state.json \
  --audit-ledger data/kalshi_shadow_market_ioc_sticky_stop_30_audit.jsonl \
  --shadow-profile sticky_stop_30 --stop-price 0.30 --dry-run --run-seconds 120

# Read-only reconciliation; it never creates an entry.
KALSHI_API_KEY_ID=... KALSHI_PEM_PATH=kalshi_private_key.pem \
  .venv/bin/python kalshi_live_trader.py --config selected_live_strategy.json --reconcile-only
```

The test suite covers fixed outcomes, no loss-skip behavior, symmetric sticky hold/flip transitions, Decimal fractional sizing, zero-fill invariants, recovery/base transitions, path nesting, P&L conventions, caps, funding checks, restart reconciliation, order idempotency, provisional-outcome handling, immediate protected IOC entry, rejection at/under the fixed stop, fixed-floor stop behavior for entries above 50¢, stale synthetic-maker cancellation repair, profile-isolated safe handoff timing, v8-state rejection, and the hard shadow-only live gate.

## Remaining risks

The public settlement API cannot prove historical execution paths. The model does not yet have a representative sample for immediate fresh-book IOC fills or their slippage, and the immediate provisional-quote signal must earn its reliability through live shadow verification. A displayed ask/size is not a guarantee of an IOC fill at that price. Fee schedules, liquidity, stale/disconnected data, stop slippage, API behavior, market rules, and a changed directional regime may turn the modeled result negative. Treat the backtest as a reproducible risk study, not an assurance of profitability or capital safety.
