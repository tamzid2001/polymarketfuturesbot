# KXBTC15M hybrid Kalshi strategy

This repository’s active Kalshi path is a KXBTC15M contrarian strategy with a shared historical-replay and live state engine. The research method is:

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

[`selected_live_strategy.json`](selected_live_strategy.json) is the only canonical persisted configuration used by the live workflow. It is versioned as `kxbtc15m-hybrid-live-v4` / schema `4`; a legacy configuration is rejected before the worker can run. Optimizer exports use the same schema, so a future action cannot silently reinterpret an old configuration.

| Setting | Current value | Notes |
| --- | ---: | --- |
| Series | `KXBTC15M` | Discovered from Kalshi market metadata, not ticker arithmetic |
| Starting permanent base | **1.00 share** | Two-decimal `Decimal`, `ROUND_HALF_UP` |
| Opening-price discovery | **3 seconds** | Retains fresh complete selected-side quotes and takes their maximum executable ask |
| Entry | **Opening maximum − 1¢ post-only limit** | For example, a 52¢ maximum posts at 51¢; there is **no 50¢ ceiling** |
| Maker window / telemetry | **15 seconds / 500 records** | The maker order expires at 15 seconds; complete opening books are retained for calibration |
| Fallback | One protected IOC | Only at a fresh executable ask **strictly above 40¢**; it is not an unbounded market order |
| Stop floor | **40¢ executable bid** | Fills at or below 50¢ retain this fixed stop |
| Stop above 50¢ entry | **Entry-adjusted** | Actual average entry − 10¢: 52¢ entry → 42¢ stop |
| Recovery multiplier | **1.01×** | The selection favors $1,000-survivability, not maximum modeled P&L |
| First base threshold | **$350.00** | Realized net P&L only |
| Threshold growth | **1.01×** | Geometric after each permanent-base step |
| Base increment | **+0.50 share** | Supports +0.25, +0.50, and +1.00 |
| Position cap | **100.00 shares** | Cap is applied after two-decimal sizing |
| Shadow balance | **$1,000.00** | Isolated from the live account state |
| Real-money mode | **Disabled by default** | Requires both `KALSHI_LIVE_ENABLED=true` and an explicit live workflow input |

The selected configuration’s basis is recorded verbatim in its JSON: highest modeled $1,000 survival followed by lower P99 bankroll under the base **49¢** calibration. `entry_price=0.49` is now a historical/replay reference, not a live ceiling or live order price. The opening-maximum-minus-1¢ execution rule is a shadow-validation candidate—not a claim that fixed-price 49¢/50¢ calibration proves its profitability. The worker records actual quote and fill evidence so it can be calibrated before any live enablement.

### Opening quote capture and dynamic maker rule

From market open through the 15-second entry window, the WebSocket worker retains up to 500 **fresh complete top-of-book** observations for the selected signal side. Every retained observation records exchange/receive timestamps, quote ID and age, YES and NO bid/ask/depth, selected-side executable bid/ask/depth, and the implied one-cent-lower price. This is durable per-market audit data, not a claim that a resting order filled.

At the configured discovery deadline (currently three seconds after open), the selected-side maximum executable ask is frozen. The submitted maker price is exactly:

```text
maker_limit = maximum_selected_executable_ask_during_discovery - 0.01
```

There is no upper price cap: a maximum of 52¢ produces a 51¢ post-only limit; a maximum of 67¢ produces a 66¢ post-only limit. The limit is never posted if it would cross the then-current book (post-only must remain maker) or if the derived price is at or below the 40¢ stop. In either case the worker waits for the protected fallback or records a zero fill as appropriate. Later observations through second 15 remain immutable calibration telemetry and cannot rewrite an already-derived entry price. At expiry, the worker cancels any unfilled maker remainder and may use one fresh displayed-depth IOC only when its executable ask is strictly above 40¢.

Every market record and append-only audit event now reports the actual entry source, based only on filled quantities: `maker_limit`, `market_ioc`, `mixed`, `none`, or `other`. It separately records maker-limit and IOC filled quantities, their average fill prices, filled order IDs, and the combined weighted average. `market_ioc` specifically means the bounded, price-protected immediate-or-cancel fallback—not a raw/unbounded market order. A partial maker fill followed by an IOC remainder is therefore auditable as `mixed` rather than being mislabeled as either one alone.

The stop uses the **actual weighted average filled entry price**, not the maker limit. Its persistent policy is `max(0.40, actual_average_entry − 0.10)`: a 49¢ or 50¢ fill keeps a 40¢ stop, while a 52¢ fill raises the executable stop to 42¢. Partial fills and a later IOC remainder are weighted together before stop monitoring. Stop prices are rounded up to the exchange cent, making the realized pre-fee stop loss no larger than the intended 10¢ merely because the average fill has fractional cents. Each entry-price and effective-stop update is written to the audit ledger.

## Reconstructed historical directional results

The following snapshot was regenerated from Kalshi’s public settlement endpoints on **2026-08-08**. The cache is intentionally ignored by Git because it is downloaded source data; the exact retrieval commands are below.

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

The difference from the earlier headline is therefore additional public history, not a random redraw or a changed directional rule. For historical replay, the source signal is the most recently settled earlier market published by the target market’s `open + 45 seconds`; YES maps to predicted NO and NO maps to predicted YES. No two-loss/two-market skip exists in this strategy.

Live signal timing is intentionally faster: it freezes a provisional prior outcome from the final fresh executable 99¢ bid before the boundary, produces the inverse signal at the next market’s open, and later verifies it against official settlement. This is auditable in the shadow ledger. The historical API does not contain that final quote stream, so the 45-second settlement-causality replay and the live provisional-quote mechanism are distinct evidence paths; their agreement must be measured in shadow rather than assumed.

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

## Static expected value: transparent, limited, and reproducible

The following is a **one-share, fixed-size, no-fee, 40¢-stop calculation** using the current 22,406 fixed directional outcomes and the base 49¢ execution scenario. It does not include recovery sizing, permanent-base scaling, the 100-share cap, funding failures, slippage, or calibration uncertainty.

For entry price `e`, stop `s`, win rate `pW`, fill probability `f`, and joint 40¢-region probabilities `rW`/`rL`, the gross EV per eligible signal is:

```text
pW * ((f - rW) * (1 - e) + rW * (s - e))
+ (1 - pW) * ((f - rL) * (-e) + rL * (s - e))
```

| Mechanical price sensitivity | EV / eligible signal | EV / expected filled share | Gross / 1,000 eligible signals |
| --- | ---: | ---: | ---: |
| 49¢ entry, 40¢ stop | **+$0.02232** | **+$0.02625** | **+$22.32** |
| 50¢ entry, 40¢ stop | **+$0.01382** | **+$0.01625** | **+$13.82** |

The 50¢ row changes only payout math while holding the **49¢** base-fill/path scenario fixed. It is a sensitivity calculation, not a calibrated 50¢ maker-fill forecast. Neither fixed-price row is an expected-value claim for the production opening-maximum-minus-1¢ rule or the new entry-adjusted stop: the historical rung model only identifies 40¢/30¢/20¢/10¢ touches, not 41¢–49¢ stop touches. The worker therefore records actual average entries, effective stops, post-entry minimum executable bids, stop exits, and later official outcomes for stopped positions before that adjustment is assigned an EV. At either fixed price, one cent of fee per filled share would reduce the per-eligible-signal figure by approximately $0.00850 under the 85% participation assumption, before any slippage. A positive static EV is not a capital guarantee: nonlinear recovery sizing can still create drawdowns, cap hits, and funding failures.

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

### Earlier relative stop comparison

An earlier static reconstruction ranked 40¢ first, followed closely by 10¢, then staged 40/30/20/10, 20¢, and 30¢. Its per-share gross estimates were 2.68¢, 2.64¢, 2.53¢, 2.44¢, and 2.37¢ respectively. This ranking is descriptive only: the current implementation reruns all fixed stops on the current fixed settlement snapshot and reports stop results in `stop_optimization_results.csv`. The primary live stop remains 40¢ because the observed 40¢ region was materially more common among eventual directional losses.

## Full reproducible backtest

Use Python 3.13 and the pinned research requirements. The commands create an ignored cache and a self-contained output directory; no live secrets are needed.

```bash
python3.13 -m venv .venv
.venv/bin/pip install -r requirements_kalshi_hybrid_backtest.txt

# 1. Download/cache actual Kalshi settlement outcomes and reconstruct signals.
.venv/bin/python kalshi_settlement_loader.py --refresh \
  --cache data/raw/kalshi_kxbtc15m_settlements.json \
  --signals historical_signals.parquet

# 2. Validate the calibration layer alone (100,000 simulated calibration draws).
.venv/bin/python calibration.py \
  --output outputs/kalshi_hybrid_backtest/calibration_report.csv \
  --replications 100000 --seed 42

# 3. Full 363-configuration 49¢ screen, stop finalists, 100,000-rep final runs,
#    walk-forward, stress tests, plots, and the six prior-style comparisons.
.venv/bin/python optimizer.py \
  --output-dir outputs/kalshi_hybrid_backtest/base_49c \
  --entry-price .49 --execution-scenario base_case \
  --coarse-simulations 5000 --final-simulations 100000 \
  --walkforward-simulations 500 --finalists 15 \
  --reconciliation-simulations 50000 --seed 42

# 4. Optional 50¢ fixed-price sensitivity. This reuses the 49¢ path
#    calibration; it does not model the live opening-maximum-minus-1¢ rule.
.venv/bin/python optimizer.py \
  --output-dir outputs/kalshi_hybrid_backtest/sensitivity_50c \
  --entry-price .50 --execution-scenario base_case \
  --coarse-simulations 5000 --final-simulations 100000 \
  --walkforward-simulations 500 --finalists 15 --seed 42
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
| `reconciliation_comparison.csv` | Explicit prior-style 1.11× reference runs over fixed actual settlement prefixes |
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
- The worker retains fresh complete selected-side books throughout the 15-second opening window. At second 3 it freezes the maximum selected-side executable ask and submits exactly 1¢ below it post-only, with no upper-price ceiling. Later captured quotes cannot change that submitted price.
- The stop is `max(40¢, actual average entry − 10¢)`: an entry below 50¢ never lowers the 40¢ floor, while an entry above 50¢ raises its executable-bid trigger one-for-one. A stale book, a derived maker limit at/below 40¢, or a price that would cross the current book creates no unsafe maker exposure. The protected one-shot IOC fallback is considered only at 15 seconds and only strictly above 40¢; a stop flattens only confirmed filled exposure.
- State and append-only audit ledgers are separate for `data/kalshi_shadow_strategy_*` and `data/kalshi_live_strategy_*`. The shadow run starts at $1,000 and tracks realized P&L, peak equity, and max drawdown without mutating live strategy state.
- A five-hour worker checkpoints and queues its successor only in the middle 13 minutes of a market—from one minute after open through one minute before close. The watchdog is serialized and mode-preserving; it cannot convert a shadow worker into a live worker.

Real-money orders require both the repository/environment variable `KALSHI_LIVE_ENABLED=true` and a workflow run with `live_enabled=true` and `dry_run=false`. This repository is left in shadow mode. Credentials are referenced only by the names `KALSHI_PROD_API_KEY` and `KALSHI_PRIVATE_KEY`; they are never written to state, logs, artifacts, source, or README.

## Tests and operational commands

```bash
# Shared-core, replay, path, reconciliation, and live-execution safety suite.
PYTHONPATH=. .venv/bin/python -m unittest -v \
  tests.test_strategy_core tests.test_live_execution tests.test_reconciliation \
  tests.test_recovery_sizing tests.test_execution_path_model tests.test_historical_replay

# Dry-run worker (uses isolated $1,000 shadow state).
KALSHI_API_KEY_ID=... KALSHI_PEM_PATH=kalshi_private_key.pem \
  .venv/bin/python kalshi_live_trader.py --config selected_live_strategy.json \
  --state-file data/kalshi_shadow_strategy_state.json \
  --audit-ledger data/kalshi_shadow_strategy_audit.jsonl --dry-run --run-seconds 120

# Read-only reconciliation; it never creates an entry.
KALSHI_API_KEY_ID=... KALSHI_PEM_PATH=kalshi_private_key.pem \
  .venv/bin/python kalshi_live_trader.py --config selected_live_strategy.json --reconcile-only
```

The test suite covers fixed outcomes, no loss-skip behavior, Decimal fractional sizing, zero-fill invariants, recovery/base transitions, path nesting, P&L conventions, caps, funding checks, restart reconciliation, order idempotency, provisional-outcome handling, opening quote capture, the no-cap one-cent-below-opening-maximum maker rule, the asymmetric 40¢-floor/above-50¢ stop policy, the 15-second maker/IOC sequence, the 40¢ guard, and safe handoff timing.

## Remaining risks

The public settlement API cannot prove historical execution paths. The model does not yet have a representative sample for the dynamic opening-maximum-minus-1¢ maker rule, and the immediate provisional-quote signal must earn its reliability through live shadow verification. A maximum observed ask does not establish a fillable maker queue position, and the uncapped rule can select materially higher entry prices than the fixed-price studies. Fee schedules, liquidity, queue priority, stale/disconnected data, stop slippage, API behavior, market rules, and a changed directional regime may turn the modeled result negative. Treat the backtest as a reproducible risk study, not an assurance of profitability or capital safety.
