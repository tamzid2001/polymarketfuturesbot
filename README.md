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

[`selected_live_strategy.json`](selected_live_strategy.json) is the canonical base configuration. The active contract is `kxbtc15m-hybrid-live-v11` / schema `11`. The Python loader and GitHub Action both assert that exact version, the maker-entry mode, all 40–49¢ analytics levels, a valid hybrid-stop hierarchy, and the single `sticky_stop_40` lane before the worker can submit anything. Older IOC/fixed-stop configurations and checkpoints cannot be loaded under the v11 state paths. Only the reviewed sizing/profit/hard-stop fields described below can change through Actions; execution and timing internals remain pinned.

| Setting | Current value | Notes |
| --- | ---: | --- |
| Series | `KXBTC15M` | Discovered from Kalshi market metadata, not ticker arithmetic |
| Market discovery | **Bounded previous/current/upcoming window, 1-second poll** | Uses Kalshi `min_close_ts`/`max_close_ts` metadata to preload the real API successor before open without scanning far-future markets |
| Direction observation window | **Final 5 seconds** | Configurable to 15 seconds; only a ≥99¢ executable bid observed for exactly one side inside this ending-market window can supply the next direction |
| Direction rule | **Sticky until directional win** | Seed inverse to the first prior result; hold the same side after a wrong prediction; flip only after that side settles correctly |
| Starting permanent base | **1.00 share default** | Configurable for a brand-new state; two-decimal `Decimal`, `ROUND_HALF_UP` |
| Entry reference | **First fresh post-open selected-side executable ask** | Frozen once; a pre-open, stale, missing, or fractional-cent quote is rejected |
| Entry order | **Post-only GTC limit, exactly 1¢ below the frozen ask** | One deterministic order; no market/IOC fallback; only exchange fills or conservative shadow trade-through evidence create exposure |
| Entry lifetime | **Until filled or market close** | No strategy-time expiry; a resting remainder is cancelled only at market close or when confirmed cancellation is required to protect filled exposure |
| Shadow entry analytics | **40¢ through 49¢, every cent** | Touch, simulated fill, eventual winner capture, and missed winner are distinct facts |
| Active workflow lane | **`sticky_stop_40` only** | The 30/20/10 comparison Actions are retired and the watchdog never recreates them |
| Hybrid trigger | **Executable bid ≤45¢ default** | Always one cent above the configured hard-stop input |
| Hybrid maker exit | **46¢ default** | Always two cents above the configured hard-stop input |
| Hybrid hard stop | **Executable bid ≤44¢ default** | Confirms maker cancellation/fills, then IOC-exits only authoritative residual exposure |
| Recovery multiplier | **1.01×** | The selection favors $1,000-survivability, not maximum modeled P&L |
| Recovery exponent ceiling | **Disabled (`0`)** | The 1.01× sequence continues after every filled trade while cycle P&L is negative in both shadow and live modes |
| First base threshold | **$350.00** | Realized net P&L only |
| Threshold growth | **1.01×** | Geometric after each permanent-base step |
| Base increment | **+0.50 share** | Supports +0.25, +0.50, and +1.00 |
| Position cap | **100.00 shares** | Separate absolute exposure safety limit, applied after otherwise-unbounded two-decimal 1.01× sizing |
| Shadow balance | **$1,000.00** | Isolated from the live account state |
| Real-money mode | **Currently gated off** | It requires `KALSHI_SHADOW_ONLY=false`, `KALSHI_LIVE_ENABLED=true`, and an explicit workflow `live_enabled=true` / `dry_run=false` request |

The selected recovery settings still come from the earlier $1,000-survivability screen: 1.01× recovery, $350 first threshold, 1.01× threshold growth, +0.50 base increment, 1.00 starting share, and a 100-share cap. That selection does **not** establish expected value for v11 maker entries or the hybrid stop. `entry_price=0.49` and `stop_price=0.40` remain backtest/profile reference fields; the actual live entry is dynamic and the actual stop state machine is 45/46/44.

### Immutable maker entry and hybrid stop

Every second, the worker requests a bounded KXBTC15M close-time window and maintains WebSocket subscriptions for the predecessor, current market, and API-provided successor. During the final five seconds (15 is supported), exactly one side must show a fresh executable bid ≥99¢ to supply the next sticky-direction transition. Neither side, both sides, stale evidence, or an unavailable predecessor fails closed.

At open, the first fresh complete book freezes `initial_signal_price_cents`. `entry_limit_cents = initial_signal_price_cents - 1`; later quotes can never move it. The worker submits one deterministic GTC/post-only buy for the selected side. `maker_order_time_in_force=good_till_canceled`, `entry_order_lifetime=until_filled_or_market_close`, and the disabled timeout sentinel `entry_timeout_seconds=0` are required fail-closed production contracts. There is no elapsed-time cancellation: the order rests until fully filled, the market closes, or confirmed cancellation is required to protect a partially filled position during the hybrid stop. `opening_quote_capture_seconds` limits telemetry collection only and never changes order lifetime. Live mode uses exchange order/fill responses. Shadow mode requires post-submission public trades at or below the buy limit and labels the evidence `conservative_public_trade_through`; a quote touch alone is never a simulated fill and no queue priority is claimed. Partial fills open only their actual quantity. Cancellation must be confirmed before the record can become zero-fill or before any stop exit can proceed.

For the default hard stop of 44¢, bid ≤45¢ starts a post-only/reduce-only maker sale at 46¢. Shadow maker exits require a later fresh executable bid at or above 46¢ and are bounded by displayed depth. If bid reaches ≤44¢ before that maker exit is complete, the worker cancels it, captures final fills, reconciles actual residual exposure, and sends a reduce-only IOC only for that residual. Changing the single `max_stop_loss_cents` workflow input moves this hierarchy together: hard stop `H`, trigger `H+1`, maker exit `H+2`. A 1.00-share entry with a 0.40 maker-exit fill can therefore hard-exit at most 0.60. Unknown cancellation or position state blocks new exposure and never grants permission to oversell.

Every signal also maintains independent analytics for 40, 41, …, 49¢. It records executable-ask touches separately from conservative simulated fills. At official settlement, it records winner capture and missed-winner rates, the minimum selected-side ask, eventual-winner maximum drawdown, and whether a stopped position would later have won. A stopped record remains observed until official settlement; verification never changes already-realized recovery P&L.

The `OPENING ENTRY SNAPSHOT` log prints the exact selected side, first fresh post-open executable ask, derived ask-minus-1¢ limit, exchange timestamp, exchange-quote lag from open, worker-observation lag, and the monitored 40–49¢ range. Those same facts are atomically checkpointed in the per-market state and appended to the fsynced audit ledger. `SETTLEMENT PRICE PATH` later prints and persists the initial ask, actual average fill (if any), minimum observed ask, every 40–49¢ hit/miss, and the maximum drawdown in cents for an eventual directional winner. The five-minute aggregate also distinguishes the lowest actual fill among eventual directional winners from the lowest entry that produced positive realized net P&L.

### Sticky signal transition

The v11 signal has no loss-skip rule and is independent of execution. For each new market, the worker freezes the immediately preceding market’s realtime provisional outcome, later checks it against official settlement, and records the transition in both state and audit ledger:

```text
fresh state + previous YES  -> enter NO
entered NO + current settles YES -> enter NO again  (directional loss: hold)
entered NO + current settles NO  -> enter YES next  (directional win: flip)
entered YES + current settles NO -> enter YES again (directional loss: hold)
entered YES + current settles YES -> enter NO next  (directional win: flip)
```

Entry fills, zero fills, hybrid exits, recovery P&L, and permanent-base scaling never change that directional side. Only the completed market result relative to the prior selected side does. A provisional/official mismatch is preserved as an audit discrepancy; it never rewrites an already-submitted entry.

### Single production shadow lane

Only `sticky_stop_40` is accepted by configuration, workflow input, watchdog, optimizer export, and controlled restart:

| Mode | Durable state | Append-only audit ledger |
| --- | --- | --- |
| Shadow | `data/kalshi_shadow_maker_hybrid_v11_sticky_stop_40_state.json` | `data/kalshi_shadow_maker_hybrid_v11_sticky_stop_40_audit.jsonl` |
| Live | `data/kalshi_live_maker_hybrid_v11_state.json` | `data/kalshi_live_maker_hybrid_v11_audit.jsonl` |

The v8/v9/v10 files remain archived evidence. v11 starts in a separate namespace at a $1,000 shadow balance and 1.00 permanent base, so no older IOC, comparison-stop, or synthetic-cancellation state can be reinterpreted as current execution. The single workflow concurrency group serializes shadow/live workers, and the watchdog dispatches only v11 `sticky_stop_40`.

Forensic check of the archived v10 40¢ IOC shadow state (not a v11 maker result): it contains **214 actual filled markets**, all with a later outcome, split into **105 eventual directional winners / 109 losers**. The lowest actual selected-side fill that later settled correctly was **41¢ NO**, 2.17 shares, in `KXBTC15M-26AUG181215-15`; that position hit its stop and realized **-$0.0868**, so it is not labeled a profitable trade. The lowest actual entry with positive realized net P&L was **42¢ YES**, 2.42 shares, in `KXBTC15M-26AUG181500-00`; it was held to a YES settlement and realized **+$1.4036** in the archived shadow ledger. This distinction prevents “eventual directional winner” from being confused with “profitable after the stop policy.”

## Reconstructed historical directional results — prior inverse baseline

The following snapshot was regenerated from Kalshi’s public settlement endpoints on **2026-08-08** using the prior `inverse_latest_settlement` rule. The cache is intentionally ignored by Git because it is downloaded source data; the exact retrieval commands are below. It is a reproducibility baseline, **not** the v11 sticky-direction/maker/hybrid-stop expected value.

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

Live signal timing is intentionally faster: it freezes a provisional prior outcome from the final fresh executable 99¢ bid before the boundary, produces the v11 sticky transition at the next market’s open, and later verifies it against official settlement. The historical optimizer’s `--signal-mode sticky_until_directional_win` rebuild uses the actual previous settlement as an explicitly labelled **provisional-outcome proxy**; it does not claim that the delayed public endpoint was available at the boundary. The historical API does not contain that final quote stream, so the proxy and the live provisional-quote mechanism are distinct evidence paths; their agreement must be measured in shadow rather than assumed.

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

The following is a **one-share, fixed-size, no-fee, 40¢-stop calculation** using the current 22,406 *prior inverse* fixed directional outcomes and the base 49¢ execution scenario. It does not include recovery sizing, permanent-base scaling, the 100-share cap, funding failures, slippage, the new dynamic entry, or calibration uncertainty. It must not be read as v11 sticky-direction/maker/hybrid-stop EV.

For entry price `e`, stop `s`, win rate `pW`, fill probability `f`, and joint 40¢-region probabilities `rW`/`rL`, the gross EV per eligible signal is:

```text
pW * ((f - rW) * (1 - e) + rW * (s - e))
+ (1 - pW) * ((f - rL) * (-e) + rL * (s - e))
```

| Mechanical price sensitivity | EV / eligible signal | EV / expected filled share | Gross / 1,000 eligible signals |
| --- | ---: | ---: | ---: |
| 49¢ entry, 40¢ stop | **+$0.02232** | **+$0.02625** | **+$22.32** |
| 50¢ entry, 40¢ stop | **+$0.01382** | **+$0.01625** | **+$13.82** |

The 50¢ row changes only payout math while holding the **49¢** base-fill/path scenario fixed. It is a sensitivity calculation, not a calibrated 50¢ maker-fill forecast. Neither fixed-price row is an expected-value claim for v11: the historical API cannot tell whether a dynamic ask-minus-one maker order filled or whether the 45/46/44 hybrid exit completed. The v11 ledger therefore measures actual/simulated entry price, touch versus fill evidence, fees, partial exits, stop mechanism, and later official outcome before this new execution rule is assigned an EV. At either fixed price, one cent of fee per filled share would reduce the per-eligible-signal figure by approximately $0.00850 under the 85% participation assumption, before any slippage. A positive static EV is not a capital guarantee: nonlinear recovery sizing can still create drawdowns, cap hits, and funding failures.

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

An earlier static inverse reconstruction ranked 40¢ first, followed closely by 10¢, then staged 40/30/20/10, 20¢, and 30¢. Its per-share gross estimates were 2.68¢, 2.64¢, 2.53¢, 2.44¢, and 2.37¢ respectively. This ranking is descriptive historical evidence. The production workflow now retains only the `sticky_stop_40` lane, whose v11 execution is the separate 45/46/44 hybrid state machine. Archived optimizer output may still compare historical fixed stops; it cannot activate the retired Actions.

## Full reproducible backtest

Use Python 3.13 and the pinned research requirements. The commands create an ignored cache and a self-contained output directory; no live secrets are needed.

```bash
python3.13 -m venv .venv
.venv/bin/pip install -r requirements_kalshi_hybrid_backtest.txt

# 1. Download/cache actual Kalshi settlement outcomes and reconstruct the
#    prior inverse baseline. Use --signal-mode sticky_until_directional_win
#    below for the active v11 fixed-settlement proxy replay.
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
| `optimization_summary.md` | Human-readable rankings and the exported live configuration provenance |

The optimizer uses common random numbers for competing configurations, keeps every actual directional settlement fixed, and applies the same `strategy_core.py` state transitions as the live worker. It reports P&L, drawdown, bankroll, cap-binding, recovery-cycle, fill, zero-fill, and stop distributions. Calibration uncertainty can be added with `--calibration-uncertainty-draws N`.

## Production behavior and persistence

- The live engine and historical replay share the recovery/base-sizing transitions. A filled trade updates realized net P&L; a zero fill is exactly $0 and changes neither the recovery exponent nor permanent base.
- Recovery exponent increases after **every filled closed trade** while cumulative recovery-cycle P&L remains negative. It resets only when that cumulative amount reaches at least $0.
- `max_recovery_exponent=0` is the explicit disabled sentinel. The shared shadow/live engine does not stop the 1.01× sequence at exponent 12; the independent 100-share position limit, funding check, recovery-loss breaker, and daily-loss breaker remain active.
- Permanent-base steps use realized net P&L only. No unrealized value, cancelled order, or zero fill can scale the base.
- Startup reconciles Kalshi balance, open managed orders, positions, fills, and settlements before any entry. Unknown or ambiguous ownership fails closed; Kalshi is authoritative.
- Client order IDs are deterministic, partial fills use actual quantities, exits are reduce-only where supported, and the same market cannot be counted twice after restart.
- The worker discovers a bounded previous/current/upcoming market window every second using `min_close_ts`/`max_close_ts`, subscribes the API-provided successor before open, and keeps the ending market subscribed for final 99¢ executable-bid inference. It freezes the first fresh **post-open** selected-side ask and submits one deterministic post-only limit one cent below it. There is no v11 IOC entry fallback and later quotes cannot move the limit.
- The hybrid stop defaults to 45¢ trigger / 46¢ maker sale / 44¢ hard-stop threshold. The one hard-stop workflow input moves all three together; entry price never shifts them. Actual entry and exit fills, quantities, fees, and residual exposure drive accounting.
- Shadow and live state are isolated at `data/kalshi_shadow_maker_hybrid_v11_sticky_stop_40_*` and `data/kalshi_live_maker_hybrid_v11_*`. The sole v11 shadow lane starts at $1,000 and the configured initial base (1.00 share by default), and tracks realized P&L, peak equity, maximum drawdown, 40–49¢ analytics, false stops, and timing.
- Every audit JSONL record is appended, flushed, and `fsync`ed before the worker resumes order/position management. Its companion strategy state is atomically written and `fsync`ed immediately after every audit event; therefore a state transition, fill observation, stop event, funding failure, settlement, reconciliation result, and handoff is checkpointed while the worker is running—not merely at its end. Remote checkpoints are coalesced at `durable_checkpoint_interval_seconds` and force-update one parentless `runtime-state` snapshot with an exact lease. The runtime ref cannot accumulate commit history, while `main` remains code-only and stable. A pending material event is retried by ordinary worker checkpoints once the interval expires.
- Each market ledger record includes the initial signal quote, derived limit, exchange/client order IDs, partial fills, maker/taker status where exposed, opening quote evidence, and observed timing: first-fresh-book lag, market-open-to-submission, market-open-to-first-fill, submission-to-first-fill, entry completion, first-fill-to-trigger, trigger-to-maker submission, and trigger-to-observed-flat position. These are explicitly **worker-observed** timestamps. Heartbeats report balance, cumulative shadow P&L, maximum drawdown, entry composition, latency, the enforced GTC contract, captured opening quotes, and actual stop-safety no-entries. Five-minute tables print every 40–49¢ level, winner capture/misses, drawdown buckets, and hybrid-stop outcomes. They also print, separately for every hypothetical stop from 40¢ through 49¢, the number/rate of frozen initial prices at or below that stop, plus exact-price and actual configured safety-rejection counts. Those are no-entry diagnostics, not the retired directional loss-skip rule and not ordinary GTC zero-fills.
- A five-hour worker checkpoints and queues its successor only in the middle 13 minutes of a market—from one minute after open through one minute before close. One concurrency group serializes the strategy, and the watchdog is mode-preserving and 40-lane-only; it cannot recreate 30/20/10 or convert shadow into live.
- Workflow-dispatch parameter overrides are validated and written back to `selected_live_strategy.json` before execution, then included in the material-event and end-of-run checkpoints. A change is accepted only while exchange/order state is flat. If recovery P&L is negative, its saved multiplier/base parameters remain authoritative until that recovery cycle resets; the new settings then govern the fresh cycle. Any non-approved config-hash difference still fails closed. The watchdog is the sole five-minute scheduler; the long worker has no independent cron, preventing redundant five-hour jobs from accumulating behind the singleton concurrency group.

### GitHub Actions inputs

The production worker now presents only seven manual inputs. Blank strategy values preserve the version already stored in `runtime-state`, so watchdog and five-hour handoffs cannot overwrite a deliberate setting with an old default.

| Input | Meaning |
| --- | --- |
| `live_enabled` | Requests live execution, but only when both repository safety gates also permit it; default `false` |
| `reconcile_only` | Reconcile authoritative Kalshi state without opening exposure |
| `initial_shares` | Two-decimal starting base for a brand-new state; current default `1.00` |
| `scaling_multiplier` | Sets both recovery sizing and geometric profit-threshold growth |
| `profit_threshold` | First realized-net-profit threshold for a permanent base increase |
| `shares_added_after_profit_threshold` | Two-decimal permanent base increment after each threshold crossing |
| `max_stop_loss_cents` | Hard-stop price `H` from 40 through 49; trigger is `H+1` and maker exit is `H+2` |

The controlled-restart workflow exposes only `source_run_id` and `target_live`. Run duration, sticky-direction lane, GTC order lifetime, quote timing, 40–49¢ analytics, checkpoint cadence, maximum position, and all other safety limits remain canonical configuration rather than routine UI knobs.

`KALSHI_SHADOW_ONLY=true` is the current repository setting and hard-forces `MODE=DRY_RUN` in both workflow and Python code. To switch deliberately, set `KALSHI_SHADOW_ONLY=false` and `KALSHI_LIVE_ENABLED=true`, then run the controlled-restart workflow with `target_live=true` while the named source lane is flat. The handoff refuses boundary timing or persisted exposure, dispatches the current `main`, preserves state, and the replacement reconciles before creating risk. Reversing either repository gate disables live placement again. Credentials are referenced only by the names `KALSHI_PROD_API_KEY` and `KALSHI_PRIVATE_KEY`; they are never written to state, logs, artifacts, source, or README.

## Tests and operational commands

```bash
# Shared-core, replay, path, reconciliation, v11 maker/hybrid, and live safety suite.
PYTHONPATH=. .venv/bin/python -m unittest -v \
  tests.test_strategy_core tests.test_live_execution tests.test_maker_hybrid_v11 tests.test_reconciliation \
  tests.test_recovery_sizing tests.test_execution_path_model tests.test_historical_replay

# Canonical v11 shadow run (isolated $1,000 state, never real orders).
KALSHI_API_KEY_ID=... KALSHI_PEM_PATH=kalshi_private_key.pem \
  .venv/bin/python kalshi_live_trader.py --config selected_live_strategy.json \
  --state-file data/kalshi_shadow_maker_hybrid_v11_sticky_stop_40_state.json \
  --audit-ledger data/kalshi_shadow_maker_hybrid_v11_sticky_stop_40_audit.jsonl \
  --shadow-profile sticky_stop_40 --stop-price 0.40 --trading-mode shadow --dry-run --run-seconds 120

# Read-only reconciliation; it never creates an entry.
KALSHI_API_KEY_ID=... KALSHI_PEM_PATH=kalshi_private_key.pem \
  .venv/bin/python kalshi_live_trader.py --config selected_live_strategy.json \
  --state-file data/kalshi_live_maker_hybrid_v11_state.json --trading-mode live --reconcile-only
```

The test suite covers fixed outcomes, no loss-skip behavior, sticky hold/flip transitions, Decimal sizing, strict zero-fill invariants, recovery/base transitions, caps, funding checks, startup reconciliation, deterministic idempotency, provisional outcome timing, immutable ask-minus-one entry, no-fill/full/partial/cancelled maker orders, all 40–49¢ levels, winner drawdowns, touch-versus-fill separation, full maker exits, hard-stop fallback, partial-maker residual exits, duplicate ticks, restart with a pending maker exit, post-stop settlement analytics, shadow/live state parity, workflow anti-regression assertions, and the hard shadow-only live gate.

## Remaining risks

The public settlement API cannot prove historical execution paths. The dynamic maker fill model does not know queue priority; shadow public trade-through evidence is conservative but is still not an exchange fill. A displayed bid that could execute a maker exit may disappear before a live fill. Fee schedules, liquidity, stale/disconnected data, stop slippage, API behavior, market rules, and a changed directional regime may turn the modeled result negative. The fixed-price EV tables are not v11 forecasts. Treat the backtest and new shadow analytics as reproducible risk studies, not assurances of profitability or capital safety.
