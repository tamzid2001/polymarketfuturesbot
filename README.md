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

[`selected_live_strategy.json`](selected_live_strategy.json) is the canonical base configuration. The active contract is `kxbtc15m-hybrid-live-v11` / schema `11`. The Python loader and GitHub Action both assert that exact version, the maker-entry mode, all 40–49¢ analytics levels, and a valid hybrid-stop hierarchy before the worker can submit anything. `sticky_stop_40` remains the only canonical lane that can participate in the separately gated live workflow. The 10¢/20¢/25¢/30¢/35¢ profiles are accepted only with `trading_mode=shadow` and run through a workflow that contains no live input or `--live-enabled` path. Older IOC/fixed-stop configurations and checkpoints cannot be loaded under the v11 state paths.

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
| Delayed ≥53¢ analytics | **Frozen trigger ask minus 1¢ through market close** | For a below-53¢ initial ask, the first later fresh executable ask ≥53¢ freezes an analytics-only resting limit 1¢ lower; only subsequent public-trade volume at/below that limit can simulate a partial/full fill |
| Canonical workflow lane | **`sticky_stop_40`** | Sole live-capable lane; still gated off by default |
| Shadow comparisons | **`sticky_stop_10`, `sticky_stop_20`, `sticky_stop_25`, `sticky_stop_30`, `sticky_stop_35`** | Isolated dry-run-only workers; no live control exists in their workflow |
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

At open, the pre-subscribed WebSocket freezes the earliest fresh **price-only** selected-side executable ask; it does not wait for displayed sizes. `entry_limit_cents = initial_signal_price_cents - 1`; later quotes can never move it. YES uses the first fresh YES ask and NO uses `1 - first fresh YES bid`, so an unrelated stale component cannot delay the selected-side reference. The ledger stores this immutable price reference, component/exchange timestamp, subscription start, observed lag, and whether the subscription covered the market continuously from before open. A worker that subscribed after open marks the reference `PARTIAL` and cannot use it as a complete opening entry. Displayed-depth arrival is recorded separately with its own lag and never retroactively changes the opening cohort.

The worker submits one deterministic GTC/post-only buy for the selected side. `maker_order_time_in_force=good_till_canceled`, `entry_order_lifetime=until_filled_or_market_close`, and the disabled timeout sentinel `entry_timeout_seconds=0` are required fail-closed production contracts. There is no elapsed-time cancellation: the order rests until fully filled, the market closes, or confirmed cancellation is required to protect a partially filled position during the hybrid stop. `opening_quote_capture_seconds` limits full-depth telemetry collection only and never changes order lifetime. Live mode uses exchange order/fill responses. Shadow mode requires post-submission public trades at or below the buy limit and labels the evidence `conservative_public_trade_through`; a price-only quote, a complete-book touch, or order submission alone is never a simulated fill and no queue priority is claimed. Partial fills open only their actual quantity. Cancellation must be confirmed before the record can become zero-fill or before any stop exit can proceed.

For the default hard stop of 44¢, bid ≤45¢ starts a post-only/reduce-only maker sale at 46¢. Shadow maker exits require a later fresh executable bid at or above 46¢ and are bounded by displayed depth. If bid reaches ≤44¢ before that maker exit is complete, the worker cancels it, captures final fills, reconciles actual residual exposure, and sends a reduce-only IOC only for that residual. Changing the single `max_stop_loss_cents` workflow input moves this hierarchy together: hard stop `H`, trigger `H+1`, maker exit `H+2`. A 1.00-share entry with a 0.40 maker-exit fill can therefore hard-exit at most 0.60. Unknown cancellation or position state blocks new exposure and never grants permission to oversell.

Every signal also maintains independent analytics for 40, 41, …, 49¢. It records executable-ask touches separately from conservative simulated fills. At official settlement, it records winner capture and missed-winner rates, the minimum selected-side ask, eventual-winner maximum drawdown, and whether a stopped position would later have won. A stopped record remains observed until official settlement; verification never changes already-realized recovery P&L.

The bounded 60-second opening-book sample remains available for detailed timing calibration, but it is no longer the horizon for the ≥53¢ research question. When the immutable initial selected-side ask is below 53¢, a compact analytics-only tracker watches fresh selected-side executable asks until market close. The first ask at or above 53¢ is a **trigger, not a fill**: delayed-ladder contract v3 freezes `entry_limit_cents = trigger_ask_cents - 1` and keeps that counterfactual limit resting through market close without repricing or timing out. A later quote at/below the limit records a touch only. A simulated partial/full entry requires subsequent public-trade volume at or below the frozen limit; settlement without that evidence is an `ENTRY_NOT_FILLED` zero-fill with $0 P&L. The tracker persists trigger price/timing separately from actual simulated entry price/timing, requested and filled quantity, partial/full status, and no-stop P&L based only on filled quantity.

For filled delayed entries, contract v3 also tracks an isolated **51/52/50 hybrid stop**: executable selected-side bid ≤51¢ cancels the unfilled entry remainder and starts a 52¢ maker exit for only the quantity actually filled; subsequent public-trade volume at/above 52¢ is required for maker-exit fills. If bid reaches ≤50¢ first, the remaining exposure hard-exits at the observed executable bid, never above 50¢. Results distinguish maker-full, maker-partial-then-hard, hard-only, settlement, and false-stop outcomes. This stop is delayed-cohort research only; it does not silently replace the canonical live lane's separately configured hybrid stop.

The same trigger also activates the independent analytics-only 50/40/30/20/10¢ ladder with 1/2/4/8/16 shares. One durable allocator sorts the direct limit and ladder rungs by descending price and consumes each public-trade unit once, so the same displayed trade volume cannot fill both the direct order and a rung. It makes no queue-priority claim. Separate ≥53¢ through ≥59¢ summaries report directional trigger W/L independently from direct-limit filled W/L, partial/full/zero-fill counts, trigger-ask buckets, filled-limit buckets, ladder results, and actual filled-quantity P&L. Existing v1/v2 checkpoint records remain visible as legacy but are not retroactively converted or mixed into exact v3 direct-limit results. This lane never submits an auxiliary exchange order, never changes the primary GTC entry, and never touches recovery or permanent-base state.

The canonical shadow worker also maintains seven independent first-minute trigger cohorts: selected-side ask ≥53¢, ≥54¢, …, ≥59¢. A threshold may activate only from a price-only quote in the first 60 seconds; once activated, its analytics-only 50/40/30/20/10¢ ladder remains observed through market close with requested quantities 1/2/4/8/16 shares. A selected-side executable ask at or below a rung is a `touch`, not a fill. Conservative simulated fills require subsequent public-trade volume at or below the rung, allocated once across the five orders in descending price priority; the model never reuses the same trade volume for every rung and never claims queue priority. Each lane advances from a durable public-trade cursor, so a WebSocket reconnect or worker handoff cannot erase an earlier simulated fill or reuse its volume. Each threshold cohort records coverage completeness, trigger time/price, every rung touch, partial/full simulated fills, settlement W/L, and gross no-stop P&L before fees. It also carries the exact exchange-provided underlying BTC target/strike, comparison type, display label, and source field for that market (for example, `BTC ≥ $80,019.40`). The worker enriches bounded discovery results from Kalshi's authoritative per-market endpoint when the list response omits strike metadata. Structured `floor_strike`/`cap_strike`/`functional_strike` data is authoritative; a Kalshi subtitle is only a fallback, and the target is never guessed from a ticker. Missing targets are stored explicitly as `UNAVAILABLE`. This lane is shadow-only, submits no Kalshi orders, does not affect recovery/base state, and continues collecting while an entry circuit breaker is latched. Live risk circuit breakers remain enforced.

`NEW MARKET SIGNAL`, `OPENING CROSS LADDER`, every heartbeat, and `SETTLEMENT PRICE PATH` print the market's underlying BTC target and comparison alongside the contract-price analytics. `FIRST OPEN PRICE` prints the exact selected side, earliest post-open price-only ask, exchange lag, worker lag, subscription coverage status, and confirms that depth was not inferred. `FIRST DISPLAYED DEPTH` later prints the independently observed size-bearing book and its delay after the price reference. `OPENING ENTRY SNAPSHOT` prints the derived ask-minus-1¢ limit, intended quantity, exact opening entry cost (`quantity × limit` using `Decimal`), exchange timestamp, and monitored 40–49¢ range. Those facts—including the BTC target—are atomically checkpointed in the per-market state and appended to the fsynced audit ledger. Requested entry cost remains distinct from actual filled notional, actual entry fees, and actual cash cost; those execution values update only from observed fills. `SETTLEMENT PRICE PATH` later prints and persists the initial ask, actual average fill (if any), minimum observed ask, every 40–49¢ hit/miss, and the maximum drawdown in cents for an eventual directional winner. The five-minute aggregate also distinguishes the lowest actual fill among eventual directional winners from the lowest entry that produced positive realized net P&L.

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

### Canonical lane and isolated stop comparisons

`sticky_stop_40` remains the only profile accepted by the optimizer export, canonical worker, canonical watchdog, and controlled restart. The comparison workflow runs 10¢/20¢/25¢/30¢/35¢ profiles with the same signal, GTC ask-minus-1¢ entry, sizing engine, analytics, and audit code, but with a shadow-only hybrid hierarchy `hard=H`, `trigger=H+1¢`, `maker exit=H+2¢`:

| Lane | Durable state | Append-only audit ledger | Runtime ref |
| --- | --- | --- | --- |
| Canonical 40¢ shadow | `data/kalshi_shadow_maker_hybrid_v11_sticky_stop_40_state.json` | `data/kalshi_shadow_maker_hybrid_v11_sticky_stop_40_audit.jsonl` | `runtime-state-kxbtc15m` |
| Live | `data/kalshi_live_maker_hybrid_v11_state.json` | `data/kalshi_live_maker_hybrid_v11_audit.jsonl` | `runtime-state-kxbtc15m` |
| 10¢ shadow comparison | `data/kalshi_shadow_maker_hybrid_v11_sticky_stop_10_state.json` | `data/kalshi_shadow_maker_hybrid_v11_sticky_stop_10_audit.jsonl` | `runtime-state-stop-10` |
| 20¢ shadow comparison | `data/kalshi_shadow_maker_hybrid_v11_sticky_stop_20_state.json` | `data/kalshi_shadow_maker_hybrid_v11_sticky_stop_20_audit.jsonl` | `runtime-state-stop-20` |
| 25¢ shadow comparison | `data/kalshi_shadow_maker_hybrid_v11_sticky_stop_25_state.json` | `data/kalshi_shadow_maker_hybrid_v11_sticky_stop_25_audit.jsonl` | `runtime-state-stop-25` |
| 30¢ shadow comparison | `data/kalshi_shadow_maker_hybrid_v11_sticky_stop_30_state.json` | `data/kalshi_shadow_maker_hybrid_v11_sticky_stop_30_audit.jsonl` | `runtime-state-stop-30` |
| 35¢ shadow comparison | `data/kalshi_shadow_maker_hybrid_v11_sticky_stop_35_state.json` | `data/kalshi_shadow_maker_hybrid_v11_sticky_stop_35_audit.jsonl` | `runtime-state-stop-35` |

The v8/v9/v10 files remain archived evidence. Every v11 lane starts in a separate namespace at a $1,000 shadow balance and 1.00 permanent base, so no older IOC, comparison-stop, or synthetic-cancellation state can be reinterpreted as current execution. Each comparison worker has a dedicated concurrency group and bounded parentless runtime branch; concurrent checkpoints cannot overwrite the canonical lane or another stop lane. [`kalshi_btc15m_shadow_stop_watchdog.yml`](.github/workflows/kalshi_btc15m_shadow_stop_watchdog.yml) can recreate only these shadow-only comparison workers.

Forensic check of the archived v10 40¢ IOC shadow state (not live exchange execution and not a v11 maker result): it contains **214 shadow-executed IOC fills**, all with a later outcome, split into **105 eventual directional winners / 109 losers**. The lowest recorded shadow selected-side fill that later settled correctly was **41¢ NO**, 2.17 shares, in `KXBTC15M-26AUG181215-15`; that simulated position hit its stop and realized **-$0.0868**, so it is not labeled a profitable trade. The lowest recorded shadow entry with positive realized net P&L was **42¢ YES**, 2.42 shares, in `KXBTC15M-26AUG181500-00`; it was held to a YES settlement and realized **+$1.4036** in the archived shadow ledger. This distinction prevents “eventual directional winner” from being confused with “profitable after the stop policy,” or shadow execution from being confused with exchange fills.

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
- Every audit JSONL record is appended, flushed, and `fsync`ed before the worker resumes order/position management. Its companion strategy state is atomically written and `fsync`ed immediately after every audit event; therefore a state transition, fill observation, stop event, funding failure, settlement, reconciliation result, and handoff is checkpointed locally while the worker is running—not merely at its end. Remote checkpoints are coalesced every 30 seconds and force-update one parentless `runtime-state-kxbtc15m` snapshot with an exact lease. Snapshot schema v2 deterministically gzip-compresses each durable file in independent 8 MiB source chunks, verifies every compressed and uncompressed digest/size on restore, and reuses unchanged append-only ledger chunks. This prevents GitHub's 100 MiB single-blob limit from breaking a handoff as the ledger grows. The branch starts from an empty Git tree and contains only the allow-listed KXBTC15M payload chunks plus `.kxbtc15m-runtime-state.json`; it cannot accumulate commit history or inherit another bot's tracked files, while `main` remains code-only and stable. Schema-v1 runtime snapshots remain restorable during migration. Finalized markets also discard only active replay-deduplication ID buffers; all quote, fill, target, settlement, drawdown, ladder, stop, fee, and P&L evidence remains durable. A pending material event is retried by ordinary worker checkpoints once the interval expires.
- Each market ledger record includes the immutable first price-only opening reference, its completeness status, separately timed first displayed-depth book, derived limit, exchange/client order IDs, partial fills, and maker/taker status where exposed. Timing includes exchange-price lag, worker-observation lag, depth-after-price lag, market-open-to-submission, market-open-to-first-fill, submission-to-first-fill, entry completion, first-fill-to-trigger, trigger-to-maker submission, and trigger-to-observed-flat position. Heartbeats report `first_price_quote_lag`, `first_depth_quote_lag`, and `opening_price_coverage` separately; delayed or missing price history is never relabeled as a complete opening quote. Five-minute tables print every 40–49¢ level, winner capture/misses, drawdown buckets, and hybrid-stop outcomes. They also print, separately for every hypothetical stop from 40¢ through 49¢, the number/rate of frozen initial prices at or below that stop, plus exact-price and actual configured safety-rejection counts. Those are no-entry diagnostics, not the retired directional loss-skip rule and not ordinary GTC zero-fills.
- A five-hour worker checkpoints and queues its successor only in the middle 13 minutes of a market—from one minute after open through one minute before close. One concurrency group serializes the strategy, and the watchdog is mode-preserving and 40-lane-only; it cannot recreate 30/20/10 or convert shadow into live.
- Workflow-dispatch parameter overrides are validated and written back to `selected_live_strategy.json` before execution, then included in the material-event and end-of-run checkpoints. A change is accepted only while exchange/order state is flat. If recovery P&L is negative, its saved multiplier/base parameters remain authoritative until that recovery cycle resets; the new settings then govern the fresh cycle. Any non-approved config-hash difference still fails closed. The watchdog is the sole five-minute scheduler; the long worker has no independent cron, preventing redundant five-hour jobs from accumulating behind the singleton concurrency group.

### GitHub Actions inputs

The production worker now presents only seven manual inputs. Blank strategy values preserve the version already stored in `runtime-state-kxbtc15m`, so watchdog and five-hour handoffs cannot overwrite a deliberate setting with an old default.

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
