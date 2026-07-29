# Trading Automation

This repository contains a live Kalshi BTC 15-minute settlement-contrarian trader and a separate Polymarket portfolio system. Retired ML and paper-study material is preserved under [`archive/`](archive/README.md) and is not deployed.

## Kalshi BTC 15-minute trader

[`kalshi_btc15m_average_down.py`](kalshi_btc15m_average_down.py) is the only active Kalshi execution path.

- At a market open, it waits through the **penultimate minute** (up to 14 minutes) for the **immediately preceding** KXBTC15M market to finalize. It locks the opposite YES/NO side and submits the GTC ladder immediately when that result is available, with no fallback to an older result. A source that misses this causal window is recorded once as a terminal no-order skip.
- It posts a single-side GTC ladder at 40¢ / 30¢ / 20¢ / 10¢, expiring at the market close. The default base is **3**, giving **3 / 6 / 9 / 12** contracts (30 total, $6 principal before fees).
- Filled contracts hold to settlement. The only early exit is a reduce-only stop when the selected-side fresh full-depth bid is **≤5¢**. There is no ML signal, profit gate, or trailing stop in the live path.
- After two consecutive realized losses on filled live trades, it still computes and records the normal next two signals, but sends no balance check or exchange orders for those two markets. It then resets and resumes. A realized win resets the loss count immediately.
- Signal accuracy is reported separately from execution: every frozen settlement-contrarian side is scored against the target market's final YES/NO outcome, including zero-fill ladders and intentional loss-skip markets. Financial P&L, the realized-trade W/L, and the two-loss entry guard remain based only on contracts that actually filled.

### Dynamic base-share scaling

Dynamic scaling is enabled by default. Configure it from the **Kalshi BTC 15m Settlement Contrarian** Action with:

- `enable_dynamic_scaling`: `true` or `false` (default `true`)
- `base_share_increment`: base shares added after a threshold, in 0.01-share increments (default `1`)
- `scaling_profit_multiplier`: actual account profit required per current base share (default `16.5`)

When enabled, the runner uses the configured `starting_balance` (default **$100**) as the fixed first baseline and the authenticated Kalshi account balance as the only equity source. For example, an actual balance of `$134.8222` reports `profit_since_increase=$+34.8222` and `$14.6778` remaining to the first `$49.5000` threshold. It never derives scaling profit from the local order ledger or from the shadow-equity curve. At:

```text
profit_since_baseline >= (starting_base + each promoted base through current_base) × scaling_profit_multiplier
```

It increases the base by `base_share_increment` once that cumulative threshold is reached. With a 3-share start, 1-share increments, and a $16.50 multiplier, the thresholds are `$100 + (3 × $16.50) = $149.50`, then `$100 + ((3 + 4) × $16.50) = $215.50`, then `$100 + ((3 + 4 + 5) × $16.50) = $298.00`. The configured `$100` starting balance remains fixed for the whole run: deposits, withdrawals, and other authenticated cash adjustments are retained for audit but never shift the scaling baseline or next threshold. The new **1/2/3/4** ladder is used only for later markets. Bases and rungs retain 0.01-share precision (for example, base `3.25` creates `3.25 / 6.50 / 9.75 / 13.00`). Existing GTC ladders retain their original size. Runner-owned contract and principal caps grow as needed; explicitly supplied caps are never overridden and will safely block an oversized full ladder rather than submit it partially.

The live report and periodic `LIVE DYNAMIC BASE SCALING` log include the active base, profit balance, next threshold, increase count, and whether capacity is automatic or explicit. Settings are persisted across controlled GitHub Actions handoffs.

### Live Prophet equity regime

The live controller is enabled with live state transitions (`equity_regime_enabled=true`, `equity_regime_dry_run=false`). It retains the most recent 200 completed markets, refits Prophet for the next eligible market after every finalized balance observation, and applies a P90/P10 signal only to that next market.

- It stops new real entries after a saved shadow-equity P90 observation and leaves already-filled positions to the existing 5¢ stop or settlement logic.
- While stopped, it always creates a fixed settlement-contrarian shadow decision: **3/6/9/12** contracts at **40¢/30¢/20¢/10¢**. The shadow fill model uses only post-decision public trades and is explicitly a conservative approximation, not a queue-aware replay.
- Dynamic base-share scaling applies only to the real, authenticated account balance. It can change a later live order, but it never changes the shadow ladder or rebases the shadow balance.
- A P10 observation restarts real entry placement for the following eligible market. The controller persists its state, forecasts, shadow curve, and actual curve under `data/`; the workflow includes them in checkpoints and audit artifacts.

The retained diagnostic horizon is exploratory sensitivity evidence, not an unbiased live-performance claim. Its P10–P90 interval coverage was poor in the strict 200-trade review, so the saved forecast and shadow-simulation records should be monitored rather than treated as a deployment guarantee.

## Operations

Use these active Actions:

1. **Kalshi BTC 15m Settlement Contrarian** — continuous trader and configuration inputs.
2. **Controlled Restart — Kalshi BTC 15m Settlement Contrarian** — safe runner replacement.
3. **Kalshi BTC 15m Live Trader Watchdog** — recovery safety net.
4. **Kalshi BTC 15m Position Audit (Read Only)** — position inspection without orders.

Every live start runs [`tests/live/test_kalshi_btc15m_settlement_trader.py`](tests/live/test_kalshi_btc15m_settlement_trader.py) first. The runner checkpoints the configuration, trade state, compact report, loss-skip state, and dynamic-scaling state before handoff.

## Layout

```text
.github/workflows/   active workflows
archive/             retired code and research
tests/live/          live-trader safety checks
kalshi_btc15m_average_down.py
kalshi_btc15m_average_down_{config,state,report}.json
kalshi_btc15m_position_audit.py
polymarket_bot.py
```
