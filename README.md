# Trading Automation

This repository contains a live Kalshi BTC 15-minute settlement-contrarian trader and a separate Polymarket portfolio system. Retired ML, Prophet, paper-trading, and backtest material is preserved under [`archive/`](archive/README.md) and is not deployed.

## Kalshi BTC 15-minute trader

[`kalshi_btc15m_average_down.py`](kalshi_btc15m_average_down.py) is the only active Kalshi execution path.

- At a market open, it waits for the **immediately preceding** KXBTC15M market to finalize. It locks the opposite YES/NO side as soon as that result is available—there is no fixed 45-second wait and no fallback to an older result.
- It posts a single-side GTC ladder at 40¢ / 30¢ / 20¢ / 10¢, expiring at the market close. The default base is **3**, giving **3 / 6 / 9 / 12** contracts (30 total, $6 principal before fees).
- Filled contracts hold to settlement. The only early exit is a reduce-only stop when the selected-side fresh full-depth bid is **≤5¢**. There is no ML signal, profit gate, or trailing stop in the live path.
- After two consecutive realized losses on filled live trades, it still computes and records the normal next two signals, but sends no balance check or exchange orders for those two markets. It then resets and resumes. A realized win resets the loss count immediately.

### Optional dynamic base-share scaling

Dynamic scaling is disabled by default, so the configured starting base stays fixed. Enable it from the **Kalshi BTC 15m Settlement Contrarian** Action with:

- `enable_dynamic_scaling`: `true` or `false` (default `false`)
- `base_share_increment`: base shares added after a threshold, in 0.01-share increments (default `1`)
- `scaling_profit_multiplier`: realized net profit required per current base share (default `16.5`)

When enabled, the runner starts a fresh scaling balance at the configured base. It accumulates realized net P&L from subsequently completed, filled live trades. At:

```text
profit_since_last_increase >= current_base_share_count × scaling_profit_multiplier
```

it increases the base by `base_share_increment`, resets that balance to zero, and uses the new **1/2/3/4** ladder only for later markets. Bases and rungs retain 0.01-share precision (for example, base `3.25` creates `3.25 / 6.50 / 9.75 / 13.00`). Existing GTC ladders retain their original size. Runner-owned contract and principal caps grow as needed; explicitly supplied caps are never overridden and will safely block an oversized full ladder rather than submit it partially.

The live report and periodic `LIVE DYNAMIC BASE SCALING` log include the active base, profit balance, next threshold, increase count, and whether capacity is automatic or explicit. Settings are persisted across controlled GitHub Actions handoffs.

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
