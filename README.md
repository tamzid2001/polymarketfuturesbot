# Trading Automation

This repository is organized around the systems that are currently operational. Historical ML, Prophet, paper-trading, and backtest material is preserved in [`archive/`](archive/README.md) and is not part of a live deployment.

## Active Kalshi BTC 15-minute trader

[`kalshi_btc15m_average_down.py`](kalshi_btc15m_average_down.py) is the only active Kalshi execution strategy.

- At market open, it waits only for the **immediately preceding** KXBTC15M market to finalize, then freezes the opposite side and submits the ladder without a fixed delay. It never substitutes an older outcome while that predecessor is still settling; the maximum wait is 120 seconds after open.
- It posts a single-side GTC ladder at 40¢ / 30¢ / 20¢ / 10¢.
- Default share multiplier is **3**, so the four rungs are **3 / 6 / 9 / 12** contracts: 30 contracts and $6 maximum principal before fees.
- There is no ML model, profit gate, 60¢ activation, or trailing stop in the live path.
- Every fill holds to settlement unless the selected-side fresh full-depth bid is **≤5¢**. That is the sole emergency reduce-only exit.
- After **two consecutive completed realized losses** on filled live trades, it keeps generating the normal settlement-contrarian signal but skips the next **two** signaled markets without submitting orders. A completed winner clears the loss count immediately; zero-fill and dry-run records do not count.
- Configuration, state, and the compact live performance report persist at the repository root so every Actions handoff resumes the same ladder sizing and open-position state.

Use these Actions:

1. **Kalshi BTC 15m Settlement Contrarian** — continuous live trader.
2. **Controlled Restart — Kalshi BTC 15m Settlement Contrarian** — stops a specified older runner and queues one safe replacement.
3. **Kalshi BTC 15m Live Trader Watchdog** — recovery safety net.
4. **Kalshi BTC 15m Position Audit (Read Only)** — inspect an exact Kalshi position without submitting orders.

The live action verifies [`tests/live/test_kalshi_btc15m_settlement_trader.py`](tests/live/test_kalshi_btc15m_settlement_trader.py) before it starts. The checks cover the 3/6/9/12 ladder, persistent sizing, the flat 5¢ stop, the two-loss/two-signal skip, and the absence of the retired trailing/gate exit logic.

## Active Polymarket system

[`polymarket_bot.py`](polymarket_bot.py) and **Polymarket Portfolio Execution Engine** remain a separate active system. Its operational files stay at the root because that runner persists `state.json` and `markets.json` between handoffs.

## Layout

```text
.
├── .github/workflows/       # only currently operational workflows
├── archive/                 # retired code, reports, tests, and disabled workflows
├── tests/live/              # live-trader safety checks
├── kalshi_btc15m_average_down.py
├── kalshi_btc15m_average_down_config.json
├── kalshi_btc15m_average_down_state.json
├── kalshi_btc15m_average_down_report.json
├── kalshi_btc15m_position_audit.py
├── requirements_kalshi_settlement_trader.txt
├── polymarket_bot.py
├── markets.json
└── state.json
```

`archive/workflows/` is intentionally outside `.github/workflows/`, so GitHub Actions no longer discovers or schedules those retired jobs. Historical files remain available for reference without competing with the live trader or cluttering the Actions page.
