# Trading Automation

This repository is organized around the systems that are currently operational. Historical ML, Prophet, paper-trading, and backtest material is preserved in [`archive/`](archive/README.md) and is not part of a live deployment.

## Active Kalshi BTC 15-minute trader

[`kalshi_btc15m_average_down.py`](kalshi_btc15m_average_down.py) is the only active Kalshi execution strategy.

- At market open, it checks for the **immediately preceding** KXBTC15M market to finalize at the configured polling cadence (two seconds by default). A source is valid only when its `close_time` equals the new market's `open_time`; it never substitutes an older result while that predecessor is still settling.
- As soon as that predecessor is available, it freezes and persists the opposite YES/NO side, then posts one single-side GTC ladder. There is no fixed 45-second delay; the maximum source-and-entry window is 120 seconds after open.
- The GTC ladder is 40¢ / 30¢ / 20¢ / 10¢ and expires at that market's close.
- Default share multiplier is **3**, so the four rungs are **3 / 6 / 9 / 12** contracts: 30 contracts and $6 maximum principal before fees.
- There is no ML model, profit gate, 60¢ activation, or trailing stop in the live path.
- Every fill holds to settlement unless the selected-side fresh full-depth bid is **≤5¢**. That is the sole emergency reduce-only exit.
- After **two consecutive completed realized losses** on filled live trades, it still computes, locks, and records the normal signal but skips the next **two** signaled markets without a balance check or exchange order. The second skip clears the counter, so the following eligible signal submits normally. A completed winner clears the loss count immediately; zero-fill and dry-run records do not count.
- GitHub-hosted jobs use controlled handoffs: each job checkpoints configuration, open-order state, the loss-skip state, and the compact performance report before queuing the next runner. This provides continuous operation without relying on one indefinite job.

Use these Actions:

1. **Kalshi BTC 15m Settlement Contrarian** — continuous live trader.
2. **Controlled Restart — Kalshi BTC 15m Settlement Contrarian** — stops a specified older runner and queues one safe replacement.
3. **Kalshi BTC 15m Live Trader Watchdog** — recovery safety net.
4. **Kalshi BTC 15m Position Audit (Read Only)** — inspect an exact Kalshi position without submitting orders.

The live action verifies [`tests/live/test_kalshi_btc15m_settlement_trader.py`](tests/live/test_kalshi_btc15m_settlement_trader.py) before it starts. The checks cover immediate-predecessor signal locking, no older-settlement fallback, the 120-second entry window, the 3/6/9/12 ladder, persistent sizing, the flat 5¢ stop, the two-loss/two-signal skip and resume, and the absence of retired trailing/gate exit logic.

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
