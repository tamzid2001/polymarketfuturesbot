# Trading Automation

This repository contains a KXBTC15M hybrid Kalshi trader and a separate Polymarket portfolio system. Retired ML and paper-study material is preserved under [`archive/`](archive/README.md) and is not deployed.

## Kalshi BTC 15-minute trader

[`kalshi_live_trader.py`](kalshi_live_trader.py) is the only active Kalshi execution path. It reuses the established signed Kalshi REST/WebSocket transport, but never invokes the legacy ladder, shadow-balance, loss-skip, Prophet, or equity-regime logic.

- It subscribes to the active/previous/upcoming exchange-provided KXBTC15M markets. In the final configurable observation window it freezes a provisional prior outcome only when the latest pre-boundary executable YES or NO bid is at least 99¢ and fresh. The next signal is the inverse side at the new market open. A missing, stale, or conflicting outcome fails closed; it never substitutes the market from two windows ago.
- The default is a 50¢ post-only maker limit. A crossed maker order is never converted to a taker order, and an entry is refused if the selected-side ask is at or below the 40¢ stop. A no-fill changes neither recovery nor permanent-base state.
- The permanent base starts at **1.00** and all quantities are two-decimal `Decimal` values. Filled trade P&L alone advances recovery and geometric permanent-base scaling through the shared [`strategy_core.py`](strategy_core.py) functions used by the historical replay.
- A fresh executable bid at or below the configured 40¢ stop cancels remaining entries and sends reduce-only exits for the exchange-confirmed filled position only. Otherwise a filled position is reconciled through official settlement. Every completed trade records actual entry/exit fills, fees, and net P&L once.
- [`selected_live_strategy.json`](selected_live_strategy.json) is the canonical persistent live configuration. The workflow writes configuration-input changes back to it. The optimizer can produce the same schema with `optimizer.py --entry-price .50`.

The worker persists atomic durable state and append-only JSONL audit records under `data/`. It reconciles authenticated balance, open hybrid-prefixed orders, positions, fills, and settlement state before any new risk. Unknown ownership, stale/ambiguous state, position-direction mismatch, insufficient funding, stale quote, or a circuit breaker blocks entries. State and audit are separate for dry/shadow runs so observation mode cannot alter live recovery state.

Real money requires both the repository/environment variable `KALSHI_LIVE_ENABLED=true` and the workflow-dispatch `live_enabled=true` with `dry_run=false`. Scheduled jobs use the same repository variable and are dry by default when it is unset. Credentials are only `KALSHI_API_KEY_ID` and `KALSHI_PRIVATE_KEY` secrets; no credential values are persisted or logged.

### Historical Kalshi hybrid backtest

The research framework is a **historical settlement replay with Monte Carlo execution-path simulation**. It caches actual KXBTC15M settlement outcomes, reconstructs the causal 45-second settlement-contrarian signal, and never redraws a directional result. Because Kalshi's settlement history has no complete intramarket path, 49¢ maker fills, adverse rung depth, and stops are simulated.

The observed 139/(139+318) and 209/(209+221) old-ladder rates calibrate the joint adverse **40¢-region** probability conditional on the actual settlement outcome. They do not identify a 49¢ maker-order fill rate. The optimizer therefore reports 49¢ participation as an explicit conservative/base/optimistic scenario; `reconstruction_compatible` is a clearly labelled full-participation comparison to the earlier reconstruction, not a factual historical-fill assertion.

```bash
python3.13 -m venv .venv
.venv/bin/pip install -r requirements_kalshi_hybrid_backtest.txt
.venv/bin/python kalshi_settlement_loader.py --refresh
.venv/bin/python optimizer.py --output-dir outputs/kalshi_hybrid_backtest/base_case --execution-scenario base_case --entry-price .50
# Optional like-for-like comparison to the older 1,500-market reconstruction:
.venv/bin/python optimizer.py --output-dir outputs/kalshi_hybrid_backtest/base_case --execution-scenario base_case --reconciliation-simulations 50000
```

The loader writes `historical_signals.parquet`; the optimization directory contains calibration, grid, Pareto, walk-forward, stress, bankroll, regime, and plot artifacts. Simulated fills and stop events are explicitly hypothetical execution paths, not asserted historical events.

## Operations

Use these active Actions:

1. **Kalshi KXBTC15M Hybrid Live** — serialized continuous worker, with configuration inputs.
2. **Controlled Restart — Kalshi KXBTC15M Hybrid** — queues a reconcile-only handoff.
3. **Kalshi KXBTC15M Hybrid Live Watchdog** — recovery safety net.
4. **Kalshi BTC 15m Position Audit (Read Only)** — position inspection without orders.

Every worker starts with the shared-core, execution, reconciliation, and historical-replay unit suite. A manual dry run is:

```bash
KALSHI_API_KEY_ID=... KALSHI_PEM_PATH=kalshi_private_key.pem \
  .venv/bin/python kalshi_live_trader.py --config selected_live_strategy.json \
  --state-file data/kalshi_shadow_strategy_state.json \
  --audit-ledger data/kalshi_shadow_strategy_audit.jsonl --dry-run --run-seconds 120
```

Reconciliation only is:

```bash
KALSHI_API_KEY_ID=... KALSHI_PEM_PATH=kalshi_private_key.pem \
  .venv/bin/python kalshi_live_trader.py --config selected_live_strategy.json --reconcile-only
```

## Layout

```text
.github/workflows/   active workflows
archive/             retired code and research
tests/               hybrid live/backtest safety checks
kalshi_live_trader.py
strategy_core.py
selected_live_strategy.json
data/kalshi_{live,shadow}_strategy_{state,audit}.json*
kalshi_btc15m_position_audit.py
polymarket_bot.py
```
