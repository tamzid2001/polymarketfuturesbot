# Multi-series historical settlement directional replay

Snapshot cutoff: 2026-09-08T01:27:59.221576Z

Primary: unchanged original BTC algorithm, market open +45s, inverse of the most recent available official prior settlement. Every eligible market is scored against its actual settlement. No loss-based skips. Sticky-after-loss/flip-after-win was checked against the same source sequence.

Secondary boundary proxy: immediately previous eventual settlement used as a provisional-outcome proxy. This is NOT proof that the result was known at the opening boundary; separate CSV prevents mixing it into causal historical evidence.

No fills, stops, fees, recovery-sizing P&L, or live orders are inferred or simulated in this directional-only test.

| Series | Settled | Eligible | W / L | WR | 95% Wilson CI | Max W/L streak |
|---|---:|---:|---:|---:|---:|---:|
| KXBNB15M | 16,288 | 16,287 | 8,365 / 7,922 | 51.3600% | 50.59%–52.13% | 12 / 14 |
| KXBTC15M | 25,267 | 25,262 | 13,002 / 12,260 | 51.4686% | 50.85%–52.08% | 14 / 11 |
| KXCOPPER15M | 731 | 730 | 366 / 364 | 50.1370% | 46.52%–53.75% | 10 / 8 |
| KXDOGE15M | 16,287 | 16,286 | 8,448 / 7,838 | 51.8728% | 51.11%–52.64% | 13 / 11 |
| KXETH15M | 25,254 | 25,249 | 12,987 / 12,262 | 51.4357% | 50.82%–52.05% | 12 / 14 |
| KXGOLD15M | 2,596 | 2,595 | 1,291 / 1,304 | 49.7495% | 47.83%–51.67% | 12 / 9 |
| KXHYPE15M | 16,287 | 16,286 | 8,275 / 8,011 | 50.8105% | 50.04%–51.58% | 13 / 11 |
| KXNATGAS15M | 731 | 730 | 366 / 364 | 50.1370% | 46.52%–53.75% | 15 / 7 |
| KXNEAR15M | 6,568 | 6,567 | 3,283 / 3,284 | 49.9924% | 48.78%–51.20% | 14 / 10 |
| KXSILVER15M | 2,596 | 2,595 | 1,331 / 1,264 | 51.2909% | 49.37%–53.21% | 10 / 8 |
| KXSOL15M | 22,890 | 22,888 | 11,826 / 11,062 | 51.6690% | 51.02%–52.32% | 14 / 11 |
| KXWTI15M | 2,596 | 2,595 | 1,350 / 1,245 | 52.0231% | 50.10%–53.94% | 15 / 10 |
| KXXRP15M | 19,671 | 19,669 | 9,982 / 9,687 | 50.7499% | 50.05%–51.45% | 13 / 11 |
| KXZEC15M | 6,568 | 6,567 | 3,434 / 3,133 | 52.2918% | 51.08%–53.50% | 14 / 10 |

The universe is the catalog's active fifteen-minute Crypto/Commodities up-or-down series with settled markets in the current API, plus BTC as a control. Templates with no current settled rows are listed separately in series_discovery.json; this does not assert their archive is empty.

Both current and historical settlement endpoints must finish pagination. Duplicate tickers are deduplicated, conflicts abort, and post-cutoff settlements are excluded. Young series have shorter histories; their estimates are not as precise as BTC's. CI/p-values are nominal independent-trial benchmarks; serial dependence, cross-asset correlation, and testing multiple series can invalidate a simple significance interpretation. Streaks span eligible signals, including market/session gaps.

Sources: https://docs.kalshi.com/api-reference/market/get-markets and https://docs.kalshi.com/api-reference/market/get-series-list

Reproduce offline in this checkout: `python kalshi_multiseries_backtest.py --output reports/multiseries_20260908 --cache data/raw/multiseries_20260908 --offline`

Or extract reproducible_settlement_replay.zip into an empty folder and run: `python kalshi_multiseries_backtest.py --output reports --cache cache --offline` (Python 3.11+; standard library only).
