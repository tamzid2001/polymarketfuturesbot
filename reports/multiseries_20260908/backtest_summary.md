# Multi-series historical settlement directional replay

Snapshot cutoff: 2026-09-08T01:27:59.221576Z

Primary: unchanged original BTC algorithm, market open +45s, inverse of the most recent available official prior settlement. Every eligible market is scored against its actual settlement. No loss-based skips. Sticky-after-loss/flip-after-win was checked against the same source sequence.

Secondary boundary proxy: immediately previous eventual settlement used as a provisional-outcome proxy. This is NOT proof that the result was known at the opening boundary; separate CSV prevents mixing it into causal historical evidence.

No fills, stops, fees, recovery-sizing P&L, or live orders are inferred or simulated in this directional-only test.

| Series | Settled | Eligible | W / L | WR | 95% Wilson CI | Binomial p | Bonferroni p | Max W/L streak |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| KXBNB15M | 16,288 | 16,287 | 8,365 / 7,922 | 51.3600% | 50.59%–52.13% | 0.000533023 | 0.0069293 | 12 / 14 |
| KXBTC15M | 25,267 | 25,262 | 13,002 / 12,260 | 51.4686% | 50.85%–52.08% | 3.12469e-06 | control; excluded | 14 / 11 |
| KXCOPPER15M | 731 | 730 | 366 / 364 | 50.1370% | 46.52%–53.75% | 0.970479 | 1 | 10 / 8 |
| KXDOGE15M | 16,287 | 16,286 | 8,448 / 7,838 | 51.8728% | 51.11%–52.64% | 1.81829e-06 | 2.36377e-05 | 13 / 11 |
| KXETH15M | 25,254 | 25,249 | 12,987 / 12,262 | 51.4357% | 50.82%–52.05% | 5.19782e-06 | 6.75716e-05 | 12 / 14 |
| KXGOLD15M | 2,596 | 2,595 | 1,291 / 1,304 | 49.7495% | 47.83%–51.67% | 0.813775 | 1 | 12 / 9 |
| KXHYPE15M | 16,287 | 16,286 | 8,275 / 8,011 | 50.8105% | 50.04%–51.58% | 0.0393131 | 0.51107 | 13 / 11 |
| KXNATGAS15M | 731 | 730 | 366 / 364 | 50.1370% | 46.52%–53.75% | 0.970479 | 1 | 15 / 7 |
| KXNEAR15M | 6,568 | 6,567 | 3,283 / 3,284 | 49.9924% | 48.78%–51.20% | 1 | 1 | 14 / 10 |
| KXSILVER15M | 2,596 | 2,595 | 1,331 / 1,264 | 51.2909% | 49.37%–53.21% | 0.195099 | 1 | 10 / 8 |
| KXSOL15M | 22,890 | 22,888 | 11,826 / 11,062 | 51.6690% | 51.02%–52.32% | 4.56337e-07 | 5.93238e-06 | 14 / 11 |
| KXWTI15M | 2,596 | 2,595 | 1,350 / 1,245 | 52.0231% | 50.10%–53.94% | 0.0411734 | 0.535254 | 15 / 10 |
| KXXRP15M | 19,671 | 19,669 | 9,982 / 9,687 | 50.7499% | 50.05%–51.45% | 0.0360517 | 0.468673 | 13 / 11 |
| KXZEC15M | 6,568 | 6,567 | 3,434 / 3,133 | 52.2918% | 51.08%–53.50% | 0.000213394 | 0.00277412 | 14 / 10 |

Non-BTC descriptive total: **13 series; 139,063 settled markets; 139,044 eligible signals; 71,304 wins / 67,740 losses; 51.2816% WR**. No pooled binomial p-value or cross-series streak is claimed: simultaneous asset signals are correlated and do not form one independent trading sequence.

## Statistical interpretation

Two-sided exact binomial test: H0 is P(directional win)=0.50 versus H1 !=0.50. For n signals and w wins, p=min(1, 2*sum(comb(n,k), k=0..min(w,n-w))/2**n). The implementation evaluates this probability in log space. This is not the probability that H0 is true, and 50% is not the live strategy's fee-adjusted break-even rate.

Bonferroni p=min(1, 13*raw p) across the 13 non-BTC primary tests; BTC is a separate pre-existing control. A corrected p<0.05 survives this specified comparison family only. Wilson intervals are individual, not simultaneous. Both intervals and tests are nominal IID-Bernoulli benchmarks: correction for multiple series does not fix serial dependence, earlier strategy selection, or execution uncertainty.

## Coverage and chronological stability

Halves split eligible signals chronologically, not calendar days; the second half has one extra observation when n is odd. The recent window is the latest min(1000,n) eligible signals. Current streak means at the frozen snapshot, not the current live worker.

| Series | First settled market open (UTC) | Last settled market open (UTC) | No eligible signal | First-half WR | Second-half WR | Recent n | Recent W / L | Recent WR | Current streak |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| KXBNB15M | 2026-03-07T00:15:00Z | 2026-09-08T01:00:00Z | 1 | 51.19% | 51.53% | 1,000 | 501 / 499 | 50.10% | 3W |
| KXBTC15M | 2025-12-10T21:45:00Z | 2026-09-08T01:00:00Z | 5 | 51.80% | 51.14% | 1,000 | 499 / 501 | 49.90% | 2L |
| KXCOPPER15M | 2026-08-27T20:30:00Z | 2026-09-08T01:00:00Z | 1 | 53.42% | 46.85% | 730 | 366 / 364 | 50.14% | 1W |
| KXDOGE15M | 2026-03-18T20:00:00Z | 2026-09-08T01:00:00Z | 1 | 51.66% | 52.08% | 1,000 | 519 / 481 | 51.90% | 1L |
| KXETH15M | 2025-12-10T21:45:00Z | 2026-09-08T01:00:00Z | 5 | 51.05% | 51.83% | 1,000 | 516 / 484 | 51.60% | 2L |
| KXGOLD15M | 2026-07-31T18:00:00Z | 2026-09-08T01:00:00Z | 1 | 49.19% | 50.31% | 1,000 | 502 / 498 | 50.20% | 1L |
| KXHYPE15M | 2026-03-18T20:00:00Z | 2026-09-08T01:00:00Z | 1 | 50.42% | 51.20% | 1,000 | 518 / 482 | 51.80% | 1W |
| KXNATGAS15M | 2026-08-27T20:30:00Z | 2026-09-08T01:00:00Z | 1 | 49.04% | 51.23% | 730 | 366 / 364 | 50.14% | 4L |
| KXNEAR15M | 2026-06-30T17:15:00Z | 2026-09-08T01:00:00Z | 1 | 50.84% | 49.15% | 1,000 | 494 / 506 | 49.40% | 1W |
| KXSILVER15M | 2026-07-31T18:00:00Z | 2026-09-08T01:00:00Z | 1 | 51.04% | 51.54% | 1,000 | 514 / 486 | 51.40% | 3L |
| KXSOL15M | 2026-01-09T00:30:00Z | 2026-09-08T01:00:00Z | 2 | 52.28% | 51.06% | 1,000 | 509 / 491 | 50.90% | 2L |
| KXWTI15M | 2026-07-31T18:00:00Z | 2026-09-08T01:00:00Z | 1 | 51.50% | 52.54% | 1,000 | 528 / 472 | 52.80% | 3W |
| KXXRP15M | 2026-02-11T05:00:00Z | 2026-09-08T01:00:00Z | 2 | 51.31% | 50.19% | 1,000 | 527 / 473 | 52.70% | 2L |
| KXZEC15M | 2026-06-30T17:15:00Z | 2026-09-08T01:00:00Z | 1 | 52.54% | 52.04% | 1,000 | 516 / 484 | 51.60% | 2L |

The universe is the catalog's active fifteen-minute Crypto/Commodities up-or-down series with settled markets in the current API, plus BTC as a control. Templates with no current settled rows are listed separately in series_discovery.json; this does not assert their archive is empty.

Both current and historical settlement endpoints must finish pagination. Duplicate tickers are deduplicated, conflicts abort, and post-cutoff settlements are excluded. Young series have shorter histories; their estimates are not as precise as BTC's. CI/p-values are nominal independent-trial benchmarks; serial dependence, cross-asset correlation, and testing multiple series can invalidate a simple significance interpretation. Streaks span eligible signals, including market/session gaps.

Sources: [Kalshi markets](https://docs.kalshi.com/api-reference/market/get-markets), [series discovery](https://docs.kalshi.com/api-reference/market/get-series-list), and [binomial-test definition](https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.binomtest.html).

Reproduce offline in this checkout: `python kalshi_multiseries_backtest.py --output reports/multiseries_20260908 --cache data/raw/multiseries_20260908 --offline`

Or extract reproducible_settlement_replay.zip into an empty folder and run: `python kalshi_multiseries_backtest.py --output reports --cache cache --offline` (Python 3.11+; standard library only).
