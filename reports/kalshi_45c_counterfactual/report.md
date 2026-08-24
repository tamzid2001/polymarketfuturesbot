# Archived-ledger 45c stop counterfactual

This is not an exact historical fill/stop replay. Settlements and retained books are fixed; unobserved late execution is simulated.

- Markets: 214
- Eligible derived limits above 45c: 190
- Rejected because derived limit was at/below stop: 24
- Winner-survivor first-minute one-cent-lower touches: 26/50
- Observed 45c stop after those touches: 10/26

| Sizing | Scenario | P&L | Final balance | Return | Max drawdown | Fills | Stops | False stops |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| recovery_1.01x | strict_60s_trade_through | $-10.3332 | $989.6668 | -1.0333% | $11.9477 | 161 | 150 | 53 |
| recovery_1.01x | observed_60s_quote_touch | $-4.3909 | $995.6091 | -0.4391% | $6.0868 | 166 | 150 | 53 |
| recovery_1.01x | all_eligible_fill | $9.2446 | $1009.2446 | 0.9245% | $1.0089 | 190 | 150 | 53 |
| fixed_one_share | strict_60s_trade_through | $-5.2000 | $994.8000 | -0.5200% | $5.5500 | 161 | 150 | 53 |
| fixed_one_share | observed_60s_quote_touch | $-2.9000 | $997.1000 | -0.2900% | $3.8900 | 166 | 150 | 53 |
| fixed_one_share | all_eligible_fill | $8.5300 | $1008.5300 | 0.8530% | $0.9600 | 190 | 150 | 53 |

## Empirical late-path Monte Carlo

### recovery_1.01x

- Simulations: 10000
- Mean P&L: $-4.4887
- P5 / median / P95 P&L: $-13.3196 / $-3.5660 / $1.5592
- Mean return: -0.4489%
- P5 / median / P95 return: -1.3320% / -0.3566% / 0.1559%
- Median / P95 max drawdown: $7.9667 / $15.3614
- P50 / P95 required bankroll: $10.2607 / $18.0184

### fixed_one_share

- Simulations: 10000
- Mean P&L: $-2.9463
- P5 / median / P95 P&L: $-5.4900 / $-2.9600 / $-0.3300
- Mean return: -0.2946%
- P5 / median / P95 return: -0.5490% / -0.2960% / -0.0330%
- Median / P95 max drawdown: $3.9100 / $6.0900
- P50 / P95 required bankroll: $4.3600 / $6.5500

The Monte Carlo probabilities are proxies derived from the retained first-minute winner-survivor books. They are sensitivity assumptions, not recovered later-market paths.
