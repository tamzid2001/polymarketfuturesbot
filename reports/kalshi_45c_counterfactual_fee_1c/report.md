# Archived-ledger 45c stop counterfactual

This is not an exact historical fill/stop replay. Settlements and retained books are fixed; unobserved late execution is simulated.

- Markets: 214
- Eligible derived limits above 45c: 190
- Rejected because derived limit was at/below stop: 24
- Winner-survivor first-minute one-cent-lower touches: 26/50
- Observed 45c stop after those touches: 10/26

| Sizing | Scenario | P&L | Final balance | Return | Max drawdown | Fills | Stops | False stops |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| recovery_1.01x | strict_60s_trade_through | $-14.2960 | $985.7040 | -1.4296% | $15.7168 | 161 | 150 | 53 |
| recovery_1.01x | observed_60s_quote_touch | $-8.6067 | $991.3933 | -0.8607% | $10.0992 | 166 | 150 | 53 |
| recovery_1.01x | all_eligible_fill | $7.5329 | $1007.5329 | 0.7533% | $1.1457 | 190 | 150 | 53 |
| fixed_one_share | strict_60s_trade_through | $-6.8100 | $993.1900 | -0.6810% | $7.1100 | 161 | 150 | 53 |
| fixed_one_share | observed_60s_quote_touch | $-4.5600 | $995.4400 | -0.4560% | $4.8600 | 166 | 150 | 53 |
| fixed_one_share | all_eligible_fill | $6.6300 | $1006.6300 | 0.6630% | $1.0800 | 190 | 150 | 53 |

## Empirical late-path Monte Carlo

### recovery_1.01x

- Simulations: 5000
- Mean P&L: $-9.6458
- P5 / median / P95 P&L: $-18.2984 / $-9.9020 / $-0.3578
- Mean return: -0.9646%
- P5 / median / P95 return: -1.8298% / -0.9902% / -0.0358%
- Median / P95 max drawdown: $12.9803 / $19.6675
- P50 / P95 required bankroll: $15.5458 / $22.3780

### fixed_one_share

- Simulations: 5000
- Mean P&L: $-4.7376
- P5 / median / P95 P&L: $-7.2405 / $-4.7700 / $-2.1795
- Mean return: -0.4738%
- P5 / median / P95 return: -0.7240% / -0.4770% / -0.2179%
- Median / P95 max drawdown: $5.4100 / $7.7300
- P50 / P95 required bankroll: $5.8700 / $8.2005

The Monte Carlo probabilities are proxies derived from the retained first-minute winner-survivor books. They are sensitivity assumptions, not recovered later-market paths.
