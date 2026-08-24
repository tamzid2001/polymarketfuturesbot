# Archived-ledger 45c stop counterfactual

This is not an exact historical fill/stop replay. Settlements and retained books are fixed; unobserved late execution is simulated.

- Markets: 214
- Eligible derived limits above 45c: 190
- Rejected because derived limit was at/below stop: 24
- Winner-survivor first-minute one-cent-lower touches: 26/50
- Observed 45c stop after those touches: 10/26

| Sizing | Scenario | P&L | Final balance | Return | Max drawdown | Fills | Stops | False stops |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| recovery_1.01x | strict_60s_trade_through | $-13.9858 | $986.0142 | -1.3986% | $15.4543 | 161 | 150 | 53 |
| recovery_1.01x | observed_60s_quote_touch | $-8.1499 | $991.8501 | -0.8150% | $9.6925 | 166 | 150 | 53 |
| recovery_1.01x | all_eligible_fill | $7.9686 | $1007.9686 | 0.7969% | $1.1356 | 190 | 150 | 53 |
| fixed_one_share | strict_60s_trade_through | $-6.7000 | $993.3000 | -0.6700% | $7.0100 | 161 | 150 | 53 |
| fixed_one_share | observed_60s_quote_touch | $-4.4000 | $995.6000 | -0.4400% | $4.7100 | 166 | 150 | 53 |
| fixed_one_share | all_eligible_fill | $7.0300 | $1007.0300 | 0.7030% | $1.0800 | 190 | 150 | 53 |

## Empirical late-path Monte Carlo

### recovery_1.01x

- Simulations: 5000
- Mean P&L: $-9.0336
- P5 / median / P95 P&L: $-17.9392 / $-9.2821 / $0.1010
- Mean return: -0.9034%
- P5 / median / P95 return: -1.7939% / -0.9282% / 0.0101%
- Median / P95 max drawdown: $12.5517 / $19.3527
- P50 / P95 required bankroll: $15.1062 / $22.0811

### fixed_one_share

- Simulations: 5000
- Mean P&L: $-4.5618
- P5 / median / P95 P&L: $-7.1105 / $-4.6000 / $-1.9495
- Mean return: -0.4562%
- P5 / median / P95 return: -0.7110% / -0.4600% / -0.1949%
- Median / P95 max drawdown: $5.2700 / $7.6200
- P50 / P95 required bankroll: $5.7300 / $8.0900

The Monte Carlo probabilities are proxies derived from the retained first-minute winner-survivor books. They are sensitivity assumptions, not recovered later-market paths.
