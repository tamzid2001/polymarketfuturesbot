# Archived-ledger 45c stop counterfactual

This is not an exact historical fill/stop replay. Settlements and retained books are fixed; unobserved late execution is simulated.

- Archived realized P&L: $1.7803 (final balance $1001.7803)
- Markets: 214
- Eligible derived limits above 45c: 190
- Rejected because derived limit was at/below stop: 24
- Winner-survivor first-minute one-cent-lower touches: 26/50
- Observed 45c stop after those touches: 10/26

## Archived-fill arithmetic bounds

These fixed-one-share bounds assume every old IOC fill, or every old IOC fill minus the configured offset, would participate. They do not model maker non-fills.

| Price proxy | Eligible | Old settlements | Old stops | Additional observed winner stops | Optimistic P&L | First-minute-evidence P&L |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Old actual IOC entry | 193 | 50 | 143 | 10 | $11.98 | $6.48 |
| Old actual IOC entry minus offset | 187 | 50 | 137 | 10 | $13.91 | $8.41 |

The optimistic column is the zero-additional-false-stop calculation. The first-minute-evidence column also stops old profitable settlements whose retained post-fill executable bid was already at or below the new stop. Complete later paths remain unavailable.

| Sizing | Scenario | P&L | Final balance | Return | Max drawdown | Fills | Stops | False stops |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| recovery_1.01x | strict_60s_trade_through | $-17.6384 | $982.3616 | -1.7638% | $18.9609 | 161 | 150 | 53 |
| recovery_1.01x | observed_60s_quote_touch | $-11.9089 | $988.0911 | -1.1909% | $13.2982 | 166 | 150 | 53 |
| recovery_1.01x | all_eligible_fill | $6.5965 | $1006.5965 | 0.6596% | $1.3699 | 190 | 150 | 53 |
| fixed_one_share | strict_60s_trade_through | $-8.2000 | $991.8000 | -0.8200% | $8.4800 | 161 | 150 | 53 |
| fixed_one_share | observed_60s_quote_touch | $-5.9000 | $994.1000 | -0.5900% | $6.1800 | 166 | 150 | 53 |
| fixed_one_share | all_eligible_fill | $5.5300 | $1005.5300 | 0.5530% | $1.2400 | 190 | 150 | 53 |

## Empirical late-path Monte Carlo

### recovery_1.01x

- Simulations: 5000
- Mean P&L: $-13.6555
- P5 / median / P95 P&L: $-22.4218 / $-13.8912 / $-3.9426
- Mean return: -1.3655%
- P5 / median / P95 return: -2.2422% / -1.3891% / -0.3943%
- Median / P95 max drawdown: $16.4564 / $23.5172
- P50 / P95 required bankroll: $19.0360 / $26.1807

### fixed_one_share

- Simulations: 5000
- Mean P&L: $-6.1713
- P5 / median / P95 P&L: $-8.7600 / $-6.2000 / $-3.5295
- Mean return: -0.6171%
- P5 / median / P95 return: -0.8760% / -0.6200% / -0.3529%
- Median / P95 max drawdown: $6.7500 / $9.1705
- P50 / P95 required bankroll: $7.2100 / $9.6400

The Monte Carlo probabilities are proxies derived from the retained first-minute winner-survivor books. They are sensitivity assumptions, not recovered later-market paths.
