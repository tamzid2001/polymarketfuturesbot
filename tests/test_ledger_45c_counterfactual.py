from __future__ import annotations

import unittest
from decimal import Decimal
from pathlib import Path

from ledger_45c_counterfactual import (
    LedgerTrade,
    archived_ledger_reconciliation,
    empirical_calibration,
    replay,
    report_decimal,
)
from strategy_core import StrategyParameters


class Ledger45cCounterfactualTests(unittest.TestCase):
    def parameters(self) -> StrategyParameters:
        return StrategyParameters(
            recovery_multiplier=Decimal("1.01"), first_base_threshold=Decimal("350"),
            threshold_growth_multiplier=Decimal("1.01"), base_increment=Decimal("0.50"),
            starting_base=Decimal("1.00"), max_position=Decimal("100.00"),
        )

    @staticmethod
    def trade(
        ticker: str, initial: int, *, winner: bool, old_stop: bool = False,
        touch: bool = False, strict: bool = False, stop45: bool = False,
    ) -> LedgerTrade:
        side = "yes"
        return LedgerTrade(
            ticker=ticker, market_open_epoch=float(len(ticker)), signal_side=side,
            outcome=side if winner else "no", initial_ask_cents=initial,
            entry_limit_cents=initial - 1, old_reached_40=old_stop,
            eventual_winner=winner, observed_limit_touch_60s=touch,
            observed_strict_trade_through_60s=strict,
            observed_stop_45_after_touch_60s=stop45,
            lowest_recorded_bid_60s_cents=45 if stop45 else initial - 1,
        )

    def test_one_cent_lower_entry_and_45c_stop_pnl(self) -> None:
        trades = [self.trade("a", 50, winner=False, old_stop=True)]
        result = replay(trades, scenario="observed_60s_quote_touch", parameters=self.parameters())
        self.assertEqual(result.fills, 1)
        self.assertEqual(result.stopped_trades, 1)
        self.assertEqual(result.pnl, Decimal("-0.04"))  # 45c exit - 49c entry

    def test_entry_at_or_below_stop_is_a_zero_fill_sizing_noop(self) -> None:
        trades = [
            self.trade("a", 46, winner=False, old_stop=True),  # derived 45c entry is rejected
            self.trade("longer", 50, winner=True, touch=True),
        ]
        result = replay(trades, scenario="observed_60s_quote_touch", parameters=self.parameters())
        self.assertEqual(result.stop_ineligible_signals, 1)
        self.assertEqual(result.fills, 1)
        self.assertEqual(result.highest_recovery_exponent, 0)
        self.assertEqual(result.maximum_quantity, Decimal("1.00"))

    def test_actual_outcome_is_fixed_and_false_stop_is_separate(self) -> None:
        trades = [self.trade("winner", 52, winner=True, touch=True, stop45=True)]
        result = replay(trades, scenario="observed_60s_quote_touch", parameters=self.parameters())
        self.assertEqual(result.filled_directional_winners, 1)
        self.assertEqual(result.stopped_eventual_winners, 1)
        self.assertEqual(result.settlement_winners, 0)
        self.assertEqual(result.pnl, Decimal("-0.06"))

    def test_observed_and_strict_fill_rules_differ(self) -> None:
        trades = [self.trade("winner", 50, winner=True, touch=True, strict=False)]
        observed = replay(trades, scenario="observed_60s_quote_touch", parameters=self.parameters())
        strict = replay(trades, scenario="strict_60s_trade_through", parameters=self.parameters())
        self.assertEqual(observed.fills, 1)
        self.assertEqual(strict.fills, 0)

    def test_empirical_calibration_and_seed_reproducibility(self) -> None:
        trades = [
            self.trade("a", 50, winner=True, touch=True, stop45=True),
            self.trade("bb", 51, winner=True, touch=False),
            self.trade("ccc", 52, winner=False, old_stop=True),
        ]
        calibration = empirical_calibration(trades)
        self.assertEqual(calibration["eligible_winner_survivors"], 2)
        self.assertEqual(calibration["winner_survivor_limit_touches_60s"], 1)
        first = replay(
            trades, scenario="empirical_late_path_mc", parameters=self.parameters(), seed=7,
            late_fill_probability=0.5, late_stop_probability=0.5,
        )
        second = replay(
            trades, scenario="empirical_late_path_mc", parameters=self.parameters(), seed=7,
            late_fill_probability=0.5, late_stop_probability=0.5,
        )
        self.assertEqual(first, second)

    def test_report_decimal_removes_interpolation_noise(self) -> None:
        self.assertEqual(report_decimal(Decimal("1.5592049999999984720484")), "1.5592")

    def test_archived_positive_pnl_and_complete_45c_cohorts_are_reconciled(self) -> None:
        result = archived_ledger_reconciliation(
            Path("data/kalshi_shadow_market_ioc_v10_sticky_stop_40_state.json"),
        )
        self.assertEqual(result["completed_fills"], 214)
        self.assertEqual(result["archived_realized_pnl"], "1.7803")
        self.assertEqual(result["eventual_directional_winners"], 105)
        self.assertEqual(result["eventual_directional_losers"], 109)

        actual = result["old_actual_entry_proxy"]
        self.assertEqual(actual["eligible"], 193)
        self.assertEqual(actual["old_stopped_eventual_winners"], 47)
        self.assertEqual(actual["old_stopped_eventual_losers"], 96)
        self.assertEqual(actual["optimistic_fixed_one_share_gross_pnl"], "11.98")
        self.assertEqual(
            actual[
                "additional_profitable_winners_with_observed_post_fill_bid_at_or_below_stop"
            ],
            10,
        )
        self.assertEqual(actual["observed_first_minimum_fixed_one_share_gross_pnl"], "6.48")

        shifted = result["old_actual_entry_minus_offset_proxy"]
        self.assertEqual(shifted["eligible"], 187)
        self.assertEqual(shifted["optimistic_fixed_one_share_gross_pnl"], "13.91")
        self.assertEqual(shifted["observed_first_minimum_fixed_one_share_gross_pnl"], "8.41")


if __name__ == "__main__":
    unittest.main()
