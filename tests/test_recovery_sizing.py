from __future__ import annotations

import unittest
from decimal import Decimal

from recovery_sizing import RecoverySizingState


class RecoverySizingTests(unittest.TestCase):
    def state(self, increment: str = ".25") -> RecoverySizingState:
        return RecoverySizingState(Decimal("1.11"), Decimal("50"), Decimal(increment))

    def test_base_starts_at_one_and_quantities_are_two_decimal_half_up(self) -> None:
        state = self.state()
        quantities = []
        for _ in range(4):
            quantities.append(state.prescribed_quantity())
            state.apply_filled_trade(Decimal("-.01"))
        self.assertEqual(quantities, [Decimal("1.00"), Decimal("1.11"), Decimal("1.23"), Decimal("1.37")])

    def test_zero_fill_does_not_change_recovery_state(self) -> None:
        state = self.state()
        state.apply_filled_trade(Decimal("-5"))
        before = state.snapshot()
        state.apply_zero_fill()
        self.assertEqual(before, state.snapshot())

    def test_recovery_exponent_advances_after_every_negative_cycle_trade(self) -> None:
        state = self.state()
        state.apply_filled_trade(Decimal("-5"))
        self.assertEqual(state.recovery_exponent, 1)
        state.apply_filled_trade(Decimal("2"))
        self.assertEqual(state.recovery_cycle_pnl, Decimal("-3"))
        self.assertEqual(state.recovery_exponent, 2)
        state.apply_filled_trade(Decimal("3"))
        self.assertEqual(state.recovery_cycle_pnl, Decimal("0"))
        self.assertEqual(state.recovery_exponent, 0)

    def test_all_base_increments_and_geometric_thresholds(self) -> None:
        for increment, expected in ((".25", "1.25"), (".50", "1.50"), ("1.00", "2.00")):
            state = RecoverySizingState(Decimal("1.11"), Decimal("50"), Decimal(increment))
            state.apply_filled_trade(Decimal("50"))
            self.assertEqual(state.base_share_count, Decimal(expected))
            self.assertEqual(state.next_base_threshold, Decimal("55.50"))

    def test_cap_applies_after_rounding(self) -> None:
        state = RecoverySizingState(Decimal("1.11"), Decimal("50"), Decimal(".25"))
        state.recovery_exponent = 100
        self.assertLessEqual(state.prescribed_quantity(), Decimal("100.00"))
        self.assertTrue(state.last_quantity_was_capped)


if __name__ == "__main__":
    unittest.main()
