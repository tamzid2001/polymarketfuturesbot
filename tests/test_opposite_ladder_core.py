import unittest
from decimal import Decimal

from opposite_ladder_core import (
    build_opposite_ladder_plan,
    opposite_side,
    take_profit_triggered,
)


class OppositeLadderCoreTests(unittest.TestCase):
    def test_no_prediction_buys_yes_with_example_prices(self):
        plan = build_opposite_ladder_plan(
            sticky_side="no", sticky_ask_cents=53, opposite_ask_cents=48,
            base_shares="1.00",
        )
        self.assertEqual(plan.trade_side, "yes")
        self.assertEqual(
            [(row.price_cents, row.quantity) for row in plan.orders],
            [(47, Decimal("1.00")), (40, Decimal("2.00")),
             (30, Decimal("4.00")), (20, Decimal("8.00")),
             (10, Decimal("16.00"))],
        )
        self.assertEqual(plan.maximum_quantity, Decimal("31.00"))
        self.assertEqual(plan.maximum_cash_required, Decimal("5.6700"))

    def test_yes_prediction_buys_no_symmetrically(self):
        plan = build_opposite_ladder_plan(
            sticky_side="yes", sticky_ask_cents=56, opposite_ask_cents=45,
            base_shares="1.00",
        )
        self.assertEqual(plan.trade_side, "no")
        self.assertEqual(plan.orders[0].price_cents, 44)

    def test_base_two_scales_every_rung_and_has_no_cap(self):
        plan = build_opposite_ladder_plan(
            sticky_side="no", sticky_ask_cents=53, opposite_ask_cents=48,
            base_shares="2.00",
        )
        self.assertEqual(
            [row.quantity for row in plan.orders],
            [Decimal("2.00"), Decimal("4.00"), Decimal("8.00"),
             Decimal("16.00"), Decimal("32.00")],
        )
        self.assertEqual(plan.maximum_quantity, Decimal("62.00"))

    def test_trigger_band_is_inclusive(self):
        for trigger in (53, 58):
            build_opposite_ladder_plan(
                sticky_side="no", sticky_ask_cents=trigger,
                opposite_ask_cents=48, base_shares="1.00",
            )
        for trigger in (52, 59):
            with self.assertRaises(ValueError):
                build_opposite_ladder_plan(
                    sticky_side="no", sticky_ask_cents=trigger,
                    opposite_ask_cents=48, base_shares="1.00",
                )

    def test_flatten_uses_opposite_side_executable_51c_bid(self):
        self.assertFalse(take_profit_triggered(50))
        self.assertTrue(take_profit_triggered(51))
        self.assertTrue(take_profit_triggered(61))

    def test_flatten_accepts_independent_sticky_side_51c_ask_boundary(self):
        self.assertFalse(take_profit_triggered(50, sticky_side_ask_cents=52))
        self.assertTrue(take_profit_triggered(50, sticky_side_ask_cents=51))
        self.assertTrue(take_profit_triggered(None, sticky_side_ask_cents=49))

    def test_side_validation(self):
        self.assertEqual(opposite_side("yes"), "no")
        self.assertEqual(opposite_side("no"), "yes")
        with self.assertRaises(ValueError):
            opposite_side("down")


if __name__ == "__main__":
    unittest.main()
