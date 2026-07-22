"""Unit tests for the isolated paper-only ladder scalp simulator."""

from __future__ import annotations

import unittest

from kalshi_ladder_scalp_shadow import (
    finalize_ladder_average_entry_scalp,
    new_ladder_average_entry_scalp_shadow,
    simulate_ladder_average_entry_scalp,
)


def quote(price: float, depth: float, quote_id: str) -> dict:
    return {"quote_id": quote_id, "economic_price": price, "displayed_depth": depth, "quote_age_seconds": 0.1}


class LadderScalpShadowTests(unittest.TestCase):
    def new_shadow(self) -> dict:
        return new_ladder_average_entry_scalp_shadow(
            strategy="test", ticker="KXBTC15M-TEST", side="yes", quantity_per_rung=1.0,
            profit_target_per_contract=0.01, quote_max_age_seconds=3.0, market_close_time="later",
        )

    def test_exits_at_one_cent_above_average_after_two_rungs(self) -> None:
        shadow = self.new_shadow()
        # A 30c ask fills the 40c and 30c limits. Their average is exactly 35c.
        events = simulate_ladder_average_entry_scalp(
            shadow, entry_quote=quote(0.30, 2.0, "entry-1"), entry_quote_state="fresh",
            exit_quote=quote(0.35, 2.0, "exit-too-low"), exit_quote_state="fresh",
        )
        self.assertEqual(2, len(events))
        self.assertEqual("active", shadow["status"])
        self.assertEqual(0.35, shadow["entry_summary"]["average_entry_price"])
        self.assertEqual(0.36, shadow["entry_summary"]["take_profit_bid"])
        events = simulate_ladder_average_entry_scalp(
            shadow, entry_quote=None, entry_quote_state="fresh",
            exit_quote=quote(0.36, 2.0, "exit-target"), exit_quote_state="fresh",
        )
        self.assertEqual(1, len(events))
        self.assertEqual("scalp_exited", shadow["status"])
        self.assertEqual(0.02, shadow["net_profit_loss"])
        self.assertEqual("paper_canceled_after_scalp_exit", shadow["rungs"]["0.2000"]["status"])

    def test_requires_full_exit_depth(self) -> None:
        shadow = self.new_shadow()
        simulate_ladder_average_entry_scalp(
            shadow, entry_quote=quote(0.40, 1.0, "entry-1"), entry_quote_state="fresh",
            exit_quote=quote(0.41, 0.99, "thin-exit"), exit_quote_state="insufficient_depth",
        )
        self.assertEqual("active", shadow["status"])
        self.assertEqual(1.0, shadow["entry_summary"]["filled_contracts"])

    def test_open_position_settles_when_target_never_appears(self) -> None:
        shadow = self.new_shadow()
        simulate_ladder_average_entry_scalp(
            shadow, entry_quote=quote(0.40, 1.0, "entry-1"), entry_quote_state="fresh",
            exit_quote=None, exit_quote_state="missing",
        )
        self.assertTrue(finalize_ladder_average_entry_scalp(shadow, "yes"))
        self.assertEqual("finalized_settlement", shadow["status"])
        self.assertEqual(0.6, shadow["net_profit_loss"])


if __name__ == "__main__":
    unittest.main()
