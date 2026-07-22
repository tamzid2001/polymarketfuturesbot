"""Unit tests for the isolated paper-only ladder scalp simulator."""

from __future__ import annotations

import unittest

from kalshi_ladder_scalp_shadow import (
    EXTENDED_PROFIT_TARGETS,
    finalize_ladder_average_entry_scalp,
    new_ladder_average_entry_scalp_shadow,
    scalp_performance,
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

    def test_range_observer_measures_larger_depth_supported_excursions(self) -> None:
        shadow = new_ladder_average_entry_scalp_shadow(
            strategy="test-range", ticker="KXBTC15M-TEST", side="yes", quantity_per_rung=1.0,
            profit_target_per_contract=0.01, quote_max_age_seconds=3.0, market_close_time="later",
            observation_only=True,
        )
        # Two contemporaneous entries produce a held two-contract, 35c VWAP.
        events = simulate_ladder_average_entry_scalp(
            shadow, entry_quote=quote(0.30, 2.0, "entry"), entry_quote_state="fresh",
            exit_quote=quote(0.37, 1.99, "thin-bid"), exit_quote_state="insufficient_depth",
        )
        self.assertEqual(2, len(events))
        self.assertEqual("active", shadow["status"])
        self.assertEqual({}, shadow["position_epochs"][0]["target_hits"])

        events = simulate_ladder_average_entry_scalp(
            shadow, entry_quote=None, entry_quote_state="fresh",
            exit_quote=quote(0.37, 2.0, "two-cent-bid"), exit_quote_state="fresh",
        )
        self.assertEqual(["paper_scalp_maximum_update", "paper_scalp_target_hit", "paper_scalp_target_hit"],
                         [event["kind"] for event in events])
        epoch = shadow["position_epochs"][0]
        self.assertEqual(0.02, epoch["max_executable_gross_per_contract"])
        self.assertEqual({"0.01", "0.02"}, set(epoch["target_hits"]))

        simulate_ladder_average_entry_scalp(
            shadow, entry_quote=None, entry_quote_state="fresh",
            exit_quote=quote(0.40, 2.0, "five-cent-bid"), exit_quote_state="fresh",
        )
        self.assertTrue(finalize_ladder_average_entry_scalp(shadow, "yes"))
        report = scalp_performance([shadow])
        excursion = report["excursion_observer"]
        self.assertEqual((1, 1), (excursion["completed_position_states"], excursion["depth_observed_position_states"]))
        self.assertEqual(0.05, excursion["maximum_gross_per_contract"]["median"])
        self.assertEqual((1, 1), (
            excursion["target_opportunities"]["0.05"]["hit_position_states"],
            excursion["target_opportunities"]["0.05"]["depth_observed_position_states"],
        ))
        self.assertEqual(0, excursion["target_opportunities"]["0.10"]["hit_position_states"])

    def test_weighted_ladder_uses_full_depth_and_closes_on_ten_cent_trailing_stop(self) -> None:
        shadow = new_ladder_average_entry_scalp_shadow(
            strategy="weighted-trailing", ticker="KXBTC15M-TEST", side="yes", quantity_per_rung=1.0,
            profit_target_per_contract=0.01, quote_max_age_seconds=3.0, market_close_time="later",
            profit_targets_per_contract=EXTENDED_PROFIT_TARGETS,
            rung_quantities={0.40: 1.0, 0.30: 2.0, 0.20: 3.0, 0.10: 4.0},
            trailing_stop_per_contract=0.10,
        )
        # A 20c ask can fill the 40c/30c/20c weighted rungs: 6 contracts,
        # $1.60 cost, and a 26.6667c average. The final 10c rung remains out.
        events = simulate_ladder_average_entry_scalp(
            shadow, entry_quote=quote(0.20, 6.0, "weighted-entry"), entry_quote_state="fresh",
            exit_quote=quote(0.50, 6.0, "high"), exit_quote_state="fresh",
        )
        self.assertEqual("active", shadow["status"])
        self.assertEqual((6.0, 1.6, 0.266667), (
            shadow["entry_summary"]["filled_contracts"], shadow["entry_summary"]["entry_cost"],
            shadow["entry_summary"]["average_entry_price"],
        ))
        self.assertIn("0.20", shadow["position_epochs"][0]["target_hits"])
        self.assertFalse(any(event["kind"] == "paper_scalp_trailing_stop_exit" for event in events))

        # A 10c retracement is not counted unless the whole 6-contract paper
        # position is executable at that bid.
        simulate_ladder_average_entry_scalp(
            shadow, entry_quote=None, entry_quote_state="fresh",
            exit_quote=quote(0.40, 5.99, "thin-stop"), exit_quote_state="insufficient_depth",
        )
        self.assertEqual("active", shadow["status"])
        events = simulate_ladder_average_entry_scalp(
            shadow, entry_quote=None, entry_quote_state="fresh",
            exit_quote=quote(0.40, 6.0, "stop"), exit_quote_state="fresh",
        )
        self.assertEqual("scalp_exited", shadow["status"])
        self.assertEqual("paper_scalp_trailing_stop_exit", events[-1]["kind"])
        self.assertEqual((0.50, 0.40, 0.40), (
            events[-1]["highest_executable_bid"], events[-1]["trailing_stop_bid"], events[-1]["exit_price"],
        ))
        report = scalp_performance([shadow])
        self.assertEqual((1, 0, 1), (
            report["trailing_stop_exits"], report["scalp_exits"], report["filled_market_trades"],
        ))
        self.assertEqual(0.8, report["net_profit"])
        self.assertIn("0.2667", report["average_entry_profiles"])


if __name__ == "__main__":
    unittest.main()
