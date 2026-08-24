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
        self.assertEqual((1, 0.8), (
            report["average_entry_profiles"]["0.2667"]["trailing_stop_exits"],
            report["average_entry_profiles"]["0.2667"]["net_profit"],
        ))
        self.assertEqual(("win", 1, 1, 0), (
            report["current_streak_kind"], report["current_streak"],
            report["longest_winning_streak"], report["longest_losing_streak"],
        ))
        point = report["pnl_time_series"][0]
        self.assertEqual(("KXBTC15M-TEST", "win", 6.0, 0.8), (
            point["ticker"], point["trade_outcome"], point["filled_contracts"], point["net_profit"],
        ))
        self.assertEqual((0.5, 0.4, 0.4), (
            point["trailing_stop"]["highest_executable_bid"],
            point["trailing_stop"]["trailing_stop_bid"],
            point["trailing_stop"]["observed_exit_bid"],
        ))

    def test_weighted_ladder_fixed_five_cent_stop_requires_full_depth(self) -> None:
        shadow = new_ladder_average_entry_scalp_shadow(
            strategy="weighted-fixed-stop", ticker="KXBTC15M-TEST", side="yes", quantity_per_rung=1.0,
            profit_target_per_contract=0.01, quote_max_age_seconds=3.0, market_close_time="later",
            profit_targets_per_contract=EXTENDED_PROFIT_TARGETS,
            rung_quantities={0.40: 1.0, 0.30: 2.0, 0.20: 3.0, 0.10: 4.0},
            fixed_stop_loss_per_contract=0.05,
        )
        # Three weighted rungs fill for a 26.6667c VWAP.  The fixed stop is
        # therefore 21.6667c and must use a fresh bid with all six contracts
        # of displayed depth; it never assumes a fill at the stop price.
        simulate_ladder_average_entry_scalp(
            shadow, entry_quote=quote(0.20, 6.0, "weighted-entry"), entry_quote_state="fresh",
            exit_quote=quote(0.21, 5.99, "thin-stop"), exit_quote_state="insufficient_depth",
        )
        self.assertEqual("active", shadow["status"])
        events = simulate_ladder_average_entry_scalp(
            shadow, entry_quote=None, entry_quote_state="fresh",
            exit_quote=quote(0.21, 6.0, "full-stop"), exit_quote_state="fresh",
        )
        self.assertEqual("scalp_exited", shadow["status"])
        self.assertEqual("paper_scalp_fixed_stop_loss_exit", events[-1]["kind"])
        self.assertEqual(0.216667, shadow["position_epochs"][0]["fixed_stop_loss_bid"])
        self.assertEqual((0.216667, 0.21, -0.34), (
            events[-1]["fixed_stop_loss_bid"], events[-1]["exit_price"], events[-1]["gross_profit_loss"],
        ))
        report = scalp_performance([shadow])
        self.assertEqual((1, 0, -0.34), (
            report["fixed_stop_loss_exits"], report["trailing_stop_exits"], report["net_profit"],
        ))
        self.assertEqual((1, -0.34), (
            report["average_entry_profiles"]["0.2667"]["fixed_stop_loss_exits"],
            report["average_entry_profiles"]["0.2667"]["net_profit"],
        ))
        point = report["pnl_time_series"][0]
        self.assertEqual((0.216667, 0.21), (
            point["fixed_stop_loss"]["fixed_stop_loss_bid"], point["fixed_stop_loss"]["observed_exit_bid"],
        ))

    def test_weighted_bracket_arms_ten_cent_trailing_stop_only_after_ten_cent_gain(self) -> None:
        shadow = new_ladder_average_entry_scalp_shadow(
            strategy="weighted-bracket", ticker="KXBTC15M-TEST", side="yes", quantity_per_rung=1.0,
            profit_target_per_contract=0.01, quote_max_age_seconds=3.0, market_close_time="later",
            profit_targets_per_contract=EXTENDED_PROFIT_TARGETS,
            rung_quantities={0.40: 1.0, 0.30: 2.0, 0.20: 3.0, 0.10: 4.0},
            absolute_stop_price=0.05,
            trailing_stop_per_contract=0.10,
            trailing_activation_gain_per_contract=0.10,
        )
        simulate_ladder_average_entry_scalp(
            shadow, entry_quote=quote(0.20, 6.0, "entry"), entry_quote_state="fresh",
            exit_quote=quote(0.36, 6.0, "not-armed"), exit_quote_state="fresh",
        )
        self.assertEqual("active", shadow["status"])
        self.assertNotIn("trailing_armed_at", shadow["position_epochs"][0])
        simulate_ladder_average_entry_scalp(
            shadow, entry_quote=None, entry_quote_state="fresh",
            exit_quote=quote(0.37, 6.0, "armed"), exit_quote_state="fresh",
        )
        epoch = shadow["position_epochs"][0]
        self.assertEqual((0.366667, 0.37), (epoch["trailing_activation_bid"], epoch["trailing_armed_high_bid"]))
        events = simulate_ladder_average_entry_scalp(
            shadow, entry_quote=None, entry_quote_state="fresh",
            exit_quote=quote(0.27, 6.0, "trailing-exit"), exit_quote_state="fresh",
        )
        self.assertEqual(("scalp_exited", "paper_scalp_trailing_stop_exit", 0.27), (
            shadow["status"], events[-1]["kind"], events[-1]["exit_price"],
        ))
        report = scalp_performance([shadow])
        self.assertEqual((1, [0.1]), (
            report["trailing_stop_exits"], report["trailing_stop"]["configured_activation_gains_per_contract"],
        ))
        self.assertEqual([0.05], report["fixed_stop_loss"]["configured_absolute_prices"])

    def test_absolute_five_cent_stop_is_independent_of_average_and_armed_trail_survives_averaging(self) -> None:
        shadow = new_ladder_average_entry_scalp_shadow(
            strategy="absolute-stop-market-wide-trail", ticker="KXBTC15M-TEST", side="yes", quantity_per_rung=1.0,
            profit_target_per_contract=0.01, quote_max_age_seconds=3.0, market_close_time="later",
            profit_targets_per_contract=EXTENDED_PROFIT_TARGETS,
            rung_quantities={0.40: 1.0, 0.30: 2.0, 0.20: 3.0, 0.10: 4.0},
            absolute_stop_price=0.05, trailing_stop_per_contract=0.10,
            trailing_activation_gain_per_contract=0.10,
        )
        # First two rungs average 33.3333c. A 44c bid arms the trail.
        simulate_ladder_average_entry_scalp(
            shadow, entry_quote=quote(0.30, 3.0, "entry-high"), entry_quote_state="fresh",
            exit_quote=quote(0.44, 3.0, "arm"), exit_quote_state="fresh",
        )
        self.assertTrue(shadow.get("market_trailing_armed_at"))
        self.assertEqual(0.44, shadow["market_trailing_high_bid"])
        # Averaging into the 20c rung changes the average to 26.6667c, but
        # it must not de-arm the market-wide trail. The 25c full-depth bid
        # exits below the retained 34c trailing price, not via the 5c stop.
        events = simulate_ladder_average_entry_scalp(
            shadow, entry_quote=quote(0.20, 3.0, "average-down"), entry_quote_state="fresh",
            exit_quote=quote(0.25, 6.0, "armed-retrace"), exit_quote_state="fresh",
        )
        self.assertEqual(("scalp_exited", "paper_scalp_trailing_stop_exit", 0.25), (
            shadow["status"], events[-1]["kind"], events[-1]["exit_price"],
        ))
        self.assertEqual(0.34, events[-1]["trailing_stop_bid"])

    def test_later_average_down_records_fresh_targets_without_dearming_trail(self) -> None:
        shadow = new_ladder_average_entry_scalp_shadow(
            strategy="new-average-targets", ticker="KXBTC15M-TEST", side="yes", quantity_per_rung=1.0,
            profit_target_per_contract=0.01, quote_max_age_seconds=3.0, market_close_time="later",
            profit_targets_per_contract=EXTENDED_PROFIT_TARGETS,
            rung_quantities={0.40: 1.0, 0.30: 2.0, 0.20: 3.0, 0.10: 4.0},
            absolute_stop_price=0.05, trailing_stop_per_contract=0.10,
            trailing_activation_gain_per_contract=0.01,
        )
        # The 40c position hits its 1c activation and arms the market trail.
        simulate_ladder_average_entry_scalp(
            shadow, entry_quote=quote(0.40, 1.0, "entry-40"), entry_quote_state="fresh",
            exit_quote=quote(0.42, 1.0, "first-target"), exit_quote_state="fresh",
        )
        first_arm = shadow["market_trailing_armed_at"]
        self.assertEqual(0.32, shadow["position_epochs"][0]["trailing_stop_bid"])

        # Two 30c fills lower the average to 33.3333c. The old 32c market
        # trail remains, but this new 3-contract epoch gets its own 34.3333c
        # target and records the later fresh quote that reaches it.
        simulate_ladder_average_entry_scalp(
            shadow, entry_quote=quote(0.30, 2.0, "entry-30"), entry_quote_state="fresh",
            exit_quote=quote(0.34, 3.0, "new-average-not-yet"), exit_quote_state="fresh",
        )
        self.assertEqual("active", shadow["status"])
        self.assertEqual(2, len(shadow["position_epochs"]))
        new_epoch = shadow["position_epochs"][-1]
        self.assertEqual(0.343333, new_epoch["trailing_activation_bid"])
        self.assertNotIn("trailing_activation_hit_at", new_epoch)

        events = simulate_ladder_average_entry_scalp(
            shadow, entry_quote=None, entry_quote_state="fresh",
            exit_quote=quote(0.35, 3.0, "new-average-target"), exit_quote_state="fresh",
        )
        self.assertEqual("active", shadow["status"])
        self.assertEqual(first_arm, shadow["market_trailing_armed_at"])
        self.assertEqual(0.343333, new_epoch["trailing_activation_bid"])
        self.assertEqual(0.35, new_epoch["trailing_activation_hit_bid"])
        self.assertTrue(new_epoch["trailing_activation_market_previously_armed"])
        self.assertTrue(any(event["kind"] == "paper_scalp_trailing_activation_target_hit" for event in events))


if __name__ == "__main__":
    unittest.main()
