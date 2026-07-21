import unittest
from datetime import datetime, timedelta, timezone

from polymarket_mlb_average_down import (
    DEFAULT_CONFIG,
    api_price_for_outcome,
    choose_first_trigger,
    discover_games,
    executable_outcome_asks,
    lower_levels,
    observe_inverse_ml_shadow,
    order_snapshot,
    opposite_outcome,
    inverse_shadow_performance,
    reserved_capital,
    threshold_from_baseline,
    validate_config,
)


class MechanicalMlbAverageDownTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 7, 20, 14, 0, tzinfo=timezone.utc)
        self.future = (self.now + timedelta(hours=2)).isoformat().replace("+00:00", "Z")

    def test_discovers_only_full_game_moneyline_with_home_and_away(self):
        payload = {
            "events": [{
                "ticker": "KXMLBTEST", "title": "Away at Home", "markets": [
                    {
                        "slug": "away-at-home-moneyline", "active": True, "closed": False,
                        "sportsMarketType": "baseball_team_full_game_moneyline",
                        "gameStartTime": self.future, "orderPriceMinTickSize": "0.01",
                        "minimumTradeQty": "1", "marketSides": [
                            {"long": True, "team": {"name": "Away", "ordering": "away"}},
                            {"long": False, "team": {"name": "Home", "ordering": "home"}},
                        ],
                    },
                    {
                        "slug": "away-at-home-f5", "active": True, "closed": False,
                        "sportsMarketType": "baseball_team_first_five_innings_moneyline",
                        "gameStartTime": self.future, "orderPriceMinTickSize": "0.01",
                        "minimumTradeQty": "1", "marketSides": [],
                    },
                ],
            }],
        }
        games = discover_games(payload, self.now)
        self.assertEqual(len(games), 1)
        self.assertEqual(games[0].market_slug, "away-at-home-moneyline")
        self.assertEqual(games[0].outcomes["long"], {"role": "away", "team": "Away"})
        self.assertEqual(games[0].outcomes["short"], {"role": "home", "team": "Home"})

    def test_binary_bbo_converts_short_ask_and_inverts_api_price(self):
        asks = executable_outcome_asks({"bestAsk": {"value": "0.80"}, "bestBid": {"value": "0.79"}})
        self.assertEqual(asks, {"long": 0.8, "short": 0.21})
        wrapped = executable_outcome_asks({"marketData": {"bestAsk": {"value": "0.525"}, "bestBid": {"value": "0.52"}}})
        self.assertEqual(wrapped, {"long": 0.525, "short": 0.48})
        self.assertEqual(api_price_for_outcome("long", 0.70), 0.70)
        self.assertEqual(api_price_for_outcome("short", 0.10), 0.90)

    def test_short_fill_cost_is_inverted_back_from_long_api_price(self):
        snapshot = order_snapshot(
            {"quantity": 1, "cumQuantity": 1, "leavesQuantity": 0, "avgPx": {"value": "0.70"}},
            0.30, "short",
        )
        self.assertEqual(snapshot["average_outcome_cost"], 0.30)

    def test_example_80_20_baseline_uses_70_10_targets(self):
        self.assertEqual(threshold_from_baseline(0.80, 0.10, 0.01), 0.70)
        self.assertEqual(threshold_from_baseline(0.20, 0.10, 0.01), 0.10)

    def test_first_trigger_locks_the_observed_outcome(self):
        outcomes = {
            "long": {"role": "away", "team": "Away", "initial_ask": 0.20, "entry_target": 0.10},
            "short": {"role": "home", "team": "Home", "initial_ask": 0.80, "entry_target": 0.70},
        }
        self.assertEqual(
            choose_first_trigger(outcomes, {"long": 0.10, "short": 0.78}, 0.10),
            ("long", 0.10, 0.10),
        )

    def test_ml_selected_outcome_blocks_an_opposite_team_trigger(self):
        outcomes = {
            "long": {"role": "away", "team": "Away", "initial_ask": 0.30, "entry_target": 0.20},
            "short": {"role": "home", "team": "Home", "initial_ask": 0.80, "entry_target": 0.70},
        }
        # Away crosses first, but an ML choice of home means no away order is
        # permitted; when home later reaches its own discount it is the only
        # side that can start the ladder.
        self.assertIsNone(choose_first_trigger(outcomes, {"long": 0.10, "short": 0.80}, 0.10, "short"))
        self.assertEqual(choose_first_trigger(outcomes, {"long": 0.10, "short": 0.70}, 0.10, "short"), ("short", 0.70, 0.70))

    def test_lower_ladder_is_strictly_below_actual_fill(self):
        self.assertEqual(lower_levels(0.67, 0.10, 0.01, 10), [0.57, 0.47, 0.37, 0.27, 0.17, 0.07])

    def test_inverse_ml_shadow_is_paper_only_and_uses_the_opposite_teams_own_ladder(self):
        config = validate_config({**DEFAULT_CONFIG, "strategy_mode": "ml_side_average_down"})
        record = {
            "market_slug": "away-at-home-moneyline", "tick_size": 0.01, "quantity": 1,
            "ml_selected_outcome": "long",
            "outcomes": {
                "long": {"role": "away", "team": "Away", "initial_ask": 0.80, "entry_target": 0.70},
                "short": {"role": "home", "team": "Home", "initial_ask": 0.30, "entry_target": 0.20},
            },
        }
        self.assertTrue(observe_inverse_ml_shadow(record, config, True, {"long": 0.80, "short": 0.20}))
        shadow = record["ml_inverse_shadow"]
        self.assertEqual(opposite_outcome(record["ml_selected_outcome"]), "short")
        self.assertEqual(shadow["outcome"], "short")
        self.assertEqual(shadow["rungs"]["initial"]["status"], "simulated_quote_hit")
        self.assertEqual(shadow["rungs"]["initial"]["simulated_cost"], 0.20)
        self.assertNotIn("order_id", shadow["rungs"]["initial"])
        self.assertEqual(shadow["rungs"]["ladder-0.10000000"]["status"], "watching_fresh_bbo_quote")
        # The lower rung is considered only on a later BBO poll, not filled
        # retroactively by the initial trigger snapshot.
        self.assertTrue(observe_inverse_ml_shadow(record, config, True, {"long": 0.89, "short": 0.10}))
        self.assertEqual(shadow["rungs"]["ladder-0.10000000"]["status"], "simulated_quote_hit")

    def test_inverse_ml_shadow_cannot_start_during_a_live_run(self):
        config = validate_config({**DEFAULT_CONFIG, "strategy_mode": "ml_side_average_down"})
        record = {
            "market_slug": "game", "tick_size": 0.01, "quantity": 1, "ml_selected_outcome": "long",
            "outcomes": {
                "long": {"role": "away", "team": "Away", "initial_ask": 0.60, "entry_target": 0.50},
                "short": {"role": "home", "team": "Home", "initial_ask": 0.40, "entry_target": 0.30},
            },
        }
        self.assertFalse(observe_inverse_ml_shadow(record, config, False, {"long": 0.60, "short": 0.30}))
        self.assertNotIn("ml_inverse_shadow", record)

    def test_inverse_shadow_report_separates_directional_result_from_quote_hit_pnl(self):
        config = validate_config({**DEFAULT_CONFIG, "strategy_mode": "ml_side_average_down"})
        state = {"games": {
            "game": {"ml_inverse_shadow": {
                "status": "settled", "selection_result": "win",
                "simulated_quote_hit_payout": 1.0, "simulated_quote_hit_pnl_before_fees": 0.8,
                "rungs": {"initial": {"status": "simulated_quote_hit", "simulated_cost": 0.2, "quantity": 1}},
            }},
        }}
        report = inverse_shadow_performance(list(state["games"].values()), config)
        self.assertEqual(report["shadow_selections"], 1)
        self.assertEqual(report["inverse_directional_wins"], 1)
        self.assertEqual(report["inverse_directional_win_rate"], 1.0)
        self.assertEqual(report["simulated_quote_hit_rungs"], 1)
        self.assertEqual(report["order_policy"], "paper_only_no_order_submission")

    def test_filled_contracts_remain_reserved_after_game_start(self):
        state = {"games": {
            "game": {
                "status": "game_started", "game_start": "2020-01-01T00:00:00Z",
                "orders": {"initial": {
                    "outcome_cost": 0.70, "average_outcome_cost": 0.69, "quantity": 1,
                    "filled_quantity": 1, "remaining_quantity": 0, "status": "filled",
                }},
            },
        }}
        self.assertEqual(reserved_capital(state, self.now), 0.69)

    def test_defaults_are_one_contract_and_ml_is_explicitly_disabled(self):
        config = validate_config(DEFAULT_CONFIG)
        self.assertEqual(config["initial_position_size"], 1)
        self.assertEqual(config["price_step"], 0.10)
        self.assertEqual(config["strategy_mode"], "mechanical")
        self.assertEqual(config["ml_model_path"], "")
        self.assertTrue(config["ml_inverse_shadow_enabled"])


if __name__ == "__main__":
    unittest.main()
