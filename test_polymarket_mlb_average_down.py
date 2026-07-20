import unittest
from datetime import datetime, timedelta, timezone

from polymarket_mlb_average_down import (
    DEFAULT_CONFIG,
    api_price_for_outcome,
    choose_first_trigger,
    discover_games,
    executable_outcome_asks,
    lower_levels,
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
        self.assertEqual(api_price_for_outcome("long", 0.70), 0.70)
        self.assertEqual(api_price_for_outcome("short", 0.10), 0.90)

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

    def test_lower_ladder_is_strictly_below_actual_fill(self):
        self.assertEqual(lower_levels(0.67, 0.10, 0.01, 10), [0.57, 0.47, 0.37, 0.27, 0.17, 0.07])

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

    def test_defaults_are_one_contract_and_no_ml_fields(self):
        config = validate_config(DEFAULT_CONFIG)
        self.assertEqual(config["initial_position_size"], 1)
        self.assertEqual(config["price_step"], 0.10)
        self.assertNotIn("model", config)


if __name__ == "__main__":
    unittest.main()
