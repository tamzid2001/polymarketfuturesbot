import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

from polymarket_mlb_ml_backtest import (
    HORIZONS_HOURS,
    Paths,
    PolymarketHistory,
    chronological_splits,
    extract_sides,
    is_full_game_moneyline,
    last_candle_at_or_before,
    market_outcome_from_settlement,
    paired_accuracy_comparison,
    rolling_team_features,
    simulate_trading,
    snapshot_features,
)


class MlbBacktestTests(unittest.TestCase):
    def setUp(self):
        self.start = datetime(2026, 7, 20, 19, tzinfo=UTC)

    def market(self):
        return {
            "slug": "aec-mlb-away-home-2026-07-20",
            "sportsMarketType": "baseball_team_full_game_winner",
            "sportsMarketTypeV2": "SPORTS_MARKET_TYPE_MONEYLINE",
            "marketType": "moneyline",
            "marketSides": [
                {"long": True, "ordering": "away", "description": "Away Team", "id": "a"},
                {"long": False, "ordering": "home", "description": "Home Team", "id": "h"},
            ],
        }

    def test_full_game_filter_rejects_five_inning_and_props(self):
        self.assertTrue(is_full_game_moneyline(self.market()))
        f5 = self.market() | {"sportsMarketType": "baseball_team_first_five_innings_moneyline"}
        self.assertFalse(is_full_game_moneyline(f5))
        generic_f5 = self.market() | {"sportsMarketType": "moneyline", "slug": "aec-mlb-away-home-f5"}
        self.assertFalse(is_full_game_moneyline(generic_f5))
        prop = self.market() | {"sportsMarketType": "baseball_player_home_run", "sportsMarketTypeV2": "SPORTS_MARKET_TYPE_PROP"}
        self.assertFalse(is_full_game_moneyline(prop))

    def test_market_side_and_settlement_label_are_unambiguous(self):
        sides = extract_sides(self.market())
        self.assertIsNotNone(sides)
        home, away = sides
        self.assertEqual(home["team"], "Home Team")
        self.assertFalse(home["long"])
        self.assertEqual(away["team"], "Away Team")
        self.assertEqual(market_outcome_from_settlement(home, 0.0), 1)
        self.assertEqual(market_outcome_from_settlement(home, 1.0), 0)
        self.assertIsNone(market_outcome_from_settlement(home, 0.5))

    def test_public_event_pagination_is_exhaustive(self):
        with tempfile.TemporaryDirectory() as directory:
            history = PolymarketHistory(Paths(Path(directory)), key_id=None, secret_key=None)
            seen = []
            def fake_get(url):
                seen.append(url)
                return {"events": [{"id": str(len(seen) * 2 - 1)}, {"id": str(len(seen) * 2)}]} if len(seen) == 1 else {"events": [{"id": "3"}]}
            history.http.get = fake_get
            events = history.closed_events(refresh=True, page_size=2)
            self.assertEqual([event["id"] for event in events], ["1", "2", "3"])
            self.assertEqual(len(seen), 2)
            self.assertIn("offset=2", seen[1])

    def test_snapshot_never_uses_candle_after_cutoff(self):
        cutoff = self.start
        candles = [
            {"interval_end": (cutoff - timedelta(minutes=1)).isoformat(), "close": .55, "open": .54, "high": .56, "low": .53, "volume": 10, "notional": 5},
            {"interval_end": (cutoff + timedelta(minutes=1)).isoformat(), "close": .99, "open": .99, "high": .99, "low": .99, "volume": 100, "notional": 99},
        ]
        selected = last_candle_at_or_before(candles, cutoff)
        self.assertEqual(selected["close"], .55)
        features, error = snapshot_features(candles, cutoff, home_is_long=True)
        self.assertIsNone(error)
        self.assertEqual(features["market_implied_home"], .55)
        self.assertEqual(features["market_implied_home"] + features["market_implied_away"], 1.0)
        self.assertFalse(features["historical_book_available"])

    def test_current_report_schema_uses_scoped_bearer_and_price_scale(self):
        with tempfile.TemporaryDirectory() as directory:
            paths = Paths(Path(directory))
            history = PolymarketHistory(paths, report_jwt="test-reports-token")
            calls = []

            def fake_post(url, payload, *, headers=None):
                calls.append((url, payload, headers))
                self.assertEqual(headers["Authorization"], "Bearer test-reports-token")
                if url.endswith("/v1/report/trades/stats"):
                    self.assertEqual(payload["symbol"], "aec-mlb-away-home-2026-07-20")
                    self.assertIn("startTime", payload)
                    self.assertIn("endTime", payload)
                    self.assertIn("startTradeDate", payload)
                    self.assertIn("endTradeDate", payload)
                    self.assertNotIn("start_time", payload)
                    return {
                        "bars": [{"first": "500", "last": "550", "high": "560", "low": "490", "volume": "10", "notional": "5500"}],
                        "barStartTime": ["2026-07-19T18:00:00Z"],
                        "barEndTime": ["2026-07-19T19:00:00Z"],
                    }
                self.assertTrue(url.endswith("/v1/refdata/instruments"))
                self.assertEqual(payload["eventSeries"], "mlb")
                return {"instruments": [{"symbol": "aec-mlb-away-home-2026-07-20", "priceScale": "1000"}], "eof": True}

            history.http.post = fake_post
            candles, error = history.candles("aec-mlb-away-home-2026-07-20", self.start - timedelta(days=1), self.start)
            self.assertIsNone(error)
            self.assertEqual(len(candles), 1)
            self.assertAlmostEqual(candles[0]["close"], .55)
            self.assertAlmostEqual(candles[0]["notional"], 5.5)
            self.assertEqual(len(calls), 2)

    def test_missing_report_jwt_blocks_repeated_historical_requests(self):
        with tempfile.TemporaryDirectory() as directory:
            history = PolymarketHistory(Paths(Path(directory)))
            calls = []
            history.http.post = lambda *args, **kwargs: calls.append(args)
            first, first_error = history.candles("symbol", self.start - timedelta(days=1), self.start)
            second, second_error = history.candles("another-symbol", self.start - timedelta(days=1), self.start)
            self.assertIsNone(first)
            self.assertIsNone(second)
            self.assertEqual(first_error, "historical_reporting_jwt_unavailable:POLYMARKET_REPORT_JWT")
            self.assertEqual(second_error, first_error)
            self.assertEqual(calls, [])

    def test_rolling_features_are_shifted_before_current_game(self):
        first = {
            "game_pk": "1", "scheduled_start": "2026-07-10T18:00:00Z", "home_team": "Home", "away_team": "Away",
            "home_score": 5, "away_score": 2, "home_won": 1,
        }
        second = {
            "game_pk": "2", "scheduled_start": "2026-07-11T18:00:00Z", "home_team": "Home", "away_team": "Other",
            "home_score": 1, "away_score": 4, "home_won": 0,
        }
        features = rolling_team_features([first, second])
        self.assertIsNone(features["1"]["home_win_pct_10"])
        self.assertEqual(features["2"]["home_win_pct_10"], 1)
        self.assertEqual(features["2"]["home_run_diff_10"], 3)

    def test_chronological_holdout_never_precedes_training(self):
        rows = [{"scheduled_start": (self.start + timedelta(hours=index)).isoformat(), "home_target": index % 2,
                 "market_implied_home": .5, "horizon_hours": HORIZONS_HOURS[0]} for index in range(200)]
        folds, development, holdout = chronological_splits(rows)
        self.assertTrue(folds)
        self.assertTrue(holdout)
        self.assertLess(development[-1]["scheduled_start"], holdout[0]["scheduled_start"])
        for _name, train, test in folds:
            self.assertLess(train[-1]["scheduled_start"], test[0]["scheduled_start"])

    def test_missing_historical_ask_never_creates_a_fill(self):
        row = {"scheduled_start": self.start.isoformat(), "event_finished_at": (self.start + timedelta(hours=3)).isoformat(),
               "probability_home": .8, "home_target": 1, "home_executable_ask": None, "away_executable_ask": None}
        summary, trades = simulate_trading([row], threshold=0, fee_rate=0, slippage=0, capital_cap=100, quantity=1)
        self.assertEqual(summary["executable_trades"], 0)
        self.assertEqual(summary["unavailable_historical_ask"], 1)
        self.assertEqual(trades, [])

    def test_paired_comparison_does_not_claim_small_accuracy_change_is_significant(self):
        result = paired_accuracy_comparison([.6, .4, .6, .6], [.6, .6, .6, .4], [1, 0, 0, 0])
        self.assertEqual(result["candidate_only_correct"], 1)
        self.assertEqual(result["baseline_only_correct"], 1)
        self.assertEqual(result["interpretation"], "not_statistically_significant")

    def test_fee_and_pnl_use_executable_ask_not_midpoint(self):
        row = {"scheduled_start": self.start.isoformat(), "event_finished_at": (self.start + timedelta(hours=3)).isoformat(),
               "probability_home": .8, "home_target": 1, "home_executable_ask": .6, "away_executable_ask": .4}
        summary, trades = simulate_trading([row], threshold=.05, fee_rate=.01, slippage=.01, capital_cap=100, quantity=1)
        self.assertEqual(summary["executable_trades"], 1)
        self.assertAlmostEqual(trades[0]["entry_with_slippage"], .61)
        self.assertAlmostEqual(trades[0]["pnl"], .38)


if __name__ == "__main__":
    unittest.main()
