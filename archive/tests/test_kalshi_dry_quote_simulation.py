"""Behavior tests for conservative top-of-book dry ladder simulation."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import kalshibtc15minupordown as runner


NOW = datetime(2026, 7, 21, 17, 30, tzinfo=timezone.utc)
TICKER = "KXBTC15M-26JUL211330-00"


class ExecutableDryQuoteTests(unittest.TestCase):
    def setUp(self) -> None:
        runner.kalshi_quotes.clear()

    def tearDown(self) -> None:
        runner.kalshi_quotes.clear()

    @staticmethod
    def set_book(
        *,
        yes_bid: float = 0.80,
        yes_bid_size: float = 0.02,
        yes_ask: float = 0.81,
        yes_ask_size: float = 0.02,
        received_at: datetime = NOW,
        sequence: int = 1,
    ) -> None:
        runner.kalshi_quotes[TICKER] = {
            "yes_bid": yes_bid,
            "yes_bid_size": yes_bid_size,
            "yes_ask": yes_ask,
            "yes_ask_size": yes_ask_size,
            "book_received_at": received_at,
            "book_source_time": "2026-07-21T17:30:00Z",
            "book_source_ts_ms": 1784655000000,
            "book_sequence": sequence,
            # A favorable last trade must not affect the executable quote.
            "last": 0.10,
        }

    def test_no_buy_uses_yes_bid_and_records_full_top_of_book(self) -> None:
        self.set_book()
        quote, state = runner.fresh_executable_dry_quote(
            TICKER, "no", 0.01, now=NOW + timedelta(seconds=1))

        self.assertEqual(state, "executable_top_of_book")
        self.assertEqual(quote["executable_field"], "yes_bid")
        self.assertEqual(quote["economic_price"], 0.20)
        self.assertEqual(quote["displayed_depth"], 0.02)
        self.assertEqual(quote["yes_bid"], 0.80)
        self.assertEqual(quote["yes_ask"], 0.81)
        self.assertEqual(quote["quote_received_at"], NOW.isoformat())
        self.assertEqual(quote["quote_source_ts_ms"], 1784655000000)

    def test_exit_quote_uses_the_executable_bid_for_each_contract_side(self) -> None:
        self.set_book()
        yes_quote, yes_state = runner.fresh_executable_dry_exit_quote(
            TICKER, "yes", 0.01, now=NOW + timedelta(seconds=1))
        no_quote, no_state = runner.fresh_executable_dry_exit_quote(
            TICKER, "no", 0.01, now=NOW + timedelta(seconds=1))

        self.assertEqual((yes_state, yes_quote["executable_field"], yes_quote["economic_price"]),
                         ("executable_top_of_book", "yes_bid", 0.80))
        self.assertEqual((no_state, no_quote["executable_field"], no_quote["economic_price"]),
                         ("executable_top_of_book", "yes_ask", 0.19))

    def test_complete_ticker_message_records_fixed_point_top_of_book_depth(self) -> None:
        ws = runner.KalshiMarketWS(auth=None)
        ws._handle(json.dumps({
            "type": "ticker",
            "msg": {
                "market_ticker": TICKER,
                "yes_bid_dollars": "0.8000",
                "yes_ask_dollars": "0.8100",
                "yes_bid_size_fp": "0.02",
                "yes_ask_size_fp": "0.03",
                "last_price_dollars": "0.1000",
                "time": "2026-07-21T17:30:00Z",
                "ts_ms": 1784655000000,
            },
        }))

        quote = runner.get_kalshi_quote(TICKER)
        self.assertEqual(quote["yes_bid_size"], 0.02)
        self.assertEqual(quote["yes_ask_size"], 0.03)
        self.assertIsInstance(quote["book_received_at"], datetime)
        self.assertEqual(quote["book_sequence"], 1)

    def test_stale_or_shallow_book_cannot_qualify_a_dry_fill(self) -> None:
        self.set_book(received_at=NOW - timedelta(seconds=3.1))
        quote, state = runner.fresh_executable_dry_quote(TICKER, "yes", 0.01, now=NOW)
        self.assertIsNone(quote)
        self.assertEqual(state, "stale_book_quote")

        self.set_book(yes_ask_size=0.009)
        quote, state = runner.fresh_executable_dry_quote(TICKER, "yes", 0.01, now=NOW)
        self.assertIsNone(quote)
        self.assertEqual(state, "insufficient_top_of_book_depth")

    def test_one_quote_depth_is_consumed_and_preposted_limit_price_is_used(self) -> None:
        self.set_book(received_at=datetime.now(tz=timezone.utc))
        record = {
            "ticker": TICKER,
            "side": "NO",
            "result": "pending",
            "rungs": [
                {"economic_price": 0.40, "count": 0.01, "fill_count": 0.0},
                {"economic_price": 0.30, "count": 0.01, "fill_count": 0.0},
                {"economic_price": 0.20, "count": 0.01, "fill_count": 0.0},
                {"economic_price": 0.10, "count": 0.01, "fill_count": 0.0},
            ],
        }
        saves = []
        dry_run = runner.DRY_RUN
        try:
            runner.DRY_RUN = True
            with patch.object(runner, "tracker", SimpleNamespace(save=lambda: saves.append(True))):
                runner._simulate_dry_ladder_fills(record)
                self.assertEqual(
                    [rung["status"] for rung in record["rungs"][:2]],
                    ["simulated_executable_quote_hit", "simulated_executable_quote_hit"],
                )
                self.assertEqual(record["rungs"][2].get("status"), None)
                self.assertEqual(record["rungs"][0]["fill_economic_price"], 0.40)
                self.assertEqual(record["rungs"][1]["fill_economic_price"], 0.30)
                self.assertEqual(record["rungs"][0]["simulation_quote"]["yes_bid_size"], 0.02)

                # Re-polling the same quote cannot spend its depth a second time.
                runner._simulate_dry_ladder_fills(record)
                self.assertEqual(record["rungs"][2].get("status"), None)

                # A new top-of-book update provides new, separately auditable depth.
                self.set_book(sequence=2, received_at=datetime.now(tz=timezone.utc))
                runner._simulate_dry_ladder_fills(record)
                self.assertEqual(record["rungs"][2]["status"], "simulated_executable_quote_hit")
                self.assertEqual(record["rungs"][2]["fill_economic_price"], 0.20)
        finally:
            runner.DRY_RUN = dry_run

        self.assertEqual(len(saves), 2)

    def test_inverse_prophet_shadow_is_separate_paper_ladder_with_own_summary(self) -> None:
        class MemoryShadowLedger:
            def __init__(self):
                self.trades = []
                self.saves = 0

            def already_shadowed(self, ticker):
                return any(rec.get("ticker") == ticker for rec in self.trades)

            def record_open(self, rec):
                self.trades.append(rec)
                self.save()
                return True

            def find_pending(self):
                return [rec for rec in self.trades if rec.get("result") == "pending"]

            def save(self):
                self.saves += 1

            def settle(self, rec, result, pnl):
                rec["result"] = result
                rec["profit_loss"] = round(pnl, 4)
                self.save()

        class FakeRest:
            async def get_market(self, _ticker):
                return SimpleNamespace(result="no")

        self.set_book(received_at=datetime.now(tz=timezone.utc))
        primary = {
            "ticker": TICKER,
            "timestamp": NOW.isoformat(),
            "settle_et": "2020-01-01T00:00:00-05:00",
            "side": "YES",
            "decision_basis": "prophet_p50_vs_live_strike_locked_side",
            "btc_entry": 100_000.0,
            "strike": 100_100.0,
            "p50_prediction": 100_200.0,
            "forecast_horizon_minutes": 17,
            "bet_amount_shares": 0.01,
        }
        ledger = MemoryShadowLedger()
        old_dry = runner.DRY_RUN
        old_enabled = runner.INVERSE_PROPHET_SHADOW_ENABLED
        try:
            runner.DRY_RUN = True
            runner.INVERSE_PROPHET_SHADOW_ENABLED = True
            with patch.object(runner, "inverse_shadow_tracker", ledger):
                shadow = runner.create_inverse_prophet_shadow(primary)
                self.assertIsNotNone(shadow)
                assert shadow is not None
                self.assertEqual((shadow["source_prophet_side"], shadow["side"]), ("YES", "NO"))
                self.assertEqual(shadow["order_submitted"], "none — paper-only inverse shadow")
                self.assertTrue(all(rung["order_id"] is None for rung in shadow["rungs"]))

                runner._simulate_dry_ladder_fills(shadow)
                self.assertEqual(
                    [rung["status"] for rung in shadow["rungs"][:2]],
                    ["simulated_executable_quote_hit", "simulated_executable_quote_hit"],
                )
                self.assertTrue(asyncio.run(runner._settle_record_if_ready(FakeRest(), shadow)))
                self.assertEqual((shadow["market_result"], shadow["result"]), ("no", "WIN"))

                report = runner.inverse_shadow_performance(ledger.trades)
        finally:
            runner.DRY_RUN = old_dry
            runner.INVERSE_PROPHET_SHADOW_ENABLED = old_enabled

        self.assertEqual((report["settled_signal_markets"], report["directional_wins"]), (1, 1))
        self.assertEqual((report["filled_market_trades"], report["winning_trades"]), (1, 1))
        self.assertEqual(report["rung_performance"]["0.40"]["quote_hits"], 1)
        self.assertEqual(report["rung_performance"]["0.30"]["quote_hits"], 1)

    def test_prophet_selector_starts_inverse_and_freezes_a_separate_paper_ladder(self) -> None:
        class MemorySelectorLedger:
            def __init__(self):
                self.trades = []
                self.saves = 0

            def already_shadowed(self, ticker):
                return any(rec.get("ticker") == ticker for rec in self.trades)

            def record_open(self, rec):
                self.trades.append(rec)
                self.save()
                return True

            def find_pending(self):
                return [rec for rec in self.trades if rec.get("result") == "pending"]

            def save(self):
                self.saves += 1

            def settle(self, rec, result, pnl):
                rec["result"] = result
                rec["profit_loss"] = round(pnl, 4)
                self.save()

        class FakeRest:
            async def get_market(self, _ticker):
                return SimpleNamespace(result="no")

        self.set_book(received_at=datetime.now(tz=timezone.utc))
        primary = {
            "ticker": TICKER,
            "timestamp": NOW.isoformat(),
            "settle_et": "2020-01-01T00:00:00-05:00",
            "source_prophet_side": "YES",
            "side": "YES",
            "decision_basis": "prophet_p50_vs_live_strike_locked_side",
            "btc_entry": 100_000.0,
            "strike": 100_100.0,
            "p50_prediction": 100_200.0,
            "forecast_horizon_minutes": 17,
            "bet_amount_shares": 0.01,
        }
        ledger = MemorySelectorLedger()
        empty_history = SimpleNamespace(trades=[])
        old_dry = runner.DRY_RUN
        old_enabled = runner.PROPHET_SELECTOR_ENABLED
        try:
            runner.DRY_RUN = True
            runner.PROPHET_SELECTOR_ENABLED = True
            with patch.object(runner, "selector_tracker", ledger), \
                    patch.object(runner, "inverse_shadow_tracker", empty_history), \
                    patch.object(runner, "tracker", empty_history):
                selection = runner.prophet_selector_decision("yes")
                self.assertIsNotNone(selection)
                assert selection is not None
                self.assertEqual((selection["selected_mode"], selection["selected_side"]),
                                 ("inverse", "NO"))
                self.assertTrue(selection["bootstrap_inverse"])
                self.assertEqual(set(selection["windows"]), {"3", "5", "7", "10", "25", "50"})

                selector = runner.create_prophet_selector_shadow(primary, selection)
                self.assertIsNotNone(selector)
                assert selector is not None
                self.assertEqual((selector["source_prophet_side"], selector["side"]), ("YES", "NO"))
                self.assertEqual(selector["order_submitted"], "none — paper-only Prophet selector")
                self.assertTrue(all(rung["order_id"] is None for rung in selector["rungs"]))

                runner._simulate_dry_ladder_fills(selector)
                self.assertTrue(asyncio.run(runner._settle_record_if_ready(FakeRest(), selector)))
                self.assertEqual((selector["market_result"], selector["result"]), ("no", "WIN"))
        finally:
            runner.DRY_RUN = old_dry
            runner.PROPHET_SELECTOR_ENABLED = old_enabled

    def test_selector_uses_each_requested_window_and_majority_vote(self) -> None:
        # The newest six signals favor normal; the older six favor inverse.
        # Four of six requested windows therefore vote normal, two vote inverse.
        records = [
            {
                "ticker": f"old-{index}", "settle_et": f"2026-07-21T00:{index:02d}:00+00:00",
                "source_prophet_side": "YES", "market_result": "no",
            }
            for index in range(6)
        ] + [
            {
                "ticker": f"new-{index}", "settle_et": f"2026-07-21T01:{index:02d}:00+00:00",
                "source_prophet_side": "YES", "market_result": "yes",
            }
            for index in range(6)
        ]
        # A prior selector entry disables the deliberately inverse first-market
        # bootstrap so this test isolates the rolling-window majority rule.
        selector_history = SimpleNamespace(trades=[{"ticker": "already-started"}])
        inverse_history = SimpleNamespace(trades=records)
        with patch.object(runner, "inverse_shadow_tracker", inverse_history), \
                patch.object(runner, "selector_tracker", selector_history), \
                patch.object(runner, "tracker", SimpleNamespace(trades=[])):
            decision = runner.prophet_selector_decision("yes")

        self.assertIsNotNone(decision)
        assert decision is not None
        self.assertEqual((decision["normal_votes"], decision["inverse_votes"]), (4, 2))
        self.assertEqual((decision["selected_mode"], decision["selected_side"]), ("normal", "YES"))
        self.assertEqual(decision["windows"]["3"]["leader"], "normal")
        self.assertEqual(decision["windows"]["50"]["leader"], "inverse")

    def test_paper_pnl_time_series_separates_entry_cost_from_settlement_payout(self) -> None:
        records = [
            {
                "ticker": "cash-flow-win", "settled_at": "2026-07-21T18:00:00+00:00",
                "source_prophet_side": "YES", "side": "NO", "selector_mode": "inverse",
                "market_result": "NO", "result": "WIN",
                "rungs": [
                    {"economic_price": 0.40, "fill_count": 1.0, "fill_economic_price": 0.40},
                    {"economic_price": 0.30, "fill_count": 1.0, "fill_economic_price": 0.30},
                ],
            },
            {
                "ticker": "cash-flow-loss", "settled_at": "2026-07-21T18:15:00+00:00",
                "source_prophet_side": "NO", "side": "YES", "selector_mode": "normal",
                "market_result": "NO", "result": "LOSS",
                "rungs": [
                    {"economic_price": 0.20, "fill_count": 1.0, "fill_economic_price": 0.20},
                ],
            },
        ]

        report = runner.inverse_shadow_performance(records)
        series = report["pnl_time_series"]

        self.assertEqual((report["total_simulated_cost"], report["gross_settlement_payout"],
                          report["net_profit"]), (0.9, 2.0, 1.1))
        self.assertEqual((series[0]["entry_cost"], series[0]["settlement_payout"],
                          series[0]["net_profit"]), (0.7, 2.0, 1.3))
        self.assertEqual((series[1]["entry_cost"], series[1]["settlement_payout"],
                          series[1]["net_profit"], series[1]["cumulative_net_profit"]),
                         (0.2, 0.0, -0.2, 1.1))
        self.assertEqual((report["current_kind"], report["current_streak"],
                          report["longest_winning_streak"], report["longest_losing_streak"]),
                         ("LOSS", 1, 1, 1))


if __name__ == "__main__":
    unittest.main(verbosity=2)
