"""Behavior tests for conservative top-of-book dry ladder simulation."""

from __future__ import annotations

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


if __name__ == "__main__":
    unittest.main(verbosity=2)
