"""Focused tests for order-book conversion and paper trailing exits."""

from __future__ import annotations

import unittest

from kalshi_ml_scalp_paper import LiveOrderBook, PaperScalper


class DummyAuth:
    def create_auth_headers(self, method: str, path: str) -> dict[str, str]:
        return {}


class OrderBookAndPaperScalperTests(unittest.TestCase):
    def test_snapshot_converts_yes_and_no_bids_to_executable_quotes(self) -> None:
        book = LiveOrderBook(DummyAuth(), "KXBTC15M-TEST")
        book.handle(
            '{"type":"orderbook_snapshot","seq":1,"msg":{'
            '"market_ticker":"KXBTC15M-TEST",'
            '"yes_dollars_fp":[["0.45","2.00"]],'
            '"no_dollars_fp":[["0.52","3.00"]]}}'
        )
        quote = book.quote()
        self.assertEqual(quote["yes_bid"], 0.45)
        self.assertEqual(quote["yes_ask"], 0.48)
        self.assertEqual(quote["yes_ask_depth"], 3.0)
        self.assertEqual(quote["no_bid"], 0.52)
        self.assertEqual(quote["no_ask"], 0.55)
        self.assertEqual(quote["no_ask_depth"], 2.0)

    def test_trailing_exit_activates_after_probability_plus_step(self) -> None:
        scalper = PaperScalper(
            probability_yes=0.60,
            count=0.01,
            min_entry_edge=0.03,
            profit_step=0.05,
            trailing_step=0.05,
            max_round_trips=1,
        )
        entry_quote = {
            "yes_bid": 0.54,
            "yes_bid_depth": 1.0,
            "yes_ask": 0.55,
            "yes_ask_depth": 1.0,
            "no_bid": 0.44,
            "no_bid_depth": 1.0,
            "no_ask": 0.46,
            "no_ask_depth": 1.0,
        }
        self.assertEqual(scalper.update(entry_quote)[0]["kind"], "paper_entry")
        activation_quote = {**entry_quote, "yes_bid": 0.65}
        levels = scalper.update(activation_quote)
        self.assertEqual(levels[0]["kind"], "paper_trailing_level")
        self.assertEqual(levels[0]["trailing_stop"], 0.60)
        exit_quote = {**activation_quote, "yes_bid": 0.60}
        exits = scalper.update(exit_quote)
        self.assertEqual(exits[0]["kind"], "paper_exit_trailing_stop")
        self.assertEqual(exits[0]["gross_total"], 0.0005)


if __name__ == "__main__":
    unittest.main()
