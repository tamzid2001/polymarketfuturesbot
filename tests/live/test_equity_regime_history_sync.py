"""Read-only history-sync accounting evidence tests."""

from __future__ import annotations

import unittest

from bot.equity_regime import HistoricalSynchronizer, RegimeConfig


class HistorySyncAuditTests(unittest.IsolatedAsyncioTestCase):
    async def test_exports_ownership_evidence_and_manual_settlement_without_counting_it(self) -> None:
        manual_ticker = "KXBTC15M-26JUL262100-00"
        bot_ticker = "KXBTC15M-26JUL262115-15"

        class API:
            async def get_json(self, path, params=None):
                if path == "/historical/cutoff":
                    return {"trades_created_ts": "2026-05-27T00:00:00Z"}
                if path == "/historical/fills":
                    return {"fills": []}
                if path == "/portfolio/fills":
                    return {
                        "fills": [
                            {
                                "fill_id": "manual-fill", "ticker": manual_ticker,
                                "side": "no", "action": "buy", "count_fp": "2",
                                "no_price_dollars": "0.97", "fee_cost": "0.01",
                                "created_time": "2026-07-27T01:00:00Z",
                            },
                            {
                                "fill_id": "bot-fill", "ticker": bot_ticker,
                                "side": "yes", "action": "buy", "count_fp": "3",
                                "yes_price_dollars": "0.40", "fee_cost": "0.02",
                                "client_order_id": "settlement-contrarian-test",
                                "created_time": "2026-07-27T01:01:00Z",
                            },
                        ],
                    }
                if path == "/portfolio/settlements":
                    return {
                        "settlements": [
                            {"ticker": manual_ticker, "market_result": "no", "payout_dollars": "2.00"},
                            {"ticker": bot_ticker, "market_result": "yes", "payout_dollars": "3.00"},
                        ],
                    }
                if path == "/portfolio/balance":
                    return {"balance_dollars": "100.00"}
                raise AssertionError(path)

        result = await HistoricalSynchronizer(RegimeConfig(), {}).sync(API())

        self.assertEqual([fill.fill_id for fill in result.fills], ["bot-fill"])
        self.assertEqual(result.owned_fill_audit[0]["ownership_evidence"], "client_order_id_prefix")
        self.assertEqual(result.ambiguous_fills[0]["fill_id"], "manual-fill")
        self.assertEqual(result.ambiguous_settlement_audit[0]["ticker"], manual_ticker)
        self.assertEqual(result.ambiguous_settlement_audit[0]["payout"], "2.00")
        self.assertEqual([settlement.ticker for settlement in result.settlements], [bot_ticker])


if __name__ == "__main__":
    unittest.main()
