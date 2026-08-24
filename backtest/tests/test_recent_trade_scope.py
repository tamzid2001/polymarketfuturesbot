import unittest

import pandas as pd

from backtest.data_loader import most_recent_trades


class RecentTradeScopeTest(unittest.TestCase):
    def test_keeps_newest_200_without_rebasing_shadow_equity(self):
        trades = pd.DataFrame({
            "trade_index": range(205),
            "ds": pd.date_range("2026-07-01", periods=205, freq="15min"),
            "ticker": [f"KXBTC15M-{index}" for index in range(205)],
            "trade_pnl": [1.0] * 205,
            "shadow_equity_after": [100.0 + index + 1 for index in range(205)],
        })
        result = most_recent_trades(trades, 200, 100.0)
        self.assertEqual(len(result), 200)
        self.assertEqual(result.iloc[0]["ticker"], "KXBTC15M-5")
        self.assertEqual(result.iloc[0]["trade_index"], 0)
        self.assertEqual(result.iloc[0]["shadow_equity_after"], 106.0)
        self.assertEqual(result.iloc[-1]["shadow_equity_after"], 305.0)
        self.assertEqual(result.iloc[0]["balance_before_first_trade"], 105.0)


if __name__ == "__main__":
    unittest.main()
