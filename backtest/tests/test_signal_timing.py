import unittest

import pandas as pd

from backtest.signals import PRIMARY_RULE, apply_rule


class SignalTimingTest(unittest.TestCase):
    def test_entry_applies_to_following_trade(self):
        frame = pd.DataFrame({
            "trade_index": [0, 1, 2],
            "trade_pnl": [0.0, -10.0, 5.0],
            "shadow_equity_after": [100.0, 90.0, 95.0],
            "filtered_equity_before": [100.0, 100.0, 100.0],
            "p01": [80, 80, 80], "p10": [90, 91, 91], "p25": [95, 95, 95],
            "p50": [100, 100, 100], "p75": [105, 105, 105], "p90": [110, 110, 110], "p99": [120, 120, 120],
        })
        result = apply_rule(frame, PRIMARY_RULE, "level", "off", evaluation_start_index=1)
        self.assertFalse(result.loc[1, "bot_active_for_trade"])
        self.assertTrue(result.loc[1, "entry_signal_after_trade"])
        self.assertTrue(result.loc[2, "bot_active_for_trade"])
        self.assertEqual(result.loc[2, "selected_trade_pnl"], 5.0)


if __name__ == "__main__":
    unittest.main()
