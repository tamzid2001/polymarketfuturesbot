import unittest

import pandas as pd

from backtest.config import BacktestConfig
from backtest.walk_forward import integrity_checks


class NoLookAheadTest(unittest.TestCase):
    def test_rejects_training_end_equal_to_forecast_trade(self):
        log = pd.DataFrame({
            "trade_index": [0, 1],
            "ds": pd.to_datetime(["2026-07-01 00:00", "2026-07-01 00:15"]),
            "forecast_timestamp": pd.to_datetime([None, "2026-07-01 00:15"]),
            "training_start": pd.to_datetime([None, "2026-07-01 00:15"]),
            "training_end": pd.to_datetime([None, "2026-07-01 00:15"]),
            "p01": [float("nan"), 1], "p10": [float("nan"), 2], "p25": [float("nan"), 3],
            "p50": [float("nan"), 4], "p75": [float("nan"), 5], "p90": [float("nan"), 6], "p99": [float("nan"), 7],
            "state_before_trade": ["off", "off"], "state_after_trade": ["off", "off"],
            "bot_active_for_trade": [False, False], "selected_trade_pnl": [0.0, 0.0],
            "trade_pnl": [0.0, 0.0], "shadow_equity_after": [100.0, 100.0],
            "filtered_equity_after": [100.0, 100.0], "always_on_equity": [100.0, 100.0],
            "is_walk_forward_evaluation": [False, True],
        })
        with self.assertRaises(AssertionError):
            integrity_checks(log, BacktestConfig())


if __name__ == "__main__":
    unittest.main()
