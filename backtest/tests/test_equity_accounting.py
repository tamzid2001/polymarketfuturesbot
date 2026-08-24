import unittest

import numpy as np
import pandas as pd

from backtest.equity import performance_summary


class EquityAccountingTest(unittest.TestCase):
    def test_inactive_opportunity_has_zero_selected_pnl(self):
        frame = pd.DataFrame({"trade_pnl": [2.0, -3.0], "selected": [2.0, 0.0], "active": [True, False]})
        summary = performance_summary(frame, "test", "selected", "active", 100.0)
        self.assertEqual(summary["net_pnl"], 2.0)
        self.assertEqual(summary["pnl_of_skipped_trades"], -3.0)
        self.assertTrue(np.isclose(summary["ending_balance"], 102.0))


if __name__ == "__main__":
    unittest.main()
