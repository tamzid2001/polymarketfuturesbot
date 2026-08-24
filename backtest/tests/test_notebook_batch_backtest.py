"""Tests for the Colab-compatible fitted-band replay."""

from __future__ import annotations

import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd

from backtest.notebook_batch_backtest import NotebookBatchVariant, replay_notebook_bands


class NotebookBatchBacktestTests(unittest.TestCase):
    def setUp(self) -> None:
        self.curve = pd.DataFrame({
            "ds": pd.date_range("2026-07-01", periods=6, freq="15min"),
            "y": [100.0, 101.0, 99.0, 102.0, 98.0, 103.0],
        })

    @patch("backtest.notebook_batch_backtest.fit_colab_prophet")
    def test_absolute_balance_window_and_next_trade_timing(self, fit_mock) -> None:
        # The fake model returns bands that enter after market 1 and exit after
        # market 3.  Market 1 is still skipped; markets 2 and 3 are taken.
        forecast = self.curve.copy().rename(columns={"y": "actual"})
        forecast["p01"] = 1.0
        forecast["p10"] = [1.0, 102.0, 1.0, 1.0, 1.0, 1.0]
        forecast["p25"] = [1.0, 102.0, 1.0, 1.0, 1.0, 1.0]
        forecast["p50"] = [1.0, 102.0, 1.0, 1.0, 1.0, 1.0]
        forecast["p75"] = [1.0, 102.0, 1.0, 1.0, 1.0, 1.0]
        forecast["p90"] = [200.0, 200.0, 200.0, 102.0, 200.0, 200.0]
        forecast["p99"] = 300.0
        forecast["is_future"] = False
        fit_mock.return_value = forecast
        variant = NotebookBatchVariant("test", None, .05, 10.0)

        _, log, summary = replay_notebook_bands(self.curve, variant)

        self.assertEqual(log["state_before_trade"].tolist(), ["off", "on", "on", "off", "off"])
        self.assertEqual(log["bot_active_for_trade"].tolist(), [False, True, True, False, False])
        self.assertTrue(log.loc[0, "entry_signal_after_trade"])
        self.assertTrue(log.loc[2, "exit_signal_after_trade"])
        self.assertEqual(log.loc[3, "state_before_trade"], "off")
        self.assertEqual(log.loc[3, "selected_trade_pnl"], 0.0)
        self.assertAlmostEqual(float(log["always_on_balance"].iloc[-1]), 103.0)
        self.assertAlmostEqual(float(log["shadow_balance_after"].iloc[-1]), 103.0)
        self.assertAlmostEqual(summary["balance_before_first_selected_trade"], 100.0)

    @patch("backtest.notebook_batch_backtest.fit_colab_prophet")
    def test_tail_window_does_not_rebase_balance_values(self, fit_mock) -> None:
        selected = self.curve.tail(4).reset_index(drop=True)
        forecast = selected.copy().rename(columns={"y": "actual"})
        for name, value in {"p01": 1.0, "p10": 1.0, "p25": 1.0, "p50": 100.0,
                            "p75": 200.0, "p90": 200.0, "p99": 300.0}.items():
            forecast[name] = value
        forecast["is_future"] = False
        fit_mock.return_value = forecast

        _, log, summary = replay_notebook_bands(self.curve, NotebookBatchVariant("tail", 4, .05, 10.0))

        self.assertEqual(log["shadow_balance_after"].tolist(), [102.0, 98.0, 103.0])
        self.assertEqual(summary["balance_before_first_selected_trade"], 99.0)
        self.assertTrue(np.allclose(log["shadow_balance_after"], [102.0, 98.0, 103.0]))


if __name__ == "__main__":
    unittest.main()
