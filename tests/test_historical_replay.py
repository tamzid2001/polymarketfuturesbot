from __future__ import annotations

import unittest
from decimal import Decimal

import kalshi_btc15m_average_down as live_trader
from execution_path_model import ExecutionCalibration, ExecutionPathModel
from historical_replay import ReplayConfiguration, replay_one, trade_pnl_per_share


class HistoricalReplayTests(unittest.TestCase):
    def fixed_model(self) -> ExecutionPathModel:
        return ExecutionPathModel(ExecutionCalibration(
            win_entry_fill_probability=1, loss_entry_fill_probability=1,
            win_reach_40_joint_probability=0, loss_reach_40_joint_probability=0,
        ))

    def test_actual_outcomes_are_not_redrawn_and_no_loss_skip_exists(self) -> None:
        signals = [{"ticker": "a", "directional_win": True}, {"ticker": "b", "directional_win": False}]
        result = replay_one(signals, self.fixed_model(), ReplayConfiguration(stop_price=Decimal(".40")), seed=2)
        self.assertEqual((result.filled_wins, result.filled_losses), (1, 1))
        config = live_trader.validate_config({"live_consecutive_loss_limit": 2, "live_markets_to_skip_after_loss_limit": 2})
        self.assertEqual(config["live_consecutive_loss_limit"], 0)
        self.assertEqual(config["live_markets_to_skip_after_loss_limit"], 0)

    def test_zero_fill_is_zero_pnl_and_no_recovery_progression(self) -> None:
        model = ExecutionPathModel(ExecutionCalibration(
            win_entry_fill_probability=0, loss_entry_fill_probability=0,
            win_reach_40_joint_probability=0, loss_reach_40_joint_probability=0,
        ))
        result = replay_one([{"directional_win": False}], model, ReplayConfiguration(), seed=1)
        self.assertEqual(result.net_pnl, Decimal("0"))
        self.assertEqual(result.filled_trades, 0)
        self.assertEqual(result.zero_fills, 1)

    def test_per_share_pnl_conventions(self) -> None:
        model = self.fixed_model()
        win_path = model.sample(True, __import__("random").Random(3), None)
        loss_path = model.sample(False, __import__("random").Random(3), None)
        config = ReplayConfiguration()
        self.assertEqual(trade_pnl_per_share(True, win_path, config)[0], Decimal(".51"))
        self.assertEqual(trade_pnl_per_share(False, loss_path, config)[0], Decimal("-.49"))
        stopped = ExecutionPathModel(ExecutionCalibration(win_entry_fill_probability=1, loss_entry_fill_probability=1, win_reach_40_joint_probability=1, loss_reach_40_joint_probability=1)).sample(True, __import__("random").Random(3), .40)
        self.assertEqual(trade_pnl_per_share(True, stopped, config)[0], Decimal("-.09"))

    def test_shared_stop_policy_keeps_49c_at_40c_and_refuses_unobserved_upper_touches(self) -> None:
        self.assertEqual(ReplayConfiguration(entry_price=Decimal(".49")).effective_stop_price(), Decimal(".40"))
        self.assertEqual(ReplayConfiguration(entry_price=Decimal(".50")).effective_stop_price(), Decimal(".40"))
        with self.assertRaisesRegex(ValueError, "41c-49c path calibration"):
            ReplayConfiguration(entry_price=Decimal(".51"))

    def test_bankroll_is_checked_before_entry(self) -> None:
        result = replay_one(
            [{"ticker": "funding", "directional_win": False}], self.fixed_model(),
            ReplayConfiguration(starting_bankroll=Decimal(".10")), seed=2,
        )
        self.assertIsNotNone(result.funding_failure)
        self.assertEqual(result.funding_failure.required_cash, Decimal(".49"))


if __name__ == "__main__":
    unittest.main()
