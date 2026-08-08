from __future__ import annotations

import random
import unittest

from execution_path_model import ExecutionCalibration, ExecutionPathModel, simulate_calibration


class ExecutionPathModelTests(unittest.TestCase):
    def test_nested_path_reaches_are_monotonic(self) -> None:
        model = ExecutionPathModel(ExecutionCalibration(
            win_entry_fill_probability=1, loss_entry_fill_probability=1,
            win_reach_40_joint_probability=1, loss_reach_40_joint_probability=1,
            win_continue_30_given_40=1, win_continue_20_given_30=1, win_continue_10_given_20=1,
            loss_continue_30_given_40=1, loss_continue_20_given_30=1, loss_continue_10_given_20=1,
        ))
        for outcome in (True, False):
            path = model.sample(outcome, random.Random(3), .40)
            self.assertTrue(path.entry_filled)
            self.assertTrue(path.reached_40)
            self.assertTrue(path.reached_30)
            self.assertTrue(path.reached_20)
            self.assertTrue(path.reached_10)
            self.assertTrue(path.stop_triggered)

    def test_entry_can_fill_without_a_40c_touch(self) -> None:
        model = ExecutionPathModel(ExecutionCalibration(
            win_entry_fill_probability=1, loss_entry_fill_probability=1,
            win_reach_40_joint_probability=0, loss_reach_40_joint_probability=0,
        ))
        path = model.sample(True, random.Random(4), .40)
        self.assertTrue(path.entry_filled)
        self.assertEqual(path.deepest_adverse_price_level, .49)
        self.assertFalse(path.reached_40)
        self.assertFalse(path.stop_triggered)

    def test_seed_reproducibility(self) -> None:
        model = ExecutionPathModel()
        first = [model.sample(True, random.Random(9 + value), .30) for value in range(10)]
        second = [model.sample(True, random.Random(9 + value), .30) for value in range(10)]
        self.assertEqual(first, second)

    def test_calibration_reproduces_empirical_targets(self) -> None:
        report = simulate_calibration(ExecutionPathModel(), replications=500, seed=12)
        for metric in (
            "40_region_rate_win", "40_region_rate_loss", "40_region_directional_wr", "no_40_region_directional_wr",
            "rung_wr_40", "rung_wr_30", "rung_wr_20", "rung_wr_10",
        ):
            self.assertLess(abs(report[f"error_{metric}"]), .02, metric)

    def test_joint_40c_calibration_is_not_reused_as_49c_fill(self) -> None:
        calibration = ExecutionCalibration.base_case()
        self.assertEqual(calibration.win_entry_fill_probability, .85)
        self.assertAlmostEqual(calibration.win_reach_40_joint_probability, 139 / 457)
        self.assertAlmostEqual(
            calibration.reach_40_probability_given_entry(True),
            (139 / 457) / .85,
        )

    def test_reconstruction_compatible_scenario_is_explicit(self) -> None:
        calibration = ExecutionCalibration.reconstruction_compatible()
        self.assertEqual(calibration.win_entry_fill_probability, 1.0)
        self.assertEqual(calibration.loss_entry_fill_probability, 1.0)


if __name__ == "__main__":
    unittest.main()
