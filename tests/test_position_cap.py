"""Offline tests for fixed-cap tuning; all exchange requests use mocks."""
from __future__ import annotations

import asyncio
import json
import os
import re
import subprocess
import sys
import time
import unittest
from decimal import Decimal, localcontext
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np

import optimizer
from execution_path_model import ExecutionCalibration, ExecutionPathModel
from historical_replay import replay_one
from kalshi_live_trader import (
    LiveEngine, apply_overrides, load_config, load_config_from_value,
    parser, save_config, strategy_parameters,
)
from live_state import default_state, load_state, save_state
from optimizer import ParameterSet, export_selected_live_strategy
from recovery_sizing import RecoverySizingState, round_shares
from strategy_core import apply_realized_filled_trade, full_snapshot, prescribed_quantity, sizing_state, zero_fill_snapshot
from tests.test_delayed_band_v12 import BandFeed, EntryRest

ROOT = Path(__file__).resolve().parents[1]
D = Decimal


class PositionCapTests(unittest.TestCase):
    def setUp(self):
        self.config = load_config(ROOT / "selected_live_strategy.json")

    def parameters(self, cap="100.00", **changes):
        return strategy_parameters(load_config_from_value(dict(self.config, max_position=cap, **changes)))

    def test_exact_two_point_five_sequence_clamps_to_fixed_cap(self):
        parameters = self.parameters()
        state = {}
        quantities = []
        for _ in range(9):
            quantities.append(prescribed_quantity(parameters, state)[0])
            state, _ = apply_realized_filled_trade(parameters, state, "-1.00")
        self.assertEqual(quantities, list(map(D, ["1", "2.50", "6.25", "15.63", "39.06", "97.66", "100", "100", "100"])))

    def test_cap_remains_constant_after_permanent_base_increase(self):
        parameters = self.parameters(base_increment="1.00")
        state, _ = apply_realized_filled_trade(parameters, {}, "350.00")
        self.assertEqual(D(state["base_share_count"]), D("2.00"))
        state["recovery_exponent"] = 20
        self.assertEqual(prescribed_quantity(parameters, state)[0], D("100.00"))

    def test_fractional_custom_cap_and_larger_cap_are_honored(self):
        for cap in ("37.55", "200.00", "1000.25"):
            self.assertEqual(prescribed_quantity(self.parameters(cap), {"recovery_exponent": 100})[0], D(cap))

    def test_zero_fill_does_not_reset_and_partial_recovery_increments(self):
        p = self.parameters()
        state, _ = apply_realized_filled_trade(p, {}, "-5")
        self.assertEqual(zero_fill_snapshot(p, state), state)
        state, change = apply_realized_filled_trade(p, state, "2")
        self.assertEqual(state["recovery_exponent"], 2)
        self.assertFalse(change["recovery_reset"])
        state, change = apply_realized_filled_trade(p, state, "3")
        self.assertTrue(change["recovery_reset"])
        self.assertEqual(prescribed_quantity(p, state)[0], D("1.00"))

    def test_large_exponents_do_not_overflow_or_falsely_cap_one_x(self):
        self.assertEqual(prescribed_quantity(self.parameters("200"), {"recovery_exponent": 100000})[0], D("200"))
        self.assertEqual(prescribed_quantity(self.parameters(recovery_multiplier="1.00"), {"recovery_exponent": 100000})[0], D("1.00"))

    def test_capped_power_matches_high_precision_reference(self):
        for multiplier in ("1.01", "1.11", "2.00", "2.50"):
            for cap in ("1.00", "37.55", "100.00", "1000.25"):
                p = self.parameters(cap, recovery_multiplier=multiplier)
                for base in ("1.00", "1.50", "2.00", "0.10"):
                    for exponent in range(45):
                        with localcontext() as ctx:
                            ctx.prec = 100
                            expected = min(round_shares(D(base) * D(multiplier) ** exponent), D(cap))
                        actual = prescribed_quantity(p, {"base_share_count": base, "recovery_exponent": exponent})[0]
                        self.assertEqual(actual, expected, (multiplier, cap, base, exponent))

    def test_invalid_cap_fails_closed(self):
        for cap in ("0", "-1", "NaN", "Infinity", "100.001", "0.99"):
            with self.subTest(cap=cap), self.assertRaises(ValueError):
                self.parameters(cap)

    def test_blank_input_and_config_roundtrip_preserve_selected_cap(self):
        config = load_config_from_value(dict(self.config, max_position="200.25"))
        args = parser().parse_args([])
        self.assertEqual(apply_overrides(config, args)["max_position"], "200.25")
        args = parser().parse_args(["--max-position", "125.50"])
        updated = apply_overrides(config, args)
        with TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            save_config(path, updated)
            self.assertEqual(load_config(path)["max_position"], "125.50")

    def test_flat_tuning_preserves_old_negative_cycle_until_recovered(self):
        old = self.parameters()
        config = load_config_from_value(dict(self.config, max_position="200.00"))
        state = default_state(self.config)
        state["sizing"], _ = apply_realized_filled_trade(old, {}, "-1")
        state["cycle_strategy_parameters"] = old.as_dict()
        with TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            save_state(path, state)
            restored = load_state(path, config)
            engine = LiveEngine(config, restored, path, path.with_suffix(".jsonl"), dry_run=True)
            self.assertEqual(engine.current_parameters().max_position, D("100.00"))
            engine.state["sizing"], _ = apply_realized_filled_trade(old, restored["sizing"], "1")
            self.assertEqual(engine.current_parameters().max_position, D("200.00"))
            engine.checkpoint("test")
            again = load_state(path, config)
            self.assertEqual(again["active_config_snapshot"]["max_position"], "200.00")

    def test_cap_change_refused_with_active_order_or_position(self):
        config = load_config_from_value(dict(self.config, max_position="200"))
        for changes in ({"current_order_id": "existing"}, {"current_position": "1.00"}):
            with TemporaryDirectory() as directory:
                path = Path(directory) / "state.json"
                state = default_state(self.config)
                state.update(changes)
                save_state(path, state)
                with self.assertRaisesRegex(RuntimeError, "configuration hash differs"):
                    load_state(path, config)
                self.assertEqual(json.loads(path.read_text())["config_hash"], state["config_hash"])

    def test_shadow_and_mocked_live_use_identical_fixed_cap(self):
        async def scenario():
            for dry_run in (True, False):
                config = load_config_from_value(dict(self.config, max_position="200.00"))
                with TemporaryDirectory() as directory:
                    path = Path(directory) / "state.json"
                    state = default_state(config)
                    state["sizing"] = {"base_share_count": "2.00", "recovery_exponent": 6}
                    engine = LiveEngine(config, state, path, path.with_suffix(".jsonl"), dry_run=dry_run)
                    opened = time.time() - 61
                    record = engine.set_signal(
                        {"ticker": "KXBTC15M-cap-test", "open_epoch": opened, "close_epoch": opened + 900},
                        {"ticker": "KXBTC15M-prior", "outcome": "no"},
                    )
                    rest = EntryRest()
                    await engine.submit_entry(rest, BandFeed(opened, 52, 53), record, opened + 60.2)
                    await engine.submit_entry(rest, BandFeed(opened, 52, 53), record, opened + 60.3)
                    self.assertEqual(record["intended_quantity"], "200.00")
                    self.assertEqual(record["effective_position_cap"], "200.00")
                    self.assertEqual(record["status"], "ENTRY_PENDING")
                    self.assertEqual(len(record["entry_orders"]), 1)
                    self.assertEqual(len(rest.calls), 0 if dry_run else 1)
                    self.assertEqual(D(str(record["entry_orders"][0]["quantity"])), D("200.00"))
        asyncio.run(scenario())

    def test_oversized_intended_order_is_blocked_before_post(self):
        async def scenario():
            with TemporaryDirectory() as directory:
                path = Path(directory) / "state.json"
                engine = LiveEngine(self.config, default_state(self.config), path, path.with_suffix(".jsonl"), dry_run=False)
                opened = time.time() - 61
                record = engine.set_signal(
                    {"ticker": "KXBTC15M-bad-cap", "open_epoch": opened, "close_epoch": opened + 900},
                    {"ticker": "KXBTC15M-prior", "outcome": "no"},
                )
                record["intended_quantity"] = "101.00"
                rest = EntryRest()
                await engine.submit_entry(rest, BandFeed(opened, 52, 53), record, opened + 60.2)
                self.assertEqual(rest.calls, [])
                self.assertTrue(engine.state["circuit_breaker"]["blocked"])
        asyncio.run(scenario())

    def test_historical_state_and_live_state_match_with_custom_cap(self):
        p = self.parameters("200.00", first_base_threshold="1.00", base_increment="1.00")
        replay = RecoverySizingState(p.recovery_multiplier, p.first_base_threshold, p.base_increment,
                                     p.threshold_growth_multiplier, p.starting_base, p.max_position)
        live = {}
        for event in ("1", "-1", "-1", "0.10", "-1", "3", "-1", "-1", "-1", "-1", "-1", "-1"):
            self.assertEqual(prescribed_quantity(p, live)[0], sizing_state(p, full_snapshot(replay)).prescribed_quantity())
            replay.apply_filled_trade(D(event))
            live, _ = apply_realized_filled_trade(p, live, event)
            self.assertEqual(live, full_snapshot(replay))

    def test_corrupt_cap_snapshot_blocks_order_instead_of_using_fallback(self):
        async def scenario():
            with TemporaryDirectory() as directory:
                path = Path(directory) / "state.json"
                engine = LiveEngine(self.config, default_state(self.config), path, path.with_suffix(".jsonl"), dry_run=False)
                opened = time.time() - 61
                record = engine.set_signal(
                    {"ticker": "KXBTC15M-corrupt-cap", "open_epoch": opened, "close_epoch": opened + 900},
                    {"ticker": "KXBTC15M-prior", "outcome": "no"},
                )
                record["config_snapshot"]["max_position"] = "NaN"
                rest = EntryRest()
                await engine.submit_entry(rest, BandFeed(opened, 52, 53), record, opened + 60.2)
                self.assertEqual(rest.calls, [])
                self.assertEqual(record["status"], "ERROR_RECONCILIATION")
        asyncio.run(scenario())

    def test_optimizer_export_preserves_cap_instead_of_hardcoding_100(self):
        self.assertEqual(ParameterSet(2.5, 350, .5, max_position=200.25).reference_configuration().max_position, D("200.25"))
        row = {"execution_profile": "delayed_53_57_stop_50", "entry_price": .52,
               "stop_price": .50, "recovery_multiplier": 2.50, "first_base_threshold": 350,
               "threshold_growth_multiplier": 2.50, "base_increment": .5, "max_position": "200.25"}
        with TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            export_selected_live_strategy(path, row, selection_basis="offline_test")
            self.assertEqual(load_config(path)["max_position"], "200.25")

    @unittest.skipIf(optimizer.njit is None, "optional Numba screening dependency")
    def test_accelerated_screen_agrees_with_decimal_replay_at_custom_cap(self):
        calibration = ExecutionCalibration(win_entry_fill_probability=1, loss_entry_fill_probability=1)
        outcomes = [False] * 12 + [True] * 4
        for cap in (37.55, 100.0, 200.25):
            parameters = ParameterSet(2.5, 10000, .5, stop_price=None, max_position=cap)
            reference = replay_one([{"directional_win": won} for won in outcomes], ExecutionPathModel(calibration), parameters.reference_configuration())
            results = optimizer.fast_results(np.asarray(outcomes), parameters, calibration, 2, 42)
            self.assertAlmostEqual(results[0, 1], float(reference.net_pnl), places=6)
            self.assertAlmostEqual(results[0, 16], float(reference.max_recovery_quantity), places=6)

    def test_workflow_input_and_startup_contract_accept_durable_custom_cap(self):
        # Do not require a YAML library in the minimal production environment.
        workflow = (ROOT / ".github/workflows/kalshi_btc15m_average_down.yml").read_text()
        cap_input = workflow.split("      max_share_cap:\n", 1)[1].split("      profit_threshold:", 1)[0]
        self.assertIn('default: ""', cap_input)
        self.assertIn('--max-position "$MAX_SHARE_CAP"', workflow)
        self.assertIn("--persist-config", workflow)
        code = next(code for code in re.findall(r"python -c '([^\n]+)'", workflow) if "CONFIGURED_FIXED_SHARE_CAP=" in code)
        with TemporaryDirectory() as directory:
            save_config(Path(directory) / "selected_live_strategy.json", dict(self.config, max_position="200.25"))
            result = subprocess.run([sys.executable, "-c", code], cwd=directory, env=dict(os.environ, PYTHONPATH=str(ROOT)), text=True, capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("CONFIGURED_FIXED_SHARE_CAP=200.25", result.stdout)


if __name__ == "__main__":
    unittest.main()
