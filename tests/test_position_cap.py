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
        # These are archived unit tests for the reusable recovery/cap
        # primitives.  Pin an explicit legacy fixture so the active v14
        # no-recovery/no-cap production config cannot change their meaning.
        self.config.update({
            "entry_execution_mode": "delayed_threshold_band_maker",
            "shadow_profile": "delayed_53_57_exit_51",
            "stop_policy": "direct_ioc_at_trigger",
            "stop_price": "0.51",
            "hybrid_stop_trigger_cents": 51,
            "hybrid_maker_exit_cents": 51,
            "hybrid_hard_stop_cents": 51,
            "recovery_multiplier": "2.50",
            "recovery_enabled": True,
            "first_base_threshold": "350.00",
            "threshold_growth_multiplier": "2.50",
            "base_increment": "0.50",
            "base_scaling_enabled": True,
            "max_position": "100.00",
            "max_position_per_base_share": "100.00",
            "position_cap_enabled": True,
        })

    def parameters(self, cap="100.00", **changes):
        values = dict(self.config, max_position=cap, max_position_per_base_share=None)
        values.update(changes)
        return strategy_parameters(values)

    def test_exact_two_point_five_sequence_clamps_to_fixed_cap(self):
        parameters = self.parameters()
        state = {}
        quantities = []
        for _ in range(9):
            quantities.append(prescribed_quantity(parameters, state)[0])
            state, _ = apply_realized_filled_trade(parameters, state, "-1.00")
        self.assertEqual(quantities, list(map(D, ["1", "2.50", "6.25", "15.63", "39.06", "97.66", "100", "100", "100"])))

    def test_fixed_cap_remains_constant_after_permanent_base_increase(self):
        parameters = self.parameters(base_increment="1.00")
        state, _ = apply_realized_filled_trade(parameters, {}, "350.00")
        self.assertEqual(D(state["base_share_count"]), D("2.00"))
        state["recovery_exponent"] = 20
        self.assertEqual(prescribed_quantity(parameters, state)[0], D("100.00"))

    def test_default_base_linked_cap_grows_only_with_permanent_base(self):
        parameters = strategy_parameters(self.config)
        self.assertEqual(parameters.effective_max_position("1.00"), D("100.00"))
        self.assertEqual(prescribed_quantity(parameters, {"recovery_exponent": 20})[0], D("100.00"))
        state, changes = apply_realized_filled_trade(parameters, {}, "350.00")
        self.assertTrue(changes["base_increased"])
        self.assertEqual(D(state["base_share_count"]), D("1.50"))
        state["recovery_exponent"] = 20
        self.assertEqual(prescribed_quantity(parameters, state)[0], D("150.00"))
        state["base_share_count"] = "2.00"
        self.assertEqual(prescribed_quantity(parameters, state)[0], D("200.00"))

    def test_pre_base_linked_runtime_state_migrates_without_losing_sizing(self):
        legacy_config = dict(self.config)
        legacy_config.pop("max_position_per_base_share", None)
        state = default_state(legacy_config)
        state["sizing"].update({
            "base_share_count": "1.50", "recovery_exponent": 3,
            "recovery_cycle_pnl": "-4.25",
        })
        state["cycle_strategy_parameters"] = strategy_parameters(
            dict(legacy_config, max_position_per_base_share=None)
        ).as_dict()
        with TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            save_state(path, state)
            restored = load_state(path, self.config)
        self.assertEqual(restored["sizing"], state["sizing"])
        self.assertEqual(restored["active_config_snapshot"]["max_position_per_base_share"], "100.00")
        self.assertIn(
            "enable_permanent_base_linked_position_cap",
            {item["kind"] for item in restored["config_migrations"]},
        )

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
        config = dict(self.config, max_position="200.25")
        updated = dict(config, max_position="125.50")
        with TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            save_config(path, updated)
            self.assertEqual(json.loads(path.read_text())["max_position"], "125.50")

    def test_flat_tuning_preserves_old_negative_cycle_until_recovered(self):
        old = self.parameters()
        config = dict(self.config, max_position="200.00")
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
        config = dict(self.config, max_position="200")
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
                config = dict(self.config, max_position="200.00")
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

    def test_historical_state_and_live_state_match_with_base_linked_cap(self):
        p = strategy_parameters(dict(
            self.config, first_base_threshold="1.00", base_increment="0.50",
            max_position="100.00", max_position_per_base_share="100.00",
        ))
        replay = RecoverySizingState(
            p.recovery_multiplier, p.first_base_threshold, p.base_increment,
            p.threshold_growth_multiplier, p.starting_base, p.max_position,
            max_position_per_base_share=p.max_position_per_base_share,
        )
        live = {}
        for event in ("1", "-1", "-1", "0.10", "-1", "3", "-1", "-1", "-1", "-1"):
            self.assertEqual(
                prescribed_quantity(p, live)[0],
                sizing_state(p, full_snapshot(replay)).prescribed_quantity(),
            )
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

    def test_optimizer_v14_export_omits_retired_live_cap_fields(self):
        self.assertEqual(ParameterSet(2.5, 350, .5, max_position=200.25).reference_configuration().max_position, D("200.25"))
        row = {"execution_profile": "opposite_ladder_53_58_flatten_51", "starting_base": "1.00"}
        with TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            export_selected_live_strategy(path, row, selection_basis="offline_test")
            exported = load_config(path)
            self.assertNotIn("max_position", exported)
            self.assertNotIn("max_position_per_base_share", exported)
            self.assertNotIn("position_cap_enabled", exported)

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

    def test_v14_workflow_exposes_no_retired_cap_input(self):
        workflow = (ROOT / ".github/workflows/kalshi_btc15m_average_down.yml").read_text()
        self.assertNotIn("max_share_cap:", workflow)
        self.assertNotIn("max_cap_per_base_share:", workflow)
        self.assertNotIn("--max-position", workflow)
        self.assertNotIn("--max-position-per-base-share", workflow)
        self.assertIn("--persist-config", workflow)


if __name__ == "__main__":
    unittest.main()
