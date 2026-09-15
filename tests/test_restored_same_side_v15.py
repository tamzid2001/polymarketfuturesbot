"""Forward-restoration guards; all execution is mocked, with no real orders."""
from __future__ import annotations

import asyncio
import json
import time
import unittest
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import AsyncMock

from kalshi_live_trader import (
    ACTIVE_CONFIG_SCHEMA_VERSION, ACTIVE_STRATEGY_VERSION, LiveEngine,
    RESTORED_SAME_SIDE_CONTRACT_VERSION, enforce_active_runtime_config,
    load_config, load_config_from_value, strategy_parameters,
)
from live_checkpoint import (
    DELAYED_V13_RUNTIME_STATE_REF, DELAYED_V15_RUNTIME_STATE_REF,
    OPPOSITE_LADDER_V14_RUNTIME_STATE_REF, validate_runtime_paths,
)
from live_state import default_state, load_state, save_state
from optimizer import export_selected_live_strategy
from strategy_core import apply_realized_filled_trade, prescribed_quantity
from tests.test_delayed_band_v12 import BandFeed, EntryRest


ROOT = Path(__file__).resolve().parents[1]
D = Decimal


class RestoredSameSideV15Tests(unittest.TestCase):
    def setUp(self):
        self.config = load_config(ROOT / "selected_live_strategy.json")

    def test_defaults_restore_recovery_cap_and_direct_exit_without_ladder(self):
        self.assertEqual((ACTIVE_STRATEGY_VERSION, ACTIVE_CONFIG_SCHEMA_VERSION),
                         ("kxbtc15m-delayed-band-live-v15", 15))
        self.assertEqual(RESTORED_SAME_SIDE_CONTRACT_VERSION, 1)
        self.assertFalse(self.config["opposite_ladder_enabled"])
        self.assertTrue(self.config["recovery_enabled"])
        self.assertTrue(self.config["base_scaling_enabled"])
        self.assertEqual(D(self.config["recovery_multiplier"]), D("2.50"))
        self.assertEqual(D(self.config["starting_base"]), D("1.00"))
        self.assertEqual(D(self.config["max_position_per_base_share"]), D("100.00"))
        self.assertEqual(self.config["stop_policy"], "direct_ioc_at_trigger")
        self.assertEqual(self.config["hybrid_stop_trigger_cents"], 51)

    def test_2_5_recovery_sequence_and_base_two_cap_200(self):
        parameters = strategy_parameters(self.config)
        state = {}
        expected = ["1.00", "2.50", "6.25", "15.63", "39.06", "97.66", "100.00"]
        for target in expected:
            self.assertEqual(prescribed_quantity(parameters, state)[0], D(target))
            state, _ = apply_realized_filled_trade(parameters, state, "-0.10")
        state["base_share_count"] = "2.00"
        self.assertEqual(prescribed_quantity(parameters, state)[0], D("200.00"))

    def test_archived_configs_cannot_be_silently_reinterpreted(self):
        archived = json.loads((ROOT / "tests/fixtures/opposite_ladder_v14_strategy.json").read_text())
        for config in (archived, dict(self.config, strategy_version="kxbtc15m-delayed-band-live-v13",
                                     config_schema_version=13)):
            with self.subTest(version=config["strategy_version"]), TemporaryDirectory() as directory:
                path = Path(directory) / "selected_live_strategy.json"
                text = json.dumps(config)
                path.write_text(text)
                with self.assertRaises(ValueError):
                    enforce_active_runtime_config(path)
                self.assertEqual(path.read_text(), text)

    def test_archived_recovery_and_ladder_states_are_not_imported(self):
        for version in ("kxbtc15m-delayed-band-live-v13", "kxbtc15m-opposite-ladder-live-v14"):
            with self.subTest(version=version), TemporaryDirectory() as directory:
                path = Path(directory) / "state.json"
                state = default_state(self.config)
                state["strategy_version"] = version
                state["sizing"] = {"recovery_exponent": 17, "recovery_cycle_pnl": "-64.934590"}
                save_state(path, state)
                with self.assertRaisesRegex(RuntimeError, "strategy version differs"):
                    load_state(path, self.config)

    def test_runtime_ownership_is_separate_from_v13_and_v14(self):
        self.assertNotEqual(DELAYED_V15_RUNTIME_STATE_REF, DELAYED_V13_RUNTIME_STATE_REF)
        self.assertNotEqual(DELAYED_V15_RUNTIME_STATE_REF, OPPOSITE_LADDER_V14_RUNTIME_STATE_REF)
        self.assertNotEqual(DELAYED_V13_RUNTIME_STATE_REF, OPPOSITE_LADDER_V14_RUNTIME_STATE_REF)
        validate_runtime_paths(DELAYED_V15_RUNTIME_STATE_REF, [
            "selected_live_strategy.json", "data/kalshi_live_delayed_band_v15_state.json",
        ])
        for archived_path in ("data/kalshi_live_delayed_band_v13_state.json",
                              "data/kalshi_live_opposite_ladder_v14_state.json"):
            with self.subTest(path=archived_path), self.assertRaises(ValueError):
                validate_runtime_paths(DELAYED_V15_RUNTIME_STATE_REF, [archived_path])

    def test_v15_cannot_disable_recovery_or_switch_to_opposite_ladder(self):
        for changed in ({"recovery_enabled": False}, {"base_scaling_enabled": False},
                        {"opposite_ladder_enabled": True}, {"entry_limit_offset_cents": 2},
                        {"entry_execution_mode": "opposite_side_doubling_ladder"}):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                load_config_from_value(dict(self.config, **changed))

    def test_same_side_gtc_52_to_57_order_band_in_both_modes(self):
        async def scenario():
            for dry_run in (False, True):
                for outcome, side in (("no", "yes"), ("yes", "no")):
                    for ask in range(53, 59):
                        with self.subTest(shadow=dry_run, side=side, ask=ask), TemporaryDirectory() as directory:
                            root = Path(directory)
                            engine = LiveEngine(self.config, default_state(self.config),
                                                root / "state.json", root / "audit.jsonl", dry_run=dry_run)
                            opened = time.time() - 61
                            record = engine.set_signal({"ticker": "KXBTC15M-restored", "open_epoch": opened,
                                                        "close_epoch": opened + 900},
                                                       {"ticker": "KXBTC15M-prior", "outcome": outcome})
                            rest = EntryRest()
                            await engine.submit_entry(rest, BandFeed(opened, 52, ask, side=side), record,
                                                      opened + 60.2)
                            self.assertEqual(record["signal_side"], side)
                            self.assertIsNone(record["opposite_ladder"])
                            self.assertEqual(record["entry_limit_cents"], ask - 1)
                            self.assertEqual(len(record["entry_orders"]), 1)
                            order = record["entry_orders"][0]
                            self.assertEqual(order["time_in_force"], "good_till_canceled")
                            self.assertTrue(order["post_only"])
                            self.assertEqual(D(str(order["quantity"])), D("1.00"))
                            self.assertEqual(D(str(record["actual_quantity"])), D("0"))
                            self.assertEqual(len(rest.calls), 0 if dry_run else 1)
        asyncio.run(scenario())

    def test_live_and_shadow_completed_events_match_shared_recovery_engine(self):
        async def scenario():
            snapshots = []
            for dry_run in (False, True):
                with TemporaryDirectory() as directory:
                    root = Path(directory)
                    engine = LiveEngine(self.config, default_state(self.config),
                                        root / "state.json", root / "audit.jsonl", dry_run=dry_run)
                    expected = {}
                    for index, net in enumerate(("-1.00", "0.40", "0.60")):
                        opened = time.time() - 61
                        ticker = f"KXBTC15M-realized-{index}"
                        record = engine.set_signal({"ticker": ticker, "open_epoch": opened,
                                                    "close_epoch": opened + 900},
                                                   {"ticker": ticker + "-prior", "outcome": "no"})
                        await engine.submit_entry(EntryRest(), BandFeed(opened, 52, 54), record,
                                                  opened + 60.2)
                        quantity = prescribed_quantity(strategy_parameters(self.config), expected)[0]
                        order = record["entry_orders"][0]
                        order.update(fill_count=str(quantity), remaining_count="0", average_fill_price="0.53",
                                     fees_paid="0", status="executed")
                        record["actual_quantity"] = str(quantity)
                        expected, _ = apply_realized_filled_trade(strategy_parameters(self.config), expected, net)
                        engine.record_realized(record, D(net), "stop", ticker)
                        self.assertEqual(engine.state["sizing"], expected)
                        engine.record_realized(record, D(net), "stop", ticker)
                        self.assertEqual(engine.state["sizing"], expected)  # duplicate close is a no-op
                    snapshots.append(engine.state["sizing"])
            self.assertEqual(snapshots[0], snapshots[1])
        asyncio.run(scenario())

    def test_handoff_repairs_only_a_proven_terminal_order_pointer(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            engine = LiveEngine(self.config, default_state(self.config), root / "state.json",
                                root / "audit.jsonl", dry_run=True)
            engine.markets = [{"ticker": "KXBTC15M-current", "open_epoch": 1000, "close_epoch": 1900}]
            engine.state["current_order_id"] = "confirmed-canceled"
            engine.state["markets"]["KXBTC15M-closed"] = {
                "ticker": "KXBTC15M-closed", "status": "ZERO_FILL", "exit_orders": [],
                "entry_orders": [{"order_id": "confirmed-canceled", "status": "canceled",
                                  "remaining_count": "0", "fill_count": "0"}],
            }
            self.assertTrue(engine.handoff_ready(1300)[0])
            self.assertIsNone(engine.state["current_order_id"])
            engine.state["current_order_id"] = "unknown-order"
            self.assertFalse(engine.handoff_ready(1300)[0])

    def test_workflow_pause_and_main_source_guards_are_before_real_orders(self):
        workflow = (ROOT / ".github/workflows/kalshi_btc15m_average_down.yml").read_text()
        self.assertLess(workflow.index("MAINTENANCE HOLD:"), workflow.index("kalshi_startup_order_check.py --execute"))
        self.assertIn("gh variable get KALSHI_MAINTENANCE_MODE", workflow)
        self.assertIn('"$workflow_state" != "active"', workflow)
        self.assertIn("RESTORED_SAME_SIDE_CONTRACT_VERSION == 1", workflow)
        self.assertIn("ref: main", workflow)
        self.assertNotIn("runtime-state-kxbtc15m-opposite-ladder-v14", workflow)
        watchdog = (ROOT / ".github/workflows/kalshi_btc15m_watchdog.yml").read_text()
        self.assertIn('startswith("Kalshi KXBTC15M Direct 51c v15")', watchdog)

    def test_restored_worker_heartbeat_has_recovery_cap_and_no_ladder_dependencies(self):
        class EndTick(Exception):
            pass

        async def scenario():
            for dry_run in (False, True):
                with self.subTest(shadow=dry_run), TemporaryDirectory() as directory:
                    root = Path(directory)
                    engine = LiveEngine(self.config, default_state(self.config), root / "state.json",
                                        root / "audit.jsonl", dry_run=dry_run)
                    engine.reconcile_startup = AsyncMock(return_value=True)
                    engine.discover = AsyncMock()
                    engine.reconcile_active = AsyncMock()
                    engine.refresh_live_account_status = AsyncMock(return_value={
                        "aggregate_balance": "1000.00", "exchange_index": 2,
                        "market_shard_available": "1000.00", "read_status": "PASS",
                    })
                    engine.last_analytics_log = time.monotonic()
                    feed = SimpleNamespace(update_count=0, wait_for_update=AsyncMock(side_effect=EndTick))
                    with self.assertLogs("kalshi_live_trader", level="WARNING") as captured:
                        with self.assertRaises(EndTick):
                            await engine.run(SimpleNamespace(), feed, 3600, False)
                    logs = "\n".join(captured.output)
                    self.assertIn("exponent=0", logs)
                    self.assertIn("target=1.00", logs)
                    self.assertIn("cap=100.00", logs)
                    self.assertIn("trigger=selected_bid<=51c", logs)
                    self.assertNotIn("recovery=DISABLED", logs)
        asyncio.run(scenario())

    def test_signal_log_distinguishes_permanent_base_from_recovery_quantity(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            engine = LiveEngine(self.config, default_state(self.config), root / "state.json",
                                root / "audit.jsonl", dry_run=True)
            engine.state["sizing"], _ = apply_realized_filled_trade(strategy_parameters(self.config), {}, "-0.10")
            with self.assertLogs("kalshi_live_trader", level="WARNING") as captured:
                engine.set_signal({"ticker": "KXBTC15M-base-log", "open_epoch": 1000, "close_epoch": 1900},
                                  {"ticker": "KXBTC15M-prior", "outcome": "no"})
            logs = "\n".join(captured.output)
            self.assertIn("base=1.00 target_qty=2.50 exponent=1", logs)

    def test_optimizer_export_preserves_exact_multiplier_and_base_linked_cap(self):
        row = {
            "execution_profile": "delayed_53_57_exit_51", "entry_price": "0.52", "stop_price": "0.51",
            "recovery_multiplier": "2.5125", "first_base_threshold": "350.00",
            "threshold_growth_multiplier": "2.5125", "base_increment": "0.50",
            "max_position": "100.00", "max_position_per_base_share": "100.00",
        }
        with TemporaryDirectory() as directory:
            path = Path(directory) / "selected_live_strategy.json"
            export_selected_live_strategy(path, row, selection_basis="offline_test")
            restored = load_config(path)
            self.assertEqual(restored["recovery_multiplier"], "2.5125")
            self.assertEqual(restored["threshold_growth_multiplier"], "2.5125")
            self.assertEqual(strategy_parameters(restored).effective_max_position("2.00"), D("200.00"))


if __name__ == "__main__":
    unittest.main()
