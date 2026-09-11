"""Offline regressions for operator inputs and early-failure checkpointing."""
import argparse
from copy import deepcopy
from decimal import Decimal
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from kalshi_live_trader import LiveEngine, apply_overrides, load_config, save_config, strategy_parameters
from live_checkpoint import DELAYED_V13_RUNTIME_STATE_REF, publish_runtime_snapshot, restore_runtime_snapshot
from live_state import default_state, load_state, save_state
from strategy_core import apply_realized_filled_trade

ROOT = Path(__file__).resolve().parents[1]


class WorkflowConfigurationTests(unittest.TestCase):
    def test_equivalent_base_text_is_valid_without_accounting_rounding(self):
        config = load_config(ROOT / "selected_live_strategy.json")
        for value in ("2.5", "2.50", "2.500"):
            updated = apply_overrides(config, argparse.Namespace(starting_base=value))
            self.assertEqual(Decimal(updated["starting_base"]), Decimal(value))

    def test_changed_inputs_and_blank_next_run_roundtrip_through_runtime_branch(self):
        config = load_config(ROOT / "selected_live_strategy.json")
        names = dict(starting_base="2.00")
        changed = apply_overrides(config, argparse.Namespace(**names))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            remote, work = root / "remote.git", root / "work"
            for command in (["git", "init", "--bare", str(remote)], ["git", "init", str(work)],
                            ["git", "-C", str(work), "remote", "add", "origin", str(remote)]):
                subprocess.run(command, capture_output=True, check=True)
            path = work / "selected_live_strategy.json"
            before = deepcopy(changed)
            save_config(path, changed)
            self.assertEqual(changed, before)  # atomic save must not mutate config/hash
            publish_runtime_snapshot((path,), "operator-inputs", root=work, runtime_ref=DELAYED_V13_RUNTIME_STATE_REF)
            sha = subprocess.check_output(["git", "ls-remote", str(remote), "refs/heads/" + DELAYED_V13_RUNTIME_STATE_REF], text=True).split()[0]
            path.unlink()  # disposable test fixture, not real runtime state
            restore_runtime_snapshot(sha, root=work, runtime_ref=DELAYED_V13_RUNTIME_STATE_REF)
            for blank in (None, ""):
                next_run = apply_overrides(load_config(path), argparse.Namespace(**dict.fromkeys(names, blank)))
                for key, value in names.items():
                    self.assertEqual(Decimal(str(next_run[key])), Decimal(str(value)), key)

    def test_v14_base_override_does_not_enable_recovery_or_caps(self):
        config = load_config(ROOT / "selected_live_strategy.json")
        updated = apply_overrides(config, argparse.Namespace(starting_base="2.00"))
        self.assertEqual(updated["starting_base"], "2.00")
        self.assertEqual(updated["recovery_multiplier"], "1.00")
        self.assertFalse(updated["recovery_enabled"])
        self.assertFalse(updated["base_scaling_enabled"])
        self.assertNotIn("max_position", updated)
        self.assertNotIn("max_position_per_base_share", updated)
        self.assertNotIn("position_cap_enabled", updated)

    def test_changed_inputs_refused_while_order_or_position_unresolved(self):
        config = load_config(ROOT / "selected_live_strategy.json")
        updated = apply_overrides(config, argparse.Namespace(starting_base="2.00"))
        for field, value in (("current_position", "0.01"), ("current_order_id", "test-order")):
            with tempfile.TemporaryDirectory() as directory:
                state = default_state(config)
                state[field] = value
                path = Path(directory) / "state.json"
                save_state(path, state)
                with self.assertRaises(RuntimeError):
                    load_state(path, updated)

    def test_workflow_tests_source_defaults_before_restoring_operator_settings(self):
        text = (ROOT / ".github/workflows/kalshi_btc15m_average_down.yml").read_text()
        self.assertLess(text.index("name: Verify shared live strategy engine"), text.index("name: Restore latest bounded runtime state"))
        self.assertLess(text.index("name: Restore latest bounded runtime state"), text.index("name: Assert canonical opposite-ladder strategy contract"))
        self.assertIn("steps.runtime_restore.outcome == 'success'", text)
        self.assertNotIn('--stop-price "$profile_stop"', text)
        self.assertIn("ref: main", text)
        self.assertIn('test "$(git rev-parse HEAD)" = "$KALSHI_SOURCE_SHA"', text)

    def test_live_startup_probe_is_gated_ordered_and_durable(self):
        workflow = (ROOT / ".github/workflows/kalshi_btc15m_average_down.yml").read_text()
        probe = "name: Verify live create and cancel path on the market shard"
        worker = "name: Run KXBTC15M opposite-ladder worker"
        self.assertLess(workflow.index(probe), workflow.index(worker))
        probe_block = workflow[workflow.index(probe):workflow.index(worker)]
        self.assertIn("!inputs.reconcile_only", probe_block)
        self.assertIn("inputs.live_enabled", probe_block)
        self.assertIn("vars.KALSHI_LIVE_ENABLED == 'true'", probe_block)
        self.assertIn("vars.KALSHI_SHADOW_ONLY == 'false'", probe_block)
        self.assertIn("kalshi_startup_order_check.py --execute", probe_block)
        worker_block = workflow[workflow.index(worker):]
        self.assertIn('KALSHI_STARTUP_ORDER_CHECK_ENABLED: "true"', worker_block)
        journal = "data/.kalshi_live_opposite_ladder_v14_startup_order_check/order-smoke-test.json"
        self.assertGreaterEqual(workflow.count(journal), 2)

    def test_fresh_state_reset_is_explicit_live_only_and_not_forwarded(self):
        workflow = (ROOT / ".github/workflows/kalshi_btc15m_average_down.yml").read_text()
        self.assertIn("fresh_state_reset:", workflow)
        self.assertIn('FRESH_STATE_RESET: ${{ inputs.fresh_state_reset || false }}', workflow)
        self.assertIn('[ "$FRESH_STATE_RESET" = "true" ] && args+=(--reset-state)', workflow)
        self.assertIn("fresh_state_reset requires live_enabled=true and reconcile_only=false", workflow)
        handoff = workflow[workflow.index("name: Queue the next five-hour worker only after a safe handoff"):]
        self.assertNotIn("fresh_state_reset=true", handoff)

    def test_controlled_restart_distinguishes_terminal_rejection_from_unknown_order(self):
        workflow = (ROOT / ".github/workflows/kalshi_btc15m_controlled_restart.yml").read_text()
        self.assertIn("def definitively_rejected_without_exchange_order(record):", workflow)
        self.assertIn('order.get("submission_outcome") == "rejected"', workflow)
        self.assertIn("http_status in {400, 404}", workflow)
        self.assertIn("not order.get(\"order_id\")", workflow)
        self.assertIn("abs(fill_count) <= 1e-9", workflow)
        self.assertIn("abs(remaining_count) <= 1e-9", workflow)
        self.assertIn('== "maker_entry_submission_rejected"', workflow)
        self.assertNotIn('== "maker_entry_submission_unknown"\n                  and not has_order_work(record)', workflow)

    def test_production_workflow_pins_resilient_entry_delivery_contract(self):
        workflow = (ROOT / ".github/workflows/kalshi_btc15m_average_down.yml").read_text()
        self.assertIn("ENTRY_DELIVERY_CONTRACT_VERSION == 1", workflow)
        self.assertIn("OPPOSITE_LADDER_CONTRACT_VERSION == 1", workflow)
        self.assertIn("orders=ask-1@1x,40@2x,30@4x,20@8x,10@16x", workflow)

    def test_checkpoint_does_not_require_runner_git_identity_setup(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            remote, work = root / "remote.git", root / "work"
            isolated = {k: v for k, v in os.environ.items() if not k.startswith("GIT_") and k != "EMAIL"}
            isolated.update(GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM="1")
            with patch.dict(os.environ, isolated, clear=True):
                for command in (["git", "init", "--bare", str(remote)], ["git", "init", str(work)],
                                ["git", "-C", str(work), "config", "user.useConfigOnly", "true"],
                                ["git", "-C", str(work), "remote", "add", "origin", str(remote)]):
                    subprocess.run(command, capture_output=True, check=True)
                state = work / "data/kalshi_live_opposite_ladder_v14_state.json"
                state.parent.mkdir()
                state.write_text('{"test":"early_failure"}\n')
                self.assertTrue(publish_runtime_snapshot((state,), "early-failure", root=work,
                                                       runtime_ref=DELAYED_V13_RUNTIME_STATE_REF))
                author = subprocess.check_output(["git", "--git-dir", str(remote), "show", "-s",
                                                  "--format=%an <%ae>", DELAYED_V13_RUNTIME_STATE_REF], text=True).strip()
                self.assertEqual(author, "github-actions[bot] <41898282+github-actions[bot]@users.noreply.github.com>")
                self.assertNotIn("GIT_AUTHOR_NAME", os.environ)
