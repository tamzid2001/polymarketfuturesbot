"""Offline startup-probe tests. No credential or network access is used."""
from __future__ import annotations

import argparse
from contextlib import redirect_stdout
from copy import deepcopy
from decimal import Decimal
import io
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import kalshi_order_smoke_test as smoke
import kalshi_startup_order_check as startup
from kalshi_shard_admin import ApiError, Journal, SafetyError
from live_state import default_state, save_state
from kalshi_live_trader import load_config
from tests.test_order_smoke_test import FakeApi, stream


ROOT = Path(__file__).resolve().parents[1]


class StartupOrderCheckTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.config = root / "selected_live_strategy.json"
        self.config.write_text((ROOT / "selected_live_strategy.json").read_text())
        self.state = root / "state.json"
        save_state(self.state, default_state(load_config(self.config)))
        self.audit = root / "audit.jsonl"
        self.journal = Journal(root / "journal")
        self.journal.root.mkdir()
        self.api = FakeApi()
        self.publications = []
        self.output = io.StringIO()
        redirection = redirect_stdout(self.output)
        redirection.__enter__()
        self.addCleanup(redirection.__exit__, None, None, None)
        environment = {
            "GITHUB_ACTIONS": "true",
            "KALSHI_LIVE_ENABLED": "true",
            "KALSHI_SHADOW_ONLY": "false",
            "KALSHI_STARTUP_ORDER_CHECK_ENABLED": "true",
        }
        patcher = patch.dict(os.environ, environment, clear=False)
        patcher.start()
        self.addCleanup(patcher.stop)

    def args(self, worker_id="1234"):
        return argparse.Namespace(
            execute=True, worker_id=worker_id, config=self.config,
            state_file=self.state, audit_ledger=self.audit,
            runtime_ref="runtime-state-kxbtc15m-delayed-v12",
        )

    def publisher(self, args, journal, reason):
        self.publications.append((reason, deepcopy(journal.load(smoke.KIND))))

    async def run_check(self, worker_id="1234"):
        return await startup.run_startup_check(
            self.args(worker_id), self.api, self.journal,
            stream=stream, publisher=self.publisher,
        )

    async def test_create_cancel_and_flat_proof_precedes_strategy_permission(self):
        await self.run_check()
        operation = self.journal.load(smoke.KIND)
        self.assertEqual(operation["state"], "CANCELED_NO_FILL")
        self.assertTrue(operation["cancel_acknowledged"])
        self.assertEqual(operation["position"], "0")
        self.assertEqual(self.api.post_count, 1)
        self.assertEqual(self.publications[0][0], "startup-order-check-intent")
        self.assertEqual(self.publications[0][1]["state"], "SUBMISSION_INTENT")
        self.assertEqual(self.publications[-1][0], "startup-order-check-passed")
        self.assertIn("STARTUP_ORDER_CHECK_PASS", self.output.getvalue())

    async def test_same_worker_is_idempotent_but_next_worker_gets_one_new_probe(self):
        await self.run_check("100")
        await self.run_check("100")
        self.assertEqual(self.api.post_count, 1)
        # The exchange assigns distinct IDs. Teach the compact shared fake the
        # same behavior for the second worker while retaining terminal history.
        self.api.orders[0]["order_id"] = "old-test-order"
        original = self.api.request
        async def distinct_cancel(method, path, **kwargs):
            if method == "DELETE":
                order_id = path.rsplit("/", 1)[-1]
                order = next(row for row in self.api.orders if row["order_id"] == order_id)
                order.update(status="canceled", remaining_count_fp="0.00")
                return {"order_id": order_id, "reduced_by": "1.00"}
            return await original(method, path, **kwargs)
        self.api.request = distinct_cancel
        await self.run_check("101")
        self.assertEqual(self.api.post_count, 2)
        self.assertEqual(self.journal.load(smoke.KIND)["startup_worker_id"], "101")

    async def test_non_reconcilable_breaker_blocks_probe_without_any_api_call(self):
        value = default_state(load_config(self.config))
        value["circuit_breaker"].update(blocked=True, reason="max_daily_realized_loss")
        save_state(self.state, value)
        with self.assertRaisesRegex(SafetyError, "breaker remains active"):
            await self.run_check()
        self.assertEqual(self.api.calls, [])

    def seed_legacy_unknown(self):
        ticker = "KXBTC15M-legacy-closed"
        client_id = "11111111-1111-4111-8111-111111111111"
        value = default_state(load_config(self.config))
        value["circuit_breaker"].update(
            blocked=True, reason="maker_entry_submission_unknown", triggered_at="2026-01-01T00:00:00Z",
        )
        value["markets"][ticker] = {
            "ticker": ticker, "status": "RECONCILIATION_PENDING", "signal_side": "yes",
            "market_close_epoch": 1,
            "entry_orders": [{
                "order_id": None, "client_order_id": client_id, "status": "submit_failed",
                "submission_outcome": "unknown", "quantity": "1.00", "position_price": "0.52",
            }],
        }
        save_state(self.state, value)
        original = self.api.request

        async def request(method, path, **kwargs):
            if method == "GET" and path == "/markets/" + ticker:
                return {"market": {
                    "ticker": ticker, "market_type": "binary", "exchange_index": 2,
                    "status": "settled", "result": "yes", "open_time": 0, "close_time": 1,
                }}
            return await original(method, path, **kwargs)

        self.api.request = request
        return ticker, client_id

    async def test_legacy_unknown_breaker_clears_only_after_v2_terminal_flat_proof(self):
        ticker, _ = self.seed_legacy_unknown()
        await self.run_check("legacy-recovery")
        value = __import__("json").loads(self.state.read_text())
        self.assertFalse(value["circuit_breaker"]["blocked"])
        self.assertEqual(
            value["markets"][ticker]["entry_orders"][0]["status"],
            "reconciled_terminal_no_fill",
        )
        self.assertEqual(value["markets"][ticker]["status"], "ZERO_FILL")
        self.assertEqual(self.api.post_count, 1)
        self.assertIn("legacy-maker-entry-breaker-resolved", [item[0] for item in self.publications])

    async def test_legacy_unknown_breaker_preserved_when_exact_fill_exists(self):
        ticker, client_id = self.seed_legacy_unknown()
        self.api.fills = [{
            "fill_id": "old-fill", "order_id": "old-order", "client_order_id": client_id,
            "ticker": ticker, "count_fp": "1.00", "yes_price_dollars": "0.52",
        }]
        with self.assertRaisesRegex(SafetyError, "fill exists"):
            await self.run_check("legacy-fill")
        value = __import__("json").loads(self.state.read_text())
        self.assertTrue(value["circuit_breaker"]["blocked"])
        self.assertEqual(self.api.post_count, 0)

    async def test_authoritative_shard_balance_and_write_scope_each_block_post(self):
        cases = (("balance", "0.00"), ("scopes", ["read"]))
        for field, value in cases:
            self.api = FakeApi()
            setattr(self.api, field, value)
            with self.assertRaises(SafetyError):
                await self.run_check(field)
            self.assertEqual(self.api.post_count, 0)
        self.api = FakeApi()
        self.api.market["exchange_index"] = 0
        with self.assertRaises(SafetyError):
            await self.run_check("wrong-shard")
        self.assertEqual(self.api.post_count, 0)

    async def test_remote_intent_failure_prevents_create(self):
        def fail(*_):
            raise RuntimeError("mock checkpoint outage")
        with self.assertRaises(RuntimeError):
            await startup.run_startup_check(
                self.args(), self.api, self.journal, stream=stream, publisher=fail,
            )
        self.assertEqual(self.api.post_count, 0)
        self.assertEqual(self.journal.load(smoke.KIND)["state"], "SUBMISSION_INTENT")

    async def test_unknown_create_is_reconciled_by_id_without_second_post(self):
        self.api.post_error = ApiError(None)
        await self.run_check()
        self.assertEqual(self.api.post_count, 1)
        self.assertTrue(self.journal.load(smoke.KIND)["cancel_acknowledged"])

    async def test_fill_or_unconfirmed_cancel_never_passes(self):
        self.api.fill_on_cancel = "0.25"
        with self.assertRaisesRegex(SafetyError, "filled"):
            await self.run_check("filled")
        self.assertEqual(self.journal.load(smoke.KIND)["state"], "FILLED_REVIEW_REQUIRED")
        self.assertEqual(self.api.post_count, 1)

        # Use a new fixture because the first correctly retains real exposure.
        self.api = FakeApi()
        original = self.api.request
        async def missing_ack(method, path, **kwargs):
            result = await original(method, path, **kwargs)
            if method == "DELETE":
                return {"reduced_by": "1.00"}
            return result
        self.api.request = missing_ack
        self.journal = Journal(Path(self.temp.name) / "journal-two")
        self.journal.root.mkdir()
        with self.assertRaisesRegex(SafetyError, "acknowledgment"):
            await self.run_check("missing-ack")

    async def test_shadow_reconcile_or_disabled_gate_cannot_submit(self):
        for updates in (
            {"KALSHI_LIVE_ENABLED": "false"},
            {"KALSHI_SHADOW_ONLY": "true"},
            {"GITHUB_ACTIONS": "false"},
            {"KALSHI_STARTUP_ORDER_CHECK_ENABLED": "false"},
        ):
            self.api = FakeApi()
            with patch.dict(os.environ, updates, clear=False), self.assertRaises(SafetyError):
                await self.run_check(next(iter(updates)))
            self.assertEqual(self.api.post_count, 0)

    def test_fresh_state_can_be_initialized_but_existing_order_or_position_blocks(self):
        self.state.unlink()
        value = startup.load_strategy_safety_state(self.state, self.config)
        self.assertFalse(value["circuit_breaker"]["blocked"])
        for key, value in (("current_order_id", "known-order"), ("current_position", "0.01")):
            state = default_state(load_config(self.config))
            state[key] = value
            save_state(self.state, state)
            with self.assertRaises(SafetyError):
                startup.load_strategy_safety_state(self.state, self.config)


if __name__ == "__main__":
    unittest.main()
