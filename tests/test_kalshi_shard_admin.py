"""Pure/mocked admin tests: no account credentials, transfers or live orders."""
from __future__ import annotations

from contextlib import redirect_stdout
from copy import deepcopy
from decimal import Decimal
import io
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, Mock, patch

import kalshi_shard_admin as admin


class FakeApi:
    key_id = "mock-key"

    def __init__(self):
        self.balances = {0: "120.4724", 2: "0.0000"}
        self.allocations = []
        self.scopes = ["read", "write"]
        self.subaccount = None
        self.resting = []
        self.positions = []
        self.transfers = []
        self.calls = []
        self.post_error = None
        self.transfer_status = "complete"
        self.transfer_payload = None
        self.market = {"ticker": "KXBTC15M-test", "exchange_index": 2, "status": "active",
                       "open_time": time.time() - 60, "close_time": time.time() + 840}

    async def request(self, method, path, *, params=None, body=None):
        self.calls.append((method, path, deepcopy(params), deepcopy(body)))
        if method == "GET":
            if path == "/markets": return {"markets": [self.market]}
            if path.startswith("/markets/"): return {"market": self.market}
            if path == "/api_keys":
                return {"api_keys": [{"api_key_id": self.key_id, "scopes": self.scopes, "subaccount": self.subaccount}]}
            if path == "/portfolio/balance":
                return {"balance_dollars": self.balances[params["exchange_index"]]} if params else {
                    "balance_dollars": str(sum(Decimal(v) for v in self.balances.values())),
                    "balance_breakdown": [{"exchange_index": k, "balance": v} for k, v in self.balances.items()]}
            if path == "/portfolio/orders": return {"orders": self.resting}
            if path == "/portfolio/positions": return {"market_positions": self.positions}
            if path == admin.ALLOCATION: return {"allocations": self.allocations}
            if path == admin.TRANSFERS: return {"transfers": self.transfers}
            if path.startswith(admin.TRANSFERS + "/"): return {"transfer": self.transfer_payload}
        if method == "POST" and path == admin.TRANSFER:
            if self.post_error: raise self.post_error
            dollars = Decimal(body["amount"]) / 10000
            self.transfer_payload = {**body, "amount": str(dollars), "transfer_id": "mock-transfer",
                                     "status": self.transfer_status, "created_ts": time.time()}
            if self.transfer_status == "complete":
                self.balances[0] = str(Decimal(self.balances[0]) - dollars)
                self.balances[2] = str(Decimal(self.balances[2]) + dollars)
            return {"transfer_id": "mock-transfer"}
        if method == "POST" and path == admin.ALLOCATION:
            if self.post_error: raise self.post_error
            self.allocations = body["allocations"]
            return {}
        raise AssertionError("Unexpected mock endpoint")

    def posts(self):
        return [c for c in self.calls if c[0] == "POST"]


class ShardAdminTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.journal = admin.Journal(self.root)
        self.api = FakeApi()
        self.stdout = io.StringIO()
        self.output = redirect_stdout(self.stdout)
        self.output.__enter__()
        self.addCleanup(self.output.__exit__, None, None, None)

    @staticmethod
    def approve(prompt):
        return prompt.split("Type exactly to authorize: ", 1)[1].split("\n", 1)[0]

    async def transfer(self, **kwargs):
        return await admin.transfer_all(self.api, self.journal, execute=True, workers_paused=True,
                                         answer=self.approve, timeout=0, **kwargs)

    def test_exact_centicents_no_float_or_rounding(self):
        self.assertEqual(admin.centicents("120.4724"), 1204724)
        self.assertEqual(admin.centicents("100.00"), 1000000)
        self.assertEqual(admin.centicents("0.0001"), 1)
        for value in ("0.00001", "-1", "NaN", "Infinity", "bad", str(2**63)):
            with self.assertRaises(admin.SafetyError): admin.centicents(value)

    def test_epoch_seconds_millis_and_iso_match(self):
        self.assertEqual(admin.epoch("2026-09-08T00:00:00Z"), admin.epoch("1788825600000"))
        self.assertEqual(admin.epoch("1788825600"), admin.epoch(1788825600000))

    async def test_default_command_is_read_only_without_creating_journal(self):
        args = admin.parser().parse_args([])
        absent = self.root / "absent"
        await admin.run(args, self.api, root=absent)
        self.assertEqual(self.api.posts(), [])
        self.assertFalse(absent.exists())
        self.assertIn('"api_key_write": "PASS"', self.stdout.getvalue())
        self.assertNotIn(self.api.key_id, self.stdout.getvalue())

    async def test_transfer_preview_does_not_post_or_persist_intent(self):
        plan = await admin.transfer_all(self.api, self.journal)
        self.assertEqual(plan["amount"], 1204724)
        self.assertEqual(plan["destination_exchange_shard"], 2)
        self.assertEqual(self.api.posts(), [])
        self.assertIsNone(self.journal.load("transfer"))

    async def test_complete_transfer_exact_body_then_verify_balances(self):
        result = await self.transfer()
        self.assertEqual(result["status"], "COMPLETED")
        self.assertTrue(result["balance_delta_matches_plan"])
        self.assertEqual(len(self.api.posts()), 1)
        self.assertEqual(self.api.posts()[0][3], {
            "source": "event_contract", "destination": "event_contract", "amount": 1204724,
            "source_exchange_shard": 0, "destination_exchange_shard": 2,
            "source_subaccount": 0, "destination_subaccount": 0})
        self.assertEqual(Decimal(self.api.balances[2]), Decimal("120.4724"))

    async def test_intent_is_fsynced_before_post(self):
        original = self.api.request
        async def inspect(method, path, **kwargs):
            if method == "POST":
                saved = self.journal.load("transfer")
                self.assertEqual(saved["status"], "SUBMITTING")
                self.assertEqual(saved["request"], kwargs["body"])
            return await original(method, path, **kwargs)
        self.api.request = inspect
        await self.transfer()

    async def test_restart_completed_transfer_never_posts_again(self):
        await self.transfer()
        await admin.transfer_all(self.api, admin.Journal(self.root), timeout=0)
        self.assertEqual(len(self.api.posts()), 1)

    async def test_timeout_keeps_unknown_intent_and_blocks_duplicate(self):
        self.api.post_error = TimeoutError("secret-no-output")
        with self.assertRaises(admin.SafetyError): await self.transfer()
        self.assertEqual(self.journal.load("transfer")["status"], "SUBMISSION_UNKNOWN")
        with self.assertRaises(admin.SafetyError): await self.transfer()
        self.assertEqual(len(self.api.posts()), 1)
        self.assertNotIn("secret-no-output", (self.root / "transfer.json").read_text())

    async def test_pending_transfer_can_only_be_polled_not_resent(self):
        self.api.transfer_status = "pending"
        with self.assertRaises(admin.SafetyError): await self.transfer()
        self.assertEqual(self.journal.load("transfer")["status"], "PENDING")
        with self.assertRaises(admin.SafetyError): await self.transfer()
        self.assertEqual(len(self.api.posts()), 1)

    async def test_write_scope_and_bound_keys_fail_closed(self):
        for scopes, subaccount in ((["read"], None), (["write"], 0), (["write"], 1)):
            self.api.scopes, self.api.subaccount = scopes, subaccount
            with self.assertRaises(admin.SafetyError): await self.transfer()
        self.assertEqual(self.api.posts(), [])

    async def test_explicit_worker_pause_and_exact_confirmation_required(self):
        with self.assertRaises(admin.SafetyError):
            await admin.transfer_all(self.api, self.journal, execute=True, answer=self.approve)
        with self.assertRaises(admin.SafetyError):
            await admin.transfer_all(self.api, self.journal, execute=True, workers_paused=True, answer=lambda _: "yes")
        self.assertEqual(self.api.posts(), [])

    async def test_resting_orders_positions_pending_transfers_block_admin_write(self):
        for attr, value in (("resting", [{}]), ("positions", [{"position_fp": "0.01"}]),
                            ("positions", [{}]), ("transfers", [{"status": "pending"}])):
            self.api = FakeApi()
            setattr(self.api, attr, value)
            with self.assertRaises(admin.SafetyError): await self.transfer()
            self.assertEqual(self.api.posts(), [])

    async def test_conflicting_auto_allocation_blocks_one_time_transfer(self):
        self.api.allocations = [{"exchange_index": 0, "percent": 10}, {"exchange_index": 2, "percent": 90}]
        with self.assertRaises(admin.SafetyError): await self.transfer()
        self.assertEqual(self.api.posts(), [])

    async def test_balance_change_after_confirmation_requires_new_preview(self):
        def changed(prompt):
            self.api.balances[0] = "130.00"
            return self.approve(prompt)
        with self.assertRaises(admin.SafetyError):
            await admin.transfer_all(self.api, self.journal, execute=True, workers_paused=True, answer=changed)
        self.assertEqual(self.api.posts(), [])

    async def test_wrong_amount_or_route_cannot_attach_to_saved_intent(self):
        self.api.transfer_status = "pending"
        with self.assertRaises(admin.SafetyError): await self.transfer()
        operation = self.journal.load("transfer")
        self.api.transfer_payload["amount"] = "1.00"
        with self.assertRaises(admin.SafetyError):
            await admin.verify_transfer(self.api, self.journal, operation, timeout=0)
        self.api.transfer_payload["amount"] = "120.4724"
        self.api.transfer_payload["destination_exchange_shard"] = 3
        with self.assertRaises(admin.SafetyError):
            await admin.verify_transfer(self.api, self.journal, operation, timeout=0)
        self.assertEqual(len(self.api.posts()), 1)

    async def test_complete_receipt_does_not_hide_balance_mismatch(self):
        await self.transfer()
        self.api.balances[2] = "1.00"
        with self.assertRaises(admin.SafetyError):
            await admin.transfer_all(self.api, self.journal, timeout=0)
        self.assertEqual(self.journal.load("transfer")["status"], "COMPLETED")
        self.assertFalse(self.journal.load("transfer")["balance_delta_matches_plan"])
        self.assertEqual(len(self.api.posts()), 1)

    async def test_allocation_is_a_separate_explicit_operation(self):
        await admin.allocation_all(self.api, self.journal)
        self.assertEqual(self.api.posts(), [])
        result = await admin.allocation_all(self.api, self.journal, execute=True, workers_paused=True, answer=self.approve)
        self.assertEqual(result["status"], "VERIFIED")
        self.assertEqual(self.api.posts()[0][1:], (admin.ALLOCATION, None, {"allocations": [{"exchange_index": 2, "percent": 100}]}))
        await admin.allocation_all(self.api, self.journal)
        self.assertEqual(len(self.api.posts()), 1)

    async def test_uncertain_allocation_is_not_blindly_retried(self):
        self.api.post_error = TimeoutError()
        with self.assertRaises(admin.SafetyError):
            await admin.allocation_all(self.api, self.journal, execute=True, workers_paused=True, answer=self.approve)
        with self.assertRaises(admin.SafetyError):
            await admin.allocation_all(self.api, self.journal, execute=True, workers_paused=True, answer=self.approve)
        self.assertEqual(len(self.api.posts()), 1)

    def test_duplicate_and_invalid_allocations_rejected(self):
        for rows in ([{"exchange_index": 2, "percent": 99}],
                     [{"exchange_index": 2, "percent": 0}, {"exchange_index": 2, "percent": 100}],
                     [{"exchange_index": True, "percent": 100}]):
            with self.assertRaises(admin.SafetyError): admin.allocation_map({"allocations": rows})

    def test_two_local_process_locks_cannot_overlap(self):
        with admin.operation_lock(self.root):
            with self.assertRaises(admin.SafetyError):
                with admin.operation_lock(self.root): pass

    async def test_no_trade_withdrawal_key_change_or_unconfirmed_write_endpoint(self):
        api = object.__new__(admin.Api)
        api.execute = True
        for method, path in (("POST", "/portfolio/events/orders"), ("DELETE", "/portfolio/events/orders/order"),
                             ("POST", "/portfolio/withdrawals"), ("POST", "/api_keys")):
            with self.assertRaises(admin.SafetyError): await api.request(method, path)
        api.execute = False
        with self.assertRaises(admin.SafetyError): await api.request("POST", admin.TRANSFER)

    async def test_read_only_command_rejects_execute_flag_before_authentication(self):
        for command in ("status", "resume-transfer"):
            with self.assertRaises(admin.SafetyError):
                await admin.run(admin.parser().parse_args([command, "--execute"]), self.api)
        self.assertEqual(self.api.calls, [])

    async def test_transfer_id_cannot_be_mistaken_for_a_post_idempotency_key(self):
        with self.assertRaises(admin.SafetyError):
            await admin.run(admin.parser().parse_args(["transfer-all", "--execute", "--transfer-id", "example"]), self.api)
        self.assertEqual(self.api.calls, [])

    async def test_manual_recovery_of_unknown_post_is_read_only_and_matches_intent(self):
        self.api.post_error = TimeoutError()
        with self.assertRaises(admin.SafetyError): await self.transfer()
        operation = self.journal.load("transfer")
        self.api.transfer_payload = {**operation["request"], "transfer_id": "recovered-id", "amount": "120.4724",
                                     "created_ts": time.time(), "status": "complete"}
        self.api.balances = {0: "0", 2: "120.4724"}
        args = admin.parser().parse_args(["resume-transfer", "--transfer-id", "recovered-id", "--timeout", "0"])
        await admin.run(args, self.api, root=self.root)
        self.assertEqual(len(self.api.posts()), 1)
        self.assertEqual(self.journal.load("transfer")["status"], "COMPLETED")

    async def test_allocation_checks_other_account_shards_not_only_source_and_btc(self):
        self.api.balances[1] = "0"
        original = self.api.request
        async def get(method, path, **kwargs):
            if path == "/portfolio/orders" and kwargs.get("params", {}).get("exchange_index") == 1:
                return {"orders": [{"order_id": "other-shard-order"}]}
            return await original(method, path, **kwargs)
        self.api.request = get
        with self.assertRaises(admin.SafetyError):
            await admin.allocation_all(self.api, self.journal, execute=True, workers_paused=True, answer=self.approve)
        self.assertEqual(self.api.posts(), [])

    async def test_transport_does_not_retry_post_but_retries_get(self):
        api = object.__new__(admin.Api)
        api.execute = True
        api._auth = Mock()
        api._auth.create_auth_headers.return_value = {}
        session = Mock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=False)
        session.request.side_effect = TimeoutError()
        with patch("aiohttp.ClientSession", return_value=session), patch("kalshi_shard_admin.asyncio.sleep", new_callable=AsyncMock):
            with self.assertRaises(admin.ApiError): await api.request("POST", admin.TRANSFER, body={})
            self.assertEqual(session.request.call_count, 1)
            session.request.reset_mock()
            with self.assertRaises(admin.ApiError): await api.request("GET", "/portfolio/balance")
            self.assertEqual(session.request.call_count, 3)


if __name__ == "__main__": unittest.main()
