"""Pure/mocked admin tests: no account credentials, transfers or live orders."""
from __future__ import annotations

from contextlib import redirect_stdout
from copy import deepcopy
from decimal import Decimal
import io
import json
from pathlib import Path
import os
import tempfile
import time
import unittest
import warnings
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
    @classmethod
    def setUpClass(cls):
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        cls.ephemeral_pem = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                              serialization.NoEncryption()).decode()
        cls.mock_key_id = "00000000-0000-4000-8000-000000000001"

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

    def test_current_v2_transfer_request_schema_is_exact(self):
        request = admin.transfer_request("100.0000", 0, 2)
        self.assertEqual(request, {
            "source": "event_contract", "destination": "event_contract",
            "amount": 1000000, "source_exchange_shard": 0,
            "destination_exchange_shard": 2, "source_subaccount": 0,
            "destination_subaccount": 0,
        })
        admin.validate_transfer_request(request)
        for invalid in (
            {**request, "amount": "1000000"},
            {**request, "amount": True},
            {**request, "source": "margined"},
            {**request, "source_subaccount": 1},
            {**request, "source_subaccount": False},
            {**request, "destination_exchange_shard": 0},
            {**request, "undocumented": 1},
        ):
            with self.assertRaises(admin.SafetyError):
                admin.validate_transfer_request(invalid)

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

    async def test_explicit_amount_and_destination_do_not_depend_on_market_discovery(self):
        result = await admin.transfer_funds(
            self.api, self.journal, destination_shard=2,
            amount_dollars="100.0000", execute=True, workers_paused=True,
            answer=self.approve, timeout=0,
        )
        self.assertEqual(result["status"], "COMPLETED")
        self.assertIsNone(result["market_ticker"])
        self.assertEqual(result["destination_source"], "explicit_argument")
        self.assertEqual(result["request"]["amount"], 1000000)
        self.assertEqual(Decimal(self.api.balances[0]), Decimal("20.4724"))
        self.assertEqual(Decimal(self.api.balances[2]), Decimal("100.0000"))
        self.assertFalse(any(call[1] in {"/markets"} or call[1].startswith("/markets/")
                             for call in self.api.calls))

    async def test_explicit_amount_cannot_exceed_authenticated_source_balance(self):
        with self.assertRaises(admin.SafetyError):
            await admin.transfer_funds(
                self.api, self.journal, destination_shard=2,
                amount_dollars="120.4725", execute=True,
                workers_paused=True, answer=self.approve, timeout=0,
            )
        self.assertEqual(self.api.posts(), [])
        self.assertIsNone(self.journal.load("transfer"))

    async def test_transfer_cli_requires_exact_amount_and_destination(self):
        for arguments in (["transfer"], ["transfer", "--destination-shard", "2"],
                          ["transfer", "--amount-dollars", "1.0000"]):
            with self.assertRaises(admin.SafetyError):
                await admin.run(admin.parser().parse_args(arguments), self.api, root=self.root)
        self.assertEqual(self.api.calls, [])

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
        for command in ("status", "auth-check", "transfers", "resume-transfer"):
            with self.assertRaises(admin.SafetyError):
                await admin.run(admin.parser().parse_args([command, "--execute"]), self.api)
        self.assertEqual(self.api.calls, [])

    async def test_stale_pending_list_is_confirmed_by_read_only_id_lookup(self):
        self.api.transfers = [{"transfer_id": "old-transfer", "status": "pending"}]
        self.api.transfer_payload = {"transfer_id": "old-transfer", "status": "complete"}
        records = await admin.transfer_history(self.api)
        self.assertEqual(records[0]["status"], "complete")
        self.assertEqual(self.api.posts(), [])
        self.assertTrue(any(call[1] == admin.TRANSFERS + "/old-transfer" for call in self.api.calls))

    async def test_pending_incoming_margin_is_visible_but_not_counted_as_cash(self):
        self.api.transfer_payload = {
            "transfer_id": "old-transfer", "status": "pending", "amount": "0.01",
            "source": "margined", "destination": "event_contract",
            "source_exchange_shard": 0, "destination_exchange_shard": 0, "created_ts": 1700000000,
        }
        self.api.transfers = [deepcopy(self.api.transfer_payload)]
        await admin.run(admin.parser().parse_args(["transfers"]), self.api, root=self.root / "absent")
        self.assertIn('"transfer_history_clear": false', self.stdout.getvalue())
        self.assertIn('"transfer_id": "old-transfer"', self.stdout.getvalue())
        self.assertFalse((self.root / "absent").exists())
        self.assertEqual(self.api.posts(), [])
        result = await self.transfer()
        self.assertEqual(result["request"]["amount"], 1204724)
        self.assertEqual(result["pending_incoming_margin_transfer_ids"], ["old-transfer"])
        self.assertEqual(len(self.api.posts()), 1)

    async def test_pending_event_contract_transfer_still_blocks_new_post(self):
        self.api.transfer_payload = {
            "transfer_id": "pending-event-transfer", "status": "pending", "amount": "1.00",
            "source": "event_contract", "destination": "event_contract",
            "source_exchange_shard": 0, "destination_exchange_shard": 2, "created_ts": 1700000000,
        }
        self.api.transfers = [deepcopy(self.api.transfer_payload)]
        with self.assertRaises(admin.SafetyError): await self.transfer()
        self.assertEqual(self.api.posts(), [])

    def test_incoming_transfer_exception_requires_known_status_route_and_amount(self):
        incoming = {"transfer_id": "incoming", "status": "pending", "amount": "1.00",
                    "source": "margined", "destination": "event_contract",
                    "source_exchange_shard": 0, "destination_exchange_shard": 0, "created_ts": 1700000000}
        for changes in ({"status": "unknown"}, {"source": "event_contract"}, {"destination": "margined"}):
            assessment = admin.pending_transfer_assessment([{**incoming, **changes}])
            self.assertEqual(assessment["blocking_transfer_ids"], ["incoming"])
            self.assertEqual(assessment["pending_incoming_margin_transfer_ids"], [])
        for changes in ({"amount": "-1"}, {"source_exchange_shard": None}, {"created_ts": None}):
            with self.assertRaises(admin.SafetyError):
                admin.pending_transfer_assessment([{**incoming, **changes}])

    async def test_preview_performs_full_read_only_exposure_preflight(self):
        self.api.resting = [{"order_id": "mock"}]
        with self.assertRaises(admin.SafetyError):
            await admin.transfer_all(self.api, self.journal)
        self.assertEqual(self.api.posts(), [])
        self.assertIsNone(self.journal.load("transfer"))

    async def test_pending_transfer_id_lookup_mismatch_fails_closed(self):
        self.api.transfers = [{"transfer_id": "expected", "status": "pending"}]
        self.api.transfer_payload = {"transfer_id": "wrong", "status": "complete"}
        with self.assertRaises(admin.SafetyError): await self.transfer()
        self.assertEqual(self.api.posts(), [])

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

    async def test_transfer_post_signs_current_full_v2_path(self):
        api = admin.Api(self.mock_key_id, self.ephemeral_pem, execute=True)
        api._auth = Mock()
        api._auth.create_auth_headers.return_value = {
            "KALSHI-ACCESS-KEY": "present",
            "KALSHI-ACCESS-SIGNATURE": "present",
            "KALSHI-ACCESS-TIMESTAMP": "present",
        }
        response = Mock(status=200, headers={})
        response.read = AsyncMock(return_value=b'{"transfer_id":"mock-transfer"}')
        response.__aenter__ = AsyncMock(return_value=response)
        response.__aexit__ = AsyncMock(return_value=False)
        session = Mock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=False)
        session.request.return_value = response
        request = admin.transfer_request("1.0000", 0, 2)
        with patch("aiohttp.ClientSession", return_value=session):
            result = await api.request("POST", admin.TRANSFER, body=request)
        api._auth.create_auth_headers.assert_called_once_with(
            "POST", "/trade-api/v2/portfolio/intra_exchange_instance_transfer",
        )
        session.request.assert_called_once_with(
            "POST", "https://external-api.kalshi.com/trade-api/v2/portfolio/intra_exchange_instance_transfer",
            params=None, json=request,
            headers=api._auth.create_auth_headers.return_value,
            allow_redirects=False,
        )
        self.assertEqual(result, {"transfer_id": "mock-transfer"})

    def test_conflicting_key_id_aliases_are_not_silently_selected(self):
        with patch.dict(os.environ, {"KALSHI_API_KEY_ID": "old-id", "KALSHI_PROD_API_KEY": "new-id",
                                     "KALSHI_PRIVATE_KEY": self.ephemeral_pem}, clear=True):
            report = admin.credential_environment_report()
            self.assertTrue(report["key_id_alias_conflict"])
            with self.assertRaises(admin.SafetyError) as error:
                admin.Api.from_environment()
            self.assertNotIn("old-id", str(error.exception))
            self.assertNotIn("new-id", str(error.exception))
            self.assertNotIn(self.ephemeral_pem, str(error.exception))

    def test_credentials_trim_whitespace_and_allow_matching_aliases(self):
        env = {"KALSHI_API_KEY_ID": "  " + self.mock_key_id + "\n", "KALSHI_PROD_API_KEY": self.mock_key_id,
               "KALSHI_PRIVATE_KEY": self.ephemeral_pem}
        with patch.dict(os.environ, env, clear=True):
            api = admin.Api.from_environment()
        self.assertEqual(api.key_id, self.mock_key_id)
        self.assertFalse(api.execute)
        self.assertEqual(len(api.credential_sources["key_id_sources"]), 2)

    def test_single_key_alias_and_pem_newline_transport_forms(self):
        for name in ("KALSHI_API_KEY_ID", "KALSHI_PROD_API_KEY"):
            for pem in (self.ephemeral_pem, self.ephemeral_pem.replace("\n", "\r\n"),
                        self.ephemeral_pem.replace("\n", "\\n"), self.ephemeral_pem.replace("\n", "\\r\\n")):
                with patch.dict(os.environ, {name: self.mock_key_id, "KALSHI_PRIVATE_KEY": pem}, clear=True):
                    api = admin.Api.from_environment()
                self.assertEqual(api.credential_sources["key_id_sources"], [name])
                self.assertEqual(api._auth.private_key.key_size, 2048)

    def test_missing_quoted_and_invalid_private_keys_fail_without_secrets(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(admin.SafetyError): admin.Api.from_environment()
        for pem in ('"' + self.ephemeral_pem + '"', "private-value-that-must-not-print"):
            with patch.dict(os.environ, {"KALSHI_PROD_API_KEY": self.mock_key_id, "KALSHI_PRIVATE_KEY": pem}, clear=True):
                with self.assertRaises(admin.SafetyError) as error:
                    admin.Api.from_environment()
                self.assertNotIn(pem, str(error.exception))

    def test_pem_path_source_only_when_environment_pem_missing(self):
        with patch.dict(os.environ, {"KALSHI_PROD_API_KEY": self.mock_key_id, "KALSHI_PEM_PATH": "private-file"}, clear=True), \
                patch.object(Path, "read_text", return_value=self.ephemeral_pem):
            api = admin.Api.from_environment()
        self.assertEqual(api.credential_sources["private_key_source"], "KALSHI_PEM_PATH")

    async def test_auth_check_verifies_sdk_signing_and_reads_only_balance(self):
        api = admin.Api(self.mock_key_id, self.ephemeral_pem, execute=False)
        api.request = AsyncMock(return_value={"balance_dollars": "1.00"})
        await admin.run(admin.parser().parse_args(["auth-check"]), api, root=self.root / "absent")
        api.request.assert_awaited_once_with("GET", "/portfolio/balance")
        self.assertIn('"signer_self_check": "PASS"', self.stdout.getvalue())
        self.assertIn('"diagnostic_version": 2', self.stdout.getvalue())
        self.assertIn('"authenticated_balance_read": "PASS"', self.stdout.getvalue())
        self.assertNotIn(self.mock_key_id, self.stdout.getvalue())
        self.assertNotIn(self.ephemeral_pem, self.stdout.getvalue())
        self.assertFalse((self.root / "absent").exists())

    async def test_auth_check_401_is_not_reported_as_success_or_retried(self):
        api = admin.Api(self.mock_key_id, self.ephemeral_pem, execute=False)
        api.request = AsyncMock(side_effect=admin.ApiError(401, "authentication_error"))
        with self.assertRaises(admin.ApiError): await admin.authentication_check(api)
        self.assertEqual(api.request.await_count, 1)
        self.assertIn('"result": "FAILED"', self.stdout.getvalue())
        self.assertNotIn('"authenticated_balance_read": "PASS"', self.stdout.getvalue())

    async def test_bad_sdk_timestamp_fails_local_check_before_network(self):
        api = admin.Api(self.mock_key_id, self.ephemeral_pem, execute=False)
        api.request = AsyncMock()
        headers = api._auth.create_auth_headers("GET", admin.PREFIX + "/portfolio/balance")
        headers["KALSHI-ACCESS-TIMESTAMP"] = str(int(time.time()))
        with patch.object(api._auth, "create_auth_headers", return_value=headers):
            with self.assertRaises(admin.SafetyError): await admin.authentication_check(api)
        api.request.assert_not_awaited()

    async def test_auth_check_rejects_write_enabled_client(self):
        api = admin.Api(self.mock_key_id, self.ephemeral_pem, execute=True)
        api.request = AsyncMock()
        with self.assertRaises(admin.SafetyError): await admin.authentication_check(api)
        api.request.assert_not_awaited()

    async def test_transport_401_has_endpoint_but_never_raw_error_or_headers(self):
        api = admin.Api(self.mock_key_id, self.ephemeral_pem, execute=False)
        response = Mock(status=401, headers={"Date": "Tue, 08 Sep 2026 12:00:00 GMT"})
        response.read = AsyncMock(return_value=b'{"error":{"code":"authentication_error","message":"secret-must-not-print"}}')
        response.__aenter__ = AsyncMock(return_value=response)
        response.__aexit__ = AsyncMock(return_value=False)
        session = Mock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=False)
        session.request.return_value = response
        with patch("aiohttp.ClientSession", return_value=session):
            with self.assertRaises(admin.ApiError) as error:
                await api.request("GET", "/portfolio/balance")
        self.assertEqual(session.request.call_count, 1)
        self.assertEqual(error.exception.request_path, "/portfolio/balance")
        self.assertEqual(error.exception.request_method, "GET")
        self.assertIn("approx_http_date_clock_offset_seconds", api.last_response_diagnostics)
        self.assertNotIn("secret-must-not-print", str(error.exception))
        self.assertNotIn(self.mock_key_id, str(api.last_response_diagnostics))

    def test_server_error_decoder_preserves_categories_not_raw_credentials(self):
        examples = {
            "INCORRECT_API_KEY_SIGNATURE": "SIGNATURE_REJECTED",
            "INVALID_API_KEY_SIGNATURE": "SIGNATURE_REJECTED",
            "API_KEY_NOT_FOUND": "KEY_NOT_RECOGNIZED",
            "INVALID_API_KEY": "KEY_REJECTED_UNSPECIFIED",
            "API_KEY_EXPIRED": "KEY_REVOKED_OR_EXPIRED",
            "Invalid timestamp": "TIMESTAMP_REJECTED",
            "IP not allowed": "ACCESS_RESTRICTED",
            "Missing auth headers": "AUTH_HEADERS_MISSING",
            "Insufficient permissions": "SCOPE_REJECTED",
        }
        for marker, category in examples.items():
            payload = {"error": {"code": "authentication_error", "message": marker,
                                  "details": {"reason": marker, "message": self.mock_key_id + self.ephemeral_pem}}}
            result = admin.safe_server_error_details(payload)
            self.assertEqual(result["server_reasons"], [category])
            self.assertNotIn(self.mock_key_id, str(result))
            self.assertNotIn(self.ephemeral_pem, str(result))

    def test_unknown_server_error_is_not_invented_or_echoed(self):
        for payload in ({"error": {"message": "new-secret-bearing-error", "details": "private-detail"}},
                        {"error": "private-value"}, [], None):
            result = admin.safe_server_error_details(payload)
            self.assertEqual(result["server_reasons"], [])
            self.assertFalse(result["server_detail_recognized"])
            self.assertNotIn("private", str(result))

    def test_only_safe_trace_identifiers_leave_response_headers(self):
        result = admin.safe_response_ids({"X-Request-ID": "safe-request-123", "Authorization": "secret-token",
                                         "X-Correlation-ID": "prefix-" + self.mock_key_id,
                                         "CF-Ray": "value\nwith-newline", "X-Amzn-RequestId": "x" * 200},
                                        secrets=(self.mock_key_id, "secret-token"))
        self.assertEqual(result, {"request_id": "safe-request-123"})

    async def test_transport_reports_specific_401_and_support_id_without_echoing_body(self):
        api = admin.Api(self.mock_key_id, self.ephemeral_pem, execute=False)
        response = Mock(status=401, headers={"X-Request-ID": "safe-request-123"})
        response.read = AsyncMock(return_value=json.dumps({"error": {"code": "authentication_error",
            "message": "INCORRECT_API_KEY_SIGNATURE", "details": self.mock_key_id + self.ephemeral_pem}}).encode())
        response.__aenter__ = AsyncMock(return_value=response)
        response.__aexit__ = AsyncMock(return_value=False)
        session = Mock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=False)
        session.request.return_value = response
        with patch("aiohttp.ClientSession", return_value=session):
            with self.assertRaises(admin.ApiError): await admin.authentication_check(api)
        self.assertIn('"SIGNATURE_REJECTED"', self.stdout.getvalue())
        self.assertIn('"request_id": "safe-request-123"', self.stdout.getvalue())
        self.assertNotIn(self.mock_key_id, self.stdout.getvalue())
        self.assertNotIn(self.ephemeral_pem, self.stdout.getvalue())
        self.assertEqual(session.request.call_count, 1)

    def test_original_file_diagnostic_ignores_conflicting_environment_values(self):
        with patch.dict(os.environ, {"KALSHI_API_KEY_ID": "old-id", "KALSHI_PROD_API_KEY": "other-id",
                                     "KALSHI_PRIVATE_KEY": "wrong-pem"}, clear=True), \
                patch.object(Path, "read_text", return_value=self.ephemeral_pem):
            api = admin.Api.for_file_auth_check("private-file", prompt=lambda _: self.mock_key_id)
        self.assertEqual(api.key_id, self.mock_key_id)
        self.assertFalse(api.execute)
        self.assertTrue(api.credential_sources["credential_environment_ignored"])

    def test_original_file_check_refuses_noninteractive_hidden_prompt(self):
        with patch("kalshi_shard_admin.sys.stdin.isatty", return_value=False), \
                patch.object(Path, "read_text") as read:
            with self.assertRaises(admin.SafetyError): admin.Api.for_file_auth_check("private-file")
        read.assert_not_called()

    def test_original_file_prompt_does_not_fall_back_to_echoing(self):
        def unsafe_prompt(_):
            warnings.warn("would fall back to visible input", admin.getpass.GetPassWarning)
            raise AssertionError("Must not reach visible input")
        with patch.object(Path, "read_text", return_value=self.ephemeral_pem):
            with self.assertRaises(admin.SafetyError):
                admin.Api.for_file_auth_check("private-file", prompt=unsafe_prompt)

    async def test_key_file_option_cannot_authorize_any_write(self):
        for command in ("status", "transfer", "transfer-all", "allocation-all", "resume-transfer"):
            with self.assertRaises(admin.SafetyError):
                arguments = [command, "--key-file", "private-file"]
                if command == "transfer":
                    arguments += ["--destination-shard", "2", "--amount-dollars", "1"]
                await admin.run(admin.parser().parse_args(arguments))
        with self.assertRaises(admin.SafetyError):
            await admin.run(admin.parser().parse_args(["auth-check", "--key-file", "private-file", "--execute"]))

    async def test_file_cli_performs_only_get_without_using_environment_or_journal(self):
        with patch("kalshi_shard_admin.sys.stdin.isatty", return_value=True), \
                patch("kalshi_shard_admin.getpass.getpass", return_value=self.mock_key_id), \
                patch.object(Path, "read_text", return_value=self.ephemeral_pem), \
                patch.object(admin.Api, "request", new_callable=AsyncMock, return_value={"balance_dollars": "1"}) as request, \
                patch.object(admin.Api, "from_environment") as env:
            await admin.run(admin.parser().parse_args(["auth-check", "--key-file", "private-file"]), root=self.root / "absent")
        env.assert_not_called()
        request.assert_awaited_once_with("GET", "/portfolio/balance")
        self.assertFalse((self.root / "absent").exists())
        self.assertNotIn(self.mock_key_id, self.stdout.getvalue())

    def test_uuid_shape_check_does_not_expose_key_id(self):
        for value, expected in ((self.mock_key_id, True), ("key-display-name", False)):
            with patch.dict(os.environ, {"KALSHI_PROD_API_KEY": value}, clear=True):
                report = admin.credential_environment_report()
            self.assertEqual(report["key_id_has_uuid_format"], expected)
            self.assertNotIn(value, str(report))


if __name__ == "__main__": unittest.main()
