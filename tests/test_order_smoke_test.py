"""No-network fault injection for the operator-only order test."""
import argparse
from contextlib import asynccontextmanager, redirect_stdout
from copy import deepcopy
from decimal import Decimal
import io
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import AsyncMock, patch

import kalshi_order_smoke_test as smoke
from kalshi_shard_admin import Api, ApiError, Journal, SafetyError


def market():
    return {"ticker": "KXBTC15M-test", "market_type": "binary", "exchange_index": 2, "status": "active",
            "open_time": time.time() - 60, "close_time": time.time() + 840,
            "price_ranges": [{"start": "0.00", "end": "1.00", "step": "0.01"}]}


class FakeApi:
    key_id = "offline-test-only"
    execute = True

    def __init__(self):
        self.market = market()
        self.balance = "1.00"
        self.scopes = ["read", "write"]
        self.orders = []
        self.fills = []
        self.positions = []
        self.calls = []
        self.post_error = None
        self.cancel_error = None
        self.fill_on_cancel = "0.00"
        self.total_failure = False
        self.post_count = 0

    async def request(self, method, path, *, params=None, body=None):
        self.calls.append((method, path, deepcopy(params), deepcopy(body)))
        if method == "GET":
            if path == "/markets": return {"markets": [self.market]}
            if path.startswith("/markets/"): return {"market": self.market}
            if path == "/exchange/status": return {"exchange_active": True, "trading_active": True}
            if path == "/api_keys": return {"api_keys": [{"api_key_id": self.key_id, "scopes": self.scopes}]}
            if path == "/portfolio/balance": return {"balance_dollars": self.balance}
            if path == "/portfolio/orders":
                return {"orders": [o for o in self.orders if not params.get("status") or o["status"] == params["status"]]}
            if path == "/portfolio/fills": return {"fills": self.fills}
            if path == "/portfolio/positions": return {"market_positions": self.positions}
        if method == "POST" and path == smoke.CREATE:
            self.post_count += 1
            if self.total_failure: raise ApiError(None)
            self.orders.append({"order_id": "test-order", "client_order_id": body["client_order_id"],
                "ticker": body["ticker"], "exchange_index": 2, "initial_count_fp": "1.00",
                "yes_price_dollars": body["price"], "book_side": body["side"],
                "fill_count_fp": "0.00", "remaining_count_fp": "1.00", "status": "resting"})
            if self.post_error: raise self.post_error
            return {"order_id": "test-order", "client_order_id": body["client_order_id"],
                    "fill_count": "0.00", "remaining_count": "1.00"}
        if method == "DELETE" and path == smoke.CREATE + "/test-order":
            if self.cancel_error: raise self.cancel_error
            order = self.orders[0]
            order.update(status="canceled", remaining_count_fp="0.00", fill_count_fp=self.fill_on_cancel)
            if Decimal(self.fill_on_cancel):
                self.fills = [{"fill_id": "fill-1", "order_id": "test-order", "ticker": self.market["ticker"],
                    "count_fp": self.fill_on_cancel, "yes_price_dollars": order["yes_price_dollars"],
                    "no_price_dollars": str(1 - Decimal(order["yes_price_dollars"])), "fee_cost": "0.001", "is_taker": False}]
                self.positions = [{"position_fp": str(Decimal(self.fill_on_cancel) * (1 if order["book_side"] == "bid" else -1))}]
            return {"order_id": "test-order", "reduced_by": "1.00"}
        raise AssertionError("Unexpected mock endpoint")


class Feed:
    def fresh(self, ticker, side):
        return {"selected_bid": Decimal("0.48"), "selected_ask": Decimal("0.50")}


@asynccontextmanager
async def stream(api, ticker, side):
    yield Feed()


def approve(prompt):
    return prompt.split("Type exactly to authorize: ", 1)[1].split("\n", 1)[0]


class OrderSmokeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.journal = Journal(Path(self.temp.name))
        self.api = FakeApi()
        self.output = io.StringIO()
        redirection = redirect_stdout(self.output)
        redirection.__enter__()
        self.addCleanup(redirection.__exit__, None, None, None)
        sleep = patch.object(smoke.asyncio, "sleep", new=AsyncMock())
        sleep.start()
        self.addCleanup(sleep.stop)

    async def run_tool(self, **overrides):
        args = argparse.Namespace(execute=True, workers_paused=True, reconcile_only=False, side="yes", ticker=None)
        for k, v in overrides.items(): setattr(args, k, v)
        return await smoke.run_test(args, self.api, self.journal, stream=stream, answer=approve)

    async def test_default_is_read_only_including_websocket_preflight(self):
        await self.run_tool(execute=False, workers_paused=False)
        self.assertEqual(self.api.post_count, 0)
        self.assertFalse(any(c[0] != "GET" for c in self.api.calls))
        self.assertIsNone(self.journal.load(smoke.KIND))

    async def test_submit_once_cancel_and_verify_no_fills(self):
        await self.run_tool()
        op = self.journal.load(smoke.KIND)
        self.assertEqual(op["state"], "CANCELED_NO_FILL")
        self.assertEqual(self.api.post_count, 1)
        post = next(c for c in self.api.calls if c[0] == "POST")
        self.assertEqual(post[3]["count"], "1.00")
        self.assertEqual(post[3]["price"], "0.0100")
        self.assertTrue(post[3]["post_only"])
        self.assertLessEqual(post[3]["expiration_time"] - time.time(), 30)
        self.assertEqual(post[3]["exchange_index"], 2)
        self.assertTrue(any(c[0] == "DELETE" and c[2]["exchange_index"] == 2 for c in self.api.calls))

    async def test_no_side_is_economic_one_cent_not_99_cent_purchase(self):
        await self.run_tool(side="no")
        plan = self.journal.load(smoke.KIND)["plan"]
        self.assertEqual((plan["side"], plan["price"]), ("ask", "0.9900"))

    async def test_restart_never_creates_second_order(self):
        await self.run_tool()
        await self.run_tool()
        self.assertEqual(self.api.post_count, 1)

    async def test_unknown_post_adopted_by_id_and_canceled_without_retry(self):
        self.api.post_error = ApiError(None)
        await self.run_tool()
        self.assertEqual(self.api.post_count, 1)
        self.assertEqual(self.journal.load(smoke.KIND)["state"], "CANCELED_NO_FILL")

    async def test_unknown_post_with_no_exchange_record_blocks_repeat(self):
        self.api.total_failure = True
        for _ in range(2):
            with self.assertRaises(SafetyError): await self.run_tool()
        self.assertEqual(self.api.post_count, 1)
        self.assertEqual(self.journal.load(smoke.KIND)["state"], "UNRESOLVED_DO_NOT_REPEAT")

    async def test_cancel_error_not_treated_as_canceled(self):
        self.api.cancel_error = ApiError(500)
        with self.assertRaises(SafetyError): await self.run_tool()
        self.assertEqual(self.api.orders[0]["remaining_count_fp"], "1.00")
        self.assertNotEqual(self.journal.load(smoke.KIND)["state"], "CANCELED_NO_FILL")

    async def test_partial_fill_while_canceling_reports_real_exposure_and_fees(self):
        self.api.fill_on_cancel = "0.25"
        with self.assertRaisesRegex(SafetyError, "filled"): await self.run_tool()
        op = self.journal.load(smoke.KIND)
        self.assertEqual(op["state"], "FILLED_REVIEW_REQUIRED")
        self.assertEqual(op["filled_quantity"], "0.25")
        self.assertEqual(op["position"], "0.25")
        self.assertEqual(op["fills"][0]["fees"], "0.001")
        self.assertEqual(self.api.post_count, 1)  # never auto-liquidate

    async def test_no_fill_direction_position_sign(self):
        self.api.fill_on_cancel = "1.00"
        with self.assertRaises(SafetyError): await self.run_tool(side="no")
        self.assertEqual(self.journal.load(smoke.KIND)["position"], "-1.00")

    async def test_persisted_intent_before_post(self):
        original = self.api.request
        async def request(method, path, **kwargs):
            if method == "POST":
                self.assertEqual(self.journal.load(smoke.KIND)["state"], "SUBMISSION_INTENT")
            return await original(method, path, **kwargs)
        self.api.request = request
        await self.run_tool()

    async def test_no_order_if_journal_cannot_save(self):
        with patch.object(self.journal, "save", side_effect=OSError), self.assertRaises(OSError):
            await self.run_tool()
        self.assertEqual(self.api.post_count, 0)

    async def test_pause_attestation_required(self):
        with self.assertRaises(SafetyError): await self.run_tool(workers_paused=False)
        self.assertEqual(self.api.calls, [])

    async def test_funding_wrong_shard_scope_and_active_risk_block(self):
        cases = (("balance", "0.00"), ("scopes", ["read"]), ("positions", [{"position_fp": "0.01"}]),
                 ("orders", [{"status": "resting"}]))
        for field, value in cases:
            self.api = FakeApi()
            setattr(self.api, field, value)
            with self.assertRaises(SafetyError): await self.run_tool()
            self.assertEqual(self.api.post_count, 0)
        self.api = FakeApi()
        self.api.market["exchange_index"] = 0
        with self.assertRaises(SafetyError): await self.run_tool()
        self.assertEqual(self.api.post_count, 0)

    async def test_reconcile_only_does_not_create_without_journal(self):
        with self.assertRaises(SafetyError): await self.run_tool(reconcile_only=True)
        self.assertEqual(self.api.calls, [])

    def test_limit_grid_status_and_boundary_checks(self):
        for key, value in (("price_ranges", []), ("status", "settled"), ("close_time", time.time() + 20),
                           ("market_type", "scalar"), ("open_time", time.time() + 30)):
            m = market(); m[key] = value
            with self.assertRaises(SafetyError): smoke.plan_for(m, "yes")

    def test_write_allowlist_is_separate_and_consumed_once(self):
        api = object.__new__(smoke.SmokeApi)
        api.execute = False
        plan = smoke.plan_for(market(), "yes")
        api.permitted_plan = plan
        with self.assertRaises(SafetyError): api.authorize_request("POST", smoke.CREATE, body=plan)
        api.execute = True
        with self.assertRaises(SafetyError): api.authorize_request("POST", smoke.CREATE, body=dict(plan, count="100.00"))
        api.authorize_request("POST", smoke.CREATE, body=plan)
        with self.assertRaises(SafetyError): api.authorize_request("POST", smoke.CREATE, body=plan)
        with self.assertRaises(SafetyError): api.authorize_request("POST", "/portfolio/intra_exchange_instance_transfer", body={})
        admin = object.__new__(Api); admin.execute = True
        with self.assertRaises(SafetyError): admin.authorize_request("POST", smoke.CREATE, body=plan)

    def test_quote_no_depth_required_but_stale_missing_and_out_of_order_rejected(self):
        import json
        feed = smoke.SmokeFeed(auth=None, url=smoke.WS_URL)
        feed.set_tickers(["KXBTC15M-test"])
        feed.connected = True
        msg = {"market_ticker": "KXBTC15M-test", "yes_bid_dollars": "0.4900", "yes_ask_dollars": "0.5100", "ts_ms": int(time.time() * 1000)}
        feed._handle(json.dumps({"type": "ticker", "msg": msg}))
        self.assertEqual(feed.fresh("KXBTC15M-test", "yes")["selected_ask"], Decimal("0.51"))
        old = deepcopy(feed.quotes)
        msg["ts_ms"] -= 10000; msg["yes_ask_dollars"] = "0.99"
        feed._handle(json.dumps({"type": "ticker", "msg": msg}))
        self.assertEqual(feed.quotes, old)
        feed.quotes["KXBTC15M-test"]["exchange_epoch"] -= 10
        with self.assertRaises(SafetyError): feed.fresh("KXBTC15M-test", "yes")

    def test_ws_error_text_never_echoes(self):
        feed = smoke.SmokeFeed(auth=None, url=smoke.WS_URL)
        with self.assertRaises(SafetyError) as raised: feed._handle('{"type":"error","msg":"SENSITIVE_MOCK"}')
        self.assertNotIn("SENSITIVE_MOCK", str(raised.exception))

