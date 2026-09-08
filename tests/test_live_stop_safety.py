"""Offline fault injection: no credentials and no network/order endpoints."""
from __future__ import annotations

import json
import unittest
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

from kalshi_btc15m_average_down import KalshiREST, record_submission_failure, order_fee_total
from tests import test_maker_hybrid_v11 as fixtures

BookFeed = fixtures.BookFeed
LiveHybridRest = fixtures.LiveHybridRest


class LiveStopSafetyTests(unittest.IsolatedAsyncioTestCase):
    def setup_trade(self, *, shadow=False):
        fixture = fixtures.MakerHybridV11Tests()
        fixture.setUp()
        fixture.config.update(hybrid_stop_trigger_cents=51, hybrid_maker_exit_cents=52,
                              hybrid_hard_stop_cents=50, stop_price="0.50")
        engine = fixture.engine(dry_run=shadow)
        record = fixture.filled_record(engine)
        record["entry_orders"][0]["average_fill_price"] = "0.54"
        rest = LiveHybridRest()
        rest.refresh_order = AsyncMock()
        return engine, record, rest, BookFeed("0.54", "0.51", "10")

    async def test_gap_through_hard_stop_exits_same_pass_without_maker(self):
        for shadow in (False, True):
            engine, record, rest, feed = self.setup_trade(shadow=shadow)
            feed.bid = Decimal("0.49")
            await engine.manage_stop(rest, feed, record)
            self.assertEqual(record["status"], "CLOSED")
            self.assertEqual(record["exit_classification"], "HARD_STOP_ONLY")
            self.assertEqual(rest.maker_creates, 0)
            self.assertEqual(len(record["exit_orders"]), 1)

    async def test_maker_intent_is_durable_before_post(self):
        engine, record, rest, feed = self.setup_trade()
        original = rest.create_reduce_only_maker_exit
        async def inspect(**kwargs):
            saved = json.loads(engine.state_path.read_text())["markets"][record["ticker"]]
            self.assertEqual(saved["status"], "MAKER_EXIT_PENDING")
            self.assertEqual(saved["exit_orders"][0]["client_order_id"], kwargs["client_order_id_override"])
            return await original(**kwargs)
        rest.create_reduce_only_maker_exit = inspect
        await engine.manage_stop(rest, feed, record)
        self.assertEqual(len(record["exit_orders"]), 1)

    async def test_partial_maker_exit_hard_exits_only_residual(self):
        engine, record, rest, feed = self.setup_trade()
        await engine.manage_stop(rest, feed, record)
        record["exit_orders"][0].update(fill_count="0.40", remaining_count="0.60", average_fill_price="0.52")
        rest.position = Decimal("0.60")
        feed.bid = Decimal("0.50")
        await engine.manage_stop(rest, feed, record)
        self.assertEqual(Decimal(record["exit_orders"][-1]["quantity"]), Decimal("0.60"))
        self.assertEqual(record["exit_classification"], "MAKER_EXIT_PARTIAL_THEN_HARD_STOP")
        self.assertEqual(record["status"], "CLOSED")

    async def test_failed_cancel_blocks_hard_exit_then_retries_when_confirmed(self):
        engine, record, rest, feed = self.setup_trade()
        await engine.manage_stop(rest, feed, record)
        original = rest.cancel_order
        rest.cancel_order = AsyncMock(return_value=False)
        feed.bid = Decimal("0.50")
        await engine.manage_stop(rest, feed, record)
        self.assertEqual(rest.hard_creates, 0)
        self.assertTrue(record["hybrid_stop"]["hard_stop_latched"])
        rest.cancel_order = original
        feed.bid = Decimal("0.51")  # recovery cannot undo an already triggered hard stop
        await engine.manage_stop(rest, feed, record)
        self.assertEqual(rest.hard_creates, 1)
        self.assertEqual(record["status"], "CLOSED")

    async def test_maker_unknown_response_recovers_id_before_cancel(self):
        engine, record, rest, feed = self.setup_trade()
        await engine.manage_stop(rest, feed, record)
        maker = record["exit_orders"][0]
        maker.update(order_id=None, status="submit_failed", submission_outcome="unknown")
        async def recover(order):
            order.update(order_id="recovered-maker", status="resting", submission_outcome="accepted")
            return True
        rest.recover_exit_submission = recover
        feed.bid = Decimal("0.50")
        await engine.manage_stop(rest, feed, record)
        self.assertEqual(maker["order_id"], "recovered-maker")
        self.assertEqual(maker["status"], "canceled")
        self.assertEqual(rest.hard_creates, 1)

    async def test_unknown_hard_response_never_blindly_retries(self):
        engine, record, rest, feed = self.setup_trade()
        record["status"] = "HARD_STOP_PENDING"
        record["hybrid_stop"]["hard_stop_latched"] = True
        record["exit_orders"] = [{"exit_phase": "hard_stop", "quantity": "1.00", "fill_count": "0",
                                  "remaining_count": "1.00", "status": "submit_failed", "client_order_id": "unknown"}]
        rest.recover_exit_submission = AsyncMock(return_value=False)
        feed.bid = Decimal("0.50")
        await engine.manage_stop(rest, feed, record)
        await engine.manage_stop(rest, feed, record)
        self.assertEqual(rest.hard_creates, 0)
        self.assertEqual(len(record["exit_orders"]), 1)

    async def test_confirmed_hard_rejection_allows_residual_retry(self):
        engine, record, rest, feed = self.setup_trade()
        record["status"] = "HARD_STOP_PENDING"
        record["exit_orders"] = [{"exit_phase": "hard_stop", "quantity": "1.00", "fill_count": "0",
                                  "remaining_count": "0", "status": "submit_failed", "submission_outcome": "rejected"}]
        feed.bid = Decimal("0.50")
        await engine.manage_stop(rest, feed, record)
        self.assertEqual(rest.hard_creates, 1)
        self.assertEqual(record["status"], "CLOSED")

    async def test_confirmed_maker_rejection_does_not_block_hard_stop(self):
        engine, record, rest, feed = self.setup_trade()
        await engine.manage_stop(rest, feed, record)
        record["exit_orders"][0].update(order_id=None, submission_outcome="rejected", status="submit_failed", remaining_count="0")
        feed.bid = Decimal("0.50")
        await engine.manage_stop(rest, feed, record)
        self.assertEqual(rest.hard_creates, 1)
        self.assertEqual(record["status"], "CLOSED")

    async def test_cancel_ack_with_remaining_quantity_is_not_confirmation(self):
        engine, record, rest, feed = self.setup_trade()
        await engine.manage_stop(rest, feed, record)
        rest.cancel_order = AsyncMock(return_value=True)  # malformed/inconsistent exchange evidence
        feed.bid = Decimal("0.50")
        await engine.manage_stop(rest, feed, record)
        self.assertEqual(rest.hard_creates, 0)
        self.assertEqual(record["status"], "MAKER_EXIT_CANCEL_UNCONFIRMED")

    async def test_hard_stop_intent_persists_across_process_restart(self):
        engine, record, rest, feed = self.setup_trade()
        feed.bid = Decimal("0.50")
        async def crash(**_kwargs):
            raise RuntimeError("simulated process interruption after POST")
        rest.create_reduce_only_exit = crash
        with self.assertRaises(RuntimeError):
            await engine.manage_stop(rest, feed, record)
        saved = json.loads(engine.state_path.read_text())["markets"][record["ticker"]]
        self.assertEqual(saved["status"], "HARD_STOP_PENDING")
        self.assertTrue(saved["hybrid_stop"]["hard_stop_latched"])
        self.assertEqual(len(saved["exit_orders"]), 1)
        engine.state["markets"][record["ticker"]] = saved
        rest.recover_exit_submission = AsyncMock(return_value=False)
        await engine.manage_stop(rest, feed, saved)
        self.assertEqual(len(saved["exit_orders"]), 1)

    async def test_recovered_full_ioc_is_accounted_without_duplicate(self):
        engine, record, rest, feed = self.setup_trade()
        record["status"] = "HARD_STOP_PENDING"
        record["exit_orders"] = [{"exit_phase": "hard_stop", "quantity": "1.00", "fill_count": "0",
                                  "remaining_count": "1", "status": "submit_failed", "client_order_id": "unknown"}]
        async def recover(order):
            order.update(order_id="hard", fill_count="1.00", remaining_count="0", status="executed",
                         average_fill_price="0.50", fees_paid="0.02")
            return True
        rest.recover_exit_submission = recover
        rest.position = Decimal("0")
        feed.bid = Decimal("0.50")
        await engine.manage_stop(rest, feed, record)
        self.assertEqual(rest.hard_creates, 0)
        self.assertEqual(record["status"], "CLOSED")
        self.assertEqual(Decimal(record["realized_net_pnl"]), Decimal("-0.06"))

    async def test_adapter_empty_lookup_cannot_prove_rejection(self):
        rest = object.__new__(KalshiREST)
        rest.orders = SimpleNamespace(get_orders=AsyncMock(return_value={"orders": [], "cursor": ""}))
        order = {"ticker": "KXBTC15M-test", "client_order_id": "unknown", "remaining_count": "1"}
        self.assertFalse(await rest.recover_exit_submission(order))
        self.assertEqual(order["remaining_count"], "1")

    async def test_adapter_finds_exact_id_on_second_page(self):
        rest = object.__new__(KalshiREST)
        order = {"ticker": "KXBTC15M-test", "client_order_id": "wanted", "side": "yes", "position_price": .52}
        exchange = {"ticker": order["ticker"], "client_order_id": "wanted", "order_id": "found",
                    "status": "resting", "fill_count_fp": "0.25", "remaining_count_fp": "0.75", "yes_price_dollars": "0.52"}
        rest.orders = SimpleNamespace(
            get_orders=AsyncMock(side_effect=[{"orders": [], "cursor": "next"}, {"orders": [exchange], "cursor": ""}]),
            get_order=AsyncMock(return_value={"order": exchange}),
        )
        self.assertTrue(await rest.recover_exit_submission(order))
        self.assertEqual(order["order_id"], "found")
        self.assertEqual(order["fill_count"], .25)
        self.assertEqual(rest.orders.get_orders.call_args.kwargs["cursor"], "next")

    def test_error_classification_preserves_uncertainty_without_secret_text(self):
        for status in (400, 401, 403, 404, 422, 408, 409, 429, 500, None):
            exc = RuntimeError("secret-auth-header-must-not-appear")
            exc.status = status
            order = {"remaining_count": 1}
            record_submission_failure(order, exc)
            self.assertNotIn("secret-auth", json.dumps(order))
            rejected = status in (400, 401, 403, 404, 422)
            self.assertEqual(order["submission_outcome"], "rejected" if rejected else "unknown")
            self.assertEqual(order["remaining_count"], 0 if rejected else 1)

    def test_v2_average_fee_is_multiplied_by_actual_partial_fill(self):
        self.assertEqual(order_fee_total({"fill_count": "2.50", "average_fee_paid": "0.015"}), .0375)
        self.assertEqual(order_fee_total({"fill_count": "0", "average_fee_paid": "0.015"}), 0)
        self.assertEqual(order_fee_total({"fill_count": "2.50", "average_fee_paid": "0.015",
                                         "taker_fees_dollars": "0.04", "maker_fees_dollars": "0.01"}), .05)

    async def test_yes_and_no_stop_adapter_use_correct_book_side_and_price(self):
        for side, book_side, api_price in (("yes", "ask", "0.5200"), ("no", "bid", "0.4800")):
            rest = object.__new__(KalshiREST)
            rest.trading_pause_active = lambda: False
            rest.orders = SimpleNamespace(create_order_v2=AsyncMock(return_value={
                "order_id": "mock-only", "fill_count": "0", "remaining_count": "0.60",
            }))
            await rest.create_reduce_only_maker_exit(ticker="KXBTC15M-test", held_side=side,
                economic_exit_price=.52, quantity=.60, expiration_time=2000000000, dry_run=False, order_key="mock")
            kwargs = rest.orders.create_order_v2.call_args.kwargs
            self.assertEqual(kwargs["side"].value, book_side)
            self.assertEqual(kwargs["price"], api_price)
            self.assertEqual(kwargs["count"], "0.60")
            self.assertTrue(kwargs["post_only"])
            self.assertTrue(kwargs["reduce_only"])
            self.assertEqual(kwargs["time_in_force"], "good_till_canceled")

    def test_explicit_live_switch_does_not_silently_fall_back(self):
        import os
        import subprocess
        import tempfile
        import textwrap
        from pathlib import Path
        workflow = Path(__file__).resolve().parents[1] / ".github/workflows/kalshi_btc15m_average_down.yml"
        text = workflow.read_text()
        block = text.split("id: live_gate", 1)[1].split("        run: |\n", 1)[1].split("\n      - name:", 1)[0]
        script = textwrap.dedent(block)
        for live, permission, shadow, read_only, expected_code, label in (
            ("true", "false", "true", "false", 2, "LIVE REQUEST BLOCKED"),
            ("true", "true", "false", "false", 0, "REQUESTED_MODE=LIVE"),
            ("false", "true", "false", "false", 0, "REQUESTED_MODE=DRY_RUN"),
            ("true", "true", "false", "true", 0, "REQUESTED_MODE=RECONCILE_ONLY"),
        ):
            with tempfile.TemporaryDirectory() as temporary:
                result = subprocess.run(["bash", "-c", script], cwd=workflow.parents[2],
                    env={**os.environ, "LIVE_SWITCH": live, "LIVE_PERMISSION": permission,
                         "SHADOW_ONLY": shadow, "READ_ONLY": read_only,
                         "GITHUB_ENV": str(Path(temporary) / "github-env"),
                         "GITHUB_STEP_SUMMARY": str(Path(temporary) / "summary")}, text=True, capture_output=True)
                self.assertEqual(result.returncode, expected_code, result.stderr)
                self.assertIn(label, result.stdout)

    def test_mode_persisted_in_checkpoint_and_every_audit_record(self):
        from kalshi_live_trader import LiveEngine
        for shadow in (False, True):
            engine, record, _rest, _feed = self.setup_trade(shadow=shadow)
            engine.audit("mode_evidence", ticker=record["ticker"])
            expected = "DRY_RUN" if shadow else "LIVE"
            state = json.loads(engine.state_path.read_text())
            event = json.loads(engine.ledger_path.read_text().splitlines()[-1])
            self.assertEqual(state["execution_context"]["mode"], expected)
            self.assertEqual(event["execution_mode"], expected)
            self.assertEqual(event["execution_context"]["real_order_submission_enabled"], not shadow)
            with self.assertRaisesRegex(RuntimeError, "mix live and shadow"):
                LiveEngine(engine.config, state, engine.state_path, engine.ledger_path, not shadow)

    def test_read_only_live_reconciliation_records_no_order_permission(self):
        from kalshi_live_trader import LiveEngine
        engine, _record, _rest, _feed = self.setup_trade()
        audit = LiveEngine(engine.config, engine.state, engine.state_path, engine.ledger_path, True, reconcile_only=True)
        self.assertEqual(audit.state["execution_context"]["mode"], "RECONCILE_ONLY")
        self.assertEqual(audit.state["execution_context"]["state_namespace"], "live")
        self.assertFalse(audit.state["execution_context"]["real_order_submission_enabled"])


if __name__ == "__main__":
    unittest.main()
