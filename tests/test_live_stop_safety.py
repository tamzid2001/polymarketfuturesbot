"""Offline fault injection: no credentials and no network/order endpoints."""
from __future__ import annotations

import json
import unittest
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

from kalshi_btc15m_average_down import KalshiREST, record_submission_failure, order_fee_total
from kalshi_live_trader import LIVE_STOP_SAFETY_CONTRACT_VERSION, POSITION_CAP_CONTRACT_VERSION
from tests import test_maker_hybrid_v11 as fixtures

BookFeed = fixtures.BookFeed
LiveHybridRest = fixtures.LiveHybridRest


class LiveStopSafetyTests(unittest.IsolatedAsyncioTestCase):
    def test_production_safety_contract_versions_prevent_old_worker_semantics(self):
        self.assertEqual(LIVE_STOP_SAFETY_CONTRACT_VERSION, 3)
        self.assertEqual(POSITION_CAP_CONTRACT_VERSION, 2)

    def setup_trade(self, *, shadow=False):
        fixture = fixtures.MakerHybridV11Tests()
        fixture.setUp()
        fixture.config.update(
            stop_policy="hybrid_maker_then_hard_stop",
            hybrid_stop_trigger_cents=51, hybrid_maker_exit_cents=52,
            hybrid_hard_stop_cents=50, stop_price="0.50",
        )
        engine = fixture.engine(dry_run=shadow)
        record = fixture.filled_record(engine)
        record["entry_orders"][0]["average_fill_price"] = "0.54"
        rest = LiveHybridRest()
        rest.refresh_order = AsyncMock()
        return engine, record, rest, BookFeed("0.54", "0.51", "10")

    def setup_direct_trade(self, *, shadow=False):
        engine, record, rest, feed = self.setup_trade(shadow=shadow)
        engine.config.update(
            stop_policy="direct_ioc_at_trigger",
            hybrid_stop_trigger_cents=51,
            hybrid_maker_exit_cents=51,
            hybrid_hard_stop_cents=51,
            stop_price="0.51",
        )
        record.update(stop_policy="direct_ioc_at_trigger", stop_floor_price="0.51")
        record["hybrid_stop"].update(trigger_cents=51, maker_exit_cents=51, hard_stop_cents=51)
        return engine, record, rest, feed

    async def test_v13_direct_exit_has_no_maker_phase_and_flattens_at_trigger(self):
        for shadow in (False, True):
            engine, record, rest, feed = self.setup_direct_trade(shadow=shadow)
            await engine.manage_stop(rest, feed, record)
            self.assertEqual(rest.maker_creates, 0)
            self.assertEqual(len(record["exit_orders"]), 1)
            self.assertEqual(record["exit_orders"][0]["time_in_force"], "immediate_or_cancel")
            self.assertTrue(record["exit_orders"][0]["reduce_only"])
            self.assertEqual(record["status"], "CLOSED")
            self.assertEqual(record["exit_classification"], "DIRECT_PROTECTIVE_EXIT")

    async def test_v13_direct_exit_does_not_trigger_above_51c(self):
        engine, record, rest, feed = self.setup_direct_trade()
        feed.bid = Decimal("0.52")
        await engine.manage_stop(rest, feed, record)
        self.assertEqual(rest.maker_creates, 0)
        self.assertEqual(rest.hard_creates, 0)
        self.assertEqual(record["status"], "POSITION_OPEN")

    async def test_v13_direct_partial_ioc_retries_only_authoritative_residual(self):
        engine, record, rest, feed = self.setup_direct_trade()
        original = rest.create_reduce_only_exit

        async def partial_then_full(**kwargs):
            if rest.hard_creates == 0:
                rest.hard_creates += 1
                requested = Decimal(str(kwargs["quantity"]))
                filled = Decimal("0.40")
                rest.position = requested - filled
                return {
                    "order_id": "direct-partial",
                    "client_order_id": kwargs["client_order_id_override"],
                    "held_side": kwargs["held_side"],
                    "side": kwargs["held_side"],
                    "exit_phase": "hard_stop",
                    "order_type": "reduce_only_exit_ioc",
                    "quantity": str(requested),
                    "position_price": str(kwargs["economic_exit_price"]),
                    "fill_count": str(filled),
                    "remaining_count": "0.60",
                    "average_fill_price": str(kwargs["economic_exit_price"]),
                    "fees_paid": "0",
                    "post_only": False,
                    "reduce_only": True,
                    "submission_outcome": "accepted",
                    "status": "partial",
                }
            return await original(**kwargs)

        rest.create_reduce_only_exit = partial_then_full
        rest.refresh_exit_order = AsyncMock(return_value=True)
        await engine.manage_stop(rest, feed, record)
        self.assertEqual(record["status"], "HARD_STOP_PENDING")
        self.assertEqual(rest.maker_creates, 0)
        self.assertEqual(rest.position, Decimal("0.60"))
        await engine.manage_stop(rest, feed, record)
        self.assertEqual(
            [Decimal(str(row["quantity"])) for row in record["exit_orders"]],
            [Decimal("1.00"), Decimal("0.60")],
        )
        self.assertEqual(rest.position, Decimal("0"))
        self.assertEqual(record["status"], "CLOSED")
        self.assertEqual(record["exit_classification"], "DIRECT_PROTECTIVE_EXIT")

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

    async def test_maker_trigger_is_durably_latched_before_entry_cancel_confirmation(self):
        engine, record, rest, feed = self.setup_trade()
        async def cancel_unconfirmed(*_args, **_kwargs):
            engine.transition(record, "ENTRY_CANCEL_UNCONFIRMED", "entry_cancellation_unconfirmed")
            return False
        engine.cancel_entry_orders_and_confirm = AsyncMock(side_effect=cancel_unconfirmed)
        feed.bid = Decimal("0.51")
        await engine.manage_stop(rest, feed, record)
        self.assertEqual(record["status"], "ENTRY_CANCEL_UNCONFIRMED")
        self.assertTrue(record["hybrid_stop"]["stop_exit_latched"])
        self.assertIn("at", record["stop_trigger"])
        saved = json.loads(engine.state_path.read_text())["markets"][record["ticker"]]
        self.assertTrue(saved["hybrid_stop"]["stop_exit_latched"])
        self.assertIn("at", saved["stop_trigger"])

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

    async def test_partial_hard_ioc_reconciles_position_and_retries_only_residual(self):
        engine, record, rest, feed = self.setup_trade()
        feed.bid = Decimal("0.50")
        original = rest.create_reduce_only_exit

        async def partial_then_full(**kwargs):
            if rest.hard_creates == 0:
                rest.hard_creates += 1
                requested = Decimal(str(kwargs["quantity"]))
                filled = Decimal("0.40")
                rest.position = requested - filled
                return {
                    "order_id": "hard-partial", "client_order_id": kwargs["client_order_id_override"],
                    "held_side": kwargs["held_side"], "side": kwargs["held_side"],
                    "exit_phase": "hard_stop", "order_type": "reduce_only_exit_ioc",
                    "quantity": str(requested), "position_price": str(kwargs["economic_exit_price"]),
                    "fill_count": str(filled),
                    # V2 can report the final unfilled/canceled IOC remainder.
                    # It is not a resting order and must not block the next
                    # authoritative-residual IOC.
                    "remaining_count": "0.60", "average_fill_price": str(kwargs["economic_exit_price"]),
                    "fees_paid": "0", "post_only": False, "reduce_only": True,
                    "submission_outcome": "accepted", "status": "partial",
                }
            return await original(**kwargs)

        rest.create_reduce_only_exit = partial_then_full
        rest.refresh_exit_order = AsyncMock(return_value=True)
        await engine.manage_stop(rest, feed, record)
        self.assertEqual(record["status"], "HARD_STOP_PENDING")
        self.assertEqual(rest.position, Decimal("0.60"))
        await engine.manage_stop(rest, feed, record)
        self.assertEqual([Decimal(str(row["quantity"])) for row in record["exit_orders"]],
                         [Decimal("1.00"), Decimal("0.60")])
        self.assertEqual(rest.position, Decimal("0"))
        self.assertEqual(record["status"], "CLOSED")

    async def test_partial_entry_cancel_404_reconciles_then_hard_exits_exact_position(self):
        engine, record, rest, feed = self.setup_trade()
        record.update(status="ENTRY_PARTIAL", actual_quantity="0.40")
        entry = record["entry_orders"][0]
        entry.update({
            "order_id": "partial-entry", "client_order_id": "partial-entry-client",
            "ticker": record["ticker"], "quantity": "1.00", "fill_count": "0.40",
            "remaining_count": "0.60", "average_fill_price": "0.54",
            "routing_exchange_index": 2, "status": "partially_filled",
        })
        rest.position = Decimal("0.40")
        rest.cancel_order = AsyncMock(return_value=False)

        async def raw(path, _params):
            if path == "/portfolio/orders":
                # Complete current-order result: the raced entry is no longer
                # resting and therefore cannot add exposure after the exit.
                return {"orders": [], "cursor": ""}
            if path == "/portfolio/fills":
                return {"fills": [{
                    "ticker": record["ticker"], "order_id": "partial-entry",
                    "client_order_id": "partial-entry-client", "fill_id": "entry-fill",
                    "count_fp": "0.40", "yes_price_dollars": "0.54",
                    "fee_cost_dollars": "0", "action": "buy",
                }], "cursor": ""}
            raise AssertionError(path)

        rest.get_raw_json = raw
        feed.bid = Decimal("0.50")
        await engine.manage_stop(rest, feed, record)
        self.assertEqual(rest.hard_creates, 1)
        self.assertEqual(Decimal(str(record["exit_orders"][-1]["quantity"])), Decimal("0.40"))
        self.assertEqual(rest.position, Decimal("0"))
        self.assertEqual(record["status"], "CLOSED")
        self.assertTrue(entry["cancel_reconciled_after_failure"])
        self.assertEqual(engine.protective_exit_health()["entry_cancel_races_reconciled"], 1)

    async def test_maker_exit_cancel_404_reconciles_then_exits_only_residual(self):
        engine, record, rest, feed = self.setup_trade()
        await engine.manage_stop(rest, feed, record)
        maker = record["exit_orders"][0]
        maker.update(fill_count="0.40", remaining_count="0.60", average_fill_price="0.52")
        rest.position = Decimal("0.60")
        rest.cancel_order = AsyncMock(return_value=False)

        async def raw(path, _params):
            if path == "/portfolio/orders":
                return {"orders": [], "cursor": ""}
            if path == "/portfolio/fills":
                return {"fills": [{
                    "ticker": record["ticker"], "order_id": maker["order_id"],
                    "client_order_id": maker["client_order_id"], "fill_id": "maker-fill",
                    "count_fp": "0.40", "yes_price_dollars": "0.52",
                    "fee_cost_dollars": "0", "action": "sell",
                }], "cursor": ""}
            raise AssertionError(path)

        rest.get_raw_json = raw
        feed.bid = Decimal("0.50")
        await engine.manage_stop(rest, feed, record)
        self.assertEqual(rest.hard_creates, 1)
        self.assertEqual(Decimal(str(record["exit_orders"][-1]["quantity"])), Decimal("0.60"))
        self.assertEqual(rest.position, Decimal("0"))
        self.assertEqual(record["status"], "CLOSED")
        self.assertTrue(maker["cancel_reconciled_after_failure"])
        self.assertEqual(engine.protective_exit_health()["maker_cancel_races_reconciled"], 1)

    async def test_latched_stop_is_not_silently_booked_as_settlement_while_position_exists(self):
        engine, record, rest, _feed = self.setup_trade()
        record["market_close_epoch"] = 1
        record["status"] = "HARD_STOP_PENDING"
        record["hybrid_stop"]["hard_stop_latched"] = True
        rest.position = Decimal("1.00")
        rest.get_market = AsyncMock(return_value={"ticker": record["ticker"], "result": "no"})
        await engine.settle(rest, record, 2)
        self.assertNotIn("realized_net_pnl", record)
        self.assertEqual(
            record["hard_stop_exit_deadline_incident"]["status"],
            "UNFLATTENED_AFTER_MARKET_CLOSE",
        )
        self.assertEqual(engine.protective_exit_health()["pending_flatten"], 1)

    async def test_flat_at_settlement_after_latched_stop_is_classified_as_exit_failure(self):
        engine, record, rest, _feed = self.setup_trade()
        record["market_close_epoch"] = 1
        record["status"] = "HARD_STOP_PENDING"
        record["hybrid_stop"].update(stop_exit_latched=True, hard_stop_latched=True)
        rest.position = Decimal("0")
        rest.get_market = AsyncMock(return_value={"ticker": record["ticker"], "result": "no"})
        await engine.settle(rest, record, 2)
        self.assertEqual(record["status"], "CLOSED")
        self.assertEqual(record["realized_method"], "settlement")
        self.assertEqual(record["exit_classification"], "HARD_STOP_EXIT_FAILURE_SETTLEMENT_LOSS")
        self.assertTrue(record["protective_exit_failed_before_settlement"])
        health = engine.protective_exit_health()
        self.assertEqual(health["settled_after_protective_exit_latch"], 1)
        self.assertEqual(health["settled_after_hard_stop_latch"], 1)

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

    def test_rolling_performance_uses_realized_net_per_actual_share(self):
        engine, _record, _rest, _feed = self.setup_trade()
        engine.state["markets"] = {
            "win": {
                "ticker": "win", "signal_side": "yes", "settlement_outcome": "yes",
                "actual_quantity": "1.00", "actual_average_entry_price": "0.55",
                "realized_net_pnl": "0.40", "completed_at": "2026-01-01T00:00:00+00:00",
                "entry_orders": [{"fees_paid": "0.01"}], "exit_orders": [],
            },
            "loss": {
                "ticker": "loss", "signal_side": "yes", "settlement_outcome": "no",
                "actual_quantity": "2.00", "actual_average_entry_price": "0.52",
                "realized_net_pnl": "-0.20", "completed_at": "2026-01-01T00:15:00+00:00",
                "entry_orders": [], "exit_orders": [{"fees_paid": "0.02"}],
            },
        }
        metrics = engine.realized_performance_metrics()["all"]
        self.assertEqual((metrics["realized_wins"], metrics["realized_losses"]), (1, 1))
        self.assertEqual(Decimal(metrics["realized_win_rate"]), Decimal("0.5"))
        self.assertEqual(Decimal(metrics["average_net_win_per_share"]), Decimal("0.4"))
        self.assertEqual(Decimal(metrics["average_net_loss_per_share"]), Decimal("0.1"))
        self.assertEqual(Decimal(metrics["average_actual_gain_cents_per_share"]), Decimal("40.0"))
        self.assertEqual(Decimal(metrics["average_actual_loss_cents_per_share"]), Decimal("10.0"))
        self.assertEqual(Decimal(metrics["fee_adjusted_break_even_win_rate"]), Decimal("0.2"))
        self.assertEqual(Decimal(metrics["win_rate_edge"]), Decimal("0.3"))
        self.assertEqual(Decimal(metrics["quantity_weighted_average_entry"]), Decimal("0.53"))
        self.assertEqual(Decimal(metrics["total_realized_net_pnl"]), Decimal("0.20"))
        self.assertEqual(Decimal(metrics["total_fees_paid"]), Decimal("0.03"))
        self.assertEqual((metrics["directional_wins"], metrics["directional_losses"]), (1, 1))

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
        block = text.split("id: live_gate", 1)[1].split("        run: |\n", 1)[1].split("\n      - ", 1)[0]
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
