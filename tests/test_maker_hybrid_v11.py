from __future__ import annotations

import asyncio
import json
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from kalshi_live_trader import LiveEngine, load_config
from live_state import default_state, load_state, save_state


ROOT = Path(__file__).resolve().parents[1]


class BookFeed:
    def __init__(self, ask: str = "0.50", bid: str = "0.49", depth: str = "10.00") -> None:
        self.ask = Decimal(ask)
        self.bid = Decimal(bid)
        self.depth = Decimal(depth)
        self.trades: list[dict] = []
        self.sequence = 0

    def _quote(self, ticker: str, side: str, exit_quote: bool) -> dict:
        self.sequence += 1
        observed = time.time() + self.sequence / 1000
        yes_bid = self.bid
        yes_ask = self.ask
        economic = yes_bid if exit_quote else yes_ask
        return {
            "ticker": ticker, "side": side, "economic_price": float(economic),
            "displayed_depth": float(self.depth), "quote_id": f"q-{self.sequence}",
            "yes_bid": float(yes_bid), "yes_ask": float(yes_ask),
            "yes_bid_size": float(self.depth), "yes_ask_size": float(self.depth),
            "source_server_timestamp": datetime.fromtimestamp(observed, timezone.utc).isoformat(),
            "source_timestamp_ms": int(observed * 1000),
            "received_at": datetime.fromtimestamp(observed, timezone.utc).isoformat(),
            "quote_age_seconds": 0.0,
        }

    def executable_shadow_quote(self, ticker: str, side: str, _quantity: float, _age: float):
        return self._quote(ticker, side, False), "complete_book"

    def executable_shadow_exit_quote(self, ticker: str, side: str, _quantity: float, _age: float):
        return self._quote(ticker, side, True), "complete_book"

    def executable_asks(self, _ticker: str):
        return {"yes": float(self.ask), "no": float(self.ask)}

    def public_trades_after(self, _ticker: str, _created):
        return list(self.trades)

    def add_trade(self, side: str, price: str, count: str = "10.00") -> None:
        observed = datetime.now(timezone.utc) + timedelta(seconds=1 + len(self.trades))
        self.trades.append({
            "trade_id": f"trade-{len(self.trades) + 1}", f"{side}_price": price,
            "count": count, "source_server_timestamp": observed.isoformat(), "received_at": observed.isoformat(),
        })


class ShadowRest:
    async def balance_decimal(self):
        return Decimal("1000.00")

    async def position_for_ticker(self, _ticker):
        return Decimal("0")

    async def cancel_order(self, order, _dry_run):
        order["remaining_count"] = "0.00"
        return True


class LiveHybridRest:
    def __init__(self) -> None:
        self.position = Decimal("1.00")
        self.maker_creates = 0
        self.hard_creates = 0

    async def position_for_ticker(self, _ticker):
        return self.position

    async def create_reduce_only_maker_exit(self, **kwargs):
        self.maker_creates += 1
        return {
            "order_id": "maker-exit", "client_order_id": kwargs["client_order_id_override"],
            "held_side": kwargs["held_side"], "side": kwargs["held_side"], "exit_phase": "maker_exit",
            "order_type": "reduce_only_exit_maker", "quantity": str(kwargs["quantity"]),
            "position_price": str(kwargs["economic_exit_price"]), "fill_count": "0.00",
            "remaining_count": str(kwargs["quantity"]), "average_fill_price": None, "fees_paid": "0",
            "post_only": True, "reduce_only": True, "time_in_force": "good_till_canceled",
            "submitted_at": datetime.now(timezone.utc).isoformat(),
            "status": "resting",
        }

    async def refresh_exit_order(self, _order):
        return None

    async def cancel_order(self, order, _dry_run):
        order["remaining_count"] = "0.00"
        order["status"] = "canceled"
        return True

    async def create_reduce_only_exit(self, **kwargs):
        self.hard_creates += 1
        quantity = Decimal(str(kwargs["quantity"]))
        self.position = max(Decimal("0"), self.position - quantity)
        return {
            "order_id": f"hard-{self.hard_creates}", "client_order_id": kwargs["client_order_id_override"],
            "held_side": kwargs["held_side"], "side": kwargs["held_side"], "exit_phase": "hard_stop",
            "order_type": "reduce_only_exit_ioc", "quantity": str(quantity),
            "position_price": str(kwargs["economic_exit_price"]), "fill_count": str(quantity),
            "remaining_count": "0.00", "average_fill_price": str(kwargs["economic_exit_price"]),
            "fees_paid": "0", "post_only": False, "reduce_only": True,
            "submitted_at": datetime.now(timezone.utc).isoformat(), "status": "filled",
        }


class MakerHybridV11Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = load_config(ROOT / "selected_live_strategy.json")

    def engine(self, *, dry_run: bool = True) -> LiveEngine:
        directory = Path(tempfile.mkdtemp())
        return LiveEngine(
            dict(self.config), default_state(self.config), directory / "state.json", directory / "audit.jsonl",
            dry_run=dry_run,
        )

    def signal(self, engine: LiveEngine, ticker: str = "KXBTC15M-v11") -> dict:
        now = time.time()
        return engine.set_signal(
            {"ticker": ticker, "open_epoch": now - 1, "close_epoch": now + 899},
            {"outcome": "no", "ticker": ticker + "-prior"},
        )

    def filled_record(self, engine: LiveEngine, ticker: str = "KXBTC15M-filled") -> dict:
        record = self.signal(engine, ticker)
        record.update({
            "status": "POSITION_OPEN", "actual_quantity": "1.00",
            "initial_signal_price_cents": 50, "minimum_selected_price_cents": 50,
            "entry_orders": [{
                "entry_phase": "maker", "quantity": "1.00", "fill_count": "1.00", "remaining_count": "0.00",
                "average_fill_price": "0.49", "fees_paid": "0", "submitted_at": datetime.now(timezone.utc).isoformat(),
            }],
        })
        engine.state["active_market"] = ticker
        return record

    def test_signal_at_50_produces_49_cent_limit_and_cannot_move(self) -> None:
        async def scenario():
            engine, feed, rest = self.engine(), BookFeed("0.50"), ShadowRest()
            record = self.signal(engine)
            await engine.submit_entry(rest, feed, record, time.time())
            self.assertEqual((record["initial_signal_price_cents"], record["entry_limit_cents"]), (50, 49))
            self.assertTrue(record["entry_orders"][0]["post_only"])
            self.assertEqual(record["entry_orders"][0]["time_in_force"], "good_till_canceled")
            feed.ask = Decimal("0.60")
            await engine.submit_entry(rest, feed, record, time.time())
            self.assertEqual(record["entry_limit_cents"], 49)
            self.assertEqual(len(record["entry_orders"]), 1)
        asyncio.run(scenario())

    def test_opening_snapshot_logs_and_checkpoints_exact_prices_and_exchange_lag(self) -> None:
        engine, feed = self.engine(), BookFeed("0.52")
        record = self.signal(engine, "KXBTC15M-opening-snapshot")
        with self.assertLogs("kalshi_live_trader", level="WARNING") as captured:
            self.assertEqual(engine.freeze_initial_signal_price(feed, record, time.time()), Decimal("0.51"))
        output = "\n".join(captured.output)
        self.assertIn("OPENING ENTRY SNAPSHOT", output)
        self.assertIn("initial_selected_ask=52c", output)
        self.assertIn("entry_limit=51c", output)
        self.assertIn("quantity=1.00", output)
        self.assertIn("opening_entry_cost=$0.5100", output)
        self.assertIn("monitored=40c-49c", output)
        self.assertEqual(record["initial_signal_price_cents"], 52)
        self.assertEqual(record["entry_limit_cents"], 51)
        self.assertEqual(record["opening_entry_quantity"], "1.00")
        self.assertEqual(record["opening_entry_price"], "0.51")
        self.assertEqual(record["opening_entry_cost"], "0.5100")
        self.assertIsNotNone(record["initial_signal_price_exchange_timestamp"])
        self.assertGreaterEqual(record["initial_signal_price_lag_seconds"], 0)
        restored = load_state(engine.state_path, engine.config)
        restored_record = restored["markets"][record["ticker"]]
        self.assertEqual(restored_record["initial_signal_price_cents"], 52)
        self.assertEqual(restored_record["entry_limit_cents"], 51)
        self.assertEqual(restored_record["opening_entry_cost"], "0.5100")

    def test_legacy_v11_checkpoint_backfills_opening_cost_without_changing_entry(self) -> None:
        engine, feed = self.engine(), BookFeed("0.52")
        record = self.signal(engine, "KXBTC15M-opening-cost-backfill")
        self.assertEqual(engine.freeze_initial_signal_price(feed, record, time.time()), Decimal("0.51"))
        immutable_entry = (record["initial_signal_price_cents"], record["entry_limit_cents"])
        for key in (
            "opening_entry_quantity", "opening_entry_price", "opening_entry_cost",
            "opening_entry_cost_recorded_at",
        ):
            record.pop(key, None)

        self.assertTrue(engine.update_entry_execution_summary(record))
        self.assertEqual(
            (record["initial_signal_price_cents"], record["entry_limit_cents"]),
            immutable_entry,
        )
        self.assertEqual(record["opening_entry_quantity"], "1.00")
        self.assertEqual(record["opening_entry_price"], "0.51")
        self.assertEqual(record["opening_entry_cost"], "0.5100")

    def test_rejected_below_hard_stop_retains_initial_quote_and_analytics(self) -> None:
        engine, feed = self.engine(), BookFeed("0.44")
        record = self.signal(engine, "KXBTC15M-rejected-low")
        self.assertIsNone(engine.freeze_initial_signal_price(feed, record, time.time()))
        self.assertEqual(record["status"], "ZERO_FILL")
        self.assertEqual(record["initial_signal_price_cents"], 44)
        self.assertEqual(record["entry_limit_cents"], 43)
        self.assertEqual(record["exit_classification"], "ENTRY_NOT_FILLED")
        self.assertEqual(engine.state["entry_execution_metrics"]["zero_fill_markets"], 1)
        engine.observe_price_analytics(feed, record)
        self.assertTrue(record["shadow_entry_levels"]["49"]["touched"])
        self.assertTrue(record["shadow_entry_levels"]["44"]["touched"])
        self.assertFalse(record["shadow_entry_levels"]["43"]["touched"])

    def test_initial_price_stop_eligibility_and_actual_rejections_are_separate(self) -> None:
        engine = self.engine()
        for initial_cents in range(39, 51):
            feed = BookFeed(format(Decimal(initial_cents) / Decimal("100"), "f"))
            record = self.signal(engine, f"KXBTC15M-initial-{initial_cents}")
            engine.freeze_initial_signal_price(feed, record, time.time())

        eligibility = engine.entry_price_performance()["initial_stop_eligibility"]
        self.assertEqual(eligibility["captured_initial_prices"], 12)
        self.assertEqual(eligibility["signals_without_initial_price"], 0)
        self.assertEqual(eligibility["levels"]["40"]["initial_at_or_below_stop"], 2)
        self.assertEqual(eligibility["levels"]["45"]["initial_at_or_below_stop"], 7)
        self.assertEqual(eligibility["levels"]["49"]["initial_at_or_below_stop"], 11)
        self.assertEqual(eligibility["actual_strategy_stop_safety_rejections"], 7)
        self.assertEqual(eligibility["actual_strategy_stop_safety_rejections_40_49"], 6)
        self.assertEqual(eligibility["actual_strategy_stop_safety_rejections_below_40"], 1)
        self.assertEqual(eligibility["exact_initial_price_counts_40_49"]["44"], 1)
        self.assertEqual(eligibility["actual_rejection_initial_price_counts_40_49"]["45"], 1)
        self.assertEqual(eligibility["actual_rejection_initial_price_counts_40_49"]["46"], 0)

    def test_limit_never_fills_and_winner_is_counted_as_missed_without_sizing_change(self) -> None:
        async def scenario():
            engine, feed, rest = self.engine(), BookFeed("0.50"), ShadowRest()
            record = self.signal(engine)
            before_exponent = engine.state["sizing"].get("recovery_exponent", 0)
            before_deficit = Decimal(str(engine.state["sizing"].get("recovery_cycle_pnl", "0")))
            await engine.submit_entry(rest, feed, record, time.time())
            await engine.manage_entry(rest, feed, record, float(record["market_close_epoch"]) + 1)
            self.assertEqual(record["status"], "ZERO_FILL")
            self.assertEqual(engine.state["sizing"].get("recovery_exponent", 0), before_exponent)
            self.assertEqual(Decimal(str(engine.state["sizing"].get("recovery_cycle_pnl", "0"))), before_deficit)
            engine.finalize_settlement_analytics(record, record["signal_side"])
            row = engine.entry_price_performance()["levels"]["49"]
            self.assertEqual(row["eventual_winners"], 1)
            self.assertEqual(row["missed_winner_count"], 1)
        asyncio.run(scenario())

    def test_full_and_partial_entry_fills_use_public_trade_through_only(self) -> None:
        async def scenario():
            engine, feed, rest = self.engine(), BookFeed("0.50"), ShadowRest()
            record = self.signal(engine)
            await engine.submit_entry(rest, feed, record, time.time())
            feed.add_trade(record["signal_side"], "0.49", "0.40")
            await engine.manage_entry(rest, feed, record, time.time())
            self.assertEqual(record["status"], "ENTRY_PARTIAL")
            self.assertEqual(record["actual_quantity"], "0.40")
            self.assertEqual(record["requested_entry_cost"], "0.4900")
            self.assertEqual(record["actual_entry_notional"], "0.1960")
            self.assertEqual(record["actual_entry_fees"], "0")
            self.assertEqual(record["actual_entry_cash_cost"], "0.1960")
            feed.add_trade(record["signal_side"], "0.48", "0.60")
            await engine.manage_entry(rest, feed, record, time.time())
            self.assertEqual(record["status"], "POSITION_OPEN")
            self.assertEqual(record["actual_quantity"], "1.00")
            self.assertEqual(record["actual_entry_notional"], "0.4900")
            self.assertEqual(record["actual_entry_cash_cost"], "0.4900")
        asyncio.run(scenario())

    def test_canceled_unfilled_order_is_not_a_loss(self) -> None:
        async def scenario():
            engine, feed, rest = self.engine(), BookFeed("0.50"), ShadowRest()
            record = self.signal(engine)
            await engine.submit_entry(rest, feed, record, time.time())
            exponent = engine.state["sizing"].get("recovery_exponent", 0)
            await engine.manage_entry(rest, feed, record, float(record["market_close_epoch"]) + 1)
            self.assertEqual(record["exit_classification"], "ENTRY_NOT_FILLED")
            self.assertEqual(engine.state["sizing"].get("recovery_exponent", 0), exponent)
        asyncio.run(scenario())

    def test_gtc_entry_does_not_expire_after_old_sixty_second_window(self) -> None:
        async def scenario():
            engine, feed, rest = self.engine(), BookFeed("0.50"), ShadowRest()
            record = self.signal(engine)
            await engine.submit_entry(rest, feed, record, time.time())
            order = record["entry_orders"][0]
            self.assertIsNone(order["expiration_time"])
            self.assertEqual(order["time_in_force"], "good_till_canceled")

            await engine.manage_entry(
                rest, feed, record, float(record["market_open_epoch"]) + 61,
            )
            self.assertEqual(record["status"], "ENTRY_PENDING")
            self.assertEqual(order["remaining_count"], "1.00")

            await engine.manage_entry(rest, feed, record, float(record["market_close_epoch"]) + 1)
            self.assertEqual(record["status"], "ZERO_FILL")
            self.assertEqual(record["exit_classification"], "ENTRY_NOT_FILLED")
            self.assertEqual(order["remaining_count"], "0.00")
        asyncio.run(scenario())

    def test_shadow_levels_winner_50_to_46_has_exact_nested_hits(self) -> None:
        engine, feed = self.engine(), BookFeed("0.50")
        record = self.signal(engine)
        engine.freeze_initial_signal_price(feed, record, time.time())
        for price in ("0.49", "0.48", "0.47", "0.46"):
            feed.ask = Decimal(price)
            engine.observe_price_analytics(feed, record)
        engine.finalize_settlement_analytics(record, record["signal_side"])
        levels = record["shadow_entry_levels"]
        self.assertTrue(all(levels[str(level)]["touched"] for level in (46, 47, 48, 49)))
        self.assertTrue(all(not levels[str(level)]["touched"] for level in range(40, 46)))
        self.assertEqual(record["winner_max_drawdown_cents"], 4)

    def test_winner_exactly_40_and_below_40_are_classified(self) -> None:
        engine, feed = self.engine(), BookFeed("0.50")
        first = self.signal(engine, "KXBTC15M-at40")
        engine.freeze_initial_signal_price(feed, first, time.time())
        feed.ask = Decimal("0.40")
        engine.observe_price_analytics(feed, first)
        engine.finalize_settlement_analytics(first, first["signal_side"])
        feed.ask = Decimal("0.50")
        second = self.signal(engine, "KXBTC15M-below40")
        engine.freeze_initial_signal_price(feed, second, time.time())
        feed.ask = Decimal("0.39")
        engine.observe_price_analytics(feed, second)
        engine.finalize_settlement_analytics(second, second["signal_side"])
        summary = engine.entry_price_performance()
        self.assertEqual(summary["winning_settlements_reached_40_or_lower"], 2)
        self.assertTrue(first["shadow_entry_levels"]["40"]["touched"])

    def test_loser_touches_all_levels_without_entering_winner_denominator(self) -> None:
        engine, feed = self.engine(), BookFeed("0.50")
        record = self.signal(engine)
        engine.freeze_initial_signal_price(feed, record, time.time())
        feed.ask = Decimal("0.40")
        engine.observe_price_analytics(feed, record)
        loser = "no" if record["signal_side"] == "yes" else "yes"
        engine.finalize_settlement_analytics(record, loser)
        self.assertTrue(all(row["touched"] for row in record["shadow_entry_levels"].values()))
        self.assertEqual(engine.entry_price_performance()["winning_settlements"], 0)

    def test_winner_drawdown_50_to_43_and_50_to_50(self) -> None:
        engine, feed = self.engine(), BookFeed("0.50")
        first = self.signal(engine, "KXBTC15M-dd7")
        engine.freeze_initial_signal_price(feed, first, time.time())
        feed.ask = Decimal("0.43")
        engine.observe_price_analytics(feed, first)
        engine.finalize_settlement_analytics(first, first["signal_side"])
        feed.ask = Decimal("0.50")
        second = self.signal(engine, "KXBTC15M-dd0")
        engine.freeze_initial_signal_price(feed, second, time.time())
        engine.observe_price_analytics(feed, second)
        engine.finalize_settlement_analytics(second, second["signal_side"])
        self.assertEqual(first["winner_max_drawdown_cents"], 7)
        self.assertEqual(second["winner_max_drawdown_cents"], 0)
        self.assertEqual(engine.entry_price_performance()["winner_drawdown_histogram_cents"], {"0": 1, "7": 1})

    def test_winner_drawdown_and_40_to_49_path_are_durable_in_state_and_audit(self) -> None:
        engine, feed = self.engine(), BookFeed("0.50")
        record = self.signal(engine, "KXBTC15M-durable-winner-path")
        engine.freeze_initial_signal_price(feed, record, time.time())
        feed.ask = Decimal("0.43")
        engine.observe_price_analytics(feed, record)
        engine.finalize_settlement_analytics(record, record["signal_side"])

        restored = load_state(engine.state_path, engine.config)
        durable = restored["markets"][record["ticker"]]
        self.assertEqual(durable["initial_signal_price_cents"], 50)
        self.assertEqual(durable["minimum_selected_price_cents"], 43)
        self.assertEqual(durable["winner_max_drawdown_cents"], 7)
        self.assertTrue(durable["shadow_entry_levels"]["49"]["touched"])
        self.assertFalse(durable["shadow_entry_levels"]["42"]["touched"])

        events = [json.loads(line) for line in engine.ledger_path.read_text().splitlines()]
        finalized = [event for event in events if event["event"] == "settlement_analytics_finalized"][-1]
        self.assertEqual(finalized["initial_signal_price_cents"], 50)
        self.assertEqual(finalized["minimum_selected_price_cents"], 43)
        self.assertEqual(finalized["winner_max_drawdown_cents"], 7)
        self.assertTrue(finalized["shadow_entry_levels"]["49"]["touched"])

    def test_lowest_actual_entry_for_eventual_and_realized_winners_is_separate(self) -> None:
        engine = self.engine()
        stopped = self.filled_record(engine, "KXBTC15M-eventual-winner-41")
        stopped.update({
            "actual_average_entry_price": "0.41", "actual_quantity": "2.17",
            "realized_method": "stop", "realized_net_pnl": "-0.0868",
        })
        engine.finalize_settlement_analytics(stopped, stopped["signal_side"])
        profitable = self.filled_record(engine, "KXBTC15M-profitable-winner-42")
        profitable.update({
            "actual_average_entry_price": "0.42", "actual_quantity": "2.42",
            "realized_method": "settlement", "realized_net_pnl": "1.4036",
        })
        engine.finalize_settlement_analytics(profitable, profitable["signal_side"])

        summary = engine.entry_price_performance()
        self.assertEqual(summary["actual_filled_eventual_winners"], 2)
        self.assertEqual(
            summary["lowest_actual_entry_eventual_winner"]["actual_average_entry_price"], "0.41",
        )
        self.assertEqual(summary["realized_profitable_filled_trades"], 1)
        self.assertEqual(
            summary["lowest_actual_entry_realized_profitable"]["actual_average_entry_price"], "0.42",
        )

    def test_hybrid_maker_exit_full(self) -> None:
        async def scenario():
            engine, feed, rest = self.engine(), BookFeed("0.50", "0.45", "10"), ShadowRest()
            record = self.filled_record(engine)
            await engine.manage_stop(rest, feed, record)
            self.assertEqual(record["status"], "MAKER_EXIT_PENDING")
            feed.bid = Decimal("0.46")
            await engine.manage_stop(rest, feed, record)
            self.assertEqual(record["status"], "CLOSED")
            self.assertEqual(record["exit_classification"], "MAKER_EXIT_FULL")
        asyncio.run(scenario())

    def test_hybrid_hard_stop_only_and_partial_then_hard_residual(self) -> None:
        async def scenario():
            engine, feed, rest = self.engine(), BookFeed("0.50", "0.45", "10"), ShadowRest()
            hard = self.filled_record(engine, "KXBTC15M-hard")
            await engine.manage_stop(rest, feed, hard)
            feed.bid = Decimal("0.44")
            await engine.manage_stop(rest, feed, hard)
            self.assertEqual(hard["exit_classification"], "HARD_STOP_ONLY")
            engine2, feed2 = self.engine(), BookFeed("0.50", "0.45", "0.40")
            partial = self.filled_record(engine2, "KXBTC15M-partial-hard")
            await engine2.manage_stop(rest, feed2, partial)
            feed2.bid = Decimal("0.46")
            await engine2.manage_stop(rest, feed2, partial)
            self.assertEqual(partial["status"], "MAKER_EXIT_PARTIAL")
            feed2.bid, feed2.depth = Decimal("0.44"), Decimal("10")
            await engine2.manage_stop(rest, feed2, partial)
            self.assertEqual(partial["exit_classification"], "MAKER_EXIT_PARTIAL_THEN_HARD_STOP")
            self.assertEqual(partial["exit_orders"][-1]["quantity"], "0.60")
        asyncio.run(scenario())

    def test_price_recovers_before_hard_stop_and_later_fills_maker(self) -> None:
        async def scenario():
            engine, feed, rest = self.engine(), BookFeed("0.50", "0.45", "10"), ShadowRest()
            record = self.filled_record(engine)
            await engine.manage_stop(rest, feed, record)
            feed.bid = Decimal("0.45")
            await engine.manage_stop(rest, feed, record)
            self.assertEqual(record["status"], "MAKER_EXIT_PENDING")
            feed.bid = Decimal("0.46")
            await engine.manage_stop(rest, feed, record)
            self.assertEqual(record["exit_classification"], "MAKER_EXIT_FULL")
        asyncio.run(scenario())

    def test_duplicate_ticks_do_not_submit_duplicate_stop(self) -> None:
        async def scenario():
            engine, feed, rest = self.engine(), BookFeed("0.50", "0.45"), ShadowRest()
            record = self.filled_record(engine)
            await engine.manage_stop(rest, feed, record)
            await engine.manage_stop(rest, feed, record)
            self.assertEqual(len([o for o in record["exit_orders"] if o.get("exit_phase") == "maker_exit"]), 1)
        asyncio.run(scenario())

    def test_restart_restores_pending_maker_exit_state(self) -> None:
        async def scenario():
            directory = Path(tempfile.mkdtemp())
            engine = LiveEngine(self.config, default_state(self.config), directory / "state", directory / "audit", True)
            feed, rest = BookFeed("0.50", "0.45"), ShadowRest()
            record = self.filled_record(engine)
            await engine.manage_stop(rest, feed, record)
            save_state(engine.state_path, engine.state)
            restored = load_state(engine.state_path, self.config)
            resumed = LiveEngine(self.config, restored, engine.state_path, engine.ledger_path, True)
            self.assertEqual(resumed.state["markets"][record["ticker"]]["status"], "MAKER_EXIT_PENDING")
            self.assertEqual(len(resumed.state["markets"][record["ticker"]]["exit_orders"]), 1)
        asyncio.run(scenario())

    def test_closed_stop_continues_path_tracking_and_false_stop_analysis(self) -> None:
        async def scenario():
            engine, feed, rest = self.engine(), BookFeed("0.50", "0.45", "10"), ShadowRest()
            record = self.filled_record(engine)
            await engine.manage_stop(rest, feed, record)
            feed.bid, feed.ask = Decimal("0.44"), Decimal("0.44")
            await engine.manage_stop(rest, feed, record)
            self.assertEqual(record["status"], "CLOSED")
            feed.ask = Decimal("0.40")
            engine.observe_price_analytics(feed, record)
            engine.finalize_settlement_analytics(record, record["signal_side"])
            self.assertTrue(record["stopped_then_eventual_winner"])
            self.assertTrue(record["shadow_entry_levels"]["40"]["touched"])
            self.assertGreater(Decimal(record["hypothetical_lost_settlement_profit_due_to_stop"]), 0)
        asyncio.run(scenario())

    def test_shadow_and_mock_live_hard_stop_have_identical_strategy_transition(self) -> None:
        async def scenario():
            shadow, live = self.engine(), self.engine(dry_run=False)
            shadow_record = self.filled_record(shadow, "KXBTC15M-parity-shadow")
            live_record = self.filled_record(live, "KXBTC15M-parity-live")
            shadow_feed, live_feed = BookFeed("0.50", "0.45", "10"), BookFeed("0.50", "0.45", "10")
            shadow_rest, live_rest = ShadowRest(), LiveHybridRest()
            await shadow.manage_stop(shadow_rest, shadow_feed, shadow_record)
            await live.manage_stop(live_rest, live_feed, live_record)
            shadow_feed.bid = live_feed.bid = Decimal("0.44")
            await shadow.manage_stop(shadow_rest, shadow_feed, shadow_record)
            await live.manage_stop(live_rest, live_feed, live_record)
            self.assertEqual(shadow_record["status"], live_record["status"])
            self.assertEqual(shadow_record["exit_classification"], live_record["exit_classification"])
            self.assertEqual(shadow.local_remaining_position(shadow_record), live.local_remaining_position(live_record))
        asyncio.run(scenario())

    def test_analytics_separates_touch_from_simulated_fill(self) -> None:
        engine, feed = self.engine(), BookFeed("0.50")
        record = self.signal(engine)
        engine.freeze_initial_signal_price(feed, record, time.time())
        feed.ask = Decimal("0.49")
        engine.observe_price_analytics(feed, record)
        self.assertTrue(record["shadow_entry_levels"]["49"]["touched"])
        self.assertFalse(record["shadow_entry_levels"]["49"]["simulated_fill"])
        feed.add_trade(record["signal_side"], "0.49", "1")
        engine.observe_price_analytics(feed, record)
        self.assertTrue(record["shadow_entry_levels"]["49"]["simulated_fill"])

    def test_retired_taker_entry_paths_are_unreachable_and_fail_closed(self) -> None:
        source = (ROOT / "kalshi_live_trader.py").read_text()
        self.assertIn("retired v10 market-fallback entry path is disabled", source)
        self.assertIn("retired v10 immediate-market entry path is disabled", source)
        self.assertNotIn("await self.submit_market_fallback(", source)
        self.assertNotIn("await self.submit_immediate_market_entry(", source)


if __name__ == "__main__":
    unittest.main()
