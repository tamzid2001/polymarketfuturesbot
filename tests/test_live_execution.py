from __future__ import annotations

import asyncio
import tempfile
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from kalshi_live_trader import (
    LiveEngine, ProvisionalOutcomeTracker, QuoteObservation, deterministic_client_order_id, load_config,
)
from live_state import default_state


ROOT = Path(__file__).resolve().parents[1]


class Feed:
    def __init__(self, ask: str = "0.60", bid: str = "0.45", depth: str = "10.00") -> None:
        self.ask = ask
        self.bid = bid
        self.depth = depth

    def executable_asks(self, ticker: str):
        return {"yes": float(self.ask), "no": float(self.ask)}

    def executable_shadow_exit_quote(self, ticker: str, side: str, _quantity: float, _age: float):
        return {"economic_price": float(self.bid)}, "test"

    def executable_shadow_quote(self, ticker: str, side: str, _quantity: float, _age: float):
        return {
            "ticker": ticker, "side": side, "economic_price": float(self.ask),
            "displayed_depth": float(self.depth), "quote_id": f"test-book:{self.ask}:{self.bid}",
            "yes_bid": float(self.bid), "yes_ask": float(self.ask),
            "yes_bid_size": float(self.depth), "yes_ask_size": float(self.depth),
            "source_server_timestamp": "test", "source_timestamp_ms": 1,
            "received_at": "test", "quote_age_seconds": 0.01,
        }, "executable_top_of_book"

    def public_trades_after(self, _ticker: str, _created):
        return []


class EntryRest:
    def __init__(self, balance: str = "100.00") -> None:
        self.balance = Decimal(balance)
        self.created = 0

    async def balance_decimal(self):
        return self.balance

    async def position_for_ticker(self, _ticker: str):
        return Decimal("0")

    async def create_order(self, **_kwargs):
        self.created += 1
        return {"order_id": "order-1", "fill_count": "0", "remaining_count": "1.00", "fees_paid": "0"}

    async def cancel_order(self, order, _dry_run):
        order["remaining_count"] = "0.00"


class LiveFallbackRest(EntryRest):
    def __init__(self) -> None:
        super().__init__("100.00")
        self.calls = []

    async def create_order(self, **kwargs):
        self.calls.append(kwargs)
        self.created += 1
        fill = "1.00" if kwargs["tif"] == "immediate_or_cancel" else "0.00"
        remaining = "0.00" if fill != "0.00" else "1.00"
        return {
            "order_id": f"order-{self.created}", "fill_count": fill, "remaining_count": remaining,
            "average_fill_price": kwargs["position_price"], "fees_paid": "0",
        }

    async def refresh_order(self, _order):
        return None


class LiveExecutionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = load_config(ROOT / "live_strategy_config.json")

    def engine(self, shadow_balance: str = "1000.00") -> LiveEngine:
        temporary = Path(tempfile.mkdtemp())
        config = dict(self.config)
        config["starting_shadow_balance"] = shadow_balance
        return LiveEngine(config, default_state(config), temporary / "state.json", temporary / "audit.jsonl", dry_run=True)

    def test_provisional_yes_and_no_and_inverse_signal(self) -> None:
        tracker = ProvisionalOutcomeTracker(Decimal("0.99"), 5, 2)
        boundary = 10_000.0
        tracker.observations["prior"] = [QuoteObservation("prior", boundary - .2, boundary - .2, Decimal(".99"), Decimal(".01"))]
        yes = tracker.infer("prior", boundary)
        self.assertEqual(yes and yes["outcome"], "yes")
        tracker.observations["prior"] = [QuoteObservation("prior", boundary - .2, boundary - .2, Decimal(".01"), Decimal(".99"))]
        no = tracker.infer("prior", boundary)
        self.assertEqual(no and no["outcome"], "no")
        engine = self.engine()
        record = engine.set_signal({"ticker": "KXBTC15M-current", "open_epoch": boundary, "close_epoch": boundary + 900}, yes)
        self.assertEqual(record["signal_side"], "no")

    def test_provisional_rejects_stale_missing_and_conflicting_quotes(self) -> None:
        tracker = ProvisionalOutcomeTracker(Decimal(".99"), 5, 2)
        boundary = 1_000.0
        tracker.observations["prior"] = [QuoteObservation("prior", boundary - 3, boundary - 3, Decimal(".99"), Decimal(".01"))]
        self.assertIsNone(tracker.infer("prior", boundary))
        tracker.observations["prior"] = [QuoteObservation("prior", boundary - .2, boundary - .2, Decimal(".98"), Decimal(".02"))]
        self.assertIsNone(tracker.infer("prior", boundary))
        tracker.observations["prior"] = [QuoteObservation("prior", boundary - .2, boundary - .2, Decimal(".99"), Decimal(".99"))]
        self.assertIsNone(tracker.infer("prior", boundary))

    def test_duplicate_signal_cannot_create_duplicate_market_entry(self) -> None:
        engine = self.engine()
        market = {"ticker": "KXBTC15M-current", "open_epoch": 1, "close_epoch": 901}
        signal = {"outcome": "yes", "ticker": "KXBTC15M-prior"}
        first = engine.set_signal(market, signal)
        second = engine.set_signal(market, signal)
        self.assertIs(first, second)
        self.assertEqual(len(engine.state["markets"]), 1)
        self.assertEqual(
            deterministic_client_order_id("KXBTC15M-current", "no", "entry", self.config),
            deterministic_client_order_id("KXBTC15M-current", "no", "entry", self.config),
        )

    def test_no_entry_at_or_under_stop_and_no_capital_downsize(self) -> None:
        async def scenario() -> None:
            engine = self.engine()
            record = engine.set_signal({"ticker": "KXBTC15M-current", "open_epoch": 1_000, "close_epoch": 1_900}, {"outcome": "yes", "ticker": "KXBTC15M-prior"})
            feed = Feed("0.40")
            await engine.submit_entry(EntryRest(), feed, record, 1_001)
            await engine.submit_entry(EntryRest(), feed, record, 1_003)
            self.assertEqual(record["status"], "ZERO_FILL")
            self.assertEqual(engine.state["sizing"].get("recovery_exponent", 0), 0)
            engine = self.engine("0.01")
            record = engine.set_signal({"ticker": "KXBTC15M-current2", "open_epoch": 1_000, "close_epoch": 1_900}, {"outcome": "yes", "ticker": "KXBTC15M-prior"})
            rest = EntryRest("0.01")
            feed = Feed("0.60")
            await engine.submit_entry(rest, feed, record, 1_001)
            await engine.submit_entry(rest, feed, record, 1_003)
            self.assertEqual(record["status"], "FUNDING_FAILURE")
            self.assertEqual(rest.created, 0)
        asyncio.run(scenario())

    def test_shadow_account_starts_at_1000_and_tracks_realized_drawdown(self) -> None:
        engine = self.engine()
        metrics = engine.shadow_metrics()
        self.assertEqual(metrics["starting_balance"], "1000.00")
        self.assertEqual(metrics["balance"], "1000.00")
        record = {
            "ticker": "KXBTC15M-current", "actual_quantity": "1.00", "signal_side": "yes",
            "entry_orders": [{"fill_count": "1.00", "average_fill_price": "0.50", "fees_paid": "0"}],
            "status": "POSITION_OPEN", "exit_orders": [],
        }
        engine.state["active_market"] = record["ticker"]
        engine.record_realized(record, Decimal("-0.10"), "stop", "KXBTC15M-current:stop:1")
        metrics = engine.shadow_metrics()
        self.assertEqual(metrics["balance"], "999.90")
        self.assertEqual(metrics["max_drawdown"], "0.10")
        self.assertEqual(metrics["completed_trades"], 1)
        self.assertEqual(metrics["stop_count"], 1)

    def test_settlement_realized_pnl_updates_once_and_uses_actual_quantity(self) -> None:
        engine = self.engine()
        record = {
            "ticker": "KXBTC15M-current", "actual_quantity": "1.50", "signal_side": "yes", "entry_orders": [
                {"fill_count": "1.50", "average_fill_price": "0.50", "fees_paid": "0"}
            ], "status": "POSITION_OPEN", "exit_orders": [],
        }
        engine.state["active_market"] = record["ticker"]
        engine.record_realized(record, Decimal(".75"), "settlement", "KXBTC15M-current:settlement:yes")
        self.assertEqual(record["realized_net_pnl"], "0.75")
        completed = engine.state["sizing"]["completed_trade_count"]
        engine.record_realized(record, Decimal(".75"), "settlement", "KXBTC15M-current:settlement:yes")
        self.assertEqual(engine.state["sizing"]["completed_trade_count"], completed)

    def test_shadow_fill_is_labeled_conservative_trade_through_evidence(self) -> None:
        class TradeFeed:
            def public_trades_after(self, _ticker, _created):
                return [
                    {"trade_id": "a", "yes_price": .49, "no_price": .51, "count": .30},
                    {"trade_id": "b", "yes_price": .50, "no_price": .50, "count": .20},
                ]
        engine = self.engine()
        record = {
            "ticker": "KXBTC15M-current", "signal_side": "yes", "intended_quantity": "1.00",
            "entry_orders": [{"quantity": "1.00", "position_price": "0.49", "submitted_at": datetime.now(timezone.utc).isoformat(), "fill_count": "0"}],
        }
        filled = engine.refresh_shadow_entry(TradeFeed(), record)
        self.assertEqual(filled, Decimal("0.30"))
        self.assertEqual(record["entry_orders"][0]["shadow_fill_evidence"]["model"], "conservative_trade_through")
        self.assertEqual(record["actual_quantity"], "0.30")
        self.assertEqual(engine.shadow_metrics()["reserved_cash"], "0.1470")
        self.assertEqual(record["entry_execution_type"], "maker_limit")
        self.assertEqual(record["entry_execution_summary"]["maker_limit_filled_quantity"], "0.30")
        self.assertEqual(record["entry_execution_summary"]["market_ioc_filled_quantity"], "0")

    def test_entry_execution_summary_separates_maker_ioc_and_mixed_fills(self) -> None:
        engine = self.engine()
        record = {
            "ticker": "KXBTC15M-execution-summary",
            "entry_orders": [
                {
                    "entry_phase": "maker", "order_id": "maker-1", "fill_count": "0.30",
                    "average_fill_price": "0.51",
                },
                {
                    "entry_phase": "market_fallback", "order_id": "ioc-1", "fill_count": "0.70",
                    "average_fill_price": "0.55",
                },
            ],
        }
        self.assertTrue(engine.update_entry_execution_summary(record))
        summary = record["entry_execution_summary"]
        self.assertEqual(record["entry_execution_type"], "mixed")
        self.assertTrue(summary["maker_limit_filled"])
        self.assertTrue(summary["market_ioc_filled"])
        self.assertEqual(summary["maker_limit_filled_quantity"], "0.30")
        self.assertEqual(summary["maker_limit_average_fill_price"], "0.51")
        self.assertEqual(summary["market_ioc_filled_quantity"], "0.70")
        self.assertEqual(summary["market_ioc_average_fill_price"], "0.55")
        self.assertEqual(summary["actual_weighted_average_entry_price"], "0.538")
        self.assertEqual(summary["maker_limit_order_ids"], ["maker-1"])
        self.assertEqual(summary["market_ioc_order_ids"], ["ioc-1"])

    def test_fifteen_second_maker_expiry_uses_one_price_protected_ioc_fallback(self) -> None:
        async def scenario() -> None:
            config = dict(self.config)
            directory = Path(tempfile.mkdtemp())
            engine = LiveEngine(config, default_state(config), directory / "state", directory / "ledger", dry_run=False)
            rest = LiveFallbackRest()
            feed = Feed("0.60")
            record = engine.set_signal(
                {"ticker": "KXBTC15M-fallback", "open_epoch": 1_000, "close_epoch": 1_900},
                {"outcome": "yes", "ticker": "KXBTC15M-prior"},
            )
            await engine.submit_entry(rest, feed, record, 1_000)
            await engine.submit_entry(rest, feed, record, 1_003)
            self.assertEqual(record["entry_deadline_epoch"], 1_015)
            self.assertTrue(rest.calls[0]["post_only"])
            self.assertEqual(rest.calls[0]["tif"], "good_till_canceled")
            await engine.manage_entry(rest, feed, record, 1_015)
            self.assertEqual(len(rest.calls), 2)
            self.assertFalse(rest.calls[1]["post_only"])
            self.assertEqual(rest.calls[1]["tif"], "immediate_or_cancel")
            self.assertEqual(rest.calls[1]["position_price"], 0.60)
            self.assertEqual(record["status"], "POSITION_OPEN")
            self.assertEqual(record["actual_quantity"], "1.00")
            self.assertTrue(record["market_fallback_attempted"])
            self.assertEqual(record["entry_execution_type"], "market_ioc")
            self.assertEqual(record["entry_execution_summary"]["maker_limit_filled_quantity"], "0")
            self.assertEqual(record["entry_execution_summary"]["market_ioc_filled_quantity"], "1.00")
            self.assertEqual(record["entry_execution_summary"]["market_ioc_average_fill_price"], "0.6")
        asyncio.run(scenario())

    def test_market_fallback_refuses_a_40c_or_lower_selected_side(self) -> None:
        async def scenario() -> None:
            engine = self.engine()
            rest = EntryRest()
            feed = Feed("0.60")
            record = engine.set_signal(
                {"ticker": "KXBTC15M-no-fallback", "open_epoch": 1_000, "close_epoch": 1_900},
                {"outcome": "yes", "ticker": "KXBTC15M-prior"},
            )
            await engine.submit_entry(rest, feed, record, 1_000)
            await engine.submit_entry(rest, feed, record, 1_003)
            feed.ask = "0.40"
            await engine.manage_entry(rest, feed, record, 1_015)
            self.assertEqual(record["status"], "ZERO_FILL")
            self.assertEqual(record["market_fallback"]["reason"], "best_available_price_at_or_below_stop")
            self.assertEqual(engine.state["sizing"].get("recovery_exponent", 0), 0)
        asyncio.run(scenario())

    def test_opening_maximum_derives_one_cent_lower_dynamic_maker_and_records_window(self) -> None:
        async def scenario() -> None:
            directory = Path(tempfile.mkdtemp())
            engine = LiveEngine(dict(self.config), default_state(self.config), directory / "state", directory / "ledger", dry_run=False)
            rest = LiveFallbackRest()
            feed = Feed("0.52", "0.47")
            record = engine.set_signal(
                {"ticker": "KXBTC15M-opening-max", "open_epoch": 1_000, "close_epoch": 1_900},
                {"outcome": "no", "ticker": "KXBTC15M-prior"},
            )
            await engine.submit_entry(rest, feed, record, 1_000.2)
            self.assertEqual(rest.calls, [])
            feed.ask = "0.54"
            await engine.submit_entry(rest, feed, record, 1_001.7)
            await engine.submit_entry(rest, feed, record, 1_003.1)
            self.assertEqual(record["opening_price_discovery"]["maximum_selected_best_ask"], "0.54")
            self.assertEqual(record["maker_entry_price"], "0.53")
            self.assertEqual(rest.calls[0]["position_price"], 0.53)
            samples = record["opening_quote_observations"]
            self.assertEqual(len(samples), 2)
            self.assertEqual(samples[0]["yes_bid"], "0.47")
            self.assertEqual(samples[0]["yes_ask"], "0.52")
            self.assertEqual(samples[0]["no_bid"], "0.48")
            self.assertEqual(samples[0]["no_ask"], "0.53")
            # Later opening observations are retained for calibration, but
            # cannot rewrite the immutable price derived from the first three
            # seconds once the maker order has been submitted.
            feed.ask = "0.70"
            engine.capture_opening_quote(feed, record, 1_004)
            self.assertEqual(record["maker_entry_price"], "0.53")
            self.assertEqual(Decimal(record["opening_quote_observations"][-1]["selected_best_ask"]), Decimal("0.70"))
            self.assertEqual(Decimal(record["opening_quote_capture"]["max_selected_best_ask"]), Decimal("0.70"))
            engine.capture_opening_quote(feed, record, 1_015)
            self.assertIsNotNone(record["opening_quote_capture"]["completed_at"])
        asyncio.run(scenario())

    def test_opening_maximum_one_cent_below_at_stop_is_a_zero_fill(self) -> None:
        async def scenario() -> None:
            engine = self.engine()
            rest = EntryRest()
            feed = Feed("0.41")
            record = engine.set_signal(
                {"ticker": "KXBTC15M-opening-at-stop", "open_epoch": 1_000, "close_epoch": 1_900},
                {"outcome": "no", "ticker": "KXBTC15M-prior"},
            )
            await engine.submit_entry(rest, feed, record, 1_000.1)
            await engine.submit_entry(rest, feed, record, 1_003)
            self.assertEqual(record["opening_price_discovery"]["maximum_selected_best_ask"], "0.41")
            self.assertEqual(record["opening_price_discovery"]["derived_maker_entry_price"], "0.40")
            self.assertEqual(record["status"], "ZERO_FILL")
            self.assertEqual(rest.created, 0)
            self.assertEqual(engine.state["sizing"].get("recovery_exponent", 0), 0)
        asyncio.run(scenario())

    def test_stop_uses_actual_entry_above_50c_but_never_moves_below_40c(self) -> None:
        async def scenario() -> None:
            class SettlementRest(EntryRest):
                async def get_market(self, _ticker):
                    return {"result": "yes"}

            engine = self.engine()
            rest = SettlementRest()
            high_entry = engine.set_signal(
                {"ticker": "KXBTC15M-high-entry", "open_epoch": 1_000, "close_epoch": 1_900},
                {"outcome": "no", "ticker": "KXBTC15M-prior"},
            )
            high_entry.update({
                "status": "POSITION_OPEN", "actual_quantity": "1.00",
                "entry_orders": [{"fill_count": "1.00", "average_fill_price": "0.52", "fees_paid": "0"}],
            })
            engine.state["active_market"] = high_entry["ticker"]
            await engine.manage_stop(rest, Feed("0.60", "0.41"), high_entry)
            self.assertEqual(high_entry["actual_average_entry_price"], "0.52")
            self.assertEqual(high_entry["effective_stop_price"], "0.42")
            self.assertEqual(high_entry["stop_trigger"]["effective_stop_price"], "0.42")
            self.assertEqual(high_entry["post_entry_stop_monitor"]["minimum_executable_bid"], "0.41")
            self.assertEqual(high_entry["status"], "CLOSED")
            completed = engine.state["sizing"]["completed_trade_count"]
            await engine.settle(rest, high_entry, 1_901)
            self.assertEqual(high_entry["post_stop_settlement_outcome"], "yes")
            self.assertTrue(high_entry["post_stop_would_have_settled_correctly"])
            self.assertEqual(engine.state["sizing"]["completed_trade_count"], completed)

            low_entry = engine.set_signal(
                {"ticker": "KXBTC15M-low-entry", "open_epoch": 2_000, "close_epoch": 2_900},
                {"outcome": "no", "ticker": "KXBTC15M-prior-2"},
            )
            low_entry.update({
                "status": "POSITION_OPEN", "actual_quantity": "1.00",
                "entry_orders": [{"fill_count": "1.00", "average_fill_price": "0.49", "fees_paid": "0"}],
            })
            engine.state["active_market"] = low_entry["ticker"]
            await engine.manage_stop(rest, Feed("0.60", "0.41"), low_entry)
            self.assertEqual(low_entry["effective_stop_price"], "0.40")
            self.assertEqual(low_entry["status"], "POSITION_OPEN")
        asyncio.run(scenario())

    def test_handoff_only_permits_the_middle_thirteen_minutes_without_pending_operations(self) -> None:
        engine = self.engine()
        engine.markets = [{"ticker": "KXBTC15M-current", "open_epoch": 1_000, "close_epoch": 1_900, "status": "active"}]
        engine.state["markets"]["KXBTC15M-current"] = {"ticker": "KXBTC15M-current", "status": "POSITION_OPEN"}
        self.assertFalse(engine.handoff_ready(1_059)[0])
        self.assertTrue(engine.handoff_ready(1_060)[0])
        self.assertFalse(engine.handoff_ready(1_841)[0])
        engine.state["markets"]["KXBTC15M-current"]["status"] = "ENTRY_PENDING"
        self.assertFalse(engine.handoff_ready(1_300)[0])


if __name__ == "__main__":
    unittest.main()
