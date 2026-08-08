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
            "displayed_depth": float(self.depth), "quote_id": "test-book",
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
            await engine.submit_entry(EntryRest(), Feed("0.40"), record, 1_001)
            self.assertEqual(record["status"], "ZERO_FILL")
            self.assertEqual(engine.state["sizing"].get("recovery_exponent", 0), 0)
            engine = self.engine("0.01")
            record = engine.set_signal({"ticker": "KXBTC15M-current2", "open_epoch": 1_000, "close_epoch": 1_900}, {"outcome": "yes", "ticker": "KXBTC15M-prior"})
            rest = EntryRest("0.01")
            await engine.submit_entry(rest, Feed("0.60"), record, 1_001)
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
                    {"trade_id": "a", "yes_price": .50, "no_price": .50, "count": .30},
                    {"trade_id": "b", "yes_price": .51, "no_price": .49, "count": .20},
                ]
        engine = self.engine()
        record = {
            "ticker": "KXBTC15M-current", "signal_side": "yes", "intended_quantity": "1.00",
            "entry_orders": [{"quantity": "1.00", "submitted_at": datetime.now(timezone.utc).isoformat(), "fill_count": "0"}],
        }
        filled = engine.refresh_shadow_entry(TradeFeed(), record)
        self.assertEqual(filled, Decimal("0.30"))
        self.assertEqual(record["entry_orders"][0]["shadow_fill_evidence"]["model"], "conservative_trade_through")
        self.assertEqual(record["actual_quantity"], "0.30")
        self.assertEqual(engine.shadow_metrics()["reserved_cash"], "0.1500")

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
            feed.ask = "0.40"
            await engine.manage_entry(rest, feed, record, 1_015)
            self.assertEqual(record["status"], "ZERO_FILL")
            self.assertEqual(record["market_fallback"]["reason"], "best_available_price_at_or_below_stop")
            self.assertEqual(engine.state["sizing"].get("recovery_exponent", 0), 0)
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
