from __future__ import annotations

import asyncio
import tempfile
import time
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from kalshi_live_trader import LiveEngine, _iso_epoch, load_config
from live_state import default_state, load_state


ROOT = Path(__file__).resolve().parents[1]


def iso(value: float) -> str:
    return datetime.fromtimestamp(value, timezone.utc).isoformat()


class BandFeed:
    """Deterministic opening reference plus a later complete executable book."""

    def __init__(self, opened: float, opening_ask: int, current_ask: int, *, side: str = "yes") -> None:
        self.opened = opened
        self.opening_ask = opening_ask
        self.current_ask = current_ask
        self.current_epoch = opened + 60.1
        self.side = side
        self.trades: list[dict] = []

    @staticmethod
    def _price(cents: int) -> float:
        return float(Decimal(cents) / Decimal(100))

    def first_post_open_price_quote(self, _ticker: str, _side: str, _opened: float):
        observed = self.opened + 0.1
        return {
            "economic_price": self._price(self.opening_ask),
            "selected_best_bid": self._price(max(1, self.opening_ask - 1)),
            "selected_component_epoch": observed,
            "source_server_timestamp": iso(observed),
            "source_timestamp_ms": int(observed * 1000),
            "received_at": iso(observed),
            "quote_id": "opening-price",
            "coverage_complete_from_market_open": True,
            "coverage_status": "COMPLETE_TEST_PREOPEN_SUBSCRIPTION",
            "displayed_depth_available": False,
            "source": "test_opening_price_only",
        }, "complete"

    def executable_shadow_quote(self, ticker: str, side: str, _quantity: float, _age: float):
        ask = self._price(self.current_ask)
        bid = self._price(max(1, self.current_ask - 1))
        return {
            "ticker": ticker,
            "side": side,
            "economic_price": ask,
            "displayed_depth": 100.0,
            "quote_id": f"book-{self.current_epoch}-{self.current_ask}",
            "yes_bid": bid,
            "yes_ask": ask,
            "yes_bid_size": 100.0,
            "yes_ask_size": 100.0,
            "source_server_timestamp": iso(self.current_epoch),
            "source_timestamp_ms": int(self.current_epoch * 1000),
            "received_at": iso(self.current_epoch),
            "quote_age_seconds": 0.0,
        }, "complete_book"

    def executable_asks(self, _ticker: str):
        price = self._price(self.current_ask)
        return {"yes": price, "no": price}

    def public_trades_after(self, _ticker: str, created: datetime):
        return [
            dict(row) for row in self.trades
            if datetime.fromisoformat(str(row["source_server_timestamp"])).timestamp() > created.timestamp()
        ]


class EntryRest:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def balance_decimal(self):
        return Decimal("150.00")

    async def market_entry_funding(self, _ticker: str):
        return 2, await self.balance_decimal()

    async def position_for_ticker(self, _ticker: str):
        return Decimal("0")

    async def create_order(self, **kwargs):
        self.calls.append(dict(kwargs))
        return {
            "order_id": "live-v12-entry",
            "client_order_id": kwargs["client_order_id_override"],
            "quantity": str(kwargs["quantity"]),
            "position_price": str(kwargs["position_price"]),
            "fill_count": "0.00",
            "remaining_count": str(kwargs["quantity"]),
            "fees_paid": "0",
            "post_only": kwargs["post_only"],
            "time_in_force": kwargs["tif"],
            "status": "resting",
        }


class DelayedBandV13Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = load_config(ROOT / "selected_live_strategy.json")

    def engine(self, *, dry_run: bool = True) -> LiveEngine:
        directory = Path(tempfile.mkdtemp())
        return LiveEngine(
            dict(self.config), default_state(self.config),
            directory / "state.json", directory / "audit.jsonl", dry_run=dry_run,
        )

    @staticmethod
    def signal(engine: LiveEngine, opened: float, ticker: str) -> dict:
        return engine.set_signal(
            {"ticker": ticker, "open_epoch": opened, "close_epoch": opened + 900},
            {"outcome": "no", "ticker": ticker + "-prior"},
        )

    def test_exact_production_contract_is_shadow_safe_by_default(self) -> None:
        self.assertEqual(self.config["strategy_version"], "kxbtc15m-delayed-band-live-v13")
        self.assertEqual(self.config["entry_execution_mode"], "delayed_threshold_band_maker")
        self.assertEqual(Decimal(self.config["recovery_multiplier"]), Decimal("2.50"))
        self.assertEqual(Decimal(self.config["starting_base"]), Decimal("1.00"))
        self.assertEqual(Decimal(self.config["max_position"]), Decimal("100.00"))
        self.assertEqual(
            (self.config["hybrid_stop_trigger_cents"], self.config["hybrid_maker_exit_cents"],
             self.config["hybrid_hard_stop_cents"]),
            (51, 51, 51),
        )
        self.assertEqual(self.config["stop_policy"], "direct_ioc_at_trigger")
        self.assertFalse(self.config["live_enabled"])
        self.assertTrue(self.config["dry_run"])
        self.assertEqual(self.config["trading_mode"], "shadow")

    def test_waits_until_sixty_seconds_then_submits_one_gtc_post_only_limit(self) -> None:
        async def scenario():
            opened = time.time() - 61
            engine = self.engine()
            feed = BandFeed(opened, 52, 53)
            rest = EntryRest()
            record = self.signal(engine, opened, "KXBTC15M-v12-entry")
            await engine.submit_entry(rest, feed, record, opened + 59.9)
            self.assertEqual(record["status"], "SIGNAL_PENDING")
            self.assertEqual(record["entry_orders"], [])
            await engine.submit_entry(rest, feed, record, opened + 60.2)
            self.assertEqual(record["status"], "ENTRY_PENDING")
            self.assertEqual(record["opening_price_reference"]["selected_side_ask_cents"], 52)
            self.assertEqual(record["initial_signal_price_cents"], 53)
            self.assertEqual(record["entry_limit_cents"], 52)
            self.assertEqual(record["opening_entry_cost"], "0.5200")
            order = record["entry_orders"][0]
            self.assertTrue(order["post_only"])
            self.assertEqual(order["time_in_force"], "good_till_canceled")
            await engine.submit_entry(rest, feed, record, opened + 61)
            self.assertEqual(len(record["entry_orders"]), 1)
        asyncio.run(scenario())

    def test_opening_at_threshold_is_filtered_without_touching_recovery(self) -> None:
        async def scenario():
            opened = time.time() - 61
            engine = self.engine()
            before = dict(engine.state["sizing"])
            record = self.signal(engine, opened, "KXBTC15M-v12-opening-filter")
            await engine.submit_entry(EntryRest(), BandFeed(opened, 53, 53), record, opened + 60.2)
            self.assertEqual(record["status"], "ENTRY_FILTERED")
            self.assertEqual(record["delayed_entry_decision"]["status"], "INELIGIBLE_OPENING")
            self.assertEqual(record["entry_orders"], [])
            self.assertEqual(engine.state["sizing"], before)
        asyncio.run(scenario())

    def test_first_qualifying_limit_above_ceiling_is_terminal(self) -> None:
        async def scenario():
            opened = time.time() - 61
            engine = self.engine()
            feed = BandFeed(opened, 51, 59)
            record = self.signal(engine, opened, "KXBTC15M-v12-ceiling")
            await engine.submit_entry(EntryRest(), feed, record, opened + 60.2)
            self.assertEqual(record["status"], "ENTRY_FILTERED")
            self.assertEqual(record["delayed_entry_decision"]["limit_price_cents"], 58)
            feed.current_ask = 54
            feed.current_epoch = opened + 61
            await engine.submit_entry(EntryRest(), feed, record, opened + 61)
            self.assertEqual(record["entry_orders"], [])
            self.assertEqual(record["delayed_entry_decision"]["limit_price_cents"], 58)
        asyncio.run(scenario())

    def test_live_and_shadow_freeze_the_same_order_price(self) -> None:
        async def scenario():
            opened = time.time() - 61
            feed = BandFeed(opened, 48, 58)
            shadow = self.engine(dry_run=True)
            shadow_record = self.signal(shadow, opened, "KXBTC15M-v12-parity")
            await shadow.submit_entry(EntryRest(), feed, shadow_record, opened + 60.2)

            live = self.engine(dry_run=False)
            live_record = self.signal(live, opened, "KXBTC15M-v12-parity")
            rest = EntryRest()
            await live.submit_entry(rest, feed, live_record, opened + 60.2)
            self.assertEqual(shadow_record["entry_limit_cents"], live_record["entry_limit_cents"])
            self.assertEqual(shadow_record["intended_quantity"], live_record["intended_quantity"])
            self.assertEqual(rest.calls[0]["position_price"], 0.57)
            self.assertEqual(rest.calls[0]["tif"], "good_till_canceled")
            self.assertTrue(rest.calls[0]["post_only"])
        asyncio.run(scenario())

    def test_restart_preserves_frozen_order_and_cannot_duplicate_it(self) -> None:
        async def scenario():
            opened = time.time() - 61
            engine = self.engine()
            feed = BandFeed(opened, 50, 54)
            ticker = "KXBTC15M-v12-restart"
            record = self.signal(engine, opened, ticker)
            await engine.submit_entry(EntryRest(), feed, record, opened + 60.2)
            restored = load_state(engine.state_path, self.config)
            resumed = LiveEngine(
                dict(self.config), restored, engine.state_path, engine.ledger_path, dry_run=True,
            )
            existing = resumed.state["markets"][ticker]
            await resumed.submit_entry(EntryRest(), feed, existing, opened + 61)
            self.assertEqual(existing["status"], "ENTRY_PENDING")
            self.assertEqual(len(existing["entry_orders"]), 1)
            self.assertEqual(existing["entry_limit_cents"], 53)
        asyncio.run(scenario())

    def test_millisecond_fill_timestamp_parser_preserves_delayed_stop_timing(self) -> None:
        seconds = 1_788_802_700.125
        self.assertAlmostEqual(_iso_epoch(int(seconds * 1000)), seconds, places=3)
        self.assertAlmostEqual(_iso_epoch(str(int(seconds * 1000))), seconds, places=3)
        self.assertAlmostEqual(_iso_epoch(iso(seconds)), seconds, places=3)


if __name__ == "__main__":
    unittest.main()
