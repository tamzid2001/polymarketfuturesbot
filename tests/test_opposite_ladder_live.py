from __future__ import annotations

import asyncio
import tempfile
import time
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from kalshi_live_trader import LiveEngine, load_config
from live_state import default_state


ROOT = Path(__file__).resolve().parents[1]


class OppositeFeed:
    def __init__(self, opened: float) -> None:
        self.opened = opened
        self.observed = opened + 60.2
        self.asks = {"no": Decimal("0.53"), "yes": Decimal("0.48")}
        self.bids = {"no": Decimal("0.52"), "yes": Decimal("0.47")}
        self.trades: list[dict] = []

    def executable_shadow_quote(self, ticker, side, _quantity, _age):
        stamp = datetime.fromtimestamp(self.observed, timezone.utc).isoformat()
        return {
            "ticker": ticker, "side": side, "economic_price": float(self.asks[side]),
            "displayed_depth": 100.0, "quote_id": f"ask:{side}:{self.observed}",
            "source_server_timestamp": stamp,
            "source_timestamp_ms": int(self.observed * 1000), "received_at": stamp,
        }, "complete"

    def executable_shadow_exit_quote(self, ticker, side, _quantity, _age):
        stamp = datetime.fromtimestamp(self.observed, timezone.utc).isoformat()
        return {
            "ticker": ticker, "side": side, "economic_price": float(self.bids[side]),
            "displayed_depth": 100.0, "quote_id": f"bid:{side}:{self.observed}",
            "source_server_timestamp": stamp,
            "source_timestamp_ms": int(self.observed * 1000), "received_at": stamp,
        }, "complete"

    def executable_asks(self, _ticker):
        return {key: float(value) for key, value in self.asks.items()}

    def public_trades_after(self, _ticker, created):
        return [
            row for row in self.trades
            if datetime.fromisoformat(row["source_server_timestamp"]).timestamp() > created.timestamp()
        ]


class Rest:
    def __init__(self) -> None:
        self.calls: list[dict] = []
        self.position = Decimal("0")

    async def market_entry_funding(self, _ticker):
        return 2, Decimal("120")

    async def position_for_ticker(self, _ticker):
        return self.position

    async def create_order(self, **kwargs):
        self.calls.append(kwargs)
        return {
            "order_id": f"order-{len(self.calls)}",
            "client_order_id": kwargs["client_order_id_override"],
            "status": "resting", "submission_outcome": "accepted",
            "fill_count": "0", "remaining_count": str(kwargs["quantity"]),
            "average_fill_price": None, "fees_paid": "0",
            "time_in_force": kwargs["tif"], "post_only": kwargs["post_only"],
        }

    async def cancel_order(self, order, _dry_run):
        order["remaining_count"] = "0"
        order["status"] = "canceled"
        return True


class RejectInitialOnceRest(Rest):
    def __init__(self) -> None:
        super().__init__()
        self.initial_rejected = False

    async def create_order(self, **kwargs):
        self.calls.append(kwargs)
        if kwargs["order_key"] == "opposite-ladder-initial_minus_offset" and not self.initial_rejected:
            self.initial_rejected = True
            return {
                "order_id": None, "status": "submit_failed",
                "submission_outcome": "rejected", "http_status": 400,
                "error_code": "temporary_test_rejection", "fill_count": "0",
                "remaining_count": str(kwargs["quantity"]), "fees_paid": "0",
            }
        return {
            "order_id": f"order-{len(self.calls)}",
            "client_order_id": kwargs["client_order_id_override"],
            "status": "resting", "submission_outcome": "accepted",
            "fill_count": "0", "remaining_count": str(kwargs["quantity"]),
            "average_fill_price": None, "fees_paid": "0",
            "time_in_force": kwargs["tif"], "post_only": kwargs["post_only"],
        }


class PartialExitRest(Rest):
    def __init__(self) -> None:
        super().__init__()
        self.exit_calls: list[dict] = []

    async def refresh_order(self, _order):
        return True

    async def refresh_exit_order(self, _order):
        return True

    async def create_reduce_only_exit(self, **kwargs):
        self.exit_calls.append(kwargs)
        requested = Decimal(str(kwargs["quantity"]))
        filled = min(self.position, Decimal("1.00")) if len(self.exit_calls) == 1 else self.position
        self.position -= filled
        return {
            "order_id": f"exit-{len(self.exit_calls)}",
            "client_order_id": kwargs["client_order_id_override"],
            "status": "executed", "submission_outcome": "accepted",
            "fill_count": format(filled, "f"),
            "remaining_count": format(requested - filled, "f"),
            "average_fill_price": str(kwargs["economic_exit_price"]),
            "fees_paid": "0", "time_in_force": "immediate_or_cancel",
            "reduce_only": True,
        }


class OppositeLadderLiveTests(unittest.TestCase):
    def setUp(self):
        self.config = load_config(ROOT / "selected_live_strategy.json")

    def engine(self, *, dry_run=True):
        root = Path(tempfile.mkdtemp())
        return LiveEngine(
            self.config, default_state(self.config), root / "state.json",
            root / "audit.jsonl", dry_run=dry_run,
        )

    def signal(self, engine, opened):
        # Prior YES seeds sticky NO; v14 therefore trades the opposite YES.
        return engine.set_signal(
            {"ticker": "KXBTC15M-v14", "open_epoch": opened, "close_epoch": opened + 900},
            {"outcome": "yes", "ticker": "KXBTC15M-prior"},
        )

    def test_live_submits_exact_five_opposite_gtc_orders_once(self):
        async def scenario():
            opened = time.time() - 61
            engine, feed, rest = self.engine(dry_run=False), OppositeFeed(opened), Rest()
            record = self.signal(engine, opened)
            await engine.submit_entry(rest, feed, record, opened + 60.3)
            self.assertEqual(record["sticky_signal_side"], "no")
            self.assertEqual(record["trade_side"], "yes")
            self.assertEqual(
                [(round(call["position_price"] * 100), call["quantity"]) for call in rest.calls],
                [(47, 1.0), (40, 2.0), (30, 4.0), (20, 8.0), (10, 16.0)],
            )
            self.assertTrue(all(call["tif"] == "good_till_canceled" for call in rest.calls))
            self.assertTrue(all(call["post_only"] for call in rest.calls))
            await engine.submit_entry(rest, feed, record, opened + 61)
            self.assertEqual(len(rest.calls), 5)
        asyncio.run(scenario())

    def test_shadow_51c_bid_cancels_unfilled_rungs_and_exits_only_fills(self):
        async def scenario():
            opened = time.time() - 61
            engine, feed, rest = self.engine(), OppositeFeed(opened), Rest()
            record = self.signal(engine, opened)
            await engine.submit_entry(rest, feed, record, opened + 60.3)
            trade_epoch = time.time() + 1
            trade_time = datetime.fromtimestamp(trade_epoch, timezone.utc).isoformat()
            feed.trades.append({
                "trade_id": "sweep-1", "yes_price": "0.39", "no_price": "0.61",
                "count": "3.00", "source_server_timestamp": trade_time,
            })
            feed.observed = trade_epoch
            await engine.manage_entry(rest, feed, record, trade_epoch)
            self.assertEqual(Decimal(record["actual_quantity"]), Decimal("3.00"))
            feed.bids["yes"] = Decimal("0.51")
            await engine.manage_stop(rest, feed, record)
            self.assertTrue(record["opposite_ladder"]["exit_latched"])
            self.assertEqual(record["status"], "CLOSED")
            self.assertEqual(record["realized_method"], "opposite_take_profit")
            self.assertEqual(sum(Decimal(row["fill_count"]) for row in record["exit_orders"]), Decimal("3.00"))
            self.assertTrue(all(Decimal(row["remaining_count"]) == 0 for row in record["entry_orders"]))
        asyncio.run(scenario())

    def test_sticky_51c_ask_also_latches_cancel_and_flatten(self):
        async def scenario():
            opened = time.time() - 61
            engine, feed, rest = self.engine(), OppositeFeed(opened), Rest()
            record = self.signal(engine, opened)
            await engine.submit_entry(rest, feed, record, opened + 60.3)
            trade_epoch = time.time() + 1
            feed.trades.append({
                "trade_id": "sweep-sticky-boundary", "yes_price": "0.46",
                "no_price": "0.54", "count": "1.00",
                "source_server_timestamp": datetime.fromtimestamp(
                    trade_epoch, timezone.utc,
                ).isoformat(),
            })
            feed.observed = trade_epoch
            await engine.manage_entry(rest, feed, record, trade_epoch)
            self.assertEqual(Decimal(record["actual_quantity"]), Decimal("1.00"))
            feed.asks["no"] = Decimal("0.51")
            feed.bids["yes"] = Decimal("0.50")
            await engine.manage_stop(rest, feed, record)
            self.assertTrue(record["opposite_ladder"]["exit_latched"])
            self.assertEqual(record["opposite_ladder"]["trigger_sticky_ask_cents"], 51)
            self.assertEqual(record["status"], "CLOSED")
            self.assertEqual(
                sum(Decimal(row["fill_count"]) for row in record["exit_orders"]),
                Decimal("1.00"),
            )
        asyncio.run(scenario())

    def test_base_two_multiplies_all_orders(self):
        async def scenario():
            config = dict(self.config)
            config["starting_base"] = "2.00"
            root = Path(tempfile.mkdtemp())
            engine = LiveEngine(config, default_state(config), root / "s", root / "a", dry_run=False)
            opened = time.time() - 61
            rest = Rest()
            await engine.submit_entry(rest, OppositeFeed(opened), self.signal(engine, opened), opened + 60.3)
            self.assertEqual([call["quantity"] for call in rest.calls], [2.0, 4.0, 8.0, 16.0, 32.0])
        asyncio.run(scenario())

    def test_rejected_initial_retries_without_delaying_other_gtc_rungs(self):
        async def scenario():
            opened = time.time() - 61
            engine, feed, rest = self.engine(dry_run=False), OppositeFeed(opened), RejectInitialOnceRest()
            record = self.signal(engine, opened)
            first_pass = opened + 60.3
            await engine.submit_entry(rest, feed, record, first_pass)
            self.assertEqual(
                [call["order_key"] for call in rest.calls],
                [
                    "opposite-ladder-initial_minus_offset", "opposite-ladder-rung_40",
                    "opposite-ladder-rung_30", "opposite-ladder-rung_20",
                    "opposite-ladder-rung_10",
                ],
            )
            await engine.submit_entry(rest, feed, record, first_pass + 0.5)
            self.assertEqual(len(rest.calls), 5)
            await engine.submit_entry(rest, feed, record, first_pass + 1.1)
            self.assertEqual(len(rest.calls), 6)
            self.assertEqual(rest.calls[-1]["order_key"], "opposite-ladder-initial_minus_offset")
            accepted_roles = {
                order["ladder_role"] for order in record["entry_orders"] if order.get("order_id")
            }
            self.assertEqual(
                accepted_roles,
                {"initial_minus_offset", "rung_40", "rung_30", "rung_20", "rung_10"},
            )
            self.assertFalse(engine.state["circuit_breaker"]["blocked"])
        asyncio.run(scenario())

    def test_live_partial_ioc_retries_authoritative_residual_until_flat(self):
        async def scenario():
            opened = time.time() - 61
            engine, feed, rest = self.engine(dry_run=False), OppositeFeed(opened), PartialExitRest()
            record = self.signal(engine, opened)
            await engine.submit_entry(rest, feed, record, opened + 60.3)
            record["entry_orders"][0].update(fill_count="1.00", average_fill_price="0.47")
            record["entry_orders"][1].update(fill_count="2.00", average_fill_price="0.40")
            rest.position = Decimal("3.00")
            feed.bids["yes"] = Decimal("0.51")
            await engine.manage_stop(rest, feed, record)
            self.assertEqual(rest.position, Decimal("2.00"))
            self.assertEqual(record["status"], "HARD_STOP_PENDING")
            await engine.manage_stop(rest, feed, record)
            self.assertEqual(rest.position, Decimal("0.00"))
            self.assertEqual(record["status"], "CLOSED")
            self.assertEqual([Decimal(str(call["quantity"])) for call in rest.exit_calls], [Decimal("3.00"), Decimal("2.00")])
            self.assertTrue(all(call["order_key"].startswith("direct-protective-exit") for call in rest.exit_calls))
            self.assertEqual(sum(Decimal(order["fill_count"]) for order in record["exit_orders"]), Decimal("3.00"))
            self.assertFalse(engine.state["circuit_breaker"]["blocked"])
        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
