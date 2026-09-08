"""Read-only funding and mocked order-boundary tests; never use credentials."""
from __future__ import annotations

import json
from pathlib import Path
import tempfile
import time
import unittest
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import kalshi_btc15m_average_down as trader
from kalshi_btc15m_average_down import KalshiREST, record_submission_failure
from tests import test_delayed_band_v12 as band_fixtures

BandFeed = band_fixtures.BandFeed
EntryRest = band_fixtures.EntryRest


class ExchangeShardSafetyTests(unittest.IsolatedAsyncioTestCase):
    ticker = "KXBTC15M-shard-safety"

    def adapter(self):
        rest = object.__new__(KalshiREST)
        rest.trading_pause_active = lambda: False
        rest.get_raw_json = AsyncMock()
        rest.orders = SimpleNamespace(
            create_order_v2=AsyncMock(return_value={
                "order_id": "mock-only", "fill_count": "0", "remaining_count": "1.00",
            }),
            cancel_order_v2=AsyncMock(return_value={"order_id": "mock-only", "reduced_by": "1.00"}),
        )
        return rest

    def test_sdk_uses_current_external_v2_hosts(self):
        with tempfile.TemporaryDirectory() as directory:
            pem = Path(directory) / "test.pem"
            pem.write_text("offline-test-key")
            for demo, expected in (
                (False, "https://external-api.kalshi.com/trade-api/v2"),
                (True, "https://external-api.demo.kalshi.co/trade-api/v2"),
            ):
                configuration = SimpleNamespace()
                with (
                    patch.object(trader, "Configuration", return_value=configuration) as constructor,
                    patch.object(trader, "KalshiClient", return_value=SimpleNamespace()),
                    patch.object(trader, "KalshiAuth", return_value=SimpleNamespace()),
                    patch.object(trader, "PortfolioApi", return_value=SimpleNamespace()),
                    patch.object(trader, "EventsApi", return_value=SimpleNamespace()),
                    patch.object(trader, "MarketApi", return_value=SimpleNamespace()),
                    patch.object(trader, "OrdersApi", return_value=SimpleNamespace()),
                ):
                    rest = KalshiREST("offline-key-id", pem, demo=demo)
                constructor.assert_called_once_with(host=expected)
                self.assertEqual(rest.base_url, expected)

    def entry(self):
        fixture = band_fixtures.DelayedBandV12Tests()
        fixture.setUp()
        engine = fixture.engine(dry_run=False)
        opened = time.time() - 61
        record = fixture.signal(engine, opened, self.ticker)
        return engine, record, BandFeed(opened, 49, 54), opened + 60.2

    async def test_funding_reads_authoritative_market_shard_not_aggregate(self):
        rest = self.adapter()
        rest.get_raw_json.side_effect = [
            {"market": {"ticker": self.ticker, "exchange_index": 2}},
            {"balance_dollars": "0.0000", "balance": 0},
        ]
        self.assertEqual(await rest.market_entry_funding(self.ticker), (2, Decimal("0.0000")))
        self.assertEqual(rest.get_raw_json.await_args_list[1].args,
                         ("/portfolio/balance", {"exchange_index": 2}))
        self.assertEqual(rest.order_exchange_index(self.ticker), 2)

    async def test_verified_shard_is_used_by_buy_both_stop_phases_and_cancel(self):
        for index in (0, 2, 3):
            rest = self.adapter()
            rest.get_raw_json.side_effect = [
                {"market": {"ticker": self.ticker, "exchange_index": index}},
                {"balance_dollars": "10.00"},
            ]
            await rest.market_entry_funding(self.ticker)
            entry = await rest.create_order(ticker=self.ticker, side="yes", position_price=.53, quantity=1.0,
                                           tif="good_till_canceled", expiration_time=None, dry_run=False,
                                           order_key="mock-entry", post_only=True)
            self.assertEqual(entry["routing_exchange_index"], index)
            self.assertEqual(rest.orders.create_order_v2.await_args.kwargs["exchange_index"], index)
            await rest.create_reduce_only_exit(ticker=self.ticker, held_side="yes", economic_exit_price=.50,
                                              quantity=1.0, dry_run=False, order_key="mock-stop")
            self.assertEqual(rest.orders.create_order_v2.await_args.kwargs["exchange_index"], index)
            await rest.create_reduce_only_maker_exit(ticker=self.ticker, held_side="yes", economic_exit_price=.52,
                                                    quantity=1.0, dry_run=False, order_key="mock-maker",
                                                    expiration_time=2000000000)
            self.assertEqual(rest.orders.create_order_v2.await_args.kwargs["exchange_index"], index)
            self.assertTrue(await rest.cancel_order(entry, False))
            self.assertEqual(rest.orders.cancel_order_v2.await_args.kwargs["exchange_index"], index)
            self.assertEqual(rest.get_raw_json.await_count, 2)  # exits never require balance reads

    def test_restart_or_unknown_market_uses_ticker_routing_not_default_zero(self):
        rest = self.adapter()
        self.assertEqual(rest.order_exchange_index(self.ticker), -1)
        rest._market_exchange_indexes = {self.ticker: 2}
        self.assertEqual(rest.order_exchange_index("KXBTC15M-other"), -1)
        restarted = self.adapter()
        self.assertEqual(restarted.order_exchange_index(self.ticker), -1)

    async def test_market_routing_cache_is_bounded(self):
        rest = self.adapter()
        rest._market_exchange_indexes = {f"old-{i}": 0 for i in range(256)}
        rest.get_raw_json.side_effect = [
            {"market": {"ticker": self.ticker, "exchange_index": 2}},
            {"balance_dollars": "0"},
        ]
        await rest.market_entry_funding(self.ticker)
        self.assertEqual(len(rest._market_exchange_indexes), 256)
        self.assertEqual(rest.order_exchange_index(self.ticker), 2)

    def test_cached_index_is_never_shared_across_accounts_or_clients(self):
        first = self.adapter()
        second = self.adapter()
        first._market_exchange_indexes = {self.ticker: 2}
        self.assertEqual(second.order_exchange_index(self.ticker), -1)

    async def test_funding_keeps_fixed_point_precision_and_cent_fallback(self):
        for payload, expected in (({"balance_dollars": "120.4724"}, "120.4724"),
                                  ({"balance": 12047}, "120.47")):
            rest = self.adapter()
            rest.get_raw_json.side_effect = [
                {"market": {"ticker": self.ticker, "exchange_index": 0}}, payload,
            ]
            self.assertEqual(await rest.market_entry_funding(self.ticker), (0, Decimal(expected)))

    async def test_missing_or_invalid_exchange_metadata_never_defaults_to_zero(self):
        for market in ({}, {"ticker": "wrong", "exchange_index": 2},
                       *({"ticker": self.ticker, "exchange_index": index}
                         for index in (None, -1, True, "2", 2.0))):
            rest = self.adapter()
            rest.get_raw_json.return_value = {"market": market}
            with self.assertRaises(ValueError):
                await rest.market_entry_funding(self.ticker)
            self.assertEqual(rest.get_raw_json.await_count, 1)

    async def test_invalid_balances_cannot_allow_entry(self):
        for payload in ({}, {"balance_dollars": "NaN"}, {"balance_dollars": "Infinity"},
                        {"balance_dollars": "-1"}, {"balance_dollars": "bad"}):
            rest = self.adapter()
            rest.get_raw_json.side_effect = [
                {"market": {"ticker": self.ticker, "exchange_index": 2}}, payload,
            ]
            with self.assertRaises((ValueError, ArithmeticError)):
                await rest.market_entry_funding(self.ticker)

    async def test_unfunded_shard_blocks_post_and_preserves_sizing(self):
        engine, record, feed, now = self.entry()
        before = dict(engine.state["sizing"])
        rest = EntryRest()
        rest.balance_decimal = AsyncMock(return_value=Decimal("120.4724"))
        rest.market_entry_funding = AsyncMock(return_value=(2, Decimal("0")))
        await engine.submit_entry(rest, feed, record, now)
        self.assertEqual(rest.calls, [])
        rest.balance_decimal.assert_not_awaited()
        self.assertEqual(record["status"], "FUNDING_FAILURE")
        self.assertEqual(record["funding_failure"]["exchange_index"], 2)
        self.assertEqual(record["funding_failure"]["required_cash"], "0.5300")
        self.assertEqual(engine.state["sizing"], before)
        saved = json.loads(engine.state_path.read_text())["markets"][self.ticker]
        self.assertEqual(saved["funding_failure"], record["funding_failure"])

    async def test_unknown_funding_fails_closed_without_exception_text(self):
        engine, record, feed, now = self.entry()
        rest = EntryRest()
        rest.market_entry_funding = AsyncMock(side_effect=RuntimeError("mock-secret-never-log"))
        await engine.submit_entry(rest, feed, record, now)
        self.assertEqual(rest.calls, [])
        self.assertEqual(record["status_reason"], "market_exchange_funding_unavailable")
        self.assertNotIn("mock-secret-never-log", engine.state_path.read_text() + engine.ledger_path.read_text())

    async def test_funded_shard_retains_exact_gtc_price_quantity_and_direction(self):
        engine, record, feed, now = self.entry()
        rest = EntryRest()
        await engine.submit_entry(rest, feed, record, now)
        await engine.submit_entry(rest, feed, record, now + 1)
        self.assertEqual(len(rest.calls), 1)
        self.assertEqual(rest.calls[0]["side"], "yes")
        self.assertEqual(rest.calls[0]["position_price"], .53)
        self.assertEqual(rest.calls[0]["quantity"], 1.0)
        self.assertEqual(rest.calls[0]["tif"], "good_till_canceled")
        self.assertTrue(rest.calls[0]["post_only"])
        self.assertEqual(record["status"], "ENTRY_PENDING")

    async def test_existing_breaker_is_never_cleared_by_new_funding(self):
        engine, record, feed, now = self.entry()
        engine.trip("maker_entry_submission_unknown")
        rest = EntryRest()
        rest.market_entry_funding = AsyncMock(return_value=(2, Decimal("150")))
        await engine.submit_entry(rest, feed, record, now)
        rest.market_entry_funding.assert_not_awaited()
        self.assertEqual(rest.calls, [])
        self.assertTrue(engine.state["circuit_breaker"]["blocked"])

    async def test_missing_acknowledgment_is_not_success(self):
        engine, record, feed, now = self.entry()
        rest = EntryRest()
        rest.create_order = AsyncMock(return_value={"status": "resting", "quantity": "1",
                                                    "fill_count": "0", "remaining_count": "1"})
        await engine.submit_entry(rest, feed, record, now)
        self.assertEqual(record["status"], "RECONCILIATION_PENDING")
        self.assertTrue(engine.state["circuit_breaker"]["blocked"])
        self.assertEqual(engine.entry_submission_health()["unresolved_submissions"], 1)

    async def test_explicit_rejection_is_distinguished_and_not_blindly_retried(self):
        engine, record, feed, now = self.entry()
        rest = EntryRest()
        rest.create_order = AsyncMock(return_value={"status": "submit_failed", "submission_outcome": "rejected",
                                                    "quantity": "1", "fill_count": "0", "remaining_count": "0"})
        await engine.submit_entry(rest, feed, record, now)
        await engine.submit_entry(rest, feed, record, now + 1)
        self.assertEqual(rest.create_order.await_count, 1)
        self.assertEqual(engine.state["circuit_breaker"]["reason"], "maker_entry_submission_rejected")
        self.assertEqual(engine.entry_submission_health()["definitive_rejections"], 1)
        self.assertEqual(engine.entry_submission_health()["exchange_acknowledgments"], 0)

    async def test_buy_routing_and_side_mapping_remain_correct(self):
        for side, book_side, price in (("yes", "bid", "0.5300"), ("no", "ask", "0.4700")):
            rest = self.adapter()
            await rest.create_order(ticker=self.ticker, side=side, position_price=.53, quantity=1.0,
                                    tif="good_till_canceled", expiration_time=None, dry_run=False,
                                    order_key="mock-entry", post_only=True)
            kwargs = rest.orders.create_order_v2.await_args.kwargs
            self.assertEqual(kwargs["exchange_index"], -1)
            self.assertEqual(kwargs["ticker"], self.ticker)
            self.assertEqual(kwargs["side"].value, book_side)
            self.assertEqual(kwargs["price"], price)
            self.assertFalse(kwargs["reduce_only"])

    async def test_both_stop_phases_auto_route_and_remain_reduce_only(self):
        for side in ("yes", "no"):
            rest = self.adapter()
            await rest.create_reduce_only_exit(ticker=self.ticker, held_side=side, economic_exit_price=.50,
                                              quantity=1.0, dry_run=False, order_key="mock-stop")
            kwargs = rest.orders.create_order_v2.await_args.kwargs
            self.assertEqual(kwargs["exchange_index"], -1)
            self.assertTrue(kwargs["reduce_only"])
            self.assertFalse(kwargs["post_only"])
            self.assertEqual(kwargs["time_in_force"], "immediate_or_cancel")
            await rest.create_reduce_only_maker_exit(ticker=self.ticker, held_side=side, economic_exit_price=.52,
                                                    quantity=1.0, dry_run=False, order_key="mock-maker",
                                                    expiration_time=2000000000)
            kwargs = rest.orders.create_order_v2.await_args.kwargs
            self.assertEqual(kwargs["exchange_index"], -1)
            self.assertTrue(kwargs["reduce_only"])
            self.assertTrue(kwargs["post_only"])
            self.assertEqual(kwargs["time_in_force"], "good_till_canceled")

    async def test_cancel_routes_by_ticker_and_missing_ticker_fails_closed(self):
        rest = self.adapter()
        record = {"order_id": "mock-only", "ticker": self.ticker, "quantity": 1.0,
                  "fill_count": 0.0, "remaining_count": 1.0}
        self.assertTrue(await rest.cancel_order(record, False))
        rest.orders.cancel_order_v2.assert_awaited_once_with(
            "mock-only", market_ticker=self.ticker, exchange_index=-1)
        rest.orders.cancel_order_v2.reset_mock()
        record.pop("ticker")
        record["remaining_count"] = 1.0
        self.assertFalse(await rest.cancel_order(record, False))
        rest.orders.cancel_order_v2.assert_not_awaited()

    async def test_cancel_failure_never_logs_raw_exception_or_assumes_flat(self):
        rest = self.adapter()
        rest.orders.cancel_order_v2.side_effect = RuntimeError("mock-secret-never-log")
        record = {"order_id": "mock-only", "ticker": self.ticker, "remaining_count": 1.0}
        with self.assertLogs(level="WARNING") as logs:
            self.assertFalse(await rest.cancel_order(record, False))
        self.assertEqual(record["remaining_count"], 1.0)
        self.assertNotIn("mock-secret-never-log", json.dumps(record) + str(logs.output))

    async def test_submission_error_never_logs_raw_exception(self):
        rest = self.adapter()
        error = RuntimeError("mock-secret-never-log")
        error.status = 404
        error.body = json.dumps({"error": {"code": "user_not_found", "details": "mock-secret-never-log"}})
        rest.orders.create_order_v2.side_effect = error
        with self.assertLogs(level="ERROR") as logs:
            order = await rest.create_order(ticker=self.ticker, side="yes", position_price=.53, quantity=1.0,
                                            tif="good_till_canceled", expiration_time=None, dry_run=False,
                                            order_key="mock-entry", post_only=True)
        self.assertEqual(order["error_code"], "user_not_found")
        self.assertEqual(order["submission_outcome"], "rejected")
        self.assertEqual(order["remaining_count"], 0)
        self.assertNotIn("mock-secret-never-log", json.dumps(order) + str(logs.output))

    def test_arbitrary_error_codes_are_not_persisted(self):
        error = RuntimeError()
        error.body = '{"error":{"code":"mock-secret-never-log"}}'
        record = {}
        record_submission_failure(record, error)
        self.assertNotIn("error_code", record)

    async def test_position_lookup_is_market_filtered_and_truncation_is_unknown(self):
        rest = self.adapter()
        rest.portfolio = SimpleNamespace(get_positions=AsyncMock(return_value={
            "market_positions": [], "cursor": "more"}))
        self.assertIsNone(await rest.position_for_ticker(self.ticker))
        rest.portfolio.get_positions.assert_awaited_once_with(ticker=self.ticker, limit=200)

    async def test_position_with_missing_quantity_is_unknown_not_flat(self):
        rest = self.adapter()
        rest.portfolio = SimpleNamespace(get_positions=AsyncMock(return_value={
            "market_positions": [{"ticker": self.ticker}]}))
        self.assertIsNone(await rest.position_for_ticker(self.ticker))

    async def test_market_closing_during_funding_check_cannot_receive_new_order(self):
        engine, record, feed, now = self.entry()
        rest = EntryRest()
        with patch("kalshi_live_trader.time.monotonic", return_value=0.0) as clock:
            async def funding(_ticker):
                clock.return_value = 1800.0
                return 2, Decimal("150")
            rest.market_entry_funding = funding
            await engine.submit_entry(rest, feed, record, now)
        self.assertEqual(rest.calls, [])

    async def test_shadow_entry_does_not_call_authenticated_funding_or_orders(self):
        fixture = band_fixtures.DelayedBandV12Tests()
        fixture.setUp()
        engine = fixture.engine(dry_run=True)
        opened = time.time() - 61
        record = fixture.signal(engine, opened, self.ticker)
        rest = EntryRest()
        rest.market_entry_funding = AsyncMock(side_effect=AssertionError("unexpected authenticated read"))
        await engine.submit_entry(rest, BandFeed(opened, 49, 54), record, opened + 60.2)
        rest.market_entry_funding.assert_not_awaited()
        self.assertEqual(rest.calls, [])
        self.assertEqual(record["status"], "ENTRY_PENDING")
        self.assertEqual(record["entry_funding"]["scope"], "shadow_cash")


if __name__ == "__main__":
    unittest.main()
