from __future__ import annotations

import asyncio
import json
import tempfile
import time
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from audit_ledger import append_audit
from live_checkpoint import MaterialCheckpointPublisher
from kalshi_btc15m_average_down import KalshiLiveFeed
from kalshi_live_trader import (
    LiveEngine, ProvisionalOutcomeTracker, QuoteObservation, deterministic_client_order_id, epoch,
    live_mode_allowed, load_config,
)
from live_state import default_state, load_state, save_state
from strategy_core import sizing_state


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
        observed = time.time()
        return {
            "ticker": ticker, "side": side, "economic_price": float(self.ask),
            "displayed_depth": float(self.depth), "quote_id": f"test-book:{self.ask}:{self.bid}",
            "yes_bid": float(self.bid), "yes_ask": float(self.ask),
            "yes_bid_size": float(self.depth), "yes_ask_size": float(self.depth),
            "source_server_timestamp": datetime.fromtimestamp(observed, timezone.utc).isoformat(),
            "source_timestamp_ms": int(observed * 1000),
            "received_at": datetime.fromtimestamp(observed, timezone.utc).isoformat(), "quote_age_seconds": 0.01,
        }, "executable_top_of_book"

    def public_trades_after(self, _ticker: str, _created):
        return []


class DelayedFreshBookFeed(Feed):
    """Simulate a WebSocket that becomes usable after the market opens."""

    def __init__(self, ask: str = "0.60", bid: str = "0.45", depth: str = "10.00") -> None:
        super().__init__(ask, bid, depth)
        self.available = False

    def executable_asks(self, ticker: str):
        return super().executable_asks(ticker) if self.available else None

    def executable_shadow_quote(self, ticker: str, side: str, quantity: float, age: float):
        return super().executable_shadow_quote(ticker, side, quantity, age) if self.available else (None, "missing_or_stale_book")


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


class CancellationFailureRest(LiveFallbackRest):
    async def cancel_order(self, order, _dry_run):
        order["cancel_error"] = "simulated_transport_failure"
        return False


class StopRetryRest(EntryRest):
    def __init__(self) -> None:
        super().__init__()
        self.position = Decimal("1.00")
        self.exit_calls = 0

    async def position_for_ticker(self, _ticker: str):
        return self.position

    async def create_reduce_only_exit(self, **kwargs):
        self.exit_calls += 1
        if self.exit_calls == 1:
            return {
                "order_id": "rejected-stop", "status": "submit_failed", "fill_count": "0.00",
                "remaining_count": str(kwargs["quantity"]), "fees_paid": "0",
            }
        self.position = Decimal("0")
        return {
            "order_id": "retry-stop", "status": "filled", "fill_count": str(kwargs["quantity"]),
            "remaining_count": "0.00", "average_fill_price": kwargs["economic_exit_price"], "fees_paid": "0",
        }

    async def refresh_exit_order(self, _order):
        return None


class LiveExecutionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.config = load_config(ROOT / "live_strategy_config.json")

    def engine(self, shadow_balance: str = "1000.00") -> LiveEngine:
        temporary = Path(tempfile.mkdtemp())
        config = dict(self.config)
        config["starting_shadow_balance"] = shadow_balance
        return LiveEngine(config, default_state(config), temporary / "state.json", temporary / "audit.jsonl", dry_run=True)

    def test_persistent_shadow_only_lock_blocks_live_mode(self) -> None:
        self.assertFalse(live_mode_allowed(True, True, True, False))
        self.assertFalse(live_mode_allowed(True, True, False, True))
        self.assertFalse(live_mode_allowed(True, False, False, False))
        self.assertTrue(live_mode_allowed(True, True, False, False))

    def test_v9_refuses_a_v8_checkpoint_instead_of_reinterpreting_recovery(self) -> None:
        temporary = Path(tempfile.mkdtemp()) / "state.json"
        state = default_state(self.config)
        state["strategy_version"] = "kxbtc15m-hybrid-live-v8"
        save_state(temporary, state)
        with self.assertRaisesRegex(RuntimeError, "strategy version differs"):
            load_state(temporary, self.config)

    def test_uncapped_recovery_exponent_migrates_without_reset_and_keeps_position_cap(self) -> None:
        temporary = Path(tempfile.mkdtemp()) / "state.json"
        legacy_config = dict(self.config, max_recovery_exponent=12)
        state = default_state(legacy_config)
        state["sizing"] = {
            "base_share_count": "1.00", "recovery_exponent": 500,
            "recovery_cycle_pnl": "-1.1494", "next_base_threshold": "350.00",
        }
        save_state(temporary, state)

        migrated = load_state(temporary, self.config)
        self.assertEqual(migrated["sizing"]["recovery_exponent"], 500)
        self.assertEqual(migrated["sizing"]["recovery_cycle_pnl"], "-1.1494")
        self.assertEqual(migrated["config_migrations"][-1]["kind"], "disable_recovery_exponent_breaker")

        engine = LiveEngine(
            self.config, migrated, temporary, temporary.with_suffix(".jsonl"), dry_run=True,
        )
        self.assertTrue(engine.circuit_allows_entry())
        self.assertFalse(engine.state["circuit_breaker"]["blocked"])
        self.assertEqual(
            sizing_state(engine.current_parameters(), engine.state["sizing"]).prescribed_quantity(),
            Decimal("100.00"),
        )

    def test_unrelated_configuration_change_still_fails_closed(self) -> None:
        temporary = Path(tempfile.mkdtemp()) / "state.json"
        save_state(temporary, default_state(self.config))
        with self.assertRaisesRegex(RuntimeError, "configuration hash differs"):
            load_state(temporary, dict(self.config, max_position="99.00"))

    def test_discovery_preloads_api_successor_from_bounded_close_window(self) -> None:
        async def scenario() -> None:
            now = time.time()

            def market(ticker: str, opened: float, closed: float, status: str, result: str = "") -> dict:
                return {
                    "ticker": ticker, "open_time": datetime.fromtimestamp(opened, timezone.utc).isoformat(),
                    "close_time": datetime.fromtimestamp(closed, timezone.utc).isoformat(),
                    "status": status, "result": result,
                }

            class DiscoveryRest:
                def __init__(self) -> None:
                    self.calls = []

                async def get_raw_json(self, path, params):
                    self.calls.append((path, params))
                    self.assert_bounded(params)
                    return {"markets": [
                        market("KXBTC15M-prior", now - 930, now - 30, "finalized", "yes"),
                        market("KXBTC15M-active", now - 30, now + 870, "active"),
                        market("KXBTC15M-upcoming", now + 870, now + 1770, "initialized"),
                    ]}

                @staticmethod
                def assert_bounded(params):
                    assert "status" not in params
                    assert params["min_close_ts"] <= now - 30
                    assert params["max_close_ts"] >= now + 1770
                    assert params["limit"] == 100

            engine = self.engine()
            rest = DiscoveryRest()
            await engine.discover(rest)
            active = engine.active_market(now)
            self.assertEqual(active and active["ticker"], "KXBTC15M-active")
            self.assertEqual(engine.predecessor(active or {}) and engine.predecessor(active or {})["ticker"], "KXBTC15M-prior")
            self.assertEqual(engine.successor(active or {}) and engine.successor(active or {})["ticker"], "KXBTC15M-upcoming")
            self.assertEqual(len(rest.calls), 1)
        asyncio.run(scenario())

    def test_millisecond_exchange_timestamp_and_price_only_quote_enable_provisional_signal(self) -> None:
        boundary = 1_786_996_800.0
        self.assertEqual(epoch(int((boundary - .2) * 1000)), boundary - .2)
        self.assertEqual(epoch(str(int((boundary - .2) * 1000))), boundary - .2)
        feed = KalshiLiveFeed(auth=None)
        with patch("kalshi_btc15m_average_down.time.time", return_value=boundary - .2):
            feed._handle(json.dumps({
                "type": "ticker",
                "msg": {
                    "market_ticker": "prior", "yes_bid_dollars": "0.99",
                    "yes_ask_dollars": "1.00", "ts_ms": int((boundary - .2) * 1000),
                },
            }))
        self.assertIn("ticker_book", feed.quotes["prior"])
        self.assertNotIn("complete_book", feed.quotes["prior"])
        tracker = ProvisionalOutcomeTracker(Decimal(".99"), 5, 2)
        tracker.observe_feed(feed, "prior")
        inferred = tracker.infer("prior", boundary)
        self.assertEqual(inferred and inferred["outcome"], "yes")
        self.assertEqual(inferred and inferred["method"], "final_window_executable_bid_threshold")
        self.assertAlmostEqual(inferred and inferred["quote_age_seconds"], .2, places=6)

    def test_provisional_uses_only_threshold_hits_inside_configured_final_window(self) -> None:
        boundary = 10_000.0
        tracker = ProvisionalOutcomeTracker(Decimal(".99"), 5, 2)
        tracker.observations["prior"] = [
            QuoteObservation("prior", boundary - 5.01, boundary - 5.01, Decimal(".99"), Decimal(".01")),
            QuoteObservation("prior", boundary - 4.9, boundary - 4.9, Decimal(".99"), Decimal(".01")),
            # The final raw quote need not itself remain at 99c; the observed
            # threshold hit anywhere in the declared final window is the fact.
            QuoteObservation("prior", boundary - .1, boundary - .1, Decimal(".98"), Decimal(".02")),
        ]
        inferred = tracker.infer("prior", boundary)
        self.assertEqual(inferred and inferred["outcome"], "yes")
        self.assertEqual(inferred and inferred["observation_window_seconds"], 5)
        self.assertEqual(inferred and inferred["qualifying_bid"], "0.99")
        self.assertAlmostEqual(inferred and inferred["quote_age_seconds"], 4.9, places=6)

        fifteen_seconds = ProvisionalOutcomeTracker(Decimal(".99"), 15, 2)
        fifteen_seconds.observations["prior"] = [
            QuoteObservation("prior", boundary - 14.9, boundary - 14.9, Decimal(".01"), Decimal(".99")),
        ]
        inferred_15 = fifteen_seconds.infer("prior", boundary)
        self.assertEqual(inferred_15 and inferred_15["outcome"], "no")
        self.assertEqual(inferred_15 and inferred_15["observation_window_seconds"], 15)

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

    def test_live_signal_holds_after_directional_loss_then_flips_after_win(self) -> None:
        engine = self.engine()
        first = engine.set_signal(
            {"ticker": "KXBTC15M-1", "open_epoch": 1_000, "close_epoch": 1_900},
            {"outcome": "yes", "ticker": "KXBTC15M-0"},
        )
        self.assertEqual(first["signal_side"], "no")
        self.assertEqual(first["directional_transition"], "seed_inverse_settlement")
        # The NO prediction was wrong because market 1 settled YES.  The
        # following market must remain NO even if the first order was a zero
        # fill; execution has no bearing on directional state.
        second = engine.set_signal(
            {"ticker": "KXBTC15M-2", "open_epoch": 1_900, "close_epoch": 2_800},
            {"outcome": "yes", "ticker": "KXBTC15M-1"},
        )
        self.assertEqual(second["prior_signal_side"], "no")
        self.assertEqual(second["signal_side"], "no")
        self.assertEqual(second["directional_transition"], "hold_after_directional_loss")
        # Market 2 then settles NO, so the carried NO side finally wins and
        # the next prediction flips to YES.
        third = engine.set_signal(
            {"ticker": "KXBTC15M-3", "open_epoch": 2_800, "close_epoch": 3_700},
            {"outcome": "no", "ticker": "KXBTC15M-2"},
        )
        self.assertEqual(third["signal_side"], "yes")
        self.assertEqual(third["directional_transition"], "flip_after_directional_win")

    def test_provisional_rejects_stale_missing_and_conflicting_quotes(self) -> None:
        tracker = ProvisionalOutcomeTracker(Decimal(".99"), 5, 2)
        boundary = 1_000.0
        # Inside the five-second decision window but delivered from a source
        # timestamp more than two seconds old: reject as stale transport.
        tracker.observations["prior"] = [QuoteObservation("prior", boundary - .2, boundary - 3, Decimal(".99"), Decimal(".01"))]
        self.assertIsNone(tracker.infer("prior", boundary))
        tracker.observations["prior"] = [QuoteObservation("prior", boundary - 5.01, boundary - 5.01, Decimal(".99"), Decimal(".01"))]
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
            await engine.submit_entry(EntryRest(), feed, record, 1_004.1)
            self.assertEqual(record["status"], "ZERO_FILL")
            self.assertEqual(engine.state["sizing"].get("recovery_exponent", 0), 0)
            engine = self.engine("0.01")
            record = engine.set_signal({"ticker": "KXBTC15M-current2", "open_epoch": 1_000, "close_epoch": 1_900}, {"outcome": "yes", "ticker": "KXBTC15M-prior"})
            rest = EntryRest("0.01")
            feed = Feed("0.60")
            await engine.submit_entry(rest, feed, record, 1_001)
            await engine.submit_entry(rest, feed, record, 1_004.1)
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
        engine.state["markets"][record["ticker"]] = record
        metrics = engine.refresh_entry_execution_metrics()
        self.assertEqual(metrics["markets_with_entry_fill"], 1)
        self.assertEqual(metrics["maker_limit_fill_markets"], 1)
        self.assertEqual(metrics["market_ioc_fill_markets"], 1)
        self.assertEqual(metrics["mixed_entry_markets"], 1)
        self.assertEqual(metrics["maker_limit_filled_quantity"], "0.30")
        self.assertEqual(metrics["market_ioc_filled_quantity"], "0.70")
        # A repeated refresh after a runner restart cannot increment totals.
        self.assertEqual(engine.refresh_entry_execution_metrics(), metrics)

    def test_v9_submits_one_immediate_protected_market_ioc(self) -> None:
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
            self.assertEqual(len(rest.calls), 1)
            self.assertFalse(rest.calls[0]["post_only"])
            self.assertEqual(rest.calls[0]["tif"], "immediate_or_cancel")
            self.assertEqual(rest.calls[0]["position_price"], 0.60)
            self.assertEqual(record["status"], "POSITION_OPEN")
            self.assertEqual(record["actual_quantity"], "1.00")
            self.assertTrue(record["market_entry_attempted"])
            self.assertEqual(record["entry_orders"][0]["entry_phase"], "market_entry")
            self.assertEqual(record["entry_execution_type"], "market_ioc")
            self.assertEqual(record["entry_execution_summary"]["maker_limit_filled_quantity"], "0")
            self.assertEqual(record["entry_execution_summary"]["market_ioc_filled_quantity"], "1.00")
            self.assertEqual(record["entry_execution_summary"]["market_ioc_average_fill_price"], "0.6")
            self.assertEqual(engine.state["entry_execution_metrics"]["maker_limit_fill_markets"], 0)
            self.assertEqual(engine.state["entry_execution_metrics"]["market_ioc_fill_markets"], 1)
        asyncio.run(scenario())

    def test_v9_market_entry_does_not_attempt_maker_cancellation(self) -> None:
        async def scenario() -> None:
            config = dict(self.config)
            directory = Path(tempfile.mkdtemp())
            engine = LiveEngine(config, default_state(config), directory / "state", directory / "ledger", dry_run=False)
            rest = CancellationFailureRest()
            feed = Feed("0.60")
            record = engine.set_signal(
                {"ticker": "KXBTC15M-cancel-uncertain", "open_epoch": 1_000, "close_epoch": 1_900},
                {"outcome": "yes", "ticker": "KXBTC15M-prior"},
            )
            await engine.submit_entry(rest, feed, record, 1_000)
            self.assertEqual(len(rest.calls), 1)
            self.assertEqual(record["status"], "POSITION_OPEN")
            self.assertFalse(engine.state["circuit_breaker"]["blocked"])
            self.assertNotIn("entry_cancel_pending", record)
        asyncio.run(scenario())

    def test_archived_shadow_maker_without_exchange_id_cancels_locally(self) -> None:
        async def scenario() -> None:
            class NoCancelRest:
                def __init__(self) -> None:
                    self.cancel_calls = 0

                async def cancel_order(self, _order, _dry_run):
                    self.cancel_calls += 1
                    return False

            engine = self.engine()
            record = {
                "ticker": "KXBTC15M-old-shadow-maker", "status": "ENTRY_PENDING",
                "entry_orders": [{
                    "entry_phase": "maker", "order_id": None, "status": "shadow_maker_open",
                    "remaining_count": "1.00", "fill_count": "0.00",
                }],
            }
            rest = NoCancelRest()
            self.assertTrue(await engine.cancel_entry_orders_and_confirm(rest, record, next_action="finish_entry"))
            self.assertEqual(rest.cancel_calls, 0)
            self.assertEqual(record["entry_orders"][0]["remaining_count"], "0.00")
            self.assertFalse(engine.state["circuit_breaker"]["blocked"])
        asyncio.run(scenario())

    def test_rejected_stop_ioc_retries_using_authoritative_residual_position(self) -> None:
        async def scenario() -> None:
            config = dict(self.config)
            directory = Path(tempfile.mkdtemp())
            engine = LiveEngine(config, default_state(config), directory / "state", directory / "ledger", dry_run=False)
            rest = StopRetryRest()
            record = {
                "ticker": "KXBTC15M-stop-retry", "signal_side": "yes", "status": "POSITION_OPEN",
                "actual_quantity": "1.00", "entry_orders": [
                    {"entry_phase": "maker", "fill_count": "1.00", "remaining_count": "0", "average_fill_price": "0.50", "fees_paid": "0"},
                ], "exit_orders": [],
            }
            engine.state["markets"][record["ticker"]] = record
            engine.state["active_market"] = record["ticker"]
            await engine.close_at_stop(rest, record, Decimal("0.40"))
            self.assertEqual(rest.exit_calls, 1)
            self.assertEqual(record["status"], "STOP_PENDING")
            await engine.close_at_stop(rest, record, Decimal("0.40"))
            self.assertEqual(rest.exit_calls, 2, "a rejected IOC must not suppress the next flattening attempt")
            await engine.manage_stop(rest, Feed("0.60", "0.40"), record)
            self.assertEqual(record["status"], "CLOSED")
        asyncio.run(scenario())

    def test_shadow_stop_is_recorded_as_non_resting_reduce_only_ioc(self) -> None:
        async def scenario() -> None:
            engine = self.engine()
            record = {
                "ticker": "KXBTC15M-shadow-taker-stop", "signal_side": "yes", "status": "POSITION_OPEN",
                "actual_quantity": "1.00", "entry_orders": [
                    {
                        "entry_phase": "market_entry", "fill_count": "1.00", "remaining_count": "0",
                        "average_fill_price": "0.50", "fees_paid": "0",
                        "time_in_force": "immediate_or_cancel", "post_only": False,
                    },
                ], "exit_orders": [],
            }
            engine.state["markets"][record["ticker"]] = record
            engine.state["active_market"] = record["ticker"]
            await engine.close_at_stop(EntryRest(), record, Decimal("0.40"), entries_confirmed=True)
            stop = record["exit_orders"][0]
            self.assertEqual(stop["order_type"], "reduce_only_exit_ioc")
            self.assertEqual(stop["time_in_force"], "immediate_or_cancel")
            self.assertFalse(stop["post_only"])
            self.assertTrue(stop["reduce_only"])
            self.assertEqual(record["status"], "CLOSED")
        asyncio.run(scenario())

    def test_market_entry_refuses_a_40c_or_lower_selected_side(self) -> None:
        async def scenario() -> None:
            engine = self.engine()
            rest = EntryRest()
            feed = Feed("0.40")
            record = engine.set_signal(
                {"ticker": "KXBTC15M-no-fallback", "open_epoch": 1_000, "close_epoch": 1_900},
                {"outcome": "yes", "ticker": "KXBTC15M-prior"},
            )
            await engine.submit_entry(rest, feed, record, 1_000)
            self.assertEqual(record["status"], "ZERO_FILL")
            self.assertEqual(record["market_entry"]["reason"], "best_available_price_at_or_below_fixed_stop")
            self.assertEqual(engine.state["sizing"].get("recovery_exponent", 0), 0)
        asyncio.run(scenario())

    def test_market_ioc_uses_the_fresh_selected_side_ask(self) -> None:
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
            self.assertEqual(len(rest.calls), 1)
            self.assertEqual(rest.calls[0]["position_price"], 0.52)
            self.assertFalse(rest.calls[0]["post_only"])
            self.assertEqual(record["market_entry"]["best_available_price"], "0.52")
            samples = record["opening_quote_observations"]
            self.assertEqual(len(samples), 1)
            self.assertEqual(samples[0]["yes_bid"], "0.47")
            self.assertEqual(samples[0]["yes_ask"], "0.52")
            self.assertEqual(samples[0]["no_bid"], "0.48")
            self.assertEqual(samples[0]["no_ask"], "0.53")
            # Later opening observations are retained for calibration but
            # cannot rewrite the already-submitted IOC price.
            feed.ask = "0.70"
            engine.capture_opening_quote(feed, record, 1_004)
            self.assertEqual(Decimal(record["opening_quote_observations"][-1]["selected_best_ask"]), Decimal("0.70"))
            self.assertEqual(Decimal(record["opening_quote_capture"]["max_selected_best_ask"]), Decimal("0.70"))
            engine.capture_opening_quote(feed, record, 1_060)
            self.assertIsNotNone(record["opening_quote_capture"]["completed_at"])
        asyncio.run(scenario())

    def test_delayed_first_fresh_book_enters_immediately_when_available(self) -> None:
        async def scenario() -> None:
            config = dict(self.config)
            directory = Path(tempfile.mkdtemp())
            engine = LiveEngine(config, default_state(config), directory / "state", directory / "ledger", dry_run=False)
            rest = LiveFallbackRest()
            feed = DelayedFreshBookFeed("0.54")
            record = engine.set_signal(
                {"ticker": "KXBTC15M-delayed-book", "open_epoch": 1_000, "close_epoch": 1_900},
                {"outcome": "yes", "ticker": "KXBTC15M-prior"},
            )

            await engine.submit_entry(rest, feed, record, 1_003)
            self.assertEqual(record["status"], "SIGNAL_PENDING")
            self.assertEqual(rest.created, 0)

            feed.available = True
            await engine.submit_entry(rest, feed, record, 1_010)
            self.assertEqual(record["status"], "POSITION_OPEN")
            self.assertEqual(rest.created, 1)
            self.assertEqual(rest.calls[0]["position_price"], 0.54)
            self.assertEqual(record["entry_orders"][0]["entry_phase"], "market_entry")
        asyncio.run(scenario())

    def test_preloaded_preopen_quote_cannot_be_used_for_market_entry(self) -> None:
        async def scenario() -> None:
            market_open = time.time()

            class TimestampedFeed(Feed):
                def __init__(self) -> None:
                    super().__init__("0.54")
                    self.quote_epoch = market_open - .2

                def executable_shadow_quote(self, ticker: str, side: str, quantity: float, age: float):
                    quote, state = super().executable_shadow_quote(ticker, side, quantity, age)
                    quote["source_timestamp_ms"] = int(self.quote_epoch * 1000)
                    quote["source_server_timestamp"] = datetime.fromtimestamp(
                        self.quote_epoch, timezone.utc,
                    ).isoformat()
                    quote["received_at"] = quote["source_server_timestamp"]
                    return quote, state

            engine = self.engine()
            feed = TimestampedFeed()
            rest = EntryRest()
            record = engine.set_signal(
                {"ticker": "KXBTC15M-preloaded", "open_epoch": market_open, "close_epoch": market_open + 900},
                {"outcome": "yes", "ticker": "KXBTC15M-prior"},
            )
            await engine.submit_entry(rest, feed, record, market_open + .05)
            self.assertEqual(record["status"], "SIGNAL_PENDING")
            self.assertFalse(record["market_entry_attempted"])
            self.assertEqual(record["market_entry"]["reason"], "preopen_or_unstamped_top_of_book")
            self.assertEqual(record["opening_quote_observations"], [])

            feed.quote_epoch = market_open + .1
            await engine.submit_entry(rest, feed, record, market_open + .1)
            self.assertEqual(record["status"], "POSITION_OPEN")
            self.assertEqual(record["entry_execution_type"], "market_ioc")
            self.assertEqual(record["actual_quantity"], "1.00")
        asyncio.run(scenario())

    def test_market_entry_at_fixed_stop_is_a_zero_fill(self) -> None:
        async def scenario() -> None:
            engine = self.engine()
            rest = EntryRest()
            feed = Feed("0.40")
            record = engine.set_signal(
                {"ticker": "KXBTC15M-opening-at-stop", "open_epoch": 1_000, "close_epoch": 1_900},
                {"outcome": "no", "ticker": "KXBTC15M-prior"},
            )
            await engine.submit_entry(rest, feed, record, 1_000.1)
            self.assertEqual(record["status"], "ZERO_FILL")
            self.assertEqual(rest.created, 0)
            self.assertEqual(engine.state["sizing"].get("recovery_exponent", 0), 0)
        asyncio.run(scenario())

    def test_v9_stop_is_fixed_at_40c_even_when_actual_entry_is_above_50c(self) -> None:
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
            self.assertEqual(high_entry["effective_stop_price"], "0.40")
            self.assertEqual(high_entry["post_entry_stop_monitor"]["minimum_executable_bid"], "0.41")
            self.assertEqual(high_entry["status"], "POSITION_OPEN")
            await engine.manage_stop(rest, Feed("0.60", "0.40"), high_entry)
            self.assertEqual(high_entry["stop_trigger"]["effective_stop_price"], "0.40")
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

    def test_audit_and_state_are_checkpointed_for_each_material_event(self) -> None:
        temporary = Path(tempfile.mkdtemp())
        engine = LiveEngine(
            dict(self.config), default_state(self.config), temporary / "state.json", temporary / "audit.jsonl", dry_run=True,
        )
        engine.audit("checkpoint_contract_test", ticker="KXBTC15M-audit")
        self.assertTrue(engine.state_path.exists())
        records = [json.loads(line) for line in engine.ledger_path.read_text(encoding="utf-8").splitlines()]
        self.assertEqual(records[-1]["event"], "checkpoint_contract_test")
        persisted = json.loads(engine.state_path.read_text(encoding="utf-8"))
        self.assertEqual(persisted["strategy_version"], self.config["strategy_version"])

    def test_ledger_append_fsyncs_a_jsonl_record(self) -> None:
        path = Path(tempfile.mkdtemp()) / "audit.jsonl"
        with patch("audit_ledger.os.fsync") as fsync:
            append_audit(path, {"event": "durability_test"})
        self.assertGreaterEqual(fsync.call_count, 1)
        self.assertEqual(json.loads(path.read_text(encoding="utf-8"))["event"], "durability_test")

    def test_coalesced_remote_checkpoint_is_retried_by_normal_worker_checkpoint(self) -> None:
        path = Path(tempfile.mkdtemp()) / "state.json"
        path.write_text("{}\n", encoding="utf-8")
        publisher = MaterialCheckpointPublisher(path, minimum_interval_seconds=5.0)
        publisher.enabled = True
        publisher.last_attempt = 100.0
        with patch("live_checkpoint.time.monotonic", return_value=102.0):
            self.assertFalse(publisher.publish_if_changed("entry_fill_observed"))
        self.assertEqual(publisher.pending_reason, "entry_fill_observed")
        with patch.object(publisher, "publish_if_changed", return_value=True) as retry:
            self.assertTrue(publisher.publish_if_due())
        retry.assert_called_once_with("entry_fill_observed")

    def test_entry_and_stop_latency_is_durable_and_observational(self) -> None:
        engine = self.engine()
        record = engine.set_signal(
            {"ticker": "KXBTC15M-timing", "open_epoch": 1_000, "close_epoch": 1_900},
            {"outcome": "yes", "ticker": "KXBTC15M-prior"},
        )
        maker = {
            "order_id": "maker-timing", "client_order_id": "maker-client", "entry_phase": "maker",
            "quantity": "1.00", "position_price": "0.51", "fill_count": "0.00",
            "submitted_at": datetime.fromtimestamp(1_002, timezone.utc).isoformat(),
        }
        record["entry_orders"].append(maker)
        engine.note_entry_order_submitted(record, maker, "maker")
        with patch("kalshi_live_trader.time.time", return_value=1_005.0):
            engine.note_entry_fill_observed(record, Decimal("0"), Decimal("1.00"), "exchange_order_refresh")
        entry = record["entry_timing"]
        self.assertEqual(entry["market_open_to_first_submission_seconds"], 2.0)
        self.assertEqual(entry["market_open_to_first_fill_seconds"], 5.0)
        self.assertEqual(entry["first_submission_to_first_fill_seconds"], 3.0)
        self.assertEqual(entry["first_fill_source"], "exchange_order_refresh")

        with patch("kalshi_live_trader.time.time", return_value=1_012.0):
            engine.note_stop_trigger(record, Decimal("0.40"), Decimal("0.40"), Decimal("1.00"), shadow=True)
        exit_order = {
            "order_id": "stop-timing", "client_order_id": "stop-client",
            "submitted_at": datetime.fromtimestamp(1_012.25, timezone.utc).isoformat(),
        }
        engine.note_stop_exit_submitted(record, exit_order)
        with patch("kalshi_live_trader.time.time", return_value=1_013.0):
            engine.note_stop_position_closed(record)
        stop = record["stop_timing"]
        self.assertEqual(stop["first_fill_to_stop_trigger_seconds"], 7.0)
        self.assertEqual(stop["market_open_to_stop_trigger_seconds"], 12.0)
        self.assertEqual(stop["stop_trigger_to_first_exit_submission_seconds"], 0.25)
        self.assertEqual(stop["stop_trigger_to_position_closed_seconds"], 1.0)
        self.assertEqual(engine.state["execution_timing_metrics"]["entry_first_fill_from_market_open"]["count"], 1)
        events = [json.loads(line)["event"] for line in engine.ledger_path.read_text(encoding="utf-8").splitlines()]
        self.assertIn("entry_fill_observed", events)
        self.assertIn("stop_trigger_timing", events)
        self.assertIn("stop_position_closed_timing", events)


if __name__ == "__main__":
    unittest.main()
