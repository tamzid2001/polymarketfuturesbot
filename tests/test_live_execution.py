from __future__ import annotations

import asyncio
import json
import subprocess
import tempfile
import time
import unittest
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

from audit_ledger import append_audit
from live_checkpoint import MaterialCheckpointPublisher, publish_runtime_snapshot
from kalshi_btc15m_average_down import KalshiLiveFeed
from kalshi_live_trader import (
    BTC_TARGET_CAPTURE_CONTRACT_VERSION, LiveEngine, ProvisionalOutcomeTracker, QuoteObservation,
    btc_target_metadata, deterministic_client_order_id, epoch, live_mode_allowed, load_config,
    market_metadata,
)
from live_state import config_hash, default_state, load_state, save_state
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
        observed = time.time()
        return {
            "economic_price": float(self.bid), "displayed_depth": float(self.depth),
            "quote_id": f"test-exit:{self.ask}:{self.bid}:{observed}",
            "source_server_timestamp": datetime.fromtimestamp(observed, timezone.utc).isoformat(),
            "source_timestamp_ms": int(observed * 1000),
            "received_at": datetime.fromtimestamp(observed, timezone.utc).isoformat(),
        }, "test"

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


class TimestampedFeed(Feed):
    """Complete top of book with a deterministic exchange timestamp."""

    def __init__(self, ask: str, observed_at: float, bid: str = "0.45", depth: str = "10.00") -> None:
        super().__init__(ask, bid, depth)
        self.observed_at = observed_at

    def executable_shadow_quote(self, ticker: str, side: str, quantity: float, age: float):
        quote, source = super().executable_shadow_quote(ticker, side, quantity, age)
        observed = datetime.fromtimestamp(self.observed_at, timezone.utc).isoformat()
        quote.update({
            "quote_id": f"timestamped:{self.ask}:{self.bid}:{self.observed_at}",
            "source_server_timestamp": observed,
            "source_timestamp_ms": int(self.observed_at * 1000),
            "received_at": observed,
        })
        return quote, source


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

    def test_market_metadata_preserves_exchange_btc_target_without_ticker_parsing(self) -> None:
        metadata = market_metadata({
            "ticker": "KXBTC15M-do-not-parse-me",
            "open_time": "2026-08-27T20:00:00Z",
            "close_time": "2026-08-27T20:15:00Z",
            "status": "active",
            "title": "BTC price up in next 15 mins?",
            "yes_sub_title": "Target Price: $80,019.40",
            "no_sub_title": "Target price: TBD",
            "floor_strike": 80019.4,
            "strike_type": "greater_or_equal",
        })
        self.assertIsNotNone(metadata)
        assert metadata is not None
        self.assertEqual(metadata["btc_target_capture_version"], BTC_TARGET_CAPTURE_CONTRACT_VERSION)
        self.assertEqual(metadata["btc_target_price"], "80019.4")
        self.assertEqual(metadata["btc_target_price_display"], "$80,019.40")
        self.assertEqual(metadata["btc_target_source"], "floor_strike")
        self.assertEqual(metadata["btc_target_comparison"], "greater_or_equal")
        self.assertEqual(metadata["btc_target_status"], "AVAILABLE")

    def test_btc_target_subtitle_fallback_and_unavailable_state_are_explicit(self) -> None:
        fallback = btc_target_metadata({
            "yes_sub_title": "Target Price: $81,234.56", "strike_type": "greater_or_equal",
        })
        self.assertEqual(fallback["btc_target_price"], "81234.56")
        self.assertEqual(fallback["btc_target_price_display"], "$81,234.56")
        self.assertEqual(fallback["btc_target_source"], "yes_sub_title")
        unavailable = btc_target_metadata({"ticker": "KXBTC15M-contains-no-reliable-target"})
        self.assertEqual(unavailable["btc_target_status"], "UNAVAILABLE")
        self.assertIsNone(unavailable["btc_target_price"])

    def test_signal_and_triggered_ladder_persist_the_same_btc_target(self) -> None:
        ticker = "KXBTC15M-target-persistence"
        opened = time.time() - 10
        feed = KalshiLiveFeed(auth=None)
        feed.set_tickers([ticker])
        feed.subscription_started_epoch[ticker] = opened - 5
        feed._handle(json.dumps({
            "type": "ticker", "msg": {
                "market_ticker": ticker, "yes_bid_dollars": "0.54", "yes_ask_dollars": "0.55",
                "ts_ms": int((opened + 1) * 1000),
            },
        }))
        engine = self.engine()
        record = engine.set_signal({
            "ticker": ticker, "open_epoch": opened, "close_epoch": opened + 900,
            "floor_strike": "80123.45", "strike_type": "greater_or_equal",
            "yes_sub_title": "Target Price: $80,123.45",
        }, {"outcome": "no", "ticker": "KXBTC15M-prior"})
        engine.capture_opening_price_reference(feed, record, opened + 1)
        self.assertTrue(engine.observe_opening_cross_ladder(feed, record, opened + 2))
        self.assertEqual(record["btc_target_price"], "80123.45")
        self.assertEqual(record["opening_cross_ladder"]["btc_target_price"], "80123.45")
        self.assertEqual(
            record["opening_cross_ladder"]["triggered"]["53"]["btc_target_price"], "80123.45",
        )
        audit_rows = [json.loads(line) for line in engine.ledger_path.read_text(encoding="utf-8").splitlines()]
        self.assertTrue(any(row.get("btc_target_price") == "80123.45" for row in audit_rows))

    def test_price_only_feed_retains_earliest_post_open_selected_ask_without_depth(self) -> None:
        feed = KalshiLiveFeed(auth=None)
        ticker = "KXBTC15M-first-price-only"
        feed.set_tickers([ticker])
        opened = time.time() + 10
        feed.subscription_started_epoch[ticker] = opened - 30
        feed._handle(json.dumps({
            "type": "ticker",
            "msg": {
                "market_ticker": ticker,
                "yes_bid_dollars": "0.56", "yes_ask_dollars": "0.57",
                "ts_ms": int((opened + 0.125) * 1000),
            },
        }))
        # A later full-depth message must not replace the first price-only
        # opening reference.
        feed._handle(json.dumps({
            "type": "ticker",
            "msg": {
                "market_ticker": ticker,
                "yes_bid_dollars": "0.61", "yes_ask_dollars": "0.62",
                "yes_bid_size_fp": "20.00", "yes_ask_size_fp": "20.00",
                "ts_ms": int((opened + 8.5) * 1000),
            },
        }))
        quote, reason = feed.first_post_open_price_quote(ticker, "yes", opened)
        self.assertEqual(reason, "first_post_open_selected_side_price")
        self.assertIsNotNone(quote)
        assert quote is not None
        self.assertEqual(quote["economic_price"], 0.57)
        self.assertEqual(quote["selected_best_bid"], 0.56)
        self.assertFalse(quote["displayed_depth_available"])
        self.assertIsNone(quote["displayed_depth"])
        self.assertTrue(quote["coverage_complete_from_market_open"])
        self.assertAlmostEqual(quote["selected_component_epoch"], opened + 0.125, delta=0.0011)

    def test_first_selected_side_component_does_not_wait_for_opposite_price(self) -> None:
        feed = KalshiLiveFeed(auth=None)
        ticker = "KXBTC15M-first-single-component"
        feed.set_tickers([ticker])
        opened = time.time() + 10
        feed.subscription_started_epoch[ticker] = opened - 10
        # Kalshi ticker updates may contain only the component that changed.
        # The opening YES ask is still the selected-side executable price even
        # though a YES bid has not arrived in this socket session yet.
        feed._handle(json.dumps({
            "type": "ticker", "msg": {
                "market_ticker": ticker, "yes_ask_dollars": "0.58",
                "ts_ms": int((opened + 0.05) * 1000),
            },
        }))
        feed._handle(json.dumps({
            "type": "ticker", "msg": {
                "market_ticker": ticker, "yes_bid_dollars": "0.60",
                "yes_ask_dollars": "0.61", "ts_ms": int((opened + 1.0) * 1000),
            },
        }))

        quote, reason = feed.first_post_open_price_quote(ticker, "yes", opened)
        self.assertEqual(reason, "first_post_open_selected_side_price")
        self.assertIsNotNone(quote)
        assert quote is not None
        self.assertEqual(quote["economic_price"], 0.58)
        self.assertIsNone(quote["selected_best_bid"])
        self.assertAlmostEqual(quote["selected_component_epoch"], opened + 0.05, delta=0.0011)

    def test_no_side_opening_ask_uses_first_fresh_yes_bid_component(self) -> None:
        feed = KalshiLiveFeed(auth=None)
        ticker = "KXBTC15M-first-no-price"
        feed.set_tickers([ticker])
        opened = time.time() + 10
        feed.subscription_started_epoch[ticker] = opened - 5
        # Cache a pre-open ask, then update only YES bid after open. The NO
        # executable ask is 1 - YES bid and is immediately fresh.
        feed._handle(json.dumps({
            "type": "ticker", "msg": {
                "market_ticker": ticker, "yes_bid_dollars": "0.44",
                "yes_ask_dollars": "0.46", "ts_ms": int((opened - 1) * 1000),
            },
        }))
        feed._handle(json.dumps({
            "type": "ticker", "msg": {
                "market_ticker": ticker, "yes_bid_dollars": "0.47",
                "ts_ms": int((opened + 0.2) * 1000),
            },
        }))
        quote, _ = feed.first_post_open_price_quote(ticker, "no", opened)
        self.assertIsNotNone(quote)
        assert quote is not None
        self.assertEqual(quote["economic_price"], 0.53)
        self.assertAlmostEqual(quote["selected_component_epoch"], opened + 0.2, delta=0.0011)

    def test_post_open_subscription_is_persisted_as_partial_and_cannot_seed_entry(self) -> None:
        feed = KalshiLiveFeed(auth=None)
        ticker = "KXBTC15M-partial-opening-price"
        feed.set_tickers([ticker])
        opened = time.time() - 20
        feed.subscription_started_epoch[ticker] = opened + 10
        feed._handle(json.dumps({
            "type": "ticker", "msg": {
                "market_ticker": ticker, "yes_bid_dollars": "0.54", "yes_ask_dollars": "0.55",
                "ts_ms": int((opened + 10.1) * 1000),
            },
        }))
        engine = self.engine()
        record = engine.set_signal(
            {"ticker": ticker, "open_epoch": opened, "close_epoch": opened + 900},
            {"outcome": "no", "ticker": "KXBTC15M-prior"},
        )
        reference = engine.capture_opening_price_reference(feed, record, opened + 10.2)
        self.assertIsNotNone(reference)
        assert reference is not None
        self.assertFalse(reference["coverage_complete_from_market_open"])
        self.assertEqual(record["opening_price_reference_status"], "PARTIAL")
        self.assertIsNone(engine.freeze_initial_signal_price(feed, record, opened + 10.3))
        self.assertEqual(record["initial_signal_price_wait_reason"], "partial_opening_price_coverage")
        self.assertFalse(record["delayed_entry_tracking"]["coverage_complete_from_initial_quote"])

    def test_first_60_second_cross_activates_independent_ladder_and_tracks_all_rungs(self) -> None:
        feed = KalshiLiveFeed(auth=None)
        ticker = "KXBTC15M-opening-cross-ladder"
        feed.set_tickers([ticker])
        opened = time.time() - 100
        feed.subscription_started_epoch[ticker] = opened - 10

        def price(at: float, bid: str, ask: str) -> None:
            feed._handle(json.dumps({
                "type": "ticker", "msg": {
                    "market_ticker": ticker, "yes_bid_dollars": bid,
                    "yes_ask_dollars": ask, "ts_ms": int(at * 1000),
                },
            }))

        price(opened + 1, "0.51", "0.52")
        price(opened + 5, "0.54", "0.55")
        price(opened + 10, "0.48", "0.49")
        price(opened + 20, "0.38", "0.39")
        price(opened + 30, "0.28", "0.29")
        price(opened + 40, "0.18", "0.19")
        price(opened + 50, "0.08", "0.09")
        # Outside the trigger window: it may update an active ladder path but
        # cannot create the >=56c threshold cohort.
        price(opened + 61, "0.59", "0.60")
        feed.public_trades[ticker] = [
            {
                "trade_id": f"trade-{index}", "ticker": ticker, "yes_price": value,
                "no_price": None, "count": quantity,
                "source_server_timestamp": datetime.fromtimestamp(opened + seconds, timezone.utc).isoformat(),
                "received_at": datetime.fromtimestamp(opened + seconds, timezone.utc).isoformat(),
            }
            for index, (seconds, value, quantity) in enumerate((
                (11, "0.49", "1.00"), (21, "0.39", "2.00"),
                (31, "0.29", "4.00"), (41, "0.19", "8.00"),
                (51, "0.09", "16.00"),
            ), start=1)
        ]
        engine = self.engine()
        engine.state["circuit_breaker"] = {
            "blocked": True, "reason": "test_entry_breaker",
            "triggered_at": datetime.now(timezone.utc).isoformat(),
        }
        record = engine.set_signal(
            {"ticker": ticker, "open_epoch": opened, "close_epoch": opened + 900},
            {"outcome": "no", "ticker": "KXBTC15M-prior"},
        )
        engine.capture_opening_price_reference(feed, record, opened + 1)
        self.assertTrue(engine.observe_opening_cross_ladder(feed, record, opened + 62))
        tracker = record["opening_cross_ladder"]
        self.assertEqual(sorted(tracker["triggered"]), ["53", "54", "55"])
        self.assertTrue(tracker["window_complete"])
        for threshold in (53, 54, 55):
            lane = tracker["triggered"][str(threshold)]
            for level in (50, 40, 30, 20, 10):
                self.assertTrue(lane["rungs"][str(level)]["touched"])
                self.assertTrue(lane["rungs"][str(level)]["simulated_full_fill"])
        engine.finalize_settlement_analytics(record, "yes")
        self.assertEqual(tracker["triggered"]["53"]["gross_no_stop_pnl"], "25.300")
        summary = engine.opening_cross_ladder_performance()["thresholds"]
        self.assertEqual(summary["53"]["threshold_crosses_first_60_seconds"], 1)
        self.assertEqual(summary["53"]["directional_wins"], 1)
        self.assertEqual(summary["56"]["threshold_crosses_first_60_seconds"], 0)

    def test_opening_cross_ladder_is_shadow_only(self) -> None:
        temporary = Path(tempfile.mkdtemp())
        live_engine = LiveEngine(
            dict(self.config), default_state(self.config), temporary / "state.json",
            temporary / "audit.jsonl", dry_run=False,
        )
        ticker = "KXBTC15M-no-live-cross-ladder"
        opened = time.time() - 5
        record = live_engine.set_signal(
            {"ticker": ticker, "open_epoch": opened, "close_epoch": opened + 900},
            {"outcome": "no", "ticker": "KXBTC15M-prior"},
        )
        feed = KalshiLiveFeed(auth=None)
        feed.set_tickers([ticker])
        feed.subscription_started_epoch[ticker] = opened - 10
        feed._handle(json.dumps({
            "type": "ticker", "msg": {
                "market_ticker": ticker, "yes_bid_dollars": "0.54",
                "yes_ask_dollars": "0.55", "ts_ms": int((opened + 1) * 1000),
            },
        }))

        self.assertIsNone(record["opening_cross_ladder"])
        self.assertFalse(live_engine.observe_opening_cross_ladder(feed, record, opened + 2))
        self.assertIsNone(record["opening_cross_ladder"])

    def test_cross_ladder_public_trade_volume_is_not_reused_across_rungs(self) -> None:
        feed = KalshiLiveFeed(auth=None)
        ticker = "KXBTC15M-ladder-volume-allocation"
        feed.set_tickers([ticker])
        opened = time.time() - 100
        feed.subscription_started_epoch[ticker] = opened - 5
        for seconds, bid, ask in ((1, "0.54", "0.55"), (10, "0.08", "0.09")):
            feed._handle(json.dumps({
                "type": "ticker", "msg": {
                    "market_ticker": ticker, "yes_bid_dollars": bid,
                    "yes_ask_dollars": ask, "ts_ms": int((opened + seconds) * 1000),
                },
            }))
        trade_time = datetime.fromtimestamp(opened + 11, timezone.utc).isoformat()
        feed.public_trades[ticker] = [{
            "trade_id": "single-16-share-print", "ticker": ticker,
            "yes_price": "0.09", "no_price": None, "count": "16.00",
            "source_server_timestamp": trade_time, "received_at": trade_time,
        }]
        engine = self.engine()
        record = engine.set_signal(
            {"ticker": ticker, "open_epoch": opened, "close_epoch": opened + 900},
            {"outcome": "no", "ticker": "KXBTC15M-prior"},
        )
        engine.capture_opening_price_reference(feed, record, opened + 1)
        engine.observe_opening_cross_ladder(feed, record, opened + 12)
        rungs = record["opening_cross_ladder"]["triggered"]["53"]["rungs"]
        self.assertEqual([rungs[str(level)]["simulated_filled_quantity"] for level in (50, 40, 30, 20, 10)], [
            "1.00", "2.00", "4.00", "8.00", "1.00",
        ])
        self.assertFalse(rungs["10"]["simulated_full_fill"])

    def test_cross_ladder_fill_cursor_survives_feed_restart_without_erasing_or_reusing_volume(self) -> None:
        ticker = "KXBTC15M-ladder-restart-cursor"
        opened = time.time() - 100
        first_feed = KalshiLiveFeed(auth=None)
        first_feed.set_tickers([ticker])
        first_feed.subscription_started_epoch[ticker] = opened - 5
        first_feed._handle(json.dumps({
            "type": "ticker", "msg": {
                "market_ticker": ticker, "yes_bid_dollars": "0.54",
                "yes_ask_dollars": "0.55", "ts_ms": int((opened + 1) * 1000),
            },
        }))
        first_trade_time = datetime.fromtimestamp(opened + 2, timezone.utc).isoformat()
        first_feed.public_trades[ticker] = [{
            "trade_id": "before-restart-49", "ticker": ticker,
            "yes_price": "0.49", "no_price": None, "count": "1.00",
            "source_server_timestamp": first_trade_time, "received_at": first_trade_time,
        }]
        engine = self.engine()
        record = engine.set_signal(
            {"ticker": ticker, "open_epoch": opened, "close_epoch": opened + 900},
            {"outcome": "no", "ticker": "KXBTC15M-prior"},
        )
        engine.capture_opening_price_reference(first_feed, record, opened + 1)
        engine.observe_opening_cross_ladder(first_feed, record, opened + 3)
        rungs = record["opening_cross_ladder"]["triggered"]["53"]["rungs"]
        self.assertEqual(rungs["50"]["simulated_filled_quantity"], "1.00")

        # A replacement worker has a new feed session and no old in-memory
        # trades. Existing fills remain durable; only the new 39c print may
        # allocate volume, and a repeated observation is idempotent.
        replacement_feed = KalshiLiveFeed(auth=None)
        replacement_feed.set_tickers([ticker])
        replacement_feed.subscription_started_epoch[ticker] = opened + 10
        engine.observe_opening_cross_ladder(replacement_feed, record, opened + 11)
        self.assertEqual(rungs["50"]["simulated_filled_quantity"], "1.00")
        second_trade_time = datetime.fromtimestamp(opened + 12, timezone.utc).isoformat()
        replacement_feed.public_trades[ticker] = [{
            "trade_id": "after-restart-39", "ticker": ticker,
            "yes_price": "0.39", "no_price": None, "count": "2.00",
            "source_server_timestamp": second_trade_time, "received_at": second_trade_time,
        }]
        engine.observe_opening_cross_ladder(replacement_feed, record, opened + 13)
        self.assertEqual(rungs["50"]["simulated_filled_quantity"], "1.00")
        self.assertEqual(rungs["40"]["simulated_filled_quantity"], "2.00")
        self.assertEqual(rungs["30"]["simulated_filled_quantity"], "0.00")
        engine.observe_opening_cross_ladder(replacement_feed, record, opened + 14)
        self.assertEqual(rungs["40"]["simulated_filled_quantity"], "2.00")

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

    def test_explicit_gtc_contract_migrates_only_the_exact_implicit_gtc_hash(self) -> None:
        temporary = Path(tempfile.mkdtemp()) / "state.json"
        implicit_gtc_config = dict(self.config)
        implicit_gtc_config.pop("maker_order_time_in_force")
        state = default_state(implicit_gtc_config)
        state["sizing"] = {
            "base_share_count": "1.00", "recovery_exponent": 7,
            "recovery_cycle_pnl": "-0.42", "next_base_threshold": "350.00",
        }
        state["markets"] = {
            "KXBTC15M-existing": {
                "ticker": "KXBTC15M-existing", "status": "ZERO_FILL",
                "initial_signal_price_cents": 42,
            },
        }
        save_state(temporary, state)

        migrated = load_state(temporary, self.config)
        self.assertEqual(migrated["sizing"], state["sizing"])
        self.assertEqual(migrated["markets"], state["markets"])
        self.assertEqual(
            migrated["config_migrations"][-1]["kind"],
            "make_existing_gtc_order_contract_explicit",
        )
        self.assertEqual(migrated["config_hash"], config_hash(self.config))

    def test_delayed_entry_analytics_config_migrates_without_resetting_state(self) -> None:
        temporary = Path(tempfile.mkdtemp()) / "state.json"
        prior_config = dict(self.config)
        prior_config.pop("delayed_entry_tracking_enabled")
        prior_config.pop("delayed_entry_threshold_cents")
        state = default_state(prior_config)
        state["sizing"] = {
            "base_share_count": "1.00", "recovery_exponent": 7,
            "recovery_cycle_pnl": "-0.88", "next_base_threshold": "350.00",
        }
        save_state(temporary, state)

        migrated = load_state(temporary, self.config)
        self.assertEqual(migrated["sizing"], state["sizing"])
        self.assertEqual(
            migrated["config_migrations"][-1]["kind"],
            "enable_compact_full_market_delayed_entry_analytics",
        )
        self.assertEqual(migrated["config_hash"], config_hash(self.config))

    def test_gtc_strategy_timeout_removal_migrates_exact_checkpoint_without_reset(self) -> None:
        temporary = Path(tempfile.mkdtemp()) / "state.json"
        bounded_config = dict(self.config)
        bounded_config["entry_timeout_seconds"] = bounded_config.pop("opening_quote_capture_seconds")
        bounded_config.pop("entry_order_lifetime")
        state = default_state(bounded_config)
        state["sizing"] = {
            "base_share_count": "1.00", "recovery_exponent": 11,
            "recovery_cycle_pnl": "-0.73", "next_base_threshold": "350.00",
        }
        state["markets"] = {
            "KXBTC15M-resting": {
                "ticker": "KXBTC15M-resting", "status": "ENTRY_PENDING",
                "entry_deadline_epoch": 1_060,
                "entry_orders": [{"remaining_count": "1.00", "time_in_force": "good_till_canceled"}],
            },
        }
        save_state(temporary, state)

        migrated = load_state(temporary, self.config)
        self.assertEqual(migrated["sizing"], state["sizing"])
        self.assertEqual(migrated["markets"], state["markets"])
        self.assertEqual(migrated["config_migrations"][-1]["kind"], "remove_gtc_strategy_timeout")
        self.assertEqual(migrated["config_hash"], config_hash(self.config))

    def test_unrelated_configuration_change_still_fails_closed(self) -> None:
        temporary = Path(tempfile.mkdtemp()) / "state.json"
        save_state(temporary, default_state(self.config))
        with self.assertRaisesRegex(RuntimeError, "configuration hash differs"):
            load_state(temporary, dict(self.config, max_position="99.00"))

    def test_reviewed_strategy_inputs_migrate_only_flat_state_and_preserve_cycle(self) -> None:
        temporary = Path(tempfile.mkdtemp()) / "state.json"
        state = default_state(self.config)
        state["sizing"] = {
            "base_share_count": "1.00", "recovery_exponent": 2,
            "recovery_cycle_pnl": "-0.13", "next_base_threshold": "350.00",
        }
        state["cycle_strategy_parameters"] = {
            name: self.config[name] for name in (
                "recovery_multiplier", "first_base_threshold", "threshold_growth_multiplier",
                "base_increment", "starting_base", "max_position",
            )
        }
        save_state(temporary, state)
        changed = dict(
            self.config,
            starting_base="2.00",
            recovery_multiplier="1.02",
            threshold_growth_multiplier="1.02",
            first_base_threshold="400.00",
            base_increment="0.25",
            hybrid_hard_stop_cents=40,
            hybrid_stop_trigger_cents=41,
            hybrid_maker_exit_cents=42,
        )

        migrated = load_state(temporary, changed)
        self.assertEqual(migrated["sizing"], state["sizing"])
        self.assertEqual(migrated["active_config_snapshot"], changed)
        self.assertEqual(
            migrated["config_migrations"][-1]["kind"],
            "apply_reviewed_flat_state_strategy_tuning",
        )
        engine = LiveEngine(
            changed, migrated, temporary, temporary.with_suffix(".jsonl"), dry_run=True,
        )
        self.assertEqual(engine.current_parameters().recovery_multiplier, Decimal("1.01"))
        self.assertEqual(engine.current_parameters().starting_base, Decimal("1.00"))

    def test_reviewed_strategy_inputs_fail_closed_while_order_is_active(self) -> None:
        temporary = Path(tempfile.mkdtemp()) / "state.json"
        state = default_state(self.config)
        state["current_order_id"] = "existing-order"
        state["markets"] = {"KXBTC15M-active": {"status": "ENTRY_PENDING"}}
        save_state(temporary, state)
        changed = dict(self.config, recovery_multiplier="1.02", threshold_growth_multiplier="1.02")
        with self.assertRaisesRegex(RuntimeError, "configuration hash differs"):
            load_state(temporary, changed)

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
                    self.detail_calls = []

                async def get_raw_json(self, path, params):
                    self.calls.append((path, params))
                    self.assert_bounded(params)
                    return {"markets": [
                        market("KXBTC15M-prior", now - 930, now - 30, "finalized", "yes"),
                        market("KXBTC15M-active", now - 30, now + 870, "active"),
                        market("KXBTC15M-upcoming", now + 870, now + 1770, "initialized"),
                    ]}

                async def get_market(self, ticker):
                    self.detail_calls.append(ticker)
                    return {
                        "ticker": ticker, "floor_strike": "80276.37",
                        "strike_type": "greater_or_equal",
                        "yes_sub_title": "Target Price: $80,276.37",
                    }

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
            self.assertCountEqual(rest.detail_calls, [
                "KXBTC15M-prior", "KXBTC15M-active", "KXBTC15M-upcoming",
            ])
            self.assertEqual(active and active["btc_target_price"], "80276.37")
            self.assertEqual(active and active["btc_target_source"], "floor_strike")
            # A second refresh reuses captured targets instead of hammering
            # the per-market endpoint every discovery interval.
            await engine.discover(rest)
            self.assertEqual(len(rest.detail_calls), 3)
        asyncio.run(scenario())

    def test_millisecond_exchange_timestamp_and_price_only_quote_enable_provisional_signal(self) -> None:
        boundary = 1_786_996_800.0
        self.assertEqual(epoch(int((boundary - .2) * 1000)), boundary - .2)
        self.assertEqual(epoch(str(int((boundary - .2) * 1000))), boundary - .2)
        feed = KalshiLiveFeed(auth=None)
        feed.set_tickers(["prior"])
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

    def test_v11_submits_one_signal_minus_one_cent_post_only_limit(self) -> None:
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
            self.assertTrue(rest.calls[0]["post_only"])
            self.assertEqual(rest.calls[0]["tif"], "good_till_canceled")
            self.assertIsNone(rest.calls[0]["expiration_time"])
            self.assertEqual(rest.calls[0]["position_price"], 0.59)
            self.assertEqual(record["initial_signal_price_cents"], 60)
            self.assertEqual(record["entry_limit_cents"], 59)
            self.assertEqual(record["status"], "ENTRY_PENDING")
            self.assertEqual(record["actual_quantity"], "0.00")
            self.assertTrue(record["maker_entry_submission_attempted"])
            self.assertEqual(record["entry_orders"][0]["entry_phase"], "maker")
            self.assertEqual(record["entry_execution_type"], "none")
            self.assertEqual(record["entry_execution_summary"]["maker_limit_filled_quantity"], "0")
            self.assertEqual(record["entry_execution_summary"]["market_ioc_filled_quantity"], "0")
            self.assertEqual(engine.state["entry_execution_metrics"]["maker_limit_fill_markets"], 0)
            self.assertEqual(engine.state["entry_execution_metrics"]["market_ioc_fill_markets"], 0)
        asyncio.run(scenario())

    def test_v11_unconfirmed_maker_cancellation_blocks_all_replacement(self) -> None:
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
            await engine.manage_entry(rest, feed, record, 1_901)
            self.assertEqual(record["status"], "ENTRY_CANCEL_UNCONFIRMED")
            self.assertTrue(engine.state["circuit_breaker"]["blocked"])
            self.assertEqual(record["entry_cancel_pending"]["next_action"], "finish_entry")
            self.assertEqual(len(rest.calls), 1, "there is no IOC replacement in v11")
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

    def test_exit_residual_never_exceeds_actual_filled_entry(self) -> None:
        engine = self.engine()
        record = {
            "actual_quantity": "1.00", "exit_orders": [
                {"fill_count": "0.35"}, {"fill_count": "0.40"},
            ],
        }
        self.assertEqual(engine.local_remaining_position(record), Decimal("0.25"))

    def test_shadow_hybrid_hard_stop_is_non_resting_and_reduce_only(self) -> None:
        async def scenario() -> None:
            engine = self.engine()
            record = engine.set_signal(
                {"ticker": "KXBTC15M-shadow-taker-stop", "open_epoch": 1_000, "close_epoch": 1_900},
                {"outcome": "no", "ticker": "KXBTC15M-prior"},
            )
            record.update({"status": "POSITION_OPEN", "actual_quantity": "1.00", "entry_orders": [{
                "entry_phase": "maker", "fill_count": "1.00", "remaining_count": "0",
                "average_fill_price": "0.50", "fees_paid": "0",
            }]})
            engine.state["markets"][record["ticker"]] = record
            engine.state["active_market"] = record["ticker"]
            feed = Feed("0.60", "0.44", "10")
            await engine.start_hybrid_maker_exit(EntryRest(), feed, record, Decimal("0.45"), entries_confirmed=True)
            await engine.manage_stop(EntryRest(), feed, record)
            stop = record["exit_orders"][-1]
            self.assertEqual(stop["order_type"], "reduce_only_exit_ioc")
            self.assertEqual(stop["time_in_force"], "immediate_or_cancel")
            self.assertFalse(stop["post_only"])
            self.assertTrue(stop["reduce_only"])
            self.assertEqual(record["status"], "CLOSED")
            self.assertEqual(record["exit_classification"], "HARD_STOP_ONLY")
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
            self.assertEqual(record["entry_rejection_quote"]["reason"], "derived_limit_at_or_below_hybrid_hard_stop")
            self.assertEqual(engine.state["sizing"].get("recovery_exponent", 0), 0)
        asyncio.run(scenario())

    def test_maker_limit_uses_immutable_fresh_selected_side_ask_minus_one(self) -> None:
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
            self.assertEqual(rest.calls[0]["position_price"], 0.51)
            self.assertTrue(rest.calls[0]["post_only"])
            self.assertEqual(record["initial_signal_price_cents"], 52)
            self.assertEqual(record["entry_limit_cents"], 51)
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
            await engine.submit_entry(rest, feed, record, 1_004)
            self.assertEqual(len(rest.calls), 1)
            self.assertEqual(record["entry_limit_cents"], 51)
            self.assertEqual(Decimal(record["opening_quote_observations"][-1]["selected_best_ask"]), Decimal("0.70"))
            self.assertEqual(Decimal(record["opening_quote_capture"]["max_selected_best_ask"]), Decimal("0.70"))
            engine.capture_opening_quote(feed, record, 1_060)
            self.assertIsNotNone(record["opening_quote_capture"]["completed_at"])
        asyncio.run(scenario())

    def test_below_53_initial_quote_tracks_first_later_executable_ask_after_60_seconds(self) -> None:
        engine = self.engine()
        market_open = 1_800_000_000.0
        feed = TimestampedFeed("0.52", market_open + 0.5, depth="4.25")
        record = engine.set_signal(
            {"ticker": "KXBTC15M-delayed-53", "open_epoch": market_open, "close_epoch": market_open + 900},
            {"outcome": "no", "ticker": "KXBTC15M-prior"},
        )
        self.assertEqual(engine.freeze_initial_signal_price(feed, record, market_open + 0.5), Decimal("0.51"))
        tracker = record["delayed_entry_tracking"]
        self.assertTrue(tracker["eligible"])
        self.assertEqual(tracker["status"], "WATCHING")

        feed.ask = "0.54"
        feed.observed_at = market_open + 65.25
        self.assertTrue(engine.observe_price_analytics(feed, record))
        self.assertEqual(tracker["status"], "THRESHOLD_REACHED")
        self.assertEqual(tracker["first_entry_price_cents"], 54)
        self.assertEqual(tracker["first_entry_after_open_seconds"], 65.25)
        self.assertTrue(tracker["first_entry_after_opening_capture_window"])
        self.assertEqual(tracker["displayed_ask_depth"], "4.25")
        self.assertTrue(tracker["configured_quantity_fully_fillable"])
        self.assertEqual(record["minimum_selected_price_cents"], 52)

        engine.finalize_settlement_analytics(record, "yes")
        self.assertEqual(tracker["status"], "SETTLED_WIN")
        self.assertEqual(tracker["no_stop_gross_pnl_per_share"], "0.46")
        summary = engine.delayed_entry_performance()
        self.assertEqual(summary["entries_after_opening_capture_window"], 1)
        self.assertEqual(summary["directional_wins"], 1)
        self.assertEqual(summary["directional_losses"], 0)

    def test_delayed_53_tracker_never_submits_a_second_live_order(self) -> None:
        async def scenario() -> None:
            directory = Path(tempfile.mkdtemp())
            engine = LiveEngine(
                dict(self.config), default_state(self.config),
                directory / "state", directory / "ledger", dry_run=False,
            )
            rest = LiveFallbackRest()
            market_open = 1_800_000_000.0
            feed = TimestampedFeed("0.52", market_open + 0.5)
            record = engine.set_signal(
                {"ticker": "KXBTC15M-delayed-no-order", "open_epoch": market_open, "close_epoch": market_open + 900},
                {"outcome": "no", "ticker": "KXBTC15M-prior"},
            )
            await engine.submit_entry(rest, feed, record, market_open + 0.5)
            self.assertEqual(len(rest.calls), 1)
            feed.ask = "0.55"
            feed.observed_at = market_open + 500
            engine.observe_price_analytics(feed, record)
            self.assertEqual(len(rest.calls), 1)
            self.assertTrue(record["delayed_entry_tracking"]["hypothetical_fill"])
        asyncio.run(scenario())

    def test_at_or_above_53_initial_quote_is_not_in_delayed_cohort(self) -> None:
        engine = self.engine()
        market_open = 1_800_000_000.0
        feed = TimestampedFeed("0.53", market_open + 0.5)
        record = engine.set_signal(
            {"ticker": "KXBTC15M-not-delayed-53", "open_epoch": market_open, "close_epoch": market_open + 900},
            {"outcome": "no", "ticker": "KXBTC15M-prior"},
        )
        engine.freeze_initial_signal_price(feed, record, market_open + 0.5)
        tracker = record["delayed_entry_tracking"]
        self.assertFalse(tracker["eligible"])
        self.assertEqual(tracker["status"], "INELIGIBLE_INITIAL_AT_OR_ABOVE_THRESHOLD")

    def test_delayed_tracker_persists_no_cross_without_claiming_a_fill(self) -> None:
        engine = self.engine()
        market_open = 1_800_000_000.0
        feed = TimestampedFeed("0.50", market_open + 0.5)
        record = engine.set_signal(
            {"ticker": "KXBTC15M-delayed-no-cross", "open_epoch": market_open, "close_epoch": market_open + 900},
            {"outcome": "no", "ticker": "KXBTC15M-prior"},
        )
        engine.freeze_initial_signal_price(feed, record, market_open + 0.5)
        feed.ask = "0.52"
        feed.observed_at = market_open + 899
        engine.observe_price_analytics(feed, record)
        engine.finalize_settlement_analytics(record, "no")
        tracker = record["delayed_entry_tracking"]
        self.assertEqual(tracker["status"], "NO_THRESHOLD_CROSS_OBSERVED")
        self.assertFalse(tracker["hypothetical_fill"])
        self.assertIsNone(tracker["no_stop_gross_pnl_per_share"])

    def test_delayed_tracker_keeps_complete_quote_coverage_while_entry_breaker_is_latched(self) -> None:
        engine = self.engine()
        engine.state["circuit_breaker"] = {
            "blocked": True,
            "reason": "max_daily_realized_loss",
            "triggered_at": datetime.now(timezone.utc).isoformat(),
        }
        market_open = 1_800_000_000.0
        feed = TimestampedFeed("0.52", market_open + 1.25, depth="3.00")
        record = engine.set_signal(
            {"ticker": "KXBTC15M-delayed-breaker", "open_epoch": market_open, "close_epoch": market_open + 900},
            {"outcome": "no", "ticker": "KXBTC15M-prior"},
        )

        # The primary execution reference remains unset because no entry path
        # ran, while the independent research tracker starts from the first
        # fresh executable quote and retains complete coverage.
        self.assertTrue(engine.observe_price_analytics(feed, record))
        tracker = record["delayed_entry_tracking"]
        self.assertIsNone(record["initial_signal_price_cents"])
        self.assertEqual(tracker["initial_signal_price_cents"], 52)
        self.assertTrue(tracker["eligible"])
        self.assertTrue(tracker["coverage_complete_from_initial_quote"])
        self.assertEqual(tracker["status"], "WATCHING")

        feed.ask = "0.54"
        feed.observed_at = market_open + 75.5
        self.assertTrue(engine.observe_price_analytics(feed, record))
        self.assertEqual(tracker["status"], "THRESHOLD_REACHED")
        self.assertEqual(tracker["first_entry_price_cents"], 54)
        self.assertTrue(tracker["first_entry_after_opening_capture_window"])
        engine.finalize_settlement_analytics(record, "yes")
        summary = engine.delayed_entry_performance()
        self.assertEqual(summary["complete_coverage_signals"], 1)
        self.assertEqual(summary["partial_legacy_coverage_signals"], 0)
        self.assertEqual(summary["directional_wins"], 1)

    def test_delayed_tracker_marks_pre_restart_quote_gap_as_partial(self) -> None:
        engine = self.engine()
        market_open = 1_800_000_000.0
        record = engine.set_signal(
            {"ticker": "KXBTC15M-delayed-restart-gap", "open_epoch": market_open, "close_epoch": market_open + 900},
            {"outcome": "no", "ticker": "KXBTC15M-prior"},
        )
        record["opening_quote_observations"] = [{
            "quote_id": "pre-restart-quote",
            "source_timestamp_ms": int((market_open + 5) * 1000),
            "selected_best_ask": "0.50",
        }]
        feed = TimestampedFeed("0.54", market_open + 200, depth="3.00")

        self.assertTrue(engine.observe_price_analytics(feed, record))
        tracker = record["delayed_entry_tracking"]
        self.assertTrue(tracker["threshold_reached"])
        self.assertFalse(tracker["coverage_complete_from_initial_quote"])
        self.assertIn("pre-migration threshold crosses are unknown", tracker["migration_note"])

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
            self.assertEqual(record["status"], "ENTRY_PENDING")
            self.assertEqual(rest.created, 1)
            self.assertEqual(rest.calls[0]["position_price"], 0.53)
            self.assertEqual(record["entry_orders"][0]["entry_phase"], "maker")
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
            self.assertFalse(record.get("maker_entry_submission_attempted", False))
            self.assertEqual(record["initial_signal_price_wait_reason"], "preopen_or_unstamped_top_of_book")
            self.assertEqual(record["opening_quote_observations"], [])

            feed.quote_epoch = market_open + .1
            await engine.submit_entry(rest, feed, record, market_open + .1)
            self.assertEqual(record["status"], "ENTRY_PENDING")
            self.assertEqual(record["entry_execution_type"], "none")
            self.assertEqual(record["actual_quantity"], "0.00")
            self.assertEqual(record["entry_limit_cents"], 53)
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

    def test_v11_hybrid_trigger_is_fixed_at_45c_regardless_of_entry(self) -> None:
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
                "entry_orders": [{"entry_phase": "maker", "fill_count": "1.00", "remaining_count": "0", "average_fill_price": "0.52", "fees_paid": "0"}],
            })
            engine.state["active_market"] = high_entry["ticker"]
            await engine.manage_stop(rest, Feed("0.60", "0.46"), high_entry)
            self.assertEqual(high_entry["actual_average_entry_price"], "0.52")
            self.assertEqual(high_entry["effective_stop_price"], "0.45")
            self.assertEqual(high_entry["post_entry_stop_monitor"]["minimum_executable_bid"], "0.46")
            self.assertEqual(high_entry["status"], "POSITION_OPEN")
            await engine.manage_stop(rest, Feed("0.60", "0.45"), high_entry)
            self.assertEqual(high_entry["stop_trigger"]["effective_stop_price"], "0.45")
            self.assertEqual(high_entry["status"], "MAKER_EXIT_PENDING")
            await engine.manage_stop(rest, Feed("0.60", "0.44", "10"), high_entry)
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
                "entry_orders": [{"entry_phase": "maker", "fill_count": "1.00", "remaining_count": "0", "average_fill_price": "0.49", "fees_paid": "0"}],
            })
            engine.state["active_market"] = low_entry["ticker"]
            await engine.manage_stop(rest, Feed("0.60", "0.46"), low_entry)
            self.assertEqual(low_entry["effective_stop_price"], "0.45")
            self.assertEqual(low_entry["status"], "POSITION_OPEN")
        asyncio.run(scenario())

    def test_handoff_only_permits_the_middle_thirteen_minutes_without_pending_operations(self) -> None:
        engine = self.engine()
        engine.markets = [{"ticker": "KXBTC15M-current", "open_epoch": 1_000, "close_epoch": 1_900, "status": "active"}]
        engine.state["markets"]["KXBTC15M-current"] = {"ticker": "KXBTC15M-current", "status": "POSITION_OPEN"}
        self.assertFalse(engine.handoff_ready(1_059)[0])
        self.assertFalse(engine.handoff_ready(1_060)[0])
        self.assertFalse(engine.handoff_ready(1_841)[0])
        engine.state["markets"]["KXBTC15M-current"]["status"] = "CLOSED"
        self.assertTrue(engine.handoff_ready(1_300)[0])

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

    def test_runtime_checkpoint_ref_is_a_single_parentless_snapshot_and_main_is_unchanged(self) -> None:
        temporary = Path(tempfile.mkdtemp())
        remote = temporary / "remote.git"
        work = temporary / "work"
        subprocess.run(["git", "init", "--bare", str(remote)], check=True, capture_output=True)
        subprocess.run(["git", "init", "-b", "main", str(work)], check=True, capture_output=True)
        for key, value in (("user.name", "test"), ("user.email", "test@example.com")):
            subprocess.run(["git", "-C", str(work), "config", key, value], check=True)
        subprocess.run(["git", "-C", str(work), "remote", "add", "origin", str(remote)], check=True)
        state = work / "state.json"
        state.write_text('{"sequence":1}\n', encoding="utf-8")
        subprocess.run(["git", "-C", str(work), "add", "state.json"], check=True)
        subprocess.run(["git", "-C", str(work), "commit", "-m", "source"], check=True, capture_output=True)
        subprocess.run(["git", "-C", str(work), "push", "-u", "origin", "main"], check=True, capture_output=True)
        main_before = subprocess.check_output(["git", "-C", str(work), "rev-parse", "main"], text=True).strip()

        self.assertTrue(publish_runtime_snapshot((state,), "first", root=work))
        first = subprocess.check_output(
            ["git", "--git-dir", str(remote), "rev-parse", "runtime-state"], text=True,
        ).strip()
        state.write_text('{"sequence":2}\n', encoding="utf-8")
        self.assertTrue(publish_runtime_snapshot((state,), "second", root=work))
        second = subprocess.check_output(
            ["git", "--git-dir", str(remote), "rev-parse", "runtime-state"], text=True,
        ).strip()

        self.assertNotEqual(first, second)
        self.assertEqual(
            subprocess.check_output(
                ["git", "--git-dir", str(remote), "rev-list", "--count", "runtime-state"], text=True,
            ).strip(),
            "1",
        )
        parents = subprocess.check_output(
            ["git", "--git-dir", str(remote), "show", "-s", "--format=%P", "runtime-state"], text=True,
        ).strip()
        self.assertEqual(parents, "")
        self.assertEqual(
            subprocess.check_output(["git", "-C", str(work), "rev-parse", "main"], text=True).strip(),
            main_before,
        )
        self.assertEqual(
            subprocess.check_output(
                ["git", "--git-dir", str(remote), "show", "runtime-state:state.json"], text=True,
            ),
            '{"sequence":2}\n',
        )

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
