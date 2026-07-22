import asyncio
import importlib
import json
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from kalshi_btc15m_average_down import (
    DEFAULT_CONFIG,
    KalshiLiveFeed,
    LADDER_LEVELS,
    MLDirectionSelector,
    active_strategy_records,
    apply_config_overrides,
    classify_submission,
    client_order_id,
    choose_entry_side,
    consider_initial_entry,
    default_state,
    ensure_model_transition_shadow,
    ensure_inverse_shadow,
    ensure_ml_scalp_shadow,
    ensure_ml_weighted_trailing_scalp_shadows,
    exchange_position_guard,
    exchange_outcome_side,
    ladder_principal,
    performance_report,
    pause_error,
    reconcile_orders,
    recover_exchange_state,
    scheduled_trading_pause_active,
    settle_or_cancel,
    side_api_price,
    submit_ladder,
    market_is_tradeable,
    market_can_start_watcher,
    market_record,
    market_asks,
    managed_mechanical_order_role,
    model_transition_side_comparison,
    ml_live_directional_performance,
    inverse_shadow_performance,
    ml_scalp_shadow_performance,
    ml_weighted_trailing_ledger,
    save_ml_weighted_trailing_outputs,
    model_transition_shadow_performance,
    normalized_order_status,
    normalized_outcome_side,
    rung_order_activity,
    simulate_inverse_shadow,
    simulate_ml_scalp_shadow,
    simulate_model_transition_shadow,
    finalize_inverse_shadow,
    finalize_ml_scalp_shadow,
    finalize_model_transition_shadow,
    validate_config,
)
from datetime import datetime, timezone
from kalshi_ml_features import FEATURE_SCHEMA, ML_ONLY_FEATURE_COLUMNS, feature_values


class MechanicalAverageDownTests(unittest.TestCase):
    def test_default_is_one_hundredth_contract_per_rung(self):
        config = validate_config(DEFAULT_CONFIG)
        self.assertEqual(config["initial_position_size"], 0.01)
        self.assertEqual(config["max_contracts_per_market"], 0.04)
        self.assertEqual(ladder_principal(0.01), 0.01)
        self.assertEqual(config["market_refresh_seconds"], 15.0)
        self.assertEqual(config["order_reconcile_seconds"], 5.0)
        self.assertEqual(config["watch_start_grace_seconds"], 45.0)
        self.assertEqual(config["ml_min_confidence"], 0.5)
        self.assertEqual(config["inverse_shadow_position_size"], 1.0)
        self.assertEqual(config["model_transition_shadow_position_size"], 1.0)

    def test_ml_only_features_have_no_prophet_inputs(self):
        candles = pd.DataFrame({
            "ds": pd.date_range("2026-07-20T00:00:00Z", periods=61, freq="min"),
            "close": [60_000.0 + value for value in range(61)],
        })
        values = feature_values(
            candles, 60_050.0, [1, 0, 1, 1], pd.Timestamp("2026-07-20T00:15:00Z"),
        )
        self.assertEqual(set(values), set(ML_ONLY_FEATURE_COLUMNS))
        self.assertEqual(FEATURE_SCHEMA, "ml_only_raw_candles_settled_outcomes_v1")
        self.assertFalse(any("prophet" in name for name in ML_ONLY_FEATURE_COLUMNS))

    def test_live_ml_inference_import_does_not_load_legacy_forecast_runner(self):
        # The deployed average-down runner imports this module to prepare its
        # frozen ML side.  Keep the legacy forecasting runner out of that
        # import path so it cannot be a hidden live-inference dependency.
        sys.modules.pop("kalshi_ml_inference_live", None)
        sys.modules.pop("kalshibtc15minupordown", None)
        inference = importlib.import_module("kalshi_ml_inference_live")
        self.assertFalse("kalshibtc15minupordown" in sys.modules)
        self.assertEqual(inference.market_data.SERIES_TICKER, "KXBTC15M")

    def test_unfinished_ml_task_is_failed_at_market_open(self):
        async def scenario():
            with self.subTest("late task"):
                import tempfile
                from pathlib import Path

                with tempfile.TemporaryDirectory() as temporary:
                    base = Path(temporary)
                    (base / "rows.csv").write_text("placeholder\n", encoding="utf-8")
                    (base / "model.joblib").write_text("placeholder\n", encoding="utf-8")
                    selector = MLDirectionSelector(
                        base / "rows.csv", base / "model.joblib", 120.0, 0.5,
                    )
                    selector._module = SimpleNamespace(
                        pd=pd,
                        next_open_timestamp=lambda _ticker: pd.Timestamp.now(tz="UTC") - pd.Timedelta(seconds=1),
                    )
                    ticker = "KXBTC15M-26JUL201630-30"
                    selector.tasks[ticker] = asyncio.create_task(asyncio.sleep(60))
                    record = {"ticker": ticker}
                    side = await selector.side_for_market(SimpleNamespace(ticker=ticker), record)
                    self.assertIsNone(side)
                    self.assertEqual(record["ml_inference_status"], "late_preopen")
                    self.assertTrue(selector.tasks[ticker].cancelled() or selector.tasks[ticker].cancelling())
                    await selector.close()

        asyncio.run(scenario())

    def test_matching_frozen_ml_side_is_resumed_after_an_actions_handoff(self):
        async def scenario():
            import tempfile
            from pathlib import Path

            with tempfile.TemporaryDirectory() as temporary:
                base = Path(temporary)
                (base / "rows.csv").write_text("placeholder\n", encoding="utf-8")
                (base / "model.joblib").write_text("placeholder\n", encoding="utf-8")
                ticker = "KXBTC15M-26JUL201645-45"
                selector = MLDirectionSelector(
                    base / "rows.csv", base / "model.joblib", 120.0, 0.5,
                    model_run_id="same-frozen-model",
                )
                record = {
                    "ticker": ticker,
                    "ml_inference": {
                        "source": "stored_ml_preopen",
                        "model_run_id": "same-frozen-model",
                        "side": "no",
                        "probability_yes": 0.41,
                        "confidence": 0.59,
                    },
                }
                side = await selector.side_for_market(SimpleNamespace(ticker=ticker), record)
                self.assertEqual(side, "no")
                self.assertEqual(record["ml_inference_status"], "resumed")
                self.assertNotIn(ticker, selector.tasks)

        asyncio.run(scenario())

    def test_fresh_websocket_quote_supplies_both_executable_sides(self):
        feed = KalshiLiveFeed(auth=None)
        feed._handle(
            '{"type":"ticker","msg":{"market_ticker":"KXBTC15M-TEST",'
            '"yes_bid_dollars":"0.7200","yes_ask_dollars":"0.7500"}}'
        )
        self.assertEqual(feed.executable_asks("KXBTC15M-TEST"), {"yes": 0.75, "no": 0.28})

    def test_inverse_shadow_quote_requires_complete_fresh_book_and_displayed_depth(self):
        feed = KalshiLiveFeed(auth=None)
        ticker = "KXBTC15M-TEST-SHADOW-QUOTE"
        feed._handle(
            '{"type":"ticker","msg":{"market_ticker":"%s",'
            '"yes_bid_dollars":"0.7200","yes_ask_dollars":"0.7500",'
            '"yes_bid_size_fp":"0.0200","yes_ask_size_fp":"0.0100","ts_ms":123}}' % ticker
        )
        quote, reason = feed.executable_shadow_quote(ticker, "no", 0.01, 3.0)
        self.assertEqual(reason, "executable_top_of_book")
        self.assertIsNotNone(quote)
        assert quote is not None
        self.assertEqual((quote["economic_price"], quote["displayed_depth"]), (0.28, 0.02))
        self.assertEqual((quote["yes_bid"], quote["yes_ask"]), (0.72, 0.75))
        self.assertEqual((quote["yes_bid_size"], quote["yes_ask_size"]), (0.02, 0.01))
        feed.quotes[ticker]["complete_book"]["received_monotonic"] -= 4.0
        stale, stale_reason = feed.executable_shadow_quote(ticker, "no", 0.01, 3.0)
        self.assertIsNone(stale)
        self.assertEqual(stale_reason, "stale_top_of_book")

    def test_inverse_shadow_uses_opposite_side_and_consumes_quote_depth(self):
        class FakeFeed:
            def __init__(self):
                self.quotes = [
                    ({"quote_id": "one", "economic_price": 0.19, "displayed_depth": 1.0,
                      "yes_bid": 0.81, "yes_ask": 0.82, "yes_bid_size": 1.0, "yes_ask_size": 1.0,
                      "received_at": "2026-07-21T16:00:00+00:00", "quote_age_seconds": 0.1}, "executable_top_of_book"),
                    ({"quote_id": "two", "economic_price": 0.09, "displayed_depth": 3.0,
                      "yes_bid": 0.91, "yes_ask": 0.92, "yes_bid_size": 3.0, "yes_ask_size": 1.0,
                      "received_at": "2026-07-21T16:00:01+00:00", "quote_age_seconds": 0.1}, "executable_top_of_book"),
                ]

            def executable_shadow_quote(self, *_args):
                return self.quotes.pop(0)

        config = validate_config(DEFAULT_CONFIG)
        state = default_state()
        record = market_record(state, "KXBTC15M-TEST-INVERSE")
        market = SimpleNamespace(close_time="2099-07-20T00:15:00Z")
        shadow = ensure_inverse_shadow(record, market, config, "yes")
        self.assertIsNotNone(shadow)
        assert shadow is not None
        self.assertEqual((shadow["source_ml_side"], shadow["side"], record["orders"]), ("yes", "no", {}))
        feed = FakeFeed()
        self.assertTrue(simulate_inverse_shadow(record, feed, config))
        self.assertEqual(shadow["quantity_per_rung"], 1.0)
        self.assertEqual(shadow["rungs"]["0.4000"]["fill_count"], 1.0)
        self.assertEqual(shadow["rungs"]["0.3000"]["fill_count"], 0.0)
        self.assertTrue(simulate_inverse_shadow(record, feed, config))
        self.assertTrue(all(float(rung["fill_count"]) == 1.0 for rung in shadow["rungs"].values()))
        self.assertTrue(finalize_inverse_shadow(record, "no"))
        summary = inverse_shadow_performance(state)
        self.assertEqual((summary["settled_signal_markets"], summary["signal_directional_wins"]), (1, 1))
        self.assertEqual((summary["filled_market_trades"], summary["total_simulated_contracts"]), (1, 4.0))
        self.assertEqual(summary["net_profit"], 3.0)
        self.assertEqual(summary["rung_performance"]["0.40"]["simulated_quote_hits"], 1)

    def test_inverse_shadow_rung_report_keeps_active_quote_hits_out_of_realized_pnl(self):
        state = {"markets": {
            "active": {"inverse_ml_shadow": {
                "status": "active", "side": "yes", "rungs": {
                    "0.4000": {"quantity": 1.0, "fill_count": 1.0, "average_fill_price": 0.40},
                },
            }},
        }}
        rung = inverse_shadow_performance(state)["rung_performance"]["0.40"]
        self.assertEqual(rung["simulated_quote_hits"], 1)
        self.assertEqual(rung["unsettled_quote_hits"], 1)
        self.assertEqual((rung["winning_orders"], rung["losing_orders"], rung["net_profit"]), (0, 0, 0.0))

    def test_ml_scalp_range_shadow_records_depth_supported_excursion_without_exit(self):
        class FakeFeed:
            def __init__(self):
                self.entry = [
                    ({"quote_id": "entry", "economic_price": 0.30, "displayed_depth": 2.0}, "executable_top_of_book"),
                    ({"quote_id": "later", "economic_price": 0.50, "displayed_depth": 2.0}, "executable_top_of_book"),
                ]
                self.exit = [
                    ({"quote_id": "bid-low", "economic_price": 0.35, "displayed_depth": 2.0}, "executable_top_of_book"),
                    ({"quote_id": "bid-target", "economic_price": 0.36, "displayed_depth": 2.0}, "executable_top_of_book"),
                ]

            def executable_shadow_quote(self, *_args):
                return self.entry.pop(0)

            def executable_shadow_exit_quote(self, *_args):
                return self.exit.pop(0)

        config = validate_config(DEFAULT_CONFIG)
        state = default_state()
        record = market_record(state, "KXBTC15M-TEST-SCALP")
        shadow = ensure_ml_scalp_shadow(
            record, SimpleNamespace(close_time="2099-07-20T00:15:00Z"), config, "yes")
        self.assertIsNotNone(shadow)
        assert shadow is not None
        self.assertEqual(record["orders"], {})
        feed = FakeFeed()
        self.assertTrue(simulate_ml_scalp_shadow(record, feed, config))
        self.assertEqual((shadow["status"], shadow["entry_summary"]["average_entry_price"]), ("active", 0.35))
        self.assertTrue(simulate_ml_scalp_shadow(record, feed, config))
        self.assertEqual("active", shadow["status"])
        self.assertEqual(0.01, shadow["position_epochs"][0]["max_executable_gross_per_contract"])
        self.assertIn("0.01", shadow["position_epochs"][0]["target_hits"])
        self.assertTrue(finalize_ml_scalp_shadow(record, "yes"))
        summary = ml_scalp_shadow_performance(state)
        self.assertEqual(0, summary["scalp_exits"])
        self.assertEqual(0.01, summary["excursion_observer"]["maximum_gross_per_contract"]["median"])
        self.assertEqual(1, summary["excursion_observer"]["target_opportunities"]["0.01"]["hit_position_states"])

    def test_ml_weighted_trailing_studies_lock_normal_and_inverse_sides_before_quotes(self):
        config = validate_config(DEFAULT_CONFIG)
        state = default_state()
        record = market_record(state, "KXBTC15M-TEST-WEIGHTED")
        record["ml_inference"] = {"side": "yes", "model_run_id": "test-model"}
        ensure_ml_weighted_trailing_scalp_shadows(
            record, SimpleNamespace(close_time="2099-07-20T00:15:00Z"), config, "yes")
        normal = record["ml_weighted_trailing_scalp_shadow"]
        inverse = record["inverse_ml_weighted_trailing_scalp_shadow"]
        self.assertEqual(("yes", "no"), (normal["side"], inverse["side"]))
        self.assertEqual(0.10, normal["trailing_stop_per_contract"])
        self.assertEqual((1.0, 2.0, 3.0, 4.0), tuple(
            normal["rungs"][f"{level:.4f}"]["quantity"] for level in (0.40, 0.30, 0.20, 0.10)))
        self.assertEqual({}, record["orders"])
        normal_ledger = ml_weighted_trailing_ledger(state, config, inverse=False)
        inverse_ledger = ml_weighted_trailing_ledger(state, config, inverse=True)
        self.assertEqual(("normal_ml", "inverse_ml"), (
            normal_ledger["model_variant"], inverse_ledger["model_variant"],
        ))
        self.assertEqual(("yes", "no"), (
            normal_ledger["records"][0]["locked_study_side"],
            inverse_ledger["records"][0]["locked_study_side"],
        ))
        self.assertEqual({"0.40": 1.0, "0.30": 2.0, "0.20": 3.0, "0.10": 4.0},
                         normal_ledger["strategy_definition"]["rung_quantities"])
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            save_ml_weighted_trailing_outputs(
                state, config,
                normal_ledger_path=root / "normal-ledger.json",
                normal_report_path=root / "normal-report.json",
                inverse_ledger_path=root / "inverse-ledger.json",
                inverse_report_path=root / "inverse-report.json",
            )
            payload = json.loads((root / "normal-ledger.json").read_text(encoding="utf-8"))
            self.assertEqual("ml_weighted_1234_trailing_paper_ledger_v1", payload["schema"])
            self.assertEqual("yes", payload["records"][0]["locked_study_side"])

    def test_model_transition_shadow_paper_tests_predecessor_and_current_model_separately(self):
        class FakeFeed:
            def __init__(self):
                self.counter = 0

            def executable_shadow_quote(self, _ticker, side, _quantity, _max_age):
                self.counter += 1
                return ({
                    "quote_id": f"{side}-{self.counter}", "economic_price": 0.39, "displayed_depth": 1.0,
                    "yes_bid": 0.39, "yes_ask": 0.40, "yes_bid_size": 1.0, "yes_ask_size": 1.0,
                    "received_at": "2026-07-21T16:00:00+00:00", "quote_age_seconds": 0.1,
                }, "executable_top_of_book")

        state = default_state()
        record = market_record(state, "KXBTC15M-TEST-TRANSITION-SHADOW")
        record["ml_model_transition"] = {
            "previous_model_run_id": "old", "previous_model_type": "logistic_regression",
            "previous_probability_yes": 0.62, "previous_side": "yes",
            "current_model_run_id": "new", "current_model_type": "logistic_regression",
            "current_probability_yes": 0.38, "current_side": "no", "input_basis": "same frozen vector",
        }
        pair = ensure_model_transition_shadow(
            record, SimpleNamespace(close_time="2099-07-20T00:15:00Z"), validate_config(DEFAULT_CONFIG),
        )
        self.assertIsNotNone(pair)
        assert pair is not None
        self.assertEqual((pair["previous_model"]["side"], pair["current_model"]["side"]), ("yes", "no"))
        self.assertEqual(pair["previous_model"]["quantity_per_rung"], 1.0)
        self.assertNotIn("order_id", pair["current_model"]["rungs"]["0.4000"])
        self.assertTrue(simulate_model_transition_shadow(record, FakeFeed(), validate_config(DEFAULT_CONFIG)))
        self.assertTrue(finalize_model_transition_shadow(record, "no"))
        comparison = model_transition_shadow_performance(state)["comparisons"][0]
        self.assertEqual(comparison["paired_shadows_started"], 1)
        self.assertEqual((comparison["previous_model_paper"]["directional_wins"],
                          comparison["current_model_paper"]["directional_wins"]), (0, 1))
        self.assertEqual(comparison["current_minus_previous_paper_pnl"], 1.0)

    def test_live_quote_overrides_discovery_snapshot(self):
        market = SimpleNamespace(yes_ask_dollars="0.70", no_ask_dollars="0.30")
        self.assertEqual(market_asks(market, {"yes": 0.39, "no": 0.61}), {"yes": 0.39, "no": 0.61})

    def test_only_price_selects_entry_side(self):
        self.assertEqual(choose_entry_side({"yes": 0.40, "no": 0.41}), ("yes", 0.40))
        self.assertEqual(choose_entry_side({"yes": 0.39, "no": 0.25}), ("no", 0.25))
        self.assertIsNone(choose_entry_side({"yes": 0.41, "no": 0.42}))

    def test_no_orders_map_to_the_requested_economic_cost(self):
        self.assertEqual(side_api_price("yes", 0.30), "0.3000")
        self.assertEqual(side_api_price("no", 0.30), "0.7000")

    def test_handoff_cancels_only_orders_owned_by_this_mechanical_runner(self):
        ticker = "KXBTC15M-TEST-HANDOFF"
        owned = {"ticker": ticker, "client_order_id": client_order_id(ticker, "no", "0.3000")}
        self.assertEqual(managed_mechanical_order_role(owned), ("no", "0.3000"))
        self.assertIsNone(managed_mechanical_order_role({"ticker": ticker, "client_order_id": "manual-order"}))

    def test_documented_thursday_maintenance_window_is_pause_aware(self):
        self.assertTrue(scheduled_trading_pause_active(datetime(2026, 7, 23, 7, 0, tzinfo=timezone.utc)))
        self.assertTrue(scheduled_trading_pause_active(datetime(2026, 7, 23, 8, 59, tzinfo=timezone.utc)))
        self.assertFalse(scheduled_trading_pause_active(datetime(2026, 7, 23, 9, 0, tzinfo=timezone.utc)))
        self.assertTrue(pause_error("Kalshi Exchange is paused for maintenance"))
        self.assertFalse(pause_error("insufficient balance"))

    def test_exchange_recovery_attaches_only_owned_same_side_rungs(self):
        class FakeRest:
            async def resting_mechanical_orders(self):
                ticker = "KXBTC15M-TEST-RECOVERY"
                order = {
                    "ticker": ticker,
                    "order_id": "recovered-order",
                    "client_order_id": client_order_id(ticker, "no", "0.3000"),
                    "status": "resting",
                    "count": "1.00",
                    "fill_count": "0.00",
                    "remaining_count": "1.00",
                }
                return [(order, ("no", "0.3000"))]

            async def get_market(self, _ticker):
                return SimpleNamespace(
                    open_time="2026-07-20T00:00:00Z", close_time="2099-07-20T00:15:00Z",
                )

            async def position_for_ticker(self, _ticker):
                return 0.0

        async def scenario():
            state = default_state()
            ok = await recover_exchange_state(FakeRest(), state, validate_config(DEFAULT_CONFIG), dry_run=False)
            return ok, state["markets"]["KXBTC15M-TEST-RECOVERY"]

        ok, record = asyncio.run(scenario())
        self.assertTrue(ok)
        self.assertEqual(record["locked_side"], "no")
        self.assertEqual(record["status"], "ladder_active")
        self.assertEqual(record["orders"]["0.3000"]["order_id"], "recovered-order")

    def test_recovery_marks_mixed_side_orders_ambiguous(self):
        class FakeRest:
            async def resting_mechanical_orders(self):
                ticker = "KXBTC15M-TEST-MIXED"
                return [
                    ({"ticker": ticker, "client_order_id": client_order_id(ticker, "yes", "initial")}, ("yes", "initial")),
                    ({"ticker": ticker, "client_order_id": client_order_id(ticker, "no", "0.3000")}, ("no", "0.3000")),
                ]

        async def scenario():
            state = default_state()
            ok = await recover_exchange_state(FakeRest(), state, validate_config(DEFAULT_CONFIG), dry_run=False)
            return ok, state["markets"]["KXBTC15M-TEST-MIXED"]

        ok, record = asyncio.run(scenario())
        self.assertTrue(ok)
        self.assertEqual(record["status"], "recovery_blocked_ambiguous")

    def test_unfilled_ioc_is_never_reported_as_filled(self):
        self.assertEqual(
            classify_submission(0.0, 0.0, 1.0, "immediate_or_cancel"),
            "canceled_unfilled",
        )
        self.assertEqual(
            classify_submission(1.0, 0.0, 1.0, "immediate_or_cancel"),
            "filled",
        )

    def test_sdk_enum_style_status_is_normalized(self):
        self.assertEqual(normalized_order_status("OrderStatus.CANCELED"), "canceled")
        self.assertEqual(normalized_order_status("FILLED"), "filled")

    def test_exchange_outcome_side_is_checked_without_reversing_no_prices(self):
        self.assertEqual(normalized_outcome_side("OutcomeSide.NO"), "no")
        self.assertEqual(normalized_outcome_side("yes"), "yes")
        self.assertIsNone(normalized_outcome_side("ask"))
        self.assertEqual(exchange_outcome_side({"book_side": "ask"}), "no")
        self.assertEqual(exchange_outcome_side({"side": "yes", "action": "sell"}), "no")

    def test_config_rejects_an_unfunded_ladder(self):
        invalid = {**DEFAULT_CONFIG, "max_total_capital": 0.009}
        with self.assertRaises(ValueError):
            validate_config(invalid)

    def test_share_size_override_auto_scales_contract_and_capital_guards(self):
        args = SimpleNamespace(initial_position_size=10.0)
        config, changed = apply_config_overrides(validate_config(DEFAULT_CONFIG), args)
        self.assertTrue(changed)
        self.assertEqual(config["initial_position_size"], 10.0)
        self.assertEqual(config["max_contracts_per_market"], 40.0)
        self.assertEqual(config["max_total_capital"], 10.0)

    def test_exchange_position_over_cap_blocks_all_further_orders(self):
        class FakeRest:
            async def position_for_ticker(self, _ticker):
                return -18.0

        async def scenario():
            record = market_record(default_state(), "KXBTC15M-TEST-OVER-CAP")
            record.update({"candidate_side": "no", "quantity": 1.0, "status": "initial_submitted"})
            allowed = await exchange_position_guard(FakeRest(), record, validate_config(DEFAULT_CONFIG))
            return allowed, record

        allowed, record = asyncio.run(scenario())
        self.assertFalse(allowed)
        self.assertIn("exceeds cap", record["exchange_position_guard_blocked"])

    def test_report_identifies_the_ml_side_mechanical_strategy(self):
        report = performance_report({"markets": {}}, validate_config(DEFAULT_CONFIG))
        self.assertEqual(report["strategy"], "ml_side_mechanical_price_average_down_v1")
        self.assertEqual(report["total_markets_traded"], 0)
        self.assertEqual(tuple(LADDER_LEVELS), (0.40, 0.30, 0.20, 0.10))

    def test_ml_live_directional_performance_keeps_direction_separate_from_pnl(self):
        settled = [
            {
                "settlement_outcome": "yes", "locked_side": "yes",
                "ml_inference": {"probability_yes": 0.60, "confidence": 0.60},
            },
            {
                "settlement_outcome": "yes", "candidate_side": "no",
                "ml_inference": {"probability_yes": 0.40, "confidence": 0.60},
            },
            {"settlement_outcome": "no", "locked_side": "no"},
        ]
        summary = ml_live_directional_performance(settled)
        self.assertEqual((summary["settled_markets"], summary["directional_wins"], summary["directional_losses"]), (2, 1, 1))
        self.assertEqual(summary["directional_win_rate"], 0.5)
        self.assertEqual(summary["average_model_confidence"], 0.6)

    def test_model_transition_summary_counts_same_and_changed_frozen_sides(self):
        state = {"markets": {
            "changed": {
                "settlement_outcome": "no",
                "ml_model_transition": {
                    "previous_model_run_id": "old", "current_model_run_id": "new",
                    "previous_side": "yes", "current_side": "no", "probability_yes_delta": -0.24,
                },
            },
            "same": {
                "settlement_outcome": "no",
                "ml_model_transition": {
                    "previous_model_run_id": "old", "current_model_run_id": "new",
                    "previous_side": "no", "current_side": "no", "probability_yes_delta": 0.03,
                },
            },
        }}
        summary = model_transition_side_comparison(state)
        self.assertEqual(len(summary["comparisons"]), 1)
        comparison = summary["comparisons"][0]
        self.assertEqual((comparison["compared_markets"], comparison["same_side"], comparison["side_changed"]), (2, 1, 1))
        self.assertEqual((comparison["yes_to_no"], comparison["no_to_yes"]), (1, 0))
        self.assertEqual((comparison["previous_directional_wins"], comparison["current_directional_wins"]), (1, 2))
        self.assertEqual(comparison["current_minus_previous_directional_wins"], 1)

    def test_transition_comparator_scores_predecessor_on_the_same_vector(self):
        class PreviousModel:
            def predict_proba(self, _vector):
                return [[0.63, 0.37]]

        selector = object.__new__(MLDirectionSelector)
        selector.previous_model_path = "previous-model.joblib"
        selector.previous_model_run_id = "old-model"
        selector.model_run_id = "new-model"
        selector.previous_model = None
        selector.previous_model_load_failed = False
        selector.previous_model_metadata = {"model_type": "logistic_regression"}
        selector.model_metadata = {"model_type": "logistic_regression"}
        comparison = selector._model_transition_comparison(
            SimpleNamespace(load_saved_model=lambda _path: PreviousModel()),
            [[1.0] * 16], 0.62, "yes",
        )
        self.assertIsNotNone(comparison)
        assert comparison is not None
        self.assertEqual((comparison["previous_side"], comparison["current_side"]), ("no", "yes"))
        self.assertTrue(comparison["side_changed"])
        self.assertEqual(comparison["probability_yes_delta"], 0.25)

    def test_zero_contract_finalization_is_excluded_from_performance(self):
        state = {"markets": {
            "unfilled": {"status": "finalized", "contracts": 0.0, "net_profit_loss": 0.0},
        }}
        report = performance_report(state, validate_config(DEFAULT_CONFIG))
        self.assertEqual(report["total_markets_traded"], 0)
        self.assertEqual(report["unfilled_market_attempts"], 1)
        self.assertEqual(report["winning_trades"], 0)
        self.assertEqual(report["losing_trades"], 0)

    def test_report_breaks_out_profit_loss_for_each_filled_rung(self):
        def order(fill, price, fee=0.0):
            return {"fill_count": fill, "average_fill_price": price, "fees_paid": fee}

        state = {"markets": {
            "winner": {
                "status": "finalized", "settled_at": "2026-07-20T00:00:00Z",
                "settlement_outcome": "yes", "locked_side": "yes", "total_cost": 0.70,
                "contracts": 2.0, "gross_profit_loss": 1.30, "kalshi_fees": 0.02,
                "net_profit_loss": 1.28, "orders": {
                    "0.4000": order(1.0, 0.40, 0.01), "0.3000": order(1.0, 0.30, 0.01),
                },
            },
            "loser": {
                "status": "finalized", "settled_at": "2026-07-20T00:15:00Z",
                "settlement_outcome": "yes", "locked_side": "no", "total_cost": 0.40,
                "contracts": 1.0, "gross_profit_loss": -0.40, "kalshi_fees": 0.01,
                "net_profit_loss": -0.41, "orders": {"0.4000": order(1.0, 0.40, 0.01)},
            },
        }}
        report = performance_report(state, validate_config(DEFAULT_CONFIG))
        first = report["rung_performance"]["0.40"]
        second = report["rung_performance"]["0.30"]
        self.assertEqual((report["winning_trades"], report["losing_trades"], report["win_loss_ratio"]), (1, 1, 1.0))
        self.assertEqual((first["filled_orders"], first["winning_orders"], first["losing_orders"]), (2, 1, 1))
        self.assertEqual(first["net_profit"], 0.18)
        self.assertEqual((second["filled_orders"], second["winning_orders"], second["losing_orders"]), (1, 1, 0))
        self.assertEqual(second["net_profit"], 0.69)

    def test_rung_activity_counts_submitted_filled_resting_and_canceled_orders(self):
        state = {"markets": {
            "one": {"orders": {
                "0.4000": {"quantity": 1.0, "fill_count": 1.0, "remaining_count": 0.0},
                "0.3000": {"quantity": 1.0, "fill_count": 0.0, "remaining_count": 1.0},
                "0.2000": {"quantity": 1.0, "fill_count": 0.0, "remaining_count": 0.0},
            }},
        }}
        activity = rung_order_activity(state)
        self.assertEqual((activity["0.40"]["submitted_orders"], activity["0.40"]["filled_order_submissions"]), (1, 1))
        self.assertEqual(activity["0.30"]["resting_orders"], 1)
        self.assertEqual(activity["0.20"]["canceled_unfilled_orders"], 1)

    def test_ml_selected_side_preposts_all_four_gtc_rungs_at_open(self):
        class FakeRest:
            def __init__(self):
                self.requests = []

            async def balance_dollars(self):
                return 10.0

            async def create_order(self, **kwargs):
                self.requests.append(kwargs)
                return {
                    "order_id": f"order-{len(self.requests)}", "side": kwargs["side"],
                    "position_price": kwargs["position_price"], "quantity": kwargs["quantity"],
                    "fill_count": kwargs["quantity"] if len(self.requests) == 1 else 0.0,
                    "remaining_count": 0.0 if len(self.requests) == 1 else kwargs["quantity"],
                    "fees_paid": 0.0, "status": "filled" if len(self.requests) == 1 else "resting",
                }

            async def refresh_order(self, _order):
                return None

        async def scenario():
            config = validate_config(DEFAULT_CONFIG)
            state = default_state()
            market = SimpleNamespace(
                ticker="KXBTC15M-TEST", status="active", yes_ask_dollars="0.3500",
                no_ask_dollars="0.6500", open_time=time.time() - 5,
                close_time="2099-07-20T00:15:00Z",
            )
            rest = FakeRest()
            record = market_record(state, market.ticker)
            record.update({"status": "watching", "market_open_time": market.open_time})
            entered = await consider_initial_entry(rest, state, market, config, dry_run=False, ml_side="yes")
            record = state["markets"]["KXBTC15M-TEST"]
            await reconcile_orders(rest, record, dry_run=False)
            await submit_ladder(rest, record, market, config, dry_run=False)
            return entered, rest.requests, record

        entered, requests, record = asyncio.run(scenario())
        self.assertTrue(entered)
        self.assertEqual(record["locked_side"], "yes")
        self.assertEqual(record["ladder_mode"], "preposted_gtc_v2")
        self.assertEqual([request["position_price"] for request in requests], list(LADDER_LEVELS))
        self.assertTrue(all(request["tif"] == "good_till_canceled" for request in requests))
        self.assertTrue(all(request["expiration_time"] is not None for request in requests))
        self.assertEqual(len({request["expiration_time"] for request in requests}), 1)
        self.assertTrue(all(request["side"] == "yes" for request in requests))

    def test_preposted_ladder_never_switches_sides_after_a_later_opposite_quote(self):
        class FakeRest:
            def __init__(self):
                self.requests = []

            async def balance_dollars(self):
                return 10.0

            async def create_order(self, **kwargs):
                self.requests.append(kwargs)
                return {
                    "order_id": "initial", "side": kwargs["side"],
                    "position_price": kwargs["position_price"], "quantity": kwargs["quantity"],
                    "fill_count": 0.0, "remaining_count": kwargs["quantity"],
                    "fees_paid": 0.0, "status": "resting",
                }

        async def scenario():
            config = validate_config(DEFAULT_CONFIG)
            state = default_state()
            market = SimpleNamespace(
                ticker="KXBTC15M-TEST-GTC", status="active", yes_ask_dollars="0.3400",
                no_ask_dollars="0.6600", open_time=time.time() - 5, close_time=time.time() + 895,
            )
            rest = FakeRest()
            record = market_record(state, market.ticker)
            record.update({"status": "watching", "market_open_time": market.open_time})
            await consider_initial_entry(rest, state, market, config, dry_run=False, ml_side="yes")
            # A later qualifying NO quote must not create an opposite-side order.
            market.yes_ask_dollars, market.no_ask_dollars = "0.6600", "0.3400"
            await consider_initial_entry(rest, state, market, config, dry_run=False, ml_side="no")
            return record, rest.requests

        record, requests = asyncio.run(scenario())
        self.assertEqual(record["status"], "ladder_active")
        self.assertEqual(len(requests), 4)
        self.assertTrue(all(request["side"] == "yes" for request in requests))
        self.assertEqual([request["position_price"] for request in requests], list(LADDER_LEVELS))
        self.assertTrue(all(request["tif"] == "good_till_canceled" for request in requests))

    def test_preposted_ladder_retries_only_a_rejected_same_side_rung(self):
        class FakeRest:
            def __init__(self):
                self.requests = []
                self.fail_once = True

            async def balance_dollars(self):
                return 10.0

            async def create_order(self, **kwargs):
                self.requests.append(kwargs)
                if kwargs["position_price"] == 0.30 and self.fail_once:
                    self.fail_once = False
                    return {
                        "side": kwargs["side"], "position_price": kwargs["position_price"],
                        "quantity": kwargs["quantity"], "fill_count": 0.0,
                        "remaining_count": 0.0, "fees_paid": 0.0, "status": "submit_failed",
                    }
                return {
                    "order_id": f"order-{len(self.requests)}", "side": kwargs["side"],
                    "position_price": kwargs["position_price"], "quantity": kwargs["quantity"],
                    "fill_count": 0.0, "remaining_count": kwargs["quantity"],
                    "fees_paid": 0.0, "status": "resting",
                }

        async def scenario():
            state = default_state()
            market = SimpleNamespace(
                ticker="KXBTC15M-TEST-RETRY", status="active", yes_ask_dollars="0.75",
                no_ask_dollars="0.25", open_time=time.time() - 5, close_time=time.time() + 895,
            )
            rest = FakeRest()
            record = market_record(state, market.ticker)
            record.update({"status": "watching", "market_open_time": market.open_time})
            first = await consider_initial_entry(rest, state, market, validate_config(DEFAULT_CONFIG), dry_run=False, ml_side="yes")
            second = await consider_initial_entry(rest, state, market, validate_config(DEFAULT_CONFIG), dry_run=False, ml_side="yes")
            return first, second, record, rest.requests

        first, second, record, requests = asyncio.run(scenario())
        self.assertTrue(first)
        self.assertTrue(second)
        self.assertTrue(record["ladder_preposted_complete"])
        self.assertEqual(record["status"], "ladder_active")
        self.assertEqual(list(record["orders"]), ["0.4000", "0.2000", "0.1000", "0.3000"])
        self.assertEqual([request["position_price"] for request in requests], [0.40, 0.30, 0.20, 0.10, 0.30])
        self.assertTrue(all(request["side"] == "yes" for request in requests))

    def test_ml_side_not_cheapest_side_receives_its_full_ladder_without_a_price_gate(self):
        class FakeRest:
            def __init__(self):
                self.requests = []

            async def balance_dollars(self):
                return 10.0

            async def create_order(self, **kwargs):
                self.requests.append(kwargs)
                return {
                    "order_id": "ml-side", "side": kwargs["side"],
                    "position_price": kwargs["position_price"], "quantity": kwargs["quantity"],
                    "fill_count": 0.0, "remaining_count": kwargs["quantity"],
                    "fees_paid": 0.0, "status": "resting",
                }

        async def scenario():
            state = default_state()
            market = SimpleNamespace(
                ticker="KXBTC15M-TEST-ML-SIDE", status="active", yes_ask_dollars="0.7500",
                no_ask_dollars="0.2000", open_time=time.time() - 5, close_time=time.time() + 895,
            )
            rest = FakeRest()
            record = market_record(state, market.ticker)
            record.update({"status": "watching", "market_open_time": market.open_time})
            await consider_initial_entry(rest, state, market, validate_config(DEFAULT_CONFIG), dry_run=False, ml_side="yes")
            return record, rest.requests

        record, requests = asyncio.run(scenario())
        self.assertEqual(record["locked_side"], "yes")
        self.assertEqual([request["position_price"] for request in requests], list(LADDER_LEVELS))
        self.assertTrue(all(request["side"] == "yes" for request in requests))

    def test_missing_ml_side_cannot_fall_back_to_a_price_selected_entry(self):
        class FakeRest:
            async def balance_dollars(self):
                return 10.0

            async def create_order(self, **_kwargs):
                raise AssertionError("No order may be submitted without ML direction")

        async def scenario():
            state = default_state()
            market = SimpleNamespace(
                ticker="KXBTC15M-TEST-NO-ML", status="active", yes_ask_dollars="0.20",
                no_ask_dollars="0.80", open_time=time.time() - 5, close_time=time.time() + 895,
            )
            record = market_record(state, market.ticker)
            record.update({"status": "watching", "market_open_time": market.open_time})
            return await consider_initial_entry(FakeRest(), state, market, validate_config(DEFAULT_CONFIG), dry_run=False)

        self.assertFalse(asyncio.run(scenario()))

    def test_legacy_watcher_never_converts_to_a_new_gtc_ladder_mid_market(self):
        class FakeRest:
            async def balance_dollars(self):
                return 10.0

            async def create_order(self, **_kwargs):
                raise AssertionError("A legacy watcher must not post a new mid-market ladder")

        async def scenario():
            state = default_state()
            market = SimpleNamespace(
                ticker="KXBTC15M-TEST-LEGACY-WATCH", status="active", yes_ask_dollars="0.70",
                no_ask_dollars="0.30", open_time=time.time() - 60, close_time=time.time() + 840,
            )
            record = market_record(state, market.ticker)
            record.update({"status": "watching", "market_open_time": market.open_time})
            entered = await consider_initial_entry(
                FakeRest(), state, market, validate_config(DEFAULT_CONFIG), dry_run=False, ml_side="no",
            )
            return entered, record

        entered, record = asyncio.run(scenario())
        self.assertFalse(entered)
        self.assertEqual(record["status"], "prepost_window_missed")
        self.assertEqual(record["orders"], {})

    def test_selected_side_at_10_cents_still_gets_the_fixed_preposted_ladder(self):
        class FakeRest:
            async def balance_dollars(self):
                return 10.0

            def __init__(self):
                self.requests = []

            async def create_order(self, **kwargs):
                self.requests.append(kwargs)
                return {
                    "order_id": "initial", "side": kwargs["side"],
                    "position_price": kwargs["position_price"], "quantity": kwargs["quantity"],
                    "fill_count": 1.0, "remaining_count": 0.0, "fees_paid": 0.0, "status": "filled",
                }

            async def refresh_order(self, _order):
                return None

        async def scenario():
            config = validate_config(DEFAULT_CONFIG)
            state = default_state()
            market = SimpleNamespace(
                ticker="KXBTC15M-TEST-10", status="active", yes_ask_dollars="0.90",
                no_ask_dollars="0.10", open_time=time.time() - 1,
                close_time=time.time() + 899,
            )
            rest = FakeRest()
            await consider_initial_entry(rest, state, market, config, dry_run=False, ml_side="no")
            record = state["markets"]["KXBTC15M-TEST-10"]
            await reconcile_orders(rest, record, dry_run=False)
            await submit_ladder(rest, record, market, config, dry_run=False)
            return record, rest.requests

        record, requests = asyncio.run(scenario())
        self.assertEqual(list(record["orders"]), ["0.4000", "0.3000", "0.2000", "0.1000"])
        self.assertEqual([request["position_price"] for request in requests], list(LADDER_LEVELS))
        self.assertTrue(all(request["side"] == "no" for request in requests))

    def test_closed_prior_market_does_not_block_a_fresh_new_market(self):
        state = {"markets": {"old": {"status": "closed_waiting_finalization", "quantity": 1.0}}}
        self.assertEqual(active_strategy_records(state), [])
        expired = SimpleNamespace(status="active", close_time="2020-01-01T00:00:00Z")
        self.assertFalse(market_is_tradeable(expired))

    def test_watcher_without_an_ml_side_is_not_counted_as_an_unfilled_order(self):
        class FakeRest:
            async def cancel_order(self, _order, _dry_run):
                raise AssertionError("A watcher with no order must not cancel anything")

        async def scenario():
            state = default_state()
            record = market_record(state, "KXBTC15M-NO-SIGNAL")
            record.update({"status": "watching", "market_close_time": time.time() - 1})
            market = SimpleNamespace(status="finalized", result="yes", close_time=time.time() - 1)
            await settle_or_cancel(FakeRest(), record, market, dry_run=False)
            return record

        record = asyncio.run(scenario())
        self.assertEqual(record["status"], "finalized_no_signal")
        self.assertEqual(record["contracts"], 0.0)

    def test_market_is_not_tradeable_before_its_open_time(self):
        pre_open = SimpleNamespace(
            status="active", open_time="2099-01-01T00:00:00Z", close_time="2099-01-01T00:15:00Z",
        )
        self.assertFalse(market_is_tradeable(pre_open))

    def test_watcher_starts_at_open_and_an_existing_watcher_can_wait_longer(self):
        just_opened = SimpleNamespace(
            status="active", open_time=time.time() - 5,
            close_time=time.time() + 895,
        )
        late = SimpleNamespace(
            status="active", open_time=time.time() - 16,
            close_time=time.time() + 884,
        )
        missing_open_time = SimpleNamespace(status="active", close_time=time.time() + 895)
        self.assertTrue(market_can_start_watcher(just_opened, 45.0))
        self.assertTrue(market_can_start_watcher(late, 45.0))
        self.assertFalse(market_can_start_watcher(missing_open_time, 45.0))
        self.assertFalse(market_can_start_watcher(
            SimpleNamespace(status="active", open_time=time.time() - 46, close_time=time.time() + 854), 45.0,
        ))


if __name__ == "__main__":
    unittest.main()
