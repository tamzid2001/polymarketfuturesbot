import asyncio
import time
import unittest
from types import SimpleNamespace

from kalshi_btc15m_average_down import (
    DEFAULT_CONFIG,
    KalshiLiveFeed,
    LADDER_LEVELS,
    active_strategy_records,
    apply_config_overrides,
    classify_submission,
    client_order_id,
    choose_entry_side,
    consider_initial_entry,
    default_state,
    exchange_position_guard,
    exchange_outcome_side,
    ladder_principal,
    performance_report,
    reconcile_orders,
    settle_or_cancel,
    side_api_price,
    submit_ladder,
    market_is_tradeable,
    market_can_start_watcher,
    market_record,
    market_asks,
    managed_mechanical_order_role,
    ml_live_directional_performance,
    normalized_order_status,
    normalized_outcome_side,
    rung_order_activity,
    validate_config,
)


class MechanicalAverageDownTests(unittest.TestCase):
    def test_default_is_one_contract_per_rung(self):
        config = validate_config(DEFAULT_CONFIG)
        self.assertEqual(config["initial_position_size"], 1.0)
        self.assertEqual(config["max_contracts_per_market"], 4.0)
        self.assertEqual(ladder_principal(1.0), 1.0)
        self.assertEqual(config["market_refresh_seconds"], 15.0)
        self.assertEqual(config["order_reconcile_seconds"], 5.0)
        self.assertEqual(config["watch_start_grace_seconds"], 45.0)
        self.assertEqual(config["ml_min_confidence"], 0.5)

    def test_fresh_websocket_quote_supplies_both_executable_sides(self):
        feed = KalshiLiveFeed(auth=None)
        feed._handle(
            '{"type":"ticker","msg":{"market_ticker":"KXBTC15M-TEST",'
            '"yes_bid_dollars":"0.7200","yes_ask_dollars":"0.7500"}}'
        )
        self.assertEqual(feed.executable_asks("KXBTC15M-TEST"), {"yes": 0.75, "no": 0.28})

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
        invalid = {**DEFAULT_CONFIG, "max_total_capital": 0.99}
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

    def test_below_40_entry_locks_one_side_then_places_only_30_20_10_limits(self):
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
                no_ask_dollars="0.6500", open_time=time.time() - 120,
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
        self.assertEqual([request["position_price"] for request in requests], [0.35, 0.30, 0.20, 0.10])
        self.assertEqual(requests[0]["tif"], "good_till_canceled")
        self.assertIsNotNone(requests[0]["expiration_time"])
        self.assertTrue(all(request["side"] == "yes" for request in requests))

    def test_first_below_40_trigger_remains_a_resting_limit_and_never_switches_sides(self):
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
        self.assertEqual(record["status"], "initial_submitted")
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0]["side"], "yes")
        self.assertEqual(requests[0]["position_price"], 0.34)
        self.assertEqual(requests[0]["tif"], "good_till_canceled")

    def test_ml_side_not_cheapest_side_selects_the_initial_limit(self):
        class FakeRest:
            async def balance_dollars(self):
                return 10.0

            async def create_order(self, **kwargs):
                return {
                    "order_id": "ml-side", "side": kwargs["side"],
                    "position_price": kwargs["position_price"], "quantity": kwargs["quantity"],
                    "fill_count": 0.0, "remaining_count": kwargs["quantity"],
                    "fees_paid": 0.0, "status": "resting",
                }

        async def scenario():
            state = default_state()
            market = SimpleNamespace(
                ticker="KXBTC15M-TEST-ML-SIDE", status="active", yes_ask_dollars="0.3500",
                no_ask_dollars="0.2000", open_time=time.time() - 5, close_time=time.time() + 895,
            )
            rest = FakeRest()
            record = market_record(state, market.ticker)
            record.update({"status": "watching", "market_open_time": market.open_time})
            await consider_initial_entry(rest, state, market, validate_config(DEFAULT_CONFIG), dry_run=False, ml_side="yes")
            return record

        record = asyncio.run(scenario())
        initial = record["orders"]["0.4000"]
        self.assertEqual(initial["side"], "yes")
        self.assertEqual(initial["position_price"], 0.35)

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

    def test_10_cent_initial_entry_never_averages_up(self):
        class FakeRest:
            async def balance_dollars(self):
                return 10.0

            async def create_order(self, **kwargs):
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
            return record

        record = asyncio.run(scenario())
        self.assertEqual(list(record["orders"]), ["0.4000"])

    def test_closed_prior_market_does_not_block_a_fresh_new_market(self):
        state = {"markets": {"old": {"status": "closed_waiting_finalization", "quantity": 1.0}}}
        self.assertEqual(active_strategy_records(state), [])
        expired = SimpleNamespace(status="active", close_time="2020-01-01T00:00:00Z")
        self.assertFalse(market_is_tradeable(expired))

    def test_watcher_without_a_40_cent_signal_is_not_counted_as_an_unfilled_order(self):
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
