import asyncio
import unittest
from types import SimpleNamespace

from kalshi_btc15m_average_down import (
    DEFAULT_CONFIG,
    KalshiLiveFeed,
    LADDER_LEVELS,
    active_strategy_records,
    classify_submission,
    choose_entry_side,
    consider_initial_entry,
    default_state,
    ladder_principal,
    performance_report,
    reconcile_orders,
    side_api_price,
    submit_ladder,
    market_is_tradeable,
    market_asks,
    normalized_order_status,
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

    def test_no_orders_use_complementary_yes_book_price(self):
        self.assertEqual(side_api_price("yes", 0.30), "0.3000")
        self.assertEqual(side_api_price("no", 0.30), "0.7000")

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

    def test_config_rejects_an_unfunded_ladder(self):
        invalid = {**DEFAULT_CONFIG, "max_total_capital": 0.99}
        with self.assertRaises(ValueError):
            validate_config(invalid)

    def test_report_has_no_model_metrics(self):
        report = performance_report({"markets": {}}, validate_config(DEFAULT_CONFIG))
        self.assertEqual(report["strategy"], "pure_mechanical_price_average_down_v1")
        self.assertEqual(report["total_markets_traded"], 0)
        self.assertEqual(tuple(LADDER_LEVELS), (0.40, 0.30, 0.20, 0.10))

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
                no_ask_dollars="0.6500", close_time="2099-07-20T00:15:00Z",
            )
            rest = FakeRest()
            entered = await consider_initial_entry(rest, state, market, config, dry_run=False)
            record = state["markets"]["KXBTC15M-TEST"]
            await reconcile_orders(rest, record, dry_run=False)
            await submit_ladder(rest, record, market, config, dry_run=False)
            return entered, rest.requests, record

        entered, requests, record = asyncio.run(scenario())
        self.assertTrue(entered)
        self.assertEqual(record["locked_side"], "yes")
        self.assertEqual([request["position_price"] for request in requests], [0.35, 0.30, 0.20, 0.10])
        self.assertEqual(requests[0]["tif"], "immediate_or_cancel")
        self.assertTrue(all(request["side"] == "yes" for request in requests))

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
            market = SimpleNamespace(ticker="KXBTC15M-TEST-10", status="active", yes_ask_dollars="0.90", no_ask_dollars="0.10")
            rest = FakeRest()
            await consider_initial_entry(rest, state, market, config, dry_run=False)
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

    def test_market_is_not_tradeable_before_its_open_time(self):
        pre_open = SimpleNamespace(
            status="active", open_time="2099-01-01T00:00:00Z", close_time="2099-01-01T00:15:00Z",
        )
        self.assertFalse(market_is_tradeable(pre_open))


if __name__ == "__main__":
    unittest.main()
