"""Focused safety checks for the settlement-only live execution path."""

from __future__ import annotations

import argparse
import contextlib
import io
import unittest

import kalshi_btc15m_average_down as trader


class SettlementTraderTests(unittest.IsolatedAsyncioTestCase):
    def test_default_three_share_ladder_and_reserve(self) -> None:
        config = trader.validate_config({})
        self.assertEqual(trader.live_rung_quantities(config), {0.40: 3.0, 0.30: 6.0, 0.20: 9.0, 0.10: 12.0})
        self.assertEqual(config["max_contracts_per_market"], 30.0)
        self.assertEqual(config["max_total_capital"], 6.0)
        self.assertEqual(trader.ladder_principal_for_rungs(trader.live_rung_quantities(config)), 6.0)

    def test_share_override_persists_scaled_caps(self) -> None:
        args = argparse.Namespace(
            initial_position_size=2.0,
            max_active_markets=None,
            max_contracts_per_market=None,
            max_total_capital=None,
            fee_reserve=None,
            poll_seconds=None,
            market_refresh_seconds=None,
            order_reconcile_seconds=None,
            watch_start_grace_seconds=None,
            status_log_seconds=None,
        )
        config, changed = trader.apply_config_overrides(trader.validate_config({}), args)
        self.assertTrue(changed)
        self.assertEqual(trader.live_rung_quantities(config), {0.40: 2.0, 0.30: 4.0, 0.20: 6.0, 0.10: 8.0})
        self.assertEqual(config["max_contracts_per_market"], 20.0)
        self.assertEqual(config["max_total_capital"], 4.0)

    async def test_high_price_never_arms_or_closes_a_profit_trail(self) -> None:
        class Feed:
            def executable_shadow_exit_quote(self, *args, **kwargs):
                return ({"economic_price": 0.95, "displayed_depth": 3.0}, "executable_top_of_book")

        class Rest:
            async def cancel_order(self, *args, **kwargs):
                raise AssertionError("a 95c bid must not cause a close")

        config = trader.validate_config({})
        record = {
            "ticker": "KXBTC15M-test",
            "strategy": trader.LIVE_EXECUTION_STRATEGY,
            "status": "ladder_active",
            "locked_side": "yes",
            "orders": {"0.4000": {"fill_count": 3.0, "position_price": 0.40, "quantity": 3.0}},
        }
        changed = await trader.monitor_live_absolute_stop(Rest(), record, Feed(), config, dry_run=False)
        self.assertFalse(changed)
        self.assertEqual(record["status"], "ladder_active")
        self.assertNotIn("trigger", record.get("live_exit_protection", {}))

    async def test_only_the_flat_five_cent_price_starts_an_exit(self) -> None:
        class Feed:
            def executable_shadow_exit_quote(self, *args, **kwargs):
                return ({"economic_price": 0.05, "displayed_depth": 3.0}, "executable_top_of_book")

        class Rest:
            async def cancel_order(self, *args, **kwargs):
                return None

            async def refresh_order(self, *args, **kwargs):
                return None

            async def position_for_ticker(self, ticker):
                return 3.0

            async def create_reduce_only_exit(self, **kwargs):
                self.kwargs = kwargs
                return {"fill_count": 3.0, "remaining_count": 0.0, "position_price": 0.05}

        config = trader.validate_config({})
        record = {
            "ticker": "KXBTC15M-test", "strategy": trader.LIVE_EXECUTION_STRATEGY,
            "status": "ladder_active", "locked_side": "yes",
            "orders": {"0.4000": {"fill_count": 3.0, "position_price": 0.40, "quantity": 3.0}},
        }
        rest = Rest()
        changed = await trader.monitor_live_absolute_stop(rest, record, Feed(), config, dry_run=False)
        self.assertTrue(changed)
        self.assertEqual(record["live_exit_protection"]["trigger"], "absolute_5c_stop")
        self.assertEqual(rest.kwargs["economic_exit_price"], 0.05)
        self.assertNotIn("armed", record["live_exit_protection"])

    async def test_pending_legacy_profit_exit_is_retired_not_sent(self) -> None:
        class Feed:
            def executable_shadow_exit_quote(self, *args, **kwargs):
                return ({"economic_price": 0.50, "displayed_depth": 3.0}, "executable_top_of_book")

        class Rest:
            async def cancel_order(self, *args, **kwargs):
                raise AssertionError("a retired profit trail must not cancel entry orders")

        config = trader.validate_config({})
        record = {
            "ticker": "KXBTC15M-test", "strategy": "settlement_contrarian_weighted_hold_gate_live_v1",
            "status": "live_exit_pending", "locked_side": "yes",
            "orders": {"0.4000": {"fill_count": 3.0, "position_price": 0.40, "quantity": 3.0}},
            "live_exit_protection": {"trigger": "trailing_stop", "armed": True, "trailing_high_bid": 0.95},
        }
        changed = await trader.monitor_live_absolute_stop(Rest(), record, Feed(), config, dry_run=False)
        self.assertFalse(changed)
        self.assertEqual(record["status"], "ladder_active")
        self.assertEqual(record["live_exit_protection"]["retired_exit_trigger"], "trailing_stop")

    def test_live_parser_does_not_accept_retired_model_or_gate_flags(self) -> None:
        parser = trader.build_parser()
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                parser.parse_args(["--ml-model-path", "retired.joblib"])
            with self.assertRaises(SystemExit):
                parser.parse_args(["--live-inverse-ml-hold-gate", "0.60"])


if __name__ == "__main__":
    unittest.main()
