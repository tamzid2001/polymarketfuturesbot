"""Focused safety checks for the settlement-only live execution path."""

from __future__ import annotations

import argparse
import contextlib
import io
import time
import unittest
from datetime import datetime, timezone

import kalshi_btc15m_average_down as trader


class SettlementTraderTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def completed_live_record(ticker: str, completed_epoch: int, profit_loss: float) -> dict:
        return {
            "ticker": ticker,
            "strategy": trader.LIVE_EXECUTION_STRATEGY,
            "execution_mode": "live",
            "status": "finalized",
            "contracts": 3.0,
            "net_profit_loss": profit_loss,
            "settled_at": datetime.fromtimestamp(completed_epoch, tz=timezone.utc).isoformat(),
        }

    def test_default_three_share_ladder_and_reserve(self) -> None:
        config = trader.validate_config({})
        self.assertEqual(trader.live_rung_quantities(config), {0.40: 3.0, 0.30: 6.0, 0.20: 9.0, 0.10: 12.0})
        self.assertEqual(config["max_contracts_per_market"], 30.0)
        self.assertEqual(config["max_total_capital"], 6.0)
        self.assertEqual(trader.ladder_principal_for_rungs(trader.live_rung_quantities(config)), 6.0)

    async def test_immediate_predecessor_signal_has_no_fixed_delay(self) -> None:
        class Rest:
            async def immediately_preceding_settled_btc15m(self, current_open_epoch, available_by_epoch):
                self.current_open_epoch = current_open_epoch
                self.available_by_epoch = available_by_epoch
                return {
                    "ticker": "KXBTC15M-prior",
                    "result": "yes",
                    "close_epoch": current_open_epoch,
                    "settlement_epoch": current_open_epoch + 6,
                    "settlement_ts": datetime.fromtimestamp(current_open_epoch + 6, tz=timezone.utc).isoformat(),
                    "source": "test",
                }

        config = trader.validate_config({
            "settlement_contrarian_entry_grace_seconds": 999.0,
        })
        self.assertEqual(config["settlement_contrarian_entry_grace_seconds"], 840.0)
        opened_at = 1_700_000_000
        market = {"ticker": "KXBTC15M-current", "open_time": opened_at}
        record = {"ticker": "KXBTC15M-current", "status": "watching"}
        rest = Rest()
        side = await trader.settlement_contrarian_side_for_market(
            rest, market, record, config, now_epoch=opened_at + 7,
        )
        self.assertEqual(side, "no")
        self.assertEqual(rest.current_open_epoch, opened_at)
        self.assertEqual(rest.available_by_epoch, opened_at + 7)
        self.assertEqual(record["settlement_contrarian_signal"]["source_close_epoch"], opened_at)
        self.assertEqual(record["settlement_contrarian_signal"]["decision_available_epoch"], opened_at + 7)

    async def test_immediate_predecessor_can_arrive_late_within_penultimate_minute_window(self) -> None:
        class Rest:
            async def immediately_preceding_settled_btc15m(self, current_open_epoch, available_by_epoch):
                self.available_by_epoch = available_by_epoch
                return {
                    "ticker": "KXBTC15M-prior", "result": "no",
                    "close_epoch": current_open_epoch,
                    "settlement_epoch": available_by_epoch,
                    "settlement_ts": datetime.fromtimestamp(available_by_epoch, tz=timezone.utc).isoformat(),
                    "source": "test",
                }

        config = trader.validate_config({})
        opened_at = 1_700_000_000
        record = {"ticker": "KXBTC15M-current", "status": "watching"}
        rest = Rest()
        side = await trader.settlement_contrarian_side_for_market(
            rest, {"ticker": "KXBTC15M-current", "open_time": opened_at}, record, config,
            now_epoch=opened_at + 839,
        )
        self.assertEqual(side, "yes")
        self.assertEqual(rest.available_by_epoch, opened_at + 839)
        self.assertEqual(record["status"], "watching")

    async def test_source_window_expiry_is_a_single_terminal_no_order_state(self) -> None:
        class Rest:
            async def immediately_preceding_settled_btc15m(self, *args):
                raise AssertionError("expired source window must not issue another settlement lookup")

        config = trader.validate_config({})
        opened_at = 1_700_000_000
        record = {"ticker": "KXBTC15M-current", "status": "watching", "orders": {}}
        side = await trader.settlement_contrarian_side_for_market(
            Rest(), {"ticker": "KXBTC15M-current", "open_time": opened_at}, record, config,
            now_epoch=opened_at + 840.01,
        )
        self.assertIsNone(side)
        self.assertEqual(record["status"], "signal_window_missed")
        self.assertEqual(record["settlement_contrarian_status"], "signal_window_missed")
        self.assertIn(record["status"], trader.FINAL_RECORD_STATUSES)
        self.assertEqual(record["reserved_principal"], 0.0)
        self.assertIsNone(await trader.settlement_contrarian_side_for_market(
            Rest(), {"ticker": "KXBTC15M-current", "open_time": opened_at}, record, config,
            now_epoch=opened_at + 400,
        ))

    def test_early_exit_later_records_official_outcome_without_repricing_realized_pnl(self) -> None:
        record = {
            "ticker": "KXBTC15M-stopped",
            "strategy": trader.LIVE_EXECUTION_STRATEGY,
            "status": "exited_early",
            "locked_side": "yes",
            "orders": {
                "0.4000": {
                    "fill_count": 3.0,
                    "average_fill_price": 0.40,
                    "fees_paid": 0.0,
                },
            },
            "live_exit_orders": [{
                "fill_count": 3.0,
                "average_fill_price": 0.05,
                "fees_paid": 0.0,
            }],
            "net_profit_loss": -1.05,
        }
        self.assertTrue(trader.annotate_early_exit_settlement_outcome(
            record, {"status": "finalized", "result": "yes"},
        ))
        self.assertEqual(record["settlement_outcome"], "yes")
        self.assertEqual(record["post_exit_directional_result"], "would_have_won")
        self.assertFalse(trader.annotate_early_exit_settlement_outcome(
            record, {"status": "finalized", "result": "yes"},
        ))
        rung = trader.rung_performance([record])["0.40"]
        self.assertEqual(rung["net_profit"], -1.05)
        self.assertEqual(rung["losing_orders"], 1)

    async def test_signal_waits_for_immediate_predecessor_not_an_older_settlement(self) -> None:
        class Rest:
            async def immediately_preceding_settled_btc15m(self, current_open_epoch, available_by_epoch):
                self.current_open_epoch = current_open_epoch
                self.available_by_epoch = available_by_epoch
                return None

        config = trader.validate_config({})
        opened_at = 1_700_000_000
        record = {"ticker": "KXBTC15M-current"}
        rest = Rest()
        side = await trader.settlement_contrarian_side_for_market(
            rest, {"ticker": "KXBTC15M-current", "open_time": opened_at}, record, config, now_epoch=opened_at + 7,
        )
        self.assertIsNone(side)
        self.assertEqual(record["settlement_contrarian_status"], "awaiting_immediate_predecessor")
        self.assertEqual(rest.current_open_epoch, opened_at)

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

    def test_dynamic_scaling_disabled_keeps_starting_base_and_ignores_ledger_profit(self) -> None:
        config = trader.validate_config({
            "enable_dynamic_scaling": False,
            "base_share_increment": 2,
            "scaling_profit_multiplier": 1.0,
        })
        base = 1_700_001_000
        state = {"markets": {
            "KXBTC15M-historical-win": self.completed_live_record(
                "KXBTC15M-historical-win", base, 500.0,
            ),
        }}
        trader.refresh_dynamic_base_share_scaling(state, config, now_epoch=base + 1)
        snapshot = trader.dynamic_scaling_snapshot(state, config)
        self.assertFalse(snapshot["enabled"])
        self.assertEqual(snapshot["current_base_share_count"], 3.0)
        self.assertEqual(snapshot["profit_since_last_increase"], 0.0)
        self.assertEqual(
            trader.live_rung_quantities(config, state),
            {0.40: 3.0, 0.30: 6.0, 0.20: 9.0, 0.10: 12.0},
        )

    def test_dynamic_scaling_starts_fresh_then_increases_and_expands_auto_caps(self) -> None:
        config = trader.validate_config({
            "enable_dynamic_scaling": True,
            "base_share_increment": 1,
            "scaling_profit_multiplier": 16.5,
        })
        base = 1_700_001_100
        state = {"markets": {
            # Enabling must not replay this older completed trade.
            "KXBTC15M-before-enable": self.completed_live_record(
                "KXBTC15M-before-enable", base, 999.0,
            ),
        }}
        trader.refresh_dynamic_base_share_scaling(state, config, now_epoch=base + 1)
        self.assertEqual(trader.dynamic_base_share_count(state, config), 3.0)
        state["markets"]["KXBTC15M-new-threshold-win"] = self.completed_live_record(
            "KXBTC15M-new-threshold-win", base + 2, 49.5,
        )
        trader.refresh_dynamic_base_share_scaling(state, config, now_epoch=base + 3)
        snapshot = trader.dynamic_scaling_snapshot(state, config)
        self.assertEqual(snapshot["current_base_share_count"], 4.0)
        self.assertEqual(snapshot["profit_since_last_increase"], 0.0)
        self.assertEqual(snapshot["scale_count"], 1)
        self.assertEqual(
            trader.live_rung_quantities(config, state),
            {0.40: 4.0, 0.30: 8.0, 0.20: 12.0, 0.10: 16.0},
        )
        self.assertEqual(config["max_contracts_per_market"], 40.0)
        self.assertEqual(config["max_total_capital"], 8.0)

    def test_dynamic_scaling_uses_net_profit_since_last_increase_and_increment(self) -> None:
        config = trader.validate_config({
            "enable_dynamic_scaling": True,
            "base_share_increment": 2,
            "scaling_profit_multiplier": 1.0,
        })
        base = 1_700_001_200
        state = {"markets": {}}
        trader.refresh_dynamic_base_share_scaling(state, config, now_epoch=base)
        state["markets"]["KXBTC15M-loss"] = self.completed_live_record("KXBTC15M-loss", base + 1, -1.0)
        state["markets"]["KXBTC15M-win"] = self.completed_live_record("KXBTC15M-win", base + 2, 4.0)
        trader.refresh_dynamic_base_share_scaling(state, config, now_epoch=base + 3)
        snapshot = trader.dynamic_scaling_snapshot(state, config)
        # -$1 + $4 reaches the $3 threshold for base three, then the exact
        # specified reset leaves no carried profit after a 3 -> 5 increase.
        self.assertEqual(snapshot["current_base_share_count"], 5.0)
        self.assertEqual(snapshot["profit_since_last_increase"], 0.0)
        self.assertEqual(
            trader.live_rung_quantities(config, state),
            {0.40: 5.0, 0.30: 10.0, 0.20: 15.0, 0.10: 20.0},
        )

    def test_dynamic_scaling_accepts_fractional_base_increment_at_cent_precision(self) -> None:
        config = trader.validate_config({
            "initial_position_size": 3.0,
            "enable_dynamic_scaling": True,
            "base_share_increment": 0.25,
            "scaling_profit_multiplier": 0.75,
        })
        base = 1_700_001_250
        state = {"markets": {}}
        trader.refresh_dynamic_base_share_scaling(state, config, now_epoch=base)
        state["markets"]["KXBTC15M-fractional-threshold-win"] = self.completed_live_record(
            "KXBTC15M-fractional-threshold-win", base + 1, 2.25,
        )
        trader.refresh_dynamic_base_share_scaling(state, config, now_epoch=base + 2)
        snapshot = trader.dynamic_scaling_snapshot(state, config)
        self.assertEqual(snapshot["required_profit"], 2.4375)
        self.assertEqual(snapshot["current_base_share_count"], 3.25)
        self.assertEqual(
            trader.live_rung_quantities(config, state),
            {0.40: 3.25, 0.30: 6.5, 0.20: 9.75, 0.10: 13.0},
        )

    def test_share_sizing_rejects_more_than_cent_precision(self) -> None:
        minimum = trader.validate_config({"initial_position_size": 0.01})
        self.assertEqual(
            trader.live_rung_quantities(minimum),
            {0.40: 0.01, 0.30: 0.02, 0.20: 0.03, 0.10: 0.04},
        )
        with self.assertRaisesRegex(ValueError, "two decimal places"):
            trader.validate_config({"base_share_increment": 0.001})
        with self.assertRaisesRegex(ValueError, "two decimal places"):
            trader.validate_config({"initial_position_size": 3.001})

    async def test_live_order_boundaries_preserve_cent_precision(self) -> None:
        # ``dry_run`` reaches the exact live request-construction boundary
        # without requiring credentials or a network call.
        rest = object.__new__(trader.KalshiREST)
        entry = await rest.create_order(
            ticker="KXBTC15M-cent-precision", side="yes", position_price=0.40,
            quantity=3.25, tif="good_till_canceled", expiration_time=1_700_000_000,
            dry_run=True, order_key="fractional",
        )
        self.assertEqual(entry["quantity"], 3.25)
        with self.assertRaisesRegex(ValueError, "two decimal places"):
            await rest.create_order(
                ticker="KXBTC15M-cent-precision", side="yes", position_price=0.40,
                quantity=3.251, tif="good_till_canceled", expiration_time=1_700_000_000,
                dry_run=True, order_key="invalid",
            )
        with self.assertRaisesRegex(ValueError, "two decimal places"):
            await rest.create_reduce_only_exit(
                ticker="KXBTC15M-cent-precision", held_side="yes", economic_exit_price=0.05,
                quantity=3.251, dry_run=True, order_key="invalid-exit",
            )

    def test_dynamic_scaling_action_overrides_are_persistable_and_explicit_caps_stay_manual(self) -> None:
        parser = trader.build_parser()
        args = parser.parse_args([
            "--enable-dynamic-scaling", "true",
            "--base-share-increment", "2",
            "--scaling-profit-multiplier", "20",
            "--max-contracts-per-market", "60",
            "--max-total-capital", "12",
        ])
        config, changed = trader.apply_config_overrides(trader.validate_config({}), args)
        self.assertTrue(changed)
        self.assertTrue(config["enable_dynamic_scaling"])
        self.assertEqual(config["base_share_increment"], 2.0)
        self.assertEqual(config["scaling_profit_multiplier"], 20.0)
        self.assertFalse(config["max_contracts_per_market_auto"])
        self.assertFalse(config["max_total_capital_auto"])

    async def test_fractional_scaled_base_is_snapshotted_on_the_next_full_live_ladder(self) -> None:
        class Rest:
            def __init__(self) -> None:
                self.orders: list[dict] = []

            async def balance_dollars(self):
                return 100.0

            async def create_order(self, **kwargs):
                self.orders.append(kwargs)
                return {
                    "fill_count": 0.0,
                    "remaining_count": kwargs["quantity"],
                    "position_price": kwargs["position_price"],
                    "status": "resting",
                }

        config = trader.validate_config({
            "enable_dynamic_scaling": True,
            "base_share_increment": 0.25,
            "scaling_profit_multiplier": 0.75,
        })
        now = time.time()
        state = {"markets": {}}
        trader.refresh_dynamic_base_share_scaling(state, config, now_epoch=now)
        state["markets"]["KXBTC15M-scale-win"] = self.completed_live_record(
            "KXBTC15M-scale-win", int(now) + 1, 2.25,
        )
        trader.refresh_dynamic_base_share_scaling(state, config, now_epoch=now + 2)
        ticker = "KXBTC15M-scaled-entry"
        market = {"ticker": ticker, "status": "active", "open_time": now - 1, "close_time": now + 300}
        rest = Rest()
        submitted = await trader.consider_initial_entry(
            rest, state, market, config, dry_run=False, ml_side="yes", signal_source="settlement_contrarian",
        )
        self.assertTrue(submitted)
        record = state["markets"][ticker]
        self.assertEqual(record["base_share_count"], 3.25)
        self.assertEqual(
            record["rung_quantities"],
            {"0.4000": 3.25, "0.3000": 6.5, "0.2000": 9.75, "0.1000": 13.0},
        )
        self.assertEqual([order["quantity"] for order in rest.orders], [3.25, 6.5, 9.75, 13.0])

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

    def test_two_completed_losses_skip_next_two_normal_signals(self) -> None:
        config = trader.validate_config({})
        state = {"markets": {}}
        base = 1_700_000_000
        trader.refresh_entry_loss_skip(state, config, now_epoch=base)
        state["markets"]["KXBTC15M-loss-1"] = self.completed_live_record(
            "KXBTC15M-loss-1", base + 1, -0.40,
        )
        trader.refresh_entry_loss_skip(state, config, now_epoch=base + 2)
        self.assertEqual(state["entry_loss_skip"]["consecutive_completed_losses"], 1)
        state["markets"]["KXBTC15M-loss-2"] = self.completed_live_record(
            "KXBTC15M-loss-2", base + 3, -0.30,
        )
        trader.refresh_entry_loss_skip(state, config, now_epoch=base + 4)
        self.assertEqual(state["entry_loss_skip"]["markets_remaining_to_skip"], 2)

        skipped_one = {
            "ticker": "KXBTC15M-skip-1", "status": "watching", "orders": {},
            "settlement_contrarian_signal": {"side": "yes", "source_ticker": "KXBTC15M-loss-2"},
        }
        self.assertTrue(trader.consume_entry_loss_skip(state, skipped_one, "yes", config))
        self.assertEqual(skipped_one["status"], "entry_skipped_loss_circuit_breaker")
        self.assertEqual(skipped_one["candidate_side"], "yes")
        self.assertEqual(skipped_one["settlement_contrarian_signal"]["side"], "yes")
        self.assertEqual(skipped_one["orders"], {})
        self.assertEqual(state["entry_loss_skip"]["markets_remaining_to_skip"], 1)

        skipped_two = {"ticker": "KXBTC15M-skip-2", "status": "watching", "orders": {}}
        self.assertTrue(trader.consume_entry_loss_skip(state, skipped_two, "no", config))
        self.assertEqual(skipped_two["status"], "entry_skipped_loss_circuit_breaker")
        self.assertEqual(state["entry_loss_skip"]["markets_remaining_to_skip"], 0)
        self.assertEqual(state["entry_loss_skip"]["consecutive_completed_losses"], 0)

        next_normal_signal = {"ticker": "KXBTC15M-resume", "status": "watching", "orders": {}}
        self.assertFalse(trader.consume_entry_loss_skip(state, next_normal_signal, "yes", config))
        self.assertEqual(next_normal_signal["status"], "watching")

    def test_completed_win_immediately_clears_pending_skips(self) -> None:
        config = trader.validate_config({})
        base = 1_700_000_100
        state = {
            "markets": {
                "KXBTC15M-loss-2": self.completed_live_record("KXBTC15M-loss-2", base, -0.30),
                "KXBTC15M-win": self.completed_live_record("KXBTC15M-win", base + 1, 0.40),
            },
            "entry_loss_skip": {
                "initialized": True,
                "consecutive_completed_losses": 2,
                "markets_remaining_to_skip": 2,
                "last_processed_completion_epoch": base,
                "last_processed_completion_ticker": "KXBTC15M-loss-2",
            },
        }
        trader.refresh_entry_loss_skip(state, config, now_epoch=base + 2)
        self.assertEqual(state["entry_loss_skip"]["consecutive_completed_losses"], 0)
        self.assertEqual(state["entry_loss_skip"]["markets_remaining_to_skip"], 0)
        self.assertEqual(state["entry_loss_skip"]["last_reset_reason"], "completed_winning_trade")

    def test_completed_loss_while_skip_is_pending_does_not_extend_two_market_skip(self) -> None:
        config = trader.validate_config({})
        base = 1_700_000_150
        state = {
            "markets": {
                "KXBTC15M-loss-2": self.completed_live_record("KXBTC15M-loss-2", base, -0.30),
                "KXBTC15M-prior-open-loss": self.completed_live_record("KXBTC15M-prior-open-loss", base + 1, -0.20),
            },
            "entry_loss_skip": {
                "initialized": True,
                "consecutive_completed_losses": 2,
                "markets_remaining_to_skip": 1,
                "last_processed_completion_epoch": base,
                "last_processed_completion_ticker": "KXBTC15M-loss-2",
            },
        }
        trader.refresh_entry_loss_skip(state, config, now_epoch=base + 2)
        self.assertEqual(state["entry_loss_skip"]["markets_remaining_to_skip"], 1)
        self.assertEqual(state["entry_loss_skip"]["consecutive_completed_losses"], 2)

    def test_zero_fill_or_dry_run_records_never_count_as_completed_losses(self) -> None:
        config = trader.validate_config({})
        base = 1_700_000_200
        state = {
            "markets": {
                "KXBTC15M-unfilled": {
                    "ticker": "KXBTC15M-unfilled", "strategy": trader.LIVE_EXECUTION_STRATEGY,
                    "status": "finalized_unfilled", "contracts": 0.0,
                    "net_profit_loss": -1.0,
                    "settled_at": datetime.fromtimestamp(base, tz=timezone.utc).isoformat(),
                },
                "KXBTC15M-paper": {
                    "ticker": "KXBTC15M-paper", "strategy": trader.LIVE_EXECUTION_STRATEGY,
                    "execution_mode": "dry_run", "status": "finalized", "contracts": 3.0,
                    "net_profit_loss": -1.0,
                    "settled_at": datetime.fromtimestamp(base + 1, tz=timezone.utc).isoformat(),
                },
            },
        }
        trader.refresh_entry_loss_skip(state, config, now_epoch=base + 2)
        snapshot = trader.entry_loss_skip_snapshot(state, config)
        self.assertEqual(snapshot["consecutive_completed_losses"], 0)
        self.assertEqual(snapshot["markets_remaining_to_skip"], 0)

    async def test_loss_skip_blocks_the_actual_order_path_after_normal_signal(self) -> None:
        class Rest:
            async def balance_dollars(self):
                raise AssertionError("a skipped signal must not check balance or submit an order")

        config = trader.validate_config({})
        ticker = "KXBTC15M-skip-order-path"
        now = time.time()
        state = {
            "markets": {
                ticker: {
                    "ticker": ticker,
                    "status": "watching",
                    "orders": {},
                    "settlement_contrarian_signal": {"side": "no", "source_ticker": "KXBTC15M-prior"},
                },
            },
            "entry_loss_skip": {
                "initialized": True,
                "consecutive_completed_losses": 2,
                "markets_remaining_to_skip": 2,
            },
        }
        market = {"ticker": ticker, "status": "active", "open_time": now - 1, "close_time": now + 300}
        submitted = await trader.consider_initial_entry(
            Rest(), state, market, config, dry_run=False, ml_side="no", signal_source="settlement_contrarian",
        )
        self.assertFalse(submitted)
        record = state["markets"][ticker]
        self.assertEqual(record["status"], "entry_skipped_loss_circuit_breaker")
        self.assertEqual(record["locked_side"], "no")
        self.assertEqual(record["orders"], {})


if __name__ == "__main__":
    unittest.main()
