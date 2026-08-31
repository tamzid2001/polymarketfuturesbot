from __future__ import annotations

import unittest
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

from optimizer import export_selected_live_strategy
from recovery_sizing import RecoverySizingState
from strategy_core import (
    StrategyParameters,
    apply_realized_filled_trade,
    effective_stop_price,
    prescribed_quantity,
    sizing_state,
    sticky_directional_prediction,
    zero_fill_snapshot,
)
from kalshi_live_trader import (
    ACTIVE_CONFIG_SCHEMA_VERSION,
    ACTIVE_STRATEGY_VERSION,
    load_config,
    load_config_from_value,
)


ROOT = Path(__file__).resolve().parents[1]


class StrategyCoreTests(unittest.TestCase):
    def parameters(self, increment: str = "0.25") -> StrategyParameters:
        return StrategyParameters(
            recovery_multiplier=Decimal("1.11"), first_base_threshold=Decimal("50"),
            threshold_growth_multiplier=Decimal("1.11"), base_increment=Decimal(increment),
        )

    def test_exact_fractional_sequence_starts_at_one_share(self) -> None:
        parameters = self.parameters()
        snapshot: dict = {}
        quantities = []
        for _ in range(5):
            quantity, _ = prescribed_quantity(parameters, snapshot)
            quantities.append(quantity)
            snapshot, _ = apply_realized_filled_trade(parameters, snapshot, Decimal("-0.01"))
        self.assertEqual(quantities, [Decimal("1.00"), Decimal("1.11"), Decimal("1.23"), Decimal("1.37"), Decimal("1.52")])

    def test_zero_fill_is_a_strict_noop(self) -> None:
        parameters = self.parameters()
        snapshot, _ = apply_realized_filled_trade(parameters, {}, Decimal("-5.00"))
        self.assertEqual(zero_fill_snapshot(parameters, snapshot), snapshot)

    def test_individual_profit_does_not_reset_negative_recovery_cycle(self) -> None:
        parameters = self.parameters()
        snapshot, _ = apply_realized_filled_trade(parameters, {}, Decimal("-5.00"))
        snapshot, change = apply_realized_filled_trade(parameters, snapshot, Decimal("2.00"))
        self.assertEqual(snapshot["recovery_cycle_pnl"], "-3.00")
        self.assertEqual(snapshot["recovery_exponent"], 2)
        self.assertFalse(change["recovery_reset"])
        snapshot, change = apply_realized_filled_trade(parameters, snapshot, Decimal("3.00"))
        self.assertEqual(snapshot["recovery_cycle_pnl"], "0")
        self.assertEqual(snapshot["recovery_exponent"], 0)
        self.assertTrue(change["recovery_reset"])

    def test_replay_and_live_snapshots_are_identical(self) -> None:
        parameters = self.parameters("0.50")
        events = [Decimal("-0.49"), Decimal("0.51"), Decimal("-0.09"), Decimal("1.11")]
        live_snapshot: dict = {}
        replay_state = RecoverySizingState(
            parameters.recovery_multiplier, parameters.first_base_threshold, parameters.base_increment,
            parameters.threshold_growth_multiplier, parameters.starting_base, parameters.max_position,
        )
        for event in events:
            live_snapshot, _ = apply_realized_filled_trade(parameters, live_snapshot, event)
            replay_state.apply_filled_trade(event)
            self.assertEqual(sizing_state(parameters, live_snapshot).snapshot(), replay_state.snapshot())

    def test_all_supported_base_increments_are_shared(self) -> None:
        for increment, expected in (("0.25", "1.25"), ("0.50", "1.50"), ("1.00", "2.00")):
            snapshot, _ = apply_realized_filled_trade(self.parameters(increment), {}, Decimal("50"))
            self.assertEqual(snapshot["base_share_count"], expected)

    def test_optimizer_live_export_round_trips_without_reinterpreting_decimals(self) -> None:
        row = {
            "entry_price": .50, "stop_price": .40, "recovery_multiplier": 1.01,
            "first_base_threshold": 100, "threshold_growth_multiplier": 1.01, "base_increment": 1.00,
        }
        with TemporaryDirectory() as directory:
            path = Path(directory) / "selected_live_strategy.json"
            export_selected_live_strategy(path, row, selection_basis="test")
            config = load_config(path)
        self.assertEqual(config["entry_price"], "0.50")
        self.assertEqual(config["starting_base"], "1.00")
        self.assertEqual(config["base_increment"], "1.00")
        self.assertEqual(config["opening_price_discovery_seconds"], 3)
        self.assertEqual(config["entry_execution_mode"], "signal_price_minus_offset_maker")
        self.assertEqual(config["maker_order_time_in_force"], "good_till_canceled")
        self.assertEqual(config["entry_order_lifetime"], "until_filled_or_market_close")
        self.assertEqual(config["entry_timeout_seconds"], 0)
        self.assertEqual(config["opening_quote_capture_seconds"], 60)
        self.assertTrue(config["delayed_entry_tracking_enabled"])
        self.assertEqual(config["delayed_entry_threshold_cents"], 53)
        self.assertEqual(config["entry_limit_offset_cents"], 1)
        self.assertEqual(config["max_recovery_exponent"], 0)
        self.assertEqual(config["stop_policy"], "hybrid_maker_then_hard_stop")
        self.assertEqual(
            (config["hybrid_stop_trigger_cents"], config["hybrid_maker_exit_cents"], config["hybrid_hard_stop_cents"]),
            (45, 46, 44),
        )
        self.assertEqual(config["stop_baseline_entry_price"], "0.50")
        self.assertEqual(config["strategy_version"], ACTIVE_STRATEGY_VERSION)
        self.assertEqual(config["config_schema_version"], ACTIVE_CONFIG_SCHEMA_VERSION)

    def test_optimizer_export_rejects_retired_stop_profiles(self) -> None:
        row = {
            "entry_price": .49, "stop_price": .20, "recovery_multiplier": 1.01,
            "first_base_threshold": 350, "threshold_growth_multiplier": 1.01, "base_increment": .50,
        }
        with TemporaryDirectory() as directory:
            path = Path(directory) / "selected_live_strategy.json"
            with self.assertRaisesRegex(ValueError, "only the canonical sticky_stop_40"):
                export_selected_live_strategy(path, row, selection_basis="test")

    def test_initial_shares_are_configurable_but_exact_and_capped(self) -> None:
        config = load_config(ROOT / "selected_live_strategy.json")
        changed = load_config_from_value(dict(config, starting_base="2.50"))
        self.assertEqual(changed["starting_base"], "2.50")
        self.assertEqual(prescribed_quantity(StrategyParameters(
            recovery_multiplier=Decimal(changed["recovery_multiplier"]),
            first_base_threshold=Decimal(changed["first_base_threshold"]),
            threshold_growth_multiplier=Decimal(changed["threshold_growth_multiplier"]),
            base_increment=Decimal(changed["base_increment"]),
            starting_base=Decimal(changed["starting_base"]),
            max_position=Decimal(changed["max_position"]),
        ))[0], Decimal("2.50"))
        with self.assertRaisesRegex(ValueError, "at most two decimal"):
            load_config_from_value(dict(config, starting_base="1.001"))
        with self.assertRaisesRegex(ValueError, "no greater than max_position"):
            load_config_from_value(dict(config, starting_base="100.01"))
        with self.assertRaisesRegex(ValueError, "at least 1"):
            load_config_from_value(dict(config, recovery_multiplier="0.99"))
        with self.assertRaisesRegex(ValueError, "canonical 40c profile"):
            load_config_from_value(dict(
                config,
                hybrid_hard_stop_cents=39,
                hybrid_stop_trigger_cents=40,
                hybrid_maker_exit_cents=41,
            ))

    def test_legacy_live_configuration_fails_closed(self) -> None:
        config = load_config(ROOT / "selected_live_strategy.json")
        legacy_version = dict(config, strategy_version="kxbtc15m-hybrid-live-v1")
        legacy_schema = dict(config, config_schema_version=1)
        pre_reconciliation_safety_schema = dict(config, config_schema_version=4)
        with self.assertRaisesRegex(ValueError, "non-current live strategy"):
            load_config_from_value(legacy_version)
        with self.assertRaisesRegex(ValueError, "non-current live configuration schema"):
            load_config_from_value(legacy_schema)
        with self.assertRaisesRegex(ValueError, "non-current live configuration schema"):
            load_config_from_value(pre_reconciliation_safety_schema)

    def test_active_config_rejects_legacy_maker_execution_mode(self) -> None:
        config = load_config(ROOT / "selected_live_strategy.json")
        with self.assertRaisesRegex(ValueError, "entry_execution_mode"):
            load_config_from_value(dict(config, entry_execution_mode="maker_then_ioc"))

    def test_active_config_rejects_non_gtc_maker_orders(self) -> None:
        config = load_config(ROOT / "selected_live_strategy.json")
        with self.assertRaisesRegex(ValueError, "maker_order_time_in_force"):
            load_config_from_value(dict(config, maker_order_time_in_force="immediate_or_cancel"))

    def test_active_config_rejects_any_strategy_time_expiry(self) -> None:
        config = load_config(ROOT / "selected_live_strategy.json")
        with self.assertRaisesRegex(ValueError, "entry_timeout_seconds"):
            load_config_from_value(dict(config, entry_timeout_seconds=60))
        with self.assertRaisesRegex(ValueError, "entry_order_lifetime"):
            load_config_from_value(dict(config, entry_order_lifetime="until_timeout"))

    def test_active_config_rejects_delayed_signal_or_recovery_exponent_breaker(self) -> None:
        config = load_config(ROOT / "selected_live_strategy.json")
        with self.assertRaisesRegex(ValueError, "signal_delay_seconds must be 0"):
            load_config_from_value(dict(config, signal_delay_seconds=1))
        with self.assertRaisesRegex(ValueError, "max_recovery_exponent must be 0"):
            load_config_from_value(dict(config, max_recovery_exponent=12))

    def test_active_config_requires_full_market_delayed_53_analytics(self) -> None:
        config = load_config(ROOT / "selected_live_strategy.json")
        with self.assertRaisesRegex(ValueError, "delayed_entry_tracking_enabled"):
            load_config_from_value(dict(config, delayed_entry_tracking_enabled=False))
        with self.assertRaisesRegex(ValueError, "delayed_entry_threshold_cents=53"):
            load_config_from_value(dict(config, delayed_entry_threshold_cents=54))

    def test_v9_fixed_stop_is_not_adjusted_by_actual_entry_price(self) -> None:
        floor = Decimal("0.40")
        baseline = Decimal("0.50")
        self.assertEqual(effective_stop_price(Decimal("0.49"), floor, baseline), floor)
        self.assertEqual(effective_stop_price(Decimal("0.50"), floor, baseline), floor)
        # This helper remains for archived research, but the v9 live contract
        # does not invoke it for an active position.
        self.assertEqual(effective_stop_price(Decimal("0.52"), floor, baseline), Decimal("0.42"))
        self.assertEqual(effective_stop_price(Decimal("0.54"), floor, baseline), Decimal("0.44"))

    def test_sticky_direction_holds_after_loss_and_flips_after_win(self) -> None:
        # Fresh state is contrarian to the just-completed market.  Thereafter
        # the side is a state machine, independent of fills, stops, or P&L.
        self.assertEqual(sticky_directional_prediction(None, "yes"), ("no", "seed_inverse_settlement"))
        self.assertEqual(sticky_directional_prediction("no", "yes"), ("no", "hold_after_directional_loss"))
        self.assertEqual(sticky_directional_prediction("no", "no"), ("yes", "flip_after_directional_win"))
        self.assertEqual(sticky_directional_prediction("yes", "no"), ("yes", "hold_after_directional_loss"))
        self.assertEqual(sticky_directional_prediction("yes", "yes"), ("no", "flip_after_directional_win"))

    def test_comparison_stop_profiles_are_shadow_only_and_isolated(self) -> None:
        config = load_config(ROOT / "selected_live_strategy.json")
        for cents in (10, 20, 25, 30, 35):
            profile = load_config_from_value(dict(
                config,
                shadow_profile=f"sticky_stop_{cents}",
                stop_price=f"0.{cents:02d}",
                hybrid_hard_stop_cents=cents,
                hybrid_stop_trigger_cents=cents + 1,
                hybrid_maker_exit_cents=cents + 2,
                trading_mode="shadow",
            ))
            self.assertEqual(profile["shadow_profile"], f"sticky_stop_{cents}")
            self.assertEqual(profile["hybrid_hard_stop_cents"], cents)
            with self.assertRaisesRegex(ValueError, "shadow-only"):
                load_config_from_value(dict(profile, trading_mode="live"))
            with self.assertRaisesRegex(ValueError, r"profile/profile\+1c/profile\+2c"):
                load_config_from_value(dict(profile, hybrid_stop_trigger_cents=cents))

    def test_production_workflows_pin_v11_and_never_dispatch_retired_lanes(self) -> None:
        worker = (ROOT / ".github/workflows/kalshi_btc15m_average_down.yml").read_text(encoding="utf-8")
        watchdog = (ROOT / ".github/workflows/kalshi_btc15m_watchdog.yml").read_text(encoding="utf-8")
        controlled = (ROOT / ".github/workflows/kalshi_btc15m_controlled_restart.yml").read_text(encoding="utf-8")
        trader = (ROOT / "kalshi_live_trader.py").read_text(encoding="utf-8")
        self.assertIn('entry_execution_mode"] == "signal_price_minus_offset_maker"', worker)
        self.assertIn('maker_order_time_in_force"] == "good_till_canceled"', worker)
        self.assertIn('entry_order_lifetime"] == "until_filled_or_market_close"', worker)
        self.assertIn('entry_timeout_seconds"] == 0', worker)
        self.assertIn("OPENING_PRICE_CAPTURE_CONTRACT_VERSION == 3", worker)
        self.assertIn("BTC_TARGET_CAPTURE_CONTRACT_VERSION == 2", worker)
        self.assertIn("DELAYED_ENTRY_LADDER_CONTRACT_VERSION == 3", worker)
        self.assertIn("(DELAYED_HYBRID_STOP_TRIGGER_CENTS, DELAYED_HYBRID_MAKER_EXIT_CENTS, DELAYED_HYBRID_HARD_STOP_CENTS) == (51, 52, 50)", worker)
        self.assertIn("delayed_ladder=contract_v3_trigger_ask_minus_1c_public_trade_fill_volume_shared_with_rungs_stop=51trigger_52maker_50hard", worker)
        self.assertIn("opening_price=first_price_only_post_open contract=v3", worker)
        self.assertIn("btc_target=exchange_market_detail contract=v2", worker)
        self.assertIn("cross_ladder=53-59c:50x1,40x2,30x4,20x8,10x16", worker)
        self.assertNotIn("--entry-timeout-seconds", worker)
        self.assertIn("hybrid=validated_configurable", worker)
        self.assertIn("kalshi_shadow_maker_hybrid_v11_sticky_stop_40", worker)
        self.assertIn("--persist-config", worker)
        self.assertIn('--starting-base "$INITIAL_SHARES"', worker)
        self.assertIn('--recovery-multiplier "$SCALING_MULTIPLIER"', worker)
        self.assertIn('--threshold-growth-multiplier "$SCALING_MULTIPLIER"', worker)
        self.assertIn('--first-base-threshold "$PROFIT_THRESHOLD"', worker)
        self.assertIn('--base-increment "$SHARES_ADDED_AFTER_PROFIT_THRESHOLD"', worker)
        self.assertIn('--hybrid-hard-stop-cents "$MAX_STOP_LOSS_CENTS"', worker)
        self.assertIn("RUNTIME_STATE_RESTORED=$runtime_ref", worker)
        self.assertIn("runtime_ref=runtime-state-kxbtc15m", worker)
        self.assertIn("legacy_runtime_ref=runtime-state", worker)
        self.assertIn("RUNTIME_STATE_OWNER=kalshi-kxbtc15m", worker)
        self.assertIn('live_checkpoint.py --restore-sha "$restore_sha" --runtime-ref "$runtime_ref"', worker)
        self.assertIn('c["durable_checkpoint_interval_seconds"] == 30.0', worker)
        self.assertIn(
            "python live_checkpoint.py --reason end-of-run --runtime-ref runtime-state-kxbtc15m",
            worker,
        )
        self.assertLess(
            trader.index("state = load_state(args.state_file, config)"),
            trader.index("if args.persist_config:", trader.index("async def async_main")),
        )
        self.assertNotIn("git push origin HEAD:main", worker)
        self.assertNotIn("chore: checkpoint KXBTC15M hybrid state", worker)
        self.assertIn("RUNTIME_STATE_RESTORED=$runtime_ref", controlled)
        self.assertIn("runtime_ref=runtime-state-kxbtc15m", controlled)
        self.assertIn("legacy_runtime_ref=runtime-state", controlled)
        self.assertIn("RUNTIME_STATE_OWNER=kalshi-kxbtc15m", controlled)
        self.assertIn('live_checkpoint.py --restore-sha "$restore_sha" --runtime-ref "$runtime_ref"', controlled)
        self.assertIn('record.get("status") == "SIGNAL_PENDING"', controlled)
        self.assertIn("and breaker_blocked", controlled)
        self.assertIn("and not has_order_work(record)", controlled)
        self.assertIn("WATCHDOG MAINTENANCE HOLD", watchdog)
        self.assertNotIn("cron:", worker)
        self.assertIn('cron: "2-59/5 * * * *"', watchdog)
        input_names = set(__import__("re").findall(
            r"^      [a-zA-Z0-9_]+:\s*$",
            worker[worker.index("    inputs:"):worker.index("# Serialized runs")],
            flags=__import__("re").MULTILINE,
        ))
        input_names = {name.strip()[:-1] for name in input_names}
        self.assertEqual(input_names, {
            "live_enabled", "reconcile_only", "initial_shares", "scaling_multiplier",
            "profit_threshold", "shares_added_after_profit_threshold", "max_stop_loss_cents",
        })
        controlled_inputs = set(__import__("re").findall(
            r"^      [a-zA-Z0-9_]+:\s*$",
            controlled[controlled.index("    inputs:"):controlled.index("permissions:")],
            flags=__import__("re").MULTILINE,
        ))
        self.assertEqual({name.strip()[:-1] for name in controlled_inputs}, {"source_run_id", "target_live"})
        for retired_input in ("dry_run", "shadow_profile", "reset_state", "run_seconds"):
            self.assertNotIn(f"-f {retired_input}=", watchdog)
            self.assertNotIn(f"-f {retired_input}=", controlled)
        for retired in ("sticky_stop_30", "sticky_stop_20", "sticky_stop_10"):
            self.assertNotIn(retired, watchdog)
            self.assertNotIn(retired, controlled)
            self.assertNotIn(f"- {retired}", worker)

    def test_comparison_stop_workflows_are_shadow_only_and_state_isolated(self) -> None:
        worker = (ROOT / ".github/workflows/kalshi_btc15m_shadow_stop_experiments.yml").read_text(
            encoding="utf-8"
        )
        watchdog = (ROOT / ".github/workflows/kalshi_btc15m_shadow_stop_watchdog.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn('KALSHI_LIVE_ENABLED: "false"', worker)
        self.assertIn('KALSHI_SHADOW_ONLY: "true"', worker)
        self.assertIn("--trading-mode shadow", worker)
        self.assertIn("--dry-run", worker)
        self.assertNotIn("--live-enabled", worker)
        self.assertIn('candidate["signal_delay_seconds"] == 0', worker)
        self.assertIn('candidate["entry_timeout_seconds"] == 0', worker)
        self.assertIn('candidate["max_recovery_exponent"] == 0', worker)
        self.assertIn('runtime-state-stop-${STOP_CENTS}', worker)
        self.assertIn('kalshi_shadow_maker_hybrid_v11_sticky_stop_${STOP_CENTS}_state.json', worker)
        self.assertIn('kalshi-kxbtc15m-shadow-stop-${{ inputs.stop_cents }}', worker)
        self.assertIn('for stop_cents in 10 20 25 30 35', watchdog)
        self.assertNotIn("live_enabled", watchdog)


if __name__ == "__main__":
    unittest.main()
