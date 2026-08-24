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
        self.assertEqual(config["entry_execution_mode"], "immediate_market_ioc")
        self.assertEqual(config["stop_policy"], "fixed_profile_floor")
        self.assertEqual(config["stop_baseline_entry_price"], "0.50")
        self.assertEqual(config["strategy_version"], ACTIVE_STRATEGY_VERSION)
        self.assertEqual(config["config_schema_version"], ACTIVE_CONFIG_SCHEMA_VERSION)

    def test_optimizer_export_preserves_the_selected_stop_profile(self) -> None:
        row = {
            "entry_price": .49, "stop_price": .20, "recovery_multiplier": 1.01,
            "first_base_threshold": 350, "threshold_growth_multiplier": 1.01, "base_increment": .50,
        }
        with TemporaryDirectory() as directory:
            path = Path(directory) / "selected_live_strategy.json"
            config = export_selected_live_strategy(path, row, selection_basis="test")
            self.assertEqual(load_config(path)["shadow_profile"], "sticky_stop_20")
        self.assertEqual(config["stop_price"], "0.20")

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

    def test_shadow_stop_profiles_cannot_silently_reinterpret_a_stop(self) -> None:
        config = load_config(ROOT / "selected_live_strategy.json")
        tested = load_config_from_value(dict(config, shadow_profile="sticky_stop_30", stop_price="0.30"))
        self.assertEqual(tested["shadow_profile"], "sticky_stop_30")
        self.assertEqual(tested["stop_price"], "0.30")
        with self.assertRaisesRegex(ValueError, "requires stop_price"):
            load_config_from_value(dict(config, shadow_profile="sticky_stop_30"))


if __name__ == "__main__":
    unittest.main()
