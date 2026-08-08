from __future__ import annotations

import unittest
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

from optimizer import export_selected_live_strategy
from recovery_sizing import RecoverySizingState
from strategy_core import StrategyParameters, apply_realized_filled_trade, prescribed_quantity, sizing_state, zero_fill_snapshot
from kalshi_live_trader import load_config


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


if __name__ == "__main__":
    unittest.main()
