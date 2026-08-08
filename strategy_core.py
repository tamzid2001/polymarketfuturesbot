"""Shared, deterministic strategy state transitions for replay and live use.

There is deliberately no Kalshi API code in this module.  A completed live
trade and a completed historical-replay trade call the same Decimal sizing and
recovery transition implementation.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from recovery_sizing import CENT, DEFAULT_MAX_POSITION, ZERO, RecoverySizingState, decimal, round_shares


@dataclass(frozen=True)
class StrategyParameters:
    """The shared interpretation of optimizer and live configuration fields."""

    recovery_multiplier: Decimal
    first_base_threshold: Decimal
    threshold_growth_multiplier: Decimal
    base_increment: Decimal
    starting_base: Decimal = Decimal("1.00")
    max_position: Decimal = DEFAULT_MAX_POSITION

    def __post_init__(self) -> None:
        for name in (
            "recovery_multiplier", "first_base_threshold", "threshold_growth_multiplier",
            "base_increment", "starting_base", "max_position",
        ):
            object.__setattr__(self, name, decimal(getattr(self, name)))
        if self.starting_base != round_shares(self.starting_base):
            raise ValueError("starting_base must have at most two decimal places")

    def as_dict(self) -> dict[str, str]:
        return {
            "recovery_multiplier": format(self.recovery_multiplier, "f"),
            "first_base_threshold": format(self.first_base_threshold, "f"),
            "threshold_growth_multiplier": format(self.threshold_growth_multiplier, "f"),
            "base_increment": format(self.base_increment, "f"),
            "starting_base": format(self.starting_base, "f"),
            "max_position": format(self.max_position, "f"),
        }


def sizing_state(parameters: StrategyParameters, snapshot: dict[str, Any] | None = None) -> RecoverySizingState:
    """Rehydrate the one authoritative sizing state from a JSON-safe snapshot."""

    snapshot = snapshot or {}
    return RecoverySizingState(
        recovery_multiplier=parameters.recovery_multiplier,
        first_base_threshold=parameters.first_base_threshold,
        base_increment=parameters.base_increment,
        threshold_growth_multiplier=parameters.threshold_growth_multiplier,
        base_share_count=snapshot.get("base_share_count", parameters.starting_base),
        max_position=parameters.max_position,
        recovery_cycle_pnl=snapshot.get("recovery_cycle_pnl", ZERO),
        recovery_exponent=int(snapshot.get("recovery_exponent", 0)),
        profit_since_last_base_scale=snapshot.get("profit_since_last_base_scale", ZERO),
        next_base_threshold=snapshot.get("next_base_threshold", parameters.first_base_threshold),
        filled_trades_in_current_cycle=int(snapshot.get("filled_trades_in_current_cycle", 0)),
        longest_recovery_cycle=int(snapshot.get("longest_recovery_cycle", 0)),
        max_recovery_quantity=snapshot.get("max_recovery_quantity", ZERO),
        cap_hit_count=int(snapshot.get("cap_hit_count", 0)),
        completed_trade_count=int(snapshot.get("completed_trade_count", 0)),
        base_scale_count=int(snapshot.get("base_scale_count", 0)),
    )


def full_snapshot(state: RecoverySizingState) -> dict[str, str | int]:
    """Persist all state needed to resume without changing future sizing."""

    snapshot = state.snapshot()
    snapshot.update({
        "filled_trades_in_current_cycle": state.filled_trades_in_current_cycle,
        "completed_trade_count": state.completed_trade_count,
        "base_scale_count": state.base_scale_count,
    })
    return snapshot


def prescribed_quantity(parameters: StrategyParameters, snapshot: dict[str, Any] | None = None) -> tuple[Decimal, bool]:
    state = sizing_state(parameters, snapshot)
    quantity = state.prescribed_quantity()
    return quantity, state.last_quantity_was_capped


def apply_realized_filled_trade(
    parameters: StrategyParameters, snapshot: dict[str, Any] | None, realized_net_pnl: Decimal | str,
) -> tuple[dict[str, str | int], dict[str, bool]]:
    """Apply one real filled-trade P&L exactly once at a higher-level ledger key."""

    state = sizing_state(parameters, snapshot)
    base_before = state.base_share_count
    exponent_before = state.recovery_exponent
    state.apply_filled_trade(decimal(realized_net_pnl))
    return full_snapshot(state), {
        "recovery_reset": exponent_before > 0 and state.recovery_exponent == 0,
        "base_increased": state.base_share_count > base_before,
    }


def zero_fill_snapshot(parameters: StrategyParameters, snapshot: dict[str, Any] | None) -> dict[str, str | int]:
    """Return the unmodified state, explicitly documenting zero-fill semantics."""

    state = sizing_state(parameters, snapshot)
    state.apply_zero_fill()
    return full_snapshot(state)
