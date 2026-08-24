"""Shared, deterministic strategy state transitions for replay and live use.

There is deliberately no Kalshi API code in this module.  A completed live
trade and a completed historical-replay trade call the same Decimal sizing and
recovery transition implementation.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_CEILING, Decimal
from typing import Any

from recovery_sizing import CENT, DEFAULT_MAX_POSITION, ZERO, RecoverySizingState, decimal, round_shares


def sticky_directional_prediction(
    prior_prediction_side: str | None,
    settled_outcome: str,
) -> tuple[str, str]:
    """Advance the shadow-only sticky directional state deterministically.

    A fresh sequence is seeded contrarian to the just-settled market.  Once a
    side is active, a settlement *against* that side keeps it for the next
    market; a settlement *on* that side flips it.  This is directional-state
    logic only: fills, stops, zero fills, and P&L never affect the side.
    """

    outcome = str(settled_outcome).lower()
    if outcome not in {"yes", "no"}:
        raise ValueError("settled_outcome must be yes or no")
    prior = None if prior_prediction_side is None else str(prior_prediction_side).lower()
    if prior is None:
        return ("no" if outcome == "yes" else "yes"), "seed_inverse_settlement"
    if prior not in {"yes", "no"}:
        raise ValueError("prior_prediction_side must be yes, no, or None")
    if outcome == prior:
        return ("no" if prior == "yes" else "yes"), "flip_after_directional_win"
    return prior, "hold_after_directional_loss"


def effective_stop_price(
    actual_entry_price: Decimal | str,
    stop_floor_price: Decimal | str,
    stop_baseline_entry_price: Decimal | str = Decimal("0.50"),
) -> Decimal:
    """Return the asymmetric live stop used by replay-compatible live state.

    Entries at or below the 50-cent baseline retain the fixed 40-cent floor.
    Above the baseline, the stop rises one-for-one with the actual average
    entry price: 52 cents -> 42 cents.  Rounding is upward to the exchange
    cent so the intended gross loss is never larger merely because a partial
    fill produced a fractional-cent average.
    """

    entry = decimal(actual_entry_price)
    floor = decimal(stop_floor_price)
    baseline = decimal(stop_baseline_entry_price)
    if not ZERO < floor < baseline < Decimal("1"):
        raise ValueError("stop floor and baseline must satisfy 0 < floor < baseline < 1")
    if not floor < entry < Decimal("1"):
        raise ValueError("actual entry price must be above the fixed stop floor")
    adjustment = max(ZERO, entry - baseline)
    return (floor + adjustment).quantize(CENT, rounding=ROUND_CEILING)


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
