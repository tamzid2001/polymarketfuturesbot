"""Exact two-decimal recovery sizing and permanent-base scaling state."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, ROUND_HALF_UP


CENT = Decimal("0.01")
ZERO = Decimal("0")
DEFAULT_MAX_POSITION = Decimal("100.00")


def decimal(value: Decimal | str | float | int) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


def round_shares(value: Decimal) -> Decimal:
    return value.quantize(CENT, rounding=ROUND_HALF_UP)


@dataclass
class RecoverySizingState:
    """The authoritative Decimal implementation used by the replay engine."""

    recovery_multiplier: Decimal
    first_base_threshold: Decimal
    base_increment: Decimal
    threshold_growth_multiplier: Decimal | None = None
    base_share_count: Decimal = Decimal("1.00")
    max_position: Decimal = DEFAULT_MAX_POSITION
    max_position_per_base_share: Decimal | None = None
    recovery_cycle_pnl: Decimal = ZERO
    recovery_exponent: int = 0
    profit_since_last_base_scale: Decimal = ZERO
    next_base_threshold: Decimal | None = None
    filled_trades_in_current_cycle: int = 0
    longest_recovery_cycle: int = 0
    max_recovery_quantity: Decimal = ZERO
    cap_hit_count: int = 0
    completed_trade_count: int = 0
    base_scale_count: int = 0
    _last_quantity_was_capped: bool = field(default=False, init=False, repr=False)

    def __post_init__(self) -> None:
        self.recovery_multiplier = decimal(self.recovery_multiplier)
        self.first_base_threshold = decimal(self.first_base_threshold)
        self.base_increment = decimal(self.base_increment)
        self.threshold_growth_multiplier = decimal(self.threshold_growth_multiplier or self.recovery_multiplier)
        self.base_share_count = round_shares(decimal(self.base_share_count))
        self.max_position = decimal(self.max_position)
        if not self.max_position.is_finite() or self.max_position <= ZERO or self.max_position != round_shares(self.max_position):
            raise ValueError("max_position must be finite, positive, and at most two decimal places")
        if self.max_position_per_base_share is not None:
            self.max_position_per_base_share = decimal(self.max_position_per_base_share)
            if (
                not self.max_position_per_base_share.is_finite()
                or self.max_position_per_base_share < Decimal("1.00")
                or self.max_position_per_base_share != round_shares(self.max_position_per_base_share)
            ):
                raise ValueError(
                    "max_position_per_base_share must be at least 1.00 and at most two decimal places"
                )
        self.recovery_cycle_pnl = decimal(self.recovery_cycle_pnl)
        self.profit_since_last_base_scale = decimal(self.profit_since_last_base_scale)
        self.next_base_threshold = decimal(self.next_base_threshold or self.first_base_threshold)
        # Snapshots are JSON-safe strings.  Convert every accounting value
        # back to Decimal on restart rather than allowing a persisted value to
        # leak into arithmetic/max comparisons.
        self.max_recovery_quantity = decimal(self.max_recovery_quantity)
        if self.base_share_count <= ZERO or self.max_position <= ZERO:
            raise ValueError("base_share_count and max_position must be positive")
        if self.recovery_multiplier <= ZERO or self.threshold_growth_multiplier <= ZERO:
            raise ValueError("multipliers must be positive")
        if self.first_base_threshold <= ZERO or self.base_increment <= ZERO:
            raise ValueError("base scaling parameters must be positive")

    @property
    def last_quantity_was_capped(self) -> bool:
        return self._last_quantity_was_capped

    def prescribed_quantity(self) -> Decimal:
        cap = (
            self.max_position
            if self.max_position_per_base_share is None
            else round_shares(self.base_share_count * self.max_position_per_base_share)
        )
        # Bound the power before quantizing. This also works for configurable
        # caps and m=1; an arbitrary exponent cutoff would not be equivalent.
        exponent = self.recovery_exponent
        if exponent < 0:
            raise ValueError("recovery_exponent cannot be negative")
        if self.recovery_multiplier >= 1:
            raw_quantity = self.base_share_count
            factor = self.recovery_multiplier
            factor_ceiling = max(Decimal("1"), (cap + CENT) / self.base_share_count)
            while exponent:
                if exponent & 1:
                    raw_quantity *= factor
                    if raw_quantity > cap + CENT:
                        raw_quantity = cap + CENT
                        break
                exponent //= 2
                if exponent:
                    factor = min(factor * factor, factor_ceiling)
        else:
            raw_quantity = self.base_share_count * (self.recovery_multiplier ** exponent)
        rounded_quantity = round_shares(raw_quantity)
        self._last_quantity_was_capped = rounded_quantity > cap
        quantity = min(rounded_quantity, cap)
        self.max_recovery_quantity = max(self.max_recovery_quantity, quantity)
        if self._last_quantity_was_capped:
            self.cap_hit_count += 1
        return quantity

    def apply_zero_fill(self) -> None:
        """A no-fill leaves both recovery and permanent-base state untouched."""

    def apply_filled_trade(self, realized_trade_pnl: Decimal | str | float | int) -> None:
        """Apply a completed filled trade in the specified order.

        A profitable trade does not reset the recovery exponent unless the
        *cumulative* recovery-cycle P/L has reached zero.  Base thresholds are
        geometric and can promote more than once after an unusually large
        filled trade.
        """

        pnl = decimal(realized_trade_pnl)
        self.completed_trade_count += 1
        self.filled_trades_in_current_cycle += 1
        self.recovery_cycle_pnl += pnl
        if self.recovery_cycle_pnl >= ZERO:
            self.recovery_cycle_pnl = ZERO
            self.recovery_exponent = 0
            self.longest_recovery_cycle = max(self.longest_recovery_cycle, self.filled_trades_in_current_cycle)
            self.filled_trades_in_current_cycle = 0
        else:
            # This advances after every completed filled trade while the
            # cumulative cycle remains negative, including an individual win.
            self.recovery_exponent += 1
            self.longest_recovery_cycle = max(self.longest_recovery_cycle, self.filled_trades_in_current_cycle)

        self.profit_since_last_base_scale += pnl
        while self.profit_since_last_base_scale >= self.next_base_threshold:
            self.profit_since_last_base_scale -= self.next_base_threshold
            self.base_share_count = round_shares(self.base_share_count + self.base_increment)
            self.next_base_threshold *= self.threshold_growth_multiplier
            self.base_scale_count += 1

    def snapshot(self) -> dict[str, str | int]:
        return {
            "base_share_count": format(self.base_share_count, "f"),
            "recovery_cycle_pnl": format(self.recovery_cycle_pnl, "f"),
            "recovery_exponent": self.recovery_exponent,
            "profit_since_last_base_scale": format(self.profit_since_last_base_scale, "f"),
            "next_base_threshold": format(self.next_base_threshold, "f"),
            "max_recovery_quantity": format(self.max_recovery_quantity, "f"),
            "cap_hit_count": self.cap_hit_count,
            "longest_recovery_cycle": self.longest_recovery_cycle,
        }
