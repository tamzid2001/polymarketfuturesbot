"""Pure, fixed-point planning for the v14 opposite-side doubling ladder.

The module intentionally contains no exchange I/O.  Live and shadow execution
consume the same immutable :class:`OppositeLadderPlan`, which prevents quote
side inversion, floating-point price drift, and base-size interpretation from
diverging between adapters.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Any


CENT = Decimal("0.01")
LADDER_RUNGS: tuple[tuple[int, Decimal], ...] = (
    (40, Decimal("2.00")),
    (30, Decimal("4.00")),
    (20, Decimal("8.00")),
    (10, Decimal("16.00")),
)


def opposite_side(side: str) -> str:
    normalized = str(side).lower()
    if normalized == "yes":
        return "no"
    if normalized == "no":
        return "yes"
    raise ValueError("side must be yes or no")


def two_decimal_shares(value: Decimal | str) -> Decimal:
    result = Decimal(str(value)).quantize(CENT, rounding=ROUND_HALF_UP)
    if not result.is_finite() or result <= 0:
        raise ValueError("initial shares must be a finite positive quantity")
    return result


@dataclass(frozen=True)
class OppositeLadderOrder:
    role: str
    price_cents: int
    quantity: Decimal

    def as_dict(self) -> dict[str, Any]:
        return {
            "role": self.role,
            "price_cents": self.price_cents,
            "price": f"{Decimal(self.price_cents) / Decimal(100):.2f}",
            "quantity": f"{self.quantity:.2f}",
        }


@dataclass(frozen=True)
class OppositeLadderPlan:
    sticky_side: str
    trade_side: str
    sticky_trigger_ask_cents: int
    opposite_ask_cents: int
    base_shares: Decimal
    orders: tuple[OppositeLadderOrder, ...]

    @property
    def maximum_cash_required(self) -> Decimal:
        return sum(
            (order.quantity * Decimal(order.price_cents) / Decimal(100) for order in self.orders),
            Decimal("0"),
        ).quantize(Decimal("0.0001"))

    @property
    def maximum_quantity(self) -> Decimal:
        return sum((order.quantity for order in self.orders), Decimal("0"))

    def as_dict(self) -> dict[str, Any]:
        return {
            "sticky_side": self.sticky_side,
            "trade_side": self.trade_side,
            "sticky_trigger_ask_cents": self.sticky_trigger_ask_cents,
            "opposite_ask_cents": self.opposite_ask_cents,
            "base_shares": f"{self.base_shares:.2f}",
            "maximum_quantity": f"{self.maximum_quantity:.2f}",
            "maximum_cash_required": f"{self.maximum_cash_required:.4f}",
            "orders": [order.as_dict() for order in self.orders],
        }


def build_opposite_ladder_plan(
    *,
    sticky_side: str,
    sticky_ask_cents: int,
    opposite_ask_cents: int,
    base_shares: Decimal | str,
    trigger_min_cents: int = 53,
    trigger_max_cents: int = 57,
    initial_offset_cents: int = 1,
) -> OppositeLadderPlan:
    """Build the exact five-order plan after a delayed sticky-side trigger.

    The first order is the exact complementary bid implied by the sticky-side
    ask: ``100 - sticky_ask``.  With the reviewed 53c..57c gate this makes the
    first opposite-side order exactly 47c..43c.  ``opposite_ask_cents`` is
    retained as the independently observed executable ask used by adapters to
    prove that the derived limit remains post-only; it does not redefine the
    frozen entry tick when the spread is wider than one cent.

    Example: sticky NO/DOWN ask=53c produces a YES bid at 47c x base, then
    40c x 2*base, 30c x 4*base, 20c x 8*base, and 10c x 16*base.
    """

    sticky = str(sticky_side).lower()
    trade = opposite_side(sticky)
    sticky_ask = int(sticky_ask_cents)
    opposite_ask = int(opposite_ask_cents)
    lower = int(trigger_min_cents)
    upper = int(trigger_max_cents)
    offset = int(initial_offset_cents)
    base = two_decimal_shares(base_shares)
    if not 1 <= lower <= upper <= 99:
        raise ValueError("trigger band must be valid ordered integer-cent prices")
    if not lower <= sticky_ask <= upper:
        raise ValueError("sticky-side ask is outside the configured trigger band")
    if not 1 <= opposite_ask <= 99:
        raise ValueError("opposite-side ask must be 1 through 99 cents")
    if offset != 1:
        raise ValueError("the reviewed opposite-side contract requires a one-cent maker offset")
    initial = 100 - sticky_ask
    if not 43 <= initial <= 47:
        raise ValueError("derived opposite-side initial limit must be 43c through 47c")
    orders = [OppositeLadderOrder("initial_minus_offset", initial, base)]
    orders.extend(
        OppositeLadderOrder(f"rung_{price_cents}", price_cents, two_decimal_shares(base * multiple))
        for price_cents, multiple in LADDER_RUNGS
    )
    return OppositeLadderPlan(
        sticky_side=sticky,
        trade_side=trade,
        sticky_trigger_ask_cents=sticky_ask,
        opposite_ask_cents=opposite_ask,
        base_shares=base,
        orders=tuple(orders),
    )


def take_profit_triggered(
    trade_side_bid_cents: int | None,
    threshold_cents: int = 51,
    *,
    sticky_side_ask_cents: int | None = None,
) -> bool:
    """Return whether either executable representation reached the boundary.

    Revision 2 intentionally watches both routes into the midpoint band:
    traded-side BID >= threshold or sticky-side ASK <= threshold.  At 51c
    those are separate triggers, not complementary-price aliases.  The traded
    opposite side merely starting below 51c is deliberately not an exit.
    """

    threshold = int(threshold_cents)
    if not 1 <= threshold <= 99:
        raise ValueError("take-profit threshold must be 1 through 99 cents")
    return bool(
        (trade_side_bid_cents is not None and int(trade_side_bid_cents) >= threshold)
        or (sticky_side_ask_cents is not None and int(sticky_side_ask_cents) <= threshold)
    )
