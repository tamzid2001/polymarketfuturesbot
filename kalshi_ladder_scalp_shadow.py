"""Auditable paper-only scalp simulation for a fixed Kalshi ladder.

The simulator is deliberately isolated from the live order paths.  It mirrors
a 40c/30c/20c/10c pre-posted purchase ladder at a readable paper size, then
closes the *entire* hypothetical position only when a fresh, depth-supported
best bid is at least one configurable cent above its volume-weighted entry.

It is not an exchange-fill model: queue priority, latency, cancellations,
hidden liquidity, partial fills, and fees are all excluded and retained as
explicit limitations in every result.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


LADDER_LEVELS = (0.40, 0.30, 0.20, 0.10)
EPSILON = 0.004


def now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def new_ladder_average_entry_scalp_shadow(
    *,
    strategy: str,
    ticker: str,
    side: str,
    quantity_per_rung: float,
    profit_target_per_contract: float,
    quote_max_age_seconds: float,
    market_close_time: Any,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return a one-round-trip paper ladder with a VWAP take-profit exit."""
    normalized_side = str(side).lower()
    if normalized_side not in {"yes", "no"}:
        raise ValueError("side must be yes or no")
    if quantity_per_rung <= 0:
        raise ValueError("quantity_per_rung must be positive")
    if not 0.0 < profit_target_per_contract < 1.0:
        raise ValueError("profit_target_per_contract must be between zero and one")
    return {
        "strategy": strategy,
        "mode": "paper_only_no_exchange_orders",
        "status": "active",
        "ticker": ticker,
        "side": normalized_side,
        "created_at": now_iso(),
        "market_close_time": market_close_time,
        "quantity_per_rung": round(float(quantity_per_rung), 4),
        "profit_target_per_contract": round(float(profit_target_per_contract), 4),
        "quote_max_age_seconds": round(float(quote_max_age_seconds), 4),
        "rungs": {
            f"{level:.4f}": {
                "rung_price": level,
                "quantity": round(float(quantity_per_rung), 4),
                "fill_count": 0.0,
                "average_fill_price": None,
                "status": "simulated_resting",
            }
            for level in LADDER_LEVELS
        },
        "entry_quote_depth_consumed": {},
        "events": [],
        "limitation": (
            "Paper-only. Entry and exit require a fresh complete top-of-book and displayed depth, "
            "but queue priority, latency, cancellations, hidden liquidity, partial fills, and fees are excluded."
        ),
        **(extra or {}),
    }


def entry_summary(shadow: dict[str, Any]) -> dict[str, float | None]:
    """Calculate the current paper position from filled fixed-price rungs."""
    contracts = cost = 0.0
    rungs = shadow.get("rungs") if isinstance(shadow.get("rungs"), dict) else {}
    for rung in rungs.values():
        if not isinstance(rung, dict):
            continue
        count = float(rung.get("fill_count") or 0.0)
        if count <= EPSILON:
            continue
        price = float(rung.get("average_fill_price") or rung.get("rung_price") or 0.0)
        contracts += count
        cost += count * price
    average = cost / contracts if contracts > EPSILON else None
    target = average + float(shadow.get("profit_target_per_contract") or 0.0) if average is not None else None
    return {
        "filled_contracts": round(contracts, 4),
        "entry_cost": round(cost, 6),
        "average_entry_price": round(average, 6) if average is not None else None,
        "take_profit_bid": round(target, 6) if target is not None else None,
    }


def _record_event(shadow: dict[str, Any], event: dict[str, Any]) -> None:
    shadow.setdefault("events", []).append(event)


def simulate_ladder_average_entry_scalp(
    shadow: dict[str, Any],
    *,
    entry_quote: dict[str, Any] | None,
    entry_quote_state: str,
    exit_quote: dict[str, Any] | None,
    exit_quote_state: str,
) -> list[dict[str, Any]]:
    """Advance one isolated paper ladder using fresh entry and exit evidence.

    ``entry_quote`` must represent a side's executable *ask* and its displayed
    ask depth.  ``exit_quote`` must represent that same side's executable
    *bid* and its displayed bid depth.  Callers are responsible for freshness
    and complete-book validation before providing either quote.
    """
    if shadow.get("status") != "active":
        return []
    events: list[dict[str, Any]] = []
    shadow["last_entry_quote_state"] = entry_quote_state
    shadow["last_exit_quote_state"] = exit_quote_state
    rungs = shadow.get("rungs") if isinstance(shadow.get("rungs"), dict) else {}

    if entry_quote is not None:
        quote_id = str(entry_quote.get("quote_id") or "")
        depth = float(entry_quote.get("displayed_depth") or 0.0)
        consumed = float((shadow.setdefault("entry_quote_depth_consumed", {})).get(quote_id) or 0.0)
        available = max(0.0, depth - consumed)
        ask = float(entry_quote.get("economic_price") or 0.0)
        for level in LADDER_LEVELS:
            rung = rungs.get(f"{level:.4f}")
            if not isinstance(rung, dict) or float(rung.get("fill_count") or 0.0) > EPSILON:
                continue
            quantity = float(rung.get("quantity") or shadow.get("quantity_per_rung") or 0.0)
            if ask > level + 1e-9 or available + 1e-9 < quantity:
                continue
            rung.update({
                "fill_count": round(quantity, 4),
                "average_fill_price": round(level, 4),
                "status": "simulated_executable_quote_hit",
                "filled_at": now_iso(),
                "simulation_entry_quote": dict(entry_quote),
            })
            consumed += quantity
            available -= quantity
            shadow["entry_quote_depth_consumed"][quote_id] = round(consumed, 4)
            event = {
                "kind": "paper_scalp_entry_rung_hit",
                "at": now_iso(),
                "rung_price": level,
                "quantity": round(quantity, 4),
                "entry_quote": dict(entry_quote),
            }
            _record_event(shadow, event)
            events.append(event)

    position = entry_summary(shadow)
    shadow["entry_summary"] = position
    contracts = float(position["filled_contracts"] or 0.0)
    target_bid = position["take_profit_bid"]
    if contracts <= EPSILON or target_bid is None or exit_quote is None:
        return events
    bid = float(exit_quote.get("economic_price") or 0.0)
    depth = float(exit_quote.get("displayed_depth") or 0.0)
    if bid + 1e-9 < float(target_bid) or depth + 1e-9 < contracts:
        return events

    entry_cost = float(position["entry_cost"] or 0.0)
    proceeds = bid * contracts
    gross = proceeds - entry_cost
    # Mark any unfilled GTC rungs as canceled only inside this alternate paper
    # scenario.  No exchange order is created, canceled, or otherwise touched.
    for rung in rungs.values():
        if isinstance(rung, dict) and float(rung.get("fill_count") or 0.0) <= EPSILON:
            rung["status"] = "paper_canceled_after_scalp_exit"
            rung["paper_canceled_at"] = now_iso()
    exit_event = {
        "kind": "paper_scalp_take_profit_exit",
        "at": now_iso(),
        "exit_quote": dict(exit_quote),
        "filled_contracts": round(contracts, 4),
        "average_entry_price": position["average_entry_price"],
        "take_profit_bid": target_bid,
        "exit_price": round(bid, 6),
        "entry_cost": round(entry_cost, 6),
        "exit_proceeds": round(proceeds, 6),
        "gross_profit_loss": round(gross, 6),
        "fees_model": "excluded_no_exchange_fill",
    }
    shadow.update({
        "status": "scalp_exited",
        "scalp_exit": exit_event,
        "settled_at": exit_event["at"],
        "contracts": round(contracts, 4),
        "total_cost": round(entry_cost, 6),
        "average_entry": position["average_entry_price"],
        "gross_payout": round(proceeds, 6),
        "gross_profit_loss": round(gross, 6),
        "net_profit_loss": round(gross, 6),
        "estimated_fees": 0.0,
        "fees_model": "excluded_no_exchange_fill",
        "return_percentage": round(100.0 * gross / entry_cost, 4) if entry_cost > 0 else None,
        "exit_method": "fresh_executable_bid_take_profit",
    })
    _record_event(shadow, exit_event)
    events.append(exit_event)
    return events


def finalize_ladder_average_entry_scalp(shadow: dict[str, Any], result: str | None) -> bool:
    """Settle a still-open paper scalp position at the binary contract value."""
    if shadow.get("status") != "active":
        return False
    outcome = str(result or "").lower()
    if outcome not in {"yes", "no"}:
        return False
    position = entry_summary(shadow)
    contracts = float(position["filled_contracts"] or 0.0)
    cost = float(position["entry_cost"] or 0.0)
    shadow["entry_summary"] = position
    if contracts <= EPSILON:
        shadow.update({
            "status": "finalized_unfilled",
            "settled_at": now_iso(),
            "settlement_outcome": outcome,
            "contracts": 0.0,
            "total_cost": 0.0,
            "net_profit_loss": 0.0,
            "exit_method": "market_closed_without_paper_entry",
        })
        return True
    payout = contracts if str(shadow.get("side") or "").lower() == outcome else 0.0
    gross = payout - cost
    shadow.update({
        "status": "finalized_settlement",
        "settled_at": now_iso(),
        "settlement_outcome": outcome,
        "contracts": round(contracts, 4),
        "total_cost": round(cost, 6),
        "average_entry": position["average_entry_price"],
        "gross_payout": round(payout, 6),
        "gross_profit_loss": round(gross, 6),
        "net_profit_loss": round(gross, 6),
        "estimated_fees": 0.0,
        "fees_model": "excluded_no_exchange_fill",
        "return_percentage": round(100.0 * gross / cost, 4) if cost > 0 else None,
        "exit_method": "settlement_no_take_profit_exit",
    })
    _record_event(shadow, {
        "kind": "paper_scalp_settlement",
        "at": shadow["settled_at"],
        "settlement_outcome": outcome,
        "filled_contracts": round(contracts, 4),
        "entry_cost": round(cost, 6),
        "settlement_payout": round(payout, 6),
        "gross_profit_loss": round(gross, 6),
    })
    return True


def scalp_performance(shadows: list[dict[str, Any]]) -> dict[str, Any]:
    """Return a compact, cash-flow-correct report across paper scalp shadows."""
    finalized = [
        shadow for shadow in shadows
        if str(shadow.get("status")) in {"scalp_exited", "finalized_settlement", "finalized_unfilled"}
    ]
    filled = [shadow for shadow in finalized if float(shadow.get("contracts") or 0.0) > EPSILON]
    pnls = [float(shadow.get("net_profit_loss") or 0.0) for shadow in filled]
    costs = [float(shadow.get("total_cost") or 0.0) for shadow in filled]
    equity = peak = drawdown = 0.0
    for value in pnls:
        equity += value
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    wins = sum(value > 1e-9 for value in pnls)
    losses = sum(value < -1e-9 for value in pnls)
    scalps = [shadow for shadow in filled if shadow.get("exit_method") == "fresh_executable_bid_take_profit"]
    settlements = [shadow for shadow in filled if shadow.get("exit_method") == "settlement_no_take_profit_exit"]
    profiles = {
        f"{price:.2f}": {
            "average_entry_price": price,
            "observed_positions": 0,
            "active_positions": 0,
            "scalp_exits": 0,
            "settlement_exits": 0,
            "net_profit": 0.0,
        }
        for price in (0.40, 0.35, 0.30, 0.25)
    }
    for shadow in shadows:
        position = entry_summary(shadow)
        average = position["average_entry_price"]
        if average is None:
            continue
        key = f"{float(average):.2f}"
        profile = profiles.get(key)
        if profile is None:
            continue
        profile["observed_positions"] += 1
        profile["active_positions"] += str(shadow.get("status")) == "active"
        profile["scalp_exits"] += shadow.get("exit_method") == "fresh_executable_bid_take_profit"
        profile["settlement_exits"] += shadow.get("exit_method") == "settlement_no_take_profit_exit"
        profile["net_profit"] += float(shadow.get("net_profit_loss") or 0.0)
    for profile in profiles.values():
        profile["net_profit"] = round(profile["net_profit"], 6)
    return {
        "strategy": "ladder_average_entry_scalp_executable_quote_shadow_v1",
        "mode": "paper_only_no_exchange_orders",
        "fill_rule": "buy at fresh executable ask <= rung with displayed ask depth; close all filled rungs only at fresh executable bid >= average entry + target with displayed bid depth >= full position",
        "fee_treatment": "excluded_no_exchange_fill",
        "limitations": [
            "A quote hit or exit is a paper event, not a Kalshi exchange fill.",
            "Queue priority, quote cancellation, latency, hidden liquidity, partial fills, and fees are not modeled.",
            "The full simulated position is closed at the observed bid; no favorable price improvement is assumed.",
        ],
        "paper_markets_started": len(shadows),
        "active_paper_markets": sum(str(shadow.get("status")) == "active" for shadow in shadows),
        "scalp_exits": len(scalps),
        "settlement_exits_without_take_profit": len(settlements),
        "unfilled_markets": sum(str(shadow.get("status")) == "finalized_unfilled" for shadow in shadows),
        "filled_market_trades": len(filled),
        "winning_trades": wins,
        "losing_trades": losses,
        "win_rate": round(wins / len(filled), 6) if filled else None,
        "total_simulated_contracts": round(sum(float(shadow.get("contracts") or 0.0) for shadow in filled), 4),
        "total_entry_cost": round(sum(costs), 6),
        "total_exit_proceeds": round(sum(float(shadow.get("gross_payout") or 0.0) for shadow in filled), 6),
        "net_profit": round(sum(pnls), 6),
        "return_on_simulated_capital": round(sum(pnls) / sum(costs), 6) if sum(costs) else None,
        "maximum_drawdown": round(drawdown, 6),
        "average_entry_profiles": profiles,
    }
