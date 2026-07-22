"""Auditable paper-only scalp simulation for a fixed Kalshi ladder.

The simulator is deliberately isolated from the live order paths.  It mirrors
a 40c/30c/20c/10c pre-posted purchase ladder at a readable paper size.  It can
either model one fixed take-profit exit or, for the production paper audit,
observe the full later excursion and report which pre-specified exits were
actually supported by a fresh bid and sufficient displayed depth.

It is not an exchange-fill model: queue priority, latency, cancellations,
hidden liquidity, partial fills, and fees are all excluded and retained as
explicit limitations in every result.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


LADDER_LEVELS = (0.40, 0.30, 0.20, 0.10)
DEFAULT_PROFIT_TARGETS = (0.01, 0.02, 0.03, 0.05, 0.10)
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
    observation_only: bool = False,
    profit_targets_per_contract: tuple[float, ...] = DEFAULT_PROFIT_TARGETS,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return a paper ladder with either a fixed exit or a range observer."""
    normalized_side = str(side).lower()
    if normalized_side not in {"yes", "no"}:
        raise ValueError("side must be yes or no")
    if quantity_per_rung <= 0:
        raise ValueError("quantity_per_rung must be positive")
    if not 0.0 < profit_target_per_contract < 1.0:
        raise ValueError("profit_target_per_contract must be between zero and one")
    targets = tuple(sorted({round(float(target), 6) for target in profit_targets_per_contract}))
    if not targets or any(not 0.0 < target < 1.0 for target in targets):
        raise ValueError("profit_targets_per_contract must contain valid probabilities")
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
        "observation_only": bool(observation_only),
        "profit_targets_per_contract": list(targets),
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
        # Each epoch starts after a change in the filled position.  It captures
        # the maximum *executable* bid thereafter for that exact VWAP/size,
        # rather than incorrectly applying a later lower average to an earlier
        # bid that could not have closed the eventual larger position.
        "position_epochs": [],
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


def _observation_epoch(shadow: dict[str, Any], position: dict[str, float | None]) -> dict[str, Any] | None:
    """Get or start the audit segment for the currently filled VWAP/size."""
    contracts = float(position["filled_contracts"] or 0.0)
    average = position["average_entry_price"]
    if contracts <= EPSILON or average is None:
        return None
    signature = (round(contracts, 4), round(float(average), 6))
    epochs = shadow.setdefault("position_epochs", [])
    if epochs:
        previous = epochs[-1]
        if (float(previous.get("filled_contracts") or 0.0), float(previous.get("average_entry_price") or 0.0)) == signature:
            return previous
        previous["ended_at"] = now_iso()
    epoch = {
        "started_at": now_iso(),
        "filled_contracts": signature[0],
        "average_entry_price": signature[1],
        "entry_cost": round(float(position["entry_cost"] or 0.0), 6),
        "target_bids": {f"{target:.2f}": round(signature[1] + target, 6)
                        for target in shadow.get("profit_targets_per_contract", DEFAULT_PROFIT_TARGETS)},
        "max_executable_exit_bid": None,
        "max_executable_gross_per_contract": None,
        "max_executable_gross_total": None,
        "max_exit_quote": None,
        "target_hits": {},
    }
    epochs.append(epoch)
    return epoch


def _observe_executable_exit(
    shadow: dict[str, Any], position: dict[str, float | None], exit_quote: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    """Record MFE and pre-specified target hits without selecting an exit."""
    epoch = _observation_epoch(shadow, position)
    contracts = float(position["filled_contracts"] or 0.0)
    average = position["average_entry_price"]
    if epoch is None or average is None or contracts <= EPSILON or exit_quote is None:
        return []
    bid = float(exit_quote.get("economic_price") or 0.0)
    depth = float(exit_quote.get("displayed_depth") or 0.0)
    if depth + 1e-9 < contracts:
        return []
    gross_per_contract = bid - float(average)
    events: list[dict[str, Any]] = []
    prior_maximum = epoch.get("max_executable_gross_per_contract")
    if prior_maximum is None or gross_per_contract > float(prior_maximum) + 1e-9:
        epoch.update({
            "max_executable_exit_bid": round(bid, 6),
            "max_executable_gross_per_contract": round(gross_per_contract, 6),
            "max_executable_gross_total": round(gross_per_contract * contracts, 6),
            "max_exit_quote": dict(exit_quote),
            "max_recorded_at": now_iso(),
        })
        event = {
            "kind": "paper_scalp_maximum_update",
            "at": epoch["max_recorded_at"],
            "filled_contracts": round(contracts, 4),
            "average_entry_price": round(float(average), 6),
            "exit_price": round(bid, 6),
            "gross_per_contract": round(gross_per_contract, 6),
            "gross_total": round(gross_per_contract * contracts, 6),
            "exit_quote": dict(exit_quote),
        }
        _record_event(shadow, event)
        events.append(event)
    for target in shadow.get("profit_targets_per_contract", DEFAULT_PROFIT_TARGETS):
        target = float(target)
        key = f"{target:.2f}"
        if gross_per_contract + 1e-9 < target or key in epoch["target_hits"]:
            continue
        hit = {
            "at": now_iso(),
            "target_per_contract": target,
            "target_bid": round(float(average) + target, 6),
            "observed_bid": round(bid, 6),
            "observed_gross_per_contract": round(gross_per_contract, 6),
            "observed_gross_total": round(gross_per_contract * contracts, 6),
            "exit_quote": dict(exit_quote),
        }
        epoch["target_hits"][key] = hit
        event = {"kind": "paper_scalp_target_hit", "filled_contracts": round(contracts, 4),
                 "average_entry_price": round(float(average), 6), **hit}
        _record_event(shadow, event)
        events.append(event)
    return events


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
    if shadow.get("observation_only"):
        return events + _observe_executable_exit(shadow, position, exit_quote)
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
    finalized_at = now_iso()
    if shadow.get("observation_only"):
        for epoch in shadow.get("position_epochs", []):
            if isinstance(epoch, dict) and not epoch.get("ended_at"):
                epoch["ended_at"] = finalized_at
                epoch["ended_by"] = "settlement"
    contracts = float(position["filled_contracts"] or 0.0)
    cost = float(position["entry_cost"] or 0.0)
    shadow["entry_summary"] = position
    if contracts <= EPSILON:
        shadow.update({
            "status": "finalized_unfilled",
            "settled_at": finalized_at,
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
        "settled_at": finalized_at,
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
            "completed_position_states": 0,
            "scalp_exits": 0,
            "settlement_exits": 0,
            "net_profit": 0.0,
            "depth_observed_positions": 0,
            "maximum_gross_per_contract_values": [],
        }
        for price in (0.40, 0.35, 0.30, 0.25)
    }
    observation_epochs: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for shadow in shadows:
        # The observation audit reports every VWAP/size state actually held.
        # A state that is immediately superseded by another rung gets no later
        # quote attribution; it has no executable holding interval to study.
        if shadow.get("observation_only"):
            for epoch in shadow.get("position_epochs", []):
                if not isinstance(epoch, dict):
                    continue
                key = f"{float(epoch.get('average_entry_price') or 0.0):.2f}"
                profile = profiles.get(key)
                if profile is None:
                    continue
                profile["observed_positions"] += 1
                profile["active_positions"] += str(shadow.get("status")) == "active"
                profile["completed_position_states"] += str(shadow.get("status")) != "active"
                maximum = epoch.get("max_executable_gross_per_contract")
                if maximum is not None:
                    profile["depth_observed_positions"] += 1
                    profile["maximum_gross_per_contract_values"].append(float(maximum))
                observation_epochs.append((shadow, epoch))
            continue
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
    # Excursion statistics use only completed markets, preventing an active
    # position from overstating the typical maximum it could ultimately reach.
    completed_observers = {
        id(shadow) for shadow in finalized if shadow.get("observation_only")
    }
    completed_epochs = [epoch for shadow, epoch in observation_epochs if id(shadow) in completed_observers]
    maxima = [float(epoch["max_executable_gross_per_contract"])
              for epoch in completed_epochs if epoch.get("max_executable_gross_per_contract") is not None]
    observation_targets = sorted({
        round(float(target), 6)
        for shadow in shadows if shadow.get("observation_only")
        for target in shadow.get("profit_targets_per_contract", DEFAULT_PROFIT_TARGETS)
        if 0.0 < float(target) < 1.0
    }) or list(DEFAULT_PROFIT_TARGETS)
    target_summary = {}
    for target in observation_targets:
        key = f"{target:.2f}"
        eligible = [epoch for epoch in completed_epochs if epoch.get("max_executable_gross_per_contract") is not None]
        hits = [epoch for epoch in eligible if key in (epoch.get("target_hits") or {})]
        target_summary[key] = {
            "target_per_contract": target,
            "completed_position_states": len(completed_epochs),
            "depth_observed_position_states": len(eligible),
            "hit_position_states": len(hits),
            "hit_rate_given_depth_observation": round(len(hits) / len(eligible), 6) if eligible else None,
        }
    for profile in profiles.values():
        values = profile.pop("maximum_gross_per_contract_values")
        profile["median_maximum_gross_per_contract"] = _quantile(values, 0.50)
        profile["p75_maximum_gross_per_contract"] = _quantile(values, 0.75)
        profile["p90_maximum_gross_per_contract"] = _quantile(values, 0.90)
        profile["maximum_gross_per_contract"] = _quantile(values, 1.0)
        profile["average_maximum_gross_per_contract"] = round(sum(values) / len(values), 6) if values else None
    return {
        "strategy": "ladder_average_entry_scalp_executable_quote_shadow_v1",
        "mode": "paper_only_no_exchange_orders",
        "fill_rule": "buy at fresh executable ask <= rung with displayed ask depth; fixed-exit shadows close all filled rungs at their target, while observation shadows record later executable bids with displayed bid depth >= the full position",
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
        "excursion_observer": {
            "enabled": any(bool(shadow.get("observation_only")) for shadow in shadows),
            "method": "records the maximum later fresh executable bid with displayed depth for each unchanged filled VWAP/size; it does not select or simulate one exit target",
            "unit": "gross dollars per contract; fees, queue position, latency, and price impact excluded",
            "completed_position_states": len(completed_epochs),
            "depth_observed_position_states": len(maxima),
            "maximum_gross_per_contract": {
                "minimum": _quantile(maxima, 0.0),
                "p25": _quantile(maxima, 0.25),
                "median": _quantile(maxima, 0.50),
                "p75": _quantile(maxima, 0.75),
                "p90": _quantile(maxima, 0.90),
                "maximum": _quantile(maxima, 1.0),
                "average": round(sum(maxima) / len(maxima), 6) if maxima else None,
            },
            "target_opportunities": target_summary,
        },
    }


def _quantile(values: list[float], probability: float) -> float | None:
    """Small dependency-free linear-interpolated quantile helper."""
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return round(ordered[0], 6)
    position = (len(ordered) - 1) * probability
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return round(ordered[lower] + (ordered[upper] - ordered[lower]) * fraction, 6)
