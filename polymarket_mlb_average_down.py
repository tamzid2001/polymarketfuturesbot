"""Mechanical Polymarket US MLB full-game moneyline average-down bot.

The runner takes no view on baseball.  For each MLB full-game moneyline that
starts today (America/New_York), it snapshots the first executable cost of the
home and away outcomes.  It waits until either outcome can be bought at least
``price_step`` below that snapshot, buys the first triggered outcome with a
protected IOC limit order, locks that outcome, and posts lower GTC limits at
each further ``price_step``.  It never hedges, flips sides, uses ML, or opens a
position after the game has begun.

Live submission is deliberately opt-in: DRY_RUN must be false and both
--submit and --allow-live must be present. The supplied GitHub workflow runs a
24/7 dry-monitoring handoff chain; only an explicit manual dispatch can submit
live orders, and that live run does not self-handoff.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

try:  # Allows the pure helper tests to run without the live SDK installed.
    from polymarket_us import AsyncPolymarketUS
except ImportError:  # pragma: no cover - only for minimal local environments
    AsyncPolymarketUS = None


LOG = logging.getLogger("polymarket_mlb_average_down")
GATEWAY_BASE_URL = "https://gateway.polymarket.us"
LEAGUE = "mlb"
ET = ZoneInfo("America/New_York")
CONFIG_VERSION = 2
STATE_VERSION = 1

DEFAULT_CONFIG: dict[str, Any] = {
    "format_version": CONFIG_VERSION,
    # Number of contracts on every initial/lower rung; never dollars.
    "initial_position_size": 1,
    "price_step": 0.10,
    "max_active_games": 15,
    "max_contracts_per_game": 10,
    "max_total_capital": 50.00,
    "poll_seconds": 20.0,
    "status_log_seconds": 60.0,
    # The historical ML pipeline is opt-in. Existing monitoring remains the
    # original price-only mechanical strategy unless this is explicitly set.
    "strategy_mode": "mechanical",
    "ml_model_path": "",
    "ml_min_confidence": 0.50,
}


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def as_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def field(obj: Any, *names: str) -> Any:
    for name in names:
        if isinstance(obj, dict) and obj.get(name) is not None:
            return obj[name]
        value = getattr(obj, name, None)
        if value is not None:
            return value
    return None


def as_bool(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes"}
    return bool(value)


def parse_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def price_amount(value: Any) -> float | None:
    """Read the SDK Amount object/dict and reject non-contract prices."""
    if isinstance(value, dict):
        value = value.get("value")
    number = as_float(value)
    return number if number is not None and 0.0 < number < 1.0 else None


def decimal_floor(value: float, tick_size: float) -> float | None:
    if value <= 0 or tick_size <= 0:
        return None
    quotient = (Decimal(str(value)) / Decimal(str(tick_size))).to_integral_value(rounding=ROUND_DOWN)
    result = quotient * Decimal(str(tick_size))
    return float(result) if result > 0 else None


def api_price_for_outcome(outcome: str, outcome_cost: float) -> float:
    """Polymarket price is always LONG/YES price, including BUY_SHORT orders."""
    if outcome == "long":
        return round(outcome_cost, 8)
    if outcome == "short":
        return round(1.0 - outcome_cost, 8)
    raise ValueError(f"Unsupported outcome: {outcome}")


def executable_outcome_asks(bbo: Any) -> dict[str, float | None]:
    """LONG ask is direct; SHORT ask is 1 - LONG bid for the binary market."""
    # The public SDK currently wraps the BBO payload in ``marketData`` while
    # the documented inner object exposes bestAsk/bestBid.
    payload = field(bbo, "marketData", "market_data") or bbo
    long_ask = price_amount(field(payload, "bestAsk", "best_ask"))
    long_bid = price_amount(field(payload, "bestBid", "best_bid"))
    short_ask = round(1.0 - long_bid, 8) if long_bid is not None else None
    return {"long": long_ask, "short": short_ask}


def threshold_from_baseline(baseline: float, step: float, tick_size: float) -> float | None:
    """The first permitted buy: exactly one configured step below the snapshot."""
    return decimal_floor(baseline - step, tick_size)


def lower_levels(first_fill: float, step: float, tick_size: float, max_orders: int) -> list[float]:
    """Generate strictly lower outcome costs; never average up after price improvement."""
    levels: list[float] = []
    first = Decimal(str(first_fill))
    increment = Decimal(str(step))
    for index in range(1, max_orders):
        # Do the subtraction as Decimal before tick alignment.  Binary float
        # arithmetic turns e.g. 0.67 - 0.60 into 0.069999..., which would
        # incorrectly floor a 7c rung to 6c.
        level = decimal_floor(float(first - increment * index), tick_size)
        if level is None or level >= first_fill - 1e-9:
            continue
        if level not in levels:
            levels.append(level)
    return levels


def choose_first_trigger(
    outcomes: dict[str, dict[str, Any]], asks: dict[str, float | None], step: float, permitted_outcome: str | None = None,
) -> tuple[str, float, float] | None:
    """Select a triggered home/away outcome.  Exact simultaneous ties prefer home."""
    candidates: list[tuple[float, int, str, float, float]] = []
    for outcome, metadata in outcomes.items():
        if permitted_outcome is not None and outcome != permitted_outcome:
            continue
        baseline = as_float(metadata.get("initial_ask"))
        target = as_float(metadata.get("entry_target"))
        ask = asks.get(outcome)
        if baseline is None or target is None or ask is None or ask > target + 1e-9:
            continue
        # First observed poll wins.  If both breach in that same poll, choose
        # the larger breach, then home before away for a reproducible audit.
        role_rank = 0 if metadata.get("role") == "home" else 1
        candidates.append((target - ask, role_rank, outcome, ask, target))
    if not candidates:
        return None
    _breach, _role_rank, outcome, ask, target = min(candidates, key=lambda item: (-item[0], item[1], item[2]))
    return outcome, ask, target


def full_game_moneyline(market: dict[str, Any]) -> bool:
    """Exclude F5, spreads, totals, props, and futures by exact sports type."""
    # The public feed currently emits ``..._winner`` for the full-game
    # moneyline; older responses/documentation use ``..._moneyline``.
    return str(market.get("sportsMarketType") or "") in {
        "baseball_team_full_game_winner",
        "baseball_team_full_game_moneyline",
    }


def side_name(side: dict[str, Any]) -> str | None:
    team = side.get("team") if isinstance(side.get("team"), dict) else {}
    for value in (team.get("name"), side.get("displayName"), side.get("name"), side.get("title")):
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


@dataclass(frozen=True)
class GameMarket:
    event_ticker: str
    event_title: str
    market_slug: str
    game_start: datetime
    tick_size: float
    minimum_trade_qty: int
    outcomes: dict[str, dict[str, Any]]


def discover_games(payload: dict[str, Any], now: datetime) -> list[GameMarket]:
    """Return only pre-game MLB full-game moneylines that start on today's ET date."""
    games: list[GameMarket] = []
    today = now.astimezone(ET).date()
    for event in payload.get("events", []) if isinstance(payload.get("events"), list) else []:
        if not isinstance(event, dict):
            continue
        for market in event.get("markets", []) if isinstance(event.get("markets"), list) else []:
            if not isinstance(market, dict) or not full_game_moneyline(market):
                continue
            if market.get("active") is False or market.get("closed") is True:
                continue
            start = parse_timestamp(market.get("gameStartTime") or event.get("startTime"))
            tick_size = as_float(market.get("orderPriceMinTickSize"))
            min_qty = as_float(market.get("minimumTradeQty"))
            slug = str(market.get("slug") or "")
            if not start or start <= now or start.astimezone(ET).date() != today or not tick_size or not slug:
                continue
            sides = market.get("marketSides")
            if not isinstance(sides, list) or len(sides) != 2:
                continue
            outcomes: dict[str, dict[str, Any]] = {}
            for side in sides:
                if not isinstance(side, dict):
                    continue
                outcome = "long" if as_bool(side.get("long")) else "short"
                team = side.get("team") if isinstance(side.get("team"), dict) else {}
                role = str(side.get("ordering") or team.get("ordering") or "").lower()
                name = side_name(side)
                if outcome in outcomes or role not in {"home", "away"} or not name:
                    outcomes = {}
                    break
                outcomes[outcome] = {"role": role, "team": name}
            if set(outcomes) != {"long", "short"}:
                continue
            games.append(GameMarket(
                event_ticker=str(event.get("ticker") or event.get("slug") or ""),
                event_title=str(event.get("title") or ""), market_slug=slug, game_start=start,
                tick_size=tick_size, minimum_trade_qty=max(1, math.ceil(min_qty or 1)), outcomes=outcomes,
            ))
    return sorted(games, key=lambda game: game.game_start)


def default_state() -> dict[str, Any]:
    return {"format_version": STATE_VERSION, "games": {}}


def load_json(path: Path, default: dict[str, Any]) -> dict[str, Any]:
    if not path.exists():
        return dict(default)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        LOG.warning("Cannot read %s; using defaults.", path)
        return dict(default)
    return value if isinstance(value, dict) else dict(default)


def save_json(path: Path, payload: dict[str, Any]) -> None:
    """Atomic state save: an accepted external order must not be memory-only."""
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    temporary.replace(path)


def validate_config(config: dict[str, Any]) -> dict[str, Any]:
    merged = {**DEFAULT_CONFIG, **config, "format_version": CONFIG_VERSION}
    for key in ("initial_position_size", "max_active_games", "max_contracts_per_game"):
        value = as_float(merged.get(key))
        if value is None or value < 1 or int(value) != value:
            raise ValueError(f"{key} must be a positive whole number")
        merged[key] = int(value)
    for key in ("price_step", "max_total_capital", "poll_seconds", "status_log_seconds"):
        value = as_float(merged.get(key))
        if value is None or value <= 0:
            raise ValueError(f"{key} must be positive")
        merged[key] = value
    if merged["price_step"] >= 1:
        raise ValueError("price_step must be less than $1")
    if merged["max_contracts_per_game"] < merged["initial_position_size"]:
        raise ValueError("max_contracts_per_game must fund the initial contract quantity")
    merged["strategy_mode"] = str(merged.get("strategy_mode") or "mechanical")
    if merged["strategy_mode"] not in {"mechanical", "ml_side_average_down"}:
        raise ValueError("strategy_mode must be mechanical or ml_side_average_down")
    merged["ml_model_path"] = str(merged.get("ml_model_path") or "")
    confidence = as_float(merged.get("ml_min_confidence"))
    if confidence is None or not .5 <= confidence <= 1:
        raise ValueError("ml_min_confidence must be between 0.50 and 1.00")
    merged["ml_min_confidence"] = confidence
    return merged


def apply_config_overrides(config: dict[str, Any], args: argparse.Namespace) -> tuple[dict[str, Any], bool]:
    changed = False
    updated = dict(config)
    for key in (
        "initial_position_size", "max_active_games", "max_contracts_per_game",
        "max_total_capital", "price_step", "poll_seconds", "status_log_seconds",
        "strategy_mode", "ml_model_path", "ml_min_confidence",
    ):
        value = getattr(args, key, None)
        if value is not None:
            updated[key] = value
            changed = True
    return validate_config(updated), changed


def game_record(state: dict[str, Any], game: GameMarket) -> dict[str, Any]:
    records = state.setdefault("games", {})
    record = records.get(game.market_slug)
    if isinstance(record, dict):
        return record
    record = {
        "market_slug": game.market_slug,
        "event_ticker": game.event_ticker,
        "event_title": game.event_title,
        "game_start": game.game_start.isoformat(),
        "tick_size": game.tick_size,
        "minimum_trade_qty": game.minimum_trade_qty,
        "status": "watching_baseline",
        "orders": {},
        "created_at": now_iso(),
    }
    records[game.market_slug] = record
    return record


def orders_for_game(record: dict[str, Any]) -> list[dict[str, Any]]:
    return [item for item in (record.get("orders") or {}).values() if isinstance(item, dict)]


def order_fill_count(order: dict[str, Any]) -> int:
    return max(0, int(as_float(order.get("filled_quantity")) or 0))


def order_quantity(order: dict[str, Any]) -> int:
    return max(0, int(as_float(order.get("quantity")) or 0))


def filled_contracts(record: dict[str, Any]) -> int:
    return sum(order_fill_count(order) for order in orders_for_game(record))


def game_started(record: dict[str, Any], now: datetime) -> bool:
    start = parse_timestamp(record.get("game_start"))
    return start is None or now >= start


def active_position_records(state: dict[str, Any], now: datetime) -> list[dict[str, Any]]:
    return [record for record in state.get("games", {}).values()
            if isinstance(record, dict) and record.get("status") in {"ladder_active", "game_started"}]


def reserved_capital(state: dict[str, Any], now: datetime) -> float:
    total = 0.0
    for record in active_position_records(state, now):
        for order in orders_for_game(record):
            filled = order_fill_count(order)
            # Keep filled cost reserved even after game start/ladder cancel:
            # that capital is still exposed until the contract settles.
            outstanding = int(as_float(order.get("remaining_quantity")) or 0)
            if order.get("status") in {"canceled", "canceled_unfilled", "rejected", "submission_unknown"}:
                outstanding = 0
            total += float(order.get("average_outcome_cost") or order.get("outcome_cost") or 0.0) * filled
            total += float(order.get("outcome_cost") or 0.0) * outstanding
    return round(total, 8)


def planned_ladder_cost(entry_cost: float, quantity: int, step: float, tick_size: float, max_contracts: int) -> float:
    order_count = max_contracts // quantity
    return round(sum([entry_cost, *lower_levels(entry_cost, step, tick_size, order_count)]) * quantity, 8)


def order_snapshot(order: dict[str, Any], fallback_cost: float, outcome: str) -> dict[str, Any]:
    quantity = int(as_float(order.get("quantity")) or 0)
    filled = int(as_float(order.get("cumQuantity")) or 0)
    leaves = int(as_float(order.get("leavesQuantity")) or max(0, quantity - filled))
    average_long_price = price_amount(order.get("avgPx"))
    if average_long_price is None:
        average = fallback_cost
    elif outcome == "long":
        average = average_long_price
    else:
        average = round(1.0 - average_long_price, 8)
    return {
        "exchange_state": order.get("state"), "filled_quantity": filled,
        "remaining_quantity": leaves, "average_outcome_cost": average,
    }


async def fetch_mlb_events() -> dict[str, Any]:
    query = urlencode({"limit": 1000, "active": "true", "closed": "false"})
    url = f"{GATEWAY_BASE_URL}/v2/leagues/{LEAGUE}/events?{query}"

    def fetch() -> dict[str, Any]:
        request = Request(url, headers={"Accept": "application/json", "User-Agent": "polymarket-mlb-average-down/1.0"})
        with urlopen(request, timeout=20) as response:  # noqa: S310 - fixed first-party HTTPS endpoint
            payload = json.loads(response.read().decode("utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("MLB events endpoint returned a non-object response")
        return payload

    return await asyncio.to_thread(fetch)


async def bbo(client: Any, market_slug: str) -> dict[str, float | None]:
    return executable_outcome_asks(await client.markets.bbo(market_slug))


def initial_order_key() -> str:
    return "initial"


def ladder_order_key(level: float) -> str:
    return f"ladder-{level:.8f}"


async def refresh_order(client: Any, order: dict[str, Any]) -> bool:
    order_id = order.get("order_id")
    if not order_id or order.get("status") in {"canceled", "canceled_unfilled", "rejected", "submission_unknown"}:
        return False
    response = await client.orders.retrieve(str(order_id))
    raw = field(response, "order")
    if not isinstance(raw, dict):
        return False
    before = (order.get("status"), order.get("filled_quantity"), order.get("remaining_quantity"))
    order.update(order_snapshot(raw, float(order.get("outcome_cost") or 0.0), str(order.get("outcome") or "long")))
    state = str(order.get("exchange_state") or "")
    order["status"] = state.lower().replace("order_state_", "") or order.get("status")
    order["checked_at"] = now_iso()
    changed = before != (order.get("status"), order.get("filled_quantity"), order.get("remaining_quantity"))
    if changed:
        LOG.info(
            "ORDER UPDATE | %s %s @ $%.4f state=%s fill=%s remaining=%s id=%s",
            order.get("market_slug"), str(order.get("outcome", "?")).upper(),
            float(order.get("outcome_cost") or 0.0), order.get("status"),
            order.get("filled_quantity"), order.get("remaining_quantity"), order_id,
        )
    return changed


async def submit_order(
    client: Any, *, record: dict[str, Any], key: str, outcome: str, outcome_cost: float,
    quantity: int, tif: str, dry_run: bool, state_path: Path, state: dict[str, Any], reason: str,
) -> dict[str, Any]:
    """Persist submission intent before I/O; never retry an ambiguous external order."""
    order = {
        "submission_token": str(uuid.uuid4()), "market_slug": record["market_slug"],
        "outcome": outcome, "outcome_cost": round(outcome_cost, 8),
        "api_long_price": round(api_price_for_outcome(outcome, outcome_cost), 8),
        "quantity": quantity, "time_in_force": tif, "reason": reason,
        "submitted_at": now_iso(), "filled_quantity": 0, "remaining_quantity": quantity,
        "status": "dry_run" if dry_run else "submitting",
    }
    record.setdefault("orders", {})[key] = order
    save_json(state_path, state)
    if dry_run:
        LOG.info(
            "DRY RUN ORDER | %s %s @ outcome_cost=$%.4f api_long_price=$%.4f qty=%d tif=%s | %s",
            record["market_slug"], outcome.upper(), outcome_cost, order["api_long_price"], quantity, tif, reason,
        )
        return order
    payload = {
        "marketSlug": record["market_slug"],
        "intent": "ORDER_INTENT_BUY_LONG" if outcome == "long" else "ORDER_INTENT_BUY_SHORT",
        "type": "ORDER_TYPE_LIMIT", "price": {"value": f"{order['api_long_price']:.8f}", "currency": "USD"},
        "quantity": quantity, "tif": tif, "manualOrderIndicator": "MANUAL_ORDER_INDICATOR_AUTOMATIC",
        "synchronousExecution": True,
    }
    try:
        response = await client.orders.create(payload)
    except Exception as exc:  # noqa: BLE001
        # A network failure after request transmission has uncertain exchange
        # state.  Stop instead of blindly retrying and duplicating an order.
        order.update({"status": "submission_unknown", "error": str(exc), "failed_at": now_iso()})
        save_json(state_path, state)
        LOG.error("ORDER UNKNOWN | %s key=%s: %s. Manual reconciliation required.", record["market_slug"], key, exc)
        return order
    executions = field(response, "executions") or []
    raw_order = field(executions[0], "order") if executions and isinstance(executions[0], dict) else None
    order_id = field(response, "id") or field(raw_order, "id")
    order["order_id"] = str(order_id) if order_id else None
    if isinstance(raw_order, dict):
        order.update(order_snapshot(raw_order, outcome_cost, outcome))
        order["status"] = str(order.get("exchange_state") or "submitted").lower().replace("order_state_", "")
    else:
        order["status"] = "submitted"
    if not order["order_id"]:
        order["status"] = "submission_unknown"
    save_json(state_path, state)
    LOG.info(
        "ORDER %s | %s %s @ outcome_cost=$%.4f api_long_price=$%.4f qty=%d fill=%s remaining=%s id=%s",
        str(order["status"]).upper(), record["market_slug"], outcome.upper(), outcome_cost,
        order["api_long_price"], quantity, order.get("filled_quantity"), order.get("remaining_quantity"),
        order.get("order_id") or "?",
    )
    return order


async def cancel_order(client: Any, order: dict[str, Any], dry_run: bool) -> None:
    if not order.get("order_id") or order.get("status") in {"filled", "canceled", "canceled_unfilled", "rejected", "submission_unknown"}:
        return
    if dry_run:
        order["status"] = "dry_run_canceled"
        return
    try:
        await client.orders.cancel(str(order["order_id"]), {"marketSlug": str(order["market_slug"])})
        order.update({"status": "canceled", "canceled_at": now_iso()})
        LOG.info("CANCELED | %s", order["order_id"])
    except Exception as exc:  # noqa: BLE001
        order["cancel_error"] = str(exc)
        LOG.warning("Cancel failed for %s: %s", order["order_id"], exc)


async def reconcile_game(client: Any, record: dict[str, Any], dry_run: bool) -> None:
    for order in orders_for_game(record):
        await refresh_order(client, order)
    initial = (record.get("orders") or {}).get(initial_order_key())
    if isinstance(initial, dict) and order_fill_count(initial) > 0 and not record.get("locked_outcome"):
        record["locked_outcome"] = initial["outcome"]
        record["locked_at"] = now_iso()
        record["actual_initial_fill"] = float(initial.get("average_outcome_cost") or initial["outcome_cost"])
        record["status"] = "ladder_active"
        LOG.info("SIDE LOCKED | %s %s after %d contract fill @ $%.4f", record["market_slug"],
                 record["locked_outcome"].upper(), order_fill_count(initial), record["actual_initial_fill"])
    elif isinstance(initial, dict) and initial.get("status") in {"canceled", "canceled_unfilled", "rejected"} and not record.get("locked_outcome"):
        record["status"] = "watching_trigger"


async def submit_ladder(
    client: Any, record: dict[str, Any], config: dict[str, Any], dry_run: bool,
    state_path: Path, state: dict[str, Any],
) -> None:
    if record.get("status") != "ladder_active" or not record.get("locked_outcome"):
        return
    initial_fill = as_float(record.get("actual_initial_fill"))
    tick_size = as_float(record.get("tick_size"))
    if initial_fill is None or tick_size is None:
        return
    quantity = int(record["quantity"])
    existing = orders_for_game(record)
    slots = config["max_contracts_per_game"] // quantity
    for level in lower_levels(initial_fill, config["price_step"], tick_size, slots):
        key = ladder_order_key(level)
        if key in record["orders"]:
            continue
        if sum(order_quantity(order) for order in existing) + quantity > config["max_contracts_per_game"]:
            return
        await submit_order(
            client, record=record, key=key, outcome=record["locked_outcome"], outcome_cost=level,
            quantity=quantity, tif="TIME_IN_FORCE_GOOD_TILL_CANCEL", dry_run=dry_run,
            state_path=state_path, state=state,
            reason="Initial outcome filled; adding the next strictly lower 10-cent averaging rung.",
        )
        existing = orders_for_game(record)


async def snapshot_baseline(client: Any, record: dict[str, Any], game: GameMarket, config: dict[str, Any]) -> bool:
    asks = await bbo(client, game.market_slug)
    if any(asks[outcome] is None for outcome in ("long", "short")):
        LOG.info("WAIT BASELINE | %s needs both long ask and long bid to price both teams.", game.market_slug)
        return False
    outcomes: dict[str, dict[str, Any]] = {}
    for outcome, side in game.outcomes.items():
        baseline = float(asks[outcome])
        target = threshold_from_baseline(baseline, config["price_step"], game.tick_size)
        if target is None:
            LOG.info("SKIP BASELINE | %s %s baseline=$%.4f has no valid 10-cent lower price.",
                     game.market_slug, side["team"], baseline)
            return False
        outcomes[outcome] = {**side, "initial_ask": baseline, "entry_target": target}
    record.update({
        "outcomes": outcomes, "status": "watching_trigger", "baseline_observed_at": now_iso(),
        "quantity": max(config["initial_position_size"], game.minimum_trade_qty), "baseline_asks": asks,
    })
    LOG.info(
        "BASELINE | %s home=%s $%.4f -> buy <= $%.4f | away=%s $%.4f -> buy <= $%.4f",
        game.market_slug,
        next(side["team"] for side in outcomes.values() if side["role"] == "home"),
        next(side["initial_ask"] for side in outcomes.values() if side["role"] == "home"),
        next(side["entry_target"] for side in outcomes.values() if side["role"] == "home"),
        next(side["team"] for side in outcomes.values() if side["role"] == "away"),
        next(side["initial_ask"] for side in outcomes.values() if side["role"] == "away"),
        next(side["entry_target"] for side in outcomes.values() if side["role"] == "away"),
    )
    return True


async def resolve_ml_side(game: GameMarket, record: dict[str, Any], config: dict[str, Any]) -> bool:
    """Freeze one ML-selected team before the mechanical discount watcher runs."""
    if config["strategy_mode"] != "ml_side_average_down":
        return True
    if record.get("ml_selected_outcome") in {"long", "short"}:
        return True
    try:
        from polymarket_mlb_ml_inference import choose_ml_side
    except ImportError as exc:
        record.update({"status": "ml_inference_unavailable", "ml_error": f"ml_module_import_failed:{exc}"})
        LOG.error("ML SIDE FAILED | %s %s", game.market_slug, record["ml_error"])
        return False
    result = await asyncio.to_thread(
        choose_ml_side, model_path=Path(config["ml_model_path"]), game_start=game.game_start,
        outcomes=record.get("outcomes") or {}, asks=record.get("baseline_asks") or {},
        min_confidence=float(config["ml_min_confidence"]), root=Path("."),
    )
    outcome = result.get("outcome") if isinstance(result, dict) else None
    if outcome not in {"long", "short"}:
        error = result.get("error", "unknown_ml_inference_failure") if isinstance(result, dict) else "unknown_ml_inference_failure"
        record.update({"status": "ml_inference_unavailable", "ml_error": error, "ml_result": result})
        LOG.warning("ML SIDE FAILED | %s %s; no mechanical side will be substituted.", game.market_slug, error)
        return False
    record.update({
        "ml_selected_outcome": outcome, "ml_selected_at": now_iso(), "ml_p_home": result.get("p_home"),
        "ml_confidence": result.get("confidence"), "ml_model": result.get("model"), "ml_result": result,
    })
    side = record["outcomes"][outcome]
    LOG.info(
        "ML SIDE FROZEN | %s %s %s p_home=%.4f confidence=%.4f model=%s; only this team can trigger the 10c ladder.",
        game.market_slug, side["role"].upper(), side["team"], float(result["p_home"]), float(result["confidence"]), result.get("model"),
    )
    return True


async def consider_entry(
    client: Any, record: dict[str, Any], config: dict[str, Any], dry_run: bool,
    state_path: Path, state: dict[str, Any], now: datetime,
) -> None:
    if record.get("status") != "watching_trigger" or record.get("locked_outcome"):
        return
    if len(active_position_records(state, now)) >= config["max_active_games"]:
        return
    outcomes = record.get("outcomes") or {}
    if set(outcomes) != {"long", "short"}:
        return
    asks = await bbo(client, record["market_slug"])
    selected = record.get("ml_selected_outcome") if config["strategy_mode"] == "ml_side_average_down" else None
    if config["strategy_mode"] == "ml_side_average_down" and selected not in {"long", "short"}:
        return
    trigger = choose_first_trigger(outcomes, asks, config["price_step"], selected)
    if trigger is None:
        return
    outcome, ask, target = trigger
    quantity = int(record["quantity"])
    anticipated = planned_ladder_cost(target, quantity, config["price_step"], float(record["tick_size"]), config["max_contracts_per_game"])
    if reserved_capital(state, now) + anticipated > config["max_total_capital"] + 1e-9:
        LOG.warning("SKIP CAPITAL | %s requires up to $%.4f; reserved=$%.4f cap=$%.4f", record["market_slug"],
                    anticipated, reserved_capital(state, now), config["max_total_capital"])
        return
    # An IOC *limit* is safer than a market order: it can fill at the observed
    # lower ask but never above the precomputed trigger.  The actual fill cost
    # becomes the start point for the lower ladder.
    record["status"] = "entry_submitting"
    order = await submit_order(
        client, record=record, key=initial_order_key(), outcome=outcome, outcome_cost=target,
        quantity=quantity, tif="TIME_IN_FORCE_IMMEDIATE_OR_CANCEL", dry_run=dry_run,
        state_path=state_path, state=state,
        reason=(f"{outcomes[outcome]['role']} {outcomes[outcome]['team']} ask observed ${ask:.4f}; "
                f"baseline target is <= ${target:.4f}."),
    )
    if order_fill_count(order) > 0:
        await reconcile_game(client, record, dry_run)
    elif dry_run:
        record["status"] = "dry_run_entry_observed"
    elif order.get("status") != "submission_unknown":
        record["status"] = "watching_trigger"


async def close_started_games(client: Any, state: dict[str, Any], dry_run: bool, now: datetime) -> None:
    for record in state.get("games", {}).values():
        if not isinstance(record, dict) or record.get("status") in {"game_started", "submission_unknown"}:
            continue
        if not game_started(record, now):
            continue
        for order in orders_for_game(record):
            await cancel_order(client, order, dry_run)
        record.update({"status": "game_started", "orders_canceled_at_start": now_iso()})
        LOG.info("GAME STARTED | %s: no new orders; unfilled ladder orders canceled.", record.get("market_slug", "?"))


def performance_report(state: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    records = [record for record in state.get("games", {}).values() if isinstance(record, dict)]
    locked = [record for record in records if record.get("locked_outcome")]
    return {
        "generated_at": now_iso(), "strategy": "mechanical_mlb_price_average_down_v1",
        "configuration": config, "games_observed": len(records), "games_with_filled_entry": len(locked),
        "contracts_filled": sum(filled_contracts(record) for record in records),
        "active_ladders": sum(record.get("status") == "ladder_active" for record in records),
        "game_started_records": sum(record.get("status") == "game_started" for record in records),
        "note": "Entry and ladder audit only. This runner intentionally does not forecast games or add an exit strategy.",
    }


async def log_heartbeat(state: dict[str, Any], games: list[GameMarket], config: dict[str, Any], dry_run: bool) -> None:
    now = datetime.now(timezone.utc)
    LOG.info("HEARTBEAT | mode=%s discovered=%d tracked=%d active_ladders=%d reserved=$%.4f cap=$%.4f",
             "DRY_RUN" if dry_run else "LIVE", len(games), len(state.get("games", {})),
             len(active_position_records(state, now)), reserved_capital(state, now), config["max_total_capital"])
    for record in state.get("games", {}).values():
        if not isinstance(record, dict) or record.get("status") == "game_started":
            continue
        LOG.info("GAME | %s status=%s locked=%s filled=%d start=%s", record.get("market_slug"), record.get("status"),
                 str(record.get("locked_outcome") or "none").upper(), filled_contracts(record), record.get("game_start"))


async def run(args: argparse.Namespace) -> int:
    config = validate_config(load_json(args.config, DEFAULT_CONFIG))
    config, changed = apply_config_overrides(config, args)
    if args.persist_config or changed:
        save_json(args.config, config)
    dry_run = os.getenv("DRY_RUN", "true").lower() in {"1", "true", "yes"}
    live_allowed = not dry_run and args.submit and args.allow_live
    if not dry_run and not live_allowed:
        raise SystemExit("Refusing live orders: use DRY_RUN=false with both --submit and --allow-live")
    if AsyncPolymarketUS is None:
        raise SystemExit("Install requirements.txt (polymarket-us) before running")
    api_key = os.getenv("POLYMARKET_PUBLIC_KEY")
    secret_key = os.getenv("POLYMARKET_SECRET_KEY")
    if live_allowed and (not api_key or not secret_key):
        raise SystemExit("POLYMARKET_PUBLIC_KEY and POLYMARKET_SECRET_KEY are required for live orders")
    state = load_json(args.state_file, default_state())
    state.setdefault("format_version", STATE_VERSION)
    state.setdefault("games", {})
    started = asyncio.get_running_loop().time()
    deadline = started + args.run_seconds
    last_heartbeat = float("-inf")
    LOG.info("STARTUP | mode=%s contracts_per_rung=%d step=$%.2f max_game_contracts=%d",
             "DRY_RUN" if dry_run else "LIVE", config["initial_position_size"], config["price_step"],
             config["max_contracts_per_game"])
    LOG.info("STRATEGY | mode=%s%s", config["strategy_mode"],
             f" model={config['ml_model_path']} confidence_gate={config['ml_min_confidence']:.2f}" if config["strategy_mode"] == "ml_side_average_down" else "")
    async with AsyncPolymarketUS(key_id=api_key, secret_key=secret_key) as client:
        try:
            while True:
                now = datetime.now(timezone.utc)
                await close_started_games(client, state, dry_run, now)
                payload = await fetch_mlb_events()
                games = discover_games(payload, now)
                for game in games:
                    record = game_record(state, game)
                    if game_started(record, now):
                        continue
                    await reconcile_game(client, record, dry_run)
                    if record.get("status") == "watching_baseline":
                        if await snapshot_baseline(client, record, game, config):
                            await resolve_ml_side(game, record, config)
                            save_json(args.state_file, state)
                    await consider_entry(client, record, config, dry_run, args.state_file, state, now)
                    await reconcile_game(client, record, dry_run)
                    await submit_ladder(client, record, config, dry_run, args.state_file, state)
                monotonic_now = asyncio.get_running_loop().time()
                if monotonic_now - last_heartbeat >= config["status_log_seconds"]:
                    await log_heartbeat(state, games, config, dry_run)
                    last_heartbeat = monotonic_now
                save_json(args.state_file, state)
                save_json(args.report, performance_report(state, config))
                if monotonic_now >= deadline:
                    break
                await asyncio.sleep(config["poll_seconds"])
        finally:
            save_json(args.state_file, state)
            save_json(args.report, performance_report(state, config))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("polymarket_mlb_average_down_config.json"))
    parser.add_argument("--state-file", type=Path, default=Path("polymarket_mlb_average_down_state.json"))
    parser.add_argument("--report", type=Path, default=Path("polymarket_mlb_average_down_report.json"))
    parser.add_argument("--run-seconds", type=float, default=840.0)
    parser.add_argument("--submit", action="store_true")
    parser.add_argument("--allow-live", action="store_true")
    parser.add_argument("--persist-config", action="store_true")
    parser.add_argument("--initial-position-size", type=int)
    parser.add_argument("--max-active-games", type=int)
    parser.add_argument("--max-contracts-per-game", type=int)
    parser.add_argument("--max-total-capital", type=float)
    parser.add_argument("--price-step", type=float)
    parser.add_argument("--poll-seconds", type=float)
    parser.add_argument("--status-log-seconds", type=float)
    parser.add_argument("--strategy-mode", choices=("mechanical", "ml_side_average_down"))
    parser.add_argument("--ml-model-path")
    parser.add_argument("--ml-min-confidence", type=float)
    return parser


if __name__ == "__main__":
    configure_logging()
    raise SystemExit(asyncio.run(run(build_parser().parse_args())))
