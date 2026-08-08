"""Production KXBTC15M hybrid live trader.

This runner intentionally replaces the old ladder runner in the GitHub
workflow.  It reuses the established Kalshi V2 REST/WebSocket/auth transport,
but owns no Prophet, shadow-equity, loss-skip, or ladder behavior.

The only sizing transitions are from :mod:`strategy_core`, which is also used
by the historical settlement replay.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any, Iterable

from audit_ledger import append_audit
from live_checkpoint import MaterialCheckpointPublisher
from kalshi_btc15m_average_down import (
    KalshiLiveFeed,
    KalshiREST,
    field,
    market_result,
    order_average_position_price,
    order_fee_total,
    order_fill_count,
    order_remaining_count,
    timestamp_epoch,
)
from live_state import append_unique, config_hash, load_state, save_state, utc_now
from strategy_core import (
    StrategyParameters,
    apply_realized_filled_trade,
    decimal,
    full_snapshot,
    prescribed_quantity,
    round_shares,
    sizing_state,
    zero_fill_snapshot,
)


LOG = logging.getLogger("kalshi_live_trader")
ORDER_NAMESPACE = uuid.UUID("602ca251-d5dc-43c7-ae11-a6be6f19a43b")
ORDER_PREFIX = "kxbtc15m-hybrid-v1-"
TERMINAL_STATES = {"CLOSED", "ZERO_FILL", "FUNDING_FAILURE", "MISSED_SIGNAL", "ERROR_RECONCILIATION"}
ACTIVE_STATES = {"SIGNAL_PENDING", "ENTRY_PENDING", "ENTRY_PARTIAL", "POSITION_OPEN", "STOP_PENDING", "SETTLEMENT_PENDING"}


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )


def _bool(value: Any) -> bool:
    return str(value).lower() in {"1", "true", "yes", "on"}


def _decimal_string(value: Any, name: str) -> str:
    amount = decimal(value)
    if amount <= Decimal("0"):
        raise ValueError(f"{name} must be positive")
    return format(amount, "f")


DECIMAL_CONFIG_FIELDS = {
    "entry_price", "stop_price", "starting_base", "recovery_multiplier", "first_base_threshold",
    "threshold_growth_multiplier", "base_increment", "max_position", "provisional_outcome_threshold",
    "max_recovery_cycle_loss", "max_daily_realized_loss", "starting_shadow_balance",
}
INTEGER_CONFIG_FIELDS = {
    "signal_delay_seconds", "entry_timeout_seconds", "entry_lateness_seconds", "outcome_observation_seconds",
    "max_recovery_exponent", "max_api_failures",
}
FLOAT_CONFIG_FIELDS = {"stop_poll_interval", "reconciliation_interval", "max_outcome_quote_age_seconds", "max_stale_quote_seconds"}


def load_config(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("live strategy configuration must be a JSON object")
    required = {
        "strategy_version", "series", "entry_price", "stop_price", "starting_base", "recovery_multiplier",
        "first_base_threshold", "threshold_growth_multiplier", "base_increment", "max_position",
    }
    missing = required - value.keys()
    if missing:
        raise ValueError(f"live strategy configuration is missing: {', '.join(sorted(missing))}")
    if value["series"] != "KXBTC15M":
        raise ValueError("this production runner is intentionally limited to KXBTC15M")
    for name in DECIMAL_CONFIG_FIELDS & value.keys():
        value[name] = _decimal_string(value[name], name)
    for name in INTEGER_CONFIG_FIELDS & value.keys():
        value[name] = int(value[name])
        if value[name] < 0:
            raise ValueError(f"{name} cannot be negative")
    for name in FLOAT_CONFIG_FIELDS & value.keys():
        value[name] = float(value[name])
        if value[name] <= 0:
            raise ValueError(f"{name} must be positive")
    value.setdefault("live_enabled", False)
    value.setdefault("dry_run", True)
    value.setdefault("allow_capital_downsize", False)
    value.setdefault("shadow_fill_model", "conservative_trade_through")
    value.setdefault("starting_shadow_balance", "1000.00")
    value.setdefault("signal_delay_seconds", 0)
    value.setdefault("entry_timeout_seconds", 120)
    value.setdefault("entry_lateness_seconds", 30)
    value.setdefault("stop_poll_interval", 1.0)
    value.setdefault("reconciliation_interval", 5.0)
    value.setdefault("outcome_observation_seconds", 5)
    value.setdefault("provisional_outcome_threshold", "0.99")
    value.setdefault("max_outcome_quote_age_seconds", 2.0)
    value.setdefault("max_stale_quote_seconds", 2.0)
    value.setdefault("max_recovery_exponent", 12)
    value.setdefault("max_recovery_cycle_loss", "50.00")
    value.setdefault("max_daily_realized_loss", "25.00")
    value.setdefault("max_api_failures", 5)
    if value["shadow_fill_model"] != "conservative_trade_through":
        raise ValueError("only conservative_trade_through is supported for shadow maker-fill evidence")
    if decimal(value["entry_price"]) <= decimal(value["stop_price"]):
        raise ValueError("entry_price must be above stop_price")
    if decimal(value["starting_base"]) != Decimal("1.00"):
        raise ValueError("the hybrid strategy must start at exactly 1.00 share")
    return value


def save_config(path: Path, config: dict[str, Any]) -> None:
    path.write_text(json.dumps(config, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def apply_overrides(config: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    updated = dict(config)
    for name in DECIMAL_CONFIG_FIELDS | INTEGER_CONFIG_FIELDS | FLOAT_CONFIG_FIELDS:
        value = getattr(args, name, None)
        if value not in (None, ""):
            updated[name] = str(value) if name in DECIMAL_CONFIG_FIELDS else value
    for name in ("allow_capital_downsize",):
        value = getattr(args, name, None)
        if value is not None:
            updated[name] = value
    return load_config_from_value(updated)


def load_config_from_value(value: dict[str, Any]) -> dict[str, Any]:
    temporary = dict(value)
    # Keep validation identical for a persisted config and command overrides.
    for name in DECIMAL_CONFIG_FIELDS & temporary.keys():
        temporary[name] = _decimal_string(temporary[name], name)
    for name in INTEGER_CONFIG_FIELDS & temporary.keys():
        temporary[name] = int(temporary[name])
    for name in FLOAT_CONFIG_FIELDS & temporary.keys():
        temporary[name] = float(temporary[name])
    required = {"strategy_version", "series", "entry_price", "stop_price", "starting_base", "recovery_multiplier", "first_base_threshold", "threshold_growth_multiplier", "base_increment", "max_position"}
    if required - temporary.keys():
        raise ValueError("invalid overridden live strategy configuration")
    # Reuse the normal rules without doing a file round-trip.
    if temporary["series"] != "KXBTC15M" or decimal(temporary["starting_base"]) != Decimal("1.00"):
        raise ValueError("invalid strategy series or starting base")
    if decimal(temporary["entry_price"]) <= decimal(temporary["stop_price"]):
        raise ValueError("entry_price must be above stop_price")
    temporary.setdefault("shadow_fill_model", "conservative_trade_through")
    temporary.setdefault("starting_shadow_balance", "1000.00")
    if temporary["shadow_fill_model"] != "conservative_trade_through":
        raise ValueError("only conservative_trade_through is supported for shadow maker-fill evidence")
    return temporary


def strategy_parameters(config: dict[str, Any]) -> StrategyParameters:
    return StrategyParameters(
        recovery_multiplier=Decimal(config["recovery_multiplier"]),
        first_base_threshold=Decimal(config["first_base_threshold"]),
        threshold_growth_multiplier=Decimal(config["threshold_growth_multiplier"]),
        base_increment=Decimal(config["base_increment"]),
        starting_base=Decimal(config["starting_base"]),
        max_position=Decimal(config["max_position"]),
    )


def deterministic_client_order_id(ticker: str, side: str, purpose: str, config: dict[str, Any]) -> str:
    key = f"{config['strategy_version']}:{config_hash(config)}:{ticker}:{side}:{purpose}"
    return ORDER_PREFIX + uuid.uuid5(ORDER_NAMESPACE, key).hex


def epoch(value: Any) -> float | None:
    converted = timestamp_epoch(value)
    if converted is not None:
        return float(converted)
    if isinstance(value, (int, float)):
        return float(value) / 1000.0 if value > 10_000_000_000 else float(value)
    return None


def market_metadata(market: Any) -> dict[str, Any] | None:
    ticker = str(field(market, "ticker") or "")
    opened = epoch(field(market, "open_time", "open_ts", "open_ts_ms"))
    closed = epoch(field(market, "close_time", "expected_expiration_time", "close_ts", "close_ts_ms"))
    if not ticker or opened is None or closed is None or closed <= opened:
        return None
    return {
        "ticker": ticker, "open_epoch": opened, "close_epoch": closed,
        "status": str(field(market, "status") or "").lower(), "raw": market,
    }


@dataclass(frozen=True)
class QuoteObservation:
    ticker: str
    received_epoch: float
    exchange_epoch: float | None
    yes_bid: Decimal
    no_bid: Decimal
    source: str = "kalshi_websocket_ticker"


class ProvisionalOutcomeTracker:
    """Rolling, auditable final-quote inference; it never guesses a side."""

    def __init__(self, threshold: Decimal, observation_seconds: int, max_quote_age_seconds: float) -> None:
        self.threshold = threshold
        self.observation_seconds = observation_seconds
        self.max_quote_age_seconds = max_quote_age_seconds
        self.observations: dict[str, list[QuoteObservation]] = {}
        self._last_book_id: dict[str, str] = {}

    def observe_feed(self, feed: KalshiLiveFeed, ticker: str) -> None:
        quote = feed.quotes.get(ticker)
        book = quote.get("complete_book") if isinstance(quote, dict) else None
        if not isinstance(book, dict):
            return
        book_id = str(book.get("quote_id") or "")
        if not book_id or self._last_book_id.get(ticker) == book_id:
            return
        yes_bid = book.get("yes_bid")
        yes_ask = book.get("yes_ask")
        try:
            yes_bid_d = Decimal(str(yes_bid))
            no_bid_d = Decimal("1") - Decimal(str(yes_ask))
        except Exception:
            return
        if not (Decimal("0") <= yes_bid_d <= Decimal("1") and Decimal("0") <= no_bid_d <= Decimal("1")):
            return
        self._last_book_id[ticker] = book_id
        observation = QuoteObservation(
            ticker=ticker,
            received_epoch=time.time(),
            exchange_epoch=epoch(book.get("source_timestamp_ms") or book.get("source_server_timestamp")),
            yes_bid=yes_bid_d,
            no_bid=no_bid_d,
        )
        records = self.observations.setdefault(ticker, [])
        records.append(observation)
        cutoff = observation.received_epoch - max(30, self.observation_seconds * 3)
        self.observations[ticker] = [item for item in records if item.received_epoch >= cutoff]

    def infer(self, ticker: str, boundary_epoch: float) -> dict[str, Any] | None:
        candidates = [
            item for item in self.observations.get(ticker, [])
            # The directional fact must have been observable *before* the
            # old market closed.  Do not let a post-boundary websocket update
            # rewrite the fact used to enter the new market.
            if boundary_epoch - self.observation_seconds <= item.received_epoch <= boundary_epoch
        ]
        if not candidates:
            return None
        latest = max(candidates, key=lambda item: item.received_epoch)
        quote_age = boundary_epoch - (latest.exchange_epoch or latest.received_epoch)
        if quote_age < -1.0 or quote_age > self.max_quote_age_seconds:
            return None
        yes_qualifying = [item for item in candidates if item.yes_bid >= self.threshold]
        no_qualifying = [item for item in candidates if item.no_bid >= self.threshold]
        latest_yes = latest.yes_bid >= self.threshold
        latest_no = latest.no_bid >= self.threshold
        if latest_yes == latest_no:  # both true is conflict; both false is unavailable.
            return None
        side = "yes" if latest_yes else "no"
        selected = yes_qualifying if side == "yes" else no_qualifying
        return {
            "outcome": side,
            "ticker": ticker,
            "timestamp": datetime.fromtimestamp(latest.received_epoch, timezone.utc).isoformat(),
            "exchange_timestamp": latest.exchange_epoch,
            "quote_age_seconds": round(max(0.0, quote_age), 6),
            "method": "final_executable_bid_threshold",
            "threshold": format(self.threshold, "f"),
            "final_yes_bid": format(latest.yes_bid, "f"),
            "final_no_bid": format(latest.no_bid, "f"),
            "max_yes_bid": format(max(item.yes_bid for item in candidates), "f"),
            "max_no_bid": format(max(item.no_bid for item in candidates), "f"),
            "qualifying_observations": len(selected),
        }


class LiveEngine:
    def __init__(self, config: dict[str, Any], state: dict[str, Any], state_path: Path, ledger_path: Path, dry_run: bool, config_path: Path | None = None) -> None:
        self.config = config
        self.state = state
        self.state_path = state_path
        self.ledger_path = ledger_path
        self.config_path = config_path
        self.dry_run = dry_run
        self.parameters = strategy_parameters(config)
        previous_hash = str(self.state.get("config_hash") or "")
        current_hash = config_hash(config)
        if previous_hash and previous_hash != current_hash:
            # A negative recovery cycle remains bound to its persisted
            # parameters; an open record also carries its own snapshot.  A
            # new configuration therefore affects only a future fresh cycle.
            self.state["config_change"] = {
                "previous_config_hash": previous_hash, "current_config_hash": current_hash,
                "recorded_at": utc_now(), "policy": "existing_cycle_continues_under_its_creation_configuration",
            }
        self.state["strategy_version"] = config["strategy_version"]
        self.state["config_hash"] = current_hash
        self.tracker = ProvisionalOutcomeTracker(
            Decimal(config["provisional_outcome_threshold"]), int(config["outcome_observation_seconds"]),
            float(config["max_outcome_quote_age_seconds"]),
        )
        self.last_reconcile = 0.0
        self.last_heartbeat = 0.0
        self.last_market_discovery = 0.0
        self.markets: list[dict[str, Any]] = []
        checkpoint_paths = [state_path, ledger_path]
        if config_path is not None:
            checkpoint_paths.insert(0, config_path)
        self.publisher = MaterialCheckpointPublisher(*checkpoint_paths)

    def checkpoint(self, reason: str | None = None) -> None:
        save_state(self.state_path, self.state)
        if reason:
            self.publisher.publish_if_changed(reason)

    def shadow_metrics(self) -> dict[str, Any]:
        """The isolated, simulated-account metrics ledger used only in dry mode."""

        initial = Decimal(self.config["starting_shadow_balance"])
        metrics = self.state.setdefault("shadow_metrics", {})
        metrics.setdefault("starting_balance", format(initial, "f"))
        metrics.setdefault("balance", format(initial, "f"))
        metrics.setdefault("peak_balance", format(initial, "f"))
        metrics.setdefault("reserved_cash", "0.00")
        metrics.setdefault("max_reserved_cash", "0.00")
        metrics.setdefault("max_required_cash", "0.00")
        metrics.setdefault("max_drawdown", "0.00")
        metrics.setdefault("funding_failures", 0)
        metrics.setdefault("zero_fills", 0)
        metrics.setdefault("completed_trades", 0)
        metrics.setdefault("stop_count", 0)
        metrics.setdefault("settlement_count", 0)
        return metrics

    def shadow_available_cash(self) -> Decimal:
        metrics = self.shadow_metrics()
        return Decimal(str(metrics["balance"])) - Decimal(str(metrics["reserved_cash"]))

    def note_zero_fill(self) -> None:
        if self.dry_run:
            metrics = self.shadow_metrics()
            metrics["zero_fills"] = int(metrics["zero_fills"]) + 1

    def audit(self, event: str, **details: Any) -> None:
        append_audit(self.ledger_path, {
            "event": event, "strategy_version": self.config["strategy_version"],
            "config_hash": config_hash(self.config), **details,
        })

    def transition(self, record: dict[str, Any], status: str, reason: str | None = None) -> None:
        prior = record.get("status")
        record["status"] = status
        record["updated_at"] = utc_now()
        if reason:
            record["status_reason"] = reason
        self.audit("state_transition", ticker=record.get("ticker"), from_state=prior, to_state=status, reason=reason)
        LOG.info("STATE | ticker=%s %s→%s%s", record.get("ticker"), prior, status, f" reason={reason}" if reason else "")
        self.checkpoint("state_transition")

    def trip(self, reason: str) -> None:
        breaker = self.state["circuit_breaker"]
        if not breaker.get("blocked"):
            breaker.update({"blocked": True, "reason": reason, "triggered_at": utc_now()})
            self.audit("circuit_breaker", reason=reason)
            LOG.critical("CIRCUIT BREAKER | %s; new exposure disabled", reason)
            self.checkpoint("circuit_breaker")

    def current_parameters(self) -> StrategyParameters:
        cycle = self.state.get("cycle_strategy_parameters")
        if isinstance(cycle, dict) and Decimal(str(self.state.get("sizing", {}).get("recovery_cycle_pnl", "0"))) < 0:
            return StrategyParameters(**{key: Decimal(value) for key, value in cycle.items()})
        return self.parameters

    def record_parameters(self, record: dict[str, Any]) -> StrategyParameters:
        snapshot = record.get("config_snapshot")
        if isinstance(snapshot, dict):
            try:
                return StrategyParameters(**{key: Decimal(str(value)) for key, value in snapshot.items()})
            except (ArithmeticError, TypeError, ValueError):
                self.trip("invalid_persisted_record_configuration")
        return self.current_parameters()

    def circuit_allows_entry(self) -> bool:
        if self.state["circuit_breaker"].get("blocked"):
            return False
        sizing = sizing_state(self.current_parameters(), self.state.get("sizing"))
        if sizing.recovery_exponent >= int(self.config["max_recovery_exponent"]):
            self.trip("max_recovery_exponent")
        if -sizing.recovery_cycle_pnl >= Decimal(self.config["max_recovery_cycle_loss"]):
            self.trip("max_recovery_cycle_loss")
        today = datetime.now(timezone.utc).date().isoformat()
        realized = Decimal(str(self.state.get("daily_realized", {}).get(today, "0")))
        if -realized >= Decimal(self.config["max_daily_realized_loss"]):
            self.trip("max_daily_realized_loss")
        return not self.state["circuit_breaker"].get("blocked")

    async def discover(self, rest: KalshiREST) -> None:
        try:
            payload = await rest.get_raw_json("/markets", {"series_ticker": self.config["series"], "limit": 1000})
            candidates = [market_metadata(value) for value in payload.get("markets", []) if isinstance(value, dict)]
            self.markets = sorted([item for item in candidates if item is not None], key=lambda item: item["open_epoch"])
        except Exception as exc:  # read failure: no new entry, but active risk remains managed
            self.state["api_failure_count"] = int(self.state.get("api_failure_count", 0)) + 1
            LOG.warning("MARKET DISCOVERY FAILED | %s", type(exc).__name__)
            if self.state["api_failure_count"] >= int(self.config["max_api_failures"]):
                self.trip("max_api_failures")

    def active_market(self, now: float) -> dict[str, Any] | None:
        candidates = [item for item in self.markets if item["open_epoch"] <= now < item["close_epoch"]]
        return max(candidates, key=lambda item: item["open_epoch"]) if candidates else None

    def predecessor(self, market: dict[str, Any]) -> dict[str, Any] | None:
        candidates = [item for item in self.markets if abs(item["close_epoch"] - market["open_epoch"]) <= 1]
        return max(candidates, key=lambda item: item["open_epoch"]) if candidates else None

    def successor(self, market: dict[str, Any]) -> dict[str, Any] | None:
        """Return the next exchange-provided market, never a guessed ticker."""

        candidates = [item for item in self.markets if abs(item["open_epoch"] - market["close_epoch"]) <= 1]
        return min(candidates, key=lambda item: item["open_epoch"]) if candidates else None

    def set_signal(self, market: dict[str, Any], provisional: dict[str, Any]) -> dict[str, Any]:
        ticker = market["ticker"]
        record = self.state["markets"].get(ticker)
        if isinstance(record, dict):
            return record
        source = str(provisional["outcome"])
        side = "no" if source == "yes" else "yes"
        parameters = self.current_parameters()
        quantity, capped = prescribed_quantity(parameters, self.state.get("sizing"))
        record = {
            "ticker": ticker,
            "market_open_epoch": market["open_epoch"], "market_close_epoch": market["close_epoch"],
            "source_market_ticker": provisional["ticker"], "provisional_outcome": source,
            "provisional_outcome_details": provisional, "signal_side": side,
            "signal_timestamp": utc_now(), "intended_quantity": format(quantity, "f"),
            "quantity_capped": capped, "base_before": format(sizing_state(parameters, self.state.get("sizing")).base_share_count, "f"),
            "recovery_exponent_before": sizing_state(parameters, self.state.get("sizing")).recovery_exponent,
            "recovery_cycle_pnl_before": str(self.state.get("sizing", {}).get("recovery_cycle_pnl", "0")),
            "status": "SIGNAL_PENDING", "entry_orders": [], "exit_orders": [], "actual_quantity": "0.00",
            "strategy_version": self.config["strategy_version"], "config_hash": config_hash(self.config),
            "config_snapshot": self.current_parameters().as_dict(), "created_at": utc_now(),
        }
        self.state["markets"][ticker] = record
        self.state["active_market"] = ticker
        self.audit("signal_created", ticker=ticker, source_ticker=provisional["ticker"], provisional_outcome=source, prediction=side, intended_quantity=format(quantity, "f"))
        LOG.warning("NEW MARKET SIGNAL | ticker=%s source=%s provisional=%s prediction=%s qty=%s entry=$%s", ticker, provisional["ticker"], source.upper(), side.upper(), quantity, self.config["entry_price"])
        self.checkpoint("signal_created")
        return record

    def selected_quote(self, feed: KalshiLiveFeed, ticker: str, side: str, executable: str) -> Decimal | None:
        if executable == "ask":
            quotes = feed.executable_asks(ticker)
            if not quotes:
                return None
            value = quotes.get(side)
            return Decimal(str(value)) if value is not None else None
        quote, _ = feed.executable_shadow_exit_quote(ticker, side, 0.0, float(self.config["max_stale_quote_seconds"]))
        return Decimal(str(quote["economic_price"])) if quote else None

    async def managed_orders(self, rest: KalshiREST) -> list[Any]:
        try:
            response = await rest.orders.get_orders(status="resting", limit=1000)
        except Exception as exc:
            raise RuntimeError(f"open-order reconciliation unavailable ({type(exc).__name__})") from exc
        return [item for item in (field(response, "orders") or []) if str(field(item, "client_order_id") or "").startswith(ORDER_PREFIX)]

    async def cancel_managed_entries(self, rest: KalshiREST) -> int:
        """Explicit emergency action: cancel only this strategy's resting orders.

        It never creates a replacement or closes a filled position.  Unlike a
        normal startup reconcile it can act when the local state is missing,
        because a human explicitly requested risk reduction by the immutable
        client-order prefix.
        """

        orders = await self.managed_orders(rest)
        cancelled = 0
        for order in orders:
            record = {
                "order_id": str(field(order, "order_id") or ""),
                "remaining_count": field(order, "remaining_count", "remaining_count_fp", "count") or "0",
            }
            if not record["order_id"]:
                continue
            await rest.cancel_order(record, dry_run=False)
            cancelled += 1
            self.audit("emergency_managed_order_cancel", order_id=record["order_id"])
        LOG.warning("EMERGENCY CANCEL COMPLETE | managed_resting_orders=%d", cancelled)
        return cancelled

    async def reconcile_startup(self, rest: KalshiREST) -> bool:
        """Reconcile exchange first; uncertainty blocks entries, never closes manual risk."""
        try:
            balance = await rest.balance_decimal()
            orders = await self.managed_orders(rest)
            positions = await rest.portfolio.get_positions(limit=1000)
            recent_fills = await rest.get_raw_json("/portfolio/fills", {"limit": 1000})
        except Exception as exc:
            self.trip("startup_reconciliation_failed")
            LOG.critical("RECONCILIATION FAILED | %s", type(exc).__name__)
            return False
        if balance is None or balance < 0:
            self.trip("invalid_authenticated_balance")
            return False
        known = self.state["markets"]
        for order in orders:
            ticker = str(field(order, "ticker") or "")
            client_id = str(field(order, "client_order_id") or "")
            if not ticker or ticker not in known:
                # The order belongs to this strategy prefix but its durable
                # state is missing. Do not guess its intended signal/order role.
                self.trip("unknown_managed_open_order")
                self.audit("reconciliation_discrepancy", ticker=ticker, client_order_id=client_id, reason="unknown_managed_open_order")
                return False
        for position in field(positions, "market_positions", "positions") or []:
            ticker = str(field(position, "ticker") or "")
            # This strategy never assumes ownership of a different series.
            # Such a position is neither changed nor allowed to poison the
            # KXBTC15M sizing state.
            if not ticker.startswith(self.config["series"] + "-"):
                continue
            raw = Decimal(str(field(position, "position_fp", "position") or "0"))
            if raw == 0:
                continue
            record = known.get(ticker)
            if not isinstance(record, dict):
                # Never classify an unrecognised exchange position as ours.
                self.trip("unknown_exchange_position")
                self.audit("reconciliation_discrepancy", ticker=ticker, reason="unknown_exchange_position")
                return False
            side = str(record.get("signal_side") or "")
            if (side == "yes" and raw < 0) or (side == "no" and raw > 0) or side not in {"yes", "no"}:
                self.trip("position_direction_mismatch")
                return False
            record["actual_quantity"] = format(abs(raw), "f")
            self.state["current_position"] = record["actual_quantity"]
            if record.get("status") in {"ENTRY_PENDING", "ENTRY_PARTIAL", "SIGNAL_PENDING"}:
                self.transition(record, "POSITION_OPEN", "startup_authoritative_position")
        # Settlement/current-market discovery is independently retried during
        # the event loop.  Its result is recorded here for startup audit, but
        # a temporary discovery outage cannot make us forget known exposure.
        await self.discover(rest)
        fill_count = len(recent_fills.get("fills", [])) if isinstance(recent_fills, dict) and isinstance(recent_fills.get("fills"), list) else 0
        self.state["last_reconciliation"] = {"at": utc_now(), "balance": format(balance, "f"), "managed_open_orders": len(orders), "recent_fill_records": fill_count, "markets_discovered": len(self.markets), "success": True}
        self.state["api_failure_count"] = 0
        self.audit("startup_reconciliation", balance=format(balance, "f"), managed_open_orders=len(orders), recent_fill_records=fill_count, markets_discovered=len(self.markets))
        return True

    async def verify_previous_outcome(self, rest: KalshiREST, record: dict[str, Any]) -> None:
        if record.get("official_outcome") in {"yes", "no"}:
            return
        source = str(record.get("source_market_ticker") or "")
        if not source:
            return
        market = await rest.get_market(source)
        official = market_result(market) if market is not None else None
        if official not in {"yes", "no"}:
            return
        provisional = record.get("provisional_outcome")
        matched = official == provisional
        record.update({"official_outcome": official, "official_settlement_timestamp": utc_now(), "provisional_matches_official": matched})
        verification = self.state["outcome_verification"]
        verification["provisional"] = int(verification.get("provisional", 0)) + 1
        verification["verified"] = int(verification.get("verified", 0)) + 1
        verification["matches" if matched else "mismatches"] = int(verification.get("matches" if matched else "mismatches", 0)) + 1
        self.audit("outcome_verified", source_ticker=source, provisional=provisional, official=official, match=matched)
        if not matched:
            LOG.critical("OUTCOME DISCREPANCY | source=%s provisional=%s official=%s; entered signal remains immutable", source, provisional, official)

    async def refresh_entry(self, rest: KalshiREST, record: dict[str, Any]) -> Decimal:
        if self.dry_run:
            raise RuntimeError("shadow entry refresh requires the market-data feed")
        total = Decimal("0")
        for order in record.get("entry_orders", []):
            await rest.refresh_order(order)
            total += Decimal(str(order.get("fill_count") or "0"))
        record["actual_quantity"] = format(total, "f")
        self.state["current_position"] = record["actual_quantity"]
        filled_orders = [item for item in record.get("entry_orders", []) if Decimal(str(item.get("fill_count") or "0")) > 0]
        if filled_orders:
            total_cost = sum(
                Decimal(str(item.get("fill_count") or "0")) * Decimal(str(item.get("average_fill_price") or self.config["entry_price"]))
                for item in filled_orders
            )
            self.state["average_entry"] = format(total_cost / total, "f") if total else None
        return total

    def refresh_shadow_entry(self, feed: KalshiLiveFeed, record: dict[str, Any]) -> Decimal:
        """Conservative public-trade-through evidence for a shadow maker fill.

        It is deliberately an observation model, not a claim that our order
        was actually at the front of the exchange queue.  Only public trades
        after submission at or through the limit can consume the simulated
        order, and the evidence is kept in the separate shadow ledger.
        """

        order = next(iter(record.get("entry_orders", [])), None)
        if not isinstance(order, dict):
            return Decimal("0")
        try:
            created = datetime.fromisoformat(str(order["submitted_at"]).replace("Z", "+00:00"))
            events = feed.public_trades_after(record["ticker"], created)
        except (AttributeError, KeyError, TypeError, ValueError):
            return Decimal(str(order.get("fill_count") or "0"))
        side = str(record["signal_side"])
        limit = Decimal(self.config["entry_price"])
        eligible = []
        for event in events:
            raw_price = event.get(f"{side}_price")
            raw_count = event.get("count")
            try:
                price, count = Decimal(str(raw_price)), Decimal(str(raw_count))
            except Exception:
                continue
            if Decimal("0") < price <= limit and count > 0:
                eligible.append({"trade_id": event.get("trade_id"), "price": format(price, "f"), "count": format(count, "f")})
        evidence_count = sum((Decimal(item["count"]) for item in eligible), Decimal("0"))
        requested = Decimal(str(order.get("quantity") or record.get("intended_quantity") or "0"))
        fill = round_shares(min(requested, evidence_count))
        previous_fill = Decimal(str(order.get("fill_count") or "0"))
        if self.dry_run and fill > previous_fill:
            affordable_delta = (self.shadow_available_cash() / limit).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
            fill = min(fill, previous_fill + affordable_delta)
            reserve_delta = max(Decimal("0"), fill - previous_fill) * limit
            metrics = self.shadow_metrics()
            reserved = Decimal(str(metrics["reserved_cash"])) + reserve_delta
            metrics["reserved_cash"] = format(reserved, "f")
            metrics["max_reserved_cash"] = format(max(Decimal(str(metrics["max_reserved_cash"])), reserved), "f")
        order.update({
            "fill_count": format(fill, "f"), "remaining_count": format(round_shares(requested - fill), "f"),
            "average_fill_price": self.config["entry_price"], "fees_paid": "0",
            "shadow_fill_evidence": {"model": "conservative_trade_through", "eligible_trades": eligible, "eligible_trade_quantity": format(evidence_count, "f")},
        })
        record["actual_quantity"] = format(fill, "f")
        self.state["current_position"] = record["actual_quantity"]
        self.state["average_entry"] = self.config["entry_price"] if fill else None
        return fill

    async def submit_entry(self, rest: KalshiREST, feed: KalshiLiveFeed, record: dict[str, Any], now: float) -> None:
        if record.get("status") != "SIGNAL_PENDING" or not self.circuit_allows_entry():
            return
        if now > float(record["market_open_epoch"]) + int(self.config["entry_lateness_seconds"]):
            self.transition(record, "MISSED_SIGNAL", "entry_lateness_exceeded")
            return
        side = str(record["signal_side"])
        ask = self.selected_quote(feed, record["ticker"], side, "ask")
        if ask is None:
            return
        if ask <= Decimal(self.config["stop_price"]):
            # Do not create an order that would enter at/under the stop.
            self.transition(record, "ZERO_FILL", "selected_side_started_at_or_below_stop")
            self.state["sizing"] = zero_fill_snapshot(self.current_parameters(), self.state.get("sizing"))
            self.note_zero_fill()
            self.audit("zero_fill", ticker=record["ticker"], reason="selected_side_started_at_or_below_stop", selected_ask=format(ask, "f"))
            return
        if ask <= Decimal(self.config["entry_price"]):
            # A post-only order at this price would cross the displayed book.
            # Do not convert the planned maker entry into an accidental taker
            # fill and do not reprice/chase it.
            self.transition(record, "ZERO_FILL", "post_only_would_cross_current_book")
            self.state["sizing"] = zero_fill_snapshot(self.current_parameters(), self.state.get("sizing"))
            self.note_zero_fill()
            self.audit("zero_fill", ticker=record["ticker"], reason="post_only_would_cross_current_book", selected_ask=format(ask, "f"))
            return
        balance = self.shadow_available_cash() if self.dry_run else await rest.balance_decimal()
        quantity = Decimal(str(record["intended_quantity"]))
        required = quantity * Decimal(self.config["entry_price"])
        if self.dry_run:
            metrics = self.shadow_metrics()
            metrics["max_required_cash"] = format(max(Decimal(str(metrics["max_required_cash"])), required), "f")
        if balance is None or balance < required:
            record["funding_failure"] = {"at": utc_now(), "available_balance": None if balance is None else format(balance, "f"), "required_cash": format(required, "f"), "quantity": format(quantity, "f")}
            self.transition(record, "FUNDING_FAILURE", "insufficient_authenticated_cash")
            if self.dry_run:
                self.shadow_metrics()["funding_failures"] = int(self.shadow_metrics()["funding_failures"]) + 1
            self.audit("funding_failure", ticker=record["ticker"], **record["funding_failure"])
            return
        existing = await rest.position_for_ticker(record["ticker"])
        if existing is None or abs(Decimal(str(existing))) > 0:
            self.trip("existing_position_before_entry")
            return
        client_id = deterministic_client_order_id(record["ticker"], side, "entry", self.config)
        expiry = min(int(record["market_close_epoch"]) - 1, int(now) + int(self.config["entry_timeout_seconds"]))
        if expiry <= int(now):
            self.transition(record, "MISSED_SIGNAL", "entry_expiry_elapsed")
            return
        order = await rest.create_order(
            ticker=record["ticker"], side=side, position_price=float(Decimal(self.config["entry_price"])),
            quantity=float(quantity), tif="good_till_canceled", expiration_time=expiry, dry_run=self.dry_run,
            order_key="hybrid-entry", post_only=True, client_order_id_override=client_id,
        )
        # A type/API incompatibility cannot degrade to a non-post-only entry.
        if order.get("status") in {"submit_failed", "paused"}:
            self.transition(record, "ERROR_RECONCILIATION", "post_only_entry_not_accepted")
            return
        record["entry_orders"].append(order)
        self.state["current_order_id"] = order.get("order_id")
        record["entry_deadline_epoch"] = expiry
        self.transition(record, "ENTRY_PENDING", "post_only_limit_submitted")
        self.audit("entry_submitted", ticker=record["ticker"], side=side, requested_quantity=format(quantity, "f"), requested_price=self.config["entry_price"], client_order_id=client_id, exchange_order_id=order.get("order_id"), post_only=True)

    def entry_cost(self, record: dict[str, Any]) -> Decimal:
        cost = Decimal("0")
        for order in record.get("entry_orders", []):
            filled = Decimal(str(order.get("fill_count") or "0"))
            average = Decimal(str(order.get("average_fill_price") or self.config["entry_price"]))
            fees = Decimal(str(order.get("fees_paid") or "0"))
            cost += filled * average + fees
        return cost

    async def manage_entry(self, rest: KalshiREST, feed: KalshiLiveFeed, record: dict[str, Any], now: float) -> None:
        if record.get("status") not in {"ENTRY_PENDING", "ENTRY_PARTIAL"}:
            return
        filled = self.refresh_shadow_entry(feed, record) if self.dry_run else await self.refresh_entry(rest, record)
        if filled > 0:
            self.transition(record, "ENTRY_PARTIAL" if any(Decimal(str(item.get("remaining_count") or "0")) > 0 for item in record["entry_orders"]) else "POSITION_OPEN", "entry_fill_observed")
        side = str(record["signal_side"])
        ask = self.selected_quote(feed, record["ticker"], side, "ask")
        deadline = float(record.get("entry_deadline_epoch") or 0)
        if (ask is not None and ask <= Decimal(self.config["stop_price"])) or now >= deadline or now >= float(record["market_close_epoch"]):
            for order in record.get("entry_orders", []):
                await rest.cancel_order(order, self.dry_run)
            # For shadow mode, preserve only the trade-through evidence that
            # existed before cancellation; later public trades cannot fill a
            # cancelled simulated maker order.
            filled = filled if self.dry_run else await self.refresh_entry(rest, record)
            if filled == 0:
                self.transition(record, "ZERO_FILL", "entry_window_expired")
                self.state["sizing"] = zero_fill_snapshot(self.current_parameters(), self.state.get("sizing"))
                self.note_zero_fill()
                self.audit("zero_fill", ticker=record["ticker"], reason="entry_window_expired")
            else:
                self.transition(record, "POSITION_OPEN", "entry_remainder_canceled")

    async def close_at_stop(self, rest: KalshiREST, record: dict[str, Any], executable_bid: Decimal) -> None:
        side = str(record["signal_side"])
        for order in record.get("entry_orders", []):
            await rest.cancel_order(order, self.dry_run)
        if self.dry_run:
            quantity = Decimal(str(record.get("actual_quantity") or "0"))
            if quantity == 0:
                return
            record.setdefault("exit_orders", []).append({
                "order_id": None, "fill_count": format(quantity, "f"), "remaining_count": "0",
                "average_fill_price": format(executable_bid, "f"), "fees_paid": "0",
                "shadow_execution": "fresh_executable_bid",
            })
            record["stop_trigger"] = {"at": utc_now(), "best_executable_bid": format(executable_bid, "f"), "requested_quantity": format(quantity, "f"), "shadow": True}
            self.transition(record, "STOP_PENDING", "shadow_40c_executable_bid")
            await self.finalize_stop(record)
            return
        exchange_position = await rest.position_for_ticker(record["ticker"])
        if exchange_position is None:
            self.trip("stop_position_reconciliation_failed")
            return
        quantity = abs(Decimal(str(exchange_position)))
        if quantity == 0:
            await self.finalize_stop(record)
            return
        if (side == "yes" and exchange_position < 0) or (side == "no" and exchange_position > 0):
            self.trip("stop_position_direction_mismatch")
            return
        prior = record.get("exit_orders", [])
        if prior and Decimal(str(prior[-1].get("remaining_count") or "0")) > 0:
            return
        order = await rest.create_reduce_only_exit(
            ticker=record["ticker"], held_side=side, economic_exit_price=float(executable_bid), quantity=float(quantity),
            dry_run=self.dry_run, order_key=f"hybrid-stop-{len(prior)}",
            client_order_id_override=deterministic_client_order_id(record["ticker"], side, f"stop-{len(prior)}", self.config),
        )
        record.setdefault("exit_orders", []).append(order)
        record["stop_trigger"] = {"at": utc_now(), "best_executable_bid": format(executable_bid, "f"), "requested_quantity": format(quantity, "f")}
        self.transition(record, "STOP_PENDING", "40c_executable_bid")
        self.audit("stop_triggered", ticker=record["ticker"], side=side, executable_bid=format(executable_bid, "f"), quantity=format(quantity, "f"), exchange_order_id=order.get("order_id"))

    def exit_proceeds(self, record: dict[str, Any]) -> tuple[Decimal, Decimal]:
        proceeds = fees = Decimal("0")
        for order in record.get("exit_orders", []):
            filled = Decimal(str(order.get("fill_count") or "0"))
            proceeds += filled * Decimal(str(order.get("average_fill_price") or "0"))
            fees += Decimal(str(order.get("fees_paid") or "0"))
        return proceeds, fees

    def record_realized(self, record: dict[str, Any], net: Decimal, method: str, settlement_id: str) -> None:
        if settlement_id in self.state["processed_settlements"]:
            return
        parameters = self.record_parameters(record)
        before = dict(self.state.get("sizing") or {})
        after, changes = apply_realized_filled_trade(parameters, before, net)
        self.state["sizing"] = after
        if Decimal(str(after["recovery_cycle_pnl"])) < 0:
            self.state["cycle_strategy_parameters"] = parameters.as_dict()
        else:
            self.state.pop("cycle_strategy_parameters", None)
        today = datetime.now(timezone.utc).date().isoformat()
        self.state.setdefault("daily_realized", {})[today] = format(Decimal(str(self.state.get("daily_realized", {}).get(today, "0"))) + net, "f")
        cumulative = Decimal(str(self.state.get("cumulative_realized_pnl", "0"))) + net
        self.state["cumulative_realized_pnl"] = format(cumulative, "f")
        self.state["peak_equity"] = format(max(Decimal(str(self.state.get("peak_equity", "0"))), cumulative), "f")
        if self.dry_run:
            metrics = self.shadow_metrics()
            balance = Decimal(str(metrics["balance"])) + net
            peak = max(Decimal(str(metrics["peak_balance"])), balance)
            entry_cash = self.entry_cost(record)
            metrics["balance"] = format(balance, "f")
            metrics["peak_balance"] = format(peak, "f")
            metrics["reserved_cash"] = format(max(Decimal("0"), Decimal(str(metrics["reserved_cash"])) - entry_cash), "f")
            metrics["max_drawdown"] = format(max(Decimal(str(metrics["max_drawdown"])), peak - balance), "f")
            metrics["completed_trades"] = int(metrics["completed_trades"]) + 1
            count_key = "stop_count" if method == "stop" else "settlement_count"
            metrics[count_key] = int(metrics[count_key]) + 1
        append_unique(self.state["processed_settlements"], settlement_id)
        record.update({
            "realized_net_pnl": format(net, "f"), "realized_method": method, "completed_at": utc_now(),
            "recovery_cycle_pnl_after": after["recovery_cycle_pnl"], "recovery_exponent_after": after["recovery_exponent"],
            "base_after": after["base_share_count"], "next_base_threshold_after": after["next_base_threshold"],
        })
        self.transition(record, "CLOSED", method)
        if self.state.get("active_market") == record["ticker"]:
            self.state["active_market"] = None
        self.state.update({"current_order_id": None, "current_position": "0.00", "average_entry": None, "last_completed_trade": record["ticker"]})
        self.audit("trade_closed", ticker=record["ticker"], method=method, net_pnl=format(net, "f"), quantity=record.get("actual_quantity"), recovery_reset=changes["recovery_reset"], base_increased=changes["base_increased"])

    async def finalize_stop(self, record: dict[str, Any]) -> None:
        proceeds, exit_fees = self.exit_proceeds(record)
        net = proceeds - exit_fees - self.entry_cost(record)
        self.record_realized(record, net, "stop", f"{record['ticker']}:stop")

    async def manage_stop(self, rest: KalshiREST, feed: KalshiLiveFeed, record: dict[str, Any]) -> None:
        if record.get("status") not in {"ENTRY_PARTIAL", "POSITION_OPEN", "STOP_PENDING"}:
            return
        if record.get("status") == "STOP_PENDING":
            for order in record.get("exit_orders", []):
                await rest.refresh_exit_order(order)
            position = await rest.position_for_ticker(record["ticker"])
            if position is not None and abs(Decimal(str(position))) == 0:
                await self.finalize_stop(record)
            elif position is not None:
                # Reduce-only IOC can partially fill.  Continue flattening
                # only the exchange-confirmed residual; never reverse it.
                bid = self.selected_quote(feed, record["ticker"], str(record["signal_side"]), "bid")
                if bid is not None:
                    await self.close_at_stop(rest, record, bid)
            return
        bid = self.selected_quote(feed, record["ticker"], str(record["signal_side"]), "bid")
        if bid is not None and bid <= Decimal(self.config["stop_price"]):
            await self.close_at_stop(rest, record, bid)

    async def settle(self, rest: KalshiREST, record: dict[str, Any], now: float) -> None:
        if record.get("status") not in {"ENTRY_PARTIAL", "POSITION_OPEN", "SETTLEMENT_PENDING"} or now < float(record["market_close_epoch"]):
            return
        for order in record.get("entry_orders", []):
            await rest.cancel_order(order, self.dry_run)
        market = await rest.get_market(record["ticker"])
        outcome = market_result(market) if market is not None else None
        if outcome not in {"yes", "no"}:
            self.transition(record, "SETTLEMENT_PENDING", "awaiting_official_settlement")
            return
        quantity = Decimal(str(record.get("actual_quantity") or "0"))
        payout = quantity if outcome == record.get("signal_side") else Decimal("0")
        net = payout - self.entry_cost(record)
        record["settlement_outcome"] = outcome
        self.record_realized(record, net, "settlement", f"{record['ticker']}:settlement:{outcome}")

    async def reconcile_active(self, rest: KalshiREST, feed: KalshiLiveFeed, now: float) -> None:
        for record in list(self.state["markets"].values()):
            if not isinstance(record, dict):
                continue
            await self.verify_previous_outcome(rest, record)
            await self.manage_entry(rest, feed, record, now)
            await self.manage_stop(rest, feed, record)
            await self.settle(rest, record, now)

    async def run(self, rest: KalshiREST, feed: KalshiLiveFeed, run_seconds: float, reconcile_only: bool) -> int:
        if not await self.reconcile_startup(rest):
            self.checkpoint()
            return 2
        if reconcile_only:
            self.checkpoint()
            LOG.warning("RECONCILE_ONLY COMPLETE | no entry endpoint was called")
            return 0
        start = time.monotonic()
        last_update = feed.update_count
        while time.monotonic() - start < run_seconds:
            now = time.time()
            if time.monotonic() - self.last_market_discovery >= 10.0:
                await self.discover(rest)
                self.last_market_discovery = time.monotonic()
            active = self.active_market(now)
            if active:
                previous = self.predecessor(active)
                upcoming = self.successor(active)
                # While the active market approaches close, its subscription
                # supplies the final quote for the next immediate signal; the
                # successor is preloaded so order preparation has no discovery
                # race at the exchange boundary.
                subscribed = [active["ticker"]] + ([previous["ticker"]] if previous else []) + ([upcoming["ticker"]] if upcoming else [])
                feed.set_tickers(subscribed)
                for ticker in subscribed:
                    self.tracker.observe_feed(feed, ticker)
                # Persist a final-quote provisional result as soon as it is
                # available in the last second before close.  This makes a
                # restart between the old close and new entry auditable rather
                # than depending on in-memory websocket history.
                if active["close_epoch"] - 1.0 <= now <= active["close_epoch"]:
                    closing = self.tracker.infer(active["ticker"], active["close_epoch"])
                    if closing is not None:
                        self.state.setdefault("provisional_outcomes", {})[active["ticker"]] = closing
                        self.audit("provisional_outcome_frozen", ticker=active["ticker"], outcome=closing["outcome"], method=closing["method"], quote_age=closing["quote_age_seconds"])
                        self.checkpoint("provisional_outcome")
                if previous and now >= active["open_epoch"]:
                    provisional = self.state.get("provisional_outcomes", {}).get(previous["ticker"])
                    if not isinstance(provisional, dict):
                        provisional = self.tracker.infer(previous["ticker"], active["open_epoch"])
                    if provisional is None:
                        # Level 3/4 fallback: an official result is allowed,
                        # but no older market and no guessed signal are used.
                        source_market = await rest.get_market(previous["ticker"])
                        official = market_result(source_market) if source_market is not None else None
                        if official in {"yes", "no"} and now <= active["open_epoch"] + int(self.config["entry_lateness_seconds"]):
                            provisional = {"outcome": official, "ticker": previous["ticker"], "timestamp": utc_now(), "method": "official_rest_fallback", "quote_age_seconds": None}
                    if provisional is not None:
                        record = self.set_signal(active, provisional)
                        await self.submit_entry(rest, feed, record, now)
            if time.monotonic() - self.last_reconcile >= float(self.config["reconciliation_interval"]):
                await self.reconcile_active(rest, feed, now)
                self.last_reconcile = time.monotonic()
            if time.monotonic() - self.last_heartbeat >= 60:
                sizing = sizing_state(self.current_parameters(), self.state.get("sizing"))
                LOG.warning("HEARTBEAT | mode=%s ticker=%s state=%s base=%s exponent=%d target=%s deficit=%s threshold=%s active=%s breaker=%s", "DRY_RUN" if self.dry_run else "LIVE", active and active["ticker"], (self.state.get("markets", {}).get(active["ticker"], {}) if active else {}).get("status"), sizing.base_share_count, sizing.recovery_exponent, sizing.prescribed_quantity(), sizing.recovery_cycle_pnl, sizing.next_base_threshold, self.state.get("active_market"), self.state["circuit_breaker"].get("blocked"))
                self.last_heartbeat = time.monotonic()
            self.checkpoint()
            last_update = await feed.wait_for_update(0.25, last_update)
        return 0


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--config", type=Path, default=Path("live_strategy_config.json"))
    result.add_argument("--state-file", type=Path, default=Path("data/kalshi_live_strategy_state.json"))
    result.add_argument("--audit-ledger", type=Path, default=Path("data/kalshi_live_strategy_audit.jsonl"))
    result.add_argument("--run-seconds", type=float, default=19_200)
    result.add_argument("--persist-config", action="store_true")
    result.add_argument("--reconcile-only", action="store_true")
    result.add_argument("--cancel-managed-entries", action="store_true", help="explicitly cancel only hybrid-prefixed resting orders; never opens or closes a position")
    result.add_argument("--reset-state", action="store_true")
    result.add_argument("--live-enabled", action="store_true")
    result.add_argument("--dry-run", action="store_true")
    for name in sorted(DECIMAL_CONFIG_FIELDS | INTEGER_CONFIG_FIELDS | FLOAT_CONFIG_FIELDS):
        result.add_argument("--" + name.replace("_", "-"), dest=name)
    result.add_argument("--allow-capital-downsize", action=argparse.BooleanOptionalAction, default=None)
    return result


async def async_main(args: argparse.Namespace) -> int:
    config = apply_overrides(load_config(args.config), args)
    if args.persist_config:
        save_config(args.config, config)
    requested_live = bool(args.live_enabled)
    environment_live = _bool(os.getenv("KALSHI_LIVE_ENABLED", "false"))
    live = requested_live and environment_live and not args.dry_run
    dry_run = not live
    LOG.warning("MODE=%s | strategy=%s config_hash=%s", "LIVE" if live else "DRY_RUN", config["strategy_version"], config_hash(config)[:12])
    api_key = os.getenv("KALSHI_API_KEY_ID", "")
    pem_path = Path(os.getenv("KALSHI_PEM_PATH", "kalshi_private_key.pem"))
    if not api_key or not pem_path.exists():
        raise SystemExit("Kalshi authentication is required; credentials are never logged")
    state = load_state(args.state_file, config)
    if args.reset_state:
        if state.get("active_market") or any(item.get("status") in ACTIVE_STATES for item in state.get("markets", {}).values() if isinstance(item, dict)):
            raise SystemExit("refusing reset_state with active local strategy exposure; use reconciliation instead")
    rest = KalshiREST(api_key, pem_path, _bool(os.getenv("KALSHI_DEMO", "false")))
    if args.reset_state:
        # Resetting a local file never authorizes forgetting exchange exposure.
        # Check the authoritative portfolio before replacing even an apparently
        # idle local state.
        positions = await rest.portfolio.get_positions(limit=1000)
        active_positions = [
            position for position in (field(positions, "market_positions", "positions") or [])
            if str(field(position, "ticker") or "").startswith(config["series"] + "-")
            and Decimal(str(field(position, "position_fp", "position") or "0")) != 0
        ]
        if active_positions or await LiveEngine(config, state, args.state_file, args.audit_ledger, dry_run).managed_orders(rest):
            await rest.close()
            raise SystemExit("refusing reset_state while exchange KXBTC15M exposure or managed orders exist")
        state = load_state(Path("/nonexistent"), config)
    feed = KalshiLiveFeed(rest.auth)
    feed_task = asyncio.create_task(feed.run(), name="kalshi-hybrid-live-feed")
    engine = LiveEngine(config, state, args.state_file, args.audit_ledger, dry_run, args.config)
    try:
        if args.cancel_managed_entries:
            await engine.cancel_managed_entries(rest)
            return 0
        return await engine.run(rest, feed, args.run_seconds, args.reconcile_only)
    finally:
        engine.checkpoint()
        feed_task.cancel()
        await asyncio.gather(feed_task, return_exceptions=True)
        await rest.close()


def main() -> int:
    configure_logging()
    return asyncio.run(async_main(parser().parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
