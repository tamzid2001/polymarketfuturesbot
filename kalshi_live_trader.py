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
    effective_stop_price,
    full_snapshot,
    prescribed_quantity,
    round_shares,
    sizing_state,
    sticky_directional_prediction,
    zero_fill_snapshot,
)


LOG = logging.getLogger("kalshi_live_trader")
ORDER_NAMESPACE = uuid.UUID("602ca251-d5dc-43c7-ae11-a6be6f19a43b")
ORDER_PREFIX = "kxbtc15m-hybrid-v1-"
# This is an execution contract, not a cosmetic label.  A worker may never
# silently reinterpret an older selected configuration after a restart or a
# watchdog handoff.  Bump both values deliberately with a reviewed migration
# whenever the shared live/backtest strategy semantics change.
# v10 is a hard compatibility boundary for status-filtered active-market
# discovery, immediate protected-IOC entry, and fixed-floor stop contract.
# An older worker cannot silently load this
# configuration; it fails closed before it can submit an order.
ACTIVE_STRATEGY_VERSION = "kxbtc15m-hybrid-live-v10"
ACTIVE_CONFIG_SCHEMA_VERSION = 10
TERMINAL_STATES = {"CLOSED", "ZERO_FILL", "FUNDING_FAILURE", "MISSED_SIGNAL", "ERROR_RECONCILIATION"}
# A cancellation acknowledgement or an order-submission response can be
# uncertain.  These are deliberately active, risk-managed states: they block
# every new entry, are retried through exchange reconciliation, and must not
# be treated like a terminal bookkeeping error while an exchange position may
# still exist.
ACTIVE_STATES = {
    "SIGNAL_PENDING", "ENTRY_PENDING", "ENTRY_PARTIAL", "POSITION_OPEN", "STOP_PENDING", "SETTLEMENT_PENDING",
    "ENTRY_CANCEL_UNCONFIRMED", "RECONCILIATION_PENDING", "ACCOUNTING_RECONCILIATION_PENDING",
}


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )


def _bool(value: Any) -> bool:
    return str(value).lower() in {"1", "true", "yes", "on"}


def live_mode_allowed(
    requested_live: bool, environment_live: bool, shadow_only_lock: bool, dry_run_requested: bool,
) -> bool:
    """Return true only for the deliberately enabled real-money path.

    Keeping this gate pure gives the repository a regression test that a
    future workflow/config change cannot turn a shadow run live accidentally.
    """

    return requested_live and environment_live and not shadow_only_lock and not dry_run_requested


def _iso_epoch(value: Any) -> float | None:
    """Parse a persisted UTC timestamp for non-accounting telemetry only."""

    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.timestamp()
    except (TypeError, ValueError):
        return None


def _seconds_since(start: float | None, end: float) -> float | None:
    """Return a non-negative telemetry duration, never a clock artefact."""

    if start is None:
        return None
    return round(max(0.0, end - start), 6)


def _decimal_string(value: Any, name: str) -> str:
    amount = decimal(value)
    if amount <= Decimal("0"):
        raise ValueError(f"{name} must be positive")
    return format(amount, "f")


DECIMAL_CONFIG_FIELDS = {
    "entry_price", "stop_price", "starting_base", "recovery_multiplier", "first_base_threshold",
    "threshold_growth_multiplier", "base_increment", "max_position", "provisional_outcome_threshold",
    "max_recovery_cycle_loss", "max_daily_realized_loss", "starting_shadow_balance", "maker_price_offset",
    "stop_baseline_entry_price",
}
INTEGER_CONFIG_FIELDS = {
    "signal_delay_seconds", "entry_timeout_seconds", "entry_lateness_seconds", "outcome_observation_seconds",
    "max_recovery_exponent", "max_api_failures", "handoff_guard_seconds", "opening_quote_max_observations",
    "opening_price_discovery_seconds",
}
FLOAT_CONFIG_FIELDS = {
    "stop_poll_interval", "reconciliation_interval", "max_outcome_quote_age_seconds", "max_stale_quote_seconds",
    "durable_checkpoint_interval_seconds", "market_discovery_interval_seconds",
}

# These profile names are deliberately part of the durable configuration hash.
# A 30c replay must never inherit recovery/base state from a 40c replay merely
# because both happen to observe the same Kalshi account in dry-run mode.
SHADOW_STOP_PROFILE_PRICES = {
    "sticky_stop_40": Decimal("0.40"),
    "sticky_stop_30": Decimal("0.30"),
    "sticky_stop_20": Decimal("0.20"),
    "sticky_stop_10": Decimal("0.10"),
}


def assert_active_strategy_contract(value: dict[str, Any]) -> None:
    """Fail closed rather than load a legacy or underspecified live config."""

    if value.get("strategy_version") != ACTIVE_STRATEGY_VERSION:
        raise ValueError(
            "refusing non-current live strategy configuration; "
            f"expected strategy_version={ACTIVE_STRATEGY_VERSION!r}"
        )
    try:
        schema_version = int(value.get("config_schema_version"))
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "refusing live configuration without the current config_schema_version"
        ) from exc
    if schema_version != ACTIVE_CONFIG_SCHEMA_VERSION:
        raise ValueError(
            "refusing non-current live configuration schema; "
            f"expected config_schema_version={ACTIVE_CONFIG_SCHEMA_VERSION}"
        )


def validate_entry_price_contract(value: dict[str, Any]) -> None:
    """Validate the historical reference and v9 protected-IOC/fixed-stop rule."""

    entry = decimal(value["entry_price"])
    offset = decimal(value["maker_price_offset"])
    stop = decimal(value["stop_price"])
    stop_baseline = decimal(value["stop_baseline_entry_price"])
    if not stop < entry < Decimal("1"):
        raise ValueError("reference entry_price must satisfy stop < entry_price < 1")
    if value.get("entry_execution_mode") != "immediate_market_ioc":
        raise ValueError("the active strategy requires entry_execution_mode=immediate_market_ioc")
    if value.get("stop_policy") != "fixed_profile_floor":
        raise ValueError("the active strategy requires stop_policy=fixed_profile_floor")
    if stop_baseline != Decimal("0.50"):
        raise ValueError("the hybrid strategy requires stop_baseline_entry_price to equal exactly 0.50")
    profile = str(value.get("shadow_profile") or "sticky_stop_40")
    expected_profile_stop = SHADOW_STOP_PROFILE_PRICES.get(profile)
    if expected_profile_stop is None:
        raise ValueError(
            "shadow_profile must be one of " + ", ".join(sorted(SHADOW_STOP_PROFILE_PRICES))
        )
    if stop != expected_profile_stop:
        raise ValueError(
            f"shadow_profile={profile} requires stop_price={format(expected_profile_stop, 'f')}"
        )
    # The old maker-only controls remain telemetry-only fields so archived
    # v8 evidence is readable, but they may never alter v9 execution.
    if offset < Decimal("0"):
        raise ValueError("maker_price_offset cannot be negative")
    if int(value["opening_quote_max_observations"]) < 1:
        raise ValueError("opening_quote_max_observations must be at least one")


def load_config(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("live strategy configuration must be a JSON object")
    required = {
        "strategy_version", "series", "entry_price", "stop_price", "starting_base", "recovery_multiplier",
        "first_base_threshold", "threshold_growth_multiplier", "base_increment", "max_position",
        "stop_policy", "stop_baseline_entry_price", "entry_execution_mode",
    }
    missing = required - value.keys()
    if missing:
        raise ValueError(f"live strategy configuration is missing: {', '.join(sorted(missing))}")
    assert_active_strategy_contract(value)
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
    value.setdefault("shadow_fill_model", "fresh_displayed_top_of_book_ioc")
    value.setdefault("starting_shadow_balance", "1000.00")
    value.setdefault("maker_price_offset", "0.01")
    value.setdefault("stop_policy", "fixed_profile_floor")
    value.setdefault("entry_execution_mode", "immediate_market_ioc")
    value.setdefault("stop_baseline_entry_price", "0.50")
    value.setdefault("signal_delay_seconds", 0)
    value.setdefault("signal_mode", "sticky_until_directional_win")
    value.setdefault("shadow_profile", "sticky_stop_40")
    # A fresh executable book is required for the protected IOC.  The runner
    # may wait through ``entry_lateness_seconds`` for that book, but never
    # post a resting maker order or substitute an older execution path.
    value.setdefault("entry_timeout_seconds", 60)
    value.setdefault("entry_lateness_seconds", 60)
    value.setdefault("stop_poll_interval", 1.0)
    value.setdefault("reconciliation_interval", 5.0)
    value.setdefault("market_discovery_interval_seconds", 1.0)
    value.setdefault("outcome_observation_seconds", 5)
    value.setdefault("provisional_outcome_threshold", "0.99")
    value.setdefault("max_outcome_quote_age_seconds", 2.0)
    value.setdefault("max_stale_quote_seconds", 2.0)
    value.setdefault("max_recovery_exponent", 12)
    value.setdefault("max_recovery_cycle_loss", "50.00")
    value.setdefault("max_daily_realized_loss", "25.00")
    value.setdefault("max_api_failures", 5)
    value.setdefault("handoff_guard_seconds", 60)
    value.setdefault("opening_quote_max_observations", 500)
    value.setdefault("opening_price_discovery_seconds", 3)
    # State and audit writes are fsynced locally for every material event.
    # This only bounds GitHub checkpoint publication, avoiding a Git push for
    # every market-data update while preserving a short handoff-loss window.
    value.setdefault("durable_checkpoint_interval_seconds", 5.0)
    if value["shadow_fill_model"] != "fresh_displayed_top_of_book_ioc":
        raise ValueError("v9 shadow mode requires fresh_displayed_top_of_book_ioc")
    if value["signal_mode"] != "sticky_until_directional_win":
        raise ValueError("the active shadow strategy requires signal_mode=sticky_until_directional_win")
    validate_entry_price_contract(value)
    if decimal(value["starting_base"]) != Decimal("1.00"):
        raise ValueError("the hybrid strategy must start at exactly 1.00 share")
    if not 1 <= int(value["entry_timeout_seconds"]) <= 60:
        raise ValueError("entry_timeout_seconds must be between 1 and 60")
    if int(value["handoff_guard_seconds"]) < 60:
        raise ValueError("handoff_guard_seconds must keep at least one minute clear at each market boundary")
    if not 1.0 <= float(value["durable_checkpoint_interval_seconds"]) <= 60.0:
        raise ValueError("durable_checkpoint_interval_seconds must be between 1 and 60")
    if not 0.25 <= float(value["market_discovery_interval_seconds"]) <= 10.0:
        raise ValueError("market_discovery_interval_seconds must be between 0.25 and 10")
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
    shadow_profile = getattr(args, "shadow_profile", None)
    if shadow_profile not in (None, ""):
        updated["shadow_profile"] = str(shadow_profile)
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
    required = {"strategy_version", "series", "entry_price", "stop_price", "starting_base", "recovery_multiplier", "first_base_threshold", "threshold_growth_multiplier", "base_increment", "max_position", "stop_policy", "stop_baseline_entry_price", "entry_execution_mode"}
    if required - temporary.keys():
        raise ValueError("invalid overridden live strategy configuration")
    assert_active_strategy_contract(temporary)
    # Reuse the normal rules without doing a file round-trip.
    if temporary["series"] != "KXBTC15M" or decimal(temporary["starting_base"]) != Decimal("1.00"):
        raise ValueError("invalid strategy series or starting base")
    temporary.setdefault("shadow_fill_model", "fresh_displayed_top_of_book_ioc")
    temporary.setdefault("starting_shadow_balance", "1000.00")
    temporary.setdefault("maker_price_offset", "0.01")
    temporary.setdefault("stop_policy", "fixed_profile_floor")
    temporary.setdefault("entry_execution_mode", "immediate_market_ioc")
    temporary.setdefault("stop_baseline_entry_price", "0.50")
    temporary.setdefault("signal_mode", "sticky_until_directional_win")
    temporary.setdefault("shadow_profile", "sticky_stop_40")
    temporary.setdefault("entry_timeout_seconds", 60)
    temporary.setdefault("entry_lateness_seconds", 60)
    temporary.setdefault("handoff_guard_seconds", 60)
    temporary.setdefault("opening_quote_max_observations", 500)
    temporary.setdefault("opening_price_discovery_seconds", 3)
    temporary.setdefault("durable_checkpoint_interval_seconds", 5.0)
    temporary.setdefault("market_discovery_interval_seconds", 1.0)
    validate_entry_price_contract(temporary)
    if temporary["shadow_fill_model"] != "fresh_displayed_top_of_book_ioc":
        raise ValueError("v9 shadow mode requires fresh_displayed_top_of_book_ioc")
    if temporary["signal_mode"] != "sticky_until_directional_win":
        raise ValueError("the active shadow strategy requires signal_mode=sticky_until_directional_win")
    if not 1 <= int(temporary["entry_timeout_seconds"]) <= 60:
        raise ValueError("entry_timeout_seconds must be between 1 and 60")
    if int(temporary["handoff_guard_seconds"]) < 60:
        raise ValueError("handoff_guard_seconds must keep at least one minute clear at each market boundary")
    if not 1.0 <= float(temporary["durable_checkpoint_interval_seconds"]) <= 60.0:
        raise ValueError("durable_checkpoint_interval_seconds must be between 1 and 60")
    if not 0.25 <= float(temporary["market_discovery_interval_seconds"]) <= 10.0:
        raise ValueError("market_discovery_interval_seconds must be between 0.25 and 10")
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
        self.publisher = MaterialCheckpointPublisher(
            *checkpoint_paths,
            minimum_interval_seconds=float(config["durable_checkpoint_interval_seconds"]),
        )

    def checkpoint(self, reason: str | None = None) -> None:
        save_state(self.state_path, self.state)
        if reason:
            self.publisher.publish_if_changed(reason)
        else:
            # A material audit which was coalesced inside the remote-publish
            # interval is flushed by the ordinary live-loop checkpoints.  Do
            # not publish every quote/state timestamp when nothing material
            # is pending.
            self.publisher.publish_if_due()

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
        metrics.setdefault("maker_limit_fill_markets", 0)
        metrics.setdefault("market_ioc_fill_markets", 0)
        metrics.setdefault("mixed_entry_markets", 0)
        metrics.setdefault("maker_limit_filled_quantity", "0.00")
        metrics.setdefault("market_ioc_filled_quantity", "0.00")
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
        # An append is fsynced by ``append_audit``.  Immediately pair it with
        # the atomic state file so a crash cannot leave a fresh ledger event
        # behind only an old local strategy snapshot.  The publisher itself
        # coalesces GitHub commits at the configured interval; local safety
        # never waits on that network operation.
        self.checkpoint(f"audit:{event}")

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
        """Discover the exchange's active market without scanning future pages.

        The unfiltered endpoint is ordered through a large initialized-future
        inventory.  A ``limit=1000`` scan can therefore omit the market that
        is trading now, leaving the worker idle even with a healthy API.  The
        exchange-supported ``status=open`` filter is the authoritative
        current-market discovery mechanism.  Recently settled markets are
        merged as an API-provided startup fallback; markets seen while active
        remain in the rolling cache to preserve the just-ended quote source at
        the next boundary.
        """
        try:
            active_payload = await rest.get_raw_json(
                "/markets", {"series_ticker": self.config["series"], "status": "open", "limit": 10},
            )
            settled_payload = await rest.get_raw_json(
                "/markets", {"series_ticker": self.config["series"], "status": "settled", "limit": 10},
            )
            candidates = [
                market_metadata(value)
                for payload in (active_payload, settled_payload)
                for value in payload.get("markets", []) if isinstance(value, dict)
            ]
            now = time.time()
            # Keep a narrow rolling window: the immediate predecessor is
            # enough for signal causality, and bounded retention prevents an
            # old API page from becoming a substitute predecessor.
            retained = {
                item["ticker"]: item for item in self.markets
                if item["close_epoch"] >= now - 3_600
            }
            retained.update({item["ticker"]: item for item in candidates if item is not None})
            self.markets = sorted(retained.values(), key=lambda item: item["open_epoch"])
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
        signal_state = self.state.setdefault("directional_signal_state", {
            "mode": self.config["signal_mode"], "active_side": None,
            "last_source_market": None, "last_source_outcome": None,
            "last_transition": None, "updated_at": None,
        })
        source_record = self.state["markets"].get(provisional["ticker"])
        source_record_side = (
            str(source_record.get("signal_side"))
            if isinstance(source_record, dict) and source_record.get("signal_side") in {"yes", "no"}
            else None
        )
        prior_side = source_record_side or (
            str(signal_state.get("active_side"))
            if signal_state.get("active_side") in {"yes", "no"}
            else None
        )
        side, directional_transition = sticky_directional_prediction(prior_side, source)
        signal_state.update({
            "mode": self.config["signal_mode"], "active_side": side,
            "last_source_market": provisional["ticker"], "last_source_outcome": source,
            "last_transition": directional_transition, "updated_at": utc_now(),
        })
        parameters = self.current_parameters()
        quantity, capped = prescribed_quantity(parameters, self.state.get("sizing"))
        record = {
            "ticker": ticker,
            "market_open_epoch": market["open_epoch"], "market_close_epoch": market["close_epoch"],
            "source_market_ticker": provisional["ticker"], "provisional_outcome": source,
            "provisional_outcome_details": provisional, "signal_side": side,
            "signal_mode": self.config["signal_mode"], "prior_signal_side": prior_side,
            "directional_transition": directional_transition,
            "signal_timestamp": utc_now(), "intended_quantity": format(quantity, "f"),
            "quantity_capped": capped, "base_before": format(sizing_state(parameters, self.state.get("sizing")).base_share_count, "f"),
            "recovery_exponent_before": sizing_state(parameters, self.state.get("sizing")).recovery_exponent,
            "recovery_cycle_pnl_before": str(self.state.get("sizing", {}).get("recovery_cycle_pnl", "0")),
            "status": "SIGNAL_PENDING", "entry_orders": [], "exit_orders": [], "actual_quantity": "0.00",
            # This is a durable summary of what actually opened exposure.
            # ``market_ioc`` means a price-protected IOC at the fresh
            # executable ask, never an unbounded market order.
            "entry_execution_type": "none",
            "entry_execution_summary": {
                "entry_execution_type": "none",
                "maker_limit_filled": False,
                "market_ioc_filled": False,
                "maker_limit_filled_quantity": "0.00",
                "maker_limit_average_fill_price": None,
                "market_ioc_filled_quantity": "0.00",
                "market_ioc_average_fill_price": None,
                "other_entry_filled_quantity": "0.00",
                "total_filled_quantity": "0.00",
                "actual_weighted_average_entry_price": None,
                "maker_limit_order_ids": [],
                "market_ioc_order_ids": [],
            },
            "strategy_version": self.config["strategy_version"], "config_hash": config_hash(self.config),
            "config_snapshot": self.current_parameters().as_dict(), "created_at": utc_now(),
            "entry_execution_mode": self.config["entry_execution_mode"],
            # ``entry_price`` is the historical-reference price. v9 records
            # the actual selected-side IOC ask in ``market_entry``/orders.
            "maker_entry_price": None,
            "reference_maker_entry_price": self.config["entry_price"],
            "maker_price_offset": self.config["maker_price_offset"],
            # These values are per-market immutable policy inputs.  v9 uses
            # the floor exactly; an actual entry above 50c never raises it.
            "stop_policy": self.config["stop_policy"],
            "stop_floor_price": self.config["stop_price"],
            "stop_baseline_entry_price": self.config["stop_baseline_entry_price"],
            "actual_average_entry_price": None,
            "effective_stop_price": None,
            "opening_quote_observations": [],
            "opening_quote_capture": {
                "window_seconds": int(self.config["entry_timeout_seconds"]),
                "max_observations": int(self.config["opening_quote_max_observations"]),
                "started_at": None,
                "discovery_anchor_at": None,
                "discovery_anchor_epoch": None,
                "completed_at": None,
                "observation_count": 0,
                "dropped_observation_count": 0,
                "unavailable_quote_count": 0,
            },
            "opening_price_discovery": {
                "window_seconds": int(self.config["opening_price_discovery_seconds"]),
                "anchor_at": None,
                "anchor_epoch": None,
                "anchor_lag_after_open_seconds": None,
                "completed_at": None,
                "maximum_selected_best_ask": None,
                "derived_maker_entry_price": None,
            },
            # All timestamps in this object describe when the worker observed
            # an event.  Kalshi does not expose a guaranteed matching-engine
            # fill timestamp in every order response, so the ledger must not
            # overstate these as exact exchange-fill instants.
            "entry_timing": {
                "market_open_epoch": market["open_epoch"],
                "entry_window_seconds": int(self.config["entry_timeout_seconds"]),
                "submission_events": [],
                "first_submission_at": None,
                "first_submission_epoch": None,
                "first_fill_observed_at": None,
                "first_fill_observed_epoch": None,
                "first_fill_source": None,
                "last_fill_observed_at": None,
                "last_filled_quantity": "0.00",
                "entry_attempt_completed_at": None,
                "entry_attempt_completed_epoch": None,
            },
            "stop_timing": {
                "stop_trigger_observed_at": None,
                "stop_trigger_observed_epoch": None,
                "first_exit_submission_at": None,
                "first_exit_submission_epoch": None,
                "position_closed_observed_at": None,
                "position_closed_observed_epoch": None,
            },
        }
        self.state["markets"][ticker] = record
        self.state["active_market"] = ticker
        self.audit(
            "signal_created", ticker=ticker, source_ticker=provisional["ticker"], provisional_outcome=source,
            prior_signal_side=prior_side, directional_transition=directional_transition,
            prediction=side, intended_quantity=format(quantity, "f"),
        )
        LOG.warning(
            "NEW MARKET SIGNAL | ticker=%s source=%s provisional=%s prior_side=%s transition=%s prediction=%s qty=%s entry_mode=%s fixed_stop=$%s",
            ticker, provisional["ticker"], source.upper(), prior_side and prior_side.upper(), directional_transition,
            side.upper(), quantity, self.config["entry_execution_mode"], self.config["stop_price"],
        )
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

    @staticmethod
    def average_filled_entry_price(record: dict[str, Any]) -> Decimal | None:
        """Return the weighted actual entry price, excluding fees.

        Stops are price triggers, so fee-inclusive accounting must not move
        their threshold.  The realized-P&L path continues to use actual fees
        through ``entry_cost``.
        """

        total_quantity = Decimal("0")
        total_cost = Decimal("0")
        for order in record.get("entry_orders", []):
            try:
                filled = Decimal(str(order.get("fill_count") or "0"))
                price = Decimal(str(order.get("average_fill_price")))
            except (ArithmeticError, TypeError, ValueError):
                continue
            if filled > 0 and Decimal("0") < price < Decimal("1"):
                total_quantity += filled
                total_cost += filled * price
        return total_cost / total_quantity if total_quantity > 0 else None

    def update_entry_execution_summary(self, record: dict[str, Any]) -> bool:
        """Persist how the entry exposure was actually opened.

        An entry can be wholly filled by the resting post-only maker limit,
        wholly filled by the one-shot price-protected IOC fallback, or be a
        mixture when a partial maker fill precedes an IOC remainder.  This
        method deliberately uses the exchange-reported filled quantities and
        average prices; requested order sizes are never treated as fills.

        It returns whether the durable summary materially changed, allowing
        callers to emit one audit/checkpoint update per fill change.
        """

        buckets: dict[str, dict[str, Any]] = {
            "maker_limit": {"quantity": Decimal("0"), "cost": Decimal("0"), "priced_quantity": Decimal("0"), "order_ids": []},
            "market_ioc": {"quantity": Decimal("0"), "cost": Decimal("0"), "priced_quantity": Decimal("0"), "order_ids": []},
            "other": {"quantity": Decimal("0"), "cost": Decimal("0"), "priced_quantity": Decimal("0"), "order_ids": []},
        }
        for order in record.get("entry_orders", []):
            try:
                filled = Decimal(str(order.get("fill_count") or "0"))
            except (ArithmeticError, TypeError, ValueError):
                continue
            if filled <= 0:
                continue
            phase = str(order.get("entry_phase") or "maker")
            bucket_name = (
                "maker_limit" if phase == "maker"
                else "market_ioc" if phase in {"market_fallback", "market_entry"}
                else "other"
            )
            bucket = buckets[bucket_name]
            bucket["quantity"] += filled
            order_id = order.get("order_id") or order.get("client_order_id")
            if order_id is not None:
                bucket["order_ids"].append(str(order_id))
            try:
                price = Decimal(str(order.get("average_fill_price")))
            except (ArithmeticError, TypeError, ValueError):
                price = None
            if price is not None and Decimal("0") < price < Decimal("1"):
                bucket["cost"] += filled * price
                bucket["priced_quantity"] += filled

        maker, ioc, other = buckets["maker_limit"], buckets["market_ioc"], buckets["other"]
        total_quantity = maker["quantity"] + ioc["quantity"] + other["quantity"]
        total_priced_quantity = maker["priced_quantity"] + ioc["priced_quantity"] + other["priced_quantity"]
        total_cost = maker["cost"] + ioc["cost"] + other["cost"]

        def average(bucket: dict[str, Any]) -> str | None:
            if bucket["quantity"] <= 0 or bucket["priced_quantity"] != bucket["quantity"]:
                return None
            return format(bucket["cost"] / bucket["quantity"], "f")

        if total_quantity <= 0:
            execution_type = "none"
        elif maker["quantity"] > 0 and ioc["quantity"] > 0:
            execution_type = "mixed"
        elif maker["quantity"] > 0 and other["quantity"] == 0:
            execution_type = "maker_limit"
        elif ioc["quantity"] > 0 and other["quantity"] == 0:
            execution_type = "market_ioc"
        else:
            execution_type = "other"

        summary = {
            "entry_execution_type": execution_type,
            "maker_limit_filled": maker["quantity"] > 0,
            "market_ioc_filled": ioc["quantity"] > 0,
            "maker_limit_filled_quantity": format(maker["quantity"], "f"),
            "maker_limit_average_fill_price": average(maker),
            "market_ioc_filled_quantity": format(ioc["quantity"], "f"),
            "market_ioc_average_fill_price": average(ioc),
            "other_entry_filled_quantity": format(other["quantity"], "f"),
            "total_filled_quantity": format(total_quantity, "f"),
            "actual_weighted_average_entry_price": (
                format(total_cost / total_quantity, "f")
                if total_quantity > 0 and total_priced_quantity == total_quantity else None
            ),
            "maker_limit_order_ids": maker["order_ids"],
            "market_ioc_order_ids": ioc["order_ids"],
        }
        changed = summary != record.get("entry_execution_summary") or execution_type != record.get("entry_execution_type")
        record["entry_execution_summary"] = summary
        record["entry_execution_type"] = execution_type
        return changed

    def refresh_entry_execution_metrics(self) -> dict[str, Any]:
        """Recompute fill-method totals from durable per-market fill facts.

        This intentionally recomputes rather than incrementing counters.  A
        maker partial can later become a mixed maker/IOC entry, and a runner
        may restart between those events.  Recalculation makes the aggregate
        idempotent and prevents a restart or refresh from counting a market
        twice.
        """

        counts = {
            "tracked_markets": 0,
            "markets_with_entry_fill": 0,
            "zero_fill_markets": 0,
            "funding_failure_markets": 0,
            "missed_signal_markets": 0,
            "entry_pending_markets": 0,
            "maker_limit_only_markets": 0,
            "market_ioc_only_markets": 0,
            "mixed_entry_markets": 0,
            "other_entry_markets": 0,
            "maker_limit_fill_markets": 0,
            "market_ioc_fill_markets": 0,
        }
        quantities = {
            "maker_limit_filled_quantity": Decimal("0"),
            "market_ioc_filled_quantity": Decimal("0"),
            "other_entry_filled_quantity": Decimal("0"),
            "total_entry_filled_quantity": Decimal("0"),
        }
        for record in self.state.get("markets", {}).values():
            if not isinstance(record, dict):
                continue
            counts["tracked_markets"] += 1
            status = str(record.get("status") or "")
            if status == "ZERO_FILL":
                counts["zero_fill_markets"] += 1
            elif status == "FUNDING_FAILURE":
                counts["funding_failure_markets"] += 1
            elif status == "MISSED_SIGNAL":
                counts["missed_signal_markets"] += 1
            elif status in {"SIGNAL_PENDING", "ENTRY_PENDING", "ENTRY_PARTIAL"}:
                counts["entry_pending_markets"] += 1
            self.update_entry_execution_summary(record)
            summary = record["entry_execution_summary"]
            try:
                maker_quantity = Decimal(str(summary["maker_limit_filled_quantity"]))
                ioc_quantity = Decimal(str(summary["market_ioc_filled_quantity"]))
                other_quantity = Decimal(str(summary["other_entry_filled_quantity"]))
            except (ArithmeticError, KeyError, TypeError, ValueError):
                continue
            total = maker_quantity + ioc_quantity + other_quantity
            if total <= 0:
                continue
            counts["markets_with_entry_fill"] += 1
            execution_type = str(summary["entry_execution_type"])
            if execution_type == "maker_limit":
                counts["maker_limit_only_markets"] += 1
            elif execution_type == "market_ioc":
                counts["market_ioc_only_markets"] += 1
            elif execution_type == "mixed":
                counts["mixed_entry_markets"] += 1
            else:
                counts["other_entry_markets"] += 1
            if maker_quantity > 0:
                counts["maker_limit_fill_markets"] += 1
            if ioc_quantity > 0:
                counts["market_ioc_fill_markets"] += 1
            quantities["maker_limit_filled_quantity"] += maker_quantity
            quantities["market_ioc_filled_quantity"] += ioc_quantity
            quantities["other_entry_filled_quantity"] += other_quantity
            quantities["total_entry_filled_quantity"] += total

        metrics = {**counts, **{key: format(value, "f") for key, value in quantities.items()}}
        self.state["entry_execution_metrics"] = metrics
        if self.dry_run:
            # Surface the two requested headline counters alongside balance,
            # drawdown, and other shadow-run metrics for convenient reporting.
            shadow = self.shadow_metrics()
            for key in (
                "maker_limit_fill_markets", "market_ioc_fill_markets", "mixed_entry_markets",
                "maker_limit_filled_quantity", "market_ioc_filled_quantity",
                "zero_fill_markets", "funding_failure_markets", "missed_signal_markets",
            ):
                shadow[key] = metrics[key]
        return metrics

    def note_entry_execution_summary(self, record: dict[str, Any], reason: str) -> None:
        """Audit and checkpoint an actual maker/IOC fill composition change."""

        if not self.update_entry_execution_summary(record):
            return
        summary = record["entry_execution_summary"]
        aggregate = self.refresh_entry_execution_metrics()
        self.audit(
            "entry_execution_updated", ticker=record["ticker"], reason=reason,
            entry_execution_type=summary["entry_execution_type"],
            maker_limit_filled_quantity=summary["maker_limit_filled_quantity"],
            maker_limit_average_fill_price=summary["maker_limit_average_fill_price"],
            market_ioc_filled_quantity=summary["market_ioc_filled_quantity"],
            market_ioc_average_fill_price=summary["market_ioc_average_fill_price"],
            actual_weighted_average_entry_price=summary["actual_weighted_average_entry_price"],
            maker_limit_fill_markets=aggregate["maker_limit_fill_markets"],
            market_ioc_fill_markets=aggregate["market_ioc_fill_markets"],
            mixed_entry_markets=aggregate["mixed_entry_markets"],
        )
        self.checkpoint("entry_execution_updated")

    @staticmethod
    def _timing_summary(values: list[float]) -> dict[str, float | int | None]:
        """Compact, deterministic latency statistics for the durable state."""

        if not values:
            return {"count": 0, "mean_seconds": None, "median_seconds": None, "p95_seconds": None, "maximum_seconds": None}
        ordered = sorted(values)

        def percentile(probability: float) -> float:
            position = (len(ordered) - 1) * probability
            lower = int(position)
            upper = min(len(ordered) - 1, lower + 1)
            fraction = position - lower
            return round(ordered[lower] + (ordered[upper] - ordered[lower]) * fraction, 6)

        return {
            "count": len(ordered),
            "mean_seconds": round(sum(ordered) / len(ordered), 6),
            "median_seconds": percentile(0.5),
            "p95_seconds": percentile(0.95),
            "maximum_seconds": round(ordered[-1], 6),
        }

    def refresh_execution_timing_metrics(self) -> dict[str, Any]:
        """Rebuild latency telemetry from per-market facts idempotently.

        This is intentionally observational telemetry only: recovery sizing
        and P&L never depend on it.  Rebuilding instead of incrementing keeps
        it accurate through restarts and reconciliation.
        """

        entry_from_open: list[float] = []
        entry_from_submit: list[float] = []
        stop_from_entry: list[float] = []
        stop_from_open: list[float] = []
        stop_exit_submission: list[float] = []
        stop_exit_close: list[float] = []
        for record in self.state.get("markets", {}).values():
            if not isinstance(record, dict):
                continue
            entry = record.get("entry_timing")
            if isinstance(entry, dict):
                for value, bucket in (
                    (entry.get("market_open_to_first_fill_seconds"), entry_from_open),
                    (entry.get("first_submission_to_first_fill_seconds"), entry_from_submit),
                ):
                    try:
                        if value is not None:
                            bucket.append(float(value))
                    except (TypeError, ValueError):
                        pass
            stop = record.get("stop_timing")
            if isinstance(stop, dict):
                for value, bucket in (
                    (stop.get("first_fill_to_stop_trigger_seconds"), stop_from_entry),
                    (stop.get("market_open_to_stop_trigger_seconds"), stop_from_open),
                    (stop.get("stop_trigger_to_first_exit_submission_seconds"), stop_exit_submission),
                    (stop.get("stop_trigger_to_position_closed_seconds"), stop_exit_close),
                ):
                    try:
                        if value is not None:
                            bucket.append(float(value))
                    except (TypeError, ValueError):
                        pass
        metrics = {
            "entry_first_fill_from_market_open": self._timing_summary(entry_from_open),
            "entry_first_fill_from_submission": self._timing_summary(entry_from_submit),
            "stop_trigger_from_first_fill": self._timing_summary(stop_from_entry),
            "stop_trigger_from_market_open": self._timing_summary(stop_from_open),
            "stop_first_exit_submission": self._timing_summary(stop_exit_submission),
            "stop_position_closed": self._timing_summary(stop_exit_close),
        }
        self.state["execution_timing_metrics"] = metrics
        return metrics

    def note_entry_order_submitted(self, record: dict[str, Any], order: dict[str, Any], phase: str) -> None:
        """Persist an entry submission and its latency from the market open."""

        observed_epoch = time.time()
        submitted_at = str(order.get("submitted_at") or utc_now())
        order.setdefault("submitted_at", submitted_at)
        submitted_epoch = _iso_epoch(submitted_at) or observed_epoch
        timing = record.setdefault("entry_timing", {"market_open_epoch": record.get("market_open_epoch"), "submission_events": []})
        submissions = timing.setdefault("submission_events", [])
        identity = str(order.get("order_id") or order.get("client_order_id") or f"{phase}:{len(submissions)}")
        if any(str(item.get("identity")) == identity for item in submissions if isinstance(item, dict)):
            return
        market_open = float(record.get("market_open_epoch") or submitted_epoch)
        event = {
            "identity": identity,
            "phase": phase,
            "submitted_at": submitted_at,
            "submitted_epoch": submitted_epoch,
            "market_open_to_submission_seconds": _seconds_since(market_open, submitted_epoch),
            "requested_quantity": str(order.get("quantity") or "0"),
            "requested_price": str(order.get("position_price") or ""),
        }
        submissions.append(event)
        if timing.get("first_submission_epoch") is None:
            timing.update({
                "first_submission_at": submitted_at,
                "first_submission_epoch": submitted_epoch,
                "market_open_to_first_submission_seconds": event["market_open_to_submission_seconds"],
            })
        self.audit(
            "entry_submission_timing", ticker=record["ticker"], phase=phase,
            order_id=order.get("order_id"), client_order_id=order.get("client_order_id"),
            market_open_to_submission_seconds=event["market_open_to_submission_seconds"],
        )

    def note_entry_fill_observed(
        self, record: dict[str, Any], previous_quantity: Decimal, filled_quantity: Decimal, source: str,
    ) -> None:
        """Record the first/last observed entry fill without inventing fill time."""

        if filled_quantity <= previous_quantity:
            return
        observed_epoch = time.time()
        observed_at = utc_now()
        timing = record.setdefault("entry_timing", {"market_open_epoch": record.get("market_open_epoch"), "submission_events": []})
        market_open = float(record.get("market_open_epoch") or observed_epoch)
        timing.update({
            "last_fill_observed_at": observed_at,
            "last_fill_observed_epoch": observed_epoch,
            "last_filled_quantity": format(filled_quantity, "f"),
        })
        if timing.get("first_fill_observed_epoch") is None:
            first_submission = timing.get("first_submission_epoch")
            timing.update({
                "first_fill_observed_at": observed_at,
                "first_fill_observed_epoch": observed_epoch,
                "first_fill_source": source,
                "market_open_to_first_fill_seconds": _seconds_since(market_open, observed_epoch),
                "first_submission_to_first_fill_seconds": _seconds_since(
                    float(first_submission) if first_submission is not None else None, observed_epoch,
                ),
            })
        metrics = self.refresh_execution_timing_metrics()
        self.audit(
            "entry_fill_observed", ticker=record["ticker"], source=source,
            previous_quantity=format(previous_quantity, "f"), filled_quantity=format(filled_quantity, "f"),
            market_open_to_first_fill_seconds=timing.get("market_open_to_first_fill_seconds"),
            first_submission_to_first_fill_seconds=timing.get("first_submission_to_first_fill_seconds"),
            timing_metrics=metrics,
        )

    def note_entry_attempt_completed(self, record: dict[str, Any], filled_quantity: Decimal, reason: str) -> None:
        """Freeze the measurable result of the maker/IOC entry window."""

        timing = record.setdefault("entry_timing", {"market_open_epoch": record.get("market_open_epoch")})
        if timing.get("entry_attempt_completed_epoch") is not None:
            return
        observed_epoch = time.time()
        market_open = float(record.get("market_open_epoch") or observed_epoch)
        first_submission = timing.get("first_submission_epoch")
        timing.update({
            "entry_attempt_completed_at": utc_now(),
            "entry_attempt_completed_epoch": observed_epoch,
            "entry_attempt_reason": reason,
            "entry_attempt_final_filled_quantity": format(filled_quantity, "f"),
            "market_open_to_entry_attempt_completion_seconds": _seconds_since(market_open, observed_epoch),
            "first_submission_to_entry_attempt_completion_seconds": _seconds_since(
                float(first_submission) if first_submission is not None else None, observed_epoch,
            ),
        })
        self.audit(
            "entry_attempt_completed", ticker=record["ticker"], reason=reason,
            final_filled_quantity=format(filled_quantity, "f"),
            market_open_to_entry_attempt_completion_seconds=timing["market_open_to_entry_attempt_completion_seconds"],
        )

    def note_stop_trigger(
        self, record: dict[str, Any], executable_bid: Decimal, stop_price: Decimal, quantity: Decimal, *, shadow: bool,
    ) -> None:
        """Preserve first stop detection time across IOC retries and restarts."""

        observed_epoch = time.time()
        trigger = record.setdefault("stop_trigger", {})
        trigger.setdefault("at", utc_now())
        trigger.setdefault("observed_epoch", observed_epoch)
        trigger.update({
            "best_executable_bid": format(executable_bid, "f"),
            "effective_stop_price": format(stop_price, "f"),
            "requested_quantity": format(quantity, "f"),
            "shadow": shadow,
        })
        timing = record.setdefault("stop_timing", {})
        if timing.get("stop_trigger_observed_epoch") is not None:
            return
        trigger_epoch = float(trigger.get("observed_epoch") or observed_epoch)
        entry = record.get("entry_timing") if isinstance(record.get("entry_timing"), dict) else {}
        first_fill = entry.get("first_fill_observed_epoch") if isinstance(entry, dict) else None
        market_open = float(record.get("market_open_epoch") or trigger_epoch)
        timing.update({
            "stop_trigger_observed_at": trigger["at"],
            "stop_trigger_observed_epoch": trigger_epoch,
            "first_fill_to_stop_trigger_seconds": _seconds_since(
                float(first_fill) if first_fill is not None else None, trigger_epoch,
            ),
            "market_open_to_stop_trigger_seconds": _seconds_since(market_open, trigger_epoch),
        })
        metrics = self.refresh_execution_timing_metrics()
        self.audit(
            "stop_trigger_timing", ticker=record["ticker"], executable_bid=format(executable_bid, "f"),
            effective_stop_price=format(stop_price, "f"),
            first_fill_to_stop_trigger_seconds=timing["first_fill_to_stop_trigger_seconds"],
            market_open_to_stop_trigger_seconds=timing["market_open_to_stop_trigger_seconds"], timing_metrics=metrics,
        )

    def note_stop_exit_submitted(self, record: dict[str, Any], order: dict[str, Any]) -> None:
        """Capture time from a stop trigger to the first flattening request."""

        timing = record.setdefault("stop_timing", {})
        if timing.get("first_exit_submission_epoch") is not None:
            return
        observed_epoch = time.time()
        submitted_at = str(order.get("submitted_at") or utc_now())
        order.setdefault("submitted_at", submitted_at)
        submitted_epoch = _iso_epoch(submitted_at) or observed_epoch
        trigger_epoch = timing.get("stop_trigger_observed_epoch")
        timing.update({
            "first_exit_submission_at": submitted_at,
            "first_exit_submission_epoch": submitted_epoch,
            "stop_trigger_to_first_exit_submission_seconds": _seconds_since(
                float(trigger_epoch) if trigger_epoch is not None else None, submitted_epoch,
            ),
            "first_exit_order_id": order.get("order_id"),
            "first_exit_client_order_id": order.get("client_order_id"),
        })
        self.audit(
            "stop_exit_submitted_timing", ticker=record["ticker"], exchange_order_id=order.get("order_id"),
            stop_trigger_to_first_exit_submission_seconds=timing["stop_trigger_to_first_exit_submission_seconds"],
        )

    def note_stop_position_closed(self, record: dict[str, Any]) -> None:
        """Record observed time to flatten; live fills remain exchange-authoritative."""

        timing = record.setdefault("stop_timing", {})
        if timing.get("position_closed_observed_epoch") is not None:
            return
        observed_epoch = time.time()
        trigger_epoch = timing.get("stop_trigger_observed_epoch")
        timing.update({
            "position_closed_observed_at": utc_now(),
            "position_closed_observed_epoch": observed_epoch,
            "stop_trigger_to_position_closed_seconds": _seconds_since(
                float(trigger_epoch) if trigger_epoch is not None else None, observed_epoch,
            ),
        })
        metrics = self.refresh_execution_timing_metrics()
        self.audit(
            "stop_position_closed_timing", ticker=record["ticker"],
            stop_trigger_to_position_closed_seconds=timing["stop_trigger_to_position_closed_seconds"],
            timing_metrics=metrics,
        )

    def update_effective_stop_price(self, record: dict[str, Any]) -> Decimal | None:
        """Persist the v9 fixed profile stop used for active monitoring.

        The entry price is a fact for P&L accounting, never a reason to move
        the stop.  In particular, an IOC fill at 54c still exits only when
        the selected-side executable bid is at or below the configured 40c
        floor (or the explicit 30/20/10 shadow comparison floor).
        """

        average = self.average_filled_entry_price(record)
        if average is None:
            return None
        floor = Decimal(str(record.get("stop_floor_price") or self.config["stop_price"]))
        baseline = Decimal(str(record.get("stop_baseline_entry_price") or self.config["stop_baseline_entry_price"]))
        if record.get("stop_policy", self.config["stop_policy"]) == "fixed_profile_floor":
            effective = floor
        else:
            # Retain a defensive reader for archived records, but v9 config
            # validation makes this branch unreachable for new execution.
            effective = effective_stop_price(average, floor, baseline)
        prior = record.get("effective_stop_price")
        record.update({
            "actual_average_entry_price": format(average, "f"),
            "effective_stop_price": format(effective, "f"),
            "stop_adjustment_from_floor": format(effective - floor, "f"),
        })
        if prior != record["effective_stop_price"]:
            self.audit(
                "effective_stop_price_updated", ticker=record["ticker"], policy=record.get("stop_policy"),
                actual_average_entry_price=record["actual_average_entry_price"], stop_floor_price=format(floor, "f"),
                stop_baseline_entry_price=format(baseline, "f"), effective_stop_price=record["effective_stop_price"],
            )
            self.checkpoint("effective_stop_price_updated")
        return effective

    def stop_price_for_record(self, record: dict[str, Any]) -> Decimal:
        """Use the persisted fixed executable stop floor."""

        calculated = self.update_effective_stop_price(record)
        if calculated is not None:
            return calculated
        persisted = record.get("effective_stop_price")
        if persisted not in (None, ""):
            return Decimal(str(persisted))
        return Decimal(str(record.get("stop_floor_price") or self.config["stop_price"]))

    def note_stop_monitor_quote(
        self, record: dict[str, Any], executable_bid: Decimal | None, effective_stop: Decimal,
    ) -> None:
        """Keep durable aggregate evidence for every post-entry stop check."""

        monitor = record.setdefault("post_entry_stop_monitor", {
            "quote_count": 0, "unavailable_quote_count": 0, "minimum_executable_bid": None,
            "minimum_bid_observed_at": None, "last_executable_bid": None,
        })
        if executable_bid is None:
            monitor["unavailable_quote_count"] = int(monitor.get("unavailable_quote_count", 0)) + 1
            return
        monitor["quote_count"] = int(monitor.get("quote_count", 0)) + 1
        monitor["last_executable_bid"] = format(executable_bid, "f")
        monitor["last_effective_stop_price"] = format(effective_stop, "f")
        minimum = monitor.get("minimum_executable_bid")
        if minimum in (None, "") or executable_bid < Decimal(str(minimum)):
            monitor["minimum_executable_bid"] = format(executable_bid, "f")
            monitor["minimum_bid_observed_at"] = utc_now()

    def capture_opening_quote(self, feed: KalshiLiveFeed, record: dict[str, Any], now: float) -> None:
        """Persist every available fresh complete top-of-book during maker time.

        A record is tied to a directional side and therefore contains the
        selected-side bid/ask as well as both YES/NO derived prices.  The
        capture is evidence for later fill calibration, not an assertion that
        a maker order had queue priority or filled.
        """

        start = float(record["market_open_epoch"])
        deadline = float(record.get("entry_deadline_epoch") or self.entry_deadline(record))
        capture = record.setdefault("opening_quote_capture", {})
        observations = record.setdefault("opening_quote_observations", [])
        if now < start:
            return
        if now >= deadline:
            if capture.get("completed_at") is None:
                capture["completed_at"] = utc_now()
                capture["observation_count"] = len(observations)
                self.audit(
                    "opening_quote_capture_completed", ticker=record["ticker"], side=record["signal_side"],
                    observation_count=len(observations),
                    unavailable_quote_count=int(capture.get("unavailable_quote_count", 0)),
                    dropped_observation_count=int(capture.get("dropped_observation_count", 0)),
                )
                self.checkpoint("opening_quote_capture_completed")
            return
        quote, quote_state = feed.executable_shadow_quote(
            record["ticker"], str(record["signal_side"]), 0.0,
            float(self.config["max_stale_quote_seconds"]),
        )
        if quote is None:
            capture["unavailable_quote_count"] = int(capture.get("unavailable_quote_count", 0)) + 1
            capture["last_unavailable_reason"] = quote_state
            return
        quote_id = str(quote.get("quote_id") or "")
        if observations and quote_id and observations[-1].get("quote_id") == quote_id:
            return
        max_observations = int(self.config["opening_quote_max_observations"])
        if len(observations) >= max_observations:
            capture["dropped_observation_count"] = int(capture.get("dropped_observation_count", 0)) + 1
            return
        try:
            yes_bid = Decimal(str(quote["yes_bid"]))
            yes_ask = Decimal(str(quote["yes_ask"]))
            yes_bid_size = Decimal(str(quote["yes_bid_size"]))
            yes_ask_size = Decimal(str(quote["yes_ask_size"]))
        except (KeyError, ArithmeticError, ValueError):
            capture["unavailable_quote_count"] = int(capture.get("unavailable_quote_count", 0)) + 1
            capture["last_unavailable_reason"] = "incomplete_complete_top_of_book"
            return
        side = str(record["signal_side"])
        no_bid, no_ask = Decimal("1") - yes_ask, Decimal("1") - yes_bid
        selected_bid = yes_bid if side == "yes" else no_bid
        selected_ask = yes_ask if side == "yes" else no_ask
        selected_bid_size = yes_bid_size if side == "yes" else yes_ask_size
        selected_ask_size = yes_ask_size if side == "yes" else yes_bid_size
        observation = {
            "captured_at": utc_now(), "captured_epoch": now,
            "elapsed_after_open_seconds": round(max(0.0, now - start), 6),
            "quote_id": quote_id or None, "source": "kalshi_websocket_complete_top_of_book",
            "source_server_timestamp": quote.get("source_server_timestamp"),
            "source_timestamp_ms": quote.get("source_timestamp_ms"), "received_at": quote.get("received_at"),
            "quote_age_seconds": quote.get("quote_age_seconds"),
            "yes_bid": format(yes_bid, "f"), "yes_ask": format(yes_ask, "f"),
            "yes_bid_size": format(yes_bid_size, "f"), "yes_ask_size": format(yes_ask_size, "f"),
            "no_bid": format(no_bid, "f"), "no_ask": format(no_ask, "f"),
            "no_bid_size": format(yes_ask_size, "f"), "no_ask_size": format(yes_bid_size, "f"),
            "selected_side": side, "selected_best_bid": format(selected_bid, "f"),
            "selected_best_ask": format(selected_ask, "f"),
            "selected_bid_size": format(selected_bid_size, "f"), "selected_ask_size": format(selected_ask_size, "f"),
            "reference_maker_entry_price": str(record.get("reference_maker_entry_price") or self.config["entry_price"]),
            "proposed_limit_from_this_ask": format(selected_ask - Decimal(str(record.get("maker_price_offset") or self.config["maker_price_offset"])), "f"),
            "maker_price_offset": str(record.get("maker_price_offset") or self.config["maker_price_offset"]),
            "post_only_would_cross": None,
        }
        observations.append(observation)
        capture.update({
            "started_at": capture.get("started_at") or utc_now(), "first_capture_lag_seconds": capture.get("first_capture_lag_seconds", observation["elapsed_after_open_seconds"]),
            # The first usable book, not a wall-clock moment before the feed
            # was ready, anchors the short maximum-price discovery sample.
            "discovery_anchor_at": capture.get("discovery_anchor_at") or observation["captured_at"],
            "discovery_anchor_epoch": capture.get("discovery_anchor_epoch") or now,
            "last_capture_at": observation["captured_at"], "last_quote_id": observation["quote_id"],
            "observation_count": len(observations), "last_selected_best_ask": observation["selected_best_ask"],
            "min_selected_best_ask": format(min(Decimal(str(capture.get("min_selected_best_ask") or selected_ask)), selected_ask), "f"),
            "max_selected_best_ask": format(max(Decimal(str(capture.get("max_selected_best_ask") or selected_ask)), selected_ask), "f"),
            "max_selected_best_bid": format(max(Decimal(str(capture.get("max_selected_best_bid") or selected_bid)), selected_bid), "f"),
        })
        if len(observations) == 1:
            self.audit(
                "opening_quote_capture_started", ticker=record["ticker"], side=side,
                maker_price_offset=record.get("maker_price_offset"),
            )
            self.checkpoint("opening_quote_capture_started")

    def derive_opening_maker_price(self, record: dict[str, Any], now: float) -> Decimal | None:
        """Choose a one-cent-below-max after a sample from the first fresh book.

        A feed can legitimately become usable a few seconds after market
        open.  The discovery sample starts at that first fresh, complete
        selected-side book instead of treating transport warm-up as a signal
        to abandon the market.
        """

        existing = record.get("maker_entry_price")
        if existing not in (None, ""):
            return Decimal(str(existing))
        discovery = int(self.config["opening_price_discovery_seconds"])
        capture = record.setdefault("opening_quote_capture", {})
        anchor_value = capture.get("discovery_anchor_epoch")
        if anchor_value is None:
            return None
        anchor = float(anchor_value)
        if now < anchor + discovery:
            return None
        observed = [
            Decimal(str(item["selected_best_ask"]))
            for item in record.get("opening_quote_observations", [])
            if isinstance(item, dict)
            and anchor <= float(item.get("captured_epoch") or float("-inf")) <= anchor + discovery
        ]
        details = record.setdefault("opening_price_discovery", {})
        details["completed_at"] = details.get("completed_at") or utc_now()
        details["anchor_at"] = capture.get("discovery_anchor_at")
        details["anchor_epoch"] = anchor
        details["anchor_lag_after_open_seconds"] = round(max(0.0, anchor - float(record["market_open_epoch"])), 6)
        if not observed:
            details["reason"] = "no_fresh_complete_top_of_book_in_discovery_window"
            return None
        observed_max = max(observed)
        offset = Decimal(str(record.get("maker_price_offset") or self.config["maker_price_offset"]))
        maker = observed_max - offset
        details.update({
            "maximum_selected_best_ask": format(observed_max, "f"),
            "maker_price_offset": format(offset, "f"),
            "derived_maker_entry_price": format(maker, "f"),
            "discovery_seconds": discovery,
        })
        if maker <= Decimal(self.config["stop_price"]):
            details["reason"] = "one_cent_below_opening_max_at_or_below_stop"
            return None
        record["maker_entry_price"] = format(maker, "f")
        self.audit(
            "opening_maker_price_derived", ticker=record["ticker"], side=record["signal_side"],
            maximum_selected_best_ask=format(observed_max, "f"), maker_entry_price=format(maker, "f"),
            maker_price_offset=format(offset, "f"),
        )
        LOG.warning(
            "ENTRY MAKER READY | ticker=%s side=%s max_ask=%s maker_limit=%s "
            "first_quote_lag=%ss discovery_seconds=%s",
            record["ticker"], record["signal_side"], format(observed_max, "f"), format(maker, "f"),
            details["anchor_lag_after_open_seconds"], discovery,
        )
        return maker

    def handoff_ready(self, now: float) -> tuple[bool, dict[str, Any]]:
        """Permit an Actions handoff only in the quiet middle 13 minutes.

        The final minute is reserved for outcome observation/preloading, and
        the first minute is reserved for signal, maker, and IOC entry work.
        Entry/stop/settlement transitions also block handoff even in that
        middle window.  A plain open position may be transferred because the
        next serialized worker starts with authoritative reconciliation.
        """

        active = self.active_market(now)
        if active is None:
            return False, {"reason": "no_current_exchange_market"}
        guard = int(self.config["handoff_guard_seconds"])
        window_start = float(active["open_epoch"]) + guard
        window_end = float(active["close_epoch"]) - guard
        if now < window_start or now > window_end:
            return False, {
                "reason": "outside_middle_13_minute_window", "ticker": active["ticker"],
                "window_start_epoch": window_start, "window_end_epoch": window_end,
            }
        blocking_states = {"SIGNAL_PENDING", "ENTRY_PENDING", "ENTRY_PARTIAL", "STOP_PENDING", "SETTLEMENT_PENDING"}
        blockers = [
            {"ticker": item.get("ticker"), "status": item.get("status")}
            for item in self.state.get("markets", {}).values()
            if isinstance(item, dict) and item.get("status") in blocking_states
        ]
        if blockers:
            return False, {"reason": "operational_state_requires_current_worker", "ticker": active["ticker"], "blockers": blockers}
        return True, {
            "reason": "safe_middle_13_minute_handoff", "ticker": active["ticker"],
            "window_start_epoch": window_start, "window_end_epoch": window_end,
        }

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
            confirmed = await rest.cancel_order(record, dry_run=False)
            if not confirmed:
                self.trip("emergency_entry_cancellation_unconfirmed")
                self.audit("emergency_managed_order_cancel_unconfirmed", order_id=record["order_id"])
                continue
            cancelled += 1
            self.audit("emergency_managed_order_cancel", order_id=record["order_id"])
        LOG.warning("EMERGENCY CANCEL COMPLETE | confirmed_managed_resting_orders=%d", cancelled)
        return cancelled

    @staticmethod
    def _portfolio_fill_quantity(fill: Any) -> Decimal:
        raw = field(fill, "count_fp", "count", "quantity", "fill_count_fp")
        return Decimal(str(raw)) if raw is not None else Decimal("0")

    @staticmethod
    def _portfolio_fill_fee(fill: Any) -> Decimal:
        raw = field(fill, "fee_cost", "fee_cost_dollars", "fees_paid_dollars", "fee")
        return Decimal(str(raw)) if raw is not None else Decimal("0")

    @staticmethod
    def _portfolio_fill_price(fill: Any, side: str) -> Decimal | None:
        """Decode a portfolio-fill price on the strategy's economic side."""

        side_value = field(fill, f"{side}_price_dollars", f"{side}_price")
        explicit_dollar_value = field(fill, f"{side}_price_dollars")
        if side_value is None:
            yes_value = field(fill, "yes_price_dollars", "yes_price")
            if yes_value is None:
                side_value = field(fill, "price_dollars", "price")
                explicit_dollar_value = field(fill, "price_dollars")
            elif side == "no":
                yes_price = Decimal(str(yes_value))
                if field(fill, "yes_price_dollars") is None and yes_price > 1:
                    yes_price /= Decimal("100")
                return Decimal("1") - yes_price
            else:
                side_value = yes_value
                explicit_dollar_value = field(fill, "yes_price_dollars")
        if side_value is None:
            return None
        price = Decimal(str(side_value))
        if explicit_dollar_value is None and price > 1:
            price /= Decimal("100")
        return price if Decimal("0") <= price <= Decimal("1") else None

    def reconstruct_entry_accounting_from_fills(
        self, record: dict[str, Any], fills: Iterable[Any], authoritative_quantity: Decimal,
    ) -> bool:
        """Restore known entry fill cost/fees from exact exchange identifiers.

        Ticker matching alone is intentionally insufficient: a manual trade
        can exist in the same 15-minute market.  Only persisted order IDs or
        this strategy's deterministic entry client IDs may supply accounting
        facts.  If the exchange position cannot be fully explained, stop
        management continues but realized P&L/recovery transitions are blocked.
        """

        side = str(record.get("signal_side") or "")
        if side not in {"yes", "no"}:
            return authoritative_quantity == 0
        entry_ids = {
            str(value) for order in record.get("entry_orders", []) if isinstance(order, dict)
            for value in (order.get("order_id"), order.get("client_order_id")) if value
        }
        entry_ids.update({
            deterministic_client_order_id(record["ticker"], side, "entry", self.config),
            deterministic_client_order_id(record["ticker"], side, "market-fallback", self.config),
            deterministic_client_order_id(record["ticker"], side, "market-entry", self.config),
        })
        groups: dict[str, dict[str, Any]] = {}
        seen: set[str] = set()
        for fill in fills:
            if str(field(fill, "ticker", "market_ticker") or "") != record.get("ticker"):
                continue
            order_id = str(field(fill, "order_id") or "")
            client_id = str(field(fill, "client_order_id") or "")
            if not ({order_id, client_id} - {""}) & entry_ids:
                continue
            quantity = self._portfolio_fill_quantity(fill)
            price = self._portfolio_fill_price(fill, side)
            if quantity <= 0 or price is None:
                continue
            fill_id = str(field(fill, "fill_id", "id", "trade_id") or f"{order_id}:{client_id}:{quantity}:{price}")
            if fill_id in seen:
                continue
            seen.add(fill_id)
            key = order_id or client_id
            group = groups.setdefault(key, {
                "order_id": order_id or None, "client_order_id": client_id or None,
                "quantity": Decimal("0"), "cost": Decimal("0"), "fees": Decimal("0"), "fill_ids": [],
            })
            group["quantity"] += quantity
            group["cost"] += quantity * price
            group["fees"] += self._portfolio_fill_fee(fill)
            group["fill_ids"].append(fill_id)

        if groups:
            known_orders = [order for order in record.setdefault("entry_orders", []) if isinstance(order, dict)]
            for group in groups.values():
                target = next((order for order in known_orders if group["order_id"] and order.get("order_id") == group["order_id"]), None)
                if target is None:
                    target = next((order for order in known_orders if group["client_order_id"] and order.get("client_order_id") == group["client_order_id"]), None)
                if target is None:
                    phase = (
                        "market_entry" if group["client_order_id"] == deterministic_client_order_id(record["ticker"], side, "market-entry", self.config)
                        else "market_fallback" if group["client_order_id"] == deterministic_client_order_id(record["ticker"], side, "market-fallback", self.config)
                        else "maker"
                    )
                    target = {"order_id": group["order_id"], "client_order_id": group["client_order_id"], "entry_phase": phase, "remaining_count": "0"}
                    known_orders.append(target)
                    record["entry_orders"].append(target)
                target.update({
                    "fill_count": format(group["quantity"], "f"),
                    "remaining_count": "0", "average_fill_price": format(group["cost"] / group["quantity"], "f"),
                    "fees_paid": format(group["fees"], "f"), "reconciled_from_portfolio_fills": True,
                    "reconciled_fill_ids": group["fill_ids"],
                })

        accounted = sum(
            Decimal(str(order.get("fill_count") or "0"))
            for order in record.get("entry_orders", []) if isinstance(order, dict)
        )
        reconciled = authoritative_quantity <= 0 or accounted + Decimal("0.004") >= authoritative_quantity
        record["entry_accounting_reconciled"] = reconciled
        record["entry_accounting_reconciliation"] = {
            "at": utc_now(), "authoritative_quantity": format(authoritative_quantity, "f"),
            "accounted_entry_quantity": format(accounted, "f"), "matched_fill_count": len(seen), "reconciled": reconciled,
        }
        self.note_entry_execution_summary(record, "portfolio_fill_reconciliation")
        return reconciled

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
        fill_rows = recent_fills.get("fills", []) if isinstance(recent_fills, dict) and isinstance(recent_fills.get("fills"), list) else []
        for order in orders:
            ticker = str(field(order, "ticker") or "")
            client_id = str(field(order, "client_order_id") or "")
            if not ticker or ticker not in known:
                # The order belongs to this strategy prefix but its durable
                # state is missing. Do not guess its intended signal/order role.
                self.trip("unknown_managed_open_order")
                self.audit("reconciliation_discrepancy", ticker=ticker, client_order_id=client_id, reason="unknown_managed_open_order")
                return False
            record = known[ticker]
            side = str(record.get("signal_side") or "")
            maker_client_id = deterministic_client_order_id(ticker, side, "entry", self.config) if side in {"yes", "no"} else ""
            fallback_client_id = deterministic_client_order_id(ticker, side, "market-fallback", self.config) if side in {"yes", "no"} else ""
            market_entry_client_id = deterministic_client_order_id(ticker, side, "market-entry", self.config) if side in {"yes", "no"} else ""
            entry_client_ids = {maker_client_id, fallback_client_id, market_entry_client_id} - {""}
            if client_id in entry_client_ids:
                # A timed-out POST can still have created a resting maker.
                # Recover its exchange ID before any entry/stop management so
                # future cancellation is not attempted against a missing ID.
                exchange_order_id = str(field(order, "order_id") or "") or None
                local = next((item for item in record.setdefault("entry_orders", []) if isinstance(item, dict) and (
                    (exchange_order_id and item.get("order_id") == exchange_order_id)
                    or item.get("client_order_id") == client_id
                )), None)
                if local is None:
                    local = {"order_id": exchange_order_id, "client_order_id": client_id}
                    record["entry_orders"].append(local)
                local.update({
                    "order_id": exchange_order_id, "client_order_id": client_id,
                    "entry_phase": "market_entry" if client_id == market_entry_client_id else "market_fallback" if client_id == fallback_client_id else "maker",
                    "fill_count": format(Decimal(str(order_fill_count(order))), "f"),
                    "remaining_count": format(Decimal(str(order_remaining_count(order) or "0")), "f"),
                    "average_fill_price": format(Decimal(str(order_average_position_price(order, side, float(self.config["entry_price"])))), "f"),
                    "fees_paid": format(Decimal(str(order_fee_total(order))), "f"),
                    "reconciled_from_open_order": True,
                })
                self.trip("unconfirmed_managed_entry_recovered")
                if record.get("status") in {"ERROR_RECONCILIATION", "RECONCILIATION_PENDING"}:
                    self.transition(record, "ENTRY_PENDING", "startup_recovered_unconfirmed_resting_entry")
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
            accounting_reconciled = self.reconstruct_entry_accounting_from_fills(record, fill_rows, abs(raw))
            if not accounting_reconciled:
                # Keep stop management alive for the actual exposure, but do
                # not manufacture a cost basis or advance recovery from it.
                self.trip("entry_fill_accounting_unreconciled")
                self.audit(
                    "reconciliation_discrepancy", ticker=ticker, reason="entry_fill_accounting_unreconciled",
                    reconciliation=record["entry_accounting_reconciliation"],
                )
            self.update_effective_stop_price(record)
            if record.get("status") not in {"STOP_PENDING", "POSITION_OPEN"}:
                # This includes historical ERROR_RECONCILIATION records.  An
                # exchange-confirmed position is never allowed to remain in a
                # terminal local status where stop management would ignore it.
                self.transition(record, "POSITION_OPEN", "startup_authoritative_position")
        # Settlement/current-market discovery is independently retried during
        # the event loop.  Its result is recorded here for startup audit, but
        # a temporary discovery outage cannot make us forget known exposure.
        await self.discover(rest)
        # Rebuild aggregate maker/IOC metrics from the persisted per-market
        # fill facts on every startup.  This is safe across Actions handoffs
        # and does not infer a fill from a merely requested order.
        self.refresh_entry_execution_metrics()
        fill_count = len(fill_rows)
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
        previous_total = Decimal(str(record.get("actual_quantity") or "0"))
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
            self.update_effective_stop_price(record)
        self.note_entry_fill_observed(record, previous_total, total, "exchange_order_refresh")
        self.note_entry_execution_summary(record, "exchange_order_refresh")
        return total

    async def cancel_entry_orders_and_confirm(
        self, rest: KalshiREST, record: dict[str, Any], *, next_action: str, executable_bid: Decimal | None = None,
    ) -> bool:
        """Cancel every potentially resting entry and refuse to proceed on doubt.

        A replacement IOC or reduce-only exit is safe only after every earlier
        entry is known to be gone.  ``cancel_order`` returns an exchange
        acknowledgement; test doubles written before that contract are also
        accepted when they explicitly set the remaining quantity to zero.
        """

        unconfirmed: list[dict[str, Any]] = []
        for order in record.get("entry_orders", []):
            # The protected replacement is IOC and therefore cannot remain a
            # future source of exposure after its response.  Only the GTC
            # maker (or a legacy order without a recorded phase) must be
            # canceled before an exit/replacement may proceed.
            if order.get("entry_phase", "maker") != "maker":
                continue
            remaining = Decimal(str(order.get("remaining_count") or "0"))
            if remaining <= 0:
                continue
            # A v8 dry-run maker record has no exchange order ID by design.
            # It is simulated public-trade evidence, not an order that can
            # fill after its shadow deadline.  Treating it as an uncertain
            # exchange cancellation was the source of the stale
            # ENTRY_CANCEL_UNCONFIRMED loop observed on August 17.  Mark it
            # conclusively closed locally; real/missing-ID orders still fail
            # closed below.
            if self.dry_run and not order.get("order_id") and str(order.get("status") or "").startswith("shadow_"):
                order["remaining_count"] = "0.00"
                order["status"] = "shadow_cancelled"
                order["cancelled_at"] = utc_now()
                continue
            try:
                acknowledged = await rest.cancel_order(order, self.dry_run)
            except Exception as exc:  # Defensive: adapters must never turn an exception into permission to replace.
                order["cancel_error"] = type(exc).__name__
                acknowledged = False
            confirmed = bool(acknowledged) or Decimal(str(order.get("remaining_count") or "0")) <= 0
            if not confirmed:
                unconfirmed.append({
                    "order_id": order.get("order_id"), "client_order_id": order.get("client_order_id"),
                    "remaining_count": str(order.get("remaining_count") or "0"),
                    "cancel_error": order.get("cancel_error"),
                })
        if not unconfirmed:
            record.pop("entry_cancel_pending", None)
            record["entry_cancellation_confirmed_at"] = utc_now()
            return True

        record["entry_cancel_pending"] = {
            "at": utc_now(), "next_action": next_action, "orders": unconfirmed,
            "executable_bid": None if executable_bid is None else format(executable_bid, "f"),
        }
        self.trip("entry_cancellation_unconfirmed")
        self.transition(record, "ENTRY_CANCEL_UNCONFIRMED", "entry_cancellation_unconfirmed")
        self.audit(
            "entry_cancellation_unconfirmed", ticker=record["ticker"], next_action=next_action, orders=unconfirmed,
        )
        return False

    async def resume_unconfirmed_entry_cancellation(
        self, rest: KalshiREST, feed: KalshiLiveFeed, record: dict[str, Any],
    ) -> bool:
        """Retry cancellation reconciliation without creating new exposure."""

        pending = record.get("entry_cancel_pending")
        if record.get("status") != "ENTRY_CANCEL_UNCONFIRMED" or not isinstance(pending, dict):
            return False
        action = str(pending.get("next_action") or "finish_entry")
        bid_text = pending.get("executable_bid")
        bid = Decimal(str(bid_text)) if bid_text is not None else None
        if not await self.cancel_entry_orders_and_confirm(rest, record, next_action=action, executable_bid=bid):
            return True

        filled = self.refresh_shadow_entry(feed, record) if self.dry_run else await self.refresh_entry(rest, record)
        if not self.dry_run:
            exchange_position = await rest.position_for_ticker(record["ticker"])
            if exchange_position is None:
                self.trip("entry_cancellation_position_reconciliation_failed")
                self.transition(record, "RECONCILIATION_PENDING", "position_unknown_after_entry_cancellation")
                return True
            filled = max(filled, abs(Decimal(str(exchange_position))))
            record["actual_quantity"] = format(filled, "f")
            self.state["current_position"] = record["actual_quantity"]
        if action == "stop":
            if filled > 0:
                await self.close_at_stop(rest, record, bid or Decimal(self.config["stop_price"]), entries_confirmed=True)
            else:
                self.finish_entry_attempt(record, filled, "entry_cancel_confirmed_before_stop")
            return True

        # An uncertain cancellation is never followed by an IOC replacement.
        # Once it is resolved we either manage the confirmed position or count
        # a strict zero fill, preserving the recovery state in the latter case.
        self.finish_entry_attempt(record, filled, "entry_cancel_confirmed_without_replacement")
        return True

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
        limit = Decimal(str(order.get("position_price") or record.get("maker_entry_price") or self.config["entry_price"]))
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
        previous_total = Decimal(str(record.get("actual_quantity") or "0"))
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
            "average_fill_price": format(limit, "f"), "fees_paid": "0",
            "shadow_fill_evidence": {"model": "conservative_trade_through", "eligible_trades": eligible, "eligible_trade_quantity": format(evidence_count, "f")},
        })
        record["actual_quantity"] = format(fill, "f")
        self.state["current_position"] = record["actual_quantity"]
        self.state["average_entry"] = format(limit, "f") if fill else None
        if fill > 0:
            self.update_effective_stop_price(record)
        self.note_entry_fill_observed(record, previous_total, fill, "shadow_trade_through")
        self.note_entry_execution_summary(record, "shadow_trade_through_refresh")
        return fill

    def entry_deadline(self, record: dict[str, Any]) -> float:
        """The dynamic-maker phase ends no later than the first minute."""

        configured = float(record["market_open_epoch"]) + int(self.config["entry_timeout_seconds"])
        return min(float(record["market_close_epoch"]) - 1.0, configured)

    def finish_entry_attempt(self, record: dict[str, Any], filled: Decimal, reason: str) -> None:
        """Close an exhausted entry attempt without ever changing recovery on a zero fill."""

        self.note_entry_execution_summary(record, reason)
        self.note_entry_attempt_completed(record, filled, reason)
        if filled > 0:
            self.update_effective_stop_price(record)
            self.transition(record, "POSITION_OPEN", reason)
            self.audit(
                "entry_completed", ticker=record["ticker"], reason=reason,
                actual_quantity=format(filled, "f"), entry_execution_type=record["entry_execution_type"],
                entry_execution_summary=record["entry_execution_summary"],
            )
            return
        self.transition(record, "ZERO_FILL", reason)
        self.state["sizing"] = zero_fill_snapshot(self.current_parameters(), self.state.get("sizing"))
        self.note_zero_fill()
        self.audit("zero_fill", ticker=record["ticker"], reason=reason, entry_execution_type=record["entry_execution_type"])

    def reserve_shadow_entry_cash(self, quantity: Decimal, price: Decimal) -> None:
        """Reserve only simulated exposure actually supported by a fresh displayed quote."""

        if quantity <= 0:
            return
        metrics = self.shadow_metrics()
        reserved = Decimal(str(metrics["reserved_cash"])) + quantity * price
        metrics["reserved_cash"] = format(reserved, "f")
        metrics["max_reserved_cash"] = format(max(Decimal(str(metrics["max_reserved_cash"])), reserved), "f")

    async def submit_market_fallback(
        self, rest: KalshiREST, feed: KalshiLiveFeed, record: dict[str, Any], filled: Decimal,
    ) -> None:
        """Submit one idempotent, price-protected IOC for the post-maker remainder.

        Kalshi's safe equivalent of a market buy is an immediate-or-cancel
        limit at the current executable ask.  It cannot pay through that
        observed price, and it is never sent at or below the configured stop.
        """

        if record.get("market_fallback_attempted"):
            return
        # Do not trust a caller's prior cancellation attempt.  The maker must
        # be confirmed gone immediately before a replacement IOC is eligible.
        if not await self.cancel_entry_orders_and_confirm(rest, record, next_action="finish_entry"):
            return
        record["market_fallback_attempted"] = True
        side = str(record["signal_side"])
        intended = Decimal(str(record["intended_quantity"]))

        if not self.dry_run:
            exchange_position = await rest.position_for_ticker(record["ticker"])
            if exchange_position is None:
                self.trip("market_fallback_position_reconciliation_failed")
                return
            exchange_position = Decimal(str(exchange_position))
            if (side == "yes" and exchange_position < 0) or (side == "no" and exchange_position > 0):
                self.trip("market_fallback_position_direction_mismatch")
                return
            filled = max(filled, abs(exchange_position))
            record["actual_quantity"] = format(filled, "f")

        remaining = round_shares(max(Decimal("0"), intended - filled))
        if remaining == 0:
            self.finish_entry_attempt(record, filled, "maker_fully_filled_before_market_fallback")
            return

        quote, quote_state = feed.executable_shadow_quote(
            record["ticker"], side, 0.0, float(self.config["max_stale_quote_seconds"]),
        )
        if quote is None:
            record["market_fallback"] = {"attempted_at": utc_now(), "status": "not_submitted", "reason": quote_state}
            self.finish_entry_attempt(record, filled, "market_fallback_quote_unavailable")
            return
        price = Decimal(str(quote["economic_price"]))
        if price <= Decimal(self.config["stop_price"]):
            record["market_fallback"] = {
                "attempted_at": utc_now(), "status": "not_submitted", "reason": "best_available_price_at_or_below_stop",
                "best_available_price": format(price, "f"), "quote": quote,
            }
            self.finish_entry_attempt(record, filled, "market_fallback_at_or_below_stop")
            return

        available = self.shadow_available_cash() if self.dry_run else await rest.balance_decimal()
        required = remaining * price
        if self.dry_run:
            metrics = self.shadow_metrics()
            metrics["max_required_cash"] = format(max(Decimal(str(metrics["max_required_cash"])), required), "f")
        if available is None or available < required:
            details = {
                "at": utc_now(), "available_balance": None if available is None else format(available, "f"),
                "required_cash": format(required, "f"), "quantity": format(remaining, "f"),
                "best_available_price": format(price, "f"),
            }
            record["market_fallback"] = {"attempted_at": utc_now(), "status": "not_submitted", "reason": "insufficient_cash", **details}
            if self.dry_run:
                self.shadow_metrics()["funding_failures"] = int(self.shadow_metrics()["funding_failures"]) + 1
            if filled > 0:
                self.note_entry_attempt_completed(record, filled, "market_fallback_insufficient_cash_partial_maker_fill")
                self.transition(record, "POSITION_OPEN", "market_fallback_insufficient_cash_partial_maker_fill")
            else:
                record["funding_failure"] = details
                self.note_entry_attempt_completed(record, Decimal("0"), "market_fallback_insufficient_cash")
                self.transition(record, "FUNDING_FAILURE", "market_fallback_insufficient_cash")
            self.audit("funding_failure", ticker=record["ticker"], **details)
            return

        client_id = deterministic_client_order_id(record["ticker"], side, "market-fallback", self.config)
        if self.dry_run:
            # The shadow IOC is bounded by the fresh displayed top-of-book
            # size; it is evidence of a possible IOC fill, never an exchange
            # execution claim.
            displayed = Decimal(str(quote.get("displayed_depth") or "0"))
            shadow_fill = round_shares(min(remaining, displayed))
            affordable = (self.shadow_available_cash() / price).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
            shadow_fill = min(shadow_fill, affordable)
            order = {
                "order_id": None, "client_order_id": client_id, "ticker": record["ticker"], "side": side,
                "quantity": format(remaining, "f"), "position_price": format(price, "f"),
                "time_in_force": "immediate_or_cancel", "post_only": False,
                "fill_count": format(shadow_fill, "f"), "remaining_count": format(round_shares(remaining - shadow_fill), "f"),
                "average_fill_price": format(price, "f"), "fees_paid": "0", "entry_phase": "market_fallback",
                "status": "shadow_ioc_filled" if shadow_fill == remaining else "shadow_ioc_partial_or_unfilled",
                "shadow_execution": "fresh_displayed_top_of_book_ioc", "shadow_quote": quote,
                "submitted_at": utc_now(),
            }
            self.reserve_shadow_entry_cash(shadow_fill, price)
        else:
            order = await rest.create_order(
                ticker=record["ticker"], side=side, position_price=float(price), quantity=float(remaining),
                tif="immediate_or_cancel", expiration_time=None, dry_run=False, order_key="hybrid-market-fallback",
                post_only=False, client_order_id_override=client_id,
            )
            order["entry_phase"] = "market_fallback"
            order["fallback_quote"] = quote
            if order.get("status") in {"submit_failed", "paused", "direction_mismatch"}:
                # Preserve the deterministic client ID even when the HTTP
                # response is unknown.  Startup can then tie a late exchange
                # fill back to this record instead of inventing P&L.
                record.setdefault("entry_orders", []).append(order)
                self.note_entry_order_submitted(record, order, "market_fallback")
                record["market_fallback"] = {"attempted_at": utc_now(), "status": "submission_unknown_or_rejected", "quote": quote}
                self.trip("market_fallback_submission_unknown")
                self.transition(record, "RECONCILIATION_PENDING", "market_fallback_submission_unknown")
                return
        record.setdefault("entry_orders", []).append(order)
        self.note_entry_order_submitted(record, order, "market_fallback")
        record["market_fallback"] = {
            "attempted_at": utc_now(), "status": "submitted", "requested_quantity": format(remaining, "f"),
            "best_available_price": format(price, "f"), "client_order_id": client_id,
            "exchange_order_id": order.get("order_id"), "quote": quote,
        }
        previous_total = Decimal(str(record.get("actual_quantity") or "0"))
        final_filled = (
            filled + Decimal(str(order.get("fill_count") or "0"))
            if self.dry_run else await self.refresh_entry(rest, record)
        )
        record["actual_quantity"] = format(final_filled, "f")
        self.state["current_position"] = record["actual_quantity"]
        if self.dry_run:
            self.note_entry_fill_observed(record, previous_total, final_filled, "shadow_market_ioc")
        if self.dry_run and final_filled > 0:
            self.state["average_entry"] = format(self.entry_cost(record) / final_filled, "f")
        self.finish_entry_attempt(record, final_filled, "market_fallback_ioc_completed")
        self.audit(
            "market_fallback_submitted", ticker=record["ticker"], side=side,
            requested_quantity=format(remaining, "f"), best_available_price=format(price, "f"),
            client_order_id=client_id, exchange_order_id=order.get("order_id"), shadow=self.dry_run,
            entry_execution_type=record["entry_execution_type"],
            entry_execution_summary=record["entry_execution_summary"],
        )

    async def submit_immediate_market_entry(
        self, rest: KalshiREST, feed: KalshiLiveFeed, record: dict[str, Any], now: float,
    ) -> None:
        """Submit the one v9 entry attempt as a protected IOC at the fresh ask.

        Kalshi does not need an unbounded buy instruction here: an IOC limit at
        the current executable selected-side ask is the safe market-order
        equivalent.  It either fills immediately at no worse than the just
        observed price or leaves no resting order.  Unlike the retired maker
        path, this method never submits GTC/post-only entry exposure and
        never performs a maker cancellation before entering.
        """

        if record.get("market_entry_attempted"):
            return
        record["market_entry_attempted"] = True
        side = str(record["signal_side"])
        intended = Decimal(str(record["intended_quantity"]))
        quote, quote_state = feed.executable_shadow_quote(
            record["ticker"], side, 0.0, float(self.config["max_stale_quote_seconds"]),
        )
        if quote is None:
            # A missing/stale book is not evidence of a zero fill.  Permit the
            # outer loop to retry the *same*, deterministic IOC attempt while
            # the configured opening-lateness window remains open.
            record["market_entry_attempted"] = False
            record["market_entry"] = {
                "attempted_at": utc_now(), "status": "waiting_for_fresh_executable_book", "reason": quote_state,
            }
            LOG.warning(
                "MARKET IOC WAIT | ticker=%s side=%s reason=%s age_after_open=%.3fs",
                record["ticker"], side.upper(), quote_state, now - float(record["market_open_epoch"]),
            )
            return
        price = Decimal(str(quote["economic_price"]))
        stop = Decimal(str(record.get("stop_floor_price") or self.config["stop_price"]))
        if price <= stop:
            record["market_entry"] = {
                "attempted_at": utc_now(), "status": "not_submitted",
                "reason": "best_available_price_at_or_below_fixed_stop",
                "best_available_price": format(price, "f"), "quote": quote,
            }
            LOG.warning(
                "MARKET IOC NO ENTRY | ticker=%s side=%s ask=$%s fixed_stop=$%s reason=at_or_below_stop",
                record["ticker"], side.upper(), format(price, "f"), format(stop, "f"),
            )
            self.finish_entry_attempt(record, Decimal("0"), "market_entry_at_or_below_fixed_stop")
            return

        if not self.dry_run:
            exchange_position = await rest.position_for_ticker(record["ticker"])
            if exchange_position is None:
                self.trip("market_entry_position_reconciliation_failed")
                self.transition(record, "RECONCILIATION_PENDING", "market_entry_position_unknown")
                return
            if Decimal(str(exchange_position)) != 0:
                self.trip("existing_position_before_market_entry")
                return
        available = self.shadow_available_cash() if self.dry_run else await rest.balance_decimal()
        required = intended * price
        if self.dry_run:
            metrics = self.shadow_metrics()
            metrics["max_required_cash"] = format(max(Decimal(str(metrics["max_required_cash"])), required), "f")
        if available is None or available < required:
            details = {
                "at": utc_now(), "available_balance": None if available is None else format(available, "f"),
                "required_cash": format(required, "f"), "quantity": format(intended, "f"),
                "best_available_price": format(price, "f"),
            }
            record["funding_failure"] = details
            record["market_entry"] = {"attempted_at": utc_now(), "status": "not_submitted", "reason": "insufficient_cash", **details}
            self.note_entry_attempt_completed(record, Decimal("0"), "market_entry_insufficient_cash")
            self.transition(record, "FUNDING_FAILURE", "market_entry_insufficient_cash")
            if self.dry_run:
                self.shadow_metrics()["funding_failures"] = int(self.shadow_metrics()["funding_failures"]) + 1
            self.audit("funding_failure", ticker=record["ticker"], **details)
            return

        client_id = deterministic_client_order_id(record["ticker"], side, "market-entry", self.config)
        if self.dry_run:
            # Shadow fills are deliberately bounded by displayed top-of-book
            # depth.  They are simulated IOC participation, not an assertion
            # that an exchange order filled.
            displayed = Decimal(str(quote.get("displayed_depth") or "0"))
            filled = round_shares(min(intended, displayed))
            affordable = (self.shadow_available_cash() / price).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
            filled = min(filled, affordable)
            order = {
                "order_id": None, "client_order_id": client_id, "ticker": record["ticker"], "side": side,
                "quantity": format(intended, "f"), "position_price": format(price, "f"),
                "time_in_force": "immediate_or_cancel", "post_only": False,
                "fill_count": format(filled, "f"), "remaining_count": format(round_shares(intended - filled), "f"),
                "average_fill_price": format(price, "f"), "fees_paid": "0", "entry_phase": "market_entry",
                "status": "shadow_market_ioc_filled" if filled == intended else "shadow_market_ioc_partial_or_unfilled",
                "shadow_execution": "fresh_displayed_top_of_book_ioc", "shadow_quote": quote, "submitted_at": utc_now(),
            }
            self.reserve_shadow_entry_cash(filled, price)
        else:
            order = await rest.create_order(
                ticker=record["ticker"], side=side, position_price=float(price), quantity=float(intended),
                tif="immediate_or_cancel", expiration_time=None, dry_run=False, order_key="hybrid-market-entry",
                post_only=False, client_order_id_override=client_id,
            )
            order["entry_phase"] = "market_entry"
            order["market_entry_quote"] = quote
            if order.get("status") in {"submit_failed", "paused", "direction_mismatch"}:
                record.setdefault("entry_orders", []).append(order)
                self.note_entry_order_submitted(record, order, "market_entry")
                record["market_entry"] = {"attempted_at": utc_now(), "status": "submission_unknown_or_rejected", "quote": quote}
                self.trip("market_entry_submission_unknown")
                self.transition(record, "RECONCILIATION_PENDING", "market_entry_submission_unknown")
                return

        record.setdefault("entry_orders", []).append(order)
        self.note_entry_order_submitted(record, order, "market_entry")
        record["market_entry"] = {
            "attempted_at": utc_now(), "status": "submitted", "requested_quantity": format(intended, "f"),
            "best_available_price": format(price, "f"), "client_order_id": client_id,
            "exchange_order_id": order.get("order_id"), "quote": quote,
        }
        previous_total = Decimal(str(record.get("actual_quantity") or "0"))
        final_filled = Decimal(str(order.get("fill_count") or "0")) if self.dry_run else await self.refresh_entry(rest, record)
        record["actual_quantity"] = format(final_filled, "f")
        self.state["current_position"] = record["actual_quantity"]
        if self.dry_run:
            self.note_entry_fill_observed(record, previous_total, final_filled, "shadow_market_ioc")
            if final_filled > 0:
                self.state["average_entry"] = format(self.entry_cost(record) / final_filled, "f")
        self.finish_entry_attempt(record, final_filled, "immediate_market_ioc_completed")
        LOG.warning(
            "MARKET IOC ENTRY | ticker=%s side=%s requested=%s ask=$%s filled=%s status=%s shadow=%s",
            record["ticker"], side.upper(), format(intended, "f"), format(price, "f"),
            format(final_filled, "f"), record.get("status"), self.dry_run,
        )
        self.audit(
            "market_entry_submitted", ticker=record["ticker"], side=side, requested_quantity=format(intended, "f"),
            best_available_price=format(price, "f"), client_order_id=client_id, exchange_order_id=order.get("order_id"),
            shadow=self.dry_run, entry_execution_type=record["entry_execution_type"],
            entry_execution_summary=record["entry_execution_summary"],
        )

    async def submit_entry(self, rest: KalshiREST, feed: KalshiLiveFeed, record: dict[str, Any], now: float) -> None:
        if record.get("status") != "SIGNAL_PENDING" or not self.circuit_allows_entry():
            return
        if now > float(record["market_open_epoch"]) + int(self.config["entry_lateness_seconds"]):
            self.transition(record, "MISSED_SIGNAL", "entry_lateness_exceeded")
            return
        # v9 has exactly one protected market-equivalent IOC entry attempt.
        # This hard branch is intentionally above all retained v8 maker code
        # so a future edit cannot accidentally revive the old post-only/
        # cancellation/fallback state machine for the active configuration.
        if self.config["entry_execution_mode"] == "immediate_market_ioc":
            self.capture_opening_quote(feed, record, now)
            await self.submit_immediate_market_entry(rest, feed, record, now)
            return
        side = str(record["signal_side"])
        deadline = float(record.setdefault("entry_deadline_epoch", self.entry_deadline(record)))
        self.capture_opening_quote(feed, record, now)
        # At the deadline, use the fresh price-protected IOC fallback rather
        # than declaring a false zero fill because the WebSocket warmed up
        # after the first three wall-clock seconds.
        if now >= deadline:
            await self.submit_market_fallback(rest, feed, record, Decimal(str(record.get("actual_quantity") or "0")))
            return
        maker_price = self.derive_opening_maker_price(record, now)
        if maker_price is None:
            if record.get("opening_price_discovery", {}).get("reason") == "one_cent_below_opening_max_at_or_below_stop":
                # This is a completed, safety-preserving decision, not a
                # temporary lack of market data.  The strategy may not enter
                # at 40c or lower because that would already be at its stop.
                self.finish_entry_attempt(record, Decimal("0"), "opening_price_discovery_at_or_below_stop")
                return
            # A valid discovery sample starts only once the first fresh book
            # has been observed. Keep the market eligible through the one
            # minute deadline instead of converting startup latency to a
            # zero-fill trading decision.
            last_log = float(record.get("entry_discovery_wait_logged_epoch") or 0.0)
            if now - last_log >= 10.0:
                capture = record.get("opening_quote_capture", {})
                record["entry_discovery_wait_logged_epoch"] = now
                LOG.warning(
                    "ENTRY DISCOVERY WAIT | ticker=%s elapsed=%.3fs quotes=%s first_quote_lag=%s deadline_in=%.3fs",
                    record["ticker"], now - float(record["market_open_epoch"]),
                    capture.get("observation_count", 0), capture.get("first_capture_lag_seconds"), max(0.0, deadline - now),
                )
            return
        ask = self.selected_quote(feed, record["ticker"], side, "ask")
        if ask is None:
            return
        if ask <= Decimal(self.config["stop_price"]):
            # Do not create an order that would enter at/under the stop.
            record["entry_rejection_quote"] = {
                "at": utc_now(), "reason": "selected_side_started_at_or_below_stop",
                "selected_ask": format(ask, "f"),
            }
            self.audit("entry_rejected_at_or_below_stop", ticker=record["ticker"], **record["entry_rejection_quote"])
            self.finish_entry_attempt(record, Decimal("0"), "selected_side_started_at_or_below_stop")
            return
        if ask <= maker_price:
            # The dynamically derived limit cannot cross. Wait out the rest
            # of the one-minute maker phase, then evaluate the IOC fallback.
            record["maker_submission_skipped"] = {
                "at": utc_now(), "reason": "post_only_would_cross_current_book",
                "selected_ask": format(ask, "f"), "maker_entry_price": format(maker_price, "f"),
            }
            self.transition(record, "ENTRY_PENDING", "waiting_for_market_fallback_after_post_only_cross")
            return
        balance = self.shadow_available_cash() if self.dry_run else await rest.balance_decimal()
        quantity = Decimal(str(record["intended_quantity"]))
        required = quantity * maker_price
        if self.dry_run:
            metrics = self.shadow_metrics()
            metrics["max_required_cash"] = format(max(Decimal(str(metrics["max_required_cash"])), required), "f")
        if balance is None or balance < required:
            record["funding_failure"] = {"at": utc_now(), "available_balance": None if balance is None else format(balance, "f"), "required_cash": format(required, "f"), "quantity": format(quantity, "f")}
            self.note_entry_attempt_completed(record, Decimal("0"), "insufficient_authenticated_cash")
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
        expiry = int(deadline)
        if expiry <= int(now):
            self.transition(record, "MISSED_SIGNAL", "entry_expiry_elapsed")
            return
        order = await rest.create_order(
            ticker=record["ticker"], side=side, position_price=float(maker_price),
            quantity=float(quantity), tif="good_till_canceled", expiration_time=expiry, dry_run=self.dry_run,
            order_key="hybrid-entry", post_only=True, client_order_id_override=client_id,
        )
        order["entry_phase"] = "maker"
        # A type/API incompatibility cannot degrade to a non-post-only entry.
        if order.get("status") in {"submit_failed", "paused"}:
            # A failed response is not evidence that the exchange rejected
            # the request.  Persist its deterministic client ID and hold all
            # new exposure until authoritative reconciliation says otherwise.
            record["entry_orders"].append(order)
            self.note_entry_order_submitted(record, order, "maker")
            self.trip("maker_entry_submission_unknown")
            self.transition(record, "RECONCILIATION_PENDING", "post_only_entry_submission_unknown")
            return
        record["entry_orders"].append(order)
        self.note_entry_order_submitted(record, order, "maker")
        self.note_entry_execution_summary(record, "maker_limit_submitted")
        self.state["current_order_id"] = order.get("order_id")
        record["entry_deadline_epoch"] = expiry
        self.transition(record, "ENTRY_PENDING", "post_only_limit_submitted")
        self.audit(
            "entry_submitted", ticker=record["ticker"], side=side, requested_quantity=format(quantity, "f"),
            requested_price=format(maker_price, "f"), maximum_opening_ask=record.get("opening_price_discovery", {}).get("maximum_selected_best_ask"),
            client_order_id=client_id, exchange_order_id=order.get("order_id"), post_only=True,
        )

    def entry_cost(self, record: dict[str, Any]) -> Decimal:
        cost = Decimal("0")
        for order in record.get("entry_orders", []):
            filled = Decimal(str(order.get("fill_count") or "0"))
            average = Decimal(str(order.get("average_fill_price") or self.config["entry_price"]))
            fees = Decimal(str(order.get("fees_paid") or "0"))
            cost += filled * average + fees
        return cost

    async def manage_entry(self, rest: KalshiREST, feed: KalshiLiveFeed, record: dict[str, Any], now: float) -> None:
        if record.get("status") == "ENTRY_CANCEL_UNCONFIRMED":
            pending = record.get("entry_cancel_pending")
            if isinstance(pending, dict) and pending.get("next_action") != "stop":
                await self.resume_unconfirmed_entry_cancellation(rest, feed, record)
            return
        if record.get("status") not in {"ENTRY_PENDING", "ENTRY_PARTIAL"}:
            return
        filled = self.refresh_shadow_entry(feed, record) if self.dry_run else await self.refresh_entry(rest, record)
        if filled > 0:
            self.transition(record, "ENTRY_PARTIAL" if any(Decimal(str(item.get("remaining_count") or "0")) > 0 for item in record["entry_orders"]) else "POSITION_OPEN", "entry_fill_observed")
        side = str(record["signal_side"])
        ask = self.selected_quote(feed, record["ticker"], side, "ask")
        deadline = float(record.get("entry_deadline_epoch") or 0)
        if now >= deadline:
            if not await self.cancel_entry_orders_and_confirm(rest, record, next_action="finish_entry"):
                return
            # For shadow mode, preserve only the trade-through evidence that
            # existed before cancellation; later public trades cannot fill a
            # cancelled simulated maker order.
            filled = filled if self.dry_run else await self.refresh_entry(rest, record)
            await self.submit_market_fallback(rest, feed, record, filled)
            return
        if (ask is not None and ask <= Decimal(self.config["stop_price"])) or now >= float(record["market_close_epoch"]):
            if not await self.cancel_entry_orders_and_confirm(rest, record, next_action="finish_entry"):
                return
            filled = filled if self.dry_run else await self.refresh_entry(rest, record)
            self.finish_entry_attempt(record, filled, "selected_side_at_or_below_stop_before_fallback")

    async def close_at_stop(
        self, rest: KalshiREST, record: dict[str, Any], executable_bid: Decimal, *, entries_confirmed: bool = False,
    ) -> None:
        side = str(record["signal_side"])
        stop_price = self.stop_price_for_record(record)
        if not entries_confirmed and not await self.cancel_entry_orders_and_confirm(
            rest, record, next_action="stop", executable_bid=executable_bid,
        ):
            return
        if self.dry_run:
            quantity = Decimal(str(record.get("actual_quantity") or "0"))
            if quantity == 0:
                return
            self.note_stop_trigger(record, executable_bid, stop_price, quantity, shadow=True)
            exit_order = {
                "order_id": None, "fill_count": format(quantity, "f"), "remaining_count": "0",
                "average_fill_price": format(executable_bid, "f"), "fees_paid": "0",
                "shadow_execution": "fresh_executable_bid", "submitted_at": utc_now(),
            }
            record.setdefault("exit_orders", []).append(exit_order)
            self.note_stop_exit_submitted(record, exit_order)
            self.transition(record, "STOP_PENDING", "shadow_effective_stop_executable_bid")
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
        self.note_stop_trigger(record, executable_bid, stop_price, quantity, shadow=False)
        prior = record.get("exit_orders", [])
        # An IOC can be rejected or partially filled and still retain its
        # requested remaining quantity in the local record.  It is not a
        # resting order.  The just-read exchange position is authoritative,
        # so retry with only that residual instead of permanently returning.
        order = await rest.create_reduce_only_exit(
            ticker=record["ticker"], held_side=side, economic_exit_price=float(executable_bid), quantity=float(quantity),
            dry_run=self.dry_run, order_key=f"hybrid-stop-{len(prior)}",
            client_order_id_override=deterministic_client_order_id(record["ticker"], side, f"stop-{len(prior)}", self.config),
        )
        record.setdefault("exit_orders", []).append(order)
        self.note_stop_exit_submitted(record, order)
        self.transition(record, "STOP_PENDING", "effective_stop_executable_bid")
        self.audit(
            "stop_triggered", ticker=record["ticker"], side=side, executable_bid=format(executable_bid, "f"),
            effective_stop_price=format(stop_price, "f"), actual_average_entry_price=record.get("actual_average_entry_price"),
            quantity=format(quantity, "f"), exchange_order_id=order.get("order_id"),
            first_fill_to_stop_trigger_seconds=record.get("stop_timing", {}).get("first_fill_to_stop_trigger_seconds"),
            stop_trigger_to_first_exit_submission_seconds=record.get("stop_timing", {}).get("stop_trigger_to_first_exit_submission_seconds"),
        )

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
        if record.get("entry_accounting_reconciled") is False:
            # Exchange exposure may already be flat, but a made-up entry cost
            # would corrupt both realized P&L and the shared recovery state.
            # Keep the durable exception for a later fill-history reconcile and
            # prevent all new entries through the circuit breaker.
            self.trip("realized_pnl_requires_entry_fill_reconciliation")
            record["realized_pnl_blocked"] = {
                "at": utc_now(), "method": method, "settlement_id": settlement_id,
                "reason": "entry_fill_accounting_unreconciled",
            }
            self.transition(record, "ACCOUNTING_RECONCILIATION_PENDING", "realized_pnl_blocked_missing_entry_fills")
            self.audit("realized_pnl_blocked", ticker=record["ticker"], method=method, settlement_id=settlement_id)
            return
        self.note_entry_execution_summary(record, "realized_trade_finalization")
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
        self.audit(
            "trade_closed", ticker=record["ticker"], method=method, net_pnl=format(net, "f"),
            quantity=record.get("actual_quantity"), recovery_reset=changes["recovery_reset"],
            base_increased=changes["base_increased"], entry_execution_type=record["entry_execution_type"],
            entry_execution_summary=record["entry_execution_summary"],
        )
        if self.dry_run:
            metrics = self.shadow_metrics()
            LOG.warning(
                "SHADOW PNL | ticker=%s method=%s net=%s cumulative=%s balance=%s max_drawdown=%s "
                "completed=%d stops=%d settlements=%d",
                record["ticker"], method, format(net, "f"), self.state["cumulative_realized_pnl"],
                metrics["balance"], metrics["max_drawdown"], metrics["completed_trades"],
                metrics["stop_count"], metrics["settlement_count"],
            )

    async def finalize_stop(self, record: dict[str, Any]) -> None:
        self.note_stop_position_closed(record)
        proceeds, exit_fees = self.exit_proceeds(record)
        net = proceeds - exit_fees - self.entry_cost(record)
        self.record_realized(record, net, "stop", f"{record['ticker']}:stop")

    async def manage_stop(self, rest: KalshiREST, feed: KalshiLiveFeed, record: dict[str, Any]) -> None:
        if record.get("status") == "ENTRY_CANCEL_UNCONFIRMED":
            pending = record.get("entry_cancel_pending")
            if isinstance(pending, dict) and pending.get("next_action") == "stop":
                await self.resume_unconfirmed_entry_cancellation(rest, feed, record)
            return
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
        effective_stop = self.stop_price_for_record(record)
        self.note_stop_monitor_quote(record, bid, effective_stop)
        if bid is not None and bid <= effective_stop:
            await self.close_at_stop(rest, record, bid)

    async def verify_post_stop_settlement(self, rest: KalshiREST, record: dict[str, Any], now: float) -> None:
        """Record the later official result of a stopped position without P&L changes."""

        if record.get("post_stop_settlement_outcome") in {"yes", "no"}:
            return
        # Settlement can lag the market close. Bound REST retries while the
        # official result is unavailable; this observation never changes an
        # already-realized stop P&L or the recovery/base state.
        next_check = float(record.get("post_stop_settlement_next_check_epoch") or 0)
        if now < next_check:
            return
        record["post_stop_settlement_next_check_epoch"] = now + 60.0
        market = await rest.get_market(record["ticker"])
        outcome = market_result(market) if market is not None else None
        if outcome not in {"yes", "no"}:
            return
        would_have_won = outcome == record.get("signal_side")
        record.update({
            "post_stop_settlement_outcome": outcome,
            "post_stop_settlement_timestamp": utc_now(),
            "post_stop_would_have_settled_correctly": would_have_won,
        })
        self.audit(
            "post_stop_settlement_verified", ticker=record["ticker"], outcome=outcome,
            would_have_settled_correctly=would_have_won,
            actual_average_entry_price=record.get("actual_average_entry_price"),
            effective_stop_price=record.get("effective_stop_price"),
        )
        self.checkpoint("post_stop_settlement_verified")

    async def settle(self, rest: KalshiREST, record: dict[str, Any], now: float) -> None:
        if record.get("status") == "CLOSED":
            if record.get("realized_method") == "stop" and now >= float(record["market_close_epoch"]):
                await self.verify_post_stop_settlement(rest, record, now)
            return
        if record.get("status") not in {"ENTRY_PARTIAL", "POSITION_OPEN", "SETTLEMENT_PENDING"} or now < float(record["market_close_epoch"]):
            return
        if not await self.cancel_entry_orders_and_confirm(rest, record, next_action="finish_entry"):
            return
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

    async def reconcile_uncertain_record(self, rest: KalshiREST, record: dict[str, Any]) -> bool:
        """Restore protection if an earlier create/cancel response was unknown."""

        if record.get("status") not in {"ERROR_RECONCILIATION", "RECONCILIATION_PENDING"}:
            return False
        position = await rest.position_for_ticker(record["ticker"])
        if position is None:
            self.trip("uncertain_order_position_lookup_failed")
            return True
        signed = Decimal(str(position))
        if signed == 0:
            # Do not infer rejection from a zero position: the uncertain order
            # may still be resting.  New entry remains disabled until a full
            # startup reconciliation can account for open orders/fills.
            return True
        side = str(record.get("signal_side") or "")
        if (side == "yes" and signed < 0) or (side == "no" and signed > 0) or side not in {"yes", "no"}:
            self.trip("uncertain_order_position_direction_mismatch")
            return True
        record["actual_quantity"] = format(abs(signed), "f")
        self.state["current_position"] = record["actual_quantity"]
        try:
            payload = await rest.get_raw_json("/portfolio/fills", {"limit": 1000})
            rows = payload.get("fills", []) if isinstance(payload, dict) and isinstance(payload.get("fills"), list) else []
            accounting_reconciled = self.reconstruct_entry_accounting_from_fills(record, rows, abs(signed))
        except Exception:
            accounting_reconciled = False
        if not accounting_reconciled:
            self.trip("uncertain_order_entry_accounting_unreconciled")
        self.update_effective_stop_price(record)
        self.transition(record, "POSITION_OPEN", "uncertain_submission_authoritative_position")
        self.audit(
            "uncertain_submission_reconciled", ticker=record["ticker"], quantity=record["actual_quantity"],
            entry_accounting_reconciled=accounting_reconciled,
        )
        return True

    async def reconcile_active(self, rest: KalshiREST, feed: KalshiLiveFeed, now: float) -> None:
        for record in list(self.state["markets"].values()):
            if not isinstance(record, dict):
                continue
            await self.verify_previous_outcome(rest, record)
            await self.reconcile_uncertain_record(rest, record)
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
        self.state["handoff"] = {"ready": False, "started_at": utc_now(), "minimum_run_seconds": run_seconds}
        last_update = feed.update_count
        handoff_wait_logged_at = 0.0
        while True:
            now = time.time()
            if time.monotonic() - self.last_market_discovery >= float(self.config["market_discovery_interval_seconds"]):
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
                        # Keep collecting complete top-of-book observations
                        # after the v10 IOC attempt as durable calibration
                        # evidence; they cannot rewrite an executed order.
                        self.capture_opening_quote(feed, record, now)
                        await self.submit_entry(rest, feed, record, now)
            if time.monotonic() - self.last_reconcile >= float(self.config["reconciliation_interval"]):
                await self.reconcile_active(rest, feed, now)
                self.last_reconcile = time.monotonic()
            if time.monotonic() - self.last_heartbeat >= 60:
                sizing = sizing_state(self.current_parameters(), self.state.get("sizing"))
                execution = self.refresh_entry_execution_metrics()
                timing = self.refresh_execution_timing_metrics()
                record = self.state.get("markets", {}).get(active["ticker"], {}) if active else {}
                capture = record.get("opening_quote_capture", {}) if isinstance(record, dict) else {}
                deadline = record.get("entry_deadline_epoch") if isinstance(record, dict) else None
                deadline_in = (
                    round(max(0.0, float(deadline) - now), 3)
                    if deadline not in (None, "") else None
                )
                entry_latency = timing["entry_first_fill_from_market_open"]
                stop_latency = timing["stop_trigger_from_first_fill"]
                shadow = self.shadow_metrics() if self.dry_run else {}
                LOG.warning(
                    "HEARTBEAT | mode=%s ticker=%s state=%s base=%s exponent=%d target=%s "
                    "deficit=%s threshold=%s tracked=%d filled=%d zero=%d funding_failures=%d "
                    "missed=%d maker_fills=%d ioc_fills=%d mixed=%d opening_quotes=%s "
                    "first_quote_lag=%s maker_limit=%s deadline_in=%s entry_fill_p50=%s "
                    "stop_from_fill_p50=%s shadow_balance=%s shadow_pnl=%s shadow_dd=%s "
                    "completed=%s stops=%s settlements=%s entry_mode=%s active=%s breaker=%s",
                    "DRY_RUN" if self.dry_run else "LIVE",
                    active and active["ticker"],
                    record.get("status") if isinstance(record, dict) else None,
                    sizing.base_share_count, sizing.recovery_exponent, sizing.prescribed_quantity(),
                    sizing.recovery_cycle_pnl, sizing.next_base_threshold,
                    execution["tracked_markets"], execution["markets_with_entry_fill"],
                    execution["zero_fill_markets"], execution["funding_failure_markets"],
                    execution["missed_signal_markets"], execution["maker_limit_fill_markets"],
                    execution["market_ioc_fill_markets"], execution["mixed_entry_markets"],
                    capture.get("observation_count"), capture.get("first_capture_lag_seconds"),
                    record.get("maker_entry_price") if isinstance(record, dict) else None,
                    deadline_in, entry_latency.get("median_seconds"), stop_latency.get("median_seconds"),
                    shadow.get("balance"), self.state.get("cumulative_realized_pnl"), shadow.get("max_drawdown"),
                    shadow.get("completed_trades"), shadow.get("stop_count"), shadow.get("settlement_count"),
                    self.config["entry_execution_mode"],
                    self.state.get("active_market"), self.state["circuit_breaker"].get("blocked"),
                )
                self.last_heartbeat = time.monotonic()
            elapsed = time.monotonic() - start
            if elapsed >= run_seconds:
                ready, details = self.handoff_ready(now)
                if ready:
                    self.state["handoff"] = {"ready": True, "at": utc_now(), "elapsed_seconds": round(elapsed, 3), **details}
                    self.audit("safe_handoff_ready", **details, elapsed_seconds=round(elapsed, 3))
                    self.checkpoint("safe_handoff_ready")
                    LOG.warning(
                        "SAFE HANDOFF READY | ticker=%s elapsed=%.1fs window=%s..%s; queuing may proceed.",
                        details["ticker"], elapsed, details["window_start_epoch"], details["window_end_epoch"],
                    )
                    return 0
                if time.monotonic() - handoff_wait_logged_at >= 60:
                    LOG.warning("HANDOFF DEFERRED | elapsed=%.1fs reason=%s", elapsed, details.get("reason"))
                    handoff_wait_logged_at = time.monotonic()
            self.checkpoint()
            last_update = await feed.wait_for_update(0.25, last_update)
        return 0


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--config", type=Path, default=Path("live_strategy_config.json"))
    result.add_argument("--state-file", type=Path, default=Path("data/kalshi_live_market_ioc_state.json"))
    result.add_argument("--audit-ledger", type=Path, default=Path("data/kalshi_live_market_ioc_audit.jsonl"))
    result.add_argument("--run-seconds", type=float, default=19_200)
    result.add_argument("--persist-config", action="store_true")
    result.add_argument("--reconcile-only", action="store_true")
    result.add_argument("--cancel-managed-entries", action="store_true", help="explicitly cancel only hybrid-prefixed resting orders; never opens or closes a position")
    result.add_argument("--reset-state", action="store_true")
    result.add_argument("--live-enabled", action="store_true")
    result.add_argument("--dry-run", action="store_true")
    result.add_argument(
        "--shadow-profile", choices=sorted(SHADOW_STOP_PROFILE_PRICES),
        help="isolated dry-run stop/profile identity; must agree with --stop-price",
    )
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
    shadow_only_lock = _bool(os.getenv("KALSHI_SHADOW_ONLY", "false"))
    live = live_mode_allowed(requested_live, environment_live, shadow_only_lock, bool(args.dry_run))
    dry_run = not live
    if requested_live and shadow_only_lock:
        LOG.warning("LIVE REQUEST BLOCKED | KALSHI_SHADOW_ONLY=true; forcing MODE=DRY_RUN")
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
