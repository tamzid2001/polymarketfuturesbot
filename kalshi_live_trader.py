"""Production KXBTC15M hybrid live trader.

This runner replaces the retired recovery runner in the GitHub workflow.  It
reuses the established Kalshi V2 REST/WebSocket/auth transport and executes the
pure fixed-point plan from :mod:`opposite_ladder_core`.  Shadow-account metrics
remain isolated from live state.  Prophet, directional-loss skipping, recovery
sizing, permanent base scaling, and position caps are not part of v14.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
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
    normalized_order_status,
    order_average_position_price,
    order_fee_total,
    order_fill_count,
    order_remaining_count,
    timestamp_epoch,
)
from live_state import (
    append_unique,
    clear_stale_current_order_pointer,
    config_hash,
    current_order_pointer_requires_recovery,
    load_state,
    save_state,
    utc_now,
)
from opposite_ladder_core import (
    build_opposite_ladder_plan,
    opposite_side,
    take_profit_triggered,
)
from strategy_core import (
    delayed_band_entry_decision,
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
# v14 is a hard compatibility boundary.  At/after 60 seconds, a sticky-side
# ask in the inclusive 53c..57c band freezes five post-only GTC BUY orders on
# the opposite side: exact complement 100-minus-sticky-ask (47c..43c) at 1x
# base and 40/30/20/10c at
# 2/4/8/16x base.  Recovery, permanent-base scaling, and position caps are not
# part of this contract. Contract revision 3 preserves revision 2's flatten
# path, which latches when the
# held-side executable bid reaches 51c OR the sticky-side executable ask falls
# to 51c. An older revision cannot load the revised canonical configuration.
ACTIVE_STRATEGY_VERSION = "kxbtc15m-opposite-ladder-live-v14"
ACTIVE_CONFIG_SCHEMA_VERSION = 14
# Operational release guard used by the production workflow.  Version 3 means
# opening entry prices come from the buffered price-only stream, not the first
# later complete-depth book.  This can advance without reinterpreting recovery
# accounting in an existing v11 state file.
OPENING_PRICE_CAPTURE_CONTRACT_VERSION = 3
# v2 requires enrichment from Kalshi's authoritative per-market endpoint when
# the bounded list response omits strike metadata. A list-only v1 worker must
# fail the workflow guard instead of silently restoring UNAVAILABLE targets.
BTC_TARGET_CAPTURE_CONTRACT_VERSION = 2
# v3 freezes an analytics-only resting limit one cent below the delayed
# threshold ask.  It requires subsequent public-trade volume for a fill and
# consumes that volume only once across the direct order and ladder rungs. v4
# replaces the non-executing 52c maker-exit experiment with the same direct
# 51c protective-exit rule used by live and primary shadow execution.
DELAYED_ENTRY_LADDER_CONTRACT_VERSION = 4
LIVE_STOP_SAFETY_CONTRACT_VERSION = 3
POSITION_CAP_CONTRACT_VERSION = 2
ENTRY_DELIVERY_CONTRACT_VERSION = 1
# v3 makes the first post-60-second sticky-side quote a terminal 53c..57c
# eligibility decision and derives the opposite-side initial tick exactly as
# 100 - sticky ask (47c..43c). This prevents spread-dependent out-of-band
# initial orders while retaining the reviewed v2 two-sided 51c flatten logic.
OPPOSITE_LADDER_CONTRACT_VERSION = 3
# A definitive HTTP rejection proves that the matching engine did not create
# an order. Keep the frozen strategy intent alive and retry it at a bounded
# cadence while the same market remains safe. Unknown POST outcomes are never
# handled by this path: they must first be reconciled by exact client ID.
ENTRY_REJECTION_RETRY_SECONDS = 1.0
ENTRY_RETRY_ABANDON_ASK_CENTS = 51
ENTRY_RETRYABLE_REJECTION_HTTP_STATUSES = frozenset({400, 404})
DELAYED_HYBRID_STOP_TRIGGER_CENTS = 51
DELAYED_HYBRID_MAKER_EXIT_CENTS = 51
DELAYED_HYBRID_HARD_STOP_CENTS = 51
BTC_TARGET_RECORD_FIELDS = (
    "btc_target_capture_version",
    "btc_target_status",
    "btc_target_price",
    "btc_target_price_display",
    "btc_target_source",
    "btc_target_comparison",
    "btc_target_yes_sub_title",
    "btc_target_no_sub_title",
    "market_title",
)
OPENING_CROSS_LADDER_TRIGGER_LEVELS = tuple(range(53, 60))
OPENING_CROSS_LADDER_TRIGGER_WINDOW_SECONDS = 60
OPENING_CROSS_LADDER_RUNGS: tuple[tuple[int, Decimal], ...] = (
    (50, Decimal("1.00")),
    (40, Decimal("2.00")),
    (30, Decimal("4.00")),
    (20, Decimal("8.00")),
    (10, Decimal("16.00")),
)
MARKET_DISCOVERY_LOOKBACK_SECONDS = 3_600
MARKET_DISCOVERY_LOOKAHEAD_SECONDS = 3_600
TERMINAL_STATES = {
    "CLOSED", "ZERO_FILL", "ENTRY_FILTERED", "FUNDING_FAILURE", "MISSED_SIGNAL",
    "ERROR_RECONCILIATION",
}
# A cancellation acknowledgement or an order-submission response can be
# uncertain.  These are deliberately active, risk-managed states: they block
# every new entry, are retried through exchange reconciliation, and must not
# be treated like a terminal bookkeeping error while an exchange position may
# still exist.
ACTIVE_STATES = {
    "SIGNAL_PENDING", "ENTRY_PENDING", "ENTRY_PARTIAL", "POSITION_OPEN", "STOP_PENDING", "SETTLEMENT_PENDING",
    "ENTRY_CANCEL_UNCONFIRMED", "RECONCILIATION_PENDING", "ACCOUNTING_RECONCILIATION_PENDING",
    "MAKER_EXIT_PENDING", "MAKER_EXIT_PARTIAL", "MAKER_EXIT_CANCEL_UNCONFIRMED", "HARD_STOP_PENDING",
}
ENTRY_NOTIONAL_QUANTUM = Decimal("0.0001")
_AUDIT_TRANSIENT_KEYS = frozenset({"processed_public_trade_keys"})


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


def startup_order_check_allows_worker(state: dict[str, Any], worker_id: str) -> bool:
    """Require the current Actions run's exact create/cancel/flat proof.

    The workflow performs the probe in a separate process before launching the
    strategy.  This second gate prevents a future workflow edit from launching
    the LIVE worker after a skipped, stale, filled, or unresolved probe.
    """

    check = state.get("startup_order_check")
    if not isinstance(check, dict) or not worker_id or check.get("worker_id") != worker_id:
        return False
    if check.get("state") == "DEFERRED_TO_RISK_RECOVERY":
        # A startup probe must never be placed while durable state may still
        # represent exposure.  The checker persists this narrowly scoped
        # authorization for the *current* Actions run so the strategy worker
        # can reconcile/flatten risk.  Its still-latched breaker prevents new
        # entries until that reconciliation is authoritative.
        breaker = state.get("circuit_breaker", {})
        try:
            has_position = Decimal(str(state.get("current_position") or "0")) != 0
        except (ArithmeticError, TypeError, ValueError):
            return False
        has_managed_risk = bool(
            state.get("current_order_id")
            or has_position
            or any(
                isinstance(record, dict) and record.get("status") in ACTIVE_STATES
                for record in state.get("markets", {}).values()
            )
        )
        return bool(
            check.get("recovery_worker_allowed") is True
            and check.get("strategy_entries_allowed") is False
            and breaker.get("blocked") is True
            and str(check.get("breaker_reason") or "")
            == str(breaker.get("reason") or "")
            and has_managed_risk
        )
    try:
        zero = all(
            Decimal(str(check.get(key))) == 0
            for key in ("filled_quantity", "remaining_quantity", "position")
        )
        exact_probe = (
            Decimal(str(check.get("economic_limit"))) == Decimal("0.01")
            and Decimal(str(check.get("quantity"))) == Decimal("1.00")
        )
    except (ArithmeticError, TypeError, ValueError):
        return False
    exchange_index = check.get("exchange_index")
    return bool(
        check.get("state") == "CANCELED_NO_FILL"
        and check.get("ticker")
        and check.get("order_id")
        and check.get("client_order_id")
        and check.get("create_acknowledged") is True
        and check.get("cancel_acknowledged") is True
        and type(exchange_index) is int
        and exchange_index == 2
        and exact_probe
        and zero
    )


def state_has_reset_blocking_risk(state: dict[str, Any]) -> bool:
    """Return true when a fresh-state reset could forget managed exposure.

    A signal-only record is disposable because it has submitted no order and
    owns no position. Every order/position/reconciliation state remains a
    hard block until the authoritative exchange checks below also confirm the
    account is flat.
    """

    try:
        if Decimal(str(state.get("current_position") or "0")) != 0:
            return True
    except (ArithmeticError, TypeError, ValueError):
        return True
    if state.get("current_order_id"):
        return True
    reset_blocking_states = ACTIVE_STATES - {"SIGNAL_PENDING"}
    for record in state.get("markets", {}).values():
        if not isinstance(record, dict):
            continue
        status = str(record.get("status") or "")
        if status in reset_blocking_states:
            return True
        if status != "SIGNAL_PENDING":
            continue
        try:
            if Decimal(str(record.get("actual_quantity") or "0")) != 0:
                return True
        except (ArithmeticError, TypeError, ValueError):
            return True
        for key in ("entry_orders", "exit_orders"):
            for order in record.get(key, []):
                if not isinstance(order, dict):
                    return True
                try:
                    if Decimal(str(order.get("remaining_count") or "0")) != 0:
                        return True
                    if Decimal(str(order.get("fill_count") or "0")) != 0:
                        return True
                except (ArithmeticError, TypeError, ValueError):
                    return True
                if order.get("order_id") and str(order.get("status") or "").lower() in {
                    "resting", "pending", "open", "unknown", "submit_failed",
                }:
                    return True
    return False


def _iso_epoch(value: Any) -> float | None:
    """Parse persisted ISO or numeric timestamps without confusing ms and s."""

    if value in (None, ""):
        return None
    if isinstance(value, (int, float, Decimal)):
        numeric = float(value)
        return numeric / 1000.0 if abs(numeric) > 10_000_000_000 else numeric
    if isinstance(value, str):
        try:
            numeric = float(value)
        except ValueError:
            pass
        else:
            return numeric / 1000.0 if abs(numeric) > 10_000_000_000 else numeric
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
    if not amount.is_finite() or amount <= Decimal("0"):
        raise ValueError(f"{name} must be finite and positive")
    return format(amount, "f")


DECIMAL_CONFIG_FIELDS = {
    "entry_price", "stop_price", "starting_base", "recovery_multiplier", "first_base_threshold",
    "threshold_growth_multiplier", "base_increment", "provisional_outcome_threshold",
    "max_recovery_cycle_loss", "max_daily_realized_loss", "starting_shadow_balance", "maker_price_offset",
    "stop_baseline_entry_price",
}
OPTIONAL_DECIMAL_CONFIG_FIELDS: set[str] = set()
INTEGER_CONFIG_FIELDS = {
    "signal_delay_seconds", "entry_timeout_seconds", "entry_lateness_seconds", "outcome_observation_seconds",
    "max_recovery_exponent", "max_api_failures", "handoff_guard_seconds", "opening_quote_max_observations",
    "opening_price_discovery_seconds",
    "entry_limit_offset_cents", "shadow_entry_level_min_cents", "shadow_entry_level_max_cents",
    "shadow_entry_level_step_cents", "hybrid_stop_trigger_cents", "hybrid_maker_exit_cents",
    "hybrid_hard_stop_cents", "opening_quote_capture_seconds", "delayed_entry_threshold_cents",
    "delayed_entry_start_seconds", "delayed_entry_max_limit_cents",
    "delayed_entry_max_trigger_cents", "opposite_initial_limit_min_cents",
    "opposite_initial_limit_max_cents", "opposite_take_profit_cents",
}
FLOAT_CONFIG_FIELDS = {
    "stop_poll_interval", "reconciliation_interval", "max_outcome_quote_age_seconds", "max_stale_quote_seconds",
    "durable_checkpoint_interval_seconds", "market_discovery_interval_seconds",
}

# These profile names are deliberately part of the durable configuration hash.
# Experimental stop lanes are shadow-only and each receives its own state,
# ledger, concurrency key, and runtime-state ref.  They can never inherit the
# canonical 40c lane's recovery/base state or become a live configuration.
SHADOW_STOP_PROFILE_PRICES = {
    "sticky_stop_10": Decimal("0.10"),
    "sticky_stop_20": Decimal("0.20"),
    "sticky_stop_25": Decimal("0.25"),
    "sticky_stop_30": Decimal("0.30"),
    "sticky_stop_35": Decimal("0.35"),
    "sticky_stop_40": Decimal("0.40"),
    "delayed_53_57_stop_50": Decimal("0.50"),
    "delayed_53_57_exit_51": Decimal("0.51"),
    "opposite_ladder_53_58_take_profit_50": Decimal("0.50"),
    "opposite_ladder_53_58_flatten_51": Decimal("0.51"),
    "opposite_ladder_53_57_flatten_51": Decimal("0.51"),
}
CANONICAL_LIVE_SHADOW_PROFILE = "opposite_ladder_53_57_flatten_51"


def price_to_cents(value: Decimal | str, name: str = "price") -> int:
    """Convert a KXBTC15M price to an exact integer-cent tick.

    KXBTC15M is configured as a one-cent-tick strategy.  Rounding an unusual
    fractional-cent quote could move a live limit or stop, so fail closed
    instead of silently changing the exchange price.
    """

    price = decimal(value)
    scaled = price * Decimal("100")
    integral = scaled.to_integral_value()
    if scaled != integral or not 1 <= int(integral) <= 99:
        raise ValueError(f"{name} must be an exact supported 1c tick between 1c and 99c")
    return int(integral)


def cents_price(value: int) -> Decimal:
    if not 1 <= int(value) <= 99:
        raise ValueError("price cents must be between 1 and 99")
    return Decimal(int(value)) / Decimal("100")


def audit_safe_value(value: Any) -> Any:
    """Copy analytics payloads without unbounded replay-only dedupe arrays.

    The per-lane keys are operational cursors while a market is active; they
    are not strategy evidence.  Persisting the same 4,096 IDs in every ladder
    lane made individual audit rows exceed a megabyte.  Keep the count in the
    append-only ledger and all actual trigger/fill/price facts, but omit the
    redundant keys themselves.
    """

    if isinstance(value, dict):
        result = {
            key: audit_safe_value(item)
            for key, item in value.items()
            if key not in _AUDIT_TRANSIENT_KEYS
        }
        for key in _AUDIT_TRANSIENT_KEYS:
            items = value.get(key)
            if isinstance(items, list):
                result[f"{key}_count"] = len(items)
        return result
    if isinstance(value, list):
        return [audit_safe_value(item) for item in value]
    return value


def prune_finalized_public_trade_keys(record: dict[str, Any]) -> int:
    """Remove active-only dedupe buffers once settlement makes them immutable."""

    removed = 0

    def visit(value: Any) -> None:
        nonlocal removed
        if isinstance(value, dict):
            for key in tuple(value):
                if key in _AUDIT_TRANSIENT_KEYS:
                    items = value.pop(key)
                    removed += len(items) if isinstance(items, list) else 1
                else:
                    visit(value[key])
        elif isinstance(value, list):
            for item in value:
                visit(item)

    if record.get("analytics_settlement_finalized"):
        visit(record)
    return removed


def record_has_exchange_work(record: dict[str, Any]) -> bool:
    """Return whether a durable market record still represents order/risk work."""

    if record.get("current_order_id"):
        return True
    try:
        if Decimal(str(record.get("actual_quantity") or "0")) != 0:
            return True
    except (ArithmeticError, TypeError, ValueError):
        return True
    for key in ("entry_orders", "exit_orders"):
        for order in record.get(key, []):
            if not isinstance(order, dict):
                return True
            try:
                if Decimal(str(order.get("remaining_count") or "0")) != 0:
                    return True
            except (ArithmeticError, TypeError, ValueError):
                return True
            if order.get("order_id") and str(order.get("status") or "").lower() in {
                "resting", "pending", "open", "unknown", "submit_failed",
            }:
                return True
    return False


def exact_entry_notional(quantity: Decimal | str, price: Decimal | str) -> Decimal:
    """Return the exact cash notional for two-decimal shares at a 1c tick.

    Both operands are fixed point, so their product is exact to four decimal
    places.  Rejecting any unexpected higher-precision value keeps opening
    intent deterministic instead of silently rounding accounting data.
    """

    value = Decimal(str(quantity)) * Decimal(str(price))
    fixed = value.quantize(ENTRY_NOTIONAL_QUANTUM)
    if fixed != value:
        raise ValueError("entry quantity and price must produce an exact four-decimal notional")
    return fixed


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
    """Validate the immutable v14 opposite-side ladder contract."""

    entry = decimal(value["entry_price"])
    offset = decimal(value["maker_price_offset"])
    stop = decimal(value["stop_price"])
    stop_baseline = decimal(value["stop_baseline_entry_price"])
    if not Decimal("0") < entry < Decimal("1"):
        raise ValueError("reference entry_price must be within the contract range")
    if value.get("entry_execution_mode") != "opposite_side_doubling_ladder":
        raise ValueError("the active strategy requires entry_execution_mode=opposite_side_doubling_ladder")
    if value.get("maker_order_time_in_force") != "good_till_canceled":
        raise ValueError("the active strategy requires maker_order_time_in_force=good_till_canceled")
    if value.get("entry_order_lifetime") != "until_filled_or_market_close":
        raise ValueError("the active strategy requires entry_order_lifetime=until_filled_or_market_close")
    if int(value.get("entry_timeout_seconds", -1)) != 0:
        raise ValueError("entry_timeout_seconds must be 0; strategy-time expiration is disabled")
    if int(value.get("signal_delay_seconds", -1)) != 0:
        raise ValueError("signal_delay_seconds must be 0; entries are prepared for the opening boundary")
    if int(value.get("max_recovery_exponent", -1)) != 0:
        raise ValueError(
            "max_recovery_exponent must be 0; the active strategy disables the exponent circuit breaker"
        )
    if stop_baseline != Decimal("0.51") or stop != Decimal("0.51"):
        raise ValueError("the opposite ladder requires a fixed two-sided 51c flatten threshold")
    profile = str(value.get("shadow_profile") or CANONICAL_LIVE_SHADOW_PROFILE)
    expected_profile_stop = SHADOW_STOP_PROFILE_PRICES.get(profile)
    if expected_profile_stop is None:
        raise ValueError(
            "shadow_profile must be one of " + ", ".join(sorted(SHADOW_STOP_PROFILE_PRICES))
        )
    if profile != CANONICAL_LIVE_SHADOW_PROFILE and stop != expected_profile_stop:
        raise ValueError(
            f"shadow_profile={profile} requires stop_price={format(expected_profile_stop, 'f')}"
        )
    trading_mode = str(value.get("trading_mode") or "shadow")
    if profile != CANONICAL_LIVE_SHADOW_PROFILE and trading_mode != "shadow":
        raise ValueError("comparison stop profiles are shadow-only and cannot be loaded in live mode")
    if profile != CANONICAL_LIVE_SHADOW_PROFILE:
        raise ValueError("v14 accepts only the opposite-side ladder profile")
    if value.get("stop_policy") != "opposite_side_take_profit_ioc":
        raise ValueError("the active strategy requires stop_policy=opposite_side_take_profit_ioc")
    if int(value["entry_limit_offset_cents"]) != 1:
        raise ValueError("the active opposite ladder requires entry_limit_offset_cents=1")
    level_min = int(value["shadow_entry_level_min_cents"])
    level_max = int(value["shadow_entry_level_max_cents"])
    level_step = int(value["shadow_entry_level_step_cents"])
    if (level_min, level_max, level_step) != (40, 49, 1):
        raise ValueError("the active analytics contract requires every 1c level from 40c through 49c")
    trigger = int(value["hybrid_stop_trigger_cents"])
    maker_exit = int(value["hybrid_maker_exit_cents"])
    hard_stop = int(value["hybrid_hard_stop_cents"])
    if not 1 <= trigger <= 99:
        raise ValueError("direct protective-exit price must be a valid integer-cent tick")
    if (trigger, maker_exit, hard_stop) != (51, 51, 51):
        raise ValueError("v14 revision 2 flatten compatibility fields must all equal 51c")
    if int(value.get("opposite_take_profit_cents", 0)) != 51:
        raise ValueError("v14 revision 2 requires opposite_take_profit_cents=51")
    if not _bool(value.get("hybrid_stop_enabled", True)):
        raise ValueError("the active v14 strategy requires take-profit monitoring")
    if value.get("shadow_fill_model") != "conservative_public_trade_through":
        raise ValueError("v14 shadow mode requires conservative_public_trade_through")
    if offset < Decimal("0"):
        raise ValueError("maker_price_offset cannot be negative")
    if int(value["opening_quote_max_observations"]) < 1:
        raise ValueError("opening_quote_max_observations must be at least one")
    if not _bool(value.get("delayed_entry_tracking_enabled", True)):
        raise ValueError("the active analytics contract requires delayed_entry_tracking_enabled=true")
    if int(value.get("delayed_entry_threshold_cents", 0)) != 53:
        raise ValueError("the active analytics contract requires delayed_entry_threshold_cents=53")
    if int(value.get("delayed_entry_start_seconds", -1)) != 60:
        raise ValueError("the active delayed entry contract requires delayed_entry_start_seconds=60")
    if int(value.get("delayed_entry_max_trigger_cents", 0)) != 57:
        raise ValueError("the active opposite ladder requires a 57c inclusive sticky-side trigger ceiling")
    if int(value.get("opposite_initial_limit_min_cents", 0)) != 43:
        raise ValueError("the active opposite ladder requires a 43c initial-limit floor")
    if int(value.get("opposite_initial_limit_max_cents", 0)) != 47:
        raise ValueError("the active opposite ladder requires a 47c initial-limit ceiling")
    if int(value.get("delayed_entry_max_limit_cents", 0)) != 57:
        raise ValueError("the compatibility delayed-entry limit ceiling must remain 57c")
    if not _bool(value.get("opposite_ladder_enabled", False)):
        raise ValueError("opposite_ladder_enabled must be true")
    if _bool(value.get("recovery_enabled", True)):
        raise ValueError("v14 removes recovery sizing")
    if _bool(value.get("position_cap_enabled", False)):
        raise ValueError("v14 removes the position cap")
    if _bool(value.get("base_scaling_enabled", True)):
        raise ValueError("v14 uses only the fixed operator base input")


def validate_sizing_config(value: dict[str, Any]) -> None:
    """Validate the sole v14 sizing input: a fixed two-decimal base."""

    starting_base = decimal(value["starting_base"])
    if starting_base != round_shares(starting_base):
        raise ValueError("starting_base must have at most two decimal places")
    if starting_base <= Decimal("0"):
        raise ValueError("starting_base must be positive")
    if decimal(value["recovery_multiplier"]) != Decimal("1"):
        raise ValueError("v14 recovery_multiplier compatibility field must equal 1")


def load_config(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("live strategy configuration must be a JSON object")
    required = {
        "strategy_version", "series", "entry_price", "stop_price", "starting_base", "recovery_multiplier",
        "first_base_threshold", "threshold_growth_multiplier", "base_increment",
        "stop_policy", "stop_baseline_entry_price", "entry_execution_mode", "entry_limit_offset_cents",
        "maker_order_time_in_force", "entry_order_lifetime", "entry_timeout_seconds",
        "opening_quote_capture_seconds", "delayed_entry_start_seconds", "delayed_entry_max_limit_cents",
        "opposite_initial_limit_min_cents", "opposite_initial_limit_max_cents",
        "delayed_entry_max_trigger_cents", "opposite_take_profit_cents",
        "shadow_fill_model", "shadow_entry_level_min_cents", "shadow_entry_level_max_cents",
        "shadow_entry_level_step_cents", "hybrid_stop_enabled", "hybrid_stop_trigger_cents",
        "hybrid_maker_exit_cents", "hybrid_hard_stop_cents", "trading_mode", "config_schema_version",
    }
    missing = required - value.keys()
    if missing:
        raise ValueError(f"live strategy configuration is missing: {', '.join(sorted(missing))}")
    assert_active_strategy_contract(value)
    # v14 sizes the immutable 1/2/4/8/16 order ladder directly from
    # ``starting_base``.  Strip retired cap keys restored from an early v14
    # checkpoint so they cannot remain part of the active config/hash.
    value.pop("max_position", None)
    value.pop("max_position_per_base_share", None)
    value.pop("position_cap_enabled", None)
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
    value.setdefault("shadow_fill_model", "conservative_public_trade_through")
    value.setdefault("starting_shadow_balance", "1000.00")
    value.setdefault("maker_price_offset", "0.01")
    value.setdefault("stop_policy", "opposite_side_take_profit_ioc")
    value.setdefault("entry_execution_mode", "opposite_side_doubling_ladder")
    value.setdefault("maker_order_time_in_force", "good_till_canceled")
    value.setdefault("stop_baseline_entry_price", "0.51")
    value.setdefault("signal_delay_seconds", 0)
    value.setdefault("signal_mode", "sticky_until_directional_win")
    value.setdefault("shadow_profile", CANONICAL_LIVE_SHADOW_PROFILE)
    # A fresh post-open complete book is required to freeze the one-cent-below
    # maker limit. Missing or stale evidence fails closed.
    value.setdefault("entry_timeout_seconds", 0)
    value.setdefault("entry_order_lifetime", "until_filled_or_market_close")
    value.setdefault("opening_quote_capture_seconds", 60)
    # Detailed opening samples remain bounded to 60 seconds.  A separate
    # compact tracker observes the first fresh executable ask >=53c through
    # market close without retaining every full-book update.
    value.setdefault("delayed_entry_tracking_enabled", True)
    value.setdefault("delayed_entry_threshold_cents", 53)
    value.setdefault("delayed_entry_start_seconds", 60)
    value.setdefault("delayed_entry_max_limit_cents", 57)
    value.setdefault("delayed_entry_max_trigger_cents", 57)
    value.setdefault("opposite_initial_limit_min_cents", 43)
    value.setdefault("opposite_initial_limit_max_cents", 47)
    value.setdefault("opposite_take_profit_cents", 51)
    value.setdefault("opposite_ladder_enabled", True)
    value.setdefault("recovery_enabled", False)
    value.setdefault("base_scaling_enabled", False)
    value.setdefault("entry_lateness_seconds", 60)
    value.setdefault("stop_poll_interval", 1.0)
    value.setdefault("reconciliation_interval", 5.0)
    value.setdefault("market_discovery_interval_seconds", 1.0)
    value.setdefault("outcome_observation_seconds", 5)
    value.setdefault("provisional_outcome_threshold", "0.99")
    value.setdefault("max_outcome_quote_age_seconds", 2.0)
    value.setdefault("max_stale_quote_seconds", 2.0)
    # Zero disables only the exponent circuit breaker. Recovery sizing still
    # obeys the shared 100-share position cap and the configured loss/funding
    # circuit breakers in both shadow and live modes.
    value.setdefault("max_recovery_exponent", 0)
    value.setdefault("max_recovery_cycle_loss", "50.00")
    value.setdefault("max_daily_realized_loss", "25.00")
    value.setdefault("max_api_failures", 5)
    value.setdefault("handoff_guard_seconds", 60)
    value.setdefault("opening_quote_max_observations", 500)
    value.setdefault("opening_price_discovery_seconds", 3)
    value.setdefault("entry_limit_offset_cents", 1)
    value.setdefault("shadow_entry_level_min_cents", 40)
    value.setdefault("shadow_entry_level_max_cents", 49)
    value.setdefault("shadow_entry_level_step_cents", 1)
    value.setdefault("hybrid_stop_enabled", True)
    value.setdefault("hybrid_stop_trigger_cents", 51)
    value.setdefault("hybrid_maker_exit_cents", 51)
    value.setdefault("hybrid_hard_stop_cents", 51)
    value.setdefault("trading_mode", "shadow")
    # State and audit writes are fsynced locally for every material event.
    # This only bounds GitHub checkpoint publication, avoiding a Git push for
    # every market-data update while preserving a short handoff-loss window.
    value.setdefault("durable_checkpoint_interval_seconds", 30.0)
    if value["trading_mode"] not in {"shadow", "live"}:
        raise ValueError("trading_mode must be shadow or live")
    if value["signal_mode"] != "sticky_until_directional_win":
        raise ValueError("the active shadow strategy requires signal_mode=sticky_until_directional_win")
    validate_entry_price_contract(value)
    validate_sizing_config(value)
    if not 1 <= int(value["opening_quote_capture_seconds"]) <= 300:
        raise ValueError("opening_quote_capture_seconds must be between 1 and 300")
    if int(value["handoff_guard_seconds"]) < 60:
        raise ValueError("handoff_guard_seconds must keep at least one minute clear at each market boundary")
    if not 1.0 <= float(value["durable_checkpoint_interval_seconds"]) <= 60.0:
        raise ValueError("durable_checkpoint_interval_seconds must be between 1 and 60")
    if not 0.25 <= float(value["market_discovery_interval_seconds"]) <= 10.0:
        raise ValueError("market_discovery_interval_seconds must be between 0.25 and 10")
    return value


def save_config(path: Path, config: dict[str, Any]) -> None:
    from live_state import save_json_atomic
    save_json_atomic(path, config)


def enforce_active_runtime_config(path: Path) -> dict[str, Any]:
    """Upgrade only reviewed v14 opposite-ladder contracts at handoff.

    A runtime checkpoint can restore revision 2's 53c..58c band over the
    repository default.  Accept only that exact predecessor (or revision 3
    itself), then atomically install the terminal 53c..57c decision and its
    exact complementary 43c..47c initial-entry band.  Unrelated configuration
    remains a hard error.
    """

    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("runtime strategy configuration must be an object")
    expected = {
        "strategy_version": ACTIVE_STRATEGY_VERSION,
        "config_schema_version": ACTIVE_CONFIG_SCHEMA_VERSION,
        "entry_execution_mode": "opposite_side_doubling_ladder",
        "delayed_entry_threshold_cents": 53,
        "delayed_entry_start_seconds": 60,
        "entry_limit_offset_cents": 1,
        "maker_order_time_in_force": "good_till_canceled",
        "entry_order_lifetime": "until_filled_or_market_close",
    }
    mismatches = {
        key: (raw.get(key), value)
        for key, value in expected.items()
        if raw.get(key) != value
    }
    if mismatches:
        raise ValueError(f"refusing to rewrite noncanonical runtime config: {mismatches}")
    prior_contract = (
        str(raw.get("stop_price")),
        str(raw.get("stop_baseline_entry_price")),
        int(raw.get("hybrid_stop_trigger_cents", 0)),
        int(raw.get("hybrid_maker_exit_cents", 0)),
        int(raw.get("hybrid_hard_stop_cents", 0)),
        int(raw.get("opposite_take_profit_cents", 0)),
        str(raw.get("shadow_profile")),
        int(raw.get("delayed_entry_max_trigger_cents", 0)),
        raw.get("opposite_initial_limit_min_cents"),
        raw.get("opposite_initial_limit_max_cents"),
        int(raw.get("delayed_entry_max_limit_cents", 0)),
    )
    allowed_contracts = {
        (
            "0.51", "0.51", 51, 51, 51, 51,
            "opposite_ladder_53_58_flatten_51", 58, None, None, 57,
        ),
        (
            "0.51", "0.51", 51, 51, 51, 51,
            CANONICAL_LIVE_SHADOW_PROFILE, 57, 43, 47, 57,
        ),
    }
    if prior_contract not in allowed_contracts:
        raise ValueError(f"refusing unrecognized runtime ladder contract: {prior_contract}")
    raw.update({
        "stop_price": "0.51",
        "stop_baseline_entry_price": "0.51",
        "hybrid_stop_trigger_cents": 51,
        "hybrid_maker_exit_cents": 51,
        "hybrid_hard_stop_cents": 51,
        "opposite_take_profit_cents": 51,
        "delayed_entry_max_trigger_cents": 57,
        "delayed_entry_max_limit_cents": 57,
        "opposite_initial_limit_min_cents": 43,
        "opposite_initial_limit_max_cents": 47,
        "shadow_profile": CANONICAL_LIVE_SHADOW_PROFILE,
        "selection_basis": (
            "first_fresh_post60_sticky_ask_53_57_terminal_gate_then_trade_opposite_"
            "at_exact_complement_47_43_and_40_30_20_10_doubling_gtc_flatten_"
            "when_either_side_touches_51"
        ),
    })
    validated = load_config_from_value(raw)
    save_config(path, validated)
    return validated


def apply_overrides(config: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    updated = dict(config)
    for name in DECIMAL_CONFIG_FIELDS | INTEGER_CONFIG_FIELDS | FLOAT_CONFIG_FIELDS:
        value = getattr(args, name, None)
        if value not in (None, ""):
            updated[name] = str(value) if name in DECIMAL_CONFIG_FIELDS else value
    for name in OPTIONAL_DECIMAL_CONFIG_FIELDS:
        value = getattr(args, name, None)
        if value not in (None, ""):
            amount = decimal(value)
            updated[name] = None if amount == 0 else format(amount, "f")
    for name in ("allow_capital_downsize", "hybrid_stop_enabled"):
        value = getattr(args, name, None)
        if value is not None:
            updated[name] = value
    shadow_profile = getattr(args, "shadow_profile", None)
    if shadow_profile not in (None, ""):
        updated["shadow_profile"] = str(shadow_profile)
    trading_mode = getattr(args, "trading_mode", None)
    if trading_mode not in (None, ""):
        updated["trading_mode"] = str(trading_mode)
    return load_config_from_value(updated)


def load_config_from_value(value: dict[str, Any]) -> dict[str, Any]:
    temporary = dict(value)
    # Keep validation identical for a persisted config and command overrides.
    for name in DECIMAL_CONFIG_FIELDS & temporary.keys():
        temporary[name] = _decimal_string(temporary[name], name)
    for name in OPTIONAL_DECIMAL_CONFIG_FIELDS:
        raw = temporary.get(name, "100.00")
        if raw in (None, "", "0", "0.00"):
            temporary[name] = None
        else:
            temporary[name] = _decimal_string(raw, name)
    for name in INTEGER_CONFIG_FIELDS & temporary.keys():
        temporary[name] = int(temporary[name])
    for name in FLOAT_CONFIG_FIELDS & temporary.keys():
        temporary[name] = float(temporary[name])
    required = {
        "strategy_version", "series", "entry_price", "stop_price", "starting_base", "recovery_multiplier",
        "first_base_threshold", "threshold_growth_multiplier", "base_increment", "stop_policy",
        "stop_baseline_entry_price", "entry_execution_mode", "entry_limit_offset_cents", "shadow_fill_model",
        "maker_order_time_in_force", "entry_order_lifetime", "entry_timeout_seconds",
        "opening_quote_capture_seconds", "delayed_entry_start_seconds", "delayed_entry_max_limit_cents",
        "opposite_initial_limit_min_cents", "opposite_initial_limit_max_cents",
        "delayed_entry_max_trigger_cents", "opposite_take_profit_cents",
        "shadow_entry_level_min_cents", "shadow_entry_level_max_cents", "shadow_entry_level_step_cents",
        "hybrid_stop_enabled", "hybrid_stop_trigger_cents", "hybrid_maker_exit_cents",
        "hybrid_hard_stop_cents", "trading_mode", "config_schema_version",
    }
    if required - temporary.keys():
        raise ValueError("invalid overridden live strategy configuration")
    assert_active_strategy_contract(temporary)
    temporary.pop("max_position", None)
    temporary.pop("max_position_per_base_share", None)
    temporary.pop("position_cap_enabled", None)
    # Reuse the normal rules without doing a file round-trip.
    if temporary["series"] != "KXBTC15M":
        raise ValueError("invalid strategy series")
    temporary.setdefault("shadow_fill_model", "conservative_public_trade_through")
    temporary.setdefault("starting_shadow_balance", "1000.00")
    temporary.setdefault("maker_price_offset", "0.01")
    temporary.setdefault("stop_policy", "opposite_side_take_profit_ioc")
    temporary.setdefault("entry_execution_mode", "opposite_side_doubling_ladder")
    temporary.setdefault("maker_order_time_in_force", "good_till_canceled")
    temporary.setdefault("stop_baseline_entry_price", "0.51")
    temporary.setdefault("signal_mode", "sticky_until_directional_win")
    temporary.setdefault("shadow_profile", CANONICAL_LIVE_SHADOW_PROFILE)
    temporary.setdefault("entry_timeout_seconds", 0)
    temporary.setdefault("entry_order_lifetime", "until_filled_or_market_close")
    temporary.setdefault("opening_quote_capture_seconds", 60)
    temporary.setdefault("delayed_entry_tracking_enabled", True)
    temporary.setdefault("delayed_entry_threshold_cents", 53)
    temporary.setdefault("delayed_entry_start_seconds", 60)
    temporary.setdefault("delayed_entry_max_limit_cents", 57)
    temporary.setdefault("delayed_entry_max_trigger_cents", 57)
    temporary.setdefault("opposite_initial_limit_min_cents", 43)
    temporary.setdefault("opposite_initial_limit_max_cents", 47)
    temporary.setdefault("opposite_take_profit_cents", 51)
    temporary.setdefault("opposite_ladder_enabled", True)
    temporary.setdefault("recovery_enabled", False)
    temporary.setdefault("base_scaling_enabled", False)
    temporary.setdefault("entry_lateness_seconds", 60)
    temporary.setdefault("handoff_guard_seconds", 60)
    temporary.setdefault("opening_quote_max_observations", 500)
    temporary.setdefault("opening_price_discovery_seconds", 3)
    temporary.setdefault("durable_checkpoint_interval_seconds", 30.0)
    temporary.setdefault("market_discovery_interval_seconds", 1.0)
    temporary.setdefault("entry_limit_offset_cents", 1)
    temporary.setdefault("shadow_entry_level_min_cents", 40)
    temporary.setdefault("shadow_entry_level_max_cents", 49)
    temporary.setdefault("shadow_entry_level_step_cents", 1)
    temporary.setdefault("hybrid_stop_enabled", True)
    temporary.setdefault("hybrid_stop_trigger_cents", 51)
    temporary.setdefault("hybrid_maker_exit_cents", 51)
    temporary.setdefault("hybrid_hard_stop_cents", 51)
    temporary.setdefault("trading_mode", "shadow")
    validate_entry_price_contract(temporary)
    validate_sizing_config(temporary)
    if temporary["trading_mode"] not in {"shadow", "live"}:
        raise ValueError("trading_mode must be shadow or live")
    if temporary["signal_mode"] != "sticky_until_directional_win":
        raise ValueError("the active shadow strategy requires signal_mode=sticky_until_directional_win")
    if not 1 <= int(temporary["opening_quote_capture_seconds"]) <= 300:
        raise ValueError("opening_quote_capture_seconds must be between 1 and 300")
    if int(temporary["handoff_guard_seconds"]) < 60:
        raise ValueError("handoff_guard_seconds must keep at least one minute clear at each market boundary")
    if not 1.0 <= float(temporary["durable_checkpoint_interval_seconds"]) <= 60.0:
        raise ValueError("durable_checkpoint_interval_seconds must be between 1 and 60")
    if not 0.25 <= float(temporary["market_discovery_interval_seconds"]) <= 10.0:
        raise ValueError("market_discovery_interval_seconds must be between 0.25 and 10")
    return temporary


def strategy_parameters(config: dict[str, Any]) -> StrategyParameters:
    # StrategyParameters is still shared with archived historical/recovery
    # replay code.  The live v14 ladder has no cap fields; this unreachable
    # internal bound only satisfies that legacy type and is never applied to
    # a v14 ladder order.
    return StrategyParameters(
        recovery_multiplier=Decimal(config["recovery_multiplier"]),
        first_base_threshold=Decimal(config["first_base_threshold"]),
        threshold_growth_multiplier=Decimal(config["threshold_growth_multiplier"]),
        base_increment=Decimal(config["base_increment"]),
        starting_base=Decimal(config["starting_base"]),
        max_position=Decimal(str(config.get("max_position", "999999999.99"))),
        max_position_per_base_share=(
            None
            if config.get("max_position_per_base_share") in (None, "", "0", "0.00")
            else Decimal(str(config["max_position_per_base_share"]))
        ),
    )


def deterministic_client_order_id(ticker: str, side: str, purpose: str, config: dict[str, Any]) -> str:
    key = f"{config['strategy_version']}:{config_hash(config)}:{ticker}:{side}:{purpose}"
    return ORDER_PREFIX + uuid.uuid5(ORDER_NAMESPACE, key).hex


def epoch(value: Any) -> float | None:
    """Parse exchange timestamps without mistaking milliseconds for seconds.

    Kalshi ticker messages normally expose ``ts_ms`` as a 13-digit integer.
    The legacy helper accepts numeric values as seconds, so normalize numeric
    inputs before asking it to parse ISO-8601 strings.  Otherwise every final
    quote appears thousands of years in the future and provisional outcome
    inference always fails closed.
    """

    if value in (None, ""):
        return None
    if isinstance(value, (int, float, Decimal)):
        numeric = float(value)
        return numeric / 1000.0 if abs(numeric) > 10_000_000_000 else numeric
    if isinstance(value, str):
        try:
            numeric = float(value)
        except ValueError:
            pass
        else:
            return numeric / 1000.0 if abs(numeric) > 10_000_000_000 else numeric
    converted = timestamp_epoch(value)
    return None if converted is None else float(converted)


def executable_quote_epoch(quote: dict[str, Any]) -> float | None:
    """Return the best auditable exchange/receive time for one book quote."""

    return (
        epoch(quote.get("source_timestamp_ms"))
        or epoch(quote.get("source_server_timestamp"))
        or _iso_epoch(quote.get("received_at"))
    )


def btc_target_metadata(market: Any) -> dict[str, Any]:
    """Extract the exchange-provided KXBTC15M BTC comparison target exactly.

    Numeric strike fields are authoritative.  The YES/NO subtitle is only a
    fallback for API representations that omit those structured fields; the
    ticker is never parsed or used to invent a target.
    """

    yes_label = str(field(market, "yes_sub_title") or "") or None
    no_label = str(field(market, "no_sub_title") or "") or None
    target: Decimal | None = None
    source: str | None = None
    for name in ("floor_strike", "cap_strike", "functional_strike"):
        raw = field(market, name)
        if raw in (None, ""):
            continue
        try:
            candidate = Decimal(str(raw).replace(",", "").replace("$", "").strip())
        except (ArithmeticError, ValueError):
            continue
        if candidate.is_finite() and candidate > 0:
            target, source = candidate, name
            break
    if target is None:
        for name, label in (("yes_sub_title", yes_label), ("no_sub_title", no_label)):
            match = re.search(r"\$\s*([0-9][0-9,]*(?:\.[0-9]+)?)", label or "")
            if match is None:
                continue
            candidate = Decimal(match.group(1).replace(",", ""))
            if candidate.is_finite() and candidate > 0:
                target, source = candidate, name
                break
    comparison = str(field(market, "strike_type") or "").strip() or None
    return {
        "btc_target_capture_version": BTC_TARGET_CAPTURE_CONTRACT_VERSION,
        "btc_target_status": "AVAILABLE" if target is not None else "UNAVAILABLE",
        "btc_target_price": format(target, "f") if target is not None else None,
        "btc_target_price_display": f"${target:,.2f}" if target is not None else None,
        "btc_target_source": source,
        "btc_target_comparison": comparison,
        "btc_target_yes_sub_title": yes_label,
        "btc_target_no_sub_title": no_label,
        "market_title": str(field(market, "title") or "") or None,
    }


def market_metadata(market: Any) -> dict[str, Any] | None:
    ticker = str(field(market, "ticker") or "")
    opened = epoch(field(market, "open_time", "open_ts", "open_ts_ms"))
    closed = epoch(field(market, "close_time", "expected_expiration_time", "close_ts", "close_ts_ms"))
    if not ticker or opened is None or closed is None or closed <= opened:
        return None
    return {
        "ticker": ticker, "open_epoch": opened, "close_epoch": closed,
        "status": str(field(market, "status") or "").lower(), "raw": market,
        **btc_target_metadata(market),
    }


@dataclass(frozen=True)
class QuoteObservation:
    ticker: str
    received_epoch: float
    exchange_epoch: float | None
    yes_bid: Decimal
    no_bid: Decimal
    source: str = "kalshi_websocket_ticker"
    yes_bid_epoch: float | None = None
    no_bid_epoch: float | None = None


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
        # Direction inference only requires executable bids.  Prefer the
        # ticker-level reconstructed quote, whose bid/ask component times are
        # tracked independently, rather than requiring one message to carry
        # both prices and both sizes.  Entry execution remains stricter and
        # still requires ``complete_book`` with displayed depth.
        book = quote.get("ticker_book") if isinstance(quote, dict) else None
        if not isinstance(book, dict) and isinstance(quote, dict):
            book = quote.get("complete_book")
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
        received_epoch = float(book.get("received_epoch") or time.time())
        common_exchange_epoch = epoch(
            book.get("source_timestamp_ms") or book.get("source_server_timestamp")
        )
        yes_bid_epoch = epoch(book.get("yes_bid_source_timestamp"))
        no_bid_epoch = epoch(book.get("yes_ask_source_timestamp"))
        try:
            yes_bid_epoch = yes_bid_epoch or float(book.get("yes_bid_received_epoch") or received_epoch)
            no_bid_epoch = no_bid_epoch or float(book.get("yes_ask_received_epoch") or received_epoch)
        except (TypeError, ValueError):
            return
        observation = QuoteObservation(
            ticker=ticker,
            received_epoch=received_epoch,
            exchange_epoch=common_exchange_epoch,
            yes_bid=yes_bid_d,
            no_bid=no_bid_d,
            yes_bid_epoch=yes_bid_epoch,
            no_bid_epoch=no_bid_epoch,
        )
        records = self.observations.setdefault(ticker, [])
        records.append(observation)
        cutoff = observation.received_epoch - max(30, self.observation_seconds * 3)
        self.observations[ticker] = [item for item in records if item.received_epoch >= cutoff]

    def infer(self, ticker: str, boundary_epoch: float) -> dict[str, Any] | None:
        window_start = boundary_epoch - self.observation_seconds
        candidates = [
            item for item in self.observations.get(ticker, [])
            # Only the configured final 5/15-second window may determine the
            # next direction. Do not use an earlier intramarket quote or let a
            # post-boundary update rewrite the decision-time fact.
            if window_start <= item.received_epoch <= boundary_epoch
        ]
        if not candidates:
            return None
        latest = max(candidates, key=lambda item: item.received_epoch)

        def side_epoch(item: QuoteObservation, side: str) -> float:
            component_epoch = item.yes_bid_epoch if side == "yes" else item.no_bid_epoch
            return component_epoch or item.exchange_epoch or item.received_epoch

        def is_valid_window_quote(item: QuoteObservation, side: str) -> bool:
            observed_epoch = side_epoch(item, side)
            # The final-window bounds define signal recency. The independent
            # staleness guard checks transport/source lag at observation time,
            # rather than shrinking a configured 5/15-second window to two
            # seconds at the boundary.
            transport_age = item.received_epoch - observed_epoch
            return (
                window_start <= observed_epoch <= boundary_epoch
                and -1.0 <= transport_age <= self.max_quote_age_seconds
            )

        yes_qualifying = [
            item for item in candidates
            if item.yes_bid >= self.threshold and is_valid_window_quote(item, "yes")
        ]
        no_qualifying = [
            item for item in candidates
            if item.no_bid >= self.threshold and is_valid_window_quote(item, "no")
        ]
        if bool(yes_qualifying) == bool(no_qualifying):
            # Both sides qualifying anywhere in the final window is a data
            # conflict; neither side qualifying is an unavailable signal.
            return None
        side = "yes" if yes_qualifying else "no"
        selected = yes_qualifying if side == "yes" else no_qualifying
        qualifying = max(selected, key=lambda item: side_epoch(item, side))
        qualifying_epoch = side_epoch(qualifying, side)
        quote_age = boundary_epoch - qualifying_epoch
        return {
            "outcome": side,
            "ticker": ticker,
            "timestamp": datetime.fromtimestamp(qualifying.received_epoch, timezone.utc).isoformat(),
            "exchange_timestamp": qualifying_epoch,
            "quote_age_seconds": round(max(0.0, quote_age), 6),
            "method": "final_window_executable_bid_threshold",
            "threshold": format(self.threshold, "f"),
            "final_yes_bid": format(latest.yes_bid, "f"),
            "final_no_bid": format(latest.no_bid, "f"),
            "qualifying_bid": format(qualifying.yes_bid if side == "yes" else qualifying.no_bid, "f"),
            "observation_window_seconds": self.observation_seconds,
            "max_yes_bid": format(max(item.yes_bid for item in candidates), "f"),
            "max_no_bid": format(max(item.no_bid for item in candidates), "f"),
            "qualifying_observations": len(selected),
        }


class LiveEngine:
    def __init__(self, config: dict[str, Any], state: dict[str, Any], state_path: Path, ledger_path: Path, dry_run: bool, config_path: Path | None = None, *, reconcile_only: bool = False) -> None:
        self.config = config
        self.state = state
        self.state_path = state_path
        self.ledger_path = ledger_path
        self.config_path = config_path
        self.dry_run = dry_run
        self.execution_mode = "RECONCILE_ONLY" if reconcile_only else "DRY_RUN" if dry_run else "LIVE"
        namespace = "live" if reconcile_only or not dry_run else "shadow"
        prior_namespace = state.get("execution_context", {}).get("state_namespace")
        if prior_namespace is not None and prior_namespace != namespace:
            raise RuntimeError("refusing to mix live and shadow checkpoint namespaces")
        self.state["execution_context"] = {
            "mode": self.execution_mode, "state_namespace": namespace,
            "real_order_submission_enabled": not dry_run and not reconcile_only,
            "workflow_run_id": os.getenv("GITHUB_RUN_ID"),
            "source_commit": os.getenv("KALSHI_SOURCE_SHA") or os.getenv("GITHUB_SHA"),
            "stop_safety_contract": LIVE_STOP_SAFETY_CONTRACT_VERSION,
            "observed_at": utc_now(),
        }
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
        compacted_keys = sum(
            prune_finalized_public_trade_keys(record)
            for record in self.state.get("markets", {}).values()
            if isinstance(record, dict)
        )
        if compacted_keys:
            self.state["storage_compaction"] = {
                "version": 1,
                "at": utc_now(),
                "removed_finalized_public_trade_keys": compacted_keys,
                "preserved_strategy_and_analytics_facts": True,
            }
            LOG.warning(
                "DURABLE STATE COMPACTED | removed_finalized_public_trade_keys=%s "
                "strategy_facts_preserved=true",
                compacted_keys,
            )
        self.tracker = ProvisionalOutcomeTracker(
            Decimal(config["provisional_outcome_threshold"]), int(config["outcome_observation_seconds"]),
            float(config["max_outcome_quote_age_seconds"]),
        )
        self.last_reconcile = 0.0
        self.last_heartbeat = 0.0
        self.last_stop_poll = 0.0
        self.last_analytics_log = 0.0
        self.last_market_discovery = 0.0
        self.markets: list[dict[str, Any]] = []
        checkpoint_paths = [state_path, ledger_path]
        if config_path is not None:
            checkpoint_paths.insert(0, config_path)
        startup_order_check_journal = Path(os.getenv(
            "KALSHI_STARTUP_ORDER_CHECK_JOURNAL",
            "data/.kalshi_live_opposite_ladder_v14_startup_order_check/order-smoke-test.json",
        ))
        if not dry_run and startup_order_check_journal.exists():
            # Once the pre-worker create/cancel proof succeeds, retain it in
            # every later live snapshot instead of dropping it on the first
            # ordinary strategy checkpoint.
            checkpoint_paths.append(startup_order_check_journal)
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
            "config_hash": config_hash(self.config),
            **{key: audit_safe_value(value) for key, value in details.items()},
            "execution_mode": self.execution_mode,
            "execution_context": self.state["execution_context"],
        })
        # An append is fsynced by ``append_audit``.  Immediately pair it with
        # the atomic state file so a crash cannot leave a fresh ledger event
        # behind only an old local strategy snapshot.  The publisher itself
        # coalesces GitHub commits at the configured interval; local safety
        # never waits on that network operation.
        self.checkpoint(f"audit:{event}")

    def transition(self, record: dict[str, Any], status: str, reason: str | None = None) -> None:
        prior = record.get("status")
        if prior == status and (reason is None or record.get("status_reason") == reason):
            # Reconciliation is polled continuously.  Repeating an identical
            # state is not a transition and previously produced thousands of
            # duplicate ledger rows while an order cancellation was pending.
            return
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

    def clear_resolved_entry_interlock(self, record: dict[str, Any], resolution: str) -> bool:
        """Resume entries after exchange evidence resolves an ambiguous POST.

        The worker itself never stops for an ambiguous entry response.  It
        keeps managing quotes and risk, and this method releases only the
        narrow entry-submission interlock after every ambiguous record has an
        authoritative order/position/fill resolution.  Risk, accounting,
        authentication and loss-limit breakers are intentionally unrelated.
        """

        breaker = self.state.get("circuit_breaker", {})
        original_reason = str(breaker.get("reason") or "")
        if not breaker.get("blocked") or original_reason not in {
            "maker_entry_submission_unknown", "maker_entry_submission_rejected",
            "opposite_ladder_submission_unknown",
            "entry_cancellation_unconfirmed",
            "hard_stop_unflattened_at_market_close",
        }:
            return False
        unresolved = [
            str(item.get("ticker") or "")
            for item in self.state.get("markets", {}).values()
            if isinstance(item, dict)
            and item is not record
            and item.get("status") in {"ERROR_RECONCILIATION", "RECONCILIATION_PENDING"}
        ]
        if unresolved:
            return False
        resolved_at = utc_now()
        breaker.update({
            "blocked": False, "reason": None, "triggered_at": None,
            "last_resolution": {
                "at": resolved_at, "kind": "continuous_entry_reconciliation",
                "original_reason": original_reason, "resolution": resolution,
                "ticker": record.get("ticker"),
            },
        })
        self.audit(
            "entry_submission_interlock_resolved", ticker=record.get("ticker"),
            original_reason=original_reason, resolution=resolution,
        )
        LOG.warning(
            "ENTRY INTERLOCK RESOLVED | ticker=%s original_reason=%s resolution=%s "
            "worker_continued=true new_entries_enabled=true",
            record.get("ticker"), original_reason, resolution,
        )
        return True

    def current_parameters(self) -> StrategyParameters:
        cycle = self.state.get("cycle_strategy_parameters")
        if isinstance(cycle, dict) and Decimal(str(self.state.get("sizing", {}).get("recovery_cycle_pnl", "0"))) < 0:
            return StrategyParameters(**{
                key: (None if value is None else Decimal(str(value)))
                for key, value in cycle.items()
            })
        return self.parameters

    def record_parameters(self, record: dict[str, Any]) -> StrategyParameters:
        snapshot = record.get("config_snapshot")
        if isinstance(snapshot, dict):
            try:
                return StrategyParameters(**{
                    key: (None if value is None else Decimal(str(value)))
                    for key, value in snapshot.items()
                })
            except (ArithmeticError, TypeError, ValueError):
                self.trip("invalid_persisted_record_configuration")
        return self.current_parameters()

    def circuit_allows_entry(self) -> bool:
        if self.state["circuit_breaker"].get("blocked"):
            return False
        sizing = sizing_state(self.current_parameters(), self.state.get("sizing"))
        maximum_exponent = int(self.config["max_recovery_exponent"])
        if maximum_exponent > 0 and sizing.recovery_exponent >= maximum_exponent:
            self.trip("max_recovery_exponent")
        if -sizing.recovery_cycle_pnl >= Decimal(self.config["max_recovery_cycle_loss"]):
            self.trip("max_recovery_cycle_loss")
        today = datetime.now(timezone.utc).date().isoformat()
        realized = Decimal(str(self.state.get("daily_realized", {}).get(today, "0")))
        if -realized >= Decimal(self.config["max_daily_realized_loss"]):
            self.trip("max_daily_realized_loss")
        return not self.state["circuit_breaker"].get("blocked")

    def entry_submission_health(self) -> dict[str, Any]:
        """Submission evidence is separate from analytical touches and fills."""
        records = [
            record for record in self.state.get("markets", {}).values()
            if isinstance(record, dict)
        ]
        orders = [order for record in records for order in record.get("entry_orders", [])]
        ladder_retries = sum(
            max(0, len([
                order for order in record.get("entry_orders", [])
                if isinstance(order, dict) and order.get("ladder_role") == role
            ]) - 1)
            for record in records
            for role in {str(order.get("ladder_role")) for order in record.get("entry_orders", []) if order.get("ladder_role")}
        )
        health = {
            "mode": self.execution_mode,
            "recorded_entry_attempts": len(orders),
            "exchange_acknowledgments": sum(bool(order.get("order_id")) for order in orders),
            "definitive_rejections": sum(order.get("submission_outcome") == "rejected" for order in orders),
            "market_scoped_retries": ladder_retries + sum(
                max(0, int(record.get("entry_retry", {}).get("retry_submission_count", 0)))
                for record in self.state.get("markets", {}).values()
                if isinstance(record, dict) and isinstance(record.get("entry_retry"), dict)
            ),
            "retry_pending_markets": sum(
                bool(
                    isinstance(record.get("opposite_ladder"), dict)
                    and record["opposite_ladder"].get("retry_pending")
                )
                or (
                    record.get("status") == "SIGNAL_PENDING"
                    and isinstance(record.get("entry_retry"), dict)
                    and record["entry_retry"].get("status") == "RETRY_PENDING"
                )
                for record in records
            ),
            "abandoned_at_or_below_51c": sum(
                isinstance(record, dict)
                and record.get("status_reason") == "entry_abandoned_at_or_below_51c"
                for record in records
            ),
            "unresolved_submissions": sum(
                not order.get("order_id") and order.get("status") in {"submit_failed", "submitting"}
                and order.get("submission_outcome") != "rejected" for order in orders
            ),
            "breaker_blocked": bool(self.state["circuit_breaker"].get("blocked")),
            "breaker_reason": self.state["circuit_breaker"].get("reason"),
        }
        self.state["entry_submission_health"] = health
        return health

    def backfill_btc_target(
        self, record: dict[str, Any], target_details: dict[str, Any], *, reason: str,
    ) -> bool:
        """Upgrade target telemetry without altering any trading decision.

        An unavailable lookup can never erase an older valid strike.  The
        signal side, order state, sizing, and P&L fields are intentionally
        outside ``BTC_TARGET_RECORD_FIELDS`` and therefore immutable here.
        """

        if target_details.get("btc_target_status") != "AVAILABLE":
            return False
        if (
            record.get("btc_target_capture_version") == BTC_TARGET_CAPTURE_CONTRACT_VERSION
            and record.get("btc_target_status") == "AVAILABLE"
            and record.get("btc_target_price") == target_details.get("btc_target_price")
            and record.get("btc_target_comparison") == target_details.get("btc_target_comparison")
        ):
            return False
        record.update({key: target_details.get(key) for key in BTC_TARGET_RECORD_FIELDS})
        ladder = record.get("opening_cross_ladder")
        if isinstance(ladder, dict):
            ladder.update({key: target_details.get(key) for key in BTC_TARGET_RECORD_FIELDS})
            for lane in ladder.get("triggered", {}).values():
                if isinstance(lane, dict):
                    lane.update({key: target_details.get(key) for key in BTC_TARGET_RECORD_FIELDS})
        self.audit(
            "btc_target_backfilled", ticker=record.get("ticker"), reason=reason,
            btc_target_price=record.get("btc_target_price"),
            btc_target_price_display=record.get("btc_target_price_display"),
            btc_target_comparison=record.get("btc_target_comparison"),
            btc_target_source=record.get("btc_target_source"),
            btc_target_status=record.get("btc_target_status"),
            btc_target_capture_version=record.get("btc_target_capture_version"),
        )
        LOG.warning(
            "BTC TARGET BACKFILLED | ticker=%s target=%s comparison=%s source=%s "
            "status=%s contract=v%s reason=%s",
            record.get("ticker"), record.get("btc_target_price_display"),
            record.get("btc_target_comparison"), record.get("btc_target_source"),
            record.get("btc_target_status"), record.get("btc_target_capture_version"), reason,
        )
        return True

    async def discover(self, rest: KalshiREST) -> None:
        """Discover the previous, current, and upcoming exchange markets.

        A status-only ``open`` query cannot preload the successor, while an
        unbounded query may return a page composed entirely of far-future
        initialized markets.  Kalshi supports close-time filters without a
        status filter, so use a narrow window around exchange time.  That
        provides the real unopened successor ticker before the boundary and
        still avoids deriving tickers or scanning fragile future pages.
        """
        try:
            now = time.time()
            window_payload = await rest.get_raw_json(
                "/markets",
                {
                    "series_ticker": self.config["series"],
                    "min_close_ts": int(now) - MARKET_DISCOVERY_LOOKBACK_SECONDS,
                    "max_close_ts": int(now) + MARKET_DISCOVERY_LOOKAHEAD_SECONDS,
                    "limit": 100,
                },
            )
            candidates = [
                market_metadata(value)
                for value in window_payload.get("markets", []) if isinstance(value, dict)
            ]
            candidates = [item for item in candidates if item is not None]
            # Kalshi's bounded market-list payload can omit strike metadata
            # even though the authoritative per-market endpoint exposes it.
            # Reuse already captured targets first, then enrich only unknown
            # tickers concurrently. This keeps boundary discovery fast and
            # never guesses an underlying BTC target from the ticker.
            known_targets = {
                str(item.get("ticker")): {
                    key: item.get(key) for key in BTC_TARGET_RECORD_FIELDS
                }
                for item in self.markets
                if (
                    isinstance(item, dict)
                    and item.get("btc_target_status") == "AVAILABLE"
                    and item.get("btc_target_capture_version") == BTC_TARGET_CAPTURE_CONTRACT_VERSION
                )
            }
            known_targets.update({
                str(ticker): {key: record.get(key) for key in BTC_TARGET_RECORD_FIELDS}
                for ticker, record in self.state.get("markets", {}).items()
                if (
                    isinstance(record, dict)
                    and record.get("btc_target_status") == "AVAILABLE"
                    and record.get("btc_target_capture_version") == BTC_TARGET_CAPTURE_CONTRACT_VERSION
                )
            })
            get_market = getattr(rest, "get_market", None)

            async def enrich_target(item: dict[str, Any]) -> None:
                if item.get("btc_target_status") == "AVAILABLE":
                    return
                cached = known_targets.get(str(item["ticker"]))
                if cached is not None:
                    item.update(cached)
                    return
                if not callable(get_market):
                    return
                try:
                    detailed = await get_market(item["ticker"])
                except Exception as exc:  # target telemetry cannot authorize trading
                    LOG.warning(
                        "BTC TARGET LOOKUP FAILED | ticker=%s error_type=%s",
                        item["ticker"], type(exc).__name__,
                    )
                    return
                details = btc_target_metadata(detailed)
                item.update(details)
                if details["btc_target_status"] == "AVAILABLE":
                    known_targets[str(item["ticker"])] = details

            await asyncio.gather(*(enrich_target(item) for item in candidates))
            for item in candidates:
                record = self.state.get("markets", {}).get(item["ticker"])
                if isinstance(record, dict):
                    self.backfill_btc_target(record, item, reason="authoritative_market_discovery")
            # Keep a narrow rolling window: one predecessor and the nearby
            # initialized successors are enough for signal causality and
            # pre-subscription.  Bounded retention prevents an old API page
            # from becoming a substitute predecessor.
            retained = {
                item["ticker"]: item for item in self.markets
                if item["close_epoch"] >= now - 3_600
            }
            retained.update({
                item["ticker"]: item for item in candidates
                if item["close_epoch"] >= now - MARKET_DISCOVERY_LOOKBACK_SECONDS
                and item["open_epoch"] <= now + MARKET_DISCOVERY_LOOKAHEAD_SECONDS
            })
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
        target_details = btc_target_metadata(market.get("raw", market))
        # ``market_metadata`` already normalized these fields. Prefer that
        # preserved representation when present so every consumer sees the
        # same exact Decimal string and source label.
        for key in tuple(target_details):
            if key in market:
                target_details[key] = market[key]
        record = self.state["markets"].get(ticker)
        if isinstance(record, dict):
            self.backfill_btc_target(record, target_details, reason="signal_reconciliation")
            return record
        source = str(provisional["outcome"])
        signal_state = self.state.setdefault("directional_signal_state", {
            "mode": self.config["signal_mode"], "active_side": None,
            "last_source_market": None, "last_source_outcome": None,
            "last_transition": None, "updated_at": None,
        })
        source_record = self.state["markets"].get(provisional["ticker"])
        source_record_side = (
            str(source_record.get("sticky_signal_side") or source_record.get("signal_side"))
            if isinstance(source_record, dict)
            and (source_record.get("sticky_signal_side") or source_record.get("signal_side")) in {"yes", "no"}
            else None
        )
        prior_side = source_record_side or (
            str(signal_state.get("active_side"))
            if signal_state.get("active_side") in {"yes", "no"}
            else None
        )
        sticky_side, directional_transition = sticky_directional_prediction(prior_side, source)
        opposite_ladder = self.config.get("entry_execution_mode") == "opposite_side_doubling_ladder"
        side = opposite_side(sticky_side) if opposite_ladder else sticky_side
        signal_state.update({
            "mode": self.config["signal_mode"], "active_side": sticky_side,
            "last_source_market": provisional["ticker"], "last_source_outcome": source,
            "last_transition": directional_transition, "updated_at": utc_now(),
        })
        parameters = self.current_parameters()
        if opposite_ladder:
            base_before = round_shares(Decimal(self.config["starting_base"]))
            quantity, capped = base_before, False
        else:
            quantity, capped = prescribed_quantity(parameters, self.state.get("sizing"))
            base_before = sizing_state(parameters, self.state.get("sizing")).base_share_count
            effective_cap = parameters.effective_max_position(base_before)
        record = {
            "ticker": ticker,
            "market_open_epoch": market["open_epoch"], "market_close_epoch": market["close_epoch"],
            **target_details,
            "source_market_ticker": provisional["ticker"], "provisional_outcome": source,
            "provisional_outcome_details": provisional, "signal_side": side,
            "sticky_signal_side": sticky_side, "trade_side": side,
            "signal_mode": self.config["signal_mode"], "prior_signal_side": prior_side,
            "directional_transition": directional_transition,
            "signal_timestamp": utc_now(), "intended_quantity": format(quantity, "f"),
            "quantity_capped": capped, "base_before": format(base_before, "f"),
            "recovery_exponent_before": (
                0 if opposite_ladder else sizing_state(parameters, self.state.get("sizing")).recovery_exponent
            ),
            "recovery_cycle_pnl_before": (
                "0" if opposite_ladder else str(self.state.get("sizing", {}).get("recovery_cycle_pnl", "0"))
            ),
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
                "actual_entry_notional": "0",
                "actual_entry_fees": "0",
                "actual_entry_cash_cost": "0",
                "maker_limit_order_ids": [],
                "market_ioc_order_ids": [],
            },
            "strategy_version": self.config["strategy_version"], "config_hash": config_hash(self.config),
            "config_snapshot": (
                {
                    "starting_base": format(base_before, "f"),
                    "ladder_quantity_multiples": ["1", "2", "4", "8", "16"],
                    "position_cap": None,
                    "recovery": None,
                }
                if opposite_ladder else self.current_parameters().as_dict()
            ),
            "created_at": utc_now(),
            "opposite_ladder": ({
                "contract_version": OPPOSITE_LADDER_CONTRACT_VERSION,
                "state": "WAITING_FOR_DELAYED_STICKY_BAND",
                "trigger_min_cents": int(self.config["delayed_entry_threshold_cents"]),
                "trigger_max_cents": int(self.config["delayed_entry_max_trigger_cents"]),
                "take_profit_cents": int(self.config["opposite_take_profit_cents"]),
                "base_shares": format(base_before, "f"),
                "plan": None,
                "exit_latched": False,
            } if opposite_ladder else None),
            "entry_execution_mode": self.config["entry_execution_mode"],
            # The actual entry tick is frozen from the first fresh selected-
            # side executable ask observed after the exchange opens.  It is
            # never moved by later quotes.
            "initial_signal_price_cents": None,
            "initial_signal_price": None,
            "initial_signal_price_timestamp": None,
            "initial_signal_price_exchange_timestamp": None,
            "initial_signal_price_lag_seconds": None,
            "initial_signal_price_observed_lag_seconds": None,
            "initial_signal_quote": None,
            # Immutable price-only opening evidence.  It is captured from the
            # pre-subscribed WebSocket independently of later displayed-depth
            # evidence, so a slow size update cannot redefine the cohort.
            "opening_price_reference": None,
            "opening_price_reference_status": "PENDING",
            "entry_limit_cents": None,
            "maker_entry_price": None,
            # Opening intent and actual execution cost are deliberately
            # separate.  The former is frozen from quantity * ask-minus-1c;
            # the latter is rebuilt only from filled quantity, exchange fill
            # price, and fees.
            "opening_entry_quantity": format(quantity, "f"),
            "opening_entry_price": None,
            "opening_entry_cost": None,
            "opening_entry_cost_recorded_at": None,
            "requested_entry_cost": None,
            "requested_entry_cost_recorded_at": None,
            "actual_entry_notional": "0",
            "actual_entry_fees": "0",
            "actual_entry_cash_cost": "0",
            "reference_maker_entry_price": self.config["entry_price"],
            "maker_price_offset": self.config["maker_price_offset"],
            "shadow_entry_levels": {
                str(level): {
                    "eligible": True, "touched": False, "touch_timestamp": None,
                    "simulated_fill": False, "simulated_fill_timestamp": None,
                    "simulated_fill_evidence_quantity": "0.00",
                }
                for level in range(
                    int(self.config["shadow_entry_level_min_cents"]),
                    int(self.config["shadow_entry_level_max_cents"]) + 1,
                    int(self.config["shadow_entry_level_step_cents"]),
                )
            },
            "minimum_selected_price_cents": None,
            "minimum_selected_price_timestamp": None,
            "last_analytics_quote_id": None,
            "analytics_settlement_finalized": False,
            # These values are immutable policy inputs for this market.
            "stop_policy": self.config["stop_policy"],
            "stop_floor_price": self.config["stop_price"],
            "stop_baseline_entry_price": self.config["stop_baseline_entry_price"],
            # The historical field name remains schema-compatible, but v13
            # never submits a maker exit. All three prices are 51c and the
            # direct reduce-only IOC path owns every protective close.
            "hybrid_stop": {
                "enabled": bool(self.config["hybrid_stop_enabled"]),
                "trigger_cents": int(self.config["hybrid_stop_trigger_cents"]),
                "maker_exit_cents": int(self.config["hybrid_maker_exit_cents"]),
                "hard_stop_cents": int(self.config["hybrid_hard_stop_cents"]),
                "state": "ARMED", "triggered_at": None, "maker_order_id": None,
                "maker_filled_quantity": "0.00", "hard_filled_quantity": "0.00",
            },
            "exit_classification": None,
            "actual_average_entry_price": None,
            "effective_stop_price": None,
            "opening_quote_observations": [],
            "opening_quote_capture": {
                "window_seconds": int(self.config["opening_quote_capture_seconds"]),
                "max_observations": int(self.config["opening_quote_max_observations"]),
                "started_at": None,
                "discovery_anchor_at": None,
                "discovery_anchor_epoch": None,
                "completed_at": None,
                "observation_count": 0,
                "dropped_observation_count": 0,
                "unavailable_quote_count": 0,
            },
            "opening_cross_ladder": ({
                "analytics_only": True,
                "execution_mode": "shadow_only_no_exchange_orders",
                **target_details,
                "trigger_window_seconds": OPENING_CROSS_LADDER_TRIGGER_WINDOW_SECONDS,
                "trigger_levels_cents": list(OPENING_CROSS_LADDER_TRIGGER_LEVELS),
                "rung_quantities": {
                    str(level): format(quantity, "f")
                    for level, quantity in OPENING_CROSS_LADDER_RUNGS
                },
                "price_observations": [],
                "price_observation_count": 0,
                "dropped_price_observation_count": 0,
                "triggered": {},
                "window_complete": False,
                "coverage_complete": None,
                "coverage_note": None,
                "last_processed_quote_id": None,
                "feed_session_token": None,
            } if self.dry_run else None),
            # Analytics-only delayed entry lane.  It never submits an order
            # or mutates primary sizing/P&L.  For signals whose first fresh
            # selected-side ask is below 53c, the first later executable ask
            # >=53c freezes a resting limit one cent lower. Public-trade
            # volume, not the trigger quote, determines partial/full fill.
            "delayed_entry_tracking": {
                "enabled": bool(self.config["delayed_entry_tracking_enabled"]),
                "ladder_contract_version": DELAYED_ENTRY_LADDER_CONTRACT_VERSION,
                "threshold_cents": int(self.config["delayed_entry_threshold_cents"]),
                "status": "PENDING_INITIAL_PRICE",
                "eligible": None,
                "coverage_complete_from_initial_quote": False,
                "threshold_reached": False,
                "threshold_observed_ask_cents": None,
                "threshold_timestamp": None,
                "threshold_exchange_epoch": None,
                "threshold_after_open_seconds": None,
                "hypothetical_fill": False,
                "entry_limit_offset_cents": int(self.config["entry_limit_offset_cents"]),
                "entry_limit_cents": None,
                "first_entry_price_cents": None,
                "first_entry_timestamp": None,
                "first_entry_exchange_epoch": None,
                "first_entry_after_open_seconds": None,
                "first_entry_after_signal_seconds": None,
                "first_entry_after_opening_capture_window": None,
                "displayed_ask_depth": None,
                "configured_quantity_fully_fillable": None,
                "minimum_selected_ask_after_entry_cents": None,
                "maximum_selected_ask_after_entry_cents": None,
                "settlement_outcome": None,
                "directional_winner": None,
                "no_stop_gross_pnl_per_share": None,
                "no_stop_gross_pnl": None,
                "zero_fill": False,
                # Created only when the delayed >=53c entry actually
                # activates. It is analytics-only and can never create an
                # exchange order or change primary sizing/accounting state.
                "ladder": None,
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
                "entry_order_lifetime": self.config["entry_order_lifetime"],
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
        if not opposite_ladder:
            record.update({
                "position_cap_mode": (
                    "permanent_base_linked"
                    if parameters.max_position_per_base_share is not None else "fixed"
                ),
                "effective_position_cap": format(effective_cap, "f"),
            })
        self.state["markets"][ticker] = record
        self.state["active_market"] = ticker
        self.audit(
            "signal_created", ticker=ticker, source_ticker=provisional["ticker"], provisional_outcome=source,
            provisional_method=provisional.get("method"), provisional_quote_age=provisional.get("quote_age_seconds"),
            provisional_observation_window=provisional.get("observation_window_seconds"),
            provisional_qualifying_bid=provisional.get("qualifying_bid"),
            prior_signal_side=prior_side, directional_transition=directional_transition,
            prediction=sticky_side if opposite_ladder else side,
            trade_side=side, intended_quantity=format(quantity, "f"),
            btc_target_price=record.get("btc_target_price"),
            btc_target_price_display=record.get("btc_target_price_display"),
            btc_target_comparison=record.get("btc_target_comparison"),
            btc_target_source=record.get("btc_target_source"),
            btc_target_status=record.get("btc_target_status"),
        )
        observation_window = provisional.get("observation_window_seconds")
        observation_window_label = f"{observation_window}s" if observation_window is not None else "n/a"
        LOG.warning(
            "SIGNAL SOURCE | source=%s method=%s window=%s provisional=%s qualifying_bid=%s "
            "final_yes_bid=%s final_no_bid=%s quote_age=%s",
            provisional["ticker"], provisional.get("method"), observation_window_label, source.upper(),
            provisional.get("qualifying_bid"),
            provisional.get("final_yes_bid"), provisional.get("final_no_bid"),
            provisional.get("quote_age_seconds"),
        )
        exit_description = (
            (
                f"flatten_if_trade_bid_at_or_above_{self.config['opposite_take_profit_cents']}c_"
                f"or_sticky_ask_at_or_below_{self.config['opposite_take_profit_cents']}c"
            )
            if opposite_ladder
            else f"direct_ioc_at_or_below_{self.config['hybrid_stop_trigger_cents']}c"
            if self.config["stop_policy"] == "direct_ioc_at_trigger"
            else (
                f"hybrid_{self.config['hybrid_stop_trigger_cents']}c/"
                f"{self.config['hybrid_maker_exit_cents']}c/"
                f"{self.config['hybrid_hard_stop_cents']}c"
            )
        )
        LOG.warning(
            "NEW MARKET SIGNAL | ticker=%s source=%s provisional=%s prior_side=%s transition=%s "
            "prediction=%s trade_opposite=%s btc_target=%s comparison=%s base=%s entry_mode=%s exit_policy=%s",
            ticker, provisional["ticker"], source.upper(), prior_side and prior_side.upper(), directional_transition,
            (sticky_side if opposite_ladder else side).upper(), side.upper(),
            record.get("btc_target_price_display"), record.get("btc_target_comparison"),
            quantity, self.config["entry_execution_mode"], exit_description,
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

    def selected_book(self, feed: KalshiLiveFeed, record: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        """Return one fresh complete book for depth-sensitive analytics/fills/stops."""

        return feed.executable_shadow_quote(
            record["ticker"], str(record["signal_side"]), 0.0,
            float(self.config["max_stale_quote_seconds"]),
        )

    @staticmethod
    def ensure_opening_entry_cost(record: dict[str, Any]) -> bool:
        """Backfill immutable opening intent for a checkpoint from older v11 code."""

        if record.get("opening_entry_cost") is not None:
            return False
        if record.get("entry_limit_cents") is None or record.get("intended_quantity") is None:
            return False
        try:
            limit_cents = int(record["entry_limit_cents"])
            quantity = Decimal(str(record["intended_quantity"]))
            entry_price = cents_price(limit_cents)
            opening_cost = exact_entry_notional(quantity, entry_price)
        except (ArithmeticError, TypeError, ValueError):
            return False
        record.update({
            "opening_entry_quantity": format(quantity, "f"),
            "opening_entry_price": format(entry_price, "f"),
            "opening_entry_cost": format(opening_cost, "f"),
            "opening_entry_cost_recorded_at": utc_now(),
        })
        return True

    def capture_opening_price_reference(
        self, feed: KalshiLiveFeed, record: dict[str, Any], now: float,
    ) -> dict[str, Any] | None:
        """Persist the earliest post-open selected-side ask without requiring depth.

        Production feeds retain price-only WebSocket history from the upcoming
        market subscription.  Displayed depth remains a separate evidence
        stream and is never inferred here.  A worker that subscribed after the
        market opened may retain a partial reference for audit/analytics, but
        it cannot present that delayed price as complete opening coverage.
        """

        existing = record.get("opening_price_reference")
        if isinstance(existing, dict):
            return existing
        opened = float(record["market_open_epoch"])
        if now < opened:
            return None
        first_quote = getattr(feed, "first_post_open_price_quote", None)
        if callable(first_quote):
            quote, reason = first_quote(record["ticker"], str(record["signal_side"]), opened)
        else:
            # Execution adapters used by tests and offline replay predate the
            # buffered feed API. They still provide an explicitly timestamped
            # complete book; production always takes the branch above.
            if record.get("opening_quote_observations"):
                record["opening_price_reference_status"] = "PARTIAL_LEGACY_CAPTURE_ONLY"
                record["opening_price_reference_wait_reason"] = (
                    "legacy checkpoint has earlier complete-book observations but no price-only history"
                )
                return None
            quote, reason = self.selected_book(feed, record)
            if quote is not None:
                quote = dict(quote)
                quote.setdefault("coverage_complete_from_market_open", True)
                quote.setdefault("coverage_status", "COMPLETE_TIMESTAMPED_ADAPTER")
                quote.setdefault("source", "timestamped_complete_book_adapter")
                quote.setdefault("displayed_depth_available", True)
        if quote is None:
            record["opening_price_reference_status"] = "WAITING"
            record["opening_price_reference_wait_reason"] = reason
            return None
        quote_epoch = epoch(quote.get("selected_component_epoch")) or executable_quote_epoch(quote)
        if quote_epoch is None or quote_epoch < opened:
            record["opening_price_reference_status"] = "WAITING"
            record["opening_price_reference_wait_reason"] = "preopen_or_unstamped_top_of_book"
            return None
        try:
            selected_ask_cents = price_to_cents(
                str(quote["economic_price"]), "opening selected-side ask",
            )
        except (KeyError, TypeError, ValueError) as exc:
            record["opening_price_reference_status"] = "WAITING"
            record["opening_price_reference_wait_reason"] = str(exc)
            return None
        coverage_complete = bool(quote.get("coverage_complete_from_market_open"))
        quote_lag = round(max(0.0, quote_epoch - opened), 6)
        observed_lag = round(max(0.0, now - opened), 6)
        received_epoch = epoch(quote.get("received_epoch")) or _iso_epoch(quote.get("received_at"))
        reference = {
            "ticker": record["ticker"],
            "selected_side": str(record["signal_side"]),
            "selected_side_ask_cents": selected_ask_cents,
            "selected_side_ask": format(cents_price(selected_ask_cents), "f"),
            "selected_side_bid": quote.get("selected_best_bid"),
            "yes_bid": quote.get("yes_bid"),
            "yes_ask": quote.get("yes_ask"),
            "quote_id": quote.get("quote_id") or None,
            "source": quote.get("source") or "kalshi_websocket_price_only_top_of_book",
            "selected_component_epoch": quote_epoch,
            "selected_component_timestamp": datetime.fromtimestamp(quote_epoch, timezone.utc).isoformat(),
            "received_epoch": received_epoch,
            "received_at": quote.get("received_at"),
            "quote_lag_after_market_open_seconds": quote_lag,
            "worker_observation_lag_after_market_open_seconds": observed_lag,
            "subscription_started_epoch": quote.get("subscription_started_epoch"),
            "coverage_complete_from_market_open": coverage_complete,
            "coverage_status": quote.get("coverage_status") or (
                "COMPLETE" if coverage_complete else "PARTIAL"
            ),
            "displayed_depth_available": bool(quote.get("displayed_depth_available", False)),
            "displayed_depth": quote.get("displayed_depth"),
            "captured_at": utc_now(),
        }
        record["opening_price_reference"] = reference
        record["opening_price_reference_status"] = (
            "COMPLETE" if coverage_complete else "PARTIAL"
        )
        record.pop("opening_price_reference_wait_reason", None)
        capture = record.setdefault("opening_quote_capture", {})
        capture["first_price_capture_lag_seconds"] = quote_lag
        capture["first_price_observed_lag_seconds"] = observed_lag
        capture["first_price_quote_id"] = reference["quote_id"]
        capture["first_price_source"] = reference["source"]
        capture["first_price_coverage_status"] = reference["coverage_status"]
        capture["first_price_depth_available"] = reference["displayed_depth_available"]
        self.ensure_delayed_entry_tracking(
            record, quote_epoch, selected_ask_cents, str(reference["quote_id"] or "") or None,
        )
        self.audit(
            "opening_price_reference_captured", ticker=record["ticker"],
            side=record["signal_side"], selected_side_ask_cents=selected_ask_cents,
            quote_lag_after_market_open_seconds=quote_lag,
            worker_observation_lag_after_market_open_seconds=observed_lag,
            coverage_complete_from_market_open=coverage_complete,
            coverage_status=reference["coverage_status"], source=reference["source"],
            displayed_depth_available=reference["displayed_depth_available"],
            quote_id=reference["quote_id"],
        )
        LOG.warning(
            "FIRST OPEN PRICE | ticker=%s side=%s ask=%sc exchange_lag=%.6fs "
            "worker_lag=%.6fs coverage=%s depth_available=%s source=%s",
            record["ticker"], str(record["signal_side"]).upper(), selected_ask_cents,
            quote_lag, observed_lag, reference["coverage_status"],
            reference["displayed_depth_available"], reference["source"],
        )
        self.checkpoint("opening_price_reference_captured")
        return reference

    def freeze_initial_signal_price(
        self, feed: KalshiLiveFeed, record: dict[str, Any], now: float,
    ) -> Decimal | None:
        """Freeze the first valid post-open selected-side ask and its -1c limit.

        A preloaded, pre-open quote is rejected.  Later prices can update
        analytics, but can never rewrite this entry reference.
        """

        existing = record.get("initial_signal_price_cents")
        if existing is not None:
            if self.ensure_opening_entry_cost(record):
                self.audit(
                    "opening_entry_cost_backfilled", ticker=record["ticker"],
                    requested_quantity=record["opening_entry_quantity"],
                    requested_price=record["opening_entry_price"],
                    opening_entry_cost=record["opening_entry_cost"],
                )
            return cents_price(int(record["entry_limit_cents"]))
        reference = self.capture_opening_price_reference(feed, record, now)
        if reference is None:
            record["initial_signal_price_wait_reason"] = record.get(
                "opening_price_reference_wait_reason", "opening_price_reference_unavailable",
            )
            return None
        if not reference.get("coverage_complete_from_market_open"):
            record["initial_signal_price_wait_reason"] = "partial_opening_price_coverage"
            return None
        try:
            initial_cents = int(reference["selected_side_ask_cents"])
            quote_epoch = float(reference["selected_component_epoch"])
        except (KeyError, TypeError, ValueError) as exc:
            record["initial_signal_price_wait_reason"] = str(exc)
            return None
        limit_cents = initial_cents - int(self.config["entry_limit_offset_cents"])
        market_open_epoch = float(record["market_open_epoch"])
        exchange_timestamp = str(reference["selected_component_timestamp"])
        quote_lag_seconds = float(reference["quote_lag_after_market_open_seconds"])
        observed_lag_seconds = float(reference["worker_observation_lag_after_market_open_seconds"])
        intended_quantity = Decimal(str(record["intended_quantity"]))
        opening_entry_price = cents_price(limit_cents)
        opening_entry_cost = exact_entry_notional(intended_quantity, opening_entry_price)
        cost_recorded_at = utc_now()
        # Freeze the source quote before validating whether the derived order
        # is admissible. A safely rejected/no-entry signal is still part of
        # the 40–49c path and missed-winner denominators and must remain fully
        # auditable after a restart.
        record.update({
            "initial_signal_price_cents": initial_cents,
            "initial_signal_price": format(cents_price(initial_cents), "f"),
            "initial_signal_price_timestamp": utc_now(),
            "initial_signal_price_epoch": quote_epoch,
            "initial_signal_price_exchange_timestamp": exchange_timestamp,
            "initial_signal_price_lag_seconds": quote_lag_seconds,
            "initial_signal_price_observed_lag_seconds": observed_lag_seconds,
            "initial_signal_quote": reference,
            "entry_limit_cents": limit_cents,
            "opening_entry_quantity": format(intended_quantity, "f"),
            "opening_entry_price": format(opening_entry_price, "f"),
            "opening_entry_cost": format(opening_entry_cost, "f"),
            "opening_entry_cost_recorded_at": cost_recorded_at,
            # Seed the path minimum from the immutable first executable ask.
            # This keeps drawdown analytics complete even when the next book
            # event is the delayed >=53c observation rather than a duplicate
            # of the opening quote.
            "minimum_selected_price_cents": initial_cents,
            "minimum_selected_price_timestamp": exchange_timestamp,
        })
        delayed = record.setdefault("delayed_entry_tracking", {})
        threshold_cents = int(self.config["delayed_entry_threshold_cents"])
        delayed.update({
            "enabled": bool(self.config["delayed_entry_tracking_enabled"]),
            "threshold_cents": threshold_cents,
            "initial_signal_price_cents": initial_cents,
            "eligible": bool(initial_cents < threshold_cents),
            "status": "WATCHING" if initial_cents < threshold_cents else "INELIGIBLE_INITIAL_AT_OR_ABOVE_THRESHOLD",
            "coverage_complete_from_initial_quote": True,
            "coverage_started_at": utc_now(),
            "coverage_started_exchange_epoch": quote_epoch,
            "coverage_started_after_open_seconds": quote_lag_seconds,
        })
        record["opening_quote_capture_deadline_epoch"] = self.opening_quote_capture_deadline(record)
        if 1 <= limit_cents <= 99:
            record["maker_entry_price"] = format(cents_price(limit_cents), "f")
        self.audit(
            "initial_signal_price_frozen", ticker=record["ticker"], side=record["signal_side"],
            initial_signal_price_cents=initial_cents, entry_limit_cents=limit_cents,
            requested_quantity=record["opening_entry_quantity"],
            requested_price=record["opening_entry_price"],
            opening_entry_cost=record["opening_entry_cost"],
            quote_timestamp=quote_epoch, quote_exchange_timestamp=exchange_timestamp,
            quote_lag_after_market_open_seconds=quote_lag_seconds,
            worker_observation_lag_after_market_open_seconds=observed_lag_seconds,
            signal_timestamp=record.get("signal_timestamp"), quote=reference,
            monitored_entry_levels_cents=list(range(
                int(self.config["shadow_entry_level_min_cents"]),
                int(self.config["shadow_entry_level_max_cents"]) + 1,
                int(self.config["shadow_entry_level_step_cents"]),
            )),
        )
        LOG.warning(
            "OPENING ENTRY SNAPSHOT | ticker=%s side=%s market_open_epoch=%.3f "
            "signal_at=%s quote_exchange_at=%s quote_lag=%.6fs observed_lag=%.6fs "
            "initial_selected_ask=%sc entry_limit=%sc quantity=%s opening_entry_cost=$%s "
            "offset=%sc monitored=40c-49c",
            record["ticker"], str(record["signal_side"]).upper(), market_open_epoch,
            record.get("signal_timestamp"), exchange_timestamp, quote_lag_seconds,
            observed_lag_seconds, initial_cents, limit_cents,
            record["opening_entry_quantity"], record["opening_entry_cost"],
            int(self.config["entry_limit_offset_cents"]),
        )
        if not 1 <= limit_cents <= 99:
            record["entry_rejection_quote"] = {
                "at": utc_now(), "reason": "derived_limit_outside_supported_market_ticks",
                "initial_signal_price_cents": initial_cents, "entry_limit_cents": limit_cents,
            }
            self.finish_entry_attempt(record, Decimal("0"), "derived_limit_outside_supported_market_ticks")
            return None
        hard_stop = int(self.config["hybrid_hard_stop_cents"])
        if limit_cents <= hard_stop:
            record["entry_rejection_quote"] = {
                "at": utc_now(), "reason": "derived_limit_at_or_below_hybrid_hard_stop",
                "initial_signal_price_cents": initial_cents, "entry_limit_cents": limit_cents,
                "hybrid_hard_stop_cents": hard_stop,
            }
            self.finish_entry_attempt(record, Decimal("0"), "derived_limit_at_or_below_hybrid_hard_stop")
            return None
        LOG.warning("=" * 60)
        LOG.warning("SIGNAL: %s", record["ticker"])
        LOG.warning("SIDE: %s", str(record["signal_side"]).upper())
        LOG.warning("INITIAL PRICE: %sc", initial_cents)
        LOG.warning(
            "%s ENTRY: BUY %s @ %sc | QUANTITY: %s",
            "SHADOW" if self.dry_run else "LIVE", str(record["signal_side"]).upper(),
            limit_cents, record["intended_quantity"],
        )
        LOG.warning("=" * 60)
        self.checkpoint("initial_signal_price_frozen")
        return cents_price(limit_cents)

    def freeze_delayed_band_entry_price(
        self, feed: KalshiLiveFeed, record: dict[str, Any], now: float,
    ) -> Decimal | None:
        """Freeze the v13 post-60-second >=53c ask-minus-1c entry.

        The immutable opening selected-side ask is used only to establish that
        this market belongs to the delayed cohort.  Execution waits until the
        configured observation boundary, then the first fresh complete book
        at or above the threshold is decisive.  A derived limit above the
        ceiling filters the market instead of waiting for a later price and
        silently changing the researched cohort.
        """

        existing = record.get("delayed_entry_decision")
        if isinstance(existing, dict):
            if existing.get("status") == "ELIGIBLE" and record.get("entry_limit_cents") is not None:
                if self.ensure_opening_entry_cost(record):
                    self.audit(
                        "delayed_entry_cost_backfilled", ticker=record["ticker"],
                        requested_quantity=record["opening_entry_quantity"],
                        requested_price=record["opening_entry_price"],
                        opening_entry_cost=record["opening_entry_cost"],
                    )
                return cents_price(int(record["entry_limit_cents"]))
            return None

        opened = float(record["market_open_epoch"])
        start_seconds = int(self.config["delayed_entry_start_seconds"])
        if now < opened + start_seconds:
            record["delayed_entry_wait_reason"] = "observation_window_not_complete"
            return None

        reference = self.capture_opening_price_reference(feed, record, now)
        if reference is None or not reference.get("coverage_complete_from_market_open"):
            record["delayed_entry_decision"] = {
                "status": "MISSED", "reason": "complete_opening_price_reference_unavailable",
                "decided_at": utc_now(), "seconds_after_open": round(max(0.0, now - opened), 6),
            }
            self.transition(record, "MISSED_SIGNAL", "complete_opening_price_reference_unavailable")
            if self.state.get("active_market") == record["ticker"]:
                self.state["active_market"] = None
            return None

        quote, quote_state = self.selected_book(feed, record)
        if quote is None:
            record["delayed_entry_wait_reason"] = quote_state
            return None
        quote_epoch = executable_quote_epoch(quote)
        if quote_epoch is None or quote_epoch < opened + start_seconds:
            record["delayed_entry_wait_reason"] = "waiting_for_post_window_executable_book"
            return None
        try:
            opening_cents = int(reference["selected_side_ask_cents"])
            observed_cents = price_to_cents(
                str(quote["economic_price"]), "delayed selected-side executable ask",
            )
        except (KeyError, TypeError, ValueError) as exc:
            record["delayed_entry_wait_reason"] = str(exc)
            return None
        elapsed = Decimal(str(max(0.0, quote_epoch - opened)))
        decision = delayed_band_entry_decision(
            opening_ask_cents=opening_cents,
            observed_ask_cents=observed_cents,
            seconds_after_open=elapsed,
            observation_start_seconds=start_seconds,
            threshold_ask_cents=int(self.config["delayed_entry_threshold_cents"]),
            maximum_limit_cents=int(self.config["delayed_entry_max_limit_cents"]),
            offset_cents=int(self.config["entry_limit_offset_cents"]),
        )
        if decision.status == "WAITING":
            record["delayed_entry_wait_reason"] = decision.reason
            return None

        decided_at = utc_now()
        decision_record = {
            "status": decision.status,
            "reason": decision.reason,
            "decided_at": decided_at,
            "opening_selected_side_ask_cents": opening_cents,
            "observed_selected_side_ask_cents": observed_cents,
            "limit_price_cents": decision.limit_price_cents,
            "quote_exchange_epoch": quote_epoch,
            "quote_exchange_timestamp": datetime.fromtimestamp(quote_epoch, timezone.utc).isoformat(),
            "seconds_after_open": round(float(elapsed), 6),
            "threshold_ask_cents": int(self.config["delayed_entry_threshold_cents"]),
            "maximum_limit_cents": int(self.config["delayed_entry_max_limit_cents"]),
            "offset_cents": int(self.config["entry_limit_offset_cents"]),
            "quote": quote,
        }
        record["delayed_entry_decision"] = decision_record
        record.pop("delayed_entry_wait_reason", None)
        if decision.status != "ELIGIBLE" or decision.limit_price_cents is None:
            record["entry_rejection_quote"] = decision_record
            self.transition(record, "ENTRY_FILTERED", decision.reason or "delayed_entry_filtered")
            if self.state.get("active_market") == record["ticker"]:
                self.state["active_market"] = None
            self.audit("delayed_entry_filtered", ticker=record["ticker"], side=record["signal_side"], **decision_record)
            LOG.warning(
                "DELAYED ENTRY FILTERED | ticker=%s side=%s opening_ask=%sc observed_ask=%sc "
                "derived_limit=%s ceiling=%sc reason=%s",
                record["ticker"], str(record["signal_side"]).upper(), opening_cents,
                observed_cents, decision.limit_price_cents,
                int(self.config["delayed_entry_max_limit_cents"]), decision.reason,
            )
            return None

        limit_cents = int(decision.limit_price_cents)
        intended = Decimal(str(record["intended_quantity"]))
        limit_price = cents_price(limit_cents)
        requested_cost = exact_entry_notional(intended, limit_price)
        record.update({
            # For v12 these fields intentionally identify the immutable quote
            # which created the actual order; the true opening quote remains
            # separately preserved in ``opening_price_reference``.
            "initial_signal_price_cents": observed_cents,
            "initial_signal_price": format(cents_price(observed_cents), "f"),
            "initial_signal_price_timestamp": decided_at,
            "initial_signal_price_epoch": quote_epoch,
            "initial_signal_price_exchange_timestamp": decision_record["quote_exchange_timestamp"],
            "initial_signal_price_lag_seconds": decision_record["seconds_after_open"],
            "initial_signal_price_observed_lag_seconds": round(max(0.0, now - opened), 6),
            "initial_signal_quote": quote,
            "entry_limit_cents": limit_cents,
            "maker_entry_price": format(limit_price, "f"),
            "opening_entry_quantity": format(intended, "f"),
            "opening_entry_price": format(limit_price, "f"),
            "opening_entry_cost": format(requested_cost, "f"),
            "opening_entry_cost_recorded_at": decided_at,
            "minimum_selected_price_cents": observed_cents,
            "minimum_selected_price_timestamp": decision_record["quote_exchange_timestamp"],
        })
        self.audit(
            "delayed_entry_price_frozen", ticker=record["ticker"], side=record["signal_side"],
            opening_selected_side_ask_cents=opening_cents,
            qualifying_selected_side_ask_cents=observed_cents,
            entry_limit_cents=limit_cents, requested_quantity=format(intended, "f"),
            requested_entry_cost=format(requested_cost, "f"),
            seconds_after_open=decision_record["seconds_after_open"],
            maximum_limit_cents=int(self.config["delayed_entry_max_limit_cents"]),
            quote=quote,
        )
        LOG.warning("=" * 60)
        LOG.warning("SIGNAL: %s", record["ticker"])
        LOG.warning("SIDE: %s", str(record["signal_side"]).upper())
        LOG.warning("OPENING SELECTED-SIDE ASK: %sc", opening_cents)
        LOG.warning(
            "POST-60 QUALIFYING ASK: %sc | %s GTC ENTRY: BUY %s @ %sc | QUANTITY: %s | CEILING: %sc",
            observed_cents, "SHADOW" if self.dry_run else "LIVE",
            str(record["signal_side"]).upper(), limit_cents, record["intended_quantity"],
            int(self.config["delayed_entry_max_limit_cents"]),
        )
        if record.get("stop_policy", self.config["stop_policy"]) == "direct_ioc_at_trigger":
            LOG.warning(
                "DIRECT PROTECTIVE EXIT: executable selected-side bid <=%sc | "
                "reduce_only=true tif=immediate_or_cancel residual_retries=authoritative_position",
                record["hybrid_stop"]["trigger_cents"],
            )
        else:
            LOG.warning(
                "HYBRID STOP: trigger=%sc maker_exit=%sc hard_stop=%sc",
                record["hybrid_stop"]["trigger_cents"], record["hybrid_stop"]["maker_exit_cents"],
                record["hybrid_stop"]["hard_stop_cents"],
            )
        LOG.warning("=" * 60)
        self.checkpoint("delayed_entry_price_frozen")
        return limit_price

    def freeze_opposite_ladder_plan(
        self, feed: KalshiLiveFeed, record: dict[str, Any], now: float,
    ) -> dict[str, Any] | None:
        """Freeze or reject from the first fresh post-60-second quote pair.

        The sticky prediction-side ask must be 53c..57c inclusive at that
        single decision point. Outside the band is a terminal no-entry for the
        market. Inside the band, every order is a BUY on the opposite side and
        the first tick is the exact complement ``100 - sticky ask`` (47c..43c).
        The independently observed opposite ask remains the post-only safety
        reference; it never moves the frozen entry tick.
        """

        ladder = record.setdefault("opposite_ladder", {})
        existing = ladder.get("plan")
        if isinstance(existing, dict):
            return existing
        # A restored revision-2 record without a frozen plan has no exchange
        # intent to preserve. Mark it revision 3 before making the new,
        # exposure-narrowing one-time decision. Frozen revision-2 plans return
        # above and continue under their original durable contract.
        ladder["contract_version"] = OPPOSITE_LADDER_CONTRACT_VERSION
        opened = float(record["market_open_epoch"])
        start_seconds = int(self.config["delayed_entry_start_seconds"])
        if now < opened + start_seconds:
            ladder["wait_reason"] = "delayed_observation_window_not_complete"
            return None
        sticky_side = str(record["sticky_signal_side"])
        trade_side = str(record["trade_side"])
        sticky_quote, sticky_state = feed.executable_shadow_quote(
            record["ticker"], sticky_side, 0.0,
            float(self.config["max_stale_quote_seconds"]),
        )
        opposite_quote, opposite_state = feed.executable_shadow_quote(
            record["ticker"], trade_side, 0.0,
            float(self.config["max_stale_quote_seconds"]),
        )
        if sticky_quote is None or opposite_quote is None:
            ladder["wait_reason"] = (
                f"fresh_books_unavailable:sticky={sticky_state},opposite={opposite_state}"
            )
            return None
        sticky_epoch = executable_quote_epoch(sticky_quote)
        opposite_epoch = executable_quote_epoch(opposite_quote)
        if (
            sticky_epoch is None or opposite_epoch is None
            or sticky_epoch < opened + start_seconds
            or opposite_epoch < opened + start_seconds
        ):
            ladder["wait_reason"] = "waiting_for_fresh_post_window_books"
            return None
        try:
            sticky_ask = price_to_cents(
                str(sticky_quote["economic_price"]), "sticky-side delayed ask",
            )
            opposite_ask = price_to_cents(
                str(opposite_quote["economic_price"]), "opposite-side delayed ask",
            )
        except (KeyError, TypeError, ValueError) as exc:
            ladder["wait_reason"] = str(exc)
            return None
        lower = int(self.config["delayed_entry_threshold_cents"])
        upper = int(self.config["delayed_entry_max_trigger_cents"])
        if not lower <= sticky_ask <= upper:
            decided_at = utc_now()
            reason = "first_post60_sticky_ask_outside_53_57_band"
            ladder.update({
                "state": "FILTERED", "wait_reason": None,
                "filtered_reason": reason, "filtered_at": decided_at,
                "last_sticky_ask_cents": sticky_ask,
                "last_opposite_ask_cents": opposite_ask,
                "last_quote_at": decided_at,
            })
            record["delayed_entry_decision"] = {
                "status": "REJECTED", "reason": reason,
                "sticky_side": sticky_side, "trade_side": trade_side,
                "sticky_ask_cents": sticky_ask,
                "opposite_ask_cents": opposite_ask,
                "trigger_min_cents": lower, "trigger_max_cents": upper,
                "quote_exchange_epoch": max(sticky_epoch, opposite_epoch),
                "seconds_after_open": round(max(sticky_epoch, opposite_epoch) - opened, 6),
                "decided_at": decided_at,
            }
            self.transition(record, "ENTRY_FILTERED", reason)
            if self.state.get("active_market") == record["ticker"]:
                self.state["active_market"] = None
            self.audit(
                "opposite_ladder_entry_filtered", ticker=record["ticker"],
                sticky_side=sticky_side, trade_side=trade_side,
                sticky_ask_cents=sticky_ask, opposite_ask_cents=opposite_ask,
                trigger_min_cents=lower, trigger_max_cents=upper,
                terminal_for_market=True,
            )
            LOG.warning(
                "OPPOSITE LADDER SKIP | ticker=%s sticky_prediction=%s first_post60_ask=%sc "
                "required_band=%s..%sc trade_side=%s orders_sent=0 terminal_for_market=true",
                record["ticker"], sticky_side.upper(), sticky_ask, lower, upper,
                trade_side.upper(),
            )
            return None
        plan = build_opposite_ladder_plan(
            sticky_side=sticky_side,
            sticky_ask_cents=sticky_ask,
            opposite_ask_cents=opposite_ask,
            base_shares=self.config["starting_base"],
            trigger_min_cents=lower,
            trigger_max_cents=upper,
            initial_offset_cents=int(self.config["entry_limit_offset_cents"]),
        ).as_dict()
        frozen_at = utc_now()
        plan.update({
            "frozen_at": frozen_at,
            "sticky_quote_exchange_epoch": sticky_epoch,
            "opposite_quote_exchange_epoch": opposite_epoch,
            "seconds_after_open": round(max(sticky_epoch, opposite_epoch) - opened, 6),
            "sticky_quote": sticky_quote,
            "opposite_quote": opposite_quote,
        })
        ladder.update({"state": "PLAN_FROZEN", "plan": plan, "wait_reason": None})
        initial = plan["orders"][0]
        initial_cents = int(initial["price_cents"])
        minimum_initial = int(self.config["opposite_initial_limit_min_cents"])
        maximum_initial = int(self.config["opposite_initial_limit_max_cents"])
        if not minimum_initial <= initial_cents <= maximum_initial:
            raise RuntimeError("frozen opposite-side initial order escaped the reviewed 43c..47c band")
        record.update({
            "initial_signal_price_cents": opposite_ask,
            "initial_signal_price": format(cents_price(opposite_ask), "f"),
            "initial_signal_price_timestamp": frozen_at,
            "initial_signal_price_exchange_timestamp": datetime.fromtimestamp(
                opposite_epoch, timezone.utc,
            ).isoformat(),
            "initial_signal_price_lag_seconds": plan["seconds_after_open"],
            "entry_limit_cents": initial["price_cents"],
            "maker_entry_price": initial["price"],
            "opening_entry_quantity": initial["quantity"],
            "opening_entry_price": initial["price"],
            "opening_entry_cost": format(
                exact_entry_notional(initial["quantity"], initial["price"]), "f",
            ),
            "opening_entry_cost_recorded_at": frozen_at,
            "intended_quantity": plan["maximum_quantity"],
            "ladder_maximum_quantity": plan["maximum_quantity"],
            "minimum_selected_price_cents": opposite_ask,
            "minimum_selected_price_timestamp": datetime.fromtimestamp(
                opposite_epoch, timezone.utc,
            ).isoformat(),
            "delayed_entry_decision": {
                "status": "ELIGIBLE", "reason": "first_post60_sticky_ask_inside_53_57_band",
                "sticky_side": sticky_side, "trade_side": trade_side,
                "sticky_ask_cents": sticky_ask, "opposite_ask_cents": opposite_ask,
                "limit_price_cents": initial_cents,
                "limit_price_basis": "100_minus_sticky_ask",
                "trigger_min_cents": lower, "trigger_max_cents": upper,
                "initial_limit_min_cents": minimum_initial,
                "initial_limit_max_cents": maximum_initial,
                "terminal_decision": True, "decided_at": frozen_at,
            },
        })
        self.audit(
            "opposite_ladder_plan_frozen", ticker=record["ticker"],
            sticky_side=sticky_side, trade_side=trade_side,
            sticky_ask_cents=sticky_ask, opposite_ask_cents=opposite_ask,
            plan=plan,
        )
        LOG.warning("=" * 68)
        LOG.warning(
            "OPPOSITE LADDER SIGNAL | ticker=%s sticky_prediction=%s first_post60_ask=%sc "
            "required_band=53..57c trade_side=%s observed_opposite_ask=%sc "
            "initial_limit=%sc basis=100-minus-sticky-ask allowed_initial=43..47c base=%s",
            record["ticker"], sticky_side.upper(), sticky_ask, trade_side.upper(),
            opposite_ask, initial["price_cents"], plan["base_shares"],
        )
        LOG.warning(
            "LADDER PLAN | %s",
            " | ".join(
                f"BUY {trade_side.upper()} {item['quantity']}@{item['price_cents']}c GTC"
                for item in plan["orders"]
            ),
        )
        LOG.warning(
            "FLATTEN BOUNDARY | trigger=executable %s bid >=%sc OR executable %s ask <=%sc "
            "action=cancel_all_entries_then_reduce_only_IOC_flatten",
            trade_side.upper(), int(self.config["opposite_take_profit_cents"]),
            sticky_side.upper(), int(self.config["opposite_take_profit_cents"]),
        )
        LOG.warning("=" * 68)
        self.checkpoint("opposite_ladder_plan_frozen")
        return plan

    def ensure_delayed_entry_tracking(
        self,
        record: dict[str, Any],
        quote_epoch: float | None = None,
        observed_ask_cents: int | None = None,
        observed_quote_id: str | None = None,
    ) -> dict[str, Any] | None:
        """Return/backfill the compact >=53c tracker for an active record.

        Older v11 checkpoints did not have this analytics-only object.  They
        can continue being managed safely, but their tracker is explicitly
        marked as late-started so downstream analysis cannot mistake partial
        quote coverage for complete history.  New records may initialize this
        tracker from their first fresh book even when a persistent circuit
        breaker correctly prevents the primary order path from freezing an
        execution price.
        """

        existing_tracker = isinstance(record.get("delayed_entry_tracking"), dict)
        tracker = record.get("delayed_entry_tracking") if existing_tracker else {}
        if not isinstance(tracker, dict):
            tracker = {}
        record["delayed_entry_tracking"] = tracker
        if tracker.get("status") not in {None, "PENDING_INITIAL_PRICE"}:
            return tracker
        initial_raw = record.get("initial_signal_price_cents")
        initial_source = "frozen_strategy_entry_reference" if initial_raw is not None else None
        reference = record.get("opening_price_reference")
        reference_used = False
        earliest_opening_quote: tuple[float, int] | None = None
        if initial_raw is None:
            reference_is_complete = bool(
                isinstance(reference, dict)
                and reference.get("coverage_complete_from_market_open")
            )
            if reference_is_complete and reference.get("selected_side_ask_cents") is not None:
                initial_raw = reference["selected_side_ask_cents"]
                reference_used = True
                initial_source = "immutable_price_only_opening_reference"
            else:
                # Backward compatibility only: an old checkpoint may contain
                # complete-depth opening observations but no price reference.
                # Keep its earliest captured ask while explicitly retaining
                # partial coverage; do not fabricate a true opening quote.
                for observation in record.get("opening_quote_observations", []):
                    if not isinstance(observation, dict):
                        continue
                    try:
                        observation_cents = price_to_cents(
                            str(observation["selected_best_ask"]),
                            "stored opening selected-side ask",
                        )
                        source_ms = observation.get("source_timestamp_ms")
                        observation_epoch = (
                            float(source_ms) / 1000
                            if source_ms is not None
                            else float(observation.get("captured_epoch") or 0)
                        )
                    except (KeyError, TypeError, ValueError):
                        continue
                    candidate = (observation_epoch, observation_cents)
                    if earliest_opening_quote is None or candidate[0] < earliest_opening_quote[0]:
                        earliest_opening_quote = candidate
                if earliest_opening_quote is not None:
                    initial_raw = earliest_opening_quote[1]
                    initial_source = "legacy_first_captured_complete_book"
                elif isinstance(reference, dict) and reference.get("selected_side_ask_cents") is not None:
                    initial_raw = reference["selected_side_ask_cents"]
                    reference_used = True
                    initial_source = "partial_price_only_opening_reference"
                else:
                    initial_raw = observed_ask_cents
                    if initial_raw is not None:
                        initial_source = "late_observed_complete_book"
        if initial_raw is None:
            return tracker
        threshold = int(self.config["delayed_entry_threshold_cents"])
        initial = int(initial_raw)
        if reference_used:
            started_epoch = float(reference.get("selected_component_epoch") or 0)
        elif record.get("initial_signal_price_epoch") is not None:
            started_epoch = float(record["initial_signal_price_epoch"])
        elif earliest_opening_quote is not None:
            started_epoch = earliest_opening_quote[0]
        else:
            started_epoch = float(quote_epoch or 0)
        open_epoch = float(record.get("market_open_epoch") or 0)
        complete_coverage = bool(
            isinstance(reference, dict)
            and reference_used
            and reference.get("coverage_complete_from_market_open")
        )
        if record.get("initial_signal_price_cents") is not None and isinstance(reference, dict):
            complete_coverage = bool(reference.get("coverage_complete_from_market_open"))
        tracker.update({
            "enabled": bool(self.config["delayed_entry_tracking_enabled"]),
            "threshold_cents": threshold,
            "initial_signal_price_cents": initial,
            "eligible": bool(initial < threshold),
            "status": "WATCHING" if initial < threshold else "INELIGIBLE_INITIAL_AT_OR_ABOVE_THRESHOLD",
            "coverage_complete_from_initial_quote": complete_coverage,
            "initial_quote_source": initial_source,
            "coverage_started_at": utc_now(),
            "coverage_started_exchange_epoch": started_epoch or None,
            "coverage_started_after_open_seconds": (
                round(max(0.0, started_epoch - open_epoch), 6) if started_epoch and open_epoch else None
            ),
            "threshold_reached": False,
            "hypothetical_fill": False,
        })
        if not complete_coverage:
            tracker["migration_note"] = (
                "tracker_added_after_signal; pre-migration threshold crosses are unknown; "
                "opening coverage is partial and pre-subscription price events are unknown"
            )
        else:
            tracker.pop("migration_note", None)
        return tracker

    def observe_opening_cross_ladder(
        self, feed: KalshiLiveFeed, record: dict[str, Any], now: float,
    ) -> bool:
        """Track shadow 53–59c triggers and one conservative five-rung ladder.

        Trigger prices are observed only in the first 60 seconds. Once a
        threshold activates, its independent counterfactual ladder remains
        under observation through market close/settlement. Price-only asks
        establish touches. Simulated fills require subsequent public trade
        volume, allocated once across the 50/40/30/20/10 orders in descending
        price priority; no queue priority is claimed and no exchange order is
        ever submitted by this analytics lane.
        """

        if not self.dry_run:
            return False
        tracker = record.get("opening_cross_ladder")
        if not isinstance(tracker, dict):
            tracker = {
                "analytics_only": True,
                "execution_mode": "shadow_only_no_exchange_orders",
                **{
                    key: record.get(key)
                    for key in BTC_TARGET_RECORD_FIELDS
                },
                "trigger_window_seconds": OPENING_CROSS_LADDER_TRIGGER_WINDOW_SECONDS,
                "trigger_levels_cents": list(OPENING_CROSS_LADDER_TRIGGER_LEVELS),
                "rung_quantities": {
                    str(level): format(quantity, "f")
                    for level, quantity in OPENING_CROSS_LADDER_RUNGS
                },
                "price_observations": [], "price_observation_count": 0,
                "dropped_price_observation_count": 0, "triggered": {},
                "window_complete": False, "coverage_complete": None,
                "coverage_note": "migrated active record; earlier price-only events may be unavailable",
                "last_processed_quote_id": None, "feed_session_token": None,
            }
            record["opening_cross_ladder"] = tracker
        getter = getattr(feed, "selected_price_quotes_between", None)
        if not callable(getter):
            return False
        opened = float(record["market_open_epoch"])
        closed = float(record["market_close_epoch"])
        window_end = min(closed, opened + OPENING_CROSS_LADDER_TRIGGER_WINDOW_SECONDS)
        quotes = getter(record["ticker"], str(record["signal_side"]), opened, min(now, closed))
        changed = False
        reference = record.get("opening_price_reference")
        if tracker.get("coverage_complete") is None and isinstance(reference, dict):
            tracker["coverage_complete"] = bool(reference.get("coverage_complete_from_market_open"))
            tracker["coverage_note"] = reference.get("coverage_status")
            changed = True
        feed_token = str(getattr(feed, "session_token", "") or "")
        prior_token = str(tracker.get("feed_session_token") or "")
        if prior_token and feed_token and prior_token != feed_token and now < closed:
            tracker["coverage_complete"] = False
            tracker["coverage_note"] = "worker_or_websocket_session_changed_during_market"
            tracker["continuity_gap_count"] = int(tracker.get("continuity_gap_count", 0)) + 1
            tracker["last_processed_quote_id"] = None
            changed = True
        if feed_token:
            tracker["feed_session_token"] = feed_token

        last_id = str(tracker.get("last_processed_quote_id") or "")
        start_index = 0
        if last_id:
            for index, item in enumerate(quotes):
                if str(item.get("quote_id") or "") == last_id:
                    start_index = index + 1
                    break
            else:
                tracker["coverage_complete"] = False
                tracker["coverage_note"] = "prior_price_quote_not_present_after_feed_restart"
                tracker["continuity_gap_count"] = int(tracker.get("continuity_gap_count", 0)) + 1
                changed = True

        observations = tracker.setdefault("price_observations", [])
        observation_ids = {
            str(item.get("quote_id") or "") for item in observations if isinstance(item, dict)
        }
        triggered = tracker.setdefault("triggered", {})
        newly_triggered: list[int] = []
        newly_touched: list[str] = []
        max_observations = int(self.config["opening_quote_max_observations"])
        for quote in quotes[start_index:]:
            quote_id = str(quote.get("quote_id") or "")
            try:
                quote_epoch = float(quote["selected_component_epoch"])
                ask_cents = price_to_cents(str(quote["economic_price"]), "ladder selected-side ask")
            except (KeyError, TypeError, ValueError):
                continue
            if quote_epoch <= window_end:
                tracker["price_observation_count"] = int(tracker.get("price_observation_count", 0)) + 1
                if (not quote_id or quote_id not in observation_ids) and len(observations) < max_observations:
                    observations.append({
                        "quote_id": quote_id or None,
                        "selected_side_ask_cents": ask_cents,
                        "selected_side_bid": quote.get("selected_best_bid"),
                        "selected_component_epoch": quote_epoch,
                        "selected_component_timestamp": datetime.fromtimestamp(
                            quote_epoch, timezone.utc,
                        ).isoformat(),
                        "elapsed_after_open_seconds": round(max(0.0, quote_epoch - opened), 6),
                        "source": quote.get("source"),
                        "displayed_depth_available": False,
                    })
                    if quote_id:
                        observation_ids.add(quote_id)
                    changed = True
                elif quote_id not in observation_ids:
                    tracker["dropped_price_observation_count"] = int(
                        tracker.get("dropped_price_observation_count", 0)
                    ) + 1
                prior_max = tracker.get("maximum_ask_cents_first_60_seconds")
                prior_min = tracker.get("minimum_ask_cents_first_60_seconds")
                if prior_max is None or ask_cents > int(prior_max):
                    tracker["maximum_ask_cents_first_60_seconds"] = ask_cents
                    changed = True
                if prior_min is None or ask_cents < int(prior_min):
                    tracker["minimum_ask_cents_first_60_seconds"] = ask_cents
                    changed = True
                for threshold in OPENING_CROSS_LADDER_TRIGGER_LEVELS:
                    key = str(threshold)
                    if ask_cents < threshold or key in triggered:
                        continue
                    triggered[key] = {
                        "trigger_cents": threshold,
                        "trigger_observed_ask_cents": ask_cents,
                        **{
                            field_name: record.get(field_name)
                            for field_name in BTC_TARGET_RECORD_FIELDS
                        },
                        "trigger_quote_id": quote_id or None,
                        "trigger_exchange_epoch": quote_epoch,
                        "trigger_exchange_timestamp": datetime.fromtimestamp(
                            quote_epoch, timezone.utc,
                        ).isoformat(),
                        "trigger_after_open_seconds": round(max(0.0, quote_epoch - opened), 6),
                        "coverage_complete": bool(tracker.get("coverage_complete")),
                        "status": "ACTIVE",
                        "public_trade_cursor_version": 1,
                        "public_trade_feed_session_token": None,
                        "last_processed_public_trade_id": None,
                        "public_trade_events_processed": 0,
                        "rungs": {
                            str(level): {
                                "price_cents": level, "requested_quantity": format(quantity, "f"),
                                "touched": False, "touch_timestamp": None,
                                "simulated_filled_quantity": "0.00", "simulated_full_fill": False,
                                "first_fill_timestamp": None, "final_fill_timestamp": None,
                            }
                            for level, quantity in OPENING_CROSS_LADDER_RUNGS
                        },
                        "settlement_outcome": None, "directional_winner": None,
                        "gross_no_stop_pnl": None,
                    }
                    newly_triggered.append(threshold)
                    changed = True
            for threshold_text, lane in triggered.items():
                if quote_epoch < float(lane.get("trigger_exchange_epoch") or float("inf")):
                    continue
                for level_text, rung in lane.get("rungs", {}).items():
                    level = int(level_text)
                    if ask_cents <= level and not rung.get("touched"):
                        rung.update({
                            "touched": True,
                            "touch_timestamp": datetime.fromtimestamp(quote_epoch, timezone.utc).isoformat(),
                            "touch_quote_id": quote_id or None,
                            "touch_observed_ask_cents": ask_cents,
                            "touch_evidence": "price_only_executable_selected_side_ask",
                        })
                        newly_touched.append(f">={threshold_text}c:{level}c")
                        changed = True
            tracker["last_processed_quote_id"] = quote_id or tracker.get("last_processed_quote_id")

        # Both the first-60 and delayed-entry cohorts use this exact durable
        # fill allocator. This prevents their ladder fill semantics from
        # drifting apart in later releases.
        side = str(record["signal_side"])
        for lane in triggered.values():
            changed = self.advance_shadow_ladder_fills(feed, record, lane) or changed

        if now >= window_end and not tracker.get("window_complete"):
            tracker["window_complete"] = True
            tracker["window_completed_at"] = utc_now()
            changed = True
        if changed:
            self.audit(
                "opening_cross_ladder_updated", ticker=record["ticker"],
                side=record["signal_side"], analytics_only=True,
                btc_target_price=record.get("btc_target_price"),
                btc_target_price_display=record.get("btc_target_price_display"),
                btc_target_comparison=record.get("btc_target_comparison"),
                btc_target_source=record.get("btc_target_source"),
                entry_circuit_breaker_ignored_for_observation=True,
                newly_triggered_thresholds=newly_triggered,
                newly_touched_rungs=newly_touched,
                price_observation_count=tracker.get("price_observation_count"),
                coverage_complete=tracker.get("coverage_complete"),
                coverage_note=tracker.get("coverage_note"),
                triggered=triggered,
            )
            if newly_triggered or newly_touched:
                LOG.warning(
                    "OPENING CROSS LADDER | ticker=%s side=%s btc_target=%s comparison=%s "
                    "triggers=%s touches=%s "
                    "coverage=%s analytics_only=true breaker_independent=true",
                    record["ticker"], side.upper(), record.get("btc_target_price_display"),
                    record.get("btc_target_comparison"), newly_triggered, newly_touched,
                    tracker.get("coverage_note"),
                )
            self.checkpoint("opening_cross_ladder_updated")
        return changed

    def advance_shadow_ladder_fills(
        self, feed: KalshiLiveFeed, record: dict[str, Any], lane: dict[str, Any],
    ) -> bool:
        """Advance one analytics lane using durable public-trade evidence.

        Public volume is consumed once in descending limit-price priority.
        This deliberately claims neither queue priority nor an exchange fill;
        it is the shared deterministic shadow assumption for opening and
        delayed cohorts.  A delayed direct entry is placed ahead of the fixed
        ladder because its limit is always at least 52c, above the 50c rung.
        """

        trigger_epoch = float(lane["trigger_exchange_epoch"])
        try:
            events = feed.public_trades_after(
                record["ticker"], datetime.fromtimestamp(trigger_epoch, timezone.utc),
            )
        except (AttributeError, TypeError, ValueError):
            events = []
        feed_session_token = str(getattr(feed, "session_token", "") or "")
        prior_session_token = str(lane.get("public_trade_feed_session_token") or "")
        last_trade_id = str(lane.get("last_processed_public_trade_id") or "")
        processed_keys = list(lane.get("processed_public_trade_keys") or [])
        processed_key_set = set(processed_keys)
        start_event_index = 0
        if last_trade_id and not processed_keys:
            # Backward-compatible recovery for pre-key checkpoints. Trade IDs
            # are exchange-stable, so locating the cursor does not depend on
            # the WebSocket session that originally observed it.
            for index, event in enumerate(events):
                if str(event.get("trade_id") or "") == last_trade_id:
                    start_event_index = index + 1
                    break
            else:
                # The bounded feed buffer may have rolled past the cursor.
                # Its remaining events are newer and can be consumed once.
                if events:
                    lane["public_trade_cursor_gap_count"] = int(
                        lane.get("public_trade_cursor_gap_count", 0)
                    ) + 1
        if prior_session_token and feed_session_token and prior_session_token != feed_session_token:
            lane["public_trade_session_change_count"] = int(
                lane.get("public_trade_session_change_count", 0)
            ) + 1
        lane["public_trade_feed_session_token"] = feed_session_token
        order_specs: list[tuple[str, int, Decimal, dict[str, Any]]] = []
        direct_order = lane.get("direct_entry_order")
        if isinstance(direct_order, dict):
            try:
                direct_price = int(direct_order["price_cents"])
                direct_requested = Decimal(str(
                    direct_order.get("fill_ceiling_quantity")
                    if direct_order.get("fill_ceiling_quantity") is not None
                    else direct_order["requested_quantity"]
                ))
            except (ArithmeticError, KeyError, TypeError, ValueError):
                direct_price = 0
                direct_requested = Decimal("0")
            if direct_price > 0 and direct_requested > 0:
                order_specs.append(("direct_entry", direct_price, direct_requested, direct_order))
        for level, requested in OPENING_CROSS_LADDER_RUNGS:
            rung = lane.get("rungs", {}).get(str(level))
            if isinstance(rung, dict):
                order_specs.append((f"rung:{level}", level, requested, rung))
        order_specs.sort(key=lambda item: item[1], reverse=True)
        allocations = {
            key: {
                "quantity": Decimal(str(order.get("simulated_filled_quantity") or "0")),
                "first": order.get("first_fill_timestamp"),
                "last": order.get("last_fill_timestamp"),
            }
            for key, _, _, order in order_specs
        }
        changed = False
        side = str(record["signal_side"])
        for event in events[start_event_index:]:
            trade_id = str(event.get("trade_id") or "")
            event_key = trade_id or "|".join((
                str(event.get("source_server_timestamp") or event.get("received_at") or ""),
                str(event.get(f"{side}_price") or ""),
                str(event.get("count") or ""),
            ))
            if event_key in processed_key_set:
                continue
            if event_key:
                append_unique(processed_keys, event_key, maximum=4096)
                processed_key_set.add(event_key)
            if trade_id:
                lane["last_processed_public_trade_id"] = trade_id
            lane["public_trade_events_processed"] = int(
                lane.get("public_trade_events_processed", 0)
            ) + 1
            try:
                event_cents = price_to_cents(str(event[f"{side}_price"]), "ladder public trade price")
                available = Decimal(str(event.get("count") or "0"))
            except (KeyError, ArithmeticError, TypeError, ValueError):
                continue
            if available <= 0:
                continue
            for key, limit_cents, requested, _order in order_specs:
                if event_cents > limit_cents or available <= 0:
                    continue
                bucket = allocations[key]
                remaining = requested - bucket["quantity"]
                if remaining <= 0:
                    continue
                allocated = min(remaining, available)
                if allocated <= 0:
                    continue
                bucket["quantity"] += allocated
                bucket["first"] = (
                    bucket["first"] or event.get("source_server_timestamp") or event.get("received_at")
                )
                bucket["last"] = event.get("source_server_timestamp") or event.get("received_at")
                available -= allocated
        lane["processed_public_trade_keys"] = processed_keys
        for key, _limit_cents, requested, order in order_specs:
            allocation = allocations[key]
            prior_quantity = Decimal(str(order.get("simulated_filled_quantity") or "0"))
            filled_quantity = allocation["quantity"]
            if filled_quantity != prior_quantity:
                order.update({
                    "simulated_filled_quantity": format(filled_quantity, "f"),
                    "simulated_full_fill": filled_quantity >= requested,
                    "first_fill_timestamp": allocation["first"],
                    "final_fill_timestamp": allocation["last"] if filled_quantity >= requested else None,
                    "fill_assumption": (
                        "post_trigger_public_trade_volume_allocated_once_across_descending_limits_"
                        "without_queue_priority"
                    ),
                })
                changed = True
        return changed

    def sync_delayed_direct_entry(self, record: dict[str, Any], delayed: dict[str, Any]) -> bool:
        """Copy v3 direct-limit fill evidence into the durable summary fields."""

        lane = delayed.get("ladder")
        order = lane.get("direct_entry_order") if isinstance(lane, dict) else None
        if not isinstance(order, dict):
            return False
        requested = Decimal(str(order.get("requested_quantity") or "0"))
        filled = Decimal(str(order.get("simulated_filled_quantity") or "0"))
        full = bool(requested > 0 and filled >= requested)
        prior = (
            delayed.get("hypothetical_fill"), delayed.get("hypothetical_filled_quantity"),
            delayed.get("configured_quantity_fully_fillable"), delayed.get("status"),
            delayed.get("first_entry_timestamp"), delayed.get("first_entry_price_cents"),
        )
        delayed.update({
            "hypothetical_fill": filled > 0,
            "hypothetical_filled_quantity": format(filled, "f"),
            "configured_quantity_fully_fillable": full,
        })
        if order.get("fill_ceiling_quantity") is not None and filled < requested:
            order["status"] = "CANCELLED_REMAINDER_FOR_STOP"
        else:
            order["status"] = "FILLED" if full else ("PARTIAL" if filled > 0 else "RESTING")
        if filled > 0:
            fill_timestamp = order.get("first_fill_timestamp")
            fill_epoch = _iso_epoch(fill_timestamp)
            open_epoch = float(record.get("market_open_epoch") or 0)
            signal_epoch = _iso_epoch(record.get("signal_timestamp"))
            delayed.update({
                "status": "LIMIT_FILLED" if full else "LIMIT_PARTIAL",
                "first_entry_price_cents": int(order["price_cents"]),
                "first_entry_price": format(cents_price(int(order["price_cents"])), "f"),
                "first_entry_timestamp": fill_timestamp,
                "first_entry_exchange_epoch": fill_epoch or None,
                "first_entry_exchange_timestamp": fill_timestamp,
                "first_entry_after_open_seconds": (
                    round(max(0.0, fill_epoch - open_epoch), 6)
                    if fill_epoch and open_epoch else None
                ),
                "first_entry_after_signal_seconds": (
                    _seconds_since(signal_epoch, fill_epoch) if fill_epoch is not None else None
                ),
                "first_entry_after_opening_capture_window": bool(
                    fill_epoch and open_epoch
                    and fill_epoch - open_epoch >= int(self.config["opening_quote_capture_seconds"])
                ),
                "final_entry_timestamp": order.get("final_fill_timestamp"),
            })
        elif delayed.get("settlement_outcome") is None:
            delayed["status"] = "LIMIT_PENDING"
        current = (
            delayed.get("hypothetical_fill"), delayed.get("hypothetical_filled_quantity"),
            delayed.get("configured_quantity_fully_fillable"), delayed.get("status"),
            delayed.get("first_entry_timestamp"), delayed.get("first_entry_price_cents"),
        )
        return current != prior

    def advance_delayed_direct_hybrid_stop(
        self, feed: KalshiLiveFeed, record: dict[str, Any], delayed: dict[str, Any], now: float,
    ) -> bool:
        """Advance the analytics-only protective exit for simulated entry quantity.

        A selected-side executable bid at or below 51c freezes the filled entry
        quantity and cancels the unfilled direct-limit remainder. The 52c maker
        exit then requires subsequent public-trade volume at or above 52c. If
        an executable bid reaches 50c first, only the still-open quantity exits
        at that bid (capped at 50c). This makes partial exits and retries
        deterministic without claiming queue priority or historical fills.
        """

        contract_version = int(delayed.get("ladder_contract_version") or 1)
        if contract_version < 3:
            return False
        lane = delayed.get("ladder")
        if not isinstance(lane, dict):
            return False
        order = lane.get("direct_entry_order")
        stop = lane.get("direct_hybrid_stop")
        if not isinstance(order, dict) or not isinstance(stop, dict) or not stop.get("enabled"):
            return False
        filled = Decimal(str(order.get("simulated_filled_quantity") or "0"))
        if filled <= 0 or stop.get("state") == "CLOSED":
            return False
        first_fill_epoch = _iso_epoch(order.get("first_fill_timestamp"))
        if first_fill_epoch is None:
            return False
        getter = getattr(feed, "selected_price_quotes_between", None)
        if not callable(getter):
            return False
        closed = float(record["market_close_epoch"])
        quote_start = float(stop.get("trigger_exchange_epoch") or first_fill_epoch)
        quote_rows: list[tuple[float, int, dict[str, Any]]] = []
        for quote in getter(
            record["ticker"], str(record["signal_side"]), quote_start, min(now, closed),
        ):
            try:
                quote_epoch = float(quote["selected_component_epoch"])
                bid_cents = price_to_cents(
                    str(quote["selected_best_bid"]), "delayed hybrid selected-side bid",
                )
            except (KeyError, TypeError, ValueError):
                continue
            if quote_epoch >= first_fill_epoch:
                quote_rows.append((quote_epoch, bid_cents, quote))
        quote_rows.sort(key=lambda row: row[0])
        changed = False
        if contract_version >= 4:
            if stop.get("state") != "ARMED":
                return False
            trigger_row = next(
                (row for row in quote_rows if row[1] <= int(stop["trigger_cents"])), None,
            )
            if trigger_row is None:
                return False
            trigger_epoch, trigger_bid, trigger_quote = trigger_row
            trigger_timestamp = datetime.fromtimestamp(trigger_epoch, timezone.utc).isoformat()
            target_quantity = filled
            exit_price_cents = min(int(stop["trigger_cents"]), trigger_bid)
            entry_cost = target_quantity * cents_price(int(order["price_cents"]))
            proceeds = target_quantity * cents_price(exit_price_cents)
            stop.update({
                "state": "CLOSED",
                "trigger_timestamp": trigger_timestamp,
                "trigger_exchange_epoch": trigger_epoch,
                "trigger_bid_cents": trigger_bid,
                "trigger_quote_id": trigger_quote.get("quote_id"),
                "entry_quantity_at_trigger": format(target_quantity, "f"),
                "direct_requested_quantity": format(target_quantity, "f"),
                "direct_filled_quantity": format(target_quantity, "f"),
                "direct_fill_price_cents": exit_price_cents,
                "direct_fill_timestamp": trigger_timestamp,
                "hard_filled_quantity": format(target_quantity, "f"),
                "hard_fill_price_cents": exit_price_cents,
                "hard_fill_timestamp": trigger_timestamp,
                "closed_quantity": format(target_quantity, "f"),
                "exit_classification": "DIRECT_PROTECTIVE_EXIT",
                "gross_pnl": format(proceeds - entry_cost, "f"),
                "closed_at": trigger_timestamp,
                "execution_assumption": (
                    "analytics_only_full_exit_at_observed_executable_bid; live_and_primary_shadow_"
                    "paths_use_authoritative_or_displayed-depth_residual_reconciliation"
                ),
            })
            order["fill_ceiling_quantity"] = format(target_quantity, "f")
            if target_quantity < Decimal(str(order.get("requested_quantity") or "0")):
                order["status"] = "CANCELLED_REMAINDER_FOR_STOP"
            return True
        if stop.get("state") == "ARMED":
            trigger_row = next(
                (row for row in quote_rows if row[1] <= int(stop["trigger_cents"])), None,
            )
            if trigger_row is None:
                return False
            trigger_epoch, trigger_bid, trigger_quote = trigger_row
            trigger_timestamp = datetime.fromtimestamp(trigger_epoch, timezone.utc).isoformat()
            stop.update({
                "state": "MAKER_EXIT_PENDING",
                "trigger_timestamp": trigger_timestamp,
                "trigger_exchange_epoch": trigger_epoch,
                "trigger_bid_cents": trigger_bid,
                "trigger_quote_id": trigger_quote.get("quote_id"),
                "entry_quantity_at_trigger": format(filled, "f"),
                "maker_requested_quantity": format(filled, "f"),
                "maker_submitted_at": trigger_timestamp,
            })
            order["fill_ceiling_quantity"] = format(filled, "f")
            if filled < Decimal(str(order.get("requested_quantity") or "0")):
                order["status"] = "CANCELLED_REMAINDER_FOR_STOP"
            changed = True

        trigger_epoch = float(stop["trigger_exchange_epoch"])
        target_quantity = Decimal(str(stop.get("entry_quantity_at_trigger") or filled))
        maker_filled = Decimal(str(stop.get("maker_filled_quantity") or "0"))
        hard_row = next(
            (
                row for row in quote_rows
                if row[0] >= trigger_epoch and row[1] <= int(stop["hard_stop_cents"])
            ),
            None,
        )
        hard_epoch = hard_row[0] if hard_row is not None else None

        try:
            events = feed.public_trades_after(
                record["ticker"], datetime.fromtimestamp(trigger_epoch, timezone.utc),
            )
        except (AttributeError, TypeError, ValueError):
            events = []
        feed_token = str(getattr(feed, "session_token", "") or "")
        prior_token = str(stop.get("public_trade_feed_session_token") or "")
        last_trade_id = str(stop.get("last_processed_public_trade_id") or "")
        processed_keys = list(stop.get("processed_public_trade_keys") or [])
        processed_key_set = set(processed_keys)
        start_index = 0
        if last_trade_id and not processed_keys:
            for index, event in enumerate(events):
                if str(event.get("trade_id") or "") == last_trade_id:
                    start_index = index + 1
                    break
            else:
                if events:
                    stop["public_trade_cursor_gap_count"] = int(
                        stop.get("public_trade_cursor_gap_count", 0)
                    ) + 1
        if prior_token and feed_token and prior_token != feed_token:
            stop["public_trade_session_change_count"] = int(
                stop.get("public_trade_session_change_count", 0)
            ) + 1
        stop["public_trade_feed_session_token"] = feed_token
        side = str(record["signal_side"])
        maker_first = stop.get("maker_first_fill_timestamp")
        maker_last = stop.get("maker_last_fill_timestamp")
        for event in events[start_index:]:
            event_timestamp = event.get("source_server_timestamp") or event.get("received_at")
            event_epoch = _iso_epoch(event_timestamp)
            if event_epoch is None or event_epoch <= trigger_epoch:
                continue
            if hard_epoch is not None and event_epoch >= hard_epoch:
                break
            trade_id = str(event.get("trade_id") or "")
            event_key = trade_id or "|".join((
                str(event_timestamp or ""),
                str(event.get(f"{side}_price") or ""),
                str(event.get("count") or ""),
            ))
            if event_key in processed_key_set:
                continue
            if event_key:
                append_unique(processed_keys, event_key, maximum=4096)
                processed_key_set.add(event_key)
            if trade_id:
                stop["last_processed_public_trade_id"] = trade_id
            stop["public_trade_events_processed"] = int(
                stop.get("public_trade_events_processed", 0)
            ) + 1
            try:
                event_cents = price_to_cents(
                    str(event[f"{side}_price"]), "delayed maker-exit public trade price",
                )
                available = Decimal(str(event.get("count") or "0"))
            except (KeyError, ArithmeticError, TypeError, ValueError):
                continue
            if event_cents < int(stop["maker_exit_cents"]) or available <= 0:
                continue
            remaining = target_quantity - maker_filled
            if remaining <= 0:
                break
            allocated = min(remaining, available)
            maker_filled += allocated
            maker_first = maker_first or event_timestamp
            maker_last = event_timestamp
            changed = True
        stop["processed_public_trade_keys"] = processed_keys
        stop.update({
            "maker_filled_quantity": format(maker_filled, "f"),
            "maker_first_fill_timestamp": maker_first,
            "maker_last_fill_timestamp": maker_last,
        })

        entry_cost = target_quantity * cents_price(int(order["price_cents"]))
        maker_proceeds = maker_filled * cents_price(int(stop["maker_exit_cents"]))
        if maker_filled >= target_quantity:
            stop.update({
                "state": "CLOSED",
                "maker_final_fill_timestamp": maker_last,
                "closed_quantity": format(target_quantity, "f"),
                "exit_classification": "MAKER_EXIT_FULL",
                "gross_pnl": format(maker_proceeds - entry_cost, "f"),
                "closed_at": maker_last,
            })
            changed = True
        elif hard_row is not None:
            hard_epoch, hard_bid_cents, hard_quote = hard_row
            remaining = max(Decimal("0"), target_quantity - maker_filled)
            hard_price_cents = min(int(stop["hard_stop_cents"]), hard_bid_cents)
            hard_proceeds = remaining * cents_price(hard_price_cents)
            hard_timestamp = datetime.fromtimestamp(hard_epoch, timezone.utc).isoformat()
            stop.update({
                "state": "CLOSED",
                "hard_filled_quantity": format(remaining, "f"),
                "hard_fill_price_cents": hard_price_cents,
                "hard_fill_timestamp": hard_timestamp,
                "hard_quote_id": hard_quote.get("quote_id"),
                "closed_quantity": format(target_quantity, "f"),
                "exit_classification": (
                    "MAKER_EXIT_PARTIAL_THEN_HARD_STOP"
                    if maker_filled > 0 else "HARD_STOP_ONLY"
                ),
                "gross_pnl": format(maker_proceeds + hard_proceeds - entry_cost, "f"),
                "closed_at": hard_timestamp,
            })
            changed = True
        return changed

    def observe_delayed_entry_ladder(
        self, feed: KalshiLiveFeed, record: dict[str, Any], delayed: dict[str, Any], now: float,
    ) -> bool:
        """Track the direct limit and 50/40/30/20/10 ladder after a >=53c trigger.

        The activation quote freezes the trigger ask and lower limit. Quote
        touches and public-trade evidence are then collected through market
        close. The object is analytics-only in both shadow and live workers.
        """

        lane = delayed.get("ladder")
        if not isinstance(lane, dict):
            return False
        getter = getattr(feed, "selected_price_quotes_between", None)
        if not callable(getter):
            return False
        trigger_epoch = float(lane["trigger_exchange_epoch"])
        closed = float(record["market_close_epoch"])
        quotes = getter(
            record["ticker"], str(record["signal_side"]), trigger_epoch, min(now, closed),
        )
        changed = False
        feed_token = str(getattr(feed, "session_token", "") or "")
        prior_token = str(lane.get("quote_feed_session_token") or "")
        if prior_token and feed_token and prior_token != feed_token and now < closed:
            lane["coverage_complete"] = False
            lane["coverage_note"] = "worker_or_websocket_session_changed_after_delayed_entry"
            lane["quote_continuity_gap_count"] = int(lane.get("quote_continuity_gap_count", 0)) + 1
            lane["last_processed_quote_id"] = None
            changed = True
        if feed_token:
            lane["quote_feed_session_token"] = feed_token

        last_id = str(lane.get("last_processed_quote_id") or "")
        start_index = 0
        if last_id:
            for index, item in enumerate(quotes):
                if str(item.get("quote_id") or "") == last_id:
                    start_index = index + 1
                    break
            else:
                if quotes:
                    lane["coverage_complete"] = False
                    lane["coverage_note"] = "prior_delayed_ladder_quote_not_present_after_feed_restart"
                    lane["quote_continuity_gap_count"] = int(
                        lane.get("quote_continuity_gap_count", 0)
                    ) + 1
                    changed = True

        newly_touched: list[int] = []
        direct_limit_touched = False
        for quote in quotes[start_index:]:
            quote_id = str(quote.get("quote_id") or "")
            try:
                quote_epoch = float(quote["selected_component_epoch"])
                ask_cents = price_to_cents(
                    str(quote["economic_price"]), "delayed ladder selected-side ask",
                )
            except (KeyError, TypeError, ValueError):
                continue
            if quote_epoch < trigger_epoch:
                continue
            lane["price_events_processed"] = int(lane.get("price_events_processed", 0)) + 1
            direct_order = lane.get("direct_entry_order")
            if isinstance(direct_order, dict):
                direct_limit = int(direct_order["price_cents"])
                if ask_cents <= direct_limit and not direct_order.get("touched"):
                    direct_order.update({
                        "touched": True,
                        "touch_timestamp": datetime.fromtimestamp(quote_epoch, timezone.utc).isoformat(),
                        "touch_quote_id": quote_id or None,
                        "touch_observed_ask_cents": ask_cents,
                        "touch_evidence": "price_only_executable_selected_side_ask",
                    })
                    direct_limit_touched = True
                    changed = True
            for level_text, rung in lane.get("rungs", {}).items():
                level = int(level_text)
                if ask_cents <= level and not rung.get("touched"):
                    rung.update({
                        "touched": True,
                        "touch_timestamp": datetime.fromtimestamp(quote_epoch, timezone.utc).isoformat(),
                        "touch_quote_id": quote_id or None,
                        "touch_observed_ask_cents": ask_cents,
                        "touch_evidence": "price_only_executable_selected_side_ask",
                    })
                    newly_touched.append(level)
                    changed = True
            lane["last_processed_quote_id"] = quote_id or lane.get("last_processed_quote_id")
        fills_changed = self.advance_shadow_ladder_fills(feed, record, lane)
        changed = fills_changed or changed
        direct_sync_changed = self.sync_delayed_direct_entry(record, delayed)
        changed = direct_sync_changed or changed
        stop_changed = self.advance_delayed_direct_hybrid_stop(
            feed, record, delayed, min(now, closed),
        )
        changed = stop_changed or changed
        if changed:
            self.audit(
                "delayed_entry_ladder_updated",
                ticker=record["ticker"], side=record["signal_side"], analytics_only=True,
                ladder_contract_version=DELAYED_ENTRY_LADDER_CONTRACT_VERSION,
                delayed_entry_price_cents=delayed.get("first_entry_price_cents"),
                delayed_entry_limit_cents=delayed.get("entry_limit_cents"),
                direct_limit_touched=direct_limit_touched,
                newly_touched_rungs=sorted(set(newly_touched), reverse=True),
                fills_changed=fills_changed or direct_sync_changed,
                hybrid_stop_changed=stop_changed,
                direct_hybrid_stop=lane.get("direct_hybrid_stop"),
                coverage_complete=lane.get("coverage_complete"),
                coverage_note=lane.get("coverage_note"),
                rungs=lane.get("rungs"),
            )
            if newly_touched or direct_limit_touched or fills_changed or direct_sync_changed or stop_changed:
                stop = lane.get("direct_hybrid_stop", {})
                LOG.warning(
                    "DELAYED ENTRY/LADDER | ticker=%s side=%s trigger_ask=%sc limit=%sc "
                    "limit_touch=%s fill=%s filled_qty=%s rung_touches=%s "
                    "fills_changed=%s stop=%s stop_exit=%s maker_exit_qty=%s hard_exit_qty=%s "
                    "coverage=%s analytics_only=true",
                    record["ticker"], str(record["signal_side"]).upper(),
                    delayed.get("threshold_observed_ask_cents"), delayed.get("entry_limit_cents"),
                    bool(lane.get("direct_entry_order", {}).get("touched")),
                    delayed.get("hypothetical_fill"), delayed.get("hypothetical_filled_quantity"),
                    sorted(set(newly_touched), reverse=True),
                    fills_changed or direct_sync_changed, stop.get("state"),
                    stop.get("exit_classification"), stop.get("maker_filled_quantity"),
                    stop.get("hard_filled_quantity"), lane.get("coverage_note"),
                )
        return changed

    def observe_price_analytics(self, feed: KalshiLiveFeed, record: dict[str, Any]) -> bool:
        """Update durable per-signal price-path facts from one fresh book.

        Touches use the selected-side executable ask.  The separate simulated
        fill flag requires a post-signal public trade at or through that limit;
        a quote touch alone is never called a fill.
        """

        observed_now = time.time()
        ladder_changed = self.observe_opening_cross_ladder(feed, record, observed_now)
        existing_delayed = record.get("delayed_entry_tracking")
        delayed_ladder_changed = bool(
            isinstance(existing_delayed, dict)
            and existing_delayed.get("threshold_reached")
            and self.observe_delayed_entry_ladder(feed, record, existing_delayed, observed_now)
        )
        quote, _ = self.selected_book(feed, record)
        if quote is None:
            if ladder_changed or delayed_ladder_changed:
                self.checkpoint("price_analytics_updated")
            return ladder_changed or delayed_ladder_changed
        quote_id = str(quote.get("quote_id") or "")
        quote_epoch = executable_quote_epoch(quote)
        observation_floor = float(
            record.get("initial_signal_price_epoch")
            or record.get("market_open_epoch")
            or 0
        )
        if quote_epoch is None or quote_epoch < observation_floor:
            return False
        if not isinstance(record.get("opening_price_reference"), dict):
            self.capture_opening_price_reference(
                feed, record, max(float(record.get("market_open_epoch") or 0), quote_epoch),
            )
        last_epoch = float(record.get("last_analytics_quote_epoch") or 0)
        if quote_epoch < last_epoch:
            return False
        try:
            selected_ask_cents = price_to_cents(str(quote["economic_price"]), "selected-side analytics ask")
        except (KeyError, ValueError):
            return False
        changed = ladder_changed or delayed_ladder_changed
        duplicate_quote = bool(quote_id and quote_id == record.get("last_analytics_quote_id"))
        delayed = self.ensure_delayed_entry_tracking(
            record, quote_epoch, selected_ask_cents, quote_id or None,
        )
        delayed_entry_recorded = False
        if (
            isinstance(delayed, dict)
            and delayed.get("enabled")
            and delayed.get("eligible")
            and delayed.get("status") == "WATCHING"
            and quote_epoch < float(record.get("market_close_epoch") or float("inf"))
            and quote_epoch - float(record.get("market_open_epoch") or 0)
            >= int(self.config["delayed_entry_start_seconds"])
            and selected_ask_cents >= int(delayed["threshold_cents"])
        ):
            try:
                displayed_depth = Decimal(str(quote.get("displayed_depth") or "0"))
                intended_quantity = Decimal(str(record.get("intended_quantity") or "0"))
            except (ArithmeticError, TypeError, ValueError):
                displayed_depth = Decimal("0")
                intended_quantity = Decimal("0")
            # A fresh price-only top-of-book update is sufficient to freeze a
            # resting limit. Displayed depth is optional trigger metadata and
            # is never treated as fill evidence; only later public trades can
            # advance the simulated filled quantity.
            open_epoch = float(record["market_open_epoch"])
            signal_epoch = _iso_epoch(record.get("signal_timestamp"))
            elapsed_open = round(max(0.0, quote_epoch - open_epoch), 6)
            entry_limit_offset = int(self.config["entry_limit_offset_cents"])
            entry_limit_cents = selected_ask_cents - entry_limit_offset
            if not 1 <= entry_limit_cents <= 99:
                delayed.update({
                    "status": "INVALID_LIMIT_PRICE",
                    "threshold_reached": True,
                    "threshold_observed_ask_cents": selected_ask_cents,
                    "entry_limit_cents": entry_limit_cents,
                })
                delayed_entry_recorded = True
                changed = True
                entry_limit_cents = 0
            trigger_timestamp = (
                quote.get("source_server_timestamp")
                or datetime.fromtimestamp(quote_epoch, timezone.utc).isoformat()
            )
            if entry_limit_cents:
                delayed.update({
                    "status": "LIMIT_PENDING",
                    "threshold_reached": True,
                    "threshold_observed_ask_cents": selected_ask_cents,
                    "threshold_timestamp": trigger_timestamp,
                    "threshold_exchange_epoch": quote_epoch,
                    "threshold_exchange_timestamp": trigger_timestamp,
                    "threshold_quote_id": quote_id or None,
                    "threshold_after_open_seconds": elapsed_open,
                    "threshold_after_signal_seconds": _seconds_since(signal_epoch, quote_epoch),
                    "threshold_after_opening_capture_window": elapsed_open >= int(self.config["opening_quote_capture_seconds"]),
                    "hypothetical_fill": False,
                    "hypothetical_filled_quantity": "0.00",
                    "entry_limit_offset_cents": entry_limit_offset,
                    "entry_limit_cents": entry_limit_cents,
                    "entry_limit_price": format(cents_price(entry_limit_cents), "f"),
                    "first_entry_price_cents": None,
                    "first_entry_price": None,
                    "first_entry_timestamp": None,
                    "first_entry_exchange_epoch": None,
                    "first_entry_exchange_timestamp": None,
                    "first_entry_quote_id": None,
                    "first_entry_after_open_seconds": None,
                    "first_entry_after_signal_seconds": None,
                    "first_entry_after_opening_capture_window": None,
                    "displayed_ask_depth": format(displayed_depth, "f"),
                    "configured_quantity_fully_fillable": False,
                    "execution_assumption": (
                        "resting_limit_one_cent_below_frozen_threshold_ask; quote_touch_is_not_fill; "
                        "post_trigger_public_trade_volume_at_or_below_limit_required"
                    ),
                    "minimum_selected_ask_after_entry_cents": selected_ask_cents,
                    "maximum_selected_ask_after_entry_cents": selected_ask_cents,
                    "ladder_contract_version": DELAYED_ENTRY_LADDER_CONTRACT_VERSION,
                    "ladder": {
                        "contract_version": DELAYED_ENTRY_LADDER_CONTRACT_VERSION,
                        "analytics_only": True,
                        "execution_mode": "shadow_only_no_exchange_orders",
                        **{
                            field_name: record.get(field_name)
                            for field_name in BTC_TARGET_RECORD_FIELDS
                        },
                        "trigger_cents": int(delayed["threshold_cents"]),
                        "trigger_observed_ask_cents": selected_ask_cents,
                        "trigger_quote_id": quote_id or None,
                        "trigger_exchange_epoch": quote_epoch,
                        "trigger_exchange_timestamp": quote.get("source_server_timestamp"),
                        "trigger_after_open_seconds": elapsed_open,
                        "coverage_complete": bool(delayed.get("coverage_complete_from_initial_quote")),
                        "coverage_note": (
                            "complete_from_initial_quote"
                            if delayed.get("coverage_complete_from_initial_quote")
                            else "partial_legacy_or_restart_coverage"
                        ),
                        "quote_feed_session_token": None,
                        "last_processed_quote_id": None,
                        "price_events_processed": 0,
                        "public_trade_cursor_version": 1,
                        "public_trade_feed_session_token": None,
                        "last_processed_public_trade_id": None,
                        "public_trade_events_processed": 0,
                        "processed_public_trade_keys": [],
                        "direct_entry_order": {
                            "analytics_only": True,
                            "order_type": "resting_limit",
                            "time_in_force": "through_market_close",
                            "price_cents": entry_limit_cents,
                            "requested_quantity": format(intended_quantity, "f"),
                            "fill_ceiling_quantity": None,
                            "touched": False,
                            "touch_timestamp": None,
                            "simulated_filled_quantity": "0.00",
                            "simulated_full_fill": False,
                            "first_fill_timestamp": None,
                            "final_fill_timestamp": None,
                            "status": "RESTING",
                            "fill_assumption": (
                                "post_trigger_public_trade_volume_at_or_below_limit_allocated_once_"
                                "without_queue_priority"
                            ),
                        },
                        "direct_hybrid_stop": {
                            "enabled": True,
                            "policy": "direct_ioc_at_trigger",
                            "trigger_cents": DELAYED_HYBRID_STOP_TRIGGER_CENTS,
                            "maker_exit_cents": DELAYED_HYBRID_MAKER_EXIT_CENTS,
                            "hard_stop_cents": DELAYED_HYBRID_HARD_STOP_CENTS,
                            "state": "ARMED",
                            "trigger_timestamp": None,
                            "trigger_exchange_epoch": None,
                            "entry_quantity_at_trigger": "0.00",
                            "maker_requested_quantity": "0.00",
                            "maker_filled_quantity": "0.00",
                            "maker_first_fill_timestamp": None,
                            "maker_final_fill_timestamp": None,
                            "hard_filled_quantity": "0.00",
                            "hard_fill_price_cents": None,
                            "hard_fill_timestamp": None,
                            "closed_quantity": "0.00",
                            "exit_classification": None,
                            "gross_pnl": None,
                            "public_trade_feed_session_token": None,
                            "last_processed_public_trade_id": None,
                            "public_trade_events_processed": 0,
                            "processed_public_trade_keys": [],
                        },
                        "rungs": {
                            str(level): {
                                "price_cents": level,
                                "requested_quantity": format(quantity, "f"),
                                "touched": False,
                                "touch_timestamp": None,
                                "simulated_filled_quantity": "0.00",
                                "simulated_full_fill": False,
                                "first_fill_timestamp": None,
                                "final_fill_timestamp": None,
                            }
                            for level, quantity in OPENING_CROSS_LADDER_RUNGS
                        },
                        "settlement_outcome": None,
                        "directional_winner": None,
                        "gross_no_stop_pnl": None,
                    },
                })
                delayed_entry_recorded = True
                changed = True
        elif isinstance(delayed, dict) and delayed.get("threshold_reached"):
            prior_min = delayed.get("minimum_selected_ask_after_entry_cents")
            prior_max = delayed.get("maximum_selected_ask_after_entry_cents")
            if prior_min is None or selected_ask_cents < int(prior_min):
                delayed["minimum_selected_ask_after_entry_cents"] = selected_ask_cents
                changed = True
            if prior_max is None or selected_ask_cents > int(prior_max):
                delayed["maximum_selected_ask_after_entry_cents"] = selected_ask_cents
                changed = True
        if isinstance(delayed, dict) and delayed.get("threshold_reached"):
            changed = self.observe_delayed_entry_ladder(
                feed, record, delayed, quote_epoch,
            ) or changed
        if not duplicate_quote:
            record["last_analytics_quote_id"] = quote_id or None
            record["last_analytics_quote_epoch"] = quote_epoch
            previous_min = record.get("minimum_selected_price_cents")
            if previous_min is None or selected_ask_cents < int(previous_min):
                record["minimum_selected_price_cents"] = selected_ask_cents
                record["minimum_selected_price_timestamp"] = utc_now()
                changed = True
            for text, level_state in record.get("shadow_entry_levels", {}).items():
                level = int(text)
                if selected_ask_cents <= level and not level_state.get("touched"):
                    level_state.update({
                        "touched": True, "touch_timestamp": utc_now(),
                        "touch_quote_id": quote_id or None, "touch_price_cents": selected_ask_cents,
                    })
                    changed = True

        if delayed_entry_recorded:
            self.audit(
                "delayed_entry_threshold_reached", ticker=record["ticker"], side=record["signal_side"],
                initial_signal_price_cents=delayed.get("initial_signal_price_cents"),
                threshold_cents=delayed.get("threshold_cents"),
                threshold_observed_ask_cents=delayed.get("threshold_observed_ask_cents"),
                entry_limit_offset_cents=delayed.get("entry_limit_offset_cents"),
                entry_limit_cents=delayed.get("entry_limit_cents"),
                threshold_after_open_seconds=delayed.get("threshold_after_open_seconds"),
                after_opening_capture_window=delayed.get("threshold_after_opening_capture_window"),
                displayed_ask_depth=delayed.get("displayed_ask_depth"),
                hypothetical_fill=False,
                coverage_complete_from_initial_quote=delayed.get("coverage_complete_from_initial_quote"),
            )
            LOG.warning(
                "DELAYED >=53C LIMIT RESTING | ticker=%s side=%s initial=%sc trigger_ask=%sc "
                "limit=%sc offset=%sc trigger_after_open=%.3fs after_60s=%s depth=%s "
                "filled=false tif=through_market_close analytics_only=true",
                record["ticker"], str(record["signal_side"]).upper(),
                delayed.get("initial_signal_price_cents"), delayed.get("threshold_observed_ask_cents"),
                delayed.get("entry_limit_cents"), delayed.get("entry_limit_offset_cents"),
                float(delayed.get("threshold_after_open_seconds") or 0),
                delayed.get("threshold_after_opening_capture_window"), delayed.get("displayed_ask_depth"),
            )

        try:
            signal_time = datetime.fromisoformat(str(record["signal_timestamp"]).replace("Z", "+00:00"))
            events = feed.public_trades_after(record["ticker"], signal_time)
        except (AttributeError, KeyError, TypeError, ValueError):
            events = []
        side = str(record["signal_side"])
        for text, level_state in record.get("shadow_entry_levels", {}).items():
            if level_state.get("simulated_fill"):
                continue
            level = int(text)
            evidence_quantity = Decimal("0")
            first_event: dict[str, Any] | None = None
            for event in events:
                try:
                    event_cents = price_to_cents(str(event[f"{side}_price"]), "public trade price")
                    count = Decimal(str(event.get("count") or "0"))
                except (KeyError, ValueError, ArithmeticError):
                    continue
                if event_cents <= level and count > 0:
                    evidence_quantity += count
                    if first_event is None:
                        first_event = event
            if evidence_quantity > 0:
                level_state.update({
                    "simulated_fill": True,
                    "simulated_fill_timestamp": (first_event or {}).get("source_server_timestamp") or utc_now(),
                    "simulated_fill_evidence_quantity": format(evidence_quantity, "f"),
                    "simulated_fill_trade_id": (first_event or {}).get("trade_id"),
                    "shadow_fill_assumption": "post_signal_public_trade_at_or_below_buy_limit_no_queue_priority_claim",
                })
                changed = True
        if changed:
            self.checkpoint("price_analytics_updated")
        return changed

    def entry_price_performance(self) -> dict[str, Any]:
        """Rebuild idempotent entry, winner, and initial-stop metrics.

        ``initial_at_or_below_stop`` is a counterfactual eligibility measure,
        not a claim that ten strategies ran. The separate actual rejection
        counters only include records which this configured strategy refused
        before order submission. Neither category is a directional skip.
        """

        levels = range(
            int(self.config["shadow_entry_level_min_cents"]),
            int(self.config["shadow_entry_level_max_cents"]) + 1,
            int(self.config["shadow_entry_level_step_cents"]),
        )
        output: dict[str, Any] = {}
        winners = 0
        histogram: dict[str, int] = {}
        minimum_histogram: dict[str, int] = {}
        winners_at_or_below_40 = 0
        actual_filled_eventual_winners = 0
        realized_profitable_filled_trades = 0
        lowest_actual_entry_eventual_winner: dict[str, Any] | None = None
        lowest_actual_entry_realized_profitable: dict[str, Any] | None = None
        initial_prices: list[int] = []
        actual_stop_rejections: list[int] = []
        rejection_reasons: dict[str, int] = {}
        signals_without_initial_price = 0
        for record in self.state.get("markets", {}).values():
            if not isinstance(record, dict) or not record.get("signal_side"):
                continue
            rejection = record.get("entry_rejection_quote", {})
            initial_value = record.get("initial_signal_price_cents")
            # Early v11 rejection records retained this exact price in their
            # quote evidence even before the top-level field was added.
            if initial_value is None and isinstance(rejection, dict):
                initial_value = rejection.get("initial_signal_price_cents")
            if initial_value is None:
                signals_without_initial_price += 1
                continue
            initial_price = int(initial_value)
            initial_prices.append(initial_price)
            rejection_reason = str(rejection.get("reason") or "") if isinstance(rejection, dict) else ""
            if rejection_reason:
                rejection_reasons[rejection_reason] = rejection_reasons.get(rejection_reason, 0) + 1
            if rejection_reason == "derived_limit_at_or_below_hybrid_hard_stop":
                actual_stop_rejections.append(initial_price)
            outcome = record.get("settlement_outcome") or record.get("post_stop_settlement_outcome")
            is_winner = outcome in {"yes", "no"} and outcome == record.get("signal_side")
            if is_winner:
                winners += 1
                initial = initial_price
                minimum = int(record.get("minimum_selected_price_cents") or initial)
                drawdown = max(0, initial - minimum)
                record["winner_max_drawdown_cents"] = drawdown
                histogram[str(drawdown)] = histogram.get(str(drawdown), 0) + 1
                minimum_histogram[str(minimum)] = minimum_histogram.get(str(minimum), 0) + 1
                if minimum <= 40:
                    winners_at_or_below_40 += 1
                try:
                    actual_quantity = Decimal(str(record.get("actual_quantity") or "0"))
                    actual_entry = Decimal(str(record.get("actual_average_entry_price")))
                except (ArithmeticError, TypeError, ValueError):
                    actual_quantity = Decimal("0")
                    actual_entry = Decimal("0")
                if actual_quantity > 0 and Decimal("0") < actual_entry < Decimal("1"):
                    actual_filled_eventual_winners += 1
                    candidate = {
                        "ticker": record.get("ticker"),
                        "signal_side": record.get("signal_side"),
                        "actual_average_entry_price": format(actual_entry, "f"),
                        "actual_quantity": format(actual_quantity, "f"),
                        "settlement_outcome": outcome,
                        "realized_method": record.get("realized_method"),
                        "realized_net_pnl": record.get("realized_net_pnl"),
                    }
                    current = lowest_actual_entry_eventual_winner
                    if current is None or actual_entry < Decimal(str(current["actual_average_entry_price"])):
                        lowest_actual_entry_eventual_winner = candidate
                    try:
                        realized_net_pnl = Decimal(str(record.get("realized_net_pnl") or "0"))
                    except (ArithmeticError, TypeError, ValueError):
                        realized_net_pnl = Decimal("0")
                    if realized_net_pnl > 0:
                        realized_profitable_filled_trades += 1
                        profitable = lowest_actual_entry_realized_profitable
                        if profitable is None or actual_entry < Decimal(str(profitable["actual_average_entry_price"])):
                            lowest_actual_entry_realized_profitable = candidate
            for level in levels:
                state = record.get("shadow_entry_levels", {}).get(str(level), {})
                row = output.setdefault(str(level), {
                    "price_cents": level, "eligible_signals": 0, "touched": 0,
                    "simulated_fills": 0, "eventual_winners": 0,
                    "winner_touches": 0, "winner_simulated_fills": 0,
                })
                row["eligible_signals"] += 1
                row["touched"] += int(bool(state.get("touched")))
                row["simulated_fills"] += int(bool(state.get("simulated_fill")))
                if is_winner:
                    row["eventual_winners"] += 1
                    row["winner_touches"] += int(bool(state.get("touched")))
                    row["winner_simulated_fills"] += int(bool(state.get("simulated_fill")))
        for row in output.values():
            eligible = row["eligible_signals"]
            winner_count = row["eventual_winners"]
            row["hit_rate"] = row["touched"] / eligible if eligible else None
            row["simulated_fill_rate"] = row["simulated_fills"] / eligible if eligible else None
            row["winner_capture_rate"] = row["winner_touches"] / winner_count if winner_count else None
            row["winner_simulated_fill_rate"] = row["winner_simulated_fills"] / winner_count if winner_count else None
            row["missed_winner_count"] = winner_count - row["winner_touches"]
            row["missed_winner_rate"] = row["missed_winner_count"] / winner_count if winner_count else None
        captured = len(initial_prices)
        stop_levels: dict[str, Any] = {}
        for stop_cents in levels:
            count = sum(price <= stop_cents for price in initial_prices)
            stop_levels[str(stop_cents)] = {
                "stop_cents": stop_cents,
                "signals_with_initial_price": captured,
                "initial_at_or_below_stop": count,
                "would_skip_rate": count / captured if captured else None,
            }
        exact_initial = {
            str(price): sum(value == price for value in initial_prices)
            for price in levels
        }
        exact_actual_rejections = {
            str(price): sum(value == price for value in actual_stop_rejections)
            for price in levels
        }
        summary = {
            "levels": output, "winning_settlements": winners,
            "winning_settlements_reached_40_or_lower": winners_at_or_below_40,
            "winning_settlements_stayed_above_40": winners - winners_at_or_below_40,
            "winner_drawdown_histogram_cents": dict(sorted(histogram.items(), key=lambda item: int(item[0]))),
            "winner_minimum_price_histogram_cents": dict(sorted(minimum_histogram.items(), key=lambda item: int(item[0]))),
            "actual_filled_eventual_winners": actual_filled_eventual_winners,
            "lowest_actual_entry_eventual_winner": lowest_actual_entry_eventual_winner,
            "realized_profitable_filled_trades": realized_profitable_filled_trades,
            "lowest_actual_entry_realized_profitable": lowest_actual_entry_realized_profitable,
            "initial_stop_eligibility": {
                "definition": "initial_selected_side_price_cents <= hypothetical_stop_cents",
                "captured_initial_prices": captured,
                "signals_without_initial_price": signals_without_initial_price,
                "levels": stop_levels,
                "exact_initial_price_counts_40_49": exact_initial,
                "initial_prices_below_40": sum(value < 40 for value in initial_prices),
                "initial_prices_above_49": sum(value > 49 for value in initial_prices),
                "actual_strategy_stop_safety_rejections": len(actual_stop_rejections),
                "actual_strategy_stop_safety_rejections_40_49": sum(
                    40 <= value <= 49 for value in actual_stop_rejections
                ),
                "actual_strategy_stop_safety_rejections_below_40": sum(
                    value < 40 for value in actual_stop_rejections
                ),
                "actual_strategy_stop_safety_rejections_above_49": sum(
                    value > 49 for value in actual_stop_rejections
                ),
                "actual_rejection_initial_price_counts_40_49": exact_actual_rejections,
                "actual_entry_rejection_reasons": dict(sorted(rejection_reasons.items())),
                "actual_strategy_rule": (
                    "reject before order submission when derived entry limit is at or below "
                    f"the configured {int(self.config['hybrid_hard_stop_cents'])}c hard stop"
                ),
            },
            "updated_at": utc_now(),
        }
        self.state["entry_price_performance"] = summary
        return summary

    def delayed_entry_performance(self) -> dict[str, Any]:
        """Rebuild the full-market >=53c analytics lane idempotently."""

        threshold = int(self.config["delayed_entry_threshold_cents"])
        eligible = complete_coverage = partial_coverage = threshold_reached = 0
        before_window = after_window = fully_fillable = resolved = wins = losses = 0
        filled_entries = resolved_filled_entries = zero_fills = pending_unfilled = 0
        partial_fills = full_fills = 0
        filled_wins = filled_losses = legacy_contract_entries = 0
        hybrid_resolved = hybrid_triggers = hybrid_maker_full = 0
        hybrid_partial_hard = hybrid_hard_only = hybrid_false_stops = 0
        gross_pnl = Decimal("0")
        hybrid_gross_pnl = Decimal("0")
        timings: list[float] = []
        trigger_buckets: dict[str, int] = {}
        entry_buckets: dict[str, int] = {}
        cohort_rows: dict[str, dict[str, Any]] = {
            str(cohort_threshold): {
                "minimum_delayed_entry_cents": cohort_threshold,
                "entries": 0,
                "complete_coverage_entries": 0,
                "partial_coverage_entries": 0,
                "complete_resolved_entries": 0,
                "directional_wins": 0,
                "directional_losses": 0,
                "direct_filled_entries": 0,
                "direct_zero_fills": 0,
                "direct_partial_fills": 0,
                "direct_full_fills": 0,
                "direct_filled_wins": 0,
                "direct_filled_losses": 0,
                "legacy_contract_entries": 0,
                "gross_no_stop_direct_entry_pnl_per_share": Decimal("0"),
                "gross_no_stop_direct_entry_pnl": Decimal("0"),
                "hybrid_resolved_entries": 0,
                "hybrid_stop_triggers": 0,
                "hybrid_maker_exit_full": 0,
                "hybrid_partial_then_hard": 0,
                "hybrid_hard_only": 0,
                "hybrid_false_stops": 0,
                "gross_hybrid_pnl": Decimal("0"),
                "ladder_tracked_entries": 0,
                "legacy_entries_without_ladder": 0,
                "ladder_resolved_entries": 0,
                "gross_no_stop_ladder_pnl": Decimal("0"),
                "rungs": {
                    str(level): {
                        "price_cents": level,
                        "requested_quantity": format(quantity, "f"),
                        "touches": 0,
                        "full_fills": 0,
                        "partial_fills": 0,
                        "simulated_filled_quantity": Decimal("0"),
                        "gross_no_stop_pnl": Decimal("0"),
                    }
                    for level, quantity in OPENING_CROSS_LADDER_RUNGS
                },
            }
            for cohort_threshold in OPENING_CROSS_LADDER_TRIGGER_LEVELS
        }
        for record in self.state.get("markets", {}).values():
            if not isinstance(record, dict):
                continue
            tracker = record.get("delayed_entry_tracking")
            if not isinstance(tracker, dict) or not tracker.get("eligible"):
                continue
            eligible += 1
            if tracker.get("coverage_complete_from_initial_quote"):
                complete_coverage += 1
            else:
                partial_coverage += 1
            if not tracker.get("threshold_reached"):
                continue
            threshold_reached += 1
            after = bool(
                tracker.get("threshold_after_opening_capture_window")
                if tracker.get("threshold_after_opening_capture_window") is not None
                else tracker.get("first_entry_after_opening_capture_window")
            )
            after_window += int(after)
            before_window += int(not after)
            fully_fillable += int(bool(tracker.get("configured_quantity_fully_fillable")))
            timing = (
                tracker.get("threshold_after_open_seconds")
                if tracker.get("threshold_after_open_seconds") is not None
                else tracker.get("first_entry_after_open_seconds")
            )
            if timing is not None:
                timings.append(float(timing))
            contract_version = int(tracker.get("ladder_contract_version") or 1)
            trigger_cents = tracker.get("threshold_observed_ask_cents")
            entry_cents = tracker.get("first_entry_price_cents")
            if trigger_cents is None and contract_version < 2:
                trigger_cents = entry_cents
            if trigger_cents is not None:
                text = str(int(trigger_cents))
                trigger_buckets[text] = trigger_buckets.get(text, 0) + 1
            if entry_cents is not None:
                text = str(int(entry_cents))
                entry_buckets[text] = entry_buckets.get(text, 0) + 1
            winner = tracker.get("directional_winner")
            pnl = tracker.get("no_stop_gross_pnl_per_share")
            ladder = tracker.get("ladder")
            direct_stop = (
                ladder.get("direct_hybrid_stop") if isinstance(ladder, dict) else None
            )
            filled_quantity = Decimal(str(tracker.get("hypothetical_filled_quantity") or "0"))
            intended_quantity = Decimal(str(record.get("intended_quantity") or "0"))
            is_filled = filled_quantity > 0
            is_full = bool(is_filled and intended_quantity > 0 and filled_quantity >= intended_quantity)
            if contract_version >= 2:
                filled_entries += int(is_filled)
                resolved_filled_entries += int(winner is not None and is_filled)
                zero_fills += int(winner is not None and not is_filled)
                pending_unfilled += int(winner is None and not is_filled)
                partial_fills += int(is_filled and not is_full)
                full_fills += int(is_full)
            else:
                legacy_contract_entries += 1
            for cohort_threshold in OPENING_CROSS_LADDER_TRIGGER_LEVELS:
                if trigger_cents is None or int(trigger_cents) < cohort_threshold:
                    continue
                cohort = cohort_rows[str(cohort_threshold)]
                cohort["entries"] += 1
                complete = bool(tracker.get("coverage_complete_from_initial_quote"))
                cohort["complete_coverage_entries"] += int(complete)
                cohort["partial_coverage_entries"] += int(not complete)
                if contract_version >= 2 and isinstance(ladder, dict):
                    cohort["ladder_tracked_entries"] += 1
                    for level, requested in OPENING_CROSS_LADDER_RUNGS:
                        rung = ladder.get("rungs", {}).get(str(level), {})
                        row = cohort["rungs"][str(level)]
                        quantity = Decimal(str(rung.get("simulated_filled_quantity") or "0"))
                        row["touches"] += int(bool(rung.get("touched")))
                        row["full_fills"] += int(quantity >= requested)
                        row["partial_fills"] += int(Decimal("0") < quantity < requested)
                        row["simulated_filled_quantity"] += quantity
                else:
                    cohort["legacy_entries_without_ladder"] += 1
                if contract_version < 2:
                    cohort["legacy_contract_entries"] += 1
                # Published cohort W/L and P&L intentionally use only records
                # whose quote coverage began with the immutable opening
                # reference. Partial legacy records remain visible above but
                # cannot contaminate the complete-data result.
                if not complete or winner is None:
                    continue
                cohort["complete_resolved_entries"] += 1
                cohort["directional_wins"] += int(bool(winner))
                cohort["directional_losses"] += int(not bool(winner))
                if contract_version < 2:
                    continue
                cohort["direct_filled_entries"] += int(is_filled)
                cohort["direct_zero_fills"] += int(not is_filled)
                cohort["direct_partial_fills"] += int(is_filled and not is_full)
                cohort["direct_full_fills"] += int(is_full)
                cohort["direct_filled_wins"] += int(is_filled and bool(winner))
                cohort["direct_filled_losses"] += int(is_filled and not bool(winner))
                if is_filled and pnl is not None:
                    cohort["gross_no_stop_direct_entry_pnl_per_share"] += Decimal(str(pnl))
                    cohort["gross_no_stop_direct_entry_pnl"] += Decimal(str(
                        tracker.get("no_stop_gross_pnl") or "0"
                    ))
                if (
                    contract_version >= 3 and is_filled
                    and isinstance(direct_stop, dict)
                    and direct_stop.get("gross_pnl") is not None
                ):
                    classification = str(direct_stop.get("exit_classification") or "")
                    cohort["hybrid_resolved_entries"] += 1
                    cohort["hybrid_stop_triggers"] += int(
                        direct_stop.get("trigger_timestamp") is not None
                    )
                    cohort["hybrid_maker_exit_full"] += int(classification == "MAKER_EXIT_FULL")
                    cohort["hybrid_partial_then_hard"] += int(
                        classification == "MAKER_EXIT_PARTIAL_THEN_HARD_STOP"
                    )
                    cohort["hybrid_hard_only"] += int(
                        classification in {"HARD_STOP_ONLY", "DIRECT_PROTECTIVE_EXIT"}
                    )
                    cohort["hybrid_false_stops"] += int(bool(
                        direct_stop.get("stopped_then_eventual_winner")
                    ))
                    cohort["gross_hybrid_pnl"] += Decimal(str(direct_stop["gross_pnl"]))
                if not isinstance(ladder, dict) or ladder.get("gross_no_stop_pnl") is None:
                    continue
                cohort["ladder_resolved_entries"] += 1
                cohort["gross_no_stop_ladder_pnl"] += Decimal(str(ladder["gross_no_stop_pnl"]))
                for level, _ in OPENING_CROSS_LADDER_RUNGS:
                    rung = ladder.get("rungs", {}).get(str(level), {})
                    quantity = Decimal(str(rung.get("simulated_filled_quantity") or "0"))
                    rung_pnl = (
                        quantity * (Decimal("1") - cents_price(level))
                        if winner else -quantity * cents_price(level)
                    )
                    cohort["rungs"][str(level)]["gross_no_stop_pnl"] += rung_pnl
            if winner is None:
                continue
            resolved += 1
            wins += int(bool(winner))
            losses += int(not bool(winner))
            if contract_version >= 2 and is_filled and pnl is not None:
                filled_wins += int(bool(winner))
                filled_losses += int(not bool(winner))
                gross_pnl += Decimal(str(tracker.get("no_stop_gross_pnl") or "0"))
            if (
                contract_version >= 3 and is_filled
                and isinstance(direct_stop, dict)
                and direct_stop.get("gross_pnl") is not None
            ):
                classification = str(direct_stop.get("exit_classification") or "")
                hybrid_resolved += 1
                hybrid_triggers += int(direct_stop.get("trigger_timestamp") is not None)
                hybrid_maker_full += int(classification == "MAKER_EXIT_FULL")
                hybrid_partial_hard += int(
                    classification == "MAKER_EXIT_PARTIAL_THEN_HARD_STOP"
                )
                hybrid_hard_only += int(
                    classification in {"HARD_STOP_ONLY", "DIRECT_PROTECTIVE_EXIT"}
                )
                hybrid_false_stops += int(bool(
                    direct_stop.get("stopped_then_eventual_winner")
                ))
                hybrid_gross_pnl += Decimal(str(direct_stop["gross_pnl"]))

        timings.sort()

        def percentile(probability: float) -> float | None:
            if not timings:
                return None
            index = int(round((len(timings) - 1) * probability))
            return round(timings[index], 6)

        serialized_cohorts: dict[str, dict[str, Any]] = {}
        for cohort_threshold, cohort in cohort_rows.items():
            cohort_resolved = int(cohort["complete_resolved_entries"])
            direct_filled = int(cohort["direct_filled_entries"])
            ladder_resolved = int(cohort["ladder_resolved_entries"])
            direct_pnl = Decimal(cohort["gross_no_stop_direct_entry_pnl_per_share"])
            direct_total_pnl = Decimal(cohort["gross_no_stop_direct_entry_pnl"])
            hybrid_pnl = Decimal(cohort["gross_hybrid_pnl"])
            ladder_pnl = Decimal(cohort["gross_no_stop_ladder_pnl"])
            rung_output: dict[str, dict[str, Any]] = {}
            for level, row in cohort["rungs"].items():
                rung_output[level] = {
                    **row,
                    "simulated_filled_quantity": format(
                        Decimal(row["simulated_filled_quantity"]), "f",
                    ),
                    "gross_no_stop_pnl": format(Decimal(row["gross_no_stop_pnl"]), "f"),
                }
            serialized_cohorts[cohort_threshold] = {
                **{key: value for key, value in cohort.items() if key not in {
                    "rungs", "gross_no_stop_direct_entry_pnl_per_share",
                    "gross_no_stop_direct_entry_pnl", "gross_hybrid_pnl",
                    "gross_no_stop_ladder_pnl",
                }},
                "directional_win_rate": (
                    int(cohort["directional_wins"]) / cohort_resolved if cohort_resolved else None
                ),
                "gross_no_stop_direct_entry_pnl_per_share": format(direct_pnl, "f"),
                "gross_no_stop_direct_entry_ev": (
                    format(direct_pnl / direct_filled, "f") if direct_filled else None
                ),
                "gross_no_stop_direct_entry_pnl": format(direct_total_pnl, "f"),
                "gross_hybrid_pnl": format(hybrid_pnl, "f"),
                "gross_hybrid_ev": (
                    format(hybrid_pnl / int(cohort["hybrid_resolved_entries"]), "f")
                    if int(cohort["hybrid_resolved_entries"]) else None
                ),
                "direct_filled_win_rate": (
                    int(cohort["direct_filled_wins"]) / direct_filled if direct_filled else None
                ),
                "gross_no_stop_ladder_pnl": format(ladder_pnl, "f"),
                "gross_no_stop_ladder_ev": (
                    format(ladder_pnl / ladder_resolved, "f") if ladder_resolved else None
                ),
                "gross_no_stop_direct_plus_ladder_pnl": format(direct_total_pnl + ladder_pnl, "f"),
                "rungs": rung_output,
            }

        output = {
            "analytics_only": True,
            "entry_rule": (
                f"initial selected-side ask below {threshold}c; first later fresh executable ask "
                f"at or above {threshold}c freezes a resting limit "
                f"{int(self.config['entry_limit_offset_cents'])}c lower through market close"
            ),
            "threshold_cents": threshold,
            "eligible_below_threshold_signals": eligible,
            "complete_coverage_signals": complete_coverage,
            "partial_legacy_coverage_signals": partial_coverage,
            "threshold_reached": threshold_reached,
            "threshold_reach_rate": threshold_reached / eligible if eligible else None,
            "entries_within_opening_capture_window": before_window,
            "entries_after_opening_capture_window": after_window,
            "configured_quantity_fully_fillable": fully_fillable,
            "resolved_entries": resolved,
            "directional_wins": wins,
            "directional_losses": losses,
            "directional_win_rate": wins / resolved if resolved else None,
            "direct_limit_filled_entries": filled_entries,
            "direct_limit_resolved_filled_entries": resolved_filled_entries,
            "direct_limit_zero_fills": zero_fills,
            "direct_limit_pending_unfilled": pending_unfilled,
            "direct_limit_partial_fills": partial_fills,
            "direct_limit_full_fills": full_fills,
            "direct_limit_fill_rate": (
                resolved_filled_entries / (resolved_filled_entries + zero_fills)
                if resolved_filled_entries + zero_fills else None
            ),
            "direct_limit_filled_wins": filled_wins,
            "direct_limit_filled_losses": filled_losses,
            "direct_limit_filled_win_rate": (
                filled_wins / (filled_wins + filled_losses)
                if filled_wins + filled_losses else None
            ),
            "legacy_contract_entries": legacy_contract_entries,
            "gross_no_stop_pnl_total": format(gross_pnl, "f"),
            "gross_no_stop_ev_per_filled_entry": (
                format(gross_pnl / resolved_filled_entries, "f")
                if resolved_filled_entries else None
            ),
            "hybrid_stop_contract": {
                "policy": "direct_ioc_at_trigger",
                "trigger_cents": DELAYED_HYBRID_STOP_TRIGGER_CENTS,
                "maker_exit_cents": DELAYED_HYBRID_MAKER_EXIT_CENTS,
                "hard_stop_cents": DELAYED_HYBRID_HARD_STOP_CENTS,
            },
            "hybrid_resolved_entries": hybrid_resolved,
            "hybrid_stop_triggers": hybrid_triggers,
            "hybrid_maker_exit_full": hybrid_maker_full,
            "hybrid_partial_then_hard": hybrid_partial_hard,
            "hybrid_hard_only": hybrid_hard_only,
            "hybrid_false_stops": hybrid_false_stops,
            "gross_hybrid_pnl_total": format(hybrid_gross_pnl, "f"),
            "gross_hybrid_ev_per_filled_entry": (
                format(hybrid_gross_pnl / hybrid_resolved, "f")
                if hybrid_resolved else None
            ),
            "trigger_ask_buckets_cents": dict(sorted(
                trigger_buckets.items(), key=lambda item: int(item[0]),
            )),
            "entry_price_buckets_cents": dict(sorted(entry_buckets.items(), key=lambda item: int(item[0]))),
            "cohorts": serialized_cohorts,
            "ladder_contract_version": DELAYED_ENTRY_LADDER_CONTRACT_VERSION,
            "ladder_fill_assumption": (
                "post_trigger_public_trade_volume_allocated_once_across_the_direct_limit_and_"
                "descending_ladder_limits_without_queue_priority"
            ),
            "legacy_pre_v3_entries_are_not_backfilled_or_mixed_into_v3_direct_results": True,
            "threshold_after_open_seconds_p50": percentile(0.50),
            "threshold_after_open_seconds_p90": percentile(0.90),
            "threshold_after_open_seconds_max": max(timings) if timings else None,
            "updated_at": utc_now(),
        }
        self.state["delayed_entry_performance"] = output
        return output

    def log_delayed_entry_performance(self) -> None:
        result = self.delayed_entry_performance()
        rate = result["directional_win_rate"]
        hit_rate = result["threshold_reach_rate"]
        fill_rate = result["direct_limit_fill_rate"]
        filled_win_rate = result["direct_limit_filled_win_rate"]
        LOG.warning("================ FULL-MARKET DELAYED >=53C ANALYTICS ================")
        LOG.warning(
            "eligible_below53=%d complete_coverage=%d partial_legacy_coverage=%d reached=%d "
            "reach_rate=%s within60s=%d after60s=%d resolved_signals=%d W/L=%d/%d WR=%s "
            "limit_filled=%d resolved_filled=%d zero=%d pending=%d partial=%d full=%d "
            "resolved_fill_rate=%s filled_W/L=%d/%d "
            "filled_WR=%s gross_no_stop_pnl=%s EV_per_fill=%s trigger_p50=%s "
            "trigger_p90=%s trigger_max=%s legacy_pre_v3=%d",
            result["eligible_below_threshold_signals"], result["complete_coverage_signals"],
            result["partial_legacy_coverage_signals"], result["threshold_reached"],
            "n/a" if hit_rate is None else f"{100 * hit_rate:.2f}%",
            result["entries_within_opening_capture_window"],
            result["entries_after_opening_capture_window"],
            result["resolved_entries"],
            result["directional_wins"], result["directional_losses"],
            "n/a" if rate is None else f"{100 * rate:.2f}%",
            result["direct_limit_filled_entries"], result["direct_limit_resolved_filled_entries"],
            result["direct_limit_zero_fills"], result["direct_limit_pending_unfilled"],
            result["direct_limit_partial_fills"], result["direct_limit_full_fills"],
            "n/a" if fill_rate is None else f"{100 * fill_rate:.2f}%",
            result["direct_limit_filled_wins"], result["direct_limit_filled_losses"],
            "n/a" if filled_win_rate is None else f"{100 * filled_win_rate:.2f}%",
            result["gross_no_stop_pnl_total"], result["gross_no_stop_ev_per_filled_entry"],
            result["threshold_after_open_seconds_p50"], result["threshold_after_open_seconds_p90"],
            result["threshold_after_open_seconds_max"], result["legacy_contract_entries"],
        )
        LOG.warning(
            "DELAYED DIRECT 51C EXIT | resolved=%d triggers=%d legacy_maker_full=%d "
            "legacy_partial_then_hard=%d direct_or_hard=%d false_stops=%d gross_pnl=$%s EV_per_fill=%s",
            result["hybrid_resolved_entries"], result["hybrid_stop_triggers"],
            result["hybrid_maker_exit_full"], result["hybrid_partial_then_hard"],
            result["hybrid_hard_only"], result["hybrid_false_stops"],
            result["gross_hybrid_pnl_total"], result["gross_hybrid_ev_per_filled_entry"],
        )
        LOG.warning("DELAYED TRIGGER ASK BUCKETS | %s", result["trigger_ask_buckets_cents"])
        LOG.warning("DELAYED ENTRY PRICE BUCKETS | %s", result["entry_price_buckets_cents"])
        LOG.warning("================ DELAYED-ENTRY COHORT LADDERS ================")
        LOG.warning(
            "COHORT | TRIGGERS | COMPLETE | PARTIAL | RESOLVED | W/L | WR | "
            "LIMIT FILLED/ZERO/PARTIAL/FULL | FILLED W/L | NO-STOP PNL | HYBRID PNL | "
            "LADDER TRACKED/RESOLVED | LADDER PNL | COMBINED"
        )
        for threshold in OPENING_CROSS_LADDER_TRIGGER_LEVELS:
            row = result["cohorts"][str(threshold)]
            win_rate = row["directional_win_rate"]
            LOG.warning(
                ">=%sc | %d | %d | %d | %d | %d/%d | %s | %d/%d/%d/%d | %d/%d | "
                "$%s | $%s | %d/%d | $%s | $%s",
                threshold, row["entries"], row["complete_coverage_entries"],
                row["partial_coverage_entries"], row["complete_resolved_entries"],
                row["directional_wins"], row["directional_losses"],
                "n/a" if win_rate is None else f"{100 * win_rate:.2f}%",
                row["direct_filled_entries"], row["direct_zero_fills"],
                row["direct_partial_fills"], row["direct_full_fills"],
                row["direct_filled_wins"], row["direct_filled_losses"],
                row["gross_no_stop_direct_entry_pnl"],
                row["gross_hybrid_pnl"],
                row["ladder_tracked_entries"], row["ladder_resolved_entries"],
                row["gross_no_stop_ladder_pnl"],
                row["gross_no_stop_direct_plus_ladder_pnl"],
            )
            LOG.warning(
                "DELAYED >=%sc LADDER | %s | legacy_without_ladder=%d",
                threshold,
                " | ".join(
                    f"{level}c x{format(quantity, 'f')}:"
                    f"touch={row['rungs'][str(level)]['touches']} "
                    f"full={row['rungs'][str(level)]['full_fills']} "
                    f"partial={row['rungs'][str(level)]['partial_fills']} "
                    f"pnl=${row['rungs'][str(level)]['gross_no_stop_pnl']}"
                    for level, quantity in OPENING_CROSS_LADDER_RUNGS
                ),
                row["legacy_entries_without_ladder"],
            )

    def opening_cross_ladder_performance(self) -> dict[str, Any]:
        """Rebuild 53–59c first-minute trigger and ladder results idempotently."""

        output: dict[str, Any] = {}
        for threshold in OPENING_CROSS_LADDER_TRIGGER_LEVELS:
            monitored = complete = partial = crossed = resolved = wins = losses = 0
            gross_pnl = Decimal("0")
            rung_rows = {
                str(level): {
                    "price_cents": level, "requested_quantity": format(quantity, "f"),
                    "touches": 0, "full_fills": 0, "partial_fills": 0,
                    "simulated_filled_quantity": "0",
                }
                for level, quantity in OPENING_CROSS_LADDER_RUNGS
            }
            for record in self.state.get("markets", {}).values():
                if not isinstance(record, dict):
                    continue
                tracker = record.get("opening_cross_ladder")
                if not isinstance(tracker, dict):
                    continue
                monitored += 1
                if tracker.get("coverage_complete"):
                    complete += 1
                else:
                    partial += 1
                lane = tracker.get("triggered", {}).get(str(threshold))
                if not isinstance(lane, dict):
                    continue
                crossed += 1
                winner = lane.get("directional_winner")
                pnl = lane.get("gross_no_stop_pnl")
                if winner is not None and pnl is not None:
                    resolved += 1
                    wins += int(bool(winner))
                    losses += int(not bool(winner))
                    gross_pnl += Decimal(str(pnl))
                for level, requested in OPENING_CROSS_LADDER_RUNGS:
                    rung = lane.get("rungs", {}).get(str(level), {})
                    row = rung_rows[str(level)]
                    quantity = Decimal(str(rung.get("simulated_filled_quantity") or "0"))
                    row["touches"] += int(bool(rung.get("touched")))
                    row["full_fills"] += int(quantity >= requested)
                    row["partial_fills"] += int(Decimal("0") < quantity < requested)
                    row["simulated_filled_quantity"] = format(
                        Decimal(str(row["simulated_filled_quantity"])) + quantity, "f",
                    )
            output[str(threshold)] = {
                "trigger_cents": threshold,
                "monitored_markets": monitored,
                "complete_coverage_markets": complete,
                "partial_coverage_markets": partial,
                "threshold_crosses_first_60_seconds": crossed,
                "threshold_cross_rate": crossed / monitored if monitored else None,
                "resolved_crosses": resolved,
                "directional_wins": wins,
                "directional_losses": losses,
                "directional_win_rate": wins / resolved if resolved else None,
                "gross_no_stop_pnl": format(gross_pnl, "f"),
                "gross_no_stop_ev_per_resolved_cross": (
                    format(gross_pnl / resolved, "f") if resolved else None
                ),
                "rungs": rung_rows,
            }
        summary = {
            "analytics_only": True,
            "trigger_window_seconds": OPENING_CROSS_LADDER_TRIGGER_WINDOW_SECONDS,
            "trigger_levels_cents": list(OPENING_CROSS_LADDER_TRIGGER_LEVELS),
            "rung_quantities": {
                str(level): format(quantity, "f") for level, quantity in OPENING_CROSS_LADDER_RUNGS
            },
            "touch_evidence": "price_only_executable_selected_side_ask_at_or_below_rung",
            "fill_assumption": (
                "post_trigger_public_trade_volume_allocated_once_across_descending_limits_"
                "without_queue_priority"
            ),
            "entry_circuit_breakers_affect_observation": False,
            "thresholds": output,
            "updated_at": utc_now(),
        }
        self.state["opening_cross_ladder_performance"] = summary
        return summary

    def log_opening_cross_ladder_performance(self) -> None:
        summary = self.opening_cross_ladder_performance()
        LOG.warning("================ FIRST-60S 53-59C CROSS LADDER ================")
        LOG.warning(
            "TRIGGER | MARKETS | COMPLETE | PARTIAL | CROSSES | CROSS RATE | RESOLVED | W/L | WR | GROSS PNL"
        )
        for threshold in OPENING_CROSS_LADDER_TRIGGER_LEVELS:
            row = summary["thresholds"][str(threshold)]
            cross_rate = row["threshold_cross_rate"]
            win_rate = row["directional_win_rate"]
            LOG.warning(
                ">=%sc | %7d | %8d | %7d | %7d | %s | %8d | %d/%d | %s | $%s",
                threshold, row["monitored_markets"], row["complete_coverage_markets"],
                row["partial_coverage_markets"], row["threshold_crosses_first_60_seconds"],
                "n/a" if cross_rate is None else f"{100 * cross_rate:.2f}%",
                row["resolved_crosses"], row["directional_wins"], row["directional_losses"],
                "n/a" if win_rate is None else f"{100 * win_rate:.2f}%",
                row["gross_no_stop_pnl"],
            )
            LOG.warning(
                ">=%sc LADDER | %s",
                threshold,
                " | ".join(
                    f"{level}c x{format(quantity, 'f')}:"
                    f"touch={row['rungs'][str(level)]['touches']} "
                    f"full={row['rungs'][str(level)]['full_fills']} "
                    f"partial={row['rungs'][str(level)]['partial_fills']}"
                    for level, quantity in OPENING_CROSS_LADDER_RUNGS
                ),
            )

    def log_entry_price_performance(self) -> None:
        summary = self.entry_price_performance()
        LOG.warning("================ ENTRY PRICE PERFORMANCE ================")
        LOG.warning("PRICE | SIGNALS | HITS | HIT RATE | FILLS | WINNERS | WINNER HITS | CAPTURE | MISSED")
        for level in range(49, 39, -1):
            row = summary["levels"].get(str(level), {})
            percentage = lambda value: "n/a" if value is None else f"{100 * value:.2f}%"
            LOG.warning(
                "%2sc | %7s | %4s | %8s | %5s | %7s | %11s | %7s | %s (%s)",
                level, row.get("eligible_signals", 0), row.get("touched", 0), percentage(row.get("hit_rate")),
                row.get("simulated_fills", 0), row.get("eventual_winners", 0), row.get("winner_touches", 0),
                percentage(row.get("winner_capture_rate")), row.get("missed_winner_count", 0),
                percentage(row.get("missed_winner_rate")),
            )
        winners = int(summary["winning_settlements"])
        stayed = int(summary["winning_settlements_stayed_above_40"])
        reached = int(summary["winning_settlements_reached_40_or_lower"])
        LOG.warning("============= EVENTUAL WINNER DRAWDOWNS =============")
        LOG.warning(
            "Winners=%d | never_reached_40c=%d/%d (%.2f%%) | reached_40c_or_lower=%d/%d (%.2f%%)",
            winners, stayed, winners, (100 * stayed / winners if winners else 0),
            reached, winners, (100 * reached / winners if winners else 0),
        )
        for bucket, count in summary["winner_drawdown_histogram_cents"].items():
            LOG.warning("MAX DRAWDOWN %sc: %d trades (%.2f%%)", bucket, count, 100 * count / winners if winners else 0)
        for bucket, count in summary["winner_minimum_price_histogram_cents"].items():
            LOG.warning("WINNER MINIMUM %sc: %d trades (%.2f%%)", bucket, count, 100 * count / winners if winners else 0)
        eventual_low = summary.get("lowest_actual_entry_eventual_winner")
        profitable_low = summary.get("lowest_actual_entry_realized_profitable")
        LOG.warning(
            "LOWEST FILLED EVENTUAL WINNER | count=%d ticker=%s side=%s actual_entry=%s qty=%s method=%s net_pnl=%s",
            summary.get("actual_filled_eventual_winners", 0),
            (eventual_low or {}).get("ticker"), (eventual_low or {}).get("signal_side"),
            (eventual_low or {}).get("actual_average_entry_price"), (eventual_low or {}).get("actual_quantity"),
            (eventual_low or {}).get("realized_method"), (eventual_low or {}).get("realized_net_pnl"),
        )
        LOG.warning(
            "LOWEST REALIZED-PROFIT ENTRY | count=%d ticker=%s side=%s actual_entry=%s qty=%s method=%s net_pnl=%s",
            summary.get("realized_profitable_filled_trades", 0),
            (profitable_low or {}).get("ticker"), (profitable_low or {}).get("signal_side"),
            (profitable_low or {}).get("actual_average_entry_price"),
            (profitable_low or {}).get("actual_quantity"), (profitable_low or {}).get("realized_method"),
            (profitable_low or {}).get("realized_net_pnl"),
        )
        eligibility = summary["initial_stop_eligibility"]
        LOG.warning("============= INITIAL PRICE STOP ELIGIBILITY =============")
        LOG.warning(
            "Captured initial prices=%d | unavailable=%d | actual stop-safety no-entry=%d "
            "(40-49c=%d below40c=%d above49c=%d)",
            eligibility["captured_initial_prices"], eligibility["signals_without_initial_price"],
            eligibility["actual_strategy_stop_safety_rejections"],
            eligibility["actual_strategy_stop_safety_rejections_40_49"],
            eligibility["actual_strategy_stop_safety_rejections_below_40"],
            eligibility["actual_strategy_stop_safety_rejections_above_49"],
        )
        LOG.warning("STOP | INITIAL <= STOP | WOULD-BE NO-ENTRY RATE | EXACT INITIAL | ACTUAL SAFETY NO-ENTRY")
        for level in range(49, 39, -1):
            row = eligibility["levels"][str(level)]
            rate = row["would_skip_rate"]
            LOG.warning(
                "%2sc | %15d | %21s | %13d | %22d",
                level, row["initial_at_or_below_stop"],
                "n/a" if rate is None else f"{100 * rate:.2f}%",
                eligibility["exact_initial_price_counts_40_49"][str(level)],
                eligibility["actual_rejection_initial_price_counts_40_49"][str(level)],
            )
        LOG.warning(
            "NOTE | INITIAL<=STOP columns are counterfactual threshold counts; actual safety no-entry is "
            "the configured rule. Neither is a directional skip or an unfilled GTC order."
        )

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

        opening_cost_backfilled = self.ensure_opening_entry_cost(record)
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
        total_fees = Decimal("0")
        for order in record.get("entry_orders", []):
            try:
                if Decimal(str(order.get("fill_count") or "0")) > 0:
                    total_fees += Decimal(str(order.get("fees_paid") or "0"))
            except (ArithmeticError, TypeError, ValueError):
                continue

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
            "actual_entry_notional": format(total_cost, "f"),
            "actual_entry_fees": format(total_fees, "f"),
            "actual_entry_cash_cost": format(total_cost + total_fees, "f"),
            "maker_limit_order_ids": maker["order_ids"],
            "market_ioc_order_ids": ioc["order_ids"],
        }
        changed = (
            opening_cost_backfilled
            or summary != record.get("entry_execution_summary")
            or execution_type != record.get("entry_execution_type")
        )
        record["entry_execution_summary"] = summary
        record["entry_execution_type"] = execution_type
        record["actual_entry_notional"] = summary["actual_entry_notional"]
        record["actual_entry_fees"] = summary["actual_entry_fees"]
        record["actual_entry_cash_cost"] = summary["actual_entry_cash_cost"]
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
            "entry_filtered_markets": 0,
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
            elif status == "ENTRY_FILTERED":
                counts["entry_filtered_markets"] += 1
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
            actual_entry_notional=summary["actual_entry_notional"],
            actual_entry_fees=summary["actual_entry_fees"],
            actual_entry_cash_cost=summary["actual_entry_cash_cost"],
            opening_entry_cost=record.get("opening_entry_cost"),
            requested_entry_cost=record.get("requested_entry_cost"),
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
        """Persist the immutable protective-exit trigger."""

        average = self.average_filled_entry_price(record)
        if average is None:
            return None
        floor = Decimal(str(record.get("stop_floor_price") or self.config["stop_price"]))
        baseline = Decimal(str(record.get("stop_baseline_entry_price") or self.config["stop_baseline_entry_price"]))
        if record.get("stop_policy", self.config["stop_policy"]) not in {
            "hybrid_maker_then_hard_stop", "direct_ioc_at_trigger",
            "opposite_side_take_profit_ioc",
        }:
            self.trip("unsupported_persisted_stop_policy")
            return None
        effective = cents_price(int(record.get("hybrid_stop", {}).get("trigger_cents") or self.config["hybrid_stop_trigger_cents"]))
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
        """Use the persisted hybrid trigger price."""

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
        deadline = float(
            record.get("opening_quote_capture_deadline_epoch")
            or self.opening_quote_capture_deadline(record)
        )
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
        quote_epoch = executable_quote_epoch(quote)
        if quote_epoch is None or quote_epoch < start:
            # A book observed while the successor was unopened is useful only
            # for transport warm-up.  It is never entry evidence and cannot
            # be used to simulate or submit a post-open IOC.
            capture["unavailable_quote_count"] = int(capture.get("unavailable_quote_count", 0)) + 1
            capture["last_unavailable_reason"] = "preopen_or_unstamped_top_of_book"
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
            "exchange_elapsed_after_open_seconds": round(max(0.0, quote_epoch - start), 6),
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
        price_reference = record.get("opening_price_reference")
        reference_epoch = (
            float(price_reference.get("selected_component_epoch") or 0)
            if isinstance(price_reference, dict) else 0.0
        )
        capture.update({
            "started_at": capture.get("started_at") or utc_now(), "first_capture_lag_seconds": capture.get("first_capture_lag_seconds", observation["elapsed_after_open_seconds"]),
            "first_depth_capture_lag_seconds": capture.get(
                "first_depth_capture_lag_seconds", observation["exchange_elapsed_after_open_seconds"],
            ),
            "first_depth_observed_lag_seconds": capture.get(
                "first_depth_observed_lag_seconds", observation["elapsed_after_open_seconds"],
            ),
            "first_depth_after_price_seconds": capture.get(
                "first_depth_after_price_seconds",
                round(max(0.0, quote_epoch - reference_epoch), 6) if reference_epoch else None,
            ),
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
                first_depth_exchange_lag_seconds=capture.get("first_depth_capture_lag_seconds"),
                first_depth_worker_lag_seconds=capture.get("first_depth_observed_lag_seconds"),
                first_depth_after_price_seconds=capture.get("first_depth_after_price_seconds"),
            )
            LOG.warning(
                "FIRST DISPLAYED DEPTH | ticker=%s side=%s selected_ask=%s depth=%s "
                "exchange_lag=%ss after_first_price=%ss quote_id=%s",
                record["ticker"], side.upper(), observation["selected_best_ask"],
                observation["selected_ask_size"], capture.get("first_depth_capture_lag_seconds"),
                capture.get("first_depth_after_price_seconds"), observation["quote_id"],
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
        the first minute is reserved for signal and maker-entry work.
        Every entry, position, stop, or settlement transition also blocks
        handoff even in that middle window. The worker exits only while it has
        no order or position work to perform.
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
        cleared_order_id = clear_stale_current_order_pointer(self.state)
        if cleared_order_id:
            self.state.setdefault("state_repairs", []).append({
                "at": utc_now(),
                "kind": "clear_terminal_current_order_pointer_before_handoff",
                "order_id": cleared_order_id,
                "policy": "matched durable order was terminal with zero remaining quantity",
            })
            self.audit(
                "terminal_current_order_pointer_cleared",
                order_id=cleared_order_id,
                reason="safe_handoff_validation",
            )
        try:
            current_position = Decimal(str(self.state.get("current_position") or "0"))
        except (ArithmeticError, TypeError, ValueError):
            return False, {
                "reason": "invalid_current_position_requires_reconciliation",
                "ticker": active["ticker"],
            }
        if current_position != 0 or current_order_pointer_requires_recovery(self.state):
            return False, {
                "reason": "top_level_order_or_position_requires_current_worker",
                "ticker": active["ticker"],
                "current_order_id": self.state.get("current_order_id"),
                "current_position": format(current_position, "f"),
            }
        blocking_states = set(ACTIVE_STATES)
        breaker_blocked = bool(self.state.get("circuit_breaker", {}).get("blocked"))
        blockers = []
        observational_signals = []
        for item in self.state.get("markets", {}).values():
            if not isinstance(item, dict) or item.get("status") not in blocking_states:
                continue
            if (
                item.get("status") == "SIGNAL_PENDING"
                and breaker_blocked
                and not record_has_exchange_work(item)
            ):
                observational_signals.append(item.get("ticker"))
                continue
            blockers.append({"ticker": item.get("ticker"), "status": item.get("status")})
        if blockers:
            return False, {"reason": "operational_state_requires_current_worker", "ticker": active["ticker"], "blockers": blockers}
        return True, {
            "reason": "safe_middle_13_minute_handoff", "ticker": active["ticker"],
            "window_start_epoch": window_start, "window_end_epoch": window_end,
            "observational_signal_pending": observational_signals,
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
                "ticker": str(field(order, "ticker") or ""),
                "remaining_count": field(order, "remaining_count", "remaining_count_fp", "count") or "0",
                "routing_exchange_index": field(order, "exchange_index"),
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
        retry_client_id = (
            str(record.get("entry_retry", {}).get("client_order_id") or "")
            if isinstance(record.get("entry_retry"), dict) else ""
        )
        if retry_client_id:
            entry_ids.add(retry_client_id)
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

    def reconstruct_exit_accounting_from_fills(
        self, record: dict[str, Any], fills: Iterable[Any],
    ) -> Decimal:
        """Restore maker/hard-stop fill proceeds from exact exchange IDs.

        This is the restart counterpart to entry reconstruction.  It never
        attributes a ticker-only or manual fill to the strategy, and it keeps
        the original opened quantity separate from the authoritative
        *remaining* position after a partial exit.
        """

        side = str(record.get("signal_side") or "")
        if side not in {"yes", "no"}:
            return Decimal("0")
        known_orders = [
            order for order in record.setdefault("exit_orders", []) if isinstance(order, dict)
        ]
        exit_ids = {
            str(value) for order in known_orders
            for value in (order.get("order_id"), order.get("client_order_id")) if value
        }
        maker_client_id = deterministic_client_order_id(
            record["ticker"], side, "hybrid-maker-exit", self.config,
        )
        exit_ids.add(maker_client_id)
        groups: dict[str, dict[str, Any]] = {}
        seen: set[str] = set()
        for fill in fills:
            if str(field(fill, "ticker", "market_ticker") or "") != record.get("ticker"):
                continue
            order_id = str(field(fill, "order_id") or "")
            client_id = str(field(fill, "client_order_id") or "")
            if not ({order_id, client_id} - {""}) & exit_ids:
                continue
            action = str(field(fill, "action") or "").lower()
            if action and action != "sell":
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
                "quantity": Decimal("0"), "proceeds": Decimal("0"), "fees": Decimal("0"), "fill_ids": [],
            })
            group["quantity"] += quantity
            group["proceeds"] += quantity * price
            group["fees"] += self._portfolio_fill_fee(fill)
            group["fill_ids"].append(fill_id)

        for group in groups.values():
            target = next((
                order for order in known_orders
                if group["order_id"] and order.get("order_id") == group["order_id"]
            ), None)
            if target is None:
                target = next((
                    order for order in known_orders
                    if group["client_order_id"] and order.get("client_order_id") == group["client_order_id"]
                ), None)
            if target is None:
                # Only the deterministic maker-exit ID can be reconstructed
                # without a persisted local order.  Hard-stop IDs are accepted
                # only when their exact ID was checkpointed before a restart.
                if group["client_order_id"] != maker_client_id:
                    continue
                target = {
                    "order_id": group["order_id"], "client_order_id": group["client_order_id"],
                    "exit_phase": "maker_exit", "quantity": format(group["quantity"], "f"),
                    "position_price": format(cents_price(int(self.config["hybrid_maker_exit_cents"])), "f"),
                    "post_only": True, "reduce_only": True,
                }
                known_orders.append(target)
                record["exit_orders"].append(target)
            requested = Decimal(str(target.get("quantity") or group["quantity"]))
            target.update({
                "order_id": group["order_id"] or target.get("order_id"),
                "client_order_id": group["client_order_id"] or target.get("client_order_id"),
                "fill_count": format(group["quantity"], "f"),
                "remaining_count": format(round_shares(max(Decimal("0"), requested - group["quantity"])), "f"),
                "average_fill_price": format(group["proceeds"] / group["quantity"], "f"),
                "fees_paid": format(group["fees"], "f"),
                "reconciled_from_portfolio_fills": True,
                "reconciled_fill_ids": group["fill_ids"],
            })
        total = self.exit_filled_quantity(record)
        record["exit_accounting_reconciliation"] = {
            "at": utc_now(), "accounted_exit_quantity": format(total, "f"),
            "matched_fill_count": len(seen),
        }
        return total

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
        open_managed_tickers: set[str] = set()
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
            open_managed_tickers.add(ticker)
            side = str(record.get("signal_side") or "")
            maker_client_id = deterministic_client_order_id(ticker, side, "entry", self.config) if side in {"yes", "no"} else ""
            fallback_client_id = deterministic_client_order_id(ticker, side, "market-fallback", self.config) if side in {"yes", "no"} else ""
            market_entry_client_id = deterministic_client_order_id(ticker, side, "market-entry", self.config) if side in {"yes", "no"} else ""
            # Retry attempts use distinct deterministic client IDs and are
            # persisted before any successful handoff. Recognize every exact
            # persisted entry ID so startup can adopt a resting retry order;
            # ticker-only matching remains prohibited.
            persisted_entry_client_ids = {
                str(item.get("client_order_id") or "")
                for item in record.get("entry_orders", [])
                if isinstance(item, dict)
            }
            if isinstance(record.get("entry_retry"), dict):
                persisted_entry_client_ids.add(
                    str(record["entry_retry"].get("client_order_id") or "")
                )
            entry_client_ids = {
                maker_client_id, fallback_client_id, market_entry_client_id,
                *persisted_entry_client_ids,
            } - {""}
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
                if record.get("status") in {"ERROR_RECONCILIATION", "RECONCILIATION_PENDING"}:
                    # An unknown create response is a persisted circuit-breaker
                    # event.  An ordinary expected Actions restart with a
                    # known resting maker is not itself a discrepancy.
                    self.trip("unconfirmed_managed_entry_recovered")
                if record.get("status") in {
                    "SIGNAL_PENDING", "ERROR_RECONCILIATION", "RECONCILIATION_PENDING",
                }:
                    self.transition(record, "ENTRY_PENDING", "startup_recovered_unconfirmed_resting_entry")
                continue
            maker_exit_client_id = deterministic_client_order_id(ticker, side, "hybrid-maker-exit", self.config) if side in {"yes", "no"} else ""
            if client_id == maker_exit_client_id:
                exchange_order_id = str(field(order, "order_id") or "") or None
                local = next((item for item in record.setdefault("exit_orders", []) if isinstance(item, dict) and (
                    (exchange_order_id and item.get("order_id") == exchange_order_id)
                    or item.get("client_order_id") == client_id
                )), None)
                if local is None:
                    local = {"order_id": exchange_order_id, "client_order_id": client_id}
                    record["exit_orders"].append(local)
                local.update({
                    "order_id": exchange_order_id, "client_order_id": client_id, "held_side": side, "side": side,
                    "exit_phase": "maker_exit", "order_type": "reduce_only_exit_maker",
                    "position_price": format(cents_price(int(self.config["hybrid_maker_exit_cents"])), "f"),
                    "fill_count": format(Decimal(str(order_fill_count(order))), "f"),
                    "remaining_count": format(Decimal(str(order_remaining_count(order) or "0")), "f"),
                    "average_fill_price": format(Decimal(str(order_average_position_price(
                        order, side, float(cents_price(int(self.config["hybrid_maker_exit_cents"]))),
                    ))), "f"),
                    "fees_paid": format(Decimal(str(order_fee_total(order))), "f"),
                    "reconciled_from_open_order": True, "post_only": True, "reduce_only": True,
                })
                record.setdefault("hybrid_stop", {})["maker_order_id"] = exchange_order_id
                record["hybrid_stop"]["state"] = "MAKER_EXIT_PENDING"
                if record.get("status") not in {"MAKER_EXIT_PENDING", "MAKER_EXIT_PARTIAL"}:
                    self.transition(record, "MAKER_EXIT_PENDING", "startup_recovered_hybrid_maker_exit")
                continue
            self.trip("unknown_managed_order_role")
            self.audit(
                "reconciliation_discrepancy", ticker=ticker, client_order_id=client_id,
                reason="unknown_managed_order_role",
            )
            return False
        authoritative_positions: dict[str, Decimal] = {}
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
            authoritative_positions[ticker] = raw
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
            exit_filled = self.reconstruct_exit_accounting_from_fills(record, fill_rows)
            required_opened_quantity = abs(raw) + exit_filled
            accounting_reconciled = self.reconstruct_entry_accounting_from_fills(
                record, fill_rows, required_opened_quantity,
            )
            accounted_entry = sum(
                (Decimal(str(order.get("fill_count") or "0")) for order in record.get("entry_orders", [])),
                Decimal("0"),
            )
            original_opened = max(
                Decimal(str(record.get("actual_quantity") or "0")),
                accounted_entry,
                required_opened_quantity,
            )
            record["actual_quantity"] = format(original_opened, "f")
            record["authoritative_open_quantity"] = format(abs(raw), "f")
            if not accounting_reconciled:
                # Keep stop management alive for the actual exposure, but do
                # not manufacture a cost basis or advance recovery from it.
                self.trip("entry_fill_accounting_unreconciled")
                self.audit(
                    "reconciliation_discrepancy", ticker=ticker, reason="entry_fill_accounting_unreconciled",
                    reconciliation=record["entry_accounting_reconciliation"],
                )
            self.update_effective_stop_price(record)
            if record.get("status") not in {
                "POSITION_OPEN", "ENTRY_PARTIAL", "MAKER_EXIT_PENDING", "MAKER_EXIT_PARTIAL",
                "MAKER_EXIT_CANCEL_UNCONFIRMED", "HARD_STOP_PENDING",
            }:
                # This includes historical ERROR_RECONCILIATION records.  An
                # exchange-confirmed position is never allowed to remain in a
                # terminal local status where stop management would ignore it.
                self.transition(record, "POSITION_OPEN", "startup_authoritative_position")
        # Reconstruct fills even when the current authoritative position is
        # flat. This covers a maker or hard-stop exit completing between a
        # material checkpoint and the next Actions worker startup.
        for ticker, record in known.items():
            if not isinstance(record, dict) or ticker in authoritative_positions:
                continue
            exit_filled = self.reconstruct_exit_accounting_from_fills(record, fill_rows)
            self.reconstruct_entry_accounting_from_fills(record, fill_rows, exit_filled)
            accounted_entry = sum(
                (Decimal(str(order.get("fill_count") or "0")) for order in record.get("entry_orders", [])),
                Decimal("0"),
            )
            if accounted_entry > 0:
                record["actual_quantity"] = format(max(
                    Decimal(str(record.get("actual_quantity") or "0")), accounted_entry,
                ), "f")
            record["authoritative_open_quantity"] = "0"
            if (
                record.get("status") in {
                    "MAKER_EXIT_PENDING", "MAKER_EXIT_PARTIAL", "MAKER_EXIT_CANCEL_UNCONFIRMED", "HARD_STOP_PENDING",
                }
                and ticker not in open_managed_tickers
                and accounted_entry > 0
                and exit_filled + Decimal("0.004") >= accounted_entry
                and record.get("entry_accounting_reconciled") is not False
            ):
                maker_filled = sum(
                    (Decimal(str(order.get("fill_count") or "0")) for order in record.get("exit_orders", []) if order.get("exit_phase") == "maker_exit"),
                    Decimal("0"),
                )
                hard_filled = exit_filled - maker_filled
                record["exit_classification"] = (
                    "MAKER_EXIT_FULL" if hard_filled <= 0
                    else "MAKER_EXIT_PARTIAL_THEN_HARD_STOP" if maker_filled > 0
                    else "HARD_STOP_ONLY"
                )
                record.setdefault("hybrid_stop", {})["state"] = record["exit_classification"]
                await self.finalize_stop(record)
        self.state["current_position"] = format(sum(
            (abs(value) for value in authoritative_positions.values()), Decimal("0"),
        ), "f")
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

    @staticmethod
    async def portfolio_pages(
        rest: KalshiREST, path: str, key: str, params: dict[str, Any],
    ) -> list[Any]:
        """Read a complete bounded portfolio result set for exact reconciliation."""

        rows: list[Any] = []
        cursor: str | None = None
        seen: set[str] = set()
        for _ in range(20):
            request = dict(params, limit=1000)
            if cursor:
                request["cursor"] = cursor
            payload = await rest.get_raw_json(path, request)
            page = payload.get(key, []) if isinstance(payload, dict) else []
            if not isinstance(page, list):
                raise RuntimeError("unexpected reconciliation page")
            rows.extend(page)
            next_cursor = str(payload.get("cursor") or "") if isinstance(payload, dict) else ""
            if not next_cursor:
                return rows
            if next_cursor in seen:
                raise RuntimeError("repeated reconciliation cursor")
            seen.add(next_cursor)
            cursor = next_cursor
        raise RuntimeError("reconciliation pagination exceeded bound")

    async def reconcile_failed_entry_cancellation(
        self, rest: KalshiREST, record: dict[str, Any], order: dict[str, Any],
    ) -> bool:
        """Prove a failed-cancel entry can no longer create more exposure.

        A Cancel V2 404 is not itself permission to sell: the order may have
        completed during the cancel race.  Kalshi guarantees every resting
        order remains visible through ``GET /portfolio/orders``.  Therefore a
        complete ticker-scoped read, exact fill read, and authoritative
        position read can distinguish a still-resting order from a terminal
        one while also capturing any fill that raced the cancel request.
        """

        if self.dry_run or not hasattr(rest, "get_raw_json"):
            return False
        ticker = str(record.get("ticker") or "")
        order_id = str(order.get("order_id") or "")
        client_id = str(order.get("client_order_id") or "")
        if not ticker or (not order_id and not client_id):
            return False

        try:
            order_rows, fill_rows, position = await asyncio.gather(
                self.portfolio_pages(rest, "/portfolio/orders", "orders", {"ticker": ticker}),
                self.portfolio_pages(
                    rest,
                    "/portfolio/fills", "fills",
                    {"ticker": ticker, **({"order_id": order_id} if order_id else {})},
                ),
                rest.position_for_ticker(ticker),
            )
        except Exception as exc:
            order["cancel_reconciliation"] = {
                "at": utc_now(), "status": "READ_UNAVAILABLE", "error_type": type(exc).__name__,
            }
            return False
        if position is None:
            order["cancel_reconciliation"] = {"at": utc_now(), "status": "POSITION_UNKNOWN"}
            return False

        exact_orders = [
            item for item in order_rows
            if str(field(item, "ticker") or "") == ticker
            and (
                (order_id and str(field(item, "order_id") or "") == order_id)
                or (client_id and str(field(item, "client_order_id") or "") == client_id)
            )
        ]
        if len({str(field(item, "order_id") or "") for item in exact_orders}) > 1:
            self.trip("entry_cancel_reconciliation_multiple_orders")
            return False

        exchange_order = exact_orders[0] if exact_orders else None
        if exchange_order is not None:
            exchange_remaining = order_remaining_count(exchange_order)
            exchange_status = normalized_order_status(field(exchange_order, "status"))
            if exchange_status == "resting" and (exchange_remaining is None or exchange_remaining > 0):
                order["cancel_reconciliation"] = {
                    "at": utc_now(), "status": "STILL_RESTING",
                    "remaining_count": None if exchange_remaining is None else format(Decimal(str(exchange_remaining)), "f"),
                }
                return False
            if exchange_remaining is None and exchange_status not in {"canceled", "cancelled", "executed", "filled"}:
                order["cancel_reconciliation"] = {
                    "at": utc_now(), "status": "ORDER_TERMINALITY_UNKNOWN",
                    "exchange_status": exchange_status,
                }
                return False
            exchange_fill = Decimal(str(order_fill_count(exchange_order)))
            order.update({
                "fill_count": format(max(Decimal(str(order.get("fill_count") or "0")), exchange_fill), "f"),
                "average_fill_price": format(Decimal(str(order_average_position_price(
                    exchange_order, str(record.get("signal_side")),
                    float(order.get("position_price") or self.config["entry_price"]),
                ))), "f"),
                "fees_paid": format(max(
                    Decimal(str(order.get("fees_paid") or "0")), Decimal(str(order_fee_total(exchange_order))),
                ), "f"),
                "exchange_terminal_status": exchange_status,
            })

        exact_fills = [
            item for item in fill_rows
            if str(field(item, "ticker", "market_ticker") or "") == ticker
            and (
                (order_id and str(field(item, "order_id") or "") == order_id)
                or (client_id and str(field(item, "client_order_id") or "") == client_id)
            )
        ]
        signed_position = Decimal(str(position))
        side = str(record.get("signal_side") or "")
        if (side == "yes" and signed_position < 0) or (side == "no" and signed_position > 0):
            self.trip("entry_cancel_reconciliation_position_direction_mismatch")
            return False

        self.reconstruct_entry_accounting_from_fills(record, exact_fills, abs(signed_position))
        accounted = sum(
            (Decimal(str(item.get("fill_count") or "0")) for item in record.get("entry_orders", [])),
            Decimal("0"),
        )
        actual = max(Decimal(str(record.get("actual_quantity") or "0")), accounted, abs(signed_position))
        record["actual_quantity"] = format(actual, "f")
        record["authoritative_open_quantity"] = format(abs(signed_position), "f")
        self.state["current_position"] = format(abs(signed_position), "f")
        order.update({
            "remaining_count": "0", "status": "cancel_reconciled_not_resting",
            "canceled_at": utc_now(), "cancel_reconciled_after_failure": True,
            "cancel_reconciliation": {
                "at": utc_now(), "status": "CONFIRMED_NOT_RESTING",
                "exchange_order_found": exchange_order is not None,
                "exact_fill_records": len(exact_fills),
                "authoritative_position": format(signed_position, "f"),
                "accounted_entry_quantity": format(accounted, "f"),
            },
        })
        self.audit(
            "entry_cancel_reconciled", ticker=ticker, order_id=order_id,
            exchange_order_found=exchange_order is not None, exact_fill_records=len(exact_fills),
            authoritative_position=format(signed_position, "f"),
            accounted_entry_quantity=format(accounted, "f"),
        )
        LOG.warning(
            "ENTRY CANCEL RECONCILED | ticker=%s order_id=%s not_resting=true "
            "exchange_order_found=%s fills=%d position=%s accounted=%s next=risk_management",
            ticker, order_id, exchange_order is not None, len(exact_fills),
            format(signed_position, "f"), format(accounted, "f"),
        )
        return True

    async def reconcile_failed_maker_exit_cancellation(
        self, rest: KalshiREST, record: dict[str, Any], order: dict[str, Any],
    ) -> bool:
        """Resolve a maker-exit cancel race before sending a residual IOC.

        A failed DELETE can mean the maker exit filled or became terminal just
        before the request reached the matching engine.  A complete current
        order read proves whether it can still sell, while exact fills and the
        authoritative position determine the only safe IOC residual.
        """

        if self.dry_run or not hasattr(rest, "get_raw_json"):
            return False
        ticker = str(record.get("ticker") or "")
        order_id = str(order.get("order_id") or "")
        client_id = str(order.get("client_order_id") or "")
        if not ticker or (not order_id and not client_id):
            return False
        try:
            order_rows, fill_rows, position = await asyncio.gather(
                self.portfolio_pages(rest, "/portfolio/orders", "orders", {"ticker": ticker}),
                self.portfolio_pages(
                    rest, "/portfolio/fills", "fills",
                    {"ticker": ticker, **({"order_id": order_id} if order_id else {})},
                ),
                rest.position_for_ticker(ticker),
            )
        except Exception as exc:
            order["cancel_reconciliation"] = {
                "at": utc_now(), "status": "READ_UNAVAILABLE", "error_type": type(exc).__name__,
            }
            return False
        if position is None:
            order["cancel_reconciliation"] = {"at": utc_now(), "status": "POSITION_UNKNOWN"}
            return False

        exact_orders = [
            item for item in order_rows
            if str(field(item, "ticker") or "") == ticker
            and (
                (order_id and str(field(item, "order_id") or "") == order_id)
                or (client_id and str(field(item, "client_order_id") or "") == client_id)
            )
        ]
        if len({str(field(item, "order_id") or "") for item in exact_orders}) > 1:
            self.trip("maker_exit_cancel_reconciliation_multiple_orders")
            return False
        exchange_order = exact_orders[0] if exact_orders else None
        if exchange_order is not None:
            exchange_remaining = order_remaining_count(exchange_order)
            exchange_status = normalized_order_status(field(exchange_order, "status"))
            if exchange_status == "resting" and (exchange_remaining is None or exchange_remaining > 0):
                order["cancel_reconciliation"] = {
                    "at": utc_now(), "status": "STILL_RESTING",
                    "remaining_count": (
                        None if exchange_remaining is None
                        else format(Decimal(str(exchange_remaining)), "f")
                    ),
                }
                return False
            if exchange_remaining is None and exchange_status not in {
                "canceled", "cancelled", "executed", "filled",
            }:
                order["cancel_reconciliation"] = {
                    "at": utc_now(), "status": "ORDER_TERMINALITY_UNKNOWN",
                    "exchange_status": exchange_status,
                }
                return False

        side = str(record.get("signal_side") or "")
        signed_position = Decimal(str(position))
        if (side == "yes" and signed_position < 0) or (side == "no" and signed_position > 0):
            self.trip("maker_exit_cancel_reconciliation_position_direction_mismatch")
            return False
        self.reconstruct_exit_accounting_from_fills(record, fill_rows)
        order.update({
            "remaining_count": "0", "status": "cancel_reconciled_not_resting",
            "canceled_at": utc_now(), "cancel_reconciled_after_failure": True,
            "cancel_reconciliation": {
                "at": utc_now(), "status": "CONFIRMED_NOT_RESTING",
                "exchange_order_found": exchange_order is not None,
                "authoritative_position": format(signed_position, "f"),
            },
        })
        record["authoritative_open_quantity"] = format(abs(signed_position), "f")
        self.state["current_position"] = format(abs(signed_position), "f")
        self.audit(
            "maker_exit_cancel_reconciled", ticker=ticker, order_id=order_id,
            exchange_order_found=exchange_order is not None,
            authoritative_position=format(signed_position, "f"),
        )
        LOG.warning(
            "MAKER EXIT CANCEL RECONCILED | ticker=%s order_id=%s not_resting=true "
            "position=%s next=residual_hard_stop",
            ticker, order_id, format(signed_position, "f"),
        )
        return True

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
            # A definitive exchange rejection (or a locally deferred intent)
            # never became a resting order and therefore cannot fill later.
            # Close only that local attempt; accepted/unknown intents still
            # require exchange cancellation/reconciliation below.
            if order.get("submission_outcome") in {"rejected", "not_submitted"}:
                order.update({
                    "remaining_count": "0.00",
                    "status": "rejected_terminal" if order.get("submission_outcome") == "rejected" else "not_submitted",
                    "cancellation_reason": "no_exchange_order_created",
                    "canceled_at": utc_now(),
                })
                continue
            # A dry-run maker record has no exchange order ID by design. It is
            # simulated public-trade evidence, not an exchange order. Once a
            # risk or market-close cancellation closes it locally, later
            # public trades cannot fill it. Treating it as an uncertain
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
            if not acknowledged and not self.dry_run:
                acknowledged = await self.reconcile_failed_entry_cancellation(rest, record, order)
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

        first_unconfirmed = record.get("status") != "ENTRY_CANCEL_UNCONFIRMED"
        record["entry_cancel_pending"] = {
            "at": utc_now(), "next_action": next_action, "orders": unconfirmed,
            "executable_bid": None if executable_bid is None else format(executable_bid, "f"),
        }
        self.trip("entry_cancellation_unconfirmed")
        self.transition(record, "ENTRY_CANCEL_UNCONFIRMED", "entry_cancellation_unconfirmed")
        if first_unconfirmed:
            self.audit(
                "entry_cancellation_unconfirmed", ticker=record["ticker"],
                next_action=next_action, orders=unconfirmed,
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
                self.finish_entry_attempt(record, filled, "entry_cancel_confirmed_before_hybrid_stop")
                self.clear_resolved_entry_interlock(record, "entry_cancel_confirmed_before_hybrid_stop")
                # Re-evaluate fresh executable pricing, never the stale bid
                # saved before the failed cancellation. Hard-stop latch stays.
                await self.manage_stop(rest, feed, record)
            else:
                self.finish_entry_attempt(record, filled, "entry_cancel_confirmed_before_stop")
                self.clear_resolved_entry_interlock(record, "entry_cancel_confirmed_before_stop")
            return True

        # An uncertain cancellation is never followed by an IOC replacement.
        # Once it is resolved we either manage the confirmed position or count
        # a strict zero fill, preserving the recovery state in the latter case.
        self.finish_entry_attempt(record, filled, "entry_cancel_confirmed_without_replacement")
        self.clear_resolved_entry_interlock(record, "entry_cancel_confirmed_without_replacement")
        return True

    def refresh_shadow_entry(self, feed: KalshiLiveFeed, record: dict[str, Any]) -> Decimal:
        """Conservative public-trade-through evidence for a shadow maker fill.

        It is deliberately an observation model, not a claim that our order
        was actually at the front of the exchange queue.  Only public trades
        after submission at or through the limit can consume the simulated
        order, and the evidence is kept in the separate shadow ledger.
        """

        if record.get("entry_execution_mode") == "opposite_side_doubling_ladder":
            return self.refresh_shadow_opposite_ladder(feed, record)
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
        # Exchange and simulated fills are irreversible. A bounded feed
        # buffer or reconnect may forget old trade events, but it can never
        # reduce already-observed filled quantity.
        fill = max(previous_fill, fill)
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

    def refresh_shadow_opposite_ladder(
        self, feed: KalshiLiveFeed, record: dict[str, Any],
    ) -> Decimal:
        """Allocate each public trade once across the resting shadow ladder.

        Higher bids receive volume first.  This conservative deterministic
        queue proxy never uses one public-trade quantity to fill multiple
        shadow rungs and never infers a fill from an ask/bid touch alone.
        """

        orders = [
            order for order in record.get("entry_orders", [])
            if isinstance(order, dict) and order.get("entry_phase") == "maker"
        ]
        if not orders:
            return Decimal("0")
        ladder = record.setdefault("opposite_ladder", {})
        processed = set(str(value) for value in ladder.get("shadow_processed_trade_ids", []))
        timestamps = [_iso_epoch(order.get("submitted_at")) for order in orders]
        earliest = min((value for value in timestamps if value is not None), default=None)
        if earliest is None:
            return Decimal(str(record.get("actual_quantity") or "0"))
        try:
            created = datetime.fromtimestamp(earliest, timezone.utc)
            events = feed.public_trades_after(record["ticker"], created)
        except (AttributeError, TypeError, ValueError):
            return Decimal(str(record.get("actual_quantity") or "0"))
        side = str(record["trade_side"])
        for index, event in enumerate(events):
            identity = str(event.get("trade_id") or f"{event.get('ts_ms')}:{index}")
            if identity in processed:
                continue
            try:
                price = Decimal(str(event[f"{side}_price"]))
                available = Decimal(str(event["count"]))
            except (ArithmeticError, KeyError, TypeError, ValueError):
                continue
            if available <= 0:
                continue
            for order in sorted(orders, key=lambda item: Decimal(str(item["position_price"])), reverse=True):
                if available <= 0:
                    break
                if str(order.get("status")) in {"shadow_cancelled", "canceled"}:
                    continue
                limit = Decimal(str(order["position_price"]))
                if not Decimal("0") < price <= limit:
                    continue
                requested = Decimal(str(order["quantity"]))
                prior = Decimal(str(order.get("fill_count") or "0"))
                remaining = max(Decimal("0"), requested - prior)
                if remaining <= 0:
                    continue
                affordable = (self.shadow_available_cash() / limit).quantize(
                    Decimal("0.01"), rounding=ROUND_DOWN,
                )
                delta = round_shares(min(remaining, available, affordable))
                if delta <= 0:
                    continue
                filled = prior + delta
                order.update({
                    "fill_count": format(filled, "f"),
                    "remaining_count": format(requested - filled, "f"),
                    "average_fill_price": format(limit, "f"),
                    "status": "shadow_filled" if filled >= requested else "shadow_partially_filled",
                    "last_shadow_fill_trade_id": identity,
                })
                self.reserve_shadow_entry_cash(delta, limit)
                available -= delta
            processed.add(identity)
        ladder["shadow_processed_trade_ids"] = list(processed)[-4096:]
        previous_total = Decimal(str(record.get("actual_quantity") or "0"))
        total = sum((Decimal(str(order.get("fill_count") or "0")) for order in orders), Decimal("0"))
        record["actual_quantity"] = format(total, "f")
        self.state["current_position"] = record["actual_quantity"]
        if total > 0:
            cost = sum(
                Decimal(str(order.get("fill_count") or "0")) * Decimal(str(order["position_price"]))
                for order in orders
            )
            self.state["average_entry"] = format(cost / total, "f")
            record["actual_average_entry_price"] = self.state["average_entry"]
        self.note_entry_fill_observed(record, previous_total, total, "shadow_opposite_ladder_trade_through")
        self.note_entry_execution_summary(record, "shadow_opposite_ladder_refresh")
        return total

    def opening_quote_capture_deadline(self, record: dict[str, Any]) -> float:
        """Bound high-frequency opening telemetry without expiring the GTC."""

        configured = float(record["market_open_epoch"]) + int(self.config["opening_quote_capture_seconds"])
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
        record["exit_classification"] = "ENTRY_NOT_FILLED"
        if all(Decimal(str(order.get("remaining_count") or "0")) <= 0 for order in record.get("entry_orders", [])):
            self.state["current_order_id"] = None
        self.state["sizing"] = zero_fill_snapshot(self.current_parameters(), self.state.get("sizing"))
        self.note_zero_fill()
        self.refresh_entry_execution_metrics()
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

        raise RuntimeError(
            "retired v10 market-fallback entry path is disabled; "
            "v11 permits only the immutable signal-price maker entry"
        )

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
        """Submit the one v10 entry attempt as a protected IOC at the fresh ask.

        Kalshi does not need an unbounded buy instruction here: an IOC limit at
        the current executable selected-side ask is the safe market-order
        equivalent.  It either fills immediately at no worse than the just
        observed price or leaves no resting order.  Unlike the retired maker
        path, this method never submits GTC/post-only entry exposure and
        never performs a maker cancellation before entering.
        """

        raise RuntimeError(
            "retired v10 immediate-market entry path is disabled; "
            "v11 permits only the immutable signal-price maker entry"
        )

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
        quote_epoch = executable_quote_epoch(quote)
        if quote_epoch is None or quote_epoch < float(record["market_open_epoch"]):
            # Pre-subscription is intentional, but a pre-open quote is not
            # evidence that the new contract is trading.  Wait for the first
            # fresh exchange/receive timestamp at or after the actual open.
            record["market_entry_attempted"] = False
            record["market_entry"] = {
                "attempted_at": utc_now(), "status": "waiting_for_post_open_executable_book",
                "reason": "preopen_or_unstamped_top_of_book",
                "quote_timestamp": quote_epoch,
            }
            LOG.warning(
                "MARKET IOC WAIT | ticker=%s side=%s reason=preopen_or_unstamped_top_of_book age_after_open=%.3fs",
                record["ticker"], side.upper(), now - float(record["market_open_epoch"]),
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

    async def submit_signal_maker_entry(
        self, rest: KalshiREST, feed: KalshiLiveFeed, record: dict[str, Any], now: float,
    ) -> None:
        """Submit the immutable post-only GTC intent until Kalshi accepts it.

        A definitive rejection contains proof that no exchange order exists,
        so retrying with a new deterministic attempt ID cannot duplicate
        exposure. A missing/unknown response is categorically different and
        remains interlocked behind exact order/fill/position reconciliation.
        """

        # Any accepted or ambiguous attempt owns the intent. Only attempts
        # explicitly proven rejected by Kalshi, or locally deferred before a
        # POST was made, may permit another POST.
        if any(
            isinstance(order, dict)
            and (
                order.get("order_id")
                or (
                    order.get("submission_outcome") not in {"rejected", "not_submitted"}
                    and order.get("status") != "submitting"
                )
            )
            for order in record.get("entry_orders", [])
        ):
            return
        retry = record.get("entry_retry")
        rejected_attempts = sum(
            isinstance(order, dict) and order.get("submission_outcome") == "rejected"
            for order in record.get("entry_orders", [])
        )
        locally_deferred_attempt = any(
            isinstance(order, dict) and order.get("submission_outcome") == "not_submitted"
            for order in record.get("entry_orders", [])
        )
        retry_cycle_active = rejected_attempts > 0 or locally_deferred_attempt or (
            isinstance(retry, dict) and retry.get("status") == "RETRY_PENDING"
        )
        if isinstance(retry, dict):
            try:
                next_attempt_epoch = float(retry.get("next_attempt_epoch") or 0)
            except (TypeError, ValueError):
                next_attempt_epoch = float("inf")
            if now < next_attempt_epoch:
                return
        if self.config["entry_execution_mode"] == "delayed_threshold_band_maker":
            maker_price = self.freeze_delayed_band_entry_price(feed, record, now)
        elif self.config["entry_execution_mode"] == "signal_price_minus_offset_maker":
            # Retained only for unit-level regression tests and forensic replay
            # of v11 state.  Current configuration validation rejects this mode,
            # so a production v13 worker cannot select it.
            maker_price = self.freeze_initial_signal_price(feed, record, now)
        else:
            maker_price = None
        if maker_price is None or record.get("status") != "SIGNAL_PENDING":
            return
        side = str(record["signal_side"])
        quantity = Decimal(str(record["intended_quantity"]))
        # Revalidate against the signal's frozen fixed cap, not a later
        # base, recovery exponent or requested configuration. Existing
        # position/order guards below still prohibit additive/duplicate risk.
        record_parameters = self.record_parameters(record)
        cap = record_parameters.effective_max_position(record.get("base_before"))
        if self.state["circuit_breaker"].get("blocked"):
            self.transition(record, "ERROR_RECONCILIATION", "invalid_prescribed_entry_configuration")
            return
        if not quantity.is_finite() or quantity <= 0 or quantity != round_shares(quantity) or quantity > cap:
            self.trip("entry_quantity_exceeds_position_cap_or_invalid")
            self.transition(record, "ERROR_RECONCILIATION", "invalid_prescribed_entry_quantity")
            return
        required = exact_entry_notional(quantity, maker_price)
        record["requested_entry_cost"] = format(required, "f")
        record["requested_entry_cost_recorded_at"] = utc_now()
        preflight_started = time.monotonic()
        funding_scope = "shadow_cash" if self.dry_run else "market_exchange"
        exchange_index = None
        funding_error_type = None
        if self.dry_run:
            balance = self.shadow_available_cash()
        else:
            try:
                exchange_index, balance = await rest.market_entry_funding(record["ticker"])
            except Exception as exc:  # no aggregate-balance fallback, no raw SDK exception
                balance = None
                funding_error_type = type(exc).__name__
        record["entry_funding"] = {
            "at": utc_now(), "scope": funding_scope, "exchange_index": exchange_index,
            "available_balance": None if balance is None else format(balance, "f"),
            "required_cash": format(required, "f"), "error_type": funding_error_type,
        }
        LOG.warning("ENTRY FUNDING | ticker=%s scope=%s exchange_index=%s available=%s required=%s error_type=%s",
                    record["ticker"], funding_scope, exchange_index,
                    record["entry_funding"]["available_balance"], required, funding_error_type)
        if self.dry_run:
            metrics = self.shadow_metrics()
            metrics["max_required_cash"] = format(max(Decimal(str(metrics["max_required_cash"])), required), "f")
        if balance is None or balance < required:
            details = {
                **record["entry_funding"], "quantity": format(quantity, "f"),
                "requested_price": format(maker_price, "f"),
            }
            record["funding_failure"] = details
            reason = "market_exchange_funding_unavailable" if balance is None else "insufficient_cash_for_signal_limit"
            self.note_entry_attempt_completed(record, Decimal("0"), reason)
            self.transition(record, "FUNDING_FAILURE", reason)
            if self.dry_run:
                self.shadow_metrics()["funding_failures"] = int(self.shadow_metrics()["funding_failures"]) + 1
            self.audit("funding_failure", ticker=record["ticker"], **details)
            return
        if not self.dry_run:
            existing = await rest.position_for_ticker(record["ticker"])
            if existing is None:
                self.trip("entry_position_reconciliation_failed")
                self.transition(record, "RECONCILIATION_PENDING", "entry_position_unknown")
                return
            if abs(Decimal(str(existing))) > 0:
                self.trip("existing_position_before_entry")
                return
        # The limit was frozen when the strategy first observed an eligible
        # threshold quote.  Re-read a fresh executable book immediately before
        # POST: a post-only BUY is valid only while the selected-side ask is
        # strictly above that frozen limit.  If the book has moved through the
        # limit, wait for it to become maker-safe again instead of knowingly
        # sending a crossing order that the exchange must reject.  This does
        # not chase/reprice the order and applies identically in shadow/live.
        fresh_quote, fresh_state = feed.executable_shadow_quote(
            record["ticker"], side, 0.0, float(self.config["max_stale_quote_seconds"]),
        )
        fresh_ask = None
        if fresh_quote is not None:
            try:
                fresh_ask = Decimal(str(fresh_quote["economic_price"]))
            except (ArithmeticError, KeyError, TypeError, ValueError):
                fresh_ask = None
        abandon_price = cents_price(ENTRY_RETRY_ABANDON_ASK_CENTS)
        # The v13 delayed-band strategy freezes only after observing >=53c.
        # If a fresh executable ask has already fallen to the 51c protective
        # exit level, opening new risk is pointless even before the first POST.
        # Keep the condition mode-scoped so forensic v11 replay semantics do
        # not change retroactively.
        abandon_allowed = retry_cycle_active or (
            self.config["entry_execution_mode"] == "delayed_threshold_band_maker"
        )
        if abandon_allowed and fresh_ask is not None and fresh_ask <= abandon_price:
            record["entry_retry"] = {
                **(retry if isinstance(retry, dict) else {}),
                "status": "ABANDONED_AT_OR_BELOW_51C",
                "abandoned_at": utc_now(),
                "fresh_selected_side_ask": format(fresh_ask, "f"),
                "abandon_ask_cents": ENTRY_RETRY_ABANDON_ASK_CENTS,
            }
            self.audit(
                "entry_abandoned_at_or_below_51c", ticker=record["ticker"], side=side,
                frozen_limit=format(maker_price, "f"),
                fresh_selected_side_ask=format(fresh_ask, "f"),
                abandon_ask_cents=ENTRY_RETRY_ABANDON_ASK_CENTS,
                attempted_orders=len(record.get("entry_orders", [])),
            )
            LOG.warning(
                "ENTRY ABANDONED | ticker=%s side=%s frozen_limit=$%s "
                "fresh_ask=$%s threshold=%sc prior_rejection=%s recovery_unchanged=true",
                record["ticker"], side.upper(), format(maker_price, "f"),
                format(fresh_ask, "f"), ENTRY_RETRY_ABANDON_ASK_CENTS,
                rejected_attempts > 0,
            )
            self.finish_entry_attempt(
                record, Decimal("0"), "entry_abandoned_at_or_below_51c",
            )
            return
        if fresh_ask is None or fresh_ask <= maker_price:
            wait_reason = (
                "fresh_book_unavailable" if fresh_ask is None
                else "frozen_post_only_limit_would_cross"
            )
            previous_wait = record.get("maker_entry_wait")
            record["maker_entry_wait"] = {
                "status": "WAITING_FOR_MAKER_SAFE_BOOK", "reason": wait_reason,
                "checked_at": utc_now(), "frozen_limit": format(maker_price, "f"),
                "fresh_selected_side_ask": None if fresh_ask is None else format(fresh_ask, "f"),
                "quote_state": fresh_state,
            }
            if not isinstance(previous_wait, dict) or previous_wait.get("reason") != wait_reason:
                self.audit(
                    "maker_entry_wait", ticker=record["ticker"], side=side,
                    reason=wait_reason, frozen_limit=format(maker_price, "f"),
                    fresh_selected_side_ask=(
                        None if fresh_ask is None else format(fresh_ask, "f")
                    ),
                    quote_state=fresh_state,
                )
                LOG.warning(
                    "ENTRY WAIT | ticker=%s side=%s frozen_limit=$%s fresh_ask=%s "
                    "reason=%s post_only_rejection_prevented=true",
                    record["ticker"], side.upper(), format(maker_price, "f"),
                    None if fresh_ask is None else format(fresh_ask, "f"), wait_reason,
                )
            return
        record.pop("maker_entry_wait", None)
        # Include time spent awaiting authenticated preflight calls while
        # preserving the event clock supplied by deterministic replay/tests.
        if now + max(0.0, time.monotonic() - preflight_started) >= float(record["market_close_epoch"]):
            self.finish_entry_attempt(record, Decimal("0"), "market_closed_before_gtc_submission")
            return
        purpose = "entry" if rejected_attempts == 0 else f"entry-retry-{rejected_attempts}"
        client_id = deterministic_client_order_id(record["ticker"], side, purpose, self.config)
        maker_tif = str(self.config["maker_order_time_in_force"])
        # Historical telemetry only. Retry authority comes from each persisted
        # order's explicit submission_outcome and deterministic client ID.
        record["maker_entry_submission_attempted"] = True
        if self.dry_run:
            order = {
                "order_id": None, "client_order_id": client_id, "ticker": record["ticker"], "side": side,
                "quantity": format(quantity, "f"), "position_price": format(maker_price, "f"),
                "time_in_force": maker_tif, "post_only": True, "reduce_only": False,
                "expiration_time": None, "fill_count": "0.00", "remaining_count": format(quantity, "f"),
                "average_fill_price": None, "fees_paid": "0", "entry_phase": "maker",
                "status": "shadow_resting", "shadow_execution": "conservative_public_trade_through",
                "submitted_at": utc_now(),
            }
        else:
            # Persist the exact non-idempotent intent before calling Kalshi.
            # If the process dies after the POST, startup can recover the
            # order/fill using this client ID instead of submitting again.
            order = next((
                item for item in record.get("entry_orders", [])
                if isinstance(item, dict)
                and item.get("client_order_id") == client_id
                and item.get("status") in {"submitting", "paused"}
                and item.get("submission_outcome") in {"unknown", "not_submitted"}
                and not item.get("order_id")
            ), None)
            resumed_pre_submit_intent = order is not None
            if order is None:
                order = {
                    "order_id": None, "client_order_id": client_id,
                    "ticker": record["ticker"], "side": side,
                    "quantity": format(quantity, "f"),
                    "position_price": format(maker_price, "f"),
                    "time_in_force": maker_tif, "post_only": True,
                    "reduce_only": False, "expiration_time": None,
                    "fill_count": "0", "remaining_count": format(quantity, "f"),
                    "fees_paid": "0", "entry_phase": "maker",
                    "status": "submitting", "submission_outcome": "unknown",
                    "submitted_at": utc_now(),
                }
                record["entry_orders"].append(order)
                self.note_entry_order_submitted(record, order, "maker")
            else:
                order.update(
                    status="submitting", submission_outcome="unknown",
                    submitted_at=utc_now(),
                )
            if rejected_attempts > 0:
                prior_retry = retry if isinstance(retry, dict) else {}
                record["entry_retry"] = {
                    **prior_retry,
                    "status": "SUBMITTING_RETRY",
                    "client_order_id": client_id,
                    "retry_ordinal": rejected_attempts,
                    "retry_submission_count": int(prior_retry.get("retry_submission_count", 0)) + 1,
                    "submission_intent_at": utc_now(),
                    "frozen_limit": format(maker_price, "f"),
                    "frozen_quantity": format(quantity, "f"),
                }
            self.audit(
                "maker_entry_submission_intent", ticker=record["ticker"], side=side,
                client_order_id=client_id, retry_ordinal=rejected_attempts,
                frozen_limit=format(maker_price, "f"), frozen_quantity=format(quantity, "f"),
                post_only=True, time_in_force=maker_tif,
                resumed_pre_submit_intent=resumed_pre_submit_intent,
            )
            response = await rest.create_order(
                ticker=record["ticker"], side=side, position_price=float(maker_price), quantity=float(quantity),
                tif=maker_tif, expiration_time=None, dry_run=False,
                order_key="signal-minus-offset-entry", post_only=True, client_order_id_override=client_id,
            )
            order.update(response)
            # The request ID is the persisted idempotency authority. A V2
            # response normally echoes it, but an absent/malformed echo must
            # never break restart linkage to the exact submitted intent.
            order["client_order_id"] = client_id
            order["entry_phase"] = "maker"
            if not order.get("order_id") and order.get("status") not in {"submit_failed", "paused", "direction_mismatch"}:
                order.update(status="submit_failed", submission_outcome="unknown")
            if order.get("status") in {"submit_failed", "paused", "direction_mismatch"} or not order.get("order_id"):
                if order.get("submission_outcome") == "not_submitted":
                    prior_retry = (
                        record.get("entry_retry")
                        if isinstance(record.get("entry_retry"), dict) else {}
                    )
                    record["entry_retry"] = {
                        **prior_retry,
                        "status": "RETRY_PENDING",
                        "attempt_count": rejected_attempts,
                        "last_deferred_at": utc_now(),
                        "next_attempt_epoch": now + ENTRY_REJECTION_RETRY_SECONDS,
                        "retry_interval_seconds": ENTRY_REJECTION_RETRY_SECONDS,
                        "client_order_id": client_id,
                        "frozen_limit": format(maker_price, "f"),
                        "frozen_quantity": format(quantity, "f"),
                        "reason": "exchange_pause_before_post",
                    }
                    self.audit(
                        "maker_entry_retry_deferred", ticker=record["ticker"],
                        side=side, next_attempt_epoch=record["entry_retry"]["next_attempt_epoch"],
                        frozen_limit=format(maker_price, "f"),
                        frozen_quantity=format(quantity, "f"),
                        reason="exchange_pause_before_post",
                    )
                    LOG.warning(
                        "ENTRY RETRY DEFERRED | ticker=%s side=%s frozen_limit=$%s "
                        "qty=%s retry_in=%.1fs reason=exchange_pause_before_post",
                        record["ticker"], side.upper(), format(maker_price, "f"),
                        format(quantity, "f"), ENTRY_REJECTION_RETRY_SECONDS,
                    )
                elif order.get("submission_outcome") == "rejected":
                    status = order.get("http_status")
                    record["entry_rejection"] = {
                        "at": utc_now(), "http_status": status,
                        "error_code": order.get("error_code"),
                        "error_type": order.get("error_type"),
                        "definitively_no_exchange_order": True,
                    }
                    self.audit(
                        "maker_entry_definitively_rejected", ticker=record["ticker"],
                        side=side, requested_price=format(maker_price, "f"),
                        requested_quantity=format(quantity, "f"), http_status=status,
                        error_code=order.get("error_code"),
                    )
                    # A market-scoped 400/404 definitively created no order.
                    # Keep the immutable price/side/quantity and try again
                    # after a short durable delay. Each attempt has a distinct
                    # deterministic ID. Authentication/schema failures are
                    # not hammered; they end only this market's entry while the
                    # long-running worker remains available for later markets.
                    if status in ENTRY_RETRYABLE_REJECTION_HTTP_STATUSES:
                        attempt_count = rejected_attempts + 1
                        prior_retry = record.get("entry_retry") if isinstance(record.get("entry_retry"), dict) else {}
                        record["entry_retry"] = {
                            **prior_retry,
                            "status": "RETRY_PENDING",
                            "attempt_count": attempt_count,
                            "last_rejected_at": utc_now(),
                            "last_http_status": status,
                            "last_error_code": order.get("error_code"),
                            "next_attempt_epoch": now + ENTRY_REJECTION_RETRY_SECONDS,
                            "retry_interval_seconds": ENTRY_REJECTION_RETRY_SECONDS,
                            "client_order_id": client_id,
                            "frozen_limit": format(maker_price, "f"),
                            "frozen_quantity": format(quantity, "f"),
                        }
                        self.audit(
                            "maker_entry_retry_scheduled", ticker=record["ticker"],
                            side=side, attempt_count=attempt_count,
                            next_attempt_epoch=record["entry_retry"]["next_attempt_epoch"],
                            retry_interval_seconds=ENTRY_REJECTION_RETRY_SECONDS,
                            frozen_limit=format(maker_price, "f"),
                            frozen_quantity=format(quantity, "f"),
                        )
                        LOG.warning(
                            "ENTRY RETRY SCHEDULED | ticker=%s side=%s attempt=%s "
                            "frozen_limit=$%s qty=%s retry_in=%.1fs abandon_if_ask<=%sc",
                            record["ticker"], side.upper(), attempt_count,
                            format(maker_price, "f"), format(quantity, "f"),
                            ENTRY_REJECTION_RETRY_SECONDS, ENTRY_RETRY_ABANDON_ASK_CENTS,
                        )
                    else:
                        prior_retry = record.get("entry_retry") if isinstance(record.get("entry_retry"), dict) else {}
                        record["entry_retry"] = {
                            **prior_retry,
                            "status": "NON_RETRYABLE_REJECTION",
                            "attempt_count": rejected_attempts + 1,
                            "last_rejected_at": utc_now(),
                            "last_http_status": status,
                            "last_error_code": order.get("error_code"),
                        }
                        self.finish_entry_attempt(
                            record, Decimal("0"), "maker_entry_non_retryable_rejection",
                        )
                else:
                    self.trip("maker_entry_submission_unknown")
                    self.transition(
                        record, "RECONCILIATION_PENDING",
                        "maker_entry_submission_unknown",
                    )
                return
        if self.dry_run:
            record["entry_orders"].append(order)
            self.note_entry_order_submitted(record, order, "maker")
        if isinstance(record.get("entry_retry"), dict):
            record["entry_retry"].update({
                "status": "ACCEPTED", "accepted_at": utc_now(),
                "accepted_client_order_id": client_id,
                "accepted_order_id": order.get("order_id"),
            })
        self.note_entry_execution_summary(record, "maker_limit_submitted")
        self.state["current_order_id"] = order.get("order_id")
        self.transition(record, "ENTRY_PENDING", "delayed_band_post_only_gtc_limit_submitted")
        self.audit(
            "entry_submitted", ticker=record["ticker"], side=side,
            initial_signal_price_cents=record["initial_signal_price_cents"],
            opening_selected_side_ask_cents=record.get("opening_price_reference", {}).get(
                "selected_side_ask_cents"
            ),
            requested_price_cents=record["entry_limit_cents"], requested_quantity=format(quantity, "f"),
            effective_position_cap=format(cap, "f"),
            position_cap_mode=record.get("position_cap_mode", "fixed"),
            opening_entry_cost=record.get("opening_entry_cost"),
            requested_entry_cost=record["requested_entry_cost"],
            client_order_id=client_id, exchange_order_id=order.get("order_id"), post_only=True,
            time_in_force=maker_tif, entry_order_lifetime=self.config["entry_order_lifetime"],
            shadow=self.dry_run,
        )

    async def submit_opposite_ladder_entries(
        self, rest: KalshiREST, feed: KalshiLiveFeed, record: dict[str, Any], now: float,
    ) -> None:
        """Create every missing v14 GTC maker order without duplicating a rung."""

        ladder = record.setdefault("opposite_ladder", {})
        if ladder.get("exit_latched") or now >= float(record["market_close_epoch"]):
            return
        plan = self.freeze_opposite_ladder_plan(feed, record, now)
        if not isinstance(plan, dict):
            return
        side = str(record["trade_side"])
        planned_orders = list(plan.get("orders") or [])
        if not ladder.get("funding_preflight_complete"):
            required = Decimal(str(plan["maximum_cash_required"]))
            if self.dry_run:
                exchange_index, available = None, self.shadow_available_cash()
            else:
                try:
                    exchange_index, available = await rest.market_entry_funding(record["ticker"])
                except Exception as exc:
                    ladder["funding_preflight"] = {
                        "at": utc_now(), "status": "UNAVAILABLE",
                        "error_type": type(exc).__name__, "required_cash": format(required, "f"),
                    }
                    return
            ladder["funding_preflight"] = {
                "at": utc_now(), "status": "PASS" if available >= required else "INSUFFICIENT",
                "exchange_index": exchange_index, "available_balance": format(available, "f"),
                "required_cash": format(required, "f"),
            }
            LOG.warning(
                "OPPOSITE LADDER FUNDING | ticker=%s shard=%s available=$%s maximum_required=$%s status=%s",
                record["ticker"], exchange_index, format(available, "f"), format(required, "f"),
                ladder["funding_preflight"]["status"],
            )
            if available < required:
                self.transition(record, "FUNDING_FAILURE", "insufficient_cash_for_complete_opposite_ladder")
                return
            if not self.dry_run:
                position = await rest.position_for_ticker(record["ticker"])
                if position is None:
                    ladder["funding_preflight"]["status"] = "POSITION_READ_UNAVAILABLE"
                    return
                if abs(Decimal(str(position))) > 0:
                    self.trip("existing_position_before_opposite_ladder")
                    return
            ladder["funding_preflight_complete"] = True
            self.audit("opposite_ladder_funding_passed", ticker=record["ticker"], **ladder["funding_preflight"])

        accepted = 0
        unresolved = False
        retry_pending = False
        for desired in planned_orders:
            role = str(desired["role"])
            attempts = [
                order for order in record.get("entry_orders", [])
                if isinstance(order, dict) and order.get("ladder_role") == role
            ]
            if any(order.get("order_id") or order.get("status") == "shadow_resting" for order in attempts):
                accepted += 1
                continue
            if any(
                order.get("submission_outcome") not in {"rejected", "not_submitted"}
                and order.get("status") in {"submitting", "submit_failed", "paused"}
                for order in attempts
            ):
                unresolved = True
                continue
            latest_attempt = attempts[-1] if attempts else None
            if (
                isinstance(latest_attempt, dict)
                and latest_attempt.get("submission_outcome") == "rejected"
                and now < float(latest_attempt.get("next_retry_epoch") or 0)
            ):
                retry_pending = True
                continue
            limit = cents_price(int(desired["price_cents"]))
            quantity = round_shares(Decimal(str(desired["quantity"])))
            fresh_quote, quote_state = feed.executable_shadow_quote(
                record["ticker"], side, 0.0, float(self.config["max_stale_quote_seconds"]),
            )
            fresh_ask = None
            if fresh_quote is not None:
                try:
                    fresh_ask = Decimal(str(fresh_quote["economic_price"]))
                except (ArithmeticError, KeyError, TypeError, ValueError):
                    fresh_ask = None
            if fresh_ask is None or fresh_ask <= limit:
                retry_pending = True
                ladder["last_submission_wait"] = {
                    "at": utc_now(), "role": role,
                    "reason": "fresh_book_unavailable" if fresh_ask is None else "post_only_would_cross",
                    "fresh_trade_side_ask": None if fresh_ask is None else format(fresh_ask, "f"),
                    "limit": format(limit, "f"), "quote_state": quote_state,
                }
                continue
            ordinal = len(attempts)
            client_id = deterministic_client_order_id(
                record["ticker"], side, f"opposite-ladder-{role}-{ordinal}", self.config,
            )
            intent = {
                "order_id": None, "client_order_id": client_id, "ticker": record["ticker"],
                "side": side, "quantity": format(quantity, "f"),
                "position_price": format(limit, "f"), "time_in_force": "good_till_canceled",
                "post_only": True, "reduce_only": False, "expiration_time": None,
                "fill_count": "0", "remaining_count": format(quantity, "f"),
                "fees_paid": "0", "entry_phase": "maker", "ladder_role": role,
                "status": "shadow_resting" if self.dry_run else "submitting",
                "submission_outcome": "accepted" if self.dry_run else "unknown",
                "submitted_at": utc_now(),
            }
            record.setdefault("entry_orders", []).append(intent)
            self.note_entry_order_submitted(record, intent, role)
            self.audit(
                "opposite_ladder_order_intent", ticker=record["ticker"], role=role,
                side=side, client_order_id=client_id, price=format(limit, "f"),
                quantity=format(quantity, "f"), attempt=ordinal, post_only=True,
                time_in_force="good_till_canceled",
            )
            if not self.dry_run:
                response = await rest.create_order(
                    ticker=record["ticker"], side=side, position_price=float(limit),
                    quantity=float(quantity), tif="good_till_canceled", expiration_time=None,
                    dry_run=False, order_key=f"opposite-ladder-{role}", post_only=True,
                    client_order_id_override=client_id,
                )
                intent.update(response)
                intent["client_order_id"] = client_id
                intent["entry_phase"] = "maker"
                intent["ladder_role"] = role
                if intent.get("order_id"):
                    intent["submission_outcome"] = "accepted"
                elif intent.get("submission_outcome") == "rejected":
                    intent.update({
                        "retryable": True,
                        "next_retry_epoch": now + ENTRY_REJECTION_RETRY_SECONDS,
                        "retry_interval_seconds": ENTRY_REJECTION_RETRY_SECONDS,
                    })
                    retry_pending = True
                    LOG.warning(
                        "OPPOSITE LADDER RETRY | ticker=%s role=%s price=%sc quantity=%s "
                        "http_status=%s error_code=%s next=bounded_market_retry",
                        record["ticker"], role, desired["price_cents"], desired["quantity"],
                        intent.get("http_status"), intent.get("error_code"),
                    )
                    continue
                else:
                    unresolved = True
                    # Never create a second ambiguous intent in the same
                    # pass. Reconcile this deterministic client ID first;
                    # later rungs are posted on the next safe pass.
                    break
            accepted += 1
            LOG.warning(
                "OPPOSITE LADDER ORDER %s | ticker=%s role=%s BUY=%s price=%sc quantity=%s "
                "tif=GTC post_only=true order_id=%s fill=%s remaining=%s",
                "SHADOW" if self.dry_run else "ACK", record["ticker"], role,
                side.upper(), desired["price_cents"], desired["quantity"],
                intent.get("order_id"), intent.get("fill_count"), intent.get("remaining_count"),
            )

        ladder.update({
            "state": "SUBMISSION_UNRESOLVED" if unresolved else "RESTING",
            "accepted_order_count": accepted,
            "planned_order_count": len(planned_orders),
            "retry_pending": retry_pending,
            "last_submission_pass_at": utc_now(),
        })
        self.state["current_order_id"] = next(
            (order.get("order_id") for order in record.get("entry_orders", []) if order.get("order_id")),
            None,
        )
        if unresolved:
            self.trip("opposite_ladder_submission_unknown")
            self.transition(record, "RECONCILIATION_PENDING", "opposite_ladder_submission_unknown")
        elif accepted or retry_pending:
            self.transition(record, "ENTRY_PENDING", "opposite_ladder_gtc_orders_managed")

    async def submit_entry(self, rest: KalshiREST, feed: KalshiLiveFeed, record: dict[str, Any], now: float) -> None:
        if self.config["entry_execution_mode"] == "opposite_side_doubling_ladder":
            if record.get("status") in {"SIGNAL_PENDING", "ENTRY_PENDING", "ENTRY_PARTIAL", "POSITION_OPEN"}:
                await self.submit_opposite_ladder_entries(rest, feed, record, now)
            return
        if record.get("status") != "SIGNAL_PENDING":
            return
        # Opening-price evidence is observational and remains active even
        # while a persistent circuit breaker correctly blocks new exposure.
        # This keeps the shadow ledger useful without weakening the breaker.
        self.capture_opening_price_reference(feed, record, now)
        self.capture_opening_quote(feed, record, now)
        if not self.circuit_allows_entry():
            return
        if now >= float(record["market_close_epoch"]):
            self.finish_entry_attempt(record, Decimal("0"), "market_closed_before_delayed_entry")
            return
        # Current config validation permits only v13.  The v11 name remains
        # reachable for pinned regression fixtures that instantiate LiveEngine
        # directly; it cannot be loaded by a production worker.
        if self.config["entry_execution_mode"] not in {
            "delayed_threshold_band_maker", "signal_price_minus_offset_maker",
        }:
            self.trip("unsupported_entry_execution_mode")
            return
        await self.submit_signal_maker_entry(rest, feed, record, now)

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
        if now >= float(record["market_close_epoch"]):
            if not await self.cancel_entry_orders_and_confirm(rest, record, next_action="finish_entry"):
                return
            filled = filled if self.dry_run else await self.refresh_entry(rest, record)
            self.finish_entry_attempt(record, filled, "gtc_entry_canceled_at_market_close")

    def exit_filled_quantity(self, record: dict[str, Any]) -> Decimal:
        return sum(
            (Decimal(str(order.get("fill_count") or "0")) for order in record.get("exit_orders", [])),
            Decimal("0"),
        )

    def local_remaining_position(self, record: dict[str, Any]) -> Decimal:
        opened = Decimal(str(record.get("actual_quantity") or "0"))
        return round_shares(max(Decimal("0"), opened - self.exit_filled_quantity(record)))

    @staticmethod
    def hybrid_maker_exit_order(record: dict[str, Any]) -> dict[str, Any] | None:
        return next(
            (order for order in record.get("exit_orders", []) if order.get("exit_phase") == "maker_exit"),
            None,
        )

    def refresh_shadow_maker_exit(self, feed: KalshiLiveFeed, record: dict[str, Any]) -> Decimal:
        """Fill a shadow maker sale only against a later executable bid/depth.

        This is deterministic and conservative: a mere last price or ask touch
        is insufficient, the quote must be a fresh complete book received
        after submission with selected-side bid >= the resting sale limit.
        """

        order = self.hybrid_maker_exit_order(record)
        if not isinstance(order, dict):
            return Decimal("0")
        requested = Decimal(str(order.get("quantity") or "0"))
        previous = Decimal(str(order.get("fill_count") or "0"))
        remaining = max(Decimal("0"), requested - previous)
        if remaining == 0 or str(order.get("status")) in {"shadow_canceled", "canceled"}:
            return previous
        quote, _ = feed.executable_shadow_exit_quote(
            record["ticker"], str(record["signal_side"]), 0.0,
            float(self.config["max_stale_quote_seconds"]),
        )
        if quote is None:
            return previous
        submitted_epoch = _iso_epoch(str(order.get("submitted_at") or ""))
        quote_epoch = executable_quote_epoch(quote)
        if submitted_epoch is None or quote_epoch is None or quote_epoch <= submitted_epoch:
            return previous
        bid = Decimal(str(quote["economic_price"]))
        limit = Decimal(str(order["position_price"]))
        if bid < limit:
            return previous
        delta = round_shares(min(remaining, Decimal(str(quote.get("displayed_depth") or "0"))))
        if delta <= 0:
            return previous
        filled = round_shares(previous + delta)
        order.update({
            "fill_count": format(filled, "f"), "remaining_count": format(round_shares(requested - filled), "f"),
            "average_fill_price": format(limit, "f"), "fees_paid": "0",
            "status": "shadow_filled" if filled == requested else "shadow_partially_filled",
            "shadow_fill_evidence": {
                "model": "later_fresh_executable_bid_with_displayed_depth", "quote": quote,
                "filled_on_this_quote": format(delta, "f"),
            },
        })
        record["hybrid_stop"]["maker_filled_quantity"] = format(filled, "f")
        self.audit(
            "hybrid_maker_exit_fill", ticker=record["ticker"], filled_quantity=format(filled, "f"),
            remaining_quantity=order["remaining_count"], price=order["average_fill_price"], shadow=True,
        )
        return filled

    async def start_hybrid_maker_exit(
        self, rest: KalshiREST, feed: KalshiLiveFeed, record: dict[str, Any], executable_bid: Decimal,
        *, entries_confirmed: bool = False,
    ) -> None:
        if self.hybrid_maker_exit_order(record) is not None:
            return
        # Latch the protective-exit obligation before attempting to cancel a
        # still-resting entry.  A cancel timeout/404 must not erase the fact
        # that the executable bid crossed the trigger, and a later price
        # rebound or process restart must not return this exposure to a normal
        # hold-to-settlement path.
        hybrid = record.setdefault("hybrid_stop", {})
        hybrid["stop_exit_latched"] = True
        trigger = cents_price(int(hybrid["trigger_cents"]))
        self.note_stop_trigger(
            record, executable_bid, trigger, self.local_remaining_position(record), shadow=self.dry_run,
        )
        if not entries_confirmed and not await self.cancel_entry_orders_and_confirm(
            rest, record, next_action="stop", executable_bid=executable_bid,
        ):
            return
        side = str(record["signal_side"])
        quantity = self.local_remaining_position(record)
        if not self.dry_run:
            position = await rest.position_for_ticker(record["ticker"])
            if position is None:
                self.trip("hybrid_stop_position_reconciliation_failed")
                return
            signed = Decimal(str(position))
            if (side == "yes" and signed < 0) or (side == "no" and signed > 0):
                self.trip("hybrid_stop_position_direction_mismatch")
                return
            quantity = abs(signed)
            previous_quantity = Decimal(str(record.get("actual_quantity") or "0"))
            record["actual_quantity"] = format(quantity, "f")
            self.state["current_position"] = record["actual_quantity"]
            self.note_entry_fill_observed(record, previous_quantity, quantity, "stop_entry_cancel_position_reconciliation")
            self.note_entry_execution_summary(record, "stop_entry_cancel_position_reconciliation")
        if quantity <= 0:
            return
        maker_price = cents_price(int(record["hybrid_stop"]["maker_exit_cents"]))
        self.note_stop_trigger(record, executable_bid, trigger, quantity, shadow=self.dry_run)
        client_id = deterministic_client_order_id(record["ticker"], side, "hybrid-maker-exit", self.config)
        maker_tif = str(self.config["maker_order_time_in_force"])
        if self.dry_run:
            order = {
                "order_id": None, "client_order_id": client_id, "ticker": record["ticker"],
                "side": side, "held_side": side, "order_type": "reduce_only_exit_maker",
                "exit_phase": "maker_exit", "position_price": format(maker_price, "f"),
                "quantity": format(quantity, "f"), "fill_count": "0.00", "remaining_count": format(quantity, "f"),
                "average_fill_price": None, "fees_paid": "0", "time_in_force": maker_tif,
                "post_only": True, "reduce_only": True, "status": "shadow_resting", "submitted_at": utc_now(),
            }
        else:
            intent = self.persist_exit_intent(record, client_id, "maker_exit", quantity, maker_price)
            order = await rest.create_reduce_only_maker_exit(
                ticker=record["ticker"], held_side=side, economic_exit_price=float(maker_price),
                quantity=float(quantity), expiration_time=int(float(record["market_close_epoch"])), dry_run=False,
                order_key="hybrid-maker-exit",
                client_order_id_override=client_id,
            )
            intent.update(order)
            if order.get("order_id") and order.get("status") not in {"submit_failed", "paused"}:
                intent["submission_outcome"] = "accepted"
            order = intent
            if order.get("time_in_force") != maker_tif:
                self.trip("hybrid_maker_exit_time_in_force_mismatch")
                self.transition(record, "RECONCILIATION_PENDING", "hybrid_maker_exit_time_in_force_mismatch")
                return
            if (order.get("status") in {"submit_failed", "paused"}
                    and order.get("submission_outcome") not in {"rejected", "not_submitted"}):
                record["hybrid_stop"]["maker_order_id"] = order.get("order_id")
                self.trip("hybrid_maker_exit_submission_unknown")
                self.transition(record, "MAKER_EXIT_CANCEL_UNCONFIRMED", "hybrid_maker_exit_submission_unknown")
                return
        if self.dry_run:
            record.setdefault("exit_orders", []).append(order)
        record["hybrid_stop"].update({
            "state": "MAKER_EXIT_PENDING", "triggered_at": record.get("stop_trigger", {}).get("at"),
            "maker_order_id": order.get("order_id"), "maker_client_order_id": client_id,
        })
        self.note_stop_exit_submitted(record, order)
        self.transition(record, "MAKER_EXIT_PENDING", "hybrid_trigger_maker_exit_submitted")
        self.audit(
            "hybrid_maker_exit_submitted", ticker=record["ticker"], side=side,
            trigger_bid=format(executable_bid, "f"), maker_exit_price=format(maker_price, "f"),
            quantity=format(quantity, "f"), client_order_id=client_id,
            exchange_order_id=order.get("order_id"), shadow=self.dry_run,
        )

    def persist_exit_intent(
        self, record: dict[str, Any], client_id: str, phase: str,
        quantity: Decimal, price: Decimal,
    ) -> dict[str, Any]:
        """Fsync the order identity BEFORE the POST; restart cannot invent a retry."""
        maker = phase == "maker_exit"
        intent = {
            "client_order_id": client_id, "order_id": None, "ticker": record["ticker"],
            "side": record["signal_side"], "held_side": record["signal_side"],
            "exit_phase": phase, "quantity": format(quantity, "f"),
            "position_price": format(price, "f"), "fill_count": "0.00",
            "remaining_count": format(quantity, "f"), "fees_paid": "0",
            "time_in_force": "good_till_canceled" if maker else "immediate_or_cancel",
            "post_only": maker, "reduce_only": True, "status": "submitting",
            "submission_outcome": "unknown", "submitted_at": utc_now(),
        }
        record.setdefault("exit_orders", []).append(intent)
        self.transition(record, "MAKER_EXIT_PENDING" if maker else "HARD_STOP_PENDING", "exit_submission_intent")
        self.audit("exit_submission_intent", ticker=record["ticker"], client_order_id=client_id,
                   phase=phase, quantity=format(quantity, "f"), price=format(price, "f"))
        return intent

    async def cancel_hybrid_maker_exit_and_confirm(self, rest: KalshiREST, record: dict[str, Any]) -> bool:
        order = self.hybrid_maker_exit_order(record)
        if not isinstance(order, dict):
            return True
        if order.get("submission_outcome") in {"rejected", "not_submitted"}:
            return True
        if not self.dry_run and not order.get("order_id"):
            if not await rest.recover_exit_submission(order):
                self.trip("hybrid_maker_exit_submission_unresolved")
                self.transition(record, "MAKER_EXIT_CANCEL_UNCONFIRMED", "hybrid_maker_exit_submission_unresolved")
                return False
        if Decimal(str(order.get("remaining_count") or "0")) <= 0:
            return True
        if self.dry_run:
            order["remaining_count"] = "0.00"
            order["status"] = "shadow_canceled"
            order["canceled_at"] = utc_now()
            return True
        if await rest.refresh_exit_order(order) is False:
            return False
        if Decimal(str(order.get("remaining_count") or "0")) <= 0:
            return True
        acknowledged = await rest.cancel_order(order, False)
        if acknowledged:
            # Capture the cancellation response and then the final exchange
            # history view before calculating the residual hard-stop size.
            if await rest.refresh_exit_order(order) is False:
                acknowledged = False
        if not acknowledged:
            acknowledged = await self.reconcile_failed_maker_exit_cancellation(rest, record, order)
        if not acknowledged or Decimal(str(order.get("remaining_count") or "0")) > 0:
            record["hybrid_stop"]["state"] = "MAKER_EXIT_CANCEL_UNCONFIRMED"
            self.trip("hybrid_maker_exit_cancellation_unconfirmed")
            self.transition(record, "MAKER_EXIT_CANCEL_UNCONFIRMED", "hybrid_maker_exit_cancellation_unconfirmed")
            return False
        return True

    async def submit_hybrid_hard_stop(
        self, rest: KalshiREST, feed: KalshiLiveFeed, record: dict[str, Any], executable_bid: Decimal,
    ) -> None:
        hybrid = record.setdefault("hybrid_stop", {})
        hybrid["stop_exit_latched"] = True
        hybrid["hard_stop_latched"] = True
        if not await self.cancel_hybrid_maker_exit_and_confirm(rest, record):
            return
        prior_hard_orders = [order for order in record.get("exit_orders", []) if order.get("exit_phase") == "hard_stop"]
        if not self.dry_run:
            for previous in prior_hard_orders:
                if previous.get("submission_outcome") in {"rejected", "not_submitted"}:
                    continue
                accepted_ioc = (
                    previous.get("submission_outcome") == "accepted"
                    and previous.get("order_id")
                    and previous.get("time_in_force", "immediate_or_cancel") == "immediate_or_cancel"
                )
                if accepted_ioc:
                    # An acknowledged IOC can never remain on the book.  A
                    # positive V2 ``remaining_count`` is the unfilled portion
                    # canceled by that IOC, not a resting order. Refresh is
                    # best-effort; the authoritative position below determines
                    # the next exact residual and reduce_only prevents a flip.
                    await rest.refresh_exit_order(previous)
                    continue
                if not previous.get("order_id") or previous.get("status") in {"submitting", "submit_failed", "paused"}:
                    confirmed = await rest.recover_exit_submission(previous)
                else:
                    confirmed = await rest.refresh_exit_order(previous)
                if confirmed is False:
                    self.trip("hard_stop_previous_submission_unresolved")
                    self.checkpoint("hard_stop_previous_submission_unresolved")
                    return
        side = str(record["signal_side"])
        maker_order = self.hybrid_maker_exit_order(record)
        maker_filled = Decimal(str((maker_order or {}).get("fill_count") or "0"))
        record["hybrid_stop"]["maker_filled_quantity"] = format(maker_filled, "f")
        residual = self.local_remaining_position(record)
        quote: dict[str, Any] | None = None
        if self.dry_run:
            quote, _ = feed.executable_shadow_exit_quote(
                record["ticker"], side, 0.0, float(self.config["max_stale_quote_seconds"]),
            )
            if quote is None:
                return
            executable_bid = Decimal(str(quote["economic_price"]))
        else:
            position = await rest.position_for_ticker(record["ticker"])
            if position is None:
                self.trip("hard_stop_position_reconciliation_failed")
                return
            signed = Decimal(str(position))
            if (side == "yes" and signed < 0) or (side == "no" and signed > 0):
                self.trip("hard_stop_position_direction_mismatch")
                return
            residual = abs(signed)
        if residual <= 0:
            direct = record.get("stop_policy", self.config["stop_policy"]) in {
                "direct_ioc_at_trigger", "opposite_side_take_profit_ioc",
            }
            classification = (
                "DIRECT_PROTECTIVE_EXIT" if direct else
                (("MAKER_EXIT_PARTIAL_THEN_HARD_STOP" if maker_filled > 0
                  else "HARD_STOP_ONLY") if prior_hard_orders else "MAKER_EXIT_FULL")
            )
            record["exit_classification"] = classification
            record["hybrid_stop"]["state"] = classification
            await self.finalize_stop(record)
            return
        direct = record.get("stop_policy", self.config["stop_policy"]) in {
            "direct_ioc_at_trigger", "opposite_side_take_profit_ioc",
        }
        exit_key = "direct-protective-exit" if direct else "hybrid-hard-stop"
        client_id = deterministic_client_order_id(
            record["ticker"], side, f"{exit_key}-{len(prior_hard_orders)}", self.config,
        )
        if self.dry_run:
            displayed = Decimal(str((quote or {}).get("displayed_depth") or "0"))
            filled = round_shares(min(residual, displayed))
            order = {
                "order_id": None, "client_order_id": client_id, "ticker": record["ticker"], "side": side,
                "held_side": side, "order_type": "reduce_only_exit_ioc", "exit_phase": "hard_stop",
                "position_price": format(executable_bid, "f"), "quantity": format(residual, "f"),
                "fill_count": format(filled, "f"), "remaining_count": format(round_shares(residual - filled), "f"),
                "average_fill_price": format(executable_bid, "f"), "fees_paid": "0",
                "time_in_force": "immediate_or_cancel", "post_only": False, "reduce_only": True,
                "status": "shadow_filled" if filled == residual else "shadow_partial_or_unfilled",
                "shadow_execution": "fresh_executable_bid_with_displayed_depth", "shadow_quote": quote,
                "submitted_at": utc_now(),
            }
        else:
            intent = self.persist_exit_intent(record, client_id, "hard_stop", residual, executable_bid)
            order = await rest.create_reduce_only_exit(
                ticker=record["ticker"], held_side=side, economic_exit_price=float(executable_bid),
                quantity=float(residual), dry_run=False, order_key=f"{exit_key}-{len(prior_hard_orders)}",
                client_order_id_override=client_id,
            )
            order["exit_phase"] = "hard_stop"
            intent.update(order)
            if order.get("order_id") and order.get("status") not in {"submit_failed", "paused"}:
                intent["submission_outcome"] = "accepted"
            order = intent
        if self.dry_run:
            record.setdefault("exit_orders", []).append(order)
        self.note_stop_exit_submitted(record, order)
        hard_filled = sum(
            (Decimal(str(item.get("fill_count") or "0")) for item in record["exit_orders"] if item.get("exit_phase") == "hard_stop"),
            Decimal("0"),
        )
        record["hybrid_stop"].update({"state": "HARD_STOP_PENDING", "hard_filled_quantity": format(hard_filled, "f")})
        self.transition(
            record, "HARD_STOP_PENDING",
            "direct_protective_exit_submitted" if direct else "hybrid_hard_stop_submitted",
        )
        self.audit(
            "direct_protective_exit_submitted" if direct else "hybrid_hard_stop_submitted",
            ticker=record["ticker"], side=side,
            executable_bid=format(executable_bid, "f"), requested_residual=format(residual, "f"),
            filled_quantity=order.get("fill_count"), maker_filled_quantity=format(maker_filled, "f"),
            client_order_id=client_id, exchange_order_id=order.get("order_id"), shadow=self.dry_run,
        )
        if self.dry_run:
            remaining = self.local_remaining_position(record)
        else:
            refreshed = await rest.position_for_ticker(record["ticker"])
            remaining = abs(Decimal(str(refreshed))) if refreshed is not None else residual
        if remaining <= 0:
            classification = (
                "DIRECT_PROTECTIVE_EXIT"
                if record.get("stop_policy", self.config["stop_policy"]) in {
                    "direct_ioc_at_trigger", "opposite_side_take_profit_ioc",
                }
                else "MAKER_EXIT_PARTIAL_THEN_HARD_STOP" if maker_filled > 0 else "HARD_STOP_ONLY"
            )
            record["exit_classification"] = classification
            record["hybrid_stop"]["state"] = classification
            await self.finalize_stop(record)

    async def close_at_stop(
        self, rest: KalshiREST, record: dict[str, Any], executable_bid: Decimal, *, entries_confirmed: bool = False,
    ) -> None:
        """Compatibility wrapper: v11 hard-closes only via the hybrid state machine."""

        if not entries_confirmed and not await self.cancel_entry_orders_and_confirm(
            rest, record, next_action="stop", executable_bid=executable_bid,
        ):
            return
        # The feed is required for conservative shadow depth. Normal v13
        # execution calls ``submit_hybrid_hard_stop`` directly from manage_stop.
        if self.dry_run:
            raise RuntimeError("shadow hybrid hard stop requires the live feed adapter")

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
        opposite_ladder = self.config["entry_execution_mode"] == "opposite_side_doubling_ladder"
        parameters = self.current_parameters() if opposite_ladder else self.record_parameters(record)
        before = dict(self.state.get("sizing") or {})
        if opposite_ladder:
            base = format(round_shares(Decimal(self.config["starting_base"])), "f")
            after = {
                "base_share_count": base,
                "recovery_exponent": 0,
                "recovery_cycle_pnl": "0",
                "profit_since_last_base_scale": "0",
                "next_base_threshold": self.config["first_base_threshold"],
                "completed_trade_count": int(before.get("completed_trade_count", 0)) + 1,
            }
            changes = {"recovery_reset": False, "base_increased": False}
        else:
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
            count_key = (
                "stop_count" if method in {"stop", "opposite_take_profit"}
                else "settlement_count"
            )
            metrics[count_key] = int(metrics[count_key]) + 1
        append_unique(self.state["processed_settlements"], settlement_id)
        record.update({
            "realized_net_pnl": format(net, "f"), "realized_method": method, "completed_at": utc_now(),
            "recovery_cycle_pnl_after": after["recovery_cycle_pnl"], "recovery_exponent_after": after["recovery_exponent"],
            "base_after": after["base_share_count"], "next_base_threshold_after": after["next_base_threshold"],
        })
        if not opposite_ladder:
            record["effective_position_cap_after"] = format(
                self.current_parameters().effective_max_position(after["base_share_count"]), "f",
            )
        self.realized_performance_metrics()
        self.transition(record, "CLOSED", method)
        if self.state.get("active_market") == record["ticker"]:
            self.state["active_market"] = None
        self.state.update({"current_order_id": None, "current_position": "0.00", "average_entry": None, "last_completed_trade": record["ticker"]})
        close_audit = {
            "ticker": record["ticker"], "method": method, "net_pnl": format(net, "f"),
            "quantity": record.get("actual_quantity"), "recovery_reset": changes["recovery_reset"],
            "base_increased": changes["base_increased"],
            "entry_execution_type": record["entry_execution_type"],
            "entry_execution_summary": record["entry_execution_summary"],
        }
        if not opposite_ladder:
            close_audit.update({
                "effective_position_cap_before": record.get("effective_position_cap"),
                "effective_position_cap_after": record.get("effective_position_cap_after"),
            })
        self.audit("trade_closed", **close_audit)
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
        if self.local_remaining_position(record) > 0:
            return
        self.note_stop_position_closed(record)
        proceeds, exit_fees = self.exit_proceeds(record)
        net = proceeds - exit_fees - self.entry_cost(record)
        method = "opposite_take_profit" if record.get("exit_purpose") == "opposite_take_profit" else "stop"
        self.record_realized(record, net, method, f"{record['ticker']}:{method}")

    async def manage_stop(self, rest: KalshiREST, feed: KalshiLiveFeed, record: dict[str, Any]) -> None:
        if record.get("entry_execution_mode") == "opposite_side_doubling_ladder":
            await self.manage_opposite_take_profit(rest, feed, record)
            return
        if record.get("status") == "ENTRY_CANCEL_UNCONFIRMED":
            pending = record.get("entry_cancel_pending")
            if isinstance(pending, dict) and pending.get("next_action") == "stop":
                await self.resume_unconfirmed_entry_cancellation(rest, feed, record)
            return
        await self.manage_legacy_stop(rest, feed, record)

    async def manage_opposite_take_profit(
        self, rest: KalshiREST, feed: KalshiLiveFeed, record: dict[str, Any],
    ) -> None:
        """Cancel the ladder and flatten fills at the two-sided 51c boundary.

        Either executable boundary latches the exit: traded-side BID >=51c or
        sticky-side ASK <=51c. The two observations are intentionally
        independent at 51c. The opposite contract merely starting below 51c
        does not trigger an exit. Once latched it never
        rearms; residual reduce-only IOC attempts use the latest executable
        bid and can never reverse the position.
        """

        ladder = record.setdefault("opposite_ladder", {})
        status = str(record.get("status") or "")
        if status not in {
            "ENTRY_PENDING", "ENTRY_PARTIAL", "POSITION_OPEN",
            "ENTRY_CANCEL_UNCONFIRMED", "HARD_STOP_PENDING",
        }:
            return
        if status == "ENTRY_CANCEL_UNCONFIRMED":
            pending = record.get("entry_cancel_pending")
            if isinstance(pending, dict) and pending.get("next_action") == "stop":
                await self.resume_unconfirmed_entry_cancellation(rest, feed, record)
            return
        bid = self.selected_quote(feed, record["ticker"], str(record["trade_side"]), "bid")
        sticky_ask = self.selected_quote(
            feed, record["ticker"], str(record["sticky_signal_side"]), "ask",
        )
        threshold_cents = int(self.config["opposite_take_profit_cents"])
        bid_cents = None
        sticky_ask_cents = None
        if bid is not None:
            try:
                bid_cents = price_to_cents(bid, "opposite-side executable bid")
            except ValueError:
                bid_cents = None
        if sticky_ask is not None:
            try:
                sticky_ask_cents = price_to_cents(
                    sticky_ask, "sticky-side executable ask",
                )
            except ValueError:
                sticky_ask_cents = None
        if not ladder.get("exit_latched"):
            if not take_profit_triggered(
                bid_cents, threshold_cents,
                sticky_side_ask_cents=sticky_ask_cents,
            ):
                return
            ladder.update({
                "exit_latched": True, "state": "TAKE_PROFIT_LATCHED",
                "take_profit_triggered_at": utc_now(),
                "trigger_executable_bid_cents": bid_cents,
                "trigger_sticky_ask_cents": sticky_ask_cents,
            })
            record["exit_purpose"] = "opposite_take_profit"
            record.setdefault("hybrid_stop", {}).update({
                "stop_exit_latched": True, "hard_stop_latched": True,
                "triggered_at": utc_now(), "state": "TAKE_PROFIT_LATCHED",
            })
            self.audit(
                "opposite_take_profit_latched", ticker=record["ticker"],
                trade_side=record["trade_side"],
                executable_bid=None if bid is None else format(bid, "f"),
                sticky_side=record["sticky_signal_side"],
                sticky_executable_ask=(
                    None if sticky_ask is None else format(sticky_ask, "f")
                ),
                threshold_cents=threshold_cents,
            )
            LOG.warning(
                "OPPOSITE FLATTEN LATCHED | ticker=%s held_side=%s executable_bid=%sc "
                "sticky_side=%s executable_ask=%sc threshold=%sc "
                "action=cancel_all_ladder_orders_then_flatten_actual_position",
                record["ticker"], str(record["trade_side"]).upper(), bid_cents,
                str(record["sticky_signal_side"]).upper(), sticky_ask_cents,
                threshold_cents,
            )
        if not await self.cancel_entry_orders_and_confirm(
            rest, record, next_action="stop", executable_bid=bid,
        ):
            ladder["state"] = "ENTRY_CANCELLATION_PENDING"
            return
        filled = self.refresh_shadow_entry(feed, record) if self.dry_run else await self.refresh_entry(rest, record)
        if not self.dry_run:
            position = await rest.position_for_ticker(record["ticker"])
            if position is None:
                self.trip("opposite_take_profit_position_reconciliation_failed")
                return
            filled = max(filled, abs(Decimal(str(position))))
            record["actual_quantity"] = format(filled, "f")
            self.state["current_position"] = record["actual_quantity"]
        if filled <= 0:
            ladder["state"] = "CLOSED_NO_FILL"
            self.finish_entry_attempt(record, Decimal("0"), "opposite_take_profit_before_any_fill")
            return
        if bid is None:
            return
        ladder["state"] = "TAKE_PROFIT_EXIT_PENDING"
        await self.submit_hybrid_hard_stop(rest, feed, record, bid)

    async def manage_legacy_stop(
        self, rest: KalshiREST, feed: KalshiLiveFeed, record: dict[str, Any],
    ) -> None:
        active_stop_states = {
            "ENTRY_PARTIAL", "POSITION_OPEN", "MAKER_EXIT_PENDING", "MAKER_EXIT_PARTIAL",
            "MAKER_EXIT_CANCEL_UNCONFIRMED", "HARD_STOP_PENDING",
        }
        if record.get("status") not in active_stop_states:
            return
        self.update_effective_stop_price(record)
        bid = self.selected_quote(feed, record["ticker"], str(record["signal_side"]), "bid")
        trigger = cents_price(int(record.get("hybrid_stop", {}).get("trigger_cents") or self.config["hybrid_stop_trigger_cents"]))
        hard = cents_price(int(record.get("hybrid_stop", {}).get("hard_stop_cents") or self.config["hybrid_hard_stop_cents"]))
        self.note_stop_monitor_quote(record, bid, trigger)
        status = str(record.get("status"))
        maker_order = self.hybrid_maker_exit_order(record)
        direct_exit = record.get("stop_policy", self.config["stop_policy"]) == "direct_ioc_at_trigger"
        if direct_exit:
            if status in {"ENTRY_PARTIAL", "POSITION_OPEN"} and bid is not None and (
                bid <= trigger or record.get("hybrid_stop", {}).get("stop_exit_latched")
            ):
                record["hybrid_stop"]["stop_exit_latched"] = True
                record["hybrid_stop"]["hard_stop_latched"] = True
                self.note_stop_trigger(
                    record, bid, trigger, self.local_remaining_position(record), shadow=self.dry_run,
                )
                if not await self.cancel_entry_orders_and_confirm(
                    rest, record, next_action="stop", executable_bid=bid,
                ):
                    return
                if not self.dry_run:
                    await self.refresh_entry(rest, record)
                await self.submit_hybrid_hard_stop(rest, feed, record, bid)
                return
            if status == "HARD_STOP_PENDING" or record.get("hybrid_stop", {}).get("stop_exit_latched"):
                if bid is not None:
                    await self.submit_hybrid_hard_stop(rest, feed, record, bid)
                return
        if status in {"ENTRY_PARTIAL", "POSITION_OPEN"}:
            if bid is not None and (bid <= hard or record.get("hybrid_stop", {}).get("hard_stop_latched")):
                record["hybrid_stop"]["stop_exit_latched"] = True
                record["hybrid_stop"]["hard_stop_latched"] = True
                self.note_stop_trigger(record, bid, trigger, self.local_remaining_position(record), shadow=self.dry_run)
                if not await self.cancel_entry_orders_and_confirm(rest, record, next_action="stop", executable_bid=bid):
                    return
                if not self.dry_run:
                    await self.refresh_entry(rest, record)
                await self.submit_hybrid_hard_stop(rest, feed, record, bid)
            elif bid is not None and (
                bid <= trigger or record.get("hybrid_stop", {}).get("stop_exit_latched")
            ):
                await self.start_hybrid_maker_exit(rest, feed, record, bid)
            return
        if maker_order is not None:
            if self.dry_run:
                self.refresh_shadow_maker_exit(feed, record)
            elif (not maker_order.get("order_id")
                  and maker_order.get("submission_outcome") not in {"rejected", "not_submitted"}):
                # Recovery is needed even if prices rebound: the lost maker
                # response may already represent a fully filled protective exit.
                await rest.recover_exit_submission(maker_order)
            else:
                await rest.refresh_exit_order(maker_order)
            maker_filled = Decimal(str(maker_order.get("fill_count") or "0"))
            requested = Decimal(str(maker_order.get("quantity") or "0"))
            record["hybrid_stop"]["maker_filled_quantity"] = format(maker_filled, "f")
            if maker_filled >= requested and requested > 0:
                if not self.dry_run:
                    position = await rest.position_for_ticker(record["ticker"])
                    if position is None:
                        self.trip("maker_exit_flatten_reconciliation_failed")
                        return
                    if abs(Decimal(str(position))) > 0:
                        # The order response and position endpoint disagree.
                        # Preserve stop management; never book a close early.
                        self.trip("maker_exit_reported_full_but_position_remains")
                        return
                record["exit_classification"] = "MAKER_EXIT_FULL"
                record["hybrid_stop"]["state"] = "MAKER_EXIT_FULL"
                await self.finalize_stop(record)
                return
            if maker_filled > 0 and status == "MAKER_EXIT_PENDING":
                self.transition(record, "MAKER_EXIT_PARTIAL", "hybrid_maker_exit_partial_fill")
        if bid is not None and (bid <= hard or record.get("hybrid_stop", {}).get("hard_stop_latched")
                                or status == "HARD_STOP_PENDING"):
            await self.submit_hybrid_hard_stop(rest, feed, record, bid)
            return

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
        self.finalize_settlement_analytics(record, outcome)
        self.realized_performance_metrics()
        self.audit(
            "post_stop_settlement_verified", ticker=record["ticker"], outcome=outcome,
            would_have_settled_correctly=would_have_won,
            actual_average_entry_price=record.get("actual_average_entry_price"),
            effective_stop_price=record.get("effective_stop_price"),
        )
        self.checkpoint("post_stop_settlement_verified")

    def finalize_settlement_analytics(self, record: dict[str, Any], outcome: str) -> None:
        """Finalize execution-independent path analytics exactly once."""

        if record.get("analytics_settlement_finalized"):
            return
        winner = outcome == record.get("signal_side")
        initial_raw = record.get("initial_signal_price_cents")
        minimum_raw = record.get("minimum_selected_price_cents")
        initial = int(initial_raw) if initial_raw is not None else None
        minimum = int(minimum_raw) if minimum_raw is not None else initial
        drawdown = max(0, initial - minimum) if initial is not None and minimum is not None else None
        stopped = record.get("realized_method") == "stop"
        stopped_then_winner = bool(stopped and winner)
        record.update({
            "settlement_outcome": outcome, "directional_settlement_winner": winner,
            "winner_max_drawdown_cents": drawdown if winner else None,
            "stopped_then_eventual_winner": stopped_then_winner,
            "analytics_settlement_finalized": True, "analytics_settlement_finalized_at": utc_now(),
        })
        delayed = record.get("delayed_entry_tracking")
        if isinstance(delayed, dict) and delayed.get("eligible"):
            delayed.update({
                "settlement_outcome": outcome,
                "directional_winner": winner,
                "settlement_finalized_at": utc_now(),
            })
            entry_cents = delayed.get("first_entry_price_cents")
            contract_version = int(delayed.get("ladder_contract_version") or 1)
            direct_order = (
                delayed.get("ladder", {}).get("direct_entry_order")
                if isinstance(delayed.get("ladder"), dict) else None
            )
            if contract_version >= 2 and delayed.get("threshold_reached"):
                filled_quantity = Decimal(str(
                    direct_order.get("simulated_filled_quantity") or "0"
                    if isinstance(direct_order, dict) else "0"
                ))
                if filled_quantity > 0 and entry_cents is not None:
                    entry_price = cents_price(int(entry_cents))
                    gross_per_share = Decimal("1") - entry_price if winner else -entry_price
                    delayed.update({
                        "status": "SETTLED_WIN" if winner else "SETTLED_LOSS",
                        "zero_fill": False,
                        "no_stop_gross_pnl_per_share": format(gross_per_share, "f"),
                        "no_stop_gross_pnl": format(filled_quantity * gross_per_share, "f"),
                    })
                    if isinstance(direct_order, dict):
                        direct_order["status"] = "SETTLED"
                else:
                    delayed.update({
                        "status": "ENTRY_NOT_FILLED",
                        "zero_fill": True,
                        "hypothetical_fill": False,
                        "no_stop_gross_pnl_per_share": None,
                        "no_stop_gross_pnl": "0",
                    })
                    if isinstance(direct_order, dict):
                        direct_order["status"] = "EXPIRED_UNFILLED_AT_MARKET_CLOSE"
            elif delayed.get("threshold_reached") and entry_cents is not None:
                entry_price = cents_price(int(entry_cents))
                gross_pnl = Decimal("1") - entry_price if winner else -entry_price
                delayed.update({
                    "status": "SETTLED_WIN" if winner else "SETTLED_LOSS",
                    "no_stop_gross_pnl_per_share": format(gross_pnl, "f"),
                    "no_stop_gross_pnl": format(gross_pnl, "f"),
                })
            else:
                delayed["status"] = "NO_THRESHOLD_CROSS_OBSERVED"
            delayed_ladder = delayed.get("ladder")
            if isinstance(delayed_ladder, dict):
                direct_stop = delayed_ladder.get("direct_hybrid_stop")
                if contract_version >= 3 and isinstance(direct_stop, dict):
                    direct_filled = Decimal(str(
                        direct_order.get("simulated_filled_quantity") or "0"
                        if isinstance(direct_order, dict) else "0"
                    ))
                    if direct_filled <= 0:
                        direct_stop.update({
                            "state": "SETTLED",
                            "exit_classification": "ENTRY_NOT_FILLED",
                            "gross_pnl": "0",
                            "stopped_then_eventual_winner": False,
                        })
                    elif direct_stop.get("state") == "CLOSED":
                        delayed_stopped_then_winner = bool(winner)
                        direct_stop["stopped_then_eventual_winner"] = delayed_stopped_then_winner
                        if delayed_stopped_then_winner:
                            hold_pnl = direct_filled * (
                                Decimal("1") - cents_price(int(direct_order["price_cents"]))
                            )
                            direct_stop["hypothetical_lost_settlement_profit"] = format(
                                hold_pnl - Decimal(str(direct_stop.get("gross_pnl") or "0")), "f",
                            )
                    else:
                        maker_filled = Decimal(str(
                            direct_stop.get("maker_filled_quantity") or "0"
                        ))
                        hard_filled = Decimal(str(
                            direct_stop.get("hard_filled_quantity") or "0"
                        ))
                        remaining = max(
                            Decimal("0"), direct_filled - maker_filled - hard_filled,
                        )
                        entry_cost = direct_filled * cents_price(int(direct_order["price_cents"]))
                        maker_proceeds = maker_filled * cents_price(
                            int(direct_stop["maker_exit_cents"]),
                        )
                        hard_proceeds = (
                            hard_filled * cents_price(int(direct_stop["hard_fill_price_cents"]))
                            if hard_filled > 0 and direct_stop.get("hard_fill_price_cents") is not None
                            else Decimal("0")
                        )
                        settlement_payout = remaining if winner else Decimal("0")
                        direct_stop.update({
                            "state": "SETTLED",
                            "settlement_quantity": format(remaining, "f"),
                            "settlement_payout": format(settlement_payout, "f"),
                            "exit_classification": (
                                f"MAKER_EXIT_PARTIAL_THEN_SETTLEMENT_{'WIN' if winner else 'LOSS'}"
                                if maker_filled > 0
                                else f"SETTLEMENT_{'WIN' if winner else 'LOSS'}"
                            ),
                            "gross_pnl": format(
                                maker_proceeds + hard_proceeds + settlement_payout - entry_cost, "f",
                            ),
                            "stopped_then_eventual_winner": False,
                        })
                filled_quantity = Decimal("0")
                entry_cost = Decimal("0")
                for level, _ in OPENING_CROSS_LADDER_RUNGS:
                    rung = delayed_ladder.get("rungs", {}).get(str(level), {})
                    quantity = Decimal(str(rung.get("simulated_filled_quantity") or "0"))
                    filled_quantity += quantity
                    entry_cost += quantity * cents_price(level)
                payout = filled_quantity if winner else Decimal("0")
                delayed_ladder.update({
                    "settlement_outcome": outcome,
                    "directional_winner": winner,
                    "simulated_filled_quantity": format(filled_quantity, "f"),
                    "simulated_entry_cost": format(entry_cost, "f"),
                    "simulated_settlement_payout": format(payout, "f"),
                    "gross_no_stop_pnl": format(payout - entry_cost, "f"),
                    "status": "SETTLED_WIN" if winner else "SETTLED_LOSS",
                    "settlement_finalized_at": utc_now(),
                })
        opening_ladder = record.get("opening_cross_ladder")
        if isinstance(opening_ladder, dict):
            opening_ladder["settlement_outcome"] = outcome
            opening_ladder["directional_winner"] = winner
            opening_ladder["settlement_finalized_at"] = utc_now()
            for lane in opening_ladder.get("triggered", {}).values():
                filled_quantity = Decimal("0")
                entry_cost = Decimal("0")
                for level, _ in OPENING_CROSS_LADDER_RUNGS:
                    rung = lane.get("rungs", {}).get(str(level), {})
                    quantity = Decimal(str(rung.get("simulated_filled_quantity") or "0"))
                    filled_quantity += quantity
                    entry_cost += quantity * cents_price(level)
                payout = filled_quantity if winner else Decimal("0")
                lane.update({
                    "settlement_outcome": outcome,
                    "directional_winner": winner,
                    "simulated_filled_quantity": format(filled_quantity, "f"),
                    "simulated_entry_cost": format(entry_cost, "f"),
                    "simulated_settlement_payout": format(payout, "f"),
                    "gross_no_stop_pnl": format(payout - entry_cost, "f"),
                    "status": "SETTLED_WIN" if winner else "SETTLED_LOSS",
                    "settlement_finalized_at": utc_now(),
                })
        # Public-trade IDs only prevent duplicate allocation while this market
        # is active.  Settlement freezes every fill/result above, so retaining
        # 4,096 keys in each of seven threshold lanes adds tens of megabytes
        # without adding evidence or restart safety.
        prune_finalized_public_trade_keys(record)
        if stopped_then_winner:
            proceeds, _ = self.exit_proceeds(record)
            quantity = Decimal(str(record.get("actual_quantity") or "0"))
            record["hypothetical_lost_settlement_profit_due_to_stop"] = format(max(Decimal("0"), quantity - proceeds), "f")
        if Decimal(str(record.get("actual_quantity") or "0")) == 0:
            record["exit_classification"] = "ENTRY_NOT_FILLED"
        summary = self.entry_price_performance()
        delayed_summary = self.delayed_entry_performance()
        ladder_summary = self.opening_cross_ladder_performance()
        level_status = " ".join(
            f"{level}c:{'HIT' if record.get('shadow_entry_levels', {}).get(str(level), {}).get('touched') else 'MISS'}"
            for level in range(49, 39, -1)
        )
        self.audit(
            "settlement_analytics_finalized", ticker=record["ticker"], outcome=outcome, winner=winner,
            btc_target_price=record.get("btc_target_price"),
            btc_target_price_display=record.get("btc_target_price_display"),
            btc_target_comparison=record.get("btc_target_comparison"),
            btc_target_source=record.get("btc_target_source"),
            initial_signal_price_cents=initial, minimum_selected_price_cents=minimum,
            entry_limit_cents=record.get("entry_limit_cents"),
            actual_average_entry_price=record.get("actual_average_entry_price"),
            actual_quantity=record.get("actual_quantity"),
            winner_max_drawdown_cents=drawdown if winner else None,
            stopped_then_eventual_winner=stopped_then_winner,
            shadow_entry_levels=record.get("shadow_entry_levels"),
            aggregate_winning_settlements=summary["winning_settlements"],
            delayed_entry_tracking=record.get("delayed_entry_tracking"),
            delayed_entry_resolved=delayed_summary["resolved_entries"],
            delayed_entry_directional_wins=delayed_summary["directional_wins"],
            delayed_entry_directional_losses=delayed_summary["directional_losses"],
            opening_cross_ladder=record.get("opening_cross_ladder"),
            opening_cross_ladder_summary=ladder_summary,
        )
        LOG.warning(
            "SETTLEMENT PRICE PATH | ticker=%s side=%s btc_target=%s comparison=%s outcome=%s winner=%s "
            "initial_selected_ask=%sc entry_limit=%sc actual_entry=%s minimum_selected_ask=%sc "
            "winner_max_drawdown=%s levels_40_49=[%s]",
            record["ticker"], str(record.get("signal_side")).upper(),
            record.get("btc_target_price_display"), record.get("btc_target_comparison"),
            outcome.upper(), winner,
            initial, record.get("entry_limit_cents"), record.get("actual_average_entry_price"), minimum,
            f"{drawdown}c" if winner and drawdown is not None else "n/a", level_status,
        )

    def hybrid_stop_performance(self) -> dict[str, Any]:
        rows = [
            record for record in self.state.get("markets", {}).values()
            if isinstance(record, dict) and record.get("hybrid_stop", {}).get("triggered_at")
        ]
        full = sum(record.get("exit_classification") == "MAKER_EXIT_FULL" for record in rows)
        partial_hard = sum(record.get("exit_classification") == "MAKER_EXIT_PARTIAL_THEN_HARD_STOP" for record in rows)
        hard_only = sum(
            record.get("exit_classification") in {"HARD_STOP_ONLY", "DIRECT_PROTECTIVE_EXIT"}
            for record in rows
        )
        false_stops = sum(bool(record.get("stopped_then_eventual_winner")) for record in rows)
        false_stop_maker = sum(
            bool(record.get("stopped_then_eventual_winner")) and record.get("exit_classification") == "MAKER_EXIT_FULL"
            for record in rows
        )
        false_stop_hard = false_stops - false_stop_maker
        maximum_adverse = max(
            (int(record.get("winner_max_drawdown_cents") or 0) for record in rows if record.get("stopped_then_eventual_winner")),
            default=None,
        )
        lost_profit = sum(
            (Decimal(str(record.get("hypothetical_lost_settlement_profit_due_to_stop") or "0")) for record in rows),
            Decimal("0"),
        )
        maker_attempts = sum(
            any(order.get("exit_phase") == "maker_exit" for order in record.get("exit_orders", []))
            for record in rows
        )
        maker_filled = [
            Decimal(str(record.get("hybrid_stop", {}).get("maker_filled_quantity") or "0")) for record in rows
        ]
        hard_filled = [
            Decimal(str(record.get("hybrid_stop", {}).get("hard_filled_quantity") or "0")) for record in rows
        ]
        stopped_losses = [
            Decimal(str(record.get("realized_net_pnl") or "0")) for record in rows
            if record.get("realized_method") == "stop"
        ]
        maker_fill_quantity = maker_fill_proceeds = Decimal("0")
        hard_fill_quantity = hard_fill_proceeds = Decimal("0")
        for record in rows:
            for order in record.get("exit_orders", []):
                quantity = Decimal(str(order.get("fill_count") or "0"))
                price = Decimal(str(order.get("average_fill_price") or "0"))
                if order.get("exit_phase") == "maker_exit":
                    maker_fill_quantity += quantity
                    maker_fill_proceeds += quantity * price
                elif order.get("exit_phase") == "hard_stop":
                    hard_fill_quantity += quantity
                    hard_fill_proceeds += quantity * price
        result = {
            "stop_triggers": len(rows), "maker_exits_attempted": maker_attempts,
            "maker_exits_fully_filled": full, "maker_exits_partially_then_hard_stop": partial_hard,
            "hard_stop_only": hard_only, "hard_stop_fallbacks": partial_hard + hard_only,
            "average_maker_exit_filled_quantity": format(sum(maker_filled, Decimal("0")) / len(maker_filled), "f") if maker_filled else None,
            "average_hard_stop_filled_quantity": format(sum(hard_filled, Decimal("0")) / len(hard_filled), "f") if hard_filled else None,
            "average_maker_exit_fill_price": format(maker_fill_proceeds / maker_fill_quantity, "f") if maker_fill_quantity else None,
            "average_hard_stop_fill_price": format(hard_fill_proceeds / hard_fill_quantity, "f") if hard_fill_quantity else None,
            "average_realized_stopped_pnl": format(sum(stopped_losses, Decimal("0")) / len(stopped_losses), "f") if stopped_losses else None,
            "stopped_then_eventual_winner": false_stops,
            "stopped_then_eventual_winner_maker": false_stop_maker,
            "stopped_then_eventual_winner_hard": false_stop_hard,
            "false_stop_rate": false_stops / len(rows) if rows else None,
            "false_stop_maximum_adverse_excursion_cents": maximum_adverse,
            "hypothetical_lost_settlement_profit": format(lost_profit, "f"),
            "updated_at": utc_now(),
        }
        self.state["hybrid_stop_performance"] = result
        return result

    def log_hybrid_stop_performance(self) -> None:
        result = self.hybrid_stop_performance()
        rate = result["false_stop_rate"]
        direct = self.config["stop_policy"] == "direct_ioc_at_trigger"
        LOG.warning(
            "================ %s PERFORMANCE ================",
            "DIRECT 51C PROTECTIVE EXIT" if direct else "HYBRID STOP",
        )
        LOG.warning(
            "policy=%s triggers=%s maker_attempts=%s maker_full=%s partial_then_hard=%s hard_or_direct=%s "
            "hard_fallbacks=%s false_stops=%s (maker=%s hard=%s) false_stop_rate=%s "
            "maker_avg_price=%s hard_avg_price=%s false_stop_max_adverse=%s "
            "lost_settlement_profit=%s average_stopped_pnl=%s",
            self.config["stop_policy"], result["stop_triggers"], result["maker_exits_attempted"],
            result["maker_exits_fully_filled"],
            result["maker_exits_partially_then_hard_stop"], result["hard_stop_only"], result["hard_stop_fallbacks"],
            result["stopped_then_eventual_winner"], result["stopped_then_eventual_winner_maker"],
            result["stopped_then_eventual_winner_hard"], "n/a" if rate is None else f"{100 * rate:.2f}%",
            result["average_maker_exit_fill_price"], result["average_hard_stop_fill_price"],
            result["false_stop_maximum_adverse_excursion_cents"], result["hypothetical_lost_settlement_profit"],
            result["average_realized_stopped_pnl"],
        )

    def protective_exit_health(self) -> dict[str, Any]:
        """Summarize durable hard-stop outcomes without hiding settlement leaks."""

        rows = [record for record in self.state.get("markets", {}).values() if isinstance(record, dict)]
        protective_latched = [
            record for record in rows
            if (
                record.get("hybrid_stop", {}).get("stop_exit_latched")
                or record.get("hybrid_stop", {}).get("hard_stop_latched")
            )
        ]
        latched = [record for record in rows if record.get("hybrid_stop", {}).get("hard_stop_latched")]
        settled_after_latch = [
            record for record in protective_latched if record.get("realized_method") == "settlement"
        ]
        pending = [
            record for record in protective_latched
            if record.get("status") in {
                "ENTRY_CANCEL_UNCONFIRMED", "MAKER_EXIT_CANCEL_UNCONFIRMED", "HARD_STOP_PENDING",
                "POSITION_OPEN", "ENTRY_PARTIAL", "SETTLEMENT_PENDING",
            }
        ]
        result = {
            "protective_exit_latched": len(protective_latched),
            "hard_stop_latched": len(latched),
            "closed_by_stop": sum(
                record.get("realized_method") == "stop" for record in protective_latched
            ),
            "pending_flatten": len(pending),
            "settled_after_protective_exit_latch": len(settled_after_latch),
            # Retain the prior key for backward-compatible checkpoint readers.
            "settled_after_hard_stop_latch": sum(
                record.get("realized_method") == "settlement" for record in latched
            ),
            "entry_cancel_races_reconciled": sum(
                any(bool(order.get("cancel_reconciled_after_failure")) for order in record.get("entry_orders", []))
                for record in rows
            ),
            "maker_cancel_races_reconciled": sum(
                any(bool(order.get("cancel_reconciled_after_failure")) for order in record.get("exit_orders", []))
                for record in rows
            ),
            "updated_at": utc_now(),
        }
        self.state["protective_exit_health"] = result
        return result

    def fee_metrics(self) -> dict[str, str]:
        entry_fees = exit_fees = Decimal("0")
        for record in self.state.get("markets", {}).values():
            if not isinstance(record, dict):
                continue
            entry_fees += sum(
                (Decimal(str(order.get("fees_paid") or "0")) for order in record.get("entry_orders", [])),
                Decimal("0"),
            )
            exit_fees += sum(
                (Decimal(str(order.get("fees_paid") or "0")) for order in record.get("exit_orders", [])),
                Decimal("0"),
            )
        result = {
            "entry_fees_paid": format(entry_fees, "f"), "exit_fees_paid": format(exit_fees, "f"),
            "total_fees_paid": format(entry_fees + exit_fees, "f"),
        }
        self.state["fee_metrics"] = result
        return result

    @staticmethod
    def _realized_window_metrics(records: list[dict[str, Any]]) -> dict[str, Any]:
        """Return fee-adjusted per-share performance for completed live facts.

        Recovery quantities vary, so the displayed win rate is trade-count
        based while average win/loss and break-even rate normalize every trade
        by its actual filled quantity.  A zero-fill is never a realized trade.
        """

        rows: list[tuple[dict[str, Any], Decimal, Decimal, Decimal | None]] = []
        for record in records:
            try:
                quantity = Decimal(str(record.get("actual_quantity") or "0"))
                net = Decimal(str(record["realized_net_pnl"]))
            except (ArithmeticError, KeyError, TypeError, ValueError):
                continue
            if not quantity.is_finite() or not net.is_finite() or quantity <= 0:
                continue
            raw_entry = record.get("actual_average_entry_price")
            if raw_entry is None:
                raw_entry = record.get("entry_execution_summary", {}).get(
                    "actual_weighted_average_entry_price"
                )
            try:
                entry = Decimal(str(raw_entry)) if raw_entry is not None else None
                if entry is not None and (not entry.is_finite() or not Decimal("0") < entry < Decimal("1")):
                    entry = None
            except (ArithmeticError, TypeError, ValueError):
                entry = None
            rows.append((record, quantity, net, entry))

        normalized = [(record, net / quantity) for record, quantity, net, _entry in rows]
        winning = [per_share for _record, per_share in normalized if per_share > 0]
        losing = [per_share for _record, per_share in normalized if per_share < 0]
        flat = sum(per_share == 0 for _record, per_share in normalized)
        decided = len(winning) + len(losing)
        realized_rate = Decimal(len(winning)) / Decimal(decided) if decided else None
        average_win = sum(winning, Decimal("0")) / len(winning) if winning else None
        average_loss = -sum(losing, Decimal("0")) / len(losing) if losing else None
        break_even = (
            average_loss / (average_win + average_loss)
            if average_win is not None and average_loss is not None and average_win + average_loss > 0
            else None
        )
        entry_quantity = sum(
            (quantity for _record, quantity, _net, entry in rows if entry is not None), Decimal("0")
        )
        entry_notional = sum(
            (quantity * entry for _record, quantity, _net, entry in rows if entry is not None), Decimal("0")
        )
        directional_wins = directional_losses = 0
        total_fees = Decimal("0")
        for record, _quantity, _net, _entry in rows:
            outcome = record.get("settlement_outcome") or record.get("post_stop_settlement_outcome")
            if outcome in {"yes", "no"} and record.get("signal_side") in {"yes", "no"}:
                if outcome == record.get("signal_side"):
                    directional_wins += 1
                else:
                    directional_losses += 1
            for order in record.get("entry_orders", []) + record.get("exit_orders", []):
                try:
                    total_fees += Decimal(str(order.get("fees_paid") or "0"))
                except (ArithmeticError, TypeError, ValueError):
                    continue
        directional_total = directional_wins + directional_losses
        return {
            "completed_trades": len(rows),
            "realized_wins": len(winning), "realized_losses": len(losing), "realized_flat": flat,
            "realized_win_rate": None if realized_rate is None else format(realized_rate, "f"),
            "average_net_win_per_share": None if average_win is None else format(average_win, "f"),
            "average_net_loss_per_share": None if average_loss is None else format(average_loss, "f"),
            "average_actual_gain_cents_per_share": (
                None if average_win is None else format(average_win * Decimal("100"), "f")
            ),
            "average_actual_loss_cents_per_share": (
                None if average_loss is None else format(average_loss * Decimal("100"), "f")
            ),
            "fee_adjusted_break_even_win_rate": None if break_even is None else format(break_even, "f"),
            "win_rate_edge": (
                None if realized_rate is None or break_even is None else format(realized_rate - break_even, "f")
            ),
            "quantity_weighted_average_entry": (
                None if entry_quantity <= 0 else format(entry_notional / entry_quantity, "f")
            ),
            "total_realized_net_pnl": format(sum((net for _record, _quantity, net, _entry in rows), Decimal("0")), "f"),
            "total_fees_paid": format(total_fees, "f"),
            "directional_wins": directional_wins, "directional_losses": directional_losses,
            "directional_win_rate": (
                None if directional_total == 0
                else format(Decimal(directional_wins) / Decimal(directional_total), "f")
            ),
        }

    def realized_performance_metrics(self) -> dict[str, Any]:
        completed = [
            record for record in self.state.get("markets", {}).values()
            if isinstance(record, dict) and record.get("realized_net_pnl") is not None
        ]
        completed.sort(key=lambda row: (
            _iso_epoch(row.get("completed_at")) or float(row.get("market_close_epoch") or 0),
            str(row.get("ticker") or ""),
        ))
        result = {
            "all": self._realized_window_metrics(completed),
            "last_20": self._realized_window_metrics(completed[-20:]),
            "last_50": self._realized_window_metrics(completed[-50:]),
            "updated_at": utc_now(),
        }
        self.state["live_performance"] = result
        return result

    async def refresh_live_account_status(
        self, rest: KalshiREST, ticker: str | None,
    ) -> dict[str, Any]:
        """Refresh bounded, credential-safe account telemetry for heartbeats."""

        if self.dry_run:
            shadow = self.shadow_metrics()
            return {
                "aggregate_balance": shadow.get("balance"), "exchange_index": None,
                "market_shard_available": shadow.get("balance"), "read_status": "SHADOW",
            }
        aggregate = None
        exchange_index = None
        available = None
        read_status = "OK"
        try:
            aggregate = await rest.balance_decimal()
        except Exception:
            # Heartbeat telemetry must never terminate position management.  The
            # authenticated pre-entry funding check remains the authoritative
            # fail-closed control for opening exposure.
            read_status = "AGGREGATE_READ_FAILED"
        if ticker:
            try:
                exchange_index, available = await rest.market_entry_funding(ticker)
            except Exception:
                read_status = (
                    "SHARD_READ_FAILED" if read_status == "OK"
                    else "ALL_READS_FAILED"
                )
        status = {
            "at": utc_now(),
            "aggregate_balance": None if aggregate is None else format(aggregate, "f"),
            "exchange_index": exchange_index,
            "market_shard_available": None if available is None else format(available, "f"),
            "read_status": read_status,
        }
        self.state["live_account_status"] = status
        return status

    async def settle(self, rest: KalshiREST, record: dict[str, Any], now: float) -> None:
        if record.get("status") == "CLOSED":
            if now >= float(record["market_close_epoch"]) and not record.get("analytics_settlement_finalized"):
                await self.verify_post_stop_settlement(rest, record, now)
            return
        if now < float(record["market_close_epoch"]):
            return
        # A latched protective exit must remain an exit-management incident until
        # Kalshi reports the position flat.  Do not silently relabel known
        # unflattened exposure as an ordinary hold-to-settlement trade.
        if (
            not self.dry_run
            and (
                record.get("hybrid_stop", {}).get("stop_exit_latched")
                or record.get("hybrid_stop", {}).get("hard_stop_latched")
            )
            and self.local_remaining_position(record) > 0
        ):
            authoritative = await rest.position_for_ticker(record["ticker"])
            if authoritative is None:
                self.trip("hard_stop_close_position_reconciliation_failed")
                return
            if abs(Decimal(str(authoritative))) > 0:
                incident = record.setdefault("hard_stop_exit_deadline_incident", {})
                first = not incident
                incident.update({
                    "first_detected_at": incident.get("first_detected_at") or utc_now(),
                    "last_detected_at": utc_now(),
                    "authoritative_position": format(abs(Decimal(str(authoritative))), "f"),
                    "market_close_epoch": record["market_close_epoch"],
                    "status": "UNFLATTENED_AFTER_MARKET_CLOSE",
                })
                self.trip("hard_stop_unflattened_at_market_close")
                if first:
                    self.audit(
                        "hard_stop_unflattened_at_market_close", ticker=record["ticker"],
                        authoritative_position=incident["authoritative_position"],
                    )
                LOG.critical(
                    "PROTECTIVE EXIT INCIDENT | ticker=%s stop_exit_latched=true hard_stop_latched=%s "
                    "position=%s market_closed=true action=continue_authoritative_reconciliation",
                    record["ticker"], bool(record.get("hybrid_stop", {}).get("hard_stop_latched")),
                    incident["authoritative_position"],
                )
                return
            record.setdefault("hard_stop_exit_deadline_incident", {}).update({
                "flattened_or_settled_at": utc_now(), "authoritative_position": "0",
            })
            self.clear_resolved_entry_interlock(record, "hard_stop_position_now_flat")
        status = str(record.get("status") or "")
        if status in {"SIGNAL_PENDING", "ENTRY_PENDING", "ENTRY_PARTIAL", "ENTRY_CANCEL_UNCONFIRMED"}:
            if not await self.cancel_entry_orders_and_confirm(rest, record, next_action="finish_entry"):
                return
            filled = (
                Decimal(str(record.get("actual_quantity") or "0"))
                if self.dry_run else await self.refresh_entry(rest, record)
            )
            if status in {"SIGNAL_PENDING", "ENTRY_PENDING", "ENTRY_CANCEL_UNCONFIRMED"} and filled == 0:
                self.finish_entry_attempt(record, Decimal("0"), "gtc_entry_canceled_at_market_close")
                status = str(record.get("status"))
        maker_exit = self.hybrid_maker_exit_order(record)
        if isinstance(maker_exit, dict) and Decimal(str(maker_exit.get("remaining_count") or "0")) > 0:
            if not await self.cancel_hybrid_maker_exit_and_confirm(rest, record):
                return
        market = await rest.get_market(record["ticker"])
        outcome = market_result(market) if market is not None else None
        if outcome not in {"yes", "no"}:
            if Decimal(str(record.get("actual_quantity") or "0")) > 0:
                self.transition(record, "SETTLEMENT_PENDING", "awaiting_official_settlement")
            return
        self.finalize_settlement_analytics(record, outcome)
        quantity = Decimal(str(record.get("actual_quantity") or "0"))
        if quantity <= 0:
            return
        remaining = self.local_remaining_position(record)
        payout = remaining if outcome == record.get("signal_side") else Decimal("0")
        proceeds, exit_fees = self.exit_proceeds(record)
        net = payout + proceeds - exit_fees - self.entry_cost(record)
        maker_filled = Decimal(str((maker_exit or {}).get("fill_count") or "0"))
        classification = "SETTLEMENT_WIN" if outcome == record.get("signal_side") else "SETTLEMENT_LOSS"
        if (
            record.get("hybrid_stop", {}).get("stop_exit_latched")
            or record.get("hybrid_stop", {}).get("hard_stop_latched")
        ):
            prefix = (
                "HARD_STOP_EXIT_FAILURE_"
                if record.get("hybrid_stop", {}).get("hard_stop_latched")
                else "PROTECTIVE_EXIT_FAILURE_"
            )
            classification = prefix + classification
            record["protective_exit_failed_before_settlement"] = True
            if record.get("hybrid_stop", {}).get("hard_stop_latched"):
                record["hard_stop_exit_failed_before_settlement"] = True
        if maker_filled > 0:
            classification = "MAKER_EXIT_PARTIAL_THEN_" + classification
        record["exit_classification"] = classification
        self.record_realized(record, net, "settlement", f"{record['ticker']}:settlement:{outcome}")

    async def reconcile_uncertain_record(self, rest: KalshiREST, record: dict[str, Any]) -> bool:
        """Continuously resolve an earlier create response without a blind retry.

        An unknown HTTP response is not treated as a rejection.  The worker
        remains alive and queries the exact deterministic client ID, fills and
        position every reconciliation interval.  A recovered order is adopted;
        recovered exposure immediately enters stop management.  If no order,
        fill or position exists, the intent becomes a zero-fill only once its
        market is closed, after which later execution is impossible.
        """

        if record.get("status") not in {"ERROR_RECONCILIATION", "RECONCILIATION_PENDING"}:
            return False
        ticker = str(record.get("ticker") or "")
        side = str(record.get("signal_side") or "")
        ambiguous_orders = [
            item for item in record.get("entry_orders", [])
            if isinstance(item, dict)
            and not item.get("order_id")
            and item.get("status") in {"submitting", "submit_failed"}
            and item.get("submission_outcome") != "rejected"
        ]
        if not ticker or side not in {"yes", "no"} or len(ambiguous_orders) != 1:
            self.trip("uncertain_entry_intent_not_unique")
            return True
        intent = ambiguous_orders[0]
        client_id = str(intent.get("client_order_id") or "")
        if not client_id:
            self.trip("uncertain_entry_intent_missing_client_id")
            return True

        async def exact_pages(path: str, key: str) -> list[Any]:
            rows: list[Any] = []
            cursor: str | None = None
            seen: set[str] = set()
            for _ in range(20):
                params: dict[str, Any] = {"ticker": ticker, "limit": 1000}
                if cursor:
                    params["cursor"] = cursor
                payload = await rest.get_raw_json(path, params)
                page = payload.get(key, []) if isinstance(payload, dict) else []
                if not isinstance(page, list):
                    raise RuntimeError("unexpected reconciliation page")
                rows.extend(page)
                next_cursor = str(payload.get("cursor") or "") if isinstance(payload, dict) else ""
                if not next_cursor:
                    return rows
                if next_cursor in seen:
                    raise RuntimeError("repeated reconciliation cursor")
                seen.add(next_cursor)
                cursor = next_cursor
            raise RuntimeError("reconciliation pagination exceeded bound")

        try:
            orders, fills, position = await asyncio.gather(
                exact_pages("/portfolio/orders", "orders"),
                exact_pages("/portfolio/fills", "fills"),
                rest.position_for_ticker(ticker),
            )
        except Exception as exc:
            observation = record.setdefault("continuous_entry_reconciliation", {})
            observation.update({
                "last_checked_at": utc_now(), "status": "EXCHANGE_READ_UNAVAILABLE",
                "error_type": type(exc).__name__,
            })
            return True
        if position is None:
            record.setdefault("continuous_entry_reconciliation", {}).update({
                "last_checked_at": utc_now(), "status": "POSITION_UNKNOWN",
            })
            return True
        signed = Decimal(str(position))
        matches = [
            item for item in orders
            if str(field(item, "ticker") or "") == ticker
            and str(field(item, "client_order_id") or "") == client_id
        ]
        if len(matches) > 1:
            self.trip("uncertain_entry_multiple_exchange_orders")
            return True
        if (side == "yes" and signed < 0) or (side == "no" and signed > 0) or side not in {"yes", "no"}:
            self.trip("uncertain_order_position_direction_mismatch")
            return True

        exchange_order = matches[0] if matches else None
        if exchange_order is not None:
            intent.update({
                "order_id": str(field(exchange_order, "order_id") or "") or None,
                "fill_count": format(Decimal(str(order_fill_count(exchange_order))), "f"),
                "remaining_count": format(Decimal(str(order_remaining_count(exchange_order) or "0")), "f"),
                "average_fill_price": format(Decimal(str(order_average_position_price(
                    exchange_order, side, float(intent.get("position_price") or self.config["entry_price"]),
                ))), "f"),
                "fees_paid": format(Decimal(str(order_fee_total(exchange_order))), "f"),
                "submission_outcome": "accepted", "reconciled_from_exact_client_id": True,
                "last_checked_at": utc_now(),
            })

        accounting_reconciled = self.reconstruct_entry_accounting_from_fills(record, fills, abs(signed))
        filled = sum(
            (Decimal(str(item.get("fill_count") or "0")) for item in record.get("entry_orders", [])),
            Decimal("0"),
        )
        remaining = Decimal(str(intent.get("remaining_count") or "0")) if exchange_order is not None else Decimal("0")
        record["actual_quantity"] = format(max(filled, abs(signed)), "f")
        self.state["current_position"] = format(abs(signed), "f")
        record["continuous_entry_reconciliation"] = {
            "last_checked_at": utc_now(), "status": "RESOLVED" if exchange_order is not None else "NO_ORDER_FOUND",
            "client_order_id": client_id, "exchange_order_found": exchange_order is not None,
            "linked_fill_quantity": format(filled, "f"), "position": format(signed, "f"),
            "accounting_reconciled": accounting_reconciled,
        }

        if abs(signed) > 0:
            if not accounting_reconciled:
                breaker = self.state["circuit_breaker"]
                breaker.update({
                    "blocked": True,
                    "reason": "uncertain_order_entry_accounting_unreconciled",
                    "triggered_at": breaker.get("triggered_at") or utc_now(),
                })
                self.audit(
                    "circuit_breaker_escalated", ticker=ticker,
                    reason="uncertain_order_entry_accounting_unreconciled",
                )
            self.update_effective_stop_price(record)
            self.transition(record, "POSITION_OPEN", "uncertain_submission_authoritative_position")
            self.audit(
                "uncertain_submission_reconciled", ticker=ticker,
                quantity=record["actual_quantity"], entry_accounting_reconciled=accounting_reconciled,
            )
            if accounting_reconciled:
                self.clear_resolved_entry_interlock(record, "authoritative_position_recovered")
            return True

        if exchange_order is not None and remaining > 0:
            self.transition(record, "ENTRY_PENDING", "uncertain_submission_resting_order_recovered")
            self.audit(
                "uncertain_submission_reconciled", ticker=ticker, order_id=intent.get("order_id"),
                remaining_quantity=format(remaining, "f"), resolution="resting_order_adopted",
            )
            self.clear_resolved_entry_interlock(record, "resting_order_adopted")
            return True

        if exchange_order is not None and filled == 0:
            self.finish_entry_attempt(record, Decimal("0"), "uncertain_submission_terminal_no_fill")
            self.clear_resolved_entry_interlock(record, "terminal_exchange_order_no_fill")
            return True

        close_epoch = float(record.get("market_close_epoch") or float("inf"))
        if exchange_order is not None and filled > 0 and time.time() >= close_epoch and accounting_reconciled:
            self.transition(record, "SETTLEMENT_PENDING", "uncertain_submission_filled_market_closed")
            self.clear_resolved_entry_interlock(record, "closed_market_fill_recovered")
            return True
        if exchange_order is None and filled == 0 and time.time() >= close_epoch:
            # This is the only safe no-order inference: once the market has
            # closed, an absent exact order plus no fills and a flat position
            # cannot later create exposure.
            intent.update({
                "status": "reconciled_terminal_no_fill", "remaining_count": "0",
                "fill_count": "0", "resolved_at": utc_now(),
            })
            self.finish_entry_attempt(record, Decimal("0"), "uncertain_submission_closed_market_no_order")
            self.clear_resolved_entry_interlock(record, "closed_market_no_order_no_fill_no_position")
            return True

        # The worker remains online and checks again on the next interval.  It
        # does not create another order for this or a later market until the
        # unknown intent has become one of the authoritative cases above.
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
                if upcoming:
                    preloaded = self.state.setdefault("preloaded_markets", [])
                    if upcoming["ticker"] not in preloaded:
                        append_unique(preloaded, upcoming["ticker"], maximum=200)
                        LOG.warning(
                            "UPCOMING MARKET PRELOADED | current=%s upcoming=%s opens_in=%.3fs status=%s",
                            active["ticker"], upcoming["ticker"], upcoming["open_epoch"] - now,
                            upcoming.get("status"),
                        )
                        self.audit(
                            "upcoming_market_preloaded", ticker=upcoming["ticker"],
                            current_ticker=active["ticker"], open_epoch=upcoming["open_epoch"],
                            status=upcoming.get("status"),
                        )
                observation_window = int(self.config["outcome_observation_seconds"])
                if active["close_epoch"] - observation_window <= now <= active["close_epoch"]:
                    # The quote stream remains pre-subscribed, but only the
                    # ending market's configured final 5/15-second window is
                    # admitted to the next-market direction tracker.
                    self.tracker.observe_feed(feed, active["ticker"])
                # Persist a final-quote provisional result as soon as it is
                # available in the last second before close.  This makes a
                # restart between the old close and new entry auditable rather
                # than depending on in-memory websocket history.
                if active["close_epoch"] - 1.0 <= now <= active["close_epoch"]:
                    closing = self.tracker.infer(active["ticker"], active["close_epoch"])
                    if closing is not None:
                        self.state.setdefault("provisional_outcomes", {})[active["ticker"]] = closing
                        self.audit(
                            "provisional_outcome_frozen", ticker=active["ticker"],
                            outcome=closing["outcome"], method=closing["method"],
                            quote_age=closing["quote_age_seconds"],
                            observation_window_seconds=closing["observation_window_seconds"],
                            qualifying_bid=closing["qualifying_bid"],
                            final_yes_bid=closing["final_yes_bid"],
                            final_no_bid=closing["final_no_bid"],
                        )
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
                current_record = self.state.get("markets", {}).get(active["ticker"])
                if isinstance(current_record, dict):
                    # Quotes update analytics but never rewrite the frozen
                    # signal-side initial price or its derived limit. This
                    # continues after a stop until official settlement.
                    self.capture_opening_price_reference(feed, current_record, now)
                    self.capture_opening_quote(feed, current_record, now)
                    self.observe_price_analytics(feed, current_record)
                    if time.monotonic() - self.last_stop_poll >= float(self.config["stop_poll_interval"]):
                        await self.manage_entry(rest, feed, current_record, now)
                        await self.manage_stop(rest, feed, current_record)
                        self.last_stop_poll = time.monotonic()
            if time.monotonic() - self.last_reconcile >= float(self.config["reconciliation_interval"]):
                await self.reconcile_active(rest, feed, now)
                self.last_reconcile = time.monotonic()
            if time.monotonic() - self.last_heartbeat >= 60:
                sizing = sizing_state(self.current_parameters(), self.state.get("sizing"))
                execution = self.refresh_entry_execution_metrics()
                timing = self.refresh_execution_timing_metrics()
                record = self.state.get("markets", {}).get(active["ticker"], {}) if active else {}
                capture = record.get("opening_quote_capture", {}) if isinstance(record, dict) else {}
                market_close = record.get("market_close_epoch") if isinstance(record, dict) else None
                gtc_market_close_in = (
                    round(max(0.0, float(market_close) - now), 3)
                    if market_close not in (None, "") else None
                )
                entry_latency = timing["entry_first_fill_from_market_open"]
                stop_latency = timing["stop_trigger_from_first_fill"]
                shadow = self.shadow_metrics() if self.dry_run else {}
                fees = self.fee_metrics()
                eligibility = self.entry_price_performance()["initial_stop_eligibility"]
                delayed_metrics = self.delayed_entry_performance()
                health = self.entry_submission_health()
                exit_health = self.protective_exit_health()
                performance = self.realized_performance_metrics()
                account = await self.refresh_live_account_status(
                    rest, active["ticker"] if active else None,
                )
                entry_orders = record.get("entry_orders", []) if isinstance(record, dict) else []
                latest_entry = entry_orders[-1] if entry_orders else {}
                hybrid = record.get("hybrid_stop", {}) if isinstance(record, dict) else {}

                def percentage(value: Any) -> str:
                    if value in (None, ""):
                        return "n/a"
                    return f"{Decimal(str(value)) * Decimal('100'):.2f}%"

                # Each line is intentionally bounded. GitHub's viewer and
                # notification transports truncate very long records; no
                # safety-critical fact is hidden at the end of one giant line.
                LOG.warning(
                    "ORDER HEALTH | mode=%s attempts=%s exchange_acks=%s rejected=%s "
                    "retries=%s retry_pending=%s retry_skip_51c=%s unresolved=%s "
                    "breaker=%s reason=%s",
                    health["mode"], health["recorded_entry_attempts"],
                    health["exchange_acknowledgments"], health["definitive_rejections"],
                    health["market_scoped_retries"], health["retry_pending_markets"],
                    health["abandoned_at_or_below_51c"],
                    health["unresolved_submissions"], health["breaker_blocked"],
                    health["breaker_reason"],
                )
                LOG.warning(
                    "LIVE ACCOUNT | mode=%s aggregate_balance=$%s market_shard=%s "
                    "shard_available=$%s read=%s current_position=%s open_orders=%s "
                    "cumulative_net=$%s fees=$%s worker_online=true safety_alert=%s",
                    "DRY_RUN" if self.dry_run else "LIVE",
                    account.get("aggregate_balance"), account.get("exchange_index"),
                    account.get("market_shard_available"), account.get("read_status"),
                    self.state.get("current_position"),
                    (self.state.get("last_reconciliation") or {}).get("managed_open_orders"),
                    self.state.get("cumulative_realized_pnl"), fees["total_fees_paid"],
                    health["breaker_reason"],
                )
                ladder = record.get("opposite_ladder", {}) if isinstance(record, dict) else {}
                plan = ladder.get("plan", {}) if isinstance(ladder.get("plan"), dict) else {}
                LOG.warning(
                    "HEARTBEAT | mode=%s ticker=%s state=%s sticky_side=%s trade_side=%s "
                    "base=%s ladder_quantities=%s recovery=DISABLED cap=NONE "
                    "orders_acknowledged=%s/%s exit_latched=%s active=%s close_in=%s",
                    "DRY_RUN" if self.dry_run else "LIVE",
                    active and active["ticker"], record.get("status"),
                    record.get("sticky_signal_side"), record.get("trade_side"),
                    self.config["starting_base"],
                    "/".join(str(item.get("quantity")) for item in plan.get("orders", [])) or "pending",
                    ladder.get("accepted_order_count", 0), ladder.get("planned_order_count", 5),
                    bool(ladder.get("exit_latched")), self.state.get("active_market"), gtc_market_close_in,
                )
                LOG.warning(
                    "ENTRY STATUS | ticker=%s decision=%s opening_ask=%s trigger_ask=%s "
                    "limit=%s order_id=%s submission=%s fill=%s remaining=%s maker_wait=%s "
                    "tracked=%d filled=%d zero=%d filtered=%d funding_failures=%d missed=%d "
                    "first_price_lag=%s first_depth_lag=%s fill_p50=%s",
                    active and active["ticker"],
                    record.get("delayed_entry_decision", {}).get("status"),
                    record.get("opening_price_reference", {}).get("selected_side_ask_cents"),
                    record.get("delayed_entry_decision", {}).get("observed_selected_side_ask_cents"),
                    record.get("delayed_entry_decision", {}).get("limit_price_cents"),
                    latest_entry.get("order_id"), latest_entry.get("submission_outcome"),
                    latest_entry.get("fill_count"), latest_entry.get("remaining_count"),
                    record.get("maker_entry_wait", {}).get("reason"),
                    execution["tracked_markets"], execution["markets_with_entry_fill"],
                    execution["zero_fill_markets"], execution["entry_filtered_markets"],
                    execution["funding_failure_markets"], execution["missed_signal_markets"],
                    capture.get("first_price_capture_lag_seconds"),
                    capture.get("first_depth_capture_lag_seconds"), entry_latency.get("median_seconds"),
                )
                for window_name in ("all", "last_20"):
                    stats = performance[window_name]
                    LOG.warning(
                        "LIVE PERFORMANCE | window=%s completed=%s realized_wl=%s/%s flat=%s "
                        "win_rate=%s break_even=%s edge=%s avg_entry=%s avg_gain_cents=%s "
                        "avg_loss_cents=%s directional_wl=%s/%s directional_wr=%s net=$%s fees=$%s",
                        window_name, stats["completed_trades"], stats["realized_wins"],
                        stats["realized_losses"], stats["realized_flat"],
                        percentage(stats["realized_win_rate"]),
                        percentage(stats["fee_adjusted_break_even_win_rate"]),
                        percentage(stats["win_rate_edge"]), stats["quantity_weighted_average_entry"],
                        stats["average_actual_gain_cents_per_share"],
                        stats["average_actual_loss_cents_per_share"],
                        stats["directional_wins"], stats["directional_losses"],
                        percentage(stats["directional_win_rate"]), stats["total_realized_net_pnl"],
                        stats["total_fees_paid"],
                    )
                LOG.warning(
                    "EXIT STATUS | ticker=%s boundary=trade_bid>=%sc_or_sticky_ask<=%sc "
                    "state=%s class=%s entry_filled_qty=%s exit_filled_qty=%s "
                    "authoritative_position=%s residual_retry=reduce_only_IOC",
                    active and active["ticker"], self.config["opposite_take_profit_cents"],
                    self.config["opposite_take_profit_cents"], ladder.get("state"),
                    record.get("exit_classification"), record.get("actual_quantity"),
                    format(self.exit_filled_quantity(record), "f"),
                    self.state.get("current_position"),
                )
                LOG.warning(
                    "PROTECTIVE EXIT HEALTH | protective_latched=%s hard_latched=%s "
                    "closed_by_stop=%s pending_flatten=%s settled_after_protective_latch=%s "
                    "settled_after_hard_latch=%s entry_cancel_races_reconciled=%s "
                    "maker_cancel_races_reconciled=%s",
                    exit_health["protective_exit_latched"], exit_health["hard_stop_latched"],
                    exit_health["closed_by_stop"], exit_health["pending_flatten"],
                    exit_health["settled_after_protective_exit_latch"],
                    exit_health["settled_after_hard_stop_latch"],
                    exit_health["entry_cancel_races_reconciled"],
                    exit_health["maker_cancel_races_reconciled"],
                )
                LOG.warning(
                    "RESEARCH COHORT | delayed53_resolved=%s directional_wl=%s/%s wr=%s "
                    "limit_filled=%s zero=%s partial=%s no_stop_pnl=$%s hybrid_pnl=$%s "
                    "initial_price_records=%s stop_safety_no_entry=%s",
                    delayed_metrics["resolved_entries"], delayed_metrics["directional_wins"],
                    delayed_metrics["directional_losses"],
                    percentage(delayed_metrics.get("directional_win_rate")),
                    delayed_metrics["direct_limit_filled_entries"], delayed_metrics["direct_limit_zero_fills"],
                    delayed_metrics["direct_limit_partial_fills"], delayed_metrics.get("gross_no_stop_pnl_total"),
                    delayed_metrics.get("gross_hybrid_pnl_total"), eligibility["captured_initial_prices"],
                    eligibility["actual_strategy_stop_safety_rejections"],
                )
                self.last_heartbeat = time.monotonic()
            if time.monotonic() - self.last_analytics_log >= 300:
                self.log_entry_price_performance()
                self.log_delayed_entry_performance()
                self.log_opening_cross_ladder_performance()
                self.log_hybrid_stop_performance()
                self.last_analytics_log = time.monotonic()
            elapsed = time.monotonic() - start
            if elapsed >= run_seconds:
                ready, details = self.handoff_ready(now)
                if ready:
                    self.state["handoff"] = {"ready": True, "at": utc_now(), "elapsed_seconds": round(elapsed, 3), **details}
                    self.audit("safe_handoff_ready", **details, elapsed_seconds=round(elapsed, 3))
                    self.checkpoint("safe_handoff_ready")
                    self.log_entry_price_performance()
                    self.log_delayed_entry_performance()
                    self.log_opening_cross_ladder_performance()
                    self.log_hybrid_stop_performance()
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
    result.add_argument("--state-file", type=Path, default=Path("data/kalshi_live_opposite_ladder_v14_state.json"))
    result.add_argument("--audit-ledger", type=Path, default=Path("data/kalshi_live_opposite_ladder_v14_audit.jsonl"))
    result.add_argument("--run-seconds", type=float, default=19_200)
    result.add_argument("--persist-config", action="store_true")
    result.add_argument("--reconcile-only", action="store_true")
    result.add_argument("--cancel-managed-entries", action="store_true", help="explicitly cancel only hybrid-prefixed resting orders; never opens or closes a position")
    result.add_argument("--reset-state", action="store_true")
    result.add_argument("--live-enabled", action="store_true")
    result.add_argument("--dry-run", action="store_true")
    result.add_argument("--trading-mode", choices=("shadow", "live"))
    result.add_argument(
        "--shadow-profile", choices=sorted(SHADOW_STOP_PROFILE_PRICES),
        help="isolated dry-run stop/profile identity; must agree with --stop-price",
    )
    for name in sorted(
        DECIMAL_CONFIG_FIELDS | OPTIONAL_DECIMAL_CONFIG_FIELDS | INTEGER_CONFIG_FIELDS | FLOAT_CONFIG_FIELDS
    ):
        result.add_argument("--" + name.replace("_", "-"), dest=name)
    result.add_argument("--allow-capital-downsize", action=argparse.BooleanOptionalAction, default=None)
    result.add_argument("--hybrid-stop-enabled", action=argparse.BooleanOptionalAction, default=None)
    return result


async def async_main(args: argparse.Namespace) -> int:
    config = apply_overrides(load_config(args.config), args)
    requested_live = bool(args.live_enabled)
    environment_live = _bool(os.getenv("KALSHI_LIVE_ENABLED", "false"))
    shadow_only_lock = _bool(os.getenv("KALSHI_SHADOW_ONLY", "false"))
    live = live_mode_allowed(requested_live, environment_live, shadow_only_lock, bool(args.dry_run))
    dry_run = not live
    if requested_live and not live and not args.reconcile_only:
        raise SystemExit("LIVE REQUEST BLOCKED | explicit live permission required and shadow-only lock must be off; no orders sent")
    # Reconciliation-only intentionally opens no risk but reads the live
    # namespace/configuration, so it is the sole dry execution allowed to use
    # trading_mode=live.
    expected_mode = "live" if live or args.reconcile_only else "shadow"
    if config["trading_mode"] != expected_mode:
        raise SystemExit(
            f"configuration trading_mode={config['trading_mode']} does not match guarded runtime mode={expected_mode}"
        )
    LOG.warning(
        "MODE=%s | strategy=%s config_hash=%s opening_price_capture_contract=v%s",
        "RECONCILE_ONLY" if args.reconcile_only else "LIVE" if live else "DRY_RUN", config["strategy_version"], config_hash(config)[:12],
        OPENING_PRICE_CAPTURE_CONTRACT_VERSION,
    )
    api_key = os.getenv("KALSHI_API_KEY_ID", "")
    pem_path = Path(os.getenv("KALSHI_PEM_PATH", "kalshi_private_key.pem"))
    if not api_key or not pem_path.exists():
        raise SystemExit("Kalshi authentication is required; credentials are never logged")
    state = load_state(args.state_file, config)
    startup_check = state.get("startup_order_check")
    startup_check_required = (
        live
        and _bool(os.getenv("GITHUB_ACTIONS", "false"))
        and _bool(os.getenv("KALSHI_STARTUP_ORDER_CHECK_ENABLED", "true"))
    )
    if startup_check_required and not startup_order_check_allows_worker(
        state, os.getenv("GITHUB_RUN_ID", ""),
    ):
        raise SystemExit(
            "LIVE STARTUP BLOCKED | current GitHub run lacks exact canceled/no-fill shard-2 order proof"
        )
    if live and isinstance(startup_check, dict):
        LOG.warning(
            "STARTUP ORDER CHECK | state=%s worker_id=%s ticker=%s shard=%s limit=%s "
            "quantity=%s create_ack=%s cancel_ack=%s fills=%s remaining=%s position=%s",
            startup_check.get("state"), startup_check.get("worker_id"),
            startup_check.get("ticker"), startup_check.get("exchange_index"),
            startup_check.get("economic_limit"), startup_check.get("quantity"),
            startup_check.get("create_acknowledged"), startup_check.get("cancel_acknowledged"),
            startup_check.get("filled_quantity"), startup_check.get("remaining_quantity"),
            startup_check.get("position"),
        )
    # Never checkpoint an override until its hash has been accepted against
    # durable state. An active-order rejection must leave the last known-good
    # config in place so the watchdog can restart it without manual repair.
    if args.persist_config:
        save_config(args.config, config)
        LOG.warning(
            "CONFIG SAVED LOCALLY | initial_base_shares=%s recovery=DISABLED position_cap=DISABLED "
            "base_scaling=DISABLED sticky_trigger_band=%s-%sc opposite_initial=ask_minus_%sc "
            "rungs=40c@2x,30c@4x,20c@8x,10c@16x take_profit_bid=%sc "
            "mode=%s hash=%s | blank_inputs=preserve_restored_values remote_persistence=requires_successful_checkpoint "
            "v13_recovery_state=NOT_IMPORTED",
            config["starting_base"], config["delayed_entry_threshold_cents"],
            config["delayed_entry_max_trigger_cents"], config["entry_limit_offset_cents"],
            config["opposite_take_profit_cents"], expected_mode, config_hash(config)[:12],
        )
    migrations = state.get("config_migrations", [])
    if migrations and migrations[-1].get("kind") == "disable_recovery_exponent_breaker":
        LOG.warning(
            "CONFIG MIGRATION | recovery exponent breaker disabled; preserved exponent=%s deficit=%s",
            state.get("sizing", {}).get("recovery_exponent", 0),
            state.get("sizing", {}).get("recovery_cycle_pnl", "0"),
        )
    if migrations and migrations[-1].get("kind") == "make_existing_gtc_order_contract_explicit":
        LOG.warning(
            "CONFIG MIGRATION | pre-existing maker behavior recorded explicitly as GTC; "
            "preserved exponent=%s deficit=%s markets=%d",
            state.get("sizing", {}).get("recovery_exponent", 0),
            state.get("sizing", {}).get("recovery_cycle_pnl", "0"),
            len(state.get("markets", {})),
        )
    if migrations and migrations[-1].get("kind") == "remove_gtc_strategy_timeout":
        LOG.warning(
            "CONFIG MIGRATION | GTC strategy timeout removed; entries now rest until fill, "
            "confirmed risk cancellation, or market close; preserved exponent=%s deficit=%s markets=%d",
            state.get("sizing", {}).get("recovery_exponent", 0),
            state.get("sizing", {}).get("recovery_cycle_pnl", "0"),
            len(state.get("markets", {})),
        )
    if migrations and migrations[-1].get("kind") == "enable_compact_full_market_delayed_entry_analytics":
        LOG.warning(
            "CONFIG MIGRATION | full-market delayed >=53c analytics enabled; preserved "
            "base=%s exponent=%s deficit=%s markets=%d; legacy records remain partial coverage",
            state.get("sizing", {}).get("base_share_count", "1.00"),
            state.get("sizing", {}).get("recovery_exponent", 0),
            state.get("sizing", {}).get("recovery_cycle_pnl", "0"),
            len(state.get("markets", {})),
        )
    if args.reset_state:
        if state_has_reset_blocking_risk(state):
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
        if active_positions or await LiveEngine(config, state, args.state_file, args.audit_ledger, dry_run, reconcile_only=args.reconcile_only).managed_orders(rest):
            await rest.close()
            raise SystemExit("refusing reset_state while exchange KXBTC15M exposure or managed orders exist")
        reset_at = utc_now()
        prior_sizing = state.get("sizing", {})
        prior_fees = state.get("fee_metrics", {})
        reset_summary = {
            "at": reset_at,
            "reason": "explicit_operator_fresh_state_request",
            "previous_base": prior_sizing.get("base_share_count", config["starting_base"]),
            "previous_recovery_exponent": prior_sizing.get("recovery_exponent", 0),
            "previous_recovery_cycle_pnl": prior_sizing.get("recovery_cycle_pnl", "0"),
            "previous_cumulative_realized_pnl": state.get("cumulative_realized_pnl", "0"),
            "previous_total_fees_paid": prior_fees.get("total_fees_paid", "0"),
            "previous_completed_trade_count": prior_sizing.get("completed_trade_count", 0),
            "reset_base": config["starting_base"],
            "reset_recovery_exponent": 0,
            "reset_recovery_cycle_pnl": "0.00",
            "reset_total_fees_paid": "0.00",
        }
        # Retain the append-only forensic ledger while replacing only the
        # active strategy state. This makes the reset auditable without
        # allowing old fills, losses, or fees to seed the new recovery cycle.
        append_audit(args.audit_ledger, {
            "event": "fresh_strategy_state_reset",
            "strategy_version": config["strategy_version"],
            "config_hash": config_hash(config),
            **reset_summary,
        })
        state = load_state(Path("/nonexistent"), config)
        state["fresh_reset"] = reset_summary
        LOG.warning(
            "FRESH STRATEGY STATE | exchange_flat=true managed_orders=0 "
            "base=%s exponent=0 deficit=0.00 fees=0.00 prior_exponent=%s "
            "prior_deficit=%s prior_fees=%s audit_preserved=true",
            config["starting_base"], reset_summary["previous_recovery_exponent"],
            reset_summary["previous_recovery_cycle_pnl"],
            reset_summary["previous_total_fees_paid"],
        )
    feed = KalshiLiveFeed(rest.auth)
    feed_task = asyncio.create_task(feed.run(), name="kalshi-hybrid-live-feed")
    engine = LiveEngine(config, state, args.state_file, args.audit_ledger, dry_run, args.config, reconcile_only=args.reconcile_only)
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
