"""Atomic durable state for the KXBTC15M hybrid live strategy."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any


STATE_VERSION = 1
UNCAPPED_RECOVERY_EXPONENT = 0
LEGACY_RECOVERY_EXPONENT_LIMIT = 12
TUNABLE_STRATEGY_FIELDS = {
    "starting_base",
    "recovery_multiplier",
    "threshold_growth_multiplier",
    "first_base_threshold",
    "base_increment",
    "hybrid_stop_trigger_cents",
    "hybrid_maker_exit_cents",
    "hybrid_hard_stop_cents",
}
TUNABLE_OPERATIONAL_FIELDS = {"trading_mode"}
CONFIG_TUNING_ACTIVE_STATES = {
    "SIGNAL_PENDING",
    "ENTRY_PENDING",
    "ENTRY_PARTIAL",
    "POSITION_OPEN",
    "STOP_PENDING",
    "SETTLEMENT_PENDING",
    "ENTRY_CANCEL_UNCONFIRMED",
    "RECONCILIATION_PENDING",
    "ACCOUNTING_RECONCILIATION_PENDING",
    "MAKER_EXIT_PENDING",
    "MAKER_EXIT_PARTIAL",
    "MAKER_EXIT_CANCEL_UNCONFIRMED",
    "HARD_STOP_PENDING",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def config_hash(config: dict[str, Any]) -> str:
    stable = json.dumps(config, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(stable.encode("utf-8")).hexdigest()


def _flat_for_config_tuning(value: dict[str, Any]) -> bool:
    """Only permit a reviewed parameter change when no order/risk is active."""

    try:
        if Decimal(str(value.get("current_position", "0"))) != 0:
            return False
    except (InvalidOperation, TypeError, ValueError):
        return False
    if value.get("current_order_id"):
        return False
    return not any(
        isinstance(record, dict) and record.get("status") in CONFIG_TUNING_ACTIVE_STATES
        for record in value.get("markets", {}).values()
    )


def default_state(config: dict[str, Any]) -> dict[str, Any]:
    return {
        "state_version": STATE_VERSION,
        "strategy_version": config["strategy_version"],
        "config_hash": config_hash(config),
        # This snapshot lets a later worker prove that a config-hash change is
        # limited to reviewed workflow inputs. Hash mismatch alone cannot show
        # which fields changed, so older states first acquire this while their
        # hash still matches before any tuning is accepted.
        "active_config_snapshot": dict(config),
        "config_migrations": [],
        "created_at": utc_now(),
        "updated_at": utc_now(),
        "sizing": {},
        "active_market": None,
        "current_order_id": None,
        "current_position": "0.00",
        "average_entry": None,
        "markets": {},
        "provisional_outcomes": {},
        "preloaded_markets": [],
        # Directional side is intentionally independent of execution.  The
        # v8 shadow experiment holds a losing side until that side eventually
        # settles correctly, then flips for the following market.
        "directional_signal_state": {
            "mode": config.get("signal_mode", "sticky_until_directional_win"),
            "active_side": None,
            "last_source_market": None,
            "last_source_outcome": None,
            "last_transition": None,
            "updated_at": None,
        },
        "processed_settlements": [],
        "outcome_verification": {"provisional": 0, "verified": 0, "matches": 0, "mismatches": 0},
        # Recomputed from durable per-market actual fills.  Keeping this
        # aggregate separate from order requests makes maker/IOC participation
        # measurable across runner handoffs without double counting retries.
        "entry_execution_metrics": {},
        # Rebuilt from individual market timing records; telemetry only, not
        # an input to sizing, funding, or realized P&L.
        "execution_timing_metrics": {},
        # Idempotently rebuilt from durable per-market v11 analytics facts.
        "entry_price_performance": {},
        "delayed_entry_performance": {},
        "hybrid_stop_performance": {},
        "fee_metrics": {"entry_fees_paid": "0", "exit_fees_paid": "0", "total_fees_paid": "0"},
        "circuit_breaker": {"blocked": False, "reason": None, "triggered_at": None},
        "daily_realized": {},
        "shadow_metrics": {},
        "cumulative_realized_pnl": "0.00",
        "peak_equity": "0.00",
        "last_completed_trade": None,
        "api_failure_count": 0,
        "last_reconciliation": None,
        "handoff": None,
    }


def load_state(path: Path, config: dict[str, Any]) -> dict[str, Any]:
    if not path.exists():
        return default_state(config)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"cannot load durable live state: {exc}") from exc
    if not isinstance(value, dict) or int(value.get("state_version", 0)) != STATE_VERSION:
        raise RuntimeError("live state has an unsupported schema; fail closed rather than migrate unknown risk")
    # A recovery cycle is defined by its exact configuration.  Loading a v8
    # maker/entry-adjusted-stop checkpoint under v9 would silently reinterpret
    # exposure and P&L, so reject it rather than attempting a migration.  The
    # workflow gives v9 its own durable state/ledger namespace.
    if value.get("strategy_version") != config.get("strategy_version"):
        raise RuntimeError("live state strategy version differs from active configuration; fail closed")
    expected_hash = config_hash(config)
    if value.get("config_hash") != expected_hash:
        # Only narrowly-scoped operational migrations are supported. Verify
        # the exact prior hash before preserving state; every other change
        # still fails closed. The first changed the recovery-exponent circuit
        # breaker from 12 to the explicit zero sentinel (disabled).
        legacy_config = dict(config)
        legacy_config["max_recovery_exponent"] = LEGACY_RECOVERY_EXPONENT_LIMIT
        is_uncapped_migration = (
            int(config.get("max_recovery_exponent", LEGACY_RECOVERY_EXPONENT_LIMIT))
            == UNCAPPED_RECOVERY_EXPONENT
            and value.get("config_hash") == config_hash(legacy_config)
        )
        # The maker engine already submitted both entry and maker-exit orders
        # as GTC before this field became explicit.  Permit only the exact
        # prior hash obtained by removing this one declarative field.  This
        # makes the contract fail closed without resetting or reinterpreting
        # any existing fills, position, recovery state, P&L, or analytics.
        implicit_gtc_config = dict(config)
        implicit_gtc_config.pop("maker_order_time_in_force", None)
        is_explicit_gtc_migration = (
            config.get("maker_order_time_in_force") == "good_till_canceled"
            and value.get("config_hash") == config_hash(implicit_gtc_config)
        )
        # v11 originally bounded the resting maker entry with a 60-second
        # strategy timer. The new contract leaves the exchange GTC order
        # resting until it fills, market close, or a confirmed risk-driven
        # cancellation. Reverse only these exact declarative changes when
        # validating the persisted checkpoint hash; all trading/accounting
        # state remains byte-for-byte meaningful under the new lifetime.
        bounded_gtc_config = dict(config)
        bounded_gtc_config["entry_timeout_seconds"] = bounded_gtc_config.pop(
            "opening_quote_capture_seconds"
        )
        bounded_gtc_config.pop("entry_order_lifetime", None)
        bounded_implicit_gtc_config = dict(bounded_gtc_config)
        bounded_implicit_gtc_config.pop("maker_order_time_in_force", None)
        is_persistent_gtc_migration = (
            int(config.get("entry_timeout_seconds", -1)) == 0
            and config.get("entry_order_lifetime") == "until_filled_or_market_close"
            and value.get("config_hash") in {
                config_hash(bounded_gtc_config),
                config_hash(bounded_implicit_gtc_config),
            }
        )
        # Adding the compact full-market >=53c tracker changes only durable
        # analytics. It does not reinterpret an order, fill, position, P&L,
        # recovery cycle, or stop. Accept exactly the prior configuration
        # hash with these two new declarative fields absent.
        pre_delayed_entry_analytics_config = dict(config)
        pre_delayed_entry_analytics_config.pop("delayed_entry_tracking_enabled", None)
        pre_delayed_entry_analytics_config.pop("delayed_entry_threshold_cents", None)
        is_delayed_entry_analytics_migration = (
            bool(config.get("delayed_entry_tracking_enabled"))
            and int(config.get("delayed_entry_threshold_cents", 0)) == 53
            and value.get("config_hash") == config_hash(pre_delayed_entry_analytics_config)
        )
        prior_snapshot = value.get("active_config_snapshot")
        changed_fields: set[str] = set()
        if isinstance(prior_snapshot, dict):
            changed_fields = {
                name
                for name in set(prior_snapshot) | set(config)
                if prior_snapshot.get(name) != config.get(name)
            }
        tuning_fields = TUNABLE_STRATEGY_FIELDS | TUNABLE_OPERATIONAL_FIELDS
        negative_cycle = Decimal(str(value.get("sizing", {}).get("recovery_cycle_pnl", "0"))) < 0
        cycle_is_preserved = not negative_cycle or isinstance(value.get("cycle_strategy_parameters"), dict)
        is_reviewed_tuning = (
            bool(changed_fields)
            and changed_fields <= tuning_fields
            and _flat_for_config_tuning(value)
            and cycle_is_preserved
        )
        if not any((
            is_uncapped_migration,
            is_explicit_gtc_migration,
            is_persistent_gtc_migration,
            is_delayed_entry_analytics_migration,
            is_reviewed_tuning,
        )):
            raise RuntimeError("live state configuration hash differs from active configuration; fail closed")
        migrations = value.setdefault("config_migrations", [])
        if is_uncapped_migration:
            migrations.append({
                "at": utc_now(),
                "kind": "disable_recovery_exponent_breaker",
                "previous_max_recovery_exponent": LEGACY_RECOVERY_EXPONENT_LIMIT,
                "max_recovery_exponent": UNCAPPED_RECOVERY_EXPONENT,
                "previous_config_hash": value.get("config_hash"),
                "config_hash": expected_hash,
                "policy": "preserve_existing_recovery_cycle_without_reinterpretation",
            })
        if is_explicit_gtc_migration:
            migrations.append({
                "at": utc_now(),
                "kind": "make_existing_gtc_order_contract_explicit",
                "maker_order_time_in_force": "good_till_canceled",
                "previous_config_hash": value.get("config_hash"),
                "config_hash": expected_hash,
                "policy": "preserve_all_existing_state_because_order_execution_was_already_gtc",
            })
        if is_persistent_gtc_migration:
            migrations.append({
                "at": utc_now(),
                "kind": "remove_gtc_strategy_timeout",
                "previous_entry_timeout_seconds": bounded_gtc_config["entry_timeout_seconds"],
                "entry_timeout_seconds": 0,
                "entry_order_lifetime": "until_filled_or_market_close",
                "opening_quote_capture_seconds": config["opening_quote_capture_seconds"],
                "previous_config_hash": value.get("config_hash"),
                "config_hash": expected_hash,
                "policy": "preserve_existing_state_and_apply_unbounded_lifetime_to_open_and_future_gtc_entries",
            })
        if is_delayed_entry_analytics_migration:
            migrations.append({
                "at": utc_now(),
                "kind": "enable_compact_full_market_delayed_entry_analytics",
                "delayed_entry_tracking_enabled": True,
                "delayed_entry_threshold_cents": 53,
                "previous_config_hash": value.get("config_hash"),
                "config_hash": expected_hash,
                "policy": "preserve_all_execution_and_accounting_state; new records receive complete analytics coverage",
            })
        if is_reviewed_tuning:
            migrations.append({
                "at": utc_now(),
                "kind": "apply_reviewed_flat_state_strategy_tuning",
                "changed_fields": sorted(changed_fields),
                "previous_config_hash": value.get("config_hash"),
                "config_hash": expected_hash,
                "policy": "open_records_and_negative_recovery_cycle_keep_their_creation_parameters",
            })
        value["config_hash"] = expected_hash
        value["active_config_snapshot"] = dict(config)
    for key, default in default_state(config).items():
        value.setdefault(key, default)
    return value


def save_state(path: Path, state: dict[str, Any]) -> None:
    state["updated_at"] = utc_now()
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(state, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        # ``replace`` is atomic, but the parent directory owns the rename
        # metadata.  Sync it as well so an acknowledged strategy checkpoint
        # survives a host crash, not merely a graceful worker shutdown.
        try:
            directory = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        except OSError:
            # The state file itself is already fsynced.  Directory fsync is
            # unavailable on a few development filesystems.
            pass
    except Exception:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


def append_unique(values: list[str], value: str, maximum: int = 2_000) -> None:
    if value not in values:
        values.append(value)
    if len(values) > maximum:
        del values[:-maximum]
