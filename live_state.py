"""Atomic durable state for the KXBTC15M hybrid live strategy."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


STATE_VERSION = 1


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def config_hash(config: dict[str, Any]) -> str:
    stable = json.dumps(config, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(stable.encode("utf-8")).hexdigest()


def default_state(config: dict[str, Any]) -> dict[str, Any]:
    return {
        "state_version": STATE_VERSION,
        "strategy_version": config["strategy_version"],
        "config_hash": config_hash(config),
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
    if value.get("config_hash") != config_hash(config):
        raise RuntimeError("live state configuration hash differs from active configuration; fail closed")
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
