"""Fail-closed GitHub Actions startup order/cancellation health check.

This is not a strategy order.  A LIVE worker startup may create exactly one
1.00-contract, one-cent, post-only order on the market's authoritative shard,
then immediately cancel it.  Strategy execution remains blocked until REST
proves: the create was acknowledged, the exact order cancellation was
acknowledged, remaining quantity is zero, no fill exists, and the position is
flat.  The durable intent is checkpointed before the non-idempotent POST.

The check never retries creation. It can clear a terminal maker-entry breaker
only after exact closed-market V2 order/fill/position proof.  That covers both
an old unknown POST response and a definitive HTTP rejection: neither is
allowed to become a permanent 24/7 halt after the source market has closed,
but neither may be discarded without authoritative exchange proof.  Every
other breaker remains fail-closed. It is deliberately unavailable in shadow
and reconciliation-only modes.
"""
from __future__ import annotations

import argparse
import asyncio
from contextlib import suppress
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import re
import time

from audit_ledger import append_audit
from kalshi_order_smoke_test import (
    CREATE, KIND, PRICE, QUANTITY, SmokeApi, SafetyError, checked_id, emit,
    find_order, plan_for, preflight, quote_stream, reconcile, validate_market,
)
from kalshi_shard_admin import ApiError, Journal, epoch, money, operation_lock, pages, shard
from live_checkpoint import DELAYED_V12_RUNTIME_STATE_REF, publish_runtime_snapshot
from live_state import default_state, save_state, utc_now


DEFAULT_ROOT = Path("data/.kalshi_live_delayed_band_v12_startup_order_check")
DEFAULT_STATE = Path("data/kalshi_live_delayed_band_v12_state.json")
DEFAULT_CONFIG = Path("selected_live_strategy.json")
DEFAULT_AUDIT = Path("data/kalshi_live_delayed_band_v12_audit.jsonl")
WORKER_ID = re.compile(r"[A-Za-z0-9_.:-]{1,128}\Z")
TERMINAL_PASS = "CANCELED_NO_FILL"
BOUNDARY_WAIT_SECONDS = 90


def enabled_live_environment() -> bool:
    truthy = {"1", "true", "yes", "on"}
    return (
        os.getenv("GITHUB_ACTIONS", "").strip().lower() in truthy
        and os.getenv("KALSHI_LIVE_ENABLED", "").strip().lower() in truthy
        and os.getenv("KALSHI_SHADOW_ONLY", "true").strip().lower() not in truthy
        and os.getenv("KALSHI_STARTUP_ORDER_CHECK_ENABLED", "true").strip().lower() in truthy
    )


def load_strategy_safety_state(
    path: Path, config_path: Path, *, allow_recoverable_entry_breaker: bool = False,
) -> dict:
    if path.is_symlink():
        raise SafetyError("Live strategy state is symlinked; startup order check blocked")
    if not path.exists():
        try:
            from kalshi_live_trader import load_config
            save_state(path, default_state(load_config(config_path)))
        except Exception:
            raise SafetyError("Fresh live strategy state could not be initialized safely") from None
    if not path.is_file():
        raise SafetyError("Live strategy state is unavailable; startup order check blocked")
    try:
        value = json.loads(path.read_text())
    except (OSError, ValueError):
        raise SafetyError("Live strategy state is unreadable; startup order check blocked") from None
    if not isinstance(value, dict):
        raise SafetyError("Live strategy state is invalid; startup order check blocked")
    breaker = value.get("circuit_breaker")
    if not isinstance(breaker, dict):
        raise SafetyError("Strategy breaker state is unavailable; startup order check blocked")
    recoverable_reasons = {
        "maker_entry_submission_unknown", "maker_entry_submission_rejected",
    }
    if breaker.get("blocked") and not (
        allow_recoverable_entry_breaker
        and breaker.get("reason") in recoverable_reasons
    ):
        reason = str(breaker.get("reason") or "unknown")
        raise SafetyError(f"Strategy circuit breaker remains active ({reason}); reconcile it before any probe")
    if value.get("current_order_id") or Decimal(str(value.get("current_position") or "0")) != 0:
        raise SafetyError("Strategy state reports an order or position; startup probe requires no strategy exposure")
    return value


def _recoverable_entry_intents(state: dict, reason: str) -> list[tuple[dict, dict]]:
    result = []
    for record in state.get("markets", {}).values():
        if not isinstance(record, dict):
            continue
        for order in record.get("entry_orders", []):
            if not isinstance(order, dict):
                continue
            unknown = (
                reason == "maker_entry_submission_unknown"
                and order.get("submission_outcome") != "rejected"
            )
            rejected = (
                reason == "maker_entry_submission_rejected"
                and order.get("submission_outcome") == "rejected"
                and order.get("http_status") in {400, 401, 403, 404, 422}
            )
            if (not order.get("order_id")
                    and order.get("status") in {"submitting", "submit_failed"}
                    and (unknown or rejected)):
                result.append((record, order))
    return result


async def resolve_terminal_entry_breaker(
    args: argparse.Namespace, api: SmokeApi, state: dict, journal: Journal,
    *, publisher=None,
) -> dict:
    """Clear a terminal entry-submission breaker after exact REST proof.

    A missing HTTP acknowledgment is not proof of rejection.  Resolution
    therefore requires all-shard-2 preflight to have proved no resting orders
    and no positions, then checks the exact deterministic client order ID in
    order and fill history and confirms the market can no longer trade.  Any
    exposure, fill, live market, malformed response, or unmatched state remains
    blocked for manual reconciliation.
    """

    if publisher is None:
        publisher = publish_required
    breaker = state.get("circuit_breaker", {})
    if not breaker.get("blocked"):
        return state
    reason = str(breaker.get("reason") or "")
    if reason not in {"maker_entry_submission_unknown", "maker_entry_submission_rejected"}:
        raise SafetyError("Only a maker-entry submission breaker has an automatic proof path")
    intents = _recoverable_entry_intents(state, reason)
    if not intents:
        raise SafetyError("Entry breaker has no exact persisted submission intent; manual review required")

    resolved = []
    for record, intent in intents:
        ticker = checked_id(record.get("ticker"))
        client_id = checked_id(intent.get("client_order_id"))
        market_payload = await api.request("GET", "/markets/" + ticker)
        market = market_payload.get("market")
        if not isinstance(market, dict) or market.get("ticker") != ticker:
            raise SafetyError("Entry intent market metadata could not be verified")
        close_at = epoch(market.get("close_time"))
        if market.get("status") in {"active", "open"} and time.time() < close_at:
            raise SafetyError("Entry intent market remains tradable; reconciliation must wait for close")
        if shard(market.get("exchange_index")) != 2:
            raise SafetyError("Entry intent is not on the expected market shard")

        orders = await pages(
            api, "/portfolio/orders", "orders", exchange_index=2,
            ticker=ticker, subaccount=0, limit=1000,
        )
        matches = [row for row in orders if row.get("client_order_id") == client_id]
        if len(matches) > 1:
            raise SafetyError("Multiple exchange orders match the entry intent")
        order_ids = set()
        if matches:
            order = matches[0]
            order_id = checked_id(order.get("order_id"))
            order_ids.add(order_id)
            filled = money(order.get("fill_count", order.get("fill_count_fp", "0")))
            remaining = money(order.get("remaining_count", order.get("remaining_count_fp", "0")))
            status = str(order.get("status") or "").lower()
            if filled != 0 or remaining != 0 or status not in {
                "canceled", "cancelled", "expired", "executed", "filled",
            }:
                raise SafetyError("Exchange order is not proven terminal and unfilled")

        fills = await pages(
            api, "/portfolio/fills", "fills", exchange_index=2,
            ticker=ticker, subaccount=0, limit=1000,
        )
        linked_fills = [row for row in fills if (
            row.get("client_order_id") == client_id
            or (row.get("order_id") and row.get("order_id") in order_ids)
        )]
        if linked_fills:
            raise SafetyError("A fill exists for the entry submission; preserve state for accounting")

        # The shard-wide position/open-order preflight was already flat.  This
        # exact history proves this closed market's unknown POST created no
        # exposure.  Preserve the intent and append the reconciliation facts;
        # do not erase the forensic record.
        intent.update({
            "status": "reconciled_terminal_no_fill",
            "submission_outcome": "reconciled_terminal_no_fill",
            "fill_count": "0.00", "remaining_count": "0.00",
            "reconciled_at": utc_now(), "reconciled_via": "v2_order_fill_position_proof",
        })
        if record.get("status") in {"ERROR_RECONCILIATION", "RECONCILIATION_PENDING"}:
            record.update({
                "status": "ZERO_FILL", "status_reason": "entry_submission_proven_terminal_unfilled",
                "updated_at": utc_now(),
            })
        resolved.append({"ticker": ticker, "client_order_id": client_id})

    state["circuit_breaker"] = {
        "blocked": False, "reason": None, "triggered_at": None,
        "last_resolution": {
            "at": utc_now(), "kind": "maker_entry_submission_proven_terminal_unfilled",
            "original_breaker_reason": reason,
            "resolved_intents": len(resolved), "exchange_index": 2,
        },
    }
    state["current_order_id"] = None
    state["current_position"] = "0.00"
    state["entry_submission_health"] = {
        "mode": "live", "recorded_entry_attempts": sum(
            len(record.get("entry_orders", [])) for record in state.get("markets", {}).values()
            if isinstance(record, dict)
        ),
        "exchange_acknowledgments": sum(
            bool(order.get("order_id")) for record in state.get("markets", {}).values()
            if isinstance(record, dict) for order in record.get("entry_orders", []) if isinstance(order, dict)
        ),
        "definitive_rejections": sum(
            order.get("submission_outcome") == "rejected"
            for record in state.get("markets", {}).values() if isinstance(record, dict)
            for order in record.get("entry_orders", []) if isinstance(order, dict)
        ),
        "unresolved_submissions": 0, "breaker_blocked": False, "breaker_reason": None,
    }
    save_state(args.state_file, state)
    append_audit(args.audit_ledger, {
        "event": "entry_submission_breaker_resolved", "at": utc_now(),
        "reason": "v2_order_fill_position_proof", "exchange_index": 2,
        "original_breaker_reason": reason,
        "resolved_intents": resolved, "orders_sent": 0,
    })
    publisher(args, journal, "maker-entry-breaker-resolved")
    emit(
        action="ENTRY_BREAKER_RESOLVED", reason="terminal_no_order_no_fill_no_position",
        original_breaker_reason=reason, resolved_intents=len(resolved), exchange_index=2, orders_sent=0,
    )
    return state


def checkpoint_paths(args: argparse.Namespace, journal: Journal) -> tuple[Path, ...]:
    journal_path = journal.root / (KIND + ".json")
    candidates = (args.config, args.state_file, args.audit_ledger, journal_path)
    return tuple(path for path in candidates if path.exists())


def record_strategy_probe_state(args: argparse.Namespace, operation: dict) -> None:
    state = load_strategy_safety_state(args.state_file, args.config)
    plan = operation.get("plan", {})
    state["startup_order_check"] = {
        "state": operation.get("state"),
        "worker_id": operation.get("startup_worker_id"),
        "ticker": plan.get("ticker"),
        "exchange_index": plan.get("exchange_index"),
        "selected_side": operation.get("side"),
        "economic_limit": "0.01",
        "quantity": "1.00",
        "order_id": operation.get("order_id"),
        "client_order_id": plan.get("client_order_id"),
        "create_acknowledged": bool(operation.get("accepted_verified")),
        "cancel_acknowledged": bool(operation.get("cancel_acknowledged")),
        "filled_quantity": operation.get("filled_quantity"),
        "remaining_quantity": operation.get("remaining_quantity"),
        "position": operation.get("position"),
        "checked_at": utc_now(),
    }
    save_state(args.state_file, state)


def publish_required(args: argparse.Namespace, journal: Journal, reason: str) -> None:
    """A remote pre-POST intent is mandatory inside Actions."""

    publish_runtime_snapshot(
        checkpoint_paths(args, journal), reason,
        runtime_ref=args.runtime_ref,
    )


def choose_safe_side(feed, market: dict):
    candidates = []
    for side in ("yes", "no"):
        with suppress(SafetyError):
            quote = feed.fresh(market["ticker"], side)
            candidates.append((quote["selected_bid"] - PRICE, side, quote))
    if not candidates:
        raise SafetyError("Neither side has a sufficiently deep fresh quote for the one-cent probe")
    _, side, quote = max(candidates, key=lambda item: (item[0], item[1] == "yes"))
    return side, quote


async def preflight_current_when_safe(api: SmokeApi) -> dict:
    """Wait across one boundary only when the current market is too near close."""

    deadline = time.monotonic() + BOUNDARY_WAIT_SECONDS
    waiting_logged = False
    while True:
        try:
            return await preflight(api, None, "yes")
        except SafetyError as exc:
            if str(exc) != "Market is not open or has less than 60 seconds remaining":
                raise
            if time.monotonic() >= deadline:
                raise SafetyError("No safely open KXBTC15M market appeared within the boundary wait") from None
            if not waiting_logged:
                emit(
                    action="STARTUP_ORDER_CHECK_WAITING_FOR_NEXT_MARKET",
                    reason="current_market_has_less_than_60_seconds_remaining",
                    maximum_wait_seconds=BOUNDARY_WAIT_SECONDS, orders_sent=0,
                )
                waiting_logged = True
            await asyncio.sleep(1)


async def run_startup_check(
    args: argparse.Namespace, api: SmokeApi, journal: Journal, *, stream=quote_stream,
    publisher=publish_required,
) -> None:
    if not args.execute:
        raise SafetyError("Startup order check requires explicit --execute")
    if not enabled_live_environment():
        raise SafetyError("Startup order check is allowed only for an explicitly enabled LIVE GitHub worker")
    if not WORKER_ID.fullmatch(args.worker_id or ""):
        raise SafetyError("A valid durable GitHub worker ID is required")
    state = load_strategy_safety_state(
        args.state_file, args.config, allow_recoverable_entry_breaker=True,
    )

    fingerprint = hashlib.sha256(api.key_id.encode()).hexdigest()
    prior = journal.load(KIND)
    if prior:
        if prior.get("credential_fingerprint") != fingerprint:
            raise SafetyError("Startup journal belongs to another credential; manual review required")
        await reconcile(api, journal, prior, cancel=True, attempts=10, require_cancel_ack=True)
        if prior.get("state") != TERMINAL_PASS or not prior.get("cancel_acknowledged"):
            raise SafetyError("Previous startup probe is not proven canceled and flat")
        if prior.get("startup_worker_id") == args.worker_id and not state["circuit_breaker"].get("blocked"):
            record_strategy_probe_state(args, prior)
            emit(action="STARTUP_ORDER_CHECK_ALREADY_PASSED", worker_id=args.worker_id,
                 order_id=prior.get("order_id"), orders_sent=0)
            return

    # Preflight obtains the market from Kalshi and verifies its exchange_index,
    # write scope, shard-specific balance, all resting orders, and all positions.
    market = await preflight_current_when_safe(api)
    if state["circuit_breaker"].get("blocked"):
        state = await resolve_terminal_entry_breaker(
            args, api, state, journal, publisher=publisher,
        )
        # Refuse to rely on the state mutation alone. Re-run the strict local
        # gate and the full exchange preflight before considering a new POST.
        load_strategy_safety_state(args.state_file, args.config)
        market = await preflight(api, market["ticker"], "yes")
    if prior and prior.get("startup_worker_id") == args.worker_id:
        record_strategy_probe_state(args, prior)
        emit(action="STARTUP_ORDER_CHECK_ALREADY_PASSED", worker_id=args.worker_id,
             order_id=prior.get("order_id"), orders_sent=0)
        return
    async with stream(api, market["ticker"], "yes") as feed:
        side, quote = choose_safe_side(feed, market)
        market = await preflight(api, market["ticker"], side)
        plan = plan_for(market, side, client_scope=f"startup:{args.worker_id}")
        if await find_order(api, plan):
            raise SafetyError("Matching startup order already exists without the durable current intent")
        # Do not let REST preflight latency make the quote stale silently.
        quote = feed.fresh(market["ticker"], side)

        operation = {
            "schema_version": 1,
            "kind": KIND,
            "state": "SUBMISSION_INTENT",
            "credential_fingerprint": fingerprint,
            "startup_worker_id": args.worker_id,
            "side": side,
            "plan": plan,
            "selected_bid": str(quote["selected_bid"]),
            "selected_ask": str(quote["selected_ask"]),
            "created_at": utc_now(),
            "post_attempts": 1,
            "accepted_verified": False,
            "cancel_acknowledged": False,
        }
        journal.save(operation)
        record_strategy_probe_state(args, operation)
        publisher(args, journal, "startup-order-check-intent")
        # Publishing the non-idempotent intent can take long enough for a
        # quote or market boundary to change. Revalidate both immediately
        # before consuming the single POST capability.
        market = await preflight(api, market["ticker"], side)
        quote = feed.fresh(market["ticker"], side)
        validate_market(market, side)
        emit(
            action="STARTUP_ORDER_CHECK_SUBMIT", worker_id=args.worker_id,
            ticker=market["ticker"], selected_side=side, economic_limit=PRICE,
            quantity=QUANTITY, exchange_index=market["exchange_index"],
            selected_bid=quote["selected_bid"], selected_ask=quote["selected_ask"],
            post_only=True, cancel="IMMEDIATE_AFTER_ACK", post_attempts=1,
        )
        api.permitted_plan = dict(plan)
        try:
            response = await api.request("POST", CREATE, body=plan)
            order_id = checked_id(response.get("order_id"))
            if response.get("client_order_id") not in {None, plan["client_order_id"]}:
                raise SafetyError("Startup order acknowledgment identity mismatch")
            operation.update(order_id=order_id, state="ACK_RECEIVED", acknowledged_at=utc_now())
            journal.save(operation)
            emit(action="STARTUP_ORDER_CHECK_ACK", worker_id=args.worker_id,
                 order_id=order_id, client_order_id=plan["client_order_id"])
        except ApiError as exc:
            operation["state"] = (
                "SUBMISSION_REJECTED" if exc.status in {400, 401, 403, 404, 422}
                else "SUBMISSION_UNKNOWN"
            )
            journal.save(operation)
            emit(action=operation["state"], http_status=exc.status, code=exc.code,
                 retry_create=False, **exc.server_diagnostics)
        finally:
            await reconcile(api, journal, operation, cancel=True, attempts=10, require_cancel_ack=True)

    if operation.get("state") != TERMINAL_PASS or not operation.get("cancel_acknowledged"):
        raise SafetyError("Startup order did not complete its verified canceled/no-fill lifecycle")
    record_strategy_probe_state(args, operation)
    publisher(args, journal, "startup-order-check-passed")
    emit(action="STARTUP_ORDER_CHECK_PASS", worker_id=args.worker_id,
         order_id=operation.get("order_id"), exchange_index=market["exchange_index"],
         create_acknowledged=True, cancel_acknowledged=True, fill_quantity="0",
         position="0", strategy_entries_allowed=True)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--execute", action="store_true")
    result.add_argument("--worker-id", default=os.getenv("GITHUB_RUN_ID", ""))
    result.add_argument("--journal-root", type=Path, default=DEFAULT_ROOT)
    result.add_argument("--state-file", type=Path, default=DEFAULT_STATE)
    result.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    result.add_argument("--audit-ledger", type=Path, default=DEFAULT_AUDIT)
    result.add_argument("--runtime-ref", default=DELAYED_V12_RUNTIME_STATE_REF)
    return result


def main() -> int:
    args = parser().parse_args()
    try:
        api = SmokeApi.from_environment(execute=args.execute)
        with operation_lock(args.journal_root) as journal:
            asyncio.run(run_startup_check(args, api, journal))
        return 0
    except SafetyError as exc:
        emit(action="STARTUP_ORDER_CHECK_BLOCKED", error=str(exc),
             no_strategy_worker_started=True, no_create_retry=True)
        return 2
    except Exception:
        emit(action="STARTUP_ORDER_CHECK_FAILED", error="details suppressed for credential safety",
             no_strategy_worker_started=True, preserve_journal=True)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
