"""Operator-run Kalshi shard funding. Default: authenticated READ-ONLY checks.

This utility has no order endpoint, breaker reset, worker restart, withdrawal,
or API-key creation capability. Writes require --execute, --workers-paused,
and an interactive confirmation of the exact operation after fresh preflight.
Transfers POST once, with an fsynced intent journal and read-only status polling.
Keep .kalshi-shard-admin/: losing that journal loses local duplicate protection.
"""
from __future__ import annotations

import argparse
import asyncio
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import fcntl
import json
import os
from pathlib import Path
import re
import time
from typing import Any
from urllib.parse import quote

from live_state import save_state, utc_now

HOST = "https://external-api.kalshi.com"
PREFIX = "/trade-api/v2"
TRANSFER = "/portfolio/intra_exchange_instance_transfer"
TRANSFERS = "/portfolio/intra_exchange_instance_transfers"
ALLOCATION = "/portfolio/target_balance_allocation"
JOURNAL_DIR = Path(".kalshi-shard-admin")
CENTICENTS = Decimal("10000")
SAFE_CODES = {"user_not_found", "available_balance_too_low", "insufficient_balance",
              "insufficient_funds", "authentication_error", "unauthorized", "forbidden",
              "invalid_parameters", "rate_limit_exceeded"}


class SafetyError(Exception):
    """A controlled, credential-free diagnostic safe to show to the operator."""


class ApiError(SafetyError):
    def __init__(self, status: int | None, code: str = "unclassified"):
        self.status = status
        self.code = code if isinstance(code, str) and code in SAFE_CODES else "unclassified"
        super().__init__(f"API request failed: HTTP {status}; code={self.code}. No automatic write retry.")


def emit(**values: Any) -> None:
    print(json.dumps(values, sort_keys=True, default=str), flush=True)


def money(value: Any) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise SafetyError("Missing or invalid fixed-point dollar amount") from None
    if not result.is_finite() or result < 0:
        raise SafetyError("Dollar amount must be finite and nonnegative")
    return result


def centicents(value: Any) -> int:
    amount = money(value) * CENTICENTS
    if amount != amount.to_integral_value() or amount > 2**63 - 1:
        raise SafetyError("Amount cannot be represented exactly in transfer centicents; refusing rounding")
    return int(amount)


def shard(value: Any) -> int:
    if type(value) is not int or not 0 <= value <= 100:
        raise SafetyError("Invalid exchange index; expected an integer from 0 through 100")
    return value


def epoch(value: Any) -> float:
    try:
        if isinstance(value, str) and not value.replace(".", "", 1).isdigit():
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                raise ValueError()
            return parsed.timestamp()
        numeric = Decimal(str(value))
        if not numeric.is_finite() or numeric < 0:
            raise ValueError()
        return float(numeric / 1000 if numeric >= 100000000000 else numeric)
    except (ValueError, InvalidOperation, OverflowError, TypeError):
        raise SafetyError("Invalid exchange timestamp") from None


def checked_id(value: Any) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,160}", value):
        raise SafetyError("Invalid exchange identifier; do not repeat the write")
    return value


class Api:
    """Reuse the project's KalshiAuth signer; never expose raw API errors."""
    def __init__(self, key_id: str, pem: str, *, execute: bool = False):
        from kalshi_python_async import KalshiAuth
        self.key_id = key_id
        self._auth = KalshiAuth(key_id, pem)
        self.execute = execute

    @classmethod
    def from_environment(cls, *, execute: bool = False) -> "Api":
        key_id = os.getenv("KALSHI_API_KEY_ID") or os.getenv("KALSHI_PROD_API_KEY")
        pem = os.getenv("KALSHI_PRIVATE_KEY")
        if not pem and os.getenv("KALSHI_PEM_PATH"):
            pem = Path(os.environ["KALSHI_PEM_PATH"]).read_text(encoding="utf-8")
        if not key_id or not pem:
            raise SafetyError("Set Codespaces secrets KALSHI_PROD_API_KEY and KALSHI_PRIVATE_KEY; never put them in source")
        return cls(key_id, pem, execute=execute)

    async def request(self, method: str, path: str, *, params=None, body=None) -> dict:
        import aiohttp
        get_allowed = path in {"/markets", "/api_keys", "/portfolio/balance", "/portfolio/orders",
                              "/portfolio/positions", TRANSFERS, ALLOCATION} or (
            path.startswith("/markets/") or path.startswith(TRANSFERS + "/"))
        if method == "GET":
            if not get_allowed or body is not None:
                raise SafetyError("Read endpoint is not allowlisted")
        elif method != "POST" or not self.execute or path not in {TRANSFER, ALLOCATION}:
            raise SafetyError("Writes are disabled or endpoint is not allowlisted; this tool cannot place orders")
        if ".." in path or "?" in path or "#" in path or not path.startswith("/"):
            raise SafetyError("Invalid API path")
        attempts = 3 if method == "GET" else 1
        for attempt in range(attempts):
            try:
                headers = self._auth.create_auth_headers(method, PREFIX + path)
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
                    async with session.request(method, HOST + PREFIX + path, params=params,
                                               json=body, headers=headers, allow_redirects=False) as response:
                        raw = await response.read()
                        try:
                            payload = json.loads(raw) if raw else {}
                        except (ValueError, UnicodeError):
                            raise ApiError(response.status) from None
                        if not 200 <= response.status < 300:
                            error = payload.get("error", payload) if isinstance(payload, dict) else {}
                            code = error.get("code") if isinstance(error, dict) else None
                            raise ApiError(response.status, code)
                        if not isinstance(payload, dict):
                            raise ApiError(response.status)
                        return payload
            except (aiohttp.ClientError, asyncio.TimeoutError, ApiError) as exc:
                status = exc.status if isinstance(exc, ApiError) else None
                if method == "GET" and attempt + 1 < attempts and (status is None or status == 429 or status >= 500):
                    await asyncio.sleep(2**attempt)
                    continue
                if isinstance(exc, ApiError):
                    raise
                raise ApiError(None) from None
        raise SafetyError("Read retries exhausted")


async def pages(api: Api, path: str, key: str, **params) -> list[dict]:
    rows, seen = [], set()
    for _ in range(100):
        payload = await api.request("GET", path, params=params)
        page = payload.get(key)
        if not isinstance(page, list) or any(not isinstance(row, dict) for row in page):
            raise SafetyError("Malformed paginated response; cannot prove account state")
        rows.extend(page)
        cursor = payload.get("cursor")
        if not cursor:
            return rows
        if not isinstance(cursor, str) or cursor in seen:
            raise SafetyError("Pagination did not advance; cannot prove account state")
        seen.add(cursor)
        params = {**params, "cursor": cursor}
    raise SafetyError("Pagination limit reached; cannot prove account state")


async def market_metadata(api: Api, ticker: str | None, series: str) -> dict:
    if ticker:
        checked_id(ticker)
        market = (await api.request("GET", "/markets/" + ticker)).get("market")
        if not isinstance(market, dict) or market.get("ticker") != ticker:
            raise SafetyError("Market metadata mismatch")
    else:
        checked_id(series)
        candidates = await pages(api, "/markets", "markets", series_ticker=series, status="open", limit=100)
        now = time.time()
        active = [m for m in candidates if m.get("status") in {"active", "open"}
                  and epoch(m.get("open_time")) <= now < epoch(m.get("close_time"))]
        if len(active) != 1:
            raise SafetyError("Cannot identify exactly one active market; provide --ticker with an API-discovered ticker")
        market = active[0]
    shard(market.get("exchange_index"))
    checked_id(market.get("ticker"))
    return market


def allocation_map(payload: dict) -> dict[int, int]:
    values = payload.get("allocations")
    if not isinstance(values, list):
        raise SafetyError("Target allocation could not be verified")
    result, seen = {}, set()
    for row in values:
        index, percent = shard(row.get("exchange_index")), row.get("percent")
        if index in seen or type(percent) is not int or not 0 <= percent <= 100:
            raise SafetyError("Invalid or duplicate target allocation")
        seen.add(index)
        if percent:
            result[index] = percent
    if result and sum(result.values()) != 100:
        raise SafetyError("Allocation percentages do not total 100")
    return result


async def inspect_account(api: Api, *, ticker=None, series="KXBTC15M", source=0) -> dict:
    shard(source)
    market = await market_metadata(api, ticker, series)
    destination = market["exchange_index"]
    total = await api.request("GET", "/portfolio/balance")
    total_balance = money(total.get("balance_dollars"))
    breakdown = total.get("balance_breakdown")
    if not isinstance(breakdown, list):
        raise SafetyError("Full account balance breakdown unavailable; cannot verify all exchange shards")
    indexes = {shard(row.get("exchange_index")) for row in breakdown}
    balances = {}
    for index in {source, destination}:
        value = await api.request("GET", "/portfolio/balance", params={"exchange_index": index, "subaccount": 0})
        balances[index] = format(money(value.get("balance_dollars")), "f")
    write_scope, unrestricted, region = "UNKNOWN", False, "UNKNOWN"
    try:
        payload = await api.request("GET", "/api_keys")
        matches = [key for key in payload.get("api_keys", []) if key.get("api_key_id") == api.key_id]
        if len(matches) == 1:
            key = matches[0]
            scopes = key.get("scopes", [])
            write_scope = "PASS" if isinstance(scopes, list) and "write" in scopes else "BLOCKED"
            unrestricted = key.get("subaccount") is None and not key.get("fcm_subtrader_id")
        expiration = payload.get("api_key_region_expiration_ts")
        region = "NOT_REPORTED" if expiration is None else "EXPIRED" if epoch(expiration) <= time.time() else "VALID"
    except ApiError:
        pass  # A balance read proves authentication, not write permission.
    allocation = allocation_map(await api.request("GET", ALLOCATION))
    return {"api_auth": "PASS", "api_key_write": write_scope, "unrestricted_primary_key": unrestricted,
            "region_attestation": region, "market_ticker": market["ticker"],
            "market_exchange_index": destination, "source_exchange_index": source,
            "account_exchange_indexes": sorted(indexes | {source, destination}),
            "total_balance": format(total_balance, "f"), "shard_balances": balances,
            "target_allocation": allocation, "bot_breaker": "NOT_MODIFIED_OR_CLEARED",
            "can_submit_orders": False, "trading_readiness": "NOT_ASSESSED_BY_ADMIN_TOOL"}


async def assert_quiet_account(api: Api, snapshot: dict) -> None:
    if snapshot["api_key_write"] != "PASS" or not snapshot["unrestricted_primary_key"]:
        raise SafetyError("An unrestricted account API key with verified write scope is required")
    if snapshot["region_attestation"] == "EXPIRED":
        raise SafetyError("Kalshi location attestation has expired; resolve it with Kalshi")
    for index in snapshot["account_exchange_indexes"]:
        orders = await pages(api, "/portfolio/orders", "orders", exchange_index=index, status="resting", limit=1000)
        if orders:
            raise SafetyError("Resting orders exist on an account shard; resolve them yourself before moving funds")
        positions = await pages(api, "/portfolio/positions", "market_positions", exchange_index=index, limit=1000)
        for position in positions:
            try:
                quantity = Decimal(str(position.get("position_fp", position.get("position"))))
            except InvalidOperation:
                raise SafetyError("Position quantity unavailable") from None
            if not quantity.is_finite() or quantity != 0:
                raise SafetyError("Open or unknown exposure exists; resolve it before shard administration")
    transfers = await transfer_history(api)
    if any(t.get("status") not in {"complete", "failed", "cancelled", "canceled"} for t in transfers):
        raise SafetyError("Pending or unknown-status account transfer exists; do not submit another")


async def transfer_history(api: Api) -> list[dict]:
    """Confirm nonterminal list records by ID, never dismiss old pending transfers."""
    transfers = await pages(api, TRANSFERS, "transfers", limit=500)
    verified = []
    for transfer in transfers:
        if transfer.get("status") not in {"complete", "failed", "cancelled", "canceled"}:
            identifier = checked_id(transfer.get("transfer_id"))
            response = await api.request("GET", TRANSFERS + "/" + identifier)
            current = response.get("transfer")
            if not isinstance(current, dict) or current.get("transfer_id") != identifier:
                raise SafetyError("Pending-transfer lookup could not be verified; do not submit another")
            transfer = current
        verified.append(transfer)
    return verified


async def report_transfers(api: Api) -> None:
    transfers = await transfer_history(api)
    counts: dict[str, int] = {}
    for transfer in transfers:
        raw_status = transfer.get("status")
        status = raw_status if isinstance(raw_status, str) and raw_status in {"complete", "pending", "failed", "cancelled", "canceled"} else "unknown"
        counts[status] = counts.get(status, 0) + 1
        if status not in {"complete", "failed", "cancelled", "canceled"}:
            emit(action="UNRESOLVED_TRANSFER", transfer_id=checked_id(transfer.get("transfer_id")),
                 status=status, amount_dollars=str(money(transfer.get("amount"))),
                 created_at_utc=datetime.fromtimestamp(epoch(transfer.get("created_ts")), timezone.utc).isoformat(),
                 source_exchange_shard=shard(transfer.get("source_exchange_shard")),
                 destination_exchange_shard=shard(transfer.get("destination_exchange_shard")),
                 source=transfer.get("source") if transfer.get("source") in {"event_contract", "margined"} else "unknown",
                 destination=transfer.get("destination") if transfer.get("destination") in {"event_contract", "margined"} else "unknown",
                 orders_sent=0, transfer_posts=0)
    emit(action="READ_ONLY_TRANSFER_HISTORY", total=len(transfers), statuses=counts,
         transfer_history_clear=not any(key in counts for key in ("pending", "unknown")),
         note="Other write preflight checks still required; no worker or breaker changes")


class Journal:
    def __init__(self, root: Path):
        self.root = root

    def load(self, kind: str) -> dict | None:
        path = self.root / (kind + ".json")
        if path.is_symlink():
            raise SafetyError("Refusing a symlinked operation journal")
        if not path.exists():
            return None
        value = json.loads(path.read_text())
        if not isinstance(value, dict) or value.get("schema_version") != 1 or value.get("kind") != kind:
            raise SafetyError("Unrecognized operation journal; preserve it and review manually")
        return value

    def save(self, value: dict) -> None:
        save_state(self.root / (value["kind"] + ".json"), value)


@contextmanager
def operation_lock(root: Path = JOURNAL_DIR):
    if root.is_symlink():
        raise SafetyError("Refusing a symlinked operation directory")
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    descriptor = os.open(root / "operation.lock", os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SafetyError("Another local shard-admin process holds the lock") from None
        yield Journal(root)
    finally:
        os.close(descriptor)


def confirm(phrase: str, answer=None) -> None:
    reply = (answer or input)("Type exactly to authorize: " + phrase + "\n> ")
    if reply.strip() != phrase:
        raise SafetyError("Confirmation did not match; nothing submitted")


async def verify_transfer(api: Api, journal: Journal, operation: dict, *, timeout=120, transfer_id=None) -> dict:
    identifier = checked_id(transfer_id or operation.get("transfer_id"))
    deadline = time.monotonic() + timeout
    expected = operation["request"]
    while True:
        payload = await api.request("GET", TRANSFERS + "/" + quote(identifier, safe=""))
        transfer = payload.get("transfer")
        if not isinstance(transfer, dict) or transfer.get("transfer_id") != identifier:
            raise SafetyError("Transfer lookup did not return the requested operation")
        for key in ("source", "destination", "source_exchange_shard", "destination_exchange_shard"):
            if transfer.get(key) != expected[key]:
                raise SafetyError("Transfer routing does not match saved intent; preserve journal")
        if centicents(transfer.get("amount")) != expected["amount"]:
            raise SafetyError("Transfer amount does not match saved intent; preserve journal")
        if epoch(transfer.get("created_ts")) < epoch(operation["created_at"]) - 30:
            raise SafetyError("Transfer predates saved intent; refusing to associate a different transfer")
        operation["transfer_id"] = identifier
        status = transfer.get("status")
        operation["status"] = "COMPLETED" if status == "complete" else "PENDING" if status == "pending" else "NEEDS_REVIEW"
        journal.save(operation)
        if status == "complete":
            observed = {}
            for index in (expected["source_exchange_shard"], expected["destination_exchange_shard"]):
                value = await api.request("GET", "/portfolio/balance", params={"exchange_index": index, "subaccount": 0})
                observed[index] = format(money(value.get("balance_dollars")), "f")
            operation["verified_balances"] = observed
            before = operation["balances_before"]
            source, destination = expected["source_exchange_shard"], expected["destination_exchange_shard"]
            # JSON converts integer mapping keys to strings across restart.
            source_before = money(before.get(str(source), before.get(source)))
            destination_before = money(before.get(str(destination), before.get(destination)))
            dollars = Decimal(expected["amount"]) / CENTICENTS
            operation["balance_delta_matches_plan"] = (
                money(observed[source]) == source_before - dollars
                and money(observed[destination]) == destination_before + dollars
            )
            journal.save(operation)
            emit(action="TRANSFER_VERIFIED_COMPLETE", transfer_id=identifier, balances=observed,
                 amount_dollars=str(Decimal(expected["amount"]) / CENTICENTS),
                 balance_delta_matches_plan=operation["balance_delta_matches_plan"],
                 breaker="UNCHANGED", orders_sent=0)
            if not operation["balance_delta_matches_plan"]:
                raise SafetyError("Transfer is officially complete, but balances differ from the plan; inspect concurrent activity, never resubmit")
            return operation
        if status != "pending":
            raise SafetyError("Transfer has a non-complete, non-pending status; manual review required, no resubmission")
        if time.monotonic() >= deadline:
            raise SafetyError("Transfer is still pending; run resume-transfer later, never submit again")
        await asyncio.sleep(2)


async def transfer_all(api: Api, journal: Journal, *, ticker=None, series="KXBTC15M", source=0,
                       execute=False, workers_paused=False, answer=None, timeout=120) -> dict:
    previous = journal.load("transfer")
    if previous:
        if previous.get("transfer_id"):
            return await verify_transfer(api, journal, previous, timeout=timeout)
        raise SafetyError("Existing transfer intent has no confirmed ID; use resume-transfer after manual lookup, not another POST")
    snapshot = await inspect_account(api, ticker=ticker, series=series, source=source)
    emit(action="READ_ONLY_PREFLIGHT", **snapshot)
    destination = snapshot["market_exchange_index"]
    if source == destination:
        raise SafetyError("Source and destination exchange are identical")
    amount = centicents(snapshot["shard_balances"][source])
    if amount <= 0:
        raise SafetyError("Source shard has no available cash to transfer")
    request = {"source": "event_contract", "destination": "event_contract", "amount": amount,
               "source_exchange_shard": source, "destination_exchange_shard": destination,
               "source_subaccount": 0, "destination_subaccount": 0}
    emit(action="TRANSFER_PREVIEW", amount_dollars=str(Decimal(amount) / CENTICENTS), request=request, execute=execute)
    if not execute:
        return request
    if not workers_paused:
        raise SafetyError("Pause all trading workers/watchdogs yourself, then provide --workers-paused")
    await assert_quiet_account(api, snapshot)
    if snapshot["target_allocation"] not in ({}, {destination: 100}):
        raise SafetyError("Existing rebalancing could undo this transfer; review/change allocation separately first")
    phrase = f"TRANSFER {Decimal(amount) / CENTICENTS:.4f} USD FROM SHARD {source} TO SHARD {destination}"
    confirm(phrase, answer)
    # Recheck after the operator has read/typed the confirmation. Do not sweep
    # a newly increased balance or tolerate a decreased balance silently.
    fresh = await inspect_account(api, ticker=snapshot["market_ticker"], source=source)
    if fresh["market_exchange_index"] != destination or fresh["shard_balances"] != snapshot["shard_balances"] or fresh["target_allocation"] != snapshot["target_allocation"]:
        raise SafetyError("Funding/routing changed during confirmation; nothing submitted, rerun preview")
    await assert_quiet_account(api, fresh)
    operation = {"schema_version": 1, "kind": "transfer", "status": "SUBMITTING", "created_at": utc_now(),
                 "market_ticker": snapshot["market_ticker"], "request": request,
                 "balances_before": snapshot["shard_balances"]}
    journal.save(operation)  # Durable BEFORE the only POST; no client idempotency field is documented.
    try:
        response = await api.request("POST", TRANSFER, body=request)
        operation["transfer_id"] = checked_id(response.get("transfer_id"))
        operation["status"] = "ACCEPTED"
        journal.save(operation)
    except Exception as exc:
        operation["status"] = "SUBMISSION_UNKNOWN"
        operation["error_type"] = type(exc).__name__
        journal.save(operation)
        raise SafetyError("Transfer response is unconfirmed. Preserve journal, inspect Kalshi history, and never blindly repeat POST") from None
    return await verify_transfer(api, journal, operation, timeout=timeout)


async def allocation_all(api: Api, journal: Journal, *, ticker=None, series="KXBTC15M", source=0,
                         execute=False, workers_paused=False, answer=None) -> dict:
    snapshot = await inspect_account(api, ticker=ticker, series=series, source=source)
    destination = snapshot["market_exchange_index"]
    desired = {"allocations": [{"exchange_index": destination, "percent": 100}]}
    previous = journal.load("allocation")
    if previous:
        if previous["request"] != desired:
            raise SafetyError("Saved allocation intent targets another exchange; manual review required")
        if snapshot["target_allocation"] != {destination: 100}:
            raise SafetyError("Existing allocation intent is unconfirmed; verify it with Kalshi before any new write")
        previous["status"] = "VERIFIED"
        journal.save(previous)
        emit(action="ALLOCATION_VERIFIED", allocation=desired, orders_sent=0)
        return previous
    emit(action="ALLOCATION_PREVIEW", current=snapshot["target_allocation"], requested=desired, execute=execute,
         effect="Continuously rebalance sweepable cash across the account; NOT just a one-time shard-0 transfer")
    if not execute or snapshot["target_allocation"] == {destination: 100}:
        return desired
    if not workers_paused:
        raise SafetyError("Pause all trading workers/watchdogs yourself, then provide --workers-paused")
    transfer = journal.load("transfer")
    if transfer and transfer.get("status") != "COMPLETED":
        raise SafetyError("Resolve the existing transfer journal before enabling automatic allocation")
    await assert_quiet_account(api, snapshot)
    confirm(f"ALLOCATE 100 PERCENT OF SWEEPABLE ACCOUNT CASH TO SHARD {destination}", answer)
    fresh = await inspect_account(api, ticker=snapshot["market_ticker"], source=source)
    if fresh["market_exchange_index"] != destination or fresh["target_allocation"] != snapshot["target_allocation"]:
        raise SafetyError("Routing/allocation changed during confirmation; nothing submitted")
    await assert_quiet_account(api, fresh)
    operation = {"schema_version": 1, "kind": "allocation", "status": "SUBMITTING", "created_at": utc_now(), "request": desired}
    journal.save(operation)
    try:
        await api.request("POST", ALLOCATION, body=desired)
    except Exception as exc:
        operation.update(status="SUBMISSION_UNKNOWN", error_type=type(exc).__name__)
        journal.save(operation)
        raise SafetyError("Allocation response unknown; rerun allocation-all read-only to verify, not to resend") from None
    actual = allocation_map(await api.request("GET", ALLOCATION))
    operation["status"] = "VERIFIED" if actual == {destination: 100} else "NEEDS_REVIEW"
    journal.save(operation)
    if operation["status"] != "VERIFIED":
        raise SafetyError("Allocation was not confirmed by GET; do not assume success")
    emit(action="ALLOCATION_VERIFIED", allocation=desired, orders_sent=0, breaker="UNCHANGED")
    return operation


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("command", nargs="?", default="status", choices=("status", "transfers", "transfer-all", "resume-transfer", "allocation-all"))
    result.add_argument("--ticker", help="Optional API-discovered market ticker; otherwise discover the active series market")
    result.add_argument("--series", default="KXBTC15M")
    result.add_argument("--source-shard", type=int, default=0)
    result.add_argument("--execute", action="store_true", help="Permit ONLY the selected admin write, after interactive confirmation")
    result.add_argument("--workers-paused", action="store_true", help="Operator attestation: all account workers/watchdogs were paused manually")
    result.add_argument("--transfer-id", help="For read-only recovery of a saved uncertain transfer; must match amount, routing and time")
    result.add_argument("--timeout", type=int, default=120, help="Transfer status polling deadline, 0-600 seconds")
    return result


async def run(args, api=None, *, root=JOURNAL_DIR) -> None:
    if args.transfer_id and args.command != "resume-transfer":
        raise SafetyError("--transfer-id is only for read-only resume-transfer; it is not a POST idempotency key")
    if args.execute and args.command not in {"transfer-all", "allocation-all"}:
        raise SafetyError("This command is read-only and does not accept --execute")
    if not 0 <= args.timeout <= 600:
        raise SafetyError("Timeout must be between 0 and 600 seconds")
    shard(args.source_shard)
    api = api or Api.from_environment(execute=args.execute)
    if args.command == "transfers":
        await report_transfers(api)
        return
    if args.command == "status":
        emit(action="READ_ONLY_STATUS", **await inspect_account(api, ticker=args.ticker, series=args.series, source=args.source_shard))
        return
    with operation_lock(root) as journal:
        if args.command == "resume-transfer":
            operation = journal.load("transfer")
            if not operation:
                raise SafetyError("No saved transfer intent to resume")
            await verify_transfer(api, journal, operation, timeout=args.timeout, transfer_id=args.transfer_id)
        elif args.command == "transfer-all":
            await transfer_all(api, journal, ticker=args.ticker, series=args.series, source=args.source_shard,
                               execute=args.execute, workers_paused=args.workers_paused, timeout=args.timeout)
        else:
            await allocation_all(api, journal, ticker=args.ticker, series=args.series, source=args.source_shard,
                                 execute=args.execute, workers_paused=args.workers_paused)


def main() -> int:
    try:
        asyncio.run(run(parser().parse_args()))
        return 0
    except SafetyError as exc:
        emit(error=str(exc), orders_sent=0, breaker="UNCHANGED")
        return 2
    except (Exception, KeyboardInterrupt) as exc:
        emit(error_type=type(exc).__name__, error="Stopped safely; sensitive details suppressed. Preserve any operation journal.")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
