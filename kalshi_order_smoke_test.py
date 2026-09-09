"""Operator-run shard-2 order smoke test; READ ONLY unless --execute is supplied.

Not a strategy runner. Fixed 1.00-contract buy at 1 cent, post-only, with a
30-second server expiration and immediate cancellation/reconciliation. Deep
orders CAN fill. Never retries creation, liquidates fills, transfers funds,
resets a breaker, or changes production/shadow state. Preserve its journal.
"""
from __future__ import annotations

import argparse
import asyncio
from contextlib import asynccontextmanager, suppress
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import re
import time
import uuid

from kalshi_btc15m_average_down import KalshiLiveFeed
from kalshi_shard_admin import (
    Api, ApiError, SafetyError, checked_id, confirm, emit, epoch, market_metadata,
    money, operation_lock, pages, shard,
)
from live_state import utc_now

ROOT = Path(".kalshi-order-smoke-test")
KIND = "order-smoke-test"
CREATE = "/portfolio/events/orders"
SHARD = 2
QUANTITY = Decimal("1.00")
PRICE = Decimal("0.01")
FUNDING_RESERVE = Decimal("0.02")  # principal plus a conservative fee allowance
MIN_QUOTE_GAP = Decimal("0.10")
MAX_QUOTE_AGE = 3
EXPIRY_SECONDS = 30
WS_URL = "wss://external-api-ws.kalshi.com/trade-api/ws/v2"


class SmokeApi(Api):
    """Same tested signing/transport, a separate narrow order-test allowlist.

    The shard admin's allowlist remains unable to send orders. A POST needs
    the exact authorized, journaled plan; a DELETE needs its own order ID.
    """
    permitted_plan = None
    permitted_cancel_id = None

    def authorize_request(self, method, path, *, params=None, body=None):
        if not re.fullmatch(r"/[A-Za-z0-9_/-]+", path) or ".." in path:
            raise SafetyError("Invalid smoke-test API path")
        if method == "GET" and body is None and (
            path in {"/markets", "/api_keys", "/portfolio/balance", "/portfolio/orders",
                     "/portfolio/positions", "/portfolio/fills", "/exchange/status"}
            or re.fullmatch(r"/markets/[A-Za-z0-9_-]+", path)
        ):
            return
        if self.execute and method == "POST" and path == CREATE and self.permitted_plan is not None:
            if body == self.permitted_plan and params is None:
                self.permitted_plan = None  # consume before HTTP; no blind retry
                return
        if self.execute and method == "DELETE" and self.permitted_cancel_id:
            if path == CREATE + "/" + checked_id(self.permitted_cancel_id) and body is None:
                if params and params.get("exchange_index") == SHARD and params.get("subaccount") == 0:
                    self.permitted_cancel_id = None
                    return
        raise SafetyError("Smoke-test write disabled or not the single authorized order")


class SmokeFeed(KalshiLiveFeed):
    """Reuse subscription/session machinery, retaining exact Decimal prices.

    Deliberately one connection: disconnects fail the preflight; REST handles
    order cleanup independently. Never forwards arbitrary WS error text.
    """
    async def _subscribe_private(self, ws):
        pass  # Public ticker proves subscription; authoritative fills use REST.

    def _handle(self, raw):
        payload = json.loads(raw)
        if payload.get("type") == "error":
            raise SafetyError("WebSocket subscription rejected; no new order permitted")
        if payload.get("type") != "ticker":
            return
        msg = payload.get("msg", {})
        ticker = msg.get("market_ticker", msg.get("ticker"))
        if ticker not in self.desired_tickers:
            return
        # A single fresh message must supply both prices; no last-trade or
        # stale-component synthesis. Sizes aren't needed for a resting bid.
        if msg.get("yes_bid_dollars") is None or msg.get("yes_ask_dollars") is None:
            return
        stamp = next((msg[k] for k in ("ts_ms", "time", "ts") if msg.get(k) is not None), None)
        q = {"yes_bid": money(msg["yes_bid_dollars"]), "yes_ask": money(msg["yes_ask_dollars"]),
             "exchange_epoch": epoch(stamp), "received_epoch": time.time()}
        old = self.quotes.get(ticker)
        if old and q["exchange_epoch"] <= old["exchange_epoch"]:
            return
        # Kalshi can legitimately publish a 0.00 bid or 1.00 ask near the
        # binary outcome.  Those are valid book endpoints, and rejecting the
        # whole quote can hide the opposite, safely testable side.
        if not 0 <= q["yes_bid"] <= q["yes_ask"] <= 1:
            return
        self.quotes[ticker] = q
        self.update_count += 1
        self._wake.set()

    async def run(self):
        import aiohttp
        try:
            headers = self.auth.create_auth_headers("GET", self.path)
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None, sock_connect=10)) as session:
                async with session.ws_connect(self.url, headers=headers, heartbeat=10) as ws:
                    self.connected = True
                    emit(action="WS_CONNECTED", host=WS_URL)
                    await self._session_loop(ws)
            raise SafetyError("WebSocket disconnected")
        except asyncio.CancelledError:
            raise
        except SafetyError:
            raise
        except Exception:
            raise SafetyError("WebSocket connection or message validation failed; credentials omitted") from None
        finally:
            self.connected = False

    def fresh(self, ticker, side):
        q = self.quotes.get(ticker)
        now = time.time()
        if not self.connected or not q or not all(-1 <= now - q[k] <= MAX_QUOTE_AGE
                                                 for k in ("exchange_epoch", "received_epoch")):
            raise SafetyError("No fresh two-sided WebSocket quote; no new order permitted")
        ask = q["yes_ask"] if side == "yes" else 1 - q["yes_bid"]
        bid = q["yes_bid"] if side == "yes" else 1 - q["yes_ask"]
        # A post-only BUY at PRICE is non-crossing when the selected-side ask
        # is strictly above PRICE.  The extra gap keeps this diagnostic order
        # deliberately deep.  The selected bid may validly be 0.00 and is not
        # the executable price against which a new bid would cross.
        if ask - PRICE < MIN_QUOTE_GAP:
            raise SafetyError("1-cent order is not sufficiently deep for this side; no new order permitted")
        return {**q, "selected_bid": bid, "selected_ask": ask}


@asynccontextmanager
async def quote_stream(api, ticker, side):
    feed = SmokeFeed(auth=api._auth, url=WS_URL)
    feed.set_tickers([ticker])
    task = asyncio.create_task(feed.run())
    try:
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if task.done():
                await task
                raise SafetyError("WebSocket ended without a quote")
            sides = ("yes", "no") if side is None else (side,)
            if any(_fresh_without_error(feed, ticker, candidate) for candidate in sides):
                break
            await feed.wait_for_update(0.2, feed.update_count)
        sides = ("yes", "no") if side is None else (side,)
        if not any(_fresh_without_error(feed, ticker, candidate) for candidate in sides):
            raise SafetyError("No safely deep fresh quote on either side; no new order permitted")
        yield feed
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError, SafetyError):
            await task


def _fresh_without_error(feed, ticker, side):
    try:
        feed.fresh(ticker, side)
        return True
    except SafetyError:
        return False


def validate_market(market, side):
    if side not in {"yes", "no"}:
        raise SafetyError("Test side must be yes or no")
    if shard(market.get("exchange_index")) != SHARD or market.get("market_type") != "binary":
        raise SafetyError("Test requires a binary market confirmed on exchange shard 2")
    now = time.time()
    if market.get("status") not in {"active", "open"} or market.get("result"):
        raise SafetyError("Market is not tradable")
    if not epoch(market.get("open_time")) <= now < epoch(market.get("close_time")) - 60:
        raise SafetyError("Market is not open or has less than 60 seconds remaining")
    price = PRICE if side == "yes" else 1 - PRICE
    ranges = market.get("price_ranges")
    if not isinstance(ranges, list) or not ranges:
        raise SafetyError("Market price grid unavailable; refusing to guess tick size")
    valid = False
    for row in ranges:
        start, end, step = (money(row.get(k)) for k in ("start", "end", "step"))
        if step <= 0 or end < start:
            raise SafetyError("Invalid market price grid")
        valid |= start <= price <= end and (price - start) % step == 0
    if not valid:
        raise SafetyError("One-cent economic price is not valid on this market price grid")


def plan_for(market, side, *, client_scope="manual"):
    validate_market(market, side)
    ticker = checked_id(market["ticker"])
    return {"ticker": ticker, "side": "bid" if side == "yes" else "ask", "count": "1.00",
            "price": "0.0100" if side == "yes" else "0.9900",
            "client_order_id": str(uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"kalshi-order-smoke-v2:{client_scope}:{ticker}:{side}",
            )),
            "time_in_force": "good_till_canceled", "expiration_time": int(time.time()) + EXPIRY_SECONDS,
            "self_trade_prevention_type": "taker_at_cross", "post_only": True,
            "cancel_order_on_pause": True, "reduce_only": False, "subaccount": 0, "exchange_index": SHARD}


async def preflight(api, ticker, side):
    market = await market_metadata(api, ticker, "KXBTC15M")
    validate_market(market, side)
    status = await api.request("GET", "/exchange/status", params={"exchange_index": SHARD})
    if status.get("trading_active") is not True or status.get("exchange_active") is not True:
        raise SafetyError("Exchange does not confirm active trading")
    keys = await api.request("GET", "/api_keys")
    matched = [k for k in keys.get("api_keys", []) if k.get("api_key_id") == api.key_id]
    if len(matched) != 1 or "write" not in matched[0].get("scopes", []):
        raise SafetyError("Key write permission not verified")
    if matched[0].get("subaccount") is not None or matched[0].get("fcm_subtrader_id"):
        raise SafetyError("Test supports an unrestricted primary-account key only")
    expiration = keys.get("api_key_region_expiration_ts")
    if expiration is not None and epoch(expiration) <= time.time():
        raise SafetyError("Kalshi location attestation expired; resolve it with Kalshi")
    balance = await api.request("GET", "/portfolio/balance", params={"exchange_index": SHARD, "subaccount": 0})
    available = money(balance.get("balance_dollars"))
    if available < FUNDING_RESERVE:
        raise SafetyError("Shard 2 has insufficient available cash for this test and fee allowance")
    if await pages(api, "/portfolio/orders", "orders", exchange_index=SHARD, status="resting", limit=1000):
        raise SafetyError("Resting orders exist on shard 2; pause workers and resolve them before this test")
    positions = await pages(api, "/portfolio/positions", "market_positions", exchange_index=SHARD, limit=1000)
    for p in positions:
        if signed_quantity(p.get("position_fp", p.get("position"))) != 0:
            raise SafetyError("Exposure exists on shard 2; this test requires a flat shard and paused workers")
    emit(action="PREFLIGHT_PASS", ticker=market["ticker"], exchange_index=SHARD,
         shard_available=available, write_scope="PASS", production_state="UNCHANGED")
    return market


def signed_quantity(value):
    try:
        q = Decimal(str(value))
        if not q.is_finite():
            raise InvalidOperation()
        return q
    except (InvalidOperation, ValueError, TypeError):
        raise SafetyError("Exchange position quantity is missing or invalid") from None


async def find_order(api, plan):
    rows = await pages(api, "/portfolio/orders", "orders", exchange_index=SHARD,
                       ticker=plan["ticker"], subaccount=0, limit=1000)
    matches = [r for r in rows if r.get("client_order_id") == plan["client_order_id"]]
    if len(matches) > 1:
        raise SafetyError("Multiple orders have this test ID; manual reconciliation required")
    if not matches:
        return None
    order = matches[0]
    expected_price = Decimal(plan["price"])
    if (order.get("ticker") != plan["ticker"] or shard(order.get("exchange_index")) != SHARD
            or money(order.get("initial_count_fp")) != QUANTITY
            or money(order.get("yes_price_dollars")) != expected_price
            or order.get("book_side") != plan["side"]):
        raise SafetyError("Order identity/quantity/price mismatch; refusing to cancel unrelated exposure")
    checked_id(order.get("order_id"))
    return order


async def reconcile(api, journal, op, *, cancel, attempts=3, require_cancel_ack=False):
    """Only cancel this test's order; an ACK/DELETE isn't proof of no fill."""
    plan = op["plan"]
    cancel_attempted = False
    for attempt in range(attempts):
        order = await find_order(api, plan)
        if order:
            op["order_id"] = order["order_id"]
            filled = money(order.get("fill_count_fp"))
            remaining = money(order.get("remaining_count_fp"))
            if filled > QUANTITY or remaining > QUANTITY or filled + remaining > QUANTITY:
                raise SafetyError("Impossible order quantities; manual reconciliation required")
            op.update(filled_quantity=str(filled), remaining_quantity=str(remaining),
                      order_status=order.get("status"), accepted_verified=True)
            journal.save(op)
            emit(action="ORDER_RECONCILED", order_id=op["order_id"], status=op["order_status"],
                 filled_quantity=filled, remaining_quantity=remaining, exchange_index=SHARD)
            terminal = order.get("status") in {"canceled", "cancelled", "executed", "filled", "expired"}
            if remaining == 0 and terminal:
                fills = await pages(api, "/portfolio/fills", "fills", exchange_index=SHARD,
                                    ticker=plan["ticker"], order_id=order["order_id"], subaccount=0, limit=1000)
                unique = {}
                for f in fills:
                    fid = checked_id(f.get("fill_id", f.get("trade_id")))
                    if f.get("order_id") != order["order_id"] or f.get("ticker", f.get("market_ticker")) != plan["ticker"]:
                        raise SafetyError("Fill linkage mismatch")
                    item = {"fill_id": fid, "quantity": str(money(f.get("count_fp"))),
                            "economic_price": str(money(f.get("yes_price_dollars" if op["side"] == "yes" else "no_price_dollars"))),
                            "fees": str(money(f.get("fee_cost"))), "is_taker": f.get("is_taker")}
                    if fid in unique and unique[fid] != item:
                        raise SafetyError("Conflicting duplicate fill")
                    unique[fid] = item
                actual = sum((Decimal(f["quantity"]) for f in unique.values()), Decimal(0))
                positions = await pages(api, "/portfolio/positions", "market_positions", exchange_index=SHARD,
                                        ticker=plan["ticker"], subaccount=0, limit=1000)
                position = sum((signed_quantity(p.get("position_fp", p.get("position"))) for p in positions), Decimal(0))
                expected_position = filled if op["side"] == "yes" else -filled
                cancellation_proven = bool(op.get("cancel_acknowledged"))
                if actual == filled and position == expected_position and (
                    not require_cancel_ack or filled > 0 or cancellation_proven
                ):
                    op.update(state="FILLED_REVIEW_REQUIRED" if filled else "CANCELED_NO_FILL",
                              fills=list(unique.values()), position=str(position), reconciled_at=utc_now())
                    journal.save(op)
                    emit(action="ORDER_TEST_RESULT", state=op["state"], order_id=op["order_id"],
                         client_order_id=plan["client_order_id"], exchange_index=SHARD,
                         filled_quantity=filled, remaining_quantity=remaining, position=position,
                         fees=sum((Decimal(f["fees"]) for f in unique.values()), Decimal(0)),
                         automatic_liquidation=False, breaker="UNCHANGED",
                         cancel_acknowledged=cancellation_proven)
                    if filled:
                        raise SafetyError("Test order filled; no more orders sent. Review the real position in Kalshi before resuming your bot")
                    return
        # A known ACK ID can be canceled even while the order-list read lags.
        if cancel and not cancel_attempted and op.get("order_id") and (not order or money(order.get("remaining_count_fp")) > 0):
            cancel_attempted = True
            oid = checked_id(op["order_id"])
            op["cancel_attempted_at"] = utc_now()
            journal.save(op)
            api.permitted_cancel_id = oid
            try:
                response = await api.request(
                    "DELETE", CREATE + "/" + oid,
                    params={"exchange_index": SHARD, "market_ticker": plan["ticker"], "subaccount": 0},
                )
                canceled = response.get("order") if isinstance(response, dict) else None
                acknowledged_id = (
                    canceled.get("order_id") if isinstance(canceled, dict)
                    else response.get("order_id") if isinstance(response, dict) else None
                )
                if not acknowledged_id:
                    raise SafetyError("Cancellation acknowledgment did not identify the test order")
                if checked_id(acknowledged_id) != oid:
                    raise SafetyError("Cancellation acknowledgment did not identify the test order")
                op.update(cancel_acknowledged=True, cancel_acknowledged_at=utc_now())
                journal.save(op)
                emit(action="CANCEL_ACK_RECEIVED", order_id=oid, exchange_index=SHARD,
                     note="REST reads must still prove zero remaining, zero fills, and a flat position")
            except ApiError as exc:
                emit(action="CANCEL_NOT_CONFIRMED", http_status=exc.status, code=exc.code,
                     note="REST reconciliation follows; do not assume cancellation")
        if attempt + 1 < attempts:
            await asyncio.sleep(1)
    op["state"] = "UNRESOLVED_DO_NOT_REPEAT"
    journal.save(op)
    raise SafetyError("Test remains unresolved. Preserve journal; check Kalshi UI and run --reconcile-only. Never repeat creation")


async def run_test(args, api, journal, *, stream=quote_stream, answer=None):
    if args.execute and not args.workers_paused:
        raise SafetyError("--execute requires --workers-paused; pause workers/watchdog yourself first")
    prior = journal.load(KIND)
    fingerprint = hashlib.sha256(api.key_id.encode()).hexdigest()
    if prior:
        if prior.get("credential_fingerprint") != fingerprint:
            raise SafetyError("Journal belongs to a different credential; review in Kalshi UI, do not repeat creation")
        emit(action="EXISTING_TEST", note="Creation disabled; reconciling the existing test only")
        await reconcile(api, journal, prior, cancel=args.execute)
        return
    if args.reconcile_only:
        raise SafetyError("No local smoke-test journal; no order will be created")
    market = await preflight(api, args.ticker, args.side)
    plan = plan_for(market, args.side)
    if await find_order(api, plan):
        raise SafetyError("An exchange order already exists for this test/market/side; preserve/recover its journal, do not repeat")
    async with stream(api, market["ticker"], args.side) as feed:
        q = feed.fresh(market["ticker"], args.side)
        emit(action="ORDER_TEST_PREVIEW", mode="LIVE_REQUESTED" if args.execute else "READ_ONLY",
             ticker=market["ticker"], selected_side=args.side, economic_limit=PRICE, quantity=QUANTITY,
             maximum_principal=PRICE * QUANTITY, fee_allowance=FUNDING_RESERVE - PRICE * QUANTITY,
             selected_bid=q["selected_bid"], selected_ask=q["selected_ask"], post_only=True,
             expiry_seconds=EXPIRY_SECONDS, cancel="IMMEDIATE_AFTER_ACK", orders_sent=0)
        if not args.execute:
            return
        phrase = f"BUY 1.00 {args.side.upper()} AT 0.01 ON {market['ticker']} SHARD 2 AND CANCEL"
        await asyncio.to_thread(confirm, phrase, answer)  # keep the quote socket alive while the operator types
        # Human confirmation can take time; do not reuse an old balance/quote/expiry.
        market = await preflight(api, market["ticker"], args.side)
        plan = plan_for(market, args.side)
        if await find_order(api, plan):
            raise SafetyError("A matching order appeared during preflight; refusing duplicate")
        feed.fresh(market["ticker"], args.side)
        op = {"schema_version": 1, "kind": KIND, "state": "SUBMISSION_INTENT",
              "credential_fingerprint": fingerprint, "side": args.side, "plan": plan,
              "created_at": utc_now(), "post_attempts": 1, "accepted_verified": False}
        journal.save(op)  # fsync before POST; resume never performs another POST
        api.permitted_plan = dict(plan)
        try:
            response = await api.request("POST", CREATE, body=plan)
            oid = checked_id(response.get("order_id"))
            if response.get("client_order_id") not in {None, plan["client_order_id"]}:
                raise SafetyError("Order acknowledgment identity mismatch")
            op.update(order_id=oid, state="ACK_RECEIVED", acknowledged_at=utc_now())
            journal.save(op)
            emit(action="ORDER_ACK_RECEIVED", order_id=oid, client_order_id=plan["client_order_id"],
                 note="Acceptance is not proof of a fill; cancel and reconcile now")
        except ApiError as exc:
            op["state"] = "SUBMISSION_REJECTED" if exc.status in {400, 401, 403, 404, 422} else "SUBMISSION_UNKNOWN"
            journal.save(op)
            emit(action=op["state"], http_status=exc.status, code=exc.code,
                 retry_create=False, **exc.server_diagnostics)
        finally:
            await reconcile(api, journal, op, cancel=True)


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--ticker", help="Optional API-discovered ticker; default discovers the active KXBTC15M market")
    p.add_argument("--side", choices=("yes", "no"), default="yes", help="Test side only; not a strategy prediction")
    p.add_argument("--execute", action="store_true", help="Permit the one confirmed real test order and its cancellation")
    p.add_argument("--workers-paused", action="store_true", help="Attest all trading workers/watchdog are paused")
    p.add_argument("--reconcile-only", action="store_true", help="Never create; inspect existing test (--execute also permits cancellation)")
    return p


def main():
    args = parser().parse_args()
    try:
        api = SmokeApi.from_environment(execute=args.execute)
        with operation_lock(ROOT) as journal:
            asyncio.run(run_test(args, api, journal))
        return 0
    except SafetyError as exc:
        emit(action="TEST_BLOCKED_OR_REVIEW_REQUIRED", error=str(exc), breaker="UNCHANGED",
             production_state="UNCHANGED", no_automatic_create_retry=True)
        return 2
    except KeyboardInterrupt:
        emit(action="INTERRUPTED", note="Preserve journal and reconcile in Kalshi UI; expiry is a backup, not proof of cancellation")
        return 130
    except Exception:
        emit(action="UNEXPECTED_FAILURE", note="Details suppressed for credential safety. Preserve journal and check Kalshi UI before retrying")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
