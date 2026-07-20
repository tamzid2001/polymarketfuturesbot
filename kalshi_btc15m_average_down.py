"""Pure mechanical KXBTC15M average-down trader.

This program intentionally has no model, forecast, technical indicator, price
prediction, or historical score.  It uses only executable Kalshi YES/NO asks
and the fixed ladder 40c -> 30c -> 20c -> 10c.

Live submission is deliberately opt-in: ``DRY_RUN`` must be false and both
``--submit`` and ``--allow-live`` are required.  The GitHub workflow persists
its configuration and state so scheduled runs retain the latest manual share
amount and can reconcile resting/settled orders from previous windows.
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
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

try:  # Installed by the dedicated live-runner requirements file.
    import aiohttp
except ImportError:  # pragma: no cover - keeps pure helper tests dependency-light
    aiohttp = None

try:  # Keeps the pure helper tests importable before the live SDK is installed.
    from kalshi_python_async import (
        BookSide,
        Configuration,
        EventsApi,
        KalshiAuth,
        KalshiClient,
        MarketApi,
        OrdersApi,
        PortfolioApi,
        SelfTradePreventionType,
    )
except ImportError:  # pragma: no cover - exercised only in minimal local environments
    BookSide = Configuration = EventsApi = KalshiAuth = KalshiClient = MarketApi = OrdersApi = PortfolioApi = None
    SelfTradePreventionType = None


LOG = logging.getLogger("kalshi_btc15m_average_down")
SERIES_TICKER = "KXBTC15M"
LADDER_LEVELS = (0.40, 0.30, 0.20, 0.10)
CONFIG_VERSION = 3
STATE_VERSION = 1
ORDER_NAMESPACE = uuid.UUID("4d85857e-4dc6-43ec-960f-0b342523bdb7")
KALSHI_WS_URL = os.getenv(
    "KALSHI_WS_URL",
    "wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2"
    if os.getenv("KALSHI_DEMO", "false").lower() in {"1", "true", "yes"}
    else "wss://external-api-ws.kalshi.com/trade-api/ws/v2",
)
QUOTE_STALE_SECONDS = 20.0

DEFAULT_CONFIG = {
    "format_version": CONFIG_VERSION,
    # Contracts per rung.  This is a quantity, not a dollar amount.
    "initial_position_size": 1.00,
    "max_active_markets": 1,
    "max_contracts_per_market": 4.00,
    # Principal reserved for all four possible rungs.  Fees are checked against
    # available balance separately with fee_reserve.
    "max_total_capital": 1.00,
    "fee_reserve": 0.05,
    # Upper bound on sleep while waiting for the WebSocket; it is not a REST
    # quote-poll interval. Quote changes wake the runner immediately.
    "poll_seconds": 2.0,
    # REST is retained only for market discovery, settlement, and authoritative
    # order reconciliation if the stream is interrupted.
    "market_refresh_seconds": 15.0,
    "order_reconcile_seconds": 5.0,
    "status_log_seconds": 30.0,
}


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )


def now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def as_float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def field(obj: Any, *names: str) -> Any:
    for name in names:
        value = getattr(obj, name, None)
        if value is not None:
            return value
        if isinstance(obj, dict) and obj.get(name) is not None:
            return obj[name]
    return None


def side_api_price(side: str, position_price: float) -> str:
    """Translate a YES/NO purchase into Kalshi's single YES-book V2 price."""
    if side == "yes":
        return f"{position_price:.4f}"
    if side == "no":
        return f"{1.0 - position_price:.4f}"
    raise ValueError(f"Unsupported side: {side}")


def side_book_side(side: str):
    if BookSide is None:
        raise RuntimeError("kalshi-python-async is not installed")
    if side == "yes":
        return BookSide.BID
    if side == "no":
        return BookSide.ASK
    raise ValueError(f"Unsupported side: {side}")


def market_is_active(market: Any) -> bool:
    return str(field(market, "status") or "").lower() == "active"


def timestamp_epoch(raw: Any) -> int | None:
    if raw is None:
        return None
    numeric = as_float(raw)
    if numeric is not None:
        return int(numeric)
    try:
        parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp())
    except ValueError:
        return None


def market_is_tradeable(market: Any) -> bool:
    """Trade only in the actual open window, never during an active pre-open."""
    open_time = timestamp_epoch(field(market, "open_time"))
    close_time = timestamp_epoch(field(market, "close_time", "expected_expiration_time"))
    now = time.time()
    return (
        market_is_active(market)
        and (open_time is None or now >= open_time)
        and (close_time is None or now < close_time)
    )


def market_result(market: Any) -> str | None:
    raw = field(market, "result")
    result = str(getattr(raw, "value", raw) or "").lower()
    return result if result in {"yes", "no"} else None


def normalized_order_status(raw: Any) -> str:
    """Accept SDK enums as well as plain status strings (e.g. OrderStatus.CANCELED)."""
    value = getattr(raw, "value", raw)
    status = str(value or "").lower()
    return status.rsplit(".", 1)[-1]


def market_asks(market: Any, live_asks: dict[str, float | None] | None = None) -> dict[str, float | None]:
    """Read executable position asks as dollars, accepting API migrations."""
    if live_asks is not None:
        return {
            side: value if (value := as_float(live_asks.get(side))) is not None and 0.0 < value < 1.0 else None
            for side in ("yes", "no")
        }
    result: dict[str, float | None] = {}
    for side in ("yes", "no"):
        value = as_float(field(market, f"{side}_ask_dollars", f"{side}_ask"))
        result[side] = value if value is not None and 0.0 < value < 1.0 else None
    return result


def choose_entry_side(asks: dict[str, float | None]) -> tuple[str, float] | None:
    """Choose only from <=40c asks; lower price wins, YES breaks an exact tie."""
    candidates = [(price, side) for side, price in asks.items()
                  if price is not None and price <= LADDER_LEVELS[0]]
    if not candidates:
        return None
    price, side = min(candidates, key=lambda item: (item[0], item[1] != "yes"))
    return side, price


def ladder_principal(quantity: float) -> float:
    return round(sum(LADDER_LEVELS) * quantity, 6)


def load_json(path: Path, default: dict[str, Any]) -> dict[str, Any]:
    if not path.exists():
        return dict(default)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        LOG.warning("Cannot read %s; using defaults.", path)
        return dict(default)
    return payload if isinstance(payload, dict) else dict(default)


def save_json(path: Path, payload: dict[str, Any]) -> None:
    # SDK response objects may expose close_time as datetime.  State persistence
    # must never fail after an accepted live order, so serialize those values as
    # ISO-like strings rather than leaving an in-memory-only position.
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")


def validate_config(config: dict[str, Any]) -> dict[str, Any]:
    merged = {**DEFAULT_CONFIG, **config, "format_version": CONFIG_VERSION}
    for name in (
        "initial_position_size", "max_contracts_per_market", "max_total_capital",
        "fee_reserve", "poll_seconds", "market_refresh_seconds", "order_reconcile_seconds",
        "status_log_seconds",
    ):
        value = as_float(merged.get(name))
        if value is None or value <= 0:
            raise ValueError(f"{name} must be positive")
        merged[name] = value
    active = int(merged.get("max_active_markets", 0))
    if active < 1:
        raise ValueError("max_active_markets must be at least one")
    merged["max_active_markets"] = active
    quantity = merged["initial_position_size"]
    if round(quantity * len(LADDER_LEVELS), 2) > merged["max_contracts_per_market"] + 1e-9:
        raise ValueError("initial_position_size * four ladder levels exceeds max_contracts_per_market")
    if ladder_principal(quantity) > merged["max_total_capital"] + 1e-9:
        raise ValueError("max_total_capital cannot fund the complete four-level ladder")
    return merged


def apply_config_overrides(config: dict[str, Any], args: argparse.Namespace) -> tuple[dict[str, Any], bool]:
    names = (
        "initial_position_size", "max_active_markets", "max_contracts_per_market",
        "max_total_capital", "fee_reserve", "poll_seconds", "market_refresh_seconds",
        "order_reconcile_seconds", "status_log_seconds",
    )
    changed = False
    updated = dict(config)
    for name in names:
        value = getattr(args, name, None)
        if value is not None:
            updated[name] = value
            changed = True
    return validate_config(updated), changed


def default_state() -> dict[str, Any]:
    return {"format_version": STATE_VERSION, "markets": {}}


def order_fill_count(order: Any) -> float:
    return as_float(field(order, "fill_count_fp", "fill_count")) or 0.0


def order_remaining_count(order: Any) -> float | None:
    return as_float(field(order, "remaining_count_fp", "remaining_count"))


def order_average_position_price(order: Any, side: str, fallback: float) -> float:
    yes_price = as_float(field(order, "average_fill_price", "yes_price_dollars", "yes_price"))
    if yes_price is None:
        return fallback
    return round(yes_price if side == "yes" else 1.0 - yes_price, 4)


def order_fee_total(order: Any) -> float:
    explicit = as_float(field(order, "fees_paid_dollars", "fee_paid_dollars"))
    if explicit is not None:
        return max(0.0, explicit)
    parts = [as_float(field(order, "taker_fees_dollars")), as_float(field(order, "maker_fees_dollars"))]
    return round(sum(value for value in parts if value is not None), 6)


def client_order_id(ticker: str, side: str, order_key: str) -> str:
    """Stable idempotency key, including rung role rather than just price.

    An initial protected IOC can itself be observed at 30c/20c/10c. It must
    not collide with the separately requested averaging rung at that price.
    """
    return str(uuid.uuid5(ORDER_NAMESPACE, f"average-down-v1:{ticker}:{side}:{order_key}"))


def classify_submission(fill_count: float, remaining_count: float, quantity: float, tif: str) -> str:
    """Classify a create-order response without mistaking canceled IOC for fill."""
    tolerance = 0.004
    if fill_count >= quantity - tolerance and fill_count > tolerance:
        return "filled"
    if fill_count > tolerance:
        return "resting_partial" if tif == "good_till_canceled" else "partially_filled_canceled"
    if tif == "good_till_canceled" and remaining_count > tolerance:
        return "resting"
    return "canceled_unfilled"


@dataclass
class KalshiLiveFeed:
    """Authenticated Kalshi stream used for quote-triggered decisions.

    REST remains the source of truth for discovery, order status, and
    settlement.  The stream prevents those periodic checks from becoming the
    quote trigger: a ticker update wakes the strategy immediately.
    """

    auth: Any
    url: str = KALSHI_WS_URL

    def __post_init__(self) -> None:
        self.path = urlparse(self.url).path or "/trade-api/ws/v2"
        self.desired_tickers: set[str] = set()
        self.subscribed_tickers: set[str] = set()
        self.quotes: dict[str, dict[str, Any]] = {}
        self.connected = False
        self.message_count = 0
        self.update_count = 0
        self.private_update_count = 0
        self._command_id = 0
        self._wake = asyncio.Event()

    def set_tickers(self, tickers: list[str] | set[str] | tuple[str, ...]) -> None:
        desired = {str(ticker) for ticker in tickers if ticker}
        if desired != self.desired_tickers:
            self.desired_tickers = desired
            self._wake.set()

    def executable_asks(self, ticker: str) -> dict[str, float | None] | None:
        """Return current YES/NO executable asks from a fresh ticker update."""
        quote = self.quotes.get(ticker)
        if not quote or time.monotonic() - float(quote.get("received_monotonic") or 0.0) > QUOTE_STALE_SECONDS:
            return None
        yes_ask = as_float(quote.get("yes_ask"))
        yes_bid = as_float(quote.get("yes_bid"))
        if yes_ask is None or yes_bid is None:
            return None
        no_ask = 1.0 - yes_bid
        if not (0.0 < yes_ask < 1.0 and 0.0 < no_ask < 1.0):
            return None
        return {"yes": round(yes_ask, 4), "no": round(no_ask, 4)}

    async def wait_for_update(self, timeout: float, observed_update_count: int) -> int:
        """Return the latest update sequence without losing a just-arrived event."""
        if self.update_count != observed_update_count:
            return self.update_count
        self._wake.clear()
        # A message can arrive between the check above and clear(). Re-check
        # the monotonically increasing sequence so that event is not lost.
        if self.update_count != observed_update_count:
            return self.update_count
        try:
            await asyncio.wait_for(self._wake.wait(), timeout=max(0.01, timeout))
        except asyncio.TimeoutError:
            pass
        return self.update_count

    async def _subscribe_private(self, ws: Any) -> None:
        self._command_id += 1
        await ws.send_json({
            "id": self._command_id,
            "cmd": "subscribe",
            "params": {"channels": ["fill", "user_orders"]},
        })

    async def _subscribe_tickers(self, ws: Any, tickers: set[str]) -> None:
        if not tickers:
            return
        self._command_id += 1
        await ws.send_json({
            "id": self._command_id,
            "cmd": "subscribe",
            "params": {"channels": ["ticker"], "market_tickers": sorted(tickers)},
        })
        self.subscribed_tickers.update(tickers)
        LOG.info("WS SUBSCRIBE | tickers=%s", ",".join(sorted(tickers)))

    async def _sync_subscriptions(self, ws: Any) -> None:
        missing = self.desired_tickers - self.subscribed_tickers
        if missing:
            await self._subscribe_tickers(ws, missing)

    def _handle(self, raw: str) -> None:
        try:
            payload = json.loads(raw)
        except (TypeError, ValueError):
            return
        self.message_count += 1
        message_type = str(payload.get("type") or "").lower()
        if message_type == "error":
            LOG.warning("WS ERROR | %s", payload.get("msg"))
            return
        if message_type in {"fill", "user_orders"}:
            self.update_count += 1
            self.private_update_count += 1
            self._wake.set()
            return
        if message_type != "ticker":
            return
        message = payload.get("msg") or {}
        ticker = str(message.get("market_ticker") or message.get("ticker") or "")
        if not ticker:
            return
        quote = self.quotes.setdefault(ticker, {})
        yes_bid = as_float(message.get("yes_bid_dollars", message.get("yes_bid")))
        yes_ask = as_float(message.get("yes_ask_dollars", message.get("yes_ask")))
        if yes_bid is not None:
            quote["yes_bid"] = yes_bid
        if yes_ask is not None:
            quote["yes_ask"] = yes_ask
        quote["received_monotonic"] = time.monotonic()
        self.update_count += 1
        self._wake.set()

    async def _session_loop(self, ws: Any) -> None:
        await self._subscribe_private(ws)
        while True:
            await self._sync_subscriptions(ws)
            try:
                message = await ws.receive(timeout=1.0)
            except asyncio.TimeoutError:
                continue
            if message.type == aiohttp.WSMsgType.TEXT:
                self._handle(message.data)
            elif message.type in {aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.ERROR}:
                return

    async def run(self) -> None:
        if aiohttp is None:
            LOG.warning("WS UNAVAILABLE | aiohttp is not installed; using REST fallback checks.")
            return
        while True:
            try:
                headers = self.auth.create_auth_headers("GET", self.path)
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(self.url, headers=headers, heartbeat=10) as ws:
                        self.connected = True
                        self.subscribed_tickers.clear()
                        LOG.info("WS CONNECTED | %s", self.url)
                        await self._session_loop(ws)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                LOG.warning("WS DISCONNECTED | %s", exc)
            finally:
                self.connected = False
                self.subscribed_tickers.clear()
            LOG.info("WS RECONNECT | retrying in 5s")
            await asyncio.sleep(5)


@dataclass
class KalshiREST:
    """Minimal Kalshi V2 client.  It contains no strategy decision code."""

    api_key_id: str
    pem_path: Path
    demo: bool = False

    def __post_init__(self) -> None:
        if KalshiClient is None or KalshiAuth is None:
            raise RuntimeError("Install requirements_kalshi_average_down.txt before running")
        pem = self.pem_path.read_text(encoding="utf-8")
        base_url = "https://demo-api.kalshi.co/trade-api/v2" if self.demo else "https://api.elections.kalshi.com/trade-api/v2"
        configuration = Configuration(host=base_url)
        configuration.api_key_id = self.api_key_id
        configuration.private_key_pem = pem
        self.client = KalshiClient(configuration)
        self.auth = KalshiAuth(self.api_key_id, pem)
        self.portfolio = PortfolioApi(self.client)
        self.events = EventsApi(self.client)
        self.markets = MarketApi(self.client)
        self.orders = OrdersApi(self.client)

    async def close(self) -> None:
        await self.client.close()

    async def balance_dollars(self) -> float | None:
        try:
            response = await self.portfolio.get_balance()
            balance = as_float(field(response, "balance_dollars"))
            if balance is not None:
                return balance
            cents = as_float(field(response, "balance"))
            return cents / 100.0 if cents is not None else None
        except Exception as exc:  # noqa: BLE001
            LOG.warning("Balance lookup failed: %s", exc)
            return None

    async def get_market(self, ticker: str) -> Any | None:
        try:
            response = await self.markets.get_market(ticker)
            return field(response, "market")
        except Exception as exc:  # noqa: BLE001
            LOG.warning("Market lookup failed for %s: %s", ticker, exc)
            return None

    async def active_markets(self) -> list[Any]:
        try:
            response = await self.events.get_events(
                series_ticker=SERIES_TICKER, status="open", with_nested_markets=True, limit=100,
            )
        except Exception as exc:  # noqa: BLE001
            LOG.warning("Active KXBTC15M lookup failed: %s", exc)
            return []
        markets: list[Any] = []
        for event in field(response, "events") or []:
            for market in field(event, "markets") or []:
                ticker = str(field(market, "ticker") or "")
                if ticker.startswith(SERIES_TICKER + "-") and market_is_tradeable(market):
                    markets.append(market)
        return markets

    async def create_order(
        self, *, ticker: str, side: str, position_price: float, quantity: float,
        tif: str, expiration_time: int | None, dry_run: bool, order_key: str,
    ) -> dict[str, Any]:
        record = {
            "client_order_id": client_order_id(ticker, side, order_key),
            "ticker": ticker,
            "side": side,
            "order_type": "ioc_protected" if tif == "immediate_or_cancel" else "limit",
            "position_price": round(position_price, 4),
            "api_price": side_api_price(side, position_price),
            "quantity": round(quantity, 2),
            "time_in_force": tif,
            "submitted_at": now_iso(),
            "status": "dry_run" if dry_run else "submitting",
            "fill_count": 0.0,
            "remaining_count": round(quantity, 2),
            "fees_paid": 0.0,
        }
        if dry_run:
            LOG.info("DRY RUN ORDER | %s %s @ $%.2f x %.2f (%s)", ticker, side.upper(), position_price, quantity, tif)
            return record
        kwargs = {
            "ticker": ticker,
            "side": side_book_side(side),
            "count": f"{quantity:.2f}",
            "price": record["api_price"],
            "time_in_force": tif,
            "client_order_id": record["client_order_id"],
            "self_trade_prevention_type": SelfTradePreventionType.TAKER_AT_CROSS,
            "reduce_only": False,
        }
        if expiration_time is not None:
            kwargs["expiration_time"] = int(expiration_time)
        try:
            response = await self.orders.create_order_v2(**kwargs)
        except Exception as exc:  # noqa: BLE001
            record["status"] = "submit_failed"
            record["error"] = str(exc)
            LOG.error("ORDER REJECTED | %s %s @ $%.2f: %s", ticker, side.upper(), position_price, exc)
            return record
        record["order_id"] = str(field(response, "order_id") or "") or None
        record["fill_count"] = round(order_fill_count(response), 2)
        record["remaining_count"] = round(order_remaining_count(response) if order_remaining_count(response) is not None else quantity - record["fill_count"], 2)
        record["average_fill_price"] = order_average_position_price(response, side, position_price)
        record["fees_paid"] = order_fee_total(response)
        # An IOC that gets no execution is returned with zero remaining
        # quantity because the exchange cancelled the remainder.  Zero
        # remaining is therefore *not* evidence of a fill.  Only call an
        # order filled when the reported fill quantity covers the request.
        record["status"] = classify_submission(
            record["fill_count"], record["remaining_count"], quantity, tif,
        )
        LOG.info("ORDER %s | %s %s @ $%.2f x %.2f | fill=%.2f remaining=%.2f id=%s",
                 record["status"].upper(), ticker, side.upper(), position_price, quantity,
                 record["fill_count"], record["remaining_count"], record["order_id"] or "?")
        return record

    async def refresh_order(self, record: dict[str, Any]) -> None:
        order_id = record.get("order_id")
        if not order_id or record.get("status") in {"dry_run", "submit_failed", "canceled"}:
            return
        try:
            response = await self.orders.get_order(order_id)
            order = field(response, "order")
            if order is None:
                return
            record["fill_count"] = round(order_fill_count(order), 2)
            remaining = order_remaining_count(order)
            if remaining is not None:
                record["remaining_count"] = round(remaining, 2)
            record["average_fill_price"] = order_average_position_price(order, record["side"], record["position_price"])
            record["fees_paid"] = max(float(record.get("fees_paid") or 0.0), order_fee_total(order))
            status = normalized_order_status(field(order, "status"))
            if status:
                record["status"] = status
            record["last_checked_at"] = now_iso()
        except Exception as exc:  # noqa: BLE001
            LOG.warning("Order lookup failed for %s: %s", order_id, exc)

    async def cancel_order(self, record: dict[str, Any], dry_run: bool) -> None:
        order_id = record.get("order_id")
        if not order_id or float(record.get("remaining_count") or 0.0) <= 0.004:
            return
        if dry_run:
            record["status"] = "dry_run_canceled"
            return
        try:
            await self.orders.cancel_order_v2(order_id)
            record["status"] = "canceled"
            record["canceled_at"] = now_iso()
            LOG.info("CANCELED | %s", order_id)
        except Exception as exc:  # noqa: BLE001
            # Closed markets reject cancellation after the exchange has already
            # canceled resting orders.  Keep the failure audit trail.
            record["cancel_error"] = str(exc)
            LOG.warning("Cancel failed for %s: %s", order_id, exc)


def expiration_epoch(market: Any) -> int | None:
    return timestamp_epoch(field(market, "close_time", "expected_expiration_time"))


def market_record(state: dict[str, Any], ticker: str) -> dict[str, Any]:
    markets = state.setdefault("markets", {})
    if ticker not in markets:
        markets[ticker] = {
            "ticker": ticker,
            "created_at": now_iso(),
            "strategy": "mechanical_price_average_down_v1",
            "orders": {},
            "locked_side": None,
            "status": "watching",
        }
    return markets[ticker]


def orders_for_market(record: dict[str, Any]) -> list[dict[str, Any]]:
    return [order for order in (record.get("orders") or {}).values() if isinstance(order, dict)]


def filled_contracts(record: dict[str, Any]) -> float:
    return round(sum(float(order.get("fill_count") or 0.0) for order in orders_for_market(record)), 2)


def active_strategy_records(state: dict[str, Any]) -> list[dict[str, Any]]:
    active: list[dict[str, Any]] = []
    for record in state.get("markets", {}).values():
        if not isinstance(record, dict) or record.get("status") not in {"initial_submitted", "ladder_active"}:
            continue
        close_time = timestamp_epoch(record.get("market_close_time"))
        if close_time is None or time.time() < close_time:
            active.append(record)
    return active


def reserved_principal(state: dict[str, Any]) -> float:
    total = 0.0
    for record in active_strategy_records(state):
        quantity = as_float(record.get("quantity"))
        if quantity is not None and record.get("candidate_side"):
            total += ladder_principal(quantity)
    return round(total, 6)


async def reconcile_orders(rest: KalshiREST, record: dict[str, Any], dry_run: bool) -> bool:
    """Refresh fills.  The first non-zero initial fill locks the market side."""
    changed = False
    for order in orders_for_market(record):
        before = (order.get("fill_count"), order.get("status"), order.get("fees_paid"))
        await rest.refresh_order(order)
        if before != (order.get("fill_count"), order.get("status"), order.get("fees_paid")):
            LOG.info(
                "ORDER UPDATE | %s %s status=%s->%s fill=%.2f->%.2f remaining=%.2f fees=$%.4f id=%s",
                record.get("ticker", "?"), str(order.get("side", "?")).upper(),
                before[1], order.get("status"), float(before[0] or 0.0),
                float(order.get("fill_count") or 0.0), float(order.get("remaining_count") or 0.0),
                float(order.get("fees_paid") or 0.0),
                order.get("order_id") or order.get("client_order_id") or "?",
            )
            changed = True
    initial = (record.get("orders") or {}).get("0.4000")
    if isinstance(initial, dict) and float(initial.get("fill_count") or 0.0) > 0.004 and not record.get("locked_side"):
        record["locked_side"] = record.get("candidate_side")
        record["locked_at"] = now_iso()
        record["status"] = "ladder_active"
        LOG.info("SIDE LOCKED | %s %s after initial fill %.2f", record["ticker"], record["locked_side"].upper(), initial["fill_count"])
        changed = True
    elif isinstance(initial, dict) and float(initial.get("fill_count") or 0.0) <= 0.004 and initial.get("status") in {
        "canceled", "canceled_unfilled", "rejected", "expired",
    } and record.get("status") == "initial_submitted":
        # A protected IOC with no fill is not a position and must not consume
        # the active-market slot or the capital reserve on a later handoff.
        record["status"] = "initial_unfilled"
        record["initial_unfilled_at"] = now_iso()
        LOG.info("INITIAL UNFILLED | %s %s; zero contracts held, slot released.",
                 record["ticker"], str(record.get("candidate_side") or "?").upper())
        changed = True
    return changed


async def submit_ladder(
    rest: KalshiREST, record: dict[str, Any], market: Any, config: dict[str, Any], dry_run: bool,
) -> None:
    if not record.get("locked_side"):
        return
    side = record["locked_side"]
    quantity = float(record["quantity"])
    expiry = expiration_epoch(market)
    initial = (record.get("orders") or {}).get("0.4000") or {}
    initial_fill_price = float(initial.get("average_fill_price") or initial.get("position_price") or LADDER_LEVELS[0])
    for level in LADDER_LEVELS[1:]:
        # If discovery was already below 40c, only submit genuinely lower
        # rungs.  A 10c entry must never generate a 30c/20c buy, which would
        # be averaging *up* rather than down.
        if level >= initial_fill_price - 1e-9:
            continue
        key = f"{level:.4f}"
        if key in record["orders"]:
            continue
        submitted_contracts = sum(float(order.get("quantity") or 0.0) for order in orders_for_market(record))
        if submitted_contracts + quantity > config["max_contracts_per_market"] + 1e-9:
            LOG.warning("SKIP CONTRACT CAP | %s requested=%.2f cap=%.2f", record["ticker"], submitted_contracts + quantity, config["max_contracts_per_market"])
            return
        balance = await rest.balance_dollars()
        required_cash = level * quantity + config["fee_reserve"]
        if balance is None or balance + 1e-9 < required_cash:
            LOG.warning("SKIP LADDER BALANCE | %s %s @ $%.2f need >= $%.2f available=%s",
                        record["ticker"], side.upper(), level, required_cash, balance)
            return
        LOG.info("AVERAGING LIMIT | %s %s @ $%.2f x %.2f", record["ticker"], side.upper(), level, quantity)
        record["orders"][key] = await rest.create_order(
            ticker=record["ticker"], side=side, position_price=level, quantity=quantity,
            tif="good_till_canceled", expiration_time=expiry, dry_run=dry_run, order_key=key,
        )


async def settle_or_cancel(rest: KalshiREST, record: dict[str, Any], market: Any, dry_run: bool) -> None:
    if market_is_tradeable(market):
        return
    record["status"] = "closed_waiting_finalization"
    record["closed_at"] = now_iso()
    for order in orders_for_market(record):
        await rest.cancel_order(order, dry_run)
    result = market_result(market)
    status = str(field(market, "status") or "").lower()
    if result is None or status != "finalized":
        return
    side = record.get("locked_side") or record.get("candidate_side")
    quantity = filled_contracts(record)
    if quantity <= 0.004:
        # An expired/canceled initial order with no execution is operational
        # audit data, not a trade.  Keep it in the ledger but never let it
        # distort win rate, P&L, streaks, or rung statistics.
        record.update({
            "status": "finalized_unfilled", "settled_at": now_iso(), "settlement_outcome": result,
            "contracts": 0.0, "total_cost": 0.0, "average_entry": None,
            "gross_payout": 0.0, "gross_profit_loss": 0.0, "kalshi_fees": 0.0,
            "net_profit_loss": 0.0, "return_percentage": None,
        })
        LOG.info("FINALIZED UNFILLED | %s %s: zero contracts held; excluded from performance.",
                 record["ticker"], result.upper())
        return
    cost = sum(float(order.get("fill_count") or 0.0) * float(order.get("average_fill_price") or order.get("position_price") or 0.0)
               for order in orders_for_market(record))
    fees = sum(float(order.get("fees_paid") or 0.0) for order in orders_for_market(record))
    payout = quantity if side == result else 0.0
    gross = payout - cost
    net = gross - fees
    record.update({
        "status": "finalized", "settled_at": now_iso(), "settlement_outcome": result,
        "contracts": round(quantity, 2), "total_cost": round(cost, 6),
        "average_entry": round(cost / quantity, 6) if quantity else None,
        "gross_payout": round(payout, 6), "gross_profit_loss": round(gross, 6),
        "kalshi_fees": round(fees, 6), "net_profit_loss": round(net, 6),
        "return_percentage": round(100.0 * net / cost, 4) if cost else None,
    })
    LOG.info("SETTLED | %s %s contracts=%.2f net=$%.4f", record["ticker"], result.upper(), quantity, net)


async def consider_initial_entry(
    rest: KalshiREST, state: dict[str, Any], market: Any, config: dict[str, Any], dry_run: bool,
    live_asks: dict[str, float | None] | None = None,
) -> bool:
    ticker = str(field(market, "ticker") or "")
    if not ticker or not market_is_tradeable(market):
        return False
    record = state.get("markets", {}).get(ticker)
    if isinstance(record, dict) and record.get("candidate_side"):
        return False
    if len(active_strategy_records(state)) >= config["max_active_markets"]:
        return False
    choice = choose_entry_side(market_asks(market, live_asks))
    if choice is None:
        return False
    side, ask = choice
    quantity = config["initial_position_size"]
    reserve = ladder_principal(quantity)
    if reserved_principal(state) + reserve > config["max_total_capital"] + 1e-9:
        LOG.warning("SKIP CAPITAL | %s reserve=$%.2f cap=$%.2f", ticker, reserve, config["max_total_capital"])
        return False
    balance = await rest.balance_dollars()
    if balance is None or balance + 1e-9 < reserve + config["fee_reserve"]:
        LOG.warning("SKIP BALANCE | %s need >= $%.2f including fee reserve; available=%s", ticker, reserve + config["fee_reserve"], balance)
        return False
    record = market_record(state, ticker)
    record.update({
        "candidate_side": side, "quantity": quantity, "status": "initial_submitted",
        "initial_ask": round(ask, 4), "initial_reason": f"{side.upper()} ask reached <= $0.40",
        "reserved_principal": reserve, "market_close_time": field(market, "close_time"),
    })
    # When already below 40c, IOC at the observed price is the protected
    # equivalent of a market order.  It cannot fill above the observed <=40c ask.
    below_threshold = ask < LADDER_LEVELS[0] - 1e-9
    tif = "immediate_or_cancel" if below_threshold else "good_till_canceled"
    price = ask if below_threshold else LADDER_LEVELS[0]
    record["orders"]["0.4000"] = await rest.create_order(
        ticker=ticker, side=side, position_price=price, quantity=quantity, tif=tif,
        expiration_time=None if below_threshold else expiration_epoch(market), dry_run=dry_run,
        order_key="initial",
    )
    record["orders"]["0.4000"]["ladder_level"] = 0.40
    record["orders"]["0.4000"]["reason"] = (
        "Market discovered below 40c; protected immediate-or-cancel order."
        if below_threshold else "Ask reached the 40c initial threshold."
    )
    return True


def streak(values: list[float], winning: bool) -> int:
    best = current = 0
    for value in values:
        if (value > 0) == winning:
            current += 1
            best = max(best, current)
        else:
            current = 0
    return best


def empty_rung_performance(level: float) -> dict[str, Any]:
    return {
        "rung_price": level,
        "filled_orders": 0,
        "filled_contracts": 0.0,
        "winning_orders": 0,
        "losing_orders": 0,
        "breakeven_orders": 0,
        "winning_contracts": 0.0,
        "losing_contracts": 0.0,
        "net_profit": 0.0,
    }


def rung_performance(settled: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Attribute settled P&L to the actual filled 40c/30c/20c/10c order rungs."""
    stats = {f"{level:.2f}": empty_rung_performance(level) for level in LADDER_LEVELS}
    for record in settled:
        resolved_side = record.get("settlement_outcome")
        position_side = record.get("locked_side") or record.get("candidate_side")
        for level in LADDER_LEVELS:
            order = (record.get("orders") or {}).get(f"{level:.4f}")
            if not isinstance(order, dict):
                continue
            fill = float(order.get("fill_count") or 0.0)
            if fill <= 0.004:
                continue
            result = stats[f"{level:.2f}"]
            average_price = float(order.get("average_fill_price") or order.get("position_price") or 0.0)
            fee = float(order.get("fees_paid") or 0.0)
            order_net = (fill if position_side == resolved_side else 0.0) - fill * average_price - fee
            result["filled_orders"] += 1
            result["filled_contracts"] += fill
            result["net_profit"] += order_net
            if order_net > 1e-9:
                result["winning_orders"] += 1
                result["winning_contracts"] += fill
            elif order_net < -1e-9:
                result["losing_orders"] += 1
                result["losing_contracts"] += fill
            else:
                result["breakeven_orders"] += 1
    for result in stats.values():
        denominator = result["winning_orders"] + result["losing_orders"]
        result["win_rate"] = round(result["winning_orders"] / denominator, 6) if denominator else None
        result["filled_contracts"] = round(result["filled_contracts"], 2)
        result["winning_contracts"] = round(result["winning_contracts"], 2)
        result["losing_contracts"] = round(result["losing_contracts"], 2)
        result["net_profit"] = round(result["net_profit"], 6)
    return stats


def performance_report(state: dict[str, Any], config: dict[str, Any]) -> dict[str, Any]:
    settled = sorted((record for record in state.get("markets", {}).values()
                      if isinstance(record, dict) and record.get("status") == "finalized"
                      and float(record.get("contracts") or 0.0) > 0.004),
                     key=lambda record: str(record.get("settled_at") or ""))
    unfilled = sum(1 for record in state.get("markets", {}).values()
                   if isinstance(record, dict) and (
                       record.get("status") == "finalized_unfilled"
                       or (record.get("status") == "finalized" and float(record.get("contracts") or 0.0) <= 0.004)
                   ))
    pnls = [float(record.get("net_profit_loss") or 0.0) for record in settled]
    costs = [float(record.get("total_cost") or 0.0) for record in settled]
    contracts = [float(record.get("contracts") or 0.0) for record in settled]
    wins = sum(value > 0 for value in pnls)
    losses = sum(value < 0 for value in pnls)
    gross_win = sum(value for value in pnls if value > 0)
    gross_loss = -sum(value for value in pnls if value < 0)
    equity = peak = 0.0
    drawdowns: list[float] = []
    for value in pnls:
        equity += value
        peak = max(peak, equity)
        drawdowns.append(peak - equity)
    mean = sum(pnls) / len(pnls) if pnls else 0.0
    variance = sum((value - mean) ** 2 for value in pnls) / (len(pnls) - 1) if len(pnls) > 1 else 0.0
    report = {
        "generated_at": now_iso(), "strategy": "pure_mechanical_price_average_down_v1",
        "configuration": config, "total_markets_traded": len(settled), "unfilled_market_attempts": unfilled,
        "total_contracts_purchased": round(sum(contracts), 2), "winning_trades": wins,
        "losing_trades": losses, "win_rate": round(wins / len(settled), 6) if settled else None,
        "win_loss_ratio": round(wins / losses, 6) if losses else None,
        "average_contracts_per_market": round(sum(contracts) / len(settled), 6) if settled else None,
        "average_entry_price": round(sum(costs) / sum(contracts), 6) if sum(contracts) else None,
        "percentage_entering_at_40c": None, "percentage_starting_below_40c": None,
        "percentage_reaching_30c": None, "percentage_reaching_20c": None,
        "percentage_reaching_10c": None, "average_number_of_fills_per_trade": None,
        "total_gross_profit": round(sum(float(record.get("gross_profit_loss") or 0.0) for record in settled), 6),
        "total_fees": round(sum(float(record.get("kalshi_fees") or 0.0) for record in settled), 6),
        "net_profit": round(sum(pnls), 6), "return_on_capital": round(sum(pnls) / sum(costs), 6) if sum(costs) else None,
        "average_profit_per_market": round(mean, 6) if settled else None,
        "profit_factor": round(gross_win / gross_loss, 6) if gross_loss else None,
        "sharpe_ratio_per_market": round(mean / math.sqrt(variance), 6) if variance > 0 else None,
        "maximum_drawdown": round(max(drawdowns, default=0.0), 6),
        "longest_winning_streak": streak(pnls, True), "longest_losing_streak": streak(pnls, False),
        "largest_losing_trade": round(min(pnls, default=0.0), 6), "largest_winning_trade": round(max(pnls, default=0.0), 6),
        "worst_historical_drawdown": round(max(drawdowns, default=0.0), 6),
        "rung_performance": rung_performance(settled),
        "note": "Only finalized records with filled contracts count as trades. No historical model or backtest is used.",
    }
    if settled:
        levels = {level: 0 for level in LADDER_LEVELS}
        starts_below = 0
        fills = []
        for record in settled:
            initial = (record.get("orders") or {}).get("0.4000") or {}
            if float(initial.get("position_price") or 0.40) < 0.40:
                starts_below += 1
            fills.append(sum(float(order.get("fill_count") or 0.0) > 0.004 for order in orders_for_market(record)))
            for level in LADDER_LEVELS:
                order = (record.get("orders") or {}).get(f"{level:.4f}") or {}
                if float(order.get("fill_count") or 0.0) > 0.004:
                    levels[level] += 1
        report.update({
            "percentage_entering_at_40c": round(100 * (len(settled) - starts_below) / len(settled), 4),
            "percentage_starting_below_40c": round(100 * starts_below / len(settled), 4),
            "percentage_reaching_30c": round(100 * levels[0.30] / len(settled), 4),
            "percentage_reaching_20c": round(100 * levels[0.20] / len(settled), 4),
            "percentage_reaching_10c": round(100 * levels[0.10] / len(settled), 4),
            "average_number_of_fills_per_trade": round(sum(fills) / len(fills), 6),
        })
    return report


def log_performance_summary(report: dict[str, Any], context: str) -> None:
    """Print the realized ledger summary alongside live quote/position logs."""
    ratio = report["win_loss_ratio"]
    ratio_text = "n/a (no losses yet)" if ratio is None and report["winning_trades"] else (
        "n/a" if ratio is None else f"{ratio:.2f}"
    )
    LOG.info(
        "PERFORMANCE | %s settled=%d unfilled=%d wins=%d losses=%d win_rate=%s win_loss_ratio=%s "
        "net=$%.4f gross=$%.4f fees=$%.4f roi=%s profit_factor=%s max_drawdown=$%.4f "
        "streaks=W%d/L%d",
        context, report["total_markets_traded"], report["unfilled_market_attempts"],
        report["winning_trades"], report["losing_trades"],
        "n/a" if report["win_rate"] is None else f"{100 * report['win_rate']:.2f}%", ratio_text,
        report["net_profit"], report["total_gross_profit"], report["total_fees"],
        "n/a" if report["return_on_capital"] is None else f"{100 * report['return_on_capital']:.2f}%",
        "n/a" if report["profit_factor"] is None else f"{report['profit_factor']:.2f}",
        report["maximum_drawdown"], report["longest_winning_streak"], report["longest_losing_streak"],
    )
    for level, rung in report["rung_performance"].items():
        LOG.info(
            "RUNG PERFORMANCE | %sc filled_orders=%d contracts=%.2f winners=%d losers=%d "
            "win_rate=%s net=$%.4f",
            level, rung["filled_orders"], rung["filled_contracts"], rung["winning_orders"],
            rung["losing_orders"], "n/a" if rung["win_rate"] is None else f"{100 * rung['win_rate']:.2f}%",
            rung["net_profit"],
        )


async def log_heartbeat(
    rest: KalshiREST,
    state: dict[str, Any],
    active_markets: list[Any],
    config: dict[str, Any],
    dry_run: bool,
    elapsed_seconds: float,
    feed: KalshiLiveFeed | None = None,
) -> None:
    """Emit enough live context to audit decisions without 2-second log spam."""
    balance = await rest.balance_dollars()
    LOG.info(
        "HEARTBEAT | mode=%s elapsed=%.0fs quotes=%d tracked=%d active_positions=%d "
        "reserved=$%.4f cap=$%.4f balance=%s stream=%s stream_messages=%d fallback_check=%.1fs",
        "DRY_RUN" if dry_run else "LIVE", elapsed_seconds, len(active_markets),
        len(state.get("markets", {})), len(active_strategy_records(state)),
        reserved_principal(state), config["max_total_capital"],
        "unknown" if balance is None else f"${balance:.4f}",
        "connected" if feed and feed.connected else "fallback",
        0 if feed is None else feed.message_count, config["poll_seconds"],
    )
    for market in active_markets:
        ticker = str(field(market, "ticker") or "")
        live_asks = feed.executable_asks(ticker) if feed else None
        asks = market_asks(market, live_asks)
        choice = choose_entry_side(asks)
        LOG.info(
            "QUOTE | %s source=%s yes_ask=%s no_ask=%s entry_signal=%s close=%s",
            ticker or "?", "WS" if live_asks is not None else "REST_FALLBACK",
            "none" if asks["yes"] is None else f"${asks['yes']:.4f}",
            "none" if asks["no"] is None else f"${asks['no']:.4f}",
            "none" if choice is None else f"{choice[0].upper()} @ ${choice[1]:.4f}",
            field(market, "close_time", "expected_expiration_time") or "unknown",
        )
    for record in state.get("markets", {}).values():
        if not isinstance(record, dict) or record.get("status") == "finalized":
            continue
        LOG.info(
            "POSITION | %s status=%s side=%s filled=%.2f submitted=%.2f",
            record.get("ticker", "?"), record.get("status", "?"),
            str(record.get("locked_side") or record.get("candidate_side") or "none").upper(),
            filled_contracts(record),
            sum(float(order.get("quantity") or 0.0) for order in orders_for_market(record)),
        )
        for order in orders_for_market(record):
            LOG.info(
                "ORDER | %s side=%s price=$%.4f qty=%.2f fill=%.2f remaining=%.2f status=%s id=%s",
                record.get("ticker", "?"), str(order.get("side", "?")).upper(),
                float(order.get("position_price") or 0.0), float(order.get("quantity") or 0.0),
                float(order.get("fill_count") or 0.0), float(order.get("remaining_count") or 0.0),
                order.get("status", "?"), order.get("order_id") or order.get("client_order_id") or "?",
            )
    log_performance_summary(performance_report(state, config), "heartbeat")


async def run(args: argparse.Namespace) -> int:
    config_path = args.config.expanduser()
    config = validate_config(load_json(config_path, DEFAULT_CONFIG))
    config, config_changed = apply_config_overrides(config, args)
    if args.persist_config or config_changed:
        save_json(config_path, config)
    dry_run = os.getenv("DRY_RUN", "true").lower() in {"1", "true", "yes"}
    live_allowed = not dry_run and args.submit and args.allow_live
    if not dry_run and not live_allowed:
        raise SystemExit("Refusing live orders: pass both --submit and --allow-live with DRY_RUN=false")
    state_path = args.state_file.expanduser()
    state = load_json(state_path, default_state())
    state.setdefault("format_version", STATE_VERSION)
    state.setdefault("markets", {})
    api_key = os.getenv("KALSHI_API_KEY_ID", "")
    pem_path = Path(os.getenv("KALSHI_PEM_PATH", "kalshi_private_key.pem"))
    if not api_key or not pem_path.exists():
        raise SystemExit("KALSHI_API_KEY_ID and KALSHI_PEM_PATH are required")
    rest = KalshiREST(api_key, pem_path, os.getenv("KALSHI_DEMO", "false").lower() in {"1", "true", "yes"})
    feed = KalshiLiveFeed(rest.auth)
    feed_task = asyncio.create_task(feed.run(), name="kalshi-average-down-ws")
    started_at = asyncio.get_running_loop().time()
    deadline = started_at + args.run_seconds
    last_heartbeat_at = float("-inf")
    last_market_refresh_at = float("-inf")
    last_order_reconcile_at = float("-inf")
    last_feed_update_count = feed.update_count
    last_private_update_count = 0
    active_markets: list[Any] = []
    LOG.info(
        "STARTUP | mode=%s run_seconds=%.0f quantity_per_rung=%.2f ladder=%s capital_cap=$%.4f",
        "DRY_RUN" if dry_run else "LIVE", args.run_seconds, config["initial_position_size"],
        "/".join(f"${level:.2f}" for level in LADDER_LEVELS), config["max_total_capital"],
    )
    log_performance_summary(performance_report(state, config), "startup")
    try:
        while True:
            monotonic_now = asyncio.get_running_loop().time()
            # Discover the current KXBTC15M window periodically.  Once known,
            # the authenticated ticker stream—not REST polling—is the entry
            # trigger for every quote change.
            if monotonic_now - last_market_refresh_at >= config["market_refresh_seconds"]:
                active_markets = await rest.active_markets()
                last_market_refresh_at = monotonic_now

            tracked_tickers = [
                str(ticker) for ticker, record in state["markets"].items()
                if isinstance(record, dict) and record.get("status") not in {"finalized", "finalized_unfilled"}
            ]
            feed.set_tickers(tracked_tickers + [str(field(market, "ticker") or "") for market in active_markets])

            # A private fill/order update gets an immediate authoritative REST
            # reconciliation.  The interval is only a recovery path when a
            # stream message was missed or the connection was interrupted.
            private_update = feed.private_update_count != last_private_update_count
            if private_update or monotonic_now - last_order_reconcile_at >= config["order_reconcile_seconds"]:
                for ticker, record in list(state["markets"].items()):
                    if not isinstance(record, dict) or record.get("status") in {"finalized", "finalized_unfilled"}:
                        continue
                    market = await rest.get_market(ticker)
                    if market is None:
                        continue
                    await reconcile_orders(rest, record, dry_run)
                    await settle_or_cancel(rest, record, market, dry_run)
                    if market_is_tradeable(market):
                        await submit_ladder(rest, record, market, config, dry_run)
                last_order_reconcile_at = monotonic_now
                last_private_update_count = feed.private_update_count

            for market in active_markets:
                ticker = str(field(market, "ticker") or "")
                await consider_initial_entry(
                    rest, state, market, config, dry_run,
                    live_asks=feed.executable_asks(ticker),
                )
            monotonic_now = asyncio.get_running_loop().time()
            if monotonic_now - last_heartbeat_at >= config["status_log_seconds"]:
                await log_heartbeat(rest, state, active_markets, config, dry_run, monotonic_now - started_at, feed)
                last_heartbeat_at = monotonic_now
            save_json(state_path, state)
            save_json(args.report.expanduser(), performance_report(state, config))
            if monotonic_now >= deadline:
                break
            next_due = min(
                deadline - monotonic_now,
                config["market_refresh_seconds"] - (monotonic_now - last_market_refresh_at),
                config["order_reconcile_seconds"] - (monotonic_now - last_order_reconcile_at),
                config["status_log_seconds"] - (monotonic_now - last_heartbeat_at),
            )
            last_feed_update_count = await feed.wait_for_update(
                min(config["poll_seconds"], max(0.01, next_due)), last_feed_update_count,
            )
    finally:
        feed_task.cancel()
        await asyncio.gather(feed_task, return_exceptions=True)
        save_json(state_path, state)
        final_report = performance_report(state, config)
        save_json(args.report.expanduser(), final_report)
        log_performance_summary(final_report, "run_complete")
        await rest.close()
    LOG.info("Average-down run complete | mode=%s active_records=%d", "DRY_RUN" if dry_run else "LIVE", len(active_strategy_records(state)))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("kalshi_btc15m_average_down_config.json"))
    parser.add_argument("--state-file", type=Path, default=Path("kalshi_btc15m_average_down_state.json"))
    parser.add_argument("--report", type=Path, default=Path("kalshi_btc15m_average_down_report.json"))
    parser.add_argument("--run-seconds", type=float, default=840.0)
    parser.add_argument("--persist-config", action="store_true")
    parser.add_argument("--submit", action="store_true")
    parser.add_argument("--allow-live", action="store_true")
    parser.add_argument("--initial-position-size", type=float)
    parser.add_argument("--max-active-markets", type=int)
    parser.add_argument("--max-contracts-per-market", type=float)
    parser.add_argument("--max-total-capital", type=float)
    parser.add_argument("--fee-reserve", type=float)
    parser.add_argument("--poll-seconds", type=float)
    parser.add_argument("--market-refresh-seconds", type=float)
    parser.add_argument("--order-reconcile-seconds", type=float)
    parser.add_argument("--status-log-seconds", type=float)
    return parser


if __name__ == "__main__":
    configure_logging()
    raise SystemExit(asyncio.run(run(build_parser().parse_args())))
