"""ML-side-selected KXBTC15M mechanical average-down trader.

The stored ML inference chooses one side before the market opens.  The
execution rule is then mechanical: it watches *only* that side's executable
ask and uses the fixed 40c -> 30c -> 20c -> 10c ladder.  There is no Prophet,
forecast, or mechanical-side fallback if ML inference is unavailable.

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

from kalshi_ml_features import FEATURE_SCHEMA, ML_ONLY_FEATURE_COLUMNS

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
CONFIG_VERSION = 5
STATE_VERSION = 3
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
    # This is *not* an entry window.  It is only the short allowance for
    # observing a brand-new market and starting its watcher.  Once started,
    # the watcher runs until the market closes or one side reaches 40c.
    "watch_start_grace_seconds": 45.0,
    # ML is computed before the next market opens from raw candles and settled
    # outcomes only. A watcher never chooses a side from prices; it only acts
    # after this frozen model side is ready.
    "ml_preopen_lead_seconds": 120.0,
    # Inclusive 50% confidence: every valid binary-model direction is eligible
    # for the price ladder. This is model coverage, not order coverage: the
    # selected side must still reach the mechanical 40c entry level.
    "ml_min_confidence": 0.50,
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
    """Translate an economic YES/NO purchase to the V2 exchange price."""
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


def market_can_start_watcher(market: Any, start_grace_seconds: float) -> bool:
    """Start a market watcher only at the market's opening, never late.

    The watcher is allowed to continue for the whole market after it starts.
    The small grace period absorbs normal market-discovery and handoff delay;
    it does not limit how long the watcher may wait for a 40c trigger.
    """
    open_time = timestamp_epoch(field(market, "open_time"))
    if open_time is None or not market_is_tradeable(market):
        return False
    seconds_since_open = time.time() - open_time
    return 0.0 <= seconds_since_open <= start_grace_seconds


def market_result(market: Any) -> str | None:
    raw = field(market, "result")
    result = str(getattr(raw, "value", raw) or "").lower()
    return result if result in {"yes", "no"} else None


def normalized_order_status(raw: Any) -> str:
    """Accept SDK enums as well as plain status strings (e.g. OrderStatus.CANCELED)."""
    value = getattr(raw, "value", raw)
    status = str(value or "").lower()
    return status.rsplit(".", 1)[-1]


def normalized_outcome_side(raw: Any) -> str | None:
    """Normalize Kalshi's canonical outcome-side field without guessing."""
    value = getattr(raw, "value", raw)
    side = str(value or "").lower().rsplit(".", 1)[-1]
    return side if side in {"yes", "no"} else None


def normalized_book_side(raw: Any) -> str | None:
    """Normalize the V2 book-side field, where bid is YES and ask is NO."""
    value = getattr(raw, "value", raw)
    side = str(value or "").lower().rsplit(".", 1)[-1]
    return side if side in {"bid", "ask"} else None


def exchange_outcome_side(order: Any) -> str | None:
    """Read Kalshi's canonical direction, with documented V2/legacy fallbacks."""
    canonical = normalized_outcome_side(field(order, "outcome_side"))
    if canonical is not None:
        return canonical
    book_side = normalized_book_side(field(order, "book_side"))
    if book_side is not None:
        return "yes" if book_side == "bid" else "no"
    legacy_side = normalized_outcome_side(field(order, "side"))
    action = str(getattr(field(order, "action"), "value", field(order, "action")) or "").lower()
    if legacy_side is None or action not in {"buy", "sell"}:
        return None
    if action == "buy":
        return legacy_side
    return "no" if legacy_side == "yes" else "yes"


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
    # Version 3 used a 15-second *entry* window.  Version 4 uses a persisted
    # whole-market watcher instead, so remove the retired setting when an old
    # state/configuration is handed forward.
    merged.pop("initial_entry_window_seconds", None)
    for name in (
        "initial_position_size", "max_contracts_per_market", "max_total_capital",
        "fee_reserve", "poll_seconds", "market_refresh_seconds", "order_reconcile_seconds",
        "watch_start_grace_seconds", "ml_preopen_lead_seconds", "ml_min_confidence",
        "status_log_seconds",
    ):
        value = as_float(merged.get(name))
        if value is None or value <= 0:
            raise ValueError(f"{name} must be positive")
        merged[name] = value
    if merged["ml_min_confidence"] < 0.5 or merged["ml_min_confidence"] > 1.0:
        raise ValueError("ml_min_confidence must be between 0.5 and 1.0")
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
        "order_reconcile_seconds", "watch_start_grace_seconds", "ml_preopen_lead_seconds",
        "ml_min_confidence", "status_log_seconds",
    )
    changed = False
    updated = dict(config)
    for name in names:
        value = getattr(args, name, None)
        if value is not None:
            updated[name] = value
            changed = True
    # The share input is the primary sizing control.  When it is changed by
    # itself, carry the complete four-rung contract cap and principal reserve
    # with it: 10 contracts per rung becomes a 40-contract/$10 ladder, not an
    # invalid 10-contract request against the old one-contract defaults.
    rung_override = as_float(getattr(args, "initial_position_size", None))
    if rung_override is not None and rung_override > 0:
        if getattr(args, "max_contracts_per_market", None) is None:
            updated["max_contracts_per_market"] = round(rung_override * len(LADDER_LEVELS), 2)
        if getattr(args, "max_total_capital", None) is None:
            updated["max_total_capital"] = ladder_principal(rung_override)
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


def managed_mechanical_order_role(order: Any) -> tuple[str, str] | None:
    """Identify an open order created by this runner without touching manual orders."""
    ticker = str(field(order, "ticker") or "")
    client_id = str(field(order, "client_order_id") or "")
    if not ticker.startswith(SERIES_TICKER + "-") or not client_id:
        return None
    for side in ("yes", "no"):
        for order_key in ("initial", "0.3000", "0.2000", "0.1000"):
            if client_id == client_order_id(ticker, side, order_key):
                return side, order_key
    return None


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


class MLDirectionSelector:
    """Prepare frozen ML directions before each BTC 15-minute market opens.

    The ML runner's Prophet-free feature construction is intentionally reused
    so this execution layer sees the exact saved model, raw-candle snapshot,
    and settled-outcome history used by the ML inference workflow. It never
    returns a price-derived direction and never falls back to a mechanical
    YES/NO choice.
    """

    def __init__(
        self,
        training_csv: Path,
        model_path: Path,
        preopen_lead_seconds: float,
        min_confidence: float,
        model_metadata: dict[str, Any] | None = None,
        model_run_id: str = "",
        training_run_id: str = "",
    ) -> None:
        if not training_csv.is_file():
            raise FileNotFoundError(f"Missing ML feature ledger: {training_csv}")
        if not model_path.is_file():
            raise FileNotFoundError(f"Missing saved ML model: {model_path}")
        self.training_csv = training_csv
        self.model_path = model_path
        self.preopen_lead_seconds = preopen_lead_seconds
        self.min_confidence = min_confidence
        self.model_metadata = model_metadata or {}
        self.model_run_id = model_run_id
        self.training_run_id = training_run_id
        self.tasks: dict[str, asyncio.Task[Any]] = {}
        # The exact instant at which each task's input is frozen.  A task may
        # finish after the market opens, but remains causal when this timestamp
        # is no later than that market's opening time.
        self.task_as_of: dict[str, Any] = {}
        self.ready: dict[str, dict[str, Any]] = {}
        self.completed_preopen: dict[str, bool] = {}
        self.logged_status: dict[str, str] = {}
        self._module: Any | None = None

    def module(self) -> Any:
        if self._module is None:
            # Delayed import keeps pure order/ledger unit tests lightweight.
            import kalshi_ml_inference_live as ml_inference
            self._module = ml_inference
        return self._module

    def _schedule(self, ml: Any, ticker: str, as_of: Any, reason: str) -> None:
        """Launch exactly one ML-input task with a recorded causal cutoff."""
        if ticker in self.tasks or ticker in self.ready:
            return
        frozen_at = ml.pd.Timestamp(as_of)
        frozen_at = frozen_at.tz_localize("UTC") if frozen_at.tzinfo is None else frozen_at.tz_convert("UTC")
        self.task_as_of[ticker] = frozen_at
        LOG.info(
            "ML PREOPEN START | %s %s frozen_as_of=%s.",
            ticker, reason, frozen_at.isoformat(),
        )
        self.tasks[ticker] = asyncio.create_task(
            ml.build_preopen_signal(self.training_csv, ticker, self.model_path, as_of=frozen_at),
            name=f"ml-preopen-{ticker}",
        )

    async def maybe_prepare_next(self) -> None:
        """Begin feature/model preparation only during the documented pre-open lead."""
        try:
            ml = self.module()
            self._harvest_completion(ml)
            current_ticker, next_ticker = ml.market_data.current_and_next_tickers(SERIES_TICKER)
            seconds_to_open = ml.market_data.seconds_until_ticker_settle(current_ticker)
        except Exception as exc:  # noqa: BLE001
            LOG.error("ML PREOPEN UNAVAILABLE | cannot resolve next market: %s", exc)
            return
        if seconds_to_open is None or not (0 < seconds_to_open <= self.preopen_lead_seconds):
            return
        self._schedule(
            ml, next_ticker, ml.pd.Timestamp.now(tz="UTC"),
            f"preparing {seconds_to_open:.0f}s before open",
        )

    async def side_for_market(self, market: Any, record: dict[str, Any]) -> str | None:
        """Return the frozen ML YES/NO direction for this exact live ticker."""
        ticker = str(field(market, "ticker") or "")
        # A continuous Actions handoff persists the frozen direction in the
        # strategy ledger.  Resume it rather than incorrectly treating the
        # already-open current market as having no ML side.
        persisted = record.get("ml_inference") if isinstance(record.get("ml_inference"), dict) else None
        if persisted is not None:
            side = str(persisted.get("side") or "").lower()
            confidence = as_float(persisted.get("confidence"))
            probability_yes = as_float(persisted.get("probability_yes"))
            prior_model_run = str(persisted.get("model_run_id") or "")
            if (
                side in {"yes", "no"}
                and confidence is not None and probability_yes is not None
                and confidence + 1e-12 >= self.min_confidence
                and (not self.model_run_id or prior_model_run == self.model_run_id)
            ):
                self._log_once(
                    record, "resumed",
                    "ML SIDE RESUMED | %s frozen side=%s p_yes=%.4f confidence=%.4f from prior handoff.",
                    ticker, side.upper(), probability_yes, confidence,
                )
                return side
        task = self.tasks.get(ticker)
        if task is None:
            try:
                # This covers a worker that begins in the first seconds of an
                # open market.  The reconstruction still uses only data before
                # the known market-open timestamp, never post-open prices.
                open_at = self.module().next_open_timestamp(ticker)
                self._schedule(
                    self.module(), ticker, open_at,
                    "reconstructing from this market's opening snapshot",
                )
            except Exception as exc:  # noqa: BLE001
                self._log_once(record, "missing_preopen", "ML SIDE FAILED | %s cannot determine market open: %s.", ticker, exc)
                return None
            self._log_once(
                record, "reconstructing",
                "ML SIDE WAIT | %s reconstructing its frozen market-open ML input; no order until the ML side is ready.",
                ticker,
            )
            return None
        if not task.done():
            # Production tasks always carry ``task_as_of``.  Retain the strict
            # rejection for an unlabelled/unknown task, but do not discard a
            # valid frozen calculation merely because network work finishes a
            # few seconds after the market opens.
            if ticker not in self.task_as_of:
                self.completed_preopen[ticker] = False
                task.cancel()
                self._log_once(
                    record, "late_preopen",
                    "ML SIDE FAILED | %s had no causal frozen-input timestamp; no order will be placed.",
                    ticker,
                )
                return None
            self._log_once(
                record, "preparing",
                "ML SIDE WAIT | %s frozen ML input is preparing; no order until the ML side is ready.", ticker,
            )
            return None
        ml = self.module()
        self._harvest_completion(ml)
        if not self.completed_preopen.get(ticker, False):
            self._log_once(
                record, "late_preopen", "ML SIDE FAILED | %s pre-open calculation was not complete before open; no order will be placed.", ticker,
            )
            return None
        if ticker not in self.ready:
            try:
                cached = task.result()
            except Exception as exc:  # noqa: BLE001
                self._log_once(record, "failed", "ML SIDE FAILED | %s inference error: %s; no order will be placed.", ticker, exc)
                return None
            if cached is None:
                self._log_once(record, "invalid", "ML SIDE FAILED | %s input validation failed; no order will be placed.", ticker)
                return None
            self.ready[ticker] = cached
        cached = self.ready[ticker]
        try:
            target = ml.market_data.extract_target(market)
            if target is None:
                raise ValueError("market strike is unavailable")
            features = ml.feature_values(
                cached["candles"], float(target), ml.known_outcomes(cached["rows"]),
                ml.next_open_timestamp(ticker),
            )
            vector = ml.np.asarray(
                [[float(features[name]) for name in ml.ML_ONLY_FEATURE_COLUMNS]], dtype=float,
            )
            probability_yes = float(cached["model"].predict_proba(vector)[0][1])
        except Exception as exc:  # noqa: BLE001
            self._log_once(record, "score_failed", "ML SIDE FAILED | %s scoring error: %s; no order will be placed.", ticker, exc)
            return None
        side = "yes" if probability_yes >= 0.5 else "no"
        confidence = probability_yes if side == "yes" else 1.0 - probability_yes
        if confidence + 1e-12 < self.min_confidence:
            self._log_once(
                record, "below_confidence",
                "ML SIDE SKIP | %s p_yes=%.4f confidence=%.4f < gate=%.4f; no order will be placed.",
                ticker, probability_yes, confidence, self.min_confidence,
            )
            return None
        training_rows = cached.get("rows")
        record["ml_inference"] = {
            "source": "stored_ml_preopen",
            "model_run_id": self.model_run_id or None,
            "training_ledger_run_id": self.training_run_id or None,
            "model_type": self.model_metadata.get("model_type"),
            "trained_at": self.model_metadata.get("trained_at"),
            "settlement_cutoff": self.model_metadata.get("settlement_cutoff"),
            "model_training_rows": self.model_metadata.get("training_rows"),
            "side": side,
            "probability_yes": round(probability_yes, 6),
            "confidence": round(confidence, 6),
            "prepared_at": str(cached.get("as_of") or ""),
            "training_rows": int(len(training_rows)) if training_rows is not None else 0,
        }
        self._log_once(
            record, "ready",
            "ML SIDE READY | %s model=%s run=%s side=%s p_yes=%.4f confidence=%.4f "
            "gate=%.4f; watching only this side for <= $0.40.",
            ticker, self.model_metadata.get("model_type", "unknown"), self.model_run_id or "unknown",
            side.upper(), probability_yes, confidence, self.min_confidence,
        )
        return side

    def _harvest_completion(self, ml: Any) -> None:
        """Mark whether each completed task used a causal frozen snapshot."""
        for ticker, task in self.tasks.items():
            if ticker in self.completed_preopen or not task.done():
                continue
            try:
                open_at = ml.next_open_timestamp(ticker)
                frozen_at = self.task_as_of.get(ticker)
                if frozen_at is None:
                    self.completed_preopen[ticker] = False
                    continue
                frozen_at = ml.pd.Timestamp(frozen_at)
                frozen_at = frozen_at.tz_localize("UTC") if frozen_at.tzinfo is None else frozen_at.tz_convert("UTC")
                self.completed_preopen[ticker] = bool(frozen_at <= open_at)
            except Exception:  # noqa: BLE001
                self.completed_preopen[ticker] = False

    def _log_once(self, record: dict[str, Any], status: str, message: str, *args: Any) -> None:
        if self.logged_status.get(str(record.get("ticker") or "")) == status:
            return
        self.logged_status[str(record.get("ticker") or "")] = status
        record["ml_inference_status"] = status
        LOG.info(message, *args)

    async def close(self) -> None:
        for task in self.tasks.values():
            if not task.done():
                task.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks.values(), return_exceptions=True)


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

    async def position_for_ticker(self, ticker: str) -> float | None:
        """Return the signed live Kalshi position for one market.

        Kalshi represents a long YES position as positive contracts and a
        long NO position as negative contracts.  ``None`` means the portfolio
        endpoint could not be read; zero means the endpoint was read and no
        position exists for the ticker.  This method never creates, changes,
        or cancels an order.
        """
        try:
            response = await self.portfolio.get_positions(limit=200)
            for position in field(response, "market_positions", "positions") or []:
                if str(field(position, "ticker") or "") != ticker:
                    continue
                raw_position = as_float(field(position, "position_fp", "position"))
                return round(raw_position or 0.0, 2)
            return 0.0
        except Exception as exc:  # noqa: BLE001
            LOG.warning("Position lookup failed for %s: %s", ticker, exc)
            return None

    async def cancel_resting_mechanical_orders(self) -> int:
        """Cancel only this strategy's resting KXBTC15M orders for a handoff."""
        try:
            response = await self.orders.get_orders(status="resting", limit=1000)
        except Exception as exc:  # noqa: BLE001
            LOG.error("HANDOFF ORDER LOOKUP FAILED | %s", exc)
            raise
        canceled = 0
        for order in field(response, "orders") or []:
            role = managed_mechanical_order_role(order)
            order_id = str(field(order, "order_id") or "")
            if role is None or not order_id:
                continue
            ticker = str(field(order, "ticker") or "?")
            try:
                await self.orders.cancel_order_v2(order_id)
                canceled += 1
                LOG.warning(
                    "HANDOFF CANCELED | %s %s role=%s id=%s",
                    ticker, role[0].upper(), role[1], order_id,
                )
            except Exception as exc:  # noqa: BLE001
                LOG.error("HANDOFF CANCEL FAILED | %s id=%s: %s", ticker, order_id, exc)
                raise
        return canceled

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
            "expected_outcome_side": side,
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
        observed_side = exchange_outcome_side(response)
        if observed_side is not None:
            record["observed_outcome_side"] = observed_side
            record["direction_verified"] = observed_side == side
        # An IOC that gets no execution is returned with zero remaining
        # quantity because the exchange cancelled the remainder.  Zero
        # remaining is therefore *not* evidence of a fill.  Only call an
        # order filled when the reported fill quantity covers the request.
        record["status"] = classify_submission(
            record["fill_count"], record["remaining_count"], quantity, tif,
        )
        LOG.info(
            "ORDER DIRECTION | %s expected_outcome=%s economic_price=$%.4f response_outcome=%s",
            ticker, side.upper(), position_price, observed_side or "not_returned",
        )
        if observed_side is not None and observed_side != side:
            record["status"] = "direction_mismatch"
            LOG.critical(
                "DIRECTION MISMATCH | %s expected=%s exchange_returned=%s; no additional ladder orders will be placed.",
                ticker, side.upper(), observed_side.upper(),
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
            prior_observed_side = record.get("observed_outcome_side")
            observed_side = exchange_outcome_side(order)
            if observed_side is not None:
                record["observed_outcome_side"] = observed_side
                record["direction_verified"] = observed_side == record.get("side")
                if not record["direction_verified"]:
                    record["status"] = "direction_mismatch"
                    LOG.critical(
                        "DIRECTION MISMATCH | %s expected=%s exchange_returned=%s; order is quarantined.",
                        order_id, str(record.get("side") or "?").upper(), observed_side.upper(),
                    )
                elif prior_observed_side != observed_side:
                    LOG.info(
                        "DIRECTION VERIFIED | %s long=%s economic_price=$%.4f",
                        order_id, observed_side.upper(), float(record.get("position_price") or 0.0),
                    )
            record["last_checked_at"] = now_iso()
        except Exception as exc:  # noqa: BLE001
            # A just-executed IOC can briefly be absent from the active-order
            # endpoint before it appears in historical orders. The write
            # response already confirmed its fill, so this is not a rejection.
            if (getattr(exc, "status", None) == 404
                    and float(record.get("fill_count") or 0.0) > 0.004
                    and float(record.get("remaining_count") or 0.0) <= 0.004):
                LOG.info("ORDER LOOKUP PENDING HISTORY | %s; accepted fill is retained.", order_id)
                return
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


FINAL_RECORD_STATUSES = {"finalized", "finalized_unfilled", "finalized_no_signal"}


def start_market_watcher(state: dict[str, Any], market: Any, config: dict[str, Any]) -> dict[str, Any] | None:
    """Persist one whole-market watcher as soon as a new market is observed.

    A saved watcher is deliberately resumed by the next Actions handoff.  A
    ticker first discovered after its opening grace is ignored rather than
    becoming a late fresh entry.
    """
    ticker = str(field(market, "ticker") or "")
    if not ticker:
        return None
    existing = state.get("markets", {}).get(ticker)
    if isinstance(existing, dict):
        return existing if existing.get("status") == "watching" else None
    if not market_can_start_watcher(market, config["watch_start_grace_seconds"]):
        return None
    record = market_record(state, ticker)
    record.update({
        "status": "watching",
        "market_open_time": field(market, "open_time"),
        "market_close_time": field(market, "close_time", "expected_expiration_time"),
        "watch_started_at": now_iso(),
    })
    LOG.info(
        "WATCH STARTED | %s awaiting its frozen ML side, then watching only that side until its executable ask reaches $0.40 or lower.",
        ticker,
    )
    return record


def orders_for_market(record: dict[str, Any]) -> list[dict[str, Any]]:
    return [order for order in (record.get("orders") or {}).values() if isinstance(order, dict)]


def filled_contracts(record: dict[str, Any]) -> float:
    return round(sum(float(order.get("fill_count") or 0.0) for order in orders_for_market(record)), 2)


async def refresh_exchange_position(rest: KalshiREST, record: dict[str, Any]) -> float | None:
    """Read and persist the exchange's signed position without changing it."""
    lookup = getattr(rest, "position_for_ticker", None)
    # Lightweight test doubles predate this safety check.  Real KalshiREST
    # always implements it; preserving this fallback keeps decision-unit tests
    # focused on the mechanical ladder itself.
    if not callable(lookup):
        return 0.0
    position = await lookup(str(record.get("ticker") or ""))
    record["exchange_position_checked_at"] = now_iso()
    if position is None:
        record["exchange_position_status"] = "unavailable"
        return None
    record["exchange_position_status"] = "ok"
    record["exchange_position_contracts"] = round(position, 2)
    record["exchange_position_side"] = "yes" if position > 0.004 else ("no" if position < -0.004 else "flat")
    return position


async def exchange_position_guard(rest: KalshiREST, record: dict[str, Any], config: dict[str, Any]) -> bool:
    """Fail closed if Kalshi's account position cannot match this one-rung ledger.

    The ledger alone prevents this process from submitting more than four
    contracts.  This independent exchange check additionally prevents it from
    adding any order to a ticker with an unexpected, externally created, or
    over-cap position.
    """
    if record.get("exchange_position_guard_blocked"):
        return False
    position = await refresh_exchange_position(rest, record)
    ticker = str(record.get("ticker") or "?")
    if position is None:
        record["exchange_position_guard_blocked"] = "position lookup unavailable"
        LOG.critical("EXCHANGE POSITION GUARD | %s lookup unavailable; refusing new orders for this ticker.", ticker)
        return False
    abs_position = abs(position)
    expected_side = record.get("locked_side") or record.get("candidate_side")
    actual_side = "yes" if position > 0.004 else ("no" if position < -0.004 else None)
    recorded_fills = filled_contracts(record)
    reason: str | None = None
    if abs_position > config["max_contracts_per_market"] + 0.004:
        reason = f"exchange position {position:+.2f} exceeds cap {config['max_contracts_per_market']:.2f}"
    elif actual_side is not None and expected_side is not None and actual_side != expected_side:
        reason = f"exchange side {actual_side.upper()} conflicts with ledger side {str(expected_side).upper()}"
    elif abs_position > recorded_fills + 0.004:
        reason = f"exchange position {position:+.2f} exceeds ledger fills {recorded_fills:.2f}"
    if reason is not None:
        record["exchange_position_guard_blocked"] = reason
        LOG.critical("EXCHANGE POSITION GUARD | %s %s; refusing all further orders for this ticker.", ticker, reason)
        return False
    return True


def active_strategy_records(state: dict[str, Any]) -> list[dict[str, Any]]:
    active: list[dict[str, Any]] = []
    for record in state.get("markets", {}).values():
        if not isinstance(record, dict) or record.get("status") not in {"watching", "initial_submitted", "ladder_active"}:
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
    if isinstance(initial, dict) and initial.get("direction_verified") is False:
        record["status"] = "direction_mismatch"
        LOG.critical("LADDER BLOCKED | %s initial order direction did not match the requested side.", record["ticker"])
        changed = True
    elif isinstance(initial, dict) and float(initial.get("fill_count") or 0.0) > 0.004 and not record.get("locked_side"):
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
    if not await exchange_position_guard(rest, record, config):
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
    prior_status = record.get("status")
    record["status"] = "closed_waiting_finalization"
    record["closed_at"] = now_iso()
    if not record.get("candidate_side") and not orders_for_market(record):
        result = market_result(market)
        status = str(field(market, "status") or "").lower()
        if result is None or status != "finalized":
            if prior_status != "closed_waiting_finalization":
                LOG.info("WATCH CLOSED | %s no side reached $0.40; awaiting final market status.", record["ticker"])
            return
        record.update({
            "status": "finalized_no_signal", "settled_at": now_iso(), "settlement_outcome": result,
            "contracts": 0.0, "total_cost": 0.0, "average_entry": None,
            "gross_payout": 0.0, "gross_profit_loss": 0.0, "kalshi_fees": 0.0,
            "net_profit_loss": 0.0, "return_percentage": None,
        })
        LOG.info("WATCH COMPLETE | %s settled %s with no 40c trigger; no order submitted.",
                 record["ticker"], result.upper())
        return
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
    live_asks: dict[str, float | None] | None = None, ml_side: str | None = None,
) -> bool:
    ticker = str(field(market, "ticker") or "")
    if not ticker or not market_is_tradeable(market):
        return False
    record = state.get("markets", {}).get(ticker)
    if not isinstance(record, dict):
        record = start_market_watcher(state, market, config)
    if not isinstance(record, dict) or record.get("status") != "watching":
        return False
    other_active = [candidate for candidate in active_strategy_records(state) if candidate is not record]
    if len(other_active) >= config["max_active_markets"]:
        return False
    if ml_side not in {"yes", "no"}:
        return False
    asks = market_asks(market, live_asks)
    ask = asks[ml_side]
    if ask is None or ask > LADDER_LEVELS[0] + 1e-9:
        return False
    side = ml_side
    quantity = config["initial_position_size"]
    reserve = ladder_principal(quantity)
    if reserved_principal(state) + reserve > config["max_total_capital"] + 1e-9:
        LOG.warning("SKIP CAPITAL | %s reserve=$%.2f cap=$%.2f", ticker, reserve, config["max_total_capital"])
        return False
    balance = await rest.balance_dollars()
    if balance is None or balance + 1e-9 < reserve + config["fee_reserve"]:
        LOG.warning("SKIP BALANCE | %s need >= $%.2f including fee reserve; available=%s", ticker, reserve + config["fee_reserve"], balance)
        return False
    record.update({
        "candidate_side": side, "quantity": quantity, "status": "initial_submitted",
        "initial_ask": round(ask, 4), "initial_reason": f"ML selected {side.upper()}; its ask reached <= $0.40",
        "reserved_principal": reserve, "market_close_time": field(market, "close_time"),
    })
    if not await exchange_position_guard(rest, record, config):
        record["status"] = "initial_blocked_exchange_position"
        LOG.critical("INITIAL ENTRY BLOCKED | %s no order submitted because the live exchange position is unsafe.", ticker)
        return False
    # Lock the first qualifying side with a resting, market-close-expiring
    # limit.  An IOC can miss a transient quote and abandon the entire market;
    # an unbounded market order could buy above the mechanical 40c ceiling.
    # This GTC order preserves both requirements: the selected side cannot
    # switch, and its executable cost can never exceed the observed <=40c ask.
    price = min(ask, LADDER_LEVELS[0])
    record["orders"]["0.4000"] = await rest.create_order(
        ticker=ticker, side=side, position_price=price, quantity=quantity, tif="good_till_canceled",
        expiration_time=expiration_epoch(market), dry_run=dry_run,
        order_key="initial",
    )
    record["orders"]["0.4000"]["ladder_level"] = 0.40
    record["orders"]["0.4000"]["reason"] = (
        "First qualifying executable ask; resting same-side limit through market close."
    )
    ml_signal = record.get("ml_inference") if isinstance(record.get("ml_inference"), dict) else {}
    LOG.info(
        "WATCH TRIGGERED | %s %s selected p_yes=%s confidence=%s; stopped watching the other side.",
        ticker, side.upper(),
        "unknown" if ml_signal.get("probability_yes") is None else f"{float(ml_signal['probability_yes']):.4f}",
        "unknown" if ml_signal.get("confidence") is None else f"{float(ml_signal['confidence']):.4f}",
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


def rung_order_activity(state: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Count every submitted rung, including unfilled and still-resting orders."""
    stats = {
        f"{level:.2f}": {
            "rung_price": level, "submitted_orders": 0, "submitted_contracts": 0.0,
            "filled_order_submissions": 0, "filled_contracts": 0.0,
            "resting_orders": 0, "canceled_unfilled_orders": 0,
        }
        for level in LADDER_LEVELS
    }
    for record in state.get("markets", {}).values():
        if not isinstance(record, dict):
            continue
        for level in LADDER_LEVELS:
            order = (record.get("orders") or {}).get(f"{level:.4f}")
            if not isinstance(order, dict):
                continue
            result = stats[f"{level:.2f}"]
            quantity = float(order.get("quantity") or 0.0)
            fill = float(order.get("fill_count") or 0.0)
            remaining = float(order.get("remaining_count") or 0.0)
            result["submitted_orders"] += 1
            result["submitted_contracts"] += quantity
            if fill > 0.004:
                result["filled_order_submissions"] += 1
                result["filled_contracts"] += fill
            elif remaining > 0.004:
                result["resting_orders"] += 1
            else:
                result["canceled_unfilled_orders"] += 1
    for result in stats.values():
        result["submitted_contracts"] = round(result["submitted_contracts"], 2)
        result["filled_contracts"] = round(result["filled_contracts"], 2)
    return stats


def ml_live_directional_performance(settled: list[dict[str, Any]]) -> dict[str, Any]:
    """Summarize realized outcomes for filled markets with ML provenance.

    Directional correctness is intentionally kept separate from executable
    P&L, which remains the main report's source for prices, fills, and fees.
    """
    records = [
        record for record in settled
        if isinstance(record.get("ml_inference"), dict)
        and str(record.get("settlement_outcome") or "") in {"yes", "no"}
        and str(record.get("locked_side") or record.get("candidate_side") or "") in {"yes", "no"}
    ]
    wins = sum(
        str(record.get("locked_side") or record.get("candidate_side")) == str(record.get("settlement_outcome"))
        for record in records
    )
    confidences = [as_float((record.get("ml_inference") or {}).get("confidence")) for record in records]
    probabilities = [as_float((record.get("ml_inference") or {}).get("probability_yes")) for record in records]
    confidence_values = [value for value in confidences if value is not None]
    probability_values = [value for value in probabilities if value is not None]
    return {
        "settled_markets": len(records),
        "directional_wins": wins,
        "directional_losses": len(records) - wins,
        "directional_win_rate": round(wins / len(records), 6) if records else None,
        "average_model_confidence": round(sum(confidence_values) / len(confidence_values), 6) if confidence_values else None,
        "average_probability_yes": round(sum(probability_values) / len(probability_values), 6) if probability_values else None,
    }


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
        "generated_at": now_iso(), "strategy": "ml_side_mechanical_price_average_down_v1",
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
        "rung_order_activity": rung_order_activity(state),
        "ml_live_directional_performance": ml_live_directional_performance(settled),
        "note": "Only finalized records with filled contracts count as trades. The pre-open ML side is an execution filter; this report is realized live-ledger data, not a profitability proof.",
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
    for level, activity in report["rung_order_activity"].items():
        LOG.info(
            "RUNG ACTIVITY | %sc sent_orders=%d sent_contracts=%.2f filled_orders=%d "
            "filled_contracts=%.2f resting=%d canceled_unfilled=%d",
            level, activity["submitted_orders"], activity["submitted_contracts"],
            activity["filled_order_submissions"], activity["filled_contracts"],
            activity["resting_orders"], activity["canceled_unfilled_orders"],
        )
    model = report["ml_live_directional_performance"]
    LOG.info(
        "ML LIVE PERFORMANCE | %s settled_with_ml=%d directional_wins=%d directional_losses=%d "
        "directional_win_rate=%s avg_confidence=%s avg_p_yes=%s | P&L above includes fills and fees.",
        context, model["settled_markets"], model["directional_wins"], model["directional_losses"],
        "n/a" if model["directional_win_rate"] is None else f"{100 * model['directional_win_rate']:.2f}%",
        "n/a" if model["average_model_confidence"] is None else f"{model['average_model_confidence']:.4f}",
        "n/a" if model["average_probability_yes"] is None else f"{model['average_probability_yes']:.4f}",
    )


def log_ml_deployment(
    metadata: dict[str, Any], validation: dict[str, Any], model_run_id: str, training_run_id: str,
    execution_gate: float,
) -> None:
    """Print immutable model provenance and validation context once per run."""
    LOG.info(
        "ML MODEL | model_run=%s training_ledger_run=%s type=%s calibration=%s trained_at=%s "
        "settlement_cutoff=%s training_rows=%s base_rows=%s calibration_rows=%s source_sha256=%s",
        model_run_id or "unknown", training_run_id or "unknown", metadata.get("model_type", "unknown"),
        metadata.get("calibration", "raw"), metadata.get("trained_at", "unknown"),
        metadata.get("settlement_cutoff", "unknown"), metadata.get("training_rows", "unknown"),
        metadata.get("base_training_rows", metadata.get("training_rows", "unknown")),
        metadata.get("calibration_rows", 0), metadata.get("source_sha256", "unknown"),
    )
    metrics = field(validation.get("selected_model_untouched_test"), "selected_metrics") or {}
    research_threshold = as_float(validation.get("selected_threshold"))
    LOG.info(
        "ML VALIDATION | research_selected=%s research_gate=%s final_test=%s/%s=%.2f%% "
        "streaks=W%s/L%s p_value=%s | deployed_confidence_gate=%.2f.",
        validation.get("selected_model", "unknown"),
        "unknown" if research_threshold is None else f"{research_threshold:.3f}",
        metrics.get("wins", "?"), metrics.get("trades", "?"), 100.0 * float(metrics.get("win_rate") or 0.0),
        field(metrics.get("streaks") or {}, "longest_win") or "?",
        field(metrics.get("streaks") or {}, "longest_loss") or "?",
        metrics.get("win_rate_vs_50pct_pvalue", "unknown"), execution_gate,
    )
    LOG.info(
        "ML EXECUTION POLICY | model gate >= %.2f (100%% valid-model direction coverage at 0.50); "
        "actual orders still require the selected side's executable ask <= $0.40 and then use only lower same-side rungs.",
        execution_gate,
    )


def preflight_ml_deployment(model_path: Path, metadata: dict[str, Any]) -> None:
    """Reject an unusable ML artifact before the live loop starts.

    Model loading previously first happened during the next market's pre-open
    task.  That delayed artifact compatibility failures until the bot was
    already running and left every watcher without an eligible ML side.
    """
    if metadata.get("feature_schema") != FEATURE_SCHEMA:
        raise ValueError("ML metadata does not declare the required ML-only feature schema")
    if metadata.get("feature_columns") != ML_ONLY_FEATURE_COLUMNS:
        raise ValueError("ML metadata feature columns do not match the live ML-only schema")
    import kalshi_ml_inference_live as ml_inference

    model = ml_inference.load_saved_model(model_path)
    if not hasattr(model, "predict_proba"):
        raise ValueError("Stored ML model has no probability inference method")
    LOG.info(
        "ML PREFLIGHT OK | stored_model=%s schema=%s type=%s calibration=%s rows=%s",
        model_path, FEATURE_SCHEMA, metadata.get("model_type", "unknown"),
        metadata.get("calibration", "unknown"), metadata.get("training_rows", "unknown"),
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
    active_records = active_strategy_records(state)
    watching_count = sum(record.get("status") == "watching" for record in active_records)
    active_ladders = sum(record.get("status") in {"initial_submitted", "ladder_active"} for record in active_records)
    LOG.info(
        "HEARTBEAT | mode=%s elapsed=%.0fs quotes=%d tracked=%d watching=%d active_ladders=%d "
        "reserved=$%.4f cap=$%.4f balance=%s stream=%s stream_messages=%d fallback_check=%.1fs",
        "DRY_RUN" if dry_run else "LIVE", elapsed_seconds, len(active_markets),
        len(state.get("markets", {})), watching_count, active_ladders,
        reserved_principal(state), config["max_total_capital"],
        "unknown" if balance is None else f"${balance:.4f}",
        "connected" if feed and feed.connected else "fallback",
        0 if feed is None else feed.message_count, config["poll_seconds"],
    )
    for market in active_markets:
        ticker = str(field(market, "ticker") or "")
        live_asks = feed.executable_asks(ticker) if feed else None
        asks = market_asks(market, live_asks)
        record = state.get("markets", {}).get(ticker)
        watch_state = str(record.get("status")) if isinstance(record, dict) else "not_started"
        ml_signal = record.get("ml_inference") if isinstance(record, dict) and isinstance(record.get("ml_inference"), dict) else {}
        ml_side = str(ml_signal.get("side") or "").lower()
        selected_ask = asks.get(ml_side) if ml_side in {"yes", "no"} else None
        trigger = (
            f"ML_{ml_side.upper()} @ ${selected_ask:.4f}"
            if selected_ask is not None and selected_ask <= LADDER_LEVELS[0] else
            ("awaiting_ml" if watch_state == "watching" and not ml_side else "none")
        )
        LOG.info(
            "QUOTE | %s source=%s yes_ask=%s no_ask=%s watcher=%s ml_side=%s trigger=%s close=%s",
            ticker or "?", "WS" if live_asks is not None else "REST_FALLBACK",
            "none" if asks["yes"] is None else f"${asks['yes']:.4f}",
            "none" if asks["no"] is None else f"${asks['no']:.4f}",
            watch_state, ml_side.upper() or "none", trigger,
            field(market, "close_time", "expected_expiration_time") or "unknown",
        )
    for record in state.get("markets", {}).values():
        if not isinstance(record, dict) or record.get("status") in FINAL_RECORD_STATUSES:
            continue
        if record.get("status") == "watching":
            ml_signal = record.get("ml_inference") if isinstance(record.get("ml_inference"), dict) else {}
            ml_side = str(ml_signal.get("side") or "").upper()
            LOG.info(
                "WATCH | %s active; %s.", record.get("ticker", "?"),
                (f"ML selected {ml_side} p_yes={float(ml_signal.get('probability_yes')):.4f} "
                 f"confidence={float(ml_signal.get('confidence')):.4f}; only its ask can trigger at <= $0.40" if ml_side
                 else "awaiting frozen ML direction; no order can be placed"),
            )
            continue
        exchange_position = await refresh_exchange_position(rest, record)
        LOG.info(
            "POSITION | %s status=%s side=%s ledger_filled=%.2f submitted=%.2f exchange=%s guard=%s",
            record.get("ticker", "?"), record.get("status", "?"),
            str(record.get("locked_side") or record.get("candidate_side") or "none").upper(),
            filled_contracts(record),
            sum(float(order.get("quantity") or 0.0) for order in orders_for_market(record)),
            "unavailable" if exchange_position is None else f"{exchange_position:+.2f}",
            record.get("exchange_position_guard_blocked") or "clear",
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


async def cancel_open_mechanical_orders(rest: KalshiREST, state: dict[str, Any], dry_run: bool) -> int:
    """Cancel only the known unfilled rungs in the persisted mechanical ledger."""
    canceled = 0
    for record in state.get("markets", {}).values():
        if not isinstance(record, dict) or record.get("status") in FINAL_RECORD_STATUSES:
            continue
        for order in orders_for_market(record):
            if float(order.get("remaining_count") or 0.0) <= 0.004:
                continue
            await rest.cancel_order(order, dry_run)
            canceled += 1
            LOG.info(
                "EMERGENCY CANCEL | %s %s @ $%.4f id=%s",
                record.get("ticker", "?"), str(order.get("side") or "?").upper(),
                float(order.get("position_price") or 0.0), order.get("order_id") or "?",
            )
    return canceled


async def run(args: argparse.Namespace) -> int:
    config_path = args.config.expanduser()
    config = validate_config(load_json(config_path, DEFAULT_CONFIG))
    config, config_changed = apply_config_overrides(config, args)
    if args.persist_config or config_changed:
        save_json(config_path, config)
    dry_run = os.getenv("DRY_RUN", "true").lower() in {"1", "true", "yes"}
    live_allowed = not dry_run and args.submit and args.allow_live
    control_only = args.cancel_open_orders or args.cancel_all_resting_mechanical_orders
    if not dry_run and not live_allowed and not control_only:
        raise SystemExit("Refusing live orders: pass both --submit and --allow-live with DRY_RUN=false")
    state_path = args.state_file.expanduser()
    state = load_json(state_path, default_state())
    state["format_version"] = STATE_VERSION
    state.setdefault("markets", {})
    api_key = os.getenv("KALSHI_API_KEY_ID", "")
    pem_path = Path(os.getenv("KALSHI_PEM_PATH", "kalshi_private_key.pem"))
    if not api_key or not pem_path.exists():
        raise SystemExit("KALSHI_API_KEY_ID and KALSHI_PEM_PATH are required")
    rest = KalshiREST(api_key, pem_path, os.getenv("KALSHI_DEMO", "false").lower() in {"1", "true", "yes"})
    if control_only:
        try:
            canceled = (
                await rest.cancel_resting_mechanical_orders()
                if args.cancel_all_resting_mechanical_orders
                else await cancel_open_mechanical_orders(rest, state, dry_run)
            )
            save_json(state_path, state)
            save_json(args.report.expanduser(), performance_report(state, config))
            LOG.warning("CANCEL-ONLY COMPLETE | canceled_open_mechanical_orders=%d", canceled)
            return 0
        finally:
            await rest.close()
    if args.ml_training_csv is None or args.ml_model_path is None:
        await rest.close()
        raise SystemExit("ML-side execution requires --ml-training-csv and --ml-model-path; refusing a price-only fallback")
    model_metadata = load_json(args.ml_model_metadata.expanduser(), {}) if args.ml_model_metadata else {}
    validation_report = load_json(args.ml_validation_report.expanduser(), {}) if args.ml_validation_report else {}
    try:
        preflight_ml_deployment(args.ml_model_path.expanduser(), model_metadata)
        ml_selector = MLDirectionSelector(
            args.ml_training_csv.expanduser(), args.ml_model_path.expanduser(), config["ml_preopen_lead_seconds"],
            config["ml_min_confidence"], model_metadata, args.ml_model_run_id, args.ml_training_run_id,
        )
    except Exception:
        await rest.close()
        raise
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
        "STARTUP | mode=%s run_seconds=%.0f quantity_per_rung=%.2f market_contract_cap=%.2f ladder=%s capital_cap=$%.4f",
        "DRY_RUN" if dry_run else "LIVE", args.run_seconds, config["initial_position_size"],
        config["max_contracts_per_market"], "/".join(f"${level:.2f}" for level in LADDER_LEVELS),
        config["max_total_capital"],
    )
    LOG.info(
        "ML SIDE FILTER | stored_model=%s feature_ledger=%s preopen_lead=%.0fs confidence_gate=%.2f fallback=disabled",
        args.ml_model_path, args.ml_training_csv, config["ml_preopen_lead_seconds"], config["ml_min_confidence"],
    )
    log_ml_deployment(
        model_metadata, validation_report, args.ml_model_run_id, args.ml_training_run_id, config["ml_min_confidence"],
    )
    log_performance_summary(performance_report(state, config), "startup")
    try:
        while True:
            monotonic_now = asyncio.get_running_loop().time()
            await ml_selector.maybe_prepare_next()
            # Discover the current KXBTC15M window periodically.  Once known,
            # the authenticated ticker stream—not REST polling—is the entry
            # trigger for every quote change.
            if monotonic_now - last_market_refresh_at >= config["market_refresh_seconds"]:
                active_markets = await rest.active_markets()
                last_market_refresh_at = monotonic_now

            tracked_tickers = [
                str(ticker) for ticker, record in state["markets"].items()
                if isinstance(record, dict) and record.get("status") not in FINAL_RECORD_STATUSES
            ]
            feed.set_tickers(tracked_tickers + [str(field(market, "ticker") or "") for market in active_markets])

            # A private fill/order update gets an immediate authoritative REST
            # reconciliation.  The interval is only a recovery path when a
            # stream message was missed or the connection was interrupted.
            private_update = feed.private_update_count != last_private_update_count
            if private_update or monotonic_now - last_order_reconcile_at >= config["order_reconcile_seconds"]:
                for ticker, record in list(state["markets"].items()):
                    if not isinstance(record, dict) or record.get("status") in FINAL_RECORD_STATUSES:
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
                record = state.get("markets", {}).get(ticker)
                if not isinstance(record, dict):
                    record = start_market_watcher(state, market, config)
                if not isinstance(record, dict) or record.get("status") != "watching":
                    continue
                ml_side = await ml_selector.side_for_market(market, record)
                await consider_initial_entry(
                    rest, state, market, config, dry_run,
                    live_asks=feed.executable_asks(ticker), ml_side=ml_side,
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
        await ml_selector.close()
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
    parser.add_argument("--cancel-open-orders", action="store_true")
    parser.add_argument(
        "--cancel-all-resting-mechanical-orders", action="store_true",
        help="Cancel only resting orders with this runner's deterministic KXBTC15M client IDs; used for controlled handoff.",
    )
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
    parser.add_argument("--watch-start-grace-seconds", type=float)
    parser.add_argument("--ml-preopen-lead-seconds", type=float)
    parser.add_argument("--ml-min-confidence", type=float)
    parser.add_argument("--ml-training-csv", type=Path, help="Frozen historical feature ledger for the stored ML model.")
    parser.add_argument("--ml-model-path", type=Path, help="Stored ML joblib model used to choose the only eligible side.")
    parser.add_argument("--ml-model-metadata", type=Path, help="Optional JSON metadata paired with the stored ML model.")
    parser.add_argument("--ml-validation-report", type=Path, help="Optional immutable validation report for startup audit logging.")
    parser.add_argument("--ml-model-run-id", default="", help="Actions run ID that produced the exact stored model artifact.")
    parser.add_argument("--ml-training-run-id", default="", help="Actions run ID that produced the feature ledger artifact.")
    parser.add_argument("--status-log-seconds", type=float)
    return parser


if __name__ == "__main__":
    configure_logging()
    raise SystemExit(asyncio.run(run(build_parser().parse_args())))
