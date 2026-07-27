"""Persisted, fail-closed Kalshi shadow-equity regime controller.

This module deliberately owns accounting, forecasting, and the execution gate,
but not the trading strategy.  The live runner creates an immutable
``StrategyDecision`` from its existing settlement-contrarian ladder and feeds
it to this controller.  The same decision is then used for shadow simulation.

Nothing in this module sends or cancels an exchange order.  A caller must opt
in separately before it can use ``execution_enabled_for_market`` to suppress
new live entries.  This makes the default behaviour observational/dry-run.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import math
import os
import random
import re
import tempfile
import time
from collections import defaultdict
from dataclasses import asdict, dataclass, field, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Awaitable, Callable, Iterable, Mapping, Protocol


LOG = logging.getLogger(__name__)
ZERO = Decimal("0")
ONE = Decimal("1")
CENT = Decimal("0.01")
ACCOUNTING_TOLERANCE = Decimal("0.01")
QUANTILES = ("p01", "p10", "p25", "p50", "p75", "p90", "p99")
QUANTILE_PROBABILITIES = (0.01, 0.10, 0.25, 0.50, 0.75, 0.90, 0.99)
DEFAULT_CLIENT_ORDER_PREFIX = "settlement-contrarian-"
KALSHI_TICKER_TIMESTAMP_RE = re.compile(
    r"KXBTC15M-(\d{2})([A-Z]{3})(\d{2})(\d{2})(\d{2})-(\d{2})"
)


def decimal_value(value: Any, default: Decimal = ZERO) -> Decimal:
    """Convert API fixed-point-ish values without ever using binary math.

    Kalshi responses vary between Decimal-compatible strings and numeric JSON
    values.  Converting through ``str`` preserves the supplied decimal text.
    Invalid/missing optional values are deliberately a caller-controlled zero.
    """

    if value is None or value == "":
        return default
    try:
        result = Decimal(str(value).replace("$", "").replace(",", "").strip())
    except (InvalidOperation, ValueError):
        return default
    return result if result.is_finite() else default


def money_text(value: Decimal) -> str:
    return format(value.quantize(CENT, rounding=ROUND_HALF_UP), ".2f")


def as_json(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Cannot serialize {type(value).__name__}")


def value_from(record: Any, *names: str) -> Any:
    for name in names:
        if isinstance(record, Mapping) and record.get(name) is not None:
            return record[name]
        candidate = getattr(record, name, None)
        if candidate is not None:
            return candidate
    return None


def utc_timestamp(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, (int, float, Decimal)):
        number = float(value)
        if number > 10_000_000_000:
            number /= 1000
        try:
            return datetime.fromtimestamp(number, tz=UTC)
        except (OverflowError, OSError, ValueError):
            return None
    text = str(value).strip()
    if text.isdigit():
        return utc_timestamp(int(text))
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def timestamp_text(value: Any) -> str:
    parsed = utc_timestamp(value)
    return parsed.isoformat() if parsed is not None else ""


def raw_record_text(record: Mapping[str, Any]) -> str:
    """Preserve a raw API record in a CSV cell for read-only reconciliation.

    The artifacts contain account-order metadata but no API credentials.  A
    normalized field can be ambiguous (for example, a Kalshi fill's action
    versus a maker/taker action), so the exact response is retained solely to
    audit accounting discrepancies.
    """

    return json.dumps(dict(record), default=as_json, sort_keys=True, separators=(",", ":"))


def ticker_clock_timestamp(ticker: Any, fallback: Any = None) -> datetime | None:
    """Return the ticker clock time used by the supplied Colab notebooks.

    The reference curve's ``ds`` comes from the KXBTC15M ticker rather than
    the API's settlement/last-updated time.  The latter can differ by hours,
    which changes intraday seasonality and prevents an otherwise identical
    Prophet configuration from matching the notebook.
    """

    matched = KALSHI_TICKER_TIMESTAMP_RE.search(str(ticker or ""))
    if matched:
        year, month, day, hour, minute, second = matched.groups()
        try:
            return datetime.strptime(
                f"{year}{month}{day}{hour}{minute}{second}", "%y%b%d%H%M%S",
            ).replace(tzinfo=UTC)
        except ValueError:
            pass
    return utc_timestamp(fallback)


def observation_timestamp(row: Mapping[str, Any]) -> datetime | None:
    """Use the reference ticker time, falling back only for non-Kalshi tests."""

    return ticker_clock_timestamp(row.get("market_ticker"), row.get("market_close_time"))


def bool_value(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class LadderOrder:
    """An immutable existing-strategy limit order expressed economically."""

    price: Decimal
    quantity: Decimal
    order_key: str


@dataclass(frozen=True)
class StrategyDecision:
    """The existing bot's complete causal decision for one target market."""

    target_ticker: str
    source_ticker: str | None
    selected_side: str
    eligible: bool
    skip_reason: str | None
    ladder_orders: tuple[LadderOrder, ...]
    stop_price: Decimal
    trailing_activation_gain: Decimal
    trailing_retracement: Decimal
    generated_at: datetime
    target_close_time: datetime
    strategy_name: str = "settlement_contrarian_weighted_hold_gate_live_v1"

    def __post_init__(self) -> None:
        if self.selected_side not in {"yes", "no"}:
            raise ValueError("selected_side must be yes or no")
        if self.target_close_time <= self.generated_at:
            raise ValueError("target_close_time must be after generated_at")
        if any(order.quantity <= ZERO or not ZERO < order.price < ONE for order in self.ladder_orders):
            raise ValueError("each ladder order must have positive quantity and a price in (0, 1)")


@dataclass(frozen=True)
class RegimeConfig:
    enabled: bool = False
    dry_run: bool = True
    allow_live_state_transitions: bool = False
    subaccount: int = 0
    series_ticker: str = "KXBTC15M"
    # Kept only for historical P/L reconstruction.  It must never be used to
    # rebase a rolling live lookback; live balances come from /portfolio/balance.
    starting_balance: Decimal = Decimal("100.00")
    # Required for a historical P/L reconstruction.  It is deliberately
    # separate from the legacy strategy-size setting above so a default $100
    # can never be mistaken for an authenticated account snapshot.
    historical_starting_balance: Decimal | None = None
    history_start_ts: datetime | None = None
    history_end_ts: datetime | None = None
    history_max_markets: int = 200
    bot_client_order_prefix: str = DEFAULT_CLIENT_ORDER_PREFIX
    bot_order_group_id: str | None = None
    # A shared series ticker is not ownership evidence: a user can trade the
    # same KXBTC15M market manually.  It is disabled by default and exists
    # only for a deliberately documented legacy import.
    allow_series_ticker_ownership_fallback: bool = False
    # ``account_series`` matches the supplied Colab workflow: use every
    # closed account market in the configured series, including manual
    # activity, solely to construct Prophet's balance input.
    prophet_history_source: str = "bot_owned"
    # Use the supplied closed-position export when exact Colab parity is
    # required; its rows include the account's manual and bot positions.
    prophet_reference_closed_positions_path: Path | None = None
    prophet_enabled: bool = True
    prophet_min_history: int = 200
    # A lookback is a row selector, never a P/L rebase.  The production
    # controller uses the latest 200 closed-trade balances plus their opening
    # balance anchor, matching the 201-row Colab Prophet input.
    prophet_training_window: int | None = 201
    prophet_refit_every_markets: int = 75
    # This is a diagnostic horizon only.  The live P10/P90 state machine
    # always consumes the first, actual next-market forecast.
    prophet_future_horizon_markets: int = 100
    prophet_forecast_frequency_minutes: int = 15
    # An authenticated end-balance plus the durable bot ledger can reconstruct
    # the preceding 200 *absolute* balances backwards only when there is no
    # filled open position.  It is opt-in because account adjustments remain
    # a material accounting assumption.
    allow_endpoint_anchored_ledger_bootstrap: bool = False
    prophet_use_log_transform: bool = True
    prophet_uncertainty_samples: int = 2000
    prophet_changepoint_prior_scale: float = 0.05
    prophet_seasonality_prior_scale: float = 10.0
    prophet_daily_seasonality: bool = True
    prophet_weekly_seasonality: bool = False
    prophet_yearly_seasonality: bool = False
    prophet_random_seed: int = 42
    shadow_fill_model: str = "conservative_trade_through"
    shadow_latency_ms: int = 0
    shadow_slippage_cents: Decimal = ZERO
    shadow_partial_fills: bool = True
    shadow_require_trade_through: bool = True
    scaling_equity_source: str = "actual"
    cooldown_state_source: str = "separate"
    accounting_tolerance: Decimal = ACCOUNTING_TOLERANCE

    def __post_init__(self) -> None:
        if self.starting_balance <= ZERO:
            raise ValueError("starting_balance must be positive")
        if self.historical_starting_balance is not None and self.historical_starting_balance <= ZERO:
            raise ValueError("historical_starting_balance must be positive when supplied")
        if self.accounting_tolerance < ZERO:
            raise ValueError("accounting_tolerance cannot be negative")
        if self.prophet_min_history < 2:
            raise ValueError("prophet_min_history must be at least 2")
        if self.prophet_training_window is not None and self.prophet_training_window < 2:
            raise ValueError("prophet_training_window must be None or at least 2")
        if self.prophet_refit_every_markets < 1:
            raise ValueError("prophet_refit_every_markets must be positive")
        if self.prophet_future_horizon_markets < 1:
            raise ValueError("prophet_future_horizon_markets must be positive")
        if self.prophet_forecast_frequency_minutes < 1:
            raise ValueError("prophet_forecast_frequency_minutes must be positive")
        if self.history_max_markets < self.prophet_min_history:
            raise ValueError("history_max_markets must be at least prophet_min_history")
        if self.prophet_history_source not in {"bot_owned", "account_series"}:
            raise ValueError("prophet_history_source must be bot_owned or account_series")
        if self.shadow_fill_model not in {"conservative_trade_through", "touch", "live_equivalent"}:
            raise ValueError("unsupported shadow_fill_model")
        if self.scaling_equity_source not in {"actual", "shadow", "disabled_while_off"}:
            raise ValueError("unsupported scaling_equity_source")
        if self.cooldown_state_source not in {"actual", "shadow", "separate"}:
            raise ValueError("unsupported cooldown_state_source")

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any]) -> "RegimeConfig":
        def pick(name: str, default: Any) -> Any:
            return values.get(name, values.get(name.upper(), default))

        window = pick("prophet_training_window", 201)
        return cls(
            enabled=bool_value(pick("equity_regime_enabled", False)),
            dry_run=bool_value(pick("equity_regime_dry_run", True), True),
            allow_live_state_transitions=bool_value(pick("allow_live_state_transitions", False)),
            subaccount=int(pick("subaccount", 0)),
            series_ticker=str(pick("series_ticker", "KXBTC15M")),
            starting_balance=decimal_value(pick("starting_balance", "100.00")),
            historical_starting_balance=(
                None if pick("historical_starting_balance", None) in {None, "", "None", "none"}
                else decimal_value(pick("historical_starting_balance", None))
            ),
            history_start_ts=utc_timestamp(pick("history_start_ts", None)),
            history_end_ts=utc_timestamp(pick("history_end_ts", None)),
            history_max_markets=int(pick("history_max_markets", 200)),
            bot_client_order_prefix=str(pick("bot_client_order_prefix", DEFAULT_CLIENT_ORDER_PREFIX)),
            bot_order_group_id=(str(pick("bot_order_group_id", "")).strip() or None),
            allow_series_ticker_ownership_fallback=bool_value(
                pick("allow_series_ticker_ownership_fallback", False),
            ),
            prophet_history_source=str(pick("prophet_history_source", "bot_owned")),
            prophet_reference_closed_positions_path=(
                None
                if pick("prophet_reference_closed_positions_path", None) in {None, "", "None", "none"}
                else Path(str(pick("prophet_reference_closed_positions_path", "")))
            ),
            prophet_enabled=bool_value(pick("prophet_enabled", True), True),
            prophet_min_history=int(pick("prophet_min_history", 200)),
            prophet_training_window=None if window in {None, "", "None", "none"} else int(window),
            prophet_refit_every_markets=int(pick("prophet_refit_every_markets", 75)),
            prophet_future_horizon_markets=int(pick("prophet_future_horizon_markets", 100)),
            prophet_forecast_frequency_minutes=int(pick("prophet_forecast_frequency_minutes", 15)),
            allow_endpoint_anchored_ledger_bootstrap=bool_value(
                pick("allow_endpoint_anchored_ledger_bootstrap", False),
            ),
            prophet_use_log_transform=bool_value(pick("prophet_use_log_transform", True), True),
            prophet_uncertainty_samples=int(pick("prophet_uncertainty_samples", 2000)),
            prophet_changepoint_prior_scale=float(pick("prophet_changepoint_prior_scale", 0.05)),
            prophet_seasonality_prior_scale=float(pick("prophet_seasonality_prior_scale", 10.0)),
            prophet_daily_seasonality=bool_value(pick("prophet_daily_seasonality", True), True),
            prophet_weekly_seasonality=bool_value(pick("prophet_weekly_seasonality", False)),
            prophet_yearly_seasonality=bool_value(pick("prophet_yearly_seasonality", False)),
            prophet_random_seed=int(pick("prophet_random_seed", 42)),
            shadow_fill_model=str(pick("shadow_fill_model", "conservative_trade_through")),
            shadow_latency_ms=int(pick("shadow_latency_ms", 0)),
            shadow_slippage_cents=decimal_value(pick("shadow_slippage_cents", "0")),
            shadow_partial_fills=bool_value(pick("shadow_partial_fills", True), True),
            shadow_require_trade_through=bool_value(pick("shadow_require_trade_through", True), True),
            scaling_equity_source=str(pick("scaling_equity_source", "actual")),
            cooldown_state_source=str(pick("cooldown_state_source", "separate")),
            accounting_tolerance=decimal_value(pick("accounting_tolerance", "0.01")),
        )

    @property
    def controls_live_execution(self) -> bool:
        """The only condition under which this layer may suppress new orders."""

        return self.enabled and not self.dry_run and self.allow_live_state_transitions


class AtomicJsonStore:
    """Crash-safe JSON state persistence using fsync + atomic replacement."""

    def __init__(self, path: Path, default_factory: Callable[[], dict[str, Any]]) -> None:
        self.path = path
        self.default_factory = default_factory

    def load(self) -> dict[str, Any]:
        if not self.path.exists():
            return self.default_factory()
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise RuntimeError(f"Cannot safely restore regime state {self.path}: {exc}") from exc
        if not isinstance(payload, dict):
            raise RuntimeError(f"Regime state {self.path} is not a JSON object")
        return payload

    def save(self, payload: Mapping[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        encoded = json.dumps(payload, indent=2, sort_keys=True, default=as_json) + "\n"
        with tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", dir=self.path.parent, prefix=f".{self.path.name}.", delete=False,
        ) as handle:
            temp_name = handle.name
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.replace(temp_name, self.path)
            directory_fd = os.open(self.path.parent, os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            if os.path.exists(temp_name):
                os.unlink(temp_name)


def default_regime_state(config: RegimeConfig) -> dict[str, Any]:
    return {
        "format_version": 3,
        "historical_starting_balance": None,
        "actual_balance": None,
        "shadow_balance": None,
        "balance_source": "uninitialized_requires_authenticated_balance",
        "balance_reconciled": False,
        "balance_reconciliation_error": "not_initialized",
        "prophet_history_ready": False,
        "legacy_migration": None,
        "balance_adjustments": [],
        "execution_enabled": True,
        "shadow_enabled": True,
        "state_reason": "initial_state",
        "state_changed_at": None,
        "disabled_since": None,
        "shadow_pnl_while_disabled": "0",
        "last_processed_fill_id": None,
        "last_processed_settlement_ticker": None,
        "last_processed_market_ticker": None,
        "last_p01": None,
        "last_p10": None,
        "last_p25": None,
        "last_p50": None,
        "last_p75": None,
        "last_p90": None,
        "last_p99": None,
        "forecast_generated_at": None,
        "forecast_target_ticker": None,
        "prophet_training_end": None,
        "prophet_training_rows": 0,
        # The balance immediately before the oldest retained closed trade.
        # It is a model observation (like the $100 row in the Colab curve),
        # not a new market or a rebased P/L index.
        "actual_balance_anchor": None,
        "shadow_balance_anchor": None,
        "markets_since_refit": 0,
        "actual_history": [],
        "shadow_history": [],
        "forecasts": [],
        # Latest 100-row diagnostic path from the same fit as the live
        # one-step forecast.  It is deliberately separate from ``forecasts``
        # so later horizons cannot be mistaken for tradable signals.
        "future_forecast_snapshot": [],
        "transitions": [],
        "live_vs_shadow": [],
        "processed_market_tickers": [],
        "processed_fill_ids": [],
        "processed_settlement_tickers": [],
        "ambiguous_fills": [],
        "fit_failures": [],
        "live_cooldown_state": {},
        "shadow_cooldown_state": {},
        "live_dynamic_base": None,
        "shadow_dynamic_base": None,
    }


@dataclass(frozen=True)
class NormalizedFill:
    fill_id: str
    trade_id: str | None
    order_id: str | None
    client_order_id: str | None
    order_group_id: str | None
    ticker: str
    created_at: datetime | None
    side: str
    action: str
    price: Decimal
    count: Decimal
    fee: Decimal
    subaccount: int | None
    source: str
    raw: Mapping[str, Any] = field(compare=False, repr=False)
    # The portfolio API's canonical direction describes the exchange book,
    # whereas a reduce-only exit can be represented on the reciprocal leg.
    # When an exact local order id is available, these fields carry the
    # strategy's economic position side and cash-flow direction instead.
    economic_side: str | None = field(default=None, compare=False)
    economic_action: str | None = field(default=None, compare=False)
    economic_price: Decimal | None = field(default=None, compare=False)
    order_role: str | None = field(default=None, compare=False)

    @property
    def dedupe_key(self) -> tuple[str, ...]:
        if self.fill_id:
            return ("fill_id", self.fill_id)
        return (
            "fallback", self.trade_id or "", self.order_id or "", self.ticker,
            timestamp_text(self.created_at), format(self.count, "f"), format(self.price, "f"),
        )


@dataclass(frozen=True)
class NormalizedSettlement:
    ticker: str
    settled_at: datetime | None
    result: str | None
    payout: Decimal | None
    yes_count: Decimal | None
    no_count: Decimal | None
    source: str
    raw: Mapping[str, Any] = field(compare=False, repr=False)


def normalize_price(record: Mapping[str, Any], side: str) -> Decimal:
    specific = value_from(
        record,
        "yes_price_dollars" if side == "yes" else "no_price_dollars",
        "yes_price" if side == "yes" else "no_price",
        "price_dollars",
        "price",
    )
    price = decimal_value(specific)
    # Legacy API payloads return cents as an integer in ``price``.  Explicit
    # *_dollars fields are never converted.
    if value_from(record, "price_dollars", "yes_price_dollars", "no_price_dollars") is None and price > ONE:
        price /= Decimal("100")
    if side == "no" and value_from(record, "no_price_dollars", "no_price") is None and value_from(record, "yes_price_dollars", "yes_price") is not None:
        price = ONE - price
    return price


def normalize_fill(record: Mapping[str, Any], source: str) -> NormalizedFill:
    side = str(value_from(record, "side", "market_side", "outcome", "outcome_side", "taker_outcome_side") or "").lower()
    if side not in {"yes", "no"}:
        raise ValueError(f"fill has unsupported side: {side!r}")
    action = str(value_from(record, "action", "trade_action", "taker_action", "type") or "buy").lower()
    if action in {"purchase", "bought"}:
        action = "buy"
    if action in {"sale", "sold"}:
        action = "sell"
    if action not in {"buy", "sell"}:
        raise ValueError(f"fill has unsupported action: {action!r}")
    count = decimal_value(value_from(record, "count_fp", "count", "quantity", "fill_count_fp"))
    if count <= ZERO:
        raise ValueError("fill count must be positive")
    ticker = str(value_from(record, "ticker", "market_ticker") or "")
    if not ticker:
        raise ValueError("fill has no ticker")
    return NormalizedFill(
        fill_id=str(value_from(record, "fill_id", "id") or ""),
        trade_id=(str(value_from(record, "trade_id")) if value_from(record, "trade_id") is not None else None),
        order_id=(str(value_from(record, "order_id")) if value_from(record, "order_id") is not None else None),
        client_order_id=(str(value_from(record, "client_order_id")) if value_from(record, "client_order_id") is not None else None),
        order_group_id=(str(value_from(record, "order_group_id")) if value_from(record, "order_group_id") is not None else None),
        ticker=ticker,
        created_at=utc_timestamp(value_from(record, "created_time", "created_at", "timestamp", "ts")),
        side=side,
        action=action,
        price=normalize_price(record, side),
        count=count,
        fee=decimal_value(value_from(record, "fee_cost", "fee_cost_dollars", "fees_paid_dollars", "fee")),
        subaccount=(
            int(value_from(record, "subaccount", "subaccount_id", "subaccount_number"))
            if value_from(record, "subaccount", "subaccount_id", "subaccount_number") is not None
            else None
        ),
        source=source,
        raw=record,
    )


def normalize_settlement(record: Mapping[str, Any], source: str) -> NormalizedSettlement:
    ticker = str(value_from(record, "ticker", "market_ticker") or "")
    if not ticker:
        raise ValueError("settlement has no ticker")
    result = str(value_from(record, "market_result", "result", "settlement_value") or "").lower() or None
    if result not in {None, "yes", "no"}:
        result = None
    payout_raw = value_from(record, "payout_dollars", "settlement_payout_dollars", "payout")
    return NormalizedSettlement(
        ticker=ticker,
        settled_at=utc_timestamp(value_from(record, "settlement_time", "settled_time", "created_time", "settlement_ts")),
        result=result,
        payout=decimal_value(payout_raw) if payout_raw is not None else None,
        yes_count=(decimal_value(value_from(record, "yes_count_fp", "yes_count")) if value_from(record, "yes_count_fp", "yes_count") is not None else None),
        no_count=(decimal_value(value_from(record, "no_count_fp", "no_count")) if value_from(record, "no_count_fp", "no_count") is not None else None),
        source=source,
        raw=record,
    )


class JsonAPI(Protocol):
    async def get_json(self, path: str, params: Mapping[str, Any] | None = None) -> Mapping[str, Any]: ...


async def paginated_records(
    api: JsonAPI, path: str, *, params: Mapping[str, Any] | None = None, list_keys: tuple[str, ...],
) -> list[Mapping[str, Any]]:
    """Fetch every cursor page without trusting endpoint-specific SDK models."""

    cursor: str | None = None
    rows: list[Mapping[str, Any]] = []
    for _ in range(10_000):
        request = dict(params or {})
        request["limit"] = request.get("limit", 1000)
        if cursor:
            request["cursor"] = cursor
        payload = await api.get_json(path, request)
        batch: Any = []
        for key in list_keys:
            if isinstance(payload.get(key), list):
                batch = payload[key]
                break
        if not isinstance(batch, list):
            raise RuntimeError(f"{path} response does not contain one of {list_keys}")
        rows.extend(row for row in batch if isinstance(row, Mapping))
        next_cursor = value_from(payload, "cursor", "next_cursor")
        if not next_cursor or str(next_cursor) == cursor:
            return rows
        cursor = str(next_cursor)
    raise RuntimeError(f"pagination limit exceeded for {path}")


def cutoff_timestamp(payload: Mapping[str, Any]) -> datetime | None:
    # Kalshi publishes separate cutoffs.  Fills must use ``trades_created_ts``;
    # settlement and order cutoffs are not a safe substitute for the fill
    # boundary.  Older names remain for backward-compatible test fixtures.
    return utc_timestamp(value_from(
        payload,
        "trades_created_ts",
        "historical_cutoff_ts",
        "cutoff_ts",
        "cutoff_time",
        "timestamp",
    ))


def known_bot_identifiers(local_state: Mapping[str, Any]) -> tuple[set[str], set[str]]:
    order_ids: set[str] = set()
    tickers: set[str] = set()
    for ticker, record in (local_state.get("markets") or {}).items():
        if not isinstance(record, Mapping):
            continue
        tickers.add(str(ticker))
        for order in (record.get("orders") or {}).values():
            if isinstance(order, Mapping):
                for key in ("order_id", "client_order_id"):
                    if order.get(key):
                        order_ids.add(str(order[key]))
        for order in record.get("live_exit_orders") or []:
            if isinstance(order, Mapping):
                for key in ("order_id", "client_order_id"):
                    if order.get(key):
                        order_ids.add(str(order[key]))
    return order_ids, tickers


def known_bot_order_metadata(local_state: Mapping[str, Any]) -> dict[str, dict[str, str]]:
    """Map durable local order ids to their economic strategy meaning.

    Kalshi's canonical fill direction is the current book direction.  A
    reduce-only exit of a YES position can therefore arrive as an ASK/NO
    record, which is not an additional NO position.  The existing bot ledger
    persists the intended held side and gives us the only safe way to map that
    fill back to cash-flow accounting.
    """

    metadata: dict[str, dict[str, str]] = {}
    for ticker, record in (local_state.get("markets") or {}).items():
        if not isinstance(record, Mapping):
            continue
        default_side = str(record.get("locked_side") or record.get("candidate_side") or "").lower()
        for order in (record.get("orders") or {}).values():
            if not isinstance(order, Mapping):
                continue
            side = str(order.get("side") or default_side).lower()
            if side not in {"yes", "no"}:
                continue
            for key in ("order_id", "client_order_id"):
                identifier = order.get(key)
                if identifier:
                    metadata[str(identifier)] = {
                        "economic_side": side,
                        "economic_action": "buy",
                        "order_role": "entry",
                        "market_ticker": str(ticker),
                    }
        for order in record.get("live_exit_orders") or []:
            if not isinstance(order, Mapping):
                continue
            side = str(order.get("held_side") or order.get("side") or default_side).lower()
            if side not in {"yes", "no"}:
                continue
            for key in ("order_id", "client_order_id"):
                identifier = order.get(key)
                if identifier:
                    metadata[str(identifier)] = {
                        "economic_side": side,
                        "economic_action": "sell",
                        "order_role": "reduce_only_exit",
                        "market_ticker": str(ticker),
                    }
    return metadata


def price_for_economic_side(record: Mapping[str, Any], side: str) -> Decimal:
    """Get the API's fixed-point price on the bot's economic YES/NO leg."""

    if side not in {"yes", "no"}:
        raise ValueError(f"unsupported economic side: {side!r}")
    return normalize_price(record, side)


def ownership_evidence(fill: NormalizedFill, config: RegimeConfig, local_order_ids: set[str], local_tickers: set[str]) -> str | None:
    identities = {candidate for candidate in (fill.order_id, fill.client_order_id) if candidate}
    if identities & local_order_ids:
        return "local_bot_trade_ledger"
    if fill.client_order_id and fill.client_order_id.startswith(config.bot_client_order_prefix):
        return "client_order_id_prefix"
    if config.bot_order_group_id and fill.order_group_id == config.bot_order_group_id:
        return "order_group_id"
    # A ticker in the local market ledger does *not* prove this particular
    # fill belongs to the bot: manual orders can use the same 15-minute
    # market.  Exact order identifiers are handled above.  The broad series
    # fallback is opt-in only for audited legacy imports and must never be the
    # default for live balance reconstruction.
    if (
        config.allow_series_ticker_ownership_fallback
        and fill.ticker.startswith(config.series_ticker + "-")
        and (fill.subaccount is None or fill.subaccount == config.subaccount)
    ):
        return "configured_series_ticker_explicit_legacy_fallback"
    return None


@dataclass
class HistorySyncResult:
    cutoff: datetime | None
    fills: list[NormalizedFill]
    settlements: list[NormalizedSettlement]
    series_fills: list[NormalizedFill]
    series_settlements: list[NormalizedSettlement]
    duplicate_fills_removed: int
    ambiguous_fills: list[dict[str, Any]]
    owned_fill_audit: list[dict[str, Any]]
    owned_settlement_audit: list[dict[str, Any]]
    ambiguous_settlement_audit: list[dict[str, Any]]
    balance_payload: Mapping[str, Any] | None


class HistoricalSynchronizer:
    """Causally route historical/live API pages, then preserve only bot fills."""

    def __init__(self, config: RegimeConfig, local_state: Mapping[str, Any]) -> None:
        self.config = config
        self.local_order_ids, self.local_tickers = known_bot_identifiers(local_state)
        self.local_order_metadata = known_bot_order_metadata(local_state)

    async def sync(self, api: JsonAPI) -> HistorySyncResult:
        cutoff_payload = await api.get_json("/historical/cutoff")
        cutoff = cutoff_timestamp(cutoff_payload)
        live_request: dict[str, Any] = {"subaccount": self.config.subaccount}
        if self.config.history_start_ts:
            live_request["min_ts"] = int(self.config.history_start_ts.timestamp())
        if self.config.history_end_ts:
            live_request["max_ts"] = int(self.config.history_end_ts.timestamp())
        # The historical-fill endpoint documents only its archival cursor and
        # upper-bound parameters; unlike the live endpoint, do not send a
        # speculative ``min_ts`` or ``subaccount`` that can turn a startup
        # reconstruction into a 400.  We apply the configured time and
        # subaccount scope after normalized decoding below.
        historical_request: dict[str, Any] = {}
        if self.config.history_end_ts:
            historical_request["max_ts"] = int(self.config.history_end_ts.timestamp())
        start = self.config.history_start_ts
        end = self.config.history_end_ts
        fetch_historical = cutoff is None or start is None or start <= cutoff
        fetch_live = cutoff is None or end is None or end >= cutoff
        raw_fills: list[tuple[str, Mapping[str, Any]]] = []
        if fetch_historical:
            raw_fills.extend(("historical", row) for row in await paginated_records(
                api, "/historical/fills", params=historical_request, list_keys=("fills", "historical_fills"),
            ))
        if fetch_live:
            raw_fills.extend(("portfolio", row) for row in await paginated_records(
                api, "/portfolio/fills", params=live_request, list_keys=("fills", "portfolio_fills"),
            ))
        raw_settlements = await paginated_records(
            api, "/portfolio/settlements", params=live_request, list_keys=("settlements", "market_settlements"),
        )
        balance_payload = await api.get_json("/portfolio/balance", {"subaccount": self.config.subaccount})
        # Keep the complete normalized account-series set separately from the
        # bot-owned subset.  The latter remains the accounting source for the
        # strategy; the former is used only when the operator explicitly
        # selects the Colab-compatible account-series Prophet input.
        all_deduplicated: dict[tuple[str, ...], NormalizedFill] = {}
        deduplicated: dict[tuple[str, ...], NormalizedFill] = {}
        duplicate_count = 0
        ambiguous: list[dict[str, Any]] = []
        owned_fill_audit: list[dict[str, Any]] = []
        for source, row in raw_fills:
            try:
                fill = normalize_fill(row, source)
            except ValueError as exc:
                ambiguous.append({"reason": str(exc), "raw": dict(row)})
                continue
            if (
                (self.config.history_start_ts and fill.created_at and fill.created_at < self.config.history_start_ts)
                or (self.config.history_end_ts and fill.created_at and fill.created_at > self.config.history_end_ts)
            ):
                continue
            if fill.dedupe_key in all_deduplicated:
                duplicate_count += 1
                continue
            all_deduplicated[fill.dedupe_key] = fill
            evidence = ownership_evidence(fill, self.config, self.local_order_ids, self.local_tickers)
            if evidence is None:
                ambiguous.append({
                    "reason": "ambiguous_bot_ownership", "fill_id": fill.fill_id,
                    "ticker": fill.ticker, "order_id": fill.order_id,
                    "client_order_id": fill.client_order_id, "order_group_id": fill.order_group_id,
                    "created_at": timestamp_text(fill.created_at), "action": fill.action,
                    "side": fill.side, "price": format(fill.price, "f"),
                    "count": format(fill.count, "f"), "fee": format(fill.fee, "f"),
                    "source": fill.source,
                })
                continue
            order_metadata = (
                self.local_order_metadata.get(fill.order_id or "")
                or self.local_order_metadata.get(fill.client_order_id or "")
            )
            if order_metadata is not None:
                economic_side = order_metadata["economic_side"]
                fill = replace(
                    fill,
                    economic_side=economic_side,
                    economic_action=order_metadata["economic_action"],
                    economic_price=price_for_economic_side(fill.raw, economic_side),
                    order_role=order_metadata["order_role"],
                )
                # The account-series curve must account bot reduce-only exits
                # with the same economic side/action as bot accounting.
                all_deduplicated[fill.dedupe_key] = fill
            deduplicated[fill.dedupe_key] = fill
            owned_fill_audit.append({
                "ownership_evidence": evidence,
                "fill_id": fill.fill_id,
                "trade_id": fill.trade_id,
                "order_id": fill.order_id,
                "client_order_id": fill.client_order_id,
                "order_group_id": fill.order_group_id,
                "ticker": fill.ticker,
                "created_at": timestamp_text(fill.created_at),
                "action": fill.action,
                "side": fill.side,
                "price": format(fill.price, "f"),
                "count": format(fill.count, "f"),
                "fee": format(fill.fee, "f"),
                "economic_side": fill.economic_side,
                "economic_action": fill.economic_action,
                "economic_price": None if fill.economic_price is None else format(fill.economic_price, "f"),
                "order_role": fill.order_role,
                "subaccount": fill.subaccount,
                "source": fill.source,
                "raw_api_record": raw_record_text(fill.raw),
            })
        owned_tickers = {fill.ticker for fill in deduplicated.values()}
        ambiguous_tickers = {str(row.get("ticker")) for row in ambiguous if row.get("ticker")}
        settlements: list[NormalizedSettlement] = []
        series_settlement_by_ticker: dict[str, NormalizedSettlement] = {}
        owned_settlement_audit: list[dict[str, Any]] = []
        ambiguous_settlement_audit: list[dict[str, Any]] = []
        for row in raw_settlements:
            try:
                settlement = normalize_settlement(row, "portfolio")
            except ValueError:
                continue
            settlement_audit = {
                "ticker": settlement.ticker,
                "settled_at": timestamp_text(settlement.settled_at),
                "result": settlement.result,
                "payout": None if settlement.payout is None else format(settlement.payout, "f"),
                "yes_count": None if settlement.yes_count is None else format(settlement.yes_count, "f"),
                "no_count": None if settlement.no_count is None else format(settlement.no_count, "f"),
                "source": settlement.source,
                "raw_api_record": raw_record_text(settlement.raw),
            }
            if (
                settlement.ticker.startswith(self.config.series_ticker + "-")
                and not (
                    (self.config.history_start_ts and settlement.settled_at and settlement.settled_at < self.config.history_start_ts)
                    or (self.config.history_end_ts and settlement.settled_at and settlement.settled_at > self.config.history_end_ts)
                )
            ):
                existing = series_settlement_by_ticker.get(settlement.ticker)
                if existing is None or (settlement.settled_at or datetime.min.replace(tzinfo=UTC)) > (existing.settled_at or datetime.min.replace(tzinfo=UTC)):
                    series_settlement_by_ticker[settlement.ticker] = settlement
            if (
                settlement.ticker in owned_tickers
                and not (
                    (self.config.history_start_ts and settlement.settled_at and settlement.settled_at < self.config.history_start_ts)
                    or (self.config.history_end_ts and settlement.settled_at and settlement.settled_at > self.config.history_end_ts)
                )
            ):
                settlements.append(settlement)
                owned_settlement_audit.append(settlement_audit)
            elif settlement.ticker in ambiguous_tickers:
                # This deliberately exposes only a settlement which belongs
                # to an already-reported ambiguous fill.  It lets the
                # operator calculate a user-disclosed manual trade's cash
                # result without promoting that trade into bot accounting.
                ambiguous_settlement_audit.append(settlement_audit)
        fills = sorted(deduplicated.values(), key=lambda item: (item.created_at or datetime.min.replace(tzinfo=UTC), item.fill_id))
        settlements.sort(key=lambda item: (item.settled_at or datetime.min.replace(tzinfo=UTC), item.ticker))
        series_fills = sorted(
            (fill for fill in all_deduplicated.values() if fill.ticker.startswith(self.config.series_ticker + "-")),
            key=lambda item: (item.created_at or datetime.min.replace(tzinfo=UTC), item.fill_id),
        )
        series_settlements = sorted(
            series_settlement_by_ticker.values(),
            key=lambda item: (item.settled_at or datetime.min.replace(tzinfo=UTC), item.ticker),
        )
        owned_fill_audit.sort(key=lambda item: (item["created_at"], item["fill_id"]))
        owned_settlement_audit.sort(key=lambda item: (item["settled_at"], item["ticker"]))
        ambiguous_settlement_audit.sort(key=lambda item: (item["settled_at"], item["ticker"]))
        return HistorySyncResult(
            cutoff, fills, settlements, series_fills, series_settlements, duplicate_count, ambiguous,
            owned_fill_audit, owned_settlement_audit, ambiguous_settlement_audit, balance_payload,
        )


@dataclass(frozen=True)
class ReconstructedMarket:
    market_ticker: str
    market_close_time: datetime | None
    selected_side: str | None
    contracts_bought: Decimal
    contracts_sold: Decimal
    average_entry: Decimal | None
    entry_cost: Decimal
    exit_proceeds: Decimal
    settlement_payout: Decimal
    fees: Decimal
    realized_pnl: Decimal
    exit_method: str
    source: str
    reconciliation_status: str


def reconstruct_realized_pnl(fills: Iterable[NormalizedFill], settlements: Iterable[NormalizedSettlement]) -> list[ReconstructedMarket]:
    """Reconstruct closed-market cash P/L including partial exits and fees."""

    by_ticker: dict[str, list[NormalizedFill]] = defaultdict(list)
    for fill in fills:
        by_ticker[fill.ticker].append(fill)
    settlement_by_ticker = {item.ticker: item for item in settlements}
    reconstructed: list[ReconstructedMarket] = []
    for ticker, market_fills in by_ticker.items():
        market_fills.sort(key=lambda item: (item.created_at or datetime.min.replace(tzinfo=UTC), item.fill_id))
        held: dict[str, Decimal] = defaultdict(lambda: ZERO)
        bought: dict[str, Decimal] = defaultdict(lambda: ZERO)
        sold: dict[str, Decimal] = defaultdict(lambda: ZERO)
        entry_cost = ZERO
        exit_proceeds = ZERO
        fees = ZERO
        entry_units = ZERO
        selected_side: str | None = None
        for fill in market_fills:
            # A fill matched to a durable local bot order is accounted using
            # that order's economic side/action.  This is required for
            # reduce-only exits: Kalshi represents a YES exit on the
            # reciprocal NO/ASK book leg.  Falling back to raw fields is
            # retained only for explicitly supported legacy imports.
            economic_side = fill.economic_side or fill.side
            economic_action = fill.economic_action or fill.action
            economic_price = fill.economic_price or fill.price
            fees += fill.fee
            if economic_action == "buy":
                held[economic_side] += fill.count
                bought[economic_side] += fill.count
                entry_cost += economic_price * fill.count
                entry_units += fill.count
                selected_side = selected_side or economic_side
            else:
                held[economic_side] -= fill.count
                sold[economic_side] += fill.count
                exit_proceeds += economic_price * fill.count
        settlement = settlement_by_ticker.get(ticker)
        payout = ZERO
        exit_method = "sold" if exit_proceeds > ZERO else "settlement"
        if settlement is not None:
            if settlement.payout is not None:
                payout = settlement.payout
            elif settlement.result in {"yes", "no"}:
                payout = max(ZERO, held[settlement.result])
            if payout > ZERO:
                exit_method = "settlement" if exit_proceeds == ZERO else "partial_exit_and_settlement"
        # Fees are tracked once: every fill cash flow contains exactly one fee.
        pnl = exit_proceeds + payout - entry_cost - fees
        side_bought = bought[selected_side] if selected_side else sum(bought.values(), ZERO)
        side_sold = sold[selected_side] if selected_side else sum(sold.values(), ZERO)
        average_entry = entry_cost / entry_units if entry_units > ZERO else None
        sources = "+".join(sorted({fill.source for fill in market_fills}))
        reconstructed.append(ReconstructedMarket(
            market_ticker=ticker,
            market_close_time=settlement.settled_at if settlement else max((fill.created_at for fill in market_fills if fill.created_at), default=None),
            selected_side=selected_side,
            contracts_bought=side_bought,
            contracts_sold=side_sold,
            average_entry=average_entry,
            entry_cost=entry_cost,
            exit_proceeds=exit_proceeds,
            settlement_payout=payout,
            fees=fees,
            realized_pnl=pnl,
            exit_method=exit_method,
            source=sources,
            reconciliation_status="reconstructed_from_fills_and_settlements",
        ))
    return sorted(reconstructed, key=lambda item: (item.market_close_time or datetime.min.replace(tzinfo=UTC), item.market_ticker))


class ProphetForecaster:
    """Absolute-balance Prophet wrapper with exact predictive quantiles."""

    def __init__(self, config: RegimeConfig) -> None:
        self.config = config
        self.fit_number = 0
        self.last_future_timestamps: list[datetime] = []

    def forecast(self, observations: list[Mapping[str, Any]], target_time: datetime) -> dict[str, Decimal]:
        """Return the one, actual next-market forecast used by the gate."""

        return self.forecast_horizon(observations, target_time, 1)[0]

    def forecast_horizon(
        self,
        observations: list[Mapping[str, Any]],
        target_time: datetime,
        horizon_markets: int,
    ) -> list[dict[str, Decimal]]:
        """Fit once and return a diagnostic future path on the balance scale.

        The first row is exactly ``target_time`` and is the *only* row the
        P10/P90 state machine may consume.  Rows 2..N are a visibility report
        at the configured 15-minute cadence; they are never matched to later
        outcomes or used to make an earlier trade decision.
        """

        if len(observations) < self.config.prophet_min_history:
            raise ValueError("insufficient shadow-balance history")
        if horizon_markets < 1:
            raise ValueError("horizon_markets must be positive")
        if self.config.prophet_training_window is not None:
            observations = observations[-self.config.prophet_training_window :]
        training_times = [observation_timestamp(row) for row in observations]
        if not all(training_times) or max(training_times) >= target_time:
            raise ValueError("forecast training contains target or future shadow observation")
        balances = [decimal_value(row.get("shadow_balance_after")) for row in observations]
        if self.config.prophet_use_log_transform and any(value <= ZERO for value in balances):
            raise ValueError("log-transform Prophet requires positive shadow balances")
        try:
            import numpy as np
            import pandas as pd
            from prophet import Prophet
        except ImportError as exc:  # pragma: no cover - dependency is checked by workflow
            raise RuntimeError("Prophet dependencies are unavailable") from exc
        training = pd.DataFrame({
            "ds": [item.replace(tzinfo=None) for item in training_times],
            "y": [math.log(float(value)) if self.config.prophet_use_log_transform else float(value) for value in balances],
        })
        model = Prophet(
            daily_seasonality=self.config.prophet_daily_seasonality,
            weekly_seasonality=self.config.prophet_weekly_seasonality,
            yearly_seasonality=self.config.prophet_yearly_seasonality,
            changepoint_prior_scale=self.config.prophet_changepoint_prior_scale,
            seasonality_prior_scale=self.config.prophet_seasonality_prior_scale,
            uncertainty_samples=self.config.prophet_uncertainty_samples,
        )
        model.fit(training)
        # Match the reference Colab forecast procedure: Prophet samples one
        # historical-plus-future frame made by ``make_future_dataframe``.
        # Sampling only a hand-built future frame changes the RNG draw layout
        # and therefore produces different P01/P10/.../P99 values even when
        # the fitted balance curve and model settings are identical.
        future = model.make_future_dataframe(
            periods=horizon_markets,
            freq=f"{self.config.prophet_forecast_frequency_minutes}min",
            include_history=True,
        )
        self.last_future_timestamps = [
            utc_timestamp(value) for value in future["ds"].iloc[-horizon_markets:]
        ]
        if any(value is None for value in self.last_future_timestamps):
            raise RuntimeError("Prophet produced an invalid future timestamp")
        # The supplied Colab seeds immediately before predictive_samples, not
        # before fitting.  Use the same stable seed for every refit so a
        # unchanged 200-row balance curve yields the same published bands.
        np.random.seed(self.config.prophet_random_seed)
        samples = np.asarray(model.predictive_samples(future).get("yhat"))
        if samples.ndim != 2:
            raise RuntimeError(f"unexpected Prophet predictive sample shape {samples.shape}")
        if samples.shape[0] != len(future):
            if samples.shape[1] == len(future):
                samples = samples.T
            else:
                raise RuntimeError(f"samples do not match requested horizon: {samples.shape}")
        values = np.quantile(samples, QUANTILE_PROBABILITIES, axis=1).T[-horizon_markets:]
        if self.config.prophet_use_log_transform:
            values = np.exp(values)
        self.fit_number += 1
        results: list[dict[str, Decimal]] = []
        for row in values:
            result = {name: Decimal(str(value)) for name, value in zip(QUANTILES, row, strict=True)}
            ordered = [result[name] for name in QUANTILES]
            if ordered != sorted(ordered):
                raise RuntimeError("Prophet predictive quantiles are not ordered")
            results.append(result)
        return results


@dataclass
class ShadowExecutor:
    """A conservative stateful shadow fill/stop/settlement simulation."""

    config: RegimeConfig

    def start(self, decision: StrategyDecision, execution_was_enabled: bool) -> dict[str, Any]:
        quality = "unavailable" if self.config.shadow_fill_model == "conservative_trade_through" else "conservative_approximation"
        return {
            "target_ticker": decision.target_ticker,
            "source_ticker": decision.source_ticker,
            "selected_side": decision.selected_side,
            "eligible": decision.eligible,
            "skip_reason": decision.skip_reason,
            "generated_at": timestamp_text(decision.generated_at),
            "market_close_time": timestamp_text(decision.target_close_time),
            "orders": [
                {"price": format(order.price, "f"), "quantity": format(order.quantity, "f"), "order_key": order.order_key,
                 "filled": "0", "created_at": timestamp_text(decision.generated_at)}
                for order in decision.ladder_orders
            ],
            "stop_price": format(decision.stop_price, "f"),
            "trailing_activation_gain": format(decision.trailing_activation_gain, "f"),
            "trailing_retracement": format(decision.trailing_retracement, "f"),
            "entry_cost": "0", "proceeds": "0", "fees": "0", "payout": "0",
            "highest_bid": None, "exit_method": None, "status": "active" if decision.eligible else "skipped",
            "shadow_fill_model": self.config.shadow_fill_model,
            "shadow_simulation_quality": quality,
            "live_execution_enabled": execution_was_enabled,
            "fill_notes": [],
        }

    def observe_touch_quote(self, trade: dict[str, Any], quote_getter: Callable[[str, str, float, float], tuple[dict[str, Any] | None, str]]) -> bool:
        """Fill resting shadow rungs only against post-decision fresh BBO data.

        ``conservative_trade_through`` intentionally does not claim a fill:
        the current feed has BBO snapshots, not an ordered trade tape.  A
        touch fill is supported and explicitly labelled an approximation.
        """

        if trade.get("status") != "active" or self.config.shadow_fill_model != "touch":
            return False
        changed = False
        now = datetime.now(tz=UTC)
        created = utc_timestamp(trade.get("generated_at"))
        if created and now < created + timedelta_milliseconds(self.config.shadow_latency_ms):
            return False
        for order in trade.get("orders", []):
            price = decimal_value(order.get("price"))
            quantity = decimal_value(order.get("quantity"))
            already = decimal_value(order.get("filled"))
            remaining = quantity - already
            if remaining <= ZERO:
                continue
            quote, reason = quote_getter(
                str(trade["target_ticker"]), str(trade["selected_side"]), float(remaining), 3.0,
            )
            if quote is None:
                continue
            ask = decimal_value(quote.get("economic_price"))
            if ask > price:
                continue
            depth = decimal_value(quote.get("displayed_depth"))
            fill = min(remaining, depth) if self.config.shadow_partial_fills else (remaining if depth >= remaining else ZERO)
            if fill <= ZERO:
                continue
            # A resting limit's base assumed fill is at its limit, not a
            # favourable instantaneous ask. Optional configured slippage is
            # then applied pessimistically and recorded as an approximation.
            modeled_price = min(ONE, price + self.config.shadow_slippage_cents / Decimal("100"))
            order["filled"] = format(already + fill, "f")
            trade["entry_cost"] = format(decimal_value(trade["entry_cost"]) + modeled_price * fill, "f")
            trade["fill_notes"].append({"order_key": order.get("order_key"), "fill": format(fill, "f"), "modeled_price": format(modeled_price, "f"), "reason": "touch_after_decision"})
            changed = True
        return changed

    def observe_trade_through(
        self,
        trade: dict[str, Any],
        public_trade_getter: Callable[[str, datetime], list[dict[str, Any]]],
    ) -> bool:
        """Conservatively fill a resting order from post-order public trades.

        Kalshi's public trade stream gives a timestamp, fixed-point count, and
        YES/NO price.  It does not expose our queue position, so this is a
        conservative *approximation*, not an exact replay.  We consume each
        public trade's displayed volume at most once and retain the event ID
        and timestamp with the shadow record for auditability.
        """
        if trade.get("status") != "active" or self.config.shadow_fill_model != "conservative_trade_through":
            return False
        created = utc_timestamp(trade.get("generated_at"))
        if created is None or datetime.now(tz=UTC) < created + timedelta_milliseconds(self.config.shadow_latency_ms):
            return False
        try:
            events = public_trade_getter(str(trade["target_ticker"]), created)
        except Exception as exc:  # noqa: BLE001 - data absence remains unavailable, never a fill
            trade.setdefault("fill_notes", []).append({"reason": "public_trade_getter_error", "error": str(exc)})
            return False
        # A successfully queried, post-decision public tape is still useful
        # evidence when no trade crossed an order. It remains conservative
        # because queue position is unavailable, but it is not the same as a
        # missing stream/reconnect gap.  This lets a genuine zero-fill market
        # contribute a zero shadow P/L instead of permanently blocking a P10
        # restart merely because no public executions occurred.
        if trade.get("shadow_simulation_quality") == "unavailable":
            trade["shadow_simulation_quality"] = "conservative_approximation"
            trade.setdefault("fill_notes", []).append({
                "reason": "post_order_public_tape_checked",
                "events_seen": len(events),
            })
        consumed = trade.setdefault("consumed_public_trade_count", {})
        changed = False
        # Highest-priced buy has the strongest fill priority.  A single public
        # execution cannot fill multiple rungs beyond its reported quantity.
        orders = sorted(trade.get("orders", []), key=lambda item: decimal_value(item.get("price")), reverse=True)
        for event in events:
            trade_id = str(event.get("trade_id") or "")
            if not trade_id:
                continue
            total = decimal_value(event.get("count"))
            remaining_event = total - decimal_value(consumed.get(trade_id))
            if remaining_event <= ZERO:
                continue
            side = str(trade.get("selected_side") or "").lower()
            event_price = decimal_value(event.get(f"{side}_price"))
            if not ZERO < event_price < ONE:
                continue
            for order in orders:
                limit = decimal_value(order.get("price"))
                quantity = decimal_value(order.get("quantity"))
                already = decimal_value(order.get("filled"))
                remaining_order = quantity - already
                if remaining_order <= ZERO:
                    continue
                # "Trade through" here implements the configured causal
                # rule: a post-order execution at or below our resting buy
                # limit may fill. Queue position is still unknown, hence the
                # non-exact quality label.
                if event_price > limit:
                    continue
                fill = min(remaining_order, remaining_event)
                if not self.config.shadow_partial_fills and fill < remaining_order:
                    continue
                if fill <= ZERO:
                    continue
                modeled_price = min(ONE, limit + self.config.shadow_slippage_cents / Decimal("100"))
                order["filled"] = format(already + fill, "f")
                trade["entry_cost"] = format(decimal_value(trade["entry_cost"]) + modeled_price * fill, "f")
                remaining_event -= fill
                consumed[trade_id] = format(total - remaining_event, "f")
                trade["shadow_simulation_quality"] = "conservative_approximation"
                trade.setdefault("fill_notes", []).append({
                    "order_key": order.get("order_key"), "fill": format(fill, "f"),
                    "modeled_price": format(modeled_price, "f"), "reason": "post_order_public_trade_through",
                    "public_trade_id": trade_id,
                    "public_trade_timestamp": event.get("source_server_timestamp"),
                    "public_trade_price": format(event_price, "f"),
                })
                changed = True
                if remaining_event <= ZERO:
                    break
        return changed

    def observe_exit_quote(self, trade: dict[str, Any], quote_getter: Callable[[str, str, float, float], tuple[dict[str, Any] | None, str]]) -> bool:
        if trade.get("status") != "active":
            return False
        filled = sum((decimal_value(order.get("filled")) for order in trade.get("orders", [])), ZERO)
        exited = decimal_value(trade.get("exit_contracts"))
        held = filled - exited
        if held <= ZERO:
            return False
        quote, _ = quote_getter(str(trade["target_ticker"]), str(trade["selected_side"]), float(held), 3.0)
        if quote is None:
            return False
        bid = decimal_value(quote.get("economic_price"))
        average = decimal_value(trade["entry_cost"]) / filled if filled > ZERO else ZERO
        high = decimal_value(trade["highest_bid"]) if trade.get("highest_bid") is not None else bid
        high = max(high, bid)
        trade["highest_bid"] = format(high, "f")
        stop = decimal_value(trade["stop_price"])
        activation = decimal_value(trade["trailing_activation_gain"])
        retracement = decimal_value(trade["trailing_retracement"])
        trigger = "absolute_stop" if bid <= stop else ("trailing_stop" if high >= average + activation and bid <= high - retracement else None)
        if trigger is None:
            return False
        trade["proceeds"] = format(decimal_value(trade["proceeds"]) + bid * held, "f")
        trade["exit_contracts"] = format(exited + held, "f")
        trade["exit_method"] = trigger
        trade["status"] = "exited"
        return True

    def finalize(self, trade: dict[str, Any], outcome: str | None) -> dict[str, Any]:
        if trade.get("status") == "finalized":
            return trade
        filled = sum((decimal_value(order.get("filled")) for order in trade.get("orders", [])), ZERO)
        exited = decimal_value(trade.get("exit_contracts"))
        held = max(ZERO, filled - exited)
        payout = held if outcome == trade.get("selected_side") else ZERO
        trade["payout"] = format(payout, "f")
        trade["contracts"] = format(filled, "f")
        trade["shadow_realized_pnl"] = format(
            decimal_value(trade["proceeds"]) + payout - decimal_value(trade["entry_cost"]) - decimal_value(trade["fees"]), "f",
        )
        trade["settlement_outcome"] = outcome
        trade["exit_method"] = trade.get("exit_method") or "settlement"
        trade["status"] = "finalized"
        return trade


def timedelta_milliseconds(milliseconds: int):
    from datetime import timedelta
    return timedelta(milliseconds=max(0, milliseconds))


class EquityRegimeController:
    """Transactionally maintains actual/shadow curves and the P10/P90 gate."""

    def __init__(self, config: RegimeConfig, state_path: Path, output_dir: Path) -> None:
        self.config = config
        self.store = AtomicJsonStore(state_path, lambda: default_regime_state(config))
        self.state = self.store.load()
        self.output_dir = output_dir
        self.forecaster = ProphetForecaster(config)
        self.shadow_executor = ShadowExecutor(config)
        self._normalize_state()
        LOG.info(
            "STATE RESTORED | actual_balance=%s shadow_balance=%s execution_enabled=%s dry_run=%s reconciled=%s",
            self.state["actual_balance"], self.state["shadow_balance"], self.state["execution_enabled"], config.dry_run,
            self.state["balance_reconciled"],
        )

    def _normalize_state(self) -> None:
        # Version 1 stored a rebased ``100 + recent P/L`` index in these
        # fields.  Never translate those numbers into balances; preserve them
        # only long enough to write an audit archive during initialization.
        if "actual_equity" in self.state or "shadow_equity" in self.state:
            self.state.setdefault("legacy_rebased_actual_equity", self.state.get("actual_equity"))
            self.state.setdefault("legacy_rebased_shadow_equity", self.state.get("shadow_equity"))
        defaults = default_regime_state(self.config)
        for key, value in defaults.items():
            self.state.setdefault(key, value)
        version = int(self.state.get("format_version") or 0)
        # Version 2's endpoint bootstrap treated unfilled/zero-contract
        # market attempts as historical balance observations and used API
        # close timestamps rather than the ticker timestamps used by the
        # canonical Colab curve.  Those fitted bands are not comparable to
        # the supplied closed-position export, so never continue from them.
        if (
            version < 3
            and self.state.get("balance_source") == "authenticated_endpoint_anchored_durable_live_bot_ledger"
        ):
            prior_actual_rows = len(self.state.get("actual_history") or [])
            prior_shadow_rows = len(self.state.get("shadow_history") or [])
            stamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
            archive = self.store.path.with_name(f"{self.store.path.stem}.pre-colab-reference-{stamp}.json")
            AtomicJsonStore(archive, lambda: {}).save(dict(self.state))
            self.state.update({
                "actual_history": [], "shadow_history": [],
                "actual_balance_anchor": None, "shadow_balance_anchor": None,
                "forecasts": [], "future_forecast_snapshot": [], "transitions": [], "live_vs_shadow": [],
                "processed_market_tickers": [], "markets_since_refit": 0,
                "balance_reconciled": False,
                "balance_reconciliation_error": "pre_colab_reference_history_invalidated",
                "balance_source": "requires_verified_closed_position_rebuild",
                "state_reason": "pre_colab_reference_history_invalidated",
                "execution_enabled": True,
                "history_migration": {
                    "migration_timestamp": timestamp_text(datetime.now(tz=UTC)),
                    "migration_reason": "market_attempts_and_api_close_timestamps_do_not_match_closed_position_balance_curve",
                    "prior_actual_rows": prior_actual_rows,
                    "prior_shadow_rows": prior_shadow_rows,
                    "archive": str(archive),
                },
            })
            LOG.error("PRE-COLAB-REFERENCE HISTORY INVALIDATED | archive=%s", archive)
        self.state["format_version"] = max(version, 3)
        self.state["shadow_enabled"] = True  # invariant; regime never disables the shadow strategy
        self.state["execution_enabled"] = bool_value(self.state.get("execution_enabled"), True)

    @property
    def balance_reconciled(self) -> bool:
        return bool_value(self.state.get("balance_reconciled"), False)

    def migrate_legacy_rebased_state(self, api_current_balance: Decimal) -> bool:
        """Archive a version-1 rebased index and invalidate every derived band.

        The legacy controller initialized the last 200-market P/L replay at
        $100.  That value is not recoverable account balance data and cannot
        be made valid by relabeling it.  The safe migration starts both curves
        at the authenticated current balance and requires fresh observations.
        """

        legacy_shadow = self.state.get("legacy_rebased_shadow_equity")
        legacy_actual = self.state.get("legacy_rebased_actual_equity")
        if legacy_shadow is None and legacy_actual is None:
            return False
        stamp = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
        archive = self.store.path.with_name(f"{self.store.path.stem}.legacy-rebased-{stamp}.json")
        AtomicJsonStore(archive, lambda: {}).save(dict(self.state))
        self.state.update({
            "actual_balance": format(api_current_balance, "f"),
            "shadow_balance": format(api_current_balance, "f"),
            "historical_starting_balance": format(api_current_balance, "f"),
            "balance_source": "authenticated_api_balance_after_legacy_rebased_migration",
            "balance_reconciled": True,
            "balance_reconciliation_error": None,
            "actual_history": [], "shadow_history": [], "actual_balance_anchor": None, "shadow_balance_anchor": None,
            "markets_since_refit": 0, "forecasts": [], "future_forecast_snapshot": [], "transitions": [],
            "live_vs_shadow": [], "processed_market_tickers": [], "shadow_open": {},
            "last_p01": None, "last_p10": None, "last_p25": None, "last_p50": None,
            "last_p75": None, "last_p90": None, "last_p99": None,
            "forecast_generated_at": None, "forecast_target_ticker": None,
            "prophet_training_end": None, "prophet_training_rows": 0,
            "execution_enabled": True, "state_reason": "legacy_rebased_state_migrated_dry_run",
            "legacy_migration": {
                "legacy_shadow_value": legacy_shadow,
                "legacy_actual_value": legacy_actual,
                "corrected_actual_balance": format(api_current_balance, "f"),
                "corrected_shadow_balance": format(api_current_balance, "f"),
                "migration_timestamp": timestamp_text(datetime.now(tz=UTC)),
                "migration_reason": "legacy_rolling_pnl_index_is_not_absolute_balance",
                "archive": str(archive),
            },
        })
        self.state.pop("actual_equity", None)
        self.state.pop("shadow_equity", None)
        self.state.pop("legacy_rebased_actual_equity", None)
        self.state.pop("legacy_rebased_shadow_equity", None)
        LOG.error(
            "LEGACY REBASED STATE MIGRATED | archived=%s actual_balance=%s shadow_balance=%s",
            archive, api_current_balance, api_current_balance,
        )
        return True

    def initialize_absolute_balances(self, api_current_balance: Decimal, *, reason: str) -> None:
        """Initialize or reconcile both curves against authenticated Kalshi cash."""

        if api_current_balance <= ZERO:
            raise RuntimeError("authenticated Kalshi balance is non-positive; regime controller fails closed")
        if self.migrate_legacy_rebased_state(api_current_balance):
            self.save()
            return
        actual = self.state.get("actual_balance")
        shadow = self.state.get("shadow_balance")
        if actual is None or shadow is None:
            self.state.update({
                "actual_balance": format(api_current_balance, "f"),
                "shadow_balance": format(api_current_balance, "f"),
                "historical_starting_balance": format(api_current_balance, "f"),
                "balance_source": "authenticated_api_balance_initialization",
                "balance_reconciled": True,
                "balance_reconciliation_error": None,
                # The empty curve is safe but cannot forecast until it meets
                # prophet_min_history.  This flag distinguishes it from a
                # missing/invalid history source.
                "prophet_history_ready": True,
                "state_reason": reason,
            })
            self.state["forecasts"] = []
            LOG.info("ABSOLUTE BALANCES INITIALIZED | actual=%s shadow=%s", api_current_balance, api_current_balance)
            self.save()
            return
        reconstructed = decimal_value(actual)
        difference = reconstructed - api_current_balance
        self.state["balance_reconciled"] = abs(difference) <= self.config.accounting_tolerance
        self.state["balance_reconciliation_error"] = None if self.balance_reconciled else format(difference, "f")
        if not self.balance_reconciled:
            self.state["forecasts"] = []
            self.state["prophet_history_ready"] = False
            self.state["execution_enabled"] = True
            self.state["state_reason"] = "actual_balance_api_reconciliation_failed"
            LOG.error(
                "EQUITY RECONCILIATION FAILED | api_balance=%s reconstructed_balance=%s difference=%s; regime control disabled",
                api_current_balance, reconstructed, difference,
            )
        self.save()

    def rebuild_absolute_history(
        self,
        rows: Iterable[ReconstructedMarket],
        *,
        historical_starting_balance: Decimal,
        api_current_balance: Decimal,
    ) -> bool:
        """Rebuild an absolute curve only from an explicitly verified anchor.

        Fills and settlements tell us P/L, not the balance before the first
        fill.  This method refuses to publish that P/L series as a balance
        unless its supplied historical anchor reconciles to the authenticated
        current balance within a cent.
        """

        ordered = [row for row in rows if row.market_close_time is not None]
        expected = historical_starting_balance + sum((row.realized_pnl for row in ordered), ZERO)
        difference = expected - api_current_balance
        if abs(difference) > self.config.accounting_tolerance:
            self.state.update({
                "actual_balance": format(api_current_balance, "f"),
                "shadow_balance": format(api_current_balance, "f"),
                "historical_starting_balance": format(historical_starting_balance, "f"),
                "balance_source": "authenticated_api_balance_historical_reconstruction_unreconciled",
                "balance_reconciled": False,
                "balance_reconciliation_error": format(difference, "f"),
                "state_reason": "historical_absolute_balance_reconstruction_failed",
                "forecasts": [], "execution_enabled": True,
            })
            LOG.error(
                "EQUITY RECONCILIATION FAILED | historical_start=%s included_pnl=%s reconstructed=%s api=%s difference=%s",
                historical_starting_balance, expected - historical_starting_balance, expected, api_current_balance, difference,
            )
            return False
        actual = shadow = historical_starting_balance
        actual_history: list[dict[str, Any]] = []
        shadow_history: list[dict[str, Any]] = []
        for row in ordered:
            actual_before = actual
            shadow_before = shadow
            actual += row.realized_pnl
            shadow += row.realized_pnl
            timestamp = timestamp_text(row.market_close_time)
            actual_history.append({
                "timestamp": timestamp, "market_ticker": row.market_ticker, "market_close_time": timestamp,
                "actual_balance_before": format(actual_before, "f"), "actual_realized_pnl": format(row.realized_pnl, "f"),
                "actual_balance_after": format(actual, "f"), "execution_enabled_for_market": True,
                "state_before_market": "on", "state_after_market": "on", "balance_source": "reconstructed_from_verified_historical_starting_balance",
                "reconciled": True, "selected_side": row.selected_side, "contracts_bought": format(row.contracts_bought, "f"),
                "contracts_sold": format(row.contracts_sold, "f"), "average_entry": None if row.average_entry is None else format(row.average_entry, "f"),
                "entry_cost": format(row.entry_cost, "f"), "exit_proceeds": format(row.exit_proceeds, "f"),
                "settlement_payout": format(row.settlement_payout, "f"), "fees": format(row.fees, "f"),
                "exit_method": row.exit_method, "source": row.source, "reconciliation_status": row.reconciliation_status,
            })
            shadow_history.append({
                "timestamp": timestamp, "market_ticker": row.market_ticker, "market_close_time": timestamp,
                "shadow_balance_before": format(shadow_before, "f"), "shadow_market_pnl": format(row.realized_pnl, "f"),
                "shadow_realized_pnl": format(row.realized_pnl, "f"), "shadow_balance_after": format(shadow, "f"),
                "shadow_selected_side": row.selected_side, "shadow_eligible": row.contracts_bought > ZERO,
                "shadow_skip_reason": None, "shadow_contracts": format(row.contracts_bought, "f"),
                "shadow_average_entry": None if row.average_entry is None else format(row.average_entry, "f"),
                "shadow_cost": format(row.entry_cost, "f"), "shadow_proceeds": format(row.exit_proceeds, "f"),
                "shadow_payout": format(row.settlement_payout, "f"), "shadow_fees": format(row.fees, "f"),
                "shadow_exit_method": row.exit_method, "shadow_fill_model": "live_equivalent",
                "shadow_simulation_quality": "exact_replay", "live_execution_enabled": True,
                "completed_at": timestamp,
            })
        self.state.update({
            "historical_starting_balance": format(historical_starting_balance, "f"),
            "actual_balance": format(actual, "f"), "shadow_balance": format(shadow, "f"),
            "actual_balance_anchor": {
                "market_ticker": "__STARTING_BALANCE__",
                "market_close_time": timestamp_text(observation_timestamp(actual_history[0]) - timedelta(minutes=15)),
                "completed_at": timestamp_text(observation_timestamp(actual_history[0]) - timedelta(minutes=15)),
                "actual_balance_after": format(historical_starting_balance, "f"),
            },
            "shadow_balance_anchor": {
                "market_ticker": "__STARTING_BALANCE__",
                "market_close_time": timestamp_text(observation_timestamp(shadow_history[0]) - timedelta(minutes=15)),
                "completed_at": timestamp_text(observation_timestamp(shadow_history[0]) - timedelta(minutes=15)),
                "shadow_balance_after": format(historical_starting_balance, "f"),
            },
            "actual_history": actual_history, "shadow_history": shadow_history,
            "processed_market_tickers": [row.market_ticker for row in ordered],
            "forecasts": [], "future_forecast_snapshot": [], "transitions": [], "live_vs_shadow": [],
            "balance_source": "reconstructed_from_verified_historical_starting_balance",
            "balance_reconciled": True, "balance_reconciliation_error": None,
            "prophet_history_ready": True,
        })
        LOG.info("ABSOLUTE BALANCE HISTORY REBUILT | rows=%d start=%s end=%s", len(ordered), historical_starting_balance, actual)
        return True

    def rebuild_colab_reference_account_series_history(
        self,
        rows: Iterable[ReconstructedMarket],
        *,
        api_current_balance: Decimal,
    ) -> bool:
        """Build the exact $100-plus-cumulative-P/L Colab-style input.

        This deliberately does not attribute P/L to the bot and does not use
        the current API cash balance as an anchor.  It mirrors the reference
        notebook's construction: sort closed KXBTC15M markets by ticker clock,
        retain the last ``history_max_markets`` rows, place one configured
        starting-balance observation immediately before them, then accumulate
        their realized P/L.  API cash is retained separately as ``actual``
        operational balance and is not a prerequisite for Prophet bands.
        """

        candidates: list[tuple[datetime, ReconstructedMarket]] = []
        for row in rows:
            observed = ticker_clock_timestamp(row.market_ticker, row.market_close_time)
            if observed is None or not row.market_ticker.startswith(self.config.series_ticker + "-"):
                continue
            candidates.append((observed, row))
        candidates.sort(key=lambda item: (item[0], item[1].market_ticker))
        selected = candidates[-self.config.history_max_markets :]
        if len(selected) < self.config.prophet_min_history:
            LOG.error(
                "COLAB ACCOUNT-SERIES HISTORY UNAVAILABLE | closed_rows=%d requires=%d",
                len(selected), self.config.prophet_min_history,
            )
            self.state["prophet_history_ready"] = False
            return False

        balance = self.config.starting_balance
        actual_history: list[dict[str, Any]] = []
        shadow_history: list[dict[str, Any]] = []
        for observed, row in selected:
            before = balance
            balance += row.realized_pnl
            observed_text = timestamp_text(observed)
            completed = timestamp_text(row.market_close_time or observed)
            common = {
                "timestamp": observed_text,
                "market_ticker": row.market_ticker,
                "market_close_time": observed_text,
                "completed_at": completed,
            }
            actual_history.append({
                **common,
                "actual_balance_before": format(before, "f"),
                "actual_realized_pnl": format(row.realized_pnl, "f"),
                "actual_balance_after": format(balance, "f"),
                "execution_enabled_for_market": True,
                "state_before_market": "on", "state_after_market": "on",
                "balance_source": "colab_reference_account_series", "reconciled": False,
                "selected_side": row.selected_side,
                "contracts_bought": format(row.contracts_bought, "f"),
                "contracts_sold": format(row.contracts_sold, "f"),
                "average_entry": None if row.average_entry is None else format(row.average_entry, "f"),
                "entry_cost": format(row.entry_cost, "f"),
                "exit_proceeds": format(row.exit_proceeds, "f"),
                "settlement_payout": format(row.settlement_payout, "f"),
                "fees": format(row.fees, "f"), "exit_method": row.exit_method,
                "source": row.source,
                "reconciliation_status": "not_requested_colab_reference_history",
            })
            shadow_history.append({
                **common,
                "shadow_balance_before": format(before, "f"),
                "shadow_market_pnl": format(row.realized_pnl, "f"),
                "shadow_realized_pnl": format(row.realized_pnl, "f"),
                "shadow_balance_after": format(balance, "f"),
                "shadow_selected_side": row.selected_side,
                "shadow_eligible": row.contracts_bought > ZERO,
                "shadow_skip_reason": None, "shadow_contracts": format(row.contracts_bought, "f"),
                "shadow_average_entry": None if row.average_entry is None else format(row.average_entry, "f"),
                "shadow_cost": format(row.entry_cost, "f"), "shadow_proceeds": format(row.exit_proceeds, "f"),
                "shadow_payout": format(row.settlement_payout, "f"), "shadow_fees": format(row.fees, "f"),
                "shadow_exit_method": row.exit_method, "shadow_fill_model": "account_series_replay",
                "shadow_simulation_quality": "exact_replay", "live_execution_enabled": True,
            })

        first_observed = selected[0][0]
        anchor_time = first_observed - timedelta(minutes=15)
        self.state.update({
            "historical_starting_balance": format(self.config.starting_balance, "f"),
            # Keep operational cash distinct from the notebook-model balance.
            "actual_balance": format(api_current_balance, "f"),
            "shadow_balance": format(balance, "f"),
            "actual_balance_anchor": {
                "market_ticker": "__STARTING_BALANCE__", "market_close_time": timestamp_text(anchor_time),
                "completed_at": timestamp_text(anchor_time), "actual_balance_after": format(self.config.starting_balance, "f"),
            },
            "shadow_balance_anchor": {
                "market_ticker": "__STARTING_BALANCE__", "market_close_time": timestamp_text(anchor_time),
                "completed_at": timestamp_text(anchor_time), "shadow_balance_after": format(self.config.starting_balance, "f"),
            },
            "actual_history": actual_history, "shadow_history": shadow_history,
            "processed_market_tickers": [row.market_ticker for _, row in selected],
            "forecasts": [], "future_forecast_snapshot": [], "transitions": [], "live_vs_shadow": [], "shadow_open": {},
            "last_p01": None, "last_p10": None, "last_p25": None, "last_p50": None,
            "last_p75": None, "last_p90": None, "last_p99": None,
            "forecast_generated_at": None, "forecast_target_ticker": None,
            "prophet_training_end": None, "prophet_training_rows": 0,
            "balance_source": "colab_reference_account_series_starting_balance",
            "balance_reconciled": False, "balance_reconciliation_error": "not_requested_for_colab_reference_history",
            "prophet_history_ready": True,
            "state_reason": "colab_reference_account_series_history_rebuilt",
        })
        LOG.warning(
            "COLAB ACCOUNT-SERIES HISTORY BUILT | rows=%d anchor=%s ending_shadow=%s api_cash=%s",
            len(selected), self.config.starting_balance, balance, api_current_balance,
        )
        return True

    def rebuild_colab_reference_closed_positions_csv(
        self,
        path: Path,
        *,
        api_current_balance: Decimal,
    ) -> bool:
        """Parse the supplied closed-position export exactly like the Colab notebook.

        The notebook strips column whitespace, extracts KXBTC15M ticker time,
        parses ``Total return ($)``, sorts chronologically, keeps the last
        duplicate ticker, and accumulates P/L from one $100 starting row.  Do
        those same operations here rather than trying to infer a different
        account statement from raw fills.
        """

        if not path.is_file():
            raise RuntimeError(f"Colab reference closed-positions CSV is missing: {path}")
        parsed: list[tuple[datetime, int, str, Decimal]] = []
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            fieldnames = reader.fieldnames or []
            columns = {str(name).strip(): str(name) for name in fieldnames if name is not None}
            ticker_column = columns.get("Ticker")
            return_column = next(
                (columns[name] for name in ("Total return ($)", "Total return", "Total Return ($)", "Total Return") if name in columns),
                None,
            )
            if ticker_column is None or return_column is None:
                raise RuntimeError(
                    "Colab reference CSV must contain Ticker and a Total return column; "
                    f"available={sorted(columns)}"
                )
            for source_index, record in enumerate(reader):
                ticker = str(record.get(ticker_column) or "")
                timestamp = ticker_clock_timestamp(ticker)
                cleaned = re.sub(r"[^0-9.\\-]", "", str(record.get(return_column) or ""))
                if timestamp is None or not cleaned:
                    continue
                try:
                    pnl = Decimal(cleaned)
                except InvalidOperation:
                    continue
                if not pnl.is_finite():
                    continue
                parsed.append((timestamp, source_index, ticker, pnl))
        parsed.sort(key=lambda item: (item[0], item[1]))
        deduplicated: dict[str, tuple[datetime, int, str, Decimal]] = {}
        for item in parsed:
            # pandas ``drop_duplicates(..., keep='last')`` after a stable
            # chronological sort has precisely this replacement behavior.
            deduplicated[item[2]] = item
        markets = [
            ReconstructedMarket(
                market_ticker=ticker, market_close_time=timestamp, selected_side=None,
                contracts_bought=ZERO, contracts_sold=ZERO, average_entry=None,
                entry_cost=ZERO, exit_proceeds=ZERO, settlement_payout=ZERO,
                fees=ZERO, realized_pnl=pnl, exit_method="closed_position_export",
                source="closed_positions_csv", reconciliation_status="colab_reference_export",
            )
            for timestamp, _source_index, ticker, pnl in sorted(
                deduplicated.values(), key=lambda item: (item[0], item[2]),
            )
        ]
        rebuilt = self.rebuild_colab_reference_account_series_history(
            markets, api_current_balance=api_current_balance,
        )
        if rebuilt:
            self.state["balance_source"] = "colab_reference_closed_positions_csv"
            self.state["colab_reference_csv"] = {
                "path": str(path), "closed_btc_rows": len(markets),
                "selected_rows": min(len(markets), self.config.history_max_markets),
                "starting_balance": format(self.config.starting_balance, "f"),
            }
            LOG.warning(
                "COLAB CLOSED-POSITIONS HISTORY BUILT | path=%s rows=%d selected=%d ending_shadow=%s",
                path, len(markets), min(len(markets), self.config.history_max_markets), self.state["shadow_balance"],
            )
        return rebuilt

    def save(self) -> None:
        self._bound_persisted_history()
        self._assert_state_integrity()
        self.store.save(self.state)
        self.write_outputs()

    def _bound_persisted_history(self) -> None:
        """Retain the requested recent market scope without changing equity.

        Equity is a cumulative Decimal balance, so trimming old audit rows
        never rebases it or changes a future Prophet input.  The corresponding
        processed IDs are retained over exactly the same 200-market horizon so
        a restart cannot replay a record that remains in API scope.
        """
        limit = self.config.history_max_markets
        for key, balance_key, anchor_key in (
            ("actual_history", "actual_balance_after", "actual_balance_anchor"),
            ("shadow_history", "shadow_balance_after", "shadow_balance_anchor"),
        ):
            values = self.state.get(key)
            if isinstance(values, list) and len(values) > limit:
                predecessor = values[-limit - 1]
                observed_at = observation_timestamp(predecessor)
                if observed_at is not None:
                    self.state[anchor_key] = {
                        "market_ticker": "__RETAINED_BALANCE_ANCHOR__",
                        "market_close_time": timestamp_text(observed_at),
                        "completed_at": timestamp_text(observed_at),
                        balance_key: str(predecessor.get(balance_key)),
                    }
                self.state[key] = values[-limit:]
        for key in ("actual_history", "shadow_history", "forecasts", "live_vs_shadow", "processed_market_tickers"):
            values = self.state.get(key)
            if isinstance(values, list) and len(values) > limit:
                self.state[key] = values[-limit:]

    def _assert_state_integrity(self) -> None:
        if self.state.get("actual_balance") is None or self.state.get("shadow_balance") is None:
            return
        shadow = decimal_value(self.state["shadow_balance"])
        actual = decimal_value(self.state["actual_balance"])
        if shadow <= ZERO or actual <= ZERO:
            # The live deployment's default log transform has a hard positive
            # balance invariant. A breached account must fail closed.
            raise RuntimeError("equity became non-positive; refusing to continue Prophet regime control")
        for forecast in self.state["forecasts"]:
            values = [decimal_value(forecast.get(name)) for name in QUANTILES]
            if any(value == ZERO for value in values) or values != sorted(values):
                raise RuntimeError("persisted Prophet forecast has missing or unordered quantiles")
            training_end = utc_timestamp(forecast.get("training_end"))
            target = utc_timestamp(forecast.get("forecast_target_time"))
            if training_end is not None and target is not None and training_end >= target:
                raise RuntimeError("persisted forecast violates no-look-ahead invariant")

    def execution_enabled_for_market(self) -> bool:
        return bool(self.state["execution_enabled"])

    @property
    def prophet_history_ready(self) -> bool:
        return bool_value(self.state.get("prophet_history_ready"), False)

    def should_suppress_new_live_orders(self) -> bool:
        return self.config.controls_live_execution and self.prophet_history_ready and not self.execution_enabled_for_market()

    def _shadow_rows_before(self, decision_time: datetime) -> list[dict[str, Any]]:
        rows = [
            dict(row) for row in self.state["shadow_history"]
            # A curve point becomes usable only after this controller has
            # completed accounting for it.  ``completed_at`` prevents a late
            # settlement from being smuggled into a forecast made earlier.
            if (completed := utc_timestamp(row.get("completed_at") or row.get("market_close_time"))) is not None
            and completed < decision_time
        ]
        anchor = self.state.get("shadow_balance_anchor")
        if isinstance(anchor, Mapping):
            completed = utc_timestamp(anchor.get("completed_at") or anchor.get("market_close_time"))
            if completed is not None and completed < decision_time:
                rows.append(dict(anchor))
        rows.sort(key=lambda item: (
            timestamp_text(observation_timestamp(item)),
            str(item.get("market_ticker")),
        ))
        return rows

    def prepare_forecast(self, decision: StrategyDecision) -> dict[str, Any] | None:
        for item in self.state["forecasts"]:
            if item.get("forecast_target_ticker") == decision.target_ticker:
                return item
        if not self.prophet_history_ready:
            LOG.error("PROPHET FORECAST SKIPPED | target=%s balance history is not ready", decision.target_ticker)
            return None
        training = self._shadow_rows_before(decision.generated_at)
        if not self.config.prophet_enabled or len(training) < self.config.prophet_min_history:
            return None
        model_training = (
            training[-self.config.prophet_training_window :]
            if self.config.prophet_training_window is not None
            else training
        )
        forecast_target_time = ticker_clock_timestamp(decision.target_ticker, decision.target_close_time)
        if forecast_target_time is None:
            raise RuntimeError(f"cannot derive a forecast timestamp for {decision.target_ticker}")
        markets_since_refit = int(self.state.get("markets_since_refit") or 0)
        if self.state["forecasts"] and markets_since_refit < self.config.prophet_refit_every_markets:
            # A cadence fit creates a sequential 100-market Prophet path.  A
            # prior version incorrectly copied the first row for every market
            # until the next refit.  That both ignored the requested horizon
            # and compared each later shadow balance to a prediction made for
            # a different market.  Consume row N for the Nth completed market
            # since this fit; force a new fit if the retained horizon is not
            # long enough.  The snapshot contains model predictions only, so
            # no later realized balance can enter this branch.
            snapshot = self.state.get("future_forecast_snapshot") or []
            snapshot_index = markets_since_refit
            if snapshot_index >= len(snapshot):
                LOG.warning(
                    "PROPHET HORIZON EXHAUSTED | target=%s offset=%d horizon=%d; refitting early",
                    decision.target_ticker, snapshot_index, len(snapshot),
                )
                markets_since_refit = self.config.prophet_refit_every_markets
            else:
                horizon_row = snapshot[snapshot_index]
                copied = {name: str(horizon_row[name]) for name in QUANTILES}
                horizon_row.update({
                    "used_for_live_filter": True,
                    "consumed_target_ticker": decision.target_ticker,
                    "consumed_target_time": timestamp_text(forecast_target_time),
                })
                fit_error = f"refit_deferred_reused_horizon_row_{snapshot_index + 1}"
        if not self.state["forecasts"] or markets_since_refit >= self.config.prophet_refit_every_markets:
            try:
                # Fit once to the unmodified latest 200 closed-trade balances
                # plus their opening-balance anchor (201 rows in the Colab
                # reference curve).
                # Row one is the causal next-market forecast.  The additional
                # rows are exported solely as a 100-market diagnostic path.
                forecast_horizon = getattr(self.forecaster, "forecast_horizon", None)
                if callable(forecast_horizon):
                    future_quantiles = forecast_horizon(
                        model_training,
                        forecast_target_time,
                        self.config.prophet_future_horizon_markets,
                    )
                    copied = {name: format(value, "f") for name, value in future_quantiles[0].items()}
                    generated_at = timestamp_text(datetime.now(tz=UTC))
                    sampled_timestamps = getattr(self.forecaster, "last_future_timestamps", [])
                    self.state["future_forecast_snapshot"] = [
                        {
                            "forecast_generated_at": generated_at,
                            "forecast_origin_target_ticker": decision.target_ticker,
                            "forecast_horizon_market": index,
                            "forecast_timestamp": timestamp_text(
                                sampled_timestamps[index - 1]
                                if len(sampled_timestamps) >= index
                                else forecast_target_time + timedelta(
                                    minutes=self.config.prophet_forecast_frequency_minutes * (index - 1),
                                )
                            ),
                            "used_for_live_filter": index == 1,
                            "consumed_target_ticker": decision.target_ticker if index == 1 else None,
                            "consumed_target_time": timestamp_text(forecast_target_time) if index == 1 else None,
                            "training_start": timestamp_text(observation_timestamp(model_training[0])),
                            "training_end": timestamp_text(observation_timestamp(model_training[-1])),
                            "training_rows": len(model_training),
                            **{name: format(value, "f") for name, value in values.items()},
                        }
                        for index, values in enumerate(future_quantiles, start=1)
                    ]
                else:  # lightweight test doubles retain the old one-row protocol
                    copied = {
                        name: format(value, "f")
                        for name, value in self.forecaster.forecast(model_training, forecast_target_time).items()
                    }
                fit_error = None
                self.state["markets_since_refit"] = 0
            except Exception as exc:  # noqa: BLE001 - fail closed to preserved prior forecast only
                previous = self.state["forecasts"][-1] if self.state["forecasts"] else None
                self.state["fit_failures"].append({"at": timestamp_text(datetime.now(tz=UTC)), "target": decision.target_ticker, "error": str(exc)})
                if previous is None:
                    LOG.error("PROPHET FORECAST FAILED | target=%s error=%s; no earlier forecast is available", decision.target_ticker, exc)
                    self.save()
                    return None
                copied = {name: previous[name] for name in QUANTILES}
                fit_error = f"fallback_previous_forecast: {exc}"
        training_start = observation_timestamp(model_training[0])
        training_end = observation_timestamp(model_training[-1])
        if training_end is None or training_start is None or forecast_target_time is None or training_end >= forecast_target_time:
            raise RuntimeError("refusing to save look-ahead forecast")
        forecast = {
            "forecast_generated_at": timestamp_text(datetime.now(tz=UTC)),
            "forecast_target_ticker": decision.target_ticker,
            "forecast_target_time": timestamp_text(forecast_target_time),
            "training_start": timestamp_text(training_start),
            "training_end": timestamp_text(training_end),
            "training_rows": len(model_training),
            **copied,
            "observed_shadow_balance": None,
            "entry_signal": False,
            "exit_signal": False,
            "state_before": "on" if self.execution_enabled_for_market() else "off",
            "state_after": None,
            "model_fit_error": fit_error,
            "refit_number": self.forecaster.fit_number,
        }
        self.state["forecasts"].append(forecast)
        self.state.update({
            "last_p01": forecast["p01"], "last_p10": forecast["p10"], "last_p25": forecast["p25"],
            "last_p50": forecast["p50"], "last_p75": forecast["p75"], "last_p90": forecast["p90"],
            "last_p99": forecast["p99"], "forecast_generated_at": forecast["forecast_generated_at"],
            "forecast_target_ticker": decision.target_ticker, "prophet_training_end": forecast["training_end"],
            "prophet_training_rows": forecast["training_rows"],
        })
        LOG.info(
            "PROPHET FORECAST GENERATED | target=%s training_rows=%d P10=%s P50=%s P90=%s",
            decision.target_ticker, len(model_training), forecast["p10"], forecast["p50"], forecast["p90"],
        )
        self.save()
        return forecast

    def prime_colab_reference_forecast(self) -> dict[str, Any] | None:
        """Publish the same initial 100-row forecast as the Colab notebook.

        The notebook fits immediately after loading the 201-row input rather
        than waiting for a future strategy signal.  Preloading its first
        forecast keeps heartbeat P10/P50/P90 values non-null and lets the
        first eligible next market consume that already-causal row.
        """

        if self.config.prophet_history_source != "account_series" or not self.prophet_history_ready:
            return None
        if self.state.get("forecasts"):
            return self.state["forecasts"][-1]
        values = [dict(item) for item in self.state.get("shadow_history", [])]
        anchor = self.state.get("shadow_balance_anchor")
        if isinstance(anchor, Mapping):
            values.append(dict(anchor))
        values.sort(key=lambda item: (timestamp_text(observation_timestamp(item)), str(item.get("market_ticker"))))
        if len(values) < self.config.prophet_min_history:
            return None
        last_time = observation_timestamp(values[-1])
        if last_time is None:
            return None
        target_time = last_time + timedelta(minutes=self.config.prophet_forecast_frequency_minutes)
        target_ticker = (
            f"{self.config.series_ticker}-{target_time.strftime('%y%b%d%H%M').upper()}-"
            f"{target_time.strftime('%S')}"
        )
        # ``prepare_forecast`` enforces the same no-look-ahead and
        # predictive-sample path used for live signals.  This immutable
        # placeholder is never routed to an executor.
        preload = StrategyDecision(
            target_ticker=target_ticker, source_ticker=None, selected_side="yes", eligible=False,
            skip_reason="startup_colab_forecast_preload",
            ladder_orders=(LadderOrder(Decimal("0.40"), Decimal("1"), "preload"),),
            stop_price=Decimal("0.05"), trailing_activation_gain=ZERO, trailing_retracement=ZERO,
            generated_at=target_time - timedelta(seconds=1), target_close_time=target_time,
        )
        return self.prepare_forecast(preload)

    def start_market(self, decision: StrategyDecision) -> tuple[dict[str, Any], bool]:
        """Persist the strategy decision before any current-market result exists."""

        forecast = self.prepare_forecast(decision)
        execution_before = self.execution_enabled_for_market()
        existing = self.state.get("shadow_open", {}).get(decision.target_ticker)
        if existing is None:
            shadow = self.shadow_executor.start(self._shadow_cooldown_decision(decision), execution_before)
            self.state.setdefault("shadow_open", {})[decision.target_ticker] = shadow
            self.save()
            LOG.info("SHADOW MARKET STARTED | %s side=%s model=%s", decision.target_ticker, decision.selected_side.upper(), self.config.shadow_fill_model)
        return forecast or {}, execution_before

    def _shadow_cooldown_decision(self, decision: StrategyDecision) -> StrategyDecision:
        """Apply the independent hypothetical loss breaker before a shadow market.

        The live runner owns its actual-filled breaker.  With the recommended
        ``separate`` setting, this controller owns an equivalent shadow-only
        breaker so an off-state shadow path does not inherit a frozen live
        streak.  A skipped hypothetical market remains a durable zero-P/L
        observation and is explicitly labelled rather than being omitted.
        """
        if self.config.cooldown_state_source not in {"shadow", "separate"} or not decision.eligible:
            return decision
        control = self.state.setdefault("shadow_cooldown_state", {})
        remaining = int(control.get("markets_remaining_to_skip") or 0)
        if remaining <= 0:
            return decision
        control["markets_remaining_to_skip"] = remaining - 1
        control.setdefault("skipped_markets", []).append({
            "ticker": decision.target_ticker,
            "at": timestamp_text(decision.generated_at),
            "remaining_after": remaining - 1,
        })
        control["skipped_markets"] = control["skipped_markets"][-200:]
        LOG.warning(
            "SHADOW COOLDOWN SKIP | %s remaining_after=%d; no shadow order can be filled for this market",
            decision.target_ticker,
            remaining - 1,
        )
        return replace(
            decision,
            eligible=False,
            skip_reason="shadow_two_consecutive_completed_losses",
        )

    def _update_shadow_cooldown_after_close(self, trade: Mapping[str, Any], shadow_pnl: Decimal) -> None:
        if self.config.cooldown_state_source not in {"shadow", "separate"}:
            return
        if not bool(trade.get("eligible")) or decimal_value(trade.get("contracts")) <= ZERO:
            return
        control = self.state.setdefault("shadow_cooldown_state", {})
        losses = int(control.get("consecutive_completed_losses") or 0)
        if shadow_pnl > ZERO:
            control.update({
                "consecutive_completed_losses": 0,
                "markets_remaining_to_skip": 0,
                "last_resolution": "completed_winning_shadow_trade",
            })
        elif shadow_pnl < ZERO:
            losses += 1
            control["consecutive_completed_losses"] = losses
            control["last_resolution"] = "completed_losing_shadow_trade"
            if losses >= 2:
                control["markets_remaining_to_skip"] = 2
                control["consecutive_completed_losses"] = 2
                LOG.warning(
                    "SHADOW COOLDOWN ARMED | %s has two consecutive hypothetical losses; next two shadow signals will be skipped",
                    trade.get("target_ticker"),
                )

    def observe_shadow_market(
        self,
        ticker: str,
        quote_getter: Callable[[str, str, float, float], tuple[dict[str, Any] | None, str]],
        exit_quote_getter: Callable[[str, str, float, float], tuple[dict[str, Any] | None, str]],
        public_trade_getter: Callable[[str, datetime], list[dict[str, Any]]] | None = None,
    ) -> bool:
        trade = self.state.get("shadow_open", {}).get(ticker)
        if not isinstance(trade, dict):
            return False
        changed = False
        previous_quality = trade.get("shadow_simulation_quality")
        trade_through_changed = (
            public_trade_getter is not None
            and self.shadow_executor.observe_trade_through(trade, public_trade_getter)
        )
        if trade_through_changed or trade.get("shadow_simulation_quality") != previous_quality:
            if trade_through_changed:
                LOG.info("SHADOW RUNG FILLED | %s model=conservative_trade_through", ticker)
            self.save()
            changed = True
        if self.shadow_executor.observe_touch_quote(trade, quote_getter):
            LOG.info("SHADOW RUNG FILLED | %s", ticker)
            self.save()
            changed = True
        if self.shadow_executor.observe_exit_quote(trade, exit_quote_getter):
            LOG.info("SHADOW STOP TRIGGERED | %s method=%s", ticker, trade.get("exit_method"))
            self.save()
            changed = True
        return changed

    def close_market(
        self, *, ticker: str, outcome: str | None, market_close_time: datetime,
        actual_realized_pnl: Decimal, actual_metadata: Mapping[str, Any] | None = None,
        actual_was_live: bool = False, actual_balance_after: Decimal,
    ) -> None:
        """Atomically append P/L, evaluate prior forecast, then transition next state."""

        if ticker in set(self.state["processed_market_tickers"]):
            return
        open_trades = self.state.setdefault("shadow_open", {})
        trade = open_trades.get(ticker)
        if not isinstance(trade, dict):
            # An eligible market that has no decision is still explicitly
            # represented; it cannot quietly disappear from the shadow curve.
            trade = {"target_ticker": ticker, "market_close_time": timestamp_text(market_close_time), "status": "skipped", "shadow_realized_pnl": "0", "shadow_simulation_quality": "unavailable", "shadow_fill_model": self.config.shadow_fill_model, "exit_method": "no_decision", "selected_side": None, "eligible": False, "skip_reason": "missing_strategy_decision", "contracts": "0", "entry_cost": "0", "proceeds": "0", "payout": "0", "fees": "0", "live_execution_enabled": self.execution_enabled_for_market()}
        if actual_was_live:
            metadata = dict(actual_metadata or {})
            # This is the only supported use of a real fill in the shadow
            # ledger: the exact same bot order was actually submitted. It is
            # always preferred while execution was on, irrespective of the
            # configured *off-state* simulator, so the shadow and actual
            # curves cannot diverge merely because a public trade tape has no
            # queue-position information.
            trade.update({
                "status": "finalized", "shadow_realized_pnl": format(actual_realized_pnl, "f"),
                "contracts": str(metadata.get("contracts_bought", "0")),
                "entry_cost": str(metadata.get("entry_cost", "0")),
                "proceeds": str(metadata.get("exit_proceeds", "0")),
                "payout": str(metadata.get("settlement_payout", "0")),
                "fees": str(metadata.get("fees", "0")),
                "exit_method": metadata.get("exit_method") or "settlement",
                "shadow_simulation_quality": "exact_replay", "shadow_fill_model": "live_equivalent",
            })
        self.shadow_executor.finalize(trade, outcome)
        shadow_pnl = decimal_value(trade.get("shadow_realized_pnl"))
        state_before = self.execution_enabled_for_market()
        if self.state.get("actual_balance") is None or self.state.get("shadow_balance") is None:
            raise RuntimeError("absolute balances must be initialized from the authenticated API before market accounting")
        actual_before = decimal_value(self.state["actual_balance"])
        shadow_before = decimal_value(self.state["shadow_balance"])
        actual_after = actual_balance_after
        # ``/portfolio/balance`` is the authoritative live cash balance.  A
        # recovered position may have paid its entry cost before this regime
        # process was initialized and later add only its settlement payout to
        # cash.  Mirroring the authenticated cash change while live keeps
        # actual and shadow on the same absolute balance scale; the separate
        # realized-P/L columns retain the market P/L for performance analysis.
        actual_balance_change = actual_after - actual_before
        # Colab-reference history is a $starting_balance + cumulative realized
        # P/L curve.  Continue that curve with market P/L even while orders
        # are live; never replace a synthetic model observation with the
        # portfolio cash delta (which includes entry reservation timing).
        shadow_balance_change = (
            shadow_pnl
            if self.config.prophet_history_source == "account_series"
            else (actual_balance_change if actual_was_live else shadow_pnl)
        )
        shadow_after = shadow_before + shadow_balance_change
        if actual_after <= ZERO or shadow_after <= ZERO:
            raise RuntimeError("equity update would make balance non-positive; manual intervention is required")
        actual_row = {
            "timestamp": timestamp_text(datetime.now(tz=UTC)), "market_ticker": ticker, "market_close_time": timestamp_text(market_close_time),
            "completed_at": timestamp_text(datetime.now(tz=UTC)),
            "actual_balance_before": format(actual_before, "f"),
            "actual_realized_pnl": format(actual_realized_pnl, "f"),
            "actual_balance_after": format(actual_after, "f"),
            "execution_enabled_for_market": state_before,
            "state_before_market": "on" if state_before else "off",
            "balance_source": "authenticated_kalshi_api",
            "reconciled": True,
            **dict(actual_metadata or {}),
        }
        shadow_row = {
            "timestamp": timestamp_text(datetime.now(tz=UTC)), "market_ticker": ticker, "market_close_time": timestamp_text(market_close_time),
            "completed_at": timestamp_text(datetime.now(tz=UTC)),
            "shadow_selected_side": trade.get("selected_side"), "shadow_eligible": trade.get("eligible"),
            "shadow_skip_reason": trade.get("skip_reason"), "shadow_contracts": trade.get("contracts", "0"),
            "shadow_average_entry": (
                format(decimal_value(trade.get("entry_cost")) / decimal_value(trade.get("contracts")), "f")
                if decimal_value(trade.get("contracts")) > ZERO else None
            ),
            "shadow_cost": trade.get("entry_cost", "0"), "shadow_proceeds": trade.get("proceeds", "0"),
            "shadow_payout": trade.get("payout", "0"), "shadow_fees": trade.get("fees", "0"),
            "shadow_balance_before": format(shadow_before, "f"),
            "shadow_market_pnl": format(shadow_pnl, "f"), "shadow_realized_pnl": format(shadow_pnl, "f"),
            "shadow_balance_change": format(shadow_balance_change, "f"),
            "shadow_balance_after": format(shadow_after, "f"),
            "shadow_exit_method": trade.get("exit_method"), "shadow_fill_model": trade.get("shadow_fill_model"),
            "shadow_simulation_quality": trade.get("shadow_simulation_quality"), "live_execution_enabled": state_before,
        }
        self.state["actual_history"].append(actual_row)
        self.state["shadow_history"].append(shadow_row)
        self.state["actual_balance"] = format(actual_after, "f")
        self.state["shadow_balance"] = format(shadow_after, "f")
        difference = actual_balance_change - actual_realized_pnl
        if abs(difference) > self.config.accounting_tolerance:
            adjustment = {
                "timestamp": timestamp_text(datetime.now(tz=UTC)),
                "adjustment_type": "entry_or_open_position_cash_timing",
                "amount": format(difference, "f"), "source": "portfolio_balance_after_market",
                "balance_before": format(actual_before, "f"), "balance_after": format(actual_after, "f"),
            }
            self.state["balance_adjustments"].append(adjustment)
            actual_row["reconciliation_status"] = "cash_timing_differs_from_realized_pnl"
            LOG.warning(
                "CASH TIMING ADJUSTMENT | %s balance_delta=%s realized_pnl=%s difference=%s; "
                "authenticated balance remains the absolute-balance source",
                ticker, actual_balance_change, actual_realized_pnl, difference,
            )
        self._update_shadow_cooldown_after_close(trade, shadow_pnl)
        self.state["last_processed_market_ticker"] = ticker
        self.state["processed_market_tickers"].append(ticker)
        if not state_before:
            self.state["shadow_pnl_while_disabled"] = format(decimal_value(self.state["shadow_pnl_while_disabled"]) + shadow_pnl, "f")
        forecast = next((row for row in self.state["forecasts"] if row.get("forecast_target_ticker") == ticker), None)
        if forecast:
            forecast["observed_shadow_balance"] = format(shadow_after, "f")
            p10, p90 = decimal_value(forecast["p10"]), decimal_value(forecast["p90"])
            unavailable_disabled_shadow = (
                not state_before
                and bool(trade.get("eligible"))
                and trade.get("shadow_simulation_quality") == "unavailable"
            )
            # A missing public tape is an unknown counterfactual, not a zero
            # result. Keep a stopped bot stopped in that case rather than
            # allowing an unverifiable P10 observation to restart live orders.
            entry = (not unavailable_disabled_shadow) and (not state_before) and shadow_after <= p10
            exit_signal = (not unavailable_disabled_shadow) and state_before and shadow_after >= p90
            forecast["entry_signal"] = entry
            forecast["exit_signal"] = exit_signal
            if unavailable_disabled_shadow:
                forecast["state_transition_blocked_reason"] = "shadow_fill_data_unavailable_while_execution_disabled"
                LOG.error(
                    "REGIME RESTART BLOCKED | %s has unavailable post-stop shadow fill data; retaining disabled state",
                    ticker,
                )
            desired_state = True if entry else (False if exit_signal else state_before)
            transition_requested = desired_state != state_before
            # Persist the *model's* state even in dry-run.  Dry-run therefore
            # tests a realistic sequence of stop/restart decisions; only the
            # separate controls_live_execution predicate may suppress orders.
            transition_recorded = transition_requested and self.config.enabled and self.prophet_history_ready
            live_execution_effective = transition_requested and self.config.controls_live_execution and self.prophet_history_ready
            if transition_recorded:
                self.state["execution_enabled"] = desired_state
                self.state["state_reason"] = "shadow_balance_at_or_below_p10" if entry else "shadow_balance_at_or_above_p90"
                self.state["state_changed_at"] = timestamp_text(datetime.now(tz=UTC))
                if desired_state:
                    disabled_since = utc_timestamp(self.state.get("disabled_since"))
                    disabled_seconds = (datetime.now(tz=UTC) - disabled_since).total_seconds() if disabled_since else None
                    self.state["disabled_since"] = None
                    LOG.warning("P10 LIVE EXECUTION RESTART | mode=%s next_market_after=%s shadow=%s P10=%s P50=%s P90=%s actual=%s disabled_seconds=%s shadow_pnl_while_disabled=%s", "LIVE_CONTROL" if self.config.controls_live_execution else "DRY_RUN", ticker, shadow_after, p10, forecast["p50"], p90, actual_after, disabled_seconds, self.state["shadow_pnl_while_disabled"])
                else:
                    self.state["disabled_since"] = timestamp_text(datetime.now(tz=UTC))
                    LOG.warning("P90 LIVE EXECUTION STOP | mode=%s next_market_after=%s shadow=%s P10=%s P50=%s P90=%s actual=%s", "LIVE_CONTROL" if self.config.controls_live_execution else "DRY_RUN", ticker, shadow_after, p10, forecast["p50"], p90, actual_after)
            if transition_requested:
                self.state["transitions"].append({
                    "transition_time": timestamp_text(datetime.now(tz=UTC)), "effective_market": "next_eligible_market",
                    "signal_market": ticker, "old_state": "on" if state_before else "off",
                    "new_state": "on" if desired_state else "off",
                    "reason": "shadow_balance_at_or_below_p10" if entry else "shadow_balance_at_or_above_p90",
                    "applied": transition_recorded, "live_order_suppression_effective": live_execution_effective, "actual_balance": format(actual_after, "f"),
                    "shadow_balance": format(shadow_after, "f"), "p10": forecast["p10"], "p50": forecast["p50"], "p90": forecast["p90"],
                    "markets_disabled": sum(not bool(row.get("live_execution_enabled")) for row in self.state["shadow_history"]),
                    "shadow_pnl_during_disabled_period": self.state["shadow_pnl_while_disabled"],
                })
            forecast["state_after"] = "on" if self.execution_enabled_for_market() else "off"
            actual_row.update({
                "entry_signal_after_market": forecast["entry_signal"],
                "exit_signal_after_market": forecast["exit_signal"],
                "state_after_market": forecast["state_after"],
                "p10": forecast["p10"], "p50": forecast["p50"], "p90": forecast["p90"],
            })
        else:
            actual_row.update({
                "entry_signal_after_market": False, "exit_signal_after_market": False,
                "state_after_market": "on" if self.execution_enabled_for_market() else "off",
            })
        self.state["live_vs_shadow"].append({
            "market_ticker": ticker, "market_close_time": timestamp_text(market_close_time),
            "actual_realized_pnl": format(actual_realized_pnl, "f"), "shadow_realized_pnl": format(shadow_pnl, "f"),
            "difference": format(shadow_pnl - actual_realized_pnl, "f"), "live_execution_enabled": state_before,
            "shadow_simulation_quality": trade.get("shadow_simulation_quality"),
        })
        open_trades.pop(ticker, None)
        self.state["markets_since_refit"] = int(self.state.get("markets_since_refit") or 0) + 1
        self.save()
        LOG.info("ACTUAL BALANCE UPDATE | %s pnl=%s balance=%s", ticker, actual_realized_pnl, actual_after)
        LOG.info("SHADOW BALANCE UPDATE | %s pnl=%s balance_change=%s balance=%s", ticker, shadow_pnl, shadow_balance_change, shadow_after)

    def bootstrap_from_live_ledger(
        self,
        trader_state: Mapping[str, Any],
        *,
        api_current_balance: Decimal,
    ) -> int:
        """Rebuild the latest absolute 200-balance path from the bot ledger.

        This is deliberately an *endpoint-anchored* reconstruction: the
        authenticated balance is the balance after the newest completed
        market; walking the ledger P/L backward derives the verified opening
        balance for this exact scope.  It is not a $100 rebase and it never
        adds historical P/L to the current balance.  A filled open position
        makes the endpoint ambiguous, so the method refuses to run then.
        """

        if not self.config.allow_endpoint_anchored_ledger_bootstrap:
            return 0
        if api_current_balance <= ZERO:
            raise RuntimeError("authenticated current balance must be positive for ledger bootstrap")
        if self.balance_reconciled and len(self.state.get("shadow_history", [])) >= self.config.prophet_min_history:
            return 0
        markets = trader_state.get("markets") if isinstance(trader_state, Mapping) else None
        if not isinstance(markets, Mapping):
            LOG.error("ABSOLUTE LEDGER BOOTSTRAP SKIPPED | no durable bot market ledger")
            return 0
        terminal = {
            "finalized", "finalized_unfilled", "finalized_no_signal", "exited_early",
            "entry_skipped_loss_circuit_breaker", "signal_window_missed",
        }
        for record in markets.values():
            if not isinstance(record, Mapping):
                continue
            filled = sum(
                (decimal_value(order.get("fill_count")) for order in (record.get("orders") or {}).values() if isinstance(order, Mapping)),
                ZERO,
            )
            exchange_position = abs(decimal_value(record.get("exchange_position_contracts")))
            # Only a *non-terminal* market can make the current endpoint
            # ambiguous.  Old finalized ledger rows can retain a stale
            # exchange-position snapshot (for example ``-1`` from a market
            # settled days ago); treating that archival value as live
            # exposure incorrectly prevents a later 200-market bootstrap and
            # leaves all Prophet bands null.  Closed rows below are still
            # included only through their finalized realized P/L.
            if str(record.get("status")) not in terminal and (
                exchange_position > Decimal("0.004") or filled > Decimal("0.004")
            ):
                LOG.warning(
                    "ABSOLUTE LEDGER BOOTSTRAP DEFERRED | filled open position ticker=%s filled=%s exchange=%s",
                    record.get("ticker"), filled, exchange_position,
                )
                return 0
        rows: list[tuple[datetime, str, Mapping[str, Any], Decimal]] = []
        for key, record in markets.items():
            if not isinstance(record, Mapping) or str(record.get("status")) not in terminal:
                continue
            # The supplied Kalshi closed-position export contains one row per
            # executed closed position.  Do not substitute watching, skipped,
            # or zero-contract unfilled market attempts as flat observations;
            # those extra timestamps materially change Prophet's fit.
            contracts = abs(decimal_value(record.get("contracts")))
            entry_cost = abs(decimal_value(record.get("total_cost")))
            realized_pnl = decimal_value(record.get("net_profit_loss"))
            if contracts <= Decimal("0.004") and entry_cost <= Decimal("0.0001") and realized_pnl == ZERO:
                continue
            closed = utc_timestamp(
                record.get("market_close_time")
                or record.get("settled_at")
                or record.get("exited_at")
                or record.get("closed_at")
            )
            ticker = str(record.get("ticker") or key)
            if closed is None or not ticker.startswith(self.config.series_ticker):
                continue
            rows.append((closed, ticker, record, realized_pnl))
        rows.sort(key=lambda item: (item[0], item[1]))
        rows = rows[-self.config.history_max_markets :]
        if len(rows) < self.config.prophet_min_history:
            LOG.warning(
                "ABSOLUTE LEDGER BOOTSTRAP SKIPPED | completed_rows=%d requires=%d",
                len(rows), self.config.prophet_min_history,
            )
            return 0
        total_pnl = sum((row[3] for row in rows), ZERO)
        historical_start = api_current_balance - total_pnl
        if historical_start <= ZERO:
            raise RuntimeError(
                "endpoint-anchored ledger balance before the selected history is non-positive; refusing Prophet bootstrap"
            )
        actual = shadow = historical_start
        actual_history: list[dict[str, Any]] = []
        shadow_history: list[dict[str, Any]] = []
        for close_time, ticker, record, pnl in rows:
            actual_before = actual
            shadow_before = shadow
            actual += pnl
            shadow += pnl
            closed_text = timestamp_text(close_time)
            status = str(record.get("status") or "")
            selected_side = record.get("locked_side") or record.get("candidate_side")
            contracts = decimal_value(record.get("contracts"))
            entry_cost = decimal_value(record.get("total_cost"))
            proceeds = decimal_value(record.get("gross_payout")) if status == "exited_early" else ZERO
            payout = decimal_value(record.get("gross_payout")) if status != "exited_early" else ZERO
            fees = decimal_value(record.get("kalshi_fees"))
            source = "durable_live_bot_ledger_endpoint_anchored"
            actual_history.append({
                "timestamp": closed_text, "market_ticker": ticker, "market_close_time": closed_text,
                "actual_balance_before": format(actual_before, "f"), "actual_realized_pnl": format(pnl, "f"),
                "actual_balance_after": format(actual, "f"), "execution_enabled_for_market": True,
                "state_before_market": "on", "entry_signal_after_market": False, "exit_signal_after_market": False,
                "state_after_market": "on", "p10": None, "p50": None, "p90": None,
                "balance_source": source, "reconciled": True, "selected_side": selected_side,
                "contracts_bought": format(contracts, "f"), "contracts_sold": "0",
                "average_entry": record.get("average_entry"), "entry_cost": format(entry_cost, "f"),
                "exit_proceeds": format(proceeds, "f"), "settlement_payout": format(payout, "f"),
                "fees": format(fees, "f"), "exit_method": record.get("exit_method") or "settlement",
                "source": source, "reconciliation_status": "endpoint_anchored_to_authenticated_balance",
                "completed_at": timestamp_text(record.get("closed_at") or record.get("settled_at") or close_time),
            })
            shadow_history.append({
                "timestamp": closed_text, "market_ticker": ticker, "market_close_time": closed_text,
                "shadow_balance_before": format(shadow_before, "f"), "shadow_market_pnl": format(pnl, "f"),
                "shadow_realized_pnl": format(pnl, "f"), "shadow_balance_change": format(pnl, "f"),
                "shadow_balance_after": format(shadow, "f"), "shadow_selected_side": selected_side,
                "shadow_eligible": status not in {"finalized_unfilled", "finalized_no_signal", "entry_skipped_loss_circuit_breaker", "signal_window_missed"},
                "shadow_skip_reason": status if status in {"finalized_unfilled", "finalized_no_signal", "entry_skipped_loss_circuit_breaker", "signal_window_missed"} else None,
                "shadow_contracts": format(contracts, "f"), "shadow_average_entry": record.get("average_entry"),
                "shadow_cost": format(entry_cost, "f"), "shadow_proceeds": format(proceeds, "f"),
                "shadow_payout": format(payout, "f"), "shadow_fees": format(fees, "f"),
                "shadow_exit_method": record.get("exit_method") or "settlement", "shadow_fill_model": "live_equivalent",
                "shadow_simulation_quality": "exact_replay", "live_execution_enabled": True,
                "completed_at": timestamp_text(record.get("closed_at") or record.get("settled_at") or close_time),
            })
        if abs(actual - api_current_balance) > self.config.accounting_tolerance:
            raise RuntimeError("endpoint-anchored balance reconstruction did not reconcile to authenticated balance")
        self.state.update({
            "historical_starting_balance": format(historical_start, "f"),
            "actual_balance": format(api_current_balance, "f"), "shadow_balance": format(api_current_balance, "f"),
            "actual_balance_anchor": {
                "market_ticker": "__STARTING_BALANCE__",
                "market_close_time": timestamp_text(
                    ticker_clock_timestamp(rows[0][1], rows[0][0]) - timedelta(minutes=15)
                ),
                "completed_at": timestamp_text(
                    ticker_clock_timestamp(rows[0][1], rows[0][0]) - timedelta(minutes=15)
                ),
                "actual_balance_after": format(historical_start, "f"),
            },
            "shadow_balance_anchor": {
                "market_ticker": "__STARTING_BALANCE__",
                "market_close_time": timestamp_text(
                    ticker_clock_timestamp(rows[0][1], rows[0][0]) - timedelta(minutes=15)
                ),
                "completed_at": timestamp_text(
                    ticker_clock_timestamp(rows[0][1], rows[0][0]) - timedelta(minutes=15)
                ),
                "shadow_balance_after": format(historical_start, "f"),
            },
            "actual_history": actual_history, "shadow_history": shadow_history,
            "processed_market_tickers": [ticker for _, ticker, _, _ in rows],
            "forecasts": [], "future_forecast_snapshot": [], "transitions": [], "live_vs_shadow": [], "shadow_open": {},
            "balance_source": "authenticated_endpoint_anchored_durable_live_bot_ledger",
            "balance_reconciled": True, "balance_reconciliation_error": None,
            "prophet_history_ready": True,
            "state_reason": "absolute_200_market_ledger_bootstrap",
            "ledger_endpoint_bootstrap": {
                "authenticated_ending_balance": format(api_current_balance, "f"),
                "derived_historical_starting_balance": format(historical_start, "f"),
                "included_markets": len(rows), "included_realized_pnl": format(total_pnl, "f"),
                "completed_at": timestamp_text(datetime.now(tz=UTC)),
                "assumption": "no deposits_withdrawals_transfers_or_filled_open_positions_within_selected_ledger_scope",
            },
        })
        LOG.warning(
            "ABSOLUTE LEDGER BOOTSTRAP COMPLETE | rows=%d start=%s ending_api_balance=%s pnl=%s; "
            "assumption=no external account adjustments in selected scope",
            len(rows), historical_start, api_current_balance, total_pnl,
        )
        return len(rows)

    def bootstrap_from_api_reconstruction(self, rows: Iterable[ReconstructedMarket]) -> int:
        raise RuntimeError(
            "API P/L reconstruction is not an absolute balance curve without "
            "a verified historical opening balance; refusing to rebase it."
        )

    def heartbeat(self) -> dict[str, Any]:
        last_forecast = self.state["forecasts"][-1] if self.state["forecasts"] else {}
        shadow = decimal_value(self.state["shadow_balance"]) if self.state.get("shadow_balance") else ZERO
        p10, p90 = decimal_value(last_forecast.get("p10")), decimal_value(last_forecast.get("p90"))
        result = {
            "actual_balance": self.state["actual_balance"], "shadow_balance": self.state["shadow_balance"],
            "execution_enabled": self.execution_enabled_for_market(), "shadow_enabled": True,
            "balance_reconciled": self.balance_reconciled,
            "p10": last_forecast.get("p10"), "p50": last_forecast.get("p50"), "p90": last_forecast.get("p90"),
            "distance_to_p10": format(shadow - p10, "f") if p10 else None,
            "distance_to_p90": format(p90 - shadow, "f") if p90 else None,
            "markets_since_refit": int(self.state.get("markets_since_refit") or 0), "markets_while_disabled": sum(not bool(row.get("live_execution_enabled")) for row in self.state["shadow_history"]),
            "shadow_pnl_while_disabled": self.state["shadow_pnl_while_disabled"],
            "last_actual_trade": self.state["actual_history"][-1].get("market_ticker") if self.state["actual_history"] else None,
            "last_shadow_trade": self.state["shadow_history"][-1].get("market_ticker") if self.state["shadow_history"] else None,
        }
        LOG.info("EQUITY REGIME HEARTBEAT | %s", json.dumps(result, sort_keys=True))
        return result

    def write_outputs(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        actual_columns = ("timestamp", "market_ticker", "market_close_time", "actual_balance_before", "actual_realized_pnl", "actual_balance_after", "execution_enabled_for_market", "state_before_market", "entry_signal_after_market", "exit_signal_after_market", "state_after_market", "p10", "p50", "p90", "balance_source", "reconciled", "selected_side", "contracts_bought", "contracts_sold", "average_entry", "entry_cost", "exit_proceeds", "settlement_payout", "fees", "exit_method", "source", "reconciliation_status")
        shadow_columns = ("timestamp", "market_ticker", "market_close_time", "shadow_balance_before", "shadow_market_pnl", "shadow_realized_pnl", "shadow_balance_change", "shadow_balance_after", "shadow_selected_side", "shadow_eligible", "shadow_skip_reason", "shadow_contracts", "shadow_average_entry", "shadow_cost", "shadow_proceeds", "shadow_payout", "shadow_fees", "shadow_exit_method", "shadow_fill_model", "shadow_simulation_quality", "live_execution_enabled")
        forecast_columns = ("forecast_generated_at", "forecast_target_ticker", "training_start", "training_end", "training_rows", *QUANTILES, "observed_shadow_balance", "entry_signal", "exit_signal", "state_before", "state_after")
        future_forecast_columns = ("forecast_generated_at", "forecast_origin_target_ticker", "forecast_horizon_market", "forecast_timestamp", "used_for_live_filter", "training_start", "training_end", "training_rows", *QUANTILES)
        transition_columns = ("transition_time", "effective_market", "old_state", "new_state", "reason", "actual_balance", "shadow_balance", "p10", "p50", "p90", "markets_disabled", "shadow_pnl_during_disabled_period")
        self._write_csv(self.output_dir / "actual_equity_curve.csv", self.state["actual_history"], actual_columns)
        self._write_csv(self.output_dir / "shadow_equity_curve.csv", self.state["shadow_history"], shadow_columns)
        self._write_csv(self.output_dir / "prophet_forecasts.csv", self.state["forecasts"], forecast_columns)
        self._write_csv(self.output_dir / "prophet_future_100.csv", self.state.get("future_forecast_snapshot", []), future_forecast_columns)
        self._write_csv(self.output_dir / "regime_transitions.csv", self.state["transitions"], transition_columns)
        self._write_csv(self.output_dir / "live_vs_shadow_trades.csv", self.state["live_vs_shadow"])
        self._write_csv(self.output_dir / "accounting_reconciliation.csv", self.state.get("reconciliation", []))
        # Durable source histories live beside the atomic state; output copies
        # are kept separately as run artifacts for Actions.
        self._write_csv(self.store.path.parent / "kalshi_actual_equity_history.csv", self.state["actual_history"], actual_columns)
        self._write_csv(self.store.path.parent / "kalshi_shadow_equity_history.csv", self.state["shadow_history"], shadow_columns)
        self._write_report(self.output_dir / "equity_regime_report.md")
        self._write_charts()

    @staticmethod
    def _write_csv(path: Path, rows: list[Mapping[str, Any]], required_columns: Iterable[str] = ()) -> None:
        columns: list[str] = list(required_columns)
        for row in rows:
            for key in row:
                if key not in columns:
                    columns.append(str(key))
        with tempfile.NamedTemporaryFile("w", newline="", encoding="utf-8", dir=path.parent, delete=False) as handle:
            temporary = Path(handle.name)
            writer = csv.DictWriter(handle, fieldnames=columns)
            writer.writeheader()
            for row in rows:
                writer.writerow({key: row.get(key) for key in columns})
        os.replace(temporary, path)

    def _write_report(self, path: Path) -> None:
        actual_balance = decimal_value(self.state["actual_balance"]) if self.state.get("actual_balance") else ZERO
        shadow_balance = decimal_value(self.state["shadow_balance"]) if self.state.get("shadow_balance") else ZERO
        transitions = self.state["transitions"]
        stops = sum(item.get("new_state") == "off" and item.get("applied") for item in transitions)
        restarts = sum(item.get("new_state") == "on" and item.get("applied") for item in transitions)
        effective_stops = sum(item.get("new_state") == "off" and item.get("live_order_suppression_effective") for item in transitions)
        effective_restarts = sum(item.get("new_state") == "on" and item.get("live_order_suppression_effective") for item in transitions)
        quality = defaultdict(int)
        for row in self.state["shadow_history"]:
            quality[str(row.get("shadow_simulation_quality") or "unknown")] += 1
        sync = self.state.get("history_sync", {})
        reconciliation = self.state.get("reconciliation", [])
        discrepancies = sum(row.get("reconciliation_status") == "discrepancy_exceeds_tolerance" for row in reconciliation if isinstance(row, Mapping))
        forecast_rows = [row for row in self.state["forecasts"] if row.get("observed_shadow_balance") is not None]
        def coverage(lower: str, upper: str) -> str:
            if not forecast_rows:
                return "n/a"
            observed = [decimal_value(row["observed_shadow_balance"]) for row in forecast_rows]
            inside = [decimal_value(row[lower]) <= value <= decimal_value(row[upper]) for row, value in zip(forecast_rows, observed, strict=True)]
            return f"{sum(inside) / len(inside):.1%} ({sum(inside)}/{len(inside)})"
        transition_table = "\n".join(
            f"| {item.get('signal_market')} | {item.get('old_state')} | {item.get('new_state')} | {item.get('reason')} | {item.get('applied')} |"
            for item in transitions
        ) or "| — | — | — | — | — |"
        content = f"""# Kalshi Equity-Regime Report

## Safety status

- Regime enabled: `{self.config.enabled}`
- Dry run: `{self.config.dry_run}`
- Live state transitions allowed: `{self.config.allow_live_state_transitions}`
- Live order suppression can control execution: `{self.config.controls_live_execution}`
- Prophet training window: `{self.config.prophet_training_window}` latest absolute-balance markets (no rebasing)
- Prophet minimum history: `{self.config.prophet_min_history}` markets
- Diagnostic future horizon: `{self.config.prophet_future_horizon_markets}` markets; only horizon 1 controls the P10/P90 gate
- Shadow fill model: `{self.config.shadow_fill_model}`

## Accounting and history

- Actual balance: `${money_text(actual_balance)}`
- Shadow balance: `${money_text(shadow_balance)}`
- Balance source: `{self.state.get('balance_source')}`
- Balance reconciled to authenticated API: `{self.balance_reconciled}`
- Actual history rows: `{len(self.state['actual_history'])}`
- Shadow history rows: `{len(self.state['shadow_history'])}`
- Ambiguous fills excluded: `{len(self.state['ambiguous_fills'])}`
- Prophet fit failures/fallbacks: `{len(self.state['fit_failures'])}`
- API historical/live cutoff: `{sync.get('historical_live_cutoff', 'not synced')}`
- API fills retrieved: `{sync.get('fills', 0)}`; settlements: `{sync.get('settlements', 0)}`
- Duplicate fills removed: `{sync.get('duplicate_fills_removed', 0)}`
- Reconciliation discrepancies above `${money_text(self.config.accounting_tolerance)}`: `{discrepancies}`

## Regime state

- Recorded P90 stops: `{stops}` (live-effective: `{effective_stops}`)
- Recorded P10 restarts: `{restarts}` (live-effective: `{effective_restarts}`)
- Current live execution state: `{'enabled' if self.execution_enabled_for_market() else 'disabled'}`
- Shadow processing: `enabled` (invariant)
- Hypothetical P/L while disabled: `${money_text(decimal_value(self.state['shadow_pnl_while_disabled']))}`

## Forecast calibration

- Completed one-step-ahead forecasts: `{len(forecast_rows)}`
- P01–P99 empirical coverage: `{coverage('p01', 'p99')}` (nominal 98%)
- P10–P90 empirical coverage: `{coverage('p10', 'p90')}` (nominal 80%)
- P25–P75 empirical coverage: `{coverage('p25', 'p75')}` (nominal 50%)

## State transitions

| Signal market | Before | After | Reason | Persisted regime state |
| --- | --- | --- | --- | --- |
{transition_table}

## Shadow simulation quality

{chr(10).join(f'- `{name}`: {count}' for name, count in sorted(quality.items())) or '- No completed shadow markets.'}

`conservative_trade_through` consumes Kalshi's timestamped public `trade` stream only after the immutable order-decision time. It consumes no more than each public trade's reported volume, but cannot observe queue position, so it remains a `conservative_approximation`, not an exact replay. If that tape is missing across a reconnect, the outcome is `unavailable` rather than an invented resting fill, and it cannot trigger a P10 restart. `touch` uses post-decision BBO and is explicitly more optimistic. Existing real bot ledger rows are labelled `exact_replay`; they are not hypothetical fills.

## Limitations and reconciliation

The authenticated Kalshi account balance is the actual-balance source. Deposits, withdrawals, transfers, unrelated trades, and open exposure are recorded separately in the adjustment ledger and never relabelled as strategy P/L. A discrepancy beyond the configured tolerance invalidates forecasts and prevents live regime control. P/L and fills are retained as `Decimal` values internally. Shadow results obtained without a timestamped order-book/trade replay are not exact and must not be used as proof of live profitability.

## State machine

For market *t*, the state saved before it controls real order placement. After the shadow result for *t* is finalized, the saved one-step-ahead forecast is evaluated. If state was on and shadow equity is at or above P90, state becomes off for the next eligible market. If state was off and shadow equity is at or below P10, it becomes on for the next eligible market. The signal market is never retroactively included or excluded.
"""
        AtomicJsonStore(path, lambda: {}).path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

    def _write_charts(self) -> None:
        try:
            import matplotlib
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt
        except ImportError:  # pragma: no cover
            return
        actual_rows = self.state["actual_history"]
        shadow_rows = self.state["shadow_history"]
        if not actual_rows and not shadow_rows:
            return
        dates = [utc_timestamp(row.get("market_close_time")) for row in shadow_rows]
        with plt.style.context("seaborn-v0_8-whitegrid"):
            figure, axis = plt.subplots(figsize=(14, 7), dpi=180)
            if actual_rows:
                axis.plot([utc_timestamp(row.get("market_close_time")) for row in actual_rows], [float(decimal_value(row.get("actual_balance_after"))) for row in actual_rows], label="Actual balance", linewidth=2)
            if shadow_rows:
                axis.plot(dates, [float(decimal_value(row.get("shadow_balance_after"))) for row in shadow_rows], label="Shadow balance", linewidth=2)
            axis.set(title="Actual versus shadow balance", xlabel="UTC market close", ylabel="Balance ($)")
            axis.legend()
            figure.autofmt_xdate()
            figure.tight_layout()
            figure.savefig(self.output_dir / "equity_curves.png")
            plt.close(figure)
            forecasts = [row for row in self.state["forecasts"] if row.get("observed_shadow_balance") is not None]
            if forecasts:
                figure, axis = plt.subplots(figsize=(14, 7), dpi=180)
                x = [utc_timestamp(row.get("forecast_target_time")) for row in forecasts]
                p10 = [float(decimal_value(row.get("p10"))) for row in forecasts]
                p50 = [float(decimal_value(row.get("p50"))) for row in forecasts]
                p90 = [float(decimal_value(row.get("p90"))) for row in forecasts]
                observed = [float(decimal_value(row.get("observed_shadow_balance"))) for row in forecasts]
                axis.fill_between(x, p10, p90, alpha=0.2, label="P10–P90")
                axis.plot(x, p10, linestyle="--", label="P10")
                axis.plot(x, p50, label="P50")
                axis.plot(x, p90, linestyle="--", label="P90")
                axis.plot(x, observed, color="black", linewidth=2, label="Shadow balance")
                axis.set(title="Shadow balance with one-step-ahead Prophet bands", xlabel="UTC target close", ylabel="Balance ($)")
                axis.legend()
                figure.autofmt_xdate()
                figure.tight_layout()
                figure.savefig(self.output_dir / "shadow_equity_with_p10_p90.png")
                plt.close(figure)


class KalshiRawHistoryAPI:
    """Adapter for the existing authenticated KalshiREST client."""

    def __init__(self, rest: Any) -> None:
        self.rest = rest

    async def get_json(self, path: str, params: Mapping[str, Any] | None = None) -> Mapping[str, Any]:
        getter = getattr(self.rest, "get_raw_json", None)
        if not callable(getter):
            raise RuntimeError("existing KalshiREST does not expose get_raw_json")
        return await getter(path, params)


async def synchronize_history(controller: EquityRegimeController, api: JsonAPI, trader_state: Mapping[str, Any]) -> HistorySyncResult:
    result = await HistoricalSynchronizer(controller.config, trader_state).sync(api)
    controller.state["ambiguous_fills"] = result.ambiguous_fills
    # Preserve the exact normalized records used (and deliberately excluded)
    # by this read-only reconciliation.  These artifacts make an accounting
    # mismatch auditable without allowing a weak ticker match to enter the
    # bot's P/L curve.
    controller.output_dir.mkdir(parents=True, exist_ok=True)
    controller._write_csv(controller.output_dir / "api_owned_fills.csv", result.owned_fill_audit)
    controller._write_csv(controller.output_dir / "api_owned_settlements.csv", result.owned_settlement_audit)
    controller._write_csv(controller.output_dir / "api_ambiguous_fills.csv", result.ambiguous_fills)
    controller._write_csv(
        controller.output_dir / "api_ambiguous_fill_settlements.csv",
        result.ambiguous_settlement_audit,
    )
    reconstructed = reconstruct_realized_pnl(result.fills, result.settlements)[-controller.config.history_max_markets :]
    # The Colab reference CSV contains only closed positions.  A fill in an
    # active market must never become a premature balance observation merely
    # because it already appears in /portfolio/fills.
    settled_series_tickers = {item.ticker for item in result.series_settlements}
    series_reconstructed = [
        row for row in reconstruct_realized_pnl(result.series_fills, result.series_settlements)
        if row.market_ticker in settled_series_tickers
    ]
    ledger_pnl = {
        str(record.get("ticker")): decimal_value(record.get("net_profit_loss"))
        for record in (trader_state.get("markets") or {}).values()
        if isinstance(record, Mapping)
        and str(record.get("status")) in {"finalized", "exited_early", "finalized_unfilled", "finalized_no_signal"}
        and record.get("ticker")
    }
    reconciliation: list[dict[str, Any]] = []
    api_market_tickers: set[str] = set()
    for row in reconstructed:
        api_market_tickers.add(row.market_ticker)
        ledger_value = ledger_pnl.get(row.market_ticker)
        difference = None if ledger_value is None else row.realized_pnl - ledger_value
        reconciliation.append({
            "market_ticker": row.market_ticker, "market_close_time": timestamp_text(row.market_close_time),
            "api_realized_pnl": format(row.realized_pnl, "f"), "ledger_realized_pnl": None if ledger_value is None else format(ledger_value, "f"),
            "difference": None if difference is None else format(difference, "f"),
            "entry_cost": format(row.entry_cost, "f"),
            "exit_proceeds": format(row.exit_proceeds, "f"), "settlement_payout": format(row.settlement_payout, "f"),
            "fees": format(row.fees, "f"),
            "reconciliation_status": (
                "not_in_local_ledger" if difference is None
                else ("within_tolerance" if abs(difference) <= controller.config.accounting_tolerance else "discrepancy_exceeds_tolerance")
            ),
        })
    for ticker, ledger_value in ledger_pnl.items():
        if ticker not in api_market_tickers:
            reconciliation.append({
                "market_ticker": ticker, "market_close_time": None, "api_realized_pnl": None,
                "ledger_realized_pnl": format(ledger_value, "f"), "difference": None,
                "reconciliation_status": "missing_from_api_history",
            })
    balance_payload = result.balance_payload or {}
    balance_dollars = value_from(balance_payload, "balance_dollars")
    # Legacy portfolio responses expose integer cents in ``balance``.
    raw_balance = balance_dollars if balance_dollars is not None else value_from(balance_payload, "balance")
    if raw_balance is None:
        raise RuntimeError("/portfolio/balance returned no usable balance; refusing regime initialization")
    balance = decimal_value(balance_dollars) if balance_dollars is not None else decimal_value(raw_balance) / Decimal("100")
    if controller.config.prophet_history_source == "account_series":
        controller.migrate_legacy_rebased_state(balance)
        if controller.config.prophet_reference_closed_positions_path is not None:
            controller.rebuild_colab_reference_closed_positions_csv(
                controller.config.prophet_reference_closed_positions_path,
                api_current_balance=balance,
            )
        else:
            controller.rebuild_colab_reference_account_series_history(
                series_reconstructed,
                api_current_balance=balance,
            )
    elif controller.config.historical_starting_balance is not None:
        controller.migrate_legacy_rebased_state(balance)
        controller.rebuild_absolute_history(
            reconstructed,
            historical_starting_balance=controller.config.historical_starting_balance,
            api_current_balance=balance,
        )
    else:
        LOG.warning(
            "HISTORICAL ABSOLUTE CURVE UNAVAILABLE | no verified historical_starting_balance; "
            "initializing both balances from authenticated API and withholding Prophet control until new history accrues",
        )
        controller.initialize_absolute_balances(balance, reason="authenticated_api_history_sync")
        controller.bootstrap_from_live_ledger(
            trader_state,
            api_current_balance=balance,
        )
    if controller.config.prophet_history_source == "account_series":
        controller.prime_colab_reference_forecast()
    current_actual = decimal_value(controller.state["actual_balance"]) if controller.state.get("actual_balance") else None
    current_difference = None if current_actual is None else current_actual - balance
    reconciliation.append({
        "market_ticker": "__account_balance_reconciliation__", "market_close_time": None,
        "api_balance": format(balance, "f"), "actual_balance": controller.state["actual_balance"],
        "shadow_balance": controller.state["shadow_balance"],
        "difference": None if current_difference is None else format(current_difference, "f"),
        "reconciliation_status": "within_tolerance" if controller.balance_reconciled else "discrepancy_exceeds_tolerance",
    })
    controller.state["reconciliation"] = reconciliation
    controller.state["history_sync"] = {
        "historical_live_cutoff": timestamp_text(result.cutoff), "fills": len(result.fills),
        "settlements": len(result.settlements), "duplicate_fills_removed": result.duplicate_fills_removed,
        "ambiguous_fills": len(result.ambiguous_fills),
        "series_fills": len(result.series_fills), "series_settlements": len(result.series_settlements),
        "prophet_history_source": controller.config.prophet_history_source,
    }
    if result.fills:
        controller.state["last_processed_fill_id"] = result.fills[-1].fill_id or None
    if result.settlements:
        controller.state["last_processed_settlement_ticker"] = result.settlements[-1].ticker
    controller.save()
    LOG.info("API HISTORY SYNC | fills=%d settlements=%d duplicates=%d ambiguous=%d cutoff=%s", len(result.fills), len(result.settlements), result.duplicate_fills_removed, len(result.ambiguous_fills), timestamp_text(result.cutoff))
    return result


async def _reconcile_command(args: argparse.Namespace) -> int:
    from kalshi_btc15m_average_down import KalshiREST, default_state, load_json

    config_values = json.loads(args.config.read_text(encoding="utf-8")) if args.config.exists() else {}
    # ``starting_balance`` is the caller's explicit, verified balance at the
    # beginning of the requested historical reconstruction.  The
    # reconciliation command must pass it through as the historical anchor;
    # otherwise RegimeConfig correctly withholds the balance curve but the CLI
    # appears to have accepted an anchor it never used.
    config_values.update({
        "equity_regime_enabled": True,
        "equity_regime_dry_run": True,
        "starting_balance": args.starting_balance,
        "historical_starting_balance": args.starting_balance,
    })
    config = RegimeConfig.from_mapping(config_values)
    controller = EquityRegimeController(config, args.regime_state, args.output_dir)
    trader_state = load_json(args.trader_state, default_state())
    api_key = os.getenv("KALSHI_API_KEY_ID", "")
    pem_path = Path(os.getenv("KALSHI_PEM_PATH", "kalshi_private_key.pem"))
    if not api_key or not pem_path.exists():
        raise SystemExit("KALSHI_API_KEY_ID and KALSHI_PEM_PATH are required for API reconciliation")
    rest = KalshiREST(api_key, pem_path, bool_value(os.getenv("KALSHI_DEMO", "false")))
    try:
        await synchronize_history(controller, KalshiRawHistoryAPI(rest), trader_state)
        controller.save()
    finally:
        await rest.close()
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    reconcile = commands.add_parser("reconcile", help="fetch and reconcile bot-owned Kalshi historical fills/settlements")
    reconcile.add_argument("--config", type=Path, default=Path("kalshi_btc15m_average_down_config.json"))
    reconcile.add_argument("--trader-state", type=Path, default=Path("kalshi_btc15m_average_down_state.json"))
    reconcile.add_argument("--regime-state", type=Path, default=Path("data/kalshi_equity_regime_state.json"))
    reconcile.add_argument("--output-dir", type=Path, default=Path("outputs"))
    reconcile.add_argument("--starting-balance", default="100.00")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.command == "reconcile":
        return asyncio.run(_reconcile_command(args))
    raise AssertionError("unreachable")


if __name__ == "__main__":
    raise SystemExit(main())
