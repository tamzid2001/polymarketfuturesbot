"""Paper-only KXBTC15M intramarket order-book monitor and scalp simulator.

The ML probability is frozen before the target market opens, matching the
timing of the existing ML inference runner. During that 15-minute market this
program listens to Kalshi's order-book snapshot/delta stream and records
executable YES and NO quotes, depth, model edge, and fill-conservative paper
entries/exits. It never calls an order endpoint.

Paper fills are deliberately conservative:

* an entry pays the displayed best ask and requires displayed opposite-book
  depth for the requested fractional quantity;
* a scalp exit sells at the displayed best bid and also requires depth;
* fee, queue position, latency, cancellation, and partial-fill risk are not
  assumed away. They are separately labelled as unmodelled limitations.

The output is research data, not evidence that a live scalping strategy is
profitable. It is intended to capture the data needed for that later test.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import aiohttp
import numpy as np

import kalshibtc15minupordown as kalshi
from kalshi_btc15m_backtest import FEATURE_COLUMNS, feature_values
from kalshi_ml_inference_live import (
    DEFAULT_MODEL_PATH,
    DEFAULT_TRAINING_CSV,
    build_preopen_signal,
    known_outcomes,
    next_open_timestamp,
    resolve_target_market,
    settled_outcome,
    wait_for_preopen,
)


LOG = logging.getLogger("kalshi_ml_scalp_paper")
DEFAULT_DETAIL_OUTPUT = Path(os.getenv("ML_SCALP_DETAIL_OUTPUT", "ml_scalp_paper_detail.jsonl"))
DEFAULT_SUMMARY_OUTPUT = Path(os.getenv("ML_SCALP_SUMMARY_OUTPUT", "ml_scalp_paper_summary.json"))
DEFAULT_LEDGER = Path(os.getenv("ML_SCALP_LEDGER", "ml_scalp_paper_ledger.json"))
DEFAULT_SAMPLE_SECONDS = float(os.getenv("ML_SCALP_SAMPLE_SECONDS", "1"))
DEFAULT_PAPER_SHARES = float(os.getenv("ML_SCALP_PAPER_SHARES", "0.01"))
DEFAULT_MIN_ENTRY_EDGE = float(os.getenv("ML_SCALP_MIN_ENTRY_EDGE", "0.03"))
DEFAULT_PROFIT_STEP = float(os.getenv("ML_SCALP_PROFIT_STEP", "0.05"))
DEFAULT_TRAILING_STEP = float(os.getenv("ML_SCALP_TRAILING_STEP", "0.05"))
DEFAULT_MAX_ROUND_TRIPS = int(os.getenv("ML_SCALP_MAX_ROUND_TRIPS", "3"))
DEFAULT_SETTLEMENT_WAIT_SECONDS = float(os.getenv("ML_SCALP_SETTLEMENT_WAIT_SECONDS", "120"))


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    for noisy in ("aiohttp", "asyncio", "cmdstanpy", "prophet", "yfinance"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def as_float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if np.isfinite(result) else None


class LiveOrderBook:
    """Maintain Kalshi's YES/NO bid books from snapshot plus delta messages.

    The socket deliberately uses the documented default representation: YES
    levels are YES-price bids and NO levels are NO-price bids. Consequently,
    buying YES crosses the best NO bid at ``1 - no_bid``; buying NO crosses the
    best YES bid at ``1 - yes_bid``.
    """

    def __init__(self, auth: Any, ticker: str, url: str = kalshi.KALSHI_WS_URL):
        self.auth = auth
        self.ticker = ticker
        self.url = url
        self.path = urlparse(url).path or "/trade-api/ws/v2"
        self.books: dict[str, dict[float, float]] = {"yes": {}, "no": {}}
        self.snapshot_ready = asyncio.Event()
        self.stop_event = asyncio.Event()
        self.sequence: int | None = None
        self.message_count = 0
        self.last_update: str | None = None

    def _replace_snapshot(self, message: dict[str, Any]) -> None:
        for side, field in (("yes", "yes_dollars_fp"), ("no", "no_dollars_fp")):
            levels: dict[float, float] = {}
            for raw_level in message.get(field) or []:
                if not isinstance(raw_level, list) or len(raw_level) < 2:
                    continue
                price, depth = as_float(raw_level[0]), as_float(raw_level[1])
                if price is not None and depth is not None and 0.0 < price < 1.0 and depth > 0.0:
                    levels[price] = depth
            self.books[side] = levels

    def _apply_delta(self, message: dict[str, Any]) -> None:
        side = str(message.get("side") or "").lower()
        price, delta = as_float(message.get("price_dollars")), as_float(message.get("delta_fp"))
        if side not in self.books or price is None or delta is None:
            return
        updated = self.books[side].get(price, 0.0) + delta
        if updated <= 1e-9:
            self.books[side].pop(price, None)
        else:
            self.books[side][price] = updated

    def handle(self, raw: str) -> None:
        try:
            payload = json.loads(raw)
        except (TypeError, ValueError):
            return
        message_type = str(payload.get("type") or "")
        message = payload.get("msg") or {}
        if not isinstance(message, dict) or message.get("market_ticker") != self.ticker:
            return
        if message_type == "orderbook_snapshot":
            self._replace_snapshot(message)
            self.snapshot_ready.set()
        elif message_type == "orderbook_delta":
            self._apply_delta(message)
        else:
            return
        sequence = payload.get("seq")
        self.sequence = int(sequence) if isinstance(sequence, int) else self.sequence
        self.message_count += 1
        self.last_update = now_iso()

    def quote(self) -> dict[str, float | int | None]:
        yes_bid = max(self.books["yes"], default=None)
        no_bid = max(self.books["no"], default=None)
        yes_bid_depth = self.books["yes"].get(yes_bid) if yes_bid is not None else None
        no_bid_depth = self.books["no"].get(no_bid) if no_bid is not None else None
        # A YES ask is the complement of the best NO bid, and conversely.
        yes_ask = round(1.0 - no_bid, 4) if no_bid is not None else None
        no_ask = round(1.0 - yes_bid, 4) if yes_bid is not None else None
        return {
            "yes_bid": yes_bid,
            "yes_bid_depth": yes_bid_depth,
            "yes_ask": yes_ask,
            "yes_ask_depth": no_bid_depth,
            "no_bid": no_bid,
            "no_bid_depth": no_bid_depth,
            "no_ask": no_ask,
            "no_ask_depth": yes_bid_depth,
            "sequence": self.sequence,
            "message_count": self.message_count,
            "last_update": self.last_update,
        }

    async def run(self) -> None:
        while not self.stop_event.is_set():
            try:
                headers = self.auth.create_auth_headers("GET", self.path)
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(self.url, headers=headers, heartbeat=10) as ws:
                        await ws.send_json({
                            "id": 1,
                            "cmd": "subscribe",
                            "params": {
                                "channels": ["orderbook_delta"],
                                "market_tickers": [self.ticker],
                                # Keep NO levels in NO-price terms so both executable
                                # sides and their crossing depths can be calculated.
                                "use_yes_price": False,
                            },
                        })
                        LOG.info("Subscribed to paper order book for %s.", self.ticker)
                        while not self.stop_event.is_set():
                            try:
                                received = await ws.receive(timeout=1.0)
                            except asyncio.TimeoutError:
                                continue
                            if received.type == aiohttp.WSMsgType.TEXT:
                                self.handle(received.data)
                            elif received.type in (
                                aiohttp.WSMsgType.CLOSED,
                                aiohttp.WSMsgType.CLOSING,
                                aiohttp.WSMsgType.ERROR,
                            ):
                                break
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                LOG.warning("Paper order-book socket error: %s", exc)
            if not self.stop_event.is_set():
                await asyncio.sleep(2)

    async def close(self) -> None:
        self.stop_event.set()


@dataclass
class PaperPosition:
    side: str
    entry_at: str
    entry_price: float
    entry_depth: float
    model_probability: float
    model_edge: float
    count: float
    activation_price: float
    high_watermark: float | None = None
    trailing_stop: float | None = None
    ladder_levels_reached: int = 0


class PaperScalper:
    """A transparent, single-level paper simulator for both contract sides."""

    def __init__(
        self,
        probability_yes: float,
        count: float,
        min_entry_edge: float,
        profit_step: float,
        trailing_step: float,
        max_round_trips: int,
    ):
        self.probabilities = {"yes": probability_yes, "no": 1.0 - probability_yes}
        self.count = count
        self.min_entry_edge = min_entry_edge
        self.profit_step = profit_step
        self.trailing_step = trailing_step
        self.max_round_trips = max_round_trips
        self.open_positions: dict[str, PaperPosition] = {}
        self.trades: list[dict[str, Any]] = []
        self.round_trips = {"yes": 0, "no": 0}

    def update(self, quote: dict[str, float | int | None]) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        for side in ("yes", "no"):
            bid = as_float(quote.get(f"{side}_bid"))
            bid_depth = as_float(quote.get(f"{side}_bid_depth"))
            ask = as_float(quote.get(f"{side}_ask"))
            ask_depth = as_float(quote.get(f"{side}_ask_depth"))
            position = self.open_positions.get(side)
            if position is not None:
                if bid is not None and bid_depth is not None and bid_depth >= self.count:
                    if bid >= position.activation_price:
                        prior_high = position.high_watermark
                        position.high_watermark = max(position.high_watermark or bid, bid)
                        levels = max(
                            1,
                            int((position.high_watermark - position.model_probability + 1e-9) /
                                self.profit_step),
                        )
                        if levels > position.ladder_levels_reached:
                            position.ladder_levels_reached = levels
                            # At p+5c, protect p; at p+10c, protect p+5c, and so on.
                            position.trailing_stop = round(
                                position.model_probability + (levels - 1) * self.trailing_step, 4
                            )
                            event = {
                                "kind": "paper_trailing_level",
                                "at": now_iso(),
                                "side": side,
                                "activation_price": position.activation_price,
                                "prior_high_watermark": prior_high,
                                "high_watermark": position.high_watermark,
                                "ladder_level": levels,
                                "trailing_stop": position.trailing_stop,
                                "bid": bid,
                            }
                            self.trades.append(event)
                            events.append(event)
                    if position.trailing_stop is not None and bid <= position.trailing_stop:
                        gross = bid - position.entry_price
                        event = {
                            "kind": "paper_exit_trailing_stop",
                            "at": now_iso(),
                            "side": side,
                            "exit_price": bid,
                            "exit_depth": bid_depth,
                            "entry_price": position.entry_price,
                            "gross_per_contract": round(gross, 6),
                            "gross_total": round(gross * self.count, 6),
                            "count": self.count,
                            "high_watermark": position.high_watermark,
                            "trailing_stop": position.trailing_stop,
                            "unmodelled_costs": "fees, queue position, latency, and partial fills",
                        }
                        self.trades.append(event)
                        events.append(event)
                        self.open_positions.pop(side, None)
                        self.round_trips[side] += 1
                continue
            if self.round_trips[side] >= self.max_round_trips:
                continue
            if ask is None or ask_depth is None or ask_depth < self.count:
                continue
            edge = self.probabilities[side] - ask
            if edge < self.min_entry_edge:
                continue
            position = PaperPosition(
                side=side,
                entry_at=now_iso(),
                entry_price=ask,
                entry_depth=ask_depth,
                model_probability=self.probabilities[side],
                model_edge=edge,
                count=self.count,
                activation_price=round(self.probabilities[side] + self.profit_step, 4),
            )
            self.open_positions[side] = position
            event = {
                "kind": "paper_entry",
                **asdict(position),
                "first_trailing_activation_bid": position.activation_price,
                "trailing_rule": (
                    "At each model-probability + profit-step level, move the trailing stop "
                    "up by trailing-step; exit at the observed bid if it falls to the stop."
                ),
                "unmodelled_costs": "fees, queue position, latency, and partial fills",
            }
            self.trades.append(event)
            events.append(event)
        return events

    def mark_to_settlement(self, outcome: str) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        for side, position in list(self.open_positions.items()):
            exit_price = 1.0 if side == outcome else 0.0
            gross = exit_price - position.entry_price
            event = {
                "kind": "paper_exit_settlement",
                "at": now_iso(),
                "side": side,
                "actual_outcome": outcome,
                "entry_price": position.entry_price,
                "exit_price": exit_price,
                "gross_per_contract": round(gross, 6),
                "gross_total": round(gross * self.count, 6),
                "count": self.count,
                "unmodelled_costs": "fees and entry fill uncertainty",
            }
            self.trades.append(event)
            events.append(event)
            self.open_positions.pop(side, None)
        return events


def append_json_line(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as destination:
        destination.write(json.dumps(payload, sort_keys=True) + "\n")


def write_summary(path: Path, summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def append_ledger(path: Path, summary: dict[str, Any]) -> None:
    default = {"format_version": 1, "windows": []}
    try:
        ledger = json.loads(path.read_text(encoding="utf-8")) if path.exists() else default
    except (OSError, ValueError):
        ledger = default
    if not isinstance(ledger, dict):
        ledger = default
    windows = ledger.setdefault("windows", [])
    if not isinstance(windows, list):
        windows = ledger["windows"] = []
    windows.append(summary)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(ledger, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def signal_probability(cached: dict[str, Any], market: dict[str, Any], target_ticker: str) -> float:
    features = feature_values(
        cached["candles"],
        float(market["target"]),
        cached["forecast"],
        known_outcomes(cached["rows"]),
        next_open_timestamp(target_ticker),
    )
    vector = np.asarray([[float(features[name]) for name in FEATURE_COLUMNS]], dtype=float)
    return float(cached["model"].predict_proba(vector)[0][1])


async def monitor_window(args: argparse.Namespace, rest: kalshi.KalshiREST) -> dict[str, Any] | None:
    _, target_ticker = await wait_for_preopen()
    model_path = args.model_path.expanduser() if args.model_path is not None else None
    cached = await build_preopen_signal(args.training_csv.expanduser(), target_ticker, model_path)
    if cached is None:
        return None
    market = await resolve_target_market(rest, target_ticker)
    if market is None or market.get("target") is None:
        LOG.warning("Target %s was not live inside its entry grace; paper monitor skipped.", target_ticker)
        return None
    probability_yes = signal_probability(cached, market, target_ticker)
    scalper = PaperScalper(
        probability_yes=probability_yes,
        count=args.paper_shares,
        min_entry_edge=args.min_entry_edge,
        profit_step=args.profit_step,
        trailing_step=args.trailing_step,
        max_round_trips=args.max_round_trips,
    )
    order_book = LiveOrderBook(rest.auth, target_ticker)
    socket_task = asyncio.create_task(order_book.run())
    try:
        try:
            await asyncio.wait_for(order_book.snapshot_ready.wait(), timeout=15)
        except asyncio.TimeoutError:
            LOG.warning("No order-book snapshot for %s; paper monitor skipped.", target_ticker)
            return None
        settle_time = next_open_timestamp(target_ticker) + np.timedelta64(15, "m")
        settle_timestamp = datetime.fromisoformat(str(settle_time).replace("Z", "+00:00"))
        samples = 0
        entry_candidates = {"yes": 0, "no": 0}
        detail_path = args.detail_output.expanduser()
        while datetime.now(tz=timezone.utc) < settle_timestamp:
            quote = order_book.quote()
            yes_ask, no_ask = as_float(quote.get("yes_ask")), as_float(quote.get("no_ask"))
            yes_edge = probability_yes - yes_ask if yes_ask is not None else None
            no_edge = (1.0 - probability_yes) - no_ask if no_ask is not None else None
            if yes_edge is not None and yes_edge >= args.min_entry_edge:
                entry_candidates["yes"] += 1
            if no_edge is not None and no_edge >= args.min_entry_edge:
                entry_candidates["no"] += 1
            events = scalper.update(quote)
            sample = {
                "kind": "orderbook_sample",
                "at": now_iso(),
                "ticker": target_ticker,
                "probability_yes": round(probability_yes, 6),
                "probability_no": round(1.0 - probability_yes, 6),
                "yes_model_edge": round(yes_edge, 6) if yes_edge is not None else None,
                "no_model_edge": round(no_edge, 6) if no_edge is not None else None,
                "quote": quote,
                "paper_events": events,
            }
            append_json_line(detail_path, sample)
            samples += 1
            await asyncio.sleep(args.sample_seconds)
        outcome = None
        settlement_deadline = asyncio.get_running_loop().time() + args.settlement_wait_seconds
        while asyncio.get_running_loop().time() < settlement_deadline:
            final_market = await rest.get_market(target_ticker)
            outcome = settled_outcome(final_market) if final_market is not None else None
            if outcome is not None:
                break
            await asyncio.sleep(5)
        if outcome is not None:
            settlement_events = scalper.mark_to_settlement(outcome)
            for event in settlement_events:
                append_json_line(detail_path, {"kind": "paper_event", "ticker": target_ticker, **event})
        summary = {
            "ticker": target_ticker,
            "started_at": cached["as_of"].isoformat(),
            "finished_at": now_iso(),
            "probability_yes": round(probability_yes, 6),
            "probability_no": round(1.0 - probability_yes, 6),
            "paper_shares": args.paper_shares,
            "min_entry_edge": args.min_entry_edge,
            "profit_step": args.profit_step,
            "trailing_step": args.trailing_step,
            "max_round_trips_per_side": args.max_round_trips,
            "samples": samples,
            "entry_candidate_samples": entry_candidates,
            "orderbook_messages": order_book.message_count,
            "actual_outcome": outcome,
            "open_positions_at_end": [asdict(position) for position in scalper.open_positions.values()],
            "paper_events": scalper.trades,
            "limitations": (
                "Paper-only. Entries/exits use displayed best quotes and depth, but exclude fees, queue "
                "position, latency, cancellation, partial fills, and price impact."
            ),
        }
        write_summary(args.summary_output.expanduser(), summary)
        append_ledger(args.ledger.expanduser(), summary)
        return summary
    finally:
        await order_book.close()
        socket_task.cancel()
        try:
            await socket_task
        except asyncio.CancelledError:
            pass


async def run(args: argparse.Namespace) -> None:
    rest = kalshi.KalshiREST()
    try:
        for window in range(args.windows):
            summary = await monitor_window(args, rest)
            if summary is not None:
                LOG.info(
                    "PAPER SCALP SUMMARY | %s | samples=%d events=%d outcome=%s",
                    summary["ticker"], summary["samples"], len(summary["paper_events"]),
                    summary.get("actual_outcome") or "pending",
                )
            if window + 1 < args.windows:
                await asyncio.sleep(1)
    finally:
        await rest.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--training-csv", type=Path, default=Path(DEFAULT_TRAINING_CSV))
    parser.add_argument("--model-path", type=Path, default=Path(DEFAULT_MODEL_PATH) if DEFAULT_MODEL_PATH else None)
    parser.add_argument("--detail-output", type=Path, default=DEFAULT_DETAIL_OUTPUT)
    parser.add_argument("--summary-output", type=Path, default=DEFAULT_SUMMARY_OUTPUT)
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--windows", type=int, default=1)
    parser.add_argument("--sample-seconds", type=float, default=DEFAULT_SAMPLE_SECONDS)
    parser.add_argument("--paper-shares", type=float, default=DEFAULT_PAPER_SHARES)
    parser.add_argument("--min-entry-edge", type=float, default=DEFAULT_MIN_ENTRY_EDGE)
    parser.add_argument("--profit-step", type=float, default=DEFAULT_PROFIT_STEP)
    parser.add_argument("--trailing-step", type=float, default=DEFAULT_TRAILING_STEP)
    parser.add_argument("--max-round-trips", type=int, default=DEFAULT_MAX_ROUND_TRIPS)
    parser.add_argument("--settlement-wait-seconds", type=float, default=DEFAULT_SETTLEMENT_WAIT_SECONDS)
    args = parser.parse_args()
    if args.windows < 1:
        parser.error("--windows must be at least one")
    if args.sample_seconds < 0.25:
        parser.error("--sample-seconds must be at least 0.25")
    if not 0.01 <= args.paper_shares <= 1_000:
        parser.error("--paper-shares must be from 0.01 through 1000")
    if (
        not 0.0 <= args.min_entry_edge < 1.0
        or not 0.0 < args.profit_step < 1.0
        or not 0.0 < args.trailing_step < 1.0
    ):
        parser.error("edge and trailing settings must be valid probabilities")
    if args.max_round_trips < 1:
        parser.error("--max-round-trips must be at least one")
    if not 0.0 <= args.settlement_wait_seconds <= 300.0:
        parser.error("--settlement-wait-seconds must be from 0 through 300")
    return args


def main() -> None:
    configure_logging()
    asyncio.run(run(parse_args()))


if __name__ == "__main__":
    main()
