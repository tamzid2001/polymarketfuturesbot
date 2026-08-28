"""
Polymarket US Futures — trading bot.

Primary mode: WebSocket-driven (market_data_lite BBO events, trade events,
private position/balance events).

Fallback mode: REST polling every POLLING_INTERVAL_S seconds. Activates
automatically when WS is unavailable or produces no BBO data for tracked slugs.
Both modes write to the same shared dicts so all trading logic is path-agnostic.
"""

import asyncio
import json
import math
import os
import sys
import time
from collections import deque
from datetime import datetime, date
import zoneinfo
import requests
from polymarket_us import (
    AsyncPolymarketUS,
    APIConnectionError,
    APITimeoutError,
    AuthenticationError,
    BadRequestError,
    NotFoundError,
    RateLimitError,
)

# ==============================================================
# CONFIGURATION
# ==============================================================
TAKE_PROFIT_MULTIPLIER = 3.0    # close when bid >= N × avg_entry
BUYBACK_AMOUNT_USD     = 1.00   # USD per re-entry (fallback when qty unknown)
BUYBACK_STD_DEVS       = 1      # std-dev multiplier for buyback zone
BUYBACK_STD_DEV_PCT    = 0.10   # one std dev = this fraction of avg_entry
PRICE_HISTORY_WINDOW   = 30     # rolling trade count for std dev
RUNTIME_LIMIT_SECONDS  = 20700  # 5 h 45 min — exits before 6 h GH runner limit
STATUS_LOG_INTERVAL_S  = 300    # log status every 5 min
STATE_SAVE_INTERVAL_S  = 60     # persist state every 60 s
POLLING_INTERVAL_S     = 300    # REST fallback polling interval (5 min)
TICK_INTERVAL_S        = 0.5    # main-loop tick
REST_RATE_LIMIT        = 20.0   # max REST req/s (firm cap: 100/s avg'd over 1 min; we use 20%)
REST_MAX_RETRIES       = 3      # retry attempts on 429  (waits: 2 s → 4 s → 8 s)
# ==============================================================

MARKETS_FILE     = "markets.json"
STATE_FILE       = "state.json"
TELEGRAM_CHAT_ID = "@moneyballpredictions"
EST              = zoneinfo.ZoneInfo("America/New_York")

EMPTY_STATE: dict = {
    "positions":         {},
    "pending_buybacks":  [],
    "balance_snapshots": {},
    "today_closed":      [],
    "daily_report_sent": "",
}


# ------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------

def log(msg: str) -> None:
    ts = datetime.now(EST).strftime("%Y-%m-%d %H:%M:%S EST")
    print(f"[{ts}] {msg}", flush=True)


# ------------------------------------------------------------------
# Optional Telegram
# ------------------------------------------------------------------

class Notifier:
    def __init__(self, token: str | None, chat_id: str) -> None:
        self.token   = token
        self.chat_id = chat_id
        self.enabled = bool(token)

    def send(self, text: str) -> None:
        if not self.enabled:
            return
        MAX_LEN = 4000
        if len(text) > MAX_LEN:
            text = text[:MAX_LEN] + "\n_[truncated]_"
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        try:
            r = requests.post(
                url,
                json={"chat_id": self.chat_id, "text": text, "parse_mode": "Markdown"},
                timeout=10,
            )
            if not r.ok:
                r = requests.post(
                    url,
                    json={"chat_id": self.chat_id, "text": text},
                    timeout=10,
                )
            r.raise_for_status()
            log(f"INFO  Telegram sent (msg_id={r.json()['result']['message_id']})")
        except Exception as exc:
            log(f"WARN  Telegram send failed (non-fatal): {exc}")


# ------------------------------------------------------------------
# State + markets persistence
# ------------------------------------------------------------------

def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as fh:
                data = json.load(fh)
            for k, v in EMPTY_STATE.items():
                data.setdefault(k, type(v)())
            log("INFO  State loaded from disk.")
            return data
        except Exception as exc:
            log(f"WARN  State file unreadable ({exc}) — starting fresh.")
    return {k: type(v)() for k, v in EMPTY_STATE.items()}


def save_state(state: dict) -> None:
    with open(STATE_FILE, "w") as fh:
        json.dump(state, fh, indent=2)


def load_markets() -> tuple[list, dict]:
    try:
        with open(MARKETS_FILE) as fh:
            data = json.load(fh)
        markets  = data.get("mlb_world_series", [])
        settings = data.get("settings", {})
        log(f"INFO  Loaded {len(markets)} market(s) from {MARKETS_FILE}.")
        return markets, settings
    except Exception as exc:
        log(f"WARN  Could not load {MARKETS_FILE}: {exc}. Using empty market list.")
        return [], {}


# ------------------------------------------------------------------
# Token-bucket rate limiter
# ------------------------------------------------------------------

class _RateLimiter:
    """Caps REST API throughput to stay well under Polymarket's 100 req/s firm cap."""

    def __init__(self, rate: float) -> None:
        self._rate   = rate      # tokens per second
        self._tokens = rate      # start full
        self._last   = time.monotonic()
        self._lock   = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            self._tokens = min(self._rate, self._tokens + (now - self._last) * self._rate)
            self._last   = now
            if self._tokens < 1.0:
                await asyncio.sleep((1.0 - self._tokens) / self._rate)
                self._tokens = 0.0
            else:
                self._tokens -= 1.0


# ------------------------------------------------------------------
# Workflow rollover
# ------------------------------------------------------------------

def trigger_workflow_handoff() -> bool:
    gh_pat = os.environ.get("GH_PAT") or os.environ.get("GH_TOKEN")
    repo   = os.environ.get("GITHUB_REPOSITORY", "tamzid2001/polymarketfuturesbot")
    if not gh_pat:
        log("WARN  GH_PAT not set — daily cron at 00:07 UTC is the fallback restart.")
        return False
    url     = f"https://api.github.com/repos/{repo}/actions/workflows/polymarket_monitor.yml/dispatches"
    headers = {
        "Authorization":        f"Bearer {gh_pat}",
        "Accept":               "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    try:
        r = requests.post(url, headers=headers, json={"ref": "main"}, timeout=15)
        if r.status_code == 204:
            log("INFO  Workflow handoff SUCCESS — next run queued.")
            return True
        log(f"ERROR Workflow handoff HTTP {r.status_code}: {r.text[:200]}")
        return False
    except Exception as exc:
        log(f"ERROR Workflow handoff failed: {exc}")
        return False


# ------------------------------------------------------------------
# Bot
# ------------------------------------------------------------------

class PolymarketBot:

    def __init__(self, markets: list, settings: dict, state: dict, notifier: Notifier) -> None:
        self._markets  = markets
        self._settings = settings
        self._state    = state
        self._notifier = notifier

        # Shared dicts — written by sync WS callbacks, read by async main loop
        self._live_positions: dict[str, dict]  = {}  # slug → position payload
        self._latest_bbo:     dict[str, float] = {}  # slug → latest bid
        self._price_history:  dict[str, deque] = {}  # slug → deque(maxlen=30)
        self._balance:        dict             = {}

        self._active_trades:        set[str] = set()
        self._closed_slugs_pending: set[str] = set()

        self._ws_ok       = False
        self._loop:       asyncio.AbstractEventLoop | None = None
        self._client:     AsyncPolymarketUS | None = None
        self._private_ws  = None
        self._markets_ws  = None
        self._start_time  = time.monotonic()
        self._last_poll   = 0.0
        self._rl          = _RateLimiter(REST_RATE_LIMIT)

        self._tracked_slugs: list[str] = [m["market_slug"] for m in markets]
        self._slug_to_team:  dict[str, str] = {
            m["market_slug"]: m.get("team", m["market_slug"]) for m in markets
        }
        self._last_bbo_logged: dict[str, float] = {}  # dedup: skip if bid unchanged

    # ------------------------------------------------------------------
    # Rate-limited REST helper
    # ------------------------------------------------------------------

    async def _api(self, make_coro, *, retries: int = REST_MAX_RETRIES):
        """Acquire a rate-limit token then execute one REST call.
        Retries up to `retries` times on RateLimitError with exponential backoff."""
        for attempt in range(retries):
            await self._rl.acquire()
            try:
                return await make_coro()
            except RateLimitError as exc:
                if attempt == retries - 1:
                    raise
                wait = 2 ** (attempt + 1)   # 2 s, 4 s, 8 s
                log(f"WARN  429 rate-limited — retrying in {wait}s (attempt {attempt + 1}/{retries}): {exc.message}")
                await asyncio.sleep(wait)

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    async def run(self, client: AsyncPolymarketUS) -> None:
        self._client = client
        self._loop   = asyncio.get_running_loop()
        log(f"INFO  Tracking {len(self._tracked_slugs)} market(s).")

        await self._initialize_positions()
        await self._connect_ws()
        await self._main_loop()

    # ------------------------------------------------------------------
    # Startup: open positions for underdogs not yet held
    # ------------------------------------------------------------------

    async def _initialize_positions(self) -> None:
        log("INFO  Fetching live portfolio...")
        try:
            resp      = await self._api(lambda: self._client.portfolio.positions())
            positions = resp.get("positions", {}) if isinstance(resp, dict) else {}
        except AuthenticationError as exc:
            log(f"ERROR  Auth failed: {exc.message}")
            raise
        except (APIConnectionError, APITimeoutError) as exc:
            log(f"WARN  Portfolio fetch failed ({type(exc).__name__}): {exc.message}. Skipping auto-entry.")
            return
        except Exception as exc:
            log(f"WARN  Portfolio fetch failed: {exc}. Skipping auto-entry.")
            return

        held: set[str] = set()
        for _, pos in positions.items():
            meta = pos.get("marketMetadata", {}) or {}
            slug = meta.get("slug", "")
            qty  = float(pos.get("netPosition", "0") or "0")
            if slug and qty > 0:
                held.add(slug)
                self._live_positions[slug] = pos

        log(f"INFO  Already holding: {held or '(none)'}")

        for market in self._markets:
            if not market.get("is_underdog"):
                continue
            slug = market["market_slug"]
            if slug in held:
                log(f"SKIP  {market['team']} ({slug}) — already held.")
                continue
            deployment = market.get("max_deployment_usd",
                                    self._settings.get("initial_deployment_usd", 1.0))
            await self._open_position(market, deployment)

    async def _open_position(self, market: dict, deployment_usd: float) -> None:
        slug = market["market_slug"]
        if slug in self._active_trades:
            return
        self._active_trades.add(slug)
        try:
            bbo = await self._api(lambda: self._client.markets.bbo(slug))
            ask = float((bbo.get("bestAsk") or {}).get("value", 0) or 0) if isinstance(bbo, dict) else 0
            if ask <= 0:
                log(f"WARN  No ask price for {slug} — skipping entry.")
                return
            qty = max(1, math.floor(deployment_usd / ask))
            log(f"INFO  Opening: {market['team']} ({slug}) qty={qty} @ ${ask:.4f}")
            order_payload = {
                "marketSlug": slug,
                "intent":     "ORDER_INTENT_BUY_LONG",
                "type":       "ORDER_TYPE_LIMIT",
                "price":      {"value": f"{ask:.4f}", "currency": "USD"},
                "quantity":   qty,
                "tif":        "TIME_IN_FORCE_GOOD_TILL_CANCEL",
            }
            resp     = await self._api(lambda: self._client.orders.create(order_payload))
            order_id = resp.get("id", "?") if isinstance(resp, dict) else "?"
            log(f"INFO  Opened {slug} order_id={order_id}")
            self._notifier.send(
                f"✅ *Position Opened*\n`{slug}`\nqty={qty} @ `${ask:.4f}` (~`${deployment_usd:.2f}`)"
            )
        except AuthenticationError as exc:
            log(f"ERROR  Auth failure opening {slug}: {exc.message}")
            raise
        except BadRequestError as exc:
            err = getattr(exc, "message", "") or ""
            if any(kw in err.lower() for kw in ("insufficient", "funds", "buying power", "balance")):
                log(f"SKIP  {slug} — insufficient funds (~${deployment_usd:.2f} needed). Skipping, continuing to next market.")
            else:
                log(f"ERROR  Bad order params {slug}: {err}")
        except NotFoundError as exc:
            log(f"WARN  Market closed/missing {slug}: {exc.message}")
            self._mark_market_inactive(slug)
        except RateLimitError as exc:
            log(f"WARN  Rate limited opening {slug} (retries exhausted): {exc.message}")
        except (APIConnectionError, APITimeoutError) as exc:
            log(f"WARN  Network error opening {slug}: {exc.message}")
        except Exception as exc:
            log(f"ERROR  Unexpected error opening {slug}: {exc}")
        finally:
            self._active_trades.discard(slug)

    # ------------------------------------------------------------------
    # Market closure detection
    # ------------------------------------------------------------------

    def _mark_market_inactive(self, slug: str) -> None:
        for market in self._markets:
            if market.get("market_slug") == slug and market.get("is_underdog"):
                market["is_underdog"] = False
                log(f"INFO  {market.get('team', slug)} ({slug}) — closed, is_underdog → false.")
                self._notifier.send(f"⚠️ *Market Closed*\n`{slug}` — no further entries.")
                self._save_markets()
                return

    def _save_markets(self) -> None:
        try:
            with open(MARKETS_FILE) as fh:
                data = json.load(fh)
            data["mlb_world_series"] = self._markets
            with open(MARKETS_FILE, "w") as fh:
                json.dump(data, fh, indent=2)
            log("INFO  markets.json updated on disk.")
        except Exception as exc:
            log(f"WARN  Could not save markets.json: {exc}")

    # ------------------------------------------------------------------
    # WebSocket connections
    # ------------------------------------------------------------------

    async def _connect_ws(self) -> None:
        await self._connect_private_ws()
        await self._connect_markets_ws()

    async def _connect_private_ws(self) -> None:
        try:
            ws = self._client.ws.private()
            ws.on("position_snapshot",        self._on_position_snapshot)
            ws.on("position_update",           self._on_position_update)
            ws.on("account_balance_snapshot",  self._on_balance_snapshot)
            ws.on("account_balance_update",    self._on_balance_update)
            ws.on("error", lambda err: log(f"WARN  Private WS error: {err}"))
            ws.on("close", lambda: asyncio.run_coroutine_threadsafe(
                self._reconnect_private(), self._loop))

            await ws.connect()
            await ws.subscribe_positions("positions-1")
            await ws.subscribe_account_balance("balance-1")

            self._private_ws = ws
            self._ws_ok = True
            log("INFO  Private WebSocket connected.")
        except Exception as exc:
            log(f"WARN  Private WS failed: {exc}. REST polling will cover positions.")
            self._loop.call_later(60, lambda: asyncio.run_coroutine_threadsafe(
                self._reconnect_private(), self._loop))

    async def _connect_markets_ws(self) -> None:
        if not self._tracked_slugs:
            log("WARN  No tracked slugs — skipping markets WS.")
            return
        try:
            ws = self._client.ws.markets()
            ws.on("market_data_lite", self._on_bbo_sync)
            ws.on("trade",            self._on_trade_sync)
            ws.on("error", lambda err: log(f"WARN  Markets WS error: {err}"))
            ws.on("close", lambda: asyncio.run_coroutine_threadsafe(
                self._reconnect_markets(), self._loop))

            await ws.connect()
            await ws.subscribe_market_data_lite("md-lite-1", self._tracked_slugs)
            await ws.subscribe_trades("trades-1", self._tracked_slugs)

            self._markets_ws = ws
            self._ws_ok = True
            log(f"INFO  Markets WebSocket connected ({len(self._tracked_slugs)} slugs).")
        except Exception as exc:
            log(f"WARN  Markets WS failed: {exc}. REST polling will cover BBO.")
            self._loop.call_later(60, lambda: asyncio.run_coroutine_threadsafe(
                self._reconnect_markets(), self._loop))

    async def _reconnect_private(self) -> None:
        log("INFO  Reconnecting private WS...")
        await asyncio.sleep(5)
        await self._connect_private_ws()

    async def _reconnect_markets(self) -> None:
        log("INFO  Reconnecting markets WS...")
        await asyncio.sleep(5)
        await self._connect_markets_ws()

    async def _close_ws(self) -> None:
        for ws, label in ((self._private_ws, "private"), (self._markets_ws, "markets")):
            if ws is None:
                continue
            try:
                await ws.close()
                log(f"INFO  {label.capitalize()} WS closed.")
            except Exception as exc:
                log(f"WARN  Error closing {label} WS: {exc}")

    # ------------------------------------------------------------------
    # Sync WS callbacks (called from WS thread — GIL-safe dict writes only)
    # ------------------------------------------------------------------

    def _team(self, slug: str) -> str:
        return self._slug_to_team.get(slug, slug)

    def _on_position_snapshot(self, data: dict) -> None:
        positions = (data.get("positionSubscriptionSnapshot") or {}).get("positions", {}) or {}
        for slug, pos in positions.items():
            self._live_positions[slug] = pos
        log(f"[POS-SNAP]  {len(positions)} position(s) received")
        for slug, pos in positions.items():
            qty  = float(pos.get("netPosition", "0") or "0")
            cost = float((pos.get("cost") or {}).get("value", 0) or 0)
            avg  = cost / qty if qty > 0 else 0
            log(f"  held  {self._team(slug):<28s}  qty={int(qty)}  avg_entry=${avg:.4f}  cost=${cost:.4f}")

    def _on_position_update(self, data: dict) -> None:
        upd  = data.get("positionSubscriptionUpdate") or {}
        slug = upd.get("marketSlug", "")
        pos  = upd.get("position")
        if slug and pos is not None:
            self._live_positions[slug] = pos
            qty  = float(pos.get("netPosition", "0") or "0")
            cost = float((pos.get("cost") or {}).get("value", 0) or 0)
            avg  = cost / qty if qty > 0 else 0
            bid  = self._latest_bbo.get(slug, 0)
            pnl  = (bid - avg) * qty if avg > 0 and bid > 0 else 0
            log(f"[POS-UPD]   {self._team(slug):<28s}  qty={int(qty)}  avg_entry=${avg:.4f}  bid=${bid:.4f}  P&L=${pnl:+.4f}")

    def _on_balance_snapshot(self, data: dict) -> None:
        # server may use either key name
        snap = (data.get("accountBalanceSubscriptionSnapshot")
                or data.get("accountBalancesSnapshot")
                or {})
        # server may use "balance" or "currentBalance"
        bal  = snap.get("balance") or snap.get("currentBalance") or 0
        bp   = snap.get("buyingPower", 0) or 0
        self._balance = {"balance": float(bal), "buyingPower": float(bp)}
        log(f"[BAL-SNAP]  balance=${self._balance['balance']:.2f}  buying_power=${self._balance['buyingPower']:.2f}")

    def _on_balance_update(self, data: dict) -> None:
        upd = (data.get("accountBalanceSubscriptionUpdate")
               or data.get("accountBalanceUpdate")
               or {})
        if upd:
            prev = float(self._balance.get("balance", 0))
            bal  = upd.get("balance") or upd.get("currentBalance") or prev
            bp   = upd.get("buyingPower", self._balance.get("buyingPower", 0))
            self._balance = {"balance": float(bal), "buyingPower": float(bp)}
            delta = self._balance["balance"] - prev
            log(f"[BAL-UPD]   balance=${self._balance['balance']:.2f}  buying_power=${self._balance['buyingPower']:.2f}  delta=${delta:+.4f}")

    def _on_bbo_sync(self, data: dict) -> None:
        md   = data.get("marketDataLite") or {}
        slug = md.get("marketSlug", "")
        if not slug:
            return
        if md.get("closed") or md.get("active") is False:
            self._closed_slugs_pending.add(slug)
            log(f"[BBO]  {self._team(slug):<28s}  MARKET CLOSED")
            return
        bid_raw = (md.get("bestBid") or {}).get("value")
        ask_raw = (md.get("bestAsk") or {}).get("value")
        if bid_raw is None:
            return
        bid = float(bid_raw)
        ask = float(ask_raw) if ask_raw is not None else 0.0
        self._latest_bbo[slug] = bid
        if self._last_bbo_logged.get(slug) == bid:
            return  # price unchanged — skip log
        self._last_bbo_logged[slug] = bid
        pos = self._live_positions.get(slug)
        if pos:
            qty  = float(pos.get("netPosition", "0") or "0")
            cost = float((pos.get("cost") or {}).get("value", 0) or 0)
            avg  = cost / qty if qty > 0 else 0
            tp   = avg * TAKE_PROFIT_MULTIPLIER
            pct  = (bid / tp * 100) if tp > 0 else 0
            pnl  = (bid - avg) * qty
            log(
                f"[BBO]  {self._team(slug):<28s}  bid=${bid:.4f}  ask=${ask:.4f}"
                f"  qty={int(qty)}  entry=${avg:.4f}  P&L=${pnl:+.4f}  TP={pct:.1f}%"
            )
        else:
            log(f"[BBO]  {self._team(slug):<28s}  bid=${bid:.4f}  ask=${ask:.4f}")

    def _on_trade_sync(self, data: dict) -> None:
        trade = data.get("trade") or {}
        slug  = trade.get("marketSlug", "")
        price_raw = (trade.get("price") or {}).get("value")
        if slug and price_raw is not None:
            price = float(price_raw)
            if slug not in self._price_history:
                self._price_history[slug] = deque(maxlen=PRICE_HISTORY_WINDOW)
            self._price_history[slug].append(price)
            qty_raw = trade.get("quantity")
            qty     = float((qty_raw or {}).get("value", 0)) if isinstance(qty_raw, dict) else float(qty_raw or 0)
            side    = trade.get("side", "")
            log(f"[TRADE] {self._team(slug):<28s}  ${price:.4f} × {qty:.0f}  {side}")

    # ------------------------------------------------------------------
    # REST polling fallback
    # ------------------------------------------------------------------

    async def _poll_positions_rest(self) -> None:
        """Refresh positions, BBOs, and balance via REST."""
        log("INFO  REST poll: refreshing positions, balance, and BBO...")
        try:
            resp      = await self._api(lambda: self._client.portfolio.positions())
            positions = resp.get("positions", {}) if isinstance(resp, dict) else {}
            for _, pos in positions.items():
                meta = pos.get("marketMetadata", {}) or {}
                slug = meta.get("slug", "")
                if slug:
                    self._live_positions[slug] = pos
            log(f"INFO  REST poll: {len(positions)} position(s) refreshed.")
        except AuthenticationError as exc:
            log(f"ERROR  Auth failed during REST poll: {exc.message}")
            raise
        except Exception as exc:
            log(f"WARN  REST position poll failed: {exc}")

        try:
            bal_resp = await self._api(lambda: self._client.account.balances())
            bals     = bal_resp.get("balances", []) if isinstance(bal_resp, dict) else []
            if bals:
                b    = bals[0]
                prev = float(self._balance.get("balance", 0))
                self._balance = {
                    "balance":     float(b.get("currentBalance", 0) or 0),
                    "buyingPower": float(b.get("buyingPower",     0) or 0),
                }
                log(f"INFO  REST poll: balance=${self._balance['balance']:.2f}  buying_power=${self._balance['buyingPower']:.2f}  (was ${prev:.2f})")
        except AuthenticationError as exc:
            log(f"ERROR  Auth failed fetching balance: {exc.message}")
            raise
        except Exception as exc:
            log(f"WARN  REST balance poll failed: {exc}")

        for slug in list(self._tracked_slugs):
            try:
                bbo = await self._api(lambda s=slug: self._client.markets.bbo(s))
                bid = float((bbo.get("bestBid") or {}).get("value", 0) or 0) if isinstance(bbo, dict) else 0
                if bid > 0:
                    self._latest_bbo[slug] = bid
            except NotFoundError:
                self._mark_market_inactive(slug)
            except Exception:
                pass  # transient — try next cycle

    # ------------------------------------------------------------------
    # Std dev
    # ------------------------------------------------------------------

    def _compute_std_dev(self, slug: str, fallback_entry: float = 0.0) -> float:
        history = list(self._price_history.get(slug, []))
        if len(history) >= 3:
            mean     = sum(history) / len(history)
            variance = sum((p - mean) ** 2 for p in history) / len(history)
            return math.sqrt(variance)
        return (fallback_entry or 0.0) * BUYBACK_STD_DEV_PCT

    # ------------------------------------------------------------------
    # Take-profit
    # ------------------------------------------------------------------

    async def _check_take_profit(self, slug: str, bid: float) -> None:
        pos = self._live_positions.get(slug)
        if not pos:
            return
        qty = float(pos.get("netPosition", "0") or "0")
        if qty <= 0:
            return
        cost      = float((pos.get("cost") or {}).get("value", 0) or 0)
        avg_entry = cost / qty if qty > 0 else 0
        if avg_entry <= 0 or bid < avg_entry * TAKE_PROFIT_MULTIPLIER:
            return

        log(f"TAKE-PROFIT {slug}: bid=${bid:.4f} >= {TAKE_PROFIT_MULTIPLIER}× ${avg_entry:.4f}")
        self._active_trades.add(slug)
        try:
            resp     = await self._api(lambda: self._client.orders.close_position({"marketSlug": slug}))
            close_id = resp.get("id", "?") if isinstance(resp, dict) else "?"
            profit   = (bid - avg_entry) * qty
            log(f"INFO  Closed {slug} id={close_id}  est_profit=${profit:.2f}")

            meta  = pos.get("marketMetadata", {}) or {}
            self._state.setdefault("pending_buybacks", []).append({
                "market_slug":     slug,
                "event_slug":      meta.get("eventSlug", ""),
                "intent":          "ORDER_INTENT_BUY_LONG",
                "avg_entry_price": round(avg_entry, 6),
                "entry_std_dev":   self._compute_std_dev(slug, avg_entry),
                "qty_sold":        int(qty),
                "sell_price":      round(bid, 6),
                "sell_time":       datetime.now(EST).isoformat(),
                "processed":       False,
                "failed_attempts": 0,
            })
            self._state.setdefault("today_closed", []).append({
                "slug": slug, "qty": int(qty),
                "avg_price": round(avg_entry, 4), "exit_price": round(bid, 4),
                "profit": round(profit, 2), "time": datetime.now(EST).isoformat(),
            })
            self._live_positions.pop(slug, None)
            self._notifier.send(
                f"🚨 *Take-Profit!*\n`{slug}`\n"
                f"Entry `${avg_entry:.4f}` → Exit `${bid:.4f}` ({TAKE_PROFIT_MULTIPLIER}×)\n"
                f"Profit ≈ `${profit:+.2f} USD`"
            )
        except AuthenticationError as exc:
            log(f"ERROR  Auth failure closing {slug}: {exc.message}")
            raise
        except BadRequestError as exc:
            log(f"ERROR  Bad request closing {slug}: {exc.message}")
        except NotFoundError as exc:
            log(f"WARN  Market closed on take-profit {slug}: {exc.message}")
            self._mark_market_inactive(slug)
        except RateLimitError as exc:
            log(f"WARN  Rate limited closing {slug} (retries exhausted): {exc.message}")
        except (APIConnectionError, APITimeoutError) as exc:
            log(f"WARN  Network error closing {slug}: {exc.message}")
        except Exception as exc:
            log(f"ERROR  Unexpected error closing {slug}: {exc}")
        finally:
            self._active_trades.discard(slug)

    # ------------------------------------------------------------------
    # Buyback
    # ------------------------------------------------------------------

    async def _check_buyback(self, buyback: dict, bid: float) -> None:
        slug      = buyback.get("market_slug", "")
        avg_entry = float(buyback.get("avg_entry_price", 0) or 0)
        if avg_entry <= 0 or not slug:
            return

        std_dev = float(buyback.get("entry_std_dev") or 0) or self._compute_std_dev(slug, avg_entry)
        lower   = avg_entry - BUYBACK_STD_DEVS * std_dev
        upper   = avg_entry + BUYBACK_STD_DEVS * std_dev
        if not (lower <= bid <= upper):
            return

        log(f"BUYBACK {slug}: bid=${bid:.4f} in zone [${lower:.4f}, ${upper:.4f}]")
        self._active_trades.add(slug)
        try:
            qty_sold = int(buyback.get("qty_sold", 0) or 0)
            buy_qty  = qty_sold if qty_sold > 0 else max(1, math.floor(BUYBACK_AMOUNT_USD / bid))
            alloc    = round(buy_qty * bid, 2)

            bb_payload = {
                "marketSlug": slug,
                "intent":     buyback.get("intent", "ORDER_INTENT_BUY_LONG"),
                "type":       "ORDER_TYPE_LIMIT",
                "price":      {"value": f"{bid:.4f}", "currency": "USD"},
                "quantity":   buy_qty,
                "tif":        "TIME_IN_FORCE_GOOD_TILL_CANCEL",
            }
            resp     = await self._api(lambda: self._client.orders.create(bb_payload))
            order_id = resp.get("id", "?") if isinstance(resp, dict) else "?"
            log(f"INFO  Buyback placed: {slug} qty={buy_qty} @ ${bid:.4f}  id={order_id}")
            buyback["processed"] = True
            self._notifier.send(
                f"🔄 *Buyback*\n`{slug}`\nqty={buy_qty} @ `${bid:.4f}` (~`${alloc:.2f}`)\n"
                f"Trigger: price back within {BUYBACK_STD_DEVS} std dev of `${avg_entry:.4f}`"
            )
        except AuthenticationError as exc:
            log(f"ERROR  Auth failure on buyback {slug}: {exc.message}")
            raise
        except BadRequestError as exc:
            buyback["failed_attempts"] = buyback.get("failed_attempts", 0) + 1
            log(f"ERROR  Bad order params buyback {slug}: {exc.message}  (attempt #{buyback['failed_attempts']})")
        except RateLimitError as exc:
            log(f"WARN  Rate limited on buyback {slug} (retries exhausted): {exc.message}")
        except (APIConnectionError, APITimeoutError) as exc:
            log(f"WARN  Network error on buyback {slug}: {exc.message}")
        except Exception as exc:
            buyback["failed_attempts"] = buyback.get("failed_attempts", 0) + 1
            log(f"ERROR  Buyback {slug}: {exc}  (attempt #{buyback['failed_attempts']})")
        finally:
            self._active_trades.discard(slug)

    # ------------------------------------------------------------------
    # Scan all tracked markets each tick
    # ------------------------------------------------------------------

    async def _scan(self) -> None:
        for slug in list(self._closed_slugs_pending):
            self._closed_slugs_pending.discard(slug)
            self._mark_market_inactive(slug)

        for slug, bid in list(self._latest_bbo.items()):
            if slug in self._active_trades or bid <= 0:
                continue
            await self._check_take_profit(slug, bid)

        pending = [b for b in self._state.get("pending_buybacks", []) if not b.get("processed")]
        for buyback in pending:
            slug = buyback.get("market_slug", "")
            bid  = self._latest_bbo.get(slug, 0)
            if bid > 0 and slug not in self._active_trades:
                await self._check_buyback(buyback, bid)

        self._state["pending_buybacks"] = [
            b for b in self._state.get("pending_buybacks", []) if not b.get("processed")
        ]

    # ------------------------------------------------------------------
    # Daily report
    # ------------------------------------------------------------------

    def _snapshot_balance(self) -> None:
        today = date.today().isoformat()
        snaps = self._state.setdefault("balance_snapshots", {})
        if today not in snaps and self._balance:
            snaps[today] = dict(self._balance)
        while len(snaps) > 7:
            del snaps[sorted(snaps)[0]]

    def _maybe_daily_report(self) -> None:
        today = date.today().isoformat()
        if self._state.get("daily_report_sent") == today:
            return
        snaps     = self._state.get("balance_snapshots", {})
        days      = sorted(snaps)
        t_snap    = snaps.get(today, {})
        y_snap    = snaps.get(days[-2], {}) if len(days) >= 2 and days[-1] == today else {}
        today_bal = float(t_snap.get("balance", 0))
        yest_bal  = float(y_snap.get("balance", 0))
        delta     = today_bal - yest_bal
        closed    = self._state.get("today_closed", [])
        total_pnl = sum(p.get("profit", 0) for p in closed)

        lines = [
            f"🌅 *Daily Report — {today}*",
            f"💰 Balance: `${today_bal:.2f}` {'📈' if delta >= 0 else '📉'} (`${delta:+.2f}` vs yesterday)",
            f"✅ Closed today: {len(closed)} trade(s) | total P&L `${total_pnl:+.2f}`",
        ]
        for p in closed:
            e = "🟢" if p.get("profit", 0) >= 0 else "🔴"
            lines.append(
                f"{e} `{p.get('slug')}` — qty={p.get('qty')}  "
                f"entry `${p.get('avg_price', 0):.4f}` → exit `${p.get('exit_price', 0):.4f}` "
                f"P&L `${p.get('profit', 0):+.2f}`"
            )

        report = "\n".join(lines)
        if self._notifier.enabled:
            self._notifier.send(report)
        else:
            log("INFO  Daily report (Telegram disabled):")
            for line in lines:
                log(f"      {line}")

        self._state["daily_report_sent"] = today
        self._state["today_closed"]      = []

    # ------------------------------------------------------------------
    # Periodic status log
    # ------------------------------------------------------------------

    def _log_status(self, elapsed_s: float) -> None:
        remaining_min = (RUNTIME_LIMIT_SECONDS - elapsed_s) / 60
        buybacks      = [b for b in self._state.get("pending_buybacks", []) if not b.get("processed")]
        log(
            f"{'─'*72}\n"
            f"[STATUS]  elapsed={elapsed_s/60:.1f} min  remaining={remaining_min:.1f} min  "
            f"ws={'ON' if self._ws_ok else 'OFF(polling)'}  "
            f"balance=${self._balance.get('balance', 0):.2f}  "
            f"buying_power=${self._balance.get('buyingPower', 0):.2f}"
        )
        if self._live_positions:
            log(f"[STATUS]  held positions ({len(self._live_positions)}):")
            for slug, pos in self._live_positions.items():
                qty  = float(pos.get("netPosition", "0") or "0")
                cost = float((pos.get("cost") or {}).get("value", 0) or 0)
                avg  = cost / qty if qty > 0 else 0
                bid  = self._latest_bbo.get(slug, 0)
                tp   = avg * TAKE_PROFIT_MULTIPLIER
                pct  = (bid / tp * 100) if tp > 0 else 0
                pnl  = (bid - avg) * qty
                log(f"  {self._team(slug):<28s}  qty={int(qty)}  bid=${bid:.4f}  "
                    f"entry=${avg:.4f}  P&L=${pnl:+.4f}  TP={pct:.1f}%")
        else:
            log("[STATUS]  no open positions")
        if buybacks:
            log(f"[STATUS]  pending buybacks ({len(buybacks)}):")
            for b in buybacks:
                slug = b.get("market_slug", "")
                bid  = self._latest_bbo.get(slug, 0)
                avg  = b.get("avg_entry_price", 0)
                std  = b.get("entry_std_dev", 0)
                log(f"  {self._team(slug):<28s}  target=${avg:.4f} ±{std:.4f}  current_bid=${bid:.4f}")
        log(f"{'─'*72}")

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def _main_loop(self) -> None:
        last_status = time.monotonic()
        last_save   = time.monotonic()

        while True:
            now     = time.monotonic()
            elapsed = now - self._start_time

            if elapsed >= RUNTIME_LIMIT_SECONDS:
                log(f"INFO  Runtime limit ({RUNTIME_LIMIT_SECONDS // 60} min). Initiating handoff...")
                await self._close_ws()
                save_state(self._state)
                trigger_workflow_handoff()
                log("INFO  Handoff complete. Exiting.")
                break

            # REST polling fallback: always run every POLLING_INTERVAL_S regardless of WS status
            if now - self._last_poll >= POLLING_INTERVAL_S:
                await self._poll_positions_rest()
                self._last_poll = time.monotonic()

            await self._scan()

            if now - last_save >= STATE_SAVE_INTERVAL_S:
                save_state(self._state)
                self._snapshot_balance()
                last_save = time.monotonic()

            if now - last_status >= STATUS_LOG_INTERVAL_S:
                self._log_status(elapsed)
                last_status = time.monotonic()

            now_est = datetime.now(EST)
            if now_est.hour == 10 and now_est.minute < 1:
                self._maybe_daily_report()

            await asyncio.sleep(TICK_INTERVAL_S)


# ------------------------------------------------------------------
# Async entry
# ------------------------------------------------------------------

async def run_async() -> None:
    api_key    = os.environ.get("POLYMARKET_PUBLIC_KEY")
    secret_key = os.environ.get("POLYMARKET_SECRET_KEY")
    if not api_key or not secret_key:
        log("ERROR  POLYMARKET_PUBLIC_KEY / POLYMARKET_SECRET_KEY not set.")
        sys.exit(1)

    tg_token = os.environ.get("TELEGRAM_KEY")
    notifier = Notifier(tg_token, TELEGRAM_CHAT_ID)

    log("=" * 64)
    log("STARTUP  Polymarket Futures Bot")
    log(f"         Python {sys.version.split()[0]}  PID {os.getpid()}")
    log(f"CONFIG   TAKE_PROFIT_MULTIPLIER = {TAKE_PROFIT_MULTIPLIER}×")
    log(f"CONFIG   BUYBACK_AMOUNT_USD     = ${BUYBACK_AMOUNT_USD:.2f}")
    log(f"CONFIG   BUYBACK_STD_DEVS       = {BUYBACK_STD_DEVS}  ({BUYBACK_STD_DEV_PCT*100:.0f}% per dev)")
    log(f"CONFIG   RUNTIME_LIMIT          = {RUNTIME_LIMIT_SECONDS // 60} min")
    log(f"CONFIG   POLLING_INTERVAL       = {POLLING_INTERVAL_S // 60} min (REST fallback)")
    log(f"CONFIG   REST_RATE_LIMIT        = {REST_RATE_LIMIT:.0f} req/s  max_retries={REST_MAX_RETRIES} (429 backoff: 2/4/8 s)")
    log(f"INFO     Telegram: {'ENABLED' if notifier.enabled else 'DISABLED (TELEGRAM_KEY not set)'}")
    log("=" * 64)

    markets, settings = load_markets()
    state             = load_state()
    bot               = PolymarketBot(markets, settings, state, notifier)

    async with AsyncPolymarketUS(key_id=api_key, secret_key=secret_key) as client:
        try:
            await bot.run(client)
        except KeyboardInterrupt:
            log("INFO  KeyboardInterrupt — shutting down.")
        except AuthenticationError as exc:
            log(f"FATAL  Auth failed — check credentials: {exc.message}")
            sys.exit(1)
        except (APIConnectionError, APITimeoutError) as exc:
            log(f"FATAL  Cannot reach API ({type(exc).__name__}): {exc.message}")
            sys.exit(1)
        except Exception as exc:
            log(f"ERROR  Unhandled exception: {exc}")
            raise
        finally:
            save_state(state)
            log("END    State saved. Bot complete.")
            log("=" * 64)


def main() -> None:
    asyncio.run(run_async())


if __name__ == "__main__":
    main()
