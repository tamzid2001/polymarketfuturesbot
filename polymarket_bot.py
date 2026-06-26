"""
Polymarket US Futures — async WebSocket-driven trading bot.

Architecture:
  markets WS  → market_data_lite drives take-profit / buyback checks
  markets WS  → trade events build per-slug price history (std dev)
  private WS  → position / balance snapshots keep local state current

Sync WS callbacks update shared dicts (GIL-safe); the async main loop
reads those dicts every 200 ms and fires REST calls when thresholds are hit.
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
STATUS_LOG_INTERVAL_S  = 900    # log status every 15 min
STATE_SAVE_INTERVAL_S  = 60     # persist state every 60 s
TICK_INTERVAL_S        = 0.2    # main-loop tick (200 ms)
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
                # Retry without Markdown on parse errors (400)
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
# State persistence
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
    """Return (markets_list, settings_dict) from markets.json."""
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
# Workflow rollover (self-trigger before 6 h runner limit)
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
        self._markets   = markets
        self._settings  = settings
        self._state     = state
        self._notifier  = notifier

        # Shared dicts updated by sync WS callbacks (GIL-safe for simple assignments)
        self._live_positions: dict[str, dict] = {}   # slug → position payload
        self._latest_bbo:     dict[str, float] = {}  # slug → latest bid
        self._price_history:  dict[str, deque] = {}  # slug → deque(maxlen=30)
        self._balance:        dict             = {}

        self._active_trades: set[str] = set()  # slugs with in-flight REST calls
        self._client: AsyncPolymarketUS | None = None
        self._start_time  = time.monotonic()

        # Tracked slugs for WS subscriptions
        self._tracked_slugs: list[str] = [m["market_slug"] for m in markets]

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    async def run(self, client: AsyncPolymarketUS) -> None:
        self._client = client
        log(f"INFO  Tracking {len(self._tracked_slugs)} market(s): {self._tracked_slugs}")

        await self._initialize_positions()
        await self._connect_ws()
        await self._main_loop()

    # ------------------------------------------------------------------
    # Startup: open positions for underdogs not yet held
    # ------------------------------------------------------------------

    async def _initialize_positions(self) -> None:
        log("INFO  Fetching live portfolio to check existing positions...")
        try:
            resp = await self._client.portfolio.positions()
            positions = resp.get("positions", {}) if isinstance(resp, dict) else {}
        except AuthenticationError as exc:
            log(f"ERROR  Authentication failed at startup: {exc.message}")
            raise
        except (APIConnectionError, APITimeoutError) as exc:
            log(f"WARN  Could not fetch portfolio at startup ({type(exc).__name__}): {exc.message}. Skipping auto-entry.")
            return
        except Exception as exc:
            log(f"WARN  Could not fetch portfolio at startup: {exc}. Skipping auto-entry.")
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
                log(f"SKIP  {market['team']} ({slug}) — position already open.")
                continue
            deployment = market.get("max_deployment_usd",
                                    self._settings.get("initial_deployment_usd", 5.0))
            await self._open_position(market, deployment)

    async def _open_position(self, market: dict, deployment_usd: float) -> None:
        slug = market["market_slug"]
        if slug in self._active_trades:
            return
        self._active_trades.add(slug)
        try:
            bbo = await self._client.markets.bbo(slug)
            ask = float((bbo.get("bestAsk") or {}).get("value", 0) or 0) if isinstance(bbo, dict) else 0
            if ask <= 0:
                log(f"WARN  No ask price for {slug} — skipping initial entry.")
                return
            qty = max(1, math.floor(deployment_usd / ask))
            log(f"INFO  Opening: {market['team']} ({slug}) qty={qty} @ ${ask:.4f} (~${deployment_usd:.2f})")
            resp = await self._client.orders.create({
                "marketSlug": slug,
                "intent":     "ORDER_INTENT_BUY_LONG",
                "type":       "ORDER_TYPE_LIMIT",
                "price":      {"value": f"{ask:.4f}", "currency": "USD"},
                "quantity":   qty,
                "tif":        "TIME_IN_FORCE_GOOD_TILL_CANCEL",
            })
            order_id = resp.get("id", "?") if isinstance(resp, dict) else "?"
            log(f"INFO  Opened {slug} order_id={order_id}")
            self._notifier.send(
                f"✅ *Position Opened*\n`{slug}`\nqty={qty} @ `${ask:.4f}` (~`${deployment_usd:.2f}`)"
            )
        except AuthenticationError as exc:
            log(f"ERROR  Auth failure opening {slug}: {exc.message}")
            raise
        except BadRequestError as exc:
            log(f"ERROR  Bad order params for {slug}: {exc.message}")
        except NotFoundError as exc:
            log(f"ERROR  Market not found {slug}: {exc.message}")
        except RateLimitError as exc:
            log(f"WARN  Rate limited opening {slug}: {exc.message}")
        except (APIConnectionError, APITimeoutError) as exc:
            log(f"WARN  Network error opening {slug} ({type(exc).__name__}): {exc.message}")
        except Exception as exc:
            log(f"ERROR  Unexpected error opening {slug}: {exc}")
        finally:
            self._active_trades.discard(slug)

    # ------------------------------------------------------------------
    # WebSocket connections
    # ------------------------------------------------------------------

    async def _connect_ws(self) -> None:
        await self._connect_private_ws()
        await self._connect_markets_ws()

    async def _connect_private_ws(self) -> None:
        try:
            ws = await self._client.ws.private()
            ws.on("position_snapshot", self._on_position_snapshot)
            ws.on("position_update",   self._on_position_update)
            ws.on("account_balance_snapshot", self._on_balance_snapshot)
            ws.on("account_balance_update",   self._on_balance_update)
            ws.on("close", lambda: asyncio.get_event_loop().create_task(self._reconnect_private()))
            await ws.subscribe_positions()
            await ws.subscribe_account_balance()
            asyncio.create_task(ws.listen())
            log("INFO  Private WebSocket connected.")
        except Exception as exc:
            log(f"WARN  Private WS failed to connect: {exc}. Will retry in 30 s.")
            asyncio.get_event_loop().call_later(30, lambda: asyncio.create_task(self._reconnect_private()))

    async def _connect_markets_ws(self) -> None:
        if not self._tracked_slugs:
            log("WARN  No tracked slugs — skipping markets WS.")
            return
        try:
            ws = await self._client.ws.markets()
            ws.on("market_data_lite", self._on_bbo_sync)
            ws.on("trade",            self._on_trade_sync)
            ws.on("close", lambda: asyncio.get_event_loop().create_task(self._reconnect_markets()))
            await ws.subscribe_market_data_lite(self._tracked_slugs)
            await ws.subscribe_trades(self._tracked_slugs)
            asyncio.create_task(ws.listen())
            log(f"INFO  Markets WebSocket connected ({len(self._tracked_slugs)} slugs).")
        except Exception as exc:
            log(f"WARN  Markets WS failed to connect: {exc}. Will retry in 30 s.")
            asyncio.get_event_loop().call_later(30, lambda: asyncio.create_task(self._reconnect_markets()))

    async def _reconnect_private(self) -> None:
        log("INFO  Reconnecting private WS...")
        await asyncio.sleep(5)
        await self._connect_private_ws()

    async def _reconnect_markets(self) -> None:
        log("INFO  Reconnecting markets WS...")
        await asyncio.sleep(5)
        await self._connect_markets_ws()

    # ------------------------------------------------------------------
    # Sync WS callbacks (called from WS thread — only mutate simple dicts)
    # ------------------------------------------------------------------

    def _on_position_snapshot(self, data: dict) -> None:
        positions = (data.get("positionSubscriptionSnapshot") or {}).get("positions", {}) or {}
        for slug, pos in positions.items():
            self._live_positions[slug] = pos
        log(f"INFO  Position snapshot: {len(positions)} position(s).")

    def _on_position_update(self, data: dict) -> None:
        upd  = data.get("positionSubscriptionUpdate") or {}
        slug = upd.get("marketSlug", "")
        pos  = upd.get("position")
        if slug and pos is not None:
            self._live_positions[slug] = pos

    def _on_balance_snapshot(self, data: dict) -> None:
        snap = data.get("accountBalanceSubscriptionSnapshot") or {}
        self._balance = {
            "balance":     snap.get("balance",    0),
            "buyingPower": snap.get("buyingPower", 0),
        }
        log(f"INFO  Balance snapshot: ${self._balance['balance']:.2f}  buying_power=${self._balance['buyingPower']:.2f}")

    def _on_balance_update(self, data: dict) -> None:
        upd = data.get("accountBalanceSubscriptionUpdate") or {}
        if upd:
            self._balance = {
                "balance":     upd.get("balance",    self._balance.get("balance", 0)),
                "buyingPower": upd.get("buyingPower", self._balance.get("buyingPower", 0)),
            }

    def _on_bbo_sync(self, data: dict) -> None:
        md   = data.get("marketDataLite") or {}
        slug = md.get("marketSlug", "")
        bid  = (md.get("bestBid") or {}).get("value")
        if slug and bid is not None:
            self._latest_bbo[slug] = float(bid)

    def _on_trade_sync(self, data: dict) -> None:
        trade = data.get("trade") or {}
        slug  = trade.get("marketSlug", "")
        price = (trade.get("price") or {}).get("value")
        if slug and price is not None:
            if slug not in self._price_history:
                self._price_history[slug] = deque(maxlen=PRICE_HISTORY_WINDOW)
            self._price_history[slug].append(float(price))

    # ------------------------------------------------------------------
    # Std dev
    # ------------------------------------------------------------------

    def _compute_std_dev(self, slug: str, fallback_entry: float = 0.0) -> float:
        history = list(self._price_history.get(slug, []))
        if len(history) >= 3:
            mean     = sum(history) / len(history)
            variance = sum((p - mean) ** 2 for p in history) / len(history)
            return math.sqrt(variance)
        base = fallback_entry or 0.0
        return base * BUYBACK_STD_DEV_PCT

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
        cost = float((pos.get("cost") or {}).get("value", 0) or 0)
        avg_entry = cost / qty if qty > 0 else 0
        if avg_entry <= 0:
            return
        threshold = avg_entry * TAKE_PROFIT_MULTIPLIER
        if bid < threshold:
            return

        log(f"TAKE-PROFIT {slug}: bid=${bid:.4f} >= {TAKE_PROFIT_MULTIPLIER}× ${avg_entry:.4f}")
        self._active_trades.add(slug)
        try:
            resp     = await self._client.orders.close_position({"marketSlug": slug})
            close_id = (resp.get("id", "?") if isinstance(resp, dict) else "?")
            profit   = (bid - avg_entry) * qty
            log(f"INFO  Position closed: {slug}  id={close_id}  est_profit=${profit:.2f}")

            meta  = pos.get("marketMetadata", {}) or {}
            event = meta.get("eventSlug", "")
            self._state.setdefault("pending_buybacks", []).append({
                "market_slug":     slug,
                "event_slug":      event,
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
                "slug":       slug,
                "qty":        int(qty),
                "avg_price":  round(avg_entry, 4),
                "exit_price": round(bid, 4),
                "profit":     round(profit, 2),
                "time":       datetime.now(EST).isoformat(),
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
            log(f"WARN  Position not found on close {slug}: {exc.message}")
        except RateLimitError as exc:
            log(f"WARN  Rate limited on close {slug}: {exc.message}")
        except (APIConnectionError, APITimeoutError) as exc:
            log(f"WARN  Network error closing {slug} ({type(exc).__name__}): {exc.message}")
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

        # Prefer stored std dev (captured at time of close) over recomputed
        std_dev = float(buyback.get("entry_std_dev") or 0) or self._compute_std_dev(slug, avg_entry)
        lower   = avg_entry - BUYBACK_STD_DEVS * std_dev
        upper   = avg_entry + BUYBACK_STD_DEVS * std_dev

        if not (lower <= bid <= upper):
            return

        log(f"BUYBACK {slug}: bid=${bid:.4f} in zone [${lower:.4f}, ${upper:.4f}]")
        self._active_trades.add(slug)
        try:
            qty_sold = int(buyback.get("qty_sold", 0) or 0)
            if qty_sold > 0 and avg_entry > 0:
                buy_qty   = qty_sold
                alloc_usd = round(buy_qty * bid, 2)
            else:
                buy_qty   = max(1, math.floor(BUYBACK_AMOUNT_USD / bid))
                alloc_usd = BUYBACK_AMOUNT_USD

            resp     = await self._client.orders.create({
                "marketSlug": slug,
                "intent":     buyback.get("intent", "ORDER_INTENT_BUY_LONG"),
                "type":       "ORDER_TYPE_LIMIT",
                "price":      {"value": f"{bid:.4f}", "currency": "USD"},
                "quantity":   buy_qty,
                "tif":        "TIME_IN_FORCE_GOOD_TILL_CANCEL",
            })
            order_id = resp.get("id", "?") if isinstance(resp, dict) else "?"
            log(f"INFO  Buyback order placed: {slug} qty={buy_qty} @ ${bid:.4f}  id={order_id}")
            buyback["processed"] = True
            self._notifier.send(
                f"🔄 *Buyback*\n`{slug}`\n"
                f"qty={buy_qty} @ `${bid:.4f}` (~`${alloc_usd:.2f}`)\n"
                f"Trigger: price back within {BUYBACK_STD_DEVS} std dev of `${avg_entry:.4f}`"
            )
        except AuthenticationError as exc:
            log(f"ERROR  Auth failure on buyback {slug}: {exc.message}")
            raise
        except BadRequestError as exc:
            buyback["failed_attempts"] = buyback.get("failed_attempts", 0) + 1
            log(f"ERROR  Bad order params for buyback {slug}: {exc.message}  (attempt #{buyback['failed_attempts']})")
        except RateLimitError as exc:
            log(f"WARN  Rate limited on buyback {slug}: {exc.message}")
        except (APIConnectionError, APITimeoutError) as exc:
            log(f"WARN  Network error on buyback {slug} ({type(exc).__name__}): {exc.message}")
        except Exception as exc:
            buyback["failed_attempts"] = buyback.get("failed_attempts", 0) + 1
            log(f"ERROR  Buyback {slug}: {exc}  (attempt #{buyback['failed_attempts']})")
        finally:
            self._active_trades.discard(slug)

    # ------------------------------------------------------------------
    # Scan all tracked markets each tick
    # ------------------------------------------------------------------

    async def _scan(self) -> None:
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
        snaps  = self._state.get("balance_snapshots", {})
        days   = sorted(snaps)
        t_snap = snaps.get(today, {})
        y_snap = snaps.get(days[-2], {}) if len(days) >= 2 and days[-1] == today else {}

        today_bal = float(t_snap.get("balance", 0))
        yest_bal  = float(y_snap.get("balance", 0))
        delta     = today_bal - yest_bal
        arrow     = "📈" if delta >= 0 else "📉"
        closed    = self._state.get("today_closed", [])
        total_pnl = sum(p.get("profit", 0) for p in closed)

        lines = [
            f"🌅 *Daily Report — {today}*",
            f"💰 Balance: `${today_bal:.2f}` {arrow} (`${delta:+.2f}` vs yesterday)",
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
                log(f"       {line}")

        self._state["daily_report_sent"] = today
        self._state["today_closed"]      = []

    # ------------------------------------------------------------------
    # Periodic status log
    # ------------------------------------------------------------------

    def _log_status(self, elapsed_s: float) -> None:
        remaining_min = (RUNTIME_LIMIT_SECONDS - elapsed_s) / 60
        pending       = len([b for b in self._state.get("pending_buybacks", []) if not b.get("processed")])
        log(
            f"STATUS  elapsed={elapsed_s/60:.1f} min  remaining={remaining_min:.1f} min  "
            f"balance=${self._balance.get('balance', 0):.2f}  "
            f"live_positions={len(self._live_positions)}  "
            f"pending_buybacks={pending}  "
            f"tracked_bbo={len(self._latest_bbo)}  "
            f"active_trades={len(self._active_trades)}"
        )

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def _main_loop(self) -> None:
        last_status = time.monotonic()
        last_save   = time.monotonic()

        while True:
            now     = time.monotonic()
            elapsed = now - self._start_time

            # --- Runtime rollover ---
            if elapsed >= RUNTIME_LIMIT_SECONDS:
                log(f"INFO  Runtime limit reached ({RUNTIME_LIMIT_SECONDS // 60} min). Initiating handoff...")
                save_state(self._state)
                trigger_workflow_handoff()
                log("INFO  Handoff complete. Exiting.")
                break

            # --- Core scan ---
            await self._scan()

            # --- Periodic state save ---
            if now - last_save >= STATE_SAVE_INTERVAL_S:
                save_state(self._state)
                self._snapshot_balance()
                last_save = time.monotonic()

            # --- Status log ---
            if now - last_status >= STATUS_LOG_INTERVAL_S:
                self._log_status(elapsed)
                last_status = time.monotonic()

            # --- Daily 10 AM report ---
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
    log("STARTUP  Polymarket Futures Bot (async WebSocket)")
    log(f"         Python {sys.version.split()[0]}  PID {os.getpid()}")
    log(f"CONFIG   TAKE_PROFIT_MULTIPLIER = {TAKE_PROFIT_MULTIPLIER}×")
    log(f"CONFIG   BUYBACK_AMOUNT_USD     = ${BUYBACK_AMOUNT_USD:.2f}")
    log(f"CONFIG   BUYBACK_STD_DEVS       = {BUYBACK_STD_DEVS}  ({BUYBACK_STD_DEV_PCT*100:.0f}% per dev)")
    log(f"CONFIG   RUNTIME_LIMIT          = {RUNTIME_LIMIT_SECONDS // 60} min")
    log(f"INFO     Telegram: {'ENABLED' if notifier.enabled else 'DISABLED (TELEGRAM_KEY not set)'}")
    log("=" * 64)

    markets, settings = load_markets()
    state             = load_state()
    bot               = PolymarketBot(markets, settings, state, notifier)

    async with AsyncPolymarketUS(key_id=api_key, secret_key=secret_key, timeout=30.0, max_retries=2) as client:
        try:
            await bot.run(client)
        except KeyboardInterrupt:
            log("INFO  KeyboardInterrupt — shutting down.")
        except AuthenticationError as exc:
            log(f"FATAL  Authentication failed — check POLYMARKET_PUBLIC_KEY / POLYMARKET_SECRET_KEY: {exc.message}")
            sys.exit(1)
        except (APIConnectionError, APITimeoutError) as exc:
            log(f"FATAL  Cannot reach Polymarket API ({type(exc).__name__}): {exc.message}")
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
