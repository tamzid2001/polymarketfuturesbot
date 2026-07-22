"""ML-side-selected KXBTC15M mechanical average-down trader.

The stored ML inference chooses one side before the market opens.  As soon as
that market is active, the execution rule mechanically posts one same-side,
market-close-expiring GTC limit at each fixed 40c -> 30c -> 20c -> 10c rung.
There is no Prophet, forecast, or mechanical-side fallback if ML inference is
unavailable.

Live submission is deliberately opt-in: ``DRY_RUN`` must be false and both
``--submit`` and ``--allow-live`` are required.  The GitHub workflow persists
its configuration and state so scheduled runs retain the latest manual share
amount and can reconcile resting/settled orders from previous windows.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import math
import os
import subprocess
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from kalshi_ladder_scalp_shadow import (
    EXTENDED_PROFIT_TARGETS,
    entry_summary as scalp_entry_summary,
    finalize_ladder_average_entry_scalp,
    new_ladder_average_entry_scalp_shadow,
    scalp_performance,
    simulate_ladder_average_entry_scalp,
)
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
CONFIG_VERSION = 12
STATE_VERSION = 7
ORDER_NAMESPACE = uuid.UUID("4d85857e-4dc6-43ec-960f-0b342523bdb7")
KALSHI_WS_URL = os.getenv(
    "KALSHI_WS_URL",
    "wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2"
    if os.getenv("KALSHI_DEMO", "false").lower() in {"1", "true", "yes"}
    else "wss://external-api-ws.kalshi.com/trade-api/ws/v2",
)
QUOTE_STALE_SECONDS = 20.0
MAINTENANCE_TIMEZONE = ZoneInfo("America/New_York")
EXCHANGE_RECOVERY_RETRY_SECONDS = 60.0
CHECKPOINT_RETRY_SECONDS = max(1.0, float(os.getenv("KALSHI_CHECKPOINT_RETRY_SECONDS", "60")))
# Polling metadata changes every few seconds and must never turn the bot-state
# branch into a stream of commits. Everything else is material trading state.
CHECKPOINT_IGNORED_KEYS = {
    "last_checked_at",
    "checked_at",
    "exchange_position_checked_at",
    "generated_at",
    "last_heartbeat_at",
    "pause_blocked_at",
    "last_quote_state",
}

DEFAULT_CONFIG = {
    "format_version": CONFIG_VERSION,
    # Contracts per rung.  This is a quantity, not a dollar amount.
    "initial_position_size": 0.01,
    "max_active_markets": 1,
    "max_contracts_per_market": 0.04,
    # Principal reserved for all four possible rungs.  Fees are checked against
    # available balance separately with fee_reserve.
    "max_total_capital": 0.01,
    "fee_reserve": 0.05,
    # Upper bound on sleep while waiting for the WebSocket; it is not a REST
    # quote-poll interval. Quote changes wake the runner immediately.
    "poll_seconds": 2.0,
    # REST is retained only for market discovery, settlement, and authoritative
    # order reconciliation if the stream is interrupted.
    "market_refresh_seconds": 15.0,
    "order_reconcile_seconds": 5.0,
    # This is *not* an entry window.  It is only the short allowance for
    # observing a brand-new market and starting its watcher. Once started,
    # a frozen ML side immediately receives its four-rung GTC ladder.
    "watch_start_grace_seconds": 45.0,
    # ML is computed before the next market opens from raw candles and settled
    # outcomes only. A watcher never chooses a side from prices; it only acts
    # after this frozen model side is ready.
    "ml_preopen_lead_seconds": 120.0,
    # Inclusive 50% confidence: every valid binary-model direction receives
    # the fixed GTC ladder once its market is active.
    "ml_min_confidence": 0.50,
    # A paper-only counterfactual of the *opposite* frozen ML side. It never
    # creates an exchange order. A simulated fill requires a fresh complete
    # top-of-book quote and displayed executable depth at the posted rung.
    "inverse_shadow_enabled": True,
    # Paper-only size is intentionally independent of the 0.01-contract live
    # ladder. It makes the counterfactual P&L readable without changing a
    # single live order or risk limit.
    "inverse_shadow_position_size": 1.0,
    "inverse_shadow_quote_max_age_seconds": 3.0,
    # An alternate paper-only range study for the frozen ML side. It mirrors
    # the 40c/30c/20c/10c entries at one share each and records the full
    # depth-supported favorable excursion at each held VWAP/size. It tracks
    # 1c/2c/3c/5c/10c targets but never submits an exchange close.
    "scalp_shadow_enabled": True,
    "scalp_shadow_position_size": 1.0,
    "scalp_shadow_profit_target": 0.01,
    "scalp_shadow_quote_max_age_seconds": 3.0,
    # A second, independent paper strategy with 1/2/3/4 contracts at
    # 40c/30c/20c/10c. Historical trailing results are retained but this
    # legacy study is disabled for new markets; the active comparison uses
    # the 5c fixed-loss / 10c activated-trailing bracket below.
    "weighted_scalp_shadow_enabled": True,
    "weighted_scalp_trailing_enabled": False,
    "weighted_scalp_trailing_stop_per_contract": 0.10,
    "weighted_scalp_trailing_activation_gain_per_contract": 0.10,
    # The active paper-only comparison holds identical weighted positions
    # until each separate +1c through +9c, then +10c/+20c/.../+80c gain gate
    # has been reached, then applies the same 10c trailing retracement. Every gate retains an
    # absolute selected-side 5c stop, rather than an average-entry loss gap.
    # It is never a live stop order.
    "weighted_scalp_activation_comparison_enabled": True,
    "weighted_scalp_trailing_activation_gains_per_contract": [
        0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09,
        0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80,
    ],
    "weighted_scalp_absolute_stop_price": 0.05,
    # Legacy one-gate bracket retained for archived/manual comparisons only.
    "weighted_scalp_fixed_stop_loss_enabled": False,
    "weighted_scalp_fixed_stop_loss_per_contract": 0.05,
    # When a retrain publishes a new artifact, paper-test both the retained
    # predecessor and the new model at the same readable size. Neither side
    # can create or alter an exchange order.
    "model_transition_shadow_enabled": True,
    "model_transition_shadow_position_size": 1.0,
    "status_log_seconds": 30.0,
}

# Paper-only asymmetric averaging schedule requested for the extended scalp
# study.  It deliberately has no relationship to the 0.01-contract live GTC
# ladder or its capital limits.
WEIGHTED_SCALP_RUNG_QUANTITIES = {0.40: 1.0, 0.30: 2.0, 0.20: 3.0, 0.10: 4.0}
DEFAULT_WEIGHTED_TRAILING_NORMAL_LEDGER = Path("kalshi_btc15m_weighted_trailing_normal_ledger.json")
DEFAULT_WEIGHTED_TRAILING_NORMAL_REPORT = Path("kalshi_btc15m_weighted_trailing_normal_report.json")
DEFAULT_WEIGHTED_TRAILING_INVERSE_LEDGER = Path("kalshi_btc15m_weighted_trailing_inverse_ledger.json")
DEFAULT_WEIGHTED_TRAILING_INVERSE_REPORT = Path("kalshi_btc15m_weighted_trailing_inverse_report.json")
DEFAULT_WEIGHTED_FIXED_STOP_NORMAL_LEDGER = Path("kalshi_btc15m_actual_price_bracket_normal_ledger.json")
DEFAULT_WEIGHTED_FIXED_STOP_NORMAL_REPORT = Path("kalshi_btc15m_actual_price_bracket_normal_report.json")
DEFAULT_WEIGHTED_FIXED_STOP_INVERSE_LEDGER = Path("kalshi_btc15m_actual_price_bracket_inverse_ledger.json")
DEFAULT_WEIGHTED_FIXED_STOP_INVERSE_REPORT = Path("kalshi_btc15m_actual_price_bracket_inverse_report.json")


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


def scheduled_trading_pause_active(now: datetime | None = None) -> bool:
    """Return whether Kalshi's documented Thursday maintenance window is open.

    A scheduled trading pause is exchange-wide, so an individual market can
    still look active in a market response while order creation is unavailable.
    The official window is Thursday 03:00--05:00 America/New_York.
    """
    value = now or datetime.now(tz=timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    eastern = value.astimezone(MAINTENANCE_TIMEZONE)
    return eastern.weekday() == 3 and 3 <= eastern.hour < 5


def pause_error(exc: Exception | str) -> bool:
    """Recognize the documented global trading/exchange-pause API failures."""
    message = str(exc).lower()
    markers = (
        "trading pause",
        "exchange pause",
        "exchange is paused",
        "trading is paused",
        "maintenance",
    )
    return any(marker in message for marker in markers)


def rest_pause_active(rest: Any) -> bool:
    """Ask a real REST adapter about a temporary pause; test doubles stay open."""
    checker = getattr(rest, "trading_pause_active", None)
    return bool(checker()) if callable(checker) else False


def market_can_start_watcher(market: Any, start_grace_seconds: float) -> bool:
    """Start a market watcher only at the market's opening, never late.

    The watcher is allowed to finish its ML-side setup after it starts. The
    small grace period absorbs normal market-discovery and handoff delay; it
    is not a price-entry deadline.
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


def checkpoint_projection(value: Any) -> Any:
    """Remove high-frequency audit timestamps before deciding to publish state."""
    if isinstance(value, dict):
        return {
            str(key): checkpoint_projection(item)
            for key, item in value.items()
            if str(key) not in CHECKPOINT_IGNORED_KEYS
        }
    if isinstance(value, list):
        return [checkpoint_projection(item) for item in value]
    return value


def checkpoint_fingerprint(state: dict[str, Any], config: dict[str, Any]) -> str:
    """Hash material strategy state without writing secrets or quote noise."""
    payload = {
        "state": checkpoint_projection(state),
        "config": checkpoint_projection(config),
    }
    encoded = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@dataclass
class StateCheckpointPublisher:
    """Durably publish material live-trading state from inside an Actions run.

    The normal end-of-run workflow commit remains in place. This publisher
    narrows the crash window by committing only meaningful events such as a
    locked side, an accepted rung, a fill, or settlement. It is deliberately
    disabled outside GitHub Actions and fails open with a warning: execution
    continues, while the watchdog remains available to recover the runner.
    """

    config_path: Path
    state_path: Path
    report_path: Path
    weighted_normal_ledger_path: Path
    weighted_normal_report_path: Path
    weighted_inverse_ledger_path: Path
    weighted_inverse_report_path: Path
    weighted_fixed_normal_ledger_path: Path
    weighted_fixed_normal_report_path: Path
    weighted_fixed_inverse_ledger_path: Path
    weighted_fixed_inverse_report_path: Path
    config: dict[str, Any]
    last_fingerprint: str
    enabled: bool
    last_attempt_at: float = float("-inf")

    @staticmethod
    def _rebase_checkpoint_paths(paths: list[str]) -> None:
        """Rebase a checkpoint, retaining only this publisher's own files.

        Independent Actions can publish a state snapshot at almost the same
        moment.  A plain `git pull --rebase` then leaves U entries in the
        worktree, which turns every later checkpoint into an error.  The paths
        passed here are isolated ledger files owned by this runner; during a
        rebase, `--theirs` is the local checkpoint commit being replayed.
        """
        pull = subprocess.run(
            ["git", "pull", "--rebase", "--autostash", "origin", "main"],
            check=False,
            capture_output=True,
            text=True,
        )
        if not pull.returncode:
            return
        diagnostic = (pull.stderr or pull.stdout).strip()
        allowed = set(paths)
        resolved_conflict = False
        while subprocess.run(
            ["git", "rev-parse", "-q", "--verify", "REBASE_HEAD"],
            check=False,
            capture_output=True,
            text=True,
        ).returncode == 0:
            conflicts = subprocess.run(
                ["git", "diff", "--name-only", "--diff-filter=U"],
                check=True,
                capture_output=True,
                text=True,
            ).stdout.splitlines()
            if not conflicts or any(path not in allowed for path in conflicts):
                subprocess.run(["git", "rebase", "--abort"], check=False, capture_output=True, text=True)
                raise RuntimeError(
                    "git pull --rebase could not safely resolve checkpoint conflict: "
                    + (", ".join(conflicts) if conflicts else diagnostic or "no conflicted paths")
                )
            try:
                subprocess.run(["git", "checkout", "--theirs", "--", *conflicts], check=True, capture_output=True, text=True)
                subprocess.run(["git", "add", "--", *conflicts], check=True, capture_output=True, text=True)
                subprocess.run(
                    ["git", "-c", "core.editor=true", "rebase", "--continue"],
                    check=True,
                    capture_output=True,
                    text=True,
                )
                resolved_conflict = True
            except subprocess.CalledProcessError as exc:
                subprocess.run(["git", "rebase", "--abort"], check=False, capture_output=True, text=True)
                raise RuntimeError("git rebase conflict resolution failed") from exc
        if resolved_conflict:
            return
        # A non-conflict pull failure (for example a transient network issue)
        # is still reported, but no unresolved index entries are left behind.
        raise RuntimeError(f"git pull --rebase failed: {diagnostic or 'no diagnostic'}")

    @classmethod
    def create(
        cls, config_path: Path, state_path: Path, report_path: Path,
        weighted_normal_ledger_path: Path, weighted_normal_report_path: Path,
        weighted_inverse_ledger_path: Path, weighted_inverse_report_path: Path,
        weighted_fixed_normal_ledger_path: Path, weighted_fixed_normal_report_path: Path,
        weighted_fixed_inverse_ledger_path: Path, weighted_fixed_inverse_report_path: Path,
        config: dict[str, Any], state: dict[str, Any],
    ) -> "StateCheckpointPublisher":
        enabled = (
            os.getenv("GITHUB_ACTIONS", "").lower() == "true"
            and os.getenv("KALSHI_CHECKPOINT_PUBLISH", "false").lower() in {"1", "true", "yes"}
        )
        return cls(
            config_path=config_path,
            state_path=state_path,
            report_path=report_path,
            weighted_normal_ledger_path=weighted_normal_ledger_path,
            weighted_normal_report_path=weighted_normal_report_path,
            weighted_inverse_ledger_path=weighted_inverse_ledger_path,
            weighted_inverse_report_path=weighted_inverse_report_path,
            weighted_fixed_normal_ledger_path=weighted_fixed_normal_ledger_path,
            weighted_fixed_normal_report_path=weighted_fixed_normal_report_path,
            weighted_fixed_inverse_ledger_path=weighted_fixed_inverse_ledger_path,
            weighted_fixed_inverse_report_path=weighted_fixed_inverse_report_path,
            config=config,
            last_fingerprint=checkpoint_fingerprint(state, config),
            enabled=enabled,
        )

    def publish_if_changed(self, state: dict[str, Any], reason: str) -> bool:
        current = checkpoint_fingerprint(state, self.config)
        if current == self.last_fingerprint:
            return False
        if not self.enabled:
            self.last_fingerprint = current
            return False
        now = time.monotonic()
        if now - self.last_attempt_at < CHECKPOINT_RETRY_SECONDS:
            return False
        self.last_attempt_at = now
        try:
            save_json(self.state_path, state)
            save_json(self.report_path, performance_report(state, self.config))
            save_ml_weighted_trailing_outputs(
                state, self.config,
                normal_ledger_path=self.weighted_normal_ledger_path,
                normal_report_path=self.weighted_normal_report_path,
                inverse_ledger_path=self.weighted_inverse_ledger_path,
                inverse_report_path=self.weighted_inverse_report_path,
            )
            save_ml_weighted_fixed_stop_outputs(
                state, self.config,
                normal_ledger_path=self.weighted_fixed_normal_ledger_path,
                normal_report_path=self.weighted_fixed_normal_report_path,
                inverse_ledger_path=self.weighted_fixed_inverse_ledger_path,
                inverse_report_path=self.weighted_fixed_inverse_report_path,
            )
            repository = subprocess.run(
                ["git", "rev-parse", "--show-toplevel"], check=True, capture_output=True, text=True,
            ).stdout.strip()
            root = Path(repository).resolve()
            paths = [str(path.resolve().relative_to(root)) for path in (
                self.config_path, self.state_path, self.report_path,
                self.weighted_normal_ledger_path, self.weighted_normal_report_path,
                self.weighted_inverse_ledger_path, self.weighted_inverse_report_path,
                self.weighted_fixed_normal_ledger_path, self.weighted_fixed_normal_report_path,
                self.weighted_fixed_inverse_ledger_path, self.weighted_fixed_inverse_report_path,
            )]
            subprocess.run(["git", "add", *paths], check=True, capture_output=True, text=True)
            diff = subprocess.run(
                ["git", "diff", "--cached", "--quiet"], capture_output=True, text=True,
            )
            if diff.returncode not in {0, 1}:
                raise RuntimeError(diff.stderr.strip() or "git diff --cached failed")
            if diff.returncode == 1:
                subprocess.run(
                    ["git", "commit", "-m", "chore: checkpoint BTC average-down state [skip ci]"],
                    check=True, capture_output=True, text=True,
                )
            for attempt in range(3):
                self._rebase_checkpoint_paths(paths)
                push = subprocess.run(
                    ["git", "push", "origin", "HEAD:main"], check=False, capture_output=True, text=True,
                )
                if not push.returncode:
                    break
                if attempt == 2:
                    raise RuntimeError(push.stderr.strip() or "git push failed")
                time.sleep(attempt + 1)
        except Exception as exc:  # noqa: BLE001
            LOG.warning("STATE CHECKPOINT FAILED | reason=%s error=%s; retrying after a later material event.", reason, exc)
            return False
        self.last_fingerprint = current
        LOG.info("STATE CHECKPOINTED | reason=%s fingerprint=%s", reason, current[:12])
        return True


def validate_config(config: dict[str, Any]) -> dict[str, Any]:
    merged = {**DEFAULT_CONFIG, **config, "format_version": CONFIG_VERSION}
    # Earlier versions used a short price-entry window. The persisted watcher
    # now exists only to attach the frozen ML direction and post the full GTC
    # ladder, so remove the retired setting when an old config is handed on.
    merged.pop("initial_entry_window_seconds", None)
    for name in (
        "initial_position_size", "max_contracts_per_market", "max_total_capital",
        "fee_reserve", "poll_seconds", "market_refresh_seconds", "order_reconcile_seconds",
        "watch_start_grace_seconds", "ml_preopen_lead_seconds", "ml_min_confidence",
        "inverse_shadow_position_size", "inverse_shadow_quote_max_age_seconds",
        "scalp_shadow_position_size", "scalp_shadow_profit_target", "scalp_shadow_quote_max_age_seconds",
        "weighted_scalp_trailing_stop_per_contract", "weighted_scalp_trailing_activation_gain_per_contract",
        "weighted_scalp_fixed_stop_loss_per_contract", "weighted_scalp_absolute_stop_price",
        "model_transition_shadow_position_size", "status_log_seconds",
    ):
        value = as_float(merged.get(name))
        if value is None or value <= 0:
            raise ValueError(f"{name} must be positive")
        merged[name] = value
    if merged["ml_min_confidence"] < 0.5 or merged["ml_min_confidence"] > 1.0:
        raise ValueError("ml_min_confidence must be between 0.5 and 1.0")
    shadow_enabled = merged.get("inverse_shadow_enabled", True)
    if isinstance(shadow_enabled, str):
        shadow_enabled = shadow_enabled.strip().lower() in {"1", "true", "yes", "on"}
    merged["inverse_shadow_enabled"] = bool(shadow_enabled)
    scalp_shadow_enabled = merged.get("scalp_shadow_enabled", True)
    if isinstance(scalp_shadow_enabled, str):
        scalp_shadow_enabled = scalp_shadow_enabled.strip().lower() in {"1", "true", "yes", "on"}
    merged["scalp_shadow_enabled"] = bool(scalp_shadow_enabled)
    weighted_scalp_enabled = merged.get("weighted_scalp_shadow_enabled", True)
    if isinstance(weighted_scalp_enabled, str):
        weighted_scalp_enabled = weighted_scalp_enabled.strip().lower() in {"1", "true", "yes", "on"}
    merged["weighted_scalp_shadow_enabled"] = bool(weighted_scalp_enabled)
    trailing_enabled = merged.get("weighted_scalp_trailing_enabled", False)
    if isinstance(trailing_enabled, str):
        trailing_enabled = trailing_enabled.strip().lower() in {"1", "true", "yes", "on"}
    merged["weighted_scalp_trailing_enabled"] = bool(trailing_enabled)
    fixed_stop_enabled = merged.get("weighted_scalp_fixed_stop_loss_enabled", True)
    if isinstance(fixed_stop_enabled, str):
        fixed_stop_enabled = fixed_stop_enabled.strip().lower() in {"1", "true", "yes", "on"}
    merged["weighted_scalp_fixed_stop_loss_enabled"] = bool(fixed_stop_enabled)
    activation_comparison_enabled = merged.get("weighted_scalp_activation_comparison_enabled", True)
    if isinstance(activation_comparison_enabled, str):
        activation_comparison_enabled = activation_comparison_enabled.strip().lower() in {"1", "true", "yes", "on"}
    merged["weighted_scalp_activation_comparison_enabled"] = bool(activation_comparison_enabled)
    raw_gains = merged.get("weighted_scalp_trailing_activation_gains_per_contract", [])
    if isinstance(raw_gains, str):
        raw_gains = [part.strip() for part in raw_gains.split(",") if part.strip()]
    if not isinstance(raw_gains, (list, tuple)):
        raise ValueError("weighted_scalp_trailing_activation_gains_per_contract must be a list")
    try:
        gains = sorted({round(float(gain), 6) for gain in raw_gains})
    except (TypeError, ValueError) as exc:
        raise ValueError("weighted_scalp_trailing_activation_gains_per_contract must contain numbers") from exc
    if not gains or any(gain <= 0.0 or gain >= 1.0 for gain in gains):
        raise ValueError("weighted_scalp_trailing_activation_gains_per_contract must be between zero and one")
    merged["weighted_scalp_trailing_activation_gains_per_contract"] = gains
    transition_shadow_enabled = merged.get("model_transition_shadow_enabled", True)
    if isinstance(transition_shadow_enabled, str):
        transition_shadow_enabled = transition_shadow_enabled.strip().lower() in {"1", "true", "yes", "on"}
    merged["model_transition_shadow_enabled"] = bool(transition_shadow_enabled)
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


def order_quantity(order: Any) -> float | None:
    """Read the original size, falling back to known filled plus resting size."""
    explicit = as_float(field(order, "initial_count_fp", "initial_count", "count_fp", "count", "quantity"))
    if explicit is not None and explicit > 0:
        return round(explicit, 2)
    filled = order_fill_count(order)
    remaining = order_remaining_count(order)
    reconstructed = filled + (remaining or 0.0)
    return round(reconstructed, 2) if reconstructed > 0 else None


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
            # A quote from a previous subscription must never be considered
            # executable for a newly watched ticker.
            for ticker in desired - self.desired_tickers:
                self.quotes.pop(ticker, None)
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

    def executable_shadow_quote(
        self, ticker: str, side: str, required_count: float, max_age_seconds: float,
    ) -> tuple[dict[str, Any] | None, str]:
        """Return auditable top-of-book evidence for one paper buy.

        The shadow deliberately treats a quote as executable only when the
        *same ticker message* supplied bid, ask, and both displayed sizes. It
        never synthesizes a price from a last trade, midpoint, or stale delta.
        """
        normalized_side = str(side).lower()
        if normalized_side not in {"yes", "no"}:
            return None, "invalid_side"
        quote = self.quotes.get(ticker)
        book = quote.get("complete_book") if isinstance(quote, dict) else None
        if not isinstance(book, dict):
            return None, "missing_complete_top_of_book"
        received = as_float(book.get("received_monotonic"))
        age = time.monotonic() - received if received is not None else float("inf")
        if age > max_age_seconds:
            return None, "stale_top_of_book"
        yes_bid = as_float(book.get("yes_bid"))
        yes_ask = as_float(book.get("yes_ask"))
        yes_bid_size = as_float(book.get("yes_bid_size"))
        yes_ask_size = as_float(book.get("yes_ask_size"))
        if any(value is None for value in (yes_bid, yes_ask, yes_bid_size, yes_ask_size)):
            return None, "incomplete_top_of_book"
        assert yes_bid is not None and yes_ask is not None
        assert yes_bid_size is not None and yes_ask_size is not None
        if not (0.0 < yes_bid <= yes_ask < 1.0 and yes_bid_size >= 0.0 and yes_ask_size >= 0.0):
            return None, "invalid_top_of_book"
        price = yes_ask if normalized_side == "yes" else 1.0 - yes_bid
        displayed_depth = yes_ask_size if normalized_side == "yes" else yes_bid_size
        if not (0.0 < price < 1.0):
            return None, "invalid_executable_price"
        if displayed_depth + 1e-9 < required_count:
            return None, "insufficient_displayed_depth"
        return {
            "quote_id": str(book.get("quote_id")),
            "ticker": ticker,
            "side": normalized_side,
            "economic_price": round(price, 4),
            "displayed_depth": round(displayed_depth, 4),
            "yes_bid": round(yes_bid, 4),
            "yes_ask": round(yes_ask, 4),
            "yes_bid_size": round(yes_bid_size, 4),
            "yes_ask_size": round(yes_ask_size, 4),
            "source_server_timestamp": book.get("source_server_timestamp"),
            "source_timestamp_ms": book.get("source_timestamp_ms"),
            "received_at": book.get("received_at"),
            "quote_age_seconds": round(max(0.0, age), 6),
        }, "executable_top_of_book"

    def executable_shadow_exit_quote(
        self, ticker: str, side: str, required_count: float, max_age_seconds: float,
    ) -> tuple[dict[str, Any] | None, str]:
        """Return fresh complete-book evidence for selling a paper YES/NO position.

        A YES exit takes the displayed YES bid; a NO exit takes the displayed
        NO bid, which is ``1 - yes_ask``.  ``required_count`` may be zero so a
        caller can first observe a valid bid and then let the paper simulator
        require depth for the position that actually filled on this update.
        """
        normalized_side = str(side).lower()
        if normalized_side not in {"yes", "no"}:
            return None, "invalid_side"
        if required_count < 0:
            return None, "invalid_required_count"
        quote = self.quotes.get(ticker)
        book = quote.get("complete_book") if isinstance(quote, dict) else None
        if not isinstance(book, dict):
            return None, "missing_complete_top_of_book"
        received = as_float(book.get("received_monotonic"))
        age = time.monotonic() - received if received is not None else float("inf")
        if age > max_age_seconds:
            return None, "stale_top_of_book"
        yes_bid = as_float(book.get("yes_bid"))
        yes_ask = as_float(book.get("yes_ask"))
        yes_bid_size = as_float(book.get("yes_bid_size"))
        yes_ask_size = as_float(book.get("yes_ask_size"))
        if any(value is None for value in (yes_bid, yes_ask, yes_bid_size, yes_ask_size)):
            return None, "incomplete_top_of_book"
        assert yes_bid is not None and yes_ask is not None
        assert yes_bid_size is not None and yes_ask_size is not None
        if not (0.0 < yes_bid <= yes_ask < 1.0 and yes_bid_size >= 0.0 and yes_ask_size >= 0.0):
            return None, "invalid_top_of_book"
        price = yes_bid if normalized_side == "yes" else 1.0 - yes_ask
        displayed_depth = yes_bid_size if normalized_side == "yes" else yes_ask_size
        if not (0.0 < price < 1.0):
            return None, "invalid_executable_exit_price"
        if displayed_depth + 1e-9 < required_count:
            return None, "insufficient_displayed_exit_depth"
        return {
            "quote_id": str(book.get("quote_id")),
            "ticker": ticker,
            "side": normalized_side,
            "economic_price": round(price, 4),
            "displayed_depth": round(displayed_depth, 4),
            "yes_bid": round(yes_bid, 4),
            "yes_ask": round(yes_ask, 4),
            "yes_bid_size": round(yes_bid_size, 4),
            "yes_ask_size": round(yes_ask_size, 4),
            "source_server_timestamp": book.get("source_server_timestamp"),
            "source_timestamp_ms": book.get("source_timestamp_ms"),
            "received_at": book.get("received_at"),
            "quote_age_seconds": round(max(0.0, age), 6),
        }, "executable_top_of_book"

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
        yes_bid_size = as_float(field(message, "yes_bid_size_fp", "yes_bid_size"))
        yes_ask_size = as_float(field(message, "yes_ask_size_fp", "yes_ask_size"))
        if yes_bid is not None:
            quote["yes_bid"] = yes_bid
        if yes_ask is not None:
            quote["yes_ask"] = yes_ask
        quote["received_monotonic"] = time.monotonic()
        # Preserve only a *complete snapshot* for executable-shadow fills.
        # Delta updates and last-trade-only messages are useful operational
        # signals, but cannot establish a displayed, executable quote.
        if all(value is not None for value in (yes_bid, yes_ask, yes_bid_size, yes_ask_size)):
            assert yes_bid is not None and yes_ask is not None
            assert yes_bid_size is not None and yes_ask_size is not None
            if 0.0 < yes_bid <= yes_ask < 1.0 and yes_bid_size >= 0.0 and yes_ask_size >= 0.0:
                sequence = int(quote.get("book_sequence") or 0) + 1
                received_at = now_iso()
                quote["book_sequence"] = sequence
                quote["complete_book"] = {
                    "quote_id": f"{ticker}:{sequence}:{received_at}",
                    "yes_bid": yes_bid,
                    "yes_ask": yes_ask,
                    "yes_bid_size": yes_bid_size,
                    "yes_ask_size": yes_ask_size,
                    "received_monotonic": time.monotonic(),
                    "received_at": received_at,
                    "source_server_timestamp": field(message, "time", "timestamp", "created_time"),
                    "source_timestamp_ms": field(message, "ts_ms", "timestamp_ms"),
                }
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
                        # New socket, new market-data sequence: do not reuse a
                        # quote that was received before this subscription.
                        self.quotes.clear()
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
        previous_model_path: Path | None = None,
        previous_model_metadata: dict[str, Any] | None = None,
        previous_model_run_id: str = "",
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
        self.previous_model_path = previous_model_path if previous_model_path and previous_model_path.is_file() else None
        self.previous_model_metadata = previous_model_metadata or {}
        self.previous_model_run_id = previous_model_run_id
        self.previous_model: Any | None = None
        self.previous_model_load_failed = False
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
                if ticker in self.ready:
                    self._log_once(
                        record, "confirmed",
                        "ML SIDE CONFIRMED | %s frozen side=%s p_yes=%.4f confidence=%.4f from this worker.",
                        ticker, side.upper(), probability_yes, confidence,
                    )
                    return side
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
        transition = self._model_transition_comparison(ml, vector, probability_yes, side)
        if transition is not None:
            record["ml_model_transition"] = transition
            LOG.info(
                "ML MODEL TRANSITION | %s prior_run=%s prior_side=%s prior_p_yes=%.4f current_run=%s "
                "current_side=%s current_p_yes=%.4f side_changed=%s.",
                ticker, transition["previous_model_run_id"], transition["previous_side"].upper(),
                transition["previous_probability_yes"], transition["current_model_run_id"],
                transition["current_side"].upper(), transition["current_probability_yes"], transition["side_changed"],
            )
        self._log_once(
            record, "ready",
            "ML SIDE READY | %s model=%s run=%s side=%s p_yes=%.4f confidence=%.4f "
            "gate=%.4f; full same-side GTC ladder will post when the market is active.",
            ticker, self.model_metadata.get("model_type", "unknown"), self.model_run_id or "unknown",
            side.upper(), probability_yes, confidence, self.min_confidence,
        )
        return side

    def _model_transition_comparison(
        self, ml: Any, vector: Any, current_probability_yes: float, current_side: str,
    ) -> dict[str, Any] | None:
        """Score the predecessor on the identical causal feature vector.

        A retrain never changes a running market. This audit starts only when
        the registry identifies a distinct previous artifact and the new
        runner has downloaded it alongside the active model.
        """
        if self.previous_model_path is None or not self.previous_model_run_id:
            return None
        if self.previous_model_run_id == self.model_run_id or self.previous_model_load_failed:
            return None
        try:
            if self.previous_model is None:
                self.previous_model = ml.load_saved_model(self.previous_model_path)
            previous_probability_yes = float(self.previous_model.predict_proba(vector)[0][1])
        except Exception as exc:  # noqa: BLE001
            self.previous_model_load_failed = True
            LOG.warning(
                "ML MODEL TRANSITION UNAVAILABLE | previous_run=%s path=%s error=%s; current model continues unchanged.",
                self.previous_model_run_id, self.previous_model_path, exc,
            )
            return None
        previous_side = "yes" if previous_probability_yes >= 0.5 else "no"
        return {
            "strategy": "same_frozen_input_model_transition_comparison_v1",
            "previous_model_run_id": self.previous_model_run_id,
            "previous_model_type": self.previous_model_metadata.get("model_type"),
            "previous_probability_yes": round(previous_probability_yes, 6),
            "previous_side": previous_side,
            "current_model_run_id": self.model_run_id or None,
            "current_model_type": self.model_metadata.get("model_type"),
            "current_probability_yes": round(current_probability_yes, 6),
            "current_side": current_side,
            "probability_yes_delta": round(current_probability_yes - previous_probability_yes, 6),
            "side_changed": previous_side != current_side,
            "input_basis": "identical frozen pre-open raw-candle/settled-outcome features",
            "compared_at": now_iso(),
        }

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
        self.pause_until = 0.0
        self.pause_reason: str | None = None

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

    def trading_pause_active(self) -> bool:
        """Respect scheduled maintenance and a recently observed global pause."""
        return scheduled_trading_pause_active() or time.time() < self.pause_until

    def note_trading_pause(self, reason: str) -> None:
        # An unscheduled pause has no fixed published end time. Back off for a
        # minute, then retry only if the market is still inside its opening
        # grace period. The regular Thursday pause is handled exactly above.
        self.pause_until = max(self.pause_until, time.time() + EXCHANGE_RECOVERY_RETRY_SECONDS)
        self.pause_reason = reason
        LOG.warning("KALSHI PAUSE DETECTED | %s; new ladders are paused.", reason)

    async def resting_mechanical_orders(self) -> list[tuple[Any, tuple[str, str]]]:
        """Return only live orders whose deterministic IDs belong to this bot."""
        try:
            response = await self.orders.get_orders(status="resting", limit=1000)
        except Exception as exc:  # noqa: BLE001
            LOG.error("EXCHANGE RECOVERY ORDER LOOKUP FAILED | %s", exc)
            raise
        recovered: list[tuple[Any, tuple[str, str]]] = []
        for order in field(response, "orders") or []:
            role = managed_mechanical_order_role(order)
            if role is not None:
                recovered.append((order, role))
        return recovered

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
        if self.trading_pause_active():
            record["status"] = "paused"
            record["error"] = self.pause_reason or "scheduled Kalshi trading pause"
            LOG.info("ORDER DEFERRED FOR PAUSE | %s %s @ $%.2f", ticker, side.upper(), position_price)
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
            if pause_error(exc):
                self.note_trading_pause(str(exc))
                record["status"] = "paused"
            else:
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
            "strategy": "ml_side_preposted_gtc_ladder_v2",
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
        "WATCH STARTED | %s awaiting its frozen ML side; once ready, it will immediately post that side's 40c/30c/20c/10c GTC ladder.",
        ticker,
    )
    return record


def orders_for_market(record: dict[str, Any]) -> list[dict[str, Any]]:
    return [order for order in (record.get("orders") or {}).values() if isinstance(order, dict)]


def filled_contracts(record: dict[str, Any]) -> float:
    return round(sum(float(order.get("fill_count") or 0.0) for order in orders_for_market(record)), 2)


def opposite_side(side: str) -> str | None:
    normalized = str(side).lower()
    return "no" if normalized == "yes" else ("yes" if normalized == "no" else None)


def new_quote_paper_shadow(
    *, strategy: str, side: str, quantity: float, market_close_time: Any, quote_max_age_seconds: float,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create an isolated, non-executable fixed-rung paper ladder."""
    return {
        "strategy": strategy,
        "mode": "paper_only_no_exchange_orders",
        "status": "active",
        "side": side,
        "created_at": now_iso(),
        "market_close_time": market_close_time,
        "quantity_per_rung": quantity,
        "quote_max_age_seconds": quote_max_age_seconds,
        "rungs": {
            f"{level:.4f}": {
                "rung_price": level,
                "quantity": quantity,
                "fill_count": 0.0,
                "average_fill_price": None,
                "status": "simulated_resting",
            }
            for level in LADDER_LEVELS
        },
        # Consumption is local to this *hypothetical* ladder. Separate model
        # shadows are alternatives, not simultaneous claims on exchange size.
        "quote_depth_consumed": {},
        **(extra or {}),
    }


def ensure_inverse_shadow(
    record: dict[str, Any], market: Any, config: dict[str, Any], ml_side: str,
) -> dict[str, Any] | None:
    """Start the paper-only opposite-side test for a frozen ML decision.

    This has no order IDs, no REST submission path, and is deliberately kept
    outside ``record['orders']`` so exchange reconciliation can never mistake
    it for a live position.
    """
    if not config.get("inverse_shadow_enabled", True):
        return None
    inverse_side = opposite_side(ml_side)
    if inverse_side is None:
        return None
    existing = record.get("inverse_ml_shadow")
    if isinstance(existing, dict):
        return existing
    quantity = float(config["inverse_shadow_position_size"])
    shadow = new_quote_paper_shadow(
        strategy="inverse_ml_executable_quote_shadow_v1", side=inverse_side, quantity=quantity,
        market_close_time=field(market, "close_time", "expected_expiration_time"),
        quote_max_age_seconds=float(config["inverse_shadow_quote_max_age_seconds"]),
        extra={"source_ml_side": str(ml_side).lower()},
    )
    record["inverse_ml_shadow"] = shadow
    LOG.info(
        "INVERSE SHADOW STARTED | %s ML=%s shadow=%s rungs=$0.40/$0.30/$0.20/$0.10 qty=%.2f; paper only, no exchange order.",
        record.get("ticker", "?"), str(ml_side).upper(), inverse_side.upper(), quantity,
    )
    return shadow


def simulate_quote_paper_shadow(
    record: dict[str, Any], shadow: dict[str, Any], feed: KalshiLiveFeed | None, *, label: str,
) -> bool:
    """Simulate one paper ladder only from fresh executable top-of-book data."""
    if shadow.get("status") != "active":
        return False
    if feed is None:
        shadow["last_quote_state"] = "websocket_unavailable"
        return False
    side = str(shadow.get("side") or "").lower()
    ticker = str(record.get("ticker") or "")
    quantity = as_float(shadow.get("quantity_per_rung"))
    if not ticker or side not in {"yes", "no"} or quantity is None or quantity <= 0:
        shadow["last_quote_state"] = "invalid_shadow_state"
        return False
    quote, quote_state = feed.executable_shadow_quote(
        ticker, side, quantity, float(shadow.get("quote_max_age_seconds") or 0.0),
    )
    shadow["last_quote_state"] = quote_state
    if quote is None:
        return False
    quote_id = str(quote["quote_id"])
    consumed = as_float((shadow.setdefault("quote_depth_consumed", {})).get(quote_id)) or 0.0
    available_depth = float(quote["displayed_depth"]) - consumed
    changed = False
    # A pre-posted buy limit at a higher rung would be eligible first. Use its
    # own posted limit as the fill price—never quote price improvement—to keep
    # paper P&L comparable to an actual limit order.
    for level in LADDER_LEVELS:
        key = f"{level:.4f}"
        rung = (shadow.get("rungs") or {}).get(key)
        if not isinstance(rung, dict) or float(rung.get("fill_count") or 0.0) > 0.004:
            continue
        if float(quote["economic_price"]) > level + 1e-9:
            continue
        rung_quantity = float(rung.get("quantity") or quantity)
        if available_depth + 1e-9 < rung_quantity:
            shadow["last_quote_state"] = "insufficient_remaining_displayed_depth"
            break
        rung.update({
            "fill_count": round(rung_quantity, 2),
            "average_fill_price": round(level, 4),
            "status": "simulated_executable_quote_hit",
            "filled_at": now_iso(),
            "simulation_quote": dict(quote),
        })
        consumed += rung_quantity
        available_depth -= rung_quantity
        shadow["quote_depth_consumed"][quote_id] = round(consumed, 4)
        changed = True
        LOG.info(
            "%s RUNG HIT | %s %s limit=$%.2f quote_price=$%.4f depth=%.2f quote_id=%s; paper fill %.2f, not an exchange fill.",
            label, ticker, side.upper(), level, float(quote["economic_price"]), float(quote["displayed_depth"]),
            quote_id, rung_quantity,
        )
    return changed


def simulate_inverse_shadow(
    record: dict[str, Any], feed: KalshiLiveFeed | None, config: dict[str, Any],
) -> bool:
    """Simulate inverse GTC rung fills from fresh executable book evidence."""
    shadow = record.get("inverse_ml_shadow")
    if not isinstance(shadow, dict):
        return False
    return simulate_quote_paper_shadow(record, shadow, feed, label="INVERSE SHADOW")


def finalize_quote_paper_shadow(record: dict[str, Any], shadow: dict[str, Any], result: str | None, *, label: str) -> bool:
    """Settle one non-executable paper ladder after authoritative market resolution."""
    resolved = str(result or "").lower()
    if shadow.get("status") != "active" or resolved not in {"yes", "no"}:
        return False
    rungs = shadow.get("rungs") if isinstance(shadow.get("rungs"), dict) else {}
    contracts = sum(float(rung.get("fill_count") or 0.0) for rung in rungs.values() if isinstance(rung, dict))
    cost = sum(
        float(rung.get("fill_count") or 0.0) * float(rung.get("average_fill_price") or rung.get("rung_price") or 0.0)
        for rung in rungs.values() if isinstance(rung, dict)
    )
    payout = contracts if str(shadow.get("side")) == resolved else 0.0
    gross = payout - cost
    shadow.update({
        "status": "finalized" if contracts > 0.004 else "finalized_unfilled",
        "settled_at": now_iso(),
        "settlement_outcome": resolved,
        "contracts": round(contracts, 2),
        "total_cost": round(cost, 6),
        "average_entry": round(cost / contracts, 6) if contracts > 0.004 else None,
        "gross_payout": round(payout, 6),
        "gross_profit_loss": round(gross, 6),
        # This is an executable-quote simulator, not an exchange fill record.
        # Fee/queue/hidden-liquidity effects must not be represented as known.
        "estimated_fees": 0.0,
        "fees_model": "excluded_no_exchange_fill",
        "net_profit_loss": round(gross, 6),
        "return_percentage": round(100.0 * gross / cost, 4) if cost > 0 else None,
    })
    LOG.info(
        "%s SETTLED | %s %s result=%s contracts=%.2f simulated_net=$%.4f fees=excluded; no exchange fill.",
        label, record.get("ticker", "?"), str(shadow.get("side") or "?").upper(), resolved.upper(), contracts, gross,
    )
    return True


def finalize_inverse_shadow(record: dict[str, Any], result: str | None) -> bool:
    """Settle the paper-only inverse ladder once Kalshi finalizes the market."""
    shadow = record.get("inverse_ml_shadow")
    if not isinstance(shadow, dict):
        return False
    return finalize_quote_paper_shadow(record, shadow, result, label="INVERSE SHADOW")


def ensure_ml_scalp_shadow(
    record: dict[str, Any], market: Any, config: dict[str, Any], ml_side: str,
) -> dict[str, Any] | None:
    """Start the normal-ML paper range study.

    The live GTC ladder remains completely independent.  This shadow is an
    alternative paper scenario with readable one-share rungs, created from the
    same pre-open-frozen ML side before any intramarket quote is inspected.
    """
    if not config.get("scalp_shadow_enabled", True) or ml_side not in {"yes", "no"}:
        return None
    existing = record.get("ml_ladder_scalp_shadow")
    if isinstance(existing, dict):
        return existing
    shadow = new_ladder_average_entry_scalp_shadow(
        strategy="ml_ladder_average_entry_scalp_executable_quote_shadow_v1",
        ticker=str(record.get("ticker") or ""), side=ml_side,
        quantity_per_rung=float(config["scalp_shadow_position_size"]),
        profit_target_per_contract=float(config["scalp_shadow_profit_target"]),
        quote_max_age_seconds=float(config["scalp_shadow_quote_max_age_seconds"]),
        market_close_time=field(market, "close_time", "expected_expiration_time"),
        observation_only=True,
        extra={"source_ml_side": ml_side, "source_model_run_id": record.get("ml_inference", {}).get("model_run_id")},
    )
    record["ml_ladder_scalp_shadow"] = shadow
    LOG.info(
        "ML LADDER SCALP RANGE STUDY STARTED | %s side=%s rungs=$0.40/$0.30/$0.20/$0.10 qty=%.2f "
        "observes depth-supported 1c/2c/3c/5c/10c exits and maximum excursion; paper only, no exchange order or close.",
        record.get("ticker", "?"), ml_side.upper(), float(config["scalp_shadow_position_size"]),
    )
    return shadow


def simulate_ml_scalp_shadow(
    record: dict[str, Any], feed: KalshiLiveFeed | None, config: dict[str, Any],
) -> bool:
    """Advance the separate ML scalp audit with fresh bid/ask/depth evidence."""
    shadow = record.get("ml_ladder_scalp_shadow")
    if not isinstance(shadow, dict) or shadow.get("status") != "active":
        return False
    if feed is None:
        shadow["last_entry_quote_state"] = "websocket_unavailable"
        shadow["last_exit_quote_state"] = "websocket_unavailable"
        return False
    ticker = str(record.get("ticker") or "")
    side = str(shadow.get("side") or "").lower()
    quantity = float(shadow.get("quantity_per_rung") or 0.0)
    quote_age = float(shadow.get("quote_max_age_seconds") or 0.0)
    if not ticker or side not in {"yes", "no"} or quantity <= 0.0 or quote_age <= 0.0:
        shadow["last_entry_quote_state"] = "invalid_shadow_state"
        shadow["last_exit_quote_state"] = "invalid_shadow_state"
        return False
    entry_quote, entry_state = feed.executable_shadow_quote(ticker, side, quantity, quote_age)
    # Query a valid bid independently of depth; the common simulator checks
    # that it can close the exact filled position after entry rungs are added.
    exit_quote, exit_state = feed.executable_shadow_exit_quote(ticker, side, 0.0, quote_age)
    events = simulate_ladder_average_entry_scalp(
        shadow, entry_quote=entry_quote, entry_quote_state=entry_state,
        exit_quote=exit_quote, exit_quote_state=exit_state,
    )
    for event in events:
        if event.get("kind") == "paper_scalp_entry_rung_hit":
            LOG.info(
                "ML SCALP PAPER ENTRY | %s %s rung=$%.2f ask=$%.4f depth=%.2f; paper only.",
                ticker, side.upper(), float(event["rung_price"]),
                float(event["entry_quote"]["economic_price"]), float(event["entry_quote"]["displayed_depth"]),
            )
        elif event.get("kind") == "paper_scalp_maximum_update":
            LOG.info(
                "ML SCALP RANGE MAX | %s %s contracts=%.2f avg=$%.4f bid=$%.4f "
                "gross_per_contract=$%+.4f gross_total=$%+.4f; fresh full-depth evidence only, no exchange close.",
                ticker, side.upper(), float(event["filled_contracts"]), float(event["average_entry_price"]),
                float(event["exit_price"]), float(event["gross_per_contract"]), float(event["gross_total"]),
            )
        elif event.get("kind") == "paper_scalp_target_hit":
            LOG.info(
                "ML SCALP RANGE TARGET HIT | %s %s contracts=%.2f avg=$%.4f target=+$%.2f bid=$%.4f "
                "gross_total=$%+.4f; observation only, no exchange close.",
                ticker, side.upper(), float(event["filled_contracts"]), float(event["average_entry_price"]),
                float(event["target_per_contract"]), float(event["observed_bid"]), float(event["observed_gross_total"]),
            )
    return bool(events)


def finalize_ml_scalp_shadow(record: dict[str, Any], result: str | None) -> bool:
    shadow = record.get("ml_ladder_scalp_shadow")
    if not isinstance(shadow, dict):
        return False
    changed = finalize_ladder_average_entry_scalp(shadow, result)
    if changed:
        LOG.info(
            "ML SCALP PAPER SETTLED | %s side=%s method=%s net=$%+.4f; no exchange fill.",
            record.get("ticker", "?"), str(shadow.get("side") or "?").upper(),
            shadow.get("exit_method", "?"), float(shadow.get("net_profit_loss") or 0.0),
        )
    return changed


def ensure_ml_weighted_trailing_scalp_shadow(
    record: dict[str, Any], market: Any, config: dict[str, Any], *, side: str, record_key: str, label: str,
) -> dict[str, Any] | None:
    """Create a frozen-side 1/2/3/4 paper ladder with a 10c trailing stop."""
    if (not config.get("weighted_scalp_shadow_enabled", True)
            or not config.get("weighted_scalp_trailing_enabled", False)
            or side not in {"yes", "no"}):
        return None
    existing = record.get(record_key)
    if isinstance(existing, dict):
        return existing
    shadow = new_ladder_average_entry_scalp_shadow(
        strategy=f"{label.lower().replace(' ', '_')}_weighted_trailing_scalp_shadow_v1",
        ticker=str(record.get("ticker") or ""),
        side=side,
        quantity_per_rung=1.0,
        profit_target_per_contract=float(config["scalp_shadow_profit_target"]),
        quote_max_age_seconds=float(config["scalp_shadow_quote_max_age_seconds"]),
        market_close_time=field(market, "close_time", "expected_expiration_time"),
        profit_targets_per_contract=EXTENDED_PROFIT_TARGETS,
        rung_quantities=WEIGHTED_SCALP_RUNG_QUANTITIES,
        trailing_stop_per_contract=float(config["weighted_scalp_trailing_stop_per_contract"]),
        extra={
            "source_ml_side": str(record.get("ml_inference", {}).get("side") or "").lower(),
            "source_model_run_id": record.get("ml_inference", {}).get("model_run_id"),
            "locked_study_side": side,
            "study_variant": label,
        },
    )
    record[record_key] = shadow
    LOG.info(
        "%s WEIGHTED TRAILING STUDY STARTED | %s locked_side=%s rungs=1x$0.40/2x$0.30/3x$0.20/4x$0.10 "
        "targets=1c/2c/3c/5c/10c/20c/30c/40c/50c/60c trailing_gap=$%.2f; paper only, no exchange order.",
        label, record.get("ticker", "?"), side.upper(), float(config["weighted_scalp_trailing_stop_per_contract"]),
    )
    return shadow


def ensure_ml_weighted_trailing_scalp_shadows(
    record: dict[str, Any], market: Any, config: dict[str, Any], ml_side: str,
) -> None:
    """Start separate normal and inverse studies from the same frozen ML signal."""
    ensure_ml_weighted_trailing_scalp_shadow(
        record, market, config, side=ml_side,
        record_key="ml_weighted_trailing_scalp_shadow", label="ML NORMAL",
    )
    inverse_side = opposite_side(ml_side)
    if inverse_side:
        ensure_ml_weighted_trailing_scalp_shadow(
            record, market, config, side=inverse_side,
            record_key="inverse_ml_weighted_trailing_scalp_shadow", label="ML INVERSE",
        )


def ensure_ml_weighted_fixed_stop_loss_shadow(
    record: dict[str, Any], market: Any, config: dict[str, Any], *, side: str, record_key: str, label: str,
) -> dict[str, Any] | None:
    """Create a frozen-side 1/2/3/4 actual-price 5c/10c paper bracket."""
    if (not config.get("weighted_scalp_shadow_enabled", True)
            or not config.get("weighted_scalp_fixed_stop_loss_enabled", True)
            or side not in {"yes", "no"}):
        return None
    existing = record.get(record_key)
    if isinstance(existing, dict):
        return existing
    shadow = new_ladder_average_entry_scalp_shadow(
        strategy=f"{label.lower().replace(' ', '_')}_weighted_fixed_stop_and_trailing_shadow_v2",
        ticker=str(record.get("ticker") or ""),
        side=side,
        quantity_per_rung=1.0,
        profit_target_per_contract=float(config["scalp_shadow_profit_target"]),
        quote_max_age_seconds=float(config["scalp_shadow_quote_max_age_seconds"]),
        market_close_time=field(market, "close_time", "expected_expiration_time"),
        profit_targets_per_contract=EXTENDED_PROFIT_TARGETS,
        rung_quantities=WEIGHTED_SCALP_RUNG_QUANTITIES,
        absolute_stop_price=float(config["weighted_scalp_absolute_stop_price"]),
        trailing_stop_per_contract=float(config["weighted_scalp_trailing_stop_per_contract"]),
        trailing_activation_gain_per_contract=float(config["weighted_scalp_trailing_activation_gain_per_contract"]),
        extra={
            "source_ml_side": str(record.get("ml_inference", {}).get("side") or "").lower(),
            "source_model_run_id": record.get("ml_inference", {}).get("model_run_id"),
            "locked_study_side": side,
            "study_variant": label,
        },
    )
    record[record_key] = shadow
    LOG.info(
        "%s WEIGHTED ACTUAL-PRICE BRACKET STARTED | %s locked_side=%s rungs=1x$0.40/2x$0.30/3x$0.20/4x$0.10 "
        "absolute_stop=$%.2f; trailing=$%.2f only after +$%.2f gain; fresh full-depth bid only; paper only.",
        label, record.get("ticker", "?"), side.upper(), float(config["weighted_scalp_absolute_stop_price"]),
        float(config["weighted_scalp_trailing_stop_per_contract"]),
        float(config["weighted_scalp_trailing_activation_gain_per_contract"]),
    )
    return shadow


def ensure_ml_weighted_fixed_stop_loss_shadows(
    record: dict[str, Any], market: Any, config: dict[str, Any], ml_side: str,
) -> None:
    """Start normal and inverse fixed-stop comparisons from one frozen signal."""
    ensure_ml_weighted_fixed_stop_loss_shadow(
        record, market, config, side=ml_side,
        record_key="ml_weighted_fixed_stop_loss_shadow", label="ML NORMAL",
    )
    inverse_side = opposite_side(ml_side)
    if inverse_side:
        ensure_ml_weighted_fixed_stop_loss_shadow(
            record, market, config, side=inverse_side,
            record_key="inverse_ml_weighted_fixed_stop_loss_shadow", label="ML INVERSE",
        )


def _activation_gain_key(gain: float) -> str:
    return f"{float(gain):.2f}"


def ensure_ml_weighted_activation_comparison_shadows(
    record: dict[str, Any], market: Any, config: dict[str, Any], ml_side: str,
) -> None:
    """Start parallel absolute-5c / 10c-trail hold-gate paper paths.

    Every path has the identical frozen side, rung schedule, and quote stream.
    Only its gain needed to arm the market-wide trailing stop differs.
    """
    if (not config.get("weighted_scalp_shadow_enabled", True)
            or not config.get("weighted_scalp_activation_comparison_enabled", True)
            or ml_side not in {"yes", "no"}):
        return
    for record_key, side, label, inverse in (
        ("ml_weighted_activation_comparison", ml_side, "ML NORMAL", False),
        ("inverse_ml_weighted_activation_comparison", opposite_side(ml_side), "ML INVERSE", True),
    ):
        if side not in {"yes", "no"}:
            continue
        variants = record.setdefault(record_key, {})
        if not isinstance(variants, dict):
            continue
        for gain in config["weighted_scalp_trailing_activation_gains_per_contract"]:
            key = _activation_gain_key(gain)
            if isinstance(variants.get(key), dict):
                continue
            variants[key] = new_ladder_average_entry_scalp_shadow(
                strategy=("inverse" if inverse else "normal") + "_ml_weighted_1234_hold_gate_trailing_v3",
                ticker=str(record.get("ticker") or ""), side=side, quantity_per_rung=1.0,
                profit_target_per_contract=float(config["scalp_shadow_profit_target"]),
                quote_max_age_seconds=float(config["scalp_shadow_quote_max_age_seconds"]),
                market_close_time=field(market, "close_time", "expected_expiration_time"),
                profit_targets_per_contract=EXTENDED_PROFIT_TARGETS,
                rung_quantities=WEIGHTED_SCALP_RUNG_QUANTITIES,
                absolute_stop_price=float(config["weighted_scalp_absolute_stop_price"]),
                trailing_stop_per_contract=float(config["weighted_scalp_trailing_stop_per_contract"]),
                trailing_activation_gain_per_contract=float(gain),
                extra={
                    "source_ml_side": str(record.get("ml_inference", {}).get("side") or "").lower(),
                    "source_model_run_id": record.get("ml_inference", {}).get("model_run_id"),
                    "locked_study_side": side, "study_variant": label,
                    "hold_gain_gate_per_contract": float(gain),
                    "absolute_stop_price": float(config["weighted_scalp_absolute_stop_price"]),
                },
            )
            LOG.info(
                "%s HOLD-GATE STUDY STARTED | %s locked_side=%s gate=+$%.2f absolute_stop=$%.2f trailing=$%.2f; paper only.",
                label, record.get("ticker", "?"), side.upper(), float(gain),
                float(config["weighted_scalp_absolute_stop_price"]),
                float(config["weighted_scalp_trailing_stop_per_contract"]),
            )


def _activation_comparison_variants(record: dict[str, Any], *, inverse: bool) -> dict[str, dict[str, Any]]:
    key = "inverse_ml_weighted_activation_comparison" if inverse else "ml_weighted_activation_comparison"
    variants = record.get(key)
    return variants if isinstance(variants, dict) else {}


def simulate_ml_weighted_activation_comparison(
    record: dict[str, Any], feed: KalshiLiveFeed | None, *, inverse: bool,
) -> bool:
    """Advance each independent ML hold-gate path from the same fresh book."""
    changed = False
    for gain_key, shadow in _activation_comparison_variants(record, inverse=inverse).items():
        if not isinstance(shadow, dict) or shadow.get("status") != "active":
            continue
        ticker = str(record.get("ticker") or "")
        side = str(shadow.get("side") or "").lower()
        quote_age = float(shadow.get("quote_max_age_seconds") or 0.0)
        if feed is None:
            shadow["last_entry_quote_state"] = shadow["last_exit_quote_state"] = "websocket_unavailable"
            continue
        if not ticker or side not in {"yes", "no"} or quote_age <= 0.0:
            shadow["last_entry_quote_state"] = shadow["last_exit_quote_state"] = "invalid_shadow_state"
            continue
        entry_quote, entry_state = feed.executable_shadow_quote(ticker, side, 1.0, quote_age)
        exit_quote, exit_state = feed.executable_shadow_exit_quote(ticker, side, 0.0, quote_age)
        events = simulate_ladder_average_entry_scalp(
            shadow, entry_quote=entry_quote, entry_quote_state=entry_state,
            exit_quote=exit_quote, exit_quote_state=exit_state,
        )
        changed = changed or bool(events)
        for event in events:
            if event.get("kind") not in {"paper_scalp_fixed_stop_loss_exit", "paper_scalp_trailing_stop_exit"}:
                continue
            LOG.info(
                "ML %s HOLD-GATE EXIT | %s side=%s gate=+$%s method=%s observed_bid=$%.4f net=$%+.4f; paper only.",
                "INVERSE" if inverse else "NORMAL", ticker, side.upper(), gain_key,
                event.get("kind"), float(event.get("exit_price") or 0.0),
                float(event.get("gross_profit_loss") or 0.0),
            )
    return changed


def finalize_ml_weighted_activation_comparison(
    record: dict[str, Any], result: str | None, *, inverse: bool,
) -> bool:
    changed = False
    for shadow in _activation_comparison_variants(record, inverse=inverse).values():
        if isinstance(shadow, dict):
            changed = finalize_ladder_average_entry_scalp(shadow, result) or changed
    return changed


def simulate_ml_weighted_trailing_scalp_shadow(
    record: dict[str, Any], feed: KalshiLiveFeed | None, *, record_key: str, label: str,
) -> bool:
    """Advance one locked-side weighted trailing or fixed-stop paper position."""
    shadow = record.get(record_key)
    if not isinstance(shadow, dict) or shadow.get("status") != "active":
        return False
    if feed is None:
        shadow["last_entry_quote_state"] = "websocket_unavailable"
        shadow["last_exit_quote_state"] = "websocket_unavailable"
        return False
    ticker = str(record.get("ticker") or "")
    side = str(shadow.get("side") or "").lower()
    quote_age = float(shadow.get("quote_max_age_seconds") or 0.0)
    if not ticker or side not in {"yes", "no"} or quote_age <= 0.0:
        shadow["last_entry_quote_state"] = "invalid_shadow_state"
        shadow["last_exit_quote_state"] = "invalid_shadow_state"
        return False
    # The first rung is one contract. The common simulator then consumes the
    # same quote's displayed depth across 1/2/3/4 contracts without reuse.
    entry_quote, entry_state = feed.executable_shadow_quote(ticker, side, 1.0, quote_age)
    exit_quote, exit_state = feed.executable_shadow_exit_quote(ticker, side, 0.0, quote_age)
    events = simulate_ladder_average_entry_scalp(
        shadow, entry_quote=entry_quote, entry_quote_state=entry_state,
        exit_quote=exit_quote, exit_quote_state=exit_state,
    )
    for event in events:
        if event.get("kind") == "paper_scalp_entry_rung_hit":
            LOG.info(
                "%s WEIGHTED ENTRY | %s %s rung=$%.2f qty=%.2f ask=$%.4f depth=%.2f; paper only.",
                label, ticker, side.upper(), float(event["rung_price"]), float(event["quantity"]),
                float(event["entry_quote"]["economic_price"]), float(event["entry_quote"]["displayed_depth"]),
            )
        elif event.get("kind") == "paper_scalp_maximum_update":
            LOG.info(
                "%s WEIGHTED MAX | %s %s contracts=%.2f avg=$%.4f bid=$%.4f gross_per_contract=$%+.4f "
                "gross_total=$%+.4f; fresh full-depth evidence only.",
                label, ticker, side.upper(), float(event["filled_contracts"]), float(event["average_entry_price"]),
                float(event["exit_price"]), float(event["gross_per_contract"]), float(event["gross_total"]),
            )
        elif event.get("kind") == "paper_scalp_target_hit":
            LOG.info(
                "%s WEIGHTED TARGET HIT | %s %s avg=$%.4f target=+$%.2f bid=$%.4f gross_total=$%+.4f; paper only.",
                label, ticker, side.upper(), float(event["average_entry_price"]), float(event["target_per_contract"]),
                float(event["observed_bid"]), float(event["observed_gross_total"]),
            )
        elif event.get("kind") == "paper_scalp_trailing_stop_exit":
            LOG.info(
                "%s WEIGHTED TRAILING STOP | %s %s contracts=%.2f high_bid=$%.4f stop_bid=$%.4f "
                "observed_bid=$%.4f net=$%+.4f; paper only, no exchange close.",
                label, ticker, side.upper(), float(event["filled_contracts"]), float(event["highest_executable_bid"]),
                float(event["trailing_stop_bid"]), float(event["exit_price"]), float(event["gross_profit_loss"]),
            )
        elif event.get("kind") == "paper_scalp_fixed_stop_loss_exit":
            LOG.info(
                "%s WEIGHTED FIXED STOP | %s %s contracts=%.2f avg=$%.4f stop_bid=$%.4f "
                "observed_bid=$%.4f net=$%+.4f; paper only, no exchange close.",
                label, ticker, side.upper(), float(event["filled_contracts"]), float(event["average_entry_price"]),
                float(event["fixed_stop_loss_bid"]), float(event["exit_price"]), float(event["gross_profit_loss"]),
            )
    return bool(events)


def finalize_ml_weighted_trailing_scalp_shadow(
    record: dict[str, Any], result: str | None, *, record_key: str, label: str,
) -> bool:
    shadow = record.get(record_key)
    if not isinstance(shadow, dict):
        return False
    changed = finalize_ladder_average_entry_scalp(shadow, result)
    if changed:
        LOG.info(
            "%s WEIGHTED PAPER SETTLED | %s side=%s method=%s net=$%+.4f; no exchange fill.",
            label, record.get("ticker", "?"), str(shadow.get("side") or "?").upper(),
            shadow.get("exit_method", "?"), float(shadow.get("net_profit_loss") or 0.0),
        )
    return changed


def ensure_model_transition_shadow(record: dict[str, Any], market: Any, config: dict[str, Any]) -> dict[str, Any] | None:
    """Paper-test both retained predecessor and new model on identical inputs.

    The active model can remain live at its own 0.01-contract risk setting.
    This paired comparison is separately sized, has no order IDs, and does
    not reserve capital or participate in exchange reconciliation.
    """
    if not config.get("model_transition_shadow_enabled", True):
        return None
    transition = record.get("ml_model_transition")
    if not isinstance(transition, dict):
        return None
    previous_side = str(transition.get("previous_side") or "").lower()
    current_side = str(transition.get("current_side") or "").lower()
    previous_run = str(transition.get("previous_model_run_id") or "")
    current_run = str(transition.get("current_model_run_id") or "")
    if previous_side not in {"yes", "no"} or current_side not in {"yes", "no"} or not previous_run or not current_run:
        return None
    existing = record.get("ml_model_transition_shadow")
    if isinstance(existing, dict):
        return existing
    quantity = float(config["model_transition_shadow_position_size"])
    quote_age = float(config["inverse_shadow_quote_max_age_seconds"])
    close_time = field(market, "close_time", "expected_expiration_time")
    previous = new_quote_paper_shadow(
        strategy="previous_model_executable_quote_shadow_v1", side=previous_side, quantity=quantity,
        market_close_time=close_time, quote_max_age_seconds=quote_age,
        extra={
            "model_run_id": previous_run,
            "model_type": transition.get("previous_model_type"),
            "probability_yes": transition.get("previous_probability_yes"),
        },
    )
    current = new_quote_paper_shadow(
        strategy="current_model_executable_quote_shadow_v1", side=current_side, quantity=quantity,
        market_close_time=close_time, quote_max_age_seconds=quote_age,
        extra={
            "model_run_id": current_run,
            "model_type": transition.get("current_model_type"),
            "probability_yes": transition.get("current_probability_yes"),
        },
    )
    pair = {
        "strategy": "paired_model_transition_executable_quote_shadow_v1",
        "mode": "paper_only_no_exchange_orders",
        "status": "active",
        "created_at": now_iso(),
        "input_basis": transition.get("input_basis"),
        "previous_model": previous,
        "current_model": current,
        "side_changed": previous_side != current_side,
        "market_close_time": close_time,
        "limitation": "Each model is an independent hypothetical ladder; neither creates an exchange order.",
    }
    record["ml_model_transition_shadow"] = pair
    LOG.info(
        "ML TRANSITION SHADOW STARTED | %s previous_run=%s side=%s current_run=%s side=%s qty=%.2f; "
        "paired paper only, no exchange orders.",
        record.get("ticker", "?"), previous_run, previous_side.upper(), current_run, current_side.upper(), quantity,
    )
    return pair


def simulate_model_transition_shadow(record: dict[str, Any], feed: KalshiLiveFeed | None, config: dict[str, Any]) -> bool:
    pair = record.get("ml_model_transition_shadow")
    if not isinstance(pair, dict) or pair.get("status") != "active":
        return False
    changed = False
    states: dict[str, Any] = {}
    for role, label in (("previous_model", "PREVIOUS MODEL SHADOW"), ("current_model", "CURRENT MODEL SHADOW")):
        shadow = pair.get(role)
        if not isinstance(shadow, dict):
            continue
        changed = simulate_quote_paper_shadow(record, shadow, feed, label=label) or changed
        states[role] = shadow.get("last_quote_state")
    pair["last_quote_states"] = states
    return changed


def finalize_model_transition_shadow(record: dict[str, Any], result: str | None) -> bool:
    pair = record.get("ml_model_transition_shadow")
    if not isinstance(pair, dict) or pair.get("status") != "active":
        return False
    changed = False
    for role, label in (("previous_model", "PREVIOUS MODEL SHADOW"), ("current_model", "CURRENT MODEL SHADOW")):
        shadow = pair.get(role)
        if isinstance(shadow, dict):
            changed = finalize_quote_paper_shadow(record, shadow, result, label=label) or changed
    if changed:
        pair.update({"status": "finalized", "settled_at": now_iso(), "settlement_outcome": str(result).lower()})
    return changed


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


def recovered_order_record(order: Any, side: str, role: str) -> dict[str, Any] | None:
    """Convert a resting exchange order into the exact local rung ledger form."""
    key = "0.4000" if role == "initial" else role
    level = as_float(key)
    quantity = order_quantity(order)
    if level is None or quantity is None:
        return None
    fill_count = order_fill_count(order)
    remaining = order_remaining_count(order)
    if remaining is None:
        return None
    status = normalized_order_status(field(order, "status")) or classify_submission(
        fill_count, remaining, quantity, "good_till_canceled",
    )
    ticker = str(field(order, "ticker") or "")
    return {
        "client_order_id": str(field(order, "client_order_id") or client_order_id(ticker, side, role)),
        "ticker": ticker,
        "side": side,
        "expected_outcome_side": side,
        "order_id": str(field(order, "order_id") or "") or None,
        "order_type": "limit",
        "position_price": round(level, 4),
        "api_price": side_api_price(side, level),
        "quantity": quantity,
        "time_in_force": "good_till_canceled",
        "fill_count": round(fill_count, 2),
        "remaining_count": round(remaining, 2),
        "average_fill_price": order_average_position_price(order, side, level),
        "fees_paid": order_fee_total(order),
        "status": status,
        "ladder_level": level,
        "recovered_from_exchange_at": now_iso(),
    }


async def recover_exchange_state(rest: KalshiREST, state: dict[str, Any], config: dict[str, Any], dry_run: bool) -> bool:
    """Reattach deterministic resting GTC rungs before any new live order.

    State is useful but Kalshi is authoritative after a runner interruption.
    Recovery never creates a missing rung. A mixed-side, malformed, or
    position-mismatched ticker is quarantined so the next process cannot turn
    uncertainty into a duplicate order.
    """
    recovery = state.setdefault("exchange_recovery", {})
    if dry_run:
        recovery.update({"status": "dry_run", "checked_at": now_iso()})
        return True
    lookup = getattr(rest, "resting_mechanical_orders", None)
    if not callable(lookup):
        recovery.update({"status": "blocked", "checked_at": now_iso(), "reason": "order recovery unsupported"})
        LOG.critical("EXCHANGE RECOVERY BLOCKED | adapter cannot inspect resting deterministic orders; no new ladders allowed.")
        return False
    try:
        live_orders = await lookup()
    except Exception as exc:  # noqa: BLE001
        recovery.update({"status": "blocked", "checked_at": now_iso(), "reason": str(exc)})
        LOG.critical("EXCHANGE RECOVERY BLOCKED | resting-order lookup failed; no new ladders allowed.")
        return False

    grouped: dict[str, list[tuple[Any, tuple[str, str]]]] = {}
    for order, role in live_orders:
        ticker = str(field(order, "ticker") or "")
        if ticker:
            grouped.setdefault(ticker, []).append((order, role))

    recovered_count = 0
    blocked_count = 0
    for ticker, entries in grouped.items():
        sides = {role[0] for _, role in entries}
        keys = ["0.4000" if role[1] == "initial" else role[1] for _, role in entries]
        records = [recovered_order_record(order, role[0], role[1]) for order, role in entries]
        if len(sides) != 1 or len(set(keys)) != len(keys) or any(item is None for item in records):
            record = market_record(state, ticker)
            record.update({
                "status": "recovery_blocked_ambiguous",
                "exchange_recovery_blocked": "mixed side, duplicate role, or malformed resting order",
                "recovered_at": now_iso(),
            })
            blocked_count += 1
            LOG.critical("EXCHANGE RECOVERY QUARANTINE | %s has ambiguous owned resting orders; no new orders for this ticker.", ticker)
            continue

        side = next(iter(sides))
        typed_orders = [item for item in records if item is not None]
        quantities = {float(item["quantity"]) for item in typed_orders}
        record = market_record(state, ticker)
        existing_side = record.get("locked_side") or record.get("candidate_side")
        if len(quantities) != 1 or existing_side not in {None, side}:
            record.update({
                "status": "recovery_blocked_ambiguous",
                "exchange_recovery_blocked": "quantity or side conflicts with persisted ledger",
                "recovered_at": now_iso(),
            })
            blocked_count += 1
            LOG.critical("EXCHANGE RECOVERY QUARANTINE | %s conflicts with persisted side/quantity; no new orders.", ticker)
            continue

        market = await rest.get_market(ticker)
        record.update({
            "candidate_side": side,
            "locked_side": side,
            "quantity": quantities.pop(),
            "status": "ladder_active",
            "ladder_mode": "preposted_gtc_v2",
            "reserved_principal": ladder_principal(float(typed_orders[0]["quantity"])),
            "market_open_time": field(market, "open_time") if market is not None else record.get("market_open_time"),
            "market_close_time": field(market, "close_time", "expected_expiration_time") if market is not None else record.get("market_close_time"),
            "recovered_at": now_iso(),
            "recovery_source": "Kalshi resting deterministic client-order IDs",
        })
        for item in typed_orders:
            record.setdefault("orders", {})[f"{float(item['ladder_level']):.4f}"] = item
        if not await exchange_position_guard(rest, record, config):
            record["status"] = "recovery_blocked_exchange_position"
            blocked_count += 1
            LOG.critical("EXCHANGE RECOVERY QUARANTINE | %s exchange position does not match recovered rungs.", ticker)
            continue
        recovered_count += len(typed_orders)
        LOG.info(
            "EXCHANGE RECOVERY ATTACHED | %s side=%s resting_rungs=%s; no duplicate ladder will be sent.",
            ticker, side.upper(), "/".join(f"${item['ladder_level']:.2f}" for item in typed_orders),
        )

    recovery.update({
        "status": "ready",
        "checked_at": now_iso(),
        "resting_rungs_attached": recovered_count,
        "quarantined_tickers": blocked_count,
    })
    LOG.info("EXCHANGE RECOVERY READY | attached_rungs=%d quarantined_tickers=%d", recovered_count, blocked_count)
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
    """Refresh fills and retain the frozen/pre-posted single market side."""
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
    # Version 2 posts the complete 40/30/20/10 GTC ladder atomically from the
    # strategy's point of view. Retain this function only to reconcile a
    # partially completed record from the older fill-then-ladder version.
    if record.get("ladder_mode") == "preposted_gtc_v2":
        return
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
    result = market_result(market)
    market_status = str(field(market, "status") or "").lower()
    if result is not None and market_status == "finalized":
        finalize_inverse_shadow(record, result)
        finalize_ml_scalp_shadow(record, result)
        finalize_ml_weighted_trailing_scalp_shadow(
            record, result, record_key="ml_weighted_trailing_scalp_shadow", label="ML NORMAL")
        finalize_ml_weighted_trailing_scalp_shadow(
            record, result, record_key="inverse_ml_weighted_trailing_scalp_shadow", label="ML INVERSE")
        finalize_ml_weighted_trailing_scalp_shadow(
            record, result, record_key="ml_weighted_fixed_stop_loss_shadow", label="ML NORMAL BRACKET")
        finalize_ml_weighted_trailing_scalp_shadow(
            record, result, record_key="inverse_ml_weighted_fixed_stop_loss_shadow", label="ML INVERSE BRACKET")
        finalize_ml_weighted_activation_comparison(record, result, inverse=False)
        finalize_ml_weighted_activation_comparison(record, result, inverse=True)
        finalize_model_transition_shadow(record, result)
    if not record.get("candidate_side") and not orders_for_market(record):
        if result is None or market_status != "finalized":
            if prior_status != "closed_waiting_finalization":
                LOG.info("WATCH CLOSED | %s no side reached $0.40; awaiting final market status.", record["ticker"])
            return
        record.update({
            "status": "finalized_no_signal", "settled_at": now_iso(), "settlement_outcome": result,
            "contracts": 0.0, "total_cost": 0.0, "average_entry": None,
            "gross_payout": 0.0, "gross_profit_loss": 0.0, "kalshi_fees": 0.0,
            "net_profit_loss": 0.0, "return_percentage": None,
        })
        LOG.info("WATCH COMPLETE | %s settled %s with no valid frozen ML side; no GTC ladder submitted.",
                 record["ticker"], result.upper())
        return
    for order in orders_for_market(record):
        await rest.cancel_order(order, dry_run)
    if result is None or market_status != "finalized":
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
    paper_monitor_only: bool = False,
) -> bool:
    """Post the complete fixed GTC ladder for one frozen ML side.

    The ML decision—not a transient quote—chooses the economic outcome.  The
    four limits are deliberately created only after the market is tradeable,
    with its explicit close timestamp as their expiry.  This makes the order
    set one side / four fixed rungs / one market, and prevents a later quote
    from choosing or reversing the side.
    """
    ticker = str(field(market, "ticker") or "")
    if not ticker or not market_is_tradeable(market):
        return False
    recovery_status = str(field(state.get("exchange_recovery", {}), "status") or "")
    if not dry_run and recovery_status and recovery_status != "ready":
        LOG.warning(
            "ENTRY BLOCKED BY RECOVERY | %s exchange recovery status=%s; no new ladder will be sent.",
            ticker, recovery_status or "unknown",
        )
        return False
    record = state.get("markets", {}).get(ticker)
    if not isinstance(record, dict):
        record = start_market_watcher(state, market, config)
    if not isinstance(record, dict) or record.get("status") != "watching":
        return False
    if not paper_monitor_only and rest_pause_active(rest):
        record["pause_blocked_at"] = now_iso()
        record["pause_blocked_reason"] = "scheduled or detected Kalshi trading pause"
        LOG.info("ENTRY DEFERRED FOR PAUSE | %s no new GTC ladder during a Kalshi trading/exchange pause.", ticker)
        return False
    if ml_side not in {"yes", "no"}:
        return False
    missing_or_rejected_rungs = any(
        not isinstance(record.get("orders", {}).get(f"{level:.4f}"), dict)
        or record["orders"][f"{level:.4f}"].get("status") in {"submit_failed", "paused"}
        for level in LADDER_LEVELS
    )
    if missing_or_rejected_rungs and not market_can_start_watcher(market, config["watch_start_grace_seconds"]):
        # Never let a scheduled/unscheduled pause (or a partial network/API
        # failure) turn into a late mid-market ladder. Missing rungs may retry
        # only in the original opening grace; the next market starts clean.
        record["status"] = "prepost_window_missed"
        record["prepost_window_missed_at"] = now_iso()
        LOG.warning(
            "GTC LADDER SKIPPED | %s opening grace expired; refusing to pre-post a new ladder mid-market.", ticker,
        )
        return False
    # The counterfactual begins from the exact frozen ML decision and same
    # fresh-market policy even if the live ladder is later blocked by balance,
    # capacity, or exchange recovery. It remains paper-only in all modes.
    ensure_ml_weighted_trailing_scalp_shadows(record, market, config, ml_side)
    ensure_ml_weighted_fixed_stop_loss_shadows(record, market, config, ml_side)
    ensure_ml_weighted_activation_comparison_shadows(record, market, config, ml_side)
    if paper_monitor_only:
        # The isolated report Action keeps the exact frozen ML side and uses
        # the authenticated quote stream only for its normal/inverse weighted
        # paper studies.  It has no primary ladder, reservation, balance
        # check, exchange-position check, or order endpoint call.
        record.update({
            "candidate_side": ml_side,
            "locked_side": ml_side,
            "locked_at": now_iso(),
            "quantity": 0.0,
            "status": "paper_monitor_active",
            "paper_monitor_only": True,
            "ladder_mode": "weighted_exit_protection_quote_monitor_only",
            "reserved_principal": 0.0,
            "market_close_time": field(market, "close_time", "expected_expiration_time"),
        })
        LOG.info(
            "WEIGHTED ML MONITOR STARTED | %s frozen_side=%s; subscribing to each actual YES/NO book for normal/inverse "
            "1x$0.40/2x$0.30/3x$0.20/4x$0.10 fills, a selected-side $0.05 stop, and separate 10c trails armed at +1c through +9c, then +10c/+20c/…/+80c. No order exists.",
            ticker, ml_side.upper(),
        )
        return True
    ensure_inverse_shadow(record, market, config, ml_side)
    ensure_ml_scalp_shadow(record, market, config, ml_side)
    ensure_model_transition_shadow(record, market, config)
    other_active = [candidate for candidate in active_strategy_records(state) if candidate is not record]
    if len(other_active) >= config["max_active_markets"]:
        return False
    # Quotes remain useful for heartbeat/audit output, but they do not gate
    # this mode.  Once the frozen ML side is available, every fixed rung is a
    # market-close-expiring GTC limit on that side.
    _ = live_asks
    side = ml_side
    quantity = config["initial_position_size"]
    reserve = ladder_principal(quantity)
    # A retried, partially submitted ladder is already included in the active
    # reserve. Replace that record's reserve rather than adding it twice.
    existing_quantity = as_float(record.get("quantity")) if record.get("candidate_side") else None
    existing_reserve = ladder_principal(existing_quantity) if existing_quantity is not None else 0.0
    total_after_reserve = reserved_principal(state) - existing_reserve + reserve
    if total_after_reserve > config["max_total_capital"] + 1e-9:
        LOG.warning(
            "SKIP CAPITAL | %s reserve=$%.2f total_after=$%.2f cap=$%.2f",
            ticker, reserve, total_after_reserve, config["max_total_capital"],
        )
        return False
    balance = await rest.balance_dollars()
    if balance is None or balance + 1e-9 < reserve + config["fee_reserve"]:
        LOG.warning("SKIP BALANCE | %s need >= $%.2f including fee reserve; available=%s", ticker, reserve + config["fee_reserve"], balance)
        return False
    record.update({
        "candidate_side": side,
        "locked_side": side,
        "locked_at": now_iso(),
        "quantity": quantity,
        "status": "ladder_active",
        "ladder_mode": "preposted_gtc_v2",
        "reserved_principal": reserve,
        "market_close_time": field(market, "close_time", "expected_expiration_time"),
    })
    if not await exchange_position_guard(rest, record, config):
        record["status"] = "initial_blocked_exchange_position"
        LOG.critical("GTC LADDER BLOCKED | %s no order submitted because the live exchange position is unsafe.", ticker)
        return False
    expiry = expiration_epoch(market)
    if expiry is None:
        record["status"] = "initial_blocked_no_expiry"
        LOG.critical("GTC LADDER BLOCKED | %s has no market-close expiry; no GTC orders submitted.", ticker)
        return False
    LOG.info(
        "SIDE LOCKED | %s %s from frozen ML decision; immediately posting GTC ladder $0.40/$0.30/$0.20/$0.10 through close_epoch=%d.",
        ticker, side.upper(), expiry,
    )
    for level in LADDER_LEVELS:
        key = f"{level:.4f}"
        prior_order = record["orders"].get(key)
        if isinstance(prior_order, dict):
            # A transport/API rejection has no accepted exchange order. Remove
            # only that local failure so the frozen, same-side watcher can
            # retry this exact deterministic client ID on the next loop.
            if prior_order.get("status") == "submit_failed" and not prior_order.get("order_id"):
                record["orders"].pop(key, None)
            else:
                continue
        submitted_contracts = sum(float(order.get("quantity") or 0.0) for order in orders_for_market(record))
        if submitted_contracts + quantity > config["max_contracts_per_market"] + 1e-9:
            record["ladder_prepost_error"] = "contract cap"
            LOG.critical(
                "GTC LADDER BLOCKED | %s cannot post $%.2f: requested=%.2f cap=%.2f.",
                ticker, level, submitted_contracts + quantity, config["max_contracts_per_market"],
            )
            break
        LOG.info("GTC LADDER LIMIT | %s %s @ $%.2f x %.2f expires_at=%d", ticker, side.upper(), level, quantity, expiry)
        order = await rest.create_order(
            ticker=ticker, side=side, position_price=level, quantity=quantity,
            tif="good_till_canceled", expiration_time=expiry, dry_run=dry_run,
            order_key="initial" if level == LADDER_LEVELS[0] else key,
        )
        order["ladder_level"] = level
        order["reason"] = "Frozen ML side; pre-posted fixed GTC ladder through market close."
        record["orders"][key] = order
        if order.get("status") == "paused":
            # Do not generate four rejected submissions or retry in a late
            # market. The watcher remains safe and the next fresh market will
            # receive a new frozen ML decision after trading resumes.
            break
        if order.get("direction_verified") is False:
            record["status"] = "direction_mismatch"
            LOG.critical(
                "GTC LADDER STOPPED | %s $%.2f direction mismatch; no further rungs submitted.", ticker, level,
            )
            break
    record["ladder_preposted_at"] = now_iso()
    accepted_rungs = sum(
        isinstance(record["orders"].get(f"{level:.4f}"), dict)
        and record["orders"][f"{level:.4f}"].get("status") not in {"submit_failed", "paused"}
        for level in LADDER_LEVELS
    )
    record["ladder_preposted_rungs"] = accepted_rungs
    record["ladder_preposted_complete"] = accepted_rungs == len(LADDER_LEVELS)
    if not record["ladder_preposted_complete"] and record.get("status") != "direction_mismatch":
        # Preserve the frozen ML side and any accepted rungs. Missing/rejected
        # submissions can retry only while the opening grace still applies.
        record["status"] = "watching"
        LOG.warning(
            "GTC LADDER INCOMPLETE | %s %s accepted=%d/%d; will retry only missing same-side rungs.",
            ticker, side.upper(), accepted_rungs, len(LADDER_LEVELS),
        )
    ml_signal = record.get("ml_inference") if isinstance(record.get("ml_inference"), dict) else {}
    LOG.info(
        "GTC LADDER POSTED | %s %s p_yes=%s confidence=%s rungs=%d/%d; no opposite-side order exists.",
        ticker, side.upper(),
        "unknown" if ml_signal.get("probability_yes") is None else f"{float(ml_signal['probability_yes']):.4f}",
        "unknown" if ml_signal.get("confidence") is None else f"{float(ml_signal['confidence']):.4f}",
        accepted_rungs, len(LADDER_LEVELS),
    )
    return accepted_rungs > 0


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


def model_transition_side_comparison(state: dict[str, Any]) -> dict[str, Any]:
    """Aggregate predecessor/current decisions made on identical frozen inputs."""
    groups: dict[tuple[str, str], dict[str, Any]] = {}
    for record in state.get("markets", {}).values():
        if not isinstance(record, dict):
            continue
        comparison = record.get("ml_model_transition")
        if not isinstance(comparison, dict):
            continue
        previous_run = str(comparison.get("previous_model_run_id") or "")
        current_run = str(comparison.get("current_model_run_id") or "")
        previous_side = str(comparison.get("previous_side") or "").lower()
        current_side = str(comparison.get("current_side") or "").lower()
        if not previous_run or not current_run or previous_side not in {"yes", "no"} or current_side not in {"yes", "no"}:
            continue
        item = groups.setdefault((previous_run, current_run), {
            "previous_model_run_id": previous_run,
            "current_model_run_id": current_run,
            "compared_markets": 0,
            "same_side": 0,
            "side_changed": 0,
            "yes_to_no": 0,
            "no_to_yes": 0,
            "probability_yes_delta_sum": 0.0,
            "settled_markets": 0,
            "previous_directional_wins": 0,
            "current_directional_wins": 0,
        })
        item["compared_markets"] += 1
        changed = previous_side != current_side
        item["same_side"] += not changed
        item["side_changed"] += changed
        if previous_side == "yes" and current_side == "no":
            item["yes_to_no"] += 1
        elif previous_side == "no" and current_side == "yes":
            item["no_to_yes"] += 1
        item["probability_yes_delta_sum"] += float(comparison.get("probability_yes_delta") or 0.0)
        outcome = str(record.get("settlement_outcome") or "").lower()
        if outcome in {"yes", "no"}:
            item["settled_markets"] += 1
            item["previous_directional_wins"] += previous_side == outcome
            item["current_directional_wins"] += current_side == outcome
    comparisons = []
    for item in groups.values():
        count = item["compared_markets"]
        settled_count = item["settled_markets"]
        item["side_change_rate"] = round(item["side_changed"] / count, 6) if count else None
        item["average_probability_yes_delta"] = round(item.pop("probability_yes_delta_sum") / count, 6) if count else None
        item["previous_directional_win_rate"] = round(item["previous_directional_wins"] / settled_count, 6) if settled_count else None
        item["current_directional_win_rate"] = round(item["current_directional_wins"] / settled_count, 6) if settled_count else None
        item["current_minus_previous_directional_wins"] = (
            item["current_directional_wins"] - item["previous_directional_wins"]
        )
        comparisons.append(item)
    return {
        "method": "same_frozen_input_model_transition_comparison_v1",
        "comparisons": sorted(comparisons, key=lambda item: (item["current_model_run_id"], item["previous_model_run_id"])),
        "note": "This is prospective deployment-transition tracking. It compares model sides on the same frozen feature vector; it is not a retraining promotion test or executable P&L estimate.",
    }


def inverse_shadow_entries(state: dict[str, Any]) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    return [
        (record, record["inverse_ml_shadow"])
        for record in state.get("markets", {}).values()
        if isinstance(record, dict) and isinstance(record.get("inverse_ml_shadow"), dict)
    ]


def inverse_shadow_rung_performance(entries: list[tuple[dict[str, Any], dict[str, Any]]]) -> dict[str, dict[str, Any]]:
    """Report every inverse shadow rung, including unfilled paper limits."""
    stats = {
        f"{level:.2f}": {
            "rung_price": level, "paper_orders": 0, "paper_contracts": 0.0,
            "simulated_quote_hits": 0, "filled_contracts": 0.0,
            "resting_or_unfilled": 0, "unsettled_quote_hits": 0,
            "winning_orders": 0, "losing_orders": 0,
            "net_profit": 0.0,
        }
        for level in LADDER_LEVELS
    }
    for _record, shadow in entries:
        outcome = str(shadow.get("settlement_outcome") or "").lower()
        shadow_side = str(shadow.get("side") or "").lower()
        rungs = shadow.get("rungs") if isinstance(shadow.get("rungs"), dict) else {}
        for level in LADDER_LEVELS:
            rung = rungs.get(f"{level:.4f}")
            if not isinstance(rung, dict):
                continue
            item = stats[f"{level:.2f}"]
            quantity = float(rung.get("quantity") or 0.0)
            fill = float(rung.get("fill_count") or 0.0)
            item["paper_orders"] += 1
            item["paper_contracts"] += quantity
            if fill <= 0.004:
                item["resting_or_unfilled"] += 1
                continue
            price = float(rung.get("average_fill_price") or rung.get("rung_price") or level)
            item["simulated_quote_hits"] += 1
            item["filled_contracts"] += fill
            # A quote-qualified rung remains open until the binary market is
            # settled.  Do not treat its cost as a realized loss in a
            # per-rung report merely because its outcome is still unknown.
            if outcome not in {"yes", "no"} or shadow_side not in {"yes", "no"}:
                item["unsettled_quote_hits"] += 1
                continue
            pnl = (fill if shadow_side == outcome else 0.0) - fill * price
            item["net_profit"] += pnl
            if pnl > 1e-9:
                item["winning_orders"] += 1
            elif pnl < -1e-9:
                item["losing_orders"] += 1
    for item in stats.values():
        denominator = item["winning_orders"] + item["losing_orders"]
        item["paper_contracts"] = round(item["paper_contracts"], 2)
        item["filled_contracts"] = round(item["filled_contracts"], 2)
        item["net_profit"] = round(item["net_profit"], 6)
        item["win_rate"] = round(item["winning_orders"] / denominator, 6) if denominator else None
    return stats


def inverse_shadow_performance(state: dict[str, Any]) -> dict[str, Any]:
    """Detailed, separately labeled performance for the inverse ML shadow."""
    entries = inverse_shadow_entries(state)
    settled_signals = sorted(
        [(record, shadow) for record, shadow in entries
         if str(shadow.get("settlement_outcome") or "").lower() in {"yes", "no"}
         and str(shadow.get("side") or "").lower() in {"yes", "no"}],
        key=lambda pair: str(pair[1].get("settled_at") or ""),
    )
    settled_fills = [
        (record, shadow) for record, shadow in settled_signals
        if float(shadow.get("contracts") or 0.0) > 0.004
    ]
    pnls = [float(shadow.get("net_profit_loss") or 0.0) for _record, shadow in settled_fills]
    costs = [float(shadow.get("total_cost") or 0.0) for _record, shadow in settled_fills]
    contracts = [float(shadow.get("contracts") or 0.0) for _record, shadow in settled_fills]
    signal_wins = sum(str(shadow.get("side")) == str(shadow.get("settlement_outcome"))
                      for _record, shadow in settled_signals)
    executed_wins = sum(str(shadow.get("side")) == str(shadow.get("settlement_outcome"))
                        for _record, shadow in settled_fills)
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
    return {
        "strategy": "inverse_ml_executable_quote_shadow_v1",
        "mode": "paper_only_no_exchange_orders",
        "fill_rule": "fresh top-of-book only: YES buy yes_ask <= rung; NO buy 1 - yes_bid <= rung; displayed depth >= rung quantity",
        "quote_max_age_seconds": next((shadow.get("quote_max_age_seconds") for _record, shadow in entries), None),
        "fee_treatment": "excluded_no_exchange_fill",
        "limitations": [
            "A quote hit is a paper fill, not a Kalshi exchange fill.",
            "Displayed top-of-book depth is required, but queue priority, cancellations, hidden liquidity, and fees are not modeled.",
            "P&L uses each pre-posted rung limit, not favorable quote-price improvement.",
        ],
        "shadow_signals_started": len(entries),
        "active_shadow_markets": sum(str(shadow.get("status")) == "active" for _record, shadow in entries),
        "settled_signal_markets": len(settled_signals),
        "unfilled_shadow_markets": sum(str(shadow.get("status")) == "finalized_unfilled" for _record, shadow in entries),
        "filled_market_trades": len(settled_fills),
        "signal_directional_wins": signal_wins,
        "signal_directional_losses": len(settled_signals) - signal_wins,
        "signal_directional_win_rate": round(signal_wins / len(settled_signals), 6) if settled_signals else None,
        "filled_directional_wins": executed_wins,
        "filled_directional_losses": len(settled_fills) - executed_wins,
        "filled_directional_win_rate": round(executed_wins / len(settled_fills), 6) if settled_fills else None,
        "total_simulated_contracts": round(sum(contracts), 2),
        "total_simulated_cost": round(sum(costs), 6),
        "total_simulated_fees": 0.0,
        "net_profit": round(sum(pnls), 6),
        "return_on_simulated_capital": round(sum(pnls) / sum(costs), 6) if sum(costs) else None,
        "average_profit_per_filled_market": round(mean, 6) if settled_fills else None,
        "average_contracts_per_filled_market": round(sum(contracts) / len(settled_fills), 6) if settled_fills else None,
        "average_entry_price": round(sum(costs) / sum(contracts), 6) if sum(contracts) else None,
        "winning_trades": wins,
        "losing_trades": losses,
        "win_rate": round(wins / len(settled_fills), 6) if settled_fills else None,
        "profit_factor": round(gross_win / gross_loss, 6) if gross_loss else None,
        "maximum_drawdown": round(max(drawdowns, default=0.0), 6),
        "longest_winning_streak": streak(pnls, True),
        "longest_losing_streak": streak(pnls, False),
        "largest_winning_trade": round(max(pnls, default=0.0), 6),
        "largest_losing_trade": round(min(pnls, default=0.0), 6),
        "rung_performance": inverse_shadow_rung_performance(entries),
    }


def ml_scalp_shadow_performance(state: dict[str, Any]) -> dict[str, Any]:
    """Summarize the normal-side paper scalp alternative separately from live P&L."""
    shadows = [
        record["ml_ladder_scalp_shadow"]
        for record in state.get("markets", {}).values()
        if isinstance(record, dict) and isinstance(record.get("ml_ladder_scalp_shadow"), dict)
    ]
    report = scalp_performance(shadows)
    report.update({
        "strategy": "ml_ladder_average_entry_scalp_executable_quote_shadow_v1",
        "source": "same_frozen_ml_side_as_primary_ladder",
    })
    return report


def ml_weighted_trailing_scalp_performance(state: dict[str, Any], *, inverse: bool) -> dict[str, Any]:
    """Summarize the frozen normal or inverse ML 1/2/3/4 trailing study."""
    record_key = "inverse_ml_weighted_trailing_scalp_shadow" if inverse else "ml_weighted_trailing_scalp_shadow"
    shadows = [
        record[record_key]
        for record in state.get("markets", {}).values()
        if isinstance(record, dict) and isinstance(record.get(record_key), dict)
    ]
    report = scalp_performance(shadows)
    report.update({
        "strategy": "inverse_ml_weighted_1234_trailing_scalp_shadow_v1" if inverse
        else "normal_ml_weighted_1234_trailing_scalp_shadow_v1",
        "source": "opposite_frozen_ml_side" if inverse else "same_frozen_ml_side_as_primary_ladder",
        "locked_side_policy": "side is fixed before open and never changes on later quotes",
        "weighted_rungs": {"0.40": 1.0, "0.30": 2.0, "0.20": 3.0, "0.10": 4.0},
    })
    return report


def ml_weighted_trailing_shadows(state: dict[str, Any], *, inverse: bool) -> list[dict[str, Any]]:
    """Return one chronological, model-specific paper ledger from durable state."""
    record_key = "inverse_ml_weighted_trailing_scalp_shadow" if inverse else "ml_weighted_trailing_scalp_shadow"
    return sorted([
        record[record_key]
        for record in state.get("markets", {}).values()
        if isinstance(record, dict) and isinstance(record.get(record_key), dict)
    ], key=lambda shadow: (
        str(shadow.get("settled_at") or shadow.get("created_at") or ""),
        str(shadow.get("ticker") or ""),
    ))


def ml_weighted_trailing_ledger(state: dict[str, Any], config: dict[str, Any], *, inverse: bool) -> dict[str, Any]:
    """Emit the complete auditable normal or inverse ML paper ledger.

    Each record preserves the frozen source side, locked study side, every
    entry rung/quote/depth event, all VWAP position epochs, trailing-stop
    evidence, and the final cash-flow result.  The paired report remains a
    compact view; this is the detailed source-of-truth file for review.
    """
    report = ml_weighted_trailing_scalp_performance(state, inverse=inverse)
    return {
        "schema": "ml_weighted_1234_trailing_paper_ledger_v1",
        "generated_at": now_iso(),
        "paper_only": True,
        "model_variant": "inverse_ml" if inverse else "normal_ml",
        "strategy": report["strategy"],
        "locked_side_policy": report["locked_side_policy"],
        "strategy_definition": {
            "rung_quantities": {f"{level:.2f}": quantity for level, quantity in WEIGHTED_SCALP_RUNG_QUANTITIES.items()},
            "target_opportunities_per_contract": [round(target, 2) for target in EXTENDED_PROFIT_TARGETS],
            "trailing_stop_per_contract": float(config["weighted_scalp_trailing_stop_per_contract"]),
            "entry_rule": "fresh executable side ask at or below each rung with displayed ask depth; shared quote depth is consumed across the 1/2/3/4 fills",
            "exit_rule": "after a fresh full-depth executable bid establishes a high, a later fresh full-depth bid at or below high minus the trailing gap closes at that observed bid",
            "fee_treatment": "excluded_no_exchange_fill",
        },
        "summary": report,
        "records": ml_weighted_trailing_shadows(state, inverse=inverse),
    }


def save_ml_weighted_trailing_outputs(
    state: dict[str, Any], config: dict[str, Any], *,
    normal_ledger_path: Path, normal_report_path: Path,
    inverse_ledger_path: Path, inverse_report_path: Path,
) -> None:
    """Persist viewable, model-specific weighted trailing ledgers and reports."""
    normal_ledger = ml_weighted_trailing_ledger(state, config, inverse=False)
    inverse_ledger = ml_weighted_trailing_ledger(state, config, inverse=True)
    save_json(normal_ledger_path, normal_ledger)
    save_json(normal_report_path, normal_ledger["summary"])
    save_json(inverse_ledger_path, inverse_ledger)
    save_json(inverse_report_path, inverse_ledger["summary"])


def ml_weighted_fixed_stop_loss_performance(state: dict[str, Any], *, inverse: bool) -> dict[str, Any]:
    """Summarize the normal or inverse ML 5c/10c actual-price bracket."""
    record_key = "inverse_ml_weighted_fixed_stop_loss_shadow" if inverse else "ml_weighted_fixed_stop_loss_shadow"
    shadows = [
        record[record_key]
        for record in state.get("markets", {}).values()
        if isinstance(record, dict) and isinstance(record.get(record_key), dict)
    ]
    report = scalp_performance(shadows)
    report.update({
        "strategy": "inverse_ml_weighted_1234_fixed_stop_and_trailing_shadow_v2" if inverse
        else "normal_ml_weighted_1234_fixed_stop_and_trailing_shadow_v2",
        "source": "opposite_frozen_ml_side" if inverse else "same_frozen_ml_side_as_primary_ladder",
        "locked_side_policy": "side is fixed before open and never changes on later quotes",
        "weighted_rungs": {"0.40": 1.0, "0.30": 2.0, "0.20": 3.0, "0.10": 4.0},
    })
    return report


def ml_weighted_fixed_stop_loss_shadows(state: dict[str, Any], *, inverse: bool) -> list[dict[str, Any]]:
    """Return the durable detailed ledger for one ML fixed-stop variant."""
    record_key = "inverse_ml_weighted_fixed_stop_loss_shadow" if inverse else "ml_weighted_fixed_stop_loss_shadow"
    return sorted([
        record[record_key]
        for record in state.get("markets", {}).values()
        if isinstance(record, dict) and isinstance(record.get(record_key), dict)
    ], key=lambda shadow: (
        str(shadow.get("settled_at") or shadow.get("created_at") or ""),
        str(shadow.get("ticker") or ""),
    ))


def ml_weighted_fixed_stop_loss_ledger(state: dict[str, Any], config: dict[str, Any], *, inverse: bool) -> dict[str, Any]:
    """Emit a complete legacy one-gate absolute-5c / 10c trailing ledger."""
    report = ml_weighted_fixed_stop_loss_performance(state, inverse=inverse)
    return {
        "schema": "ml_weighted_1234_fixed_stop_and_trailing_paper_ledger_v2",
        "generated_at": now_iso(),
        "paper_only": True,
        "model_variant": "inverse_ml" if inverse else "normal_ml",
        "strategy": report["strategy"],
        "locked_side_policy": report["locked_side_policy"],
        "strategy_definition": {
            "rung_quantities": {f"{level:.2f}": quantity for level, quantity in WEIGHTED_SCALP_RUNG_QUANTITIES.items()},
            "absolute_stop_price": float(config["weighted_scalp_absolute_stop_price"]),
            "trailing_stop_per_contract": float(config["weighted_scalp_trailing_stop_per_contract"]),
            "trailing_activation_gain_per_contract": float(config["weighted_scalp_trailing_activation_gain_per_contract"]),
            "entry_rule": "fresh executable side ask at or below each rung with displayed ask depth; shared quote depth is consumed across the 1/2/3/4 fills",
            "exit_rule": "record both actual YES/NO top-of-book prices; close all paper contracts at the observed full-depth selected-side bid if it is at/below the absolute 5c stop, or after a full-depth +10c gain arms a later 10c trailing retracement",
            "fee_treatment": "excluded_no_exchange_fill",
        },
        "summary": report,
        "records": ml_weighted_fixed_stop_loss_shadows(state, inverse=inverse),
    }


def save_ml_weighted_fixed_stop_outputs(
    state: dict[str, Any], config: dict[str, Any], *,
    normal_ledger_path: Path, normal_report_path: Path,
    inverse_ledger_path: Path, inverse_report_path: Path,
) -> None:
    """Persist either the legacy bracket or the live hold-gate comparison."""
    if config.get("weighted_scalp_activation_comparison_enabled", True):
        normal_ledger = ml_weighted_activation_comparison_ledger(state, config, inverse=False)
        inverse_ledger = ml_weighted_activation_comparison_ledger(state, config, inverse=True)
    else:
        normal_ledger = ml_weighted_fixed_stop_loss_ledger(state, config, inverse=False)
        inverse_ledger = ml_weighted_fixed_stop_loss_ledger(state, config, inverse=True)
    save_json(normal_ledger_path, normal_ledger)
    save_json(normal_report_path, normal_ledger["summary"])
    save_json(inverse_ledger_path, inverse_ledger)
    save_json(inverse_report_path, inverse_ledger["summary"])


def ml_weighted_activation_comparison_records(
    state: dict[str, Any], *, inverse: bool, gain: float,
) -> list[dict[str, Any]]:
    key = "inverse_ml_weighted_activation_comparison" if inverse else "ml_weighted_activation_comparison"
    gain_key = _activation_gain_key(gain)
    return sorted([
        record[key][gain_key]
        for record in state.get("markets", {}).values()
        if isinstance(record, dict) and isinstance(record.get(key), dict)
        and isinstance(record[key].get(gain_key), dict)
    ], key=lambda shadow: (str(shadow.get("settled_at") or shadow.get("created_at") or ""), str(shadow.get("ticker") or "")))


def ml_weighted_activation_comparison_performance(
    state: dict[str, Any], config: dict[str, Any], *, inverse: bool,
) -> dict[str, Any]:
    """Compare each gate using identical frozen side, rungs, and quote rules."""
    variants: dict[str, dict[str, Any]] = {}
    for gain in config["weighted_scalp_trailing_activation_gains_per_contract"]:
        records = ml_weighted_activation_comparison_records(state, inverse=inverse, gain=float(gain))
        variant = scalp_performance(records)
        variant["activation_gain_per_contract"] = round(float(gain), 6)
        variant["armed_markets"] = sum(bool(shadow.get("market_trailing_armed_at")) for shadow in records)
        variants[_activation_gain_key(gain)] = variant
    return {
        "generated_at": now_iso(),
        "strategy": "inverse_ml_weighted_1234_hold_gate_trailing_comparison_v3" if inverse
        else "normal_ml_weighted_1234_hold_gate_trailing_comparison_v3",
        "source": "opposite_frozen_ml_side" if inverse else "same_frozen_ml_side_as_primary_ladder",
        "locked_side_policy": "side is fixed before open and never changes on later quotes",
        "weighted_rungs": {"0.40": 1.0, "0.30": 2.0, "0.20": 3.0, "0.10": 4.0},
        "absolute_stop_price": float(config["weighted_scalp_absolute_stop_price"]),
        "trailing_stop_per_contract": float(config["weighted_scalp_trailing_stop_per_contract"]),
        "market_wide_armed_trail": True,
        "variants": variants,
    }


def ml_weighted_activation_comparison_ledger(
    state: dict[str, Any], config: dict[str, Any], *, inverse: bool,
) -> dict[str, Any]:
    report = ml_weighted_activation_comparison_performance(state, config, inverse=inverse)
    return {
        "schema": "ml_weighted_1234_hold_gate_trailing_comparison_paper_ledger_v3",
        "generated_at": now_iso(),
        "paper_only": True,
        "model_variant": "inverse_ml" if inverse else "normal_ml",
        "strategy": report["strategy"],
        "locked_side_policy": report["locked_side_policy"],
        "strategy_definition": {
            "rung_quantities": {f"{level:.2f}": quantity for level, quantity in WEIGHTED_SCALP_RUNG_QUANTITIES.items()},
            "absolute_stop_price": float(config["weighted_scalp_absolute_stop_price"]),
            "trailing_stop_per_contract": float(config["weighted_scalp_trailing_stop_per_contract"]),
            "activation_gain_variants_per_contract": list(config["weighted_scalp_trailing_activation_gains_per_contract"]),
            "entry_rule": "fresh executable side ask at or below each rung with displayed ask depth; shared quote depth is consumed across 1/2/3/4 paper fills",
            "exit_rule": "every variant exits at a fresh full-depth selected-side bid <= $0.05; separately, it arms only after its own average-filled-entry gain gate, then remains armed through settlement and exits on a later 10c retracement from its market-wide high",
            "fee_treatment": "excluded_no_exchange_fill",
        },
        "summary": report,
        "records_by_activation_gain": {
            _activation_gain_key(gain): ml_weighted_activation_comparison_records(state, inverse=inverse, gain=float(gain))
            for gain in config["weighted_scalp_trailing_activation_gains_per_contract"]
        },
    }


def paper_shadow_summary(shadows: list[dict[str, Any]]) -> dict[str, Any]:
    """Return comparable settled paper metrics for any one-model shadow set."""
    settled_signals = sorted(
        [shadow for shadow in shadows
         if str(shadow.get("settlement_outcome") or "").lower() in {"yes", "no"}
         and str(shadow.get("side") or "").lower() in {"yes", "no"}],
        key=lambda shadow: str(shadow.get("settled_at") or ""),
    )
    settled_fills = [shadow for shadow in settled_signals if float(shadow.get("contracts") or 0.0) > 0.004]
    pnls = [float(shadow.get("net_profit_loss") or 0.0) for shadow in settled_fills]
    costs = [float(shadow.get("total_cost") or 0.0) for shadow in settled_fills]
    contracts = [float(shadow.get("contracts") or 0.0) for shadow in settled_fills]
    directional_wins = sum(str(shadow.get("side")) == str(shadow.get("settlement_outcome")) for shadow in settled_signals)
    filled_directional_wins = sum(str(shadow.get("side")) == str(shadow.get("settlement_outcome")) for shadow in settled_fills)
    gross_win = sum(value for value in pnls if value > 0)
    gross_loss = -sum(value for value in pnls if value < 0)
    equity = peak = 0.0
    drawdowns: list[float] = []
    for value in pnls:
        equity += value
        peak = max(peak, equity)
        drawdowns.append(peak - equity)
    return {
        "paper_markets_started": len(shadows),
        "active_paper_markets": sum(str(shadow.get("status")) == "active" for shadow in shadows),
        "settled_signal_markets": len(settled_signals),
        "unfilled_shadow_markets": sum(str(shadow.get("status")) == "finalized_unfilled" for shadow in shadows),
        "filled_market_trades": len(settled_fills),
        "directional_wins": directional_wins,
        "directional_losses": len(settled_signals) - directional_wins,
        "directional_win_rate": round(directional_wins / len(settled_signals), 6) if settled_signals else None,
        "filled_directional_wins": filled_directional_wins,
        "filled_directional_losses": len(settled_fills) - filled_directional_wins,
        "filled_directional_win_rate": round(filled_directional_wins / len(settled_fills), 6) if settled_fills else None,
        "total_simulated_contracts": round(sum(contracts), 2),
        "total_simulated_cost": round(sum(costs), 6),
        "net_profit": round(sum(pnls), 6),
        "return_on_simulated_capital": round(sum(pnls) / sum(costs), 6) if sum(costs) else None,
        "profit_factor": round(gross_win / gross_loss, 6) if gross_loss else None,
        "maximum_drawdown": round(max(drawdowns, default=0.0), 6),
        "rung_performance": inverse_shadow_rung_performance([({}, shadow) for shadow in shadows]),
    }


def model_transition_shadow_performance(state: dict[str, Any]) -> dict[str, Any]:
    """Compare retained predecessor and new-model quote shadows by transition."""
    groups: dict[tuple[str, str], dict[str, Any]] = {}
    for record in state.get("markets", {}).values():
        if not isinstance(record, dict):
            continue
        pair = record.get("ml_model_transition_shadow")
        if not isinstance(pair, dict):
            continue
        previous = pair.get("previous_model")
        current = pair.get("current_model")
        if not isinstance(previous, dict) or not isinstance(current, dict):
            continue
        previous_run = str(previous.get("model_run_id") or "")
        current_run = str(current.get("model_run_id") or "")
        if not previous_run or not current_run:
            continue
        group = groups.setdefault((previous_run, current_run), {
            "previous_model_run_id": previous_run,
            "current_model_run_id": current_run,
            "input_basis": pair.get("input_basis"),
            "paired_shadows_started": 0,
            "same_side": 0,
            "side_changed": 0,
            "previous_shadows": [],
            "current_shadows": [],
        })
        group["paired_shadows_started"] += 1
        group["same_side"] += not bool(pair.get("side_changed"))
        group["side_changed"] += bool(pair.get("side_changed"))
        group["previous_shadows"].append(previous)
        group["current_shadows"].append(current)
    comparisons = []
    for group in groups.values():
        previous_metrics = paper_shadow_summary(group.pop("previous_shadows"))
        current_metrics = paper_shadow_summary(group.pop("current_shadows"))
        group.update({
            "previous_model_paper": previous_metrics,
            "current_model_paper": current_metrics,
            "current_minus_previous_directional_wins": (
                current_metrics["directional_wins"] - previous_metrics["directional_wins"]
            ),
            "current_minus_previous_paper_pnl": round(
                current_metrics["net_profit"] - previous_metrics["net_profit"], 6
            ),
        })
        comparisons.append(group)
    return {
        "strategy": "paired_model_transition_executable_quote_shadow_v1",
        "mode": "paper_only_no_exchange_orders",
        "fill_rule": "fresh top-of-book only: YES buy yes_ask <= rung; NO buy 1 - yes_bid <= rung; displayed depth >= rung quantity",
        "fee_treatment": "excluded_no_exchange_fill",
        "comparisons": sorted(comparisons, key=lambda item: (item["current_model_run_id"], item["previous_model_run_id"])),
        "limitations": [
            "The predecessor and new model score the same frozen pre-open feature vector.",
            "Each model uses an independent hypothetical ladder; no exchange order is submitted or modified.",
            "Quote hits require fresh displayed depth but do not model queue priority, cancellations, hidden liquidity, or fees.",
        ],
        "note": "Prospective transition evidence only; it is not a promotion rule or executable-P&L claim.",
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
        "ml_model_transition_side_comparison": model_transition_side_comparison(state),
        "ml_model_transition_shadow_performance": model_transition_shadow_performance(state),
        "inverse_ml_shadow_performance": inverse_shadow_performance(state),
        "ml_ladder_scalp_shadow_performance": ml_scalp_shadow_performance(state),
        "ml_weighted_trailing_scalp_performance": ml_weighted_trailing_scalp_performance(state, inverse=False),
        "inverse_ml_weighted_trailing_scalp_performance": ml_weighted_trailing_scalp_performance(state, inverse=True),
        "ml_weighted_fixed_stop_loss_performance": ml_weighted_fixed_stop_loss_performance(state, inverse=False),
        "inverse_ml_weighted_fixed_stop_loss_performance": ml_weighted_fixed_stop_loss_performance(state, inverse=True),
        "ml_weighted_activation_comparison_performance": ml_weighted_activation_comparison_performance(state, config, inverse=False),
        "inverse_ml_weighted_activation_comparison_performance": ml_weighted_activation_comparison_performance(state, config, inverse=True),
        "note": "Only finalized records with filled contracts count as trades. The pre-open ML side is an execution filter; this report is realized live-ledger data, not a profitability proof.",
    }
    if settled:
        levels = {level: 0 for level in LADDER_LEVELS}
        starts_below = 0
        fills = []
        for record in settled:
            orders = record.get("orders") or {}
            forty = orders.get("0.4000") or {}
            forty_filled = float(forty.get("fill_count") or 0.0) > 0.004
            if record.get("ladder_mode") == "preposted_gtc_v2":
                # In the pre-posted-GTC strategy the stored limit is always
                # 40c. A market "starts below 40c" operationally means a
                # lower rung filled while the 40c rung never did.
                lower_filled = any(
                    float((orders.get(f"{level:.4f}") or {}).get("fill_count") or 0.0) > 0.004
                    for level in LADDER_LEVELS[1:]
                )
                if lower_filled and not forty_filled:
                    starts_below += 1
            elif float(forty.get("position_price") or 0.40) < 0.40:
                # Preserve the historical fill-then-ladder measurement for
                # records created before the pre-posted-GTC strategy.
                starts_below += 1
            fills.append(sum(float(order.get("fill_count") or 0.0) > 0.004 for order in orders_for_market(record)))
            for level in LADDER_LEVELS:
                order = orders.get(f"{level:.4f}") or {}
                if float(order.get("fill_count") or 0.0) > 0.004:
                    levels[level] += 1
        report.update({
            "percentage_entering_at_40c": round(100 * levels[0.40] / len(settled), 4),
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
    transition_summary = report["ml_model_transition_side_comparison"]
    for transition in transition_summary["comparisons"]:
        LOG.info(
            "ML MODEL TRANSITION PERFORMANCE | %s previous_run=%s current_run=%s compared=%d same_side=%d "
            "changed=%d (Y→N=%d N→Y=%d) change_rate=%s avg_p_yes_delta=%s settled=%d "
            "previous_wins=%d current_wins=%d current_minus_previous=%+d.",
            context, transition["previous_model_run_id"], transition["current_model_run_id"],
            transition["compared_markets"], transition["same_side"], transition["side_changed"],
            transition["yes_to_no"], transition["no_to_yes"],
            "n/a" if transition["side_change_rate"] is None else f"{100 * transition['side_change_rate']:.2f}%",
            "n/a" if transition["average_probability_yes_delta"] is None else f"{transition['average_probability_yes_delta']:+.4f}",
            transition["settled_markets"], transition["previous_directional_wins"],
            transition["current_directional_wins"], transition["current_minus_previous_directional_wins"],
        )
    transition_shadow_summary = report["ml_model_transition_shadow_performance"]
    for transition in transition_shadow_summary["comparisons"]:
        previous = transition["previous_model_paper"]
        current = transition["current_model_paper"]
        LOG.info(
            "ML TRANSITION SHADOW PERFORMANCE | %s previous_run=%s current_run=%s paired=%d same_side=%d changed=%d "
            "settled=%d previous=W%d/L%d net=$%+.4f current=W%d/L%d net=$%+.4f current_minus_previous=$%+.4f "
            "| paper only; fresh quote/depth required; fees/queue excluded.",
            context, transition["previous_model_run_id"], transition["current_model_run_id"],
            transition["paired_shadows_started"], transition["same_side"], transition["side_changed"],
            current["settled_signal_markets"], previous["directional_wins"], previous["directional_losses"], previous["net_profit"],
            current["directional_wins"], current["directional_losses"], current["net_profit"],
            transition["current_minus_previous_paper_pnl"],
        )
    shadow = report["inverse_ml_shadow_performance"]
    LOG.info(
        "INVERSE ML SHADOW PERFORMANCE | %s started=%d active=%d settled_signals=%d signal_wins=%d signal_losses=%d "
        "signal_win_rate=%s filled_markets=%d net=$%.4f roi=%s max_drawdown=$%.4f | paper only; fees/queue excluded.",
        context, shadow["shadow_signals_started"], shadow["active_shadow_markets"], shadow["settled_signal_markets"],
        shadow["signal_directional_wins"], shadow["signal_directional_losses"],
        "n/a" if shadow["signal_directional_win_rate"] is None else f"{100 * shadow['signal_directional_win_rate']:.2f}%",
        shadow["filled_market_trades"], shadow["net_profit"],
        "n/a" if shadow["return_on_simulated_capital"] is None else f"{100 * shadow['return_on_simulated_capital']:.2f}%",
        shadow["maximum_drawdown"],
    )
    for level, rung in shadow["rung_performance"].items():
        LOG.info(
            "INVERSE SHADOW RUNG PERFORMANCE | %sc paper_orders=%d quote_hits=%d contracts=%.2f "
            "resting_or_unfilled=%d winners=%d losers=%d net=$%.4f",
            level, rung["paper_orders"], rung["simulated_quote_hits"], rung["filled_contracts"],
            rung["resting_or_unfilled"], rung["winning_orders"], rung["losing_orders"], rung["net_profit"],
        )
    scalp = report["ml_ladder_scalp_shadow_performance"]
    excursion = scalp["excursion_observer"]
    maximum = excursion["maximum_gross_per_contract"]
    LOG.info(
        "ML LADDER SCALP RANGE PERFORMANCE | %s started=%d active=%d completed_states=%d depth_observed=%d "
        "mfe_gross_per_contract median=%s p75=%s p90=%s max=%s "
        "| paper only; no exit selected; fresh full-depth bid/ask evidence; fees/queue excluded.",
        context, scalp["paper_markets_started"], scalp["active_paper_markets"],
        excursion["completed_position_states"], excursion["depth_observed_position_states"],
        "n/a" if maximum["median"] is None else f"${maximum['median']:+.4f}",
        "n/a" if maximum["p75"] is None else f"${maximum['p75']:+.4f}",
        "n/a" if maximum["p90"] is None else f"${maximum['p90']:+.4f}",
        "n/a" if maximum["maximum"] is None else f"${maximum['maximum']:+.4f}",
    )
    for target, opportunity in excursion["target_opportunities"].items():
        rate = opportunity["hit_rate_given_depth_observation"]
        LOG.info(
            "ML SCALP RANGE TARGET | +%sc completed_states=%d depth_observed=%d hits=%d hit_rate=%s",
            target, opportunity["completed_position_states"], opportunity["depth_observed_position_states"],
            opportunity["hit_position_states"], "n/a" if rate is None else f"{100 * rate:.1f}%",
        )
    for average, profile in scalp["average_entry_profiles"].items():
        LOG.info(
            "ML SCALP AVG ENTRY RANGE | avg=%sc states=%d completed=%d depth_observed=%d "
            "mfe_median=%s mfe_p75=%s mfe_p90=%s",
            average, profile["observed_positions"], profile["completed_position_states"],
            profile["depth_observed_positions"],
            "n/a" if profile["median_maximum_gross_per_contract"] is None else f"${profile['median_maximum_gross_per_contract']:+.4f}",
            "n/a" if profile["p75_maximum_gross_per_contract"] is None else f"${profile['p75_maximum_gross_per_contract']:+.4f}",
            "n/a" if profile["p90_maximum_gross_per_contract"] is None else f"${profile['p90_maximum_gross_per_contract']:+.4f}",
        )
    for label, weighted in (
        ("ML NORMAL WEIGHTED TRAILING", report["ml_weighted_trailing_scalp_performance"]),
        ("ML INVERSE WEIGHTED TRAILING", report["inverse_ml_weighted_trailing_scalp_performance"]),
    ):
        weighted_excursion = weighted["excursion_observer"]
        weighted_maximum = weighted_excursion["maximum_gross_per_contract"]
        trailing = weighted["trailing_stop"]
        current_streak = (
            "none" if weighted["current_streak"] <= 0
            else f"{weighted['current_streak']} {weighted['current_streak_kind']}"
        )
        LOG.info(
            "%s PERFORMANCE | %s started=%d active=%d filled=%d trailing_exits=%d settlement_exits=%d "
            "W/L=%d/%d streak=%s longest_W/L=%d/%d net=$%+.4f roi=%s max_dd=$%.4f mfe_median=%s p75=%s p90=%s "
            "| locked side; 1x40c/2x30c/3x20c/4x10c; full-depth paper only; fees/queue excluded.",
            label, context, weighted["paper_markets_started"], weighted["active_paper_markets"],
            weighted["filled_market_trades"], trailing["trailing_stop_exits"],
            weighted["settlement_exits_without_take_profit"], weighted["winning_trades"], weighted["losing_trades"],
            current_streak,
            weighted["longest_winning_streak"], weighted["longest_losing_streak"],
            weighted["net_profit"], "n/a" if weighted["return_on_simulated_capital"] is None
            else f"{100 * weighted['return_on_simulated_capital']:.2f}%", weighted["maximum_drawdown"],
            "n/a" if weighted_maximum["median"] is None else f"${weighted_maximum['median']:+.4f}",
            "n/a" if weighted_maximum["p75"] is None else f"${weighted_maximum['p75']:+.4f}",
            "n/a" if weighted_maximum["p90"] is None else f"${weighted_maximum['p90']:+.4f}",
        )
        for target, opportunity in weighted_excursion["target_opportunities"].items():
            rate = opportunity["hit_rate_given_depth_observation"]
            LOG.info(
                "%s TARGET | +%sc completed_states=%d depth_observed=%d hits=%d hit_rate=%s",
                label, target, opportunity["completed_position_states"], opportunity["depth_observed_position_states"],
                opportunity["hit_position_states"], "n/a" if rate is None else f"{100 * rate:.1f}%",
            )
        for average, profile in weighted["average_entry_profiles"].items():
            LOG.info(
                "%s AVG COST | avg=$%s contracts=%s states=%d completed=%d depth_observed=%d "
                "mfe_median=%s mfe_p75=%s mfe_p90=%s",
                label, average,
                "n/a" if profile["median_filled_contracts"] is None else f"{profile['median_filled_contracts']:.2f}",
                profile["observed_positions"], profile["completed_position_states"], profile["depth_observed_positions"],
                "n/a" if profile["median_maximum_gross_per_contract"] is None else f"${profile['median_maximum_gross_per_contract']:+.4f}",
                "n/a" if profile["p75_maximum_gross_per_contract"] is None else f"${profile['p75_maximum_gross_per_contract']:+.4f}",
                "n/a" if profile["p90_maximum_gross_per_contract"] is None else f"${profile['p90_maximum_gross_per_contract']:+.4f}",
            )
    for label, fixed in (
        ("ML NORMAL WEIGHTED BRACKET", report["ml_weighted_fixed_stop_loss_performance"]),
        ("ML INVERSE WEIGHTED BRACKET", report["inverse_ml_weighted_fixed_stop_loss_performance"]),
    ):
        current_streak = (
            "none" if fixed["current_streak"] <= 0
            else f"{fixed['current_streak']} {fixed['current_streak_kind']}"
        )
        stop = fixed["fixed_stop_loss"]
        trailing = fixed["trailing_stop"]
        activation = trailing["configured_activation_gains_per_contract"]
        LOG.info(
            "%s ACTUAL-PRICE BRACKET | %s started=%d active=%d filled=%d loss_exits=%d trailing_exits=%d settlement_exits=%d "
            "W/L=%d/%d streak=%s longest_W/L=%d/%d net=$%+.4f roi=%s max_dd=$%.4f absolute_stop=$%.2f trailing_gap=$%.2f activation_gain=$%.2f "
            "| both YES/NO books recorded; selected-side fresh full-depth bid only; paper only.",
            label, context, fixed["paper_markets_started"], fixed["active_paper_markets"],
            fixed["filled_market_trades"], stop["fixed_stop_loss_exits"],
            trailing["trailing_stop_exits"],
            fixed["settlement_exits_without_take_profit"], fixed["winning_trades"], fixed["losing_trades"],
            current_streak, fixed["longest_winning_streak"], fixed["longest_losing_streak"], fixed["net_profit"],
            "n/a" if fixed["return_on_simulated_capital"] is None
            else f"{100 * fixed['return_on_simulated_capital']:.2f}%", fixed["maximum_drawdown"],
            stop.get("configured_absolute_prices", [0.0])[0] if stop.get("configured_absolute_prices") else 0.0,
            trailing["configured_gaps_per_contract"][0] if trailing["configured_gaps_per_contract"] else 0.0,
            activation[0] if activation else 0.0,
        )
    for label, comparison in (
        ("ML NORMAL HOLD-GATE", report["ml_weighted_activation_comparison_performance"]),
        ("ML INVERSE HOLD-GATE", report["inverse_ml_weighted_activation_comparison_performance"]),
    ):
        LOG.info(
            "%s COMPARISON | %s absolute_stop=$%.2f trailing_gap=$%.2f; each trail stays armed through settlement.",
            label, context, comparison["absolute_stop_price"], comparison["trailing_stop_per_contract"],
        )
        for gain, variant in comparison["variants"].items():
            streak = "none" if variant["current_streak"] <= 0 else f"{variant['current_streak']} {variant['current_streak_kind']}"
            LOG.info(
                "%s GATE +$%s | started=%d active=%d armed=%d filled=%d stops=loss:%d/trail:%d settlement:%d "
                "W/L=%d/%d streak=%s longest_W/L=%d/%d net=$%+.4f roi=%s max_dd=$%.4f",
                label, gain, variant["paper_markets_started"], variant["active_paper_markets"], variant["armed_markets"],
                variant["filled_market_trades"], variant["fixed_stop_loss"]["fixed_stop_loss_exits"],
                variant["trailing_stop"]["trailing_stop_exits"], variant["settlement_exits_without_take_profit"],
                variant["winning_trades"], variant["losing_trades"], streak,
                variant["longest_winning_streak"], variant["longest_losing_streak"], variant["net_profit"],
                "n/a" if variant["return_on_simulated_capital"] is None else f"{100 * variant['return_on_simulated_capital']:.2f}%",
                variant["maximum_drawdown"],
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
        "at a fresh active market, immediately post the selected side's close-expiring GTC rungs at $0.40/$0.30/$0.20/$0.10.",
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
        preposted = isinstance(record, dict) and record.get("ladder_mode") == "preposted_gtc_v2"
        trigger = (
            "GTC_LADDER_POSTED"
            if preposted else
            ("ML_READY_POSTING_GTC" if watch_state == "watching" and ml_side else
             ("awaiting_ml" if watch_state == "watching" else "none"))
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
        shadow = record.get("inverse_ml_shadow")
        if isinstance(shadow, dict):
            rungs = shadow.get("rungs") if isinstance(shadow.get("rungs"), dict) else {}
            filled_rungs = sum(float(rung.get("fill_count") or 0.0) > 0.004 for rung in rungs.values() if isinstance(rung, dict))
            quote_evidence = next(
                (rung.get("simulation_quote") for rung in rungs.values()
                 if isinstance(rung, dict) and isinstance(rung.get("simulation_quote"), dict)),
                None,
            )
            LOG.info(
                "INVERSE SHADOW STATUS | %s source_ml=%s side=%s status=%s quote_state=%s filled_rungs=%d/%d "
                "last_quote=%s age=%s; no exchange order.",
                record.get("ticker", "?"), str(shadow.get("source_ml_side") or "?").upper(),
                str(shadow.get("side") or "?").upper(), shadow.get("status", "?"),
                shadow.get("last_quote_state", "awaiting_quote"), filled_rungs, len(LADDER_LEVELS),
                "none" if quote_evidence is None else f"${float(quote_evidence.get('economic_price') or 0.0):.4f}",
                "none" if quote_evidence is None else f"{float(quote_evidence.get('quote_age_seconds') or 0.0):.3f}s",
            )
        scalp = record.get("ml_ladder_scalp_shadow")
        if isinstance(scalp, dict):
            position = scalp_entry_summary(scalp)
            epochs = scalp.get("position_epochs") if isinstance(scalp.get("position_epochs"), list) else []
            epoch = epochs[-1] if epochs and isinstance(epochs[-1], dict) else {}
            maximum = epoch.get("max_executable_gross_per_contract")
            target_hits = epoch.get("target_hits") if isinstance(epoch.get("target_hits"), dict) else {}
            LOG.info(
                "ML SCALP RANGE STATUS | %s side=%s status=%s filled=%.2f avg_entry=%s max_gross_per_contract=%s "
                "targets_hit=%s entry_quote_state=%s exit_quote_state=%s; no exchange order or close.",
                record.get("ticker", "?"), str(scalp.get("side") or "?").upper(), scalp.get("status", "?"),
                float(position["filled_contracts"] or 0.0),
                "none" if position["average_entry_price"] is None else f"${float(position['average_entry_price']):.4f}",
                "none" if maximum is None else f"${float(maximum):+.4f}",
                "/".join(sorted(target_hits)) if target_hits else "none",
                scalp.get("last_entry_quote_state", "awaiting_quote"),
                scalp.get("last_exit_quote_state", "awaiting_quote"),
            )
        for weighted_key, weighted_label in (
            ("ml_weighted_trailing_scalp_shadow", "ML NORMAL WEIGHTED"),
            ("inverse_ml_weighted_trailing_scalp_shadow", "ML INVERSE WEIGHTED"),
        ):
            weighted = record.get(weighted_key)
            if not isinstance(weighted, dict):
                continue
            position = scalp_entry_summary(weighted)
            epochs = weighted.get("position_epochs") if isinstance(weighted.get("position_epochs"), list) else []
            epoch = epochs[-1] if epochs and isinstance(epochs[-1], dict) else {}
            maximum = epoch.get("max_executable_gross_per_contract")
            stop_bid = epoch.get("trailing_stop_bid")
            LOG.info(
                "%s STATUS | %s locked_side=%s status=%s filled=%.2f avg_cost=%s highest_gross=%s trailing_stop_bid=%s "
                "entry_quote_state=%s exit_quote_state=%s; no exchange order or close.",
                weighted_label, record.get("ticker", "?"), str(weighted.get("side") or "?").upper(),
                weighted.get("status", "?"), float(position["filled_contracts"] or 0.0),
                "none" if position["average_entry_price"] is None else f"${float(position['average_entry_price']):.4f}",
                "none" if maximum is None else f"${float(maximum):+.4f}",
                "none" if stop_bid is None else f"${float(stop_bid):.4f}",
                weighted.get("last_entry_quote_state", "awaiting_quote"),
                weighted.get("last_exit_quote_state", "awaiting_quote"),
            )
        for fixed_key, fixed_label in (
            ("ml_weighted_fixed_stop_loss_shadow", "ML NORMAL WEIGHTED BRACKET"),
            ("inverse_ml_weighted_fixed_stop_loss_shadow", "ML INVERSE WEIGHTED BRACKET"),
        ):
            fixed = record.get(fixed_key)
            if not isinstance(fixed, dict):
                continue
            position = scalp_entry_summary(fixed)
            epochs = fixed.get("position_epochs") if isinstance(fixed.get("position_epochs"), list) else []
            epoch = epochs[-1] if epochs and isinstance(epochs[-1], dict) else {}
            loss_bid = epoch.get("fixed_stop_loss_bid")
            activation_bid = epoch.get("trailing_activation_bid")
            trailing_bid = epoch.get("trailing_stop_bid")
            book = fixed.get("last_actual_top_of_book") if isinstance(fixed.get("last_actual_top_of_book"), dict) else {}
            LOG.info(
                "%s ACTUAL-PRICE STATUS | %s locked_side=%s status=%s filled=%.2f avg_filled_entry=%s "
                "loss_bid=%s trail_arms_at=%s trailing_bid=%s yes_bid/ask=%s/%s no_bid/ask=%s/%s "
                "entry_quote_state=%s exit_quote_state=%s; no exchange order or close.",
                fixed_label, record.get("ticker", "?"), str(fixed.get("side") or "?").upper(),
                fixed.get("status", "?"), float(position["filled_contracts"] or 0.0),
                "none" if position["average_entry_price"] is None else f"${float(position['average_entry_price']):.4f}",
                "none" if loss_bid is None else f"${float(loss_bid):.4f}",
                "none" if activation_bid is None else f"${float(activation_bid):.4f}",
                "none" if trailing_bid is None else f"${float(trailing_bid):.4f}",
                "none" if book.get("yes_bid") is None else f"${float(book['yes_bid']):.4f}",
                "none" if book.get("yes_ask") is None else f"${float(book['yes_ask']):.4f}",
                "none" if book.get("no_bid") is None else f"${float(book['no_bid']):.4f}",
                "none" if book.get("no_ask") is None else f"${float(book['no_ask']):.4f}",
                fixed.get("last_entry_quote_state", "awaiting_quote"),
                fixed.get("last_exit_quote_state", "awaiting_quote"),
            )
        for inverse, label in ((False, "ML NORMAL HOLD-GATE"), (True, "ML INVERSE HOLD-GATE")):
            for gain, shadow in _activation_comparison_variants(record, inverse=inverse).items():
                if not isinstance(shadow, dict) or shadow.get("status") != "active":
                    continue
                position = scalp_entry_summary(shadow)
                epochs = shadow.get("position_epochs") if isinstance(shadow.get("position_epochs"), list) else []
                epoch = epochs[-1] if epochs and isinstance(epochs[-1], dict) else {}
                book = shadow.get("last_actual_top_of_book") if isinstance(shadow.get("last_actual_top_of_book"), dict) else {}
                LOG.info(
                    "%s STATUS | %s side=%s gate=+$%s filled=%.2f avg_filled_entry=%s stop=$%.2f armed=%s high=%s trailing_bid=%s "
                    "yes_bid/ask=%s/%s no_bid/ask=%s/%s; paper only.",
                    label, record.get("ticker", "?"), str(shadow.get("side") or "?").upper(), gain,
                    float(position["filled_contracts"] or 0.0),
                    "none" if position["average_entry_price"] is None else f"${float(position['average_entry_price']):.4f}",
                    float(shadow.get("absolute_stop_price") or 0.0),
                    "yes" if shadow.get("market_trailing_armed_at") else "no",
                    "none" if shadow.get("market_trailing_high_bid") is None else f"${float(shadow['market_trailing_high_bid']):.4f}",
                    "none" if epoch.get("trailing_stop_bid") is None else f"${float(epoch['trailing_stop_bid']):.4f}",
                    "none" if book.get("yes_bid") is None else f"${float(book['yes_bid']):.4f}",
                    "none" if book.get("yes_ask") is None else f"${float(book['yes_ask']):.4f}",
                    "none" if book.get("no_bid") is None else f"${float(book['no_bid']):.4f}",
                    "none" if book.get("no_ask") is None else f"${float(book['no_ask']):.4f}",
                )
        if record.get("status") == "watching":
            ml_signal = record.get("ml_inference") if isinstance(record.get("ml_inference"), dict) else {}
            ml_side = str(ml_signal.get("side") or "").upper()
            LOG.info(
                "WATCH | %s active; %s.", record.get("ticker", "?"),
                (f"ML selected {ml_side} p_yes={float(ml_signal.get('probability_yes')):.4f} "
                 f"confidence={float(ml_signal.get('confidence')):.4f}; full GTC ladder will post immediately" if ml_side
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
    if args.paper_monitor_only and not dry_run:
        raise SystemExit("--paper-monitor-only requires DRY_RUN=true and can never be used for live orders")
    if not dry_run and not live_allowed and not control_only:
        raise SystemExit("Refusing live orders: pass both --submit and --allow-live with DRY_RUN=false")
    state_path = args.state_file.expanduser()
    weighted_normal_ledger_path = args.weighted_trailing_normal_ledger.expanduser()
    weighted_normal_report_path = args.weighted_trailing_normal_report.expanduser()
    weighted_inverse_ledger_path = args.weighted_trailing_inverse_ledger.expanduser()
    weighted_inverse_report_path = args.weighted_trailing_inverse_report.expanduser()
    weighted_fixed_normal_ledger_path = args.weighted_fixed_stop_normal_ledger.expanduser()
    weighted_fixed_normal_report_path = args.weighted_fixed_stop_normal_report.expanduser()
    weighted_fixed_inverse_ledger_path = args.weighted_fixed_stop_inverse_ledger.expanduser()
    weighted_fixed_inverse_report_path = args.weighted_fixed_stop_inverse_report.expanduser()
    state = load_json(state_path, default_state())
    state["format_version"] = STATE_VERSION
    state.setdefault("markets", {})
    checkpoint = StateCheckpointPublisher.create(
        config_path, state_path, args.report.expanduser(),
        weighted_normal_ledger_path, weighted_normal_report_path,
        weighted_inverse_ledger_path, weighted_inverse_report_path,
        weighted_fixed_normal_ledger_path, weighted_fixed_normal_report_path,
        weighted_fixed_inverse_ledger_path, weighted_fixed_inverse_report_path,
        config, state,
    )
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
            save_ml_weighted_trailing_outputs(
                state, config,
                normal_ledger_path=weighted_normal_ledger_path,
                normal_report_path=weighted_normal_report_path,
                inverse_ledger_path=weighted_inverse_ledger_path,
                inverse_report_path=weighted_inverse_report_path,
            )
            save_ml_weighted_fixed_stop_outputs(
                state, config,
                normal_ledger_path=weighted_fixed_normal_ledger_path,
                normal_report_path=weighted_fixed_normal_report_path,
                inverse_ledger_path=weighted_fixed_inverse_ledger_path,
                inverse_report_path=weighted_fixed_inverse_report_path,
            )
            LOG.warning("CANCEL-ONLY COMPLETE | canceled_open_mechanical_orders=%d", canceled)
            return 0
        finally:
            await rest.close()
    if args.ml_training_csv is None or args.ml_model_path is None:
        await rest.close()
        raise SystemExit("ML-side execution requires --ml-training-csv and --ml-model-path; refusing a price-only fallback")
    model_metadata = load_json(args.ml_model_metadata.expanduser(), {}) if args.ml_model_metadata else {}
    validation_report = load_json(args.ml_validation_report.expanduser(), {}) if args.ml_validation_report else {}
    previous_model_metadata = load_json(args.previous_ml_model_metadata.expanduser(), {}) if args.previous_ml_model_metadata else {}
    try:
        preflight_ml_deployment(args.ml_model_path.expanduser(), model_metadata)
        if args.previous_ml_model_path is not None:
            preflight_ml_deployment(args.previous_ml_model_path.expanduser(), previous_model_metadata)
        ml_selector = MLDirectionSelector(
            args.ml_training_csv.expanduser(), args.ml_model_path.expanduser(), config["ml_preopen_lead_seconds"],
            config["ml_min_confidence"], model_metadata, args.ml_model_run_id, args.ml_training_run_id,
            args.previous_ml_model_path.expanduser() if args.previous_ml_model_path else None,
            previous_model_metadata, args.previous_ml_model_run_id,
        )
    except Exception:
        await rest.close()
        raise
    recovery_ready = await recover_exchange_state(rest, state, config, dry_run)
    checkpoint.publish_if_changed(state, "startup_exchange_recovery")
    feed = KalshiLiveFeed(rest.auth)
    feed_task = asyncio.create_task(feed.run(), name="kalshi-average-down-ws")
    started_at = asyncio.get_running_loop().time()
    deadline = started_at + args.run_seconds
    last_heartbeat_at = float("-inf")
    last_market_refresh_at = float("-inf")
    last_order_reconcile_at = float("-inf")
    last_exchange_recovery_at = float("-inf")
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
        "INVERSE ML SHADOW POLICY | paper_only=%s quantity_per_rung=%.2f; primary live quantity remains %.2f.",
        bool(config["inverse_shadow_enabled"]), config["inverse_shadow_position_size"], config["initial_position_size"],
    )
    LOG.info(
        "ML TRANSITION SHADOW POLICY | paper_only=%s quantity_per_rung=%.2f; retained predecessor and current model "
        "will be compared only on identical frozen inputs.",
        bool(config["model_transition_shadow_enabled"]), config["model_transition_shadow_position_size"],
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
            if not recovery_ready and monotonic_now - last_exchange_recovery_at >= EXCHANGE_RECOVERY_RETRY_SECONDS:
                recovery_ready = await recover_exchange_state(rest, state, config, dry_run)
                last_exchange_recovery_at = monotonic_now
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
                    if market_is_tradeable(market) and not record.get("paper_monitor_only"):
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
                    paper_monitor_only=args.paper_monitor_only,
                )
            # The counterfactual is intentionally driven only by the
            # authenticated ticker stream. It is evaluated separately from
            # live order reconciliation and cannot call any order endpoint.
            for record in state["markets"].values():
                if isinstance(record, dict):
                    simulate_inverse_shadow(record, feed, config)
                    simulate_ml_scalp_shadow(record, feed, config)
                    simulate_ml_weighted_trailing_scalp_shadow(
                        record, feed, record_key="ml_weighted_trailing_scalp_shadow", label="ML NORMAL")
                    simulate_ml_weighted_trailing_scalp_shadow(
                        record, feed, record_key="inverse_ml_weighted_trailing_scalp_shadow", label="ML INVERSE")
                    simulate_ml_weighted_trailing_scalp_shadow(
                        record, feed, record_key="ml_weighted_fixed_stop_loss_shadow", label="ML NORMAL BRACKET")
                    simulate_ml_weighted_trailing_scalp_shadow(
                        record, feed, record_key="inverse_ml_weighted_fixed_stop_loss_shadow", label="ML INVERSE BRACKET")
                    simulate_ml_weighted_activation_comparison(record, feed, inverse=False)
                    simulate_ml_weighted_activation_comparison(record, feed, inverse=True)
                    simulate_model_transition_shadow(record, feed, config)
            monotonic_now = asyncio.get_running_loop().time()
            if monotonic_now - last_heartbeat_at >= config["status_log_seconds"]:
                await log_heartbeat(rest, state, active_markets, config, dry_run, monotonic_now - started_at, feed)
                last_heartbeat_at = monotonic_now
            save_json(state_path, state)
            save_json(args.report.expanduser(), performance_report(state, config))
            save_ml_weighted_trailing_outputs(
                state, config,
                normal_ledger_path=weighted_normal_ledger_path,
                normal_report_path=weighted_normal_report_path,
                inverse_ledger_path=weighted_inverse_ledger_path,
                inverse_report_path=weighted_inverse_report_path,
            )
            save_ml_weighted_fixed_stop_outputs(
                state, config,
                normal_ledger_path=weighted_fixed_normal_ledger_path,
                normal_report_path=weighted_fixed_normal_report_path,
                inverse_ledger_path=weighted_fixed_inverse_ledger_path,
                inverse_report_path=weighted_fixed_inverse_report_path,
            )
            checkpoint.publish_if_changed(state, "material_strategy_event")
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
        save_ml_weighted_trailing_outputs(
            state, config,
            normal_ledger_path=weighted_normal_ledger_path,
            normal_report_path=weighted_normal_report_path,
            inverse_ledger_path=weighted_inverse_ledger_path,
            inverse_report_path=weighted_inverse_report_path,
        )
        save_ml_weighted_fixed_stop_outputs(
            state, config,
            normal_ledger_path=weighted_fixed_normal_ledger_path,
            normal_report_path=weighted_fixed_normal_report_path,
            inverse_ledger_path=weighted_fixed_inverse_ledger_path,
            inverse_report_path=weighted_fixed_inverse_report_path,
        )
        log_performance_summary(final_report, "run_complete")
        await rest.close()
    LOG.info("Average-down run complete | mode=%s active_records=%d", "DRY_RUN" if dry_run else "LIVE", len(active_strategy_records(state)))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=Path("kalshi_btc15m_average_down_config.json"))
    parser.add_argument("--state-file", type=Path, default=Path("kalshi_btc15m_average_down_state.json"))
    parser.add_argument("--report", type=Path, default=Path("kalshi_btc15m_average_down_report.json"))
    parser.add_argument(
        "--weighted-trailing-normal-ledger", type=Path,
        default=DEFAULT_WEIGHTED_TRAILING_NORMAL_LEDGER,
        help="Detailed normal-ML weighted trailing paper ledger JSON.",
    )
    parser.add_argument(
        "--weighted-trailing-normal-report", type=Path,
        default=DEFAULT_WEIGHTED_TRAILING_NORMAL_REPORT,
        help="Compact normal-ML weighted trailing paper report JSON.",
    )
    parser.add_argument(
        "--weighted-trailing-inverse-ledger", type=Path,
        default=DEFAULT_WEIGHTED_TRAILING_INVERSE_LEDGER,
        help="Detailed inverse-ML weighted trailing paper ledger JSON.",
    )
    parser.add_argument(
        "--weighted-trailing-inverse-report", type=Path,
        default=DEFAULT_WEIGHTED_TRAILING_INVERSE_REPORT,
        help="Compact inverse-ML weighted trailing paper report JSON.",
    )
    parser.add_argument(
        "--weighted-fixed-stop-normal-ledger", type=Path,
        default=DEFAULT_WEIGHTED_FIXED_STOP_NORMAL_LEDGER,
        help="Detailed normal-ML weighted fixed-stop paper ledger JSON.",
    )
    parser.add_argument(
        "--weighted-fixed-stop-normal-report", type=Path,
        default=DEFAULT_WEIGHTED_FIXED_STOP_NORMAL_REPORT,
        help="Compact normal-ML weighted fixed-stop paper report JSON.",
    )
    parser.add_argument(
        "--weighted-fixed-stop-inverse-ledger", type=Path,
        default=DEFAULT_WEIGHTED_FIXED_STOP_INVERSE_LEDGER,
        help="Detailed inverse-ML weighted fixed-stop paper ledger JSON.",
    )
    parser.add_argument(
        "--weighted-fixed-stop-inverse-report", type=Path,
        default=DEFAULT_WEIGHTED_FIXED_STOP_INVERSE_REPORT,
        help="Compact inverse-ML weighted fixed-stop paper report JSON.",
    )
    parser.add_argument("--run-seconds", type=float, default=840.0)
    parser.add_argument(
        "--paper-monitor-only", action="store_true",
        help="Dry-run only: create no primary ladder; track only normal/inverse weighted quote monitors.",
    )
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
    parser.add_argument("--previous-ml-model-path", type=Path, help="Prior active ML model, compared only on the same frozen input after a retrain transition.")
    parser.add_argument("--previous-ml-model-metadata", type=Path, help="Metadata paired with --previous-ml-model-path.")
    parser.add_argument("--previous-ml-model-run-id", default="", help="Actions run ID of the prior active model being compared.")
    parser.add_argument("--status-log-seconds", type=float)
    return parser


if __name__ == "__main__":
    configure_logging()
    raise SystemExit(asyncio.run(run(build_parser().parse_args())))
