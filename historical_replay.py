"""Historical-settlement replay with Monte Carlo execution-path simulation.

The settlement direction comes from ``historical_signals.parquet``/the Kalshi
loader and is immutable during a replay.  Only hidden execution-path behavior
is simulated here.
"""

from __future__ import annotations

import random
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from typing import Any, Iterable, Sequence

from execution_path_model import ExecutionPath, ExecutionPathModel
from strategy_core import RecoverySizingState, decimal, effective_stop_price


ZERO = Decimal("0")
ONE = Decimal("1")


@dataclass(frozen=True)
class ReplayConfiguration:
    recovery_multiplier: Decimal = Decimal("1.11")
    first_base_threshold: Decimal = Decimal("125")
    base_increment: Decimal = Decimal("0.25")
    threshold_growth_multiplier: Decimal | None = None
    stop_price: Decimal | None = Decimal("0.40")
    stop_baseline_entry_price: Decimal = Decimal("0.50")
    entry_price: Decimal = Decimal("0.49")
    stop_slippage: Decimal = ZERO
    fee_per_share: Decimal = ZERO
    starting_base: Decimal = Decimal("1.00")
    max_position: Decimal = Decimal("100.00")
    starting_bankroll: Decimal = Decimal("100.00")

    def __post_init__(self) -> None:
        object.__setattr__(self, "recovery_multiplier", decimal(self.recovery_multiplier))
        object.__setattr__(self, "first_base_threshold", decimal(self.first_base_threshold))
        object.__setattr__(self, "base_increment", decimal(self.base_increment))
        object.__setattr__(self, "threshold_growth_multiplier", decimal(self.threshold_growth_multiplier or self.recovery_multiplier))
        object.__setattr__(self, "stop_price", None if self.stop_price is None else decimal(self.stop_price))
        object.__setattr__(self, "stop_baseline_entry_price", decimal(self.stop_baseline_entry_price))
        object.__setattr__(self, "entry_price", decimal(self.entry_price))
        object.__setattr__(self, "stop_slippage", decimal(self.stop_slippage))
        object.__setattr__(self, "fee_per_share", decimal(self.fee_per_share))
        object.__setattr__(self, "starting_base", decimal(self.starting_base))
        object.__setattr__(self, "max_position", decimal(self.max_position))
        object.__setattr__(self, "starting_bankroll", decimal(self.starting_bankroll))
        if not ZERO < self.entry_price < ONE:
            raise ValueError("entry_price must be between zero and one")
        if self.stop_price is not None and not ZERO < self.stop_price < self.entry_price:
            raise ValueError("stop_price must be below entry_price")
        if self.stop_price is not None and self.entry_price > self.stop_baseline_entry_price:
            raise ValueError(
                "entry-adjusted stops above 50c require observed 41c-49c path calibration; "
                "the historical replay must not invent those touches"
            )
        if self.stop_slippage < ZERO or self.fee_per_share < ZERO:
            raise ValueError("slippage and fees cannot be negative")

    def effective_stop_price(self) -> Decimal | None:
        """Use the shared policy only where calibrated historical paths exist."""

        if self.stop_price is None:
            return None
        return effective_stop_price(self.entry_price, self.stop_price, self.stop_baseline_entry_price)


@dataclass(frozen=True)
class FundingFailure:
    simulation_id: int
    historical_market_index: int
    ticker: str
    account_balance: Decimal
    required_cash: Decimal
    quantity: Decimal
    recovery_exponent: int
    base: Decimal
    recovery_cycle_deficit: Decimal

    def to_row(self) -> dict[str, Any]:
        return {
            "simulation_id": self.simulation_id,
            "historical_market_index": self.historical_market_index,
            "ticker": self.ticker,
            "account_balance": float(self.account_balance),
            "required_cash": float(self.required_cash),
            "quantity": float(self.quantity),
            "recovery_exponent": self.recovery_exponent,
            "base": float(self.base),
            "recovery_cycle_deficit": float(self.recovery_cycle_deficit),
        }


@dataclass
class ReplayResult:
    simulation_id: int
    gross_pnl: Decimal
    net_pnl: Decimal
    final_equity: Decimal
    max_drawdown: Decimal
    minimum_required_bankroll: Decimal
    filled_trades: int
    zero_fills: int
    filled_wins: int
    filled_losses: int
    zero_fill_wins: int
    zero_fill_losses: int
    stopped_trades: int
    stopped_would_have_won: int
    reached_40: int
    reached_40_wins: int
    reached_40_losses: int
    reached_30: int
    reached_20: int
    reached_10: int
    final_base: Decimal
    max_recovery_quantity: Decimal
    cap_hit_count: int
    longest_recovery_cycle: int
    funding_failure: FundingFailure | None = None
    equity_curve: list[Decimal] = field(default_factory=list)

    def metric_row(self) -> dict[str, float | int | bool]:
        fill_rate = self.filled_trades / (self.filled_trades + self.zero_fills) if self.filled_trades + self.zero_fills else 0.0
        return {
            "simulation_id": self.simulation_id,
            "gross_pnl": float(self.gross_pnl), "net_pnl": float(self.net_pnl),
            "max_drawdown": float(self.max_drawdown),
            "minimum_required_bankroll": float(self.minimum_required_bankroll),
            "filled_trades": self.filled_trades, "zero_fills": self.zero_fills,
            "fill_rate": fill_rate, "filled_wins": self.filled_wins,
            "filled_losses": self.filled_losses, "zero_fill_wins": self.zero_fill_wins,
            "zero_fill_losses": self.zero_fill_losses, "stopped_trades": self.stopped_trades,
            "stopped_would_have_won": self.stopped_would_have_won,
            "reached_40": self.reached_40, "reached_40_wins": self.reached_40_wins, "reached_40_losses": self.reached_40_losses,
            "reached_30": self.reached_30, "reached_20": self.reached_20, "reached_10": self.reached_10,
            "final_base": float(self.final_base), "max_recovery_quantity": float(self.max_recovery_quantity),
            "cap_hit_count": self.cap_hit_count, "longest_recovery_cycle": self.longest_recovery_cycle,
            "unable_to_fund_prescribed_position": self.funding_failure is not None,
        }


def trade_pnl_per_share(
    directional_win: bool,
    path: ExecutionPath,
    configuration: ReplayConfiguration,
) -> tuple[Decimal, Decimal, str]:
    """Return gross/net per-share P&L and exit type; settlement stays actual."""

    if not path.entry_filled:
        return ZERO, ZERO, "zero_fill"
    if path.stop_triggered:
        stop_price = configuration.effective_stop_price()
        assert stop_price is not None
        gross = (stop_price - configuration.stop_slippage) - configuration.entry_price
        return gross, gross - configuration.fee_per_share, "stop"
    gross = (ONE - configuration.entry_price) if directional_win else -configuration.entry_price
    return gross, gross - configuration.fee_per_share, "settlement"


def _signal_win(signal: Any) -> bool:
    value = getattr(signal, "directional_win", None)
    if value is None and isinstance(signal, dict):
        value = signal.get("directional_win")
    return bool(value)


def _signal_ticker(signal: Any, index: int) -> str:
    value = getattr(signal, "ticker", None)
    if value is None and isinstance(signal, dict):
        value = signal.get("ticker")
    return str(value or f"historical-{index}")


def replay_one(
    signals: Sequence[Any],
    model: ExecutionPathModel,
    configuration: ReplayConfiguration,
    simulation_id: int = 0,
    seed: int = 42,
    retain_equity_curve: bool = False,
) -> ReplayResult:
    """Replay one exact outcome sequence with stochastic latent execution."""

    rng = random.Random(seed)
    sizing = RecoverySizingState(
        recovery_multiplier=configuration.recovery_multiplier,
        first_base_threshold=configuration.first_base_threshold,
        base_increment=configuration.base_increment,
        threshold_growth_multiplier=configuration.threshold_growth_multiplier,
        base_share_count=configuration.starting_base,
        max_position=configuration.max_position,
    )
    gross_pnl = net_pnl = ZERO
    peak = running_pnl = max_drawdown = ZERO
    minimum_required = ZERO
    account_balance = configuration.starting_bankroll
    first_funding_failure: FundingFailure | None = None
    equity_curve: list[Decimal] = []
    metrics = {key: 0 for key in (
        "filled_trades", "zero_fills", "filled_wins", "filled_losses", "zero_fill_wins",
        "zero_fill_losses", "stopped_trades", "stopped_would_have_won", "reached_40", "reached_40_wins", "reached_40_losses",
        "reached_30", "reached_20", "reached_10",
    )}

    for index, signal in enumerate(signals):
        directional_win = _signal_win(signal)
        # Fixed consumption preserves common random numbers across parameter
        # configurations even when entry probabilities differ.
        path = model.sample_from_uniforms(
            directional_win,
            rng.random(), (rng.random(), rng.random(), rng.random(), rng.random()),
            None if configuration.effective_stop_price() is None else float(configuration.effective_stop_price()),
        )
        if not path.entry_filled:
            metrics["zero_fills"] += 1
            metrics["zero_fill_wins" if directional_win else "zero_fill_losses"] += 1
            sizing.apply_zero_fill()
            if retain_equity_curve:
                equity_curve.append(running_pnl)
            continue

        quantity = sizing.prescribed_quantity()
        required_cash = quantity * configuration.entry_price
        minimum_required = max(minimum_required, required_cash - running_pnl)
        if account_balance < required_cash and first_funding_failure is None:
            first_funding_failure = FundingFailure(
                simulation_id=simulation_id, historical_market_index=index, ticker=_signal_ticker(signal, index),
                account_balance=account_balance, required_cash=required_cash, quantity=quantity,
                recovery_exponent=sizing.recovery_exponent, base=sizing.base_share_count,
                recovery_cycle_deficit=min(ZERO, sizing.recovery_cycle_pnl),
            )
        gross_per_share, net_per_share, exit_method = trade_pnl_per_share(directional_win, path, configuration)
        gross_trade_pnl = quantity * gross_per_share
        net_trade_pnl = quantity * net_per_share
        gross_pnl += gross_trade_pnl
        net_pnl += net_trade_pnl
        running_pnl += net_trade_pnl
        account_balance += net_trade_pnl
        peak = max(peak, running_pnl)
        max_drawdown = max(max_drawdown, peak - running_pnl)
        metrics["filled_trades"] += 1
        metrics["filled_wins" if directional_win else "filled_losses"] += 1
        if path.reached_40:
            metrics["reached_40"] += 1
            metrics["reached_40_wins" if directional_win else "reached_40_losses"] += 1
        if path.reached_30:
            metrics["reached_30"] += 1
        if path.reached_20:
            metrics["reached_20"] += 1
        if path.reached_10:
            metrics["reached_10"] += 1
        if exit_method == "stop":
            metrics["stopped_trades"] += 1
            metrics["stopped_would_have_won"] += int(directional_win)
        sizing.apply_filled_trade(net_trade_pnl)
        if retain_equity_curve:
            equity_curve.append(running_pnl)

    return ReplayResult(
        simulation_id=simulation_id, gross_pnl=gross_pnl, net_pnl=net_pnl,
        final_equity=configuration.starting_bankroll + net_pnl, max_drawdown=max_drawdown,
        minimum_required_bankroll=max(ZERO, minimum_required), funding_failure=first_funding_failure,
        final_base=sizing.base_share_count, max_recovery_quantity=sizing.max_recovery_quantity,
        cap_hit_count=sizing.cap_hit_count, longest_recovery_cycle=sizing.longest_recovery_cycle,
        equity_curve=equity_curve, **metrics,
    )


def replay_many(
    signals: Sequence[Any],
    model: ExecutionPathModel,
    configuration: ReplayConfiguration,
    simulations: int,
    seed: int = 42,
    retain_equity_curves: bool = False,
) -> list[ReplayResult]:
    return [
        replay_one(signals, model, configuration, simulation_id=index, seed=seed + index, retain_equity_curve=retain_equity_curves)
        for index in range(simulations)
    ]


def percentile(values: Iterable[Decimal | float], probability: float) -> Decimal:
    ordered = sorted(decimal(value) for value in values)
    if not ordered:
        return ZERO
    if len(ordered) == 1:
        return ordered[0]
    position = Decimal(str(probability)) * Decimal(len(ordered) - 1)
    lower = int(position)
    upper = min(len(ordered) - 1, lower + 1)
    weight = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * weight


def summarize_results(results: Sequence[ReplayResult], historical_summary: dict[str, Any] | None = None) -> dict[str, Any]:
    if not results:
        raise ValueError("at least one replay result is required")
    number = Decimal(len(results))
    metric = lambda name: [getattr(result, name) for result in results]
    filled = metric("filled_trades")
    zero = metric("zero_fills")
    filled_wins = metric("filled_wins")
    filled_losses = metric("filled_losses")
    zero_wins = metric("zero_fill_wins")
    zero_losses = metric("zero_fill_losses")
    reached_40 = metric("reached_40")
    reached_40_wins = metric("reached_40_wins")
    reached_40_losses = metric("reached_40_losses")
    total_markets = sum(filled) + sum(zero)
    total_filled = sum(filled)
    total_zero = sum(zero)
    return {
        **(historical_summary or {}),
        "simulations": len(results),
        "mean_gross_pnl": float(sum(metric("gross_pnl")) / number),
        "mean_net_pnl": float(sum(metric("net_pnl")) / number),
        "p1_net_pnl": float(percentile(metric("net_pnl"), 0.01)),
        "p5_net_pnl": float(percentile(metric("net_pnl"), 0.05)),
        "p25_net_pnl": float(percentile(metric("net_pnl"), 0.25)),
        "median_net_pnl": float(percentile(metric("net_pnl"), 0.50)),
        "p75_net_pnl": float(percentile(metric("net_pnl"), 0.75)),
        "p95_net_pnl": float(percentile(metric("net_pnl"), 0.95)),
        "p99_net_pnl": float(percentile(metric("net_pnl"), 0.99)),
        "median_max_drawdown": float(percentile(metric("max_drawdown"), 0.50)),
        "p95_max_drawdown": float(percentile(metric("max_drawdown"), 0.95)),
        "p99_max_drawdown": float(percentile(metric("max_drawdown"), 0.99)),
        "p50_required_bankroll": float(percentile(metric("minimum_required_bankroll"), 0.50)),
        "p75_required_bankroll": float(percentile(metric("minimum_required_bankroll"), 0.75)),
        "p90_required_bankroll": float(percentile(metric("minimum_required_bankroll"), 0.90)),
        "p95_required_bankroll": float(percentile(metric("minimum_required_bankroll"), 0.95)),
        "p99_required_bankroll": float(percentile(metric("minimum_required_bankroll"), 0.99)),
        "p999_required_bankroll": float(percentile(metric("minimum_required_bankroll"), 0.999)),
        "simulated_mean_fill_rate": sum(filled) / total_markets if total_markets else 0.0,
        "simulated_49c_entry_fill_rate": sum(filled) / total_markets if total_markets else 0.0,
        "simulated_fill_rate_p5": float(percentile([
            Decimal(result.filled_trades) / Decimal(result.filled_trades + result.zero_fills or 1) for result in results
        ], 0.05)),
        "simulated_fill_rate_p50": float(percentile([
            Decimal(result.filled_trades) / Decimal(result.filled_trades + result.zero_fills or 1) for result in results
        ], 0.50)),
        "simulated_fill_rate_p95": float(percentile([
            Decimal(result.filled_trades) / Decimal(result.filled_trades + result.zero_fills or 1) for result in results
        ], 0.95)),
        "simulated_win_side_fill_probability": sum(filled_wins) / (sum(filled_wins) + sum(zero_wins)) if sum(filled_wins) + sum(zero_wins) else 0.0,
        "simulated_loss_side_fill_probability": sum(filled_losses) / (sum(filled_losses) + sum(zero_losses)) if sum(filled_losses) + sum(zero_losses) else 0.0,
        "simulated_zero_fill_wr": sum(zero_wins) / total_zero if total_zero else 0.0,
        "simulated_filled_trade_wr": sum(filled_wins) / total_filled if total_filled else 0.0,
        "simulated_40_region_frequency": sum(reached_40) / total_markets if total_markets else 0.0,
        "simulated_40_region_wr": sum(reached_40_wins) / (sum(reached_40_wins) + sum(reached_40_losses)) if sum(reached_40) else 0.0,
        "simulated_stop_frequency": sum(metric("stopped_trades")) / total_filled if total_filled else 0.0,
        "simulated_reach_30_frequency": sum(metric("reached_30")) / total_markets if total_markets else 0.0,
        "simulated_reach_20_frequency": sum(metric("reached_20")) / total_markets if total_markets else 0.0,
        "simulated_reach_10_frequency": sum(metric("reached_10")) / total_markets if total_markets else 0.0,
        "median_final_base": float(percentile(metric("final_base"), 0.50)),
        "median_max_recovery_quantity": float(percentile(metric("max_recovery_quantity"), 0.50)),
        "p95_max_recovery_quantity": float(percentile(metric("max_recovery_quantity"), 0.95)),
        "cap_hit_probability": sum(value > 0 for value in metric("cap_hit_count")) / len(results),
        "median_longest_recovery_cycle": float(percentile(metric("longest_recovery_cycle"), 0.50)),
        "p95_longest_recovery_cycle": float(percentile(metric("longest_recovery_cycle"), 0.95)),
        "survival_probability_100": sum(result.minimum_required_bankroll <= Decimal("100") for result in results) / len(results),
        "survival_probability_150": sum(result.minimum_required_bankroll <= Decimal("150") for result in results) / len(results),
        "survival_probability_250": sum(result.minimum_required_bankroll <= Decimal("250") for result in results) / len(results),
        "survival_probability_500": sum(result.minimum_required_bankroll <= Decimal("500") for result in results) / len(results),
        "survival_probability_1000": sum(result.minimum_required_bankroll <= Decimal("1000") for result in results) / len(results),
    }
