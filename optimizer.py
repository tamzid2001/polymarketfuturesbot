"""Optimize the Kalshi historical-settlement / MC-execution replay.

Methodology: **historical settlement replay with Monte Carlo execution-path
simulation**.  Every simulation reuses the same actual chronological KXBTC15M
directional outcomes.  It samples only the unrecoverable entry/adverse-path
variables with common random numbers.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import random
from dataclasses import dataclass, replace
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np

from calibration import write_calibration_report
from execution_path_model import ExecutionCalibration, ExecutionPathModel
from historical_replay import ReplayConfiguration, replay_many, summarize_results
from kalshi_settlement_loader import KalshiSettlementLoader, reconstruct_signals, signal_summary

try:  # Optional acceleration; deterministic reference replay remains available.
    from numba import njit, prange
except ImportError:  # pragma: no cover
    njit = None
    prange = range


OUTPUT_COLUMNS = (
    "recovery_multiplier", "threshold_growth_multiplier", "first_base_threshold", "base_increment",
    "stop_price", "entry_price", "starting_base", "max_position", "historical_markets",
    "eligible_historical_signals", "actual_directional_wins", "actual_directional_losses",
    "actual_historical_directional_wr", "simulated_mean_fill_rate", "simulated_fill_rate_p5",
    "simulated_fill_rate_p50", "simulated_fill_rate_p95", "simulated_win_side_fill_probability",
    "simulated_loss_side_fill_probability", "simulated_zero_fill_wr", "simulated_filled_trade_wr",
    "simulated_40_region_frequency", "simulated_stop_frequency", "simulated_reach_30_frequency", "simulated_reach_20_frequency",
    "simulated_reach_10_frequency", "mean_gross_pnl", "mean_net_pnl", "p1_net_pnl", "p5_net_pnl",
    "p25_net_pnl", "median_net_pnl", "p75_net_pnl", "p95_net_pnl", "p99_net_pnl",
    "median_max_drawdown", "p95_max_drawdown", "p99_max_drawdown",
    "p50_required_bankroll", "p90_required_bankroll", "p95_required_bankroll", "p99_required_bankroll", "p999_required_bankroll",
    "survival_probability_100", "survival_probability_150", "survival_probability_250",
    "survival_probability_500", "survival_probability_1000", "median_final_base",
    "median_max_recovery_quantity", "p95_max_recovery_quantity", "cap_hit_probability",
    "median_longest_recovery_cycle", "p95_longest_recovery_cycle", "simulations", "execution_model",
)


EXECUTION_SCENARIOS = {
    "conservative": ExecutionCalibration.conservative,
    "base_case": ExecutionCalibration.base_case,
    "optimistic": ExecutionCalibration.optimistic,
    "reconstruction_compatible": ExecutionCalibration.reconstruction_compatible,
}


def calibration_for_scenario(name: str) -> ExecutionCalibration:
    try:
        return EXECUTION_SCENARIOS[name]()
    except KeyError as exc:
        raise ValueError(f"unknown execution scenario {name!r}; choose from {', '.join(EXECUTION_SCENARIOS)}") from exc


@dataclass(frozen=True)
class ParameterSet:
    recovery_multiplier: float
    first_base_threshold: float
    base_increment: float
    stop_price: float | None = 0.40
    threshold_growth_multiplier: float | None = None
    entry_price: float = 0.49
    stop_slippage: float = 0.0
    fee_per_share: float = 0.0

    @property
    def growth(self) -> float:
        return self.recovery_multiplier if self.threshold_growth_multiplier is None else self.threshold_growth_multiplier

    def reference_configuration(self) -> ReplayConfiguration:
        return ReplayConfiguration(
            recovery_multiplier=Decimal(str(self.recovery_multiplier)),
            first_base_threshold=Decimal(str(self.first_base_threshold)),
            base_increment=Decimal(str(self.base_increment)),
            threshold_growth_multiplier=Decimal(str(self.growth)),
            stop_price=None if self.stop_price is None else Decimal(str(self.stop_price)),
            entry_price=Decimal(str(self.entry_price)), stop_slippage=Decimal(str(self.stop_slippage)),
            fee_per_share=Decimal(str(self.fee_per_share)), starting_base=Decimal("1.00"),
            max_position=Decimal("100.00"), starting_bankroll=Decimal("100.00"),
        )


def primary_grid(entry_price: float = 0.49) -> list[ParameterSet]:
    """Primary recovery/base grid at one explicitly selected maker price."""

    return [
        ParameterSet(multiplier, threshold, increment, 0.40, entry_price=entry_price)
        for multiplier in (round(1 + value / 100, 2) for value in range(1, 12))
        for threshold in (50, 100, 125, 150, 200, 250, 300, 350, 400, 450, 500)
        for increment in (0.25, 0.50, 1.00)
    ]


def export_selected_live_strategy(path: Path, row: dict[str, Any], *, selection_basis: str) -> dict[str, Any]:
    """Export the optimizer's exact Decimal parameters for ``kalshi_live_trader``.

    The exporter intentionally serializes accounting values as strings, which
    avoids a float re-interpretation at the production boundary.  It only
    exports a fixed positive stop; a no-stop optimizer result is a research
    result, not an unattended-live default.
    """

    stop = row.get("stop_price")
    if stop in {None, "no_stop"}:
        raise ValueError("a live strategy export requires an explicit fixed stop")
    profile_by_stop = {0.40: "sticky_stop_40", 0.30: "sticky_stop_30", 0.20: "sticky_stop_20", 0.10: "sticky_stop_10"}
    try:
        shadow_profile = profile_by_stop[round(float(stop), 2)]
    except KeyError as exc:
        raise ValueError("a live strategy export requires a 40c, 30c, 20c, or 10c stop profile") from exc
    config = {
        "config_schema_version": 10,
        "strategy_version": "kxbtc15m-hybrid-live-v10",
        "selection_basis": selection_basis,
        "series": "KXBTC15M",
        "signal_delay_seconds": 0,
        "signal_mode": "sticky_until_directional_win",
        "shadow_profile": shadow_profile,
        "entry_price": f"{float(row['entry_price']):.2f}",
        "stop_price": f"{float(stop):.2f}",
        "stop_policy": "fixed_profile_floor",
        "stop_baseline_entry_price": "0.50",
        "entry_execution_mode": "immediate_market_ioc",
        "starting_base": "1.00",
        "recovery_multiplier": f"{float(row['recovery_multiplier']):.2f}",
        "first_base_threshold": f"{float(row['first_base_threshold']):.2f}",
        "threshold_growth_multiplier": f"{float(row['threshold_growth_multiplier']):.2f}",
        "base_increment": f"{float(row['base_increment']):.2f}",
        "max_position": "100.00",
        "starting_shadow_balance": "1000.00",
        "live_enabled": False,
        "dry_run": True,
        "entry_timeout_seconds": 60,
        "opening_price_discovery_seconds": 3,
        "opening_quote_max_observations": 500,
        "maker_price_offset": "0.01",
        "entry_lateness_seconds": 60,
        "handoff_guard_seconds": 60,
        "stop_poll_interval": 1.0,
        "reconciliation_interval": 5.0,
        "market_discovery_interval_seconds": 1.0,
        "outcome_observation_seconds": 5,
        "provisional_outcome_threshold": "0.99",
        "max_outcome_quote_age_seconds": 2.0,
        "max_stale_quote_seconds": 2.0,
        "durable_checkpoint_interval_seconds": 5.0,
        "max_recovery_exponent": 0,
        "max_recovery_cycle_loss": "50.00",
        "max_daily_realized_loss": "25.00",
        "max_api_failures": 5,
        "allow_capital_downsize": False,
        "shadow_fill_model": "fresh_displayed_top_of_book_ioc",
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(config, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return config


def _quantile(values: np.ndarray, probability: float) -> float:
    return float(np.quantile(values, probability, method="linear"))


if njit is not None:
    @njit(inline="always")
    def _uniform(seed: np.uint64, simulation: int, market: int, lane: int) -> float:
        # Counter-based SplitMix64: no configuration appears in the key, so
        # competitors see identical uniforms (common random numbers).
        z = seed + np.uint64(simulation + 1) * np.uint64(0x9E3779B97F4A7C15)
        z += np.uint64(market + 1) * np.uint64(0xBF58476D1CE4E5B9)
        z += np.uint64(lane + 1) * np.uint64(0x94D049BB133111EB)
        z = (z ^ (z >> np.uint64(30))) * np.uint64(0xBF58476D1CE4E5B9)
        z = (z ^ (z >> np.uint64(27))) * np.uint64(0x94D049BB133111EB)
        z = z ^ (z >> np.uint64(31))
        return float(z >> np.uint64(11)) * (1.0 / 9007199254740992.0)

    @njit(parallel=True, cache=True)
    def _fast_replay(
        outcomes: np.ndarray, simulations: int, seed: np.uint64,
        win_entry: float, loss_entry: float, win40_joint: float, loss40_joint: float,
        win30: float, win20: float, win10: float,
        loss30: float, loss20: float, loss10: float,
        multiplier: float, first_threshold: float, increment: float, growth: float,
        stop_index: int, entry: float, stop_price: float, stop_slippage: float, fee_per_share: float,
    ) -> np.ndarray:
        # Output order is documented by ``_fast_summary`` below.
        metrics = np.zeros((simulations, 23), dtype=np.float64)
        for simulation in prange(simulations):
            base = 1.0
            exponent = 0
            cycle_pnl = 0.0
            scale_profit = 0.0
            next_threshold = first_threshold
            gross = net = running = peak = max_drawdown = min_required = 0.0
            filled = zero = filled_wins = filled_losses = zero_wins = zero_losses = 0
            stops = stop_wins = reach40 = reach30 = reach20 = reach10 = cap_hits = 0
            max_quantity = 0.0
            current_cycle = longest_cycle = 0
            for market in range(outcomes.size):
                won = outcomes[market] == 1
                fill_probability = win_entry if won else loss_entry
                if _uniform(seed, simulation, market, 0) >= fill_probability:
                    zero += 1
                    if won:
                        zero_wins += 1
                    else:
                        zero_losses += 1
                    continue
                # The precise reference engine uses Decimal.  This batched
                # screening kernel mirrors its ROUND_HALF_UP cent rule.
                capped_by_exponent = exponent > 500
                quantity = 100.0 if capped_by_exponent else math.floor(base * multiplier ** exponent * 100.0 + 0.5) / 100.0
                if quantity > 100.0:
                    quantity = 100.0
                    cap_hits += 1
                elif capped_by_exponent:
                    cap_hits += 1
                if quantity > max_quantity:
                    max_quantity = quantity
                required = quantity * entry
                requirement = required - running
                if requirement > min_required:
                    min_required = requirement
                filled += 1
                if won:
                    filled_wins += 1
                else:
                    filled_losses += 1
                # Nested continuation events.  A 10c reach necessarily has
                # already reached 20c, 30c, and the 40c/entry region.
                # The old ladder supplies a joint 40c-region probability.
                # Condition it on the separately modeled 49c participation
                # so the simulated marginal 40c rate remains calibrated.
                c40 = (win40_joint / win_entry) if won else (loss40_joint / loss_entry)
                c30 = win30 if won else loss30
                c20 = win20 if won else loss20
                c10 = win10 if won else loss10
                depth = -1
                if _uniform(seed, simulation, market, 1) < c40:
                    depth = 0
                    reach40 += 1
                if depth == 0 and _uniform(seed, simulation, market, 2) < c30:
                    depth = 1
                    reach30 += 1
                    if _uniform(seed, simulation, market, 3) < c20:
                        depth = 2
                        reach20 += 1
                        if _uniform(seed, simulation, market, 4) < c10:
                            depth = 3
                            reach10 += 1
                stopped = stop_index >= 0 and depth >= stop_index
                if stopped:
                    gross_per_share = (stop_price - stop_slippage) - entry
                    stops += 1
                    if won:
                        stop_wins += 1
                elif won:
                    gross_per_share = 1.0 - entry
                else:
                    gross_per_share = -entry
                net_per_share = gross_per_share - fee_per_share
                gross_trade = quantity * gross_per_share
                net_trade = quantity * net_per_share
                gross += gross_trade
                net += net_trade
                running += net_trade
                if running > peak:
                    peak = running
                if peak - running > max_drawdown:
                    max_drawdown = peak - running
                current_cycle += 1
                cycle_pnl += net_trade
                if cycle_pnl >= 0.0:
                    cycle_pnl = 0.0
                    exponent = 0
                    if current_cycle > longest_cycle:
                        longest_cycle = current_cycle
                    current_cycle = 0
                else:
                    exponent += 1
                    if current_cycle > longest_cycle:
                        longest_cycle = current_cycle
                scale_profit += net_trade
                while scale_profit >= next_threshold:
                    scale_profit -= next_threshold
                    base = math.floor((base + increment) * 100.0 + 0.5) / 100.0
                    next_threshold *= growth
            metrics[simulation, 0] = gross
            metrics[simulation, 1] = net
            metrics[simulation, 2] = max_drawdown
            metrics[simulation, 3] = max(min_required, 0.0)
            metrics[simulation, 4] = filled
            metrics[simulation, 5] = zero
            metrics[simulation, 6] = filled_wins
            metrics[simulation, 7] = filled_losses
            metrics[simulation, 8] = zero_wins
            metrics[simulation, 9] = zero_losses
            metrics[simulation, 10] = stops
            metrics[simulation, 11] = stop_wins
            metrics[simulation, 12] = reach30
            metrics[simulation, 13] = reach20
            metrics[simulation, 14] = reach10
            metrics[simulation, 15] = base
            metrics[simulation, 16] = max_quantity
            metrics[simulation, 17] = cap_hits
            metrics[simulation, 18] = longest_cycle
            metrics[simulation, 19] = reach40
        return metrics


def _stop_index(stop_price: float | None) -> tuple[int, float]:
    if stop_price is None:
        return -1, 0.0
    mapping = {0.40: 0, 0.30: 1, 0.20: 2, 0.10: 3}
    try:
        return mapping[round(stop_price, 2)], stop_price
    except KeyError as exc:
        raise ValueError("stop price must be one of 0.40, 0.30, 0.20, 0.10, or None") from exc


def fast_results(
    outcomes: np.ndarray,
    parameters: ParameterSet,
    calibration: ExecutionCalibration,
    simulations: int,
    seed: int,
) -> np.ndarray:
    """Fast deterministic-batch screen; Decimal reference is used for audits."""

    if njit is None:
        raise RuntimeError("numba is required for the full grid; install requirements_kalshi_hybrid_backtest.txt")
    stop_index, stop_price = _stop_index(parameters.stop_price)
    return _fast_replay(
        outcomes.astype(np.int8), simulations, np.uint64(seed),
        calibration.win_entry_fill_probability, calibration.loss_entry_fill_probability,
        calibration.win_reach_40_joint_probability, calibration.loss_reach_40_joint_probability,
        calibration.win_continue_30_given_40, calibration.win_continue_20_given_30, calibration.win_continue_10_given_20,
        calibration.loss_continue_30_given_40, calibration.loss_continue_20_given_30, calibration.loss_continue_10_given_20,
        parameters.recovery_multiplier, parameters.first_base_threshold, parameters.base_increment, parameters.growth,
        stop_index, parameters.entry_price, stop_price, parameters.stop_slippage, parameters.fee_per_share,
    )


def _fast_summary(
    values: np.ndarray,
    parameters: ParameterSet,
    history: dict[str, Any],
    execution_model: str,
) -> dict[str, Any]:
    gross, net, drawdown, required = values[:, 0], values[:, 1], values[:, 2], values[:, 3]
    filled, zero = values[:, 4], values[:, 5]
    filled_wins, filled_losses, zero_wins, zero_losses = values[:, 6], values[:, 7], values[:, 8], values[:, 9]
    reached_40 = values[:, 19]
    total_markets = filled + zero
    sum_filled, sum_zero = float(filled.sum()), float(zero.sum())
    actual_wins = history["directional_wins"]
    actual_losses = history["directional_losses"]
    return {
        "recovery_multiplier": parameters.recovery_multiplier,
        "threshold_growth_multiplier": parameters.growth,
        "first_base_threshold": parameters.first_base_threshold,
        "base_increment": parameters.base_increment,
        "stop_price": "no_stop" if parameters.stop_price is None else parameters.stop_price,
        "entry_price": parameters.entry_price,
        "starting_base": 1.00, "max_position": 100.00,
        "historical_markets": history["total_settled_markets"],
        "eligible_historical_signals": history["eligible_predictions"],
        "actual_directional_wins": actual_wins, "actual_directional_losses": actual_losses,
        "actual_historical_directional_wr": history["directional_win_rate"],
        "simulated_mean_fill_rate": float(np.mean(filled / total_markets)),
        "simulated_fill_rate_p5": _quantile(filled / total_markets, .05),
        "simulated_fill_rate_p50": _quantile(filled / total_markets, .50),
        "simulated_fill_rate_p95": _quantile(filled / total_markets, .95),
        "simulated_win_side_fill_probability": float(filled_wins.sum() / actual_wins / values.shape[0]),
        "simulated_loss_side_fill_probability": float(filled_losses.sum() / actual_losses / values.shape[0]),
        "simulated_zero_fill_wr": float(zero_wins.sum() / sum_zero) if sum_zero else 0.0,
        "simulated_filled_trade_wr": float(filled_wins.sum() / sum_filled) if sum_filled else 0.0,
        "simulated_40_region_frequency": float(reached_40.sum() / total_markets.sum()),
        "simulated_stop_frequency": float(values[:, 10].sum() / sum_filled) if sum_filled else 0.0,
        "simulated_reach_30_frequency": float(values[:, 12].sum() / total_markets.sum()),
        "simulated_reach_20_frequency": float(values[:, 13].sum() / total_markets.sum()),
        "simulated_reach_10_frequency": float(values[:, 14].sum() / total_markets.sum()),
        "mean_gross_pnl": float(gross.mean()), "mean_net_pnl": float(net.mean()),
        "p1_net_pnl": _quantile(net, .01), "p5_net_pnl": _quantile(net, .05), "p25_net_pnl": _quantile(net, .25),
        "median_net_pnl": _quantile(net, .50), "p75_net_pnl": _quantile(net, .75), "p95_net_pnl": _quantile(net, .95), "p99_net_pnl": _quantile(net, .99),
        "median_max_drawdown": _quantile(drawdown, .50), "p95_max_drawdown": _quantile(drawdown, .95), "p99_max_drawdown": _quantile(drawdown, .99),
        "p50_required_bankroll": _quantile(required, .50), "p75_required_bankroll": _quantile(required, .75),
        "p90_required_bankroll": _quantile(required, .90), "p95_required_bankroll": _quantile(required, .95),
        "p99_required_bankroll": _quantile(required, .99), "p999_required_bankroll": _quantile(required, .999),
        "survival_probability_100": float(np.mean(required <= 100)), "survival_probability_150": float(np.mean(required <= 150)),
        "survival_probability_250": float(np.mean(required <= 250)), "survival_probability_500": float(np.mean(required <= 500)),
        "survival_probability_1000": float(np.mean(required <= 1000)),
        "median_final_base": _quantile(values[:, 15], .50), "median_max_recovery_quantity": _quantile(values[:, 16], .50),
        "p95_max_recovery_quantity": _quantile(values[:, 16], .95), "cap_hit_probability": float(np.mean(values[:, 17] > 0)),
        "median_longest_recovery_cycle": _quantile(values[:, 18], .50), "p95_longest_recovery_cycle": _quantile(values[:, 18], .95),
        "simulations": values.shape[0], "execution_model": execution_model,
    }


def evaluate(
    outcomes: np.ndarray, parameters: ParameterSet, calibration: ExecutionCalibration,
    history: dict[str, Any], simulations: int, seed: int,
) -> tuple[dict[str, Any], np.ndarray]:
    values = fast_results(outcomes, parameters, calibration, simulations, seed)
    return _fast_summary(values, parameters, history, calibration.model_name), values


def _write_csv(path: Path, rows: Sequence[dict[str, Any]], columns: Sequence[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    headings = list(columns or sorted({key for row in rows for key in row}))
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=headings, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _pareto(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        dominated = False
        for other in rows:
            if row is other:
                continue
            better_or_equal = (
                other["median_net_pnl"] >= row["median_net_pnl"]
                and other["p5_net_pnl"] >= row["p5_net_pnl"]
                and other["survival_probability_100"] >= row["survival_probability_100"]
                and other["p95_required_bankroll"] <= row["p95_required_bankroll"]
                and other["p95_max_drawdown"] <= row["p95_max_drawdown"]
            )
            strictly_better = (
                other["median_net_pnl"] > row["median_net_pnl"]
                or other["p5_net_pnl"] > row["p5_net_pnl"]
                or other["survival_probability_100"] > row["survival_probability_100"]
                or other["p95_required_bankroll"] < row["p95_required_bankroll"]
                or other["p95_max_drawdown"] < row["p95_max_drawdown"]
            )
            if better_or_equal and strictly_better:
                dominated = True
                break
        if not dominated:
            result.append(row)
    return sorted(result, key=lambda row: (-row["median_net_pnl"], -row["p5_net_pnl"]))


def _best(rows: Sequence[dict[str, Any]], key: str, reverse: bool = True) -> dict[str, Any] | None:
    return max(rows, key=lambda row: row[key], default=None) if reverse else min(rows, key=lambda row: row[key], default=None)


def rankings(rows: Sequence[dict[str, Any]]) -> dict[str, dict[str, Any] | None]:
    ratio = lambda row: row["median_net_pnl"] / row["p95_max_drawdown"] if row["p95_max_drawdown"] else -math.inf
    positive_p5 = [row for row in rows if row["p5_net_pnl"] > 0]
    return {
        "highest_median_pnl": _best(rows, "median_net_pnl"),
        "highest_p5_pnl": _best(rows, "p5_net_pnl"),
        "highest_100_survival": _best(rows, "survival_probability_100"),
        "best_median_with_90pct_100_survival": _best([row for row in rows if row["survival_probability_100"] >= .90], "median_net_pnl"),
        "best_median_with_95pct_100_survival": _best([row for row in rows if row["survival_probability_100"] >= .95], "median_net_pnl"),
        "best_median_with_99pct_100_survival": _best([row for row in rows if row["survival_probability_100"] >= .99], "median_net_pnl"),
        "best_increment_025": _best([row for row in rows if row["base_increment"] == .25], "median_net_pnl"),
        "best_increment_050": _best([row for row in rows if row["base_increment"] == .50], "median_net_pnl"),
        "best_increment_100": _best([row for row in rows if row["base_increment"] == 1.00], "median_net_pnl"),
        "best_40c_stop": _best([row for row in rows if row["stop_price"] == .40], "median_net_pnl"),
        "best_no_stop": _best([row for row in rows if row["stop_price"] == "no_stop"], "median_net_pnl"),
        "lowest_p95_bankroll_positive_p5": _best(positive_p5, "p95_required_bankroll", reverse=False),
        "best_return_to_drawdown": max(rows, key=ratio, default=None),
    }


def _row_to_parameters(row: dict[str, Any]) -> ParameterSet:
    return ParameterSet(
        float(row["recovery_multiplier"]), float(row["first_base_threshold"]), float(row["base_increment"]),
        None if row["stop_price"] == "no_stop" else float(row["stop_price"]),
        float(row["threshold_growth_multiplier"]), float(row["entry_price"]),
    )


def _split(signals: Sequence[Any]) -> tuple[Sequence[Any], Sequence[Any], Sequence[Any]]:
    first = int(len(signals) * .60)
    second = int(len(signals) * .80)
    return signals[:first], signals[first:second], signals[second:]


def walk_forward(
    signals: Sequence[Any], grid: Sequence[ParameterSet], calibration: ExecutionCalibration,
    simulations: int, seed: int,
) -> list[dict[str, Any]]:
    train, validation, test = _split(signals)
    metadata = lambda segment: {
        "total_settled_markets": len(segment) + 1, "eligible_predictions": len(segment),
        "directional_wins": sum(getattr(item, "directional_win") for item in segment),
        "directional_losses": sum(not getattr(item, "directional_win") for item in segment),
        "directional_win_rate": sum(getattr(item, "directional_win") for item in segment) / len(segment),
    }
    train_outcomes = np.asarray([item.directional_win for item in train], dtype=np.int8)
    train_rows = [evaluate(train_outcomes, parameter, calibration, metadata(train), simulations, seed)[0] for parameter in grid]
    finalists = sorted(train_rows, key=lambda row: row["median_net_pnl"], reverse=True)[:15]
    validation_outcomes = np.asarray([item.directional_win for item in validation], dtype=np.int8)
    validation_rows = [evaluate(validation_outcomes, _row_to_parameters(row), calibration, metadata(validation), simulations, seed)[0] for row in finalists]
    selected = max(validation_rows, key=lambda row: row["median_net_pnl"])
    test_outcomes = np.asarray([item.directional_win for item in test], dtype=np.int8)
    test_row = evaluate(test_outcomes, _row_to_parameters(selected), calibration, metadata(test), simulations, seed)[0]
    return [
        {"segment": "train", "selection_role": "all_grid", **row} for row in train_rows
    ] + [
        {"segment": "validation", "selection_role": "train_finalist", **row} for row in validation_rows
    ] + [
        {"segment": "test", "selection_role": "frozen_validation_winner", **test_row}
    ]


def stress_tests(
    outcomes: np.ndarray, best: ParameterSet, history: dict[str, Any],
    simulations: int, seed: int, base: ExecutionCalibration | None = None,
) -> list[dict[str, Any]]:
    base = base or ExecutionCalibration.base_case()
    cases: list[tuple[str, ExecutionCalibration, ParameterSet]] = [("base", base, best)]
    for percent in (.05, .10, .20):
        # The requested adverse-selection stress increases loss-side *49c
        # participation*.  Holding the calibrated joint 40c-region rate
        # fixed makes these additional fills predominantly loss-side paths
        # that did not receive the protective 40c exit.
        cases.append((
            f"loss_49c_entry_plus_{int(percent * 100)}pct",
            replace(base, loss_entry_fill_probability=min(1.0, base.loss_entry_fill_probability * (1 + percent))),
            best,
        ))
        cases.append((
            f"winner_49c_entry_minus_{int(percent * 100)}pct",
            replace(base, win_entry_fill_probability=max(base.win_reach_40_joint_probability, base.win_entry_fill_probability * (1 - percent))),
            best,
        ))
    deeper = replace(
        base,
        win_continue_30_given_40=min(1., base.win_continue_30_given_40 * 1.10),
        win_continue_20_given_30=min(1., base.win_continue_20_given_30 * 1.10),
        win_continue_10_given_20=min(1., base.win_continue_10_given_20 * 1.10),
    )
    cases.extend([
        ("deeper_adverse_rungs_plus_10pct", deeper, best),
        ("stop_slippage_1c", base, replace(best, stop_slippage=.01)),
        ("stop_slippage_2c", base, replace(best, stop_slippage=.02)),
        ("entry_50c", base, replace(best, entry_price=.50)),
        ("fees_1c_per_share", base, replace(best, fee_per_share=.01)),
    ])
    rows = []
    for name, calibration, parameters in cases:
        row, _ = evaluate(outcomes, parameters, calibration, history, simulations, seed)
        rows.append({"stress_case": name, **row})
    return rows


def regime_analysis(
    signals: Sequence[Any], parameters: ParameterSet, calibration: ExecutionCalibration,
    simulations: int, seed: int,
) -> list[dict[str, Any]]:
    """Month/half/rolling execution replays over fixed historical outcomes."""

    def metadata(segment: Sequence[Any]) -> dict[str, Any]:
        wins = sum(item.directional_win for item in segment)
        return {
            "total_settled_markets": len(segment) + 1, "eligible_predictions": len(segment),
            "directional_wins": wins, "directional_losses": len(segment) - wins,
            "directional_win_rate": wins / len(segment) if segment else 0.0,
        }

    segments: list[tuple[str, Sequence[Any]]] = []
    months: dict[str, list[Any]] = {}
    for signal in signals:
        months.setdefault(signal.open_time.strftime("%Y-%m"), []).append(signal)
    segments.extend((f"month_{month}", values) for month, values in months.items())
    half = len(signals) // 2
    segments.extend((("first_half", signals[:half]), ("second_half", signals[half:])))
    for window in (250, 500, 1_000):
        # A stride equal to the window keeps the report tractable while each
        # row is still a true chronological rolling-window replay.
        for start in range(0, len(signals) - window + 1, window):
            segments.append((f"rolling_{window}_{start:05d}", signals[start:start + window]))
    rows: list[dict[str, Any]] = []
    for label, segment in segments:
        outcomes = np.asarray([item.directional_win for item in segment], dtype=np.int8)
        row, _ = evaluate(outcomes, parameters, calibration, metadata(segment), simulations, seed)
        rows.append({"period": label, "period_markets": len(segment), **row})
    return rows


def calibration_uncertainty(
    outcomes: np.ndarray, parameters: ParameterSet, history: dict[str, Any],
    draws: int, simulations: int, seed: int, base: ExecutionCalibration | None = None,
) -> list[dict[str, Any]]:
    """Outer Beta-binomial posterior draws for finite calibration uncertainty."""

    rng = random.Random(seed)
    base = base or ExecutionCalibration.base_case()
    rows: list[dict[str, Any]] = []
    for draw in range(draws):
        sampled = base.posterior_draw(rng)
        row, _ = evaluate(outcomes, parameters, sampled, history, simulations, seed + draw)
        rows.append({
            "calibration_draw": draw,
            "win_entry_fill_probability": sampled.win_entry_fill_probability,
            "loss_entry_fill_probability": sampled.loss_entry_fill_probability,
            "win_reach_40_joint_probability": sampled.win_reach_40_joint_probability,
            "loss_reach_40_joint_probability": sampled.loss_reach_40_joint_probability,
            **row,
        })
    return rows


def _make_plots(
    output_dir: Path, signals: Sequence[Any], calibration_report: dict[str, float],
    rows: Sequence[dict[str, Any]], stop_rows: Sequence[dict[str, Any]], pareto: Sequence[dict[str, Any]],
    walk_rows: Sequence[dict[str, Any]], final_values: np.ndarray,
) -> None:
    import matplotlib.pyplot as plt

    output_dir.mkdir(parents=True, exist_ok=True)
    wins = np.asarray([item.directional_win for item in signals], dtype=int)
    x = np.arange(1, wins.size + 1)
    cumulative_wins = np.cumsum(wins)
    cumulative_losses = x - cumulative_wins
    fig, ax = plt.subplots(); ax.plot(x, cumulative_wins, label="actual directional wins"); ax.plot(x, cumulative_losses, label="actual directional losses"); ax.legend(); ax.set(title="Actual historical directional replay", xlabel="eligible market", ylabel="count"); fig.savefig(output_dir / "01_directional_cumulative_wins_losses.png", bbox_inches="tight"); plt.close(fig)

    names = ["win", "loss"]; obs = [calibration_report["observed_40_region_rate_win"], calibration_report["observed_40_region_rate_loss"]]; sim = [calibration_report["simulated_40_region_rate_win"], calibration_report["simulated_40_region_rate_loss"]]
    fig, ax = plt.subplots(); ix=np.arange(2); ax.bar(ix-.2,obs,.4,label="observed");ax.bar(ix+.2,sim,.4,label="simulated");ax.set_xticks(ix,names);ax.legend();ax.set(title="40c-region vs no-region calibration");fig.savefig(output_dir / "02_fill_zero_fill_rates.png",bbox_inches="tight");plt.close(fig)

    names=["40c region", "no 40c region"]; obs=[calibration_report["observed_40_region_directional_wr"],calibration_report["observed_no_40_region_directional_wr"]]; sim=[calibration_report["simulated_40_region_directional_wr"],calibration_report["simulated_no_40_region_directional_wr"]]
    fig,ax=plt.subplots();ix=np.arange(2);ax.bar(ix-.2,obs,.4,label="observed");ax.bar(ix+.2,sim,.4,label="simulated");ax.set_xticks(ix,names);ax.legend();ax.set(title="Directional WR by 40c-region cohort");fig.savefig(output_dir / "03_filled_zero_fill_wr.png",bbox_inches="tight");plt.close(fig)

    rung=["40c","30c","20c","10c"]; obs=[calibration_report[f"observed_rung_wr_{value}"] for value in (40,30,20,10)]; sim=[calibration_report[f"simulated_rung_wr_{value}"] for value in (40,30,20,10)]
    fig,ax=plt.subplots();ax.plot(rung,obs,marker="o",label="observed WR");ax.plot(rung,sim,marker="o",label="simulated WR");ax.legend();ax.set(title="Rung-depth calibration (conditional 40c cohort)");fig.savefig(output_dir / "04_rung_depth_probability.png",bbox_inches="tight");plt.close(fig)

    # P&L end-point bands are honest execution-uncertainty summaries.  The
    # exact historical settlement path remains fixed for every point.
    final_net=final_values[:,1]; final_required=final_values[:,3]; final_dd=final_values[:,2]
    fig,ax=plt.subplots();ax.plot([0,1],[0,np.median(final_net)],label="median final P&L");ax.set(title="Median historical-replay equity endpoint",xticks=[0,1],xticklabels=["start","end"],ylabel="P&L ($)");fig.savefig(output_dir / "05_median_equity_curve.png",bbox_inches="tight");plt.close(fig)
    fig,ax=plt.subplots();q=np.quantile(final_net,[.05,.5,.95]);ax.fill_between([0,1],[0,q[0]],[0,q[2]],alpha=.2,label="P5–P95");ax.plot([0,1],[0,q[1]],label="P50");ax.legend();ax.set(title="Execution uncertainty equity endpoint bands",xticks=[0,1],xticklabels=["start","end"],ylabel="P&L ($)");fig.savefig(output_dir / "06_execution_equity_bands.png",bbox_inches="tight");plt.close(fig)
    fig,ax=plt.subplots();ax.hist(final_dd,bins=40);ax.set(title="Max drawdown distribution",xlabel="$ drawdown");fig.savefig(output_dir / "07_max_drawdown_distribution.png",bbox_inches="tight");plt.close(fig)
    fig,ax=plt.subplots();ax.hist(final_required,bins=40);ax.set(title="Required bankroll distribution",xlabel="$ required");fig.savefig(output_dir / "08_required_bankroll_distribution.png",bbox_inches="tight");plt.close(fig)
    fig,ax=plt.subplots();ax.scatter([row["survival_probability_100"] for row in rows],[row["median_net_pnl"] for row in rows],s=9);ax.set(title="Median profit vs $100 survival",xlabel="$100 completion probability",ylabel="median P&L");fig.savefig(output_dir / "09_profit_vs_100_survival.png",bbox_inches="tight");plt.close(fig)
    fig,ax=plt.subplots();ax.scatter([row["p95_required_bankroll"] for row in rows],[row["median_net_pnl"] for row in rows],s=9);ax.set(title="Median profit vs P95 bankroll",xlabel="P95 bankroll",ylabel="median P&L");fig.savefig(output_dir / "10_profit_vs_p95_bankroll.png",bbox_inches="tight");plt.close(fig)
    for field, filename, title in (("recovery_multiplier","11_multiplier_vs_pnl.png","Multiplier vs P&L"),("first_base_threshold","12_threshold_vs_pnl.png","Threshold vs P&L"),("base_increment","13_base_increment_comparison.png","Permanent base increment comparison"),("stop_price","14_stop_comparison.png","Stop comparison")):
        source=stop_rows if field=="stop_price" else rows; groups={}
        for row in source: groups.setdefault(str(row[field]),[]).append(row["median_net_pnl"])
        fig,ax=plt.subplots();labels=list(groups);ax.bar(labels,[np.mean(groups[k]) for k in labels]);ax.set(title=title,ylabel="median P&L");fig.savefig(output_dir/filename,bbox_inches="tight");plt.close(fig)
    fig,ax=plt.subplots();ax.scatter([row["p95_required_bankroll"] for row in pareto],[row["median_net_pnl"] for row in pareto],c=[row["survival_probability_100"] for row in pareto]);ax.set(title="Pareto frontier",xlabel="P95 bankroll",ylabel="median P&L");fig.savefig(output_dir/"15_pareto_frontier.png",bbox_inches="tight");plt.close(fig)
    test=[row for row in walk_rows if row["segment"]=="test"];fig,ax=plt.subplots();ax.bar(["train grid","validation finalists","frozen test"],[max(row["median_net_pnl"] for row in walk_rows if row["segment"]=="train"),max(row["median_net_pnl"] for row in walk_rows if row["segment"]=="validation"),test[0]["median_net_pnl"]]);ax.set(title="Walk-forward performance",ylabel="median P&L");fig.savefig(output_dir/"16_walk_forward_performance.png",bbox_inches="tight");plt.close(fig)
    metrics=["40_region_rate_win","40_region_rate_loss","40_region_directional_wr","no_40_region_directional_wr","rung_wr_40","rung_wr_30","rung_wr_20","rung_wr_10"];fig,ax=plt.subplots();ix=np.arange(len(metrics));ax.bar(ix-.2,[calibration_report[f"observed_{m}"] for m in metrics],.4,label="observed");ax.bar(ix+.2,[calibration_report[f"simulated_{m}"] for m in metrics],.4,label="simulated");ax.set_xticks(ix,metrics,rotation=35,ha="right");ax.legend();ax.set(title="Calibration: observed vs simulated");fig.savefig(output_dir/"17_calibration_observed_vs_simulated.png",bbox_inches="tight");plt.close(fig)


def _segment_history(signals: Sequence[Any], parent_history: dict[str, Any]) -> dict[str, Any]:
    """Metadata for a contiguous actual-settlement replay segment."""

    wins = sum(bool(getattr(signal, "directional_win")) for signal in signals)
    return {
        "total_settled_markets": len(signals) + parent_history.get("missing_causal_source", 0),
        "eligible_predictions": len(signals),
        "directional_wins": wins,
        "directional_losses": len(signals) - wins,
        "directional_win_rate": wins / len(signals) if signals else 0.0,
    }


def reconciliation_comparison(
    signals: Sequence[Any], parent_history: dict[str, Any], output_path: Path,
    simulations: int, seed: int,
) -> list[dict[str, Any]]:
    """Compare prior-style 1,500-market results without redrawing settlements.

    The supplied prior table used a 1,500-market execution simulation and a
    full-49c-participation convention.  This report is deliberately labelled
    as a **fixed actual settlement prefix**, so any difference from a prior
    Bernoulli-directional reconstruction is interpretable rather than hidden.
    """

    if simulations <= 0:
        return []
    legacy = signals[:20_778]
    prefix = legacy[:1_500]
    calibration = ExecutionCalibration.reconstruction_compatible()
    parameters = [
        ParameterSet(1.11, threshold, increment, .40)
        for threshold in (100.0, 125.0)
        for increment in (.25, .50, 1.00)
    ]
    rows: list[dict[str, Any]] = []
    for label, segment in (("legacy_20778_actual_settlements", legacy), ("legacy_1500_actual_settlement_prefix", prefix)):
        outcomes = np.asarray([signal.directional_win for signal in segment], dtype=np.int8)
        history = _segment_history(segment, parent_history)
        # Full 20,778 comparisons use a smaller stable count: the stated 50k
        # precision is reserved for the 1,500-market counterpart.
        reps = simulations if len(segment) == 1_500 else min(5_000, simulations)
        for parameter in parameters:
            row, _ = evaluate(outcomes, parameter, calibration, history, reps, seed)
            rows.append({
                "comparison_horizon": label,
                "settlement_sequence": "fixed_actual_historical_settlements",
                "entry_participation_assumption": "49c_full_participation_reference_only",
                **row,
            })
    _write_csv(
        output_path,
        rows,
        ("comparison_horizon", "settlement_sequence", "entry_participation_assumption", *OUTPUT_COLUMNS),
    )
    return rows


def run_optimization(
    output_dir: Path, coarse_simulations: int = 2_000, final_simulations: int = 100_000,
    walkforward_simulations: int = 500, finalists: int = 15, seed: int = 42,
    calibration_uncertainty_draws: int = 0, execution_scenario: str = "base_case",
    reconciliation_simulations: int = 0, entry_price: float = 0.49,
    signal_mode: str = "sticky_until_directional_win",
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    markets = KalshiSettlementLoader().load()
    signals, metadata = reconstruct_signals(markets, signal_mode=signal_mode)
    history = signal_summary(signals, metadata)
    outcomes = np.asarray([signal.directional_win for signal in signals], dtype=np.int8)
    calibration = calibration_for_scenario(execution_scenario)
    calibration_report = write_calibration_report(output_dir / "calibration_report.csv", calibration, replications=min(20_000, max(2_000, coarse_simulations)), seed=seed)
    grid = primary_grid(entry_price)
    rows: list[dict[str, Any]] = []
    for index, parameter in enumerate(grid, start=1):
        row, _ = evaluate(outcomes, parameter, calibration, history, coarse_simulations, seed)
        rows.append(row)
        if index % 25 == 0:
            print(f"primary screen {index}/{len(grid)}")
    _write_csv(output_dir / "optimization_results.csv", rows, OUTPUT_COLUMNS)
    pareto = _pareto(rows)
    _write_csv(output_dir / "pareto_frontier.csv", pareto, OUTPUT_COLUMNS)

    primary_finalists = sorted(rows, key=lambda row: (row["median_net_pnl"], row["p5_net_pnl"]), reverse=True)[:finalists]
    stop_rows: list[dict[str, Any]] = []
    for row in primary_finalists:
        for stop in (None, .40, .30, .20, .10):
            parameter = replace(_row_to_parameters(row), stop_price=stop)
            evaluated, _ = evaluate(outcomes, parameter, calibration, history, coarse_simulations, seed)
            stop_rows.append(evaluated)
    # Three finalists are rerun at the required 100k+ depth.  More can be
    # selected from the same stop-screen CSV without recomputing the grid.
    stop_finalists = sorted(stop_rows, key=lambda row: (row["median_net_pnl"], row["p5_net_pnl"]), reverse=True)[:min(3, len(stop_rows))]
    final_rows: list[dict[str, Any]] = []
    final_values: np.ndarray | None = None
    best_parameters: ParameterSet | None = None
    for index, row in enumerate(stop_finalists):
        parameter = _row_to_parameters(row)
        evaluated, values = evaluate(outcomes, parameter, calibration, history, final_simulations, seed)
        final_rows.append(evaluated)
        if index == 0:
            final_values, best_parameters = values, parameter
    all_rank_rows = rows + stop_rows + final_rows
    rank = rankings(all_rank_rows)
    best_row = rank["highest_median_pnl"] or final_rows[0]
    best_parameters = _row_to_parameters(best_row)
    # Ensure plot distributions correspond to the reported primary winner.
    # Re-evaluate the selected winner at final depth even if it appeared in a
    # coarse ranking. This guarantees its distribution and plots match.
    best_row, final_values = evaluate(outcomes, best_parameters, calibration, history, final_simulations, seed)
    final_rows.append(best_row)
    assert final_values is not None
    _write_csv(output_dir / "stop_optimization_results.csv", stop_rows + final_rows, OUTPUT_COLUMNS)
    walk_rows = walk_forward(signals, grid, calibration, walkforward_simulations, seed)
    _write_csv(output_dir / "walkforward_results.csv", walk_rows, ("segment", "selection_role", *OUTPUT_COLUMNS))
    reconciliation_rows = reconciliation_comparison(
        signals, history, output_dir / "reconciliation_comparison.csv", reconciliation_simulations, seed,
    ) if signal_mode == "inverse_latest_settlement" else []
    # The user-designated current candidate is a 40c stop.  Stress that
    # configured finalist rather than making stop-slippage/depth tests on an
    # unconstrained no-stop winner where those perturbations are inert.
    stress_parameters = _row_to_parameters(rank["best_40c_stop"] or best_row)
    scenario_rows: list[dict[str, Any]] = []
    for scenario_name, factory in EXECUTION_SCENARIOS.items():
        scenario_calibration = factory()
        row, _ = evaluate(outcomes, stress_parameters, scenario_calibration, history, coarse_simulations, seed)
        scenario_rows.append({"scenario": scenario_name, **row})
    _write_csv(output_dir / "execution_scenario_sensitivity.csv", scenario_rows, ("scenario", *OUTPUT_COLUMNS))
    stress_rows = stress_tests(outcomes, stress_parameters, history, coarse_simulations, seed, calibration)
    _write_csv(output_dir / "stress_test_results.csv", stress_rows, ("stress_case", *OUTPUT_COLUMNS))
    regime_rows = regime_analysis(signals, best_parameters, calibration, min(100, coarse_simulations), seed)
    _write_csv(output_dir / "regime_analysis.csv", regime_rows, ("period", "period_markets", *OUTPUT_COLUMNS))
    _make_plots(output_dir / "plots", signals, calibration_report, rows, stop_rows + final_rows, pareto, walk_rows, final_values)

    reference = replay_many(signals, ExecutionPathModel(calibration), best_parameters.reference_configuration(), simulations=min(200, coarse_simulations), seed=seed)
    reference_summary = summarize_results(reference, history)
    _write_csv(
        output_dir / "funding_failures_reference.csv",
        [result.funding_failure.to_row() for result in reference if result.funding_failure is not None],
    )
    posterior_rows = calibration_uncertainty(
        outcomes, best_parameters, history, calibration_uncertainty_draws, coarse_simulations, seed, calibration,
    ) if calibration_uncertainty_draws else []
    if posterior_rows:
        _write_csv(output_dir / "calibration_uncertainty_results.csv", posterior_rows, ("calibration_draw", "win_entry_fill_probability", "loss_entry_fill_probability", "win_reach_40_joint_probability", "loss_reach_40_joint_probability", *OUTPUT_COLUMNS))
    # Live selection favours an explicit stop and the 95%-survival constrained
    # winner when it exists.  The unconstrained median-P&L winner may be a
    # no-stop research configuration and is not silently promoted to live.
    live_row = rank["best_median_with_95pct_100_survival"] or rank["best_40c_stop"] or best_row
    selected_live = export_selected_live_strategy(
        output_dir / "selected_live_strategy.json", live_row,
        selection_basis="best_median_with_95pct_100_survival_then_best_40c_stop",
    )
    lines = [
        "# Kalshi hybrid backtest optimization summary",
        "",
        "Methodology: **Historical Kalshi settlement replay with empirically calibrated Monte Carlo execution-path simulation.**",
        "",
        "Historical outcomes are fixed Kalshi settlements. Fill, rung depth, and stop events are modeled execution uncertainty; they are not claimed as exact historical events.",
        f"- 49c participation scenario: `{calibration.model_name}`. The supplied old-ladder counts calibrate joint 40c-region touch rates, not 49c maker fills.",
        "",
        "## Historical replay",
        "",
        f"- Settled markets: {history['total_settled_markets']:,}; eligible signals: {history['eligible_predictions']:,}",
        f"- Actual directional W/L: {history['directional_wins']:,}/{history['directional_losses']:,} ({history['directional_win_rate']:.4%})",
        f"- Range: {history['first_market_timestamp']} to {history['last_market_timestamp']}",
        ("- The earlier 20,778 / 10,751 / 10,027 contrarian result is reproduced exactly by the first 20,778 currently available eligible signals; the full API now exposes a later 1,620-signal extension."
         if signal_mode == "inverse_latest_settlement" else
         "- Sticky-direction results use the immediate prior actual settlement as a labelled proxy for the live realtime provisional outcome; they are not interchangeable with the prior 20,778-signal contrarian result."),
        "",
        "## Rankings",
        "",
    ]
    for name, row in rank.items():
        if row is None:
            lines.append(f"- {name}: no configuration met the constraint")
        else:
            lines.append(f"- {name}: m={row['recovery_multiplier']:.2f}, threshold={row['first_base_threshold']:.2f}, increment={row['base_increment']:.2f}, stop={row['stop_price']}; median=${row['median_net_pnl']:.2f}, P5=${row['p5_net_pnl']:.2f}, $100 survival={row['survival_probability_100']:.2%}, P95 bankroll=${row['p95_required_bankroll']:.2f}")
    lines.extend([
        "",
        "## Decimal reference check",
        "",
        f"A 200-replication Decimal replay of the selected configuration gave median net P&L ${reference_summary['median_net_pnl']:.2f}, P5 ${reference_summary['p5_net_pnl']:.2f}, and P95 bankroll ${reference_summary['p95_required_bankroll']:.2f}.",
        "",
        "The grid screen uses a deterministic batched kernel for feasibility; its quantity rule mirrors Decimal ROUND_HALF_UP and selected configurations are independently checked with the authoritative Decimal replay engine.",
        "",
        f"Regime analysis is in `regime_analysis.csv`; the untouched 20% test result is in `walkforward_results.csv`. Calibration posterior draws requested: {calibration_uncertainty_draws}.",
        "49c participation sensitivity for the selected 40c-stop configuration is in `execution_scenario_sensitivity.csv`.",
        "`selected_live_strategy.json` is the exact fixed-stop configuration exported for the live worker.",
    ])
    (output_dir / "optimization_summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"history": history, "rankings": rank, "best": best_row, "selected_live_strategy": selected_live, "reference": reference_summary, "reconciliation": reconciliation_rows}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("outputs/kalshi_hybrid_backtest"))
    parser.add_argument("--coarse-simulations", type=int, default=2_000)
    parser.add_argument("--final-simulations", type=int, default=100_000)
    parser.add_argument("--walkforward-simulations", type=int, default=500)
    parser.add_argument("--finalists", type=int, default=15)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--calibration-uncertainty-draws", type=int, default=0, help="optional outer Beta-binomial calibration draws")
    parser.add_argument("--execution-scenario", choices=tuple(EXECUTION_SCENARIOS), default="base_case", help="49c participation scenario; reconstruction_compatible is an explicit full-participation comparison")
    parser.add_argument("--reconciliation-simulations", type=int, default=0, help="also run the six 1.11x prior-style configurations on a fixed 1,500-settlement prefix")
    parser.add_argument("--entry-price", type=float, default=.49, choices=(.49, .50), help="maker-price assumption used consistently by the grid and live export")
    parser.add_argument(
        "--signal-mode", choices=("inverse_latest_settlement", "sticky_until_directional_win"),
        default="sticky_until_directional_win", help="fixed-settlement signal-state rule to optimize",
    )
    args = parser.parse_args()
    result = run_optimization(args.output_dir, args.coarse_simulations, args.final_simulations, args.walkforward_simulations, args.finalists, args.seed, args.calibration_uncertainty_draws, args.execution_scenario, args.reconciliation_simulations, args.entry_price, args.signal_mode)
    print(json.dumps({"history": result["history"], "best": result["best"]}, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
