"""Counterfactual replay of the archived v10 40c shadow ledger.

This is an empirical shadow-ledger replay, not an exact historical execution
backtest.  Settlement outcomes, recorded first-minute books, and old 40c stop
events are fixed.  A one-cent-lower maker fill and a 45c stop are inferred only
where the retained evidence supports them; optional Monte Carlo samples the
late path that the old ledger did not retain.
"""

from __future__ import annotations

import argparse
import csv
import json
import random
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any, Iterable

from strategy_core import (
    StrategyParameters,
    apply_realized_filled_trade,
    prescribed_quantity,
    sizing_state,
    zero_fill_snapshot,
)


ZERO = Decimal("0")
ONE = Decimal("1")
REPORT_QUANTUM = Decimal("0.0001")


def decimal(value: Any) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


def price_cents(value: Any) -> int:
    cents = decimal(value) * Decimal("100")
    if cents != cents.to_integral_value():
        raise ValueError(f"price is not an integer-cent tick: {value!r}")
    return int(cents)


def quote_epoch(observation: dict[str, Any]) -> float | None:
    raw_ms = observation.get("source_timestamp_ms")
    if raw_ms is not None:
        try:
            return float(raw_ms) / 1000.0
        except (TypeError, ValueError):
            pass
    raw = observation.get("source_server_timestamp") or observation.get("received_at")
    if raw:
        try:
            return datetime.fromisoformat(str(raw).replace("Z", "+00:00")).timestamp()
        except ValueError:
            pass
    try:
        return float(observation["captured_epoch"])
    except (KeyError, TypeError, ValueError):
        return None


@dataclass(frozen=True)
class LedgerTrade:
    ticker: str
    market_open_epoch: float
    signal_side: str
    outcome: str
    initial_ask_cents: int
    entry_limit_cents: int
    old_reached_40: bool
    eventual_winner: bool
    observed_limit_touch_60s: bool
    observed_strict_trade_through_60s: bool
    observed_stop_45_after_touch_60s: bool
    lowest_recorded_bid_60s_cents: int


@dataclass(frozen=True)
class ReplayMetrics:
    sizing_profile: str
    scenario: str
    pnl: Decimal
    final_balance: Decimal
    return_percent: Decimal
    max_drawdown: Decimal
    minimum_required_bankroll: Decimal
    eligible_signals: int
    stop_ineligible_signals: int
    fills: int
    zero_fills: int
    filled_directional_winners: int
    filled_directional_losers: int
    stopped_trades: int
    stopped_eventual_winners: int
    settlement_winners: int
    settlement_losers: int
    highest_recovery_exponent: int
    maximum_quantity: Decimal
    final_recovery_exponent: int
    final_recovery_cycle_pnl: Decimal
    final_base: Decimal
    funding_failure: bool

    def json_row(self) -> dict[str, Any]:
        return {
            key: format(value, "f") if isinstance(value, Decimal) else value
            for key, value in asdict(self).items()
        }


def _outcome(record: dict[str, Any]) -> str | None:
    value = record.get("settlement_outcome") or record.get("post_stop_settlement_outcome")
    return str(value).lower() if value in {"yes", "no"} else None


def load_ledger_trades(path: Path, entry_offset_cents: int = 1) -> list[LedgerTrade]:
    state = json.loads(path.read_text(encoding="utf-8"))
    result: list[LedgerTrade] = []
    for ticker, record in state.get("markets", {}).items():
        if decimal(record.get("actual_quantity") or "0") <= ZERO:
            continue
        outcome = _outcome(record)
        side = str(record.get("signal_side") or "").lower()
        if outcome not in {"yes", "no"} or side not in {"yes", "no"}:
            continue
        market_open = float(record["market_open_epoch"])
        observations: list[tuple[float, dict[str, Any]]] = []
        for observation in record.get("opening_quote_observations") or []:
            observed_epoch = quote_epoch(observation)
            if observed_epoch is None or observed_epoch < market_open:
                continue
            if str(observation.get("selected_side") or side).lower() != side:
                continue
            try:
                price_cents(observation["selected_best_ask"])
                price_cents(observation["selected_best_bid"])
            except (KeyError, ValueError):
                continue
            observations.append((observed_epoch, observation))
        observations.sort(key=lambda item: item[0])
        if not observations:
            continue
        first_epoch, first = observations[0]
        initial_ask = price_cents(first["selected_best_ask"])
        limit = initial_ask - entry_offset_cents
        touch_epoch: float | None = None
        strict_touch = False
        lowest_bid = price_cents(first["selected_best_bid"])
        for observed_epoch, observation in observations[1:]:
            ask = price_cents(observation["selected_best_ask"])
            bid = price_cents(observation["selected_best_bid"])
            lowest_bid = min(lowest_bid, bid)
            if touch_epoch is None and ask <= limit:
                touch_epoch = observed_epoch
            if ask < limit:
                strict_touch = True
        stop_after_touch = bool(
            touch_epoch is not None
            and any(
                observed_epoch >= touch_epoch
                and price_cents(observation["selected_best_bid"]) <= 45
                for observed_epoch, observation in observations[1:]
            )
        )
        result.append(LedgerTrade(
            ticker=str(ticker), market_open_epoch=market_open, signal_side=side, outcome=outcome,
            initial_ask_cents=initial_ask, entry_limit_cents=limit,
            old_reached_40=record.get("realized_method") == "stop",
            eventual_winner=outcome == side,
            observed_limit_touch_60s=touch_epoch is not None,
            observed_strict_trade_through_60s=strict_touch,
            observed_stop_45_after_touch_60s=stop_after_touch,
            lowest_recorded_bid_60s_cents=lowest_bid,
        ))
    return sorted(result, key=lambda trade: (trade.market_open_epoch, trade.ticker))


def archived_ledger_reconciliation(
    path: Path, *, stop_cents: int = 45, entry_offset_cents: int = 1,
) -> dict[str, Any]:
    """Reconcile the old realized ledger and two fixed-share price proxies.

    The old IOC entry and ``old IOC entry - offset`` views are arithmetic
    bounds, not maker-fill reconstructions. They deliberately preserve the old
    stop/settlement classification so the archived positive P&L can be compared
    with the newer signal-time-ask execution model without conflating them.
    """
    state = json.loads(path.read_text(encoding="utf-8"))
    completed: list[dict[str, Any]] = []
    for record in state.get("markets", {}).values():
        if decimal(record.get("actual_quantity") or "0") <= ZERO:
            continue
        outcome = _outcome(record)
        side = str(record.get("signal_side") or "").lower()
        if outcome not in {"yes", "no"} or side not in {"yes", "no"}:
            continue
        entry_cents = price_cents(record["actual_average_entry_price"])
        fill_epoch = float(
            (record.get("entry_timing") or {}).get("first_fill_observed_epoch") or 0.0
        )
        observed_stop_after_fill = any(
            (quote_epoch(observation) or -1.0) >= fill_epoch
            and price_cents(observation["selected_best_bid"]) <= stop_cents
            for observation in (record.get("opening_quote_observations") or [])
            if observation.get("selected_best_bid") is not None
        )
        completed.append({
            "entry_cents": entry_cents,
            "eventual_winner": outcome == side,
            "old_stopped": record.get("realized_method") == "stop",
            "realized_positive": decimal(record.get("realized_net_pnl") or "0") > ZERO,
            "realized_pnl": decimal(record.get("realized_net_pnl") or "0"),
            "observed_stop_after_fill": observed_stop_after_fill,
        })

    def fixed_share_proxy(offset_cents: int) -> dict[str, Any]:
        eligible = [
            (record, record["entry_cents"] - offset_cents)
            for record in completed
            if record["entry_cents"] - offset_cents > stop_cents
        ]
        optimistic_pnl = ZERO
        observed_pnl = ZERO
        for record, entry_cents in eligible:
            settlement_pnl = Decimal(100 - entry_cents) / Decimal(100)
            stop_pnl = Decimal(stop_cents - entry_cents) / Decimal(100)
            optimistic_pnl += stop_pnl if record["old_stopped"] else settlement_pnl
            observed_pnl += (
                stop_pnl
                if record["old_stopped"] or record["observed_stop_after_fill"]
                else settlement_pnl
            )
        old_stops = [record for record, _ in eligible if record["old_stopped"]]
        return {
            "eligible": len(eligible),
            "eventual_winners": sum(record["eventual_winner"] for record, _ in eligible),
            "eventual_losers": sum(not record["eventual_winner"] for record, _ in eligible),
            "old_profitable_settlements": sum(
                record["realized_positive"] for record, _ in eligible
            ),
            "old_stops": len(old_stops),
            "old_stopped_eventual_winners": sum(
                record["eventual_winner"] for record in old_stops
            ),
            "old_stopped_eventual_losers": sum(
                not record["eventual_winner"] for record in old_stops
            ),
            "additional_profitable_winners_with_observed_post_fill_bid_at_or_below_stop": sum(
                not record["old_stopped"] and record["observed_stop_after_fill"]
                for record, _ in eligible
            ),
            "optimistic_fixed_one_share_gross_pnl": format(optimistic_pnl, "f"),
            "observed_first_minimum_fixed_one_share_gross_pnl": format(observed_pnl, "f"),
            "optimistic_fixed_one_share_ev_per_eligible": report_decimal(
                optimistic_pnl / Decimal(len(eligible)),
            ),
            "observed_first_minimum_fixed_one_share_ev_per_eligible": report_decimal(
                observed_pnl / Decimal(len(eligible)),
            ),
        }

    realized = sum((record["realized_pnl"] for record in completed), ZERO)
    return {
        "completed_fills": len(completed),
        "realized_positive_trades": sum(record["realized_positive"] for record in completed),
        "realized_nonpositive_trades": sum(not record["realized_positive"] for record in completed),
        "eventual_directional_winners": sum(record["eventual_winner"] for record in completed),
        "eventual_directional_losers": sum(not record["eventual_winner"] for record in completed),
        "old_stops": sum(record["old_stopped"] for record in completed),
        "old_settlements": sum(not record["old_stopped"] for record in completed),
        "archived_realized_pnl": format(realized, "f"),
        "archived_final_balance": format(decimal(state["shadow_metrics"]["balance"]), "f"),
        "old_actual_entry_proxy": fixed_share_proxy(0),
        "old_actual_entry_minus_offset_proxy": fixed_share_proxy(entry_offset_cents),
    }


def empirical_calibration(trades: Iterable[LedgerTrade], stop_cents: int = 45) -> dict[str, Any]:
    rows = list(trades)
    eligible = [trade for trade in rows if trade.entry_limit_cents > stop_cents]
    survivors = [trade for trade in eligible if trade.eventual_winner and not trade.old_reached_40]
    touches = [trade for trade in survivors if trade.observed_limit_touch_60s]
    stop_touches = [trade for trade in touches if trade.observed_stop_45_after_touch_60s]
    return {
        "markets": len(rows),
        "eligible": len(eligible),
        "stop_ineligible": len(rows) - len(eligible),
        "eligible_winners": sum(trade.eventual_winner for trade in eligible),
        "eligible_losers": sum(not trade.eventual_winner for trade in eligible),
        "known_40_reaches": sum(trade.old_reached_40 for trade in eligible),
        "known_40_reach_winners": sum(trade.old_reached_40 and trade.eventual_winner for trade in eligible),
        "known_40_reach_losers": sum(trade.old_reached_40 and not trade.eventual_winner for trade in eligible),
        "eligible_winner_survivors": len(survivors),
        "winner_survivor_limit_touches_60s": len(touches),
        "winner_survivor_stop45_after_touch_60s": len(stop_touches),
        "observed_winner_survivor_fill_rate": len(touches) / len(survivors) if survivors else 0.0,
        "observed_stop45_given_touch_rate": len(stop_touches) / len(touches) if touches else 0.0,
    }


def _execution(
    trade: LedgerTrade,
    scenario: str,
    stop_cents: int,
    rng: random.Random | None,
    late_fill_probability: float,
    late_stop_probability: float,
) -> tuple[bool, bool, str]:
    if trade.entry_limit_cents <= stop_cents:
        return False, False, "initial_limit_at_or_below_stop"
    # Reaching the old 40c stop necessarily passed a resting limit above 45c
    # and the new 45c trigger. Queue priority remains unknowable, so this is
    # an execution opportunity, not a claim of an exchange fill.
    if trade.old_reached_40:
        return True, True, "known_old_40_path"
    if scenario == "strict_60s_trade_through":
        filled = trade.observed_strict_trade_through_60s
    elif scenario == "observed_60s_quote_touch":
        filled = trade.observed_limit_touch_60s
    elif scenario == "all_eligible_fill":
        filled = True
    elif scenario == "empirical_late_path_mc":
        assert rng is not None
        filled = trade.observed_limit_touch_60s or rng.random() < late_fill_probability
    else:
        raise ValueError(f"unknown scenario: {scenario}")
    if not filled:
        return False, False, "no_retained_fill_evidence"
    stopped = trade.observed_stop_45_after_touch_60s
    if scenario == "empirical_late_path_mc" and not stopped:
        assert rng is not None
        stopped = rng.random() < late_stop_probability
    return True, stopped, "retained_or_sampled_winner_path"


def replay(
    trades: Iterable[LedgerTrade],
    *, scenario: str,
    parameters: StrategyParameters,
    starting_bankroll: Decimal = Decimal("1000.00"),
    stop_cents: int = 45,
    stop_slippage_cents: int = 0,
    fee_per_filled_share: Decimal = ZERO,
    sizing_profile: str = "recovery_1.01x",
    seed: int = 42,
    late_fill_probability: float = 0.0,
    late_stop_probability: float = 0.0,
) -> ReplayMetrics:
    rng = random.Random(seed) if scenario == "empirical_late_path_mc" else None
    snapshot: dict[str, Any] = {}
    running = ZERO
    peak = ZERO
    max_drawdown = ZERO
    minimum_required = ZERO
    highest_exponent = 0
    maximum_quantity = ZERO
    funding_failure = False
    counts = {
        "eligible": 0, "ineligible": 0, "fills": 0, "zero": 0,
        "filled_wins": 0, "filled_losses": 0, "stops": 0,
        "false_stops": 0, "settlement_wins": 0, "settlement_losses": 0,
    }
    for trade in trades:
        if trade.entry_limit_cents <= stop_cents:
            counts["ineligible"] += 1
            snapshot = zero_fill_snapshot(parameters, snapshot)
            continue
        counts["eligible"] += 1
        filled, stopped, _ = _execution(
            trade, scenario, stop_cents, rng, late_fill_probability, late_stop_probability,
        )
        if not filled:
            counts["zero"] += 1
            snapshot = zero_fill_snapshot(parameters, snapshot)
            continue
        current = sizing_state(parameters, snapshot)
        highest_exponent = max(highest_exponent, current.recovery_exponent)
        quantity, _ = prescribed_quantity(parameters, snapshot)
        maximum_quantity = max(maximum_quantity, quantity)
        entry = Decimal(trade.entry_limit_cents) / Decimal("100")
        required_cash = quantity * entry
        minimum_required = max(minimum_required, required_cash - running)
        if starting_bankroll + running < required_cash:
            funding_failure = True
        if stopped:
            exit_price = Decimal(stop_cents - stop_slippage_cents) / Decimal("100")
            pnl_per_share = exit_price - entry - fee_per_filled_share
            counts["stops"] += 1
            counts["false_stops"] += int(trade.eventual_winner)
        elif trade.eventual_winner:
            pnl_per_share = ONE - entry - fee_per_filled_share
            counts["settlement_wins"] += 1
        else:
            pnl_per_share = -entry - fee_per_filled_share
            counts["settlement_losses"] += 1
        trade_pnl = quantity * pnl_per_share
        running += trade_pnl
        peak = max(peak, running)
        max_drawdown = max(max_drawdown, peak - running)
        counts["fills"] += 1
        counts["filled_wins" if trade.eventual_winner else "filled_losses"] += 1
        snapshot, _ = apply_realized_filled_trade(parameters, snapshot, trade_pnl)
    final = sizing_state(parameters, snapshot)
    return ReplayMetrics(
        sizing_profile=sizing_profile, scenario=scenario, pnl=running,
        final_balance=starting_bankroll + running,
        return_percent=(running / starting_bankroll * Decimal("100")), max_drawdown=max_drawdown,
        minimum_required_bankroll=max(ZERO, minimum_required), eligible_signals=counts["eligible"],
        stop_ineligible_signals=counts["ineligible"], fills=counts["fills"], zero_fills=counts["zero"],
        filled_directional_winners=counts["filled_wins"], filled_directional_losers=counts["filled_losses"],
        stopped_trades=counts["stops"], stopped_eventual_winners=counts["false_stops"],
        settlement_winners=counts["settlement_wins"], settlement_losers=counts["settlement_losses"],
        highest_recovery_exponent=highest_exponent, maximum_quantity=maximum_quantity,
        final_recovery_exponent=final.recovery_exponent, final_recovery_cycle_pnl=final.recovery_cycle_pnl,
        final_base=final.base_share_count, funding_failure=funding_failure,
    )


def percentile(values: list[Decimal], quantile: float) -> Decimal:
    ordered = sorted(values)
    if not ordered:
        return ZERO
    position = (len(ordered) - 1) * quantile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    weight = Decimal(str(position - lower))
    return ordered[lower] * (ONE - weight) + ordered[upper] * weight


def report_decimal(value: Decimal) -> str:
    """Render simulation output deterministically without interpolation noise."""
    return format(value.quantize(REPORT_QUANTUM, rounding=ROUND_HALF_UP), "f")


def monte_carlo_summary(results: list[ReplayMetrics]) -> dict[str, Any]:
    pnls = [row.pnl for row in results]
    drawdowns = [row.max_drawdown for row in results]
    bankroll = [row.minimum_required_bankroll for row in results]
    returns = [row.return_percent for row in results]
    return {
        "simulations": len(results),
        "mean_pnl": report_decimal(sum(pnls, ZERO) / Decimal(len(pnls))),
        "p5_pnl": report_decimal(percentile(pnls, 0.05)),
        "median_pnl": report_decimal(percentile(pnls, 0.50)),
        "p95_pnl": report_decimal(percentile(pnls, 0.95)),
        "mean_return_percent": report_decimal(sum(returns, ZERO) / Decimal(len(returns))),
        "p5_return_percent": report_decimal(percentile(returns, 0.05)),
        "median_return_percent": report_decimal(percentile(returns, 0.50)),
        "p95_return_percent": report_decimal(percentile(returns, 0.95)),
        "median_max_drawdown": report_decimal(percentile(drawdowns, 0.50)),
        "p95_max_drawdown": report_decimal(percentile(drawdowns, 0.95)),
        "p50_required_bankroll": report_decimal(percentile(bankroll, 0.50)),
        "p95_required_bankroll": report_decimal(percentile(bankroll, 0.95)),
        "funding_failure_probability_1000": sum(row.funding_failure for row in results) / len(results),
    }


def write_outputs(
    output_dir: Path,
    trades: list[LedgerTrade],
    reconciliation: dict[str, Any],
    calibration: dict[str, Any],
    deterministic: list[ReplayMetrics],
    mc_summaries: dict[str, dict[str, Any]],
    arguments: argparse.Namespace,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    with (output_dir / "market_evidence.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=list(asdict(trades[0]).keys()), lineterminator="\n",
        )
        writer.writeheader()
        for trade in trades:
            writer.writerow(asdict(trade))
    rows = [row.json_row() for row in deterministic]
    with (output_dir / "scenario_results.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=list(rows[0].keys()), lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)
    payload = {
        "methodology": "fixed shadow-ledger settlement replay with observed/inferred execution and Monte Carlo late-path sensitivity",
        "source": str(arguments.state_file),
        "entry_offset_cents": arguments.entry_offset_cents,
        "stop_cents": arguments.stop_cents,
        "stop_slippage_cents": arguments.stop_slippage_cents,
        "fee_per_filled_share": arguments.fee_per_filled_share,
        "archived_ledger_reconciliation": reconciliation,
        "calibration": calibration,
        "deterministic_scenarios": rows,
        "monte_carlo": mc_summaries,
    }
    (output_dir / "summary.json").write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    report = [
        "# Archived-ledger 45c stop counterfactual",
        "",
        "This is not an exact historical fill/stop replay. Settlements and retained books are fixed; unobserved late execution is simulated.",
        "",
        f"- Archived realized P&L: ${reconciliation['archived_realized_pnl']} (final balance ${reconciliation['archived_final_balance']})",
        f"- Markets: {calibration['markets']}",
        f"- Eligible derived limits above {arguments.stop_cents}c: {calibration['eligible']}",
        f"- Rejected because derived limit was at/below stop: {calibration['stop_ineligible']}",
        f"- Winner-survivor first-minute one-cent-lower touches: {calibration['winner_survivor_limit_touches_60s']}/{calibration['eligible_winner_survivors']}",
        f"- Observed 45c stop after those touches: {calibration['winner_survivor_stop45_after_touch_60s']}/{calibration['winner_survivor_limit_touches_60s']}",
        "",
        "## Archived-fill arithmetic bounds",
        "",
        "These fixed-one-share bounds assume every old IOC fill, or every old IOC fill minus the configured offset, would participate. They do not model maker non-fills.",
        "",
        "| Price proxy | Eligible | Old settlements | Old stops | Additional observed winner stops | Optimistic P&L | First-minute-evidence P&L |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for label, key in (
        ("Old actual IOC entry", "old_actual_entry_proxy"),
        ("Old actual IOC entry minus offset", "old_actual_entry_minus_offset_proxy"),
    ):
        row = reconciliation[key]
        report.append(
            f"| {label} | {row['eligible']} | {row['old_profitable_settlements']} | {row['old_stops']} "
            f"| {row['additional_profitable_winners_with_observed_post_fill_bid_at_or_below_stop']} "
            f"| ${row['optimistic_fixed_one_share_gross_pnl']} | ${row['observed_first_minimum_fixed_one_share_gross_pnl']} |"
        )
    report.extend([
        "",
        "The optimistic column is the zero-additional-false-stop calculation. The first-minute-evidence column also stops old profitable settlements whose retained post-fill executable bid was already at or below the new stop. Complete later paths remain unavailable.",
        "",
        "| Sizing | Scenario | P&L | Final balance | Return | Max drawdown | Fills | Stops | False stops |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ])
    for row in deterministic:
        report.append(
            f"| {row.sizing_profile} | {row.scenario} | ${row.pnl:.4f} | ${row.final_balance:.4f} | {row.return_percent:.4f}% "
            f"| ${row.max_drawdown:.4f} | {row.fills} | {row.stopped_trades} | {row.stopped_eventual_winners} |"
        )
    report.extend([
        "",
        "## Empirical late-path Monte Carlo",
        "",
    ])
    for profile, mc_summary in mc_summaries.items():
        report.extend([
            f"### {profile}",
            "",
            f"- Simulations: {mc_summary['simulations']}",
            f"- Mean P&L: ${mc_summary['mean_pnl']}",
            f"- P5 / median / P95 P&L: ${mc_summary['p5_pnl']} / ${mc_summary['median_pnl']} / ${mc_summary['p95_pnl']}",
            f"- Mean return: {mc_summary['mean_return_percent']}%",
            f"- P5 / median / P95 return: {mc_summary['p5_return_percent']}% / {mc_summary['median_return_percent']}% / {mc_summary['p95_return_percent']}%",
            f"- Median / P95 max drawdown: ${mc_summary['median_max_drawdown']} / ${mc_summary['p95_max_drawdown']}",
            f"- P50 / P95 required bankroll: ${mc_summary['p50_required_bankroll']} / ${mc_summary['p95_required_bankroll']}",
            "",
        ])
    report.append(
        "The Monte Carlo probabilities are proxies derived from the retained first-minute winner-survivor books. They are sensitivity assumptions, not recovered later-market paths."
    )
    (output_dir / "report.md").write_text("\n".join(report) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-file", type=Path, default=Path("data/kalshi_shadow_market_ioc_v10_sticky_stop_40_state.json"))
    parser.add_argument("--output-dir", type=Path, default=Path("reports/kalshi_45c_counterfactual"))
    parser.add_argument("--entry-offset-cents", type=int, default=1)
    parser.add_argument("--stop-cents", type=int, default=45)
    parser.add_argument("--stop-slippage-cents", type=int, default=0)
    parser.add_argument("--fee-per-filled-share", default="0")
    parser.add_argument("--simulations", type=int, default=10_000)
    parser.add_argument("--seed", type=int, default=20260823)
    return parser.parse_args()


def main() -> int:
    arguments = parse_args()
    if arguments.entry_offset_cents < 0:
        raise SystemExit("entry offset must be non-negative")
    if not 1 <= arguments.stop_cents <= 98:
        raise SystemExit("stop must be an integer cent from 1 through 98")
    if arguments.stop_slippage_cents < 0 or arguments.stop_slippage_cents >= arguments.stop_cents:
        raise SystemExit("stop slippage must be non-negative and less than the stop")
    if arguments.simulations <= 0:
        raise SystemExit("simulations must be positive")
    trades = load_ledger_trades(arguments.state_file, arguments.entry_offset_cents)
    if not trades:
        raise SystemExit("no complete filled ledger records were found")
    calibration = empirical_calibration(trades, arguments.stop_cents)
    reconciliation = archived_ledger_reconciliation(
        arguments.state_file,
        stop_cents=arguments.stop_cents,
        entry_offset_cents=arguments.entry_offset_cents,
    )
    recovery_parameters = StrategyParameters(
        recovery_multiplier=Decimal("1.01"), first_base_threshold=Decimal("350.00"),
        threshold_growth_multiplier=Decimal("1.01"), base_increment=Decimal("0.50"),
        starting_base=Decimal("1.00"), max_position=Decimal("100.00"),
    )
    fixed_parameters = StrategyParameters(
        recovery_multiplier=Decimal("1.00"), first_base_threshold=Decimal("1000000.00"),
        threshold_growth_multiplier=Decimal("1.00"), base_increment=Decimal("0.01"),
        starting_base=Decimal("1.00"), max_position=Decimal("1.00"),
    )
    common_kwargs = {
        "starting_bankroll": Decimal("1000.00"),
        "stop_cents": arguments.stop_cents,
        "stop_slippage_cents": arguments.stop_slippage_cents,
        "fee_per_filled_share": decimal(arguments.fee_per_filled_share),
    }
    deterministic = []
    for profile, parameters in (
        ("recovery_1.01x", recovery_parameters),
        ("fixed_one_share", fixed_parameters),
    ):
        deterministic.extend(
            replay(
                trades, scenario=scenario, parameters=parameters, sizing_profile=profile,
                **common_kwargs,
            )
            for scenario in ("strict_60s_trade_through", "observed_60s_quote_touch", "all_eligible_fill")
        )
    late_fill = float(calibration["observed_winner_survivor_fill_rate"])
    late_stop = float(calibration["observed_stop45_given_touch_rate"])
    summaries: dict[str, dict[str, Any]] = {}
    for profile, parameters in (
        ("recovery_1.01x", recovery_parameters),
        ("fixed_one_share", fixed_parameters),
    ):
        simulations = [
            replay(
                trades, scenario="empirical_late_path_mc", seed=arguments.seed + simulation,
                late_fill_probability=late_fill, late_stop_probability=late_stop,
                parameters=parameters, sizing_profile=profile, **common_kwargs,
            )
            for simulation in range(arguments.simulations)
        ]
        summaries[profile] = monte_carlo_summary(simulations)
        summaries[profile].update({
            "late_fill_probability_proxy": late_fill,
            "late_stop_probability_proxy": late_stop,
        })
    write_outputs(
        arguments.output_dir, trades, reconciliation, calibration,
        deterministic, summaries, arguments,
    )
    print(json.dumps({
        "archived_ledger_reconciliation": reconciliation,
        "calibration": calibration,
        "deterministic": [row.json_row() for row in deterministic],
        "monte_carlo": summaries,
        "output_dir": str(arguments.output_dir),
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
