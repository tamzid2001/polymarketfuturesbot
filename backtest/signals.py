"""State transitions whose signals deliberately apply to the next trade."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class SignalRule:
    name: str
    entry_quantile: str
    exit_quantile: str
    direction: str = "normal"  # normal: on at/below entry; inverse: on at/above entry
    exploratory: bool = False


PRIMARY_RULE = SignalRule("p10_p90", "p10", "p90", "normal", False)
EXPLORATORY_RULES = (
    SignalRule("inverse_p90_p10", "p90", "p10", "inverse", True),
    SignalRule("p10_p50", "p10", "p50", "normal", True),
    SignalRule("p25_p75", "p25", "p75", "normal", True),
)


def _crossing(
    previous_equity: float | None,
    previous_band: float | None,
    current_equity: float,
    current_band: float,
    direction: str,
) -> bool:
    if any(value is None or not np.isfinite(value) for value in (previous_equity, previous_band)):
        return False
    if direction == "down":
        return bool(previous_equity > previous_band and current_equity <= current_band)
    return bool(previous_equity < previous_band and current_equity >= current_band)


def signal_after_trade(
    *,
    active_before: bool,
    current_equity: float,
    current: pd.Series,
    previous_equity: float | None,
    previous: pd.Series | None,
    rule: SignalRule,
    signal_mode: str,
) -> tuple[bool, bool]:
    """Return entry/exit signals observed only after a trade has closed."""

    entry_band = current.get(rule.entry_quantile, np.nan)
    exit_band = current.get(rule.exit_quantile, np.nan)
    if not np.isfinite(entry_band) or not np.isfinite(exit_band):
        return False, False
    previous_entry = previous.get(rule.entry_quantile, np.nan) if previous is not None else np.nan
    previous_exit = previous.get(rule.exit_quantile, np.nan) if previous is not None else np.nan

    if rule.direction == "normal":
        raw_entry = current_equity <= entry_band
        raw_exit = current_equity >= exit_band
        cross_entry = _crossing(previous_equity, previous_entry, current_equity, entry_band, "down")
        cross_exit = _crossing(previous_equity, previous_exit, current_equity, exit_band, "up")
    elif rule.direction == "inverse":
        raw_entry = current_equity >= entry_band
        raw_exit = current_equity <= exit_band
        cross_entry = _crossing(previous_equity, previous_entry, current_equity, entry_band, "up")
        cross_exit = _crossing(previous_equity, previous_exit, current_equity, exit_band, "down")
    else:
        raise ValueError(f"Unknown rule direction: {rule.direction}")

    use_crossing = signal_mode == "crossing"
    return (not active_before and (cross_entry if use_crossing else raw_entry),
            active_before and (cross_exit if use_crossing else raw_exit))


def apply_rule(
    forecast_log: pd.DataFrame,
    rule: SignalRule,
    signal_mode: str,
    initial_state: str,
    evaluation_start_index: int,
) -> pd.DataFrame:
    """Replay a rule with current state governing *this* opportunity.

    ``entry_signal_after_trade`` and ``exit_signal_after_trade`` are calculated
    after row ``t`` is known.  The resulting state is written to row ``t`` as
    ``state_after_trade`` and is only used as ``state_before_trade`` on row
    ``t+1``.
    """

    output = forecast_log.copy()
    active = initial_state == "on"
    before: list[str] = []
    after: list[str] = []
    active_for_trade: list[bool] = []
    entries: list[bool] = []
    exits: list[bool] = []
    previous_equity: float | None = None
    previous_forecast: pd.Series | None = None

    for position, (_, row) in enumerate(output.iterrows()):
        before.append("on" if active else "off")
        active_for_trade.append(active)
        is_evaluation = position >= evaluation_start_index
        entry = exit_ = False
        if is_evaluation:
            entry, exit_ = signal_after_trade(
                active_before=active,
                current_equity=float(row["shadow_equity_after"]),
                current=row,
                previous_equity=previous_equity,
                previous=previous_forecast,
                rule=rule,
                signal_mode=signal_mode,
            )
            if entry:
                active = True
            elif exit_:
                active = False
        entries.append(entry)
        exits.append(exit_)
        after.append("on" if active else "off")
        previous_equity = float(row["shadow_equity_after"])
        previous_forecast = row

    output["state_before_trade"] = before
    output["bot_active_for_trade"] = active_for_trade
    output["entry_signal_after_trade"] = entries
    output["exit_signal_after_trade"] = exits
    output["state_after_trade"] = after
    output["selected_trade_pnl"] = np.where(output["bot_active_for_trade"], output["trade_pnl"], 0.0)
    output["skipped_trade_pnl"] = np.where(~output["bot_active_for_trade"], output["trade_pnl"], 0.0)
    output["filtered_equity_after"] = output["filtered_equity_before"].iloc[0] + output["selected_trade_pnl"].cumsum()
    output["filtered_equity_before"] = output["filtered_equity_after"].shift(1).fillna(output["filtered_equity_before"].iloc[0])
    output["strategy"] = rule.name
    output["signal_mode"] = signal_mode
    return output
