"""Backtest the P10/P90 controller using the user's Colab Prophet procedure.

This is intentionally a *batch fitted-band replay*, not a causal
walk-forward test.  It exists to reproduce and explore the exact workflow in
the supplied Colab notebook:

1. construct the absolute account-balance curve from closed positions;
2. fit one log-Prophet model to the complete selected balance window;
3. obtain historical P01..P99 bands by sampling the complete
   ``include_history=True`` Prophet frame; and
4. replay the P10-entry/P90-exit state machine over those historical bands.

The state transition remains correctly delayed: a comparison made after
market *t* changes the state for market *t + 1*.  ``model_window`` selects
rows from the absolute balance curve; it never rebases them.

Do not use this fitted-band replay as evidence of live out-of-sample edge.
Use :mod:`backtest.run_backtest` for a causal walk-forward evaluation.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from .notebook_reference import (
    build_colab_equity_curve,
    fit_colab_prophet,
    load_colab_equity_curve,
)


QUANTILE_NAMES = ("p01", "p10", "p25", "p50", "p75", "p90", "p99")
QUANTILE_TO_COLUMN = {int(name[1:]): name for name in QUANTILE_NAMES}


@dataclass(frozen=True)
class NotebookBatchVariant:
    """One fitted-band replay configuration."""

    label: str
    model_window: int | None
    changepoint_prior_scale: float
    seasonality_prior_scale: float
    entry_quantile: int = 10
    exit_quantile: int = 90

    @property
    def entry_column(self) -> str:
        return QUANTILE_TO_COLUMN[self.entry_quantile]

    @property
    def exit_column(self) -> str:
        return QUANTILE_TO_COLUMN[self.exit_quantile]


def _select_model_curve(full_curve: pd.DataFrame, model_window: int | None) -> pd.DataFrame:
    """Select an absolute-balance model window without changing any balance."""

    if model_window is None:
        return full_curve.copy().reset_index(drop=True)
    if model_window < 2:
        raise ValueError("model_window must be at least two balance observations")
    return full_curve.tail(model_window).copy().reset_index(drop=True)


def _trade_rows(full_curve: pd.DataFrame, model_curve: pd.DataFrame) -> pd.DataFrame:
    """Return actual completed markets within the selected notebook window.

    The curve has one explicit starting-balance anchor and then an after-market
    balance for each completed position.  For a tail window, the balance before
    its first selected market comes from the immediately preceding full-curve
    observation.  This preserves the real dollar scale of the account.
    """

    if len(model_curve) < 2:
        raise ValueError("Need an anchor plus at least one completed market")
    first_timestamp = model_curve["ds"].iloc[0]
    first_position = int(full_curve.index[full_curve["ds"] == first_timestamp][0])
    # When the selected window begins at a genuine trade (rather than the
    # original STARTING_BALANCE anchor), that row is a known balance before the
    # selected first trade, so skip it from the replay.
    selected = model_curve.iloc[1:].copy().reset_index(drop=True)
    selected["trade_pnl"] = selected["y"].diff()
    selected.loc[0, "trade_pnl"] = selected.loc[0, "y"] - model_curve.loc[0, "y"]
    selected["balance_before_window"] = float(model_curve.loc[0, "y"])
    selected["model_window_start_index"] = first_position
    return selected


def replay_notebook_bands(
    full_curve: pd.DataFrame,
    variant: NotebookBatchVariant,
    *,
    forecast_periods: int = 100,
    forecast_frequency: str = "15min",
    uncertainty_samples: int = 2000,
    random_seed: int = 42,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, object]]:
    """Fit the notebook model once and replay delayed P10/P90 signals."""

    model_curve = _select_model_curve(full_curve, variant.model_window)
    forecast = fit_colab_prophet(
        model_curve,
        forecast_periods=forecast_periods,
        forecast_frequency=forecast_frequency,
        changepoint_prior_scale=variant.changepoint_prior_scale,
        seasonality_prior_scale=variant.seasonality_prior_scale,
        uncertainty_samples=uncertainty_samples,
        random_seed=random_seed,
    )
    historical = forecast[forecast["actual"].notna()].copy()
    if len(historical) != len(model_curve):
        raise AssertionError("Notebook forecast did not retain every selected historical balance")
    if not historical["actual"].equals(model_curve["y"]):
        raise AssertionError("Notebook historical actuals no longer match the selected absolute balance curve")

    trades = _trade_rows(full_curve, model_curve)
    bands = historical.iloc[1:][["ds", *QUANTILE_NAMES]].reset_index(drop=True)
    if not trades["ds"].equals(bands["ds"]):
        raise AssertionError("Trade timestamps do not align with notebook historical forecast rows")
    log = pd.concat([trades.reset_index(drop=True), bands.drop(columns="ds")], axis=1)
    log.insert(0, "trade_index", np.arange(len(log), dtype=int))
    log["shadow_balance_before"] = log["y"].shift(1).fillna(model_curve.loc[0, "y"])
    log["shadow_balance_after"] = log["y"]
    log["always_on_balance"] = log["shadow_balance_after"]

    active = False
    active_for_trade: list[bool] = []
    entry_signals: list[bool] = []
    exit_signals: list[bool] = []
    state_before: list[str] = []
    state_after: list[str] = []
    filtered_before: list[float] = []
    filtered_after: list[float] = []
    filtered_balance = float(model_curve.loc[0, "y"])
    for row in log.itertuples(index=False):
        state_before.append("on" if active else "off")
        active_for_trade.append(active)
        filtered_before.append(filtered_balance)
        pnl = float(row.trade_pnl) if active else 0.0
        filtered_balance += pnl
        filtered_after.append(filtered_balance)
        enter = (not active) and float(row.shadow_balance_after) <= float(getattr(row, variant.entry_column))
        exit_ = active and float(row.shadow_balance_after) >= float(getattr(row, variant.exit_column))
        if enter:
            active = True
        elif exit_:
            active = False
        entry_signals.append(enter)
        exit_signals.append(exit_)
        state_after.append("on" if active else "off")

    log["strategy"] = f"{variant.entry_column}_entry_{variant.exit_column}_exit"
    log["state_before_trade"] = state_before
    log["bot_active_for_trade"] = active_for_trade
    log["entry_signal_after_trade"] = entry_signals
    log["exit_signal_after_trade"] = exit_signals
    log["state_after_trade"] = state_after
    log["selected_trade_pnl"] = np.where(log["bot_active_for_trade"], log["trade_pnl"], 0.0)
    log["skipped_trade_pnl"] = np.where(~log["bot_active_for_trade"], log["trade_pnl"], 0.0)
    log["filtered_balance_before"] = filtered_before
    log["filtered_balance_after"] = filtered_after
    log["model_window"] = "all" if variant.model_window is None else variant.model_window
    log["changepoint_prior_scale"] = variant.changepoint_prior_scale
    log["seasonality_prior_scale"] = variant.seasonality_prior_scale
    log["fitted_band_replay"] = True

    _assert_replay_integrity(log, model_curve)
    summary = _summary(log, model_curve, variant)
    return forecast, log, summary


def _maximum_drawdown(balance: pd.Series) -> float:
    return float((balance.cummax() - balance).max())


def _profit_factor(pnl: pd.Series) -> float:
    profit = float(pnl[pnl > 0].sum())
    loss = float(-pnl[pnl < 0].sum())
    return np.inf if loss == 0 and profit > 0 else (profit / loss if loss else np.nan)


def _summary(log: pd.DataFrame, model_curve: pd.DataFrame, variant: NotebookBatchVariant) -> dict[str, object]:
    active_pnl = log.loc[log["bot_active_for_trade"], "selected_trade_pnl"]
    always_pnl = log["trade_pnl"]
    active_wins = int((active_pnl > 0).sum())
    active_losses = int((active_pnl < 0).sum())
    return {
        "variant": variant.label,
        "scope": "notebook_fitted_band_replay_in_sample",
        "model_window": "all" if variant.model_window is None else variant.model_window,
        "model_observations": len(model_curve),
        "entry_threshold": variant.entry_column,
        "exit_threshold": variant.exit_column,
        "changepoint_prior_scale": variant.changepoint_prior_scale,
        "seasonality_prior_scale": variant.seasonality_prior_scale,
        "balance_before_first_selected_trade": float(model_curve.loc[0, "y"]),
        "total_opportunities": len(log),
        "trades_taken": int(log["bot_active_for_trade"].sum()),
        "trades_skipped": int((~log["bot_active_for_trade"]).sum()),
        "wins": active_wins,
        "losses": active_losses,
        "win_rate": active_wins / len(active_pnl) if len(active_pnl) else np.nan,
        "net_pnl": float(active_pnl.sum()),
        "ending_balance": float(log["filtered_balance_after"].iloc[-1]),
        "maximum_drawdown_dollars": _maximum_drawdown(log["filtered_balance_after"]),
        "profit_factor": _profit_factor(active_pnl),
        "always_on_net_pnl": float(always_pnl.sum()),
        "always_on_ending_balance": float(log["always_on_balance"].iloc[-1]),
        "always_on_maximum_drawdown_dollars": _maximum_drawdown(log["always_on_balance"]),
        "improvement_vs_always_on": float(active_pnl.sum() - always_pnl.sum()),
        "entry_signals": int(log["entry_signal_after_trade"].sum()),
        "exit_signals": int(log["exit_signal_after_trade"].sum()),
        "final_state": str(log["state_after_trade"].iloc[-1]),
        "p01_p99_coverage": float(((log["shadow_balance_after"] >= log["p01"]) & (log["shadow_balance_after"] <= log["p99"])).mean()),
        "p10_p90_coverage": float(((log["shadow_balance_after"] >= log["p10"]) & (log["shadow_balance_after"] <= log["p90"])).mean()),
        "p25_p75_coverage": float(((log["shadow_balance_after"] >= log["p25"]) & (log["shadow_balance_after"] <= log["p75"])).mean()),
        "p50_mae": float((log["shadow_balance_after"] - log["p50"]).abs().mean()),
    }


def _assert_replay_integrity(log: pd.DataFrame, model_curve: pd.DataFrame) -> None:
    """Protect the account-balance and next-market signal conventions."""

    assert np.allclose(log["shadow_balance_after"], log["always_on_balance"])
    assert np.allclose(log["trade_pnl"], log["shadow_balance_after"] - log["shadow_balance_before"])
    assert np.allclose(
        log["filtered_balance_after"],
        float(model_curve.loc[0, "y"]) + log["selected_trade_pnl"].cumsum(),
    )
    assert (log.loc[~log["bot_active_for_trade"], "selected_trade_pnl"] == 0).all()
    assert np.allclose(
        log.loc[log["bot_active_for_trade"], "selected_trade_pnl"],
        log.loc[log["bot_active_for_trade"], "trade_pnl"],
    )
    assert (log[list(QUANTILE_NAMES)].diff(axis=1).iloc[:, 1:] >= -1e-10).all().all()
    # The state after row t is, by construction, the state before row t + 1.
    assert (log["state_after_trade"].iloc[:-1].to_numpy() == log["state_before_trade"].iloc[1:].to_numpy()).all()


def _default_variants() -> Iterable[NotebookBatchVariant]:
    # Primary notebook configuration plus isolated (one-at-a-time) exploratory
    # changes.  Each model window preserves original absolute balances.
    yield NotebookBatchVariant("primary_full_history_p10_p90", None, 0.05, 10.0)
    for window in (50, 75, 100, 150, 200):
        yield NotebookBatchVariant(f"window_{window}_p10_p90", window, 0.05, 10.0)
    for cps in (0.01, 0.10, 0.25):
        yield NotebookBatchVariant(f"cps_{cps:.2f}_p10_p90", None, cps, 10.0)
    for sps in (1.0, 20.0):
        yield NotebookBatchVariant(f"sps_{sps:.0f}_p10_p90", None, 0.05, sps)
    yield NotebookBatchVariant("full_history_p10_p50", None, 0.05, 10.0, 10, 50)
    yield NotebookBatchVariant("full_history_p25_p75", None, 0.05, 10.0, 25, 75)
    yield NotebookBatchVariant("full_history_p01_p99", None, 0.05, 10.0, 1, 99)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True)
    parser.add_argument("--input-format", choices=("kalshi", "equity"), default="kalshi")
    parser.add_argument("--starting-balance", type=float, default=100.0)
    parser.add_argument("--output-dir", default="outputs/notebook_batch_backtest")
    parser.add_argument("--forecast-periods", type=int, default=100)
    parser.add_argument("--forecast-frequency", default="15min")
    parser.add_argument("--uncertainty-samples", type=int, default=2000)
    parser.add_argument("--random-seed", type=int, default=42)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    full_curve = (
        build_colab_equity_curve(args.input, args.starting_balance)
        if args.input_format == "kalshi"
        else load_colab_equity_curve(args.input)
    )
    full_curve.to_csv(output_dir / "kalshi_equity_curve_metaprophet.csv", index=False, date_format="%Y-%m-%d %H:%M:%S")
    summaries: list[dict[str, object]] = []
    primary_forecast: pd.DataFrame | None = None
    primary_log: pd.DataFrame | None = None
    for number, variant in enumerate(_default_variants(), start=1):
        print(f"[{number}] notebook fitted-band replay: {variant.label}", flush=True)
        forecast, log, summary = replay_notebook_bands(
            full_curve, variant,
            forecast_periods=args.forecast_periods,
            forecast_frequency=args.forecast_frequency,
            uncertainty_samples=args.uncertainty_samples,
            random_seed=args.random_seed,
        )
        summaries.append(summary)
        if variant.label == "primary_full_history_p10_p90":
            primary_forecast, primary_log = forecast, log
    summary_frame = pd.DataFrame(summaries)
    summary_frame.to_csv(output_dir / "notebook_batch_sensitivity_results.csv", index=False)
    assert primary_forecast is not None and primary_log is not None
    primary_forecast.to_csv(output_dir / "kalshi_equity_prophet_quantiles.csv", index=False, date_format="%Y-%m-%d %H:%M:%S")
    primary_log.to_csv(output_dir / "notebook_batch_p10_p90_trade_log.csv", index=False, date_format="%Y-%m-%d %H:%M:%S")
    primary_summary = summary_frame[summary_frame["variant"] == "primary_full_history_p10_p90"]
    primary_summary.to_csv(output_dir / "notebook_batch_p10_p90_summary.csv", index=False)
    (output_dir / "README.md").write_text(
        "# Notebook fitted-band replay\n\n"
        "This output exactly follows the supplied Colab Prophet fit: one model is fitted "
        "to each complete selected absolute-balance window, then its historical fitted "
        "quantile bands are replayed with next-market state timing. It is an in-sample "
        "diagnostic and must not be treated as causal walk-forward evidence.\n",
        encoding="utf-8",
    )
    print("\nPrimary notebook-style replay:")
    print(primary_summary[["trades_taken", "net_pnl", "ending_balance", "maximum_drawdown_dollars", "profit_factor"]].to_string(index=False))
    print(f"Outputs: {output_dir.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
