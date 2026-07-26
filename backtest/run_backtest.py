"""Command-line entry point for the full Kalshi equity walk-forward analysis."""

from __future__ import annotations

import argparse
from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

from .charts import create_charts
from .config import BacktestConfig, output_paths
from .data_loader import load_input
from .equity import performance_summary
from .regimes import regime_windows
from .report import write_report
from .signals import EXPLORATORY_RULES, PRIMARY_RULE, SignalRule, apply_rule
from .statistics import calibration_analysis, statistical_analysis
from .walk_forward import integrity_checks, run_walk_forward, verify_reproducibility


def _quick_one_sided(values: pd.Series) -> float:
    x = values.dropna().astype(float).to_numpy()
    if len(x) < 2:
        return np.nan
    std = np.std(x, ddof=1)
    if std == 0:
        return np.nan
    return float(stats.t.sf(np.mean(x) / (std / np.sqrt(len(x))), len(x) - 1))


def _summary_for_rule(log: pd.DataFrame, rule: SignalRule, config: BacktestConfig) -> tuple[dict[str, object], pd.DataFrame]:
    windows = regime_windows(log)
    evaluation = log[log["is_walk_forward_evaluation"]].copy()
    summary = performance_summary(
        evaluation, rule.name, "selected_trade_pnl", "bot_active_for_trade", config.starting_balance,
        "entry_signal_after_trade", "exit_signal_after_trade",
        int((windows["status"] == "closed").sum()) if not windows.empty else 0,
        bool(not windows.empty and windows["status"].iloc[-1] == "open_at_end"), rule.exploratory,
    )
    summary["model_fallback_count"] = int(log["fallback_used"].sum())
    return summary, windows


def _baseline_summary(log: pd.DataFrame, config: BacktestConfig, name: str, active: bool) -> dict[str, object]:
    evaluation = log[log["is_walk_forward_evaluation"]].copy()
    evaluation["baseline_active"] = active
    evaluation["baseline_pnl"] = evaluation["trade_pnl"] if active else 0.0
    return performance_summary(evaluation, name, "baseline_pnl", "baseline_active", config.starting_balance)


def sensitivity_configs(primary: BacktestConfig) -> list[tuple[str, str, BacktestConfig]]:
    """Declared OAT sensitivity matrix; primary remains the sole confirmatory test."""

    specs: list[tuple[str, str, BacktestConfig]] = [("primary", "preregistered", primary)]
    for value in (50, 75, 125, 150):
        specs.append(("min_training_trades", str(value), replace(primary, min_training_trades=value)))
    for value in (75, 100, 150, 200):
        specs.append(("training_window", str(value), replace(primary, training_window=value)))
    for value in (4, 8, 16):
        specs.append(("refit_every_n_trades", str(value), replace(primary, refit_every_n_trades=value)))
    for value in (0.01, 0.10, 0.25):
        specs.append(("changepoint_prior_scale", str(value), replace(primary, changepoint_prior_scale=value)))
    specs.append(("signal_mode", "crossing", replace(primary, signal_mode="crossing")))
    return specs


def run_sensitivity(trades: pd.DataFrame, primary: BacktestConfig, output_dir: Path) -> tuple[pd.DataFrame, dict[str, float]]:
    rows: list[dict[str, object]] = []
    exploratory_p: dict[str, float] = {}
    configurations = sensitivity_configs(primary)
    for number, (axis, value, config) in enumerate(configurations, start=1):
        print(f"[sensitivity {number}/{len(configurations)}] {axis}={value}", flush=True)
        result = run_walk_forward(trades, config, output_dir=output_dir, use_cache=True)
        integrity_checks(result.log, config)
        summary, windows = _summary_for_rule(result.log, PRIMARY_RULE, config)
        calibration = calibration_analysis(result.log)
        overall = calibration[calibration["group_type"] == "overall"]
        evaluation = result.log[result.log["is_walk_forward_evaluation"]]
        selected = evaluation.loc[evaluation["bot_active_for_trade"], "selected_trade_pnl"]
        paired = evaluation["selected_trade_pnl"] - evaluation["trade_pnl"]
        rows.append({
            "sensitivity_axis": axis, "parameter_value": value,
            "min_training_trades": config.min_training_trades, "training_window": config.training_window_label,
            "refit_every_n_trades": config.refit_every_n_trades,
            "changepoint_prior_scale": config.changepoint_prior_scale, "signal_mode": config.signal_mode,
            "selected_trades": summary["trades_taken"], "net_pnl": summary["net_pnl"],
            "ending_balance": summary["ending_balance"], "maximum_drawdown": summary["maximum_drawdown_dollars"],
            "win_rate": summary["win_rate"], "profit_factor": summary["profit_factor"],
            "one_sided_p_value": _quick_one_sided(selected), "paired_improvement_p_value": _quick_one_sided(paired),
            "p10_p90_coverage": overall.iloc[0]["p10_p90_coverage"] if not overall.empty else np.nan,
            "p50_mae": overall.iloc[0]["p50_mae"] if not overall.empty else np.nan,
            "number_of_regime_windows": int((windows["status"] == "closed").sum()) if not windows.empty else 0,
        })
        if axis == "primary":
            for rule in EXPLORATORY_RULES:
                alternative = apply_rule(result.log, rule, config.signal_mode, config.initial_bot_state, config.min_training_trades)
                alt_eval = alternative[alternative["is_walk_forward_evaluation"]]
                exploratory_p[rule.name] = _quick_one_sided(alt_eval["selected_trade_pnl"] - alt_eval["trade_pnl"])
    return pd.DataFrame(rows), exploratory_p


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Original Kalshi closed-position CSV or ds/y equity CSV")
    parser.add_argument("--input-format", choices=("kalshi", "equity"), required=True)
    parser.add_argument("--output-dir", default="outputs")
    parser.add_argument("--starting-balance", type=float, default=100.0)
    parser.add_argument("--min-training-trades", type=int, default=100)
    parser.add_argument("--training-window", type=int, default=None)
    parser.add_argument("--refit-every", type=int, default=1)
    parser.add_argument("--forecast-frequency", default="15min")
    parser.add_argument("--uncertainty-samples", type=int, default=2000)
    parser.add_argument("--changepoint-prior-scale", type=float, default=.05)
    parser.add_argument("--seasonality-prior-scale", type=float, default=10.0)
    parser.add_argument("--no-log-transform", action="store_true")
    parser.add_argument("--random-seed", type=int, default=42)
    parser.add_argument("--initial-bot-state", choices=("on", "off"), default="off")
    parser.add_argument("--signal-mode", choices=("level", "crossing"), default="level")
    parser.add_argument("--bootstrap-resamples", type=int, default=10_000)
    parser.add_argument("--rolling-window", type=int, default=30)
    parser.add_argument("--run-sensitivity", action=argparse.BooleanOptionalAction, default=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = BacktestConfig(
        starting_balance=args.starting_balance, min_training_trades=args.min_training_trades,
        training_window=args.training_window, refit_every_n_trades=args.refit_every,
        forecast_frequency=args.forecast_frequency, uncertainty_samples=args.uncertainty_samples,
        changepoint_prior_scale=args.changepoint_prior_scale, seasonality_prior_scale=args.seasonality_prior_scale,
        use_log_transform=not args.no_log_transform, random_seed=args.random_seed,
        initial_bot_state=args.initial_bot_state, signal_mode=args.signal_mode,
        bootstrap_resamples=args.bootstrap_resamples, rolling_window=args.rolling_window,
    )
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = output_paths(output_dir)
    print("Loading input and constructing shadow equity...", flush=True)
    trades = load_input(args.input, args.input_format, config.starting_balance)
    print(f"Loaded {len(trades)} chronological opportunities from {trades['ds'].iloc[0]} to {trades['ds'].iloc[-1]}", flush=True)
    print("Running primary strict walk-forward Prophet forecast...", flush=True)
    primary_result = run_walk_forward(trades, config, output_dir=output_dir, persist_partial=True)
    primary_log = primary_result.log
    print("Repeating primary forecast for the same-seed reproducibility assertion...", flush=True)
    reproducible = verify_reproducibility(trades, config, primary_log)
    integrity = integrity_checks(primary_log, config, reproducibility_passed=reproducible)
    integrity.to_csv(paths["integrity"], index=False)
    primary_log.to_csv(paths["trade_log"], index=False)
    primary_result.fit_failures.to_csv(paths["fit_failures"], index=False)
    primary_summary, windows = _summary_for_rule(primary_log, PRIMARY_RULE, config)
    windows.to_csv(paths["regime_windows"], index=False)
    summary_rows = [_baseline_summary(primary_log, config, "always_on", True), primary_summary, _baseline_summary(primary_log, config, "always_off_cash", False)]
    for rule in EXPLORATORY_RULES:
        alternative = apply_rule(primary_log, rule, config.signal_mode, config.initial_bot_state, config.min_training_trades)
        alternative_summary, _ = _summary_for_rule(alternative, rule, config)
        summary_rows.append(alternative_summary)
    summary = pd.DataFrame(summary_rows)
    summary.to_csv(paths["summary"], index=False)
    calibration = calibration_analysis(primary_log)
    calibration.to_csv(paths["calibration"], index=False)
    sensitivity, exploratory_p = run_sensitivity(trades, config, output_dir) if args.run_sensitivity else (pd.DataFrame(), {})
    sensitivity.to_csv(paths["sensitivity"], index=False)
    statistics, bootstrap = statistical_analysis(primary_log, windows, config, exploratory_p)
    statistics.to_csv(paths["statistics"], index=False)
    bootstrap.to_csv(paths["bootstrap"], index=False)
    create_charts(primary_log, calibration, output_dir, config)
    write_report(paths["report"], config, summary, windows, calibration, statistics, bootstrap, sensitivity, primary_result.fit_failures, integrity)
    (output_dir / "run_configuration.json").write_text(pd.Series(config.as_dict()).to_json(indent=2), encoding="utf-8")
    print("Backtest complete. Primary results:", flush=True)
    print(summary.loc[summary["strategy"].isin(["always_on", "p10_p90"]), ["strategy", "net_pnl", "ending_balance", "maximum_drawdown_dollars", "trades_taken"]].to_string(index=False), flush=True)
    print(f"Outputs: {output_dir.resolve()}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
