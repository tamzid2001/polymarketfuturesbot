"""Strict expanding/rolling walk-forward replay with no look-ahead leakage."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from .config import BacktestConfig
from .data_loader import data_signature
from .prophet_forecaster import QUANTILE_COLUMNS, ProphetForecaster
from .signals import PRIMARY_RULE, SignalRule, apply_rule


@dataclass
class WalkForwardResult:
    log: pd.DataFrame
    fit_failures: pd.DataFrame
    evaluation_start_index: int
    cache_key: str


def verify_reproducibility(trades: pd.DataFrame, config: BacktestConfig, reference_log: pd.DataFrame) -> bool:
    """Actually repeat the primary seeded run and compare causal forecasts.

    This intentionally avoids the on-disk cache.  It is a real reproducibility
    assertion, not merely a claim that a random seed was configured.
    """

    repeated = run_walk_forward(trades, config, use_cache=False, persist_partial=False).log
    if len(repeated) != len(reference_log):
        return False
    for column in (*QUANTILE_COLUMNS, "selected_trade_pnl", "filtered_equity_after"):
        if not np.allclose(
            reference_log[column].to_numpy(dtype=float), repeated[column].to_numpy(dtype=float),
            equal_nan=True, rtol=1e-10, atol=1e-10,
        ):
            return False
    return bool((reference_log["state_after_trade"] == repeated["state_after_trade"]).all())


def _cache_key(trades: pd.DataFrame, config: BacktestConfig) -> str:
    fields = {
        "data": data_signature(trades),
        "min_training_trades": config.min_training_trades,
        "training_window": config.training_window,
        "refit_every_n_trades": config.refit_every_n_trades,
        "uncertainty_samples": config.uncertainty_samples,
        "changepoint_prior_scale": config.changepoint_prior_scale,
        "seasonality_prior_scale": config.seasonality_prior_scale,
        "use_log_transform": config.use_log_transform,
        "random_seed": config.random_seed,
    }
    return hashlib.sha256(json.dumps(fields, sort_keys=True).encode()).hexdigest()[:20]


def _build_base(trades: pd.DataFrame, config: BacktestConfig) -> pd.DataFrame:
    log = trades.copy().reset_index(drop=True)
    log["shadow_equity_before"] = (
        config.starting_balance + log["trade_pnl"].cumsum().shift(1).fillna(0.0)
    )
    log["filtered_equity_before"] = config.starting_balance
    log["forecast_timestamp"] = pd.NaT
    log["training_start"] = pd.NaT
    log["training_end"] = pd.NaT
    log["training_rows"] = 0
    log["refit_number"] = pd.NA
    log["model_fit_success"] = pd.NA
    log["model_fit_error"] = pd.NA
    log["fallback_used"] = False
    for column in QUANTILE_COLUMNS:
        log[column] = np.nan
    return log


def _read_cached(cache_path: Path, trades: pd.DataFrame, config: BacktestConfig) -> pd.DataFrame | None:
    if not cache_path.exists():
        return None
    cached = pd.read_csv(cache_path, parse_dates=["ds", "forecast_timestamp", "training_start", "training_end"])
    if len(cached) != len(trades) or not cached["ds"].equals(trades["ds"]):
        return None
    # Source P/L and account values are rebuilt rather than trusted from cache.
    base = _build_base(trades, config)
    reusable = [
        "forecast_timestamp", "training_start", "training_end", "training_rows", "refit_number",
        "model_fit_success", "model_fit_error", "fallback_used", *QUANTILE_COLUMNS,
    ]
    for column in reusable:
        if column in cached:
            base[column] = cached[column].to_numpy()
    return base


def _write_cache(log: pd.DataFrame, cache_path: Path) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = cache_path.with_suffix(".partial.csv")
    log.to_csv(temporary, index=False)
    temporary.replace(cache_path)


def run_walk_forward(
    trades: pd.DataFrame,
    config: BacktestConfig,
    *,
    rule: SignalRule = PRIMARY_RULE,
    output_dir: str | Path | None = None,
    use_cache: bool = True,
    persist_partial: bool = False,
) -> WalkForwardResult:
    """Forecast each post-training trade before its P/L is revealed.

    The returned primary log has one row per original opportunity.  Any signal
    produced on row ``t`` has no effect until row ``t+1`` because the state is
    deliberately applied by :func:`backtest.signals.apply_rule` afterwards.
    """

    if len(trades) <= config.min_training_trades:
        raise ValueError(
            f"Need more than {config.min_training_trades} trades; received {len(trades)}"
        )
    if config.use_log_transform and (trades["shadow_equity_after"] <= 0).any():
        raise ValueError("All shadow equity observations must be positive when log-transforming")
    if not trades["ds"].is_monotonic_increasing or trades["ds"].duplicated().any():
        raise ValueError("Trades must be chronological with unique timestamps")

    root = Path(output_dir) if output_dir else None
    key = _cache_key(trades, config)
    cache_path = root / "forecast_cache" / f"{key}.csv" if root else None
    log = _read_cached(cache_path, trades, config) if cache_path and use_cache else None
    failures: list[dict[str, object]] = []

    if log is None:
        log = _build_base(trades, config)
        forecaster = ProphetForecaster(config)
        active_training_start: pd.Timestamp | pd.NaT = pd.NaT
        active_training_end: pd.Timestamp | pd.NaT = pd.NaT
        active_training_rows = 0
        last_quantiles: dict[str, float] | None = None
        last_quantile_trade_index: int | None = None

        for trade_index in range(len(log)):
            if trade_index < config.min_training_trades:
                continue
            refit_due = (trade_index - config.min_training_trades) % config.refit_every_n_trades == 0
            fit_error: str | None = None
            if refit_due:
                start = 0 if config.training_window is None else max(0, trade_index - config.training_window)
                training = log.iloc[start:trade_index][["ds", "shadow_equity_after"]].rename(
                    columns={"shadow_equity_after": "y"}
                )
                try:
                    forecaster.fit(training, trade_index)
                    active_training_start = training["ds"].iloc[0]
                    active_training_end = training["ds"].iloc[-1]
                    active_training_rows = len(training)
                except Exception as exc:
                    fit_error = f"fit failed: {type(exc).__name__}: {exc}"
                    failures.append(
                        {
                            "trade_index": trade_index,
                            "ds": log.at[trade_index, "ds"],
                            "stage": "fit",
                            "error": fit_error,
                        }
                    )

            log.at[trade_index, "forecast_timestamp"] = log.at[trade_index, "ds"]
            log.at[trade_index, "training_start"] = active_training_start
            log.at[trade_index, "training_end"] = active_training_end
            log.at[trade_index, "training_rows"] = active_training_rows
            log.at[trade_index, "refit_number"] = forecaster.refit_number

            result = None
            if fit_error is None:
                result = forecaster.forecast(log.at[trade_index, "ds"])
                if not result.success:
                    failures.append(
                        {
                            "trade_index": trade_index,
                            "ds": log.at[trade_index, "ds"],
                            "stage": "predict",
                            "error": result.error,
                        }
                    )
            if result is not None and result.success:
                for column, value in result.quantiles.items():
                    log.at[trade_index, column] = value
                log.at[trade_index, "model_fit_success"] = True
                log.at[trade_index, "model_fit_error"] = pd.NA
                last_quantiles = result.quantiles
                last_quantile_trade_index = trade_index
            else:
                error = fit_error if fit_error is not None else (result.error if result else "Unknown model failure")
                can_fallback = (
                    config.fallback_policy == "previous_one_trade"
                    and last_quantiles is not None
                    and last_quantile_trade_index == trade_index - 1
                )
                if can_fallback:
                    for column, value in last_quantiles.items():
                        log.at[trade_index, column] = value
                    log.at[trade_index, "fallback_used"] = True
                log.at[trade_index, "model_fit_success"] = False
                log.at[trade_index, "model_fit_error"] = error

            if root and persist_partial and (trade_index + 1) % config.save_every == 0:
                root.mkdir(parents=True, exist_ok=True)
                log.iloc[: trade_index + 1].to_csv(root / "walk_forward_trade_log.partial.csv", index=False)
        if cache_path:
            _write_cache(log, cache_path)

    # Derive after-outcome diagnostics only once the forecast made before that
    # outcome is fixed in the log.
    has_forecast = log["p50"].notna()
    log["forecast_error_p50"] = np.where(has_forecast, log["shadow_equity_after"] - log["p50"], np.nan)
    log["inside_p01_p99"] = np.where(
        has_forecast,
        (log["shadow_equity_after"] >= log["p01"]) & (log["shadow_equity_after"] <= log["p99"]),
        np.nan,
    )
    log["inside_p10_p90"] = np.where(
        has_forecast,
        (log["shadow_equity_after"] >= log["p10"]) & (log["shadow_equity_after"] <= log["p90"]),
        np.nan,
    )
    log["inside_p25_p75"] = np.where(
        has_forecast,
        (log["shadow_equity_after"] >= log["p25"]) & (log["shadow_equity_after"] <= log["p75"]),
        np.nan,
    )
    log["signal_mode"] = config.signal_mode
    log["training_window"] = config.training_window_label
    log = apply_rule(log, rule, config.signal_mode, config.initial_bot_state, config.min_training_trades)
    log["always_on_equity"] = config.starting_balance + log["trade_pnl"].cumsum()
    log["is_walk_forward_evaluation"] = log["trade_index"] >= config.min_training_trades
    return WalkForwardResult(
        log=log,
        fit_failures=pd.DataFrame(failures, columns=["trade_index", "ds", "stage", "error"]),
        evaluation_start_index=config.min_training_trades,
        cache_key=key,
    )


def integrity_checks(
    log: pd.DataFrame,
    config: BacktestConfig,
    reproducibility_passed: bool | None = None,
) -> pd.DataFrame:
    """Assert and record every accounting and anti-leakage invariant."""

    evaluation = log[log["is_walk_forward_evaluation"]].copy()
    has_forecast = evaluation["p50"].notna()
    checks: list[tuple[str, bool, str]] = []
    checks.append((
        "forecast_timestamp_after_training_end",
        bool((evaluation.loc[has_forecast, "forecast_timestamp"] > evaluation.loc[has_forecast, "training_end"]).all()),
        "Each forecasted trade occurs after the final model-training observation.",
    ))
    checks.append((
        "no_training_timestamp_at_or_after_forecast",
        bool((evaluation.loc[has_forecast, "training_end"] < evaluation.loc[has_forecast, "ds"]).all()),
        "The recorded training end is strictly earlier than its forecasted trade.",
    ))
    state_next = log["state_before_trade"].shift(-1)
    checks.append((
        "signals_apply_only_to_next_trade",
        bool((log.iloc[:-1]["state_after_trade"] == state_next.iloc[:-1]).all()),
        "State after t equals state before t+1; state before t controls trade t.",
    ))
    active = log["bot_active_for_trade"].astype(bool)
    checks.append(("active_trade_pnl_matches_source", bool(np.allclose(log.loc[active, "selected_trade_pnl"], log.loc[active, "trade_pnl"])), "Selected P/L equals source P/L only when active."))
    checks.append(("inactive_trade_pnl_is_zero", bool(np.allclose(log.loc[~active, "selected_trade_pnl"], 0.0)), "Selected P/L is zero while inactive."))
    expected_shadow = config.starting_balance + log["trade_pnl"].cumsum()
    checks.append(("shadow_includes_every_trade", bool(np.allclose(log["shadow_equity_after"], expected_shadow)), "Shadow equity includes every opportunity."))
    expected_filtered = config.starting_balance + log["selected_trade_pnl"].cumsum()
    checks.append(("filtered_includes_selected_only", bool(np.allclose(log["filtered_equity_after"], expected_filtered)), "Filtered equity includes selected opportunities only."))
    ordered = log.loc[log["p50"].notna(), list(QUANTILE_COLUMNS)].to_numpy(dtype=float)
    checks.append(("quantiles_are_ordered", bool((np.diff(ordered, axis=1) >= 0).all()), "p01 <= p10 <= p25 <= p50 <= p75 <= p90 <= p99."))
    checks.append(("always_on_ending_matches_source", bool(np.isclose(log["always_on_equity"].iloc[-1], log["shadow_equity_after"].iloc[-1])), "Always-on ledger reconciles to source equity."))
    checks.append(("no_duplicate_trade_timestamps", bool(not log["ds"].duplicated().any()), "No duplicate trade timestamps remain."))
    checks.append(("no_future_actuals_in_training", bool((evaluation.loc[has_forecast, "training_end"] < evaluation.loc[has_forecast, "ds"]).all()), "No future actual is in a Prophet input."))
    if reproducibility_passed is not None:
        checks.append((
            "same_seed_repeat_reproduces_forecasts", reproducibility_passed,
            "A second uncached run with the same seed matched quantiles, state, and filtered equity.",
        ))
    result = pd.DataFrame(checks, columns=["check", "passed", "description"])
    failed = result.loc[~result["passed"]]
    if not failed.empty:
        raise AssertionError("Walk-forward integrity check failed: " + "; ".join(failed["check"]))
    return result
