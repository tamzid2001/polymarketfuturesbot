"""Out-of-sample statistics, bootstrap inference, and forecast calibration."""

from __future__ import annotations

from statistics import NormalDist

import numpy as np
import pandas as pd
from scipy import stats

from .config import BacktestConfig
from .prophet_forecaster import QUANTILE_COLUMNS, QUANTILE_PROBABILITIES


def _ci(mean: float, std: float, n: int, level: float) -> tuple[float, float]:
    if n < 2 or not np.isfinite(std):
        return np.nan, np.nan
    critical = stats.t.ppf((1 + level) / 2, df=n - 1)
    margin = critical * std / np.sqrt(n)
    return float(mean - margin), float(mean + margin)


def _one_sample(values: pd.Series, label: str) -> dict[str, object]:
    x = values.astype(float).dropna().to_numpy()
    n = len(x)
    mean = float(np.mean(x)) if n else np.nan
    std = float(np.std(x, ddof=1)) if n > 1 else np.nan
    se = std / np.sqrt(n) if n > 1 else np.nan
    t = mean / se if se and np.isfinite(se) and se > 0 else np.nan
    two_sided = float(2 * stats.t.sf(abs(t), n - 1)) if n > 1 and np.isfinite(t) else np.nan
    one_sided = float(stats.t.sf(t, n - 1)) if n > 1 and np.isfinite(t) else np.nan
    ci90 = _ci(mean, std, n, 0.90)
    ci95 = _ci(mean, std, n, 0.95)
    ci99 = _ci(mean, std, n, 0.99)
    return {
        "test": label,
        "n": n,
        "mean": mean,
        "standard_deviation": std,
        "standard_error": se,
        "t_statistic": t,
        "one_sided_p_value_greater": one_sided,
        "two_sided_p_value": two_sided,
        "ci90_lower": ci90[0], "ci90_upper": ci90[1],
        "ci95_lower": ci95[0], "ci95_upper": ci95[1],
        "ci99_lower": ci99[0], "ci99_upper": ci99[1],
    }


def moving_block_bootstrap(
    values: np.ndarray,
    block_length: int,
    resamples: int,
    seed: int,
) -> dict[str, float]:
    """Moving-block bootstrap CI and centered one-sided null p-value."""

    x = np.asarray(values, dtype=float)
    n = len(x)
    if n < 2:
        return {"block_length": block_length, "resamples": resamples, "ci95_lower": np.nan, "ci95_upper": np.nan, "p_value_greater": np.nan}
    block = max(1, min(int(block_length), n))
    rng = np.random.default_rng(seed + block)
    starts = rng.integers(0, n - block + 1, size=(resamples, int(np.ceil(n / block))))
    offsets = np.arange(block)
    indices = (starts[:, :, None] + offsets).reshape(resamples, -1)[:, :n]
    boot_means = x[indices].mean(axis=1)
    centered = x - x.mean()
    null_means = centered[indices].mean(axis=1)
    observed = float(x.mean())
    return {
        "block_length": block,
        "resamples": resamples,
        "ci95_lower": float(np.quantile(boot_means, 0.025)),
        "ci95_upper": float(np.quantile(boot_means, 0.975)),
        "p_value_greater": float((1 + np.sum(null_means >= observed)) / (resamples + 1)),
    }


def _benjamini_hochberg(p_values: list[float]) -> list[float]:
    valid = [(i, value) for i, value in enumerate(p_values) if np.isfinite(value)]
    output = [np.nan] * len(p_values)
    if not valid:
        return output
    order = sorted(valid, key=lambda item: item[1])
    m = len(order)
    adjusted = []
    for rank, (_, value) in enumerate(order, start=1):
        adjusted.append(min(1.0, value * m / rank))
    for index in range(m - 2, -1, -1):
        adjusted[index] = min(adjusted[index], adjusted[index + 1])
    for (original_index, _), value in zip(order, adjusted, strict=True):
        output[original_index] = value
    return output


def statistical_analysis(
    primary_log: pd.DataFrame,
    windows: pd.DataFrame,
    config: BacktestConfig,
    exploratory_p_values: dict[str, float] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Tests only the genuine post-initial-training opportunity rows."""

    eval_log = primary_log[primary_log["is_walk_forward_evaluation"]].copy()
    selected = eval_log.loc[eval_log["bot_active_for_trade"], "selected_trade_pnl"]
    rows: list[dict[str, object]] = [_one_sample(selected, "positive_ev_selected_trade_pnl")]

    paired_difference = eval_log["selected_trade_pnl"] - eval_log["trade_pnl"]
    paired = _one_sample(paired_difference, "paired_filtered_minus_always_on")
    paired["interpretation"] = "Positive values improve on always-on at the same opportunity."
    rows.append(paired)

    nonzero = selected[selected != 0]
    if len(nonzero):
        successes = int((nonzero > 0).sum())
        binomial = stats.binomtest(successes, n=len(nonzero), p=0.5, alternative="greater")
        rows.append({
            "test": "exact_binomial_win_rate_greater_than_50pct",
            "n": int(len(nonzero)),
            "wins": successes,
            "win_rate": successes / len(nonzero),
            "one_sided_p_value_greater": float(binomial.pvalue),
        })

    closed = windows[windows["status"] == "closed"] if not windows.empty else windows
    if len(closed):
        window_test = _one_sample(closed["net_pnl"], "window_level_net_pnl")
        window_test["number_independent_regime_windows"] = int(len(closed))
        window_test["window_level_average_return"] = float(closed["net_pnl"].mean())
        window_test["warning"] = "Window-level t-test has low reliability when the number of windows is small."
        rows.append(window_test)
    else:
        rows.append({"test": "window_level_net_pnl", "n": 0, "warning": "No completed regime windows."})

    if len(selected) > 1 and selected.std(ddof=1) > 0:
        d = selected.mean() / selected.std(ddof=1)
        z_alpha = NormalDist().inv_cdf(0.95)  # one-sided alpha 5%
        z_power = NormalDist().inv_cdf(0.80)
        needed = int(np.ceil(((z_alpha + z_power) / abs(d)) ** 2))
        rows.append({
            "test": "estimated_selected_trades_for_80pct_power",
            "observed_standardized_effect": float(d),
            "estimated_total_selected_trades": needed,
            "minimum_additional_selected_trades": max(0, needed - len(selected)),
            "assumption": "Normal independent-trade approximation; serial dependence can increase the requirement.",
        })

    if exploratory_p_values:
        names = list(exploratory_p_values)
        adjusted = _benjamini_hochberg([exploratory_p_values[name] for name in names])
        for name, raw, corrected in zip(names, (exploratory_p_values[name] for name in names), adjusted, strict=True):
            rows.append({
                "test": f"exploratory_{name}_multiple_testing_adjustment",
                "one_sided_p_value_greater": raw,
                "benjamini_hochberg_adjusted_p_value": corrected,
                "warning": "Exploratory threshold result; not a preregistered primary inference.",
            })

    bootstrap_rows = []
    for block in (4, 8, 16, 32):
        record = moving_block_bootstrap(
            paired_difference.to_numpy(), block, config.bootstrap_resamples, config.random_seed
        )
        record["test"] = "moving_block_bootstrap_paired_improvement"
        bootstrap_rows.append(record)
    return pd.DataFrame(rows), pd.DataFrame(bootstrap_rows)


def _pinball(actual: pd.Series, forecast: pd.Series, quantile: float) -> float:
    error = actual.to_numpy(float) - forecast.to_numpy(float)
    return float(np.mean(np.maximum(quantile * error, (quantile - 1) * error)))


def calibration_analysis(primary_log: pd.DataFrame) -> pd.DataFrame:
    """One-step-ahead coverage diagnostics, never historical fitted coverage."""

    frame = primary_log[primary_log["is_walk_forward_evaluation"] & primary_log["p50"].notna()].copy()
    if frame.empty:
        return pd.DataFrame()
    peak = frame["shadow_equity_after"].cummax()
    drawdown = frame["shadow_equity_after"] - peak
    magnitude = -drawdown
    cutoff = magnitude.quantile(0.75)
    frame["drawdown_group"] = np.where(magnitude >= cutoff, "during_top_drawdown_quartile", "outside_top_drawdown_quartile")
    frame["chronological_quartile"] = pd.qcut(np.arange(len(frame)), 4, labels=["Q1", "Q2", "Q3", "Q4"])

    def summarize(part: pd.DataFrame, group_type: str, group: str) -> dict[str, object]:
        result: dict[str, object] = {
            "group_type": group_type,
            "group": group,
            "n": len(part),
            "p01_p99_coverage": part["inside_p01_p99"].astype(float).mean(),
            "p10_p90_coverage": part["inside_p10_p90"].astype(float).mean(),
            "p25_p75_coverage": part["inside_p25_p75"].astype(float).mean(),
            "p50_mae": part["forecast_error_p50"].abs().mean(),
            "p50_rmse": np.sqrt(np.mean(part["forecast_error_p50"] ** 2)),
            "p50_median_absolute_error": part["forecast_error_p50"].abs().median(),
            "mean_signed_error": part["forecast_error_p50"].mean(),
        }
        for name, q in zip(QUANTILE_COLUMNS, QUANTILE_PROBABILITIES, strict=True):
            result[f"{name}_pinball_loss"] = _pinball(part["shadow_equity_after"], part[name], q)
        return result

    rows = [summarize(frame, "overall", "all_walk_forward_forecasts")]
    for group, part in frame.groupby("chronological_quartile", observed=True):
        rows.append(summarize(part, "chronological_quartile", str(group)))
    for active, part in frame.groupby("bot_active_for_trade"):
        rows.append(summarize(part, "state_before_trade", "active" if active else "inactive"))
    for group, part in frame.groupby("drawdown_group"):
        rows.append(summarize(part, "drawdown_period", str(group)))
    return pd.DataFrame(rows)
