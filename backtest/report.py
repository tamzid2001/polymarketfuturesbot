"""Evidence-focused Markdown report generated from saved walk-forward outputs."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from .config import BacktestConfig


def _fmt(value: object) -> str:
    if isinstance(value, (float, np.floating)):
        if not np.isfinite(value):
            return "NA" if np.isnan(value) else "∞"
        return f"{value:.4f}"
    if isinstance(value, (pd.Timestamp, pd.Timedelta)):
        return str(value)
    if pd.isna(value):
        return "NA"
    return str(value)


def markdown_table(frame: pd.DataFrame, columns: list[str] | None = None, max_rows: int = 30) -> str:
    if frame.empty:
        return "_No rows._"
    table = frame if columns is None else frame[[column for column in columns if column in frame.columns]]
    table = table.head(max_rows)
    headers = list(table.columns)
    divider = ["---"] * len(headers)
    rows = [headers, divider] + [[_fmt(value) for value in row] for row in table.itertuples(index=False, name=None)]
    return "\n".join("| " + " | ".join(row) + " |" for row in rows)


def _recommendation(primary: pd.Series, paired: pd.Series, calibration: pd.DataFrame) -> tuple[str, str]:
    net = float(primary.get("net_pnl", np.nan))
    dd = float(primary.get("maximum_drawdown_dollars", np.nan))
    p = float(paired.get("one_sided_p_value_greater", np.nan))
    coverage = np.nan
    if not calibration.empty:
        overall = calibration[calibration["group_type"] == "overall"]
        if not overall.empty:
            coverage = float(overall.iloc[0]["p10_p90_coverage"])
    if np.isfinite(net) and net <= 0:
        return "Reject the rule", "The true walk-forward filtered strategy lost money over the evaluation opportunities."
    if np.isfinite(p) and p < .05 and np.isfinite(coverage) and abs(coverage - .80) <= .15:
        return "Paper trade", "The observed improvement is statistically supported, but the sample and regime dependence still do not justify normal live deployment."
    return "Continue collecting evidence", "The data do not provide sufficiently robust out-of-sample evidence for live deployment at normal size."


def write_report(
    path: str | Path,
    config: BacktestConfig,
    summary: pd.DataFrame,
    windows: pd.DataFrame,
    calibration: pd.DataFrame,
    statistics: pd.DataFrame,
    bootstrap: pd.DataFrame,
    sensitivity: pd.DataFrame,
    fit_failures: pd.DataFrame,
    integrity: pd.DataFrame,
) -> None:
    primary = summary.loc[summary["strategy"] == "p10_p90"].iloc[0]
    always = summary.loc[summary["strategy"] == "always_on"].iloc[0]
    paired = statistics.loc[statistics["test"] == "paired_filtered_minus_always_on"].iloc[0]
    recommendation, rationale = _recommendation(primary, paired, calibration)
    drawdown_reduced = primary["maximum_drawdown_dollars"] < always["maximum_drawdown_dollars"]
    beat_always = primary["net_pnl"] > always["net_pnl"]
    coverage = calibration.loc[calibration["group_type"] == "overall"] if not calibration.empty else pd.DataFrame()
    coverage_text = "NA" if coverage.empty else f"{coverage.iloc[0]['p10_p90_coverage']:.1%}"
    significant = bool(np.isfinite(paired.get("one_sided_p_value_greater", np.nan)) and paired["one_sided_p_value_greater"] < .05)

    text = f"""# Kalshi Prophet Equity-Curve Walk-Forward Backtest

## Executive summary

Primary preregistered configuration: `MIN_TRAINING_TRADES={config.min_training_trades}`, `TRAINING_WINDOW={config.training_window_label}`, `REFIT_EVERY_N_TRADES={config.refit_every_n_trades}`, `CHANGEPOINT_PRIOR_SCALE={config.changepoint_prior_scale}`, `SIGNAL_MODE={config.signal_mode}`, and P10-entry/P90-exit.

- P10/P90 {'beat' if beat_always else 'did not beat'} always-on on the true one-step-ahead evaluation (`${primary['net_pnl']:.2f}` versus `${always['net_pnl']:.2f}` OOS net P/L).
- It {'reduced' if drawdown_reduced else 'did not reduce'} maximum drawdown (`${primary['maximum_drawdown_dollars']:.2f}` versus `${always['maximum_drawdown_dollars']:.2f}`).
- The valid paired one-sided OOS p-value is `{paired.get('one_sided_p_value_greater', np.nan):.4f}`; the result is {'statistically significant at 5%' if significant else 'not statistically significant at 5%'}. This statement is based on opportunity-level walk-forward differences, not fitted history.
- P10-P90 empirical one-step-ahead coverage is {coverage_text}; well-calibrated 80% bands would be near 80%.
- Completed regime windows: `{primary['number_of_completed_regime_windows']}`. Interpret any window-level result cautiously when that count is small.
- Recommendation: **{recommendation}** — {rationale}

## Methodology

The input is parsed as UTC and timezone information is removed only after conversion, preserving UTC clock time for Prophet.  For original Kalshi positions, ticker timestamps are parsed with the documented `KXBTC15M` regex and realized `Total return ($)` forms the P/L series. Confirmed duplicate economic records only are removed; unresolved duplicate timestamps stop the run.

The always-on **shadow equity** is starting balance plus every realized opportunity P/L, including results during inactive periods. The filtered curve includes only opportunities whose state was already on before that opportunity. At trade index *t*, the model is trained on shadow-equity rows ending at *t-1*, forecasts the exact timestamp of *t*, then the outcome is revealed. The P10/P90 comparison changes state only for *t+1*. This sequencing prevents the trade generating a signal from being counted in the new regime.

Prophet is refit on each trade for the primary configuration, uses daily seasonality only, and obtains exact P01/P10/P25/P50/P75/P90/P99 from `predictive_samples`. With the log setting, only positive balances are fitted and samples are transformed back to dollars before quantiles. A failed fit may use the immediately preceding causal forecast for one trade; all failures are separately recorded.

## Results

### Strategy comparison (true walk-forward evaluation only)

{markdown_table(summary, ['strategy','exploratory','total_opportunities','trades_taken','time_in_market_pct','net_pnl','ending_balance','maximum_drawdown_dollars','win_rate','profit_factor','pnl_of_skipped_trades'])}

### Regime windows

{markdown_table(windows, ['window_number','entry_signal_timestamp','first_traded_timestamp','exit_signal_timestamp','status','number_of_selected_trades','win_rate','net_pnl','maximum_window_drawdown'])}

### Forecast calibration

{markdown_table(calibration, ['group_type','group','n','p01_p99_coverage','p10_p90_coverage','p25_p75_coverage','p50_mae','p50_rmse','mean_signed_error'])}

### Statistical tests

{markdown_table(statistics, ['test','n','mean','standard_deviation','standard_error','t_statistic','one_sided_p_value_greater','two_sided_p_value','ci95_lower','ci95_upper','warning'])}

### Moving-block bootstrap sensitivity

{markdown_table(bootstrap, ['block_length','resamples','ci95_lower','ci95_upper','p_value_greater'])}

### Exploratory sensitivity analysis

The rows below are one-factor-at-a-time variations around the declared primary configuration. They are sensitivity checks, not a threshold optimization exercise and not unbiased confirmatory evidence.

{markdown_table(sensitivity, ['sensitivity_axis','parameter_value','selected_trades','net_pnl','ending_balance','maximum_drawdown','win_rate','profit_factor','one_sided_p_value','paired_improvement_p_value','p10_p90_coverage','p50_mae','number_of_regime_windows'])}

## Interpretation

Low-equity deviations can only be described as mean reversion if the selected trades, skipped trades, and paired opportunity differences jointly support it; the comparison table and cumulative-skipped-P/L chart expose whether the rule simply selected a few profitable clusters. P90 exits may protect gains only if they lower drawdown without eliminating more profitable opportunities than they avoid. Long inactive periods appear directly in time-in-market and skipped P/L. Forecast calibration and the P50 error chart show whether Prophet adapted quickly enough and whether every-trade refitting produced unstable bands.

## Limitations

This is a small, serially dependent sample of changing trade sizes and a nonstationary bot, not an ordinary market-price series. P/L is non-Gaussian and equity observations are cumulative, so simple t-tests are only one diagnostic; moving-block bootstrap results are included for this reason. Stops, fill quality, latency, fees, and execution constraints may materially change realized results. The primary parameters are identified above, while all threshold/window/refit alternatives are exploratory and multiple-tested. The report does not claim an in-sample fitted band is predictive. Irregular timestamps are retained exactly. Fit failures are reported below. Repeated Prophet fitting is computationally expensive.

### Model robustness and integrity

- Fit/prediction failures: `{len(fit_failures)}`.
- One-trade causal forecast fallbacks used: `{int(primary.get('model_fallback_count', 0))}`.
- Fallback policy: `{config.fallback_policy}`.
- All automated integrity checks passed: `{bool(integrity['passed'].all())}`.

{markdown_table(integrity)}

## Recommendation

**{recommendation}.** {rationale} The decision is driven by the primary OOS P/L, drawdown, paired test, calibration, and the count of independent regime windows shown above—not by sensitivity winners or in-sample fitted values.
"""
    Path(path).write_text(text, encoding="utf-8")
