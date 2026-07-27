"""Exact command-line implementation of the user's two Colab cells.

This module intentionally preserves the Colab procedure used to produce
``kalshi_equity_curve_metaprophet.csv`` and
``kalshi_equity_prophet_quantiles.csv``:

* parse the KXBTC15M ticker clock time;
* start at the supplied historical balance exactly once;
* fit one Prophet model to the complete selected balance curve;
* call ``make_future_dataframe(..., include_history=True)``; and
* call ``predictive_samples`` on that *full* historical-plus-future frame
  after setting NumPy's seed immediately before sampling.

It is deliberately separate from ``run_backtest``.  The latter is a causal
walk-forward evaluator; this module reproduces the visible in-sample/future
notebook artifact exactly, without relabelling fitted historical intervals as
out-of-sample evidence.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
from prophet import Prophet

from .data_loader import KALSHI_TICKER_RE, RETURN_CANDIDATES
from .prophet_forecaster import QUANTILE_COLUMNS, QUANTILE_PROBABILITIES, _as_forecast_by_sample


def _as_utc_naive(values: pd.Series) -> pd.Series:
    """Parse UTC input and remove the timezone without changing its clock."""

    return pd.to_datetime(values, errors="coerce", utc=True).dt.tz_convert(None)


def _parse_ticker_clock(tickers: pd.Series) -> pd.Series:
    """Use the same regex and timestamp construction as the supplied Colab."""

    parts = tickers.astype(str).str.extract(KALSHI_TICKER_RE)
    parts.columns = ["year", "month", "day", "hour", "minute", "second"]
    timestamp_text = (
        parts["year"] + parts["month"] + parts["day"]
        + parts["hour"] + parts["minute"] + parts["second"]
    )
    return pd.to_datetime(timestamp_text, format="%y%b%d%H%M%S", errors="coerce", utc=True).dt.tz_convert(None)


def build_colab_equity_curve(path: str | Path, starting_balance: float = 100.0) -> pd.DataFrame:
    """Reproduce the first supplied Colab cell for an original Kalshi CSV."""

    source = pd.read_csv(path)
    source.columns = source.columns.str.strip()
    if "Ticker" not in source.columns:
        raise KeyError(f"Missing 'Ticker'; available columns: {source.columns.tolist()}")
    return_column = next((column for column in RETURN_CANDIDATES if column in source.columns), None)
    if return_column is None:
        raise KeyError(f"Could not locate the total-return column; available columns: {source.columns.tolist()}")

    source["ds"] = _parse_ticker_clock(source["Ticker"])
    source["trade_return"] = pd.to_numeric(
        source[return_column].astype(str).str.replace(r"[^0-9.\-]", "", regex=True),
        errors="coerce",
    )
    equity = (
        source.loc[source["ds"].notna() & source["trade_return"].notna(), ["ds", "Ticker", "trade_return"]]
        .sort_values("ds")
        .drop_duplicates(subset=["Ticker"], keep="last")
        .reset_index(drop=True)
    )
    if equity.empty:
        raise ValueError("No valid trades remained after cleaning")
    equity["y"] = float(starting_balance) + equity["trade_return"].cumsum()
    starting_row = pd.DataFrame({
        "ds": [equity["ds"].iloc[0] - pd.Timedelta(minutes=15)],
        "Ticker": ["STARTING_BALANCE"],
        "trade_return": [0.0],
        "y": [float(starting_balance)],
    })
    return (
        pd.concat([starting_row, equity], ignore_index=True)[["ds", "y"]]
        .sort_values("ds")
        .drop_duplicates(subset=["ds"], keep="last")
        .reset_index(drop=True)
    )


def load_colab_equity_curve(path: str | Path) -> pd.DataFrame:
    """Load an already-created ``ds,y`` curve exactly as the Colab does."""

    frame = pd.read_csv(path)
    frame.columns = frame.columns.str.strip()
    if not {"ds", "y"}.issubset(frame.columns):
        raise KeyError(f"CSV must contain ds and y; available columns: {frame.columns.tolist()}")
    frame["ds"] = _as_utc_naive(frame["ds"])
    frame["y"] = pd.to_numeric(frame["y"], errors="coerce")
    frame = (
        frame.dropna(subset=["ds", "y"])
        .sort_values("ds")
        .drop_duplicates(subset=["ds"], keep="last")
        .reset_index(drop=True)
    )
    if frame.empty:
        raise ValueError("No valid rows remained after cleaning")
    return frame[["ds", "y"]]


def fit_colab_prophet(
    equity: pd.DataFrame,
    *,
    forecast_periods: int = 100,
    forecast_frequency: str = "15min",
    changepoint_prior_scale: float = 0.05,
    seasonality_prior_scale: float = 10.0,
    uncertainty_samples: int = 2000,
    random_seed: int = 42,
) -> pd.DataFrame:
    """Reproduce the second supplied Colab cell without rebasing any balance."""

    curve = equity[["ds", "y"]].copy()
    curve["ds"] = _as_utc_naive(curve["ds"])
    curve["y"] = pd.to_numeric(curve["y"], errors="coerce")
    curve = (
        curve.dropna(subset=["ds", "y"])
        .sort_values("ds")
        .drop_duplicates(subset=["ds"], keep="last")
        .reset_index(drop=True)
    )
    if curve.empty:
        raise ValueError("No valid equity rows remained")
    if (curve["y"] <= 0).any():
        raise ValueError("All selected equity values must be above zero for the logarithmic transformation")

    prophet_df = curve[["ds", "y"]].copy()
    prophet_df["y"] = np.log(prophet_df["y"])
    model = Prophet(
        daily_seasonality=True,
        weekly_seasonality=False,
        yearly_seasonality=False,
        changepoint_prior_scale=changepoint_prior_scale,
        seasonality_prior_scale=seasonality_prior_scale,
        uncertainty_samples=uncertainty_samples,
    )
    # The supplied notebook does not pass a fit seed.  Keeping that behavior
    # is essential for same-environment numerical reproduction.
    model.fit(prophet_df)
    last_actual_time = curve["ds"].max()
    future = model.make_future_dataframe(
        periods=forecast_periods,
        freq=forecast_frequency,
        include_history=True,
    )
    # This is deliberately immediately before predictive_samples, matching
    # the supplied notebook rather than seeding the earlier model fit.
    np.random.seed(random_seed)
    sample_output = model.predictive_samples(future)
    if "yhat" not in sample_output:
        raise KeyError(f"predictive_samples did not return yhat: {list(sample_output)}")
    samples_log = _as_forecast_by_sample(sample_output["yhat"], len(future))
    quantiles_log = np.quantile(samples_log, QUANTILE_PROBABILITIES, axis=1).T
    forecast = pd.DataFrame(quantiles_log, columns=QUANTILE_COLUMNS)
    forecast.insert(0, "ds", future["ds"].to_numpy())
    forecast[list(QUANTILE_COLUMNS)] = np.exp(forecast[list(QUANTILE_COLUMNS)])
    output = (
        forecast.merge(curve.rename(columns={"y": "actual"}), on="ds", how="left")
        .sort_values("ds")
        .reset_index(drop=True)
    )
    output["is_future"] = output["ds"] > last_actual_time
    return output


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Original Kalshi closed-position CSV or ds,y balance curve")
    parser.add_argument("--input-format", choices=("kalshi", "equity"), default="kalshi")
    parser.add_argument("--starting-balance", type=float, default=100.0)
    parser.add_argument("--output-dir", default="outputs/notebook_reference")
    parser.add_argument("--forecast-periods", type=int, default=100)
    parser.add_argument("--forecast-frequency", default="15min")
    parser.add_argument("--changepoint-prior-scale", type=float, default=0.05)
    parser.add_argument("--seasonality-prior-scale", type=float, default=10.0)
    parser.add_argument("--uncertainty-samples", type=int, default=2000)
    parser.add_argument("--random-seed", type=int, default=42)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    equity = (
        build_colab_equity_curve(args.input, args.starting_balance)
        if args.input_format == "kalshi"
        else load_colab_equity_curve(args.input)
    )
    quantiles = fit_colab_prophet(
        equity,
        forecast_periods=args.forecast_periods,
        forecast_frequency=args.forecast_frequency,
        changepoint_prior_scale=args.changepoint_prior_scale,
        seasonality_prior_scale=args.seasonality_prior_scale,
        uncertainty_samples=args.uncertainty_samples,
        random_seed=args.random_seed,
    )
    equity_path = output_dir / "kalshi_equity_curve_metaprophet.csv"
    quantile_path = output_dir / "kalshi_equity_prophet_quantiles.csv"
    equity.to_csv(equity_path, index=False, date_format="%Y-%m-%d %H:%M:%S")
    quantiles.to_csv(quantile_path, index=False, date_format="%Y-%m-%d %H:%M:%S")
    history = quantiles[quantiles["actual"].notna()]
    print(f"Equity rows: {len(equity)}")
    print(f"Starting balance: ${equity['y'].iloc[0]:.2f}")
    print(f"Ending balance: ${equity['y'].iloc[-1]:.2f}")
    print(f"P01-P99 coverage: {((history.actual >= history.p01) & (history.actual <= history.p99)).mean():.2%}")
    print(f"P10-P90 coverage: {((history.actual >= history.p10) & (history.actual <= history.p90)).mean():.2%}")
    print(f"P25-P75 coverage: {((history.actual >= history.p25) & (history.actual <= history.p75)).mean():.2%}")
    print(f"Wrote {equity_path}")
    print(f"Wrote {quantile_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
