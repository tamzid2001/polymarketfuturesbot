"""Causal Prophet one-step-ahead forecasts and safe predictive quantiles."""

from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Any

import numpy as np
import pandas as pd

# Long sensitivity runs otherwise emit one CmdStan line per refit, obscuring
# the explicit backtest progress messages without adding diagnostic value.
logging.getLogger("cmdstanpy").setLevel(logging.WARNING)

from prophet import Prophet

from .config import BacktestConfig

logging.getLogger("cmdstanpy").setLevel(logging.WARNING)

QUANTILE_PROBABILITIES = (0.01, 0.10, 0.25, 0.50, 0.75, 0.90, 0.99)
QUANTILE_COLUMNS = ("p01", "p10", "p25", "p50", "p75", "p90", "p99")


@dataclass
class ForecastResult:
    quantiles: dict[str, float]
    success: bool
    error: str | None = None


def _as_forecast_by_sample(values: Any, forecast_rows: int) -> np.ndarray:
    """Normalize Prophet sample orientations to ``(forecast_rows, samples)``."""

    samples = np.asarray(values, dtype=float)
    if samples.ndim == 1 and forecast_rows == 1:
        return samples.reshape(1, -1)
    if samples.ndim != 2:
        raise ValueError(f"Unexpected predictive sample dimensions: {samples.shape}")
    if samples.shape[0] == forecast_rows:
        return samples
    if samples.shape[1] == forecast_rows:
        return samples.T
    raise ValueError(
        "Could not align predictive samples with requested forecast timestamps: "
        f"sample shape={samples.shape}, forecast rows={forecast_rows}"
    )


class ProphetForecaster:
    """Fits Prophet only to the caller-provided prior observations."""

    def __init__(self, config: BacktestConfig):
        self.config = config
        self.model: Prophet | None = None
        self.last_fit_index: int | None = None
        self.refit_number = 0

    def fit(self, training: pd.DataFrame, fit_index: int) -> None:
        if len(training) < 2:
            raise ValueError("Prophet requires at least two historical observations")
        if training["ds"].isna().any() or training["y"].isna().any():
            raise ValueError("Training frame contains null ds or y")
        if not training["ds"].is_monotonic_increasing:
            raise ValueError("Training timestamps must be chronological")
        if self.config.use_log_transform and (training["y"] <= 0).any():
            raise ValueError("Log-transform configuration requires all training balances to be positive")

        fit_data = training[["ds", "y"]].copy()
        if self.config.use_log_transform:
            fit_data["y"] = np.log(fit_data["y"])
        np.random.seed(self.config.random_seed + self.refit_number)
        model = Prophet(
            daily_seasonality=True,
            weekly_seasonality=False,
            yearly_seasonality=False,
            changepoint_prior_scale=self.config.changepoint_prior_scale,
            seasonality_prior_scale=self.config.seasonality_prior_scale,
            uncertainty_samples=self.config.uncertainty_samples,
        )
        # Prophet passes the seed to CmdStanPy.  It makes this fit reproducible
        # without changing the information set supplied to the model.
        model.fit(fit_data, seed=self.config.random_seed + self.refit_number)
        self.model = model
        self.last_fit_index = fit_index
        self.refit_number += 1

    def forecast(self, actual_next_trade_timestamp: pd.Timestamp) -> ForecastResult:
        if self.model is None:
            return ForecastResult({}, False, "No previously fitted Prophet model")
        next_timestamp_df = pd.DataFrame({"ds": [actual_next_trade_timestamp]})
        try:
            np.random.seed(self.config.random_seed + self.refit_number)
            sample_output = self.model.predictive_samples(next_timestamp_df)
            if "yhat" not in sample_output:
                raise KeyError(f"predictive_samples did not return yhat: {list(sample_output)}")
            samples = _as_forecast_by_sample(sample_output["yhat"], len(next_timestamp_df))
            if self.config.use_log_transform:
                samples = np.exp(samples)
            values = np.quantile(samples, QUANTILE_PROBABILITIES, axis=1).T[0]
            quantiles = dict(zip(QUANTILE_COLUMNS, map(float, values), strict=True))
            if not all(np.isfinite(list(quantiles.values()))):
                raise ValueError("Non-finite predictive quantile")
            if list(quantiles.values()) != sorted(quantiles.values()):
                raise ValueError("Predictive quantiles were not ordered")
            return ForecastResult(quantiles, True)
        except Exception as exc:  # fit success does not guarantee predictive sampling success.
            return ForecastResult({}, False, f"predictive_samples failed: {type(exc).__name__}: {exc}")
