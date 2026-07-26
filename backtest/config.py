"""Configuration shared by the walk-forward backtest modules."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal


SignalMode = Literal["level", "crossing"]


@dataclass(frozen=True)
class BacktestConfig:
    """Primary preregistered configuration unless explicitly overridden.

    A model is always fit only to equity observations that ended before the
    opportunity currently being forecast.  ``refit_every_n_trades`` changes
    how long that already-causal model is re-used; it never allows later rows
    into an earlier forecast.
    """

    starting_balance: float = 100.0
    max_trades: int = 200
    min_training_trades: int = 100
    training_window: int | None = None
    refit_every_n_trades: int = 1
    forecast_frequency: str = "15min"
    uncertainty_samples: int = 2000
    changepoint_prior_scale: float = 0.05
    seasonality_prior_scale: float = 10.0
    use_log_transform: bool = True
    random_seed: int = 42
    initial_bot_state: Literal["on", "off"] = "off"
    signal_mode: SignalMode = "level"
    fallback_policy: Literal["previous_one_trade", "hold_state"] = "previous_one_trade"
    bootstrap_resamples: int = 10_000
    bootstrap_block_length: int = 8
    rolling_window: int = 30
    save_every: int = 25

    def __post_init__(self) -> None:
        if self.starting_balance <= 0:
            raise ValueError("starting_balance must be positive")
        if self.min_training_trades < 2:
            raise ValueError("min_training_trades must be at least 2")
        if self.max_trades <= self.min_training_trades:
            raise ValueError("max_trades must exceed min_training_trades")
        if self.training_window is not None and self.training_window <= 1:
            raise ValueError("training_window must be None or an integer above 1")
        if self.refit_every_n_trades < 1:
            raise ValueError("refit_every_n_trades must be positive")
        if self.uncertainty_samples < 100:
            raise ValueError("uncertainty_samples must be at least 100")
        if self.signal_mode not in {"level", "crossing"}:
            raise ValueError("signal_mode must be 'level' or 'crossing'")

    @property
    def training_window_label(self) -> str:
        return "expanding" if self.training_window is None else str(self.training_window)

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


PRIMARY_CONFIGURATION = BacktestConfig()


def output_paths(output_dir: str | Path) -> dict[str, Path]:
    """Return canonical output file locations without creating any input path."""

    root = Path(output_dir)
    return {
        "trade_log": root / "walk_forward_trade_log.csv",
        "regime_windows": root / "walk_forward_regime_windows.csv",
        "summary": root / "walk_forward_summary.csv",
        "sensitivity": root / "sensitivity_results.csv",
        "statistics": root / "walk_forward_statistics.csv",
        "bootstrap": root / "bootstrap_sensitivity.csv",
        "calibration": root / "forecast_calibration.csv",
        "fit_failures": root / "model_fit_failures.csv",
        "report": root / "walk_forward_backtest_report.md",
        "interactive": root / "walk_forward_interactive.html",
        "integrity": root / "integrity_checks.csv",
    }
