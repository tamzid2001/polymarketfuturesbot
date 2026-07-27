from __future__ import annotations

from pathlib import Path
import unittest
from unittest.mock import patch

import numpy as np
import pandas as pd

from backtest.notebook_reference import build_colab_equity_curve, fit_colab_prophet


SOURCE = Path(__file__).parents[1] / "data" / "closed-positions-2026-07-26.csv"


class _ReferenceProphet:
    """Small deterministic Prophet double that exposes the full future frame."""

    latest: "_ReferenceProphet | None" = None

    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs
        self.training: pd.DataFrame | None = None
        _ReferenceProphet.latest = self

    def fit(self, frame: pd.DataFrame) -> None:
        self.training = frame.copy()

    def make_future_dataframe(self, periods: int, freq: str, include_history: bool) -> pd.DataFrame:
        assert include_history is True
        assert self.training is not None
        future = pd.date_range(self.training["ds"].iloc[-1] + pd.Timedelta(freq), periods=periods, freq=freq)
        return pd.concat([self.training[["ds"]], pd.DataFrame({"ds": future})], ignore_index=True)

    def predictive_samples(self, future: pd.DataFrame) -> dict[str, np.ndarray]:
        # One row per historical-plus-future timestamp verifies that sampling
        # occurs over the same full frame as the supplied Colab code.
        return {"yhat": np.column_stack([np.arange(len(future)), np.arange(len(future)) + 1.0])}


class NotebookReferenceTests(unittest.TestCase):
    def test_account_csv_matches_the_supplied_colab_balance_construction(self) -> None:
        curve = build_colab_equity_curve(SOURCE, starting_balance=100.0)
        self.assertEqual(len(curve), 201)
        self.assertEqual(curve.iloc[0]["ds"], pd.Timestamp("2026-07-23 07:45:00"))
        self.assertEqual(curve.iloc[-1]["ds"], pd.Timestamp("2026-07-26 13:45:45"))
        self.assertAlmostEqual(curve.iloc[0]["y"], 100.0)
        self.assertAlmostEqual(curve.iloc[-1]["y"], 103.23)
        self.assertAlmostEqual(curve["y"].min(), 82.89)
        self.assertAlmostEqual(curve["y"].max(), 197.20)

    def test_prophet_reference_path_samples_history_plus_100_future_rows(self) -> None:
        curve = pd.DataFrame({
            "ds": pd.to_datetime(["2026-07-01 00:00:00", "2026-07-01 00:15:00"]),
            "y": [100.0, 101.0],
        })
        with patch("backtest.notebook_reference.Prophet", _ReferenceProphet):
            output = fit_colab_prophet(curve, forecast_periods=3, uncertainty_samples=2000)
        self.assertEqual(len(output), 5)
        self.assertEqual(int(output["is_future"].sum()), 3)
        self.assertTrue(output.iloc[:2]["actual"].notna().all())
        self.assertTrue(output.iloc[2:]["actual"].isna().all())
        self.assertEqual(_ReferenceProphet.latest.kwargs["uncertainty_samples"], 2000)
        # exp(median([row, row+1])) proves quantiles were transformed after
        # sampling the one full five-row frame, exactly as in the notebook.
        self.assertAlmostEqual(output.iloc[-1]["p50"], float(np.exp(4.5)))


if __name__ == "__main__":
    unittest.main()
