"""Stable, serializable probability-calibration components for Kalshi ML."""

from __future__ import annotations

from typing import Any

import numpy as np
from sklearn.isotonic import IsotonicRegression


class IsotonicCalibratedClassifier:
    """Wrap a binary classifier with a fitted isotonic probability mapping.

    This class deliberately lives in an importable module.  Pickled models
    trained by a script otherwise record the wrapper as ``__main__``, which
    prevents a different live-entry script from loading that model.
    """

    def __init__(self, base_model: Any, calibrator: IsotonicRegression) -> None:
        self.base_model = base_model
        self.calibrator = calibrator

    def predict_proba(self, values: Any) -> np.ndarray:
        raw_probability = self.base_model.predict_proba(values)[:, 1]
        probability = np.clip(self.calibrator.predict(raw_probability), 0.0, 1.0)
        return np.column_stack((1.0 - probability, probability))
