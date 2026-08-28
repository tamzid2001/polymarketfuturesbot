import unittest

import numpy as np

from backtest.prophet_forecaster import _as_forecast_by_sample


class QuantileShapeTest(unittest.TestCase):
    def test_transposes_sample_by_forecast_orientation(self):
        samples = np.arange(12).reshape(4, 3)
        output = _as_forecast_by_sample(samples, forecast_rows=3)
        self.assertEqual(output.shape, (3, 4))
        self.assertTrue(np.array_equal(output, samples.T))


if __name__ == "__main__":
    unittest.main()
