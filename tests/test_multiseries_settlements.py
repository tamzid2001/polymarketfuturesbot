from __future__ import annotations

import json
import math
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from types import SimpleNamespace

from kalshi_multiseries_backtest import AuditedLoader, summary
from kalshi_settlement_loader import KalshiSettlementLoader, SettlementMarket, reconstruct_signals


class PublicOnlyReader:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def get(self, url, params):
        self.calls.append((url, params))
        return {"markets": self.rows, "cursor": ""}


class MultiSeriesSettlementTests(unittest.TestCase):
    @staticmethod
    def rows(series="KXETH15M"):
        opened = datetime(2026, 1, 1, tzinfo=UTC)
        return [dict(ticker=f"{series}-{i}", open_time=(opened+timedelta(minutes=15*i)).isoformat(),
                     close_time=(opened+timedelta(minutes=15*(i+1))).isoformat(),
                     settlement_ts=(opened+timedelta(minutes=15*(i+1), seconds=5)).isoformat(),
                     result=result) for i, result in enumerate(("yes", "yes", "no", "no", "yes"))]

    def test_series_parameter_changes_only_namespace_not_signals(self):
        outcomes = []
        for series in ("KXBTC15M", "KXETH15M", "KXGOLD15M"):
            markets = [SettlementMarket.from_api(row, "test", series) for row in self.rows(series)]
            with patch("random.random", side_effect=AssertionError("direction must never be randomized")):
                signals, meta = reconstruct_signals(markets)
            self.assertEqual(meta["eligible_predictions"], 4)
            self.assertEqual([s.predicted_side for s in signals], ["no", "no", "yes", "yes"])
            outcomes.append([s.directional_win for s in signals])
            self.assertTrue(all(s.source_settlement_time <= s.decision_time for s in signals))
        self.assertEqual(outcomes, [[False, True, False, True]]*3)

    def test_primary_never_uses_a_delayed_official_result_early(self):
        rows = self.rows()
        rows[1]["settlement_ts"] = (datetime.fromisoformat(rows[1]["close_time"])+timedelta(seconds=120)).isoformat()
        markets = [SettlementMarket.from_api(row, "test", "KXETH15M") for row in rows]
        signals, _ = reconstruct_signals(markets)
        self.assertEqual(signals[1].source_ticker, "KXETH15M-0")

    def test_public_endpoints_dedupe_and_cache_series_is_checked(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "eth.json"
            reader = PublicOnlyReader(self.rows())
            loader = AuditedLoader(path, "KXETH15M", reader, datetime(2026, 2, 1, tzinfo=UTC))
            markets = loader.refresh()
            self.assertEqual(len(markets), 5)
            self.assertEqual(len(reader.calls), 2)
            self.assertTrue(all(params["series_ticker"] == "KXETH15M" for _, params in reader.calls))
            self.assertEqual(json.loads(path.read_text())["duplicate_rows"], 5)
            self.assertEqual(loader.load(), markets)
            with self.assertRaisesRegex(ValueError, "series mismatch"):
                KalshiSettlementLoader(path).load()

    def test_failed_endpoint_cannot_publish_partial_history(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "eth.json"
            reader = PublicOnlyReader(self.rows())
            loader = AuditedLoader(path, "KXETH15M", reader, datetime(2026, 2, 1, tzinfo=UTC))
            with patch.object(reader, "get", side_effect=[{"markets": self.rows()}, TimeoutError("read failed")]):
                with self.assertRaises(TimeoutError):
                    loader.refresh()
            self.assertFalse(path.exists())

    def test_summary_uses_resolved_actual_results(self):
        markets = [SettlementMarket.from_api(row, "test", "KXETH15M") for row in self.rows()]
        signals, meta = reconstruct_signals(markets)
        value = summary(signals, meta)
        self.assertEqual((value["directional_wins"], value["directional_losses"]), (2, 2))
        self.assertEqual(value["nominal_binomial_p_two_sided"], 1)
        self.assertEqual((value["longest_win_streak"], value["longest_loss_streak"]), (1, 1))

    def test_two_sided_binomial_matches_exact_integer_probability(self):
        # Independent exact combinatorial oracle; no SciPy/runtime dependency.
        for n, wins in ((4, 0), (4, 4), (10, 8), (10, 2), (11, 5), (100, 64)):
            signals = [SimpleNamespace(directional_win=i < wins) for i in range(n)]
            with patch("kalshi_multiseries_backtest.signal_summary", return_value={}):
                value = summary(signals, {})
            expected = min(1, 2*sum(math.comb(n, k) for k in range(min(wins, n-wins)+1))/2**n)
            self.assertAlmostEqual(value["nominal_binomial_p_two_sided"], expected, places=12)

    def test_streak_and_recent_window_denominators(self):
        outcomes = [True]*10 + [False]*3 + [True]*5 + [False]*2
        with patch("kalshi_multiseries_backtest.signal_summary", return_value={}):
            value = summary([SimpleNamespace(directional_win=x) for x in outcomes], {})
        self.assertEqual((value["longest_win_streak"], value["longest_loss_streak"]), (10, 3))
        self.assertEqual((value["current_streak_side"], value["current_streak_length"]), ("L", 2))
        self.assertEqual((value["latest_1000_n"], value["latest_1000_wr"]), (20, .75))
        self.assertEqual((value["first_half_wr"], value["second_half_wr"]), (1, .5))


if __name__ == "__main__":
    unittest.main()
