"""Tests for preserving a real predecessor across ML model registry updates."""

from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import kalshi_ml_model_registry as registry
from kalshi_ml_features import FEATURE_SCHEMA


class ModelRegistryTransitionTests(unittest.TestCase):
    def _update(self, path: Path, run_id: str) -> dict:
        with patch("sys.argv", [
            "kalshi_ml_model_registry.py",
            "--path", str(path),
            "--model-run-id", run_id,
            "--artifact-name", f"kalshi-kxbtc15m-ml-model-{run_id}",
            "--ledger-path", "training_data/kxbtc15m_ml_only_feature_ledger.csv",
        ]):
            self.assertEqual(registry.main(), 0)
        return json.loads(path.read_text(encoding="utf-8"))

    def test_new_active_model_preserves_the_prior_model_for_live_comparison(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "registry.json"
            path.write_text(json.dumps({
                "format_version": 1,
                "feature_schema": FEATURE_SCHEMA,
                "active_model_run_id": "111",
                "active_model_artifact_name": "kalshi-kxbtc15m-ml-model-111",
                "active_ledger_run_id": "111",
                "active_ledger_artifact_name": "kalshi-kxbtc15m-ml-model-111",
                "active_ledger_path": "training_data/kxbtc15m_ml_only_feature_ledger.csv",
            }), encoding="utf-8")
            updated = self._update(path, "222")
            self.assertEqual(updated["format_version"], 2)
            self.assertEqual(updated["active_model_run_id"], "222")
            self.assertEqual(updated["previous_model_run_id"], "111")
            self.assertEqual(updated["previous_model_artifact_name"], "kalshi-kxbtc15m-ml-model-111")

            repeated = self._update(path, "222")
            self.assertEqual(repeated["previous_model_run_id"], "111")


if __name__ == "__main__":
    unittest.main(verbosity=2)
