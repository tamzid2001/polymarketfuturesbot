"""Validate and update the active Prophet-free KXBTC15M model registry."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from kalshi_ml_features import FEATURE_SCHEMA


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--path", type=Path, default=Path("kalshi_ml_model_registry.json"))
    parser.add_argument("--model-run-id", required=True)
    parser.add_argument("--artifact-name", required=True)
    parser.add_argument("--ledger-path", required=True)
    parser.add_argument("--cadence", default="1d")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.model_run_id.isdigit():
        raise ValueError("--model-run-id must be a GitHub Actions run ID")
    if not args.artifact_name.startswith("kalshi-kxbtc15m-ml-model-"):
        raise ValueError("--artifact-name is not a KXBTC15M model artifact")
    if "prophet" in args.ledger_path.lower():
        raise ValueError("New active ledgers must use the ML-only filename, not a legacy Prophet filename")
    payload = {
        "format_version": 1,
        "feature_schema": FEATURE_SCHEMA,
        "retraining_cadence": args.cadence,
        "active_model_run_id": args.model_run_id,
        "active_model_artifact_name": args.artifact_name,
        "active_ledger_run_id": args.model_run_id,
        "active_ledger_artifact_name": args.artifact_name,
        "active_ledger_path": args.ledger_path,
        "updated_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    args.path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
