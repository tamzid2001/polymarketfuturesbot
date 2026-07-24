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
    prior: dict = {}
    if args.path.exists():
        try:
            loaded = json.loads(args.path.read_text(encoding="utf-8"))
            prior = loaded if isinstance(loaded, dict) else {}
        except (OSError, ValueError):
            prior = {}
    previous = {
        "previous_model_run_id": prior.get("active_model_run_id"),
        "previous_model_artifact_name": prior.get("active_model_artifact_name"),
        "previous_ledger_run_id": prior.get("active_ledger_run_id"),
        "previous_ledger_artifact_name": prior.get("active_ledger_artifact_name"),
        "previous_ledger_path": prior.get("active_ledger_path"),
    }
    # Rewriting the registry for the identical model must not manufacture a
    # fictitious transition. Preserve the last real predecessor instead.
    if prior.get("active_model_run_id") == args.model_run_id:
        previous = {
            key: prior.get(key) for key in previous
        }
    payload = {
        "format_version": 2,
        "feature_schema": FEATURE_SCHEMA,
        "retraining_cadence": args.cadence,
        "active_model_run_id": args.model_run_id,
        "active_model_artifact_name": args.artifact_name,
        "active_ledger_run_id": args.model_run_id,
        "active_ledger_artifact_name": args.artifact_name,
        "active_ledger_path": args.ledger_path,
        "updated_at": datetime.now(tz=timezone.utc).isoformat(),
        **{key: value for key, value in previous.items() if value},
    }
    args.path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
