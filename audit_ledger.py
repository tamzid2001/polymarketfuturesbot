"""Append-only JSONL audit ledger with atomic local writes."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def append_audit(path: Path, event: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {"recorded_at": datetime.now(timezone.utc).isoformat(), **event}
    # JSON Lines writes are append-only. The Actions concurrency singleton
    # prevents two workers from appending concurrently; state remains the
    # idempotency authority if a runner dies between ledger and checkpoint.
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True, default=str) + "\n")
        handle.flush()
