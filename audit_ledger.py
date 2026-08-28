"""Append-only JSONL audit ledger with atomic local writes."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def append_audit(path: Path, event: dict[str, Any]) -> None:
    existed = path.exists()
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {"recorded_at": datetime.now(timezone.utc).isoformat(), **event}
    # JSON Lines writes are append-only. The Actions concurrency singleton
    # prevents two workers from appending concurrently; state remains the
    # idempotency authority if a runner dies between ledger and checkpoint.
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True, default=str) + "\n")
        handle.flush()
        # ``flush`` only reaches Python's buffered file object.  An audit
        # record is part of the live-trading safety boundary, so make the
        # append durable before returning to order/position management.
        os.fsync(handle.fileno())
    if not existed:
        # The first append also creates a directory entry.  Sync the parent
        # once so a runner/host crash cannot leave a state file referring to
        # a ledger whose creation was never persisted.
        try:
            directory = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        except OSError:
            # File data has already been fsynced.  Some platforms do not
            # permit fsync on a directory, so do not mask a valid append.
            pass
