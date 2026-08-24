"""Best-effort material-event Git checkpointing for the hybrid Actions worker.

The local atomic state file remains the immediate checkpoint.  When running in
GitHub Actions this publisher narrows the runner-loss window by committing only
the configuration, state, and append-only audit files after a material event.
It never blocks order management: a publish failure is recorded for the next
reconciliation/handoff to repair.
"""

from __future__ import annotations

import hashlib
import os
import subprocess
import time
from pathlib import Path


class MaterialCheckpointPublisher:
    def __init__(self, *paths: Path, minimum_interval_seconds: float = 15.0) -> None:
        self.paths = tuple(path.resolve() for path in paths)
        self.enabled = (
            os.getenv("GITHUB_ACTIONS", "").lower() == "true"
            and os.getenv("KALSHI_CHECKPOINT_PUBLISH", "false").lower() in {"1", "true", "yes"}
        )
        self.minimum_interval_seconds = minimum_interval_seconds
        self.last_attempt = float("-inf")
        self.last_fingerprint = ""
        # A material write which arrives within the throttle interval must not
        # wait for a different market event before it is published.  The live
        # loop calls ``publish_if_due`` while maintaining local checkpoints.
        self.pending_reason: str | None = None

    def fingerprint(self) -> str:
        digest = hashlib.sha256()
        for path in self.paths:
            digest.update(str(path).encode("utf-8"))
            try:
                digest.update(path.read_bytes())
            except OSError:
                digest.update(b"<missing>")
        return digest.hexdigest()

    def publish_if_changed(self, reason: str) -> bool:
        fingerprint = self.fingerprint()
        if fingerprint == self.last_fingerprint:
            self.pending_reason = None
            return False
        if not self.enabled:
            self.last_fingerprint = fingerprint
            self.pending_reason = None
            return False
        now = time.monotonic()
        if now - self.last_attempt < self.minimum_interval_seconds:
            self.pending_reason = reason
            return False
        self.last_attempt = now
        try:
            root = Path(subprocess.run(
                ["git", "rev-parse", "--show-toplevel"], check=True, text=True, capture_output=True,
            ).stdout.strip()).resolve()
            relative = [str(path.relative_to(root)) for path in self.paths if path.exists()]
            if not relative:
                return False
            subprocess.run(["git", "add", "--", *relative], check=True, text=True, capture_output=True)
            changed = subprocess.run(["git", "diff", "--cached", "--quiet"], check=False, text=True, capture_output=True)
            if changed.returncode == 0:
                self.last_fingerprint = fingerprint
                return False
            if changed.returncode != 1:
                raise RuntimeError("git diff --cached failed")
            subprocess.run(
                ["git", "commit", "-m", f"chore: checkpoint KXBTC15M hybrid state ({reason}) [skip ci]"],
                check=True, text=True, capture_output=True,
            )
            # Only strategy-owned paths may be retained through a state-file
            # rebase.  If the branch changed elsewhere, leave that conflict to
            # the normal end-of-run publisher rather than overwrite it.
            pull = subprocess.run(
                ["git", "pull", "--rebase", "--autostash", "origin", "main"], check=False, text=True, capture_output=True,
            )
            if pull.returncode:
                subprocess.run(["git", "rebase", "--abort"], check=False, text=True, capture_output=True)
                raise RuntimeError("git pull --rebase failed")
            push = subprocess.run(["git", "push", "origin", "HEAD:main"], check=False, text=True, capture_output=True)
            if push.returncode:
                raise RuntimeError("git push failed")
        except Exception:
            # Keep the reason queued for the next regular worker checkpoint.
            # The local state/ledger is already fsynced; this is only the
            # best-effort remote handoff copy.
            self.pending_reason = reason
            return False
        self.last_fingerprint = fingerprint
        self.pending_reason = None
        return True

    def publish_if_due(self) -> bool:
        """Flush a coalesced material checkpoint once its throttle expires."""

        if self.pending_reason is None:
            return False
        return self.publish_if_changed(self.pending_reason)
