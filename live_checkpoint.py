"""Best-effort material-event checkpointing for the hybrid Actions worker.

The local atomic state file remains the immediate checkpoint.  When running in
GitHub Actions this publisher narrows the runner-loss window by force-updating
one parentless snapshot on the dedicated ``runtime-state-kxbtc15m`` ref. Main
never receives checkpoint commits, and the runtime ref never develops history.
The snapshot contains only explicitly supplied KXBTC15M durable paths plus an
ownership manifest; unrelated files from the source checkout cannot leak into
the branch. It never blocks order management: a publish failure is recorded for
the next reconciliation/handoff to repair.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import tempfile
import time
from pathlib import Path


DEFAULT_RUNTIME_STATE_REF = "runtime-state-kxbtc15m"
RUNTIME_STATE_OWNER = "kalshi-kxbtc15m"
RUNTIME_STATE_MANIFEST = ".kxbtc15m-runtime-state.json"
_ALLOWED_RUNTIME_STATE_REF = re.compile(
    r"runtime-state-(?:kxbtc15m|stop-(?:10|20|25|30|35))\Z"
)
_CANONICAL_RUNTIME_PATHS = frozenset({
    "selected_live_strategy.json",
    "data/kalshi_live_maker_hybrid_v11_state.json",
    "data/kalshi_live_maker_hybrid_v11_audit.jsonl",
    "data/kalshi_shadow_maker_hybrid_v11_sticky_stop_40_state.json",
    "data/kalshi_shadow_maker_hybrid_v11_sticky_stop_40_audit.jsonl",
})


def validate_runtime_ref(runtime_ref: str) -> str:
    """Restrict checkpoint writes to KXBTC15M-owned runtime namespaces."""

    if not _ALLOWED_RUNTIME_STATE_REF.fullmatch(runtime_ref):
        raise ValueError(f"runtime ref is not KXBTC15M-owned: {runtime_ref!r}")
    return runtime_ref


def validate_runtime_paths(runtime_ref: str, relative_paths: list[str]) -> None:
    """Reject any path not owned by the selected KXBTC15M state lane."""

    if runtime_ref == DEFAULT_RUNTIME_STATE_REF:
        allowed = _CANONICAL_RUNTIME_PATHS
    else:
        stop_cents = runtime_ref.rsplit("-", 1)[-1]
        allowed = frozenset({
            "selected_live_strategy.json",
            f"data/kalshi_shadow_maker_hybrid_v11_sticky_stop_{stop_cents}_state.json",
            f"data/kalshi_shadow_maker_hybrid_v11_sticky_stop_{stop_cents}_audit.jsonl",
        })
    unexpected = sorted(set(relative_paths) - allowed)
    if unexpected:
        raise ValueError(
            f"runtime ref {runtime_ref!r} received non-owned durable paths: {unexpected}"
        )


def _run(
    arguments: list[str], *, root: Path, environment: dict[str, str] | None = None,
    input_text: str | None = None, check: bool = True,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        arguments, cwd=root, env=environment, input=input_text, check=check,
        text=True, capture_output=True,
    )


def publish_runtime_snapshot(
    paths: tuple[Path, ...], reason: str, *, runtime_ref: str = DEFAULT_RUNTIME_STATE_REF,
    root: Path | None = None,
) -> bool:
    """Publish one root snapshot with an exact lease; never extend Git history."""

    runtime_ref = validate_runtime_ref(runtime_ref)
    if root is None:
        root = Path(_run(["git", "rev-parse", "--show-toplevel"], root=Path.cwd()).stdout.strip())
    root = root.resolve()
    relative = [str(path.resolve().relative_to(root)) for path in paths if path.exists()]
    if not relative:
        return False
    validate_runtime_paths(runtime_ref, relative)
    remote_ref = f"refs/heads/{runtime_ref}"
    prior = _run(
        ["git", "ls-remote", "--heads", "origin", remote_ref], root=root,
    ).stdout.strip().split()
    prior_sha = prior[0] if prior else ""

    descriptor, index_name = tempfile.mkstemp(prefix="kalshi-runtime-index-")
    os.close(descriptor)
    os.unlink(index_name)
    environment = dict(os.environ, GIT_INDEX_FILE=index_name)
    try:
        # A runtime branch is durable data, not another copy of the repository.
        # Starting empty prevents unrelated tracked files (including another
        # bot's state.json) from being mistaken for KXBTC15M-owned state.
        _run(["git", "read-tree", "--empty"], root=root, environment=environment)
        for relative_path in relative:
            blob = _run(
                ["git", "hash-object", "-w", "--", relative_path], root=root,
            ).stdout.strip()
            _run(
                ["git", "update-index", "--add", "--cacheinfo", f"100644,{blob},{relative_path}"],
                root=root, environment=environment,
            )
        manifest = json.dumps(
            {
                "schema_version": 1,
                "owner": RUNTIME_STATE_OWNER,
                "runtime_ref": runtime_ref,
                "paths": sorted(relative),
            },
            sort_keys=True,
            separators=(",", ":"),
        ) + "\n"
        manifest_blob = _run(
            ["git", "hash-object", "-w", "--stdin"], root=root,
            input_text=manifest,
        ).stdout.strip()
        _run(
            [
                "git", "update-index", "--add", "--cacheinfo",
                f"100644,{manifest_blob},{RUNTIME_STATE_MANIFEST}",
            ],
            root=root, environment=environment,
        )
        tree = _run(["git", "write-tree"], root=root, environment=environment).stdout.strip()
        commit = _run(
            ["git", "commit-tree", tree], root=root, environment=environment,
            input_text=f"runtime: KXBTC15M durable snapshot ({reason})\n",
        ).stdout.strip()
        lease = f"--force-with-lease={remote_ref}:{prior_sha}"
        pushed = _run(
            ["git", "push", lease, "origin", f"{commit}:{remote_ref}"],
            root=root, environment=environment, check=False,
        )
        if pushed.returncode:
            raise RuntimeError(f"runtime-state push failed for {runtime_ref}")
    finally:
        try:
            os.unlink(index_name)
        except OSError:
            pass
    return True


class MaterialCheckpointPublisher:
    def __init__(self, *paths: Path, minimum_interval_seconds: float = 15.0) -> None:
        self.paths = tuple(path.resolve() for path in paths)
        self.enabled = (
            os.getenv("GITHUB_ACTIONS", "").lower() == "true"
            and os.getenv("KALSHI_CHECKPOINT_PUBLISH", "false").lower() in {"1", "true", "yes"}
        )
        self.minimum_interval_seconds = minimum_interval_seconds
        self.runtime_ref = os.getenv("KALSHI_RUNTIME_STATE_REF", DEFAULT_RUNTIME_STATE_REF)
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
            publish_runtime_snapshot(self.paths, reason, runtime_ref=self.runtime_ref)
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


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Publish a bounded Kalshi runtime-state snapshot.")
    parser.add_argument("--reason", required=True)
    parser.add_argument("--runtime-ref", default=DEFAULT_RUNTIME_STATE_REF)
    parser.add_argument("paths", nargs="+")
    arguments = parser.parse_args()
    publish_runtime_snapshot(
        tuple(Path(path) for path in arguments.paths), arguments.reason,
        runtime_ref=arguments.runtime_ref,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
