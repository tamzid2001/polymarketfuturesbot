"""Bounded, restart-safe checkpoints for the KXBTC15M Actions workers.

The local atomic state and fsynced JSONL ledger remain the immediate source of
truth.  GitHub Actions additionally force-updates one parentless commit on a
dedicated runtime ref.  Runtime snapshot schema v2 stores each durable file as
deterministic gzip chunks.  This avoids GitHub's 100 MiB single-blob limit and
lets Git reuse unchanged historical ledger chunks instead of uploading the
entire append-only ledger after every material event.

Only explicitly allow-listed KXBTC15M paths can enter a runtime snapshot.  The
restore path validates ownership, compressed and uncompressed hashes, sizes,
and the source-path allow-list before atomically replacing local files.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import re
import subprocess
import tempfile
import time
from pathlib import Path
from typing import BinaryIO


DEFAULT_RUNTIME_STATE_REF = "runtime-state-kxbtc15m"
RUNTIME_STATE_OWNER = "kalshi-kxbtc15m"
RUNTIME_STATE_MANIFEST = ".kxbtc15m-runtime-state.json"
RUNTIME_PAYLOAD_PREFIX = ".kxbtc15m-runtime-payload"
RUNTIME_STATE_SCHEMA_VERSION = 2
RUNTIME_CHUNK_BYTES = 8 * 1024 * 1024
RUNTIME_CACHE_MAX_FILES = 40
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


def _git_path(root: Path, name: str) -> Path:
    value = Path(_run(["git", "rev-parse", "--git-path", name], root=root).stdout.strip())
    if not value.is_absolute():
        value = root / value
    return value.resolve()


def _cache_directory(root: Path) -> Path:
    path = _git_path(root, "kxbtc15m-runtime-compressed-cache")
    path.mkdir(parents=True, exist_ok=True)
    return path


def _compressed_chunk(raw: bytes, cache: Path) -> tuple[Path, str]:
    raw_digest = hashlib.sha256(raw).hexdigest()
    cached = cache / f"{raw_digest}.gz"
    if not cached.exists():
        descriptor, temporary_name = tempfile.mkstemp(prefix="chunk-", suffix=".gz", dir=cache)
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(gzip.compress(raw, compresslevel=6, mtime=0))
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary_name, cached)
        finally:
            try:
                os.unlink(temporary_name)
            except OSError:
                pass
    os.utime(cached, None)
    return cached, hashlib.sha256(cached.read_bytes()).hexdigest()


def _prune_cache(cache: Path, keep: set[Path]) -> None:
    candidates = sorted(
        (item for item in cache.glob("*.gz") if item not in keep),
        key=lambda item: item.stat().st_mtime_ns,
        reverse=True,
    )
    for item in candidates[max(0, RUNTIME_CACHE_MAX_FILES - len(keep)):]:
        try:
            item.unlink()
        except OSError:
            pass


def _snapshot_file(
    source: Path, relative_path: str, *, root: Path, cache: Path,
    environment: dict[str, str], indexed_payloads: set[str],
) -> tuple[dict[str, object], set[Path]]:
    source_digest = hashlib.sha256()
    source_size = 0
    chunks: list[dict[str, object]] = []
    cache_files: set[Path] = set()
    with source.open("rb") as handle:
        while True:
            raw = handle.read(RUNTIME_CHUNK_BYTES)
            if not raw and chunks:
                break
            source_digest.update(raw)
            source_size += len(raw)
            cached, compressed_digest = _compressed_chunk(raw, cache)
            cache_files.add(cached)
            payload_path = f"{RUNTIME_PAYLOAD_PREFIX}/{compressed_digest}.gz"
            if payload_path not in indexed_payloads:
                blob = _run(
                    ["git", "hash-object", "-w", "--", str(cached)], root=root,
                ).stdout.strip()
                _run(
                    ["git", "update-index", "--add", "--cacheinfo", f"100644,{blob},{payload_path}"],
                    root=root, environment=environment,
                )
                indexed_payloads.add(payload_path)
            chunks.append({
                "path": payload_path,
                "compressed_size": cached.stat().st_size,
                "compressed_sha256": compressed_digest,
                "uncompressed_size": len(raw),
                "uncompressed_sha256": hashlib.sha256(raw).hexdigest(),
            })
            if not raw:
                break
    return ({
        "path": relative_path,
        "size": source_size,
        "sha256": source_digest.hexdigest(),
        "encoding": "independent_gzip_chunks",
        "chunks": chunks,
    }, cache_files)


def publish_runtime_snapshot(
    paths: tuple[Path, ...], reason: str, *, runtime_ref: str = DEFAULT_RUNTIME_STATE_REF,
    root: Path | None = None,
) -> bool:
    """Publish one parentless, chunked snapshot with an exact force lease."""

    runtime_ref = validate_runtime_ref(runtime_ref)
    if root is None:
        root = Path(_run(["git", "rev-parse", "--show-toplevel"], root=Path.cwd()).stdout.strip())
    root = root.resolve()
    resolved: list[tuple[Path, str]] = []
    for path in paths:
        if not path.exists():
            continue
        absolute = path.resolve()
        relative = str(absolute.relative_to(root))
        resolved.append((absolute, relative))
    if not resolved:
        return False
    relative_paths = [relative for _, relative in resolved]
    validate_runtime_paths(runtime_ref, relative_paths)
    remote_ref = f"refs/heads/{runtime_ref}"
    prior = _run(["git", "ls-remote", "--heads", "origin", remote_ref], root=root).stdout.strip().split()
    prior_sha = prior[0] if prior else ""

    descriptor, index_name = tempfile.mkstemp(prefix="kalshi-runtime-index-")
    os.close(descriptor)
    os.unlink(index_name)
    environment = dict(os.environ, GIT_INDEX_FILE=index_name)
    cache = _cache_directory(root)
    used_cache_files: set[Path] = set()
    try:
        _run(["git", "read-tree", "--empty"], root=root, environment=environment)
        indexed_payloads: set[str] = set()
        files: list[dict[str, object]] = []
        for source, relative_path in sorted(resolved, key=lambda item: item[1]):
            descriptor_value, used = _snapshot_file(
                source, relative_path, root=root, cache=cache,
                environment=environment, indexed_payloads=indexed_payloads,
            )
            files.append(descriptor_value)
            used_cache_files.update(used)
        manifest = json.dumps(
            {
                "schema_version": RUNTIME_STATE_SCHEMA_VERSION,
                "owner": RUNTIME_STATE_OWNER,
                "runtime_ref": runtime_ref,
                "paths": sorted(relative_paths),
                "chunk_bytes": RUNTIME_CHUNK_BYTES,
                "files": files,
            },
            sort_keys=True,
            separators=(",", ":"),
        ) + "\n"
        manifest_blob = _run(
            ["git", "hash-object", "-w", "--stdin"], root=root, input_text=manifest,
        ).stdout.strip()
        _run(
            ["git", "update-index", "--add", "--cacheinfo", f"100644,{manifest_blob},{RUNTIME_STATE_MANIFEST}"],
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
            diagnostic = "\n".join(
                part.strip() for part in (pushed.stdout, pushed.stderr) if part.strip()
            )
            raise RuntimeError(
                f"runtime-state push failed for {runtime_ref} (exit={pushed.returncode}): {diagnostic[-4000:]}"
            )
    finally:
        try:
            os.unlink(index_name)
        except OSError:
            pass
        _prune_cache(cache, used_cache_files)
    return True


def _git_blob_bytes(root: Path, revision: str, path: str) -> bytes:
    result = subprocess.run(
        ["git", "show", f"{revision}:{path}"], cwd=root,
        check=False, capture_output=True,
    )
    if result.returncode:
        diagnostic = result.stderr.decode("utf-8", errors="replace")[-2000:]
        raise RuntimeError(f"cannot read runtime payload {path!r}: {diagnostic}")
    return result.stdout


def _atomic_restore_target(root: Path, relative_path: str) -> tuple[Path, BinaryIO, str]:
    target = (root / relative_path).resolve()
    target.relative_to(root)
    target.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{target.name}.", dir=target.parent)
    return target, os.fdopen(descriptor, "wb"), temporary_name


def restore_runtime_snapshot(
    revision: str, *, runtime_ref: str = DEFAULT_RUNTIME_STATE_REF,
    root: Path | None = None,
) -> list[str]:
    """Verify and atomically restore a schema-v1 or schema-v2 runtime commit."""

    runtime_ref = validate_runtime_ref(runtime_ref)
    if root is None:
        root = Path(_run(["git", "rev-parse", "--show-toplevel"], root=Path.cwd()).stdout.strip())
    root = root.resolve()
    try:
        manifest = json.loads(_git_blob_bytes(root, revision, RUNTIME_STATE_MANIFEST))
    except (UnicodeDecodeError, ValueError) as exc:
        raise RuntimeError(f"invalid runtime-state manifest: {exc}") from exc
    if manifest.get("owner") != RUNTIME_STATE_OWNER or manifest.get("runtime_ref") != runtime_ref:
        raise RuntimeError("runtime-state ownership/ref validation failed")
    paths = manifest.get("paths")
    if not isinstance(paths, list) or paths != sorted(paths) or not all(isinstance(item, str) for item in paths):
        raise RuntimeError("runtime-state manifest paths are not a sorted string list")
    validate_runtime_paths(runtime_ref, paths)
    schema_version = int(manifest.get("schema_version", 0))
    restored: list[str] = []
    if schema_version == 1:
        for relative_path in paths:
            payload = _git_blob_bytes(root, revision, relative_path)
            target, handle, temporary_name = _atomic_restore_target(root, relative_path)
            try:
                with handle:
                    handle.write(payload)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temporary_name, target)
            finally:
                try:
                    os.unlink(temporary_name)
                except OSError:
                    pass
            restored.append(relative_path)
        return restored
    if schema_version != RUNTIME_STATE_SCHEMA_VERSION:
        raise RuntimeError(f"unsupported runtime-state schema: {schema_version}")
    files = manifest.get("files")
    if not isinstance(files, list) or [item.get("path") for item in files if isinstance(item, dict)] != paths:
        raise RuntimeError("runtime-state file descriptors do not match manifest paths")
    for file_descriptor in files:
        relative_path = file_descriptor["path"]
        chunks = file_descriptor.get("chunks")
        if file_descriptor.get("encoding") != "independent_gzip_chunks" or not isinstance(chunks, list):
            raise RuntimeError(f"invalid runtime encoding for {relative_path}")
        target, handle, temporary_name = _atomic_restore_target(root, relative_path)
        digest = hashlib.sha256()
        size = 0
        try:
            with handle:
                for chunk in chunks:
                    payload_path = str(chunk.get("path") or "")
                    if not payload_path.startswith(f"{RUNTIME_PAYLOAD_PREFIX}/"):
                        raise RuntimeError(f"invalid runtime payload path: {payload_path!r}")
                    compressed = _git_blob_bytes(root, revision, payload_path)
                    if len(compressed) != int(chunk["compressed_size"]):
                        raise RuntimeError(f"compressed size mismatch for {payload_path}")
                    if hashlib.sha256(compressed).hexdigest() != chunk["compressed_sha256"]:
                        raise RuntimeError(f"compressed digest mismatch for {payload_path}")
                    try:
                        raw = gzip.decompress(compressed)
                    except OSError as exc:
                        raise RuntimeError(f"invalid gzip payload {payload_path}: {exc}") from exc
                    if len(raw) != int(chunk["uncompressed_size"]):
                        raise RuntimeError(f"uncompressed size mismatch for {payload_path}")
                    if hashlib.sha256(raw).hexdigest() != chunk["uncompressed_sha256"]:
                        raise RuntimeError(f"uncompressed digest mismatch for {payload_path}")
                    handle.write(raw)
                    digest.update(raw)
                    size += len(raw)
                handle.flush()
                os.fsync(handle.fileno())
            if size != int(file_descriptor["size"]) or digest.hexdigest() != file_descriptor["sha256"]:
                raise RuntimeError(f"restored file validation failed for {relative_path}")
            os.replace(temporary_name, target)
        finally:
            try:
                os.unlink(temporary_name)
            except OSError:
                pass
        restored.append(relative_path)
    return restored


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
        self.pending_reason: str | None = None

    def fingerprint(self) -> str:
        """Use durable file metadata; atomic state writes and ledger appends change it."""

        digest = hashlib.sha256()
        for path in self.paths:
            digest.update(str(path).encode("utf-8"))
            try:
                stat = path.stat()
            except OSError:
                digest.update(b"<missing>")
            else:
                digest.update(f"{stat.st_ino}:{stat.st_size}:{stat.st_mtime_ns}".encode("ascii"))
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
            self.pending_reason = reason
            return False
        self.last_fingerprint = fingerprint
        self.pending_reason = None
        return True

    def publish_if_due(self) -> bool:
        if self.pending_reason is None:
            return False
        return self.publish_if_changed(self.pending_reason)


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Publish or restore a bounded Kalshi runtime snapshot.")
    parser.add_argument("--reason")
    parser.add_argument("--restore-sha")
    parser.add_argument("--runtime-ref", default=DEFAULT_RUNTIME_STATE_REF)
    parser.add_argument("paths", nargs="*")
    arguments = parser.parse_args()
    if bool(arguments.reason) == bool(arguments.restore_sha):
        parser.error("specify exactly one of --reason or --restore-sha")
    if arguments.restore_sha:
        if arguments.paths:
            parser.error("restore does not accept source paths")
        restored = restore_runtime_snapshot(
            arguments.restore_sha, runtime_ref=arguments.runtime_ref,
        )
        print(f"RUNTIME_STATE_RESTORE_OK | schema<=v{RUNTIME_STATE_SCHEMA_VERSION} files={len(restored)}")
    else:
        if not arguments.paths:
            parser.error("publish requires at least one durable path")
        publish_runtime_snapshot(
            tuple(Path(path) for path in arguments.paths), arguments.reason,
            runtime_ref=arguments.runtime_ref,
        )
        print(f"RUNTIME_STATE_PUBLISH_OK | schema=v{RUNTIME_STATE_SCHEMA_VERSION}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
