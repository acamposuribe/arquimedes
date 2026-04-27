"""Scheduled publication scanner for the server-maintainer workflow."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable

from arquimedes.config import (
    get_library_root,
    get_logs_root,
    get_project_root,
    load_config,
)
from arquimedes.git_publish import git_env, push as git_push
from arquimedes.ingest import SUPPORTED_EXTENSIONS, load_manifest
from arquimedes.models import compute_file_hash, compute_material_id
from arquimedes.removal import RemovalReport, cascade_delete


Runner = Callable[[list[str], Path, dict[str, str]], subprocess.CompletedProcess[str]]


@dataclass
class FileSnapshotEntry:
    path: Path
    relative_path: str
    material_id: str
    file_hash: str


@dataclass
class WatchBatch:
    add_or_modify: list[Path] = field(default_factory=list)
    move: list[Path] = field(default_factory=list)
    delete: list[str] = field(default_factory=list)
    added_ids: list[str] = field(default_factory=list)
    updated_ids: list[str] = field(default_factory=list)
    moved_ids: list[str] = field(default_factory=list)

    @property
    def ingest_paths(self) -> list[Path]:
        return [*self.add_or_modify, *self.move]

    def is_empty(self) -> bool:
        return not (self.add_or_modify or self.move or self.delete)

    def to_dict(self) -> dict:
        return {
            "add_or_modify": [str(path) for path in self.add_or_modify],
            "move": [str(path) for path in self.move],
            "delete": self.delete,
            "added_ids": self.added_ids,
            "updated_ids": self.updated_ids,
            "moved_ids": self.moved_ids,
        }


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _log_path() -> Path:
    path = Path.home() / ".arquimedes" / "watch.log"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _append_log(payload: dict) -> None:
    with _log_path().open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _write_batch_log(project_root: Path, payload: dict) -> None:
    logs = get_logs_root()
    logs.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    (logs / f"watch-{stamp}.log").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def default_runner(args: list[str], cwd: Path, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, cwd=cwd, env=env, text=True, capture_output=True, check=False)


def _is_supported(path: Path) -> bool:
    return path.is_file() and path.suffix.lower() in SUPPORTED_EXTENSIONS


class LibraryScanner:
    def __init__(self, library_root: Path):
        self.library_root = library_root

    def scan(self) -> list[FileSnapshotEntry]:
        if not self.library_root.exists():
            raise FileNotFoundError(f"Library root does not exist: {self.library_root}")
        entries: list[FileSnapshotEntry] = []
        for path in sorted(self.library_root.rglob("*")):
            if not _is_supported(path):
                continue
            try:
                relative = path.resolve().relative_to(self.library_root.resolve())
            except ValueError:
                relative = Path(path.name)
            entries.append(
                FileSnapshotEntry(
                    path=path,
                    relative_path=relative.as_posix(),
                    material_id=compute_material_id(path),
                    file_hash=compute_file_hash(path),
                )
            )
        return entries


class BatchPlanner:
    def __init__(self, project_root: Path):
        self.project_root = project_root

    def plan(self, snapshot: Iterable[FileSnapshotEntry]) -> WatchBatch:
        manifest = load_manifest(self.project_root)
        by_path = {m.relative_path.replace("\\", "/"): m for m in manifest.values()}
        by_hash = {m.file_hash: m for m in manifest.values()}
        live_paths = set()
        batch = WatchBatch()

        for entry in snapshot:
            live_paths.add(entry.relative_path)
            path_match = by_path.get(entry.relative_path)
            if path_match:
                if path_match.file_hash != entry.file_hash or path_match.material_id != entry.material_id:
                    batch.add_or_modify.append(entry.path)
                    batch.updated_ids.append(entry.material_id)
                    batch.delete.append(path_match.material_id)
                continue

            hash_match = by_hash.get(entry.file_hash)
            if hash_match:
                batch.move.append(entry.path)
                batch.moved_ids.append(hash_match.material_id)
                continue

            batch.add_or_modify.append(entry.path)
            batch.added_ids.append(entry.material_id)

        for material in manifest.values():
            if material.relative_path.replace("\\", "/") not in live_paths:
                batch.delete.append(material.material_id)

        rehomed = set(batch.added_ids) | set(batch.moved_ids)
        batch.delete = [mid for mid in dict.fromkeys(batch.delete) if mid not in rehomed]
        return batch


class BatchPipeline:
    def __init__(
        self,
        *,
        project_root: Path | None = None,
        config: dict | None = None,
        config_path: str | None = None,
        runner: Runner = default_runner,
    ):
        self.project_root = project_root or get_project_root()
        self.config = config or load_config()
        self.runner = runner
        self.env = git_env(self.config)
        self.env.setdefault("ARQUIMEDES_ROOT", str(self.project_root))
        if config_path:
            self.env["ARQUIMEDES_CONFIG"] = config_path

    def _arq(self, *args: str) -> list[str]:
        return [sys.executable, "-m", "arquimedes.cli", *args]

    def _run(self, args: list[str]) -> subprocess.CompletedProcess[str]:
        return self.runner(args, self.project_root, self.env)

    def _run_checked(self, args: list[str], *, retry: int = 0, tolerate_failure: bool = False) -> subprocess.CompletedProcess[str]:
        attempts = retry + 1
        last: subprocess.CompletedProcess[str] | None = None
        for _ in range(attempts):
            last = self._run(args)
            if last.returncode == 0:
                return last
        assert last is not None
        if tolerate_failure:
            return last
        detail = (last.stderr or last.stdout or "").strip()
        raise RuntimeError(f"command failed: {' '.join(args)}\n{detail}")

    def _git(self, *args: str) -> subprocess.CompletedProcess[str]:
        return self._run(["git", *args])

    def _commit_message(self, batch: WatchBatch, removal: RemovalReport | None) -> str:
        removed = removal.removed_material_ids if removal else batch.delete
        lines = [
            f"auto: ingest {len(batch.added_ids)} add / {len(batch.updated_ids) + len(batch.moved_ids)} update / {len(removed)} remove",
            "",
        ]
        if batch.added_ids:
            lines.append("added:")
            lines.extend(f"  - {mid}" for mid in batch.added_ids)
        if batch.updated_ids or batch.moved_ids:
            lines.append("updated:")
            lines.extend(f"  - {mid}" for mid in [*batch.updated_ids, *batch.moved_ids])
        if removed:
            lines.append("removed:")
            lines.extend(f"  - {mid}" for mid in removed)
        return "\n".join(lines)

    def run(self, batch: WatchBatch) -> dict:
        if batch.is_empty():
            return {"status": "skipped", "reason": "empty batch", "batch": batch.to_dict()}

        removal_report: RemovalReport | None = None
        if batch.ingest_paths:
            ingest_args = self._arq("ingest", *[str(path) for path in batch.ingest_paths])
            self._run_checked(ingest_args)

        if batch.delete:
            removal_report = cascade_delete(batch.delete, project_root=self.project_root)

        if batch.add_or_modify:
            extract_result = self._run_checked(self._arq("extract"), retry=int(self.config.get("watch", {}).get("enrich_retries", 1) or 0), tolerate_failure=True)
        else:
            extract_result = None

        self._run_checked(self._arq("index", "rebuild"))
        self._run_checked(self._arq("compile"))

        self._run_checked(["git", "add", "-A"])
        status = self._git("status", "--porcelain")
        committed = False
        pushed = False
        if status.stdout.strip():
            message = self._commit_message(batch, removal_report)
            self._run_checked(["git", "commit", "-m", message])
            committed = True
            push_result = git_push(self.project_root, config=self.config, runner=self.runner, env=self.env)
            if push_result.returncode != 0:
                detail = (push_result.stderr or push_result.stdout or "git push failed").strip()
                raise RuntimeError(detail)
            pushed = True

        return {
            "status": "ok",
            "batch": batch.to_dict(),
            "removal": removal_report.to_dict() if removal_report else None,
            "extract_returncode": extract_result.returncode if extract_result else None,
            "committed": committed,
            "pushed": pushed,
        }


class WatchDaemon:
    def __init__(
        self,
        *,
        config: dict | None = None,
        config_path: str | None = None,
        project_root: Path | None = None,
        runner: Runner = default_runner,
    ):
        self.config = config or load_config(config_path)
        self.project_root = project_root or get_project_root()
        self.library_root = get_library_root(self.config)
        self.interval = int(self.config.get("watch", {}).get("scan_interval_minutes", 30) or 30) * 60
        self.scanner = LibraryScanner(self.library_root)
        self.planner = BatchPlanner(self.project_root)
        self.pipeline = BatchPipeline(project_root=self.project_root, config=self.config, config_path=config_path, runner=runner)
        self._running = False

    def run_once(self) -> dict:
        started_at = _utc_now()
        try:
            snapshot = self.scanner.scan()
            batch = self.planner.plan(snapshot)
            result = self.pipeline.run(batch)
            result["checked_at"] = _utc_now()
            result["started_at"] = started_at
            _append_log({"event": "watch_cycle", **result})
            if result.get("status") != "skipped":
                _write_batch_log(self.project_root, result)
            return result
        except Exception as exc:
            payload = {
                "event": "watch_cycle",
                "status": "error",
                "started_at": started_at,
                "checked_at": _utc_now(),
                "message": str(exc),
            }
            _append_log(payload)
            _write_batch_log(self.project_root, payload)
            raise

    def start(self) -> None:
        self._running = True
        while self._running:
            self.run_once()
            time.sleep(self.interval)

    def stop(self) -> None:
        self._running = False


def run_once(*, config_path: str | None = None) -> dict:
    return WatchDaemon(config_path=config_path).run_once()
