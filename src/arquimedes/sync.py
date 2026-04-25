"""Collaborator sync helpers."""

from __future__ import annotations

import os
import json
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from arquimedes.config import get_project_root, load_config
from arquimedes.index import ensure_index_and_memory


Runner = Callable[[list[str], Path, dict[str, str]], subprocess.CompletedProcess[str]]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _append_log(payload: dict) -> None:
    path = Path.home() / ".arquimedes" / "sync.log"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False) + "\n")


def default_runner(args: list[str], cwd: Path, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, cwd=cwd, env=env, text=True, capture_output=True, check=False)


@dataclass
class SyncCycle:
    project_root: Path | None = None
    runner: Runner = default_runner
    branch_ref: str = "origin/main"

    def __post_init__(self) -> None:
        self.project_root = self.project_root or get_project_root()
        self.env = os.environ.copy()
        self.env.setdefault("ARQUIMEDES_ROOT", str(self.project_root))

    def _run(self, *args: str) -> subprocess.CompletedProcess[str]:
        assert self.project_root is not None
        return self.runner(["git", *args], self.project_root, self.env)

    def run(self) -> dict:
        fetch = self._run("fetch", "origin")
        if fetch.returncode != 0:
            return {
                "status": "error",
                "stage": "fetch",
                "message": (fetch.stderr or fetch.stdout or "git fetch failed").strip(),
                "checked_at": _utc_now(),
            }

        head = self._run("rev-parse", "HEAD")
        remote = self._run("rev-parse", self.branch_ref)
        if head.returncode != 0 or remote.returncode != 0:
            return {
                "status": "error",
                "stage": "rev-parse",
                "message": (head.stderr or remote.stderr or "could not resolve refs").strip(),
                "checked_at": _utc_now(),
            }

        prior_head = head.stdout.strip()
        remote_head = remote.stdout.strip()
        dirty = self._run("status", "--porcelain")
        if dirty.returncode != 0:
            return {
                "status": "error",
                "stage": "status",
                "message": (dirty.stderr or dirty.stdout or "git status failed").strip(),
                "checked_at": _utc_now(),
            }

        had_local_changes = bool(dirty.stdout.strip())
        reset = self._run("reset", "--hard", self.branch_ref)
        if reset.returncode != 0:
            return {
                "status": "error",
                "stage": "reset",
                "prior_head": prior_head,
                "message": (reset.stderr or reset.stdout or "git reset failed").strip(),
                "checked_at": _utc_now(),
            }
        clean = self._run("clean", "-fd")
        if clean.returncode != 0:
            return {
                "status": "error",
                "stage": "clean",
                "prior_head": prior_head,
                "message": (clean.stderr or clean.stdout or "git clean failed").strip(),
                "checked_at": _utc_now(),
            }

        index_rebuilt, _stats, memory_rebuilt, _counts = ensure_index_and_memory()
        return {
            "status": "ok",
            "updated": prior_head != remote_head,
            "restored": had_local_changes,
            "prior_head": prior_head,
            "new_head": remote_head,
            "index_rebuilt": index_rebuilt,
            "memory_rebuilt": memory_rebuilt,
            "checked_at": _utc_now(),
        }


class SyncDaemon:
    def __init__(self, *, config: dict | None = None, project_root: Path | None = None, runner: Runner = default_runner):
        self.config = config or load_config()
        self.interval = int(self.config.get("sync", {}).get("pull_interval", 300) or 300)
        self.cycle = SyncCycle(project_root=project_root, runner=runner)
        self._running = False

    def run_once(self) -> dict:
        result = self.cycle.run()
        _append_log({"event": "sync_cycle", **result})
        return result

    def start(self) -> None:
        self._running = True
        while self._running:
            self.run_once()
            time.sleep(self.interval)

    def stop(self) -> None:
        self._running = False
