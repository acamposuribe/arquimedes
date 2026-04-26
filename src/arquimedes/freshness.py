"""Workspace freshness helpers for the web UI."""

from __future__ import annotations

import subprocess
from datetime import datetime, timezone
from pathlib import Path

from arquimedes.config import get_project_root
from arquimedes.index import ensure_index_and_memory


def _is_collaborator_machine(root: Path) -> bool:
    """Return True only on collaborator machines.

    The freshness path's ``git reset --hard`` + ``git clean -fd`` is destructive
    and is only safe where the local repo is a read-only mirror of upstream
    (collaborator role). Maintainers and developers routinely have uncommitted
    work; for them this function returns False and the destructive steps are
    skipped.

    Heuristic: a machine is a collaborator iff a collaborator overlay exists
    AND no maintainer overlay exists. If neither exists (developer working in
    a fresh clone of the code repo), this also returns False.
    """
    collaborator_overlay = root / "config" / "collaborator" / "config.local.yaml"
    maintainer_overlay = root / "config" / "maintainer" / "config.yaml"
    return collaborator_overlay.exists() and not maintainer_overlay.exists()


def _checked_at() -> str:
    return datetime.now(timezone.utc).isoformat()


def _git(root: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )


def _status_base() -> dict:
    return {
        "repo_applicable": False,
        "repo_dirty": False,
        "has_upstream": False,
        "update_available": False,
        "pull_attempted": False,
        "pull_result": "skipped",
        "reset_attempted": False,
        "reset_result": "skipped",
        "clean_attempted": False,
        "clean_result": "skipped",
        "index_rebuilt": False,
        "memory_rebuilt": False,
        "message": "",
        "checked_at": _checked_at(),
    }


def workspace_freshness_status() -> dict:
    root = get_project_root()
    status = _status_base()
    probe = _git(root, "rev-parse", "--is-inside-work-tree")
    if probe.returncode != 0:
        status["message"] = "Git repo not available."
        return status
    status["repo_applicable"] = True
    dirty = _git(root, "status", "--porcelain")
    status["repo_dirty"] = bool(dirty.stdout.strip())
    upstream = _git(root, "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}")
    if upstream.returncode == 0:
        status["has_upstream"] = True
        counts = _git(root, "rev-list", "--left-right", "--count", "HEAD...@{upstream}")
        if counts.returncode == 0:
            ahead, behind = (int(part) for part in counts.stdout.strip().split())
            status["update_available"] = behind > 0
            if ahead > 0 and behind > 0:
                status["message"] = "Local branch diverged from upstream."
            elif behind > 0:
                status["message"] = "Update available."
            elif ahead > 0:
                status["message"] = "Local branch is ahead of upstream."
    if not status["has_upstream"]:
        status["message"] = "No upstream configured."
    elif status["repo_dirty"]:
        status["message"] = "Repository has local changes; refresh will restore upstream."
    elif not status["message"]:
        status["message"] = "Workspace is up to date."
    return status


def update_workspace() -> dict:
    root = get_project_root()
    status = workspace_freshness_status()
    if status["repo_applicable"] and status["has_upstream"] and _is_collaborator_machine(root):
        status["pull_attempted"] = True
        fetch = _git(root, "fetch", "--prune")
        if fetch.returncode == 0:
            status["pull_result"] = "ok"
            status["reset_attempted"] = True
            reset = _git(root, "reset", "--hard", "@{upstream}")
            if reset.returncode == 0:
                status["reset_result"] = "ok"
                status["clean_attempted"] = True
                clean = _git(root, "clean", "-fd")
                if clean.returncode == 0:
                    status["clean_result"] = "ok"
                    if status["repo_dirty"]:
                        status["message"] = "Workspace restored to upstream."
                    else:
                        status["message"] = "Workspace updated." if status["update_available"] else "Workspace already up to date."
                    status["repo_dirty"] = False
                    status["update_available"] = False
                else:
                    status["clean_result"] = "error"
                    status["message"] = (clean.stderr or clean.stdout or "git clean failed").strip()
            else:
                status["reset_result"] = "error"
                status["message"] = (reset.stderr or reset.stdout or "git reset failed").strip()
        else:
            status["pull_result"] = "error"
            status["message"] = (fetch.stderr or fetch.stdout or "git fetch failed").strip()
    elif status["repo_applicable"] and not _is_collaborator_machine(root):
        status["message"] = "Maintainer/developer machine; skipping destructive git restore."
    index_rebuilt, _stats, memory_rebuilt, _counts = ensure_index_and_memory()
    status["index_rebuilt"] = index_rebuilt
    status["memory_rebuilt"] = memory_rebuilt
    status["checked_at"] = _checked_at()
    return status
