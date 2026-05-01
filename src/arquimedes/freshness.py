"""Workspace freshness helpers for the web UI and MCP server.

The vault repo lives only on the maintainer machine; collaborators access
the corpus via the remote MCP server. There is no inbound sync path
anymore, so freshness reduces to: is the index/memory current with the
latest compile, and when did that compile run?
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from arquimedes.config import get_project_root
from arquimedes.index import ensure_index_and_memory


def _checked_at() -> str:
    return datetime.now(timezone.utc).isoformat()


def _last_compiled_at(root: Path) -> str | None:
    stamp_path = root / "derived" / "compile_stamp.json"
    try:
        data = json.loads(stamp_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return None
    value = data.get("compiled_at")
    return str(value) if value else None


def workspace_freshness_status() -> dict:
    root = get_project_root()
    compiled_at = _last_compiled_at(root)
    return {
        "compiled_at": compiled_at,
        "message": f"Last compiled {compiled_at}" if compiled_at else "Wiki has not been compiled yet.",
        "checked_at": _checked_at(),
    }


def update_workspace() -> dict:
    """Ensure the index + memory are current and return the freshness status."""
    index_rebuilt, _stats, memory_rebuilt, _counts = ensure_index_and_memory()
    status = workspace_freshness_status()
    status["index_rebuilt"] = index_rebuilt
    status["memory_rebuilt"] = memory_rebuilt
    return status
