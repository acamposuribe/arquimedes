"""Small launchd installer helpers."""

from __future__ import annotations

import os
import plistlib
import shutil
import subprocess
from pathlib import Path
from typing import Any


LAUNCH_AGENTS = Path.home() / "Library" / "LaunchAgents"


def _uid() -> str:
    return str(os.getuid())


def plist_path(label: str) -> Path:
    return LAUNCH_AGENTS / f"{label}.plist"


def render_plist(label: str, program_arguments: list[str], *, working_directory: str, start_interval: int | None = None, start_calendar_interval: dict[str, int] | None = None, run_at_load: bool = False, keep_alive: bool = False) -> str:
    payload: dict[str, Any] = {
        "Label": label,
        "ProgramArguments": program_arguments,
        "WorkingDirectory": working_directory,
        "RunAtLoad": run_at_load,
        "KeepAlive": keep_alive,
        "StandardOutPath": str(Path.home() / ".arquimedes" / f"{label}.out.log"),
        "StandardErrorPath": str(Path.home() / ".arquimedes" / f"{label}.err.log"),
    }
    if start_interval is not None:
        payload["StartInterval"] = int(start_interval)
    if start_calendar_interval is not None:
        payload["StartCalendarInterval"] = start_calendar_interval
    return plistlib.dumps(payload, sort_keys=False).decode("utf-8")


def install(label: str, plist_text: str) -> dict:
    if shutil.which("launchctl") is None:
        raise RuntimeError("launchctl is not available on this system")
    LAUNCH_AGENTS.mkdir(parents=True, exist_ok=True)
    path = plist_path(label)
    path.write_text(plist_text, encoding="utf-8")
    subprocess.run(["launchctl", "bootout", f"gui/{_uid()}", str(path)], capture_output=True, text=True, check=False)
    proc = subprocess.run(["launchctl", "bootstrap", f"gui/{_uid()}", str(path)], capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError((proc.stderr or proc.stdout or "launchctl bootstrap failed").strip())
    return {"label": label, "path": str(path), "installed": True}


def uninstall(label: str) -> dict:
    if shutil.which("launchctl") is None:
        raise RuntimeError("launchctl is not available on this system")
    path = plist_path(label)
    subprocess.run(["launchctl", "bootout", f"gui/{_uid()}", str(path)], capture_output=True, text=True, check=False)
    if path.exists():
        path.unlink()
    return {"label": label, "path": str(path), "installed": False}


def status(label: str) -> dict:
    if shutil.which("launchctl") is None:
        return {"label": label, "available": False, "loaded": False, "message": "launchctl is not available"}
    proc = subprocess.run(["launchctl", "print", f"gui/{_uid()}/{label}"], capture_output=True, text=True, check=False)
    return {
        "label": label,
        "available": True,
        "loaded": proc.returncode == 0,
        "message": (proc.stdout if proc.returncode == 0 else proc.stderr).strip(),
    }
