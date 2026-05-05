"""Scheduled publication scanner for the server-maintainer workflow."""

from __future__ import annotations

import html
import json
import os
import subprocess
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
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
from arquimedes.ingest import SUPPORTED_EXTENSIONS, _relative_to_library, load_ignored_material_hashes, load_manifest, scan_library
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


def _state_dir() -> Path:
    path = Path.home() / ".arquimedes"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _log_path() -> Path:
    return _state_dir() / "watch.log"


def _state_path() -> Path:
    return _state_dir() / "watch.state.json"


def _current_log_path() -> Path:
    return _state_dir() / "watch.current.log"


def _write_state(**updates) -> None:
    state = {}
    path = _state_path()
    if path.exists():
        try:
            state = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            state = {}
    state.update(updates)
    state["updated_at"] = _utc_now()
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _append_current_log(line: str) -> None:
    with _current_log_path().open("a", encoding="utf-8") as fh:
        fh.write(line.rstrip("\n") + "\n")


def _tail_current_log(max_lines: int = 200) -> list[str]:
    path = _current_log_path()
    if not path.exists():
        return []
    return path.read_text(encoding="utf-8", errors="replace").splitlines()[-max_lines:]


def _append_log(payload: dict) -> None:
    with _log_path().open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _write_batch_log(project_root: Path, payload: dict) -> None:
    logs = get_logs_root()
    logs.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    (logs / f"watch-{stamp}.log").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def default_runner(args: list[str], cwd: Path, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    """Run a command while teeing live output to the watcher current log."""
    _append_current_log(f"$ {' '.join(args)}")
    proc = subprocess.Popen(
        args,
        cwd=cwd,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
    )
    output: list[str] = []
    assert proc.stdout is not None
    for line in proc.stdout:
        output.append(line)
        _append_current_log(line)
    returncode = proc.wait()
    stdout = "".join(output)
    _append_current_log(f"[exit {returncode}] {' '.join(args)}")
    return subprocess.CompletedProcess(args, returncode, stdout=stdout, stderr="")


def _command_failure_detail(result: subprocess.CompletedProcess[str]) -> str:
    detail = (result.stderr or result.stdout or "").strip()
    return detail or f"command exited with status {result.returncode}"


def _is_supported(path: Path) -> bool:
    return path.is_file() and path.suffix.lower() in SUPPORTED_EXTENSIONS


class LibraryScanner:
    def __init__(self, library_root: Path):
        self.library_root = library_root

    def scan(self) -> list[FileSnapshotEntry]:
        if not self.library_root.exists():
            raise FileNotFoundError(f"Library root does not exist: {self.library_root}")
        entries: list[FileSnapshotEntry] = []
        for path in scan_library(self.library_root):
            if not _is_supported(path):
                continue
            relative = _relative_to_library(path, self.library_root)
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
        ignored_hashes = load_ignored_material_hashes(self.project_root)
        by_path = {m.relative_path.replace("\\", "/"): m for m in manifest.values()}
        by_hash = {m.file_hash: m for m in manifest.values()}
        live_paths = set()
        batch = WatchBatch()

        for entry in snapshot:
            if entry.file_hash in ignored_hashes:
                continue
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
        for attempt in range(attempts):
            _write_state(command=args, command_attempt=attempt + 1, command_attempts=attempts)
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
        batch_empty = batch.is_empty()

        removal_report: RemovalReport | None = None
        _write_state(step="ingest", batch=batch.to_dict(), message="ingesting new/changed files")
        if batch.ingest_paths:
            ingest_args = self._arq("ingest", *[str(path) for path in batch.ingest_paths])
            self._run_checked(ingest_args)

        _write_state(step="remove", message="removing deleted materials")
        if batch.delete:
            removal_report = cascade_delete(batch.delete, project_root=self.project_root)

        _write_state(step="extract", message="extracting and enriching stale materials")
        extract_result = self._run_checked(
            self._arq("extract"),
            retry=int(self.config.get("watch", {}).get("enrich_retries", 1) or 0),
            tolerate_failure=True,
        )
        if extract_result and extract_result.returncode != 0:
            raise RuntimeError(
                "command failed: "
                + " ".join(self._arq("extract"))
                + "\n"
                + _command_failure_detail(extract_result)
            )

        _write_state(step="index", message="ensuring search index")
        self._run_checked(self._arq("index", "ensure"))
        _write_state(step="compile", message="compiling wiki pages")
        self._run_checked(self._arq("compile"))

        _write_state(step="git", message="checking repository changes")
        self._run_checked(["git", "add", "-A"])
        status = self._git("status", "--porcelain")
        committed = False
        pushed = False
        if status.stdout.strip():
            message = self._commit_message(batch, removal_report)
            _write_state(step="commit", message="committing published changes")
            self._run_checked(["git", "commit", "-m", message])
            committed = True
            _write_state(step="push", message="pushing published changes")
            push_result = git_push(self.project_root, config=self.config, runner=self.runner, env=self.env)
            if push_result.returncode != 0:
                detail = (push_result.stderr or push_result.stdout or "git push failed").strip()
                raise RuntimeError(detail)
            pushed = True

        cycle_status = "ok"
        reason = None
        if batch_empty and not committed and removal_report is None:
            cycle_status = "skipped"
            reason = "no file changes or stale work"

        return {
            "status": cycle_status,
            "reason": reason,
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
        _current_log_path().write_text("", encoding="utf-8")
        _write_state(
            running=True,
            status="running",
            step="scan",
            message="scanning library",
            started_at=started_at,
            project_root=str(self.project_root),
            library_root=str(self.library_root),
            interval_seconds=self.interval,
            command=None,
        )
        try:
            snapshot = self.scanner.scan()
            _write_state(step="plan", message=f"planning batch from {len(snapshot)} files")
            batch = self.planner.plan(snapshot)
            result = self.pipeline.run(batch)
            result["checked_at"] = _utc_now()
            result["started_at"] = started_at
            _append_log({"event": "watch_cycle", **result})
            if result.get("status") != "skipped":
                _write_batch_log(self.project_root, result)
            _write_state(
                running=False,
                status=result.get("status", "ok"),
                step="done",
                message=result.get("reason") or "cycle complete",
                last_result=result,
                command=None,
            )
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
            _write_state(running=False, status="error", step="error", message=str(exc), last_result=payload)
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


def _monitor_payload() -> dict:
    state = {}
    if _state_path().exists():
        try:
            state = json.loads(_state_path().read_text(encoding="utf-8"))
        except Exception as exc:
            state = {"status": "unknown", "message": f"could not read state: {exc}"}
    return {
        "state": state,
        "log_tail": _tail_current_log(),
        "paths": {
            "state": str(_state_path()),
            "current_log": str(_current_log_path()),
            "watch_log": str(_log_path()),
        },
    }


def _monitor_html(payload: dict) -> str:
    state = payload.get("state", {})
    rows = "".join(
        f"<tr><th>{html.escape(str(k))}</th><td><code>{html.escape(json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else str(v))}</code></td></tr>"
        for k, v in state.items()
        if k not in {"last_result"}
    )
    log = html.escape("\n".join(payload.get("log_tail", [])))
    paths = payload.get("paths", {})
    return f"""<!doctype html>
<html><head><meta charset='utf-8'>
<title>Arquimedes Watch Monitor</title>
<style>
body{{font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;margin:24px;background:#111;color:#eee}}
.badge{{display:inline-block;padding:4px 10px;border-radius:999px;background:#333}}
.running{{background:#1d4ed8}} .error{{background:#991b1b}} .ok,.skipped{{background:#166534}}
table{{border-collapse:collapse;margin-top:16px;max-width:1200px}} th{{text-align:left;color:#aaa;padding:6px 14px 6px 0;vertical-align:top}}td{{padding:6px 0}}
pre{{background:#050505;border:1px solid #333;border-radius:8px;padding:14px;overflow:auto;max-height:55vh;white-space:pre-wrap}}
a{{color:#93c5fd}} code{{color:#ddd}}
</style></head><body>
<h1>Arquimedes Watch Monitor</h1>
<p id="summary"><span class='badge {html.escape(str(state.get('status','')))}'>{html.escape(str(state.get('status','unknown')))}</span>
<strong>{html.escape(str(state.get('step','')))}</strong> — {html.escape(str(state.get('message','')))}</p>
<table id="state-table">{rows}</table>
<h2>Live command output</h2><pre id="log-output">{log or '(no command output yet)'}</pre>
<h2>Files</h2>
<ul><li>{html.escape(paths.get('state',''))}</li><li>{html.escape(paths.get('current_log',''))}</li><li>{html.escape(paths.get('watch_log',''))}</li></ul>
<script>
const logEl = document.getElementById('log-output');
const summaryEl = document.getElementById('summary');
const tableEl = document.getElementById('state-table');
function esc(value) {{
  return String(value).replace(/[&<>"']/g, ch => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}}[ch]));
}}
function fmt(value) {{
  if (typeof value === 'string' && value.includes('T')) {{
    const d = new Date(value);
    if (!Number.isNaN(d.getTime())) {{
      const pad = n => String(n).padStart(2, '0');
      return `${{d.getFullYear()}}-${{pad(d.getMonth() + 1)}}-${{pad(d.getDate())}} ${{pad(d.getHours())}}:${{pad(d.getMinutes())}}`;
    }}
  }}
  if (Array.isArray(value) || (value && typeof value === 'object')) return JSON.stringify(value);
  return String(value ?? '');
}}
async function refresh() {{
  const wasNearBottom = logEl.scrollTop + logEl.clientHeight >= logEl.scrollHeight - 24;
  const res = await fetch('/state.json?ts=' + Date.now());
  const data = await res.json();
  const state = data.state || {{}};
  const status = esc(state.status || 'unknown');
  summaryEl.innerHTML = `<span class="badge ${{status}}">${{status}}</span> <strong>${{esc(state.step || '')}}</strong> — ${{esc(state.message || '')}}`;
  tableEl.innerHTML = Object.entries(state)
    .filter(([k]) => k !== 'last_result')
    .map(([k, v]) => `<tr><th>${{esc(k)}}</th><td><code>${{esc(fmt(v))}}</code></td></tr>`)
    .join('');
  logEl.textContent = (data.log_tail || []).join('\n') || '(no command output yet)';
  if (wasNearBottom) logEl.scrollTop = logEl.scrollHeight;
}}
setInterval(refresh, 2000);
</script>
</body></html>"""


def serve_monitor(*, host: str = "127.0.0.1", port: int = 8765, duration_minutes: int = 10) -> None:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            payload = _monitor_payload()
            if self.path.startswith("/state.json"):
                body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
                content_type = "application/json; charset=utf-8"
            else:
                body = _monitor_html(payload).encode("utf-8")
                content_type = "text/html; charset=utf-8"
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args) -> None:  # noqa: A002
            return

    server = ThreadingHTTPServer((host, port), Handler)
    server.timeout = 1
    deadline = time.monotonic() + max(duration_minutes, 1) * 60
    print(f"Arquimedes watch monitor: http://{host}:{port}  (auto-stops in {duration_minutes} min)")
    try:
        while time.monotonic() < deadline:
            server.handle_request()
    finally:
        server.server_close()
        print("Arquimedes watch monitor stopped.")
