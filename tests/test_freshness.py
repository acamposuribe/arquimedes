from __future__ import annotations

import json
from pathlib import Path

import arquimedes.freshness as freshness_mod


def test_status_reports_no_compile(tmp_path, monkeypatch):
    monkeypatch.setattr(freshness_mod, "get_project_root", lambda: tmp_path)
    status = freshness_mod.workspace_freshness_status()
    assert status["compiled_at"] is None
    assert "not been compiled" in status["message"]
    assert status["checked_at"]


def test_status_reads_compile_stamp(tmp_path, monkeypatch):
    derived = tmp_path / "derived"
    derived.mkdir()
    (derived / "compile_stamp.json").write_text(
        json.dumps({"compiled_at": "2026-05-02T10:00:00+00:00"}), encoding="utf-8"
    )
    monkeypatch.setattr(freshness_mod, "get_project_root", lambda: tmp_path)
    status = freshness_mod.workspace_freshness_status()
    assert status["compiled_at"] == "2026-05-02T10:00:00+00:00"
    assert "2026-05-02T10:00:00+00:00" in status["message"]


def test_update_runs_ensure_and_returns_status(tmp_path, monkeypatch):
    monkeypatch.setattr(freshness_mod, "get_project_root", lambda: tmp_path)
    called = {}

    def _ensure():
        called["value"] = True
        return (True, None, True, {})

    monkeypatch.setattr(freshness_mod, "ensure_index_and_memory", _ensure)
    status = freshness_mod.update_workspace()
    assert called.get("value") is True
    assert status["index_rebuilt"] is True
    assert status["memory_rebuilt"] is True
    assert "checked_at" in status
