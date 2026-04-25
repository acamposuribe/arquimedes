from __future__ import annotations

from types import SimpleNamespace

import arquimedes.sync as sync_mod
from arquimedes.sync import SyncCycle


def _proc(returncode=0, stdout="", stderr=""):
    return SimpleNamespace(returncode=returncode, stdout=stdout, stderr=stderr)


def test_sync_cycle_fetch_reset_and_ensure(tmp_path, monkeypatch):
    calls = []
    responses = {
        ("git", "fetch", "origin"): _proc(),
        ("git", "rev-parse", "HEAD"): _proc(stdout="old\n"),
        ("git", "rev-parse", "origin/main"): _proc(stdout="new\n"),
        ("git", "status", "--porcelain"): _proc(),
        ("git", "reset", "--hard", "origin/main"): _proc(stdout="HEAD is now new\n"),
        ("git", "clean", "-fd"): _proc(),
    }

    def runner(args, cwd, env):
        calls.append(tuple(args))
        return responses[tuple(args)]

    monkeypatch.setattr(sync_mod, "ensure_index_and_memory", lambda: (True, None, True, {}))
    result = SyncCycle(project_root=tmp_path, runner=runner).run()

    assert result["status"] == "ok"
    assert result["updated"] is True
    assert result["index_rebuilt"] is True
    assert ("git", "reset", "--hard", "origin/main") in calls
    assert ("git", "clean", "-fd") in calls


def test_sync_cycle_resets_dirty_worktree_even_when_head_is_current(tmp_path, monkeypatch):
    calls = []
    responses = {
        ("git", "fetch", "origin"): _proc(),
        ("git", "rev-parse", "HEAD"): _proc(stdout="same\n"),
        ("git", "rev-parse", "origin/main"): _proc(stdout="same\n"),
        ("git", "status", "--porcelain"): _proc(stdout=" M wiki/page.md\n"),
        ("git", "reset", "--hard", "origin/main"): _proc(stdout="HEAD is now same\n"),
        ("git", "clean", "-fd"): _proc(stdout="Removing scratch.md\n"),
    }

    def runner(args, cwd, env):
        calls.append(tuple(args))
        return responses[tuple(args)]

    monkeypatch.setattr(sync_mod, "ensure_index_and_memory", lambda: (False, None, False, {}))
    result = SyncCycle(project_root=tmp_path, runner=runner).run()

    assert result["status"] == "ok"
    assert result["updated"] is False
    assert result["restored"] is True
    assert ("git", "reset", "--hard", "origin/main") in calls
    assert ("git", "clean", "-fd") in calls


def test_sync_cycle_fetch_failure_skips_reset_and_ensure(tmp_path, monkeypatch):
    called = {"ensure": False}

    def runner(args, cwd, env):
        return _proc(returncode=1, stderr="offline")

    def ensure():
        called["ensure"] = True
        return (False, None, False, {})

    monkeypatch.setattr(sync_mod, "ensure_index_and_memory", ensure)
    result = SyncCycle(project_root=tmp_path, runner=runner).run()

    assert result["status"] == "error"
    assert result["stage"] == "fetch"
    assert called["ensure"] is False
