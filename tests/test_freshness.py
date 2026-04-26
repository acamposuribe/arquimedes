from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import arquimedes.freshness as freshness_mod


def _proc(returncode: int = 0, stdout: str = "", stderr: str = ""):
    return SimpleNamespace(returncode=returncode, stdout=stdout, stderr=stderr)


def _seed_collaborator(root: Path) -> None:
    overlay = root / "config" / "collaborator" / "config.local.yaml"
    overlay.parent.mkdir(parents=True, exist_ok=True)
    overlay.write_text('library_root: "~/Shared"\n', encoding="utf-8")


def _seed_maintainer(root: Path) -> None:
    overlay = root / "config" / "maintainer" / "config.yaml"
    overlay.parent.mkdir(parents=True, exist_ok=True)
    overlay.write_text('library_root: "~/Shared"\n', encoding="utf-8")


def test_freshness_non_git_repo(tmp_path, monkeypatch):
    monkeypatch.setattr(freshness_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(freshness_mod, "_git", lambda root, *args: _proc(returncode=1, stderr="fatal"))
    called = {}
    monkeypatch.setattr(freshness_mod, "ensure_index_and_memory", lambda: called.setdefault("value", (False, None, False, {})))
    status = freshness_mod.update_workspace()
    assert status["repo_applicable"] is False
    assert status["pull_attempted"] is False
    assert "value" in called


def test_freshness_dirty_repo(tmp_path, monkeypatch):
    _seed_collaborator(tmp_path)
    monkeypatch.setattr(freshness_mod, "get_project_root", lambda: tmp_path)
    responses = {
        ("rev-parse", "--is-inside-work-tree"): _proc(stdout="true\n"),
        ("status", "--porcelain"): _proc(stdout=" M file.py\n"),
        ("rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}"): _proc(stdout="origin/main\n"),
        ("rev-list", "--left-right", "--count", "HEAD...@{upstream}"): _proc(stdout="0 1\n"),
        ("fetch", "--prune"): _proc(stdout="fetch ok\n"),
        ("reset", "--hard", "@{upstream}"): _proc(stdout="HEAD is now upstream\n"),
        ("clean", "-fd"): _proc(stdout="Removing scratch.md\n"),
    }
    monkeypatch.setattr(freshness_mod, "_git", lambda root, *args: responses[args])
    monkeypatch.setattr(freshness_mod, "ensure_index_and_memory", lambda: (False, None, False, {}))
    status = freshness_mod.update_workspace()
    assert status["repo_dirty"] is False
    assert status["pull_attempted"] is True
    assert status["pull_result"] == "ok"
    assert status["reset_attempted"] is True
    assert status["reset_result"] == "ok"
    assert status["clean_result"] == "ok"


def test_freshness_clean_repo(tmp_path, monkeypatch):
    _seed_collaborator(tmp_path)
    monkeypatch.setattr(freshness_mod, "get_project_root", lambda: tmp_path)
    responses = {
        ("rev-parse", "--is-inside-work-tree"): _proc(stdout="true\n"),
        ("status", "--porcelain"): _proc(),
        ("rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}"): _proc(stdout="origin/main\n"),
        ("rev-list", "--left-right", "--count", "HEAD...@{upstream}"): _proc(stdout="0 1\n"),
        ("fetch", "--prune"): _proc(stdout="fetch ok\n"),
        ("reset", "--hard", "@{upstream}"): _proc(stdout="HEAD is now upstream\n"),
        ("clean", "-fd"): _proc(),
    }
    monkeypatch.setattr(freshness_mod, "_git", lambda root, *args: responses[args])
    monkeypatch.setattr(freshness_mod, "ensure_index_and_memory", lambda: (True, None, True, {}))
    status = freshness_mod.update_workspace()
    assert status["pull_attempted"] is True
    assert status["pull_result"] == "ok"
    assert status["reset_result"] == "ok"
    assert status["clean_result"] == "ok"
    assert status["index_rebuilt"] is True
    assert status["memory_rebuilt"] is True


def test_update_always_runs_index_ensure(tmp_path, monkeypatch):
    _seed_collaborator(tmp_path)
    monkeypatch.setattr(freshness_mod, "get_project_root", lambda: tmp_path)
    responses = {
        ("rev-parse", "--is-inside-work-tree"): _proc(stdout="true\n"),
        ("status", "--porcelain"): _proc(),
        ("rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}"): _proc(stdout="origin/main\n"),
        ("rev-list", "--left-right", "--count", "HEAD...@{upstream}"): _proc(stdout="0 1\n"),
        ("fetch", "--prune"): _proc(returncode=1, stderr="fetch failed"),
    }
    monkeypatch.setattr(freshness_mod, "_git", lambda root, *args: responses[args])
    called = {}
    monkeypatch.setattr(freshness_mod, "ensure_index_and_memory", lambda: called.setdefault("value", (False, None, False, {})))
    status = freshness_mod.update_workspace()
    assert status["pull_result"] == "error"
    assert status["reset_attempted"] is False
    assert "value" in called


def test_freshness_skips_destructive_steps_on_maintainer_machine(tmp_path, monkeypatch):
    _seed_maintainer(tmp_path)
    monkeypatch.setattr(freshness_mod, "get_project_root", lambda: tmp_path)
    responses = {
        ("rev-parse", "--is-inside-work-tree"): _proc(stdout="true\n"),
        ("status", "--porcelain"): _proc(stdout=" M src/foo.py\n"),
        ("rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}"): _proc(stdout="origin/main\n"),
        ("rev-list", "--left-right", "--count", "HEAD...@{upstream}"): _proc(stdout="0 0\n"),
    }

    def _git_stub(root, *args):
        if args in responses:
            return responses[args]
        raise AssertionError(f"unexpected git call: {args!r}")

    monkeypatch.setattr(freshness_mod, "_git", _git_stub)
    called = {}
    monkeypatch.setattr(freshness_mod, "ensure_index_and_memory", lambda: called.setdefault("value", (False, None, False, {})))
    status = freshness_mod.update_workspace()
    assert status["pull_attempted"] is False
    assert status["reset_attempted"] is False
    assert status["clean_attempted"] is False
    assert "skipping destructive" in status["message"].lower()
    assert "value" in called


def test_freshness_skips_destructive_steps_on_developer_machine(tmp_path, monkeypatch):
    monkeypatch.setattr(freshness_mod, "get_project_root", lambda: tmp_path)
    responses = {
        ("rev-parse", "--is-inside-work-tree"): _proc(stdout="true\n"),
        ("status", "--porcelain"): _proc(stdout=" M src/foo.py\n"),
        ("rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}"): _proc(stdout="origin/main\n"),
        ("rev-list", "--left-right", "--count", "HEAD...@{upstream}"): _proc(stdout="0 1\n"),
    }

    def _git_stub(root, *args):
        if args in responses:
            return responses[args]
        raise AssertionError(f"unexpected git call: {args!r}")

    monkeypatch.setattr(freshness_mod, "_git", _git_stub)
    called = {}
    monkeypatch.setattr(freshness_mod, "ensure_index_and_memory", lambda: called.setdefault("value", (False, None, False, {})))
    status = freshness_mod.update_workspace()
    assert status["pull_attempted"] is False
    assert status["reset_attempted"] is False
    assert "value" in called


def test_freshness_collaborator_with_maintainer_overlay_skips(tmp_path, monkeypatch):
    """If both overlays are present, treat as maintainer (safer choice)."""
    _seed_collaborator(tmp_path)
    _seed_maintainer(tmp_path)
    monkeypatch.setattr(freshness_mod, "get_project_root", lambda: tmp_path)
    responses = {
        ("rev-parse", "--is-inside-work-tree"): _proc(stdout="true\n"),
        ("status", "--porcelain"): _proc(),
        ("rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}"): _proc(stdout="origin/main\n"),
        ("rev-list", "--left-right", "--count", "HEAD...@{upstream}"): _proc(stdout="0 1\n"),
    }
    monkeypatch.setattr(freshness_mod, "_git", lambda root, *args: responses[args])
    monkeypatch.setattr(freshness_mod, "ensure_index_and_memory", lambda: (False, None, False, {}))
    status = freshness_mod.update_workspace()
    assert status["pull_attempted"] is False
    assert status["reset_attempted"] is False
