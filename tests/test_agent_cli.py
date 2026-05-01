from __future__ import annotations

import json

import click
import pytest
from click.testing import CliRunner

import arquimedes.agent_cli as agent_cli


def test_ensure_guard_calls_ensure_when_env_unset(monkeypatch):
    monkeypatch.delenv("ARQ_SKIP_FRESHNESS", raising=False)
    calls = {"n": 0}

    def fake_refresh():
        calls["n"] += 1
        return {
            "compiled_at": "2026-05-02T10:00:00+00:00",
            "index_rebuilt": False,
            "memory_rebuilt": False,
        }

    import arquimedes.freshness as freshness_mod

    monkeypatch.setattr(freshness_mod, "update_workspace", fake_refresh)

    @agent_cli.ensure_guard
    def cmd():
        return "ok"

    assert cmd() == "ok"
    assert calls["n"] == 1


def test_ensure_guard_skips_when_env_truthy(monkeypatch):
    monkeypatch.setenv("ARQ_SKIP_FRESHNESS", "1")
    import arquimedes.freshness as freshness_mod

    def boom():
        raise AssertionError("update_workspace must not be called")

    monkeypatch.setattr(freshness_mod, "update_workspace", boom)

    @agent_cli.ensure_guard
    def cmd():
        return "ok"

    assert cmd() == "ok"


def test_ensure_guard_converts_file_not_found(monkeypatch):
    monkeypatch.delenv("ARQ_SKIP_FRESHNESS", raising=False)
    import arquimedes.freshness as freshness_mod

    def fake_refresh():
        raise FileNotFoundError("no index at /tmp/foo")

    monkeypatch.setattr(freshness_mod, "update_workspace", fake_refresh)

    @agent_cli.ensure_guard
    def cmd():
        return "ok"

    with pytest.raises(click.ClickException) as exc_info:
        cmd()
    assert "no index at /tmp/foo" in str(exc_info.value.message)


def test_emit_default_is_json():
    @click.command()
    def cmd():
        agent_cli.emit({"a": 1, "b": [1, 2]}, human=False)

    result = CliRunner().invoke(cmd, [])
    assert result.exit_code == 0
    assert json.loads(result.output) == {"a": 1, "b": [1, 2]}


def test_emit_human_with_string_payload():
    @click.command()
    def cmd():
        agent_cli.emit("hello world", human=True)

    result = CliRunner().invoke(cmd, [])
    assert result.exit_code == 0
    assert result.output.strip() == "hello world"


def test_emit_human_uses_formatter():
    @click.command()
    def cmd():
        agent_cli.emit({"x": 42}, human=True, human_formatter=lambda d: f"x is {d['x']}")

    result = CliRunner().invoke(cmd, [])
    assert result.exit_code == 0
    assert result.output.strip() == "x is 42"


def test_emit_human_without_formatter_raises():
    with pytest.raises(RuntimeError):
        agent_cli.emit({"x": 1}, human=True)


def test_emit_uses_to_dict_for_json():
    class Thing:
        def to_dict(self):
            return {"kind": "thing"}

    @click.command()
    def cmd():
        agent_cli.emit(Thing(), human=False)

    result = CliRunner().invoke(cmd, [])
    assert result.exit_code == 0
    assert json.loads(result.output) == {"kind": "thing"}


def test_not_found_without_hint():
    err = agent_cli.not_found("material \"abc\"")
    assert isinstance(err, click.ClickException)
    assert err.message == "Error: material \"abc\" not found."


def test_not_found_with_hint():
    err = agent_cli.not_found("chunk \"xy\"", hint="arq read abc --detail chunks")
    assert err.message == "Error: chunk \"xy\" not found. Try: arq read abc --detail chunks"
