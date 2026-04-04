"""Tests for enrich_llm: LLM callable abstraction and agent CLI adapter."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from arquimedes.enrich_llm import (
    EnrichmentError,
    _build_agent_cmd,
    _build_prompt_text,
    _run_agent_subprocess,
    get_model_id,
    make_cli_llm_fn,
    parse_json_or_repair,
)


def _make_llm_fn(responses: list[str]):
    return MagicMock(side_effect=responses)


# ---------------------------------------------------------------------------
# parse_json_or_repair
# ---------------------------------------------------------------------------


class TestParseJsonOrRepair:
    def test_valid_json_and_fences_pass_without_llm_call(self):
        llm_fn = _make_llm_fn([])
        assert parse_json_or_repair(llm_fn, '{"k": 1}', "s") == {"k": 1}
        assert parse_json_or_repair(llm_fn, '[1, 2]', "s") == [1, 2]
        assert parse_json_or_repair(llm_fn, '```json\n{"k": 1}\n```', "s") == {"k": 1}
        assert parse_json_or_repair(llm_fn, '  \n```\n{"ok": true}\n```\n  ', "s") == {"ok": True}
        llm_fn.assert_not_called()

    def test_invalid_json_triggers_repair(self):
        llm_fn = _make_llm_fn([json.dumps({"fixed": True})])
        data = parse_json_or_repair(llm_fn, "not json", "my schema")
        assert data == {"fixed": True}
        assert llm_fn.call_count == 1
        system_arg = llm_fn.call_args[0][0]
        assert "JSON repair" in system_arg
        user_content = llm_fn.call_args[0][1][0]["content"]
        assert "my schema" in user_content and "not json" in user_content

    def test_repair_failure_raises(self):
        llm_fn = _make_llm_fn(["still broken"])
        with pytest.raises(EnrichmentError, match="Schema repair failed"):
            parse_json_or_repair(llm_fn, "bad", "schema")


# ---------------------------------------------------------------------------
# _build_prompt_text
# ---------------------------------------------------------------------------


class TestBuildPromptText:
    def test_simple_text_messages(self):
        system, user = _build_prompt_text("Be helpful.", [{"role": "user", "content": "Hello"}])
        assert system == "Be helpful."
        assert "[USER]\nHello" in user

    def test_multimodal_includes_image_paths(self):
        messages = [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "Describe this figure."},
                    {"type": "image", "source": {"type": "base64", "data": "abc123"},
                     "_source_path": "/path/to/fig_0001.png"},
                    {"type": "text", "text": "It shows a floor plan."},
                ],
            }
        ]
        system, user = _build_prompt_text("system", messages)
        assert system == "system"
        assert "Describe this figure." in user
        assert "floor plan" in user
        assert "abc123" not in user  # base64 data not leaked
        assert "/path/to/fig_0001.png" in user
        assert "[IMAGE:" in user


# ---------------------------------------------------------------------------
# get_model_id
# ---------------------------------------------------------------------------


class TestGetModelId:
    def test_single_string(self):
        assert get_model_id({"llm": {"agent_cmd": "claude --print"}}) == "claude"

    def test_list(self):
        assert get_model_id({"llm": {"agent_cmd": ["claude --print", "codex exec"]}}) == "claude|codex"

    def test_default(self):
        assert get_model_id({}) == "claude"


# ---------------------------------------------------------------------------
# _build_agent_cmd
# ---------------------------------------------------------------------------


class TestBuildAgentCmd:
    def test_claude_gets_bare_and_system_prompt(self):
        cmd = _build_agent_cmd(["claude", "--print"], "Be helpful.")
        assert "--bare" in cmd
        assert "--no-session-persistence" in cmd
        assert "--system-prompt" in cmd
        idx = cmd.index("--system-prompt")
        assert cmd[idx + 1] == "Be helpful."

    def test_claude_no_duplicate_bare(self):
        cmd = _build_agent_cmd(["claude", "--print", "--bare"], "sys")
        assert cmd.count("--bare") == 1

    def test_non_claude_unchanged(self):
        cmd = _build_agent_cmd(["myagent", "run"], "sys")
        assert cmd == ["myagent", "run"]
        assert "--bare" not in cmd

    def test_codex_gets_ephemeral_and_skip_git(self):
        cmd = _build_agent_cmd(["codex", "exec"], "sys")
        assert "--ephemeral" in cmd
        assert "--skip-git-repo-check" in cmd
        assert "--bare" not in cmd  # claude-only flag

    def test_codex_no_duplicate_ephemeral(self):
        cmd = _build_agent_cmd(["codex", "exec", "--ephemeral"], "sys")
        assert cmd.count("--ephemeral") == 1


# ---------------------------------------------------------------------------
# make_cli_llm_fn
# ---------------------------------------------------------------------------


class TestMakeCliLlmFn:
    def test_raises_when_no_command_found(self):
        config = {"llm": {"agent_cmd": ["nonexistent-cmd-xyz", "also-missing-abc"]}}
        with pytest.raises(EnrichmentError, match="No agent CLI found"):
            make_cli_llm_fn(config)

    def test_single_string_works(self, tmp_path):
        script = tmp_path / "fake-agent"
        script.write_text('#!/bin/bash\ncat - > /dev/null\necho \'{"result": "ok"}\'')
        script.chmod(0o755)

        config = {"llm": {"agent_cmd": str(script)}, "enrichment": {"max_retries": 1}}
        fn = make_cli_llm_fn(config)
        assert callable(fn)
        result = fn("system prompt", [{"role": "user", "content": "hello"}])
        assert '{"result": "ok"}' in result

    def test_fallback_to_second_agent(self, tmp_path):
        failing = tmp_path / "failing-agent"
        failing.write_text('#!/bin/bash\necho "rate limited" >&2\nexit 1')
        failing.chmod(0o755)

        working = tmp_path / "working-agent"
        working.write_text('#!/bin/bash\ncat - > /dev/null\necho \'{"ok": true}\'')
        working.chmod(0o755)

        config = {
            "llm": {"agent_cmd": [str(failing), str(working)]},
            "enrichment": {"max_retries": 1},
        }
        fn = make_cli_llm_fn(config)
        result = fn("system", [{"role": "user", "content": "hi"}])
        assert '{"ok": true}' in result

    def test_raises_on_all_agents_failing(self, tmp_path):
        failing = tmp_path / "failing-agent"
        failing.write_text('#!/bin/bash\necho "error" >&2\nexit 1')
        failing.chmod(0o755)

        config = {"llm": {"agent_cmd": [str(failing)]}, "enrichment": {"max_retries": 1}}
        fn = make_cli_llm_fn(config)
        with pytest.raises(EnrichmentError, match="failed"):
            fn("system", [{"role": "user", "content": "hi"}])

    def test_fast_fail_kills_hanging_agent(self, tmp_path):
        """Agent that prints auth error to stderr then hangs is killed fast."""
        hanging = tmp_path / "hanging-agent"
        # Prints "Not logged in" to stderr then sleeps forever
        hanging.write_text(
            '#!/bin/bash\n'
            'cat - > /dev/null\n'
            'echo "Not logged in · Please run /login" >&2\n'
            'sleep 300\n'
        )
        hanging.chmod(0o755)

        working = tmp_path / "working-agent"
        working.write_text('#!/bin/bash\ncat - > /dev/null\necho "ok"')
        working.chmod(0o755)

        config = {
            "llm": {"agent_cmd": [str(hanging), str(working)]},
            "enrichment": {"max_retries": 1},
        }
        fn = make_cli_llm_fn(config)
        import time
        t0 = time.monotonic()
        result = fn("system", [{"role": "user", "content": "hi"}])
        elapsed = time.monotonic() - t0
        assert "ok" in result
        # Should be killed quickly, not wait 300s
        assert elapsed < 30, f"Fast-fail took too long: {elapsed:.1f}s"

    def test_fast_fail_rate_limit(self, tmp_path):
        """Agent that prints rate-limit error is killed and fallback used."""
        limited = tmp_path / "limited-agent"
        limited.write_text(
            '#!/bin/bash\n'
            'cat - > /dev/null\n'
            'echo "Error: rate limit exceeded" >&2\n'
            'sleep 300\n'
        )
        limited.chmod(0o755)

        working = tmp_path / "working-agent"
        working.write_text('#!/bin/bash\ncat - > /dev/null\necho "ok"')
        working.chmod(0o755)

        config = {
            "llm": {"agent_cmd": [str(limited), str(working)]},
            "enrichment": {"max_retries": 1},
        }
        fn = make_cli_llm_fn(config)
        result = fn("system", [{"role": "user", "content": "hi"}])
        assert "ok" in result
