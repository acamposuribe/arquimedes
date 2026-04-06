"""Tests for enrich_llm: LLM callable abstraction and agent CLI adapter."""

from __future__ import annotations

import json
import os
from unittest.mock import MagicMock

import pytest

from arquimedes.enrich_llm import (
    EnrichmentError,
    _build_agent_cmd,
    _build_prompt_text,
    _build_stage_request,
    _run_agent_subprocess,
    get_agent_model_name,
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

    def test_trailing_non_json_text_is_ignored_without_llm_call(self):
        llm_fn = _make_llm_fn([])
        text = '{"k": 1}\n\nI\'m sorry, but I cannot assist with that request.'

        assert parse_json_or_repair(llm_fn, text, "s") == {"k": 1}
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

    def test_strips_embedded_null_bytes(self):
        system, user = _build_prompt_text(
            "Be\x00 helpful.",
            [{"role": "user", "content": "Hel\x00lo"}],
        )
        assert "\x00" not in system
        assert "\x00" not in user

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
        assert get_model_id({"llm": {"agent_cmd": "claude --print"}}) == "claude --print"

    def test_list(self):
        assert get_model_id({"llm": {"agent_cmd": ["claude --print", "codex exec"]}}) == "claude --print|codex exec"

    def test_default(self):
        assert get_model_id({}) == "claude --print"


class TestGetAgentModelName:
    def test_claude_with_model(self):
        assert get_agent_model_name(["claude", "--print", "--model", "sonnet"]) == "claude:sonnet"

    def test_claude_without_model(self):
        assert get_agent_model_name(["claude", "--print"]) == "claude"

    def test_codex(self):
        assert get_agent_model_name(["codex", "exec"]) == "codex"

    def test_codex_with_model(self):
        assert get_agent_model_name(["codex", "exec", "-m", "gpt-5.4-mini"]) == "codex:gpt-5.4-mini"

    def test_copilot_with_model(self):
        assert get_agent_model_name(["copilot", "--model", "gpt-4.1"]) == "copilot:gpt-4.1"

    def test_other_agent(self):
        assert get_agent_model_name(["myagent", "run"]) == "myagent"


# ---------------------------------------------------------------------------
# _build_agent_cmd
# ---------------------------------------------------------------------------


class TestBuildAgentCmd:
    def test_claude_gets_system_prompt(self):
        cmd = _build_agent_cmd(["claude", "--print"], "Be helpful.")
        assert "--bare" not in cmd  # --bare breaks credential discovery
        assert "--no-session-persistence" in cmd
        assert "--disable-slash-commands" in cmd
        assert "--tools" in cmd
        idx_tools = cmd.index("--tools")
        assert cmd[idx_tools + 1] == "Read,Write,Bash"
        assert "--model" in cmd
        idx_model = cmd.index("--model")
        assert cmd[idx_model + 1] == "sonnet"  # default model
        assert "--system-prompt" in cmd
        idx = cmd.index("--system-prompt")
        assert cmd[idx + 1] == "Be helpful."

    def test_claude_respects_explicit_model(self):
        cmd = _build_agent_cmd(["claude", "--print", "--model", "opus"], "sys")
        assert cmd.count("--model") == 1
        idx = cmd.index("--model")
        assert cmd[idx + 1] == "opus"  # user's choice preserved

    def test_claude_no_duplicate_no_session(self):
        cmd = _build_agent_cmd(["claude", "--print", "--no-session-persistence"], "sys")
        assert cmd.count("--no-session-persistence") == 1

    def test_claude_respects_explicit_tools(self):
        cmd = _build_agent_cmd(["claude", "--print", "--tools", "Bash"], "sys")
        assert cmd.count("--tools") == 1  # don't override user's explicit tools

    def test_non_claude_unchanged(self):
        cmd = _build_agent_cmd(["myagent", "run"], "sys")
        assert cmd == ["myagent", "run"]
        assert "--bare" not in cmd

    def test_codex_gets_ephemeral_and_skip_git(self):
        cmd = _build_agent_cmd(["codex", "exec"], "sys")
        assert "--ephemeral" in cmd
        assert "--skip-git-repo-check" not in cmd  # not a valid codex flag

    def test_codex_no_duplicate_ephemeral(self):
        cmd = _build_agent_cmd(["codex", "exec", "--ephemeral"], "sys")
        assert cmd.count("--ephemeral") == 1

    def test_codex_model_and_effort(self):
        cmd = _build_agent_cmd(["codex", "exec"], "sys", model_override="gpt-5.4-mini", effort="high")
        assert "-m" in cmd
        assert cmd[cmd.index("-m") + 1] == "gpt-5.4-mini"
        assert "-c" in cmd
        assert cmd[cmd.index("-c") + 1] == "model_reasoning_effort=high"  # no extra quotes

    def test_codex_no_model_when_not_specified(self):
        cmd = _build_agent_cmd(["codex", "exec"], "sys")
        assert "-m" not in cmd
        assert "-c" not in cmd

    def test_claude_effort_added_when_specified(self):
        cmd = _build_agent_cmd(["claude", "--print"], "sys", effort="low")
        assert "--effort" in cmd
        idx = cmd.index("--effort")
        assert cmd[idx + 1] == "low"

    def test_claude_effort_omitted_when_none(self):
        cmd = _build_agent_cmd(["claude", "--print"], "sys", effort=None)
        assert "--effort" not in cmd

    def test_claude_explicit_effort_not_overridden(self):
        cmd = _build_agent_cmd(["claude", "--print", "--effort", "high"], "sys", effort="low")
        assert cmd.count("--effort") == 1
        idx = cmd.index("--effort")
        assert cmd[idx + 1] == "high"  # user's explicit choice preserved

    def test_claude_model_override(self):
        cmd = _build_agent_cmd(["claude", "--print"], "sys", model_override="haiku")
        assert "--model" in cmd
        idx = cmd.index("--model")
        assert cmd[idx + 1] == "haiku"

    def test_claude_model_override_replaces_default(self):
        """When base_parts don't have --model, override takes precedence over sonnet default."""
        cmd = _build_agent_cmd(["claude", "--print"], "sys", model_override="haiku")
        assert cmd.count("--model") == 1
        idx = cmd.index("--model")
        assert cmd[idx + 1] == "haiku"

    def test_claude_no_model_override_defaults_to_sonnet(self):
        cmd = _build_agent_cmd(["claude", "--print"], "sys", model_override=None)
        idx = cmd.index("--model")
        assert cmd[idx + 1] == "sonnet"


class TestBuildStageRequest:
    def test_copilot_uses_prompt_flag_without_broad_tools(self):
        cmd, stdin_text, fast_fail = _build_stage_request(
            ["copilot"],
            "copilot",
            "system",
            "user prompt",
            model="gpt-4.1",
            effort="high",
            route={
                "agent": "copilot-no-tools-json",
                "silent": True,
                "no_ask_user": True,
                "no_auto_update": True,
                "no_custom_instructions": True,
                "allow_all": False,
            },
        )
        assert stdin_text == ""
        assert fast_fail is True
        assert "--agent" in cmd
        assert cmd[cmd.index("--agent") + 1] == "copilot-no-tools-json"
        assert "--prompt" in cmd
        assert "--silent" in cmd
        assert "--allow-all" not in cmd
        assert "--allow-all-tools" not in cmd
        assert "--no-ask-user" in cmd
        assert "--no-auto-update" in cmd
        assert "--no-custom-instructions" in cmd
        assert cmd[cmd.index("--model") + 1] == "gpt-4.1"
        assert cmd[cmd.index("--effort") + 1] == "high"

    def test_codex_uses_stdin_and_model_flag(self):
        cmd, stdin_text, fast_fail = _build_stage_request(
            ["codex", "exec"],
            "codex",
            "system",
            "user prompt",
            model="gpt-5.4-mini",
            effort="medium",
        )
        assert stdin_text.startswith("[SYSTEM]")
        assert fast_fail is False
        assert "--ephemeral" in cmd
        assert cmd[cmd.index("-m") + 1] == "gpt-5.4-mini"


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
        # last_model should be set to the script name
        assert fn.last_model == script.name

    def test_completion_sentinel_terminates_cleanly(self, tmp_path):
        script = tmp_path / "sentinel-agent"
        script.write_text(
            '#!/bin/bash\n'
            'cat - > /dev/null\n'
            'echo "PROCESS_FINISHED"\n'
            'sleep 300\n'
        )
        script.chmod(0o755)

        config = {"llm": {"agent_cmd": str(script)}, "enrichment": {"max_retries": 1}}
        fn = make_cli_llm_fn(config)

        import time

        t0 = time.monotonic()
        result = fn("system prompt", [{"role": "user", "content": "hello"}])
        elapsed = time.monotonic() - t0

        assert result == "{}"
        assert elapsed < 15, f"Sentinel shutdown took too long: {elapsed:.1f}s"

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

    def test_stage_routes_use_copilot_fallback(self, tmp_path, monkeypatch):
        codex = tmp_path / "codex"
        codex.write_text('#!/bin/bash\necho "rate limit exceeded" >&2\nexit 1')
        codex.chmod(0o755)

        copilot = tmp_path / "copilot"
        args_path = tmp_path / "copilot-args.txt"
        copilot.write_text(
            f'#!/bin/bash\n'
            f'printf "%s\\n" "$@" > "{args_path}"\n'
            f'cat - > /dev/null\n'
            f'echo "{{\\"ok\\": true}}"\n'
        )
        copilot.chmod(0o755)

        monkeypatch.setenv("PATH", f"{tmp_path}:{os.environ.get('PATH', '')}")

        config = {
            "enrichment": {
                "max_retries": 1,
                "llm_routes": {
                    "document": [
                        {"provider": "codex", "command": "codex exec", "model": "gpt-5.4-mini", "effort": "high"},
                        {"provider": "copilot", "command": "copilot", "model": "gpt-4.1", "effort": "high"},
                    ]
                },
            }
        }
        fn = make_cli_llm_fn(config, "document")
        result = fn("system", [{"role": "user", "content": "hi"}])
        assert '{"ok": true}' in result
        args = args_path.read_text().splitlines()
        assert "--prompt" in args
        assert "--silent" in args
        assert "--allow-all" in args
        assert "--no-ask-user" in args
        assert "--no-auto-update" in args
        assert "--no-custom-instructions" in args
        assert "--model" in args and args[args.index("--model") + 1] == "gpt-4.1"
        assert "--effort" in args and args[args.index("--effort") + 1] == "high"

    def test_stage_route_timeout_seconds_is_honored(self, tmp_path, monkeypatch):
        hanging = tmp_path / "sleep-agent"
        hanging.write_text(
            '#!/bin/bash\n'
            'cat - > /dev/null\n'
            'sleep 300\n'
        )
        hanging.chmod(0o755)

        monkeypatch.setenv("PATH", f"{tmp_path}:{os.environ.get('PATH', '')}")

        config = {
            "enrichment": {
                "max_retries": 1,
                "llm_routes": {
                    "chunk": [
                        {
                            "provider": "codex",
                            "command": "sleep-agent",
                            "model": "gpt-5.4-mini",
                            "timeout_seconds": 1,
                        }
                    ]
                },
            }
        }
        fn = make_cli_llm_fn(config, "chunk")

        import time

        t0 = time.monotonic()
        with pytest.raises(EnrichmentError, match="timed out after 1s"):
            fn("system", [{"role": "user", "content": "hi"}])
        elapsed = time.monotonic() - t0
        assert elapsed < 15, f"Timeout took too long: {elapsed:.1f}s"

    def test_stage_route_model_id_includes_stage_signature(self):
        config = {
            "enrichment": {
                "llm_routes": {
                    "chunk": [
                        {"provider": "copilot", "command": "copilot", "model": "gpt-4.1"},
                    ]
                }
            }
        }
        assert get_model_id(config, "chunk").startswith("copilot:gpt-4.1")

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

    def test_exhausted_provider_is_not_retried_on_next_call(self, tmp_path):
        limited = tmp_path / "limited-agent"
        count_path = tmp_path / "limited-count.txt"
        limited.write_text(
            '#!/bin/bash\n'
            f'count=0\n'
            f'if [ -f "{count_path}" ]; then count=$(cat "{count_path}"); fi\n'
            f'count=$((count + 1))\n'
            f'printf "%s" "$count" > "{count_path}"\n'
            'cat - > /dev/null\n'
            'echo "Error: rate limit exceeded" >&2\n'
            'exit 1\n'
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
        assert "ok" in fn("system", [{"role": "user", "content": "hi"}])
        assert "ok" in fn("system", [{"role": "user", "content": "hi again"}])
        assert count_path.read_text() == "1"
