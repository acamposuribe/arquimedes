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

    def test_repair_accepts_fenced_json(self):
        llm_fn = _make_llm_fn(['```json\n{"fixed": true}\n```'])

        data = parse_json_or_repair(llm_fn, "not json", "my schema")

        assert data == {"fixed": True}

    def test_repair_accepts_json_prefix_with_trailing_text(self):
        llm_fn = _make_llm_fn(['{"fixed": true}\n\nDone.'])

        data = parse_json_or_repair(llm_fn, "not json", "my schema")

        assert data == {"fixed": True}

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

    def test_pi_with_model(self):
        assert get_agent_model_name(["pi", "--model", "copilot/gpt-4.1"]) == "pi:copilot/gpt-4.1"

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
        assert cmd[cmd.index("-c") + 1] == 'model_reasoning_effort="high"'

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

    def test_claude_strips_bare_flags_from_base_command(self):
        cmd = _build_agent_cmd(
            [
                "claude",
                "--bare",
                "--settings",
                "scripts/claude_bare_settings.json",
                "--dangerously-skip-permissions",
            ],
            "sys",
            effort="medium",
        )
        assert "--bare" not in cmd
        assert "--settings" not in cmd
        assert "--dangerously-skip-permissions" not in cmd
        assert "--no-session-persistence" in cmd
        assert "--disable-slash-commands" in cmd
        assert cmd[cmd.index("--effort") + 1] == "medium"

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
        cmd, stdin_text = _build_stage_request(
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
        cmd, stdin_text = _build_stage_request(
            ["codex", "exec"],
            "codex",
            "system",
            "user prompt",
            model="gpt-5.4-mini",
            effort="medium",
        )
        assert stdin_text.startswith("[SYSTEM]")
        assert "--ephemeral" in cmd
        assert cmd[cmd.index("-m") + 1] == "gpt-5.4-mini"

    def test_pi_uses_print_mode_stdin_and_minimal_resources(self):
        cmd, stdin_text = _build_stage_request(
            ["pi"],
            "pi",
            "system",
            "user prompt",
            model="copilot/gpt-4.1",
            thinking="off",
        )
        assert stdin_text == "user prompt"
        assert "--print" in cmd
        assert "--no-session" in cmd
        assert "--no-context-files" in cmd
        assert "--tools" in cmd
        assert cmd[cmd.index("--tools") + 1] == "read"
        assert "--no-tools" not in cmd
        assert "--no-extensions" in cmd
        assert "--no-skills" in cmd
        assert "--no-prompt-templates" in cmd
        assert "--no-themes" in cmd
        assert cmd[cmd.index("--model") + 1] == "copilot/gpt-4.1"
        assert cmd[cmd.index("--thinking") + 1] == "off"
        assert cmd[cmd.index("--system-prompt") + 1] == "system"

    def test_pi_route_can_override_tools(self):
        cmd, stdin_text = _build_stage_request(
            ["pi"],
            "pi",
            "system",
            "user prompt",
            model="openai-codex/gpt-5.5",
            thinking="off",
            route={"tools": ["read"]},
        )
        assert stdin_text == "user prompt"
        assert "--tools" in cmd
        assert cmd[cmd.index("--tools") + 1] == "read"
        assert "--no-tools" not in cmd

    def test_pi_route_can_disable_tools_explicitly(self):
        cmd, stdin_text = _build_stage_request(
            ["pi"],
            "pi",
            "system",
            "user prompt",
            model="openai-codex/gpt-5.5",
            thinking="off",
            route={"no_tools": True},
        )
        assert stdin_text == "user prompt"
        assert "--no-tools" in cmd
        assert "--tools" not in cmd


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

    def test_stage_routes_find_agent_in_home_local_bin_when_path_is_minimal(self, tmp_path, monkeypatch):
        home = tmp_path / "home"
        bin_dir = home / ".local" / "bin"
        bin_dir.mkdir(parents=True)
        codex = bin_dir / "codex"
        codex.write_text('#!/bin/bash\ncat - > /dev/null\necho \'{"ok": true}\'\n')
        codex.chmod(0o755)

        monkeypatch.setenv("HOME", str(home))
        monkeypatch.setenv("PATH", "/usr/bin:/bin")

        config = {
            "enrichment": {
                "max_retries": 1,
                "llm_routes": {
                    "document": [
                        {"provider": "codex", "command": "codex exec", "model": "gpt-5.4-mini"},
                    ]
                },
            }
        }
        fn = make_cli_llm_fn(config, "document")

        result = fn("system", [{"role": "user", "content": "hi"}])

        assert '{"ok": true}' in result
        assert fn.last_model == "codex:gpt-5.4-mini"

    def test_claude_fallback_can_be_blocked_by_env(self, tmp_path, monkeypatch):
        claude = tmp_path / "claude"
        claude.write_text('#!/bin/bash\necho "Invalid API key"\nexit 1')
        claude.chmod(0o755)

        copilot = tmp_path / "copilot"
        copilot.write_text('#!/bin/bash\ncat - > /dev/null\necho \'{"ok": true}\'\n')
        copilot.chmod(0o755)

        monkeypatch.setenv("PATH", f"{tmp_path}:{os.environ.get('PATH', '')}")
        monkeypatch.setenv("ARQ_ABORT_ON_CLAUDE_FALLBACK", "1")

        config = {
            "enrichment": {
                "max_retries": 1,
                "llm_routes": {
                    "document": [
                        {"provider": "claude", "command": "claude", "model": "sonnet", "bare": True},
                        {"provider": "copilot", "command": "copilot", "model": "gpt-4.1"},
                    ]
                },
            }
        }

        fn = make_cli_llm_fn(config, "document")

        with pytest.raises(EnrichmentError, match="claude fallback blocked"):
            fn("system", [{"role": "user", "content": "hi"}])

    def test_stage_routes_fall_back_when_first_provider_returns_empty_stdout(self, tmp_path, monkeypatch):
        copilot = tmp_path / "copilot"
        copilot.write_text(
            '#!/bin/bash\n'
            'echo "no content produced" >&2\n'
            'exit 0\n'
        )
        copilot.chmod(0o755)

        codex = tmp_path / "codex"
        codex.write_text(
            '#!/bin/bash\n'
            'cat - > /dev/null\n'
            'echo \'{"ok": true}\'\n'
        )
        codex.chmod(0o755)

        monkeypatch.setenv("PATH", f"{tmp_path}:{os.environ.get('PATH', '')}")

        config = {
            "enrichment": {
                "max_retries": 1,
                "llm_routes": {
                    "figure": [
                        {"provider": "copilot", "command": "copilot", "model": "gpt-4o"},
                        {"provider": "codex", "command": "codex exec", "model": "gpt-5.4-mini"},
                    ]
                },
            }
        }
        fn = make_cli_llm_fn(config, "figure")

        result = fn("system", [{"role": "user", "content": "hi"}])

        assert '{"ok": true}' in result
        assert fn.last_model == "codex:gpt-5.4-mini"

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

    def test_timeout_falls_back_from_hanging_agent(self, tmp_path):
        hanging = tmp_path / "hanging-agent"
        hanging.write_text(
            '#!/bin/bash\n'
            'cat - > /dev/null\n'
            'sleep 300\n'
        )
        hanging.chmod(0o755)

        working = tmp_path / "working-agent"
        working.write_text('#!/bin/bash\ncat - > /dev/null\necho "ok"')
        working.chmod(0o755)

        config = {
            "llm": {"agent_cmd": [str(hanging), str(working)]},
            "enrichment": {"max_retries": 1, "llm_timeout_seconds": 1},
        }
        fn = make_cli_llm_fn(config)
        import time
        t0 = time.monotonic()
        result = fn("system", [{"role": "user", "content": "hi"}])
        elapsed = time.monotonic() - t0
        assert "ok" in result
        assert elapsed < 30, f"Timeout fallback took too long: {elapsed:.1f}s"

    def test_nonzero_exit_falls_back_to_next_agent(self, tmp_path):
        limited = tmp_path / "limited-agent"
        limited.write_text(
            '#!/bin/bash\n'
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
        result = fn("system", [{"role": "user", "content": "hi"}])
        assert "ok" in result

    def test_failed_provider_is_retried_on_next_call(self, tmp_path):
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
        assert count_path.read_text() == "2"

    def test_copilot_nonzero_exit_falls_back(self, tmp_path, monkeypatch):
        copilot = tmp_path / "copilot"
        copilot.write_text(
            '#!/bin/bash\n'
            'echo "Error: rate limit exceeded" >&2\n'
            'exit 1\n'
        )
        copilot.chmod(0o755)

        codex = tmp_path / "codex"
        codex.write_text(
            '#!/bin/bash\n'
            'cat - > /dev/null\n'
            'echo \'{"ok": true}\'\n'
        )
        codex.chmod(0o755)

        monkeypatch.setenv("PATH", f"{tmp_path}:{os.environ.get('PATH', '')}")

        config = {
            "enrichment": {
                "max_retries": 1,
                "llm_routes": {
                    "document": [
                        {"provider": "copilot", "command": "copilot", "model": "gpt-4.1", "effort": "high"},
                        {"provider": "codex", "command": "codex exec", "model": "gpt-5.4-mini", "effort": "high"},
                    ]
                },
            }
        }

        fn = make_cli_llm_fn(config, "document")

        result = fn("system", [{"role": "user", "content": "hi"}])

        assert '{"ok": true}' in result
        assert fn.last_model == "codex:gpt-5.4-mini"

    def test_copilot_json_output_with_unauthorized_text_is_not_marked_exhausted(self, tmp_path, monkeypatch):
        copilot = tmp_path / "copilot"
        copilot.write_text(
            '#!/bin/bash\n'
            "echo '{\"id\":\"chk_00062\",\"cls\":\"case_study\",\"kw\":[\"Frans Carl Valck\",\"archival disruption\",\"house-breaking\"],\"s\":\"European colonial archives were disrupted by unauthorized disclosures inside the archive.\"}'\n"
            'exit 0\n'
        )
        copilot.chmod(0o755)

        monkeypatch.setenv("PATH", f"{tmp_path}:{os.environ.get('PATH', '')}")

        config = {
            "enrichment": {
                "max_retries": 1,
                "llm_routes": {
                    "chunk": [
                        {"provider": "copilot", "command": "copilot", "model": "gpt-4.1", "effort": "high"},
                    ]
                },
            }
        }

        fn = make_cli_llm_fn(config, "chunk")

        result = fn("system", [{"role": "user", "content": "hi"}])

        assert 'unauthorized disclosures' in result
        assert fn.last_model == "copilot:gpt-4.1"
