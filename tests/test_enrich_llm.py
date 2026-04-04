"""Tests for enrich_llm: LLM callable abstraction and agent CLI adapter."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from arquimedes.enrich_llm import (
    EnrichmentError,
    _build_prompt_text,
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
        result = _build_prompt_text("Be helpful.", [{"role": "user", "content": "Hello"}])
        assert "[SYSTEM]\nBe helpful." in result
        assert "[USER]\nHello" in result

    def test_multimodal_skips_image_blocks(self):
        messages = [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "Describe this figure."},
                    {"type": "image", "source": {"type": "base64", "data": "abc123"}},
                    {"type": "text", "text": "It shows a floor plan."},
                ],
            }
        ]
        result = _build_prompt_text("system", messages)
        assert "Describe this figure." in result
        assert "floor plan" in result
        assert "abc123" not in result


# ---------------------------------------------------------------------------
# make_cli_llm_fn
# ---------------------------------------------------------------------------


class TestMakeCliLlmFn:
    def test_raises_when_command_not_found(self):
        config = {"llm": {"agent_cmd": "nonexistent-agent-cmd-xyz --print"}}
        with pytest.raises(EnrichmentError, match="Agent CLI not found"):
            make_cli_llm_fn(config)

    def test_returns_callable_that_shells_out(self, tmp_path):
        script = tmp_path / "fake-agent"
        script.write_text('#!/bin/bash\ncat - > /dev/null\necho \'{"result": "ok"}\'')
        script.chmod(0o755)

        config = {"llm": {"agent_cmd": str(script)}, "enrichment": {"max_retries": 1}}
        fn = make_cli_llm_fn(config)
        assert callable(fn)
        result = fn("system prompt", [{"role": "user", "content": "hello"}])
        assert '{"result": "ok"}' in result

    def test_raises_on_nonzero_exit(self, tmp_path):
        script = tmp_path / "failing-agent"
        script.write_text('#!/bin/bash\necho "error message" >&2\nexit 1')
        script.chmod(0o755)

        config = {"llm": {"agent_cmd": str(script)}, "enrichment": {"max_retries": 1}}
        fn = make_cli_llm_fn(config)
        with pytest.raises(EnrichmentError, match="Agent CLI failed"):
            fn("system", [{"role": "user", "content": "hi"}])
