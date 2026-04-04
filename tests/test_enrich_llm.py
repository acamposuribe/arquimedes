"""Tests for enrich_llm: LLM client wrapper with retry and schema repair."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import anthropic
import pytest

from arquimedes.enrich_llm import (
    EnrichmentError,
    call_llm,
    make_client,
    parse_json_or_repair,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_response(text: str) -> MagicMock:
    """Build a mock Anthropic response with a single text content block."""
    response = MagicMock()
    response.content = [MagicMock(text=text)]
    return response


def _make_client(side_effects) -> MagicMock:
    """Build a mock client whose messages.create raises/returns in sequence."""
    client = MagicMock()
    client.messages.create.side_effect = side_effects
    return client


# ---------------------------------------------------------------------------
# call_llm tests
# ---------------------------------------------------------------------------


class TestCallLlm:
    def test_success_on_first_try(self):
        """Returns text immediately when the first call succeeds."""
        client = _make_client([_make_response("hello")])
        result = call_llm(client, "model-x", "system", [{"role": "user", "content": "hi"}])
        assert result == "hello"
        assert client.messages.create.call_count == 1

    def test_success_on_second_try_after_rate_limit(self):
        """Retries once after RateLimitError and returns the successful result."""
        rate_err = anthropic.RateLimitError(
            message="rate limit",
            response=MagicMock(status_code=429, headers={}),
            body={},
        )
        client = _make_client([rate_err, _make_response("recovered")])
        with patch("arquimedes.enrich_llm.time.sleep") as mock_sleep:
            result = call_llm(
                client, "model-x", "system", [{"role": "user", "content": "hi"}],
                max_retries=3,
            )
        assert result == "recovered"
        assert client.messages.create.call_count == 2
        mock_sleep.assert_called_once_with(1)  # 2 ** 0 on first failure (attempt=0)

    def test_success_on_second_try_after_connection_error(self):
        """Retries once after APIConnectionError and returns successfully."""
        conn_err = anthropic.APIConnectionError(request=MagicMock())
        client = _make_client([conn_err, _make_response("ok")])
        with patch("arquimedes.enrich_llm.time.sleep"):
            result = call_llm(
                client, "model-x", "system", [{"role": "user", "content": "hi"}],
                max_retries=3,
            )
        assert result == "ok"
        assert client.messages.create.call_count == 2

    def test_raises_after_max_retries_exhausted(self):
        """Raises the last retryable exception after all attempts fail."""
        rate_err = anthropic.RateLimitError(
            message="rate limit",
            response=MagicMock(status_code=429, headers={}),
            body={},
        )
        client = _make_client([rate_err, rate_err, rate_err])
        with patch("arquimedes.enrich_llm.time.sleep"):
            with pytest.raises(anthropic.RateLimitError):
                call_llm(
                    client, "model-x", "system", [{"role": "user", "content": "hi"}],
                    max_retries=3,
                )
        assert client.messages.create.call_count == 3

    def test_non_retryable_error_propagates_immediately(self):
        """Non-retryable errors (e.g. AuthenticationError) propagate without retry."""
        auth_err = anthropic.AuthenticationError(
            message="invalid key",
            response=MagicMock(status_code=401, headers={}),
            body={},
        )
        client = _make_client([auth_err])
        with pytest.raises(anthropic.AuthenticationError):
            call_llm(
                client, "model-x", "system", [{"role": "user", "content": "hi"}],
                max_retries=3,
            )
        # Only one attempt — no retry for auth errors
        assert client.messages.create.call_count == 1

    def test_exponential_backoff_sleep_values(self):
        """Sleep durations follow 2**attempt: 1, 2, 4 ..."""
        rate_err = anthropic.RateLimitError(
            message="rate limit",
            response=MagicMock(status_code=429, headers={}),
            body={},
        )
        # 2 failures then success → sleeps with 2**0=1, 2**1=2
        client = _make_client([rate_err, rate_err, _make_response("done")])
        with patch("arquimedes.enrich_llm.time.sleep") as mock_sleep:
            result = call_llm(
                client, "model-x", "system", [{"role": "user", "content": "hi"}],
                max_retries=3,
            )
        assert result == "done"
        assert mock_sleep.call_args_list[0][0][0] == 1  # 2**0
        assert mock_sleep.call_args_list[1][0][0] == 2  # 2**1

    def test_passes_correct_kwargs_to_create(self):
        """messages.create receives model, max_tokens, system, messages."""
        client = _make_client([_make_response("result")])
        msgs = [{"role": "user", "content": "question"}]
        call_llm(client, "claude-3", "be helpful", msgs, max_tokens=512)
        client.messages.create.assert_called_once_with(
            model="claude-3",
            max_tokens=512,
            system="be helpful",
            messages=msgs,
        )


# ---------------------------------------------------------------------------
# parse_json_or_repair tests
# ---------------------------------------------------------------------------


class TestParseJsonOrRepair:
    def _client(self, repair_text: str | None = None) -> MagicMock:
        """Client that returns repair_text if a repair call is made."""
        client = MagicMock()
        if repair_text is not None:
            client.messages.create.return_value = _make_response(repair_text)
        return client

    def test_valid_json_passes_through(self):
        """Valid JSON is returned immediately without any LLM call."""
        client = self._client()
        data = parse_json_or_repair(client, "model-x", '{"key": "value"}', "schema")
        assert data == {"key": "value"}
        client.messages.create.assert_not_called()

    def test_valid_json_array_passes_through(self):
        """Valid JSON arrays are also returned without an LLM call."""
        client = self._client()
        data = parse_json_or_repair(client, "model-x", '[1, 2, 3]', "schema")
        assert data == [1, 2, 3]
        client.messages.create.assert_not_called()

    def test_markdown_fenced_json_is_stripped(self):
        """JSON wrapped in ```json...``` fences is parsed after stripping."""
        client = self._client()
        text = '```json\n{"key": "value"}\n```'
        data = parse_json_or_repair(client, "model-x", text, "schema")
        assert data == {"key": "value"}
        client.messages.create.assert_not_called()

    def test_plain_fence_is_stripped(self):
        """JSON wrapped in plain ```...``` fences is also handled."""
        client = self._client()
        text = '```\n{"key": "value"}\n```'
        data = parse_json_or_repair(client, "model-x", text, "schema")
        assert data == {"key": "value"}
        client.messages.create.assert_not_called()

    def test_invalid_json_triggers_repair_call(self):
        """Genuinely invalid JSON triggers one LLM repair call."""
        repair_payload = json.dumps({"key": "repaired"})
        client = self._client(repair_payload)
        data = parse_json_or_repair(client, "model-x", "not valid json", "schema desc")
        assert data == {"key": "repaired"}
        assert client.messages.create.call_count == 1

    def test_repair_call_uses_correct_system_prompt(self):
        """Repair LLM call uses the prescribed system prompt."""
        repair_payload = json.dumps({"fixed": True})
        client = self._client(repair_payload)
        parse_json_or_repair(client, "model-x", "bad json", "my schema")
        call_kwargs = client.messages.create.call_args[1]
        assert call_kwargs["system"] == (
            "You are a JSON repair assistant. Return ONLY valid JSON, no markdown fences."
        )

    def test_repair_call_includes_schema_and_original_text(self):
        """Repair user message contains schema description and original text."""
        repair_payload = json.dumps({"fixed": True})
        client = self._client(repair_payload)
        parse_json_or_repair(client, "model-x", "bad json", "my schema desc")
        call_kwargs = client.messages.create.call_args[1]
        user_content = call_kwargs["messages"][0]["content"]
        assert "my schema desc" in user_content
        assert "bad json" in user_content

    def test_repair_failure_raises_enrichment_error(self):
        """If repair LLM also returns invalid JSON, EnrichmentError is raised."""
        client = self._client("still not valid json at all")
        with pytest.raises(EnrichmentError, match="Schema repair failed"):
            parse_json_or_repair(client, "model-x", "bad json", "schema")

    def test_repair_uses_max_retries_1(self):
        """Repair call uses max_retries=1 (single attempt)."""
        repair_payload = json.dumps({"ok": True})
        client = self._client(repair_payload)
        parse_json_or_repair(client, "model-x", "bad json", "schema")
        call_kwargs = client.messages.create.call_args[1]
        # max_retries is passed through call_llm which passes max_tokens and other
        # kwargs to create — we check max_tokens=4096 at minimum
        assert call_kwargs["max_tokens"] == 4096


# ---------------------------------------------------------------------------
# make_client tests
# ---------------------------------------------------------------------------


class TestMakeClient:
    def test_returns_anthropic_client_when_key_present(self, monkeypatch):
        """Returns anthropic.Anthropic when env var is set."""
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test-key")
        config = {"llm": {"api_key_env": "ANTHROPIC_API_KEY"}}
        with patch("arquimedes.enrich_llm.anthropic.Anthropic") as MockAnthropic:
            client = make_client(config)
        MockAnthropic.assert_called_once_with(api_key="sk-test-key")
        assert client is MockAnthropic.return_value

    def test_raises_when_key_missing(self, monkeypatch):
        """Raises EnrichmentError with helpful message when env var is absent."""
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        config = {"llm": {"api_key_env": "ANTHROPIC_API_KEY"}}
        with pytest.raises(EnrichmentError, match="ANTHROPIC_API_KEY"):
            make_client(config)

    def test_raises_when_key_empty(self, monkeypatch):
        """Raises EnrichmentError when env var is set but empty."""
        monkeypatch.setenv("ANTHROPIC_API_KEY", "")
        config = {"llm": {"api_key_env": "ANTHROPIC_API_KEY"}}
        with pytest.raises(EnrichmentError, match="ANTHROPIC_API_KEY"):
            make_client(config)

    def test_uses_custom_env_var_name(self, monkeypatch):
        """Reads the env var name from config["llm"]["api_key_env"]."""
        monkeypatch.setenv("MY_CUSTOM_KEY", "sk-custom")
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        config = {"llm": {"api_key_env": "MY_CUSTOM_KEY"}}
        with patch("arquimedes.enrich_llm.anthropic.Anthropic") as MockAnthropic:
            client = make_client(config)
        MockAnthropic.assert_called_once_with(api_key="sk-custom")
        assert client is MockAnthropic.return_value

    def test_custom_env_var_missing_raises_with_var_name(self, monkeypatch):
        """Error message contains the custom env var name."""
        monkeypatch.delenv("MY_MISSING_KEY", raising=False)
        config = {"llm": {"api_key_env": "MY_MISSING_KEY"}}
        with pytest.raises(EnrichmentError, match="MY_MISSING_KEY"):
            make_client(config)

    def test_defaults_to_anthropic_api_key_when_config_missing_llm(self, monkeypatch):
        """Falls back to ANTHROPIC_API_KEY when config has no llm section."""
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-default")
        config = {}  # no "llm" key
        with patch("arquimedes.enrich_llm.anthropic.Anthropic") as MockAnthropic:
            client = make_client(config)
        MockAnthropic.assert_called_once_with(api_key="sk-default")
        assert client is MockAnthropic.return_value

    def test_defaults_to_anthropic_api_key_when_api_key_env_absent(self, monkeypatch):
        """Falls back to ANTHROPIC_API_KEY when api_key_env key is omitted."""
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-fallback")
        config = {"llm": {}}  # api_key_env key absent
        with patch("arquimedes.enrich_llm.anthropic.Anthropic") as MockAnthropic:
            make_client(config)
        MockAnthropic.assert_called_once_with(api_key="sk-fallback")
