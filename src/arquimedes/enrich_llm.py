"""Thin LLM client wrapper for Phase 3 enrichment.

Provides:
- call_llm: retry-aware Anthropic messages.create call
- parse_json_or_repair: JSON parse with markdown fence stripping and one-shot
  LLM schema repair fallback
- make_client: construct anthropic.Anthropic from config + env
"""

from __future__ import annotations

import json
import os
import re
import time

import anthropic


# ---------------------------------------------------------------------------
# Custom exception
# ---------------------------------------------------------------------------


class EnrichmentError(Exception):
    """Raised when LLM enrichment fails unrecoverably."""


# ---------------------------------------------------------------------------
# LLM call with retry
# ---------------------------------------------------------------------------


def call_llm(
    client,
    model: str,
    system: str,
    messages: list[dict],
    max_tokens: int = 4096,
    max_retries: int = 3,
) -> str:
    """Call client.messages.create and return the first text content block.

    Retries on RateLimitError and APIConnectionError with exponential backoff
    (``time.sleep(2 ** attempt)``).  After max_retries exhausted, re-raises
    the last exception.  All other errors propagate immediately.
    """
    last_exc: Exception | None = None
    for attempt in range(max_retries):
        try:
            response = client.messages.create(
                model=model,
                max_tokens=max_tokens,
                system=system,
                messages=messages,
            )
            return response.content[0].text
        except (anthropic.RateLimitError, anthropic.APIConnectionError) as exc:
            last_exc = exc
            time.sleep(2 ** attempt)
    raise last_exc


# ---------------------------------------------------------------------------
# JSON parse + markdown fence strip + one-shot repair
# ---------------------------------------------------------------------------

_FENCE_RE = re.compile(r"^```(?:json)?\s*\n?(.*?)\n?```\s*$", re.DOTALL)


def _strip_fences(text: str) -> str:
    """Remove surrounding ```json...``` or ```...``` fences if present."""
    m = _FENCE_RE.match(text.strip())
    return m.group(1).strip() if m else text.strip()


def parse_json_or_repair(
    client,
    model: str,
    text: str,
    schema_description: str,
) -> dict:
    """Parse JSON from *text*, repairing via LLM if needed.

    Steps:
    1. Try json.loads(text) directly — return on success.
    2. Strip markdown code fences and retry json.loads — return on success.
    3. Make ONE schema-repair LLM call and parse the result.
    4. If still invalid, raise EnrichmentError("Schema repair failed").
    """
    # Step 1: direct parse
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Step 2: strip fences and retry
    stripped = _strip_fences(text)
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        pass

    # Step 3: one-shot LLM schema repair
    repair_response = call_llm(
        client=client,
        model=model,
        system="You are a JSON repair assistant. Return ONLY valid JSON, no markdown fences.",
        messages=[
            {
                "role": "user",
                "content": (
                    f"Return valid JSON matching this schema:\n{schema_description}"
                    f"\n\nYour previous output was:\n{text}"
                ),
            }
        ],
        max_tokens=4096,
        max_retries=1,
    )

    try:
        return json.loads(repair_response)
    except json.JSONDecodeError:
        raise EnrichmentError("Schema repair failed")


# ---------------------------------------------------------------------------
# Client factory
# ---------------------------------------------------------------------------


def make_client(config: dict) -> anthropic.Anthropic:
    """Construct an anthropic.Anthropic client from *config* and environment.

    Reads the API key env var name from ``config["llm"]["api_key_env"]``
    (default ``"ANTHROPIC_API_KEY"``).  Raises EnrichmentError if the env var
    is missing or empty.
    """
    key_env: str = config.get("llm", {}).get("api_key_env", "ANTHROPIC_API_KEY")
    if not os.environ.get(key_env):
        raise EnrichmentError(f"Set {key_env} environment variable to use LLM enrichment")
    return anthropic.Anthropic(api_key=os.environ[key_env])
