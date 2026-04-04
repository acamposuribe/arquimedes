"""LLM abstraction layer for Phase 3 enrichment.

The enrichment pipeline does not depend on any specific LLM provider.
All stages receive an ``LlmFn`` — a callable with signature::

    (system: str, messages: list[dict]) -> str

The default implementation (``make_cli_llm_fn``) shells out to a
configurable agent CLI (e.g. ``claude``, ``openai``, ``gemini``).
The agent authenticates with its own credentials — no API keys needed
in this codebase.  Callers can also supply a custom ``LlmFn`` directly.

Also provides:
- parse_json_or_repair: JSON parse with markdown fence stripping and
  one-shot LLM schema repair fallback
- EnrichmentError: raised on unrecoverable enrichment failures
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
from typing import Callable

# ---------------------------------------------------------------------------
# Type alias
# ---------------------------------------------------------------------------

LlmFn = Callable[[str, list[dict]], str]
"""(system_prompt, messages) -> response_text"""


# ---------------------------------------------------------------------------
# Custom exception
# ---------------------------------------------------------------------------


class EnrichmentError(Exception):
    """Raised when LLM enrichment fails unrecoverably."""


# ---------------------------------------------------------------------------
# JSON parse + markdown fence strip + one-shot repair
# ---------------------------------------------------------------------------

_FENCE_RE = re.compile(r"^```(?:json)?\s*\n?(.*?)\n?```\s*$", re.DOTALL)


def _strip_fences(text: str) -> str:
    """Remove surrounding ```json...``` or ```...``` fences if present."""
    m = _FENCE_RE.match(text.strip())
    return m.group(1).strip() if m else text.strip()


def parse_json_or_repair(
    llm_fn: LlmFn,
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
    repair_response = llm_fn(
        "You are a JSON repair assistant. Return ONLY valid JSON, no markdown fences.",
        [
            {
                "role": "user",
                "content": (
                    f"Return valid JSON matching this schema:\n{schema_description}"
                    f"\n\nYour previous output was:\n{text}"
                ),
            }
        ],
    )

    try:
        return json.loads(repair_response)
    except json.JSONDecodeError:
        raise EnrichmentError("Schema repair failed")


# ---------------------------------------------------------------------------
# Agent CLI adapter (default — shells out to configurable agent command)
# ---------------------------------------------------------------------------


def _build_prompt_text(system: str, messages: list[dict]) -> str:
    """Flatten system + messages into a single text prompt for the CLI agent.

    For multimodal messages (image blocks in figure enrichment), image files
    are referenced by their original file path so the agent can read them.
    """
    parts = [f"[SYSTEM]\n{system}\n"]
    for msg in messages:
        role = msg.get("role", "user").upper()
        content = msg.get("content", "")
        if isinstance(content, str):
            parts.append(f"[{role}]\n{content}\n")
        elif isinstance(content, list):
            # Multimodal content blocks (text + image)
            text_parts = []
            for block in content:
                if isinstance(block, dict) and block.get("type") == "text":
                    text_parts.append(block["text"])
                elif isinstance(block, dict) and block.get("type") == "image":
                    # Include image file reference for the agent CLI
                    source_path = block.get("_source_path", "")
                    if source_path:
                        text_parts.append(
                            f"[IMAGE: {source_path}]\n"
                            f"(Read and analyze this image file for figure enrichment)\n"
                        )
                elif isinstance(block, str):
                    text_parts.append(block)
            if text_parts:
                parts.append(f"[{role}]\n{''.join(text_parts)}\n")
    return "\n".join(parts)


def make_cli_llm_fn(config: dict) -> LlmFn:
    """Build an LlmFn that shells out to an agent CLI.

    Reads the command(s) from ``config["llm"]["agent_cmd"]``.  Accepts a
    single string or a **list of strings** (tried in order — first success
    wins, next command tried on failure).  The agent authenticates with its
    own credentials — no API keys needed in this codebase.

    The prompt is passed via stdin.  The agent's stdout is the response.

    Raises EnrichmentError if none of the configured commands are found.
    """
    raw_cmd = config.get("llm", {}).get("agent_cmd", "claude --print")
    if isinstance(raw_cmd, str):
        cmd_list = [raw_cmd]
    elif isinstance(raw_cmd, list):
        cmd_list = [c for c in raw_cmd if isinstance(c, str) and c.strip()]
    else:
        cmd_list = ["claude --print"]

    if not cmd_list:
        raise EnrichmentError(
            "No agent CLI configured. Set llm.agent_cmd in config.yaml "
            "(e.g. 'claude --print', 'codex exec')."
        )

    # Resolve each command to its parts and verify at least one exists
    resolved: list[list[str]] = []
    for cmd_str in cmd_list:
        parts = cmd_str.split()
        if shutil.which(parts[0]):
            resolved.append(parts)

    if not resolved:
        names = ", ".join(repr(c.split()[0]) for c in cmd_list)
        raise EnrichmentError(
            f"No agent CLI found on PATH (tried {names}). "
            f"Install one or set llm.agent_cmd in config.yaml."
        )

    max_retries: int = config.get("enrichment", {}).get("max_retries", 3)

    def llm_fn(system: str, messages: list[dict]) -> str:
        prompt_text = _build_prompt_text(system, messages)

        for cmd_parts in resolved:
            cmd_name = cmd_parts[0]
            last_exc: Exception | None = None

            for attempt in range(max_retries):
                try:
                    result = subprocess.run(
                        cmd_parts,
                        input=prompt_text,
                        capture_output=True,
                        text=True,
                        timeout=300,  # 5-minute timeout per call
                    )
                    if result.returncode != 0:
                        detail = result.stderr.strip() or result.stdout.strip()
                        last_exc = EnrichmentError(
                            f"{cmd_name} failed (exit {result.returncode}): "
                            f"{detail[:500]}"
                        )
                        break  # don't retry non-timeout failures, try next agent
                    return result.stdout
                except subprocess.TimeoutExpired:
                    last_exc = EnrichmentError(
                        f"{cmd_name} timed out after 300s "
                        f"(attempt {attempt + 1}/{max_retries})"
                    )
                except FileNotFoundError:
                    last_exc = EnrichmentError(f"Agent CLI not found: {cmd_name!r}")
                    break

            # If we get here, this agent failed — log and try next
            if len(resolved) > 1:
                import sys
                print(f"  [{cmd_name}] {last_exc} — trying next agent", file=sys.stderr)
                continue

        # All agents exhausted
        raise last_exc  # type: ignore[arg-type]

    return llm_fn
