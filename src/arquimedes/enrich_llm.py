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
import os
import re
import shutil
import signal
import subprocess
import threading
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
# Model identifier from config
# ---------------------------------------------------------------------------


def get_model_id(config: dict) -> str:
    """Derive a stable model identifier from the agent_cmd config.

    Uses the full agent commands (e.g. ``"claude --print|codex exec"``).  This
    is stored in stamps for staleness detection — if the config changes,
    artifacts re-enrich.
    """
    raw_cmd = config.get("llm", {}).get("agent_cmd", "claude --print")
    if isinstance(raw_cmd, str):
        cmds = [raw_cmd]
    elif isinstance(raw_cmd, list):
        cmds = [c.strip() for c in raw_cmd if isinstance(c, str) and c.strip()]
    else:
        cmds = ["claude --print"]
    return "|".join(cmds) if cmds else "unknown"


def get_agent_model_name(base_parts: list[str]) -> str:
    """Derive a human-readable model name from an agent command.

    For ``claude``: reads the ``--model`` arg if present, else ``"claude"``.
    For ``codex``: returns ``"codex"``.
    Otherwise: returns the executable basename.
    """
    exe = os.path.basename(base_parts[0])
    if exe == "claude":
        try:
            idx = base_parts.index("--model")
            return f"claude:{base_parts[idx + 1]}"
        except (ValueError, IndexError):
            return "claude"
    return exe


# ---------------------------------------------------------------------------
# Agent CLI adapter (default — shells out to configurable agent command)
# ---------------------------------------------------------------------------


# Patterns that indicate an agent will never succeed — kill immediately.
_FAST_FAIL_RE = re.compile(
    r"not logged in|/login|rate.?limit|unauthorized|quota.?exceeded"
    r"|authentication.?failed|exceeded your|too many requests",
    re.IGNORECASE,
)


def _run_agent_subprocess(
    cmd: list[str],
    stdin_text: str,
    timeout: int,
) -> subprocess.CompletedProcess[str]:
    """Run an agent CLI with fast-fail stderr monitoring.

    Monitors stderr in a background thread.  If a fast-fail pattern is
    detected (auth error, rate-limit, etc.) the process is killed
    immediately instead of waiting for it to hang or time out.
    """
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,  # own process group so we can kill children
    )

    # Write stdin — the agent reads the prompt from here
    try:
        proc.stdin.write(stdin_text)  # type: ignore[union-attr]
        proc.stdin.close()  # type: ignore[union-attr]
    except BrokenPipeError:
        pass  # process already exited

    stdout_chunks: list[str] = []
    stderr_chunks: list[str] = []
    done = threading.Event()

    def _kill_tree() -> None:
        """Kill the process and all its children (entire process group)."""
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            try:
                proc.kill()
            except ProcessLookupError:
                pass

    def _read_stdout() -> None:
        try:
            assert proc.stdout is not None
            while True:
                chunk = proc.stdout.read(4096)
                if not chunk:
                    break
                stdout_chunks.append(chunk)
        except (ValueError, OSError):
            pass
        done.set()

    def _read_stderr() -> None:
        try:
            assert proc.stderr is not None
            while True:
                line = proc.stderr.readline()
                if not line:
                    break
                stderr_chunks.append(line)
                if _FAST_FAIL_RE.search(line):
                    _kill_tree()
                    done.set()
                    return
        except (ValueError, OSError):
            pass

    t_out = threading.Thread(target=_read_stdout, daemon=True)
    t_err = threading.Thread(target=_read_stderr, daemon=True)
    t_out.start()
    t_err.start()

    # Wait for stdout EOF (normal) or fast-fail kill
    if not done.wait(timeout=timeout):
        _kill_tree()
        t_out.join(timeout=5)
        t_err.join(timeout=5)
        proc.wait()
        raise subprocess.TimeoutExpired(cmd, timeout)

    proc.wait(timeout=10)
    t_out.join(timeout=5)
    t_err.join(timeout=5)

    return subprocess.CompletedProcess(
        cmd, proc.returncode or 0, "".join(stdout_chunks), "".join(stderr_chunks)
    )


def _build_prompt_text(system: str, messages: list[dict]) -> tuple[str, str]:
    """Flatten system + messages into a system prompt and user prompt for the CLI agent.

    Returns (system_prompt, user_prompt).

    For multimodal messages (image blocks in figure enrichment), image files
    are referenced by their original file path so the agent can read them.
    """
    parts = []
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
    return system, "\n".join(parts)


def _build_agent_cmd(base_parts: list[str], system: str) -> list[str]:
    """Build the full command for an agent CLI, adding speed optimizations.

    For ``claude``: adds flags to minimize startup overhead without
    breaking credential discovery (``--bare`` is avoided for that reason):
    - ``--no-session-persistence``: skip saving session to disk
    - ``--system-prompt``: pass system prompt natively
    - ``--tools ""``: disable built-in tools (we only need text output)
    - ``--disable-slash-commands``: skip skill resolution

    For ``codex``: adds ``--ephemeral`` and ``--skip-git-repo-check``
    to reduce startup overhead and avoid repo enforcement.

    Other agents get the base command as-is (system prompt stays in stdin).
    """
    exe = base_parts[0]
    if exe == "claude":
        cmd = list(base_parts)
        if "--no-session-persistence" not in cmd:
            cmd.append("--no-session-persistence")
        if "--disable-slash-commands" not in cmd:
            cmd.append("--disable-slash-commands")
        # Disable all built-in tools — we only want a text response
        if "--tools" not in cmd:
            cmd.extend(["--tools", ""])
        # Default to sonnet for cost/speed balance
        if "--model" not in cmd:
            cmd.extend(["--model", "sonnet"])
        cmd.extend(["--system-prompt", system])
        return cmd
    if exe == "codex":
        cmd = list(base_parts)
        if "--ephemeral" not in cmd:
            cmd.append("--ephemeral")
        if "--skip-git-repo-check" not in cmd:
            cmd.append("--skip-git-repo-check")
        return cmd
    return list(base_parts)


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
        system_prompt, user_prompt = _build_prompt_text(system, messages)

        for base_parts in resolved:
            cmd_name = base_parts[0]
            cmd = _build_agent_cmd(base_parts, system_prompt)
            # For non-claude agents, prepend system to stdin
            stdin_text = user_prompt if cmd_name == "claude" else f"[SYSTEM]\n{system_prompt}\n\n{user_prompt}"
            last_exc: Exception | None = None

            for attempt in range(max_retries):
                try:
                    result = _run_agent_subprocess(
                        cmd, stdin_text, timeout=300,
                    )
                    if result.returncode != 0:
                        detail = result.stderr.strip() or result.stdout.strip()
                        last_exc = EnrichmentError(
                            f"{cmd_name} failed (exit {result.returncode}): "
                            f"{detail[:500]}"
                        )
                        break  # don't retry non-timeout failures, try next agent
                    # Record which agent+model actually responded
                    llm_fn.last_model = get_agent_model_name(cmd)
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

    llm_fn.last_model = "unknown"  # type: ignore[attr-defined]
    return llm_fn  # type: ignore[return-value]
