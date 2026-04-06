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
import shlex
import re
import shutil
import signal
import subprocess
import threading
import sys
import urllib.request
import urllib.error
from typing import Callable

# ---------------------------------------------------------------------------
# Type alias
# ---------------------------------------------------------------------------

LlmFn = Callable[[str, list[dict]], str]
"""(system_prompt, messages) -> response_text"""


def _llm_debug_enabled() -> bool:
    value = os.getenv("ARQ_LLM_DEBUG", "")
    return value.strip().lower() not in {"", "0", "false", "no", "off"}


def _llm_debug_preview(text: str, limit: int = 240) -> str:
    flat = text.replace("\n", "\\n")
    return flat[:limit] + ("..." if len(flat) > limit else "")


def _llm_debug(message: str) -> None:
    if _llm_debug_enabled():
        print(f"[llm-debug] {message}", file=sys.stderr)


def _coerce_timeout_seconds(value, default: int | None = None) -> int | None:
    try:
        timeout_seconds = int(value)
    except (TypeError, ValueError):
        timeout_seconds = default
    if timeout_seconds is None:
        return None
    return max(timeout_seconds, 1)

# ---------------------------------------------------------------------------
# Claude OAuth usage pre-flight check
# ---------------------------------------------------------------------------

_CLAUDE_USAGE_EXHAUSTED_THRESHOLD = float(
    os.getenv("ARQ_CLAUDE_USAGE_THRESHOLD", "90")
)


def check_claude_oauth_usage() -> dict | None:
    """Fetch current Claude OAuth usage from the Anthropic API.

    Returns the parsed JSON dict on success, or None if the check cannot be
    performed (missing keychain, network error, etc.).

    Reads credentials from the macOS keychain (``Claude Code-credentials``)
    or falls back to ``~/.claude/.credentials.json``.
    """
    token = _read_claude_oauth_token()
    if not token:
        return None
    try:
        req = urllib.request.Request(
            "https://api.anthropic.com/api/oauth/usage",
            headers={
                "Authorization": f"Bearer {token}",
                "anthropic-beta": "oauth-2025-04-20",
            },
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.loads(resp.read().decode())
    except Exception:
        return None


def _read_claude_oauth_token() -> str | None:
    """Read the Claude Code OAuth access token from keychain or file fallback."""
    # macOS keychain
    try:
        result = subprocess.run(
            ["security", "find-generic-password", "-s", "Claude Code-credentials", "-w"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0 and result.stdout.strip():
            data = json.loads(result.stdout.strip())
            token = (data.get("claudeAiOauth") or {}).get("accessToken")
            if token:
                return token
    except Exception:
        pass
    # File fallback
    try:
        creds_path = os.path.expanduser("~/.claude/.credentials.json")
        with open(creds_path) as f:
            data = json.load(f)
        token = (data.get("claudeAiOauth") or {}).get("accessToken")
        if token:
            return token
    except Exception:
        pass
    return None


def _claude_usage_over_threshold(threshold: float = _CLAUDE_USAGE_EXHAUSTED_THRESHOLD) -> bool:
    """Return True if any Claude usage window is at or above *threshold* percent.

    Checks ``five_hour`` and ``seven_day`` windows. Returns False if the usage
    data cannot be fetched (fail open — let the provider attempt run normally).
    """
    usage = check_claude_oauth_usage()
    if not usage:
        return False
    for window in ("five_hour", "seven_day"):
        entry = usage.get(window)
        if isinstance(entry, dict):
            util = entry.get("utilization")
            if isinstance(util, (int, float)) and util >= threshold:
                _llm_debug(
                    f"claude usage pre-flight: {window} utilization={util}% >= threshold={threshold}% — skipping claude"
                )
                return True
    return False


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


def _parse_json_prefix(text: str):
    """Parse the first JSON value in *text*, ignoring trailing noise."""
    decoder = json.JSONDecoder()
    stripped = text.lstrip()
    if not stripped:
        raise json.JSONDecodeError("Empty input", text, 0)
    value, end_index = decoder.raw_decode(stripped)
    trailing = stripped[end_index:].strip()
    if trailing:
        _llm_debug(
            f"json prefix parsed with trailing noise ignored; trailing_chars={len(trailing)} preview={_llm_debug_preview(trailing)}"
        )
    return value


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
        _llm_debug(
            f"initial JSON parse failed; text_chars={len(text)} preview={_llm_debug_preview(text)}"
        )

    # Step 2: strip fences and retry
    stripped = _strip_fences(text)
    try:
        return json.loads(stripped)
    except json.JSONDecodeError:
        if stripped != text:
            _llm_debug(
                f"fence-stripped JSON parse failed; text_chars={len(stripped)} preview={_llm_debug_preview(stripped)}"
            )

    # Step 3: salvage a valid JSON prefix before asking the LLM to repair.
    try:
        return _parse_json_prefix(stripped)
    except json.JSONDecodeError:
        pass

    # Step 4: one-shot LLM schema repair
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

    _llm_debug(
        f"schema repair response_chars={len(repair_response)} preview={_llm_debug_preview(repair_response)}"
    )

    try:
        return json.loads(repair_response)
    except json.JSONDecodeError:
        _llm_debug("schema repair failed to produce valid JSON")
        raise EnrichmentError("Schema repair failed")


# ---------------------------------------------------------------------------
# Model identifier from config
# ---------------------------------------------------------------------------


def get_model_id(config: dict, stage: str | None = None) -> str:
    """Derive a stable model identifier from the config.

    If stage-specific routes are configured, return a stable signature for the
    ordered route list for that stage. Otherwise fall back to the configured
    ``llm.agent_cmd`` command list (e.g. ``"claude --print|codex exec"``).

    The value is stored in stamps for audit/debugging.
    """
    routes = _stage_route_config(config, stage)
    if routes:
        return "|".join(_route_signature(route) for route in routes)

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
    For ``codex``: reads ``-m`` if present, else ``"codex"``.
    For ``copilot``: reads ``--model`` if present, else ``"copilot"``.
    Otherwise: returns the executable basename.
    """
    exe = os.path.basename(base_parts[0])
    if exe == "claude":
        try:
            idx = base_parts.index("--model")
            return f"claude:{base_parts[idx + 1]}"
        except (ValueError, IndexError):
            return "claude"
    if exe == "copilot":
        try:
            idx = base_parts.index("--model")
            return f"copilot:{base_parts[idx + 1]}"
        except (ValueError, IndexError):
            return "copilot"
    if exe == "codex":
        try:
            idx = base_parts.index("-m")
            return f"codex:{base_parts[idx + 1]}"
        except (ValueError, IndexError):
            return "codex"
    return exe


def _command_to_parts(command_spec) -> list[str]:
    """Normalize a command spec string/list into argv parts."""
    if isinstance(command_spec, list):
        return [part for part in command_spec if isinstance(part, str) and part.strip()]
    if isinstance(command_spec, str):
        return shlex.split(command_spec)
    return []


def _provider_from_parts(parts: list[str]) -> str:
    """Infer provider name from the executable basename."""
    if not parts:
        return "unknown"
    return os.path.basename(parts[0])


def _stage_route_config(config: dict, stage: str | None) -> list[dict]:
    """Return normalized per-stage route entries if configured."""
    if not stage:
        return []
    enrichment = config.get("enrichment", {})
    routes_cfg = enrichment.get("llm_routes")
    if not isinstance(routes_cfg, dict):
        routes_cfg = enrichment.get("routes")
    if not isinstance(routes_cfg, dict):
        return []

    stage_cfg = routes_cfg.get(stage)
    if not isinstance(stage_cfg, list):
        return []

    routes: list[dict] = []
    for entry in stage_cfg:
        if isinstance(entry, str):
            parts = _command_to_parts(entry)
            if parts:
                routes.append({
                    "provider": _provider_from_parts(parts),
                    "command_parts": parts,
                })
            continue
        if not isinstance(entry, dict):
            continue
        command_spec = entry.get("command") or entry.get("cmd") or entry.get("agent_cmd")
        parts = _command_to_parts(command_spec)
        if not parts:
            continue
        routes.append({
            "provider": entry.get("provider") or _provider_from_parts(parts),
            "command_parts": parts,
            "model": entry.get("model"),
            "effort": entry.get("effort"),
            "tools": entry.get("tools"),
            "agent": entry.get("agent"),
            "timeout_seconds": entry.get("timeout_seconds"),
            "prompt_mode": entry.get("prompt_mode"),
            "silent": entry.get("silent"),
            "allow_all": entry.get("allow_all"),
            "allow_all_tools": entry.get("allow_all_tools"),
            "allow_all_paths": entry.get("allow_all_paths"),
            "allow_all_urls": entry.get("allow_all_urls"),
            "available_tools": entry.get("available_tools"),
            "excluded_tools": entry.get("excluded_tools"),
            "no_ask_user": entry.get("no_ask_user"),
            "no_auto_update": entry.get("no_auto_update"),
            "no_custom_instructions": entry.get("no_custom_instructions"),
            "fast_fail": entry.get("fast_fail"),
            "bare": entry.get("bare"),
        })
    return routes


def _route_signature(route: dict) -> str:
    """Compact, stable string describing a route for audit and combine checks."""
    provider = str(route.get("provider") or "unknown")
    model = route.get("model")
    effort = route.get("effort")
    command_parts = route.get("command_parts") or []
    command = " ".join(command_parts) if isinstance(command_parts, list) else str(command_parts)
    bits = [provider]
    if model:
        bits.append(str(model))
    if effort:
        bits.append(str(effort))
    if command:
        bits.append(command)
    return ":".join(bits)


def _route_flag(route: dict, key: str, default: bool) -> bool:
    value = route.get(key)
    if value is None:
        return default
    return bool(value)


def _tools_arg(tools) -> str | None:
    if tools is None:
        return None
    if isinstance(tools, str):
        value = tools.strip()
        return value if value else ""
    if isinstance(tools, list):
        cleaned = [str(tool).strip() for tool in tools if str(tool).strip()]
        return ",".join(cleaned)
    return None


def _is_exhaustion_signal(text: str) -> bool:
    return bool(_FAST_FAIL_RE.search(text or ""))


# ---------------------------------------------------------------------------
# Agent CLI adapter (default — shells out to configurable agent command)
# ---------------------------------------------------------------------------


# Patterns that indicate an agent will never succeed — kill immediately.
_FAST_FAIL_RE = re.compile(
    r"not logged in|/login|rate.?limit|unauthorized|quota.?exceeded"
    r"|authentication.?failed|exceeded your|too many requests",
    re.IGNORECASE,
)

_COMPLETION_SENTINELS = {"PROCESS_FINISHED", "x"}

def _strip_nuls(text: str) -> str:
    """Remove embedded NUL bytes that break subprocess argv/stdin handling."""
    return text.replace("\x00", "")


def _run_agent_subprocess(
    cmd: list[str],
    stdin_text: str,
    timeout: int | None,
    fast_fail: bool = True,
) -> subprocess.CompletedProcess[str]:
    """Run an agent CLI with optional fast-fail stderr monitoring.

    When *fast_fail* is True, monitors stderr in a background thread and
    kills the process immediately if an auth/rate-limit pattern is detected.

    Set *fast_fail* to False for agents that echo the full prompt to stderr
    (e.g. codex), which would otherwise cause false-positive kills on
    document content containing words like 'unauthorized' or 'exceeded'.
    """
    safe_cmd = [_strip_nuls(part) for part in cmd]
    env = os.environ.copy()
    if "--bare" not in safe_cmd:
        # Legacy env-var suppression — not needed in --bare mode (handled natively).
        env["CLAUDE_CODE_DISABLE_1M_CONTEXT"] = "1"
        env["CLAUDE_CODE_DISABLE_CLAUDE_MDS"] = "1"
        env["CLAUDE_CODE_DISABLE_AUTO_MEMORY"] = "1"
        env["CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC"] = "1"
        env["CLAUDE_CODE_DISABLE_ADAPTIVE_THINKING"] = "1"
        env["CLAUDE_CODE_DISABLE_HOOKS"] = "1"
        env["MAX_THINKING_TOKENS"] = "0"
        env["ENABLE_CLAUDEAI_MCP_SERVERS"] = "false"

    safe_stdin = _strip_nuls(stdin_text)

    proc = subprocess.Popen(
        safe_cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
        start_new_session=True,  # own process group so we can kill children
    )

    # Write stdin — the agent reads the prompt from here
    try:
        proc.stdin.write(safe_stdin)  # type: ignore[union-attr]
        proc.stdin.close()  # type: ignore[union-attr]
    except BrokenPipeError:
        pass  # process already exited

    stdout_chunks: list[str] = []
    stderr_chunks: list[str] = []
    done = threading.Event()
    completion_seen = threading.Event()

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
                line = proc.stdout.readline()
                if not line:
                    break
                stdout_chunks.append(line)
                if line.strip() in _COMPLETION_SENTINELS:
                    completion_seen.set()
                    _kill_tree()
                    done.set()
                    return
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
                if fast_fail and _FAST_FAIL_RE.search(line):
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
        raise subprocess.TimeoutExpired(safe_cmd, timeout)

    proc.wait(timeout=10)
    t_out.join(timeout=5)
    t_err.join(timeout=5)

    if completion_seen.is_set():
        return subprocess.CompletedProcess(
            safe_cmd,
            0,
            "".join(stdout_chunks),
            "".join(stderr_chunks),
        )

    return subprocess.CompletedProcess(
        safe_cmd, proc.returncode or 0, "".join(stdout_chunks), "".join(stderr_chunks)
    )


def _build_prompt_text(system: str, messages: list[dict]) -> tuple[str, str]:
    """Flatten system + messages into a system prompt and user prompt for the CLI agent.

    Returns (system_prompt, user_prompt).

    For multimodal messages (image blocks in figure enrichment), image files
    are referenced by their original file path so the agent can read them.
    """
    system = _strip_nuls(system)
    parts = []
    for msg in messages:
        role = msg.get("role", "user").upper()
        content = msg.get("content", "")
        if isinstance(content, str):
            parts.append(f"[{role}]\n{_strip_nuls(content)}\n")
        elif isinstance(content, list):
            # Multimodal content blocks (text + image)
            text_parts = []
            for block in content:
                if isinstance(block, dict) and block.get("type") == "text":
                    text_parts.append(_strip_nuls(block["text"]))
                elif isinstance(block, dict) and block.get("type") == "image":
                    # Include image file reference for the agent CLI
                    source_path = _strip_nuls(block.get("_source_path", ""))
                    if source_path:
                        text_parts.append(
                            f"[IMAGE: {source_path}]\n"
                            f"(Read and analyze this image file for figure enrichment)\n"
                        )
                elif isinstance(block, str):
                    text_parts.append(_strip_nuls(block))
            if text_parts:
                parts.append(f"[{role}]\n{''.join(text_parts)}\n")
    return system, "\n".join(parts)


def _build_agent_cmd(
    base_parts: list[str],
    system: str,
    *,
    effort: str | None = None,
    model_override: str | None = None,
    tools=None,
    disable_slash_commands: bool = True,
    bare: bool = False,
) -> list[str]:
    """Build the full command for an agent CLI, adding speed optimizations.

    For ``claude``: adds flags to minimize startup overhead without
    breaking credential discovery (``--bare`` is avoided for that reason):
    - ``--no-session-persistence``: skip saving session to disk
    - ``--system-prompt``: pass system prompt natively
    - ``--tools Read,Write,Bash``: default tool surface for our Claude routes
    - ``--disable-slash-commands``: skip skill resolution
    - ``--effort``: control thinking budget (low/medium/high)
    - ``--model``: per-stage model override (e.g. haiku for chunks)

    For ``codex``: adds ``--ephemeral`` and optional ``-m`` /
    ``-c model_reasoning_effort=...`` overrides.

    Other agents get the base command as-is (system prompt stays in stdin).
    """
    exe = base_parts[0]
    if exe == "claude":
        cmd = list(base_parts)
        if bare and "--bare" not in cmd:
            cmd.append("--bare")
        if bare:
            # --bare handles sessions, hooks, memory natively — inject settings
            # for OAuth (apiKeyHelper) and skip-permissions for headless use.
            if "--settings" not in cmd:
                _project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                _settings_path = os.path.join(_project_root, "scripts", "claude_bare_settings.json")
                cmd.extend(["--settings", _settings_path])
            if "--dangerously-skip-permissions" not in cmd:
                cmd.append("--dangerously-skip-permissions")
        else:
            # Legacy non-bare flags
            if "--no-session-persistence" not in cmd:
                cmd.append("--no-session-persistence")
            if disable_slash_commands and "--disable-slash-commands" not in cmd:
                cmd.append("--disable-slash-commands")
        tools_value = _tools_arg(tools if tools is not None else ["Read", "Write", "Bash"])
        if "--tools" not in cmd and "--allowedTools" not in cmd and "--allowed-tools" not in cmd:
            if tools_value is None:
                cmd.extend(["--tools", "Read,Write,Bash"])
            else:
                cmd.extend(["--tools", tools_value])
        # Model: per-stage override takes precedence, then default to sonnet
        if "--model" not in cmd:
            cmd.extend(["--model", model_override or "sonnet"])
        elif model_override and "--model" in cmd:
            # Replace existing default model with stage-specific override
            idx = cmd.index("--model")
            cmd[idx + 1] = model_override
        # Control thinking budget — bare defaults to low (no thinking)
        if "--effort" not in cmd:
            effective_effort = effort or ("low" if bare else None)
            if effective_effort:
                cmd.extend(["--effort", effective_effort])
        cmd.extend(["--system-prompt", system])
        return cmd
    if exe == "codex":
        cmd = list(base_parts)
        if "--ephemeral" not in cmd:
            cmd.append("--ephemeral")
        if model_override and "-m" not in cmd:
            cmd.extend(["-m", model_override])
        if effort:
            cmd.extend(["-c", f"model_reasoning_effort={effort}"])
        return cmd
    return list(base_parts)


def _build_stage_request(
    base_parts: list[str],
    provider: str,
    system: str,
    user_prompt: str,
    *,
    model: str | None = None,
    effort: str | None = None,
    route: dict | None = None,
) -> tuple[list[str], str, bool]:
    """Return (cmd, stdin_text, fast_fail) for a provider-specific attempt."""
    route = route or {}
    provider = provider.lower()
    if provider == "claude":
        cmd = _build_agent_cmd(
            base_parts,
            system,
            effort=effort,
            model_override=model,
            tools=route.get("tools"),
            disable_slash_commands=_route_flag(route, "disable_slash_commands", True),
            bare=_route_flag(route, "bare", False),
        )
        return cmd, user_prompt, True

    if provider == "codex":
        cmd = list(base_parts)
        if "--ephemeral" not in cmd:
            cmd.append("--ephemeral")
        if model and "-m" not in cmd:
            cmd.extend(["-m", model])
        elif model and "-m" in cmd:
            idx = cmd.index("-m")
            if idx + 1 < len(cmd):
                cmd[idx + 1] = model
        if effort and "-c" not in cmd:
            cmd.extend(["-c", f"model_reasoning_effort={effort}"])
        return cmd, f"[SYSTEM]\n{system}\n\n{user_prompt}", False

    if provider == "copilot":
        cmd = list(base_parts)
        agent_name = route.get("agent")
        if agent_name:
            if "--agent" not in cmd:
                cmd.extend(["--agent", str(agent_name)])
            else:
                idx = cmd.index("--agent")
                if idx + 1 < len(cmd):
                    cmd[idx + 1] = str(agent_name)
        if _route_flag(route, "silent", True) and "--silent" not in cmd:
            cmd.append("--silent")
        if _route_flag(route, "no_ask_user", True) and "--no-ask-user" not in cmd:
            cmd.append("--no-ask-user")
        if _route_flag(route, "no_auto_update", True) and "--no-auto-update" not in cmd:
            cmd.append("--no-auto-update")
        if _route_flag(route, "allow_all", True):
            if "--allow-all" not in cmd:
                cmd.append("--allow-all")
        elif route.get("allow_all_tools"):
            if "--allow-all-tools" not in cmd:
                cmd.append("--allow-all-tools")
        if _route_flag(route, "allow_all_paths", True) and "--allow-all-paths" not in cmd:
            cmd.append("--allow-all-paths")
        if _route_flag(route, "allow_all_urls", True) and "--allow-all-urls" not in cmd:
            cmd.append("--allow-all-urls")
        available_tools = route.get("available_tools")
        if isinstance(available_tools, list) and available_tools:
            tools_arg = ",".join(str(tool) for tool in available_tools if str(tool).strip())
            if tools_arg:
                cmd.extend(["--available-tools", tools_arg])
        excluded_tools = route.get("excluded_tools")
        if isinstance(excluded_tools, list) and excluded_tools:
            tools_arg = ",".join(str(tool) for tool in excluded_tools if str(tool).strip())
            if tools_arg:
                cmd.extend(["--excluded-tools", tools_arg])
        if _route_flag(route, "no_custom_instructions", True) and "--no-custom-instructions" not in cmd:
            cmd.append("--no-custom-instructions")
        if model and "--model" not in cmd:
            cmd.extend(["--model", model])
        elif model and "--model" in cmd:
            idx = cmd.index("--model")
            if idx + 1 < len(cmd):
                cmd[idx + 1] = model
        if effort and "--effort" not in cmd:
            cmd.extend(["--effort", effort])
        prompt = f"[SYSTEM]\n{system}\n\n[USER]\n{user_prompt}"
        cmd.extend(["--prompt", prompt])
        return cmd, "", True

    cmd = list(base_parts)
    return cmd, f"[SYSTEM]\n{system}\n\n{user_prompt}", True


def _normalize_completion_output(text: str) -> str:
    stripped = (text or "").strip()
    if stripped in _COMPLETION_SENTINELS:
        return "{}"
    return text


def make_cli_llm_fn(config: dict, stage: str | None = None, *, state: dict | None = None) -> LlmFn:
    """Build an LlmFn that shells out to an agent CLI.

    If stage-specific routes are configured under ``enrichment.llm_routes``
    (or ``enrichment.routes``), they are used in order for that stage. Each
    route can specify provider, command, model, and effort. Otherwise the
    plain ``llm.agent_cmd`` fallback is used (single string or list of strings,
    tried in order — first success wins). The agent authenticates with its own
    credentials — no API keys needed in this codebase.

    The prompt is passed via stdin for Claude/Codex-like providers, or via
    Copilot's ``--prompt`` flag for Copilot routes.

    Raises EnrichmentError if none of the configured commands are found.
    """
    route_entries = _stage_route_config(config, stage)
    use_stage_routes = bool(route_entries)

    if use_stage_routes:
        resolved: list[dict] = []
        for route in route_entries:
            parts = route.get("command_parts", [])
            if parts and shutil.which(parts[0]):
                resolved.append(route)

        if not resolved:
            names = ", ".join(repr((route.get("command_parts") or ["?"])[0]) for route in route_entries)
            raise EnrichmentError(
                f"No agent CLI found on PATH (tried {names}). "
                f"Install one or adjust enrichment.llm_routes in config.yaml."
            )
    else:
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

        resolved = []
        for cmd_str in cmd_list:
            parts = _command_to_parts(cmd_str)
            if parts and shutil.which(parts[0]):
                resolved.append({"provider": _provider_from_parts(parts), "command_parts": parts})

        if not resolved:
            names = ", ".join(repr((_command_to_parts(c)[0] if _command_to_parts(c) else c)) for c in cmd_list)
            raise EnrichmentError(
                f"No agent CLI found on PATH (tried {names}). "
                f"Install one or set llm.agent_cmd in config.yaml."
            )

    enrichment_config = config.get("enrichment", {})
    max_retries: int = enrichment_config.get("max_retries", 3)
    default_timeout_seconds = _coerce_timeout_seconds(enrichment_config.get("llm_timeout_seconds"))
    state = state if isinstance(state, dict) else {}
    exhausted_providers: set[str] = state.setdefault("exhausted_providers", set())
    exhausted_lock: threading.RLock = state.setdefault("exhausted_lock", threading.RLock())

    # Pre-flight: skip Claude entirely if usage is already near the limit.
    has_claude_route = any(
        str(r.get("provider") or _provider_from_parts(r.get("command_parts", []))) == "claude"
        for r in resolved
    )
    has_fallback = len(resolved) > 1
    if has_claude_route and has_fallback and _claude_usage_over_threshold():
        with exhausted_lock:
            exhausted_providers.add("claude")

    def llm_fn(system: str, messages: list[dict]) -> str:
        system_prompt, user_prompt = _build_prompt_text(system, messages)
        debug_enabled = _llm_debug_enabled()

        with exhausted_lock:
            active_routes = [route for route in resolved if str(route.get("provider") or _provider_from_parts(route.get("command_parts", []))) not in exhausted_providers]

        if not active_routes:
            raise EnrichmentError("All configured providers are currently exhausted")

        for attempt_cfg in active_routes:
            base_parts = attempt_cfg["command_parts"]
            provider = str(attempt_cfg.get("provider") or _provider_from_parts(base_parts))

            if use_stage_routes:
                model = attempt_cfg.get("model")
                effort_value = attempt_cfg.get("effort")
                timeout_seconds = _coerce_timeout_seconds(
                    attempt_cfg.get("timeout_seconds"),
                    default_timeout_seconds,
                )
                prompt_mode = attempt_cfg.get("prompt_mode")
                if prompt_mode == "stdin":
                    cmd = _build_agent_cmd(
                        base_parts,
                        system_prompt,
                        effort=effort_value,
                        model_override=model,
                        tools=attempt_cfg.get("tools"),
                        disable_slash_commands=_route_flag(attempt_cfg, "disable_slash_commands", True),
                        bare=_route_flag(attempt_cfg, "bare", False),
                    )
                    stdin_text = f"[SYSTEM]\n{system_prompt}\n\n{user_prompt}"
                    fast_fail = attempt_cfg.get("fast_fail")
                    if fast_fail is None:
                        fast_fail = provider != "codex"
                else:
                    cmd, stdin_text, fast_fail = _build_stage_request(
                        base_parts,
                        provider,
                        system_prompt,
                        user_prompt,
                        model=model,
                        effort=effort_value,
                        route=attempt_cfg,
                    )
            else:
                timeout_seconds = default_timeout_seconds
                cmd, stdin_text, fast_fail = _build_stage_request(
                    base_parts,
                    provider,
                    system_prompt,
                    user_prompt,
                    route=attempt_cfg,
                )

            if debug_enabled:
                _llm_debug(
                    "attempt "
                    f"stage={stage or 'fallback'} provider={provider} "
                    f"cmd={shlex.join(cmd)} "
                    f"system_chars={len(system_prompt)} user_chars={len(user_prompt)} "
                    f"stdin_chars={len(stdin_text)} fast_fail={bool(fast_fail)} "
                    f"timeout_seconds={timeout_seconds} "
                    f"system_preview={_llm_debug_preview(system_prompt)} "
                    f"user_preview={_llm_debug_preview(user_prompt)}"
                )

            last_exc: Exception | None = None

            for attempt in range(max_retries):
                try:
                    result = _run_agent_subprocess(
                        cmd,
                        stdin_text,
                        timeout=timeout_seconds,
                        fast_fail=bool(fast_fail),
                    )
                    if result.returncode != 0:
                        detail = result.stderr.strip() or result.stdout.strip()
                        if debug_enabled:
                            _llm_debug(
                                f"provider={provider} exit={result.returncode} "
                                f"stdout_chars={len(result.stdout)} stderr_chars={len(result.stderr)} "
                                f"detail_preview={_llm_debug_preview(detail, 200)}"
                            )
                        if _is_exhaustion_signal(detail):
                            with exhausted_lock:
                                exhausted_providers.add(provider)
                        last_exc = EnrichmentError(
                            f"{provider} failed (exit {result.returncode}): {detail[:500]}"
                        )
                        break
                    if debug_enabled:
                        _llm_debug(
                            f"provider={provider} succeeded stdout_chars={len(result.stdout)} "
                            f"stderr_chars={len(result.stderr)} model={get_agent_model_name(cmd)} "
                            f"stdout_preview={_llm_debug_preview(result.stdout)}"
                        )
                    llm_fn.last_model = get_agent_model_name(cmd)
                    return _normalize_completion_output(result.stdout)
                except subprocess.TimeoutExpired:
                    if debug_enabled:
                        _llm_debug(
                            f"provider={provider} timed out after {timeout_seconds}s attempt={attempt + 1}/{max_retries}"
                        )
                    last_exc = EnrichmentError(
                        f"{provider} timed out after {timeout_seconds}s (attempt {attempt + 1}/{max_retries})"
                    )
                except FileNotFoundError:
                    if debug_enabled:
                        _llm_debug(f"provider CLI missing: {provider}")
                    last_exc = EnrichmentError(f"Agent CLI not found: {provider!r}")
                    break

            if last_exc and _is_exhaustion_signal(str(last_exc)):
                with exhausted_lock:
                    exhausted_providers.add(provider)

            if len(resolved) > 1:
                print(f"  [{provider}] {last_exc} — trying next agent", file=sys.stderr)
                continue

        raise last_exc  # type: ignore[arg-type]

    llm_fn.last_model = get_model_id(config, stage) if stage else get_model_id(config)  # type: ignore[attr-defined]
    llm_fn.route_mode = "stage-routes" if use_stage_routes else "fallback"  # type: ignore[attr-defined]
    llm_fn.route_stage = stage  # type: ignore[attr-defined]
    return llm_fn  # type: ignore[return-value]
