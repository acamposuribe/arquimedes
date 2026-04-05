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
from typing import Callable

# ---------------------------------------------------------------------------
# Type alias
# ---------------------------------------------------------------------------

LlmFn = Callable[[str, list[dict]], str]
"""(system_prompt, messages) -> response_text"""


def set_effort(llm_fn: LlmFn, config: dict, stage: str) -> None:
    """Set the thinking effort on llm_fn from config for the given stage."""
    effort = config.get("enrichment", {}).get("effort", {})
    if isinstance(effort, dict):
        level = effort.get(stage)
    else:
        level = None
    if hasattr(llm_fn, "effort"):
        llm_fn.effort = level  # type: ignore[attr-defined]


def set_model(llm_fn: LlmFn, config: dict, stage: str) -> None:
    """Set the per-stage model override on llm_fn from config."""
    model_cfg = config.get("enrichment", {}).get("model", {})
    if isinstance(model_cfg, dict):
        model = model_cfg.get(stage)
    else:
        model = None
    if hasattr(llm_fn, "model_override"):
        llm_fn.model_override = model  # type: ignore[attr-defined]


def set_codex_params(llm_fn: LlmFn, config: dict, stage: str) -> None:
    """Set codex-specific model and effort on llm_fn from config."""
    enrichment = config.get("enrichment", {})
    codex_model = enrichment.get("codex_model")
    codex_effort_cfg = enrichment.get("codex_effort", {})
    codex_effort = codex_effort_cfg.get(stage) if isinstance(codex_effort_cfg, dict) else None
    if hasattr(llm_fn, "codex_model"):
        llm_fn.codex_model = codex_model  # type: ignore[attr-defined]
    if hasattr(llm_fn, "codex_effort"):
        llm_fn.codex_effort = codex_effort  # type: ignore[attr-defined]


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


def get_model_id(config: dict, stage: str | None = None) -> str:
    """Derive a stable model identifier from the config.

    If stage-specific routes are configured, return a stable signature for the
    ordered route list for that stage. Otherwise fall back to the legacy
    ``llm.agent_cmd`` configuration (e.g. ``"claude --print|codex exec"``).

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
            "prompt_mode": entry.get("prompt_mode"),
            "silent": entry.get("silent"),
            "allow_all": entry.get("allow_all"),
            "no_custom_instructions": entry.get("no_custom_instructions"),
            "fast_fail": entry.get("fast_fail"),
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
    fast_fail: bool = True,
) -> subprocess.CompletedProcess[str]:
    """Run an agent CLI with optional fast-fail stderr monitoring.

    When *fast_fail* is True, monitors stderr in a background thread and
    kills the process immediately if an auth/rate-limit pattern is detected.

    Set *fast_fail* to False for agents that echo the full prompt to stderr
    (e.g. codex), which would otherwise cause false-positive kills on
    document content containing words like 'unauthorized' or 'exceeded'.
    """
    # Enrichment prompts fit well within 200K — disable 1M context to avoid
    # the drastically higher token consumption of extended context windows.
    env = os.environ.copy()
    env["CLAUDE_CODE_DISABLE_1M_CONTEXT"] = "1"

    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
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


def _build_agent_cmd(base_parts: list[str], system: str, *, effort: str | None = None, model_override: str | None = None, codex_model: str | None = None, codex_effort: str | None = None) -> list[str]:
    """Build the full command for an agent CLI, adding speed optimizations.

    For ``claude``: adds flags to minimize startup overhead without
    breaking credential discovery (``--bare`` is avoided for that reason):
    - ``--no-session-persistence``: skip saving session to disk
    - ``--system-prompt``: pass system prompt natively
    - ``--tools ""``: disable built-in tools (we only need text output)
    - ``--disable-slash-commands``: skip skill resolution
    - ``--effort``: control thinking budget (low/medium/high)
    - ``--model``: per-stage model override (e.g. haiku for chunks)

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
        # Model: per-stage override takes precedence, then default to sonnet
        if "--model" not in cmd:
            cmd.extend(["--model", model_override or "sonnet"])
        elif model_override and "--model" in cmd:
            # Replace existing default model with stage-specific override
            idx = cmd.index("--model")
            cmd[idx + 1] = model_override
        # Control thinking budget — lower = cheaper + faster
        if "--effort" not in cmd and effort:
            cmd.extend(["--effort", effort])
        cmd.extend(["--system-prompt", system])
        return cmd
    if exe == "codex":
        cmd = list(base_parts)
        if "--ephemeral" not in cmd:
            cmd.append("--ephemeral")
        if codex_model and "-m" not in cmd:
            cmd.extend(["-m", codex_model])
        if codex_effort:
            cmd.extend(["-c", f"model_reasoning_effort={codex_effort}"])
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
) -> tuple[list[str], str, bool]:
    """Return (cmd, stdin_text, fast_fail) for a provider-specific attempt."""
    provider = provider.lower()
    if provider == "claude":
        cmd = _build_agent_cmd(base_parts, system, effort=effort, model_override=model)
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
        if "--silent" not in cmd:
            cmd.append("--silent")
        if "--allow-all" not in cmd and "--allow-all-tools" not in cmd:
            cmd.append("--allow-all")
        if "--no-custom-instructions" not in cmd:
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


def make_cli_llm_fn(config: dict, stage: str | None = None) -> LlmFn:
    """Build an LlmFn that shells out to an agent CLI.

    If stage-specific routes are configured under ``enrichment.llm_routes``
    (or ``enrichment.routes``), they are used in order for that stage. Each
    route can specify provider, command, model, and effort. Otherwise the
    legacy ``llm.agent_cmd`` setting is used (single string or list of strings,
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

    max_retries: int = config.get("enrichment", {}).get("max_retries", 3)

    def llm_fn(system: str, messages: list[dict]) -> str:
        system_prompt, user_prompt = _build_prompt_text(system, messages)
        effort = getattr(llm_fn, "effort", None)
        model_override = getattr(llm_fn, "model_override", None)
        codex_model = getattr(llm_fn, "codex_model", None)
        codex_effort = getattr(llm_fn, "codex_effort", None)

        for attempt_cfg in resolved:
            base_parts = attempt_cfg["command_parts"]
            provider = str(attempt_cfg.get("provider") or _provider_from_parts(base_parts))

            if use_stage_routes:
                model = attempt_cfg.get("model")
                effort_value = attempt_cfg.get("effort")
                prompt_mode = attempt_cfg.get("prompt_mode")
                if prompt_mode == "stdin":
                    cmd = _build_agent_cmd(
                        base_parts,
                        system_prompt,
                        effort=effort_value,
                        model_override=model,
                        codex_model=model,
                        codex_effort=effort_value,
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
                    )
            else:
                effective_model = model_override if provider != "codex" else codex_model
                effective_effort = effort if provider != "codex" else codex_effort
                cmd, stdin_text, fast_fail = _build_stage_request(
                    base_parts,
                    provider,
                    system_prompt,
                    user_prompt,
                    model=effective_model,
                    effort=effective_effort,
                )

            last_exc: Exception | None = None

            for attempt in range(max_retries):
                try:
                    result = _run_agent_subprocess(
                        cmd,
                        stdin_text,
                        timeout=300,
                        fast_fail=bool(fast_fail),
                    )
                    if result.returncode != 0:
                        detail = result.stderr.strip() or result.stdout.strip()
                        last_exc = EnrichmentError(
                            f"{provider} failed (exit {result.returncode}): {detail[:500]}"
                        )
                        break
                    llm_fn.last_model = get_agent_model_name(cmd)
                    return result.stdout
                except subprocess.TimeoutExpired:
                    last_exc = EnrichmentError(
                        f"{provider} timed out after 300s (attempt {attempt + 1}/{max_retries})"
                    )
                except FileNotFoundError:
                    last_exc = EnrichmentError(f"Agent CLI not found: {provider!r}")
                    break

            if len(resolved) > 1:
                import sys
                print(f"  [{provider}] {last_exc} — trying next agent", file=sys.stderr)
                continue

        raise last_exc  # type: ignore[arg-type]

    llm_fn.last_model = get_model_id(config, stage) if stage else get_model_id(config)  # type: ignore[attr-defined]
    llm_fn.effort = None  # type: ignore[attr-defined]
    llm_fn.model_override = None  # type: ignore[attr-defined]
    llm_fn.codex_model = None  # type: ignore[attr-defined]
    llm_fn.codex_effort = None  # type: ignore[attr-defined]
    llm_fn.route_mode = "stage-routes" if use_stage_routes else "legacy"  # type: ignore[attr-defined]
    llm_fn.route_stage = stage  # type: ignore[attr-defined]
    return llm_fn  # type: ignore[return-value]
