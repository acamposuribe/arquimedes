"""Shared helpers for agent-facing CLI commands.

Phase 7 introduces a small, disciplined read-only command surface for
collaborator agents. Every agent-facing command follows the same contract:

- stays fresh transparently (calls `ensure_index_and_memory()` unless
  `ARQ_SKIP_FRESHNESS` is set), so agents never see stale results
- emits JSON by default, human-readable text with `--human`
- converts `FileNotFoundError` into a `click.ClickException` with a helpful
  message; never swallows other exceptions

The `ensure_guard` decorator below concentrates that contract in one place so
new commands do not duplicate the same four lines of plumbing.
"""

from __future__ import annotations

import json
import os
from functools import wraps
from typing import Any, Callable

import click


_SKIP_FRESHNESS_ENV = "ARQ_SKIP_FRESHNESS"


def _truthy(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


def ensure_guard(func: Callable) -> Callable:
    """Wrap an agent-facing command so it runs after a freshness check.

    Skips the check when ``ARQ_SKIP_FRESHNESS`` is truthy in the environment,
    so test fixtures and bulk scripted runs can opt out. Converts
    ``FileNotFoundError`` into a ``click.ClickException``; any other exception
    propagates unchanged.
    """

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        if not _truthy(os.environ.get(_SKIP_FRESHNESS_ENV)):
            from arquimedes.index import ensure_index_and_memory

            try:
                ensure_index_and_memory()
            except FileNotFoundError as exc:
                raise click.ClickException(str(exc))
        return func(*args, **kwargs)

    return wrapper


def emit(
    payload: Any,
    *,
    human: bool,
    human_formatter: Callable[[Any], str] | None = None,
) -> None:
    """Emit a result either as indented JSON (default) or human text.

    ``payload`` may be a dataclass-like object with ``to_dict()``, a plain
    dict/list, or a string. ``human_formatter`` is required when ``human``
    is true and ``payload`` is not already a string.
    """
    if human:
        if isinstance(payload, str):
            click.echo(payload)
            return
        if human_formatter is None:
            raise RuntimeError("emit(human=True) requires a human_formatter for non-string payloads")
        click.echo(human_formatter(payload))
        return

    if hasattr(payload, "to_dict") and callable(payload.to_dict):
        data = payload.to_dict()
    else:
        data = payload
    click.echo(json.dumps(data, ensure_ascii=False, indent=2))


def not_found(what: str, *, hint: str | None = None) -> click.ClickException:
    """Build a single-line ClickException for a missing artifact.

    The message follows the Phase 7 convention: state what was not found and,
    when useful, suggest a next command the agent can try.
    """
    message = f"Error: {what} not found."
    if hint:
        message = f"{message} Try: {hint}"
    return click.ClickException(message)
