"""Sanity checks for docs/agent-handbook.md.

Verifies the handbook exists, stays under its token budget, and that every
`arq <command>` it names is actually registered in the CLI.
"""

from __future__ import annotations

import re
from pathlib import Path

from arquimedes.cli import cli


HANDBOOK = Path(__file__).resolve().parents[1] / "docs" / "agent-handbook.md"
# Token budget: ~800 tokens. Using 1.4 tokens/word as a safe upper bound,
# this caps the handbook body at ~570 words.
WORD_BUDGET = 600


def _cli_command_names() -> set[str]:
    names: set[str] = set()
    for name, command in cli.commands.items():
        names.add(name)
        # Walk group subcommands (e.g. `arq index ensure`).
        subcommands = getattr(command, "commands", None)
        if subcommands:
            for sub_name in subcommands:
                names.add(f"{name} {sub_name}")
    return names


def test_handbook_exists():
    assert HANDBOOK.exists(), f"missing handbook at {HANDBOOK}"


def test_handbook_under_token_budget():
    text = HANDBOOK.read_text(encoding="utf-8")
    word_count = len(text.split())
    assert word_count <= WORD_BUDGET, f"handbook {word_count} words > {WORD_BUDGET} budget"


def test_handbook_commands_exist_in_cli():
    text = HANDBOOK.read_text(encoding="utf-8")
    referenced = set()
    for match in re.finditer(r"`arq ([a-z][a-z-]*(?: [a-z][a-z-]*)?)\b", text):
        referenced.add(match.group(1))
    known = _cli_command_names()
    missing = sorted(referenced - known)
    assert not missing, f"handbook references unknown commands: {missing}"
