"""Helpers for non-interactive git publication on maintainer machines."""

from __future__ import annotations

import os
from pathlib import Path
import re
import subprocess
from typing import Callable


Runner = Callable[[list[str], Path, dict[str, str]], subprocess.CompletedProcess[str]]

_GITHUB_HTTPS_RE = re.compile(r"^https://github\.com/(?P<owner>[^/]+)/(?P<repo>[^/]+?)(?:\.git)?/?$")


def default_runner(
    args: list[str],
    cwd: Path,
    env: dict[str, str],
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(args, cwd=cwd, env=env, text=True, capture_output=True, check=False)


def git_env(
    config: dict | None = None,
    *,
    base_env: dict[str, str] | None = None,
) -> dict[str, str]:
    env = dict(base_env or os.environ.copy())
    env.setdefault("GIT_TERMINAL_PROMPT", "0")

    git_cfg = (config or {}).get("git", {}) if isinstance(config, dict) else {}
    if isinstance(git_cfg, dict):
        ssh_command = str(git_cfg.get("ssh_command") or "").strip()
        if ssh_command:
            env["GIT_SSH_COMMAND"] = ssh_command
            return env

        ssh_key_path = str(git_cfg.get("ssh_key_path") or "").strip()
        if ssh_key_path:
            key_path = str(Path(ssh_key_path).expanduser())
            env["GIT_SSH_COMMAND"] = " ".join(
                [
                    "ssh",
                    f"-i {key_path}",
                    "-o IdentitiesOnly=yes",
                    "-o BatchMode=yes",
                ]
            )
    return env


def configured_push_remote_url(config: dict | None = None) -> str | None:
    git_cfg = (config or {}).get("git", {}) if isinstance(config, dict) else {}
    if not isinstance(git_cfg, dict):
        return None
    value = str(git_cfg.get("push_remote_url") or "").strip()
    return value or None


def github_https_to_ssh(url: str) -> str | None:
    match = _GITHUB_HTTPS_RE.match(url.strip())
    if not match:
        return None
    return f"git@github.com:{match.group('owner')}/{match.group('repo')}.git"


def _git(
    runner: Runner,
    project_root: Path,
    env: dict[str, str],
    *args: str,
) -> subprocess.CompletedProcess[str]:
    return runner(["git", *args], project_root, env)


def ensure_push_remote(
    project_root: Path,
    *,
    config: dict | None = None,
    runner: Runner = default_runner,
    env: dict[str, str] | None = None,
    remote_name: str = "origin",
) -> dict[str, object]:
    resolved_env = git_env(config, base_env=env)
    current = _git(runner, project_root, resolved_env, "remote", "get-url", "--push", remote_name)
    if current.returncode != 0:
        return {
            "remote_name": remote_name,
            "push_url": None,
            "changed": False,
            "strategy": "unresolved",
            "message": (current.stderr or current.stdout or "git remote get-url failed").strip(),
        }

    current_url = current.stdout.strip()
    configured_url = configured_push_remote_url(config)
    desired_url = configured_url or github_https_to_ssh(current_url)
    strategy = "configured" if configured_url else ("github-ssh" if desired_url else "current")

    if not desired_url or desired_url == current_url:
        return {
            "remote_name": remote_name,
            "push_url": current_url,
            "changed": False,
            "strategy": strategy,
        }

    updated = _git(
        runner,
        project_root,
        resolved_env,
        "remote",
        "set-url",
        "--push",
        remote_name,
        desired_url,
    )
    if updated.returncode != 0:
        raise RuntimeError((updated.stderr or updated.stdout or "git remote set-url --push failed").strip())

    return {
        "remote_name": remote_name,
        "push_url": desired_url,
        "changed": True,
        "strategy": strategy,
    }


def push(
    project_root: Path,
    *,
    config: dict | None = None,
    runner: Runner = default_runner,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    resolved_env = git_env(config, base_env=env)
    ensure_push_remote(project_root, config=config, runner=runner, env=resolved_env)
    return _git(runner, project_root, resolved_env, "push")
