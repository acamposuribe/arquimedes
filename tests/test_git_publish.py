from __future__ import annotations

from types import SimpleNamespace

import arquimedes.git_publish as git_publish


def _proc(returncode=0, stdout="", stderr=""):
    return SimpleNamespace(returncode=returncode, stdout=stdout, stderr=stderr)


def test_git_env_builds_deploy_key_ssh_command():
    env = git_publish.git_env(
        {
            "git": {
                "ssh_key_path": "~/.ssh/arq_vault_personal",
            }
        },
        base_env={},
    )

    assert env["GIT_TERMINAL_PROMPT"] == "0"
    assert "GIT_SSH_COMMAND" in env
    assert "-o IdentitiesOnly=yes" in env["GIT_SSH_COMMAND"]
    assert "arq_vault_personal" in env["GIT_SSH_COMMAND"]


def test_ensure_push_remote_converts_github_https_to_ssh(tmp_path):
    calls: list[tuple[str, ...]] = []

    def runner(args, cwd, env):
        calls.append(tuple(args))
        if tuple(args) == ("git", "remote", "get-url", "--push", "origin"):
            return _proc(stdout="https://github.com/example/arq-vault-personal.git\n")
        if tuple(args) == (
            "git",
            "remote",
            "set-url",
            "--push",
            "origin",
            "git@github.com:example/arq-vault-personal.git",
        ):
            return _proc()
        raise AssertionError(f"unexpected command: {args}")

    result = git_publish.ensure_push_remote(tmp_path, runner=runner, env={})

    assert result["changed"] is True
    assert result["push_url"] == "git@github.com:example/arq-vault-personal.git"
    assert result["strategy"] == "github-ssh"
    assert calls[-1] == (
        "git",
        "remote",
        "set-url",
        "--push",
        "origin",
        "git@github.com:example/arq-vault-personal.git",
    )


def test_push_respects_explicit_push_remote_url(tmp_path):
    calls: list[tuple[str, ...]] = []

    def runner(args, cwd, env):
        calls.append(tuple(args))
        if tuple(args) == ("git", "remote", "get-url", "--push", "origin"):
            return _proc(stdout="https://github.com/example/arq-vault-personal.git\n")
        if tuple(args) == (
            "git",
            "remote",
            "set-url",
            "--push",
            "origin",
            "git@github.com:custom/arq-vault-personal.git",
        ):
            return _proc()
        if tuple(args) == ("git", "push"):
            return _proc(stdout="done\n")
        raise AssertionError(f"unexpected command: {args}")

    result = git_publish.push(
        tmp_path,
        config={"git": {"push_remote_url": "git@github.com:custom/arq-vault-personal.git"}},
        runner=runner,
        env={},
    )

    assert result.returncode == 0
    assert calls[-1] == ("git", "push")
