from __future__ import annotations

import json
import subprocess

from click.testing import CliRunner

import arquimedes.cli as cli_mod
from arquimedes.cli import cli


def test_default_upgrade_install_spec_uses_current_git_source(monkeypatch):
    class FakeDistribution:
        @staticmethod
        def read_text(name: str) -> str | None:
            assert name == "direct_url.json"
            return json.dumps(
                {
                    "url": "https://github.com/example/arquimedes.git",
                    "vcs_info": {
                        "vcs": "git",
                        "requested_revision": "main",
                    },
                }
            )

    monkeypatch.setattr(cli_mod.metadata, "distribution", lambda name: FakeDistribution())

    assert cli_mod._default_upgrade_install_spec() == "git+https://github.com/example/arquimedes.git@main"


def test_default_upgrade_install_spec_falls_back_to_pypi(monkeypatch):
    monkeypatch.setattr(
        cli_mod.metadata,
        "distribution",
        lambda name: (_ for _ in ()).throw(cli_mod.metadata.PackageNotFoundError()),
    )

    assert cli_mod._default_upgrade_install_spec() == "arquimedes"


def test_default_upgrade_install_spec_converts_local_repo_to_remote_branch(monkeypatch):
    class FakeDistribution:
        @staticmethod
        def read_text(name: str) -> str | None:
            assert name == "direct_url.json"
            return json.dumps(
                {
                    "url": "file:///Users/test/Sites/arquimedes-code",
                    "dir_info": {},
                }
            )

    monkeypatch.setattr(cli_mod.metadata, "distribution", lambda name: FakeDistribution())
    monkeypatch.setattr(
        cli_mod,
        "_upgrade_spec_from_local_repo",
        lambda repo_path: "git+https://github.com/example/arquimedes.git@main",
    )

    assert cli_mod._default_upgrade_install_spec() == "git+https://github.com/example/arquimedes.git@main"


def test_upgrade_force_reinstalls_package_and_launch_agents(monkeypatch):
    calls: list[list[str]] = []

    def _fake_which(name: str) -> str | None:
        if name == "pipx":
            return "/usr/local/bin/pipx"
        if name == "arq":
            return "/Users/test/.local/bin/arq"
        return None

    def _fake_run(args, capture_output=True, text=True, check=False):
        calls.append(list(args))
        if args[:3] == ["/usr/local/bin/pipx", "install", "--force"]:
            return subprocess.CompletedProcess(args, 0, stdout="reinstalled", stderr="")
        if args[-2:] == ["watch", "--install"]:
            return subprocess.CompletedProcess(args, 0, stdout=json.dumps({"label": "com.arquimedes.watch"}), stderr="")
        if args[-2:] == ["serve", "--install"]:
            return subprocess.CompletedProcess(args, 0, stdout=json.dumps({"label": "com.arquimedes.serve"}), stderr="")
        if args[-2:] == ["mcp", "--install"]:
            return subprocess.CompletedProcess(args, 0, stdout=json.dumps({"mcp": {"label": "com.arquimedes.mcp"}}), stderr="")
        if args[-2:] == ["lint", "--install-full"]:
            return subprocess.CompletedProcess(args, 0, stdout=json.dumps({"label": "com.arquimedes.lint-full"}), stderr="")
        raise AssertionError(f"unexpected command: {args}")

    monkeypatch.setattr(cli_mod.shutil, "which", _fake_which)
    monkeypatch.setattr(cli_mod.subprocess, "run", _fake_run)

    result = CliRunner().invoke(
        cli,
        [
            "--config",
            "/vault/config/maintainer/config.yaml",
            "upgrade",
            "--spec",
            "git+https://github.com/example/arquimedes.git@main",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)

    assert payload["install_spec"] == "git+https://github.com/example/arquimedes.git@main"
    assert payload["arq_binary"] == "/Users/test/.local/bin/arq"
    assert payload["launch_agents"]["watch"]["label"] == "com.arquimedes.watch"
    assert payload["launch_agents"]["lint_full"]["label"] == "com.arquimedes.lint-full"
    assert payload["launch_agents"]["serve"]["label"] == "com.arquimedes.serve"
    assert payload["launch_agents"]["mcp"]["mcp"]["label"] == "com.arquimedes.mcp"

    assert calls == [
        [
            "/usr/local/bin/pipx",
            "install",
            "--force",
            "git+https://github.com/example/arquimedes.git@main",
        ],
        [
            "/Users/test/.local/bin/arq",
            "--config",
            "/vault/config/maintainer/config.yaml",
            "watch",
            "--install",
        ],
        [
            "/Users/test/.local/bin/arq",
            "--config",
            "/vault/config/maintainer/config.yaml",
            "lint",
            "--install-full",
        ],
        [
            "/Users/test/.local/bin/arq",
            "--config",
            "/vault/config/maintainer/config.yaml",
            "serve",
            "--install",
        ],
        [
            "/Users/test/.local/bin/arq",
            "--config",
            "/vault/config/maintainer/config.yaml",
            "mcp",
            "--install",
        ],
    ]
