from __future__ import annotations

from pathlib import Path

from arquimedes.config import load_config


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_default_config_uses_maintainer_profile_until_collaborator_local_exists(tmp_path, monkeypatch):
    root = tmp_path
    monkeypatch.setenv("ARQUIMEDES_ROOT", str(root))
    monkeypatch.delenv("ARQUIMEDES_CONFIG", raising=False)

    _write(root / "config" / "config.yaml", 'library_root: "~/Shared"\nsync:\n  pull_interval: 300\n')
    _write(root / "config" / "maintainer" / "config.yaml", 'library_root: "~/Maintainer"\nextraction:\n  chunk_size: 500\nllm:\n  agent_cmd: "codex exec"\n')

    maintainer_config = load_config()
    assert maintainer_config["library_root"].endswith("Maintainer")
    assert maintainer_config["sync"]["pull_interval"] == 300
    assert maintainer_config["extraction"]["chunk_size"] == 500
    assert maintainer_config["llm"]["agent_cmd"] == "codex exec"

    _write(root / "config" / "collaborator" / "config.local.yaml", 'library_root: "~/Collaborator"\n')
    collaborator_config = load_config()
    assert collaborator_config["library_root"].endswith("Collaborator")
    assert collaborator_config["sync"]["pull_interval"] == 300
    assert "extraction" not in collaborator_config
    assert "enrichment" not in collaborator_config
    assert "llm" not in collaborator_config


def test_explicit_config_overlays_shared_base_even_with_collaborator_local(tmp_path, monkeypatch):
    root = tmp_path
    monkeypatch.setenv("ARQUIMEDES_ROOT", str(root))
    monkeypatch.delenv("ARQUIMEDES_CONFIG", raising=False)

    _write(root / "config" / "config.yaml", 'library_root: "~/Shared"\nsync:\n  pull_interval: 300\n')
    _write(root / "config" / "collaborator" / "config.local.yaml", 'library_root: "~/Collaborator"\n')
    _write(root / "config" / "maintainer" / "config.yaml", 'enrichment:\n  max_retries: 3\nllm:\n  agent_cmd: "claude --print"\n')

    config = load_config(root / "config" / "maintainer" / "config.yaml")
    assert config["library_root"].endswith("Shared")
    assert config["sync"]["pull_interval"] == 300
    assert config["enrichment"]["max_retries"] == 3
    assert config["llm"]["agent_cmd"] == "claude --print"


def test_stale_env_config_falls_back_to_current_vault_defaults(tmp_path, monkeypatch):
    root = tmp_path
    stale = tmp_path / "missing-vault" / "config" / "maintainer" / "config.yaml"
    monkeypatch.delenv("ARQUIMEDES_ROOT", raising=False)
    monkeypatch.setenv("ARQUIMEDES_CONFIG", str(stale))
    monkeypatch.chdir(root)

    _write(root / "config" / "config.yaml", 'library_root: "~/Shared"\nsync:\n  pull_interval: 300\n')
    _write(root / "config" / "maintainer" / "config.yaml", 'library_root: "~/Maintainer"\nllm:\n  agent_cmd: "codex exec"\n')

    config = load_config()
    assert config["library_root"].endswith("Maintainer")
    assert config["sync"]["pull_interval"] == 300
    assert config["llm"]["agent_cmd"] == "codex exec"
