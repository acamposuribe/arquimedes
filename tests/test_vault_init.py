from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml

from arquimedes.vault import VaultExistsError, init_vault, vault_info


def test_init_vault_creates_subtree(tmp_path):
    target = tmp_path / "myvault"
    result = init_vault(target, init_git=False)

    assert result.root == target.resolve()
    assert (target / "extracted").is_dir()
    assert (target / "manifests").is_dir()
    assert (target / "derived").is_dir()
    assert (target / "wiki").is_dir()
    assert (target / "config").is_dir()
    assert (target / "config" / "maintainer").is_dir()
    assert (target / "config" / "config.yaml").is_file()
    assert (target / "config" / "maintainer" / "config.yaml").is_file()
    assert (target / "manifests" / "materials.jsonl").is_file()
    assert (target / ".gitignore").is_file()
    assert result.git_initialized is False

    shared = yaml.safe_load((target / "config" / "config.yaml").read_text(encoding="utf-8"))
    maintainer = yaml.safe_load((target / "config" / "maintainer" / "config.yaml").read_text(encoding="utf-8"))
    assert shared["library_root"] == "~/Library/Mobile Documents/com~apple~CloudDocs/Arquimedes"
    assert maintainer["llm"]["agent_cmd"] == ["claude --print", "codex exec"]
    assert maintainer["enrichment"]["llm_routes"]["document"][1]["provider"] == "codex"
    assert maintainer["mcp"]["transport"] == "streamable-http"
    assert maintainer["watch"]["scan_interval_minutes"] == 30
    assert maintainer["lint"]["graph_schedule"]["min_cluster_delta"] == 3


def test_init_vault_skip_git(tmp_path):
    target = tmp_path / "myvault"
    result = init_vault(target, init_git=False)
    assert result.git_initialized is False
    assert not (target / ".git").exists()


def test_init_vault_with_git(tmp_path):
    target = tmp_path / "myvault"
    result = init_vault(target, init_git=True)
    assert result.git_initialized is True
    assert (target / ".git").is_dir()


def test_init_vault_refuses_existing_non_empty(tmp_path):
    target = tmp_path / "myvault"
    target.mkdir()
    (target / "stray").write_text("something", encoding="utf-8")

    with pytest.raises(VaultExistsError):
        init_vault(target, init_git=False)


def test_init_vault_allows_existing_empty(tmp_path):
    target = tmp_path / "myvault"
    target.mkdir()
    result = init_vault(target, init_git=False)
    assert result.root == target.resolve()


def test_init_vault_gitignore_excludes_runtime_state(tmp_path):
    target = tmp_path / "myvault"
    init_vault(target, init_git=False)
    body = (target / ".gitignore").read_text(encoding="utf-8")
    assert "indexes/" in body
    assert "logs/" in body
    assert "*.sqlite-shm" in body
    assert "config/maintainer/config.local.yaml" in body


def test_init_vault_applies_public_hosts_and_tunnel_defaults(tmp_path):
    target = tmp_path / "personal"
    init_vault(
        target,
        init_git=False,
        library_root="~/CustomLibrary",
        serve_public_host="arquimedes.example.com",
        mcp_public_host="mcp.example.com",
        tunnel_name="arquimedes-personal",
        cloudflared_binary="/custom/cloudflared",
    )

    shared = yaml.safe_load((target / "config" / "config.yaml").read_text(encoding="utf-8"))
    maintainer = yaml.safe_load((target / "config" / "maintainer" / "config.yaml").read_text(encoding="utf-8"))

    assert shared["library_root"] == "~/CustomLibrary"
    assert maintainer["serve"]["public_exposure"] is True
    assert maintainer["serve"]["allowed_hosts"] == ["arquimedes.example.com"]
    assert maintainer["mcp"]["allowed_hosts"] == ["mcp.example.com"]
    assert maintainer["mcp"]["allowed_origins"] == ["https://mcp.example.com", "https://chatgpt.com"]
    assert maintainer["mcp"]["cloudflare_tunnel"]["enabled"] is True
    assert maintainer["mcp"]["cloudflare_tunnel"]["tunnel_name"] == "arquimedes-personal"
    assert maintainer["mcp"]["cloudflare_tunnel"]["binary_path"] == "/custom/cloudflared"


def test_vault_info_reports_resolved_paths(tmp_path, monkeypatch):
    target = tmp_path / "myvault"
    init_vault(target, init_git=False)
    monkeypatch.setenv("ARQUIMEDES_ROOT", str(target))
    monkeypatch.delenv("ARQUIMEDES_LOCAL_CACHE", raising=False)
    monkeypatch.delenv("ARQUIMEDES_CONFIG", raising=False)

    info = vault_info()
    assert info.vault_root == target
    assert info.local_cache_root == target
    assert info.has_index is False
    assert info.library_root is not None
    assert any(str(target) in str(p) for p in info.config_sources)


def test_clone_vault_via_local_repo(tmp_path):
    """clone_vault should clone a real local git URL into the target path."""
    import subprocess

    from arquimedes.vault import clone_vault

    source = tmp_path / "source-vault"
    source.mkdir()
    subprocess.run(["git", "init", "-q", "-b", "main"], cwd=source, check=True)
    (source / "README").write_text("hello vault\n", encoding="utf-8")
    subprocess.run(["git", "add", "-A"], cwd=source, check=True)
    env = {"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t", "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t", "PATH": os.environ["PATH"]}
    subprocess.run(["git", "commit", "-q", "-m", "init"], cwd=source, check=True, env=env)

    target = tmp_path / "cloned"
    result = clone_vault(str(source), target)
    assert result.root == target.resolve()
    assert result.git_initialized is True
    assert (target / ".git").is_dir()
    assert (target / "README").read_text() == "hello vault\n"


def test_clone_vault_refuses_existing_non_empty(tmp_path):
    from arquimedes.vault import VaultExistsError, clone_vault

    target = tmp_path / "occupied"
    target.mkdir()
    (target / "stray").write_text("x", encoding="utf-8")
    with pytest.raises(VaultExistsError):
        clone_vault("file:///does/not/matter", target)


def test_clone_vault_propagates_git_error(tmp_path):
    from arquimedes.vault import clone_vault

    target = tmp_path / "fail"
    with pytest.raises(RuntimeError):
        clone_vault("file:///nonexistent/repo.git", target)


def test_vault_info_to_dict(tmp_path, monkeypatch):
    target = tmp_path / "myvault"
    init_vault(target, init_git=False)
    monkeypatch.setenv("ARQUIMEDES_ROOT", str(target))
    monkeypatch.delenv("ARQUIMEDES_LOCAL_CACHE", raising=False)
    monkeypatch.delenv("ARQUIMEDES_CONFIG", raising=False)

    payload = vault_info().to_dict()
    assert payload["vault_root"] == str(target)
    assert payload["has_index"] is False
    assert "config_sources" in payload
