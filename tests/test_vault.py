from __future__ import annotations

from pathlib import Path

from arquimedes.config import (
    get_derived_root,
    get_extracted_root,
    get_local_cache_root,
    get_logs_root,
    get_indexes_root,
    get_manifests_root,
    get_project_root,
    get_vault_root,
    get_wiki_root,
)


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _seed_vault(tmp_path: Path) -> Path:
    _write(tmp_path / "config" / "config.yaml", 'library_root: "~/Shared"\n')
    return tmp_path


def test_get_vault_root_matches_get_project_root(tmp_path, monkeypatch):
    root = _seed_vault(tmp_path)
    monkeypatch.setenv("ARQUIMEDES_ROOT", str(root))
    monkeypatch.delenv("ARQUIMEDES_LOCAL_CACHE", raising=False)

    assert get_vault_root() == root
    assert get_vault_root() == get_project_root()


def test_local_cache_defaults_to_vault_root(tmp_path, monkeypatch):
    root = _seed_vault(tmp_path)
    monkeypatch.setenv("ARQUIMEDES_ROOT", str(root))
    monkeypatch.delenv("ARQUIMEDES_LOCAL_CACHE", raising=False)

    assert get_local_cache_root() == get_vault_root()
    assert get_local_cache_root({}) == get_vault_root()


def test_local_cache_env_var_override(tmp_path, monkeypatch):
    root = _seed_vault(tmp_path)
    cache = tmp_path / "cache"
    cache.mkdir()
    monkeypatch.setenv("ARQUIMEDES_ROOT", str(root))
    monkeypatch.setenv("ARQUIMEDES_LOCAL_CACHE", str(cache))

    assert get_local_cache_root() == cache
    assert get_indexes_root() == cache / "indexes"
    assert get_logs_root() == cache / "logs"


def test_local_cache_config_override(tmp_path, monkeypatch):
    root = _seed_vault(tmp_path)
    cache = tmp_path / "alt-cache"
    monkeypatch.setenv("ARQUIMEDES_ROOT", str(root))
    monkeypatch.delenv("ARQUIMEDES_LOCAL_CACHE", raising=False)

    config = {"local_cache_root": str(cache)}
    assert get_local_cache_root(config) == cache
    assert get_indexes_root(config) == cache / "indexes"
    assert get_logs_root(config) == cache / "logs"


def test_env_var_takes_precedence_over_config(tmp_path, monkeypatch):
    root = _seed_vault(tmp_path)
    env_cache = tmp_path / "env-cache"
    config_cache = tmp_path / "config-cache"
    env_cache.mkdir()
    monkeypatch.setenv("ARQUIMEDES_ROOT", str(root))
    monkeypatch.setenv("ARQUIMEDES_LOCAL_CACHE", str(env_cache))

    config = {"local_cache_root": str(config_cache)}
    assert get_local_cache_root(config) == env_cache


def test_vault_subpaths(tmp_path, monkeypatch):
    root = _seed_vault(tmp_path)
    monkeypatch.setenv("ARQUIMEDES_ROOT", str(root))
    monkeypatch.delenv("ARQUIMEDES_LOCAL_CACHE", raising=False)

    assert get_extracted_root() == root / "extracted"
    assert get_wiki_root() == root / "wiki"
    assert get_derived_root() == root / "derived"
    assert get_manifests_root() == root / "manifests"


def test_get_index_path_uses_local_cache(tmp_path, monkeypatch):
    """`get_index_path()` must resolve via local cache root, not vault root."""
    from arquimedes.index import get_index_path

    root = _seed_vault(tmp_path)
    cache = tmp_path / "cache"
    cache.mkdir()
    monkeypatch.setenv("ARQUIMEDES_ROOT", str(root))
    monkeypatch.setenv("ARQUIMEDES_LOCAL_CACHE", str(cache))

    assert get_index_path() == cache / "indexes" / "search.sqlite"
    assert get_index_path().parent.parent != root


def test_get_index_path_default_back_compat(tmp_path, monkeypatch):
    """With no local cache override, index path falls back to vault root."""
    from arquimedes.index import get_index_path

    root = _seed_vault(tmp_path)
    monkeypatch.setenv("ARQUIMEDES_ROOT", str(root))
    monkeypatch.delenv("ARQUIMEDES_LOCAL_CACHE", raising=False)

    assert get_index_path() == root / "indexes" / "search.sqlite"


def test_local_cache_expands_user_home(tmp_path, monkeypatch):
    root = _seed_vault(tmp_path)
    monkeypatch.setenv("ARQUIMEDES_ROOT", str(root))
    monkeypatch.delenv("ARQUIMEDES_LOCAL_CACHE", raising=False)

    config = {"local_cache_root": "~/some-cache-dir"}
    resolved = get_local_cache_root(config)
    assert "~" not in str(resolved)
    assert str(resolved).startswith(str(Path.home()))
