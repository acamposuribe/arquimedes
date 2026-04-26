"""Configuration loading with local overrides."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

# Environment variable to explicitly set the project root
_ENV_VAR = "ARQUIMEDES_ROOT"
_CONFIG_ENV_VAR = "ARQUIMEDES_CONFIG"
_LOCAL_CACHE_ENV_VAR = "ARQUIMEDES_LOCAL_CACHE"


def _find_project_root() -> Path:
    """Find the project root directory.

    Resolution order:
    1. ARQUIMEDES_ROOT environment variable (for launchd, agents, remote invocation)
    2. Walk up from CWD looking for config/config.yaml
    3. Walk up from this file's installed location (works when `arq` is on PATH)
    """
    # 1. Explicit env var
    env_root = os.environ.get(_ENV_VAR)
    if env_root:
        root = Path(env_root)
        if (root / "config" / "config.yaml").exists():
            return root
        raise FileNotFoundError(
            f"{_ENV_VAR}={env_root} does not contain config/config.yaml"
        )

    env_config = os.environ.get(_CONFIG_ENV_VAR)
    if env_config:
        config_path = Path(env_config).expanduser()
        if config_path.exists():
            for ancestor in config_path.parents:
                if (ancestor / "config" / "config.yaml").exists():
                    return ancestor
            if config_path.parent.name == "config":
                return config_path.parent.parent
            return config_path.parent
        raise FileNotFoundError(f"{_CONFIG_ENV_VAR}={env_config} does not exist")

    # 2. Walk up from CWD
    current = Path.cwd()
    for parent in [current, *current.parents]:
        if (parent / "config" / "config.yaml").exists():
            return parent

    # 3. Walk up from this file's location (installed package)
    pkg_dir = Path(__file__).resolve().parent
    for parent in [pkg_dir, *pkg_dir.parents]:
        if (parent / "config" / "config.yaml").exists():
            return parent

    raise FileNotFoundError(
        "No vault found. Looked for config/config.yaml by walking up from the "
        "current directory and from the installed arquimedes package, and found "
        "none. To proceed, do one of: (a) `cd` into a vault folder; (b) pass "
        "`--config <path-to-vault-config>`; (c) export "
        f"{_ENV_VAR}=<vault-root> or {_CONFIG_ENV_VAR}=<vault-config>; (d) "
        "create a new vault with `arq init <path>`."
    )


def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge override into base, returning a new dict."""
    result = dict(base)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _resolve_config_path(config_path: str | Path) -> Path:
    selected = Path(config_path).expanduser()
    if not selected.is_absolute():
        selected = (_find_project_root() / selected).resolve()
    return selected


def _config_stack(config_path: str | Path | None = None) -> list[Path]:
    if config_path is None:
        env_config = os.environ.get(_CONFIG_ENV_VAR)
        if env_config:
            config_path = env_config

    root = _find_project_root()
    shared = root / "config" / "config.yaml"
    if config_path is not None:
        selected = _resolve_config_path(config_path)
        stack = [shared]
        if selected != shared:
            stack.append(selected)
        if selected.name != "config.local.yaml":
            local = selected.parent / "config.local.yaml"
            if local.exists():
                stack.append(local)
        return stack

    stack = [shared]
    collaborator_local = root / "config" / "collaborator" / "config.local.yaml"
    maintainer_profile = root / "config" / "maintainer" / "config.yaml"
    maintainer_local = root / "config" / "maintainer" / "config.local.yaml"
    legacy_local = root / "config" / "config.local.yaml"

    if collaborator_local.exists():
        stack.append(collaborator_local)
    elif maintainer_profile.exists():
        stack.append(maintainer_profile)
        if maintainer_local.exists():
            stack.append(maintainer_local)
    elif legacy_local.exists():
        stack.append(legacy_local)
    return stack


def load_config(config_path: str | Path | None = None) -> dict[str, Any]:
    """Load shared config with role-specific local overrides."""
    paths = _config_stack(config_path)

    config: dict[str, Any] = {}
    for path in paths:
        if not path.exists():
            if path == paths[0]:
                raise FileNotFoundError(f"Config file not found: {path}")
            continue
        with open(path) as f:
            fragment = yaml.safe_load(f) or {}
        config = _deep_merge(config, fragment)

    # Expand ~ in library_root
    if "library_root" in config:
        config["library_root"] = str(Path(config["library_root"]).expanduser())

    return config


def get_library_root(config: dict[str, Any] | None = None) -> Path:
    """Get the resolved library root path."""
    if config is None:
        config = load_config()
    return Path(config["library_root"])


def get_project_root() -> Path:
    """Get the project root directory.

    Back-compat alias for ``get_vault_root()``. New code should call
    ``get_vault_root()`` (data tree) or ``get_local_cache_root()`` (per-machine
    runtime tree) explicitly.
    """
    return _find_project_root()


def get_vault_root() -> Path:
    """Return the vault root: directory containing extracted/, wiki/, etc."""
    return _find_project_root()


def get_local_cache_root(config: dict[str, Any] | None = None) -> Path:
    """Return the per-machine cache root for runtime artifacts.

    Resolution order:
    1. ``ARQUIMEDES_LOCAL_CACHE`` environment variable
    2. ``local_cache_root`` key in the active config
    3. Vault root (back-compat default — runtime artifacts colocated with data)
    """
    env_cache = os.environ.get(_LOCAL_CACHE_ENV_VAR)
    if env_cache:
        return Path(env_cache).expanduser()
    if config is not None:
        explicit = config.get("local_cache_root")
        if explicit:
            return Path(explicit).expanduser()
    return get_vault_root()


def get_extracted_root() -> Path:
    return get_vault_root() / "extracted"


def get_wiki_root() -> Path:
    return get_vault_root() / "wiki"


def get_derived_root() -> Path:
    return get_vault_root() / "derived"


def get_manifests_root() -> Path:
    return get_vault_root() / "manifests"


def get_indexes_root(config: dict[str, Any] | None = None) -> Path:
    return get_local_cache_root(config) / "indexes"


def get_logs_root(config: dict[str, Any] | None = None) -> Path:
    return get_local_cache_root(config) / "logs"
