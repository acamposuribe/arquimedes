"""Configuration loading with local overrides."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

# Environment variable to explicitly set the project root
_ENV_VAR = "ARQUIMEDES_ROOT"


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
        "Cannot find config/config.yaml. Either run from inside the arquimedes repo, "
        f"or set {_ENV_VAR} to the repo root."
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


def load_config() -> dict[str, Any]:
    """Load config.yaml with config.local.yaml overrides."""
    root = _find_project_root()
    config_dir = root / "config"

    with open(config_dir / "config.yaml") as f:
        config = yaml.safe_load(f) or {}

    local_path = config_dir / "config.local.yaml"
    if local_path.exists():
        with open(local_path) as f:
            local = yaml.safe_load(f) or {}
        config = _deep_merge(config, local)

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
    """Get the project root directory."""
    return _find_project_root()
