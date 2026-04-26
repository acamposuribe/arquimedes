"""Arquimedes CLI — `arq` command entrypoint."""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

import click
import yaml

from arquimedes import __version__
from arquimedes.vault import DEFAULT_CLOUDFLARED_BINARY, DEFAULT_LIBRARY_ROOT


def _resolved_config_path() -> str | None:
    """Return the active vault config path for embedding into a launchd plist.

    Honors `$ARQUIMEDES_CONFIG` (which the global `--config` flag on the cli
    group sets when used). Returns None when nothing explicit is in effect, in
    which case the launchd job will fall back to cwd-walk under
    `working_directory`.
    """
    explicit = os.environ.get("ARQUIMEDES_CONFIG")
    return explicit or None


def _arq_program_args(*subcommand: str) -> list[str]:
    """Build a `python -m arquimedes.cli [--config <path>] <subcommand>` list.

    Used by every `--install` path so the launchd job is pinned to the vault
    config the maintainer was using when they ran `--install`.
    """
    base = [sys.executable, "-m", "arquimedes.cli"]
    cfg = _resolved_config_path()
    if cfg:
        base.extend(["--config", cfg])
    base.extend(subcommand)
    return base


def _mcp_config(config: dict) -> dict:
    mcp_cfg = config.get("mcp") or {}
    if not isinstance(mcp_cfg, dict):
        raise click.ClickException("mcp config must be a mapping in config.yaml")
    return mcp_cfg


def _string_list(value) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, (list, tuple)):
        return [str(item) for item in value if str(item).strip()]
    raise click.ClickException("expected a string or list of strings in mcp config")


def _mcp_server_from_config(config_path: str | None):
    from arquimedes.config import load_config
    from arquimedes.mcp_server import (
        _auth_config_from_mapping,
        build_server,
        build_transport_security,
    )

    config = load_config(config_path)
    mcp_cfg = _mcp_config(config)
    host = str(mcp_cfg.get("host") or "127.0.0.1")
    port = int(mcp_cfg.get("port") or 8000)
    mount_path = str(mcp_cfg.get("mount_path") or "/")
    sse_path = str(mcp_cfg.get("sse_path") or "/sse")
    streamable_http_path = str(mcp_cfg.get("streamable_http_path") or "/mcp")
    debug_http_log = bool(mcp_cfg.get("debug_http_log", False))
    transport = str(mcp_cfg.get("transport") or "streamable-http")
    if transport not in {"stdio", "sse", "streamable-http"}:
        raise click.ClickException("mcp.transport must be one of: stdio, sse, streamable-http")

    auth_mapping = mcp_cfg.get("auth") if isinstance(mcp_cfg.get("auth"), dict) else None
    auth_config = _auth_config_from_mapping(auth_mapping)
    transport_security = build_transport_security(
        allowed_hosts=_string_list(mcp_cfg.get("allowed_hosts")),
        allowed_origins=_string_list(mcp_cfg.get("allowed_origins")),
        disable_dns_rebinding_protection=bool(
            mcp_cfg.get("disable_dns_rebinding_protection", False)
        ),
    )
    server = build_server(
        config_path=config_path,
        host=host,
        port=port,
        mount_path=mount_path,
        sse_path=sse_path,
        streamable_http_path=streamable_http_path,
        auth_config=auth_config,
        debug_http_log=debug_http_log,
        transport_security=transport_security,
    )
    return server, transport, mount_path


def _mcp_cloudflare_tunnel_config(config: dict) -> dict | None:
    mcp_cfg = _mcp_config(config)
    tunnel_cfg = mcp_cfg.get("cloudflare_tunnel") or {}
    if not isinstance(tunnel_cfg, dict):
        raise click.ClickException("mcp.cloudflare_tunnel config must be a mapping in config.yaml")
    if not tunnel_cfg.get("enabled"):
        return None

    tunnel_name = str(tunnel_cfg.get("tunnel_name") or "").strip()
    if not tunnel_name:
        raise click.ClickException("mcp.cloudflare_tunnel.tunnel_name is required when enabled")

    binary_path = str(tunnel_cfg.get("binary_path") or "/opt/homebrew/bin/cloudflared")
    label = str(tunnel_cfg.get("label") or "com.arquimedes.cloudflared-tunnel")
    return {
        "label": label,
        "binary_path": binary_path,
        "tunnel_name": tunnel_name,
        "keep_alive": bool(tunnel_cfg.get("keep_alive", True)),
    }


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or "vault"


def _is_tty() -> bool:
    return sys.stdin.isatty() and sys.stdout.isatty()


def _cloudflared_default_binary() -> str:
    return shutil.which("cloudflared") or DEFAULT_CLOUDFLARED_BINARY


def _maintainer_config_file(config_path: str | None = None) -> Path:
    from arquimedes.config import get_vault_root

    if config_path:
        path = Path(config_path).expanduser()
        if not path.is_absolute():
            path = (Path.cwd() / path).resolve()
        return path
    return get_vault_root() / "config" / "maintainer" / "config.yaml"


def _load_yaml_file(path: Path) -> dict:
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        raise click.ClickException(f"{path} must contain a YAML mapping")
    return payload


def _write_yaml_file(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = yaml.safe_dump(payload, sort_keys=False, allow_unicode=False).strip()
    path.write_text(f"{text}\n", encoding="utf-8")


def _serve_hostnames(config: dict) -> list[str]:
    serve_cfg = config.get("serve") or {}
    if not isinstance(serve_cfg, dict):
        raise click.ClickException("serve config must be a mapping in config.yaml")
    if not bool(serve_cfg.get("public_exposure", False)):
        return []
    return _string_list(serve_cfg.get("allowed_hosts"))


def _cloudflare_ingress_entries(config: dict) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    seen: set[str] = set()

    mcp_cfg = _mcp_config(config)
    mcp_port = int(mcp_cfg.get("port") or 8000)
    for host in _string_list(mcp_cfg.get("allowed_hosts")):
        if host not in seen:
            entries.append({"hostname": host, "service": f"http://127.0.0.1:{mcp_port}"})
            seen.add(host)

    serve_cfg = config.get("serve") or {}
    if not isinstance(serve_cfg, dict):
        raise click.ClickException("serve config must be a mapping in config.yaml")
    serve_port = int(serve_cfg.get("port") or 8420)
    for host in _serve_hostnames(config):
        if host not in seen:
            entries.append({"hostname": host, "service": f"http://127.0.0.1:{serve_port}"})
            seen.add(host)

    return entries


def _ensure_cloudflared_binary(binary_path: str) -> dict:
    path = Path(binary_path)
    if path.exists():
        return {"binary_path": str(path), "installed": False}

    resolved = shutil.which("cloudflared")
    if resolved:
        return {"binary_path": resolved, "installed": False}

    brew = shutil.which("brew")
    if not brew:
        raise click.ClickException(
            "cloudflared is not installed and Homebrew is unavailable. "
            "Install cloudflared manually or set mcp.cloudflare_tunnel.binary_path."
        )

    proc = subprocess.run(
        [brew, "install", "cloudflared"],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise click.ClickException((proc.stderr or proc.stdout or "brew install cloudflared failed").strip())

    resolved = shutil.which("cloudflared")
    if resolved:
        return {"binary_path": resolved, "installed": True}

    if path.exists():
        return {"binary_path": str(path), "installed": True}

    raise click.ClickException("cloudflared install completed but the binary could not be located.")


def _ensure_cloudflared_login(binary_path: str) -> dict:
    cert_path = Path.home() / ".cloudflared" / "cert.pem"
    if cert_path.exists():
        return {"logged_in": False, "cert_path": str(cert_path)}

    if not _is_tty():
        raise click.ClickException(
            "Cloudflare login is required before tunnel setup. "
            "Re-run `arq mcp --install` interactively or run `cloudflared tunnel login` first."
        )

    proc = subprocess.run([binary_path, "tunnel", "login"], check=False)
    if proc.returncode != 0:
        raise click.ClickException("cloudflared tunnel login failed")
    if not cert_path.exists():
        raise click.ClickException("cloudflared tunnel login did not create ~/.cloudflared/cert.pem")
    return {"logged_in": True, "cert_path": str(cert_path)}


def _cloudflare_tunnel_list(binary_path: str, tunnel_name: str | None = None) -> list[dict]:
    args = [binary_path, "tunnel", "list", "--output", "json"]
    if tunnel_name:
        args.extend(["--name", tunnel_name])
    proc = subprocess.run(args, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        raise click.ClickException((proc.stderr or proc.stdout or "cloudflared tunnel list failed").strip())
    try:
        payload = json.loads(proc.stdout or "[]")
    except json.JSONDecodeError as exc:
        raise click.ClickException("cloudflared tunnel list returned invalid JSON") from exc
    if not isinstance(payload, list):
        raise click.ClickException("cloudflared tunnel list returned an unexpected payload")
    return payload


def _ensure_cloudflare_tunnel(binary_path: str, tunnel_name: str) -> dict:
    created = False
    for item in _cloudflare_tunnel_list(binary_path, tunnel_name):
        if str(item.get("name") or "") == tunnel_name and str(item.get("deleted_at") or "").startswith("0001-01-01"):
            tunnel_id = str(item.get("id") or "").strip()
            if tunnel_id:
                break
    else:
        proc = subprocess.run(
            [binary_path, "tunnel", "create", tunnel_name],
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode != 0:
            raise click.ClickException((proc.stderr or proc.stdout or "cloudflared tunnel create failed").strip())
        created = True
        tunnel_id = ""
        for item in _cloudflare_tunnel_list(binary_path, tunnel_name):
            if str(item.get("name") or "") == tunnel_name and str(item.get("deleted_at") or "").startswith("0001-01-01"):
                tunnel_id = str(item.get("id") or "").strip()
                break
        if not tunnel_id:
            raise click.ClickException(f"Created tunnel {tunnel_name!r} but could not resolve its id.")

    credentials_file = Path.home() / ".cloudflared" / f"{tunnel_id}.json"
    if not credentials_file.exists():
        proc = subprocess.run(
            [binary_path, "tunnel", "token", "--cred-file", str(credentials_file), tunnel_name],
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode != 0:
            raise click.ClickException((proc.stderr or proc.stdout or "cloudflared tunnel token failed").strip())

    return {
        "tunnel_name": tunnel_name,
        "tunnel_id": tunnel_id,
        "credentials_file": str(credentials_file),
        "created": created,
    }


def _ensure_cloudflare_dns_route(binary_path: str, tunnel_name: str, hostname: str) -> dict:
    proc = subprocess.run(
        [binary_path, "tunnel", "route", "dns", tunnel_name, hostname],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode == 0:
        return {"hostname": hostname, "created": True}

    message = (proc.stderr or proc.stdout or "cloudflared tunnel route dns failed").strip()
    lowered = message.lower()
    if "already exists" in lowered or "already routed" in lowered:
        return {"hostname": hostname, "created": False, "message": message}
    raise click.ClickException(message)


def _write_cloudflared_config(config: dict, tunnel_id: str, credentials_file: str) -> dict:
    ingress = _cloudflare_ingress_entries(config)
    if not ingress:
        raise click.ClickException(
            "No public hostnames are configured for the Cloudflare tunnel. "
            "Set mcp.allowed_hosts and optionally serve.allowed_hosts/public_exposure first."
        )

    config_path = Path.home() / ".cloudflared" / "config.yml"
    payload = {
        "tunnel": tunnel_id,
        "credentials-file": credentials_file,
        "ingress": [*ingress, {"service": "http_status:404"}],
    }
    config_path.parent.mkdir(parents=True, exist_ok=True)
    text = yaml.safe_dump(payload, sort_keys=False, allow_unicode=False).strip()
    config_path.write_text(f"{text}\n", encoding="utf-8")
    return {"path": str(config_path), "ingress": ingress}


def _prompt_required(prompt: str, *, default: str) -> str:
    value = str(click.prompt(prompt, default=default, show_default=True)).strip()
    if not value:
        raise click.ClickException(f"{prompt} is required")
    return value


def _ensure_mcp_install_config(config: dict, *, config_path: str | None = None) -> tuple[dict, Path]:
    maintainer_path = _maintainer_config_file(config_path)
    maintainer_config = _load_yaml_file(maintainer_path)
    changed = False

    mcp_cfg = maintainer_config.get("mcp")
    if not isinstance(mcp_cfg, dict):
        mcp_cfg = {}
        maintainer_config["mcp"] = mcp_cfg
        changed = True
    mcp_cfg.setdefault("transport", "streamable-http")
    mcp_cfg.setdefault("host", "127.0.0.1")
    mcp_cfg.setdefault("port", 8000)
    mcp_cfg.setdefault("streamable_http_path", "/mcp")
    mcp_cfg.setdefault("keep_alive", True)

    tunnel_cfg = mcp_cfg.get("cloudflare_tunnel")
    if not isinstance(tunnel_cfg, dict):
        tunnel_cfg = {}
        mcp_cfg["cloudflare_tunnel"] = tunnel_cfg
        changed = True
    tunnel_cfg.setdefault("label", "com.arquimedes.cloudflared-tunnel")
    tunnel_cfg.setdefault("keep_alive", True)
    tunnel_cfg.setdefault("binary_path", _cloudflared_default_binary())

    current_hosts = _string_list(mcp_cfg.get("allowed_hosts"))
    current_origins = _string_list(mcp_cfg.get("allowed_origins"))
    tunnel_name = str(tunnel_cfg.get("tunnel_name") or "").strip()
    cloudflared_enabled = bool(tunnel_cfg.get("enabled", False))

    if (not current_hosts or not tunnel_name or not cloudflared_enabled) and not _is_tty():
        raise click.ClickException(
            "MCP tunnel config is incomplete. Re-run `arq mcp --install` interactively "
            "or pre-fill mcp.allowed_hosts, mcp.allowed_origins, and mcp.cloudflare_tunnel."
        )

    if not current_hosts:
        default_host = f"mcp-{_slugify(maintainer_path.parent.parent.parent.name)}.example.com"
        host = _prompt_required("Public MCP hostname", default=default_host)
        mcp_cfg["allowed_hosts"] = [host]
        mcp_cfg["allowed_origins"] = [f"https://{host}", "https://chatgpt.com"]
        current_hosts = [host]
        current_origins = mcp_cfg["allowed_origins"]
        changed = True
    elif not current_origins:
        mcp_cfg["allowed_origins"] = [f"https://{current_hosts[0]}", "https://chatgpt.com"]
        changed = True

    if not tunnel_name:
        default_tunnel = f"arquimedes-{_slugify(maintainer_path.parent.parent.parent.name)}"
        tunnel_cfg["tunnel_name"] = _prompt_required("Cloudflare tunnel name", default=default_tunnel)
        changed = True
    if not cloudflared_enabled:
        tunnel_cfg["enabled"] = True
        changed = True

    serve_cfg = maintainer_config.get("serve")
    if serve_cfg is None:
        serve_cfg = {}
        maintainer_config["serve"] = serve_cfg
        changed = True
    if not isinstance(serve_cfg, dict):
        raise click.ClickException("serve config must be a mapping in config/maintainer/config.yaml")
    serve_cfg.setdefault("host", "0.0.0.0")
    serve_cfg.setdefault("port", 8420)

    if bool(serve_cfg.get("public_exposure", False)) and not _serve_hostnames(maintainer_config):
        if not _is_tty():
            raise click.ClickException(
                "serve.public_exposure is enabled but serve.allowed_hosts is empty. "
                "Set serve.allowed_hosts or re-run interactively."
            )
        default_host = f"{_slugify(maintainer_path.parent.parent.parent.name)}.example.com"
        serve_cfg["allowed_hosts"] = [_prompt_required("Public web UI hostname", default=default_host)]
        changed = True

    if changed:
        _write_yaml_file(maintainer_path, maintainer_config)
        from arquimedes.config import load_config

        return load_config(str(maintainer_path)), maintainer_path
    return config, maintainer_path


def _install_cloudflare_tunnel_launch_agent(tunnel_cfg: dict, *, working_directory: str) -> dict:
    from arquimedes.launchd import install as install_plist, render_plist

    binary_path = str(tunnel_cfg["binary_path"])
    if not Path(binary_path).exists():
        raise click.ClickException(
            f"cloudflared binary not found at {binary_path}. "
            "Set mcp.cloudflare_tunnel.binary_path or install cloudflared first."
        )

    plist = render_plist(
        str(tunnel_cfg["label"]),
        [binary_path, "tunnel", "run", str(tunnel_cfg["tunnel_name"])],
        working_directory=working_directory,
        run_at_load=True,
        keep_alive=bool(tunnel_cfg.get("keep_alive", True)),
    )
    return install_plist(str(tunnel_cfg["label"]), plist)


def _cloudflare_tunnel_status(tunnel_cfg: dict | None) -> dict:
    from arquimedes.launchd import status as launchd_status

    if tunnel_cfg is None:
        return {"enabled": False, "label": "com.arquimedes.cloudflared-tunnel", "managed": False}
    payload = launchd_status(str(tunnel_cfg["label"]))
    payload["enabled"] = True
    payload["managed"] = True
    payload["tunnel_name"] = str(tunnel_cfg["tunnel_name"])
    return payload


def _uninstall_cloudflare_tunnel(tunnel_cfg: dict | None) -> dict:
    from arquimedes.launchd import plist_path, uninstall as uninstall_plist

    label = str((tunnel_cfg or {}).get("label") or "com.arquimedes.cloudflared-tunnel")
    path = plist_path(label)
    if not path.exists():
        return {"label": label, "path": str(path), "installed": False, "present": False}
    payload = uninstall_plist(label)
    payload["present"] = False
    return payload


def _commit_and_push_if_changed(message: str) -> dict:
    from arquimedes.config import get_project_root

    root = get_project_root()
    subprocess.run(["git", "add", "-A"], cwd=root, capture_output=True, text=True, check=False)
    status = subprocess.run(["git", "status", "--porcelain"], cwd=root, capture_output=True, text=True, check=False)
    if not status.stdout.strip():
        return {"committed": False, "pushed": False}
    commit = subprocess.run(["git", "commit", "-m", message], cwd=root, capture_output=True, text=True, check=False)
    if commit.returncode != 0:
        raise click.ClickException((commit.stderr or commit.stdout or "git commit failed").strip())
    push = subprocess.run(["git", "push"], cwd=root, capture_output=True, text=True, check=False)
    if push.returncode != 0:
        raise click.ClickException((push.stderr or push.stdout or "git push failed").strip())
    return {"committed": True, "pushed": True}


@click.group()
@click.version_option(version=__version__, prog_name="arquimedes")
@click.option(
    "--config",
    "global_config_path",
    default=None,
    type=click.Path(),
    help="Path to a vault config file (overrides $ARQUIMEDES_CONFIG). Applies to every subcommand.",
)
@click.pass_context
def cli(ctx: click.Context, global_config_path: str | None):
    """Arquimedes — Collaborative LLM knowledge base for architecture."""
    ctx.ensure_object(dict)
    ctx.obj["global_config_path"] = global_config_path
    if global_config_path:
        os.environ["ARQUIMEDES_CONFIG"] = global_config_path


@cli.command("init")
@click.argument("path", type=click.Path())
@click.option(
    "--from",
    "from_url",
    default=None,
    help="Clone an existing vault from a git URL instead of scaffolding a new one.",
)
@click.option(
    "--no-git",
    is_flag=True,
    help="(scaffold mode only) Do not run `git init` in the new vault.",
)
@click.option(
    "--human",
    is_flag=True,
    help="Emit a short human-readable summary instead of JSON.",
)
@click.option(
    "--library-root",
    default=None,
    help="Shared library root to write into config/config.yaml.",
)
@click.option(
    "--serve-public-host",
    default=None,
    help="Public hostname for the read-only web UI behind Cloudflare Tunnel.",
)
@click.option(
    "--mcp-public-host",
    default=None,
    help="Public hostname for the remote MCP endpoint behind Cloudflare Tunnel.",
)
@click.option(
    "--tunnel-name",
    default=None,
    help="Named Cloudflare Tunnel to write into config/maintainer/config.yaml.",
)
@click.option(
    "--cloudflared-bin",
    default=None,
    help="cloudflared binary path to write into config/maintainer/config.yaml.",
)
def init_cmd(
    path: str,
    from_url: str | None,
    no_git: bool,
    human: bool,
    library_root: str | None,
    serve_public_host: str | None,
    mcp_public_host: str | None,
    tunnel_name: str | None,
    cloudflared_bin: str | None,
):
    """Scaffold a new vault at <path>, or clone one with --from <git-url>."""
    from arquimedes.vault import VaultExistsError, clone_vault, init_vault

    target = Path(path).expanduser().resolve()
    default_tunnel = f"arquimedes-{_slugify(target.name)}"

    if not from_url and _is_tty():
        library_root = library_root or _prompt_required("Library root", default=DEFAULT_LIBRARY_ROOT)
        if serve_public_host is None and click.confirm("Expose the web UI publicly through Cloudflare Tunnel?", default=False):
            serve_public_host = _prompt_required(
                "Public web UI hostname",
                default=f"{_slugify(target.name)}.example.com",
            )
        if mcp_public_host is None and click.confirm("Expose the MCP publicly through Cloudflare Tunnel?", default=True):
            mcp_public_host = _prompt_required(
                "Public MCP hostname",
                default=f"mcp-{_slugify(target.name)}.example.com",
            )
        if mcp_public_host and tunnel_name is None:
            tunnel_name = _prompt_required("Cloudflare tunnel name", default=default_tunnel)

    try:
        if from_url:
            result = clone_vault(from_url, path)
        else:
            result = init_vault(
                path,
                init_git=not no_git,
                library_root=library_root or DEFAULT_LIBRARY_ROOT,
                serve_public_host=serve_public_host,
                mcp_public_host=mcp_public_host,
                tunnel_name=tunnel_name or default_tunnel,
                cloudflared_binary=cloudflared_bin or _cloudflared_default_binary(),
            )
    except VaultExistsError as exc:
        raise click.ClickException(str(exc))
    except RuntimeError as exc:
        raise click.ClickException(str(exc))

    if human:
        verb = "Cloned" if from_url else "Initialized"
        click.echo(f"{verb} vault at {result.root}")
        if result.git_initialized:
            click.echo("git: initialized")
        click.echo(f"files: {len(result.files_created)} written")
        if from_url:
            click.echo("Next: write config/collaborator/config.local.yaml with library_root, then run `arq overview`.")
        else:
            click.echo(f"library_root: {library_root or DEFAULT_LIBRARY_ROOT}")
            if serve_public_host:
                click.echo(f"web_ui: https://{serve_public_host}")
            if mcp_public_host:
                click.echo(f"mcp: https://{mcp_public_host}/mcp")
                click.echo("Next: export ARQUIMEDES_CONFIG to config/maintainer/config.yaml, then run `arq mcp --install`.")
            else:
                click.echo("Next: export ARQUIMEDES_CONFIG to config/maintainer/config.yaml, then run `arq watch --install` and `arq serve --install`.")
    else:
        click.echo(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))


@cli.group("vault")
def vault_group():
    """Vault inspection and lifecycle commands."""


@vault_group.command("info")
@click.option(
    "--human",
    is_flag=True,
    help="Emit a short human-readable summary instead of JSON.",
)
def vault_info_cmd(human: bool):
    """Show the resolved vault root, library, cache, config sources, and git remote."""
    from arquimedes.vault import format_vault_info_human, vault_info

    try:
        info = vault_info()
    except FileNotFoundError as exc:
        raise click.ClickException(str(exc))

    if human:
        click.echo(format_vault_info_human(info))
    else:
        click.echo(json.dumps(info.to_dict(), ensure_ascii=False, indent=2))


@cli.command()
@click.argument("path", required=False)
def ingest(path: str | None):
    """Scan library for new materials and register them."""
    from arquimedes.ingest import ingest as do_ingest

    try:
        new_materials = do_ingest(path=path)
    except FileNotFoundError as e:
        raise click.ClickException(str(e))

    if new_materials:
        click.echo(f"Registered {len(new_materials)} new material(s):")
        for m in new_materials:
            click.echo(f"  {m.material_id}  {m.relative_path}  [{m.file_type}] ({m.collection})")
    else:
        click.echo("No new materials found.")


@cli.command("extract-raw")
@click.argument("material_id", required=False)
@click.option("--force", is_flag=True, help="Re-extract even if output already exists.")
def extract_raw(material_id: str | None, force: bool):
    """Deterministic extraction: text, pages, figures, tables, TOC, annotations."""
    from arquimedes.extract import extract_raw as do_extract

    click.echo("Running deterministic extraction...")
    try:
        extracted = do_extract(material_id=material_id, force=force)
    except (ValueError, FileNotFoundError) as e:
        raise click.ClickException(str(e))

    if extracted:
        click.echo(f"Extracted {len(extracted)} material(s):")
        for mid in extracted:
            click.echo(f"  {mid}")
    else:
        click.echo("Nothing to extract (all materials already extracted).")


@cli.command()
@click.argument("material_id", required=False)
@click.option("--force", is_flag=True, help="Re-enrich even if not stale.")
@click.option(
    "--stage",
    "stages",
    multiple=True,
    type=click.Choice(["document", "metadata", "chunk", "figure"]),
    help="Run only specific stage(s). Repeatable. Default: all stages.",
)
@click.option("--dry-run", is_flag=True, help="Report staleness without calling LLM.")
def enrich(material_id: str | None, force: bool, stages: tuple[str, ...], dry_run: bool):
    """LLM enrichment: summaries, facets, descriptions (stage stamps track provenance)."""
    from arquimedes.enrich import enrich as do_enrich
    from arquimedes.llm import EnrichmentError

    try:
        results, all_succeeded = do_enrich(
            material_id=material_id,
            force=force,
            stages=list(stages) if stages else None,
            dry_run=dry_run,
        )
    except EnrichmentError as e:
        raise click.ClickException(str(e))
    except (ValueError, FileNotFoundError) as e:
        raise click.ClickException(str(e))

    if not results:
        click.echo("Nothing to enrich (all materials up to date).")
        return

    for mid, material_result in results.items():
        title = material_result.get("title", mid)
        click.echo(f"\n{mid}  {title}")
        for stage_name in ["document", "metadata", "chunk", "figure"]:
            if stage_name in material_result:
                r = material_result[stage_name]
                status = r.get("status", "?")
                detail = r.get("detail", "")
                click.echo(f"  [{stage_name}] {status}: {detail}")

    if not all_succeeded:
        raise SystemExit(1)


@cli.command()
@click.argument("material_id", required=False)
@click.option("--force", is_flag=True, help="Re-extract and re-enrich even if not stale.")
@click.option(
    "--stage",
    "stages",
    multiple=True,
    type=click.Choice(["document", "metadata", "chunk", "figure"]),
    help="Run only specific enrichment stage(s). Repeatable. Default: all stages.",
)
def extract(material_id: str | None, force: bool, stages: tuple[str, ...]):
    """Convenience: runs extract-raw + enrich."""
    from arquimedes.extract import extract_raw as do_extract_raw
    from arquimedes.enrich import enrich as do_enrich
    from arquimedes.llm import EnrichmentError

    click.echo("Running deterministic extraction...")
    try:
        extracted = do_extract_raw(material_id=material_id, force=force)
    except (ValueError, FileNotFoundError) as e:
        raise click.ClickException(str(e))

    if extracted:
        click.echo(f"Extracted {len(extracted)} material(s):")
        for mid in extracted:
            click.echo(f"  {mid}")
    else:
        click.echo("Nothing to extract (all materials already extracted).")

    click.echo("Running LLM enrichment...")
    try:
        results, all_succeeded = do_enrich(
            material_id=material_id,
            force=force,
            stages=list(stages) if stages else None,
        )
    except EnrichmentError as e:
        raise click.ClickException(str(e))
    except (ValueError, FileNotFoundError) as e:
        raise click.ClickException(str(e))

    if not results:
        click.echo("Nothing to enrich (all materials up to date).")
    else:
        for mid, material_result in results.items():
            title = material_result.get("title", mid)
            click.echo(f"\n{mid}  {title}")
            for stage_name in ["document", "metadata", "chunk", "figure"]:
                if stage_name in material_result:
                    r = material_result[stage_name]
                    status = r.get("status", "?")
                    detail = r.get("detail", "")
                    click.echo(f"  [{stage_name}] {status}: {detail}")

    if not all_succeeded:
        raise SystemExit(1)


@cli.command()
@click.argument("query")
@click.option("--deep", is_flag=True, help="Multi-layer retrieval (depth 2 by default).")
@click.option("--depth", type=click.IntRange(1, 3), default=None, help="Retrieval depth 1-3 (overrides --deep default of 2).")
@click.option("--facet", multiple=True, help="Facet filter: key=value or key==value (exact). Repeatable.")
@click.option("--collection", help="Search within a specific collection.")
@click.option("--limit", default=20, show_default=True, help="Max number of material cards.")
@click.option("--chunk-limit", default=5, show_default=True, help="Max chunks per material at depth 2+.")
@click.option("--annotation-limit", default=3, show_default=True, help="Max annotations per material at depth 2+.")
@click.option("--figure-limit", default=3, show_default=True, help="Max figures per material at depth 2+.")
@click.option("--concept-limit", default=3, show_default=True, help="Max concept hits per material at depth 2+.")
@click.option("--human", is_flag=True, help="Pretty-printed output (default: JSON).")
def search(
    query: str,
    deep: bool,
    depth: int | None,
    facet: tuple[str, ...],
    collection: str | None,
    limit: int,
    chunk_limit: int,
    annotation_limit: int,
    figure_limit: int,
    concept_limit: int,
    human: bool,
):
    """Search the knowledge base (JSON output by default)."""
    from arquimedes.search import search as do_search, format_human

    # Resolve effective depth
    if depth is not None:
        effective_depth = depth
    elif deep:
        effective_depth = 2
    else:
        effective_depth = 1

    try:
        result = do_search(
            query,
            depth=effective_depth,
            facets=list(facet),
            collection=collection,
            limit=limit,
            chunk_limit=chunk_limit,
            annotation_limit=annotation_limit,
            figure_limit=figure_limit,
            concept_limit=concept_limit,
        )
    except FileNotFoundError as e:
        raise click.ClickException(str(e))

    if human:
        click.echo(format_human(result))
    else:
        click.echo(result.to_json())

    if result.total == 0:
        raise SystemExit(0)


@cli.command()
@click.argument("material_id")
@click.option("--limit", default=10, show_default=True, help="Max related materials to return.")
@click.option("--human", is_flag=True, help="Pretty-printed output (default: JSON).")
def related(material_id: str, limit: int, human: bool):
    """Find materials related to MATERIAL_ID via shared concepts, keywords, facets, or authors."""
    import json as _json
    from arquimedes.search import find_related, format_related_human

    try:
        results = find_related(material_id, limit=limit)
    except FileNotFoundError as e:
        raise click.ClickException(str(e))

    if human:
        click.echo(format_related_human(material_id, results))
    else:
        click.echo(_json.dumps(
            {"material_id": material_id, "related": [r.to_dict() for r in results]},
            ensure_ascii=False, indent=2,
        ))


@cli.command("material-clusters")
@click.argument("material_id")
@click.option("--human", is_flag=True, help="Pretty-printed output (default: JSON).")
def material_clusters(material_id: str, human: bool):
    """List collection-local clusters connected to MATERIAL_ID."""
    import json as _json
    from arquimedes.search import format_cluster_hits_human, get_material_clusters

    try:
        results = get_material_clusters(material_id)
    except FileNotFoundError as e:
        raise click.ClickException(str(e))

    if human:
        click.echo(format_cluster_hits_human(material_id, results))
    else:
        click.echo(_json.dumps(
            {"material_id": material_id, "local_clusters": [r.to_dict() for r in results]},
            ensure_ascii=False, indent=2,
        ))


@cli.command("collection-clusters")
@click.argument("domain")
@click.argument("collection")
@click.option("--human", is_flag=True, help="Pretty-printed output (default: JSON).")
def collection_clusters(domain: str, collection: str, human: bool):
    """List collection-local clusters for DOMAIN/COLLECTION."""
    import json as _json
    from arquimedes.search import format_cluster_hits_human, get_collection_clusters

    try:
        results = get_collection_clusters(domain, collection)
    except FileNotFoundError as e:
        raise click.ClickException(str(e))

    if human:
        click.echo(format_cluster_hits_human(f"{domain}/{collection}", results))
    else:
        click.echo(_json.dumps(
            {"domain": domain, "collection": collection, "local_clusters": [r.to_dict() for r in results]},
            ensure_ascii=False, indent=2,
        ))


@cli.command()
@click.option("--min-materials", default=1, show_default=True, help="Only show concepts appearing in at least N materials.")
@click.option("--limit", default=100, show_default=True, help="Max concepts to return.")
@click.option("--human", is_flag=True, help="Pretty-printed output (default: JSON).")
def concepts(min_materials: int, limit: int, human: bool):
    """List concept candidates across the collection with material counts."""
    import json as _json
    from arquimedes.search import list_concepts, format_concepts_human

    try:
        entries = list_concepts(min_materials=min_materials, limit=limit)
    except FileNotFoundError as e:
        raise click.ClickException(str(e))

    if human:
        click.echo(format_concepts_human(entries))
    else:
        click.echo(_json.dumps(
            [e.to_dict() for e in entries],
            ensure_ascii=False, indent=2,
        ))


def _format_card_human(card: dict) -> str:
    lines = [
        f"{card['title']} ({card['material_id']})",
        f"  {card['domain']}/{card['collection']}  {card['document_type']}  {card['year']}",
        f"  wiki: {card['wiki_path']}",
    ]
    if card.get("authors"):
        lines.append(f"  authors: {', '.join(card['authors'])}")
    c = card["counts"]
    lines.append(
        f"  counts: {c['pages']} pages, {c['chunks']} chunks, {c['annotations']} annotations, {c['figures']} figures"
    )
    if card.get("summary"):
        lines.append("")
        lines.append(card["summary"])
    return "\n".join(lines)


def _format_chunk_list_human(chunks: list[dict]) -> str:
    if not chunks:
        return "(no chunks)"
    out = []
    for c in chunks:
        marker = "*" if c.get("emphasized") else " "
        pages = ",".join(str(p) for p in c.get("source_pages") or [])
        summary = c.get("summary") or ""
        out.append(f"{marker} {c['chunk_id']}  p{pages}  {summary}")
    return "\n".join(out)


def _format_figure_list_human(rows: list[dict]) -> str:
    if not rows:
        return "(no figures)"
    out = []
    for f in rows:
        out.append(f"  {f['figure_id']}  p{f['source_page']}  [{f.get('visual_type') or '-'}]  {f.get('caption') or ''}")
    return "\n".join(out)


def _format_annotations_human(rows: list[dict]) -> str:
    if not rows:
        return "(no annotations)"
    out = []
    for a in rows:
        body = a.get("quoted_text") or a.get("comment") or ""
        out.append(f"  {a.get('annotation_id','')}  p{a.get('page',0)}  [{a.get('type','')}]  {body}")
    return "\n".join(out)


def _format_overview_human(overview: dict) -> str:
    c = overview["counts"]
    lines = [
        f"corpus at {overview['project_root']}",
        f"  index: {'present' if overview['index_exists'] else 'missing'} ({overview['index_path']})",
        f"  counts: {c['materials']} materials, {c['chunks']} chunks, {c['figures']} figures, {c['annotations']} annotations, {c['wiki_pages']} wiki pages",
    ]
    if overview.get("collections"):
        lines.append("  collections:")
        for col in overview["collections"]:
            lines.append(f"    {col['domain']}/{col['collection']}: {col['material_count']}")
    return "\n".join(lines)


@cli.command()
@click.argument("material_id")
@click.option("--page", type=int, help="Return the text for one page.")
@click.option("--chunk", "chunk_id", help="Return one chunk by id.")
@click.option("--full", is_flag=True, help="Return the full text.md body (large).")
@click.option(
    "--detail",
    type=click.Choice(["chunks", "figures", "annotations"]),
    help="Return a compact index of one aspect alongside the card.",
)
@click.option("--human", is_flag=True, help="Pretty-printed output (default: JSON).")
def read(
    material_id: str,
    page: int | None,
    chunk_id: str | None,
    full: bool,
    detail: str | None,
    human: bool,
):
    """Read extracted content for a material. See docs/collaborator/agent-handbook.md."""
    from arquimedes.agent_cli import ensure_guard, emit, not_found
    from arquimedes import read as read_mod

    @ensure_guard
    def run():
        selected = [
            name for name, value in (("--page", page is not None), ("--chunk", bool(chunk_id)), ("--full", full), ("--detail", bool(detail)))
            if value
        ]
        if len(selected) > 1:
            raise click.UsageError(f"Options are mutually exclusive: {', '.join(selected)}")

        try:
            if page is not None:
                payload = read_mod.get_page(material_id, page)
                emit(payload, human=human, human_formatter=lambda p: p.get("text", ""))
                return
            if chunk_id:
                payload = read_mod.get_chunk_by_id(material_id, chunk_id)
                emit(payload, human=human, human_formatter=lambda c: c.get("text", ""))
                return
            if full:
                path = read_mod.material_extracted_text_path(material_id)
                if not path:
                    raise not_found(f"text.md for material {material_id!r}")
                emit(path.read_text(encoding="utf-8"), human=human)
                return

            card = read_mod.build_material_card(material_id)
            if detail == "chunks":
                payload = {"card": card, "chunks": read_mod.list_chunks_compact(material_id)}
                emit(payload, human=human, human_formatter=lambda d: _format_card_human(d["card"]) + "\n\n" + _format_chunk_list_human(d["chunks"]))
                return
            if detail == "figures":
                payload = {"card": card, "figures": read_mod.list_figures_compact(material_id)}
                emit(payload, human=human, human_formatter=lambda d: _format_card_human(d["card"]) + "\n\n" + _format_figure_list_human(d["figures"]))
                return
            if detail == "annotations":
                payload = {"card": card, "annotations": read_mod.list_annotations(material_id)}
                emit(payload, human=human, human_formatter=lambda d: _format_card_human(d["card"]) + "\n\n" + _format_annotations_human(d["annotations"]))
                return

            emit(card, human=human, human_formatter=_format_card_human)
        except FileNotFoundError as exc:
            raise not_found(str(exc), hint=f"arq read {material_id}")

    run()


@cli.command()
@click.argument("material_id")
@click.option("--visual-type", "visual_type", help="Filter by visual_type (e.g. diagram, photograph).")
@click.option("--figure", "figure_id", help="Return one figure by id.")
@click.option("--human", is_flag=True, help="Pretty-printed output (default: JSON).")
def figures(material_id: str, visual_type: str | None, figure_id: str | None, human: bool):
    """List figures (or one figure) for a material. See docs/collaborator/agent-handbook.md."""
    from arquimedes.agent_cli import ensure_guard, emit, not_found
    from arquimedes import read as read_mod

    @ensure_guard
    def run():
        try:
            if figure_id:
                payload = read_mod.get_figure(material_id, figure_id)
                emit(
                    payload,
                    human=human,
                    human_formatter=lambda f: f"{f.get('figure_id','')}  p{f.get('source_page',0)}\n{(f.get('caption') or {}).get('value') if isinstance(f.get('caption'), dict) else (f.get('caption') or '')}",
                )
                return
            rows = read_mod.list_figures_compact(material_id, visual_type=visual_type)
            emit(rows, human=human, human_formatter=_format_figure_list_human)
        except FileNotFoundError as exc:
            raise not_found(str(exc), hint=f"arq read {material_id}")

    run()


@cli.command()
@click.argument("material_id")
@click.option("--page", type=int, help="Filter by page number.")
@click.option("--type", "kind", help="Filter by annotation type (highlight, note, underline, strikeout).")
@click.option("--human", is_flag=True, help="Pretty-printed output (default: JSON).")
def annotations(material_id: str, page: int | None, kind: str | None, human: bool):
    """List reader annotations for a material. See docs/collaborator/agent-handbook.md."""
    from arquimedes.agent_cli import ensure_guard, emit, not_found
    from arquimedes import read as read_mod

    @ensure_guard
    def run():
        try:
            rows = read_mod.list_annotations(material_id, page=page, kind=kind)
        except FileNotFoundError as exc:
            raise not_found(str(exc), hint=f"arq read {material_id}")
        emit(rows, human=human, human_formatter=_format_annotations_human)

    run()


@cli.command()
@click.option("--human", is_flag=True, help="Pretty-printed output (default: JSON).")
def overview(human: bool):
    """Corpus-wide snapshot: counts, collections, stamps. See docs/collaborator/agent-handbook.md."""
    from arquimedes.agent_cli import ensure_guard, emit
    from arquimedes import read as read_mod

    @ensure_guard
    def run():
        emit(read_mod.build_corpus_overview(), human=human, human_formatter=_format_overview_human)

    run()


@cli.command()
@click.option("--human", is_flag=True, help="Pretty-printed output (default: JSON).")
def refresh(human: bool):
    """Pull (if applicable) and ensure the index + memory are current."""
    from arquimedes.freshness import update_workspace

    def _format_refresh_human(status: dict) -> str:
        lines = [
            f"repo_applicable: {status.get('repo_applicable')}",
            f"pull_attempted:  {status.get('pull_attempted')}",
            f"pull_result:     {status.get('pull_result')}",
            f"reset_result:    {status.get('reset_result')}",
            f"clean_result:    {status.get('clean_result')}",
            f"index_rebuilt:   {status.get('index_rebuilt')}",
            f"memory_rebuilt:  {status.get('memory_rebuilt')}",
        ]
        return "\n".join(lines)

    try:
        status = update_workspace()
    except FileNotFoundError as exc:
        raise click.ClickException(str(exc))

    from arquimedes.agent_cli import emit
    emit(status, human=human, human_formatter=_format_refresh_human)


@cli.command("cluster")
@click.option("--force", is_flag=True, help="Re-cluster even if input is unchanged.")
@click.option("--domain", help="Cluster only one domain.")
@click.option("--collection", help="Cluster only one collection.")
def cluster_cmd(force: bool, domain: str | None, collection: str | None):
    """Cluster concepts into collection-local canonical concept homes."""
    from arquimedes.cluster import cluster_concepts
    from arquimedes.llm import EnrichmentError
    from arquimedes.config import load_config

    llm_state: dict = {}

    try:
        summary = cluster_concepts(load_config(), force=force, llm_state=llm_state, domain=domain, collection=collection)
    except EnrichmentError as e:
        raise click.ClickException(str(e))
    except FileNotFoundError as e:
        raise click.ClickException(str(e))

    if summary and summary.get("skipped"):
        click.echo("Clustering is up to date — skipped.")
    elif summary:
        total = summary.get("total_concepts", 0)
        n_clusters = summary.get("clusters", 0)
        multi = summary.get("multi_material", 0)
        scopes = summary.get("collections", 0)
        click.echo(f"Local: {total} concepts → {n_clusters} clusters ({multi} multi-material, {scopes} collections)")


@cli.command()
@click.option("--full", is_flag=True, help="Full rebuild instead of incremental.")
@click.option("--force-cluster", is_flag=True, help="Re-run clustering before compiling.")
@click.option("--recompile-pages", is_flag=True, help="Re-render wiki pages from existing clusters without reclustering.")
def compile(full: bool, force_cluster: bool, recompile_pages: bool):
    """Compile wiki pages from enriched materials and concept clusters."""
    from arquimedes.compile import compile_wiki
    from arquimedes.llm import EnrichmentError
    from arquimedes.config import load_config

    try:
        summary = compile_wiki(
            load_config(),
            force=full,
            force_cluster=force_cluster,
            recompile_pages=recompile_pages,
        )
    except EnrichmentError as e:
        raise click.ClickException(str(e))
    except FileNotFoundError as e:
        raise click.ClickException(str(e))

    cl = summary.get("clustering", {})
    local = cl.get("local", {}) if isinstance(cl, dict) else {}
    bridge = cl.get("bridge", {}) if isinstance(cl, dict) else {}
    if local:
        if local.get("skipped"):
            click.echo("Local clustering is up to date — skipped.")
        else:
            total = local.get("total_concepts", 0)
            n_clusters = local.get("clusters", 0)
            multi = local.get("multi_material", 0)
            click.echo(f"Local: {total} concepts → {n_clusters} clusters ({multi} multi-material)")
    if bridge:
        if bridge.get("skipped"):
            click.echo("Bridge clustering is up to date — skipped.")
        else:
            total = bridge.get("bridge_concepts", 0)
            n_clusters = bridge.get("clusters", 0)
            multi = bridge.get("multi_material", 0)
            click.echo(f"Bridge: {total} concepts → {n_clusters} clusters ({multi} multi-material)")
    click.echo("Compiling:")
    click.echo(f"  {summary['material_pages']} material page(s) written, {summary['material_pages_skipped']} skipped")
    click.echo(f"  {summary['concept_pages']} concept page(s) written")
    click.echo(f"  {summary['index_pages']} index page(s) written")
    if summary["orphans_removed"]:
        click.echo(f"  {summary['orphans_removed']} orphan page(s) removed")
    click.echo("Done. wiki/ updated.")


@cli.command()
@click.option("--quick", is_flag=True, help="Deterministic checks only (no LLM)")
@click.option("--full", is_flag=True, help="Deterministic checks plus reflective LLM passes")
@click.option(
    "--stage",
    "stages",
    multiple=True,
    type=click.Choice(["cluster-audit", "concept-reflection", "collection-reflection", "global-bridge", "graph-maintenance"], case_sensitive=False),
    help="Run only specific reflective stage(s). Repeatable.",
)
@click.option("--report", is_flag=True, help="Write report to wiki/_lint_report.md")
@click.option("--fix", is_flag=True, help="Auto-fix deterministic issues, queue LLM suggestions")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable JSON")
@click.option("--install-full", is_flag=True, help="Install daily launchd job for arq lint --full")
@click.option("--commit-push", is_flag=True, help="Commit and push changed artifacts after lint succeeds.")
def lint(quick: bool, full: bool, stages: tuple[str, ...], report: bool, fix: bool, as_json: bool, install_full: bool, commit_push: bool):
    """Run health checks on the knowledge base."""
    from arquimedes.config import load_config
    from arquimedes.lint import lint_exit_code, run_lint

    if install_full:
        from arquimedes.config import get_project_root
        from arquimedes.launchd import install, render_plist

        config = load_config()
        cron = str(config.get("lint_full", {}).get("schedule_cron", "0 2 * * *"))
        minute, hour, *_ = cron.split()
        plist = render_plist(
            "com.arquimedes.lint-full",
            _arq_program_args("lint", "--full", "--commit-push"),
            working_directory=str(get_project_root()),
            start_calendar_interval={"Hour": int(hour), "Minute": int(minute)},
        )
        try:
            result = install("com.arquimedes.lint-full", plist)
        except RuntimeError as e:
            raise click.ClickException(str(e))
        click.echo(json.dumps(result, indent=2))
        return

    try:
        result = run_lint(load_config(), quick=quick, full=full, report=report, fix=fix, stages=list(stages) if stages else None)
    except (ValueError, FileNotFoundError) as e:
        raise click.ClickException(str(e))
    except Exception as e:
        raise click.ClickException(str(e))

    if as_json:
        click.echo(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        det = result.get("deterministic", {})
        summary = det.get("summary", {}) if isinstance(det, dict) else {}
        click.echo(f"Deterministic lint ({result.get('mode', 'quick')}):")
        click.echo(f"  issues: {summary.get('issues', 0)}")
        click.echo(f"  high:   {summary.get('high', 0)}")
        click.echo(f"  medium: {summary.get('medium', 0)}")
        click.echo(f"  low:    {summary.get('low', 0)}")

        fixes = result.get("fixes")
        if isinstance(fixes, dict) and fixes.get("details"):
            click.echo("Fixes:")
            for item in fixes["details"]:
                click.echo(f"  - {item}")

        reflection = result.get("reflection")
        if isinstance(reflection, dict):
            click.echo("Reflective passes:")
            click.echo(f"  cluster reviews:      {reflection.get('cluster_reviews', 0)}")
            click.echo(f"  concept reflections:  {reflection.get('concept_reflections', 0)}")
            click.echo(f"  collection reflections: {reflection.get('collection_reflections', 0)}")
            click.echo(f"  global bridges:       {reflection.get('global_bridges', 0)}")
            click.echo(f"  graph maintenance:    {reflection.get('graph_maintenance', 0)}")

        click.echo(f"Lint report: {result.get('report_path')}")

    if commit_push:
        publish = _commit_and_push_if_changed("auto: nightly lint --full")
        if as_json:
            click.echo(json.dumps({"publish": publish}, ensure_ascii=False, indent=2))
        else:
            click.echo(f"Publish: committed={publish['committed']} pushed={publish['pushed']}")

    raise SystemExit(lint_exit_code(result))


@cli.group()
def index():
    """Manage the search index."""
    pass


@index.command("rebuild")
def index_rebuild():
    """Rebuild the search index from scratch."""
    from arquimedes.index import rebuild_index

    click.echo("Building search index...")
    try:
        stats = rebuild_index()
    except Exception as e:
        raise click.ClickException(str(e))

    click.echo(f"  materials:   {stats.materials}")
    click.echo(f"  chunks:      {stats.chunks}")
    click.echo(f"  figures:     {stats.figures}")
    click.echo(f"  annotations: {stats.annotations}")
    click.echo(f"  concepts:    {stats.concepts}")
    click.echo(f"Index built in {stats.elapsed:.1f}s → indexes/search.sqlite")


@index.command("ensure")
def index_ensure():
    """Rebuild search index and memory bridge only if stale."""
    from arquimedes.index import ensure_index_and_memory

    try:
        index_rebuilt, stats, memory_rebuilt, memory_counts = ensure_index_and_memory()
    except Exception as e:
        raise click.ClickException(str(e))

    if index_rebuilt and stats is not None:
        click.echo("Index is stale — rebuilding...")
        click.echo(f"  materials:   {stats.materials}")
        click.echo(f"  chunks:      {stats.chunks}")
        click.echo(f"  figures:     {stats.figures}")
        click.echo(f"  annotations: {stats.annotations}")
        click.echo(f"  concepts:    {stats.concepts}")
        click.echo(f"Index rebuilt in {stats.elapsed:.1f}s → indexes/search.sqlite")
    else:
        click.echo("Index is current.")

    if memory_rebuilt and not memory_counts.get("skipped"):
        click.echo("Memory bridge is stale — rebuilding...")
        click.echo(f"  clusters:              {memory_counts.get('clusters', 0)}")
        click.echo(f"  aliases:               {memory_counts.get('aliases', 0)}")
        click.echo(f"  cluster-material links:{memory_counts.get('cluster_material_links', 0)}")
        click.echo(f"  cluster relations:     {memory_counts.get('cluster_relations', 0)}")
        click.echo(f"  wiki pages:            {memory_counts.get('wiki_pages', 0)}")
        click.echo("Memory bridge rebuilt.")


@cli.command()
@click.option("--config", "config_path", help="Path to a role config file, e.g. config/maintainer/config.yaml.")
@click.option("--install", is_flag=True, help="Install launchd scheduled scan job.")
@click.option("--uninstall", is_flag=True, help="Uninstall launchd scheduled scan job.")
@click.option("--status", "show_status", is_flag=True, help="Show launchd job status.")
@click.option("--once", is_flag=True, help="Run one scan/publish cycle and exit.")
def watch(config_path: str | None, install: bool, uninstall: bool, show_status: bool, once: bool):
    """Run the scheduled scan publisher (server mode)."""
    from arquimedes.config import get_project_root, load_config

    label = "com.arquimedes.watch"
    if install or uninstall or show_status:
        from arquimedes.launchd import install as install_plist, render_plist, status as launchd_status, uninstall as uninstall_plist

        try:
            if uninstall:
                click.echo(json.dumps(uninstall_plist(label), indent=2))
                return
            if show_status:
                click.echo(json.dumps(launchd_status(label), indent=2))
                return
            config = load_config(config_path)
            interval = int(config.get("watch", {}).get("scan_interval_minutes", 30) or 30) * 60
            args = _arq_program_args("watch", "--once")
            if config_path and "--config" not in args:
                args.extend(["--config", config_path])
            plist = render_plist(
                label,
                args,
                working_directory=str(get_project_root()),
                start_interval=interval,
                run_at_load=True,
            )
            click.echo(json.dumps(install_plist(label, plist), indent=2))
            return
        except RuntimeError as e:
            raise click.ClickException(str(e))

    from arquimedes.watch import WatchDaemon

    daemon = WatchDaemon(config_path=config_path)
    try:
        if once:
            click.echo(json.dumps(daemon.run_once(), ensure_ascii=False, indent=2))
        else:
            daemon.start()
    except (FileNotFoundError, RuntimeError) as e:
        raise click.ClickException(str(e))


@cli.group()
def memory():
    """Manage the memory bridge (canonical concept graph in SQLite)."""
    pass


@memory.command("rebuild")
def memory_rebuild_cmd():
    """Project canonical concept clusters and wiki paths into search.sqlite."""
    from arquimedes.memory import memory_rebuild

    click.echo("Rebuilding memory bridge...")
    try:
        counts = memory_rebuild()
    except FileNotFoundError as e:
        raise click.ClickException(str(e))

    click.echo(f"  clusters:              {counts['clusters']}")
    click.echo(f"  aliases:               {counts['aliases']}")
    click.echo(f"  cluster-material links:{counts['cluster_material_links']}")
    click.echo(f"  cluster relations:     {counts['cluster_relations']}")
    click.echo(f"  wiki pages:            {counts['wiki_pages']}")
    click.echo("Memory bridge rebuilt → indexes/search.sqlite")


@memory.command("ensure")
def memory_ensure_cmd():
    """Rebuild memory bridge only if cluster or manifest inputs changed."""
    from arquimedes.memory import memory_ensure

    try:
        rebuilt, counts = memory_ensure()
    except FileNotFoundError as e:
        raise click.ClickException(str(e))

    if rebuilt:
        click.echo("Memory bridge is stale — rebuilding...")
        click.echo(f"  clusters:              {counts['clusters']}")
        click.echo(f"  aliases:               {counts['aliases']}")
        click.echo(f"  cluster-material links:{counts['cluster_material_links']}")
        click.echo(f"  cluster relations:     {counts['cluster_relations']}")
        click.echo(f"  wiki pages:            {counts['wiki_pages']}")
        click.echo("Memory bridge rebuilt → indexes/search.sqlite")
    else:
        click.echo("Memory bridge is current.")


@cli.command()
@click.option("--install", is_flag=True, help="Install launchd service for auto-sync.")
@click.option("--uninstall", is_flag=True, help="Uninstall launchd service for auto-sync.")
@click.option("--status", "show_status", is_flag=True, help="Show launchd service status.")
@click.option("--once", is_flag=True, help="Run one sync cycle and exit.")
def sync(install: bool, uninstall: bool, show_status: bool, once: bool):
    """Run collaborator auto-sync and local ensure."""
    label = "com.arquimedes.sync"
    if install or uninstall or show_status:
        from arquimedes.config import get_project_root, load_config
        from arquimedes.launchd import install as install_plist, render_plist, status as launchd_status, uninstall as uninstall_plist

        try:
            if uninstall:
                click.echo(json.dumps(uninstall_plist(label), indent=2))
                return
            if show_status:
                click.echo(json.dumps(launchd_status(label), indent=2))
                return
            config = load_config()
            interval = int(config.get("sync", {}).get("pull_interval", 300) or 300)
            plist = render_plist(
                label,
                _arq_program_args("sync", "--once"),
                working_directory=str(get_project_root()),
                start_interval=interval,
                run_at_load=True,
            )
            click.echo(json.dumps(install_plist(label, plist), indent=2))
            return
        except RuntimeError as e:
            raise click.ClickException(str(e))

    from arquimedes.sync import SyncDaemon

    daemon = SyncDaemon()
    try:
        if once:
            click.echo(json.dumps(daemon.run_once(), ensure_ascii=False, indent=2))
        else:
            daemon.start()
    except RuntimeError as e:
        raise click.ClickException(str(e))


@cli.command()
@click.option("--host", default=None, help="Host to bind to")
@click.option("--port", type=int, default=None, help="Port to listen on")
@click.option("--config", "config_path", help="Path to a role config file, e.g. config/maintainer/config.yaml.")
@click.option("--install", is_flag=True, help="Install launchd job that keeps the web UI running.")
@click.option("--uninstall", is_flag=True, help="Uninstall launchd web UI job.")
@click.option("--status", "show_status", is_flag=True, help="Show launchd web UI job status.")
def serve(host: str | None, port: int | None, config_path: str | None, install: bool, uninstall: bool, show_status: bool):
    """Start the web UI."""
    from arquimedes.config import load_config

    label = "com.arquimedes.serve"
    if install or uninstall or show_status:
        from arquimedes.config import get_project_root
        from arquimedes.launchd import install as install_plist, render_plist, status as launchd_status, uninstall as uninstall_plist

        try:
            if uninstall:
                click.echo(json.dumps(uninstall_plist(label), indent=2))
                return
            if show_status:
                click.echo(json.dumps(launchd_status(label), indent=2))
                return
            args = _arq_program_args("serve")
            if host:
                args.extend(["--host", host])
            if port is not None:
                args.extend(["--port", str(port)])
            if config_path and "--config" not in args:
                args.extend(["--config", config_path])
            plist = render_plist(
                label,
                args,
                working_directory=str(get_project_root()),
                run_at_load=True,
                keep_alive=True,
            )
            click.echo(json.dumps(install_plist(label, plist), indent=2))
            return
        except RuntimeError as e:
            raise click.ClickException(str(e))

    import uvicorn

    try:
        from arquimedes.serve import create_app
    except ModuleNotFoundError as exc:
        raise click.ClickException(
            f"Missing web UI dependency: {exc.name}. Install project dependencies "
            "for the current Python environment, e.g. `python3 -m pip install -e .`."
        ) from exc

    config = load_config(config_path)
    serve_cfg = config.get("serve") or {}
    uvicorn.run(
        create_app(config),
        host=host or serve_cfg.get("host") or "127.0.0.1",
        port=port or int(serve_cfg.get("port") or 8420),
    )


@cli.command()
@click.option("--config", "config_path", help="Path to a role config file, e.g. config/maintainer/config.yaml.")
@click.option("--install", is_flag=True, help="Install launchd job that keeps the remote MCP server running.")
@click.option("--uninstall", is_flag=True, help="Uninstall launchd MCP job.")
@click.option("--status", "show_status", is_flag=True, help="Show launchd MCP job status.")
def mcp(config_path: str | None, install: bool, uninstall: bool, show_status: bool):
    """Start the remote MCP server using the active maintainer config."""
    label = "com.arquimedes.mcp"
    if install or uninstall or show_status:
        from arquimedes.config import get_project_root, load_config
        from arquimedes.launchd import install as install_plist, render_plist, status as launchd_status, uninstall as uninstall_plist

        try:
            config = load_config(config_path)
            if uninstall:
                tunnel_cfg = _mcp_cloudflare_tunnel_config(config)
                click.echo(
                    json.dumps(
                        {
                            "mcp": uninstall_plist(label),
                            "cloudflare_tunnel": _uninstall_cloudflare_tunnel(tunnel_cfg),
                        },
                        indent=2,
                    )
                )
                return
            if show_status:
                tunnel_cfg = _mcp_cloudflare_tunnel_config(config)
                click.echo(
                    json.dumps(
                        {
                            "mcp": launchd_status(label),
                            "cloudflare_tunnel": _cloudflare_tunnel_status(tunnel_cfg),
                        },
                        indent=2,
                    )
                )
                return

            config, maintainer_path = _ensure_mcp_install_config(config, config_path=config_path)
            tunnel_cfg = _mcp_cloudflare_tunnel_config(config)
            mcp_cfg = _mcp_config(config)

            cloudflared_setup: dict[str, object]
            cloudflared_routes: list[dict] = []
            cloudflared_config: dict[str, object] | None = None
            tunnel_setup: dict[str, object] | None = None
            login_setup: dict[str, object] | None = None

            if tunnel_cfg is not None:
                cloudflared_setup = _ensure_cloudflared_binary(str(tunnel_cfg["binary_path"]))
                resolved_binary = str(cloudflared_setup["binary_path"])
                tunnel_cfg["binary_path"] = resolved_binary

                maintainer_payload = _load_yaml_file(maintainer_path)
                maintainer_mcp = maintainer_payload.setdefault("mcp", {})
                maintainer_tunnel = maintainer_mcp.setdefault("cloudflare_tunnel", {})
                maintainer_tunnel["binary_path"] = resolved_binary
                _write_yaml_file(maintainer_path, maintainer_payload)

                config = load_config(str(maintainer_path))
                tunnel_cfg = _mcp_cloudflare_tunnel_config(config)
                login_setup = _ensure_cloudflared_login(resolved_binary)
                tunnel_setup = _ensure_cloudflare_tunnel(resolved_binary, str(tunnel_cfg["tunnel_name"]))
                cloudflared_config = _write_cloudflared_config(
                    config,
                    str(tunnel_setup["tunnel_id"]),
                    str(tunnel_setup["credentials_file"]),
                )
                for entry in _cloudflare_ingress_entries(config):
                    cloudflared_routes.append(
                        _ensure_cloudflare_dns_route(
                            resolved_binary,
                            str(tunnel_cfg["tunnel_name"]),
                            str(entry["hostname"]),
                        )
                    )
            else:
                cloudflared_setup = {"enabled": False, "managed": False}

            args = _arq_program_args("mcp")
            effective_config_path = str(maintainer_path if install else _maintainer_config_file(config_path))
            if effective_config_path and "--config" not in args:
                args.extend(["--config", effective_config_path])
            plist = render_plist(
                label,
                args,
                working_directory=str(get_project_root()),
                run_at_load=True,
                keep_alive=bool(mcp_cfg.get("keep_alive", True)),
            )
            result = {
                "mcp": install_plist(label, plist),
                "cloudflare_tunnel": (
                    _install_cloudflare_tunnel_launch_agent(
                        tunnel_cfg,
                        working_directory=str(get_project_root()),
                    )
                    if tunnel_cfg is not None
                    else {"enabled": False, "managed": False}
                ),
                "cloudflared_binary": cloudflared_setup,
                "cloudflare_login": login_setup,
                "cloudflare_named_tunnel": tunnel_setup,
                "cloudflare_dns": cloudflared_routes,
                "cloudflared_config": cloudflared_config,
            }
            click.echo(json.dumps(result, indent=2))
            return
        except RuntimeError as e:
            raise click.ClickException(str(e))

    try:
        server, transport, mount_path = _mcp_server_from_config(config_path)
    except RuntimeError as e:
        raise click.ClickException(str(e))
    server.run(transport=transport, mount_path=mount_path)


@cli.command()
def status():
    """Show system stats and recent additions."""
    click.echo("arq status: not yet implemented")


if __name__ == "__main__":
    cli()
