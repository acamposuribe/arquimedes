"""Minimal MCP server exposing collaborator-safe Arquimedes tools.

This server exists for local agent clients that cannot invoke the `arq` CLI
through shell access but can connect to an MCP server over stdio.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
from typing import Any


_SKIP_FRESHNESS_ENV = "ARQ_SKIP_FRESHNESS"
_LOCAL_UI_HOST = "127.0.0.1"
_LOCAL_UI_START_TIMEOUT_SECONDS = 5.0


def _csvish(values: list[str] | None) -> tuple[str, ...]:
    cleaned: list[str] = []
    for value in values or []:
        for part in value.split(","):
            token = part.strip()
            if token:
                cleaned.append(token)
    return tuple(cleaned)


def _auth_config_from_args(args: argparse.Namespace):
    from arquimedes.mcp_auth import OIDCAuthConfig

    if not args.auth_issuer_url:
        return None
    if not args.resource_server_url:
        raise ValueError("--resource-server-url is required when auth is enabled")
    return OIDCAuthConfig(
        issuer_url=args.auth_issuer_url,
        resource_server_url=args.resource_server_url,
        required_scopes=_csvish(args.auth_required_scope),
        audience=_csvish(args.auth_audience),
        allowed_subjects=frozenset(_csvish(args.auth_allowed_subject)),
        allowed_emails=frozenset(value.lower() for value in _csvish(args.auth_allowed_email)),
        allowed_email_domains=frozenset(value.lower() for value in _csvish(args.auth_allowed_email_domain)),
        service_documentation_url=args.auth_service_documentation_url,
        jwks_url=args.auth_jwks_url,
    )


def _auth_config_from_mapping(mapping: dict[str, Any] | None):
    from arquimedes.mcp_auth import OIDCAuthConfig

    auth = mapping or {}
    issuer_url = auth.get("issuer_url")
    if not issuer_url:
        return None
    resource_server_url = auth.get("resource_server_url")
    if not resource_server_url:
        raise ValueError("mcp.auth.resource_server_url is required when mcp.auth.issuer_url is set")
    return OIDCAuthConfig(
        issuer_url=str(issuer_url),
        resource_server_url=str(resource_server_url),
        required_scopes=tuple(str(v) for v in auth.get("required_scopes") or []),
        audience=tuple(str(v) for v in auth.get("audience") or []),
        allowed_subjects=frozenset(str(v) for v in auth.get("allowed_subjects") or []),
        allowed_emails=frozenset(str(v).lower() for v in auth.get("allowed_emails") or []),
        allowed_email_domains=frozenset(str(v).lower() for v in auth.get("allowed_email_domains") or []),
        service_documentation_url=str(auth["service_documentation_url"]) if auth.get("service_documentation_url") else None,
        jwks_url=str(auth["jwks_url"]) if auth.get("jwks_url") else None,
    )


def _truthy(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _configure(config_path: str | None = None) -> None:
    if config_path:
        os.environ["ARQUIMEDES_CONFIG"] = config_path


def _ensure_fresh() -> None:
    if _truthy(os.environ.get(_SKIP_FRESHNESS_ENV)):
        return
    from arquimedes.freshness import update_workspace

    update_workspace()


def _effective_depth(*, deep: bool, depth: int | None) -> int:
    if depth is not None:
        return depth
    if deep:
        return 2
    return 1


def tool_refresh() -> dict[str, Any]:
    from arquimedes.freshness import update_workspace

    return update_workspace()


def tool_overview() -> dict[str, Any]:
    from arquimedes import read as read_mod

    _ensure_fresh()
    return read_mod.build_corpus_overview()


def tool_search(
    query: str,
    *,
    deep: bool = False,
    depth: int | None = None,
    facet: list[str] | None = None,
    collection: str | None = None,
    limit: int = 20,
    chunk_limit: int = 5,
    annotation_limit: int = 3,
    figure_limit: int = 3,
    concept_limit: int = 3,
) -> dict[str, Any]:
    from arquimedes.search import search as do_search

    _ensure_fresh()
    result = do_search(
        query,
        depth=_effective_depth(deep=deep, depth=depth),
        facets=list(facet or []),
        collection=collection,
        limit=limit,
        chunk_limit=chunk_limit,
        annotation_limit=annotation_limit,
        figure_limit=figure_limit,
        concept_limit=concept_limit,
    )
    return result.to_dict()


def tool_read(
    material_id: str,
    *,
    page: int | None = None,
    chunk_id: str | None = None,
    full: bool = False,
    detail: str | None = None,
) -> dict[str, Any]:
    from arquimedes import read as read_mod

    _ensure_fresh()
    selected = [
        name
        for name, value in (
            ("page", page is not None),
            ("chunk_id", bool(chunk_id)),
            ("full", full),
            ("detail", bool(detail)),
        )
        if value
    ]
    if len(selected) > 1:
        raise ValueError(f"Options are mutually exclusive: {', '.join(selected)}")

    if page is not None:
        return {"material_id": material_id, "page": read_mod.get_page(material_id, page)}
    if chunk_id:
        return {"material_id": material_id, "chunk": read_mod.get_chunk_by_id(material_id, chunk_id)}
    if full:
        path = read_mod.material_extracted_text_path(material_id)
        if not path:
            raise FileNotFoundError(f"text.md for material {material_id!r}")
        return {"material_id": material_id, "text": path.read_text(encoding="utf-8")}

    card = read_mod.build_material_card(material_id)
    if detail == "chunks":
        return {"card": card, "chunks": read_mod.list_chunks_compact(material_id)}
    if detail == "figures":
        return {"card": card, "figures": read_mod.list_figures_compact(material_id)}
    if detail == "annotations":
        return {"card": card, "annotations": read_mod.list_annotations(material_id)}
    if detail not in {None, "", "chunks", "figures", "annotations"}:
        raise ValueError("detail must be one of: chunks, figures, annotations")
    return {"card": card}


def tool_figures(
    material_id: str,
    *,
    visual_type: str | None = None,
    figure_id: str | None = None,
) -> dict[str, Any]:
    from arquimedes import read as read_mod

    _ensure_fresh()
    if figure_id:
        return {"material_id": material_id, "figure": read_mod.get_figure(material_id, figure_id)}
    return {"material_id": material_id, "figures": read_mod.list_figures_compact(material_id, visual_type=visual_type)}


def tool_annotations(
    material_id: str,
    *,
    page: int | None = None,
    kind: str | None = None,
) -> dict[str, Any]:
    from arquimedes import read as read_mod

    _ensure_fresh()
    return {"material_id": material_id, "annotations": read_mod.list_annotations(material_id, page=page, kind=kind)}


def tool_related(material_id: str, *, limit: int = 10) -> dict[str, Any]:
    from arquimedes.search import find_related

    _ensure_fresh()
    related = find_related(material_id, limit=limit)
    return {"material_id": material_id, "related": [row.to_dict() for row in related]}


def tool_material_clusters(material_id: str) -> dict[str, Any]:
    from arquimedes.search import get_material_clusters

    _ensure_fresh()
    hits = get_material_clusters(material_id)
    return {"material_id": material_id, "local_clusters": [row.to_dict() for row in hits]}


def tool_collection_clusters(domain: str, collection: str) -> dict[str, Any]:
    from arquimedes.search import get_collection_clusters

    _ensure_fresh()
    hits = get_collection_clusters(domain, collection)
    return {"domain": domain, "collection": collection, "local_clusters": [row.to_dict() for row in hits]}


def tool_concepts(
    *,
    min_materials: int = 1,
    limit: int = 100,
) -> list[dict[str, Any]]:
    from arquimedes.search import list_concepts

    _ensure_fresh()
    return [row.to_dict() for row in list_concepts(min_materials=min_materials, limit=limit)]


def tool_list_domains_and_collections() -> list[dict[str, Any]]:
    from arquimedes import read as read_mod

    _ensure_fresh()
    return read_mod.list_domains_and_collections()


def tool_list_wiki_dir(rel_path: str = "") -> dict[str, Any]:
    from arquimedes import read as read_mod

    _ensure_fresh()
    return read_mod.list_wiki_dir(rel_path)


def tool_wiki_page_record(rel_path: str) -> dict[str, Any]:
    from arquimedes import read as read_mod

    _ensure_fresh()
    path, body = read_mod.load_wiki_page(rel_path)
    record = read_mod.wiki_page_record(path) or {}
    payload: dict[str, Any] = {
        "path": path.relative_to(read_mod.get_project_root()).as_posix(),
        "record": record,
    }
    if record.get("page_type") == "concept" and record.get("page_id"):
        payload["materials"] = read_mod.materials_for_concept(str(record["page_id"]))
    elif record.get("page_type") == "collection":
        payload["materials"] = read_mod.materials_for_collection(
            str(record.get("domain") or ""),
            str(record.get("collection") or ""),
        )
    else:
        payload["materials"] = read_mod.materials_for_concept_page(path, body)
    return payload


def tool_recent_materials(limit: int = 10) -> list[dict[str, Any]]:
    from arquimedes import read as read_mod

    _ensure_fresh()
    return read_mod.recent_materials(limit=limit)


def tool_materials_for_collection(domain: str, collection: str) -> dict[str, Any]:
    from arquimedes import read as read_mod

    _ensure_fresh()
    return {
        "domain": domain,
        "collection": collection,
        "materials": read_mod.materials_for_collection(domain, collection),
    }


def tool_materials_for_concept(cluster_id: str) -> dict[str, Any]:
    from arquimedes import read as read_mod

    _ensure_fresh()
    return {
        "cluster_id": cluster_id,
        "materials": read_mod.materials_for_concept(cluster_id),
    }


def _local_ui_state_path(config: dict[str, Any] | None = None) -> Path:
    from arquimedes.config import get_local_cache_root

    cache_root = get_local_cache_root(config)
    cache_root.mkdir(parents=True, exist_ok=True)
    return cache_root / "serve_local_ui.json"


def _local_ui_log_path(config: dict[str, Any] | None = None) -> Path:
    from arquimedes.config import get_logs_root

    logs_root = get_logs_root(config)
    logs_root.mkdir(parents=True, exist_ok=True)
    return logs_root / "serve-local-ui.log"


def _read_local_ui_state(config: dict[str, Any] | None = None) -> dict[str, Any] | None:
    path = _local_ui_state_path(config)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    return data if isinstance(data, dict) else None


def _write_local_ui_state(payload: dict[str, Any], config: dict[str, Any] | None = None) -> None:
    path = _local_ui_state_path(config)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _pid_is_running(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _port_accepts_connections(host: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(0.2)
        try:
            return sock.connect_ex((host, port)) == 0
        except OSError:
            return False


def tool_serve_local_ui(*, port: int = 8420) -> dict[str, Any]:
    from arquimedes.config import load_config

    if port <= 0 or port > 65535:
        raise ValueError("port must be between 1 and 65535")

    _ensure_fresh()
    config = load_config()
    state = _read_local_ui_state(config)
    if state:
        existing_pid = int(state.get("pid") or 0)
        existing_port = int(state.get("port") or 0)
        if (
            existing_pid
            and existing_port == port
            and _pid_is_running(existing_pid)
            and _port_accepts_connections(_LOCAL_UI_HOST, port)
        ):
            return {
                "status": "already_running",
                "pid": existing_pid,
                "host": _LOCAL_UI_HOST,
                "port": port,
                "url": f"http://{_LOCAL_UI_HOST}:{port}",
                "log_path": str(_local_ui_log_path(config)),
                "state_path": str(_local_ui_state_path(config)),
            }

    if _port_accepts_connections(_LOCAL_UI_HOST, port):
        raise RuntimeError(
            f"Port {port} is already in use on {_LOCAL_UI_HOST}. "
            "Choose a different port or stop the existing local server."
        )

    log_path = _local_ui_log_path(config)
    env = os.environ.copy()
    log_handle = log_path.open("ab")
    try:
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "arquimedes.cli",
                "serve",
                "--host",
                _LOCAL_UI_HOST,
                "--port",
                str(port),
            ],
            stdin=subprocess.DEVNULL,
            stdout=log_handle,
            stderr=log_handle,
            start_new_session=True,
            env=env,
        )
    finally:
        log_handle.close()

    deadline = time.time() + _LOCAL_UI_START_TIMEOUT_SECONDS
    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(
                f"Local web UI failed to start. Check log: {log_path}"
            )
        if _port_accepts_connections(_LOCAL_UI_HOST, port):
            payload = {
                "status": "started",
                "pid": proc.pid,
                "host": _LOCAL_UI_HOST,
                "port": port,
                "url": f"http://{_LOCAL_UI_HOST}:{port}",
                "log_path": str(log_path),
                "state_path": str(_local_ui_state_path(config)),
            }
            _write_local_ui_state(payload, config)
            return payload
        time.sleep(0.1)

    raise RuntimeError(
        f"Timed out waiting for local web UI on {_LOCAL_UI_HOST}:{port}. "
        f"Check log: {log_path}"
    )


def build_server(
    config_path: str | None = None,
    *,
    host: str = "127.0.0.1",
    port: int = 8000,
    mount_path: str = "/",
    sse_path: str = "/sse",
    streamable_http_path: str = "/mcp",
    auth_config=None,
):
    """Build the FastMCP server lazily so tests don't require the SDK."""
    _configure(config_path)

    try:
        from mcp.server.fastmcp import FastMCP
        from mcp.server.auth.settings import AuthSettings
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised in real installs
        raise RuntimeError(
            "Missing MCP dependency: mcp. Install project dependencies for the current "
            "Python environment, e.g. `python3 -m pip install -e .`."
        ) from exc

    auth_settings = None
    token_verifier = None
    if auth_config is not None:
        from arquimedes.mcp_auth import OIDCTokenVerifier

        auth_settings = AuthSettings(
            issuer_url=auth_config.issuer_url,
            service_documentation_url=auth_config.service_documentation_url,
            required_scopes=list(auth_config.required_scopes) or None,
            resource_server_url=auth_config.resource_server_url,
        )
        token_verifier = OIDCTokenVerifier(auth_config)

    mcp = FastMCP(
        "Arquimedes",
        json_response=True,
        auth=auth_settings,
        token_verifier=token_verifier,
        host=host,
        port=port,
        mount_path=mount_path,
        sse_path=sse_path,
        streamable_http_path=streamable_http_path,
    )

    @mcp.tool()
    def refresh() -> dict[str, Any]:
        """Pull collaborator updates when applicable and ensure the local index is current."""
        return tool_refresh()

    @mcp.tool()
    def overview() -> dict[str, Any]:
        """Return a compact corpus-wide overview: counts, collections, and freshness stamps."""
        return tool_overview()

    @mcp.tool()
    def search(
        query: str,
        deep: bool = False,
        depth: int | None = None,
        facet: list[str] | None = None,
        collection: str | None = None,
        limit: int = 20,
        chunk_limit: int = 5,
        annotation_limit: int = 3,
        figure_limit: int = 3,
        concept_limit: int = 3,
    ) -> dict[str, Any]:
        """Search materials, passages, figures, annotations, and concepts."""
        return tool_search(
            query,
            deep=deep,
            depth=depth,
            facet=facet,
            collection=collection,
            limit=limit,
            chunk_limit=chunk_limit,
            annotation_limit=annotation_limit,
            figure_limit=figure_limit,
            concept_limit=concept_limit,
        )

    @mcp.tool(name="read")
    def read_tool(
        material_id: str,
        page: int | None = None,
        chunk_id: str | None = None,
        full: bool = False,
        detail: str | None = None,
    ) -> dict[str, Any]:
        """Read a material card, one page, one chunk, full text, or one compact detail listing."""
        return tool_read(material_id, page=page, chunk_id=chunk_id, full=full, detail=detail)

    @mcp.tool()
    def figures(
        material_id: str,
        visual_type: str | None = None,
        figure_id: str | None = None,
    ) -> dict[str, Any]:
        """List figures for a material, or return a single figure by id."""
        return tool_figures(material_id, visual_type=visual_type, figure_id=figure_id)

    @mcp.tool()
    def annotations(
        material_id: str,
        page: int | None = None,
        kind: str | None = None,
    ) -> dict[str, Any]:
        """List reader annotations for a material, optionally filtered by page or type."""
        return tool_annotations(material_id, page=page, kind=kind)

    @mcp.tool()
    def related(material_id: str, limit: int = 10) -> dict[str, Any]:
        """Find materials related to one material via shared concepts, authors, keywords, or facets."""
        return tool_related(material_id, limit=limit)

    @mcp.tool()
    def material_clusters(material_id: str) -> dict[str, Any]:
        """List collection-local concept clusters connected to one material."""
        return tool_material_clusters(material_id)

    @mcp.tool()
    def collection_clusters(domain: str, collection: str) -> dict[str, Any]:
        """List collection-local concept clusters for one domain/collection pair."""
        return tool_collection_clusters(domain, collection)

    @mcp.tool()
    def concepts(min_materials: int = 1, limit: int = 100) -> list[dict[str, Any]]:
        """List concept candidates across the corpus with material counts."""
        return tool_concepts(min_materials=min_materials, limit=limit)

    @mcp.tool()
    def list_domains_and_collections() -> list[dict[str, Any]]:
        """List all domain/collection pairs currently present in the corpus."""
        return tool_list_domains_and_collections()

    @mcp.tool()
    def list_wiki_dir(rel_path: str = "") -> dict[str, Any]:
        """List published wiki subdirectories and pages for one relative wiki path."""
        return tool_list_wiki_dir(rel_path)

    @mcp.tool()
    def wiki_page_record(rel_path: str) -> dict[str, Any]:
        """Resolve one published wiki page to its indexed record plus linked materials."""
        return tool_wiki_page_record(rel_path)

    @mcp.tool()
    def recent_materials(limit: int = 10) -> list[dict[str, Any]]:
        """List recently indexed materials with compact card metadata."""
        return tool_recent_materials(limit=limit)

    @mcp.tool()
    def materials_for_collection(domain: str, collection: str) -> dict[str, Any]:
        """List all materials in one domain/collection pair."""
        return tool_materials_for_collection(domain, collection)

    @mcp.tool()
    def materials_for_concept(cluster_id: str) -> dict[str, Any]:
        """List materials linked to one local concept cluster id."""
        return tool_materials_for_concept(cluster_id)

    @mcp.tool()
    def serve_local_ui(port: int = 8420) -> dict[str, Any]:
        """Start the local-only web UI in the background and return its URL and pid."""
        return tool_serve_local_ui(port=port)

    return mcp


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Arquimedes read-only MCP server")
    parser.add_argument("--config", help="Path to a vault config file.")
    parser.add_argument(
        "--transport",
        choices=["stdio", "sse", "streamable-http"],
        default="stdio",
        help="MCP transport to expose.",
    )
    parser.add_argument("--host", default="127.0.0.1", help="Bind host for remote transports.")
    parser.add_argument("--port", type=int, default=8000, help="Bind port for remote transports.")
    parser.add_argument("--mount-path", default="/", help="Mount path for SSE transport.")
    parser.add_argument("--sse-path", default="/sse", help="SSE endpoint path.")
    parser.add_argument("--streamable-http-path", default="/mcp", help="Streamable HTTP endpoint path.")
    parser.add_argument("--resource-server-url", help="Public HTTPS MCP URL for OAuth resource metadata.")
    parser.add_argument("--auth-issuer-url", help="OIDC issuer URL for remote OAuth.")
    parser.add_argument("--auth-jwks-url", help="Override JWKS URL if discovery is unavailable.")
    parser.add_argument(
        "--auth-required-scope",
        action="append",
        help="Required OAuth scope. Repeat or use comma-separated values.",
    )
    parser.add_argument(
        "--auth-audience",
        action="append",
        help="Accepted token audience. Repeat or use comma-separated values.",
    )
    parser.add_argument(
        "--auth-allowed-subject",
        action="append",
        help="Restrict access to one or more OIDC subject ids.",
    )
    parser.add_argument(
        "--auth-allowed-email",
        action="append",
        help="Restrict access to one or more email addresses.",
    )
    parser.add_argument(
        "--auth-allowed-email-domain",
        action="append",
        help="Restrict access to one or more email domains.",
    )
    parser.add_argument(
        "--auth-service-documentation-url",
        help="Optional public docs URL advertised in OAuth metadata.",
    )
    args = parser.parse_args(argv)
    auth_config = _auth_config_from_args(args)

    server = build_server(
        config_path=args.config,
        host=args.host,
        port=args.port,
        mount_path=args.mount_path,
        sse_path=args.sse_path,
        streamable_http_path=args.streamable_http_path,
        auth_config=auth_config,
    )
    server.run(transport=args.transport, mount_path=args.mount_path)


if __name__ == "__main__":
    main()
