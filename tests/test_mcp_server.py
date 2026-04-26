from __future__ import annotations

import asyncio
import json
from pathlib import Path
import types

import pytest
from click.testing import CliRunner

from arquimedes.cli import cli
from arquimedes import mcp_server


def test_effective_depth_prefers_explicit_depth():
    assert mcp_server._effective_depth(deep=False, depth=3) == 3
    assert mcp_server._effective_depth(deep=True, depth=1) == 1


def test_effective_depth_uses_deep_default():
    assert mcp_server._effective_depth(deep=True, depth=None) == 2
    assert mcp_server._effective_depth(deep=False, depth=None) == 1


def test_configure_sets_env(monkeypatch):
    monkeypatch.delenv("ARQUIMEDES_CONFIG", raising=False)

    mcp_server._configure("/tmp/vault/config.yaml")

    assert mcp_server.os.environ["ARQUIMEDES_CONFIG"] == "/tmp/vault/config.yaml"


def test_tool_read_rejects_mutually_exclusive_options(monkeypatch):
    calls: list[str] = []

    def _ensure():
        calls.append("fresh")

    monkeypatch.setattr(mcp_server, "_ensure_fresh", _ensure)

    with pytest.raises(ValueError, match="mutually exclusive"):
        mcp_server.tool_read("mat_001", page=1, full=True)

    assert calls == ["fresh"]


def test_tool_search_returns_search_payload(monkeypatch):
    calls: dict[str, object] = {}

    class FakeResult:
        def to_dict(self) -> dict[str, object]:
            return {"query": "archive", "depth": 2, "results": []}

    def _ensure():
        calls["fresh"] = True

    def _search(query: str, **kwargs):
        calls["query"] = query
        calls["kwargs"] = kwargs
        return FakeResult()

    monkeypatch.setattr(mcp_server, "_ensure_fresh", _ensure)
    monkeypatch.setattr("arquimedes.search.search", _search)

    payload = mcp_server.tool_search("archive", deep=True, limit=7)

    assert payload["query"] == "archive"
    assert calls["fresh"] is True
    assert calls["query"] == "archive"
    assert calls["kwargs"]["depth"] == 2
    assert calls["kwargs"]["limit"] == 7


def test_tool_read_full_returns_text(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(mcp_server, "_ensure_fresh", lambda: None)

    class FakeReadModule:
        @staticmethod
        def material_extracted_text_path(material_id: str):
            assert material_id == "mat_001"
            path = tmp_path / "text.md"
            path.write_text("full body", encoding="utf-8")
            return path

    monkeypatch.setattr("arquimedes.read.material_extracted_text_path", FakeReadModule.material_extracted_text_path)

    payload = mcp_server.tool_read("mat_001", full=True)

    assert payload == {"material_id": "mat_001", "text": "full body"}


def test_tool_serve_local_ui_rejects_invalid_port():
    with pytest.raises(ValueError, match="port must be between 1 and 65535"):
        mcp_server.tool_serve_local_ui(port=70000)


def test_tool_serve_local_ui_returns_existing_process(monkeypatch, tmp_path: Path):
    config = {"local_cache_root": str(tmp_path)}
    state = {
        "pid": 4321,
        "port": 8420,
        "url": "http://127.0.0.1:8420",
    }

    monkeypatch.setattr(mcp_server, "_ensure_fresh", lambda: None)
    monkeypatch.setattr("arquimedes.config.load_config", lambda: config)
    monkeypatch.setattr(mcp_server, "_read_local_ui_state", lambda cfg=None: state)
    monkeypatch.setattr(mcp_server, "_pid_is_running", lambda pid: pid == 4321)
    monkeypatch.setattr(mcp_server, "_port_accepts_connections", lambda host, port: host == "127.0.0.1" and port == 8420)

    payload = mcp_server.tool_serve_local_ui(port=8420)

    assert payload["status"] == "already_running"
    assert payload["pid"] == 4321
    assert payload["url"] == "http://127.0.0.1:8420"


def test_tool_serve_local_ui_starts_background_process(monkeypatch, tmp_path: Path):
    config = {"local_cache_root": str(tmp_path)}
    popen_calls: dict[str, object] = {}
    port_checks = iter([False, True])
    written_state: dict[str, object] = {}

    class FakeProcess:
        pid = 9876

        @staticmethod
        def poll():
            return None

    def _fake_popen(args, **kwargs):
        popen_calls["args"] = args
        popen_calls["kwargs"] = kwargs
        return FakeProcess()

    monkeypatch.setattr(mcp_server, "_ensure_fresh", lambda: None)
    monkeypatch.setattr("arquimedes.config.load_config", lambda: config)
    monkeypatch.setattr(mcp_server, "_read_local_ui_state", lambda cfg=None: None)
    monkeypatch.setattr(mcp_server, "_write_local_ui_state", lambda payload, cfg=None: written_state.update(payload))
    monkeypatch.setattr(mcp_server, "_port_accepts_connections", lambda host, port: next(port_checks))
    monkeypatch.setattr(mcp_server.subprocess, "Popen", _fake_popen)
    monkeypatch.setattr(mcp_server.time, "sleep", lambda _: None)

    payload = mcp_server.tool_serve_local_ui(port=8421)

    assert payload["status"] == "started"
    assert payload["pid"] == 9876
    assert payload["url"] == "http://127.0.0.1:8421"
    assert written_state["pid"] == 9876
    assert popen_calls["args"][:4] == [mcp_server.sys.executable, "-m", "arquimedes.cli", "serve"]


def test_tool_list_domains_and_collections(monkeypatch):
    monkeypatch.setattr(mcp_server, "_ensure_fresh", lambda: None)
    monkeypatch.setattr(
        "arquimedes.read.list_domains_and_collections",
        lambda: [{"domain": "research", "collection": "papers"}],
    )

    payload = mcp_server.tool_list_domains_and_collections()

    assert payload == [{"domain": "research", "collection": "papers"}]


def test_tool_list_wiki_dir(monkeypatch):
    monkeypatch.setattr(mcp_server, "_ensure_fresh", lambda: None)
    monkeypatch.setattr(
        "arquimedes.read.list_wiki_dir",
        lambda rel_path="": {"path": rel_path, "dirs": [], "pages": [{"name": "mat_001", "path": "research/papers/mat_001"}], "index_exists": True},
    )

    payload = mcp_server.tool_list_wiki_dir("research/papers")

    assert payload["path"] == "research/papers"
    assert payload["index_exists"] is True


def test_tool_wiki_page_record_for_collection(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(mcp_server, "_ensure_fresh", lambda: None)
    path = tmp_path / "wiki" / "research" / "papers" / "_index.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("# Papers", encoding="utf-8")

    monkeypatch.setattr("arquimedes.read.load_wiki_page", lambda rel_path: (path, "# Papers"))
    monkeypatch.setattr("arquimedes.read.get_project_root", lambda: tmp_path)
    monkeypatch.setattr(
        "arquimedes.read.wiki_page_record",
        lambda page_path: {"page_type": "collection", "domain": "research", "collection": "papers", "page_id": "research/papers"},
    )
    monkeypatch.setattr(
        "arquimedes.read.materials_for_collection",
        lambda domain, collection: [{"material_id": "mat_001", "title": "Archive"}],
    )

    payload = mcp_server.tool_wiki_page_record("research/papers")

    assert payload["path"] == "wiki/research/papers/_index.md"
    assert payload["record"]["page_type"] == "collection"
    assert payload["materials"][0]["material_id"] == "mat_001"


def test_tool_recent_materials(monkeypatch):
    monkeypatch.setattr(mcp_server, "_ensure_fresh", lambda: None)
    monkeypatch.setattr(
        "arquimedes.read.recent_materials",
        lambda limit=10: [{"material_id": "mat_001", "title": "Archive", "domain": "research", "collection": "papers"}],
    )

    payload = mcp_server.tool_recent_materials(limit=5)

    assert payload[0]["material_id"] == "mat_001"


def test_tool_materials_for_concept(monkeypatch):
    monkeypatch.setattr(mcp_server, "_ensure_fresh", lambda: None)
    monkeypatch.setattr(
        "arquimedes.read.materials_for_concept",
        lambda cluster_id: [{"material_id": "mat_001", "title": "Archive"}],
    )

    payload = mcp_server.tool_materials_for_concept("cluster-1")

    assert payload["cluster_id"] == "cluster-1"
    assert payload["materials"][0]["title"] == "Archive"


def test_wrap_http_logging_writes_one_line(monkeypatch, tmp_path: Path):
    lines: list[dict[str, object]] = []

    monkeypatch.setattr(mcp_server, "_append_mcp_http_log", lambda cfg, payload: lines.append(payload))

    async def app(scope, receive, send):
        await send({"type": "http.response.start", "status": 406, "headers": []})
        await send({"type": "http.response.body", "body": b"{}", "more_body": False})

    wrapped = mcp_server._wrap_http_logging(app)

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    sent: list[dict[str, object]] = []

    async def send(message):
        sent.append(message)

    asyncio.run(
        wrapped(
            {
                "type": "http",
                "method": "GET",
                "path": "/mcp",
                "query_string": b"",
                "headers": [
                    (b"host", b"mcp.example.com"),
                    (b"accept", b"text/event-stream"),
                    (b"user-agent", b"ChatGPT"),
                    (b"cf-access-jwt-assertion", b"token"),
                ],
            },
            receive,
            send,
        )
    )

    assert sent[0]["status"] == 406
    assert lines[0]["path"] == "/mcp"
    assert lines[0]["cf_access_jwt_present"] is True
    assert lines[0]["status"] == 406


def test_enable_http_logging_wraps_streamable_http_app():
    class FakeServer:
        def streamable_http_app(self):
            return "app"

    server = FakeServer()
    mcp_server._enable_http_logging(server)

    wrapped = server.streamable_http_app()
    assert callable(wrapped)


def test_build_server_passes_remote_settings(monkeypatch):
    captured: dict[str, object] = {}

    class FakeFastMCP:
        def __init__(self, *args, **kwargs):
            captured["args"] = args
            captured["kwargs"] = kwargs

        def tool(self, *args, **kwargs):
            def decorator(fn):
                return fn
            return decorator

    fake_module = types.ModuleType("mcp.server.fastmcp")
    fake_module.FastMCP = FakeFastMCP
    fake_auth_settings = types.ModuleType("mcp.server.auth.settings")

    class FakeAuthSettings:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    fake_auth_settings.AuthSettings = FakeAuthSettings

    monkeypatch.setitem(mcp_server.sys.modules, "mcp", types.ModuleType("mcp"))
    monkeypatch.setitem(mcp_server.sys.modules, "mcp.server", types.ModuleType("mcp.server"))
    monkeypatch.setitem(mcp_server.sys.modules, "mcp.server.fastmcp", fake_module)
    monkeypatch.setitem(mcp_server.sys.modules, "mcp.server.auth", types.ModuleType("mcp.server.auth"))
    monkeypatch.setitem(mcp_server.sys.modules, "mcp.server.auth.settings", fake_auth_settings)

    server = mcp_server.build_server(
        config_path="/tmp/vault/config.yaml",
        host="0.0.0.0",
        port=9000,
        mount_path="/",
        sse_path="/events",
        streamable_http_path="/mcp-test",
    )

    assert isinstance(server, FakeFastMCP)
    assert captured["kwargs"]["host"] == "0.0.0.0"
    assert captured["kwargs"]["port"] == 9000
    assert captured["kwargs"]["sse_path"] == "/events"
    assert captured["kwargs"]["streamable_http_path"] == "/mcp-test"
    assert "Start with overview" in captured["kwargs"]["instructions"]
    assert captured["kwargs"]["auth"] is None
    assert captured["kwargs"]["token_verifier"] is None


def test_auth_config_from_args_parses_restrictions():
    args = types.SimpleNamespace(
        auth_issuer_url="https://auth.example.com",
        resource_server_url="https://mcp.example.com/mcp",
        auth_required_scope=["arq.read,offline_access"],
        auth_audience=["https://mcp.example.com/mcp"],
        auth_allowed_subject=["sub-1"],
        auth_allowed_email=["owner@example.com"],
        auth_allowed_email_domain=["example.com"],
        auth_service_documentation_url="https://docs.example.com/arq-mcp",
        auth_jwks_url="https://auth.example.com/jwks.json",
    )

    auth_config = mcp_server._auth_config_from_args(args)

    assert auth_config is not None
    assert auth_config.required_scopes == ("arq.read", "offline_access")
    assert auth_config.allowed_emails == frozenset({"owner@example.com"})
    assert auth_config.allowed_email_domains == frozenset({"example.com"})


def test_build_server_passes_auth_settings(monkeypatch):
    captured: dict[str, object] = {}

    class FakeFastMCP:
        def __init__(self, *args, **kwargs):
            captured["kwargs"] = kwargs

        def tool(self, *args, **kwargs):
            def decorator(fn):
                return fn
            return decorator

    class FakeAuthSettings:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    class FakeVerifier:
        def __init__(self, config):
            captured["auth_config"] = config

    fake_fastmcp = types.ModuleType("mcp.server.fastmcp")
    fake_fastmcp.FastMCP = FakeFastMCP
    fake_auth_settings = types.ModuleType("mcp.server.auth.settings")
    fake_auth_settings.AuthSettings = FakeAuthSettings

    monkeypatch.setitem(mcp_server.sys.modules, "mcp", types.ModuleType("mcp"))
    monkeypatch.setitem(mcp_server.sys.modules, "mcp.server", types.ModuleType("mcp.server"))
    monkeypatch.setitem(mcp_server.sys.modules, "mcp.server.fastmcp", fake_fastmcp)
    monkeypatch.setitem(mcp_server.sys.modules, "mcp.server.auth", types.ModuleType("mcp.server.auth"))
    monkeypatch.setitem(mcp_server.sys.modules, "mcp.server.auth.settings", fake_auth_settings)
    monkeypatch.setattr("arquimedes.mcp_auth.OIDCTokenVerifier", FakeVerifier)

    auth_config = types.SimpleNamespace(
        issuer_url="https://auth.example.com",
        service_documentation_url="https://docs.example.com/arq-mcp",
        required_scopes=("arq.read",),
        resource_server_url="https://mcp.example.com/mcp",
    )
    mcp_server.build_server(auth_config=auth_config)

    assert captured["kwargs"]["auth"].kwargs["issuer_url"] == "https://auth.example.com"
    assert captured["kwargs"]["token_verifier"].__class__ is FakeVerifier


def test_auth_config_from_mapping_parses_yaml_shape():
    auth_config = mcp_server._auth_config_from_mapping(
        {
            "issuer_url": "https://auth.example.com",
            "resource_server_url": "https://mcp.example.com/mcp",
            "required_scopes": ["arq.read"],
            "allowed_emails": ["owner@example.com"],
            "allowed_email_domains": ["example.com"],
        }
    )

    assert auth_config is not None
    assert auth_config.required_scopes == ("arq.read",)
    assert auth_config.allowed_emails == frozenset({"owner@example.com"})


def test_mcp_cli_install_uses_config_profile(monkeypatch):
    calls: dict[str, object] = {}

    monkeypatch.setattr(
        "arquimedes.config.load_config",
        lambda config_path=None: {
            "mcp": {"keep_alive": True},
        },
    )
    monkeypatch.setattr("arquimedes.config.get_project_root", lambda: Path("/repo"))
    monkeypatch.setattr(
        "arquimedes.launchd.render_plist",
        lambda label, program_arguments, **kwargs: calls.setdefault(
            "render",
            {
                "label": label,
                "program_arguments": program_arguments,
                "kwargs": kwargs,
            },
        ) or "<plist />",
    )
    monkeypatch.setattr(
        "arquimedes.launchd.install",
        lambda label, plist_text: {"label": label, "plist": plist_text},
    )

    result = CliRunner().invoke(cli, ["mcp", "--install"])

    assert result.exit_code == 0
    assert calls["render"]["label"] == "com.arquimedes.mcp"
    assert calls["render"]["program_arguments"][-1] == "mcp"
    assert calls["render"]["kwargs"]["keep_alive"] is True


def test_mcp_cli_runs_server_from_config(monkeypatch):
    calls: dict[str, object] = {}

    class FakeServer:
        def run(self, transport="stdio", mount_path=None):
            calls["transport"] = transport
            calls["mount_path"] = mount_path

    monkeypatch.setattr(
        "arquimedes.config.load_config",
        lambda config_path=None: {
            "mcp": {
                "transport": "streamable-http",
                "mount_path": "/",
            }
        },
    )
    monkeypatch.setattr(
        "arquimedes.cli._mcp_server_from_config",
        lambda config_path=None: (FakeServer(), "streamable-http", "/"),
    )

    result = CliRunner().invoke(cli, ["mcp"])

    assert result.exit_code == 0
    assert calls["transport"] == "streamable-http"


def test_mcp_cloudflare_tunnel_config_parses_enabled_block():
    from arquimedes import cli as cli_mod

    payload = cli_mod._mcp_cloudflare_tunnel_config(
        {
            "mcp": {
                "cloudflare_tunnel": {
                    "enabled": True,
                    "tunnel_name": "arquimedes-personal",
                }
            }
        }
    )

    assert payload is not None
    assert payload["label"] == "com.arquimedes.cloudflared-tunnel"
    assert payload["tunnel_name"] == "arquimedes-personal"


def test_mcp_cli_install_also_installs_cloudflare_tunnel(monkeypatch):
    real_exists = Path.exists

    monkeypatch.setattr(
        "arquimedes.config.load_config",
        lambda config_path=None: {
            "mcp": {
                "keep_alive": True,
                "cloudflare_tunnel": {
                    "enabled": True,
                    "tunnel_name": "arquimedes-personal",
                    "binary_path": "/opt/homebrew/bin/cloudflared",
                },
            }
        },
    )
    monkeypatch.setattr("arquimedes.config.get_project_root", lambda: Path("/repo"))
    monkeypatch.setattr("arquimedes.launchd.render_plist", lambda *args, **kwargs: "<plist />")
    monkeypatch.setattr(
        "arquimedes.launchd.install",
        lambda label, plist_text: {"label": label, "plist": plist_text},
    )
    monkeypatch.setattr(
        Path,
        "exists",
        lambda self: True if str(self) == "/opt/homebrew/bin/cloudflared" else real_exists(self),
    )

    result = CliRunner().invoke(cli, ["mcp", "--install"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["mcp"]["label"] == "com.arquimedes.mcp"
    assert payload["cloudflare_tunnel"]["label"] == "com.arquimedes.cloudflared-tunnel"


def test_mcp_cli_status_includes_cloudflare_tunnel(monkeypatch):
    monkeypatch.setattr(
        "arquimedes.config.load_config",
        lambda config_path=None: {
            "mcp": {
                "cloudflare_tunnel": {
                    "enabled": True,
                    "tunnel_name": "arquimedes-personal",
                }
            }
        },
    )
    monkeypatch.setattr(
        "arquimedes.launchd.status",
        lambda label: {"label": label, "loaded": label == "com.arquimedes.cloudflared-tunnel"},
    )

    result = CliRunner().invoke(cli, ["mcp", "--status"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["mcp"]["label"] == "com.arquimedes.mcp"
    assert payload["cloudflare_tunnel"]["label"] == "com.arquimedes.cloudflared-tunnel"


def test_main_uses_requested_transport(monkeypatch):
    calls: dict[str, object] = {}

    class FakeServer:
        def run(self, transport="stdio", mount_path=None):
            calls["transport"] = transport
            calls["mount_path"] = mount_path

    def _build_server(**kwargs):
        calls["kwargs"] = kwargs
        return FakeServer()

    monkeypatch.setattr(mcp_server, "build_server", _build_server)

    mcp_server.main([
        "--config", "/tmp/vault/config.yaml",
        "--transport", "streamable-http",
        "--host", "0.0.0.0",
        "--port", "9000",
        "--streamable-http-path", "/mcp-test",
    ])

    assert calls["kwargs"]["config_path"] == "/tmp/vault/config.yaml"
    assert calls["kwargs"]["host"] == "0.0.0.0"
    assert calls["kwargs"]["port"] == 9000
    assert calls["kwargs"]["streamable_http_path"] == "/mcp-test"
    assert calls["transport"] == "streamable-http"


def test_main_forwards_auth_args(monkeypatch):
    calls: dict[str, object] = {}

    class FakeServer:
        def run(self, transport="stdio", mount_path=None):
            calls["transport"] = transport

    def _build_server(**kwargs):
        calls["kwargs"] = kwargs
        return FakeServer()

    monkeypatch.setattr(mcp_server, "build_server", _build_server)

    mcp_server.main([
        "--transport", "streamable-http",
        "--resource-server-url", "https://mcp.example.com/mcp",
        "--auth-issuer-url", "https://auth.example.com",
        "--auth-required-scope", "arq.read",
        "--auth-allowed-email-domain", "example.com",
    ])

    auth_config = calls["kwargs"]["auth_config"]
    assert auth_config.issuer_url == "https://auth.example.com"
    assert auth_config.required_scopes == ("arq.read",)


def test_build_transport_security_returns_none_when_unconfigured():
    assert mcp_server.build_transport_security() is None


def test_build_transport_security_extends_allowlist():
    settings = mcp_server.build_transport_security(
        allowed_hosts=["mcp.example.com"],
        allowed_origins=["https://mcp.example.com", "https://chatgpt.com"],
    )

    assert settings is not None
    assert settings.enable_dns_rebinding_protection is True
    assert "mcp.example.com" in settings.allowed_hosts
    assert "127.0.0.1:*" in settings.allowed_hosts
    assert "https://mcp.example.com" in settings.allowed_origins
    assert "http://127.0.0.1:*" in settings.allowed_origins


def test_build_transport_security_can_disable_protection():
    settings = mcp_server.build_transport_security(disable_dns_rebinding_protection=True)

    assert settings is not None
    assert settings.enable_dns_rebinding_protection is False


def test_main_forwards_transport_security_args(monkeypatch):
    calls: dict[str, object] = {}

    class FakeServer:
        def run(self, transport="stdio", mount_path=None):
            calls["transport"] = transport

    def _build_server(**kwargs):
        calls["kwargs"] = kwargs
        return FakeServer()

    monkeypatch.setattr(mcp_server, "build_server", _build_server)

    mcp_server.main([
        "--transport", "streamable-http",
        "--allowed-host", "mcp.example.com",
        "--allowed-origin", "https://mcp.example.com,https://chatgpt.com",
    ])

    transport_security = calls["kwargs"]["transport_security"]
    assert transport_security is not None
    assert "mcp.example.com" in transport_security.allowed_hosts
    assert "https://chatgpt.com" in transport_security.allowed_origins


def test_mcp_server_from_config_passes_transport_security(monkeypatch):
    from arquimedes import cli as cli_module

    captured: dict[str, object] = {}

    def _load_config(_path):
        return {
            "mcp": {
                "transport": "streamable-http",
                "host": "127.0.0.1",
                "port": 8000,
                "allowed_hosts": ["mcp.example.com"],
                "allowed_origins": ["https://mcp.example.com"],
            }
        }

    def _build_server(**kwargs):
        captured["kwargs"] = kwargs
        return object()

    monkeypatch.setattr("arquimedes.config.load_config", _load_config)
    monkeypatch.setattr(mcp_server, "build_server", _build_server)

    server, transport, mount_path = cli_module._mcp_server_from_config("/tmp/vault.yaml")

    assert transport == "streamable-http"
    assert mount_path == "/"
    transport_security = captured["kwargs"]["transport_security"]
    assert transport_security is not None
    assert "mcp.example.com" in transport_security.allowed_hosts
    assert "https://mcp.example.com" in transport_security.allowed_origins
