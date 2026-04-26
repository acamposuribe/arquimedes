from __future__ import annotations

from pathlib import Path

import pytest

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
