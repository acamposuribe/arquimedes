from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from fastapi.testclient import TestClient

import arquimedes.read as read_mod
import arquimedes.serve as serve_mod


def _write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")


def _repo(tmp_path, monkeypatch):
    library = tmp_path / "Library" / "Research"
    library.mkdir(parents=True)
    (tmp_path / "wiki" / "research" / "papers").mkdir(parents=True)
    (tmp_path / "extracted" / "mat_001" / "figures").mkdir(parents=True)
    (tmp_path / "indexes").mkdir()
    monkeypatch.setattr(read_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(read_mod, "get_library_root", lambda: tmp_path / "Library")
    monkeypatch.setattr(read_mod, "get_index_path", lambda: tmp_path / "indexes" / "search.sqlite")
    monkeypatch.setattr(serve_mod.read_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(serve_mod.read_mod, "get_library_root", lambda: tmp_path / "Library")
    monkeypatch.setattr(serve_mod.read_mod, "get_index_path", lambda: tmp_path / "indexes" / "search.sqlite")
    monkeypatch.setattr(serve_mod, "get_index_path", lambda: tmp_path / "indexes" / "search.sqlite")
    return tmp_path


def test_health_endpoint(tmp_path, monkeypatch):
    _repo(tmp_path, monkeypatch)
    client = TestClient(serve_mod.create_app())
    assert client.get("/health").json() == {"ok": True}


def test_home_handles_missing_index(tmp_path, monkeypatch):
    _repo(tmp_path, monkeypatch)
    client = TestClient(serve_mod.create_app())
    response = client.get("/")
    assert response.status_code == 200
    assert "arq index rebuild" in response.text


def test_material_route_rewrites_links(tmp_path, monkeypatch):
    root = _repo(tmp_path, monkeypatch)
    _write_json(root / "extracted" / "mat_001" / "meta.json", {
        "material_id": "mat_001",
        "title": "Material One",
        "domain": "research",
        "collection": "papers",
        "source_path": "Research/mat_001.pdf",
    })
    (root / "Library" / "Research" / "mat_001.pdf").write_text("pdf", encoding="utf-8")
    (root / "extracted" / "mat_001" / "text.md").write_text("Extracted", encoding="utf-8")
    (root / "extracted" / "mat_001" / "figures" / "fig_0001.png").write_bytes(b"png")
    _write_json(root / "extracted" / "mat_001" / "figures" / "fig_0001.json", {"figure_id": "fig_0001", "image_path": "figures/fig_0001.png"})
    (root / "wiki" / "research" / "papers" / "mat_001.md").write_text(
        "[Concept](../../../wiki/shared/concepts/archive.md)\n\n"
        "[Source](file:///tmp/source.pdf)\n\n"
        "[Text](../../../extracted/mat_001/text.md)\n\n"
        "![Fig](figures/fig_0001.png)\n",
        encoding="utf-8",
    )
    client = TestClient(serve_mod.create_app())
    response = client.get("/materials/mat_001")
    assert response.status_code == 200
    assert '/wiki/shared/concepts/archive' in response.text
    assert '/source/mat_001' in response.text
    assert '/extracted/mat_001/text' in response.text
    assert '/figures/mat_001/fig_0001.png' in response.text


def test_wiki_directory_listing_route(tmp_path, monkeypatch):
    root = _repo(tmp_path, monkeypatch)
    (root / "wiki" / "research" / "papers" / "concepts").mkdir()
    (root / "wiki" / "research" / "papers" / "mat_001.md").write_text("Page", encoding="utf-8")
    client = TestClient(serve_mod.create_app())
    response = client.get("/wiki/research/papers")
    assert response.status_code == 200
    assert "Directories" in response.text
    assert "/wiki/research/papers/concepts" in response.text


def test_wiki_material_page_rewrites_figure_links(tmp_path, monkeypatch):
    root = _repo(tmp_path, monkeypatch)
    _write_json(root / "extracted" / "mat_001" / "meta.json", {
        "material_id": "mat_001",
        "title": "Material One",
        "domain": "research",
        "collection": "papers",
        "source_path": "Research/mat_001.pdf",
    })
    (root / "wiki" / "research" / "papers" / "mat_001.md").write_text("![Fig](figures/fig_0001.png)\n", encoding="utf-8")
    (root / "extracted" / "mat_001" / "figures" / "fig_0001.png").write_bytes(b"png")
    client = TestClient(serve_mod.create_app())
    response = client.get("/wiki/research/papers/mat_001")
    assert response.status_code == 200
    assert '/figures/mat_001/fig_0001.png' in response.text


def test_figure_and_source_routes(tmp_path, monkeypatch):
    root = _repo(tmp_path, monkeypatch)
    _write_json(root / "extracted" / "mat_001" / "meta.json", {
        "material_id": "mat_001",
        "title": "Material One",
        "domain": "research",
        "collection": "papers",
        "source_path": "Research/mat_001.pdf",
    })
    (root / "Library" / "Research" / "mat_001.pdf").write_text("pdf", encoding="utf-8")
    (root / "extracted" / "mat_001" / "figures" / "fig_0001.png").write_bytes(b"png")
    client = TestClient(serve_mod.create_app())
    assert client.get("/source/mat_001").status_code == 200
    assert client.get("/figures/mat_001/fig_0001.png").status_code == 200


def test_search_route_renders_results(tmp_path, monkeypatch):
    root = _repo(tmp_path, monkeypatch)
    con = sqlite3.connect(str(root / "indexes" / "search.sqlite"))
    con.execute("CREATE TABLE materials (material_id TEXT, title TEXT, summary TEXT, domain TEXT, collection TEXT, document_type TEXT, year TEXT)")
    con.execute("INSERT INTO materials VALUES (?,?,?,?,?,?,?)", ("mat_001", "Material One", "Summary", "research", "papers", "paper", "2024"))
    con.commit()
    con.close()

    class Result:
        query = "material"
        total = 1
        canonical_clusters = []
        results = [type("Card", (), {"material_id": "mat_001", "title": "Material One", "summary": "Summary", "domain": "research", "collection": "papers", "document_type": "paper", "year": "2024"})()]

    monkeypatch.setattr(serve_mod.search_mod, "search", lambda *args, **kwargs: Result())
    client = TestClient(serve_mod.create_app())
    response = client.get("/search?q=material")
    assert response.status_code == 200
    assert "Material One" in response.text


def test_update_and_freshness_endpoints(tmp_path, monkeypatch):
    _repo(tmp_path, monkeypatch)
    monkeypatch.setattr(serve_mod.freshness_mod, "workspace_freshness_status", lambda: {"message": "Up to date"})
    monkeypatch.setattr(serve_mod.freshness_mod, "update_workspace", lambda: {"message": "Updated", "pull_result": "ok"})
    client = TestClient(serve_mod.create_app())
    assert client.get("/api/freshness").json()["message"] == "Up to date"
    assert client.post("/update").json()["message"] == "Updated"
