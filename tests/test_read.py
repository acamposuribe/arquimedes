from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

import arquimedes.read as read_mod


@pytest.fixture
def repo(tmp_path, monkeypatch):
    library = tmp_path / "Library"
    library.mkdir()
    (tmp_path / "wiki").mkdir()
    (tmp_path / "extracted").mkdir()
    (tmp_path / "indexes").mkdir()
    monkeypatch.setattr(read_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(read_mod, "get_library_root", lambda: library)
    monkeypatch.setattr(read_mod, "get_index_path", lambda: tmp_path / "indexes" / "search.sqlite")
    return tmp_path


def _write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")


def _material_meta(material_id: str, **overrides) -> dict:
    return {
        "material_id": material_id,
        "domain": overrides.get("domain", "research"),
        "collection": overrides.get("collection", "papers"),
        "source_path": overrides.get("source_path", f"Research/{material_id}.pdf"),
    }


def test_load_wiki_page_rejects_traversal(repo):
    with pytest.raises(FileNotFoundError):
        read_mod.load_wiki_page("../secret")


def test_material_wiki_resolution(repo):
    material_id = "mat_001"
    _write_json(repo / "extracted" / material_id / "meta.json", _material_meta(material_id))
    path = read_mod.material_wiki_path(material_id)
    assert path == repo / "wiki" / "research" / "papers" / "mat_001.md"


def test_load_material_figures_skips_malformed(repo):
    material_id = "mat_001"
    figures_dir = repo / "extracted" / material_id / "figures"
    figures_dir.mkdir(parents=True)
    (repo / "extracted" / material_id / "meta.json").write_text(json.dumps(_material_meta(material_id)), encoding="utf-8")
    (figures_dir / "fig_0001.json").write_text(json.dumps({"figure_id": "fig_0001"}), encoding="utf-8")
    (figures_dir / "fig_0002.json").write_text("{bad", encoding="utf-8")
    assert read_mod.load_material_figures(material_id) == [{"figure_id": "fig_0001"}]


def test_list_wiki_dir_structure(repo):
    (repo / "wiki" / "research" / "papers").mkdir(parents=True)
    (repo / "wiki" / "research" / "papers" / "_index.md").write_text("Index", encoding="utf-8")
    (repo / "wiki" / "research" / "papers" / "mat_001.md").write_text("Page", encoding="utf-8")
    (repo / "wiki" / "research" / "papers" / "concepts").mkdir()
    listing = read_mod.list_wiki_dir("research/papers")
    assert listing == {
        "path": "research/papers",
        "dirs": [{"name": "concepts", "path": "research/papers/concepts"}],
        "pages": [{"name": "mat_001", "path": "research/papers/mat_001"}],
        "index_exists": True,
    }


def test_recent_materials_returns_rows(repo):
    con = sqlite3.connect(str(repo / "indexes" / "search.sqlite"))
    con.execute("CREATE TABLE materials (material_id TEXT, title TEXT, summary TEXT, domain TEXT, collection TEXT, document_type TEXT, year TEXT)")
    con.execute("INSERT INTO materials VALUES (?,?,?,?,?,?,?)", ("one", "One", "First", "research", "papers", "paper", "2024"))
    con.execute("INSERT INTO materials VALUES (?,?,?,?,?,?,?)", ("two", "Two", "Second", "practice", "_general", "book", "2023"))
    con.commit()
    con.close()
    assert [row["material_id"] for row in read_mod.recent_materials()] == ["two", "one"]
