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


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(r) for r in rows), encoding="utf-8")


def _populated_material(repo: Path, material_id: str = "mat_001") -> str:
    material_dir = repo / "extracted" / material_id
    meta = dict(_material_meta(material_id), title="Sample", authors=["Ada"], year="2024", summary="A paper.")
    _write_json(material_dir / "meta.json", meta)
    _write_jsonl(material_dir / "pages.jsonl", [
        {"page_number": 1, "text": "Page one text"},
        {"page_number": 2, "text": "Page two text"},
    ])
    _write_jsonl(material_dir / "chunks.jsonl", [
        {"chunk_id": "chk_00001", "text": "First chunk body.", "source_pages": [1], "emphasized": True,
         "summary": {"value": "first chunk summary"}, "content_class": "argument"},
        {"chunk_id": "chk_00002", "text": "Second chunk body.", "source_pages": [2], "emphasized": False,
         "summary": {"value": "second chunk summary"}, "keywords": {"value": ["kw1", "kw2"]}},
    ])
    _write_jsonl(material_dir / "annotations.jsonl", [
        {"annotation_id": "ann_0001", "type": "highlight", "page": 1, "quoted_text": "hi"},
        {"annotation_id": "ann_0002", "type": "note", "page": 2, "comment": "check this"},
    ])
    figures_dir = material_dir / "figures"
    _write_json(figures_dir / "fig_001.json", {
        "figure_id": "fig_001", "source_page": 1,
        "image_path": "figures/fig_001.png",
        "visual_type": {"value": "photograph"},
        "caption": {"value": "A photo"},
        "relevance": "substantive",
    })
    _write_json(figures_dir / "fig_002.json", {
        "figure_id": "fig_002", "source_page": 2,
        "image_path": "figures/fig_002.png",
        "visual_type": {"value": "diagram"},
        "caption": {"value": "A diagram"},
        "description": {"value": "Diagram description"},
    })
    return material_id


def test_build_material_card(repo):
    mid = _populated_material(repo)
    card = read_mod.build_material_card(mid)
    assert card["material_id"] == mid
    assert card["title"] == "Sample"
    assert card["wiki_path"] == "wiki/research/papers/mat_001.md"
    assert card["counts"] == {"pages": 2, "chunks": 2, "annotations": 2, "figures": 2}


def test_list_chunks_compact_and_get_chunk_by_id(repo):
    mid = _populated_material(repo)
    listing = read_mod.list_chunks_compact(mid)
    assert [r["chunk_id"] for r in listing] == ["chk_00001", "chk_00002"]
    assert listing[0]["emphasized"] is True
    assert listing[0]["summary"] == "first chunk summary"
    chunk = read_mod.get_chunk_by_id(mid, "chk_00002")
    assert chunk["text"] == "Second chunk body."
    assert chunk["summary"] == "second chunk summary"
    assert chunk["keywords"] == ["kw1", "kw2"]
    with pytest.raises(FileNotFoundError):
        read_mod.get_chunk_by_id(mid, "chk_00099")


def test_get_page_returns_record(repo):
    mid = _populated_material(repo)
    page = read_mod.get_page(mid, 2)
    assert page["text"] == "Page two text"
    with pytest.raises(FileNotFoundError):
        read_mod.get_page(mid, 99)


def test_list_figures_compact_filters_by_visual_type(repo):
    mid = _populated_material(repo)
    all_figs = read_mod.list_figures_compact(mid)
    assert {f["figure_id"] for f in all_figs} == {"fig_001", "fig_002"}
    diagrams = read_mod.list_figures_compact(mid, visual_type="diagram")
    assert [f["figure_id"] for f in diagrams] == ["fig_002"]
    assert diagrams[0]["caption"] == "A diagram"


def test_get_figure(repo):
    mid = _populated_material(repo)
    fig = read_mod.get_figure(mid, "fig_001")
    assert fig["source_page"] == 1
    assert fig["visual_type"] == "photograph"
    assert fig["caption"] == "A photo"
    fig2 = read_mod.get_figure(mid, "fig_002")
    assert fig2["visual_type"] == "diagram"
    assert fig2["caption"] == "A diagram"
    with pytest.raises(FileNotFoundError):
        read_mod.get_figure(mid, "fig_999")


def test_list_annotations_filters(repo):
    mid = _populated_material(repo)
    all_anns = read_mod.list_annotations(mid)
    assert [a["annotation_id"] for a in all_anns] == ["ann_0001", "ann_0002"]
    page_two = read_mod.list_annotations(mid, page=2)
    assert [a["annotation_id"] for a in page_two] == ["ann_0002"]
    highlights = read_mod.list_annotations(mid, kind="highlight")
    assert [a["annotation_id"] for a in highlights] == ["ann_0001"]


def test_build_corpus_overview(repo):
    _populated_material(repo, "mat_001")
    _populated_material(repo, "mat_002")
    con = sqlite3.connect(str(repo / "indexes" / "search.sqlite"))
    con.executescript("""
        CREATE TABLE materials (material_id TEXT, domain TEXT, collection TEXT);
        CREATE TABLE chunks (chunk_id TEXT);
        CREATE TABLE figures (figure_id TEXT);
        CREATE TABLE annotations (annotation_id TEXT);
        CREATE TABLE wiki_pages (path TEXT);
    """)
    con.execute("INSERT INTO materials VALUES ('mat_001', 'research', 'papers')")
    con.execute("INSERT INTO materials VALUES ('mat_002', 'practice', '_general')")
    con.execute("INSERT INTO chunks VALUES ('c1')")
    con.execute("INSERT INTO figures VALUES ('f1')")
    con.execute("INSERT INTO annotations VALUES ('a1')")
    con.execute("INSERT INTO wiki_pages VALUES ('wiki/x.md')")
    con.commit()
    con.close()
    (repo / "derived").mkdir()
    (repo / "derived" / "compile_stamp.json").write_text(json.dumps({"when": "2026-04-16"}), encoding="utf-8")

    overview = read_mod.build_corpus_overview()
    assert overview["counts"]["materials"] == 2
    assert overview["index_exists"] is True
    assert {c["domain"] for c in overview["collections"]} == {"research", "practice"}
    assert overview["stamps"]["compile"] == {"when": "2026-04-16"}
    assert overview["stamps"]["memory_bridge"] is None
