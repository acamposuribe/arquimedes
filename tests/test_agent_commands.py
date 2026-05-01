"""End-to-end tests for Phase 7 agent-facing CLI commands."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest
from click.testing import CliRunner

import arquimedes.cli as cli_mod
import arquimedes.read as read_mod
from arquimedes.cli import cli


@pytest.fixture
def repo(tmp_path, monkeypatch):
    library = tmp_path / "Library"
    library.mkdir()
    (tmp_path / "wiki").mkdir()
    (tmp_path / "extracted").mkdir()
    (tmp_path / "indexes").mkdir()
    (tmp_path / "derived").mkdir()
    monkeypatch.setenv("ARQ_SKIP_FRESHNESS", "1")
    monkeypatch.setattr(read_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(read_mod, "get_library_root", lambda: library)
    monkeypatch.setattr(read_mod, "get_index_path", lambda: tmp_path / "indexes" / "search.sqlite")
    return tmp_path


def _write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(r) for r in rows), encoding="utf-8")


def _seed_material(repo: Path, material_id: str = "mat_001") -> str:
    material_dir = repo / "extracted" / material_id
    meta = {
        "material_id": material_id,
        "title": "Sample",
        "authors": ["Ada"],
        "year": "2024",
        "summary": "A paper.",
        "domain": "research",
        "collection": "papers",
        "document_type": "paper",
        "source_path": f"Research/{material_id}.pdf",
    }
    _write_json(material_dir / "meta.json", meta)
    _write_jsonl(material_dir / "pages.jsonl", [
        {"page_number": 1, "text": "Page one."},
        {"page_number": 2, "text": "Page two."},
    ])
    _write_jsonl(material_dir / "chunks.jsonl", [
        {"chunk_id": "chk_00001", "text": "Chunk one body.", "source_pages": [1], "emphasized": True,
         "summary": {"value": "first chunk summary"}},
        {"chunk_id": "chk_00002", "text": "Chunk two body.", "source_pages": [2], "emphasized": False},
    ])
    _write_jsonl(material_dir / "annotations.jsonl", [
        {"annotation_id": "ann_0001", "type": "highlight", "page": 1, "quoted_text": "hi"},
        {"annotation_id": "ann_0002", "type": "note", "page": 2, "comment": "check"},
    ])
    _write_json(material_dir / "figures" / "fig_001.json", {
        "figure_id": "fig_001", "source_page": 1, "image_path": "figures/fig_001.png",
        "visual_type": {"value": "diagram"}, "caption": {"value": "A diagram"},
    })
    (material_dir / "text.md").write_text("# Full text\n\nBody.", encoding="utf-8")
    return material_id


def test_read_card_json_default(repo):
    mid = _seed_material(repo)
    result = CliRunner().invoke(cli, ["read", mid])
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert data["material_id"] == mid
    assert data["counts"]["chunks"] == 2


def test_read_card_human(repo):
    mid = _seed_material(repo)
    result = CliRunner().invoke(cli, ["read", mid, "--human"])
    assert result.exit_code == 0
    assert "Sample" in result.output
    assert "counts:" in result.output


def test_read_page_and_chunk(repo):
    mid = _seed_material(repo)
    page = CliRunner().invoke(cli, ["read", mid, "--page", "2"])
    assert page.exit_code == 0
    assert json.loads(page.output)["text"] == "Page two."

    chunk = CliRunner().invoke(cli, ["read", mid, "--chunk", "chk_00001"])
    assert chunk.exit_code == 0
    assert json.loads(chunk.output)["text"] == "Chunk one body."


def test_read_full_returns_text_md(repo):
    mid = _seed_material(repo)
    result = CliRunner().invoke(cli, ["read", mid, "--full"])
    assert result.exit_code == 0
    assert "# Full text" in result.output


def test_read_detail_chunks(repo):
    mid = _seed_material(repo)
    result = CliRunner().invoke(cli, ["read", mid, "--detail", "chunks"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert "card" in data and "chunks" in data
    assert [c["chunk_id"] for c in data["chunks"]] == ["chk_00001", "chk_00002"]


def test_read_mutually_exclusive_flags(repo):
    mid = _seed_material(repo)
    result = CliRunner().invoke(cli, ["read", mid, "--page", "1", "--chunk", "chk_00001"])
    assert result.exit_code != 0
    assert "mutually exclusive" in result.output.lower()


def test_read_unknown_material_errors(repo):
    result = CliRunner().invoke(cli, ["read", "nope_xxx"])
    assert result.exit_code != 0
    assert "not found" in result.output.lower()


def test_figures_list_and_filter(repo):
    mid = _seed_material(repo)
    result = CliRunner().invoke(cli, ["figures", mid])
    assert result.exit_code == 0
    rows = json.loads(result.output)
    assert [r["figure_id"] for r in rows] == ["fig_001"]

    result_vt = CliRunner().invoke(cli, ["figures", mid, "--visual-type", "photograph"])
    assert result_vt.exit_code == 0
    assert json.loads(result_vt.output) == []


def test_figures_by_id(repo):
    mid = _seed_material(repo)
    result = CliRunner().invoke(cli, ["figures", mid, "--figure", "fig_001"])
    assert result.exit_code == 0
    assert json.loads(result.output)["figure_id"] == "fig_001"


def test_annotations_filters(repo):
    mid = _seed_material(repo)
    result = CliRunner().invoke(cli, ["annotations", mid, "--page", "1"])
    assert result.exit_code == 0
    rows = json.loads(result.output)
    assert [r["annotation_id"] for r in rows] == ["ann_0001"]

    result_t = CliRunner().invoke(cli, ["annotations", mid, "--type", "note"])
    assert result_t.exit_code == 0
    assert [r["annotation_id"] for r in json.loads(result_t.output)] == ["ann_0002"]


def test_overview_snapshot(repo):
    _seed_material(repo)
    con = sqlite3.connect(str(repo / "indexes" / "search.sqlite"))
    con.executescript("""
        CREATE TABLE materials (material_id TEXT, domain TEXT, collection TEXT);
        CREATE TABLE chunks (chunk_id TEXT);
        CREATE TABLE figures (figure_id TEXT);
        CREATE TABLE annotations (annotation_id TEXT);
        CREATE TABLE wiki_pages (path TEXT);
    """)
    con.execute("INSERT INTO materials VALUES ('mat_001', 'research', 'papers')")
    con.commit()
    con.close()

    result = CliRunner().invoke(cli, ["overview"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["counts"]["materials"] == 1
    assert data["collections"][0]["domain"] == "research"


def test_refresh_delegates_to_update_workspace(repo, monkeypatch):
    import arquimedes.freshness as freshness_mod

    status = {
        "compiled_at": "2026-05-02T10:00:00+00:00",
        "index_rebuilt": False,
        "memory_rebuilt": False,
    }
    monkeypatch.setattr(freshness_mod, "update_workspace", lambda: status)

    result = CliRunner().invoke(cli, ["refresh"])
    assert result.exit_code == 0
    assert json.loads(result.output) == status

    result_human = CliRunner().invoke(cli, ["refresh", "--human"])
    assert result_human.exit_code == 0
    assert "compiled_at" in result_human.output
