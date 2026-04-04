"""Tests for search.py — multi-depth FTS5 search interface."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from arquimedes.index import rebuild_index
from arquimedes.search import SearchResult, format_human, search


# --- Fixtures (reuse helpers from test_index pattern) ---

def _write_meta(mat_dir: Path, mid: str, **overrides) -> None:
    mat_dir.mkdir(parents=True, exist_ok=True)
    (mat_dir / "meta.json").write_text(json.dumps({
        "material_id": mid,
        "title": overrides.get("title", "Test Document on Thermal Mass"),
        "authors": overrides.get("authors", ["Author A"]),
        "year": overrides.get("year", "2024"),
        "page_count": 10,
        "file_type": "pdf",
        "domain": overrides.get("domain", "research"),
        "collection": overrides.get("collection", "_general"),
        "ingested_at": "2026-01-01T00:00:00+00:00",
        "raw_keywords": ["thermal", "mass"],
        "raw_document_type": "paper",
        "summary": {"value": overrides.get("summary", "Study of thermal mass."), "provenance": {}},
        "keywords": {"value": overrides.get("keywords", ["thermal mass", "concrete"]), "provenance": {}},
        "document_type": {"value": overrides.get("document_type", "paper"), "provenance": {}},
        "facets": {
            "scale": {"value": overrides.get("scale", "building"), "provenance": {}},
            "location": {"value": overrides.get("location", "Spain"), "provenance": {}},
        },
        "_enrichment_stamp": {"prompt_version": "enrich-v1.0", "enrichment_schema_version": "1"},
    }))


def _write_chunks(mat_dir: Path, mid: str) -> None:
    chunks = [
        {"chunk_id": "chk_00001", "text": "Thermal mass concrete walls reduce heat transfer.", "source_pages": [1], "emphasized": True, "summary": {"value": "Thermal mass walls.", "provenance": {}}, "keywords": {"value": ["thermal mass"], "provenance": {}}, "content_class": "argument"},
        {"chunk_id": "chk_00002", "text": "Passive cooling through night ventilation.", "source_pages": [2], "emphasized": False, "summary": {"value": "Night ventilation.", "provenance": {}}, "keywords": {"value": ["passive cooling"], "provenance": {}}, "content_class": "methodology"},
    ]
    (mat_dir / "chunks.jsonl").write_text("\n".join(json.dumps(c) for c in chunks))


def _write_annotations(mat_dir: Path) -> None:
    (mat_dir / "annotations.jsonl").write_text(json.dumps({
        "annotation_id": "ann_0001", "type": "highlight",
        "page": 1, "quoted_text": "thermal mass", "comment": "key term", "color": "", "rect": [],
    }))


def _write_figure(mat_dir: Path) -> None:
    fig_dir = mat_dir / "figures"
    fig_dir.mkdir(exist_ok=True)
    (fig_dir / "fig_0001.json").write_text(json.dumps({
        "figure_id": "fig_0001", "source_page": 1,
        "image_path": "figures/fig_0001.jpeg", "bbox": [],
        "extraction_method": "embedded",
        "visual_type": {"value": "diagram", "provenance": {}},
        "description": {"value": "A wall section diagram.", "provenance": {}},
        "caption": {"value": "Figure 1", "provenance": {}},
        "relevance": "substantive", "analysis_mode": "vision",
        "_enrichment_stamp": {},
    }))


def _setup_repo(root: Path, materials: list[dict]) -> None:
    (root / "config").mkdir()
    (root / "config" / "config.yaml").write_text("library_root: ~/dummy\n")
    (root / "indexes").mkdir()
    (root / "extracted").mkdir()
    manifests = root / "manifests"
    manifests.mkdir()

    manifest_lines = []
    for m in materials:
        mid = m["mid"]
        mat_dir = root / "extracted" / mid
        _write_meta(mat_dir, mid, **{k: v for k, v in m.items() if k != "mid"})
        _write_chunks(mat_dir, mid)
        _write_annotations(mat_dir)
        _write_figure(mat_dir)
        manifest_lines.append(json.dumps({
            "material_id": mid, "file_hash": mid,
            "relative_path": f"Research/{mid}.pdf", "file_type": "pdf",
            "domain": m.get("domain", "research"), "collection": "_general",
            "ingested_at": "2026-01-01T00:00:00+00:00",
        }))
    (manifests / "materials.jsonl").write_text("\n".join(manifest_lines))


@pytest.fixture
def index_repo(tmp_path, monkeypatch):
    """Pre-built index with two materials."""
    _setup_repo(tmp_path, [
        {"mid": "aabb001122", "title": "Thermal Mass in Mediterranean Climate",
         "domain": "research", "document_type": "paper", "scale": "building",
         "summary": "A study of thermal mass in Mediterranean buildings."},
        {"mid": "ccdd334455", "title": "Barcelona Building Code Section 4",
         "domain": "practice", "collection": "regulations", "document_type": "regulation",
         "scale": "urban", "summary": "Fire and thermal regulations for Barcelona."},
    ])
    monkeypatch.chdir(tmp_path)
    rebuild_index()
    return tmp_path


# --- Tests ---

class TestCardSearch:
    def test_returns_search_result(self, index_repo):
        result = search("thermal mass")
        assert isinstance(result, SearchResult)
        assert result.depth == 1

    def test_finds_matching_material(self, index_repo):
        result = search("thermal mass")
        assert result.total >= 1
        ids = [r.material_id for r in result.results]
        assert "aabb001122" in ids

    def test_empty_results(self, index_repo):
        result = search("xyzzy_not_in_index_abc123")
        assert result.total == 0
        assert result.results == []

    def test_no_chunks_at_depth_1(self, index_repo):
        result = search("thermal mass", depth=1)
        for card in result.results:
            assert card.chunks == []
            assert card.annotations == []

    def test_keywords_are_list(self, index_repo):
        result = search("thermal mass")
        assert result.total > 0
        assert isinstance(result.results[0].keywords, list)

    def test_rank_starts_at_1(self, index_repo):
        result = search("thermal")
        assert result.results[0].rank == 1

    def test_limit_respected(self, index_repo):
        result = search("thermal", limit=1)
        assert len(result.results) <= 1


class TestFacetFiltering:
    def test_domain_filter_research(self, index_repo):
        result = search("thermal", facets=["domain=research"])
        ids = [r.material_id for r in result.results]
        assert "aabb001122" in ids
        assert "ccdd334455" not in ids

    def test_domain_filter_practice(self, index_repo):
        result = search("thermal", facets=["domain=practice"])
        ids = [r.material_id for r in result.results]
        assert "ccdd334455" in ids
        assert "aabb001122" not in ids

    def test_collection_shorthand(self, index_repo):
        result = search("Barcelona", collection="regulations")
        ids = [r.material_id for r in result.results]
        assert "ccdd334455" in ids

    def test_unknown_facet_ignored(self, index_repo):
        # Should not raise, just returns results without the invalid filter
        result = search("thermal", facets=["nonexistent_col=value"])
        assert isinstance(result, SearchResult)

    def test_multiple_facets_anded(self, index_repo):
        # Both conditions must match
        result = search("thermal", facets=["domain=research", "scale=building"])
        ids = [r.material_id for r in result.results]
        assert "aabb001122" in ids

    def test_exact_match_facet(self, index_repo):
        result = search("thermal", facets=["domain==research"])
        ids = [r.material_id for r in result.results]
        assert "aabb001122" in ids


class TestDeepSearch:
    def test_depth_2_includes_chunks(self, index_repo):
        result = search("thermal mass", depth=2)
        card = next(r for r in result.results if r.material_id == "aabb001122")
        assert len(card.chunks) > 0

    def test_depth_2_chunks_have_no_text(self, index_repo):
        result = search("thermal mass", depth=2)
        card = next(r for r in result.results if r.material_id == "aabb001122")
        for chunk in card.chunks:
            assert chunk.text == ""

    def test_depth_3_chunks_have_text(self, index_repo):
        result = search("thermal mass", depth=3)
        card = next(r for r in result.results if r.material_id == "aabb001122")
        assert len(card.chunks) > 0
        assert all(chunk.text != "" for chunk in card.chunks)

    def test_depth_2_annotations_included(self, index_repo):
        result = search("thermal mass", depth=2)
        card = next(r for r in result.results if r.material_id == "aabb001122")
        assert len(card.annotations) > 0

    def test_chunk_limit_respected(self, index_repo):
        result = search("thermal", depth=2, chunk_limit=1)
        for card in result.results:
            assert len(card.chunks) <= 1


class TestSearchResult:
    def test_to_dict_structure(self, index_repo):
        result = search("thermal")
        d = result.to_dict()
        assert "query" in d
        assert "depth" in d
        assert "total" in d
        assert "results" in d

    def test_to_json_valid(self, index_repo):
        result = search("thermal")
        j = result.to_json()
        parsed = json.loads(j)
        assert parsed["query"] == "thermal"

    def test_missing_index_raises(self, tmp_path, monkeypatch):
        (tmp_path / "config").mkdir()
        (tmp_path / "config" / "config.yaml").write_text("library_root: ~/dummy\n")
        monkeypatch.chdir(tmp_path)
        with pytest.raises(FileNotFoundError, match="arq index rebuild"):
            search("thermal")


class TestHumanFormat:
    def test_depth_1_shows_table(self, index_repo):
        result = search("thermal mass", depth=1)
        output = format_human(result)
        assert "thermal mass" in output.lower() or "thermal" in output.lower()
        assert "#" in output  # table header

    def test_depth_2_shows_chunks(self, index_repo):
        result = search("thermal mass", depth=2)
        output = format_human(result)
        assert "Chunks:" in output

    def test_empty_result_message(self, index_repo):
        result = search("xyzzy_not_in_index_abc123")
        output = format_human(result)
        assert "No results" in output
