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

    def test_annotation_limit_respected(self, index_repo):
        result = search("thermal mass", depth=2, annotation_limit=0)
        for card in result.results:
            assert len(card.annotations) == 0

    def test_figure_limit_respected(self, index_repo):
        result = search("thermal mass", depth=2, figure_limit=0)
        for card in result.results:
            assert len(card.figures) == 0


@pytest.fixture
def content_first_repo(tmp_path, monkeypatch):
    """Index with two materials: one matched only at chunk level, one only at annotation level.

    'zorblax' appears only in a chunk of chunk_only_mid (not in the card).
    'qryvex' appears only in an annotation of ann_only_mid (not in the card).
    Neither term appears in meta.json titles or summaries.
    """
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "config.yaml").write_text("library_root: ~/dummy\n")
    (tmp_path / "indexes").mkdir()
    (tmp_path / "extracted").mkdir()
    (tmp_path / "manifests").mkdir()

    def _write_material(mid, chunks_text=None, annotation_text=None):
        mat_dir = tmp_path / "extracted" / mid
        mat_dir.mkdir(parents=True)
        (mat_dir / "meta.json").write_text(json.dumps({
            "material_id": mid, "title": "Generic Architecture Document",
            "authors": ["A"], "year": "2024", "page_count": 5, "file_type": "pdf",
            "domain": "research", "collection": "_general",
            "ingested_at": "2026-01-01T00:00:00+00:00", "raw_keywords": [],
            "raw_document_type": "paper",
            "summary": {"value": "A generic document about architecture.", "provenance": {}},
            "keywords": {"value": [], "provenance": {}},
            "document_type": {"value": "paper", "provenance": {}},
            "facets": {},
            "_enrichment_stamp": {"prompt_version": "v1", "enrichment_schema_version": "1"},
        }))
        chunks = []
        if chunks_text:
            chunks.append({
                "chunk_id": "chk_00001", "text": chunks_text, "source_pages": [1],
                "emphasized": False,
                "summary": {"value": "irrelevant generic summary", "provenance": {}},
                "keywords": {"value": [], "provenance": {}}, "content_class": "argument",
            })
        (mat_dir / "chunks.jsonl").write_text("\n".join(json.dumps(c) for c in chunks))
        if annotation_text:
            (mat_dir / "annotations.jsonl").write_text(json.dumps({
                "annotation_id": "ann_0001", "type": "highlight", "page": 1,
                "quoted_text": annotation_text, "comment": "", "color": "", "rect": [],
            }))
        else:
            (mat_dir / "annotations.jsonl").write_text("")
        (mat_dir / "figures").mkdir()

    _write_material("chunk_only_mid", chunks_text="zorblax is a unique term in chunk text only")
    _write_material("ann_only_mid", annotation_text="qryvex is a unique term in annotation only")

    (tmp_path / "manifests" / "materials.jsonl").write_text("\n".join([
        json.dumps({"material_id": "chunk_only_mid", "file_hash": "x", "relative_path": "a.pdf", "file_type": "pdf", "domain": "research", "collection": "_general", "ingested_at": "2026-01-01T00:00:00+00:00"}),
        json.dumps({"material_id": "ann_only_mid", "file_hash": "y", "relative_path": "b.pdf", "file_type": "pdf", "domain": "research", "collection": "_general", "ingested_at": "2026-01-01T00:00:00+00:00"}),
    ]))
    monkeypatch.chdir(tmp_path)
    from arquimedes.index import rebuild_index
    rebuild_index()
    return tmp_path


class TestContentFirstDeepSearch:
    """Materials with content hits but no card hits must surface at depth >= 2."""

    def test_chunk_only_not_surfaced_at_depth_1(self, content_first_repo):
        result = search("zorblax", depth=1)
        ids = [r.material_id for r in result.results]
        assert "chunk_only_mid" not in ids

    def test_chunk_only_surfaced_at_depth_2(self, content_first_repo):
        result = search("zorblax", depth=2)
        ids = [r.material_id for r in result.results]
        assert "chunk_only_mid" in ids

    def test_chunk_only_surfaced_at_depth_3(self, content_first_repo):
        result = search("zorblax", depth=3)
        ids = [r.material_id for r in result.results]
        assert "chunk_only_mid" in ids

    def test_annotation_only_not_surfaced_at_depth_1(self, content_first_repo):
        result = search("qryvex", depth=1)
        ids = [r.material_id for r in result.results]
        assert "ann_only_mid" not in ids

    def test_annotation_only_surfaced_at_depth_2(self, content_first_repo):
        result = search("qryvex", depth=2)
        ids = [r.material_id for r in result.results]
        assert "ann_only_mid" in ids

    def test_content_only_card_has_correct_material_id(self, content_first_repo):
        result = search("zorblax", depth=2)
        card = next(r for r in result.results if r.material_id == "chunk_only_mid")
        assert card.material_id == "chunk_only_mid"
        assert len(card.chunks) > 0

    def test_facets_still_filter_content_only_materials(self, content_first_repo):
        # chunk_only_mid is domain=research; filtering for domain=practice should exclude it
        result = search("zorblax", depth=2, facets=["domain=practice"])
        ids = [r.material_id for r in result.results]
        assert "chunk_only_mid" not in ids


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

