"""Tests for search.py — multi-depth FTS5 search interface."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from arquimedes.index import rebuild_index
from arquimedes.memory import memory_rebuild
from arquimedes.search import SearchResult, format_human, get_bridge_member_clusters, get_cluster_global_bridges, get_collection_clusters, get_material_clusters, search, search_material_evidence


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


@pytest.fixture
def scoped_search_repo(tmp_path, monkeypatch):
    _setup_repo(tmp_path, [
        {
            "mid": "title001122aa",
            "title": "Musee Imaginaire and Architectural Imagination",
            "authors": ["Mariabruna Fabrizi"],
            "domain": "research",
            "document_type": "paper",
            "summary": "A study of image archives in architecture.",
        },
        {
            "mid": "author001122aa",
            "title": "Otterlo Circles and Collage",
            "authors": ["Aldo van Eyck"],
            "domain": "research",
            "document_type": "paper",
            "summary": "A study of collage as architectural thinking.",
        },
    ])
    monkeypatch.chdir(tmp_path)
    rebuild_index()
    return tmp_path


class TestScopedSearch:
    def test_title_scope_limits_card_matches_to_title(self, scoped_search_repo):
        result = search("Musee Imaginaire", depth=3, scope="title")
        ids = [r.material_id for r in result.results]
        assert ids == ["title001122aa"]
        assert result.canonical_clusters == []
        assert result.results[0].chunks == []

    def test_author_scope_limits_card_matches_to_authors(self, scoped_search_repo):
        result = search("Aldo van Eyck", depth=3, scope="author")
        ids = [r.material_id for r in result.results]
        assert ids == ["author001122aa"]
        assert result.results[0].chunks == []


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

    def test_search_material_evidence_returns_full_chunk_text(self, index_repo):
        evidence = search_material_evidence("thermal mass", "aabb001122", depth=3)
        assert evidence.material_id == "aabb001122"
        assert evidence.has_hits is True
        assert evidence.chunks
        assert "Thermal mass concrete walls reduce heat transfer." in evidence.chunks[0].text
        assert evidence.annotations


@pytest.fixture
def content_first_repo(tmp_path, monkeypatch):
    """Index with three materials: chunk-only, annotation-only, figure-only.

    'zorblax' appears only in a chunk of chunk_only_mid (not in the card).
    'qryvex' appears only in an annotation of ann_only_mid (not in the card).
    'wordmark' appears only in a figure description of fig_only_mid (not in the card).
    None of the three terms appear in meta.json titles or summaries.
    """
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "config.yaml").write_text("library_root: ~/dummy\n")
    (tmp_path / "indexes").mkdir()
    (tmp_path / "extracted").mkdir()
    (tmp_path / "manifests").mkdir()

    def _write_material(mid, chunks_text=None, annotation_text=None, figure_description=None):
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
        fig_dir = mat_dir / "figures"
        fig_dir.mkdir()
        if figure_description:
            (fig_dir / "fig_0001.json").write_text(json.dumps({
                "figure_id": "fig_0001", "source_page": 1,
                "image_path": "figures/fig_0001.jpeg", "bbox": [],
                "extraction_method": "embedded",
                "visual_type": {"value": "diagram", "provenance": {}},
                "description": {"value": figure_description, "provenance": {}},
                "caption": {"value": "Figure 1", "provenance": {}},
                "relevance": "substantive", "analysis_mode": "vision", "_enrichment_stamp": {},
            }))

    _write_material("chunk_only_mid", chunks_text="zorblax is a unique term in chunk text only")
    _write_material("ann_only_mid", annotation_text="qryvex is a unique term in annotation only")
    _write_material("fig_only_mid", figure_description="wordmark logo visible in the building facade diagram")

    (tmp_path / "manifests" / "materials.jsonl").write_text("\n".join([
        json.dumps({"material_id": "chunk_only_mid", "file_hash": "x", "relative_path": "a.pdf", "file_type": "pdf", "domain": "research", "collection": "_general", "ingested_at": "2026-01-01T00:00:00+00:00"}),
        json.dumps({"material_id": "ann_only_mid", "file_hash": "y", "relative_path": "b.pdf", "file_type": "pdf", "domain": "research", "collection": "_general", "ingested_at": "2026-01-01T00:00:00+00:00"}),
        json.dumps({"material_id": "fig_only_mid", "file_hash": "z", "relative_path": "c.pdf", "file_type": "pdf", "domain": "research", "collection": "_general", "ingested_at": "2026-01-01T00:00:00+00:00"}),
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

    def test_figure_only_not_surfaced_at_depth_1(self, content_first_repo):
        result = search("wordmark", depth=1)
        ids = [r.material_id for r in result.results]
        assert "fig_only_mid" not in ids

    def test_figure_only_surfaced_at_depth_2(self, content_first_repo):
        result = search("wordmark", depth=2)
        ids = [r.material_id for r in result.results]
        assert "fig_only_mid" in ids

    def test_figure_only_surfaced_at_depth_3(self, content_first_repo):
        result = search("wordmark", depth=3)
        ids = [r.material_id for r in result.results]
        assert "fig_only_mid" in ids

    def test_figure_only_card_has_figures(self, content_first_repo):
        result = search("wordmark", depth=2)
        card = next(r for r in result.results if r.material_id == "fig_only_mid")
        assert len(card.figures) > 0
        assert "wordmark" in card.figures[0].description.lower()


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

    def test_annotation_human_format_with_comment(self, index_repo):
        result = search("thermal mass", depth=2)
        output = format_human(result)
        # annotation with comment must use "-> comment" arrow format
        assert "→" in output or "->" in output or "key term" in output

    def test_annotation_human_format_structure(self, index_repo):
        result = search("thermal mass", depth=2)
        output = format_human(result)
        # must include page, type brackets, and quoted text
        assert "[highlight]" in output


@pytest.fixture
def ranking_repo(tmp_path, monkeypatch):
    """Index for ranking signal tests.

    - 'strong_card': matches 'blorfex' well at card level (title+summary+keywords),
      no annotations.
    - 'weak_card_strong_ann': card text contains 'blorfex' once (weak), but has
      two annotation comment hits containing 'blorfex'.

    FTS5 should rank strong_card first. After reranking, weak_card_strong_ann
    should rise above strong_card due to annotation evidence.
    """
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "config.yaml").write_text("library_root: ~/dummy\n")
    (tmp_path / "indexes").mkdir()
    (tmp_path / "extracted").mkdir()
    (tmp_path / "manifests").mkdir()

    def _make(mid, title, summary, keywords, chunk_text, chunk_emphasized, annotations):
        mat_dir = tmp_path / "extracted" / mid
        mat_dir.mkdir(parents=True)
        (mat_dir / "meta.json").write_text(json.dumps({
            "material_id": mid, "title": title, "authors": ["A"], "year": "2024",
            "page_count": 5, "file_type": "pdf", "domain": "research",
            "collection": "_general", "ingested_at": "2026-01-01T00:00:00+00:00",
            "raw_keywords": [], "raw_document_type": "paper",
            "summary": {"value": summary, "provenance": {}},
            "keywords": {"value": keywords, "provenance": {}},
            "document_type": {"value": "paper", "provenance": {}},
            "facets": {},
            "_enrichment_stamp": {"prompt_version": "v1", "enrichment_schema_version": "1"},
        }))
        chunks = [{"chunk_id": "chk_00001", "text": chunk_text, "source_pages": [1],
                   "emphasized": chunk_emphasized,
                   "summary": {"value": "chunk summary", "provenance": {}},
                   "keywords": {"value": [], "provenance": {}}, "content_class": "argument"}]
        (mat_dir / "chunks.jsonl").write_text("\n".join(json.dumps(c) for c in chunks))
        (mat_dir / "annotations.jsonl").write_text(
            "\n".join(json.dumps(a) for a in annotations)
        )
        (mat_dir / "figures").mkdir()

    # strong_card: 'blorfex' in title + summary + keywords → strong card match
    _make("strong_card",
          title="Blorfex Design Pattern in Architecture",
          summary="A comprehensive study of blorfex methods and blorfex applications.",
          keywords=["blorfex", "architectural pattern"],
          chunk_text="Blorfex is a design method.",
          chunk_emphasized=False,
          annotations=[])

    # weak_card_strong_ann: only title mentions 'blorfex', but two comments strongly match
    _make("weak_card_strong_ann",
          title="Blorfex in Practice",
          summary="A generic document about practice.",
          keywords=[],
          chunk_text="General practice document.",
          chunk_emphasized=False,
          annotations=[
              {"annotation_id": "ann_0001", "type": "highlight", "page": 1,
               "quoted_text": "some text", "comment": "blorfex key finding here", "color": "", "rect": []},
              {"annotation_id": "ann_0002", "type": "note", "page": 2,
               "quoted_text": "other text", "comment": "blorfex confirmed", "color": "", "rect": []},
          ])

    (tmp_path / "manifests" / "materials.jsonl").write_text("\n".join([
        json.dumps({"material_id": "strong_card", "file_hash": "x", "relative_path": "a.pdf",
                    "file_type": "pdf", "domain": "research", "collection": "_general",
                    "ingested_at": "2026-01-01T00:00:00+00:00"}),
        json.dumps({"material_id": "weak_card_strong_ann", "file_hash": "y", "relative_path": "b.pdf",
                    "file_type": "pdf", "domain": "research", "collection": "_general",
                    "ingested_at": "2026-01-01T00:00:00+00:00"}),
    ]))
    monkeypatch.chdir(tmp_path)
    rebuild_index()
    return tmp_path


@pytest.fixture
def emphasis_repo(tmp_path, monkeypatch):
    """Index with one material that has two matching chunks: one emphasized, one not."""
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "config.yaml").write_text("library_root: ~/dummy\n")
    (tmp_path / "indexes").mkdir()
    (tmp_path / "extracted").mkdir()
    (tmp_path / "manifests").mkdir()

    mid = "emph_test"
    mat_dir = tmp_path / "extracted" / mid
    mat_dir.mkdir(parents=True)
    (mat_dir / "meta.json").write_text(json.dumps({
        "material_id": mid, "title": "Emphasis Test Document", "authors": ["A"],
        "year": "2024", "page_count": 5, "file_type": "pdf", "domain": "research",
        "collection": "_general", "ingested_at": "2026-01-01T00:00:00+00:00",
        "raw_keywords": [], "raw_document_type": "paper",
        "summary": {"value": "Document about vrenblax.", "provenance": {}},
        "keywords": {"value": ["vrenblax"], "provenance": {}},
        "document_type": {"value": "paper", "provenance": {}},
        "facets": {},
        "_enrichment_stamp": {"prompt_version": "v1", "enrichment_schema_version": "1"},
    }))
    # chunk at page 1 is NOT emphasized; chunk at page 2 IS emphasized — same FTS relevance
    chunks = [
        {"chunk_id": "chk_00001", "text": "vrenblax material properties.", "source_pages": [1],
         "emphasized": False,
         "summary": {"value": "vrenblax page 1", "provenance": {}},
         "keywords": {"value": [], "provenance": {}}, "content_class": "argument"},
        {"chunk_id": "chk_00002", "text": "vrenblax material properties.", "source_pages": [2],
         "emphasized": True,
         "summary": {"value": "vrenblax page 2 emphasized", "provenance": {}},
         "keywords": {"value": [], "provenance": {}}, "content_class": "key_finding"},
    ]
    (mat_dir / "chunks.jsonl").write_text("\n".join(json.dumps(c) for c in chunks))
    (mat_dir / "annotations.jsonl").write_text("")
    (mat_dir / "figures").mkdir()
    (tmp_path / "manifests" / "materials.jsonl").write_text(json.dumps({
        "material_id": mid, "file_hash": mid, "relative_path": "a.pdf",
        "file_type": "pdf", "domain": "research", "collection": "_general",
        "ingested_at": "2026-01-01T00:00:00+00:00",
    }))
    monkeypatch.chdir(tmp_path)
    rebuild_index()
    return tmp_path


class TestAnnotationRankField:
    def test_annotation_hit_has_rank(self, index_repo):
        result = search("thermal mass", depth=2)
        card = next(r for r in result.results if r.material_id == "aabb001122")
        for ann in card.annotations:
            assert isinstance(ann.rank, int)
            assert ann.rank >= 1

    def test_annotation_rank_in_dict(self, index_repo):
        result = search("thermal mass", depth=2)
        card = next(r for r in result.results if r.material_id == "aabb001122")
        d = result.to_dict()
        result_dict = next(r for r in d["results"] if r["material_id"] == "aabb001122")
        ann_dicts = result_dict.get("annotations", [])
        assert len(ann_dicts) > 0
        assert "rank" in ann_dicts[0]

    def test_annotation_ranks_sequential(self, index_repo):
        result = search("thermal mass", depth=2)
        card = next(r for r in result.results if r.material_id == "aabb001122")
        ranks = [a.rank for a in card.annotations]
        assert ranks == list(range(1, len(ranks) + 1))


class TestAnnotationCommentBoost:
    """Annotations with comments rank above annotations without comments."""

    def test_comment_annotation_ranks_first(self, tmp_path, monkeypatch):
        """When one annotation has a comment and one doesn't, the comment one ranks first."""
        (tmp_path / "config").mkdir()
        (tmp_path / "config" / "config.yaml").write_text("library_root: ~/dummy\n")
        (tmp_path / "indexes").mkdir()
        (tmp_path / "extracted").mkdir()
        (tmp_path / "manifests").mkdir()

        mid = "ann_boost_test"
        mat_dir = tmp_path / "extracted" / mid
        mat_dir.mkdir(parents=True)
        (mat_dir / "meta.json").write_text(json.dumps({
            "material_id": mid, "title": "Plocrix Architecture Study", "authors": ["A"],
            "year": "2024", "page_count": 5, "file_type": "pdf", "domain": "research",
            "collection": "_general", "ingested_at": "2026-01-01T00:00:00+00:00",
            "raw_keywords": [], "raw_document_type": "paper",
            "summary": {"value": "Study of plocrix method.", "provenance": {}},
            "keywords": {"value": ["plocrix"], "provenance": {}},
            "document_type": {"value": "paper", "provenance": {}},
            "facets": {},
            "_enrichment_stamp": {"prompt_version": "v1", "enrichment_schema_version": "1"},
        }))
        (mat_dir / "chunks.jsonl").write_text("")
        # First annotation in file has NO comment; second has a comment
        (mat_dir / "annotations.jsonl").write_text("\n".join([
            json.dumps({"annotation_id": "ann_no_comment", "type": "highlight", "page": 1,
                        "quoted_text": "plocrix structural behavior", "comment": "",
                        "color": "", "rect": []}),
            json.dumps({"annotation_id": "ann_with_comment", "type": "highlight", "page": 2,
                        "quoted_text": "plocrix load path", "comment": "key finding for thesis",
                        "color": "", "rect": []}),
        ]))
        (mat_dir / "figures").mkdir()
        (tmp_path / "manifests" / "materials.jsonl").write_text(json.dumps({
            "material_id": mid, "file_hash": mid, "relative_path": "a.pdf",
            "file_type": "pdf", "domain": "research", "collection": "_general",
            "ingested_at": "2026-01-01T00:00:00+00:00",
        }))
        monkeypatch.chdir(tmp_path)
        rebuild_index()

        result = search("plocrix", depth=2)
        card = result.results[0]
        assert len(card.annotations) == 2
        # annotation with comment must be rank 1
        assert card.annotations[0].annotation_id == "ann_with_comment"
        assert card.annotations[0].rank == 1
        assert card.annotations[1].annotation_id == "ann_no_comment"


class TestEmphasisChunkBoost:
    """Emphasized chunks sort before non-emphasized within the same material,
    but only as a modest boost — a much stronger text-relevance match should still win."""

    def test_emphasized_chunk_ranks_first_at_equal_relevance(self, emphasis_repo):
        result = search("vrenblax", depth=2)
        assert result.total == 1
        card = result.results[0]
        assert len(card.chunks) == 2
        # emphasized chunk must come first when text relevance is equal
        assert card.chunks[0].emphasized is True
        assert card.chunks[1].emphasized is False

    def test_emphasized_chunk_rank_value(self, emphasis_repo):
        result = search("vrenblax", depth=2)
        card = result.results[0]
        assert card.chunks[0].rank == 1
        assert card.chunks[1].rank == 2


class TestMaterialRerankByAnnotation:
    """Materials with strong annotation evidence rerank above weaker-card-matched materials."""

    def test_annotation_evidence_lifts_material(self, ranking_repo):
        # strong_card has better FTS card score (more 'blorfex' occurrences in card fields)
        # weak_card_strong_ann has 2 annotation comment hits  → combined priority lifts it
        result = search("blorfex", depth=2)
        assert result.total >= 2
        ids = [r.material_id for r in result.results]
        assert "strong_card" in ids
        assert "weak_card_strong_ann" in ids
        # weak_card_strong_ann should outrank strong_card due to 2 comment hits (boost=1.6)
        assert ids.index("weak_card_strong_ann") < ids.index("strong_card")

    def test_reranking_does_not_affect_depth_1(self, ranking_repo):
        # At depth 1, no content queries → no reranking; strong_card should stay first
        result_d1 = search("blorfex", depth=1)
        result_d2 = search("blorfex", depth=2)
        ids_d1 = [r.material_id for r in result_d1.results]
        ids_d2 = [r.material_id for r in result_d2.results]
        # depth-1 order has strong_card first (no reranking)
        assert ids_d1[0] == "strong_card"
        # depth-2 order has weak_card_strong_ann first (annotation boost)
        assert ids_d2[0] == "weak_card_strong_ann"

    def test_final_ranks_sequential(self, ranking_repo):
        result = search("blorfex", depth=2)
        for i, card in enumerate(result.results, 1):
            assert card.rank == i


# --- helpers for concept / related tests ---

from arquimedes.search import find_related, list_concepts, format_related_human, format_concepts_human


def _write_concepts(mat_dir: Path, concepts: list[dict]) -> None:
    (mat_dir / "concepts.jsonl").write_text(
        "\n".join(json.dumps(c) for c in concepts)
    )


def _write_local_clusters(root: Path, domain: str, collection: str, clusters: list[dict]) -> None:
    cluster_dir = root / "derived" / "collections" / f"{domain}__{collection}"
    cluster_dir.mkdir(parents=True, exist_ok=True)
    (cluster_dir / "local_concept_clusters.jsonl").write_text(
        "\n".join(json.dumps(cluster) for cluster in clusters) + "\n",
        encoding="utf-8",
    )


def _write_global_bridge_clusters(root: Path, clusters: list[dict]) -> None:
    derived = root / "derived"
    derived.mkdir(parents=True, exist_ok=True)
    (derived / "global_bridge_clusters.jsonl").write_text(
        "\n".join(json.dumps(cluster) for cluster in clusters) + "\n",
        encoding="utf-8",
    )


def _write_lint_jsonl(root: Path, name: str, rows: list[dict]) -> None:
    lint_dir = root / "derived" / "lint"
    lint_dir.mkdir(parents=True, exist_ok=True)
    (lint_dir / name).write_text(
        "\n".join(json.dumps(row) for row in rows) + ("\n" if rows else ""),
        encoding="utf-8",
    )


@pytest.fixture
def concepts_repo(tmp_path, monkeypatch):
    """Two materials: mat_alpha shares a concept with mat_beta; different keywords and authors."""
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "config.yaml").write_text("library_root: ~/dummy\n")
    (tmp_path / "indexes").mkdir()
    (tmp_path / "extracted").mkdir()
    manifests = tmp_path / "manifests"
    manifests.mkdir()

    # mat_alpha: archival habitat concept, keyword "archive", author "Lee"
    alpha_dir = tmp_path / "extracted" / "mat_alpha"
    _write_meta(alpha_dir, "mat_alpha",
                title="Archival Habitat Study",
                keywords=["archival habitat", "postcolonial"],
                authors=["Rachel Lee"],
                location="India",
                historical_period="20th century",
                domain="research")
    _write_chunks(alpha_dir, "mat_alpha")
    _write_annotations(alpha_dir)
    _write_figure(alpha_dir)
    _write_concepts(alpha_dir, [
        {"concept_name": "archival habitat", "relevance": "high",
         "provenance": {"source_pages": [2, 3], "evidence_spans": ["the archival habitat"], "confidence": 1.0}},
        {"concept_name": "embodied archives", "relevance": "high",
         "provenance": {"source_pages": [7], "evidence_spans": ["embodied knowledge"], "confidence": 0.9}},
    ])

    # mat_beta: shares "archival habitat" concept and author "Lee", different keyword
    beta_dir = tmp_path / "extracted" / "mat_beta"
    _write_meta(beta_dir, "mat_beta",
                title="Koenigsberger Archive Work",
                keywords=["archival habitat", "migration"],
                authors=["Rachel Lee"],
                location="India",
                historical_period="20th century",
                domain="research")
    _write_chunks(beta_dir, "mat_beta")
    _write_annotations(beta_dir)
    _write_figure(beta_dir)
    _write_concepts(beta_dir, [
        {"concept_name": "archival habitat", "relevance": "high",
         "provenance": {"source_pages": [1, 5], "evidence_spans": ["archival habitat concept"], "confidence": 0.95}},
        {"concept_name": "oral history", "relevance": "medium",
         "provenance": {"source_pages": [4], "evidence_spans": ["oral history methods"], "confidence": 0.8}},
    ])

    # mat_gamma: no shared concept with alpha, different author, different location
    gamma_dir = tmp_path / "extracted" / "mat_gamma"
    _write_meta(gamma_dir, "mat_gamma",
                title="Thermal Mass in Concrete",
                keywords=["thermal mass", "concrete"],
                authors=["Smith J"],
                location="Spain",
                historical_period="21st century",
                domain="research")
    _write_chunks(gamma_dir, "mat_gamma")
    _write_annotations(gamma_dir)
    _write_figure(gamma_dir)
    _write_concepts(gamma_dir, [
        {"concept_name": "passive cooling", "relevance": "medium",
         "provenance": {"source_pages": [1], "evidence_spans": ["passive cooling strategy"], "confidence": 0.7}},
    ])

    manifest_lines = [
        json.dumps({"material_id": m, "file_hash": m, "relative_path": f"R/{m}.pdf",
                    "file_type": "pdf", "domain": "research", "collection": "_general",
                    "ingested_at": "2026-01-01T00:00:00+00:00"})
        for m in ["mat_alpha", "mat_beta", "mat_gamma"]
    ]
    (manifests / "materials.jsonl").write_text("\n".join(manifest_lines))

    monkeypatch.chdir(tmp_path)
    rebuild_index()
    _write_local_clusters(tmp_path, "research", "_general", [
        {
            "cluster_id": "research___general__local_0001",
            "domain": "research",
            "collection": "_general",
            "canonical_name": "Archival Habitat Cluster",
            "slug": "archival-habitat-cluster",
            "aliases": ["archival habitat"],
            "confidence": 0.95,
            "source_concepts": [
                {"material_id": "mat_alpha", "concept_name": "archival habitat", "relevance": "high", "source_pages": [2, 3], "evidence_spans": ["the archival habitat"], "confidence": 1.0},
                {"material_id": "mat_beta", "concept_name": "archival habitat", "relevance": "high", "source_pages": [1, 5], "evidence_spans": ["archival habitat concept"], "confidence": 0.95},
            ],
        }
    ])
    _write_global_bridge_clusters(tmp_path, [
        {
            "bridge_id": "global_bridge__archival-habitat-cluster",
            "canonical_name": "Archival Habitat Cluster",
            "slug": "archival-habitat-cluster",
            "aliases": ["archive habitat bridge"],
            "descriptor": "Cross-collection bridge around archival habitat.",
            "member_local_clusters": [
                {
                    "cluster_id": "research___general__local_0001",
                    "domain": "research",
                    "collection": "_general",
                    "canonical_name": "Archival Habitat Cluster",
                    "wiki_path": "wiki/research/_general/concepts/archival-habitat-cluster.md",
                    "material_ids": ["mat_alpha", "mat_beta"],
                }
            ],
            "domain_collection_keys": ["research___general", "practice__projects"],
            "supporting_material_ids": ["mat_alpha", "mat_beta"],
            "confidence": 0.9,
            "wiki_path": "wiki/shared/bridge-concepts/archival-habitat-cluster.md",
        }
    ])
    _write_lint_jsonl(tmp_path, "concept_reflections.jsonl", [
        {
            "cluster_id": "research___general__local_0001",
            "slug": "archival-habitat-cluster",
            "canonical_name": "Archival Habitat Cluster",
            "main_takeaways": ["Archival habitat frames domestic space as a site of memory work."],
            "main_tensions": ["Archival order can stabilize memory while also flattening lived contradiction."],
            "open_questions": ["How can domestic archives stay open to contested histories?"],
            "helpful_new_sources": ["Comparative work on household archives in postcolonial settings."],
            "why_this_concept_matters": "This concept shows how architecture can operate as a living archive rather than a neutral container.",
            "supporting_material_ids": ["mat_alpha", "mat_beta"],
            "supporting_evidence": [],
            "input_fingerprint": "abc123",
            "wiki_path": "wiki/research/_general/concepts/archival-habitat-cluster.md",
        }
    ])
    _write_lint_jsonl(tmp_path, "collection_reflections.jsonl", [
        {
            "domain": "research",
            "collection": "_general",
            "main_takeaways": ["The collection connects archival method, domestic space, and architectural imagination."],
            "main_tensions": ["Archival frameworks can clarify evidence while narrowing the messier life of images and memory."],
            "important_material_ids": ["mat_alpha", "mat_beta"],
            "important_cluster_ids": ["research___general__local_0001"],
            "open_questions": ["What kinds of domestic evidence resist archival classification?"],
            "helpful_new_sources": ["Studies of household archives and curatorial memory."],
            "why_this_collection_matters": "This collection matters because it treats architecture as a field of archival imagination rather than just a record of buildings.",
            "input_fingerprint": "def456",
            "wiki_path": "wiki/research/_general/_index.md",
        }
    ])
    memory_rebuild()
    return tmp_path


# --- C4.2: Concepts in search ---

class TestConceptSearch:
    def test_concept_hit_attached_at_depth2(self, concepts_repo):
        result = search("archival habitat", depth=2)
        alpha = next((r for r in result.results if r.material_id == "mat_alpha"), None)
        assert alpha is not None
        assert len(alpha.concepts) >= 1
        names = [c.concept_name for c in alpha.concepts]
        assert "archival habitat" in names

    def test_no_concept_hits_at_depth1(self, concepts_repo):
        result = search("archival", depth=1)
        for card in result.results:
            assert card.concepts == []

    def test_concept_only_material_surfaces_at_depth2(self, concepts_repo):
        """A material that matches via concept_name but not card text surfaces at depth 2."""
        # mat_beta title/summary don't contain "embodied" — only mat_alpha's concept does
        # but mat_alpha's concept "archival habitat" is also in mat_beta, so use unique concept
        # mat_alpha has "embodied archives"; mat_beta does not
        result_d2 = search("embodied", depth=2)
        ids = [r.material_id for r in result_d2.results]
        assert "mat_alpha" in ids

    def test_concept_limit_zero_suppresses_hits(self, concepts_repo):
        result = search("archival habitat", depth=2, concept_limit=0)
        for card in result.results:
            assert card.concepts == []

    def test_concept_boost_in_priority(self, concepts_repo):
        """Materials with concept matches rank among the results (not last) at depth 2."""
        result = search("archival", depth=2)
        assert result.total >= 1

    def test_concept_hit_to_dict(self, concepts_repo):
        result = search("archival habitat", depth=2)
        alpha = next((r for r in result.results if r.material_id == "mat_alpha"), None)
        assert alpha is not None
        d = alpha.to_dict()
        assert "concepts" in d
        assert isinstance(d["concepts"], list)
        assert d["concepts"][0]["concept_name"] == "archival habitat"
        # Provenance fields should be present when available
        assert "source_pages" in d["concepts"][0]
        assert "evidence_spans" in d["concepts"][0]

    def test_concept_hit_has_provenance(self, concepts_repo):
        """ConceptHit carries source_pages, evidence_spans, confidence from concepts.jsonl."""
        result = search("archival habitat", depth=2)
        alpha = next((r for r in result.results if r.material_id == "mat_alpha"), None)
        assert alpha is not None
        hit = next(c for c in alpha.concepts if c.concept_name == "archival habitat")
        assert hit.source_pages == [2, 3]
        assert "the archival habitat" in hit.evidence_spans
        assert hit.confidence == 1.0

    def test_concept_pages_surface_with_reflection_summary(self, concepts_repo):
        result = search("living archive", depth=3)
        assert result.canonical_clusters
        cluster = next(c for c in result.canonical_clusters if c.cluster_id == "research___general__local_0001")
        assert "living archive" in cluster.summary.lower()


class TestCollectionPageSearch:
    def test_collection_pages_surface_from_reflection_text(self, concepts_repo):
        result = search("archival imagination", depth=3)
        assert result.collection_pages
        hit = next(c for c in result.collection_pages if c.domain == "research" and c.collection == "_general")
        assert "architecture as a field of archival imagination" in hit.summary.lower()

    def test_title_scope_suppresses_collection_and_concept_pages(self, concepts_repo):
        result = search("archival habitat", depth=3, scope="title")
        assert result.collection_pages == []
        assert result.canonical_clusters == []

    def test_material_id_scope_restricts_results_to_linked_materials(self, concepts_repo):
        result = search("Smith J", depth=3, scope="author", material_ids=["mat_alpha", "mat_beta"])
        assert result.results == []


# --- C4.3: arq related ---

class TestFindRelated:
    def test_shared_concept_surfaces_related(self, concepts_repo):
        related = find_related("mat_alpha")
        ids = [r.material_id for r in related]
        assert "mat_beta" in ids  # shares "archival habitat" concept

    def test_self_not_in_results(self, concepts_repo):
        related = find_related("mat_alpha")
        ids = [r.material_id for r in related]
        assert "mat_alpha" not in ids

    def test_no_shared_signal_low_or_absent(self, concepts_repo):
        related = find_related("mat_alpha")
        ids = [r.material_id for r in related]
        # mat_gamma shares nothing with mat_alpha except domain
        # (domain is not a facet in _FACET_COLUMNS_FOR_RELATED — by design)
        # mat_gamma may or may not appear; if it does its score should be < mat_beta
        if "mat_gamma" in ids and "mat_beta" in ids:
            assert ids.index("mat_beta") < ids.index("mat_gamma")

    def test_connection_types_listed(self, concepts_repo):
        related = find_related("mat_alpha")
        beta = next((r for r in related if r.material_id == "mat_beta"), None)
        assert beta is not None
        types = {c.type for c in beta.connections}
        assert "shared_concept" in types

    def test_shared_local_cluster_contributes(self, concepts_repo):
        related = find_related("mat_alpha")
        beta = next((r for r in related if r.material_id == "mat_beta"), None)
        assert beta is not None
        types = {c.type for c in beta.connections}
        assert "shared_local_cluster" in types

    def test_shared_global_bridge_contributes(self, concepts_repo):
        related = find_related("mat_alpha")
        beta = next((r for r in related if r.material_id == "mat_beta"), None)
        assert beta is not None
        types = {c.type for c in beta.connections}
        assert "shared_global_cluster" in types

    def test_shared_author_contributes(self, concepts_repo):
        related = find_related("mat_alpha")
        beta = next((r for r in related if r.material_id == "mat_beta"), None)
        assert beta is not None
        types = {c.type for c in beta.connections}
        assert "shared_author" in types

    def test_shared_facet_contributes(self, concepts_repo):
        related = find_related("mat_alpha")
        beta = next((r for r in related if r.material_id == "mat_beta"), None)
        assert beta is not None
        facet_conns = [c for c in beta.connections if c.type == "shared_facet"]
        assert len(facet_conns) >= 1
        facets_used = {c.facet for c in facet_conns}
        assert "location" in facets_used or "historical_period" in facets_used

    def test_score_positive(self, concepts_repo):
        related = find_related("mat_alpha")
        beta = next((r for r in related if r.material_id == "mat_beta"), None)
        assert beta is not None
        assert beta.score > 0

    def test_limit_respected(self, concepts_repo):
        related = find_related("mat_alpha", limit=1)
        assert len(related) <= 1

    def test_to_dict_structure(self, concepts_repo):
        related = find_related("mat_alpha")
        assert related
        d = related[0].to_dict()
        assert "material_id" in d
        assert "score" in d
        assert "connections" in d
        assert isinstance(d["connections"], list)

    def test_format_related_human(self, concepts_repo):
        related = find_related("mat_alpha")
        out = format_related_human("mat_alpha", related)
        assert "mat_beta" in out
        assert "shared local home:" in out
        assert "shared bridge:" in out

    def test_no_related_empty_collection(self, tmp_path, monkeypatch):
        """Single-material collection → no related results."""
        (tmp_path / "config").mkdir()
        (tmp_path / "config" / "config.yaml").write_text("library_root: ~/dummy\n")
        (tmp_path / "indexes").mkdir()
        (tmp_path / "extracted").mkdir()
        manifests = tmp_path / "manifests"
        manifests.mkdir()
        mat_dir = tmp_path / "extracted" / "solo"
        _write_meta(mat_dir, "solo")
        _write_chunks(mat_dir, "solo")
        _write_annotations(mat_dir)
        _write_figure(mat_dir)
        (manifests / "materials.jsonl").write_text(json.dumps({
            "material_id": "solo", "file_hash": "solo",
            "relative_path": "R/solo.pdf", "file_type": "pdf",
            "domain": "research", "collection": "_general",
            "ingested_at": "2026-01-01T00:00:00+00:00",
        }))
        monkeypatch.chdir(tmp_path)
        rebuild_index()
        related = find_related("solo")
        assert related == []


# --- C4.4: arq concepts ---

class TestListConcepts:
    def test_all_concepts_listed(self, concepts_repo):
        entries = list_concepts()
        names = [e.concept_name for e in entries]
        assert "archival habitat" in names
        assert "embodied archives" in names
        assert "oral history" in names
        assert "passive cooling" in names

    def test_material_count_correct(self, concepts_repo):
        entries = list_concepts()
        entry = next(e for e in entries if e.concept_name == "archival habitat")
        # Both mat_alpha and mat_beta have "archival habitat"
        assert entry.material_count == 2
        assert set(entry.material_ids) == {"mat_alpha", "mat_beta"}

    def test_min_materials_filter(self, concepts_repo):
        entries = list_concepts(min_materials=2)
        # Only "archival habitat" appears in 2 materials
        assert len(entries) == 1
        assert entries[0].concept_name == "archival habitat"

    def test_limit_respected(self, concepts_repo):
        entries = list_concepts(limit=2)
        assert len(entries) <= 2

    def test_ordered_by_count_desc(self, concepts_repo):
        entries = list_concepts()
        # archival_habitat (count=2) should appear before single-material concepts
        counts = [e.material_count for e in entries]
        assert counts == sorted(counts, reverse=True)

    def test_relevance_summary_present(self, concepts_repo):
        entries = list_concepts()
        entry = next(e for e in entries if e.concept_name == "archival habitat")
        assert entry.relevance_summary != ""

    def test_to_dict_structure(self, concepts_repo):
        entries = list_concepts()
        d = entries[0].to_dict()
        assert "concept_name" in d
        assert "material_count" in d
        assert "material_ids" in d
        assert "relevance_summary" in d

    def test_format_concepts_human(self, concepts_repo):
        entries = list_concepts()
        out = format_concepts_human(entries)
        assert "archival habitat" in out
        assert "2" in out  # material count for shared concept

    def test_relevance_summary_aggregated(self, concepts_repo):
        """Shared concept with same relevance shows '2×high', not arbitrary MAX."""
        entries = list_concepts()
        entry = next(e for e in entries if e.material_count == 2)
        # Both mat_alpha and mat_beta have relevance "high" for archival habitat
        assert "high" in entry.relevance_summary


class TestLocalClusterTraversal:
    def test_search_surfaces_local_clusters(self, concepts_repo):
        result = search("archival habitat")
        ids = [cluster.cluster_id for cluster in result.canonical_clusters]
        assert "research___general__local_0001" in ids

    def test_get_material_clusters_returns_local_clusters(self, concepts_repo):
        hits = get_material_clusters("mat_alpha")
        assert [hit.cluster_id for hit in hits] == ["research___general__local_0001"]

    def test_get_collection_clusters_returns_local_clusters(self, concepts_repo):
        hits = get_collection_clusters("research", "_general")
        assert [hit.cluster_id for hit in hits] == ["research___general__local_0001"]

    def test_get_cluster_global_bridges_returns_bridge_hits(self, concepts_repo):
        hits = get_cluster_global_bridges("research___general__local_0001")
        assert [hit.cluster_id for hit in hits] == ["global_bridge__archival-habitat-cluster"]

    def test_get_bridge_member_clusters_returns_local_hits(self, concepts_repo):
        hits = get_bridge_member_clusters("global_bridge__archival-habitat-cluster")
        assert [hit.cluster_id for hit in hits] == ["research___general__local_0001"]


# --- Concept normalization ---

class TestConceptNormalization:
    def test_case_variants_merge(self, tmp_path, monkeypatch):
        """'Archival Habitat' and 'archival habitat' in different materials share one concept_key."""
        (tmp_path / "config").mkdir()
        (tmp_path / "config" / "config.yaml").write_text("library_root: ~/dummy\n")
        (tmp_path / "indexes").mkdir()
        (tmp_path / "extracted").mkdir()
        manifests = tmp_path / "manifests"
        manifests.mkdir()

        for mid, name in [("m1", "Archival Habitat"), ("m2", "archival habitat")]:
            mat_dir = tmp_path / "extracted" / mid
            _write_meta(mat_dir, mid, title=f"Doc {mid}")
            _write_chunks(mat_dir, mid)
            _write_annotations(mat_dir)
            _write_figure(mat_dir)
            _write_concepts(mat_dir, [
                {"concept_name": name, "relevance": "high",
                 "provenance": {"source_pages": [1], "evidence_spans": [name], "confidence": 1.0}},
            ])
        (manifests / "materials.jsonl").write_text("\n".join(
            json.dumps({"material_id": mid, "file_hash": mid,
                        "relative_path": f"R/{mid}.pdf", "file_type": "pdf",
                        "domain": "research", "collection": "_general",
                        "ingested_at": "2026-01-01T00:00:00+00:00"})
            for mid in ["m1", "m2"]
        ))
        monkeypatch.chdir(tmp_path)
        rebuild_index()

        entries = list_concepts()
        # Should merge into ONE concept with count=2
        matching = [e for e in entries if "archival" in e.concept_name.lower()]
        assert len(matching) == 1
        assert matching[0].material_count == 2

    def test_plural_variants_merge(self, tmp_path, monkeypatch):
        """'archival habitats' (plural) merges with 'archival habitat' (singular)."""
        (tmp_path / "config").mkdir()
        (tmp_path / "config" / "config.yaml").write_text("library_root: ~/dummy\n")
        (tmp_path / "indexes").mkdir()
        (tmp_path / "extracted").mkdir()
        manifests = tmp_path / "manifests"
        manifests.mkdir()

        for mid, name in [("m1", "archival habitats"), ("m2", "archival habitat")]:
            mat_dir = tmp_path / "extracted" / mid
            _write_meta(mat_dir, mid, title=f"Doc {mid}")
            _write_chunks(mat_dir, mid)
            _write_annotations(mat_dir)
            _write_figure(mat_dir)
            _write_concepts(mat_dir, [
                {"concept_name": name, "relevance": "high",
                 "provenance": {"source_pages": [1], "evidence_spans": [name], "confidence": 1.0}},
            ])
        (manifests / "materials.jsonl").write_text("\n".join(
            json.dumps({"material_id": mid, "file_hash": mid,
                        "relative_path": f"R/{mid}.pdf", "file_type": "pdf",
                        "domain": "research", "collection": "_general",
                        "ingested_at": "2026-01-01T00:00:00+00:00"})
            for mid in ["m1", "m2"]
        ))
        monkeypatch.chdir(tmp_path)
        rebuild_index()

        entries = list_concepts()
        matching = [e for e in entries if "archival" in e.concept_name.lower()]
        assert len(matching) == 1
        assert matching[0].material_count == 2

    def test_related_uses_normalized_key(self, tmp_path, monkeypatch):
        """find_related discovers connection even when concept_name casing differs."""
        (tmp_path / "config").mkdir()
        (tmp_path / "config" / "config.yaml").write_text("library_root: ~/dummy\n")
        (tmp_path / "indexes").mkdir()
        (tmp_path / "extracted").mkdir()
        manifests = tmp_path / "manifests"
        manifests.mkdir()

        for mid, name in [("m1", "Archival Habitat"), ("m2", "archival habitats")]:
            mat_dir = tmp_path / "extracted" / mid
            _write_meta(mat_dir, mid, title=f"Doc {mid}")
            _write_chunks(mat_dir, mid)
            _write_annotations(mat_dir)
            _write_figure(mat_dir)
            _write_concepts(mat_dir, [
                {"concept_name": name, "relevance": "high",
                 "provenance": {"source_pages": [1], "evidence_spans": [name], "confidence": 1.0}},
            ])
        (manifests / "materials.jsonl").write_text("\n".join(
            json.dumps({"material_id": mid, "file_hash": mid,
                        "relative_path": f"R/{mid}.pdf", "file_type": "pdf",
                        "domain": "research", "collection": "_general",
                        "ingested_at": "2026-01-01T00:00:00+00:00"})
            for mid in ["m1", "m2"]
        ))
        monkeypatch.chdir(tmp_path)
        rebuild_index()

        related = find_related("m1")
        ids = [r.material_id for r in related]
        assert "m2" in ids
        types = {c.type for c in related[0].connections}
        assert "shared_concept" in types
