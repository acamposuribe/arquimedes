"""Tests for index.py — SQLite FTS5 index builder."""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from arquimedes.index import (
    IndexStats,
    _compute_manifest_hash,
    _compute_extracted_snapshot,
    ensure_index,
    rebuild_index,
    index_clusters,
)


# --- Fixtures ---

def _write_meta(mat_dir: Path, mid: str, **overrides) -> None:
    meta = {
        "material_id": mid,
        "file_hash": mid,
        "source_path": f"Research/{mid}.pdf",
        "title": overrides.get("title", "Test Document"),
        "authors": overrides.get("authors", ["Author One"]),
        "year": overrides.get("year", "2024"),
        "language": "",
        "source_url": "",
        "page_count": overrides.get("page_count", 10),
        "file_type": "pdf",
        "domain": overrides.get("domain", "research"),
        "collection": overrides.get("collection", "_general"),
        "ingested_at": "2026-01-01T00:00:00+00:00",
        "raw_keywords": overrides.get("raw_keywords", ["keyword"]),
        "raw_document_type": overrides.get("raw_document_type", "paper"),
        "summary": {"value": overrides.get("summary", "A test summary."), "provenance": {}},
        "keywords": {"value": overrides.get("keywords", ["keyword one", "keyword two"]), "provenance": {}},
        "document_type": {"value": overrides.get("document_type", "paper"), "provenance": {}},
        "facets": {
            "scale": {"value": overrides.get("scale", "building"), "provenance": {}},
            "location": {"value": overrides.get("location", "Spain"), "provenance": {}},
        },
        "_enrichment_stamp": {"prompt_version": "enrich-v1.0", "enrichment_schema_version": "1"},
    }
    mat_dir.mkdir(parents=True, exist_ok=True)
    (mat_dir / "meta.json").write_text(json.dumps(meta))


def _write_chunks(mat_dir: Path, mid: str, n: int = 3) -> None:
    lines = []
    for i in range(1, n + 1):
        chunk = {
            "chunk_id": f"chk_{i:05d}",
            "text": f"Chunk text number {i} about thermal mass and concrete structures.",
            "source_pages": [i],
            "emphasized": i == 1,
            "summary": {"value": f"Summary of chunk {i}.", "provenance": {}},
            "keywords": {"value": [f"keyword{i}"], "provenance": {}},
            "content_class": "argument",
        }
        lines.append(json.dumps(chunk))
    (mat_dir / "chunks.jsonl").write_text("\n".join(lines))


def _write_annotations(mat_dir: Path, mid: str) -> None:
    ann = {
        "annotation_id": "ann_0001",
        "type": "highlight",
        "page": 2,
        "quoted_text": "thermal mass",
        "comment": "important",
        "color": "#ffc100",
        "rect": [],
    }
    (mat_dir / "annotations.jsonl").write_text(json.dumps(ann))


def _write_figure(mat_dir: Path, mid: str) -> None:
    fig_dir = mat_dir / "figures"
    fig_dir.mkdir(exist_ok=True)
    fig = {
        "figure_id": "fig_0001",
        "source_page": 1,
        "image_path": "figures/fig_0001.jpeg",
        "bbox": [],
        "extraction_method": "embedded",
        "visual_type": {"value": "diagram", "provenance": {}},
        "description": {"value": "A plan view of a building.", "provenance": {}},
        "caption": {"value": "Figure 1", "provenance": {}},
        "relevance": "substantive",
        "analysis_mode": "vision",
        "_enrichment_stamp": {"prompt_version": "enrich-v1.0", "enrichment_schema_version": "1"},
    }
    (fig_dir / "fig_0001.json").write_text(json.dumps(fig))


def _write_manifest(root: Path, mids: list[str]) -> None:
    manifests = root / "manifests"
    manifests.mkdir(exist_ok=True)
    lines = []
    for mid in mids:
        lines.append(json.dumps({
            "material_id": mid,
            "file_hash": mid,
            "relative_path": f"Research/{mid}.pdf",
            "file_type": "pdf",
            "domain": "research",
            "collection": "_general",
            "ingested_at": "2026-01-01T00:00:00+00:00",
        }))
    (manifests / "materials.jsonl").write_text("\n".join(lines))


@pytest.fixture
def repo(tmp_path, monkeypatch):
    """Minimal repo with config.yaml, one material."""
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "config.yaml").write_text(
        "library_root: ~/dummy\n"
    )
    (tmp_path / "indexes").mkdir()
    (tmp_path / "extracted").mkdir()

    monkeypatch.chdir(tmp_path)
    return tmp_path


def _add_material(repo: Path, mid: str = "aabbcc112233", **meta_overrides) -> Path:
    mat_dir = repo / "extracted" / mid
    _write_meta(mat_dir, mid, **meta_overrides)
    _write_chunks(mat_dir, mid)
    _write_annotations(mat_dir, mid)
    _write_figure(mat_dir, mid)
    _write_manifest(repo, [mid])
    return mat_dir


# --- Tests ---

class TestRebuildIndex:
    def test_creates_index_file(self, repo):
        _add_material(repo)
        stats = rebuild_index()
        assert (repo / "indexes" / "search.sqlite").exists()

    def test_correct_row_counts(self, repo):
        _add_material(repo)
        stats = rebuild_index()
        assert stats.materials == 1
        assert stats.chunks == 3
        assert stats.figures == 1
        assert stats.annotations == 1

    def test_empty_manifest(self, repo):
        _write_manifest(repo, [])
        stats = rebuild_index()
        assert stats.materials == 0
        assert stats.chunks == 0

    def test_unenriched_material_still_indexed(self, repo):
        """Materials without enriched fields are still findable by title."""
        mid = "aabbcc112233"
        mat_dir = repo / "extracted" / mid
        mat_dir.mkdir(parents=True)
        # Minimal meta — no enriched fields
        (mat_dir / "meta.json").write_text(json.dumps({
            "material_id": mid,
            "title": "Unenriched Paper",
            "domain": "research",
            "collection": "_general",
            "raw_keywords": ["thermal"],
            "raw_document_type": "paper",
        }))
        _write_manifest(repo, [mid])
        stats = rebuild_index()
        assert stats.materials == 1

    def test_multiple_materials(self, repo):
        for i, mid in enumerate(["aabb001122", "ccdd334455"]):
            mat_dir = repo / "extracted" / mid
            _write_meta(mat_dir, mid, title=f"Material {i}")
            _write_chunks(mat_dir, mid, n=5)
            _write_manifest(repo, ["aabb001122", "ccdd334455"])
        stats = rebuild_index()
        assert stats.materials == 2
        assert stats.chunks == 10

    def test_elapsed_is_positive(self, repo):
        _add_material(repo)
        stats = rebuild_index()
        assert stats.elapsed >= 0

    def test_value_extraction_from_enriched_fields(self, repo):
        """EnrichedField .value is extracted, not the whole dict."""
        import sqlite3
        _add_material(repo, summary="A wonderful summary text.")
        rebuild_index()
        con = sqlite3.connect(str(repo / "indexes" / "search.sqlite"))
        row = con.execute("SELECT summary FROM materials WHERE material_id = ?", ("aabbcc112233",)).fetchone()
        con.close()
        assert row[0] == "A wonderful summary text."

    def test_keywords_stored_as_json_array(self, repo):
        import sqlite3
        _add_material(repo, keywords=["thermal mass", "concrete"])
        rebuild_index()
        con = sqlite3.connect(str(repo / "indexes" / "search.sqlite"))
        row = con.execute("SELECT keywords FROM materials WHERE material_id = ?", ("aabbcc112233",)).fetchone()
        con.close()
        kws = json.loads(row[0])
        assert kws == ["thermal mass", "concrete"]

    def test_facets_indexed(self, repo):
        import sqlite3
        _add_material(repo, scale="urban", location="Barcelona")
        rebuild_index()
        con = sqlite3.connect(str(repo / "indexes" / "search.sqlite"))
        row = con.execute("SELECT scale, location FROM materials WHERE material_id = ?", ("aabbcc112233",)).fetchone()
        con.close()
        assert row[0] == "urban"
        assert row[1] == "Barcelona"

    def test_atomic_write_no_tmp_left_on_success(self, repo):
        _add_material(repo)
        rebuild_index()
        tmps = list((repo / "indexes").glob("*.tmp"))
        assert tmps == []


class TestEnsureIndex:
    def test_rebuilds_when_no_index(self, repo):
        _add_material(repo)
        rebuilt, stats = ensure_index()
        assert rebuilt is True
        assert stats is not None
        assert stats.materials == 1

    def test_skips_when_current(self, repo):
        _add_material(repo)
        ensure_index()
        rebuilt, stats = ensure_index()
        assert rebuilt is False
        assert stats is None

    def test_rebuilds_when_new_material_added(self, repo):
        mid1 = "aabbcc112233"
        mat_dir = repo / "extracted" / mid1
        _write_meta(mat_dir, mid1)
        _write_manifest(repo, [mid1])
        ensure_index()

        # Add second material
        mid2 = "ddeeff445566"
        mat_dir2 = repo / "extracted" / mid2
        _write_meta(mat_dir2, mid2, title="Second Material")
        _write_manifest(repo, [mid1, mid2])

        rebuilt, stats = ensure_index()
        assert rebuilt is True
        assert stats is not None
        assert stats.materials == 2


class TestSnapshotHelpers:
    def test_manifest_hash_deterministic(self, repo):
        _add_material(repo)
        manifest_path = repo / "manifests" / "materials.jsonl"
        h1 = _compute_manifest_hash(manifest_path)
        h2 = _compute_manifest_hash(manifest_path)
        assert h1 == h2
        assert len(h1) == 16

    def test_manifest_hash_empty(self, repo):
        _write_manifest(repo, [])
        manifest_path = repo / "manifests" / "materials.jsonl"
        h = _compute_manifest_hash(manifest_path)
        assert isinstance(h, str)

    def test_extracted_snapshot_deterministic(self, repo):
        _add_material(repo)
        extracted_dir = repo / "extracted"
        s1 = _compute_extracted_snapshot(extracted_dir, ["aabbcc112233"])
        s2 = _compute_extracted_snapshot(extracted_dir, ["aabbcc112233"])
        assert s1 == s2
        assert len(s1) == 16


class TestSnapshotCoversAllInputs:
    """Snapshot must change when any index-input file changes, not just meta.json stamps."""

    def _snapshot(self, repo: Path, mid: str = "aabbcc112233") -> str:
        return _compute_extracted_snapshot(repo / "extracted", [mid])

    def test_changes_on_chunks_edit(self, repo):
        _add_material(repo)
        s1 = self._snapshot(repo)
        extra = {
            "chunk_id": "chk_extra", "text": "new chunk content xyz", "source_pages": [99],
            "emphasized": False, "summary": {"value": "extra", "provenance": {}},
            "keywords": {"value": [], "provenance": {}}, "content_class": "argument",
        }
        with open(repo / "extracted" / "aabbcc112233" / "chunks.jsonl", "a") as f:
            f.write("\n" + json.dumps(extra))
        assert self._snapshot(repo) != s1

    def test_changes_on_annotations_edit(self, repo):
        _add_material(repo)
        s1 = self._snapshot(repo)
        ann_path = repo / "extracted" / "aabbcc112233" / "annotations.jsonl"
        ann_path.write_text(ann_path.read_text() + "\n" + json.dumps({
            "annotation_id": "ann_extra", "type": "note", "page": 5,
            "quoted_text": "extra annotation text", "comment": "note", "color": "", "rect": [],
        }))
        assert self._snapshot(repo) != s1

    def test_changes_on_figure_edit(self, repo):
        _add_material(repo)
        s1 = self._snapshot(repo)
        fig_path = repo / "extracted" / "aabbcc112233" / "figures" / "fig_0001.json"
        fig = json.loads(fig_path.read_text())
        fig["description"] = {"value": "Updated description xyz", "provenance": {}}
        fig_path.write_text(json.dumps(fig))
        assert self._snapshot(repo) != s1


class TestEnsureIndexDetectsContentChanges:
    """ensure_index must rebuild when chunks/annotations/figures change."""

    def _backdate_index(self, repo: Path) -> None:
        import sqlite3 as _sqlite3
        con = _sqlite3.connect(str(repo / "indexes" / "search.sqlite"))
        con.execute("UPDATE index_state SET built_at = '2020-01-01T00:00:00+00:00' WHERE id = 1")
        con.commit()
        con.close()

    def test_rebuilds_when_chunks_changed(self, repo):
        _add_material(repo)
        ensure_index()
        self._backdate_index(repo)
        extra = {
            "chunk_id": "chk_extra", "text": "new extra chunk", "source_pages": [99],
            "emphasized": False, "summary": {"value": "extra", "provenance": {}},
            "keywords": {"value": [], "provenance": {}}, "content_class": "argument",
        }
        with open(repo / "extracted" / "aabbcc112233" / "chunks.jsonl", "a") as f:
            f.write("\n" + json.dumps(extra))
        rebuilt, _ = ensure_index()
        assert rebuilt is True

    def test_rebuilds_when_annotations_changed(self, repo):
        _add_material(repo)
        ensure_index()
        self._backdate_index(repo)
        ann_path = repo / "extracted" / "aabbcc112233" / "annotations.jsonl"
        ann_path.write_text(ann_path.read_text() + "\n" + json.dumps({
            "annotation_id": "ann_extra", "type": "note", "page": 5,
            "quoted_text": "extra annotation", "comment": "", "color": "", "rect": [],
        }))
        rebuilt, _ = ensure_index()
        assert rebuilt is True

    def test_rebuilds_when_figure_changed(self, repo):
        _add_material(repo)
        ensure_index()
        self._backdate_index(repo)
        fig_path = repo / "extracted" / "aabbcc112233" / "figures" / "fig_0001.json"
        fig = json.loads(fig_path.read_text())
        fig["description"] = {"value": "Completely new description", "provenance": {}}
        fig_path.write_text(json.dumps(fig))
        rebuilt, _ = ensure_index()
        assert rebuilt is True


# --- concepts.jsonl helpers ---

def _write_concepts(mat_dir: Path, concepts: list[dict]) -> None:
    lines = [json.dumps(c) for c in concepts]
    (mat_dir / "concepts.jsonl").write_text("\n".join(lines))


# --- C4.1: Concepts indexing ---

class TestConceptsIndexed:
    def test_concepts_in_stats(self, repo):
        mat_dir = _add_material(repo)
        _write_concepts(mat_dir, [
            {"concept_name": "archival habitat", "relevance": "high"},
            {"concept_name": "embodied archives", "relevance": "high"},
        ])
        stats = rebuild_index()
        assert stats.concepts == 2

    def test_concepts_table_populated(self, repo):
        import sqlite3
        mat_dir = _add_material(repo)
        _write_concepts(mat_dir, [
            {"concept_name": "archival habitat", "relevance": "high",
             "provenance": {"source_pages": [2, 3], "evidence_spans": ["the archival habitat"], "confidence": 1.0}},
        ])
        rebuild_index()
        con = sqlite3.connect(str(repo / "indexes" / "search.sqlite"))
        rows = con.execute(
            "SELECT concept_name, material_id, concept_type, concept_key, relevance, source_pages, evidence_spans, confidence FROM concepts"
        ).fetchall()
        con.close()
        assert len(rows) == 1
        assert rows[0][0] == "archival habitat"
        assert rows[0][1] == "aabbcc112233"
        assert rows[0][2] == "local"
        assert rows[0][3] == "archival habitat"  # concept_key (normalized)
        assert rows[0][4] == "high"
        assert "[2, 3]" in rows[0][5]  # source_pages JSON
        assert "the archival habitat" in rows[0][6]  # evidence_spans JSON
        assert rows[0][7] == 1.0  # confidence

    def test_bridge_candidate_concepts_are_preserved(self, repo):
        import sqlite3
        mat_dir = _add_material(repo)
        _write_concepts(mat_dir, [
            {"concept_name": "thermal performance in architecture", "concept_type": "bridge_candidate", "relevance": "medium"},
        ])
        rebuild_index()
        con = sqlite3.connect(str(repo / "indexes" / "search.sqlite"))
        row = con.execute("SELECT concept_type FROM concepts").fetchone()
        con.close()
        assert row[0] == "bridge_candidate"

    def test_concepts_fts_searchable(self, repo):
        import sqlite3
        mat_dir = _add_material(repo)
        _write_concepts(mat_dir, [
            {"concept_name": "postcolonial historiography", "relevance": "medium"},
        ])
        rebuild_index()
        con = sqlite3.connect(str(repo / "indexes" / "search.sqlite"))
        rows = con.execute(
            "SELECT co.material_id FROM concepts_fts JOIN concepts co ON concepts_fts.rowid = co.rowid WHERE concepts_fts MATCH 'postcolonial'"
        ).fetchall()
        con.close()
        assert len(rows) == 1

    def test_no_concepts_file_ok(self, repo):
        _add_material(repo)
        # No concepts.jsonl written — should not crash, concepts count = 0
        stats = rebuild_index()
        assert stats.concepts == 0

    def test_concept_deduplication_by_primary_key(self, repo):
        import sqlite3
        mat_dir = _add_material(repo)
        # Same concept_key (after normalization) + material_id — INSERT OR REPLACE deduplicates
        _write_concepts(mat_dir, [
            {"concept_name": "shared concept", "relevance": "high"},
            {"concept_name": "shared concept", "relevance": "medium"},
        ])
        rebuild_index()
        con = sqlite3.connect(str(repo / "indexes" / "search.sqlite"))
        count = con.execute("SELECT COUNT(*) FROM concepts").fetchone()[0]
        con.close()
        assert count == 1

    def test_concept_key_normalization_case(self, repo):
        """Case variants map to same concept_key, so only one row per material."""
        import sqlite3
        mat_dir = _add_material(repo)
        _write_concepts(mat_dir, [
            {"concept_name": "Archival Habitat", "relevance": "high"},
            {"concept_name": "archival habitat", "relevance": "medium"},
        ])
        rebuild_index()
        con = sqlite3.connect(str(repo / "indexes" / "search.sqlite"))
        count = con.execute("SELECT COUNT(*) FROM concepts").fetchone()[0]
        key = con.execute("SELECT concept_key FROM concepts").fetchone()[0]
        con.close()
        assert count == 1
        assert key == "archival habitat"

    def test_concept_key_normalization_plural(self, repo):
        """Trailing plural on last word normalizes to same concept_key."""
        import sqlite3
        mat_dir = _add_material(repo)
        _write_concepts(mat_dir, [
            {"concept_name": "archival habitats", "relevance": "high"},
            {"concept_name": "archival habitat", "relevance": "medium"},
        ])
        rebuild_index()
        con = sqlite3.connect(str(repo / "indexes" / "search.sqlite"))
        count = con.execute("SELECT COUNT(*) FROM concepts").fetchone()[0]
        con.close()
        assert count == 1

    def test_material_keywords_populated(self, repo):
        import sqlite3
        mat_dir = _add_material(repo)
        rebuild_index()
        con = sqlite3.connect(str(repo / "indexes" / "search.sqlite"))
        rows = con.execute("SELECT keyword FROM material_keywords WHERE material_id = 'aabbcc112233'").fetchall()
        con.close()
        keywords = {r[0] for r in rows}
        # Default _add_material writes keywords=["test keyword"]
        assert len(keywords) >= 1

    def test_material_authors_populated(self, repo):
        import sqlite3
        mat_dir = _add_material(repo)
        rebuild_index()
        con = sqlite3.connect(str(repo / "indexes" / "search.sqlite"))
        rows = con.execute("SELECT author FROM material_authors WHERE material_id = 'aabbcc112233'").fetchall()
        con.close()
        # Default _add_material writes authors=["Author One"]
        assert len(rows) >= 1
        assert rows[0][0] == "author one"  # normalized to lowercase

    def test_staleness_when_concepts_changed(self, repo):
        mat_dir = _add_material(repo)
        _write_concepts(mat_dir, [
            {"concept_name": "archival habitat", "relevance": "high"},
        ])
        ensure_index()
        # Backdate index
        import sqlite3
        con = sqlite3.connect(str(repo / "indexes" / "search.sqlite"))
        con.execute("UPDATE index_state SET built_at = '2020-01-01T00:00:00+00:00' WHERE id = 1")
        con.commit()
        con.close()
        # Modify concepts.jsonl
        _write_concepts(mat_dir, [
            {"concept_name": "archival habitat", "relevance": "high"},
            {"concept_name": "new concept xyz", "relevance": "medium"},
        ])
        rebuilt, _ = ensure_index()
        assert rebuilt is True


# ---------------------------------------------------------------------------
# Cluster graph indexing
# ---------------------------------------------------------------------------

def _write_clusters(root: Path, clusters: list[dict]) -> None:
    derived = root / "derived"
    derived.mkdir(exist_ok=True)
    lines = "\n".join(json.dumps(c) for c in clusters) + "\n"
    (derived / "bridge_concept_clusters.jsonl").write_text(lines)


def _write_bridge_clusters(root: Path, clusters: list[dict]) -> None:
    derived = root / "derived"
    derived.mkdir(exist_ok=True)
    lines = "\n".join(json.dumps(c) for c in clusters) + "\n"
    (derived / "bridge_concept_clusters.jsonl").write_text(lines)


_SAMPLE_CLUSTERS = [
    {
        "cluster_id": "concept_0001",
        "canonical_name": "Archive as Architectural Space",
        "slug": "archive-as-architectural-space",
        "aliases": ["archival habitat", "archive as space"],
        "material_ids": ["aabbcc112233", "ddeeff445566"],
        "source_concepts": [
            {
                "material_id": "aabbcc112233",
                "concept_name": "archival habitat",
                "relevance": "high",
                "source_pages": [2, 3],
                "evidence_spans": ["the archive as built environment"],
                "confidence": 0.95,
            },
            {
                "material_id": "ddeeff445566",
                "concept_name": "archive as space",
                "relevance": "medium",
                "source_pages": [1],
                "evidence_spans": ["spatial dimension of archives"],
                "confidence": 0.8,
            },
        ],
        "confidence": 0.9,
    },
    {
        "cluster_id": "concept_0002",
        "canonical_name": "Memory and Place",
        "slug": "memory-and-place",
        "aliases": ["memory palace", "spatial memory"],
        "material_ids": ["aabbcc112233"],
        "source_concepts": [
            {
                "material_id": "aabbcc112233",
                "concept_name": "memory palace",
                "relevance": "medium",
                "source_pages": [5],
                "evidence_spans": ["structures of collective memory"],
                "confidence": 0.7,
            },
        ],
        "confidence": 0.85,
    },
]


class TestClusterGraphIndexing:
    """concept_clusters, cluster_materials, cluster_relations tables."""

    def test_index_clusters_populates_tables(self, repo):
        """index_clusters() writes all three cluster tables."""
        import sqlite3
        _add_material(repo, "aabbcc112233")
        _add_material(repo, "ddeeff445566")
        _write_manifest(repo, ["aabbcc112233", "ddeeff445566"])
        rebuild_index()
        _write_clusters(repo, _SAMPLE_CLUSTERS)

        count = index_clusters()
        assert count == 2

        con = sqlite3.connect(str(repo / "indexes" / "search.sqlite"))
        clusters = con.execute("SELECT cluster_id, canonical_name, slug FROM concept_clusters ORDER BY cluster_id").fetchall()
        assert len(clusters) == 2
        assert clusters[0][1] == "Archive as Architectural Space"
        assert clusters[0][2] == "archive-as-architectural-space"

        members = con.execute("SELECT cluster_id, material_id, relevance FROM cluster_materials ORDER BY cluster_id, material_id").fetchall()
        assert len(members) == 3  # concept_0001 has 2, concept_0002 has 1

    def test_bridge_clusters_are_indexed_and_related(self, repo):
        import sqlite3
        from arquimedes.search import find_related

        _add_material(repo, "aabbcc112233")
        _add_material(repo, "ddeeff445566")
        _write_manifest(repo, ["aabbcc112233", "ddeeff445566"])
        rebuild_index()

        _write_clusters(repo, [])
        _write_bridge_clusters(repo, [{
            "cluster_id": "bridge_0001",
            "canonical_name": "Archive Space Framework",
            "slug": "archive-space-framework",
            "aliases": ["archival space framework"],
            "wiki_path": "wiki/shared/bridge-concepts/archive-space-framework.md",
            "material_ids": ["aabbcc112233", "ddeeff445566"],
            "source_concepts": [
                {
                    "material_id": "aabbcc112233",
                    "concept_name": "archival habitat",
                    "relevance": "high",
                    "source_pages": [2, 3],
                    "evidence_spans": ["the archive as built environment"],
                    "confidence": 0.95,
                },
                {
                    "material_id": "ddeeff445566",
                    "concept_name": "archive architecture",
                    "relevance": "medium",
                    "source_pages": [1],
                    "evidence_spans": ["spatial dimension of archives"],
                    "confidence": 0.8,
                },
            ],
            "confidence": 0.9,
        }])

        rebuild_index()

        con = sqlite3.connect(str(repo / "indexes" / "search.sqlite"))
        row = con.execute(
            "SELECT wiki_path FROM concept_clusters WHERE cluster_id='bridge_0001'"
        ).fetchone()
        members = con.execute(
            "SELECT cluster_id, material_id, relevance FROM cluster_materials ORDER BY cluster_id, material_id"
        ).fetchall()
        con.close()
        assert row is not None
        assert "bridge-concepts" in row[0]

        related = find_related("aabbcc112233")
        ids = [r.material_id for r in related]
        assert "ddeeff445566" in ids
        assert ("bridge_0001", "aabbcc112233", "high") in members
        assert ("bridge_0001", "ddeeff445566", "medium") in members

    def test_cluster_relations_derived_from_shared_material(self, repo):
        """Clusters sharing a material get mutual cluster_relations rows."""
        import sqlite3
        _add_material(repo)
        rebuild_index()
        _write_clusters(repo, _SAMPLE_CLUSTERS)
        index_clusters()

        con = sqlite3.connect(str(repo / "indexes" / "search.sqlite"))
        rels = con.execute(
            "SELECT cluster_id, related_cluster_id, shared_material_count FROM cluster_relations"
        ).fetchall()
        con.close()

        # concept_0001 and concept_0002 share aabbcc112233 → 2 symmetric rows
        assert len(rels) == 2
        assert ("concept_0001", "concept_0002", 1) in rels
        assert ("concept_0002", "concept_0001", 1) in rels

    def test_cluster_fts_searchable(self, repo):
        """concept_clusters_fts returns matches on canonical_name and aliases."""
        import sqlite3
        _add_material(repo)
        rebuild_index()
        _write_clusters(repo, _SAMPLE_CLUSTERS)
        index_clusters()

        con = sqlite3.connect(str(repo / "indexes" / "search.sqlite"))
        # Search by canonical name
        rows = con.execute(
            "SELECT cluster_id FROM concept_clusters_fts WHERE concept_clusters_fts MATCH 'architectural'"
        ).fetchall()
        assert any(r[0] == "concept_0001" for r in rows)

        # Search by alias
        rows = con.execute(
            "SELECT cluster_id FROM concept_clusters_fts WHERE concept_clusters_fts MATCH 'memory'"
        ).fetchall()
        assert any(r[0] == "concept_0002" for r in rows)
        con.close()

    def test_index_clusters_idempotent(self, repo):
        """Calling index_clusters() twice leaves exactly the expected rows."""
        import sqlite3
        _add_material(repo)
        rebuild_index()
        _write_clusters(repo, _SAMPLE_CLUSTERS)
        index_clusters()
        index_clusters()  # second call should replace, not duplicate

        con = sqlite3.connect(str(repo / "indexes" / "search.sqlite"))
        count = con.execute("SELECT COUNT(*) FROM concept_clusters").fetchone()[0]
        con.close()
        assert count == 2

    def test_rebuild_index_includes_clusters(self, repo):
        """rebuild_index() auto-populates clusters if derived/bridge_concept_clusters.jsonl exists."""
        import sqlite3
        _add_material(repo)
        _write_clusters(repo, _SAMPLE_CLUSTERS)
        rebuild_index()

        con = sqlite3.connect(str(repo / "indexes" / "search.sqlite"))
        count = con.execute("SELECT COUNT(*) FROM concept_clusters").fetchone()[0]
        con.close()
        assert count == 2

    def test_staleness_when_clusters_changed(self, repo):
        """ensure_index() rebuilds when bridge_concept_clusters.jsonl changes."""
        import sqlite3
        _add_material(repo)
        _write_clusters(repo, _SAMPLE_CLUSTERS)
        ensure_index()

        # Backdate index
        con = sqlite3.connect(str(repo / "indexes" / "search.sqlite"))
        con.execute("UPDATE index_state SET built_at = '2020-01-01T00:00:00+00:00' WHERE id = 1")
        con.commit()
        con.close()

        # Write different clusters
        _write_clusters(repo, [_SAMPLE_CLUSTERS[0]])  # remove one cluster
        rebuilt, _ = ensure_index()
        assert rebuilt is True


class TestEnsureIndexAndMemory:
    """ensure_index_and_memory() is the collaborator recovery path."""

    def test_returns_four_values(self, repo):
        _add_material(repo)
        _write_clusters(repo, _SAMPLE_CLUSTERS)
        from arquimedes.index import ensure_index_and_memory
        result = ensure_index_and_memory()
        assert len(result) == 4

    def test_index_rebuilt_when_stale(self, repo):
        from arquimedes.index import ensure_index_and_memory
        _add_material(repo)
        _write_clusters(repo, _SAMPLE_CLUSTERS)
        index_rebuilt, stats, _mb, _mc = ensure_index_and_memory()
        assert index_rebuilt is True
        assert stats is not None
        assert stats.materials == 1

    def test_memory_rebuilt_on_first_run(self, repo):
        from arquimedes.index import ensure_index_and_memory
        _add_material(repo)
        _write_clusters(repo, _SAMPLE_CLUSTERS)
        _index_rebuilt, _stats, memory_rebuilt, memory_counts = ensure_index_and_memory()
        assert memory_rebuilt is True
        assert memory_counts["clusters"] == 2

    def test_memory_skipped_when_current(self, repo):
        from arquimedes.index import ensure_index_and_memory, rebuild_index
        _add_material(repo)
        _write_clusters(repo, _SAMPLE_CLUSTERS)
        rebuild_index()
        # First ensure writes the memory stamp
        ensure_index_and_memory()
        # Second call: index current, memory current
        _index_rebuilt, _stats, memory_rebuilt, memory_counts = ensure_index_and_memory()
        assert memory_rebuilt is False
        assert memory_counts.get("skipped") is True

    def test_no_cluster_file_does_not_raise(self, repo):
        """Collaborator without cluster file — index rebuilds, memory bridge skips gracefully."""
        from arquimedes.index import ensure_index_and_memory
        _add_material(repo)
        # No cluster file — memory_ensure raises FileNotFoundError internally, handled
        _index_rebuilt, _stats, memory_rebuilt, memory_counts = ensure_index_and_memory()
        assert memory_rebuilt is False

    def test_no_lllm_call_required(self, repo, monkeypatch):
        """Collaborator path must work without any LLM call."""
        from arquimedes.index import ensure_index_and_memory
        import arquimedes.llm as llm_mod
        monkeypatch.setattr(llm_mod, "make_cli_llm_fn", lambda *a, **kw: (_ for _ in ()).throw(AssertionError("LLM called")))
        _add_material(repo)
        _write_clusters(repo, _SAMPLE_CLUSTERS)
        # Must not raise
        ensure_index_and_memory()
