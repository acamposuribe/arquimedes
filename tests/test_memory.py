"""Tests for memory.py — Phase 5.5 memory bridge."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from arquimedes.index import rebuild_index
from arquimedes.memory import memory_rebuild, memory_ensure


# ---------------------------------------------------------------------------
# Shared helpers (mirrors test_index.py setup)
# ---------------------------------------------------------------------------

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
        "raw_keywords": ["keyword"],
        "raw_document_type": "paper",
        "summary": {"value": "A test summary.", "provenance": {}},
        "keywords": {"value": ["keyword one"], "provenance": {}},
        "document_type": {"value": "paper", "provenance": {}},
        "facets": {},
        "_enrichment_stamp": {"prompt_version": "enrich-v1.0", "enrichment_schema_version": "1"},
    }
    mat_dir.mkdir(parents=True, exist_ok=True)
    (mat_dir / "meta.json").write_text(json.dumps(meta))


def _write_minimal_extracted(mat_dir: Path, mid: str) -> None:
    (mat_dir / "chunks.jsonl").write_text("")
    (mat_dir / "annotations.jsonl").write_text("")


def _write_manifest(root: Path, mids: list[str], domain: str = "research", collection: str = "_general") -> None:
    manifests = root / "manifests"
    manifests.mkdir(exist_ok=True)
    lines = [
        json.dumps({
            "material_id": mid,
            "file_hash": mid,
            "relative_path": f"Research/{mid}.pdf",
            "file_type": "pdf",
            "domain": domain,
            "collection": collection,
            "ingested_at": "2026-01-01T00:00:00+00:00",
        })
        for mid in mids
    ]
    (manifests / "materials.jsonl").write_text("\n".join(lines))


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


def _add_material(repo: Path, mid: str = "aabbcc112233", **meta_overrides) -> Path:
    mat_dir = repo / "extracted" / mid
    _write_meta(mat_dir, mid, **meta_overrides)
    _write_minimal_extracted(mat_dir, mid)
    _write_manifest(repo, [mid])
    return mat_dir


@pytest.fixture
def repo(tmp_path, monkeypatch):
    """Minimal repo with config.yaml."""
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "config.yaml").write_text("library_root: ~/dummy\n")
    (tmp_path / "indexes").mkdir()
    (tmp_path / "extracted").mkdir()
    monkeypatch.chdir(tmp_path)
    return tmp_path


# ---------------------------------------------------------------------------
# Sample cluster data
# ---------------------------------------------------------------------------

_MID_A = "aabbcc112233"
_MID_B = "ddeeff445566"

_SAMPLE_CLUSTERS = [
    {
        "cluster_id": "concept_0001",
        "canonical_name": "Archive as Architectural Space",
        "slug": "archive-as-architectural-space",
        "aliases": ["archival habitat", "archive architecture"],
        "confidence": 0.9,
        "source_concepts": [
            {
                "material_id": _MID_A,
                "concept_name": "archival habitat",
                "relevance": "high",
                "source_pages": [3, 5],
                "evidence_spans": ["the archive as built form"],
                "confidence": 0.88,
            },
            {
                "material_id": _MID_B,
                "concept_name": "archive architecture",
                "relevance": "medium",
                "source_pages": [7],
                "evidence_spans": ["structures of collective memory"],
                "confidence": 0.75,
            },
        ],
    },
    {
        "cluster_id": "concept_0002",
        "canonical_name": "Memory and Place",
        "slug": "memory-and-place",
        "aliases": ["memory palace", "spatial memory"],
        "confidence": 0.85,
        "source_concepts": [
            {
                "material_id": _MID_A,
                "concept_name": "memory palace",
                "relevance": "medium",
                "source_pages": [2],
                "evidence_spans": ["spaces of remembrance"],
                "confidence": 0.80,
            },
        ],
    },
]


def _setup_two_materials_with_clusters(repo: Path) -> None:
    """Populate repo with two materials, rebuild index, write clusters, run memory_rebuild."""
    for mid in [_MID_A, _MID_B]:
        _add_material(repo, mid=mid)
    _write_manifest(repo, [_MID_A, _MID_B])
    rebuild_index()
    _write_clusters(repo, _SAMPLE_CLUSTERS)
    memory_rebuild()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestMemoryRebuild:
    def test_raises_without_index(self, repo):
        _write_clusters(repo, _SAMPLE_CLUSTERS)
        with pytest.raises(FileNotFoundError, match="index"):
            memory_rebuild()

    def test_raises_without_clusters(self, repo):
        _add_material(repo)
        rebuild_index()
        with pytest.raises(FileNotFoundError, match="[Cc]luster"):
            memory_rebuild()

    def test_returns_counts(self, repo):
        _setup_two_materials_with_clusters(repo)
        counts = memory_rebuild()
        assert counts["clusters"] == 2
        assert counts["aliases"] > 0
        assert counts["wiki_pages"] > 0

    def test_writes_stamp_file(self, repo):
        _setup_two_materials_with_clusters(repo)
        stamp_path = repo / "derived" / "memory_bridge_stamp.json"
        assert stamp_path.exists()
        stamp = json.loads(stamp_path.read_text())
        assert "built_at" in stamp
        assert "clusters_fingerprint" in stamp
        assert "manifest_fingerprint" in stamp


class TestAliasTableRebuild:
    def test_alias_rows_written(self, repo):
        _setup_two_materials_with_clusters(repo)
        con = sqlite3.connect(str(repo / "indexes" / "search.sqlite"))
        rows = con.execute(
            "SELECT cluster_id, alias FROM concept_cluster_aliases ORDER BY cluster_id, alias"
        ).fetchall()
        con.close()

        aliases = {(r[0], r[1]) for r in rows}
        assert ("concept_0001", "archival habitat") in aliases
        assert ("concept_0001", "archive architecture") in aliases
        assert ("concept_0002", "memory palace") in aliases
        assert ("concept_0002", "spatial memory") in aliases


class TestAliasTable:
    def test_alias_rows_replaced_on_rebuild(self, repo):
        _setup_two_materials_with_clusters(repo)
        # Write minimal single-cluster file and rebuild
        _write_clusters(repo, [_SAMPLE_CLUSTERS[0]])
        memory_rebuild()
        con = sqlite3.connect(str(repo / "indexes" / "search.sqlite"))
        rows = con.execute("SELECT alias FROM concept_cluster_aliases").fetchall()
        con.close()
        # Only aliases from concept_0001 remain
        assert len(rows) == 2


class TestClusterMaterialEvidence:
    def test_confidence_and_wiki_path_populated(self, repo):
        _setup_two_materials_with_clusters(repo)
        con = sqlite3.connect(str(repo / "indexes" / "search.sqlite"))
        row = con.execute(
            "SELECT confidence, material_wiki_path FROM cluster_materials WHERE cluster_id=? AND material_id=?",
            ("concept_0001", _MID_A),
        ).fetchone()
        con.close()
        assert row is not None
        assert row[0] == pytest.approx(0.88, abs=0.01)
        assert _MID_A in row[1]
        assert row[1].endswith(".md")

    def test_material_wiki_path_structure(self, repo):
        _setup_two_materials_with_clusters(repo)
        con = sqlite3.connect(str(repo / "indexes" / "search.sqlite"))
        rows = con.execute(
            "SELECT material_wiki_path FROM cluster_materials WHERE material_id=?",
            (_MID_A,),
        ).fetchall()
        con.close()
        for row in rows:
            path = row[0]
            assert path.startswith("wiki/")
            assert path.endswith(f"{_MID_A}.md")


class TestClusterRelationsSharedMaterialIds:
    def test_shared_material_ids_populated(self, repo):
        _setup_two_materials_with_clusters(repo)
        con = sqlite3.connect(str(repo / "indexes" / "search.sqlite"))
        row = con.execute(
            "SELECT shared_material_ids FROM cluster_relations WHERE cluster_id=? AND related_cluster_id=?",
            ("concept_0001", "concept_0002"),
        ).fetchone()
        con.close()
        assert row is not None
        shared = json.loads(row[0])
        assert _MID_A in shared


class TestWikiPages:
    def test_local_concept_index_written(self, repo):
        _setup_two_materials_with_clusters(repo)
        con = sqlite3.connect(str(repo / "indexes" / "search.sqlite"))
        concept_index = con.execute(
            "SELECT page_id, path FROM wiki_pages WHERE page_type='concept_index'"
        ).fetchall()
        con.close()

        ids = [r[0] for r in concept_index]
        paths = [r[1] for r in concept_index]
        assert "local" in ids
        assert any("shared/concepts/_index.md" in p for p in paths)

    def test_material_pages_written(self, repo):
        _setup_two_materials_with_clusters(repo)
        con = sqlite3.connect(str(repo / "indexes" / "search.sqlite"))
        mat_pages = con.execute(
            "SELECT page_id, path FROM wiki_pages WHERE page_type='material'"
        ).fetchall()
        con.close()

        ids = [r[0] for r in mat_pages]
        assert _MID_A in ids
        assert _MID_B in ids

    def test_material_page_path_includes_domain(self, repo):
        _setup_two_materials_with_clusters(repo)
        con = sqlite3.connect(str(repo / "indexes" / "search.sqlite"))
        row = con.execute(
            "SELECT path FROM wiki_pages WHERE page_type='material' AND page_id=?",
            (_MID_A,),
        ).fetchone()
        con.close()
        assert row is not None
        path = row[0]
        assert path.startswith("wiki/research/")
        assert path.endswith(f"{_MID_A}.md")

    def test_wiki_pages_replaced_on_rebuild(self, repo):
        _setup_two_materials_with_clusters(repo)
        count_before = sqlite3.connect(str(repo / "indexes" / "search.sqlite")).execute(
            "SELECT COUNT(*) FROM wiki_pages"
        ).fetchone()[0]
        memory_rebuild()
        count_after = sqlite3.connect(str(repo / "indexes" / "search.sqlite")).execute(
            "SELECT COUNT(*) FROM wiki_pages"
        ).fetchone()[0]
        assert count_after == count_before

    def test_bridge_concept_pages_written(self, repo):
        _setup_two_materials_with_clusters(repo)
        _write_bridge_clusters(repo, [{
            "cluster_id": "bridge_0001",
            "canonical_name": "Archive Space Framework",
            "slug": "archive-space-framework",
            "aliases": ["archival space framework"],
            "wiki_path": "wiki/shared/bridge-concepts/archive-space-framework.md",
            "confidence": 0.9,
            "material_ids": [
                _MID_A,
                _MID_B,
            ],
            "source_concepts": [
                {
                    "material_id": _MID_A,
                    "concept_name": "archival habitat",
                    "relevance": "high",
                    "source_pages": [3],
                    "evidence_spans": ["the archive as built form"],
                    "confidence": 0.88,
                },
                {
                    "material_id": _MID_B,
                    "concept_name": "archive architecture",
                    "relevance": "medium",
                    "source_pages": [7],
                    "evidence_spans": ["structures of collective memory"],
                    "confidence": 0.75,
                },
            ],
        }])
        memory_rebuild()

        con = sqlite3.connect(str(repo / "indexes" / "search.sqlite"))
        row = con.execute(
            "SELECT path FROM wiki_pages WHERE page_type='concept' AND page_id='bridge_0001'"
        ).fetchone()
        con.close()

        assert row is not None
        assert "bridge-concepts" in row[0]


class TestReflectionTables:
    def test_reflection_records_are_queryable(self, repo):
        _setup_two_materials_with_clusters(repo)
        lint_dir = repo / "derived" / "lint"
        lint_dir.mkdir(parents=True, exist_ok=True)
        (lint_dir / "cluster_reviews.jsonl").write_text(
            json.dumps({
                "review_id": "concept_0001:0:merge",
                "cluster_id": "concept_0001",
                "finding_type": "merge",
                "severity": "medium",
                "recommendation": "Consider merging.",
                "affected_material_ids": [_MID_A, _MID_B],
                "affected_concept_names": ["archival habitat"],
                "evidence": ["shared archive frame"],
                "input_fingerprint": "abc",
                "wiki_path": "wiki/shared/bridge-concepts/archive-as-architectural-space.md",
            }) + "\n",
            encoding="utf-8",
        )
        (lint_dir / "concept_reflections.jsonl").write_text(
            json.dumps({
                "cluster_id": "concept_0001",
                "slug": "archive-as-architectural-space",
                "canonical_name": "Archive as Architectural Space",
                "main_takeaways": ["Shared concern with spatial archives"],
                "main_tensions": ["Theory vs use"],
                "open_questions": ["What is the archive doing?"],
                "why_this_concept_matters": "It shapes the corpus.",
                "supporting_material_ids": [_MID_A, _MID_B],
                "supporting_evidence": ["shared archive frame"],
                "input_fingerprint": "def",
                "wiki_path": "wiki/shared/bridge-concepts/archive-as-architectural-space.md",
            }) + "\n",
            encoding="utf-8",
        )
        (lint_dir / "collection_reflections.jsonl").write_text(
            json.dumps({
                "collection_key": "research/_general",
                "domain": "research",
                "collection": "_general",
                "main_takeaways": ["The collection centers archival space."],
                "main_tensions": ["Theory vs use"],
                "important_material_ids": [_MID_A, _MID_B],
                "important_cluster_ids": ["concept_0001"],
                "open_questions": ["What else is in the archive?"],
                "input_fingerprint": "ghi",
                "wiki_path": "wiki/research/_general/_index.md",
            }) + "\n",
            encoding="utf-8",
        )
        (lint_dir / "graph_findings.jsonl").write_text(
            json.dumps({
                "finding_id": "graph:0",
                "finding_type": "bridge",
                "severity": "low",
                "summary": "Add a missing bridge link.",
                "details": "The graph could connect these materials more directly.",
                "affected_material_ids": [_MID_A, _MID_B],
                "affected_cluster_ids": ["concept_0001"],
                "candidate_future_sources": ["oral history"],
                "candidate_bridge_links": ["archive and memory"],
                "input_fingerprint": "jkl",
            }) + "\n",
            encoding="utf-8",
        )
        memory_rebuild()

        con = sqlite3.connect(str(repo / "indexes" / "search.sqlite"))
        counts = {
            "cluster_reviews": con.execute("SELECT COUNT(*) FROM cluster_reviews").fetchone()[0],
            "concept_reflections": con.execute("SELECT COUNT(*) FROM concept_reflections").fetchone()[0],
            "collection_reflections": con.execute("SELECT COUNT(*) FROM collection_reflections").fetchone()[0],
            "graph_findings": con.execute("SELECT COUNT(*) FROM graph_findings").fetchone()[0],
        }
        con.close()

        assert counts == {
            "cluster_reviews": 1,
            "concept_reflections": 1,
            "collection_reflections": 1,
            "graph_findings": 1,
        }


class TestConceptClustersExtraColumns:
    def test_wiki_path_populated(self, repo):
        _setup_two_materials_with_clusters(repo)
        con = sqlite3.connect(str(repo / "indexes" / "search.sqlite"))
        row = con.execute(
            "SELECT wiki_path FROM concept_clusters WHERE cluster_id=?",
            ("concept_0001",),
        ).fetchone()
        con.close()
        assert row is not None
        assert "archive-as-architectural-space" in row[0]
        assert row[0].endswith(".md")

    def test_material_count_populated(self, repo):
        _setup_two_materials_with_clusters(repo)
        con = sqlite3.connect(str(repo / "indexes" / "search.sqlite"))
        row = con.execute(
            "SELECT material_count FROM concept_clusters WHERE cluster_id=?",
            ("concept_0001",),
        ).fetchone()
        con.close()
        # concept_0001 has 2 source materials
        assert row is not None
        assert row[0] == 2


class TestMemoryEnsure:
    def test_ensure_rebuilds_when_missing(self, repo):
        _setup_two_materials_with_clusters(repo)
        # Remove stamp to force rebuild
        (repo / "derived" / "memory_bridge_stamp.json").unlink()
        rebuilt, counts = memory_ensure()
        assert rebuilt is True
        assert counts["clusters"] == 2

    def test_ensure_skips_when_current(self, repo):
        _setup_two_materials_with_clusters(repo)
        # Already rebuilt + stamp written; ensure should skip
        rebuilt, counts = memory_ensure()
        assert rebuilt is False
        assert counts.get("skipped") is True

    def test_ensure_rebuilds_when_clusters_change(self, repo):
        _setup_two_materials_with_clusters(repo)
        # Overwrite cluster file with different content
        _write_clusters(repo, [_SAMPLE_CLUSTERS[0]])
        rebuilt, counts = memory_ensure()
        assert rebuilt is True

    def test_ensure_rebuilds_when_manifest_changes(self, repo):
        _setup_two_materials_with_clusters(repo)
        # Add another entry to manifest
        mid3 = "112233445566"
        _add_material(repo, mid=mid3)
        _write_manifest(repo, [_MID_A, _MID_B, mid3])
        rebuilt, _ = memory_ensure()
        assert rebuilt is True
