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
