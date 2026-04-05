"""Tests for enrich_stamps: fingerprinting, stamp creation, and staleness tracking."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from arquimedes.enrich_stamps import (
    canonical_hash,
    chunk_fingerprint,
    document_fingerprint,
    figure_fingerprint,
    is_stale,
    make_stamp,
    read_chunk_stamps,
    read_document_stamp,
    read_figure_stamp,
    single_chunk_fingerprint,
    write_chunk_stamps,
    write_document_stamp,
    write_figure_stamp,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_extraction(tmp_path: Path, **overrides) -> Path:
    """Create a minimal extraction directory with required files."""
    d = tmp_path / "material"
    d.mkdir(exist_ok=True)
    meta = {
        "material_id": "abc123",
        "title": "Urban Housing",
        "authors": ["Alice"],
        "year": "2022",
        "raw_keywords": ["housing"],
        "raw_document_type": "regulation",
        "domain": "practice",
        "collection": "regulations",
        "page_count": 2,
    }
    meta.update(overrides.get("meta_extra", {}))
    (d / "meta.json").write_text(json.dumps(meta), encoding="utf-8")
    (d / "pages.jsonl").write_text(
        overrides.get("pages", '{"page": 1, "text": "Hello"}\n'), encoding="utf-8"
    )
    (d / "chunks.jsonl").write_text(
        overrides.get("chunks", '{"chunk_id": "c0", "text": "Hello", "source_pages": [1], "emphasized": false}\n'),
        encoding="utf-8",
    )
    if "annotations" in overrides:
        (d / "annotations.jsonl").write_text(overrides["annotations"], encoding="utf-8")
    if "toc" in overrides:
        (d / "toc.json").write_text(overrides["toc"], encoding="utf-8")
    return d


def _doc_context() -> dict:
    return {"title": "Urban Housing", "raw_document_type": "regulation", "headings": []}


# ---------------------------------------------------------------------------
# canonical_hash
# ---------------------------------------------------------------------------


class TestCanonicalHash:
    def test_deterministic_and_hex_format(self):
        h1 = canonical_hash({"a": 1, "b": 2})
        h2 = canonical_hash({"b": 2, "a": 1})
        assert h1 == h2  # key order doesn't matter
        assert len(h1) == 16
        int(h1, 16)  # valid hex

    def test_different_inputs_differ(self):
        assert canonical_hash("hello") != canonical_hash("world")
        assert canonical_hash({"x": 1}) != canonical_hash({"x": 2})


# ---------------------------------------------------------------------------
# document_fingerprint
# ---------------------------------------------------------------------------


class TestDocumentFingerprint:
    def test_deterministic(self, tmp_path):
        d = _write_extraction(tmp_path)
        assert document_fingerprint(d) == document_fingerprint(d)

    def test_changes_on_input_change(self, tmp_path):
        d = _write_extraction(tmp_path)
        fp1 = document_fingerprint(d)
        # Change chunks
        (d / "chunks.jsonl").write_text('{"chunk_id": "c0", "text": "Changed", "source_pages": [1]}\n')
        assert document_fingerprint(d) != fp1

    def test_enriched_meta_excluded(self, tmp_path):
        d = _write_extraction(tmp_path)
        fp1 = document_fingerprint(d)
        # Add enriched field to meta
        meta = json.loads((d / "meta.json").read_text())
        meta["summary"] = {"value": "A study on housing"}
        (d / "meta.json").write_text(json.dumps(meta))
        assert document_fingerprint(d) == fp1

    def test_unknown_raw_document_type_is_stable_fallback(self, tmp_path):
        d = _write_extraction(tmp_path, meta_extra={"raw_document_type": ""})
        fp_blank = document_fingerprint(d)
        meta = json.loads((d / "meta.json").read_text())
        meta["raw_document_type"] = "unknown"
        (d / "meta.json").write_text(json.dumps(meta))
        assert document_fingerprint(d) == fp_blank


# ---------------------------------------------------------------------------
# chunk_fingerprint
# ---------------------------------------------------------------------------


class TestChunkFingerprint:
    def test_deterministic(self, tmp_path):
        d = _write_extraction(tmp_path)
        ctx = _doc_context()
        assert chunk_fingerprint(d, ctx) == chunk_fingerprint(d, ctx)

    def test_changes_on_context_change(self, tmp_path):
        d = _write_extraction(tmp_path)
        fp1 = chunk_fingerprint(d, _doc_context())
        fp2 = chunk_fingerprint(d, {**_doc_context(), "title": "Different Title"})
        assert fp1 != fp2

    def test_blank_and_unknown_raw_document_type_match(self, tmp_path):
        d = _write_extraction(tmp_path, meta_extra={"raw_document_type": ""})
        blank_ctx = {"title": "Urban Housing", "raw_document_type": "", "headings": []}
        unknown_ctx = {"title": "Urban Housing", "raw_document_type": "unknown", "headings": []}
        assert chunk_fingerprint(d, blank_ctx) == chunk_fingerprint(d, unknown_ctx)


# ---------------------------------------------------------------------------
# single_chunk_fingerprint
# ---------------------------------------------------------------------------


class TestSingleChunkFingerprint:
    def test_deterministic(self):
        chunk = {"chunk_id": "c0", "text": "Hello", "source_pages": [1], "emphasized": False}
        fp1 = single_chunk_fingerprint(chunk, [], _doc_context())
        fp2 = single_chunk_fingerprint(chunk, [], _doc_context())
        assert fp1 == fp2

    def test_sensitive_to_text_and_annotations(self):
        chunk = {"chunk_id": "c0", "text": "Hello", "source_pages": [1], "emphasized": False}
        ctx = _doc_context()
        fp_base = single_chunk_fingerprint(chunk, [], ctx)
        # Different text
        chunk2 = {**chunk, "text": "Goodbye"}
        assert single_chunk_fingerprint(chunk2, [], ctx) != fp_base
        # Annotation on matching page
        ann = [{"page": 1, "quoted_text": "Hello", "comment": "note"}]
        assert single_chunk_fingerprint(chunk, ann, ctx) != fp_base
        # Annotation on non-matching page (no change)
        ann_other = [{"page": 99, "quoted_text": "x", "comment": ""}]
        assert single_chunk_fingerprint(chunk, ann_other, ctx) == fp_base

    def test_blank_and_unknown_raw_document_type_match(self):
        chunk = {"chunk_id": "c0", "text": "Hello", "source_pages": [1], "emphasized": False}
        blank_ctx = {"title": "Urban Housing", "raw_document_type": "", "headings": []}
        unknown_ctx = {"title": "Urban Housing", "raw_document_type": "unknown", "headings": []}
        assert single_chunk_fingerprint(chunk, [], blank_ctx) == single_chunk_fingerprint(chunk, [], unknown_ctx)


# ---------------------------------------------------------------------------
# figure_fingerprint
# ---------------------------------------------------------------------------


class TestFigureFingerprint:
    def test_deterministic(self, tmp_path):
        img = tmp_path / "fig.png"
        img.write_bytes(b"\x89PNG fake data")
        sidecar = {"source_page": 1, "bbox": [0, 0, 100, 100], "extraction_method": "rule"}
        fp1 = figure_fingerprint(sidecar, img, "page text", ["caption"], _doc_context())
        fp2 = figure_fingerprint(sidecar, img, "page text", ["caption"], _doc_context())
        assert fp1 == fp2

    def test_changes_on_image_or_text_change(self, tmp_path):
        img = tmp_path / "fig.png"
        img.write_bytes(b"\x89PNG fake data")
        sidecar = {"source_page": 1, "bbox": [0, 0, 100, 100], "extraction_method": "rule"}
        fp1 = figure_fingerprint(sidecar, img, "page text", ["caption"], _doc_context())
        # Different image
        img.write_bytes(b"\x89PNG different")
        assert figure_fingerprint(sidecar, img, "page text", ["caption"], _doc_context()) != fp1
        # Different page text
        img.write_bytes(b"\x89PNG fake data")
        assert figure_fingerprint(sidecar, img, "changed text", ["caption"], _doc_context()) != fp1


# ---------------------------------------------------------------------------
# make_stamp / is_stale
# ---------------------------------------------------------------------------


class TestStampAndStaleness:
    def test_make_stamp_returns_four_fields(self):
        stamp = make_stamp("v1", "claude-4", "1.0", "abcd1234")
        assert set(stamp.keys()) == {"prompt_version", "model", "enrichment_schema_version", "input_fingerprint"}

    def test_stale_when_none_or_different(self):
        current = make_stamp("v1", "claude-4", "1.0", "abcd1234")
        assert is_stale(None, current) is True
        assert is_stale(current, current) is False
        # Staleness-driving fields
        for field in ("prompt_version", "enrichment_schema_version", "input_fingerprint"):
            modified = {**current, field: "different"}
            assert is_stale(modified, current) is True

    def test_model_change_does_not_trigger_stale(self):
        """Model is audit-only; changing it should NOT cause staleness."""
        current = make_stamp("v1", "claude-4", "1.0", "abcd1234")
        different_model = {**current, "model": "codex"}
        assert is_stale(different_model, current) is False


# ---------------------------------------------------------------------------
# Stamp I/O round-trips
# ---------------------------------------------------------------------------


class TestStampIO:
    def test_document_stamp_round_trip(self, tmp_path):
        d = _write_extraction(tmp_path)
        assert read_document_stamp(d) is None
        stamp = make_stamp("v1", "model", "1.0", "fp123")
        write_document_stamp(d, stamp)
        assert read_document_stamp(d) == stamp
        # Overwrite
        stamp2 = make_stamp("v2", "model2", "2.0", "fp456")
        write_document_stamp(d, stamp2)
        assert read_document_stamp(d) == stamp2

    def test_chunk_stamps_round_trip(self, tmp_path):
        d = _write_extraction(tmp_path)
        assert read_chunk_stamps(d) == {}
        stamps = {"c0": make_stamp("v1", "m", "1.0", "fp1"), "c1": make_stamp("v1", "m", "1.0", "fp2")}
        write_chunk_stamps(d, stamps)
        assert read_chunk_stamps(d) == stamps

    def test_figure_stamp_round_trip(self, tmp_path):
        fig_path = tmp_path / "fig_0001.json"
        fig_path.write_text(json.dumps({"source_page": 1, "bbox": [0, 0, 100, 100]}))
        assert read_figure_stamp(fig_path) is None
        stamp = make_stamp("v1", "m", "1.0", "fp1")
        write_figure_stamp(fig_path, stamp)
        assert read_figure_stamp(fig_path) == stamp
        # Original fields preserved
        data = json.loads(fig_path.read_text())
        assert data["source_page"] == 1
