"""Tests for enrich_stamps: fingerprinting and staleness tracking."""

from __future__ import annotations

import json
import hashlib
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
    write_chunk_stamps,
    write_document_stamp,
    write_figure_stamp,
)


# ---------------------------------------------------------------------------
# Fixtures — minimal extracted directory
# ---------------------------------------------------------------------------


def _write_meta(output_dir: Path, **overrides) -> dict:
    """Write a minimal meta.json and return the dict."""
    meta = {
        "material_id": "abc123",
        "file_hash": "deadbeef",
        "source_path": "Practice/Regulations/test.pdf",
        "title": "Test Document",
        "authors": ["Alice", "Bob"],
        "year": "2024",
        "language": "en",
        "source_url": "",
        "page_count": 3,
        "file_type": "pdf",
        "domain": "practice",
        "collection": "regulations",
        "ingested_at": "2024-01-01T00:00:00+00:00",
        "raw_keywords": ["architecture", "regulation"],
        "raw_document_type": "regulation",
    }
    meta.update(overrides)
    (output_dir / "meta.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    return meta


def _write_pages(output_dir: Path, pages: list[dict] | None = None) -> None:
    """Write pages.jsonl."""
    if pages is None:
        pages = [
            {"page_number": 1, "text": "Page one text.", "headings": ["Chapter 1"],
             "section_boundaries": [], "figure_refs": [], "table_refs": [],
             "thumbnail_path": "", "has_annotations": False, "annotation_ids": []},
            {"page_number": 2, "text": "Page two text.", "headings": [],
             "section_boundaries": [], "figure_refs": [], "table_refs": [],
             "thumbnail_path": "", "has_annotations": False, "annotation_ids": []},
        ]
    lines = "\n".join(json.dumps(p, ensure_ascii=False) for p in pages)
    (output_dir / "pages.jsonl").write_text(lines, encoding="utf-8")


def _write_chunks(output_dir: Path, chunks: list[dict] | None = None) -> None:
    """Write chunks.jsonl."""
    if chunks is None:
        chunks = [
            {"chunk_id": "abc123-c0", "text": "Page one text.", "source_pages": [1],
             "emphasized": False},
            {"chunk_id": "abc123-c1", "text": "Page two text.", "source_pages": [2],
             "emphasized": True},
        ]
    lines = "\n".join(json.dumps(c, ensure_ascii=False) for c in chunks)
    (output_dir / "chunks.jsonl").write_text(lines, encoding="utf-8")


def _write_annotations(output_dir: Path, annotations: list[dict] | None = None) -> None:
    """Write annotations.jsonl."""
    if annotations is None:
        annotations = [
            {"annotation_id": "ann-1", "type": "highlight", "page": 2,
             "quoted_text": "Page two text.", "comment": "", "color": "yellow",
             "rect": [0.1, 0.2, 0.3, 0.4]},
        ]
    lines = "\n".join(json.dumps(a, ensure_ascii=False) for a in annotations)
    (output_dir / "annotations.jsonl").write_text(lines, encoding="utf-8")


def _write_toc(output_dir: Path, toc: list[dict] | None = None) -> None:
    """Write toc.json."""
    if toc is None:
        toc = [{"title": "Chapter 1", "page": 1, "level": 1}]
    (output_dir / "toc.json").write_text(json.dumps(toc, ensure_ascii=False), encoding="utf-8")


@pytest.fixture
def extracted_dir(tmp_path: Path) -> Path:
    """Minimal extracted directory with all files present."""
    d = tmp_path / "abc123"
    d.mkdir()
    _write_meta(d)
    _write_pages(d)
    _write_chunks(d)
    _write_annotations(d)
    _write_toc(d)
    return d


@pytest.fixture
def figure_dir(tmp_path: Path) -> Path:
    """Minimal figures directory with sidecar + image."""
    d = tmp_path / "figures"
    d.mkdir()
    return d


# ---------------------------------------------------------------------------
# canonical_hash tests
# ---------------------------------------------------------------------------


class TestCanonicalHash:
    def test_determinism_same_input(self):
        """Same input always produces the same hash."""
        h1 = canonical_hash({"a": 1, "b": [1, 2]}, "hello")
        h2 = canonical_hash({"a": 1, "b": [1, 2]}, "hello")
        assert h1 == h2

    def test_determinism_key_order_irrelevant(self):
        """Dicts with different key insertion order hash the same (sort_keys=True)."""
        h1 = canonical_hash({"b": 2, "a": 1})
        h2 = canonical_hash({"a": 1, "b": 2})
        assert h1 == h2

    def test_different_input_different_hash(self):
        """Different inputs produce different hashes."""
        h1 = canonical_hash({"a": 1})
        h2 = canonical_hash({"a": 2})
        assert h1 != h2

    def test_different_part_count_different_hash(self):
        """Adding an extra part changes the hash."""
        h1 = canonical_hash("hello")
        h2 = canonical_hash("hello", "world")
        assert h1 != h2

    def test_returns_16_char_hex(self):
        """Output is exactly 16 lowercase hex characters."""
        h = canonical_hash("test")
        assert len(h) == 16
        assert all(c in "0123456789abcdef" for c in h)

    def test_empty_input(self):
        """No parts still produces a consistent 16-char hash."""
        h1 = canonical_hash()
        h2 = canonical_hash()
        assert h1 == h2
        assert len(h1) == 16

    def test_non_ascii_preserved(self):
        """Non-ASCII strings are handled (ensure_ascii=False)."""
        h1 = canonical_hash("arquitectura")
        h2 = canonical_hash("arquitectura")
        assert h1 == h2
        h3 = canonical_hash("architecture")
        assert h1 != h3


# ---------------------------------------------------------------------------
# document_fingerprint tests
# ---------------------------------------------------------------------------


class TestDocumentFingerprint:
    def test_determinism(self, extracted_dir: Path):
        """Same directory → same fingerprint."""
        fp1 = document_fingerprint(extracted_dir)
        fp2 = document_fingerprint(extracted_dir)
        assert fp1 == fp2

    def test_returns_16_char_hex(self, extracted_dir: Path):
        fp = document_fingerprint(extracted_dir)
        assert len(fp) == 16
        assert all(c in "0123456789abcdef" for c in fp)

    def test_changes_when_raw_meta_changes(self, extracted_dir: Path):
        fp1 = document_fingerprint(extracted_dir)
        _write_meta(extracted_dir, title="Different Title")
        fp2 = document_fingerprint(extracted_dir)
        assert fp1 != fp2

    def test_changes_when_pages_change(self, extracted_dir: Path):
        fp1 = document_fingerprint(extracted_dir)
        _write_pages(extracted_dir, pages=[
            {"page_number": 1, "text": "Completely different text.", "headings": [],
             "section_boundaries": [], "figure_refs": [], "table_refs": [],
             "thumbnail_path": "", "has_annotations": False, "annotation_ids": []}
        ])
        fp2 = document_fingerprint(extracted_dir)
        assert fp1 != fp2

    def test_changes_when_chunks_change(self, extracted_dir: Path):
        fp1 = document_fingerprint(extracted_dir)
        _write_chunks(extracted_dir, chunks=[
            {"chunk_id": "abc123-c0", "text": "New chunk text.", "source_pages": [1],
             "emphasized": False},
        ])
        fp2 = document_fingerprint(extracted_dir)
        assert fp1 != fp2

    def test_changes_when_annotations_change(self, extracted_dir: Path):
        fp1 = document_fingerprint(extracted_dir)
        _write_annotations(extracted_dir, annotations=[
            {"annotation_id": "ann-new", "type": "note", "page": 1,
             "quoted_text": "New annotation.", "comment": "important", "color": "red",
             "rect": [0.0, 0.0, 1.0, 1.0]},
        ])
        fp2 = document_fingerprint(extracted_dir)
        assert fp1 != fp2

    def test_no_annotations_file_uses_empty_fallback(self, tmp_path: Path):
        """Missing annotations.jsonl uses '[]' fallback — fingerprint is consistent."""
        d = tmp_path / "noann"
        d.mkdir()
        _write_meta(d)
        _write_pages(d)
        _write_chunks(d)
        # No annotations.jsonl
        fp1 = document_fingerprint(d)
        fp2 = document_fingerprint(d)
        assert fp1 == fp2

    def test_no_toc_file_uses_empty_fallback(self, tmp_path: Path):
        """Missing toc.json uses '[]' fallback — fingerprint is consistent."""
        d = tmp_path / "notoc"
        d.mkdir()
        _write_meta(d)
        _write_pages(d)
        _write_chunks(d)
        # No toc.json
        fp1 = document_fingerprint(d)
        fp2 = document_fingerprint(d)
        assert fp1 == fp2

    def test_enriched_fields_in_meta_do_not_change_fingerprint(self, extracted_dir: Path):
        """Enriched fields (document_type, summary, etc.) are excluded from projection."""
        fp1 = document_fingerprint(extracted_dir)
        # Add an enriched field directly to meta.json
        meta_path = extracted_dir / "meta.json"
        data = json.loads(meta_path.read_text())
        data["document_type"] = {
            "value": "regulation",
            "provenance": {
                "source_pages": [], "evidence_spans": [], "model": "claude",
                "prompt_version": "v1", "confidence": 0.95, "enriched_at": "2024-01-01T00:00:00+00:00"
            }
        }
        meta_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        fp2 = document_fingerprint(extracted_dir)
        assert fp1 == fp2


# ---------------------------------------------------------------------------
# chunk_fingerprint tests
# ---------------------------------------------------------------------------


class TestChunkFingerprint:
    def test_determinism(self, extracted_dir: Path):
        ctx = {"title": "Test Document", "raw_document_type": "regulation",
               "headings": ["Chapter 1"]}
        fp1 = chunk_fingerprint(extracted_dir, ctx)
        fp2 = chunk_fingerprint(extracted_dir, ctx)
        assert fp1 == fp2

    def test_returns_16_char_hex(self, extracted_dir: Path):
        ctx = {"title": "Test Document", "raw_document_type": "regulation", "headings": []}
        fp = chunk_fingerprint(extracted_dir, ctx)
        assert len(fp) == 16

    def test_changes_when_chunks_change(self, extracted_dir: Path):
        ctx = {"title": "Test Document", "raw_document_type": "regulation", "headings": []}
        fp1 = chunk_fingerprint(extracted_dir, ctx)
        _write_chunks(extracted_dir, chunks=[
            {"chunk_id": "abc123-c0", "text": "Changed text.", "source_pages": [1],
             "emphasized": True},
        ])
        fp2 = chunk_fingerprint(extracted_dir, ctx)
        assert fp1 != fp2

    def test_changes_when_doc_context_changes(self, extracted_dir: Path):
        ctx1 = {"title": "Test Document", "raw_document_type": "regulation", "headings": []}
        ctx2 = {"title": "Test Document", "raw_document_type": "paper", "headings": []}
        fp1 = chunk_fingerprint(extracted_dir, ctx1)
        fp2 = chunk_fingerprint(extracted_dir, ctx2)
        assert fp1 != fp2

    def test_changes_when_annotations_change(self, extracted_dir: Path):
        ctx = {"title": "Test Document", "raw_document_type": "regulation", "headings": []}
        fp1 = chunk_fingerprint(extracted_dir, ctx)
        _write_annotations(extracted_dir, annotations=[])
        fp2 = chunk_fingerprint(extracted_dir, ctx)
        assert fp1 != fp2

    def test_no_annotations_uses_empty_fallback(self, tmp_path: Path):
        d = tmp_path / "noann"
        d.mkdir()
        _write_meta(d)
        _write_chunks(d)
        ctx = {"title": "Test Document", "raw_document_type": "regulation", "headings": []}
        fp1 = chunk_fingerprint(d, ctx)
        fp2 = chunk_fingerprint(d, ctx)
        assert fp1 == fp2

    def test_enriched_chunk_fields_excluded(self, extracted_dir: Path):
        """Enriched chunk fields (summary, keywords) don't affect fingerprint."""
        ctx = {"title": "Test Document", "raw_document_type": "regulation", "headings": []}
        fp1 = chunk_fingerprint(extracted_dir, ctx)
        # Add enriched fields to chunks.jsonl
        chunks_path = extracted_dir / "chunks.jsonl"
        lines = []
        for line in chunks_path.read_text().splitlines():
            c = json.loads(line)
            c["summary"] = {"value": "A summary.", "provenance": {
                "source_pages": [], "evidence_spans": [], "model": "claude",
                "prompt_version": "v1", "confidence": 0.9, "enriched_at": "now"
            }}
            lines.append(json.dumps(c))
        chunks_path.write_text("\n".join(lines), encoding="utf-8")
        fp2 = chunk_fingerprint(extracted_dir, ctx)
        assert fp1 == fp2


# ---------------------------------------------------------------------------
# figure_fingerprint tests
# ---------------------------------------------------------------------------


class TestFigureFingerprint:
    def _make_image(self, path: Path, content: bytes = b"\x89PNG\r\n\x1a\n") -> Path:
        path.write_bytes(content)
        return path

    def _make_sidecar(self) -> dict:
        return {
            "figure_id": "fig_0001",
            "source_page": 1,
            "image_path": "figures/fig_0001.png",
            "bbox": [0.1, 0.2, 0.8, 0.9],
            "extraction_method": "embedded",
        }

    def test_determinism(self, tmp_path: Path):
        img = self._make_image(tmp_path / "fig_0001.png")
        sidecar = self._make_sidecar()
        ctx = {"title": "Test Doc", "document_type": "regulation", "domain": "practice"}
        fp1 = figure_fingerprint(sidecar, img, "page text", ["caption A"], ctx)
        fp2 = figure_fingerprint(sidecar, img, "page text", ["caption A"], ctx)
        assert fp1 == fp2

    def test_returns_16_char_hex(self, tmp_path: Path):
        img = self._make_image(tmp_path / "fig_0001.png")
        sidecar = self._make_sidecar()
        ctx = {"title": "Test Doc", "domain": "practice"}
        fp = figure_fingerprint(sidecar, img, "page text", [], ctx)
        assert len(fp) == 16

    def test_changes_when_image_changes(self, tmp_path: Path):
        img = self._make_image(tmp_path / "fig.png", b"image_v1")
        sidecar = self._make_sidecar()
        ctx = {"title": "Test Doc", "domain": "practice"}
        fp1 = figure_fingerprint(sidecar, img, "page text", [], ctx)
        img.write_bytes(b"image_v2")
        fp2 = figure_fingerprint(sidecar, img, "page text", [], ctx)
        assert fp1 != fp2

    def test_changes_when_page_text_changes(self, tmp_path: Path):
        img = self._make_image(tmp_path / "fig.png")
        sidecar = self._make_sidecar()
        ctx = {"title": "Test Doc", "domain": "practice"}
        fp1 = figure_fingerprint(sidecar, img, "original text", [], ctx)
        fp2 = figure_fingerprint(sidecar, img, "changed text", [], ctx)
        assert fp1 != fp2

    def test_changes_when_caption_candidates_change(self, tmp_path: Path):
        img = self._make_image(tmp_path / "fig.png")
        sidecar = self._make_sidecar()
        ctx = {"title": "Test Doc", "domain": "practice"}
        fp1 = figure_fingerprint(sidecar, img, "text", ["caption A"], ctx)
        fp2 = figure_fingerprint(sidecar, img, "text", ["caption B"], ctx)
        assert fp1 != fp2

    def test_changes_when_doc_context_changes(self, tmp_path: Path):
        img = self._make_image(tmp_path / "fig.png")
        sidecar = self._make_sidecar()
        ctx1 = {"title": "Test Doc", "domain": "practice"}
        ctx2 = {"title": "Test Doc", "domain": "research"}
        fp1 = figure_fingerprint(sidecar, img, "text", [], ctx1)
        fp2 = figure_fingerprint(sidecar, img, "text", [], ctx2)
        assert fp1 != fp2

    def test_changes_when_sidecar_raw_fields_change(self, tmp_path: Path):
        img = self._make_image(tmp_path / "fig.png")
        sidecar1 = self._make_sidecar()
        sidecar2 = {**self._make_sidecar(), "source_page": 5}
        ctx = {"title": "Test Doc", "domain": "practice"}
        fp1 = figure_fingerprint(sidecar1, img, "text", [], ctx)
        fp2 = figure_fingerprint(sidecar2, img, "text", [], ctx)
        assert fp1 != fp2

    def test_enriched_sidecar_fields_excluded(self, tmp_path: Path):
        """Enriched figure fields (visual_type, description, caption) don't affect fingerprint."""
        img = self._make_image(tmp_path / "fig.png")
        sidecar_raw = self._make_sidecar()
        sidecar_enriched = {
            **sidecar_raw,
            "visual_type": {"value": "plan", "provenance": {}},
            "description": {"value": "A floor plan.", "provenance": {}},
        }
        ctx = {"title": "Test Doc", "domain": "practice"}
        fp1 = figure_fingerprint(sidecar_raw, img, "text", [], ctx)
        fp2 = figure_fingerprint(sidecar_enriched, img, "text", [], ctx)
        # Both use only source_page, bbox, extraction_method → same hash
        assert fp1 == fp2


# ---------------------------------------------------------------------------
# make_stamp + is_stale tests
# ---------------------------------------------------------------------------


class TestMakeStamp:
    def test_returns_correct_fields(self):
        stamp = make_stamp("enrich-v1.0", "claude-sonnet-4-6", "1", "abc123def456abcd")
        assert stamp == {
            "prompt_version": "enrich-v1.0",
            "model": "claude-sonnet-4-6",
            "enrichment_schema_version": "1",
            "input_fingerprint": "abc123def456abcd",
        }


class TestIsStale:
    def _stamp(self, **overrides) -> dict:
        base = {
            "prompt_version": "enrich-v1.0",
            "model": "claude-sonnet-4-6",
            "enrichment_schema_version": "1",
            "input_fingerprint": "abc123def456abcd",
        }
        base.update(overrides)
        return base

    def test_stale_when_none(self):
        assert is_stale(None, self._stamp()) is True

    def test_not_stale_when_all_fields_match(self):
        s = self._stamp()
        assert is_stale(s, s) is False

    def test_not_stale_with_copy(self):
        s = self._stamp()
        assert is_stale(dict(s), dict(s)) is False

    def test_stale_when_prompt_version_differs(self):
        existing = self._stamp()
        current = self._stamp(prompt_version="enrich-v2.0")
        assert is_stale(existing, current) is True

    def test_stale_when_model_differs(self):
        existing = self._stamp()
        current = self._stamp(model="claude-opus-4-5")
        assert is_stale(existing, current) is True

    def test_stale_when_schema_version_differs(self):
        existing = self._stamp()
        current = self._stamp(enrichment_schema_version="2")
        assert is_stale(existing, current) is True

    def test_stale_when_fingerprint_differs(self):
        existing = self._stamp()
        current = self._stamp(input_fingerprint="0000000000000000")
        assert is_stale(existing, current) is True


# ---------------------------------------------------------------------------
# Round-trip I/O tests
# ---------------------------------------------------------------------------


class TestDocumentStampRoundTrip:
    def test_round_trip(self, extracted_dir: Path):
        stamp = make_stamp("enrich-v1.0", "claude-sonnet-4-6", "1", "abc123def456abcd")
        write_document_stamp(extracted_dir, stamp)
        result = read_document_stamp(extracted_dir)
        assert result == stamp

    def test_read_returns_none_when_absent(self, extracted_dir: Path):
        # meta.json exists but has no _enrichment_stamp
        assert read_document_stamp(extracted_dir) is None

    def test_read_returns_none_when_file_missing(self, tmp_path: Path):
        d = tmp_path / "empty"
        d.mkdir()
        assert read_document_stamp(d) is None

    def test_write_preserves_other_meta_fields(self, extracted_dir: Path):
        """Writing a stamp doesn't destroy existing meta fields."""
        stamp = make_stamp("enrich-v1.0", "claude-sonnet-4-6", "1", "abc123def456abcd")
        write_document_stamp(extracted_dir, stamp)
        data = json.loads((extracted_dir / "meta.json").read_text())
        assert data["material_id"] == "abc123"
        assert data["title"] == "Test Document"
        assert data["_enrichment_stamp"] == stamp

    def test_overwrite_updates_stamp(self, extracted_dir: Path):
        stamp1 = make_stamp("enrich-v1.0", "claude-sonnet-4-6", "1", "aaaaaaaaaaaaaaaa")
        stamp2 = make_stamp("enrich-v2.0", "claude-sonnet-4-6", "1", "bbbbbbbbbbbbbbbb")
        write_document_stamp(extracted_dir, stamp1)
        write_document_stamp(extracted_dir, stamp2)
        assert read_document_stamp(extracted_dir) == stamp2


class TestChunkStampsRoundTrip:
    def test_round_trip(self, extracted_dir: Path):
        stamps = {
            "abc123-c0": make_stamp("enrich-v1.0", "claude-sonnet-4-6", "1", "aaaaaaaaaaaaaaaa"),
            "abc123-c1": make_stamp("enrich-v1.0", "claude-sonnet-4-6", "1", "bbbbbbbbbbbbbbbb"),
        }
        write_chunk_stamps(extracted_dir, stamps)
        result = read_chunk_stamps(extracted_dir)
        assert result == stamps

    def test_read_returns_empty_dict_when_absent(self, extracted_dir: Path):
        assert read_chunk_stamps(extracted_dir) == {}

    def test_overwrite_replaces_fully(self, extracted_dir: Path):
        stamps1 = {"chunk-a": make_stamp("v1", "model-a", "1", "aaaa" * 4)}
        stamps2 = {"chunk-b": make_stamp("v2", "model-b", "2", "bbbb" * 4)}
        write_chunk_stamps(extracted_dir, stamps1)
        write_chunk_stamps(extracted_dir, stamps2)
        assert read_chunk_stamps(extracted_dir) == stamps2

    def test_file_is_valid_json(self, extracted_dir: Path):
        stamps = {"c0": make_stamp("v1", "m", "1", "f" * 16)}
        write_chunk_stamps(extracted_dir, stamps)
        data = json.loads((extracted_dir / "chunk_enrichment_stamps.json").read_text())
        assert data == stamps


class TestFigureStampRoundTrip:
    def _make_sidecar_file(self, path: Path) -> Path:
        sidecar = {
            "figure_id": "fig_0001",
            "source_page": 1,
            "image_path": "figures/fig_0001.png",
            "bbox": [0.1, 0.2, 0.8, 0.9],
            "extraction_method": "embedded",
        }
        path.write_text(json.dumps(sidecar, indent=2), encoding="utf-8")
        return path

    def test_round_trip(self, tmp_path: Path):
        fig_path = self._make_sidecar_file(tmp_path / "fig_0001.json")
        stamp = make_stamp("enrich-v1.0", "claude-sonnet-4-6", "1", "abc123def456abcd")
        write_figure_stamp(fig_path, stamp)
        result = read_figure_stamp(fig_path)
        assert result == stamp

    def test_read_returns_none_when_absent(self, tmp_path: Path):
        fig_path = self._make_sidecar_file(tmp_path / "fig_0001.json")
        assert read_figure_stamp(fig_path) is None

    def test_read_returns_none_when_file_missing(self, tmp_path: Path):
        assert read_figure_stamp(tmp_path / "nonexistent.json") is None

    def test_write_preserves_other_sidecar_fields(self, tmp_path: Path):
        fig_path = self._make_sidecar_file(tmp_path / "fig_0001.json")
        stamp = make_stamp("enrich-v1.0", "claude-sonnet-4-6", "1", "abc123def456abcd")
        write_figure_stamp(fig_path, stamp)
        data = json.loads(fig_path.read_text())
        assert data["figure_id"] == "fig_0001"
        assert data["source_page"] == 1
        assert data["_enrichment_stamp"] == stamp

    def test_overwrite_updates_stamp(self, tmp_path: Path):
        fig_path = self._make_sidecar_file(tmp_path / "fig_0001.json")
        stamp1 = make_stamp("enrich-v1.0", "claude-sonnet-4-6", "1", "aaaaaaaaaaaaaaaa")
        stamp2 = make_stamp("enrich-v2.0", "claude-sonnet-4-6", "1", "bbbbbbbbbbbbbbbb")
        write_figure_stamp(fig_path, stamp1)
        write_figure_stamp(fig_path, stamp2)
        assert read_figure_stamp(fig_path) == stamp2
