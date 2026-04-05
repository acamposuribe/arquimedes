"""Tests for chunk_pages — annotation_overlap_ids (C2.1)."""

from __future__ import annotations

import pytest

from arquimedes.chunking import chunk_pages
from arquimedes.models import Annotation, Page


def _page(number: int, text: str) -> Page:
    return Page(page_number=number, text=text)


def _annotation(ann_id: str, page: int, text: str) -> Annotation:
    return Annotation(
        annotation_id=ann_id,
        type="highlight",
        page=page,
        quoted_text=text,
    )


class TestAnnotationOverlapIds:
    def test_overlapping_annotation_carries_id(self):
        """Chunk that contains highlighted text must list that annotation's ID."""
        pages = [_page(1, "Memory and the archive are central to postcolonial thought.")]
        annotations = [
            _annotation("ann_001", 1, "Memory and the archive are central to postcolonial thought.")
        ]
        chunks = chunk_pages(pages, annotations=annotations)
        assert len(chunks) == 1
        assert "ann_001" in chunks[0].annotation_overlap_ids
        assert chunks[0].emphasized is True

    def test_non_overlapping_chunk_has_empty_list(self):
        """Chunk with no annotation overlap must have empty annotation_overlap_ids."""
        pages = [_page(1, "Concrete is a composite material made from aggregate and cement.")]
        annotations = [
            _annotation("ann_002", 2, "Some text that appears on a completely different page.")
        ]
        chunks = chunk_pages(pages, annotations=annotations)
        assert len(chunks) == 1
        assert chunks[0].annotation_overlap_ids == []
        assert chunks[0].emphasized is False

    def test_multiple_annotations_on_same_chunk(self):
        """Multiple annotations overlapping the same chunk all appear in the list."""
        text = "The spatial turn in humanities transformed how scholars read cities and territories."
        pages = [_page(1, text)]
        annotations = [
            _annotation("ann_010", 1, "spatial turn in humanities transformed how scholars"),
            _annotation("ann_011", 1, "how scholars read cities and territories"),
        ]
        chunks = chunk_pages(pages, annotations=annotations)
        assert len(chunks) == 1
        assert "ann_010" in chunks[0].annotation_overlap_ids
        assert "ann_011" in chunks[0].annotation_overlap_ids

    def test_no_annotations_gives_empty_list(self):
        """Calling chunk_pages without annotations must produce empty overlap lists."""
        pages = [_page(1, "Architecture mediates between body and environment.")]
        chunks = chunk_pages(pages, annotations=None)
        assert len(chunks) == 1
        assert chunks[0].annotation_overlap_ids == []
        assert chunks[0].emphasized is False

    def test_roundtrip_via_dict(self):
        """annotation_overlap_ids must survive to_dict / from_dict round-trip."""
        from arquimedes.models import Chunk

        chunk = Chunk(
            chunk_id="chk_00001",
            text="Test text.",
            source_pages=[1],
            emphasized=True,
            annotation_overlap_ids=["ann_001", "ann_002"],
        )
        restored = Chunk.from_dict(chunk.to_dict())
        assert restored.annotation_overlap_ids == ["ann_001", "ann_002"]
