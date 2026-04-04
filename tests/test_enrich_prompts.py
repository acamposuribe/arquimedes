"""Tests for enrich_prompts: prompt builders for LLM enrichment stages."""

from __future__ import annotations

import base64
import json
from pathlib import Path

import pytest

from arquimedes.enrich_prompts import (
    build_chunk_batch_prompt,
    build_combined_prompt,
    build_document_context,
    build_document_prompt,
    build_figure_batch_prompt,
    format_toc,
    inject_annotations,
)


# ---------------------------------------------------------------------------
# Fixtures — minimal data helpers
# ---------------------------------------------------------------------------


def _meta(**overrides) -> dict:
    base = {
        "material_id": "abc123",
        "file_hash": "deadbeef",
        "title": "Urban Housing Standards",
        "authors": ["Alice Arch", "Bob Build"],
        "year": "2022",
        "domain": "practice",
        "collection": "regulations",
        "raw_document_type": "regulation",
        "raw_keywords": ["housing", "density", "zoning"],
        "page_count": 5,
    }
    base.update(overrides)
    return base


def _toc() -> list[dict]:
    return [
        {"title": "Introduction", "page": 1, "level": 0},
        {"title": "Chapter 1: Definitions", "page": 3, "level": 1},
        {"title": "1.1 Scope", "page": 4, "level": 2},
    ]


def _chunks() -> list[dict]:
    return [
        {
            "chunk_id": "abc123-c0",
            "text": "This document regulates housing density.",
            "source_pages": [1],
            "emphasized": False,
        },
        {
            "chunk_id": "abc123-c1",
            "text": "Maximum FAR is 2.5 for residential zones.",
            "source_pages": [2],
            "emphasized": True,
        },
    ]


def _annotations() -> list[dict]:
    return [
        {
            "annotation_id": "ann-1",
            "type": "highlight",
            "page": 2,
            "quoted_text": "Maximum FAR is 2.5",
            "comment": "check this limit",
            "color": "yellow",
            "rect": [0.1, 0.2, 0.3, 0.4],
        },
        {
            "annotation_id": "ann-2",
            "type": "highlight",
            "page": 1,
            "quoted_text": "housing density",
            "comment": "",
            "color": "green",
            "rect": [0.0, 0.0, 1.0, 1.0],
        },
    ]


# ---------------------------------------------------------------------------
# format_toc tests
# ---------------------------------------------------------------------------


class TestFormatToc:
    def test_none_returns_not_available(self):
        assert format_toc(None) == "Not available"

    def test_empty_list_returns_not_available(self):
        assert format_toc([]) == "Not available"

    def test_single_entry_no_indent(self):
        toc = [{"title": "Introduction", "page": 1, "level": 0}]
        result = format_toc(toc)
        assert "Introduction" in result
        assert "(p. 1)" in result
        # level 0 → no leading spaces
        assert result.startswith("Introduction")

    def test_indentation_by_level(self):
        toc = [
            {"title": "Chapter 1", "page": 3, "level": 1},
            {"title": "Section 1.1", "page": 4, "level": 2},
        ]
        result = format_toc(toc)
        lines = result.split("\n")
        # level 1 → 2 spaces, level 2 → 4 spaces
        assert lines[0].startswith("  Chapter 1")
        assert lines[1].startswith("    Section 1.1")

    def test_multiple_entries_newline_separated(self):
        toc = _toc()
        result = format_toc(toc)
        lines = result.split("\n")
        assert len(lines) == 3

    def test_page_numbers_included(self):
        toc = [{"title": "Appendix", "page": 99, "level": 0}]
        result = format_toc(toc)
        assert "99" in result


# ---------------------------------------------------------------------------
# inject_annotations tests
# ---------------------------------------------------------------------------


class TestInjectAnnotations:
    def test_no_annotations_returns_unchanged(self):
        text = "Some plain text without any annotations."
        result = inject_annotations(text, [], page_number=1)
        assert result == text

    def test_annotation_on_wrong_page_skipped(self):
        text = "Some plain text."
        ann = [
            {
                "annotation_id": "a1",
                "type": "highlight",
                "page": 5,
                "quoted_text": "Some plain text",
                "comment": "",
                "color": "yellow",
                "rect": [],
            }
        ]
        result = inject_annotations(text, ann, page_number=1)
        assert result == text

    def test_single_highlight_no_comment(self):
        text = "The thermal mass is important here."
        ann = [
            {
                "annotation_id": "a1",
                "type": "highlight",
                "page": 3,
                "quoted_text": "thermal mass",
                "comment": "",
                "color": "yellow",
                "rect": [],
            }
        ]
        result = inject_annotations(text, ann, page_number=3)
        assert "[HIGHLIGHTED]thermal mass[/HIGHLIGHTED]" in result
        assert "[NOTE:" not in result

    def test_single_highlight_with_comment(self):
        text = "Maximum FAR is 2.5 for residential zones."
        ann = [
            {
                "annotation_id": "a1",
                "type": "highlight",
                "page": 2,
                "quoted_text": "Maximum FAR is 2.5",
                "comment": "check this limit",
                "color": "yellow",
                "rect": [],
            }
        ]
        result = inject_annotations(text, ann, page_number=2)
        assert "[HIGHLIGHTED]Maximum FAR is 2.5[/HIGHLIGHTED]" in result
        assert "[NOTE: check this limit]" in result

    def test_quoted_text_not_found_skipped_silently(self):
        text = "This text does not contain the annotation."
        ann = [
            {
                "annotation_id": "a1",
                "type": "highlight",
                "page": 1,
                "quoted_text": "completely absent phrase",
                "comment": "",
                "color": "yellow",
                "rect": [],
            }
        ]
        result = inject_annotations(text, ann, page_number=1)
        assert result == text

    def test_multiple_annotations_correct_page(self):
        text = "housing density maximum FAR residential zones"
        anns = [
            {
                "annotation_id": "a1",
                "type": "highlight",
                "page": 1,
                "quoted_text": "housing density",
                "comment": "",
                "color": "yellow",
                "rect": [],
            },
            {
                "annotation_id": "a2",
                "type": "highlight",
                "page": 1,
                "quoted_text": "residential zones",
                "comment": "important",
                "color": "green",
                "rect": [],
            },
        ]
        result = inject_annotations(text, anns, page_number=1)
        assert "[HIGHLIGHTED]housing density[/HIGHLIGHTED]" in result
        assert "[HIGHLIGHTED]residential zones[/HIGHLIGHTED]" in result
        assert "[NOTE: important]" in result

    def test_empty_quoted_text_skipped(self):
        text = "Some text on the page."
        ann = [
            {
                "annotation_id": "a1",
                "type": "highlight",
                "page": 1,
                "quoted_text": "",
                "comment": "a note without highlight",
                "color": "yellow",
                "rect": [],
            }
        ]
        result = inject_annotations(text, ann, page_number=1)
        assert result == text


# ---------------------------------------------------------------------------
# build_document_context tests
# ---------------------------------------------------------------------------


class TestBuildDocumentContext:
    def test_returns_string(self):
        result = build_document_context(_meta(), _toc(), None)
        assert isinstance(result, str)

    def test_contains_title(self):
        result = build_document_context(_meta(), None, None)
        assert "Urban Housing Standards" in result

    def test_contains_authors_joined(self):
        result = build_document_context(_meta(), None, None)
        assert "Alice Arch, Bob Build" in result

    def test_contains_year(self):
        result = build_document_context(_meta(), None, None)
        assert "2022" in result

    def test_contains_domain_and_collection(self):
        result = build_document_context(_meta(), None, None)
        assert "practice" in result
        assert "regulations" in result

    def test_contains_raw_document_type(self):
        result = build_document_context(_meta(), None, None)
        assert "regulation" in result

    def test_contains_raw_keywords(self):
        result = build_document_context(_meta(), None, None)
        assert "housing" in result
        assert "density" in result
        assert "zoning" in result

    def test_toc_included_when_provided(self):
        result = build_document_context(_meta(), _toc(), None)
        assert "Introduction" in result
        assert "Chapter 1" in result

    def test_toc_not_available_when_none(self):
        result = build_document_context(_meta(), None, None)
        assert "Not available" in result

    def test_single_author(self):
        result = build_document_context(_meta(authors=["Solo Author"]), None, None)
        assert "Solo Author" in result
        assert ", " not in result.split("Authors:")[1].split("\n")[0]

    def test_empty_authors(self):
        result = build_document_context(_meta(authors=[]), None, None)
        assert "Authors:" in result


# ---------------------------------------------------------------------------
# build_document_prompt tests
# ---------------------------------------------------------------------------


class TestBuildDocumentPrompt:
    def test_returns_tuple_str_list(self):
        system, messages = build_document_prompt(_meta(), _toc(), _chunks(), _annotations())
        assert isinstance(system, str)
        assert isinstance(messages, list)

    def test_system_prompt_mentions_json(self):
        system, _ = build_document_prompt(_meta(), _toc(), _chunks(), _annotations())
        assert "JSON" in system

    def test_single_user_message(self):
        _, messages = build_document_prompt(_meta(), _toc(), _chunks(), _annotations())
        assert len(messages) == 1
        assert messages[0]["role"] == "user"

    def test_user_message_contains_title(self):
        _, messages = build_document_prompt(_meta(), _toc(), _chunks(), _annotations())
        assert "Urban Housing Standards" in messages[0]["content"]

    def test_user_message_contains_chunk_ids(self):
        _, messages = build_document_prompt(_meta(), _toc(), _chunks(), _annotations())
        content = messages[0]["content"]
        assert "abc123-c0" in content
        assert "abc123-c1" in content

    def test_user_message_contains_chunk_text(self):
        _, messages = build_document_prompt(_meta(), _toc(), _chunks(), _annotations())
        content = messages[0]["content"]
        assert "housing density" in content
        assert "Maximum FAR" in content

    def test_annotations_injected_in_chunks(self):
        _, messages = build_document_prompt(_meta(), _toc(), _chunks(), _annotations())
        content = messages[0]["content"]
        assert "[HIGHLIGHTED]" in content

    def test_schema_in_user_message(self):
        _, messages = build_document_prompt(_meta(), _toc(), _chunks(), _annotations())
        content = messages[0]["content"]
        assert "summary" in content
        assert "document_type" in content
        assert "facets" in content
        assert "concepts" in content

    def test_no_annotations(self):
        # With no annotations the chunk texts should not contain highlight markers,
        # though the instruction text may still reference them as documentation.
        system, messages = build_document_prompt(_meta(), None, _chunks(), [])
        assert isinstance(system, str)
        content = messages[0]["content"]
        # Chunk texts should be plain (no actual [HIGHLIGHTED] spans from injection)
        # Verify the chunk text is present without wrapping
        assert "This document regulates housing density." in content
        assert "Maximum FAR is 2.5 for residential zones." in content

    def test_chunk_format_includes_pages(self):
        _, messages = build_document_prompt(_meta(), None, _chunks(), [])
        content = messages[0]["content"]
        # Chunks formatted as: --- Chunk {id} (pages {pages}) ---
        assert "(pages 1)" in content
        assert "(pages 2)" in content


# ---------------------------------------------------------------------------
# build_combined_prompt tests
# ---------------------------------------------------------------------------


class TestBuildCombinedPrompt:
    def test_returns_tuple_str_list(self):
        system, messages = build_combined_prompt(_meta(), _toc(), _chunks(), _annotations())
        assert isinstance(system, str)
        assert isinstance(messages, list)

    def test_system_prompt_mentions_json(self):
        system, _ = build_combined_prompt(_meta(), _toc(), _chunks(), _annotations())
        assert "JSON" in system

    def test_schema_has_document_key(self):
        _, messages = build_combined_prompt(_meta(), _toc(), _chunks(), _annotations())
        content = messages[0]["content"]
        assert '"document"' in content

    def test_schema_has_chunks_key(self):
        _, messages = build_combined_prompt(_meta(), _toc(), _chunks(), _annotations())
        content = messages[0]["content"]
        assert '"chunks"' in content

    def test_chunk_ids_in_user_message(self):
        _, messages = build_combined_prompt(_meta(), _toc(), _chunks(), _annotations())
        content = messages[0]["content"]
        assert "abc123-c0" in content
        assert "abc123-c1" in content

    def test_document_schema_fields_present(self):
        _, messages = build_combined_prompt(_meta(), _toc(), _chunks(), _annotations())
        content = messages[0]["content"]
        # Document-level fields
        assert "summary" in content
        assert "document_type" in content
        assert "facets" in content
        assert "concepts" in content

    def test_chunk_schema_fields_present(self):
        _, messages = build_combined_prompt(_meta(), _toc(), _chunks(), _annotations())
        content = messages[0]["content"]
        # Chunk-level fields in schema
        assert "chunk_id" in content
        assert "keywords" in content

    def test_annotation_markers_present(self):
        _, messages = build_combined_prompt(_meta(), _toc(), _chunks(), _annotations())
        content = messages[0]["content"]
        assert "[HIGHLIGHTED]" in content


# ---------------------------------------------------------------------------
# build_chunk_batch_prompt tests
# ---------------------------------------------------------------------------


class TestBuildChunkBatchPrompt:
    def test_returns_tuple_str_list(self):
        doc_ctx = build_document_context(_meta(), None, None)
        system, messages = build_chunk_batch_prompt(_chunks(), doc_ctx, _annotations())
        assert isinstance(system, str)
        assert isinstance(messages, list)

    def test_system_prompt_mentions_chunks(self):
        doc_ctx = build_document_context(_meta(), None, None)
        system, _ = build_chunk_batch_prompt(_chunks(), doc_ctx, [])
        assert "chunk" in system.lower()

    def test_system_prompt_mentions_json(self):
        doc_ctx = build_document_context(_meta(), None, None)
        system, _ = build_chunk_batch_prompt(_chunks(), doc_ctx, [])
        assert "JSON" in system

    def test_chunk_ids_in_user_message(self):
        doc_ctx = build_document_context(_meta(), None, None)
        _, messages = build_chunk_batch_prompt(_chunks(), doc_ctx, [])
        content = messages[0]["content"]
        assert "abc123-c0" in content
        assert "abc123-c1" in content

    def test_annotation_markers_injected(self):
        doc_ctx = build_document_context(_meta(), None, None)
        _, messages = build_chunk_batch_prompt(_chunks(), doc_ctx, _annotations())
        content = messages[0]["content"]
        assert "[HIGHLIGHTED]" in content

    def test_doc_context_str_included(self):
        doc_ctx = build_document_context(_meta(), None, None)
        _, messages = build_chunk_batch_prompt(_chunks(), doc_ctx, [])
        content = messages[0]["content"]
        assert "Urban Housing Standards" in content

    def test_schema_in_user_message(self):
        doc_ctx = build_document_context(_meta(), None, None)
        _, messages = build_chunk_batch_prompt(_chunks(), doc_ctx, [])
        content = messages[0]["content"]
        assert "chunk_id" in content
        assert "summary" in content
        assert "keywords" in content

    def test_single_user_message(self):
        doc_ctx = build_document_context(_meta(), None, None)
        _, messages = build_chunk_batch_prompt(_chunks(), doc_ctx, [])
        assert len(messages) == 1
        assert messages[0]["role"] == "user"

    def test_chunk_format_includes_pages(self):
        doc_ctx = build_document_context(_meta(), None, None)
        _, messages = build_chunk_batch_prompt(_chunks(), doc_ctx, [])
        content = messages[0]["content"]
        assert "(pages 1)" in content
        assert "(pages 2)" in content


# ---------------------------------------------------------------------------
# build_figure_batch_prompt tests
# ---------------------------------------------------------------------------


class TestBuildFigureBatchPrompt:
    def _make_png(self, path: Path) -> Path:
        """Write a minimal 1x1 white PNG."""
        # Minimal valid PNG bytes (1x1 white pixel)
        png_bytes = base64.b64decode(
            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwADhQGAWjR9awAAAABJRU5ErkJggg=="
        )
        path.write_bytes(png_bytes)
        return path

    def test_returns_tuple_str_list(self):
        figs = [
            {
                "figure_id": "fig_0001",
                "image_path": None,
                "source_page_text": "A floor plan.",
                "caption_candidates": ["Figure 1: Ground floor plan"],
                "sidecar": {"source_page": 3, "bbox": [], "extraction_method": "embedded"},
            }
        ]
        system, messages = build_figure_batch_prompt(figs, "Doc context string")
        assert isinstance(system, str)
        assert isinstance(messages, list)

    def test_system_prompt_mentions_figures(self):
        system, _ = build_figure_batch_prompt([], "ctx")
        assert "figure" in system.lower()

    def test_system_prompt_mentions_json(self):
        system, _ = build_figure_batch_prompt([], "ctx")
        assert "JSON" in system

    def test_single_user_message(self):
        _, messages = build_figure_batch_prompt([], "ctx")
        assert len(messages) == 1
        assert messages[0]["role"] == "user"

    def test_user_message_is_list_of_content_blocks(self):
        """Figure prompts use multimodal content (list of content blocks)."""
        _, messages = build_figure_batch_prompt([], "ctx")
        assert isinstance(messages[0]["content"], list)

    def test_figure_without_image_is_text_only(self):
        figs = [
            {
                "figure_id": "fig_0001",
                "image_path": None,
                "source_page_text": "A section drawing.",
                "caption_candidates": [],
                "sidecar": {"source_page": 2},
            }
        ]
        _, messages = build_figure_batch_prompt(figs, "ctx")
        content_blocks = messages[0]["content"]
        types = [block["type"] for block in content_blocks]
        # Should have text blocks only, no image block
        assert "image" not in types
        # Should note that image is unavailable
        all_text = " ".join(
            b["text"] for b in content_blocks if b["type"] == "text"
        )
        assert "unavailable" in all_text.lower() or "fig_0001" in all_text

    def test_figure_with_missing_path_is_text_only(self):
        figs = [
            {
                "figure_id": "fig_0002",
                "image_path": "/nonexistent/path/fig.png",
                "source_page_text": "An elevation.",
                "caption_candidates": [],
                "sidecar": {"source_page": 4},
            }
        ]
        _, messages = build_figure_batch_prompt(figs, "ctx")
        content_blocks = messages[0]["content"]
        types = [block["type"] for block in content_blocks]
        assert "image" not in types

    def test_figure_with_valid_image_includes_base64_block(self, tmp_path):
        img_path = self._make_png(tmp_path / "fig_0001.png")
        figs = [
            {
                "figure_id": "fig_0001",
                "image_path": str(img_path),
                "source_page_text": "A floor plan.",
                "caption_candidates": ["Ground floor"],
                "sidecar": {"source_page": 1},
            }
        ]
        _, messages = build_figure_batch_prompt(figs, "ctx")
        content_blocks = messages[0]["content"]
        types = [block["type"] for block in content_blocks]
        assert "image" in types

    def test_base64_image_block_has_correct_structure(self, tmp_path):
        img_path = self._make_png(tmp_path / "fig_0001.png")
        figs = [
            {
                "figure_id": "fig_0001",
                "image_path": str(img_path),
                "source_page_text": "A section.",
                "caption_candidates": [],
                "sidecar": {"source_page": 2},
            }
        ]
        _, messages = build_figure_batch_prompt(figs, "ctx")
        content_blocks = messages[0]["content"]
        img_block = next(b for b in content_blocks if b["type"] == "image")
        assert img_block["source"]["type"] == "base64"
        assert "media_type" in img_block["source"]
        assert "data" in img_block["source"]
        # data should be valid base64
        base64.b64decode(img_block["source"]["data"])  # raises if invalid

    def test_figure_id_in_text_output(self):
        figs = [
            {
                "figure_id": "fig_9999",
                "image_path": None,
                "source_page_text": "Some text.",
                "caption_candidates": [],
                "sidecar": {"source_page": 10},
            }
        ]
        _, messages = build_figure_batch_prompt(figs, "doc context here")
        all_text = " ".join(
            b["text"] for b in messages[0]["content"] if b["type"] == "text"
        )
        assert "fig_9999" in all_text

    def test_doc_context_in_text_output(self):
        figs = []
        _, messages = build_figure_batch_prompt(figs, "Unique context string XYZ")
        all_text = " ".join(
            b["text"] for b in messages[0]["content"] if b["type"] == "text"
        )
        assert "Unique context string XYZ" in all_text

    def test_schema_in_text_output(self):
        figs = []
        _, messages = build_figure_batch_prompt(figs, "ctx")
        all_text = " ".join(
            b["text"] for b in messages[0]["content"] if b["type"] == "text"
        )
        assert "visual_type" in all_text
        assert "description" in all_text
        assert "caption" in all_text

    def test_caption_candidates_in_text_output(self):
        figs = [
            {
                "figure_id": "fig_0001",
                "image_path": None,
                "source_page_text": "Context.",
                "caption_candidates": ["Figure 1: The north elevation", "Fig. 1"],
                "sidecar": {"source_page": 5},
            }
        ]
        _, messages = build_figure_batch_prompt(figs, "ctx")
        all_text = " ".join(
            b["text"] for b in messages[0]["content"] if b["type"] == "text"
        )
        assert "Figure 1: The north elevation" in all_text
        assert "Fig. 1" in all_text

    def test_multiple_figures(self, tmp_path):
        img_path = self._make_png(tmp_path / "fig_0001.png")
        figs = [
            {
                "figure_id": "fig_0001",
                "image_path": str(img_path),
                "source_page_text": "Floor plan text.",
                "caption_candidates": ["Plan"],
                "sidecar": {"source_page": 1},
            },
            {
                "figure_id": "fig_0002",
                "image_path": None,
                "source_page_text": "Section text.",
                "caption_candidates": [],
                "sidecar": {"source_page": 2},
            },
        ]
        _, messages = build_figure_batch_prompt(figs, "ctx")
        content_blocks = messages[0]["content"]
        types = [block["type"] for block in content_blocks]
        # One image block for fig_0001; no image for fig_0002
        assert types.count("image") == 1
        all_text = " ".join(b["text"] for b in content_blocks if b["type"] == "text")
        assert "fig_0001" in all_text
        assert "fig_0002" in all_text
