"""Tests for enrich_prompts: prompt builders for LLM enrichment stages."""

from __future__ import annotations

import base64
from pathlib import Path

import pytest

from arquimedes.enrich_prompts import (
    build_chunk_batch_prompt,
    build_document_context,
    build_document_file_prompt,
    build_figure_context,
    build_document_input_files,
    build_figure_batch_prompt,
    build_metadata_fix_prompt,
    estimate_tokens,
    format_toc,
    inject_annotations,
)


# ---------------------------------------------------------------------------
# Fixtures — minimal data helpers
# ---------------------------------------------------------------------------


def _meta(**overrides) -> dict:
    base = {
        "material_id": "abc123",
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
        {"chunk_id": "abc123-c0", "text": "This document regulates housing density.",
         "source_pages": [1], "emphasized": False},
        {"chunk_id": "abc123-c1", "text": "Maximum FAR is 2.5 for residential zones.",
         "source_pages": [2], "emphasized": True},
    ]


def _annotations() -> list[dict]:
    return [
        {"annotation_id": "ann-1", "type": "highlight", "page": 2,
         "quoted_text": "Maximum FAR is 2.5", "comment": "check this limit",
         "color": "yellow", "rect": [0.1, 0.2, 0.3, 0.4]},
        {"annotation_id": "ann-2", "type": "highlight", "page": 1,
         "quoted_text": "housing density", "comment": "", "color": "green",
         "rect": [0.0, 0.0, 1.0, 1.0]},
    ]


# ---------------------------------------------------------------------------
# format_toc
# ---------------------------------------------------------------------------


class TestFormatToc:
    def test_none_or_empty_returns_not_available(self):
        assert format_toc(None) == "Not available"
        assert format_toc([]) == "Not available"

    def test_multi_level_entries(self):
        result = format_toc(_toc())
        lines = result.split("\n")
        assert len(lines) == 3
        assert lines[0].startswith("Introduction")
        assert "(p. 1)" in lines[0]
        assert lines[1].startswith("  Chapter 1")  # level 1 indent
        assert lines[2].startswith("    1.1 Scope")  # level 2 indent


# ---------------------------------------------------------------------------
# inject_annotations
# ---------------------------------------------------------------------------


class TestInjectAnnotations:
    def test_no_matching_annotations_unchanged(self):
        text = "Some plain text."
        assert inject_annotations(text, [], page_number=1) == text
        # Wrong page
        ann = [{"page": 5, "quoted_text": "Some plain text", "comment": ""}]
        assert inject_annotations(text, ann, page_number=1) == text

    def test_highlight_with_and_without_comment(self):
        text = "The thermal mass is important here."
        ann = [{"page": 3, "quoted_text": "thermal mass", "comment": ""}]
        result = inject_annotations(text, ann, page_number=3)
        assert "[HIGHLIGHTED]thermal mass[/HIGHLIGHTED]" in result
        assert "[NOTE:" not in result

        text2 = "Maximum FAR is 2.5 for residential zones."
        ann2 = [{"page": 2, "quoted_text": "Maximum FAR is 2.5", "comment": "check this limit"}]
        result2 = inject_annotations(text2, ann2, page_number=2)
        assert "[HIGHLIGHTED]Maximum FAR is 2.5[/HIGHLIGHTED]" in result2
        assert "[NOTE: check this limit]" in result2

    def test_multiple_annotations(self):
        text = "housing density maximum FAR residential zones"
        anns = [
            {"page": 1, "quoted_text": "housing density", "comment": ""},
            {"page": 1, "quoted_text": "residential zones", "comment": "important"},
        ]
        result = inject_annotations(text, anns, page_number=1)
        assert "[HIGHLIGHTED]housing density[/HIGHLIGHTED]" in result
        assert "[HIGHLIGHTED]residential zones[/HIGHLIGHTED]" in result


# ---------------------------------------------------------------------------
# build_document_context
# ---------------------------------------------------------------------------


class TestBuildDocumentContext:
    def test_includes_all_meta_fields(self):
        result = build_document_context(_meta(), None, None)
        assert "Urban Housing Standards" in result
        assert "Alice Arch, Bob Build" in result
        assert "2022" in result
        assert "practice" in result
        assert "regulation" in result
        assert "housing" in result

    def test_toc_and_headings_behavior(self):
        # TOC present → included
        result_toc = build_document_context(_meta(), _toc(), None)
        assert "Introduction" in result_toc
        assert "Section headings:" not in result_toc

        # No TOC, headings present → headings included
        result_heads = build_document_context(_meta(), None, ["Background", "Methods"])
        assert "Section headings:" in result_heads
        assert "Background" in result_heads

    def test_enriched_fields_included(self):
        meta = _meta()
        meta["summary"] = {"value": "A study of urban housing density."}
        meta["document_type"] = {"value": "regulation"}
        result = build_document_context(meta, None, None)
        assert "Document summary:" in result
        assert "Document type: regulation" in result


class TestBuildFigureContext:
    def test_keeps_only_minimal_figure_context(self):
        meta = _meta()
        meta["summary"] = {"value": "A study of urban housing density."}
        meta["document_type"] = {"value": "regulation"}
        meta["bridge_concepts"] = [{"canonical_name": "archive theory"}]
        result = build_figure_context(meta)
        assert "Urban Housing Standards" in result
        assert "Alice Arch, Bob Build" in result
        assert "2022" in result
        assert "practice" in result
        assert "regulations" in result
        assert "Document summary:" in result
        assert "Document type:" not in result
        assert "Bridge concepts:" not in result
        assert "Raw keywords:" not in result
        assert "Table of Contents:" not in result


# ---------------------------------------------------------------------------
# build_document_input_files + build_document_file_prompt
# ---------------------------------------------------------------------------


class TestBuildDocumentInputFiles:
    def test_returns_meta_json_and_flattened_document_file(self, tmp_path):
        (tmp_path / "meta.json").write_text('{"title":"Urban Housing Standards"}', encoding="utf-8")
        (tmp_path / "text.md").write_text("Line one.\n\nLine two.\nLine three.", encoding="utf-8")

        meta_path, document_text_path = build_document_input_files(
            tmp_path, _chunks(), _annotations()
        )

        assert meta_path == tmp_path / "meta.json"
        assert document_text_path.exists()
        assert document_text_path.read_text(encoding="utf-8") == "Line one. Line two. Line three."

    def test_falls_back_to_chunks_when_text_md_missing(self, tmp_path):
        (tmp_path / "meta.json").write_text('{"title":"Urban Housing Standards"}', encoding="utf-8")

        _, document_text_path = build_document_input_files(tmp_path, _chunks(), _annotations())

        content = document_text_path.read_text(encoding="utf-8")
        assert "regulates" in content
        assert "Maximum FAR is 2.5" in content
        assert "\n" not in content


class TestBuildDocumentFilePrompt:
    def test_shape_and_references_paths(self, tmp_path):
        meta_path = tmp_path / "meta.json"
        document_text_path = tmp_path / "document.work.md"
        meta_path.write_text("{}", encoding="utf-8")
        document_text_path.write_text("text", encoding="utf-8")
        system, messages = build_document_file_prompt(meta_path, document_text_path)
        assert isinstance(system, str) and len(system) > 0
        assert len(messages) == 1 and messages[0]["role"] == "user"
        content = messages[0]["content"]
        assert str(meta_path) in content
        assert str(document_text_path) in content

    def test_system_prompt_contains_key_field_instructions(self, tmp_path):
        meta_path = tmp_path / "meta.json"
        document_text_path = tmp_path / "document.work.md"
        meta_path.write_text("{}", encoding="utf-8")
        document_text_path.write_text("text", encoding="utf-8")
        system, _ = build_document_file_prompt(meta_path, document_text_path)
        assert "summary" in system
        assert "facets" in system
        assert "concepts_local" in system
        assert "concepts_bridge_candidates" in system
        assert "methodological_conclusions" in system
        assert "main_content_learnings" in system
        assert "complete JSON output object" in system
        assert '"title": "... optional, only if replacing ..."' not in system
        assert "Replace only if clearly wrong from document evidence." not in system

    def test_practice_prompt_requests_spanish_output(self, tmp_path):
        meta_path = tmp_path / "meta.json"
        document_text_path = tmp_path / "document.work.md"
        meta_path.write_text("{}", encoding="utf-8")
        document_text_path.write_text("texto", encoding="utf-8")
        system, _ = build_document_file_prompt(meta_path, document_text_path, domain="practice")
        assert "orientada a la práctica" in system
        assert "Escribe en español" in system
        assert '"document_type"' in system


# ---------------------------------------------------------------------------
# build_chunk_batch_prompt
# ---------------------------------------------------------------------------


class TestBuildChunkBatchPrompt:
    def test_shape_and_content(self):
        doc_ctx = build_document_context(_meta(), None, None)
        system, messages = build_chunk_batch_prompt(_chunks(), doc_ctx, _annotations())
        assert "JSON" in system and "chunk" in system.lower()
        assert len(messages) == 1 and messages[0]["role"] == "user"
        content = messages[0]["content"]
        assert "abc123-c0" in content
        assert "Urban Housing Standards" in content
        assert "[HIGHLIGHTED]" in content

    def test_practice_prompt_switches_to_spanish(self):
        doc_ctx = build_document_context(_meta(domain="practice"), None, None)
        system, messages = build_chunk_batch_prompt(_chunks(), doc_ctx, _annotations(), domain="practice")
        assert "orientada a la práctica" in system
        content = messages[0]["content"]
        assert "Todos los textos libres deben estar en español" in content
        assert "Language must be English" not in content


# ---------------------------------------------------------------------------
# build_figure_batch_prompt
# ---------------------------------------------------------------------------


class TestBuildFigureBatchPrompt:
    def _make_png(self, path: Path) -> Path:
        png_bytes = base64.b64decode(
            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwADhQGAWjR9awAAAABJRU5ErkJggg=="
        )
        path.write_bytes(png_bytes)
        return path

    def test_text_only_figure(self):
        figs = [{"figure_id": "fig_0001", "image_path": None,
                 "source_page_text": "A floor plan.", "caption_candidates": ["Figure 1"],
                 "sidecar": {"source_page": 3}}]
        system, messages = build_figure_batch_prompt(figs, "Doc context")
        assert "JSON" in system
        content_blocks = messages[0]["content"]
        types = [b["type"] for b in content_blocks]
        assert "image" not in types
        all_text = " ".join(b["text"] for b in content_blocks if b["type"] == "text")
        assert "fig_0001" in all_text and "Figure 1" in all_text
        assert "Image unavailable" in all_text

    def test_vision_figure_includes_base64(self, tmp_path):
        img_path = self._make_png(tmp_path / "fig_0001.png")
        figs = [{"figure_id": "fig_0001", "image_path": str(img_path),
                 "source_page_text": "A floor plan.", "caption_candidates": [],
                 "sidecar": {"source_page": 1}}]
        _, messages = build_figure_batch_prompt(figs, "ctx")
        content_blocks = messages[0]["content"]
        img_block = next(b for b in content_blocks if b["type"] == "image")
        assert img_block["source"]["type"] == "base64"
        base64.b64decode(img_block["source"]["data"])

    def test_multiple_figures_mixed(self, tmp_path):
        img_path = self._make_png(tmp_path / "fig_0001.png")
        figs = [
            {"figure_id": "fig_0001", "image_path": str(img_path),
             "source_page_text": "Plan.", "caption_candidates": [], "sidecar": {"source_page": 1}},
            {"figure_id": "fig_0002", "image_path": None,
             "source_page_text": "Section.", "caption_candidates": [], "sidecar": {"source_page": 2}},
        ]
        _, messages = build_figure_batch_prompt(figs, "ctx")
        content_blocks = messages[0]["content"]
        types = [b["type"] for b in content_blocks]
        assert types.count("image") == 1
        all_text = " ".join(b["text"] for b in content_blocks if b["type"] == "text")
        assert "fig_0001" in all_text and "fig_0002" in all_text

    def test_prompt_includes_bbox_and_artifact_hint(self):
        figs = [{
            "figure_id": "fig_0001",
            "image_path": None,
            "source_page_text": "Dense article text.",
            "caption_candidates": [],
            "artifact_hint": "very small crop; likely inline artifact, partial scan fragment, icon, or empty non-figure",
            "sidecar": {
                "source_page": 3,
                "bbox": [5.0, 5.0, 18.0, 24.0],
                "extraction_method": "embedded",
            },
        }]
        _, messages = build_figure_batch_prompt(figs, "Doc context")
        all_text = " ".join(b["text"] for b in messages[0]["content"] if b["type"] == "text")
        assert "Bounding box:" in all_text
        assert "Artifact hint:" in all_text

    def test_practice_prompt_prioritizes_figures_as_primary_evidence(self):
        figs = [{"figure_id": "fig_0001", "image_path": None,
                 "source_page_text": "Una planta con cotas.", "caption_candidates": ["Figura 1"],
                 "sidecar": {"source_page": 3}}]
        system, messages = build_figure_batch_prompt(figs, "Contexto", domain="practice")
        assert "orientada a la práctica" in system
        all_text = " ".join(b["text"] for b in messages[0]["content"] if b["type"] == "text")
        assert "evidencia principal" in all_text
        assert "Todos los textos libres deben estar en español" in all_text


class TestBuildMetadataFixPrompt:
    def _make_png(self, path: Path) -> Path:
        png_bytes = base64.b64decode(
            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwADhQGAWjR9awAAAABJRU5ErkJggg=="
        )
        path.write_bytes(png_bytes)
        return path

    def test_uses_first_four_page_thumbnails(self, tmp_path):
        thumbs_dir = tmp_path / "thumbnails"
        thumbs_dir.mkdir()
        for i in range(1, 6):
            self._make_png(thumbs_dir / f"page_{i:04d}.png")

        pages = [
            {
                "page_number": i,
                "text": f"Page {i} text",
                "thumbnail_path": f"thumbnails/page_{i:04d}.png",
            }
            for i in range(1, 6)
        ]

        _, messages = build_metadata_fix_prompt(_meta(), pages, tmp_path)
        content_blocks = messages[0]["content"]
        assert sum(1 for block in content_blocks if block["type"] == "image") == 4
        text_blob = " ".join(block["text"] for block in content_blocks if block["type"] == "text")
        assert "Current metadata:" in text_blob
        assert "Page 4" in text_blob
        assert "Page 5" not in text_blob


# ---------------------------------------------------------------------------
# estimate_tokens
# ---------------------------------------------------------------------------


class TestEstimateTokens:
    def test_basic_estimation(self):
        assert estimate_tokens("") == 0
        assert estimate_tokens("a" * 400) == 100
