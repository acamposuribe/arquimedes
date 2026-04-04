"""Tests for enrich_figures: figure-level LLM enrichment stage."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from arquimedes.enrich_figures import enrich_figures_stage


# ---------------------------------------------------------------------------
# Shared mock responses
# ---------------------------------------------------------------------------


def _make_figure_response(figure_ids: list[str]) -> str:
    """Build a mock figure-batch LLM response for the given figure IDs."""
    return json.dumps({
        "figures": [
            {
                "figure_id": fid,
                "visual_type": {
                    "value": "plan",
                    "source_pages": [1],
                    "evidence_spans": ["floor plan shown"],
                    "confidence": 0.9,
                },
                "description": {
                    "value": f"Floor plan of building for {fid}",
                    "source_pages": [1],
                    "evidence_spans": ["the plan depicts"],
                    "confidence": 0.85,
                },
                "caption": {
                    "value": "Figure 1: Plan view",
                    "source_pages": [1],
                    "evidence_spans": ["Figure 1"],
                    "confidence": 0.8,
                },
            }
            for fid in figure_ids
        ]
    })


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_extracted_dir(
    tmp_path: Path,
    figure_ids: list[str] | None = None,
    with_image: bool = True,
) -> Path:
    """Create a minimal extracted/<id>/ directory with figure sidecars."""
    d = tmp_path / "extracted" / "test123"
    d.mkdir(parents=True)

    meta = {
        "material_id": "test123",
        "title": "Test Doc",
        "authors": ["Author A"],
        "year": "2024",
        "raw_keywords": ["arch"],
        "raw_document_type": "paper",
        "domain": "research",
        "collection": "test",
        "page_count": 1,
    }
    (d / "meta.json").write_text(json.dumps(meta), encoding="utf-8")

    pages = [
        {
            "page_number": 1,
            "text": "Some text. Figure 1: Plan view of the building.",
            "headings": ["Introduction"],
            "section_boundaries": [],
            "figure_refs": [],
            "table_refs": [],
            "thumbnail_path": "",
            "has_annotations": False,
            "annotation_ids": [],
        }
    ]
    with open(d / "pages.jsonl", "w", encoding="utf-8") as f:
        for p in pages:
            f.write(json.dumps(p) + "\n")

    chunks = [
        {
            "chunk_id": "c001",
            "text": "Some text about architecture.",
            "source_pages": [1],
            "emphasized": False,
        }
    ]
    with open(d / "chunks.jsonl", "w", encoding="utf-8") as f:
        for c in chunks:
            f.write(json.dumps(c) + "\n")

    # Create figures directory and sidecar(s)
    figures_dir = d / "figures"
    figures_dir.mkdir()

    if figure_ids is None:
        figure_ids = ["fig_0001"]

    for fid in figure_ids:
        image_filename = f"{fid}.png"
        if with_image:
            # Write a minimal dummy image file (not a real PNG, just bytes)
            (figures_dir / image_filename).write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 32)
            image_path_in_sidecar = f"figures/{image_filename}"
        else:
            image_path_in_sidecar = ""

        sidecar = {
            "figure_id": fid,
            "source_page": 1,
            "image_path": image_path_in_sidecar,
            "bbox": [10.0, 20.0, 200.0, 300.0],
            "extraction_method": "embedded",
        }
        (figures_dir / f"{fid}.json").write_text(
            json.dumps(sidecar, indent=2), encoding="utf-8"
        )

    return d


def _make_llm_fn(response_texts: list[str]) -> MagicMock:
    """Create a mock llm_fn returning each response_text in sequence."""
    return MagicMock(side_effect=response_texts)


def _make_config(figure_batch_size: int = 6) -> dict:
    return {
        "llm": {"model": "claude-test", "api_key_env": "ANTHROPIC_API_KEY"},
        "enrichment": {
            "prompt_version": "enrich-v1.0",
            "enrichment_schema_version": "1",
            "chunk_batch_target": 50,
            "figure_batch_size": figure_batch_size,
            "max_retries": 3,
        },
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestEnrichFiguresStage:
    def test_vision_path_has_analysis_mode_vision(self, tmp_path):
        """When figure has an image file, analysis_mode should be 'vision'."""
        output_dir = _make_extracted_dir(tmp_path, figure_ids=["fig_0001"], with_image=True)
        llm_fn = _make_llm_fn([_make_figure_response(["fig_0001"])])
        config = _make_config()

        result = enrich_figures_stage(output_dir, config, llm_fn, force=True)

        assert result["status"] == "enriched", result["detail"]

        sidecar_path = output_dir / "figures" / "fig_0001.json"
        sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
        assert sidecar["analysis_mode"] == "vision"

    def test_text_fallback_when_no_image(self, tmp_path):
        """When figure has no image, analysis_mode should be 'text_fallback'."""
        output_dir = _make_extracted_dir(tmp_path, figure_ids=["fig_0001"], with_image=False)
        llm_fn = _make_llm_fn([_make_figure_response(["fig_0001"])])
        config = _make_config()

        result = enrich_figures_stage(output_dir, config, llm_fn, force=True)

        assert result["status"] == "enriched", result["detail"]

        sidecar_path = output_dir / "figures" / "fig_0001.json"
        sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
        assert sidecar["analysis_mode"] == "text_fallback"

    def test_stamp_written_per_figure_sidecar(self, tmp_path):
        """After enrichment, each sidecar should contain _enrichment_stamp."""
        output_dir = _make_extracted_dir(tmp_path, figure_ids=["fig_0001"], with_image=True)
        llm_fn = _make_llm_fn([_make_figure_response(["fig_0001"])])
        config = _make_config()

        enrich_figures_stage(output_dir, config, llm_fn, force=True)

        sidecar_path = output_dir / "figures" / "fig_0001.json"
        sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
        assert "_enrichment_stamp" in sidecar
        stamp = sidecar["_enrichment_stamp"]
        assert "prompt_version" in stamp
        assert "model" in stamp
        assert "input_fingerprint" in stamp

    def test_enriched_fields_written_to_sidecar(self, tmp_path):
        """visual_type, description, and caption should be written to the sidecar."""
        output_dir = _make_extracted_dir(tmp_path, figure_ids=["fig_0001"], with_image=True)
        llm_fn = _make_llm_fn([_make_figure_response(["fig_0001"])])
        config = _make_config()

        enrich_figures_stage(output_dir, config, llm_fn, force=True)

        sidecar_path = output_dir / "figures" / "fig_0001.json"
        sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
        assert "visual_type" in sidecar
        assert sidecar["visual_type"]["value"] == "plan"
        assert "description" in sidecar
        assert "Floor plan" in sidecar["description"]["value"]
        assert "caption" in sidecar
        assert "Figure 1" in sidecar["caption"]["value"]

    def test_skipped_when_no_figures_dir(self, tmp_path):
        """If there is no figures directory, the stage is skipped."""
        output_dir = tmp_path / "extracted" / "test123"
        output_dir.mkdir(parents=True)
        meta = {
            "material_id": "test123",
            "title": "Test",
            "authors": [],
            "year": "2024",
            "raw_keywords": [],
            "raw_document_type": "paper",
            "domain": "research",
            "collection": "test",
            "page_count": 1,
        }
        (output_dir / "meta.json").write_text(json.dumps(meta), encoding="utf-8")
        (output_dir / "pages.jsonl").write_text("", encoding="utf-8")

        llm_fn = MagicMock()
        config = _make_config()

        result = enrich_figures_stage(output_dir, config, llm_fn, force=True)
        assert result["status"] == "skipped"
        llm_fn.assert_not_called()

    def test_skipped_when_not_stale(self, tmp_path):
        """Second run without force should be skipped if nothing changed."""
        output_dir = _make_extracted_dir(tmp_path, figure_ids=["fig_0001"], with_image=True)
        llm_fn = _make_llm_fn([
            _make_figure_response(["fig_0001"]),
            _make_figure_response(["fig_0001"]),
        ])
        config = _make_config()

        result1 = enrich_figures_stage(output_dir, config, llm_fn, force=True)
        assert result1["status"] == "enriched"

        call_count_after_first = llm_fn.call_count

        result2 = enrich_figures_stage(output_dir, config, llm_fn, force=False)
        assert result2["status"] == "skipped"
        assert llm_fn.call_count == call_count_after_first

    def test_multiple_figures_single_batch(self, tmp_path):
        """Two figures within batch_size should result in a single LLM call."""
        figure_ids = ["fig_0001", "fig_0002"]
        output_dir = _make_extracted_dir(tmp_path, figure_ids=figure_ids, with_image=False)
        llm_fn = _make_llm_fn([_make_figure_response(figure_ids)])
        config = _make_config(figure_batch_size=6)

        result = enrich_figures_stage(output_dir, config, llm_fn, force=True)

        assert result["status"] == "enriched"
        assert llm_fn.call_count == 1

        # Both sidecars should be enriched
        for fid in figure_ids:
            sidecar = json.loads(
                (output_dir / "figures" / f"{fid}.json").read_text(encoding="utf-8")
            )
            assert "_enrichment_stamp" in sidecar
            assert "analysis_mode" in sidecar

    def test_fails_when_figure_missing_from_response(self, tmp_path):
        """If LLM omits a figure_id from its response, stage should fail."""
        figure_ids = ["fig_0001", "fig_0002"]
        output_dir = _make_extracted_dir(tmp_path, figure_ids=figure_ids, with_image=False)
        # Only return fig_0001, omit fig_0002
        incomplete = _make_figure_response(["fig_0001"])
        llm_fn = _make_llm_fn([incomplete])
        config = _make_config()

        result = enrich_figures_stage(output_dir, config, llm_fn, force=True)
        assert result["status"] == "failed"
        assert "fig_0002" in result["detail"]

    def test_fails_when_required_field_missing(self, tmp_path):
        """If a figure response lacks visual_type, stage should fail."""
        output_dir = _make_extracted_dir(tmp_path, figure_ids=["fig_0001"], with_image=False)
        bad = json.dumps({
            "figures": [{
                "figure_id": "fig_0001",
                # no visual_type
                "description": {"value": "A plan", "source_pages": [1], "evidence_spans": [], "confidence": 0.8},
                "caption": {"value": "Fig 1", "source_pages": [1], "evidence_spans": [], "confidence": 0.8},
            }]
        })
        llm_fn = _make_llm_fn([bad])
        config = _make_config()

        result = enrich_figures_stage(output_dir, config, llm_fn, force=True)
        assert result["status"] == "failed"
        assert "visual_type" in result["detail"]

    def test_fails_when_field_dict_missing_value_key(self, tmp_path):
        """If a field dict is present but lacks 'value', stage should fail."""
        output_dir = _make_extracted_dir(tmp_path, figure_ids=["fig_0001"], with_image=False)
        bad = json.dumps({
            "figures": [{
                "figure_id": "fig_0001",
                "visual_type": {"confidence": 0.9},  # dict but no 'value'
                "description": {"value": "A plan", "source_pages": [1], "evidence_spans": [], "confidence": 0.8},
                "caption": {"value": "Fig 1", "source_pages": [1], "evidence_spans": [], "confidence": 0.8},
            }]
        })
        llm_fn = _make_llm_fn([bad])
        config = _make_config()

        result = enrich_figures_stage(output_dir, config, llm_fn, force=True)
        assert result["status"] == "failed"
        assert "visual_type" in result["detail"]
        assert "value" in result["detail"]
