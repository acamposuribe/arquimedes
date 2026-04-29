"""Tests for enrich_figures: figure-level LLM enrichment stage."""

from __future__ import annotations

import json
import re
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from arquimedes.enrich_figures import enrich_figures_stage


# ---------------------------------------------------------------------------
# Shared mock responses
# ---------------------------------------------------------------------------


def _make_figure_response(figure_ids: list[str]) -> str:
    """Build a mock figure-batch LLM response for the given figure IDs.

    Uses compact JSONL format: one JSON object per line.
    Format: {"id":"fig_NNN","vt":"plan","rel":"substantive","desc":"...","cap":"..."}
    """
    lines = [
        json.dumps({
            "id": fid,
            "vt": "plan",
            "rel": "substantive",
            "desc": f"Floor plan of building for {fid}",
            "cap": "Figure 1: Plan view",
        })
        for fid in figure_ids
    ]
    return "\n".join(lines)


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
    fn = MagicMock(side_effect=response_texts)
    fn.last_model = "test-agent"
    return fn


def _make_config(figure_batch_size: int = 6, figure_parallel_requests: int = 1) -> dict:
    return {
        "llm": {"agent_cmd": "test-agent --print"},
        "enrichment": {
            "prompt_version": "enrich-v1.0",
            "enrichment_schema_version": "1",
            "chunk_batch_target": 50,
            "figure_batch_size": figure_batch_size,
            "figure_parallel_requests": figure_parallel_requests,
            "max_retries": 3,
        },
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestEnrichFiguresStage:
    def test_parallel_requests_use_worker_local_llm_clones(self, tmp_path):
        figure_ids = ["fig_0001", "fig_0002"]
        output_dir = _make_extracted_dir(tmp_path, figure_ids=figure_ids, with_image=False)
        config = _make_config(figure_batch_size=1, figure_parallel_requests=2)
        created_models: list[str] = []

        class _CloneLlm:
            def __init__(self, model_name: str):
                self.last_model = model_name

            def __call__(self, system, messages):
                del system
                prompt_text = json.dumps(messages)
                figure_ids = re.findall(r"### Figure: ([^\\n]+)", prompt_text)
                return _make_figure_response(figure_ids)

        class _BaseLlm:
            def __init__(self):
                self.last_model = "base"
                self.calls = 0

            def __call__(self, system, messages):
                del system, messages
                self.calls += 1
                raise AssertionError("base llm should not be called when worker clones are available")

        base_llm = _BaseLlm()

        def _factory():
            model_name = f"clone-{len(created_models) + 1}"
            created_models.append(model_name)
            return _CloneLlm(model_name)

        base_llm._arq_factory = _factory

        result = enrich_figures_stage(output_dir, config, base_llm, force=True)

        assert result["status"] == "enriched", result["detail"]
        assert base_llm.calls == 0
        assert len(created_models) == 2
        for figure_id in figure_ids:
            sidecar = json.loads((output_dir / "figures" / f"{figure_id}.json").read_text())
            assert sidecar["_enrichment_stamp"]["model"] in created_models

    def test_accepts_json_array_response(self, tmp_path):
        output_dir = _make_extracted_dir(tmp_path, figure_ids=["fig_0001"], with_image=False)
        resp = json.dumps([
            {
                "id": "fig_0001",
                "vt": "plan",
                "rel": "substantive",
                "desc": "Floor plan in array form",
                "cap": "Figure 1",
            }
        ])
        llm_fn = _make_llm_fn([resp])
        config = _make_config()

        result = enrich_figures_stage(output_dir, config, llm_fn, force=True)

        assert result["status"] == "enriched", result["detail"]
        sidecar = json.loads((output_dir / "figures" / "fig_0001.json").read_text())
        assert sidecar["description"]["value"] == "Floor plan in array form"

    def test_accepts_long_form_keys_in_wrapped_object(self, tmp_path):
        output_dir = _make_extracted_dir(tmp_path, figure_ids=["fig_0001"], with_image=False)
        resp = json.dumps({
            "figures": [
                {
                    "figure_id": "fig_0001",
                    "visual_type": "photo",
                    "relevance": "substantive",
                    "description": "Wrapped response using long-form keys",
                    "caption": "Wrapped caption",
                }
            ]
        })
        llm_fn = _make_llm_fn([resp])
        config = _make_config()

        result = enrich_figures_stage(output_dir, config, llm_fn, force=True)

        assert result["status"] == "enriched", result["detail"]
        sidecar = json.loads((output_dir / "figures" / "fig_0001.json").read_text())
        assert sidecar["visual_type"]["value"] == "photo"
        assert sidecar["caption"]["value"] == "Wrapped caption"

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
        assert sidecar.get("relevance") == "substantive"

    def test_figure_source_page_stays_on_sidecar_and_field_provenance_omitted(self, tmp_path):
        output_dir = _make_extracted_dir(tmp_path, figure_ids=["fig_0001"], with_image=True)
        llm_fn = _make_llm_fn([_make_figure_response(["fig_0001"])])
        config = _make_config()

        result = enrich_figures_stage(output_dir, config, llm_fn, force=True)

        assert result["status"] == "enriched"
        sidecar = json.loads((output_dir / "figures" / "fig_0001.json").read_text())
        assert sidecar["source_page"] == 1
        assert sidecar["visual_type"] == {"value": "plan"}
        assert "provenance" not in sidecar["visual_type"]
        assert sidecar["_enrichment_stamp"]["model"] == "test-agent"

    def test_figure_fields_are_value_only(self, tmp_path):
        output_dir = _make_extracted_dir(tmp_path, figure_ids=["fig_0001"], with_image=True)
        llm_fn = _make_llm_fn([_make_figure_response(["fig_0001"])])
        config = _make_config()

        result = enrich_figures_stage(output_dir, config, llm_fn, force=True)

        assert result["status"] == "enriched"
        sidecar = json.loads((output_dir / "figures" / "fig_0001.json").read_text())
        assert sidecar["visual_type"] == {"value": "plan"}
        assert sidecar["description"] == {"value": "Floor plan of building for fig_0001"}
        assert sidecar["caption"] == {"value": "Figure 1: Plan view"}

    def test_tiny_artifact_with_artifact_description_is_deleted(self, tmp_path):
        output_dir = _make_extracted_dir(tmp_path, figure_ids=["fig_0001"], with_image=True)
        sidecar_path = output_dir / "figures" / "fig_0001.json"
        sidecar = json.loads(sidecar_path.read_text())
        sidecar["bbox"] = [5.0, 5.0, 28.0, 33.0]
        sidecar_path.write_text(json.dumps(sidecar, indent=2), encoding="utf-8")

        resp = json.dumps({
            "id": "fig_0001",
            "vt": "photo",
            "rel": "substantive",
            "desc": "Tiny cropped fragment near the page edge; likely scanner artifact rather than a standalone figure.",
            "cap": "",
        })
        llm_fn = _make_llm_fn([resp])
        config = _make_config()

        result = enrich_figures_stage(output_dir, config, llm_fn, force=True)

        assert result["status"] == "enriched"
        assert "1 deleted" in result["detail"]
        assert not sidecar_path.exists()

    def test_relevance_stored_on_sidecar(self, tmp_path):
        """relevance from LLM response should be stored as a plain string on the sidecar."""
        output_dir = _make_extracted_dir(tmp_path, figure_ids=["fig_0001"], with_image=False)
        resp = json.dumps({"id": "fig_0001", "vt": "photo", "rel": "substantive",
                           "desc": "Portrait photograph of an architect", "cap": "Portrait of architect"})
        llm_fn = _make_llm_fn([resp])
        config = _make_config()

        result = enrich_figures_stage(output_dir, config, llm_fn, force=True)
        assert result["status"] == "enriched"

        sidecar = json.loads((output_dir / "figures" / "fig_0001.json").read_text())
        assert sidecar["relevance"] == "substantive"

    def test_deletes_decorative_figure_and_clears_page_ref(self, tmp_path):
        output_dir = _make_extracted_dir(tmp_path, figure_ids=["fig_0001"], with_image=True)
        pages = [json.loads(line) for line in (output_dir / "pages.jsonl").read_text().splitlines() if line.strip()]
        pages[0]["figure_refs"] = ["fig_0001"]
        with open(output_dir / "pages.jsonl", "w", encoding="utf-8") as f:
            for p in pages:
                f.write(json.dumps(p) + "\n")

        resp = json.dumps({"id": "fig_0001", "vt": "photo", "rel": "decorative",
                           "desc": "Publisher logo on a blank background.", "cap": ""})
        llm_fn = _make_llm_fn([resp])
        config = _make_config()

        result = enrich_figures_stage(output_dir, config, llm_fn, force=True)
        assert result["status"] == "enriched"
        assert "1 deleted" in result["detail"]
        assert not (output_dir / "figures" / "fig_0001.json").exists()
        assert not (output_dir / "figures" / "fig_0001.png").exists()

        page = json.loads((output_dir / "pages.jsonl").read_text().splitlines()[0])
        assert page["figure_refs"] == []

    def test_deletes_full_page_scan_artifact_even_if_mislabeled_substantive(self, tmp_path):
        output_dir = _make_extracted_dir(tmp_path, figure_ids=["fig_0001"], with_image=True)
        sidecar_path = output_dir / "figures" / "fig_0001.json"
        sidecar = json.loads(sidecar_path.read_text())
        sidecar["bbox"] = [0.0, 0.0, 458.4, 762.0]
        sidecar["extraction_method"] = "embedded"
        sidecar_path.write_text(json.dumps(sidecar, indent=2), encoding="utf-8")

        resp = json.dumps({"id": "fig_0001", "vt": "photo", "rel": "substantive",
                           "desc": "Scanned article text page with continuous prose and no standalone illustration visible.",
                           "cap": ""})
        llm_fn = _make_llm_fn([resp])
        config = _make_config()

        result = enrich_figures_stage(output_dir, config, llm_fn, force=True)
        assert result["status"] == "enriched"
        assert "1 deleted" in result["detail"]
        assert not sidecar_path.exists()

    def test_deletes_spanish_scan_artifact_description(self, tmp_path):
        output_dir = _make_extracted_dir(tmp_path, figure_ids=["fig_0001"], with_image=True)
        sidecar_path = output_dir / "figures" / "fig_0001.json"
        sidecar = json.loads(sidecar_path.read_text())
        sidecar["bbox"] = [0.0, 0.0, 458.4, 762.0]
        sidecar["extraction_method"] = "embedded"
        sidecar_path.write_text(json.dumps(sidecar, indent=2), encoding="utf-8")

        resp = json.dumps({"id": "fig_0001", "vt": "photo", "rel": "substantive",
                           "desc": "Página escaneada con texto continuo y sin ilustración independiente visible.",
                           "cap": ""})
        llm_fn = _make_llm_fn([resp])
        config = _make_config()

        result = enrich_figures_stage(output_dir, config, llm_fn, force=True)
        assert result["status"] == "enriched"
        assert "1 deleted" in result["detail"]
        assert not sidecar_path.exists()

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

    def test_batch_failure_keeps_completed_figure_sidecars(self, tmp_path):
        """Completed figure batches should be saved before a later batch failure."""
        figure_ids = ["fig_0001", "fig_0002"]
        output_dir = _make_extracted_dir(tmp_path, figure_ids=figure_ids, with_image=False)
        llm_fn = _make_llm_fn([
            _make_figure_response(["fig_0001"]),
            _make_figure_response(["fig_9999"]),
        ])
        config = _make_config(figure_batch_size=1)

        result = enrich_figures_stage(output_dir, config, llm_fn, force=True)

        assert result["status"] == "failed"
        assert "saved 1/2 figures" in result["detail"]
        saved = json.loads((output_dir / "figures" / "fig_0001.json").read_text())
        failed = json.loads((output_dir / "figures" / "fig_0002.json").read_text())
        assert "_enrichment_stamp" in saved
        assert "_enrichment_stamp" not in failed

    def test_fails_when_figure_id_not_in_response(self, tmp_path):
        """If LLM returns a line with a different figure_id, the expected one is missing."""
        output_dir = _make_extracted_dir(tmp_path, figure_ids=["fig_0001"], with_image=False)
        # Response references wrong figure id
        bad = json.dumps({"id": "fig_9999", "vt": "plan", "rel": "substantive",
                          "desc": "Wrong figure", "cap": ""})
        llm_fn = _make_llm_fn([bad])
        config = _make_config()

        result = enrich_figures_stage(output_dir, config, llm_fn, force=True)
        assert result["status"] == "failed"
        assert "fig_0001" in result["detail"]

    def test_invalid_visual_type_stored_as_empty(self, tmp_path):
        """If LLM returns an invalid visual_type, it is stored as empty string (no failure)."""
        output_dir = _make_extracted_dir(tmp_path, figure_ids=["fig_0001"], with_image=False)
        resp = json.dumps({"id": "fig_0001", "vt": "not_a_valid_type", "rel": "substantive",
                           "desc": "A plan", "cap": "Fig 1"})
        llm_fn = _make_llm_fn([resp])
        config = _make_config()

        result = enrich_figures_stage(output_dir, config, llm_fn, force=True)
        assert result["status"] == "enriched"
        sidecar = json.loads((output_dir / "figures" / "fig_0001.json").read_text())
        assert sidecar["visual_type"]["value"] == ""
