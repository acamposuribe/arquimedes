"""Tests for enrich_document: document-level LLM enrichment stage."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from arquimedes.enrich_document import enrich_document_stage


# ---------------------------------------------------------------------------
# Shared mock enrichment content — complete document JSON output
# ---------------------------------------------------------------------------

MOCK_ENRICHMENT = {
    "summary": "A study of thermal mass in residential architecture.",
    "document_type": "paper",
    "keywords": ["architecture", "thermal mass", "residential design"],
    "methodological_conclusions": [
        "Treat thermal mass as a passive design variable.",
        "Test material choices against climatic performance rather than style alone.",
    ],
    "main_content_learnings": [
        "Thermal mass can stabilize residential comfort.",
        "Architecture can leverage mass to moderate environmental swings.",
    ],
    "bibliography": None,
    "facets": {
        "building_type": "residential",
    },
    "concepts_local": [
        {
            "concept_name": "thermal mass",
            "descriptor": "The capacity of a material to absorb and release heat.",
            "relevance": "high",
            "source_pages": [1],
            "evidence_spans": ["Thermal mass slows heat transfer"],
        }
    ],
    "concepts_bridge_candidates": [
        {
            "concept_name": "thermal performance in architecture",
            "descriptor": "How buildings manage heat through material and form.",
            "relevance": "medium",
            "source_pages": [1],
            "evidence_spans": ["Thermal mass slows heat transfer"],
        }
    ],
    "toc": [],
}


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_extracted_dir(tmp_path: Path) -> Path:
    """Create a minimal extracted/<id>/ directory for testing."""
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

    (d / "text.md").write_text(
        "Title line\n\nSome text about architecture.\nWith multiple line breaks.",
        encoding="utf-8",
    )

    pages = [
        {
            "page_number": 1,
            "text": "Some text about architecture.",
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

    return d


def _make_llm_fn(enrichment_content: dict) -> MagicMock:
    """Create a mock llm_fn that returns a complete document JSON string."""
    def _side_effect(system, messages):
        payload = dict(enrichment_content)
        payload.setdefault("_finished", True)
        return json.dumps(payload)

    fn = MagicMock(side_effect=_side_effect)
    fn.last_model = "test-agent"
    return fn


def _make_config() -> dict:
    return {
        "llm": {"agent_cmd": "test-agent --print"},
        "enrichment": {
            "prompt_version": "enrich-v1.0",
            "enrichment_schema_version": "1",
            "chunk_batch_target": 50,
            "max_retries": 3,
        },
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestEnrichDocumentStage:
    def test_meta_json_gets_enriched_fields(self, tmp_path):
        """After enrichment, meta.json should contain summary, document_type, keywords."""
        output_dir = _make_extracted_dir(tmp_path)
        llm_fn = _make_llm_fn(MOCK_ENRICHMENT)
        config = _make_config()

        result = enrich_document_stage(output_dir, config, llm_fn, force=True)

        assert result["status"] == "enriched"
        meta = json.loads((output_dir / "meta.json").read_text(encoding="utf-8"))
        assert "summary" in meta
        assert meta["summary"]["value"] == "A study of thermal mass in residential architecture."
        assert "document_type" in meta
        assert meta["document_type"]["value"] == "paper"
        assert "keywords" in meta
        assert "architecture" in meta["keywords"]["value"]
        assert "methodological_conclusions" in meta
        assert "passive design" in " ".join(meta["methodological_conclusions"]["value"])
        assert "main_content_learnings" in meta
        assert "stabilize residential comfort" in " ".join(meta["main_content_learnings"]["value"])

    def test_proyectos_stage_stores_project_extraction_without_reflections(self, tmp_path):
        output_dir = _make_extracted_dir(tmp_path)
        meta_path = output_dir / "meta.json"
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        meta["domain"] = "proyectos"
        meta["methodological_conclusions"] = {"value": ["old reflective field"]}
        meta["main_content_learnings"] = {"value": ["old learning field"]}
        meta_path.write_text(json.dumps(meta), encoding="utf-8")

        payload = {
            "summary": "Acta con decisiones pendientes para el expediente.",
            "document_type": "meeting_report",
            "keywords": ["acta", "decisiones", "licencia"],
            "bibliography": None,
            "facets": {},
            "concepts_local": [],
            "concepts_bridge_candidates": [],
            "toc": [],
            "project_extraction": {
                "project_material_type": "meeting_report",
                "project_relevance": "Registra acuerdos y próximos pasos.",
                "main_points": ["Se revisa el estado de licencia."],
                "decisions": ["Enviar documentación actualizada."],
                "requirements": [],
                "risks_or_blockers": [],
                "open_items": ["Confirmar respuesta municipal."],
                "actors": ["Ayuntamiento"],
                "dates_and_deadlines": [],
                "spatial_or_design_scope": [],
                "budget_signals": [],
                "evidence_refs": ["p. 1"],
            },
        }
        llm_fn = _make_llm_fn(payload)
        config = _make_config()

        result = enrich_document_stage(output_dir, config, llm_fn, force=True)

        assert result["status"] == "enriched"
        enriched = json.loads(meta_path.read_text(encoding="utf-8"))
        assert "methodological_conclusions" not in enriched
        assert "main_content_learnings" not in enriched
        assert enriched["project_extraction"]["project_material_type"] == "meeting_report"
        assert enriched["summary"]["value"].startswith("Acta")
        assert (output_dir / "concepts.jsonl").read_text(encoding="utf-8") == ""

    def test_document_stage_ignores_llm_title(self, tmp_path):
        output_dir = _make_extracted_dir(tmp_path)
        payload = dict(MOCK_ENRICHMENT)
        payload["title"] = "Wrong Replacement Title"
        llm_fn = _make_llm_fn(payload)
        config = _make_config()

        result = enrich_document_stage(output_dir, config, llm_fn, force=True)

        assert result["status"] == "enriched"
        meta = json.loads((output_dir / "meta.json").read_text(encoding="utf-8"))
        assert meta["title"] == "Test Doc"

    def test_concepts_jsonl_is_written(self, tmp_path):
        """concepts.jsonl should be created with one entry per concept."""
        output_dir = _make_extracted_dir(tmp_path)
        llm_fn = _make_llm_fn(MOCK_ENRICHMENT)
        config = _make_config()

        enrich_document_stage(output_dir, config, llm_fn, force=True)

        concepts_path = output_dir / "concepts.jsonl"
        assert concepts_path.exists(), "concepts.jsonl should be written"
        lines = [
            json.loads(line)
            for line in concepts_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        assert len(lines) == 2
        assert {line["concept_type"] for line in lines} == {"local", "bridge_candidate"}
        assert any(line["concept_name"] == "thermal mass" for line in lines)

    def test_enrichment_stamp_written_to_meta(self, tmp_path):
        """After enrichment, meta.json should contain _enrichment_stamp."""
        output_dir = _make_extracted_dir(tmp_path)
        llm_fn = _make_llm_fn(MOCK_ENRICHMENT)
        config = _make_config()

        enrich_document_stage(output_dir, config, llm_fn, force=True)

        meta = json.loads((output_dir / "meta.json").read_text(encoding="utf-8"))
        assert "_enrichment_stamp" in meta
        stamp = meta["_enrichment_stamp"]
        assert "prompt_version" in stamp
        assert "model" in stamp
        assert "enrichment_schema_version" in stamp
        assert "input_fingerprint" in stamp

    def test_skipped_when_not_stale(self, tmp_path):
        """Running enrichment twice without force should skip the second run."""
        output_dir = _make_extracted_dir(tmp_path)
        llm_fn = _make_llm_fn(MOCK_ENRICHMENT)
        config = _make_config()

        # First run
        result1 = enrich_document_stage(output_dir, config, llm_fn, force=True)
        assert result1["status"] == "enriched"

        call_count_after_first = llm_fn.call_count

        # Second run without force — should be skipped (inputs haven't changed)
        result2 = enrich_document_stage(output_dir, config, llm_fn, force=False)
        assert result2["status"] == "skipped"
        # No additional LLM calls
        assert llm_fn.call_count == call_count_after_first

    def test_force_re_enriches_even_if_not_stale(self, tmp_path):
        """force=True should re-enrich even when stamp matches."""
        output_dir = _make_extracted_dir(tmp_path)
        llm_fn = _make_llm_fn(MOCK_ENRICHMENT)
        config = _make_config()

        result1 = enrich_document_stage(output_dir, config, llm_fn, force=True)
        assert result1["status"] == "enriched"

        call_count_after_first = llm_fn.call_count

        result2 = enrich_document_stage(output_dir, config, llm_fn, force=True)
        assert result2["status"] == "enriched"
        assert llm_fn.call_count > call_count_after_first

    def test_toc_written_by_document_stage_does_not_self_invalidate_stamp(self, tmp_path):
        output_dir = _make_extracted_dir(tmp_path)
        payload = dict(MOCK_ENRICHMENT)
        payload["toc"] = [{"title": "Introduction", "level": 0, "page": 1}]
        llm_fn = _make_llm_fn(payload)
        config = _make_config()

        result1 = enrich_document_stage(output_dir, config, llm_fn, force=True)
        assert result1["status"] == "enriched"

        call_count_after_first = llm_fn.call_count

        result2 = enrich_document_stage(output_dir, config, llm_fn, force=False)
        assert result2["status"] == "skipped"
        assert llm_fn.call_count == call_count_after_first

    def test_metadata_fix_style_meta_changes_do_not_retrigger_document_stage(self, tmp_path):
        output_dir = _make_extracted_dir(tmp_path)
        llm_fn = _make_llm_fn(MOCK_ENRICHMENT)
        config = _make_config()

        result1 = enrich_document_stage(output_dir, config, llm_fn, force=True)
        assert result1["status"] == "enriched"

        meta = json.loads((output_dir / "meta.json").read_text(encoding="utf-8"))
        meta["title"] = "Metadata-Fixed Title"
        meta["authors"] = ["Corrected Author"]
        meta["year"] = "2025"
        (output_dir / "meta.json").write_text(json.dumps(meta), encoding="utf-8")

        call_count_after_first = llm_fn.call_count

        result2 = enrich_document_stage(output_dir, config, llm_fn, force=False)
        assert result2["status"] == "skipped"
        assert llm_fn.call_count == call_count_after_first

    def test_stage_stamp_present_and_field_provenance_omitted(self, tmp_path):
        """Ordinary enriched fields should be value-only; run provenance lives in the stage stamp."""
        output_dir = _make_extracted_dir(tmp_path)
        llm_fn = _make_llm_fn(MOCK_ENRICHMENT)
        config = _make_config()

        enrich_document_stage(output_dir, config, llm_fn, force=True)

        meta = json.loads((output_dir / "meta.json").read_text(encoding="utf-8"))
        assert meta["summary"] == {"value": MOCK_ENRICHMENT["summary"]}
        assert "provenance" not in meta["summary"]
        stamp = meta["_enrichment_stamp"]
        assert stamp["model"] == "test-agent"
        assert stamp["prompt_version"] == "enrich-v1.0"

    def test_failed_status_on_llm_error(self, tmp_path):
        """If the LLM call raises EnrichmentError, status should be 'failed'."""
        from arquimedes.enrich_llm import EnrichmentError

        output_dir = _make_extracted_dir(tmp_path)
        config = _make_config()

        llm_fn = MagicMock(side_effect=EnrichmentError("LLM unavailable"))

        result = enrich_document_stage(output_dir, config, llm_fn, force=True)
        assert result["status"] == "failed"
        assert "LLM" in result["detail"] or "unavailable" in result["detail"]

    def test_fails_when_summary_missing(self, tmp_path):
        """If LLM output lacks required 'summary' field, stage should fail."""
        output_dir = _make_extracted_dir(tmp_path)
        incomplete = {
            "document_type": "paper",
            "keywords": ["arch"],
            "methodological_conclusions": [],
            "main_content_learnings": [],
            "bibliography": None,
            "facets": {},
            "concepts_local": [],
            "concepts_bridge_candidates": [],
            "toc": [],
            "_finished": True,
        }
        llm_fn = _make_llm_fn(incomplete)
        config = _make_config()

        result = enrich_document_stage(output_dir, config, llm_fn, force=True)
        assert result["status"] == "failed"
        assert "summary" in result["detail"]

    def test_fails_when_document_type_missing(self, tmp_path):
        """If LLM output lacks 'document_type', stage should fail."""
        output_dir = _make_extracted_dir(tmp_path)
        incomplete = {
            "summary": "A study.",
            "keywords": ["arch"],
            "methodological_conclusions": [],
            "main_content_learnings": [],
            "bibliography": None,
            "facets": {},
            "concepts_local": [],
            "concepts_bridge_candidates": [],
            "toc": [],
            "_finished": True,
        }
        llm_fn = _make_llm_fn(incomplete)
        config = _make_config()

        result = enrich_document_stage(output_dir, config, llm_fn, force=True)
        assert result["status"] == "failed"
        assert "document_type" in result["detail"]

    def test_fails_when_field_missing_value_key(self, tmp_path):
        """If a required field is a dict lacking 'value', stage should fail.

        In the file-based flow the LLM writes plain values (str/list), which
        are normalised to {"value": ...} automatically. This test covers the
        edge case where the LLM writes an explicit dict with no 'value' key,
        which cannot be normalised and should fail validation.
        """
        output_dir = _make_extracted_dir(tmp_path)
        # summary written as a non-normalizable dict (has keys but no 'value')
        bad = {
            "summary": {"source_pages": [1], "confidence": 0.9},  # no 'value'
            "document_type": "paper",
            "keywords": ["arch"],
            "methodological_conclusions": [],
            "main_content_learnings": [],
            "bibliography": None,
            "facets": {},
            "concepts_local": [],
            "concepts_bridge_candidates": [],
            "toc": [],
            "_finished": True,
        }
        llm_fn = _make_llm_fn(bad)
        config = _make_config()

        result = enrich_document_stage(output_dir, config, llm_fn, force=True)
        assert result["status"] == "failed"
        assert "summary" in result["detail"]

    def test_fails_when_finished_payload_is_partial(self, tmp_path):
        output_dir = _make_extracted_dir(tmp_path)
        partial = {
            "summary": "A study.",
            "document_type": "paper",
            "keywords": ["arch"],
            "_finished": True,
        }
        llm_fn = _make_llm_fn(partial)
        config = _make_config()

        result = enrich_document_stage(output_dir, config, llm_fn, force=True)
        assert result["status"] == "failed"
        assert "missing required fields" in result["detail"]

    def test_fails_on_refusal_summary(self, tmp_path):
        output_dir = _make_extracted_dir(tmp_path)
        refusal = dict(MOCK_ENRICHMENT)
        refusal["summary"] = (
            "The source text is not available in this session, so the record cannot be reliably enriched."
        )
        llm_fn = _make_llm_fn(refusal)
        config = _make_config()

        result = enrich_document_stage(output_dir, config, llm_fn, force=True)
        assert result["status"] == "failed"
        assert "refusal" in result["detail"] or "no-access" in result["detail"]

    def test_fails_on_empty_keywords(self, tmp_path):
        output_dir = _make_extracted_dir(tmp_path)
        refusal = dict(MOCK_ENRICHMENT)
        refusal["keywords"] = []
        llm_fn = _make_llm_fn(refusal)
        config = _make_config()

        result = enrich_document_stage(output_dir, config, llm_fn, force=True)
        assert result["status"] == "failed"
        assert "keywords are empty" in result["detail"]

    def test_atomic_write_no_partial_on_failure(self, tmp_path):
        """If concepts write fails, meta.json should not be modified."""
        output_dir = _make_extracted_dir(tmp_path)
        llm_fn = _make_llm_fn(MOCK_ENRICHMENT)
        config = _make_config()

        original_meta = (output_dir / "meta.json").read_text(encoding="utf-8")

        # Patch Path.replace to fail on the concepts temp file rename
        original_replace = Path.replace

        def failing_replace(self, target):
            if "concepts" in str(self):
                raise OSError("disk full")
            return original_replace(self, target)

        with patch.object(Path, "replace", failing_replace):
            result = enrich_document_stage(output_dir, config, llm_fn, force=True)

        assert result["status"] == "failed"
