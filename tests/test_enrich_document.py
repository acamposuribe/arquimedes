"""Tests for enrich_document: document-level LLM enrichment stage."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from arquimedes.enrich_document import enrich_document_stage


# ---------------------------------------------------------------------------
# Shared mock response
# ---------------------------------------------------------------------------

MOCK_DOC_RESPONSE = json.dumps({
    "summary": {
        "value": "A study of thermal mass in residential architecture.",
        "source_pages": [1],
        "evidence_spans": ["This paper examines..."],
        "confidence": 0.9,
    },
    "document_type": {
        "value": "paper",
        "source_pages": [1],
        "evidence_spans": ["Published in Journal of Architecture"],
        "confidence": 0.95,
    },
    "keywords": {
        "value": ["architecture", "thermal mass"],
        "source_pages": [1, 2],
        "evidence_spans": ["thermal mass is key"],
        "confidence": 0.85,
    },
    "facets": {
        "building_type": {
            "value": "residential",
            "source_pages": [1],
            "evidence_spans": ["residential housing"],
            "confidence": 0.8,
        }
    },
    "concepts": [
        {
            "concept_name": "thermal mass",
            "relevance": "primary topic",
            "source_pages": [1],
            "evidence_spans": ["Thermal mass slows heat transfer"],
        }
    ],
})


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


def _make_client(response_text: str) -> MagicMock:
    """Create a mock Anthropic client that returns response_text."""
    client = MagicMock()
    mock_response = MagicMock()
    mock_response.content = [MagicMock(text=response_text)]
    client.messages.create.return_value = mock_response
    return client


def _make_config() -> dict:
    return {
        "llm": {"model": "claude-test", "api_key_env": "ANTHROPIC_API_KEY"},
        "enrichment": {
            "prompt_version": "enrich-v1.0",
            "enrichment_schema_version": "1",
            "chunk_batch_target": 50,
        },
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestEnrichDocumentStage:
    def test_meta_json_gets_enriched_fields(self, tmp_path):
        """After enrichment, meta.json should contain summary, document_type, keywords."""
        output_dir = _make_extracted_dir(tmp_path)
        client = _make_client(MOCK_DOC_RESPONSE)
        config = _make_config()

        result = enrich_document_stage(output_dir, config, client, force=True)

        assert result["status"] == "enriched"
        meta = json.loads((output_dir / "meta.json").read_text(encoding="utf-8"))
        assert "summary" in meta
        assert meta["summary"]["value"] == "A study of thermal mass in residential architecture."
        assert "document_type" in meta
        assert meta["document_type"]["value"] == "paper"
        assert "keywords" in meta
        assert "architecture" in meta["keywords"]["value"]

    def test_concepts_jsonl_is_written(self, tmp_path):
        """concepts.jsonl should be created with one entry per concept."""
        output_dir = _make_extracted_dir(tmp_path)
        client = _make_client(MOCK_DOC_RESPONSE)
        config = _make_config()

        enrich_document_stage(output_dir, config, client, force=True)

        concepts_path = output_dir / "concepts.jsonl"
        assert concepts_path.exists(), "concepts.jsonl should be written"
        lines = [
            json.loads(line)
            for line in concepts_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        assert len(lines) == 1
        assert lines[0]["concept_name"] == "thermal mass"

    def test_enrichment_stamp_written_to_meta(self, tmp_path):
        """After enrichment, meta.json should contain _enrichment_stamp."""
        output_dir = _make_extracted_dir(tmp_path)
        client = _make_client(MOCK_DOC_RESPONSE)
        config = _make_config()

        enrich_document_stage(output_dir, config, client, force=True)

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
        client = _make_client(MOCK_DOC_RESPONSE)
        config = _make_config()

        # First run
        result1 = enrich_document_stage(output_dir, config, client, force=True)
        assert result1["status"] == "enriched"

        call_count_after_first = client.messages.create.call_count

        # Second run without force — should be skipped (inputs haven't changed)
        result2 = enrich_document_stage(output_dir, config, client, force=False)
        assert result2["status"] == "skipped"
        # No additional LLM calls
        assert client.messages.create.call_count == call_count_after_first

    def test_force_re_enriches_even_if_not_stale(self, tmp_path):
        """force=True should re-enrich even when stamp matches."""
        output_dir = _make_extracted_dir(tmp_path)
        client = _make_client(MOCK_DOC_RESPONSE)
        config = _make_config()

        result1 = enrich_document_stage(output_dir, config, client, force=True)
        assert result1["status"] == "enriched"

        call_count_after_first = client.messages.create.call_count

        result2 = enrich_document_stage(output_dir, config, client, force=True)
        assert result2["status"] == "enriched"
        assert client.messages.create.call_count > call_count_after_first

    def test_provenance_fields_present_in_enriched_field(self, tmp_path):
        """Each enriched field should carry a provenance sub-object."""
        output_dir = _make_extracted_dir(tmp_path)
        client = _make_client(MOCK_DOC_RESPONSE)
        config = _make_config()

        enrich_document_stage(output_dir, config, client, force=True)

        meta = json.loads((output_dir / "meta.json").read_text(encoding="utf-8"))
        prov = meta["summary"]["provenance"]
        assert prov["model"] == "claude-test"
        assert prov["prompt_version"] == "enrich-v1.0"
        assert prov["confidence"] == pytest.approx(0.9)

    def test_failed_status_on_llm_error(self, tmp_path):
        """If the LLM call raises EnrichmentError, status should be 'failed'."""
        from arquimedes.enrich_llm import EnrichmentError

        output_dir = _make_extracted_dir(tmp_path)
        config = _make_config()

        client = MagicMock()
        client.messages.create.side_effect = EnrichmentError("LLM unavailable")

        result = enrich_document_stage(output_dir, config, client, force=True)
        assert result["status"] == "failed"
        assert "LLM" in result["detail"] or "unavailable" in result["detail"]
