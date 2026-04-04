"""Tests for enrich_chunks: chunk-level LLM enrichment stage."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, call

import pytest

from arquimedes.enrich_chunks import enrich_chunks_stage


# ---------------------------------------------------------------------------
# Shared mock responses
# ---------------------------------------------------------------------------


def _make_chunk_response(chunk_ids: list[str]) -> str:
    """Build a mock chunk-batch LLM response for the given chunk IDs."""
    return json.dumps({
        "chunks": [
            {
                "chunk_id": cid,
                "summary": {
                    "value": f"Summary for {cid}",
                    "source_pages": [1],
                    "evidence_spans": ["Some text..."],
                    "confidence": 0.9,
                },
                "keywords": {
                    "value": ["term1", "term2"],
                    "source_pages": [1],
                    "evidence_spans": ["Some text..."],
                    "confidence": 0.85,
                },
            }
            for cid in chunk_ids
        ]
    })


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_extracted_dir(tmp_path: Path, n_chunks: int = 1) -> Path:
    """Create a minimal extracted/<id>/ directory with n_chunks chunks."""
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
            "chunk_id": f"c{i:03d}",
            "text": f"Chunk text {i} about architecture.",
            "source_pages": [1],
            "emphasized": False,
        }
        for i in range(1, n_chunks + 1)
    ]
    with open(d / "chunks.jsonl", "w", encoding="utf-8") as f:
        for c in chunks:
            f.write(json.dumps(c) + "\n")

    return d


def _make_client_returning(response_texts: list[str]) -> MagicMock:
    """Create a mock client returning each response_text in sequence."""
    client = MagicMock()
    responses = []
    for text in response_texts:
        mock_resp = MagicMock()
        mock_resp.content = [MagicMock(text=text)]
        responses.append(mock_resp)
    client.messages.create.side_effect = responses
    return client


def _make_config(batch_target: int = 50) -> dict:
    return {
        "llm": {"model": "claude-test", "api_key_env": "ANTHROPIC_API_KEY"},
        "enrichment": {
            "prompt_version": "enrich-v1.0",
            "enrichment_schema_version": "1",
            "chunk_batch_target": batch_target,
        },
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestEnrichChunksStage:
    def test_chunks_get_enriched_fields(self, tmp_path):
        """After enrichment, chunks.jsonl records should have summary and keywords."""
        output_dir = _make_extracted_dir(tmp_path, n_chunks=1)
        chunk_ids = ["c001"]
        client = _make_client_returning([_make_chunk_response(chunk_ids)])
        config = _make_config()

        result = enrich_chunks_stage(output_dir, config, client, force=True)

        assert result["status"] == "enriched", result["detail"]

        chunks_path = output_dir / "chunks.jsonl"
        chunks = [
            json.loads(line)
            for line in chunks_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        assert len(chunks) == 1
        c = chunks[0]
        assert "summary" in c
        assert c["summary"]["value"] == "Summary for c001"
        assert "keywords" in c
        assert "term1" in c["keywords"]["value"]

    def test_chunk_enrichment_stamps_written(self, tmp_path):
        """chunk_enrichment_stamps.json should be written after successful enrichment."""
        output_dir = _make_extracted_dir(tmp_path, n_chunks=1)
        client = _make_client_returning([_make_chunk_response(["c001"])])
        config = _make_config()

        enrich_chunks_stage(output_dir, config, client, force=True)

        stamps_path = output_dir / "chunk_enrichment_stamps.json"
        assert stamps_path.exists(), "chunk_enrichment_stamps.json should be created"
        stamps = json.loads(stamps_path.read_text(encoding="utf-8"))
        assert "_stage" in stamps
        stage_stamp = stamps["_stage"]
        assert "prompt_version" in stage_stamp
        assert "input_fingerprint" in stage_stamp

    def test_batch_failure_no_writes(self, tmp_path):
        """If the LLM call fails for any batch, no writes should occur (atomic)."""
        from arquimedes.enrich_llm import EnrichmentError

        output_dir = _make_extracted_dir(tmp_path, n_chunks=3)
        config = _make_config(batch_target=50)

        # Read original chunks before enrichment
        original_chunks_text = (output_dir / "chunks.jsonl").read_text(encoding="utf-8")

        client = MagicMock()
        client.messages.create.side_effect = EnrichmentError("LLM failed")

        result = enrich_chunks_stage(output_dir, config, client, force=True)

        assert result["status"] == "failed"
        # chunks.jsonl should be unchanged
        assert (output_dir / "chunks.jsonl").read_text(encoding="utf-8") == original_chunks_text
        # stamps should not have been written
        assert not (output_dir / "chunk_enrichment_stamps.json").exists()

    def test_100_chunks_with_batch_50_makes_2_llm_calls(self, tmp_path):
        """100 chunks with batch_target=50 should result in exactly 2 LLM calls."""
        output_dir = _make_extracted_dir(tmp_path, n_chunks=100)
        config = _make_config(batch_target=50)

        # Prepare two batch responses
        batch1_ids = [f"c{i:03d}" for i in range(1, 51)]
        batch2_ids = [f"c{i:03d}" for i in range(51, 101)]
        client = _make_client_returning([
            _make_chunk_response(batch1_ids),
            _make_chunk_response(batch2_ids),
        ])

        result = enrich_chunks_stage(output_dir, config, client, force=True)

        assert result["status"] == "enriched", result["detail"]
        assert client.messages.create.call_count == 2

    def test_skipped_when_not_stale(self, tmp_path):
        """Second run without force should be skipped."""
        output_dir = _make_extracted_dir(tmp_path, n_chunks=1)
        chunk_ids = ["c001"]
        client = _make_client_returning([
            _make_chunk_response(chunk_ids),
            _make_chunk_response(chunk_ids),
        ])
        config = _make_config()

        result1 = enrich_chunks_stage(output_dir, config, client, force=True)
        assert result1["status"] == "enriched"

        call_count_after_first = client.messages.create.call_count

        result2 = enrich_chunks_stage(output_dir, config, client, force=False)
        assert result2["status"] == "skipped"
        assert client.messages.create.call_count == call_count_after_first

    def test_original_chunk_fields_preserved(self, tmp_path):
        """Original chunk fields (text, source_pages, emphasized) must be kept."""
        output_dir = _make_extracted_dir(tmp_path, n_chunks=1)
        client = _make_client_returning([_make_chunk_response(["c001"])])
        config = _make_config()

        enrich_chunks_stage(output_dir, config, client, force=True)

        chunks = [
            json.loads(line)
            for line in (output_dir / "chunks.jsonl").read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        c = chunks[0]
        assert c["chunk_id"] == "c001"
        assert "Chunk text" in c["text"]
        assert c["source_pages"] == [1]
        assert c["emphasized"] is False

    def test_no_chunks_returns_skipped(self, tmp_path):
        """If chunks.jsonl is empty or absent, the stage should be skipped."""
        output_dir = _make_extracted_dir(tmp_path, n_chunks=0)
        # Overwrite with empty file
        (output_dir / "chunks.jsonl").write_text("", encoding="utf-8")
        client = MagicMock()
        config = _make_config()

        result = enrich_chunks_stage(output_dir, config, client, force=True)
        assert result["status"] == "skipped"
        client.messages.create.assert_not_called()
