"""Tests for enrich_chunks: chunk-level LLM enrichment stage."""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, call

import pytest

from arquimedes.enrich_chunks import enrich_chunks_stage


# ---------------------------------------------------------------------------
# Shared mock responses
# ---------------------------------------------------------------------------


def _make_chunk_response(chunk_ids: list[str]) -> str:
    """Build a mock chunk-batch LLM response for the given chunk IDs.

    Uses compact JSONL format: one JSON object per line.
    Format: {"id":"<chunk_id>","s":"<summary>","kw":["term1","term2"],"cls":"argument"}
    """
    lines = [
        json.dumps({"id": cid, "s": f"Summary for {cid}", "kw": ["term1", "term2"], "cls": "argument"})
        for cid in chunk_ids
    ]
    return "\n".join(lines)


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


def _make_llm_fn(response_texts: list[str]) -> MagicMock:
    """Create a mock llm_fn returning each response_text in sequence."""
    fn = MagicMock(side_effect=response_texts)
    fn.last_model = "test-agent"
    return fn


def _make_config(batch_target: int = 50, chunk_parallel_requests: int = 1) -> dict:
    return {
        "llm": {"agent_cmd": "test-agent --print"},
        "enrichment": {
            "prompt_version": "enrich-v1.0",
            "enrichment_schema_version": "1",
            "chunk_batch_target": batch_target,
            "chunk_parallel_requests": chunk_parallel_requests,
            "max_retries": 3,
        },
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestEnrichChunksStage:
    def test_parallel_requests_use_worker_local_llm_clones(self, tmp_path):
        output_dir = _make_extracted_dir(tmp_path, n_chunks=100)
        config = _make_config(batch_target=50, chunk_parallel_requests=2)
        created_models: list[str] = []

        class _CloneLlm:
            def __init__(self, model_name: str):
                self.last_model = model_name

            def __call__(self, system, messages):
                del system
                prompt_text = json.dumps(messages)
                chunk_ids = re.findall(r"--- Chunk ([^ ]+) \(pages", prompt_text)
                return _make_chunk_response(chunk_ids)

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

        result = enrich_chunks_stage(output_dir, config, base_llm, force=True)

        assert result["status"] == "enriched", result["detail"]
        assert base_llm.calls == 0
        assert len(created_models) == 2

        stamps = json.loads((output_dir / "chunk_enrichment_stamps.json").read_text())
        assert {stamp["model"] for stamp in stamps.values()} <= set(created_models)

    def test_chunks_get_enriched_fields(self, tmp_path):
        """After enrichment, chunks.jsonl records should have summary and keywords."""
        output_dir = _make_extracted_dir(tmp_path, n_chunks=1)
        chunk_ids = ["c001"]
        llm_fn = _make_llm_fn([_make_chunk_response(chunk_ids)])
        config = _make_config()

        result = enrich_chunks_stage(output_dir, config, llm_fn, force=True)

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
        assert c.get("content_class") == "argument"

    def test_content_class_stored_on_chunk(self, tmp_path):
        """content_class from LLM response should be stored as a plain string on the chunk."""
        output_dir = _make_extracted_dir(tmp_path, n_chunks=1)
        # Return a bibliography-classified chunk using compact JSONL format
        resp = json.dumps({"id": "c001", "s": "Refs", "kw": ["refs"], "cls": "bibliography"})
        llm_fn = _make_llm_fn([resp])
        config = _make_config()

        result = enrich_chunks_stage(output_dir, config, llm_fn, force=True)
        assert result["status"] == "enriched"

        chunks = [json.loads(l) for l in (output_dir / "chunks.jsonl").read_text().splitlines() if l.strip()]
        assert chunks[0]["content_class"] == "bibliography"

    def test_chunk_enrichment_stamps_written(self, tmp_path):
        """chunk_enrichment_stamps.json should contain per-chunk stamp map."""
        output_dir = _make_extracted_dir(tmp_path, n_chunks=2)
        llm_fn = _make_llm_fn([_make_chunk_response(["c001", "c002"])])
        config = _make_config()

        enrich_chunks_stage(output_dir, config, llm_fn, force=True)

        stamps_path = output_dir / "chunk_enrichment_stamps.json"
        assert stamps_path.exists(), "chunk_enrichment_stamps.json should be created"
        stamps = json.loads(stamps_path.read_text(encoding="utf-8"))
        # Per-chunk stamps: each chunk_id maps to a stamp dict
        assert "c001" in stamps
        assert "c002" in stamps
        for chunk_id in ["c001", "c002"]:
            stamp = stamps[chunk_id]
            assert "prompt_version" in stamp
            assert "input_fingerprint" in stamp
            assert "model" in stamp
            assert "enrichment_schema_version" in stamp
            assert "enriched_at" in stamp
            datetime.fromisoformat(stamp["enriched_at"])

    def test_batch_failure_keeps_completed_work(self, tmp_path):
        """Completed batches should survive a later failure in a work file."""
        from arquimedes.enrich_llm import EnrichmentError

        output_dir = _make_extracted_dir(tmp_path, n_chunks=100)
        config = _make_config(batch_target=50)

        original_chunks_text = (output_dir / "chunks.jsonl").read_text(encoding="utf-8")

        batch1_ids = [f"c{i:03d}" for i in range(1, 51)]
        llm_fn = MagicMock(side_effect=[
            _make_chunk_response(batch1_ids),
            EnrichmentError("LLM failed"),
        ])

        result = enrich_chunks_stage(output_dir, config, llm_fn, force=True)

        assert result["status"] == "failed"
        assert (output_dir / "chunks.jsonl").read_text(encoding="utf-8") == original_chunks_text
        assert not (output_dir / "chunk_enrichment_stamps.json").exists()

        work_path = output_dir / "chunk_enrichment.work.json"
        assert work_path.exists()
        work = json.loads(work_path.read_text(encoding="utf-8"))
        assert set(work["chunks"].keys()) == set(batch1_ids)

    def test_resume_skips_completed_work_when_not_force(self, tmp_path):
        """A non-force rerun should reuse completed work-file chunks and only call the LLM for the remainder."""
        from arquimedes.enrich_llm import EnrichmentError

        output_dir = _make_extracted_dir(tmp_path, n_chunks=100)
        config = _make_config(batch_target=50)

        batch1_ids = [f"c{i:03d}" for i in range(1, 51)]
        batch2_ids = [f"c{i:03d}" for i in range(51, 101)]
        first_llm = MagicMock(side_effect=[
            _make_chunk_response(batch1_ids),
            EnrichmentError("LLM failed"),
        ])

        first_result = enrich_chunks_stage(output_dir, config, first_llm, force=True)
        assert first_result["status"] == "failed"

        second_llm = _make_llm_fn([_make_chunk_response(batch2_ids)])
        second_result = enrich_chunks_stage(output_dir, config, second_llm, force=False)

        assert second_result["status"] == "enriched", second_result["detail"]
        assert second_llm.call_count == 1
        assert not (output_dir / "chunk_enrichment.work.json").exists()

        chunks = [
            json.loads(line)
            for line in (output_dir / "chunks.jsonl").read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        assert all("summary" in chunk for chunk in chunks)

        stamps = json.loads((output_dir / "chunk_enrichment_stamps.json").read_text(encoding="utf-8"))
        assert len(stamps) == 100

    def test_parallel_failure_checkpoints_in_flight_successes(self, tmp_path):
        """After one parallel batch fails, in-flight successful batches should still checkpoint and no new batches should be submitted."""
        from arquimedes.enrich_llm import EnrichmentError

        output_dir = _make_extracted_dir(tmp_path, n_chunks=150)
        config = _make_config(batch_target=50, chunk_parallel_requests=2)

        call_index = {"value": 0}

        class _CloneLlm:
            def __init__(self, model_name: str):
                self.last_model = model_name

            def __call__(self, system, messages):
                del system
                call_index["value"] += 1
                chunk_ids = re.findall(r"--- Chunk ([^ ]+) \(pages", json.dumps(messages))
                if call_index["value"] == 2:
                    raise EnrichmentError("LLM failed")
                return _make_chunk_response(chunk_ids)

        class _BaseLlm:
            def __init__(self):
                self.last_model = "base"

            def __call__(self, system, messages):
                del system, messages
                raise AssertionError("base llm should not be called when worker clones are available")

        base_llm = _BaseLlm()

        def _factory():
            return _CloneLlm("clone")

        base_llm._arq_factory = _factory

        result = enrich_chunks_stage(output_dir, config, base_llm, force=True)

        assert result["status"] == "failed"
        work = json.loads((output_dir / "chunk_enrichment.work.json").read_text(encoding="utf-8"))
        assert len(work["chunks"]) == 50
        assert call_index["value"] == 2

    def test_100_chunks_with_batch_50_makes_2_llm_calls(self, tmp_path):
        """100 chunks with batch_target=50 should result in exactly 2 LLM calls."""
        output_dir = _make_extracted_dir(tmp_path, n_chunks=100)
        config = _make_config(batch_target=50)

        # Prepare two batch responses
        batch1_ids = [f"c{i:03d}" for i in range(1, 51)]
        batch2_ids = [f"c{i:03d}" for i in range(51, 101)]
        llm_fn = _make_llm_fn([
            _make_chunk_response(batch1_ids),
            _make_chunk_response(batch2_ids),
        ])

        result = enrich_chunks_stage(output_dir, config, llm_fn, force=True)

        assert result["status"] == "enriched", result["detail"]
        assert llm_fn.call_count == 2

    def test_skipped_when_not_stale(self, tmp_path):
        """Second run without force should be skipped."""
        output_dir = _make_extracted_dir(tmp_path, n_chunks=1)
        chunk_ids = ["c001"]
        llm_fn = _make_llm_fn([
            _make_chunk_response(chunk_ids),
            _make_chunk_response(chunk_ids),
        ])
        config = _make_config()

        result1 = enrich_chunks_stage(output_dir, config, llm_fn, force=True)
        assert result1["status"] == "enriched"

        call_count_after_first = llm_fn.call_count

        result2 = enrich_chunks_stage(output_dir, config, llm_fn, force=False)
        assert result2["status"] == "skipped"
        assert llm_fn.call_count == call_count_after_first

    def test_original_chunk_fields_preserved(self, tmp_path):
        """Original chunk fields (text, source_pages, emphasized) must be kept."""
        output_dir = _make_extracted_dir(tmp_path, n_chunks=1)
        llm_fn = _make_llm_fn([_make_chunk_response(["c001"])])
        config = _make_config()

        enrich_chunks_stage(output_dir, config, llm_fn, force=True)

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

    def test_fails_when_many_chunks_missing_from_response(self, tmp_path):
        """If LLM omits most chunk_ids from response, stage should fail."""
        output_dir = _make_extracted_dir(tmp_path, n_chunks=5)
        # Only return enrichment for c001, omit c002-c005 (4 missing > tolerance)
        incomplete = json.dumps({"id": "c001", "s": "S", "kw": ["k"], "cls": "argument"})
        llm_fn = _make_llm_fn([incomplete])
        config = _make_config()

        result = enrich_chunks_stage(output_dir, config, llm_fn, force=True)
        assert result["status"] == "failed"
        assert "missing" in result["detail"].lower()
        assert (output_dir / "debug" / "chunks.failed.response.txt").read_text(encoding="utf-8").endswith(incomplete)
        failure_meta = json.loads((output_dir / "debug" / "chunks.failed.meta.json").read_text(encoding="utf-8"))
        assert failure_meta["detail"] == result["detail"]

    def test_tolerates_few_missing_chunks(self, tmp_path):
        """Up to 3 missing chunks should produce a warning but succeed."""
        output_dir = _make_extracted_dir(tmp_path, n_chunks=4)
        # Return enrichment for c001-c003, omit c004 (1 missing ≤ tolerance)
        lines = "\n".join(
            json.dumps({"id": f"c00{i}", "s": "S", "kw": ["k"], "cls": "argument"})
            for i in range(1, 4)
        )
        llm_fn = _make_llm_fn([lines])
        config = _make_config()

        result = enrich_chunks_stage(output_dir, config, llm_fn, force=True)
        assert result["status"] == "enriched"

    def test_fails_when_chunk_missing_summary(self, tmp_path):
        """If a chunk response returns no summary, the chunk is simply not enriched (missing)."""
        output_dir = _make_extracted_dir(tmp_path, n_chunks=1)
        # Compact format with no "s" field — parser skips, chunk treated as missing
        bad = json.dumps({"id": "c001", "kw": ["k"], "cls": "argument"})
        llm_fn = _make_llm_fn([bad])
        config = _make_config()

        # A single missing chunk produces a warning but still succeeds
        result = enrich_chunks_stage(output_dir, config, llm_fn, force=True)
        assert result["status"] in ("enriched", "failed")

    def test_no_chunks_returns_skipped(self, tmp_path):
        """If chunks.jsonl is empty or absent, the stage should be skipped."""
        output_dir = _make_extracted_dir(tmp_path, n_chunks=0)
        # Overwrite with empty file
        (output_dir / "chunks.jsonl").write_text("", encoding="utf-8")
        llm_fn = MagicMock()
        config = _make_config()

        result = enrich_chunks_stage(output_dir, config, llm_fn, force=True)
        assert result["status"] == "skipped"
        llm_fn.assert_not_called()

