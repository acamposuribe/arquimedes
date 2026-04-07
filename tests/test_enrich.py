"""Tests for enrich orchestrator: coordinates document/chunk/figure stages."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from arquimedes.enrich import enrich


# ---------------------------------------------------------------------------
# Helpers — set up a minimal project layout in tmp_path
# ---------------------------------------------------------------------------


def _setup_project(tmp_path: Path, material_id: str = "test123") -> Path:
    """
    Set up a minimal project layout:
      tmp_path/
        manifests/materials.jsonl
        extracted/<material_id>/meta.json
        extracted/<material_id>/pages.jsonl
        extracted/<material_id>/chunks.jsonl
    Returns the tmp_path (project root).
    """
    manifests_dir = tmp_path / "manifests"
    manifests_dir.mkdir()
    manifest_entry = {
        "material_id": material_id,
        "file_hash": "deadbeef",
        "relative_path": f"Research/test/{material_id}.pdf",
        "file_type": "pdf",
        "domain": "research",
        "collection": "test",
        "ingested_at": "2024-01-01T00:00:00+00:00",
        "ingested_by": "",
    }
    (manifests_dir / "materials.jsonl").write_text(
        json.dumps(manifest_entry) + "\n", encoding="utf-8"
    )

    output_dir = tmp_path / "extracted" / material_id
    output_dir.mkdir(parents=True)

    meta = {
        "material_id": material_id,
        "title": "Test Doc",
        "authors": ["Author A"],
        "year": "2024",
        "raw_keywords": ["arch"],
        "raw_document_type": "paper",
        "domain": "research",
        "collection": "test",
        "page_count": 1,
    }
    (output_dir / "meta.json").write_text(json.dumps(meta), encoding="utf-8")

    pages = [
        {
            "page_number": 1,
            "text": "Architecture text.",
            "headings": ["Introduction"],
            "section_boundaries": [],
            "figure_refs": [],
            "table_refs": [],
            "thumbnail_path": "",
            "has_annotations": False,
            "annotation_ids": [],
        }
    ]
    with open(output_dir / "pages.jsonl", "w", encoding="utf-8") as f:
        for p in pages:
            f.write(json.dumps(p) + "\n")

    chunks = [{"chunk_id": "c001", "text": "Architecture text.", "source_pages": [1], "emphasized": False}]
    with open(output_dir / "chunks.jsonl", "w", encoding="utf-8") as f:
        for c in chunks:
            f.write(json.dumps(c) + "\n")

    return tmp_path


def _make_config() -> dict:
    return {
        "llm": {"agent_cmd": "test-agent --print"},
        "enrichment": {
            "prompt_version": "enrich-v1.0",
            "enrichment_schema_version": "1",
            "chunk_batch_target": 50,
            "figure_batch_size": 6,
            "max_retries": 3,
        },
    }


def _make_stage_result(status: str = "enriched") -> dict:
    return {"status": status, "detail": "ok"}


# ---------------------------------------------------------------------------
# Patch targets
# ---------------------------------------------------------------------------

_PATCH_DOC = "arquimedes.enrich.enrich_document_stage"
_PATCH_CHUNK = "arquimedes.enrich.enrich_chunks_stage"
_PATCH_FIGURE = "arquimedes.enrich.enrich_figures_stage"
# make_cli_llm_fn is imported inside the function body, so patch the source module
_PATCH_CLIENT = "arquimedes.enrich_llm.make_cli_llm_fn"
_PATCH_PROJECT_ROOT = "arquimedes.enrich.get_project_root"
_PATCH_LOAD_CONFIG = "arquimedes.enrich.load_config"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestEnrichOrchestrator:
    def test_force_passes_through_to_all_stages(self, tmp_path):
        """force=True should be passed to each stage function."""
        project_root = _setup_project(tmp_path)
        config = _make_config()
        mock_llm_fn = MagicMock()

        with (
            patch(_PATCH_PROJECT_ROOT, return_value=project_root),
            patch(_PATCH_CLIENT, return_value=mock_llm_fn),
            patch(_PATCH_DOC, return_value=_make_stage_result()) as mock_doc,
            patch(_PATCH_CHUNK, return_value=_make_stage_result()) as mock_chunk,
            patch(_PATCH_FIGURE, return_value=_make_stage_result()) as mock_figure,
        ):
            results, all_succeeded = enrich(
                material_id="test123",
                config=config,
                force=True,
            )

        assert all_succeeded is True
        # Each stage should have been called with force=True
        _, doc_kwargs = mock_doc.call_args
        assert doc_kwargs["force"] is True
        _, chunk_kwargs = mock_chunk.call_args
        assert chunk_kwargs["force"] is True
        _, figure_kwargs = mock_figure.call_args
        assert figure_kwargs["force"] is True

    def test_stage_document_only_runs_document_stage(self, tmp_path):
        """stages=['document'] should only call enrich_document_stage."""
        project_root = _setup_project(tmp_path)
        config = _make_config()
        mock_llm_fn = MagicMock()

        with (
            patch(_PATCH_PROJECT_ROOT, return_value=project_root),
            patch(_PATCH_CLIENT, return_value=mock_llm_fn),
            patch(_PATCH_DOC, return_value=_make_stage_result()) as mock_doc,
            patch(_PATCH_CHUNK, return_value=_make_stage_result()) as mock_chunk,
            patch(_PATCH_FIGURE, return_value=_make_stage_result()) as mock_figure,
        ):
            results, all_succeeded = enrich(
                material_id="test123",
                config=config,
                stages=["document"],
                force=True,
            )

        mock_doc.assert_called_once()
        mock_chunk.assert_not_called()
        mock_figure.assert_not_called()
        assert all_succeeded is True

    def test_dry_run_does_not_create_client(self, tmp_path):
        """dry_run=True should not call make_cli_llm_fn."""
        project_root = _setup_project(tmp_path)
        config = _make_config()

        with (
            patch(_PATCH_PROJECT_ROOT, return_value=project_root),
            patch(_PATCH_CLIENT) as mock_make_llm_fn,
            patch(_PATCH_DOC) as mock_doc,
            patch(_PATCH_CHUNK) as mock_chunk,
            patch(_PATCH_FIGURE) as mock_figure,
        ):
            results, all_succeeded = enrich(
                material_id="test123",
                config=config,
                dry_run=True,
                force=True,
            )

        mock_make_llm_fn.assert_not_called()
        # Stage functions should NOT have been called (dry_run path uses staleness checks only)
        mock_doc.assert_not_called()
        mock_chunk.assert_not_called()
        mock_figure.assert_not_called()

    def test_dry_run_reports_staleness(self, tmp_path):
        """dry_run=True should return staleness info for each stage."""
        project_root = _setup_project(tmp_path)
        config = _make_config()

        with (
            patch(_PATCH_PROJECT_ROOT, return_value=project_root),
            patch(_PATCH_CLIENT),
        ):
            results, all_succeeded = enrich(
                material_id="test123",
                config=config,
                dry_run=True,
                force=True,
            )

        assert "test123" in results

    def test_up_to_date_does_not_create_client(self, tmp_path):
        """If all requested stages are current, no agent CLI should be constructed."""
        project_root = _setup_project(tmp_path)
        config = _make_config()

        with (
            patch(_PATCH_PROJECT_ROOT, return_value=project_root),
            patch(_PATCH_CLIENT) as mock_make_llm_fn,
            patch("arquimedes.enrich._is_document_stale", return_value=False),
            patch("arquimedes.enrich._is_chunk_stale", return_value=False),
            patch("arquimedes.enrich._is_figure_stale", return_value=False),
        ):
            results, all_succeeded = enrich(
                material_id="test123",
                config=config,
                force=False,
            )

        mock_make_llm_fn.assert_not_called()
        assert all_succeeded is True
        assert results["test123"]["document"]["status"] == "skipped"
        assert results["test123"]["chunk"]["status"] == "skipped"
        assert results["test123"]["figure"]["status"] == "skipped"

    def test_all_stages_run_by_default(self, tmp_path):
        """With no stages filter, all three stage functions should be called."""
        project_root = _setup_project(tmp_path)
        config = _make_config()
        mock_llm_fn = MagicMock()

        with (
            patch(_PATCH_PROJECT_ROOT, return_value=project_root),
            patch(_PATCH_CLIENT, return_value=mock_llm_fn),
            patch(_PATCH_DOC, return_value=_make_stage_result()) as mock_doc,
            patch(_PATCH_CHUNK, return_value=_make_stage_result()) as mock_chunk,
            patch(_PATCH_FIGURE, return_value=_make_stage_result()) as mock_figure,
        ):
            results, all_succeeded = enrich(
                material_id="test123",
                config=config,
                force=True,
            )

        mock_doc.assert_called_once()
        mock_chunk.assert_called_once()
        mock_figure.assert_called_once()

    def test_failed_stage_sets_all_succeeded_false(self, tmp_path):
        """If any stage returns status='failed', all_succeeded should be False."""
        project_root = _setup_project(tmp_path)
        config = _make_config()
        mock_llm_fn = MagicMock()

        with (
            patch(_PATCH_PROJECT_ROOT, return_value=project_root),
            patch(_PATCH_CLIENT, return_value=mock_llm_fn),
            patch(_PATCH_DOC, return_value={"status": "failed", "detail": "LLM error"}),
            patch(_PATCH_CHUNK, return_value=_make_stage_result()),
            patch(_PATCH_FIGURE, return_value=_make_stage_result()),
        ):
            results, all_succeeded = enrich(
                material_id="test123",
                config=config,
                force=True,
            )

        assert all_succeeded is False

    def test_invalid_stage_raises_value_error(self, tmp_path):
        """Passing an unknown stage name should raise ValueError."""
        project_root = _setup_project(tmp_path)
        config = _make_config()

        with (
            patch(_PATCH_PROJECT_ROOT, return_value=project_root),
            patch(_PATCH_CLIENT),
        ):
            with pytest.raises(ValueError, match="Unknown stage"):
                enrich(
                    material_id="test123",
                    config=config,
                    stages=["invalid_stage"],
                    force=True,
                )

    def test_unknown_material_raises_value_error(self, tmp_path):
        """Requesting a material_id not in manifest should raise ValueError."""
        project_root = _setup_project(tmp_path)
        config = _make_config()

        with (
            patch(_PATCH_PROJECT_ROOT, return_value=project_root),
            patch(_PATCH_CLIENT),
        ):
            with pytest.raises(ValueError, match="not found in manifest"):
                enrich(
                    material_id="nonexistent_id",
                    config=config,
                    force=True,
                )

    def test_results_contain_title(self, tmp_path):
        """Result dict for each material should include the title."""
        project_root = _setup_project(tmp_path)
        config = _make_config()
        mock_llm_fn = MagicMock()

        with (
            patch(_PATCH_PROJECT_ROOT, return_value=project_root),
            patch(_PATCH_CLIENT, return_value=mock_llm_fn),
            patch(_PATCH_DOC, return_value=_make_stage_result()),
            patch(_PATCH_CHUNK, return_value=_make_stage_result()),
            patch(_PATCH_FIGURE, return_value=_make_stage_result()),
        ):
            results, _ = enrich(
                material_id="test123",
                config=config,
                force=True,
            )

        assert results["test123"]["title"] == "Test Doc"

    def test_stage_chunk_only(self, tmp_path):
        """stages=['chunk'] should only call enrich_chunks_stage."""
        project_root = _setup_project(tmp_path)
        config = _make_config()
        mock_llm_fn = MagicMock()

        with (
            patch(_PATCH_PROJECT_ROOT, return_value=project_root),
            patch(_PATCH_CLIENT, return_value=mock_llm_fn),
            patch(_PATCH_DOC, return_value=_make_stage_result()) as mock_doc,
            patch(_PATCH_CHUNK, return_value=_make_stage_result()) as mock_chunk,
            patch(_PATCH_FIGURE, return_value=_make_stage_result()) as mock_figure,
        ):
            results, all_succeeded = enrich(
                material_id="test123",
                config=config,
                stages=["chunk"],
                force=True,
            )

        mock_doc.assert_not_called()
        mock_chunk.assert_called_once()
        mock_figure.assert_not_called()


class TestParallelStages:
    """Tests for parallel document + figure execution."""

    def test_doc_and_figure_run_in_parallel_when_both_stale(self, tmp_path):
        """When combined mode is off and all stages are stale, doc+figure run concurrently."""
        import threading

        project_root = _setup_project(tmp_path)
        config = _make_config()
        mock_llm_fn = MagicMock()

        # Track which threads each stage runs on
        stage_threads: dict[str, int] = {}

        def _doc_stage(*args, **kwargs):
            stage_threads["document"] = threading.current_thread().ident
            return _make_stage_result()

        def _chunk_stage(*args, **kwargs):
            stage_threads["chunk"] = threading.current_thread().ident
            return _make_stage_result()

        def _figure_stage(*args, **kwargs):
            stage_threads["figure"] = threading.current_thread().ident
            return _make_stage_result()

        with (
            patch(_PATCH_PROJECT_ROOT, return_value=project_root),
            patch(_PATCH_CLIENT, return_value=mock_llm_fn),
            patch(_PATCH_DOC, side_effect=_doc_stage),
            patch(_PATCH_CHUNK, side_effect=_chunk_stage),
            patch(_PATCH_FIGURE, side_effect=_figure_stage),
        ):
            results, all_succeeded = enrich(
                material_id="test123",
                config=config,
                force=True,
            )

        assert all_succeeded is True
        # Figure should run on a different thread than document (parallel)
        assert stage_threads["figure"] != stage_threads["document"]
        # Chunk should run on the same thread as document (waits for doc)
        assert stage_threads["chunk"] == stage_threads["document"]

    def test_parallel_not_used_when_only_doc_stale(self, tmp_path):
        """When figure is up-to-date, no parallel scheduling — just sequential."""
        project_root = _setup_project(tmp_path)
        config = _make_config()
        mock_llm_fn = MagicMock()

        with (
            patch(_PATCH_PROJECT_ROOT, return_value=project_root),
            patch(_PATCH_CLIENT, return_value=mock_llm_fn),
            patch(_PATCH_DOC, return_value=_make_stage_result()) as mock_doc,
            patch(_PATCH_CHUNK, return_value=_make_stage_result()) as mock_chunk,
            patch(_PATCH_FIGURE, return_value=_make_stage_result()) as mock_figure,
        ):
            results, _ = enrich(
                material_id="test123",
                config=config,
                stages=["document", "chunk"],
                force=True,
            )

        mock_doc.assert_called_once()
        mock_chunk.assert_called_once()
        mock_figure.assert_not_called()

    def test_parallel_figure_failure_reported(self, tmp_path):
        """If figure fails in parallel path, all_succeeded should be False."""
        project_root = _setup_project(tmp_path)
        config = _make_config()
        mock_llm_fn = MagicMock()

        with (
            patch(_PATCH_PROJECT_ROOT, return_value=project_root),
            patch(_PATCH_CLIENT, return_value=mock_llm_fn),
            patch(_PATCH_DOC, return_value=_make_stage_result()),
            patch(_PATCH_CHUNK, return_value=_make_stage_result()),
            patch(_PATCH_FIGURE, return_value={"status": "failed", "detail": "vision error"}),
        ):
            results, all_succeeded = enrich(
                material_id="test123",
                config=config,
                force=True,
            )

        assert all_succeeded is False
        assert results["test123"]["document"]["status"] == "enriched"
        assert results["test123"]["figure"]["status"] == "failed"
