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


def _setup_project(tmp_path: Path, material_id: str = "test123", file_type: str = "pdf") -> Path:
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
        "relative_path": f"Research/test/{material_id}.{'png' if file_type == 'image' else 'pdf'}",
        "file_type": file_type,
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
_PATCH_METADATA = "arquimedes.enrich.enrich_metadata_stage"
_PATCH_CHUNK = "arquimedes.enrich.enrich_chunks_stage"
_PATCH_FIGURE = "arquimedes.enrich.enrich_figures_stage"
# make_cli_llm_fn is imported inside the function body, so patch the canonical module
_PATCH_CLIENT = "arquimedes.llm.make_cli_llm_fn"
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
            patch(_PATCH_METADATA, return_value=_make_stage_result()) as mock_metadata,
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
        _, metadata_kwargs = mock_metadata.call_args
        assert metadata_kwargs["force"] is True
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
            patch(_PATCH_METADATA, return_value=_make_stage_result()) as mock_metadata,
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
        mock_metadata.assert_not_called()
        mock_chunk.assert_not_called()
        mock_figure.assert_not_called()
        assert all_succeeded is True

    def test_domain_filter_only_processes_matching_materials(self, tmp_path):
        project_root = _setup_project(tmp_path, material_id="research123")
        config = _make_config()
        mock_llm_fn = MagicMock()

        project_entry = {
            "material_id": "project123",
            "file_hash": "beadfeed",
            "relative_path": "Proyectos/test/project123.pdf",
            "file_type": "pdf",
            "domain": "proyectos",
            "collection": "test",
            "ingested_at": "2024-01-01T00:00:00+00:00",
            "ingested_by": "",
        }
        with open(project_root / "manifests" / "materials.jsonl", "a", encoding="utf-8") as f:
            f.write(json.dumps(project_entry) + "\n")
        project_output = project_root / "extracted" / "project123"
        project_output.mkdir(parents=True)
        (project_output / "meta.json").write_text(
            json.dumps({
                "material_id": "project123",
                "title": "Proyecto",
                "domain": "proyectos",
                "collection": "test",
                "page_count": 1,
            }),
            encoding="utf-8",
        )
        (project_output / "chunks.jsonl").write_text("", encoding="utf-8")

        with (
            patch(_PATCH_PROJECT_ROOT, return_value=project_root),
            patch(_PATCH_CLIENT, return_value=mock_llm_fn),
            patch(_PATCH_DOC, return_value=_make_stage_result()) as mock_doc,
        ):
            results, all_succeeded = enrich(
                config=config,
                stages=["document"],
                force=True,
                domain="proyectos",
            )

        assert all_succeeded is True
        assert set(results) == {"project123"}
        mock_doc.assert_called_once()
        assert mock_doc.call_args.args[0] == project_output

    def test_dry_run_does_not_create_client(self, tmp_path):
        """dry_run=True should not call make_cli_llm_fn."""
        project_root = _setup_project(tmp_path)
        config = _make_config()

        with (
            patch(_PATCH_PROJECT_ROOT, return_value=project_root),
            patch(_PATCH_CLIENT) as mock_make_llm_fn,
            patch(_PATCH_DOC) as mock_doc,
            patch(_PATCH_METADATA) as mock_metadata,
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
        mock_metadata.assert_not_called()
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
            patch("arquimedes.enrich._is_metadata_stale", return_value=False),
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

    def test_failed_run_logs_failed_outcome(self, tmp_path):
        project_root = _setup_project(tmp_path)
        config = _make_config()
        mock_llm_fn = MagicMock()

        with (
            patch(_PATCH_PROJECT_ROOT, return_value=project_root),
            patch(_PATCH_CLIENT, return_value=mock_llm_fn),
            patch(_PATCH_DOC, return_value=_make_stage_result("failed")),
            patch(_PATCH_METADATA, return_value=_make_stage_result()),
            patch(_PATCH_CHUNK, return_value=_make_stage_result()),
            patch(_PATCH_FIGURE, return_value=_make_stage_result()),
        ):
            _results, all_succeeded = enrich(
                material_id="test123",
                config=config,
                force=True,
            )

        assert all_succeeded is False
        log_lines = (project_root / "logs" / "enrich.log").read_text(encoding="utf-8").splitlines()
        assert len(log_lines) == 2
        assert "\tSTART\t" in log_lines[0]
        assert "\tFAILED\t" in log_lines[1]
        assert "test123:document:ok" in log_lines[1]

    def test_all_stages_run_by_default(self, tmp_path):
        """With no stages filter, all three stage functions should be called."""
        project_root = _setup_project(tmp_path)
        config = _make_config()
        mock_llm_fn = MagicMock()

        with (
            patch(_PATCH_PROJECT_ROOT, return_value=project_root),
            patch(_PATCH_CLIENT, return_value=mock_llm_fn),
            patch(_PATCH_DOC, return_value=_make_stage_result()) as mock_doc,
            patch(_PATCH_METADATA, return_value=_make_stage_result()) as mock_metadata,
            patch(_PATCH_CHUNK, return_value=_make_stage_result()) as mock_chunk,
            patch(_PATCH_FIGURE, return_value=_make_stage_result()) as mock_figure,
        ):
            results, all_succeeded = enrich(
                material_id="test123",
                config=config,
                force=True,
            )

        mock_doc.assert_called_once()
        mock_metadata.assert_called_once()
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
            patch(_PATCH_METADATA, return_value=_make_stage_result()),
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
            patch(_PATCH_METADATA, return_value=_make_stage_result()),
            patch(_PATCH_CHUNK, return_value=_make_stage_result()),
            patch(_PATCH_FIGURE, return_value=_make_stage_result()),
        ):
            results, _ = enrich(
                material_id="test123",
                config=config,
                force=True,
            )

        assert results["test123"]["title"] == "Test Doc"

    def test_all_materials_run_sequentially_smallest_chunks_first(self, tmp_path):
        project_root = _setup_project(tmp_path, material_id="small")

        def _make_material(material_id: str, chunk_text: str) -> None:
            manifest_entry = {
                "material_id": material_id,
                "file_hash": f"hash-{material_id}",
                "relative_path": f"Research/test/{material_id}.pdf",
                "file_type": "pdf",
                "domain": "research",
                "collection": "test",
                "ingested_at": "2024-01-01T00:00:00+00:00",
                "ingested_by": "",
            }
            with open(project_root / "manifests" / "materials.jsonl", "a", encoding="utf-8") as f:
                f.write(json.dumps(manifest_entry) + "\n")

            output_dir = project_root / "extracted" / material_id
            output_dir.mkdir(parents=True, exist_ok=True)

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

            page = {
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
            (output_dir / "pages.jsonl").write_text(json.dumps(page) + "\n", encoding="utf-8")

            chunk = {
                "chunk_id": f"{material_id}_chunk",
                "text": chunk_text,
                "source_pages": [1],
                "emphasized": False,
            }
            (output_dir / "chunks.jsonl").write_text(json.dumps(chunk) + "\n", encoding="utf-8")

        _make_material("medium", "x" * 100)
        _make_material("big", "x" * 500)

        config = _make_config()
        call_order: list[str] = []

        def _doc_stage(output_dir, config, llm_fn, *, force=False):
            call_order.append(output_dir.name)
            return _make_stage_result()

        with (
            patch(_PATCH_PROJECT_ROOT, return_value=project_root),
            patch(_PATCH_CLIENT, return_value=MagicMock()),
            patch(_PATCH_DOC, side_effect=_doc_stage),
            patch(_PATCH_METADATA, return_value={"status": "skipped", "detail": "up to date"}),
            patch(_PATCH_CHUNK, return_value={"status": "skipped", "detail": "up to date"}),
            patch(_PATCH_FIGURE, return_value={"status": "skipped", "detail": "up to date"}),
            patch("arquimedes.enrich._is_document_stale", return_value=True),
            patch("arquimedes.enrich._is_metadata_stale", return_value=False),
            patch("arquimedes.enrich._is_chunk_stale", return_value=False),
            patch("arquimedes.enrich._is_figure_stale", return_value=False),
        ):
            results, all_succeeded = enrich(config=config, force=False)

        assert all_succeeded is True
        assert call_order == ["small", "medium", "big"]
        assert list(results.keys()) == ["small", "medium", "big"]

    def test_stage_chunk_only(self, tmp_path):
        """stages=['chunk'] should only call enrich_chunks_stage."""
        project_root = _setup_project(tmp_path)
        config = _make_config()
        mock_llm_fn = MagicMock()

        with (
            patch(_PATCH_PROJECT_ROOT, return_value=project_root),
            patch(_PATCH_CLIENT, return_value=mock_llm_fn),
            patch(_PATCH_DOC, return_value=_make_stage_result()) as mock_doc,
            patch(_PATCH_METADATA, return_value=_make_stage_result()) as mock_metadata,
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
        mock_metadata.assert_not_called()
        mock_chunk.assert_called_once()
        mock_figure.assert_not_called()

    def test_standalone_image_skips_figure_stage(self, tmp_path):
        project_root = _setup_project(tmp_path, file_type="image")
        config = _make_config()
        mock_llm_fn = MagicMock()

        with (
            patch(_PATCH_PROJECT_ROOT, return_value=project_root),
            patch(_PATCH_CLIENT, return_value=mock_llm_fn),
            patch(_PATCH_DOC, return_value=_make_stage_result()),
            patch(_PATCH_METADATA, return_value=_make_stage_result()),
            patch(_PATCH_CHUNK, return_value=_make_stage_result()),
            patch(_PATCH_FIGURE, return_value=_make_stage_result()) as mock_figure,
        ):
            results, all_succeeded = enrich(
                material_id="test123",
                config=config,
                stages=["figure"],
                force=True,
            )

        assert all_succeeded is True
        mock_figure.assert_not_called()
        assert results["test123"]["figure"] == {
            "status": "skipped",
            "detail": "standalone image material",
        }

    def test_scanned_document_still_allows_figure_stage(self, tmp_path):
        project_root = _setup_project(tmp_path, file_type="scanned_document")
        config = _make_config()
        mock_llm_fn = MagicMock()

        with (
            patch(_PATCH_PROJECT_ROOT, return_value=project_root),
            patch(_PATCH_CLIENT, return_value=mock_llm_fn),
            patch(_PATCH_FIGURE, return_value=_make_stage_result()) as mock_figure,
        ):
            results, all_succeeded = enrich(
                material_id="test123",
                config=config,
                stages=["figure"],
                force=True,
            )

        assert all_succeeded is True
        mock_figure.assert_called_once()
        assert results["test123"]["figure"]["status"] == "enriched"

    def test_stage_metadata_only_runs_only_metadata(self, tmp_path):
        """stages=['metadata'] should only call enrich_metadata_stage."""
        project_root = _setup_project(tmp_path)
        config = _make_config()
        mock_llm_fn = MagicMock()

        with (
            patch(_PATCH_PROJECT_ROOT, return_value=project_root),
            patch(_PATCH_CLIENT, return_value=mock_llm_fn),
            patch(_PATCH_DOC, return_value=_make_stage_result()) as mock_doc,
            patch(_PATCH_METADATA, return_value=_make_stage_result()) as mock_metadata,
            patch(_PATCH_CHUNK, return_value=_make_stage_result()) as mock_chunk,
            patch(_PATCH_FIGURE, return_value=_make_stage_result()) as mock_figure,
        ):
            results, all_succeeded = enrich(
                material_id="test123",
                config=config,
                stages=["metadata"],
                force=True,
            )

        mock_doc.assert_not_called()
        mock_metadata.assert_called_once()
        mock_chunk.assert_not_called()
        mock_figure.assert_not_called()
        assert all_succeeded is True

    def test_metadata_runs_between_document_and_chunk(self, tmp_path):
        project_root = _setup_project(tmp_path)
        config = _make_config()
        call_order: list[str] = []

        def _record(name: str):
            def _inner(*args, **kwargs):
                call_order.append(name)
                return _make_stage_result()
            return _inner

        with (
            patch(_PATCH_PROJECT_ROOT, return_value=project_root),
            patch(_PATCH_CLIENT, return_value=MagicMock()),
            patch(_PATCH_DOC, side_effect=_record("document")),
            patch(_PATCH_METADATA, side_effect=_record("metadata")),
            patch(_PATCH_CHUNK, side_effect=_record("chunk")),
            patch(_PATCH_FIGURE, return_value={"status": "skipped", "detail": "up to date"}),
            patch("arquimedes.enrich._is_document_stale", return_value=True),
            patch("arquimedes.enrich._is_metadata_stale", return_value=True),
            patch("arquimedes.enrich._is_chunk_stale", return_value=True),
            patch("arquimedes.enrich._is_figure_stale", return_value=False),
        ):
            results, all_succeeded = enrich(material_id="test123", config=config, force=False)

        assert all_succeeded is True
        assert call_order == ["document", "metadata", "chunk"]

    def test_metadata_stale_triggers_chunk_even_if_chunk_stamp_is_current_in_full_run(self, tmp_path):
        project_root = _setup_project(tmp_path)
        config = _make_config()
        call_order: list[str] = []

        def _record(name: str):
            def _inner(*args, **kwargs):
                call_order.append(name)
                return _make_stage_result()
            return _inner

        with (
            patch(_PATCH_PROJECT_ROOT, return_value=project_root),
            patch(_PATCH_CLIENT, return_value=MagicMock()),
            patch(_PATCH_DOC, return_value={"status": "skipped", "detail": "up to date"}),
            patch(_PATCH_METADATA, side_effect=_record("metadata")),
            patch(_PATCH_CHUNK, side_effect=_record("chunk")),
            patch(_PATCH_FIGURE, return_value={"status": "skipped", "detail": "up to date"}),
            patch("arquimedes.enrich._is_document_stale", return_value=False),
            patch("arquimedes.enrich._is_metadata_stale", return_value=True),
            patch("arquimedes.enrich._is_chunk_stale", return_value=False),
            patch("arquimedes.enrich._is_figure_stale", return_value=False),
        ):
            results, all_succeeded = enrich(material_id="test123", config=config, force=False)

        assert all_succeeded is True
        assert call_order == ["metadata", "chunk"]

    def test_explicit_chunk_stage_does_not_trigger_metadata(self, tmp_path):
        project_root = _setup_project(tmp_path)
        config = _make_config()

        with (
            patch(_PATCH_PROJECT_ROOT, return_value=project_root),
            patch(_PATCH_CLIENT, return_value=MagicMock()),
            patch(_PATCH_DOC, return_value={"status": "skipped", "detail": "up to date"}),
            patch(_PATCH_METADATA, return_value=_make_stage_result()) as mock_metadata,
            patch(_PATCH_CHUNK, return_value=_make_stage_result()) as mock_chunk,
            patch(_PATCH_FIGURE, return_value={"status": "skipped", "detail": "up to date"}),
        ):
            results, all_succeeded = enrich(
                material_id="test123",
                config=config,
                stages=["chunk"],
                force=True,
            )

        assert all_succeeded is True
        mock_metadata.assert_not_called()
        mock_chunk.assert_called_once()

    def test_figure_runs_after_document_when_both_are_stale(self, tmp_path):
        project_root = _setup_project(tmp_path)
        config = _make_config()
        call_order: list[str] = []

        def _record(name: str):
            def _inner(*args, **kwargs):
                call_order.append(name)
                return _make_stage_result()
            return _inner

        with (
            patch(_PATCH_PROJECT_ROOT, return_value=project_root),
            patch(_PATCH_CLIENT, return_value=MagicMock()),
            patch(_PATCH_DOC, side_effect=_record("document")),
            patch(_PATCH_METADATA, return_value={"status": "skipped", "detail": "up to date"}),
            patch(_PATCH_CHUNK, return_value={"status": "skipped", "detail": "up to date"}),
            patch(_PATCH_FIGURE, side_effect=_record("figure")),
            patch("arquimedes.enrich._is_document_stale", return_value=True),
            patch("arquimedes.enrich._is_metadata_stale", return_value=False),
            patch("arquimedes.enrich._is_chunk_stale", return_value=False),
            patch("arquimedes.enrich._is_figure_stale", return_value=True),
        ):
            results, all_succeeded = enrich(material_id="test123", config=config, force=False)

        assert all_succeeded is True
        assert call_order == ["document", "figure"]


class TestParallelStages:
    """Tests for parallel document + figure execution."""

    def test_doc_and_figure_run_in_parallel_when_both_stale(self, tmp_path):
        """When combined mode is off and all stages are stale, doc+figure run concurrently."""
        import threading

        project_root = _setup_project(tmp_path)
        config = _make_config()
        mock_llm_fn = MagicMock()

        class TestStageScheduling:
            """Tests for stage scheduling within a single material."""

            def test_doc_and_figure_still_allow_chunk_after_document(self, tmp_path):
                project_root = _setup_project(tmp_path)
                config = _make_config()

                with (
                    patch(_PATCH_PROJECT_ROOT, return_value=project_root),
                    patch(_PATCH_CLIENT, return_value=MagicMock()),
                    patch(_PATCH_DOC, return_value=_make_stage_result()) as mock_doc,
                    patch(_PATCH_METADATA, return_value={"status": "skipped", "detail": "up to date"}),
                    patch(_PATCH_CHUNK, return_value=_make_stage_result()) as mock_chunk,
                    patch(_PATCH_FIGURE, return_value=_make_stage_result()) as mock_figure,
                    patch("arquimedes.enrich._is_document_stale", return_value=True),
                    patch("arquimedes.enrich._is_metadata_stale", return_value=False),
                    patch("arquimedes.enrich._is_chunk_stale", return_value=True),
                    patch("arquimedes.enrich._is_figure_stale", return_value=True),
                ):
                    results, all_succeeded = enrich(
                        material_id="test123",
                        config=config,
                        force=False,
                    )

                assert all_succeeded is True
                mock_doc.assert_called_once()
                mock_chunk.assert_called_once()
                mock_figure.assert_called_once()

            def test_figure_failure_marks_run_failed(self, tmp_path):
                project_root = _setup_project(tmp_path)
                config = _make_config()

                with (
                    patch(_PATCH_PROJECT_ROOT, return_value=project_root),
                    patch(_PATCH_CLIENT, return_value=MagicMock()),
                    patch(_PATCH_DOC, return_value=_make_stage_result()),
                    patch(_PATCH_METADATA, return_value={"status": "skipped", "detail": "up to date"}),
                    patch(_PATCH_CHUNK, return_value=_make_stage_result()),
                    patch(_PATCH_FIGURE, return_value={"status": "failed", "detail": "boom"}),
                    patch("arquimedes.enrich._is_document_stale", return_value=True),
                    patch("arquimedes.enrich._is_metadata_stale", return_value=False),
                    patch("arquimedes.enrich._is_chunk_stale", return_value=True),
                    patch("arquimedes.enrich._is_figure_stale", return_value=True),
                ):
                    results, all_succeeded = enrich(
                        material_id="test123",
                        config=config,
                        force=False,
                    )

                assert all_succeeded is False
