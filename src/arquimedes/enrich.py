"""Enrichment orchestrator — Phase 3.

Runs document, chunk, and figure enrichment stages for one or all materials.
"""

from __future__ import annotations

import json
from pathlib import Path

from arquimedes import enrich_stamps
from arquimedes.config import get_project_root, load_config
from arquimedes.enrich_document import enrich_document_stage
from arquimedes.enrich_chunks import enrich_chunks_stage
from arquimedes.enrich_figures import enrich_figures_stage
from arquimedes.ingest import load_manifest


# ---------------------------------------------------------------------------
# Staleness check helpers (for dry_run and "all stale" filtering)
# ---------------------------------------------------------------------------

_ALL_STAGES = ["document", "chunk", "figure"]


def _is_document_stale(output_dir: Path, config: dict) -> bool:
    """Quick staleness check for document stage without calling LLM."""
    enrichment_config = config.get("enrichment", {})
    model = config.get("llm", {}).get("model", "claude-sonnet-4-6")
    prompt_version = enrichment_config.get("prompt_version", "enrich-v1.0")
    schema_version = enrichment_config.get("enrichment_schema_version", "1")
    try:
        fingerprint = enrich_stamps.document_fingerprint(output_dir)
        stamp = enrich_stamps.make_stamp(prompt_version, model, schema_version, fingerprint)
        existing = enrich_stamps.read_document_stamp(output_dir)
        return enrich_stamps.is_stale(existing, stamp)
    except Exception:
        return True


def _is_chunk_stale(output_dir: Path, config: dict) -> bool:
    """Quick staleness check for chunk stage."""
    enrichment_config = config.get("enrichment", {})
    model = config.get("llm", {}).get("model", "claude-sonnet-4-6")
    prompt_version = enrichment_config.get("prompt_version", "enrich-v1.0")
    schema_version = enrichment_config.get("enrichment_schema_version", "1")

    chunks_path = output_dir / "chunks.jsonl"
    if not chunks_path.exists():
        return False

    try:
        meta_path = output_dir / "meta.json"
        meta = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.exists() else {}
        toc_path = output_dir / "toc.json"
        toc = json.loads(toc_path.read_text(encoding="utf-8")) if toc_path.exists() else None

        headings: list[str] = []
        if toc and isinstance(toc, list):
            headings = [e.get("title", "") for e in toc if e.get("title")]

        doc_context: dict = {
            "title": meta.get("title", ""),
            "raw_document_type": meta.get("raw_document_type", ""),
            "headings": headings,
        }
        if "summary" in meta:
            doc_context["summary"] = meta["summary"]
        existing_doc_stamp = enrich_stamps.read_document_stamp(output_dir)
        if existing_doc_stamp:
            doc_context["doc_stamp"] = existing_doc_stamp

        fingerprint = enrich_stamps.chunk_fingerprint(output_dir, doc_context)
        stamp = enrich_stamps.make_stamp(prompt_version, model, schema_version, fingerprint)
        existing_stamps = enrich_stamps.read_chunk_stamps(output_dir)
        existing_stage_stamp = existing_stamps.get("_stage")
        return enrich_stamps.is_stale(existing_stage_stamp, stamp)
    except Exception:
        return True


def _is_figure_stale(output_dir: Path, config: dict) -> bool:
    """Quick staleness check for figure stage — True if any figure is stale."""
    figures_dir = output_dir / "figures"
    if not figures_dir.exists():
        return False
    sidecar_paths = list(figures_dir.glob("*.json"))
    if not sidecar_paths:
        return False

    enrichment_config = config.get("enrichment", {})
    model = config.get("llm", {}).get("model", "claude-sonnet-4-6")
    prompt_version = enrichment_config.get("prompt_version", "enrich-v1.0")
    schema_version = enrichment_config.get("enrichment_schema_version", "1")

    try:
        meta_path = output_dir / "meta.json"
        meta = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.exists() else {}
        doc_context: dict = {
            "title": meta.get("title", ""),
            "domain": meta.get("domain", ""),
        }
        if "document_type" in meta and isinstance(meta["document_type"], dict):
            dt = meta["document_type"]
            if isinstance(dt.get("value"), str):
                doc_context["document_type"] = dt["value"]

        pages_path = output_dir / "pages.jsonl"
        pages_by_num: dict[int, str] = {}
        if pages_path.exists():
            for line in pages_path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line:
                    p = json.loads(line)
                    pages_by_num[p["page_number"]] = p.get("text", "")

        for sidecar_path in sidecar_paths:
            sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
            source_page = sidecar.get("source_page", 0)
            page_text = pages_by_num.get(source_page, "")
            from arquimedes.enrich_figures import _extract_caption_candidates
            caption_candidates = _extract_caption_candidates(page_text)

            image_path_str = sidecar.get("image_path", "")
            image_path = Path(image_path_str) if image_path_str else None
            if image_path and not image_path.is_absolute():
                image_path = output_dir / image_path

            try:
                if image_path and image_path.exists():
                    fingerprint = enrich_stamps.figure_fingerprint(
                        sidecar, image_path, page_text, caption_candidates, doc_context
                    )
                else:
                    from arquimedes.enrich_stamps import canonical_hash
                    raw_sidecar = {
                        "source_page": sidecar.get("source_page"),
                        "bbox": sidecar.get("bbox", []),
                        "extraction_method": sidecar.get("extraction_method", ""),
                    }
                    fingerprint = canonical_hash(
                        "no_image", raw_sidecar, page_text, caption_candidates, doc_context
                    )
            except Exception:
                return True

            stamp = enrich_stamps.make_stamp(prompt_version, model, schema_version, fingerprint)
            existing = enrich_stamps.read_figure_stamp(sidecar_path)
            if enrich_stamps.is_stale(existing, stamp):
                return True
    except Exception:
        return True

    return False


def _has_extraction(output_dir: Path) -> bool:
    """Return True if this material has been extracted (meta.json exists)."""
    return (output_dir / "meta.json").exists()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def enrich(
    material_id: str | None = None,
    config: dict | None = None,
    *,
    force: bool = False,
    stages: list[str] | None = None,
    dry_run: bool = False,
) -> tuple[dict, bool]:
    """Run LLM enrichment for one or all materials.

    Args:
        material_id: Specific material to enrich, or None for all with stale enrichment.
        config: Optional config dict. Loaded from disk if not provided.
        force: Re-enrich even if not stale.
        stages: List of stage names to run (default: all three).
        dry_run: Report staleness without calling LLM.

    Returns:
        (results_dict, all_succeeded) where results_dict maps material_id to
        {"title": ..., "document": {...}, "chunk": {...}, "figure": {...}}
    """
    if config is None:
        config = load_config()

    project_root = get_project_root()
    extracted_dir = project_root / "extracted"
    manifest = load_manifest(project_root)

    # Normalize stages
    requested_stages = list(stages) if stages else _ALL_STAGES
    for s in requested_stages:
        if s not in _ALL_STAGES:
            raise ValueError(f"Unknown stage: {s!r}. Valid stages: {_ALL_STAGES}")

    # Create client (not needed for dry_run)
    client = None
    if not dry_run:
        from arquimedes.enrich_llm import make_client
        client = make_client(config)

    # Determine which materials to process
    if material_id:
        if material_id not in manifest:
            raise ValueError(f"Material {material_id!r} not found in manifest")
        output_dir = extracted_dir / material_id
        if not _has_extraction(output_dir):
            raise ValueError(
                f"Material {material_id!r} has not been extracted yet. Run `arq extract-raw` first."
            )
        to_process = {material_id: manifest[material_id]}
    else:
        # Process all materials that have been extracted AND have at least one stale stage
        to_process = {}
        for mid, entry in manifest.items():
            output_dir = extracted_dir / mid
            if not _has_extraction(output_dir):
                continue
            # Check if any requested stage is stale
            any_stale = False
            if "document" in requested_stages and _is_document_stale(output_dir, config):
                any_stale = True
            elif "chunk" in requested_stages and _is_chunk_stale(output_dir, config):
                any_stale = True
            elif "figure" in requested_stages and _is_figure_stale(output_dir, config):
                any_stale = True
            if force or any_stale:
                to_process[mid] = entry

    results: dict[str, dict] = {}
    all_succeeded = True

    for mid, entry in to_process.items():
        output_dir = extracted_dir / mid

        # Load title for display
        try:
            meta_path = output_dir / "meta.json"
            meta = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.exists() else {}
            title = meta.get("title", mid)
        except Exception:
            title = mid

        material_results: dict = {"title": title}

        for stage_name in requested_stages:
            if dry_run:
                # Report staleness only
                if stage_name == "document":
                    stale = _is_document_stale(output_dir, config)
                elif stage_name == "chunk":
                    stale = _is_chunk_stale(output_dir, config)
                else:  # figure
                    stale = _is_figure_stale(output_dir, config)
                material_results[stage_name] = {
                    "status": "stale" if stale else "up_to_date",
                    "detail": "dry-run",
                }
                continue

            # Run the stage
            if stage_name == "document":
                result = enrich_document_stage(output_dir, config, client, force=force)
            elif stage_name == "chunk":
                result = enrich_chunks_stage(output_dir, config, client, force=force)
            else:  # figure
                result = enrich_figures_stage(output_dir, config, client, force=force)

            material_results[stage_name] = result
            if result["status"] == "failed":
                all_succeeded = False

        results[mid] = material_results

    return results, all_succeeded
