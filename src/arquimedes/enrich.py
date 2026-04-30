"""Enrichment orchestrator — Phase 3.

Runs document, metadata, chunk, and figure enrichment stages for one or all
materials.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from arquimedes import enrich_stamps
from arquimedes.llm import get_model_id
from arquimedes.config import get_logs_root, get_project_root, load_config
from arquimedes.domain_profiles import domain_prompt_version, normalize_domain
from arquimedes.enrich_document import enrich_document_stage
from arquimedes.enrich_chunks import enrich_chunks_stage
from arquimedes.enrich_figures import enrich_figures_stage
from arquimedes.enrich_metadata import enrich_metadata_stage
from arquimedes.ingest import load_manifest


def _progress(message: str) -> None:
    """Emit lightweight progress logs for long-running enrich operations."""
    print(message, file=sys.stderr, flush=True)


# ---------------------------------------------------------------------------
# Staleness check helpers (for dry_run and "all stale" filtering)
# ---------------------------------------------------------------------------

_ALL_STAGES = ["document", "metadata", "chunk", "figure"]


def _allows_figure_enrichment(entry) -> bool:
    """Figure enrichment is for document-like materials, not standalone images."""
    return getattr(entry, "file_type", "") != "image"


def _is_document_stale(output_dir: Path, config: dict) -> bool:
    """Quick staleness check for document stage without calling LLM."""
    enrichment_config = config.get("enrichment", {})
    prompt_version = enrichment_config.get("prompt_version", "enrich-v1.0")
    schema_version = enrichment_config.get("enrichment_schema_version", "1")
    try:
        meta_path = output_dir / "meta.json"
        meta = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.exists() else {}
        prompt_version = domain_prompt_version(prompt_version, str(meta.get("domain", "")))
        existing = enrich_stamps.read_document_stamp(output_dir)
        return not enrich_stamps.matches_stage_version(existing, prompt_version, schema_version)
    except Exception:
        return True


def _is_chunk_stale(output_dir: Path, config: dict) -> bool:
    """Quick staleness check for chunk stage."""
    stale_ids, _total = _chunk_staleness_info(output_dir, config)
    return bool(stale_ids)


def _is_metadata_stale(output_dir: Path, config: dict) -> bool:
    """Quick staleness check for thumbnail-based metadata correction."""
    enrichment_config = config.get("enrichment", {})
    model = get_model_id(config, "metadata")
    prompt_version = f"{enrichment_config.get('prompt_version', 'enrich-v1.0')}-metadata"
    schema_version = enrichment_config.get("enrichment_schema_version", "1")
    try:
        fingerprint = enrich_stamps.metadata_fingerprint(output_dir)
        stamp = enrich_stamps.make_stamp(prompt_version, model, schema_version, fingerprint)
        existing = enrich_stamps.read_metadata_fix_stamp(output_dir)
        return enrich_stamps.is_stale(existing, stamp)
    except Exception:
        return True


def _chunk_staleness_info(output_dir: Path, config: dict) -> tuple[set[str], int]:
    """Return (stale_chunk_ids, total_chunk_count) for a material."""
    enrichment_config = config.get("enrichment", {})
    model = get_model_id(config, "chunk")
    prompt_version = enrichment_config.get("prompt_version", "enrich-v1.0")
    schema_version = enrichment_config.get("enrichment_schema_version", "1")

    chunks_path = output_dir / "chunks.jsonl"
    if not chunks_path.exists():
        return set(), 0

    try:
        meta_path = output_dir / "meta.json"
        meta = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.exists() else {}
        prompt_version = domain_prompt_version(prompt_version, str(meta.get("domain", "")))
        toc_path = output_dir / "toc.json"
        toc = json.loads(toc_path.read_text(encoding="utf-8")) if toc_path.exists() else None

        headings: list[str] = []
        if toc and isinstance(toc, list):
            headings = [e.get("title", "") for e in toc if e.get("title")]
        if not headings:
            pages_path = output_dir / "pages.jsonl"
            if pages_path.exists():
                for line in pages_path.read_text(encoding="utf-8").splitlines():
                    line = line.strip()
                    if line:
                        p = json.loads(line)
                        headings.extend(p.get("headings", []))

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

        # Load chunks and annotations for per-chunk fingerprinting
        chunks = []
        for line in chunks_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line:
                chunks.append(json.loads(line))

        ann_path = output_dir / "annotations.jsonl"
        annotations = []
        if ann_path.exists():
            for line in ann_path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line:
                    annotations.append(json.loads(line))

        existing_stamps = enrich_stamps.read_chunk_stamps(output_dir)

        stale_ids: set[str] = set()
        for chunk in chunks:
            chunk_id = chunk.get("chunk_id", "")
            fp = enrich_stamps.single_chunk_fingerprint(chunk, annotations, doc_context)
            current_stamp = enrich_stamps.make_stamp(prompt_version, model, schema_version, fp)
            existing = existing_stamps.get(chunk_id)
            if enrich_stamps.is_stale(existing, current_stamp):
                stale_ids.add(chunk_id)
        return stale_ids, len(chunks)
    except Exception:
        return {"unknown"}, 0


def _is_figure_stale(output_dir: Path, config: dict) -> bool:
    """Quick staleness check for figure stage — True if any figure is stale."""
    figures_dir = output_dir / "figures"
    if not figures_dir.exists():
        return False
    sidecar_paths = list(figures_dir.glob("*.json"))
    if not sidecar_paths:
        return False

    enrichment_config = config.get("enrichment", {})
    model = get_model_id(config, "figure")
    prompt_version = enrichment_config.get("prompt_version", "enrich-v1.0")
    schema_version = enrichment_config.get("enrichment_schema_version", "1")

    try:
        meta_path = output_dir / "meta.json"
        meta = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.exists() else {}
        prompt_version = domain_prompt_version(prompt_version, str(meta.get("domain", "")))
        doc_context: dict = {
            "title": meta.get("title", ""),
            "authors": meta.get("authors", []),
            "year": meta.get("year", ""),
            "domain": meta.get("domain", ""),
            "collection": meta.get("collection", ""),
        }
        if "summary" in meta and isinstance(meta["summary"], dict):
            summary = meta["summary"].get("value")
            if isinstance(summary, str):
                doc_context["summary"] = summary

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


def _material_chunk_bytes(output_dir: Path) -> int:
    """Return chunks.jsonl size in bytes for deterministic smallest-first ordering."""
    chunks_path = output_dir / "chunks.jsonl"
    if not chunks_path.exists():
        return 0
    try:
        return chunks_path.stat().st_size
    except OSError:
        return 0




# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def enrich(
    material_id: str | None = None,
    config: dict | None = None,
    *,
    llm_fn=None,
    force: bool = False,
    stages: list[str] | None = None,
    dry_run: bool = False,
    domain: str | None = None,
) -> tuple[dict, bool]:
    """Run LLM enrichment for one or all materials.

    Args:
        material_id: Specific material to enrich, or None for all with stale enrichment.
        config: Optional config dict. Loaded from disk if not provided.
        llm_fn: Callable (system, messages) -> str. If not provided and not
            dry_run, falls back to make_cli_llm_fn(config).
        force: Re-enrich even if not stale.
        stages: List of stage names to run (default: all three).
        dry_run: Report staleness without calling LLM.
        domain: Optional domain filter (research, practice, or proyectos).

    Returns:
        (results_dict, all_succeeded) where results_dict maps material_id to
        {"title": ..., "document": {...}, "metadata": {...}, "chunk": {...}, "figure": {...}}
    """

    import datetime

    enrich_start_time = datetime.datetime.now()

    def _log_value(value) -> str:
        return str(value).replace("\t", " ").replace("\n", " ").strip()

    def _append_log(*fields) -> None:
        try:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            with open(log_path, "a", encoding="utf-8") as f:
                f.write("\t".join(_log_value(field) for field in fields) + "\n")
        except Exception:
            pass

    if config is None:
        config = load_config()

    domain_filter = normalize_domain(domain, default="") if domain else None
    if domain_filter and domain_filter not in {"research", "practice", "proyectos"}:
        raise ValueError("Unknown domain: {!r}. Valid domains: research, practice, proyectos".format(domain))

    project_root = get_project_root()
    try:
        if config.get("local_cache_root") or os.environ.get("ARQUIMEDES_LOCAL_CACHE"):
            log_path = get_logs_root(config) / "enrich.log"
        else:
            log_path = project_root / "logs" / "enrich.log"
    except FileNotFoundError:
        log_path = project_root / "logs" / "enrich.log"
    extracted_dir = project_root / "extracted"
    manifest = load_manifest(project_root)

    # Normalize stages
    requested_stages = list(stages) if stages else _ALL_STAGES
    explicit_stage_selection = stages is not None
    for s in requested_stages:
        if s not in _ALL_STAGES:
            raise ValueError(f"Unknown stage: {s!r}. Valid stages: {_ALL_STAGES}")

    _append_log(
        enrich_start_time.isoformat(),
        "START",
        material_id or "ALL",
        ",".join(requested_stages) if requested_stages else "ALL",
        force,
    )

    llm_state: dict = {}

    def _entry_domain(entry) -> str:
        entry_domain = getattr(entry, "domain", "")
        return normalize_domain(str(entry_domain), default="research")

    def _matches_domain_filter(entry) -> bool:
        return domain_filter is None or _entry_domain(entry) == domain_filter

    def _get_llm_fn(stage: str):
        if llm_fn is not None:
            return llm_fn
        from arquimedes.llm import make_cli_llm_fn
        return make_cli_llm_fn(config, stage, state=llm_state)

    # Determine which materials to process
    if material_id:
        if material_id not in manifest:
            raise ValueError(f"Material {material_id!r} not found in manifest")
        output_dir = extracted_dir / material_id
        if not _has_extraction(output_dir):
            raise ValueError(
                f"Material {material_id!r} has not been extracted yet. Run `arq extract-raw` first."
            )
        to_process = {material_id: manifest[material_id]} if _matches_domain_filter(manifest[material_id]) else {}
    else:
        # Process all materials that have been extracted AND have at least one stale stage
        to_process = {}
        for mid, entry in manifest.items():
            if not _matches_domain_filter(entry):
                continue
            output_dir = extracted_dir / mid
            if not _has_extraction(output_dir):
                continue
            metadata_needed = (
                not explicit_stage_selection
                and ("document" in requested_stages or "chunk" in requested_stages)
            )
            any_stale = (
                ("document" in requested_stages and _is_document_stale(output_dir, config))
                or ("chunk" in requested_stages and _is_chunk_stale(output_dir, config))
                or ("figure" in requested_stages and _allows_figure_enrichment(entry) and _is_figure_stale(output_dir, config))
                or ("metadata" in requested_stages and _is_metadata_stale(output_dir, config))
                or (metadata_needed and _is_metadata_stale(output_dir, config))
            )
            if force or any_stale:
                to_process[mid] = entry

    def _enrich_one_material(mid: str) -> tuple[str, dict, bool]:
        """Process a single material. Returns (mid, results_dict, succeeded)."""
        output_dir = extracted_dir / mid
        succeeded = True

        # Load title for display
        try:
            meta_path = output_dir / "meta.json"
            meta = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.exists() else {}
            title = meta.get("title", mid)
        except Exception:
            title = mid

        material_results: dict = {"title": title}
        figure_allowed = _allows_figure_enrichment(manifest[mid])

        doc_stale_now = force or (
            "document" in requested_stages and _is_document_stale(output_dir, config)
        )
        metadata_stale_now = force or (
            (
                "metadata" in requested_stages
                or (
                    not explicit_stage_selection
                    and ("document" in requested_stages or "chunk" in requested_stages)
                )
            )
            and _is_metadata_stale(output_dir, config)
        )
        chunk_stale_now = force or (
            "chunk" in requested_stages and _is_chunk_stale(output_dir, config)
        )

        # If the document is being re-enriched, chunk fingerprints become stale too
        # because chunk prompts depend on current document-level context.
        effective_chunk_stale = chunk_stale_now or (
            "chunk" in requested_stages and doc_stale_now
        )
        if not explicit_stage_selection and metadata_stale_now and "chunk" in requested_stages:
            effective_chunk_stale = True

        if not dry_run:
            _progress(f"[enrich] start {mid}  {title}")

        remaining = requested_stages

        if dry_run:
            for stage_name in remaining:
                if stage_name == "document":
                    stale = _is_document_stale(output_dir, config)
                elif stage_name == "metadata":
                    stale = _is_metadata_stale(output_dir, config)
                elif stage_name == "chunk":
                    stale = _is_chunk_stale(output_dir, config)
                elif stage_name == "figure" and not figure_allowed:
                    stale = False
                else:
                    stale = _is_figure_stale(output_dir, config)
                material_results[stage_name] = {
                    "status": "stale" if stale else "up_to_date",
                    "detail": "dry-run",
                }
        else:
            # Pre-check staleness per stage
            stale_stages: set[str] = set()
            for s in remaining:
                if s == "figure" and not figure_allowed:
                    continue
                if force:
                    stale_stages.add(s)
                elif s == "document" and _is_document_stale(output_dir, config):
                    stale_stages.add(s)
                elif s == "metadata" and _is_metadata_stale(output_dir, config):
                    stale_stages.add(s)
                elif s == "chunk" and effective_chunk_stale:
                    stale_stages.add(s)
                elif s == "figure" and _is_figure_stale(output_dir, config):
                    stale_stages.add(s)

            if (
                "document" in stale_stages
                and "chunk" in remaining
                and (output_dir / "chunks.jsonl").exists()
            ):
                stale_stages.add("chunk")

            metadata_needed = (not explicit_stage_selection) and metadata_stale_now and (
                "document" in remaining or "metadata" in remaining or "chunk" in remaining
            )
            metadata_done = False

            def _run_metadata_if_needed() -> bool:
                nonlocal metadata_done, succeeded
                if not metadata_needed or metadata_done:
                    return True
                _progress(f"[metadata] start {mid}")
                metadata_result = enrich_metadata_stage(
                    output_dir, config, _get_llm_fn("metadata"), force=force
                )
                material_results["metadata"] = metadata_result
                _progress(f"[metadata] done {mid}: {metadata_result.get('status', '?')}")
                metadata_done = True
                if metadata_result["status"] == "failed":
                    succeeded = False
                    return False
                return True

            if stale_stages:
                _progress(
                    f"[enrich] stages {mid}: {', '.join(s for s in ('document', 'metadata', 'chunk', 'figure') if s in stale_stages)}"
                )

            for s in remaining:
                if s not in stale_stages:
                    detail = "standalone image material" if s == "figure" and not figure_allowed else "up to date"
                    material_results[s] = {"status": "skipped", "detail": detail}

            # Sequential stage order is intentional: figure uses the current
            # document summary, and chunk must see post-metadata document fields.
            for s in ("document", "metadata", "chunk", "figure"):
                if s == "chunk" and not _run_metadata_if_needed():
                    if s in stale_stages:
                        material_results["chunk"] = {
                            "status": "failed",
                            "detail": "metadata prerequisite failed",
                        }
                    continue
                if s not in stale_stages:
                    continue
                _progress(f"[{s}] start {mid}")
                if s == "document":
                    result = enrich_document_stage(
                        output_dir, config, _get_llm_fn("document"), force=force
                    )
                    if result["status"] != "failed" and metadata_needed and "chunk" not in remaining:
                        _run_metadata_if_needed()
                elif s == "metadata":
                    result = enrich_metadata_stage(
                        output_dir, config, _get_llm_fn("metadata"), force=force
                    )
                    metadata_done = True
                elif s == "chunk":
                    result = enrich_chunks_stage(
                        output_dir, config, _get_llm_fn("chunk"), force=force
                    )
                else:
                    result = enrich_figures_stage(
                        output_dir, config, _get_llm_fn("figure"), force=force
                    )
                material_results[s] = result
                _progress(f"[{s}] done {mid}: {result.get('status', '?')}")
                if result["status"] == "failed":
                    succeeded = False

            if metadata_needed and not metadata_done and succeeded:
                _run_metadata_if_needed()

        if not dry_run:
            _progress(f"[enrich] done {mid}  {title}")

        return mid, material_results, succeeded

    # -----------------------------------------------------------------------
    # Execute — parallel when multiple materials, sequential for single
    # -----------------------------------------------------------------------

    results: dict[str, dict] = {}
    all_succeeded = True
    material_ids = sorted(
        to_process.keys(),
        key=lambda mid: (_material_chunk_bytes(extracted_dir / mid), mid),
    )

    try:
        for mid in material_ids:
            mid, mat_result, ok = _enrich_one_material(mid)
            results[mid] = mat_result
            if not ok:
                all_succeeded = False

        failed_stages: list[str] = []
        if not all_succeeded:
            for mid, material_result in results.items():
                for stage_name in requested_stages:
                    stage_result = material_result.get(stage_name)
                    if isinstance(stage_result, dict) and stage_result.get("status") == "failed":
                        detail = str(stage_result.get("detail", "failed")).strip()
                        failed_stages.append(f"{mid}:{stage_name}:{detail}")

        enrich_end_time = datetime.datetime.now()
        _append_log(
            enrich_start_time.isoformat(),
            enrich_end_time.isoformat(),
            material_id or "ALL",
            ",".join(requested_stages) if requested_stages else "ALL",
            force,
            "DONE" if all_succeeded else "FAILED",
            "ok" if all_succeeded else (" | ".join(failed_stages) if failed_stages else "one_or_more_materials_failed"),
        )
        return results, all_succeeded
    except Exception as exc:
        enrich_end_time = datetime.datetime.now()
        _append_log(
            enrich_start_time.isoformat(),
            enrich_end_time.isoformat(),
            material_id or "ALL",
            ",".join(requested_stages) if requested_stages else "ALL",
            force,
            "FAILED",
            exc,
        )
        raise
