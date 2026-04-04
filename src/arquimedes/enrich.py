"""Enrichment orchestrator — Phase 3.

Runs document, chunk, and figure enrichment stages for one or all materials.
"""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from arquimedes import enrich_stamps
from arquimedes.enrich_llm import get_model_id
from arquimedes.config import get_project_root, load_config
from arquimedes.enrich_document import enrich_document_stage
from arquimedes.enrich_chunks import enrich_chunks_stage
from arquimedes.enrich_figures import enrich_figures_stage
from arquimedes.ingest import load_manifest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_jsonl(path: Path) -> list[dict]:
    """Load a .jsonl file into a list of dicts. Returns [] if absent."""
    if not path.exists():
        return []
    records = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            records.append(json.loads(line))
    return records


def _load_json(path: Path, default=None):
    """Load a JSON file. Returns default if absent."""
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# Staleness check helpers (for dry_run and "all stale" filtering)
# ---------------------------------------------------------------------------

_ALL_STAGES = ["document", "chunk", "figure"]


def _is_document_stale(output_dir: Path, config: dict) -> bool:
    """Quick staleness check for document stage without calling LLM."""
    enrichment_config = config.get("enrichment", {})
    model = get_model_id(config)
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
    model = get_model_id(config)
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

        for chunk in chunks:
            chunk_id = chunk.get("chunk_id", "")
            fp = enrich_stamps.single_chunk_fingerprint(chunk, annotations, doc_context)
            current_stamp = enrich_stamps.make_stamp(prompt_version, model, schema_version, fp)
            existing = existing_stamps.get(chunk_id)
            if enrich_stamps.is_stale(existing, current_stamp):
                return True
        return False
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
    model = get_model_id(config)
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
# Combined doc+chunk call
# ---------------------------------------------------------------------------

# ~4 chars per token, threshold for fitting doc+chunk in one call
_COMBINED_TOKEN_THRESHOLD = 80_000

_COMBINED_SCHEMA_DESC = """\
{
  "document": {
    "summary": {"value": "...", ...},
    "document_type": {"value": "...", ...},
    "keywords": {"value": [...], ...},
    "facets": {...},
    "concepts": [...]
  },
  "chunks": [
    {"chunk_id": "...", "summary": {"value": "...", ...}, "keywords": {"value": [...], ...}}
  ]
}"""


def _should_combine(output_dir: Path) -> bool:
    """Return True if this material is small enough for a combined doc+chunk call."""
    chunks_path = output_dir / "chunks.jsonl"
    if not chunks_path.exists():
        return False
    total_chars = 0
    for line in chunks_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            c = json.loads(line)
            total_chars += len(c.get("text", ""))
    return total_chars // 4 < _COMBINED_TOKEN_THRESHOLD


def _run_combined_enrichment(
    output_dir: Path,
    config: dict,
    llm_fn,
    *,
    force: bool = False,
) -> tuple[dict, dict]:
    """Run doc+chunk enrichment in a single LLM call.

    Returns (doc_result, chunk_result). Each stage is committed independently
    per the spec's combined-call failure semantics.
    """
    from arquimedes import enrich_llm, enrich_prompts

    enrichment_config = config.get("enrichment", {})
    model = get_model_id(config)

    # Load artifacts
    meta = _load_json(output_dir / "meta.json", {})
    toc = _load_json(output_dir / "toc.json")
    chunks = _load_jsonl(output_dir / "chunks.jsonl")
    annotations = _load_jsonl(output_dir / "annotations.jsonl")

    # Build combined prompt and make one LLM call
    system, messages = enrich_prompts.build_combined_prompt(meta, toc, chunks, annotations)
    try:
        raw_text = llm_fn(system, messages)
        parsed = enrich_llm.parse_json_or_repair(
            llm_fn, raw_text, _COMBINED_SCHEMA_DESC
        )
    except (enrich_llm.EnrichmentError, Exception):
        # Full parse failed — fall back to separate calls
        return None, None

    # Extract and validate portions independently
    doc_data = parsed.get("document")
    chunks_data = parsed.get("chunks")

    # If a portion is missing or wrong type, try a targeted schema-repair
    if not isinstance(doc_data, dict):
        try:
            repair_text = llm_fn(
                "You are a JSON repair assistant. Return ONLY valid JSON, no markdown fences.",
                [{"role": "user", "content": (
                    "Extract and return ONLY the document portion as valid JSON.\n"
                    f"Original response:\n{raw_text}\n"
                )}],
            )
            doc_data = enrich_llm.parse_json_or_repair(
                llm_fn, repair_text, "document enrichment object"
            )
            if not isinstance(doc_data, dict):
                doc_data = None
        except Exception:
            doc_data = None

    if not isinstance(chunks_data, list):
        try:
            repair_text = llm_fn(
                "You are a JSON repair assistant. Return ONLY valid JSON, no markdown fences.",
                [{"role": "user", "content": (
                    "Extract and return ONLY the chunks array as valid JSON "
                    "wrapped in {\"chunks\": [...]}.\n"
                    f"Original response:\n{raw_text}\n"
                )}],
            )
            repaired = enrich_llm.parse_json_or_repair(
                llm_fn, repair_text, "chunks enrichment array"
            )
            chunks_data = repaired.get("chunks") if isinstance(repaired, dict) else None
            if not isinstance(chunks_data, list):
                chunks_data = None
        except Exception:
            chunks_data = None

    # Commit each portion independently via the stage functions
    doc_result = enrich_document_stage(
        output_dir, config, llm_fn,
        force=force,
        _pre_parsed_response=doc_data,
    ) if doc_data is not None else {"status": "failed", "detail": "Combined call: document portion invalid"}

    chunk_result = enrich_chunks_stage(
        output_dir, config, llm_fn,
        force=force,
        _pre_parsed_response={"chunks": chunks_data},
    ) if chunks_data is not None else {"status": "failed", "detail": "Combined call: chunks portion invalid"}

    return doc_result, chunk_result


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

    # Defer llm_fn construction until we know something is stale (agent CLIs are slow)
    import threading
    _llm_fn_ref = [llm_fn]
    _llm_fn_lock = threading.Lock()

    def _get_llm_fn():
        with _llm_fn_lock:
            if _llm_fn_ref[0] is None:
                from arquimedes.enrich_llm import make_cli_llm_fn
                _llm_fn_ref[0] = make_cli_llm_fn(config)
            return _llm_fn_ref[0]

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

        # Check if combined doc+chunk call is appropriate
        use_combined = (
            not dry_run
            and "document" in requested_stages
            and "chunk" in requested_stages
            and _should_combine(output_dir)
        )

        # Track which stages were handled by the combined call
        combined_handled: set[str] = set()

        if use_combined:
            doc_stale = force or _is_document_stale(output_dir, config)
            chunk_stale = force or _is_chunk_stale(output_dir, config)

            if doc_stale and chunk_stale:
                doc_result, chunk_result = _run_combined_enrichment(
                    output_dir, config, _get_llm_fn(), force=force,
                )
                if doc_result is not None:
                    material_results["document"] = doc_result
                    combined_handled.add("document")
                    if doc_result["status"] == "failed":
                        succeeded = False
                if chunk_result is not None:
                    material_results["chunk"] = chunk_result
                    combined_handled.add("chunk")
                    if chunk_result["status"] == "failed":
                        succeeded = False

        # Determine remaining stages after combined handling
        remaining = [s for s in requested_stages if s not in combined_handled]

        if dry_run:
            for stage_name in remaining:
                if stage_name == "document":
                    stale = _is_document_stale(output_dir, config)
                elif stage_name == "chunk":
                    stale = _is_chunk_stale(output_dir, config)
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
                if force:
                    stale_stages.add(s)
                elif s == "document" and _is_document_stale(output_dir, config):
                    stale_stages.add(s)
                elif s == "chunk" and _is_chunk_stale(output_dir, config):
                    stale_stages.add(s)
                elif s == "figure" and _is_figure_stale(output_dir, config):
                    stale_stages.add(s)

            for s in remaining:
                if s not in stale_stages:
                    material_results[s] = {"status": "skipped", "detail": "up to date"}

            # Parallel path: document + figure are independent; chunk waits for document
            parallel_doc_fig = "document" in stale_stages and "figure" in stale_stages

            if parallel_doc_fig:
                from arquimedes.enrich_llm import make_cli_llm_fn as _make_fn

                # Separate llm_fn per thread to avoid last_model race
                doc_fn = _get_llm_fn()
                fig_fn = _make_fn(config)

                with ThreadPoolExecutor(max_workers=2) as stage_pool:
                    fig_future = stage_pool.submit(
                        enrich_figures_stage, output_dir, config, fig_fn, force=force
                    )

                    # Document in main thread so chunk can follow immediately
                    doc_result = enrich_document_stage(
                        output_dir, config, doc_fn, force=force
                    )
                    material_results["document"] = doc_result
                    if doc_result["status"] == "failed":
                        succeeded = False

                    # Chunk after document (uses doc summary in prompt context)
                    if "chunk" in stale_stages:
                        chunk_result = enrich_chunks_stage(
                            output_dir, config, doc_fn, force=force
                        )
                        material_results["chunk"] = chunk_result
                        if chunk_result["status"] == "failed":
                            succeeded = False

                    # Collect figure result
                    fig_result = fig_future.result()
                    material_results["figure"] = fig_result
                    if fig_result["status"] == "failed":
                        succeeded = False
            else:
                # Sequential: doc → chunk → figure (preserves dependency order)
                for s in ("document", "chunk", "figure"):
                    if s not in stale_stages:
                        continue
                    if s == "document":
                        result = enrich_document_stage(
                            output_dir, config, _get_llm_fn(), force=force
                        )
                    elif s == "chunk":
                        result = enrich_chunks_stage(
                            output_dir, config, _get_llm_fn(), force=force
                        )
                    else:
                        result = enrich_figures_stage(
                            output_dir, config, _get_llm_fn(), force=force
                        )
                    material_results[s] = result
                    if result["status"] == "failed":
                        succeeded = False

        return mid, material_results, succeeded

    # -----------------------------------------------------------------------
    # Execute — parallel when multiple materials, sequential for single
    # -----------------------------------------------------------------------

    results: dict[str, dict] = {}
    all_succeeded = True
    material_ids = list(to_process.keys())
    parallel = config.get("enrichment", {}).get("parallel", 4)

    if len(material_ids) <= 1 or dry_run or parallel <= 1:
        # Sequential
        for mid in material_ids:
            mid, mat_result, ok = _enrich_one_material(mid)
            results[mid] = mat_result
            if not ok:
                all_succeeded = False
    else:
        # Parallel — each thread gets its own llm_fn via _get_llm_fn()
        with ThreadPoolExecutor(max_workers=parallel) as pool:
            futures = {pool.submit(_enrich_one_material, mid): mid for mid in material_ids}
            for future in as_completed(futures):
                mid, mat_result, ok = future.result()
                results[mid] = mat_result
                if not ok:
                    all_succeeded = False

    return results, all_succeeded
