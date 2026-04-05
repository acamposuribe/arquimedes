"""Figure enrichment stage — Phase 3.

Enriches per-figure metadata: visual_type, description, caption.
Uses vision (image content) when available, falls back to text-only context.
Processes stale figures per-figure, batched into groups.
"""

from __future__ import annotations

import json
from pathlib import Path

from arquimedes import enrich_llm, enrich_prompts, enrich_stamps
from arquimedes.enrich_llm import get_model_id, set_effort, set_model, set_codex_params
from arquimedes.models import EnrichedField, Provenance


# ---------------------------------------------------------------------------
# Helpers — load artifacts
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
# Caption candidate extraction
# ---------------------------------------------------------------------------


def _extract_caption_candidates(page_text: str) -> list[str]:
    """Extract likely caption lines from page text.

    Heuristic: split page text into lines and return lines containing
    'fig' or 'figure' (case-insensitive).
    """
    if not page_text:
        return []
    candidates = []
    for line in page_text.splitlines():
        stripped = line.strip()
        if stripped and ("fig" in stripped.lower() or "figure" in stripped.lower()):
            candidates.append(stripped)
    return candidates


# ---------------------------------------------------------------------------
# Schema description for repair fallback
# ---------------------------------------------------------------------------

_FIGURE_BATCH_SCHEMA_DESC = """\
{
  "figures": [
    {
      "figure_id": "...",
      "visual_type": {"value": "plan|section|elevation|detail|photo|diagram|chart|render|sketch", "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0},
      "description": {"value": "...", "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0},
      "caption": {"value": "...", "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0},
      "relevance": {"value": "substantive|decorative|front_matter", "confidence": 0.0-1.0}
    }
  ]
}"""


# ---------------------------------------------------------------------------
# Field-level helper
# ---------------------------------------------------------------------------


def _make_enriched_field(llm_field: dict, model: str, prompt_version: str) -> EnrichedField:
    """Build an EnrichedField from an LLM response dict for a single field."""
    provenance = Provenance.create(
        model=model,
        prompt_version=prompt_version,
        confidence=float(llm_field.get("confidence", 0.0)),
        source_pages=llm_field.get("source_pages", []),
        evidence_spans=llm_field.get("evidence_spans", []),
    )
    return EnrichedField(value=llm_field["value"], provenance=provenance)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def enrich_figures_stage(
    output_dir: Path,
    config: dict,
    llm_fn,
    *,
    force: bool = False,
) -> dict:
    """Enrich figure-level metadata for a single material.

    Args:
        output_dir: Path to extracted/<material_id>/ directory.
        config: Full config dict.
        llm_fn: Callable (system, messages) -> str. The LLM implementation.
        force: Re-enrich even if not stale.

    Returns:
        {"status": "enriched"|"skipped"|"failed", "detail": str}
    """
    enrichment_config = config.get("enrichment", {})
    model: str = get_model_id(config, "figure")
    prompt_version: str = enrichment_config.get("prompt_version", "enrich-v1.0")
    schema_version: str = enrichment_config.get("enrichment_schema_version", "1")
    figure_batch_size: int = enrichment_config.get("figure_batch_size", 6)

    # 1. Find all figure sidecar JSONs
    figures_dir = output_dir / "figures"
    if not figures_dir.exists():
        return {"status": "skipped", "detail": "no figures directory"}

    sidecar_paths = sorted(figures_dir.glob("*.json"))
    if not sidecar_paths:
        return {"status": "skipped", "detail": "no figures"}

    # 2. Load meta for doc_context
    try:
        meta = _load_json(output_dir / "meta.json", default={})
    except Exception as exc:
        return {"status": "failed", "detail": f"Load meta error: {exc}"}

    # Build doc_context for fingerprints
    doc_context: dict = {
        "title": meta.get("title", ""),
        "domain": meta.get("domain", ""),
    }
    if "document_type" in meta and isinstance(meta["document_type"], dict):
        dt = meta["document_type"]
        if isinstance(dt.get("value"), str):
            doc_context["document_type"] = dt["value"]

    # 3. Load pages for text lookup (keyed by page number)
    try:
        pages_list = _load_jsonl(output_dir / "pages.jsonl")
        pages_by_num: dict[int, str] = {p["page_number"]: p.get("text", "") for p in pages_list}
    except Exception as exc:
        return {"status": "failed", "detail": f"Load pages error: {exc}"}

    # 4. Determine stale figures
    stale_figures: list[dict] = []  # list of {"path": Path, "sidecar": dict, "stamp": dict}

    for sidecar_path in sidecar_paths:
        try:
            sidecar = _load_json(sidecar_path, default={})
        except Exception:
            continue

        source_page = sidecar.get("source_page", 0)
        page_text = pages_by_num.get(source_page, "")
        caption_candidates = _extract_caption_candidates(page_text)

        image_path_str = sidecar.get("image_path", "")
        # image_path may be relative to output_dir or absolute
        image_path = Path(image_path_str) if image_path_str else None
        if image_path and not image_path.is_absolute():
            image_path = output_dir / image_path

        # Compute fingerprint
        try:
            if image_path and image_path.exists():
                fingerprint = enrich_stamps.figure_fingerprint(
                    sidecar, image_path, page_text, caption_candidates, doc_context
                )
            else:
                # No image — use page_text hash as substitute
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
            fingerprint = "unknown"

        stamp = enrich_stamps.make_stamp(prompt_version, model, schema_version, fingerprint)
        existing_stamp = enrich_stamps.read_figure_stamp(sidecar_path)

        if force or enrich_stamps.is_stale(existing_stamp, stamp):
            stale_figures.append({
                "path": sidecar_path,
                "sidecar": sidecar,
                "stamp": stamp,
                "page_text": page_text,
                "caption_candidates": caption_candidates,
                "image_path": image_path,
            })

    # 5. If none stale → skip
    if not stale_figures:
        return {"status": "skipped", "detail": "up to date"}

    # 6. Build doc_context_str for prompts
    toc = _load_json(output_dir / "toc.json", default=None)
    doc_context_str = enrich_prompts.build_document_context(meta, toc, None)

    # 7. Track whether any batches used vision
    n_vision_batches = 0
    n_text_batches = 0

    # 8. Process in batches
    batches = [
        stale_figures[i : i + figure_batch_size]
        for i in range(0, len(stale_figures), figure_batch_size)
    ]

    # Accumulate enriched sidecar data per figure path (in memory, write at end)
    # Map: sidecar_path → {enriched fields + analysis_mode + stamp}
    enriched_by_path: dict[Path, dict] = {}

    for batch_idx, batch in enumerate(batches):
        # Build figures_with_context for the prompt builder
        figures_with_context = []
        batch_has_vision = False

        for fig in batch:
            sidecar = fig["sidecar"]
            image_path = fig["image_path"]
            has_image = image_path is not None and image_path.exists()
            if has_image:
                batch_has_vision = True

            figures_with_context.append({
                "figure_id": sidecar.get("figure_id", ""),
                "image_path": str(image_path) if image_path else "",
                "source_page_text": fig["page_text"],
                "caption_candidates": fig["caption_candidates"],
                "sidecar": sidecar,
            })

        if batch_has_vision:
            n_vision_batches += 1
        else:
            n_text_batches += 1

        # Call LLM
        try:
            set_effort(llm_fn, config, "figure")
            set_model(llm_fn, config, "figure")
            set_codex_params(llm_fn, config, "figure")
            system, messages = enrich_prompts.build_figure_batch_prompt(
                figures_with_context, doc_context_str
            )
            raw_text = llm_fn(system, messages)
            actual_model = getattr(llm_fn, "last_model", model)
            parsed = enrich_llm.parse_json_or_repair(
                llm_fn, raw_text, _FIGURE_BATCH_SCHEMA_DESC
            )
        except enrich_llm.EnrichmentError as exc:
            return {
                "status": "failed",
                "detail": f"Batch {batch_idx + 1}/{len(batches)} LLM error: {exc}",
            }
        except Exception as exc:
            return {
                "status": "failed",
                "detail": f"Batch {batch_idx + 1}/{len(batches)} error: {exc}",
            }

        # Map responses back by figure_id
        response_by_id: dict[str, dict] = {}
        figures_response = parsed.get("figures", [])
        if isinstance(figures_response, list):
            for fig_data in figures_response:
                fid = fig_data.get("figure_id", "")
                if fid:
                    response_by_id[fid] = fig_data

        for fig, fig_ctx in zip(batch, figures_with_context):
            sidecar = fig["sidecar"]
            figure_id = sidecar.get("figure_id", "")
            image_path = fig["image_path"]
            has_image = image_path is not None and image_path.exists()
            analysis_mode = "vision" if has_image else "text_fallback"

            fig_response = response_by_id.get(figure_id)
            if fig_response is None:
                return {
                    "status": "failed",
                    "detail": f"Batch {batch_idx + 1}/{len(batches)}: LLM output missing figure '{figure_id}'",
                }

            # Validate required fields and their 'value' key
            for req_field in ("visual_type", "description", "caption"):
                if req_field not in fig_response or not isinstance(fig_response[req_field], dict):
                    return {
                        "status": "failed",
                        "detail": f"Figure '{figure_id}' missing required field '{req_field}'",
                    }
                if "value" not in fig_response[req_field]:
                    return {
                        "status": "failed",
                        "detail": f"Figure '{figure_id}' field '{req_field}' missing 'value'",
                    }

            # Build enriched sidecar fields
            enriched: dict = dict(sidecar)
            enriched["analysis_mode"] = analysis_mode

            try:
                ef = _make_enriched_field(fig_response["visual_type"], actual_model, prompt_version)
                enriched["visual_type"] = ef.to_dict()

                ef = _make_enriched_field(fig_response["description"], actual_model, prompt_version)
                enriched["description"] = ef.to_dict()

                ef = _make_enriched_field(fig_response["caption"], actual_model, prompt_version)
                enriched["caption"] = ef.to_dict()

                # Relevance classification (optional — graceful fallback)
                rel = fig_response.get("relevance")
                if isinstance(rel, dict) and "value" in rel:
                    enriched["relevance"] = rel["value"]
                elif isinstance(rel, str) and rel:
                    enriched["relevance"] = rel
                else:
                    enriched["relevance"] = "substantive"  # default
            except Exception as exc:
                return {
                    "status": "failed",
                    "detail": f"Figure '{figure_id}' field mapping error: {exc}",
                }

            enriched["_enrichment_stamp"] = fig["stamp"]
            enriched["_enrichment_stamp"]["model"] = actual_model  # actual responding model
            enriched_by_path[fig["path"]] = enriched

    # 9. Atomic write: stage all sidecar files, then commit with rollback
    try:
        # Stage: write all to temp files
        temp_pairs: list[tuple[Path, Path]] = []
        for sidecar_path, enriched in enriched_by_path.items():
            tmp = sidecar_path.with_suffix(".json.tmp")
            tmp.write_text(
                json.dumps(enriched, indent=2, ensure_ascii=False), encoding="utf-8"
            )
            temp_pairs.append((tmp, sidecar_path))

        # Backup originals for rollback
        backup_pairs: list[tuple[Path, Path]] = []  # (final, backup)
        for _tmp, final in temp_pairs:
            bak = final.with_suffix(".json.bak")
            if final.exists():
                final.replace(bak)
            backup_pairs.append((final, bak))

        # Commit: rename all temps to final
        committed: list[tuple[Path, Path]] = []
        try:
            for tmp, final in temp_pairs:
                tmp.replace(final)
                bak = final.with_suffix(".json.bak")
                committed.append((final, bak))
        except Exception:
            # Rollback: restore backups for committed files
            for final_path, backup_path in committed:
                try:
                    if backup_path.exists():
                        backup_path.replace(final_path)
                except Exception:
                    pass
            # Clean up remaining temp files
            for tmp, _ in temp_pairs:
                try:
                    tmp.unlink(missing_ok=True)
                except Exception:
                    pass
            raise

        # Clean up backups on success
        for _final, bak in backup_pairs:
            try:
                bak.unlink(missing_ok=True)
            except Exception:
                pass
    except Exception as exc:
        return {"status": "failed", "detail": f"Write sidecar error: {exc}"}

    # Build detail string
    total_batches = len(batches)
    detail_parts = [f"{len(stale_figures)} figures", f"{total_batches} batch{'es' if total_batches != 1 else ''}"]
    if n_vision_batches:
        detail_parts.append(f"{n_vision_batches} vision batch{'es' if n_vision_batches != 1 else ''}")
    if n_text_batches:
        detail_parts.append(f"{n_text_batches} text fallback batch{'es' if n_text_batches != 1 else ''}")
    detail = ", ".join(detail_parts)

    return {"status": "enriched", "detail": detail}
