"""Figure enrichment stage — Phase 3.

Enriches per-figure metadata: visual_type, description, caption.
Uses vision (image content) when available, falls back to text-only context.
Processes stale figures per-figure, batched into groups.
"""

from __future__ import annotations

import json
from pathlib import Path

from arquimedes import enrich_llm, enrich_prompts, enrich_stamps
from arquimedes.enrich_llm import get_model_id
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


def _save_jsonl(path: Path, rows: list[dict]) -> None:
    """Write rows to a JSONL file."""
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


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


_SCAN_ARTIFACT_PHRASES = (
    "scanned title page",
    "scanned article text page",
    "scanned page of",
    "text-only page",
    "body-text page",
    "dense prose",
    "continuous prose",
    "no standalone illustration",
    "no graphic content",
    "article text and page footer",
    "journal footer and page number",
    "title page of the article",
)


def _is_full_page_bbox(bbox: list[float]) -> bool:
    """Heuristic: bbox looks like a full-page capture."""
    if len(bbox) != 4:
        return False
    x0, y0, x1, y1 = bbox
    width = x1 - x0
    height = y1 - y0
    return x0 <= 1.0 and y0 <= 1.0 and width >= 400 and height >= 700


def _should_delete_figure(sidecar: dict) -> bool:
    """Return True if the enriched figure is safe to delete as non-substantive."""
    relevance = str(sidecar.get("relevance", "")).strip().lower()
    if relevance in {"decorative", "front_matter"}:
        return True

    if sidecar.get("extraction_method") != "embedded":
        return False
    if not _is_full_page_bbox(sidecar.get("bbox", [])):
        return False

    description = sidecar.get("description")
    if isinstance(description, dict):
        description = description.get("value", "")
    description_text = str(description or "").strip().lower()
    return any(phrase in description_text for phrase in _SCAN_ARTIFACT_PHRASES)


def _cleanup_non_substantive_figures(output_dir: Path) -> int:
    """Delete non-substantive figure artifacts and remove page refs."""
    figures_dir = output_dir / "figures"
    if not figures_dir.exists():
        return 0

    sidecar_paths = sorted(figures_dir.glob("*.json"))
    delete_ids: set[str] = set()
    delete_paths: list[Path] = []
    image_paths: list[Path] = []

    for sidecar_path in sidecar_paths:
        sidecar = _load_json(sidecar_path, default={}) or {}
        if not _should_delete_figure(sidecar):
            continue
        figure_id = sidecar.get("figure_id", "")
        if figure_id:
            delete_ids.add(figure_id)
        delete_paths.append(sidecar_path)
        image_path_str = sidecar.get("image_path", "")
        if image_path_str:
            image_path = Path(image_path_str)
            if not image_path.is_absolute():
                image_path = output_dir / image_path
            image_paths.append(image_path)

    if not delete_ids:
        return 0

    pages_path = output_dir / "pages.jsonl"
    if pages_path.exists():
        pages = _load_jsonl(pages_path)
        for page in pages:
            refs = page.get("figure_refs", [])
            if refs:
                page["figure_refs"] = [ref for ref in refs if ref not in delete_ids]
        _save_jsonl(pages_path, pages)

    for path in delete_paths:
        path.unlink(missing_ok=True)
    for path in image_paths:
        path.unlink(missing_ok=True)

    return len(delete_ids)


_VALID_VISUAL_TYPES = frozenset(
    {"plan", "section", "elevation", "detail", "photo", "diagram", "chart", "render", "sketch"}
)
_VALID_RELEVANCE = frozenset({"substantive", "decorative", "front_matter"})


def _parse_figure_jsonl(raw_text: str) -> dict[str, dict]:
    """Parse compact JSONL figure response into figure_id → fields dict.

    Each line: {"id":"fig_NNN","vt":"...","rel":"...","desc":"...","cap":"..."}
    Malformed lines are skipped.
    """
    result: dict[str, dict] = {}
    for line in raw_text.splitlines():
        line = line.strip()
        if not line or not line.startswith("{"):
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        figure_id = obj.get("id", "")
        if not figure_id:
            continue
        result[figure_id] = {
            "visual_type": obj.get("vt", "") if obj.get("vt", "") in _VALID_VISUAL_TYPES else "",
            "relevance": obj.get("rel", "substantive") if obj.get("rel", "") in _VALID_RELEVANCE else "substantive",
            "description": obj.get("desc", ""),
            "caption": obj.get("cap", ""),
        }
    return result


def _make_enriched_field_value(value: str, model: str, prompt_version: str) -> dict:
    """Build an EnrichedField dict with deterministic confidence from a plain value."""
    return EnrichedField(
        value=value,
        provenance=Provenance.create(
            model=model,
            prompt_version=prompt_version,
            confidence=1.0,
        ),
    ).to_dict()


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
            system, messages = enrich_prompts.build_figure_batch_prompt(
                figures_with_context, doc_context_str
            )
            raw_text = llm_fn(system, messages)
            actual_model = getattr(llm_fn, "last_model", model)
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

        # Parse compact JSONL response
        response_by_id = _parse_figure_jsonl(raw_text)

        for fig in batch:
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

            # Build enriched sidecar fields
            enriched: dict = dict(sidecar)
            enriched["analysis_mode"] = analysis_mode
            enriched["visual_type"] = _make_enriched_field_value(fig_response["visual_type"], actual_model, prompt_version)
            enriched["description"] = _make_enriched_field_value(fig_response["description"], actual_model, prompt_version)
            enriched["caption"] = _make_enriched_field_value(fig_response["caption"], actual_model, prompt_version)
            enriched["relevance"] = fig_response["relevance"]
            enriched["_enrichment_stamp"] = fig["stamp"]
            enriched["_enrichment_stamp"]["model"] = actual_model
            enriched_by_path[fig["path"]] = enriched

    # 9. Atomic write: stage all sidecar files, then commit with rollback
    try:
        # Stage: write all to temp files
        temp_pairs: list[tuple[Path, Path]] = []
        for sidecar_path, enriched in enriched_by_path.items():
            tmp = sidecar_path.with_suffix(".json.tmp")
            tmp.write_text(
                json.dumps(enriched, separators=(',', ':'), ensure_ascii=False), encoding="utf-8"
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

    # 10. Deterministic cleanup of non-substantive/artifact figures
    deleted_count = _cleanup_non_substantive_figures(output_dir)

    # Build detail string
    total_batches = len(batches)
    detail_parts = [f"{len(stale_figures)} figures", f"{total_batches} batch{'es' if total_batches != 1 else ''}"]
    if n_vision_batches:
        detail_parts.append(f"{n_vision_batches} vision batch{'es' if n_vision_batches != 1 else ''}")
    if n_text_batches:
        detail_parts.append(f"{n_text_batches} text fallback batch{'es' if n_text_batches != 1 else ''}")
    if deleted_count:
        detail_parts.append(f"{deleted_count} deleted")
    detail = ", ".join(detail_parts)

    return {"status": "enriched", "detail": detail}
