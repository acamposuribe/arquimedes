"""Figure enrichment stage — Phase 3.

Enriches per-figure metadata: visual_type, description, caption.
Uses vision (image content) when available, falls back to text-only context.
Processes stale figures per-figure, batched into groups.
"""

from __future__ import annotations

import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from arquimedes import enrich_prompts, enrich_stamps, llm
from arquimedes.domain_profiles import domain_prompt_version
from arquimedes.llm import get_model_id
from arquimedes.models import EnrichedField


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


def _configured_parallel_requests(config: dict, key: str) -> int:
    value = config.get("enrichment", {}).get(key, 1)
    try:
        return max(1, int(value))
    except (TypeError, ValueError):
        return 1


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
        lowered = stripped.lower()
        if stripped and ("fig" in lowered or "figure" in lowered or "figura" in lowered):
            candidates.append(stripped)
    return candidates


_SCAN_ARTIFACT_PHRASES = (
    "scanned title page",
    "scanned article text page",
    "scanned page of",
    "text-only page",
    "empty image",
    "blank image",
    "blank scan",
    "scanner artifact",
    "scan artifact",
    "page-edge artifact",
    "cropped sliver",
    "tiny cropped fragment",
    "body-text page",
    "dense prose",
    "continuous prose",
    "no standalone illustration",
    "no graphic content",
    "article text and page footer",
    "journal footer and page number",
    "title page of the article",
    "portada escaneada",
    "página escaneada",
    "pagina escaneada con texto",
    "página escaneada con texto",
    "pagina de solo texto",
    "página de solo texto",
    "imagen vacia",
    "imagen vacía",
    "imagen en blanco",
    "escaneo en blanco",
    "artefacto de escaneo",
    "artefacto del escaner",
    "artefacto del escáner",
    "fragmento recortado",
    "recorte muy pequeno",
    "recorte muy pequeño",
    "pagina de texto",
    "página de texto",
    "prosa densa",
    "prosa continua",
    "sin ilustracion independiente",
    "sin ilustración independiente",
    "sin contenido grafico",
    "sin contenido gráfico",
    "pie de pagina y numero de pagina",
    "pie de página y número de página",
    "portada del articulo",
    "portada del artículo",
)


def _is_full_page_bbox(bbox: list[float]) -> bool:
    """Heuristic: bbox looks like a full-page capture."""
    if len(bbox) != 4:
        return False
    x0, y0, x1, y1 = bbox
    width = x1 - x0
    height = y1 - y0
    return x0 <= 1.0 and y0 <= 1.0 and width >= 400 and height >= 700


def _bbox_dimensions(bbox: list[float]) -> tuple[float, float]:
    if len(bbox) != 4:
        return 0.0, 0.0
    x0, y0, x1, y1 = bbox
    return max(x1 - x0, 0.0), max(y1 - y0, 0.0)


def _is_tiny_bbox(bbox: list[float]) -> bool:
    """Heuristic: bbox is too small to plausibly contain substantial figure content."""
    width, height = _bbox_dimensions(bbox)
    if width <= 0 or height <= 0:
        return False
    return width < 48 or height < 48 or (width * height) < 3500


def _artifact_hint(sidecar: dict, page_text: str) -> str:
    """Deterministic hint describing likely artifact behavior for the prompt."""
    bbox = sidecar.get("bbox", [])
    if _is_full_page_bbox(bbox):
        return "near-full-page capture; likely scan artifact or text page rather than a standalone figure"
    if _is_tiny_bbox(bbox):
        return "very small crop; likely inline artifact, partial scan fragment, icon, or empty non-figure"
    if sidecar.get("extraction_method") == "embedded" and len((page_text or "").strip()) > 1200:
        return "embedded image on text-heavy page; check whether this is merely an inline scan fragment or artifact"
    return ""


def _should_delete_figure(sidecar: dict) -> bool:
    """Return True if the enriched figure is safe to delete as non-substantive."""
    relevance = str(sidecar.get("relevance", "")).strip().lower()
    if relevance in {"decorative", "front_matter"}:
        return True

    if sidecar.get("extraction_method") != "embedded":
        return False
    if _is_tiny_bbox(sidecar.get("bbox", [])):
        description = sidecar.get("description")
        if isinstance(description, dict):
            description = description.get("value", "")
        description_text = str(description or "").strip().lower()
        if any(phrase in description_text for phrase in _SCAN_ARTIFACT_PHRASES):
            return True
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

_FENCE_RE = re.compile(r"```(?:json)?\s*(.*?)```", re.DOTALL | re.IGNORECASE)


def _extract_json_objects(raw_text: str) -> list[dict]:
    """Recover figure response objects from JSONL, arrays, or wrapped JSON.

    The live figure routes sometimes return valid JSON in shapes other than
    strict one-object-per-line JSONL, especially for multimodal requests.
    """
    text = raw_text.strip()
    if not text:
        return []

    match = _FENCE_RE.search(text)
    if match:
        text = match.group(1).strip()

    objects: list[dict] = []

    def _extend_from_value(value) -> bool:
        if isinstance(value, dict):
            if any(key in value for key in ("id", "figure_id", "vt", "visual_type", "figures", "items")):
                seq = value.get("figures") or value.get("items")
                if isinstance(seq, list):
                    for item in seq:
                        if isinstance(item, dict):
                            objects.append(item)
                    return True
                if "id" in value or "figure_id" in value:
                    objects.append(value)
                    return True
            return False
        if isinstance(value, list):
            appended = False
            for item in value:
                if isinstance(item, dict):
                    objects.append(item)
                    appended = True
            return appended
        return False

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        parsed = None
    if parsed is not None and _extend_from_value(parsed):
        return objects

    for line in text.splitlines():
        line = line.strip().rstrip(",")
        if not line:
            continue
        if line.startswith("[") or line.startswith("]"):
            continue
        try:
            parsed_line = json.loads(line)
        except json.JSONDecodeError:
            continue
        _extend_from_value(parsed_line)

    return objects


def _normalize_figure_response(obj: dict) -> tuple[str, dict] | None:
    """Normalize a parsed figure response object into the internal schema."""
    figure_id = str(obj.get("id") or obj.get("figure_id") or "").strip()
    if not figure_id:
        return None

    visual_type = str(obj.get("vt") or obj.get("visual_type") or "").strip()
    if visual_type not in _VALID_VISUAL_TYPES:
        visual_type = ""

    relevance = str(obj.get("rel") or obj.get("relevance") or "substantive").strip()
    if relevance not in _VALID_RELEVANCE:
        relevance = "substantive"

    description = str(obj.get("desc") or obj.get("description") or "")
    caption = str(obj.get("cap") or obj.get("caption") or "")

    return figure_id, {
        "visual_type": visual_type,
        "relevance": relevance,
        "description": description,
        "caption": caption,
    }


def _parse_figure_jsonl(raw_text: str) -> dict[str, dict]:
    """Parse compact JSONL figure response into figure_id → fields dict.

    Each line: {"id":"fig_NNN","vt":"...","rel":"...","desc":"...","cap":"..."}
    Malformed lines are skipped.
    """
    result: dict[str, dict] = {}
    for obj in _extract_json_objects(raw_text):
        normalized = _normalize_figure_response(obj)
        if normalized is None:
            continue
        figure_id, payload = normalized
        result[figure_id] = payload
    return result


def _make_enriched_field_value(
    value: str,
    model: str,
    prompt_version: str,
    *,
    source_page: int,
    evidence_spans: list[str] | None = None,
) -> dict:
    """Build a value-only figure field; provenance lives in the figure stamp."""
    del model, prompt_version, source_page, evidence_spans
    return EnrichedField(value=value).to_dict()


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

    prompt_version = domain_prompt_version(prompt_version, str(meta.get("domain", "")))

    # Build doc_context for fingerprints
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
    doc_context_str = enrich_prompts.build_figure_context(meta)

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

    llm_factory = llm_fn.__dict__.get("_arq_factory") if hasattr(llm_fn, "__dict__") else None
    requested_parallelism = _configured_parallel_requests(config, "figure_parallel_requests")
    can_parallelize = callable(llm_factory) and len(batches) > 1
    worker_count = min(requested_parallelism, len(batches)) if can_parallelize else 1

    def _run_batch(batch_idx: int, batch: list[dict]) -> tuple[int, dict[Path, dict], bool]:
        batch_llm_fn = llm_factory() if worker_count > 1 else llm_fn
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
                "artifact_hint": _artifact_hint(sidecar, fig["page_text"]),
                "sidecar": sidecar,
            })

        system, messages = enrich_prompts.build_figure_batch_prompt(
            figures_with_context,
            doc_context_str,
            domain=str(meta.get("domain", "")),
        )
        raw_text = batch_llm_fn(system, messages)
        actual_model = getattr(batch_llm_fn, "last_model", model)
        response_by_id = _parse_figure_jsonl(raw_text)
        enriched_batch: dict[Path, dict] = {}

        for fig in batch:
            sidecar = fig["sidecar"]
            figure_id = sidecar.get("figure_id", "")
            image_path = fig["image_path"]
            has_image = image_path is not None and image_path.exists()
            analysis_mode = "vision" if has_image else "text_fallback"
            fig_response = response_by_id.get(figure_id)
            if fig_response is None:
                raise ValueError(f"LLM output missing figure '{figure_id}'")

            enriched: dict = dict(sidecar)
            enriched["analysis_mode"] = analysis_mode
            enriched["relevance"] = fig_response["relevance"]
            enriched["visual_type"] = _make_enriched_field_value(
                fig_response["visual_type"],
                actual_model,
                prompt_version,
                source_page=0,
            )
            enriched["description"] = _make_enriched_field_value(
                fig_response["description"],
                actual_model,
                prompt_version,
                source_page=0,
            )
            enriched["caption"] = _make_enriched_field_value(
                fig_response["caption"],
                actual_model,
                prompt_version,
                source_page=0,
            )
            enriched["_enrichment_stamp"] = fig["stamp"].copy()
            enriched["_enrichment_stamp"]["model"] = actual_model
            enriched_batch[fig["path"]] = enriched

        return batch_idx, enriched_batch, batch_has_vision

    batch_results: dict[int, tuple[dict[Path, dict], bool]] = {}
    if worker_count > 1:
        with ThreadPoolExecutor(max_workers=worker_count) as pool:
            future_to_idx = {
                pool.submit(_run_batch, batch_idx, batch): batch_idx
                for batch_idx, batch in enumerate(batches)
            }
            for future in as_completed(future_to_idx):
                batch_idx = future_to_idx[future]
                try:
                    result_idx, enriched_batch, batch_has_vision = future.result()
                except llm.EnrichmentError as exc:
                    return {
                        "status": "failed",
                        "detail": f"Batch {batch_idx + 1}/{len(batches)} LLM error: {exc}",
                    }
                except Exception as exc:
                    return {
                        "status": "failed",
                        "detail": f"Batch {batch_idx + 1}/{len(batches)} error: {exc}",
                    }
                batch_results[result_idx] = (enriched_batch, batch_has_vision)
    else:
        for batch_idx, batch in enumerate(batches):
            try:
                result_idx, enriched_batch, batch_has_vision = _run_batch(batch_idx, batch)
            except llm.EnrichmentError as exc:
                return {
                    "status": "failed",
                    "detail": f"Batch {batch_idx + 1}/{len(batches)} LLM error: {exc}",
                }
            except Exception as exc:
                return {
                    "status": "failed",
                    "detail": f"Batch {batch_idx + 1}/{len(batches)} error: {exc}",
                }
            batch_results[result_idx] = (enriched_batch, batch_has_vision)

    for batch_idx in range(len(batches)):
        enriched_batch, batch_has_vision = batch_results[batch_idx]
        if batch_has_vision:
            n_vision_batches += 1
        else:
            n_text_batches += 1
        enriched_by_path.update(enriched_batch)

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
