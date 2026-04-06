"""Chunk enrichment stage — Phase 3.

Enriches per-chunk metadata: summary and keywords for each text chunk.
Uses batched LLM calls. All-or-nothing: if any batch fails, no writes occur.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from arquimedes import enrich_llm, enrich_prompts, enrich_stamps
from arquimedes.enrich_llm import get_model_id
from arquimedes.enrich_prompts import estimate_tokens


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


def _debug_enabled() -> bool:
    value = os.getenv("ARQ_LLM_DEBUG", "")
    return value.strip().lower() not in {"", "0", "false", "no", "off"}


def _debug(message: str) -> None:
    if _debug_enabled():
        print(f"[chunk-debug] {message}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Schema description for repair fallback
# ---------------------------------------------------------------------------

_VALID_CONTENT_CLASSES = frozenset(
    {"argument", "methodology", "case_study", "bibliography", "front_matter", "caption", "appendix"}
)


def _parse_chunk_jsonl(raw_text: str) -> dict[str, dict]:
    """Parse compact JSONL chunk response into chunk_id → enrichment dict.

    Each line: {"id":"chk_XXXXX","cls":"...","kw":["..."],"s":"..."}
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
        chunk_id = obj.get("id", "")
        if not chunk_id:
            continue
        enrichment: dict = {}
        s = obj.get("s", "")
        if s:
            enrichment["summary"] = s
        kw = obj.get("kw")
        if isinstance(kw, list):
            enrichment["keywords"] = [str(k) for k in kw if k][:3]
        cls = obj.get("cls", "")
        if cls in _VALID_CONTENT_CLASSES:
            enrichment["content_class"] = cls
        if enrichment:
            result[chunk_id] = enrichment
    return result


# ---------------------------------------------------------------------------
# Field-level helper
# ---------------------------------------------------------------------------


def _make_enrichment(parsed: dict, model: str, prompt_version: str) -> dict:
    """Build chunk enrichment dict from parsed JSONL entry.

    Wraps summary and keywords in EnrichedField shape for compatibility with
    index._val() and index._kw_json() which expect {"value": ...}.
    """
    provenance = {
        "model": model,
        "prompt_version": prompt_version,
        "confidence": 1.0,
        "enriched_at": datetime.now(timezone.utc).isoformat(),
    }
    enrichment: dict = {}
    s = parsed.get("summary", "")
    if s:
        enrichment["summary"] = {"value": s, "provenance": provenance}
    kw = parsed.get("keywords", [])
    if kw:
        enrichment["keywords"] = {"value": kw, "provenance": provenance}
    cls = parsed.get("content_class", "")
    if cls:
        enrichment["content_class"] = cls
    return enrichment


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def enrich_chunks_stage(
    output_dir: Path,
    config: dict,
    llm_fn,
    *,
    force: bool = False,
) -> dict:
    """Enrich chunk-level metadata for a single material.

    Args:
        output_dir: Path to extracted/<material_id>/ directory.
        config: Full config dict.
        llm_fn: Callable (system, messages) -> str. The LLM implementation.
        force: Re-enrich even if not stale.

    Returns:
        {"status": "enriched"|"skipped"|"failed", "detail": str}
    """
    enrichment_config = config.get("enrichment", {})
    model: str = get_model_id(config, "chunk")
    prompt_version: str = enrichment_config.get("prompt_version", "enrich-v1.0")
    schema_version: str = enrichment_config.get("enrichment_schema_version", "1")
    batch_target: int = enrichment_config.get("chunk_batch_target", 50)

    # 1. Load artifacts
    try:
        chunks = _load_jsonl(output_dir / "chunks.jsonl")
        meta = _load_json(output_dir / "meta.json", default={})
        toc = _load_json(output_dir / "toc.json", default=None)
    except Exception as exc:
        return {"status": "failed", "detail": f"Load error: {exc}"}

    if not chunks:
        return {"status": "skipped", "detail": "no chunks"}

    # 2. Build doc_context for fingerprint and prompt
    # Collect headings from toc or from pages
    headings: list[str] = []
    if toc and isinstance(toc, list):
        headings = [entry.get("title", "") for entry in toc if entry.get("title")]
    else:
        pages = _load_jsonl(output_dir / "pages.jsonl")
        for page in pages:
            headings.extend(page.get("headings", []))

    # Include current doc summary + stamp if present, to detect doc-level re-enrichment
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

    # 3. Compute per-chunk fingerprints and staleness
    try:
        annotations = _load_jsonl(output_dir / "annotations.jsonl")
    except Exception as exc:
        return {"status": "failed", "detail": f"Load annotations error: {exc}"}

    existing_stamps = enrich_stamps.read_chunk_stamps(output_dir)
    stale_chunks: list[dict] = []
    stale_stamps: dict[str, dict] = {}  # chunk_id → target stamp for stale chunks
    for chunk in chunks:
        chunk_id = chunk.get("chunk_id", "")
        fp = enrich_stamps.single_chunk_fingerprint(chunk, annotations, doc_context)
        target_stamp = enrich_stamps.make_stamp(prompt_version, model, schema_version, fp)
        existing = existing_stamps.get(chunk_id)
        if force or enrich_stamps.is_stale(existing, target_stamp):
            stale_chunks.append(chunk)
            stale_stamps[chunk_id] = target_stamp

    if not stale_chunks:
        return {"status": "skipped", "detail": "up to date"}

    # 4. Build doc_context_str for prompts
    doc_context_str = enrich_prompts.build_document_context(meta, toc, headings if headings else None)

    # 5. Batch stale chunks, writing chunks.jsonl + stamps incrementally after each batch
    #    so retries resume without re-spending tokens.
    # Key: chunk_id → enrichment dict
    chunk_enrichments: dict[str, dict] = {}
    # Track actual responding model for provenance (updated after LLM calls)
    actual_model: str = getattr(llm_fn, "last_model", model)

    # Build an in-memory map of all chunks (preserves existing enrichment on fresh ones)
    enriched_chunks_map: dict[str, dict] = {
        c.get("chunk_id", ""): dict(c) for c in chunks
    }

    avg_chunk_tokens = max(
        sum(estimate_tokens(c.get("text", "")) for c in stale_chunks) // len(stale_chunks), 1
    )
    batch_token_budget = batch_target * avg_chunk_tokens

    batches: list[list[dict]] = []
    current_batch: list[dict] = []
    current_tokens = 0
    for chunk in stale_chunks:
        tok = estimate_tokens(chunk.get("text", ""))
        if current_batch and current_tokens + tok > batch_token_budget:
            batches.append(current_batch)
            current_batch = [chunk]
            current_tokens = tok
        else:
            current_batch.append(chunk)
            current_tokens += tok
    if current_batch:
        batches.append(current_batch)

    n_batches = len(batches)
    chunks_path = output_dir / "chunks.jsonl"
    if _debug_enabled():
        _debug(
            f"material={output_dir.name} stale_chunks={len(stale_chunks)} "
            f"batch_target={batch_target} avg_chunk_tokens={avg_chunk_tokens} "
            f"batch_token_budget={batch_token_budget} batches={n_batches}"
        )

    for batch_idx, batch in enumerate(batches):
        try:
            if _debug_enabled():
                batch_ids = ",".join(c.get("chunk_id", "") for c in batch)
                _debug(
                    f"batch={batch_idx + 1}/{n_batches} size={len(batch)} chunk_ids={batch_ids}"
                )
            system, messages = enrich_prompts.build_chunk_batch_prompt(
                batch, doc_context_str, annotations
            )
            raw_text = llm_fn(system, messages)
            actual_model = getattr(llm_fn, "last_model", model)
        except enrich_llm.EnrichmentError as exc:
            return {"status": "failed", "detail": f"Batch {batch_idx + 1}/{n_batches} LLM error: {exc}"}
        except Exception as exc:
            return {"status": "failed", "detail": f"Batch {batch_idx + 1}/{n_batches} error: {exc}"}

        # Parse compact JSONL response
        parsed_batch = _parse_chunk_jsonl(raw_text)
        batch_enrichments: dict[str, dict] = {
            chunk_id: _make_enrichment(parsed, actual_model, prompt_version)
            for chunk_id, parsed in parsed_batch.items()
            if parsed
        }

        # Merge into in-memory map and accumulate enrichments
        for chunk_id, enrichment in batch_enrichments.items():
            if chunk_id in enriched_chunks_map:
                enriched_chunks_map[chunk_id].update(enrichment)
            chunk_enrichments[chunk_id] = enrichment

        # Write stamps for returned chunks immediately (missing ones stay stale)
        for chunk_id in batch_enrichments:
            if chunk_id in stale_stamps:
                st = stale_stamps[chunk_id].copy()
                st["model"] = actual_model
                existing_stamps[chunk_id] = st

        # Incremental atomic write: chunks.jsonl then stamps
        try:
            tmp_chunks = chunks_path.with_suffix(".jsonl.tmp")
            with open(tmp_chunks, "w", encoding="utf-8") as f:
                for chunk in chunks:
                    cid = chunk.get("chunk_id", "")
                    f.write(json.dumps(enriched_chunks_map.get(cid, chunk), ensure_ascii=False) + "\n")
            tmp_chunks.replace(chunks_path)
            enrich_stamps.write_chunk_stamps(output_dir, existing_stamps)
        except Exception as exc:
            return {"status": "failed", "detail": f"Write error on batch {batch_idx + 1}: {exc}"}

    # 6. Validate completeness. Allow up to 3 missing chunks (LLM occasionally skips one).
    stale_ids = {c.get("chunk_id", "") for c in stale_chunks if c.get("chunk_id")}
    missing_ids = stale_ids - set(chunk_enrichments.keys())
    if len(missing_ids) > 3:
        return {
            "status": "failed",
            "detail": f"LLM output missing {len(missing_ids)} chunk(s): {sorted(missing_ids)[:5]}",
        }
    if missing_ids:
        print(
            f"  [chunk] warning: {len(missing_ids)} chunk(s) not returned by LLM "
            f"({sorted(missing_ids)}), proceeding without them",
            file=sys.stderr,
        )

    # Validate each enriched chunk has required summary field
    for cid, enrichment in chunk_enrichments.items():
        if "summary" not in enrichment:
            return {
                "status": "failed",
                "detail": f"Chunk '{cid}' missing required field: summary",
            }

    # Incremental writes already happened per-batch — nothing more to write.
    n_enriched = len(chunk_enrichments)
    detail = f"{n_enriched} chunks, {n_batches} batch{'es' if n_batches != 1 else ''}"
    return {"status": "enriched", "detail": detail}
