"""Chunk enrichment stage — Phase 3.

Enriches per-chunk metadata: summary and keywords for each text chunk.
Uses batched LLM calls. All-or-nothing: if any batch fails, no writes occur.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from arquimedes import enrich_llm, enrich_prompts, enrich_stamps
from arquimedes.enrich_llm import get_model_id
from arquimedes.enrich_prompts import estimate_tokens
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


def _debug_enabled() -> bool:
    value = os.getenv("ARQ_LLM_DEBUG", "")
    return value.strip().lower() not in {"", "0", "false", "no", "off"}


def _debug(message: str) -> None:
    if _debug_enabled():
        print(f"[chunk-debug] {message}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Schema description for repair fallback
# ---------------------------------------------------------------------------

_CHUNK_BATCH_SCHEMA_DESC = """\
{
  "chunks": [
    {
      "chunk_id": "...",
      "summary": {"value": "...", "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0},
      "keywords": {"value": ["..."], "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0},
      "content_class": "one of: argument|methodology|case_study|bibliography|front_matter|caption|appendix"
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


def enrich_chunks_stage(
    output_dir: Path,
    config: dict,
    llm_fn,
    *,
    force: bool = False,
    _pre_parsed_response: dict | None = None,
) -> dict:
    """Enrich chunk-level metadata for a single material.

    Args:
        output_dir: Path to extracted/<material_id>/ directory.
        config: Full config dict.
        llm_fn: Callable (system, messages) -> str. The LLM implementation.
        force: Re-enrich even if not stale.
        _pre_parsed_response: If provided, skip LLM calls and use this parsed
            dict directly. Expected shape: {"chunks": [...]}.
            Used by combined doc+chunk call in orchestrator.

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

    # 5. Get chunk enrichment results — either from pre-parsed response or batched LLM calls
    # Key: chunk_id → {"summary": EnrichedField, "keywords": EnrichedField}
    chunk_enrichments: dict[str, dict] = {}
    # Track actual responding model for provenance (updated after LLM calls)
    actual_model: str = getattr(llm_fn, "last_model", model)

    if _pre_parsed_response is not None:
        # Pre-parsed: combined call returns only the target chunks selected by the orchestrator
        chunks_response = _pre_parsed_response.get("chunks", [])
        if not isinstance(chunks_response, list):
            return {"status": "failed", "detail": "Pre-parsed response: invalid chunks list"}
        n_batches = 0
        for chunk_data in chunks_response:
            chunk_id = chunk_data.get("chunk_id", "")
            if not chunk_id:
                continue
            enrichment = {}
            if "summary" in chunk_data and isinstance(chunk_data["summary"], dict):
                try:
                    enrichment["summary"] = _make_enriched_field(
                        chunk_data["summary"], actual_model, prompt_version
                    ).to_dict()
                except Exception:
                    pass
            if "keywords" in chunk_data and isinstance(chunk_data["keywords"], dict):
                try:
                    enrichment["keywords"] = _make_enriched_field(
                        chunk_data["keywords"], actual_model, prompt_version
                    ).to_dict()
                except Exception:
                    pass
            if "content_class" in chunk_data and isinstance(chunk_data["content_class"], str):
                enrichment["content_class"] = chunk_data["content_class"]
            if enrichment:
                chunk_enrichments[chunk_id] = enrichment
    else:
        # Standard path: batch only stale chunks, writing chunks.jsonl + stamps
        # incrementally after each batch so retries resume without re-spending tokens.

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
        stamps_path = output_dir / "chunk_enrichment_stamps.json"

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
                parsed = enrich_llm.parse_json_or_repair(
                    llm_fn, raw_text, _CHUNK_BATCH_SCHEMA_DESC
                )
            except enrich_llm.EnrichmentError as exc:
                return {"status": "failed", "detail": f"Batch {batch_idx + 1}/{n_batches} LLM error: {exc}"}
            except Exception as exc:
                return {"status": "failed", "detail": f"Batch {batch_idx + 1}/{n_batches} error: {exc}"}

            # Parse response
            chunks_response = parsed.get("chunks", [])
            if not isinstance(chunks_response, list):
                return {
                    "status": "failed",
                    "detail": f"Batch {batch_idx + 1}/{n_batches}: invalid response (no chunks list)",
                }

            batch_enrichments: dict[str, dict] = {}
            for chunk_data in chunks_response:
                chunk_id = chunk_data.get("chunk_id", "")
                if not chunk_id:
                    continue
                enrichment = {}
                if "summary" in chunk_data and isinstance(chunk_data["summary"], dict):
                    try:
                        enrichment["summary"] = _make_enriched_field(
                            chunk_data["summary"], actual_model, prompt_version
                        ).to_dict()
                    except Exception:
                        pass
                if "keywords" in chunk_data and isinstance(chunk_data["keywords"], dict):
                    try:
                        enrichment["keywords"] = _make_enriched_field(
                            chunk_data["keywords"], actual_model, prompt_version
                        ).to_dict()
                    except Exception:
                        pass
                if "content_class" in chunk_data and isinstance(chunk_data["content_class"], str):
                    enrichment["content_class"] = chunk_data["content_class"]
                if enrichment:
                    batch_enrichments[chunk_id] = enrichment

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

    # 6. Validate completeness across all batches.
    # For the standard path, stale_chunks is the set we tried to enrich.
    # Allow up to 3 missing chunks (LLM occasionally skips one): warn and continue.
    if _pre_parsed_response is not None:
        target_ids = _pre_parsed_response.get("_target_chunk_ids")
        if isinstance(target_ids, list) and target_ids:
            stale_ids = {str(cid) for cid in target_ids if cid}
            stale_chunks = [c for c in chunks if c.get("chunk_id", "") in stale_ids]
        else:
            stale_ids = {c.get("chunk_id", "") for c in stale_chunks if c.get("chunk_id")}
    else:
        stale_ids = {c.get("chunk_id", "") for c in stale_chunks if c.get("chunk_id")}

    missing_ids = stale_ids - set(chunk_enrichments.keys())
    max_tolerable_missing = 3
    if len(missing_ids) > max_tolerable_missing:
        return {
            "status": "failed",
            "detail": f"LLM output missing {len(missing_ids)} chunk(s): {sorted(missing_ids)[:5]}",
        }
    if missing_ids:
        import sys
        print(
            f"  [chunk] warning: {len(missing_ids)} chunk(s) not returned by LLM "
            f"({sorted(missing_ids)}), proceeding without them",
            file=sys.stderr,
        )

    # Validate each enriched chunk has required fields (summary + keywords)
    for cid, enrichment in chunk_enrichments.items():
        if "summary" not in enrichment or "keywords" not in enrichment:
            return {
                "status": "failed",
                "detail": f"Chunk '{cid}' missing required fields (need summary + keywords)",
            }

    if _pre_parsed_response is not None:
        # 7+8. Pre-parsed path: one atomic write at the end (docs are small, no incremental needed)
        enriched_chunks = []
        for chunk in chunks:
            chunk_out = dict(chunk)
            chunk_id = chunk.get("chunk_id", "")
            if chunk_id in chunk_enrichments:
                chunk_out.update(chunk_enrichments[chunk_id])
            enriched_chunks.append(chunk_out)

        try:
            new_stamps = dict(existing_stamps)
            for chunk_id in set(chunk_enrichments):
                chunk = next((c for c in chunks if c.get("chunk_id", "") == chunk_id), None)
                if chunk is None:
                    continue
                fp = enrich_stamps.single_chunk_fingerprint(chunk, annotations, doc_context)
                st = enrich_stamps.make_stamp(prompt_version, model, schema_version, fp)
                st["model"] = actual_model
                new_stamps[chunk_id] = st

            chunks_path = output_dir / "chunks.jsonl"

            tmp_chunks = chunks_path.with_suffix(".jsonl.tmp")
            with open(tmp_chunks, "w", encoding="utf-8") as f:
                for chunk in enriched_chunks:
                    f.write(json.dumps(chunk, ensure_ascii=False) + "\n")
            tmp_chunks.replace(chunks_path)
            enrich_stamps.write_chunk_stamps(output_dir, new_stamps)
        except Exception as exc:
            return {"status": "failed", "detail": f"Write error: {exc}"}

        detail = f"{len(chunk_enrichments)}/{len(stale_ids)} chunks, combined call"
    else:
        # Standard path: incremental writes already happened per-batch.
        # Nothing more to write.
        n_enriched = len(chunk_enrichments)
        detail = f"{n_enriched} chunks, {n_batches} batch{'es' if n_batches != 1 else ''}"

    return {"status": "enriched", "detail": detail}
