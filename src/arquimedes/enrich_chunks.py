"""Chunk enrichment stage — Phase 3.

Enriches per-chunk metadata: summary and keywords for each text chunk.
Uses batched LLM calls. All-or-nothing: if any batch fails, no writes occur.
"""

from __future__ import annotations

import json
from pathlib import Path

from arquimedes import enrich_llm, enrich_prompts, enrich_stamps
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


# ---------------------------------------------------------------------------
# Schema description for repair fallback
# ---------------------------------------------------------------------------

_CHUNK_BATCH_SCHEMA_DESC = """\
{
  "chunks": [
    {
      "chunk_id": "...",
      "summary": {"value": "...", "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0},
      "keywords": {"value": ["..."], "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0}
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
    model: str = config.get("llm", {}).get("model", "claude-sonnet-4-6")
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
    any_stale = False
    for chunk in chunks:
        chunk_id = chunk.get("chunk_id", "")
        fp = enrich_stamps.single_chunk_fingerprint(chunk, annotations, doc_context)
        current_stamp = enrich_stamps.make_stamp(prompt_version, model, schema_version, fp)
        existing = existing_stamps.get(chunk_id)
        if force or enrich_stamps.is_stale(existing, current_stamp):
            any_stale = True
            break

    if not any_stale:
        return {"status": "skipped", "detail": "up to date"}

    # 4. Build doc_context_str for prompts
    doc_context_str = enrich_prompts.build_document_context(meta, toc, headings if headings else None)

    # 5. Get chunk enrichment results — either from pre-parsed response or batched LLM calls
    # Key: chunk_id → {"summary": EnrichedField, "keywords": EnrichedField}
    chunk_enrichments: dict[str, dict] = {}

    if _pre_parsed_response is not None:
        # Pre-parsed: all chunk results already available (from combined call)
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
                        chunk_data["summary"], model, prompt_version
                    ).to_dict()
                except Exception:
                    pass
            if "keywords" in chunk_data and isinstance(chunk_data["keywords"], dict):
                try:
                    enrichment["keywords"] = _make_enriched_field(
                        chunk_data["keywords"], model, prompt_version
                    ).to_dict()
                except Exception:
                    pass
            if enrichment:
                chunk_enrichments[chunk_id] = enrichment
    else:
        # Standard path: batch by token budget and call LLM
        # chunk_batch_target is the heuristic count; actual batching adjusts by token size
        # Budget: average tokens per batch ≈ (batch_target chunks × avg chunk tokens)
        # We estimate a per-batch token budget from the target count
        avg_chunk_tokens = max(
            sum(estimate_tokens(c.get("text", "")) for c in chunks) // len(chunks), 1
        )
        batch_token_budget = batch_target * avg_chunk_tokens

        batches: list[list[dict]] = []
        current_batch: list[dict] = []
        current_tokens = 0
        for chunk in chunks:
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

        for batch_idx, batch in enumerate(batches):
            try:
                system, messages = enrich_prompts.build_chunk_batch_prompt(
                    batch, doc_context_str, annotations
                )
                raw_text = llm_fn(system, messages)
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

            for chunk_data in chunks_response:
                chunk_id = chunk_data.get("chunk_id", "")
                if not chunk_id:
                    continue
                enrichment = {}
                if "summary" in chunk_data and isinstance(chunk_data["summary"], dict):
                    try:
                        enrichment["summary"] = _make_enriched_field(
                            chunk_data["summary"], model, prompt_version
                        ).to_dict()
                    except Exception:
                        pass
                if "keywords" in chunk_data and isinstance(chunk_data["keywords"], dict):
                    try:
                        enrichment["keywords"] = _make_enriched_field(
                            chunk_data["keywords"], model, prompt_version
                        ).to_dict()
                    except Exception:
                        pass
                if enrichment:
                    chunk_enrichments[chunk_id] = enrichment

    # 6. Validate completeness — every chunk must have been enriched
    input_ids = {chunk.get("chunk_id", "") for chunk in chunks if chunk.get("chunk_id")}
    missing_ids = input_ids - set(chunk_enrichments.keys())
    if missing_ids:
        return {
            "status": "failed",
            "detail": f"LLM output missing {len(missing_ids)} chunk(s): {sorted(missing_ids)[:5]}",
        }

    # Validate each enriched chunk has required fields (summary + keywords)
    for cid, enrichment in chunk_enrichments.items():
        if "summary" not in enrichment or "keywords" not in enrichment:
            return {
                "status": "failed",
                "detail": f"Chunk '{cid}' missing required fields (need summary + keywords)",
            }

    # 7. All batches succeeded — merge enriched fields into chunk dicts
    enriched_chunks = []
    for chunk in chunks:
        chunk_out = dict(chunk)
        chunk_id = chunk.get("chunk_id", "")
        if chunk_id in chunk_enrichments:
            chunk_out.update(chunk_enrichments[chunk_id])
        enriched_chunks.append(chunk_out)

    # 8. Atomic write: stage all files first, then commit with rollback
    try:
        new_stamps = {}
        for chunk in chunks:
            chunk_id = chunk.get("chunk_id", "")
            fp = enrich_stamps.single_chunk_fingerprint(chunk, annotations, doc_context)
            new_stamps[chunk_id] = enrich_stamps.make_stamp(
                prompt_version, model, schema_version, fp
            )

        chunks_path = output_dir / "chunks.jsonl"
        stamps_path = output_dir / "chunk_enrichment_stamps.json"

        # Stage: write to temp files
        tmp_chunks = chunks_path.with_suffix(".jsonl.tmp")
        tmp_stamps = stamps_path.with_suffix(".json.tmp")

        with open(tmp_chunks, "w", encoding="utf-8") as f:
            for chunk in enriched_chunks:
                f.write(json.dumps(chunk, ensure_ascii=False) + "\n")

        tmp_stamps.write_text(
            json.dumps(new_stamps, indent=2, ensure_ascii=False), encoding="utf-8"
        )

        # Backup originals for rollback
        bak_chunks = chunks_path.with_suffix(".jsonl.bak")
        bak_stamps = stamps_path.with_suffix(".json.bak")
        if chunks_path.exists():
            chunks_path.replace(bak_chunks)
        if stamps_path.exists():
            stamps_path.replace(bak_stamps)

        # Commit: rename all temps to final
        committed: list[tuple[Path, Path]] = []
        try:
            tmp_chunks.replace(chunks_path)
            committed.append((chunks_path, bak_chunks))
            tmp_stamps.replace(stamps_path)
            committed.append((stamps_path, bak_stamps))
        except Exception:
            for final_path, backup_path in committed:
                try:
                    if backup_path.exists():
                        backup_path.replace(final_path)
                except Exception:
                    pass
            for tmp in (tmp_chunks, tmp_stamps):
                try:
                    tmp.unlink(missing_ok=True)
                except Exception:
                    pass
            raise

        # Clean up backups on success
        for bak in (bak_chunks, bak_stamps):
            try:
                bak.unlink(missing_ok=True)
            except Exception:
                pass
    except Exception as exc:
        return {"status": "failed", "detail": f"Write error: {exc}"}

    if _pre_parsed_response is not None:
        detail = f"{len(chunks)} chunks, combined call"
    else:
        detail = f"{len(chunks)} chunks, {n_batches} batch{'es' if n_batches != 1 else ''}"
    return {"status": "enriched", "detail": detail}
