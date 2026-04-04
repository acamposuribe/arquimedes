"""Chunk enrichment stage — Phase 3.

Enriches per-chunk metadata: summary and keywords for each text chunk.
Uses batched LLM calls. All-or-nothing: if any batch fails, no writes occur.
"""

from __future__ import annotations

import json
from pathlib import Path

from arquimedes import enrich_llm, enrich_prompts, enrich_stamps
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
    client,
    *,
    force: bool = False,
) -> dict:
    """Enrich chunk-level metadata for a single material.

    Args:
        output_dir: Path to extracted/<material_id>/ directory.
        config: Full config dict.
        client: anthropic.Anthropic client (or mock).
        force: Re-enrich even if not stale.

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
        annotations = _load_jsonl(output_dir / "annotations.jsonl")
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

    # 3. Compute fingerprint and staleness
    try:
        fingerprint = enrich_stamps.chunk_fingerprint(output_dir, doc_context)
    except Exception as exc:
        return {"status": "failed", "detail": f"Fingerprint error: {exc}"}

    stamp = enrich_stamps.make_stamp(prompt_version, model, schema_version, fingerprint)

    existing_stamps = enrich_stamps.read_chunk_stamps(output_dir)
    # Use a single stamp for the whole chunk stage: check the "_stage" key
    existing_stage_stamp = existing_stamps.get("_stage")
    if not force and not enrich_stamps.is_stale(existing_stage_stamp, stamp):
        return {"status": "skipped", "detail": "up to date"}

    # 4. Build doc_context_str for prompts
    doc_context_str = enrich_prompts.build_document_context(meta, toc, headings if headings else None)

    # 5. Split chunks into batches
    batches = [chunks[i : i + batch_target] for i in range(0, len(chunks), batch_target)]
    n_batches = len(batches)

    # 6. Process each batch, accumulate results in memory
    # Key: chunk_id → {"summary": EnrichedField, "keywords": EnrichedField}
    chunk_enrichments: dict[str, dict] = {}

    for batch_idx, batch in enumerate(batches):
        try:
            system, messages = enrich_prompts.build_chunk_batch_prompt(
                batch, doc_context_str, annotations
            )
            raw_text = enrich_llm.call_llm(client, model, system, messages)
            parsed = enrich_llm.parse_json_or_repair(
                client, model, raw_text, _CHUNK_BATCH_SCHEMA_DESC
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

    # 7. All batches succeeded — merge enriched fields into chunk dicts
    enriched_chunks = []
    for chunk in chunks:
        chunk_out = dict(chunk)
        chunk_id = chunk.get("chunk_id", "")
        if chunk_id in chunk_enrichments:
            chunk_out.update(chunk_enrichments[chunk_id])
        enriched_chunks.append(chunk_out)

    # 8. Atomic write of chunks.jsonl
    try:
        chunks_path = output_dir / "chunks.jsonl"
        tmp_path = chunks_path.with_suffix(".jsonl.tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            for chunk in enriched_chunks:
                f.write(json.dumps(chunk, ensure_ascii=False) + "\n")
        tmp_path.replace(chunks_path)
    except Exception as exc:
        return {"status": "failed", "detail": f"Write chunks error: {exc}"}

    # 9. Write chunk_enrichment_stamps.json
    try:
        new_stamps = {"_stage": stamp}
        enrich_stamps.write_chunk_stamps(output_dir, new_stamps)
    except Exception as exc:
        return {"status": "failed", "detail": f"Write stamps error: {exc}"}

    detail = f"{len(chunks)} chunks, {n_batches} batch{'es' if n_batches != 1 else ''}"
    return {"status": "enriched", "detail": detail}
