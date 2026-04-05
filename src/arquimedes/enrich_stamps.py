"""Fingerprinting and staleness tracking for LLM enrichment stamps.

Every enriched artifact (document, chunk, figure) carries a stamp:
    {
        "prompt_version": str,
        "model": str,
        "enrichment_schema_version": str,
        "input_fingerprint": str,
    }

The stamp lets us cheaply decide whether re-enrichment is needed: if any field
differs from what we would produce today, the artifact is stale.

Fingerprints are deterministic: same inputs → same 16-char hex digest always.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path


# ---------------------------------------------------------------------------
# Core hashing primitives
# ---------------------------------------------------------------------------


def canonical_hash(*parts) -> str:
    """SHA-256 over all parts serialized as canonical JSON.

    Each part is serialized with sorted keys and no extra whitespace
    (``separators=(",",":")``).  All serialized parts are fed into a single
    SHA-256 digest.  Returns the first 16 hex characters.
    """
    h = hashlib.sha256()
    for part in parts:
        serialized = json.dumps(part, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        h.update(serialized.encode("utf-8"))
    return h.hexdigest()[:16]


def _normalize_doc_context(doc_context: dict) -> dict:
    """Normalize doc context fields that should not cause stamp churn.

    In particular, treat blank and "unknown" raw document type values as the
    same fallback state so metadata cleanup does not keep invalidating stamps.
    """
    normalized = dict(doc_context or {})
    raw_type = str(normalized.get("raw_document_type", "") or "").strip().casefold()
    if not raw_type or raw_type == "unknown":
        normalized["raw_document_type"] = "unknown"
    else:
        normalized["raw_document_type"] = str(normalized.get("raw_document_type", "")).strip()
    return normalized


# ---------------------------------------------------------------------------
# Document-level fingerprint
# ---------------------------------------------------------------------------


def document_fingerprint(output_dir: Path) -> str:
    """Fingerprint for a full document enrichment pass.

    Hashes:
    - Raw-only meta projection (material_id, title, authors, year,
      raw_keywords, raw_document_type, domain, collection, page_count)
    - Full text of pages.jsonl
    - Full text of annotations.jsonl (empty list ``[]`` if absent)
    - Full text of toc.json (empty list ``[]`` if absent)
    - Full text of chunks.jsonl

    All file contents are included verbatim (as strings) so any change to
    extracted artifacts propagates into the fingerprint.
    """
    # 1. Raw meta projection
    meta_path = output_dir / "meta.json"
    raw_meta = json.loads(meta_path.read_text(encoding="utf-8"))
    meta_projection = {
        "material_id": raw_meta.get("material_id", ""),
        "title": raw_meta.get("title", ""),
        "authors": raw_meta.get("authors", []),
        "year": raw_meta.get("year", ""),
        "raw_keywords": raw_meta.get("raw_keywords", []),
        "raw_document_type": _normalize_doc_context(
            {"raw_document_type": raw_meta.get("raw_document_type", "")}
        )["raw_document_type"],
        "domain": raw_meta.get("domain", ""),
        "collection": raw_meta.get("collection", ""),
        "page_count": raw_meta.get("page_count", 0),
    }

    # 2. pages.jsonl (required)
    pages_text = (output_dir / "pages.jsonl").read_text(encoding="utf-8")

    # 3. annotations.jsonl (optional — fall back to empty list repr)
    ann_path = output_dir / "annotations.jsonl"
    annotations_text = ann_path.read_text(encoding="utf-8") if ann_path.exists() else "[]"

    # 4. toc.json (optional — fall back to empty list repr)
    toc_path = output_dir / "toc.json"
    toc_text = toc_path.read_text(encoding="utf-8") if toc_path.exists() else "[]"

    # 5. chunks.jsonl — raw fields only (exclude enriched summary/keywords)
    raw_chunks = []
    for line in (output_dir / "chunks.jsonl").read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        c = json.loads(line)
        raw_chunks.append({
            "chunk_id": c.get("chunk_id", ""),
            "text": c.get("text", ""),
            "source_pages": c.get("source_pages", []),
            "emphasized": c.get("emphasized", False),
        })

    return canonical_hash(meta_projection, pages_text, annotations_text, toc_text, raw_chunks)


# ---------------------------------------------------------------------------
# Chunk-level fingerprint
# ---------------------------------------------------------------------------


def chunk_fingerprint(output_dir: Path, doc_context: dict) -> str:
    """Fingerprint for a chunk enrichment pass (all chunks combined).

    Hashes:
    - All chunk records from chunks.jsonl (text, source_pages, emphasized,
      chunk_id) — raw fields only, enriched fields excluded.
    - Full text of annotations.jsonl (empty list ``[]`` if absent)
    - doc_context dict (caller-built: title, raw_document_type, headings,
      and optionally summary + stamp if present)

    ``doc_context`` is what the caller passes to the LLM prompt; capturing it
    in the fingerprint means a change in document summary or document_type
    triggers re-enrichment of all its chunks.
    """
    # Raw chunk projection
    chunks_path = output_dir / "chunks.jsonl"
    raw_chunks = []
    for line in chunks_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        c = json.loads(line)
        raw_chunks.append(
            {
                "chunk_id": c.get("chunk_id", ""),
                "text": c.get("text", ""),
                "source_pages": c.get("source_pages", []),
                "emphasized": c.get("emphasized", False),
            }
        )

    # annotations.jsonl
    ann_path = output_dir / "annotations.jsonl"
    annotations_text = ann_path.read_text(encoding="utf-8") if ann_path.exists() else "[]"

    return canonical_hash(raw_chunks, annotations_text, _normalize_doc_context(doc_context))


def single_chunk_fingerprint(
    chunk_record: dict,
    annotations: list[dict],
    doc_context: dict,
) -> str:
    """Fingerprint for a single chunk.

    Hashes:
    - The chunk's raw fields (chunk_id, text, source_pages, emphasized)
    - Annotations that overlap with this chunk's pages
    - doc_context digest (title, raw_document_type, headings, summary if present)

    This enables per-chunk staleness detection: if an annotation changes on
    page 5, only chunks covering page 5 become stale.
    """
    raw_chunk = {
        "chunk_id": chunk_record.get("chunk_id", ""),
        "text": chunk_record.get("text", ""),
        "source_pages": chunk_record.get("source_pages", []),
        "emphasized": chunk_record.get("emphasized", False),
    }
    # Filter annotations to pages this chunk covers
    chunk_pages = set(chunk_record.get("source_pages", []))
    relevant_annotations = sorted(
        [a for a in annotations if a.get("page") in chunk_pages],
        key=lambda a: (a.get("page", 0), a.get("quoted_text", "")),
    )
    return canonical_hash(raw_chunk, relevant_annotations, _normalize_doc_context(doc_context))


# ---------------------------------------------------------------------------
# Figure-level fingerprint
# ---------------------------------------------------------------------------


def figure_fingerprint(
    figure_sidecar: dict,
    image_path: Path,
    page_text: str,
    caption_candidates: list[str],
    doc_context: dict,
) -> str:
    """Fingerprint for a figure enrichment pass.

    Hashes:
    - SHA-256 of image file bytes (represented as a hex string so it can be
      JSON-serialized along with the other parts)
    - Raw sidecar fields: source_page, bbox, extraction_method
    - page_text (string)
    - caption_candidates (list of strings)
    - doc_context (title, document_type value if enriched, domain)
    """
    # Image content hash
    image_bytes = image_path.read_bytes()
    image_sha256 = hashlib.sha256(image_bytes).hexdigest()

    # Raw sidecar projection
    raw_sidecar = {
        "source_page": figure_sidecar.get("source_page"),
        "bbox": figure_sidecar.get("bbox", []),
        "extraction_method": figure_sidecar.get("extraction_method", ""),
    }

    return canonical_hash(image_sha256, raw_sidecar, page_text, caption_candidates, doc_context)


# ---------------------------------------------------------------------------
# Stamp construction and staleness
# ---------------------------------------------------------------------------


def make_stamp(
    prompt_version: str,
    model: str,
    schema_version: str,
    fingerprint: str,
) -> dict:
    """Build the 4-field enrichment stamp dict."""
    return {
        "prompt_version": prompt_version,
        "model": model,
        "enrichment_schema_version": schema_version,
        "input_fingerprint": fingerprint,
    }


def is_stale(existing_stamp: dict | None, current_stamp: dict) -> bool:
    """Return True if the artifact needs re-enrichment.

    Stale when existing_stamp is None or the prompt_version, schema_version,
    or input_fingerprint differ.  The ``model`` field is stored for audit
    purposes only — it records which model actually produced the output and
    is NOT compared for staleness (since the responding model may vary due to
    fallback).  To force re-enrichment with a different model, use --force.
    """
    if existing_stamp is None:
        return True
    fields = ("prompt_version", "enrichment_schema_version", "input_fingerprint")
    return any(existing_stamp.get(f) != current_stamp.get(f) for f in fields)


# ---------------------------------------------------------------------------
# Stamp I/O — document
# ---------------------------------------------------------------------------


def read_document_stamp(output_dir: Path) -> dict | None:
    """Read ``_enrichment_stamp`` from meta.json; return None if absent."""
    meta_path = output_dir / "meta.json"
    if not meta_path.exists():
        return None
    data = json.loads(meta_path.read_text(encoding="utf-8"))
    return data.get("_enrichment_stamp") or None


def write_document_stamp(output_dir: Path, stamp: dict) -> None:
    """Merge stamp into meta.json as ``_enrichment_stamp`` key."""
    meta_path = output_dir / "meta.json"
    data = json.loads(meta_path.read_text(encoding="utf-8"))
    data["_enrichment_stamp"] = stamp
    meta_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


# ---------------------------------------------------------------------------
# Stamp I/O — chunks
# ---------------------------------------------------------------------------


def read_chunk_stamps(output_dir: Path) -> dict:
    """Read chunk_enrichment_stamps.json; return {} if absent."""
    stamps_path = output_dir / "chunk_enrichment_stamps.json"
    if not stamps_path.exists():
        return {}
    return json.loads(stamps_path.read_text(encoding="utf-8"))


def write_chunk_stamps(output_dir: Path, stamps: dict) -> None:
    """Write chunk_enrichment_stamps.json."""
    stamps_path = output_dir / "chunk_enrichment_stamps.json"
    stamps_path.write_text(json.dumps(stamps, indent=2, ensure_ascii=False), encoding="utf-8")


# ---------------------------------------------------------------------------
# Stamp I/O — figures
# ---------------------------------------------------------------------------


def read_figure_stamp(figure_json_path: Path) -> dict | None:
    """Read ``_enrichment_stamp`` from figure sidecar; return None if absent."""
    if not figure_json_path.exists():
        return None
    data = json.loads(figure_json_path.read_text(encoding="utf-8"))
    return data.get("_enrichment_stamp") or None


def write_figure_stamp(figure_json_path: Path, stamp: dict) -> None:
    """Merge stamp into figure sidecar as ``_enrichment_stamp`` key."""
    data = json.loads(figure_json_path.read_text(encoding="utf-8"))
    data["_enrichment_stamp"] = stamp
    figure_json_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
