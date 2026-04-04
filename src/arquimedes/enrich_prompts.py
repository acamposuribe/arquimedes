"""Prompt builders for LLM enrichment stages.

Provides functions to construct system prompts and message lists for the three
enrichment stages:
  - Document-level: summary, document_type, keywords, facets, concepts
  - Combined (document + chunks in one call): same as document but also yields
    per-chunk summaries and keywords
  - Chunk-batch: chunk-level summaries and keywords from a batch of chunks
  - Figure-batch: visual_type, description, caption for a batch of figures

All prompts instruct the LLM to return ONLY valid JSON, no markdown fences.
"""

from __future__ import annotations

import base64
import mimetypes
from pathlib import Path


# ---------------------------------------------------------------------------
# TOC formatter (shared helper)
# ---------------------------------------------------------------------------


def format_toc(toc: list[dict] | None) -> str:
    """Format a TOC list as an indented string.

    Each entry is expected to have 'title', 'page', and 'level' keys.
    Returns 'Not available' if toc is None or empty.
    """
    if not toc:
        return "Not available"
    lines = []
    for entry in toc:
        indent = "  " * entry.get("level", 0)
        title = entry.get("title", "")
        page = entry.get("page", "")
        lines.append(f"{indent}{title} (p. {page})")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Annotation injection
# ---------------------------------------------------------------------------


def inject_annotations(text: str, annotations: list[dict], page_number: int) -> str:
    """Wrap annotated spans in [HIGHLIGHTED]...[/HIGHLIGHTED] markers.

    Only annotations whose 'page' matches *page_number* are applied.
    If the quoted_text is not found in *text*, the annotation is silently skipped.
    If an annotation has a non-empty 'comment', it is appended as [NOTE: comment].
    """
    for ann in annotations:
        if ann.get("page") != page_number:
            continue
        quoted = ann.get("quoted_text", "")
        if not quoted or quoted not in text:
            continue
        comment = ann.get("comment", "")
        if comment:
            replacement = f"[HIGHLIGHTED]{quoted}[/HIGHLIGHTED] [NOTE: {comment}]"
        else:
            replacement = f"[HIGHLIGHTED]{quoted}[/HIGHLIGHTED]"
        # Replace only the first occurrence to avoid double-wrapping overlapping annotations
        text = text.replace(quoted, replacement, 1)
    return text


# ---------------------------------------------------------------------------
# Document context header
# ---------------------------------------------------------------------------


def build_document_context(
    meta: dict,
    toc: list | None,
    headings: list[str] | None,
) -> str:
    """Build a compact context header string for document-level prompts.

    Args:
        meta: dict from meta.json (MaterialMeta fields)
        toc: list of TOC entries (title, page, level) or None
        headings: list of heading strings collected across pages, or None
    """
    title = meta.get("title", "")
    authors = meta.get("authors", [])
    year = meta.get("year", "")
    domain = meta.get("domain", "")
    collection = meta.get("collection", "")
    raw_document_type = meta.get("raw_document_type", "")
    raw_keywords = meta.get("raw_keywords", [])

    authors_str = ", ".join(authors) if authors else ""
    keywords_str = ", ".join(raw_keywords) if raw_keywords else ""
    toc_str = format_toc(toc)

    lines = [
        f"Title: {title}",
        f"Authors: {authors_str}",
        f"Year: {year}",
        f"Domain: {domain}",
        f"Collection: {collection}",
        f"Raw document type: {raw_document_type}",
        f"Raw keywords: {keywords_str}",
    ]

    # Include enriched summary/document_type when available (for chunk/figure context)
    enriched_summary = meta.get("summary")
    if isinstance(enriched_summary, dict) and "value" in enriched_summary:
        lines.append(f"Document summary: {enriched_summary['value']}")
    enriched_doc_type = meta.get("document_type")
    if isinstance(enriched_doc_type, dict) and "value" in enriched_doc_type:
        lines.append(f"Document type: {enriched_doc_type['value']}")

    lines.append("")
    lines.append("Table of Contents:")
    lines.append(toc_str)

    # Include headings if provided and TOC was empty
    if headings and (not toc):
        lines.append("")
        lines.append("Section headings:")
        for h in headings[:50]:  # cap to avoid very long context
            lines.append(f"  - {h}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Document prompt
# ---------------------------------------------------------------------------

_DOCUMENT_SYSTEM_PROMPT = """\
You are an expert architecture librarian. Analyze the provided architecture document and return \
structured JSON with enriched metadata. Return ONLY valid JSON, no markdown fences, no prose.\
"""

_DOCUMENT_SCHEMA = """\
{
  "summary": {"value": "...", "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0},
  "document_type": {"value": "one of: regulation|catalogue|monograph|paper|lecture_note|precedent|technical_spec|site_document", "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0},
  "keywords": {"value": ["term1", ...], "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0},
  "facets": {
    "building_type": {"value": "...", "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0},
    "scale": {"value": "one of: detail|building|urban|territorial", "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0},
    "location": {"value": "...", "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0},
    "jurisdiction": {"value": "...", "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0},
    "climate": {"value": "...", "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0},
    "program": {"value": "...", "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0},
    "material_system": {"value": "...", "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0},
    "structural_system": {"value": "...", "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0},
    "historical_period": {"value": "...", "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0},
    "course_topic": {"value": "...", "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0},
    "studio_project": {"value": "...", "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0}
  },
  "concepts": [
    {"concept_name": "...", "relevance": "...", "source_pages": [...], "evidence_spans": ["..."]}
  ]
}\
"""

_DOCUMENT_USER_TEMPLATE = """\
## Document Metadata

{doc_context}

## Document Text

Sections marked with [HIGHLIGHTED]...[/HIGHLIGHTED] were annotated by the reader and should \
be weighted as priority context for summaries, keywords, and facets. \
[NOTE: ...] markers contain the reader's own comments on the highlighted passage.

{chunks_text}

## Instructions

Analyze this architecture document and return a single JSON object matching the schema below. \
Only include facets where you are confident (omit fields you cannot determine). \
Return ONLY valid JSON, no markdown fences, no explanations.

{schema}\
"""


def estimate_tokens(text: str) -> int:
    """Rough token estimate: ~4 characters per token."""
    return len(text) // 4


def _build_chunks_text(chunks: list[dict], annotations: list[dict]) -> str:
    """Render all chunks with annotation markers injected."""
    parts = []
    for chunk in chunks:
        chunk_id = chunk.get("chunk_id", "")
        source_pages = chunk.get("source_pages", [])
        text = chunk.get("text", "")
        for page_num in source_pages:
            text = inject_annotations(text, annotations, page_num)
        pages_str = ", ".join(str(p) for p in source_pages)
        parts.append(f"--- Chunk {chunk_id} (pages {pages_str}) ---\n{text}\n")
    return "\n".join(parts)


def _curate_context_for_large_doc(
    chunks: list[dict],
    annotations: list[dict],
    toc: list | None,
    max_tokens: int,
) -> str:
    """Build curated context for documents too large to send in full.

    Strategy from spec: TOC/headings + first chunks + last chunks +
    chunks overlapping annotations + top emphasized chunks.
    """
    # Reserve space for TOC and framing
    toc_text = format_toc(toc)
    overhead_tokens = estimate_tokens(toc_text) + 500
    budget = max_tokens - overhead_tokens

    # Collect annotated page numbers
    annotated_pages: set[int] = set()
    for ann in annotations:
        p = ann.get("page")
        if isinstance(p, int):
            annotated_pages.add(p)

    # Score chunks: position priority + annotation overlap + emphasis
    n = len(chunks) if chunks else 1
    scored: list[tuple[float, int, dict]] = []
    for idx, chunk in enumerate(chunks):
        score = 0.0
        # First 10% and last 10% of chunks get priority
        if idx < n * 0.1 or idx >= n * 0.9:
            score += 2.0
        pages = set(chunk.get("source_pages", []))
        if pages & annotated_pages:
            score += 3.0
        if chunk.get("emphasized"):
            score += 1.0
        scored.append((score, idx, chunk))

    # Sort by score desc, then by original position
    scored.sort(key=lambda t: (-t[0], t[1]))

    # Greedily select chunks within budget
    selected_indices: list[int] = []
    used_tokens = 0
    for _score, idx, chunk in scored:
        text = chunk.get("text", "")
        tok = estimate_tokens(text)
        if used_tokens + tok > budget:
            continue
        selected_indices.append(idx)
        used_tokens += tok

    # Render in original order
    selected_indices.sort()
    parts = [f"[Curated context: {len(selected_indices)} of {len(chunks)} chunks selected]\n"]
    for idx in selected_indices:
        chunk = chunks[idx]
        chunk_id = chunk.get("chunk_id", "")
        source_pages = chunk.get("source_pages", [])
        text = chunk.get("text", "")
        for page_num in source_pages:
            text = inject_annotations(text, annotations, page_num)
        pages_str = ", ".join(str(p) for p in source_pages)
        parts.append(f"--- Chunk {chunk_id} (pages {pages_str}) ---\n{text}\n")
    return "\n".join(parts)


# Default context token limit for document prompts (characters / 4)
_DEFAULT_MAX_CONTEXT_TOKENS = 80_000


def build_document_prompt(
    meta: dict,
    toc: list | None,
    chunks: list[dict],
    annotations: list[dict],
    *,
    max_context_tokens: int = _DEFAULT_MAX_CONTEXT_TOKENS,
) -> tuple[str, list[dict]]:
    """Build (system_prompt, messages) for document-level LLM enrichment.

    For small/moderate materials, sends all chunk text. For large materials
    where full chunk text exceeds max_context_tokens, uses curated context
    (first/last chunks, annotated chunks, emphasized chunks).
    """
    doc_context = build_document_context(meta, toc, None)
    full_chunks_text = _build_chunks_text(chunks, annotations)

    # Check if full text fits within token budget
    total_tokens = estimate_tokens(doc_context + full_chunks_text + _DOCUMENT_SCHEMA)
    if total_tokens <= max_context_tokens:
        chunks_text = full_chunks_text
    else:
        chunks_text = _curate_context_for_large_doc(
            chunks, annotations, toc, max_context_tokens
        )

    user_content = _DOCUMENT_USER_TEMPLATE.format(
        doc_context=doc_context,
        chunks_text=chunks_text,
        schema=_DOCUMENT_SCHEMA,
    )

    messages = [{"role": "user", "content": user_content}]
    return _DOCUMENT_SYSTEM_PROMPT, messages


# ---------------------------------------------------------------------------
# Combined prompt (document + chunks in a single call)
# ---------------------------------------------------------------------------

_COMBINED_SCHEMA = """\
{
  "document": {
    "summary": {"value": "...", "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0},
    "document_type": {"value": "one of: regulation|catalogue|monograph|paper|lecture_note|precedent|technical_spec|site_document", "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0},
    "keywords": {"value": ["term1", ...], "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0},
    "facets": {
      "building_type": {"value": "...", "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0},
      "scale": {"value": "one of: detail|building|urban|territorial", "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0},
      "location": {"value": "...", "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0},
      "jurisdiction": {"value": "...", "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0},
      "climate": {"value": "...", "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0},
      "program": {"value": "...", "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0},
      "material_system": {"value": "...", "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0},
      "structural_system": {"value": "...", "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0},
      "historical_period": {"value": "...", "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0},
      "course_topic": {"value": "...", "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0},
      "studio_project": {"value": "...", "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0}
    },
    "concepts": [
      {"concept_name": "...", "relevance": "...", "source_pages": [...], "evidence_spans": ["..."]}
    ]
  },
  "chunks": [
    {
      "chunk_id": "...",
      "summary": {"value": "...", "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0},
      "keywords": {"value": ["..."], "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0}
    }
  ]
}\
"""

_COMBINED_USER_TEMPLATE = """\
## Document Metadata

{doc_context}

## Document Text

Sections marked with [HIGHLIGHTED]...[/HIGHLIGHTED] were annotated by the reader and should \
be weighted as priority context. [NOTE: ...] markers contain the reader's own comments.

{chunks_text}

## Instructions

Analyze this architecture document and return a single JSON object with two top-level keys: \
"document" and "chunks".

For "document": provide summary, document_type, keywords, facets (only where confident — omit \
fields you cannot determine), and concepts.

For "chunks": provide a list entry for each chunk_id listed above with a one-line summary and \
keywords specific to that chunk's content.

Return ONLY valid JSON, no markdown fences, no explanations.

{schema}\
"""


def build_combined_prompt(
    meta: dict,
    toc: list | None,
    chunks: list[dict],
    annotations: list[dict],
) -> tuple[str, list[dict]]:
    """Build (system_prompt, messages) for combined document + chunk enrichment.

    The LLM returns both document-level metadata and per-chunk summaries/keywords
    in a single response.
    """
    doc_context = build_document_context(meta, toc, None)
    chunks_text = _build_chunks_text(chunks, annotations)

    user_content = _COMBINED_USER_TEMPLATE.format(
        doc_context=doc_context,
        chunks_text=chunks_text,
        schema=_COMBINED_SCHEMA,
    )

    messages = [{"role": "user", "content": user_content}]
    return _DOCUMENT_SYSTEM_PROMPT, messages


# ---------------------------------------------------------------------------
# Chunk batch prompt
# ---------------------------------------------------------------------------

_CHUNK_BATCH_SYSTEM_PROMPT = """\
You are analyzing text chunks from an architecture document. \
Return structured JSON with per-chunk metadata. \
Return ONLY valid JSON, no markdown fences, no prose.\
"""

_CHUNK_BATCH_SCHEMA = """\
{
  "chunks": [
    {
      "chunk_id": "...",
      "summary": {"value": "...", "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0},
      "keywords": {"value": ["..."], "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0}
    }
  ]
}\
"""

_CHUNK_BATCH_USER_TEMPLATE = """\
## Document Context

{doc_context_str}

## Chunks

{chunks_text}

## Instructions

For each chunk above, provide a concise one-line summary and a list of architecture-relevant keywords. \
Return ONLY valid JSON matching the schema below. No markdown fences, no explanations.

{schema}\
"""


def build_chunk_batch_prompt(
    chunk_batch: list[dict],
    doc_context_str: str,
    annotations: list[dict],
) -> tuple[str, list[dict]]:
    """Build (system_prompt, messages) for a chunk-batch enrichment call.

    Args:
        chunk_batch: list of chunk dicts (chunk_id, text, source_pages, emphasized)
        doc_context_str: pre-built document context string (from build_document_context)
        annotations: list of annotation dicts for injecting highlights
    """
    chunks_text = _build_chunks_text(chunk_batch, annotations)

    user_content = _CHUNK_BATCH_USER_TEMPLATE.format(
        doc_context_str=doc_context_str,
        chunks_text=chunks_text,
        schema=_CHUNK_BATCH_SCHEMA,
    )

    messages = [{"role": "user", "content": user_content}]
    return _CHUNK_BATCH_SYSTEM_PROMPT, messages


# ---------------------------------------------------------------------------
# Figure batch prompt
# ---------------------------------------------------------------------------

_FIGURE_BATCH_SYSTEM_PROMPT = """\
You are analyzing figures from an architecture document. \
Return structured JSON with per-figure metadata. \
Return ONLY valid JSON, no markdown fences, no prose.\
"""

_FIGURE_BATCH_SCHEMA = """\
{
  "figures": [
    {
      "figure_id": "...",
      "visual_type": {"value": "one of: plan|section|elevation|detail|photo|diagram|chart|render|sketch", "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0},
      "description": {"value": "...", "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0},
      "caption": {"value": "...", "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0}
    }
  ]
}\
"""

_FIGURE_BATCH_USER_INTRO = """\
## Document Context

{doc_context_str}

## Figures

For each figure below, identify its visual_type (plan, section, elevation, detail, photo, \
diagram, chart, render, or sketch), write a concise description, and extract or infer a caption.

"""

_FIGURE_BATCH_USER_OUTRO = """\

## Instructions

Return ONLY valid JSON matching the schema below. No markdown fences, no explanations.

{schema}\
"""


def _encode_image(image_path: str) -> tuple[str, str]:
    """Base64-encode an image file and detect its media type.

    Returns (media_type, b64_data).
    """
    path = Path(image_path)
    media_type, _ = mimetypes.guess_type(str(path))
    if media_type is None:
        # Fallback: treat unknown as JPEG
        media_type = "image/jpeg"
    with open(path, "rb") as f:
        data = base64.b64encode(f.read()).decode("utf-8")
    return media_type, data


def build_figure_batch_prompt(
    figures_with_context: list[dict],
    doc_context_str: str,
) -> tuple[str, list[dict]]:
    """Build (system_prompt, messages) for a figure-batch enrichment call.

    Each dict in figures_with_context must have:
      - figure_id: str
      - image_path: str | None  (None or empty = no image available)
      - source_page_text: str
      - caption_candidates: list[str]
      - sidecar: dict (raw figure metadata)

    For figures WITH an image_path pointing to an existing file, the image is
    included as a base64 content block. For figures without images, only text
    context is provided and the LLM is notified.
    """
    intro = _FIGURE_BATCH_USER_INTRO.format(doc_context_str=doc_context_str)
    outro = _FIGURE_BATCH_USER_OUTRO.format(schema=_FIGURE_BATCH_SCHEMA)

    # Build the content list for the user message (multimodal)
    content: list[dict] = []
    content.append({"type": "text", "text": intro})

    for fig in figures_with_context:
        figure_id = fig.get("figure_id", "")
        image_path = fig.get("image_path") or ""
        source_page_text = fig.get("source_page_text", "")
        caption_candidates = fig.get("caption_candidates", [])
        sidecar = fig.get("sidecar", {})
        source_page = sidecar.get("source_page", "")

        captions_str = (
            "\n".join(f"  - {c}" for c in caption_candidates)
            if caption_candidates
            else "  (none found)"
        )

        has_image = bool(image_path) and Path(image_path).exists()

        # Text header for this figure
        header = (
            f"### Figure: {figure_id}\n"
            f"Source page: {source_page}\n"
            f"Caption candidates:\n{captions_str}\n"
            f"Surrounding page text excerpt:\n{source_page_text}\n"
        )
        if not has_image:
            header += "(Image unavailable — classify from text context only)\n"

        content.append({"type": "text", "text": header})

        if has_image:
            try:
                media_type, b64_data = _encode_image(image_path)
                content.append(
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": media_type,
                            "data": b64_data,
                        },
                    }
                )
            except (OSError, ValueError):
                # If encoding fails, fall back to text-only
                content.append(
                    {"type": "text", "text": "(Image could not be read — text-only fallback)\n"}
                )

    content.append({"type": "text", "text": outro})

    messages = [{"role": "user", "content": content}]
    return _FIGURE_BATCH_SYSTEM_PROMPT, messages
