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
import json
import mimetypes
from pathlib import Path

from arquimedes.domain_profiles import is_practice_domain
from arquimedes import practice_prompts


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

    bridge_concepts = meta.get("bridge_concepts") or []
    if isinstance(bridge_concepts, list) and bridge_concepts:
        names = []
        for item in bridge_concepts[:12]:
            if isinstance(item, dict):
                name = item.get("canonical_name") or item.get("slug") or ""
            else:
                name = str(item)
            if name:
                names.append(name)
        if names:
            lines.append(f"Bridge concepts: {', '.join(names)}")

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


def build_figure_context(meta: dict) -> str:
    """Build the minimal document context string used for figure prompts."""
    title = meta.get("title", "")
    authors = meta.get("authors", [])
    year = meta.get("year", "")
    domain = meta.get("domain", "")
    collection = meta.get("collection", "")

    authors_str = ", ".join(authors) if authors else ""
    lines = [
        f"Title: {title}",
        f"Authors: {authors_str}",
        f"Year: {year}",
        f"Domain: {domain}",
        f"Collection: {collection}",
    ]

    enriched_summary = meta.get("summary")
    if isinstance(enriched_summary, dict) and "value" in enriched_summary:
        lines.append(f"Document summary: {enriched_summary['value']}")

    return "\n".join(lines)



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




# ---------------------------------------------------------------------------
# Document enrichment input preparation
# ---------------------------------------------------------------------------

_DOCUMENT_FILE_SYSTEM_PROMPT = """\
You are an expert architecture librarian enriching structured metadata for a document in a research knowledge base.

You will read:
1. the raw metadata JSON object
2. the document text

Your job is to produce one complete JSON output object for document enrichment.

Rules:
- Output the JSON object directly. No introduction, no explanation, no markdown fence.
- Do not output a partial patch.
- Do not omit required top-level enrichment fields.
- Every top-level key shown in the output schema is required.
- "Optional" means the value may be null, {}, or [] where instructed; it does not mean the key may be omitted.
- "_finished": true is mandatory. The output is invalid without it.
- If a required list has no supported entries, output an empty list.
- If bibliography has no supported entries, output null.
- Be conservative. Prefer omission over guessing.
- Do not output "_enrichment_stamp"; the pipeline writes stamps itself.
- Use valid JSON only, with double quotes and no trailing comments.

Output schema:
{
  "summary": "... required ...",
  "document_type": "... required ...",
  "keywords": ["..."],
  "methodological_conclusions": ["..."],
  "main_content_learnings": ["..."],
  "bibliography": {...} or null,
  "facets": {...},
  "concepts_local": [...],
  "concepts_bridge_candidates": [...],
  "toc": [...] or [],
  "_finished": true
}


Field instructions:

summary:
- A dense but readable synthesis of the document's distinctive contribution. Do not merely restate the topic. Name the central argument, method, archive/project/case focus when important, and what the document helps the reader understand that is not obvious from the title alone. Prefer intellectual specificity and nuance over a bland generic abstract. Ideally between 150-300 words, but quality over length. If the document is very short, a concise summary is fine. If the document is very long, capture the core contributions and what makes them distinctive.

document_type:
- One of: regulation|catalogue|monograph|paper|lecture_note|precedent|technical_spec|site_document

keywords:
- 6-10 terms or short phrases that maximize retrieval value.
- Prefer a mix of named actors, places, archives, projects, methods, institutional conditions, and core concepts when they are central. Avoid generic filler and avoid repeating the broadest document theme in multiple synonymous forms. 
- When a distinctive in-text phrase is central, preserve it instead of replacing it with a broader paraphrase.

methodological_conclusions:
- Max 5 short (50-70 words each), reusable statements about how the document says methods should be used, why they matter, and what methodological stance or procedure it contributes. Keep them concrete and archival/architectural rather than generic. Avoid redundancy.
- Keep these methodological.

main_content_learnings:
- Max 5 short (50-70 words each), reusable statements about what the document contributes to architectural knowledge. Focus on the main claims, conceptual contributions, or historically useful learnings that another reader could reuse across materials.
- Preserve the document's sharpest named concepts and formulations when they are central to the claim.

bibliography:
- Only set subfields explicitly supported by the document.
- Allowed keys: journal_name, volume, issue, start_page, end_page, doi, book_title, editors, publisher, place, edition

facets:
- Infer only concrete indexing values strongly grounded in the document.
- Allowed keys:
  building_type, scale, location, climate, program, material_system, historical_period, course_topic
- scale must be one of detail|building|urban|territorial

concepts_local:
- Return 6-10 strong material-level concept candidates
- Each must be a reusable intellectual unit with strong textual evidence. Prefer concept phrases specific enough to carry real analytical content but still reusable across materials. Include named mechanisms, typologies, institutional logics, methods, conditions, and frameworks. Concepts may be theoretically dense and multi-word. Avoid near-duplicate concepts, incidental topics, and generic labels like "history", "power", "space", or "memory" unless sharply qualified.
- Prefer compound noun phrases that carry analytical charge — patterns like "archivability as selective gatekeeping", "the city interpreted as a big house", "wooden structures for carbon-neutral construction", "chronophagy" are good, but pick what's right for the document. Avoid bare single nouns. Proper nouns (people, places, institutions) may be capitalized only when central to the concept name.
- Avoid academic jargon, theoretical buzzwords, or pretentious language. Use clear, direct, and specific language that conveys real analytical meaning.
- Each item:
  {concept_name, descriptor, relevance, source_pages, evidence_spans}
- relevance: high|medium|low
- concept_name should be lowercase and specific, prefer compound noun phrases that carry analytical charge.
- source_pages: max 3 pages per concept.
- evidence_spans: 1-3 short quotes, each 1-5 words.
- Only include concepts with strong textual support.

concepts_bridge_candidates:
- Return 4-5 items max.
- Same schema as concepts_local.
- Favor larger frameworks, problematics, fields of inquiry, spatial or institutional conditions, and reusable analytic umbrellas that could connect this material to related materials. Avoid vague one-word abstractions, chapter themes, or trivial paraphrases of the title.
- Do not flatten a sharp source-grounded concept into a generic bridge label if the sharper phrase can still function across materials.

toc:
- Only include if the current work metadata has toc = null and the document text contains a recoverable table of contents or stable section headings.
- Each entry: {"title": "...", "level": 0|1|2, "page": N}

Reader annotations:
- Treat [HIGHLIGHTED]...[/HIGHLIGHTED] as priority evidence.
- Treat [NOTE: ...] as reader comments, not document claims.\
"""

_DOCUMENT_FILE_USER_TEMPLATE = """\
Read these inputs:
- METADATA JSON: {meta_path}
- DOCUMENT TEXT: {document_text_path}

Produce one complete JSON output object following the system instructions.
Important:
- Do not rewrite the full metadata object.
- Do not describe your reasoning.
- Do not wrap the JSON in markdown fences.
- Include every required top-level field, including "_finished": true.
- Only output valid JSON matching the required schema.\
"""


def build_document_input_files(
    output_dir: Path,
    chunks: list[dict],
    annotations: list[dict],
) -> tuple[Path, Path]:
    """Prepare document enrichment inputs.

    Returns the original meta.json path and a temporary flattened markdown file
    derived from text.md when available, falling back to chunk text otherwise.
    """
    meta_path = output_dir / "meta.json"
    source_text_path = output_dir / "text.md"

    if source_text_path.exists():
        raw_text = source_text_path.read_text(encoding="utf-8")
    else:
        parts = []
        for chunk in chunks:
            text = chunk.get("text", "")
            for page_num in chunk.get("source_pages", []):
                text = inject_annotations(text, annotations, page_num)
            parts.append(text)
        raw_text = "\n\n".join(parts)

    flattened_text = " ".join(raw_text.split())
    document_text_path = output_dir / "document.work.md"
    document_text_path.write_text(flattened_text, encoding="utf-8")

    return meta_path, document_text_path


def build_document_file_prompt(
    meta_path: Path,
    document_text_path: Path,
    *,
    domain: str = "",
) -> tuple[str, list[dict]]:
    """Build (system_prompt, messages) for file-based document enrichment.

    The LLM reads the metadata JSON and flattened document text directly from
    disk and returns a structured JSON patch.
    """
    user_content = _DOCUMENT_FILE_USER_TEMPLATE.format(
        meta_path=meta_path,
        document_text_path=document_text_path,
    )
    system = (
        practice_prompts.document_file_system_prompt()
        if is_practice_domain(domain)
        else _DOCUMENT_FILE_SYSTEM_PROMPT
    )
    return system, [{"role": "user", "content": user_content}]


# ---------------------------------------------------------------------------
# Metadata-fix prompt
# ---------------------------------------------------------------------------

_METADATA_FIX_SYSTEM_PROMPT = """\
You are fixing bibliographic metadata for a document using the first page thumbnails.

Return only valid JSON:
{
  "title": "...",
  "authors": ["..."],
  "year": "...",
  "_finished": true
}

Rules:
- Prioritize what is visible on the page thumbnails.
- Correct obvious placeholder or extraction-noise metadata.
- Preserve the current value when the thumbnails do not support a confident correction.
- "authors" must be an array of author names in display order.
- "year" should be a 4-digit year when clearly visible; otherwise keep the current value.
- Do not invent missing metadata.
- Output JSON only.
"""


def build_metadata_fix_prompt(
    meta: dict,
    pages: list[dict],
    output_dir: Path,
    *,
    max_images: int = 4,
) -> tuple[str, list[dict]]:
    """Build a multimodal prompt for title/authors/year correction."""
    selected_pages = sorted(
        [page for page in pages if isinstance(page, dict)],
        key=lambda page: int(page.get("page_number", 0) or 0),
    )[:max_images]

    current_meta = {
        "title": meta.get("title", ""),
        "authors": meta.get("authors", []),
        "year": meta.get("year", ""),
        "page_count": meta.get("page_count", 0),
    }
    intro = (
        "Current metadata:\n"
        f"{json.dumps(current_meta, ensure_ascii=False, indent=2)}\n\n"
        "Inspect the first page thumbnails and return corrected title, authors, and year.\n"
        "If a field is unclear, keep the current value."
    )

    content: list[dict] = [{"type": "text", "text": intro}]
    for page in selected_pages:
        page_number = page.get("page_number", "")
        excerpt = " ".join(str(page.get("text", "")).split())[:500]
        thumbnail_rel = str(page.get("thumbnail_path", "") or "")
        content.append({
            "type": "text",
            "text": f"\nPage {page_number}\nOCR excerpt: {excerpt or '(none)'}\n",
        })

        if thumbnail_rel:
            image_path = output_dir / thumbnail_rel
            if image_path.exists():
                media_type, b64_data = _encode_image(str(image_path))
                content.append({
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": media_type,
                        "data": b64_data,
                    },
                    "_source_path": str(image_path),
                })
                continue

        content.append({"type": "text", "text": "(Thumbnail unavailable)\n"})

    return _METADATA_FIX_SYSTEM_PROMPT, [{"role": "user", "content": content}]




# ---------------------------------------------------------------------------
# Chunk batch prompt
# ---------------------------------------------------------------------------

_CHUNK_BATCH_SYSTEM_PROMPT = """\
You are an architecture research librarian analyzing text chunks from an architecture document.
For each chunk, output one JSON object per line (JSONL). No wrapper, no markdown fences, no prose.
Format: {"id":"chk_XXXXX","cls":"...","kw":["term1","term2","term3"],"s":"one-line summary"}\
"""

_CHUNK_BATCH_USER_TEMPLATE = """\
## Document Context

{doc_context_str}

## Chunks

{chunks_text}

## Instructions

For each chunk, output exactly one line: {{"id":"<chunk_id>","cls":"<content_class>","kw":["term1","term2","term3"],"s":"<summary>"}}

Rules:
- "s": two-line summary in English explaining it's main claim, contribution, method, or proposal. When it centers on a specific person, archive, project, place, or event, keep that focus visible — do not flatten into abstract theory. Do not start with "This chunk..." or similar. Just the summary.
- "kw": exactly 3 architecture-relevant keywords. Prefer a mix of concrete entities, mechanisms, and named concepts central to this chunk. Preserve named actors, places, buildings, or projects when they are central. Avoid generic repeats from the overall document context unless they are truly central here.
- "cls": choose the most specific class:
  - "front_matter": title pages, abstracts, acknowledgments, author bios, journal metadata
  - "bibliography": references, citations, endnotes, works cited
  - "caption": figure or table captions
  - "appendix": supplementary material outside the main argumentative flow
  - "methodology": research methods, analytical frameworks, how the archive/research is approached
  - "case_study": a specific person, project, building, archive episode, or concrete example is the main focus
  - "argument": substantive analysis or theory only when no more specific class fits
  Prefer the most specific valid class. Do not default to "argument" when the chunk is mainly a case, method, bibliography, or front matter. If a chunk is interpretive but centered on a specific person, archive, project, or event, still prefer "case_study" — you can synthesize it, but classify it there.

Output one line per chunk, nothing else. Language must be English\
"""


def build_chunk_batch_prompt(
    chunk_batch: list[dict],
    doc_context_str: str,
    annotations: list[dict],
    *,
    domain: str = "",
) -> tuple[str, list[dict]]:
    """Build (system_prompt, messages) for a chunk-batch enrichment call.

    Args:
        chunk_batch: list of chunk dicts (chunk_id, text, source_pages, emphasized)
        doc_context_str: pre-built document context string (from build_document_context)
        annotations: list of annotation dicts for injecting highlights
    """
    chunks_text = _build_chunks_text(chunk_batch, annotations)

    system = (
        practice_prompts.chunk_batch_system_prompt()
        if is_practice_domain(domain)
        else _CHUNK_BATCH_SYSTEM_PROMPT
    )
    user_template = (
        practice_prompts.chunk_batch_user_template()
        if is_practice_domain(domain)
        else _CHUNK_BATCH_USER_TEMPLATE
    )
    user_content = user_template.format(
        doc_context_str=doc_context_str,
        chunks_text=chunks_text,
    )

    messages = [{"role": "user", "content": user_content}]
    return system, messages


# ---------------------------------------------------------------------------
# Figure batch prompt
# ---------------------------------------------------------------------------

_FIGURE_BATCH_SYSTEM_PROMPT = """\
You are an architecture research librarian analyzing figures from an architecture document.
For each figure, output one JSON object per line (JSONL). No wrapper, no markdown fences, no prose.
Format: {"id":"fig_NNN","vt":"...","rel":"...","desc":"...","cap":"..."}\
"""

_FIGURE_BATCH_USER_INTRO = """\
## Document Context

{doc_context_str}

## Figures

For each figure, output exactly one line:
{{"id":"<figure_id>","vt":"<visual_type>","rel":"<relevance>","desc":"<description>","cap":"<caption>"}}

Field rules:
- "vt": one of: plan|section|elevation|detail|photo|diagram|chart|render|sketch
- "rel": one of: substantive|decorative|front_matter
  - "substantive": architectural drawings, photos, diagrams, or other visual knowledge
  - "decorative": logos, publisher marks, decorative borders, page ornaments, empty scans, scanner artifacts
- "desc": concise description of what is visually present. Do not invent architectural content for non-informative images.
  If the figure is blank, partial, scanner-generated, heavily degraded, or contains no meaningful visual knowledge beyond logos, borders, watermarks, or platform artifacts, say so plainly and set rel to "decorative".
- "cap": extracted or inferred caption, or "" if none

When an image is available, prioritize what is visibly present. Use caption candidates and surrounding page text as supporting context only. Fall back to text-only inference only when the image is unavailable or unreadable.

Output one line per figure, nothing else.

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
    *,
    domain: str = "",
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
    system = (
        practice_prompts.figure_batch_system_prompt()
        if is_practice_domain(domain)
        else _FIGURE_BATCH_SYSTEM_PROMPT
    )
    intro_template = (
        practice_prompts.figure_batch_user_intro()
        if is_practice_domain(domain)
        else _FIGURE_BATCH_USER_INTRO
    )
    intro = intro_template.format(doc_context_str=doc_context_str)

    # Build the content list for the user message (multimodal)
    content: list[dict] = [{"type": "text", "text": intro}]

    for fig in figures_with_context:
        figure_id = fig.get("figure_id", "")
        image_path = fig.get("image_path") or ""
        source_page_text = fig.get("source_page_text", "")
        caption_candidates = fig.get("caption_candidates", [])
        artifact_hint = fig.get("artifact_hint", "")
        sidecar = fig.get("sidecar", {})
        source_page = sidecar.get("source_page", "")
        bbox = sidecar.get("bbox", [])
        extraction_method = sidecar.get("extraction_method", "")

        captions_str = (
            "\n".join(f"  - {c}" for c in caption_candidates)
            if caption_candidates
            else "  (none found)"
        )

        has_image = bool(image_path) and Path(image_path).exists()

        header = (
            f"### Figure: {figure_id}\n"
            f"Source page: {source_page}\n"
            f"Extraction method: {extraction_method}\n"
            f"Bounding box: {bbox}\n"
            f"Caption candidates:\n{captions_str}\n"
            f"Surrounding page text excerpt:\n{source_page_text}\n"
        )
        if artifact_hint:
            header += f"Artifact hint: {artifact_hint}\n"
        if not has_image:
            header += "(Image unavailable — classify from text context only)\n"

        content.append({"type": "text", "text": header})

        if has_image:
            try:
                media_type, b64_data = _encode_image(image_path)
                content.append({
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": media_type,
                        "data": b64_data,
                    },
                    "_source_path": str(image_path),
                })
            except (OSError, ValueError):
                content.append(
                    {"type": "text", "text": "(Image could not be read — text-only fallback)\n"}
                )

    messages = [{"role": "user", "content": content}]
    return system, messages
