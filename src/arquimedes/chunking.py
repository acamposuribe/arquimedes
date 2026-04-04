"""Split extracted text into retrieval-sized chunks with annotation emphasis."""

from __future__ import annotations

import re
import unicodedata

from arquimedes.models import Annotation, Chunk, Page


def chunk_pages(
    pages: list[Page],
    annotations: list[Annotation] | None = None,
    chunk_size: int = 500,
) -> list[Chunk]:
    """Split page text into retrieval-sized chunks.

    Args:
        pages: List of extracted pages.
        annotations: List of reader annotations (for emphasis marking).
        chunk_size: Target chunk size in approximate tokens (~4 chars per token).

    Returns:
        List of Chunk objects with emphasis flags set.
    """
    # Build normalized emphasized text spans from annotations, keyed by page
    emphasized_spans: dict[int, list[str]] = {}
    if annotations:
        for ann in annotations:
            if ann.type in ("highlight", "underline") and ann.quoted_text:
                normalized = _normalize_for_matching(ann.quoted_text)
                if len(normalized) >= 10:  # skip very short fragments
                    emphasized_spans.setdefault(ann.page, []).append(normalized)

    chunks: list[Chunk] = []
    chunk_counter = 0
    char_limit = chunk_size * 4  # rough token-to-char conversion

    for page in pages:
        text = page.text.strip()
        if not text:
            continue

        # Split into paragraphs first
        paragraphs = _split_paragraphs(text)
        page_emphasized = emphasized_spans.get(page.page_number, [])

        current_text = ""
        current_pages: list[int] = [page.page_number]

        for para in paragraphs:
            if len(current_text) + len(para) + 1 > char_limit and current_text:
                # Emit current chunk
                chunk_counter += 1
                chunks.append(Chunk(
                    chunk_id=f"chk_{chunk_counter:05d}",
                    text=current_text.strip(),
                    source_pages=list(current_pages),
                    emphasized=_is_emphasized(current_text, page_emphasized),
                ))
                current_text = ""
                current_pages = [page.page_number]

            current_text += para + "\n\n"

        # Emit remaining text
        if current_text.strip():
            chunk_counter += 1
            chunks.append(Chunk(
                chunk_id=f"chk_{chunk_counter:05d}",
                text=current_text.strip(),
                source_pages=list(current_pages),
                emphasized=_is_emphasized(current_text, page_emphasized),
            ))

    return chunks


def _normalize_for_matching(text: str) -> str:
    """Normalize text for fuzzy matching between annotations and chunk text.

    PDF annotation text and page text often differ due to:
    - Soft hyphens (\\xad) from hyphenation
    - Line breaks mid-word from column layouts
    - Stray single characters from rendering artifacts
    - Whitespace differences

    This normalizer strips all of that to produce a comparable string.
    """
    # Remove soft hyphens
    text = text.replace("\xad", "")
    # Remove all control characters and zero-width chars
    text = "".join(
        c for c in text
        if unicodedata.category(c)[0] not in ("C",)  # control chars
    )
    # Collapse all whitespace (newlines, tabs, multiple spaces) into single space
    text = re.sub(r"\s+", " ", text)
    # Remove stray single characters surrounded by spaces (layout artifacts)
    text = re.sub(r"\b[a-z]\b", "", text)
    # Collapse whitespace again after artifact removal
    text = re.sub(r"\s+", " ", text)
    return text.strip().lower()


def _is_emphasized(chunk_text: str, emphasized_spans: list[str]) -> bool:
    """Check if a chunk overlaps with any emphasized (highlighted/annotated) spans.

    Uses two strategies:
    1. Direct substring match on normalized text (fast, exact)
    2. Word-sequence overlap (handles leading/trailing artifacts from PDF extraction)

    PDF annotations frequently have garbage characters at the start/end from
    column boundaries, so we also check if a significant contiguous word sequence
    from the annotation appears in the chunk.
    """
    if not emphasized_spans:
        return False
    chunk_normalized = _normalize_for_matching(chunk_text)

    for span in emphasized_spans:
        # Strategy 1: direct substring
        if span in chunk_normalized:
            return True

        # Strategy 2: sliding word-sequence overlap
        span_words = span.split()
        if len(span_words) < 4:
            continue

        # Try to find a contiguous sequence of at least 4 words from the annotation
        # in the chunk, allowing for leading/trailing garbage
        window_size = min(len(span_words), 6)
        for start in range(len(span_words) - window_size + 1):
            window = " ".join(span_words[start:start + window_size])
            if window in chunk_normalized:
                return True

    return False


def _split_paragraphs(text: str) -> list[str]:
    """Split text into paragraphs on double newlines, preserving structure."""
    raw = text.split("\n\n")
    paragraphs: list[str] = []
    for p in raw:
        p = p.strip()
        if p:
            paragraphs.append(p)
    return paragraphs
