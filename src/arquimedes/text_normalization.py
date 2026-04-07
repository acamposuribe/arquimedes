"""Deterministic cleanup for extracted OCR / PDF text."""

from __future__ import annotations

import re
import unicodedata
from collections import Counter
from dataclasses import replace

from arquimedes.models import Page

try:  # Optional dependency: better Unicode repair when available.
    from ftfy import fix_text as _fix_text
except ImportError:  # pragma: no cover - optional dependency
    _fix_text = None


_CONTROL_CHARS_RE = re.compile(r"[\u00ad\u200b\u200c\u200d\u2060]")
_MULTISPACE_RE = re.compile(r"[ \t]+")
_BLANKLINE_RE = re.compile(r"\n\s*\n+")
_PUNCT_SPACE_RE = re.compile(r"\s+([,.;:!?])")
_PURE_PAGE_NUMBER_RE = re.compile(r"^\s*\d+\s*$")


def normalize_text_line(text: str) -> str:
    """Normalize a single extracted line while preserving readable content."""
    if not text:
        return ""

    cleaned = text
    if _fix_text is not None:
        cleaned = _fix_text(cleaned)
    cleaned = unicodedata.normalize("NFKC", cleaned)
    cleaned = cleaned.replace("\u2010", "-").replace("\u2011", "-")
    cleaned = cleaned.replace("\u2012", "-").replace("\u2013", "-").replace("\u2014", "-")
    cleaned = _CONTROL_CHARS_RE.sub("", cleaned)
    cleaned = cleaned.replace("\x00", "")
    cleaned = _MULTISPACE_RE.sub(" ", cleaned)
    cleaned = _PUNCT_SPACE_RE.sub(r"\1", cleaned)
    return cleaned.strip()


def _join_wrapped_lines(lines: list[str]) -> str:
    """Join wrapped lines into a single flowing paragraph."""
    sentence = ""
    for line in lines:
        line = normalize_text_line(line)
        if not line:
            continue
        if not sentence:
            sentence = line
            continue
        if sentence.endswith("-") and line and line[0].islower():
            sentence = sentence[:-1] + line
        else:
            sentence = f"{sentence} {line}"
    return sentence.strip()


def _ends_with_sentence_stop(text: str) -> bool:
    """Return True when a line should terminate a paragraph."""
    stripped = text.rstrip()
    return stripped.endswith(".")


def _normalize_page_text(text: str, repeated_lines: set[str] | None = None) -> str:
    """Normalize a page of extracted text while preserving paragraph breaks."""
    if not text:
        return ""

    repeated_lines = repeated_lines or set()
    paragraphs: list[str] = []
    current: list[str] = []

    for raw_line in text.splitlines():
        line = normalize_text_line(raw_line)
        if not line:
            if current:
                paragraph = _join_wrapped_lines(current)
                if paragraph:
                    paragraphs.append(paragraph)
                current = []
            continue

        if _PURE_PAGE_NUMBER_RE.match(line):
            continue

        if line.casefold() in repeated_lines:
            continue

        if current and _ends_with_sentence_stop(current[-1]):
            paragraph = _join_wrapped_lines(current)
            if paragraph:
                paragraphs.append(paragraph)
            current = []

        current.append(line)

    if current:
        paragraph = _join_wrapped_lines(current)
        if paragraph:
            paragraphs.append(paragraph)

    return "\n\n".join(paragraphs).strip()


def normalize_extracted_pages(pages: list[Page]) -> tuple[list[Page], str]:
    """Normalize extracted page text and return updated pages plus full text.

    The cleaner keeps content deterministic and inspectable:
    - repairs Unicode glitches and soft hyphens
    - removes obvious page numbers
    - strips repeated running headers / footers
    - preserves paragraph breaks and line structure
    """
    if not pages:
        return [], ""

    # Find repeated header/footer lines using the first/last few lines of each page.
    candidate_counts: Counter[str] = Counter()
    for page in pages:
        lines = [normalize_text_line(line) for line in page.text.splitlines()]
        lines = [line for line in lines if line]
        if not lines:
            continue
        sample = list(dict.fromkeys(lines[:3] + lines[-3:]))
        for line in sample:
            if _PURE_PAGE_NUMBER_RE.match(line):
                continue
            if len(line) > 120:
                continue
            candidate_counts[line.casefold()] += 1

    # Treat repeatedly appearing short lines as running headers/footers.
    min_repetitions = 2 if len(pages) < 8 else 3
    repeated_lines = {
        line
        for line, count in candidate_counts.items()
        if count >= min_repetitions
    }

    normalized_pages: list[Page] = []
    full_text_parts: list[str] = []
    for page in pages:
        normalized_text = _normalize_page_text(page.text, repeated_lines=repeated_lines)
        normalized_headings = [normalize_text_line(h) for h in page.headings if normalize_text_line(h)]
        normalized_section_boundaries = [
            normalize_text_line(boundary)
            for boundary in page.section_boundaries
            if normalize_text_line(boundary)
        ]
        normalized_pages.append(
            replace(
                page,
                text=normalized_text,
                headings=normalized_headings,
                section_boundaries=normalized_section_boundaries,
            )
        )
        if normalized_text:
            full_text_parts.append(normalized_text)

    full_text = "\n\n".join(full_text_parts).strip()
    return normalized_pages, full_text


def normalize_extracted_text(text: str) -> str:
    """Normalize a standalone extracted text string."""
    if not text:
        return ""

    page = Page(page_number=1, text=text)
    _, normalized = normalize_extracted_pages([page])
    return normalized
