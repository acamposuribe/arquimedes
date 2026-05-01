"""Deterministic extraction for text-like sources (.txt, .md, .markdown).

Also provides shared helpers used by Office-format extractors to write the
common raw artifacts (meta.json, text.md, pages.jsonl, chunks.jsonl) from a
list of synthetic pages.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from arquimedes.chunking import chunk_pages
from arquimedes.classify import classify_document_type, extract_keywords
from arquimedes.extract_pdf import _sanitize_strings, _strip_nuls
from arquimedes.models import MaterialMeta, Page

# Soft cap on synthetic page length (in characters). Picked to keep page text
# comfortably within enrichment context budgets while preserving paragraph
# locality; shared across text-like and office extractors.
DEFAULT_PAGE_TARGET_CHARS = 4000


def _normalize_text(raw: str) -> str:
    """Normalize line endings and strip embedded null bytes."""
    return _strip_nuls(raw).replace("\r\n", "\n").replace("\r", "\n")


def _read_text_file(path: Path) -> tuple[str, list[str]]:
    """Read a text file as UTF-8, falling back to replacement on decode errors.

    Returns (text, warnings).
    """
    warnings_: list[str] = []
    data = path.read_bytes()
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        text = data.decode("utf-8", errors="replace")
        warnings_.append(
            "non-utf8 bytes in source; decoded with replacement characters"
        )
    return _normalize_text(text), warnings_


def _split_paragraphs(text: str) -> list[str]:
    """Split text into paragraphs on blank-line boundaries."""
    paragraphs = re.split(r"\n\s*\n", text)
    return [p.strip("\n") for p in paragraphs if p.strip()]


def _pack_paragraphs_into_pages(
    paragraphs: list[str],
    target_chars: int = DEFAULT_PAGE_TARGET_CHARS,
) -> list[str]:
    """Greedily group paragraphs so each page approaches target_chars."""
    if not paragraphs:
        return []
    pages: list[str] = []
    current: list[str] = []
    current_len = 0
    for para in paragraphs:
        para_len = len(para)
        if current and current_len + para_len + 2 > target_chars:
            pages.append("\n\n".join(current))
            current = [para]
            current_len = para_len
        else:
            current.append(para)
            current_len += para_len + 2
    if current:
        pages.append("\n\n".join(current))
    return pages


def _split_markdown_by_headings(text: str) -> list[tuple[str, str]]:
    """Split Markdown text by top-level/secondary headings, preserving fences.

    Returns a list of (heading, body) tuples. Body includes the heading line.
    Lines inside fenced code blocks are not treated as headings.
    """
    lines = text.split("\n")
    in_fence = False
    sections: list[list[str]] = []
    headings: list[str] = []
    current: list[str] = []
    current_heading = ""

    fence_re = re.compile(r"^\s*(```|~~~)")
    heading_re = re.compile(r"^(#{1,2})\s+(.+?)\s*$")

    for line in lines:
        if fence_re.match(line):
            in_fence = not in_fence
            current.append(line)
            continue
        if not in_fence:
            m = heading_re.match(line)
            if m:
                if current:
                    sections.append(current)
                    headings.append(current_heading)
                current_heading = m.group(2).strip()
                current = [line]
                continue
        current.append(line)
    if current:
        sections.append(current)
        headings.append(current_heading)

    return [(h, "\n".join(body).strip("\n")) for h, body in zip(headings, sections) if "\n".join(body).strip()]


def _wrap_plain_text_as_markdown(text: str) -> str:
    """Wrap raw text in a fenced code block so Markdown control chars don't render."""
    fence = "```"
    while fence in text:
        fence += "`"
    return f"{fence}\n{text}\n{fence}\n"


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(_sanitize_strings(row), ensure_ascii=False) + "\n")


def write_synthetic_extraction(
    output_dir: Path,
    material_id: str,
    manifest_entry: dict,
    pages: list[Page],
    text_md: str,
    file_type: str,
    *,
    title: str = "",
    chunk_size: int = 500,
    warnings_: list[str] | None = None,
    classify: bool = True,
) -> MaterialMeta:
    """Write meta.json/text.md/pages.jsonl/chunks.jsonl for a synthetic-page source.

    Shared by text/markdown/docx/pptx/xlsx extractors.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    if not title:
        for page in pages:
            if page.headings:
                title = page.headings[0]
                break
    if not title:
        title = Path(manifest_entry.get("relative_path", "")).stem or material_id

    raw_keywords: list[str] = []
    raw_document_type = ""
    if classify and pages:
        try:
            raw_keywords = extract_keywords(pages)
            raw_document_type = _strip_nuls(
                classify_document_type(
                    pages,
                    title=title,
                    filename=Path(manifest_entry.get("relative_path", "")).name,
                )
                or ""
            )
        except Exception:
            raw_keywords = []
            raw_document_type = ""

    meta = MaterialMeta(
        material_id=material_id,
        file_hash=manifest_entry.get("file_hash", ""),
        source_path=manifest_entry.get("relative_path", ""),
        title=_strip_nuls(title),
        authors=[],
        year="",
        language="",
        page_count=len(pages),
        file_type=file_type,
        domain=manifest_entry.get("domain", ""),
        collection=manifest_entry.get("collection", ""),
        ingested_at=manifest_entry.get("ingested_at", ""),
        raw_keywords=raw_keywords,
        raw_document_type=raw_document_type,
    )
    meta.save(output_dir.parent)

    (output_dir / "text.md").write_text(_strip_nuls(text_md), encoding="utf-8")
    _write_jsonl(output_dir / "pages.jsonl", [p.to_dict() for p in pages])

    chunks = chunk_pages(pages, chunk_size=chunk_size)
    _write_jsonl(output_dir / "chunks.jsonl", [c.to_dict() for c in chunks])

    if warnings_:
        _write_jsonl(
            output_dir / "extraction_warnings.jsonl",
            [{"message": w} for w in warnings_],
        )

    return meta


def extract_raw_text_file(
    source_path: Path,
    output_dir: Path,
    material_id: str,
    manifest_entry: dict,
    chunk_size: int = 500,
) -> MaterialMeta:
    """Extract a plain .txt file into synthetic pages."""
    text, warnings_ = _read_text_file(source_path)
    paragraphs = _split_paragraphs(text)
    page_texts = _pack_paragraphs_into_pages(paragraphs) or [text.strip()]

    pages: list[Page] = []
    text_md_parts: list[str] = []
    for i, page_text in enumerate(page_texts, start=1):
        pages.append(Page(page_number=i, text=page_text))
        text_md_parts.append(f"# Page {i}\n\n{_wrap_plain_text_as_markdown(page_text)}")
    text_md = "\n".join(text_md_parts)

    return write_synthetic_extraction(
        output_dir,
        material_id,
        manifest_entry,
        pages,
        text_md,
        file_type="text",
        chunk_size=chunk_size,
        warnings_=warnings_,
    )


def extract_raw_markdown_file(
    source_path: Path,
    output_dir: Path,
    material_id: str,
    manifest_entry: dict,
    chunk_size: int = 500,
) -> MaterialMeta:
    """Extract a .md/.markdown file into synthetic pages keyed on top-level headings."""
    text, warnings_ = _read_text_file(source_path)
    sections = _split_markdown_by_headings(text)

    if not sections:
        page_texts = _pack_paragraphs_into_pages(_split_paragraphs(text)) or [text.strip()]
        pages = [Page(page_number=i, text=t) for i, t in enumerate(page_texts, start=1)]
    else:
        pages = []
        for i, (heading, body) in enumerate(sections, start=1):
            page = Page(page_number=i, text=body)
            if heading:
                page.headings = [heading]
            pages.append(page)

    text_md = text.strip() + "\n"

    return write_synthetic_extraction(
        output_dir,
        material_id,
        manifest_entry,
        pages,
        text_md,
        file_type="markdown",
        chunk_size=chunk_size,
        warnings_=warnings_,
    )
