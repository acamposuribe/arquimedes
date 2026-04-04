"""Deterministic PDF extraction: text, pages, TOC, tables, annotations."""

from __future__ import annotations

import json
import re
from pathlib import Path

import fitz  # PyMuPDF
import pdfplumber

from arquimedes.classify import classify_document_type, extract_keywords
from arquimedes.models import Annotation, MaterialMeta, Page, Table


def extract_text_and_pages(pdf_path: Path) -> tuple[str, list[Page], list[dict]]:
    """Extract raw text and page-level data from a PDF.

    Returns:
        (full_text, pages, toc)
    """
    doc = fitz.open(str(pdf_path))
    pages: list[Page] = []
    full_text_parts: list[str] = []

    for page_num in range(len(doc)):
        page = doc[page_num]
        text = page.get_text("text")
        full_text_parts.append(text)

        # Extract headings heuristically from text blocks
        headings = _extract_headings(page)

        pages.append(Page(
            page_number=page_num + 1,  # 1-indexed
            text=text,
            headings=headings,
        ))

    # Extract TOC from PDF outline
    toc = []
    for level, title, page_num in doc.get_toc():
        toc.append({"level": level, "title": title, "page": page_num})

    # Compute section_boundaries from TOC or headings.
    # A section boundary is the heading that starts on (or spans into) a page.
    _assign_section_boundaries(pages, toc)

    doc.close()
    full_text = "\n\n".join(full_text_parts)
    return full_text, pages, toc


def _extract_headings(page: fitz.Page) -> list[str]:
    """Extract likely headings from a page using font size heuristics."""
    blocks = page.get_text("dict", flags=fitz.TEXT_PRESERVE_WHITESPACE)["blocks"]
    headings = []
    body_sizes: list[float] = []

    # First pass: collect all font sizes to find the body size
    for block in blocks:
        if block.get("type") != 0:  # text blocks only
            continue
        for line in block.get("lines", []):
            for span in line.get("spans", []):
                if span.get("text", "").strip():
                    body_sizes.append(span["size"])

    if not body_sizes:
        return []

    # Body size is the most common font size
    size_counts: dict[float, int] = {}
    for s in body_sizes:
        rounded = round(s, 1)
        size_counts[rounded] = size_counts.get(rounded, 0) + 1
    body_size = max(size_counts, key=size_counts.get)

    # Second pass: text larger than body size or bold are candidate headings
    for block in blocks:
        if block.get("type") != 0:
            continue
        for line in block.get("lines", []):
            line_text = ""
            is_heading = False
            for span in line.get("spans", []):
                text = span.get("text", "").strip()
                if not text:
                    continue
                line_text += text + " "
                size = span.get("size", 0)
                flags = span.get("flags", 0)
                is_bold = flags & 2 ** 4  # bit 4 = bold
                if size > body_size + 0.5 or (is_bold and size >= body_size):
                    is_heading = True

            line_text = line_text.strip()
            if is_heading and line_text and len(line_text) < 200:
                headings.append(line_text)

    return headings


def _assign_section_boundaries(pages: list[Page], toc: list[dict]) -> None:
    """Populate section_boundaries on each page using TOC or headings.

    If the PDF has a TOC, we use it (more reliable hierarchy). Otherwise we fall
    back to the headings already extracted per page.  Each page gets the list of
    section titles that *begin* on that page.
    """
    if toc:
        # TOC entries have {"level", "title", "page"} where page is 1-indexed.
        page_map: dict[int, list[str]] = {}
        for entry in toc:
            pg = entry.get("page", 0)
            if pg > 0:
                page_map.setdefault(pg, []).append(entry["title"])
        for page in pages:
            page.section_boundaries = page_map.get(page.page_number, [])
    else:
        # Fall back to extracted headings (first heading per page = section start)
        for page in pages:
            page.section_boundaries = list(page.headings)


def extract_annotations(pdf_path: Path) -> list[Annotation]:
    """Extract reader annotations (highlights, notes, marks) from a PDF."""
    doc = fitz.open(str(pdf_path))
    annotations: list[Annotation] = []
    ann_counter = 0

    for page_num in range(len(doc)):
        page = doc[page_num]
        for annot in page.annots() or []:
            ann_type = annot.type[1].lower() if annot.type else "unknown"

            # Map PyMuPDF annotation types to our types
            type_map = {
                "highlight": "highlight",
                "underline": "underline",
                "strikeout": "strikeout",
                "squiggly": "underline",
                "text": "note",
                "freetext": "freetext",
                "popup": None,  # skip popups, they're linked to other annotations
            }

            mapped_type = type_map.get(ann_type)
            if mapped_type is None:
                continue

            # Extract the highlighted/annotated text
            quoted_text = ""
            if annot.vertices:
                # For markup annotations (highlight, underline, etc.),
                # extract text from the quad points
                try:
                    quads = annot.vertices
                    # vertices come in groups of 4 points (quad points)
                    for i in range(0, len(quads), 4):
                        if i + 3 < len(quads):
                            quad = fitz.Quad(quads[i], quads[i + 1], quads[i + 2], quads[i + 3])
                            quoted_text += page.get_text("text", clip=quad.rect).strip() + " "
                except Exception:
                    pass
            elif annot.rect:
                quoted_text = page.get_text("text", clip=annot.rect).strip()

            quoted_text = quoted_text.strip()

            # Get the annotation's comment/note text
            comment = annot.info.get("content", "") or ""

            # Get color
            color = ""
            if annot.colors and annot.colors.get("stroke"):
                rgb = annot.colors["stroke"]
                color = "#{:02x}{:02x}{:02x}".format(
                    int(rgb[0] * 255), int(rgb[1] * 255), int(rgb[2] * 255)
                )

            ann_counter += 1
            annotations.append(Annotation(
                annotation_id=f"ann_{ann_counter:04d}",
                type=mapped_type,
                page=page_num + 1,
                quoted_text=quoted_text,
                comment=comment,
                color=color,
                rect=[annot.rect.x0, annot.rect.y0, annot.rect.x1, annot.rect.y1],
            ))

    doc.close()
    return annotations


def extract_tables(pdf_path: Path) -> list[Table]:
    """Extract tables from a PDF using pdfplumber."""
    tables: list[Table] = []
    table_counter = 0

    with pdfplumber.open(str(pdf_path)) as pdf:
        for page_num, page in enumerate(pdf.pages):
            page_tables = page.extract_tables() or []
            for raw_table in page_tables:
                if not raw_table or len(raw_table) < 2:
                    continue

                # First row as headers, rest as data
                headers = [str(cell or "").strip() for cell in raw_table[0]]
                rows = [
                    [str(cell or "").strip() for cell in row]
                    for row in raw_table[1:]
                ]

                table_counter += 1
                tables.append(Table(
                    table_id=f"tbl_{table_counter:04d}",
                    source_page=page_num + 1,
                    headers=headers,
                    rows=rows,
                ))

    return tables


def extract_raw_pdf(
    pdf_path: Path,
    output_dir: Path,
    material_id: str,
    manifest_entry: dict,
) -> MaterialMeta:
    """Run the full deterministic extraction pipeline for a PDF.

    Args:
        pdf_path: Path to the source PDF file.
        output_dir: Directory to write extracted artifacts (extracted/<material_id>/).
        material_id: The material's unique ID.
        manifest_entry: Dict with manifest fields (file_hash, relative_path, etc.)

    Returns:
        MaterialMeta with raw fields populated.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # 1. Text and pages
    full_text, pages, toc = extract_text_and_pages(pdf_path)

    # 2. Annotations
    annotations = extract_annotations(pdf_path)

    # Mark pages that have annotations
    annotation_pages: dict[int, list[str]] = {}
    for ann in annotations:
        annotation_pages.setdefault(ann.page, []).append(ann.annotation_id)

    for page in pages:
        if page.page_number in annotation_pages:
            page.has_annotations = True
            page.annotation_ids = annotation_pages[page.page_number]

    # 3. Tables
    tables = extract_tables(pdf_path)

    # Mark pages with table refs
    for table in tables:
        for page in pages:
            if page.page_number == table.source_page:
                page.table_refs.append(table.table_id)

    # 4. Build metadata
    doc = fitz.open(str(pdf_path))
    pdf_meta = doc.metadata or {}
    title = pdf_meta.get("title", "") or ""
    if not title:
        # Fallback: use first heading or filename
        for page in pages:
            if page.headings:
                title = page.headings[0]
                break
        if not title:
            title = pdf_path.stem

    authors = []
    author_str = pdf_meta.get("author", "") or ""
    if author_str:
        # Split on common separators
        authors = [a.strip() for a in re.split(r"[,;&]| and ", author_str) if a.strip()]

    page_count = len(doc)
    doc.close()

    # 5. Deterministic classification
    raw_keywords = extract_keywords(pages)
    raw_document_type = classify_document_type(
        pages, title=title, filename=pdf_path.name,
    ) or ""

    meta = MaterialMeta(
        material_id=material_id,
        file_hash=manifest_entry.get("file_hash", ""),
        source_path=manifest_entry.get("relative_path", ""),
        title=title,
        authors=authors,
        year=_extract_year(pdf_meta, pages),
        language="",  # could detect with langdetect, left for enrichment
        page_count=page_count,
        file_type="pdf",
        domain=manifest_entry.get("domain", ""),
        collection=manifest_entry.get("collection", ""),
        ingested_at=manifest_entry.get("ingested_at", ""),
        raw_keywords=raw_keywords,
        raw_document_type=raw_document_type,
    )

    # 6. Write artifacts
    meta.save(output_dir.parent)  # saves to output_dir/meta.json via material_id

    # full text
    (output_dir / "text.md").write_text(full_text, encoding="utf-8")

    # pages
    _write_jsonl(output_dir / "pages.jsonl", [p.to_dict() for p in pages])

    # annotations
    if annotations:
        _write_jsonl(output_dir / "annotations.jsonl", [a.to_dict() for a in annotations])

    # tables
    if tables:
        _write_jsonl(output_dir / "tables.jsonl", [t.to_dict() for t in tables])

    # TOC
    (output_dir / "toc.json").write_text(
        json.dumps(toc, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    return meta


def _extract_year(pdf_meta: dict, pages: list[Page] | None = None) -> str:
    """Try to extract a publication year from page text, falling back to PDF metadata.

    Text-derived year is preferred because PDF creation/modification dates often
    reflect when the file was exported or downloaded, not when it was published.
    """
    # 1. Scan early pages for publication year patterns
    if pages:
        text_year = _extract_year_from_text(pages)
        if text_year:
            return text_year

    # 2. Fall back to PDF metadata dates
    for key in ("creationDate", "modDate"):
        date_str = pdf_meta.get(key, "") or ""
        if date_str:
            # PDF dates: D:YYYYMMDDHHmmSS or similar
            match = re.search(r"(\d{4})", date_str)
            if match:
                year = int(match.group(1))
                if 1800 <= year <= 2100:
                    return str(year)
    return ""


# Patterns that strongly indicate a publication year (ordered by specificity)
_YEAR_PATTERNS = [
    # "Published: 2020", "Published 2020", "Publication date: 2020"
    re.compile(r"[Pp]ubli(?:shed|cation[\s_-]*date)\s*:?\s*\D*(\d{4})"),
    # "© 2020", "(c) 2020"
    re.compile(r"(?:\u00a9|\([Cc]\))\s*(\d{4})"),
    # "December 2020", "Jan 2021", etc.
    re.compile(
        r"(?:January|February|March|April|May|June|July|August|September|"
        r"October|November|December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Oct|Nov|Dec)"
        r"\.?\s+(\d{4})"
    ),
    # "Received: 12 March 2020" / "Accepted: 5 May 2020" (journal papers)
    re.compile(r"(?:Received|Accepted|Revised)\s*:?\s*\d{1,2}\s+\w+\s+(\d{4})"),
    # "2020-12-15" ISO date
    re.compile(r"\b(\d{4})-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01])\b"),
]


def _extract_year_from_text(pages: list[Page]) -> str:
    """Scan the first few pages for a publication year."""
    # Check first 3 pages (title page, abstract, copyright)
    candidates: list[int] = []
    for page in pages[:3]:
        for pattern in _YEAR_PATTERNS:
            for match in pattern.finditer(page.text):
                year = int(match.group(1))
                if 1800 <= year <= 2100:
                    candidates.append(year)
    if not candidates:
        return ""
    # If multiple candidates, the most frequent is likely the publication year;
    # on a tie, prefer the earliest (publication date < access/download date)
    from collections import Counter

    counts = Counter(candidates)
    max_count = max(counts.values())
    top = [y for y, c in counts.items() if c == max_count]
    return str(min(top))


def _write_jsonl(path: Path, items: list[dict]) -> None:
    """Write a list of dicts as JSONL."""
    with open(path, "w", encoding="utf-8") as f:
        for item in items:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
