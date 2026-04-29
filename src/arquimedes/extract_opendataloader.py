"""OpenDataLoader PDF adapter for born-digital PDFs.

This module deliberately adapts OpenDataLoader into Arquimedes' existing
artifacts instead of making OpenDataLoader's layout schema canonical.
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
import warnings
from collections import defaultdict
from pathlib import Path
from typing import Any

import fitz  # PyMuPDF

from arquimedes.classify import classify_document_type, extract_keywords
from arquimedes.extract_pdf import (
    _assign_section_boundaries,
    _extract_year,
    _sanitize_strings,
    _strip_nuls,
    _write_jsonl,
    extract_annotations,
    extract_tables,
)
from arquimedes.models import MaterialMeta, Page


_TEXT_TYPES = {"heading", "paragraph", "caption", "list item", "text block"}
_SECTION_HEADING_LEVEL_MAX = 3
_TEXT_LAYER_MIN_CHARS = 40
_HOMEBREW_JAVA = Path("/opt/homebrew/opt/openjdk/bin/java")


def pdf_has_usable_text_layer(pdf_path: Path, min_chars: int = _TEXT_LAYER_MIN_CHARS) -> bool:
    """Return True when a PDF has enough embedded text to avoid OCR."""
    with fitz.open(str(pdf_path)) as doc:
        sample_pages = min(len(doc), 5)
        total = 0
        for index in range(sample_pages):
            total += len(_strip_nuls(doc[index].get_text("text")).strip())
            if total >= min_chars:
                return True
    return False


def extract_raw_pdf_opendataloader(
    pdf_path: Path,
    output_dir: Path,
    material_id: str,
    manifest_entry: dict,
) -> MaterialMeta:
    """Extract a born-digital PDF via OpenDataLoader and existing side channels.

    OpenDataLoader supplies Markdown and page text/headings. PyMuPDF/pdfplumber
    remain responsible for annotations, metadata cross-checks, TOC, and tables.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    data, markdown = _run_opendataloader(pdf_path)
    (output_dir / "text.md").write_text(_strip_nuls(markdown), encoding="utf-8")

    pages = _pages_from_opendataloader(data)
    toc = _toc_from_pdf(pdf_path)
    _assign_section_boundaries(pages, toc)

    annotations = extract_annotations(pdf_path)
    _mark_annotation_pages(pages, annotations)

    tables = extract_tables(pdf_path)
    for table in tables:
        for page in pages:
            if page.page_number == table.source_page:
                page.table_refs.append(table.table_id)

    meta = _build_meta(pdf_path, data, pages, material_id, manifest_entry)
    meta.save(output_dir.parent)

    _write_jsonl(output_dir / "pages.jsonl", [page.to_dict() for page in pages])
    if annotations:
        _write_jsonl(output_dir / "annotations.jsonl", [ann.to_dict() for ann in annotations])
    if tables:
        _write_jsonl(output_dir / "tables.jsonl", [table.to_dict() for table in tables])
    (output_dir / "toc.json").write_text(
        json.dumps(_sanitize_strings(toc), separators=(",", ":"), ensure_ascii=False),
        encoding="utf-8",
    )

    return meta


def _run_opendataloader(pdf_path: Path) -> tuple[dict[str, Any], str]:
    """Run OpenDataLoader in an isolated temporary directory."""
    java_path = find_java_runtime()
    if java_path is None:
        raise RuntimeError("OpenDataLoader requires Java on PATH")

    try:
        import opendataloader_pdf
    except ImportError as exc:  # pragma: no cover - depends on optional package
        raise RuntimeError("OpenDataLoader Python package is not installed") from exc

    with tempfile.TemporaryDirectory(prefix="arquimedes-opendataloader-") as tmp:
        tmp_path = Path(tmp)
        old_path = os.environ.get("PATH", "")
        java_dir = str(Path(java_path).parent)
        if java_dir not in old_path.split(os.pathsep):
            os.environ["PATH"] = java_dir + os.pathsep + old_path
        opendataloader_pdf.convert(
            input_path=[str(pdf_path)],
            output_dir=str(tmp_path),
            format="markdown,json",
            image_output="off",
        )
        os.environ["PATH"] = old_path
        json_files = sorted(tmp_path.glob("*.json"))
        md_files = sorted(tmp_path.glob("*.md"))
        if not json_files or not md_files:
            raise RuntimeError("OpenDataLoader did not produce Markdown and JSON outputs")
        data = json.loads(json_files[0].read_text(encoding="utf-8"))
        markdown = md_files[0].read_text(encoding="utf-8")
    if not isinstance(data, dict):
        raise RuntimeError("OpenDataLoader JSON root was not an object")
    return data, markdown


def find_java_runtime() -> str | None:
    """Find a real Java binary, including Homebrew's keg-only OpenJDK."""
    java = shutil.which("java")
    if java:
        return java
    if _HOMEBREW_JAVA.exists():
        return str(_HOMEBREW_JAVA)
    return None


def _pages_from_opendataloader(data: dict[str, Any]) -> list[Page]:
    page_count = int(data.get("number of pages") or 0)
    page_elements: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for element in _iter_layout_elements(data.get("kids", [])):
        page_number = element.get("page number")
        if isinstance(page_number, int) and page_number > 0:
            page_elements[page_number].append(element)

    if not page_count and page_elements:
        page_count = max(page_elements)

    pages: list[Page] = []
    for page_number in range(1, page_count + 1):
        elements = page_elements.get(page_number, [])
        text_parts: list[str] = []
        headings: list[str] = []
        section_boundaries: list[str] = []
        for element in elements:
            element_type = element.get("type", "")
            if element.get("_parent_type") in ("header", "footer"):
                continue
            content = _strip_nuls(str(element.get("content", "") or "")).strip()
            if not content or element_type not in _TEXT_TYPES:
                continue
            if element_type == "heading":
                headings.append(content)
                if _heading_level(element) <= _SECTION_HEADING_LEVEL_MAX:
                    section_boundaries.append(content)
            text_parts.append(_format_element_text(element_type, content))

        pages.append(Page(
            page_number=page_number,
            text="\n\n".join(part for part in text_parts if part).strip(),
            headings=headings,
            section_boundaries=section_boundaries,
        ))
    return pages


def _iter_layout_elements(nodes: Any) -> list[dict[str, Any]]:
    elements: list[dict[str, Any]] = []

    def visit(node: Any, parent_type: str = "") -> None:
        if isinstance(node, dict):
            element = {
                key: value
                for key, value in node.items()
                if key not in ("kids", "list items")
            }
            if "type" in element:
                element["_parent_type"] = parent_type
                elements.append(element)
            node_type = str(node.get("type", ""))
            for child_key in ("kids", "list items"):
                for child in node.get(child_key, []) or []:
                    visit(child, node_type)
        elif isinstance(node, list):
            for child in node:
                visit(child, parent_type)

    visit(nodes)
    return elements


def _format_element_text(element_type: str, content: str) -> str:
    if element_type == "heading":
        return content
    if element_type == "list item":
        return content
    if element_type == "caption":
        return f"Caption: {content}"
    return content


def _heading_level(element: dict[str, Any]) -> int:
    value = element.get("heading level")
    if isinstance(value, int):
        return value
    try:
        return int(value)
    except (TypeError, ValueError):
        return 99


def _toc_from_pdf(pdf_path: Path) -> list[dict]:
    with fitz.open(str(pdf_path)) as doc:
        return [
            {"level": level, "title": _strip_nuls(title), "page": page_num}
            for level, title, page_num in doc.get_toc()
        ]


def _mark_annotation_pages(pages, annotations) -> None:
    annotation_pages: dict[int, list[str]] = {}
    for ann in annotations:
        annotation_pages.setdefault(ann.page, []).append(ann.annotation_id)
    for page in pages:
        if page.page_number in annotation_pages:
            page.has_annotations = True
            page.annotation_ids = annotation_pages[page.page_number]


def _build_meta(
    pdf_path: Path,
    data: dict[str, Any],
    pages: list[Page],
    material_id: str,
    manifest_entry: dict,
) -> MaterialMeta:
    with fitz.open(str(pdf_path)) as doc:
        pdf_meta = doc.metadata or {}
        page_count = len(doc)

    title = _strip_nuls(str(data.get("title") or pdf_meta.get("title") or "")).strip()
    if not title:
        for page in pages:
            if page.headings:
                title = page.headings[0]
                break
    if not title:
        title = pdf_path.stem

    author_value = _strip_nuls(str(data.get("author") or pdf_meta.get("author") or "")).strip()
    authors = [part.strip() for part in author_value.split(",") if part.strip()]

    raw_keywords = extract_keywords(pages)
    raw_document_type = _strip_nuls(classify_document_type(
        pages,
        title=title,
        filename=pdf_path.name,
    ) or "")

    return MaterialMeta(
        material_id=material_id,
        file_hash=manifest_entry.get("file_hash", ""),
        source_path=manifest_entry.get("relative_path", ""),
        title=title,
        authors=authors,
        year=_extract_year(pdf_meta, pages),
        language="",
        page_count=page_count,
        file_type="pdf",
        domain=manifest_entry.get("domain", ""),
        collection=manifest_entry.get("collection", ""),
        ingested_at=manifest_entry.get("ingested_at", ""),
        raw_keywords=raw_keywords,
        raw_document_type=raw_document_type,
    )


def warn_opendataloader_fallback(reason: str) -> None:
    warnings.warn(
        f"OpenDataLoader PDF extraction unavailable; falling back to built-in extractor: {reason}",
        stacklevel=2,
    )
