"""Extract-raw orchestrator: dispatches to PDF or image extraction."""

from __future__ import annotations

import json
import shutil
import subprocess
from dataclasses import asdict
from pathlib import Path

from arquimedes.chunking import chunk_pages
from arquimedes.config import get_library_root, get_project_root, load_config
from arquimedes.extract_figures import extract_all_figures
from arquimedes.extract_image import extract_raw_image
from arquimedes.extract_opendataloader import (
    extract_raw_pdf_opendataloader,
    pdf_has_usable_text_layer,
    warn_opendataloader_fallback,
)
from arquimedes.extract_pdf import _sanitize_strings, extract_raw_pdf
from arquimedes.ingest import load_manifest
from arquimedes.models import Annotation, Page
from arquimedes.thumbnails import generate_thumbnails


def extract_raw(
    material_id: str | None = None,
    config: dict | None = None,
    force: bool = False,
) -> list[str]:
    """Run deterministic extraction for one or all materials.

    Args:
        material_id: Specific material to extract, or None for all pending.
        config: Optional config dict.

    Returns:
        List of material_ids that were extracted.
    """
    if config is None:
        config = load_config()

    project_root = get_project_root()
    library_root = get_library_root(config)
    extracted_dir = project_root / "extracted"
    manifest = load_manifest(project_root)

    extraction_config = config.get("extraction", {})
    chunk_size = extraction_config.get("chunk_size", 500)
    generate_thumbs = extraction_config.get("generate_thumbnails", True)
    ocr_fallback = extraction_config.get("ocr_fallback", True)
    pdf_backend = extraction_config.get("pdf_backend", "builtin")
    fig_config = extraction_config.get("figure_extraction", {})
    extract_embedded = fig_config.get("embedded", True)
    extract_rasterized = fig_config.get("rasterize", True)
    rasterize_dpi = fig_config.get("rasterize_dpi", 200)

    # Determine which materials to process
    if material_id:
        if material_id not in manifest:
            raise ValueError(f"Material {material_id} not found in manifest")
        to_process = {material_id: manifest[material_id]}
    else:
        # Process all materials that haven't been extracted yet
        to_process = {}
        for mid, entry in manifest.items():
            output_dir = extracted_dir / mid
            if force or not (output_dir / "meta.json").exists():
                to_process[mid] = entry

    extracted_ids: list[str] = []

    for mid, entry in to_process.items():
        source_path = library_root / entry.relative_path
        if not source_path.exists():
            print(f"  Warning: source file not found: {source_path}")
            continue

        output_dir = extracted_dir / mid
        entry_dict = asdict(entry)

        print(f"  Extracting {entry.relative_path} ({entry.file_type})...")

        if force and output_dir.exists():
            for child in output_dir.iterdir():
                if child.is_dir():
                    import shutil
                    shutil.rmtree(child)
                else:
                    child.unlink()

        if entry.file_type == "pdf":
            _extract_pdf_material(
                source_path, output_dir, mid, entry_dict,
                chunk_size=chunk_size,
                ocr_fallback=ocr_fallback,
                generate_thumbs=generate_thumbs,
                extract_embedded=extract_embedded,
                extract_rasterized=extract_rasterized,
                rasterize_dpi=rasterize_dpi,
                pdf_backend=pdf_backend,
            )
        elif entry.file_type in ("image", "scanned_document"):
            _extract_image_material(
                source_path, output_dir, mid, entry_dict,
                chunk_size=chunk_size,
                ocr_fallback=ocr_fallback,
            )

        extracted_ids.append(mid)

    return extracted_ids


def _extract_pdf_material(
    pdf_path: Path,
    output_dir: Path,
    material_id: str,
    manifest_entry: dict,
    chunk_size: int = 500,
    ocr_fallback: bool = True,
    generate_thumbs: bool = True,
    extract_embedded: bool = True,
    extract_rasterized: bool = True,
    rasterize_dpi: int = 200,
    pdf_backend: str = "builtin",
) -> None:
    """Full PDF extraction pipeline."""
    # 1. Core extraction: text, pages, TOC, tables, annotations
    if _should_use_opendataloader(pdf_path, pdf_backend):
        extract_raw_pdf_opendataloader(
            pdf_path,
            output_dir,
            material_id,
            manifest_entry,
        )
    else:
        extract_raw_pdf(
            pdf_path,
            output_dir,
            material_id,
            manifest_entry,
            ocr_fallback=ocr_fallback,
        )

    # 2. Figures (embedded + rasterized)
    figures = extract_all_figures(
        pdf_path, output_dir,
        dpi=rasterize_dpi,
        extract_embedded=extract_embedded,
        extract_rasterized=extract_rasterized,
    )

    # Update page figure_refs
    pages = _load_pages(output_dir)
    for fig in figures:
        for page in pages:
            if page.page_number == fig.source_page:
                page.figure_refs.append(fig.figure_id)

    # Re-save pages with figure refs
    _save_pages(output_dir, pages)

    # 3. Thumbnails
    if generate_thumbs:
        thumbnail_paths = generate_thumbnails(pdf_path, output_dir)
        # Update page thumbnail paths
        for i, page in enumerate(pages):
            if i < len(thumbnail_paths):
                page.thumbnail_path = thumbnail_paths[i]
        _save_pages(output_dir, pages)

    # 4. Chunking with annotation emphasis
    annotations = _load_annotations(output_dir)
    chunks = chunk_pages(pages, annotations=annotations, chunk_size=chunk_size)

    # Save chunks
    with open(output_dir / "chunks.jsonl", "w", encoding="utf-8") as f:
        for chunk in chunks:
            f.write(json.dumps(_sanitize_strings(chunk.to_dict()), ensure_ascii=False) + "\n")


def _extract_image_material(
    image_path: Path,
    output_dir: Path,
    material_id: str,
    manifest_entry: dict,
    chunk_size: int = 500,
    ocr_fallback: bool = True,
) -> None:
    """Image file extraction pipeline."""
    extract_raw_image(
        image_path, output_dir, material_id, manifest_entry,
        ocr_fallback=ocr_fallback,
    )

    # Chunk OCR text if available
    pages = _load_pages(output_dir)
    if pages:
        chunks = chunk_pages(pages, chunk_size=chunk_size)
        with open(output_dir / "chunks.jsonl", "w", encoding="utf-8") as f:
            for chunk in chunks:
                f.write(json.dumps(_sanitize_strings(chunk.to_dict()), ensure_ascii=False) + "\n")


def _should_use_opendataloader(pdf_path: Path, pdf_backend: str) -> bool:
    """Decide whether to use OpenDataLoader for a PDF.

    `auto` is intentionally conservative: use OpenDataLoader only for PDFs with
    embedded text and fall back quietly when its runtime dependencies are absent.
    """
    backend = (pdf_backend or "builtin").strip().lower()
    if backend in ("builtin", "pymupdf"):
        return False
    if backend not in ("auto", "opendataloader"):
        raise ValueError(
            "Unsupported extraction.pdf_backend value "
            f"{pdf_backend!r}; expected builtin, auto, or opendataloader"
        )

    has_text_layer = pdf_has_usable_text_layer(pdf_path)
    if not has_text_layer:
        return False
    if backend == "opendataloader":
        return True

    try:
        import opendataloader_pdf  # noqa: F401
    except ImportError:
        warn_opendataloader_fallback("Python package is not installed")
        return False
    if shutil.which("java") is None:
        warn_opendataloader_fallback("Java is not on PATH")
        return False
    java_check = subprocess.run(
        ["java", "-version"],
        check=False,
        capture_output=True,
        text=True,
    )
    if java_check.returncode != 0:
        warn_opendataloader_fallback("Java runtime is not available")
        return False
    return True


def _load_pages(output_dir: Path) -> list[Page]:
    """Load pages from pages.jsonl."""
    pages_path = output_dir / "pages.jsonl"
    if not pages_path.exists():
        return []
    pages = []
    for line in pages_path.read_text().strip().splitlines():
        if line.strip():
            pages.append(Page.from_dict(json.loads(line)))
    return pages


def _save_pages(output_dir: Path, pages: list[Page]) -> None:
    """Save pages to pages.jsonl."""
    with open(output_dir / "pages.jsonl", "w", encoding="utf-8") as f:
        for page in pages:
            f.write(json.dumps(_sanitize_strings(page.to_dict()), ensure_ascii=False) + "\n")


def _load_annotations(output_dir: Path) -> list[Annotation]:
    """Load annotations from annotations.jsonl."""
    ann_path = output_dir / "annotations.jsonl"
    if not ann_path.exists():
        return []
    annotations = []
    for line in ann_path.read_text().strip().splitlines():
        if line.strip():
            annotations.append(Annotation.from_dict(json.loads(line)))
    return annotations
