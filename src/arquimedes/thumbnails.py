"""Generate page thumbnails for PDFs."""

from __future__ import annotations

from pathlib import Path

import fitz  # PyMuPDF

THUMBNAIL_DPI = 108  # readable previews for LLM/page browsing without huge images
THUMBNAIL_MAX_WIDTH = 900
SINGLE_PAGE_THUMBNAIL_DPI = 144
SINGLE_PAGE_THUMBNAIL_MAX_WIDTH = 1400


def generate_thumbnails(pdf_path: Path, output_dir: Path) -> list[str]:
    """Generate a thumbnail image for each page of a PDF.

    Args:
        pdf_path: Path to the source PDF.
        output_dir: The material's output directory (extracted/<material_id>/).

    Returns:
        List of relative paths to thumbnail files.
    """
    thumbnails_dir = output_dir / "thumbnails"
    thumbnails_dir.mkdir(parents=True, exist_ok=True)

    doc = fitz.open(str(pdf_path))
    paths: list[str] = []
    is_single_page = len(doc) == 1
    dpi = SINGLE_PAGE_THUMBNAIL_DPI if is_single_page else THUMBNAIL_DPI
    max_width = SINGLE_PAGE_THUMBNAIL_MAX_WIDTH if is_single_page else THUMBNAIL_MAX_WIDTH

    for page_num in range(len(doc)):
        page = doc[page_num]

        # Render at a readable resolution, capped by max width. Single-page
        # PDFs are usually standalone drawings, so preserve more detail.
        dpi_scale = dpi / 72
        width_scale = max_width / page.rect.width
        scale = min(dpi_scale, width_scale)
        mat = fitz.Matrix(scale, scale)
        pix = page.get_pixmap(matrix=mat)

        filename = f"page_{page_num + 1:04d}.png"
        pix.save(str(thumbnails_dir / filename))
        paths.append(f"thumbnails/{filename}")

    doc.close()
    return paths
