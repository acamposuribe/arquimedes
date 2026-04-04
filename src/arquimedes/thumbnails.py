"""Generate page thumbnails for PDFs."""

from __future__ import annotations

from pathlib import Path

import fitz  # PyMuPDF

THUMBNAIL_DPI = 72  # low-res for browsing
THUMBNAIL_MAX_WIDTH = 400


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

    for page_num in range(len(doc)):
        page = doc[page_num]

        # Scale to fit within max width
        scale = min(THUMBNAIL_MAX_WIDTH / page.rect.width, 1.0)
        mat = fitz.Matrix(scale, scale)
        pix = page.get_pixmap(matrix=mat)

        filename = f"page_{page_num + 1:04d}.png"
        pix.save(str(thumbnails_dir / filename))
        paths.append(f"thumbnails/{filename}")

    doc.close()
    return paths
