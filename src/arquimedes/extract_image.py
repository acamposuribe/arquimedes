"""Extraction for standalone image files (JPG, PNG, TIFF, etc.).

Handles two cases:
1. Scanned documents (diaries, handwritten notes) → OCR to extract text
2. Project/inspiration images (photos, renders) → minimal metadata, no text
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

from arquimedes.classify import classify_document_type, extract_keywords
from arquimedes.models import Figure, MaterialMeta, Page


def _tesseract_available() -> bool:
    """Check if the Tesseract binary is installed and reachable."""
    try:
        import pytesseract
    except ImportError:
        return False
    try:
        pytesseract.get_tesseract_version()
        return True
    except pytesseract.TesseractNotFoundError:
        return False


def _is_likely_scanned_document(image_path: Path) -> bool:
    """Heuristic: detect if an image is a scanned document vs a photo/render.

    Checks:
    - Aspect ratio close to standard paper sizes (A4, letter)
    - High resolution (scans are typically 200+ DPI equivalent)
    - Filename hints (scan, diary, page, handwritten, etc.)
    """
    name_lower = image_path.stem.lower()
    doc_hints = {"scan", "diary", "page", "handwritten", "notes", "letter", "document"}
    if any(hint in name_lower for hint in doc_hints):
        return True

    # Check parent folder for hints
    parent_lower = image_path.parent.name.lower()
    if any(hint in parent_lower for hint in {"scanned", "scans", "diary", "diaries", "notes"}):
        return True

    # Check aspect ratio
    try:
        from PIL import Image

        img = Image.open(image_path)
        w, h = img.size
        ratio = max(w, h) / min(w, h)
        # Paper-like ratio: A4 is ~1.41, letter is ~1.29
        if 1.2 < ratio < 1.6 and min(w, h) > 1000:
            return True
    except Exception:
        pass

    return False


def extract_raw_image(
    image_path: Path,
    output_dir: Path,
    material_id: str,
    manifest_entry: dict,
    ocr_fallback: bool = True,
) -> MaterialMeta:
    """Run extraction on a standalone image file.

    Args:
        image_path: Path to the source image.
        output_dir: Directory to write artifacts (extracted/<material_id>/).
        material_id: The material's unique ID.
        manifest_entry: Dict with manifest fields.
        ocr_fallback: Whether to attempt OCR on scanned documents.

    Returns:
        MaterialMeta with raw fields populated.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    figures_dir = output_dir / "figures"
    figures_dir.mkdir(parents=True, exist_ok=True)

    is_scanned = _is_likely_scanned_document(image_path)
    file_type = "scanned_document" if is_scanned else "image"

    # Copy original image to figures/
    dest_ext = image_path.suffix.lower()
    dest_filename = f"fig_0001{dest_ext}"
    shutil.copy2(image_path, figures_dir / dest_filename)

    # Create figure sidecar
    figure = Figure(
        figure_id="fig_0001",
        source_page=1,
        image_path=f"figures/{dest_filename}",
        extraction_method="embedded",
    )
    (figures_dir / "fig_0001.json").write_text(
        json.dumps(figure.to_dict(), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    # OCR for scanned documents
    text = ""
    pages: list[Page] = []

    if is_scanned and ocr_fallback:
        if not _tesseract_available():
            import warnings
            warnings.warn(
                "Tesseract binary not found. Install Tesseract to enable OCR: "
                "brew install tesseract (macOS) or apt install tesseract-ocr (Linux) "
                "or download from https://github.com/UB-Mannheim/tesseract/wiki (Windows)",
                stacklevel=2,
            )
        else:
            try:
                import pytesseract
                from PIL import Image

                img = Image.open(image_path)
                text = pytesseract.image_to_string(img)
                pages = [Page(
                    page_number=1,
                    text=text,
                    headings=[],
                )]
            except Exception as e:
                import warnings
                warnings.warn(f"OCR failed for {image_path}: {e}", stacklevel=2)

    # Deterministic classification (only useful if OCR produced text)
    raw_keywords: list[str] = []
    raw_document_type = ""
    if pages:
        raw_keywords = extract_keywords(pages)
        raw_document_type = classify_document_type(
            pages, title=image_path.stem, filename=image_path.name,
        ) or ""

    # Build metadata
    meta = MaterialMeta(
        material_id=material_id,
        file_hash=manifest_entry.get("file_hash", ""),
        source_path=manifest_entry.get("relative_path", ""),
        title=image_path.stem,  # filename as title, enrichment can improve
        page_count=1,
        file_type=file_type,
        domain=manifest_entry.get("domain", ""),
        collection=manifest_entry.get("collection", ""),
        ingested_at=manifest_entry.get("ingested_at", ""),
        raw_keywords=raw_keywords,
        raw_document_type=raw_document_type,
    )

    # Write artifacts
    meta.save(output_dir.parent)
    (output_dir / "text.md").write_text(text, encoding="utf-8")

    if pages:
        with open(output_dir / "pages.jsonl", "w", encoding="utf-8") as f:
            for page in pages:
                f.write(json.dumps(page.to_dict(), ensure_ascii=False) + "\n")

    (output_dir / "toc.json").write_text("[]", encoding="utf-8")

    return meta
