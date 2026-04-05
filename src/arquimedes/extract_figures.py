"""Figure extraction from PDFs: embedded images + page rasterization with region detection."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import fitz  # PyMuPDF

from arquimedes.models import Figure

# Minimum dimensions to consider an image worth extracting (pixels)
MIN_IMAGE_WIDTH = 100
MIN_IMAGE_HEIGHT = 100
MIN_IMAGE_AREA = 15000  # skip tiny decorative images
FULL_PAGE_BBOX_THRESHOLD = 0.9
TEXT_HEAVY_PAGE_THRESHOLD = 400


def _bbox_area(bbox: list[float]) -> float:
    if len(bbox) != 4:
        return 0.0
    x0, y0, x1, y1 = bbox
    return max(x1 - x0, 0) * max(y1 - y0, 0)


def _is_near_full_page_text_scan(page: fitz.Page, bbox: list[float]) -> bool:
    """Heuristic: embedded image is effectively a scanned text page, not a figure."""
    if len(bbox) != 4:
        return False
    page_area = page.rect.width * page.rect.height
    if page_area <= 0:
        return False
    coverage = _bbox_area(bbox) / page_area
    if coverage >= FULL_PAGE_BBOX_THRESHOLD:
        return True
    if coverage < 0.8:
        return False
    # If the image is page-shaped and the page carries a lot of text, it is
    # usually a page screenshot / scan, not a meaningful figure.
    page_text = page.get_text("text") or ""
    if len(page_text.strip()) < TEXT_HEAVY_PAGE_THRESHOLD:
        return False
    page_ratio = page.rect.width / page.rect.height if page.rect.height else 0
    image_ratio = 0
    if page.rect.width > 0 and page.rect.height > 0:
        # The bbox alone is enough for most scans; keep the aspect-ratio test
        # only for the borderline cases.
        bbox_width = max(bbox[2] - bbox[0], 0)
        bbox_height = max(bbox[3] - bbox[1], 0)
        image_ratio = bbox_width / bbox_height if bbox_height else 0
    if page_ratio <= 0 or image_ratio <= 0:
        return False
    ratio_delta = abs(image_ratio - page_ratio) / page_ratio
    return ratio_delta <= 0.15


def extract_embedded_images(pdf_path: Path, output_dir: Path) -> list[Figure]:
    """Extract embedded raster images from a PDF.

    These are photos, scanned images, and raster graphics embedded in the PDF.
    """
    figures_dir = output_dir / "figures"
    figures_dir.mkdir(parents=True, exist_ok=True)

    doc = fitz.open(str(pdf_path))
    figures: list[Figure] = []
    seen_xrefs: set[int] = set()
    seen_hashes: set[str] = set()
    fig_counter = 0

    for page_num in range(len(doc)):
        page = doc[page_num]
        image_list = page.get_images(full=True)

        for img_info in image_list:
            xref = img_info[0]

            # Skip duplicate images (same xref = same image object)
            if xref in seen_xrefs:
                continue
            seen_xrefs.add(xref)

            try:
                base_image = doc.extract_image(xref)
            except Exception:
                continue

            if not base_image:
                continue

            width = base_image.get("width", 0)
            height = base_image.get("height", 0)

            # Skip small/decorative images
            if width < MIN_IMAGE_WIDTH or height < MIN_IMAGE_HEIGHT:
                continue
            if width * height < MIN_IMAGE_AREA:
                continue

            image_bytes = base_image["image"]

            # Skip duplicate content (same image re-embedded with different xref)
            content_hash = hashlib.md5(image_bytes).hexdigest()
            if content_hash in seen_hashes:
                continue
            seen_hashes.add(content_hash)

            ext = base_image.get("ext", "png")

            fig_counter += 1
            fig_id = f"fig_{fig_counter:04d}"
            image_filename = f"{fig_id}.{ext}"
            image_path = figures_dir / image_filename

            # Try to find the image's position on the page
            bbox = _find_image_bbox(page, xref)

            # Skip full-page text scans from scanned PDFs; these are page screenshots,
            # not meaningful figures, and should not go through figure enrichment.
            if bbox and _is_near_full_page_text_scan(page, bbox):
                continue

            image_path.write_bytes(image_bytes)

            figures.append(Figure(
                figure_id=fig_id,
                source_page=page_num + 1,
                image_path=f"figures/{image_filename}",
                bbox=bbox,
                extraction_method="embedded",
            ))

    doc.close()
    return figures


def _find_image_bbox(page: fitz.Page, xref: int) -> list[float]:
    """Try to find the bounding box of an image on a page by its xref."""
    for img in page.get_images(full=True):
        if img[0] == xref:
            # Get image rects - returns list of Rect where this image appears
            rects = page.get_image_rects(img)
            if rects:
                r = rects[0]
                return [r.x0, r.y0, r.x1, r.y1]
    return []


def rasterize_pages(
    pdf_path: Path,
    output_dir: Path,
    dpi: int = 200,
    existing_figures: list[Figure] | None = None,
) -> list[Figure]:
    """Rasterize PDF pages and detect visual regions that aren't embedded images.

    This catches vector drawings, composite layouts, diagrams, and other
    page-native graphics that aren't extractable as embedded images.

    Strategy:
    - Rasterize each page at the given DPI
    - Detect regions with significant visual content but little text
    - Crop and save those regions as separate figures
    - Skip regions that overlap with already-extracted embedded images
    """
    figures_dir = output_dir / "figures"
    figures_dir.mkdir(parents=True, exist_ok=True)

    doc = fitz.open(str(pdf_path))
    figures: list[Figure] = []

    # Build a set of existing figure bboxes per page for overlap detection
    existing_bboxes: dict[int, list[list[float]]] = {}
    if existing_figures:
        for fig in existing_figures:
            if fig.bbox:
                existing_bboxes.setdefault(fig.source_page, []).append(fig.bbox)

    # Count existing figures to continue numbering
    fig_offset = len(existing_figures) if existing_figures else 0

    # Track content hashes to skip duplicate rasterizations (e.g. watermarks)
    seen_hashes: set[str] = set()

    for page_num in range(len(doc)):
        page = doc[page_num]

        # Detect drawing-heavy regions on this page
        regions = _detect_visual_regions(page)

        page_existing = existing_bboxes.get(page_num + 1, [])

        for region_rect in regions:
            # Skip if this region overlaps significantly with an existing embedded image
            if _overlaps_existing(region_rect, page_existing):
                continue

            # Rasterize just this region
            clip = fitz.Rect(region_rect)
            mat = fitz.Matrix(dpi / 72, dpi / 72)
            pix = page.get_pixmap(matrix=mat, clip=clip)

            # Skip if too small after rasterization
            if pix.width < MIN_IMAGE_WIDTH or pix.height < MIN_IMAGE_HEIGHT:
                continue

            # Skip duplicate content (repeated watermarks, headers, footers)
            content_hash = hashlib.md5(pix.samples).hexdigest()
            if content_hash in seen_hashes:
                continue
            seen_hashes.add(content_hash)

            fig_offset += 1
            fig_id = f"fig_{fig_offset:04d}"
            image_filename = f"{fig_id}.png"
            image_path = figures_dir / image_filename

            pix.save(str(image_path))

            figures.append(Figure(
                figure_id=fig_id,
                source_page=page_num + 1,
                image_path=f"figures/{image_filename}",
                bbox=list(region_rect),
                extraction_method="rasterized_region",
            ))

    doc.close()
    return figures


def _detect_visual_regions(page: fitz.Page) -> list[list[float]]:
    """Detect regions on a page that contain significant drawings/graphics.

    Uses PyMuPDF's drawing extraction to find areas with vector content.
    Groups nearby drawings into regions.
    """
    drawings = page.get_drawings()
    if not drawings:
        return []

    # Collect bounding boxes of all drawing elements
    draw_rects: list[fitz.Rect] = []
    for drawing in drawings:
        rect = fitz.Rect(drawing["rect"])
        # Skip tiny decorative elements (lines, dots)
        if rect.width < 20 or rect.height < 20:
            continue
        draw_rects.append(rect)

    if not draw_rects:
        return []

    # Cluster nearby drawing rects into regions
    regions = _cluster_rects(draw_rects, margin=20)

    # Filter: only keep regions that are large enough to be meaningful
    page_area = page.rect.width * page.rect.height
    meaningful = []
    for region in regions:
        r = fitz.Rect(region)
        area = r.width * r.height
        # Region should be at least 5% of page area
        if area >= page_area * 0.05:
            meaningful.append(region)

    return meaningful


def _cluster_rects(rects: list[fitz.Rect], margin: float = 20) -> list[list[float]]:
    """Cluster overlapping/nearby rectangles into merged regions."""
    if not rects:
        return []

    # Start with each rect as its own cluster
    clusters = [[r.x0 - margin, r.y0 - margin, r.x1 + margin, r.y1 + margin] for r in rects]

    # Merge overlapping clusters iteratively
    changed = True
    while changed:
        changed = False
        merged = []
        used = [False] * len(clusters)

        for i in range(len(clusters)):
            if used[i]:
                continue
            current = list(clusters[i])
            for j in range(i + 1, len(clusters)):
                if used[j]:
                    continue
                if _rects_overlap(current, clusters[j]):
                    # Merge
                    current[0] = min(current[0], clusters[j][0])
                    current[1] = min(current[1], clusters[j][1])
                    current[2] = max(current[2], clusters[j][2])
                    current[3] = max(current[3], clusters[j][3])
                    used[j] = True
                    changed = True
            merged.append(current)
            used[i] = True

        clusters = merged

    return clusters


def _rects_overlap(a: list[float], b: list[float]) -> bool:
    """Check if two rectangles [x0, y0, x1, y1] overlap."""
    return not (a[2] < b[0] or b[2] < a[0] or a[3] < b[1] or b[3] < a[1])


def _overlaps_existing(region: list[float], existing: list[list[float]], threshold: float = 0.5) -> bool:
    """Check if a region overlaps significantly with any existing figure bbox."""
    r = fitz.Rect(region)
    for ex in existing:
        ex_rect = fitz.Rect(ex)
        intersection = r & ex_rect
        if intersection.is_empty:
            continue
        overlap_area = intersection.width * intersection.height
        region_area = r.width * r.height
        if region_area > 0 and overlap_area / region_area > threshold:
            return True
    return False


def extract_all_figures(
    pdf_path: Path,
    output_dir: Path,
    dpi: int = 200,
    extract_embedded: bool = True,
    extract_rasterized: bool = True,
) -> list[Figure]:
    """Extract all figures from a PDF using both strategies.

    Args:
        pdf_path: Path to the PDF file.
        output_dir: The material's output directory (extracted/<material_id>/).
        dpi: Resolution for page rasterization.
        extract_embedded: Whether to extract embedded images.
        extract_rasterized: Whether to rasterize and detect visual regions.

    Returns:
        List of all extracted Figure objects.
    """
    all_figures: list[Figure] = []

    if extract_embedded:
        embedded = extract_embedded_images(pdf_path, output_dir)
        all_figures.extend(embedded)

    if extract_rasterized:
        rasterized = rasterize_pages(pdf_path, output_dir, dpi=dpi, existing_figures=all_figures)
        all_figures.extend(rasterized)

    # Write figure sidecars
    for fig in all_figures:
        sidecar_path = output_dir / "figures" / f"{fig.figure_id}.json"
        sidecar_path.write_text(
            json.dumps(fig.to_dict(), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    return all_figures
