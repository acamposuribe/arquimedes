"""Tests for figure extraction heuristics and deduplication."""

from unittest.mock import MagicMock, patch
from pathlib import Path

from arquimedes.extract_figures import extract_embedded_images, rasterize_pages
from arquimedes.models import Figure


def _make_fake_doc(pages_images):
    """Create a mock fitz document with given pages and images.

    pages_images: list of lists, each inner list is
        [(xref, image_bytes, width, height, ext)] for that page.
    """
    doc = MagicMock()
    doc.__len__ = lambda self: len(pages_images)

    pages = []
    for page_imgs in pages_images:
        page = MagicMock()
        img_list = [(xref, 0, 0, 0, 0, 0, 0, 0, 0, 0) for xref, *_ in page_imgs]
        page.get_images.return_value = img_list
        page.get_image_rects.return_value = []
        page.get_text.return_value = ""
        page.rect.width = 500
        page.rect.height = 800
        pages.append(page)

    doc.__iter__ = lambda self: iter(range(len(pages)))

    def getitem(self, idx):
        return pages[idx]

    doc.__getitem__ = getitem

    image_map = {}
    for page_imgs in pages_images:
        for xref, img_bytes, w, h, ext in page_imgs:
            image_map[xref] = {
                "image": img_bytes,
                "width": w,
                "height": h,
                "ext": ext,
            }

    doc.extract_image.side_effect = lambda x: image_map.get(x)
    return doc


@patch("arquimedes.extract_figures.fitz")
def test_embedded_dedup_by_content_hash(mock_fitz, tmp_path):
    """Different xrefs with identical bytes should produce only one figure."""
    same_bytes = b"\x89PNG" + b"\x00" * 5000
    doc = _make_fake_doc([
        [(10, same_bytes, 200, 200, "png")],
        [(20, same_bytes, 200, 200, "png")],  # different xref, same content
        [(30, b"\x89PNG" + b"\xff" * 5000, 200, 200, "png")],  # unique
    ])
    mock_fitz.open.return_value = doc

    figures = extract_embedded_images(Path("fake.pdf"), tmp_path)

    assert len(figures) == 2
    assert figures[0].figure_id == "fig_0001"
    assert figures[1].figure_id == "fig_0002"


@patch("arquimedes.extract_figures.fitz")
def test_skips_full_page_text_scan_embedded_images(mock_fitz, tmp_path):
    """Near-full-page embedded scans on text-heavy pages should not become figures."""
    img_bytes = b"\x89PNG" + b"\x00" * 5000
    doc = _make_fake_doc([
        [(10, img_bytes, 500, 800, "png")],
    ])
    page = doc.__getitem__(0)
    page.get_image_rects.return_value = [MagicMock(x0=0.0, y0=0.0, x1=500.0, y1=800.0)]
    page.get_text.return_value = "A" * 1000
    page.rect.width = 500
    page.rect.height = 800
    mock_fitz.open.return_value = doc

    figures = extract_embedded_images(Path("fake.pdf"), tmp_path)

    assert figures == []
    assert not list((tmp_path / "figures").glob("*.png"))
