"""Tests for deterministic cleanup of extracted OCR / PDF text."""

from __future__ import annotations

from arquimedes.models import Page
from arquimedes.text_normalization import (
    normalize_extracted_pages,
    normalize_extracted_text,
    normalize_text_line,
)


def test_normalize_text_line_repairs_unicode_and_spacing():
    assert normalize_text_line("archi­tec­ture  and  knowl­edge") == "architecture and knowledge"


def test_normalize_extracted_text_preserves_paragraph_structure():
    text = "The archi-\ntectural archive stores\nknowledge.\n\nNew paragraph here."
    normalized = normalize_extracted_text(text)
    assert normalized == "The architectural archive stores knowledge.\n\nNew paragraph here."


def test_normalize_extracted_text_breaks_on_period_ended_lines():
    text = "First line.\nSecond line continues."
    normalized = normalize_extracted_text(text)
    assert normalized == "First line.\n\nSecond line continues."


def test_normalize_extracted_pages_removes_running_headers_and_page_numbers():
    pages = [
        Page(
            page_number=1,
            text="Project MUSE\nA title line\n\nA long para-\ngraph continues here.\n526",
            headings=["Project MUSE"],
        ),
        Page(
            page_number=2,
            text="Project MUSE\nAnother page\n\nMore text appears here.\n527",
            headings=["Project MUSE"],
        ),
    ]

    normalized_pages, full_text = normalize_extracted_pages(pages)

    assert "Project MUSE" not in full_text
    assert "526" not in full_text
    assert "527" not in full_text
    assert normalized_pages[0].text == "A title line\n\nA long paragraph continues here."
    assert normalized_pages[1].text == "Another page\n\nMore text appears here."
    assert "A long paragraph continues here." in full_text
    assert "More text appears here." in full_text
