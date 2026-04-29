"""Tests for the OpenDataLoader PDF adapter."""

from __future__ import annotations

from pathlib import Path

from arquimedes.extract import _should_use_opendataloader
from arquimedes.extract_opendataloader import _pages_from_opendataloader


def test_pages_from_opendataloader_maps_existing_page_contract():
    data = {
        "number of pages": 2,
        "kids": [
            {
                "type": "heading",
                "page number": 1,
                "heading level": 1,
                "content": "Main Title",
            },
            {
                "type": "header",
                "page number": 1,
                "kids": [
                    {
                        "type": "heading",
                        "page number": 1,
                        "heading level": 4,
                        "content": "Running Header",
                    }
                ],
            },
            {
                "type": "paragraph",
                "page number": 1,
                "content": "Opening paragraph.",
            },
            {
                "type": "list",
                "page number": 1,
                "list items": [
                    {
                        "type": "list item",
                        "page number": 1,
                        "content": "1. First item",
                    }
                ],
            },
            {
                "type": "caption",
                "page number": 2,
                "content": "A useful figure caption.",
            },
        ],
    }

    pages = _pages_from_opendataloader(data)

    assert len(pages) == 2
    assert pages[0].page_number == 1
    assert pages[0].headings == ["Main Title"]
    assert pages[0].section_boundaries == ["Main Title"]
    assert pages[0].text == "Main Title\n\nOpening paragraph.\n\n1. First item"
    assert pages[1].text == "Caption: A useful figure caption."


def test_should_use_opendataloader_auto_falls_back_without_package(monkeypatch):
    monkeypatch.setattr(
        "arquimedes.extract.pdf_has_usable_text_layer",
        lambda path: True,
    )

    real_import = __import__

    def fake_import(name, *args, **kwargs):
        if name == "opendataloader_pdf":
            raise ImportError("missing")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr("builtins.__import__", fake_import)

    assert _should_use_opendataloader(Path("/tmp/sample.pdf"), "auto") is False


def test_should_use_opendataloader_skips_scanned_pdf(monkeypatch):
    monkeypatch.setattr(
        "arquimedes.extract.pdf_has_usable_text_layer",
        lambda path: False,
    )

    assert _should_use_opendataloader(Path("/tmp/scan.pdf"), "auto") is False
