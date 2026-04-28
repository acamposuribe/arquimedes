"""Tests for PDF extraction helpers and OCR fallback."""

from pathlib import Path

from arquimedes.extract_pdf import (
    _clean_annotation_quote,
    _sanitize_strings,
    extract_text_and_pages,
)


def test_clean_annotation_quote_drops_short_artifact_lines():
    raw = "buildings\np\ny\np\n,\nwith their potential for extreme longevity"
    cleaned = _clean_annotation_quote(raw)

    assert cleaned == "buildings with their potential for extreme longevity"


def test_clean_annotation_quote_preserves_leading_capital_glyph():
    raw = "A\nrchitectures of the past should survive"
    cleaned = _clean_annotation_quote(raw)

    assert cleaned == "Architectures of the past should survive"


def test_clean_annotation_quote_collapses_spacing():
    raw = "I argue\n\nfor a more expansive   understanding"
    cleaned = _clean_annotation_quote(raw)

    assert cleaned == "I argue for a more expansive understanding"


def test_sanitize_strings_removes_null_bytes_recursively():
    raw = {
        "text": "Hel\x00lo",
        "items": ["A\x00", {"nested": "B\x00C"}],
    }

    cleaned = _sanitize_strings(raw)

    assert cleaned == {
        "text": "Hello",
        "items": ["A", {"nested": "BC"}],
    }


class _FakePage:
    def __init__(self, text: str):
        self._text = text

    def get_text(self, mode: str, **kwargs):
        if mode == "text":
            return self._text
        if mode == "dict":
            return {"blocks": []}
        raise AssertionError(f"unexpected get_text mode: {mode}")


class _FakeDoc:
    def __init__(self, texts: list[str]):
        self._pages = [_FakePage(text) for text in texts]

    def __len__(self):
        return len(self._pages)

    def __getitem__(self, index: int):
        return self._pages[index]

    def get_toc(self):
        return []

    def close(self):
        return None


def test_extract_text_and_pages_uses_ocr_when_pdf_text_layer_is_empty(monkeypatch):
    monkeypatch.setattr(
        "arquimedes.extract_pdf.fitz.open",
        lambda path: _FakeDoc(["", ""]),
    )
    monkeypatch.setattr("arquimedes.extract_pdf._tesseract_available", lambda: True)

    ocr_texts = ["OCR page one", "OCR page two"]
    calls: list[int] = []

    def fake_ocr_page(page, dpi=300):
        calls.append(dpi)
        return ocr_texts.pop(0)

    monkeypatch.setattr("arquimedes.extract_pdf._ocr_page", fake_ocr_page)

    pages, toc = extract_text_and_pages(Path("/tmp/scan.pdf"), ocr_fallback=True)

    assert toc == []
    assert [page.text for page in pages] == ["OCR page one", "OCR page two"]
    assert calls == [300, 300]


def test_extract_text_and_pages_skips_ocr_when_pdf_text_exists(monkeypatch):
    monkeypatch.setattr(
        "arquimedes.extract_pdf.fitz.open",
        lambda path: _FakeDoc(["Already extracted text"]),
    )

    called = False

    def fake_ocr_page(page, dpi=300):
        nonlocal called
        called = True
        return "unexpected"

    monkeypatch.setattr("arquimedes.extract_pdf._ocr_page", fake_ocr_page)

    pages, _ = extract_text_and_pages(Path("/tmp/digital.pdf"), ocr_fallback=True)

    assert [page.text for page in pages] == ["Already extracted text"]
    assert called is False
