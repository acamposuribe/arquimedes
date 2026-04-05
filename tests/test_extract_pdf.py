"""Tests for PDF extraction sanitization helpers."""

from arquimedes.extract_pdf import _clean_annotation_quote, _sanitize_strings


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
