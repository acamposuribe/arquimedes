"""Tests for deterministic document classification."""

from __future__ import annotations

from arquimedes.classify import classify_document_type
from arquimedes.models import Page


class TestClassifyDocumentType:
    def test_returns_unknown_when_unclear(self):
        pages = [Page(page_number=1, text="This is a short ambiguous page.")]
        assert classify_document_type(pages, filename="mystery.pdf") == "unknown"

    def test_detects_strong_paper_cues(self):
        pages = [
            Page(
                page_number=1,
                text="Abstract. Keywords: architecture. Introduction. References. DOI.",
            )
        ]
        assert classify_document_type(pages, filename="paper.pdf") == "paper"
