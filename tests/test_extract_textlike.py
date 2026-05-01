from __future__ import annotations

import json
from pathlib import Path

from arquimedes.extract_text import (
    _pack_paragraphs_into_pages,
    _split_markdown_by_headings,
    extract_raw_markdown_file,
    extract_raw_text_file,
)


def _read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def _manifest_entry(rel: str) -> dict:
    return {
        "file_hash": "deadbeef",
        "relative_path": rel,
        "domain": "research",
        "collection": "notes",
        "ingested_at": "2026-05-02T00:00:00+00:00",
    }


def test_pack_paragraphs_respects_target():
    paras = ["a" * 100, "b" * 100, "c" * 100]
    pages = _pack_paragraphs_into_pages(paras, target_chars=210)
    assert len(pages) == 2
    assert "a" * 100 in pages[0]
    assert "c" * 100 in pages[1]


def test_split_markdown_by_headings_preserves_fences():
    text = "# Intro\nbody\n\n```\n# not a heading\n```\n\n## Next\ntail\n"
    sections = _split_markdown_by_headings(text)
    headings = [h for h, _ in sections]
    assert headings == ["Intro", "Next"]
    bodies = [b for _, b in sections]
    assert "# not a heading" in bodies[0]


def test_extract_raw_text_file_writes_artifacts(tmp_path):
    src = tmp_path / "note.txt"
    src.write_text("First paragraph.\n\nSecond paragraph.\n", encoding="utf-8")
    out = tmp_path / "extracted" / "mid"
    out.mkdir(parents=True)

    meta = extract_raw_text_file(src, out, "mid", _manifest_entry("Research/notes/note.txt"))

    assert (out / "meta.json").exists()
    assert (out / "text.md").exists()
    pages = _read_jsonl(out / "pages.jsonl")
    chunks = _read_jsonl(out / "chunks.jsonl")
    assert len(pages) >= 1
    assert pages[0]["page_number"] == 1
    assert "First paragraph" in pages[0]["text"]
    assert chunks
    meta_data = json.loads((out / "meta.json").read_text())
    assert meta_data["file_type"] == "text"
    assert meta_data["page_count"] == len(pages)
    assert meta.page_count == len(pages)


def test_extract_raw_text_file_handles_invalid_utf8(tmp_path):
    src = tmp_path / "note.txt"
    src.write_bytes(b"valid \xff bytes here\n")
    out = tmp_path / "extracted" / "mid"
    out.mkdir(parents=True)

    extract_raw_text_file(src, out, "mid", _manifest_entry("Research/notes/note.txt"))
    warnings_path = out / "extraction_warnings.jsonl"
    assert warnings_path.exists()
    warnings = _read_jsonl(warnings_path)
    assert any("non-utf8" in w["message"] for w in warnings)


def test_extract_raw_markdown_splits_by_headings(tmp_path):
    src = tmp_path / "spec.md"
    src.write_text(
        "# First\nalpha line\n\n## Sub\nstill first\n\n# Second\nbeta line\n",
        encoding="utf-8",
    )
    out = tmp_path / "extracted" / "mid"
    out.mkdir(parents=True)

    extract_raw_markdown_file(src, out, "mid", _manifest_entry("Research/notes/spec.md"))
    pages = _read_jsonl(out / "pages.jsonl")
    headings = [p["headings"] for p in pages]
    # H1 splits create top-level pages; H2 also splits in our impl.
    assert ["First"] in headings
    assert ["Second"] in headings
    meta = json.loads((out / "meta.json").read_text())
    assert meta["file_type"] == "markdown"
    assert meta["title"] == "First"
