"""Tests for extract-raw orchestration."""

from pathlib import Path

from arquimedes.extract import _extract_pdf_material


def test_extract_pdf_material_forwards_ocr_fallback(monkeypatch, tmp_path):
    output_dir = tmp_path / "out"
    output_dir.mkdir()

    recorded: dict[str, object] = {}

    def fake_extract_raw_pdf(pdf_path, out_dir, material_id, manifest_entry, ocr_fallback=True):
        recorded["ocr_fallback"] = ocr_fallback
        recorded["material_id"] = material_id
        (out_dir / "pages.jsonl").write_text(
            '{"page_number": 1, "text": "OCR recovered text", "footnote_text": "", '
            '"headings": [], "section_boundaries": [], "figure_refs": [], "table_refs": [], '
            '"thumbnail_path": "", "has_annotations": false, "annotation_ids": []}\n',
            encoding="utf-8",
        )

    monkeypatch.setattr("arquimedes.extract.extract_raw_pdf", fake_extract_raw_pdf)
    monkeypatch.setattr("arquimedes.extract.extract_all_figures", lambda *args, **kwargs: [])
    monkeypatch.setattr("arquimedes.extract.generate_thumbnails", lambda *args, **kwargs: [])

    _extract_pdf_material(
        Path("/tmp/sample.pdf"),
        output_dir,
        "mat_001",
        {"relative_path": "sample.pdf"},
        ocr_fallback=True,
        generate_thumbs=False,
    )

    chunks = (output_dir / "chunks.jsonl").read_text(encoding="utf-8")

    assert recorded == {"ocr_fallback": True, "material_id": "mat_001"}
    assert "OCR recovered text" in chunks
