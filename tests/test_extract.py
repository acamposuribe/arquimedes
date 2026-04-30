"""Tests for extract-raw orchestration."""

import json
from pathlib import Path

from arquimedes.extract import _extract_pdf_material, extract_raw
from arquimedes.models import MaterialManifest


def _write_manifest(project_root: Path, entries: list[MaterialManifest]) -> None:
    manifest_dir = project_root / "manifests"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    (manifest_dir / "materials.jsonl").write_text(
        "".join(entry.to_json_line() + "\n" for entry in entries),
        encoding="utf-8",
    )


def test_extract_raw_domain_filter_and_force_recreate(monkeypatch, tmp_path):
    project_root = tmp_path / "vault"
    library_root = tmp_path / "library"
    (library_root / "Research" / "refs").mkdir(parents=True)
    (library_root / "Proyectos" / "job").mkdir(parents=True)
    (library_root / "Research" / "refs" / "r.pdf").write_bytes(b"pdf")
    (library_root / "Proyectos" / "job" / "p.jpg").write_bytes(b"jpg")

    _write_manifest(project_root, [
        MaterialManifest("research1", "hash-r", "Research/refs/r.pdf", "pdf", "research", "refs", "now"),
        MaterialManifest("project1", "hash-p", "Proyectos/job/p.jpg", "image", "proyectos", "job", "now"),
    ])
    stale_dir = project_root / "extracted" / "project1"
    stale_dir.mkdir(parents=True)
    (stale_dir / "stale.txt").write_text("remove me", encoding="utf-8")

    def fake_extract_image(image_path, output_dir, material_id, manifest_entry, ocr_fallback=True):
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "meta.json").write_text(json.dumps({"material_id": material_id}), encoding="utf-8")
        (output_dir / "pages.jsonl").write_text("", encoding="utf-8")

    monkeypatch.setattr("arquimedes.extract.get_project_root", lambda: project_root)
    monkeypatch.setattr("arquimedes.extract.extract_raw_image", fake_extract_image)

    extracted = extract_raw(config={"library_root": str(library_root)}, force=True, domain="proyectos")

    assert extracted == ["project1"]
    assert not (stale_dir / "stale.txt").exists()
    assert (stale_dir / "meta.json").exists()
    assert not (project_root / "extracted" / "research1").exists()


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
