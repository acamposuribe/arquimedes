from __future__ import annotations

import json

from arquimedes import ingest as ingest_mod
from arquimedes.models import MaterialManifest, MaterialMeta, compute_file_hash, compute_material_id


def test_ingest_rehomes_existing_material_and_refreshes_extracted_scope(tmp_path, monkeypatch):
    library_root = tmp_path / "library"
    project_root = tmp_path / "project"
    moved_path = library_root / "Practice" / "codes" / "doc.pdf"
    moved_path.parent.mkdir(parents=True, exist_ok=True)
    moved_path.write_bytes(b"same-content")

    material_id = compute_material_id(moved_path)
    file_hash = compute_file_hash(moved_path)
    manifest_dir = project_root / "manifests"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    (manifest_dir / "materials.jsonl").write_text(
        MaterialManifest(
            material_id=material_id,
            file_hash=file_hash,
            relative_path="Research/old/doc.pdf",
            file_type="pdf",
            domain="research",
            collection="old",
            ingested_at="2026-01-01T00:00:00+00:00",
        ).to_json_line() + "\n",
        encoding="utf-8",
    )

    extracted_dir = project_root / "extracted" / material_id
    extracted_dir.mkdir(parents=True, exist_ok=True)
    MaterialMeta(
        material_id=material_id,
        file_hash=file_hash,
        source_path="Research/old/doc.pdf",
        title="Doc",
        file_type="pdf",
        domain="research",
        collection="old",
        ingested_at="2026-01-01T00:00:00+00:00",
    ).save(project_root / "extracted")

    monkeypatch.setattr(ingest_mod, "get_library_root", lambda _config=None: library_root)
    monkeypatch.setattr(ingest_mod, "get_project_root", lambda: project_root)

    result = ingest_mod.ingest(config={})

    assert result == []
    manifest_row = json.loads((manifest_dir / "materials.jsonl").read_text(encoding="utf-8").strip())
    assert manifest_row["relative_path"] == "Practice/codes/doc.pdf"
    assert manifest_row["domain"] == "practice"
    assert manifest_row["collection"] == "codes"

    meta = json.loads((extracted_dir / "meta.json").read_text(encoding="utf-8"))
    assert meta["source_path"] == "Practice/codes/doc.pdf"
    assert meta["domain"] == "practice"
    assert meta["collection"] == "codes"
