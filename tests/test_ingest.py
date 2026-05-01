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


def test_ingest_accepts_multiple_explicit_paths(tmp_path, monkeypatch):
    library_root = tmp_path / "library"
    project_root = tmp_path / "project"
    manifest_dir = project_root / "manifests"
    manifest_dir.mkdir(parents=True, exist_ok=True)

    first = library_root / "Research" / "papers" / "one.pdf"
    second = library_root / "Practice" / "notes" / "two.pdf"
    first.parent.mkdir(parents=True, exist_ok=True)
    second.parent.mkdir(parents=True, exist_ok=True)
    first.write_bytes(b"one")
    second.write_bytes(b"two")

    monkeypatch.setattr(ingest_mod, "get_library_root", lambda _config=None: library_root)
    monkeypatch.setattr(ingest_mod, "get_project_root", lambda: project_root)

    result = ingest_mod.ingest(path=[str(first), str(second)], config={})

    assert {item.relative_path for item in result} == {
        "Research/papers/one.pdf",
        "Practice/notes/two.pdf",
    }


def test_ingest_skips_disabled_domains(tmp_path, monkeypatch):
    library_root = tmp_path / "library"
    project_root = tmp_path / "project"
    (project_root / "manifests").mkdir(parents=True, exist_ok=True)

    project_file = library_root / "Proyectos" / "2407-casa-rio" / "acta.pdf"
    research_file = library_root / "Research" / "papers" / "paper.pdf"
    project_file.parent.mkdir(parents=True, exist_ok=True)
    research_file.parent.mkdir(parents=True, exist_ok=True)
    project_file.write_bytes(b"acta")
    research_file.write_bytes(b"paper")

    monkeypatch.setattr(ingest_mod, "get_library_root", lambda _config=None: library_root)
    monkeypatch.setattr(ingest_mod, "get_project_root", lambda: project_root)

    result = ingest_mod.ingest(config={"domains": {"enabled": ["proyectos"]}})

    assert [item.relative_path for item in result] == ["Proyectos/2407-casa-rio/acta.pdf"]


def test_ingest_recognizes_proyectos_domain_and_general_bucket(tmp_path, monkeypatch):
    library_root = tmp_path / "library"
    project_root = tmp_path / "project"
    manifest_dir = project_root / "manifests"
    manifest_dir.mkdir(parents=True, exist_ok=True)

    project_file = library_root / "Proyectos" / "2407-casa-rio" / "acta.pdf"
    loose_file = library_root / "Proyectos" / "loose.pdf"
    project_file.parent.mkdir(parents=True, exist_ok=True)
    loose_file.parent.mkdir(parents=True, exist_ok=True)
    project_file.write_bytes(b"acta")
    loose_file.write_bytes(b"loose")

    monkeypatch.setattr(ingest_mod, "get_library_root", lambda _config=None: library_root)
    monkeypatch.setattr(ingest_mod, "get_project_root", lambda: project_root)

    result = ingest_mod.ingest(config={})

    by_path = {item.relative_path: item for item in result}
    assert by_path["Proyectos/2407-casa-rio/acta.pdf"].domain == "proyectos"
    assert by_path["Proyectos/2407-casa-rio/acta.pdf"].collection == "2407-casa-rio"
    assert by_path["Proyectos/loose.pdf"].domain == "proyectos"
    assert by_path["Proyectos/loose.pdf"].collection == "_general"
