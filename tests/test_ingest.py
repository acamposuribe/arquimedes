from __future__ import annotations

import json
import os

import pytest

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


def test_detect_file_type_for_new_extensions(tmp_path):
    samples = {
        "note.txt": "text",
        "readme.md": "markdown",
        "guide.markdown": "markdown",
        "report.docx": "docx",
        "deck.pptx": "pptx",
        "budget.xlsx": "xlsx",
        "MixedCase.Docx": "docx",
        "legacy.doc": "unknown",
        "legacy.ppt": "unknown",
        "legacy.xls": "unknown",
    }
    for name, expected in samples.items():
        p = tmp_path / name
        p.write_bytes(b"x")
        assert ingest_mod._detect_file_type(p) == expected, name


def test_ingest_registers_new_file_types(tmp_path, monkeypatch):
    library_root = tmp_path / "library"
    project_root = tmp_path / "project"
    (project_root / "manifests").mkdir(parents=True, exist_ok=True)

    files = {
        "Research/notes/log.txt": b"hello",
        "Research/notes/spec.md": b"# spec md",
        "Practice/codes/spec.markdown": b"# spec markdown",
        "Research/papers/draft.docx": b"docx-bytes",
        "Practice/decks/deck.pptx": b"pptx-bytes",
        "Practice/sheets/budget.xlsx": b"xlsx-bytes",
        "Research/notes/MixedCase.Docx": b"mixed-docx-bytes",
        "Research/legacy/old.doc": b"legacy",
    }
    for rel, data in files.items():
        p = library_root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)

    monkeypatch.setattr(ingest_mod, "get_library_root", lambda _config=None: library_root)
    monkeypatch.setattr(ingest_mod, "get_project_root", lambda: project_root)

    result = ingest_mod.ingest(config={})
    by_path = {item.relative_path: item.file_type for item in result}

    assert by_path["Research/notes/log.txt"] == "text"
    assert by_path["Research/notes/spec.md"] == "markdown"
    assert by_path["Practice/codes/spec.markdown"] == "markdown"
    assert by_path["Research/papers/draft.docx"] == "docx"
    assert by_path["Practice/decks/deck.pptx"] == "pptx"
    assert by_path["Practice/sheets/budget.xlsx"] == "xlsx"
    assert by_path["Research/notes/MixedCase.Docx"] == "docx"
    assert "Research/legacy/old.doc" not in by_path


def test_ingest_can_ignore_configured_file_extensions(tmp_path, monkeypatch):
    library_root = tmp_path / "library"
    project_root = tmp_path / "project"
    (project_root / "manifests").mkdir(parents=True, exist_ok=True)

    files = {
        "Research/papers/draft.docx": b"docx-bytes",
        "Research/papers/spec.pdf": b"pdf-bytes",
        "Research/papers/notes.MD": b"markdown-bytes",
    }
    for rel, data in files.items():
        p = library_root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)

    monkeypatch.setattr(ingest_mod, "get_library_root", lambda _config=None: library_root)
    monkeypatch.setattr(ingest_mod, "get_project_root", lambda: project_root)

    result = ingest_mod.ingest(config={"ingest": {"ignore_extensions": ["docx", ".md"]}})

    by_path = {item.relative_path: item.file_type for item in result}
    assert by_path == {"Research/papers/spec.pdf": "pdf"}


def test_ingest_ignores_configured_extension_for_explicit_file(tmp_path, monkeypatch):
    library_root = tmp_path / "library"
    project_root = tmp_path / "project"
    (project_root / "manifests").mkdir(parents=True, exist_ok=True)
    docx = library_root / "Research" / "papers" / "draft.docx"
    docx.parent.mkdir(parents=True, exist_ok=True)
    docx.write_bytes(b"docx-bytes")

    monkeypatch.setattr(ingest_mod, "get_library_root", lambda _config=None: library_root)
    monkeypatch.setattr(ingest_mod, "get_project_root", lambda: project_root)

    result = ingest_mod.ingest(path="Research/papers/draft.docx", config={"ingest": {"ignore_extensions": [".docx"]}})

    assert result == []


def test_prune_missing_materials_removes_deleted_sources(tmp_path, monkeypatch):
    library_root = tmp_path / "library"
    project_root = tmp_path / "project"
    manifest_dir = project_root / "manifests"
    manifest_dir.mkdir(parents=True, exist_ok=True)

    kept = library_root / "Research" / "papers" / "kept.pdf"
    kept.parent.mkdir(parents=True, exist_ok=True)
    kept.write_bytes(b"kept")
    missing = library_root / "Research" / "papers" / "missing.pdf"

    kept_entry = MaterialManifest(
        material_id="kept",
        file_hash="hash-kept",
        relative_path="Research/papers/kept.pdf",
        file_type="pdf",
        domain="research",
        collection="papers",
        ingested_at="2026-01-01T00:00:00+00:00",
    )
    missing_entry = MaterialManifest(
        material_id="missing",
        file_hash="hash-missing",
        relative_path="Research/papers/missing.pdf",
        file_type="pdf",
        domain="research",
        collection="papers",
        ingested_at="2026-01-01T00:00:00+00:00",
    )
    (manifest_dir / "materials.jsonl").write_text(
        kept_entry.to_json_line() + "\n" + missing_entry.to_json_line() + "\n",
        encoding="utf-8",
    )
    (project_root / "extracted" / "missing").mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(ingest_mod, "get_library_root", lambda _config=None: library_root)
    monkeypatch.setattr(ingest_mod, "get_project_root", lambda: project_root)

    report = ingest_mod.prune_missing_materials(config={})

    assert report.removed_material_ids == ["missing"]
    manifest_rows = (manifest_dir / "materials.jsonl").read_text(encoding="utf-8")
    assert "kept" in manifest_rows
    assert "missing" not in manifest_rows
    assert not (project_root / "extracted" / "missing").exists()


def test_ingest_ignores_files_inside_previos_folders_case_insensitively(tmp_path, monkeypatch):
    library_root = tmp_path / "library"
    project_root = tmp_path / "project"
    (project_root / "manifests").mkdir(parents=True, exist_ok=True)

    files = {
        "Proyectos/2407-casa-rio/Planos/actual.pdf": b"actual",
        "Proyectos/2407-casa-rio/Previos/old.pdf": b"old",
        "Proyectos/2407-casa-rio/01_previos_entrega/old2.pdf": b"old2",
        "Proyectos/2407-casa-rio/PREVIOS/sub/old3.pdf": b"old3",
    }
    for rel, data in files.items():
        p = library_root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)

    monkeypatch.setattr(ingest_mod, "get_library_root", lambda _config=None: library_root)
    monkeypatch.setattr(ingest_mod, "get_project_root", lambda: project_root)

    result = ingest_mod.ingest(config={})

    assert [item.relative_path for item in result] == ["Proyectos/2407-casa-rio/Planos/actual.pdf"]


def test_ingest_ignores_explicit_file_inside_previos_folder(tmp_path, monkeypatch):
    library_root = tmp_path / "library"
    project_root = tmp_path / "project"
    (project_root / "manifests").mkdir(parents=True, exist_ok=True)
    old_file = library_root / "Proyectos" / "2407-casa-rio" / "Previos" / "old.pdf"
    old_file.parent.mkdir(parents=True, exist_ok=True)
    old_file.write_bytes(b"old")

    monkeypatch.setattr(ingest_mod, "get_library_root", lambda _config=None: library_root)
    monkeypatch.setattr(ingest_mod, "get_project_root", lambda: project_root)

    result = ingest_mod.ingest(path="Proyectos/2407-casa-rio/Previos/old.pdf", config={})

    assert result == []


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


def test_ingest_follows_symlinked_project_directories(tmp_path, monkeypatch):
    if not hasattr(os, "symlink"):
        pytest.skip("symlinks are not supported on this platform")

    library_root = tmp_path / "library"
    project_root = tmp_path / "project"
    external_root = tmp_path / "server" / "Casa Rio" / "Entregas"
    project_dir = library_root / "Proyectos" / "2407-casa-rio"
    (project_root / "manifests").mkdir(parents=True, exist_ok=True)
    external_root.mkdir(parents=True, exist_ok=True)
    project_dir.mkdir(parents=True, exist_ok=True)

    external_file = external_root / "Acta 01.PDF"
    external_file.write_bytes(b"external pdf bytes")
    symlink_path = project_dir / "server-entregas"
    symlink_path.symlink_to(external_root, target_is_directory=True)

    monkeypatch.setattr(ingest_mod, "get_library_root", lambda _config=None: library_root)
    monkeypatch.setattr(ingest_mod, "get_project_root", lambda: project_root)

    result = ingest_mod.ingest(config={})

    assert len(result) == 1
    entry = result[0]
    assert entry.relative_path == "Proyectos/2407-casa-rio/server-entregas/Acta 01.PDF"
    assert entry.file_type == "pdf"
    assert entry.domain == "proyectos"
    assert entry.collection == "2407-casa-rio"
    assert entry.file_hash == compute_file_hash(external_file)
