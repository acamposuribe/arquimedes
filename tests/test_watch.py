from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from arquimedes.ingest import save_manifest
from arquimedes.models import MaterialManifest, compute_file_hash, compute_material_id
from arquimedes.watch import BatchPlanner, LibraryScanner


def _manifest(path: Path, relative_path: str) -> MaterialManifest:
    return MaterialManifest(
        material_id=compute_material_id(path),
        file_hash=compute_file_hash(path),
        relative_path=relative_path,
        file_type="pdf",
        domain="research",
        collection="papers",
        ingested_at=datetime.now(timezone.utc).isoformat(),
    )


def test_watch_planner_detects_add_modify_move_and_delete(tmp_path):
    library = tmp_path / "Library"
    library.mkdir()
    (library / "Research" / "papers").mkdir(parents=True)
    (library / "Research" / "moved").mkdir(parents=True)
    (tmp_path / "manifests").mkdir()

    unchanged = library / "Research" / "papers" / "unchanged.pdf"
    modified = library / "Research" / "papers" / "modified.pdf"
    moved_old = library / "Research" / "papers" / "moved.pdf"
    deleted = library / "Research" / "papers" / "deleted.pdf"

    unchanged.write_text("same", encoding="utf-8")
    modified.write_text("old", encoding="utf-8")
    moved_old.write_text("move me", encoding="utf-8")
    deleted.write_text("gone", encoding="utf-8")

    moved_entry = _manifest(moved_old, "Research/papers/moved.pdf")
    save_manifest(tmp_path, {
        "unchanged": _manifest(unchanged, "Research/papers/unchanged.pdf"),
        "modified": _manifest(modified, "Research/papers/modified.pdf"),
        "moved": moved_entry,
        "deleted": _manifest(deleted, "Research/papers/deleted.pdf"),
    })

    modified.write_text("new", encoding="utf-8")
    moved_new = library / "Research" / "moved" / "moved.pdf"
    moved_new.write_text("move me", encoding="utf-8")
    moved_old.unlink()
    deleted.unlink()
    added = library / "Research" / "papers" / "added.pdf"
    added.write_text("add", encoding="utf-8")

    snapshot = LibraryScanner(library).scan()
    batch = BatchPlanner(tmp_path).plan(snapshot)

    assert {p.name for p in batch.add_or_modify} == {"added.pdf", "modified.pdf"}
    assert [p.name for p in batch.move] == ["moved.pdf"]
    assert moved_entry.material_id in batch.moved_ids
    assert moved_entry.material_id not in batch.delete
    assert len(batch.delete) == 2


def test_watch_planner_empty_when_library_matches_manifest(tmp_path):
    library = tmp_path / "Library"
    (library / "Research" / "papers").mkdir(parents=True)
    (tmp_path / "manifests").mkdir()
    pdf = library / "Research" / "papers" / "same.pdf"
    pdf.write_text("same", encoding="utf-8")
    save_manifest(tmp_path, {"same": _manifest(pdf, "Research/papers/same.pdf")})

    batch = BatchPlanner(tmp_path).plan(LibraryScanner(library).scan())

    assert batch.is_empty()
