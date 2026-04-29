"""Tests for Markdown-only refresh."""

from __future__ import annotations

from dataclasses import dataclass

from arquimedes.refresh_markdown import refresh_markdowns


@dataclass
class _Entry:
    relative_path: str
    file_type: str = "pdf"


def test_refresh_markdowns_dry_run_does_not_run_opendataloader(monkeypatch, tmp_path):
    root = tmp_path
    library = tmp_path / "library"
    source = library / "Research" / "Sample" / "paper.pdf"
    text_path = root / "extracted" / "mat_001" / "text.md"
    source.parent.mkdir(parents=True)
    text_path.parent.mkdir(parents=True)
    source.write_bytes(b"%PDF")
    text_path.write_text("old", encoding="utf-8")

    monkeypatch.setattr("arquimedes.refresh_markdown.get_project_root", lambda: root)
    monkeypatch.setattr("arquimedes.refresh_markdown.get_library_root", lambda config: library)
    monkeypatch.setattr(
        "arquimedes.refresh_markdown.load_manifest",
        lambda project_root: {
            "mat_001": _Entry("Research/Sample/paper.pdf"),
        },
    )
    monkeypatch.setattr(
        "arquimedes.refresh_markdown.pdf_has_usable_text_layer",
        lambda path: True,
    )

    def fail_run(path):
        raise AssertionError("OpenDataLoader should not run during dry run")

    monkeypatch.setattr("arquimedes.refresh_markdown._run_opendataloader", fail_run)

    summary = refresh_markdowns(config={}, apply=False)

    assert summary.to_dict()["counts"] == {"would_update": 1}
    assert text_path.read_text(encoding="utf-8") == "old"


def test_refresh_markdowns_apply_only_updates_text_md(monkeypatch, tmp_path):
    root = tmp_path
    library = tmp_path / "library"
    source = library / "Research" / "Sample" / "paper.pdf"
    material_dir = root / "extracted" / "mat_001"
    text_path = material_dir / "text.md"
    chunks_path = material_dir / "chunks.jsonl"
    source.parent.mkdir(parents=True)
    material_dir.mkdir(parents=True)
    source.write_bytes(b"%PDF")
    text_path.write_text("old", encoding="utf-8")
    chunks_path.write_text("keep me", encoding="utf-8")

    monkeypatch.setattr("arquimedes.refresh_markdown.get_project_root", lambda: root)
    monkeypatch.setattr("arquimedes.refresh_markdown.get_library_root", lambda config: library)
    monkeypatch.setattr(
        "arquimedes.refresh_markdown.load_manifest",
        lambda project_root: {
            "mat_001": _Entry("Research/Sample/paper.pdf"),
        },
    )
    monkeypatch.setattr(
        "arquimedes.refresh_markdown.pdf_has_usable_text_layer",
        lambda path: True,
    )
    monkeypatch.setattr(
        "arquimedes.refresh_markdown._run_opendataloader",
        lambda path: ({}, "# Better Markdown\n"),
    )

    summary = refresh_markdowns(config={}, apply=True)

    assert summary.to_dict()["counts"] == {"updated": 1}
    assert text_path.read_text(encoding="utf-8") == "# Better Markdown\n"
    assert chunks_path.read_text(encoding="utf-8") == "keep me"
