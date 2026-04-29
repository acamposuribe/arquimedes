"""Markdown-only refresh for existing extracted PDF materials."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from arquimedes.config import get_library_root, get_project_root, load_config
from arquimedes.extract_opendataloader import (
    _run_opendataloader,
    pdf_has_usable_text_layer,
)
from arquimedes.ingest import load_manifest


@dataclass
class MarkdownRefreshResult:
    material_id: str
    relative_path: str
    status: str
    reason: str = ""

    def to_dict(self) -> dict:
        data = {
            "material_id": self.material_id,
            "relative_path": self.relative_path,
            "status": self.status,
        }
        if self.reason:
            data["reason"] = self.reason
        return data


@dataclass
class MarkdownRefreshSummary:
    dry_run: bool
    results: list[MarkdownRefreshResult] = field(default_factory=list)

    def to_dict(self) -> dict:
        counts: dict[str, int] = {}
        for result in self.results:
            counts[result.status] = counts.get(result.status, 0) + 1
        return {
            "dry_run": self.dry_run,
            "counts": dict(sorted(counts.items())),
            "results": [result.to_dict() for result in self.results],
        }


def refresh_markdowns(
    material_id: str | None = None,
    apply: bool = False,
    config: dict | None = None,
) -> MarkdownRefreshSummary:
    """Refresh only extracted/<id>/text.md using OpenDataLoader Markdown.

    This intentionally does not touch pages, chunks, metadata, tables,
    annotations, figures, indexes, derived files, or enrichment artifacts.
    """
    if config is None:
        config = load_config()

    project_root = get_project_root()
    library_root = get_library_root(config)
    manifest = load_manifest(project_root)

    if material_id:
        if material_id not in manifest:
            raise ValueError(f"Material {material_id} not found in manifest")
        items = [(material_id, manifest[material_id])]
    else:
        items = sorted(manifest.items())

    summary = MarkdownRefreshSummary(dry_run=not apply)
    for mid, entry in items:
        result = _refresh_one(
            project_root=project_root,
            library_root=library_root,
            material_id=mid,
            entry=entry,
            apply=apply,
        )
        summary.results.append(result)
    return summary


def _refresh_one(
    project_root: Path,
    library_root: Path,
    material_id: str,
    entry,
    apply: bool,
) -> MarkdownRefreshResult:
    rel = entry.relative_path
    if entry.file_type != "pdf":
        return MarkdownRefreshResult(material_id, rel, "skipped", "not a pdf")

    source_path = library_root / rel
    if not source_path.exists():
        return MarkdownRefreshResult(material_id, rel, "skipped", "source missing")

    text_path = project_root / "extracted" / material_id / "text.md"
    if not text_path.exists():
        return MarkdownRefreshResult(material_id, rel, "skipped", "text.md missing")

    if not pdf_has_usable_text_layer(source_path):
        return MarkdownRefreshResult(material_id, rel, "skipped", "no embedded text layer")

    if not apply:
        return MarkdownRefreshResult(material_id, rel, "would_update")

    _, markdown = _run_opendataloader(source_path)
    text_path.write_text(markdown, encoding="utf-8")
    return MarkdownRefreshResult(material_id, rel, "updated")
