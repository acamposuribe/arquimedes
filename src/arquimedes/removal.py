"""Removal cascade for materials whose source files disappeared."""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from arquimedes.compile_pages import _material_wiki_path
from arquimedes.ingest import load_manifest, save_manifest
from arquimedes.config import get_project_root


@dataclass
class RemovalReport:
    removed_material_ids: list[str] = field(default_factory=list)
    missing_material_ids: list[str] = field(default_factory=list)
    touched_paths: list[str] = field(default_factory=list)
    collapsed_cluster_ids: list[str] = field(default_factory=list)
    rewritten_cluster_files: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "removed_material_ids": self.removed_material_ids,
            "missing_material_ids": self.missing_material_ids,
            "touched_paths": self.touched_paths,
            "collapsed_cluster_ids": self.collapsed_cluster_ids,
            "rewritten_cluster_files": self.rewritten_cluster_files,
        }


def _load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows: list[dict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    text = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows)
    if rows:
        text += "\n"
    path.write_text(text, encoding="utf-8")


def _cluster_material_ids(cluster: dict) -> list[str]:
    ids = [str(mid) for mid in cluster.get("material_ids", []) if str(mid).strip()]
    for concept in cluster.get("source_concepts", []) or []:
        if isinstance(concept, dict):
            mid = str(concept.get("material_id", "")).strip()
            if mid:
                ids.append(mid)
    return list(dict.fromkeys(ids))


def _remove_material_from_cluster(cluster: dict, material_ids: set[str]) -> dict:
    cluster = dict(cluster)
    cluster["material_ids"] = [
        mid for mid in cluster.get("material_ids", [])
        if str(mid) not in material_ids
    ]
    if "source_concepts" in cluster:
        cluster["source_concepts"] = [
            concept for concept in cluster.get("source_concepts", [])
            if not (isinstance(concept, dict) and str(concept.get("material_id", "")) in material_ids)
        ]
    return cluster


def cascade_delete(
    material_ids: list[str],
    *,
    project_root: Path | None = None,
    dry_run: bool = False,
) -> RemovalReport:
    """Remove material records and generated artifacts for deleted source files."""
    root = project_root or get_project_root()
    report = RemovalReport()
    unique_ids = [mid for mid in dict.fromkeys(material_ids) if mid]
    if not unique_ids:
        return report
    id_set = set(unique_ids)

    manifest = load_manifest(root)
    metas_by_id: dict[str, dict] = {}
    for mid in unique_ids:
        entry = manifest.get(mid)
        meta = _load_json(root / "extracted" / mid / "meta.json")
        if entry and not meta:
            meta = {
                "material_id": entry.material_id,
                "domain": entry.domain,
                "collection": entry.collection,
            }
        if meta:
            metas_by_id[mid] = meta
        elif mid not in manifest:
            report.missing_material_ids.append(mid)

    for mid in unique_ids:
        if mid in manifest:
            report.removed_material_ids.append(mid)
            if not dry_run:
                manifest.pop(mid, None)

        extracted_dir = root / "extracted" / mid
        if extracted_dir.exists():
            report.touched_paths.append(str(extracted_dir))
            if not dry_run:
                shutil.rmtree(extracted_dir)

        meta = metas_by_id.get(mid)
        if meta:
            wiki_path = root / _material_wiki_path(meta)
            if wiki_path.exists():
                report.touched_paths.append(str(wiki_path))
                if not dry_run:
                    wiki_path.unlink()

    if not dry_run:
        save_manifest(root, manifest)

    collections_root = root / "derived" / "collections"
    for clusters_path in sorted(collections_root.glob("*/local_concept_clusters.jsonl")):
        rows = _load_jsonl(clusters_path)
        if not rows:
            continue
        changed = False
        kept: list[dict] = []
        for row in rows:
            material_set = set(_cluster_material_ids(row))
            if not material_set.intersection(id_set):
                kept.append(row)
                continue
            changed = True
            updated = _remove_material_from_cluster(row, id_set)
            remaining = set(_cluster_material_ids(updated))
            if len(remaining) < 2:
                cluster_id = str(row.get("cluster_id", "")).strip()
                if cluster_id:
                    report.collapsed_cluster_ids.append(cluster_id)
                concept_path = row.get("wiki_path")
                if concept_path:
                    page = root / str(concept_path)
                    if page.exists():
                        report.touched_paths.append(str(page))
                        if not dry_run:
                            page.unlink()
                continue
            kept.append(updated)

        if changed:
            report.rewritten_cluster_files.append(str(clusters_path))
            report.touched_paths.append(str(clusters_path))
            if not dry_run:
                _write_jsonl(clusters_path, kept)

    return report
