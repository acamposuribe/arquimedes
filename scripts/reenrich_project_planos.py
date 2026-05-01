#!/usr/bin/env python3
"""Re-enrich Proyectos drawing-set materials with the current project prompt logic.

This targets only extracted Proyectos materials whose current
project_extraction.project_material_type is ``drawing_set`` (Planos), and reruns
the document enrichment stage so new fields such as project_phase and
drawing_scope can be populated without touching unrelated materials.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from arquimedes.config import get_project_root, load_config
from arquimedes.enrich import enrich
from arquimedes.ingest import load_manifest


def _plain(value) -> str:
    if isinstance(value, dict):
        value = value.get("value", "")
    return str(value or "").strip()


def _is_project_drawing(meta: dict) -> bool:
    project_extraction = meta.get("project_extraction")
    if not isinstance(project_extraction, dict):
        return False
    return _plain(project_extraction.get("project_material_type")) == "drawing_set"


def _candidate_materials(root: Path, *, collection: str | None = None) -> list[tuple[str, str]]:
    manifest = load_manifest(root)
    extracted = root / "extracted"
    candidates: list[tuple[str, str]] = []
    for material_id, entry in manifest.items():
        if getattr(entry, "domain", "") != "proyectos":
            continue
        if collection and getattr(entry, "collection", "") != collection:
            continue
        meta_path = extracted / material_id / "meta.json"
        if not meta_path.exists():
            continue
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        if _is_project_drawing(meta):
            candidates.append((material_id, str(meta.get("title") or material_id)))
    return sorted(candidates, key=lambda row: row[1].lower())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--collection", help="Only process one Proyectos collection/project id.")
    parser.add_argument("--limit", type=int, default=0, help="Maximum number of materials to process.")
    parser.add_argument("--dry-run", action="store_true", help="List targets without calling the LLM.")
    parser.add_argument("--yes", action="store_true", help="Skip confirmation prompt.")
    args = parser.parse_args()

    config = load_config()
    root = get_project_root()
    candidates = _candidate_materials(root, collection=args.collection)
    if args.limit > 0:
        candidates = candidates[: args.limit]

    if not candidates:
        print("No Proyectos drawing_set materials found.")
        return 0

    print(f"Found {len(candidates)} Proyectos drawing_set material(s):")
    for material_id, title in candidates:
        print(f"  {material_id}  {title}")

    if args.dry_run:
        return 0

    if not args.yes:
        answer = input("Re-enrich document stage for these materials? [y/N] ").strip().lower()
        if answer not in {"y", "yes"}:
            print("Cancelled.")
            return 1

    all_ok = True
    for material_id, title in candidates:
        print(f"\n==> {material_id}  {title}", flush=True)
        results, succeeded = enrich(
            material_id=material_id,
            config=config,
            force=True,
            stages=["document"],
            domain="proyectos",
        )
        all_ok = all_ok and succeeded
        material_result = results.get(material_id, {})
        stage_result = material_result.get("document", {})
        print(f"    [{stage_result.get('status', '?')}] {stage_result.get('detail', '')}")

    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
