from __future__ import annotations

import json
from datetime import datetime, timezone

from arquimedes.ingest import save_manifest
from arquimedes.models import MaterialManifest
from arquimedes.removal import cascade_delete


def _write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")


def _write_jsonl(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


def test_cascade_delete_removes_manifest_artifacts_and_cluster_refs(tmp_path):
    mid = "mat_removed"
    kept = "mat_kept"
    save_manifest(tmp_path, {
        mid: MaterialManifest(mid, "hash1", "Research/papers/old.pdf", "pdf", "research", "papers", datetime.now(timezone.utc).isoformat()),
        kept: MaterialManifest(kept, "hash2", "Research/papers/kept.pdf", "pdf", "research", "papers", datetime.now(timezone.utc).isoformat()),
    })
    _write_json(tmp_path / "extracted" / mid / "meta.json", {
        "material_id": mid,
        "domain": "research",
        "collection": "papers",
    })
    wiki_page = tmp_path / "wiki" / "research" / "papers" / f"{mid}.md"
    wiki_page.parent.mkdir(parents=True)
    wiki_page.write_text("old page", encoding="utf-8")
    cluster_path = tmp_path / "derived" / "collections" / "research__papers" / "local_concept_clusters.jsonl"
    _write_jsonl(cluster_path, [
        {
            "cluster_id": "cluster_keep",
            "canonical_name": "Keep",
            "material_ids": [mid, kept, "mat_other"],
            "source_concepts": [{"material_id": mid}, {"material_id": kept}, {"material_id": "mat_other"}],
        },
        {
            "cluster_id": "cluster_collapse",
            "canonical_name": "Collapse",
            "material_ids": [mid, kept],
            "source_concepts": [{"material_id": mid}, {"material_id": kept}],
            "wiki_path": "wiki/research/papers/concepts/collapse.md",
        },
    ])
    collapsed_page = tmp_path / "wiki" / "research" / "papers" / "concepts" / "collapse.md"
    collapsed_page.parent.mkdir(parents=True)
    collapsed_page.write_text("collapse", encoding="utf-8")

    report = cascade_delete([mid], project_root=tmp_path)

    assert report.removed_material_ids == [mid]
    assert not (tmp_path / "extracted" / mid).exists()
    assert not wiki_page.exists()
    assert not collapsed_page.exists()
    manifest_text = (tmp_path / "manifests" / "materials.jsonl").read_text(encoding="utf-8")
    assert mid not in manifest_text
    rows = [json.loads(line) for line in cluster_path.read_text(encoding="utf-8").splitlines()]
    assert len(rows) == 1
    assert rows[0]["material_ids"] == [kept, "mat_other"]
    assert report.collapsed_cluster_ids == ["cluster_collapse"]


def test_cascade_delete_is_idempotent_for_missing_material(tmp_path):
    (tmp_path / "manifests").mkdir()
    report = cascade_delete(["missing"], project_root=tmp_path)

    assert report.missing_material_ids == ["missing"]
    assert report.removed_material_ids == []
