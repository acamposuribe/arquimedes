from __future__ import annotations

import importlib.util
import json
from pathlib import Path


def _load_module() -> object:
    path = Path(__file__).resolve().parents[1] / "scripts" / "migrate_step1_local_graph.py"
    spec = importlib.util.spec_from_file_location("migrate_step1_local_graph", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = "\n".join(json.dumps(row) for row in rows)
    if rows:
        text += "\n"
    path.write_text(text, encoding="utf-8")


def test_migrate_step1_local_graph_rebases_legacy_bridge_artifacts(tmp_path):
    module = _load_module()
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "config.yaml").write_text("library_root: ~/dummy\n", encoding="utf-8")
    _write_jsonl(
        tmp_path / "manifests" / "materials.jsonl",
        [
            {"material_id": "mat_001", "domain": "research", "collection": "papers", "ingested_at": "2026-01-01T00:00:00+00:00"},
            {"material_id": "mat_002", "domain": "research", "collection": "papers", "ingested_at": "2026-01-01T00:00:00+00:00"},
        ],
    )
    _write_jsonl(
        tmp_path / "derived" / "bridge_concept_clusters.jsonl",
        [
            {
                "cluster_id": "concept_001",
                "canonical_name": "Archive and Space",
                "slug": "archive-and-space",
                "material_ids": ["mat_001", "mat_002"],
                "source_concepts": [
                    {"material_id": "mat_001", "concept_name": "archive and space", "evidence_spans": ["archive and space"]},
                    {"material_id": "mat_002", "concept_name": "archive as space", "evidence_spans": ["archive as space"]},
                ],
            }
        ],
    )
    _write_json(
        tmp_path / "derived" / "bridge_cluster_stamp.json",
        {"clustered_at": "2026-01-02T00:00:00+00:00", "route_signature": "cluster:gpt"},
    )
    _write_jsonl(
        tmp_path / "derived" / "lint" / "concept_reflections.jsonl",
        [
            {
                "cluster_id": "concept_001",
                "slug": "archive-and-space",
                "canonical_name": "Archive and Space",
                "main_takeaways": ["Stored takeaway"],
                "main_tensions": [],
                "open_questions": [],
                "why_this_concept_matters": "Stored reason.",
                "supporting_material_ids": ["mat_001", "mat_002"],
                "supporting_evidence": ["archive and space"],
                "input_fingerprint": "old",
                "wiki_path": "wiki/shared/bridge-concepts/archive-and-space.md",
            }
        ],
    )
    _write_jsonl(
        tmp_path / "derived" / "lint" / "cluster_reviews.jsonl",
        [
            {
                "review_id": "concept_001",
                "cluster_id": "concept_001",
                "finding_type": "validated",
                "severity": "low",
                "status": "validated",
                "note": "Stored review.",
                "recommendation": "Keep it.",
                "input_fingerprint": "old",
                "wiki_path": "wiki/shared/bridge-concepts/archive-and-space.md",
            }
        ],
    )
    _write_jsonl(
        tmp_path / "derived" / "lint" / "collection_reflections.jsonl",
        [
            {
                "collection_key": "research/papers",
                "domain": "research",
                "collection": "papers",
                "main_takeaways": ["Stored collection takeaway"],
                "main_tensions": [],
                "important_material_ids": ["mat_001"],
                "important_cluster_ids": ["concept_001"],
                "open_questions": [],
                "why_this_collection_matters": "Stored collection reason.",
                "input_fingerprint": "old",
                "wiki_path": "wiki/research/papers/_index.md",
            }
        ],
    )
    _write_json(
        tmp_path / "derived" / "lint" / "cluster_audit_state.json",
        {"pending_local_fingerprint": "pending", "pending_local_concepts": 2},
    )
    _write_json(
        tmp_path / "derived" / "lint" / "lint_stamp.json",
        {"audited_at": "2026-01-03T00:00:00+00:00"},
    )

    result = module.migrate_step1_local_graph(tmp_path, refresh=False)

    local_clusters = [
        json.loads(line)
        for line in (tmp_path / "derived" / "collections" / "research__papers" / "local_concept_clusters.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    concept_reflections = [
        json.loads(line)
        for line in (tmp_path / "derived" / "lint" / "concept_reflections.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    cluster_reviews = [
        json.loads(line)
        for line in (tmp_path / "derived" / "lint" / "cluster_reviews.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    collection_reflections = [
        json.loads(line)
        for line in (tmp_path / "derived" / "lint" / "collection_reflections.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    local_audit_stamp = json.loads((tmp_path / "derived" / "collections" / "research__papers" / "local_audit_stamp.json").read_text(encoding="utf-8"))
    local_audit_state = json.loads((tmp_path / "derived" / "collections" / "research__papers" / "local_audit_state.json").read_text(encoding="utf-8"))

    assert result["local_clusters"] == 1
    assert local_clusters[0]["cluster_id"] == "research__papers__local_0001"
    assert local_clusters[0]["wiki_path"] == "wiki/research/papers/concepts/archive-and-space.md"
    assert concept_reflections[0]["cluster_id"] == "research__papers__local_0001"
    assert concept_reflections[0]["wiki_path"] == "wiki/research/papers/concepts/archive-and-space.md"
    assert cluster_reviews[0]["review_id"] == "research__papers__local_0001"
    assert cluster_reviews[0]["cluster_id"] == "research__papers__local_0001"
    assert cluster_reviews[0]["wiki_path"] == "wiki/research/papers/concepts/archive-and-space.md"
    assert collection_reflections[0]["important_cluster_ids"] == ["research__papers__local_0001"]
    assert local_audit_stamp == {"audited_at": "2026-01-03T00:00:00+00:00", "cluster_reviews": 1}
    assert local_audit_state == {"pending_local_fingerprint": "pending", "pending_local_concepts": 2}


def test_migrate_step1_local_graph_requires_explicit_scope_for_multi_scope_manifest(tmp_path):
    module = _load_module()
    _write_jsonl(
        tmp_path / "manifests" / "materials.jsonl",
        [
            {"material_id": "mat_001", "domain": "research", "collection": "papers"},
            {"material_id": "mat_002", "domain": "practice", "collection": "codes"},
        ],
    )
    _write_jsonl(tmp_path / "derived" / "bridge_concept_clusters.jsonl", [{"cluster_id": "concept_001", "material_ids": ["mat_001"]}])

    try:
        module.migrate_step1_local_graph(tmp_path, refresh=False)
    except SystemExit as exc:
        assert "Multiple manifest scopes found" in str(exc)
    else:
        raise AssertionError("expected explicit-scope failure")