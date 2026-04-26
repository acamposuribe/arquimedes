from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from arquimedes.cli import cli
from arquimedes.lint_global_bridge import migrate_legacy_global_bridges


def _write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")


def _setup_single_domain_legacy_repo(root: Path) -> None:
    _write_jsonl(
        root / "derived" / "collections" / "research__papers" / "local_concept_clusters.jsonl",
        [
            {
                "cluster_id": "research__papers__local_0001",
                "domain": "research",
                "collection": "papers",
                "canonical_name": "Archive Space",
                "slug": "archive-space",
                "descriptor": "Archive as spatial order.",
                "material_ids": ["mat_001", "mat_002"],
                "source_concepts": [{"material_id": "mat_001", "concept_name": "archive space"}],
                "confidence": 0.91,
            }
        ],
    )
    _write_jsonl(
        root / "derived" / "collections" / "research__projects" / "local_concept_clusters.jsonl",
        [
            {
                "cluster_id": "research__projects__local_0001",
                "domain": "research",
                "collection": "projects",
                "canonical_name": "Archive Space",
                "slug": "archive-space",
                "descriptor": "Archive as project memory.",
                "material_ids": ["mat_010"],
                "source_concepts": [{"material_id": "mat_010", "concept_name": "archive space"}],
                "confidence": 0.74,
            }
        ],
    )
    _write_jsonl(
        root / "derived" / "lint" / "collection_reflections.jsonl",
        [
            {
                "collection_key": "research/papers",
                "domain": "research",
                "collection": "papers",
                "main_takeaways": ["Archive theory anchors the collection."],
                "main_tensions": [],
                "open_questions": [],
                "why_this_collection_matters": "Papers define the archive frame.",
            },
            {
                "collection_key": "research/projects",
                "domain": "research",
                "collection": "projects",
                "main_takeaways": ["Archive thinking recurs across projects."],
                "main_tensions": [],
                "open_questions": [],
                "why_this_collection_matters": "Projects extend the archive frame.",
            },
        ],
    )
    _write_jsonl(
        root / "derived" / "global_bridge_clusters.jsonl",
        [
            {
                "bridge_id": "global_bridge__archive-space-framework",
                "canonical_name": "Archive Space Framework",
                "slug": "archive-space-framework",
                "descriptor": "Legacy shared bridge.",
                "aliases": ["Archive as Framework"],
                "member_local_clusters": [
                    {"cluster_id": "research__papers__local_0001"},
                    {"cluster_id": "research__projects__local_0001"},
                ],
                "bridge_takeaways": ["Archive thinking connects theory and projects."],
                "bridge_open_questions": ["What else belongs here?"],
                "why_this_bridge_matters": "This bridge links research collections.",
                "wiki_path": "wiki/shared/bridge-concepts/archive-space-framework.md",
            }
        ],
    )
    _write_json(
        root / "derived" / "global_bridge_stamp.json",
        {"bridged_at": "2026-04-01T00:00:00+00:00"},
    )
    (root / "wiki" / "shared" / "bridge-concepts").mkdir(parents=True, exist_ok=True)
    (root / "wiki" / "shared" / "bridge-concepts" / "archive-space-framework.md").write_text(
        "# Archive Space Framework\n\nLegacy bridge page.\n",
        encoding="utf-8",
    )
    (root / "wiki" / "shared" / "glossary").mkdir(parents=True, exist_ok=True)
    (root / "wiki" / "shared" / "glossary" / "_index.md").write_text(
        "- [Archive Space Framework (main)](wiki/shared/bridge-concepts/archive-space-framework.md)\n",
        encoding="utf-8",
    )


def test_legacy_bridge_migration_dry_run_preserves_single_domain_bridge(tmp_path):
    _setup_single_domain_legacy_repo(tmp_path)

    summary = migrate_legacy_global_bridges(tmp_path, apply=False)

    assert summary["applied"] is False
    assert summary["can_apply"] is True
    assert summary["legacy_bridges"] == 1
    assert summary["migrated_bridges"] == 1
    assert summary["migrated_domains"] == {"research": 1}
    assert summary["page_copies"] == 1
    assert summary["glossary_replacements"] == 1
    assert summary["ambiguous_bridges"] == []
    assert not (tmp_path / "derived" / "domains" / "research" / "global_bridge_clusters.jsonl").exists()


def test_legacy_bridge_migration_apply_writes_domain_artifacts_and_pages(tmp_path):
    _setup_single_domain_legacy_repo(tmp_path)

    result = CliRunner().invoke(
        cli,
        ["migrate-global-bridges", "--root", str(tmp_path), "--apply", "--json"],
    )

    assert result.exit_code == 0, result.output
    summary = json.loads(result.output)
    assert summary["applied"] is True
    assert summary["migrated_domains"] == {"research": 1}
    assert summary["backup_root"]

    artifact_path = tmp_path / "derived" / "domains" / "research" / "global_bridge_clusters.jsonl"
    rows = [json.loads(line) for line in artifact_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert rows[0]["bridge_id"] == "global_bridge__research__archive-space-framework"
    assert rows[0]["domain"] == "research"
    assert rows[0]["wiki_path"] == "wiki/research/bridge-concepts/archive-space-framework.md"

    stamp_path = tmp_path / "derived" / "domains" / "research" / "global_bridge_stamp.json"
    stamp = json.loads(stamp_path.read_text(encoding="utf-8"))
    assert stamp["domain"] == "research"
    assert stamp["global_bridge_count"] == 1

    migrated_page = tmp_path / "wiki" / "research" / "bridge-concepts" / "archive-space-framework.md"
    assert migrated_page.read_text(encoding="utf-8") == "# Archive Space Framework\n\nLegacy bridge page.\n"
    glossary = (tmp_path / "wiki" / "shared" / "glossary" / "_index.md").read_text(encoding="utf-8")
    assert "wiki/research/bridge-concepts/archive-space-framework.md" in glossary
    assert "wiki/shared/bridge-concepts/archive-space-framework.md" not in glossary
    assert (tmp_path / "derived" / "global_bridge_clusters.jsonl").exists()


def test_legacy_bridge_migration_refuses_apply_for_mixed_domain_bridge(tmp_path):
    _setup_single_domain_legacy_repo(tmp_path)
    _write_jsonl(
        tmp_path / "derived" / "collections" / "practice__projects" / "local_concept_clusters.jsonl",
        [
            {
                "cluster_id": "practice__projects__local_0001",
                "domain": "practice",
                "collection": "projects",
                "canonical_name": "Archive Space",
                "slug": "archive-space",
                "descriptor": "Archive as practice memory.",
                "material_ids": ["mat_020"],
                "source_concepts": [{"material_id": "mat_020", "concept_name": "archive space"}],
                "confidence": 0.61,
            }
        ],
    )
    _write_jsonl(
        tmp_path / "derived" / "global_bridge_clusters.jsonl",
        [
            {
                "bridge_id": "global_bridge__archive-space-framework",
                "canonical_name": "Archive Space Framework",
                "slug": "archive-space-framework",
                "descriptor": "Legacy mixed-domain bridge.",
                "member_local_clusters": [
                    {"cluster_id": "research__papers__local_0001"},
                    {"cluster_id": "practice__projects__local_0001"},
                ],
                "wiki_path": "wiki/shared/bridge-concepts/archive-space-framework.md",
            }
        ],
    )

    summary = migrate_legacy_global_bridges(tmp_path, apply=False)
    assert summary["can_apply"] is False
    assert len(summary["ambiguous_bridges"]) == 1
    assert summary["ambiguous_bridges"][0]["reason"] == "bridge spans multiple domains"

    result = CliRunner().invoke(
        cli,
        ["migrate-global-bridges", "--root", str(tmp_path), "--apply"],
    )
    assert result.exit_code != 0
    assert "nothing was written" in result.output.lower()
    assert not (tmp_path / "derived" / "domains" / "research" / "global_bridge_clusters.jsonl").exists()
