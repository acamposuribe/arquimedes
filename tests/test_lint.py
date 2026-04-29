"""Tests for lint.py — Phase 6 deterministic health checks."""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from datetime import datetime, timezone

import pytest
from click.testing import CliRunner

from arquimedes.compile_pages import _concept_wiki_path, _material_wiki_path
from arquimedes.enrich_stamps import canonical_hash
from arquimedes.index import rebuild_index
from arquimedes.lint import EnrichmentError, ReflectionIndexTool, _build_collection_reflection_evidence_payload, _build_concept_reflection_evidence_payload, _build_material_info, _cluster_audit_apply_bridge_update, _expected_pages, _filter_local_rows_not_in_bridge, _graph_reflection_due, _graph_reflection_packet, _load_manifest, _memory_state_stale, _resolve_wiki_link, _run_cluster_audit, _run_collection_reflections, _run_concept_reflections, _run_graph_reflection, _stage_cluster_audit_reviews_input, run_deterministic_lint, run_lint
from arquimedes.memory import memory_rebuild
from arquimedes.cli import lint as lint_cmd


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = "\n".join(json.dumps(row) for row in rows)
    if rows:
        text += "\n"
    path.write_text(text, encoding="utf-8")


def _setup_repo(tmp_path: Path) -> tuple[Path, dict]:
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "config.yaml").write_text("library_root: ~/dummy\n", encoding="utf-8")
    (tmp_path / "indexes").mkdir()
    (tmp_path / "manifests").mkdir()
    (tmp_path / "extracted").mkdir()
    (tmp_path / "wiki").mkdir()

    mid = "mat_001"
    manifest = {
        "material_id": mid,
        "file_hash": "hash-001",
        "relative_path": "Research/One.pdf",
        "file_type": "pdf",
        "domain": "research",
        "collection": "papers",
        "ingested_at": "2026-01-01T00:00:00+00:00",
    }
    _write_jsonl(tmp_path / "manifests" / "materials.jsonl", [manifest])

    mat_dir = tmp_path / "extracted" / mid
    mat_dir.mkdir(parents=True)
    _write_json(
        mat_dir / "meta.json",
        {
            "material_id": mid,
            "file_hash": "hash-001",
            "source_path": "Research/One.pdf",
            "title": "One",
            "authors": ["Author One"],
            "year": "2026",
            "page_count": 1,
            "file_type": "pdf",
            "domain": "research",
            "collection": "papers",
            "raw_keywords": ["archive"],
            "raw_document_type": "paper",
            "summary": {"value": "Summary", "provenance": {}},
            "keywords": {"value": ["archive"], "provenance": {}},
            "document_type": {"value": "paper", "provenance": {}},
            "facets": {},
            "_enrichment_stamp": {"prompt_version": "enrich-v1.0", "enrichment_schema_version": "1"},
        },
    )
    _write_jsonl(
        mat_dir / "pages.jsonl",
        [
            {
                "page_number": 1,
                "text": "This page links to a missing note.",
                "headings": ["Intro"],
                "section_boundaries": [],
                "figure_refs": [],
                "table_refs": [],
                "thumbnail_path": "",
                "has_annotations": False,
                "annotation_ids": [],
            }
        ],
    )
    _write_jsonl(
        mat_dir / "chunks.jsonl",
        [
            {
                "chunk_id": "chk_00001",
                "text": "This chunk talks about the archive.",
                "source_pages": [1],
                "emphasized": False,
                "summary": {"value": "Chunk summary", "provenance": {}},
                "keywords": {"value": ["archive"], "provenance": {}},
                "content_class": "argument",
            }
        ],
    )

    orphan = tmp_path / "extracted" / "orphan_001"
    orphan.mkdir(parents=True)
    _write_json(orphan / "meta.json", {"material_id": "orphan_001", "title": "Orphan"})

    root_wiki = tmp_path / "wiki"
    _write_json(root_wiki / "_index.md", {"body": "[broken](wiki/research/missing.md)"})
    _write_json(root_wiki / "research" / "_index.md", {"body": "Research home."})
    _write_json(root_wiki / "research" / "papers" / "_index.md", {"body": "Collection home."})
    _write_json(root_wiki / "shared" / "concepts" / "_index.md", {"body": "Concepts."})
    _write_json(root_wiki / "shared" / "glossary" / "_index.md", {"body": "Glossary."})
    _write_json(root_wiki / "research" / "orphan.md", {"body": "Orphan page."})

    material_path = tmp_path / _material_wiki_path({
        "domain": "research",
        "collection": "papers",
        "material_id": mid,
        "title": "One",
    })
    _write_json(material_path, {"body": "Material page."})

    derived = tmp_path / "derived"
    _write_jsonl(
        derived / "bridge_concept_clusters.jsonl",
        [
            {
                "cluster_id": "concept_001",
                "canonical_name": "Archive and Space",
                "slug": "archive-and-space",
                "material_ids": [mid],
                "source_concepts": [
                    {
                        "material_id": mid,
                        "concept_name": "archive and space",
                        "relevance": "high",
                        "source_pages": [1],
                        "evidence_spans": ["archive and space"],
                        "confidence": 0.9,
                    }
                ],
            }
        ],
    )
    _write_jsonl(derived / "bridge_concept_clusters.jsonl", [])

    concept_path = tmp_path / "wiki" / "shared" / "bridge-concepts" / "archive-and-space.md"
    _write_json(concept_path, {"body": "Concept page."})

    config = {
        "llm": {"agent_cmd": "echo"},
        "enrichment": {
            "prompt_version": "enrich-v1.0",
            "enrichment_schema_version": "1",
        },
    }
    return tmp_path, config


def _write_cluster_data(root: Path) -> list[dict]:
    clusters = [
        {
            "cluster_id": "concept_001",
            "canonical_name": "Archive and Space",
            "slug": "archive-and-space",
            "material_ids": ["mat_001", "mat_002"],
            "source_concepts": [
                {
                    "material_id": "mat_001",
                    "concept_name": "archive and space",
                    "relevance": "high",
                    "source_pages": [1],
                    "evidence_spans": ["archive and space"],
                    "confidence": 0.9,
                },
                {
                    "material_id": "mat_002",
                    "concept_name": "archive space",
                    "relevance": "medium",
                    "source_pages": [1],
                    "evidence_spans": ["archive space"],
                    "confidence": 0.8,
                },
            ],
        },
        {
            "cluster_id": "concept_002",
            "canonical_name": "Memory and Place",
            "slug": "memory-and-place",
            "material_ids": ["mat_001"],
            "source_concepts": [
                {
                    "material_id": "mat_001",
                    "concept_name": "memory and place",
                    "relevance": "low",
                    "source_pages": [1],
                    "evidence_spans": ["memory and place"],
                    "confidence": 0.7,
                }
            ],
        },
    ]
    _write_jsonl(root / "derived" / "bridge_concept_clusters.jsonl", clusters)
    _write_json(root / "wiki" / "shared" / "bridge-concepts" / "archive-and-space.md", {"body": "Concept page."})
    _write_json(root / "wiki" / "shared" / "bridge-concepts" / "memory-and-place.md", {"body": "Concept page."})
    return clusters


def test_run_deterministic_lint_reports_core_issues(tmp_path, monkeypatch):
    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)

    report = run_deterministic_lint(config)

    checks = {issue["check"] for issue in report["issues"]}
    assert "broken_link" in checks
    assert "orphaned_material" in checks
    assert "orphaned_wiki_page" in checks
    assert "stale_index" in checks
    assert "stale_memory_bridge" in checks
    assert (root / "derived" / "lint" / "deterministic_report.json").exists()


def test_resolve_wiki_link_decodes_url_encoded_collection_names(tmp_path):
    root = tmp_path
    page_path = root / "wiki" / "research" / "_index.md"
    page_path.parent.mkdir(parents=True, exist_ok=True)
    page_path.write_text("Collection home.", encoding="utf-8")

    resolved = _resolve_wiki_link(
        page_path,
        "Van%20Eyck/mat_001.md",
        root / "wiki",
        root,
    )

    assert resolved == root / "wiki" / "research" / "Van Eyck" / "mat_001.md"


def test_expected_pages_include_local_concepts_and_collection_indexes(tmp_path, monkeypatch):
    import arquimedes.lint as lint_mod

    root = tmp_path
    monkeypatch.setattr(lint_mod, "load_global_bridge_clusters", lambda _root: [])
    manifest_records = [{
        "material_id": "mat_001",
        "domain": "research",
        "collection": "Van Eyck",
    }]
    metas = {
        "mat_001": {
            "material_id": "mat_001",
            "domain": "research",
            "collection": "Van Eyck",
            "title": "One",
        }
    }
    clusters = [{
        "cluster_id": "research__Van Eyck__local_0001",
        "domain": "research",
        "collection": "Van Eyck",
        "slug": "threshold-space-and-the-in-between",
        "wiki_path": "wiki/research/Van Eyck/concepts/threshold-space-and-the-in-between.md",
    }]

    expected = _expected_pages(root, manifest_records, metas, clusters)

    assert root / "wiki" / "research" / "Van Eyck" / "concepts" / "_index.md" in expected
    assert root / "wiki" / "research" / "Van Eyck" / "concepts" / "threshold-space-and-the-in-between.md" in expected


def test_expected_pages_include_domain_bridge_indexes(tmp_path, monkeypatch):
    import arquimedes.lint as lint_mod

    root = tmp_path
    monkeypatch.setattr(
        lint_mod,
        "load_global_bridge_clusters",
        lambda _root: [
            {
                "cluster_id": "research__bridge_0001",
                "domain": "research",
                "slug": "archive-space-framework",
                "wiki_path": "wiki/research/bridge-concepts/archive-space-framework.md",
            }
        ],
    )
    manifest_records = [{
        "material_id": "mat_001",
        "domain": "research",
        "collection": "Van Eyck",
    }]
    metas = {
        "mat_001": {
            "material_id": "mat_001",
            "domain": "research",
            "collection": "Van Eyck",
            "title": "One",
        }
    }

    expected = _expected_pages(root, manifest_records, metas, [])

    assert root / "wiki" / "research" / "bridge-concepts" / "_index.md" in expected


def test_run_lint_quick_writes_markdown_report(tmp_path, monkeypatch):
    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)

    result = run_lint(config, quick=True, report=True)

    assert result["mode"] == "quick"
    assert (root / "wiki" / "_lint_report.md").exists()


def test_run_lint_full_refreshes_report_after_mutations(tmp_path, monkeypatch):
    import arquimedes.lint as lint_mod

    root, config = _setup_repo(tmp_path)
    monkeypatch.delenv("ARQUIMEDES_CONFIG", raising=False)
    monkeypatch.delenv("ARQUIMEDES_ROOT", raising=False)
    monkeypatch.chdir(root)

    deterministic_runs = iter(
        [
            {"summary": {"issues": 3, "high": 1, "medium": 1, "low": 1}, "issues": [{"check": "missing_compiled_page"}]},
            {"summary": {"issues": 0, "high": 0, "medium": 0, "low": 0}, "issues": []},
        ]
    )

    monkeypatch.setattr(lint_mod, "run_deterministic_lint", lambda _config: next(deterministic_runs))
    monkeypatch.setattr(
        lint_mod,
        "_apply_deterministic_fixes",
        lambda report, _config: {"compiled": True, "details": ["recompile wiki"]},
    )
    monkeypatch.setattr(
        lint_mod,
        "run_reflective_lint",
        lambda *_args, **_kwargs: {
            "cluster_reviews": 0,
            "concept_reflections": 0,
            "collection_reflections": 0,
            "global_bridges": 0,
            "graph_maintenance": 0,
        },
    )
    monkeypatch.setattr(
        lint_mod,
        "render_lint_report",
        lambda report: f"issues={report['summary']['issues']}\n",
    )

    result = run_lint(config, full=True)

    assert result["deterministic"]["summary"]["issues"] == 0
    assert (root / "wiki" / "_lint_report.md").read_text(encoding="utf-8") == "issues=0\n"


def test_run_lint_logs_failed_outcome(tmp_path, monkeypatch):
    import arquimedes.lint as lint_mod

    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)
    monkeypatch.setattr(lint_mod, "run_deterministic_lint", lambda _config: (_ for _ in ()).throw(ValueError("lint boom")))

    with pytest.raises(ValueError, match="lint boom"):
        run_lint(config, quick=True)

    log_lines = (root / "logs" / "lint.log").read_text(encoding="utf-8").splitlines()
    assert len(log_lines) == 2
    assert "\tSTART\tquick\tFalse\tFalse\tFalse" in log_lines[0]
    assert "\tFAILED\tlint boom" in log_lines[1]


@pytest.mark.skip(reason="legacy raw-material bridge fixture retired")
def test_memory_state_stale_matches_memory_rebuild_fingerprint(tmp_path, monkeypatch):
    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)

    rebuild_index()
    _write_jsonl(
        root / "derived" / "bridge_concept_clusters.jsonl",
        [
            {
                "cluster_id": "concept_001",
                "canonical_name": "Archive and Space",
                "slug": "archive-and-space",
                "material_ids": ["mat_001"],
                "source_concepts": [
                    {
                        "material_id": "mat_001",
                        "concept_name": "archive and space",
                        "relevance": "high",
                        "source_pages": [1],
                        "evidence_spans": ["archive and space"],
                        "confidence": 0.9,
                    }
                ],
            }
        ],
    )
    _write_jsonl(
        root / "derived" / "lint" / "graph_findings.jsonl",
        [
            {
                "finding_id": "graph:0",
                "finding_type": "bridge",
                "severity": "low",
                "summary": "Add a missing bridge link.",
                "details": "The graph could connect these materials more directly.",
                "affected_material_ids": ["mat_001"],
                "affected_cluster_ids": ["concept_001"],
                "candidate_future_sources": ["oral history"],
                "candidate_bridge_links": ["archive and memory"],
                "input_fingerprint": "abc123",
            }
        ],
    )

    memory_rebuild(config)

    stale, reason = _memory_state_stale(root)
    assert stale is False
    assert reason == ""


@pytest.mark.skip(reason="legacy raw-material bridge audit retired")
def test_cluster_audit_writes_schema_and_skips_unchanged_clusters(tmp_path, monkeypatch):
    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)
    clusters = _write_cluster_data(root)

    (root / "manifests" / "materials.jsonl").write_text(
        "\n".join(
            [
                json.dumps({
                    "material_id": "mat_001",
                    "file_hash": "hash-001",
                    "relative_path": "Research/One.pdf",
                    "file_type": "pdf",
                    "domain": "research",
                    "collection": "papers",
                    "ingested_at": "2026-01-01T00:00:00+00:00",
                }),
                json.dumps({
                    "material_id": "mat_002",
                    "file_hash": "hash-002",
                    "relative_path": "Research/Two.pdf",
                    "file_type": "pdf",
                    "domain": "research",
                    "collection": "papers",
                    "ingested_at": "2026-01-02T00:00:00+00:00",
                }),
            ]
        ),
        encoding="utf-8",
    )

    (root / "extracted" / "mat_002").mkdir(parents=True)
    _write_json(
        root / "extracted" / "mat_002" / "meta.json",
        {
            "material_id": "mat_002",
            "file_hash": "hash-002",
            "source_path": "Research/Two.pdf",
            "title": "Two",
            "authors": ["Author Two"],
            "year": "2026",
            "page_count": 1,
            "file_type": "pdf",
            "domain": "research",
            "collection": "papers",
            "raw_keywords": ["archive"],
            "raw_document_type": "paper",
            "summary": {"value": "Summary", "provenance": {}},
            "keywords": {"value": ["archive"], "provenance": {}},
            "document_type": {"value": "paper", "provenance": {}},
            "facets": {},
            "_enrichment_stamp": {"prompt_version": "enrich-v1.0", "enrichment_schema_version": "1"},
        },
    )
    _write_json(root / "wiki" / "research" / "papers" / "_index.md", {"body": "Collection home.\n\n## Phase 6 Reflection\nOld note."})
    _write_jsonl(
        root / "derived" / "lint" / "cluster_reviews.jsonl",
        [
            {
                "review_id": "concept_001:merge:old",
                "cluster_id": "concept_001",
                "finding_type": "merge",
                "severity": "medium",
                "status": "open",
                "note": "The bridge still needs a sharper canonical name.",
                "recommendation": "Tighten the name and keep the bridge under review.",
                "affected_material_ids": ["mat_001", "mat_002"],
                "affected_concept_names": ["archive and space"],
                "evidence": ["shared archive frame"],
                "input_fingerprint": "old",
                "wiki_path": "wiki/shared/bridge-concepts/archive-and-space.md",
            }
        ],
    )

    def fake_local_rows_not_in_bridge(_root: Path, _bridge_clusters: list[dict]):
        local_rows = [
            ("memory archive continuum", "memory archive continuum", "mat_001", "medium", "[1]", '["memory archive continuum"]', 0.82, "local", ""),
            ("memory archive continuum", "memory archive continuum", "mat_002", "medium", "[1]", '["memory archive continuum"]', 0.81, "local", ""),
        ]
        filtered_rows = _filter_local_rows_not_in_bridge(local_rows, _bridge_clusters)
        material_ids = {row[2] for row in filtered_rows}
        material_rows = [
            row for row in [
                ("mat_001", "One", "Summary", '["archive"]'),
                ("mat_002", "Two", "Summary", '["archive"]'),
            ]
            if row[0] in material_ids
        ]
        return filtered_rows, material_rows

    monkeypatch.setattr("arquimedes.lint._local_rows_not_in_bridge", fake_local_rows_not_in_bridge)

    calls: list[str] = []

    def llm_factory(stage: str):
        def fn(system: str, messages: list[dict]) -> str:
            calls.append(stage)
            return json.dumps({
                "bridge_updates": [
                    {
                        "cluster_id": "concept_001",
                        "new_name": "Archive Spatial Memory",
                        "new_aliases": ["Archive and Space", "Spatial Archive"],
                    }
                ],
                "new_bridges": [
                    {
                        "bridge_ref": "new_bridge_memory",
                        "canonical_name": "Memory Archive Continuum",
                        "aliases": ["Archive Memory Continuum"],
                        "material_ids": ["mat_001", "mat_002"],
                        "source_concepts": [
                            {"material_id": "mat_001", "concept_name": "memory archive continuum"},
                            {"material_id": "mat_002", "concept_name": "memory archive continuum"},
                        ],
                    }
                ],
                "review_updates": [
                    {
                        "cluster_id": "concept_001",
                        "finding_type": "rename",
                        "severity": "low",
                        "status": "validated",
                        "note": "The bridge was renamed and remains coherent across both materials.",
                        "recommendation": "Keep the sharper canonical and retain the current bridge.",
                    }
                ],
                "new_reviews": [
                    {
                        "cluster_ref": "concept_002",
                        "finding_type": "coverage",
                        "severity": "medium",
                        "status": "validated",
                        "note": "Memory and Place now sits next to a clearer neighboring bridge in the audit graph.",
                        "recommendation": "Keep it as-is unless stronger cross-material evidence appears.",
                    },
                    {
                        "cluster_ref": "new_bridge_memory",
                        "finding_type": "new_bridge",
                        "severity": "low",
                        "status": "validated",
                        "note": "The new bridge usefully connects the memory archive thread across both materials.",
                        "recommendation": "Keep this bridge as-is unless stronger contradictory evidence appears.",
                    },
                ],
                "_finished": True,
            })

        return fn

    material_info = _build_material_info(root, [
        {"material_id": "mat_001"},
        {"material_id": "mat_002"},
    ])

    first, discovery = _run_cluster_audit(root, clusters, material_info, "test-route", llm_factory)
    assert len(first) == 3
    assert discovery == 1
    assert len(calls) == 1
    from arquimedes.lint import _cluster_audit_prompt
    prompt_system, prompt_user = _cluster_audit_prompt(
        root,
        root / "derived" / "tmp" / "cluster_audit_input.json",
        root / "derived" / "tmp" / "bridge_concept_clusters.audit.input.jsonl",
        root / "derived" / "tmp" / "cluster_reviews.audit.input.jsonl",
    )
    assert "## TODO" in prompt_system
    assert "- [ ] Audit the staged bridge-memory clusters:" in prompt_system
    assert "- [ ] Create genuinely new bridges only for uncovered local concepts" in prompt_system
    assert "bridge_updates" in prompt_system
    assert "new_bridges" in prompt_system
    assert "review_updates" in prompt_system
    assert "new_reviews" in prompt_system
    assert "Every new_bridges entry that you keep must have exactly one matching new_reviews row" in prompt_system
    assert "cluster_ref must repeat the exact bridge_ref from new_bridges" in prompt_system
    assert '"_finished"' in prompt_system
    assert "new_name" in prompt_system
    assert "new_aliases" in prompt_system
    assert "new_source_concepts" in prompt_system
    assert '"confidence"' not in prompt_system
    assert "You will receive exactly three read-only inputs" in prompt_system
    assert "exactly one canonical audit-log row per bridge cluster" in prompt_system
    assert "do not guess" in prompt_system
    assert "context_requests array with up to 4 read-only SQL-index lookups" in prompt_system
    assert "You will get only one read-only context round" in prompt_system
    assert "Read these files:" in prompt_user
    assert "bridge_concept_clusters.audit.input.jsonl" in prompt_user
    assert "cluster_reviews.audit.input.jsonl" in prompt_user
    assert "Treat it as read-only input" in prompt_user
    assert "only the existing bridge clusters you are allowed to review" in prompt_user
    assert "only the current audit rows for the staged bridge clusters under review" in prompt_user
    assert "Do not invent concepts that are not present in the bridge packet" in prompt_user
    assert "Return final JSON only." in prompt_user
    assert "PROCESS_FINISHED" not in prompt_user
    for record in first:
        assert {"review_id", "cluster_id", "finding_type", "severity", "status", "note", "recommendation", "input_fingerprint", "wiki_path", "context_requested", "context_request_count"} <= set(record)
        assert record["review_id"] == record["cluster_id"]
    stored_clusters = [json.loads(line) for line in (root / "derived" / "bridge_concept_clusters.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
    assert {record["cluster_id"] for record in first} == {"concept_001", "concept_002", next(cluster["cluster_id"] for cluster in stored_clusters if cluster["canonical_name"] == "Memory Archive Continuum")}
    assert {cluster["canonical_name"] for cluster in stored_clusters} >= {"Archive Spatial Memory", "Memory Archive Continuum"}
    assert any(cluster["cluster_id"] == "concept_001" and cluster["canonical_name"] == "Archive Spatial Memory" for cluster in stored_clusters)
    assert any(cluster["cluster_id"].startswith("bridge_") and cluster["canonical_name"] == "Memory Archive Continuum" for cluster in stored_clusters)
    assert not (root / "derived" / "tmp" / "cluster_audit_input.json").exists()
    assert not (root / "derived" / "tmp" / "bridge_concept_clusters.audit.input.jsonl").exists()
    assert not (root / "derived" / "tmp" / "cluster_reviews.audit.input.jsonl").exists()
    assert not (root / "derived" / "lint" / "cluster_audit_last_response.initial.txt").exists()
    assert not (root / "derived" / "lint" / "cluster_audit_last_response.final.txt").exists()
    assert not (root / "derived" / "lint" / "cluster_audit_last_response.parsed.json").exists()

    second, discovery2 = _run_cluster_audit(root, stored_clusters, material_info, "test-route", llm_factory)
    assert len(second) == 3
    assert discovery2 == 0
    assert len(calls) == 1


@pytest.mark.skip(reason="legacy raw-material bridge audit retired")
def test_cluster_audit_rewrites_resolved_reviews_to_validated_and_backfills_missing_clusters(tmp_path, monkeypatch):
    root, _config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)
    clusters = _write_cluster_data(root)
    _write_jsonl(
        root / "derived" / "lint" / "cluster_reviews.jsonl",
        [
            {
                "review_id": "concept_001:old",
                "cluster_id": "concept_001",
                "finding_type": "merge",
                "severity": "medium",
                "status": "open",
                "note": "Needs review.",
                "recommendation": "Check it again.",
                "affected_material_ids": ["mat_001", "mat_002"],
                "affected_concept_names": ["archive and space"],
                "evidence": ["shared archive frame"],
                "input_fingerprint": "old",
                "wiki_path": "wiki/shared/bridge-concepts/archive-and-space.md",
                "_provenance": {"run_at": "2026-01-01T00:00:00+00:00"},
            },
            {
                "review_id": "concept_001:older-duplicate",
                "cluster_id": "concept_001",
                "finding_type": "naming",
                "severity": "high",
                "status": "open",
                "note": "Older duplicate row that should be collapsed away.",
                "recommendation": "Drop this duplicate canonical review.",
                "affected_material_ids": ["mat_001", "mat_002"],
                "affected_concept_names": ["archive and space"],
                "evidence": ["older duplicate"],
                "input_fingerprint": "old-duplicate",
                "wiki_path": "wiki/shared/bridge-concepts/archive-and-space.md",
                "_provenance": {"run_at": "2025-01-01T00:00:00+00:00"},
            }
        ],
    )

    monkeypatch.setattr(
        "arquimedes.lint._local_rows_not_in_bridge",
        lambda *_args, **_kwargs: (
            [("memory archive continuum", "memory archive continuum", "mat_001", "medium", "[1]", '["memory"]', 0.8, "local", "")],
            [("mat_001", "One", "Summary", '["archive"]')],
        ),
    )

    def llm_factory(stage: str):
        def fn(system: str, messages: list[dict]) -> str:
            assert stage == "lint"
            return json.dumps({
                "bridge_updates": [],
                "new_bridges": [],
                "review_updates": [
                    {
                        "cluster_id": "concept_001",
                        "finding_type": "rename",
                        "severity": "low",
                        "status": "resolved",
                        "note": "The earlier rename concern is now satisfied.",
                        "recommendation": "Keep the current bridge naming.",
                    }
                ],
                "new_reviews": [
                    {
                        "cluster_ref": "concept_002",
                        "finding_type": "validated",
                        "severity": "low",
                        "status": "validated",
                        "note": "This bridge still looks acceptable in the current audit pass.",
                        "recommendation": "Keep it as-is unless stronger cross-material evidence appears.",
                    }
                ],
                "_finished": True,
            })

        return fn

    material_info = _build_material_info(root, [
        {"material_id": "mat_001"},
        {"material_id": "mat_002"},
    ])
    reviews, discovery = _run_cluster_audit(root, clusters, material_info, "test-route", llm_factory)

    assert discovery == 0
    assert len(reviews) == 2
    by_cluster = {row["cluster_id"]: row for row in reviews}
    assert {row["review_id"] for row in reviews} == {"concept_001", "concept_002"}
    assert by_cluster["concept_001"]["status"] == "validated"
    assert by_cluster["concept_001"]["note"] == "The earlier rename concern is now satisfied."
    assert by_cluster["concept_002"]["status"] == "validated"
    assert by_cluster["concept_002"]["finding_type"] == "validated"


def test_local_cluster_audit_writes_collection_scoped_findings(tmp_path, monkeypatch):
    root, _config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)
    cluster = {
        "cluster_id": "research__papers__local_0001",
        "domain": "research",
        "collection": "papers",
        "canonical_name": "Archive and Space",
        "slug": "archive-and-space",
        "material_ids": ["mat_001"],
        "source_concepts": [
            {
                "material_id": "mat_001",
                "concept_name": "archive and space",
                "relevance": "high",
                "source_pages": [1],
                "evidence_spans": ["archive and space"],
                "confidence": 0.9,
            }
        ],
        "wiki_path": "wiki/research/papers/concepts/archive-and-space.md",
    }
    _write_jsonl(root / "derived" / "collections" / "research__papers" / "local_concept_clusters.jsonl", [cluster])

    monkeypatch.setattr(
        "arquimedes.lint._local_rows_in_scope_not_in_clusters",
        lambda *_args, **_kwargs: (
            [("archive and space", "archive and space", "mat_001", "high", "[1]", '["archive and space"]', 0.9, "local", "")],
            [("mat_001", "One", "Summary", '["archive"]')],
        ),
    )

    calls: list[str] = []

    def llm_factory(stage: str):
        def fn(system: str, messages: list[dict]) -> str:
            calls.append(stage)
            return json.dumps({
                "bridge_updates": [],
                "new_bridges": [],
                    "review_updates": [],
                    "new_reviews": [
                    {
                            "cluster_ref": "research__papers__local_0001",
                        "finding_type": "validated",
                        "severity": "low",
                        "status": "validated",
                        "note": "This local cluster still looks coherent inside the collection.",
                        "recommendation": "Keep it as-is.",
                    }
                ],
                "_finished": True,
            })

        return fn

    material_info = _build_material_info(root, [{"material_id": "mat_001"}])
    reviews, discovery = _run_cluster_audit(root, [cluster], material_info, "test-route", llm_factory)

    audit_stamp = json.loads((root / "derived" / "collections" / "research__papers" / "local_audit_stamp.json").read_text(encoding="utf-8"))
    stored = [
        json.loads(line)
        for line in (root / "derived" / "lint" / "cluster_reviews.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]

    assert calls == ["lint"]
    assert discovery == 0
    assert [row["cluster_id"] for row in reviews] == ["research__papers__local_0001"]
    assert reviews[0]["wiki_path"] == "wiki/research/papers/concepts/archive-and-space.md"
    assert audit_stamp["cluster_reviews"] == 1
    assert [row["cluster_id"] for row in stored] == ["research__papers__local_0001"]


def test_local_cluster_audit_skips_busy_collection_scope(tmp_path, monkeypatch):
    import arquimedes.lint_cluster_audit as audit_mod

    root, _config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)
    cluster = {
        "cluster_id": "research__papers__local_0001",
        "domain": "research",
        "collection": "papers",
        "canonical_name": "Archive and Space",
        "slug": "archive-and-space",
        "material_ids": ["mat_001"],
        "source_concepts": [
            {
                "material_id": "mat_001",
                "concept_name": "archive and space",
                "relevance": "high",
                "source_pages": [1],
                "evidence_spans": ["archive and space"],
                "confidence": 0.9,
            }
        ],
        "wiki_path": "wiki/research/papers/concepts/archive-and-space.md",
    }
    _write_jsonl(
        root / "derived" / "lint" / "cluster_reviews.jsonl",
        [{
            "review_id": "research__papers__local_0001",
            "cluster_id": "research__papers__local_0001",
            "finding_type": "validated",
            "severity": "low",
            "status": "validated",
            "note": "Stored review.",
            "recommendation": "Keep it.",
            "input_fingerprint": "fp",
            "wiki_path": "wiki/research/papers/concepts/archive-and-space.md",
        }],
    )
    gate_path = root / "derived" / "collections" / "research__papers" / ".audit.lock"
    gate_path.parent.mkdir(parents=True, exist_ok=True)
    gate_path.write_text(f"{os.getpid()}\n", encoding="utf-8")
    monkeypatch.setattr(audit_mod, "_cluster_audit_pid_is_running", lambda pid: pid == os.getpid())

    material_info = _build_material_info(root, [{"material_id": "mat_001"}])
    reviews, discovery = _run_cluster_audit(
        root,
        [cluster],
        material_info,
        "test-route",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("llm should not run")),
    )

    assert discovery == 0
    assert [row["cluster_id"] for row in reviews] == ["research__papers__local_0001"]
    assert not (root / "derived" / "collections" / "research__papers" / "local_audit_stamp.json").exists()


def test_local_cluster_audit_removes_stale_collection_scope_lock(tmp_path, monkeypatch):
    import arquimedes.lint_cluster_audit as audit_mod

    root, _config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)
    cluster = {
        "cluster_id": "research__papers__local_0001",
        "domain": "research",
        "collection": "papers",
        "canonical_name": "Archive and Space",
        "slug": "archive-and-space",
        "material_ids": ["mat_001"],
        "source_concepts": [
            {
                "material_id": "mat_001",
                "concept_name": "archive and space",
                "relevance": "high",
                "source_pages": [1],
                "evidence_spans": ["archive and space"],
                "confidence": 0.9,
            }
        ],
        "wiki_path": "wiki/research/papers/concepts/archive-and-space.md",
    }
    gate_path = root / "derived" / "collections" / "research__papers" / ".audit.lock"
    gate_path.parent.mkdir(parents=True, exist_ok=True)
    gate_path.write_text("999999\n", encoding="utf-8")
    monkeypatch.setattr(audit_mod, "_cluster_audit_pid_is_running", lambda _pid: False)

    calls: list[str] = []

    def llm_factory(stage: str):
        def fn(system: str, messages: list[dict]) -> str:
            calls.append(stage)
            return json.dumps({
                "bridge_updates": [],
                "new_bridges": [],
                "review_updates": [],
                "new_reviews": [
                    {
                        "cluster_ref": "research__papers__local_0001",
                        "finding_type": "validated",
                        "severity": "low",
                        "status": "validated",
                        "note": "This local cluster still looks coherent inside the collection.",
                        "recommendation": "Keep it as-is.",
                    }
                ],
                "_finished": True,
            })

        return fn

    material_info = _build_material_info(root, [{"material_id": "mat_001"}])
    reviews, discovery = _run_cluster_audit(root, [cluster], material_info, "test-route", llm_factory)

    assert calls == ["lint"]
    assert discovery == 0
    assert [row["cluster_id"] for row in reviews] == ["research__papers__local_0001"]
    assert not gate_path.exists()


def test_local_cluster_audit_skips_invalid_bridge_update_and_applies_other_changes(tmp_path, monkeypatch):
    root, _config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)
    clusters = [
        {
            "cluster_id": "research__papers__local_0001",
            "domain": "research",
            "collection": "papers",
            "canonical_name": "Archive and Space",
            "slug": "archive-and-space",
            "aliases": ["Archive and Space"],
            "material_ids": ["mat_001", "mat_002"],
            "source_concepts": [
                {
                    "material_id": "mat_001",
                    "concept_name": "archive and space",
                    "relevance": "high",
                    "source_pages": [1],
                    "evidence_spans": ["archive and space"],
                    "confidence": 0.9,
                },
                {
                    "material_id": "mat_002",
                    "concept_name": "memory and place",
                    "relevance": "medium",
                    "source_pages": [2],
                    "evidence_spans": ["memory and place"],
                    "confidence": 0.8,
                },
            ],
            "wiki_path": "wiki/research/papers/concepts/archive-and-space.md",
        },
        {
            "cluster_id": "research__papers__local_0002",
            "domain": "research",
            "collection": "papers",
            "canonical_name": "Memory and Place",
            "slug": "memory-and-place",
            "aliases": ["Memory and Place"],
            "material_ids": ["mat_001", "mat_002"],
            "source_concepts": [
                {
                    "material_id": "mat_001",
                    "concept_name": "memory and place",
                    "relevance": "medium",
                    "source_pages": [1],
                    "evidence_spans": ["memory and place"],
                    "confidence": 0.8,
                },
                {
                    "material_id": "mat_002",
                    "concept_name": "archive and memory",
                    "relevance": "medium",
                    "source_pages": [2],
                    "evidence_spans": ["archive and memory"],
                    "confidence": 0.8,
                },
            ],
            "wiki_path": "wiki/research/papers/concepts/memory-and-place.md",
        },
    ]
    _write_jsonl(root / "derived" / "collections" / "research__papers" / "local_concept_clusters.jsonl", clusters)

    monkeypatch.setattr(
        "arquimedes.lint._local_rows_in_scope_not_in_clusters",
        lambda *_args, **_kwargs: ([], []),
    )

    calls: list[str] = []

    def llm_factory(stage: str):
        def fn(system: str, messages: list[dict]) -> str:
            calls.append(stage)
            return json.dumps({
                "bridge_updates": [
                    {
                        "cluster_id": "research__papers__local_0001",
                        "removed_materials": ["mat_002"],
                    },
                    {
                        "cluster_id": "research__papers__local_0002",
                        "new_name": "Memory and Place Revised",
                        "new_aliases": ["Memory and Place Revised"],
                    },
                ],
                "new_bridges": [],
                "review_updates": [],
                "new_reviews": [
                    {
                        "cluster_ref": "research__papers__local_0001",
                        "finding_type": "scope_refinement",
                        "severity": "medium",
                        "status": "validated",
                        "note": "Drop the weaker material and keep the core pair.",
                        "recommendation": "Tighten the cluster to its strongest remaining material.",
                    },
                    {
                        "cluster_ref": "research__papers__local_0002",
                        "finding_type": "naming",
                        "severity": "low",
                        "status": "validated",
                        "note": "The revised canonical is clearer.",
                        "recommendation": "Keep the revised naming.",
                    },
                ],
                "_finished": True,
            })

        return fn

    material_info = _build_material_info(root, [{"material_id": "mat_001"}, {"material_id": "mat_002"}])
    reviews, discovery = _run_cluster_audit(root, clusters, material_info, "test-route", llm_factory)

    stored_clusters = [
        json.loads(line)
        for line in (root / "derived" / "collections" / "research__papers" / "local_concept_clusters.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    clusters_by_id = {row["cluster_id"]: row for row in stored_clusters}
    reviews_by_id = {row["cluster_id"]: row for row in reviews}

    assert calls == ["lint"]
    assert discovery == 1
    assert clusters_by_id["research__papers__local_0001"]["material_ids"] == ["mat_001", "mat_002"]
    assert clusters_by_id["research__papers__local_0002"]["canonical_name"] == "Memory and Place Revised"
    assert reviews_by_id["research__papers__local_0001"]["status"] == "open"
    assert reviews_by_id["research__papers__local_0001"]["finding_type"] == "validation_error"
    assert "fewer than two distinct materials" in reviews_by_id["research__papers__local_0001"]["note"]
    assert reviews_by_id["research__papers__local_0002"]["status"] == "validated"


def test_local_cluster_audit_assigns_new_bridge_ids_after_existing_scope(tmp_path, monkeypatch):
    root, _config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)
    clusters = [
        {
            "cluster_id": "research__papers__local_0001",
            "domain": "research",
            "collection": "papers",
            "canonical_name": "Archive and Space",
            "slug": "archive-and-space",
            "aliases": ["Archive and Space"],
            "material_ids": ["mat_001", "mat_002"],
            "source_concepts": [
                {
                    "material_id": "mat_001",
                    "concept_name": "archive and space",
                    "relevance": "high",
                    "source_pages": [1],
                    "evidence_spans": ["archive and space"],
                    "confidence": 0.9,
                },
                {
                    "material_id": "mat_002",
                    "concept_name": "archive and place",
                    "relevance": "medium",
                    "source_pages": [2],
                    "evidence_spans": ["archive and place"],
                    "confidence": 0.8,
                },
            ],
            "wiki_path": "wiki/research/papers/concepts/archive-and-space.md",
        },
    ]
    _write_jsonl(root / "derived" / "collections" / "research__papers" / "local_concept_clusters.jsonl", clusters)

    monkeypatch.setattr(
        "arquimedes.lint._local_rows_in_scope_not_in_clusters",
        lambda *_args, **_kwargs: (
            [
                ("memory and place", "memory and place", "mat_001", "medium", "[3]", '["memory and place"]', 0.8, "local", ""),
                ("memory and place", "memory and place", "mat_002", "medium", "[4]", '["memory and place"]', 0.8, "local", ""),
            ],
            [
                ("mat_001", "One", "Summary", '["archive"]'),
                ("mat_002", "Two", "Summary", '["memory"]'),
            ],
        ),
    )

    def llm_factory(stage: str):
        def fn(system: str, messages: list[dict]) -> str:
            return json.dumps({
                "bridge_updates": [],
                "new_bridges": [
                    {
                        "bridge_ref": "bridge_new_001",
                        "canonical_name": "Memory and Place",
                        "aliases": ["Memory and Place"],
                        "material_ids": ["mat_001", "mat_002"],
                        "source_concepts": [
                            {"material_id": "mat_001", "concept_name": "memory and place"},
                            {"material_id": "mat_002", "concept_name": "memory and place"},
                        ],
                    }
                ],
                "review_updates": [],
                "new_reviews": [
                    {
                        "cluster_ref": "research__papers__local_0001",
                        "finding_type": "validated",
                        "severity": "low",
                        "status": "validated",
                        "note": "The existing local bridge still holds.",
                        "recommendation": "Keep it as-is.",
                    },
                    {
                        "bridge_ref": "bridge_new_001",
                        "finding_type": "new_bridge",
                        "severity": "low",
                        "status": "validated",
                        "note": "A new cross-material bridge is warranted.",
                        "recommendation": "Keep this new local bridge.",
                    },
                ],
                "_finished": True,
            })

        return fn

    material_info = _build_material_info(root, [{"material_id": "mat_001"}, {"material_id": "mat_002"}])
    reviews, discovery = _run_cluster_audit(root, clusters, material_info, "test-route", llm_factory)

    stored_clusters = [
        json.loads(line)
        for line in (root / "derived" / "collections" / "research__papers" / "local_concept_clusters.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    clusters_by_name = {row["canonical_name"]: row for row in stored_clusters}
    reviews_by_id = {row["cluster_id"]: row for row in reviews}

    assert discovery == 1
    assert clusters_by_name["Archive and Space"]["cluster_id"] == "research__papers__local_0001"
    assert clusters_by_name["Memory and Place"]["cluster_id"] == "research__papers__local_0002"
    assert "research__papers__local_0002" in reviews_by_id



def test_cluster_audit_local_rows_only_keep_unbridged_concepts():
    bridge_clusters = [
        {
            "cluster_id": "bridge_0001",
            "source_concepts": [
                {"material_id": "mat_001", "concept_name": "archive and space", "concept_key": "archive and space"},
            ],
        }
    ]
    local_rows = [
        ("archive and space", "archive and space", "mat_001", "high", "[1]", '["evidence"]', 0.9, "local"),
        ("memory and place", "memory and place", "mat_001", "medium", "[1]", '["evidence"]', 0.8, "local"),
    ]
    filtered = _filter_local_rows_not_in_bridge(local_rows, bridge_clusters)
    assert filtered == [("memory and place", "memory and place", "mat_001", "medium", "[1]", '["evidence"]', 0.8, "local")]


def test_cluster_audit_bridge_update_adds_new_source_concepts_for_existing_bridges():
    existing_cluster = {
        "cluster_id": "concept_001",
        "canonical_name": "Archive and Space",
        "aliases": ["Archive and Space"],
        "confidence": 0.9,
        "source_concepts": [
            {
                "material_id": "mat_001",
                "concept_name": "archive and space",
                "concept_key": "archive and space",
                "descriptor": "",
                "relevance": "high",
                "source_pages": [1],
                "evidence_spans": ["archive"],
                "confidence": 0.9,
            }
        ],
    }
    concept_index = {
        ("mat_002", "memory and place"): {
            "concept_name": "memory and place",
            "concept_key": "memory and place",
            "material_id": "mat_002",
            "relevance": "medium",
            "source_pages": "[2]",
            "evidence_spans": '["memory"]',
            "confidence": 0.8,
            "concept_type": "local",
            "descriptor": "",
        }
    }

    updated = _cluster_audit_apply_bridge_update(
        {
            "cluster_id": "concept_001",
            "new_source_concepts": [
                {"material_id": "mat_002", "concept_name": "memory and place"}
            ],
            "new_materials": ["mat_002"],
        },
        existing_cluster,
        concept_index,
        set(),
        label="bridge_updates[1]",
    )

    assert updated["material_ids"] == ["mat_001", "mat_002"]
    assert [source["material_id"] for source in updated["source_concepts"]] == ["mat_001", "mat_002"]


def test_cluster_audit_bridge_update_treats_null_new_name_as_unchanged():
    existing_cluster = {
        "cluster_id": "concept_001",
        "canonical_name": "Archive and Space",
        "aliases": ["Archive and Space"],
        "material_ids": ["mat_001", "mat_002"],
        "source_concepts": [
            {
                "material_id": "mat_001",
                "concept_name": "archive and space",
                "concept_key": "archive and space",
                "descriptor": "",
                "relevance": "high",
                "source_pages": [1],
                "evidence_spans": ["archive"],
                "confidence": 0.9,
            },
            {
                "material_id": "mat_002",
                "concept_name": "memory and place",
                "concept_key": "memory and place",
                "descriptor": "",
                "relevance": "medium",
                "source_pages": [2],
                "evidence_spans": ["memory"],
                "confidence": 0.8,
            },
        ],
    }

    updated = _cluster_audit_apply_bridge_update(
        {
            "cluster_id": "concept_001",
            "new_name": None,
            "new_aliases": ["Archive and Space"],
        },
        existing_cluster,
        {},
        set(),
        label="bridge_updates[1]",
    )

    assert updated["canonical_name"] == "Archive and Space"
    assert updated["slug"] == "archive-and-space"
    assert "None" not in updated["aliases"]


def test_stage_cluster_audit_reviews_input_only_writes_target_reviews(tmp_path):
    target = tmp_path / "derived" / "tmp" / "cluster_reviews.audit.input.jsonl"
    canonical_reviews = {
        "concept_001": {
            "review_id": "concept_001",
            "cluster_id": "concept_001",
            "finding_type": "rename",
            "severity": "low",
            "status": "open",
            "note": "Needs review.",
            "recommendation": "Check it again.",
            "_provenance": {"run_at": "2026-04-08T00:00:00+00:00"},
        },
        "concept_002": {
            "review_id": "concept_002",
            "cluster_id": "concept_002",
            "finding_type": "validated",
            "severity": "low",
            "status": "validated",
            "note": "Stable.",
            "recommendation": "Keep it.",
            "_provenance": {"run_at": "2026-04-08T00:00:00+00:00"},
        },
    }

    _stage_cluster_audit_reviews_input(target, canonical_reviews, {"concept_001"})

    rows = [json.loads(line) for line in target.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(rows) == 1
    assert rows[0]["cluster_id"] == "concept_001"
    assert "_provenance" not in rows[0]


@pytest.mark.skip(reason="legacy raw-material bridge audit retired")
def test_cluster_audit_skips_unchanged_pending_local_packet_after_audit(tmp_path, monkeypatch):
    root, _config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)
    clusters = _write_cluster_data(root)

    from arquimedes.lint import _cluster_audit_cluster_fingerprint

    _write_jsonl(
        root / "derived" / "lint" / "cluster_reviews.jsonl",
        [
            {
                "review_id": "concept_001",
                "cluster_id": "concept_001",
                "finding_type": "validated",
                "severity": "low",
                "status": "validated",
                "note": "This bridge still looks coherent.",
                "recommendation": "Keep it as-is.",
                "input_fingerprint": _cluster_audit_cluster_fingerprint(clusters[0], "test-route"),
                "wiki_path": "wiki/shared/bridge-concepts/archive-and-space.md",
            },
            {
                "review_id": "concept_002",
                "cluster_id": "concept_002",
                "finding_type": "validated",
                "severity": "low",
                "status": "validated",
                "note": "This bridge still looks coherent.",
                "recommendation": "Keep it as-is.",
                "input_fingerprint": _cluster_audit_cluster_fingerprint(clusters[1], "test-route"),
                "wiki_path": "wiki/shared/bridge-concepts/memory-and-place.md",
            },
        ],
    )

    def fake_local_rows_not_in_bridge(_root: Path, _bridge_clusters: list[dict]):
        return (
            [
                ("memory archive continuum", "memory archive continuum", "mat_001", "medium", "[1]", '["memory archive continuum"]', 0.82, "local", ""),
                ("memory archive continuum", "memory archive continuum", "mat_002", "medium", "[1]", '["memory archive continuum"]', 0.81, "local", ""),
            ],
            [
                ("mat_001", "One", "Summary", '["archive"]'),
                ("mat_002", "Two", "Summary", '["archive"]'),
            ],
        )

    monkeypatch.setattr("arquimedes.lint._local_rows_not_in_bridge", fake_local_rows_not_in_bridge)

    calls: list[str] = []

    def llm_factory(stage: str):
        def fn(system: str, messages: list[dict]) -> str:
            calls.append(stage)
            return json.dumps({
                "bridge_updates": [],
                "new_bridges": [],
                "review_updates": [],
                "new_reviews": [],
                "_finished": True,
            })

        return fn

    material_info = _build_material_info(root, [{"material_id": "mat_001"}, {"material_id": "mat_002"}])

    first, discovery = _run_cluster_audit(root, clusters, material_info, "test-route", llm_factory)
    assert discovery == 0
    assert len(first) == 2
    assert len(calls) == 1

    second, discovery2 = _run_cluster_audit(root, clusters, material_info, "test-route", llm_factory)
    assert discovery2 == 0
    assert len(second) == 2
    assert len(calls) == 1


def test_cluster_audit_material_fingerprint_ignores_renames_and_alias_changes():
    from arquimedes.lint import _cluster_audit_cluster_fingerprint, _cluster_audit_target_clusters

    original_cluster = {
        "cluster_id": "concept_001",
        "canonical_name": "Archive and Space",
        "aliases": ["Archive and Space"],
        "material_ids": ["mat_001", "mat_002"],
        "source_concepts": [
            {"material_id": "mat_001", "concept_name": "archive and space", "concept_key": "archive and space"},
            {"material_id": "mat_002", "concept_name": "archive space", "concept_key": "archive space"},
        ],
    }
    renamed_cluster = {
        "cluster_id": "concept_001",
        "canonical_name": "Archive as Institutional Space",
        "aliases": ["Archive as Institutional Space", "Spatial Archive"],
        "material_ids": ["mat_001", "mat_002"],
        "source_concepts": [
            {"material_id": "mat_001", "concept_name": "archive and space", "concept_key": "archive and space"},
            {"material_id": "mat_002", "concept_name": "archive as institution", "concept_key": "archive as institution"},
        ],
    }
    canonical_reviews = {
        "concept_001": {
            "review_id": "concept_001",
            "cluster_id": "concept_001",
            "finding_type": "validated",
            "severity": "low",
            "status": "validated",
            "note": "Looks good.",
            "recommendation": "Keep it as-is.",
            "input_fingerprint": _cluster_audit_cluster_fingerprint(original_cluster, "test-route"),
            "wiki_path": "wiki/shared/bridge-concepts/archive-and-space.md",
        }
    }

    targets, fingerprints = _cluster_audit_target_clusters([renamed_cluster], canonical_reviews, "test-route")

    assert targets == []
    assert fingerprints["concept_001"] == canonical_reviews["concept_001"]["input_fingerprint"]


def test_cluster_audit_open_review_waits_for_changed_input():
    from arquimedes.lint import _cluster_audit_cluster_fingerprint, _cluster_audit_target_clusters

    cluster = {
        "cluster_id": "research__papers__local_0001",
        "domain": "research",
        "collection": "papers",
        "canonical_name": "Archive and Space",
        "material_ids": ["mat_001"],
    }
    canonical_reviews = {
        "research__papers__local_0001": {
            "review_id": "cluster_audit__research__papers__local_0001",
            "cluster_id": "research__papers__local_0001",
            "finding_type": "ambiguous_scope",
            "severity": "medium",
            "status": "open",
            "note": "Needs another pass once new evidence appears.",
            "recommendation": "Wait for changed collection input.",
            "input_fingerprint": _cluster_audit_cluster_fingerprint(cluster, "test-route"),
        }
    }

    targets, fingerprints = _cluster_audit_target_clusters([cluster], canonical_reviews, "test-route")

    assert targets == []
    assert fingerprints["research__papers__local_0001"] == canonical_reviews["research__papers__local_0001"]["input_fingerprint"]


def test_cluster_audit_changed_open_review_still_includes_open_context():
    from arquimedes.lint import _cluster_audit_cluster_fingerprint, _cluster_audit_target_clusters

    original_cluster = {
        "cluster_id": "research__papers__local_0001",
        "domain": "research",
        "collection": "papers",
        "canonical_name": "Archive and Space",
        "material_ids": ["mat_001"],
    }
    changed_cluster = {
        **original_cluster,
        "material_ids": ["mat_001", "mat_002"],
    }
    canonical_reviews = {
        "research__papers__local_0001": {
            "review_id": "cluster_audit__research__papers__local_0001",
            "cluster_id": "research__papers__local_0001",
            "finding_type": "ambiguous_scope",
            "severity": "medium",
            "status": "open",
            "note": "Needs another pass once new evidence appears.",
            "recommendation": "Wait for changed collection input.",
            "input_fingerprint": _cluster_audit_cluster_fingerprint(original_cluster, "test-route"),
        }
    }

    targets, _fingerprints = _cluster_audit_target_clusters([changed_cluster], canonical_reviews, "test-route")

    assert len(targets) == 1
    assert targets[0]["reasons"] == ["changed_since_last_audit", "open_review"]


@pytest.mark.skip(reason="legacy raw-material bridge audit retired")
def test_cluster_audit_drops_new_review_for_rejected_new_bridge_and_still_builds(tmp_path, monkeypatch):
    root, _config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)
    clusters = _write_cluster_data(root)
    _write_jsonl(
        root / "derived" / "lint" / "cluster_reviews.jsonl",
        [
            {
                "review_id": "concept_001:old",
                "cluster_id": "concept_001",
                "finding_type": "rename",
                "severity": "low",
                "status": "open",
                "note": "Needs review.",
                "recommendation": "Check it again.",
                "input_fingerprint": "old",
                "wiki_path": "wiki/shared/bridge-concepts/archive-and-space.md",
            },
            {
                "review_id": "concept_002",
                "cluster_id": "concept_002",
                "finding_type": "coverage",
                "severity": "low",
                "status": "validated",
                "note": "Already reviewed.",
                "recommendation": "Keep it.",
                "input_fingerprint": "stable",
                "wiki_path": "wiki/shared/bridge-concepts/memory-and-place.md",
            },
        ],
    )
    monkeypatch.setattr(
        "arquimedes.lint._local_rows_not_in_bridge",
        lambda *_args, **_kwargs: (
            [("memory archive continuum", "memory archive continuum", "mat_001", "medium", "[1]", '["memory"]', 0.8, "local", "")],
            [("mat_001", "One", "Summary", '["archive"]')],
        ),
    )

    def llm_factory(stage: str):
        def fn(system: str, messages: list[dict]) -> str:
            assert stage == "lint"
            return json.dumps({
                "bridge_updates": [],
                "new_bridges": [
                    {
                        "bridge_ref": "bridge_new_001",
                        "canonical_name": "Broken New Bridge",
                        "aliases": ["Broken New Bridge"],
                        "material_ids": ["mat_001"],
                        "source_concepts": [
                            {"material_id": "mat_001", "concept_name": "memory archive continuum"}
                        ],
                    }
                ],
                "review_updates": [
                    {
                        "cluster_id": "concept_001",
                        "finding_type": "rename",
                        "severity": "low",
                        "status": "validated",
                        "note": "Reviewed.",
                        "recommendation": "Keep it.",
                    },
                    {
                        "cluster_id": "concept_002",
                        "finding_type": "coverage",
                        "severity": "low",
                        "status": "validated",
                        "note": "Still acceptable as-is.",
                        "recommendation": "Keep it.",
                    }
                ],
                "new_reviews": [
                    {
                        "cluster_ref": "bridge_new_001",
                        "finding_type": "new_bridge",
                        "severity": "low",
                        "status": "validated",
                        "note": "Should exist.",
                        "recommendation": "Keep it.",
                    }
                ],
                "_finished": True,
            })

        return fn

    material_info = _build_material_info(root, [
        {"material_id": "mat_001"},
        {"material_id": "mat_002"},
    ])

    reviews, discovery = _run_cluster_audit(root, clusters, material_info, "test-route", llm_factory)

    raw_path = root / "derived" / "lint" / "cluster_audit_last_response.initial.txt"
    parsed_path = root / "derived" / "lint" / "cluster_audit_last_response.parsed.json"
    assert not raw_path.exists()
    assert not parsed_path.exists()
    assert discovery == 0
    assert {row["cluster_id"] for row in reviews} == {"concept_001", "concept_002"}
    stored_clusters = [json.loads(line) for line in (root / "derived" / "bridge_concept_clusters.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
    assert {cluster["cluster_id"] for cluster in stored_clusters} == {"concept_001", "concept_002"}


@pytest.mark.skip(reason="legacy raw-material bridge audit retired")
def test_cluster_audit_accepts_bridge_ref_field_on_new_bridge_reviews(tmp_path, monkeypatch):
    root, _config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)
    clusters = _write_cluster_data(root)
    _write_jsonl(
        root / "derived" / "lint" / "cluster_reviews.jsonl",
        [
            {
                "review_id": "concept_001:old",
                "cluster_id": "concept_001",
                "finding_type": "rename",
                "severity": "low",
                "status": "open",
                "note": "Needs review.",
                "recommendation": "Check it again.",
                "input_fingerprint": "old",
                "wiki_path": "wiki/shared/bridge-concepts/archive-and-space.md",
            }
        ],
    )

    def fake_local_rows_not_in_bridge(_root: Path, _bridge_clusters: list[dict]):
        local_rows = [
            ("memory archive continuum", "memory archive continuum", "mat_001", "medium", "[1]", '["memory archive continuum"]', 0.82, "local", ""),
            ("memory archive continuum", "memory archive continuum", "mat_002", "medium", "[1]", '["memory archive continuum"]', 0.81, "local", ""),
        ]
        filtered_rows = _filter_local_rows_not_in_bridge(local_rows, _bridge_clusters)
        material_ids = {row[2] for row in filtered_rows}
        material_rows = [
            row for row in [
                ("mat_001", "One", "Summary", '["archive"]'),
                ("mat_002", "Two", "Summary", '["archive"]'),
            ]
            if row[0] in material_ids
        ]
        return filtered_rows, material_rows

    monkeypatch.setattr("arquimedes.lint._local_rows_not_in_bridge", fake_local_rows_not_in_bridge)

    def llm_factory(stage: str):
        def fn(system: str, messages: list[dict]) -> str:
            assert stage == "lint"
            return json.dumps({
                "bridge_updates": [
                    {
                        "cluster_id": "concept_001",
                        "new_name": "Archive Spatial Memory",
                        "new_aliases": ["Archive and Space", "Spatial Archive"],
                    }
                ],
                "new_bridges": [
                    {
                        "bridge_ref": "new_bridge_memory",
                        "canonical_name": "Memory Archive Continuum",
                        "aliases": ["Archive Memory Continuum"],
                        "material_ids": ["mat_001", "mat_002"],
                        "source_concepts": [
                            {"material_id": "mat_001", "concept_name": "memory archive continuum"},
                            {"material_id": "mat_002", "concept_name": "memory archive continuum"},
                        ],
                    }
                ],
                "review_updates": [
                    {
                        "cluster_id": "concept_001",
                        "finding_type": "rename",
                        "severity": "low",
                        "status": "validated",
                        "note": "The bridge was renamed and remains coherent across both materials.",
                        "recommendation": "Keep the sharper canonical and retain the current bridge.",
                    }
                ],
                "new_reviews": [
                    {
                        "cluster_ref": "concept_002",
                        "finding_type": "coverage",
                        "severity": "medium",
                        "status": "validated",
                        "note": "Memory and Place now sits next to a clearer neighboring bridge in the audit graph.",
                        "recommendation": "Keep it as-is unless stronger cross-material evidence appears.",
                    },
                    {
                        "bridge_ref": "new_bridge_memory",
                        "finding_type": "new_bridge",
                        "severity": "low",
                        "status": "validated",
                        "note": "The new bridge usefully connects the memory archive thread across both materials.",
                        "recommendation": "Keep this bridge as-is unless stronger contradictory evidence appears.",
                    },
                ],
                "_finished": True,
            })

        return fn

    material_info = _build_material_info(root, [
        {"material_id": "mat_001"},
        {"material_id": "mat_002"},
    ])

    reviews, discovery = _run_cluster_audit(root, clusters, material_info, "test-route", llm_factory)

    assert discovery == 1
    stored_clusters = [json.loads(line) for line in (root / "derived" / "bridge_concept_clusters.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()]
    new_cluster_id = next(cluster["cluster_id"] for cluster in stored_clusters if cluster["canonical_name"] == "Memory Archive Continuum")
    assert any(row["cluster_id"] == new_cluster_id and row["review_id"] == new_cluster_id for row in reviews)


def test_concept_reflection_only_targets_multi_material_clusters_and_skips_unchanged(tmp_path, monkeypatch):
    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)
    _write_cluster_data(root)

    (root / "manifests" / "materials.jsonl").write_text(
        "\n".join(
            [
                json.dumps({
                    "material_id": "mat_001",
                    "file_hash": "hash-001",
                    "relative_path": "Research/One.pdf",
                    "file_type": "pdf",
                    "domain": "research",
                    "collection": "papers",
                    "ingested_at": "2026-01-01T00:00:00+00:00",
                }),
                json.dumps({
                    "material_id": "mat_002",
                    "file_hash": "hash-002",
                    "relative_path": "Research/Two.pdf",
                    "file_type": "pdf",
                    "domain": "research",
                    "collection": "papers",
                    "ingested_at": "2026-01-02T00:00:00+00:00",
                }),
            ]
        ),
        encoding="utf-8",
    )
    for mid, title in [("mat_001", "One"), ("mat_002", "Two")]:
        mat_dir = root / "extracted" / mid
        mat_dir.mkdir(parents=True, exist_ok=True)
        _write_json(
            mat_dir / "meta.json",
            {
                "material_id": mid,
                "file_hash": f"hash-{mid[-3:]}",
                "source_path": f"Research/{title}.pdf",
                "title": title,
                "authors": [f"Author {title}"],
                "year": "2026",
                "page_count": 1,
                "file_type": "pdf",
                "domain": "research",
                "collection": "papers",
                "raw_keywords": ["archive"],
                "raw_document_type": "paper",
                "summary": {"value": "Summary", "provenance": {}},
                "keywords": {"value": ["archive"], "provenance": {}},
                "document_type": {"value": "paper", "provenance": {}},
                "facets": {},
                "_enrichment_stamp": {"prompt_version": "enrich-v1.0", "enrichment_schema_version": "1"},
            },
        )
    _write_json(root / "wiki" / "shared" / "concepts" / "archive-and-space.md", {"body": "Concept page."})
    _write_json(root / "wiki" / "shared" / "concepts" / "memory-and-place.md", {"body": "Concept page."})

    clusters = json.loads((root / "derived" / "bridge_concept_clusters.jsonl").read_text().splitlines()[0]), json.loads((root / "derived" / "bridge_concept_clusters.jsonl").read_text().splitlines()[1])
    material_info = _build_material_info(root, [
        {"material_id": "mat_001"},
        {"material_id": "mat_002"},
    ])
    rebuild_index(config)

    calls: list[str] = []
    prompts: list[str] = []
    systems: list[str] = []

    def llm_factory(stage: str):
        def fn(system: str, messages: list[dict]) -> str:
            calls.append(stage)
            systems.append(system)
            prompt = messages[0]["content"]
            prompts.append(prompt)
            return json.dumps(
                {
                    "main_takeaways": ["Shared concern with spatial archives"],
                    "main_tensions": ["Theory vs. use"],
                    "open_questions": ["What is the archive doing?"],
                    "helpful_new_sources": ["Archive design case studies with annotated floor plans."],
                    "why_this_concept_matters": "It shapes the whole corpus.",
                    "_finished": True,
                }
            )

        return fn

    first = _run_concept_reflections(root, list(clusters), material_info, llm_factory)
    assert len(first) == 1
    assert first[0]["cluster_id"] == "concept_001"
    assert len(calls) == 1
    assert "Concept wiki page:" in prompts[0]
    assert "SQL evidence file:" in prompts[0]
    assert "Work file:" not in prompts[0]
    assert "PROCESS_FINISHED" not in prompts[0]
    assert "Return final JSON only." in prompts[0]
    assert '"_finished"' in systems[0]
    assert {"cluster_id", "slug", "canonical_name", "main_takeaways", "main_tensions", "open_questions", "helpful_new_sources", "why_this_concept_matters", "supporting_material_ids", "supporting_evidence", "input_fingerprint", "wiki_path"} <= set(first[0])
    assert first[0]["helpful_new_sources"] == ["Archive design case studies with annotated floor plans."]
    assert not (root / "derived" / "tmp" / "concept_reflections" / "concept_001.work.json").exists()

    (root / "wiki" / "shared" / "concepts" / "archive-and-space.md").write_text("Concept page updated.", encoding="utf-8")
    second = _run_concept_reflections(root, list(clusters), material_info, llm_factory)
    assert len(second) == 1
    assert len(calls) == 1


def test_collection_reflection_only_targets_multi_material_collections_and_skips_unchanged(tmp_path, monkeypatch):
    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)

    (root / "extracted" / "mat_002").mkdir(parents=True, exist_ok=True)
    _write_json(
        root / "extracted" / "mat_002" / "meta.json",
        {
            "material_id": "mat_002",
            "file_hash": "hash-002",
            "source_path": "Research/Two.pdf",
            "title": "Two",
            "authors": ["Author Two"],
            "year": "2026",
            "page_count": 1,
            "file_type": "pdf",
            "domain": "research",
            "collection": "papers",
            "raw_keywords": ["archive"],
            "raw_document_type": "paper",
            "summary": {"value": "Summary", "provenance": {}},
            "keywords": {"value": ["archive"], "provenance": {}},
            "document_type": {"value": "paper", "provenance": {}},
            "facets": {},
            "_enrichment_stamp": {"prompt_version": "enrich-v1.0", "enrichment_schema_version": "1"},
        },
    )
    _write_jsonl(
        root / "manifests" / "materials.jsonl",
        [
            {
                "material_id": "mat_001",
                "file_hash": "hash-001",
                "relative_path": "Research/One.pdf",
                "file_type": "pdf",
                "domain": "research",
                "collection": "papers",
                "ingested_at": "2026-01-01T00:00:00+00:00",
            },
            {
                "material_id": "mat_002",
                "file_hash": "hash-002",
                "relative_path": "Research/Two.pdf",
                "file_type": "pdf",
                "domain": "research",
                "collection": "papers",
                "ingested_at": "2026-01-02T00:00:00+00:00",
            },
        ],
    )
    _write_json(root / "wiki" / "research" / "papers" / "_index.md", {"body": "Collection home with [archive map](wiki/shared/concepts/archive-and-space.md)."})
    _write_json(root / "wiki" / "shared" / "concepts" / "archive-and-space.md", {"body": "Concept page."})
    _write_json(root / "wiki" / "shared" / "concepts" / "memory-and-place.md", {"body": "Concept page."})
    _write_jsonl(root / "derived" / "bridge_concept_clusters.jsonl", [
        {
            "cluster_id": "concept_001",


            "canonical_name": "Archive and Space",
            "slug": "archive-and-space",
            "material_ids": ["mat_001", "mat_002"],
            "source_concepts": [
                {
                    "material_id": "mat_001",
                    "concept_name": "archive and space",
                    "relevance": "high",
                    "source_pages": [1],
                    "evidence_spans": ["archive and space"],
                    "confidence": 0.9,
                },
                {
                    "material_id": "mat_002",
                    "concept_name": "archive space",
                    "relevance": "medium",
                    "source_pages": [1],
                    "evidence_spans": ["archive space"],
                    "confidence": 0.8,
                },
            ],
        },
        {
            "cluster_id": "concept_002",
            "canonical_name": "Memory and Place",
            "slug": "memory-and-place",
            "material_ids": ["mat_001"],
            "source_concepts": [
                {
                    "material_id": "mat_001",
                    "concept_name": "memory and place",
                    "relevance": "low",
                    "source_pages": [1],
                    "evidence_spans": ["memory and place"],
                    "confidence": 0.7,
                }
            ],
        },
    ])

    calls: list[str] = []
    prompts: list[str] = []
    systems: list[str] = []

    def llm_factory(stage: str):
        def fn(system: str, messages: list[dict]) -> str:
            calls.append(stage)
            systems.append(system)
            prompt = messages[0]["content"]
            prompts.append(prompt)
            page_match = re.search(r"- Collection wiki page: (.+)", prompt)
            assert page_match is not None
            page_copy_text = Path(page_match.group(1).strip()).read_text(encoding="utf-8")
            assert "archive map" in page_copy_text
            assert ".md" not in page_copy_text
            assert "Collection wiki page:" in prompt
            assert "SQL evidence file:" in prompt
            assert "Work file:" not in prompt
            assert "main takeaways" in prompt.lower()
            assert "important local clusters" in prompt.lower()
            assert "methodological conclusions" in prompt.lower()
            assert "main content learnings" in prompt.lower()
            assert "new_materials" in prompt
            assert "old_materials" in prompt
            assert "chunks are only secondary support" in prompt.lower()
            assert "why this collection matters" in prompt.lower()
            assert "helpful new sources" in prompt.lower()
            return json.dumps(
                {
                    "main_takeaways": ["The collection centers archival space."],
                    "main_tensions": ["Theory vs use"],
                    "important_material_ids": ["mat_001", "mat_002"],
                    "important_cluster_ids": ["concept_001"],
                    "open_questions": ["What else is in the archive?"],
                    "helpful_new_sources": ["comparative archive conversion studies"],
                    "why_this_collection_matters": "It shapes the collection as a whole.",
                    "_finished": True,
                }
            )

        return fn

    material_info = _build_material_info(root, [
        {"material_id": "mat_001"},
        {"material_id": "mat_002"},
    ])
    groups = {
        ("research", "papers"): [
            material_info["mat_001"] | {"material_id": "mat_001"},
            material_info["mat_002"] | {"material_id": "mat_002"},
        ]
    }
    clusters = json.loads((root / "derived" / "bridge_concept_clusters.jsonl").read_text().splitlines()[0]), json.loads((root / "derived" / "bridge_concept_clusters.jsonl").read_text().splitlines()[1])
    rebuild_index(config)

    with ReflectionIndexTool(root) as tool:
        first = _run_collection_reflections(root, groups, list(clusters), llm_factory, tool)
    assert len(first) == 1
    assert first[0]["collection_key"] == "research/papers"
    assert len(calls) == 1
    assert first[0]["helpful_new_sources"] == ["comparative archive conversion studies"]
    assert first[0]["why_this_collection_matters"] == "It shapes the collection as a whole."
    assert {"collection_key", "domain", "collection", "main_takeaways", "main_tensions", "important_material_ids", "important_cluster_ids", "open_questions", "helpful_new_sources", "why_this_collection_matters", "input_fingerprint", "wiki_path"} <= set(first[0])
    assert "PROCESS_FINISHED" not in prompts[0]
    assert "Return final JSON only." in prompts[0]
    assert '"_finished"' in systems[0]

    second = _run_collection_reflections(root, groups, list(clusters), llm_factory, tool)
    assert len(second) == 1
    assert len(calls) == 1
    assert not (root / "derived" / "tmp" / "collection_reflections" / "research__papers.evidence.json").exists()
    assert not (root / "derived" / "tmp" / "collection_reflections" / "research__papers.work.json").exists()


def test_collection_reflection_null_fields_preserve_existing_row_values(tmp_path, monkeypatch):
    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)

    (root / "extracted" / "mat_002").mkdir(parents=True, exist_ok=True)
    _write_json(
        root / "extracted" / "mat_002" / "meta.json",
        {
            "material_id": "mat_002",
            "file_hash": "hash-002",
            "source_path": "Research/Two.pdf",
            "title": "Two",
            "authors": ["Author Two"],
            "year": "2026",
            "page_count": 1,
            "file_type": "pdf",
            "domain": "research",
            "collection": "papers",
            "raw_keywords": ["archive"],
            "raw_document_type": "paper",
            "summary": {"value": "Summary", "provenance": {}},
            "keywords": {"value": ["archive"], "provenance": {}},
            "document_type": {"value": "paper", "provenance": {}},
            "facets": {},
            "_enrichment_stamp": {"prompt_version": "enrich-v1.0", "enrichment_schema_version": "1"},
        },
    )
    _write_jsonl(
        root / "manifests" / "materials.jsonl",
        [
            {
                "material_id": "mat_001",
                "file_hash": "hash-001",
                "relative_path": "Research/One.pdf",
                "file_type": "pdf",
                "domain": "research",
                "collection": "papers",
                "ingested_at": "2026-01-01T00:00:00+00:00",
            },
            {
                "material_id": "mat_002",
                "file_hash": "hash-002",
                "relative_path": "Research/Two.pdf",
                "file_type": "pdf",
                "domain": "research",
                "collection": "papers",
                "ingested_at": "2026-01-02T00:00:00+00:00",
            },
        ],
    )
    _write_json(root / "wiki" / "research" / "papers" / "_index.md", {"body": "Collection home."})
    _write_json(root / "wiki" / "shared" / "concepts" / "archive-and-space.md", {"body": "Concept page."})
    _write_json(root / "wiki" / "shared" / "concepts" / "memory-and-place.md", {"body": "Concept page."})
    _write_jsonl(root / "derived" / "bridge_concept_clusters.jsonl", [
        {
            "cluster_id": "concept_001",
            "canonical_name": "Archive and Space",
            "slug": "archive-and-space",
            "material_ids": ["mat_001", "mat_002"],
            "source_concepts": [
                {
                    "material_id": "mat_001",
                    "concept_name": "archive and space",
                    "relevance": "high",
                    "source_pages": [1],
                    "evidence_spans": ["archive and space"],
                    "confidence": 0.9,
                },
                {
                    "material_id": "mat_002",
                    "concept_name": "archive space",
                    "relevance": "medium",
                    "source_pages": [1],
                    "evidence_spans": ["archive space"],
                    "confidence": 0.8,
                },
            ],
        },
        {
            "cluster_id": "concept_002",
            "canonical_name": "Memory and Place",
            "slug": "memory-and-place",
            "material_ids": ["mat_001"],
            "source_concepts": [
                {
                    "material_id": "mat_001",
                    "concept_name": "memory and place",
                    "relevance": "low",
                    "source_pages": [1],
                    "evidence_spans": ["memory and place"],
                    "confidence": 0.7,
                }
            ],
        },
    ])
    _write_jsonl(
        root / "derived" / "lint" / "collection_reflections.jsonl",
        [
            {
                "collection_key": "research/papers",
                "domain": "research",
                "collection": "papers",
                "main_takeaways": ["Stored takeaway"],
                "main_tensions": ["Stored tension"],
                "important_material_ids": ["mat_001"],
                "important_cluster_ids": ["concept_001"],
                "open_questions": ["Stored question"],
                "helpful_new_sources": ["Stored source"],
                "why_this_collection_matters": "Stored reason.",
                "input_fingerprint": "stale-fingerprint",
                "wiki_path": "wiki/research/papers/_index.md",
            }
        ],
    )

    calls: list[str] = []

    def llm_factory(stage: str):
        def fn(system: str, messages: list[dict]) -> str:
            calls.append(messages[0]["content"])
            return json.dumps(
                {
                    "main_takeaways": None,
                    "main_tensions": ["Updated tension"],
                    "important_material_ids": None,
                    "important_cluster_ids": ["concept_001"],
                    "open_questions": None,
                    "helpful_new_sources": None,
                    "why_this_collection_matters": None,
                    "_finished": True,
                }
            )

        return fn

    material_info = _build_material_info(root, [
        {"material_id": "mat_001"},
        {"material_id": "mat_002"},
    ])
    groups = {
        ("research", "papers"): [
            material_info["mat_001"] | {"material_id": "mat_001"},
            material_info["mat_002"] | {"material_id": "mat_002"},
        ]
    }
    clusters = json.loads((root / "derived" / "bridge_concept_clusters.jsonl").read_text().splitlines()[0]), json.loads((root / "derived" / "bridge_concept_clusters.jsonl").read_text().splitlines()[1])

    result = _run_collection_reflections(root, groups, list(clusters), llm_factory)

    assert len(result) == 1
    assert len(calls) == 1
    assert result[0]["main_takeaways"] == ["Stored takeaway"]
    assert result[0]["main_tensions"] == ["Updated tension"]
    assert result[0]["important_material_ids"] == ["mat_001"]
    assert result[0]["important_cluster_ids"] == ["concept_001"]
    assert result[0]["open_questions"] == ["Stored question"]
    assert result[0]["helpful_new_sources"] == ["Stored source"]
    assert result[0]["why_this_collection_matters"] == "Stored reason."

    stored = [
        json.loads(line)
        for line in (root / "derived" / "lint" / "collection_reflections.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert stored[0]["main_takeaways"] == ["Stored takeaway"]
    assert stored[0]["main_tensions"] == ["Updated tension"]
    assert stored[0]["important_material_ids"] == ["mat_001"]
    assert stored[0]["important_cluster_ids"] == ["concept_001"]
    assert stored[0]["open_questions"] == ["Stored question"]
    assert stored[0]["helpful_new_sources"] == ["Stored source"]
    assert stored[0]["why_this_collection_matters"] == "Stored reason."


def test_collection_reflection_evidence_uses_material_conclusions_and_keeps_chunks_small(tmp_path, monkeypatch):
    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)

    clusters = [
        {
            "cluster_id": "research__papers__local_0001",
            "canonical_name": "Archive and Space",
            "domain": "research",
            "collection": "papers",
            "material_ids": ["mat_001"],
            "source_concepts": [
                {
                    "material_id": "mat_001",
                    "concept_name": "archive and space",
                    "descriptor": "archive as spatial form",
                    "source_pages": [1],
                    "evidence_spans": ["archive and space"],
                },
            ],
        }
    ]
    metas = [
        {
            "material_id": "mat_001",
            "title": "One",
            "summary": "Summary",
            "keywords": ["archive"],
            "methodological_conclusions": ["Use dispersed holdings as evidence."],
            "main_content_learnings": ["Archives shape spatial knowledge."],
        },
        {
            "material_id": "mat_002",
            "title": "Two",
            "summary": "Summary",
            "keywords": ["memory"],
            "methodological_conclusions": ["Compare multiple repositories."],
            "main_content_learnings": ["Collections can reframe historiography."],
        },
    ]

    class Tool:
        def open_record(self, kind, ident):
            return {"reflection": {
                "main_takeaways": "[\"Shared archive frame\"]",
                "main_tensions": "[\"Theory vs use\"]",
                "open_questions": "[\"What else is in the archive?\"]",
                "why_this_concept_matters": "It matters.",
            }}

        def search_material_evidence(self, kind, material_id, query, limit=2):
            return [
                {"chunk_id": f"{material_id}-chunk-{idx}", "text": f"{query} supporting text {idx}"}
                for idx in range(4)
            ]

        def _material_evidence(self, material_id, query_terms, chunk_limit=2, annotation_limit=3, figure_limit=2, concept_limit=3):
            return {
                "chunks": [
                    {"chunk_id": f"{material_id}-fb-{idx}", "text": f"fallback text {idx}"}
                    for idx in range(4)
                ],
                "annotations": [
                    {"page": 1, "quoted_text": "note", "comment": "comment"},
                ],
                "figures": [
                    {"figure_id": "fig-1", "description": "figure desc"},
                ],
                "concepts": [
                    {"concept_name": "archive and space", "descriptor": "spatial archive", "evidence_spans": ["archive and space"]},
                ],
            }

    payload = _build_collection_reflection_evidence_payload(
        root,
        "research",
        "papers",
        metas,
        clusters,
        {"important_material_ids": ["mat_001"]},
        Tool(),
    )
    assert payload["local_clusters"][0]["main_takeaways"] == ["Shared archive frame"]
    assert len(payload["new_materials"]) == 1
    assert len(payload["old_materials"]) == 1
    first_new = payload["new_materials"][0]
    assert first_new["methodological_conclusions"] == ["Compare multiple repositories."]
    assert first_new["main_content_learnings"] == ["Collections can reframe historiography."]
    assert len(first_new["evidence"]["chunks"]) <= 2
    first_old = payload["old_materials"][0]
    assert "evidence" not in first_old
    assert "summary" not in first_old
    assert first_old["methodological_conclusions"] == ["Use dispersed holdings as evidence."]


@pytest.mark.skip(reason="legacy raw-material bridge fixture retired")
def test_reflection_index_tool_open_collection_includes_collection_prose(tmp_path, monkeypatch):
    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)

    second_manifest = {
        "material_id": "mat_002",
        "file_hash": "hash-002",
        "relative_path": "Research/Two.pdf",
        "file_type": "pdf",
        "domain": "research",
        "collection": "papers",
        "ingested_at": "2026-01-02T00:00:00+00:00",
    }
    _write_jsonl(
        root / "manifests" / "materials.jsonl",
        [
            json.loads((root / "manifests" / "materials.jsonl").read_text().splitlines()[0]),
            second_manifest,
        ],
    )
    _write_json(
        root / "extracted" / "mat_002" / "meta.json",
        {
            "material_id": "mat_002",
            "file_hash": "hash-002",
            "source_path": "Research/Two.pdf",
            "title": "Two",
            "authors": ["Author Two"],
            "year": "2026",
            "page_count": 1,
            "file_type": "pdf",
            "domain": "research",
            "collection": "papers",
            "raw_keywords": ["space"],
            "raw_document_type": "paper",
            "summary": {"value": "Second summary", "provenance": {}},
            "keywords": {"value": ["space"], "provenance": {}},
            "document_type": {"value": "paper", "provenance": {}},
            "facets": {},
            "_enrichment_stamp": {"prompt_version": "enrich-v1.0", "enrichment_schema_version": "1"},
        },
    )
    _write_jsonl(
        root / "extracted" / "mat_002" / "pages.jsonl",
        [
            {
                "page_number": 1,
                "text": "Another page.",
                "headings": ["Intro"],
                "section_boundaries": [],
                "figure_refs": [],
                "table_refs": [],
                "thumbnail_path": "",
                "has_annotations": False,
                "annotation_ids": [],
            }
        ],
    )
    _write_jsonl(
        root / "extracted" / "mat_002" / "chunks.jsonl",
        [
            {
                "chunk_id": "chk_00001",
                "text": "Another chunk.",
                "source_pages": [1],
                "emphasized": False,
                "summary": {"value": "Chunk summary", "provenance": {}},
                "keywords": {"value": ["space"], "provenance": {}},
                "content_class": "argument",
            }
        ],
    )

    rebuild_index(config)
    (root / "derived").mkdir(exist_ok=True)
    (root / "derived" / "bridge_concept_clusters.jsonl").write_text("", encoding="utf-8")
    (root / "derived" / "lint").mkdir(parents=True, exist_ok=True)
    _write_jsonl(
        root / "derived" / "lint" / "collection_reflections.jsonl",
        [
            {
                "collection_key": "research/papers",
                "domain": "research",
                "collection": "papers",
                "main_takeaways": ["The collection centers archival space."],
                "main_tensions": ["Theory vs use"],
                "important_material_ids": ["mat_001", "mat_002"],
                "important_cluster_ids": [],
                "open_questions": ["What else is in the archive?"],
                "helpful_new_sources": ["Comparative archive reuse case studies."],
                "why_this_collection_matters": "It gives the papers collection a coherent semantic role.",
                "input_fingerprint": "fp-collection",
                "wiki_path": "wiki/research/papers/_index.md",
            }
        ],
    )
    memory_rebuild(config)

    with ReflectionIndexTool(root) as tool:
        record = tool.open_record("collection", "research/papers")

    assert record is not None
    assert record["reflection"]["helpful_new_sources"] == ["Comparative archive reuse case studies."]
    assert record["reflection"]["why_this_collection_matters"] == "It gives the papers collection a coherent semantic role."


def test_graph_reflection_writes_page_and_skips_unchanged(tmp_path, monkeypatch):
    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)
    clusters = _write_cluster_data(root)
    _write_json(root / "wiki" / "research" / "papers" / "_index.md", {"body": "Collection home."})

    deterministic_report = {
        "summary": {
            "materials": 2,
            "extracted_materials": 2,
            "wiki_pages": 4,
            "clusters": 2,
            "issues": 1,
            "high": 1,
            "medium": 0,
            "low": 0,
        }
    }
    concept_refs = [
        {
            "cluster_id": "concept_001",
            "slug": "archive-and-space",
            "canonical_name": "Archive and Space",
            "main_takeaways": ["Shared concern with spatial archives"],
            "main_tensions": ["Theory vs use"],
            "open_questions": ["What is the archive doing?"],
            "why_this_concept_matters": "It shapes the whole corpus.",
            "supporting_material_ids": ["mat_001", "mat_002"],
            "supporting_evidence": ["shared archive frame"],
            "input_fingerprint": "def",
            "wiki_path": "wiki/shared/bridge-concepts/archive-and-space.md",
        }
    ]
    collection_refs = [
        {
            "collection_key": "research/papers",
            "domain": "research",
            "collection": "papers",
            "main_takeaways": ["The collection centers archival space."],
            "main_tensions": ["Theory vs use"],
            "important_material_ids": ["mat_001", "mat_002"],
            "important_cluster_ids": ["concept_001"],
            "open_questions": ["What else is in the archive?"],
            "input_fingerprint": "ghi",
            "wiki_path": "wiki/research/papers/_index.md",
        }
    ]

    calls: list[str] = []
    prompts: list[str] = []
    systems: list[str] = []

    def llm_factory(stage: str):
        def fn(system: str, messages: list[dict]) -> str:
            calls.append(stage)
            systems.append(system)
            prompt = messages[0]["content"]
            prompts.append(prompt)
            assert "Graph-state packet:" in prompt
            assert "Current graph findings file:" in prompt
            assert "Work file:" not in prompt
            return json.dumps({
                "findings": [
                    {
                        "finding_type": "bridge_gap",
                        "severity": "medium",
                        "summary": "Archive and Space still wants a stronger link to architectural form.",
                        "details": "The cluster remains semantically useful but could use a sharper architectural anchor.",
                        "affected_material_ids": ["mat_001", "mat_002"],
                        "affected_cluster_ids": ["concept_001"],
                        "candidate_future_sources": ["architectural typology"],
                        "candidate_bridge_links": ["spatial memory"],
                    }
                ],
                "_finished": True,
            })

        return fn

    first = _run_graph_reflection(root, deterministic_report, concept_refs, collection_refs, clusters, _load_manifest(root), llm_factory)
    assert first["graph_maintenance"] == 1
    assert len(calls) == 1
    assert "PROCESS_FINISHED" not in prompts[0]
    assert "Return final JSON only." in prompts[0]
    assert '"_finished"' in systems[0]
    page_path = root / "wiki" / "shared" / "maintenance" / "graph-health.md"
    assert not page_path.exists()
    findings_path = root / "derived" / "lint" / "graph_findings.jsonl"
    assert findings_path.exists()
    findings = findings_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(findings) == 1
    assert "Archive and Space still wants a stronger link to architectural form." in findings[0]

    second = _run_graph_reflection(root, deterministic_report, concept_refs, collection_refs, clusters, _load_manifest(root), llm_factory)
    assert second["graph_maintenance"] == 0
    assert len(calls) == 1


def test_graph_reflection_null_findings_preserve_existing_rows(tmp_path, monkeypatch):
    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)
    clusters = _write_cluster_data(root)
    _write_json(root / "wiki" / "research" / "papers" / "_index.md", {"body": "Collection home."})
    _write_jsonl(
        root / "derived" / "lint" / "graph_findings.jsonl",
        [
            {
                "finding_id": "graph:0",
                "finding_type": "bridge_gap",
                "severity": "medium",
                "summary": "Stored summary.",
                "details": "Stored details.",
                "affected_material_ids": ["mat_001", "mat_002"],
                "affected_cluster_ids": ["concept_001"],
                "candidate_future_sources": ["architectural typology"],
                "candidate_bridge_links": ["spatial memory"],
                "input_fingerprint": "stale-fingerprint",
            }
        ],
    )

    deterministic_report = {
        "summary": {
            "materials": 2,
            "extracted_materials": 2,
            "wiki_pages": 5,
            "clusters": 2,
            "issues": 1,
            "high": 1,
            "medium": 0,
            "low": 0,
        }
    }
    concept_refs = [
        {
            "cluster_id": "concept_001",
            "slug": "archive-and-space",
            "canonical_name": "Archive and Space",
            "main_takeaways": ["Shared concern with spatial archives"],
            "main_tensions": ["Theory vs use"],
            "open_questions": ["What is the archive doing?"],
            "why_this_concept_matters": "It shapes the whole corpus.",
            "supporting_material_ids": ["mat_001", "mat_002"],
            "supporting_evidence": ["shared archive frame"],
            "input_fingerprint": "def",
            "wiki_path": "wiki/shared/bridge-concepts/archive-and-space.md",
        }
    ]
    collection_refs = [
        {
            "collection_key": "research/papers",
            "domain": "research",
            "collection": "papers",
            "main_takeaways": ["The collection centers archival space."],
            "main_tensions": ["Theory vs use"],
            "important_material_ids": ["mat_001", "mat_002"],
            "important_cluster_ids": ["concept_001"],
            "open_questions": ["What else is in the archive?"],
            "input_fingerprint": "ghi",
            "wiki_path": "wiki/research/papers/_index.md",
        }
    ]

    calls: list[str] = []

    def llm_factory(stage: str):
        def fn(system: str, messages: list[dict]) -> str:
            calls.append(messages[0]["content"])
            return json.dumps({"findings": None, "_finished": True})

        return fn

    result = _run_graph_reflection(root, deterministic_report, concept_refs, collection_refs, clusters, _load_manifest(root), llm_factory)

    assert result["graph_maintenance"] == 1
    assert len(calls) == 1
    stored = [
        json.loads(line)
        for line in (root / "derived" / "lint" / "graph_findings.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert stored[0]["finding_id"] == "graph:0"
    assert stored[0]["summary"] == "Stored summary."
    assert stored[0]["details"] == "Stored details."
    assert stored[0]["candidate_future_sources"] == ["architectural typology"]
    assert stored[0]["candidate_bridge_links"] == ["spatial memory"]
    assert stored[0]["input_fingerprint"] != "stale-fingerprint"
@pytest.mark.skip(reason="legacy raw-material bridge fixture retired")
def test_reflection_index_tool_supports_read_only_search_and_open_record(tmp_path, monkeypatch):
    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)
    _write_cluster_data(root)
    rebuild_index(config)

    with ReflectionIndexTool(root) as tool:
        materials = tool.search_materials("archive", limit=2)
        assert materials and materials[0]["material_id"] == "mat_001"

        concepts = tool.search_concepts("archive", limit=2)
        assert concepts and any(item.get("kind") in {"cluster", "concept"} for item in concepts)

        collections = tool.search_collections("papers", limit=2)
        assert collections and collections[0]["collection_key"] == "research/papers"

        record = tool.open_record("material", "mat_001")
        assert record is not None
        assert record["evidence"]["chunks"]


def test_concept_reflection_includes_prior_reflection_and_rich_evidence(tmp_path, monkeypatch):
    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)
    _write_cluster_data(root)
    rebuild_index(config)

    concept_page = root / "wiki" / "shared" / "concepts" / "archive-and-space.md"
    concept_page.write_text(
        "Concept page with [linked material](wiki/research/papers/One.md).\n\n"
        "<!-- phase6:concept-reflection:start -->\n"
        "## Phase 6 Reflection\n"
        "- Prior note\n"
        "<!-- phase6:concept-reflection:end -->\n",
        encoding="utf-8",
    )

    calls: list[str] = []

    def llm_factory(stage: str):
        def fn(system: str, messages: list[dict]) -> str:
            calls.append(messages[0]["content"])
            prompt = messages[0]["content"]
            page_match = re.search(r"- Concept wiki page: (.+)", prompt)
            assert page_match is not None
            page_copy_text = Path(page_match.group(1).strip()).read_text(encoding="utf-8")
            assert "linked material" in page_copy_text
            assert ".md" not in page_copy_text
            return json.dumps(
                {
                    "main_takeaways": ["Shared concern with spatial archives"],
                    "main_tensions": ["Theory vs. use"],
                    "open_questions": ["What is the archive doing?"],
                    "helpful_new_sources": ["Recent archive conversion case studies."],
                    "why_this_concept_matters": "It shapes the whole corpus.",
                    "_finished": True,
                }
            )

        return fn

    material_info = _build_material_info(root, [
        {"material_id": "mat_001"},
        {"material_id": "mat_002"},
    ])
    clusters = json.loads((root / "derived" / "bridge_concept_clusters.jsonl").read_text().splitlines()[0]), json.loads((root / "derived" / "bridge_concept_clusters.jsonl").read_text().splitlines()[1])

    with ReflectionIndexTool(root) as tool:
        first = _run_concept_reflections(root, list(clusters), material_info, llm_factory, tool)

    assert len(first) == 1
    assert len(calls) == 1
    assert "Concept wiki page:" in calls[0]
    assert "SQL evidence file:" in calls[0]
    assert "Work file:" not in calls[0]
    assert "Return final JSON only." in calls[0]


def test_concept_reflection_null_fields_preserve_existing_row_values(tmp_path, monkeypatch):
    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)
    _write_cluster_data(root)
    rebuild_index(config)

    _write_jsonl(
        root / "derived" / "lint" / "concept_reflections.jsonl",
        [
            {
                "cluster_id": "concept_001",
                "slug": "archive-and-space",
                "canonical_name": "Archive and Space",
                "main_takeaways": ["Stored takeaway"],
                "main_tensions": ["Stored tension"],
                "open_questions": ["Stored question"],
                "helpful_new_sources": ["Stored source idea"],
                "why_this_concept_matters": "Stored reason.",
                "supporting_material_ids": ["mat_001", "mat_002"],
                "supporting_evidence": ["archive and space", "archive space"],
                "input_fingerprint": "stale-fingerprint",
                "wiki_path": "wiki/shared/concepts/archive-and-space.md",
            }
        ],
    )

    calls: list[str] = []

    def llm_factory(stage: str):
        def fn(system: str, messages: list[dict]) -> str:
            calls.append(messages[0]["content"])
            return json.dumps(
                {
                    "main_takeaways": None,
                    "main_tensions": ["Updated tension"],
                    "open_questions": None,
                    "helpful_new_sources": None,
                    "why_this_concept_matters": None,
                    "_finished": True,
                }
            )

        return fn

    material_info = _build_material_info(root, [
        {"material_id": "mat_001"},
        {"material_id": "mat_002"},
    ])
    clusters = json.loads((root / "derived" / "bridge_concept_clusters.jsonl").read_text().splitlines()[0]), json.loads((root / "derived" / "bridge_concept_clusters.jsonl").read_text().splitlines()[1])

    first = _run_concept_reflections(root, list(clusters), material_info, llm_factory)

    assert len(first) == 1
    assert len(calls) == 1
    assert first[0]["main_takeaways"] == ["Stored takeaway"]
    assert first[0]["main_tensions"] == ["Updated tension"]
    assert first[0]["open_questions"] == ["Stored question"]
    assert first[0]["helpful_new_sources"] == ["Stored source idea"]
    assert first[0]["why_this_concept_matters"] == "Stored reason."

    stored = [
        json.loads(line)
        for line in (root / "derived" / "lint" / "concept_reflections.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert stored[0]["main_takeaways"] == ["Stored takeaway"]
    assert stored[0]["main_tensions"] == ["Updated tension"]
    assert stored[0]["open_questions"] == ["Stored question"]
    assert stored[0]["helpful_new_sources"] == ["Stored source idea"]
    assert stored[0]["why_this_concept_matters"] == "Stored reason."


def test_concept_reflection_evidence_builder_respects_chunk_budget_and_caps(tmp_path, monkeypatch):
    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)

    cluster = {
        "cluster_id": "concept_001",
        "canonical_name": "Archive and Space",
        "slug": "archive-and-space",
        "aliases": ["archive space"],
        "source_concepts": [
            {
                "material_id": f"mat_{idx:03d}",
                "concept_name": f"archive concept {idx}",
                "concept_type": "bridge" if idx % 3 == 0 else "local",
                "relevance": "high" if idx % 2 == 0 else "medium",
                "source_pages": [1],
                "evidence_spans": [f"archive span {idx}"],
                "confidence": 0.5 + (idx * 0.01),
            }
            for idx in range(1, 13)
        ],
    }
    material_info = {
        f"mat_{idx:03d}": {
            "title": f"Material {idx}",
            "summary": f"Summary {idx}",
            "keywords": ["archive"],
        }
        for idx in range(1, 13)
    }

    payload = _build_concept_reflection_evidence_payload(cluster, material_info, None)

    assert {"cluster_id", "canonical_name", "slug", "aliases", "materials"} <= set(payload)
    assert "member_local_clusters" not in payload
    assert "bridge_synthesis" not in payload
    assert len(payload["materials"]) == 12
    assert all(set(material) == {"material_id", "title", "summary", "keywords", "evidence"} for material in payload["materials"])
    assert all("chunks" in material["evidence"] for material in payload["materials"])
    assert all("figures" in material["evidence"] for material in payload["materials"])
    assert all("concepts" in material["evidence"] for material in payload["materials"])


def test_concept_reflection_evidence_builder_uses_material_search_for_chunks(tmp_path, monkeypatch):
    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)

    cluster = {
        "cluster_id": "concept_001",
        "canonical_name": "Archive and Space",
        "slug": "archive-and-space",
        "aliases": ["archive space"],
        "source_concepts": [
            {
                "material_id": "mat_001",
                "concept_name": "archive and space",
                "concept_type": "bridge",
                "relevance": "high",
                "source_pages": [1],
                "evidence_spans": ["archive and space"],
                "confidence": 1.0,
            }
        ],
    }
    material_info = {
        "mat_001": {
            "title": "Material 1",
            "summary": "Summary 1",
            "keywords": ["archive"],
        }
    }

    calls: list[tuple[str, str, str, int]] = []

    class Tool:
        def search_material_evidence(self, kind: str, material_id: str, query: str, limit: int = 5) -> list[dict]:
            calls.append((kind, material_id, query, limit))
            if kind == "chunk" and query == "Archive and Space":
                return [
                    {
                        "chunk_id": "chk_search_001",
                        "summary": "Search summary",
                        "source_pages": [1],
                        "emphasized": False,
                        "content_class": "argument",
                        "text": "Search chunk text for Archive and Space.",
                        "snippet": "Search chunk text for Archive and Space.",
                        "rank": 1,
                    }
                ]
            return []

        def _material_evidence(self, material_id: str, query_terms: list[str] | None = None, chunk_limit: int = 2, annotation_limit: int = 2, figure_limit: int = 2, concept_limit: int = 4) -> dict:
            return {"chunks": [], "annotations": [], "figures": [], "concepts": []}

    payload = _build_concept_reflection_evidence_payload(cluster, material_info, Tool())

    assert calls
    assert calls[0][0] == "chunk"
    assert calls[0][1] == "mat_001"
    assert calls[0][2] == "Archive and Space"
    assert payload["materials"][0]["evidence"]["chunks"][0]["text"] == "Search chunk text for Archive and Space."


def test_concept_reflection_chunk_search_fills_remaining_slots_with_fallback(tmp_path, monkeypatch):
    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)

    cluster = {
        "cluster_id": "concept_001",
        "canonical_name": "Archive and Space",
        "slug": "archive-and-space",
        "aliases": ["archive space"],
        "source_concepts": [
            {
                "material_id": "mat_001",
                "concept_name": "archive and space",
                "concept_type": "bridge",
                "relevance": "high",
                "source_pages": [1],
                "evidence_spans": ["archive and space"],
                "confidence": 1.0,
            }
        ],
    }
    material_info = {
        "mat_001": {
            "title": "Material 1",
            "summary": "Summary 1",
            "keywords": ["archive"],
        }
    }

    class Tool:
        def search_material_evidence(self, kind: str, material_id: str, query: str, limit: int = 5) -> list[dict]:
            if kind == "chunk" and query == "Archive and Space":
                return [
                    {
                        "chunk_id": "chk_search_001",
                        "summary": "Search summary",
                        "source_pages": [1],
                        "emphasized": False,
                        "content_class": "argument",
                        "text": "Search chunk text for Archive and Space.",
                        "snippet": "Search chunk text for Archive and Space.",
                        "rank": 1,
                    }
                ]
            return []

        def _material_evidence(self, material_id: str, query_terms: list[str] | None = None, chunk_limit: int = 2, annotation_limit: int = 2, figure_limit: int = 2, concept_limit: int = 4) -> dict:
            return {
                "chunks": [
                    {
                        "chunk_id": "chk_fallback_001",
                        "text": "Fallback chunk text for Archive and Space.",
                    },
                    {
                        "chunk_id": "chk_search_001",
                        "text": "Search chunk text for Archive and Space.",
                    },
                ],
                "annotations": [],
                "figures": [],
                "concepts": [],
            }

    payload = _build_concept_reflection_evidence_payload(cluster, material_info, Tool())

    chunks = payload["materials"][0]["evidence"]["chunks"]
    assert [chunk["chunk_id"] for chunk in chunks] == ["chk_search_001", "chk_fallback_001"]
    assert chunks[0]["text"] == "Search chunk text for Archive and Space."
    assert chunks[1]["text"] == "Fallback chunk text for Archive and Space."


def test_concept_reflection_evidence_compacts_annotations_and_concepts(tmp_path, monkeypatch):
    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)

    cluster = {
        "cluster_id": "concept_001",
        "canonical_name": "Archive and Space",
        "slug": "archive-and-space",
        "aliases": ["archive space"],
        "source_concepts": [
            {
                "material_id": "mat_001",
                "concept_name": "archive and space",
                "concept_type": "bridge",
                "relevance": "high",
                "source_pages": [1],
                "evidence_spans": ["archive and space"],
                "confidence": 1.0,
            }
        ],
    }
    material_info = {
        "mat_001": {
            "title": "Material 1",
            "summary": "Summary 1",
            "keywords": ["archive"],
        }
    }

    class Tool:
        def search_material_evidence(self, kind: str, material_id: str, query: str, limit: int = 5) -> list[dict]:
            if kind == "chunk" and query == "Archive and Space":
                return [
                    {
                        "chunk_id": "chk_search_001",
                        "summary": "Search summary",
                        "source_pages": [1],
                        "emphasized": False,
                        "content_class": "argument",
                        "text": "Search chunk text for Archive and Space.",
                        "snippet": "Search chunk text for Archive and Space.",
                        "rank": 1,
                    }
                ]
            return []

        def _material_evidence(self, material_id: str, query_terms: list[str] | None = None, chunk_limit: int = 2, annotation_limit: int = 2, figure_limit: int = 2, concept_limit: int = 4) -> dict:
            return {
                "chunks": [],
                "annotations": [
                    {
                        "page": 12,
                        "quoted_text": "An annotation quote.",
                        "comment": "Annotation comment.",
                    }
                ],
                "figures": [],
                "concepts": [
                    {
                        "concept_name": "archival power",
                        "evidence_spans": ["archival power"],
                    }
                ],
            }

    payload = _build_concept_reflection_evidence_payload(cluster, material_info, Tool())

    evidence = payload["materials"][0]["evidence"]
    assert evidence["annotations"] == "p. 12 — An annotation quote. — Annotation comment."
    assert evidence["concepts"] == ["archival power (archival power)"]
    assert "annotation_id" not in json.dumps(evidence)
    assert "evidence_spans" not in json.dumps(evidence)


def test_lint_cli_supports_json_and_exit_codes(tmp_path, monkeypatch):
    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)
    runner = CliRunner()

    result = runner.invoke(lint_cmd, ["--quick", "--json"], obj={})

    assert result.exit_code == 2
    assert result.output.strip().startswith("{")
    assert "\"deterministic\"" in result.output


def test_lint_cli_passes_stage_selection(tmp_path, monkeypatch):
    import arquimedes.config as config_mod
    import arquimedes.lint as lint_mod

    root, _config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)
    runner = CliRunner()
    captured: dict[str, object] = {}

    monkeypatch.setattr(config_mod, "load_config", lambda: {"llm": {"agent_cmd": "echo"}})

    def fake_run_lint(config, *, quick=False, full=False, report=False, fix=False, scheduled=False, llm_factory=None, stages=None):
        captured["stages"] = stages
        return {
            "mode": "staged",
            "deterministic": {"summary": {"issues": 0, "high": 0, "medium": 0, "low": 0}},
            "reflection": {"cluster_reviews": 0, "concept_reflections": 0, "collection_reflections": 0, "global_bridges": 0, "graph_maintenance": 0},
            "fixes": None,
            "report_path": str(root / "wiki" / "_lint_report.md"),
        }

    monkeypatch.setattr(lint_mod, "run_lint", fake_run_lint)

    result = runner.invoke(lint_cmd, ["--stage", "global-bridge", "--json"], obj={})

    assert result.exit_code == 0
    assert captured["stages"] == ["global-bridge"]


@pytest.mark.skip(reason="legacy raw-material bridge fixture retired")
def test_run_reflective_lint_only_runs_requested_stage(tmp_path, monkeypatch):
    import arquimedes.lint as lint_mod

    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)
    (root / "indexes" / "search.sqlite").write_text("", encoding="utf-8")
    calls: list[str] = []

    class DummyTool:
        def __init__(self, _root: Path):
            self.root = _root

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_cluster_audit(*_args, **_kwargs):
        calls.append("cluster-audit")
        return ([{"cluster_id": "bridge_0001"}], 1)

    def fake_concept_reflections(*_args, **_kwargs):
        calls.append("concept-reflection")
        return [{"cluster_id": "bridge_0001"}]

    def fake_collection_reflections(*_args, **_kwargs):
        calls.append("collection-reflection")
        return [{"collection_key": "research/papers"}]

    def fake_global_bridges(*_args, **_kwargs):
        calls.append("global-bridge")
        return {"global_bridges": 1, "global_bridge_skipped": False, "global_bridge_skip_reason": ""}

    def fake_graph_reflection(*_args, **_kwargs):
        calls.append("graph-maintenance")
        return {"graph_maintenance": 1, "graph_skipped": False, "graph_skip_reason": ""}

    monkeypatch.setattr(lint_mod, "ReflectionIndexTool", DummyTool)
    monkeypatch.setattr(lint_mod, "_cluster_audit_due", lambda *_args, **_kwargs: (True, "cluster audit stamp missing"))
    monkeypatch.setattr(lint_mod, "_run_cluster_audit", fake_cluster_audit)
    monkeypatch.setattr(lint_mod, "_run_concept_reflections", fake_concept_reflections)
    monkeypatch.setattr(lint_mod, "_run_collection_reflections", fake_collection_reflections)
    monkeypatch.setattr(lint_mod, "_run_global_bridges", fake_global_bridges)
    monkeypatch.setattr(lint_mod, "_run_graph_reflection", fake_graph_reflection)
    monkeypatch.setattr(lint_mod, "compile_wiki", lambda *args, **kwargs: None)
    monkeypatch.setattr(lint_mod, "memory_rebuild", lambda _config: None)
    monkeypatch.setattr(lint_mod, "load_bridge_clusters", lambda _root: [])

    result = lint_mod.run_reflective_lint(
        config,
        {"summary": {"issues": 0, "high": 0, "medium": 0, "low": 0}, "issues": []},
        stages=["cluster-audit"],
    )

    assert calls == ["cluster-audit"]
    assert result["cluster_reviews"] == 1
    assert result["concept_reflections"] == 0
    assert result["collection_reflections"] == 0
    assert result["global_bridges"] == 0
    assert result["graph_maintenance"] == 0
    assert result["stages"] == ["cluster-audit"]
    assert result["global_bridge_skip_reason"] == "stage not selected"
    assert result["graph_skip_reason"] == "stage not selected"


@pytest.mark.skip(reason="legacy raw-material bridge fixture retired")
def test_run_reflective_lint_default_full_stages_skip_graph_maintenance(tmp_path, monkeypatch):
    import arquimedes.lint as lint_mod

    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)
    (root / "indexes" / "search.sqlite").write_text("", encoding="utf-8")
    calls: list[str] = []

    class DummyTool:
        def __init__(self, _root: Path):
            self.root = _root

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_cluster_audit(*_args, **_kwargs):
        calls.append("cluster-audit")
        return ([{"cluster_id": "bridge_0001"}], 1)

    def fake_concept_reflections(*_args, **_kwargs):
        calls.append("concept-reflection")
        return [{"cluster_id": "bridge_0001"}]

    def fake_collection_reflections(*_args, **_kwargs):
        calls.append("collection-reflection")
        return [{"collection_key": "research/papers"}]

    def fake_global_bridges(*_args, **_kwargs):
        calls.append("global-bridge")
        return {"global_bridges": 1, "global_bridge_skipped": False, "global_bridge_skip_reason": ""}

    def fail_graph_reflection(*_args, **_kwargs):
        raise AssertionError("graph maintenance should not run in default full lint")

    monkeypatch.setattr(lint_mod, "ReflectionIndexTool", DummyTool)
    monkeypatch.setattr(lint_mod, "_cluster_audit_due", lambda *_args, **_kwargs: (True, "cluster audit due"))
    monkeypatch.setattr(lint_mod, "_concept_reflection_due", lambda *_args, **_kwargs: (True, "concept reflection due"))
    monkeypatch.setattr(lint_mod, "_collection_reflection_due", lambda *_args, **_kwargs: (True, "collection reflection due"))
    monkeypatch.setattr(lint_mod, "_global_bridge_due", lambda *_args, **_kwargs: (True, "global bridge due"))
    monkeypatch.setattr(lint_mod, "_run_cluster_audit", fake_cluster_audit)
    monkeypatch.setattr(lint_mod, "_run_concept_reflections", fake_concept_reflections)
    monkeypatch.setattr(lint_mod, "_run_collection_reflections", fake_collection_reflections)
    monkeypatch.setattr(lint_mod, "_run_global_bridges", fake_global_bridges)
    monkeypatch.setattr(lint_mod, "_run_graph_reflection", fail_graph_reflection)
    monkeypatch.setattr(lint_mod, "compile_wiki", lambda *args, **kwargs: None)
    monkeypatch.setattr(lint_mod, "memory_rebuild", lambda _config: None)
    monkeypatch.setattr(lint_mod, "load_bridge_clusters", lambda _root: [])
    monkeypatch.setattr(lint_mod, "load_local_clusters", lambda _root: [])

    result = lint_mod.run_reflective_lint(
        config,
        {"summary": {"issues": 0, "high": 0, "medium": 0, "low": 0}, "issues": []},
    )

    assert calls == ["cluster-audit", "concept-reflection", "collection-reflection", "global-bridge"]
    assert result["stages"] == [
        "cluster-audit",
        "concept-reflection",
        "collection-reflection",
        "global-bridge",
    ]
    assert result["graph_maintenance"] == 0
    assert result["graph_skip_reason"] == "stage not selected"


@pytest.mark.skip(reason="legacy raw-material bridge fixture retired")
def test_run_reflective_lint_reports_stage_progress_to_stderr(tmp_path, monkeypatch, capsys):
    import arquimedes.lint as lint_mod

    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)
    (root / "indexes" / "search.sqlite").write_text("", encoding="utf-8")
    calls: list[str] = []

    class DummyTool:
        def __init__(self, _root: Path):
            self.root = _root

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_cluster_audit(*_args, **_kwargs):
        calls.append("cluster-audit")
        return ([{"cluster_id": "bridge_0001"}], 1)

    def fail_concept_reflections(*_args, **_kwargs):
        raise AssertionError("concept reflection should not run")

    monkeypatch.setattr(lint_mod, "ReflectionIndexTool", DummyTool)
    monkeypatch.setattr(lint_mod, "_cluster_audit_due", lambda *_args, **_kwargs: (True, "cluster audit stamp missing"))
    monkeypatch.setattr(lint_mod, "_concept_reflection_due", lambda *_args, **_kwargs: (False, "concept reflections already current"))
    monkeypatch.setattr(lint_mod, "_run_cluster_audit", fake_cluster_audit)
    monkeypatch.setattr(lint_mod, "_run_concept_reflections", fail_concept_reflections)
    monkeypatch.setattr(lint_mod, "compile_wiki", lambda *args, **kwargs: None)
    monkeypatch.setattr(lint_mod, "memory_rebuild", lambda _config: None)
    monkeypatch.setattr(lint_mod, "load_bridge_clusters", lambda _root: [])

    lint_mod.run_reflective_lint(
        config,
        {"summary": {"issues": 0, "high": 0, "medium": 0, "low": 0}, "issues": []},
        stages=["cluster-audit", "concept-reflection"],
    )

    captured = capsys.readouterr()
    assert calls == ["cluster-audit"]
    assert "cluster audit started" in captured.err
    assert "cluster audit finished" in captured.err
    assert "concept reflection skipped: concept reflections already current" in captured.err


@pytest.mark.skip(reason="legacy raw-material bridge fixture retired")
def test_run_reflective_lint_does_not_rerun_concept_reflections_after_global_bridge(tmp_path, monkeypatch):
    import arquimedes.lint as lint_mod

    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)
    (root / "indexes" / "search.sqlite").write_text("", encoding="utf-8")

    class DummyTool:
        def __init__(self, _root: Path):
            self.root = _root

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    calls: list[list[str]] = []
    local_cluster = {
        "cluster_id": "research__papers__local_0001",
        "canonical_name": "Archive and Space",
        "source_concepts": [{"material_id": "mat_001"}, {"material_id": "mat_002"}],
    }

    def fake_concept_reflections(_root, clusters, *_args, **_kwargs):
        calls.append(sorted(str(cluster.get("cluster_id", "")).strip() for cluster in clusters))
        return [{"cluster_id": cluster_id} for cluster_id in calls[-1] if cluster_id]

    def fake_global_bridges(root, *_args, **_kwargs):
        _write_jsonl(
            root / "derived" / "global_bridge_clusters.jsonl",
            [
                {
                    "bridge_id": "global_bridge__archive-and-space",
                    "cluster_id": "global_bridge__archive-and-space",
                    "canonical_name": "Archive and Space",
                    "slug": "archive-and-space",
                    "supporting_material_ids": ["mat_001", "mat_002"],
                    "member_local_clusters": [
                        {
                            "cluster_id": "research__papers__local_0001",
                            "material_ids": ["mat_001", "mat_002"],
                        }
                    ],
                    "wiki_path": "wiki/shared/bridge-concepts/archive-and-space.md",
                }
            ],
        )
        return {"global_bridges": 1, "global_bridge_skipped": False, "global_bridge_skip_reason": ""}

    monkeypatch.setattr(lint_mod, "ReflectionIndexTool", DummyTool)
    monkeypatch.setattr(lint_mod, "compile_wiki", lambda *args, **kwargs: None)
    monkeypatch.setattr(lint_mod, "memory_rebuild", lambda _config: None)
    monkeypatch.setattr(lint_mod, "_concept_reflection_due", lambda *_args, **_kwargs: (True, "concept reflection due"))
    monkeypatch.setattr(lint_mod, "_global_bridge_due", lambda *_args, **_kwargs: (True, "global bridge due"))
    monkeypatch.setattr(lint_mod, "_run_concept_reflections", fake_concept_reflections)
    monkeypatch.setattr(lint_mod, "_run_global_bridges", fake_global_bridges)
    monkeypatch.setattr(lint_mod, "_current_concepts", lambda _root: [local_cluster])
    monkeypatch.setattr(lint_mod, "load_local_clusters", lambda _root: [local_cluster])
    monkeypatch.setattr(lint_mod, "load_bridge_clusters", lambda _root: [])

    result = lint_mod.run_reflective_lint(
        config,
        {"summary": {"issues": 0, "high": 0, "medium": 0, "low": 0}, "issues": []},
        stages=["concept-reflection", "global-bridge"],
    )

    assert result["concept_reflections"] == 1
    assert calls == [["research__papers__local_0001"]]


def test_run_reflective_lint_global_bridge_stage_writes_artifact(tmp_path, monkeypatch):
    import arquimedes.lint as lint_mod
    from arquimedes.lint_global_bridge import _global_bridge_due

    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)
    (root / "indexes" / "search.sqlite").write_text("", encoding="utf-8")

    class DummyTool:
        def __init__(self, _root: Path):
            self.root = _root

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(lint_mod, "ReflectionIndexTool", DummyTool)
    monkeypatch.setattr(lint_mod, "compile_wiki", lambda *args, **kwargs: None)
    monkeypatch.setattr(lint_mod, "memory_rebuild", lambda _config: None)

    _write_jsonl(
        root / "derived" / "collections" / "research__papers" / "local_concept_clusters.jsonl",
        [
            {
                "cluster_id": "research__papers__local_0001",
                "domain": "research",
                "collection": "papers",
                "canonical_name": "Archive and Space",
                "slug": "archive-and-space",
                "aliases": ["Spatial Archive"],
                "descriptor": "Archive as a spatial ordering device.",
                "material_ids": ["mat_001", "mat_002"],
                "source_concepts": [
                    {"material_id": "mat_001", "concept_name": "archive and space"},
                    {"material_id": "mat_002", "concept_name": "archive and space"},
                ],
                "confidence": 0.92,
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
                "canonical_name": "Archive and Space",
                "slug": "archive-and-space",
                "aliases": ["Archive Spatial Memory"],
                "descriptor": "Archive as a project-scale spatial memory scaffold.",
                "material_ids": ["mat_010"],
                "source_concepts": [{"material_id": "mat_010", "concept_name": "archive and space"}],
                "confidence": 0.61,
            },
        ],
    )
    _write_jsonl(
        root / "derived" / "lint" / "collection_reflections.jsonl",
        [
            {
                "collection_key": "research/papers",
                "domain": "research",
                "collection": "papers",
                "important_material_ids": [],
                "important_cluster_ids": ["research__papers__local_0001"],
                "main_takeaways": ["Archive theory anchors the collection."],
                "main_tensions": ["Theory risks becoming detached from design use."],
                "open_questions": ["Which design archives should be compared next?"],
                "why_this_collection_matters": "Papers define the theoretical archive frame.",
            },
                {
                    "collection_key": "research/projects",
                    "domain": "research",
                    "collection": "projects",
                    "important_material_ids": [],
                    "important_cluster_ids": ["research__projects__local_0001"],
                    "main_takeaways": ["Archive thinking recurs across projects."],
                    "main_tensions": [],
                    "open_questions": [],
                    "why_this_collection_matters": "Projects keep returning to archive as a research-scale spatial system.",
                }
            ],
        )

    _write_jsonl(
        root / "derived" / "lint" / "concept_reflections.jsonl",
        [
            {
                "cluster_id": "research__papers__local_0001",
                "main_takeaways": ["Archive is an ordering device."],
                "main_tensions": ["Theory vs practice."],
                "open_questions": ["How portable is the archive frame?"],
                "helpful_new_sources": ["comparative project archive studies"],
                "why_this_concept_matters": "It anchors the research side.",
            },
            {
                "cluster_id": "research__projects__local_0001",
                "main_takeaways": ["Archive acts as a project memory."],
                "main_tensions": ["Memory vs execution."],
                "open_questions": ["How should archive-driven projects be documented?"],
                "helpful_new_sources": ["project archive case studies"],
                "why_this_concept_matters": "It grounds archive thinking in a second research collection.",
            },
        ],
    )

    def llm_factory(_stage: str):
        def fn(system: str, messages: list[dict]) -> str:
            assert "global bridge packet" in system.lower() or "global bridge packet" in messages[0]["content"].lower()
            return json.dumps(
                {
                    "links_to_existing": [],
                    "new_clusters": [
                        {
                            "canonical_name": "Archive and Space",
                            "descriptor": "A domain bridge connecting archive theory and project-scale memory work.",
                            "aliases": ["Archive as Spatial Framework"],
                            "member_local_clusters": [
                                {"cluster_id": "research__papers__local_0001"},
                                {"cluster_id": "research__projects__local_0001"},
                            ],
                            "bridge_takeaways": ["Archive thinking recurs across research collections."],
                            "bridge_tensions": ["Theory and project method frame archival space differently."],
                            "bridge_open_questions": ["Which other collections should join this bridge?"],
                            "helpful_new_sources": ["comparative archive project case studies"],
                            "why_this_bridge_matters": "It turns archive into a shared research-domain perspective.",
                        }
                    ],
                    "_finished": True,
                }
            )

        return fn

    result = lint_mod.run_reflective_lint(
        config,
        {"summary": {"issues": 0, "high": 0, "medium": 0, "low": 0}, "issues": []},
        stages=["global-bridge"],
        llm_factory=llm_factory,
    )

    bridge_rows = [
        json.loads(line)
        for line in (root / "derived" / "domains" / "research" / "global_bridge_clusters.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]

    assert result["global_bridges"] == 1
    assert result["global_bridge_skipped"] is False
    assert len(bridge_rows) == 1
    assert bridge_rows[0]["canonical_name"] == "Archive and Space"
    assert bridge_rows[0]["domain"] == "research"
    assert bridge_rows[0]["domain_collection_keys"] == ["research/papers", "research/projects"]
    assert bridge_rows[0]["bridge_takeaways"] == ["Archive thinking recurs across research collections."]
    assert bridge_rows[0]["bridge_tensions"] == ["Theory and project method frame archival space differently."]
    assert bridge_rows[0]["bridge_open_questions"] == ["Which other collections should join this bridge?"]
    assert bridge_rows[0]["helpful_new_sources"] == ["comparative archive project case studies"]
    assert bridge_rows[0]["why_this_bridge_matters"] == "It turns archive into a shared research-domain perspective."
    assert {row["collection_key"] for row in bridge_rows[0]["supporting_collection_reflections"]} == {
        "research/papers",
        "research/projects",
    }
    assert {member["cluster_id"] for member in bridge_rows[0]["member_local_clusters"]} == {
        "research__papers__local_0001",
        "research__projects__local_0001",
    }
    assert bridge_rows[0]["bridge_id"] == "global_bridge__research__archive-and-space"
    assert (root / "derived" / "domains" / "research" / "global_bridge_stamp.json").exists()

    due, reason = _global_bridge_due(
        root,
        lint_mod.load_local_clusters(root),
        lint_mod._load_jsonl(root / "derived" / "lint" / "collection_reflections.jsonl"),
    )
    assert due is False
    assert reason == "research: global bridge unchanged"


def test_global_bridge_memory_snapshot_includes_connected_cluster_reflections():
    from arquimedes.lint_global_bridge import _bridge_memory_snapshot

    rows = _bridge_memory_snapshot(
        [
            {
                "bridge_id": "global_bridge__archive-and-space",
                "canonical_name": "Archive and Space",
                "descriptor": "Shared archive perspective.",
                "aliases": ["Archive as Spatial Framework"],
                "member_local_clusters": [
                    {
                        "cluster_id": "research__papers__local_0001",
                        "collection_key": "research/papers",
                        "canonical_name": "Archive and Space",
                    }
                ],
                "bridge_takeaways": ["Archive thinking recurs across collections."],
                "bridge_tensions": ["Theory and practice frame archival space differently."],
                "bridge_open_questions": ["Which other collections should join this bridge?"],
                "helpful_new_sources": ["comparative archive design case studies"],
                "why_this_bridge_matters": "It turns archive into a shared cross-system perspective.",
            }
        ],
        {
            "research__papers__local_0001": {
                "cluster_id": "research__papers__local_0001",
                "collection_key": "research/papers",
                "canonical_name": "Archive and Space",
                "descriptor": "Archive as a spatial ordering device.",
                "reflection": {
                    "main_takeaways": ["Archive is an ordering device.", "Archive crosses media."],
                    "main_tensions": ["Theory vs practice."],
                    "open_questions": ["How portable is the archive frame?"],
                    "helpful_new_sources": ["comparative project archive studies"],
                    "why_this_concept_matters": "It anchors the research side.",
                },
            }
        },
        {
            "research/papers": {
                "collection_key": "research/papers",
                "title": "papers",
                "main_takeaways": ["Archive theory anchors the collection."],
                "main_tensions": ["Theory risks becoming detached from design use."],
                "why_this_collection_matters": "Papers define the theoretical archive frame.",
            }
        },
    )

    assert len(rows) == 1
    assert rows[0]["bridge_takeaways"] == ["Archive thinking recurs across collections."]
    assert rows[0]["bridge_open_questions"] == ["Which other collections should join this bridge?"]
    assert rows[0]["why_this_bridge_matters"] == "It turns archive into a shared cross-system perspective."
    assert rows[0]["member_local_clusters"][0]["descriptor"] == "Archive as a spatial ordering device."
    assert rows[0]["member_local_clusters"][0]["reflection"]["main_takeaways"] == [
        "Archive is an ordering device.",
        "Archive crosses media.",
    ]
    assert rows[0]["member_local_clusters"][0]["reflection"]["why_this_concept_matters"] == "It anchors the research side."
    assert rows[0]["supporting_collection_reflections"][0]["collection_key"] == "research/papers"
    assert "bridge_tensions" not in rows[0]
    assert "helpful_new_sources" not in rows[0]


def test_global_bridge_prompt_requests_page_worthy_bridge_essay():
    from arquimedes.lint_global_bridge import _global_bridge_prompt

    system, user = _global_bridge_prompt(Path("packet.json"), Path("memory.json"), "research")

    assert "grounded mini-essay" in system
    assert "2 to 4 paragraphs" in system
    assert "full connected local-cluster reflections and collection signals" in system
    assert "inside the Research domain" in system
    assert "page-worthy bridge synthesis" in user


def test_global_bridge_prompt_for_practice_is_spanish():
    from arquimedes.lint import collection_reflection_figure_limit, concept_reflection_figure_limit
    from arquimedes.lint_global_bridge import _global_bridge_prompt

    system, user = _global_bridge_prompt(Path("packet.json"), Path("memory.json"), "practice")

    assert "orientada a la práctica" in system
    assert "Todos los textos libres y listas deben estar en español." in user
    assert concept_reflection_figure_limit("practice") > concept_reflection_figure_limit("research")
    assert collection_reflection_figure_limit("practice") > collection_reflection_figure_limit("research")


def test_run_reflective_lint_global_bridge_stage_skips_with_fewer_than_two_collections(tmp_path, monkeypatch):
    import arquimedes.lint as lint_mod

    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)
    (root / "indexes" / "search.sqlite").write_text("", encoding="utf-8")

    class DummyTool:
        def __init__(self, _root: Path):
            self.root = _root

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(lint_mod, "ReflectionIndexTool", DummyTool)
    monkeypatch.setattr(lint_mod, "compile_wiki", lambda *args, **kwargs: None)
    monkeypatch.setattr(lint_mod, "memory_rebuild", lambda _config: None)

    _write_jsonl(
        root / "derived" / "collections" / "research__papers" / "local_concept_clusters.jsonl",
        [
            {
                "cluster_id": "research__papers__local_0001",
                "domain": "research",
                "collection": "papers",
                "canonical_name": "Archive and Space",
                "slug": "archive-and-space",
                "aliases": [],
                "descriptor": "Archive as a spatial ordering device.",
                "material_ids": ["mat_001", "mat_002"],
                "source_concepts": [
                    {"material_id": "mat_001", "concept_name": "archive and space"},
                    {"material_id": "mat_002", "concept_name": "archive and space"},
                ],
                "confidence": 0.92,
            }
        ],
    )
    _write_jsonl(root / "derived" / "lint" / "collection_reflections.jsonl", [])

    result = lint_mod.run_reflective_lint(
        config,
        {"summary": {"issues": 0, "high": 0, "medium": 0, "low": 0}, "issues": []},
        stages=["global-bridge"],
    )

    assert result["global_bridges"] == 0
    assert result["global_bridge_skipped"] is True
    assert result["global_bridge_skip_reason"] == "research: fewer than 2 collections"
    assert not (root / "derived" / "domains" / "research" / "global_bridge_clusters.jsonl").exists()


def test_global_bridge_due_tracks_only_new_local_clusters(tmp_path, monkeypatch):
    from arquimedes.lint_global_bridge import _global_bridge_due, _global_bridge_inputs

    root, _config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)

    local_clusters = [
        {
            "cluster_id": "research__papers__local_0001",
            "domain": "research",
            "collection": "papers",
            "canonical_name": "Archive and Space",
            "slug": "archive-and-space",
            "aliases": ["Spatial Archive"],
            "descriptor": "Archive as a spatial ordering device.",
            "material_ids": ["mat_001", "mat_002"],
            "source_concepts": [
                {"material_id": "mat_001", "concept_name": "archive and space"},
                {"material_id": "mat_002", "concept_name": "archive and space"},
            ],
            "confidence": 0.92,
        },
        {
            "cluster_id": "research__projects__local_0001",
            "domain": "research",
            "collection": "projects",
            "canonical_name": "Archive and Space",
            "slug": "archive-and-space",
            "aliases": ["Archive Spatial Memory"],
            "descriptor": "Archive as a project-scale spatial memory scaffold.",
            "material_ids": ["mat_010"],
            "source_concepts": [{"material_id": "mat_010", "concept_name": "archive and space"}],
            "confidence": 0.61,
        },
    ]
    collection_refs = [
        {
            "collection_key": "research/papers",
            "domain": "research",
            "collection": "papers",
            "main_takeaways": ["Archive theory anchors the collection."],
            "main_tensions": ["Theory risks becoming detached from design use."],
            "open_questions": ["Which design archives should be compared next?"],
            "why_this_collection_matters": "Papers define the theoretical archive frame.",
        },
        {
            "collection_key": "research/projects",
            "domain": "research",
            "collection": "projects",
            "main_takeaways": ["Archive thinking recurs across projects."],
            "main_tensions": [],
            "open_questions": [],
            "why_this_collection_matters": "Projects keep returning to archive as a research-scale spatial system.",
        },
    ]

    _write_jsonl(
        root / "derived" / "lint" / "concept_reflections.jsonl",
        [
            {
                "cluster_id": "research__papers__local_0001",
                "main_takeaways": ["Archive is an ordering device."],
                "main_tensions": ["Theory vs practice."],
                "open_questions": ["How portable is the archive frame?"],
                "helpful_new_sources": ["comparative project archive studies"],
                "why_this_concept_matters": "It anchors the research side.",
            },
            {
                "cluster_id": "research__projects__local_0001",
                "main_takeaways": ["Archive acts as a project memory."],
                "main_tensions": ["Memory vs execution."],
                "open_questions": ["How should archive-driven projects be documented?"],
                "helpful_new_sources": ["project archive case studies"],
                "why_this_concept_matters": "It grounds archive thinking in a second research collection.",
            },
        ],
    )

    _write_jsonl(
        root / "derived" / "domains" / "research" / "global_bridge_clusters.jsonl",
        [
            {
                "bridge_id": "global_bridge__research__archive-and-space",
                "domain": "research",
                "canonical_name": "Archive and Space",
                "slug": "archive-and-space",
                "descriptor": "A domain bridge connecting archive theory and project work.",
                "aliases": ["Archive as Spatial Framework"],
                "member_local_clusters": [
                    {
                        "cluster_id": "research__papers__local_0001",
                        "domain": "research",
                        "collection": "papers",
                        "collection_key": "research/papers",
                        "canonical_name": "Archive and Space",
                    },
                    {
                        "cluster_id": "research__projects__local_0001",
                        "domain": "research",
                        "collection": "projects",
                        "collection_key": "research/projects",
                        "canonical_name": "Archive and Space",
                    },
                ],
                "bridge_takeaways": ["Archive thinking recurs across research collections."],
                "bridge_open_questions": ["Which other collections should join this bridge?"],
                "why_this_bridge_matters": "It turns archive into a shared research-domain perspective.",
                "supporting_material_ids": ["mat_001", "mat_002", "mat_010"],
            }
        ],
    )
    initial_bundle = _global_bridge_inputs(root, local_clusters, collection_refs)
    _write_json(
        root / "derived" / "domains" / "research" / "global_bridge_stamp.json",
        {
            "input_fingerprint": "bootstrap",
            "local_cluster_fingerprints": initial_bundle["local_cluster_fingerprints"],
            "collection_context_fingerprints": initial_bundle["collection_context_fingerprints"],
        },
    )
    bundle = _global_bridge_inputs(root, local_clusters, collection_refs)
    _write_json(
        root / "derived" / "domains" / "research" / "global_bridge_stamp.json",
        {
            "input_fingerprint": bundle["input_fingerprint"],
            "local_cluster_fingerprints": bundle["local_cluster_fingerprints"],
            "collection_context_fingerprints": bundle["collection_context_fingerprints"],
        },
    )

    due, reason = _global_bridge_due(root, local_clusters, collection_refs)
    assert due is False
    assert reason == "research: global bridge unchanged"

    changed_local_clusters = [dict(cluster) for cluster in local_clusters]
    changed_local_clusters[0]["descriptor"] = "Archive as a changed spatial ordering device."
    due, reason = _global_bridge_due(root, changed_local_clusters, collection_refs)
    assert due is True
    assert reason == "research: new local clusters pending"

    changed_collection_refs = [dict(row) for row in collection_refs]
    changed_collection_refs[0]["why_this_collection_matters"] = "Papers now frame archive as both theory and method."
    due, reason = _global_bridge_due(root, local_clusters, changed_collection_refs)
    assert due is False
    assert reason == "research: global bridge unchanged"


def test_global_bridge_input_fingerprint_ignores_collection_reflection_changes(tmp_path, monkeypatch):
    from arquimedes.lint_global_bridge import _global_bridge_due, _global_bridge_inputs

    root, _config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)

    local_clusters = [
        {
            "cluster_id": "research__papers__local_0001",
            "domain": "research",
            "collection": "papers",
            "canonical_name": "Archive and Space",
            "slug": "archive-and-space",
            "aliases": ["Spatial Archive"],
            "descriptor": "Archive as a spatial ordering device.",
            "material_ids": ["mat_001", "mat_002"],
            "source_concepts": [
                {"material_id": "mat_001", "concept_name": "archive and space"},
                {"material_id": "mat_002", "concept_name": "archive and space"},
            ],
            "confidence": 0.92,
        },
        {
            "cluster_id": "research__projects__local_0001",
            "domain": "research",
            "collection": "projects",
            "canonical_name": "Archive and Space",
            "slug": "archive-and-space",
            "aliases": ["Archive Spatial Memory"],
            "descriptor": "Archive as a project-scale spatial memory scaffold.",
            "material_ids": ["mat_010"],
            "source_concepts": [{"material_id": "mat_010", "concept_name": "archive and space"}],
            "confidence": 0.61,
        },
    ]
    collection_refs = [
        {
            "collection_key": "research/papers",
            "domain": "research",
            "collection": "papers",
            "main_takeaways": ["Archive theory anchors the collection."],
            "main_tensions": ["Theory risks becoming detached from design use."],
            "open_questions": ["Which design archives should be compared next?"],
            "why_this_collection_matters": "Papers define the theoretical archive frame.",
        },
        {
            "collection_key": "research/projects",
            "domain": "research",
            "collection": "projects",
            "main_takeaways": ["Archive thinking recurs across projects."],
            "main_tensions": [],
            "open_questions": [],
            "why_this_collection_matters": "Projects keep returning to archive as a research-scale spatial system.",
        },
    ]

    _write_jsonl(
        root / "derived" / "lint" / "concept_reflections.jsonl",
        [
            {
                "cluster_id": "research__papers__local_0001",
                "main_takeaways": ["Archive is an ordering device."],
                "main_tensions": ["Theory vs practice."],
                "open_questions": ["How portable is the archive frame?"],
                "helpful_new_sources": ["comparative project archive studies"],
                "why_this_concept_matters": "It anchors the research side.",
            },
            {
                "cluster_id": "research__projects__local_0001",
                "main_takeaways": ["Archive acts as a project memory."],
                "main_tensions": ["Memory vs execution."],
                "open_questions": ["How should archive-driven projects be documented?"],
                "helpful_new_sources": ["project archive case studies"],
                "why_this_concept_matters": "It grounds archive thinking in a second research collection.",
            },
        ],
    )

    bundle = _global_bridge_inputs(root, local_clusters, collection_refs)
    assert bundle["packet"]["pending_local_clusters"]

    _write_jsonl(
        root / "derived" / "domains" / "research" / "global_bridge_clusters.jsonl",
        [
            {
                "bridge_id": "global_bridge__research__archive-and-space",
                "domain": "research",
                "canonical_name": "Archive and Space",
                "slug": "archive-and-space",
                "descriptor": "A domain bridge connecting archive theory and project work.",
                "aliases": ["Archive as Spatial Framework"],
                "member_local_clusters": [
                    {
                        "cluster_id": "research__papers__local_0001",
                        "domain": "research",
                        "collection": "papers",
                        "collection_key": "research/papers",
                        "canonical_name": "Archive and Space",
                    },
                    {
                        "cluster_id": "research__projects__local_0001",
                        "domain": "research",
                        "collection": "projects",
                        "collection_key": "research/projects",
                        "canonical_name": "Archive and Space",
                    },
                ],
                "bridge_takeaways": ["Archive thinking recurs across research collections."],
                "bridge_open_questions": ["Which other collections should join this bridge?"],
                "why_this_bridge_matters": "It turns archive into a shared research-domain perspective.",
                "supporting_material_ids": ["mat_001", "mat_002", "mat_010"],
            }
        ],
    )
    _write_json(
        root / "derived" / "domains" / "research" / "global_bridge_stamp.json",
        {
            "input_fingerprint": "stale-fingerprint",
            "local_cluster_fingerprints": bundle["local_cluster_fingerprints"],
            "collection_context_fingerprints": bundle["collection_context_fingerprints"],
        },
    )

    refreshed_bundle = _global_bridge_inputs(root, local_clusters, collection_refs)
    assert refreshed_bundle["packet"]["pending_local_clusters"] == []

    changed_collection_refs = [dict(row) for row in collection_refs]
    changed_collection_refs[0]["why_this_collection_matters"] = "Papers now frame archive as both theory and method."
    changed_bundle = _global_bridge_inputs(root, local_clusters, changed_collection_refs)

    assert changed_bundle["packet"]["pending_local_clusters"] == []
    assert changed_bundle["input_fingerprint"] == refreshed_bundle["input_fingerprint"]

    due, reason = _global_bridge_due(root, local_clusters, changed_collection_refs)
    assert due is False
    assert reason == "research: global bridge unchanged"


def test_global_bridge_runner_skips_with_empty_pending_packet_even_with_stale_fingerprint(tmp_path, monkeypatch):
    import arquimedes.lint as lint_mod
    from arquimedes.lint_global_bridge import _global_bridge_inputs, _run_global_bridge_impl

    root, _config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)

    local_clusters = [
        {
            "cluster_id": "research__papers__local_0001",
            "domain": "research",
            "collection": "papers",
            "canonical_name": "Archive and Space",
            "slug": "archive-and-space",
            "aliases": ["Spatial Archive"],
            "descriptor": "Archive as a spatial ordering device.",
            "material_ids": ["mat_001", "mat_002"],
            "source_concepts": [
                {"material_id": "mat_001", "concept_name": "archive and space"},
                {"material_id": "mat_002", "concept_name": "archive and space"},
            ],
            "confidence": 0.92,
        },
        {
            "cluster_id": "research__projects__local_0001",
            "domain": "research",
            "collection": "projects",
            "canonical_name": "Archive and Space",
            "slug": "archive-and-space",
            "aliases": ["Archive Spatial Memory"],
            "descriptor": "Archive as a project-scale spatial memory scaffold.",
            "material_ids": ["mat_010"],
            "source_concepts": [{"material_id": "mat_010", "concept_name": "archive and space"}],
            "confidence": 0.61,
        },
    ]
    collection_refs = [
        {
            "collection_key": "research/papers",
            "domain": "research",
            "collection": "papers",
            "main_takeaways": ["Archive theory anchors the collection."],
            "main_tensions": ["Theory risks becoming detached from design use."],
            "open_questions": ["Which design archives should be compared next?"],
            "why_this_collection_matters": "Papers define the theoretical archive frame.",
        },
        {
            "collection_key": "research/projects",
            "domain": "research",
            "collection": "projects",
            "main_takeaways": ["Archive thinking recurs across projects."],
            "main_tensions": [],
            "open_questions": [],
            "why_this_collection_matters": "Projects keep returning to archive as a research-scale spatial system.",
        },
    ]

    _write_jsonl(
        root / "derived" / "lint" / "concept_reflections.jsonl",
        [
            {
                "cluster_id": "research__papers__local_0001",
                "main_takeaways": ["Archive is an ordering device."],
                "main_tensions": ["Theory vs practice."],
                "open_questions": ["How portable is the archive frame?"],
                "helpful_new_sources": ["comparative project archive studies"],
                "why_this_concept_matters": "It anchors the research side.",
            },
            {
                "cluster_id": "research__projects__local_0001",
                "main_takeaways": ["Archive acts as a project memory."],
                "main_tensions": ["Memory vs execution."],
                "open_questions": ["How should archive-driven projects be documented?"],
                "helpful_new_sources": ["project archive case studies"],
                "why_this_concept_matters": "It grounds archive thinking in a second research collection.",
            },
        ],
    )

    bundle = _global_bridge_inputs(root, local_clusters, collection_refs)

    _write_jsonl(
        root / "derived" / "domains" / "research" / "global_bridge_clusters.jsonl",
        [
            {
                "bridge_id": "global_bridge__research__archive-and-space",
                "domain": "research",
                "canonical_name": "Archive and Space",
                "slug": "archive-and-space",
                "descriptor": "A domain bridge connecting archive theory and project work.",
                "aliases": ["Archive as Spatial Framework"],
                "member_local_clusters": [
                    {
                        "cluster_id": "research__papers__local_0001",
                        "domain": "research",
                        "collection": "papers",
                        "collection_key": "research/papers",
                        "canonical_name": "Archive and Space",
                    },
                    {
                        "cluster_id": "research__projects__local_0001",
                        "domain": "research",
                        "collection": "projects",
                        "collection_key": "research/projects",
                        "canonical_name": "Archive and Space",
                    },
                ],
                "bridge_takeaways": ["Archive thinking recurs across research collections."],
                "bridge_tensions": ["Theory and project method frame archival space differently."],
                "bridge_open_questions": ["Which other collections should join this bridge?"],
                "helpful_new_sources": ["comparative archive project case studies"],
                "why_this_bridge_matters": "It turns archive into a shared research-domain perspective.",
                "supporting_material_ids": ["mat_001", "mat_002", "mat_010"],
            }
        ],
    )
    _write_json(
        root / "derived" / "domains" / "research" / "global_bridge_stamp.json",
        {
            "input_fingerprint": "stale-fingerprint",
            "local_cluster_fingerprints": bundle["local_cluster_fingerprints"],
            "collection_context_fingerprints": bundle["collection_context_fingerprints"],
        },
    )

    def llm_factory(_stage: str):
        def fn(_system: str, _messages: list[dict]) -> str:
            raise AssertionError("LLM should not run when there are no new local clusters")

        return fn

    result = _run_global_bridge_impl(
        lint_mod,
        root,
        local_clusters,
        collection_refs,
        llm_factory,
        None,
        "",
    )

    bridge_rows = [
        json.loads(line)
        for line in (root / "derived" / "domains" / "research" / "global_bridge_clusters.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]

    assert result["global_bridge_skipped"] is True
    assert result["global_bridges"] == 1
    assert result["global_bridge_skip_reason"] == "research: global bridge unchanged"
    assert len(bridge_rows) == 1
    assert bridge_rows[0]["bridge_id"] == "global_bridge__research__archive-and-space"


def test_scheduled_full_lint_can_skip_when_fresh(tmp_path, monkeypatch):
    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)
    clusters = _write_cluster_data(root)
    _write_json(
        root / "extracted" / "mat_002" / "meta.json",
        {
            "material_id": "mat_002",
            "file_hash": "hash-002",
            "source_path": "Research/Two.pdf",
            "title": "Two",
            "authors": ["Author Two"],
            "year": "2026",
            "page_count": 1,
            "file_type": "pdf",
            "domain": "research",
            "collection": "papers",
            "raw_keywords": ["archive"],
            "raw_document_type": "paper",
            "summary": {"value": "Summary", "provenance": {}},
            "keywords": {"value": ["archive"], "provenance": {}},
            "document_type": {"value": "paper", "provenance": {}},
            "facets": {},
            "_enrichment_stamp": {"prompt_version": "enrich-v1.0", "enrichment_schema_version": "1"},
        },
    )
    (root / "extracted" / "mat_002" / "chunks.jsonl").write_text("", encoding="utf-8")
    (root / "extracted" / "mat_002" / "annotations.jsonl").write_text("", encoding="utf-8")
    _write_jsonl(
        root / "manifests" / "materials.jsonl",
        [
            {
                "material_id": "mat_001",
                "file_hash": "hash-001",
                "relative_path": "Research/One.pdf",
                "file_type": "pdf",
                "domain": "research",
                "collection": "papers",
                "ingested_at": "2026-01-01T00:00:00+00:00",
            },
            {
                "material_id": "mat_002",
                "file_hash": "hash-002",
                "relative_path": "Research/Two.pdf",
                "file_type": "pdf",
                "domain": "research",
                "collection": "papers",
                "ingested_at": "2026-01-02T00:00:00+00:00",
            },
        ],
    )
    rebuild_index(config)

@pytest.mark.skip(reason="legacy raw-material bridge audit retired")
def test_staged_lint_skips_when_cluster_audit_is_already_after_latest_clustering(tmp_path, monkeypatch):
    import arquimedes.lint as lint_mod

    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)
    (root / "indexes" / "search.sqlite").write_text("", encoding="utf-8")

    class DummyTool:
        def __init__(self, _root: Path):
            self.root = _root

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_run_cluster_audit(*_args, **_kwargs):
        raise AssertionError("cluster audit should have been skipped")

    monkeypatch.setattr(lint_mod, "ReflectionIndexTool", DummyTool)
    monkeypatch.setattr(lint_mod, "_cluster_audit_due", lambda *_args, **_kwargs: (False, "cluster audit unchanged"))
    monkeypatch.setattr(lint_mod, "_run_cluster_audit", fake_run_cluster_audit)
    monkeypatch.setattr(lint_mod, "compile_wiki", lambda *args, **kwargs: None)
    monkeypatch.setattr(lint_mod, "memory_rebuild", lambda _config: None)
    monkeypatch.setattr(lint_mod, "load_bridge_clusters", lambda _root: [])
    monkeypatch.setattr(
        lint_mod,
        "run_deterministic_lint",
        lambda _config: {"summary": {"issues": 0, "high": 0, "medium": 0, "low": 0}, "issues": []},
    )

    result = lint_mod.run_lint(config, stages=["cluster-audit"])

    assert result["reflection"]["skipped"] is True
    assert result["reflection"]["skip_reason"] == "cluster audit unchanged"
    assert result["reflection"]["graph_skip_reason"] == "stage not selected"


@pytest.mark.skip(reason="legacy raw-material bridge audit retired")
def test_staged_lint_runs_when_cluster_audit_is_older_than_latest_clustering(tmp_path, monkeypatch):
    import arquimedes.lint as lint_mod

    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)
    (root / "indexes" / "search.sqlite").write_text("", encoding="utf-8")

    class DummyTool:
        def __init__(self, _root: Path):
            self.root = _root

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    called = []

    def fake_run_cluster_audit(*_args, **_kwargs):
        called.append(True)
        return ([{"cluster_id": "bridge_0001"}], 1)

    monkeypatch.setattr(lint_mod, "ReflectionIndexTool", DummyTool)
    monkeypatch.setattr(lint_mod, "_cluster_audit_due", lambda *_args, **_kwargs: (True, "cluster audit stamp missing"))
    monkeypatch.setattr(lint_mod, "_run_cluster_audit", fake_run_cluster_audit)
    monkeypatch.setattr(lint_mod, "compile_wiki", lambda *args, **kwargs: None)
    monkeypatch.setattr(lint_mod, "memory_rebuild", lambda _config: None)
    monkeypatch.setattr(lint_mod, "load_bridge_clusters", lambda _root: [])
    monkeypatch.setattr(
        lint_mod,
        "run_deterministic_lint",
        lambda _config: {"summary": {"issues": 0, "high": 0, "medium": 0, "low": 0}, "issues": []},
    )

    result = lint_mod.run_lint(config, stages=["cluster-audit"])

    assert called == [True]
    assert result["reflection"]["cluster_reviews"] == 1
    assert result["reflection"]["skipped"] is False


def test_graph_reflection_schedule_gate_is_coarse(tmp_path, monkeypatch):
    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)
    clusters = _write_cluster_data(root)
    _write_jsonl(
        root / "derived" / "lint" / "graph_findings.jsonl",
        [
            {
                "finding_id": "graph:0",
                "finding_type": "bridge_gap",
                "severity": "medium",
                "summary": "Stored summary.",
                "details": "Stored details.",
                "affected_material_ids": ["mat_001", "mat_002"],
                "affected_cluster_ids": ["concept_001"],
                "candidate_future_sources": ["architectural typology"],
                "candidate_bridge_links": ["spatial memory"],
            }
        ],
    )
    _write_jsonl(
        root / "manifests" / "materials.jsonl",
        [
            {
                "material_id": "mat_001",
                "file_hash": "hash-001",
                "relative_path": "Research/One.pdf",
                "file_type": "pdf",
                "domain": "research",
                "collection": "papers",
                "ingested_at": "2026-01-01T00:00:00+00:00",
            },
            {
                "material_id": "mat_002",
                "file_hash": "hash-002",
                "relative_path": "Research/Two.pdf",
                "file_type": "pdf",
                "domain": "research",
                "collection": "papers",
                "ingested_at": "2026-01-02T00:00:00+00:00",
            },
        ],
    )
    _write_json(
        root / "extracted" / "mat_002" / "meta.json",
        {
            "material_id": "mat_002",
            "file_hash": "hash-002",
            "source_path": "Research/Two.pdf",
            "title": "Two",
            "authors": ["Author Two"],
            "year": "2026",
            "page_count": 1,
            "file_type": "pdf",
            "domain": "research",
            "collection": "papers",
            "raw_keywords": ["archive"],
            "raw_document_type": "paper",
            "summary": {"value": "Summary", "provenance": {}},
            "keywords": {"value": ["archive"], "provenance": {}},
            "document_type": {"value": "paper", "provenance": {}},
            "facets": {},
            "_enrichment_stamp": {"prompt_version": "enrich-v1.0", "enrichment_schema_version": "1"},
        },
    )
    rebuild_index(config)
    deterministic = run_deterministic_lint(config)
    concept_refs = [
        {
            "cluster_id": "concept_001",
            "slug": "archive-and-space",
            "canonical_name": "Archive and Space",
            "main_takeaways": ["Shared concern with spatial archives"],
            "main_tensions": ["Theory vs use"],
            "open_questions": ["What is the archive doing?"],
            "why_this_concept_matters": "It shapes the whole corpus.",
            "supporting_material_ids": ["mat_001", "mat_002"],
            "supporting_evidence": ["shared archive frame"],
            "input_fingerprint": "def",
        }
    ]
    collection_refs = [
        {
            "collection_key": "research/papers",
            "domain": "research",
            "collection": "papers",
            "main_takeaways": ["The collection centers archival space."],
            "main_tensions": ["Theory vs use"],
            "important_material_ids": ["mat_001", "mat_002"],
            "important_cluster_ids": ["concept_001"],
            "open_questions": ["What else is in the archive?"],
            "input_fingerprint": "ghi",
        }
    ]
    _write_json(
        root / "derived" / "global_bridge_stamp.json",
        {
            "bridged_at": "2026-01-03T00:00:00+00:00",
            "fingerprint": "bridge-fingerprint",
            "global_bridges": len(clusters),
        },
    )
    _write_json(
        root / "derived" / "lint" / "lint_stamp.json",
        {
            "graph_reflection_at": "2026-01-02T01:00:00+00:00",
        },
    )
    _write_json(
        root / "derived" / "lint" / "graph_reflection_stamp.json",
        {
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "graph_fingerprint": "different-fingerprint",
            "cluster_count": len(clusters),
            "material_count": 2,
            "finding_count": 1,
        },
    )

    assert _graph_reflection_due(root, config, clusters, _load_manifest(root), concept_refs, collection_refs, deterministic) == (False, "graph reflection deferred by schedule")


def test_graph_reflection_packet_uses_only_collection_and_bridge_signals(tmp_path, monkeypatch):
    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)
    deterministic = run_deterministic_lint(config)
    bridge_clusters = [
        {
            "cluster_id": "bridge_001",
            "canonical_name": "Archive and Space",
            "domain_collection_keys": ["research/papers", "practice/projects"],
            "material_ids": ["mat_001", "mat_002"],
            "bridge_takeaways": ["Archive logic recurs across collections."],
            "bridge_tensions": ["Theory and practice pull in different directions."],
            "bridge_open_questions": ["What architectural form stabilizes the archive?"],
            "helpful_new_sources": ["oral histories of archive reuse"],
            "why_this_bridge_matters": "It is a cross-collection seam.",
        }
    ]
    concept_refs = [
        {
            "cluster_id": "local_001",
            "canonical_name": "Local Archive Detail",
            "helpful_new_sources": ["site survey"],
            "why_this_concept_matters": "Purely local.",
        },
    ]
    collection_refs = [
        {
            "collection_key": "research/papers",
            "main_takeaways": ["The collection centers archival space."],
            "main_tensions": ["Theory vs use"],
            "open_questions": ["What else is in the archive?"],
            "why_this_collection_matters": "It establishes the research side.",
        }
    ]

    packet = _graph_reflection_packet(
        deterministic,
        bridge_clusters,
        concept_refs,
        collection_refs,
        _load_manifest(root),
    )

    assert "cluster_reviews" not in packet
    assert "concept_threads" not in packet
    assert packet["graph_state"]["global_bridges"] == 1
    assert packet["graph_state"]["bridge_reflections"] == 1
    assert packet["graph_state"]["collection_reflections"] == 1
    assert packet["bridge_threads"][0]["bridge_id"] == "bridge_001"
    assert packet["bridge_threads"][0]["helpful_new_sources"] == ["oral histories of archive reuse"]
    assert packet["bridge_threads"][0]["why_this_bridge_matters"] == "It is a cross-collection seam."
