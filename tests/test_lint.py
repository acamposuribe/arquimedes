"""Tests for lint.py — Phase 6 deterministic health checks."""

from __future__ import annotations

import json
import re
from pathlib import Path
from datetime import datetime, timezone

import pytest
from click.testing import CliRunner

from arquimedes.compile_pages import _concept_wiki_path, _material_wiki_path
from arquimedes.enrich_stamps import canonical_hash
from arquimedes.index import rebuild_index
from arquimedes.lint import ReflectionIndexTool, _build_collection_reflection_evidence_payload, _build_concept_reflection_evidence_payload, _build_material_info, _filter_local_rows_not_in_bridge, _graph_reflection_due, _load_manifest, _memory_state_stale, _run_cluster_audit, _run_collection_reflections, _run_concept_reflections, _run_graph_reflection, run_deterministic_lint, run_lint
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


def test_run_lint_quick_writes_markdown_report(tmp_path, monkeypatch):
    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)

    result = run_lint(config, quick=True, report=True)

    assert result["mode"] == "quick"
    assert (root / "wiki" / "_lint_report.md").exists()


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

    calls: list[str] = []

    def llm_factory(stage: str):
        def fn(system: str, messages: list[dict]) -> str:
            calls.append(stage)
            return json.dumps([
                {
                    "finding_type": "merge",
                    "severity": "medium",
                    "recommendation": "Consider merging.",
                    "affected_material_ids": ["mat_001", "mat_002"],
                    "affected_concept_names": ["archive and space"],
                    "evidence": ["shared archive frame"],
                }
            ])

        return fn

    material_info = _build_material_info(root, [
        {"material_id": "mat_001"},
        {"material_id": "mat_002"},
    ])

    first, discovery = _run_cluster_audit(root, clusters, material_info, "test-route", llm_factory)
    assert len(first) == 1
    assert discovery == 0
    assert len(calls) == 1
    from arquimedes.lint import _cluster_audit_prompt
    prompt_system, prompt_user = _cluster_audit_prompt(
        root,
        root / "derived" / "tmp" / "cluster_audit_input.json",
        root / "derived" / "tmp" / "bridge_concept_clusters.audit.work.jsonl",
        root / "derived" / "tmp" / "cluster_reviews.audit.work.jsonl",
    )
    assert "## TODO" in prompt_system
    assert "- [ ] Audit the current bridge memory" in prompt_system
    assert "- [ ] Discover genuinely new bridge concepts" in prompt_system
    assert "- [ ] Edit the duplicated work files in place." in prompt_system
    assert "- [ ] Finish the work files cleanly and emit PROCESS_FINISHED as a stop marker." in prompt_system
    assert "Prefer ambitious, useful connections" in prompt_system
    assert "Treat splitting as a last resort" in prompt_system
    assert "Respect prior bridge memory" in prompt_system
    assert "do not guess" in prompt_system
    assert "context_requests array with up to 4 read-only SQL-index lookups" in prompt_system
    assert "You will get only one read-only context round" in prompt_system
    assert "Read these files:" in prompt_user
    assert "bridge_concept_clusters.audit.work.jsonl" in prompt_user
    assert "cluster_reviews.audit.work.jsonl" in prompt_user
    assert "The duplicated work files are the source of truth" in prompt_user
    assert "living justification log" in prompt_user
    assert "remove findings that have already been resolved or acted upon" in prompt_user
    assert "keep or add positive review entries" in prompt_user
    assert "Keep the review file compact and current" in prompt_user
    assert "PROCESS_FINISHED" in prompt_user
    for record in first:
        assert {"review_id", "cluster_id", "finding_type", "severity", "recommendation", "affected_material_ids", "affected_concept_names", "evidence", "input_fingerprint", "wiki_path", "context_requested", "context_request_count"} <= set(record)
    assert not (root / "derived" / "tmp" / "cluster_audit_input.json").exists()
    assert not (root / "derived" / "tmp" / "bridge_concept_clusters.audit.work.jsonl").exists()
    assert not (root / "derived" / "tmp" / "cluster_reviews.audit.work.jsonl").exists()

    second, discovery2 = _run_cluster_audit(root, clusters, material_info, "test-route", llm_factory)
    assert len(second) == 1
    assert discovery2 == 0
    assert len(calls) == 1


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

    def llm_factory(stage: str):
        def fn(system: str, messages: list[dict]) -> str:
            calls.append(stage)
            prompt = messages[0]["content"]
            prompts.append(prompt)
            match = re.search(r"- Work file: (.+)", prompt)
            assert match is not None
            work_path = Path(match.group(1).strip())
            work_path.write_text(
                json.dumps(
                    {
                        "main_takeaways": ["Shared concern with spatial archives"],
                        "main_tensions": ["Theory vs. use"],
                        "open_questions": ["What is the archive doing?"],
                        "why_this_concept_matters": "It shapes the whole corpus.",
                    }
                ),
                encoding="utf-8",
            )
            return "PROCESS_FINISHED"

        return fn

    first = _run_concept_reflections(root, list(clusters), material_info, llm_factory)
    assert len(first) == 1
    assert first[0]["cluster_id"] == "concept_001"
    assert len(calls) == 1
    assert "Concept wiki page:" in prompts[0]
    assert "SQL evidence file:" in prompts[0]
    assert "Work file:" in prompts[0]
    assert "PROCESS_FINISHED" in prompts[0]
    assert {"cluster_id", "slug", "canonical_name", "main_takeaways", "main_tensions", "open_questions", "why_this_concept_matters", "supporting_material_ids", "supporting_evidence", "input_fingerprint", "wiki_path"} <= set(first[0])

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

    def llm_factory(stage: str):
        def fn(system: str, messages: list[dict]) -> str:
            calls.append(stage)
            prompt = messages[0]["content"]
            prompts.append(prompt)
            page_match = re.search(r"- Collection wiki page: (.+)", prompt)
            assert page_match is not None
            page_copy_text = Path(page_match.group(1).strip()).read_text(encoding="utf-8")
            assert "archive map" in page_copy_text
            assert ".md" not in page_copy_text
            assert "Collection wiki page:" in prompt
            assert "SQL evidence file:" in prompt
            assert "Work file:" in prompt
            assert "main takeaways" in prompt.lower()
            assert "important bridge concepts" in prompt.lower()
            assert "methodological conclusions" in prompt.lower()
            assert "main content learnings" in prompt.lower()
            assert "new_materials" in prompt
            assert "old_materials" in prompt
            assert "chunks are only secondary support" in prompt.lower()
            assert "why this collection matters" in prompt.lower()
            match = re.search(r"- Work file: (.+)", prompt)
            assert match is not None
            work_path = Path(match.group(1).strip())
            work_path.write_text(
                json.dumps(
                    {
                        "main_takeaways": ["The collection centers archival space."],
                        "main_tensions": ["Theory vs use"],
                        "important_material_ids": ["mat_001", "mat_002"],
                        "important_cluster_ids": ["concept_001"],
                        "open_questions": ["What else is in the archive?"],
                        "why_this_collection_matters": "It shapes the collection as a whole.",
                    }
                ),
                encoding="utf-8",
            )
            return "PROCESS_FINISHED"

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
    assert first[0]["why_this_collection_matters"] == "It shapes the collection as a whole."
    assert {"collection_key", "domain", "collection", "main_takeaways", "main_tensions", "important_material_ids", "important_cluster_ids", "open_questions", "why_this_collection_matters", "input_fingerprint", "wiki_path"} <= set(first[0])

    second = _run_collection_reflections(root, groups, list(clusters), llm_factory, tool)
    assert len(second) == 1
    assert len(calls) == 1
    assert not (root / "derived" / "tmp" / "collection_reflections" / "research__papers.evidence.json").exists()
    assert not (root / "derived" / "tmp" / "collection_reflections" / "research__papers.work.json").exists()


def test_collection_reflection_evidence_uses_material_conclusions_and_keeps_chunks_small(tmp_path, monkeypatch):
    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)

    clusters = [
        {
            "cluster_id": "concept_001",
            "canonical_name": "Archive and Space",
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
    assert payload["bridge_concepts"][0]["main_takeaways"] == ["Shared archive frame"]
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
    cluster_reviews = [
        {
            "review_id": "concept_001:0:merge",
            "cluster_id": "concept_001",
            "finding_type": "merge",
            "severity": "medium",
            "recommendation": "Consider merging.",
            "affected_material_ids": ["mat_001", "mat_002"],
            "affected_concept_names": ["archive and space"],
            "evidence": ["shared archive frame"],
            "input_fingerprint": "abc",
            "wiki_path": "wiki/shared/bridge-concepts/archive-and-space.md",
        }
    ]
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
            calls.append(stage)
            prompt = messages[0]["content"]
            assert "Graph-state packet:" in prompt
            assert "Work file:" in prompt
            work_match = re.search(r"- Work file: (.+)", prompt)
            assert work_match is not None
            work_path = Path(work_match.group(1).strip())
            work_path.write_text(
                json.dumps({
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
                    ]
                }),
                encoding="utf-8",
            )
            return "PROCESS_FINISHED"

        return fn

    first = _run_graph_reflection(root, deterministic_report, cluster_reviews, concept_refs, collection_refs, clusters, _load_manifest(root), llm_factory)
    assert first["graph_maintenance"] == 1
    assert len(calls) == 1
    page_path = root / "wiki" / "shared" / "maintenance" / "graph-health.md"
    assert not page_path.exists()
    findings_path = root / "derived" / "lint" / "graph_findings.jsonl"
    assert findings_path.exists()
    findings = findings_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(findings) == 1
    assert "Archive and Space still wants a stronger link to architectural form." in findings[0]

    second = _run_graph_reflection(root, deterministic_report, cluster_reviews, concept_refs, collection_refs, clusters, _load_manifest(root), llm_factory)
    assert second["graph_maintenance"] == 0
    assert len(calls) == 1
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
            match = re.search(r"- Work file: (.+)", prompt)
            assert match is not None
            work_path = Path(match.group(1).strip())
            work_path.write_text(
                json.dumps(
                    {
                        "main_takeaways": ["Shared concern with spatial archives"],
                        "main_tensions": ["Theory vs. use"],
                        "open_questions": ["What is the archive doing?"],
                        "why_this_concept_matters": "It shapes the whole corpus.",
                    }
                ),
                encoding="utf-8",
            )
            return "PROCESS_FINISHED"

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
    assert "Work file:" in calls[0]


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

    assert set(payload) == {"cluster_id", "canonical_name", "slug", "aliases", "materials"}
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

    # Current deterministic fingerprint must match the stamp so only scheduling matters.
    deterministic = run_deterministic_lint(config)
    full_fp = canonical_hash(deterministic.get("summary", {}), deterministic.get("issues", []))
    assert full_fp

    # Fresh full-lint stamp should skip the expensive reflective passes entirely.
    _write_json(
        root / "derived" / "lint" / "full_lint_stamp.json",
        {
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "deterministic_fingerprint": full_fp,
            "summary": deterministic.get("summary", {}),
        },
    )

    def llm_factory(stage: str):
        def fn(system: str, messages: list[dict]) -> str:
            raise AssertionError("full lint should have been skipped by schedule")
            return fn

    result = run_lint(config, full=True, scheduled=True, llm_factory=llm_factory)

    assert result["reflection"]["skipped"] is True


def test_graph_reflection_schedule_gate_is_coarse(tmp_path, monkeypatch):
    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)
    clusters = _write_cluster_data(root)
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
    cluster_reviews = [
        {
            "cluster_id": "concept_001",
            "finding_type": "merge",
            "severity": "medium",
            "recommendation": "Merge cautiously.",
            "affected_material_ids": ["mat_001", "mat_002"],
            "affected_concept_names": ["archive and space"],
            "evidence": ["shared archive frame"],
            "input_fingerprint": "abc",
        }
    ]
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
        root / "derived" / "lint" / "graph_reflection_stamp.json",
        {
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "graph_fingerprint": "different-fingerprint",
            "cluster_count": len(clusters),
            "material_count": 2,
            "finding_count": 1,
        },
    )

    assert _graph_reflection_due(root, config, clusters, _load_manifest(root), cluster_reviews, concept_refs, collection_refs, deterministic) == (False, "graph reflection deferred by schedule")
