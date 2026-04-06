"""Tests for lint.py — Phase 6 deterministic health checks."""

from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, timezone

from click.testing import CliRunner

from arquimedes.compile_pages import _concept_wiki_path, _material_wiki_path
from arquimedes.enrich_stamps import canonical_hash
from arquimedes.index import rebuild_index
from arquimedes.lint import ReflectionIndexTool, _apply_bridge_cluster_maintenance, _build_material_info, _graph_reflection_due, _load_manifest, _run_bridge_cluster_discovery, _run_bridge_cluster_maintenance, _run_cluster_audit, _run_collection_reflections, _run_concept_reflections, _run_graph_reflection, run_deterministic_lint, run_lint
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
    _write_json(root / "wiki" / "research" / "papers" / "_index.md", {"body": "Collection home."})

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
        root / "derived" / "lint" / "cluster_audit_input.json",
        root / "derived" / "lint" / "cluster_audit_output.json",
        root / "derived" / "tmp" / "cluster_audit_bridge_output.json",
    )
    assert "## TODO" in prompt_system
    assert "- [ ] Return JSON only." in prompt_system
    assert "Prefer ambitious, useful connections" in prompt_system
    assert "Treat splitting as a last resort" in prompt_system
    assert "## FILES TO READ" in prompt_user
    assert "## JSON TO RETURN" in prompt_user
    assert "## WRITE FILES" in prompt_user
    assert "PROCESS_FINISHED" in prompt_user
    for record in first:
        assert {"review_id", "cluster_id", "finding_type", "severity", "recommendation", "affected_material_ids", "affected_concept_names", "evidence", "input_fingerprint", "wiki_path"} <= set(record)
    assert not (root / "derived" / "lint" / "cluster_audit_input.json").exists()
    assert not (root / "derived" / "lint" / "cluster_audit_output.json").exists()
    assert not (root / "derived" / "tmp" / "cluster_audit_bridge_output.json").exists()

    second, discovery2 = _run_cluster_audit(root, clusters, material_info, "test-route", llm_factory)
    assert len(second) == 1
    assert discovery2 == 0
    assert len(calls) == 1


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

    def llm_factory(stage: str):
        def fn(system: str, messages: list[dict]) -> str:
            calls.append(stage)
            return json.dumps(
                {
                    "main_takeaways": ["Shared concern with spatial archives"],
                    "main_tensions": ["Theory vs. use"],
                    "open_questions": ["What is the archive doing?"],
                    "why_this_concept_matters": "It shapes the whole corpus.",
                }
            )

        return fn

    first = _run_concept_reflections(root, list(clusters), material_info, llm_factory)
    assert len(first) == 1
    assert first[0]["cluster_id"] == "concept_001"
    assert len(calls) == 1
    assert {"cluster_id", "slug", "canonical_name", "main_takeaways", "main_tensions", "open_questions", "why_this_concept_matters", "supporting_material_ids", "supporting_evidence", "input_fingerprint", "wiki_path"} <= set(first[0])

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

    calls: list[str] = []

    def llm_factory(stage: str):
        def fn(system: str, messages: list[dict]) -> str:
            calls.append(stage)
            return json.dumps(
                {
                    "main_takeaways": ["The collection centers archival space."],
                    "main_tensions": ["Theory vs use"],
                    "important_material_ids": ["mat_001", "mat_002"],
                    "important_cluster_ids": ["concept_001"],
                    "open_questions": ["What else is in the archive?"],
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

    first = _run_collection_reflections(root, groups, list(clusters), llm_factory)
    assert len(first) == 1
    assert first[0]["collection_key"] == "research/papers"
    assert len(calls) == 1
    assert {"collection_key", "domain", "collection", "main_takeaways", "main_tensions", "important_material_ids", "important_cluster_ids", "open_questions", "input_fingerprint", "wiki_path"} <= set(first[0])

    second = _run_collection_reflections(root, groups, list(clusters), llm_factory)
    assert len(second) == 1
    assert len(calls) == 1


def test_graph_reflection_writes_schema_and_skips_unchanged(tmp_path, monkeypatch):
    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)
    _write_cluster_data(root)
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
            return json.dumps(
                [
                    {
                        "finding_type": "bridge",
                        "severity": "low",
                        "summary": "Add a missing bridge link.",
                        "details": "The graph could connect these materials more directly.",
                        "affected_material_ids": ["mat_001", "mat_002"],
                        "affected_cluster_ids": ["concept_001"],
                        "candidate_future_sources": ["oral history"],
                        "candidate_bridge_links": ["archive and memory"],
                    }
                ]
            )

        return fn

    first = _run_graph_reflection(root, deterministic_report, cluster_reviews, concept_refs, collection_refs, llm_factory)
    assert len(first) == 1
    assert first[0]["finding_id"] == "graph:0"
    assert len(calls) == 1
    assert {"finding_id", "finding_type", "severity", "summary", "details", "affected_material_ids", "affected_cluster_ids", "candidate_future_sources", "candidate_bridge_links", "input_fingerprint"} <= set(first[0])

    second = _run_graph_reflection(root, deterministic_report, cluster_reviews, concept_refs, collection_refs, llm_factory)
    assert len(second) == 1
    assert len(calls) == 1


def test_bridge_cluster_maintenance_merges_reviewed_duplicates(tmp_path, monkeypatch):
    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)
    _write_jsonl(root / "derived" / "bridge_concept_clusters.jsonl", [
        {
            "cluster_id": "bridge_0001",
            "canonical_name": "Archive and Space",
            "slug": "archive-and-space",
            "aliases": ["Archive and Space"],
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
            "confidence": 0.8,
            "wiki_path": "wiki/shared/bridge-concepts/archive-and-space.md",
        },
        {
            "cluster_id": "bridge_0002",
            "canonical_name": "Archival Space",
            "slug": "archival-space",
            "aliases": ["Archival Space"],
            "material_ids": ["mat_002"],
            "source_concepts": [
                {
                    "material_id": "mat_002",
                    "concept_name": "archival space",
                    "relevance": "medium",
                    "source_pages": [2],
                    "evidence_spans": ["archival space"],
                    "confidence": 0.8,
                }
            ],
            "confidence": 0.7,
            "wiki_path": "wiki/shared/bridge-concepts/archival-space.md",
        },
    ])
    _write_jsonl(root / "derived" / "cluster_reviews.jsonl", [
        {
            "review_id": "bridge_0001:0:merge",
            "cluster_id": "bridge_0001",
            "finding_type": "merge",
            "severity": "medium",
            "recommendation": "These bridge concepts are duplicates and should merge.",
            "affected_material_ids": ["mat_001", "mat_002"],
            "affected_concept_names": ["archive and space", "archival space"],
            "evidence": ["shared archive frame"],
            "input_fingerprint": "abc",
            "wiki_path": "wiki/shared/bridge-concepts/archive-and-space.md",
        }
    ])

    material_info = _build_material_info(root, [
        {"material_id": "mat_001"},
        {"material_id": "mat_002"},
    ])
    rebuild_index(config)

    calls: list[str] = []

    def llm_factory(stage: str):
        def fn(system: str, messages: list[dict]) -> str:
            calls.append(stage)
            return json.dumps({
                "actions": [
                    {
                        "action_type": "merge",
                        "target_cluster_ids": ["bridge_0001", "bridge_0002"],
                        "canonical_name": "Archive and Space",
                        "aliases": ["Archival Space"],
                        "reason": "They describe the same cross-material concept.",
                        "confidence": 0.9,
                    }
                ]
            })

        return fn

    with ReflectionIndexTool(root) as tool:
        updated, changed = _run_bridge_cluster_maintenance(
            root,
            [json.loads(line) for line in (root / "derived" / "bridge_concept_clusters.jsonl").read_text().splitlines()],
            [
                {
                    "review_id": "bridge_0001:0:merge",
                    "cluster_id": "bridge_0001",
                    "finding_type": "merge",
                    "severity": "medium",
                    "recommendation": "These bridge concepts are duplicates and should merge.",
                    "affected_material_ids": ["mat_001", "mat_002"],
                    "affected_concept_names": ["archive and space", "archival space"],
                    "evidence": ["shared archive frame"],
                    "input_fingerprint": "abc",
                    "wiki_path": "wiki/shared/bridge-concepts/archive-and-space.md",
                }
            ],
            material_info,
            llm_factory,
            tool,
        )

    assert changed == 1
    assert len(updated) == 1
    assert updated[0]["canonical_name"] == "Archive and Space"
    assert set(updated[0]["material_ids"]) == {"mat_001", "mat_002"}
    assert len(calls) == 1
    stamp = json.loads((root / "derived" / "bridge_cluster_stamp.json").read_text(encoding="utf-8"))
    assert stamp["clusters"] == 1


def test_bridge_discovery_appends_new_clusters_from_local_concepts(tmp_path, monkeypatch):
    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)
    _write_jsonl(root / "derived" / "bridge_concept_clusters.jsonl", [
        {
            "cluster_id": "bridge_0001",
            "canonical_name": "Archive and Space",
            "slug": "archive-and-space",
            "aliases": ["Archive and Space"],
            "material_ids": ["mat_001"],
            "source_concepts": [
                {
                    "material_id": "mat_001",
                    "concept_name": "archive and space",
                    "concept_key": "archive and space",
                    "relevance": "high",
                    "source_pages": [1],
                    "evidence_spans": ["archive and space"],
                    "confidence": 0.9,
                }
            ],
            "confidence": 0.9,
        }
    ])

    local_rows = [
        ("archival habitat", "archival habitat", "mat_002", "high", "[1]", '["archival habitat"]', 0.9, "local"),
        ("counterarchive", "counterarchive", "mat_003", "high", "[1]", '["counterarchive"]', 0.9, "local"),
    ]
    material_rows = [
        ("mat_001", "One", "Summary", '["archive"]'),
        ("mat_002", "Two", "Summary", '["archive"]'),
        ("mat_003", "Three", "Summary", '["archive"]'),
    ]
    monkeypatch.setattr("arquimedes.lint._load_local_concepts", lambda _root: (local_rows, material_rows))

    calls: list[str] = []

    def llm_factory(stage: str):
        def fn(system: str, messages: list[dict]) -> str:
            calls.append(stage)
            return json.dumps({
                "clusters": [
                    {
                        "canonical_name": "Archival Habitat and Counterarchive",
                        "aliases": ["archival habitat", "counterarchive"],
                        "source_concepts": [
                            {"material_id": "mat_002", "concept_name": "archival habitat"},
                            {"material_id": "mat_003", "concept_name": "counterarchive"},
                        ],
                        "confidence": 0.8,
                    }
                ]
            })

        return fn

    updated, changed = _run_bridge_cluster_discovery(
        root,
        [json.loads(line) for line in (root / "derived" / "bridge_concept_clusters.jsonl").read_text(encoding="utf-8").splitlines()],
        llm_factory,
        None,
    )

    assert changed == 1
    assert len(updated) == 2
    assert any(cluster["canonical_name"] == "Archival Habitat and Counterarchive" for cluster in updated)
    assert calls == ["lint"]
    assert (root / "derived" / "bridge_concept_clusters.jsonl").exists()


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
        "Concept page.\n\n"
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
            return json.dumps(
                {
                    "main_takeaways": ["Shared concern with spatial archives"],
                    "main_tensions": ["Theory vs. use"],
                    "open_questions": ["What is the archive doing?"],
                    "why_this_concept_matters": "It shapes the whole corpus.",
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
    assert "Prior note" in calls[0]
    assert '"current_reflection"' in calls[0]
    assert '"evidence"' in calls[0]


def test_collection_reflection_can_request_extra_context_via_tool(tmp_path, monkeypatch):
    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)
    _write_cluster_data(root)
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
    mat_002 = root / "extracted" / "mat_002"
    mat_002.mkdir(parents=True, exist_ok=True)
    _write_json(
        mat_002 / "meta.json",
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

    calls: list[str] = []

    def llm_factory(stage: str):
        def fn(system: str, messages: list[dict]) -> str:
            calls.append(messages[0]["content"])
            if len(calls) == 1:
                return json.dumps(
                    {
                        "main_takeaways": [],
                        "main_tensions": [],
                        "important_material_ids": [],
                        "important_cluster_ids": [],
                        "open_questions": [],
                        "context_requests": [
                            {"tool": "search_materials", "query": "archive", "limit": 1}
                        ],
                    }
                )
            return json.dumps(
                {
                    "main_takeaways": ["The collection centers archival space."],
                    "main_tensions": ["Theory vs use"],
                    "important_material_ids": ["mat_001", "mat_002"],
                    "important_cluster_ids": ["concept_001"],
                    "open_questions": ["What else is in the archive?"],
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

    with ReflectionIndexTool(root) as tool:
        first = _run_collection_reflections(root, groups, list(clusters), llm_factory, tool)

    assert len(first) == 1
    assert len(calls) == 2
    assert "Tool results" in calls[1]


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
