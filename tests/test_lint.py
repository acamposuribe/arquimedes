"""Tests for lint.py — Phase 6 deterministic health checks."""

from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, timezone

from click.testing import CliRunner

from arquimedes.compile_pages import _concept_wiki_path, _material_wiki_path
from arquimedes.enrich_stamps import canonical_hash
from arquimedes.index import rebuild_index
from arquimedes.lint import ReflectionIndexTool, _apply_collection_reflection_to_page, _apply_concept_reflection_to_page, _apply_local_concept_reflection_to_page, _build_material_info, _graph_reflection_due, _load_manifest, _run_cluster_audit, _run_collection_reflections, _run_concept_reflections, _run_graph_reflection, _run_local_concept_reflections, run_deterministic_lint, run_lint
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
        derived / "concept_clusters.jsonl",
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
    _write_jsonl(root / "derived" / "concept_clusters.jsonl", clusters)
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
    _write_jsonl(root / "derived" / "bridge_concept_clusters.jsonl", [])

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

    first = _run_cluster_audit(root, clusters, material_info, llm_factory)
    assert len(first) == 2
    assert len(calls) == 2
    for record in first:
        assert {"review_id", "cluster_id", "finding_type", "severity", "recommendation", "affected_material_ids", "affected_concept_names", "evidence", "input_fingerprint", "wiki_path"} <= set(record)

    second = _run_cluster_audit(root, clusters, material_info, llm_factory)
    assert len(second) == 2
    assert len(calls) == 2


def test_concept_reflection_only_targets_multi_material_clusters_and_skips_unchanged(tmp_path, monkeypatch):
    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)
    _write_cluster_data(root)
    _write_jsonl(root / "derived" / "bridge_concept_clusters.jsonl", [])

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

    clusters = json.loads((root / "derived" / "concept_clusters.jsonl").read_text().splitlines()[0]), json.loads((root / "derived" / "concept_clusters.jsonl").read_text().splitlines()[1])
    material_info = _build_material_info(root, [
        {"material_id": "mat_001"},
        {"material_id": "mat_002"},
    ])

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
    _write_jsonl(root / "derived" / "concept_clusters.jsonl", [
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
    _write_jsonl(root / "derived" / "bridge_concept_clusters.jsonl", [])

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
    clusters = json.loads((root / "derived" / "concept_clusters.jsonl").read_text().splitlines()[0]), json.loads((root / "derived" / "concept_clusters.jsonl").read_text().splitlines()[1])

    first = _run_collection_reflections(root, groups, list(clusters), llm_factory)
    assert len(first) == 1
    assert first[0]["collection_key"] == "research/papers"
    assert len(calls) == 1
    assert {"collection_key", "domain", "collection", "main_takeaways", "main_tensions", "important_material_ids", "important_cluster_ids", "open_questions", "input_fingerprint", "wiki_path"} <= set(first[0])

    second = _run_collection_reflections(root, groups, list(clusters), llm_factory)
    assert len(second) == 1
    assert len(calls) == 1


def test_local_concept_reflection_targets_grouped_local_concepts_and_skips_unchanged(tmp_path, monkeypatch):
    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)

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
        _write_jsonl(
            mat_dir / "concepts.jsonl",
            [
                {"concept_name": "archival habitat", "relevance": "high"},
                {"concept_name": "archive architecture", "relevance": "medium"},
            ],
        )

    rebuild_index(config)
    _write_json(root / "wiki" / "shared" / "concepts" / "_index.md", {
        "body": "<!-- phase6:local-concepts-reflection:research/papers:start -->\n## Local Concepts Reflection — Research / Papers\n- Prior local note\n<!-- phase6:local-concepts-reflection:research/papers:end -->\n",
    })

    calls: list[str] = []

    def llm_factory(stage: str):
        def fn(system: str, messages: list[dict]) -> str:
            calls.append(messages[0]["content"])
            return json.dumps(
                {
                    "main_takeaways": ["The collection has a stable local vocabulary."],
                    "main_tensions": ["Specificity vs reuse"],
                    "important_concept_names": ["archival habitat"],
                    "important_material_ids": ["mat_001", "mat_002"],
                    "supporting_concepts": ["archive architecture"],
                    "supporting_material_ids": ["mat_001"],
                    "supporting_evidence": ["archival habitat"],
                    "open_questions": ["Which local concepts should bridge?"],
                    "why_this_local_concepts_group_matters": "It captures the raw vocabulary before bridging.",
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

    with ReflectionIndexTool(root) as tool:
        first = _run_local_concept_reflections(root, groups, llm_factory, tool)

    assert len(first) == 1
    assert first[0]["collection_key"] == "research/papers"
    assert len(calls) == 1
    assert "Prior local note" in calls[0]
    assert {"collection_key", "domain", "collection", "main_takeaways", "main_tensions", "important_concept_names", "important_material_ids", "supporting_concepts", "supporting_material_ids", "supporting_evidence", "open_questions", "why_this_local_concepts_group_matters", "input_fingerprint", "wiki_path"} <= set(first[0])

    with ReflectionIndexTool(root) as tool:
        second = _run_local_concept_reflections(root, groups, llm_factory, tool)

    assert len(second) == 1
    assert len(calls) == 1


def test_graph_reflection_writes_schema_and_skips_unchanged(tmp_path, monkeypatch):
    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)
    _write_cluster_data(root)
    _write_jsonl(root / "derived" / "bridge_concept_clusters.jsonl", [])
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
    local_concept_refs = [
        {
            "collection_key": "research/papers",
            "domain": "research",
            "collection": "papers",
            "main_takeaways": ["The collection has a stable local vocabulary."],
            "main_tensions": ["Specificity vs reuse"],
            "important_concept_names": ["archival habitat"],
            "important_material_ids": ["mat_001", "mat_002"],
            "supporting_concepts": ["archive architecture"],
            "supporting_material_ids": ["mat_001"],
            "supporting_evidence": ["archival habitat"],
            "open_questions": ["Which local concepts should bridge?"],
            "why_this_local_concepts_group_matters": "It captures the raw vocabulary before bridging.",
            "input_fingerprint": "loc",
            "wiki_path": "wiki/shared/concepts/_index.md",
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

    first = _run_graph_reflection(root, deterministic_report, cluster_reviews, local_concept_refs, concept_refs, collection_refs, llm_factory)
    assert len(first) == 1
    assert first[0]["finding_id"] == "graph:0"
    assert len(calls) == 1
    assert {"finding_id", "finding_type", "severity", "summary", "details", "affected_material_ids", "affected_cluster_ids", "candidate_future_sources", "candidate_bridge_links", "input_fingerprint"} <= set(first[0])

    second = _run_graph_reflection(root, deterministic_report, cluster_reviews, local_concept_refs, concept_refs, collection_refs, llm_factory)
    assert len(second) == 1
    assert len(calls) == 1


def test_reflection_index_tool_supports_read_only_search_and_open_record(tmp_path, monkeypatch):
    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)
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
    _write_jsonl(root / "derived" / "bridge_concept_clusters.jsonl", [])
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
    clusters = json.loads((root / "derived" / "concept_clusters.jsonl").read_text().splitlines()[0]), json.loads((root / "derived" / "concept_clusters.jsonl").read_text().splitlines()[1])

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
    _write_jsonl(root / "derived" / "bridge_concept_clusters.jsonl", [])
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
    clusters = json.loads((root / "derived" / "concept_clusters.jsonl").read_text().splitlines()[0]), json.loads((root / "derived" / "concept_clusters.jsonl").read_text().splitlines()[1])

    with ReflectionIndexTool(root) as tool:
        first = _run_collection_reflections(root, groups, list(clusters), llm_factory, tool)

    assert len(first) == 1
    assert len(calls) == 2
    assert "Tool results" in calls[1]


def test_page_update_helpers_write_marked_reflection_sections(tmp_path, monkeypatch):
    root, config = _setup_repo(tmp_path)
    monkeypatch.chdir(root)
    _write_json(root / "wiki" / "shared" / "concepts" / "archive-and-space.md", {"body": "Concept page."})
    _write_json(root / "wiki" / "shared" / "concepts" / "_index.md", {"body": "Local concepts index."})
    _write_json(root / "wiki" / "research" / "papers" / "_index.md", {"body": "Collection home."})

    concept_record = {
        "cluster_id": "concept_001",
        "slug": "archive-and-space",
        "canonical_name": "Archive and Space",
        "main_takeaways": ["Shared concern with spatial archives"],
        "main_tensions": ["Theory vs use"],
        "open_questions": ["What is the archive doing?"],
        "why_this_concept_matters": "It shapes the whole corpus.",
        "supporting_material_ids": ["mat_001", "mat_002"],
        "supporting_evidence": ["shared archive frame"],
        "wiki_path": "wiki/shared/bridge-concepts/archive-and-space.md",
    }
    collection_record = {
        "domain": "research",
        "collection": "papers",
        "main_takeaways": ["The collection centers archival space."],
        "main_tensions": ["Theory vs use"],
        "important_material_ids": ["mat_001", "mat_002"],
        "important_cluster_ids": ["concept_001"],
        "open_questions": ["What else is in the archive?"],
        "wiki_path": "wiki/research/papers/_index.md",
    }
    local_concept_record = {
        "collection_key": "research/papers",
        "domain": "research",
        "collection": "papers",
        "main_takeaways": ["The collection has a stable local vocabulary."],
        "main_tensions": ["Specificity vs reuse"],
        "important_concept_names": ["archival habitat"],
        "important_material_ids": ["mat_001", "mat_002"],
        "supporting_concepts": ["archive architecture"],
        "supporting_material_ids": ["mat_001"],
        "supporting_evidence": ["archival habitat"],
        "open_questions": ["Which local concepts should bridge?"],
        "why_this_local_concepts_group_matters": "It captures the raw vocabulary before bridging.",
        "wiki_path": "wiki/shared/concepts/_index.md",
    }

    assert _apply_concept_reflection_to_page(root, concept_record) is True
    assert _apply_collection_reflection_to_page(root, collection_record) is True
    assert _apply_local_concept_reflection_to_page(root, local_concept_record) is True

    concept_page = (root / "wiki" / "shared" / "bridge-concepts" / "archive-and-space.md").read_text(encoding="utf-8")
    collection_page = (root / "wiki" / "research" / "papers" / "_index.md").read_text(encoding="utf-8")
    local_concepts_page = (root / "wiki" / "shared" / "concepts" / "_index.md").read_text(encoding="utf-8")
    assert "<!-- phase6:concept-reflection:start -->" in concept_page
    assert "<!-- phase6:collection-reflection:start -->" in collection_page
    assert "<!-- phase6:local-concepts-reflection:research/papers:start -->" in local_concepts_page


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

    assert _graph_reflection_due(root, config, clusters, _load_manifest(root), cluster_reviews, [], concept_refs, collection_refs, deterministic) == (False, "graph reflection deferred by schedule")
