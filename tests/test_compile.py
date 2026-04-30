"""Tests for Phase 5 — concept clustering and wiki compilation."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from arquimedes.cluster import (
    _validate_bridge_and_attach_provenance,
    local_cluster_path,
)
from arquimedes.compile_pages import (
    _concept_wiki_path,
    _material_wiki_path,
    _relative_link,
    render_concept_page,
    render_collection_page,
    render_glossary,
    render_index_page,
    render_material_page,
    _chicago_citation,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

CONCEPT_ROWS = [
    # (concept_name, concept_key, material_id, relevance, source_pages, evidence_spans, confidence)
    ("archival habitat", "archival habitat", "mat_aaa", "high", "[1, 3]", '["the archive as a built environment"]', 0.9),
    ("archive as space", "archive as space", "mat_bbb", "high", "[2, 5]", '["spatial dimension of archival practice"]', 0.85),
    ("memory palace", "memory palace", "mat_aaa", "medium", "[4]", '["structures of collective memory"]', 0.7),
]

MATERIAL_ROWS = [
    ("mat_aaa", "Necropolitics"),
    ("mat_bbb", "Archival Landscapes"),
]

RAW_LLM_CLUSTERS = [
    {
        "cluster_id": "concept_0001",
        "canonical_name": "archive as architectural space",
        "aliases": ["archive as architectural space", "archival habitat", "archive as space"],
        "source_concepts": [
            {"material_id": "mat_aaa", "concept_name": "archival habitat"},
            {"material_id": "mat_bbb", "concept_name": "archive as space"},
        ],
        "confidence": 0.85,
    },
    {
        "cluster_id": "concept_0002",
        "canonical_name": "memory palace",
        "aliases": ["memory palace"],
        "source_concepts": [
            {"material_id": "mat_aaa", "concept_name": "memory palace"},
        ],
        "confidence": 0.9,
    },
]


def _concept_index_from_rows(rows):
    idx = {}
    for row in rows:
        if len(row) >= 9:
            concept_name, concept_key, material_id, relevance, source_pages, evidence_spans, confidence, concept_type, descriptor = row[:9]
        elif len(row) == 8:
            concept_name, concept_key, material_id, relevance, source_pages, evidence_spans, confidence, concept_type = row
            descriptor = ""
        else:
            concept_name, concept_key, material_id, relevance, source_pages, evidence_spans, confidence = row
            concept_type = "local"
            descriptor = ""
        idx[(material_id, concept_key)] = {
            "concept_name": concept_name,
            "concept_key": concept_key,
            "material_id": material_id,
            "concept_type": concept_type,
            "relevance": relevance,
            "source_pages": source_pages,
            "evidence_spans": evidence_spans,
            "confidence": confidence,
            "descriptor": descriptor,
        }
    return idx


def _make_sqlite_db(tmp_path: Path) -> Path:
    """Create a minimal search.sqlite with concepts + materials tables."""
    db_path = tmp_path / "search.sqlite"
    con = sqlite3.connect(str(db_path))
    con.execute(
        "CREATE TABLE concepts ("
        "concept_name TEXT, concept_key TEXT, material_id TEXT, concept_type TEXT DEFAULT 'local',"
        "relevance TEXT, source_pages TEXT, evidence_spans TEXT, confidence REAL,"
        "PRIMARY KEY (material_id, concept_type, concept_key))"
    )
    con.executemany(
        "INSERT INTO concepts VALUES (?,?,?,?,?,?,?,?)",
        [(row[0], row[1], row[2], "local", row[3], row[4], row[5], row[6]) for row in CONCEPT_ROWS],
    )
    con.execute(
        "CREATE TABLE materials (material_id TEXT PRIMARY KEY, title TEXT,"
        "summary TEXT DEFAULT '', domain TEXT DEFAULT '', collection TEXT DEFAULT '',"
        "document_type TEXT DEFAULT '', raw_document_type TEXT DEFAULT '',"
        "authors TEXT DEFAULT '', year TEXT DEFAULT '', file_type TEXT DEFAULT '',"
        "page_count INTEGER DEFAULT 0, keywords TEXT DEFAULT '',"
        "raw_keywords TEXT DEFAULT '', building_type TEXT DEFAULT '',"
        "scale TEXT DEFAULT '', location TEXT DEFAULT '', jurisdiction TEXT DEFAULT '',"
        "climate TEXT DEFAULT '', program TEXT DEFAULT '',"
        "material_system TEXT DEFAULT '', structural_system TEXT DEFAULT '',"
        "historical_period TEXT DEFAULT '', course_topic TEXT DEFAULT '',"
        "studio_project TEXT DEFAULT '')"
    )
    con.executemany("INSERT INTO materials (material_id, title) VALUES (?, ?)", MATERIAL_ROWS)
    # material_keywords + material_authors
    con.execute("CREATE TABLE material_keywords (material_id TEXT, keyword TEXT, PRIMARY KEY (material_id, keyword))")
    con.execute("CREATE TABLE material_authors (material_id TEXT, author TEXT, PRIMARY KEY (material_id, author))")
    con.commit()
    con.close()
    return db_path


def _make_meta(material_id: str = "mat_aaa", title: str = "Test") -> dict:
    """Realistic meta shape matching enriched meta.json (enriched fields as {value, provenance})."""
    def _ef(val):
        """Wrap a value as an enriched field."""
        return {"value": val, "provenance": {"model": "test", "confidence": 1.0}}

    return {
        "material_id": material_id,
        "title": title,
        "authors": ["Alice"],
        "year": "2024",
        "document_type": _ef("paper"),
        "raw_document_type": "article",
        "domain": "research",
        "collection": "papers",
        "page_count": 10,
        "summary": _ef("A test summary."),
        "facets": {
            "scale": _ef("building"),
            "location": _ef(""),
            "building_type": _ef(""),
        },
        # no top-level facet keys — they live under "facets"
    }


def test_validate_bridge_allows_multiple_concepts_from_same_material():
    """Bridge clusters can preserve multiple concepts from one material when they support one umbrella."""
    raw_clusters = [{
        "cluster_id": "bridge_0001",
        "canonical_name": "archive as spatial memory infrastructure",
        "descriptor": "How archives organize memory as a built spatial system.",
        "aliases": ["archival habitat", "memory palace", "archive as space"],
        "source_concepts": [
            {"material_id": "mat_aaa", "concept_name": "archival habitat"},
            {"material_id": "mat_aaa", "concept_name": "memory palace"},
            {"material_id": "mat_bbb", "concept_name": "archive as space"},
        ],
        "confidence": 0.8,
    }]

    rows = [CONCEPT_ROWS[0], CONCEPT_ROWS[1], CONCEPT_ROWS[2]]
    validated = _validate_bridge_and_attach_provenance(
        raw_clusters,
        _concept_index_from_rows(rows),
        dict(MATERIAL_ROWS),
    )

    assert len(validated) == 1
    assert validated[0]["canonical_name"] == "archive as spatial memory infrastructure"
    assert validated[0]["descriptor"] == "How archives organize memory as a built spatial system."
    assert len(validated[0]["source_concepts"]) == 3
    assert set(validated[0]["material_ids"]) == {"mat_aaa", "mat_bbb"}


def test_material_page_sections():
    """Rendered material page contains all expected sections."""
    meta = _make_meta("mat_aaa", "Necropolitics")
    meta["methodological_conclusions"] = {"value": ["Use archives as spatial evidence."], "provenance": {}}
    meta["main_content_learnings"] = {"value": ["Archival form shapes knowledge."], "provenance": {}}
    clusters = [{
        "cluster_id": "concept_0001",
        "canonical_name": "archive as architectural space",
        "slug": "archive-as-architectural-space",
        "aliases": [],
        "material_ids": ["mat_aaa"],
        "source_concepts": [{"material_id": "mat_aaa", "concept_name": "archival habitat", "descriptor": "archive as built form"}],
        "confidence": 0.85,
    }]
    chunks = [{"chunk_id": "c0", "text": "Some text", "summary": "Summary", "source_pages": [1]}]
    annotations = [{"annotation_id": "a0", "type": "highlight", "page": 1,
                    "quoted_text": "key passage", "comment": "interesting"}]
    figures = [{"figure_id": "fig_0001", "visual_type": "plan", "caption": "Ground floor plan",
                "description": "A floor plan.", "source_page": 2, "image_path": ""}]
    related = [{"material_id": "mat_bbb", "title": "Archival Landscapes",
                "reasons": ["shared concept: archive as architectural space"]}]

    page = render_material_page(
        meta, clusters, chunks, annotations, figures, related,
        material_paths={"mat_bbb": "wiki/research/papers/mat_bbb.md"},
    )

    assert "# Necropolitics" in page
    assert "## Metadata" in page
    assert "Alice" in page
    assert "## Summary" in page
    assert "A test summary." in page
    assert "## Material Conclusions" in page
    assert "- Use archives as spatial evidence." in page
    assert "Use archives as spatial evidence." in page
    assert "- Archival form shapes knowledge." in page
    assert "Archival form shapes knowledge." in page
    assert "['Use archives as spatial evidence.']" not in page
    assert "## Key Concepts" in page
    assert "archive as architectural space" in page
    assert "## Architecture Facets" in page
    assert "building" in page  # scale (extracted from {value: "building"})
    assert "## Figures" in page
    assert "Ground floor plan" in page
    assert "## Reader Annotations" in page
    assert '<details class="reader-annotations">' in page
    assert "<summary>Reader Annotations (1 annotation)</summary>" in page
    assert "key passage" in page
    assert "interesting" in page
    assert "## Related Materials" in page
    assert "Archival Landscapes" in page
    assert "[Archival Landscapes]" in page
    assert "(mat_bbb.md)" in page
    assert "## Source" in page


def test_practice_material_page_sections_are_spanish():
    meta = _make_meta("mat_aaa", "Proyecto")
    meta["domain"] = "practice"
    meta["methodological_conclusions"] = {"value": ["Comprobar compatibilidades antes de definir el detalle."], "provenance": {}}
    meta["main_content_learnings"] = {"value": ["La sección resuelve la relación entre estructura y envolvente."], "provenance": {}}
    page = render_material_page(meta, [], [], [], [], [])
    assert "## Metadatos" in page
    assert "## Resumen" in page
    assert "## Conclusiones del material" in page
    assert "**Conclusiones metodológicas**" in page
    assert "**Aprendizajes principales**" in page
    assert "## Fuente" in page


# ---------------------------------------------------------------------------
# Test 4: concept page evidence
# ---------------------------------------------------------------------------

def test_concept_page_evidence():
    """Concept page with 2 materials lists both with relevance and evidence spans."""
    cluster = {
        "cluster_id": "concept_0001",
        "canonical_name": "archive as architectural space",
        "descriptor": "How archives operate as spatial environments and not just repositories.",
        "slug": "archive-as-architectural-space",
        "aliases": ["archival habitat"],
        "material_ids": ["mat_aaa", "mat_bbb"],
        "source_concepts": [
            {
                "material_id": "mat_aaa",
                "concept_name": "archival habitat",
                "descriptor": "archive as built form",
                "relevance": "high",
                "source_pages": [1, 3],
                "evidence_spans": ["the archive as a built environment"],
                "confidence": 0.9,
            },
            {
                "material_id": "mat_bbb",
                "concept_name": "archive as space",
                "relevance": "medium",
                "source_pages": [2],
                "evidence_spans": ["spatial dimension of archival practice"],
                "confidence": 0.85,
            },
        ],
        "confidence": 0.85,
    }
    material_titles = {"mat_aaa": "Necropolitics", "mat_bbb": "Archival Landscapes"}
    material_paths = {
        "mat_aaa": "wiki/research/papers/mat_aaa.md",
        "mat_bbb": "wiki/research/papers/mat_bbb.md",
    }
    related_concepts = [{"canonical_name": "memory palace", "slug": "memory-palace"}]

    page = render_concept_page(cluster, material_titles, related_concepts, material_paths)

    assert "# archive as architectural space" in page
    assert "How archives operate as spatial environments and not just repositories." in page
    assert "archival habitat" in page  # alias
    assert page.index("_Also known as: archival habitat_") < page.index("How archives operate as spatial environments and not just repositories.")
    assert "2 materials" in page
    assert "Necropolitics" in page
    assert "archive as built form" in page
    assert "high" in page
    assert "the archive as a built environment" in page
    assert "Archival Landscapes" in page
    assert "spatial dimension of archival practice" in page
    assert "## Related Concepts" in page


def test_practice_collection_page_sections_are_spanish():
    page = render_collection_page(
        "Práctica / Vivienda",
        "practice",
        "vivienda",
        [{"name": "Caso A", "path": "caso-a.md", "summary": "Resumen breve."}],
        [{"name": "detalle constructivo", "path": "detalle.md", "count": 2}],
        [{"field": "material_system", "value": "madera", "count": 2}],
        [{"name": "Caso A", "path": "caso-a.md", "ingested_at": "2026-04-27T00:00:00+00:00"}],
        {"main_takeaways": ["Sirve para comparar soluciones."], "main_tensions": [], "open_questions": [], "helpful_new_sources": [], "why_this_collection_matters": "Organiza decisiones repetibles."},
    )
    assert "## Resumen general" in page
    assert "## Reflexiones" in page
    assert "## Incorporaciones recientes" in page
    assert "## Materiales" in page
    assert "## Conceptos clave" in page
    assert "## Facetas principales" in page


def test_bridge_concept_page_renders_recent_changes_section():
    cluster = {
        "cluster_id": "bridge_0001",
        "canonical_name": "Archive Space Framework",
        "slug": "archive-space-framework",
        "wiki_path": "wiki/shared/bridge-concepts/archive-space-framework.md",
        "aliases": ["archival space framework"],
        "material_ids": ["mat_aaa", "mat_bbb"],
        "source_concepts": [
            {
                "material_id": "mat_aaa",
                "concept_name": "archival habitat",
                "relevance": "high",
                "source_pages": [1],
                "evidence_spans": ["the archive as a built environment"],
                "confidence": 0.9,
            }
        ],
    }
    review_rows = [
        {
            "cluster_id": "bridge_0001",
            "finding_type": "scope_extension",
            "severity": "low",
            "status": "validated",
            "note": "Added a new cross-material concept.",
            "recommendation": "Keep the bridge as-is.",
            "_provenance": {"run_at": "2026-04-08T17:35:47.613231+00:00"},
        }
    ]

    page = render_concept_page(cluster, {"mat_aaa": "Necropolitics"}, [], None, None, review_rows)

    assert "## Recent Changes" in page
    assert "### Scope Extension" in page
    assert "- Status: validated" in page
    assert "- Severity: low" in page
    assert "Added a new cross-material concept." in page
    assert "Keep the bridge as-is." in page


# ---------------------------------------------------------------------------
# Test 5: index pages
# ---------------------------------------------------------------------------

def test_index_pages():
    """Master index and concept index list correct material/concept counts."""
    entries = [
        {"name": "Necropolitics", "path": "research/papers/mat_aaa.md", "summary": "A paper."},
        {"name": "Archival Landscapes", "path": "research/papers/mat_bbb.md", "summary": "Another."},
    ]
    page = render_index_page("Research Papers", entries)
    assert "# Research Papers" in page
    assert "2 pages" in page
    assert "Necropolitics" in page
    assert "Archival Landscapes" in page
    # Alphabetical order
    assert page.index("Archival") < page.index("Necropolitics")

    # Glossary
    clusters = [
        {"canonical_name": "archive as architectural space", "slug": "archive-as-architectural-space"},
        {"canonical_name": "memory palace", "slug": "memory-palace"},
    ]
    glossary = render_glossary(clusters)
    assert "# Main Concepts" in glossary
    assert "archive as architectural space" in glossary
    assert "memory palace" in glossary


# ---------------------------------------------------------------------------
# Test 6: incremental skip
# ---------------------------------------------------------------------------

def test_incremental_skip(tmp_path, monkeypatch):
    """Unchanged material stamp → material page not rewritten; unchanged cluster stamp → concept pages not rewritten."""
    import arquimedes.compile as compile_mod
    import arquimedes.config as config_mod
    import arquimedes.cluster as cluster_mod
    import arquimedes.memory as memory_mod

    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "config.yaml").write_text("library_root: ~/dummy\n")
    (tmp_path / "manifests").mkdir()
    (tmp_path / "manifests" / "materials.jsonl").write_text(
        json.dumps({"material_id": "mat_aaa", "file_hash": "x",
                    "relative_path": "R/mat_aaa.pdf", "file_type": "pdf",
                    "domain": "research", "collection": "papers",
                    "ingested_at": "2026-01-01T00:00:00+00:00"})
    )

    db_path = _make_sqlite_db(tmp_path)
    index_dir = tmp_path / "indexes"
    index_dir.mkdir()
    import shutil
    shutil.copy(str(db_path), str(index_dir / "search.sqlite"))

    # Create extracted dir for one material
    extracted_dir = tmp_path / "extracted" / "mat_aaa"
    extracted_dir.mkdir(parents=True)
    meta = _make_meta("mat_aaa", "Necropolitics")
    (extracted_dir / "meta.json").write_text(json.dumps(meta), encoding="utf-8")
    (extracted_dir / "chunks.jsonl").write_text("", encoding="utf-8")
    (extracted_dir / "annotations.jsonl").write_text("", encoding="utf-8")

    derived_dir = tmp_path / "derived"
    derived_dir.mkdir()

    # Write cluster file + stamp (current)
    clusters_data = [{
        "cluster_id": "concept_0001",
        "canonical_name": "archive as architectural space",
        "slug": "archive-as-architectural-space",
        "aliases": [],
        "material_ids": ["mat_aaa"],
        "source_concepts": [{
            "material_id": "mat_aaa",
            "concept_name": "archival habitat",
            "relevance": "high",
            "source_pages": [1],
            "evidence_spans": ["test span"],
            "confidence": 0.9,
        }],
        "confidence": 0.85,
    }]
    clusters_text = "\n".join(json.dumps(c) for c in clusters_data) + "\n"
    (derived_dir / "bridge_concept_clusters.jsonl").write_text(clusters_text, encoding="utf-8")
    (derived_dir / "lint").mkdir(parents=True, exist_ok=True)
    (derived_dir / "lint" / "graph_findings.jsonl").write_text(
        json.dumps({
            "finding_id": "graph:0",
            "finding_type": "bridge_gap",
            "severity": "medium",
            "summary": "Archive and Space needs a stronger architectural anchor.",
            "details": "The bridge is useful but still semantically thin.",
            "affected_material_ids": ["mat_aaa"],
            "affected_cluster_ids": ["concept_0001"],
            "candidate_future_sources": ["architectural typology"],
            "candidate_bridge_links": ["spatial memory"],
            "input_fingerprint": "graph-fp",
        }) + "\n",
        encoding="utf-8",
    )

    cluster_stamp = compile_mod._cluster_file_stamp(tmp_path)
    mat_stamp = compile_mod._material_stamp(extracted_dir)

    # Write up-to-date compile stamp
    compile_mod._write_compile_stamp(
        tmp_path,
        {"mat_aaa": mat_stamp},
        cluster_stamp,
    )

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(config_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(config_mod, "load_config", lambda: {"llm": {"agent_cmd": "echo"}})
    monkeypatch.setattr(compile_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(memory_mod, "get_project_root", lambda: tmp_path)

    # Patch canonical local clustering to return "skipped"
    monkeypatch.setattr(
        cluster_mod,
        "cluster_concepts",
        lambda config, llm_fn=None, force=False, domain=None, collection=None: {
            "bridge_concepts": 3, "clusters": 1, "multi_material": 0, "skipped": True
        },
    )
    result = compile_mod.compile_wiki({"llm": {"agent_cmd": "echo"}}, force=False)

    assert result["material_pages"] == 0
    assert result["material_pages_skipped"] == 1
    assert result["concept_pages"] == 0  # cluster stamp unchanged
    graph_page = tmp_path / "wiki" / "shared" / "maintenance" / "graph-health.md"
    assert graph_page.exists()
    assert "Archive and Space needs a stronger architectural anchor." in graph_page.read_text(encoding="utf-8")


def test_compile_rebuilds_pages_when_template_version_changes(tmp_path, monkeypatch):
    """Legacy compile stamps without a template version trigger a full page rebuild."""
    import shutil

    import arquimedes.cluster as cluster_mod
    import arquimedes.compile as compile_mod
    import arquimedes.config as config_mod
    import arquimedes.memory as memory_mod

    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "config.yaml").write_text("library_root: ~/dummy\n")
    (tmp_path / "manifests").mkdir()
    (tmp_path / "manifests" / "materials.jsonl").write_text(
        json.dumps({
            "material_id": "mat_aaa",
            "file_hash": "x",
            "relative_path": "R/mat_aaa.pdf",
            "file_type": "pdf",
            "domain": "research",
            "collection": "papers",
            "ingested_at": "2026-01-01T00:00:00+00:00",
        }),
        encoding="utf-8",
    )

    db_path = _make_sqlite_db(tmp_path)
    index_dir = tmp_path / "indexes"
    index_dir.mkdir()
    shutil.copy(str(db_path), str(index_dir / "search.sqlite"))

    extracted_dir = tmp_path / "extracted" / "mat_aaa"
    extracted_dir.mkdir(parents=True)
    meta = _make_meta("mat_aaa", "Necropolitics")
    (extracted_dir / "meta.json").write_text(json.dumps(meta), encoding="utf-8")
    (extracted_dir / "chunks.jsonl").write_text("", encoding="utf-8")
    (extracted_dir / "annotations.jsonl").write_text("", encoding="utf-8")

    collection_dir = tmp_path / "derived" / "collections" / "research__papers"
    collection_dir.mkdir(parents=True, exist_ok=True)
    clusters_data = [{
        "cluster_id": "research__papers__local_0001",
        "domain": "research",
        "collection": "papers",
        "canonical_name": "Archive Space Framework",
        "slug": "archive-space-framework",
        "aliases": [],
        "descriptor": "A bridge joining local archive-space clusters.",
        "material_ids": ["mat_aaa"],
        "source_concepts": [{
            "material_id": "mat_aaa",
            "concept_name": "archival habitat",
            "relevance": "high",
            "source_pages": [1],
            "evidence_spans": ["test span"],
            "confidence": 0.9,
        }],
        "confidence": 0.85,
        "wiki_path": "wiki/research/papers/concepts/archive-space-framework.md",
    }]
    (collection_dir / "local_concept_clusters.jsonl").write_text(
        "\n".join(json.dumps(c) for c in clusters_data) + "\n",
        encoding="utf-8",
    )

    mat_stamp = compile_mod._material_stamp(extracted_dir)
    cluster_stamp = compile_mod._cluster_file_stamp(tmp_path)
    stamp_path = tmp_path / "derived" / "compile_stamp.json"
    stamp_path.parent.mkdir(parents=True, exist_ok=True)
    stamp_path.write_text(
        json.dumps({
            "compiled_at": "2026-01-01T00:00:00+00:00",
            "material_stamps": {"mat_aaa": mat_stamp},
            "cluster_stamp": cluster_stamp,
        }, separators=(",", ":")),
        encoding="utf-8",
    )

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(config_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(config_mod, "load_config", lambda: {"llm": {"agent_cmd": "echo"}})
    monkeypatch.setattr(compile_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(memory_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(
        cluster_mod,
        "cluster_concepts",
        lambda config, llm_fn=None, force=False, domain=None, collection=None: {"skipped": True},
    )

    result = compile_mod.compile_wiki({"llm": {"agent_cmd": "echo"}}, force=False, run_quick_lint=False)

    assert result["material_pages"] == 1
    assert result["material_pages_skipped"] == 0
    assert result["concept_pages"] == 1
    stamp_payload = json.loads(stamp_path.read_text(encoding="utf-8"))
    assert stamp_payload["template_version"] == compile_mod.COMPILE_TEMPLATE_VERSION
    assert (tmp_path / "wiki" / "research" / "papers" / "concepts" / "archive-space-framework.md").exists()


# ---------------------------------------------------------------------------
# Test 7: orphan removal
# ---------------------------------------------------------------------------

def test_orphan_removal(tmp_path):
    """Material removed from manifest → wiki page deleted; cluster removed → concept page deleted."""
    from arquimedes.compile import _remove_orphans

    wiki = tmp_path / "wiki"
    # Create an existing material page for a removed material
    orphan_mat = wiki / "practice" / "_general" / "old_mat.md"
    orphan_mat.parent.mkdir(parents=True)
    orphan_mat.write_text("# Old Material", encoding="utf-8")

    # Create an existing concept page for a removed cluster
    orphan_concept = wiki / "shared" / "concepts" / "old-concept.md"
    orphan_concept.parent.mkdir(parents=True)
    orphan_concept.write_text("# Old Concept", encoding="utf-8")

    # Create an existing bridge concept page for a removed bridge cluster
    orphan_bridge = wiki / "shared" / "bridge-concepts" / "old-bridge.md"
    orphan_bridge.parent.mkdir(parents=True)
    orphan_bridge.write_text("# Old Bridge Concept", encoding="utf-8")

    # Create a valid material page, concept index page, and bridge concept page
    valid_mat = wiki / "practice" / "_general" / "mat_aaa.md"
    valid_mat.write_text("# Valid", encoding="utf-8")
    concept_index = wiki / "shared" / "concepts" / "_index.md"
    concept_index.parent.mkdir(parents=True, exist_ok=True)
    concept_index.write_text("# Local Concepts", encoding="utf-8")
    valid_bridge = wiki / "shared" / "bridge-concepts" / "memory-and-place.md"
    valid_bridge.parent.mkdir(parents=True, exist_ok=True)
    valid_bridge.write_text("# Memory and Place", encoding="utf-8")

    removed = _remove_orphans(wiki, {"mat_aaa"}, {"memory-and-place"})

    assert not orphan_mat.exists()
    assert not orphan_concept.exists()
    assert not orphan_bridge.exists()
    assert valid_mat.exists()
    assert valid_bridge.exists()
    assert concept_index.exists()
    assert len(removed) == 3


# ---------------------------------------------------------------------------
# Test 8: Chicago citation builder
# ---------------------------------------------------------------------------

def test_chicago_citation_journal_article():
    """Journal article: Chicago format with journal, volume, issue, pages, DOI."""
    meta = {
        "material_id": "mat_x",
        "title": "Archive as Habitat",
        "authors": ["Rachel Lee"],
        "year": "2020",
        "raw_document_type": "paper",
        "bibliography": {
            "journal_name": "Journal of Architectural History",
            "volume": "79",
            "issue": "2",
            "start_page": "45",
            "end_page": "67",
            "doi": "10.1234/jah.2020.79.2",
        },
    }
    citation = _chicago_citation(meta)
    assert "Lee, Rachel." in citation
    assert "_Journal of Architectural History_" in citation
    assert "79, no. 2" in citation
    assert "45–67" in citation
    assert "10.1234/jah.2020.79.2" in citation


def test_chicago_citation_book_chapter():
    """Book chapter: Chicago format with book title, editors, publisher."""
    meta = {
        "material_id": "mat_x",
        "title": "Colonial Modernism",
        "authors": ["Mbembe Achille"],
        "year": "2003",
        "raw_document_type": "paper",
        "bibliography": {
            "book_title": "Necropolitics and Space",
            "editors": ["John Smith"],
            "publisher": "Duke University Press",
            "place": "Durham",
            "start_page": "11",
            "end_page": "40",
        },
    }
    citation = _chicago_citation(meta)
    assert '"Colonial Modernism."' in citation
    assert "_Necropolitics and Space_" in citation
    assert "edited by John Smith" in citation
    assert "Durham: Duke University Press" in citation


def test_chicago_citation_monograph():
    """Monograph: Chicago format with publisher and place."""
    meta = {
        "material_id": "mat_x",
        "title": "Space and Power",
        "authors": ["Henri Lefebvre"],
        "year": "1991",
        "raw_document_type": "monograph",
        "bibliography": {
            "publisher": "Blackwell",
            "place": "Oxford",
        },
    }
    citation = _chicago_citation(meta)
    assert "Lefebvre, Henri." in citation
    assert "_Space and Power_" in citation
    assert "Oxford: Blackwell" in citation
    assert "1991" in citation


def test_chicago_citation_fallback():
    """No bibliography field → fallback author/title/year."""
    meta = {
        "material_id": "mat_x",
        "title": "Unnamed Paper",
        "authors": ["Alice"],
        "year": "2022",
        "raw_document_type": "paper",
    }
    citation = _chicago_citation(meta)
    assert "Alice." in citation
    assert "Unnamed Paper" in citation
    assert "2022" in citation


# ---------------------------------------------------------------------------
# Integration: compile_wiki publishes memory bridge
# ---------------------------------------------------------------------------

def test_compile_populates_memory_bridge(tmp_path, monkeypatch):
    """compile_wiki() must populate the memory bridge tables in search.sqlite."""
    import shutil
    import arquimedes.compile as compile_mod
    import arquimedes.config as config_mod
    import arquimedes.cluster as cluster_mod
    import arquimedes.memory as memory_mod
    import arquimedes.index as index_mod
    import arquimedes.memory as memory_mod
    from arquimedes.index import rebuild_index

    # Set up a real index (with all tables)
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "config.yaml").write_text("library_root: ~/dummy\n")
    (tmp_path / "indexes").mkdir()
    (tmp_path / "extracted").mkdir()
    manifests = tmp_path / "manifests"
    manifests.mkdir()
    mid = "mat_mmm"
    mid2 = "mat_nnn"
    for material_id, title in [(mid, "Bridge Test Material"), (mid2, "Bridge Test Material Two")]:
        mat_dir = tmp_path / "extracted" / material_id
        meta = _make_meta(material_id, title)
        mat_dir.mkdir(parents=True)
        (mat_dir / "meta.json").write_text(json.dumps(meta))
        (mat_dir / "chunks.jsonl").write_text("")
        (mat_dir / "annotations.jsonl").write_text("")
    (manifests / "materials.jsonl").write_text(
        "\n".join([
            json.dumps({"material_id": mid, "file_hash": mid,
                        "relative_path": f"Research/{mid}.pdf", "file_type": "pdf",
                        "domain": "research", "collection": "_general",
                        "ingested_at": "2026-01-01T00:00:00+00:00"}),
            json.dumps({"material_id": mid2, "file_hash": mid2,
                        "relative_path": f"Research/{mid2}.pdf", "file_type": "pdf",
                        "domain": "research", "collection": "_general",
                        "ingested_at": "2026-01-02T00:00:00+00:00"}),
        ])
    )
    monkeypatch.chdir(tmp_path)
    rebuild_index()

    # Write cluster file
    derived = tmp_path / "derived"
    derived.mkdir(exist_ok=True)
    cluster = {
        "cluster_id": "c_001",
        "canonical_name": "Archive Space",
        "slug": "archive-space",
        "aliases": ["archival space", "archive as space"],
        "confidence": 0.9,
        "material_ids": [mid, mid2],
        "source_concepts": [{
            "material_id": mid,
            "concept_name": "archive space",
            "relevance": "high",
            "source_pages": [1, 2],
            "evidence_spans": ["the archive as built form"],
            "confidence": 0.88,
        }],
    }
    (derived / "global_bridge_clusters.jsonl").write_text(
        json.dumps({
            "bridge_id": "b_001",
            "canonical_name": "Archive Space Framework",
            "slug": "archive-space-framework",
            "aliases": ["archival space framework"],
            "wiki_path": "wiki/shared/bridge-concepts/archive-space-framework.md",
            "confidence": 0.88,
            "supporting_material_ids": [mid, mid2],
            "bridge_takeaways": ["It links built form and archive practice."],
            "bridge_tensions": ["Storage vs. inhabitation"],
            "bridge_open_questions": ["What counts as an archive room?"],
            "helpful_new_sources": ["Adaptive reuse archive case studies with plan drawings."],
            "why_this_bridge_matters": "It anchors the corpus.",
        }) + "\n",
        encoding="utf-8",
    )
    (derived / "lint").mkdir(parents=True, exist_ok=True)
    (derived / "lint" / "concept_reflections.jsonl").write_text(
        json.dumps({
            "cluster_id": "b_001",
            "slug": "archive-space-framework",
            "canonical_name": "Archive Space Framework",
            "main_takeaways": ["It links built form and archive practice."],
            "main_tensions": ["Storage vs. inhabitation"],
            "open_questions": ["What counts as an archive room?"],
            "helpful_new_sources": ["Adaptive reuse archive case studies with plan drawings."],
            "why_this_concept_matters": "It anchors the corpus.",
            "input_fingerprint": "fp-concept",
        }) + "\n",
        encoding="utf-8",
    )
    (derived / "lint" / "collection_reflections.jsonl").write_text(
        json.dumps({
            "collection_key": "research/papers",
            "domain": "research",
            "collection": "papers",
            "main_takeaways": ["The corpus is centrally about archival space."],
            "main_tensions": ["Theory vs use"],
            "important_material_ids": [mid, mid2],
            "important_cluster_ids": ["b_001"],
            "open_questions": ["What other archives are missing?"],
            "input_fingerprint": "fp-collection",
        }) + "\n",
        encoding="utf-8",
    )
    (derived / "lint" / "cluster_reviews.jsonl").write_text(
        json.dumps({
            "review_id": "b_001",
            "cluster_id": "b_001",
            "finding_type": "scope_extension",
            "severity": "low",
            "status": "validated",
            "note": "Added one more cross-material source concept.",
            "recommendation": "Keep the revised bridge.",
            "wiki_path": "wiki/shared/bridge-concepts/archive-space-framework.md",
            "_provenance": {"run_at": "2026-04-08T17:35:47.613231+00:00"},
        }) + "\n",
        encoding="utf-8",
    )

    bridge_cluster = {
        "cluster_id": "b_001",
        "canonical_name": "Archive Space Framework",
        "slug": "archive-space-framework",
        "aliases": ["archival space framework"],
        "wiki_path": "wiki/shared/bridge-concepts/archive-space-framework.md",
        "confidence": 0.88,
        "material_ids": [mid, mid2],
        "source_concepts": [{
            "material_id": mid,
            "concept_name": "archive space",
            "relevance": "high",
            "source_pages": [1, 2],
            "evidence_spans": ["the archive as built form"],
            "confidence": 0.88,
        }, {
            "material_id": mid2,
            "concept_name": "archive space framework",
            "relevance": "high",
            "source_pages": [1, 2],
            "evidence_spans": ["the archive as built form"],
            "confidence": 0.88,
        }],
    }

    monkeypatch.setattr(config_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(config_mod, "load_config", lambda: {"llm": {"agent_cmd": "echo"}})
    monkeypatch.setattr(compile_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(index_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(memory_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(
        cluster_mod,
        "cluster_concepts",
        lambda config, llm_fn=None, force=False, domain=None, collection=None: {
            "bridge_concepts": 1, "clusters": 1, "multi_material": 1, "skipped": True
        },
    )
    compile_mod.compile_wiki({"llm": {"agent_cmd": "echo"}}, force=True)

    meta_after = json.loads((tmp_path / "extracted" / mid / "meta.json").read_text())
    assert meta_after.get("bridge_concepts"), "bridge concepts not written back to extracted meta"
    assert meta_after["bridge_concepts"][0]["canonical_name"] == "Archive Space Framework"

    concept_page = (tmp_path / "wiki" / "shared" / "bridge-concepts" / "archive-space-framework.md").read_text()
    assert "## Cross-Collection Synthesis" in concept_page
    assert "It anchors the corpus." in concept_page
    assert "Adaptive reuse archive case studies with plan drawings." in concept_page
    assert "## Recent Changes" in concept_page
    assert "Added one more cross-material source concept." in concept_page
    assert concept_page.index("## Cross-Collection Synthesis") < concept_page.index("## Recent Changes")

    collection_page = (tmp_path / "wiki" / "research" / "papers" / "_index.md").read_text()
    assert "## Reflections" in collection_page
    assert "The corpus is centrally about archival space." in collection_page
    assert collection_page.index("## Overview") < collection_page.index("## Reflections")
    assert collection_page.index("## Reflections") < collection_page.index("## Recent Additions")

    # Memory bridge tables must be populated in search.sqlite
    con = sqlite3.connect(str(tmp_path / "indexes" / "search.sqlite"))
    concept_pages = con.execute(
        "SELECT path FROM wiki_pages WHERE page_type='concept'"
    ).fetchall()
    concept_index = con.execute(
        "SELECT path FROM wiki_pages WHERE page_type='concept_index'"
    ).fetchall()
    material_pages = con.execute(
        "SELECT path FROM wiki_pages WHERE page_type='material'"
    ).fetchall()
    con.close()

    assert any("shared/concepts/_index.md" in r[0] for r in concept_index), "local concept index not in wiki_pages"
    assert any("bridge-concepts/archive-space-framework" in r[0] for r in concept_pages), "bridge concept page not in wiki_pages"
    assert any(mid in r[0] for r in material_pages), "material page not in wiki_pages"


def test_compile_prefers_global_bridge_pages_when_present(tmp_path, monkeypatch):
    import arquimedes.cluster as cluster_mod
    import arquimedes.compile as compile_mod
    import arquimedes.config as config_mod
    import arquimedes.index as index_mod
    import arquimedes.memory as memory_mod

    mid = "mat_001"
    mid2 = "mat_002"
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "config.yaml").write_text("library_root: ~/dummy\n", encoding="utf-8")
    (tmp_path / "indexes").mkdir()
    (tmp_path / "manifests").mkdir()
    (tmp_path / "extracted").mkdir()

    for material_id in (mid, mid2):
        mat_dir = tmp_path / "extracted" / material_id
        mat_dir.mkdir(parents=True)
        (mat_dir / "meta.json").write_text(json.dumps({
            "material_id": material_id,
            "file_hash": material_id,
            "source_path": f"Research/{material_id}.pdf",
            "title": f"Document {material_id}",
            "authors": ["Author One"],
            "year": "2026",
            "page_count": 1,
            "file_type": "pdf",
            "domain": "research",
            "collection": "papers",
            "summary": {"value": "Summary", "provenance": {}},
            "keywords": {"value": ["archive"], "provenance": {}},
            "document_type": {"value": "paper", "provenance": {}},
            "facets": {},
            "_enrichment_stamp": {"prompt_version": "enrich-v1.0", "enrichment_schema_version": "1"},
        }), encoding="utf-8")
        (mat_dir / "chunks.jsonl").write_text("", encoding="utf-8")
        (mat_dir / "annotations.jsonl").write_text("", encoding="utf-8")
    (tmp_path / "manifests" / "materials.jsonl").write_text(
        "\n".join([
            json.dumps({"material_id": mid, "file_hash": mid, "relative_path": f"Research/{mid}.pdf", "file_type": "pdf", "domain": "research", "collection": "papers", "ingested_at": "2026-01-01T00:00:00+00:00"}),
            json.dumps({"material_id": mid2, "file_hash": mid2, "relative_path": f"Research/{mid2}.pdf", "file_type": "pdf", "domain": "research", "collection": "papers", "ingested_at": "2026-01-01T00:00:00+00:00"}),
        ]),
        encoding="utf-8",
    )

    monkeypatch.chdir(tmp_path)
    index_mod.rebuild_index()

    local_cluster = {
        "cluster_id": "research__papers__local_0001",
        "domain": "research",
        "collection": "papers",
        "canonical_name": "Archive Space Framework",
        "slug": "archive-space-framework",
        "aliases": ["archival space framework"],
        "confidence": 0.9,
        "wiki_path": "wiki/research/papers/concepts/archive-space-framework.md",
        "source_concepts": [
            {"material_id": mid, "concept_name": "archive space", "relevance": "high", "source_pages": [1], "evidence_spans": ["archive space"], "confidence": 0.88},
            {"material_id": mid2, "concept_name": "archive space framework", "relevance": "high", "source_pages": [1], "evidence_spans": ["archive space framework"], "confidence": 0.88},
        ],
        "material_ids": [mid, mid2],
    }
    bridge_cluster = {
        "bridge_id": "global_bridge__archive-space-framework",
        "canonical_name": "Archive Space Framework",
        "slug": "archive-space-framework",
        "aliases": ["cross-collection archive space"],
        "descriptor": "A bridge joining local archive-space clusters.",
        "wiki_path": "wiki/shared/bridge-concepts/archive-space-framework.md",
        "confidence": 0.88,
        "supporting_material_ids": [mid, mid2],
        "bridge_takeaways": ["Archive thinking recurs across collections."],
        "bridge_tensions": ["Theory and practice frame archival space differently."],
        "bridge_open_questions": ["Which other collections should join this bridge?"],
        "why_this_bridge_matters": "The bridge shows how archive becomes a shared architectural frame rather than a collection-specific metaphor.",
        "supporting_collection_reflections": [
            {
                "collection_key": "research/papers",
                "main_takeaways": ["Archive thinking recurs across papers."],
                "main_tensions": ["Theory vs use"],
                "open_questions": ["What other archives are missing?"],
                "why_this_collection_matters": "The papers collection treats archive as an analytical frame.",
            }
        ],
        "member_local_clusters": [{
            "cluster_id": "research__papers__local_0001",
            "domain": "research",
            "collection": "papers",
            "canonical_name": "Archive Space Framework",
            "descriptor": "A local concept home.",
            "material_ids": [mid, mid2],
            "wiki_path": "wiki/research/papers/concepts/archive-space-framework.md",
            "promotion_reasons": ["high_confidence", "cross_collection_bridgeability"],
        }],
    }
    (tmp_path / "derived" / "collections" / "research__papers").mkdir(parents=True, exist_ok=True)
    (tmp_path / "derived" / "collections" / "research__papers" / "local_concept_clusters.jsonl").write_text(json.dumps(local_cluster) + "\n", encoding="utf-8")
    (tmp_path / "derived").mkdir(exist_ok=True)
    (tmp_path / "derived" / "global_bridge_clusters.jsonl").write_text(json.dumps(bridge_cluster) + "\n", encoding="utf-8")

    monkeypatch.setattr(config_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(config_mod, "load_config", lambda: {"llm": {"agent_cmd": "echo"}})
    monkeypatch.setattr(compile_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(index_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(memory_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(cluster_mod, "load_local_clusters", lambda root=None: [local_cluster])
    monkeypatch.setattr(cluster_mod, "cluster_concepts", lambda config, llm_fn=None, force=False: {"clusters": 1, "skipped": True})

    compile_mod.compile_wiki({"llm": {"agent_cmd": "echo"}}, force=True)

    bridge_page = (tmp_path / "wiki" / "shared" / "bridge-concepts" / "archive-space-framework.md").read_text(encoding="utf-8")
    local_page = (tmp_path / "wiki" / "research" / "papers" / "concepts" / "archive-space-framework.md").read_text(encoding="utf-8")
    assert "## Cross-Collection Synthesis" in bridge_page
    assert "### Why This Bridge Matters" in bridge_page
    assert "Archive thinking recurs across collections." in bridge_page
    assert "Theory and practice frame archival space differently." in bridge_page
    assert "The papers collection treats archive as an analytical frame." in bridge_page
    assert "## Contributing Local Clusters" in bridge_page
    assert "A local concept home." in bridge_page
    assert "### Archive Space Framework (research/papers / 2 materials)" in bridge_page
    assert "- Local cluster: [Archive Space Framework](../../research/papers/concepts/archive-space-framework.md)" in bridge_page
    assert "Promotion: high confidence, cross collection bridgeability" in bridge_page
    assert "- [Document mat_001](../../research/papers/mat_001.md)" in bridge_page
    assert "- Materials:" not in bridge_page
    assert "## Global Bridges" in local_page
    assert "wiki/shared/bridge-concepts/archive-space-framework.md" not in local_page
    assert "A bridge joining local archive-space clusters." in local_page


def test_compile_does_not_fallback_to_legacy_bridge_pages_when_local_clusters_exist(tmp_path, monkeypatch):
    import arquimedes.cluster as cluster_mod
    import arquimedes.compile as compile_mod
    import arquimedes.config as config_mod
    import arquimedes.index as index_mod
    import arquimedes.memory as memory_mod

    mid = "mat_001"
    mid2 = "mat_002"
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "config.yaml").write_text("library_root: ~/dummy\n", encoding="utf-8")
    (tmp_path / "indexes").mkdir()
    (tmp_path / "manifests").mkdir()
    (tmp_path / "extracted").mkdir()

    for material_id in (mid, mid2):
        mat_dir = tmp_path / "extracted" / material_id
        mat_dir.mkdir(parents=True)
        (mat_dir / "meta.json").write_text(json.dumps({
            "material_id": material_id,
            "file_hash": material_id,
            "source_path": f"Research/{material_id}.pdf",
            "title": f"Document {material_id}",
            "authors": ["Author One"],
            "year": "2026",
            "page_count": 1,
            "file_type": "pdf",
            "domain": "research",
            "collection": "papers",
            "summary": {"value": "Summary", "provenance": {}},
            "keywords": {"value": ["archive"], "provenance": {}},
            "document_type": {"value": "paper", "provenance": {}},
            "facets": {},
            "_enrichment_stamp": {"prompt_version": "enrich-v1.0", "enrichment_schema_version": "1"},
        }), encoding="utf-8")
        (mat_dir / "chunks.jsonl").write_text("", encoding="utf-8")
        (mat_dir / "annotations.jsonl").write_text("", encoding="utf-8")
    (tmp_path / "manifests" / "materials.jsonl").write_text(
        "\n".join([
            json.dumps({"material_id": mid, "file_hash": mid, "relative_path": f"Research/{mid}.pdf", "file_type": "pdf", "domain": "research", "collection": "papers", "ingested_at": "2026-01-01T00:00:00+00:00"}),
            json.dumps({"material_id": mid2, "file_hash": mid2, "relative_path": f"Research/{mid2}.pdf", "file_type": "pdf", "domain": "research", "collection": "papers", "ingested_at": "2026-01-01T00:00:00+00:00"}),
        ]),
        encoding="utf-8",
    )

    monkeypatch.chdir(tmp_path)
    index_mod.rebuild_index()

    local_cluster = {
        "cluster_id": "research__papers__local_0001",
        "domain": "research",
        "collection": "papers",
        "canonical_name": "Archive Space Framework",
        "slug": "archive-space-framework",
        "aliases": [],
        "confidence": 0.9,
        "wiki_path": "wiki/research/papers/concepts/archive-space-framework.md",
        "source_concepts": [
            {"material_id": mid, "concept_name": "archive space", "relevance": "high", "source_pages": [1], "evidence_spans": ["archive space"], "confidence": 0.88},
            {"material_id": mid2, "concept_name": "archive space framework", "relevance": "high", "source_pages": [1], "evidence_spans": ["archive space framework"], "confidence": 0.88},
        ],
        "material_ids": [mid, mid2],
    }
    legacy_bridge = {
        "cluster_id": "legacy_bridge_001",
        "canonical_name": "Legacy Raw Bridge",
        "slug": "legacy-raw-bridge",
        "aliases": [],
        "wiki_path": "wiki/shared/bridge-concepts/legacy-raw-bridge.md",
        "confidence": 0.8,
        "material_ids": [mid, mid2],
        "source_concepts": [
            {"material_id": mid, "concept_name": "legacy raw bridge", "relevance": "high", "source_pages": [1], "evidence_spans": ["legacy"], "confidence": 0.8},
            {"material_id": mid2, "concept_name": "legacy raw bridge", "relevance": "high", "source_pages": [1], "evidence_spans": ["legacy"], "confidence": 0.8},
        ],
    }
    (tmp_path / "derived" / "collections" / "research__papers").mkdir(parents=True, exist_ok=True)
    (tmp_path / "derived" / "collections" / "research__papers" / "local_concept_clusters.jsonl").write_text(json.dumps(local_cluster) + "\n", encoding="utf-8")

    monkeypatch.setattr(config_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(config_mod, "load_config", lambda: {"llm": {"agent_cmd": "echo"}})
    monkeypatch.setattr(compile_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(index_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(memory_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(cluster_mod, "load_local_clusters", lambda root=None: [local_cluster])
    monkeypatch.setattr(cluster_mod, "cluster_concepts", lambda config, llm_fn=None, force=False: {"clusters": 1, "skipped": True})

    compile_mod.compile_wiki({"llm": {"agent_cmd": "echo"}}, force=True)

    assert not (tmp_path / "wiki" / "shared" / "bridge-concepts" / "legacy-raw-bridge.md").exists()


# ---------------------------------------------------------------------------
# Test: Collection pages
# ---------------------------------------------------------------------------

from arquimedes.compile_pages import render_collection_page


def test_collection_page_renders_all_sections():
    """render_collection_page produces overview, materials, concepts, facets, recent."""
    materials = [
        {"name": "Paper A", "path": "paper_a.md", "summary": "About climate."},
        {"name": "Paper B", "path": "paper_b.md", "summary": "About mass."},
    ]
    key_concepts = [
        {"name": "Thermal Mass", "path": "../shared/concepts/thermal-mass.md", "count": 2},
    ]
    top_facets = [
        {"field": "climate", "value": "mediterranean", "count": 2},
    ]
    recent = [
        {"name": "Paper A", "path": "paper_a.md", "ingested_at": "2026-04-05T10:00:00+00:00"},
        {"name": "Paper B", "path": "paper_b.md", "ingested_at": "2026-04-04T10:00:00+00:00"},
    ]
    page = render_collection_page(
        "Research / Thermal Mass", "research", "thermal-mass",
        materials, key_concepts, top_facets, recent,
        {
            "main_takeaways": ["Thermal mass is central."],
            "main_tensions": [],
            "open_questions": [],
            "helpful_new_sources": ["Comparative thermal-mass retrofit case studies."],
            "why_this_collection_matters": "It anchors the collection.",
        },
    )
    assert "# Research / Thermal Mass" in page
    assert "## Overview" in page
    assert "## Reflections" in page
    assert "**Materials:** 2" in page
    assert "## Recent Additions" in page
    assert "Paper A" in page
    assert "2026-04-05" in page
    assert "## Materials" in page
    assert "## Key Concepts" in page
    assert "Thermal Mass" in page
    assert "2 materials" in page
    assert "## Top Facets" in page
    assert "Climate" in page
    assert "mediterranean" in page
    assert "Helpful new sources" in page
    assert "Comparative thermal-mass retrofit case studies." in page
    assert page.index("## Overview") < page.index("## Reflections")
    assert page.index("## Reflections") < page.index("## Recent Additions")


def test_collection_page_empty_sections():
    """Empty concepts/facets/recent omit those sections."""
    materials = [{"name": "X", "path": "x.md", "summary": ""}]
    page = render_collection_page("P / G", "p", "g", materials, [], [], [])
    assert "## Materials" in page
    assert "## Key Concepts" not in page
    assert "## Top Facets" not in page
    assert "## Recent Additions" not in page


def test_compile_writes_collection_pages(tmp_path, monkeypatch):
    """arq compile writes collection _index.md with collection page content."""
    import arquimedes.compile as compile_mod
    import arquimedes.config as config_mod
    import arquimedes.cluster as cluster_mod
    from arquimedes.index import rebuild_index

    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "config.yaml").write_text("library_root: ~/dummy\n")
    (tmp_path / "indexes").mkdir()
    (tmp_path / "extracted").mkdir()
    manifests = tmp_path / "manifests"
    manifests.mkdir()

    mid = "mat_coll"
    meta = _make_meta(mid, "Collection Test Material")
    mat_dir = tmp_path / "extracted" / mid
    mat_dir.mkdir(parents=True)
    (mat_dir / "meta.json").write_text(json.dumps(meta))
    (mat_dir / "chunks.jsonl").write_text("")
    (mat_dir / "annotations.jsonl").write_text("")
    (manifests / "materials.jsonl").write_text(
        json.dumps({
            "material_id": mid, "file_hash": mid,
            "relative_path": f"Research/{mid}.pdf", "file_type": "pdf",
            "domain": "research", "collection": "papers",
            "ingested_at": "2026-04-05T12:00:00+00:00",
        })
    )
    monkeypatch.chdir(tmp_path)
    rebuild_index()

    derived = tmp_path / "derived"
    derived.mkdir(exist_ok=True)
    cluster = {
        "cluster_id": "c_coll", "canonical_name": "Test Concept",
        "slug": "test-concept", "aliases": [], "confidence": 0.9,
        "material_ids": [mid],
        "source_concepts": [{
            "material_id": mid, "concept_name": "test concept",
            "relevance": "high", "source_pages": [1],
            "evidence_spans": [], "confidence": 0.9,
        }],
    }
    (derived / "bridge_concept_clusters.jsonl").write_text(json.dumps(cluster) + "\n")
    (derived / "lint").mkdir(parents=True, exist_ok=True)
    (derived / "lint" / "collection_reflections.jsonl").write_text(
        json.dumps({
            "collection_key": "research/papers",
            "domain": "research",
            "collection": "papers",
            "main_takeaways": ["The collection is centered on test concepts."],
            "main_tensions": ["Theory vs application"],
            "important_material_ids": [mid],
            "important_cluster_ids": ["c_coll"],
            "open_questions": ["What should be compared next?"],
            "helpful_new_sources": ["Comparable project documentation."],
            "why_this_collection_matters": "It anchors the test collection.",
            "input_fingerprint": "fp-collection",
            "wiki_path": "wiki/research/papers/_index.md",
        }) + "\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(config_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(config_mod, "load_config", lambda: {"llm": {"agent_cmd": "echo"}})
    monkeypatch.setattr(compile_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(
        cluster_mod,
        "cluster_concepts",
        lambda config, llm_fn=None, force=False, domain=None, collection=None: {"skipped": True},
    )
    compile_mod.compile_wiki({"llm": {"agent_cmd": "echo"}}, force=True)

    coll_page = (tmp_path / "wiki" / "research" / "papers" / "_index.md").read_text()
    assert "## Overview" in coll_page
    assert "## Reflections" in coll_page
    assert "## Materials" in coll_page
    assert "Collection Test Material" in coll_page
    assert "Comparable project documentation." in coll_page
    assert "## Recent Additions" in coll_page
    assert "2026-04-05" in coll_page


def test_compile_groups_local_concepts_by_collection(tmp_path, monkeypatch):
    """Local concepts index groups entries deterministically by collection."""
    import shutil
    import arquimedes.compile as compile_mod
    import arquimedes.config as config_mod
    import arquimedes.cluster as cluster_mod
    import arquimedes.memory as memory_mod

    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "config.yaml").write_text("library_root: ~/dummy\n")
    (tmp_path / "indexes").mkdir()
    (tmp_path / "extracted").mkdir()
    (tmp_path / "manifests").mkdir()
    (tmp_path / "manifests" / "materials.jsonl").write_text(
        json.dumps({
            "material_id": "mat_aaa", "file_hash": "x",
            "relative_path": "Research/mat_aaa.pdf", "file_type": "pdf",
            "domain": "research", "collection": "archives",
            "ingested_at": "2026-04-05T12:00:00+00:00",
        })
    )

    meta = _make_meta("mat_aaa", "Archive Material")
    meta["collection"] = "archives"
    mat_dir = tmp_path / "extracted" / "mat_aaa"
    mat_dir.mkdir(parents=True)
    (mat_dir / "meta.json").write_text(json.dumps(meta))
    (mat_dir / "chunks.jsonl").write_text("")
    (mat_dir / "annotations.jsonl").write_text("")

    db_path = _make_sqlite_db(tmp_path)
    index_dir = tmp_path / "indexes"
    shutil.copy(str(db_path), str(index_dir / "search.sqlite"))

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(config_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(config_mod, "load_config", lambda: {"llm": {"agent_cmd": "echo"}})
    monkeypatch.setattr(compile_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(memory_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(cluster_mod, "cluster_concepts", lambda config, llm_fn=None, force=False, domain=None, collection=None: {"skipped": True})
    compile_mod.compile_wiki({"llm": {"agent_cmd": "echo"}}, force=True)

    local_index = (tmp_path / "wiki" / "shared" / "concepts" / "_index.md").read_text()
    assert "## Archives" in local_index
    assert "archival habitat" in local_index


def test_compile_uses_collection_local_cluster_pages(tmp_path, monkeypatch):
    import arquimedes.compile as compile_mod
    import arquimedes.config as config_mod
    import arquimedes.cluster as cluster_mod
    from arquimedes.index import rebuild_index

    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "config.yaml").write_text("library_root: ~/dummy\n")
    (tmp_path / "indexes").mkdir()
    (tmp_path / "extracted").mkdir()
    manifests = tmp_path / "manifests"
    manifests.mkdir()

    mid1 = "mat_one"
    mid2 = "mat_two"
    for mid, title in ((mid1, "One"), (mid2, "Two")):
        meta = _make_meta(mid, title)
        mat_dir = tmp_path / "extracted" / mid
        mat_dir.mkdir(parents=True)
        (mat_dir / "meta.json").write_text(json.dumps(meta))
        (mat_dir / "chunks.jsonl").write_text("")
        (mat_dir / "annotations.jsonl").write_text("")
    (manifests / "materials.jsonl").write_text(
        "\n".join([
            json.dumps({"material_id": mid1, "file_hash": mid1, "relative_path": f"Research/{mid1}.pdf", "file_type": "pdf", "domain": "research", "collection": "papers", "ingested_at": "2026-04-05T12:00:00+00:00"}),
            json.dumps({"material_id": mid2, "file_hash": mid2, "relative_path": f"Research/{mid2}.pdf", "file_type": "pdf", "domain": "research", "collection": "papers", "ingested_at": "2026-04-05T12:00:00+00:00"}),
        ]),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    rebuild_index()

    cluster = {
        "cluster_id": "research__papers__local_0001",
        "domain": "research",
        "collection": "papers",
        "canonical_name": "Archive Space Framework",
        "slug": "archive-space-framework",
        "aliases": [],
        "descriptor": "Shared archival spatial idea.",
        "confidence": 0.9,
        "material_ids": [mid1, mid2],
        "source_concepts": [
            {"material_id": mid1, "concept_name": "archive space", "relevance": "high", "source_pages": [1], "evidence_spans": [], "confidence": 0.9},
            {"material_id": mid2, "concept_name": "archive framework", "relevance": "high", "source_pages": [2], "evidence_spans": [], "confidence": 0.9},
        ],
        "wiki_path": "wiki/research/papers/concepts/archive-space-framework.md",
    }
    local_cluster_path(tmp_path, "research", "papers").parent.mkdir(parents=True, exist_ok=True)
    local_cluster_path(tmp_path, "research", "papers").write_text(json.dumps(cluster) + "\n", encoding="utf-8")

    monkeypatch.setattr(config_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(config_mod, "load_config", lambda: {"llm": {"agent_cmd": "echo"}})
    monkeypatch.setattr(compile_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(cluster_mod, "cluster_concepts", lambda config, llm_fn=None, force=False, domain=None, collection=None: {"skipped": True, "collections": 1, "total_concepts": 0, "clusters": 1, "multi_material": 1})

    compile_mod.compile_wiki({"llm": {"agent_cmd": "echo"}}, force=True)

    concept_page = (tmp_path / "wiki" / "research" / "papers" / "concepts" / "archive-space-framework.md").read_text()
    collection_page = (tmp_path / "wiki" / "research" / "papers" / "_index.md").read_text()
    material_page = (tmp_path / "wiki" / "research" / "papers" / f"{mid1}.md").read_text()
    concept_index = (tmp_path / "wiki" / "research" / "papers" / "concepts" / "_index.md").read_text()
    assert "# Archive Space Framework" in concept_page
    assert "concepts/archive-space-framework.md" in collection_page
    assert "concepts/archive-space-framework.md" in material_page
    assert "Archive Space Framework" in concept_index


def test_compile_renders_single_material_local_concept_home(tmp_path, monkeypatch):
    import arquimedes.compile as compile_mod
    import arquimedes.config as config_mod
    import arquimedes.cluster as cluster_mod
    from arquimedes.index import rebuild_index

    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "config.yaml").write_text("library_root: ~/dummy\n")
    (tmp_path / "indexes").mkdir()
    (tmp_path / "extracted").mkdir()
    manifests = tmp_path / "manifests"
    manifests.mkdir()

    mid = "mat_single"
    meta = _make_meta(mid, "Single")
    mat_dir = tmp_path / "extracted" / mid
    mat_dir.mkdir(parents=True)
    (mat_dir / "meta.json").write_text(json.dumps(meta))
    (mat_dir / "chunks.jsonl").write_text("")
    (mat_dir / "annotations.jsonl").write_text("")
    (manifests / "materials.jsonl").write_text(
        json.dumps({"material_id": mid, "file_hash": mid, "relative_path": f"Research/{mid}.pdf", "file_type": "pdf", "domain": "research", "collection": "papers", "ingested_at": "2026-04-05T12:00:00+00:00"}),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    rebuild_index()

    cluster = {
        "cluster_id": "research__papers__local_0001",
        "domain": "research",
        "collection": "papers",
        "canonical_name": "Single Concept Home",
        "slug": "single-concept-home",
        "aliases": [],
        "descriptor": "Single-material concept home.",
        "confidence": 1.0,
        "material_ids": [mid],
        "source_concepts": [
            {"material_id": mid, "concept_name": "single concept", "relevance": "high", "source_pages": [1], "evidence_spans": [], "confidence": 1.0},
        ],
        "wiki_path": "wiki/research/papers/concepts/single-concept-home.md",
    }
    local_cluster_path(tmp_path, "research", "papers").parent.mkdir(parents=True, exist_ok=True)
    local_cluster_path(tmp_path, "research", "papers").write_text(json.dumps(cluster) + "\n", encoding="utf-8")

    monkeypatch.setattr(config_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(config_mod, "load_config", lambda: {"llm": {"agent_cmd": "echo"}})
    monkeypatch.setattr(compile_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(cluster_mod, "cluster_concepts", lambda config, llm_fn=None, force=False, domain=None, collection=None: {"skipped": True, "collections": 1, "total_concepts": 0, "clusters": 1, "multi_material": 0})

    compile_mod.compile_wiki({"llm": {"agent_cmd": "echo"}}, force=True)

    assert (tmp_path / "wiki" / "research" / "papers" / "concepts" / "single-concept-home.md").exists()
    assert "concepts/single-concept-home.md" in (tmp_path / "wiki" / "research" / "papers" / f"{mid}.md").read_text()


def test_compile_runs_quick_lint_after_compile(tmp_path, monkeypatch):
    """compile_wiki() should trigger a quick lint pass after publishing."""
    import arquimedes.compile as compile_mod
    import arquimedes.config as config_mod
    import arquimedes.cluster as cluster_mod
    from arquimedes.index import rebuild_index

    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "config.yaml").write_text("library_root: ~/dummy\n")
    (tmp_path / "indexes").mkdir()
    (tmp_path / "extracted").mkdir()
    manifests = tmp_path / "manifests"
    manifests.mkdir()

    mid = "mat_quick"
    meta = _make_meta(mid, "Quick Lint Material")
    mat_dir = tmp_path / "extracted" / mid
    mat_dir.mkdir(parents=True)
    (mat_dir / "meta.json").write_text(json.dumps(meta))
    (mat_dir / "chunks.jsonl").write_text("")
    (mat_dir / "annotations.jsonl").write_text("")
    (manifests / "materials.jsonl").write_text(
        json.dumps({
            "material_id": mid, "file_hash": mid,
            "relative_path": f"Research/{mid}.pdf", "file_type": "pdf",
            "domain": "research", "collection": "_general",
            "ingested_at": "2026-01-01T00:00:00+00:00",
        })
    )
    monkeypatch.chdir(tmp_path)
    rebuild_index()

    derived = tmp_path / "derived"
    derived.mkdir(exist_ok=True)
    cluster = {
        "cluster_id": "c_quick", "canonical_name": "Quick Concept",
        "slug": "quick-concept", "aliases": [], "confidence": 0.9,
        "material_ids": [mid],
        "source_concepts": [{
            "material_id": mid, "concept_name": "quick concept",
            "relevance": "high", "source_pages": [1],
            "evidence_spans": [], "confidence": 0.9,
        }],
    }
    (derived / "bridge_concept_clusters.jsonl").write_text(json.dumps(cluster) + "\n")

    lint_calls: list[dict] = []

    def fake_run_lint(config, *, quick=False, full=False, report=False, fix=False, scheduled=False, llm_factory=None):
        lint_calls.append({
            "quick": quick,
            "full": full,
            "report": report,
            "fix": fix,
            "scheduled": scheduled,
        })
        return {
            "mode": "quick" if quick else "full",
            "deterministic": {"summary": {"issues": 0, "high": 0, "medium": 0, "low": 0}},
            "reflection": None,
            "fixes": None,
            "report_path": str(tmp_path / "wiki" / "_lint_report.md"),
        }

    monkeypatch.setattr(config_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(config_mod, "load_config", lambda: {"llm": {"agent_cmd": "echo"}})
    monkeypatch.setattr(compile_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(cluster_mod, "cluster_concepts", lambda config, llm_fn=None, force=False, domain=None, collection=None: {"skipped": True})
    monkeypatch.setattr("arquimedes.lint.run_lint", fake_run_lint)

    result = compile_mod.compile_wiki({"llm": {"agent_cmd": "echo"}}, force=True)

    assert lint_calls and lint_calls[0]["quick"] is True
    assert result["quick_lint"]["mode"] == "quick"


def test_compile_recompile_pages_rerenders_without_reclustering(tmp_path, monkeypatch):
    """recompile_pages should rerender pages from existing clusters without forcing clustering."""
    import arquimedes.compile as compile_mod
    import arquimedes.config as config_mod
    import arquimedes.cluster as cluster_mod
    import arquimedes.memory as memory_mod
    from arquimedes.index import rebuild_index

    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "config.yaml").write_text("library_root: ~/dummy\n")
    (tmp_path / "indexes").mkdir()
    (tmp_path / "extracted").mkdir()
    manifests = tmp_path / "manifests"
    manifests.mkdir()

    mid = "mat_recompile"
    mid2 = "mat_recompile_two"
    meta = _make_meta(mid, "Recompile Material")
    meta2 = _make_meta(mid2, "Recompile Material Two")
    mat_dir = tmp_path / "extracted" / mid
    mat_dir.mkdir(parents=True)
    (mat_dir / "meta.json").write_text(json.dumps(meta))
    (mat_dir / "chunks.jsonl").write_text("")
    (mat_dir / "annotations.jsonl").write_text("")
    mat_dir2 = tmp_path / "extracted" / mid2
    mat_dir2.mkdir(parents=True)
    (mat_dir2 / "meta.json").write_text(json.dumps(meta2))
    (mat_dir2 / "chunks.jsonl").write_text("")
    (mat_dir2 / "annotations.jsonl").write_text("")
    (manifests / "materials.jsonl").write_text(
        "\n".join([
            json.dumps({
                "material_id": mid,
                "file_hash": mid,
                "relative_path": f"Research/{mid}.pdf",
                "file_type": "pdf",
                "domain": "research",
                "collection": "papers",
                "ingested_at": "2026-01-01T00:00:00+00:00",
            }),
            json.dumps({
                "material_id": mid2,
                "file_hash": mid2,
                "relative_path": f"Research/{mid2}.pdf",
                "file_type": "pdf",
                "domain": "research",
                "collection": "papers",
                "ingested_at": "2026-01-02T00:00:00+00:00",
            }),
        ])
    )
    monkeypatch.chdir(tmp_path)
    rebuild_index()

    derived = tmp_path / "derived"
    derived.mkdir(exist_ok=True)
    cluster = {
        "bridge_id": "b_recompile",
        "canonical_name": "Recompile Concept",
        "slug": "recompile-concept",
        "aliases": [],
        "wiki_path": "wiki/shared/bridge-concepts/recompile-concept.md",
        "confidence": 1.0,
        "supporting_material_ids": [mid, mid2],
        "bridge_takeaways": ["Reflection text from artifacts."],
        "bridge_tensions": [],
        "bridge_open_questions": [],
        "why_this_bridge_matters": "It matters.",
    }
    (derived / "global_bridge_clusters.jsonl").write_text(json.dumps(cluster) + "\n")
    (derived / "lint").mkdir(parents=True, exist_ok=True)
    (derived / "lint" / "concept_reflections.jsonl").write_text(
        json.dumps({
            "cluster_id": "b_recompile",
            "slug": "recompile-concept",
            "canonical_name": "Recompile Concept",
            "main_takeaways": ["Reflection text from artifacts."],
            "main_tensions": [],
            "open_questions": [],
            "why_this_concept_matters": "It matters.",
            "input_fingerprint": "fp-recompile",
        }) + "\n",
        encoding="utf-8",
    )

    (tmp_path / "wiki" / "shared" / "bridge-concepts").mkdir(parents=True, exist_ok=True)
    (tmp_path / "wiki" / "shared" / "bridge-concepts" / "recompile-concept.md").write_text("OLD\n", encoding="utf-8")

    calls: list[bool] = []

    monkeypatch.setattr(config_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(config_mod, "load_config", lambda: {"llm": {"agent_cmd": "echo"}})
    monkeypatch.setattr(compile_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(memory_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(
        cluster_mod,
        "cluster_concepts",
        lambda config, llm_fn=None, force=False, domain=None, collection=None: calls.append(force) or {"skipped": True},
    )
    summary = compile_mod.compile_wiki({"llm": {"agent_cmd": "echo"}}, recompile_pages=True)

    assert calls == []
    assert summary["concept_pages"] == 1
    page = (tmp_path / "wiki" / "shared" / "bridge-concepts" / "recompile-concept.md").read_text()
    assert "Reflection text from artifacts." in page


def test_collection_page_key_concepts_ranked():
    """Key concepts rendered in the order given (pre-sorted by compile.py)."""
    key_concepts = [
        {"name": "Alpha", "path": "a.md", "count": 3},
        {"name": "Beta", "path": "b.md", "count": 3},
        {"name": "Zebra", "path": "z.md", "count": 1},
    ]
    page = render_collection_page("T", "d", "c", [], key_concepts, [], [])
    assert page.index("Alpha") < page.index("Beta")
    assert page.index("Alpha") < page.index("Zebra")


def test_collection_page_general_collection():
    """_general collection also renders correctly."""
    materials = [{"name": "X", "path": "x.md", "summary": "test"}]
    page = render_collection_page(
        "Research /  General", "research", "_general",
        materials, [], [], [],
    )
    assert "## Overview" in page
    assert "_general" in page
    assert "## Materials" in page
