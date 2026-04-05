"""Tests for Phase 5 — concept clustering and wiki compilation."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from arquimedes.cluster import (
    _normalize_concept_name,
    _validate_and_attach_provenance,
    cluster_fingerprint,
    is_clustering_stale,
    load_clusters,
    slugify,
)
from arquimedes.compile_pages import (
    _concept_wiki_path,
    _material_wiki_path,
    _relative_link,
    render_concept_page,
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
        concept_name, concept_key, material_id, relevance, source_pages, evidence_spans, confidence = row
        idx[(material_id, concept_key)] = {
            "concept_name": concept_name,
            "concept_key": concept_key,
            "material_id": material_id,
            "relevance": relevance,
            "source_pages": source_pages,
            "evidence_spans": evidence_spans,
            "confidence": confidence,
        }
    return idx


def _make_sqlite_db(tmp_path: Path) -> Path:
    """Create a minimal search.sqlite with concepts + materials tables."""
    db_path = tmp_path / "search.sqlite"
    con = sqlite3.connect(str(db_path))
    con.execute(
        "CREATE TABLE concepts ("
        "concept_name TEXT, concept_key TEXT, material_id TEXT,"
        "relevance TEXT, source_pages TEXT, evidence_spans TEXT, confidence REAL,"
        "PRIMARY KEY (material_id, concept_key))"
    )
    con.executemany("INSERT INTO concepts VALUES (?,?,?,?,?,?,?)", CONCEPT_ROWS)
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


# ---------------------------------------------------------------------------
# Test 1: cluster parse and write
# ---------------------------------------------------------------------------

def test_cluster_parse_and_write(tmp_path, monkeypatch):
    """Mock LLM returns valid JSON → concept_clusters.jsonl written with correct fields."""
    import arquimedes.cluster as cluster_mod
    import arquimedes.config as config_mod

    db_path = _make_sqlite_db(tmp_path)
    derived_dir = tmp_path / "derived"
    derived_dir.mkdir()

    monkeypatch.setattr(config_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(config_mod, "load_config", lambda: {"llm": {"agent_cmd": "echo"}})
    monkeypatch.setattr(cluster_mod, "get_project_root", lambda: tmp_path)

    # Copy db to the expected index location
    index_dir = tmp_path / "indexes"
    index_dir.mkdir()
    import shutil
    shutil.copy(str(db_path), str(index_dir / "search.sqlite"))

    def mock_llm(system, messages):
        return json.dumps(RAW_LLM_CLUSTERS)

    config = {"llm": {"agent_cmd": "echo"}}
    clusters = cluster_mod.cluster_concepts(config, llm_fn=mock_llm, force=True)

    assert clusters["clusters"] == 2
    assert clusters["total_concepts"] == 3
    assert clusters["multi_material"] == 1  # only first cluster has 2 materials

    written = load_clusters(tmp_path)
    assert len(written) == 2

    c0 = written[0]
    assert c0["canonical_name"] == "archive as architectural space"
    assert c0["slug"] == "archive-as-architectural-space"
    assert set(c0["material_ids"]) == {"mat_aaa", "mat_bbb"}
    assert len(c0["source_concepts"]) == 2

    # Full provenance attached
    sc = next(s for s in c0["source_concepts"] if s["material_id"] == "mat_aaa")
    assert sc["relevance"] == "high"
    assert sc["evidence_spans"] == ["the archive as a built environment"]
    assert sc["source_pages"] == [1, 3]
    assert sc["concept_name"] == "archival habitat"


# ---------------------------------------------------------------------------
# Test 2: cluster staleness skip
# ---------------------------------------------------------------------------

def test_cluster_staleness_skip(tmp_path, monkeypatch):
    """Unchanged concept fingerprint → clustering skipped, no LLM call."""
    import arquimedes.cluster as cluster_mod
    import arquimedes.config as config_mod

    db_path = _make_sqlite_db(tmp_path)
    index_dir = tmp_path / "indexes"
    index_dir.mkdir()
    import shutil
    shutil.copy(str(db_path), str(index_dir / "search.sqlite"))

    derived_dir = tmp_path / "derived"
    derived_dir.mkdir()

    monkeypatch.setattr(config_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(config_mod, "load_config", lambda: {"llm": {"agent_cmd": "echo"}})
    monkeypatch.setattr(cluster_mod, "get_project_root", lambda: tmp_path)

    config = {"llm": {"agent_cmd": "echo"}}

    # Write a fake cluster file and stamp matching current fingerprint
    fp = cluster_mod.cluster_fingerprint(config)
    (derived_dir / "cluster_stamp.json").write_text(
        json.dumps({"fingerprint": fp}), encoding="utf-8"
    )
    (derived_dir / "concept_clusters.jsonl").write_text(
        json.dumps({"cluster_id": "concept_0001", "canonical_name": "test", "slug": "test",
                    "aliases": [], "material_ids": [], "source_concepts": [], "confidence": 1.0})
        + "\n",
        encoding="utf-8",
    )

    llm_called = []

    def mock_llm(system, messages):
        llm_called.append(True)
        return "[]"

    result = cluster_mod.cluster_concepts(config, llm_fn=mock_llm, force=False)
    assert result.get("skipped") is True
    assert not llm_called


# ---------------------------------------------------------------------------
# Test 3: material page sections
# ---------------------------------------------------------------------------

def test_material_page_sections():
    """Rendered material page contains all expected sections."""
    meta = _make_meta("mat_aaa", "Necropolitics")
    clusters = [{
        "cluster_id": "concept_0001",
        "canonical_name": "archive as architectural space",
        "slug": "archive-as-architectural-space",
        "aliases": [],
        "material_ids": ["mat_aaa"],
        "source_concepts": [],
        "confidence": 0.85,
    }]
    chunks = [{"chunk_id": "c0", "text": "Some text", "summary": "Summary", "source_pages": [1]}]
    annotations = [{"annotation_id": "a0", "type": "highlight", "page": 1,
                    "quoted_text": "key passage", "comment": "interesting"}]
    figures = [{"figure_id": "fig_0001", "visual_type": "plan", "caption": "Ground floor plan",
                "description": "A floor plan.", "source_page": 2, "image_path": ""}]
    related = [{"material_id": "mat_bbb", "title": "Archival Landscapes",
                "reasons": ["shared concept: archive as architectural space"]}]

    page = render_material_page(meta, clusters, chunks, annotations, figures, related)

    assert "# Necropolitics" in page
    assert "## Metadata" in page
    assert "Alice" in page
    assert "## Summary" in page
    assert "A test summary." in page
    assert "## Key Concepts" in page
    assert "archive as architectural space" in page
    assert "## Architecture Facets" in page
    assert "building" in page  # scale (extracted from {value: "building"})
    assert "## Figures" in page
    assert "Ground floor plan" in page
    assert "## Reader Annotations" in page
    assert "key passage" in page
    assert "interesting" in page
    assert "## Related Materials" in page
    assert "Archival Landscapes" in page
    assert "## Source" in page


# ---------------------------------------------------------------------------
# Test 4: concept page evidence
# ---------------------------------------------------------------------------

def test_concept_page_evidence():
    """Concept page with 2 materials lists both with relevance and evidence spans."""
    cluster = {
        "cluster_id": "concept_0001",
        "canonical_name": "archive as architectural space",
        "slug": "archive-as-architectural-space",
        "aliases": ["archival habitat"],
        "material_ids": ["mat_aaa", "mat_bbb"],
        "source_concepts": [
            {
                "material_id": "mat_aaa",
                "concept_name": "archival habitat",
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
    assert "archival habitat" in page  # alias
    assert "2 materials" in page
    assert "Necropolitics" in page
    assert "high" in page
    assert "the archive as a built environment" in page
    assert "Archival Landscapes" in page
    assert "spatial dimension of archival practice" in page
    assert "## Related Concepts" in page
    assert "memory palace" in page


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
    assert "# Concept Glossary" in glossary
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
    (derived_dir / "concept_clusters.jsonl").write_text(clusters_text, encoding="utf-8")

    cluster_stamp = compile_mod._cluster_file_stamp(tmp_path)
    mat_stamp = compile_mod._material_stamp(extracted_dir)

    # Write up-to-date compile stamp
    compile_mod._write_compile_stamp(
        tmp_path,
        {"mat_aaa": mat_stamp},
        cluster_stamp,
    )

    monkeypatch.setattr(config_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(config_mod, "load_config", lambda: {"llm": {"agent_cmd": "echo"}})
    monkeypatch.setattr(compile_mod, "get_project_root", lambda: tmp_path)

    # Patch cluster_concepts to return "skipped"
    monkeypatch.setattr(
        cluster_mod, "cluster_concepts",
        lambda config, llm_fn=None, force=False: {
            "total_concepts": 3, "clusters": 1, "multi_material": 0, "skipped": True
        },
    )
    monkeypatch.setattr(cluster_mod, "load_clusters", lambda root=None: clusters_data)

    result = compile_mod.compile_wiki({"llm": {"agent_cmd": "echo"}}, force=False)

    assert result["material_pages"] == 0
    assert result["material_pages_skipped"] == 1
    assert result["concept_pages"] == 0  # cluster stamp unchanged


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

    # Create a valid material page and concept page
    valid_mat = wiki / "practice" / "_general" / "mat_aaa.md"
    valid_mat.write_text("# Valid", encoding="utf-8")
    valid_concept = wiki / "shared" / "concepts" / "memory-palace.md"
    valid_concept.write_text("# Memory Palace", encoding="utf-8")

    removed = _remove_orphans(wiki, {"mat_aaa"}, {"memory-palace"})

    assert not orphan_mat.exists()
    assert not orphan_concept.exists()
    assert valid_mat.exists()
    assert valid_concept.exists()
    assert len(removed) == 2


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
    from arquimedes.index import rebuild_index

    # Set up a real index (with all tables)
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "config.yaml").write_text("library_root: ~/dummy\n")
    (tmp_path / "indexes").mkdir()
    (tmp_path / "extracted").mkdir()
    manifests = tmp_path / "manifests"
    manifests.mkdir()
    mid = "mat_mmm"
    mat_dir = tmp_path / "extracted" / mid
    meta = _make_meta(mid, "Bridge Test Material")
    mat_dir.mkdir(parents=True)
    (mat_dir / "meta.json").write_text(json.dumps(meta))
    (mat_dir / "chunks.jsonl").write_text("")
    (mat_dir / "annotations.jsonl").write_text("")
    (manifests / "materials.jsonl").write_text(
        json.dumps({"material_id": mid, "file_hash": mid,
                    "relative_path": f"Research/{mid}.pdf", "file_type": "pdf",
                    "domain": "research", "collection": "_general",
                    "ingested_at": "2026-01-01T00:00:00+00:00"})
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
        "material_ids": [mid],
        "source_concepts": [{
            "material_id": mid,
            "concept_name": "archive space",
            "relevance": "high",
            "source_pages": [1, 2],
            "evidence_spans": ["the archive as built form"],
            "confidence": 0.88,
        }],
    }
    (derived / "concept_clusters.jsonl").write_text(json.dumps(cluster) + "\n")

    monkeypatch.setattr(config_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(config_mod, "load_config", lambda: {"llm": {"agent_cmd": "echo"}})
    monkeypatch.setattr(compile_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(
        cluster_mod, "cluster_concepts",
        lambda config, llm_fn=None, force=False: {
            "total_concepts": 1, "clusters": 1, "multi_material": 0, "skipped": True
        },
    )
    monkeypatch.setattr(
        cluster_mod, "load_clusters",
        lambda root=None: [cluster],
    )

    compile_mod.compile_wiki({"llm": {"agent_cmd": "echo"}}, force=True)

    # Memory bridge tables must be populated in search.sqlite
    con = sqlite3.connect(str(tmp_path / "indexes" / "search.sqlite"))
    concept_pages = con.execute(
        "SELECT path FROM wiki_pages WHERE page_type='concept'"
    ).fetchall()
    material_pages = con.execute(
        "SELECT path FROM wiki_pages WHERE page_type='material'"
    ).fetchall()
    aliases = con.execute(
        "SELECT alias FROM concept_cluster_aliases WHERE cluster_id='c_001'"
    ).fetchall()
    con.close()

    assert any("archive-space" in r[0] for r in concept_pages), "concept page not in wiki_pages"
    assert any(mid in r[0] for r in material_pages), "material page not in wiki_pages"
    assert {r[0] for r in aliases} == {"archival space", "archive as space"}
