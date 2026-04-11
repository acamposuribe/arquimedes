"""Memory bridge — project canonical concept graph into SQLite for agent access.

Reads derived/bridge_concept_clusters.jsonl (produced by arq cluster) and
materialises graph structures into search.sqlite
so agents can traverse them without opening wiki markdown.

Phase 5.5 is deterministic: no new LLM calls. It is a projection layer.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from arquimedes.config import get_project_root, load_config
from arquimedes.index import get_index_path


# ---------------------------------------------------------------------------
# Path helpers (mirrors compile_pages, avoids circular import)
# ---------------------------------------------------------------------------

def _material_wiki_path(domain: str, collection: str, material_id: str) -> str:
    """wiki/{domain}/{collection}/{material_id}.md"""
    d = (domain or "practice").strip() or "practice"
    c = (collection or "").strip() or "_general"
    return f"wiki/{d}/{c}/{material_id}.md"


def _concept_wiki_path(slug: str) -> str:
    """wiki/shared/concepts/{slug}.md"""
    return f"wiki/shared/concepts/{slug}.md"


def _bridge_concept_wiki_path(slug: str) -> str:
    """wiki/shared/bridge-concepts/{slug}.md"""
    return f"wiki/shared/bridge-concepts/{slug}.md"


def _local_concept_wiki_path(domain: str, collection: str, slug: str) -> str:
    d = (domain or "practice").strip() or "practice"
    c = (collection or "").strip() or "_general"
    return f"wiki/{d}/{c}/concepts/{slug}.md"


def _load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------

_BRIDGE_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS concept_clusters (
    cluster_id     TEXT PRIMARY KEY,
    canonical_name TEXT NOT NULL DEFAULT '',
    slug           TEXT NOT NULL DEFAULT '',
    aliases        TEXT NOT NULL DEFAULT '[]',
    confidence     REAL NOT NULL DEFAULT 0.0,
    wiki_path      TEXT NOT NULL DEFAULT '',
    material_count INTEGER NOT NULL DEFAULT 0
);

CREATE VIRTUAL TABLE IF NOT EXISTS concept_clusters_fts USING fts5(
    cluster_id UNINDEXED,
    canonical_name,
    aliases,
    content='concept_clusters',
    content_rowid='rowid',
    tokenize='porter unicode61'
);

CREATE TABLE IF NOT EXISTS cluster_materials (
    cluster_id         TEXT NOT NULL,
    material_id        TEXT NOT NULL,
    relevance          TEXT NOT NULL DEFAULT '',
    source_pages       TEXT NOT NULL DEFAULT '[]',
    evidence_spans     TEXT NOT NULL DEFAULT '[]',
    confidence         REAL NOT NULL DEFAULT 0.0,
    material_wiki_path TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (cluster_id, material_id)
);

CREATE TABLE IF NOT EXISTS cluster_relations (
    cluster_id            TEXT NOT NULL,
    related_cluster_id    TEXT NOT NULL,
    shared_material_count INTEGER NOT NULL DEFAULT 0,
    shared_material_ids   TEXT NOT NULL DEFAULT '[]',
    PRIMARY KEY (cluster_id, related_cluster_id)
);

CREATE TABLE IF NOT EXISTS local_concept_clusters (
    cluster_id     TEXT PRIMARY KEY,
    domain         TEXT NOT NULL DEFAULT '',
    collection     TEXT NOT NULL DEFAULT '',
    canonical_name TEXT NOT NULL DEFAULT '',
    slug           TEXT NOT NULL DEFAULT '',
    aliases        TEXT NOT NULL DEFAULT '[]',
    confidence     REAL NOT NULL DEFAULT 0.0,
    wiki_path      TEXT NOT NULL DEFAULT '',
    material_count INTEGER NOT NULL DEFAULT 0
);

CREATE VIRTUAL TABLE IF NOT EXISTS local_concept_clusters_fts USING fts5(
    cluster_id UNINDEXED,
    canonical_name,
    aliases,
    content='local_concept_clusters',
    content_rowid='rowid',
    tokenize='porter unicode61'
);

CREATE TABLE IF NOT EXISTS local_cluster_materials (
    cluster_id         TEXT NOT NULL,
    material_id        TEXT NOT NULL,
    relevance          TEXT NOT NULL DEFAULT '',
    source_pages       TEXT NOT NULL DEFAULT '[]',
    evidence_spans     TEXT NOT NULL DEFAULT '[]',
    confidence         REAL NOT NULL DEFAULT 0.0,
    material_wiki_path TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (cluster_id, material_id)
);

CREATE TABLE IF NOT EXISTS local_cluster_relations (
    cluster_id            TEXT NOT NULL,
    related_cluster_id    TEXT NOT NULL,
    shared_material_count INTEGER NOT NULL DEFAULT 0,
    shared_material_ids   TEXT NOT NULL DEFAULT '[]',
    PRIMARY KEY (cluster_id, related_cluster_id)
);

CREATE TABLE IF NOT EXISTS local_concept_cluster_aliases (
    cluster_id TEXT NOT NULL,
    alias      TEXT NOT NULL,
    PRIMARY KEY (cluster_id, alias)
);

CREATE TABLE IF NOT EXISTS concept_cluster_aliases (
    cluster_id TEXT NOT NULL,
    alias      TEXT NOT NULL,
    PRIMARY KEY (cluster_id, alias)
);

CREATE TABLE IF NOT EXISTS wiki_pages (
    page_type  TEXT NOT NULL,
    page_id    TEXT NOT NULL,
    title      TEXT NOT NULL DEFAULT '',
    path       TEXT NOT NULL UNIQUE,
    domain     TEXT NOT NULL DEFAULT '',
    collection TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (page_type, page_id)
);

CREATE TABLE IF NOT EXISTS cluster_reviews (
    review_id              TEXT PRIMARY KEY,
    cluster_id             TEXT NOT NULL DEFAULT '',
    finding_type           TEXT NOT NULL DEFAULT '',
    severity               TEXT NOT NULL DEFAULT '',
    recommendation         TEXT NOT NULL DEFAULT '',
    affected_material_ids  TEXT NOT NULL DEFAULT '[]',
    affected_concept_names TEXT NOT NULL DEFAULT '[]',
    evidence               TEXT NOT NULL DEFAULT '[]',
    input_fingerprint      TEXT NOT NULL DEFAULT '',
    wiki_path              TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS concept_reflections (
    cluster_id               TEXT PRIMARY KEY,
    slug                     TEXT NOT NULL DEFAULT '',
    canonical_name           TEXT NOT NULL DEFAULT '',
    main_takeaways           TEXT NOT NULL DEFAULT '[]',
    main_tensions            TEXT NOT NULL DEFAULT '[]',
    open_questions           TEXT NOT NULL DEFAULT '[]',
    why_this_concept_matters TEXT NOT NULL DEFAULT '',
    supporting_material_ids  TEXT NOT NULL DEFAULT '[]',
    supporting_evidence      TEXT NOT NULL DEFAULT '[]',
    input_fingerprint        TEXT NOT NULL DEFAULT '',
    wiki_path                TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS collection_reflections (
    domain                  TEXT NOT NULL,
    collection              TEXT NOT NULL,
    main_takeaways          TEXT NOT NULL DEFAULT '[]',
    main_tensions           TEXT NOT NULL DEFAULT '[]',
    important_material_ids  TEXT NOT NULL DEFAULT '[]',
    important_cluster_ids   TEXT NOT NULL DEFAULT '[]',
    open_questions          TEXT NOT NULL DEFAULT '[]',
    why_this_collection_matters TEXT NOT NULL DEFAULT '',
    input_fingerprint       TEXT NOT NULL DEFAULT '',
    wiki_path               TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (domain, collection)
);

CREATE TABLE IF NOT EXISTS graph_findings (
    finding_id              TEXT PRIMARY KEY,
    finding_type            TEXT NOT NULL DEFAULT '',
    severity                TEXT NOT NULL DEFAULT '',
    summary                 TEXT NOT NULL DEFAULT '',
    details                 TEXT NOT NULL DEFAULT '',
    affected_material_ids   TEXT NOT NULL DEFAULT '[]',
    affected_cluster_ids    TEXT NOT NULL DEFAULT '[]',
    candidate_future_sources TEXT NOT NULL DEFAULT '[]',
    candidate_bridge_links  TEXT NOT NULL DEFAULT '[]',
    input_fingerprint       TEXT NOT NULL DEFAULT ''
);
"""

_BRIDGE_COLUMNS: list[tuple[str, str]] = [
    ("concept_clusters",  "ALTER TABLE concept_clusters ADD COLUMN wiki_path TEXT NOT NULL DEFAULT ''"),
    ("concept_clusters",  "ALTER TABLE concept_clusters ADD COLUMN material_count INTEGER NOT NULL DEFAULT 0"),
    ("cluster_materials", "ALTER TABLE cluster_materials ADD COLUMN confidence REAL NOT NULL DEFAULT 0.0"),
    ("cluster_materials", "ALTER TABLE cluster_materials ADD COLUMN material_wiki_path TEXT NOT NULL DEFAULT ''"),
    ("cluster_relations", "ALTER TABLE cluster_relations ADD COLUMN shared_material_ids TEXT NOT NULL DEFAULT '[]'"),
    ("collection_reflections", "ALTER TABLE collection_reflections ADD COLUMN why_this_collection_matters TEXT NOT NULL DEFAULT ''"),
]


def _ensure_bridge_schema(con: sqlite3.Connection) -> None:
    """Create bridge-only tables and add bridge columns to existing tables if absent."""
    con.executescript(_BRIDGE_TABLE_DDL)
    for _table, stmt in _BRIDGE_COLUMNS:
        try:
            con.execute(stmt)
        except sqlite3.OperationalError:
            pass  # column already exists


# ---------------------------------------------------------------------------
# Bridge population
# ---------------------------------------------------------------------------

def _build_bridge(con: sqlite3.Connection, clusters: list[dict], local_clusters: list[dict], root: Path) -> dict:
    """Populate bridge tables from cluster data and materials table.

    Does a full replacement of concept_clusters, cluster_materials,
    cluster_relations (with all columns including bridge extras), plus
    concept_cluster_aliases and wiki_pages.

    Returns summary counts.
    """
    # Load material info from search.sqlite (domain/collection for path computation)
    mat_rows = con.execute(
        "SELECT material_id, domain, collection, title FROM materials"
    ).fetchall()
    mat_info: dict[str, dict] = {
        r["material_id"]: {
            "domain": r["domain"],
            "collection": r["collection"],
            "title": r["title"],
        }
        for r in mat_rows
    }

    # Clear all cluster tables + bridge-only tables; repopulate fully below
    con.execute("DELETE FROM concept_cluster_aliases")
    con.execute("DELETE FROM local_concept_cluster_aliases")
    con.execute("DELETE FROM wiki_pages")
    con.execute("DELETE FROM cluster_materials")
    con.execute("DELETE FROM local_cluster_materials")
    con.execute("DELETE FROM cluster_relations")
    con.execute("DELETE FROM local_cluster_relations")
    con.execute("DELETE FROM concept_clusters")
    con.execute("DELETE FROM local_concept_clusters")
    con.execute("DELETE FROM cluster_reviews")
    con.execute("DELETE FROM concept_reflections")
    con.execute("DELETE FROM collection_reflections")
    con.execute("DELETE FROM graph_findings")
    # Rebuild FTS
    con.execute("INSERT INTO concept_clusters_fts(concept_clusters_fts) VALUES ('delete-all')")
    con.execute("INSERT INTO local_concept_clusters_fts(local_concept_clusters_fts) VALUES ('delete-all')")

    n_aliases = 0
    n_concept_pages = 0
    n_index_pages = 0
    n_material_pages = 0
    n_cluster_material_links = 0
    n_cluster_relations = 0
    n_local_aliases = 0
    n_local_cluster_material_links = 0
    n_local_cluster_relations = 0

    # Build material → [cluster_ids] map for cluster_relations + shared_material_ids
    mat_to_clusters: dict[str, list[str]] = defaultdict(list)
    local_mat_to_clusters: dict[str, list[str]] = defaultdict(list)

    for c in clusters:
        cluster_id = c.get("cluster_id", "")
        if not cluster_id:
            continue

        slug = c.get("slug", "")
        canonical_name = c.get("canonical_name", "")
        aliases: list[str] = c.get("aliases") or []
        source_concepts: list[dict] = c.get("source_concepts") or []
        confidence = float(c.get("confidence", 0.0))

        # Collect unique material_ids for this cluster
        seen_mids: set[str] = set()
        unique_mids: list[str] = []
        for sc in source_concepts:
            mid = sc.get("material_id", "")
            if mid and mid not in seen_mids:
                seen_mids.add(mid)
                unique_mids.append(mid)

        material_count = len(unique_mids)
        wiki_path = c.get("wiki_path") or (
            _bridge_concept_wiki_path(slug) if material_count > 1 else _concept_wiki_path(slug)
        )

        # Full INSERT: concept_clusters (with bridge columns)
        con.execute(
            """INSERT OR REPLACE INTO concept_clusters
               (cluster_id, canonical_name, slug, aliases, confidence, wiki_path, material_count)
               VALUES (?,?,?,?,?,?,?)""",
            (
                cluster_id,
                canonical_name,
                slug,
                json.dumps(aliases, ensure_ascii=False),
                confidence,
                wiki_path,
                material_count,
            ),
        )

        # Aliases
        for alias in aliases:
            if alias:
                con.execute(
                    "INSERT OR IGNORE INTO concept_cluster_aliases VALUES (?,?)",
                    (cluster_id, alias),
                )
                n_aliases += 1

        # Concept wiki page only for bridge clusters (more than one material).
        if material_count > 1:
            con.execute(
                """INSERT OR REPLACE INTO wiki_pages
                   (page_type, page_id, title, path, domain, collection)
                   VALUES (?,?,?,?,?,?)""",
                ("concept", cluster_id, canonical_name, wiki_path, "shared", "bridge-concepts"),
            )
            n_concept_pages += 1

        # cluster_materials rows (with bridge columns confidence + material_wiki_path)
        for sc in source_concepts:
            mid = sc.get("material_id", "")
            if not mid:
                continue
            sc_confidence = float(sc.get("confidence", 0.0))
            info = mat_info.get(mid, {})
            mat_wiki_path = _material_wiki_path(
                info.get("domain", ""), info.get("collection", ""), mid
            )
            con.execute(
                """INSERT OR IGNORE INTO cluster_materials
                   (cluster_id, material_id, relevance, source_pages, evidence_spans,
                    confidence, material_wiki_path)
                   VALUES (?,?,?,?,?,?,?)""",
                (
                    cluster_id,
                    mid,
                    sc.get("relevance", ""),
                    json.dumps(sc.get("source_pages") or [], ensure_ascii=False),
                    json.dumps(sc.get("evidence_spans") or [], ensure_ascii=False),
                    sc_confidence,
                    mat_wiki_path,
                ),
            )
            n_cluster_material_links += 1
            mat_to_clusters[mid].append(cluster_id)

    for c in local_clusters:
        cluster_id = c.get("cluster_id", "")
        if not cluster_id:
            continue

        domain = (c.get("domain") or "practice").strip() or "practice"
        collection = (c.get("collection") or "_general").strip() or "_general"
        slug = c.get("slug", "")
        canonical_name = c.get("canonical_name", "")
        aliases: list[str] = c.get("aliases") or []
        source_concepts: list[dict] = c.get("source_concepts") or []
        confidence = float(c.get("confidence", 0.0))

        seen_mids: set[str] = set()
        unique_mids: list[str] = []
        for sc in source_concepts:
            mid = sc.get("material_id", "")
            if mid and mid not in seen_mids:
                seen_mids.add(mid)
                unique_mids.append(mid)

        material_count = len(unique_mids)
        wiki_path = c.get("wiki_path") or _local_concept_wiki_path(domain, collection, slug)

        con.execute(
            """INSERT OR REPLACE INTO local_concept_clusters
               (cluster_id, domain, collection, canonical_name, slug, aliases, confidence, wiki_path, material_count)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (
                cluster_id,
                domain,
                collection,
                canonical_name,
                slug,
                json.dumps(aliases, ensure_ascii=False),
                confidence,
                wiki_path,
                material_count,
            ),
        )

        for alias in aliases:
            if alias:
                con.execute(
                    "INSERT OR IGNORE INTO local_concept_cluster_aliases VALUES (?,?)",
                    (cluster_id, alias),
                )
                n_local_aliases += 1

        con.execute(
            """INSERT OR REPLACE INTO wiki_pages
               (page_type, page_id, title, path, domain, collection)
               VALUES (?,?,?,?,?,?)""",
            ("concept", cluster_id, canonical_name, wiki_path, domain, collection),
        )
        n_concept_pages += 1

        for sc in source_concepts:
            mid = sc.get("material_id", "")
            if not mid:
                continue
            sc_confidence = float(sc.get("confidence", 0.0))
            info = mat_info.get(mid, {})
            mat_wiki_path = _material_wiki_path(
                info.get("domain", ""), info.get("collection", ""), mid
            )
            con.execute(
                """INSERT OR IGNORE INTO local_cluster_materials
                   (cluster_id, material_id, relevance, source_pages, evidence_spans,
                    confidence, material_wiki_path)
                   VALUES (?,?,?,?,?,?,?)""",
                (
                    cluster_id,
                    mid,
                    sc.get("relevance", ""),
                    json.dumps(sc.get("source_pages") or [], ensure_ascii=False),
                    json.dumps(sc.get("evidence_spans") or [], ensure_ascii=False),
                    sc_confidence,
                    mat_wiki_path,
                ),
            )
            n_local_cluster_material_links += 1
            local_mat_to_clusters[mid].append(cluster_id)

    # Derive cluster_relations from shared material membership (with shared_material_ids)
    pair_mids: dict[tuple[str, str], list[str]] = defaultdict(list)
    for mid, cluster_ids in mat_to_clusters.items():
        unique = list(dict.fromkeys(cluster_ids))
        for i, a in enumerate(unique):
            for b in unique[i + 1:]:
                pair_mids[(a, b)].append(mid)
                pair_mids[(b, a)].append(mid)

    for (a, b), mids in pair_mids.items():
        con.execute(
            """INSERT OR REPLACE INTO cluster_relations
               (cluster_id, related_cluster_id, shared_material_count, shared_material_ids)
               VALUES (?,?,?,?)""",
            (a, b, len(mids), json.dumps(mids, ensure_ascii=False)),
        )
        n_cluster_relations += 1

    local_pair_mids: dict[tuple[str, str], list[str]] = defaultdict(list)
    for mid, cluster_ids in local_mat_to_clusters.items():
        unique = list(dict.fromkeys(cluster_ids))
        for i, a in enumerate(unique):
            for b in unique[i + 1:]:
                local_pair_mids[(a, b)].append(mid)
                local_pair_mids[(b, a)].append(mid)

    for (a, b), mids in local_pair_mids.items():
        con.execute(
            """INSERT OR REPLACE INTO local_cluster_relations
               (cluster_id, related_cluster_id, shared_material_count, shared_material_ids)
               VALUES (?,?,?,?)""",
            (a, b, len(mids), json.dumps(mids, ensure_ascii=False)),
        )
        n_local_cluster_relations += 1

    # Rebuild FTS after inserting cluster data
    con.execute("INSERT INTO concept_clusters_fts(concept_clusters_fts) VALUES ('rebuild')")
    con.execute("INSERT INTO local_concept_clusters_fts(local_concept_clusters_fts) VALUES ('rebuild')")

    # Material wiki pages
    for mid, info in mat_info.items():
        domain = (info.get("domain") or "practice").strip() or "practice"
        collection = (info.get("collection") or "").strip() or "_general"
        path = _material_wiki_path(domain, collection, mid)
        title = info.get("title", "")
        con.execute(
            """INSERT OR REPLACE INTO wiki_pages
               (page_type, page_id, title, path, domain, collection)
               VALUES (?,?,?,?,?,?)""",
            ("material", mid, title, path, domain, collection),
        )
        n_material_pages += 1

    # Collection wiki pages
    collection_groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for info in mat_info.values():
        domain = (info.get("domain") or "practice").strip() or "practice"
        collection = (info.get("collection") or "").strip() or "_general"
        collection_groups[(domain, collection)].append(info)
    for (domain, collection), infos in collection_groups.items():
        page_path = f"wiki/{domain}/{collection}/_index.md"
        title = f"{domain.replace('_', ' ').title()} / {collection.replace('_', ' ').title()}"
        con.execute(
            """INSERT OR REPLACE INTO wiki_pages
               (page_type, page_id, title, path, domain, collection)
               VALUES (?,?,?,?,?,?)""",
            ("collection", f"{domain}/{collection}", title, page_path, domain, collection),
        )

    # Cluster reviews
    lint_dir = root / "derived" / "lint"
    for review in _load_jsonl(lint_dir / "cluster_reviews.jsonl"):
        review_id = review.get("review_id") or review.get("cluster_id") or review.get("finding_id") or ""
        if not review_id:
            continue
        con.execute(
            """INSERT OR REPLACE INTO cluster_reviews
               (review_id, cluster_id, finding_type, severity, recommendation,
                affected_material_ids, affected_concept_names, evidence,
                input_fingerprint, wiki_path)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (
                review_id,
                review.get("cluster_id", ""),
                review.get("finding_type", ""),
                review.get("severity", ""),
                review.get("recommendation", ""),
                json.dumps(review.get("affected_material_ids") or [], ensure_ascii=False),
                json.dumps(review.get("affected_concept_names") or [], ensure_ascii=False),
                json.dumps(review.get("evidence") or [], ensure_ascii=False),
                review.get("input_fingerprint", ""),
                review.get("wiki_path", ""),
            ),
        )

    # Deterministic index pages for raw local concepts and bridge glossary.
    con.execute(
        """INSERT OR REPLACE INTO wiki_pages
           (page_type, page_id, title, path, domain, collection)
           VALUES (?,?,?,?,?,?)""",
        ("concept_index", "local", "Local Concepts", "wiki/shared/concepts/_index.md", "shared", "concepts"),
    )
    n_index_pages += 1
    con.execute(
        """INSERT OR REPLACE INTO wiki_pages
           (page_type, page_id, title, path, domain, collection)
           VALUES (?,?,?,?,?,?)""",
        ("glossary", "bridge", "Main Concepts", "wiki/shared/glossary/_index.md", "shared", "glossary"),
    )
    n_index_pages += 1

    # Concept reflections
    for reflection in _load_jsonl(lint_dir / "concept_reflections.jsonl"):
        cluster_id = reflection.get("cluster_id", "")
        if not cluster_id:
            continue
        con.execute(
            """INSERT OR REPLACE INTO concept_reflections
               (cluster_id, slug, canonical_name, main_takeaways, main_tensions,
                open_questions, why_this_concept_matters, supporting_material_ids,
                supporting_evidence, input_fingerprint, wiki_path)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (
                cluster_id,
                reflection.get("slug", ""),
                reflection.get("canonical_name", ""),
                json.dumps(reflection.get("main_takeaways") or [], ensure_ascii=False),
                json.dumps(reflection.get("main_tensions") or [], ensure_ascii=False),
                json.dumps(reflection.get("open_questions") or [], ensure_ascii=False),
                reflection.get("why_this_concept_matters", ""),
                json.dumps(reflection.get("supporting_material_ids") or [], ensure_ascii=False),
                json.dumps(reflection.get("supporting_evidence") or [], ensure_ascii=False),
                reflection.get("input_fingerprint", ""),
                reflection.get("wiki_path", ""),
            ),
        )

    # Collection reflections
    for reflection in _load_jsonl(lint_dir / "collection_reflections.jsonl"):
        domain = reflection.get("domain", "")
        collection = reflection.get("collection", "")
        if not domain or not collection:
            continue
        con.execute(
            """INSERT OR REPLACE INTO collection_reflections
               (domain, collection, main_takeaways, main_tensions,
                important_material_ids, important_cluster_ids, open_questions,
                why_this_collection_matters, input_fingerprint, wiki_path)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (
                domain,
                collection,
                json.dumps(reflection.get("main_takeaways") or [], ensure_ascii=False),
                json.dumps(reflection.get("main_tensions") or [], ensure_ascii=False),
                json.dumps(reflection.get("important_material_ids") or [], ensure_ascii=False),
                json.dumps(reflection.get("important_cluster_ids") or [], ensure_ascii=False),
                json.dumps(reflection.get("open_questions") or [], ensure_ascii=False),
                reflection.get("why_this_collection_matters", ""),
                reflection.get("input_fingerprint", ""),
                reflection.get("wiki_path", ""),
            ),
        )

    # Graph findings
    for finding in _load_jsonl(lint_dir / "graph_findings.jsonl"):
        finding_id = finding.get("finding_id", "")
        if not finding_id:
            continue
        con.execute(
            """INSERT OR REPLACE INTO graph_findings
               (finding_id, finding_type, severity, summary, details,
                affected_material_ids, affected_cluster_ids,
                candidate_future_sources, candidate_bridge_links,
                input_fingerprint)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (
                finding_id,
                finding.get("finding_type", ""),
                finding.get("severity", ""),
                finding.get("summary", ""),
                finding.get("details", ""),
                json.dumps(finding.get("affected_material_ids") or [], ensure_ascii=False),
                json.dumps(finding.get("affected_cluster_ids") or [], ensure_ascii=False),
                json.dumps(finding.get("candidate_future_sources") or [], ensure_ascii=False),
                json.dumps(finding.get("candidate_bridge_links") or [], ensure_ascii=False),
                finding.get("input_fingerprint", ""),
            ),
        )

    return {
        "clusters": len(clusters),
        "aliases": n_aliases,
        "cluster_material_links": n_cluster_material_links,
        "cluster_relations": n_cluster_relations,
        "local_clusters": len(local_clusters),
        "local_aliases": n_local_aliases,
        "local_cluster_material_links": n_local_cluster_material_links,
        "local_cluster_relations": n_local_cluster_relations,
        "concept_pages": n_concept_pages,
        "material_pages": n_material_pages,
        "wiki_pages": n_concept_pages + n_material_pages + n_index_pages,
    }


# ---------------------------------------------------------------------------
# Fingerprinting / stamp
# ---------------------------------------------------------------------------

def _fingerprint_file(path: Path) -> str:
    if not path.exists():
        return ""
    return hashlib.sha256(path.read_bytes()).hexdigest()[:16]


def _cluster_fingerprint(root: Path) -> str:
    bridge_path = root / "derived" / "bridge_concept_clusters.jsonl"
    local_paths = sorted((root / "derived" / "collections").glob("*/local_concept_clusters.jsonl"))
    lint_dir = root / "derived" / "lint"
    lint_paths = [
        lint_dir / "cluster_reviews.jsonl",
        lint_dir / "concept_reflections.jsonl",
        lint_dir / "collection_reflections.jsonl",
        lint_dir / "graph_findings.jsonl",
    ]
    if not bridge_path.exists() and not local_paths and not any(path.exists() for path in lint_paths):
        return ""
    hasher = hashlib.sha256()
    if bridge_path.exists():
        hasher.update(bridge_path.read_bytes())
    for path in local_paths:
        hasher.update(path.read_bytes())
    for path in lint_paths:
        if path.exists():
            hasher.update(path.read_bytes())
    return hasher.hexdigest()[:16]


def _read_stamp(stamp_path: Path) -> dict:
    if not stamp_path.exists():
        return {}
    try:
        return json.loads(stamp_path.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def _write_stamp(
    stamp_path: Path,
    clusters_fp: str,
    manifest_fp: str,
    counts: dict,
) -> None:
    stamp = {
        "built_at": datetime.now(timezone.utc).isoformat(),
        "clusters_fingerprint": clusters_fp,
        "manifest_fingerprint": manifest_fp,
        "counts": counts,
    }
    stamp_path.parent.mkdir(parents=True, exist_ok=True)
    stamp_path.write_text(json.dumps(stamp, separators=(',', ':'), ensure_ascii=False))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def memory_rebuild(config: dict | None = None) -> dict:
    """Rebuild memory bridge tables in search.sqlite.

        Reads bridge cluster JSONL files and the materials table, then:
    - creates concept_cluster_aliases and wiki_pages rows
    - updates bridge columns on concept_clusters, cluster_materials,
      cluster_relations

    Returns summary counts dict.
    Raises FileNotFoundError if index or cluster file is missing.
    """
    if config is None:
        config = load_config()

    root = get_project_root()
    index_path = get_index_path(config)
    manifest_path = root / "manifests" / "materials.jsonl"
    stamp_path = root / "derived" / "memory_bridge_stamp.json"
    cluster_paths = [root / "derived" / "bridge_concept_clusters.jsonl", *sorted((root / "derived" / "collections").glob("*/local_concept_clusters.jsonl"))]

    if not index_path.exists():
        raise FileNotFoundError(
            f"Search index not found at {index_path}. Run `arq index rebuild` first."
        )
    if not any(path.exists() for path in cluster_paths):
        raise FileNotFoundError(
            f"Cluster file not found at {cluster_paths[0]}. Run `arq cluster` first."
        )

    from arquimedes.cluster import load_bridge_clusters, load_local_clusters

    clusters = load_bridge_clusters(root)
    local_clusters = load_local_clusters(root)

    con = sqlite3.connect(str(index_path))
    con.row_factory = sqlite3.Row
    try:
        _ensure_bridge_schema(con)
        counts = _build_bridge(con, clusters, local_clusters, root)
        con.commit()
    finally:
        con.close()

    clusters_fp = _cluster_fingerprint(root)
    manifest_fp = _fingerprint_file(manifest_path)
    _write_stamp(stamp_path, clusters_fp, manifest_fp, counts)

    return counts


def memory_ensure(config: dict | None = None) -> tuple[bool, dict]:
    """Rebuild memory bridge only if stale.

    Returns (rebuilt, counts_dict).
    When up to date, counts_dict contains {"skipped": True}.
    """
    if config is None:
        config = load_config()

    root = get_project_root()
    index_path = get_index_path(config)
    manifest_path = root / "manifests" / "materials.jsonl"
    stamp_path = root / "derived" / "memory_bridge_stamp.json"

    if not index_path.exists():
        raise FileNotFoundError(
            f"Search index not found at {index_path}. Run `arq index rebuild` first."
        )

    clusters_fp = _cluster_fingerprint(root)
    manifest_fp = _fingerprint_file(manifest_path)
    stamp = _read_stamp(stamp_path)

    if (
        clusters_fp
        and stamp.get("clusters_fingerprint") == clusters_fp
        and stamp.get("manifest_fingerprint") == manifest_fp
    ):
        return False, {"skipped": True}

    counts = memory_rebuild(config)
    return True, counts
