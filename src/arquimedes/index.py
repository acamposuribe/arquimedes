"""SQLite FTS5 index builder for Arquimedes search.

Builds a local search index from enriched extracted/ artifacts.
The index is gitignored — each device rebuilds locally via `arq index rebuild`
or `arq index ensure` (smart staleness check).
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from arquimedes.config import get_project_root, load_config


# --- Schema ---

_DDL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS materials (
    material_id       TEXT PRIMARY KEY,
    title             TEXT NOT NULL DEFAULT '',
    summary           TEXT NOT NULL DEFAULT '',
    keywords          TEXT NOT NULL DEFAULT '',
    methodological_conclusions TEXT NOT NULL DEFAULT '',
    main_content_learnings     TEXT NOT NULL DEFAULT '',
    raw_keywords      TEXT NOT NULL DEFAULT '',
    domain            TEXT NOT NULL DEFAULT '',
    collection        TEXT NOT NULL DEFAULT '',
    document_type     TEXT NOT NULL DEFAULT '',
    raw_document_type TEXT NOT NULL DEFAULT '',
    authors           TEXT NOT NULL DEFAULT '',
    year              TEXT NOT NULL DEFAULT '',
    file_type         TEXT NOT NULL DEFAULT '',
    page_count        INTEGER NOT NULL DEFAULT 0,
    building_type     TEXT NOT NULL DEFAULT '',
    scale             TEXT NOT NULL DEFAULT '',
    location          TEXT NOT NULL DEFAULT '',
    jurisdiction      TEXT NOT NULL DEFAULT '',
    climate           TEXT NOT NULL DEFAULT '',
    program           TEXT NOT NULL DEFAULT '',
    material_system   TEXT NOT NULL DEFAULT '',
    structural_system TEXT NOT NULL DEFAULT '',
    historical_period TEXT NOT NULL DEFAULT '',
    course_topic      TEXT NOT NULL DEFAULT '',
    studio_project    TEXT NOT NULL DEFAULT ''
);

CREATE VIRTUAL TABLE IF NOT EXISTS materials_fts USING fts5(
    material_id UNINDEXED,
    title,
    summary,
    keywords,
    raw_keywords,
    authors,
    methodological_conclusions,
    main_content_learnings,
    content='materials',
    content_rowid='rowid',
    tokenize='porter unicode61'
);

CREATE TABLE IF NOT EXISTS chunks (
    chunk_id      TEXT NOT NULL,
    material_id   TEXT NOT NULL,
    text          TEXT NOT NULL DEFAULT '',
    summary       TEXT NOT NULL DEFAULT '',
    keywords      TEXT NOT NULL DEFAULT '',
    source_pages  TEXT NOT NULL DEFAULT '[]',
    emphasized    INTEGER NOT NULL DEFAULT 0,
    content_class TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (material_id, chunk_id)
);

CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(
    chunk_id UNINDEXED,
    material_id UNINDEXED,
    summary,
    text,
    keywords,
    content='chunks',
    content_rowid='rowid'
);

CREATE TABLE IF NOT EXISTS figures (
    figure_id    TEXT NOT NULL,
    material_id  TEXT NOT NULL,
    description  TEXT NOT NULL DEFAULT '',
    caption      TEXT NOT NULL DEFAULT '',
    visual_type  TEXT NOT NULL DEFAULT '',
    source_page  INTEGER NOT NULL DEFAULT 0,
    relevance    TEXT NOT NULL DEFAULT '',
    image_path   TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (material_id, figure_id)
);

CREATE VIRTUAL TABLE IF NOT EXISTS figures_fts USING fts5(
    figure_id UNINDEXED,
    material_id UNINDEXED,
    description,
    caption,
    content='figures',
    content_rowid='rowid'
);

CREATE TABLE IF NOT EXISTS annotations (
    annotation_id TEXT NOT NULL,
    material_id   TEXT NOT NULL,
    type          TEXT NOT NULL DEFAULT '',
    page          INTEGER NOT NULL DEFAULT 0,
    quoted_text   TEXT NOT NULL DEFAULT '',
    comment       TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (material_id, annotation_id)
);

CREATE VIRTUAL TABLE IF NOT EXISTS annotations_fts USING fts5(
    annotation_id UNINDEXED,
    material_id UNINDEXED,
    quoted_text,
    comment,
    content='annotations',
    content_rowid='rowid'
);

CREATE TABLE IF NOT EXISTS concepts (
    concept_name   TEXT NOT NULL,
    descriptor     TEXT NOT NULL DEFAULT '',
    material_id    TEXT NOT NULL,
    concept_type   TEXT NOT NULL DEFAULT 'local',
    concept_key    TEXT NOT NULL DEFAULT '',
    relevance      TEXT NOT NULL DEFAULT '',
    source_pages   TEXT NOT NULL DEFAULT '[]',
    evidence_spans TEXT NOT NULL DEFAULT '[]',
    confidence     REAL NOT NULL DEFAULT 0.0,
    PRIMARY KEY (material_id, concept_type, concept_key)
);

CREATE VIRTUAL TABLE IF NOT EXISTS concepts_fts USING fts5(
    concept_name,
    concept_key,
    material_id UNINDEXED,
    content='concepts',
    content_rowid='rowid',
    tokenize='porter unicode61'
);

CREATE TABLE IF NOT EXISTS material_keywords (
    material_id TEXT NOT NULL,
    keyword     TEXT NOT NULL,
    PRIMARY KEY (material_id, keyword)
);

CREATE TABLE IF NOT EXISTS material_authors (
    material_id TEXT NOT NULL,
    author      TEXT NOT NULL,
    PRIMARY KEY (material_id, author)
);

-- Wiki page registry; populated by arq memory rebuild.
CREATE TABLE IF NOT EXISTS wiki_pages (
    page_type  TEXT NOT NULL,           -- material | concept | collection
    page_id    TEXT NOT NULL,           -- material_id or cluster_id
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
    helpful_new_sources      TEXT NOT NULL DEFAULT '[]',
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
    helpful_new_sources     TEXT NOT NULL DEFAULT '[]',
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

CREATE TABLE IF NOT EXISTS index_state (
    id                INTEGER PRIMARY KEY CHECK (id = 1),
    built_at          TEXT NOT NULL,
    manifest_hash     TEXT NOT NULL,
    material_count    INTEGER NOT NULL,
    extracted_snapshot TEXT NOT NULL
);
"""


@dataclass
class IndexStats:
    materials: int = 0
    chunks: int = 0
    figures: int = 0
    annotations: int = 0
    concepts: int = 0
    elapsed: float = 0.0


# --- Concept-key normalization ---

import re as _re


def _normalize_concept_key(name: str) -> str:
    """Normalize concept name to a canonical key for cross-material deduplication.

    Handles: case folding, whitespace collapse, basic English plural of final word.
    """
    key = name.lower().strip()
    key = _re.sub(r"\s+", " ", key)
    words = key.split()
    if words:
        last = words[-1]
        if len(last) > 3:
            if last.endswith("ies") and len(last) > 4:
                last = last[:-3] + "y"
            elif last.endswith("ses") or last.endswith("xes") or last.endswith("zes"):
                last = last[:-2]
            elif last.endswith("ches") or last.endswith("shes"):
                last = last[:-2]
            elif (
                last.endswith("s")
                and not last.endswith("ss")
                and not last.endswith("us")
                and not last.endswith("is")
            ):
                last = last[:-1]
        words[-1] = last
        key = " ".join(words)
    return key


# --- Value helpers ---

def _val(field: dict | None) -> str:
    """Extract .value from an EnrichedField dict; return '' if absent."""
    if field is None:
        return ""
    if isinstance(field, dict):
        v = field.get("value", "")
        if isinstance(v, list):
            return " ".join(str(x) for x in v)
        return str(v) if v else ""
    return ""


def _kw_json(field: dict | None) -> str:
    """Return keywords as a JSON array string for storage. Also produces FTS-indexable form."""
    if field is None:
        return "[]"
    v = field.get("value", [])
    if isinstance(v, list):
        return json.dumps(v, ensure_ascii=False)
    if isinstance(v, str):
        return json.dumps([v], ensure_ascii=False)
    return "[]"


def _raw_kw_json(lst: list | None) -> str:
    if not lst:
        return "[]"
    return json.dumps(lst, ensure_ascii=False)


def _ensure_concepts_descriptor_column(con: sqlite3.Connection) -> None:
    """Add the concepts.descriptor column for older indexes when needed."""
    rows = con.execute("PRAGMA table_info(concepts)").fetchall()
    columns = {str(row[1]) for row in rows}
    if "descriptor" not in columns:
        con.execute("ALTER TABLE concepts ADD COLUMN descriptor TEXT NOT NULL DEFAULT ''")


def _ensure_materials_conclusion_columns(con: sqlite3.Connection) -> None:
    """Add material conclusion columns for older indexes when needed."""
    rows = con.execute("PRAGMA table_info(materials)").fetchall()
    columns = {str(row[1]) for row in rows}
    if "methodological_conclusions" not in columns:
        con.execute("ALTER TABLE materials ADD COLUMN methodological_conclusions TEXT NOT NULL DEFAULT ''")
    if "main_content_learnings" not in columns:
        con.execute("ALTER TABLE materials ADD COLUMN main_content_learnings TEXT NOT NULL DEFAULT ''")


# --- Index build ---

def rebuild_index(config: dict | None = None) -> IndexStats:
    """Full index rebuild from scratch. Atomic: writes to temp file then renames."""
    import time
    t0 = time.monotonic()

    if config is None:
        config = load_config()

    from arquimedes.config import (
        get_enabled_domains,
        get_extracted_root,
        get_indexes_root,
        get_manifests_root,
    )

    root = get_project_root()
    extracted_dir = get_extracted_root()
    manifests_dir = get_manifests_root()
    indexes_dir = get_indexes_root(config)
    indexes_dir.mkdir(parents=True, exist_ok=True)

    manifest_path = manifests_dir / "materials.jsonl"
    enabled_domains = get_enabled_domains(config)
    material_ids: list[str] = []
    if manifest_path.exists():
        with open(manifest_path) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        record = json.loads(line)
                        if str(record.get("domain") or "").strip().lower() in enabled_domains:
                            material_ids.append(record["material_id"])
                    except (json.JSONDecodeError, KeyError):
                        pass

    index_path = indexes_dir / "search.sqlite"
    fd, tmp_path = tempfile.mkstemp(dir=indexes_dir, suffix=".sqlite.tmp")
    os.close(fd)

    try:
        con = sqlite3.connect(tmp_path)
        con.executescript(_DDL)
        _ensure_concepts_descriptor_column(con)
        _ensure_materials_conclusion_columns(con)

        stats = IndexStats()

        for mid in material_ids:
            mat_dir = extracted_dir / mid
            if not mat_dir.is_dir():
                continue

            # --- materials ---
            meta_path = mat_dir / "meta.json"
            if not meta_path.exists():
                continue
            with open(meta_path) as f:
                meta = json.load(f)

            facets = meta.get("facets") or {}
            con.execute(
                """INSERT OR REPLACE INTO materials VALUES (
                    ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
                )""",
                (
                    mid,
                    meta.get("title", ""),
                    _val(meta.get("summary")),
                    _kw_json(meta.get("keywords")),
                    _val(meta.get("methodological_conclusions")),
                    _val(meta.get("main_content_learnings")),
                    _raw_kw_json(meta.get("raw_keywords")),
                    meta.get("domain", ""),
                    meta.get("collection", ""),
                    _val(meta.get("document_type")),
                    meta.get("raw_document_type", ""),
                    json.dumps(meta.get("authors", []), ensure_ascii=False),
                    meta.get("year", ""),
                    meta.get("file_type", ""),
                    meta.get("page_count", 0),
                    _val(facets.get("building_type")),
                    _val(facets.get("scale")),
                    _val(facets.get("location")),
                    _val(facets.get("jurisdiction")),
                    _val(facets.get("climate")),
                    _val(facets.get("program")),
                    _val(facets.get("material_system")),
                    _val(facets.get("structural_system")),
                    _val(facets.get("historical_period")),
                    _val(facets.get("course_topic")),
                    _val(facets.get("studio_project")),
                ),
            )
            stats.materials += 1

            # --- material_keywords (helper table for relational joins) ---
            kw_field = meta.get("keywords")
            kw_list: list[str] = []
            if isinstance(kw_field, dict):
                v = kw_field.get("value", [])
                kw_list = v if isinstance(v, list) else [str(v)] if v else []
            elif isinstance(kw_field, list):
                kw_list = kw_field
            for kw in kw_list:
                normed = str(kw).lower().strip()
                if normed:
                    con.execute(
                        "INSERT OR REPLACE INTO material_keywords VALUES (?,?)",
                        (mid, normed),
                    )

            # --- material_authors (helper table for relational joins) ---
            auth_list = meta.get("authors", [])
            if isinstance(auth_list, list):
                for author in auth_list:
                    normed = str(author).lower().strip()
                    if normed:
                        con.execute(
                            "INSERT OR REPLACE INTO material_authors VALUES (?,?)",
                            (mid, normed),
                        )

            # --- chunks ---
            chunks_path = mat_dir / "chunks.jsonl"
            if chunks_path.exists():
                with open(chunks_path) as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            c = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        con.execute(
                            "INSERT OR REPLACE INTO chunks VALUES (?,?,?,?,?,?,?,?)",
                            (
                                c["chunk_id"],
                                mid,
                                c.get("text", ""),
                                _val(c.get("summary")),
                                _val(c.get("keywords")),
                                json.dumps(c.get("source_pages", [])),
                                1 if c.get("emphasized") else 0,
                                c.get("content_class", ""),
                            ),
                        )
                        stats.chunks += 1

            # --- figures ---
            figures_dir = mat_dir / "figures"
            if figures_dir.is_dir():
                for fig_path in sorted(figures_dir.glob("fig_*.json")):
                    try:
                        with open(fig_path) as f:
                            fig = json.load(f)
                    except (json.JSONDecodeError, OSError):
                        continue
                    con.execute(
                        "INSERT OR REPLACE INTO figures VALUES (?,?,?,?,?,?,?,?)",
                        (
                            fig["figure_id"],
                            mid,
                            _val(fig.get("description")),
                            _val(fig.get("caption")),
                            _val(fig.get("visual_type")),
                            fig.get("source_page", 0),
                            fig.get("relevance", ""),
                            fig.get("image_path", ""),
                        ),
                    )
                    stats.figures += 1

            # --- annotations ---
            ann_path = mat_dir / "annotations.jsonl"
            if ann_path.exists():
                with open(ann_path) as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            a = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        con.execute(
                            "INSERT OR REPLACE INTO annotations VALUES (?,?,?,?,?,?)",
                            (
                                a["annotation_id"],
                                mid,
                                a.get("type", ""),
                                a.get("page", 0),
                                a.get("quoted_text", ""),
                                a.get("comment", ""),
                            ),
                        )
                        stats.annotations += 1

            # --- concepts ---
            concepts_path = mat_dir / "concepts.jsonl"
            if concepts_path.exists():
                with open(concepts_path) as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            c = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        concept_name = c.get("concept_name", "").strip()
                        if not concept_name:
                            continue
                        concept_type = c.get("concept_type", "local")
                        concept_key = _normalize_concept_key(concept_name)
                        prov = c.get("provenance") or {}
                        con.execute(
                            "INSERT OR REPLACE INTO concepts (concept_name, descriptor, material_id, concept_type, concept_key, relevance, source_pages, evidence_spans, confidence) VALUES (?,?,?,?,?,?,?,?,?)",
                            (
                                concept_name,
                                c.get("descriptor", ""),
                                mid,
                                concept_type,
                                concept_key,
                                c.get("relevance", ""),
                                json.dumps(prov.get("source_pages", []), ensure_ascii=False),
                                json.dumps(prov.get("evidence_spans", []), ensure_ascii=False),
                                prov.get("confidence", 0.0),
                            ),
                        )
                        stats.concepts += 1

        # Populate FTS tables
        con.execute("INSERT INTO materials_fts(materials_fts) VALUES ('rebuild')")
        con.execute("INSERT INTO chunks_fts(chunks_fts) VALUES ('rebuild')")
        con.execute("INSERT INTO figures_fts(figures_fts) VALUES ('rebuild')")
        con.execute("INSERT INTO annotations_fts(annotations_fts) VALUES ('rebuild')")
        con.execute("INSERT INTO concepts_fts(concepts_fts) VALUES ('rebuild')")

        # Write index_state
        manifest_hash = _compute_manifest_hash(manifest_path)
        extracted_snapshot = _compute_extracted_snapshot(extracted_dir, material_ids, root)
        now = datetime.now(timezone.utc).isoformat()
        con.execute(
            "INSERT OR REPLACE INTO index_state VALUES (1, ?, ?, ?, ?)",
            (now, manifest_hash, stats.materials, extracted_snapshot),
        )

        con.commit()
        con.close()

        os.replace(tmp_path, index_path)

    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise

    stats.elapsed = time.monotonic() - t0
    return stats


# --- Cluster graph indexing ---

def index_clusters(config: dict | None = None) -> int:
    """Legacy no-op; memory_rebuild owns cluster graph projection."""
    del config
    return 0


# --- Staleness detection ---

def ensure_index(config: dict | None = None) -> tuple[bool, IndexStats | None]:
    """Rebuild index only if stale. Returns (rebuilt, stats_or_None)."""
    if config is None:
        config = load_config()

    from arquimedes.config import (
        get_extracted_root,
        get_manifests_root,
    )

    root = get_project_root()
    index_path = get_index_path(config)
    manifests_dir = get_manifests_root()
    extracted_dir = get_extracted_root()
    manifest_path = manifests_dir / "materials.jsonl"

    # Fast path: no index
    if not index_path.exists():
        stats = rebuild_index(config)
        return True, stats

    # Fast gate: count + mtime
    try:
        con = sqlite3.connect(f"file:{index_path}?mode=ro", uri=True)
        row = con.execute(
            "SELECT built_at, manifest_hash, material_count, extracted_snapshot FROM index_state WHERE id=1"
        ).fetchone()
        con.close()
    except sqlite3.Error:
        # Corrupt or missing index_state — rebuild
        stats = rebuild_index(config)
        return True, stats

    if row is None:
        stats = rebuild_index(config)
        return True, stats

    built_at_str, stored_manifest_hash, stored_count, stored_snapshot = row

    # Count materials in manifest
    current_count = _count_manifest_lines(manifest_path)
    if current_count != stored_count:
        stats = rebuild_index(config)
        return True, stats

    # Mtime gate: if any meta.json is newer than built_at, check hashes
    try:
        built_at = datetime.fromisoformat(built_at_str)
    except ValueError:
        stats = rebuild_index(config)
        return True, stats

    newest_mtime = _newest_input_mtime(extracted_dir)
    if newest_mtime is not None and newest_mtime > built_at.timestamp():
        # Ambiguous — fall through to hash comparison
        material_ids = _read_manifest_ids(manifest_path)
        current_manifest_hash = _compute_manifest_hash(manifest_path)
        current_snapshot = _compute_extracted_snapshot(extracted_dir, material_ids, root)
        if current_manifest_hash != stored_manifest_hash or current_snapshot != stored_snapshot:
            stats = rebuild_index(config)
            return True, stats

    return False, None


def ensure_index_and_memory(config: dict | None = None) -> tuple[bool, "IndexStats | None", bool, dict]:
    """ensure_index() followed by memory_ensure().

    Returns (index_rebuilt, index_stats, memory_rebuilt, memory_counts).
    This is the collaborator recovery path: deterministic, no LLM, no cluster.
    """
    index_rebuilt, stats = ensure_index(config)
    try:
        from arquimedes.memory import memory_ensure
        memory_rebuilt, memory_counts = memory_ensure(config)
    except FileNotFoundError:
        memory_rebuilt = False
        memory_counts = {"skipped": True}
    return index_rebuilt, stats, memory_rebuilt, memory_counts

def _compute_manifest_hash(manifest_path: Path) -> str:
    if not manifest_path.exists():
        return ""
    h = hashlib.sha256(manifest_path.read_bytes())
    return h.hexdigest()[:16]


def _compute_extracted_snapshot(extracted_dir: Path, material_ids: list[str], root: Path | None = None) -> str:
    """Hash of all index-input file contents across all materials, sorted for determinism.

    Includes local/global cluster artifact hashes so staleness is detected when
    clustering changes without any material metadata changing.
    """
    parts: list[str] = []
    for mid in sorted(material_ids):
        mat_dir = extracted_dir / mid
        for fname in ("meta.json", "chunks.jsonl", "annotations.jsonl", "concepts.jsonl"):
            p = mat_dir / fname
            if p.exists():
                try:
                    h = hashlib.sha256(p.read_bytes()).hexdigest()[:8]
                    parts.append(f"{mid}:{fname}:{h}")
                except OSError:
                    pass
        figures_dir = mat_dir / "figures"
        if figures_dir.is_dir():
            for fig_path in sorted(figures_dir.glob("fig_*.json")):
                try:
                    h = hashlib.sha256(fig_path.read_bytes()).hexdigest()[:8]
                    parts.append(f"{mid}:fig:{fig_path.name}:{h}")
                except OSError:
                    pass

    # Include cluster graph so re-clustering triggers index refresh
    if root is not None:
        ch = _clusters_hash(root)
        if ch:
            parts.append(f"clusters:{ch}")

    combined = "\n".join(parts)
    return hashlib.sha256(combined.encode()).hexdigest()[:16]


def _clusters_hash(root: Path) -> str:
    """Hash of current cluster JSONL files for staleness detection."""
    paths = [
        root / "derived" / "global_bridge_clusters.jsonl",
        *sorted((root / "derived" / "collections").glob("*/local_concept_clusters.jsonl")),
    ]
    existing = [path for path in paths if path.exists()]
    if not existing:
        return ""
    hasher = hashlib.sha256()
    for path in existing:
        hasher.update(path.read_bytes())
    return hasher.hexdigest()[:16]


def _count_manifest_lines(manifest_path: Path) -> int:
    if not manifest_path.exists():
        return 0
    count = 0
    with open(manifest_path) as f:
        for line in f:
            if line.strip():
                count += 1
    return count


def _read_manifest_ids(manifest_path: Path) -> list[str]:
    ids = []
    if not manifest_path.exists():
        return ids
    with open(manifest_path) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    ids.append(json.loads(line)["material_id"])
                except (json.JSONDecodeError, KeyError):
                    pass
    return ids


def _newest_input_mtime(extracted_dir: Path) -> float | None:
    """Return the newest mtime among all files that feed the index."""
    if not extracted_dir.is_dir():
        return None
    newest: float | None = None
    for pattern in ("*/meta.json", "*/chunks.jsonl", "*/annotations.jsonl", "*/concepts.jsonl", "*/figures/fig_*.json"):
        for p in extracted_dir.glob(pattern):
            try:
                mtime = p.stat().st_mtime
                if newest is None or mtime > newest:
                    newest = mtime
            except OSError:
                pass
    return newest


def get_index_path(config: dict | None = None) -> Path:
    from arquimedes.config import get_indexes_root

    return get_indexes_root(config) / "search.sqlite"
