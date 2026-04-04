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
    content='materials',
    content_rowid='rowid'
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
    elapsed: float = 0.0


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


# --- Index build ---

def rebuild_index(config: dict | None = None) -> IndexStats:
    """Full index rebuild from scratch. Atomic: writes to temp file then renames."""
    import time
    t0 = time.monotonic()

    if config is None:
        config = load_config()

    root = get_project_root()
    extracted_dir = root / "extracted"
    manifests_dir = root / "manifests"
    indexes_dir = root / "indexes"
    indexes_dir.mkdir(parents=True, exist_ok=True)

    manifest_path = manifests_dir / "materials.jsonl"
    material_ids: list[str] = []
    if manifest_path.exists():
        with open(manifest_path) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        material_ids.append(json.loads(line)["material_id"])
                    except (json.JSONDecodeError, KeyError):
                        pass

    index_path = indexes_dir / "search.sqlite"
    fd, tmp_path = tempfile.mkstemp(dir=indexes_dir, suffix=".sqlite.tmp")
    os.close(fd)

    try:
        con = sqlite3.connect(tmp_path)
        con.executescript(_DDL)

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
                    ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
                )""",
                (
                    mid,
                    meta.get("title", ""),
                    _val(meta.get("summary")),
                    _kw_json(meta.get("keywords")),
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

        # Populate FTS tables
        con.execute("INSERT INTO materials_fts(materials_fts) VALUES ('rebuild')")
        con.execute("INSERT INTO chunks_fts(chunks_fts) VALUES ('rebuild')")
        con.execute("INSERT INTO figures_fts(figures_fts) VALUES ('rebuild')")
        con.execute("INSERT INTO annotations_fts(annotations_fts) VALUES ('rebuild')")

        # Write index_state
        manifest_hash = _compute_manifest_hash(manifest_path)
        extracted_snapshot = _compute_extracted_snapshot(extracted_dir, material_ids)
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


# --- Staleness detection ---

def ensure_index(config: dict | None = None) -> tuple[bool, IndexStats | None]:
    """Rebuild index only if stale. Returns (rebuilt, stats_or_None)."""
    if config is None:
        config = load_config()

    root = get_project_root()
    index_path = root / "indexes" / "search.sqlite"
    manifests_dir = root / "manifests"
    extracted_dir = root / "extracted"
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

    newest_mtime = _newest_meta_mtime(extracted_dir)
    if newest_mtime is not None and newest_mtime > built_at.timestamp():
        # Ambiguous — fall through to hash comparison
        material_ids = _read_manifest_ids(manifest_path)
        current_manifest_hash = _compute_manifest_hash(manifest_path)
        current_snapshot = _compute_extracted_snapshot(extracted_dir, material_ids)
        if current_manifest_hash != stored_manifest_hash or current_snapshot != stored_snapshot:
            stats = rebuild_index(config)
            return True, stats

    return False, None


# --- Snapshot helpers ---

def _compute_manifest_hash(manifest_path: Path) -> str:
    if not manifest_path.exists():
        return ""
    h = hashlib.sha256(manifest_path.read_bytes())
    return h.hexdigest()[:16]


def _compute_extracted_snapshot(extracted_dir: Path, material_ids: list[str]) -> str:
    """Hash of enrichment stamps across all materials, sorted for determinism."""
    parts: list[str] = []
    for mid in sorted(material_ids):
        mat_dir = extracted_dir / mid
        # Document stamp
        meta_path = mat_dir / "meta.json"
        if meta_path.exists():
            try:
                meta = json.loads(meta_path.read_text())
                stamp = meta.get("_enrichment_stamp")
                if stamp:
                    parts.append(f"{mid}:doc:{json.dumps(stamp, sort_keys=True)}")
            except (json.JSONDecodeError, OSError):
                pass
        # Chunk stamps
        chunk_stamps_path = mat_dir / "chunk_enrichment_stamps.json"
        if chunk_stamps_path.exists():
            try:
                content = chunk_stamps_path.read_text()
                parts.append(f"{mid}:chunks:{hashlib.sha256(content.encode()).hexdigest()[:8]}")
            except OSError:
                pass
        # Figure stamps (representative sample: first figure)
        figures_dir = mat_dir / "figures"
        if figures_dir.is_dir():
            fig_files = sorted(figures_dir.glob("fig_*.json"))
            for fig_path in fig_files[:1]:
                try:
                    fig = json.loads(fig_path.read_text())
                    stamp = fig.get("_enrichment_stamp")
                    if stamp:
                        parts.append(f"{mid}:fig:{json.dumps(stamp, sort_keys=True)}")
                except (json.JSONDecodeError, OSError):
                    pass

    combined = "\n".join(parts)
    return hashlib.sha256(combined.encode()).hexdigest()[:16]


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


def _newest_meta_mtime(extracted_dir: Path) -> float | None:
    """Return the newest mtime among all extracted/*/meta.json files."""
    if not extracted_dir.is_dir():
        return None
    newest: float | None = None
    for meta_path in extracted_dir.glob("*/meta.json"):
        try:
            mtime = meta_path.stat().st_mtime
            if newest is None or mtime > newest:
                newest = mtime
        except OSError:
            pass
    return newest


def get_index_path(config: dict | None = None) -> Path:
    root = get_project_root()
    return root / "indexes" / "search.sqlite"
