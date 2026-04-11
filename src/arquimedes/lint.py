"""Phase 6 linting, reflection, and memory growth.

This module implements the maintenance pass for Arquimedes:
- deterministic health checks first
- reflective LLM passes second
- optional materialization back into wiki pages
- projection of reflection artifacts into the SQLite memory bridge

The design follows the Phase 6 spec closely:
- deterministic checks are always run first
- expensive passes are dirty-set driven
- concept reflections and collection reflections can run in parallel
- graph reflection is the final global pass
- reflective outputs are durable files under ``derived/lint/``
"""

from __future__ import annotations

import json
import os
import re
import shutil
import sqlite3
import threading
from datetime import datetime, timezone
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from arquimedes.cluster import (
    _collection_scope,
    _attach_run_provenance,
    _build_source_concept,
    _derive_cluster_confidence,
    _load_concept_rows,
    _load_material_rows,
    _normalize_concept_name,
    _resolve_concept_reference,
    _split_concept_row,
    _stage_bridge_packet_input,
    is_bridge_clustering_stale,
    bridge_cluster_fingerprint,
    load_bridge_clusters,
    load_local_clusters,
    local_cluster_dir,
    local_cluster_fingerprint,
    local_cluster_stamp_path,
    normalize_local_clusters,
    slugify,
)
from arquimedes.compile import compile_wiki
from arquimedes.compile_pages import (
    _concept_wiki_path,
    _material_wiki_path,
    _meta_val,
    render_collection_page,
)
from arquimedes.config import get_project_root, load_config
from arquimedes.enrich import _is_chunk_stale, _is_document_stale, _is_figure_stale
from arquimedes.enrich_llm import EnrichmentError, LlmFn, get_model_id, make_cli_llm_fn, parse_json_or_repair
from arquimedes.enrich_stamps import canonical_hash
from arquimedes.index import (
    _compute_extracted_snapshot,
    _compute_manifest_hash,
    _count_manifest_lines,
    _newest_input_mtime,
    _read_manifest_ids,
    get_index_path,
)
from arquimedes.memory import _cluster_fingerprint, _fingerprint_file, memory_rebuild


LINT_DIR = "derived/lint"
REPORT_PATH = Path("wiki/_lint_report.md")
LINT_STAGE_STAMP_PATH = Path(LINT_DIR) / "lint_stamp.json"
CLUSTER_AUDIT_STATE_PATH = Path(LINT_DIR) / "cluster_audit_state.json"
GRAPH_REFLECTION_STAMP_PATH = Path(LINT_DIR) / "graph_reflection_stamp.json"
LOCAL_AUDIT_STATE_NAME = "local_audit_state.json"
LOCAL_AUDIT_STAMP_NAME = "local_audit_stamp.json"
DEFAULT_GRAPH_REFLECTION_INTERVAL_HOURS = 168.0
DEFAULT_GRAPH_REFLECTION_MIN_CLUSTER_DELTA = 3
DEFAULT_GRAPH_REFLECTION_MIN_MATERIAL_DELTA = 5
MAX_CONTEXT_REQUESTS_PER_PASS = 4
CONCEPT_REFLECTION_TOTAL_CHUNK_BUDGET = 40
CONCEPT_REFLECTION_MAX_CHUNKS_PER_MATERIAL = 4
CONCEPT_REFLECTION_MAX_ANNOTATIONS_PER_MATERIAL = 4
CONCEPT_REFLECTION_MAX_FIGURES_PER_MATERIAL = 2
CONCEPT_REFLECTION_MAX_CONCEPTS_PER_MATERIAL = 4
COLLECTION_REFLECTION_MAX_MATERIALS = 8
COLLECTION_REFLECTION_TOTAL_CHUNK_BUDGET = 16
COLLECTION_REFLECTION_MAX_CHUNKS_PER_MATERIAL = 2
COLLECTION_REFLECTION_MAX_ANNOTATIONS_PER_MATERIAL = 3
COLLECTION_REFLECTION_MAX_FIGURES_PER_MATERIAL = 2
COLLECTION_REFLECTION_MAX_CONCEPTS_PER_MATERIAL = 3
_LINK_RE = re.compile(r"(?<!!)\[[^\]]+\]\(([^)]+)\)")
_LINK_WITH_LABEL_RE = re.compile(r"(?<!!)\[([^\]]+)\]\(([^)]+)\)")
_FENCE_RE = re.compile(r"```.*?```", re.DOTALL)
LINT_REFLECTIVE_STAGES = (
    "cluster-audit",
    "concept-reflection",
    "collection-reflection",
    "graph-maintenance",
)
_CLUSTER_AUDIT_DELTA_SCHEMA = '{"bridge_updates":[{"cluster_id":"existing bridge id","new_name":"string","new_aliases":["strings"],"new_source_concepts":[{"material_id":"string","concept_name":"string"}],"new_materials":["material ids"],"removed_materials":["material ids"]}],"new_bridges":[{"bridge_ref":"temporary id","canonical_name":"string","aliases":["strings"],"material_ids":["material ids"],"source_concepts":[{"material_id":"string","concept_name":"string"}]}],"review_updates":[{"cluster_id":"existing bridge id","finding_type":"string","severity":"low|medium|high","status":"open|validated","note":"string","recommendation":"string"}],"new_reviews":[{"cluster_ref":"existing bridge id or exact new_bridges.bridge_ref","bridge_ref":"optional alias for cluster_ref on new bridges","finding_type":"string","severity":"low|medium|high","status":"open|validated","note":"string","recommendation":"string"}],"context_requests":[{"tool":"search_material_evidence|open_record","...":"..."}],"_finished":true}'
_CONCEPT_REFLECTION_DELTA_SCHEMA = '{"main_takeaways":["strings"]|null,"main_tensions":["strings"]|null,"open_questions":["strings"]|null,"why_this_concept_matters":"string"|null,"context_requests":[{"tool":"search_material_evidence|open_record","...":"..."}],"_finished":true}'
_COLLECTION_REFLECTION_DELTA_SCHEMA = '{"main_takeaways":["strings"]|null,"main_tensions":["strings"]|null,"important_material_ids":["material ids"]|null,"important_cluster_ids":["cluster ids"]|null,"open_questions":["strings"]|null,"why_this_collection_matters":"string"|null,"context_requests":[{"tool":"search_material_evidence|open_record","...":"..."}],"_finished":true}'
_GRAPH_REFLECTION_DELTA_SCHEMA = '{"findings":[{"finding_id":"string(optional)","finding_type":"string","severity":"low|medium|high","summary":"string","details":"string","affected_material_ids":["material ids"],"affected_cluster_ids":["cluster ids"],"candidate_future_sources":["strings"],"candidate_bridge_links":["strings"]}]|null,"_finished":true}'


# ---------------------------------------------------------------------------
# Generic file helpers
# ---------------------------------------------------------------------------

def _load_json(path: Path, default=None):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return default


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


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows)
    if rows:
        text += "\n"
    path.write_text(text, encoding="utf-8")


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, separators=(',', ':'), ensure_ascii=False), encoding="utf-8")


def _read_stamp(path: Path) -> dict:
    return _load_json(path, {}) or {}


def _write_stamp(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, separators=(',', ':'), ensure_ascii=False), encoding="utf-8")


def _cleanup_paths(*paths: Path) -> None:
    """Best-effort cleanup for temporary staging files."""
    for path in paths:
        try:
            if path and path.exists():
                path.unlink()
        except OSError:
            pass


def _stage_work_copy(source: Path, target: Path) -> Path:
    """Duplicate a canonical file into tmp so LLM can edit the copy in place.

    For JSONL files, strips _provenance from each record so the LLM doesn't see
    or corrupt internal bookkeeping fields. Provenance is re-attached at promotion.
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    if not source.exists():
        target.write_text("", encoding="utf-8")
        return target
    if source.suffix == ".jsonl":
        lines = []
        for line in source.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                record.pop("_provenance", None)
                lines.append(json.dumps(record, ensure_ascii=False))
            except json.JSONDecodeError:
                lines.append(line)
        target.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    else:
        shutil.copyfile(source, target)
    return target


def _stage_cluster_audit_reviews_input(
    target: Path,
    canonical_reviews: dict[str, dict],
    target_cluster_ids: set[str],
) -> Path:
    target.parent.mkdir(parents=True, exist_ok=True)
    staged_rows = []
    for cluster_id in sorted(target_cluster_ids):
        row = canonical_reviews.get(cluster_id)
        if not isinstance(row, dict):
            continue
        staged_row = dict(row)
        staged_row.pop("_provenance", None)
        staged_rows.append(staged_row)
    _write_jsonl(target, staged_rows)
    return target


def _stage_reflection_page_copy(source: Path, target: Path) -> Path:
    """Stage a link-light wiki page copy for reflection prompts."""
    target.parent.mkdir(parents=True, exist_ok=True)
    text = _read_text(source)
    sanitized = _LINK_WITH_LABEL_RE.sub(lambda match: match.group(1).strip(), _strip_fences(text))
    target.write_text(sanitized, encoding="utf-8")
    return target


def _parse_iso_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _lint_schedule_config(config: dict | None = None) -> dict:
    lint_cfg = (config or {}).get("lint", {}) if isinstance(config, dict) else {}
    return lint_cfg if isinstance(lint_cfg, dict) else {}


def _issue(
    check: str,
    severity: str,
    title: str,
    detail: str,
    *,
    path: str = "",
    material_id: str = "",
    collection: str = "",
    domain: str = "",
    cluster_id: str = "",
    target: str = "",
    fixable: bool = False,
) -> dict:
    return {
        "check": check,
        "severity": severity,
        "title": title,
        "detail": detail,
        "path": path,
        "material_id": material_id,
        "collection": collection,
        "domain": domain,
        "cluster_id": cluster_id,
        "target": target,
        "fixable": fixable,
    }


def _material_page_path(meta: dict) -> Path:
    return Path(_material_wiki_path(meta))


def _concept_page_path(cluster: dict) -> Path:
    return Path(cluster.get("wiki_path") or _concept_wiki_path(cluster["slug"]))


def _collection_page_path(domain: str, collection: str) -> Path:
    collection = collection or "_general"
    return Path(f"wiki/{domain}/{collection}/_index.md")


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return ""


def _strip_fences(text: str) -> str:
    return _FENCE_RE.sub("", text)


def _extract_links(md_text: str) -> list[str]:
    links = []
    for match in _LINK_RE.finditer(_strip_fences(md_text)):
        target = match.group(1).strip()
        if target:
            links.append(target)
    return links


def _resolve_wiki_link(page_path: Path, target: str, wiki_root: Path, root: Path) -> Path | None:
    if not target or target.startswith("#"):
        return None
    if target.startswith(("http://", "https://", "mailto:", "file://")):
        return None
    target = target.split("#", 1)[0].strip()
    if not target:
        return None
    if target.startswith("wiki/"):
        resolved = root / target
    else:
        resolved = (page_path.parent / target).resolve()
    try:
        resolved.relative_to(root)
    except ValueError:
        return None
    return resolved


def _safe_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(v) for v in value if str(v).strip()]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []


def _meta_list_value(meta: dict, key: str) -> list[str]:
    field = meta.get(key)
    if isinstance(field, dict):
        return _safe_list(field.get("value", []))
    return _safe_list(field)


def _dedupe_strings(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        item = str(value).strip()
        key = item.casefold()
        if not item or key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def _normalize_lint_stages(stages: list[str] | tuple[str, ...] | None) -> list[str]:
    if not stages:
        return []
    normalized: list[str] = []
    seen: set[str] = set()
    for stage in stages:
        value = str(stage).strip().lower()
        if value not in LINT_REFLECTIVE_STAGES:
            raise ValueError(f"unknown lint stage '{stage}'")
        if value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def _truncate(text: str, limit: int = 220) -> str:
    text = (text or "").strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"


def _parse_json_list(value: str | None) -> list[Any]:
    if not value:
        return []
    try:
        parsed = json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return []
    return parsed if isinstance(parsed, list) else []


def _extract_marked_section(page_text: str, marker: str) -> str:
    start = f"<!-- phase6:{marker}:start -->"
    end = f"<!-- phase6:{marker}:end -->"
    if start not in page_text or end not in page_text:
        return ""
    before, rest = page_text.split(start, 1)
    section, _after = rest.split(end, 1)
    return section.strip()


class ReflectionIndexTool:
    """Read-only SQL-backed context tool for Phase 6 reflection passes."""

    def __init__(self, root: Path):
        self.root = root
        self.index_path = get_index_path()
        if not self.index_path.exists():
            raise FileNotFoundError(
                f"Search index not found at {self.index_path}. Run `arq index rebuild` first."
            )
        self.con = sqlite3.connect(f"file:{self.index_path}?mode=ro", uri=True, check_same_thread=False)
        self.con.row_factory = sqlite3.Row
        self._lock = threading.RLock()

    def close(self) -> None:
        self.con.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    @staticmethod
    def _row_to_dict(row: sqlite3.Row) -> dict:
        return {k: row[k] for k in row.keys()}

    def _material_evidence(
        self,
        material_id: str,
        query_terms: list[str] | None = None,
        chunk_limit: int = 2,
        annotation_limit: int = 2,
        figure_limit: int = 2,
        concept_limit: int = 4,
    ) -> dict:
        query_terms = [str(term).lower() for term in (query_terms or []) if str(term).strip()]
        with self._lock:
            chunks = []
            chunk_limit = max(0, int(chunk_limit or 0))
            if chunk_limit > 0:
                chunk_rows = self.con.execute(
                    """
                    SELECT chunk_id, summary, text, source_pages, emphasized, content_class
                    FROM chunks
                    WHERE material_id = ?
                    ORDER BY emphasized DESC, rowid
                    LIMIT ?
                    """,
                    [material_id, max(chunk_limit, 4)],
                ).fetchall()
                for row in chunk_rows:
                    text = row["text"] or ""
                    summary = row["summary"] or ""
                    haystack = f"{summary} {text}".lower()
                    score = 1 if any(term in haystack for term in query_terms) else 0
                    chunks.append({
                        "chunk_id": row["chunk_id"],
                        "source_pages": _parse_json_list(row["source_pages"]),
                        "content_class": row["content_class"],
                        "summary": summary,
                        "text": text,
                        "excerpt": _truncate(text or summary, 180),
                        "relevance": score,
                    })
                chunks.sort(key=lambda item: (-item["relevance"], item["chunk_id"]))
                chunks = chunks[:chunk_limit]

            annotations = []
            annotation_limit = max(0, int(annotation_limit or 0))
            for row in self.con.execute(
                """
                SELECT annotation_id, type, page, quoted_text, comment
                FROM annotations
                WHERE material_id = ?
                ORDER BY (CASE WHEN comment = '' THEN 1 ELSE 0 END), rowid
                LIMIT ?
                """,
                [material_id, annotation_limit],
            ).fetchall():
                annotations.append({
                    "annotation_id": row["annotation_id"],
                    "type": row["type"],
                    "page": row["page"],
                    "quoted_text": _truncate(row["quoted_text"] or "", 180),
                    "comment": _truncate(row["comment"] or "", 160),
                })

            figures = []
            figure_limit = max(0, int(figure_limit or 0))
            for row in self.con.execute(
                """
                SELECT figure_id, description, visual_type, source_page, relevance
                FROM figures
                WHERE material_id = ?
                ORDER BY (CASE WHEN relevance = 'substantive' THEN 0 ELSE 1 END), rowid
                LIMIT ?
                """,
                [material_id, figure_limit],
            ).fetchall():
                figures.append({
                    "figure_id": row["figure_id"],
                    "description": _truncate(row["description"] or "", 180),
                    "visual_type": row["visual_type"],
                    "source_page": row["source_page"],
                    "relevance": row["relevance"],
                })

            concepts = []
            concept_limit = max(0, int(concept_limit or 0))
            try:
                concept_rows = self.con.execute(
                    """
                    SELECT concept_name, descriptor, concept_type, relevance, source_pages, evidence_spans, confidence
                    FROM concepts
                    WHERE material_id = ?
                    ORDER BY (CASE WHEN concept_type = 'bridge' THEN 0 ELSE 1 END), rowid
                    LIMIT ?
                    """,
                    [material_id, concept_limit],
                ).fetchall()
            except sqlite3.OperationalError:
                concept_rows = self.con.execute(
                    """
                    SELECT concept_name, concept_type, relevance, source_pages, evidence_spans, confidence
                    FROM concepts
                    WHERE material_id = ?
                    ORDER BY (CASE WHEN concept_type = 'bridge' THEN 0 ELSE 1 END), rowid
                    LIMIT ?
                    """,
                    [material_id, concept_limit],
                ).fetchall()
            for row in concept_rows:
                concepts.append({
                    "concept_name": row["concept_name"],
                    "descriptor": row["descriptor"] if "descriptor" in row.keys() else "",
                    "concept_type": row["concept_type"],
                    "relevance": row["relevance"],
                    "source_pages": _parse_json_list(row["source_pages"]),
                    "evidence_spans": _parse_json_list(row["evidence_spans"]),
                    "confidence": row["confidence"] or 0.0,
                })

            return {
                "chunks": chunks[:chunk_limit],
                "annotations": annotations,
                "figures": figures,
                "concepts": concepts,
            }

    def search_materials(self, query: str, limit: int = 5) -> list[dict]:
        from arquimedes.search import search as do_search

        query = (query or "").strip()
        if not query:
            return []
        result = do_search(query, depth=2, limit=limit, chunk_limit=2, annotation_limit=1, figure_limit=1, concept_limit=2)
        return [card.to_dict() for card in result.results[:limit]]

    def search_material_evidence(self, kind: str, material_id: str, query: str, limit: int = 5) -> list[dict]:
        from arquimedes.search import safe_fts_query
        kind = (kind or "").strip().lower()
        material_id = (material_id or "").strip()
        query = safe_fts_query((query or "").strip())
        limit = max(1, min(int(limit or 5), 8))
        if not kind or not material_id or not query:
            return []
        with self._lock:
            if kind == "chunk":
                rows = self.con.execute(
                    """
                    SELECT c.chunk_id, c.summary, c.source_pages, c.emphasized, c.content_class, c.text, chunks_fts.rank
                    FROM chunks_fts
                    JOIN chunks c ON chunks_fts.rowid = c.rowid
                    WHERE chunks_fts MATCH ? AND c.material_id = ?
                    ORDER BY (chunks_fts.rank - CASE WHEN c.emphasized = 1 THEN 0.2 ELSE 0.0 END)
                    LIMIT ?
                    """,
                    [query, material_id, limit],
                ).fetchall()
                results = []
                for idx, row in enumerate(rows, 1):
                    results.append({
                        "kind": "chunk",
                        "chunk_id": row["chunk_id"],
                        "summary": row["summary"],
                        "source_pages": _parse_json_list(row["source_pages"]),
                        "emphasized": bool(row["emphasized"]),
                        "content_class": row["content_class"],
                        "rank": idx,
                        "text": row["text"] or "",
                        "snippet": _truncate(row["text"] or "", 360),
                    })
                return results
            if kind == "annotation":
                rows = self.con.execute(
                    """
                    SELECT a.annotation_id, a.type, a.quoted_text, a.comment, a.page, annotations_fts.rank
                    FROM annotations_fts
                    JOIN annotations a ON annotations_fts.rowid = a.rowid
                    WHERE annotations_fts MATCH ? AND a.material_id = ?
                    ORDER BY (CASE WHEN a.comment = '' THEN 1 ELSE 0 END), annotations_fts.rank
                    LIMIT ?
                    """,
                    [query, material_id, limit],
                ).fetchall()
                results = []
                for idx, row in enumerate(rows, 1):
                    results.append({
                        "kind": "annotation",
                        "annotation_id": row["annotation_id"],
                        "type": row["type"],
                        "quoted_text": row["quoted_text"],
                        "comment": row["comment"],
                        "page": row["page"],
                        "rank": idx,
                    })
                return results
            if kind == "figure":
                rows = self.con.execute(
                    """
                    SELECT f.figure_id, f.description, f.visual_type, f.source_page, f.image_path, figures_fts.rank
                    FROM figures_fts
                    JOIN figures f ON figures_fts.rowid = f.rowid
                    WHERE figures_fts MATCH ? AND f.material_id = ?
                    ORDER BY figures_fts.rank
                    LIMIT ?
                    """,
                    [query, material_id, limit],
                ).fetchall()
                results = []
                for idx, row in enumerate(rows, 1):
                    results.append({
                        "kind": "figure",
                        "figure_id": row["figure_id"],
                        "description": row["description"],
                        "visual_type": row["visual_type"],
                        "source_page": row["source_page"],
                        "image_path": row["image_path"],
                        "rank": idx,
                    })
                return results
        return []

    def search_concepts(self, query: str, limit: int = 5) -> list[dict]:
        from arquimedes.search import safe_fts_query
        query = safe_fts_query((query or "").strip())
        if not query:
            return []
        with self._lock:
            clusters = []
            try:
                rows = self.con.execute(
                    """
                    SELECT cc.cluster_id, cc.canonical_name, cc.slug, cc.aliases, cc.material_count,
                           cc.wiki_path
                    FROM local_concept_clusters_fts
                    JOIN local_concept_clusters cc ON local_concept_clusters_fts.rowid = cc.rowid
                    WHERE local_concept_clusters_fts MATCH ?
                    ORDER BY local_concept_clusters_fts.rank
                    LIMIT ?
                    """,
                    [query, limit],
                ).fetchall()
            except sqlite3.OperationalError:
                rows = []
            for row in rows:
                clusters.append({
                    "kind": "cluster",
                    "cluster_id": row["cluster_id"],
                    "canonical_name": row["canonical_name"],
                    "slug": row["slug"],
                    "aliases": _parse_json_list(row["aliases"]),
                    "material_count": row["material_count"] or 0,
                    "wiki_path": row["wiki_path"] or "",
                })
            try:
                rows = self.con.execute(
                    """
                    SELECT cc.cluster_id, cc.canonical_name, cc.slug, cc.aliases, cc.material_count,
                           cc.wiki_path
                    FROM concept_clusters_fts
                    JOIN concept_clusters cc ON concept_clusters_fts.rowid = cc.rowid
                    WHERE concept_clusters_fts MATCH ?
                    ORDER BY concept_clusters_fts.rank
                    LIMIT ?
                    """,
                    [query, limit],
                ).fetchall()
            except sqlite3.OperationalError:
                rows = []
            for row in rows:
                clusters.append({
                    "kind": "cluster",
                    "cluster_id": row["cluster_id"],
                    "canonical_name": row["canonical_name"],
                    "slug": row["slug"],
                    "aliases": _parse_json_list(row["aliases"]),
                    "material_count": row["material_count"] or 0,
                    "wiki_path": row["wiki_path"] or "",
                })
            concepts = []
            for row in self.con.execute(
                """
                SELECT co.concept_name, co.material_id, co.concept_type, co.concept_key, co.relevance, co.source_pages, co.evidence_spans, co.confidence
                FROM concepts_fts
                JOIN concepts co ON concepts_fts.rowid = co.rowid
                WHERE concepts_fts MATCH ?
                ORDER BY concepts_fts.rank
                LIMIT ?
                """,
                [query, limit],
            ).fetchall():
                concepts.append({
                    "kind": "concept",
                    "concept_name": row["concept_name"],
                    "material_id": row["material_id"],
                    "concept_type": row["concept_type"],
                    "concept_key": row["concept_key"],
                    "relevance": row["relevance"],
                    "source_pages": _parse_json_list(row["source_pages"]),
                    "evidence_spans": _parse_json_list(row["evidence_spans"]),
                    "confidence": row["confidence"] or 0.0,
                })
            return clusters + concepts

    def search_collections(self, query: str, limit: int = 5) -> list[dict]:
        query = (query or "").strip()
        if not query:
            return []
        with self._lock:
            try:
                rows = self.con.execute(
                    """
                    SELECT m.domain, m.collection, COUNT(*) AS material_count,
                           MAX(m.title) AS sample_title
                    FROM materials m
                    WHERE m.collection LIKE ? OR m.domain LIKE ? OR m.title LIKE ? OR m.summary LIKE ?
                          OR m.keywords LIKE ? OR m.raw_keywords LIKE ? OR m.authors LIKE ?
                    GROUP BY m.domain, m.collection
                    ORDER BY material_count DESC, m.domain, m.collection
                    LIMIT ?
                    """,
                    [f"%{query}%", f"%{query}%", f"%{query}%", f"%{query}%", f"%{query}%", f"%{query}%", f"%{query}%", limit],
                ).fetchall()
            except sqlite3.OperationalError:
                rows = []
            results = []
            for row in rows:
                domain = row["domain"] or "practice"
                collection = row["collection"] or "_general"
                wiki_path = f"wiki/{domain}/{collection}/_index.md"
                reflection = self.open_record("collection", f"{domain}/{collection}") or {}
                results.append({
                    "domain": domain,
                    "collection": collection,
                    "collection_key": f"{domain}/{collection}",
                    "title": f"{domain.replace('_', ' ').title()} / {collection.replace('_', ' ').title()}",
                    "sample_title": row["sample_title"] or "",
                    "material_count": row["material_count"] or 0,
                    "wiki_path": wiki_path,
                    "reflection": reflection.get("reflection", {}),
                })
            return results

    def open_record(self, kind: str, record_id: str) -> dict | None:
        kind = (kind or "").strip()
        record_id = (record_id or "").strip()
        if not kind or not record_id:
            return None
        with self._lock:
            if kind == "material":
                row = self.con.execute(
                """
                SELECT material_id, title, summary, domain, collection, document_type, year,
                       authors, keywords, raw_keywords
                FROM materials
                WHERE material_id = ?
                """,
                [record_id],
                ).fetchone()
                if row is None:
                    return None
                authors = []
                try:
                    authors = _parse_json_list(row["authors"])
                except Exception:
                    authors = []
                try:
                    keywords = _parse_json_list(row["keywords"])
                except Exception:
                    keywords = []
                try:
                    local_clusters = self.con.execute(
                        """
                        SELECT lcc.cluster_id, lcc.canonical_name, lcc.slug, lcc.wiki_path
                        FROM local_cluster_materials lcm
                        JOIN local_concept_clusters lcc ON lcm.cluster_id = lcc.cluster_id
                        WHERE lcm.material_id = ?
                        ORDER BY lcc.canonical_name, lcc.cluster_id
                        """,
                        [record_id],
                    ).fetchall()
                except sqlite3.OperationalError:
                    local_clusters = []
                return {
                    "kind": "material",
                    "material_id": row["material_id"],
                    "title": row["title"],
                    "summary": row["summary"],
                    "domain": row["domain"],
                    "collection": row["collection"],
                    "document_type": row["document_type"],
                    "year": row["year"],
                    "authors": authors,
                    "keywords": keywords,
                    "local_clusters": [
                        {
                            "cluster_id": cluster["cluster_id"],
                            "canonical_name": cluster["canonical_name"],
                            "slug": cluster["slug"],
                            "wiki_path": cluster["wiki_path"],
                        }
                        for cluster in local_clusters
                    ],
                    "evidence": self._material_evidence(record_id),
                }
            if kind == "concept":
                row = self.con.execute(
                """
                SELECT cluster_id, canonical_name, slug, aliases, confidence, wiki_path, material_count
                FROM local_concept_clusters
                WHERE cluster_id = ?
                """,
                [record_id],
                ).fetchone()
                if row is not None:
                    reflection = self.con.execute(
                    """
                    SELECT cluster_id, slug, canonical_name, main_takeaways, main_tensions,
                           open_questions, why_this_concept_matters, supporting_material_ids,
                           supporting_evidence, input_fingerprint, wiki_path
                    FROM concept_reflections
                    WHERE cluster_id = ?
                    """,
                    [record_id],
                    ).fetchone()
                    material_rows = self.con.execute(
                    """
                    SELECT material_id, relevance, source_pages, evidence_spans, confidence
                    FROM local_cluster_materials
                    WHERE cluster_id = ?
                    ORDER BY confidence DESC, material_id
                    """,
                    [record_id],
                    ).fetchall()
                    aliases = _parse_json_list(row["aliases"])
                    return {
                        "kind": "concept",
                        "cluster_id": row["cluster_id"],
                        "canonical_name": row["canonical_name"],
                        "slug": row["slug"],
                        "aliases": aliases,
                        "confidence": row["confidence"],
                        "wiki_path": row["wiki_path"],
                        "material_count": row["material_count"],
                        "cluster_materials": [
                            {
                                "material_id": r["material_id"],
                                "relevance": r["relevance"],
                                "source_pages": _parse_json_list(r["source_pages"]),
                                "evidence_spans": _parse_json_list(r["evidence_spans"]),
                                "confidence": r["confidence"],
                            }
                            for r in material_rows
                        ],
                        "reflection": self._row_to_dict(reflection) if reflection else {},
                    }
                row = self.con.execute(
                """
                SELECT cluster_id, canonical_name, slug, aliases, confidence, wiki_path, material_count
                FROM concept_clusters
                WHERE cluster_id = ?
                """,
                [record_id],
                ).fetchone()
                if row is None:
                    return None
                reflection = self.con.execute(
                """
                SELECT cluster_id, slug, canonical_name, main_takeaways, main_tensions,
                       open_questions, why_this_concept_matters, supporting_material_ids,
                       supporting_evidence, input_fingerprint, wiki_path
                FROM concept_reflections
                WHERE cluster_id = ?
                """,
                [record_id],
                ).fetchone()
                material_rows = self.con.execute(
                """
                SELECT material_id, relevance, source_pages, evidence_spans, confidence
                FROM cluster_materials
                WHERE cluster_id = ?
                ORDER BY confidence DESC, material_id
                """,
                [record_id],
                ).fetchall()
                aliases = _parse_json_list(row["aliases"])
                return {
                    "kind": "concept",
                    "cluster_id": row["cluster_id"],
                    "canonical_name": row["canonical_name"],
                    "slug": row["slug"],
                    "aliases": aliases,
                    "confidence": row["confidence"],
                    "wiki_path": row["wiki_path"],
                    "material_count": row["material_count"],
                    "cluster_materials": [
                        {
                            "material_id": r["material_id"],
                            "relevance": r["relevance"],
                            "source_pages": _parse_json_list(r["source_pages"]),
                            "evidence_spans": _parse_json_list(r["evidence_spans"]),
                            "confidence": r["confidence"],
                        }
                        for r in material_rows
                    ],
                    "reflection": self._row_to_dict(reflection) if reflection else {},
                }
            if kind == "collection":
                domain, _, collection = record_id.partition("/")
                domain = domain or "practice"
                collection = collection or "_general"
                wiki_row = self.con.execute(
                """
                SELECT page_type, page_id, title, path, domain, collection
                FROM wiki_pages
                WHERE page_type = 'collection' AND domain = ? AND collection = ?
                """,
                [domain, collection],
                ).fetchone()
                reflection = self.con.execute(
                """
                SELECT domain, collection, main_takeaways, main_tensions,
                       important_material_ids, important_cluster_ids,
                       open_questions, why_this_collection_matters,
                       input_fingerprint, wiki_path
                FROM collection_reflections
                WHERE domain = ? AND collection = ?
                """,
                [domain, collection],
                ).fetchone()
                members = self.con.execute(
                """
                SELECT material_id, title, summary
                FROM materials
                WHERE domain = ? AND collection = ?
                ORDER BY year DESC, title
                """,
                [domain, collection],
                ).fetchall()
                try:
                    local_clusters = self.con.execute(
                    """
                    SELECT cluster_id, canonical_name, slug, wiki_path, material_count
                    FROM local_concept_clusters
                    WHERE domain = ? AND collection = ?
                    ORDER BY canonical_name, cluster_id
                    """,
                    [domain, collection],
                    ).fetchall()
                except sqlite3.OperationalError:
                    local_clusters = []
                return {
                    "kind": "collection",
                    "domain": domain,
                    "collection": collection,
                    "wiki_page": self._row_to_dict(wiki_row) if wiki_row else {},
                    "local_clusters": [
                        {
                            "cluster_id": row["cluster_id"],
                            "canonical_name": row["canonical_name"],
                            "slug": row["slug"],
                            "wiki_path": row["wiki_path"],
                            "material_count": row["material_count"],
                        }
                        for row in local_clusters
                    ],
                    "members": [
                        {
                            "material_id": row["material_id"],
                            "title": row["title"],
                            "summary": _truncate(row["summary"] or "", 160),
                        }
                        for row in members
                    ],
                    "reflection": self._row_to_dict(reflection) if reflection else {},
                }
            return None

    def execute(self, request: dict) -> dict | None:
        tool = str(request.get("tool", "")).strip()
        if tool == "search_materials":
            query = str(request.get("query", "")).strip()
            limit = max(1, min(int(request.get("limit", 5) or 5), 8))
            return {"tool": tool, "query": query, "limit": limit, "results": self.search_materials(query, limit)}
        if tool == "search_material_evidence":
            kind = str(request.get("kind", "")).strip().lower()
            material_id = str(request.get("material_id", "")).strip()
            query = str(request.get("query", "")).strip()
            limit = max(1, min(int(request.get("limit", 5) or 5), 8))
            return {
                "tool": tool,
                "kind": kind,
                "material_id": material_id,
                "query": query,
                "limit": limit,
                "results": self.search_material_evidence(kind, material_id, query, limit),
            }
        if tool == "search_concepts":
            query = str(request.get("query", "")).strip()
            limit = max(1, min(int(request.get("limit", 5) or 5), 8))
            return {"tool": tool, "query": query, "limit": limit, "results": self.search_concepts(query, limit)}
        if tool == "search_collections":
            query = str(request.get("query", "")).strip()
            limit = max(1, min(int(request.get("limit", 5) or 5), 8))
            return {"tool": tool, "query": query, "limit": limit, "results": self.search_collections(query, limit)}
        if tool == "open_record":
            kind = str(request.get("kind", "")).strip()
            record_id = str(request.get("id", "")).strip()
            return {"tool": tool, "kind": kind, "id": record_id, "record": self.open_record(kind, record_id)}
        return None


def _execute_context_requests(tool: ReflectionIndexTool, requests: list[dict]) -> list[dict]:
    results: list[dict] = []
    for request in requests[:MAX_CONTEXT_REQUESTS_PER_PASS]:
        normalized = _normalize_context_request(request)
        if not normalized:
            continue
        executed = tool.execute(normalized)
        if executed is not None:
            results.append(executed)
    return results


def _normalize_context_request(request: Any) -> dict | None:
    if not isinstance(request, dict):
        return None
    tool = str(request.get("tool", "")).strip()
    if tool not in {"search_materials", "search_material_evidence", "search_concepts", "search_collections", "open_record"}:
        return None
    if tool == "search_material_evidence":
        kind = str(request.get("kind", "")).strip().lower()
        material_id = str(request.get("material_id", "")).strip()
        query = str(request.get("query", "")).strip()
        if kind not in {"chunk", "annotation", "figure"} or not material_id or not query:
            return None
        limit = request.get("limit", 5)
        try:
            limit_i = int(limit)
        except (TypeError, ValueError):
            limit_i = 5
        return {
            "tool": tool,
            "kind": kind,
            "material_id": material_id,
            "query": query,
            "limit": max(1, min(limit_i, 8)),
        }
    if tool == "open_record":
        kind = str(request.get("kind", "")).strip()
        record_id = str(request.get("id", "")).strip()
        if kind not in {"material", "concept", "collection"} or not record_id:
            return None
        return {"tool": tool, "kind": kind, "id": record_id}
    query = str(request.get("query", "")).strip()
    if not query:
        return None
    limit = request.get("limit", 5)
    try:
        limit_i = int(limit)
    except (TypeError, ValueError):
        limit_i = 5
    return {"tool": tool, "query": query, "limit": max(1, min(limit_i, 8))}


def _format_context_tool_results(results: list[dict]) -> str:
    return json.dumps(results, ensure_ascii=False, separators=(',', ':'))


def _extract_context_requests(parsed: Any) -> list[dict]:
    if isinstance(parsed, dict):
        requests = parsed.get("context_requests", [])
        if isinstance(requests, list):
            normalized = []
            for request in requests:
                req = _normalize_context_request(request)
                if req:
                    normalized.append(req)
            return normalized
    return []


def _run_reflection_prompt_with_context(
    llm_fn,
    system: str,
    user: str,
    schema_description: str,
    tool: ReflectionIndexTool,
    raw_response_recorder=None,
) -> Any:
    raw = llm_fn(system, [{"role": "user", "content": user}])
    if raw_response_recorder:
        raw_response_recorder(raw, "initial")
    parsed = parse_json_or_repair(llm_fn, raw, schema_description)
    requests = _extract_context_requests(parsed)
    context_requested = bool(requests)
    context_request_count = len(requests)
    if not requests:
        if isinstance(parsed, dict):
            parsed.pop("context_requests", None)
            parsed["context_requested"] = context_requested
            parsed["context_request_count"] = context_request_count
        return parsed

    tool_results = _execute_context_requests(tool, requests)
    followup_user = (
        "You requested more context from the read-only SQL-index tool.\n"
        "Tool results:\n"
        f"{_format_context_tool_results(tool_results)}\n\n"
        "Revise your answer using the added context. Return final JSON only."
    )
    raw = llm_fn(system, [
        {"role": "user", "content": user},
        {"role": "assistant", "content": raw},
        {"role": "user", "content": followup_user},
    ])
    if raw_response_recorder:
        raw_response_recorder(raw, "final")
    parsed = parse_json_or_repair(llm_fn, raw, schema_description)
    if isinstance(parsed, dict):
        parsed.pop("context_requests", None)
        parsed["context_requested"] = context_requested
        parsed["context_request_count"] = context_request_count
    return parsed


# ---------------------------------------------------------------------------
# Project loading
# ---------------------------------------------------------------------------

def _load_manifest(root: Path) -> list[dict]:
    return _load_jsonl(root / "manifests" / "materials.jsonl")


def _load_material_meta(root: Path, material_id: str) -> dict | None:
    path = root / "extracted" / material_id / "meta.json"
    return _load_json(path)


def _load_all_metas(root: Path, manifest_records: list[dict]) -> dict[str, dict]:
    metas: dict[str, dict] = {}
    for rec in manifest_records:
        mid = rec.get("material_id", "")
        if not mid:
            continue
        meta = _load_material_meta(root, mid)
        if meta is not None:
            metas[mid] = meta
    return metas


def _load_wiki_pages(wiki_root: Path) -> list[Path]:
    if not wiki_root.exists():
        return []
    return sorted(p for p in wiki_root.rglob("*.md") if p.is_file())


def _group_materials_by_collection(metas: dict[str, dict]) -> dict[tuple[str, str], list[dict]]:
    grouped: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for meta in metas.values():
        domain = (meta.get("domain") or "practice").strip() or "practice"
        collection = (meta.get("collection") or "").strip() or "_general"
        grouped[(domain, collection)].append(meta)
    return grouped


def _material_titles_from_metas(metas: dict[str, dict]) -> dict[str, str]:
    return {mid: (meta.get("title") or mid) for mid, meta in metas.items()}


def _current_concepts(root: Path) -> list[dict]:
    return load_local_clusters(root) or load_bridge_clusters(root)


def _cluster_scope(cluster: dict) -> tuple[str, str] | None:
    wiki_path = str(cluster.get("wiki_path", "") or "").strip()
    if "/bridge-concepts/" in wiki_path:
        return None
    domain = str(cluster.get("domain", "") or "").strip()
    collection = str(cluster.get("collection", "") or "").strip()
    cluster_id = str(cluster.get("cluster_id", "") or "").strip()
    if not (domain or collection or "__local_" in cluster_id or "/concepts/" in wiki_path):
        return None
    return _collection_scope(domain, collection)


def _clusters_are_local(clusters: list[dict]) -> bool:
    return any(_cluster_scope(cluster) is not None for cluster in clusters if isinstance(cluster, dict))


def _group_clusters_by_scope(clusters: list[dict]) -> dict[tuple[str, str], list[dict]]:
    grouped: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for cluster in clusters:
        if not isinstance(cluster, dict):
            continue
        scope = _cluster_scope(cluster)
        if scope is not None:
            grouped[scope].append(cluster)
    return grouped


def _local_audit_state_path(root: Path, domain: str, collection: str) -> Path:
    return local_cluster_dir(root, domain, collection) / LOCAL_AUDIT_STATE_NAME


def _local_audit_stamp_path(root: Path, domain: str, collection: str) -> Path:
    return local_cluster_dir(root, domain, collection) / LOCAL_AUDIT_STAMP_NAME


def _local_audit_gate_path(root: Path, domain: str, collection: str) -> Path:
    return local_cluster_dir(root, domain, collection) / ".audit.lock"


def _parallel_collection_audit_workers(config: dict | None = None) -> int:
    lint_cfg = _lint_schedule_config(config)
    try:
        return max(1, int(lint_cfg.get("parallel_collection_audits", 4) or 4))
    except (TypeError, ValueError):
        return 4


def _latest_local_clustered_at(root: Path) -> str:
    latest_dt: datetime | None = None
    latest_raw = ""
    for path in sorted((root / "derived" / "collections").glob("*/local_cluster_stamp.json")):
        clustered_at = str((_load_json(path, {}) or {}).get("clustered_at", "") or "")
        parsed = _parse_iso_datetime(clustered_at)
        if parsed is None:
            continue
        if latest_dt is None or parsed > latest_dt:
            latest_dt = parsed
            latest_raw = clustered_at
    return latest_raw


def _scope_material_info(material_info: dict[str, dict], domain: str, collection: str) -> dict[str, dict]:
    target_scope = _collection_scope(domain, collection)
    return {
        mid: info
        for mid, info in material_info.items()
        if isinstance(info, dict)
        and _collection_scope(info.get("domain", ""), info.get("collection", "")) == target_scope
    }


def _local_rows_in_scope_not_in_clusters(
    root: Path,
    domain: str,
    collection: str,
    scope_clusters: list[dict],
    material_info: dict[str, dict],
) -> tuple[list[tuple], list[tuple]]:
    local_rows, material_rows = _load_local_concepts(root)
    if not local_rows or not material_rows:
        return [], []
    target_scope = _collection_scope(domain, collection)
    scoped_local_rows = []
    for row in local_rows:
        try:
            _concept_name, _concept_key, material_id, _relevance, _source_pages, _evidence_spans, _confidence, _concept_type, _descriptor = _split_concept_row(row)
        except ValueError:
            continue
        info = material_info.get(material_id, {})
        if _collection_scope(info.get("domain", ""), info.get("collection", "")) == target_scope:
            scoped_local_rows.append(row)
    if not scoped_local_rows:
        return [], []
    pending_local_rows = _filter_local_rows_not_in_bridge(scoped_local_rows, scope_clusters)
    if not pending_local_rows:
        return [], []
    pending_material_ids = {str(row[2]).strip() for row in pending_local_rows if row and str(row[2]).strip()}
    pending_material_rows = [row for row in material_rows if row and str(row[0]).strip() in pending_material_ids]
    return pending_local_rows, pending_material_rows


def _load_local_concepts(root: Path) -> tuple[list[tuple], list[tuple]]:
    """Load local concept rows and material rows for bridge discovery."""
    db_path = get_index_path()
    if not db_path.exists():
        return [], []
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        concept_rows = _load_concept_rows(con, concept_type="local")
        material_rows = _load_material_rows(con)
    finally:
        con.close()
    return concept_rows, material_rows


def _filter_local_rows_not_in_bridge(
    local_rows: list[tuple],
    bridge_clusters: list[dict],
) -> list[tuple]:
    """Keep only local concepts not already represented in bridge memory."""
    covered_pairs = _cover_pairs_from_clusters(bridge_clusters)
    return [
        row for row in local_rows
        if row and (str(row[2]).strip(), str(row[1]).strip()) not in covered_pairs
    ]


def _local_rows_not_in_bridge(
    root: Path,
    bridge_clusters: list[dict],
) -> tuple[list[tuple], list[tuple]]:
    """Return local concept rows and matching materials not already covered by bridge clusters."""
    local_rows, material_rows = _load_local_concepts(root)
    if not local_rows or not material_rows:
        return [], []
    pending_local_rows = _filter_local_rows_not_in_bridge(local_rows, bridge_clusters)
    if not pending_local_rows:
        return [], []
    pending_material_ids = {
        str(row[2]).strip()
        for row in pending_local_rows
        if row and str(row[2]).strip()
    }
    pending_material_rows = [
        row for row in material_rows
        if row and str(row[0]).strip() in pending_material_ids
    ]
    return pending_local_rows, pending_material_rows


# ---------------------------------------------------------------------------
# Staleness helpers
# ---------------------------------------------------------------------------

def _index_state_stale(root: Path) -> tuple[bool, str]:
    index_path = get_index_path()
    if not index_path.exists():
        return True, "search index missing"

    manifest_path = root / "manifests" / "materials.jsonl"
    extracted_dir = root / "extracted"
    try:
        con = sqlite3.connect(f"file:{index_path}?mode=ro", uri=True)
        row = con.execute(
            "SELECT built_at, manifest_hash, material_count, extracted_snapshot FROM index_state WHERE id=1"
        ).fetchone()
        con.close()
    except sqlite3.Error:
        return True, "index_state missing or unreadable"

    if row is None:
        return True, "index_state missing"

    built_at_str, stored_manifest_hash, stored_count, stored_snapshot = row
    current_count = _count_manifest_lines(manifest_path)
    if current_count != stored_count:
        return True, "manifest material count changed"

    current_manifest_hash = _compute_manifest_hash(manifest_path)
    material_ids = _read_manifest_ids(manifest_path)
    current_snapshot = _compute_extracted_snapshot(extracted_dir, material_ids, root)
    if current_manifest_hash != stored_manifest_hash or current_snapshot != stored_snapshot:
        return True, "manifest or extracted snapshot changed"

    try:
        from datetime import datetime
        built_at = datetime.fromisoformat(built_at_str)
    except ValueError:
        return True, "invalid built_at timestamp"

    newest_mtime = _newest_input_mtime(extracted_dir)
    if newest_mtime is not None and newest_mtime > built_at.timestamp():
        return True, "extracted inputs newer than index_state"

    return False, ""


def _memory_state_stale(root: Path) -> tuple[bool, str]:
    stamp_path = root / "derived" / "memory_bridge_stamp.json"
    if not stamp_path.exists():
        return True, "memory stamp missing"

    stamp = _load_json(stamp_path, {}) or {}
    clusters_fp = _cluster_fingerprint(root)
    manifest_fp = _fingerprint_file(root / "manifests" / "materials.jsonl")
    if stamp.get("clusters_fingerprint") != clusters_fp:
        return True, "cluster or reflection artifacts changed"
    if stamp.get("manifest_fingerprint") != manifest_fp:
        return True, "manifest changed"
    return False, ""


# ---------------------------------------------------------------------------
# Deterministic lint
# ---------------------------------------------------------------------------

def _detect_missing_metadata(root: Path, manifest_records: list[dict]) -> list[dict]:
    issues: list[dict] = []
    required_fields = [
        "material_id",
        "file_hash",
        "source_path",
        "title",
        "domain",
        "collection",
        "page_count",
        "file_type",
        "raw_document_type",
    ]
    for rec in manifest_records:
        mid = rec.get("material_id", "")
        meta = _load_material_meta(root, mid)
        if meta is None:
            issues.append(_issue(
                "missing_extracted_material",
                "high",
                "Missing extracted metadata",
                "Material exists in the manifest but extracted/meta.json is missing.",
                material_id=mid,
                path=f"extracted/{mid}/meta.json",
                fixable=True,
            ))
            continue
        missing = [field for field in required_fields if meta.get(field) in (None, "", [], {})]
        if missing:
            issues.append(_issue(
                "missing_metadata",
                "high",
                "Missing required metadata",
                f"Missing fields: {', '.join(missing)}",
                material_id=mid,
                path=f"extracted/{mid}/meta.json",
                fixable=True,
            ))
        if not (root / "extracted" / mid / "pages.jsonl").exists():
            issues.append(_issue(
                "missing_extracted_artifact",
                "high",
                "Missing pages.jsonl",
                "Extraction output is incomplete; pages.jsonl is missing.",
                material_id=mid,
                path=f"extracted/{mid}/pages.jsonl",
                fixable=True,
            ))
        if not (root / "extracted" / mid / "chunks.jsonl").exists():
            issues.append(_issue(
                "missing_extracted_artifact",
                "high",
                "Missing chunks.jsonl",
                "Extraction output is incomplete; chunks.jsonl is missing.",
                material_id=mid,
                path=f"extracted/{mid}/chunks.jsonl",
                fixable=True,
            ))
    return issues


def _detect_orphaned_extracted_materials(root: Path, manifest_records: list[dict]) -> list[dict]:
    manifest_ids = {rec.get("material_id", "") for rec in manifest_records if rec.get("material_id")}
    issues: list[dict] = []
    extracted_root = root / "extracted"
    if not extracted_root.exists():
        return issues
    for mat_dir in sorted(p for p in extracted_root.iterdir() if p.is_dir()):
        if mat_dir.name not in manifest_ids:
            issues.append(_issue(
                "orphaned_material",
                "medium",
                "Orphaned extracted material",
                "An extracted material exists on disk but is not present in the manifest.",
                material_id=mat_dir.name,
                path=str(mat_dir.relative_to(root)),
                fixable=False,
            ))
    return issues


def _detect_duplicates(manifest_records: list[dict]) -> list[dict]:
    issues: list[dict] = []
    by_hash: dict[str, list[str]] = defaultdict(list)
    by_path: dict[str, list[str]] = defaultdict(list)
    for rec in manifest_records:
        mid = rec.get("material_id", "")
        if not mid:
            continue
        file_hash = str(rec.get("file_hash", "")).strip()
        rel_path = str(rec.get("relative_path", "")).strip()
        if file_hash:
            by_hash[file_hash].append(mid)
        if rel_path:
            by_path[rel_path.lower()].append(mid)

    for file_hash, mids in by_hash.items():
        if len(mids) > 1:
            issues.append(_issue(
                "duplicate_material",
                "medium",
                "Duplicate material hash",
                f"Multiple manifest entries share file_hash={file_hash}.",
                material_id=",".join(sorted(mids)),
                fixable=False,
            ))
    for rel_path, mids in by_path.items():
        if len(mids) > 1:
            issues.append(_issue(
                "duplicate_material",
                "medium",
                "Duplicate material path",
                f"Multiple manifest entries share relative_path={rel_path}.",
                material_id=",".join(sorted(mids)),
                fixable=False,
            ))
    return issues


def _detect_stale_enrichment(root: Path, manifest_records: list[dict], config: dict) -> list[dict]:
    issues: list[dict] = []
    for rec in manifest_records:
        mid = rec.get("material_id", "")
        if not mid:
            continue
        output_dir = root / "extracted" / mid
        if not output_dir.exists():
            continue
        stale_stages: list[str] = []
        try:
            if _is_document_stale(output_dir, config):
                stale_stages.append("document")
        except Exception:
            stale_stages.append("document")
        try:
            if _is_chunk_stale(output_dir, config):
                stale_stages.append("chunk")
        except Exception:
            stale_stages.append("chunk")
        try:
            if _is_figure_stale(output_dir, config):
                stale_stages.append("figure")
        except Exception:
            stale_stages.append("figure")
        if stale_stages:
            issues.append(_issue(
                "stale_enrichment",
                "medium",
                "Stale enrichment",
                f"Stages needing refresh: {', '.join(stale_stages)}",
                material_id=mid,
                path=f"extracted/{mid}",
                fixable=True,
            ))
    return issues


def _detect_broken_links(root: Path, wiki_pages: list[Path]) -> list[dict]:
    issues: list[dict] = []
    for page in wiki_pages:
        text = _read_text(page)
        for target in _extract_links(text):
            resolved = _resolve_wiki_link(page, target, root / "wiki", root)
            if resolved is None:
                continue
            if not resolved.exists():
                issues.append(_issue(
                    "broken_link",
                    "high",
                    "Broken wiki link",
                    f"Link target does not exist: {target}",
                    path=str(page.relative_to(root)),
                    target=target,
                    fixable=False,
                ))
    return issues


def _detect_missing_compiled_pages(
    root: Path,
    manifest_records: list[dict],
    metas: dict[str, dict],
    clusters: list[dict],
) -> list[dict]:
    issues: list[dict] = []
    bridge_clusters = load_bridge_clusters(root)

    # Material pages
    for rec in manifest_records:
        mid = rec.get("material_id", "")
        meta = metas.get(mid)
        if not mid or meta is None:
            continue
        page_path = root / _material_wiki_path(meta)
        if not page_path.exists():
            issues.append(_issue(
                "missing_compiled_page",
                "high",
                "Missing material page",
                "Compiled material page is missing.",
                material_id=mid,
                path=str(page_path.relative_to(root)),
                fixable=True,
            ))

    # Collection pages
    grouped = _group_materials_by_collection(metas)
    for (domain, collection), _metas in grouped.items():
        page_path = root / f"wiki/{domain}/{collection}/_index.md"
        if not page_path.exists():
            issues.append(_issue(
                "missing_compiled_page",
                "high",
                "Missing collection page",
                "Compiled collection _index.md page is missing.",
                domain=domain,
                collection=collection,
                path=str(page_path.relative_to(root)),
                fixable=True,
            ))
    for domain in sorted({meta.get("domain") or "practice" for meta in metas.values()}):
        page_path = root / f"wiki/{domain}/_index.md"
        if not page_path.exists():
            issues.append(_issue(
                "missing_compiled_page",
                "high",
                "Missing domain index",
                "Domain _index.md page is missing.",
                domain=domain,
                path=str(page_path.relative_to(root)),
                fixable=True,
            ))

    for page_path in [
        root / "wiki" / "_index.md",
        root / "wiki" / "shared" / "concepts" / "_index.md",
        root / "wiki" / "shared" / "glossary" / "_index.md",
    ]:
        if not page_path.exists():
            issues.append(_issue(
                "missing_compiled_page",
                "high",
                "Missing structural index",
                "Required wiki index page is missing.",
                path=str(page_path.relative_to(root)),
                fixable=True,
            ))

    # Concept pages
    for cluster in bridge_clusters:
        page_path = root / _concept_page_path(cluster)
        if not page_path.exists():
            issues.append(_issue(
                "missing_compiled_page",
                "high",
                "Missing concept page",
                "Compiled concept page is missing.",
                cluster_id=cluster.get("cluster_id", ""),
                path=str(page_path.relative_to(root)),
                fixable=True,
            ))

    return issues


def _detect_orphaned_wiki_pages(
    root: Path,
    wiki_pages: list[Path],
    expected_pages: set[Path],
) -> list[dict]:
    issues: list[dict] = []
    for page in wiki_pages:
        if page.relative_to(root) == REPORT_PATH:
            continue
        if page not in expected_pages:
            issues.append(_issue(
                "orphaned_wiki_page",
                "medium",
                "Orphaned wiki page",
                "Wiki page does not correspond to any expected material, concept, or index page.",
                path=str(page.relative_to(root)),
                fixable=False,
            ))
    return issues


def _expected_pages(
    root: Path,
    manifest_records: list[dict],
    metas: dict[str, dict],
    clusters: list[dict],
) -> set[Path]:
    expected: set[Path] = set()
    bridge_clusters = load_bridge_clusters(root)
    # Material pages
    for meta in metas.values():
        expected.add(root / _material_wiki_path(meta))
    # Collection and domain indexes
    grouped = _group_materials_by_collection(metas)
    for (domain, collection), _metas in grouped.items():
        expected.add(root / f"wiki/{domain}/{collection}/_index.md")
    for domain in sorted({meta.get("domain") or "practice" for meta in metas.values()}):
        expected.add(root / f"wiki/{domain}/_index.md")
    expected.add(root / "wiki" / "_index.md")
    expected.add(root / "wiki" / "shared" / "concepts" / "_index.md")
    expected.add(root / "wiki" / "shared" / "glossary" / "_index.md")
    # Concept pages
    for cluster in bridge_clusters:
        expected.add(root / _concept_page_path(cluster))
    # Phase 6 report itself is expected when written
    expected.add(root / REPORT_PATH)
    return expected


def run_deterministic_lint(config: dict | None = None) -> dict:
    """Run deterministic lint checks and write the JSON report."""
    if config is None:
        config = load_config()
    root = get_project_root()
    wiki_root = root / "wiki"
    manifest_records = _load_manifest(root)
    metas = _load_all_metas(root, manifest_records)
    clusters = _current_concepts(root)
    wiki_pages = _load_wiki_pages(wiki_root)
    expected_pages = _expected_pages(root, manifest_records, metas, clusters)

    issues: list[dict] = []
    issues.extend(_detect_missing_metadata(root, manifest_records))
    issues.extend(_detect_orphaned_extracted_materials(root, manifest_records))
    issues.extend(_detect_duplicates(manifest_records))
    issues.extend(_detect_stale_enrichment(root, manifest_records, config))
    issues.extend(_detect_broken_links(root, wiki_pages))
    issues.extend(_detect_missing_compiled_pages(root, manifest_records, metas, clusters))
    issues.extend(_detect_orphaned_wiki_pages(root, wiki_pages, expected_pages))

    index_stale, index_reason = _index_state_stale(root)
    if index_stale:
        issues.append(_issue(
            "stale_index",
            "high",
            "Stale search index",
            index_reason or "search index needs rebuild",
            path="indexes/search.sqlite",
            fixable=True,
        ))

    memory_stale, memory_reason = _memory_state_stale(root)
    if memory_stale:
        issues.append(_issue(
            "stale_memory_bridge",
            "medium",
            "Stale memory bridge",
            memory_reason or "memory bridge needs rebuild",
            path="indexes/search.sqlite",
            fixable=True,
        ))

    summary = {
        "materials": len(manifest_records),
        "extracted_materials": len(metas),
        "wiki_pages": len(wiki_pages),
        "clusters": len(clusters),
        "issues": len(issues),
        "high": sum(1 for issue in issues if issue["severity"] == "high"),
        "medium": sum(1 for issue in issues if issue["severity"] == "medium"),
        "low": sum(1 for issue in issues if issue["severity"] == "low"),
    }

    report = {
        "checked_at": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
        "project_root": str(root),
        "summary": summary,
        "issues": issues,
        "checks": {
            "index_stale": index_stale,
            "memory_stale": memory_stale,
        },
    }

    _write_json(root / LINT_DIR / "deterministic_report.json", report)
    return report


def render_lint_report(report: dict) -> str:
    """Render a human-readable markdown lint report."""
    lines: list[str] = []
    lines.append("# Arquimedes Lint Report\n")
    lines.append(f"_Checked at {report.get('checked_at', '')}_\n")

    summary = report.get("summary", {})
    lines.append("## Summary\n")
    lines.append(f"- Materials: {summary.get('materials', 0)}")
    lines.append(f"- Extracted materials: {summary.get('extracted_materials', 0)}")
    lines.append(f"- Wiki pages: {summary.get('wiki_pages', 0)}")
    lines.append(f"- Clusters: {summary.get('clusters', 0)}")
    lines.append(f"- Issues: {summary.get('issues', 0)}")
    lines.append("")

    issues = report.get("issues", [])
    if not issues:
        lines.append("## Findings\n")
        lines.append("- No deterministic issues found.\n")
        return "\n".join(lines)

    for severity in ("high", "medium", "low"):
        bucket = [i for i in issues if i.get("severity") == severity]
        if not bucket:
            continue
        lines.append(f"## {severity.title()} Severity\n")
        for issue in bucket:
            path = issue.get("path", "")
            title = issue.get("title", "")
            detail = issue.get("detail", "")
            check = issue.get("check", "")
            if path:
                lines.append(f"- **{title}** (`{check}`) — `{path}`")
            else:
                lines.append(f"- **{title}** (`{check}`)")
            if detail:
                lines.append(f"  - {detail}")
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Deterministic fixes
# ---------------------------------------------------------------------------

def _compile_is_safe(root: Path) -> bool:
    return not is_bridge_clustering_stale(None)


def _apply_deterministic_fixes(report: dict, config: dict) -> dict:
    """Apply safe deterministic fixes only."""
    root = get_project_root()
    fixes = {
        "index_rebuilt": False,
        "memory_rebuilt": False,
        "compiled": False,
        "details": [],
    }

    issues = report.get("issues", [])
    if any(issue["check"] in {"stale_index", "stale_memory_bridge"} for issue in issues):
        from arquimedes.index import ensure_index_and_memory
        index_rebuilt, _stats, memory_rebuilt, _memory_counts = ensure_index_and_memory(config)
        fixes["index_rebuilt"] = index_rebuilt
        fixes["memory_rebuilt"] = memory_rebuilt
        if index_rebuilt:
            fixes["details"].append("rebuild index")
        if memory_rebuilt:
            fixes["details"].append("rebuild memory")

    missing_pages = [issue for issue in issues if issue["check"] == "missing_compiled_page"]
    if missing_pages and _compile_is_safe(root):
        compile_wiki(config)
        fixes["compiled"] = True
        fixes["details"].append("recompile wiki")

    return fixes


# ---------------------------------------------------------------------------
# Reflection passes
# ---------------------------------------------------------------------------

def _existing_by_key(path: Path, key_field: str) -> dict[str, dict]:
    existing = {}
    for row in _load_jsonl(path):
        key = str(row.get(key_field, "")).strip()
        if key:
            existing[key] = row
    return existing


def _current_clustered_at(root: Path) -> str:
    local_clustered_at = _latest_local_clustered_at(root)
    if local_clustered_at:
        return local_clustered_at
    stamp = _read_stamp(root / "derived" / "bridge_cluster_stamp.json")
    if not stamp:
        return ""
    return str(stamp.get("clustered_at", "") or "")


def _lint_stage_stamp(root: Path) -> dict:
    return _read_stamp(root / LINT_STAGE_STAMP_PATH)


def _write_lint_stage_stamp(root: Path, **updates: str) -> None:
    stamp = _lint_stage_stamp(root)
    stamp.update({key: value for key, value in updates.items() if value})
    _write_stamp(root / LINT_STAGE_STAMP_PATH, stamp)


def _bridge_cluster_stage_due(root: Path, stamp_key: str, artifact_path: Path, stage_label: str) -> tuple[bool, str]:
    current_clustered_at = _current_clustered_at(root)
    if not current_clustered_at:
        return False, "bridge clustering has not run yet"

    if not artifact_path.exists():
        return True, f"{stage_label} artifact missing"

    stage_at = str(_lint_stage_stamp(root).get(stamp_key, "") or "")
    if not stage_at:
        return True, f"{stage_label} stamp missing"

    clustered_at_dt = _parse_iso_datetime(current_clustered_at)
    stage_at_dt = _parse_iso_datetime(stage_at)
    if clustered_at_dt is None or stage_at_dt is None:
        return True, f"{stage_label} stamp invalid"
    if stage_at_dt >= clustered_at_dt:
        return False, f"{stage_label} already ran after latest clustering"

    return True, f"latest clustering is newer than {stage_label}"


def _cluster_audit_due(root: Path, _bridge_clusters: list[dict]) -> tuple[bool, str]:
    local_groups = _group_clusters_by_scope(_current_concepts(root))
    if local_groups:
        for (domain, collection), scope_clusters in sorted(local_groups.items()):
            if not scope_clusters:
                continue
            cluster_stamp = _read_stamp(local_cluster_stamp_path(root, domain, collection))
            clustered_at = _parse_iso_datetime(cluster_stamp.get("clustered_at"))
            audit_stamp = _read_stamp(_local_audit_stamp_path(root, domain, collection))
            audited_at = _parse_iso_datetime(audit_stamp.get("audited_at"))
            if audited_at is None:
                return True, f"local audit stamp missing for {domain}/{collection}"
            if clustered_at is None:
                return True, f"local cluster stamp missing for {domain}/{collection}"
            if audited_at < clustered_at:
                return True, f"latest clustering is newer than local audit for {domain}/{collection}"
        return False, "local audit already ran after latest clustering"
    artifact_path = root / LINT_DIR / "cluster_reviews.jsonl"
    return _bridge_cluster_stage_due(
        root,
        "audited_at",
        artifact_path,
        "cluster audit",
    )


def _concept_reflection_due(root: Path) -> tuple[bool, str]:
    return _bridge_cluster_stage_due(
        root,
        "concept_reflection_at",
        root / LINT_DIR / "concept_reflections.jsonl",
        "concept reflection",
    )


def _collection_reflection_due(root: Path) -> tuple[bool, str]:
    return _bridge_cluster_stage_due(
        root,
        "collection_reflection_at",
        root / LINT_DIR / "collection_reflections.jsonl",
        "collection reflection",
    )


def _graph_reflection_due(
    root: Path,
    config: dict,
    clusters: list[dict],
    manifest_records: list[dict],
    cluster_reviews: list[dict],
    concept_refs: list[dict],
    collection_refs: list[dict],
    deterministic_report: dict,
) -> tuple[bool, str]:
    stage_due, stage_reason = _bridge_cluster_stage_due(
        root,
        "graph_reflection_at",
        root / LINT_DIR / "graph_findings.jsonl",
        "graph reflection",
    )
    if not stage_due:
        return False, stage_reason

    stamp = _read_stamp(root / GRAPH_REFLECTION_STAMP_PATH)
    findings_path = root / LINT_DIR / "graph_findings.jsonl"
    payload = _graph_reflection_packet(
        deterministic_report,
        clusters,
        cluster_reviews,
        concept_refs,
        collection_refs,
        manifest_records,
    )
    current_fp = canonical_hash(payload)
    if not findings_path.exists():
        return True, stage_reason
    if not stamp:
        return True, "graph reflection stamp missing"
    if stamp.get("graph_fingerprint") == current_fp:
        return False, "graph reflection unchanged"

    schedule_cfg = _lint_schedule_config(config).get("graph_schedule", {})
    min_hours = float(schedule_cfg.get("min_interval_hours", DEFAULT_GRAPH_REFLECTION_INTERVAL_HOURS) or DEFAULT_GRAPH_REFLECTION_INTERVAL_HOURS)
    min_cluster_delta = int(schedule_cfg.get("min_cluster_delta", DEFAULT_GRAPH_REFLECTION_MIN_CLUSTER_DELTA) or DEFAULT_GRAPH_REFLECTION_MIN_CLUSTER_DELTA)
    min_material_delta = int(schedule_cfg.get("min_material_delta", DEFAULT_GRAPH_REFLECTION_MIN_MATERIAL_DELTA) or DEFAULT_GRAPH_REFLECTION_MIN_MATERIAL_DELTA)

    checked_at = _parse_iso_datetime(stamp.get("checked_at"))
    if checked_at is None:
        return True, "graph reflection stamp invalid"
    age_hours = (datetime.now(timezone.utc) - checked_at).total_seconds() / 3600.0
    cluster_delta = abs(len(clusters) - int(stamp.get("cluster_count", 0) or 0))
    material_delta = abs(len(manifest_records) - int(stamp.get("material_count", 0) or 0))

    if cluster_delta >= min_cluster_delta or material_delta >= min_material_delta:
        return True, "enough graph change accumulated"
    if age_hours >= min_hours:
        return True, "graph reflection periodic interval reached"
    return False, "graph reflection deferred by schedule"
def _concept_reflection_stage_dir(root: Path) -> Path:
    return root / "derived" / "tmp" / "concept_reflections"


def _concept_reflection_page_copy_path(root: Path, cluster_id: str) -> Path:
    safe_cluster_id = (cluster_id or "cluster").strip() or "cluster"
    return _concept_reflection_stage_dir(root) / f"{safe_cluster_id}.page.md"


def _concept_reflection_evidence_path(root: Path, cluster_id: str) -> Path:
    safe_cluster_id = (cluster_id or "cluster").strip() or "cluster"
    return _concept_reflection_stage_dir(root) / f"{safe_cluster_id}.evidence.json"


def _collect_material_chunk_evidence(
    tool: ReflectionIndexTool | None,
    material_id: str,
    query_terms: list[str],
    chunk_limit: int,
) -> list[dict]:
    if not tool or chunk_limit <= 0:
        return []
    selected = []
    seen: set[str] = set()
    search_terms = [term for term in (str(t).strip() for t in query_terms) if term]
    for query in search_terms:
        try:
            rows = tool.search_material_evidence("chunk", material_id, query, limit=chunk_limit)
        except Exception:
            rows = []
        for row in rows:
            chunk_id = str(row.get("chunk_id", "")).strip()
            if not chunk_id or chunk_id in seen:
                continue
            item = dict(row)
            item.setdefault("text", item.get("snippet", ""))
            selected.append({
                "chunk_id": chunk_id,
                "text": item.get("text", ""),
                "source": "search",
            })
            seen.add(chunk_id)
            if len(selected) >= chunk_limit:
                return selected
    if selected:
        if len(selected) >= chunk_limit:
            return selected[:chunk_limit]
    fallback = tool._material_evidence(material_id, query_terms, chunk_limit=chunk_limit)
    for row in list(fallback.get("chunks", [])):
        if not isinstance(row, dict):
            continue
        chunk_id = str(row.get("chunk_id", "")).strip()
        if not chunk_id or chunk_id in seen:
            continue
        selected.append({
            "chunk_id": str(row.get("chunk_id", "")).strip(),
            "text": str(row.get("text") or row.get("excerpt") or "").strip(),
            "source": "fallback",
        })
        seen.add(chunk_id)
        if len(selected) >= chunk_limit:
            break
    return selected[:chunk_limit]


def _format_material_annotation(row: dict) -> str:
    parts = []
    page = str(row.get("page", "")).strip()
    quoted_text = str(row.get("quoted_text", "")).strip()
    comment = str(row.get("comment", "")).strip()
    if page:
        parts.append(f"p. {page}")
    if quoted_text:
        parts.append(quoted_text)
    if comment:
        parts.append(comment)
    return " — ".join(parts).strip()


def _format_material_concept(row: dict) -> str:
    concept_name = str(row.get("concept_name", "")).strip()
    spans = [
        span
        for span in (_safe_list(row.get("evidence_spans", [])))
        if span
    ]
    if spans:
        return f"{concept_name} ({', '.join(spans)})".strip()
    return concept_name


def _build_concept_reflection_evidence_payload(
    cluster: dict,
    material_info: dict[str, dict],
    tool: ReflectionIndexTool | None = None,
) -> dict:
    source_concepts = cluster.get("source_concepts", [])
    unique_material_ids = []
    seen: set[str] = set()
    for sc in sorted(
        source_concepts,
        key=lambda x: (
            x.get("material_id", ""),
            -float(x.get("confidence", 0.0) or 0.0),
        ),
    ):
        mid = sc.get("material_id", "")
        if mid and mid not in seen:
            seen.add(mid)
            unique_material_ids.append(mid)

    query_terms = [
        cluster.get("canonical_name", ""),
        *(_safe_list(cluster.get("aliases", []))),
        *[sc.get("concept_name", "") for sc in source_concepts if sc.get("concept_name")],
    ]

    material_weights: dict[str, float] = defaultdict(float)
    for sc in source_concepts:
        mid = sc.get("material_id", "")
        if not mid:
            continue
        confidence = float(sc.get("confidence", 0.0) or 0.0)
        relevance = str(sc.get("relevance", "")).strip().lower()
        material_weights[mid] += confidence
        material_weights[mid] += {
            "high": 0.5,
            "medium": 0.25,
            "low": 0.1,
        }.get(relevance, 0.0)
        if sc.get("concept_type") == "bridge":
            material_weights[mid] += 0.2
        material_weights[mid] += min(len(_safe_list(sc.get("evidence_spans", []))), 4) * 0.02
    ordered_material_ids = sorted(unique_material_ids, key=lambda mid: (material_weights.get(mid, 0.0), mid), reverse=True)
    chunk_limits: dict[str, int] = {}
    if ordered_material_ids:
        base = CONCEPT_REFLECTION_TOTAL_CHUNK_BUDGET // len(ordered_material_ids)
        remainder = CONCEPT_REFLECTION_TOTAL_CHUNK_BUDGET % len(ordered_material_ids)
        if base > 0:
            for idx, mid in enumerate(ordered_material_ids):
                chunk_limits[mid] = min(
                    CONCEPT_REFLECTION_MAX_CHUNKS_PER_MATERIAL,
                    base + (1 if idx < remainder else 0),
                )
        else:
            for idx, mid in enumerate(ordered_material_ids):
                chunk_limits[mid] = 1 if idx < CONCEPT_REFLECTION_TOTAL_CHUNK_BUDGET else 0

    materials = []
    for mid in ordered_material_ids:
        info = material_info.get(mid, {})
        chunk_limit = chunk_limits.get(mid, 0)
        chunk_evidence = _collect_material_chunk_evidence(tool, mid, query_terms, chunk_limit)
        evidence = (
            tool._material_evidence(
                mid,
                query_terms,
                chunk_limit=chunk_limit,
                annotation_limit=CONCEPT_REFLECTION_MAX_ANNOTATIONS_PER_MATERIAL,
                figure_limit=CONCEPT_REFLECTION_MAX_FIGURES_PER_MATERIAL,
                concept_limit=CONCEPT_REFLECTION_MAX_CONCEPTS_PER_MATERIAL,
            )
            if tool
            else {}
        )
        if isinstance(evidence, dict):
            annotations = [
                line
                for line in (
                    _format_material_annotation(ann)
                    for ann in list(evidence.get("annotations", []))
                    if isinstance(ann, dict)
                )
                if line
            ]
            evidence_payload = {
                "chunks": [
                    {
                        "chunk_id": str(chunk.get("chunk_id", "")).strip(),
                        "text": str(chunk.get("text", "")).strip(),
                        "source": str(chunk.get("source", "")).strip() or "fallback",
                    }
                    for chunk in chunk_evidence
                    if isinstance(chunk, dict) and str(chunk.get("text", "")).strip()
                ],
                "figures": [
                    {
                        "figure_id": str(fig.get("figure_id", "")).strip(),
                        "description": str(fig.get("description", "")).strip(),
                    }
                    for fig in list(evidence.get("figures", []))
                    if isinstance(fig, dict)
                ],
                "concepts": [
                    _format_material_concept(con)
                    for con in list(evidence.get("concepts", []))
                    if isinstance(con, dict) and str(con.get("concept_name", "")).strip()
                ],
            }
            if annotations:
                evidence_payload["annotations"] = "\n".join(annotations)
            evidence = evidence_payload
        materials.append({
            "material_id": mid,
            "title": info.get("title", mid),
            "summary": info.get("summary", ""),
            "keywords": info.get("keywords", []),
            "evidence": evidence,
        })

    return {
        "cluster_id": cluster.get("cluster_id", ""),
        "canonical_name": cluster.get("canonical_name", ""),
        "slug": cluster.get("slug", ""),
        "aliases": cluster.get("aliases", []),
        "materials": materials,
    }


def _concept_reflection_scaffold(
    cluster: dict,
    fingerprint: str,
    root: Path,
) -> dict:
    source_concepts = cluster.get("source_concepts", [])
    supporting_material_ids = sorted({
        sc.get("material_id", "")
        for sc in source_concepts
        if sc.get("material_id", "")
    })
    supporting_evidence = sorted({
        span
        for sc in source_concepts
        for span in _safe_list(sc.get("evidence_spans", []))
    })
    return {
        "cluster_id": cluster.get("cluster_id", ""),
        "slug": cluster.get("slug", ""),
        "canonical_name": cluster.get("canonical_name", ""),
        "main_takeaways": [],
        "main_tensions": [],
        "open_questions": [],
        "why_this_concept_matters": "",
        "supporting_material_ids": supporting_material_ids,
        "supporting_evidence": supporting_evidence,
        "input_fingerprint": fingerprint,
        "wiki_path": str(_concept_page_path(cluster)),
    }


def _compile_concept_reflection_response(
    cluster: dict,
    scaffold: dict,
    fingerprint: str,
    parsed: Any,
    existing_record: dict | None = None,
) -> dict:
    if not isinstance(parsed, dict):
        raise EnrichmentError("Concept reflection output must be a JSON object")
    if parsed.get("_finished") is not True:
        raise EnrichmentError("Concept reflection output missing _finished=true")

    def _resolve_list_field(field: str) -> list[str]:
        value = parsed.get(field)
        if value is None:
            return _dedupe_strings(_safe_list((existing_record or {}).get(field, scaffold.get(field, []))))
        if not isinstance(value, list):
            raise EnrichmentError(f"Concept reflection output field '{field}' must be a list or null")
        return _dedupe_strings(_safe_list(value))

    def _resolve_text_field(field: str) -> str:
        value = parsed.get(field)
        if value is None:
            return str((existing_record or {}).get(field, scaffold.get(field, "")) or "").strip()
        if not isinstance(value, str):
            raise EnrichmentError(f"Concept reflection output field '{field}' must be a string or null")
        return value.strip()

    for field in ("main_takeaways", "main_tensions", "open_questions"):
        _resolve_list_field(field)
    _resolve_text_field("why_this_concept_matters")
    return {
        **scaffold,
        "cluster_id": cluster.get("cluster_id", ""),
        "slug": cluster.get("slug", ""),
        "canonical_name": cluster.get("canonical_name", ""),
        "main_takeaways": _resolve_list_field("main_takeaways"),
        "main_tensions": _resolve_list_field("main_tensions"),
        "open_questions": _resolve_list_field("open_questions"),
        "why_this_concept_matters": _resolve_text_field("why_this_concept_matters"),
        "input_fingerprint": fingerprint,
        "wiki_path": str(scaffold.get("wiki_path", "")).strip(),
    }


def _concept_reflection_link_fingerprint(cluster: dict) -> str:
    linked = []
    seen: set[tuple[str, str]] = set()
    for sc in cluster.get("source_concepts", []):
        if not isinstance(sc, dict):
            continue
        mid = str(sc.get("material_id", "")).strip()
        cname = str(sc.get("concept_name", "")).strip()
        if not mid or not cname:
            continue
        key = (mid, cname)
        if key in seen:
            continue
        seen.add(key)
        linked.append({"material_id": mid, "concept_name": cname})
    linked.sort(key=lambda item: (item["material_id"], item["concept_name"]))
    return canonical_hash(linked)


def _cluster_audit_input_path(root: Path) -> Path:
    return root / LINT_DIR / "cluster_audit_input.json"


def _cluster_audit_raw_response_path(root: Path, phase: str = "final") -> Path:
    suffix = phase.strip() or "final"
    return root / LINT_DIR / f"cluster_audit_last_response.{suffix}.txt"


def _cluster_audit_parsed_response_path(root: Path) -> Path:
    return root / LINT_DIR / "cluster_audit_last_response.parsed.json"


def _cleanup_cluster_audit_debug_artifacts(root: Path) -> None:
    for path in (
        _cluster_audit_raw_response_path(root, "initial"),
        _cluster_audit_raw_response_path(root, "final"),
        _cluster_audit_parsed_response_path(root),
    ):
        try:
            path.unlink(missing_ok=True)
        except OSError:
            continue


def _cluster_audit_input_paths(root: Path) -> tuple[Path, Path]:
    tmp_root = root / "derived" / "tmp"
    return (
        tmp_root / "bridge_concept_clusters.audit.input.jsonl",
        tmp_root / "cluster_reviews.audit.input.jsonl",
    )


def _cluster_audit_cluster_snapshot(cluster: dict) -> dict:
    return {
        "cluster_id": str(cluster.get("cluster_id", "")).strip(),
        "canonical_name": str(cluster.get("canonical_name", "")).strip(),
        "aliases": _safe_list(cluster.get("aliases", [])),
        "material_ids": _safe_list(cluster.get("material_ids", [])),
        "source_concepts": [
            {
                "material_id": str(source.get("material_id", "")).strip(),
                "concept_name": str(source.get("concept_name", "")).strip(),
                "concept_key": str(source.get("concept_key", "")).strip() or _normalize_concept_name(str(source.get("concept_name", "")).strip()),
            }
            for source in cluster.get("source_concepts", [])
            if isinstance(source, dict)
        ],
    }


def _cluster_audit_cluster_fingerprint(cluster: dict, route_signature: str) -> str:
    del route_signature
    return canonical_hash({
        "material_ids": sorted({
            str(material_id).strip()
            for material_id in cluster.get("material_ids", [])
            if str(material_id).strip()
        }),
    })


def _cluster_audit_cluster_fingerprints(clusters: list[dict], route_signature: str) -> dict[str, str]:
    fingerprints: dict[str, str] = {}
    for cluster in clusters:
        cluster_id = str(cluster.get("cluster_id", "")).strip()
        if not cluster_id:
            continue
        fingerprints[cluster_id] = _cluster_audit_cluster_fingerprint(cluster, route_signature)
    return fingerprints


def _cluster_audit_pending_local_fingerprint(local_rows: list[tuple], route_signature: str) -> str:
    pending_local = []
    for row in local_rows:
        concept_name, concept_key, material_id, relevance, source_pages, evidence_spans, confidence, concept_type, descriptor = _split_concept_row(row)
        pending_local.append({
            "material_id": material_id,
            "concept_name": concept_name,
            "concept_key": concept_key,
            "relevance": relevance,
            "source_pages": source_pages,
            "evidence_spans": evidence_spans,
            "confidence": confidence,
            "concept_type": concept_type,
            "descriptor": descriptor,
        })
    pending_local.sort(key=lambda item: (item["material_id"], item["concept_key"], item["concept_name"]))
    return canonical_hash({
        "route_signature": route_signature,
        "pending_local_concepts": pending_local,
    })


def _cluster_audit_target_clusters(
    clusters: list[dict],
    canonical_reviews: dict[str, dict],
    route_signature: str,
) -> tuple[list[dict], dict[str, str]]:
    cluster_fingerprints = _cluster_audit_cluster_fingerprints(clusters, route_signature)
    targets = []
    for cluster in sorted(clusters, key=lambda item: str(item.get("cluster_id", ""))):
        cluster_id = str(cluster.get("cluster_id", "")).strip()
        if not cluster_id:
            continue
        review = canonical_reviews.get(cluster_id)
        reasons: list[str] = []
        if review is None:
            reasons.append("missing_review")
        else:
            if str(review.get("status", "")).strip().lower() == "open":
                reasons.append("open_review")
            if str(review.get("input_fingerprint", "")).strip() != cluster_fingerprints.get(cluster_id, ""):
                reasons.append("changed_since_last_audit")
        if reasons:
            targets.append({
                "cluster_id": cluster_id,
                "canonical_name": str(cluster.get("canonical_name", "")).strip(),
                "material_ids": _safe_list(cluster.get("material_ids", [])),
                "current_review_status": str(review.get("status", "")).strip() if isinstance(review, dict) else "",
                "reasons": reasons,
            })
    return targets, cluster_fingerprints


def _cluster_audit_finalize_reviews(
    reviews: list[dict],
    cluster_fingerprints: dict[str, str],
    *,
    context_requested: bool | None = None,
    context_request_count: int | None = None,
) -> list[dict]:
    finalized = []
    for row in sorted(reviews, key=lambda item: str(item.get("cluster_id", ""))):
        if not isinstance(row, dict):
            continue
        cluster_id = str(row.get("cluster_id", "")).strip()
        if not cluster_id:
            continue
        normalized = dict(row)
        normalized["review_id"] = _cluster_audit_review_id(cluster_id)
        normalized["input_fingerprint"] = cluster_fingerprints.get(cluster_id, str(normalized.get("input_fingerprint", "")).strip())
        if context_requested is not None:
            normalized["context_requested"] = context_requested
        else:
            normalized["context_requested"] = bool(normalized.get("context_requested", False))
        if context_request_count is not None:
            normalized["context_request_count"] = context_request_count
        else:
            normalized["context_request_count"] = int(normalized.get("context_request_count", 0) or 0)
        finalized.append(normalized)
    return finalized


def _cluster_audit_mutable_concept_index(
    local_rows: list[tuple],
) -> dict[tuple[str, str], dict]:
    concept_index: dict[tuple[str, str], dict] = {}
    for row in local_rows:
        concept_name, concept_key, material_id, relevance, source_pages, evidence_spans, confidence, concept_type, descriptor = _split_concept_row(row)
        concept_index[(material_id, concept_key)] = {
            "concept_name": concept_name,
            "concept_key": concept_key,
            "material_id": material_id,
            "relevance": relevance,
            "source_pages": source_pages,
            "evidence_spans": evidence_spans,
            "confidence": confidence,
            "concept_type": concept_type,
            "descriptor": descriptor,
        }
    return concept_index


def _audit_optional_text(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _cluster_audit_validate_bridge_candidate(
    candidate: dict,
    concept_index: dict[tuple[str, str], dict],
    assigned_pairs: set[tuple[str, str]],
    *,
    label: str,
) -> dict | None:
    if not isinstance(candidate, dict):
        raise EnrichmentError(f"{label} must be an object")
    canonical_name = _audit_optional_text(candidate.get("canonical_name", ""))
    if not canonical_name:
        raise EnrichmentError(f"{label} is missing canonical_name")
    aliases = candidate.get("aliases", [])
    if not isinstance(aliases, list):
        raise EnrichmentError(f"{label} field 'aliases' must be a list")
    raw_sources = candidate.get("source_concepts", [])
    if not isinstance(raw_sources, list):
        raise EnrichmentError(f"{label} field 'source_concepts' must be a list")

    validated_source = []
    for idx, entry in enumerate(raw_sources, start=1):
        if not isinstance(entry, dict):
            raise EnrichmentError(f"{label}.source_concepts[{idx}] must be an object")
        material_id = str(entry.get("material_id", "")).strip()
        concept_name = str(entry.get("concept_name", "")).strip()
        if not material_id or not concept_name:
            raise EnrichmentError(f"{label}.source_concepts[{idx}] must include material_id and concept_name")
        indexed = _resolve_concept_reference(material_id, concept_name, concept_index)
        if indexed is None:
            continue
        validated_source.append(_build_source_concept(indexed))

    deduped_source = []
    seen_local: set[tuple[str, str]] = set()
    for source in validated_source:
        key = (source["material_id"], source["concept_key"])
        if key in seen_local:
            continue
        seen_local.add(key)
        deduped_source.append(source)

    candidate_source = []
    candidate_pairs: list[tuple[str, str]] = []
    for source in deduped_source:
        key = (source["material_id"], source["concept_key"])
        if key in assigned_pairs:
            continue
        candidate_source.append(source)
        candidate_pairs.append(key)

    material_ids = list(dict.fromkeys(source["material_id"] for source in candidate_source))
    claimed_material_ids = _dedupe_strings(_safe_list(candidate.get("material_ids", [])))
    if claimed_material_ids and claimed_material_ids != material_ids:
        raise EnrichmentError(f"{label} field 'material_ids' must match the source_concepts material ids")
    if len(material_ids) < 2:
        return None

    assigned_pairs.update(candidate_pairs)
    alias_values = _dedupe_strings([
        canonical_name,
        *[str(value).strip() for value in aliases if str(value).strip()],
        *[source.get("concept_name", "") for source in candidate_source if source.get("concept_name", "")],
    ])
    return {
        "canonical_name": canonical_name,
        "slug": slugify(canonical_name),
        "aliases": alias_values,
        "material_ids": material_ids,
        "source_concepts": [
            {
                "material_id": source["material_id"],
                "concept_name": source["concept_name"],
                "descriptor": source.get("descriptor", ""),
                "relevance": source["relevance"],
                "source_pages": source["source_pages"],
                "evidence_spans": source["evidence_spans"],
                "confidence": source["confidence"],
            }
            for source in candidate_source
        ],
        "confidence": _derive_cluster_confidence(candidate_source),
    }


def _cluster_audit_apply_bridge_update(
    candidate: dict,
    existing_cluster: dict,
    concept_index: dict[tuple[str, str], dict],
    assigned_pairs: set[tuple[str, str]],
    *,
    label: str,
) -> dict:
    if not isinstance(candidate, dict):
        raise EnrichmentError(f"{label} must be an object")

    if "new_aliases" in candidate and not isinstance(candidate.get("new_aliases"), list):
        raise EnrichmentError(f"{label} field 'new_aliases' must be a list when provided")
    if "new_materials" in candidate and not isinstance(candidate.get("new_materials"), list):
        raise EnrichmentError(f"{label} field 'new_materials' must be a list when provided")
    if "removed_materials" in candidate and not isinstance(candidate.get("removed_materials"), list):
        raise EnrichmentError(f"{label} field 'removed_materials' must be a list when provided")
    raw_new_sources = candidate.get("new_source_concepts", []) or []
    if not isinstance(raw_new_sources, list):
        raise EnrichmentError(f"{label} field 'new_source_concepts' must be a list when provided")

    removed_materials = set(_dedupe_strings(_safe_list(candidate.get("removed_materials", []))))
    retained_sources = []
    retained_pairs: set[tuple[str, str]] = set()
    for source in existing_cluster.get("source_concepts", []):
        if not isinstance(source, dict):
            continue
        material_id = str(source.get("material_id", "")).strip()
        concept_name = str(source.get("concept_name", "")).strip()
        concept_key = str(source.get("concept_key", "")).strip() or _normalize_concept_name(concept_name)
        if not material_id or not concept_name or material_id in removed_materials:
            continue
        retained_pairs.add((material_id, concept_key))
        retained_sources.append({
            "material_id": material_id,
            "concept_name": concept_name,
            "descriptor": str(source.get("descriptor", "")).strip(),
            "relevance": str(source.get("relevance", "")).strip(),
            "source_pages": source.get("source_pages", []),
            "evidence_spans": source.get("evidence_spans", []),
            "confidence": float(source.get("confidence", 0.0) or 0.0),
            "concept_key": concept_key,
        })

    seen_pairs = assigned_pairs | retained_pairs
    added_sources = []
    added_pairs: set[tuple[str, str]] = set()
    for idx, entry in enumerate(raw_new_sources, start=1):
        if not isinstance(entry, dict):
            raise EnrichmentError(f"{label}.new_source_concepts[{idx}] must be an object")
        material_id = str(entry.get("material_id", "")).strip()
        concept_name = str(entry.get("concept_name", "")).strip()
        if not material_id or not concept_name:
            raise EnrichmentError(f"{label}.new_source_concepts[{idx}] must include material_id and concept_name")
        indexed = _resolve_concept_reference(material_id, concept_name, concept_index)
        if indexed is None:
            continue
        normalized = _build_source_concept(indexed)
        pair = (normalized["material_id"], normalized["concept_key"])
        if pair in seen_pairs or pair in added_pairs:
            continue
        added_pairs.add(pair)
        added_sources.append(normalized)

    final_sources = [*retained_sources, *added_sources]
    final_material_ids = list(dict.fromkeys(source["material_id"] for source in final_sources))
    if len(final_material_ids) < 2:
        raise EnrichmentError(f"{label} must leave the bridge connected to at least two distinct materials")

    assigned_pairs.update(retained_pairs)
    assigned_pairs.update(added_pairs)

    canonical_name = _audit_optional_text(candidate.get("new_name", "")) or _audit_optional_text(existing_cluster.get("canonical_name", ""))
    if not canonical_name:
        raise EnrichmentError(f"{label} produced an empty canonical name")
    alias_values = candidate.get("new_aliases") if "new_aliases" in candidate else existing_cluster.get("aliases", [])
    aliases = _dedupe_strings([canonical_name, *[str(value).strip() for value in (alias_values or []) if str(value).strip()]])

    return {
        "cluster_id": str(existing_cluster.get("cluster_id", "")).strip(),
        "canonical_name": canonical_name,
        "slug": slugify(canonical_name),
        "aliases": aliases,
        "material_ids": final_material_ids,
        "source_concepts": [
            {
                "material_id": source["material_id"],
                "concept_name": source["concept_name"],
                "descriptor": source.get("descriptor", ""),
                "relevance": source["relevance"],
                "source_pages": source["source_pages"],
                "evidence_spans": source["evidence_spans"],
                "confidence": source["confidence"],
            }
            for source in final_sources
        ],
        "confidence": _derive_cluster_confidence(final_sources, fallback=float(existing_cluster.get("confidence", 0.0) or 0.0)),
    }


def _cluster_audit_review_id(cluster_id: str) -> str:
    return cluster_id


def _cluster_audit_existing_review_key(row: dict, position: int) -> tuple[float, int]:
    provenance = row.get("_provenance", {}) if isinstance(row.get("_provenance", {}), dict) else {}
    run_at = _parse_iso_datetime(provenance.get("run_at"))
    return (run_at.timestamp() if run_at else float("-inf"), position)


def _cluster_audit_review_ref(review: dict) -> str:
    if not isinstance(review, dict):
        return ""
    return (
        str(review.get("cluster_ref", "")).strip()
        or str(review.get("cluster_id", "")).strip()
        or str(review.get("bridge_ref", "")).strip()
    )


def _cluster_audit_canonicalize_existing_reviews(
    existing_rows: list[dict],
    cluster_refs: dict[str, dict],
) -> dict[str, dict]:
    canonical: dict[str, tuple[tuple[float, int], dict]] = {}
    for position, row in enumerate(existing_rows):
        if not isinstance(row, dict):
            continue
        cluster_id = str(row.get("cluster_id", "")).strip()
        if not cluster_id or cluster_id not in cluster_refs:
            continue
        score = _cluster_audit_existing_review_key(row, position)
        current = canonical.get(cluster_id)
        if current is None or score >= current[0]:
            normalized = _cluster_audit_normalize_review_row(
                row,
                cluster_refs,
                review_id=_cluster_audit_review_id(cluster_id),
                default_cluster_ref=cluster_id,
            )
            for key in ("input_fingerprint", "context_requested", "context_request_count"):
                if key in row:
                    normalized[key] = row.get(key)
            provenance = row.get("_provenance")
            if isinstance(provenance, dict):
                normalized["_provenance"] = dict(provenance)
            canonical[cluster_id] = (score, normalized)
    return {cluster_id: row for cluster_id, (_score, row) in canonical.items()}


def _cluster_audit_normalize_review_row(
    review: dict,
    cluster_refs: dict[str, dict],
    *,
    review_id: str,
    default_cluster_ref: str = "",
) -> dict:
    if not isinstance(review, dict):
        raise EnrichmentError("Cluster audit review entries must be objects")
    cluster_ref = _cluster_audit_review_ref(review) or default_cluster_ref
    if not cluster_ref:
        raise EnrichmentError(f"Cluster audit review '{review_id}' is missing cluster_ref/cluster_id")
    target_cluster = cluster_refs.get(cluster_ref)
    if not isinstance(target_cluster, dict):
        raise EnrichmentError(f"Cluster audit review '{review_id}' references unknown bridge '{cluster_ref}'")
    finding_type = str(review.get("finding_type", "")).strip()
    severity = str(review.get("severity", "")).strip().lower()
    status = str(review.get("status", "")).strip().lower()
    if status == "resolved":
        status = "validated"
    if status == "improved":
        status = "open"
    note = str(review.get("note", "")).strip()
    recommendation = str(review.get("recommendation", "")).strip()
    if not finding_type:
        raise EnrichmentError(f"Cluster audit review '{review_id}' is missing finding_type")
    if severity not in {"high", "medium", "low"}:
        raise EnrichmentError(f"Cluster audit review '{review_id}' has invalid severity '{severity}'")
    valid_statuses = {"open", "validated"}
    if status not in valid_statuses:
        raise EnrichmentError(f"Cluster audit review '{review_id}' has invalid status '{status}'")
    if not note:
        raise EnrichmentError(f"Cluster audit review '{review_id}' is missing note")
    if not recommendation:
        raise EnrichmentError(f"Cluster audit review '{review_id}' is missing recommendation")
    return {
        "review_id": review_id,
        "cluster_id": str(target_cluster.get("cluster_id", "")).strip(),
        "finding_type": finding_type,
        "severity": severity,
        "status": status,
        "note": note,
        "recommendation": recommendation,
        "wiki_path": str(_concept_page_path(target_cluster)),
    }


def _cluster_audit_apply_review_delta(
    existing_rows: list[dict],
    review_updates: list[dict],
    new_reviews: list[dict],
    cluster_refs: dict[str, dict],
) -> tuple[list[dict], set[str]]:
    rows_by_cluster = _cluster_audit_canonicalize_existing_reviews(existing_rows, cluster_refs)
    touched_clusters: set[str] = set()

    for idx, review in enumerate(review_updates, start=1):
        cluster_id = _cluster_audit_review_ref(review)
        if not cluster_id:
            raise EnrichmentError(f"review_updates[{idx}] is missing cluster_id")
        existing = rows_by_cluster.get(cluster_id)
        if existing is None:
            raise EnrichmentError(f"review_updates[{idx}] references bridge '{cluster_id}' without an existing canonical review row; use new_reviews instead")
        if cluster_id in touched_clusters:
            raise EnrichmentError(f"review_updates[{idx}] duplicates cluster_id '{cluster_id}'")
        normalized = _cluster_audit_normalize_review_row(
            review,
            cluster_refs,
            review_id=_cluster_audit_review_id(cluster_id),
            default_cluster_ref=cluster_id,
        )
        rows_by_cluster[cluster_id] = {
            **existing,
            **normalized,
        }
        touched_clusters.add(cluster_id)

    for idx, review in enumerate(new_reviews, start=1):
        cluster_ref = _cluster_audit_review_ref(review)
        if not cluster_ref:
            raise EnrichmentError(f"new_reviews[{idx}] is missing cluster_ref")
        target_cluster = cluster_refs.get(cluster_ref)
        if not isinstance(target_cluster, dict):
            raise EnrichmentError(f"new_reviews[{idx}] references unknown bridge '{cluster_ref}'")
        canonical_cluster_id = str(target_cluster.get("cluster_id", "")).strip()
        if canonical_cluster_id in rows_by_cluster:
            raise EnrichmentError(f"new_reviews[{idx}] references bridge '{canonical_cluster_id}' which already has a canonical review row; use review_updates instead")
        if canonical_cluster_id in touched_clusters:
            raise EnrichmentError(f"new_reviews[{idx}] duplicates cluster_id '{canonical_cluster_id}'")
        normalized = _cluster_audit_normalize_review_row(
            review,
            cluster_refs,
            review_id=_cluster_audit_review_id(canonical_cluster_id),
            default_cluster_ref=cluster_ref,
        )
        rows_by_cluster[canonical_cluster_id] = normalized
        touched_clusters.add(canonical_cluster_id)

    return [rows_by_cluster[key] for key in sorted(rows_by_cluster)], touched_clusters


def _ensure_cluster_audit_review_coverage(
    reviews: list[dict],
    bridge_clusters: list[dict],
) -> list[dict]:
    rows = [dict(row) for row in reviews if isinstance(row, dict)]
    covered_clusters = {
        str(row.get("cluster_id", "")).strip()
        for row in rows
        if str(row.get("cluster_id", "")).strip()
    }
    for cluster in bridge_clusters:
        cluster_id = str(cluster.get("cluster_id", "")).strip()
        if not cluster_id or cluster_id in covered_clusters:
            continue
        rows.append({
            "review_id": _cluster_audit_review_id(cluster_id),
            "cluster_id": cluster_id,
            "finding_type": "validated",
            "severity": "low",
            "status": "validated",
            "note": "Current bridge remains coherent in this audit pass.",
            "recommendation": "Keep the current bridge as is unless stronger new evidence appears.",
            "wiki_path": str(_concept_page_path(cluster)),
        })
        covered_clusters.add(cluster_id)
    return rows


def _collection_reflection_key(domain: str, collection: str) -> str:
    domain = (domain or "practice").strip() or "practice"
    collection = (collection or "_general").strip() or "_general"
    return f"{domain}/{collection}"


def _collection_reflection_stage_dir(root: Path) -> Path:
    return root / "derived" / "tmp" / "collection_reflections"


def _collection_reflection_page_copy_path(root: Path, domain: str, collection: str) -> Path:
    key = _collection_reflection_key(domain, collection).replace("/", "__")
    return _collection_reflection_stage_dir(root) / f"{key}.page.md"


def _collection_reflection_evidence_path(root: Path, domain: str, collection: str) -> Path:
    key = _collection_reflection_key(domain, collection).replace("/", "__")
    return _collection_reflection_stage_dir(root) / f"{key}.evidence.json"


def _format_collection_material_concept(row: dict) -> str:
    concept_name = str(row.get("concept_name", "")).strip()
    descriptor = str(row.get("descriptor", "")).strip()
    spans = [span for span in _safe_list(row.get("evidence_spans", [])) if span]
    if descriptor:
        return f"{concept_name} ({descriptor})".strip()
    if spans:
        return f"{concept_name} ({', '.join(spans[:2])})".strip()
    return concept_name


def _collection_reflection_local_clusters(
    domain: str,
    collection: str,
    metas: list[dict],
    clusters: list[dict],
    tool: ReflectionIndexTool | None = None,
) -> list[dict]:
    material_ids = {str(meta.get("material_id", "")).strip() for meta in metas if str(meta.get("material_id", "")).strip()}
    target_scope = _collection_scope(domain, collection)
    overlapping = []
    for cluster in clusters:
        if _cluster_scope(cluster) != target_scope:
            continue
        overlap = material_ids & {str(mid).strip() for mid in _safe_list(cluster.get("material_ids", []))}
        if not overlap:
            continue
        reflection = tool.open_record("concept", cluster.get("cluster_id", "")) if tool else None
        reflection_data = reflection.get("reflection", {}) if isinstance(reflection, dict) else {}
        overlapping.append({
            "cluster_id": cluster.get("cluster_id", ""),
            "concept": cluster.get("canonical_name", ""),
            "material_count": len(overlap),
            "main_takeaways": _parse_json_list(reflection_data.get("main_takeaways", ""))[:3],
            "main_tensions": _parse_json_list(reflection_data.get("main_tensions", ""))[:2],
            "open_questions": _parse_json_list(reflection_data.get("open_questions", ""))[:2],
            "why_this_concept_matters": str(reflection_data.get("why_this_concept_matters", "")).strip(),
        })
    overlapping.sort(key=lambda item: (-item["material_count"], item["concept"].lower(), item["cluster_id"]))
    return overlapping


def _collection_reflection_materials(
    root: Path,
    domain: str,
    collection: str,
    metas: list[dict],
    clusters: list[dict],
    existing_record: dict | None = None,
    tool: ReflectionIndexTool | None = None,
) -> dict[str, list[dict]]:
    if not metas:
        return {"new_materials": [], "old_materials": []}

    cluster_lookup: dict[str, list[dict]] = defaultdict(list)
    cluster_by_material: dict[str, list[str]] = defaultdict(list)
    for cluster in clusters:
        cluster_id = str(cluster.get("cluster_id", "")).strip()
        if not cluster_id:
            continue
        for mid in _safe_list(cluster.get("material_ids", [])):
            mid = str(mid).strip()
            if mid:
                cluster_by_material[mid].append(cluster_id)
                cluster_lookup[mid].append(cluster)

    scored: list[tuple[float, str, dict, list[dict], list[str]]] = []
    for meta in metas:
        mid = str(meta.get("material_id", "")).strip()
        if not mid:
            continue
        overlap_clusters = cluster_lookup.get(mid, [])
        overlap_ids = cluster_by_material.get(mid, [])
        score = float(len(overlap_clusters))
        title = str(meta.get("title", mid)).strip().lower()
        scored.append((score, title, meta, overlap_clusters, overlap_ids))

    scored.sort(key=lambda item: (-item[0], item[1], item[2].get("material_id", "")))
    selected = scored[:COLLECTION_REFLECTION_MAX_MATERIALS]

    friendly_title = f"{domain.replace('_', ' ').title()} / {collection.replace('_', ' ').title()}"
    previous_ids = {
        str(mid).strip()
        for mid in _safe_list((existing_record or {}).get("important_material_ids", []))
        if str(mid).strip()
    }
    new_materials: list[dict] = []
    old_materials: list[dict] = []
    for _score, _title, meta, overlap_clusters, _overlap_ids in selected:
        mid = str(meta.get("material_id", "")).strip()
        title = str(meta.get("title", mid)).strip()
        summary = _meta_val(meta.get("summary"))
        methodological_conclusions = _meta_list_value(meta, "methodological_conclusions")
        main_content_learnings = _meta_list_value(meta, "main_content_learnings")

        query_terms = _dedupe_strings([
            friendly_title,
            _collection_reflection_key(domain, collection),
            title,
            summary,
            *[cluster.get("canonical_name", "") for cluster in overlap_clusters if cluster.get("canonical_name", "")],
        ])

        is_new = mid not in previous_ids
        material_entry: dict[str, Any] = {
            "material_id": mid,
            "title": title,
            "methodological_conclusions": methodological_conclusions,
            "main_content_learnings": main_content_learnings,
        }

        if is_new:
            chunk_limit = COLLECTION_REFLECTION_MAX_CHUNKS_PER_MATERIAL
            chunk_evidence = _collect_material_chunk_evidence(tool, mid, query_terms, chunk_limit)
            evidence = (
                tool._material_evidence(
                    mid,
                    query_terms,
                    chunk_limit=chunk_limit,
                    annotation_limit=COLLECTION_REFLECTION_MAX_ANNOTATIONS_PER_MATERIAL,
                    figure_limit=COLLECTION_REFLECTION_MAX_FIGURES_PER_MATERIAL,
                    concept_limit=COLLECTION_REFLECTION_MAX_CONCEPTS_PER_MATERIAL,
                )
                if tool
                else {}
            )
            evidence_payload: dict[str, Any] = {
                "chunks": [
                    {
                        "text": str(chunk.get("text", "")).strip(),
                        "source": str(chunk.get("source", "")).strip() or "fallback",
                    }
                    for chunk in chunk_evidence
                    if isinstance(chunk, dict) and str(chunk.get("text", "")).strip()
                ],
                "figures": [
                    {
                        "figure_id": str(fig.get("figure_id", "")).strip(),
                        "description": str(fig.get("description", "")).strip(),
                    }
                    for fig in list(evidence.get("figures", []))
                    if isinstance(fig, dict)
                ],
                "concepts": [
                    _format_collection_material_concept(con)
                    for con in list(evidence.get("concepts", []))
                    if isinstance(con, dict) and str(con.get("concept_name", "")).strip()
                ],
            }
            annotations = [
                line
                for line in (
                    _format_material_annotation(ann)
                    for ann in list(evidence.get("annotations", []))
                    if isinstance(ann, dict)
                )
                if line
            ]
            if annotations:
                evidence_payload["annotations"] = "\n".join(annotations)
            material_entry["evidence"] = evidence_payload
            new_materials.append(material_entry)
        else:
            old_materials.append({
                "material_id": material_entry["material_id"],
                "title": material_entry["title"],
                "methodological_conclusions": material_entry["methodological_conclusions"],
                "main_content_learnings": material_entry["main_content_learnings"],
            })

    return {"new_materials": new_materials, "old_materials": old_materials}


def _build_collection_reflection_evidence_payload(
    root: Path,
    domain: str,
    collection: str,
    metas: list[dict],
    clusters: list[dict],
    existing_record: dict | None = None,
    tool: ReflectionIndexTool | None = None,
) -> dict:
    materials = _collection_reflection_materials(root, domain, collection, metas, clusters, existing_record, tool)
    return {
        "kind": "collection_reflection",
        "collection_key": _collection_reflection_key(domain, collection),
        "domain": domain,
        "collection": collection,
        "title": f"{domain.replace('_', ' ').title()} / {collection.replace('_', ' ').title()}",
        "local_clusters": _collection_reflection_local_clusters(domain, collection, metas, clusters, tool),
        "new_materials": materials["new_materials"],
        "old_materials": materials["old_materials"],
    }


def _collection_reflection_scaffold(
    domain: str,
    collection: str,
    fingerprint: str,
    root: Path,
) -> dict:
    return {
        "collection_key": _collection_reflection_key(domain, collection),
        "domain": domain,
        "collection": collection,
        "main_takeaways": [],
        "main_tensions": [],
        "important_material_ids": [],
        "important_cluster_ids": [],
        "open_questions": [],
        "why_this_collection_matters": "",
        "input_fingerprint": fingerprint,
        "wiki_path": str(_collection_page_path(domain, collection)),
    }


def _compile_collection_reflection_response(
    domain: str,
    collection: str,
    scaffold: dict,
    fingerprint: str,
    parsed: Any,
    existing_record: dict | None = None,
) -> dict:
    if not isinstance(parsed, dict):
        raise EnrichmentError("Collection reflection output must be a JSON object")
    if parsed.get("_finished") is not True:
        raise EnrichmentError("Collection reflection output missing _finished=true")

    def _resolve_list_field(field: str) -> list[str]:
        value = parsed.get(field)
        if value is None:
            return _dedupe_strings(_safe_list((existing_record or {}).get(field, scaffold.get(field, []))))
        if not isinstance(value, list):
            raise EnrichmentError(f"Collection reflection output field '{field}' must be a list or null")
        return _dedupe_strings(_safe_list(value))

    def _resolve_id_field(field: str) -> list[str]:
        return sorted({item.strip() for item in _resolve_list_field(field) if item.strip()})

    def _resolve_text_field(field: str) -> str:
        value = parsed.get(field)
        if value is None:
            return str((existing_record or {}).get(field, scaffold.get(field, "")) or "").strip()
        if not isinstance(value, str):
            raise EnrichmentError(f"Collection reflection output field '{field}' must be a string or null")
        return value.strip()

    for field in ("main_takeaways", "main_tensions", "important_material_ids", "important_cluster_ids", "open_questions"):
        if field.startswith("important_"):
            _resolve_id_field(field)
        else:
            _resolve_list_field(field)
    _resolve_text_field("why_this_collection_matters")

    return {
        **scaffold,
        "collection_key": _collection_reflection_key(domain, collection),
        "domain": domain,
        "collection": collection,
        "main_takeaways": _resolve_list_field("main_takeaways"),
        "main_tensions": _resolve_list_field("main_tensions"),
        "important_material_ids": _resolve_id_field("important_material_ids"),
        "important_cluster_ids": _resolve_id_field("important_cluster_ids"),
        "open_questions": _resolve_list_field("open_questions"),
        "why_this_collection_matters": _resolve_text_field("why_this_collection_matters"),
        "input_fingerprint": fingerprint,
        "wiki_path": str(scaffold.get("wiki_path", "")).strip(),
    }


def _collection_reflection_fingerprint(domain: str, collection: str, metas: list[dict], clusters: list[dict]) -> str:
    material_ids = sorted({
        str(meta.get("material_id", "")).strip()
        for meta in metas
        if str(meta.get("material_id", "")).strip()
    })
    target_scope = _collection_scope(domain, collection)
    local_clusters = sorted(
        [
            {
                "cluster_id": str(cluster.get("cluster_id", "")).strip(),
                "canonical_name": str(cluster.get("canonical_name", "")).strip(),
                "material_ids": sorted(_safe_list(cluster.get("material_ids", []))),
            }
            for cluster in clusters
            if _cluster_scope(cluster) == target_scope
        ],
        key=lambda row: row["cluster_id"],
    )
    return canonical_hash({
        "collection_key": _collection_reflection_key(domain, collection),
        "material_ids": material_ids,
        "local_clusters": local_clusters,
    })


def _cluster_audit_prompt(root: Path, input_path: Path, bridge_input_path: Path, reviews_input_path: Path) -> tuple[str, str]:
    system = (
        "You are an architecture research librarian auditing the bridge concept graph.\n"
        "\n"
        "You will receive exactly three read-only inputs: the uncovered local bridge packet, the staged bridge memory file with the existing clusters under review, and the current cluster-review audit log. "
        "If you need more context for your decisions, do not guess. Return a JSON object that includes a context_requests array with up to 4 read-only SQL-index lookups. "
        "Use context requests only for targeted material evidence queries or a collection open_record."
        " Each request should look like {\"tool\":\"search_material_evidence\",\"kind\":\"chunk|annotation|figure\",\"material_id\":\"...\",\"query\":\"...\",\"limit\":5} "
        "or {\"tool\":\"open_record\",\"kind\":\"collection\",\"id\":\"...\"}. "
        "You will get only one read-only context round, so request everything you need at once.\n"
        "\n"
        f"Return exactly one final JSON object matching this schema: {_CLUSTER_AUDIT_DELTA_SCHEMA}\n"
        "- Use bridge_updates for existing clusters in the staged bridge memory file. You may rename if strictly necessary, replace aliases, attach uncovered local concepts and remove materials that no longer belong.\n"
        "- Use new_bridges for genuinely new cross-material bridges built from the uncovered local concepts in the bridge packet.\n"
        "- Every new_bridges entry that you keep must have exactly one matching new_reviews row using the same temporary bridge_ref. If you decide a candidate is not a real bridge, omit both that new_bridges entry and its review row.\n"
        "- new_source_concepts is the authoritative way to attach uncovered local concepts to a reviewed bridge. If you include new_materials, it is only a convenience hint; the pipeline will derive the actual added materials from new_source_concepts.\n"
        "The cluster_reviews file is an audit log for the next round and contains only the current review rows for the staged bridge clusters under review: what changed, why it changed, what still seems doubtful, and what is validated for now. There must be exactly one canonical audit-log row per bridge cluster when the audit is done. Status must always be open or validated.\n"
        "- Use review_updates for clusters that already have a canonical audit-log row, keyed by cluster_id.\n"
        "- Use new_reviews for clusters that do not yet have a canonical audit-log row. When the row is for a newly proposed bridge, cluster_ref must repeat the exact bridge_ref from new_bridges.\n"
        "There must be exactly one canonical audit-log row per bridge cluster, and review status must be open or validated.\n\n"

        "ABOUT CLUSTER NAMES: Bridge concepts are ambitious cross-material ideas. "
        "Only change existing names for clear improvement, and only if strictly necessary."
        "For new clustrers: Cluster names may be theoretically dense and multi-word. Avoid near-duplicate concepts, incidental topics, and generic labels like history, power, space, or memory unless sharply qualified. Prefer cluster names that carry analytical charge and group local and bridge concepts together, like spatial justice, racial capitalism, architecture as care, counter-mapping methods, or collecting as spatial practice, and many others. IMP: Avoid academic jargon, theoretical buzzwords, or pretentious language. Use clear, direct, and specific language that conveys real analytical meaning.\n\n"

        "Set _finished to true only in the final completed JSON object. Return JSON only.\n"
        "\n"
        "## TODO\n"
        "- [ ] Read the existing review rows and recommendations.\n"
        "- [ ] Audit the staged bridge-memory clusters: improve names or aliases when strictly necessary, attach uncovered local concepts when they clearly belong, and remove materials that clearly do not belong.\n"
        "- [ ] Create genuinely new bridges only for uncovered local concepts that do not fit one of the reviewed bridges.\n"
        "- [ ] Return explicit bridge and review deltas in the final JSON object.\n"
        "- [ ] Finish only when the final JSON object is complete and _finished is true.\n"
    )
    user = (
        f"Read these files:\n"
        f"- {input_path}\n"
        "- If the packet points to a bridge packet file, read that too.\n"
        f"- {reviews_input_path}\n"
        f"- {bridge_input_path}\n"
        "\n"
        "The bridge input file is JSONL and contains only the existing bridge clusters you are allowed to review. Treat it as read-only input.\n"
        "The cluster_reviews input file contains only the current audit rows for the staged bridge clusters under review. Treat it as read-only input.\n"
        "Do not invent concepts that are not present in the bridge packet.\n"
        "Return final JSON only.\n"
    )
    return system, user


def _concept_reflection_prompt(
    page_path: Path,
    evidence_path: Path,
) -> tuple[str, str]:
    system = (
        "You are an architecture research librarian writing reflective synthesis for a concept page.\n"
        "\n"
        "Your job is not to restate the page. Your job is to explain the concept's role in the corpus: "
        "what the bridge concept is really saying, why it matters, what it connects, what tensions it holds, "
        "and what remains unresolved.\n"
        "\n"
        "Use the wiki page as the current public state of the concept. Use the staged SQL-evidence file for "
        "the supporting materials, chunks, annotations, and figures that ground the synthesis. Preserve prior "
        "conclusions when they still hold, but revise them when the evidence changes.\n"
        "\n"
        "The chunk evidence is ordered by usefulness. Chunks with source=search are the strongest matches to the bridge concept query terms. "
        "Chunks with source=fallback are only fill-in evidence for the same material and may be less directly relevant. "
        "Prefer the search-sourced chunks when forming the synthesis, and treat fallback chunks as secondary support.\n"
        "\n"
        "For each reflection, be specific and cumulative. Prefer concrete main takeaways over generic summaries. "
        "Write the reflection as a synthesis, not as a list of facts. The reflection should usually cover: "
        "the central claim of the concept, the strongest supporting evidence, the main tensions or ambiguities, "
        "the open questions worth tracking, and a concise statement of why this concept matters to the larger corpus.\n"
        "If this is the first run, still write a strong first synthesis instead of waiting for prior history.\n"
        "\n"
        "Return exactly one final JSON object matching this schema: "
        f"{_CONCEPT_REFLECTION_DELTA_SCHEMA}\n"
        "Do all reasoning silently first. Do not return markdown fences, commentary, or partial JSON."
    )
    user = (
        f"Read these files:\n"
        f"- Concept wiki page: {page_path}\n"
        f"- SQL evidence file: {evidence_path}\n"
        "\n"
        "The concept wiki page is the current public page and already contains linked materials, annotations, figures, and any previous reflection.\n"
        "The SQL evidence file contains the staged evidence for this concept cluster.\n"
        "The chunks inside that file are ordered by usefulness; source=search is the strongest match to the concept query, while source=fallback is only secondary support.\n"
        "The annotations field, when present, is a single newline-delimited string of annotation notes. The concepts field, when present, is a compact list of concept names only.\n"
        "Return only the reflection fields requested by the schema: main_takeaways, main_tensions, open_questions, why_this_concept_matters, and _finished.\n"
        "If one reflection field should remain exactly as the current reflection already states it, you may return null for that field and the pipeline will preserve the stored value for that key.\n"
        "Do not return cluster metadata, supporting ids, or wiki paths.\n"
        "Write a strong reflection that includes the main takeaways, main tensions, open questions, and why this concept matters.\n"
        "If prior reflection text still fits the evidence, preserve it; if it no longer fits, revise it.\n"
        "Do not leave the work file as a mere summary of the page. Use the evidence to surface the concept's role, stakes, and unresolved questions.\n"
        "Return final JSON only.\n"
    )
    return system, user


def _collection_reflection_prompt(
    domain: str,
    collection: str,
    page_path: Path,
    evidence_path: Path,
) -> tuple[str, str]:
    system = (
        "You are an architecture research librarian writing reflective synthesis for a collection page.\n"
        "\n"
        "Your job is not to restate the collection page. Your job is to explain what the collection is doing as a whole: "
        "the main takeaways, the main tensions, the important materials and local clusters, and the open questions that should stay visible.\n"
        "\n"
        "Use the collection wiki page as the current public state of the collection. Use the staged SQL-evidence file for "
        "the supporting materials, methodological conclusions, main content learnings, chunks, annotations, figures, and compact local-concept "
        "summaries that ground the synthesis. Preserve prior "
        "conclusions when they still hold, but revise them when the evidence changes.\n"
        "\n"
        "The material-level methodological conclusions and main content learnings are the primary reusable evidence for each material. "
        "The chunks are only secondary support. Chunks with source=search are the strongest matches to the collection evidence queries. "
        "Chunks with source=fallback are only fill-in evidence for the same material and may be less directly relevant. "
        "Prefer the search-sourced chunks when forming the synthesis, and keep the chunk selection compact.\n"
        "\n"
        "For each reflection, be specific and cumulative. Prefer concrete main takeaways over generic summaries. "
        "Write the reflection as a synthesis, not as a list of facts. The reflection should usually cover: "
        "the collection's central through-line, the strongest supporting materials, the most important local clusters, "
        "the main tensions or ambiguities, the open questions worth tracking, and a concise statement of why this collection matters to the larger corpus.\n"
        "If this is the first run, still write a strong first synthesis instead of waiting for prior history.\n"
        "\n"
        "Return exactly one final JSON object matching this schema: "
        f"{_COLLECTION_REFLECTION_DELTA_SCHEMA}\n"
        "Do all reasoning silently first. Do not return markdown fences, commentary, or partial JSON."
    )
    user = (
        f"Read these files:\n"
        f"- Collection wiki page: {page_path}\n"
        f"- SQL evidence file: {evidence_path}\n"
        "\n"
        "The collection wiki page is the current public page and already contains the materials, key concepts, and any previous reflection.\n"
        "The SQL evidence file contains staged material packets split into new_materials and old_materials.\n"
        "new_materials are the materials that were not present in the previous collection reflection and therefore carry the strongest evidence.\n"
        "old_materials are materials already present in the previous collection reflection; they are only compact continuity context.\n"
        "The chunks inside new_materials are ordered by usefulness; source=search is the strongest match to the collection queries, while source=fallback is only secondary support.\n"
        "The chunks are only secondary support.\n"
        "The local_clusters entries are short synthesized cues from the concept reflections, not raw membership ids.\n"
        "Treat the new_materials as the main evidence for this run and the old_materials as compact background continuity.\n"
        "Treat the methodological conclusions and main content learnings as the primary material-level evidence; the chunks are only a small supporting slice.\n"
        "Return only the reflection fields requested by the schema: main_takeaways, main_tensions, important_material_ids, important_cluster_ids, open_questions, why_this_collection_matters, and _finished.\n"
        "If one reflection field should remain exactly as the current reflection already states it, you may return null for that field and the pipeline will preserve the stored value for that key.\n"
        "Do not return collection metadata, fingerprints, or wiki paths.\n"
        "Write a strong reflection that includes the main takeaways, main tensions, important materials, important local clusters, open questions, and why this collection matters.\n"
        "If prior reflection text still fits the evidence, preserve it; if it no longer fits, revise it.\n"
        "Do not leave the work file as a mere summary of the page. Use the evidence to surface the collection's role, stakes, and unresolved questions.\n"
        "Return final JSON only.\n"
    )
    return system, user


def _graph_reflection_stage_dir(root: Path) -> Path:
    return root / "derived" / "tmp" / "graph_reflection"


def _graph_reflection_page_path(root: Path) -> Path:
    return root / "wiki" / "shared" / "maintenance" / "graph-health.md"


def _graph_reflection_packet_path(root: Path) -> Path:
    return _graph_reflection_stage_dir(root) / "graph_health.packet.json"


def _graph_reflection_existing_path(root: Path) -> Path:
    return _graph_reflection_stage_dir(root) / "graph_health.current.jsonl"


def _graph_reflection_packet(
    deterministic_report: dict,
    bridge_clusters: list[dict],
    cluster_reviews: list[dict],
    concept_refs: list[dict],
    collection_refs: list[dict],
    manifest_records: list[dict],
) -> dict:
    material_ids = {
        str(record.get("material_id", "")).strip()
        for record in manifest_records
        if str(record.get("material_id", "")).strip()
    }
    bridge_clusters = [c for c in bridge_clusters if isinstance(c, dict)]
    multi_material_clusters = [
        c for c in bridge_clusters
        if len(dict.fromkeys(str(mid).strip() for mid in _safe_list(c.get("material_ids", [])) if str(mid).strip())) > 1
    ]

    severity_rank = {"high": 0, "medium": 1, "low": 2}
    cluster_threads = []
    for row in sorted(
        (row for row in cluster_reviews if isinstance(row, dict)),
        key=lambda r: (
            severity_rank.get(str(r.get("severity", "")).strip().lower(), 3),
            str(r.get("cluster_id", "")),
            str(r.get("status", "")),
        ),
    )[:12]:
        cluster_threads.append({
            "cluster_id": str(row.get("cluster_id", "")).strip(),
            "status": str(row.get("status", "")).strip(),
            "note": str(row.get("note", "")).strip(),
        })

    def _compact_concept_reflection(row: dict) -> dict | None:
        cluster_id = str(row.get("cluster_id", "")).strip()
        canonical_name = str(row.get("canonical_name", "")).strip()
        takeaways = _safe_list(row.get("main_takeaways", []))[:2]
        tensions = _safe_list(row.get("main_tensions", []))[:2]
        questions = _safe_list(row.get("open_questions", []))[:3]
        matter = str(row.get("why_this_concept_matters", "")).strip()
        if not (cluster_id or canonical_name or takeaways or tensions or questions or matter):
            return None
        return {
            "cluster_id": cluster_id,
            "canonical_name": canonical_name,
            "main_takeaways": takeaways,
            "main_tensions": tensions,
            "open_questions": questions,
            "why_this_concept_matters": matter,
        }

    def _compact_collection_reflection(row: dict) -> dict | None:
        collection_key = str(row.get("collection_key", "")).strip()
        takeaways = _safe_list(row.get("main_takeaways", []))[:2]
        tensions = _safe_list(row.get("main_tensions", []))[:2]
        questions = _safe_list(row.get("open_questions", []))[:3]
        matter = str(row.get("why_this_collection_matters", "")).strip()
        if not (collection_key or takeaways or tensions or questions or matter):
            return None
        return {
            "collection_key": collection_key,
            "main_takeaways": takeaways,
            "main_tensions": tensions,
            "open_questions": questions,
            "why_this_collection_matters": matter,
        }

    return {
        "kind": "graph_maintenance",
        "summary": deterministic_report.get("summary", {}),
        "graph_state": {
            "materials": len(material_ids),
            "bridge_clusters": len(bridge_clusters),
            "multi_material_clusters": len(multi_material_clusters),
            "bridge_concepts": sum(len(_safe_list(c.get("source_concepts", []))) for c in bridge_clusters),
            "cluster_reviews": len(cluster_reviews),
            "concept_reflections": len(concept_refs),
            "collection_reflections": len(collection_refs),
        },
        "cluster_reviews": cluster_threads,
        "concept_threads": [item for row in concept_refs if isinstance(row, dict) for item in [_compact_concept_reflection(row)] if item][:10],
        "collection_threads": [item for row in collection_refs if isinstance(row, dict) for item in [_compact_collection_reflection(row)] if item][:10],
    }


def _graph_reflection_prompt(
    packet_path: Path,
    current_path: Path,
) -> tuple[str, str]:
    system = (
        "You are an architecture research librarian writing structured graph-maintenance findings for SQL-backed storage.\n"
        "\n"
        "This is not a wiki page. It is a semantic maintenance record for the graph: what still feels unresolved, "
        "what bridge areas are too thin or too broad, what concept homes are still missing, what collection "
        "syntheses still need work, and what should be investigated next.\n"
        "\n"
        "Use the compact graph-state packet for the high-signal inputs. Use the current graph-findings file as the "
        "current stored state when it exists. Preserve useful prior material when it still fits, but revise stale "
        "items instead of copying them forward blindly.\n"
        "\n"
        "Deterministic lint already handles mechanical hygiene such as broken links, orphans, and stale page counts. "
        "Do not repeat that work here. Keep the findings concise, judgment-heavy, and SQL-friendly.\n"
        "\n"
        "Return exactly one final JSON object matching this schema: "
        f"{_GRAPH_REFLECTION_DELTA_SCHEMA}\n"
        "Do all reasoning silently first. Do not return markdown fences, commentary, or partial JSON."
    )
    user = (
        f"Read these files:\n"
        f"- Graph-state packet: {packet_path}\n"
        f"- Current graph findings file: {current_path}\n"
        "\n"
        "The packet is ultra-compact graph state: summary counts, bridge-graph shape, cluster reviews, "
        "concept threads, and collection threads.\n"
        "The current graph findings file is the current stored state and may be empty on the first run.\n"
        "Write a prioritized maintenance record, not a raw list of everything. Focus on the few unresolved "
        "semantic problems that matter most: weak bridge areas, missing concept homes, collection synthesis gaps, "
        "and the next questions or sources that would move the graph forward.\n"
        "Return only the fields requested by the schema: findings and _finished.\n"
        "If the current findings list still fits the evidence exactly, you may return null for findings and the pipeline will preserve the stored list unchanged.\n"
        "Do not restate deterministic lint results.\n"
        "Return final JSON only.\n"
    )
    return system, user


def _compile_graph_reflection_response(
    parsed: Any,
    existing_rows: list[dict],
    fingerprint: str,
) -> list[dict]:
    if not isinstance(parsed, dict):
        raise EnrichmentError("Graph maintenance output must be a JSON object")
    if parsed.get("_finished") is not True:
        raise EnrichmentError("Graph maintenance output missing _finished=true")

    findings = parsed.get("findings")
    if findings is None:
        source_rows = [dict(row) for row in existing_rows if isinstance(row, dict)]
    elif isinstance(findings, list):
        source_rows = findings
    else:
        raise EnrichmentError("Graph maintenance output field 'findings' must be a list or null")

    normalized = []
    for idx, finding in enumerate(source_rows):
        if not isinstance(finding, dict):
            continue
        for field in ("finding_type", "severity", "summary", "details"):
            value = finding.get(field, "")
            if not isinstance(value, str):
                raise EnrichmentError(f"Graph maintenance finding field '{field}' must be a string")
        for field in (
            "affected_material_ids",
            "affected_cluster_ids",
            "candidate_future_sources",
            "candidate_bridge_links",
        ):
            value = finding.get(field, [])
            if not isinstance(value, list):
                raise EnrichmentError(f"Graph maintenance finding field '{field}' must be a list")
        finding_id = str(finding.get("finding_id", "")).strip()
        if not finding_id:
            finding_id = f"graph:{idx}"
        normalized.append({
            "finding_id": finding_id,
            "finding_type": str(finding.get("finding_type", "")).strip(),
            "severity": str(finding.get("severity", "")).strip(),
            "summary": str(finding.get("summary", "")).strip(),
            "details": str(finding.get("details", "")).strip(),
            "affected_material_ids": _dedupe_strings(_safe_list(finding.get("affected_material_ids", []))),
            "affected_cluster_ids": _dedupe_strings(_safe_list(finding.get("affected_cluster_ids", []))),
            "candidate_future_sources": _dedupe_strings(_safe_list(finding.get("candidate_future_sources", []))),
            "candidate_bridge_links": _dedupe_strings(_safe_list(finding.get("candidate_bridge_links", []))),
            "input_fingerprint": fingerprint,
        })
    return normalized
def _next_bridge_cluster_index(clusters: list[dict]) -> int:
    max_idx = 0
    for cluster in clusters:
        cid = str(cluster.get("cluster_id", "")).strip()
        match = re.fullmatch(r"bridge_(\d{4})", cid)
        if match:
            max_idx = max(max_idx, int(match.group(1)))
    return max_idx + 1


def _assign_new_bridge_ids(clusters: list[dict], existing_clusters: list[dict]) -> list[dict]:
    """Assign sequential bridge ids to newly added clusters without touching existing ids."""
    existing_ids = {str(cluster.get("cluster_id", "")).strip() for cluster in existing_clusters if cluster.get("cluster_id", "")}
    next_idx = _next_bridge_cluster_index(existing_clusters)
    reassigned: list[dict] = []
    used_ids = set(existing_ids)
    for cluster in clusters:
        cid = str(cluster.get("cluster_id", "")).strip()
        if cid and cid not in used_ids and re.fullmatch(r"bridge_\d{4}", cid):
            reassigned.append(cluster)
            used_ids.add(cid)
            continue
        while f"bridge_{next_idx:04d}" in used_ids:
            next_idx += 1
        new_id = f"bridge_{next_idx:04d}"
        used_ids.add(new_id)
        next_idx += 1
        reassigned.append({
            **cluster,
            "cluster_id": new_id,
            "wiki_path": f"wiki/shared/bridge-concepts/{cluster.get('slug', '')}.md" if cluster.get("slug", "") else "",
        })
    return reassigned


def _cover_pairs_from_clusters(clusters: list[dict]) -> set[tuple[str, str]]:
    covered: set[tuple[str, str]] = set()
    for cluster in clusters:
        for source in cluster.get("source_concepts", []) or []:
            if not isinstance(source, dict):
                continue
            material_id = str(source.get("material_id", "")).strip()
            concept_name = str(source.get("concept_name", "")).strip()
            concept_key = str(source.get("concept_key", "")).strip() or concept_name.lower().strip()
            if material_id and concept_key:
                covered.add((material_id, concept_key))
    return covered
def _build_material_info(root: Path, manifest_records: list[dict]) -> dict[str, dict]:
    metas = _load_all_metas(root, manifest_records)
    info: dict[str, dict] = {}
    for mid, meta in metas.items():
        keywords = _meta_list_value(meta, "keywords")
        info[mid] = {
            "title": meta.get("title", mid),
            "summary": _meta_val(meta.get("summary")),
            "keywords": keywords,
            "methodological_conclusions": _meta_list_value(meta, "methodological_conclusions"),
            "main_content_learnings": _meta_list_value(meta, "main_content_learnings"),
            "domain": meta.get("domain", "practice"),
            "collection": meta.get("collection", "_general"),
            "meta": meta,
        }
    return info


def _build_deterministic_collection_page(root: Path, domain: str, collection: str, metas: list[dict], clusters: list[dict]) -> str:
    title = f"{domain.replace('_', ' ').title()} / {collection.replace('_', ' ').title()}"
    manifest_index = {rec.get("material_id", ""): rec for rec in _load_manifest(root) if rec.get("material_id")}
    material_entries = []
    for meta in metas:
        material_entries.append({
            "name": meta.get("title") or meta.get("material_id", ""),
            "path": _material_wiki_path(meta),
            "summary": _meta_val(meta.get("summary"))[:120],
        })
    coll_mids = {meta.get("material_id", "") for meta in metas if meta.get("material_id")}
    key_concepts = []
    for c in clusters:
        overlap = coll_mids & set(c.get("material_ids", []))
        if overlap:
            key_concepts.append({
                "name": c.get("canonical_name", ""),
                "path": c.get("wiki_path") or _concept_wiki_path(c.get("slug", "")),
                "count": len(overlap),
            })
    facets = []
    facet_fields = [
        "building_type", "scale", "location", "jurisdiction", "climate",
        "program", "material_system", "structural_system", "historical_period",
        "course_topic", "studio_project",
    ]
    facet_freq: dict[tuple[str, str], int] = Counter()
    for meta in metas:
        facets_meta = meta.get("facets") or {}
        for field in facet_fields:
            val = _meta_val(facets_meta.get(field) or "").strip()
            if val:
                facet_freq[(field, val)] += 1
    for (field, val), count in sorted(facet_freq.items(), key=lambda x: (-x[1], x[0])):
        if count >= 2:
            facets.append({"field": field, "value": val, "count": count})
    recent = sorted(
        [{
            "name": meta.get("title") or meta.get("material_id", ""),
            "path": _material_wiki_path(meta),
            "ingested_at": manifest_index.get(meta.get("material_id", ""), {}).get("ingested_at", ""),
        } for meta in metas],
        key=lambda x: x.get("ingested_at", ""),
        reverse=True,
    )
    return render_collection_page(title, domain, collection, material_entries, key_concepts, facets, recent)


def _run_cluster_audit(
    root: Path,
    clusters: list[dict],
    material_info: dict[str, dict],
    route_signature: str = "",
    llm_factory=None,
    tool: ReflectionIndexTool | None = None,
) -> tuple[list[dict], int]:
    if _clusters_are_local(clusters):
        return _run_local_cluster_audit(root, clusters, material_info, route_signature, llm_factory, tool)

    existing_path = root / LINT_DIR / "cluster_reviews.jsonl"
    existing_rows = _load_jsonl(existing_path)
    local_rows, material_rows = _local_rows_not_in_bridge(root, clusters)
    existing_cluster_by_id = {
        str(cluster.get("cluster_id", "")).strip(): cluster
        for cluster in clusters
        if str(cluster.get("cluster_id", "")).strip()
    }
    canonical_existing_reviews = _cluster_audit_canonicalize_existing_reviews(existing_rows, existing_cluster_by_id)
    target_clusters, cluster_fingerprints = _cluster_audit_target_clusters(clusters, canonical_existing_reviews, route_signature)
    target_cluster_ids = {
        str(target.get("cluster_id", "")).strip()
        for target in target_clusters
        if str(target.get("cluster_id", "")).strip()
    }
    current_pending_local_fingerprint = _cluster_audit_pending_local_fingerprint(local_rows, route_signature)
    audit_state = _read_stamp(root / CLUSTER_AUDIT_STATE_PATH)
    prior_pending_local_fingerprint = str(audit_state.get("pending_local_fingerprint", "") or "")
    pending_local_changed = bool(local_rows and material_rows) and current_pending_local_fingerprint != prior_pending_local_fingerprint

    normalized_reviews = [canonical_existing_reviews[key] for key in sorted(canonical_existing_reviews)]
    normalized_reviews = _ensure_cluster_audit_review_coverage(normalized_reviews, clusters)
    normalized_reviews = _cluster_audit_finalize_reviews(normalized_reviews, cluster_fingerprints)

    if not target_cluster_ids and not pending_local_changed:
        run_at = datetime.now(timezone.utc).isoformat()
        reviews_changed = normalized_reviews != existing_rows
        if reviews_changed:
            _attach_run_provenance(normalized_reviews, route_signature, run_at)
            _write_jsonl(existing_path, normalized_reviews)
        _write_lint_stage_stamp(root, audited_at=run_at)
        _write_stamp(
            root / CLUSTER_AUDIT_STATE_PATH,
            {
                "pending_local_fingerprint": current_pending_local_fingerprint,
                "pending_local_concepts": len(local_rows),
            },
        )
        _cleanup_cluster_audit_debug_artifacts(root)
        return normalized_reviews, 0

    bridge_packets_path = None
    if local_rows and material_rows:
        bridge_packets_path = _stage_bridge_packet_input(
            root,
            local_rows,
            material_rows,
            max_local_concepts_per_material=None,
            max_bridge_candidates_per_material=None,
            max_evidence_snippets_per_material=None,
        )

    bridge_input_path, reviews_input_path = _cluster_audit_input_paths(root)
    reviewable_clusters = [
        cluster
        for cluster in clusters
        if str(cluster.get("cluster_id", "")).strip() in target_cluster_ids
    ]
    packet = {
        "bridge_memory": str(bridge_input_path),
        "cluster_reviews": str(reviews_input_path),
        "bridge_packets": str(bridge_packets_path) if bridge_packets_path else "",
    }
    input_path = _cluster_audit_input_path(root)

    _write_json(input_path, packet)
    _write_jsonl(bridge_input_path, reviewable_clusters)
    _stage_cluster_audit_reviews_input(reviews_input_path, canonical_existing_reviews, target_cluster_ids)
    llm_fn = llm_factory("lint")
    system, user = _cluster_audit_prompt(root, input_path, bridge_input_path, reviews_input_path)

    def _record_cluster_audit_raw_response(raw_text: str, phase: str) -> None:
        path = _cluster_audit_raw_response_path(root, phase)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(str(raw_text), encoding="utf-8")

    parsed = _run_reflection_prompt_with_context(
        llm_fn,
        system,
        user,
        _CLUSTER_AUDIT_DELTA_SCHEMA,
        tool,
        raw_response_recorder=_record_cluster_audit_raw_response,
    )
    _write_json(_cluster_audit_parsed_response_path(root), parsed if isinstance(parsed, dict) else {"raw": parsed})

    if not isinstance(parsed, dict):
        raise EnrichmentError("Cluster audit output must be a JSON object")
    if parsed.get("_finished") is not True:
        raise EnrichmentError("Cluster audit output missing _finished=true")
    parsed.pop("_finished", None)

    bridge_updates = parsed.get("bridge_updates", [])
    new_bridges = parsed.get("new_bridges", [])
    review_updates = parsed.get("review_updates", [])
    new_reviews = parsed.get("new_reviews", [])
    if not isinstance(bridge_updates, list):
        raise EnrichmentError("Cluster audit output field 'bridge_updates' must be a list")
    if not isinstance(new_bridges, list):
        raise EnrichmentError("Cluster audit output field 'new_bridges' must be a list")
    if not isinstance(review_updates, list):
        raise EnrichmentError("Cluster audit output field 'review_updates' must be a list")
    if not isinstance(new_reviews, list):
        raise EnrichmentError("Cluster audit output field 'new_reviews' must be a list")

    context_requested = bool(parsed.get("context_requested"))
    context_request_count = int(parsed.get("context_request_count", 0) or 0)
    updated_cluster_ids: set[str] = set()
    for idx, candidate in enumerate(bridge_updates, start=1):
        if not isinstance(candidate, dict):
            raise EnrichmentError(f"bridge_updates[{idx}] must be an object")
        cluster_id = str(candidate.get("cluster_id", "")).strip()
        if not cluster_id:
            raise EnrichmentError(f"bridge_updates[{idx}] is missing cluster_id")
        if cluster_id not in existing_cluster_by_id:
            raise EnrichmentError(f"bridge_updates[{idx}] references unknown cluster_id '{cluster_id}'")
        if cluster_id not in target_cluster_ids:
            raise EnrichmentError(f"bridge_updates[{idx}] references cluster_id '{cluster_id}' outside audit_targets")
        if cluster_id in updated_cluster_ids:
            raise EnrichmentError(f"bridge_updates[{idx}] duplicates cluster_id '{cluster_id}'")
        updated_cluster_ids.add(cluster_id)

    for idx, review in enumerate(review_updates, start=1):
        if not isinstance(review, dict):
            raise EnrichmentError(f"review_updates[{idx}] must be an object")
        cluster_id = str(review.get("cluster_id", "")).strip() or str(review.get("cluster_ref", "")).strip()
        if not cluster_id:
            raise EnrichmentError(f"review_updates[{idx}] is missing cluster_id")
        if cluster_id not in target_cluster_ids:
            raise EnrichmentError(f"review_updates[{idx}] references cluster_id '{cluster_id}' outside audit_targets")

    concept_index = _cluster_audit_mutable_concept_index(local_rows)
    assigned_pairs = _cover_pairs_from_clusters(clusters)

    updated_bridge_rows = []
    for idx, candidate in enumerate(bridge_updates, start=1):
        cluster_id = str(candidate.get("cluster_id", "")).strip()
        validated = _cluster_audit_apply_bridge_update(
            candidate,
            existing_cluster_by_id[cluster_id],
            concept_index,
            assigned_pairs,
            label=f"bridge_updates[{idx}]",
        )
        updated_bridge_rows.append(validated)

    new_bridge_candidates = []
    new_bridge_refs: list[tuple[str, dict]] = []
    dropped_new_bridge_refs: set[str] = set()
    for idx, candidate in enumerate(new_bridges, start=1):
        if not isinstance(candidate, dict):
            raise EnrichmentError(f"new_bridges[{idx}] must be an object")
        bridge_ref = str(candidate.get("bridge_ref", "")).strip()
        if not bridge_ref:
            raise EnrichmentError(f"new_bridges[{idx}] is missing bridge_ref")
        validated = _cluster_audit_validate_bridge_candidate(
            candidate,
            concept_index,
            assigned_pairs,
            label=f"new_bridges[{idx}]",
        )
        if validated is None:
            dropped_new_bridge_refs.add(bridge_ref)
            continue
        new_bridge_refs.append((bridge_ref, validated))
        new_bridge_candidates.append(validated)

    assigned_new_bridges = _assign_new_bridge_ids(new_bridge_candidates, clusters) if new_bridge_candidates else []
    bridge_ref_map = {
        bridge_ref: assigned_new_bridges[idx]
        for idx, (bridge_ref, _candidate) in enumerate(new_bridge_refs)
        if idx < len(assigned_new_bridges)
    }

    normalized_new_reviews = []
    for idx, review in enumerate(new_reviews, start=1):
        if not isinstance(review, dict):
            raise EnrichmentError(f"new_reviews[{idx}] must be an object")
        cluster_ref = _cluster_audit_review_ref(review)
        if not cluster_ref:
            raise EnrichmentError(f"new_reviews[{idx}] is missing cluster_ref")
        if cluster_ref in bridge_ref_map:
            normalized_new_reviews.append(review)
            continue
        if cluster_ref in dropped_new_bridge_refs:
            continue
        if cluster_ref in existing_cluster_by_id and cluster_ref not in target_cluster_ids:
            raise EnrichmentError(f"new_reviews[{idx}] references cluster_id '{cluster_ref}' outside audit_targets")
        normalized_new_reviews.append(review)
    bridge_refs = {**existing_cluster_by_id, **bridge_ref_map}

    bridge_work = sorted([
        *[cluster for cluster in clusters if str(cluster.get("cluster_id", "")).strip() not in updated_cluster_ids],
        *updated_bridge_rows,
        *assigned_new_bridges,
    ], key=lambda c: c.get("cluster_id", ""))
    reviews_work, reviewed_cluster_ids = _cluster_audit_apply_review_delta(existing_rows, review_updates, normalized_new_reviews, bridge_refs)
    required_review_ids = set(target_cluster_ids) | {
        str(cluster.get("cluster_id", "")).strip()
        for cluster in assigned_new_bridges
        if str(cluster.get("cluster_id", "")).strip()
    }
    missing_review_ids = sorted(cluster_id for cluster_id in required_review_ids if cluster_id not in reviewed_cluster_ids)
    if missing_review_ids:
        raise EnrichmentError(
            "Cluster audit must update review rows for every target or new bridge cluster: "
            + ", ".join(missing_review_ids)
        )
    reviews_work = _ensure_cluster_audit_review_coverage(reviews_work, bridge_work)

    final_cluster_fingerprints = _cluster_audit_cluster_fingerprints(bridge_work, route_signature)
    reviews_work = _cluster_audit_finalize_reviews(
        reviews_work,
        final_cluster_fingerprints,
        context_requested=context_requested,
        context_request_count=context_request_count,
    )

    if not isinstance(bridge_work, list) or any(not isinstance(cluster, dict) for cluster in bridge_work):
        raise EnrichmentError("Cluster audit output produced invalid bridge clusters")
    if not isinstance(reviews_work, list) or any(not isinstance(row, dict) for row in reviews_work):
        raise EnrichmentError("Cluster audit output produced invalid cluster reviews")

    bridge_changed = bridge_work != clusters

    run_at = datetime.now(timezone.utc).isoformat()
    _attach_run_provenance(reviews_work, route_signature, run_at)
    _attach_run_provenance(bridge_work, route_signature, run_at)
    _write_jsonl(root / LINT_DIR / "cluster_reviews.jsonl", reviews_work)
    _write_lint_stage_stamp(root, audited_at=run_at)
    remaining_local_rows = _filter_local_rows_not_in_bridge(local_rows, bridge_work)
    _write_stamp(
        root / CLUSTER_AUDIT_STATE_PATH,
        {
            "pending_local_fingerprint": _cluster_audit_pending_local_fingerprint(remaining_local_rows, route_signature),
            "pending_local_concepts": len(remaining_local_rows),
        },
    )
    _write_jsonl(root / "derived" / "bridge_concept_clusters.jsonl", bridge_work)
    stamp_path = root / "derived" / "bridge_cluster_stamp.json"
    stamp_path.write_text(
        json.dumps({
            "clustered_at": run_at,
            "fingerprint": bridge_cluster_fingerprint(None),
            "bridge_concepts": sum(len(c.get("source_concepts", [])) for c in bridge_work),
            "clusters": len(bridge_work),
        }, separators=(',', ':')),
        encoding="utf-8",
    )
    _cleanup_paths(input_path, bridge_input_path, reviews_input_path, bridge_packets_path or Path())
    _cleanup_cluster_audit_debug_artifacts(root)
    return reviews_work, int(bridge_changed)


def _run_local_cluster_audit(
    root: Path,
    clusters: list[dict],
    material_info: dict[str, dict],
    route_signature: str = "",
    llm_factory=None,
    tool: ReflectionIndexTool | None = None,
) -> tuple[list[dict], int]:
    existing_path = root / LINT_DIR / "cluster_reviews.jsonl"
    existing_rows = _load_jsonl(existing_path)
    local_groups = _group_clusters_by_scope(clusters)
    local_cluster_ids = {
        str(cluster.get("cluster_id", "")).strip()
        for cluster in clusters
        if str(cluster.get("cluster_id", "")).strip()
    }
    preserved_rows = [
        row for row in existing_rows
        if str(row.get("cluster_id", "")).strip() not in local_cluster_ids
    ]
    workers = max(1, min(len(local_groups), _parallel_collection_audit_workers(load_config())))

    def _one(domain: str, collection: str, scope_clusters: list[dict]) -> tuple[list[dict], int]:
        gate_path = _local_audit_gate_path(root, domain, collection)
        gate_path.parent.mkdir(parents=True, exist_ok=True)
        scope_cluster_ids = {
            str(cluster.get("cluster_id", "")).strip()
            for cluster in scope_clusters
            if str(cluster.get("cluster_id", "")).strip()
        }
        scope_existing_rows = [
            row for row in existing_rows
            if str(row.get("cluster_id", "")).strip() in scope_cluster_ids
        ]
        if gate_path.exists():
            return scope_existing_rows, 0

        gate_path.write_text(str(os.getpid()), encoding="utf-8")
        try:
            scope_root = local_cluster_dir(root, domain, collection)
            local_rows, material_rows = _local_rows_in_scope_not_in_clusters(root, domain, collection, scope_clusters, material_info)
            existing_cluster_by_id = {
                str(cluster.get("cluster_id", "")).strip(): cluster
                for cluster in scope_clusters
                if str(cluster.get("cluster_id", "")).strip()
            }
            canonical_existing_reviews = _cluster_audit_canonicalize_existing_reviews(scope_existing_rows, existing_cluster_by_id)
            target_clusters, cluster_fingerprints = _cluster_audit_target_clusters(scope_clusters, canonical_existing_reviews, route_signature)
            target_cluster_ids = {
                str(target.get("cluster_id", "")).strip()
                for target in target_clusters
                if str(target.get("cluster_id", "")).strip()
            }
            current_pending_local_fingerprint = _cluster_audit_pending_local_fingerprint(local_rows, route_signature)
            audit_state = _read_stamp(_local_audit_state_path(root, domain, collection))
            prior_pending_local_fingerprint = str(audit_state.get("pending_local_fingerprint", "") or "")
            pending_local_changed = bool(local_rows and material_rows) and current_pending_local_fingerprint != prior_pending_local_fingerprint

            normalized_reviews = [canonical_existing_reviews[key] for key in sorted(canonical_existing_reviews)]
            normalized_reviews = _ensure_cluster_audit_review_coverage(normalized_reviews, scope_clusters)
            normalized_reviews = _cluster_audit_finalize_reviews(normalized_reviews, cluster_fingerprints)

            if not target_cluster_ids and not pending_local_changed:
                run_at = datetime.now(timezone.utc).isoformat()
                if normalized_reviews != scope_existing_rows:
                    _attach_run_provenance(normalized_reviews, route_signature, run_at)
                _write_stamp(
                    _local_audit_state_path(root, domain, collection),
                    {
                        "pending_local_fingerprint": current_pending_local_fingerprint,
                        "pending_local_concepts": len(local_rows),
                    },
                )
                _write_stamp(
                    _local_audit_stamp_path(root, domain, collection),
                    {
                        "audited_at": run_at,
                        "cluster_reviews": len(normalized_reviews),
                    },
                )
                return normalized_reviews, 0

            bridge_packets_path = None
            if local_rows and material_rows:
                bridge_packets_path = _stage_bridge_packet_input(
                    scope_root,
                    local_rows,
                    material_rows,
                    max_local_concepts_per_material=None,
                    max_bridge_candidates_per_material=None,
                    max_evidence_snippets_per_material=None,
                )

            bridge_input_path, reviews_input_path = _cluster_audit_input_paths(scope_root)
            reviewable_clusters = [
                cluster
                for cluster in scope_clusters
                if str(cluster.get("cluster_id", "")).strip() in target_cluster_ids
            ]
            packet = {
                "bridge_memory": str(bridge_input_path),
                "cluster_reviews": str(reviews_input_path),
                "bridge_packets": str(bridge_packets_path) if bridge_packets_path else "",
            }
            input_path = _cluster_audit_input_path(scope_root)
            _write_json(input_path, packet)
            _write_jsonl(bridge_input_path, reviewable_clusters)
            _stage_cluster_audit_reviews_input(reviews_input_path, canonical_existing_reviews, target_cluster_ids)
            llm_fn = llm_factory("lint")
            system, user = _cluster_audit_prompt(scope_root, input_path, bridge_input_path, reviews_input_path)

            def _record_cluster_audit_raw_response(raw_text: str, phase: str) -> None:
                path = _cluster_audit_raw_response_path(scope_root, phase)
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(str(raw_text), encoding="utf-8")

            parsed = _run_reflection_prompt_with_context(
                llm_fn,
                system,
                user,
                _CLUSTER_AUDIT_DELTA_SCHEMA,
                tool,
                raw_response_recorder=_record_cluster_audit_raw_response,
            )
            _write_json(_cluster_audit_parsed_response_path(scope_root), parsed if isinstance(parsed, dict) else {"raw": parsed})
            if not isinstance(parsed, dict):
                raise EnrichmentError("Cluster audit output must be a JSON object")
            if parsed.get("_finished") is not True:
                raise EnrichmentError("Cluster audit output missing _finished=true")
            parsed.pop("_finished", None)

            bridge_updates = parsed.get("bridge_updates", [])
            new_bridges = parsed.get("new_bridges", [])
            review_updates = parsed.get("review_updates", [])
            new_reviews = parsed.get("new_reviews", [])
            if not isinstance(bridge_updates, list):
                raise EnrichmentError("Cluster audit output field 'bridge_updates' must be a list")
            if not isinstance(new_bridges, list):
                raise EnrichmentError("Cluster audit output field 'new_bridges' must be a list")
            if not isinstance(review_updates, list):
                raise EnrichmentError("Cluster audit output field 'review_updates' must be a list")
            if not isinstance(new_reviews, list):
                raise EnrichmentError("Cluster audit output field 'new_reviews' must be a list")

            context_requested = bool(parsed.get("context_requested"))
            context_request_count = int(parsed.get("context_request_count", 0) or 0)
            updated_cluster_ids: set[str] = set()
            for idx, candidate in enumerate(bridge_updates, start=1):
                if not isinstance(candidate, dict):
                    raise EnrichmentError(f"bridge_updates[{idx}] must be an object")
                cluster_id = str(candidate.get("cluster_id", "")).strip()
                if not cluster_id:
                    raise EnrichmentError(f"bridge_updates[{idx}] is missing cluster_id")
                if cluster_id not in existing_cluster_by_id:
                    raise EnrichmentError(f"bridge_updates[{idx}] references unknown cluster_id '{cluster_id}'")
                if cluster_id not in target_cluster_ids:
                    raise EnrichmentError(f"bridge_updates[{idx}] references cluster_id '{cluster_id}' outside audit_targets")
                if cluster_id in updated_cluster_ids:
                    raise EnrichmentError(f"bridge_updates[{idx}] duplicates cluster_id '{cluster_id}'")
                updated_cluster_ids.add(cluster_id)

            for idx, review in enumerate(review_updates, start=1):
                if not isinstance(review, dict):
                    raise EnrichmentError(f"review_updates[{idx}] must be an object")
                cluster_id = str(review.get("cluster_id", "")).strip() or str(review.get("cluster_ref", "")).strip()
                if not cluster_id:
                    raise EnrichmentError(f"review_updates[{idx}] is missing cluster_id")
                if cluster_id not in target_cluster_ids:
                    raise EnrichmentError(f"review_updates[{idx}] references cluster_id '{cluster_id}' outside audit_targets")

            concept_index = _cluster_audit_mutable_concept_index(local_rows)
            assigned_pairs = _cover_pairs_from_clusters(scope_clusters)
            updated_scope_rows = []
            for idx, candidate in enumerate(bridge_updates, start=1):
                cluster_id = str(candidate.get("cluster_id", "")).strip()
                updated_scope_rows.append(
                    _cluster_audit_apply_bridge_update(
                        candidate,
                        existing_cluster_by_id[cluster_id],
                        concept_index,
                        assigned_pairs,
                        label=f"bridge_updates[{idx}]",
                    )
                )

            new_cluster_candidates = []
            new_cluster_refs: list[tuple[str, dict]] = []
            dropped_new_cluster_refs: set[str] = set()
            for idx, candidate in enumerate(new_bridges, start=1):
                if not isinstance(candidate, dict):
                    raise EnrichmentError(f"new_bridges[{idx}] must be an object")
                bridge_ref = str(candidate.get("bridge_ref", "")).strip()
                if not bridge_ref:
                    raise EnrichmentError(f"new_bridges[{idx}] is missing bridge_ref")
                validated = _cluster_audit_validate_bridge_candidate(
                    candidate,
                    concept_index,
                    assigned_pairs,
                    label=f"new_bridges[{idx}]",
                )
                if validated is None:
                    dropped_new_cluster_refs.add(bridge_ref)
                    continue
                new_cluster_refs.append((bridge_ref, validated))
                new_cluster_candidates.append(validated)

            assigned_new_clusters = normalize_local_clusters(domain, collection, new_cluster_candidates) if new_cluster_candidates else []
            bridge_ref_map = {
                bridge_ref: assigned_new_clusters[idx]
                for idx, (bridge_ref, _candidate) in enumerate(new_cluster_refs)
                if idx < len(assigned_new_clusters)
            }

            normalized_new_reviews = []
            for idx, review in enumerate(new_reviews, start=1):
                if not isinstance(review, dict):
                    raise EnrichmentError(f"new_reviews[{idx}] must be an object")
                cluster_ref = _cluster_audit_review_ref(review)
                if not cluster_ref:
                    raise EnrichmentError(f"new_reviews[{idx}] is missing cluster_ref")
                if cluster_ref in bridge_ref_map:
                    normalized_new_reviews.append(review)
                    continue
                if cluster_ref in dropped_new_cluster_refs:
                    continue
                if cluster_ref in existing_cluster_by_id and cluster_ref not in target_cluster_ids:
                    raise EnrichmentError(f"new_reviews[{idx}] references cluster_id '{cluster_ref}' outside audit_targets")
                normalized_new_reviews.append(review)
            bridge_refs = {**existing_cluster_by_id, **bridge_ref_map}

            scope_work = sorted([
                *[cluster for cluster in scope_clusters if str(cluster.get("cluster_id", "")).strip() not in updated_cluster_ids],
                *updated_scope_rows,
                *assigned_new_clusters,
            ], key=lambda c: c.get("cluster_id", ""))
            reviews_work, reviewed_cluster_ids = _cluster_audit_apply_review_delta(scope_existing_rows, review_updates, normalized_new_reviews, bridge_refs)
            required_review_ids = set(target_cluster_ids) | {
                str(cluster.get("cluster_id", "")).strip()
                for cluster in assigned_new_clusters
                if str(cluster.get("cluster_id", "")).strip()
            }
            missing_review_ids = sorted(cluster_id for cluster_id in required_review_ids if cluster_id not in reviewed_cluster_ids)
            if missing_review_ids:
                raise EnrichmentError(
                    "Cluster audit must update review rows for every target or new bridge cluster: "
                    + ", ".join(missing_review_ids)
                )
            reviews_work = _ensure_cluster_audit_review_coverage(reviews_work, scope_work)
            final_cluster_fingerprints = _cluster_audit_cluster_fingerprints(scope_work, route_signature)
            reviews_work = _cluster_audit_finalize_reviews(
                reviews_work,
                final_cluster_fingerprints,
                context_requested=context_requested,
                context_request_count=context_request_count,
            )

            cluster_changed = scope_work != scope_clusters
            run_at = datetime.now(timezone.utc).isoformat()
            _attach_run_provenance(reviews_work, route_signature, run_at)
            _attach_run_provenance(scope_work, route_signature, run_at)
            _write_jsonl(local_cluster_dir(root, domain, collection) / "local_concept_clusters.jsonl", scope_work)
            local_cluster_stamp_path(root, domain, collection).write_text(
                json.dumps(
                    {
                        "clustered_at": run_at,
                        "fingerprint": local_cluster_fingerprint(domain, collection, load_config()),
                        "clusters": len(scope_work),
                        "bridge_concepts": sum(len(cluster.get("source_concepts", [])) for cluster in scope_work),
                    },
                    separators=(",", ":"),
                ),
                encoding="utf-8",
            )
            remaining_local_rows = _filter_local_rows_not_in_bridge(local_rows, scope_work)
            _write_stamp(
                _local_audit_state_path(root, domain, collection),
                {
                    "pending_local_fingerprint": _cluster_audit_pending_local_fingerprint(remaining_local_rows, route_signature),
                    "pending_local_concepts": len(remaining_local_rows),
                },
            )
            _write_stamp(
                _local_audit_stamp_path(root, domain, collection),
                {
                    "audited_at": run_at,
                    "cluster_reviews": len(reviews_work),
                },
            )
            _cleanup_paths(input_path, bridge_input_path, reviews_input_path, bridge_packets_path or Path())
            _cleanup_cluster_audit_debug_artifacts(scope_root)
            return reviews_work, int(cluster_changed)
        finally:
            try:
                gate_path.unlink()
            except OSError:
                pass

    results: list[tuple[list[dict], int]] = []
    scope_items = sorted(local_groups.items())
    if len(scope_items) > 1 and workers > 1:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(_one, domain, collection, scope_clusters) for (domain, collection), scope_clusters in scope_items]
            for fut in as_completed(futures):
                results.append(fut.result())
    else:
        for (domain, collection), scope_clusters in scope_items:
            results.append(_one(domain, collection, scope_clusters))

    merged_reviews = preserved_rows + [row for reviews, _changed in results for row in reviews]
    merged_reviews.sort(key=lambda row: str(row.get("cluster_id", "")))
    _write_jsonl(existing_path, merged_reviews)
    _write_lint_stage_stamp(root, audited_at=datetime.now(timezone.utc).isoformat())
    return merged_reviews, sum(changed for _reviews, changed in results)


def _run_concept_reflections(
    root: Path,
    clusters: list[dict],
    material_info: dict[str, dict],
    llm_factory=None,
    tool: ReflectionIndexTool | None = None,
    route_signature: str = "",
) -> list[dict]:
    existing = _existing_by_key(root / LINT_DIR / "concept_reflections.jsonl", "cluster_id")
    output: list[dict] = []
    eligible = [c for c in clusters if len(dict.fromkeys(sc.get("material_id", "") for sc in c.get("source_concepts", []) if sc.get("material_id"))) >= 2]
    workers = max(1, min(len(eligible), int(load_config().get("enrichment", {}).get("parallel", 4) or 4)))

    def _one(cluster: dict) -> dict | None:
        page_path = root / _concept_page_path(cluster)
        evidence_payload = _build_concept_reflection_evidence_payload(cluster, material_info, tool)
        fingerprint = _concept_reflection_link_fingerprint(cluster)
        existing_record = existing.get(cluster.get("cluster_id", ""))
        if existing_record and existing_record.get("input_fingerprint") == fingerprint:
            return existing_record
        page_copy_path = _concept_reflection_page_copy_path(root, cluster.get("cluster_id", ""))
        evidence_path = _concept_reflection_evidence_path(root, cluster.get("cluster_id", ""))
        _stage_reflection_page_copy(page_path, page_copy_path)
        evidence_path.parent.mkdir(parents=True, exist_ok=True)
        _write_json(evidence_path, evidence_payload)
        scaffold = _concept_reflection_scaffold(cluster, fingerprint, root)
        llm_fn = llm_factory("cluster")
        system, user = _concept_reflection_prompt(page_copy_path, evidence_path)
        succeeded = False
        try:
            if tool is not None:
                parsed = _run_reflection_prompt_with_context(
                    llm_fn,
                    system,
                    user,
                    _CONCEPT_REFLECTION_DELTA_SCHEMA,
                    tool,
                )
            else:
                raw = llm_fn(system, [{"role": "user", "content": user}])
                parsed = parse_json_or_repair(llm_fn, raw, _CONCEPT_REFLECTION_DELTA_SCHEMA)
            record = _compile_concept_reflection_response(cluster, scaffold, fingerprint, parsed, existing_record)
            succeeded = True
            return record
        finally:
            if succeeded:
                _cleanup_paths(evidence_path, page_copy_path)

    if len(eligible) > 1 and workers > 1:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(_one, c) for c in eligible]
            for fut in as_completed(futures):
                record = fut.result()
                if record:
                    output.append(record)
    else:
        for cluster in eligible:
            record = _one(cluster)
            if record:
                output.append(record)

    output.sort(key=lambda r: r.get("cluster_id", ""))
    run_at = datetime.now(timezone.utc).isoformat()
    _attach_run_provenance(output, route_signature, run_at)
    _write_jsonl(root / LINT_DIR / "concept_reflections.jsonl", output)
    _write_lint_stage_stamp(root, concept_reflection_at=run_at)
    return output


def _run_collection_reflections(
    root: Path,
    groups: dict[tuple[str, str], list[dict]],
    clusters: list[dict],
    llm_factory=None,
    tool: ReflectionIndexTool | None = None,
    route_signature: str = "",
) -> list[dict]:
    existing = _existing_by_key(root / LINT_DIR / "collection_reflections.jsonl", "collection_key")
    output: list[dict] = []
    eligible = [(domain, collection, metas) for (domain, collection), metas in groups.items() if len(metas) >= 2]
    workers = max(1, min(len(eligible), int(load_config().get("enrichment", {}).get("parallel", 4) or 4)))

    def _one(domain: str, collection: str, metas: list[dict]) -> dict | None:
        key = _collection_reflection_key(domain, collection)
        page_path = root / _collection_page_path(domain, collection)
        fingerprint = _collection_reflection_fingerprint(domain, collection, metas, clusters)
        existing_record = existing.get(key)
        if existing_record and existing_record.get("input_fingerprint") == fingerprint:
            return existing_record
        page_copy_path = _collection_reflection_page_copy_path(root, domain, collection)
        evidence_path = _collection_reflection_evidence_path(root, domain, collection)
        _stage_reflection_page_copy(page_path, page_copy_path)
        evidence_path.parent.mkdir(parents=True, exist_ok=True)
        evidence_payload = _build_collection_reflection_evidence_payload(root, domain, collection, metas, clusters, existing_record, tool)
        _write_json(evidence_path, evidence_payload)
        scaffold = _collection_reflection_scaffold(domain, collection, fingerprint, root)
        llm_fn = llm_factory("cluster")
        system, user = _collection_reflection_prompt(domain, collection, page_copy_path, evidence_path)
        succeeded = False
        try:
            if tool is not None:
                parsed = _run_reflection_prompt_with_context(
                    llm_fn,
                    system,
                    user,
                    _COLLECTION_REFLECTION_DELTA_SCHEMA,
                    tool,
                )
            else:
                raw = llm_fn(system, [{"role": "user", "content": user}])
                parsed = parse_json_or_repair(llm_fn, raw, _COLLECTION_REFLECTION_DELTA_SCHEMA)
            record = _compile_collection_reflection_response(
                domain,
                collection,
                scaffold,
                fingerprint,
                parsed,
                existing_record,
            )
            succeeded = True
            return record
        finally:
            if succeeded:
                _cleanup_paths(evidence_path, page_copy_path)

    if len(eligible) > 1 and workers > 1:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(_one, domain, collection, metas) for domain, collection, metas in eligible]
            for fut in as_completed(futures):
                record = fut.result()
                if record:
                    output.append(record)
    else:
        for domain, collection, metas in eligible:
            record = _one(domain, collection, metas)
            if record:
                output.append(record)

    output.sort(key=lambda r: (r.get("domain", ""), r.get("collection", "")))
    run_at = datetime.now(timezone.utc).isoformat()
    _attach_run_provenance(output, route_signature, run_at)
    _write_jsonl(root / LINT_DIR / "collection_reflections.jsonl", output)
    _write_lint_stage_stamp(root, collection_reflection_at=run_at)
    return output


def _run_graph_reflection(
    root: Path,
    deterministic_report: dict,
    cluster_reviews: list[dict],
    concept_refs: list[dict],
    collection_refs: list[dict],
    bridge_clusters: list[dict],
    manifest_records: list[dict],
    llm_factory=None,
    tool: ReflectionIndexTool | None = None,
    route_signature: str = "",
) -> dict:
    packet_path = _graph_reflection_packet_path(root)
    current_path = _graph_reflection_existing_path(root)
    payload = _graph_reflection_packet(
        deterministic_report,
        bridge_clusters,
        cluster_reviews,
        concept_refs,
        collection_refs,
        manifest_records,
    )
    fingerprint = canonical_hash(payload)
    findings_path = root / LINT_DIR / "graph_findings.jsonl"
    existing_rows = _load_jsonl(findings_path)
    if findings_path.exists() and fingerprint and _read_stamp(root / GRAPH_REFLECTION_STAMP_PATH).get("graph_fingerprint") == fingerprint:
        return {
            "graph_maintenance": 0,
            "graph_skipped": True,
            "graph_skip_reason": "graph maintenance unchanged",
        }

    _stage_work_copy(findings_path, current_path)
    _write_json(packet_path, payload)

    llm_fn = llm_factory("lint")
    system, user = _graph_reflection_prompt(packet_path, current_path)
    succeeded = False
    try:
        raw = llm_fn(system, [{"role": "user", "content": user}])
        parsed = parse_json_or_repair(llm_fn, raw, _GRAPH_REFLECTION_DELTA_SCHEMA)
        normalized = _compile_graph_reflection_response(parsed, existing_rows, fingerprint)
        _attach_run_provenance(normalized, route_signature, datetime.now(timezone.utc).isoformat())
        _write_jsonl(findings_path, normalized)
        succeeded = True
    finally:
        if succeeded:
            _cleanup_paths(packet_path, current_path)

    _write_stamp(
        root / GRAPH_REFLECTION_STAMP_PATH,
        {
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "graph_fingerprint": fingerprint,
            "cluster_count": len({cluster.get("cluster_id", "") for cluster in bridge_clusters if cluster.get("cluster_id", "")}),
            "material_count": len({mid for cluster in bridge_clusters for mid in _safe_list(cluster.get("material_ids", [])) if mid}),
            "bridge_cluster_count": len([cluster for cluster in bridge_clusters if len(dict.fromkeys(_safe_list(cluster.get("material_ids", [])))) > 1]),
            "cluster_review_count": len(cluster_reviews),
            "concept_reflection_count": len(concept_refs),
            "collection_reflection_count": len(collection_refs),
            "finding_count": len(normalized),
        },
    )
    _write_lint_stage_stamp(root, graph_reflection_at=datetime.now(timezone.utc).isoformat())
    return {
        "graph_maintenance": len(normalized),
        "graph_skipped": False,
        "graph_skip_reason": "",
    }
def run_reflective_lint(
    config: dict,
    deterministic_report: dict,
    *,
    llm_factory=None,
    apply: bool = False,
    scheduled: bool = False,
    stages: list[str] | tuple[str, ...] | None = None,
) -> dict:
    """Run the reflective LLM passes and project outputs to disk."""
    root = get_project_root()
    selected_stages = _normalize_lint_stages(stages) or list(LINT_REFLECTIVE_STAGES)
    manifest_records = _load_manifest(root)
    metas = _load_all_metas(root, manifest_records)
    material_info = _build_material_info(root, manifest_records)
    clusters = _current_concepts(root)
    bridge_clusters = load_bridge_clusters(root)
    groups = _group_materials_by_collection(metas)

    if not get_index_path().exists():
        lint_root = root / LINT_DIR
        lint_root.mkdir(parents=True, exist_ok=True)
        for name in ("cluster_reviews.jsonl", "concept_reflections.jsonl", "collection_reflections.jsonl"):
            (lint_root / name).write_text("", encoding="utf-8")
        return {
            "cluster_reviews": 0,
            "bridge_cluster_changes": 0,
            "bridge_cluster_discovery": 0,
            "concept_reflections": 0,
            "collection_reflections": 0,
            "graph_maintenance": 0,
            "stages": selected_stages,
            "applied": False,
            "skipped": True,
            "graph_skipped": True,
            "graph_skip_reason": "search index missing",
        }

    if llm_factory is None:
        shared_llm_state: dict = {}

        def llm_factory(stage: str) -> LlmFn:
            return make_cli_llm_fn(config, "lint", state=shared_llm_state)
    lint_route_signature = get_model_id(config, "lint")

    def _refresh_sql_and_wiki() -> None:
        compile_wiki(
            config,
            skip_cluster=True,
            recompile_pages=True,
            run_quick_lint=False,
        )

    with ReflectionIndexTool(root) as tool:
        cluster_reviews = _load_jsonl(root / LINT_DIR / "cluster_reviews.jsonl")
        concept_refs = _load_jsonl(root / LINT_DIR / "concept_reflections.jsonl")
        collection_refs = _load_jsonl(root / LINT_DIR / "collection_reflections.jsonl")
        bridge_changes = 0
        cluster_review_count = 0
        concept_reflection_count = 0
        collection_reflection_count = 0
        skipped = True
        skip_reason = ""

        if "cluster-audit" in selected_stages:
            cluster_due, cluster_reason = _cluster_audit_due(root, bridge_clusters)
            if cluster_due:
                cluster_reviews, bridge_changes = _run_cluster_audit(
                    root,
                    clusters,
                    material_info,
                    lint_route_signature,
                    llm_factory,
                    tool,
                )
                cluster_review_count = len(cluster_reviews)
                _refresh_sql_and_wiki()
                clusters = _current_concepts(root)
                bridge_clusters = load_bridge_clusters(root)
                skipped = False
            elif not skip_reason:
                skip_reason = cluster_reason

        if "concept-reflection" in selected_stages:
            concept_due, concept_reason = _concept_reflection_due(root)
            if concept_due:
                concept_refs = _run_concept_reflections(root, clusters, material_info, llm_factory, tool, lint_route_signature)
                concept_reflection_count = len(concept_refs)
                _refresh_sql_and_wiki()
                clusters = _current_concepts(root)
                bridge_clusters = load_bridge_clusters(root)
                skipped = False
            elif not skip_reason:
                skip_reason = concept_reason

        if "collection-reflection" in selected_stages:
            collection_due, collection_reason = _collection_reflection_due(root)
            if collection_due:
                collection_refs = _run_collection_reflections(root, groups, clusters, llm_factory, tool, lint_route_signature)
                collection_reflection_count = len(collection_refs)
                _refresh_sql_and_wiki()
                clusters = _current_concepts(root)
                bridge_clusters = load_bridge_clusters(root)
                skipped = False
            elif not skip_reason:
                skip_reason = collection_reason

        if "graph-maintenance" in selected_stages:
            graph_due, graph_reason = _graph_reflection_due(
                root,
                config,
                bridge_clusters,
                manifest_records,
                cluster_reviews,
                concept_refs,
                collection_refs,
                deterministic_report,
            )
            if graph_due:
                graph_result = _run_graph_reflection(
                    root,
                    deterministic_report,
                    cluster_reviews,
                    concept_refs,
                    collection_refs,
                    clusters,
                    manifest_records,
                    llm_factory,
                    tool,
                    lint_route_signature,
                )
                graph_maintenance = int(graph_result.get("graph_maintenance", 0) or 0)
                graph_skipped = bool(graph_result.get("graph_skipped", False))
                graph_skip_reason = str(graph_result.get("graph_skip_reason", "") or "")
                _refresh_sql_and_wiki()
                skipped = False
            else:
                graph_maintenance = 0
                graph_skipped = True
                graph_skip_reason = graph_reason
                if not skip_reason:
                    skip_reason = graph_reason
        else:
            graph_maintenance = 0
            graph_skipped = True
            graph_skip_reason = "stage not selected"

    # Always refresh memory so the reflection tables are queryable.
    memory_rebuild(config)

    return {
        "cluster_reviews": cluster_review_count,
        "bridge_cluster_changes": bridge_changes,
        "bridge_cluster_discovery": bridge_changes,
        "concept_reflections": concept_reflection_count,
        "collection_reflections": collection_reflection_count,
        "graph_maintenance": graph_maintenance,
        "stages": selected_stages,
        "applied": apply,
        "skipped": skipped,
        "skip_reason": skip_reason,
        "graph_skipped": graph_skipped,
        "graph_skip_reason": graph_skip_reason,
    }


# ---------------------------------------------------------------------------
# Public runner
# ---------------------------------------------------------------------------

def run_lint(
    config: dict | None = None,
    *,
    quick: bool = False,
    full: bool = False,
    report: bool = False,
    fix: bool = False,
    llm_factory=None,
    scheduled: bool = False,
    stages: list[str] | tuple[str, ...] | None = None,
) -> dict:
    """Run lint in quick or full mode and return a structured summary."""

    import datetime
    lint_start_time = datetime.datetime.now()
    root = get_project_root()
    log_path = root / "logs" / "lint.log"

    def _log_value(value) -> str:
        return str(value).replace("\t", " ").replace("\n", " ").strip()

    def _append_log(*fields) -> None:
        try:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            with open(log_path, "a", encoding="utf-8") as f:
                f.write("\t".join(_log_value(field) for field in fields) + "\n")
        except Exception:
            pass

    selected_stages = _normalize_lint_stages(stages)
    requested_mode = "staged" if selected_stages else ("full" if full else "quick")
    _append_log(lint_start_time.isoformat(), "START", requested_mode, fix, report, scheduled)

    try:
        if config is None:
            config = load_config()
        if quick and full:
            raise ValueError("lint cannot be both quick and full")
        if quick and selected_stages:
            raise ValueError("lint --quick cannot be combined with --stage")
        if not quick and not full and not selected_stages:
            quick = True
        apply = fix or full or bool(selected_stages)

        deterministic = run_deterministic_lint(config)
        result = {
            "mode": "staged" if selected_stages else ("full" if full else "quick"),
            "deterministic": deterministic,
            "reflection": None,
            "fixes": None,
            "report_path": str((get_project_root() / REPORT_PATH)),
        }

        if apply:
            result["fixes"] = _apply_deterministic_fixes(deterministic, config)

        if selected_stages or full:
            result["reflection"] = run_reflective_lint(
                config,
                deterministic,
                llm_factory=llm_factory,
                apply=apply,
                scheduled=scheduled,
                stages=selected_stages if selected_stages else None,
            )

        if report or full or fix:
            report_text = render_lint_report(deterministic)
            path = get_project_root() / REPORT_PATH
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(report_text, encoding="utf-8")

        lint_end_time = datetime.datetime.now()
        summary = result.get("deterministic", {}).get("summary", {})
        _append_log(
            lint_start_time.isoformat(),
            lint_end_time.isoformat(),
            result["mode"],
            "DONE",
            f"issues={summary.get('issues', 0)} high={summary.get('high', 0)}",
        )
        return result
    except Exception as exc:
        lint_end_time = datetime.datetime.now()
        _append_log(lint_start_time.isoformat(), lint_end_time.isoformat(), requested_mode, "FAILED", exc)
        raise


def lint_exit_code(result: dict) -> int:
    """Return a deterministic CLI exit code for lint results."""
    deterministic = result.get("deterministic", {}) or {}
    summary = deterministic.get("summary", {}) if isinstance(deterministic, dict) else {}
    if summary.get("high", 0):
        return 2
    if summary.get("issues", 0):
        return 1
    return 0
