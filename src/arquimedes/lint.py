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
import sys
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
from arquimedes.llm import EnrichmentError, LlmFn, get_model_id, make_cli_llm_fn, parse_json_or_repair
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
from arquimedes.lint_cluster_audit import (
    _assign_new_bridge_ids,
    _audit_optional_text,
    _cleanup_cluster_audit_debug_artifacts,
    _cluster_audit_apply_bridge_update,
    _cluster_audit_apply_review_delta,
    _cluster_audit_canonicalize_existing_reviews,
    _cluster_audit_cluster_fingerprint,
    _cluster_audit_cluster_fingerprints,
    _cluster_audit_cluster_snapshot,
    _cluster_audit_due,
    _cluster_audit_existing_review_key,
    _cluster_audit_finalize_reviews,
    _cluster_audit_input_path,
    _cluster_audit_input_paths,
    _cluster_audit_mutable_concept_index,
    _cluster_audit_normalize_review_row,
    _cluster_audit_parsed_response_path,
    _cluster_audit_pending_local_fingerprint,
    _cluster_audit_prompt,
    _cluster_audit_raw_response_path,
    _cluster_audit_review_id,
    _cluster_audit_review_ref,
    _cluster_audit_target_clusters,
    _cluster_audit_validate_bridge_candidate,
    _cover_pairs_from_clusters,
    _ensure_cluster_audit_review_coverage,
)
from arquimedes.lint_collection_reflection import (
    _build_collection_reflection_evidence_payload,
    _collection_reflection_due,
    _collection_reflection_evidence_path,
    _collection_reflection_fingerprint,
    _collection_reflection_key,
    _collection_reflection_local_clusters,
    _collection_reflection_materials,
    _collection_reflection_page_copy_path,
    _collection_reflection_prompt,
    _collection_reflection_scaffold,
    _collection_reflection_stage_dir,
    _compile_collection_reflection_response,
    _format_collection_material_concept,
)
from arquimedes.lint_concept_reflection import (
    _build_concept_reflection_evidence_payload,
    _collect_material_chunk_evidence,
    _compile_concept_reflection_response,
    _concept_reflection_due,
    _concept_reflection_evidence_path,
    _concept_reflection_link_fingerprint,
    _concept_reflection_page_copy_path,
    _concept_reflection_prompt,
    _concept_reflection_scaffold,
    _concept_reflection_stage_dir,
    _format_material_annotation,
    _format_material_concept,
)
from arquimedes.lint_graph_reflection import (
    _compile_graph_reflection_response,
    _graph_reflection_due,
    _graph_reflection_existing_path,
    _graph_reflection_packet,
    _graph_reflection_packet_path,
    _graph_reflection_page_path,
    _graph_reflection_prompt,
    _graph_reflection_stage_dir,
)
from arquimedes.lint_global_bridge import (
    _global_bridge_artifact_path,
    _global_bridge_due,
    _global_bridge_stamp_path,
    load_global_bridge_clusters,
)


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
    "global-bridge",
    "graph-maintenance",
)
DEFAULT_LINT_FULL_STAGES = (
    "cluster-audit",
    "concept-reflection",
    "collection-reflection",
    "global-bridge",
)
_CLUSTER_AUDIT_DELTA_SCHEMA = '{"bridge_updates":[{"cluster_id":"existing bridge id","new_name":"string","new_aliases":["strings"],"new_source_concepts":[{"material_id":"string","concept_name":"string"}],"new_materials":["material ids"],"removed_materials":["material ids"]}],"new_bridges":[{"bridge_ref":"temporary id","canonical_name":"string","aliases":["strings"],"material_ids":["material ids"],"source_concepts":[{"material_id":"string","concept_name":"string"}]}],"review_updates":[{"cluster_id":"existing bridge id","finding_type":"string","severity":"low|medium|high","status":"open|validated","note":"string","recommendation":"string"}],"new_reviews":[{"cluster_ref":"existing bridge id or exact new_bridges.bridge_ref","bridge_ref":"optional alias for cluster_ref on new bridges","finding_type":"string","severity":"low|medium|high","status":"open|validated","note":"string","recommendation":"string"}],"context_requests":[{"tool":"search_material_evidence|open_record","...":"..."}],"_finished":true}'
_CONCEPT_REFLECTION_DELTA_SCHEMA = '{"main_takeaways":["strings"]|null,"main_tensions":["strings"]|null,"open_questions":["strings"]|null,"helpful_new_sources":["strings"]|null,"why_this_concept_matters":"string"|null,"context_requests":[{"tool":"search_material_evidence|open_record","...":"..."}],"_finished":true}'
_COLLECTION_REFLECTION_DELTA_SCHEMA = '{"main_takeaways":["strings"]|null,"main_tensions":["strings"]|null,"important_material_ids":["material ids"]|null,"important_cluster_ids":["cluster ids"]|null,"open_questions":["strings"]|null,"helpful_new_sources":["strings"]|null,"why_this_collection_matters":"string"|null,"context_requests":[{"tool":"search_material_evidence|open_record","...":"..."}],"_finished":true}'
_GRAPH_REFLECTION_DELTA_SCHEMA = '{"findings":[{"finding_id":"string(optional)","finding_type":"string","severity":"low|medium|high","summary":"string","details":"string","affected_material_ids":["material ids"],"affected_cluster_ids":["cluster ids"],"candidate_future_sources":["strings"],"candidate_bridge_links":["strings"]}]|null,"_finished":true}'


def _progress(message: str) -> None:
    """Emit lightweight progress logs for long-running lint operations."""
    print(message, file=sys.stderr, flush=True)


def _reflective_stage_label(stage: str) -> str:
    return stage.replace("-", " ")


def _reflective_stage_started(stage: str) -> None:
    _progress(f"{_reflective_stage_label(stage)} started")


def _reflective_stage_finished(stage: str) -> None:
    _progress(f"{_reflective_stage_label(stage)} finished")


def _reflective_stage_skipped(stage: str, reason: str) -> None:
    detail = f": {reason}" if reason else ""
    _progress(f"{_reflective_stage_label(stage)} skipped{detail}")


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
        self._table_columns_cache: dict[str, set[str]] = {}

    def close(self) -> None:
        self.con.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    @staticmethod
    def _row_to_dict(row: sqlite3.Row, json_list_fields: tuple[str, ...] = ()) -> dict:
        data = {k: row[k] for k in row.keys()}
        for field in json_list_fields:
            if field in data:
                data[field] = _parse_json_list(data[field])
        return data

    def _table_columns(self, table_name: str) -> set[str]:
        cached = self._table_columns_cache.get(table_name)
        if cached is not None:
            return cached
        rows = self.con.execute(f"PRAGMA table_info({table_name})").fetchall()
        columns = {str(row["name"]) for row in rows}
        self._table_columns_cache[table_name] = columns
        return columns

    def _select_optional_row(
        self,
        table_name: str,
        requested_fields: tuple[str, ...],
        where_clause: str,
        params: list[str],
    ) -> sqlite3.Row | None:
        available_fields = [field for field in requested_fields if field in self._table_columns(table_name)]
        if not available_fields:
            return None
        sql = f"SELECT {', '.join(available_fields)} FROM {table_name} WHERE {where_clause}"
        return self.con.execute(sql, params).fetchone()

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
                    reflection = self._select_optional_row(
                        "concept_reflections",
                        (
                            "cluster_id",
                            "slug",
                            "canonical_name",
                            "main_takeaways",
                            "main_tensions",
                            "open_questions",
                            "helpful_new_sources",
                            "why_this_concept_matters",
                            "supporting_material_ids",
                            "supporting_evidence",
                            "input_fingerprint",
                            "wiki_path",
                        ),
                        "cluster_id = ?",
                        [record_id],
                    )
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
                        "reflection": self._row_to_dict(
                            reflection,
                            (
                                "main_takeaways",
                                "main_tensions",
                                "open_questions",
                                "helpful_new_sources",
                                "supporting_material_ids",
                                "supporting_evidence",
                            ),
                        ) if reflection else {},
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
                reflection = self._select_optional_row(
                    "concept_reflections",
                    (
                        "cluster_id",
                        "slug",
                        "canonical_name",
                        "main_takeaways",
                        "main_tensions",
                        "open_questions",
                        "helpful_new_sources",
                        "why_this_concept_matters",
                        "supporting_material_ids",
                        "supporting_evidence",
                        "input_fingerprint",
                        "wiki_path",
                    ),
                    "cluster_id = ?",
                    [record_id],
                )
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
                    "reflection": self._row_to_dict(
                        reflection,
                        (
                            "main_takeaways",
                            "main_tensions",
                            "open_questions",
                            "helpful_new_sources",
                            "supporting_material_ids",
                            "supporting_evidence",
                        ),
                    ) if reflection else {},
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
                reflection = self._select_optional_row(
                    "collection_reflections",
                    (
                        "domain",
                        "collection",
                        "main_takeaways",
                        "main_tensions",
                        "important_material_ids",
                        "important_cluster_ids",
                        "open_questions",
                        "helpful_new_sources",
                        "why_this_collection_matters",
                        "input_fingerprint",
                        "wiki_path",
                    ),
                    "domain = ? AND collection = ?",
                    [domain, collection],
                )
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
                    "reflection": self._row_to_dict(
                        reflection,
                        (
                            "main_takeaways",
                            "main_tensions",
                            "important_material_ids",
                            "important_cluster_ids",
                            "open_questions",
                            "helpful_new_sources",
                        ),
                    ) if reflection else {},
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


def _concept_reflection_targets(root: Path, clusters: list[dict]) -> list[dict]:
    del root
    targets: dict[str, dict] = {}
    for cluster in clusters or []:
        if not isinstance(cluster, dict):
            continue
        cluster_id = str(cluster.get("cluster_id") or cluster.get("bridge_id", "")).strip()
        if not cluster_id:
            continue
        targets[cluster_id] = {**cluster, "cluster_id": cluster_id}
    return list(targets.values())


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
    from arquimedes.lint_cluster_audit import _run_cluster_audit_impl

    return _run_cluster_audit_impl(
        sys.modules[__name__],
        root,
        clusters,
        material_info,
        route_signature,
        llm_factory,
        tool,
    )


def _run_local_cluster_audit(
    root: Path,
    clusters: list[dict],
    material_info: dict[str, dict],
    route_signature: str = "",
    llm_factory=None,
    tool: ReflectionIndexTool | None = None,
) -> tuple[list[dict], int]:
    from arquimedes.lint_cluster_audit import _run_local_cluster_audit_impl

    return _run_local_cluster_audit_impl(
        sys.modules[__name__],
        root,
        clusters,
        material_info,
        route_signature,
        llm_factory,
        tool,
    )


def _run_concept_reflections(
    root: Path,
    clusters: list[dict],
    material_info: dict[str, dict],
    llm_factory=None,
    tool: ReflectionIndexTool | None = None,
    route_signature: str = "",
) -> list[dict]:
    from arquimedes.lint_concept_reflection import _run_concept_reflections_impl

    return _run_concept_reflections_impl(
        sys.modules[__name__],
        root,
        clusters,
        material_info,
        llm_factory,
        tool,
        route_signature,
    )


def _run_collection_reflections(
    root: Path,
    groups: dict[tuple[str, str], list[dict]],
    clusters: list[dict],
    llm_factory=None,
    tool: ReflectionIndexTool | None = None,
    route_signature: str = "",
) -> list[dict]:
    from arquimedes.lint_collection_reflection import _run_collection_reflections_impl

    return _run_collection_reflections_impl(
        sys.modules[__name__],
        root,
        groups,
        clusters,
        llm_factory,
        tool,
        route_signature,
    )


def _run_graph_reflection(
    root: Path,
    deterministic_report: dict,
    concept_refs: list[dict],
    collection_refs: list[dict],
    bridge_clusters: list[dict],
    manifest_records: list[dict],
    llm_factory=None,
    tool: ReflectionIndexTool | None = None,
    route_signature: str = "",
) -> dict:
    from arquimedes.lint_graph_reflection import _run_graph_reflection_impl

    return _run_graph_reflection_impl(
        sys.modules[__name__],
        root,
        deterministic_report,
        concept_refs,
        collection_refs,
        bridge_clusters,
        manifest_records,
        llm_factory,
        tool,
        route_signature,
    )


def _run_global_bridges(
    root: Path,
    local_clusters: list[dict],
    collection_refs: list[dict],
    llm_factory=None,
    tool: ReflectionIndexTool | None = None,
    route_signature: str = "",
) -> dict:
    from arquimedes.lint_global_bridge import _run_global_bridge_impl

    return _run_global_bridge_impl(
        sys.modules[__name__],
        root,
        local_clusters,
        collection_refs,
        llm_factory,
        tool,
        route_signature,
    )


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
    selected_stages = _normalize_lint_stages(stages) or list(DEFAULT_LINT_FULL_STAGES)
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
            "global_bridges": 0,
            "graph_maintenance": 0,
            "stages": selected_stages,
            "applied": False,
            "skipped": True,
            "global_bridge_skipped": True,
            "global_bridge_skip_reason": "search index missing",
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
        global_bridge_count = 0
        skipped = True
        skip_reason = ""
        global_bridge_skipped = True
        global_bridge_skip_reason = "stage not selected"

        if "cluster-audit" in selected_stages:
            cluster_due, cluster_reason = _cluster_audit_due(root, bridge_clusters)
            if cluster_due:
                _reflective_stage_started("cluster-audit")
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
                _reflective_stage_finished("cluster-audit")
                skipped = False
            else:
                _reflective_stage_skipped("cluster-audit", cluster_reason)
                if not skip_reason:
                    skip_reason = cluster_reason

        if "concept-reflection" in selected_stages:
            concept_due, concept_reason = _concept_reflection_due(root)
            if concept_due:
                _reflective_stage_started("concept-reflection")
                concept_refs = _run_concept_reflections(
                    root,
                    _concept_reflection_targets(root, clusters),
                    material_info,
                    llm_factory,
                    tool,
                    lint_route_signature,
                )
                concept_reflection_count = len(concept_refs)
                _refresh_sql_and_wiki()
                clusters = _current_concepts(root)
                bridge_clusters = load_bridge_clusters(root)
                _reflective_stage_finished("concept-reflection")
                skipped = False
            else:
                _reflective_stage_skipped("concept-reflection", concept_reason)
                if not skip_reason:
                    skip_reason = concept_reason

        if "collection-reflection" in selected_stages:
            collection_due, collection_reason = _collection_reflection_due(root)
            if collection_due:
                _reflective_stage_started("collection-reflection")
                collection_refs = _run_collection_reflections(root, groups, clusters, llm_factory, tool, lint_route_signature)
                collection_reflection_count = len(collection_refs)
                _refresh_sql_and_wiki()
                clusters = _current_concepts(root)
                bridge_clusters = load_bridge_clusters(root)
                _reflective_stage_finished("collection-reflection")
                skipped = False
            else:
                _reflective_stage_skipped("collection-reflection", collection_reason)
                if not skip_reason:
                    skip_reason = collection_reason

        if "global-bridge" in selected_stages:
            local_clusters = load_local_clusters(root)
            global_bridge_due, global_bridge_reason = _global_bridge_due(root, local_clusters, collection_refs)
            if global_bridge_due:
                _reflective_stage_started("global-bridge")
                global_bridge_result = _run_global_bridges(
                    root,
                    local_clusters,
                    collection_refs,
                    llm_factory,
                    tool,
                    lint_route_signature,
                )
                global_bridge_count = int(global_bridge_result.get("global_bridges", 0) or 0)
                global_bridge_skipped = bool(global_bridge_result.get("global_bridge_skipped", False))
                global_bridge_skip_reason = str(global_bridge_result.get("global_bridge_skip_reason", "") or "")
                if not global_bridge_skipped:
                    _refresh_sql_and_wiki()
                    clusters = _current_concepts(root)
                    bridge_clusters = load_global_bridge_clusters(root)
                _reflective_stage_finished("global-bridge")
                skipped = False
            else:
                global_bridge_count = 0
                global_bridge_skipped = True
                global_bridge_skip_reason = global_bridge_reason
                _reflective_stage_skipped("global-bridge", global_bridge_reason)
                if not skip_reason:
                    skip_reason = global_bridge_reason

        if "graph-maintenance" in selected_stages:
            graph_due, graph_reason = _graph_reflection_due(
                root,
                config,
                bridge_clusters,
                manifest_records,
                concept_refs,
                collection_refs,
                deterministic_report,
            )
            if graph_due:
                _reflective_stage_started("graph-maintenance")
                graph_result = _run_graph_reflection(
                    root,
                    deterministic_report,
                    concept_refs,
                    collection_refs,
                    bridge_clusters,
                    manifest_records,
                    llm_factory,
                    tool,
                    lint_route_signature,
                )
                graph_maintenance = int(graph_result.get("graph_maintenance", 0) or 0)
                graph_skipped = bool(graph_result.get("graph_skipped", False))
                graph_skip_reason = str(graph_result.get("graph_skip_reason", "") or "")
                _refresh_sql_and_wiki()
                _reflective_stage_finished("graph-maintenance")
                skipped = False
            else:
                graph_maintenance = 0
                graph_skipped = True
                graph_skip_reason = graph_reason
                _reflective_stage_skipped("graph-maintenance", graph_reason)
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
        "global_bridges": global_bridge_count,
        "graph_maintenance": graph_maintenance,
        "stages": selected_stages,
        "applied": apply,
        "skipped": skipped,
        "skip_reason": skip_reason,
        "global_bridge_skipped": global_bridge_skipped,
        "global_bridge_skip_reason": global_bridge_skip_reason,
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
