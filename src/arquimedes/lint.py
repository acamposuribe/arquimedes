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
import sqlite3
import threading
from datetime import datetime, timezone
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from arquimedes.cluster import (
    _load_concept_rows,
    _load_material_rows,
    _cluster_output_path,
    _stage_bridge_packet_input,
    _validate_bridge_and_attach_provenance,
    is_bridge_clustering_stale,
    bridge_cluster_fingerprint,
    load_bridge_clusters,
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
from arquimedes.memory import memory_rebuild


LINT_DIR = "derived/lint"
REPORT_PATH = Path("wiki/_lint_report.md")
FULL_LINT_STAMP_PATH = Path(LINT_DIR) / "full_lint_stamp.json"
GRAPH_REFLECTION_STAMP_PATH = Path(LINT_DIR) / "graph_reflection_stamp.json"
DEFAULT_FULL_LINT_INTERVAL_HOURS = 168.0
DEFAULT_GRAPH_REFLECTION_INTERVAL_HOURS = 168.0
DEFAULT_GRAPH_REFLECTION_MIN_CLUSTER_DELTA = 3
DEFAULT_GRAPH_REFLECTION_MIN_MATERIAL_DELTA = 5
MAX_CONTEXT_REQUESTS_PER_PASS = 4
_LINK_RE = re.compile(r"(?<!!)\[[^\]]+\]\(([^)]+)\)")
_FENCE_RE = re.compile(r"```.*?```", re.DOTALL)


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
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _read_stamp(path: Path) -> dict:
    return _load_json(path, {}) or {}


def _write_stamp(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


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

    def _material_evidence(self, material_id: str, query_terms: list[str] | None = None) -> dict:
        query_terms = [str(term).lower() for term in (query_terms or []) if str(term).strip()]
        with self._lock:
            chunks = []
            for row in self.con.execute(
                """
                SELECT chunk_id, summary, text, source_pages, emphasized, content_class
                FROM chunks
                WHERE material_id = ?
                ORDER BY emphasized DESC, rowid
                LIMIT 4
                """,
                [material_id],
            ).fetchall():
                text = row["text"] or ""
                summary = row["summary"] or ""
                haystack = f"{summary} {text}".lower()
                score = 1 if any(term in haystack for term in query_terms) else 0
                chunks.append({
                    "chunk_id": row["chunk_id"],
                    "source_pages": _parse_json_list(row["source_pages"]),
                    "content_class": row["content_class"],
                    "summary": summary,
                    "excerpt": _truncate(text or summary, 180),
                    "relevance": score,
                })
            chunks.sort(key=lambda item: (-item["relevance"], item["chunk_id"]))

            annotations = []
            for row in self.con.execute(
                """
                SELECT annotation_id, type, page, quoted_text, comment
                FROM annotations
                WHERE material_id = ?
                ORDER BY (CASE WHEN comment = '' THEN 1 ELSE 0 END), rowid
                LIMIT 2
                """,
                [material_id],
            ).fetchall():
                annotations.append({
                    "annotation_id": row["annotation_id"],
                    "type": row["type"],
                    "page": row["page"],
                    "quoted_text": _truncate(row["quoted_text"] or "", 180),
                    "comment": _truncate(row["comment"] or "", 160),
                })

            figures = []
            for row in self.con.execute(
                """
                SELECT figure_id, description, visual_type, source_page, relevance
                FROM figures
                WHERE material_id = ?
                ORDER BY (CASE WHEN relevance = 'substantive' THEN 0 ELSE 1 END), rowid
                LIMIT 2
                """,
                [material_id],
            ).fetchall():
                figures.append({
                    "figure_id": row["figure_id"],
                    "description": _truncate(row["description"] or "", 180),
                    "visual_type": row["visual_type"],
                    "source_page": row["source_page"],
                    "relevance": row["relevance"],
                })

            concepts = []
            for row in self.con.execute(
                """
                SELECT concept_name, concept_type, relevance, source_pages, evidence_spans, confidence
                FROM concepts
                WHERE material_id = ?
                ORDER BY (CASE WHEN concept_type = 'bridge' THEN 0 ELSE 1 END), rowid
                LIMIT 4
                """,
                [material_id],
            ).fetchall():
                concepts.append({
                    "concept_name": row["concept_name"],
                    "concept_type": row["concept_type"],
                    "relevance": row["relevance"],
                    "source_pages": _parse_json_list(row["source_pages"]),
                    "evidence_spans": _parse_json_list(row["evidence_spans"]),
                    "confidence": row["confidence"] or 0.0,
                })

            return {
                "chunks": chunks[:2],
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

    def search_concepts(self, query: str, limit: int = 5) -> list[dict]:
        query = (query or "").strip()
        if not query:
            return []
        with self._lock:
            clusters = []
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
                    "evidence": self._material_evidence(record_id),
                }
            if kind == "concept":
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
                       open_questions, input_fingerprint, wiki_path
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
                return {
                    "kind": "collection",
                    "domain": domain,
                    "collection": collection,
                    "wiki_page": self._row_to_dict(wiki_row) if wiki_row else {},
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
    if tool not in {"search_materials", "search_concepts", "search_collections", "open_record"}:
        return None
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
    return json.dumps(results, ensure_ascii=False, indent=2)


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
) -> Any:
    raw = llm_fn(system, [{"role": "user", "content": user}])
    parsed = parse_json_or_repair(llm_fn, raw, schema_description)
    requests = _extract_context_requests(parsed)
    if not requests:
        if isinstance(parsed, dict):
            parsed.pop("context_requests", None)
        return parsed

    tool_results = _execute_context_requests(tool, requests)
    followup_user = (
        f"{user}\n\n"
        "You requested more context from the read-only SQL-index tool.\n"
        "Tool results:\n"
        f"{_format_context_tool_results(tool_results)}\n\n"
        "Revise your answer using the added context. Return final JSON only."
    )
    raw = llm_fn(system, [{"role": "user", "content": followup_user}])
    parsed = parse_json_or_repair(llm_fn, raw, schema_description)
    if isinstance(parsed, dict):
        parsed.pop("context_requests", None)
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
    return load_bridge_clusters(root)


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
    clusters_fp = canonical_hash(
        _read_text(root / "derived" / "bridge_concept_clusters.jsonl"),
        _read_text(root / "derived" / "lint" / "cluster_reviews.jsonl"),
        _read_text(root / "derived" / "lint" / "concept_reflections.jsonl"),
        _read_text(root / "derived" / "lint" / "collection_reflections.jsonl"),
        _read_text(root / "derived" / "lint" / "graph_findings.jsonl"),
    )
    manifest_fp = canonical_hash(_read_text(root / "manifests" / "materials.jsonl"))
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


def _deterministic_fingerprint(report: dict) -> str:
    return canonical_hash(
        report.get("summary", {}),
        report.get("issues", []),
    )


def _full_lint_due(root: Path, deterministic_report: dict, config: dict) -> tuple[bool, str]:
    stamp = _read_stamp(root / FULL_LINT_STAMP_PATH)
    if not stamp:
        return True, "full lint stamp missing"
    current_fp = _deterministic_fingerprint(deterministic_report)
    if stamp.get("deterministic_fingerprint") == current_fp:
        checked_at = _parse_iso_datetime(stamp.get("checked_at"))
        if checked_at is None:
            return True, "full lint stamp invalid"
        hours = float(_lint_schedule_config(config).get("full_schedule_min_hours", DEFAULT_FULL_LINT_INTERVAL_HOURS) or DEFAULT_FULL_LINT_INTERVAL_HOURS)
        age_hours = (datetime.now(timezone.utc) - checked_at).total_seconds() / 3600.0
        if age_hours < hours:
            return False, "full lint is current"
    return True, "deterministic graph inputs changed or interval elapsed"


def _write_full_lint_stamp(root: Path, deterministic_report: dict) -> None:
    _write_stamp(
        root / FULL_LINT_STAMP_PATH,
        {
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "deterministic_fingerprint": _deterministic_fingerprint(deterministic_report),
            "summary": deterministic_report.get("summary", {}),
        },
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
    stamp = _read_stamp(root / GRAPH_REFLECTION_STAMP_PATH)
    payload = {
        "deterministic_report": deterministic_report.get("summary", {}),
        "cluster_reviews": cluster_reviews[:20],
        "concept_reflections": concept_refs[:20],
        "collection_reflections": collection_refs[:20],
    }
    current_fp = canonical_hash(payload)
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


def _cluster_prompt_payload(
    cluster: dict,
    material_info: dict[str, dict],
    concept_page_text: str,
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

    current_reflection = _extract_marked_section(concept_page_text, "concept-reflection")
    previous_reflection = tool.open_record("concept", cluster.get("cluster_id", "")) if tool else None

    supporting_materials = []
    query_terms = [
        cluster.get("canonical_name", ""),
        *(_safe_list(cluster.get("aliases", []))),
        *[sc.get("concept_name", "") for sc in source_concepts if sc.get("concept_name")],
    ]
    for mid in unique_material_ids:
        info = material_info.get(mid, {})
        evidence = tool._material_evidence(mid, query_terms) if tool else {}
        supporting_materials.append({
            "material_id": mid,
            "title": info.get("title", mid),
            "summary": info.get("summary", ""),
            "keywords": info.get("keywords", []),
            "relevance": next((sc.get("relevance", "") for sc in source_concepts if sc.get("material_id") == mid), ""),
            "concept_names": sorted({sc.get("concept_name", "") for sc in source_concepts if sc.get("material_id") == mid and sc.get("concept_name")}),
            "evidence_spans": sorted({
                span
                for sc in source_concepts
                if sc.get("material_id") == mid
                for span in _safe_list(sc.get("evidence_spans", []))
            }),
            "source_pages": sorted({
                int(p)
                for sc in source_concepts
                if sc.get("material_id") == mid
                for p in _safe_list(sc.get("source_pages", []))
                if str(p).isdigit()
            }),
            "evidence": evidence,
        })

    return {
        "cluster_id": cluster.get("cluster_id", ""),
        "canonical_name": cluster.get("canonical_name", ""),
        "slug": cluster.get("slug", ""),
        "aliases": cluster.get("aliases", []),
        "material_count": len(unique_material_ids),
        "materials": supporting_materials,
        "current_reflection": current_reflection,
        "previous_reflection": previous_reflection.get("reflection", {}) if isinstance(previous_reflection, dict) else {},
        "concept_page": concept_page_text,
        "query_terms": [term for term in query_terms if term],
    }


def _cluster_audit_paths(root: Path) -> tuple[Path, Path]:
    return (
        root / LINT_DIR / "cluster_audit_input.json",
        root / LINT_DIR / "cluster_audit_output.json",
    )


def _collection_prompt_payload(
    domain: str,
    collection: str,
    metas: list[dict],
    page_text: str,
    clusters: list[dict],
    tool: ReflectionIndexTool | None = None,
) -> dict:
    materials = []
    mid_set = {meta.get("material_id", "") for meta in metas if meta.get("material_id")}
    for meta in metas:
        mid = meta.get("material_id", "")
        query_terms = [domain, collection, meta.get("title", ""), _meta_val(meta.get("summary"))]
        material_record = tool._material_evidence(mid, query_terms) if tool else {}
        materials.append({
            "material_id": mid,
            "title": meta.get("title", ""),
            "summary": _meta_val(meta.get("summary")),
            "keywords": _safe_list((_meta_val(meta.get("keywords")) or "").split(",")),
            "evidence": material_record,
        })
    concepts = []
    for cluster in clusters:
        overlap = mid_set & set(cluster.get("material_ids", []))
        if overlap:
            concept_record = tool.open_record("concept", cluster.get("cluster_id", "")) if tool else None
            concepts.append({
                "cluster_id": cluster.get("cluster_id", ""),
                "canonical_name": cluster.get("canonical_name", ""),
                "slug": cluster.get("slug", ""),
                "material_count": len(overlap),
                "reflection": concept_record.get("reflection", {}) if isinstance(concept_record, dict) else {},
            })
    current_reflection = _extract_marked_section(page_text, "collection-reflection")
    previous_reflection = tool.open_record("collection", f"{domain}/{collection}") if tool else None
    return {
        "domain": domain,
        "collection": collection,
        "materials": materials,
        "concepts": concepts,
        "current_reflection": current_reflection,
        "previous_reflection": previous_reflection.get("reflection", {}) if isinstance(previous_reflection, dict) else {},
        "collection_page": page_text,
    }


def _cluster_audit_prompt(root: Path, input_path: Path, output_path: Path, bridge_output_path: Path) -> tuple[str, str]:
    system = (
        "You are an architecture research librarian auditing the bridge concept graph. "
        "Return JSON only. Read the bridge memory file and the cluster findings file directly. Then read the "
        "cluster-audit packet file. If that packet points to a bridge packet file, read it too for copied "
        "material context and local concepts.\n"
        "Task 1: audit the current bridge memory for over-merges, missed equivalences, weak naming, "
        "single-material weakness, missing materials, and bridge concepts that should merge, rename, or be improved.\n"
        "Task 2: use the bridge packet's local concepts and material context to discover genuinely new bridge concepts.\n"
        "Prefer ambitious, useful connections and preserve existing bridge concepts whenever they still form a coherent cross-material idea. "
        "Treat splitting as a last resort: only split when the evidence clearly shows that one bridge cluster is conflating distinct intellectual territories that cannot remain together. "
        "If a cluster is broad but still coherent, keep it and improve the canonical name instead of fragmenting it. "
        "Respect prior bridge memory and prior cluster findings whenever possible. "
        f"If you need more context, include a context_requests array with up to "
        f"{MAX_CONTEXT_REQUESTS_PER_PASS} read-only SQL-index lookups from the allowed toolset. "
        "You will get only one read-only context round, so request everything you need at once.\n"
        "When finished, write the outputs and, ONLY AFTER, emit exactly PROCESS_FINISHED on a single line. Do not say anything else."
    )
    user = (
        f"Read the current bridge memory from {root / 'derived' / 'bridge_concept_clusters.jsonl'}.\n"
        f"Read the current cluster findings from {root / LINT_DIR / 'cluster_reviews.jsonl'}.\n"
        f"Read the cluster-audit packet file from {input_path}.\n"
        "That packet may include the path to a bridge packet file containing copied material context and local concepts.\n"
        "Return one JSON object with keys: findings, new_clusters, context_requests.\n"
        "findings must be a JSON array of bridge audit findings. Each finding may include cluster_id or target_cluster_ids if it applies "
        "to specific bridge clusters.\n"
        "new_clusters must be a JSON array of proposed new bridge clusters discovered from the local concepts.\n"
        "context_requests is optional and must be a JSON array of read-only SQL-index lookups if you need more context.\n"
        "Each finding must include: finding_type, severity, recommendation, affected_material_ids, affected_concept_names, evidence.\n"
        f"Write the findings JSON to {output_path} using the Write tool.\n"
        f"Write the updated bridge_concept_clusters JSON to {bridge_output_path} using the Write tool.\n"
        "After writing both files, emit exactly PROCESS_FINISHED on a single line and stop."
    )
    return system, user


def _concept_reflection_prompt(cluster: dict, payload: dict) -> tuple[str, str]:
    system = (
        "You are an architecture research librarian writing reflective synthesis "
        "for a concept page. Return JSON only. Preserve prior conclusions, revise "
        "them when evidence changes, and request more read-only SQL-index context "
        "only if the provided packet is insufficient. You will get only one "
        "read-only context round, so request everything you need at once."
    )
    user = (
        "Write reflective synthesis for this concept cluster.\n"
        "Return a JSON object with keys: main_takeaways, main_tensions, open_questions, "
        "why_this_concept_matters, context_requests.\n"
        "context_requests is optional and must be a JSON array of read-only SQL-index lookups "
        "if you need more context.\n\n"
        f"{json.dumps(payload, ensure_ascii=False, indent=2)}"
    )
    return system, user


def _collection_reflection_prompt(domain: str, collection: str, payload: dict) -> tuple[str, str]:
    system = (
        "You are an architecture research librarian writing reflective synthesis "
        "for a collection page. Return JSON only. Preserve prior conclusions, revise "
        "them when evidence changes, and request more read-only SQL-index context "
        "only if the provided packet is insufficient. You will get only one "
        "read-only context round, so request everything you need at once."
    )
    user = (
        "Write reflective synthesis for this collection.\n"
        "Return a JSON object with keys: main_takeaways, main_tensions, important_material_ids, "
        "important_cluster_ids, open_questions, context_requests.\n"
        "context_requests is optional and must be a JSON array of read-only SQL-index lookups "
        "if you need more context.\n\n"
        f"{json.dumps(payload, ensure_ascii=False, indent=2)}"
    )
    return system, user


def _graph_reflection_prompt(
    report: dict,
    cluster_findings: list[dict],
    concept_refs: list[dict],
    collection_refs: list[dict],
) -> tuple[str, str]:
    system = (
        "You are an architecture research librarian reviewing the whole graph. "
        "Return JSON only. If you need more context, include a context_requests array "
        f"with up to {MAX_CONTEXT_REQUESTS_PER_PASS} read-only SQL-index lookups from the allowed toolset. "
        "You will get only one read-only context round, so request everything you need at once."
    )
    user = (
        "Review the graph globally for missing cross-references, contradictions, "
        "under-connected materials, under-connected clusters, unanswered questions, "
        "and candidate future sources or bridge links.\n"
        "Return a JSON object with keys: findings, context_requests.\n"
        "findings must be a JSON array. context_requests is optional and must be a JSON array "
        "of read-only SQL-index lookups if you need more context.\n\n"
        f"Deterministic report summary:\n{json.dumps(report.get('summary', {}), ensure_ascii=False, indent=2)}\n\n"
        f"Cluster findings:\n{json.dumps(cluster_findings[:20], ensure_ascii=False, indent=2)}\n\n"
        f"Concept reflections:\n{json.dumps(concept_refs[:10], ensure_ascii=False, indent=2)}\n\n"
        f"Collection reflections:\n{json.dumps(collection_refs[:10], ensure_ascii=False, indent=2)}"
    )
    return system, user


def _bridge_maintenance_prompt(
    maintenance_input_path: Path,
    maintenance_output_path: Path,
) -> tuple[str, str]:
    system = (
        "You are an architecture research librarian maintaining the bridge-concept graph. "
        "Return JSON only. Read the bridge-maintenance packet file and use the cluster reviews as maintainer feedback. "
        "If the reviews indicate that two bridge concepts are duplicates or should merge, "
        "propose a merge action. If a bridge concept has a better canonical name, propose a rename. "
        "If a bridge concept is clearly over-merged or internally inconsistent, propose a split. "
        "If a bridge concept is indefensible, propose a delete. "
        "Only propose safe actions that preserve the underlying evidence and prior work whenever possible. "
        "You will get only one read-only context round, so request everything you need at once."
    )
    user = (
        f"Read the bridge-maintenance packet file from {maintenance_input_path}.\n"
        "Review the bridge clusters and cluster audit findings in that file, then return a JSON object "
        "with keys: actions, context_requests.\n"
        "actions must be a JSON array. context_requests is optional and must be a JSON array of "
        "read-only SQL-index lookups if you need more context.\n"
        "Each action must include: action_type (merge|rename|split|delete|keep), target_cluster_ids, "
        "canonical_name, aliases, reason, confidence.\n"
        "Only use action_type=merge when the target clusters are truly the same cross-material concept.\n"
        "Only use action_type=rename when the cluster is correct but badly named.\n"
        "Use action_type=split when one bridge cluster should become two or more bridge clusters; include split_clusters.\n"
        "Use action_type=delete only when the cluster is clearly invalid.\n"
        f"Write the updated bridge-maintenance JSON to {maintenance_output_path} using the Write tool.\n"
        "Do not stream JSON into the response.\n"
        "Confirm with a single line when done.\n"
    )
    return system, user


def _bridge_discovery_prompt(
    local_packets_path: Path,
    bridge_clusters_path: Path,
    output_path: Path,
) -> tuple[str, str]:
    system = (
        "You are an architecture research librarian discovering new bridge concepts from local concepts. "
        "Return JSON only. Preserve the existing bridge graph and only propose genuinely new bridge concepts "
        "when the local concepts clearly connect multiple materials. Do not recreate the existing bridge graph "
        "from scratch; build on the bridge memory file you are given. You will get only one read-only "
        "context round, so request everything you need at once."
    )
    user = (
        f"Read the local concept packet file from {local_packets_path}.\n"
        f"Read the current bridge memory file from {bridge_clusters_path}.\n"
        "Use the local concepts as discovery material and propose only genuinely new bridge concepts.\n"
        "Do not duplicate bridge concepts already represented in the bridge memory file.\n"
        "Bridge clusters must connect at least two materials.\n"
        f"Write only the new bridge clusters to {output_path} using the Write tool.\n"
        "Do not stream JSON into the response.\n"
        "Confirm with a single line when done.\n\n"
        "Return a JSON object with keys: clusters, context_requests.\n"
        "clusters must be a JSON array of bridge cluster objects.\n"
        "Each cluster object should include: canonical_name, aliases, source_concepts, confidence.\n"
        "source_concepts must be a JSON array of {material_id, concept_name} entries.\n"
        "context_requests is optional and must be a JSON array of read-only SQL-index lookups if you need more context."
    )
    return system, user


def _run_reflection_prompt_with_context(
    llm_fn,
    system: str,
    user: str,
    schema_description: str,
    tool: ReflectionIndexTool | None,
) -> Any:
    raw = llm_fn(system, [{"role": "user", "content": user}])
    parsed = parse_json_or_repair(llm_fn, raw, schema_description)
    requests = _extract_context_requests(parsed)
    if requests and tool is not None:
        tool_results = _execute_context_requests(tool, requests)
        followup_user = (
            f"{user}\n\n"
            "You requested more context from the read-only SQL-index tool.\n"
            "Tool results:\n"
            f"{_format_context_tool_results(tool_results)}\n\n"
            "Revise your answer using the added context. Return final JSON only."
        )
        raw = llm_fn(system, [{"role": "user", "content": followup_user}])
        parsed = parse_json_or_repair(llm_fn, raw, schema_description)
    if isinstance(parsed, dict):
        parsed.pop("context_requests", None)
    return parsed


def _merge_bridge_clusters(
    bridge_clusters: list[dict],
    action: dict,
) -> dict | None:
    target_ids = []
    seen_ids: set[str] = set()
    for cid in _safe_list(action.get("target_cluster_ids", [])):
        if cid and cid not in seen_ids:
            seen_ids.add(cid)
            target_ids.append(cid)
    if len(target_ids) < 2:
        return None

    by_id = {cluster.get("cluster_id", ""): cluster for cluster in bridge_clusters if cluster.get("cluster_id", "")}
    selected = [by_id[cid] for cid in target_ids if cid in by_id]
    if len(selected) < 2:
        return None

    survivor = selected[0]
    merged_source: list[dict] = []
    merged_material_ids: list[str] = []
    merged_aliases = []
    seen_pairs: set[tuple[str, str]] = set()
    for cluster in selected:
        merged_aliases.extend(_safe_list(cluster.get("aliases", [])))
        for mid in _safe_list(cluster.get("material_ids", [])):
            if mid and mid not in merged_material_ids:
                merged_material_ids.append(mid)
        for source in cluster.get("source_concepts", []):
            if not isinstance(source, dict):
                continue
            pair = (str(source.get("material_id", "")).strip(), str(source.get("concept_name", "")).strip())
            if not pair[0] or not pair[1] or pair in seen_pairs:
                continue
            seen_pairs.add(pair)
            merged_source.append({
                "material_id": pair[0],
                "concept_name": pair[1],
                "relevance": source.get("relevance", ""),
                "source_pages": source.get("source_pages", []),
                "evidence_spans": source.get("evidence_spans", []),
                "confidence": source.get("confidence", 0.0),
            })

    canonical_name = str(action.get("canonical_name", "")).strip() or survivor.get("canonical_name", "")
    aliases = _dedupe_strings([
        canonical_name,
        *merged_aliases,
        *[src["concept_name"] for src in merged_source],
        *(_safe_list(action.get("aliases", []))),
    ])

    merged = {
        **survivor,
        "canonical_name": canonical_name,
        "slug": slugify(canonical_name),
        "aliases": aliases,
        "material_ids": merged_material_ids,
        "source_concepts": merged_source,
        "confidence": float(action.get("confidence", survivor.get("confidence", 0.0)) or 0.0),
        "wiki_path": f"wiki/shared/bridge-concepts/{slugify(canonical_name)}.md",
    }
    return merged


def _split_bridge_clusters(
    bridge_clusters: list[dict],
    action: dict,
) -> list[dict] | None:
    target_ids = [cid for cid in _safe_list(action.get("target_cluster_ids", [])) if cid]
    if len(target_ids) != 1:
        return None
    target_id = target_ids[0]
    existing = {cluster.get("cluster_id", ""): cluster for cluster in bridge_clusters if cluster.get("cluster_id", "")}
    if target_id not in existing:
        return None
    split_clusters = action.get("split_clusters", [])
    if not isinstance(split_clusters, list) or not split_clusters:
        return None

    target = existing[target_id]
    concept_index: dict[tuple[str, str], dict] = {}
    for source in target.get("source_concepts", []) or []:
        if not isinstance(source, dict):
            continue
        material_id = str(source.get("material_id", "")).strip()
        concept_name = str(source.get("concept_name", "")).strip()
        if not material_id or not concept_name:
            continue
        concept_key = str(source.get("concept_key", "")).strip() or concept_name.lower().strip()
        concept_index[(material_id, concept_key)] = {
            "concept_name": concept_name,
            "concept_key": concept_key,
            "material_id": material_id,
            "relevance": source.get("relevance", ""),
            "source_pages": source.get("source_pages", []),
            "evidence_spans": source.get("evidence_spans", []),
            "confidence": source.get("confidence", 0.0),
        }

    validated = _validate_bridge_and_attach_provenance(split_clusters, concept_index, {})
    if not validated:
        return None

    remaining = [cluster for cluster in bridge_clusters if cluster.get("cluster_id", "") != target_id]
    start = _next_bridge_cluster_index(remaining)
    reassigned = []
    for idx, cluster in enumerate(validated, start=start):
        reassigned.append({
            **cluster,
            "cluster_id": f"bridge_{idx:04d}",
            "wiki_path": f"wiki/shared/bridge-concepts/{cluster.get('slug', '')}.md" if cluster.get("slug", "") else "",
        })
    return remaining + reassigned


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


def _apply_bridge_cluster_maintenance(
    root: Path,
    bridge_clusters: list[dict],
    actions: list[dict],
) -> tuple[list[dict], int]:
    """Apply safe bridge-cluster maintenance actions and persist them."""
    if not actions:
        return bridge_clusters, 0

    working = {cluster.get("cluster_id", ""): cluster for cluster in bridge_clusters if cluster.get("cluster_id", "")}
    changed = 0

    for action in actions:
        if not isinstance(action, dict):
            continue
        action_type = str(action.get("action_type", "")).strip().lower()
        target_ids = [cid for cid in _safe_list(action.get("target_cluster_ids", [])) if cid in working]
        if action_type == "merge":
            if len(target_ids) < 2:
                continue
            merged = _merge_bridge_clusters([working[cid] for cid in target_ids], action)
            if merged is None:
                continue
            survivor_id = target_ids[0]
            for cid in target_ids[1:]:
                working.pop(cid, None)
            working[survivor_id] = merged
            changed += 1
        elif action_type == "rename":
            if len(target_ids) != 1:
                continue
            cid = target_ids[0]
            cluster = working[cid]
            canonical_name = str(action.get("canonical_name", "")).strip() or cluster.get("canonical_name", "")
            cluster = {
                **cluster,
                "canonical_name": canonical_name,
                "slug": slugify(canonical_name),
                "aliases": _dedupe_strings([
                    canonical_name,
                    *(_safe_list(cluster.get("aliases", []))),
                    *(_safe_list(action.get("aliases", []))),
                ]),
                "wiki_path": f"wiki/shared/bridge-concepts/{slugify(canonical_name)}.md",
            }
            working[cid] = cluster
            changed += 1
        elif action_type == "delete":
            if len(target_ids) != 1:
                continue
            working.pop(target_ids[0], None)
            changed += 1
        elif action_type == "split":
            split = _split_bridge_clusters(list(working.values()), action)
            if split is None:
                continue
            working = {cluster.get("cluster_id", ""): cluster for cluster in split if cluster.get("cluster_id", "")}
            changed += 1
        elif action_type == "keep":
            continue

    if changed <= 0:
        return bridge_clusters, 0

    updated = sorted(working.values(), key=lambda c: c.get("cluster_id", ""))
    derived_dir = root / "derived"
    derived_dir.mkdir(exist_ok=True)
    bridge_path = derived_dir / "bridge_concept_clusters.jsonl"
    with bridge_path.open("w", encoding="utf-8") as f:
        for cluster in updated:
            f.write(json.dumps(cluster, ensure_ascii=False) + "\n")

    stamp_path = derived_dir / "bridge_cluster_stamp.json"
    stamp_path.write_text(
        json.dumps({
            "clustered_at": datetime.now(timezone.utc).isoformat(),
            "fingerprint": bridge_cluster_fingerprint(None),
            "bridge_concepts": sum(len(c.get("source_concepts", [])) for c in updated),
            "clusters": len(updated),
        }, indent=2),
        encoding="utf-8",
    )
    return updated, changed


def _build_material_info(root: Path, manifest_records: list[dict]) -> dict[str, dict]:
    metas = _load_all_metas(root, manifest_records)
    info: dict[str, dict] = {}
    for mid, meta in metas.items():
        keywords_field = meta.get("keywords")
        if isinstance(keywords_field, dict):
            keywords = _safe_list(keywords_field.get("value", []))
        elif isinstance(keywords_field, list):
            keywords = [str(v) for v in keywords_field if str(v).strip()]
        else:
            keywords = []
        info[mid] = {
            "title": meta.get("title", mid),
            "summary": _meta_val(meta.get("summary")),
            "keywords": keywords,
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
    existing_rows = _load_jsonl(root / LINT_DIR / "cluster_reviews.jsonl")
    existing_by_cluster: dict[str, list[dict]] = defaultdict(list)
    for row in existing_rows:
        cluster_id = str(row.get("cluster_id", "")).strip()
        if cluster_id:
            existing_by_cluster[cluster_id].append(row)
    output: list[dict] = []
    local_rows, material_rows = _load_local_concepts(root)
    bridge_packets_path = None
    if local_rows and material_rows:
        bridge_packets_path = _stage_bridge_packet_input(root, local_rows, material_rows)

    packet = {
        "bridge_memory_path": str(root / "derived" / "bridge_concept_clusters.jsonl"),
        "cluster_reviews_path": str(root / LINT_DIR / "cluster_reviews.jsonl"),
        "bridge_packets_path": str(bridge_packets_path) if bridge_packets_path else "",
        "bridge_memory_count": len(clusters),
        "bridge_packet_material_count": len(material_rows) if bridge_packets_path else 0,
        "bridge_packet_local_concept_count": len(local_rows) if bridge_packets_path else 0,
        "lint_route_signature": route_signature,
    }
    fingerprint = canonical_hash(packet)
    if existing_rows and all(row.get("input_fingerprint") == fingerprint for row in existing_rows):
        return existing_rows, 0

    input_path, output_path = _cluster_audit_paths(root)
    bridge_output_path = root / "derived" / "tmp" / "cluster_audit_bridge_output.json"
    _write_json(input_path, packet)
    if output_path.exists():
        output_path.unlink()
    bridge_output_path.parent.mkdir(parents=True, exist_ok=True)
    bridge_output_path.write_text("", encoding="utf-8")
    llm_fn = llm_factory("lint")
    system, user = _cluster_audit_prompt(root, input_path, output_path, bridge_output_path)
    parsed = _run_reflection_prompt_with_context(
        llm_fn,
        system,
        user,
        "JSON object with keys: findings (array of findings) and optional context_requests (array of read-only SQL-index lookups)",
        tool,
    )
    if output_path.exists() and output_path.stat().st_size > 0:
        try:
            parsed = json.loads(output_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            pass
    bridge_output: dict[str, Any] | list[Any] | None = None
    if bridge_output_path.exists() and bridge_output_path.stat().st_size > 0:
        try:
            bridge_output = json.loads(bridge_output_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            bridge_output = None
    if parsed is not None:
        _write_json(output_path, parsed if isinstance(parsed, (dict, list)) else {"findings": parsed})
    findings = parsed.get("findings", []) if isinstance(parsed, dict) else parsed
    if not isinstance(findings, list):
        raise EnrichmentError("Cluster audit returned non-list findings")

    bridge_discovery = 0
    for idx, finding in enumerate(findings):
        if not isinstance(finding, dict):
            continue
        cluster_id = str(finding.get("cluster_id", "")).strip()
        record = {
            "review_id": f"{cluster_id or 'cluster'}:{idx}:{finding.get('finding_type','')}",
            "cluster_id": cluster_id,
            "finding_type": finding.get("finding_type", ""),
            "severity": finding.get("severity", ""),
            "recommendation": finding.get("recommendation", ""),
            "affected_material_ids": _safe_list(finding.get("affected_material_ids", [])),
            "affected_concept_names": _safe_list(finding.get("affected_concept_names", [])),
            "evidence": _safe_list(finding.get("evidence", [])),
            "input_fingerprint": fingerprint,
            "wiki_path": str(_concept_page_path(next((c for c in clusters if str(c.get("cluster_id", "")).strip() == cluster_id), clusters[0] if clusters else {}))),
        }
        output.append(record)

    new_clusters = parsed.get("new_clusters", []) if isinstance(parsed, dict) else []
    if not new_clusters and isinstance(bridge_output, dict):
        new_clusters = bridge_output.get("clusters", [])
    elif not new_clusters and isinstance(bridge_output, list):
        new_clusters = bridge_output
    if isinstance(new_clusters, list):
        _write_json(bridge_output_path, {"clusters": new_clusters})
    if isinstance(new_clusters, list) and local_rows and material_rows:
        concept_index: dict[tuple[str, str], dict] = {}
        for row in local_rows:
            concept_name, concept_key, material_id, relevance, source_pages, evidence_spans, confidence, concept_type = row
            concept_index[(material_id, concept_key)] = {
                "concept_name": concept_name,
                "concept_key": concept_key,
                "material_id": material_id,
                "concept_type": concept_type,
                "relevance": relevance,
                "source_pages": source_pages,
                "evidence_spans": evidence_spans,
                "confidence": confidence,
            }

        validated = _validate_bridge_and_attach_provenance(new_clusters, concept_index, {})
        covered_pairs = _cover_pairs_from_clusters(clusters)
        discovered: list[dict] = []
        for cluster in validated:
            filtered_sources = [
                source for source in cluster.get("source_concepts", [])
                if (source.get("material_id", ""), source.get("concept_key", "")) not in covered_pairs
            ]
            material_ids = list(dict.fromkeys(source.get("material_id", "") for source in filtered_sources if source.get("material_id", "")))
            if len(material_ids) < 2:
                continue
            canonical_name = cluster.get("canonical_name", "").strip()
            aliases = _dedupe_strings([
                canonical_name,
                *(_safe_list(cluster.get("aliases", []))),
                *[source.get("concept_name", "") for source in filtered_sources if source.get("concept_name", "")],
            ])
            discovered.append({
                "canonical_name": canonical_name or filtered_sources[0].get("concept_name", ""),
                "slug": slugify(canonical_name or filtered_sources[0].get("concept_name", "")),
                "aliases": aliases,
                "material_ids": material_ids,
                "source_concepts": filtered_sources,
                "confidence": float(cluster.get("confidence", 0.0) or 0.0),
            })

        if discovered:
            assigned = _assign_new_bridge_ids(discovered, clusters)
            updated = sorted([*clusters, *assigned], key=lambda c: c.get("cluster_id", ""))
            derived_dir = root / "derived"
            derived_dir.mkdir(exist_ok=True)
            bridge_path = derived_dir / "bridge_concept_clusters.jsonl"
            with bridge_path.open("w", encoding="utf-8") as f:
                for cluster in updated:
                    f.write(json.dumps(cluster, ensure_ascii=False) + "\n")
            stamp_path = derived_dir / "bridge_cluster_stamp.json"
            stamp_path.write_text(
                json.dumps({
                    "clustered_at": datetime.now(timezone.utc).isoformat(),
                    "fingerprint": bridge_cluster_fingerprint(None),
                    "bridge_concepts": sum(len(c.get("source_concepts", [])) for c in updated),
                    "clusters": len(updated),
                }, indent=2),
                encoding="utf-8",
            )
            bridge_discovery = len(assigned)
    if not output:
        for cluster in clusters:
            review_id = cluster.get("cluster_id", "")
            page_path = root / _concept_page_path(cluster)
            output.append({
                "review_id": review_id,
                "cluster_id": review_id,
                "finding_type": "none",
                "severity": "low",
                "recommendation": "",
                "affected_material_ids": [],
                "affected_concept_names": [],
                "evidence": [],
                "input_fingerprint": fingerprint,
                "wiki_path": str(page_path.relative_to(root)),
            })
    if not output:
        return existing_rows, bridge_discovery

    _write_jsonl(root / LINT_DIR / "cluster_reviews.jsonl", output)
    return output, bridge_discovery


def _run_concept_reflections(
    root: Path,
    clusters: list[dict],
    material_info: dict[str, dict],
    llm_factory=None,
    tool: ReflectionIndexTool | None = None,
) -> list[dict]:
    existing = _existing_by_key(root / LINT_DIR / "concept_reflections.jsonl", "cluster_id")
    output: list[dict] = []
    eligible = [c for c in clusters if len(dict.fromkeys(sc.get("material_id", "") for sc in c.get("source_concepts", []) if sc.get("material_id"))) >= 2]
    workers = max(1, min(len(eligible), int(load_config().get("enrichment", {}).get("parallel", 4) or 4)))

    def _one(cluster: dict) -> dict | None:
        page_path = root / _concept_page_path(cluster)
        payload = _cluster_prompt_payload(cluster, material_info, _read_text(page_path) or "", tool)
        fingerprint = canonical_hash(payload)
        existing_record = existing.get(cluster.get("cluster_id", ""))
        if existing_record and existing_record.get("input_fingerprint") == fingerprint:
            return existing_record
        llm_fn = llm_factory("cluster")
        system, user = _concept_reflection_prompt(cluster, payload)
        parsed = _run_reflection_prompt_with_context(
            llm_fn,
            system,
            user,
            "JSON object with keys: main_takeaways, main_tensions, open_questions, why_this_concept_matters, optional context_requests",
            tool,
        )
        if not isinstance(parsed, dict):
            raise EnrichmentError("Concept reflection returned non-object")
        record = {
            "cluster_id": cluster.get("cluster_id", ""),
            "slug": cluster.get("slug", ""),
            "canonical_name": cluster.get("canonical_name", ""),
            "main_takeaways": _safe_list(parsed.get("main_takeaways", [])),
            "main_tensions": _safe_list(parsed.get("main_tensions", [])),
            "open_questions": _safe_list(parsed.get("open_questions", [])),
            "why_this_concept_matters": str(parsed.get("why_this_concept_matters", "")).strip(),
            "supporting_material_ids": sorted({sc.get("material_id", "") for sc in cluster.get("source_concepts", []) if sc.get("material_id")}),
            "supporting_evidence": sorted({
                span
                for sc in cluster.get("source_concepts", [])
                for span in _safe_list(sc.get("evidence_spans", []))
            }),
            "input_fingerprint": fingerprint,
            "wiki_path": str(page_path.relative_to(root)),
        }
        return record

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
    _write_jsonl(root / LINT_DIR / "concept_reflections.jsonl", output)
    return output


def _run_collection_reflections(
    root: Path,
    groups: dict[tuple[str, str], list[dict]],
    clusters: list[dict],
    llm_factory=None,
    tool: ReflectionIndexTool | None = None,
) -> list[dict]:
    existing = _existing_by_key(root / LINT_DIR / "collection_reflections.jsonl", "collection_key")
    output: list[dict] = []
    eligible = [(domain, collection, metas) for (domain, collection), metas in groups.items() if len(metas) >= 2]
    workers = max(1, min(len(eligible), int(load_config().get("enrichment", {}).get("parallel", 4) or 4)))

    def _one(domain: str, collection: str, metas: list[dict]) -> dict | None:
        page_path = root / f"wiki/{domain}/{collection}/_index.md"
        deterministic_page = _build_deterministic_collection_page(root, domain, collection, metas, clusters)
        payload = _collection_prompt_payload(domain, collection, metas, _read_text(page_path) or deterministic_page, clusters, tool)
        fingerprint = canonical_hash(payload)
        key = f"{domain}/{collection}"
        existing_record = existing.get(key)
        if existing_record and existing_record.get("input_fingerprint") == fingerprint:
            return existing_record
        llm_fn = llm_factory("cluster")
        system, user = _collection_reflection_prompt(domain, collection, payload)
        parsed = _run_reflection_prompt_with_context(
            llm_fn,
            system,
            user,
            "JSON object with keys: main_takeaways, main_tensions, important_material_ids, important_cluster_ids, open_questions, optional context_requests",
            tool,
        )
        if not isinstance(parsed, dict):
            raise EnrichmentError("Collection reflection returned non-object")
        record = {
            "collection_key": key,
            "domain": domain,
            "collection": collection,
            "main_takeaways": _safe_list(parsed.get("main_takeaways", [])),
            "main_tensions": _safe_list(parsed.get("main_tensions", [])),
            "important_material_ids": _safe_list(parsed.get("important_material_ids", [])),
            "important_cluster_ids": _safe_list(parsed.get("important_cluster_ids", [])),
            "open_questions": _safe_list(parsed.get("open_questions", [])),
            "input_fingerprint": fingerprint,
            "wiki_path": str(page_path.relative_to(root)),
        }
        return record

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
    _write_jsonl(root / LINT_DIR / "collection_reflections.jsonl", output)
    return output


def _run_graph_reflection(
    root: Path,
    deterministic_report: dict,
    cluster_reviews: list[dict],
    concept_refs: list[dict],
    collection_refs: list[dict],
    llm_factory=None,
    tool: ReflectionIndexTool | None = None,
) -> list[dict]:
    existing_rows = _load_jsonl(root / LINT_DIR / "graph_findings.jsonl")
    payload = {
        "deterministic_report": deterministic_report.get("summary", {}),
        "cluster_reviews": cluster_reviews[:20],
        "concept_reflections": concept_refs[:20],
        "collection_reflections": collection_refs[:20],
    }
    fingerprint = canonical_hash(payload)
    if existing_rows and all(row.get("input_fingerprint") == fingerprint for row in existing_rows):
        return existing_rows

    llm_fn = llm_factory("cluster")
    system, user = _graph_reflection_prompt(deterministic_report, cluster_reviews, concept_refs, collection_refs)
    parsed = _run_reflection_prompt_with_context(
        llm_fn,
        system,
        user,
        "JSON object with keys: findings (array of findings) and optional context_requests (array of read-only SQL-index lookups)",
        tool,
    )
    findings = parsed.get("findings", []) if isinstance(parsed, dict) else parsed
    if not isinstance(findings, list):
        raise EnrichmentError("Graph reflection returned non-list findings")

    records: list[dict] = []
    for idx, finding in enumerate(findings):
        if not isinstance(finding, dict):
            continue
        record = {
            "finding_id": f"graph:{idx}",
            "finding_type": finding.get("finding_type", ""),
            "severity": finding.get("severity", ""),
            "summary": finding.get("summary", ""),
            "details": finding.get("details", ""),
            "affected_material_ids": _safe_list(finding.get("affected_material_ids", [])),
            "affected_cluster_ids": _safe_list(finding.get("affected_cluster_ids", [])),
            "candidate_future_sources": _safe_list(finding.get("candidate_future_sources", [])),
            "candidate_bridge_links": _safe_list(finding.get("candidate_bridge_links", [])),
            "input_fingerprint": fingerprint,
        }
        records.append(record)

    if not records:
        records.append({
            "finding_id": "graph:empty",
            "finding_type": "none",
            "severity": "low",
            "summary": "",
            "details": "",
            "affected_material_ids": [],
            "affected_cluster_ids": [],
            "candidate_future_sources": [],
            "candidate_bridge_links": [],
            "input_fingerprint": fingerprint,
        })

    _write_jsonl(root / LINT_DIR / "graph_findings.jsonl", records)
    _write_stamp(
        root / GRAPH_REFLECTION_STAMP_PATH,
        {
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "graph_fingerprint": fingerprint,
            "cluster_count": len({cluster.get("cluster_id", "") for cluster in cluster_reviews if cluster.get("cluster_id", "")}),
            "material_count": len({mid for row in cluster_reviews for mid in _safe_list(row.get("affected_material_ids", []))}),
            "finding_count": len(records),
        },
    )
    return records


def _run_bridge_cluster_maintenance(
    root: Path,
    bridge_clusters: list[dict],
    cluster_reviews: list[dict],
    material_info: dict[str, dict],
    llm_factory=None,
    tool: ReflectionIndexTool | None = None,
) -> tuple[list[dict], int]:
    """Ask the maintainer LLM whether bridge clusters should be merged/renamed."""
    if not bridge_clusters or not cluster_reviews:
        return bridge_clusters, 0

    prompt_reviews = [
        review for review in cluster_reviews
        if str(review.get("finding_type", "")).strip().lower() in {"merge", "duplicate", "missed_equivalence", "over_merged"}
        or str(review.get("severity", "")).strip().lower() in {"high", "medium"}
    ]
    if not prompt_reviews:
        return bridge_clusters, 0

    payload = {
        "bridge_clusters": [
            {
                "cluster_id": cluster.get("cluster_id", ""),
                "canonical_name": cluster.get("canonical_name", ""),
                "slug": cluster.get("slug", ""),
                "aliases": cluster.get("aliases", []),
                "material_ids": cluster.get("material_ids", []),
                "source_concepts": cluster.get("source_concepts", []),
                "confidence": cluster.get("confidence", 0.0),
                "wiki_path": cluster.get("wiki_path", ""),
            }
            for cluster in bridge_clusters
        ],
        "cluster_reviews": prompt_reviews,
        "material_info": material_info,
    }
    maintenance_input_path = root / "derived" / "tmp" / "bridge_maintenance_input.json"
    maintenance_output_path = root / "derived" / "tmp" / "bridge_maintenance_output.json"
    _write_json(maintenance_input_path, payload)

    existing_path = root / LINT_DIR / "bridge_maintenance.jsonl"
    existing = _load_jsonl(existing_path)
    fingerprint = canonical_hash(payload)
    if existing and all(row.get("input_fingerprint") == fingerprint for row in existing):
        return bridge_clusters, 0

    if maintenance_output_path.exists():
        maintenance_output_path.unlink()

    llm_fn = llm_factory("lint")
    system, user = _bridge_maintenance_prompt(maintenance_input_path, maintenance_output_path)
    parsed = _run_reflection_prompt_with_context(
        llm_fn,
        system,
        user,
        "JSON object with keys: actions (array) and optional context_requests (array of read-only SQL-index lookups)",
        tool,
    )
    if maintenance_output_path.exists() and maintenance_output_path.stat().st_size > 0:
        try:
            parsed = json.loads(maintenance_output_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            pass
    actions = parsed.get("actions", []) if isinstance(parsed, dict) else parsed
    if not isinstance(actions, list):
        raise EnrichmentError("Bridge maintenance returned non-list actions")

    applied_actions = []
    updated_clusters = bridge_clusters
    changed = 0
    if actions:
        updated_clusters, changed = _apply_bridge_cluster_maintenance(root, bridge_clusters, actions)
        applied_actions = [
            {
                "action_type": str(action.get("action_type", "")).strip().lower(),
                "target_cluster_ids": _safe_list(action.get("target_cluster_ids", [])),
                "canonical_name": str(action.get("canonical_name", "")).strip(),
                "aliases": _safe_list(action.get("aliases", [])),
                "reason": str(action.get("reason", "")).strip(),
                "confidence": float(action.get("confidence", 0.0) or 0.0),
            }
            for action in actions
            if isinstance(action, dict)
        ]

    _write_jsonl(
        existing_path,
        [{
            "input_fingerprint": fingerprint,
            "actions": applied_actions,
            "applied": bool(changed),
            "bridge_cluster_count": len(updated_clusters),
        }],
    )
    return updated_clusters, changed


def _run_bridge_cluster_discovery(
    root: Path,
    bridge_clusters: list[dict],
    llm_factory=None,
    tool: ReflectionIndexTool | None = None,
) -> tuple[list[dict], int]:
    """Ask the maintainer LLM for genuinely new bridge concepts from local concepts."""
    local_rows, material_rows = _load_local_concepts(root)
    if not local_rows or not material_rows:
        return bridge_clusters, 0

    existing_path = root / LINT_DIR / "bridge_discovery.jsonl"
    existing = _load_jsonl(existing_path)

    bridge_packets_path = _stage_bridge_packet_input(root, local_rows, material_rows)
    bridge_clusters_path = root / "derived" / "bridge_concept_clusters.jsonl"
    output_path = _cluster_output_path(root, "bridge")
    if output_path.exists():
        output_path.unlink()

    payload = {
        "bridge_clusters": bridge_clusters,
        "local_rows": local_rows,
        "material_rows": material_rows,
    }
    fingerprint = canonical_hash(payload)
    if existing and all(row.get("input_fingerprint") == fingerprint for row in existing):
        return bridge_clusters, 0

    llm_fn = llm_factory("lint")
    system, user = _bridge_discovery_prompt(bridge_packets_path, bridge_clusters_path, output_path)
    parsed = _run_reflection_prompt_with_context(
        llm_fn,
        system,
        user,
        "JSON object with keys: clusters (array of bridge clusters) and optional context_requests (array of read-only SQL-index lookups)",
        tool,
    )
    clusters = parsed.get("clusters", []) if isinstance(parsed, dict) else parsed
    if not isinstance(clusters, list):
        raise EnrichmentError("Bridge discovery returned non-list clusters")

    concept_index: dict[tuple[str, str], dict] = {}
    for row in local_rows:
        concept_name, concept_key, material_id, relevance, source_pages, evidence_spans, confidence, concept_type = row
        concept_index[(material_id, concept_key)] = {
            "concept_name": concept_name,
            "concept_key": concept_key,
            "material_id": material_id,
            "concept_type": concept_type,
            "relevance": relevance,
            "source_pages": source_pages,
            "evidence_spans": evidence_spans,
            "confidence": confidence,
        }

    validated = _validate_bridge_and_attach_provenance(clusters, concept_index, {})
    covered_pairs = _cover_pairs_from_clusters(bridge_clusters)
    new_clusters: list[dict] = []
    for cluster in validated:
        filtered_sources = [
            source for source in cluster.get("source_concepts", [])
            if (source.get("material_id", ""), source.get("concept_key", "")) not in covered_pairs
        ]
        material_ids = list(dict.fromkeys(source.get("material_id", "") for source in filtered_sources if source.get("material_id", "")))
        if len(material_ids) < 2:
            continue
        canonical_name = cluster.get("canonical_name", "").strip()
        aliases = _dedupe_strings([
            canonical_name,
            *(_safe_list(cluster.get("aliases", []))),
            *[source.get("concept_name", "") for source in filtered_sources if source.get("concept_name", "")],
        ])
        new_clusters.append({
            "canonical_name": canonical_name or filtered_sources[0].get("concept_name", ""),
            "slug": slugify(canonical_name or filtered_sources[0].get("concept_name", "")),
            "aliases": aliases,
            "material_ids": material_ids,
            "source_concepts": filtered_sources,
            "confidence": float(cluster.get("confidence", 0.0) or 0.0),
        })

    if not new_clusters:
        _write_jsonl(existing_path, [{
            "input_fingerprint": fingerprint,
            "new_clusters": 0,
            "applied": False,
            "bridge_cluster_count": len(bridge_clusters),
        }])
        return bridge_clusters, 0

    assigned = _assign_new_bridge_ids(new_clusters, bridge_clusters)
    updated = sorted([*bridge_clusters, *assigned], key=lambda c: c.get("cluster_id", ""))

    derived_dir = root / "derived"
    derived_dir.mkdir(exist_ok=True)
    bridge_path = derived_dir / "bridge_concept_clusters.jsonl"
    with bridge_path.open("w", encoding="utf-8") as f:
        for cluster in updated:
            f.write(json.dumps(cluster, ensure_ascii=False) + "\n")

    output_path.write_text(json.dumps(updated, indent=2, ensure_ascii=False), encoding="utf-8")

    stamp_path = derived_dir / "bridge_cluster_stamp.json"
    stamp_path.write_text(
        json.dumps({
            "clustered_at": datetime.now(timezone.utc).isoformat(),
            "fingerprint": bridge_cluster_fingerprint(None),
            "bridge_concepts": sum(len(c.get("source_concepts", [])) for c in updated),
            "clusters": len(updated),
        }, indent=2),
        encoding="utf-8",
    )

    _write_jsonl(existing_path, [{
        "input_fingerprint": fingerprint,
        "new_clusters": len(assigned),
        "applied": True,
        "bridge_cluster_count": len(updated),
    }])
    return updated, len(assigned)


def run_reflective_lint(
    config: dict,
    deterministic_report: dict,
    *,
    llm_factory=None,
    apply: bool = False,
    scheduled: bool = False,
) -> dict:
    """Run the reflective LLM passes and project outputs to disk."""
    root = get_project_root()
    manifest_records = _load_manifest(root)
    metas = _load_all_metas(root, manifest_records)
    material_info = _build_material_info(root, manifest_records)
    clusters = _current_concepts(root)
    bridge_clusters = load_bridge_clusters(root)
    groups = _group_materials_by_collection(metas)

    if not get_index_path().exists():
        lint_root = root / LINT_DIR
        lint_root.mkdir(parents=True, exist_ok=True)
        for name in ("cluster_reviews.jsonl", "concept_reflections.jsonl", "collection_reflections.jsonl", "graph_findings.jsonl"):
            (lint_root / name).write_text("", encoding="utf-8")
        return {
            "cluster_reviews": 0,
            "bridge_cluster_changes": 0,
            "bridge_cluster_discovery": 0,
            "concept_reflections": 0,
            "collection_reflections": 0,
            "graph_findings": 0,
            "applied": False,
            "skipped": True,
            "graph_skipped": True,
        }

    if llm_factory is None:
        shared_llm_state: dict = {}

        def llm_factory(stage: str) -> LlmFn:
            return make_cli_llm_fn(config, "lint", state=shared_llm_state)
    lint_route_signature = get_model_id(config, "lint")

    with ReflectionIndexTool(root) as tool:
        cluster_reviews, bridge_discovery = _run_cluster_audit(
            root,
            clusters,
            material_info,
            lint_route_signature,
            llm_factory,
            tool,
        )
        if apply:
            bridge_clusters, bridge_changes = _run_bridge_cluster_maintenance(
                root,
                bridge_clusters,
                cluster_reviews,
                material_info,
                llm_factory,
                tool,
            )
        else:
            bridge_changes = 0
        concept_refs = _run_concept_reflections(root, bridge_clusters, material_info, llm_factory, tool)
        collection_refs = _run_collection_reflections(root, groups, bridge_clusters, llm_factory, tool)
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
        if graph_due or not scheduled:
            graph_findings = _run_graph_reflection(
                root, deterministic_report, cluster_reviews, concept_refs, collection_refs, llm_factory, tool
            )
            graph_skipped = False
            graph_skip_reason = ""
        else:
            graph_findings = _load_jsonl(root / LINT_DIR / "graph_findings.jsonl")
            graph_skipped = True
            graph_skip_reason = graph_reason

    # Always refresh memory so the reflection tables are queryable.
    memory_rebuild(config)

    return {
        "cluster_reviews": len(cluster_reviews),
        "bridge_cluster_changes": bridge_changes,
        "bridge_cluster_discovery": bridge_discovery,
        "concept_reflections": len(concept_refs),
        "collection_reflections": len(collection_refs),
        "graph_findings": len(graph_findings),
        "applied": apply,
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
) -> dict:
    """Run lint in quick or full mode and return a structured summary."""
    if config is None:
        config = load_config()
    if quick and full:
        raise ValueError("lint cannot be both quick and full")
    if not quick and not full:
        quick = True
    apply = fix or full

    deterministic = run_deterministic_lint(config)
    root = get_project_root()
    result = {
        "mode": "full" if full else "quick",
        "deterministic": deterministic,
        "reflection": None,
        "fixes": None,
        "report_path": str((get_project_root() / REPORT_PATH)),
    }

    if apply:
        result["fixes"] = _apply_deterministic_fixes(deterministic, config)

    full_reflection_ran = False
    if full:
        graph_due, graph_reason = _full_lint_due(root, deterministic, config)
        if scheduled and not graph_due:
            result["reflection"] = {
                "cluster_reviews": 0,
                "bridge_cluster_changes": 0,
                "bridge_cluster_discovery": 0,
                "concept_reflections": 0,
                "collection_reflections": 0,
                "graph_findings": 0,
                "applied": False,
                "skipped": True,
                "graph_skipped": True,
                "graph_skip_reason": graph_reason,
            }
        else:
            result["reflection"] = run_reflective_lint(
                config,
                deterministic,
                llm_factory=llm_factory,
                apply=apply,
                scheduled=scheduled,
            )
            full_reflection_ran = True

    if full and full_reflection_ran:
        _write_full_lint_stamp(root, deterministic)

    if report or full or fix:
        report_text = render_lint_report(deterministic)
        path = get_project_root() / REPORT_PATH
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(report_text, encoding="utf-8")

    return result


def lint_exit_code(result: dict) -> int:
    """Return a deterministic CLI exit code for lint results."""
    deterministic = result.get("deterministic", {}) or {}
    summary = deterministic.get("summary", {}) if isinstance(deterministic, dict) else {}
    if summary.get("high", 0):
        return 2
    if summary.get("issues", 0):
        return 1
    return 0
