"""Collection-local concept clustering for Arquimedes."""

from __future__ import annotations

import os
import json
import logging
import re
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

from arquimedes import enrich_stamps, practice_prompts
from arquimedes.config import (
    get_enabled_domains,
    get_indexes_root,
    get_logs_root,
    get_project_root,
    load_config,
)
from arquimedes.domain_profiles import is_practice_domain, should_run_clustering
from arquimedes.llm import (
    EnrichmentError,
    LlmFn,
    get_model_id,
    make_cli_llm_fn,
    parse_json_or_repair,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SLUG_RE = re.compile(r"[^a-z0-9-]+")
_MULTI_DASH_RE = re.compile(r"-{2,}")
_WORD_RE = re.compile(r"[a-z0-9]+")


def _collection_scope(domain: str, collection: str) -> tuple[str, str]:
    return ((domain or "practice").strip() or "practice", (collection or "_general").strip() or "_general")


def local_collection_key(domain: str, collection: str) -> str:
    domain, collection = _collection_scope(domain, collection)
    return f"{domain}__{collection}"


def local_cluster_dir(root: Path, domain: str, collection: str) -> Path:
    return root / "derived" / "collections" / local_collection_key(domain, collection)


def local_cluster_path(root: Path, domain: str, collection: str) -> Path:
    return local_cluster_dir(root, domain, collection) / "local_concept_clusters.jsonl"


def local_cluster_stamp_path(root: Path, domain: str, collection: str) -> Path:
    return local_cluster_dir(root, domain, collection) / "local_cluster_stamp.json"


def _local_cluster_gate_path(root: Path, domain: str, collection: str) -> Path:
    return local_cluster_dir(root, domain, collection) / ".cluster.lock"


def local_concept_wiki_path(domain: str, collection: str, slug: str) -> str:
    domain, collection = _collection_scope(domain, collection)
    return f"wiki/{domain}/{collection}/concepts/{slug}.md"


def slugify(name: str) -> str:
    """Lower, spaces→hyphens, strip non-alphanum. Used for concept page paths."""
    slug = name.lower().strip()
    slug = slug.replace(" ", "-")
    slug = _SLUG_RE.sub("", slug)
    slug = _MULTI_DASH_RE.sub("-", slug)
    return slug.strip("-")


def _normalize_concept_name(name: str) -> str:
    """Normalize concept_name to a concept_key (mirrors index.py logic).

    Lowercase, collapse whitespace, strip basic English plural -s/-es/-ies.
    Keep this in sync with how concept_key is produced in index.py.
    """
    key = name.lower().strip()
    key = re.sub(r"\s+", " ", key)
    # basic depluralisation
    if key.endswith("ies") and len(key) > 4:
        key = key[:-3] + "y"
    elif key.endswith("es") and len(key) > 3:
        key = key[:-2]
    elif key.endswith("s") and len(key) > 2:
        key = key[:-1]
    return key


def _match_score(text: str, references: list[str]) -> tuple[int, int]:
    """Return a rough lexical match score between *text* and reference strings."""
    text_tokens = set(_WORD_RE.findall(text.lower()))
    best_overlap = 0
    best_chars = 0
    for ref in references:
        ref_tokens = set(_WORD_RE.findall(ref.lower()))
        overlap = len(text_tokens & ref_tokens)
        if overlap > best_overlap:
            best_overlap = overlap
            best_chars = len(ref)
        elif overlap == best_overlap:
            best_chars = max(best_chars, len(ref))
    return best_overlap, best_chars


def _resolve_concept_reference(
    material_id: str,
    concept_name: str,
    concept_index: dict[tuple[str, str], dict],
) -> dict | None:
    """Resolve an LLM-emitted concept reference back to an indexed concept row.

    Accept exact normalized matches first, then a unique prefix match to tolerate
    minor truncation errors like "whitenes" for "whiteness".
    """
    concept_key = _normalize_concept_name(concept_name)
    indexed = concept_index.get((material_id, concept_key))
    if indexed is not None:
        return indexed

    exact_name_matches = [
        row
        for (mid, _), row in concept_index.items()
        if mid == material_id and row["concept_name"].strip().casefold() == concept_name.strip().casefold()
    ]
    if len(exact_name_matches) == 1:
        return exact_name_matches[0]

    prefix_matches = [
        row
        for (mid, key), row in concept_index.items()
        if mid == material_id and (key.startswith(concept_key) or concept_key.startswith(key))
    ]
    if len(prefix_matches) == 1:
        return prefix_matches[0]
    return None


def _build_source_concept(indexed: dict) -> dict:
    """Expand an indexed concept row into a source_concepts entry with provenance."""
    return {
        "material_id": indexed["material_id"],
        "concept_name": indexed["concept_name"],
        "descriptor": indexed.get("descriptor", ""),
        "concept_key": indexed["concept_key"],
        "relevance": indexed["relevance"],
        "source_pages": json.loads(indexed["source_pages"] or "[]"),
        "evidence_spans": json.loads(indexed["evidence_spans"] or "[]"),
        "confidence": indexed["confidence"],
    }


def _derive_cluster_confidence(source_concepts: list[dict], *, fallback: float = 0.0) -> float:
    """Derive cluster confidence from validated source concept confidences."""
    values: list[float] = []
    for source in source_concepts:
        if not isinstance(source, dict):
            continue
        try:
            values.append(float(source.get("confidence", 0.0) or 0.0))
        except (TypeError, ValueError):
            continue
    if not values:
        return float(fallback or 0.0)
    average = sum(values) / len(values)
    return max(0.0, min(1.0, average))


def _split_concept_row(row: tuple) -> tuple[str, str, str, str, str, str, str, float, str]:
    """Normalize concept rows from old/new index schemas.

    New rows append descriptor as the last field so older row-indexing remains stable.
    """
    if len(row) >= 9:
        concept_name, concept_key, material_id, relevance, source_pages, evidence_spans, confidence, concept_type, descriptor = row[:9]
        return (
            str(concept_name),
            str(concept_key),
            str(material_id),
            str(relevance),
            str(source_pages),
            str(evidence_spans),
            float(confidence or 0.0),
            str(concept_type),
            str(descriptor),
        )
    if len(row) == 8:
        concept_name, concept_key, material_id, relevance, source_pages, evidence_spans, confidence, concept_type = row
        return (
            str(concept_name),
            str(concept_key),
            str(material_id),
            str(relevance),
            str(source_pages),
            str(evidence_spans),
            float(confidence or 0.0),
            str(concept_type),
            "",
        )
    raise ValueError(f"Unexpected concept row shape: {len(row)}")


def _tmp_name_fragment(value: str) -> str:
    """Return a filesystem-safe suffix for staged clustering inputs."""
    text = str(value or "").strip()
    if not text:
        return ""
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", text).strip("._-")


def _cluster_input_path(root: Path, kind: str, *, scope_key: str | None = None) -> Path:
    """Return the staged input path for a clustering run.

    Local collection clustering may run multiple scopes in parallel, so callers
    can provide a ``scope_key`` to avoid different workers overwriting each
    other's staged packet/memory files under ``derived/tmp``. Scope-specific
    runs also get their own subdirectory so different collection workers never
    share the same staging area.
    """
    suffix = _tmp_name_fragment(scope_key or "")
    if suffix:
        return root / "derived" / "tmp" / suffix / f"{kind}_cluster_input.json"
    return root / "derived" / "tmp" / f"{kind}_cluster_input.json"


def _write_json(path: Path, payload: object) -> None:
    """Write JSON payloads atomically enough for staged LLM inputs."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, separators=(',', ':'), ensure_ascii=False), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    """Write JSONL payloads for canonical cluster files."""
    path.parent.mkdir(parents=True, exist_ok=True)
    text = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows)
    if rows:
        text += "\n"
    path.write_text(text, encoding="utf-8")


def _next_local_cluster_index(domain: str, collection: str, clusters: list[dict]) -> int:
    prefix = f"{local_collection_key(domain, collection)}__local_"
    max_idx = 0
    for cluster in clusters:
        cid = str(cluster.get("cluster_id", "")).strip()
        match = re.fullmatch(rf"{re.escape(prefix)}(\d{{4}})", cid)
        if match:
            max_idx = max(max_idx, int(match.group(1)))
    return max_idx + 1


def normalize_local_clusters(domain: str, collection: str, cluster_records: list[dict]) -> list[dict]:
    domain, collection = _collection_scope(domain, collection)
    next_idx = _next_local_cluster_index(domain, collection, cluster_records)
    prefix = f"{local_collection_key(domain, collection)}__local_"
    normalized = []
    for cluster in cluster_records:
        row = dict(cluster)
        slug = str(row.get("slug", "")).strip() or slugify(str(row.get("canonical_name", "")).strip())
        cid = str(row.get("cluster_id", "")).strip()
        if not re.fullmatch(rf"{re.escape(prefix)}\d{{4}}", cid):
            cid = f"{prefix}{next_idx:04d}"
            next_idx += 1
        material_ids = [str(mid).strip() for mid in row.get("material_ids", []) if str(mid).strip()]
        normalized.append({
            "cluster_id": cid,
            "domain": domain,
            "collection": collection,
            "canonical_name": str(row.get("canonical_name", "")).strip(),
            "slug": slug,
            "aliases": _dedupe_aliases([str(alias) for alias in row.get("aliases", [])]),
            "descriptor": str(row.get("descriptor", "")).strip(),
            "material_ids": sorted(dict.fromkeys(material_ids)),
            "source_concepts": row.get("source_concepts", []),
            "confidence": float(row.get("confidence", 0.0) or 0.0),
            "wiki_path": local_concept_wiki_path(domain, collection, slug),
        })
    return normalized


def _attach_run_provenance(records: list[dict], route_signature: str, run_at: str) -> list[dict]:
    """Stamp each record with a _provenance block (non-destructive: preserves existing fields)."""
    for record in records:
        record["_provenance"] = {
            "route_signature": route_signature,
            "run_at": run_at,
        }
    return records


def _load_jsonl(path: Path) -> list[dict]:
    """Read a JSONL file into a list of dicts."""
    if not path.exists():
        return []
    rows: list[dict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            rows.append(value)
    return rows


def _cleanup_paths(*paths: Path) -> None:
    """Best-effort cleanup for temporary bridge staging files."""
    for path in paths:
        try:
            if path and path.exists():
                path.unlink()
        except OSError:
            pass
        try:
            if path and path.parent.name != "tmp":
                path.parent.rmdir()
        except OSError:
            pass


def _stage_bridge_packet_input(
    root: Path,
    concept_rows: list[tuple],
    material_rows: list[tuple],
    *,
    scope_key: str | None = None,
    max_local_concepts_per_material: int | None = 8,
    max_bridge_candidates_per_material: int | None = 8,
    max_evidence_snippets_per_material: int | None = 5,
) -> Path:
    """Write the compact bridge clustering packets used by the LLM."""
    material_info: dict[str, dict] = {}
    for mid, title, summary, keywords_json in material_rows:
        try:
            keywords = json.loads(keywords_json or "[]")
            if not isinstance(keywords, list):
                keywords = []
        except json.JSONDecodeError:
            keywords = []
        material_info[mid] = {
            "title": title,
            "summary": summary or "",
            "keywords": [str(k) for k in keywords if str(k).strip()],
        }

    grouped: dict[str, dict] = {}
    for row in concept_rows:
        concept_name, concept_key, material_id, relevance, source_pages, evidence_spans, confidence, concept_type, descriptor = _split_concept_row(row)
        item = grouped.setdefault(material_id, {
            "material_id": material_id,
            "title": material_info.get(material_id, {}).get("title", material_id),
            "summary": material_info.get(material_id, {}).get("summary", ""),
            "keywords": material_info.get(material_id, {}).get("keywords", []),
            "concepts": [],
            "bridge": [],
            "evidence": [],
        })
        concept = {
            "concept": concept_name,
        }
        try:
            spans = json.loads(evidence_spans or "[]")
        except json.JSONDecodeError:
            spans = []
        concept["descriptor"] = descriptor.strip()
        if concept_type == "bridge_candidate":
            item["bridge"].append(concept)
        else:
            item["concepts"].append(concept)
        for span in spans[:2]:
            if isinstance(span, str) and span.strip():
                item["evidence"].append(span.strip())

    material_packets = []
    for packet in grouped.values():
        local_sorted = sorted(
            packet["concepts"],
            key=lambda c: c.get("concept", ""),
        )
        if max_local_concepts_per_material is not None:
            local_sorted = local_sorted[:max_local_concepts_per_material]
        bridge_sorted = sorted(
            packet["bridge"],
            key=lambda c: c.get("concept", ""),
        )
        if max_bridge_candidates_per_material is not None:
            bridge_sorted = bridge_sorted[:max_bridge_candidates_per_material]
        snippets = []
        seen_snippets: set[str] = set()
        for snippet in packet["evidence"]:
            if snippet not in seen_snippets:
                seen_snippets.add(snippet)
                snippets.append(snippet)
        if max_evidence_snippets_per_material is not None:
            snippets = snippets[:max_evidence_snippets_per_material]
        material_packets.append({
            "material_id": packet["material_id"],
            "title": packet["title"],
            "summary": packet["summary"],
            "keywords": packet["keywords"],
            "concepts": local_sorted,
            "bridge": bridge_sorted,
            "evidence": snippets[:5],
        })

    payload = {
        "kind": "bridge_packets",
        "materials": material_packets,
    }
    path = _cluster_input_path(root, "bridge", scope_key=scope_key)
    _write_json(path, payload)
    return path


def _dedupe_aliases(values: list[str]) -> list[str]:
    """Preserve order while deduplicating empty aliases."""
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        alias = value.strip()
        key = alias.casefold()
        if not alias or key in seen:
            continue
        seen.add(key)
        out.append(alias)
    return out


def _load_concept_rows(
    con: sqlite3.Connection,
    *,
    concept_type: str | None = None,
) -> list[tuple]:
    """Load concept rows with a fallback for older indexes lacking concept_type."""
    base_sql = (
        "SELECT concept_name, concept_key, material_id, relevance, "
        "source_pages, evidence_spans, confidence, concept_type, descriptor "
        "FROM concepts"
    )
    try:
        if concept_type is None:
            rows = con.execute(base_sql + " ORDER BY concept_key, material_id").fetchall()
        else:
            rows = con.execute(
                base_sql + " WHERE concept_type = ? ORDER BY concept_key, material_id",
                [concept_type],
            ).fetchall()
        return list(rows)
    except sqlite3.OperationalError:
        fallback_sql = (
            "SELECT concept_name, concept_key, material_id, relevance, "
            "source_pages, evidence_spans, confidence "
            "FROM concepts ORDER BY concept_key, material_id"
        )
        rows = con.execute(fallback_sql).fetchall()
        if concept_type is None or concept_type == "local":
            return [tuple(row) + ("local", "") for row in rows]
        return []


def _load_material_rows(con: sqlite3.Connection) -> list[tuple]:
    """Load material metadata needed for clustering prompts."""
    rows = con.execute(
        "SELECT material_id, title, summary, keywords FROM materials ORDER BY material_id"
    ).fetchall()
    return list(rows)


def _load_manifest_index(root: Path) -> dict[str, dict]:
    """Return manifest rows keyed by material_id."""
    manifest_path = root / "manifests" / "materials.jsonl"
    manifest_index: dict[str, dict] = {}
    for row in _load_jsonl(manifest_path):
        mid = str(row.get("material_id", "")).strip()
        if mid:
            manifest_index[mid] = row
    return manifest_index


def _collection_material_ids(
    manifest_index: dict[str, dict],
    domain: str,
    collection: str,
) -> set[str]:
    domain, collection = _collection_scope(domain, collection)
    return {
        mid
        for mid, row in manifest_index.items()
        if _collection_scope(str(row.get("domain", "")), str(row.get("collection", ""))) == (domain, collection)
    }


def _parse_iso_datetime(value: object) -> datetime | None:
    """Parse an ISO datetime string into an aware datetime when possible."""
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _local_clustered_at_from_stamp(root: Path, domain: str, collection: str) -> datetime | None:
    stamp_path = local_cluster_stamp_path(root, domain, collection)
    if not stamp_path.exists():
        return None
    try:
        stamp = json.loads(stamp_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    clustered_at = _parse_iso_datetime(stamp.get("clustered_at"))
    if clustered_at is not None:
        return clustered_at
    try:
        return datetime.fromtimestamp(stamp_path.stat().st_mtime, timezone.utc)
    except OSError:
        return None


def _pending_bridge_material_rows(
    material_rows: list[tuple],
    manifest_index: dict[str, dict],
    clustered_at: datetime | None,
) -> list[tuple]:
    """Filter materials down to only those ingested after the last cluster run."""
    if clustered_at is None:
        return list(material_rows)
    pending = []
    for row in material_rows:
        if not row:
            continue
        mid = str(row[0])
        ingested_at = _parse_iso_datetime(manifest_index.get(mid, {}).get("ingested_at"))
        if ingested_at is None or ingested_at > clustered_at:
            pending.append(row)
    return pending


def _pending_local_material_rows(
    material_rows: list[tuple],
    manifest_index: dict[str, dict],
    domain: str,
    collection: str,
    clustered_at: datetime | None,
) -> list[tuple]:
    material_ids = _collection_material_ids(manifest_index, domain, collection)
    rows = [row for row in material_rows if row and str(row[0]) in material_ids]
    return _pending_bridge_material_rows(rows, manifest_index, clustered_at)


def _pending_bridge_concept_rows(
    concept_rows: list[tuple],
    manifest_index: dict[str, dict],
    clustered_at: datetime | None,
) -> list[tuple]:
    """Filter concept rows down to materials ingested after the last cluster run."""
    if clustered_at is None:
        return list(concept_rows)
    pending = []
    for row in concept_rows:
        if not row:
            continue
        mid = str(row[2])
        ingested_at = _parse_iso_datetime(manifest_index.get(mid, {}).get("ingested_at"))
        if ingested_at is None or ingested_at > clustered_at:
            pending.append(row)
    return pending


def _pending_local_concept_rows(
    concept_rows: list[tuple],
    manifest_index: dict[str, dict],
    domain: str,
    collection: str,
    clustered_at: datetime | None,
) -> list[tuple]:
    material_ids = _collection_material_ids(manifest_index, domain, collection)
    rows = [row for row in concept_rows if row and str(row[2]) in material_ids]
    return _pending_bridge_concept_rows(rows, manifest_index, clustered_at)


def _parallel_collection_workers(config: dict, scope_count: int, *, allow_parallel: bool) -> int:
    if not allow_parallel or scope_count <= 1:
        return 1
    clustering_cfg = config.get("clustering", {}) if isinstance(config, dict) else {}
    configured = int(clustering_cfg.get("parallel_collections", 1) or 1)
    return max(1, min(scope_count, configured))


def local_cluster_fingerprint(
    domain: str,
    collection: str,
    config: dict | None = None,
) -> str:
    if config is None:
        config = load_config()
    root = get_project_root()
    db_path = get_indexes_root(config) / "search.sqlite"
    if not db_path.exists():
        return ""
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        concept_rows = _load_concept_rows(con)
        material_rows = _load_material_rows(con)
    finally:
        con.close()
    manifest_index = _load_manifest_index(root)
    clustered_at = _local_clustered_at_from_stamp(root, domain, collection)
    pending_material_rows = _pending_local_material_rows(material_rows, manifest_index, domain, collection, clustered_at)
    pending_concept_rows = _pending_local_concept_rows(concept_rows, manifest_index, domain, collection, clustered_at)
    return enrich_stamps.canonical_hash(list(pending_material_rows), list(pending_concept_rows))


def is_local_clustering_stale(
    domain: str,
    collection: str,
    config: dict | None = None,
    *,
    force: bool = False,
) -> bool:
    if force:
        return True
    if config is None:
        config = load_config()
    root = get_project_root()
    stamp_path = local_cluster_stamp_path(root, domain, collection)
    clusters_path = local_cluster_path(root, domain, collection)
    if not stamp_path.exists() or not clusters_path.exists():
        return True
    try:
        stamp = json.loads(stamp_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return True
    if stamp.get("total_concepts", 0) > 0 and stamp.get("clusters", 0) == 0:
        return True
    clustered_at = _local_clustered_at_from_stamp(root, domain, collection)
    if clustered_at is None:
        return True
    manifest_index = _load_manifest_index(root)
    db_path = get_indexes_root(config) / "search.sqlite"
    if not db_path.exists():
        return True
    material_ids = _collection_material_ids(manifest_index, domain, collection)
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        for row in _load_material_rows(con):
            if not row or str(row[0]) not in material_ids:
                continue
            ingested_at = _parse_iso_datetime(manifest_index.get(str(row[0]), {}).get("ingested_at"))
            if ingested_at is None or ingested_at > clustered_at:
                return True
    finally:
        con.close()
    return False


_BRIDGE_DELTA_SCHEMA = '{"links_to_existing":[{"cluster_id":"required existing cluster id","source_concepts":[{"material_id":"required string","concept_name":"required string"}]}],"new_clusters":[{"canonical_name":"required string","descriptor":"short cluster description","aliases":["max 4 strings"],"source_concepts":[{"material_id":"required string","concept_name":"required string"}]}],"_finished":true}'

_BRIDGE_SYSTEM_PROMPT = f"""\
You are an architecture research librarian. You are grouping concepts \
from material packets into broad cross-material umbrella clusters.

Output schema (the user message restates this; both must be obeyed exactly):
{_BRIDGE_DELTA_SCHEMA}

Content rules:
- Favor broader but still meaningful canonical names that connect related materials across the collection.
- Group concepts when they participate in the same broader framework, problematic, spatial condition, institutional logic, typology, method, or field of inquiry.
- Avoid academic jargon, theoretical buzzwords, or pretentious language. Use clear, direct, and specific language that conveys real analytical meaning.
- For each new cluster, write a non-empty short descriptor of at most two brief lines that explains the umbrella idea in plain language.
- Concepts that are genuinely distinct should remain separate clusters; do not merge unrelated ideas under a vague keyword.
- Use the material summaries, local concepts, bridge candidates, and evidence snippets to judge whether two packets belong to the same broader territory.
- Bridge candidates are strong signals of potential connections.
- A cluster may include near-synonyms, differently scaled formulations, and narrower sub-phrases when they clearly belong to the same larger concept.
- It is acceptable for one material to contribute more than one source concept to the same cluster when they support the same umbrella idea.
- Avoid trivial canonical names like "space", "history", "power", or "memory" unless sharply qualified into a real concept phrase.
- Clusters must connect at least two materials, but prefer broader clusters that connect more materials when the analytical connection is strong enough.
- Cluster names may be theoretically dense and multi-word. Avoid near-duplicate concepts, incidental topics, and generic labels like "history", "power", "space", or "memory" unless sharply qualified. Prefer cluster names that carry analytical charge and group local and bridge concepts together, like "spatial justice", "racial capitalism", "architecture as care", "counter-mapping methods", "authoritarian urbanism", or "collecting as spatial practice", and many others.

The user message specifies the exact output schema. Return the final JSON object only, once, at the very end, with no markdown fences, commentary, drafts, or progress updates."""

_REQUIRED_BRIDGE_DELTA_FIELDS = (
        "links_to_existing",
        "new_clusters",
)


def _bridge_cluster_snapshot(cluster: dict) -> dict:
        """Return a minimal, deterministic bridge cluster payload for prompts and merges."""
        aliases = cluster.get("aliases", [])
        source_concepts = cluster.get("source_concepts", [])
        return {
                "cluster_id": str(cluster.get("cluster_id", "")).strip(),
                "canonical_name": str(cluster.get("canonical_name", "")).strip(),
            "descriptor": str(cluster.get("descriptor", "")).strip(),
                "aliases": _dedupe_aliases([str(alias) for alias in aliases if str(alias).strip()]),
                "material_ids": [str(mid) for mid in cluster.get("material_ids", []) if str(mid).strip()],
                "source_concepts": [
                        {
                                "material_id": str(source.get("material_id", "")).strip(),
                                "concept_name": str(source.get("concept_name", "")).strip(),
                        }
                        for source in source_concepts
                        if isinstance(source, dict)
                        and str(source.get("material_id", "")).strip()
                        and str(source.get("concept_name", "")).strip()
                ],
                "confidence": float(cluster.get("confidence", 0.0) or 0.0),
        }


def _stage_bridge_memory_input(root: Path, clusters: list[dict], *, scope_key: str | None = None) -> Path:
        """Write a compact snapshot of the current bridge graph for the LLM to read."""
        payload = {
                "kind": "bridge_memory",
                "clusters": [_bridge_cluster_snapshot(cluster) for cluster in clusters],
        }
        path = _cluster_input_path(root, "bridge_memory", scope_key=scope_key)
        _write_json(path, payload)
        return path


def _build_bridge_prompt(
    bridge_packets_path: Path,
    bridge_memory_path: Path,
    *,
    domain: str = "",
) -> str:
    if is_practice_domain(domain):
        return practice_prompts.local_cluster_user_prompt(bridge_packets_path, bridge_memory_path)
    return (
        f"Read the new concepts packet file from {bridge_packets_path}.\n"
        f"Read the existing bridge cluster memory file from {bridge_memory_path}.\n"
        "Treat both files as source material for the current clustering pass.\n"
        "Use links_to_existing only to attach packet concepts to existing clusters by cluster_id.\n"
        "Use new_clusters when packet concepts should form a new cross-material umbrella cluster instead.\n"
        "Only reference concepts that appear in the packet file.\n"
        "New clusters must connect at least two materials.\n"
        "Do not return single-material clusters.\n"
        "Do all reasoning silently first, then return exactly one final JSON object only when the full clustering job is complete.\n"
        "Do not output partial JSON, drafts, commentary, or progress updates. Follow required output schema closely. This is non-negotiable.\n"
        "Set _finished to true only in that final completed JSON object.\n"
        "Return JSON only.\n"
        "\n"
        "MANDATORY OUTPUT SHAPE — fill in this exact template. Do not rename, omit, or add keys:\n"
        '{\n'
        '  "links_to_existing": [\n'
        '    {"cluster_id": "<existing id>", "source_concepts": [{"material_id": "<id>", "concept_name": "<concept>"}]}\n'
        '  ],\n'
        '  "new_clusters": [\n'
        '    {\n'
        '      "canonical_name": "<umbrella concept phrase, REQUIRED, never empty>",\n'
        '      "descriptor": "<short plain-language description, REQUIRED and non-empty>",\n'
        '      "aliases": ["<optional alias>", "<optional alias>"],\n'
        '      "source_concepts": [\n'
        '        {"material_id": "<id>", "concept_name": "<concept exactly as in packet>"}\n'
        '      ]\n'
        '    }\n'
        '  ],\n'
        '  "_finished": true\n'
        '}\n'
        "\n"
        "Forbidden keys inside new_clusters items: \"label\", \"name\", \"title\", \"rationale\", \"summary\", \"description\", \"materials\", \"concepts\", \"members\", \"cluster_id\". "
        "Use \"canonical_name\" instead of \"label\"/\"name\"/\"title\". "
        "Use \"descriptor\" instead of \"rationale\"/\"summary\"/\"description\". "
        "Use \"source_concepts\" (array of {material_id, concept_name} objects) instead of \"concepts\"/\"members\"/\"materials\". "
        "Never put bare strings in source_concepts; always use objects with material_id and concept_name.\n"
        "\n"
        "Before emitting, self-check every new_clusters item: it must contain exactly the four keys canonical_name, descriptor, aliases, source_concepts, and source_concepts items must be objects with material_id and concept_name. If any entry uses a forbidden key, rename it before output.\n"
    )


def _bridge_system_prompt(domain: str) -> str:
    if is_practice_domain(domain):
        return practice_prompts.local_cluster_system_prompt(_BRIDGE_DELTA_SCHEMA)
    return _BRIDGE_SYSTEM_PROMPT


def _normalize_bridge_source_concepts(source_concepts, *, label: str) -> list[dict]:
    """Normalize a list of bridge source references from the LLM response."""
    if not isinstance(source_concepts, list):
        raise EnrichmentError(f"{label} must be a list of source_concepts")
    normalized: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for entry in source_concepts:
        if not isinstance(entry, dict):
            continue
        material_id = str(entry.get("material_id", "")).strip()
        concept_name = str(entry.get("concept_name", "")).strip()
        if not material_id or not concept_name:
            continue
        key = (material_id, concept_name.casefold())
        if key in seen:
            continue
        seen.add(key)
        normalized.append({
            "material_id": material_id,
            "concept_name": concept_name,
        })
    if not normalized:
        raise EnrichmentError(f"{label} has no valid source_concepts")
    return normalized


def _apply_bridge_delta(existing_clusters: list[dict], parsed: dict) -> list[dict]:
    """Apply a structured LLM delta to the current bridge graph."""
    links_to_existing = parsed.get("links_to_existing")
    new_clusters = parsed.get("new_clusters")
    if not isinstance(links_to_existing, list):
        logger.warning("Bridge clustering: output field 'links_to_existing' is not a list; skipping it")
        links_to_existing = []
    if not isinstance(new_clusters, list):
        logger.warning("Bridge clustering: output field 'new_clusters' is not a list; skipping it")
        new_clusters = []

    combined = [_bridge_cluster_snapshot(cluster) for cluster in existing_clusters]
    by_id = {
        cluster.get("cluster_id", ""): cluster
        for cluster in combined
        if cluster.get("cluster_id", "")
    }

    for idx, link in enumerate(links_to_existing, start=1):
        if not isinstance(link, dict):
            logger.warning("Bridge clustering: skipping links_to_existing[%s] because it is not an object", idx)
            continue
        cluster_id = str(link.get("cluster_id", "")).strip()
        if not cluster_id:
            logger.warning("Bridge clustering: skipping links_to_existing[%s] because cluster_id is missing", idx)
            continue
        target = by_id.get(cluster_id)
        if target is None:
            logger.warning(
                "Bridge clustering: skipping links_to_existing[%s] because cluster_id %r is unknown",
                idx,
                cluster_id,
            )
            continue
        try:
            normalized_source = _normalize_bridge_source_concepts(
                link.get("source_concepts"),
                label=f"links_to_existing[{idx}]",
            )
        except EnrichmentError as exc:
            logger.warning("Bridge clustering: skipping links_to_existing[%s]: %s", idx, exc)
            continue
        target.setdefault("source_concepts", []).extend(normalized_source)

    for idx, cluster in enumerate(new_clusters, start=1):
        if not isinstance(cluster, dict):
            logger.warning("Bridge clustering: skipping new_clusters[%s] because it is not an object", idx)
            continue
        canonical_name = str(cluster.get("canonical_name", "")).strip()
        if not canonical_name:
            logger.warning("Bridge clustering: skipping new_clusters[%s] because canonical_name is missing", idx)
            continue
        descriptor = str(cluster.get("descriptor", "")).strip()
        aliases = cluster.get("aliases", [])
        if aliases is None:
            aliases = []
        if not isinstance(aliases, list):
            logger.warning("Bridge clustering: skipping new_clusters[%s] because aliases is not a list", idx)
            continue
        try:
            normalized_source = _normalize_bridge_source_concepts(
                cluster.get("source_concepts"),
                label=f"new_clusters[{idx}]",
            )
        except EnrichmentError as exc:
            logger.warning("Bridge clustering: skipping new_clusters[%s]: %s", idx, exc)
            continue
        combined.append({
            "canonical_name": canonical_name,
            "descriptor": descriptor,
            "aliases": _dedupe_aliases([canonical_name, *[str(alias) for alias in aliases if str(alias).strip()]]),
            "source_concepts": normalized_source,
        })

    return combined


def _build_concept_index(concept_rows: list[tuple]) -> dict[tuple[str, str], dict]:
    """Index concepts by (material_id, concept_key) for response validation."""
    concept_index: dict[tuple[str, str], dict] = {}
    for row in concept_rows:
        concept_name, concept_key, material_id, relevance, source_pages, evidence_spans, confidence, concept_type, descriptor = _split_concept_row(row)
        concept_index.setdefault((material_id, concept_key), {
            "concept_name": concept_name,
            "concept_key": concept_key,
            "material_id": material_id,
            "relevance": relevance,
            "source_pages": source_pages,
            "evidence_spans": evidence_spans,
            "confidence": confidence,
            "concept_type": concept_type,
            "descriptor": descriptor,
        })
    return concept_index


def _validate_bridge_and_attach_provenance(
    raw_clusters: list[dict],
    concept_index: dict[tuple[str, str], dict],
    material_titles: dict[str, str],
) -> list[dict]:
    """Bridge-cluster validation that allows multiple concepts per material.

    Bridge clusters must remain cross-material umbrellas. Any cluster with fewer
    than two distinct materials is discarded.
    """
    del material_titles

    cluster_records = []
    assigned: set[tuple[str, str]] = set()

    for i, cluster in enumerate(raw_clusters, start=1):
        validated_source = []
        for entry in cluster.get("source_concepts", []):
            mid = entry.get("material_id", "").strip()
            cname = entry.get("concept_name", "").strip()
            indexed = _resolve_concept_reference(mid, cname, concept_index)
            if indexed is None:
                logger.warning(
                    "Bridge cluster %s: dropping unknown reference (%s, %r)",
                    cluster.get("cluster_id", f"#{i}"), mid, cname,
                )
                continue
            validated_source.append(_build_source_concept(indexed))

        if not validated_source:
            continue

        deduped_source = []
        seen_local: set[tuple[str, str]] = set()
        for source in validated_source:
            key = (source["material_id"], source["concept_key"])
            if key in seen_local:
                continue
            seen_local.add(key)
            deduped_source.append(source)
        validated_source = deduped_source

        final_source = []
        for source in validated_source:
            key = (source["material_id"], source["concept_key"])
            if key in assigned:
                continue
            assigned.add(key)
            final_source.append(source)

        material_ids = list(dict.fromkeys(source["material_id"] for source in final_source))
        if len(material_ids) < 2:
            continue

        canonical_name = cluster.get("canonical_name", "").strip() or final_source[0]["concept_name"]
        aliases = _dedupe_aliases([
            canonical_name,
            *cluster.get("aliases", []),
            *[source["concept_name"] for source in final_source],
        ])

        cluster_records.append({
            "canonical_name": canonical_name,
            "descriptor": str(cluster.get("descriptor", "")).strip(),
            "slug": slugify(canonical_name),
            "aliases": aliases,
            "material_ids": material_ids,
            "source_concepts": [
                {
                    "material_id": source["material_id"],
                    "concept_name": source["concept_name"],
                    "relevance": source["relevance"],
                    "source_pages": source["source_pages"],
                    "evidence_spans": source["evidence_spans"],
                    "confidence": source["confidence"],
                }
                for source in final_source
            ],
            "confidence": _derive_cluster_confidence(final_source),
        })

    clean_clusters = []
    for i, cluster in enumerate(cluster_records, start=1):
        clean_clusters.append({
            "cluster_id": f"bridge_{i:04d}",
            **cluster,
        })
    return clean_clusters


def _cluster_scopes(
    manifest_index: dict[str, dict],
    *,
    domain: str | None = None,
    collection: str | None = None,
) -> list[tuple[str, str]]:
    scopes = {
        _collection_scope(str(row.get("domain", "")), str(row.get("collection", "")))
        for row in manifest_index.values()
        if isinstance(row, dict)
    }
    filtered = []
    for item_domain, item_collection in scopes:
        if domain and item_domain != domain:
            continue
        if collection and item_collection != collection:
            continue
        filtered.append((item_domain, item_collection))
    return sorted(filtered)


def cluster_concepts(
    config: dict | None = None,
    *,
    llm_fn: LlmFn | None = None,
    llm_state: dict | None = None,
    force: bool = False,
    domain: str | None = None,
    collection: str | None = None,
) -> dict:
    if config is None:
        config = load_config()
    root = get_project_root()
    log_path = get_logs_root(config) / "cluster.log"
    cluster_start_time = datetime.now()

    def _log_value(value) -> str:
        return str(value).replace("\t", " ").replace("\n", " ").strip()

    def _append_log(*fields) -> None:
        try:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            with open(log_path, "a", encoding="utf-8") as f:
                f.write("\t".join(_log_value(field) for field in fields) + "\n")
        except Exception:
            pass

    domain_filter = (domain or "").strip() or None
    collection_filter = (collection or "").strip() or None
    _append_log(cluster_start_time.isoformat(), "START", "local", domain_filter or "*", collection_filter or "*", force)

    try:
        enabled_domains = get_enabled_domains(config)
        if not any(should_run_clustering(enabled_domain) for enabled_domain in enabled_domains):
            return {
                "collections": 0,
                "total_concepts": 0,
                "clusters": 0,
                "multi_material": 0,
                "skipped": True,
                "reason": "no enabled domains with local clustering enabled",
            }

        db_path = get_indexes_root(config) / "search.sqlite"
        if not db_path.exists():
            raise FileNotFoundError(f"Search index not found at {db_path}. Run `arq index rebuild` first.")

        con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        try:
            concept_rows = _load_concept_rows(con)
            material_rows = _load_material_rows(con)
        finally:
            con.close()

        manifest_index = _load_manifest_index(root)
        scopes = [
            scope
            for scope in _cluster_scopes(manifest_index, domain=domain_filter, collection=collection_filter)
            if scope[0] in enabled_domains and should_run_clustering(scope[0])
        ]
        if not scopes:
            return {
                "collections": 0,
                "total_concepts": 0,
                "clusters": 0,
                "multi_material": 0,
                "skipped": True,
                "reason": "no domains with local clustering enabled",
            }

        if not concept_rows:
            raise EnrichmentError("No concepts in index. Run `arq enrich` on materials first.")

        if llm_fn is None:
            base_llm_fn = None
        else:
            base_llm_fn = llm_fn

        route_signature = get_model_id(config, "cluster")
        total_concepts = 0
        total_clusters = 0
        total_multi = 0
        changed = 0
        workers = _parallel_collection_workers(config, len(scopes), allow_parallel=base_llm_fn is None and llm_state is None)

        def _one(scope_domain: str, scope_collection: str) -> dict:
            scope_key = f"{scope_domain}/{scope_collection}"
            gate_path = _local_cluster_gate_path(root, scope_domain, scope_collection)
            gate_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                gate_fd = os.open(gate_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except FileExistsError:
                _append_log(datetime.now().isoformat(), "SCOPE_SKIP", scope_domain, scope_collection, "busy")
                existing = load_local_clusters(root, domain=scope_domain, collection=scope_collection)
                return {"total_concepts": 0, "clusters": len(existing), "multi_material": sum(1 for row in existing if len(row.get("material_ids", [])) > 1), "changed": 0}

            try:
                os.write(gate_fd, f"{os.getpid()}\n".encode())
                _append_log(datetime.now().isoformat(), "SCOPE_START", scope_domain, scope_collection, force)
                if not is_local_clustering_stale(scope_domain, scope_collection, config, force=force):
                    existing = load_local_clusters(root, domain=scope_domain, collection=scope_collection)
                    _append_log(datetime.now().isoformat(), "SCOPE_SKIP", scope_domain, scope_collection, "up_to_date")
                    return {"total_concepts": 0, "clusters": len(existing), "multi_material": sum(1 for row in existing if len(row.get("material_ids", [])) > 1), "changed": 0}

                clustered_at = None if force else _local_clustered_at_from_stamp(root, scope_domain, scope_collection)
                pending_material_rows = _pending_local_material_rows(material_rows, manifest_index, scope_domain, scope_collection, clustered_at)
                pending_concept_rows = _pending_local_concept_rows(concept_rows, manifest_index, scope_domain, scope_collection, clustered_at)
                existing = load_local_clusters(root, domain=scope_domain, collection=scope_collection)
                scoped_material_ids = _collection_material_ids(manifest_index, scope_domain, scope_collection)
                scoped_concept_rows = [row for row in concept_rows if row and str(row[2]) in scoped_material_ids]
                scoped_material_rows = [row for row in material_rows if row and str(row[0]) in scoped_material_ids]

                if not pending_concept_rows:
                    local_cluster_path(root, scope_domain, scope_collection).parent.mkdir(parents=True, exist_ok=True)
                    _write_jsonl(local_cluster_path(root, scope_domain, scope_collection), existing)
                    local_cluster_stamp_path(root, scope_domain, scope_collection).write_text(
                        json.dumps({
                            "clustered_at": datetime.now(timezone.utc).isoformat(),
                            "fingerprint": local_cluster_fingerprint(scope_domain, scope_collection, config),
                            "route_signature": route_signature,
                            "total_concepts": 0,
                            "clusters": len(existing),
                            "domain": scope_domain,
                            "collection": scope_collection,
                        }, indent=2),
                        encoding="utf-8",
                    )
                    _append_log(datetime.now().isoformat(), "SCOPE_DONE", scope_domain, scope_collection, "no_pending_concepts")
                    return {"total_concepts": 0, "clusters": len(existing), "multi_material": sum(1 for row in existing if len(row.get("material_ids", [])) > 1), "changed": 0}

                scope_llm_fn = base_llm_fn or make_cli_llm_fn(config, "cluster")
                scope_tmp_key = f"local_{local_collection_key(scope_domain, scope_collection)}"
                bridge_packets_path = _stage_bridge_packet_input(
                    root,
                    pending_concept_rows,
                    pending_material_rows,
                    scope_key=scope_tmp_key,
                )
                bridge_memory_path = _stage_bridge_memory_input(
                    root,
                    existing,
                    scope_key=scope_tmp_key,
                )
                try:
                    user_msg = _build_bridge_prompt(
                        bridge_packets_path,
                        bridge_memory_path,
                        domain=scope_domain,
                    )
                    raw_text = scope_llm_fn(
                        _bridge_system_prompt(scope_domain),
                        [{"role": "user", "content": user_msg}],
                    )
                    cluster_debug_enabled = os.environ.get("ARQUIMEDES_CLUSTER_DEBUG", "").strip().lower() in {"1", "true", "yes", "on"}
                    if cluster_debug_enabled:
                        debug_dir = root / "derived" / "debug" / "cluster"
                        debug_dir.mkdir(parents=True, exist_ok=True)
                        debug_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                        debug_name = f"{debug_stamp}_{_tmp_name_fragment(scope_tmp_key) or 'scope'}"
                        (debug_dir / f"{debug_name}_raw.txt").write_text(raw_text or "", encoding="utf-8")
                        (debug_dir / f"{debug_name}_system.txt").write_text(
                            _bridge_system_prompt(scope_domain), encoding="utf-8"
                        )
                        (debug_dir / f"{debug_name}_user.txt").write_text(user_msg, encoding="utf-8")
                    parsed = parse_json_or_repair(scope_llm_fn, raw_text, _BRIDGE_DELTA_SCHEMA)
                    if cluster_debug_enabled:
                        try:
                            (debug_dir / f"{debug_name}_parsed.json").write_text(
                                json.dumps(parsed, indent=2, ensure_ascii=False), encoding="utf-8"
                            )
                        except (TypeError, ValueError):
                            pass
                    if not isinstance(parsed, dict):
                        raise EnrichmentError(f"Cluster output is not a JSON object for {scope_key}")
                    if parsed.get("_finished") is not True:
                        raise EnrichmentError(f"Cluster output missing _finished=true for {scope_key}")
                    parsed = dict(parsed)
                    parsed.pop("_finished", None)
                    missing_output_fields = [field for field in _REQUIRED_BRIDGE_DELTA_FIELDS if field not in parsed]
                    if missing_output_fields:
                        raise EnrichmentError(
                            f"Cluster output missing required fields for {scope_key}: {', '.join(missing_output_fields)}"
                        )
                finally:
                    _cleanup_paths(bridge_packets_path, bridge_memory_path)

                concept_index = _build_concept_index(scoped_concept_rows)
                material_titles = {str(mid): str(title or mid) for mid, title, *_ in scoped_material_rows}
                raw_clusters = _apply_bridge_delta(existing, parsed)
                validated = _validate_bridge_and_attach_provenance(raw_clusters, concept_index, material_titles)
                clusters = normalize_local_clusters(scope_domain, scope_collection, validated)

                run_at = datetime.now(timezone.utc).isoformat()
                _attach_run_provenance(clusters, route_signature, run_at)
                _write_jsonl(local_cluster_path(root, scope_domain, scope_collection), clusters)
                local_cluster_stamp_path(root, scope_domain, scope_collection).write_text(
                    json.dumps({
                        "clustered_at": run_at,
                        "fingerprint": local_cluster_fingerprint(scope_domain, scope_collection, config),
                        "route_signature": route_signature,
                        "total_concepts": len(pending_concept_rows),
                        "clusters": len(clusters),
                        "domain": scope_domain,
                        "collection": scope_collection,
                    }, indent=2),
                    encoding="utf-8",
                )

                _append_log(datetime.now().isoformat(), "SCOPE_DONE", scope_domain, scope_collection, f"concepts={len(pending_concept_rows)} clusters={len(clusters)}")
                return {
                    "total_concepts": len(pending_concept_rows),
                    "clusters": len(clusters),
                    "multi_material": sum(1 for row in clusters if len(row.get("material_ids", [])) > 1),
                    "changed": 1,
                }
            except Exception as exc:
                _append_log(datetime.now().isoformat(), "SCOPE_FAILED", scope_domain, scope_collection, exc)
                raise
            finally:
                try:
                    os.close(gate_fd)
                except OSError:
                    pass
                try:
                    gate_path.unlink()
                except OSError:
                    pass

        if workers > 1:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = [pool.submit(_one, scope_domain, scope_collection) for scope_domain, scope_collection in scopes]
                for future in as_completed(futures):
                    result = future.result()
                    total_concepts += result["total_concepts"]
                    total_clusters += result["clusters"]
                    total_multi += result["multi_material"]
                    changed += result["changed"]
        else:
            for scope_domain, scope_collection in scopes:
                result = _one(scope_domain, scope_collection)
                total_concepts += result["total_concepts"]
                total_clusters += result["clusters"]
                total_multi += result["multi_material"]
                changed += result["changed"]

        cluster_end_time = datetime.now()
        _append_log(cluster_start_time.isoformat(), cluster_end_time.isoformat(), "local", domain_filter or "*", collection_filter or "*", force, "DONE", f"collections={len(scopes)} changed={changed} clusters={total_clusters}")
        return {
            "collections": len(scopes),
            "changed": changed,
            "total_concepts": total_concepts,
            "clusters": total_clusters,
            "multi_material": total_multi,
            "skipped": changed == 0,
        }
    except Exception as exc:
        cluster_end_time = datetime.now()
        _append_log(cluster_start_time.isoformat(), cluster_end_time.isoformat(), "local", domain_filter or "*", collection_filter or "*", force, "FAILED", exc)
        raise


def load_local_clusters(
    project_root: Path | None = None,
    *,
    domain: str | None = None,
    collection: str | None = None,
) -> list[dict]:
    if project_root is None:
        project_root = get_project_root()
    paths = []
    if domain is not None or collection is not None:
        paths = [local_cluster_path(project_root, domain or "practice", collection or "_general")]
    else:
        paths = sorted((project_root / "derived" / "collections").glob("*/local_concept_clusters.jsonl"))
    clusters = []
    for path in paths:
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            cluster = json.loads(line)
            scope_domain, scope_collection = _collection_scope(
                str(cluster.get("domain", "") or path.parent.name.split("__", 1)[0]),
                str(cluster.get("collection", "") or (path.parent.name.split("__", 1)[1] if "__" in path.parent.name else "_general")),
            )
            clusters.extend(normalize_local_clusters(scope_domain, scope_collection, [cluster]))
    return clusters
