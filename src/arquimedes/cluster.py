"""Bridge concept clustering — Phase 5.

Single LLM pass over bridge candidate packets.
Groups cross-material concepts into canonical bridge clusters.
Writes derived/bridge_concept_clusters.jsonl and derived/bridge_cluster_stamp.json.
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from arquimedes import enrich_stamps
from arquimedes.config import get_project_root, load_config
from arquimedes.enrich_llm import (
    EnrichmentError,
    LlmFn,
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
        "concept_key": indexed["concept_key"],
        "relevance": indexed["relevance"],
        "source_pages": json.loads(indexed["source_pages"] or "[]"),
        "evidence_spans": json.loads(indexed["evidence_spans"] or "[]"),
        "confidence": indexed["confidence"],
    }


def _cluster_input_path(root: Path, kind: str) -> Path:
    """Return the staged input path for a clustering run."""
    return root / "derived" / "tmp" / f"{kind}_cluster_input.json"


def _write_json(path: Path, payload: object) -> None:
    """Write JSON payloads atomically enough for staged LLM inputs."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _stage_bridge_packet_input(
    root: Path,
    concept_rows: list[tuple],
    material_rows: list[tuple],
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
        concept_name, concept_key, material_id, relevance, source_pages, evidence_spans, confidence, concept_type = row
        item = grouped.setdefault(material_id, {
            "material_id": material_id,
            "title": material_info.get(material_id, {}).get("title", material_id),
            "summary": material_info.get(material_id, {}).get("summary", ""),
            "keywords": material_info.get(material_id, {}).get("keywords", []),
            "local_concepts": [],
            "bridge_candidates": [],
            "evidence_snippets": [],
        })
        concept = {
            "concept_name": concept_name,
            "concept_key": concept_key,
            "relevance": relevance,
            "source_pages": source_pages,
            "evidence_spans": evidence_spans,
            "confidence": confidence,
        }
        if concept_type == "bridge_candidate":
            item["bridge_candidates"].append(concept)
        else:
            item["local_concepts"].append(concept)
        try:
            spans = json.loads(evidence_spans or "[]")
        except json.JSONDecodeError:
            spans = []
        for span in spans[:2]:
            if isinstance(span, str) and span.strip():
                item["evidence_snippets"].append(span.strip())

    material_packets = []
    for packet in grouped.values():
        local_sorted = sorted(
            packet["local_concepts"],
            key=lambda c: (0 if c.get("relevance") == "high" else 1 if c.get("relevance") == "medium" else 2, c.get("concept_name", "")),
        )[:8]
        bridge_sorted = sorted(
            packet["bridge_candidates"],
            key=lambda c: (0 if c.get("relevance") == "high" else 1 if c.get("relevance") == "medium" else 2, c.get("concept_name", "")),
        )[:8]
        snippets = []
        seen_snippets: set[str] = set()
        for snippet in packet["evidence_snippets"]:
            if snippet not in seen_snippets:
                seen_snippets.add(snippet)
                snippets.append(snippet)
        material_packets.append({
            "material_id": packet["material_id"],
            "title": packet["title"],
            "summary": packet["summary"],
            "keywords": packet["keywords"],
            "local_concepts": local_sorted,
            "bridge_candidates": bridge_sorted,
            "evidence_snippets": snippets[:5],
        })

    payload = {
        "kind": "bridge_packets",
        "material_packet_count": len(material_packets),
        "material_packets": material_packets,
    }
    path = _cluster_input_path(root, "bridge")
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
        "source_pages, evidence_spans, confidence, concept_type "
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
            return [tuple(row) + ("local",) for row in rows]
        return []


def _load_material_rows(con: sqlite3.Connection) -> list[tuple]:
    """Load material metadata needed for clustering prompts."""
    rows = con.execute(
        "SELECT material_id, title, summary, keywords FROM materials ORDER BY material_id"
    ).fetchall()
    return list(rows)


def bridge_cluster_fingerprint(config: dict | None = None) -> str:
    """SHA256 over bridge clustering inputs: concept packets + bridge clusters."""
    if config is None:
        config = load_config()
    root = get_project_root()
    db_path = root / "indexes" / "search.sqlite"
    if not db_path.exists():
        return ""

    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        concepts = _load_concept_rows(con)
        materials = _load_material_rows(con)
    finally:
        con.close()

    root_bridge_clusters = load_bridge_clusters(root)
    return enrich_stamps.canonical_hash(list(concepts), list(materials), list(root_bridge_clusters))


def is_bridge_clustering_stale(config: dict | None = None, *, force: bool = False) -> bool:
    """Return True if bridge clustering output is missing, stale, force=True, or empty."""
    if force:
        return True
    if config is None:
        config = load_config()
    root = get_project_root()
    stamp_path = root / "derived" / "bridge_cluster_stamp.json"
    if not stamp_path.exists():
        return True
    clusters_path = root / "derived" / "bridge_concept_clusters.jsonl"
    if not clusters_path.exists():
        return True
    try:
        stamp = json.loads(stamp_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return True
    if stamp.get("bridge_concepts", 0) > 0 and stamp.get("clusters", 0) == 0:
        return True
    current = bridge_cluster_fingerprint(config)
    return stamp.get("fingerprint") != current


def _cluster_output_path(root: Path, kind: str) -> Path:
    return root / "derived" / "tmp" / f"{kind}_clusters.json"


_BRIDGE_SYSTEM_PROMPT = """\
You are an architecture research librarian. You are clustering bridge candidate concepts \
from material packets into broad cross-material umbrella concepts.

Rules:
- Favor broader but still meaningful canonical names that connect related materials across the collection
- Group concepts when they participate in the same broader framework, problematic, spatial condition, institutional logic, typology, method, or field of inquiry
- Concepts that are genuinely distinct should remain separate clusters; do not merge unrelated ideas under a vague keyword
- Use the material summaries, local concepts, bridge candidates, and evidence snippets to judge whether two packets belong to the same broader territory
- A cluster may include near-synonyms, differently scaled formulations, and narrower sub-phrases when they clearly belong to the same larger concept
- It is acceptable for one material to contribute more than one source concept to the same cluster when they support the same umbrella idea
- Avoid trivial canonical names like "space", "history", "power", or "memory" unless sharply qualified into a real concept phrase
- Bridge clusters must connect at least two materials
- Return valid JSON array, no markdown fences"""


def _build_bridge_prompt(
    bridge_packets_path: Path,
    bridge_clusters_path: Path,
    output_path: Path,
) -> str:
    schema_desc = """\
Output schema (JSON array):
[
  {
    "cluster_id": "bridge_0001",
    "canonical_name": "...",
    "aliases": ["...", "..."],
    "source_concepts": [
      {"material_id": "...", "concept_name": "..."}
    ],
    "confidence": 0.85
  }
]"""

    return (
        f"Read the bridge packet file from {bridge_packets_path}.\n"
        f"Read the current bridge memory file from {bridge_clusters_path}.\n"
        "Treat those files as the source of truth; preserve the existing bridge concepts unless strong evidence demands a merge, split, or rename.\n"
        "Use the per-material packets as the input signal for bridge clustering.\n"
        "Bridge clusters must connect at least two materials.\n"
        "Do not create single-material bridge clusters.\n"
        f"Write the updated bridge clusters to {output_path} using the Write tool.\n"
        "Do not stream JSON into the response.\n"
        "Confirm with a single line when done.\n\n"
        + schema_desc
    )


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def _validate_and_attach_provenance(
    raw_clusters: list[dict],
    concept_index: dict[tuple[str, str], dict],
    material_titles: dict[str, str],
) -> list[dict]:
    """Strict local-cluster validation with provenance recovery.

    Local clusters should behave like precise wiki concepts: at most one source
    concept per material in a cluster, with singletons backfilled for anything
    the LLM left unassigned.
    """
    del material_titles  # kept for compatibility with existing call sites/tests

    cluster_records = []
    assigned: set[tuple[str, str]] = set()
    singleton_candidates: dict[tuple[str, str], dict] = {}

    for i, cluster in enumerate(raw_clusters, start=1):
        raw_source = cluster.get("source_concepts", [])
        validated_source = []
        for entry in raw_source:
            mid = entry.get("material_id", "").strip()
            cname = entry.get("concept_name", "").strip()
            indexed = _resolve_concept_reference(mid, cname, concept_index)
            if indexed is None:
                ckey = _normalize_concept_name(cname)
                logger.warning(
                    "Cluster %s: dropping unknown reference (%s, %r) [normalized key=%r]",
                    cluster.get("cluster_id", f"#{i}"), mid, cname, ckey,
                )
                continue
            validated_source.append(_build_source_concept(indexed))

        if not validated_source:
            logger.warning(
                "Cluster %s has no valid source_concepts after validation — skipping.",
                cluster.get("cluster_id", f"#{i}"),
            )
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

        canonical_name = cluster.get("canonical_name", "").strip()
        references = [canonical_name, *cluster.get("aliases", [])]
        by_material: dict[str, list[dict]] = {}
        for source in validated_source:
            by_material.setdefault(source["material_id"], []).append(source)

        if len(by_material) == 1 and len(validated_source) > 1:
            logger.warning(
                "Cluster %s over-merged %d concepts from one material; splitting to singletons.",
                cluster.get("cluster_id", f"#{i}"),
                len(validated_source),
            )
            for source in validated_source:
                singleton_candidates.setdefault((source["material_id"], source["concept_key"]), source)
            continue

        retained_source = []
        for material_id, sources in by_material.items():
            if len(sources) == 1:
                retained_source.append(sources[0])
                continue
            ranked = sorted(
                sources,
                key=lambda source: _match_score(source["concept_name"], references),
                reverse=True,
            )
            retained_source.append(ranked[0])
            for source in ranked[1:]:
                singleton_candidates.setdefault((source["material_id"], source["concept_key"]), source)
            logger.warning(
                "Cluster %s had multiple concepts from material %s; retained one and split %d to singletons.",
                cluster.get("cluster_id", f"#{i}"),
                material_id,
                len(ranked) - 1,
            )

        final_source = []
        for source in retained_source:
            key = (source["material_id"], source["concept_key"])
            if key in assigned:
                logger.warning(
                    "Cluster %s duplicates concept (%s, %r) already assigned elsewhere; dropping later copy.",
                    cluster.get("cluster_id", f"#{i}"),
                    source["material_id"],
                    source["concept_name"],
                )
                continue
            assigned.add(key)
            final_source.append(source)

        if not final_source:
            continue

        if len(final_source) == 1:
            canonical_name = final_source[0]["concept_name"]
            aliases = [canonical_name]
            confidence = 1.0
        else:
            if not canonical_name:
                canonical_name = final_source[0]["concept_name"]
            aliases = _dedupe_aliases([
                canonical_name,
                *cluster.get("aliases", []),
                *[source["concept_name"] for source in final_source],
            ])
            confidence = float(cluster.get("confidence", 0.0))

        cluster_records.append({
            "canonical_name": canonical_name,
            "slug": slugify(canonical_name),
            "aliases": aliases,
            "material_ids": list(dict.fromkeys(source["material_id"] for source in final_source)),
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
            "confidence": confidence,
        })

    for key, indexed in sorted(concept_index.items()):
        if key in assigned:
            continue
        source = singleton_candidates.get(key) or _build_source_concept(indexed)
        cluster_records.append({
            "canonical_name": source["concept_name"],
            "slug": slugify(source["concept_name"]),
            "aliases": [source["concept_name"]],
            "material_ids": [source["material_id"]],
            "source_concepts": [{
                "material_id": source["material_id"],
                "concept_name": source["concept_name"],
                "relevance": source["relevance"],
                "source_pages": source["source_pages"],
                "evidence_spans": source["evidence_spans"],
                "confidence": source["confidence"],
            }],
            "confidence": 1.0,
        })

    clean_clusters = []
    for i, cluster in enumerate(cluster_records, start=1):
        clean_clusters.append({
            "cluster_id": f"concept_{i:04d}",
            **cluster,
        })
    return clean_clusters


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
            "confidence": float(cluster.get("confidence", 0.0)),
        })

    clean_clusters = []
    for i, cluster in enumerate(cluster_records, start=1):
        clean_clusters.append({
            "cluster_id": f"bridge_{i:04d}",
            **cluster,
        })
    return clean_clusters


# ---------------------------------------------------------------------------
# Main clustering function
# ---------------------------------------------------------------------------

def cluster_bridge_concepts(
    config: dict | None = None,
    *,
    llm_fn: LlmFn | None = None,
    llm_state: dict | None = None,
    force: bool = False,
) -> dict:
    """Run bridge clustering over bridge candidates, write derived/bridge_concept_clusters.jsonl."""
    if config is None:
        config = load_config()
    root = get_project_root()

    if not is_bridge_clustering_stale(config, force=force):
        logger.info("Bridge clustering is up to date — skipped.")
        clusters = load_bridge_clusters(root)
        multi = sum(1 for c in clusters if len(c.get("material_ids", [])) > 1)
        total = sum(len(c.get("source_concepts", [])) for c in clusters)
        return {"bridge_concepts": total, "clusters": len(clusters), "multi_material": multi, "skipped": True}

    con = sqlite3.connect(f"file:{root / 'indexes' / 'search.sqlite'}?mode=ro", uri=True)
    try:
        concept_rows = _load_concept_rows(con)
        material_rows = _load_material_rows(con)
    finally:
        con.close()

    if not concept_rows:
        raise EnrichmentError("No concepts in index. Run `arq enrich` on materials first.")

    existing_bridge_clusters = load_bridge_clusters(root)
    bridge_concept_count = len(concept_rows)
    if bridge_concept_count == 0:
        derived_dir = root / "derived"
        derived_dir.mkdir(exist_ok=True)
        bridge_path = derived_dir / "bridge_concept_clusters.jsonl"
        bridge_path.write_text("", encoding="utf-8")

        fingerprint = bridge_cluster_fingerprint(config)
        stamp_path = derived_dir / "bridge_cluster_stamp.json"
        stamp_path.write_text(
            json.dumps({
                "clustered_at": datetime.now(timezone.utc).isoformat(),
                "fingerprint": fingerprint,
                "bridge_concepts": 0,
                "clusters": 0,
            }, indent=2),
            encoding="utf-8",
        )

        logger.info("Bridge clustering skipped — no bridge candidates found.")
        return {
            "bridge_concepts": 0,
            "clusters": 0,
            "multi_material": 0,
            "skipped": True,
        }

    if llm_fn is None:
        llm_fn = make_cli_llm_fn(config, "cluster", state=llm_state)

    output_path = _cluster_output_path(root, "bridge")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        output_path.unlink()

    bridge_packets_path = _stage_bridge_packet_input(root, concept_rows, material_rows)
    bridge_clusters_path = root / "derived" / "bridge_concept_clusters.jsonl"
    user_msg = (
        _build_bridge_prompt(bridge_packets_path, bridge_clusters_path, output_path)
    )
    raw_response = llm_fn(_BRIDGE_SYSTEM_PROMPT, [{"role": "user", "content": user_msg}])
    if output_path.exists() and output_path.stat().st_size > 0:
        raw_response = output_path.read_text(encoding="utf-8")

    schema_desc = (
        'JSON array of bridge cluster objects with keys: cluster_id, canonical_name, '
        'aliases (list), source_concepts (list of {material_id, concept_name}), confidence (float)'
    )
    raw_clusters = parse_json_or_repair(llm_fn, raw_response, schema_desc)
    if not isinstance(raw_clusters, list):
        raise EnrichmentError(f"LLM returned non-list bridge clusters: {type(raw_clusters)}")

    concept_index: dict[tuple[str, str], dict] = {}
    for row in concept_rows:
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

    clusters = _validate_bridge_and_attach_provenance(raw_clusters, concept_index, {})

    if existing_bridge_clusters:
        merged: dict[str, dict] = {}
        for cluster in existing_bridge_clusters:
            cluster_id = str(cluster.get("cluster_id", "")).strip()
            if cluster_id:
                merged[cluster_id] = cluster
        for cluster in clusters:
            cluster_id = str(cluster.get("cluster_id", "")).strip()
            if cluster_id:
                merged[cluster_id] = cluster
        clusters = list(merged.values()) if merged else clusters

    derived_dir = root / "derived"
    derived_dir.mkdir(exist_ok=True)
    bridge_path = derived_dir / "bridge_concept_clusters.jsonl"
    with bridge_path.open("w", encoding="utf-8") as f:
        for c in clusters:
            f.write(json.dumps(c, ensure_ascii=False) + "\n")

    fingerprint = bridge_cluster_fingerprint(config)
    stamp_path = derived_dir / "bridge_cluster_stamp.json"
    stamp_path.write_text(
        json.dumps({
            "clustered_at": datetime.now(timezone.utc).isoformat(),
            "fingerprint": fingerprint,
            "bridge_concepts": bridge_concept_count,
            "clusters": len(clusters),
        }, indent=2),
        encoding="utf-8",
    )

    multi = sum(1 for c in clusters if len(c.get("material_ids", [])) > 1)
    return {
        "bridge_concepts": bridge_concept_count,
        "clusters": len(clusters),
        "multi_material": multi,
        "skipped": False,
    }


# ---------------------------------------------------------------------------
# Load utility
# ---------------------------------------------------------------------------

def load_bridge_clusters(project_root: Path | None = None) -> list[dict]:
    """Read derived/bridge_concept_clusters.jsonl, return list of bridge cluster dicts."""
    if project_root is None:
        project_root = get_project_root()
    path = project_root / "derived" / "bridge_concept_clusters.jsonl"
    if not path.exists():
        return []
    clusters = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            cluster = json.loads(line)
            slug = cluster.get("slug", "")
            cluster["wiki_path"] = f"wiki/shared/bridge-concepts/{slug}.md" if slug else ""
            clusters.append(cluster)
    return clusters
