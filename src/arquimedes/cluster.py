"""Concept clustering — Phase 5.

Single LLM pass over all concept candidates from the search index.
Groups semantically equivalent concepts across materials into canonical clusters.
Writes derived/concept_clusters.jsonl and derived/cluster_stamp.json.
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
    set_codex_params,
    set_effort,
    set_model,
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


# ---------------------------------------------------------------------------
# Fingerprint / staleness
# ---------------------------------------------------------------------------

def cluster_fingerprint(config: dict | None = None) -> str:
    """SHA256 over local clustering inputs: local concept rows + material titles."""
    if config is None:
        config = load_config()
    root = get_project_root()
    db_path = root / "indexes" / "search.sqlite"
    if not db_path.exists():
        return ""

    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        concepts = _load_concept_rows(con, concept_type="local")
        materials = con.execute(
            "SELECT material_id, title FROM materials ORDER BY material_id"
        ).fetchall()
    finally:
        con.close()

    return enrich_stamps.canonical_hash(list(concepts), list(materials))


def bridge_cluster_fingerprint(config: dict | None = None) -> str:
    """SHA256 over bridge clustering inputs: materials + local + bridge concepts."""
    if config is None:
        config = load_config()
    root = get_project_root()
    db_path = root / "indexes" / "search.sqlite"
    if not db_path.exists():
        return ""

    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        local_concepts = _load_concept_rows(con, concept_type="local")
        bridge_concepts = _load_concept_rows(con, concept_type="bridge_candidate")
        materials = _load_material_rows(con)
    finally:
        con.close()

    return enrich_stamps.canonical_hash(list(local_concepts), list(bridge_concepts), list(materials))


def is_clustering_stale(config: dict | None = None, *, force: bool = False) -> bool:
    """Return True if clustering output is missing, stale, force=True, or empty."""
    if force:
        return True
    if config is None:
        config = load_config()
    root = get_project_root()
    stamp_path = root / "derived" / "cluster_stamp.json"
    if not stamp_path.exists():
        return True
    clusters_path = root / "derived" / "concept_clusters.jsonl"
    if not clusters_path.exists():
        return True
    try:
        stamp = json.loads(stamp_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return True
    # Treat a stamp that recorded 0 clusters (but had concepts) as stale —
    # it means the previous run produced an empty output and must be retried.
    if stamp.get("total_concepts", 0) > 0 and stamp.get("clusters", 0) == 0:
        return True
    current = cluster_fingerprint(config)
    return stamp.get("fingerprint") != current


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


# ---------------------------------------------------------------------------
# Prompt
# ---------------------------------------------------------------------------

_LOCAL_SYSTEM_PROMPT = """\
You are an architecture research librarian. You are grouping local concept candidates \
from multiple academic papers and practice documents into canonical concept clusters.

Rules:
- Group only concepts that are semantically equivalent or clearly the same framework phrased differently
- Choose the clearest, most specific canonical name for each cluster
- Concepts that are genuinely distinct should remain separate clusters; do not over-merge
- Prefer canonical names that are specific, reusable concept phrases rather than generic topic words
- Use evidence excerpts to judge meaning; do not merge concepts just because they share one broad term
- Keep adjacent but distinct theoretical positions separate when the evidence suggests different claims
- Avoid clusters built around vague labels like "space", "history", "power", or "memory" unless the phrase is sharply qualified
- Do not create umbrella clusters, chapter themes, or document-level topic summaries
- A cluster should usually have at most one source concept per material
- If unsure whether two concepts are equivalent, keep them separate
- A concept appearing in only one material is still a valid cluster
- Return valid JSON array, no markdown fences"""


def _build_prompt(
    concept_rows: list[tuple],
    material_titles: dict[str, str],
) -> str:
    """Build the strict local-clustering prompt."""
    lines = []
    for row in concept_rows:
        if len(row) == 8:
            concept_name, concept_key, material_id, relevance, source_pages_json, evidence_spans_json, confidence, _concept_type = row
        else:
            concept_name, concept_key, material_id, relevance, source_pages_json, evidence_spans_json, confidence = row
        title = material_titles.get(material_id, material_id)
        try:
            spans = json.loads(evidence_spans_json or "[]")
        except json.JSONDecodeError:
            spans = []
        excerpt_parts = [span.strip()[:160] for span in spans[:2] if isinstance(span, str) and span.strip()]
        excerpt = " | ".join(excerpt_parts)
        line = (
            f'- concept_name="{concept_name}" | concept_key="{concept_key}" '
            f'| material="{title}" [{material_id}] | relevance={relevance} '
            f'| confidence={confidence} | evidence="{excerpt}"'
        )
        lines.append(line)

    schema_desc = """\
Output schema (JSON array):
[
  {
    "cluster_id": "concept_0001",
    "canonical_name": "...",
    "aliases": ["...", "..."],
    "source_concepts": [
      {"material_id": "...", "concept_name": "..."}
    ],
    "confidence": 0.85
  }
]"""

    return (
        "Group the following concept candidates into canonical clusters.\n"
        "Merge only when the concepts are semantically equivalent or clearly the same framework stated differently.\n"
        "Do not over-merge neighboring but distinct ideas.\n"
        f"There are {len(concept_rows)} input concepts. The total number of source_concepts across all output clusters must also be {len(concept_rows)}.\n"
        "Use the exact material_id and exact concept_name strings from the input.\n"
        "Do not group multiple distinct concepts from the same material into one umbrella cluster.\n\n"
        + "\n".join(lines)
        + "\n\n"
        + schema_desc
    )


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
    material_packets: list[dict],
) -> str:
    """Build the bridge-clustering prompt from compact material packets."""
    lines: list[str] = []
    for packet in material_packets:
        title = packet.get("title", packet.get("material_id", ""))
        material_id = packet.get("material_id", "")
        summary = packet.get("summary", "")
        keywords = packet.get("keywords", [])
        local_concepts = packet.get("local_concepts", [])
        bridge_candidates = packet.get("bridge_candidates", [])
        evidence_snippets = packet.get("evidence_snippets", [])
        lines.append(f'- material="{title}" [{material_id}]')
        if summary:
            lines.append(f'  summary="{summary}"')
        if keywords:
            lines.append(f'  keywords="{", ".join(keywords[:8])}"')
        if local_concepts:
            lines.append("  local_concepts:")
            for concept in local_concepts[:8]:
                lines.append(
                    f'    - concept_name="{concept.get("concept_name", "")}" | relevance={concept.get("relevance", "")}'
                )
        if bridge_candidates:
            lines.append("  bridge_candidates:")
            for concept in bridge_candidates[:8]:
                lines.append(
                    f'    - concept_name="{concept.get("concept_name", "")}" | relevance={concept.get("relevance", "")}'
                )
        if evidence_snippets:
            lines.append("  evidence_snippets:")
            for snippet in evidence_snippets[:5]:
                lines.append(f'    - "{snippet}"')

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
        "Cluster the following bridge candidate concept packets into broader cross-material umbrellas.\n"
        f"There are {len(material_packets)} material packets. Bridge clusters must connect at least two materials.\n"
        "Use the packet summaries, local concepts, bridge candidates, and evidence snippets to judge broader conceptual territory.\n"
        "Do not create single-material bridge clusters.\n\n"
        + "\n".join(lines)
        + "\n\n"
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

def cluster_concepts(
    config: dict | None = None,
    *,
    llm_fn: LlmFn | None = None,
    force: bool = False,
) -> dict:
    """Run concept clustering, write derived artifacts, return summary.

    Returns: {"total_concepts": N, "clusters": M, "multi_material": K}
    """
    if config is None:
        config = load_config()
    root = get_project_root()
    db_path = root / "indexes" / "search.sqlite"

    if not db_path.exists():
        raise EnrichmentError("Search index not found. Run `arq index` first.")

    if not is_clustering_stale(config, force=force):
        logger.info("Clustering is up to date — skipped.")
        # Return counts from existing clusters
        clusters = load_clusters(root)
        multi = sum(1 for c in clusters if len(c.get("material_ids", [])) > 1)
        total = sum(len(c.get("source_concepts", [])) for c in clusters)
        return {"total_concepts": total, "clusters": len(clusters), "multi_material": multi, "skipped": True}

    # --- Load from index ---
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        concept_rows = _load_concept_rows(con, concept_type="local")
        material_rows = con.execute(
            "SELECT material_id, title FROM materials ORDER BY material_id"
        ).fetchall()
    finally:
        con.close()

    if not concept_rows:
        raise EnrichmentError("No concepts in index. Run `arq enrich` on materials first.")

    material_titles: dict[str, str] = {mid: title for mid, title in material_rows}

    # Build lookup index: (material_id, concept_key) → row dict
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

    # --- Build prompt and call LLM ---
    if llm_fn is None:
        llm_fn = make_cli_llm_fn(config, "cluster")

    set_effort(llm_fn, config, "cluster")
    set_model(llm_fn, config, "cluster")
    set_codex_params(llm_fn, config, "cluster")
    user_msg = _build_prompt(concept_rows, material_titles)
    raw_response = llm_fn(_LOCAL_SYSTEM_PROMPT, [{"role": "user", "content": user_msg}])

    schema_desc = (
        'JSON array of cluster objects with keys: cluster_id, canonical_name, '
        'aliases (list), source_concepts (list of {material_id, concept_name}), confidence (float)'
    )
    raw_clusters = parse_json_or_repair(llm_fn, raw_response, schema_desc)
    if not isinstance(raw_clusters, list):
        raise EnrichmentError(f"LLM returned non-list clusters: {type(raw_clusters)}")

    # --- Validate + attach provenance ---
    clusters = _validate_and_attach_provenance(raw_clusters, concept_index, material_titles)

    # Guard: if validation dropped everything, the LLM output was unusable.
    # Refuse to write an empty artifact — this preserves the previous valid state
    # and ensures is_clustering_stale() keeps returning True until a good run succeeds.
    if not clusters and concept_rows:
        raise EnrichmentError(
            f"Clustering produced 0 valid clusters from {len(concept_rows)} concepts — "
            "all LLM-generated references failed validation. Check the LLM output and "
            "run `arq cluster --force` to retry."
        )

    # --- Write derived/ ---
    derived_dir = root / "derived"
    derived_dir.mkdir(exist_ok=True)

    clusters_path = derived_dir / "concept_clusters.jsonl"
    with clusters_path.open("w", encoding="utf-8") as f:
        for c in clusters:
            f.write(json.dumps(c, ensure_ascii=False) + "\n")

    fingerprint = cluster_fingerprint(config)
    stamp_path = derived_dir / "cluster_stamp.json"
    stamp_path.write_text(
        json.dumps({
            "clustered_at": datetime.now(timezone.utc).isoformat(),
            "fingerprint": fingerprint,
            "total_concepts": len(concept_rows),
            "clusters": len(clusters),
        }, indent=2),
        encoding="utf-8",
    )

    multi = sum(1 for c in clusters if len(c.get("material_ids", [])) > 1)
    return {
        "total_concepts": len(concept_rows),
        "clusters": len(clusters),
        "multi_material": multi,
        "skipped": False,
    }


def cluster_bridge_concepts(
    config: dict | None = None,
    *,
    llm_fn: LlmFn | None = None,
    force: bool = False,
) -> dict:
    """Run bridge clustering over bridge candidates, write derived/bridge_concept_clusters.jsonl."""
    if config is None:
        config = load_config()
    root = get_project_root()
    db_path = root / "indexes" / "search.sqlite"

    if not db_path.exists():
        raise EnrichmentError("Search index not found. Run `arq index` first.")

    if not is_bridge_clustering_stale(config, force=force):
        logger.info("Bridge clustering is up to date — skipped.")
        clusters = load_bridge_clusters(root)
        multi = sum(1 for c in clusters if len(c.get("material_ids", [])) > 1)
        total = sum(len(c.get("source_concepts", [])) for c in clusters)
        return {"bridge_concepts": total, "clusters": len(clusters), "multi_material": multi, "skipped": True}

    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        concept_rows = _load_concept_rows(con)
        material_rows = _load_material_rows(con)
    finally:
        con.close()

    if not concept_rows:
        raise EnrichmentError("No concepts in index. Run `arq enrich` on materials first.")

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

    bridge_concept_count = sum(len(packet["bridge_candidates"]) for packet in material_packets)
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
        llm_fn = make_cli_llm_fn(config, "cluster")

    set_effort(llm_fn, config, "cluster")
    set_model(llm_fn, config, "cluster")
    set_codex_params(llm_fn, config, "cluster")
    user_msg = _build_bridge_prompt(material_packets)
    raw_response = llm_fn(_BRIDGE_SYSTEM_PROMPT, [{"role": "user", "content": user_msg}])

    schema_desc = (
        'JSON array of bridge cluster objects with keys: cluster_id, canonical_name, '
        'aliases (list), source_concepts (list of {material_id, concept_name}), confidence (float)'
    )
    raw_clusters = parse_json_or_repair(llm_fn, raw_response, schema_desc)
    if not isinstance(raw_clusters, list):
        raise EnrichmentError(f"LLM returned non-list bridge clusters: {type(raw_clusters)}")

    clusters = _validate_bridge_and_attach_provenance(raw_clusters, { (row[2], row[1]): {
        "concept_name": row[0],
        "concept_key": row[1],
        "material_id": row[2],
        "relevance": row[3],
        "source_pages": row[4],
        "evidence_spans": row[5],
        "confidence": row[6],
    } for row in concept_rows }, {mid: info["title"] for mid, info in material_info.items()})

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
            "bridge_concepts": sum(len(packet["bridge_candidates"]) for packet in material_packets),
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

def load_clusters(project_root: Path | None = None) -> list[dict]:
    """Read derived/concept_clusters.jsonl, return list of cluster dicts."""
    if project_root is None:
        project_root = get_project_root()
    path = project_root / "derived" / "concept_clusters.jsonl"
    if not path.exists():
        return []
    clusters = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            cluster = json.loads(line)
            slug = cluster.get("slug", "")
            cluster["wiki_path"] = f"wiki/shared/concepts/{slug}.md" if slug else ""
            clusters.append(cluster)
    return clusters


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
