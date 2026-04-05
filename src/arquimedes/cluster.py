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


# ---------------------------------------------------------------------------
# Fingerprint / staleness
# ---------------------------------------------------------------------------

def cluster_fingerprint(config: dict | None = None) -> str:
    """SHA256 over all clustering inputs: concept rows + material titles."""
    if config is None:
        config = load_config()
    root = get_project_root()
    db_path = root / "indexes" / "search.sqlite"
    if not db_path.exists():
        return ""

    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        concepts = con.execute(
            "SELECT concept_key, material_id, relevance, source_pages, evidence_spans, confidence "
            "FROM concepts ORDER BY concept_key, material_id"
        ).fetchall()
        materials = con.execute(
            "SELECT material_id, title FROM materials ORDER BY material_id"
        ).fetchall()
    finally:
        con.close()

    return enrich_stamps.canonical_hash(list(concepts), list(materials))


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


# ---------------------------------------------------------------------------
# Prompt
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """\
You are an architecture research librarian. You are grouping concept candidates \
from multiple academic papers and practice documents into canonical concept clusters.

Rules:
- Group concepts that describe the same intellectual territory, even if phrased differently
- Choose the clearest, most specific canonical name for each cluster
- Concepts that are genuinely distinct should remain separate clusters (do not over-merge)
- A concept appearing in only one material is still a valid cluster
- Return valid JSON array, no markdown fences"""


def _build_prompt(
    concept_rows: list[tuple],
    material_titles: dict[str, str],
) -> str:
    """Build the user message listing all concept candidates with evidence."""
    lines = []
    for concept_name, concept_key, material_id, relevance, source_pages_json, evidence_spans_json, confidence in concept_rows:
        title = material_titles.get(material_id, material_id)
        try:
            spans = json.loads(evidence_spans_json or "[]")
        except json.JSONDecodeError:
            spans = []
        excerpt = spans[0][:100] if spans else ""
        line = (
            f'- concept_key="{concept_key}" | material="{title}" [{material_id}] '
            f'| relevance={relevance} | evidence="{excerpt}"'
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
        "Group the following concept candidates into canonical clusters:\n\n"
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
    """Validate source_concepts against indexed rows; attach full provenance.

    concept_index key: (material_id, concept_key)
    Drops hallucinated references and logs warnings.
    """
    clean_clusters = []
    for i, cluster in enumerate(raw_clusters, start=1):
        raw_source = cluster.get("source_concepts", [])
        validated_source = []
        for entry in raw_source:
            mid = entry.get("material_id", "").strip()
            cname = entry.get("concept_name", "").strip()
            ckey = _normalize_concept_name(cname)
            indexed = concept_index.get((mid, ckey))
            if indexed is None:
                logger.warning(
                    "Cluster %s: dropping unknown reference (%s, %r) [normalized key=%r]",
                    cluster.get("cluster_id", f"#{i}"), mid, cname, ckey,
                )
                continue
            validated_source.append({
                "material_id": mid,
                "concept_name": indexed["concept_name"],
                "relevance": indexed["relevance"],
                "source_pages": json.loads(indexed["source_pages"] or "[]"),
                "evidence_spans": json.loads(indexed["evidence_spans"] or "[]"),
                "confidence": indexed["confidence"],
            })

        if not validated_source:
            logger.warning("Cluster %s has no valid source_concepts after validation — skipping.", cluster.get("cluster_id", f"#{i}"))
            continue

        material_ids = list(dict.fromkeys(sc["material_id"] for sc in validated_source))
        canonical_name = cluster.get("canonical_name", "").strip()
        clean_clusters.append({
            "cluster_id": f"concept_{i:04d}",
            "canonical_name": canonical_name,
            "slug": slugify(canonical_name),
            "aliases": cluster.get("aliases", [canonical_name]),
            "material_ids": material_ids,
            "source_concepts": validated_source,
            "confidence": float(cluster.get("confidence", 0.0)),
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
        concept_rows = con.execute(
            "SELECT concept_name, concept_key, material_id, relevance, "
            "source_pages, evidence_spans, confidence "
            "FROM concepts ORDER BY concept_key, material_id"
        ).fetchall()
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
        concept_name, concept_key, material_id, relevance, source_pages, evidence_spans, confidence = row
        concept_index[(material_id, concept_key)] = {
            "concept_name": concept_name,
            "concept_key": concept_key,
            "material_id": material_id,
            "relevance": relevance,
            "source_pages": source_pages,
            "evidence_spans": evidence_spans,
            "confidence": confidence,
        }

    # --- Build prompt and call LLM ---
    if llm_fn is None:
        llm_fn = make_cli_llm_fn(config)

    user_msg = _build_prompt(concept_rows, material_titles)
    raw_response = llm_fn(_SYSTEM_PROMPT, [{"role": "user", "content": user_msg}])

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
            clusters.append(json.loads(line))
    return clusters
