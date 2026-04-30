"""Wiki compiler orchestrator — Phase 5.

Loads clusters + materials, renders all page types, handles incremental
tracking and orphan removal. Entry point: compile_wiki().
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from arquimedes import cluster as cluster_mod
from arquimedes import compile_pages
from arquimedes import enrich_stamps
from arquimedes import project_state
from arquimedes.config import (
    get_extracted_root,
    get_indexes_root,
    get_project_root,
    get_wiki_root,
    load_config,
)
from arquimedes.domain_profiles import display_domain_name, get_publication_mode, is_practice_domain
from arquimedes.lint_global_bridge import global_bridge_artifact_paths, load_global_bridge_clusters

logger = logging.getLogger(__name__)
COMPILE_TEMPLATE_VERSION = 2


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    records = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            records.append(json.loads(line))
    return records


def _safe_list(value) -> list:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return [value] if value else []
        return parsed if isinstance(parsed, list) else []
    return []


def _load_json(path: Path, default=None):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return default


def _write_page(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


# ---------------------------------------------------------------------------
# Material stamp
# ---------------------------------------------------------------------------

def _material_stamp(output_dir: Path) -> str:
    """Hash of source enrichment artifacts, ignoring derived feedback fields."""
    parts = []
    meta_path = output_dir / "meta.json"
    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            meta.pop("bridge_concepts", None)
            parts.append(json.dumps(meta, sort_keys=True, ensure_ascii=False))
        except Exception:
            parts.append(meta_path.read_text(encoding="utf-8"))
    else:
        parts.append("")
    for fname in ["chunks.jsonl", "annotations.jsonl"]:
        p = output_dir / fname
        parts.append(p.read_text(encoding="utf-8") if p.exists() else "")
    figs_dir = output_dir / "figures"
    if figs_dir.is_dir():
        for fp in sorted(figs_dir.glob("*.json")):
            parts.append(fp.read_text(encoding="utf-8"))
    return enrich_stamps.canonical_hash(*parts)


def _bridge_feedback_for_material(mid: str, bridge_clusters: list[dict]) -> list[dict]:
    """Return compact bridge-concept refs for a material."""
    refs = []
    for c in bridge_clusters:
        mids = c.get("supporting_material_ids") or c.get("material_ids", []) or []
        if mid not in mids:
            continue
        domain = str(c.get("domain", "")).strip()
        default_wiki_path = (
            f"wiki/{domain}/bridge-concepts/{c.get('slug', '')}.md"
            if domain
            else f"wiki/shared/bridge-concepts/{c.get('slug', '')}.md"
        )
        refs.append({
            "cluster_id": c.get("cluster_id") or c.get("bridge_id", ""),
            "canonical_name": c.get("canonical_name", ""),
            "slug": c.get("slug", ""),
            "wiki_path": c.get("wiki_path") or default_wiki_path,
            "material_count": len(dict.fromkeys(mids)),
            "confidence": c.get("confidence", 0.0),
        })
    return sorted(refs, key=lambda r: r.get("canonical_name", "").lower())


def _update_extracted_meta_bridge_feedback(extracted_root: Path, bridge_clusters: list[dict]) -> int:
    """Write bridge concept refs back into extracted/<mid>/meta.json.

    The derived feedback field is ignored by _material_stamp(), so this does
    not cause perpetual rebuild churn.
    """
    updated = 0
    for meta_path in sorted(extracted_root.glob("*/meta.json")):
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        mid = meta.get("material_id", "")
        if not mid:
            continue
        refs = _bridge_feedback_for_material(mid, bridge_clusters)
        current = meta.get("bridge_concepts") or []
        if current == refs:
            continue
        meta["bridge_concepts"] = refs
        tmp = meta_path.with_suffix(".json.tmp")
        bak = meta_path.with_suffix(".json.bak")
        tmp.write_text(json.dumps(meta, separators=(',', ':'), ensure_ascii=False), encoding="utf-8")
        if meta_path.exists():
            meta_path.replace(bak)
        try:
            tmp.replace(meta_path)
            updated += 1
        except Exception:
            if bak.exists():
                bak.replace(meta_path)
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass
            raise
        finally:
            try:
                bak.unlink(missing_ok=True)
            except Exception:
                pass
    return updated


def _load_graph_maintenance_rows(db_path: Path, lint_dir: Path | None = None) -> list[dict]:
    if not db_path.exists():
        return []
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        rows = con.execute(
            """
            SELECT finding_id, finding_type, severity, summary, details,
                   affected_material_ids, affected_cluster_ids,
                   candidate_future_sources, candidate_bridge_links
            FROM graph_findings
            ORDER BY
                CASE lower(severity)
                    WHEN 'high' THEN 0
                    WHEN 'medium' THEN 1
                    WHEN 'low' THEN 2
                    ELSE 3
                END,
                finding_type,
                finding_id
            """
        ).fetchall()
    except sqlite3.OperationalError:
        rows = []
    finally:
        con.close()

    findings: list[dict] = []
    for row in rows:
        findings.append({
            "finding_id": row[0],
            "finding_type": row[1],
            "severity": row[2],
            "summary": row[3],
            "details": row[4],
            "affected_material_ids": json.loads(row[5] or "[]"),
            "affected_cluster_ids": json.loads(row[6] or "[]"),
            "candidate_future_sources": json.loads(row[7] or "[]"),
            "candidate_bridge_links": json.loads(row[8] or "[]"),
        })
    if findings or lint_dir is None:
        return findings

    graph_path = lint_dir / "graph_findings.jsonl"
    if not graph_path.exists():
        return findings
    for row in _load_jsonl(graph_path):
        finding_id = str(row.get("finding_id", "")).strip()
        if not finding_id:
            continue
        findings.append({
            "finding_id": finding_id,
            "finding_type": str(row.get("finding_type", "")).strip(),
            "severity": str(row.get("severity", "")).strip(),
            "summary": str(row.get("summary", "")).strip(),
            "details": str(row.get("details", "")).strip(),
            "affected_material_ids": _safe_list(row.get("affected_material_ids", [])),
            "affected_cluster_ids": _safe_list(row.get("affected_cluster_ids", [])),
            "candidate_future_sources": _safe_list(row.get("candidate_future_sources", [])),
            "candidate_bridge_links": _safe_list(row.get("candidate_bridge_links", [])),
        })
    return findings


def _render_graph_maintenance_page(
    db_path: Path,
    lint_dir: Path,
    bridge_clusters: list[dict],
    material_titles: dict[str, str],
    material_paths: dict[str, str],
) -> str | None:
    findings = _load_graph_maintenance_rows(db_path, lint_dir)
    if not findings:
        return None

    cluster_lookup = {
        str(cluster.get("cluster_id", "")).strip(): {
            "name": str(cluster.get("canonical_name", "")).strip() or str(cluster.get("cluster_id", "")).strip(),
            "path": cluster.get("wiki_path")
            or (
                f"wiki/{str(cluster.get('domain', '')).strip()}/bridge-concepts/{cluster.get('slug', '')}.md"
                if str(cluster.get("domain", "")).strip()
                else f"wiki/shared/bridge-concepts/{cluster.get('slug', '')}.md"
            ),
        }
        for cluster in bridge_clusters
        if str(cluster.get("cluster_id", "")).strip()
    }

    page_path = "wiki/shared/maintenance/graph-health.md"
    lines: list[str] = [
        "# Graph Maintenance\n",
        "This page is compiled from SQL-backed graph findings and surfaces unresolved semantic maintenance items.\n",
        "",
    ]

    severity_counts = Counter(str(row.get("severity", "")).strip().lower() or "unspecified" for row in findings)
    lines.append("## Overview\n")
    lines.append(f"- Findings: {len(findings)}")
    for severity in ("high", "medium", "low", "unspecified"):
        if severity_counts.get(severity):
            label = severity.title() if severity != "unspecified" else "Unspecified"
            lines.append(f"- {label}: {severity_counts[severity]}")
    lines.append("")

    for severity in ("high", "medium", "low", "unspecified"):
        severity_rows = [row for row in findings if (str(row.get("severity", "")).strip().lower() or "unspecified") == severity]
        if not severity_rows:
            continue
        heading = severity.title() if severity != "unspecified" else "Unspecified"
        lines.append(f"## {heading} Priority\n")
        for row in severity_rows:
            title = str(row.get("summary", "")).strip() or str(row.get("finding_type", "")).strip() or row["finding_id"]
            finding_type = str(row.get("finding_type", "")).strip()
            details = str(row.get("details", "")).strip()
            lines.append(f"### {title}\n")
            if finding_type:
                lines.append(f"**Type:** {finding_type}")
            if details:
                lines.append(details)
            cluster_ids = [cid for cid in row.get("affected_cluster_ids", []) if str(cid).strip()]
            if cluster_ids:
                lines.append("")
                lines.append("**Affected bridge concepts**")
                for cid in cluster_ids:
                    cluster = cluster_lookup.get(str(cid).strip())
                    if cluster:
                        rel = compile_pages._relative_link(page_path, cluster["path"])
                        lines.append(f"- [{cluster['name']}]({rel})")
                    else:
                        lines.append(f"- {cid}")
            material_ids = [mid for mid in row.get("affected_material_ids", []) if str(mid).strip()]
            if material_ids:
                lines.append("")
                lines.append("**Affected materials**")
                for mid in material_ids:
                    title = material_titles.get(str(mid).strip(), str(mid).strip())
                    mat_path = material_paths.get(str(mid).strip(), "")
                    if mat_path:
                        rel = compile_pages._relative_link(page_path, mat_path)
                        lines.append(f"- [{title}]({rel})")
                    else:
                        lines.append(f"- {title}")
            future_sources = [src for src in row.get("candidate_future_sources", []) if str(src).strip()]
            if future_sources:
                lines.append("")
                lines.append("**Likely next sources**")
                for src in future_sources:
                    lines.append(f"- {src}")
            bridge_links = [link for link in row.get("candidate_bridge_links", []) if str(link).strip()]
            if bridge_links:
                lines.append("")
                lines.append("**Candidate bridge links**")
                for link in bridge_links:
                    lines.append(f"- {link}")
            lines.append("")

    return "\n".join(lines).rstrip() + "\n"


# ---------------------------------------------------------------------------
# Stamp I/O
# ---------------------------------------------------------------------------

def _load_compile_stamp(project_root: Path) -> dict | None:
    return _load_json(project_root / "derived" / "compile_stamp.json")


def _write_compile_stamp(
    project_root: Path,
    material_stamps: dict[str, str],
    cluster_stamp: str,
) -> None:
    path = project_root / "derived" / "compile_stamp.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({
            "compiled_at": datetime.now(timezone.utc).isoformat(),
            "material_stamps": material_stamps,
            "cluster_stamp": cluster_stamp,
            "template_version": COMPILE_TEMPLATE_VERSION,
        }, separators=(',', ':')),
        encoding="utf-8",
    )


def _cluster_file_stamp(project_root: Path) -> str:
    local_paths = sorted((project_root / "derived" / "collections").glob("*/local_concept_clusters.jsonl"))
    parts = [path.read_text(encoding="utf-8") for path in local_paths]
    for path in global_bridge_artifact_paths(project_root):
        parts.append(path.read_text(encoding="utf-8"))
    return enrich_stamps.canonical_hash(*parts)


# ---------------------------------------------------------------------------
# Related materials
# ---------------------------------------------------------------------------

def _find_related(
    material_id: str,
    clusters: list[dict],
    db_path: Path,
) -> list[dict]:
    """Compute related materials via shared clusters, keywords, authors, facets."""
    scores: dict[str, dict] = {}  # other_mid → {score, reasons, title}

    # 1. Shared clusters
    for c in clusters:
        mids = c.get("material_ids", [])
        if material_id in mids and len(mids) > 1:
            cname = c["canonical_name"]
            for other in mids:
                if other == material_id:
                    continue
                if other not in scores:
                    scores[other] = {"score": 0, "reasons": [], "title": ""}
                scores[other]["score"] += 3
                scores[other]["reasons"].append(f"shared concept: {cname}")

    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        # Titles for all related candidates
        if scores:
            placeholders = ",".join("?" for _ in scores)
            rows = con.execute(
                f"SELECT material_id, title FROM materials WHERE material_id IN ({placeholders})",
                list(scores.keys()),
            ).fetchall()
            for mid, title in rows:
                if mid in scores:
                    scores[mid]["title"] = title

        # 2. Shared keywords
        kw_rows = con.execute(
            "SELECT mk2.material_id, mk1.keyword, m.title "
            "FROM material_keywords mk1 "
            "JOIN material_keywords mk2 ON mk1.keyword = mk2.keyword "
            "JOIN materials m ON mk2.material_id = m.material_id "
            "WHERE mk1.material_id = ? AND mk2.material_id != ?",
            (material_id, material_id),
        ).fetchall()
        for other, kw, title in kw_rows:
            if other not in scores:
                scores[other] = {"score": 0, "reasons": [], "title": title}
            scores[other]["score"] += 1
            scores[other]["title"] = scores[other]["title"] or title
            scores[other]["reasons"].append(f"shared keyword: {kw}")

        # 3. Shared authors
        au_rows = con.execute(
            "SELECT ma2.material_id, ma1.author, m.title "
            "FROM material_authors ma1 "
            "JOIN material_authors ma2 ON ma1.author = ma2.author "
            "JOIN materials m ON ma2.material_id = m.material_id "
            "WHERE ma1.material_id = ? AND ma2.material_id != ?",
            (material_id, material_id),
        ).fetchall()
        for other, author, title in au_rows:
            if other not in scores:
                scores[other] = {"score": 0, "reasons": [], "title": title}
            scores[other]["score"] += 2
            scores[other]["title"] = scores[other]["title"] or title
            scores[other]["reasons"].append(f"shared author: {author}")

        # 4. Shared facets
        facet_cols = [
            "building_type", "scale", "location", "jurisdiction", "climate",
            "program", "material_system", "structural_system", "historical_period",
            "course_topic", "studio_project",
        ]
        this_row = con.execute(
            f"SELECT {', '.join(facet_cols)} FROM materials WHERE material_id = ?",
            (material_id,),
        ).fetchone()
        if this_row:
            for col_idx, col in enumerate(facet_cols):
                val = this_row[col_idx]
                if not val or not val.strip():
                    continue
                facet_rows = con.execute(
                    f"SELECT material_id, title FROM materials "
                    f"WHERE {col} = ? AND material_id != ?",
                    (val, material_id),
                ).fetchall()
                for other, title in facet_rows:
                    if other not in scores:
                        scores[other] = {"score": 0, "reasons": [], "title": title}
                    scores[other]["score"] += 1
                    scores[other]["title"] = scores[other]["title"] or title
                    scores[other]["reasons"].append(f"shared {col.replace('_', ' ')}: {val}")
    finally:
        con.close()

    # Sort by score, limit 10, deduplicate reasons
    results = []
    for mid, info in sorted(scores.items(), key=lambda x: -x[1]["score"])[:10]:
        seen_reasons = []
        for r in info["reasons"]:
            if r not in seen_reasons:
                seen_reasons.append(r)
        results.append({
            "material_id": mid,
            "title": info["title"] or mid,
            "reasons": seen_reasons[:5],
        })
    return results


# ---------------------------------------------------------------------------
# Orphan removal
# ---------------------------------------------------------------------------

def _remove_orphans(
    wiki_root: Path,
    current_material_ids: set[str],
    current_slugs: set[str] | None = None,
    current_concept_paths: set[str] | None = None,
) -> list[str]:
    """Delete wiki pages for removed materials or clusters. Returns removed paths."""
    removed = []
    if not wiki_root.is_dir():
        return removed

    for md_file in wiki_root.rglob("*.md"):
        rel = md_file.relative_to(wiki_root)
        parts = rel.parts

        rel_wiki_path = f"wiki/{rel.as_posix()}"

        # Concept page: shared/concepts/{slug}.md or {domain}/bridge-concepts/{slug}.md
        if len(parts) == 3 and parts[0] == "shared" and parts[1] in {"concepts", "bridge-concepts"}:
            slug = parts[2].replace(".md", "")
            if slug.startswith("_"):
                continue
            if current_concept_paths is not None:
                remove = rel_wiki_path not in current_concept_paths
            else:
                remove = slug not in (current_slugs or set())
            if remove:
                logger.info("Removing orphan concept page: %s", md_file)
                md_file.unlink()
                removed.append(str(md_file))
            continue

        if len(parts) == 3 and parts[1] == "bridge-concepts":
            slug = parts[2].replace(".md", "")
            if slug.startswith("_"):
                continue
            if current_concept_paths is not None and rel_wiki_path not in current_concept_paths:
                logger.info("Removing orphan concept page: %s", md_file)
                md_file.unlink()
                removed.append(str(md_file))
            continue

        # Local concept page: {domain}/{collection}/concepts/{slug}.md
        if len(parts) == 4 and parts[2] == "concepts":
            slug = parts[3].replace(".md", "")
            if slug.startswith("_"):
                continue
            if current_concept_paths is not None and rel_wiki_path not in current_concept_paths:
                logger.info("Removing orphan concept page: %s", md_file)
                md_file.unlink()
                removed.append(str(md_file))
            continue

        # Maintenance pages are compiler outputs, not orphaned material pages.
        if len(parts) == 3 and parts[0] == "shared" and parts[1] == "maintenance":
            continue

        # Material page: {domain}/{collection}/{material_id}.md
        if len(parts) == 3:
            stem = parts[2].replace(".md", "")
            if stem not in current_material_ids and not stem.startswith("_"):
                logger.info("Removing orphan material page: %s", md_file)
                md_file.unlink()
                removed.append(str(md_file))

    return removed


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def compile_wiki(
    config: dict | None = None,
    *,
    force: bool = False,
    force_cluster: bool = False,
    skip_cluster: bool = False,
    recompile_pages: bool = False,
    run_quick_lint: bool = True,
    llm_fn=None,
) -> dict:
    """Compile the wiki from enriched materials and concept clusters.

    Returns a summary dict for CLI output.
    """
    if config is None:
        config = load_config()
    root = get_project_root()
    wiki_root = get_wiki_root()
    extracted_root = get_extracted_root()
    db_path = get_indexes_root(config) / "search.sqlite"

    # 1. Ensure search index
    if not db_path.exists():
        from arquimedes.index import ensure_index
        ensure_index(config)

    # 2. Run clustering if stale or forced
    local_cluster_summary = None
    existing_local_clusters = cluster_mod.load_local_clusters(root)
    if skip_cluster:
        bridge_cluster_summary = {"skipped": True, "skip_reason": "cluster skipped for refresh"}
        local_cluster_summary = {"skipped": True, "skip_reason": "cluster skipped for refresh"} if existing_local_clusters else None
    elif recompile_pages:
        bridge_cluster_summary = {"skipped": True, "skip_reason": "recompile pages requested"}
        local_cluster_summary = {"skipped": True, "skip_reason": "recompile pages requested"} if existing_local_clusters else None
    else:
        local_cluster_summary = cluster_mod.cluster_concepts(
            config, llm_fn=llm_fn, force=force or force_cluster
        )
        bridge_cluster_summary = {
            "skipped": True,
            "skip_reason": "legacy raw-material bridge clustering retired; using local collection clusters",
        }

    # 3. Load clusters
    local_clusters = cluster_mod.load_local_clusters(root)
    global_bridge_clusters = load_global_bridge_clusters(root)
    page_clusters = local_clusters
    concept_page_clusters = local_clusters
    bridge_page_clusters = global_bridge_clusters
    material_titles: dict[str, str] = {}
    if db_path.exists():
        con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        try:
            for mid, title in con.execute("SELECT material_id, title FROM materials").fetchall():
                material_titles[mid] = title
        finally:
            con.close()

    # 4. Load all material metadata
    all_metas: dict[str, dict] = {}
    for meta_path in sorted(extracted_root.glob("*/meta.json")):
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            mid = meta["material_id"]
            all_metas[mid] = meta
        except (json.JSONDecodeError, KeyError):
            logger.warning("Skipping invalid meta.json: %s", meta_path)

    # Build material_paths: material_id → wiki-relative path (used in concept page links)
    material_paths: dict[str, str] = {
        mid: compile_pages._material_wiki_path(meta)
        for mid, meta in all_metas.items()
    }

    lint_dir = root / "derived" / "lint"
    concept_reflections = {
        row.get("cluster_id", ""): row
        for row in _load_jsonl(lint_dir / "concept_reflections.jsonl")
        if row.get("cluster_id", "")
    }
    cluster_reviews_by_cluster: dict[str, list[dict]] = {}
    for row in _load_jsonl(lint_dir / "cluster_reviews.jsonl"):
        if not isinstance(row, dict):
            continue
        cluster_id = str(row.get("cluster_id", "")).strip()
        if not cluster_id:
            continue
        cluster_reviews_by_cluster.setdefault(cluster_id, []).append(row)
    collection_reflections = {
        f"{row.get('domain', '')}/{row.get('collection', '')}": row
        for row in _load_jsonl(lint_dir / "collection_reflections.jsonl")
        if row.get("domain", "") and row.get("collection", "")
    }

    local_concept_entries: list[dict] = []
    if db_path.exists():
        con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        try:
            concept_rows = con.execute(
                """
                SELECT concept_name, concept_key, material_id, relevance, source_pages
                FROM concepts
                WHERE concept_type = 'local'
                ORDER BY concept_name, material_id
                """
            ).fetchall()
        finally:
            con.close()
        for concept_name, _concept_key, material_id, relevance, source_pages in concept_rows:
            mat_title = material_titles.get(material_id, material_id)
            mat_path = material_paths.get(material_id, "")
            collection = (all_metas.get(material_id, {}) or {}).get("collection") or "_general"
            rel = compile_pages._relative_link("wiki/shared/concepts/_index.md", mat_path) if mat_path else ""
            try:
                page_nums = [str(p) for p in json.loads(source_pages or "[]") if str(p).strip()]
            except json.JSONDecodeError:
                page_nums = []
            summary = mat_title
            if relevance:
                summary += f" · {relevance}"
            if page_nums:
                summary += f" · p. {', '.join(page_nums[:3])}"
            local_concept_entries.append({
                "name": concept_name,
                "path": rel,
                "summary": summary,
                "collection": collection,
            })

    # 5. Incremental stamps
    prev_stamp = _load_compile_stamp(root) if not force else None
    prev_material_stamps: dict[str, str] = (prev_stamp or {}).get("material_stamps", {})
    prev_cluster_stamp: str = (prev_stamp or {}).get("cluster_stamp", "")
    prev_template_version = int((prev_stamp or {}).get("template_version") or 0)
    current_cluster_stamp = _cluster_file_stamp(root)
    templates_changed = prev_template_version != COMPILE_TEMPLATE_VERSION
    clusters_changed = (current_cluster_stamp != prev_cluster_stamp) or templates_changed or force or recompile_pages

    # 6. Material clusters index: material_id → list of concept homes
    material_clusters: dict[str, list[dict]] = {mid: [] for mid in all_metas}
    for c in concept_page_clusters:
        for mid in c.get("material_ids", []):
            if mid in material_clusters:
                material_clusters[mid].append(c)

    bridge_memberships_by_local_cluster: dict[str, list[dict]] = {}
    for bridge in bridge_page_clusters:
        for member in bridge.get("member_local_clusters", []) or []:
            cluster_id = str(member.get("cluster_id", "")).strip()
            if not cluster_id:
                continue
            bridge_memberships_by_local_cluster.setdefault(cluster_id, []).append(bridge)

    # 7. Render material pages (only changed)
    mat_pages_written = 0
    mat_pages_skipped = 0
    current_material_stamps: dict[str, str] = {}
    for mid, meta in all_metas.items():
        output_dir = extracted_root / mid
        stamp = _material_stamp(output_dir)
        current_material_stamps[mid] = stamp
        if not (force or recompile_pages or templates_changed) and prev_material_stamps.get(mid) == stamp:
            mat_pages_skipped += 1
            continue

        chunks = _load_jsonl(output_dir / "chunks.jsonl")
        annotations = _load_jsonl(output_dir / "annotations.jsonl")
        figures = []
        figs_dir = output_dir / "figures"
        if figs_dir.is_dir():
            for fp in sorted(figs_dir.glob("*.json")):
                try:
                    figures.append(json.loads(fp.read_text(encoding="utf-8")))
                except json.JSONDecodeError:
                    pass

        related = _find_related(mid, concept_page_clusters, db_path) if db_path.exists() else []
        mat_clusters = material_clusters.get(mid, [])

        # Build raw file link (file:// URL) and extracted text link (relative to wiki page)
        raw_file_link: str | None = None
        source_path = meta.get("source_path") or ""
        library_root_str = config.get("library_root") or ""
        if source_path and library_root_str:
            from urllib.parse import quote
            abs_src = Path(library_root_str) / source_path
            raw_file_link = "file://" + quote(str(abs_src), safe="/:")

        page_path = wiki_root / Path(compile_pages._material_wiki_path(meta)).relative_to("wiki")
        text_md = output_dir / "text.md"
        extracted_text_link: str | None = None
        if text_md.exists():
            extracted_text_link = os.path.relpath(text_md, page_path.parent)

        content = compile_pages.render_material_page(
            meta, mat_clusters, chunks, annotations, figures, related,
            material_paths=material_paths,
            raw_file_link=raw_file_link,
            extracted_text_link=extracted_text_link,
        )
        _write_page(page_path, content)
        mat_pages_written += 1

    # 8. Render concept pages (all, when concept clusters changed)
    concept_pages_written = 0
    if clusters_changed or force or recompile_pages:
        rendered_cluster_ids: set[str] = set()
        for c in concept_page_clusters:
            cluster_id = str(c.get("cluster_id") or c.get("bridge_id", "")).strip()
            mid_set = set(c.get("material_ids", []))
            related_concepts = []
            for other in concept_page_clusters:
                if other["slug"] == c["slug"]:
                    continue
                if mid_set & set(other.get("material_ids", [])):
                    related_concepts.append({
                        "canonical_name": other["canonical_name"],
                        "slug": other["slug"],
                        "wiki_path": other.get("wiki_path")
                        or (
                            f"wiki/{str(other.get('domain', '')).strip()}/bridge-concepts/{other['slug']}.md"
                            if str(other.get("domain", "")).strip()
                            else f"wiki/shared/bridge-concepts/{other['slug']}.md"
                        ),
                    })
            content = compile_pages.render_concept_page(
                c,
                material_titles,
                related_concepts,
                material_paths,
                concept_reflections.get(c.get("cluster_id", "")),
                cluster_reviews_by_cluster.get(str(c.get("cluster_id", "")).strip(), []),
                bridge_memberships_by_local_cluster.get(cluster_id, []),
            )
            page_path = wiki_root / Path(
                c.get("wiki_path")
                or (
                    f"wiki/{str(c.get('domain', '')).strip()}/bridge-concepts/{c['slug']}.md"
                    if str(c.get("domain", "")).strip()
                    else f"wiki/shared/bridge-concepts/{c['slug']}.md"
                )
            ).relative_to("wiki")
            _write_page(page_path, content)
            concept_pages_written += 1
            if cluster_id:
                rendered_cluster_ids.add(cluster_id)
        for c in bridge_page_clusters:
            bridge_id = str(c.get("cluster_id") or c.get("bridge_id", "")).strip()
            if bridge_id and bridge_id in rendered_cluster_ids:
                continue
            member_ids = {
                str(member.get("cluster_id", "")).strip()
                for member in c.get("member_local_clusters", [])
                if str(member.get("cluster_id", "")).strip()
            }
            related_concepts = []
            for other in bridge_page_clusters:
                other_id = str(other.get("cluster_id") or other.get("bridge_id", "")).strip()
                if other_id == bridge_id:
                    continue
                other_member_ids = {
                    str(member.get("cluster_id", "")).strip()
                    for member in other.get("member_local_clusters", [])
                    if str(member.get("cluster_id", "")).strip()
                }
                if member_ids and member_ids & other_member_ids:
                    related_concepts.append({
                        "canonical_name": other["canonical_name"],
                        "slug": other["slug"],
                        "wiki_path": other.get("wiki_path")
                        or (
                            f"wiki/{str(other.get('domain', '')).strip()}/bridge-concepts/{other['slug']}.md"
                            if str(other.get("domain", "")).strip()
                            else f"wiki/shared/bridge-concepts/{other['slug']}.md"
                        ),
                    })
            content = compile_pages.render_concept_page(
                c,
                material_titles,
                related_concepts,
                material_paths,
                None,
                cluster_reviews_by_cluster.get(bridge_id, []),
                None,
            )
            page_path = wiki_root / Path(
                c.get("wiki_path")
                or (
                    f"wiki/{str(c.get('domain', '')).strip()}/bridge-concepts/{c['slug']}.md"
                    if str(c.get("domain", "")).strip()
                    else f"wiki/shared/bridge-concepts/{c['slug']}.md"
                )
            ).relative_to("wiki")
            _write_page(page_path, content)
            concept_pages_written += 1

    # 9. Render index pages (always)
    manifest_records = _load_jsonl(root / "manifests" / "materials.jsonl")
    # Make reflection tables queryable before rendering the graph-maintenance page.
    from arquimedes.memory import memory_rebuild
    try:
        memory_rebuild(config)
    except FileNotFoundError:
        pass  # index or cluster file absent — safe to skip

    maintenance_path = wiki_root / "shared" / "maintenance" / "graph-health.md"
    graph_maintenance = _render_graph_maintenance_page(
        db_path,
        lint_dir,
        concept_page_clusters,
        material_titles,
        material_paths,
    )
    if graph_maintenance:
        _write_page(maintenance_path, graph_maintenance)
    elif maintenance_path.exists():
        maintenance_path.unlink()

    index_pages_written = _render_index_pages(
        wiki_root,
        all_metas,
        concept_page_clusters,
        material_clusters,
        manifest_records,
        local_concept_entries,
        collection_reflections,
        bridge_page_clusters,
    )

    # 10. Feed bridge concepts back into extracted metadata for future enrichment/reflection.
    bridge_feedback_written = _update_extracted_meta_bridge_feedback(extracted_root, bridge_page_clusters)

    # 11. Orphan removal
    current_concept_paths = {
        str(
            c.get("wiki_path")
            or (
                f"wiki/{str(c.get('domain', '')).strip()}/bridge-concepts/{c['slug']}.md"
                if str(c.get("domain", "")).strip()
                else f"wiki/shared/bridge-concepts/{c['slug']}.md"
            )
        )
        for c in [*concept_page_clusters, *bridge_page_clusters]
    }
    orphans = _remove_orphans(wiki_root, set(all_metas.keys()), current_concept_paths=current_concept_paths)

    # 12. Write compile stamp
    _write_compile_stamp(root, current_material_stamps, current_cluster_stamp)

    quick_lint_summary = None
    lint_cfg = config.get("lint", {}) if isinstance(config, dict) else {}
    if run_quick_lint and lint_cfg.get("post_compile_quick", True):
        try:
            from arquimedes.lint import run_lint

            quick_lint_summary = run_lint(config, quick=True, report=True)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Post-compile quick lint failed: %s", exc)
            quick_lint_summary = {"error": str(exc)}

    return {
        "material_pages": mat_pages_written,
        "material_pages_skipped": mat_pages_skipped,
        "concept_pages": concept_pages_written,
        "index_pages": index_pages_written,
        "bridge_feedback_written": bridge_feedback_written,
        "orphans_removed": len(orphans),
        "quick_lint": quick_lint_summary,
        "clustering": {
            "local": local_cluster_summary,
            "bridge": bridge_cluster_summary,
        },
    }


# ---------------------------------------------------------------------------
# Index page rendering
# ---------------------------------------------------------------------------

def _render_index_pages(
    wiki_root: Path,
    all_metas: dict[str, dict],
    concept_clusters: list[dict],
    material_clusters: dict[str, list[dict]],
    manifest_records: list[dict] | None = None,
    local_concept_entries: list[dict] | None = None,
    collection_reflections: dict[str, dict] | None = None,
    bridge_clusters: list[dict] | None = None,
) -> int:
    """Render master, domain, collection, and concept index pages. Returns count."""
    written = 0
    bridge_by_domain: dict[str, list[dict]] = {}
    for cluster in bridge_clusters or []:
        domain = (str(cluster.get("domain", "")).strip() or "").strip()
        if domain:
            bridge_by_domain.setdefault(domain, []).append(cluster)

    # ingested_at lookup
    ingested_at: dict[str, str] = {}
    for rec in (manifest_records or []):
        mid = rec.get("material_id", "")
        if mid:
            ingested_at[mid] = rec.get("ingested_at", "")

    # Build domain/collection tree
    tree: dict[str, dict[str, list[dict]]] = {}
    for mid, meta in all_metas.items():
        domain = (meta.get("domain") or "practice").strip() or "practice"
        collection = (meta.get("collection") or "").strip() or "_general"
        tree.setdefault(domain, {}).setdefault(collection, []).append(meta)

    all_material_entries = []
    domain_pages: list[dict] = []
    for domain, collections in tree.items():
        domain_entries: list[dict] = []
        domain_index = f"wiki/{domain}/_index.md"
        for collection, metas in collections.items():
            coll_index = f"wiki/{domain}/{collection}/_index.md"
            coll_mids = {m["material_id"] for m in metas}

            # Material entries
            coll_entries = []
            for meta in metas:
                mid = meta["material_id"]
                mat_path = compile_pages._material_wiki_path(meta)
                coll_rel = compile_pages._relative_link(coll_index, mat_path)
                domain_rel = compile_pages._relative_link(domain_index, mat_path)
                root_rel = compile_pages._relative_link("wiki/_index.md", mat_path)
                coll_entry = {
                    "name": meta.get("title") or mid,
                    "path": coll_rel,
                    "summary": compile_pages._meta_val(meta.get("summary") or "")[:120],
                }
                domain_entry = dict(coll_entry)
                domain_entry["path"] = domain_rel
                root_entry = dict(coll_entry)
                root_entry["path"] = root_rel
                coll_entries.append(coll_entry)
                domain_entries.append(domain_entry)
                all_material_entries.append(root_entry)

            if get_publication_mode(domain) == "project_dossier":
                if collection == "_general":
                    logger.warning("Skipping Proyectos/_general project page for %s loose material(s)", len(metas))
                    continue
                state = project_state.load_project_state(collection, root=get_project_root())
                notes = project_state.load_project_notes(collection, root=get_project_root())
                sections = project_state.load_project_sections(collection, root=get_project_root())
                recent = sorted(
                    [
                        {
                            "name": m.get("title") or m["material_id"],
                            "path": compile_pages._relative_link(
                                coll_index, compile_pages._material_wiki_path(m)
                            ),
                            "ingested_at": ingested_at.get(m["material_id"], ""),
                        }
                        for m in metas
                    ],
                    key=lambda x: x.get("ingested_at", ""),
                    reverse=True,
                )
                project_title = state.get("project_title") or collection.replace("_", " ").replace("-", " ").title()
                content = compile_pages.render_project_page(
                    f"{display_domain_name(domain)} / {project_title}",
                    collection,
                    state,
                    [{**entry, "material_id": meta["material_id"]} for entry, meta in zip(coll_entries, metas)],
                    recent,
                    notes,
                    sections,
                )
                _write_page(wiki_root / domain / collection / "_index.md", content)
                written += 1
                continue

            # Key concepts: canonical concept homes with >=1 material in this collection
            concept_counts: dict[str, int] = {}
            concept_info: dict[str, dict] = {}
            concept_relevance: dict[str, float] = {}  # higher = stronger
            _rel_scores = {"high": 3, "medium": 2, "low": 1}
            for c in concept_clusters:
                overlap = coll_mids & set(c.get("material_ids", []))
                if overlap:
                    cid = str(c.get("cluster_id", "")).strip() or c["slug"]
                    concept_counts[cid] = len(overlap)
                    concept_info[cid] = c
                    # Sum relevance scores for source_concepts in this collection
                    rel_sum = sum(
                        _rel_scores.get(sc.get("relevance", "").lower(), 0)
                        for sc in c.get("source_concepts", [])
                        if sc.get("material_id") in overlap
                    )
                    concept_relevance[cid] = rel_sum
            key_concepts = []
            collection_concept_entries = []
            concept_index = f"wiki/{domain}/{collection}/concepts/_index.md"
            for slug, count in sorted(
                concept_counts.items(),
                key=lambda x: (
                    -x[1],
                    -concept_relevance.get(x[0], 0),
                    concept_info[x[0]].get("canonical_name", "").lower(),
                ),
            ):
                c = concept_info[slug]
                dest = c.get("wiki_path") or (
                    f"wiki/{str(c.get('domain', '')).strip()}/bridge-concepts/{slug}.md"
                    if str(c.get("domain", "")).strip()
                    else f"wiki/shared/bridge-concepts/{slug}.md"
                )
                rel = compile_pages._relative_link(coll_index, dest)
                name = c["canonical_name"]
                if "/bridge-concepts/" in dest:
                    name += " (bridge)"
                key_concepts.append({"name": name, "path": rel, "count": count})
                collection_concept_entries.append({
                    "name": name,
                    "path": compile_pages._relative_link(concept_index, dest),
                    "summary": str(concept_info[slug].get("descriptor", "")).strip() or f"{count} material{'s' if count != 1 else ''}",
                })

            # Top facets: frequency of non-empty facet values
            facet_fields = [
                "building_type", "scale", "location", "jurisdiction", "climate",
                "program", "material_system", "structural_system", "historical_period",
                "course_topic", "studio_project",
            ]
            facet_freq: dict[tuple[str, str], int] = {}
            for meta in metas:
                facets = meta.get("facets") or {}
                for field in facet_fields:
                    val = compile_pages._meta_val(facets.get(field) or "").strip()
                    if val:
                        facet_freq[(field, val)] = facet_freq.get((field, val), 0) + 1
            top_facets = [
                {"field": f, "value": v, "count": cnt}
                for (f, v), cnt in sorted(facet_freq.items(), key=lambda x: (-x[1], x[0]))
                if cnt >= 2
            ]

            # Recent additions: sorted by ingested_at descending
            recent = sorted(
                [
                    {
                        "name": m.get("title") or m["material_id"],
                        "path": compile_pages._relative_link(
                            coll_index, compile_pages._material_wiki_path(m)
                        ),
                        "ingested_at": ingested_at.get(m["material_id"], ""),
                    }
                    for m in metas
                ],
                key=lambda x: x.get("ingested_at", ""),
                reverse=True,
            )

            friendly_title = f"{display_domain_name(domain)} / {collection.replace('_', ' ').title()}"
            content = compile_pages.render_collection_page(
                friendly_title, domain, collection,
                coll_entries, key_concepts, top_facets, recent,
                (collection_reflections or {}).get(f"{domain}/{collection}"),
            )
            _write_page(wiki_root / domain / collection / "_index.md", content)
            written += 1
            concept_index_title = (
                f"{friendly_title} {compile_pages._label(domain, 'concepts', 'Concepts')}"
                if is_practice_domain(domain, default="research")
                else f"{friendly_title} Concepts"
            )
            _write_page(
                wiki_root / domain / collection / "concepts" / "_index.md",
                compile_pages.render_index_page(concept_index_title, collection_concept_entries),
            )
            written += 1

        bridge_entries = []
        bridge_index = f"wiki/{domain}/bridge-concepts/_index.md"
        for cluster in sorted(
            bridge_by_domain.get(domain, []),
            key=lambda row: (str(row.get("canonical_name", "")).casefold(), str(row.get("bridge_id", "")).casefold()),
        ):
            bridge_entries.append(
                {
                    "name": str(cluster.get("canonical_name", "")).strip() or str(cluster.get("bridge_id", "")).strip(),
                    "path": compile_pages._relative_link(
                        bridge_index,
                        str(cluster.get("wiki_path", "")).strip(),
                    ),
                    "summary": str(cluster.get("descriptor", "")).strip()
                    or str(cluster.get("why_this_bridge_matters", "")).strip()[:120],
                }
            )
        if bridge_entries:
            domain_entries.append(
                {
                    "name": compile_pages._label(domain, "bridge_concepts", "Bridge Concepts"),
                    "path": compile_pages._relative_link(domain_index, bridge_index),
                    "summary": f"{len(bridge_entries)} bridge concepts",
                }
            )
            _write_page(
                wiki_root / domain / "bridge-concepts" / "_index.md",
                compile_pages.render_index_page(
                    f"{display_domain_name(domain)} {compile_pages._label(domain, 'bridge_concepts', 'Bridge Concepts')}",
                    bridge_entries,
                ),
            )
            written += 1

        domain_content = compile_pages.render_index_page(display_domain_name(domain), domain_entries)
        _write_page(wiki_root / domain / "_index.md", domain_content)
        written += 1
        domain_pages.append({
            "name": display_domain_name(domain),
            "path": f"{domain}/_index.md",
            "summary": f"{sum(len(cols) for cols in collections.values())} materials",
        })

    # Raw material-level local concept index
    concept_entries = local_concept_entries or []
    _write_page(
        wiki_root / "shared" / "concepts" / "_index.md",
        compile_pages.render_grouped_index_page("Local Concepts", concept_entries, "collection", "Collection"),
    )
    written += 1

    # Bridge concept glossary
    glossary_clusters = bridge_clusters if bridge_clusters is not None else concept_clusters
    bridge_glossary = compile_pages.render_glossary(glossary_clusters)
    _write_page(
        wiki_root / "shared" / "glossary" / "_index.md",
        bridge_glossary,
    )
    written += 1

    # Master index
    maintenance_path = wiki_root / "shared" / "maintenance" / "graph-health.md"
    maintenance_entry = None
    if maintenance_path.exists():
        maintenance_entry = {
            "name": "Maintenance",
            "path": "shared/maintenance/graph-health.md",
            "summary": "Semantic maintenance page",
        }
    master_content = compile_pages.render_index_page(
        "Arquimedes Wiki",
        domain_pages + [
            {"name": "Local Concepts", "path": "shared/concepts/_index.md", "summary": f"{len(concept_entries)} local concepts"},
            {"name": "Main Concepts", "path": "shared/glossary/_index.md", "summary": f"{len(glossary_clusters)} main concepts"},
            *([maintenance_entry] if maintenance_entry else []),
        ],
    )
    _write_page(wiki_root / "_index.md", master_content)
    written += 1

    return written
