"""Wiki compiler orchestrator — Phase 5.

Loads clusters + materials, renders all page types, handles incremental
tracking and orphan removal. Entry point: compile_wiki().
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from arquimedes import cluster as cluster_mod
from arquimedes import compile_pages
from arquimedes import enrich_stamps
from arquimedes.config import get_project_root, load_config

logger = logging.getLogger(__name__)


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
    """Hash of meta.json + chunks.jsonl + annotations.jsonl + figures/*.json."""
    parts = []
    for fname in ["meta.json", "chunks.jsonl", "annotations.jsonl"]:
        p = output_dir / fname
        parts.append(p.read_text(encoding="utf-8") if p.exists() else "")
    figs_dir = output_dir / "figures"
    if figs_dir.is_dir():
        for fp in sorted(figs_dir.glob("*.json")):
            parts.append(fp.read_text(encoding="utf-8"))
    return enrich_stamps.canonical_hash(*parts)


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
        }, indent=2),
        encoding="utf-8",
    )


def _cluster_file_stamp(project_root: Path) -> str:
    p = project_root / "derived" / "concept_clusters.jsonl"
    return enrich_stamps.canonical_hash(p.read_text(encoding="utf-8") if p.exists() else "")


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
    current_slugs: set[str],
) -> list[str]:
    """Delete wiki pages for removed materials or clusters. Returns removed paths."""
    removed = []
    if not wiki_root.is_dir():
        return removed

    for md_file in wiki_root.rglob("*.md"):
        rel = md_file.relative_to(wiki_root)
        parts = rel.parts

        # Concept page: shared/concepts/{slug}.md
        if len(parts) == 3 and parts[0] == "shared" and parts[1] == "concepts":
            slug = parts[2].replace(".md", "")
            if slug not in current_slugs:
                logger.info("Removing orphan concept page: %s", md_file)
                md_file.unlink()
                removed.append(str(md_file))
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
    llm_fn=None,
) -> dict:
    """Compile the wiki from enriched materials and concept clusters.

    Returns a summary dict for CLI output.
    """
    if config is None:
        config = load_config()
    root = get_project_root()
    wiki_root = root / "wiki"
    extracted_root = root / "extracted"
    db_path = root / "indexes" / "search.sqlite"

    # 1. Ensure search index
    if not db_path.exists():
        from arquimedes.index import ensure_index
        ensure_index(config)

    # 2. Run clustering if stale or forced
    cluster_summary = cluster_mod.cluster_concepts(
        config, llm_fn=llm_fn, force=force or force_cluster
    )

    # 3. Load clusters
    clusters = cluster_mod.load_clusters(root)
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

    # 5. Incremental stamps
    prev_stamp = _load_compile_stamp(root) if not force else None
    prev_material_stamps: dict[str, str] = (prev_stamp or {}).get("material_stamps", {})
    prev_cluster_stamp: str = (prev_stamp or {}).get("cluster_stamp", "")
    current_cluster_stamp = _cluster_file_stamp(root)
    clusters_changed = (current_cluster_stamp != prev_cluster_stamp) or force

    # 6. Material clusters index: material_id → list of clusters
    material_clusters: dict[str, list[dict]] = {mid: [] for mid in all_metas}
    for c in clusters:
        for mid in c.get("material_ids", []):
            if mid in material_clusters:
                material_clusters[mid].append(c)

    # 7. Render material pages (only changed)
    mat_pages_written = 0
    mat_pages_skipped = 0
    current_material_stamps: dict[str, str] = {}
    for mid, meta in all_metas.items():
        output_dir = extracted_root / mid
        stamp = _material_stamp(output_dir)
        current_material_stamps[mid] = stamp
        if not force and prev_material_stamps.get(mid) == stamp:
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

        related = _find_related(mid, clusters, db_path) if db_path.exists() else []
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
            raw_file_link=raw_file_link,
            extracted_text_link=extracted_text_link,
        )
        _write_page(page_path, content)
        mat_pages_written += 1

    # 8. Render concept pages (all, when clusters changed)
    concept_pages_written = 0
    if clusters_changed or force:
        current_slugs = {c["slug"] for c in clusters}
        # Precompute related_concepts for each cluster
        slug_to_cluster = {c["slug"]: c for c in clusters}
        for c in clusters:
            mid_set = set(c.get("material_ids", []))
            related_concepts = []
            for other in clusters:
                if other["slug"] == c["slug"]:
                    continue
                if mid_set & set(other.get("material_ids", [])):
                    related_concepts.append({
                        "canonical_name": other["canonical_name"],
                        "slug": other["slug"],
                    })
            content = compile_pages.render_concept_page(c, material_titles, related_concepts, material_paths)
            page_path = wiki_root / "shared" / "concepts" / f"{c['slug']}.md"
            _write_page(page_path, content)
            concept_pages_written += 1

    # 9. Render index pages (always)
    index_pages_written = _render_index_pages(
        wiki_root, all_metas, clusters, material_clusters
    )

    # 10. Render glossary
    glossary_content = compile_pages.render_glossary(clusters)
    glossary_path = wiki_root / "shared" / "glossary" / "_index.md"
    _write_page(glossary_path, glossary_content)

    # 11. Orphan removal
    current_slugs = {c["slug"] for c in clusters}
    orphans = _remove_orphans(wiki_root, set(all_metas.keys()), current_slugs)

    # 12. Write compile stamp
    _write_compile_stamp(root, current_material_stamps, current_cluster_stamp)

    # 13. Rebuild full memory bridge (canonical clusters + wiki paths into SQLite).
    # No-op if search.sqlite is absent (server always has it; collaborators rebuild via index ensure).
    from arquimedes.memory import memory_rebuild
    try:
        memory_rebuild(config)
    except FileNotFoundError:
        pass  # index or cluster file absent — safe to skip

    return {
        "material_pages": mat_pages_written,
        "material_pages_skipped": mat_pages_skipped,
        "concept_pages": concept_pages_written,
        "index_pages": index_pages_written,
        "orphans_removed": len(orphans),
        "clustering": cluster_summary,
    }


# ---------------------------------------------------------------------------
# Index page rendering
# ---------------------------------------------------------------------------

def _render_index_pages(
    wiki_root: Path,
    all_metas: dict[str, dict],
    clusters: list[dict],
    material_clusters: dict[str, list[dict]],
) -> int:
    """Render master, domain, collection, and concept index pages. Returns count."""
    written = 0

    # Build domain/collection tree
    tree: dict[str, dict[str, list[dict]]] = {}
    for mid, meta in all_metas.items():
        domain = (meta.get("domain") or "practice").strip() or "practice"
        collection = (meta.get("collection") or "").strip() or "_general"
        tree.setdefault(domain, {}).setdefault(collection, []).append(meta)

    all_material_entries = []
    for domain, collections in tree.items():
        domain_entries = []
        for collection, metas in collections.items():
            coll_entries = []
            for meta in metas:
                mid = meta["material_id"]
                mat_path = compile_pages._material_wiki_path(meta)
                # Relative path from the collection index
                coll_index = f"wiki/{domain}/{collection}/_index.md"
                rel = compile_pages._relative_link(coll_index, mat_path)
                entry = {
                    "name": meta.get("title") or mid,
                    "path": rel,
                    "summary": compile_pages._meta_val(meta.get("summary") or "")[:120],
                }
                coll_entries.append(entry)
                domain_entries.append(entry)
                all_material_entries.append(entry)
            content = compile_pages.render_index_page(
                f"{domain.title()} / {collection.title()}", coll_entries
            )
            _write_page(wiki_root / domain / collection / "_index.md", content)
            written += 1

        domain_content = compile_pages.render_index_page(domain.title(), domain_entries)
        _write_page(wiki_root / domain / "_index.md", domain_content)
        written += 1

    # Concept index
    concept_entries = []
    for c in sorted(clusters, key=lambda x: x.get("canonical_name", "").lower()):
        slug = c["slug"]
        rel_path = f"wiki/shared/concepts/_index.md"
        dest = f"wiki/shared/concepts/{slug}.md"
        rel = compile_pages._relative_link(rel_path, dest)
        mids = c.get("material_ids", [])
        n = len(mids)
        concept_entries.append({
            "name": c["canonical_name"],
            "path": rel,
            "summary": f"{n} material{'s' if n != 1 else ''}",
        })
    _write_page(
        wiki_root / "shared" / "concepts" / "_index.md",
        compile_pages.render_index_page("All Concepts", concept_entries),
    )
    written += 1

    # Master index
    master_entries = []
    for e in all_material_entries:
        master_entries.append(e)
    master_content = compile_pages.render_index_page(
        "Arquimedes Wiki",
        [{"name": "Materials", "path": "practice/_index.md", "summary": f"{len(all_metas)} materials"},
         {"name": "Concepts", "path": "shared/concepts/_index.md", "summary": f"{len(clusters)} concepts"}],
    )
    _write_page(wiki_root / "_index.md", master_content)
    written += 1

    return written
