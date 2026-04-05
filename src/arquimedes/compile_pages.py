"""Wiki page renderers — Phase 5.

Pure functions: each takes data, returns a markdown string.
No I/O, no side effects. No LLM calls.
"""

from __future__ import annotations

import os
import re
from pathlib import Path, PurePosixPath


# ---------------------------------------------------------------------------
# Enriched field unwrapper
# ---------------------------------------------------------------------------

def _meta_val(field) -> str:
    """Extract plain value from an enriched field (may be {value, provenance} or plain str)."""
    if isinstance(field, dict):
        return str(field.get("value") or "")
    return str(field) if field is not None else ""


def _meta_val_list(field) -> list:
    """Extract plain list value from an enriched field (may be {value, provenance} or plain list)."""
    if isinstance(field, dict):
        val = field.get("value")
        return val if isinstance(val, list) else ([val] if val else [])
    return field if isinstance(field, list) else []


def _clean_quoted_text(text: str) -> str:
    """Remove PDF extraction artifacts from highlighted text.

    PDF text layers often insert stray single characters (ligature components,
    column separators, hyphenation artifacts) as isolated lines. Strip them.
    """
    cleaned = " ".join(
        part.strip()
        for part in text.splitlines()
        if len(part.strip()) > 2
    )
    return re.sub(r" {2,}", " ", cleaned).strip()


def _chicago_citation(meta: dict) -> str:
    """Build a Chicago 17th-edition citation string from meta.

    Uses the enriched 'bibliography' field for publication details; falls back
    gracefully to whatever is available. Returns an empty string if insufficient
    data.

    Formats supported:
      - Journal article:    Author(s). "Title." Journal Volume, no. Issue (Year): pages. DOI.
      - Book chapter:       Author(s). "Chapter." In Book Title, edited by Editor(s), pages. Place: Publisher, Year.
      - Book / monograph:   Author(s). Title. Edition. Place: Publisher, Year.
      - Fallback:           Author(s). Title. Year.
    """
    bib = meta.get("bibliography") or {}
    title = meta.get("title") or ""
    year = meta.get("year") or ""
    authors: list = meta.get("authors") or []
    if isinstance(authors, str):
        authors = [authors]

    # --- Author string (last, first for first author; rest are first last) ---
    def _fmt_authors(names: list) -> str:
        if not names:
            return ""
        parts = []
        for i, name in enumerate(names):
            name = name.strip()
            if not name:
                continue
            if i == 0 and "," not in name and " " in name:
                # "First Last" → "Last, First"
                tokens = name.rsplit(" ", 1)
                name = f"{tokens[-1]}, {tokens[0]}"
            parts.append(name)
        if len(parts) == 1:
            return parts[0] + "."
        return ", ".join(parts[:-1]) + ", and " + parts[-1] + "."

    author_str = _fmt_authors(authors)

    journal = bib.get("journal_name", "")
    volume = str(bib.get("volume", "") or "")
    issue = str(bib.get("issue", "") or "")
    start_pg = str(bib.get("start_page", "") or "")
    end_pg = str(bib.get("end_page", "") or "")
    doi = bib.get("doi", "")
    book_title = bib.get("book_title", "")
    editors: list = bib.get("editors") or []
    if isinstance(editors, str):
        editors = [editors]
    publisher = bib.get("publisher", "")
    place = bib.get("place", "")
    edition = str(bib.get("edition", "") or "")

    page_range = f"{start_pg}–{end_pg}" if start_pg and end_pg else start_pg or end_pg or ""

    # Journal article
    if journal:
        vol_issue = volume
        if issue:
            vol_issue += f", no. {issue}"
        year_pages = f"({year})" if year else ""
        if page_range:
            year_pages += f": {page_range}"
        parts = []
        if author_str:
            parts.append(author_str)
        parts.append(f'"{title}."')
        if vol_issue:
            parts.append(f"_{journal}_ {vol_issue} {year_pages}.")
        else:
            parts.append(f"_{journal}_ {year_pages}.")
        if doi:
            parts.append(f"https://doi.org/{doi.lstrip('https://doi.org/')}")
        return " ".join(parts)

    # Book chapter (has book_title but is not a monograph itself)
    doc_type = _meta_val(meta.get("document_type")) or meta.get("raw_document_type") or ""
    if book_title and doc_type not in ("monograph", "book"):
        ed_str = ""
        if editors:
            ed_str = "edited by " + ", ".join(editors)
        parts = []
        if author_str:
            parts.append(author_str)
        parts.append(f'"{title}."')
        in_part = f"In _{book_title}_"
        if ed_str:
            in_part += f", {ed_str}"
        if page_range:
            in_part += f", {page_range}"
        in_part += "."
        parts.append(in_part)
        pub_parts = []
        if place:
            pub_parts.append(place)
        if publisher:
            pub_parts.append(publisher)
        if year:
            pub_parts.append(year)
        if pub_parts:
            parts.append(": ".join(pub_parts[:2]) + (f", {pub_parts[2]}" if len(pub_parts) > 2 else "") + ".")
        return " ".join(parts)

    # Monograph / book
    if publisher or place:
        parts = []
        if author_str:
            parts.append(author_str)
        parts.append(f"_{title}_.")
        if edition and edition not in ("1", "1st"):
            parts.append(f"{edition} ed.")
        pub_parts = []
        if place:
            pub_parts.append(place)
        if publisher:
            pub_parts.append(publisher)
        if year:
            pub_parts.append(year)
        if pub_parts:
            parts.append(": ".join(pub_parts[:2]) + (f", {pub_parts[2]}" if len(pub_parts) > 2 else "") + ".")
        return " ".join(parts)

    # Fallback: author, title, year
    if not (author_str or title or year):
        return ""
    parts = []
    if author_str:
        parts.append(author_str)
    if title:
        parts.append(f'"{title}."' if doc_type == "paper" else f"_{title}_.")
    if year:
        parts.append(year + ".")
    return " ".join(parts)


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def _material_wiki_path(meta: dict) -> str:
    """Return wiki-relative path: wiki/{domain}/{collection}/{material_id}.md"""
    domain = (meta.get("domain") or "practice").strip() or "practice"
    collection = (meta.get("collection") or "").strip()
    folder = collection if collection else "_general"
    mid = meta["material_id"]
    return f"wiki/{domain}/{folder}/{mid}.md"


def _concept_wiki_path(slug: str) -> str:
    """Return wiki-relative path: wiki/shared/concepts/{slug}.md"""
    return f"wiki/shared/concepts/{slug}.md"


def _relative_link(from_path: str, to_path: str) -> str:
    """Compute a relative path between two wiki-relative paths."""
    from_dir = str(PurePosixPath(from_path).parent)
    rel = os.path.relpath(to_path, from_dir)
    return rel


def _render_reflection_section(title: str, reflection: dict | None) -> list[str]:
    lines: list[str] = []
    if not reflection:
        return lines
    lines.append(f"## {title}\n")
    takeaways = reflection.get("main_takeaways") or []
    tensions = reflection.get("main_tensions") or []
    questions = reflection.get("open_questions") or []
    prose = reflection.get("why_this_concept_matters") or ""
    if prose:
        lines.append(prose)
        lines.append("")
    if takeaways:
        lines.append("**Main takeaways**")
        for item in takeaways[:8]:
            if str(item).strip():
                lines.append(f"- {item}")
        lines.append("")
    if tensions:
        lines.append("**Main tensions**")
        for item in tensions[:8]:
            if str(item).strip():
                lines.append(f"- {item}")
        lines.append("")
    if questions:
        lines.append("**Open questions**")
        for item in questions[:8]:
            if str(item).strip():
                lines.append(f"- {item}")
        lines.append("")
    return lines


# ---------------------------------------------------------------------------
# Material page
# ---------------------------------------------------------------------------

def render_material_page(
    meta: dict,
    clusters: list[dict],
    chunks: list[dict],
    annotations: list[dict],
    figures: list[dict],
    related: list[dict],
    *,
    raw_file_link: str | None = None,
    extracted_text_link: str | None = None,
) -> str:
    """Render a material wiki page as markdown.

    Args:
        meta: material meta.json dict (fully enriched)
        clusters: all clusters that include this material_id
        chunks: list of chunk dicts from chunks.jsonl
        annotations: list of annotation dicts from annotations.jsonl
        figures: list of figure dicts from figures/*.json
        related: pre-computed list of {material_id, title, reasons: list[str]}
        raw_file_link: file:// URL or path to the original source PDF
        extracted_text_link: relative link to extracted/text.md for LLM deep-dive
    """
    mid = meta["material_id"]
    page_path = _material_wiki_path(meta)
    lines: list[str] = []

    # --- Title ---
    title = meta.get("title") or mid
    lines.append(f"# {title}\n")

    # --- Metadata block ---
    lines.append("## Metadata\n")
    lines.append("| Field | Value |")
    lines.append("| --- | --- |")
    authors = meta.get("authors") or []
    if isinstance(authors, list):
        author_str = ", ".join(authors)
    else:
        author_str = str(authors)
    if author_str:
        lines.append(f"| Authors | {author_str} |")
    year = meta.get("year") or ""
    if year:
        lines.append(f"| Year | {year} |")
    doc_type = _meta_val(meta.get("document_type")) or meta.get("raw_document_type") or ""
    if doc_type:
        lines.append(f"| Type | {doc_type} |")
    domain = meta.get("domain") or ""
    if domain:
        lines.append(f"| Domain | {domain} |")
    collection = meta.get("collection") or ""
    if collection:
        lines.append(f"| Collection | {collection} |")
    page_count = meta.get("page_count") or 0
    if page_count:
        lines.append(f"| Pages | {page_count} |")
    lines.append("")

    # --- Summary ---
    summary = _meta_val(meta.get("summary"))
    if summary:
        lines.append("## Summary\n")
        lines.append(summary)
        lines.append("")

    # --- Key concepts ---
    if clusters:
        lines.append("## Key Concepts\n")
        for c in sorted(clusters, key=lambda x: x.get("canonical_name", "")):
            cname = c["canonical_name"]
            slug = c["slug"]
            concept_path = c.get("wiki_path") or _concept_wiki_path(slug)
            rel = _relative_link(page_path, concept_path)
            link_label = cname
            if "/bridge-concepts/" in concept_path:
                link_label += " (bridge)"
            lines.append(f"- [{link_label}]({rel})")
        lines.append("")

    # --- Facets ---
    # Facets may be nested under meta["facets"][key] as {value, provenance} objects,
    # or directly on meta[key] as plain strings (synthetic/test fixtures).
    facet_fields = [
        ("building_type", "Building type"),
        ("scale", "Scale"),
        ("location", "Location"),
        ("jurisdiction", "Jurisdiction"),
        ("climate", "Climate"),
        ("program", "Program"),
        ("material_system", "Material system"),
        ("structural_system", "Structural system"),
        ("historical_period", "Historical period"),
        ("course_topic", "Course topic"),
        ("studio_project", "Studio project"),
    ]
    nested_facets = meta.get("facets") or {}
    facets = []
    for key, label in facet_fields:
        # Prefer nested facets dict, fall back to top-level key
        raw = nested_facets.get(key) or meta.get(key) or ""
        val = _meta_val(raw)
        facets.append((label, val))
    nonempty_facets = [(label, val) for label, val in facets if val.strip()]
    if nonempty_facets:
        lines.append("## Architecture Facets\n")
        for label, val in nonempty_facets:
            lines.append(f"**{label}:** {val}  ")
        lines.append("")

    # --- Figures ---
    substantive_figs = [
        f for f in figures
        if (_meta_val(f.get("description")) or _meta_val(f.get("caption")))
        and _meta_val(f.get("visual_type")) != "decorative"
    ]
    if substantive_figs:
        lines.append("## Figures\n")
        for fig in substantive_figs:
            fig_id = fig.get("figure_id", "")
            visual_type = _meta_val(fig.get("visual_type")) or ""
            caption = _meta_val(fig.get("caption")) or ""
            description = _meta_val(fig.get("description")) or ""
            image_path = fig.get("image_path") or ""
            page = fig.get("source_page") or ""
            heading = f"**{fig_id}**"
            if visual_type:
                heading += f" _{visual_type}_"
            if page:
                heading += f" (p. {page})"
            lines.append(heading)
            if image_path:
                lines.append(f"![{fig_id}]({image_path})")
            if caption:
                lines.append(f"> {caption}")
            if description:
                lines.append(description)
            lines.append("")

    # --- Annotations ---
    if annotations:
        lines.append("## Reader Annotations\n")
        for ann in annotations:
            quoted = _clean_quoted_text(ann.get("quoted_text") or "")
            comment = ann.get("comment") or ""
            page = ann.get("page") or ""
            page_suffix = f" _(p. {page})_" if page else ""
            if quoted:
                lines.append(f'> "{quoted}"{page_suffix}')
            if comment:
                lines.append(f"> **Note:** {comment}")
            lines.append("")

    # --- Related materials ---
    if related:
        lines.append("## Related Materials\n")
        for r in related:
            rtitle = r.get("title") or r["material_id"]
            reasons = r.get("reasons") or []
            reason_str = f" — {'; '.join(reasons)}" if reasons else ""
            lines.append(f"- {rtitle}{reason_str}")
        lines.append("")

    # --- Source ---
    lines.append("## Source\n")
    if page_count:
        lines.append(f"**Pages:** {page_count}  ")
    citation = _chicago_citation(meta)
    if citation:
        lines.append(f"**Citation:** {citation}")
    lines.append("")
    if raw_file_link:
        lines.append(f"[Open original file]({raw_file_link})  ")
    if extracted_text_link:
        lines.append(f"[Full extracted text]({extracted_text_link})  ")
    lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Concept page
# ---------------------------------------------------------------------------

def render_concept_page(
    cluster: dict,
    material_titles: dict[str, str],
    related_concepts: list[dict],
    material_paths: dict[str, str] | None = None,
    reflection: dict | None = None,
) -> str:
    """Render a concept wiki page as markdown.

    Args:
        cluster: cluster dict from concept_clusters.jsonl
        material_titles: mapping of material_id → title
        related_concepts: pre-computed list of {canonical_name, slug}
        material_paths: mapping of material_id → wiki-relative path (e.g. wiki/research/_general/abc.md)
    """
    canonical_name = cluster["canonical_name"]
    slug = cluster["slug"]
    page_path = cluster.get("wiki_path") or _concept_wiki_path(slug)
    is_bridge_page = "/bridge-concepts/" in page_path
    source_concepts = cluster.get("source_concepts", [])
    aliases = [a for a in cluster.get("aliases", []) if a != canonical_name]
    lines: list[str] = []

    # --- Title ---
    lines.append(f"# {canonical_name}\n")

    if "/bridge-concepts/" in page_path:
        lines.append("_Bridge cluster_\n")

    # --- Aliases ---
    if aliases:
        lines.append(f"_Also known as: {', '.join(aliases)}_\n")

    # --- Overview ---
    n_materials = len(dict.fromkeys(sc["material_id"] for sc in source_concepts))
    lines.append(f"This concept appears in {n_materials} material{'s' if n_materials != 1 else ''}.\n")

    # --- By material ---
    if source_concepts:
        lines.append("## By Material\n")
        for sc in source_concepts:
            mid = sc["material_id"]
            title = material_titles.get(mid, mid)
            relevance = sc.get("relevance") or ""
            source_pages = sc.get("source_pages") or []
            evidence_spans = sc.get("evidence_spans") or []

            # Link back to material page (if path is known)
            page_from = page_path
            if material_paths and mid in material_paths:
                mat_rel = _relative_link(page_from, material_paths[mid])
                mat_heading = f"[{title}]({mat_rel})"
            else:
                mat_heading = title

            page_refs = f" (p. {', '.join(str(p) for p in source_pages)})" if source_pages else ""
            rel_str = f" · _{relevance}_" if relevance else ""
            lines.append(f"### {mat_heading}{rel_str}{page_refs}\n")

            for span in evidence_spans[:3]:  # up to 3 spans
                lines.append(f'> "{span}"')
            lines.append("")

    # --- Related concepts ---
    if related_concepts:
        lines.append("## Related Concepts\n")
        for rc in related_concepts:
            rc_name = rc["canonical_name"]
            rc_slug = rc["slug"]
            rc_path = rc.get("wiki_path") or (
                f"wiki/shared/bridge-concepts/{rc_slug}.md" if is_bridge_page else _concept_wiki_path(rc_slug)
            )
            rel = _relative_link(page_path, rc_path)
            link_label = rc_name
            if "/bridge-concepts/" in rc_path:
                link_label += " (bridge)"
            lines.append(f"- [{link_label}]({rel})")
        lines.append("")

    lines.extend(_render_reflection_section("Phase 6 Reflection", reflection))

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Collection page
# ---------------------------------------------------------------------------

def render_collection_page(
    title: str,
    domain: str,
    collection: str,
    materials: list[dict],
    key_concepts: list[dict],
    top_facets: list[dict],
    recent_additions: list[dict],
    reflection: dict | None = None,
) -> str:
    """Render a collection _index.md page.

    Args:
        title: friendly collection title (H1)
        domain: domain slug
        collection: collection slug
        materials: list of {name, path, summary}
        key_concepts: list of {name, path, count}
        top_facets: list of {field, value, count}
        recent_additions: list of {name, path, ingested_at}
    """
    lines: list[str] = []
    lines.append(f"# {title}\n")

    # Overview
    lines.append("## Overview\n")
    lines.append(f"- **Domain:** {domain}")
    lines.append(f"- **Collection:** {collection}")
    lines.append(f"- **Materials:** {len(materials)}")
    lines.append("")

    # Recent additions
    if recent_additions:
        lines.append("## Recent Additions\n")
        for r in recent_additions[:5]:
            name = r.get("name", "")
            path = r.get("path", "")
            ts = r.get("ingested_at", "")
            date_str = ts[:10] if ts else ""
            link = f"[{name}]({path})" if path else name
            if date_str:
                lines.append(f"- {link} ({date_str})")
            else:
                lines.append(f"- {link}")
        lines.append("")

    # Materials
    if materials:
        lines.append("## Materials\n")
        sorted_mats = sorted(materials, key=lambda e: e.get("name", "").lower())
        for e in sorted_mats:
            name = e.get("name", "")
            path = e.get("path", "")
            summary = e.get("summary", "")
            link = f"[{name}]({path})" if path else name
            if summary:
                lines.append(f"- {link} — {summary}")
            else:
                lines.append(f"- {link}")
        lines.append("")

    # Key concepts
    if key_concepts:
        lines.append("## Key Concepts\n")
        for kc in key_concepts:
            name = kc.get("name", "")
            path = kc.get("path", "")
            count = kc.get("count", 0)
            link = f"[{name}]({path})" if path else name
            lines.append(f"- {link} ({count} material{'s' if count != 1 else ''})")
        lines.append("")

    # Top facets (grouped by field)
    if top_facets:
        lines.append("## Top Facets\n")
        # Group by field, preserving input order
        from collections import OrderedDict
        grouped: OrderedDict[str, list[dict]] = OrderedDict()
        for tf in top_facets:
            field = tf.get("field", "")
            grouped.setdefault(field, []).append(tf)
        for field, entries in grouped.items():
            heading = field.replace("_", " ").title()
            lines.append(f"### {heading}\n")
            for tf in entries:
                value = tf.get("value", "")
                count = tf.get("count", 0)
                lines.append(f"- {value} ({count})")
            lines.append("")

    lines.extend(_render_reflection_section("Phase 6 Reflection", reflection))

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Index page
# ---------------------------------------------------------------------------

def render_index_page(title: str, entries: list[dict]) -> str:
    """Render an index page.

    Args:
        title: page title (H1)
        entries: list of {name, path, summary}
    """
    lines: list[str] = []
    lines.append(f"# {title}\n")
    lines.append(f"_{len(entries)} page{'s' if len(entries) != 1 else ''}_\n")

    if entries:
        sorted_entries = sorted(entries, key=lambda e: e.get("name", "").lower())
        for e in sorted_entries:
            name = e.get("name", "")
            path = e.get("path", "")
            summary = e.get("summary", "")
            link = f"[{name}]({path})" if path else name
            if summary:
                lines.append(f"- {link} — {summary}")
            else:
                lines.append(f"- {link}")

    lines.append("")
    return "\n".join(lines)


def render_grouped_index_page(title: str, entries: list[dict], group_key: str, group_title: str) -> str:
    """Render an index page grouped by a field on each entry."""
    lines: list[str] = []
    lines.append(f"# {title}\n")
    lines.append(f"_{len(entries)} page{'s' if len(entries) != 1 else ''}_\n")

    groups: dict[str, list[dict]] = {}
    for e in entries:
        group = (e.get(group_key) or "").strip() or "_general"
        groups.setdefault(group, []).append(e)

    for group_name in sorted(groups.keys(), key=lambda s: s.lower()):
        heading = group_name.replace("_", " ").title()
        lines.append(f"## {heading}\n")
        for e in sorted(groups[group_name], key=lambda x: x.get("name", "").lower()):
            name = e.get("name", "")
            path = e.get("path", "")
            summary = e.get("summary", "")
            link = f"[{name}]({path})" if path else name
            if summary:
                lines.append(f"- {link} — {summary}")
            else:
                lines.append(f"- {link}")
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Glossary
# ---------------------------------------------------------------------------

def render_glossary(clusters: list[dict]) -> str:
    """Render alphabetical glossary of main concept names → concept pages."""
    lines: list[str] = []
    lines.append("# Main Concepts\n")
    lines.append("_Alphabetical index of canonical main concept names._\n")

    sorted_clusters = sorted(clusters, key=lambda c: c.get("canonical_name", "").lower())
    current_letter = ""
    for c in sorted_clusters:
        name = c.get("canonical_name", "")
        slug = c.get("slug", "")
        if not name or not slug:
            continue
        letter = name[0].upper()
        if letter != current_letter:
            current_letter = letter
            lines.append(f"\n### {letter}\n")
        path = c.get("wiki_path") or _concept_wiki_path(slug)
        link_label = name
        if "/bridge-concepts/" in path:
            link_label += " (main)"
        lines.append(f"- [{link_label}]({path})")

    lines.append("")
    return "\n".join(lines)
