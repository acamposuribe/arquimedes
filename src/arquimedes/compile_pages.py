"""Wiki page renderers — Phase 5.

Pure functions: each takes data, returns a markdown string.
No I/O, no side effects. No LLM calls.
"""

from __future__ import annotations

import os
import re
from pathlib import PurePosixPath
from urllib.parse import quote

from arquimedes.domain_profiles import generated_label, is_practice_domain


_PROJECT_MATERIAL_TYPE_LABELS: dict[str, str] = {
    "meeting_report": "Informes de reunión",
    "meeting_notes": "Notas de reunión",
    "client_request": "Solicitudes del cliente",
    "authority_request": "Requerimientos de la administración",
    "regulation": "Normativa",
    "drawing_set": "Planos",
    "technical_report": "Informes técnicos",
    "working_document": "Documentos de trabajo",
    "budget_table": "Mediciones y presupuestos",
    "site_photo": "Fotografías de obra",
    "map_or_cartography": "Cartografía",
    "contract_or_admin": "Contratos y administración",
    "email_or_message_export": "Correos y mensajes",
    "schedule": "Cronogramas",
    "unknown": "Sin clasificar",
}

_PROJECT_MATERIAL_TYPE_ORDER: dict[str, int] = {
    key: index for index, key in enumerate(_PROJECT_MATERIAL_TYPE_LABELS)
}


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


def _meta_lines(field) -> list[str]:
    items = _meta_val_list(field)
    if items:
        return [str(item).strip() for item in items if str(item).strip()]
    text = _meta_val(field)
    return [line.strip() for line in text.splitlines() if line.strip()]


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
    return quote(rel, safe="/.")


def _page_domain_from_path(page_path: str) -> str:
    parts = PurePosixPath(page_path).parts
    if len(parts) >= 2 and parts[0] == "wiki" and parts[1] in {"practice", "research", "proyectos"}:
        return parts[1]
    return ""


def _label(domain: str, key: str, default: str) -> str:
    return generated_label(key, domain, default=default)


def _material_count_text(domain: str, count: int) -> str:
    if is_practice_domain(domain, default="research"):
        return f"{count} material{'es' if count != 1 else ''}"
    return f"{count} material{'s' if count != 1 else ''}"


def _concept_appears_text(domain: str, count: int) -> str:
    if is_practice_domain(domain, default="research"):
        return f"Este concepto aparece en {_material_count_text(domain, count)}."
    return f"This concept appears in {count} material{'s' if count != 1 else ''}."


def _render_reflection_section(title: str, reflection: dict | None, *, domain: str = "") -> list[str]:
    lines: list[str] = []
    if not reflection:
        return lines
    lines.append(f"## {_label(domain, 'reflections', title)}\n")
    takeaways = reflection.get("main_takeaways") or []
    tensions = reflection.get("main_tensions") or []
    questions = reflection.get("open_questions") or []
    prose = reflection.get("why_this_concept_matters") or reflection.get("why_this_collection_matters") or ""
    if prose:
        lines.append(prose)
        lines.append("")
    if takeaways:
        lines.append(f"**{_label(domain, 'main_takeaways', 'Main takeaways')}**")
        for item in takeaways[:8]:
            if str(item).strip():
                lines.append(f"- {item}")
        lines.append("")
    if tensions:
        lines.append(f"**{_label(domain, 'main_tensions', 'Main tensions')}**")
        for item in tensions[:8]:
            if str(item).strip():
                lines.append(f"- {item}")
        lines.append("")
    if questions:
        lines.append(f"**{_label(domain, 'open_questions', 'Open questions')}**")
        for item in questions[:8]:
            if str(item).strip():
                lines.append(f"- {item}")
        lines.append("")
    helpful_new_sources = reflection.get("helpful_new_sources") or []
    if helpful_new_sources:
        lines.append(f"**{_label(domain, 'helpful_new_sources', 'Helpful new sources')}**")
        for item in helpful_new_sources[:8]:
            if str(item).strip():
                lines.append(f"- {item}")
        lines.append("")
    return lines


def _render_recent_changes_section(review_rows: list[dict] | None, *, domain: str = "") -> list[str]:
    lines: list[str] = []
    if not review_rows:
        return lines

    def _sort_key(row: dict) -> str:
        provenance = row.get("_provenance") if isinstance(row.get("_provenance"), dict) else {}
        return str(provenance.get("run_at", "")).strip()

    lines.append(f"## {_label(domain, 'recent_changes', 'Recent Changes')}\n")
    for row in sorted(review_rows, key=_sort_key, reverse=True):
        finding_type = str(row.get("finding_type", "")).strip() or "update"
        severity = str(row.get("severity", "")).strip()
        status = str(row.get("status", "")).strip()
        note = str(row.get("note", "")).strip()
        recommendation = str(row.get("recommendation", "")).strip()
        provenance = row.get("_provenance") if isinstance(row.get("_provenance"), dict) else {}
        run_at = str(provenance.get("run_at", "")).strip()

        title_bits = [finding_type.replace("_", " ").title()]
        if run_at:
            title_bits.append(run_at[:10])
        lines.append(f"### {' · '.join(title_bits)}\n")
        if status:
            lines.append(f"- {_label(domain, 'status', 'Status')}: {status}")
        if severity:
            lines.append(f"- {_label(domain, 'severity', 'Severity')}: {severity}")
        if note:
            lines.append(f"- {_label(domain, 'note', 'Note')}: {note}")
        if recommendation:
            lines.append(f"- {_label(domain, 'recommendation', 'Recommendation')}: {recommendation}")
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
    material_paths: dict[str, str] | None = None,
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
        material_paths: mapping of material_id → wiki-relative path
        raw_file_link: file:// URL or path to the original source PDF
        extracted_text_link: relative link to extracted/text.md for LLM deep-dive
    """
    mid = meta["material_id"]
    page_path = _material_wiki_path(meta)
    lines: list[str] = []

    # --- Title ---
    title = meta.get("title") or mid
    domain = meta.get("domain") or ""
    is_project_material = domain == "proyectos"
    lines.append(f"# {title}\n")

    # --- Metadata block ---
    authors = meta.get("authors") or []
    if isinstance(authors, list):
        author_str = ", ".join(authors)
    else:
        author_str = str(authors)
    year = meta.get("year") or ""
    doc_type = _meta_val(meta.get("document_type")) or meta.get("raw_document_type") or ""
    collection = meta.get("collection") or ""
    page_count = meta.get("page_count") or 0
    if not is_project_material:
        lines.append(f"## {_label(domain, 'metadata', 'Metadata')}\n")
        lines.append(f"| {_label(domain, 'field', 'Field')} | {_label(domain, 'value', 'Value')} |")
        lines.append("| --- | --- |")
        if author_str:
            lines.append(f"| {_label(domain, 'authors', 'Authors')} | {author_str} |")
        if year:
            lines.append(f"| {_label(domain, 'year', 'Year')} | {year} |")
        if doc_type:
            lines.append(f"| {_label(domain, 'type', 'Type')} | {doc_type} |")
        if domain:
            lines.append(f"| {_label(domain, 'domain', 'Domain')} | {domain} |")
        if collection:
            lines.append(f"| {_label(domain, 'collection', 'Collection')} | {collection} |")
        if page_count:
            lines.append(f"| {_label(domain, 'pages', 'Pages')} | {page_count} |")
        lines.append("")

    # --- Summary ---
    summary = _meta_val(meta.get("summary"))
    if summary and not is_project_material:
        lines.append(f"## {_label(domain, 'summary', 'Summary')}\n")
        lines.append(summary)
        lines.append("")

    methodological_conclusions = _meta_lines(meta.get("methodological_conclusions"))
    main_content_learnings = _meta_lines(meta.get("main_content_learnings"))
    if methodological_conclusions or main_content_learnings:
        lines.append(f"## {_label(domain, 'material_conclusions', 'Material Conclusions')}\n")
        if methodological_conclusions:
            lines.append(f"**{_label(domain, 'methodological_conclusions', 'Methodological conclusions')}**")
            for item in methodological_conclusions:
                lines.append(f"- {item}")
            lines.append("")
        if main_content_learnings:
            lines.append(f"**{_label(domain, 'main_content_learnings', 'Main content learnings')}**")
            for item in main_content_learnings:
                lines.append(f"- {item}")
            lines.append("")

    # --- Key concepts ---
    if clusters:
        lines.append(f"## {_label(domain, 'key_concepts', 'Key Concepts')}\n")
        for c in sorted(clusters, key=lambda x: x.get("canonical_name", "")):
            cname = c["canonical_name"]
            slug = c["slug"]
            concept_path = c.get("wiki_path") or _concept_wiki_path(slug)
            rel = _relative_link(page_path, concept_path)
            link_label = cname
            if "/bridge-concepts/" in concept_path:
                link_label += f" ({_label(domain, 'bridge_suffix', 'bridge')})"
            lines.append(f"- [{link_label}]({rel})")
        lines.append("")

    # --- Facets ---
    # Facets may be nested under meta["facets"][key] as {value, provenance} objects,
    # or directly on meta[key] as plain strings (synthetic/test fixtures).
    facet_fields = [
        ("building_type", _label(domain, "building_type", "Building type")),
        ("scale", _label(domain, "scale", "Scale")),
        ("location", _label(domain, "location", "Location")),
        ("jurisdiction", _label(domain, "jurisdiction", "Jurisdiction")),
        ("climate", _label(domain, "climate", "Climate")),
        ("program", _label(domain, "program", "Program")),
        ("material_system", _label(domain, "material_system", "Material system")),
        ("structural_system", _label(domain, "structural_system", "Structural system")),
        ("historical_period", _label(domain, "historical_period", "Historical period")),
        ("course_topic", _label(domain, "course_topic", "Course topic")),
        ("studio_project", _label(domain, "studio_project", "Studio project")),
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
        lines.append(f"## {_label(domain, 'architecture_facets', 'Architecture Facets')}\n")
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
        lines.append(f"## {_label(domain, 'figures', 'Figures')}\n")
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
        lines.append(f"## {_label(domain, 'reader_annotations', 'Reader Annotations')}\n")
        label = _label(domain, "reader_annotations", "Reader Annotations")
        count_label = "annotation" if len(annotations) == 1 else "annotations"
        if domain == "practice":
            count_label = "anotacion" if len(annotations) == 1 else "anotaciones"
        lines.append(f'<details class="reader-annotations"><summary>{label} ({len(annotations)} {count_label})</summary>\n')
        for ann in annotations:
            quoted = _clean_quoted_text(ann.get("quoted_text") or "")
            comment = ann.get("comment") or ""
            page = ann.get("page") or ""
            page_suffix = f" _(p. {page})_" if page else ""
            if quoted:
                lines.append(f'> "{quoted}"{page_suffix}')
            if comment:
                lines.append(f"> **{_label(domain, 'note', 'Note')}:** {comment}")
            lines.append("")
        lines.append("</details>\n")

    # --- Source ---
    if not is_project_material:
        lines.append(f"## {_label(domain, 'source', 'Source')}\n")
        if page_count:
            lines.append(f"**{_label(domain, 'pages', 'Pages')}:** {page_count}  ")
        citation = _chicago_citation(meta)
        if citation:
            lines.append(f"**{_label(domain, 'citation', 'Citation')}:** {citation}")
        lines.append("")
        if raw_file_link:
            lines.append(f"[{_label(domain, 'open_original_file', 'Open original file')}]({raw_file_link})  ")
        if extracted_text_link:
            lines.append(f"[{_label(domain, 'full_extracted_text', 'Full extracted text')}]({extracted_text_link})  ")
        lines.append("")

    # --- Related materials ---
    if related:
        lines.append(f"## {_label(domain, 'related_materials', 'Related Materials')}\n")
        for r in related:
            rtitle = r.get("title") or r["material_id"]
            reasons = r.get("reasons") or []
            reason_str = f" — {'; '.join(reasons)}" if reasons else ""
            rel_path = (material_paths or {}).get(r["material_id"], "")
            if rel_path:
                lines.append(f"- [{rtitle}]({_relative_link(page_path, rel_path)}){reason_str}")
            else:
                lines.append(f"- {rtitle}{reason_str}")
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
    review_rows: list[dict] | None = None,
    bridge_memberships: list[dict] | None = None,
) -> str:
    """Render a concept wiki page as markdown.

    Args:
        cluster: cluster dict from a local/global cluster artifact
        material_titles: mapping of material_id → title
        related_concepts: pre-computed list of {canonical_name, slug}
        material_paths: mapping of material_id → wiki-relative path (e.g. wiki/research/_general/abc.md)
    """
    canonical_name = cluster["canonical_name"]
    slug = cluster["slug"]
    page_path = cluster.get("wiki_path") or _concept_wiki_path(slug)
    page_domain = str(cluster.get("domain", "")).strip() or _page_domain_from_path(page_path)
    is_bridge_page = "/bridge-concepts/" in page_path
    source_concepts = cluster.get("source_concepts", [])
    member_local_clusters = cluster.get("member_local_clusters", [])
    aliases = [a for a in cluster.get("aliases", []) if a != canonical_name]
    lines: list[str] = []

    # --- Title ---
    lines.append(f"# {canonical_name}\n")

    if "/bridge-concepts/" in page_path:
        lines.append(f"_{_label(page_domain, 'bridge_cluster', 'Bridge cluster')}_\n")

    descriptor = str(cluster.get("descriptor", "")).strip()
    # --- Aliases ---
    if aliases:
        lines.append(f"_{_label(page_domain, 'also_known_as', 'Also known as')}: {', '.join(aliases)}_\n")

    if descriptor:
        lines.append(f"{descriptor}\n")

    # --- Overview ---
    if member_local_clusters:
        n_materials = len({mid for member in member_local_clusters for mid in member.get("material_ids", [])})
    else:
        n_materials = len(dict.fromkeys(sc["material_id"] for sc in source_concepts))
    lines.append(f"{_concept_appears_text(page_domain, n_materials)}\n")

    lines.extend(_render_reflection_section("Reflections", reflection, domain=page_domain))

    if is_bridge_page:
        bridge_takeaways = [str(item).strip() for item in cluster.get("bridge_takeaways", []) if str(item).strip()]
        bridge_tensions = [str(item).strip() for item in cluster.get("bridge_tensions", []) if str(item).strip()]
        bridge_questions = [str(item).strip() for item in cluster.get("bridge_open_questions", []) if str(item).strip()]
        bridge_sources = [str(item).strip() for item in cluster.get("helpful_new_sources", []) if str(item).strip()]
        why_this_bridge_matters = str(cluster.get("why_this_bridge_matters", "")).strip()
        supporting_collection_reflections = [
            row
            for row in cluster.get("supporting_collection_reflections", [])
            if isinstance(row, dict)
        ]
        if bridge_takeaways or bridge_tensions or bridge_questions or bridge_sources or why_this_bridge_matters or supporting_collection_reflections:
            lines.append(f"## {_label(page_domain, 'cross_collection_synthesis', 'Cross-Collection Synthesis')}\n")
            if why_this_bridge_matters:
                lines.append(f"### {_label(page_domain, 'why_this_bridge_matters', 'Why This Bridge Matters')}\n")
                lines.append(why_this_bridge_matters)
                lines.append("")
            if bridge_takeaways:
                lines.append(f"### {_label(page_domain, 'shared_takeaways', 'Shared Takeaways')}\n")
                for item in bridge_takeaways:
                    lines.append(f"- {item}")
                lines.append("")
            if bridge_tensions:
                lines.append(f"### {_label(page_domain, 'shared_tensions', 'Shared Tensions')}\n")
                for item in bridge_tensions:
                    lines.append(f"- {item}")
                lines.append("")
            if bridge_questions:
                lines.append(f"### {_label(page_domain, 'open_questions', 'Open Questions')}\n")
                for item in bridge_questions:
                    lines.append(f"- {item}")
                lines.append("")
            if bridge_sources:
                lines.append(f"### {_label(page_domain, 'helpful_new_sources', 'Helpful New Sources')}\n")
                for item in bridge_sources[:8]:
                    lines.append(f"- {item}")
                lines.append("")
            collection_signals = []
            for row in supporting_collection_reflections:
                collection_key = str(row.get("collection_key", "")).strip()
                why = str(row.get("why_this_collection_matters", "")).strip()
                if collection_key and why:
                    collection_signals.append(f"{collection_key} — {why}")
            if collection_signals:
                lines.append(f"### {_label(page_domain, 'collection_signals', 'Collection Signals')}\n")
                for item in collection_signals:
                    lines.append(f"- {item}")
                lines.append("")

    if bridge_memberships and not is_bridge_page:
        lines.append(f"## {_label(page_domain, 'global_bridges', 'Global Bridges')}\n")
        for bridge in bridge_memberships:
            bridge_name = str(bridge.get("canonical_name", "")).strip()
            bridge_path = str(bridge.get("wiki_path", "")).strip()
            descriptor = str(bridge.get("descriptor", "")).strip()
            supporting_material_ids = [
                str(mid).strip()
                for mid in bridge.get("supporting_material_ids", [])
                if str(mid).strip()
            ]
            link = bridge_name
            if bridge_path:
                link = f"[{bridge_name}]({_relative_link(page_path, bridge_path)})"
            summary_bits = []
            if supporting_material_ids:
                summary_bits.append(_material_count_text(page_domain, len(supporting_material_ids)))
            if bridge.get("confidence") not in (None, ""):
                try:
                    summary_bits.append(f"confidence {float(bridge.get('confidence', 0.0) or 0.0):.2f}")
                except (TypeError, ValueError):
                    pass
            summary = f" ({'; '.join(summary_bits)})" if summary_bits else ""
            if descriptor:
                lines.append(f"- {link}{summary} — {descriptor}")
            else:
                lines.append(f"- {link}{summary}")
        lines.append("")

    if member_local_clusters:
        lines.append(f"## {_label(page_domain, 'contributing_local_clusters', 'Contributing Local Clusters')}\n")
        for member in member_local_clusters:
            cluster_name = str(member.get("canonical_name", "")).strip() or str(member.get("cluster_id", "")).strip()
            cluster_path = str(member.get("wiki_path", "")).strip()
            domain = str(member.get("domain", "")).strip()
            collection = str(member.get("collection", "")).strip()
            material_ids = [str(mid).strip() for mid in member.get("material_ids", []) if str(mid).strip()]
            scope_bits = []
            if domain and collection:
                scope_bits.append(f"{domain}/{collection}")
            if material_ids:
                scope_bits.append(_material_count_text(page_domain, len(material_ids)))
            scope_suffix = f" ({' / '.join(scope_bits)})" if scope_bits else ""
            lines.append(f"### {cluster_name}{scope_suffix}\n")
            if cluster_path:
                lines.append(
                    f"- {_label(page_domain, 'local_cluster', 'Local cluster')}: "
                    f"[{cluster_name}]({_relative_link(page_path, cluster_path)})"
                )

            descriptor = str(member.get("descriptor", "")).strip()
            if descriptor:
                lines.append(descriptor)
                lines.append("")

            promotion_reasons = [str(reason).strip().replace("_", " ") for reason in member.get("promotion_reasons", []) if str(reason).strip()]
            if promotion_reasons:
                lines.append(f"- {_label(page_domain, 'promotion', 'Promotion')}: {', '.join(promotion_reasons)}")
            if material_ids:
                for mid in material_ids:
                    title = material_titles.get(mid, mid)
                    if material_paths and mid in material_paths:
                        lines.append(f"- [{title}]({_relative_link(page_path, material_paths[mid])})")
                    else:
                        lines.append(f"- {title}")
            lines.append("")

    # --- By material ---
    if source_concepts and not member_local_clusters:
        lines.append(f"## {_label(page_domain, 'by_material', 'By Material')}\n")
        for sc in source_concepts:
            mid = sc["material_id"]
            title = material_titles.get(mid, mid)
            relevance = sc.get("relevance") or ""
            source_pages = sc.get("source_pages") or []
            evidence_spans = sc.get("evidence_spans") or []
            descriptor = sc.get("descriptor") or ""

            # Link back to material page (if path is known)
            page_from = page_path
            if material_paths and mid in material_paths:
                mat_rel = _relative_link(page_from, material_paths[mid])
                mat_heading = f"[{title}]({mat_rel})"
            else:
                mat_heading = title

            page_refs = f" (p. {', '.join(str(p) for p in source_pages)})" if source_pages else ""
            rel_str = f" · _{relevance}_" if relevance else ""
            desc_str = f" · {descriptor}" if descriptor else ""
            lines.append(f"### {mat_heading}{rel_str}{desc_str}{page_refs}\n")

            for span in evidence_spans[:3]:  # up to 3 spans
                lines.append(f'> "{span}"')
            lines.append("")

    # --- Related concepts ---
    if related_concepts:
        lines.append(f"## {_label(page_domain, 'related_concepts', 'Related Concepts')}\n")
        for rc in related_concepts:
            rc_name = rc["canonical_name"]
            rc_slug = rc["slug"]
            rc_path = rc.get("wiki_path") or (
                f"wiki/shared/bridge-concepts/{rc_slug}.md" if is_bridge_page else _concept_wiki_path(rc_slug)
            )
            rel = _relative_link(page_path, rc_path)
            link_label = rc_name
            if "/bridge-concepts/" in rc_path:
                link_label += f" ({_label(page_domain, 'bridge_suffix', 'bridge')})"
            lines.append(f"- [{link_label}]({rel})")
        lines.append("")

    if is_bridge_page:
        lines.extend(_render_recent_changes_section(review_rows, domain=page_domain))

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
    lines.append(f"## {_label(domain, 'overview', 'Overview')}\n")
    lines.append(f"- **{_label(domain, 'domain', 'Domain')}:** {domain}")
    lines.append(f"- **{_label(domain, 'collection', 'Collection')}:** {collection}")
    lines.append(f"- **{_label(domain, 'materials', 'Materials')}:** {len(materials)}")
    lines.append("")

    lines.extend(_render_reflection_section("Reflections", reflection, domain=domain))

    # Recent additions
    if recent_additions:
        lines.append(f"## {_label(domain, 'recent_additions', 'Recent Additions')}\n")
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
        lines.append(f"## {_label(domain, 'materials', 'Materials')}\n")
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
        lines.append(f"## {_label(domain, 'key_concepts', 'Key Concepts')}\n")
        for kc in key_concepts:
            name = kc.get("name", "")
            path = kc.get("path", "")
            count = kc.get("count", 0)
            link = f"[{name}]({path})" if path else name
            lines.append(f"- {link} ({_material_count_text(domain, count)})")
        lines.append("")

    # Top facets (grouped by field)
    if top_facets:
        lines.append(f"## {_label(domain, 'top_facets', 'Top Facets')}\n")
        # Group by field, preserving input order
        from collections import OrderedDict
        grouped: OrderedDict[str, list[dict]] = OrderedDict()
        for tf in top_facets:
            field = tf.get("field", "")
            grouped.setdefault(field, []).append(tf)
        for field, entries in grouped.items():
            heading = _label(domain, field, field.replace("_", " ").title())
            lines.append(f"### {heading}\n")
            for tf in entries:
                value = tf.get("value", "")
                count = tf.get("count", 0)
                lines.append(f"- {value} ({count})")
            lines.append("")

    return "\n".join(lines)


def _render_bullets(lines: list[str], values: list, *, empty: str = "Sin datos registrados.") -> None:
    if values:
        for value in values:
            lines.append(f"- {value}")
    else:
        lines.append(f"- {empty}")
    lines.append("")


def render_project_page(
    title: str,
    project_id: str,
    state: dict,
    materials: list[dict],
    recent_additions: list[dict],
    notes: list[dict],
    sections: dict[str, dict] | None = None,
) -> str:
    """Render a Proyectos project dossier page."""
    section_records = sections or {}
    lines: list[str] = [f"# {title}\n"]

    estado = section_records.get("estado") or {}
    lines.append(f"## {estado.get('title') or 'Estado del proyecto'}\n")
    if estado.get("body"):
        lines.append(str(estado["body"]).strip())
        lines.append("")
    else:
        lines.append(f"- **Proyecto:** {project_id}")
        lines.append(f"- **Fase:** {state.get('stage', 'lead')}")
        confidence = state.get("stage_confidence", 0)
        lines.append(f"- **Confianza:** {confidence}")
        if state.get("updated_at"):
            lines.append(f"- **Actualizado:** {state.get('updated_at')}")
        lines.append("")

    section_specs = [
        ("trabajo_en_curso", "Trabajo en curso", "current_work_in_progress"),
        ("objetivos_principales", "Objetivos principales", "main_objectives"),
        ("condiciones_restricciones", "Condiciones y restricciones", "known_conditions"),
        ("decisiones", "Decisiones", "decisions"),
        ("requisitos", "Requisitos", "requirements"),
        ("riesgos", "Problemas, riesgos y bloqueos", "risks_or_blockers"),
        ("informacion_pendiente", "Información pendiente", "missing_information"),
        ("proximo_foco", "Próximo foco", "next_focus"),
        ("aprendizajes", "Aprendizajes positivos", "positive_learnings"),
        ("errores_reparaciones", "Errores y acciones de reparación", "mistakes_or_regrets"),
        ("acciones_reparacion", "Acciones de reparación", "repair_actions"),
    ]
    for section_id, heading, field in section_specs:
        section = section_records.get(section_id) or {}
        lines.append(f"## {section.get('title') or heading}\n")
        if section.get("body"):
            lines.append(str(section["body"]).strip())
            lines.append("")
        else:
            _render_bullets(lines, state.get(field) or [])

    important_ids = set(state.get("important_material_ids") or [])
    important = [m for m in materials if m.get("material_id") in important_ids]
    if important:
        lines.append("## Materiales importantes\n")
        for item in important:
            link = f"[{item.get('name', '')}]({item.get('path', '')})" if item.get("path") else item.get("name", "")
            lines.append(f"- {link}")
        lines.append("")

    if recent_additions:
        lines.append("## Historial reciente\n")
        for item in recent_additions[:8]:
            link = f"[{item.get('name', '')}]({item.get('path', '')})" if item.get("path") else item.get("name", "")
            date_str = str(item.get("ingested_at", ""))[:10]
            lines.append(f"- {link}" + (f" ({date_str})" if date_str else ""))
        lines.append("")

    if notes:
        lines.append("## Notas recientes\n")
        for note in sorted(notes, key=lambda row: row.get("timestamp", ""), reverse=True)[:10]:
            kind = note.get("kind", "note")
            actor = note.get("actor", "")
            text = note.get("text", "")
            date_str = str(note.get("timestamp", ""))[:10]
            suffix = f" ({actor}, {date_str})" if actor or date_str else ""
            lines.append(f"- **{kind}:** {text}{suffix}")
        lines.append("")

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
