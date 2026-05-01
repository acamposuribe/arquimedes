"""FastAPI app for the Phase 8 web UI."""

from __future__ import annotations

import json
import re
import time
from pathlib import Path, PurePosixPath
from urllib.parse import urlencode, urlsplit

import mistune
from fastapi import FastAPI, Form, HTTPException, Query, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from markupsafe import Markup, escape
from starlette.middleware.trustedhost import TrustedHostMiddleware

from arquimedes import freshness as freshness_mod
from arquimedes import project_state as project_state_mod
from arquimedes import read as read_mod
from arquimedes import search as search_mod
from arquimedes.compile_pages import (
    _PROJECT_MATERIAL_TYPE_LABELS,
    _PROJECT_MATERIAL_TYPE_ORDER,
    _PROJECT_PHASE_LABELS,
    _PROJECT_PHASE_ORDER,
)
from arquimedes.domain_profiles import (
    display_domain_name,
    generated_label,
    is_practice_domain,
    is_proyectos_domain,
)
from arquimedes.index import get_index_path

_PROJECT_GALLERY_TYPES = {"drawing_set", "site_photo", "map_or_cartography"}

_HERE = Path(__file__).resolve().parent
_TEMPLATES = Jinja2Templates(directory=str(_HERE / "templates"))
_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp"}
_DOMAINS = ("research", "practice", "proyectos")


def _label(part: str) -> str:
    return part.replace("-", " ").replace("_", " ") or "wiki"


def _slugify_segment(name: str) -> str:
    """Slugify a path segment for URLs (lowercase, dashes, no punctuation)."""
    s = str(name or "").strip().lower()
    if not s:
        return ""
    s = re.sub(r"[\s_]+", "-", s)
    s = re.sub(r"[^\w\-.]", "", s, flags=re.UNICODE)
    s = re.sub(r"-+", "-", s)
    return s.strip("-.") or s


def _slugify_path(rel: str) -> str:
    parts = [seg for seg in rel.split("/") if seg]
    return "/".join(_slugify_segment(seg) or seg for seg in parts)


def wiki_url(path: str) -> str:
    rel = path.strip().lstrip("/")
    if rel.startswith("wiki/"):
        rel = rel[5:]
    if rel == "_index.md":
        return "/wiki"
    if rel.endswith("/_index.md"):
        rel = rel[:-10]
    elif rel.endswith(".md"):
        rel = rel[:-3]
    rel = _slugify_path(rel)
    return f"/wiki/{rel}".rstrip("/") if rel else "/wiki"


def material_url(material_id: str, query: str = "", depth: int | None = None, scope: str = "") -> str:
    path = f"/materials/{material_id}"
    params: dict[str, str | int] = {}
    if query.strip():
        params["q"] = query.strip()
    if depth is not None:
        params["depth"] = depth
    if scope.strip():
        params["scope"] = scope.strip()
    return f"{path}?{urlencode(params)}" if params else path


def truncate_words(text: str, limit: int = 42) -> str:
    compact = re.sub(r"\s+", " ", str(text or "")).strip()
    if not compact:
        return ""
    words = compact.split(" ")
    if len(words) <= limit:
        return compact
    return " ".join(words[:limit]).rstrip(".,;: ") + "..."


def _normalized_domain(domain: str | None) -> str | None:
    value = str(domain or "").strip().lower()
    return value if value in _DOMAINS else None


def _resolve_wiki_slug_path(rel_path: str) -> str:
    """Map a possibly-slugified URL path to the actual on-disk wiki path.

    Walks the wiki tree segment by segment, preferring exact matches and
    falling back to slug matches when no exact entry exists. Returns the
    original path unchanged if a segment can't be resolved (caller will
    raise FileNotFoundError naturally)."""
    if not rel_path:
        return rel_path
    root = read_mod.get_project_root() / "wiki"
    parts = [p for p in rel_path.split("/") if p]
    current = root
    out: list[str] = []
    for index, part in enumerate(parts):
        is_last = index == len(parts) - 1
        if not current.is_dir():
            return rel_path
        direct_dir = current / part
        if direct_dir.is_dir():
            out.append(part)
            current = direct_dir
            continue
        if is_last:
            md_direct = current / part if part.endswith(".md") else current / f"{part}.md"
            if md_direct.exists():
                out.append(md_direct.stem)
                return "/".join(out)
        target_slug = _slugify_segment(part)
        match = None
        for child in current.iterdir():
            stem = child.stem if child.suffix == ".md" else child.name
            if _slugify_segment(stem) == target_slug:
                match = child
                break
        if match is None:
            return rel_path
        if match.is_dir():
            out.append(match.name)
            current = match
        else:
            out.append(match.stem)
            return "/".join(out)
    return "/".join(out) if out else rel_path


def _domain_label(domain: str) -> str:
    return display_domain_name(domain)


def _ui_label(key: str, domain: str | None, default: str) -> str:
    return generated_label(key, domain or "research", default=default)


def _ui_lang(domain: str | None) -> str:
    value = domain or ""
    if is_practice_domain(value, default="research") or is_proyectos_domain(value, default="research"):
        return "es"
    return "en"


def _heading_candidates(key: str, default: str) -> list[str]:
    values = [
        default,
        generated_label(key, "research", default=default),
        generated_label(key, "practice", default=default),
        generated_label(key, "proyectos", default=default),
    ]
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        clean = str(value or "").strip()
        if clean and clean not in seen:
            seen.add(clean)
            ordered.append(clean)
    return ordered


def _section_regex(headings: list[str]) -> str:
    return r"(?ms)^## (?:" + "|".join(re.escape(heading) for heading in headings) + r")\n.*?(?=^## |\Z)"


def _path_domain(path: str) -> str | None:
    rel = path.strip().strip("/")
    parts = [part for part in rel.split("/") if part]
    if len(parts) >= 2 and parts[0] == "wiki" and parts[1] in _DOMAINS:
        return parts[1]
    return None


def _page_domain_from_wiki_path(path: Path | None) -> str | None:
    if path is None:
        return None
    try:
        rel_parts = path.resolve().relative_to((read_mod.get_project_root() / "wiki").resolve()).parts
    except ValueError:
        return None
    if rel_parts and rel_parts[0] in _DOMAINS:
        return rel_parts[0]
    return None


def _project_rel_path(path: Path) -> str:
    return path.resolve().relative_to(read_mod.get_project_root().resolve()).as_posix()


def _wiki_rel_parts(path: Path) -> tuple[str, ...]:
    return path.resolve().relative_to((read_mod.get_project_root() / "wiki").resolve()).parts


def _active_domain(request: Request, explicit_domain: str | None = None, page_domain: str | None = None) -> str:
    return (
        _normalized_domain(page_domain)
        or _normalized_domain(explicit_domain)
        or _path_domain(request.url.path)
        or "research"
    )


def _domain_home_url(domain: str) -> str:
    return f"/?{urlencode({'domain': domain})}"


def _domain_search_url(domain: str) -> str:
    return f"/search?{urlencode({'domain': domain})}"


def _domain_wiki_url(domain: str) -> str:
    return f"/wiki/{domain}"


def _domain_tab_url(request: Request, target_domain: str, active_domain: str) -> str:
    path = request.url.path
    if path == "/":
        return _domain_home_url(target_domain)
    if path.startswith("/search"):
        items: list[tuple[str, str]] = []
        for key, value in request.query_params.multi_items():
            if key == "domain":
                continue
            if key == "facet" and str(value).startswith("domain=="):
                continue
            if key == "collection":
                continue
            items.append((key, value))
        items.append(("domain", target_domain))
        query = urlencode(items, doseq=True)
        return f"/search?{query}" if query else "/search"
    if path.startswith("/wiki"):
        return _domain_wiki_url(target_domain)
    if target_domain == active_domain:
        query = request.url.query
        return f"{path}?{query}" if query else path
    return _domain_wiki_url(target_domain)


def breadcrumbs(path: str) -> list[dict]:
    crumbs = [{"label": "Home", "url": "/"}]
    rel = path.strip().lstrip("/")
    if not rel.startswith("wiki/"):
        return crumbs
    crumbs.append({"label": "Wiki", "url": "/wiki"})
    parts = list(PurePosixPath(rel[5:]).parts)
    if not parts:
        return crumbs
    if parts[-1] == "_index.md":
        parts = parts[:-1]
    current: list[str] = []
    for part in parts:
        current.append(part)
        target = f"wiki/{'/'.join(current)}"
        if part.endswith(".md"):
            target = target[:-3]
            part = part[:-3]
        crumbs.append({"label": _label(part), "url": wiki_url(target)})
    return crumbs


def _resolve_relative(base: str, target: str) -> PurePosixPath | None:
    if target.startswith("/"):
        return None
    parts: list[str] = []
    for part in (*PurePosixPath(base).parent.parts, *PurePosixPath(target).parts):
        if part in ("", "."):
            continue
        if part == "..":
            if not parts:
                return None
            parts.pop()
            continue
        parts.append(part)
    return PurePosixPath(*parts)


def _rewrite_target(target: str, current_path: str, material_id: str | None) -> str:
    if not target or target.startswith(("#", "/", "mailto:", "data:", "javascript:")):
        return target
    parsed = urlsplit(target)
    if parsed.scheme in {"http", "https"}:
        return target
    if parsed.scheme == "file":
        return f"/source/{material_id}" if material_id else target
    path = parsed.path
    if material_id and path.startswith("figures/"):
        name = PurePosixPath(path).name
        if Path(name).suffix.lower() in _IMAGE_EXTENSIONS:
            return f"/figures-low/{material_id}/{name}"
    if path.startswith("wiki/") and path.endswith(".md"):
        return wiki_url(path)
    resolved = _resolve_relative(current_path, path)
    if resolved and resolved.parts[:1] == ("wiki",) and resolved.suffix == ".md":
        return wiki_url(resolved.as_posix())
    if resolved and len(resolved.parts) == 3 and resolved.parts[0] == "extracted" and resolved.parts[2] == "text.md":
        return f"/extracted/{resolved.parts[1]}/text"
    if resolved and len(resolved.parts) >= 4 and resolved.parts[0] == "extracted" and resolved.parts[2] == "figures":
        name = resolved.name
        if Path(name).suffix.lower() in _IMAGE_EXTENSIONS:
            return f"/figures-low/{resolved.parts[1]}/{name}"
    return target


def render_wiki_markdown(md_text: str, current_path: str, material_id: str | None = None) -> Markup:
    html = mistune.html(md_text)
    html = re.sub(
        r"(<h2>(?:Metadata|Metadatos)</h2>\s*<table)(>)",
        r'\1 class="metadata-table"\2',
        html,
        count=1,
    )
    html = re.sub(
        r"(<h2>(?:Related Materials|Materiales relacionados)</h2>\s*<ul)(>)",
        r'\1 class="related-materials-list"\2',
        html,
        count=1,
    )

    def _replace(match: re.Match[str]) -> str:
        attr, quote, value = match.groups()
        return f"{attr}={quote}{_rewrite_target(value, current_path, material_id)}{quote}"

    html = re.sub(r'(href|src)=(["\'])(.*?)\2', _replace, html)
    return Markup(html)


def _split_markdown_section(md_text: str, heading: str | list[str]) -> tuple[str, str, str]:
    headings = [heading] if isinstance(heading, str) else heading
    match = re.search(_section_regex(headings), md_text)
    if not match:
        return md_text.strip(), "", ""
    before = md_text[:match.start()].strip()
    section = match.group(0).strip()
    after = md_text[match.end():].strip()
    return before, section, after


def _split_material_sections(md_text: str) -> tuple[str, str, str]:
    body = re.sub(_section_regex(_heading_candidates("figures", "Figures")), "", md_text)
    body = re.sub(r"(?m)^\[(?:Open original file|Full extracted text|Abrir archivo original|Texto extraído completo)\]\(.*\)\s*$\n?", "", body)
    annotations_block = ""
    annotations = re.search(_section_regex(_heading_candidates("reader_annotations", "Reader Annotations")), body)
    if annotations:
        annotations_block = annotations.group(0).strip()
        body = body[:annotations.start()] + body[annotations.end():]
    related_block = ""
    related = re.search(_section_regex(_heading_candidates("related_materials", "Related Materials")), body)
    if related:
        related_block = related.group(0).strip()
        body = body[:related.start()] + body[related.end():]
    return body.strip(), annotations_block, related_block


def _strip_project_material_chrome(md_text: str) -> str:
    for key, default in (
        ("metadata", "Metadata"),
        ("summary", "Summary"),
        ("source", "Source"),
    ):
        md_text = re.sub(_section_regex(_heading_candidates(key, default)), "", md_text)
    return md_text.strip()


def _strip_material_sections(md_text: str) -> str:
    body, annotations_block, related_block = _split_material_sections(md_text)
    md_text = body
    if annotations_block:
        md_text = md_text.rstrip() + "\n\n" + annotations_block + "\n"
    if related_block:
        md_text = md_text.rstrip() + "\n\n" + related_block + "\n"
    return md_text.strip()


def _plain(value) -> str:
    if isinstance(value, dict):
        return str(value.get("value") or "")
    return str(value or "")


def _plain_list(value) -> list[str]:
    if isinstance(value, dict):
        value = value.get("value")
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value or "").strip()
    return [text] if text else []


_PROJECT_EXTRACTION_LIST_FIELDS = [
    ("main_points", "Puntos principales"),
    ("decisions", "Decisiones"),
    ("requirements", "Requisitos"),
    ("risks_or_blockers", "Riesgos y bloqueos"),
    ("open_items", "Pendientes"),
    ("actors", "Actores"),
    ("dates_and_deadlines", "Fechas y plazos"),
    ("spatial_or_design_scope", "Ámbito espacial o de diseño"),
    ("budget_signals", "Señales de presupuesto"),
    ("evidence_refs", "Referencias de evidencia"),
]


def _project_extraction_context(meta: dict) -> dict | None:
    if not is_proyectos_domain(str(meta.get("domain") or ""), default="research"):
        return None
    project_extraction = meta.get("project_extraction")
    if not isinstance(project_extraction, dict):
        return None
    groups = [
        {"key": key, "label": label, "items": items}
        for key, label in _PROJECT_EXTRACTION_LIST_FIELDS
        if (items := _plain_list(project_extraction.get(key)))
    ]
    relevance = _plain(project_extraction.get("project_relevance"))
    material_type = _plain(project_extraction.get("project_material_type"))
    material_type_label = _PROJECT_MATERIAL_TYPE_LABELS.get(material_type, material_type.replace("_", " ") if material_type else "")
    authors = meta.get("authors") or []
    if isinstance(authors, list):
        author_text = ", ".join(str(author).strip() for author in authors if str(author).strip())
    else:
        author_text = str(authors or "").strip()
    collection = str(meta.get("collection") or "").strip()
    domain = str(meta.get("domain") or "").strip()
    project_url = wiki_url(f"wiki/{domain}/{collection}/_index.md") if domain and collection else ""
    metadata = [
        {"label": "Proyecto", "value": collection, "url": project_url},
        {"label": "Tipo", "value": material_type_label},
        {"label": "Año", "value": str(meta.get("year") or "").strip()},
        {"label": "Autores", "value": author_text},
    ]
    metadata = [item for item in metadata if item["value"]]
    if not (relevance or material_type or groups):
        return None
    return {
        "title": str(meta.get("title") or meta.get("material_id") or "").strip(),
        "material_type": material_type_label,
        "material_type_key": material_type,
        "metadata": metadata,
        "relevance": relevance,
        "groups": groups,
    }


def _figure_view_models(material_id: str, figures: list[dict]) -> list[dict]:
    items = []
    for figure in figures:
        name = PurePosixPath(str(figure.get("image_path") or "")).name
        if not name or Path(name).suffix.lower() not in _IMAGE_EXTENSIONS:
            continue
        items.append({
            **figure,
            "image_filename": name,
            "sidecar_filename": str(figure.get("_sidecar_filename") or "").strip(),
            "visual_type_text": _plain(figure.get("visual_type")),
            "caption_text": _plain(figure.get("caption")),
            "description_text": _plain(figure.get("description")),
            "image_url": f"/figures-low/{material_id}/{name}",
            "zoom_url": f"/figures/{material_id}/{name}",
        })
    return items


def _material_preview_images(material_id: str, limit: int = 4) -> list[dict]:
    return [
        {"image_url": item["image_url"], "figure_id": item.get("figure_id") or ""}
        for item in _figure_view_models(material_id, read_mod.load_material_figures(material_id))[:limit]
    ]


def _standalone_project_image_context(meta: dict, material_id: str, figures: list[dict]) -> dict | None:
    if not is_proyectos_domain(str(meta.get("domain") or ""), default="research"):
        return None
    if str(meta.get("file_type") or "").strip() != "image":
        return None
    figure_items = _figure_view_models(material_id, figures)
    if not figure_items:
        return None
    figure = figure_items[0]
    return {
        "title": str(meta.get("title") or material_id).strip(),
        "image_url": figure["image_url"],
        "zoom_url": figure["zoom_url"],
        "caption": figure.get("caption_text") or figure.get("description_text") or "",
    }


def _thumbnail_view_models(material_id: str) -> list[dict]:
    return [
        {
            **item,
            "image_url": f"/thumbnails/{material_id}/{item['filename']}",
            "label": f"Page {item['page_number']}",
        }
        for item in read_mod.load_material_thumbnails(material_id)
    ]


def _material_sidebar_context(material_id: str) -> dict:
    meta = read_mod.load_material_meta(material_id)
    figures = read_mod.load_material_figures(material_id)
    page_thumbnails = _thumbnail_view_models(material_id)
    return {
        "material_id": material_id,
        "title": str(meta.get("title") or material_id),
        "domain": str(meta.get("domain") or ""),
        "project_extraction": _project_extraction_context(meta),
        "project_primary_image": _standalone_project_image_context(meta, material_id, figures),
        "collection_url": wiki_url(f"wiki/{meta.get('domain')}/{meta.get('collection')}/_index.md") if meta.get("domain") and meta.get("collection") else "",
        "collection_label": f"{meta.get('domain')}/{meta.get('collection')}" if meta.get("domain") and meta.get("collection") else "",
        "source_url": f"/source/{material_id}" if read_mod.material_source_path(material_id) else "",
        "extracted_text_url": f"/extracted/{material_id}/text" if read_mod.material_extracted_text_path(material_id) else "",
        "figures_url": f"/materials/{material_id}/figures" if figures else "",
        "figure_count": len(_figure_view_models(material_id, figures)),
        "page_thumbnails": page_thumbnails,
        "page_thumbnail_count": len(page_thumbnails),
        "page_thumbnails_collapsed": len(page_thumbnails) > 20,
        "no_figures": not figures,
    }


def _project_home_cards(collections: list[dict]) -> list[dict]:
    cards: list[dict] = []
    for item in collections:
        project_id = str(item.get("collection") or "").strip()
        domain = str(item.get("domain") or "").strip()
        if not project_id or project_id == "_general" or not is_proyectos_domain(domain, default="research"):
            continue
        state = project_state_mod.load_project_state(project_id, root=read_mod.get_project_root())
        material_count = len(read_mod.materials_for_collection(domain, project_id))
        cards.append({
            "project_id": project_id,
            "title": str(state.get("project_title") or project_id),
            "url": wiki_url(f"wiki/{domain}/{project_id}/_index.md"),
            "stage": str(state.get("stage") or "lead").replace("_", " "),
            "updated_at": str(state.get("updated_at") or "")[:10],
            "material_count": material_count,
            "current_work": [str(value) for value in (state.get("current_work_in_progress") or []) if str(value).strip()][:2],
            "next_focus": [str(value) for value in (state.get("next_focus") or []) if str(value).strip()][:2],
            "risks": [str(value) for value in (state.get("risks_or_blockers") or []) if str(value).strip()][:2],
        })
    return sorted(cards, key=lambda card: card["title"].lower())


def _home_figure_tiles(domain: str, limit: int = 15) -> list[dict]:
    tiles: list[dict] = []
    for row in read_mod.random_figures(limit=limit * 2, domain=domain):
        material_id = str(row.get("material_id") or "").strip()
        image_name = PurePosixPath(str(row.get("image_path") or "")).name
        if not material_id or not image_name or Path(image_name).suffix.lower() not in _IMAGE_EXTENSIONS:
            continue
        if not read_mod.material_figure_image_path(material_id, image_name):
            continue
        tiles.append({
            "image_url": f"/figures-low/{material_id}/{image_name}",
            "material_url": f"/materials/{material_id}",
            "title": str(row.get("title") or material_id),
            "caption": _plain(row.get("caption")) or _plain(row.get("description")) or str(row.get("title") or material_id),
        })
        if len(tiles) >= limit:
            break
    return tiles


def _low_res_figure_path(material_id: str, filename: str, size: int = 360) -> Path | None:
    source = read_mod.material_figure_image_path(material_id, filename)
    if not source:
        return None
    cache_dir = source.parent / ".lowres"
    cache_name = f"{source.stem}-{size}.jpg"
    cached = cache_dir / cache_name
    if cached.exists() and cached.stat().st_mtime >= source.stat().st_mtime:
        return cached

    try:
        from PIL import Image, ImageOps

        cache_dir.mkdir(parents=True, exist_ok=True)
        with Image.open(source) as image:
            thumb = ImageOps.fit(image.convert("RGB"), (size, size), method=Image.Resampling.LANCZOS)
            thumb.save(cached, format="JPEG", quality=72, optimize=True)
    except Exception:
        return source
    return cached


def _collection_sidebar_context(domain: str, collection: str) -> list[dict]:
    materials = read_mod.materials_for_collection(domain, collection)
    return _scoped_material_sidebar_items(materials)


def _collection_material_cards(domain: str, collection: str) -> list[dict]:
    cards: list[dict] = []
    for item in read_mod.materials_for_collection(domain, collection):
        material_id = item["material_id"]
        try:
            meta = read_mod.load_material_meta(material_id)
        except FileNotFoundError:
            continue
        cards.append({
            "material_id": material_id,
            "title": str(meta.get("title") or material_id),
            "summary": _plain(meta.get("summary")),
            "domain": str(meta.get("domain") or domain),
            "collection": str(meta.get("collection") or collection),
            "document_type": _plain(meta.get("document_type")) or str(meta.get("raw_document_type") or ""),
            "year": str(meta.get("year") or ""),
            "preview_images": _material_preview_images(material_id, limit=4),
        })
    return sorted(cards, key=lambda card: card["title"].lower())


def _format_project_date_es(value: str) -> str:
    match = re.search(r"(\d{4})-(\d{2})-(\d{2})", str(value or ""))
    if not match:
        return ""
    year, month, day = match.groups()
    months = {
        "01": "enero", "02": "febrero", "03": "marzo", "04": "abril",
        "05": "mayo", "06": "junio", "07": "julio", "08": "agosto",
        "09": "septiembre", "10": "octubre", "11": "noviembre", "12": "diciembre",
    }
    return f"{int(day)} {months.get(month, month)} {year}"


def _project_drawing_grid_title(title: str, phase_label: str) -> str:
    """Shorten drawing card titles inside phase subsections."""
    value = str(title or "").strip()
    phase = str(phase_label or "").strip()
    if phase and value.casefold().startswith(phase.casefold() + "."):
        return value[len(phase) + 1:].strip()
    if phase and value.casefold().startswith(phase.casefold() + " —"):
        return value[len(phase) + 2:].strip()
    return value


def _project_notes_context(project_id: str) -> dict:
    notes = sorted(
        project_state_mod.load_project_notes(project_id, root=read_mod.get_project_root(), include_deleted=True),
        key=lambda row: (str(row.get("timestamp") or ""), str(row.get("note_id") or "")),
        reverse=True,
    )
    open_notes = [note for note in notes if str(note.get("status") or "open") == "open"]
    archived_notes = [note for note in notes if str(note.get("status") or "open") != "open"]
    return {"open": open_notes, "archived": archived_notes}


def _project_state_panel_context(project_id: str) -> list[dict]:
    state = project_state_mod.load_project_state(project_id, root=read_mod.get_project_root())
    specs = [
        ("main_objectives", "Objetivos principales"),
        ("current_work_in_progress", "Trabajo en curso"),
        ("next_focus", "Próximo foco"),
        ("known_conditions", "Condiciones y restricciones"),
        ("decisions", "Decisiones"),
        ("requirements", "Requisitos"),
        ("risks_or_blockers", "Problemas, riesgos y bloqueos"),
        ("missing_information", "Información pendiente"),
        ("mistakes_or_regrets", "Errores"),
        ("repair_actions", "Acciones de reparación"),
    ]
    groups = []
    for field, label in specs:
        values = [str(value).strip() for value in (state.get(field) or []) if str(value).strip()]
        if values:
            groups.append({
                "field": field,
                "label": label,
                "items": [{"index": idx, "text": value} for idx, value in enumerate(values)],
            })
    return groups


def _project_phase_timeline(stage: str) -> dict:
    steps = [
        ("lead", "Encargo"),
        ("schematic_design", "Anteproyecto"),
        ("basic_project", "Proyecto básico"),
        ("execution_project", "Proyecto de ejecución"),
        ("construction", "Dirección de obra"),
        ("handover", "Finalizado"),
    ]
    aliases = {"feasibility": "lead", "tender": "execution_project", "archived": "handover"}
    active = aliases.get(str(stage or ""), str(stage or ""))
    active_idx = next((idx for idx, (key, _label) in enumerate(steps) if key == active), 0)
    return {
        "steps": [
            {"key": key, "label": label, "state": "done" if idx < active_idx else "active" if idx == active_idx else "pending"}
            for idx, (key, label) in enumerate(steps)
        ],
        "active_label": steps[active_idx][1],
    }


def _project_material_groups(domain: str, collection: str) -> list[dict]:
    """Group materials in a Proyectos collection by project_material_type.

    Returns list of groups in canonical type order, each with:
      - type_key, label, variant ('gallery' | 'list'), count, items[].
    Each item has material_id, title, material_url, document_type, year,
    summary (truncated for list variant), thumbnail_url, preview_images.
    """
    grouped: dict[str, list[dict]] = {}
    for entry in read_mod.materials_for_collection(domain, collection):
        material_id = entry["material_id"]
        try:
            meta = read_mod.load_material_meta(material_id)
        except FileNotFoundError:
            continue
        project_extraction = meta.get("project_extraction") or {}
        if not isinstance(project_extraction, dict):
            project_extraction = {}
        type_field = project_extraction.get("project_material_type")
        type_key = _plain(type_field) or "unknown"
        if type_key not in _PROJECT_MATERIAL_TYPE_LABELS:
            type_key = "unknown"

        thumbs = read_mod.load_material_thumbnails(material_id)
        first_thumb = thumbs[0] if thumbs else None
        thumbnail_url = (
            f"/thumbnails/{material_id}/{first_thumb['filename']}" if first_thumb else ""
        )
        preview_images = _material_preview_images(material_id, limit=4)
        if not thumbnail_url and preview_images:
            thumbnail_url = preview_images[0]["image_url"]

        phase_key = _plain(project_extraction.get("project_phase")) or "unknown"
        if phase_key not in _PROJECT_PHASE_LABELS:
            phase_key = "unknown"
        phase_label = _PROJECT_PHASE_LABELS.get(phase_key, phase_key)
        full_title = str(project_extraction.get("drawing_scope") or meta.get("title") or material_id) if type_key == "drawing_set" else str(meta.get("title") or material_id)
        display_title = _project_drawing_grid_title(full_title, phase_label) if type_key == "drawing_set" else full_title
        if type_key == "site_photo":
            display_title = _format_project_date_es(str(project_extraction.get("material_date") or "")) or display_title
        grouped.setdefault(type_key, []).append({
            "material_id": material_id,
            "title": full_title,
            "display_title": display_title,
            "phase_key": phase_key,
            "phase_label": phase_label,
            "material_date": str(project_extraction.get("material_date") or ""),
            "material_url": f"/materials/{material_id}",
            "summary": _plain(meta.get("summary")),
            "document_type": _plain(meta.get("document_type")) or str(meta.get("raw_document_type") or ""),
            "year": str(meta.get("year") or ""),
            "thumbnail_url": thumbnail_url,
            "preview_images": preview_images,
        })

    groups: list[dict] = []
    for type_key, items in grouped.items():
        if type_key == "site_photo":
            items.sort(key=lambda x: (str(x.get("material_date") or ""), x["title"].lower()), reverse=True)
        else:
            items.sort(key=lambda x: (_PROJECT_PHASE_ORDER.get(x.get("phase_key", "unknown"), 999), x["title"].lower()))
        variant = "gallery" if type_key in _PROJECT_GALLERY_TYPES else "list"
        phase_groups = []
        if type_key == "drawing_set":
            by_phase: dict[str, list[dict]] = {}
            for item in items:
                by_phase.setdefault(item.get("phase_key") or "unknown", []).append(item)
            for phase_key, phase_items in sorted(by_phase.items(), key=lambda row: _PROJECT_PHASE_ORDER.get(row[0], 999)):
                phase_groups.append({
                    "phase_key": phase_key,
                    "phase_label": _PROJECT_PHASE_LABELS.get(phase_key, phase_key),
                    "items": phase_items,
                    "count": len(phase_items),
                })
        groups.append({
            "type_key": type_key,
            "label": _PROJECT_MATERIAL_TYPE_LABELS.get(type_key, type_key),
            "variant": variant,
            "count": len(items),
            "items": items,
            "phase_groups": phase_groups,
        })
    groups.sort(key=lambda g: _PROJECT_MATERIAL_TYPE_ORDER.get(g["type_key"], 999))
    return groups


def _concept_sidebar_context(cluster_id: str) -> list[dict]:
    materials = read_mod.materials_for_concept(cluster_id)
    return _scoped_material_sidebar_items(materials)


def _is_concept_page(path: Path) -> bool:
    return path.name != "_index.md" and path.suffix == ".md" and path.parent.name in {"concepts", "bridge-concepts"}


def _scoped_material_sidebar_items(materials: list[dict]) -> list[dict]:
    items = []
    for m in materials:
        mid = m["material_id"]
        thumbs = read_mod.load_material_thumbnails(mid)
        first_thumb = thumbs[0] if thumbs else None
        items.append({
            "material_id": mid,
            "title": m["title"] or mid,
            "image_url": f"/thumbnails/{mid}/{first_thumb['filename']}" if first_thumb else None,
            "material_url": f"/materials/{mid}",
        })
    return [item for item in items if item["image_url"]]


def _query_terms(query: str) -> list[str]:
    terms = [term for term in re.findall(r"[^\W_]+(?:[-'][^\W_]+)*", query, flags=re.UNICODE) if term.strip()]
    return list(dict.fromkeys(terms))


def _excerpt_html(text: str, query: str, *, window: int = 260) -> Markup:
    compact = re.sub(r"\s+", " ", str(text or "")).strip()
    if not compact:
        return Markup("")

    terms = sorted(_query_terms(query), key=len, reverse=True)
    lower = compact.lower()
    indices = [lower.find(term.lower()) for term in terms]
    indices = [idx for idx in indices if idx >= 0]
    if indices:
        center = min(indices)
        start = max(center - 90, 0)
        end = min(len(compact), start + window)
        start = max(0, end - window)
    else:
        start = 0
        end = min(len(compact), window)
    snippet = compact[start:end].strip()
    if not snippet:
        return Markup("")

    prefix = "..." if start > 0 else ""
    suffix = "..." if end < len(compact) else ""
    if not terms:
        return Markup(f"{escape(prefix)}{escape(snippet)}{escape(suffix)}")

    pattern = re.compile("|".join(re.escape(term) for term in terms), re.IGNORECASE)
    parts: list[Markup] = [Markup(escape(prefix))]
    last = 0
    for match in pattern.finditer(snippet):
        parts.append(Markup(escape(snippet[last:match.start()])))
        parts.append(Markup(f"<mark>{escape(match.group(0))}</mark>"))
        last = match.end()
    parts.append(Markup(escape(snippet[last:])))
    parts.append(Markup(escape(suffix)))
    return Markup("").join(parts)


def _pages_label(pages: list[int]) -> str:
    if not pages:
        return ""
    if len(pages) == 1:
        return f"p. {pages[0]}"
    return "pp. " + ", ".join(str(page) for page in pages)


def _material_search_context(material_id: str, query: str, depth: int) -> dict:
    evidence = search_mod.search_material_evidence(
        query,
        material_id,
        depth=max(depth, 2),
        chunk_limit=4,
        annotation_limit=3,
        figure_limit=2,
        concept_limit=4,
    )
    return {
        "search_query": query,
        "search_depth": evidence.depth,
        "search_hits": {
            "has_matches": evidence.has_hits,
            "chunks": [
                {
                    "chunk_id": chunk.chunk_id,
                    "summary": chunk.summary,
                    "pages_label": _pages_label(chunk.source_pages),
                    "emphasized": chunk.emphasized,
                    "excerpt_html": _excerpt_html(chunk.text or chunk.summary, query),
                    "full_text_html": _excerpt_html(chunk.text or chunk.summary, query, radius=1000),
                }
                for chunk in evidence.chunks
            ],
            "annotations": [
                {
                    "annotation_id": hit.annotation_id,
                    "page_label": f"p. {hit.page}" if hit.page else "",
                    "type": hit.type,
                    "quoted_text": hit.quoted_text,
                    "comment": hit.comment,
                }
                for hit in evidence.annotations
            ],
            "concepts": [
                {
                    "concept_name": hit.concept_name,
                    "relevance": hit.relevance,
                    "pages_label": _pages_label(hit.source_pages),
                }
                for hit in evidence.concepts
            ],
            "figures": [
                {
                    "figure_id": hit.figure_id,
                    "visual_type": hit.visual_type,
                    "page_label": f"p. {hit.source_page}" if hit.source_page else "",
                    "description": hit.description,
                }
                for hit in evidence.figures
            ],
        },
    }


def _scoped_page_search_context(
    *,
    label: str,
    kind: str,
    query: str,
    results,
) -> dict:
    return {
        "page_search": {
            "kind": kind,
            "label": label,
            "query": query,
            "results": results,
        }
    }


def _material_page_context(material_id: str, body: str, *, admin_mode: bool = False) -> dict:
    meta = read_mod.load_material_meta(material_id)
    if is_proyectos_domain(str(meta.get("domain") or ""), default="research"):
        body = _strip_project_material_chrome(body)
    content_body, annotations_body, related_materials_body = _split_material_sections(body)
    return {
        **_material_sidebar_context(material_id),
        "content_body": content_body,
        "annotations_body": annotations_body,
        "related_materials_body": related_materials_body,
        "material_figures": _figure_view_models(material_id, read_mod.load_material_figures(material_id)),
        "material_admin_mode": admin_mode,
    }


def _delete_material_figures(material_id: str, selected_sidecars: list[str]) -> int:
    material_dir = read_mod.get_project_root() / "extracted" / material_id
    figures_dir = material_dir / "figures"
    if not figures_dir.is_dir():
        return 0
    delete_ids: set[str] = set()
    removed = 0
    for raw_name in selected_sidecars:
        name = PurePosixPath(str(raw_name or "")).name
        if not name or name != str(raw_name or "").strip() or not re.fullmatch(r"fig_\d+\.json", name):
            continue
        sidecar_path = figures_dir / name
        if not sidecar_path.exists():
            continue
        image_name = ""
        try:
            payload = json.loads(sidecar_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                image_name = PurePosixPath(str(payload.get("image_path") or "")).name
                figure_id = str(payload.get("figure_id") or "").strip()
                if figure_id:
                    delete_ids.add(figure_id)
        except (json.JSONDecodeError, OSError):
            payload = None
        if image_name and Path(image_name).suffix.lower() in _IMAGE_EXTENSIONS:
            image_path = figures_dir / image_name
            if image_path.exists():
                image_path.unlink()
            lowres_dir = figures_dir / ".lowres"
            for cached in lowres_dir.glob(f"{Path(image_name).stem}-*.jpg") if lowres_dir.is_dir() else []:
                cached.unlink()
        sidecar_path.unlink()
        removed += 1
    pages_path = material_dir / "pages.jsonl"
    if delete_ids and pages_path.exists():
        rewritten: list[str] = []
        changed = False
        for line in pages_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                rewritten.append(line)
                continue
            refs = row.get("figure_refs")
            if isinstance(refs, list):
                kept = [ref for ref in refs if ref not in delete_ids]
                if kept != refs:
                    row["figure_refs"] = kept
                    changed = True
            rewritten.append(json.dumps(row, ensure_ascii=False))
        if changed:
            pages_path.write_text(("\n".join(rewritten) + "\n") if rewritten else "", encoding="utf-8")
    return removed


def _wiki_context(path: Path, body: str, *, material_id: str | None = None, title: str | None = None, **extra) -> dict:
    rel = _project_rel_path(path)
    return {
        "breadcrumbs": breadcrumbs(rel),
        "content_html": render_wiki_markdown(body, rel, material_id),
        "page_title": title or _label(path.stem if path.name != "_index.md" else path.parent.name),
        "wiki_path": rel,
        **extra,
    }


def _serve_config(config: dict | None) -> dict:
    cfg = (config or {}).get("serve") or {}
    return cfg if isinstance(cfg, dict) else {}


def _public_exposure(config: dict | None) -> bool:
    return bool(_serve_config(config).get("public_exposure", False))


def _allowed_hosts(config: dict | None) -> list[str]:
    raw = _serve_config(config).get("allowed_hosts") or []
    if isinstance(raw, str):
        raw = [raw]
    return [str(item).strip() for item in raw if str(item).strip()]


def _search_facets_for_domain(facets: list[str], domain: str) -> list[str]:
    scoped = [str(item).strip() for item in facets if str(item).strip()]
    scoped = [item for item in scoped if not item.startswith("domain==")]
    scoped.append(f"domain=={domain}")
    return scoped


def _base_context(
    request: Request,
    *,
    page_title: str,
    active_domain: str | None = None,
    page_domain: str | None = None,
    **extra,
) -> dict:
    resolved_domain = _active_domain(request, active_domain, page_domain)
    collections = read_mod.list_domains_and_collections(resolved_domain)
    return {
        "page_title": page_title,
        "active_domain": resolved_domain,
        "active_domain_label": _domain_label(resolved_domain),
        "page_language": _ui_lang(page_domain or resolved_domain),
        "domain_tabs": [
            {
                "key": domain,
                "label": _domain_label(domain),
                "url": _domain_tab_url(request, domain, resolved_domain),
                "active": domain == resolved_domain,
            }
            for domain in _DOMAINS
        ],
        "nav_collections": collections,
        "nav_global_concepts": read_mod.list_glossary_concepts(resolved_domain),
        **extra,
    }


def create_app(config: dict | None = None) -> FastAPI:
    app = FastAPI(title="Arquimedes")
    app.state.config = config or {}
    public_exposure = _public_exposure(config)
    app.state.public_exposure = public_exposure
    if public_exposure:
        hosts = _allowed_hosts(config)
        if not hosts:
            raise RuntimeError(
                "serve.public_exposure is true but serve.allowed_hosts is empty. "
                "Set serve.allowed_hosts to the public hostname(s) before exposing the UI."
            )
        app.add_middleware(TrustedHostMiddleware, allowed_hosts=hosts)
    app.mount("/static", StaticFiles(directory=str(_HERE / "static")), name="static")
    asset_version = str(int(time.time()))
    _TEMPLATES.env.globals.update(
        breadcrumbs=breadcrumbs,
        wiki_url=wiki_url,
        material_url=material_url,
        truncate_words=truncate_words,
        ui_label=_ui_label,
        asset_version=asset_version,
    )
    _TEMPLATES.env.globals["public_exposure"] = public_exposure

    @app.exception_handler(404)
    async def not_found(request: Request, exc: HTTPException):
        return _TEMPLATES.TemplateResponse(
            request,
            "error.html",
            {"request": request, **_base_context(request, page_title="Not found", status_code=404, message=str(exc.detail or "Not found"))},
            status_code=404,
        )

    @app.get("/health")
    def health():
        return {"ok": True}

    if not public_exposure:
        @app.get("/api/freshness")
        def freshness():
            return JSONResponse(freshness_mod.workspace_freshness_status())

        @app.post("/update")
        def update():
            return JSONResponse(freshness_mod.update_workspace())

    @app.post("/projects/{project_id}/notes/{note_id}/edit")
    def edit_project_note(project_id: str, note_id: str, text: str = Form(...), actor: str = Form("human"), return_to: str = Form("/")):
        project_state_mod.update_project_note(project_id, note_id=note_id, text=text, actor=actor, root=read_mod.get_project_root())
        return RedirectResponse(url=return_to or "/", status_code=303)

    @app.post("/projects/{project_id}/notes/{note_id}/delete")
    def delete_project_note(project_id: str, note_id: str, actor: str = Form("human"), return_to: str = Form("/")):
        project_state_mod.delete_project_note(project_id, note_id=note_id, actor=actor, root=read_mod.get_project_root())
        return RedirectResponse(url=return_to or "/", status_code=303)

    @app.post("/projects/{project_id}/state/{field}/add")
    def add_project_state_item(project_id: str, field: str, text: str = Form(...), actor: str = Form("human"), return_to: str = Form("/")):
        project_state_mod.add_project_state_list_item(project_id, field=field, text=text, actor=actor, root=read_mod.get_project_root())
        return RedirectResponse(url=return_to or "/", status_code=303)

    @app.post("/projects/{project_id}/state/{field}/{index}/edit")
    def edit_project_state_item(project_id: str, field: str, index: int, text: str = Form(...), actor: str = Form("human"), return_to: str = Form("/")):
        project_state_mod.update_project_state_list_item(project_id, field=field, index=index, text=text, actor=actor, root=read_mod.get_project_root())
        return RedirectResponse(url=return_to or "/", status_code=303)

    @app.post("/projects/{project_id}/state/{field}/{index}/delete")
    def delete_project_state_item(project_id: str, field: str, index: int, actor: str = Form("human"), return_to: str = Form("/")):
        project_state_mod.delete_project_state_list_item(project_id, field=field, index=index, actor=actor, root=read_mod.get_project_root())
        return RedirectResponse(url=return_to or "/", status_code=303)

    @app.get("/", response_class=HTMLResponse)
    def home(request: Request, domain: str = ""):
        active_domain = _active_domain(request, domain)
        index_missing = not get_index_path().exists()
        collections = read_mod.list_domains_and_collections(active_domain)
        is_projects_home = is_proyectos_domain(active_domain, default="research")
        return _TEMPLATES.TemplateResponse(
            request,
            "home.html",
            {
                "request": request,
                **_base_context(
                    request,
                    page_title="Arquimedes",
                    active_domain=active_domain,
                    index_missing=index_missing,
                    recent_materials=[] if is_projects_home else read_mod.recent_materials(domain=active_domain),
                    collections=collections,
                    home_figures=[] if is_projects_home else _home_figure_tiles(active_domain),
                    project_cards=_project_home_cards(collections) if is_projects_home else [],
                ),
            },
        )

    @app.get("/search", response_class=HTMLResponse)
    def search(
        request: Request,
        q: str = "",
        depth: int = 3,
        scope: str = "all",
        facet: list[str] = Query(default=[]),
        collection: str | None = None,
        limit: int = 20,
        domain: str = "",
    ):
        active_domain = _active_domain(request, domain)
        scoped_facets = _search_facets_for_domain(facet, active_domain)
        result = None
        index_missing = False
        if q.strip():
            try:
                result = search_mod.search(q, depth=depth, scope=scope, facets=scoped_facets, collection=collection, limit=limit)
            except FileNotFoundError:
                index_missing = True
        return _TEMPLATES.TemplateResponse(
            request,
            "search.html",
            {
                "request": request,
                **_base_context(
                    request,
                    page_title="Search",
                    active_domain=active_domain,
                    query=q,
                    depth=depth,
                    scope=scope,
                    facets=scoped_facets,
                    collection=collection or "",
                    result=result,
                    index_missing=index_missing,
                ),
            },
        )

    @app.get("/materials/{material_id}", response_class=HTMLResponse)
    def material_page(request: Request, material_id: str, q: str = "", depth: int = 3, scope: str = "all", domain: str = "", mode: str = ""):
        try:
            path = read_mod.material_wiki_path(material_id)
            body = read_mod.load_material_wiki(material_id)
            material = _material_page_context(material_id, body, admin_mode=(mode or "").strip().lower() == "admin")
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        search_query = q.strip()
        search_scope = (scope or "all").strip().lower()
        if search_scope not in {"all", "title", "author"}:
            search_scope = "all"
        search_context = {
            "search_query": "",
            "search_depth": depth,
            "search_scope": search_scope,
            "search_hits": None,
            "page_search": {"kind": "material", "label": _ui_label("search_this_material", str(material.get("domain") or domain), "Search This Material"), "query": search_query, "results": None},
        }
        if search_query and search_scope == "all":
            try:
                search_context = _material_search_context(material_id, search_query, depth)
                search_context["search_scope"] = search_scope
                search_context["page_search"] = {"kind": "material", "label": _ui_label("search_this_material", str(material.get("domain") or domain), "Search This Material"), "query": search_query, "results": None}
            except FileNotFoundError:
                search_context = {
                    "search_query": search_query,
                    "search_depth": depth,
                    "search_scope": search_scope,
                    "search_hits": None,
                    "page_search": {"kind": "material", "label": _ui_label("search_this_material", str(material.get("domain") or domain), "Search This Material"), "query": search_query, "results": None},
                }
        return _TEMPLATES.TemplateResponse(
            request,
            "wiki_page.html",
            {
                "request": request,
                **_base_context(request, page_title=material["title"], active_domain=domain, page_domain=material.get("domain")),
                **_wiki_context(
                    path,
                    material["content_body"],
                    **material,
                    annotations_html=render_wiki_markdown(material["annotations_body"], _project_rel_path(path), material_id) if material["annotations_body"] else "",
                    related_materials_html=render_wiki_markdown(material["related_materials_body"], _project_rel_path(path), material_id) if material["related_materials_body"] else "",
                    **search_context,
                ),
            },
        )

    @app.post("/materials/{material_id}/figures/delete")
    def material_delete_figures(request: Request, material_id: str, figure_sidecar: list[str] = Form([]), return_to: str = Form("")):
        try:
            meta = read_mod.load_material_meta(material_id)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        if not is_proyectos_domain(str(meta.get("domain") or ""), default="research"):
            raise HTTPException(status_code=404, detail="Figure deletion is only available for Proyectos materials")
        _delete_material_figures(material_id, figure_sidecar)
        target = (return_to or "").strip() or f"/materials/{material_id}?mode=admin"
        return RedirectResponse(url=target, status_code=303)

    @app.get("/materials/{material_id}/figures", response_class=HTMLResponse)
    def material_figures(request: Request, material_id: str):
        try:
            meta = read_mod.load_material_meta(material_id)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        figures = read_mod.load_material_figures(material_id)
        return _TEMPLATES.TemplateResponse(
            request,
            "figures.html",
            {
                "request": request,
                **_base_context(
                    request,
                    page_title=f"{_ui_label('figures_for', str(meta.get('domain') or ''), 'Figures')} · {meta.get('title') or material_id}",
                    page_domain=str(meta.get("domain") or ""),
                ),
                "material_id": material_id,
                "figures": _figure_view_models(material_id, figures),
                "empty_message": _ui_label("no_extracted_figures_for_material", str(meta.get("domain") or ""), "No extracted figures were found for this material.") if not figures else "",
                "breadcrumbs": [{"label": _ui_label("home", str(meta.get("domain") or ""), "Home"), "url": "/"}, {"label": _label(material_id), "url": f"/materials/{material_id}"}, {"label": _ui_label("figures_for", str(meta.get("domain") or ""), "Figures"), "url": f"/materials/{material_id}/figures"}],
            },
        )

    @app.get("/figures/{material_id}/{filename}")
    def figure_image(material_id: str, filename: str):
        if "/" in filename or "\\" in filename or Path(filename).suffix.lower() not in _IMAGE_EXTENSIONS:
            raise HTTPException(status_code=404, detail="Figure not found")
        path = read_mod.material_figure_image_path(material_id, filename)
        if not path:
            raise HTTPException(status_code=404, detail="Figure not found")
        return FileResponse(path)

    @app.get("/figures-low/{material_id}/{filename}")
    def low_res_figure_image(material_id: str, filename: str):
        if "/" in filename or "\\" in filename or Path(filename).suffix.lower() not in _IMAGE_EXTENSIONS:
            raise HTTPException(status_code=404, detail="Figure not found")
        path = _low_res_figure_path(material_id, filename)
        if not path:
            raise HTTPException(status_code=404, detail="Figure not found")
        return FileResponse(path)

    @app.get("/thumbnails/{material_id}/{filename}")
    def thumbnail_image(material_id: str, filename: str):
        if "/" in filename or "\\" in filename or Path(filename).suffix.lower() not in _IMAGE_EXTENSIONS:
            raise HTTPException(status_code=404, detail="Thumbnail not found")
        path = read_mod.material_thumbnail_path(material_id, filename)
        if not path:
            raise HTTPException(status_code=404, detail="Thumbnail not found")
        return FileResponse(path)

    @app.get("/wiki", response_class=HTMLResponse)
    def wiki_root(request: Request, domain: str = ""):
        active_domain = _active_domain(request, domain)
        return RedirectResponse(url=f"/?domain={active_domain}", status_code=302)

    @app.get("/wiki/{path:path}", response_class=HTMLResponse)
    def wiki_page(request: Request, path: str, q: str = "", depth: int = 3, domain: str = ""):
        clean_path = path.strip("/")
        if clean_path in {"research", "practice", "proyectos"}:
            return RedirectResponse(url=f"/?domain={clean_path}", status_code=302)
        path = _resolve_wiki_slug_path(path)
        try:
            page_path, body = read_mod.load_wiki_page(path)
        except FileNotFoundError:
            try:
                listing = read_mod.list_wiki_dir(path)
            except FileNotFoundError as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc
            listing_domain = _page_domain_from_wiki_path(read_mod.get_project_root() / "wiki" / path)
            return _TEMPLATES.TemplateResponse(
                request,
                "wiki_dir.html",
                {
                    "request": request,
                    **_base_context(
                        request,
                        page_title=_label(PurePosixPath(path).name),
                        active_domain=domain,
                        page_domain=listing_domain,
                        breadcrumbs=breadcrumbs(f"wiki/{path}/_index.md"),
                        listing=listing,
                    ),
                },
            )

        material_id = read_mod.material_id_for_wiki_path(page_path)
        page_domain = _page_domain_from_wiki_path(page_path)
        page_search = None
        if material_id:
            material = _material_page_context(material_id, body)
            content_body = material["content_body"]
            extra = material
            extra["annotations_html"] = render_wiki_markdown(material["annotations_body"], _project_rel_path(page_path), material_id) if material["annotations_body"] else ""
            extra["related_materials_html"] = render_wiki_markdown(material["related_materials_body"], _project_rel_path(page_path), material_id) if material["related_materials_body"] else ""
            search_query = q.strip()
            page_search = {"kind": "material", "label": _ui_label("search_this_material", str(extra.get("domain") or page_domain or ""), "Search This Material"), "query": search_query, "results": None}
            extra.update({"search_query": "", "search_depth": depth, "search_scope": "all", "search_hits": None, "page_search": page_search})
            if search_query:
                try:
                    material_search = _material_search_context(material_id, search_query, depth)
                    material_search["search_scope"] = "all"
                    material_search["page_search"] = page_search
                    extra.update(material_search)
                except FileNotFoundError:
                    extra.update({"search_query": search_query, "search_depth": depth, "search_scope": "all", "search_hits": None, "page_search": page_search})
        else:
            content_body = body
            extra = {}

        collection_material_thumbs = None
        collection_material_cards = None
        collection_after_html = ""
        concept_material_thumbs = None
        project_material_groups = None
        project_collection = False
        project_recent_html = ""
        project_notes = None
        project_state_panel = None
        project_phase_timeline = None
        project_id = ""
        page_record = read_mod.wiki_page_record(page_path)
        if page_path.name == "_index.md" and not material_id:
            try:
                rel_parts = _wiki_rel_parts(page_path)
                if len(rel_parts) == 3 and "shared" not in rel_parts and "concepts" not in rel_parts:
                    coll_domain, coll_name = rel_parts[0], rel_parts[1]
                    before_materials, _, after_materials = _split_markdown_section(body, _heading_candidates("materials", "Materials"))
                    content_body = before_materials
                    collection_after_html = render_wiki_markdown(after_materials, _project_rel_path(page_path)) if after_materials else ""
                    if is_proyectos_domain(coll_domain, default="research"):
                        project_collection = True
                        project_id = coll_name
                        before_notes, notes_section, after_notes = _split_markdown_section(
                            content_body,
                            _heading_candidates("notas_recientes", "Notas recientes"),
                        )
                        if notes_section:
                            content_body = "\n\n".join(part for part in [before_notes, after_notes] if part)
                        project_notes = _project_notes_context(coll_name)
                        before_recent, recent_history, after_recent = _split_markdown_section(
                            content_body,
                            _heading_candidates("recent_additions", "Recent Additions"),
                        )
                        if recent_history:
                            content_body = "\n\n".join(part for part in [before_recent, after_recent] if part)
                            project_recent_html = render_wiki_markdown(recent_history, _project_rel_path(page_path))
                            collection_after_html = ""
                        before_state, structured_state, after_state = _split_markdown_section(
                            content_body,
                            ["Datos estructurados del proyecto", "Structured Project Data"],
                        )
                        if structured_state:
                            content_body = "\n\n".join(part for part in [before_state, after_state] if part)
                        project_state_panel = _project_state_panel_context(coll_name)
                        project_phase_timeline = _project_phase_timeline(project_state_mod.load_project_state(coll_name).get("stage", "lead"))
                        project_material_groups = _project_material_groups(coll_domain, coll_name) or None
                    else:
                        collection_material_thumbs = _collection_sidebar_context(coll_domain, coll_name) or None
                        collection_material_cards = _collection_material_cards(coll_domain, coll_name) or None
                    scoped_results = None
                    if q.strip():
                        scoped_results = search_mod.search(
                            q,
                            depth=depth,
                            scope="all",
                            facets=[f"domain=={coll_domain}"],
                            collection=coll_name,
                            limit=20,
                        )
                    page_search = {
                        "kind": "collection",
                        "label": _ui_label("search_this_collection", coll_domain, "Search This Collection"),
                        "query": q.strip(),
                        "results": scoped_results,
                    }
            except (ValueError, Exception):
                pass
        elif (page_record and page_record.get("page_type") == "concept") or _is_concept_page(page_path):
            cluster_id = str(page_record.get("page_id") or "").strip() if page_record else ""
            concept_materials = read_mod.materials_for_concept(cluster_id) if cluster_id else []
            if not concept_materials:
                concept_materials = read_mod.materials_for_concept_page(page_path, body)
            concept_material_thumbs = _scoped_material_sidebar_items(concept_materials) or None
            scoped_results = None
            scoped_material_ids = [item["material_id"] for item in concept_materials]
            if q.strip():
                if scoped_material_ids:
                    scoped_results = search_mod.search(
                        q,
                        depth=depth,
                        scope="all",
                        material_ids=scoped_material_ids,
                        limit=20,
                    )
                else:
                    scoped_results = search_mod.SearchResult(query=q.strip(), depth=depth, total=0, results=[])
            page_search = {
                "kind": "concept",
                "label": _ui_label("search_this_concept", page_domain or "", "Search This Concept"),
                "query": q.strip(),
                "results": scoped_results,
            }

        return _TEMPLATES.TemplateResponse(
            request,
            "wiki_page.html",
            {
                "request": request,
                **_base_context(
                    request,
                    page_title=_label(page_path.stem if page_path.name != "_index.md" else page_path.parent.name),
                    active_domain=domain,
                    page_domain=page_domain or extra.get("domain"),
                ),
                **_wiki_context(page_path, content_body, **extra),
                "collection_material_cards": collection_material_cards,
                "collection_after_html": collection_after_html,
                "collection_material_thumbs": collection_material_thumbs,
                "concept_material_thumbs": concept_material_thumbs,
                "project_material_groups": project_material_groups,
                "project_collection": project_collection,
                "project_recent_html": project_recent_html,
                "project_notes": project_notes,
                "project_state_panel": project_state_panel,
                "project_phase_timeline": project_phase_timeline,
                "project_id": project_id,
                "page_search": page_search,
            },
        )

    @app.get("/source/{material_id}")
    def source(material_id: str):
        path = read_mod.material_source_path(material_id)
        if not path:
            raise HTTPException(status_code=404, detail="Source file not found")
        return FileResponse(path)

    @app.get("/extracted/{material_id}/text", response_class=HTMLResponse)
    def extracted_text(request: Request, material_id: str):
        path = read_mod.material_extracted_text_path(material_id)
        if not path:
            raise HTTPException(status_code=404, detail="Extracted text not found")
        return _TEMPLATES.TemplateResponse(
            request,
            "wiki_page.html",
            {
                "request": request,
                **_base_context(
                    request,
                    page_title=f"{_ui_label('extracted_text_for', str(read_mod.load_material_meta(material_id).get('domain') or ''), 'Extracted text')} · {material_id}",
                    page_domain=str(read_mod.load_material_meta(material_id).get("domain") or ""),
                ),
                "breadcrumbs": [{"label": _ui_label("home", str(read_mod.load_material_meta(material_id).get("domain") or ""), "Home"), "url": "/"}, {"label": _label(material_id), "url": f"/materials/{material_id}"}, {"label": _ui_label("extracted_text_for", str(read_mod.load_material_meta(material_id).get("domain") or ""), "Extracted text"), "url": f"/extracted/{material_id}/text"}],
                "content_html": render_wiki_markdown(path.read_text(encoding="utf-8"), f"extracted/{material_id}/text.md", material_id),
                "page_title": f"{_ui_label('extracted_text_for', str(read_mod.load_material_meta(material_id).get('domain') or ''), 'Extracted text')} · {material_id}",
                "wiki_path": f"extracted/{material_id}/text.md",
                "collection_url": "",
                "collection_label": "",
                "source_url": "",
                "extracted_text_url": "",
                "figures_url": "",
                "figure_count": 0,
                "no_figures": False,
                "related_materials": [],
                "page_search": None,
            },
        )

    return app
