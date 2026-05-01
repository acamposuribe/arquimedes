"""FastAPI app for the Phase 8 web UI."""

from __future__ import annotations

import re
from pathlib import Path, PurePosixPath
from urllib.parse import urlencode, urlsplit

import mistune
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
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
    metadata = [
        {"label": "Año", "value": str(meta.get("year") or "").strip()},
        {"label": "Tipo", "value": material_type_label},
        {"label": "Proyecto", "value": str(meta.get("collection") or "").strip()},
    ]
    metadata = [item for item in metadata if item["value"]]
    if not (relevance or material_type or groups):
        return None
    return {
        "title": str(meta.get("title") or meta.get("material_id") or "").strip(),
        "material_type": material_type_label,
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


def _home_figure_tiles(domain: str, limit: int = 12) -> list[dict]:
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

        grouped.setdefault(type_key, []).append({
            "material_id": material_id,
            "title": str(meta.get("title") or material_id),
            "material_url": f"/materials/{material_id}",
            "summary": _plain(meta.get("summary")),
            "document_type": _plain(meta.get("document_type")) or str(meta.get("raw_document_type") or ""),
            "year": str(meta.get("year") or ""),
            "thumbnail_url": thumbnail_url,
            "preview_images": preview_images,
        })

    groups: list[dict] = []
    for type_key, items in grouped.items():
        items.sort(key=lambda x: x["title"].lower())
        variant = "gallery" if type_key in _PROJECT_GALLERY_TYPES else "list"
        groups.append({
            "type_key": type_key,
            "label": _PROJECT_MATERIAL_TYPE_LABELS.get(type_key, type_key),
            "variant": variant,
            "count": len(items),
            "items": items,
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


def _material_page_context(material_id: str, body: str) -> dict:
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
    }


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
    _TEMPLATES.env.globals.update(
        breadcrumbs=breadcrumbs,
        wiki_url=wiki_url,
        material_url=material_url,
        truncate_words=truncate_words,
        ui_label=_ui_label,
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
    def material_page(request: Request, material_id: str, q: str = "", depth: int = 3, scope: str = "all", domain: str = ""):
        try:
            path = read_mod.material_wiki_path(material_id)
            body = read_mod.load_material_wiki(material_id)
            material = _material_page_context(material_id, body)
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
        try:
            path, body = read_mod.load_wiki_page(active_domain)
        except FileNotFoundError:
            listing = read_mod.list_wiki_dir(active_domain)
            return _TEMPLATES.TemplateResponse(
                request,
                "wiki_dir.html",
                {
                    "request": request,
                    **_base_context(
                        request,
                        page_title=f"{_domain_label(active_domain)} Wiki",
                        active_domain=active_domain,
                        breadcrumbs=breadcrumbs(f"wiki/{active_domain}/_index.md"),
                        listing=listing,
                    ),
                },
            )
        return _TEMPLATES.TemplateResponse(
            request,
            "wiki_page.html",
            {
                "request": request,
                **_base_context(
                    request,
                    page_title=f"{_domain_label(active_domain)} Wiki",
                    page_domain=active_domain,
                ),
                **_wiki_context(path, body, title=f"{_domain_label(active_domain)} Wiki", material_id=read_mod.material_id_for_wiki_path(path)),
            },
        )

    @app.get("/wiki/{path:path}", response_class=HTMLResponse)
    def wiki_page(request: Request, path: str, q: str = "", depth: int = 3, domain: str = ""):
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
                        before_recent, recent_history, after_recent = _split_markdown_section(
                            content_body,
                            _heading_candidates("recent_additions", "Recent Additions"),
                        )
                        if recent_history:
                            content_body = before_recent
                            recent_tail = "\n\n".join(part for part in [recent_history, after_recent] if part)
                            collection_after_html = render_wiki_markdown(recent_tail, _project_rel_path(page_path))
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
