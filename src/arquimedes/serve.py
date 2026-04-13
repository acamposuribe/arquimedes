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

from arquimedes import freshness as freshness_mod
from arquimedes import read as read_mod
from arquimedes import search as search_mod
from arquimedes.index import get_index_path

_HERE = Path(__file__).resolve().parent
_TEMPLATES = Jinja2Templates(directory=str(_HERE / "templates"))
_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp"}


def _label(part: str) -> str:
    return part.replace("-", " ").replace("_", " ") or "wiki"


def wiki_url(path: str) -> str:
    rel = path.strip().lstrip("/")
    if rel.startswith("wiki/"):
        rel = rel[5:]
    if rel == "_index.md":
        return "/wiki"
    if rel.endswith("/_index.md"):
        return f"/wiki/{rel[:-10]}".rstrip("/")
    if rel.endswith(".md"):
        return f"/wiki/{rel[:-3]}".rstrip("/")
    return f"/wiki/{rel}".rstrip("/")


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
            return f"/figures/{material_id}/{name}"
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
            return f"/figures/{resolved.parts[1]}/{name}"
    return target


def render_wiki_markdown(md_text: str, current_path: str, material_id: str | None = None) -> Markup:
    html = mistune.html(md_text)
    html = re.sub(
        r"(<h2>Metadata</h2>\s*<table)(>)",
        r'\1 class="metadata-table"\2',
        html,
        count=1,
    )
    html = re.sub(
        r"(<h2>Related Materials</h2>\s*<ul)(>)",
        r'\1 class="related-materials-list"\2',
        html,
        count=1,
    )

    def _replace(match: re.Match[str]) -> str:
        attr, quote, value = match.groups()
        return f"{attr}={quote}{_rewrite_target(value, current_path, material_id)}{quote}"

    html = re.sub(r'(href|src)=(["\'])(.*?)\2', _replace, html)
    return Markup(html)


def _split_markdown_section(md_text: str, heading: str) -> tuple[str, str, str]:
    match = re.search(rf"(?ms)^## {re.escape(heading)}\n.*?(?=^## |\Z)", md_text)
    if not match:
        return md_text.strip(), "", ""
    before = md_text[:match.start()].strip()
    section = match.group(0).strip()
    after = md_text[match.end():].strip()
    return before, section, after


def _split_material_sections(md_text: str) -> tuple[str, str]:
    body = re.sub(r"(?ms)^## Figures\n.*?(?=^## |\Z)", "", md_text)
    body = re.sub(r"(?m)^\[(Open original file|Full extracted text)\]\(.*\)\s*$\n?", "", body)
    related_block = ""
    related = re.search(r"(?ms)^## Related Materials\n.*?(?=^## |\Z)", body)
    if related:
        related_block = related.group(0).strip()
        body = body[:related.start()] + body[related.end():]
    return body.strip(), related_block


def _strip_material_sections(md_text: str) -> str:
    body, related_block = _split_material_sections(md_text)
    md_text = body
    related = re.search(r"(?ms)^## Related Materials\n.*?(?=^## |\Z)", md_text)
    if related:
        related_block = related.group(0).strip()
        md_text = md_text[:related.start()] + md_text[related.end():]
        md_text = md_text.rstrip() + "\n\n" + related_block + "\n"
    return md_text.strip()


def _plain(value) -> str:
    if isinstance(value, dict):
        return str(value.get("value") or "")
    return str(value or "")


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
            "image_url": f"/figures/{material_id}/{name}",
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
    return {
        "material_id": material_id,
        "title": str(meta.get("title") or material_id),
        "collection_url": wiki_url(f"wiki/{meta.get('domain')}/{meta.get('collection')}/_index.md") if meta.get("domain") and meta.get("collection") else "",
        "collection_label": f"{meta.get('domain')}/{meta.get('collection')}" if meta.get("domain") and meta.get("collection") else "",
        "source_url": f"/source/{material_id}" if read_mod.material_source_path(material_id) else "",
        "extracted_text_url": f"/extracted/{material_id}/text" if read_mod.material_extracted_text_path(material_id) else "",
        "figures_url": f"/materials/{material_id}/figures" if figures else "",
        "figure_count": len(_figure_view_models(material_id, figures)),
        "page_thumbnails": _thumbnail_view_models(material_id),
        "no_figures": not figures,
    }


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
    content_body, related_materials_body = _split_material_sections(body)
    return {
        **_material_sidebar_context(material_id),
        "content_body": content_body,
        "related_materials_body": related_materials_body,
        "material_figures": _figure_view_models(material_id, read_mod.load_material_figures(material_id)),
    }


def _wiki_context(path: Path, body: str, *, material_id: str | None = None, title: str | None = None, **extra) -> dict:
    rel = path.relative_to(read_mod.get_project_root()).as_posix()
    return {
        "breadcrumbs": breadcrumbs(rel),
        "content_html": render_wiki_markdown(body, rel, material_id),
        "page_title": title or _label(path.stem if path.name != "_index.md" else path.parent.name),
        "wiki_path": rel,
        **extra,
    }


def _base_context(*, page_title: str, **extra) -> dict:
    collections = read_mod.list_domains_and_collections()
    return {"page_title": page_title, "nav_collections": collections, "nav_global_concepts": read_mod.list_glossary_concepts(), **extra}


def create_app(config: dict | None = None) -> FastAPI:
    app = FastAPI(title="Arquimedes")
    app.state.config = config or {}
    app.mount("/static", StaticFiles(directory=str(_HERE / "static")), name="static")
    _TEMPLATES.env.globals.update(
        breadcrumbs=breadcrumbs,
        wiki_url=wiki_url,
        material_url=material_url,
        truncate_words=truncate_words,
    )

    @app.exception_handler(404)
    async def not_found(request: Request, exc: HTTPException):
        return _TEMPLATES.TemplateResponse(
            request,
            "error.html",
            {"request": request, **_base_context(page_title="Not found", status_code=404, message=str(exc.detail or "Not found"))},
            status_code=404,
        )

    @app.get("/health")
    def health():
        return {"ok": True}

    @app.get("/api/freshness")
    def freshness():
        return JSONResponse(freshness_mod.workspace_freshness_status())

    @app.post("/update")
    def update():
        return JSONResponse(freshness_mod.update_workspace())

    @app.get("/", response_class=HTMLResponse)
    def home(request: Request):
        index_missing = not get_index_path().exists()
        return _TEMPLATES.TemplateResponse(
            request,
            "home.html",
            {"request": request, **_base_context(page_title="Arquimedes", index_missing=index_missing, recent_materials=read_mod.recent_materials(), collections=read_mod.list_domains_and_collections())},
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
    ):
        result = None
        index_missing = False
        if q.strip():
            try:
                result = search_mod.search(q, depth=depth, scope=scope, facets=facet, collection=collection, limit=limit)
            except FileNotFoundError:
                index_missing = True
        return _TEMPLATES.TemplateResponse(
            request,
            "search.html",
            {"request": request, **_base_context(page_title="Search", query=q, depth=depth, scope=scope, facets=facet, collection=collection or "", result=result, index_missing=index_missing)},
        )

    @app.get("/materials/{material_id}", response_class=HTMLResponse)
    def material_page(request: Request, material_id: str, q: str = "", depth: int = 3, scope: str = "all"):
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
            "page_search": {"kind": "material", "label": "Search This Material", "query": search_query, "results": None},
        }
        if search_query and search_scope == "all":
            try:
                search_context = _material_search_context(material_id, search_query, depth)
                search_context["search_scope"] = search_scope
                search_context["page_search"] = {"kind": "material", "label": "Search This Material", "query": search_query, "results": None}
            except FileNotFoundError:
                search_context = {
                    "search_query": search_query,
                    "search_depth": depth,
                    "search_scope": search_scope,
                    "search_hits": None,
                    "page_search": {"kind": "material", "label": "Search This Material", "query": search_query, "results": None},
                }
        return _TEMPLATES.TemplateResponse(
            request,
            "wiki_page.html",
            {
                "request": request,
                **_base_context(page_title=material["title"]),
                **_wiki_context(
                    path,
                    material["content_body"],
                    **material,
                    related_materials_html=render_wiki_markdown(material["related_materials_body"], path.relative_to(read_mod.get_project_root()).as_posix(), material_id) if material["related_materials_body"] else "",
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
                **_base_context(page_title=f"Figures · {meta.get('title') or material_id}"),
                "material_id": material_id,
                "figures": _figure_view_models(material_id, figures),
                "empty_message": "No extracted figures were found for this material." if not figures else "",
                "breadcrumbs": [{"label": "Home", "url": "/"}, {"label": _label(material_id), "url": f"/materials/{material_id}"}, {"label": "Figures", "url": f"/materials/{material_id}/figures"}],
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

    @app.get("/thumbnails/{material_id}/{filename}")
    def thumbnail_image(material_id: str, filename: str):
        if "/" in filename or "\\" in filename or Path(filename).suffix.lower() not in _IMAGE_EXTENSIONS:
            raise HTTPException(status_code=404, detail="Thumbnail not found")
        path = read_mod.material_thumbnail_path(material_id, filename)
        if not path:
            raise HTTPException(status_code=404, detail="Thumbnail not found")
        return FileResponse(path)

    @app.get("/wiki", response_class=HTMLResponse)
    def wiki_root(request: Request):
        try:
            path, body = read_mod.load_wiki_page("")
        except FileNotFoundError:
            listing = read_mod.list_wiki_dir("")
            return _TEMPLATES.TemplateResponse(
                request,
                "wiki_dir.html",
                {"request": request, **_base_context(page_title="Wiki", breadcrumbs=breadcrumbs("wiki/_index.md"), listing=listing)},
            )
        return _TEMPLATES.TemplateResponse(
            request,
            "wiki_page.html",
            {"request": request, **_base_context(page_title="Wiki"), **_wiki_context(path, body, title="Wiki", material_id=read_mod.material_id_for_wiki_path(path))},
        )

    @app.get("/wiki/{path:path}", response_class=HTMLResponse)
    def wiki_page(request: Request, path: str, q: str = "", depth: int = 3):
        try:
            page_path, body = read_mod.load_wiki_page(path)
        except FileNotFoundError:
            try:
                listing = read_mod.list_wiki_dir(path)
            except FileNotFoundError as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc
            return _TEMPLATES.TemplateResponse(
                request,
                "wiki_dir.html",
                {"request": request, **_base_context(page_title=_label(PurePosixPath(path).name), breadcrumbs=breadcrumbs(f"wiki/{path}/_index.md"), listing=listing)},
            )

        material_id = read_mod.material_id_for_wiki_path(page_path)
        page_search = None
        if material_id:
            material = _material_page_context(material_id, body)
            content_body = material["content_body"]
            extra = material
            extra["related_materials_html"] = render_wiki_markdown(material["related_materials_body"], page_path.relative_to(read_mod.get_project_root()).as_posix(), material_id) if material["related_materials_body"] else ""
            search_query = q.strip()
            page_search = {"kind": "material", "label": "Search This Material", "query": search_query, "results": None}
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
        page_record = read_mod.wiki_page_record(page_path)
        if page_path.name == "_index.md" and not material_id:
            try:
                wiki_root = read_mod.get_project_root() / "wiki"
                rel_parts = page_path.relative_to(wiki_root).parts
                if len(rel_parts) == 3 and "shared" not in rel_parts and "concepts" not in rel_parts:
                    coll_domain, coll_name = rel_parts[0], rel_parts[1]
                    before_materials, _, after_materials = _split_markdown_section(body, "Materials")
                    content_body = before_materials
                    collection_after_html = render_wiki_markdown(after_materials, page_path.relative_to(read_mod.get_project_root()).as_posix()) if after_materials else ""
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
                        "label": "Search This Collection",
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
                "label": "Search This Concept",
                "query": q.strip(),
                "results": scoped_results,
            }

        return _TEMPLATES.TemplateResponse(
            request,
            "wiki_page.html",
            {
                "request": request,
                **_base_context(page_title=_label(page_path.stem if page_path.name != "_index.md" else page_path.parent.name)),
                **_wiki_context(page_path, content_body, **extra),
                "collection_material_cards": collection_material_cards,
                "collection_after_html": collection_after_html,
                "collection_material_thumbs": collection_material_thumbs,
                "concept_material_thumbs": concept_material_thumbs,
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
                **_base_context(page_title=f"Extracted text · {material_id}"),
                "breadcrumbs": [{"label": "Home", "url": "/"}, {"label": _label(material_id), "url": f"/materials/{material_id}"}, {"label": "Extracted text", "url": f"/extracted/{material_id}/text"}],
                "content_html": render_wiki_markdown(path.read_text(encoding="utf-8"), f"extracted/{material_id}/text.md", material_id),
                "page_title": f"Extracted text · {material_id}",
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
