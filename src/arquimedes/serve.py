"""FastAPI app for the Phase 8 web UI."""

from __future__ import annotations

import re
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit

import mistune
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from markupsafe import Markup

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


def material_url(material_id: str) -> str:
    return f"/materials/{material_id}"


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

    def _replace(match: re.Match[str]) -> str:
        attr, quote, value = match.groups()
        return f"{attr}={quote}{_rewrite_target(value, current_path, material_id)}{quote}"

    html = re.sub(r'(href|src)=(["\'])(.*?)\2', _replace, html)
    return Markup(html)


def _strip_material_sections(md_text: str) -> str:
    md_text = re.sub(r"(?ms)^## Figures\n.*?(?=^## |\Z)", "", md_text)
    md_text = re.sub(r"(?m)^\[(Open original file|Full extracted text)\]\(.*\)\s*$\n?", "", md_text)
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


def _material_page_context(material_id: str, body: str) -> dict:
    return {**_material_sidebar_context(material_id), "content_body": _strip_material_sections(body), "material_figures": _figure_view_models(material_id, read_mod.load_material_figures(material_id))}


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
    _TEMPLATES.env.globals.update(breadcrumbs=breadcrumbs, wiki_url=wiki_url, material_url=material_url)

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
        depth: int = 1,
        facet: list[str] = Query(default=[]),
        collection: str | None = None,
        limit: int = 20,
    ):
        result = None
        index_missing = False
        if q.strip():
            try:
                result = search_mod.search(q, depth=depth, facets=facet, collection=collection, limit=limit)
            except FileNotFoundError:
                index_missing = True
        return _TEMPLATES.TemplateResponse(
            request,
            "search.html",
            {"request": request, **_base_context(page_title="Search", query=q, depth=depth, facets=facet, collection=collection or "", result=result, index_missing=index_missing)},
        )

    @app.get("/materials/{material_id}", response_class=HTMLResponse)
    def material_page(request: Request, material_id: str):
        try:
            path = read_mod.material_wiki_path(material_id)
            body = read_mod.load_material_wiki(material_id)
            material = _material_page_context(material_id, body)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
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
    def wiki_page(request: Request, path: str):
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
        return _TEMPLATES.TemplateResponse(
            request,
            "wiki_page.html",
            {
                "request": request,
                **_base_context(page_title=_label(page_path.stem if page_path.name != '_index.md' else page_path.parent.name)),
                **_wiki_context(
                    page_path,
                    material["content_body"] if (material_id := read_mod.material_id_for_wiki_path(page_path)) and (material := _material_page_context(material_id, body)) else body,
                    **(material if material_id else {}),
                ),
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
            },
        )

    return app
