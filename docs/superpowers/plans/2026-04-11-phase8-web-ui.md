# Phase 8: Web UI — Implementation Plan

> **Status:** Proposed
> **Date:** 2026-04-11
> **Spec:** [Phase 8 web UI design](../specs/2026-04-11-phase8-web-ui-design.md)

## Goal

Implement `arq serve` as a local, collaborator-facing read UI over:

- the compiled wiki
- the SQLite search/memory layer
- extracted figures
- original source files

The UI should feel like a browser for the published knowledge base, not a second data model.

## Current Implementation Snapshot

Already in place:

- FastAPI, Uvicorn, and Jinja2 are already dependencies
- `config.yaml` already defines `serve.host` / `serve.port`
- `search.py` already provides `search()`, `find_related()`, `get_material_clusters()`, `get_collection_clusters()`
- `index.py` provides `ensure_index_and_memory()`
- compiled markdown pages exist under `wiki/` (material pages, concept pages, collection `_index.md`)
- extracted artifacts under `extracted/<id>/` include `meta.json`, `text.md`, `chunks.jsonl`, `figures/*.json`, `figures/*.{png,jpg}`

Still missing:

- `src/arquimedes/serve.py`
- `src/arquimedes/read.py`
- `src/arquimedes/freshness.py`
- template/static assets
- a markdown-to-HTML rendering dependency
- real `arq serve` wiring

Important current stubs:

- `arq serve` (cli.py:583)
- `arq read` (cli.py:317)
- `arq figures` (cli.py:324)

Phase 8 implements only `arq serve`, but factors reusable read helpers so the remaining CLI/MCP work later becomes trivial.

---

## File Map

| # | File | Action | Responsibility |
|---|------|--------|---------------|
| 1 | `pyproject.toml` | Modify | Add `mistune>=3.0` for markdown rendering; add `package-data` rules for templates/static |
| 2 | `src/arquimedes/read.py` | Create | Deterministic read helpers for wiki pages, material artifacts, figures, source-file resolution |
| 3 | `src/arquimedes/freshness.py` | Create | Shared collaborator-facing freshness/update helper: safe pull when applicable, then `index ensure` |
| 4 | `src/arquimedes/serve.py` | Create | FastAPI app factory, routes, markdown rendering + link rewrite, template wiring, update endpoint |
| 5 | `src/arquimedes/cli.py` | Modify | Replace `arq serve` stub with real server startup |
| 6 | `src/arquimedes/templates/base.html` | Create | Shared app shell, nav, freshness banner |
| 7 | `src/arquimedes/templates/home.html` | Create | Home/dashboard page |
| 8 | `src/arquimedes/templates/search.html` | Create | Search form + result rendering |
| 9 | `src/arquimedes/templates/wiki_page.html` | Create | Rendered wiki page wrapper |
| 10 | `src/arquimedes/templates/wiki_dir.html` | Create | Wiki directory listing |
| 11 | `src/arquimedes/templates/figures.html` | Create | Figure gallery |
| 12 | `src/arquimedes/templates/error.html` | Create | 404 / error page |
| 13 | `src/arquimedes/templates/partials/` | Create | Shared snippets: material card, breadcrumbs, freshness banner, search form |
| 14 | `src/arquimedes/static/style.css` | Create | App styling (classless base + small additions; no build system) |
| 15 | `src/arquimedes/static/app.js` | Create | Small JS for update button and async freshness check |
| 16 | `tests/test_read.py` | Create | Read helper and path-safety tests (ships with W8.1) |
| 17 | `tests/test_freshness.py` | Create | Freshness helper behavior tests (ships with W8.2) |
| 18 | `tests/test_serve.py` | Create | FastAPI route, rendering, and update-path tests (ships with W8.3+) |

## Dependency Order

```text
read.py + tests ──────┐
                      ├──► serve.py ───► cli.py
freshness.py + tests ─┘

templates/static ─► serve.py
```

`read.py` and `freshness.py` are independent of each other and should land first (parallelizable).

---

## Key Design Decisions

### Markdown rendering

Use `mistune>=3.0` (pure Python, fast, no C dependencies). Add to `pyproject.toml` dependencies. The renderer lives in `serve.py` as a module-level function that wiki routes call.

### Link rewriting

A post-render HTML pass (not a mistune plugin) that rewrites `href` and `src` attributes:

| Source pattern | Rewritten to |
|---|---|
| `wiki/.../*.md` (absolute) | `/wiki/.../...` (drop `.md`) |
| relative `*.md` links | resolve against current page path, then `/wiki/...` |
| `file://...` source links | `/source/{material_id}` |
| extracted-text links | `/extracted/{material_id}/text` |
| `figures/*.{png,jpg}` relative | `/figures/{material_id}/{filename}` |

This is a render-time transform. Canonical markdown files are never mutated.

### Figure image serving

Add a dedicated route `GET /figures/{material_id}/{filename}` that serves images from `extracted/{material_id}/figures/`. Path-constrained to image extensions only. This is simpler and safer than mounting `extracted/` as a static directory.

### CSS strategy

Use a small classless CSS base (~5KB, hand-written) that gives good typography defaults for rendered markdown. Add minimal custom CSS for layout (nav, search form, figure gallery grid, freshness banner). No CSS framework, no build system.

### Freshness banner

The freshness check is **not** blocking on page load. Instead:

1. The app shell template always renders a `<div id="freshness-banner">` placeholder.
2. On first page load per browser session, `app.js` fires an async `GET /api/freshness` request.
3. The response populates the banner: "up to date" / "update available" / "repo dirty — update blocked."
4. The **Update** button `POST /update` is also async — the banner updates when it completes.

This keeps page loads fast while making freshness visible.

### Home page data sources

- **Recent materials**: query SQLite `materials` table `ORDER BY added_at DESC LIMIT 10`
- **Domain/collection navigation**: `SELECT DISTINCT domain, collection FROM materials ORDER BY domain, collection`
- **Quick search**: a form that submits `GET /search?q=...`

If the SQLite index doesn't exist (first open before `arq index rebuild`), the home page shows a "run `arq index rebuild` first" message instead of crashing.

### Error handling

- 404 for missing materials, wiki pages, figure images
- Graceful degradation when SQLite index doesn't exist (helpful setup message, not a crash)
- `error.html` template for all error states

---

## Tasks

### W8.1 — Read helper layer + tests

**Creates:** `src/arquimedes/read.py`, `tests/test_read.py`

Deterministic helpers for web and later CLI/MCP reuse.

Core functions:

- `load_material_meta(material_id) -> dict` — reads `extracted/{id}/meta.json`
- `material_wiki_path(material_id) -> Path` — resolves via meta `domain` + `collection` + `{id}.md`
- `load_material_wiki(material_id) -> str` — reads the compiled wiki markdown
- `load_material_figures(material_id) -> list[dict]` — reads figure sidecar JSONs, skips malformed, returns sorted list
- `material_source_path(material_id) -> Path | None` — resolves `library_root / meta["source_path"]`
- `material_extracted_text_path(material_id) -> Path | None` — resolves `extracted/{id}/text.md`
- `list_wiki_dir(rel_path="") -> dict` — returns `{"dirs": [...], "pages": [...]}` for wiki browser
- `load_wiki_page(rel_path: str) -> tuple[Path, str]` — loads a wiki `.md` file; prefers `_index.md` for directories
- `list_domains_and_collections() -> list[dict]` — `SELECT DISTINCT domain, collection` from SQLite
- `recent_materials(limit=10) -> list[dict]` — recent materials from SQLite

Path safety rules:

- all paths must resolve inside allowed roots (`wiki/`, `extracted/`, `library_root`)
- reject `..` traversal after normalization
- return `None` or raise `FileNotFoundError` for missing artifacts

Tests (ship with this task):

- `test_load_wiki_page_rejects_traversal`
- `test_material_wiki_resolution`
- `test_load_material_figures_skips_malformed`
- `test_list_wiki_dir_structure`

### W8.2 — Freshness/update helper + tests

**Creates:** `src/arquimedes/freshness.py`, `tests/test_freshness.py`

Core functions:

- `workspace_freshness_status() -> dict` — inspect git state, return status dict
- `update_workspace() -> dict` — attempt pull + `ensure_index_and_memory()`

Behavior:

- not a git repo → skip pull, run `ensure_index_and_memory()`, return `repo_applicable=False`
- git repo, dirty worktree → skip pull, report `repo_dirty=True`, still run `ensure_index_and_memory()`
- git repo, clean, has upstream → attempt `git pull --ff-only`, then `ensure_index_and_memory()`
- never merge, rebase, stash, or do destructive recovery
- pull failure → report error, still run `ensure_index_and_memory()`

Return fields: `repo_applicable`, `repo_dirty`, `pull_attempted`, `pull_result`, `index_rebuilt`, `memory_rebuilt`, `message`, `checked_at`

Tests (ship with this task):

- `test_freshness_non_git_repo` — skips pull, runs index ensure
- `test_freshness_dirty_repo` — blocks pull, reports warning
- `test_freshness_clean_repo` — allows fast-forward
- `test_update_always_runs_index_ensure` — index ensure runs regardless of pull outcome

### W8.3 — FastAPI app + markdown rendering + link rewrite

**Creates:** `src/arquimedes/serve.py` (core), initial templates (`base.html`, `error.html`)

This is the app foundation. All later route tasks build on it.

Core pieces:

- `create_app(config=None) -> FastAPI` — app factory
- Jinja2 template environment with helpers: `breadcrumbs(path)`, `wiki_url(path)`, `material_url(id)`
- static file mount at `/static/`
- `render_wiki_markdown(md_text, current_path) -> str` — mistune render + link rewrite pass
- `GET /health` — liveness endpoint
- `GET /api/freshness` — returns JSON freshness status (calls `workspace_freshness_status()`)
- `POST /update` — calls `update_workspace()`, returns JSON status
- 404 handler with `error.html`

Link rewrite implementation:

- post-render pass using `re.sub` over `href="..."` / `src="..."` attributes
- applies the rewrite table from the design decisions section
- takes `current_page_path` as context for resolving relative links

### W8.4 — Search page

**In:** `serve.py`, `templates/search.html`, `partials/material_card.html`

Route: `GET /search`

- query params: `q`, `depth` (default 2), `facet` (repeatable), `collection`, `limit`
- calls `search.search()` with matching params
- renders material cards with title, summary, domain, collection, year, keywords
- at depth ≥ 2: shows chunk summaries, annotation highlights, figure thumbnails, concept hits
- renders canonical cluster hits as a separate section
- empty query shows the search form only
- missing index shows a helpful error

The material card partial (`partials/material_card.html`) is reused on the home page and search page.

### W8.5 — Material view + figure gallery + source streaming

**In:** `serve.py`, `read.py`, `templates/wiki_page.html`, `templates/figures.html`

Routes:

- `GET /materials/{material_id}` — renders compiled wiki page with chrome:
  - breadcrumbs
  - action links: open source, extracted text, figures, related materials
  - related materials block (calls `search.find_related()`)
- `GET /materials/{material_id}/figures` — figure gallery:
  - image (served via `/figures/` route)
  - figure id, source page, visual type, caption, description
- `GET /figures/{material_id}/{filename}` — serves image files from `extracted/{id}/figures/`
  - path-constrained to `.png`, `.jpg`, `.jpeg` extensions
  - rejects traversal
- `GET /source/{material_id}` — streams original file from `library_root`
  - resolves path via `read.material_source_path()`
  - sets `Content-Type` based on extension
  - rejects if path doesn't exist or escapes `library_root`
- `GET /extracted/{material_id}/text` — renders or serves `extracted/{id}/text.md`

### W8.6 — Home page + wiki browser

**In:** `serve.py`, `templates/home.html`, `templates/wiki_page.html`, `templates/wiki_dir.html`

Routes:

- `GET /` — home page:
  - freshness banner placeholder (populated by JS)
  - recent materials (from `read.recent_materials()`)
  - domain/collection navigation (from `read.list_domains_and_collections()`)
  - quick search form
  - if no index exists: show setup instructions instead
- `GET /wiki` — wiki root, delegates to `GET /wiki/`
- `GET /wiki/{path:path}` — wiki page or directory:
  - if path resolves to a file: render markdown with `wiki_page.html`
  - if path resolves to a directory with `_index.md`: render that page
  - if path resolves to a directory without `_index.md`: render directory listing with `wiki_dir.html`
  - breadcrumbs in all cases
  - 404 for non-existent paths

### W8.7 — CLI wiring + static assets + polish

**Modifies:** `src/arquimedes/cli.py`, `pyproject.toml`
**Creates:** `static/style.css`, `static/app.js`

CLI:

- replace `arq serve` stub with real startup
- honor `--host` / `--port` overrides, fall back to config values
- start Uvicorn against `create_app()`
- print the URL on startup

`pyproject.toml`:

- add `mistune>=3.0` to dependencies
- add `[tool.setuptools.package-data]` for `templates/**` and `static/**`

Static assets:

- `style.css`: classless base typography + layout for nav, search, figures grid, freshness banner, breadcrumbs, material cards
- `app.js`: async freshness check on load, update button handler

Do not implement `arq read` / `arq figures` in this phase.

### W8.8 — Route + rendering tests

**Creates/extends:** `tests/test_serve.py`

Uses FastAPI `TestClient` to test routes without starting a real server.

Required coverage:

| Test | What it covers |
|---|---|
| `test_health_endpoint` | `/health` returns 200 |
| `test_home_page_renders` | `/` returns HTML with expected sections |
| `test_home_page_no_index` | `/` degrades gracefully when no SQLite index |
| `test_search_renders_results` | `/search?q=...` renders material cards |
| `test_search_empty_query` | `/search` with no `q` shows form only |
| `test_material_page_renders` | `/materials/{id}` renders compiled wiki content |
| `test_material_page_404` | `/materials/nonexistent` returns 404 |
| `test_figure_gallery_renders` | `/materials/{id}/figures` shows figure data |
| `test_figure_image_serves` | `/figures/{id}/img.png` serves the image |
| `test_figure_image_rejects_traversal` | `/figures/{id}/../../etc/passwd` returns 404 |
| `test_source_streams_file` | `/source/{id}` serves the original file |
| `test_wiki_page_renders` | `/wiki/research/Archives/_index` renders HTML |
| `test_wiki_links_rewritten` | rendered HTML has `/wiki/...` links, not `.md` links |
| `test_update_endpoint` | `POST /update` returns structured status |
| `test_freshness_endpoint` | `GET /api/freshness` returns JSON |

---

## Implementation Order

```text
W8.1  read.py + tests          ┐  (parallel — no dependencies between them)
W8.2  freshness.py + tests     ┘
W8.3  serve.py app + markdown + link rewrite + base templates
W8.4  search page              ┐
W8.5  material view + figures  ├  (independent routes, any order after W8.3)
W8.6  home page + wiki browser ┘
W8.7  CLI wiring + static assets + polish
W8.8  route + rendering tests
```

Critical path: `read.py` → `serve.py` → routes → `cli.py`

W8.1 and W8.2 are parallelizable. W8.4–W8.6 are independent route implementations that can be done in any order after W8.3.

---

## Definition Of Done

- [ ] `arq serve` starts a working FastAPI app
- [ ] collaborators can browse the wiki tree in a browser
- [ ] collaborators can search with facets and depth controls
- [ ] material pages render correctly from compiled markdown
- [ ] figure galleries work with images served over HTTP
- [ ] original source files are reachable through safe local HTTP routes
- [ ] wiki markdown links are rewritten to valid app routes at render time
- [ ] update/freshness UX is visible, async, and non-destructive
- [ ] the update path runs `ensure_index_and_memory()`
- [ ] missing index/data states degrade gracefully (helpful messages, not crashes)
- [ ] path traversal is rejected on all file-serving routes
- [ ] all tests pass
- [ ] Phase 8 does not require Phase 7 MCP work to be useful
