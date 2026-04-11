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
- `search.py` already provides search, relatedness, and cluster traversal
- compiled markdown pages already exist under `wiki/`

Still missing:

- `src/arquimedes/serve.py`
- template/static assets
- shared read helpers
- shared freshness/update helper
- real `arq serve` wiring

Important current stubs:

- `arq serve`
- `arq read`
- `arq figures`

Phase 8 should implement only `arq serve`, but it should factor reusable read helpers so the remaining CLI/MCP work later becomes trivial.

---

## File Map

| # | File | Action | Responsibility |
|---|------|--------|---------------|
| 1 | `pyproject.toml` | Modify | Add markdown-rendering dependency and package-data rules if needed for templates/static |
| 2 | `src/arquimedes/read.py` | Create | Deterministic read helpers for wiki pages, material artifacts, figures, source-file resolution |
| 3 | `src/arquimedes/freshness.py` | Create | Shared collaborator-facing freshness/update helper: safe pull when applicable, then `index ensure` |
| 4 | `src/arquimedes/serve.py` | Create | FastAPI app factory, routes, markdown rendering, template wiring, update endpoint |
| 5 | `src/arquimedes/cli.py` | Modify | Replace `arq serve` stub with real server startup |
| 6 | `src/arquimedes/templates/base.html` | Create | Shared app shell, nav, freshness banner |
| 7 | `src/arquimedes/templates/home.html` | Create | Home/dashboard page |
| 8 | `src/arquimedes/templates/search.html` | Create | Search form + result rendering |
| 9 | `src/arquimedes/templates/wiki_page.html` | Create | Rendered wiki page wrapper |
| 10 | `src/arquimedes/templates/figures.html` | Create | Figure gallery |
| 11 | `src/arquimedes/templates/partials/*.html` | Create | Shared snippets for cards, breadcrumbs, update status |
| 12 | `src/arquimedes/static/style.css` | Create | App styling |
| 13 | `src/arquimedes/static/app.js` | Create | Small JS for update button / first-search freshness behavior if needed |
| 14 | `tests/test_read.py` | Create | Read helper and path-safety tests |
| 15 | `tests/test_freshness.py` | Create | Freshness helper behavior tests |
| 16 | `tests/test_serve.py` | Create | FastAPI route, rendering, and update-path tests |

## Dependency Order

```text
read.py ───────┐
               ├──► serve.py ───► cli.py
freshness.py ──┘

templates/static ─► serve.py
tests ───────────► read.py + freshness.py + serve.py
```

`read.py` and `freshness.py` are independent and should land first.

---

## Tasks

### W8.1 Deterministic read helper layer

**Creates:** `src/arquimedes/read.py`

Implement deterministic helpers for web and later CLI/MCP reuse.

Core functions:

- `load_material_meta(material_id, config=None) -> dict`
- `material_wiki_path(material_id, config=None) -> Path`
- `load_material_wiki(material_id, config=None) -> str`
- `load_material_figures(material_id, config=None) -> list[dict]`
- `material_source_path(material_id, config=None) -> Path | None`
- `material_extracted_text_path(material_id, config=None) -> Path | None`
- `list_wiki_dir(rel_path="") -> dict`
- `load_wiki_page(rel_path: str) -> tuple[Path, str]`

Rules:

- all paths must stay inside allowed roots
- directory traversal must be rejected
- wiki browsing should prefer `_index.md` when a directory is requested
- figure records should ignore malformed sidecars safely

### W8.2 Freshness/update helper

**Creates:** `src/arquimedes/freshness.py`

Implement the shared collaborator-facing freshness contract.

Core functions:

- `workspace_freshness_status(config=None) -> dict`
- `update_workspace(config=None) -> dict`

Behavior:

- inspect whether git freshness is applicable
- detect dirty worktree
- if clean and applicable, attempt `git pull --ff-only`
- never merge, rebase, stash, or repair automatically
- always run `index.ensure_index_and_memory()` after the repo step
- return structured status for UI rendering

This helper should be written so Phase 7 tools can reuse it later.

### W8.3 FastAPI app shell

**Creates:** `src/arquimedes/serve.py`

Core pieces:

- `create_app(config=None) -> FastAPI`
- Jinja2 environment + template helpers
- static file mounting
- markdown renderer
- wiki-link rewrite helper

Required template helpers:

- breadcrumbs
- current freshness status
- wiki-path-to-route conversion
- material-id-to-route helpers

### W8.4 Wiki rendering + link rewrite

**In:** `serve.py`

Implement markdown rendering for compiled wiki pages.

Required transformations:

- relative wiki markdown links -> `/wiki/...`
- `wiki/.../*.md` links -> `/wiki/...`
- extracted-text links -> `/extracted/{material_id}/text`
- source-file links -> `/source/{material_id}`
- figure image links -> HTTP-served extracted image URLs

The app must render existing compiled markdown, not recompose page content from raw JSON.

### W8.5 Search page

**In:** `serve.py`, `templates/search.html`

Wire the existing `search.search()` surface into the web UI.

Requirements:

- GET-based query form
- depth selection
- facet filters
- collection filter
- material cards
- canonical cluster hits
- chunk/annotation/figure/concept evidence at depth 2/3

Keep the web search semantics aligned with the CLI output, not a new ranking model.

### W8.6 Material view + figure gallery

**In:** `serve.py`, `read.py`, `templates/wiki_page.html`, `templates/figures.html`

Implement:

- material page route by `material_id`
- figure gallery route
- extracted text route
- original source-file streaming route

Material page wrapper should expose:

- source-file action
- extracted-text action
- figure-gallery action
- related-materials block when available

### W8.7 Home and wiki browser

**In:** `serve.py`, `templates/home.html`, `templates/wiki_page.html`

Implement:

- home page
- wiki root browser
- arbitrary wiki-page rendering under `/wiki/{path:path}`
- breadcrumbs and directory navigation

Home page should show:

- freshness banner
- recent materials
- collection entry points
- search entry point

### W8.8 `arq serve` CLI wiring

**Modifies:** `src/arquimedes/cli.py`

Replace the stub with real startup.

Requirements:

- honor `--host` / `--port` overrides
- fall back to config values
- start Uvicorn against `create_app()`
- keep CLI output concise

Do not implement `arq read` / `arq figures` in this phase unless they fall out for free from the new helper layer.

### W8.9 Templates and visual system

**Creates:** templates + static assets

Constraints:

- no SPA build system
- no heavy JS framework
- clean typography and readable long-form wiki pages
- search results and figure gallery should work well on desktop and mobile
- one small JS file is acceptable for the update button and first-search freshness check

### W8.10 Tests

**Creates:** `tests/test_read.py`, `tests/test_freshness.py`, `tests/test_serve.py`

Required coverage:

- wiki path safety
- material/source/figure resolution
- markdown link rewrite
- search page rendering
- material page rendering
- figure gallery rendering
- source-file streaming
- update path behavior for:
  - non-git repo
  - clean repo
  - dirty repo
  - pull blocked/failed

---

## Implementation Order

```text
W8.1  read.py
W8.2  freshness.py
W8.3  serve.py app shell
W8.4  markdown rendering + link rewrite
W8.5  search page
W8.6  material view + figures + source streaming
W8.7  home + wiki browser
W8.8  cli.py wiring
W8.9  templates/static polish
W8.10 tests
```

The critical path is `read.py` -> `serve.py` -> `cli.py`.

---

## Test Plan

| # | Test | What it covers |
|---|------|---------------|
| 1 | `test_load_wiki_page_rejects_traversal` | read helper does not escape `wiki/` |
| 2 | `test_material_wiki_resolution` | material id resolves to the compiled wiki page path |
| 3 | `test_load_material_figures` | gallery loader returns valid figure records only |
| 4 | `test_rendered_wiki_links_are_rewritten` | markdown links become valid app routes |
| 5 | `test_search_page_renders_results` | `/search` renders SQLite-backed results |
| 6 | `test_material_page_route` | `/materials/{id}` renders compiled wiki content |
| 7 | `test_source_route_streams_original_file` | source-file endpoint serves the expected file safely |
| 8 | `test_update_workspace_non_git` | update falls back to `index ensure` only |
| 9 | `test_update_workspace_dirty_repo` | dirty repo blocks pull and reports warning |
| 10 | `test_update_workspace_clean_repo` | clean repo can fast-forward and then ensure index/memory |

---

## Definition Of Done

- [ ] `arq serve` starts a working FastAPI app
- [ ] collaborators can browse the wiki tree in a browser
- [ ] collaborators can search with facets and depth controls
- [ ] material pages render correctly from compiled markdown
- [ ] figure galleries work
- [ ] original source files are reachable through safe local HTTP routes
- [ ] update/freshness UX is visible and non-destructive
- [ ] the update path runs `arq index ensure`
- [ ] Phase 8 does not require Phase 7 MCP work to be useful
