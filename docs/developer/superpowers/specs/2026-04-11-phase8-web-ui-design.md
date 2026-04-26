# Arquimedes — Phase 8: Web UI Design

> **Status:** Proposed
> **Date:** 2026-04-11
> **Related specs:** [Full system design](2026-04-04-arquimedes-knowledge-system-design.md), [Phase 4 search index](../completed/specs/2026-04-04-phase4-search-index-design.md), [Phase 5 wiki compiler](../completed/specs/2026-04-05-phase5-wiki-compiler-design.md), [Phase 6 lint](../completed/specs/2026-04-05-phase6-lint-design.md), [Collection graph architecture](2026-04-09-collection-graph-design.md)
> **Plan:** [PLAN.md](../../PLAN.md)

## Purpose

Phase 8 gives collaborators a local, read-oriented web interface over Arquimedes.

It does not add a new semantic layer. It exposes the layers already produced by Phases 4-6:

- the compiled wiki in `wiki/`
- the local search + memory projection in SQLite
- the extracted figure and source artifacts

The goal is to make the published knowledge base browseable and searchable in a browser without forcing users into the CLI for every read action.

## Core Principle

Phase 8 is a **read surface**, not a second compiler and not a second search engine.

That means:

- wiki prose remains authored by the existing compile pipeline
- search results still come from `search.sqlite`
- the web app should compose existing deterministic outputs, not invent new ones
- freshness must be explicit and visible before read operations, not hidden behind a daemon

The web UI must also preserve the top-level separation between the two corpus domains:

- `Research`
- `Practice`

Domain switching is a browse concern, not a semantic rewrite. The UI should expose the two domains as first-class top-level tabs and keep wiki/search/navigation scoped to the selected domain wherever possible.

## Scope

Phase 8 covers four collaborator-facing jobs:

1. **Browse the wiki tree**
2. **Search the corpus with facets**
3. **View material pages and figure galleries**
4. **Run an explicit freshness/update path before read-heavy use**

Phase 8 does not wait for Phase 7 MCP work. If shared read helpers are needed, they should be introduced now as deterministic library code and reused later by MCP/CLI work.

## Current Starting Point

Already present:

- `fastapi`, `uvicorn`, and `jinja2` are already project dependencies
- `config.yaml` already has `serve.host` and `serve.port`
- `arq serve` exists in `cli.py` but is still a stub
- `arq read` and `arq figures` also still exist as stubs
- `search.py` already exposes:
  - `search()`
  - `find_related()`
  - `get_material_clusters()`
  - `get_collection_clusters()`
- `index.py` already exposes `ensure_index_and_memory()`
- the compiler already produces canonical markdown pages under `wiki/`
- extracted artifacts already exist under `extracted/<material_id>/`

Not yet present:

- no `serve.py`
- no `read.py`
- no `freshness.py`
- no templates/static assets
- no markdown-to-HTML rendering dependency
- no shared read/browse helper layer
- no freshness helper for web or future agent tools

## Design Principles

### Server-rendered, not SPA

Use FastAPI + Jinja2 with ordinary HTML pages and small amounts of JS only where needed.

Do not build a client-side app, API-heavy frontend, or bundler pipeline for this phase.

Reasons:

- the content is already mostly rendered markdown
- search is already deterministic and local
- the product need is browse/search, not collaborative editing
- this keeps the UI close to the existing system architecture

### Markdown rendering dependency

Use `mistune>=3.0` (pure Python, fast, no C dependencies) to render compiled wiki markdown into HTML.

The renderer belongs in the web layer, not in a shared core module. Markdown-to-HTML is a presentation concern.

### Wiki markdown is the visible source of truth

Compiled wiki pages are the canonical human-readable publication artifact.

The web app should render those markdown files into HTML rather than reimplement their content from raw JSON. The UI may add surrounding chrome, navigation, breadcrumbs, and metadata widgets, but it should not fork the page content model.

### Search comes from SQLite, not filesystem grep

Search results must come from the existing `search.py` / index layer.

This preserves:

- the same ranking model as the CLI
- the same memory-bridge traversals
- the same local/global cluster visibility
- the same staleness rules

### Freshness is explicit and non-blocking

Before heavy browse/search use, the UI should surface whether the local workspace is current.

Phase 8 should implement:

- an automatic freshness check on app open / first search in session
- an explicit **Update** action
- `ensure_index_and_memory()` after any successful or no-op repo update step

The freshness check must not block page rendering.

The app shell renders a placeholder banner on every page. On first load per browser session, a small JS fetch against `GET /api/freshness` populates the banner asynchronously. The **Update** button posts to `POST /update` and updates the banner when the response arrives.

Pages load fast; freshness is visible but never gates navigation.

This is not Phase 9 daemon sync. It is an explicit collaborator-facing freshness contract.

### Figure and source files are served through constrained routes

The UI must not rely on browser `file://` behavior.

Serve assets through explicit material-aware routes:

- source files through `/source/{material_id}`
- figure images through `/figures/{material_id}/{filename}`

Do not mount the whole repo or `extracted/` tree as a generic static directory.

### Graceful degradation over hard failure

If the SQLite index is missing or stale states cannot be checked, the UI should show a helpful message and keep browseable pages working where possible.

The app should not crash just because first-run setup has not happened yet.

## Execution Model

### `arq serve`

`arq serve` starts a FastAPI app bound to `serve.host` / `serve.port` unless overridden by CLI flags.

The app is read-only except for explicit freshness/update endpoints.

### Request flow

Normal usage:

1. user opens the app
2. the app checks workspace freshness state
3. the UI shows:
   - current/local-only
   - update available
   - update blocked
4. the first search in a browser session may trigger the same freshness check if the home page was skipped
5. the user may press **Update**
6. the update path runs freshness logic and then `ensure_index_and_memory()`

If freshness work fails, the user can still browse the current local state, but the UI must say so clearly.

## Canonical Data Sources

### Human-readable pages

- `wiki/**/*.md`

### Search and relation data

- `indexes/search.sqlite`
- `src/arquimedes/search.py`

### Material and figure source data

- `extracted/<material_id>/meta.json`
- `extracted/<material_id>/figures/*.json`
- `extracted/<material_id>/figures/*.{png,jpg,jpeg}`
- `extracted/<material_id>/text.md`

### Original source files

- `config["library_root"] / meta["source_path"]`

## Route Model

Minimum route set:

- `GET /`
  - home page
  - freshness banner / update CTA
  - high-level `Research` / `Practice` domain tabs beside the Arquimedes title
  - recent materials for the selected domain
  - domain-scoped collection navigation
  - quick links into wiki/search

- `GET /search`
  - query params mirror current CLI/search surface where useful:
    - `q`
    - `depth`
    - `facet`
    - `collection`
    - `limit`
    - `domain`
  - renders:
    - material cards
    - optional chunks/annotations/figures/concepts
    - canonical cluster hits
  - when a domain tab is active, search is restricted to that domain unless a narrower page-local scope already applies
  - if no index exists, renders a helpful setup state instead of crashing

- `GET /materials/{material_id}`
  - resolves the canonical compiled material wiki path
  - renders that page in HTML
  - adds top-level actions:
    - open source file
    - open extracted text
    - view figures
    - related materials

- `GET /materials/{material_id}/figures`
  - figure gallery for one material
  - uses figure sidecars + extracted images
  - shows caption, description, visual type, source page

- `GET /figures/{material_id}/{filename}`
  - serves extracted figure images from `extracted/{material_id}/figures/`
  - restricted to image extensions only

- `GET /wiki`
  - selected-domain wiki root browser

- `GET /wiki/{path:path}`
  - renders any markdown page under `wiki/`
  - directory paths should prefer `_index.md`
  - directories without `_index.md` may render a deterministic listing page
  - pages under `wiki/research/...` and `wiki/practice/...` should keep the matching domain tab active

- `GET /source/{material_id}`
  - read-only source-file streaming endpoint
  - serves the original PDF/image from `library_root`
  - replaces brittle `file://` browser behavior with local HTTP delivery

- `GET /extracted/{material_id}/text`
  - renders or downloads `extracted/<id>/text.md`

- `GET /api/freshness`
  - returns JSON freshness state for async banner population

- `POST /update`
  - explicit freshness/update action
  - returns structured status for the current UI session

- `GET /health`
  - simple liveness endpoint for tests/manual checks

## Markdown Rendering Contract

The web UI must render compiled markdown as HTML, but links inside the markdown need translation.

Link rewriting should happen as a post-render HTML pass over `href` and `src` attributes, not by mutating the source markdown files.

Required link handling:

- links to `wiki/.../*.md` must resolve to `/wiki/...`
- relative links between wiki pages must be resolved against the current page path and then mapped back into `/wiki/...`
- material page links should continue to work after markdown-to-HTML conversion
- `file://...` source links emitted by the compiler should be replaced with `/source/{material_id}` in the rendered material page chrome
- extracted-text links should map to `/extracted/{material_id}/text`
- figure image links inside material markdown should resolve to `/figures/{material_id}/{filename}`

The UI should not mutate canonical markdown files. Link rewriting is a render-time concern only.

## Freshness Contract

Phase 8 needs a shared deterministic helper that later phases can also reuse.

Suggested helper responsibilities:

- inspect whether the repo is under git
- inspect whether it has a configured upstream
- detect whether the worktree is dirty
- when safe and applicable, run a fast-forward-only pull
- always run `ensure_index_and_memory()` after the update/no-update repo step
- return a structured status object for UI display

Suggested behavior:

- if the repo is not a git checkout, skip pull and run `ensure_index_and_memory()`
- if the repo is dirty, do not pull; report blocked status and still allow manual refresh
- if the repo is clean and tracking info exists, allow `git pull --ff-only`
- never do merge, rebase, stash, or destructive recovery in the web UI

Suggested output fields:

- `repo_applicable`
- `repo_dirty`
- `pull_attempted`
- `pull_result`
- `index_rebuilt`
- `memory_rebuilt`
- `message`
- `checked_at`

## Read Helper Layer

Phase 8 should introduce deterministic helpers for read/browse work instead of embedding filesystem logic directly in FastAPI routes.

Suggested responsibilities:

- resolve a material id to:
  - `meta.json`
  - compiled wiki page path
  - extracted text path
  - figure sidecars/images
  - source file path
- enumerate wiki directories/pages safely
- load markdown page content
- normalize figure records for gallery rendering
- provide small read models for:
  - recent materials
  - domain/collection navigation
  - domain-scoped bridge/glossary navigation

This helper layer is intentionally Phase 8-safe and Phase 7-useful:

- web UI can use it now
- `arq read` / `arq figures` can reuse it later
- MCP tools can reuse it later

## UI Structure

### Home

Show:

- freshness banner
- `Research` / `Practice` tabs beside the Arquimedes title
- recent materials for the active domain
- active-domain collection entry points
- quick search form

Preferred data sources:

- recent materials from SQLite
- distinct domain/collection pairs from SQLite

If the index does not exist yet, the home page should render a setup/helpful empty state rather than fail.

### Search

Show:

- active domain tab state and explicit domain scope
- query box
- facet filters as plain form controls / chips
- material results
- cluster hits
- optional depth-2/3 evidence blocks

### Wiki page view

Show:

- active domain tab state
- breadcrumbs
- rendered markdown
- sidebar or header actions where available:
  - open source
  - extracted text
  - figures
  - related materials

### Figure gallery

Show:

- image
- figure id
- source page
- visual type
- caption
- description

### Wiki directory listing

When a wiki directory has no `_index.md`, the UI may render a deterministic directory listing page with:

- active domain tab state
- child directories
- child pages
- breadcrumbs

### Error page

The app should have a consistent HTML error surface for:

- missing wiki pages
- missing materials
- missing figures
- missing source files
- missing index-dependent search data

## Security and Safety Constraints

Phase 8 is local and read-oriented, but it still needs hard path boundaries.

Required constraints:

- only serve files under:
  - `wiki/`
  - `extracted/`
  - `library_root` via material-id lookup
- reject path traversal
- do not expose arbitrary filesystem browsing
- do not expose write endpoints except the explicit update action
- do not trigger LLM work
- image-serving routes must restrict filenames/extensions to expected image types

No auth is required for this phase. The target deployment is local/trusted collaborator use.

## Configuration

Already present:

```yaml
serve:
  host: "0.0.0.0"
  port: 8420
```

No additional Phase 8 configuration is required beyond host/port unless implementation proves otherwise.

## Non-Goals

- no editing of wiki pages
- no browser-side authoring tools
- no realtime websocket updates
- no daemonized background sync
- no auth/permissions system
- no separate JSON public API surface beyond what the app itself needs
- no requirement to finish Phase 7 MCP work first

## Verification

Minimum Phase 8 checks:

- `arq serve` starts and serves HTML
- the header exposes `Research` / `Practice` tabs and marks the active domain correctly
- `/`, `/wiki`, and `/search` keep navigation scoped to the selected domain
- `/search` returns the same search content the CLI would surface for equivalent inputs
- `/materials/{id}` renders the compiled material page
- `/wiki/...` browsing works across relative links
- `/materials/{id}/figures` shows extracted figure data and images
- `/figures/{id}/{filename}` serves images safely
- `/source/{id}` streams the original file safely
- `/api/freshness` reports current status for async UI updates
- the update path reports freshness state and runs `ensure_index_and_memory()`
- blocked/dirty repo states are visible and non-destructive
- missing index/data states degrade gracefully instead of crashing
- path traversal is rejected on all file-serving routes
