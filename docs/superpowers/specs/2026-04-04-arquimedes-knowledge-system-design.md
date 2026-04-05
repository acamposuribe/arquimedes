# Arquimedes — LLM Knowledge Base for Architecture

## Context

An architect and architecture professor needs a collaborative knowledge base that:
- Ingests raw materials (PDFs, images, documents) from a shared iCloud folder
- Extracts structured content using LLM + deterministic parsing
- Compiles a navigable markdown wiki organized by practice and research domains
- Provides token-efficient search for human and agent use
- Runs across devices for multiple collaborators with minimal friction

The system is inspired by Karpathy's LLM knowledge base pattern: raw data collected, compiled by LLM into a wiki, operated on by CLI tools and agents, viewable in a browser. The key difference is this is collaborative, domain-specific (architecture), and agent-first.

For the practical end-to-end operating flow, including what happens when a collaborator adds a file and which steps use an LLM, see `docs/PIPELINE.md`.

For the original conceptual pattern, see `docs/llm-wiki.md`. That file is the local reference for the source idea. This spec defines how Arquimedes instantiates and extends that pattern for architecture practice and research, with explicit provenance, deterministic extraction, multimodal materials, collaboration, and a future server-maintainer daemon.

For how Arquimedes should evolve from a searchable archive into a connected memory system before the wiki compiler exists, see `docs/superpowers/specs/2026-04-05-connection-model.md`. That note explains how structural, semantic, retrieval, attention, and materialized connections should emerge across phases.

For the post-compile bridge that makes the wiki graph queryable for agents, see `docs/superpowers/specs/2026-04-05-phase5-5-memory-bridge-design.md`. That phase turns the readable semantic graph into a machine-queryable memory layer inside SQLite.

The long-term operating model is an LLM-maintained wiki. In Arquimedes, the future **server agent** is that maintainer. It is responsible for ingesting new sources, enriching them, compiling and updating wiki pages, running health checks, and keeping indexes current. Semantic publication belongs to that server-maintainer path: clustering and wiki compilation are not collaborator responsibilities. Collaborator machines rebuild only deterministic local query layers from already-committed outputs. This maintainer role is assembled progressively:
- **Wiki compilation** defines what the maintainer writes and updates
- **Wiki linting** defines what the maintainer checks and improves
- **The server daemon** makes that maintenance loop always-on and automatic

Current project docs such as `CLAUDE.md` describe how to build Arquimedes itself. A separate operational instruction file for the server maintainer belongs with the daemon rollout, once compile/lint behavior is implemented and stable.

## Architecture Overview

**Three deployment contexts:**
1. **Server (Mac Mini)** — always-on, watches iCloud folder, auto-ingests/extracts/compiles, commits + pushes
2. **Admin (you)** — full CLI access, manages system, runs manual commands
3. **Collaborators** — drop files into iCloud, search via web UI or their own agents, auto-pull for updates

**Data flow:**
```
iCloud folder (shared)
  → Server agent detects new files (FSEvents)
  → arq ingest (register in manifest)
  → arq extract (deterministic extraction + LLM enrichment)
  → arq index rebuild (SQLite FTS5 over extracted/enriched artifacts)
  → arq cluster (canonical concept clustering)
  → arq compile (wiki generation; auto-runs arq memory rebuild)
  → git commit + push (extracted/, wiki/, manifests/)
  → Collaborators auto-pull (arq sync daemon)
  → arq index ensure (rebuild index locally if stale; auto-runs arq memory ensure)
  → Search via web UI, CLI, or MCP tools
```

## Repo Structure

```
arquimedes/
  config/
    config.yaml                      # defaults: library_root, LLM settings, extraction params
    config.local.yaml                # gitignored, per-device overrides
    config.template.yaml             # committed template for new device setup
  src/arquimedes/
    __init__.py
    cli.py                           # `arq` Click entrypoint
    config.py                        # config loading (yaml + local overrides)
    ingest.py                        # scan library, register new materials
    extract_pdf.py                   # deterministic PDF extraction: text, pages, TOC, tables
    extract_figures.py               # figure extraction: embedded images + page rasterization/region detection
    extract_image.py                 # standalone image file extraction: metadata, description
    classify.py                      # deterministic classifiers: TF-IDF keywords, document type
    enrich.py                        # LLM enrichment: summaries, keywords, facets
    index.py                         # SQLite FTS5 build + query
    search.py                        # search interface (card → chunk → deep)
    compile.py                       # wiki generation from extracted data
    lint.py                          # wiki health checks
    watch.py                         # file watcher daemon (fsevents/poll + debounce)
    sync.py                          # auto-pull daemon for collaborators
    serve.py                         # FastAPI web UI server
    mcp_server.py                    # MCP tool wrapper
    models.py                        # data models (Material, Chunk, Figure, etc.)
  manifests/
    materials.jsonl                  # registry of all known materials (committed)
  extracted/
    <material_id>/
      meta.json                      # document-level metadata + facets
      text.md                        # full extracted text
      pages.jsonl                    # page-level text + anchors
      annotations.jsonl              # reader highlights, notes, marks (from PDF annotations)
      chunks.jsonl                   # retrieval-sized chunks with summaries
      figures/
        fig_001.png
        fig_001.json                 # caption, page, visual description, visual type
      tables.jsonl                   # structured table data
      toc.json                       # headings / table of contents
      concepts.jsonl                 # LLM-identified concept candidates (from enrichment)
      thumbnails/
        page_001.png                 # page thumbnails for web UI browsing
  indexes/
    search.sqlite                    # FTS5 index (gitignored, rebuilt locally)
  wiki/
    practice/
      regulations/
      materials/
      precedents/
      technical/
    research/
      papers/
      lectures/
      theory/
    shared/
      concepts/                      # cross-domain concept pages
      glossary/
    _index.md                        # auto-maintained master index
  web/
    static/                          # CSS, JS, icons
    templates/                       # Jinja2 templates for web UI
  .obsidian/                         # optional Obsidian config for wiki/ viewing
  pyproject.toml
  .gitignore
  .gitattributes                     # LFS config (for large extracted figures if needed)
```

## Material Identity

- `material_id` = `sha256(file_contents)[:12]` — deterministic, path-independent
- Two collaborators ingesting the same file produce the same ID (deduplication)
- Manifest entry stores: material_id, file_hash, relative_path (within iCloud folder), file_type (pdf | image | scanned_document), domain (practice | research), collection, ingested_at, ingested_by

## Domain & Collections

### Domain (top-level folder)

LIBRARY_ROOT **must** contain two top-level folders: `Research/` and `Practice/`. Every material must live inside one of them — files outside these folders are skipped during ingest.

- **Domain is folder-derived**: `LIBRARY_ROOT/Research/paper.pdf` → domain `research`. `LIBRARY_ROOT/Practice/code.pdf` → domain `practice`. No classifier needed, no ambiguity.
- **No "both"**: the folder placement is an editorial decision. If you're studying a building code academically, file it under Research.
- **Search spans both**: `arq search "thermal mass"` queries the unified index across both domains. Agents can filter: `arq search --facet domain=research "thermal mass"`.

### Collections (second-level subfolder)

Collections let collaborators scope work to a subset of materials — useful for journal articles, studio projects, course prep, etc.

- **Derived from second-level subfolders** within the domain folder: `LIBRARY_ROOT/Research/thermal-mass/paper.pdf` → domain `research`, collection `thermal-mass`
- **No manual tagging**: just organize files in folders. The ingest step reads the relative path and assigns domain + collection.
- **Direct files**: materials directly inside `Research/` or `Practice/` (no subfolder) get collection `_general`
- **Filter, not silo**: collections are a search filter. All materials remain in the global index and are findable without specifying a collection.
- **Search scoping**: `arq search --collection thermal-mass "Mediterranean climate"` searches only that subset
- **Agent use**: collaborators can tell their agent "search collection thermal-mass" to focus on project-relevant materials

## Configuration

```yaml
# config.yaml (committed defaults)
library_root: "~/Arquimedes-Library"
llm:
  agent_cmd:                      # legacy fallback; stage routes take precedence
    - "claude --print"
    - "codex exec"
enrichment:
  llm_routes:
    document:
      - provider: codex
        command: "codex exec"
        model: gpt-5.4-mini
        effort: high
      - provider: claude
        command: "claude --print"
        model: sonnet
        effort: medium
      - provider: copilot
        command: "copilot"
        agent: copilot-no-tools-json
        model: gpt-4.1
        silent: true
        no_ask_user: true
        no_auto_update: true
        no_custom_instructions: true
        allow_all: false
    chunk:
      - provider: copilot
        command: "copilot"
        agent: copilot-no-tools-json
        model: gpt-4.1
        silent: true
        no_ask_user: true
        no_auto_update: true
        no_custom_instructions: true
        allow_all: false
    figure:
      - provider: codex
        command: "codex exec"
        model: gpt-5.4-mini
        effort: medium
      - provider: copilot
        command: "copilot"
        agent: copilot-no-tools-json
        model: gpt-4o
        silent: true
        no_ask_user: true
        no_auto_update: true
        no_custom_instructions: true
        allow_all: false
    cluster:
      - provider: claude
        command: "claude --print"
        model: sonnet
        effort: medium
      - provider: codex
        command: "codex exec"
        model: gpt-5.4-mini
        effort: high
      - provider: copilot
        command: "copilot"
        agent: copilot-no-tools-json
        model: gpt-4.1
        silent: true
        no_ask_user: true
        no_auto_update: true
        no_custom_instructions: true
        allow_all: false
extraction:
  chunk_size: 500            # tokens per chunk
  generate_thumbnails: true
  ocr_fallback: true
watch:
  backend: fsevents           # fsevents (macOS native) or poll (fallback)
  poll_interval: 30           # seconds between scans (poll backend only)
  debounce_window: 10         # seconds to wait for batch to settle
  batch_commit: true          # commit all ingested materials in one commit
sync:
  pull_interval: 300         # seconds between git pull (collaborator mode)
  auto_start: false          # collaborators opt-in to auto-pull
  auto_index: true           # run `arq index ensure` after each pull
```

`config.local.yaml` overrides any value (gitignored). `config.template.yaml` is committed with placeholder comments.

## Extraction Pipeline

Extraction is split into two explicit stages: **raw extraction** (deterministic, no LLM) and **enrichment** (LLM-dependent). This separation means re-parsing PDFs is never needed when re-enriching with a different model or prompt version.

### Stage 1: Raw Extraction (`arq extract-raw`)

Deterministic parsing — no LLM calls. Produces canonical artifacts:

**Document level** (`meta.json` — raw fields only):
- title (from PDF metadata or first heading), authors, year, language, source_url, page_count
- file_hash, material_id, source_path, ingested_at
- file_type: pdf | image | scanned_document (detected during extraction)
- domain: practice | research — derived from top-level LIBRARY_ROOT folder (not classified, folder-authoritative)
- **Deterministic classification** (no LLM, stored as plain values — no provenance needed):
  - raw_keywords: TF-IDF keyword extraction (unigrams + bigrams, stop-word filtered, soft-hyphen aware)
  - raw_document_type: rule-based using text + filename signals → regulation | paper | catalogue | ... | "" (ambiguous, defer to LLM)

**Page/section level** (`pages.jsonl`):
- page_number, raw text, headings, section_boundaries
- figure_refs, table_refs
- thumbnail_path

**Annotations** (`annotations.jsonl`) — for PDFs with reader marks:
- Extracted via PyMuPDF annotation API (highlights, sticky notes, text annotations, underlines, strikeouts)
- Each annotation stores: type (highlight | note | underline | strikeout | freetext), page, quoted_text (the highlighted/annotated span), comment (user's note text if any), color, rect
- **User-emphasized spans**: pages with annotations are flagged with `has_annotations: true` and `annotation_ids` (linking back to annotations.jsonl); chunks that overlap annotated spans are flagged as `emphasized: true`. Both signals tell the enrichment step that these sections carry reader-assigned importance
- Annotations are searchable and surfaced in the wiki material pages

**Chunk level** (`chunks.jsonl`):
- chunk_id, text (~500 tokens), source_pages
- emphasized: boolean — true if chunk overlaps with annotated/highlighted spans
- (no summaries yet — those come from enrichment)

**Figures** (`figures/`):
- **Dual extraction strategy** for architecture PDFs:
  1. Embedded image extraction via PyMuPDF (photos, raster images)
  2. Page rasterization + region detection/cropping for vector drawings, composite layouts, and page-native diagrams that are not clean "images"
- Each figure gets a JSON sidecar with deterministic fields: source_page, bbox, extraction_method (embedded | rasterized_region)
- Visual type classification and descriptions are deferred to enrichment

**Tables** (`tables.jsonl`):
- Extracted via pdfplumber
- Structured as arrays of rows, with headers identified
- Source page reference

**TOC** (`toc.json`):
- Headings and structure from PDF outline / parsed headings

### Image files (JPG, PNG, TIFF, etc.)

Not all materials are PDFs. Image files are handled differently based on their nature:

**Scanned documents** (diaries, handwritten notes, scanned pages):
- Detected heuristically (high text density after OCR, page-like aspect ratio) or via user hint in the folder structure (e.g., `LIBRARY_ROOT/scanned/`)
- OCR via PyMuPDF or Tesseract to extract text
- Treated like single-page PDFs after OCR: text goes to `text.md`, page to `pages.jsonl`
- Original image preserved in `figures/`

**Project/inspiration images** (photos, renders, sketches):
- No OCR — visual content only
- Image stored in `figures/` with a JSON sidecar
- Description and visual_type deferred to enrichment
- Minimal `meta.json` with raw fields (title from filename, no text)

The ingest step records `file_type` in the manifest. The extract-raw step dispatches to the appropriate handler based on file type and detection heuristics.

### Stage 2: Enrichment (`arq enrich`)

LLM-dependent. Reads raw extraction artifacts and adds semantic metadata. Every enriched field carries provenance:

```json
{
  "value": "A comprehensive study of thermal mass in Mediterranean climate...",
  "provenance": {
    "source_pages": [1, 2, 3],
    "evidence_spans": ["This paper examines...", "In the Mediterranean context..."],
    "model": "claude-sonnet-4-6",
    "prompt_version": "enrich-v1.2",
    "confidence": 0.92,
    "enriched_at": "2026-04-04T14:30:00Z"
  }
}
```

**Annotation-aware enrichment**:
- When annotations exist, the enrichment prompt includes highlighted/noted spans as priority context
- Summaries weight annotated sections more heavily — "the reader considered these passages important"
- Keywords are partially derived from annotated spans
- Annotations with user notes are treated as first-class insights and surfaced in the material's wiki page

**Document-level enrichment** (added to `meta.json`):
- document_type: regulation | catalogue | monograph | paper | lecture_note | precedent | technical_spec | site_document (refines or fills raw_document_type when deterministic pass returned "")
- summary: ~200 words, with provenance (weighted toward annotated sections when present)
- keywords: 5-15 terms, with provenance (refines raw_keywords — may reorder, add domain-specific terms, or remove noise)
- Architecture facets (each with provenance):
  - building_type, scale (detail | building | urban | territorial)
  - location, jurisdiction, climate
  - program, material_system, structural_system
  - historical_period, course/topic, studio/project

**Chunk-level enrichment** (added to `chunks.jsonl`):
- summary: one-line summary per chunk, with provenance
- keywords: extracted terms, with provenance

**Figure enrichment** (added to figure JSON sidecars):
- visual_type: plan | section | elevation | detail | photo | diagram | chart | render | sketch
- description: LLM-generated visual description, with provenance
- caption: extracted or inferred caption

**Concept candidates** (new file: `concepts.jsonl`):
- LLM-identified concepts that this material contributes to
- These should be rich, reusable concept phrases: broad enough to connect materials across the corpus, but specific enough to retain analytical meaning
- Used by the wiki compiler to build/update concept pages

### Why this split matters:
- **Re-enrichment is cheap**: change your prompt or switch models without re-parsing PDFs
- **Provider flexibility**: use Claude today, Gemini tomorrow, compare results
- **Provenance for integrity**: regulations, teaching, and research need traceable sources
- **Incremental**: raw extraction and enrichment each track their own completion state

## Search & Retrieval

### Token-efficient 3-layer retrieval:

**Layer 1 — Card search** (~100 tokens per result):
- Query SQLite FTS5 over: title, summary, keywords, facets
- Returns material cards: material_id, title, summary snippet, domain, type
- Typical: 10-20 cards returned

**Layer 2 — Chunk search** (~50 tokens per result):
- Query chunk summaries within specific materials
- Returns: chunk_id, one-line summary, source pages
- Typical: 5-15 chunks returned

**Layer 3 — Deep read** (~500 tokens per result):
- Read full chunk text, figures, or tables
- Only the specific content needed

**Typical query cost**: ~5K tokens to answer a specific question (vs 50K+ for a whole PDF).

### Search index (SQLite FTS5):
- `materials` table: material_id, title, summary, keywords, file_type, all facet fields
- `chunks` table: chunk_id, material_id, summary, text, page_refs, emphasized
- `figures` table: figure_id, material_id, description, visual_type, caption
- `annotations` table: annotation_id, material_id, type, quoted_text, comment, page
- Gitignored — rebuilt locally via `arq index rebuild` or `arq index ensure` (auto-rebuild if stale)

### Optional future: embeddings for semantic search (e.g., "buildings that breathe like skin"). Not needed at launch.

## Wiki Compiler

### Generated content:

**Material pages** — one per document:
- Summary, key findings, extracted figures, tables
- Reader annotations section: highlighted passages and user notes (when present)
- Links to related materials (by shared concepts/facets)
- Citation info, link to original file
- Domain tags, facets

**Concept pages** — auto-discovered topics spanning multiple materials:
- e.g., "Thermal Mass", "Le Corbusier", "Passivhaus", "Barcelona Building Code"
- Aggregates what the knowledge base knows, with citations
- Lives in `wiki/shared/concepts/`

**Index pages** — auto-maintained:
- `_index.md` per directory with document counts, recent additions
- Master `_index.md` with stats, topic map, recent activity

### Wiki organization:
```
wiki/
  practice/
    regulations/       # building codes, standards, zoning
    materials/         # material catalogues, product specs
    precedents/        # project monographs, case studies
    technical/         # construction details, structural specs
  research/
    papers/            # academic papers, journal articles
    lectures/          # lecture notes, course materials
    theory/            # theoretical texts, criticism
  shared/
    concepts/          # cross-domain concept pages
    glossary/          # terminology definitions
  _index.md
```

### Compilation:
- `arq compile` — incremental, only recompiles affected pages
- `arq compile --full` — full rebuild
- Server agent runs incremental compile after each extraction

### Wiki ownership model:

The wiki is a published semantic artifact owned by the compiler/server maintainer, not a free-edit collaboration layer.

**Currently maintainer-owned pages:**
- all material pages under `wiki/practice/**` and `wiki/research/**`
- all concept pages under `wiki/shared/concepts/`
- all generated glossary and `_index.md` pages

These pages are kept current by the semantic publication pipeline:
`cluster -> compile -> memory rebuild`

Collaborators may read, search, and cite the wiki, but they should not treat these generated pages as hand-edited working documents. Their local responsibility is to rebuild deterministic machine layers (`index ensure`, integrated memory ensure), not to republish semantic structure.

### Link format:
Standard markdown links: `[Thermal Mass](../shared/concepts/thermal-mass.md)`. No Obsidian-specific syntax. Compatible with both web UI and Obsidian.

## Wiki Linting & Health Checks

`arq lint` is not just a broken-link checker — it's an LLM-driven health system that actively improves the knowledge base over time. It runs a suite of checks and produces actionable reports.

### Deterministic checks (no LLM):
- **Broken links**: wiki pages referencing materials, concepts, or files that don't exist
- **Orphaned materials**: extracted materials with no wiki page
- **Missing metadata**: materials lacking required facets (domain, document_type, etc.)
- **Stale enrichment**: materials where any enrichment stage stamp differs from current config (prompt_version, model, enrichment_schema_version) or where input_fingerprint has changed since last enrichment (see [Phase 3 enrichment spec](2026-04-04-phase3-enrichment-design.md))
- **Index drift**: search index out of sync with extracted data
- **Duplicate materials**: different manifest entries pointing to the same content hash

### LLM-driven checks:
- **Inconsistent data**: contradictory claims across materials (e.g., conflicting regulation interpretations, different dates for the same event)
- **Missing connections**: materials that should reference each other but don't (shared authors, overlapping topics, cited works that exist in the library)
- **Concept candidates**: topics mentioned across multiple materials that don't have a concept page yet
- **Impute missing data**: suggest values for empty facets using web search + material content (e.g., infer jurisdiction from a regulation's text, identify building_type from a monograph's images)
- **Research questions**: suggest further questions worth investigating based on gaps and patterns in the knowledge base
- **Coverage gaps**: identify under-represented areas (e.g., "you have 15 materials on concrete but none on timber construction")

### Output:
- `arq lint` → prints a report to stdout (JSON or human-readable)
- `arq lint --report` → writes a detailed report to `wiki/_lint_report.md`
- `arq lint --fix` → auto-applies deterministic fixes (rebuild index, update broken links) and queues LLM suggestions for review
- Each LLM suggestion includes provenance (which materials, what evidence, confidence)

### Integration with the server agent:
The watcher can optionally run `arq lint --quick` (deterministic checks only) after each compile, and `arq lint --full` (including LLM checks) on a scheduled basis (e.g., weekly).

## Agent Tool Layer

### CLI (`arq` command):

| Command | Description |
|---------|-------------|
| `arq ingest [path]` | Scan library (or specific path) for new materials |
| `arq extract-raw [material_id]` | Deterministic extraction (text, pages, figures, tables, annotations, OCR) |
| `arq enrich [material_id]` | LLM enrichment (summaries, facets, descriptions) |
| `arq extract [material_id]` | Convenience: runs extract-raw + enrich |
| `arq search <query>` | Card-level search |
| `arq search --deep <query>` | Multi-layer auto-drill search |
| `arq search --facet domain=practice --facet scale=building <query>` | Faceted search |
| `arq search --collection thermal-mass-paper <query>` | Search within a collection |
| `arq read <material_id>` | Full extracted content |
| `arq read <material_id> --page 5` | Specific page |
| `arq figures <material_id>` | List figures with descriptions |
| `arq compile [--full]` | Generate/update wiki and rebuild memory bridge |
| `arq lint` | Run all health checks (deterministic + LLM) |
| `arq lint --quick` | Deterministic checks only (fast, no LLM) |
| `arq lint --report` | Write detailed report to wiki/_lint_report.md |
| `arq lint --fix` | Auto-fix deterministic issues, queue LLM suggestions |
| `arq index rebuild` | Rebuild search index from scratch |
| `arq index ensure` | Rebuild index only if stale (auto-check) |
| `arq memory rebuild` | Deterministically project the cluster/wiki graph into SQLite |
| `arq memory ensure` | Rebuild the local graph bridge only if stale |
| `arq watch` | Start file watcher daemon (server) |
| `arq sync` | Start auto-pull daemon (collaborator) |
| `arq serve` | Start web UI |
| `arq status` | System stats, recent additions |

### MCP Server:

Wraps the same Python functions. Tools: `search`, `deep_search`, `read_material`, `read_page`, `list_figures`, `list_annotations`, `compile`, `ingest`, `status`. Any MCP-compatible agent (Claude, OpenAI, etc.) can connect.

For collaborator-facing use, these tools should eventually follow a freshness-on-read contract: before querying the local knowledge base, check for newer published repo state when applicable and run `arq index ensure` so search and memory remain current without requiring the collaborator to think about sync manually.

### Universal access:
- CLI works from any agent via shell (Codex, Gemini, Claude Code)
- MCP works from Claude Code, Claude API, OpenAI Responses API
- Web UI works from a browser for humans

## Web UI

FastAPI + Jinja2 templates + vanilla JS (no heavy frontend framework):

- **Browse**: wiki tree by category, with thumbnails
- **Search**: full-text with faceted filters (domain, type, scale, period, etc.)
- **View**: material pages with embedded figures, cross-references
- **View**: concept pages with citations
- **Open original**: link to file in iCloud (`file://` URL for local access)
- **Recent**: latest additions and compilations

The web UI should own freshness for collaborators. It should not assume the user has already pulled and ensured locally. In later phases, the UI should provide:
- a lightweight freshness check on app open and/or first search in a session
- an explicit `Update` action
- `arq index ensure` after sync so local search and local memory stay current before results are shown

No auth initially (local network). Basic auth can be added later.

## Server Agent (Mac Mini)

This is the phase where the Karpathy-style "wiki maintainer" becomes operational. The compile and lint systems defined earlier in this spec provide the maintainer's responsibilities; the server agent automates them continuously. It is not a generic coding agent and it is not governed by the current build-system `CLAUDE.md`. It should eventually have its own dedicated maintainer instruction file describing ingest, compile, lint, indexing, logging, filing, and recovery behavior.

### `arq watch` daemon:

1. Monitors iCloud folder for new/changed files
2. **Watch backend** (configurable via `watch.backend`):
   - `fsevents` (default on macOS) — native filesystem events, low overhead
   - `poll` — fallback, configurable interval, works everywhere
3. **Debouncing + batching**: iCloud sync triggers multiple events per file. The watcher debounces events (default 10s window) and batches files that arrive together into a single pipeline run.
4. On batch ready:
   - `arq ingest` — register new materials in manifest
   - `arq extract` — deterministic parsing + LLM enrichment
   - `arq compile` — update wiki
   - `arq index rebuild` — update local search index
   - `git add . && git commit -m "auto: ingest <n> new materials" && git push`
5. Runs as a launchd service for auto-start on boot
6. Logs to `~/.arquimedes/watch.log`

### Collaborator sync (`arq sync`):

- Runs `git pull` every 5 minutes (configurable)
- Runs `arq index ensure` immediately after pull so collaborator search and local memory stay current
- Lightweight launchd service
- Collaborators install once: `arq sync --install` creates the launchd plist

## Tech Stack

- **Python 3.11+** with pyproject.toml (pip/uv installable)
- **Click** for CLI
- **PyMuPDF (fitz)** for PDF text, image, TOC, and annotation extraction
- **pdfplumber** for table extraction
- **Tesseract** (via pytesseract, optional) for OCR on scanned documents and images
- **SQLite FTS5** for search index (stdlib, no extra dependency)
- **FastAPI + Jinja2** for web UI
- **Agent CLI** (configurable: claude, openai-cli, gemini-cli) for LLM enrichment — no API keys in codebase
- **watchdog** (cross-platform) or **pyobjc-FSEvents** (macOS native) for file watching
- **launchd** for daemon management on macOS

## Verification Plan

1. **Ingest**: drop a PDF into library folder → `arq ingest` → verify material appears in `manifests/materials.jsonl`
2. **Extract-raw**: `arq extract-raw <id>` → verify `extracted/<id>/` contains meta.json (raw fields only), text.md, pages.jsonl, chunks.jsonl (no summaries), figures/, toc.json
3. **Enrich**: `arq enrich <id>` → verify meta.json gains summary/keywords/facets with provenance, chunks gain summaries, figures gain descriptions and visual_type
4. **Search**: `arq search "thermal mass"` → verify relevant cards returned with correct facets
5. **Deep search**: `arq search --deep "thermal mass"` → verify multi-layer drill returns chunk text
6. **Compile**: `arq compile` → verify wiki pages generated with correct links and cross-references, and memory bridge rebuilt
7. **Lint**: `arq lint` → verify deterministic checks catch broken links, missing metadata; LLM checks find connections and suggest concepts
8. **Watch**: start `arq watch`, drop file into iCloud folder → verify auto-pipeline runs end-to-end with debounced batch commit
9. **Sync**: on second device, `arq sync` → verify git pull brings new content, `arq index ensure` auto-rebuilds local index and local memory bridge
10. **Web UI**: `arq serve` → browse wiki, search, view material pages, open figures
11. **MCP**: connect agent to MCP server → verify search and read tools work
