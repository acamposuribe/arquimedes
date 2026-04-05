# Arquimedes — Implementation Plan

> **Status:** Phase 5.5 complete; Phase 6 next
> **Last updated:** 2026-04-05
> **Spec:** [Full design spec](superpowers/specs/2026-04-04-arquimedes-knowledge-system-design.md)
> **Phase 5 spec:** [Wiki compiler design](superpowers/specs/2026-04-05-phase5-wiki-compiler-design.md)
> **Phase 5.5 spec:** [Memory bridge design](superpowers/specs/2026-04-05-phase5-5-memory-bridge-design.md)
> **Reference:** [Karpathy-inspired LLM wiki idea](llm-wiki.md)
> **Pipeline:** [Operational pipeline](PIPELINE.md)
> **Phase 5 addendum:** [Collection pages spec](superpowers/specs/2026-04-05-phase5-collection-pages-design.md)

## Context

Building a collaborative LLM knowledge base for architecture practice and research.

Raw materials live in a shared iCloud folder. The repo contains extracted artifacts, wiki, indexes, and tools. A server agent (Mac Mini) auto-ingests new materials. Collaborators search via web UI or their own agents.

The long-term operating model is an LLM-maintained wiki. In Arquimedes, the future **server agent** is that maintainer: it will ingest new sources, enrich them, compile/update wiki pages, run health checks, and keep indexes current. This role is assembled across multiple phases:
- **Phases 5-6** define what the maintainer actually does (compile and lint the wiki)
- **Phase 7** exposes those capabilities to other agents
- **Phase 9** turns the maintainer into an always-on daemon with automation, batching, sync, and recovery behavior

Current docs like `CLAUDE.md` describe how to build Arquimedes itself. A dedicated maintainer instruction file for the server agent belongs with the Phase 9 rollout, once compile/lint behavior is implemented and stable.

Use `docs/llm-wiki.md` as the conceptual reference for the original pattern. Use this plan and the global spec to understand how Arquimedes adapts that pattern for architecture, provenance, collaboration, and an always-on server maintainer.

## How to use this plan

- Each phase has checkboxes. Mark items `[x]` when complete.
- Update the **Status** line at the top when moving to a new phase.
- Update **Last updated** date on any change.
- Any LLM agent picking up work should read this file first to understand current state.
- Then read `docs/llm-wiki.md` if conceptual grounding is needed, especially when working on wiki compile/lint/maintainer behavior.
- The search index (SQLite) is gitignored and rebuilt locally — never committed.

---

## Phase 1: Project Scaffolding

- [x] Initialize git repo, pyproject.toml, .gitignore
- [x] Create directory structure (config/, src/, manifests/, extracted/, indexes/, wiki/)
- [x] Set up config system (config.yaml, config.local.yaml, config.template.yaml)
- [x] Create `arq` CLI entrypoint with Click
- [x] Data models (Material, Chunk, Figure, etc.) with provenance support

## Phase 2: Ingest + Raw Extraction

- [x] `arq ingest` — scan LIBRARY_ROOT, register materials in materials.jsonl
- [x] Material ID generation (sha256[:12])
- [x] `arq extract-raw` — deterministic extraction via PyMuPDF + pdfplumber
  - [x] PDF: text, pages, TOC, tables
  - [x] PDF annotations: highlights, notes, marks → annotations.jsonl, emphasized flag on chunks
  - [x] Figures: embedded image extraction + page rasterization with region detection
  - [x] Page thumbnails
  - [x] Image files: OCR for scanned documents, pass-through for project/inspiration images
  - [x] Deterministic classification (TF-IDF keywords, rule-based document type)
  - [x] **(connection model)** `Chunk.annotation_overlap_ids: list[str]` — store explicit annotation IDs per chunk, not just the boolean emphasis flag (LOW — needed for Phase 5 annotation backlinks; see [connection model plan](superpowers/plans/2026-04-05-connection-model.md))

## Phase 3: LLM Enrichment

- [x] `arq enrich` — LLM enrichment with provenance on every field
  - [x] Annotation-aware: weight highlighted/noted sections in summaries and keywords
  - [x] Document-level: summary, document_type (refine raw_document_type), keywords (refine raw_keywords), architecture facets
  - [x] Chunk-level: one-line summaries, keywords
  - [x] Figure-level: visual_type, descriptions, captions
  - [x] Concept candidates for wiki compiler
- [x] `arq extract` convenience command (extract-raw + enrich)

## Phase 4: Search Index

- [x] SQLite FTS5 index builder (`arq index rebuild`)
- [x] `arq index ensure` — auto-rebuild if stale
- [x] `arq search` — card-level lexical search
- [x] `arq search --deep` — multi-layer retrieval (cards → chunks → full text)
- [x] Faceted search support
- [x] **(connection model C4.1)** `concepts` table with `concept_key` normalization (case-fold, plural-strip) + provenance columns (`source_pages`, `evidence_spans`, `confidence`); `concepts_fts` for FTS; `material_keywords` + `material_authors` helper tables for relational joins; staleness scope
- [x] **(connection model C4.2)** `ConceptHit` in search with provenance; concepts in content-first pass; concept boost in ranking; `--concept-limit` CLI option
- [x] **(connection model C4.3)** `arq related <material_id>` — shared concepts via `concept_key` JOIN, keywords via `material_keywords` JOIN, authors via `material_authors` JOIN, facets via direct comparison; scored and explained
- [x] **(connection model C4.4)** `arq concepts` — GROUP BY `concept_key` with counted relevance summary (e.g. "2×high"); Phase 5 uses this to decide which concept pages to compile
- See [connection model plan](superpowers/plans/2026-04-05-connection-model.md) for full design

## Phase 5: Wiki Compiler

- [x] **(concept clustering)** `arq cluster` — LLM pass over all concepts in the index (keys, titles, evidence); emit `derived/concept_clusters.jsonl` with `cluster_id`, `canonical_name`, `slug`, `aliases[]`, `material_ids[]`, `source_concepts[{material_id, concept_name, relevance, source_pages, evidence_spans, confidence}]`, `confidence`; canonical names should act as meaningful cross-material umbrella concepts rather than narrow one-material fragments; see [Phase 5 spec](superpowers/specs/2026-04-05-phase5-wiki-compiler-design.md)
- [x] `arq compile` — generate material pages, concept pages (one page per cluster), index pages
- [x] **(collection pages addendum)** Extend `arq compile` so `wiki/{domain}/{collection}/_index.md` becomes a first-class deterministic collection page; see [collection pages spec](superpowers/specs/2026-04-05-phase5-collection-pages-design.md)
- [x] Collection pages should include: overview, recent additions, material list, top canonical concepts by recurrence, top facets by frequency
- [x] Incremental compilation (per-material stamps for material pages; global `cluster_stamp` — when clusters change, rebuild **all** concept pages)
- [x] Cross-referencing with standard markdown links
- [x] Wiki tree: practice/, research/, shared/concepts/
- [x] Define the wiki structures the future server maintainer will own and keep current: all generated material pages, concept pages, glossary pages, and directory `_index.md` pages under `wiki/`; later phases may add maintainer-owned reports/logs/filings, but collaborators should treat the generated wiki tree as compiler/server-maintainer-owned

## Phase 5.5: Memory Bridge

- [x] `arq memory rebuild` — project canonical concept clusters and wiki identities into SQLite
- [x] `arq memory ensure` — rebuild bridge tables only when cluster/wiki graph inputs change
- [x] Add canonical-cluster tables (`concept_clusters` extended, `concept_cluster_aliases`, `wiki_pages`) + bridge columns on `cluster_materials`, `cluster_relations`
- [x] Make `arq search` query canonical clusters and aliases, not only raw per-material concepts
- [x] Make `arq related` prefer canonical cluster membership as the strongest signal
- [x] Ensure the semantic graph is queryable for agents, not only readable in markdown
- [x] Make `arq compile` auto-run `arq memory rebuild`, so semantic publication updates wiki and machine-queryable memory together
- [x] Make collaborator-side `arq index ensure` auto-run `arq memory ensure`, so pulled wiki/cluster artifacts regain their canonical connections locally without compile
- [x] Update CLI output so `arq index ensure` reports both index status and memory-bridge status
- [x] Keep semantic publication server-maintainer-only: collaborators rebuild deterministic local projections, but never re-cluster or re-compile the wiki

## Phase 6: Wiki Linting & Health Checks

- [ ] **(cluster audit)** LLM review of `derived/concept_clusters.jsonl`: over-merged concepts to split, missed equivalences to merge, orphaned single-material clusters, poorly named canonicals
- [ ] Deterministic checks: broken links, orphaned materials, missing metadata, stale enrichment, index drift, duplicates
- [ ] LLM-driven checks: missing cross-references, contradictions across materials, under-connected materials, unanswered research questions from weakly connected clusters
- [ ] `arq lint` (full), `arq lint --quick` (deterministic only), `arq lint --report`, `arq lint --fix`
- [ ] Provenance on every LLM suggestion
- [ ] Define the health-check and maintenance behaviors the future server maintainer will run automatically

## Phase 7: Agent Tools

- [ ] MCP server wrapping search, read, compile, lint functions
- [ ] `arq read`, `arq figures` CLI commands
- [ ] Freshness-on-read contract for collaborator-facing agent tools: before search/read operations, sync latest repo state when applicable and run `arq index ensure` so local search + memory are current

## Phase 8: Web UI

- [ ] FastAPI + Jinja2 server (`arq serve`)
- [ ] Browse wiki tree, search with facets, view material pages
- [ ] Figure gallery, links to original iCloud files
- [ ] Freshness UX: collaborator-facing update path before search (auto-check on app open / first search in session and explicit Update button), followed by `arq index ensure`

## Phase 9: Server Agent + Sync

- [ ] Introduce a dedicated maintainer instruction file for the server agent (operational schema, not build-system docs)
- [ ] `arq watch` — file watcher with configurable backend (fsevents | poll)
- [ ] Debouncing + batching (10s window, single commit per batch)
- [ ] `arq sync` — auto-pull daemon for collaborators with `arq index ensure` after pull, so local search and memory are always current before use
- [ ] `arq sync` should inherit the memory bridge automatically via `arq index ensure` → `arq memory ensure`
- [ ] `arq lint --quick` after each compile, `arq lint --full` on weekly schedule
- [ ] launchd integration for both watch and sync
- [ ] Auto-commit + push pipeline
- [ ] Always-on maintainer flow: ingest → extract → compile → lint/index → commit/push
- [ ] **Material removal cascade**: when a raw file is deleted from iCloud, the watcher should remove the manifest entry, extracted artifacts, wiki pages, concept cluster references, and rebuild the index. The full pipeline must be reversible.

---

## Key Files

| File | Purpose |
|------|---------|
| `src/arquimedes/cli.py` | `arq` CLI entrypoint |
| `src/arquimedes/ingest.py` | Library scanning, material registration |
| `src/arquimedes/extract_pdf.py` | Deterministic PDF extraction (text, images, tables, TOC) |
| `src/arquimedes/extract_figures.py` | Figure extraction: embedded + rasterized regions |
| `src/arquimedes/classify.py` | Deterministic classifiers (TF-IDF keywords, domain, document type) |
| `src/arquimedes/enrich.py` | LLM enrichment orchestrator with provenance tracking |
| `src/arquimedes/enrich_llm.py` | LLM callable abstraction (`LlmFn`) + agent CLI adapter (no API keys) |
| `src/arquimedes/enrich_stamps.py` | Fingerprinting, staleness tracking, stamp I/O |
| `src/arquimedes/enrich_prompts.py` | Prompt builders for document, chunk, figure stages |
| `src/arquimedes/index.py` | SQLite FTS5 index build + query + staleness check |
| `src/arquimedes/search.py` | Search interface (card → chunk → deep) |
| `src/arquimedes/compile.py` | Wiki generation |
| `src/arquimedes/mcp_server.py` | MCP tool wrapper |
| `src/arquimedes/serve.py` | Web UI (FastAPI) |
| `src/arquimedes/watch.py` | File watcher daemon (fsevents/poll + debounce) |
| `src/arquimedes/sync.py` | Auto-pull daemon + index ensure |
| `config/config.yaml` | Default configuration |

## Verification Checklist

- [ ] Ingest: drop PDF → `arq ingest` → appears in materials.jsonl
- [ ] Extract-raw: `arq extract-raw <id>` → extracted/<id>/ has meta.json, text.md, pages.jsonl, chunks.jsonl, figures/ (no LLM fields)
- [ ] Enrich: `arq enrich <id>` → meta.json gains summary/keywords/facets with provenance, chunks gain summaries
- [ ] Search: `arq search "thermal mass"` → relevant cards with correct facets
- [ ] Deep search: `arq search --deep "thermal mass"` → multi-layer drill to chunk text
- [ ] Compile: `arq compile` → wiki pages with links and cross-references, plus memory bridge rebuilt
- [ ] Watch: `arq watch` + drop file → auto-pipeline with debounced batch commit
- [ ] Sync: second device `arq sync` → git pull + `arq index ensure` auto-rebuilds local index and local memory bridge
- [ ] Web UI: `arq serve` → browse, search, view materials, open figures
- [ ] MCP: agent connects → search and read tools work
- [ ] Lint: `arq lint` → catches broken links, missing metadata
