# Arquimedes — Implementation Plan

> **Status:** Phase 3 — Complete (pending integration smoke test)
> **Last updated:** 2026-04-05
> **Spec:** [Full design spec](superpowers/specs/2026-04-04-arquimedes-knowledge-system-design.md)

## Context

Building a collaborative LLM knowledge base for architecture practice and research.

Raw materials live in a shared iCloud folder. The repo contains extracted artifacts, wiki, indexes, and tools. A server agent (Mac Mini) auto-ingests new materials. Collaborators search via web UI or their own agents.

## How to use this plan

- Each phase has checkboxes. Mark items `[x]` when complete.
- Update the **Status** line at the top when moving to a new phase.
- Update **Last updated** date on any change.
- Any LLM agent picking up work should read this file first to understand current state.
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

## Phase 3: LLM Enrichment

- [x] `arq enrich` — LLM enrichment with provenance on every field
  - [x] Annotation-aware: weight highlighted/noted sections in summaries and keywords
  - [x] Document-level: summary, document_type (refine raw_document_type), keywords (refine raw_keywords), architecture facets
  - [x] Chunk-level: one-line summaries, keywords
  - [x] Figure-level: visual_type, descriptions, captions
  - [x] Concept candidates for wiki compiler
- [x] `arq extract` convenience command (extract-raw + enrich)

## Phase 4: Search Index

- [ ] SQLite FTS5 index builder (`arq index rebuild`)
- [ ] `arq index ensure` — auto-rebuild if stale
- [ ] `arq search` — card-level lexical search
- [ ] `arq search --deep` — multi-layer retrieval (cards → chunks → full text)
- [ ] Faceted search support

## Phase 5: Wiki Compiler

- [ ] `arq compile` — generate material pages, concept pages, index pages
- [ ] Incremental compilation (only affected pages)
- [ ] Cross-referencing with standard markdown links
- [ ] Wiki tree: practice/, research/, shared/concepts/

## Phase 6: Wiki Linting & Health Checks

- [ ] Deterministic checks: broken links, orphaned materials, missing metadata, stale enrichment, index drift, duplicates
- [ ] LLM-driven checks: inconsistent data, missing connections, concept candidates, impute missing facets, research questions, coverage gaps
- [ ] `arq lint` (full), `arq lint --quick` (deterministic only), `arq lint --report`, `arq lint --fix`
- [ ] Provenance on every LLM suggestion

## Phase 7: Agent Tools

- [ ] MCP server wrapping search, read, compile, lint functions
- [ ] `arq read`, `arq figures` CLI commands

## Phase 8: Web UI

- [ ] FastAPI + Jinja2 server (`arq serve`)
- [ ] Browse wiki tree, search with facets, view material pages
- [ ] Figure gallery, links to original iCloud files

## Phase 9: Server Agent + Sync

- [ ] `arq watch` — file watcher with configurable backend (fsevents | poll)
- [ ] Debouncing + batching (10s window, single commit per batch)
- [ ] `arq sync` — auto-pull daemon for collaborators with `arq index ensure` after pull
- [ ] `arq lint --quick` after each compile, `arq lint --full` on weekly schedule
- [ ] launchd integration for both watch and sync
- [ ] Auto-commit + push pipeline

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
- [ ] Compile: `arq compile` → wiki pages with links and cross-references
- [ ] Watch: `arq watch` + drop file → auto-pipeline with debounced batch commit
- [ ] Sync: second device `arq sync` → git pull + `arq index ensure` auto-rebuilds local index
- [ ] Web UI: `arq serve` → browse, search, view materials, open figures
- [ ] MCP: agent connects → search and read tools work
- [ ] Lint: `arq lint` → catches broken links, missing metadata
