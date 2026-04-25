# Arquimedes — Implementation Plan

> **Status:** Phases 1-7 complete (Phase 7 agent tools landed 2026-04-16); Collection Graph implemented; Phase 8 web UI in progress; Phase 9 server agent + sync implemented in code/tests (2026-04-25); macOS launchd end-to-end install remains operator verification
> **Last updated:** 2026-04-25
> **Spec:** [Full design spec](superpowers/specs/2026-04-04-arquimedes-knowledge-system-design.md)
> **Phase 3 spec:** [Enrichment design](superpowers/completed/specs/2026-04-04-phase3-enrichment-design.md)
> **Phase 4 spec:** [Search index design](superpowers/completed/specs/2026-04-04-phase4-search-index-design.md)
> **Phase 5 spec:** [Wiki compiler, collection pages, and memory bridge](superpowers/completed/specs/2026-04-05-phase5-wiki-compiler-design.md)
> **Phase 6 spec:** [Lint, reflection, and memory growth](superpowers/completed/specs/2026-04-05-phase6-lint-design.md)
> **Proposed next architecture:** [Collection graph architecture](superpowers/specs/2026-04-09-collection-graph-design.md)
> **Proposed implementation plan:** [Collection graph implementation plan](superpowers/plans/2026-04-09-collection-graph-implementation.md)
> **Phase 7 spec:** [Agent tools design](superpowers/specs/2026-04-16-phase7-agent-tools-design.md)
> **Phase 7 plan:** [Agent tools implementation plan](superpowers/plans/2026-04-16-phase7-agent-tools.md)
> **Phase 8 spec:** [Web UI design](superpowers/specs/2026-04-11-phase8-web-ui-design.md)
> **Phase 8 plan:** [Web UI implementation plan](superpowers/plans/2026-04-11-phase8-web-ui.md)
> **Phase 9 spec:** [Server agent + sync design](superpowers/specs/2026-04-25-phase9-server-agent-design.md)
> **Phase 9 plan:** [Server agent + sync implementation plan](superpowers/plans/2026-04-25-phase9-server-agent.md)
> **Collaborator handbook:** [docs/collaborator/agent-handbook.md](../collaborator/agent-handbook.md) (created by Phase 7)
> **Reference:** [Karpathy-inspired LLM wiki idea](llm-wiki.md)
> **Pipeline:** [Operational pipeline](PIPELINE.md)
> **Supporting spec:** [Connection model](superpowers/completed/specs/2026-04-05-connection-model.md)
> **Completed phase docs:** `docs/developer/superpowers/completed/`

## Context

Building a collaborative LLM knowledge base for architecture practice and research.

Raw materials live in a shared iCloud folder. The repo contains extracted artifacts, wiki, indexes, and tools. A server agent (Mac Mini) auto-ingests new materials. Collaborators search via web UI or their own agents.

The long-term operating model is an LLM-maintained wiki. In Arquimedes, the future **server agent** is that maintainer: it will ingest new sources, enrich them, compile/update wiki pages, run health checks, and keep indexes current. This role is assembled across multiple phases:
- **Phases 5-6** define what the maintainer actually does (compile and lint the wiki)
- **Phase 7** exposes those capabilities to other agents
- **Phase 9** turns the maintainer into an always-on daemon with automation, batching, sync, and recovery behavior

Current docs like `CLAUDE.md` describe how to build Arquimedes itself. A dedicated maintainer instruction file for the server agent belongs with the Phase 9 rollout, once compile/lint behavior is implemented and stable.

Use `docs/developer/llm-wiki.md` as the conceptual reference for the original pattern. Use this plan and the global spec to understand how Arquimedes adapts that pattern for architecture, provenance, collaboration, and an always-on server maintainer.

## How to use this plan

- Each phase has checkboxes. Mark items `[x]` when complete.
- Update the **Status** line at the top when moving to a new phase.
- Update **Last updated** date on any change.
- Any LLM agent picking up work should read this file first to understand current state.
- Then read `docs/developer/llm-wiki.md` if conceptual grounding is needed, especially when working on wiki compile/lint/maintainer behavior.
- The search index (SQLite) is gitignored and rebuilt locally — never committed.

---

## Phase 1: Project Scaffolding

- [x] Initialize git repo, pyproject.toml, .gitignore
- [x] Create directory structure (config/, src/, manifests/, extracted/, indexes/, wiki/)
- [x] Set up config system (shared config plus role-specific collaborator/maintainer profiles)
- [x] Create `arq` CLI entrypoint with Click
- [x] Data models (Material, Chunk, Figure, etc.) with provenance support

## Phase 2: Ingest + Raw Extraction

- [x] `arq ingest` — scan LIBRARY_ROOT, register materials in materials.jsonl
- [x] Material ID generation (sha256[:12])
- [x] Re-ingesting a moved material refreshes its manifest `relative_path`, `domain`, and `collection`, and updates extracted raw scope fields so collection assignment remains trustworthy after rehoming
- [x] `arq extract-raw` — deterministic extraction via PyMuPDF + pdfplumber
  - [x] PDF: text, pages, TOC, tables
  - [x] PDF annotations: highlights, notes, marks → annotations.jsonl, emphasized flag on chunks
  - [x] Figures: embedded image extraction + page rasterization with region detection
  - [x] Page thumbnails
  - [x] Image files: OCR for scanned documents, pass-through for project/inspiration images
  - [x] Deterministic classification (TF-IDF keywords, rule-based document type)
  - [x] **(connection model)** `Chunk.annotation_overlap_ids: list[str]` — store explicit annotation IDs per chunk, not just the boolean emphasis flag (LOW — needed for Phase 5 annotation backlinks; see [connection model plan](superpowers/completed/plans/2026-04-05-connection-model.md))

## Phase 3: LLM Enrichment

- [x] `arq enrich` — LLM enrichment with stage/run stamps; concepts keep source provenance
  - [x] Annotation-aware: weight highlighted/noted sections in summaries and keywords
  - [x] Document-level: summary, document_type (refine raw_document_type), keywords (refine raw_keywords), architecture facets; the LLM reads original `meta.json` plus flattened document text and returns a structured JSON patch that the pipeline applies programmatically
  - [x] Chunk-level: one-line summaries, keywords, per-chunk stamp provenance with `enriched_at`, and resumable batch checkpointing via `chunk_enrichment.work.json` so non-force reruns skip already completed chunks and only promote to `chunks.jsonl` at full success
  - [x] Figure-level: visual_type, descriptions, captions, deterministic source_page on the sidecar, and LLM-guided artifact deletion for non-figures
  - [x] Operational run logs under `logs/` record explicit `START` and terminal `DONE`/`FAILED` outcomes for `arq enrich`, `arq cluster`, and `arq lint`
  - [x] Stage order is strictly sequential per material: document -> metadata -> chunk -> figure
  - [x] Explicit `--stage` selection is exact: `--stage chunk` runs chunk only; implicit metadata repair happens only in the default full enrichment flow
  - [x] Figure-stage document context reduced to title/authors/year/domain/collection plus summary when present; figure no longer consumes document_type, bridge concepts, TOC, or raw keywords, and waits for document when both stages are stale so summary stays current
  - [x] `enrichment.chunk_parallel_requests` and `enrichment.figure_parallel_requests` control per-material batch-level LLM fanout for chunk and figure stages
  - [x] Concept candidates for wiki compiler
  - [x] Copilot enrichment routes use a shared no-tools custom agent with programmatic prompt mode
  - [x] Claude usage preflight removed; provider routing now relies on ordered runtime fallback instead of Anthropic usage-endpoint checks
  - [x] Agent routing now treats providers as failed only on process completion outcomes: non-zero exit, timeout, or unusable empty output. Arquimedes does not inspect generated text for auth/rate-limit phrases or cache providers as exhausted across calls.
  - [x] Temporary operator env var `ARQ_ABORT_ON_CLAUDE_FALLBACK=1` can abort a run before fallback starts if Claude fails
  - [x] Claude bare mode disabled globally; all Claude routes use the legacy non-bare launch path
  - [x] Metadata-fix pass inserted between document and chunk and exposed as its own runnable `metadata` stage; it uses the first four page thumbnails and a dedicated Copilot GPT-5.4 mini route to correct title, authors, and year when confidently recoverable
  - [x] Document stage no longer proposes or applies title changes; title correction is owned by the metadata stage only
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
- See [connection model plan](superpowers/completed/plans/2026-04-05-connection-model.md) for full design

## Phase 5: Wiki Compiler

- [x] **(concept clustering)** `arq cluster` — LLM pass over collection-local concept packets and current local cluster memory; the model returns a structured JSON delta with `links_to_existing[]`, `new_clusters[]`, and `_finished`, and the pipeline validates that response before deterministically emitting `derived/collections/<domain>__<collection>/local_concept_clusters.jsonl`; canonical names should act as meaningful collection-local concept homes rather than raw per-material fragments
- [x] `arq compile` — generate material pages, concept pages (one page per cluster), index pages
- [x] **(collection pages)** Extend `arq compile` so `wiki/{domain}/{collection}/_index.md` becomes a first-class deterministic collection page; see [Phase 5 spec](superpowers/completed/specs/2026-04-05-phase5-wiki-compiler-design.md)
- [x] Collection pages should include: overview, recent additions, material list, top canonical concepts by recurrence, top facets by frequency
- [x] Incremental compilation (per-material stamps for material pages; local/global cluster artifact stamps rebuild affected concept and bridge pages)
- [x] Cross-referencing with standard markdown links
- [x] Wiki tree: practice/, research/, shared/concepts/
- [x] Define the wiki structures the future server maintainer will own and keep current: all generated material pages, concept pages, glossary pages, and directory `_index.md` pages under `wiki/`; later phases may add maintainer-owned reports/logs/filings, but collaborators should treat the generated wiki tree as compiler/server-maintainer-owned

## Phase 5.5: Memory Bridge

- [x] `arq memory rebuild` — project canonical concept clusters and wiki identities into SQLite
- [x] `arq memory ensure` — rebuild bridge tables only when cluster/wiki graph inputs change
- [x] Add canonical local/global cluster tables (`local_concept_clusters`, `global_bridge_clusters`, `global_bridge_members`, `wiki_pages`) with local material and relation projections
- [x] Make `arq search` query canonical clusters and aliases, not only raw per-material concepts
- [x] Make `arq related` prefer canonical cluster membership as the strongest signal
- [x] Ensure the semantic graph is queryable for agents, not only readable in markdown
- [x] Make `arq compile` auto-run `arq memory rebuild`, so semantic publication updates wiki and machine-queryable memory together
- [x] Make collaborator-side `arq index ensure` auto-run `arq memory ensure`, so pulled wiki/cluster artifacts regain their canonical connections locally without compile
- [x] Update CLI output so `arq index ensure` reports both index status and memory-bridge status
- [x] Keep semantic publication server-maintainer-only: collaborators rebuild deterministic local projections, but never re-cluster or re-compile the wiki

## Phase 6: Wiki Linting & Health Checks

Deterministic lint, reflective passes, memory projection, and lint scheduling are implemented in code and verified by tests. The remaining work is Phase 7+ tooling and any future daemon wiring around these maintained layers.

- [x] Deterministic checks first: broken links, orphaned materials/pages, missing metadata, stale enrichment (document stage by stamp/version drift; other stages by their own input drift rules), stale index, stale memory bridge, duplicates, missing compiled pages
- [x] **(cluster audit)** LLM review of collection-local cluster artifacts: over-merged concepts to split, missed equivalences to merge, orphaned weak homes, poorly named canonicals, missing materials in clusters; deterministically invalid edits are skipped without aborting the rest of the audit run
- [x] **(concept reflection)** improve concept pages with cross-material `main_takeaways`, `main_tensions`, `open_questions`, `helpful_new_sources`, and `why_this_concept_matters`
- [x] **(collection reflection)** improve collection pages with `main_takeaways`, `main_tensions`, important materials/concepts, `open_questions`, `helpful_new_sources`, and `why_this_collection_matters` grounded in linked materials
- [x] Persist full collection reflection prose in SQLite as part of the queryable memory layer, including `why_this_collection_matters`
- [x] LLM-driven graph checks: missing cross-references, contradictions across materials, under-connected materials/clusters, unanswered research questions from weakly connected areas
- [x] Feed reflective outputs back into searchable memory so agents can query takeaways, tensions, and open questions, not only graph topology
- [x] `arq lint` (full), `arq lint --quick` (deterministic only), `arq lint --report`, `arq lint --fix`
- [x] Reflective lint emits per-stage terminal progress lines (`started`, `finished`, `skipped`) so long-running LLM stages are visible while `arq lint` is running
- [x] Provenance on every LLM suggestion
- [x] Define the health-check and maintenance behaviors the future server maintainer will run automatically

## Collection Graph Step 1: Collection-First Semantic Homes

- [x] Rehome moved materials so collection assignment stays trustworthy after ingest
- [x] Add collection-local cluster artifacts, stamps, and stable ids under `derived/collections/`
- [x] Make `arq cluster` collection-local with per-collection stale detection, scheduling, and internal gates
- [x] Project the local graph into SQLite and register local cluster wiki pages
- [x] Compile collection-local cluster pages and make material/collection pages prefer them
- [x] Re-ground collection reflections in local clusters
- [x] Reuse the current cluster-audit model as collection-local audit with parallel fanout and per-collection gates
- [x] Add collection -> local clusters and material -> local clusters traversal
- [x] Keep default lexical search global while preserving deterministic cross-collection relatedness during the transition
- [x] Add a one-time Step 1 migration script for legacy bridge-era repos so current data can bootstrap local clusters and collection continuity without re-enrichment or re-reflection

## Collection Graph Step 2: Global Bridge Graph

- [x] Materialize `derived/global_bridge_clusters.jsonl` and `derived/global_bridge_stamp.json`
- [x] Define bridge members in terms of contributing local clusters rather than raw material-level concepts
- [x] Run the first Step 2 bridge layer as `arq lint --stage global-bridge` and include it in `arq lint --full`
- [x] Keep graph-maintenance available as an explicit lint stage without running it in the default `arq lint --full` path
- [x] Keep Step 2 global bridging owned by lint rather than adding a separate `arq bridge-global` command
- [x] Make global-bridge stale detection depend only on new or changed promoted local clusters that are not yet covered by the bridge layer
- [x] Keep the saved global-bridge input fingerprint scoped to that pending local-cluster delta rather than collection-reflection churn or bridge-memory drift
- [x] Skip the global-bridge stage when fewer than two collections are in scope
- [x] Project global bridges into SQLite traversal and search
- [x] Compile bridge pages from local-cluster members
- [x] Add local-cluster backlinks into shared bridge memberships and distinguish bridge overlap in relatedness explanations
- [x] Materialize cross-collection synthesis on global bridge rows/pages using collection-reflection signals
- [x] Feed global-bridge memory with connected local-cluster reflections and collection signals so `why_this_bridge_matters` can be written as a page-grade mini-essay
- [x] Keep concept reflections focused on local concept homes and move bridge-level synthesis into the global-bridge pass
- [x] Retire the legacy raw-material global bridge publication path

## Search Coverage for Bridge Layer (pre-Phase-7 prerequisite)

Current `arq search` covers material cards, chunks, figures, annotations, per-material concepts, local concept clusters, global bridge clusters, and concept/collection reflection prose.

- [x] Add `global_bridge_clusters` table to the memory projection (bridge_id, canonical_name, slug, descriptor, aliases, confidence, material_count, wiki_path, why_this_bridge_matters, bridge_takeaways, bridge_tensions, bridge_open_questions, helpful_new_sources)
- [x] Add `global_bridge_clusters_fts` covering name, aliases, descriptor, and reflection prose
- [x] Populate the new tables during `arq compile` / `arq memory rebuild` from `derived/global_bridge_clusters.jsonl`
- [x] Add `GlobalBridgeHit` dataclass + `_search_global_bridges` helper in `search.py`; surface as a new `global_bridges` field on `SearchResult`
- [x] Remove the retired legacy `concept_clusters_fts` compatibility path from active search coverage
- [x] Tests: FTS matches and LIKE fallback over bridge reflection fields

## Phase 7: Agent Tools

Collaborator-facing agent surface. CLI + handbook only (no MCP). Teach any shell-capable agent to investigate the knowledge base token-efficiently without installing or configuring anything beyond the existing `arq` CLI. Agents read; they never maintain. Starts after the Search Coverage prerequisite lands so every reflection layer is one search away. See [Phase 7 spec](superpowers/specs/2026-04-16-phase7-agent-tools-design.md) and [Phase 7 plan](superpowers/plans/2026-04-16-phase7-agent-tools.md).

- [x] Shared `agent_cli.ensure_guard` decorator + `ARQ_SKIP_FRESHNESS` opt-out powering transparent `ensure_index_and_memory()` on every new agent-facing command
- [x] Extend `read.py` with card, chunk-by-id, compact chunk/figure/annotation indexes, and corpus-overview accessors; no new semantic layers
- [x] Replace `arq read` stub with layered flags: default card, `--detail <chunks|figures|annotations>`, `--chunk <id>`, `--page N`, `--full` (explicit opt-in, expensive)
- [x] Replace `arq figures` stub with compact list by default; `--visual-type`, `--figure <id>` drills
- [x] Add `arq annotations <material_id>` with `--page`, `--type` filters
- [x] Add `arq overview` returning domain/collection/material/cluster/bridge counts + freshness snapshot from live SQLite + stamps
- [x] Add `arq refresh` wrapping `freshness.update_workspace()` (fetch/reset/clean + ensure) as the explicit heavier freshness path
- [x] Every new command emits JSON by default with `--human` opt-in and includes `material_id` / `wiki_path` / identifiers for direct next-step lookups
- [x] Create `docs/collaborator/agent-handbook.md` — hard cap ~800 tokens total: mental model (2-3 lines), path tree, investigation recipe, command quick-reference table, one-line maintainer-only warning; no intro prose, no examples. Add pointer from `CLAUDE.md` to this file.
- [x] Freshness-on-read contract: transparent `ensure_index_and_memory()` on search/read commands; `arq refresh` for sync-and-ensure; `ARQ_SKIP_FRESHNESS=1` opt-out
- [x] Explicitly out of scope for Phase 7: MCP server, LLM-invoking commands, any mutation of extracted/wiki/memory/index artifacts, daemon behavior

## Phase 8: Web UI

- [x] FastAPI + Jinja2 server (`arq serve`)
- [x] Browse wiki tree, search with facets, view material pages
- [x] Figure gallery, links to original iCloud files
- [x] Freshness UX: collaborator-facing update path before search (auto-check on app open / first search in session and explicit Update button), followed by `arq index ensure`

## Phase 9: Server Agent + Sync

- [x] Introduce a dedicated maintainer instruction file for the server agent (operational schema, not build-system docs): `docs/maintainer/MAINTAINER.md`
- [x] `arq watch` — 30-minute scan daemon for ingest/extract/index/compile publication
- [x] Single commit per scan cycle when changes are detected
- [x] `arq sync` — auto-sync daemon for collaborators with `arq index ensure` after canonical reset, so local search and memory are always current before use
- [x] `arq sync` inherits the memory bridge automatically via `arq index ensure` → `arq memory ensure`
- [x] `arq lint --quick` after each compile, `arq lint --full` daily at 02:00 via `arq lint --install-full`
- [x] launchd integration for watch, sync, and lint-full plist installation/status/uninstall
- [x] Auto-commit + push pipeline
- [x] Always-on maintainer flow: ingest → extract → index rebuild → compile → commit/push; reflective `global-bridge` remains nightly full-lint only
- [x] **Material removal cascade**: when a raw file is deleted from iCloud, the watcher removes manifest entries, extracted artifacts, wiki pages, and concept cluster references before rebuild/compile/index publication
- [ ] Operator verification on the actual macOS maintainer machine: install launchd jobs, run a real iCloud add/update/delete cycle, and confirm push/sync behavior against a separate collaborator clone

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
| `src/arquimedes/llm.py` | Shared LLM callable abstraction (`LlmFn`) + agent CLI adapter (no API keys) |
| `src/arquimedes/enrich_llm.py` | Backward-compatible shim re-exporting shared LLM helpers |
| `src/arquimedes/enrich_stamps.py` | Fingerprinting, staleness tracking, stamp I/O |
| `src/arquimedes/enrich_prompts.py` | Prompt builders for document, chunk, figure stages |
| `src/arquimedes/index.py` | SQLite FTS5 index build + query + staleness check |
| `src/arquimedes/search.py` | Search interface (card → chunk → deep) |
| `src/arquimedes/read.py` | Deterministic read helpers for wiki/material/figure/chunk/page/annotation/overview accessors |
| `src/arquimedes/agent_cli.py` | Shared helpers for agent-facing commands: freshness guard, JSON/human dispatch, error formatting |
| `src/arquimedes/freshness.py` | Collaborator refresh path: fetch/reset/clean + `ensure_index_and_memory()` |
| `src/arquimedes/compile.py` | Wiki generation |
| `src/arquimedes/lint_global_bridge.py` | Step 2 global bridge artifact generation from local clusters |
| `src/arquimedes/serve.py` | Web UI (FastAPI) |
| `src/arquimedes/watch.py` | Scheduled scan daemon for publication batches |
| `src/arquimedes/sync.py` | Auto-sync daemon + index ensure |
| `src/arquimedes/removal.py` | Material removal cascade for deleted library files |
| `src/arquimedes/launchd.py` | macOS launchd plist rendering/install/status helpers |
| `config/config.yaml` | Shared default configuration |
| `config/collaborator/config.local.example.yaml` | Collaborator local config example |
| `config/maintainer/config.yaml` | Maintainer LLM/provider and daemon profile |

## Verification Checklist

- [ ] Ingest: drop PDF → `arq ingest` → appears in materials.jsonl
- [ ] Extract-raw: `arq extract-raw <id>` → extracted/<id>/ has meta.json, text.md, pages.jsonl, chunks.jsonl, figures/ (no LLM fields)
- [ ] Enrich: `arq enrich <id>` → meta.json gains summary/keywords/facets with provenance, chunks gain summaries
- [ ] Search: `arq search "thermal mass"` → relevant cards with correct facets
- [ ] Deep search: `arq search --deep "thermal mass"` → multi-layer drill to chunk text
- [ ] Compile: `arq compile` → wiki pages with links and cross-references, plus memory bridge rebuilt
- [x] Watch unit behavior: scanner/planner detects add/modify/move/delete and empty cycles
- [x] Sync unit behavior: `arq sync` fetches, resets, cleans, and runs index/memory ensure
- [x] Launchd plist rendering: watch interval and lint-full calendar plist payloads render correctly
- [x] Removal cascade unit behavior: manifest, extracted artifacts, wiki page, and cluster refs are removed idempotently
- [ ] Watch operator verification: `arq watch` + real iCloud file → next scan runs auto-pipeline with one batch commit and push
- [ ] Sync operator verification: second device `arq sync` → fetch/reset/clean + `arq index ensure` auto-rebuilds local index and local memory bridge
- [ ] Web UI: `arq serve` → browse, search, view materials, open figures
- [x] Agent CLI: `arq read <id>` returns a card with `wiki_path`; `--chunk`, `--page`, `--full`, `--detail` drill layers behave per spec
- [x] Agent CLI: `arq figures <id>`, `arq annotations <id>`, `arq overview`, `arq refresh` return documented JSON shapes and respect `--human`
- [x] Agent CLI: `ARQ_SKIP_FRESHNESS=1` bypasses `ensure_index_and_memory()` on agent-facing commands
- [x] Agent handbook: `docs/collaborator/agent-handbook.md` exists; referenced wiki paths and command names resolve; under ~800-token body
- [ ] Lint: `arq lint` → catches broken links, missing metadata
