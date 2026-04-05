# Arquimedes — Operational Pipeline

> **Purpose:** quick reference for what happens when a new file is added, which steps use an LLM, and what the final published outputs are.
> **Related docs:** [Implementation plan](PLAN.md), [Global spec](superpowers/specs/2026-04-04-arquimedes-knowledge-system-design.md), [Phase 5 wiki compiler](superpowers/completed/specs/2026-04-05-phase5-wiki-compiler-design.md), [Phase 5.5 memory bridge](superpowers/completed/specs/2026-04-05-phase5-5-memory-bridge-design.md), [Phase 6 lint](superpowers/specs/2026-04-05-phase6-lint-design.md)

## End Product

Arquimedes publishes one shared knowledge system with three synchronized layers:

- **Extracted / enriched artifacts** in `extracted/`
  - raw text, chunks, annotations, figures, enriched summaries, concepts
- **Published wiki** in `wiki/`
  - material pages, concept pages, glossary, `_index.md` pages
- **Machine-queryable memory** in `indexes/search.sqlite`
  - search index, canonical concept clusters, bridge tables, wiki identities

The wiki is the readable semantic memory.
The SQLite index is the operational semantic memory for agents and tools.

## Who Does What

### Collaborators

Collaborators contribute by adding files to the shared library root folder.

They do **not**:
- run `arq cluster`
- run `arq compile`
- republish semantic structure

They may:
- drop files into the shared library root
- pull the repo
- run `arq index ensure`
- search the system via CLI, web UI, or their own agents

In practice, collaborator search assumes `arq index ensure` has run after the latest pull. This step is deterministic and LLM-free, so it is the normal lazy readiness step that keeps the local search index and local memory bridge current before use.

### Server maintainer

The server maintainer is the only semantic publisher.

It:
- watches the shared library root for changes
- ingests new materials
- extracts and enriches them
- clusters concepts
- compiles the wiki
- runs `arq lint --quick` after each compile
- runs `arq lint --full` on a schedule
- rebuilds the memory bridge
- commits and pushes the published result

## New File Pipeline

When a collaborator adds a file to the shared library root:

1. **Watcher detects change**
- The server daemon watches the shared library root
- It debounces and batches incoming file events
- No LLM

2. **`arq ingest`**
- Registers the file in `manifests/materials.jsonl`
- Assigns `material_id`, hashes the file, records source path, domain, collection
- No LLM

3. **`arq extract`**
- Main extraction command used by the server pipeline
- **Always run without arguments** — processes all pending materials automatically
- Phase 1: deterministic extraction for all pending materials (sequential, fast)
- Phase 2: LLM enrichment for all stale materials (**parallel**, up to `enrichment.parallel` threads — default 4)
- Single-material mode (`arq extract <material_id>`) exists but is only for debugging
- Deterministic outputs include:
  - `meta.json`
  - `text.md`
  - `pages.jsonl`
  - `chunks.jsonl`
  - `annotations.jsonl`
  - `figures/`
  - `toc.json`
- Enriched outputs include:
  - document summary, keywords, facets
  - chunk summaries and keywords
  - figure descriptions / visual type
  - concept candidates
- Stage-specific provider/model order comes from `enrichment.llm_routes[stage]` when configured; legacy `llm.agent_cmd` remains a fallback. Use `lint` routes for `arq lint` so full lint can stay separate from cluster routing.
- **LLM required for the enrichment part of this command**

4. **`arq index rebuild`**
- Rebuilds the local SQLite search index from extracted and enriched artifacts
- Makes materials, chunks, annotations, figures, and raw concept candidates searchable
- No LLM

5. **`arq cluster`**
- Reads bridge candidate packets and existing bridge concept memory
- Groups cross-material concepts into canonical bridge clusters
- Writes `derived/bridge_concept_clusters.jsonl`
- Uses the stage-specific route list for `cluster` when configured, with the same legacy fallback behavior
- **LLM required**

6. **`arq compile`**
- Deterministically renders the wiki:
  - material pages
  - concept pages
  - glossary
  - `_index.md` pages
- No LLM
- After compile, Arquimedes automatically runs `arq lint --quick` so deterministic health stays current

7. **Automatic memory rebuild**
- Runs as part of `arq compile`
- Projects the published wiki/cluster graph back into SQLite bridge tables
- Makes canonical concept connections queryable for agents
- No LLM

8. **Commit and push**
- The server commits the updated semantic publication
- Collaborators receive it on pull
- No LLM

## LLM vs Deterministic Steps

### LLM-required steps

- `arq extract` (enrichment stage)
- `arq cluster`
- `arq lint --full` (scheduled reflective maintenance pass)

### Deterministic steps

- `arq ingest`
- `arq extract-raw`
- `arq index rebuild`
- `arq compile`
- `arq lint --quick`
- `arq memory rebuild`
- `arq index ensure`
- `arq memory ensure`

## Collaborator Recovery Pipeline

After collaborators pull the repo:

1. `git pull`
2. `arq index ensure`

`arq index ensure` also ensures the local memory bridge is current.

Operationally, this is not optional before collaborator search after new pulls. It is the lazy deterministic readiness step that guarantees the local query layer matches the latest published repo state.

That means collaborators regain:
- full search index
- canonical concept clusters
- related-material graph
- wiki page identities in SQLite

without any LLM access and without re-running clustering or compile.

## Current Publication Rule

Semantic publication belongs only to the server-maintainer path:

`ingest -> extract -> index rebuild -> cluster -> compile -> memory rebuild -> commit/push`

Collaborators only rebuild deterministic local projections:

`pull -> index ensure`

## Mental Model

- `extract` = parse and understand the source
- `index rebuild` = make evidence searchable
- `cluster` = connect concepts across materials
- `compile` = publish the wiki
- `memory rebuild` = make the published graph queryable by agents

This is how Arquimedes stays both:
- a readable wiki for humans
- a queryable memory system for agents
