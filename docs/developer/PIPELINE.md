# Arquimedes — Operational Pipeline

> **Purpose:** quick reference for what happens when a new file is added, which steps use an LLM, and what the final published outputs are.

## End Product

Arquimedes publishes one shared knowledge system with three synchronized layers:

- **Extracted / enriched artifacts** in `extracted/`
  - raw text, chunks, annotations, figures, enriched summaries, concepts
- **Published wiki** in `wiki/`
  - material pages, collection-local cluster pages, glossary, `_index.md` pages
- **Machine-queryable memory** in `indexes/search.sqlite`
  - search index, local concept graph, bridge tables, wiki identities

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
- run `arq refresh`
- run `arq index ensure` when explicitly verifying local readiness
- search the system via CLI, web UI, or their own agents

In practice, collaborator search runs the freshness path first: restore the canonical repo state, then ensure the local SQLite index and memory bridge. This step is deterministic and LLM-free.

### Server maintainer

The server maintainer is the only semantic publisher.

It:
- scans the shared library root every 30 minutes for new, changed, moved, or deleted materials
- ingests new materials
- extracts and enriches them
- clusters concepts within collection scope
- compiles the wiki
- runs `arq lint --quick` after each compile
- runs `arq lint --full` daily at 02:00, with refreshes between reflective stages and without the optional graph-maintenance backlog pass
- rebuilds the memory bridge
- commits and pushes the published result

## New File Pipeline

When a collaborator adds a file to the shared library root:

1. **Scheduled scan detects change**
- The server daemon scans the shared library root every 30 minutes
- It computes one batch from the durable library/repo delta
- No LLM

2. **`arq ingest`**
- Registers the file in `manifests/materials.jsonl`
- Assigns `material_id`, hashes the file, records source path, domain, collection
- Recognized source types: `.pdf`, image formats (`.jpg`/`.jpeg`/`.png`/`.tiff`/`.tif`/`.bmp`/`.webp`), plain text (`.txt`), Markdown (`.md`/`.markdown`), and OOXML Office (`.docx`/`.pptx`/`.xlsx`). Legacy binary Office formats (`.doc`/`.ppt`/`.xls`) are skipped — convert to OOXML or PDF first.
- Symlinked directories inside the library root are traversed. This lets `Proyectos/<project-id>/<link-name> -> /external/server/folder` ingest external server materials without duplicating source files; the symlink path remains the canonical project placement.
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
- Reads one collection-bounded packet at a time plus that collection's existing local cluster memory
- Groups concepts into collection-local clusters
- Writes `derived/collections/<domain>__<collection>/local_concept_clusters.jsonl`
- Uses the stage-specific route list for `cluster` when configured, with the same legacy fallback behavior
- **LLM required**

6. **`arq compile`**
- Deterministically renders the wiki:
  - material pages
  - collection-local cluster pages
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

After collaborators sync the repo:

1. `git fetch && git reset --hard @{upstream} && git clean -fd`
2. `arq index ensure`

`arq index ensure` also ensures the local memory bridge is current.

Operationally, this is not optional before collaborator search after new pulls. It is the lazy deterministic readiness step that guarantees the local query layer matches the latest published repo state.

That means collaborators regain:
- full search index
- collection-local clusters
- related-material graph
- wiki page identities in SQLite

without any LLM access and without re-running clustering or compile.

## Current Publication Rule

Semantic publication belongs only to the server-maintainer path.

Daytime publication:

`ingest -> extract -> index rebuild -> cluster -> compile -> memory rebuild -> commit/push`

Nightly maintenance:

`lint --full (including global-bridge) -> compile / memory refresh as needed -> commit/push`

Collaborators only rebuild deterministic local projections:

`pull -> index ensure`

## Mental Model

- `extract` = parse and understand the source
- `index rebuild` = make evidence searchable
- `cluster` = form collection-local clusters
- `lint --full` = nightly reflective maintenance, including `global-bridge`
- `compile` = publish the collection-first wiki plus shared bridge layer
- `memory rebuild` = make the published local+global graph queryable by agents

Legacy raw-material bridge artifacts are retired. The canonical bridge graph is the Step 2 global bridge graph built from local semantic outputs.

The core Step 2 bridge publication slice is now:

`cluster -> lint(global-bridge) -> compile -> memory rebuild`

That bridge stage is incremental, operates on collection-local clusters plus compact collection context, and skips entirely when fewer than two collections are in scope.

This is how Arquimedes stays both:
- a readable wiki for humans
- a queryable memory system for agents
