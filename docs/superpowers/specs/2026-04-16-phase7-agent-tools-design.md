# Arquimedes — Phase 7: Agent Tools Design

> **Status:** Proposed
> **Date:** 2026-04-16
> **Related specs:** [Full system design](2026-04-04-arquimedes-knowledge-system-design.md), [Phase 4 search index](../completed/specs/2026-04-04-phase4-search-index-design.md), [Phase 5 wiki compiler](../completed/specs/2026-04-05-phase5-wiki-compiler-design.md), [Phase 6 lint](../completed/specs/2026-04-05-phase6-lint-design.md), [Collection graph architecture](2026-04-09-collection-graph-design.md), [Phase 8 web UI](2026-04-11-phase8-web-ui-design.md)
> **Plan:** [Phase 7 agent tools plan](../plans/2026-04-16-phase7-agent-tools.md) · [PLAN.md](../../PLAN.md)

## Purpose

Phase 7 teaches collaborator-side agents how to **use** the Arquimedes knowledge base efficiently.

It does not add new semantic layers. It exposes what Phases 2-6 and the collection-graph rollout already produced:

- the extracted material artifacts under `extracted/`
- the compiled wiki pages under `wiki/`
- the SQLite search + memory projection under `indexes/`

The goal is that a collaborator can point any coding agent (Claude Code, Codex, Gemini CLI, Cursor, etc.) at the repo and the agent can find what the collaborator asks about using the fewest tokens possible — without the collaborator having to install anything beyond the `arq` CLI they already have, and without the agent having to read any unnecessary content.

## Core Principle

**Phase 7 is a usage surface, not a maintenance surface.**

Collaborator agents read. They never ingest, extract, enrich, cluster, compile, lint, or republish semantic structure. Those responsibilities stay with the server maintainer path defined in Phases 5, 6, and 9.

Agent work should feel like:

1. think about what to look for (handbook-informed mental model)
2. search with the right scope / facets
3. drill into exactly the passage, page, or figure needed
4. optionally expand via related materials or concept clusters

Anything that would pull tokens the agent does not yet need is a design smell.

## Scope

Phase 7 covers four collaborator-agent jobs:

1. **Orient**: understand the shape of the corpus (domains, collections, clusters, wiki layout) before searching
2. **Search**: find materials, passages, concepts, and annotations with facets and scope filters
3. **Read**: fetch exactly the card, page, chunk, figure, annotation, or wiki page that matters
4. **Navigate**: follow relatedness edges from one material to its clusters, related materials, and concept homes

Phase 7 explicitly excludes:

- building an MCP server (see [Surface Choice](#surface-choice) below for rationale)
- any command that mutates extracted artifacts, wiki, cluster graph, memory bridge, manifests, or index schema
- any LLM-invoking command (enrich, cluster, compile reflections, lint reflective passes)
- any background daemon behavior — that belongs to Phase 9

## Prerequisite: Search Must Cover All Reflection Layers

Phase 7 assumes `arq search` can reach every reflection layer the corpus currently synthesizes. Today it cannot, and this gap should close **before** Phase 7 implementation begins.

### What is searchable today

- material cards, chunks, figures, annotations, per-material concepts (all FTS)
- **local concept clusters** (Step 1) — FTS on `local_concept_clusters_fts`
- **legacy bridge concept clusters** — FTS on `concept_clusters_fts` (currently being retired per Collection Graph Step 2)
- concept reflection prose (takeaways / tensions / open questions / why_this_concept_matters / helpful_new_sources) — LIKE-fallback only
- collection reflection prose (same fields plus why_this_collection_matters, title) — LIKE-fallback only

### What is NOT searchable today

- **Step 2 global bridge clusters** — no dedicated SQLite table, no FTS, not in `SearchResult`. Reachable only via traversal from a local cluster (`get_cluster_global_bridges`).
- **Bridge reflection prose (`why_this_bridge_matters`, bridge takeaways, tensions, open questions, helpful_new_sources)** — lives only in `derived/global_bridge_clusters.jsonl` and compiled `wiki/shared/concepts/*.md`. No SQLite row. An agent asking "what connects thermal mass research across Mediterranean and continental climates" will not find the bridge page that synthesizes exactly that.

### Required closure before Phase 7

A small precursor task (tracked in `PLAN.md` as a "Search Coverage" item, not part of Phase 7 itself) must:

1. Add a `global_bridge_clusters` table to the memory bridge projection, with `cluster_id`, `canonical_name`, `slug`, `aliases`, `material_count`, `wiki_path`, plus the bridge reflection fields (`why_this_bridge_matters`, `main_takeaways`, `main_tensions`, `open_questions`, `helpful_new_sources`).
2. Add `global_bridge_clusters_fts` covering name, aliases, and reflection prose.
3. Extend `SearchResult` with a `global_bridges: list[GlobalBridgeHit]` field populated by a new `_search_global_bridges` helper that combines FTS matches with the same LIKE-fallback pattern used for concept/collection reflections.
4. Have `arq compile` / `arq memory rebuild` populate the new table from `derived/global_bridge_clusters.jsonl`.
5. Keep the legacy `concept_clusters_fts` path working until the retirement item in Collection Graph Step 2 lands — the two layers must coexist during the transition.

### Why it matters for Phase 7

Phase 7 commands are thin wrappers around `search.py`. If search does not find a layer, neither will the agent. The handbook would then have to teach fallback patterns (grep the filesystem for bridge essays), which violates the token-efficiency goal. Closing this gap before implementation means the handbook can stay minimal and the agent surface stays uniform: every synthesized layer is one search away.

### What stays out of this prerequisite

- upgrading concept/collection reflection searches from LIKE to FTS — functional today; defer unless the corpus grows past its LIKE ceiling
- any change to the bridge-clustering pipeline itself
- any change to compile-side markdown output

## Current Starting Point

Already present and agent-useful:

- `arq search` — card / deep / facet / collection search with structured JSON output by default (`--human` opt-in)
- `arq related <material_id>` — relatedness via shared clusters, concepts, keywords, authors, facets
- `arq material-clusters <material_id>` — material → local clusters
- `arq collection-clusters <domain> <collection>` — collection → local clusters
- `arq concepts` — canonical concept list with material counts
- `arq index ensure` — cheap staleness-check rebuild of search + memory bridge
- `src/arquimedes/read.py` (Phase 8) — deterministic read helpers: meta, pages, chunks, figures, wiki paths
- `src/arquimedes/freshness.py` (Phase 8) — shared pull-if-applicable + ensure helper
- `src/arquimedes/search.py` already exposes library-level functions consumed by the CLI

Still missing:

- `arq read` is a stub ([cli.py:317](../../../src/arquimedes/cli.py#L317))
- `arq figures` is a stub ([cli.py:324](../../../src/arquimedes/cli.py#L324))
- no agent-facing listing of annotations
- no orientation command that returns a compact corpus snapshot
- no transparent freshness hook on agent-facing commands
- no explicit `arq refresh` for the "git pull + ensure" path outside the web UI
- no in-repo agent handbook teaching the mental model and token-efficient investigation flow

## Surface Choice

Phase 7 ships a **CLI + handbook** surface. It does not ship an MCP server.

Reasoning:

- Every coding agent that a collaborator might use already has shell and filesystem access. The `arq` CLI is the universal surface.
- An MCP server would require every collaborator, on every agent client, to configure a new connection. That violates the "no input from collaborators" goal.
- Structured outputs are already the default on every agent-facing command (`--human` is opt-in formatting). Agents parse the JSON; humans ask for prose.
- A later phase may wrap the same Python library code in an MCP adapter if a specific collaborator workflow benefits. That would reuse the Phase 7 library code rather than replace the surface.

## Design Principles

### The agent surface is the `arq` CLI plus a handbook

Collaborator agents discover and use commands through two channels:

- `docs/agent-handbook.md` — in-repo mental model and investigation playbook
- each command's `--help` and rich default JSON output

No second surface, no second config file, no server process.

### Default outputs are minimal; expansion is explicit

Every agent-facing command returns the smallest useful result by default:

- `arq read <id>` returns a card (title, summary, facets, wiki_path), not full text
- `arq search` returns ~1-line cards at depth 1
- `arq figures <id>` returns figure metadata, not image bytes

Explicit flags expand:

- `arq read <id> --page N` / `--chunk <chunk_id>` / `--full`
- `arq read <id> --detail <aspect>` where aspect ∈ {card, chunks, figures, annotations, concepts}
- `arq search --deep`, `--depth 2|3`, `--chunk-limit`, etc.

The agent asks for what it needs. Nothing streams what it did not ask for.

### JSON is the machine default; `--human` is an opt-in

All agent-facing commands keep the already-established convention:

- default output is JSON on stdout
- `--human` produces a compact human-readable form for terminal use

New commands added in Phase 7 must follow this same polarity.

### Commands reuse Phase 8 deterministic helpers

`read.py` and `freshness.py` were introduced in Phase 8 and are already the canonical read/freshness layer. Phase 7 commands consume those helpers; Phase 7 does not introduce a parallel read layer.

This keeps the CLI and the web UI answering the same questions from the same code.

### Freshness is transparent on read and explicit when broader

Every agent-facing search/read command runs `ensure_index_and_memory()` before serving the request. That call is cheap when nothing changed (stamp comparison) and rebuilds only what drifted.

Pull-then-ensure (the web UI Update action) is **not** automatic on every CLI call — it is surprising behavior for a shell command. Instead it is exposed as an explicit `arq refresh` that wraps the existing Phase 8 `freshness` helper.

An env var `ARQ_SKIP_FRESHNESS=1` opts out of the transparent ensure step for batch scripts.

### The wiki is filesystem-readable

Compiled wiki pages under `wiki/` are plain markdown. The handbook teaches the layout. Agents should read them with their native file-read tool rather than through a dedicated CLI command.

`arq read <id>` surfaces `wiki_path` in its default output so the agent knows exactly which file to read next.

### Maintenance commands are invisible to collaborator agents

`arq ingest`, `arq extract-raw`, `arq enrich`, `arq cluster`, `arq compile`, `arq lint`, `arq memory rebuild`, `arq index rebuild`, `arq watch`, `arq sync --install` stay available in the CLI for admins and the server. The **handbook** is the normative surface for collaborator agents — and the handbook explicitly lists these as maintainer-only and tells agents not to run them.

No code-level blocks on these commands. Scope is enforced by documentation, not by guards, because the same binary is used by admins and collaborators.

## Command Surface

### Already implemented (no change)

| Command | Purpose |
|---|---|
| `arq search <query>` | Card / deep / facet / collection search |
| `arq related <material_id>` | Relatedness via clusters, concepts, authors, facets |
| `arq material-clusters <material_id>` | Local clusters for one material |
| `arq collection-clusters <domain> <collection>` | Local clusters inside one collection |
| `arq concepts` | Canonical concepts with material counts |
| `arq index ensure` | Rebuild search + memory bridge if stale |

These form the majority of the agent's investigation kit. Phase 7 adds targeted read commands plus orientation and freshness.

### New in Phase 7

| Command | Purpose |
|---|---|
| `arq read <material_id>` | Material card by default; `--page N` / `--chunk <id>` / `--full` / `--detail <aspect>` |
| `arq figures <material_id>` | Figure list by default; `--visual-type <t>` / `--figure <id>` |
| `arq annotations <material_id>` | Reader-highlighted passages; `--page N` / `--type <highlight|note|...>` |
| `arq overview` | Compact corpus snapshot: domain/collection/material/cluster/bridge counts + freshness state |
| `arq refresh` | Explicit pull-if-applicable + ensure; reuses `freshness.py` |

### Cross-command behaviors

All commands in the new set must:

- emit JSON by default, `--human` opt-in
- run `ensure_index_and_memory()` before answering, unless `ARQ_SKIP_FRESHNESS=1` is set (or unless the command is `arq refresh` itself, which does its own freshness)
- include `material_id`, and where applicable `wiki_path`, in the top-level JSON so the agent can chain to filesystem reads
- respect `--limit` where result size is unbounded
- fail with a clear message (not a stack trace) when the material / page / chunk / figure does not exist

### `arq read <material_id>` contract

Default (no flags) returns a card:

```json
{
  "material_id": "...",
  "title": "...",
  "authors": ["..."],
  "year": 2024,
  "domain": "research",
  "collection": "thermal-mass",
  "summary": "...",
  "keywords": ["..."],
  "facets": { "scale": "building", "climate": "mediterranean", "...": "..." },
  "methodological_conclusions": ["..."],
  "main_content_learnings": ["..."],
  "wiki_path": "wiki/research/thermal-mass/0012ab-thermal-mass-study.md",
  "source_path": "Research/thermal-mass/study.pdf",
  "counts": { "chunks": 47, "figures": 12, "annotations": 3 }
}
```

Flags (mutually informative, not all mutually exclusive):

- `--page N` — return one page's text and page metadata only
- `--chunk <chunk_id>` — return one chunk's text, summary, source_pages, emphasized
- `--full` — return the full `text.md` (explicit opt-in, documented as expensive)
- `--detail <aspect>` — expand the card with one aspect:
  - `card` (default, same as no flag)
  - `chunks` — append compact chunk index (chunk_id, one-line summary, source_pages, emphasized)
  - `figures` — append compact figure index
  - `annotations` — append annotations list
  - `concepts` — append concepts.jsonl entries this material contributes

The handbook tells agents the expansion ladder: card → detail=chunks → `--chunk <id>` → `--page N` → `--full` only when truly needed.

### `arq figures <material_id>` contract

Default returns a compact list: `figure_id`, `page`, `visual_type`, `caption`, one-line `description`, `image_path`.

Flags:

- `--visual-type <plan|section|elevation|detail|photo|diagram|chart|render|sketch>` — filter
- `--figure <figure_id>` — return the full sidecar record for one figure (including the longer LLM description)

### `arq annotations <material_id>` contract

Default returns a compact list: `annotation_id`, `type`, `page`, `quoted_text`, `comment` (nullable), `color`.

Flags:

- `--page N` — filter to one page
- `--type <highlight|note|underline|strikeout|freetext>` — filter by annotation kind

### `arq overview` contract

Returns a compact corpus snapshot the agent can consume before deciding where to search:

```json
{
  "domains": [
    {
      "domain": "research",
      "material_count": 184,
      "collections": [
        { "name": "thermal-mass", "material_count": 12, "local_cluster_count": 7 },
        { "name": "_general", "material_count": 23, "local_cluster_count": 0 }
      ]
    },
    { "domain": "practice", "material_count": 97, "collections": [ ... ] }
  ],
  "totals": {
    "materials": 281,
    "local_clusters": 63,
    "global_bridges": 11,
    "concepts": 412,
    "annotations": 186
  },
  "wiki": {
    "material_pages": 281,
    "local_concept_pages": 63,
    "shared_concept_pages": 11,
    "collection_index_pages": 18
  },
  "freshness": {
    "index_stale": false,
    "memory_stale": false,
    "last_checked": "2026-04-16T09:12:04Z"
  }
}
```

Values come from SQLite and on-disk stamps. This is a deterministic snapshot, not a rebuild.

### `arq refresh` contract

Wraps the Phase 8 `freshness.refresh()` helper (pull-if-applicable + `ensure_index_and_memory`). Returns the same structured status as the web UI update endpoint, so agents get a uniform answer regardless of which surface triggers a refresh.

## Agent Handbook

### Audience

Coding agents used by collaborators to investigate the knowledge base. Not the build-system agent governed by `CLAUDE.md`. Not the future server maintainer (its handbook lands with Phase 9).

### Location

`docs/agent-handbook.md` — at a canonical in-repo path the collaborator can point any agent at.

### Minimalism requirement

The handbook is loaded on every agent investigation session. Its size is the standing tax on every task.

**Hard cap: ~800 tokens of body content, reference table included.** If the handbook grows beyond that, cut it. Anything longer belongs in the spec, in command `--help`, or not at all.

What makes the cut:

1. **Mental model.** Two or three lines: domains, collections, material_id, what the wiki pages synthesize.
2. **Path layout.** One ascii tree showing `wiki/…` and `extracted/<id>/…`.
3. **Investigation recipe.** A short numbered list: search → narrow → drill.
4. **Quick reference table.** Every agent-facing command on one line each.
5. **Maintainer-only list.** One line: "do not run these." Name them. One line of why.

What is explicitly cut from the handbook:

- introduction / motivation prose — the spec holds that
- long token-hygiene rules — folded into one-line bullets in the recipe
- freshness explanation — a single line with a pointer to `arq refresh --help`
- discoverability / "how to point your agent at this" — belongs in `CLAUDE.md`, not here
- examples — `--help` on each command is the example surface

### Discoverability

- `CLAUDE.md` gets a pointer section telling agents working on this repo that end-user investigation uses the handbook, not the build-system docs
- `docs/PLAN.md` lists the handbook in its supporting-docs table
- A collaborator pointing an agent at the repo for investigation can simply say "read `docs/agent-handbook.md` first"
- `arq overview` output references the handbook path

## Freshness Contract

### Transparent ensure

Every new search/read command and the three new commands (`read`, `figures`, `annotations`, `overview`) wrap their core work in `ensure_index_and_memory()` — unless `ARQ_SKIP_FRESHNESS=1` is set.

This matches the existing behavior of `arq search` (which already ensures) and extends it uniformly across the agent-facing surface.

### Explicit refresh

`arq refresh` runs the Phase 8 `freshness.refresh()` helper:

1. if the repo tracks upstream, is clean, and is online: `git pull --ff-only`
2. run `ensure_index_and_memory()`
3. print structured status

Agents call this at the start of a session when they want the latest collaborator contributions, not on every tool call.

### Opt-out

`ARQ_SKIP_FRESHNESS=1` is documented in the handbook. Useful for:

- scripted evaluation loops
- the server agent's own calls during its pipeline (it has already decided freshness elsewhere)
- debugging without repeated ensure work

## Token Efficiency Rules

Rules the implementation enforces and the handbook teaches:

- default outputs are cards / metadata, never full text
- full text is opt-in with `--full` and documented as expensive
- pagination primitive is `--limit` plus stable ordering on all list-returning commands
- JSON-by-default output avoids the prose overhead of rendered markdown for agents
- every command returns enough identifiers (`material_id`, `chunk_id`, `figure_id`, `wiki_path`) that the next targeted call is a direct lookup, not a re-search
- `arq read` surfaces `counts` so the agent can choose the cheapest expansion

## Out of Scope

- no MCP server in Phase 7
- no new HTTP endpoints (the web UI is Phase 8's surface)
- no new LLM invocations
- no changes to extracted artifact schemas
- no changes to the wiki compile output
- no new SQLite tables or new memory-bridge columns
- no daemon behavior; no auto-sync
- no auth layer; the CLI already runs as the collaborator
- no mutation commands for collaborator agents
- no code-level block on maintainer commands — scope is enforced via the handbook, not runtime guards

## Verification

Minimum Phase 7 checks:

- `arq read <id>` returns a card with the documented fields, including `wiki_path` and `counts`
- `arq read <id> --page N`, `--chunk <id>`, `--full`, `--detail <aspect>` each behave as specified
- `arq figures <id>` returns the compact list; `--visual-type` and `--figure` filter correctly
- `arq annotations <id>` returns the compact list; page and type filters work
- `arq overview` returns the documented corpus snapshot from live SQLite + stamps
- `arq refresh` runs the Phase 8 freshness helper and prints structured status
- every new command runs `ensure_index_and_memory()` by default and is skipped by `ARQ_SKIP_FRESHNESS=1`
- every new command emits JSON by default and a legible human form under `--human`
- `docs/agent-handbook.md` exists, passes a markdown link check against current wiki paths and command names, and stays under ~2k tokens of body prose
- `CLAUDE.md` and `docs/PLAN.md` reference the handbook
- no new command mutates extracted, wiki, memory, or index data
