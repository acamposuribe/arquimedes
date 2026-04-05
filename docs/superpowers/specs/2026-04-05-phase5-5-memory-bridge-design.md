# Arquimedes — Phase 5.5: Memory Bridge Design

> **Status:** Draft
> **Date:** 2026-04-05
> **Related specs:** [Full system design](2026-04-04-arquimedes-knowledge-system-design.md), [Connection model](2026-04-05-connection-model.md), [Phase 5 wiki compiler](2026-04-05-phase5-wiki-compiler-design.md)
> **Plan:** [PLAN.md](../../PLAN.md)

## Purpose

Phase 5.5 closes the gap between:

- the **search index**, which holds dense evidence and retrieval surfaces
- the **wiki**, which holds canonical concept pages and explicit cross-links

Today, these two layers are connected only indirectly via `material_id` and file paths. That is enough for manual traversal, but not enough for a real agent memory system. An agent can search chunks and annotations, or read concept pages, but it cannot query the canonical connection graph directly from SQLite.

Phase 5.5 adds that missing bridge.

The result should be:

- the **wiki** remains the human-readable semantic memory
- the **index** gains a machine-queryable version of the same graph
- agents can retrieve connections through SQL, then read wiki pages for synthesis, then drill into chunk/page evidence for proof

This phase is intentionally placed **after Phase 5 compile** and **before Phase 6 lint**:

- Phase 5 creates the first durable semantic structure
- Phase 5.5 operationalizes that structure for agents
- Phase 6 audits and improves it

## Design Principles

### The wiki is the readable brain; the index is the operational brain

Do not choose one. Keep both:

- `wiki/` is where concept clusters, backlinks, and summaries are materialized into durable markdown
- `search.sqlite` is where those same connections become queryable, filterable, rankable, and tool-usable

### No graph knowledge should exist only in markdown

If a concept cluster, related-material link, or concept alias matters enough to render into the wiki, it should also exist in an indexed table.

### No semantic page should float away from evidence

Every graph record must retain stable IDs and provenance links back to source materials and pages:

- `material_id`
- `cluster_id`
- `slug`
- `source_pages`
- `evidence_spans`

### Deterministic projection over LLM output

Phase 5.5 does **not** invent new semantic structure. It projects the outputs of Phase 5 into machine-queryable tables.

That means:

- no new LLM call
- no new semantic judgments
- no second concept-clustering pass

It is a deterministic bridge layer.

### Semantic publication is server-only; memory projection is local-safe

There are two distinct classes of work:

- **semantic publication** — clustering and wiki compilation, which may rely on LLM judgment and belong to the server maintainer
- **deterministic projection** — rebuilding the local search index and memory bridge from already-committed artifacts

Collaborators must never need to run `arq cluster` or `arq compile` after pull. They should only rebuild deterministic local query layers from the committed outputs produced by the server.

---

## Part 1: What Problem This Solves

Without Phase 5.5, the current system has this split:

| Layer | Holds | Searchable? | Connection-rich? |
|------|------|------|------|
| `search.sqlite` | chunks, annotations, figures, raw/enriched concepts | yes | partly |
| `wiki/` | material pages, concept pages, related links | no direct query layer | yes |
| `extracted/<mid>/` | full evidence artifacts | no | no |

The critical missing piece is this:

- the index knows raw per-material concept candidates
- the wiki knows canonical concept clusters
- but the index does **not** know canonical concept clusters or material-to-material semantic links derived from them

That means an agent can find materials relevant to a query, but cannot ask:

- “which canonical concept cluster connects these materials?”
- “what aliases belong to this concept?”
- “which materials participate in this cluster?”
- “which other concepts are adjacent because they share materials?”

Phase 5.5 adds exactly those query surfaces.

---

## Part 2: Memory Bridge Artifacts

Phase 5.5 writes deterministic graph tables into `search.sqlite`, derived from:

- `derived/concept_clusters.jsonl`
- compiled material page paths
- compiled concept page paths

The cluster file remains the source of semantic truth. The bridge tables are a projection of it.

### New indexed tables

#### 1. `concept_clusters`

One row per canonical concept cluster.

Columns:

- `cluster_id TEXT PRIMARY KEY`
- `canonical_name TEXT NOT NULL`
- `slug TEXT NOT NULL UNIQUE`
- `wiki_path TEXT NOT NULL`
- `confidence REAL NOT NULL`
- `material_count INTEGER NOT NULL`

Purpose:

- machine-queryable canonical concept inventory
- lookup target for concept pages
- bridge between concept pages and materials

#### 2. `concept_cluster_aliases`

One row per alias phrase belonging to a cluster.

Columns:

- `cluster_id TEXT NOT NULL`
- `alias TEXT NOT NULL`
- `PRIMARY KEY (cluster_id, alias)`

Purpose:

- support alias-aware search and retrieval
- let agents query concept territory even when the user uses a non-canonical term

#### 3. `concept_clusters_fts`

FTS table over:

- `canonical_name`
- `alias`

Purpose:

- query-time canonical concept retrieval
- bridge phrase variants like `archival habitat` and `archive as architectural space`

Implementation note:

- alias rows may be flattened into the FTS table with repeated `cluster_id`
- or you may use one row per cluster with aliases concatenated into a searchable field
- choose the simpler implementation, but make sure alias search works

#### 4. `cluster_materials`

One row per `(cluster_id, material_id)` membership.

Columns:

- `cluster_id TEXT NOT NULL`
- `material_id TEXT NOT NULL`
- `concept_name TEXT NOT NULL`
- `relevance TEXT NOT NULL`
- `source_pages TEXT NOT NULL`      # JSON array
- `evidence_spans TEXT NOT NULL`    # JSON array
- `confidence REAL NOT NULL`
- `material_wiki_path TEXT NOT NULL`
- `PRIMARY KEY (cluster_id, material_id, concept_name)`

Purpose:

- lets agents traverse from canonical cluster -> supporting materials
- preserves per-material evidence without re-reading markdown
- gives article-writing flows a direct route to proof

#### 5. `cluster_relations`

Deterministic adjacency between concept clusters based on shared materials.

Columns:

- `cluster_id TEXT NOT NULL`
- `related_cluster_id TEXT NOT NULL`
- `shared_material_count INTEGER NOT NULL`
- `shared_material_ids TEXT NOT NULL`   # JSON array
- `PRIMARY KEY (cluster_id, related_cluster_id)`

Purpose:

- machine-queryable version of the “Related concepts” section on concept pages
- lets agents traverse the concept graph without parsing wiki markdown

This table is symmetric in meaning, but store both directions for simpler querying.

#### 6. `wiki_pages`

One row per compiled wiki page.

Columns:

- `page_type TEXT NOT NULL`          # material | concept | index | glossary
- `page_id TEXT NOT NULL`            # material_id or cluster_id or synthetic id
- `title TEXT NOT NULL`
- `path TEXT NOT NULL UNIQUE`
- `domain TEXT NOT NULL DEFAULT ''`
- `collection TEXT NOT NULL DEFAULT ''`
- `PRIMARY KEY (page_type, page_id)`

Purpose:

- lets agents jump from graph nodes to concrete wiki pages
- supports future web UI and MCP read tools without crawling the tree

Material and concept pages are the important ones for Phase 5.5. Index/glossary rows are optional but useful.

---

## Part 3: Builder (`arq memory`)

Phase 5.5 introduces a deterministic bridge command:

```bash
arq memory rebuild
arq memory ensure
```

This command family reads the Phase 5 artifacts and projects them into `search.sqlite`.

### Why a separate command?

Do **not** fold this into `arq index rebuild`.

Reasons:

- Phase 4 index rebuild should stay about extracted/enriched artifacts
- Phase 5.5 depends on compiled/wiki-era outputs
- the mental model is cleaner:
  - `index` = evidence retrieval
  - `compile` = markdown materialization
  - `memory` = graph projection

That said, the future server-maintainer pipeline should usually run them together:

```text
cluster -> compile -> memory rebuild
```

In normal operation, `arq compile` should invoke `memory rebuild` automatically after successful page generation. The standalone `arq memory rebuild` command remains useful for debugging, recovery, and tests, but semantic publication should keep wiki and machine memory synchronized in one step.

### `arq memory rebuild`

Behavior:

1. Read `derived/concept_clusters.jsonl`
2. Compute material wiki paths from extracted metadata
3. Compute concept wiki paths from cluster slugs
4. Build the six tables above
5. Replace the previous bridge tables atomically
6. Write `derived/memory_bridge_stamp.json`

### `arq memory ensure`

Behavior:

1. Fast-check current bridge stamp
2. If stale, rebuild
3. If current, skip

`arq memory ensure` is intended for collaborator machines and sync flows. It must remain deterministic and LLM-free.

### Relationship to `arq index ensure`

On collaborator machines, `arq index ensure` should automatically call `arq memory ensure` after the evidence index is current.

That gives collaborators the full local query surface:

- evidence retrieval via `search.sqlite`
- canonical concept graph via the memory bridge

without ever requiring them to run:

- `arq cluster`
- `arq compile`

### Publication vs. local readiness

Recommended split:

- **server maintainer**
  - `arq enrich`
  - `arq index rebuild`
  - `arq cluster`
  - `arq compile` -> auto-runs `arq memory rebuild`

- **collaborator after pull**
  - `arq index ensure` -> auto-runs `arq memory ensure`

This preserves a clear monopoly:

- only the server publishes semantic structure
- every collaborator can rebuild deterministic local query layers

---

## Part 4: Staleness

`derived/memory_bridge_stamp.json` stores:

```json
{
  "built_at": "2026-04-05T16:00:00Z",
  "fingerprint": "<hash>",
  "cluster_stamp": "<hash of concept_clusters.jsonl>",
  "compile_stamp": "<hash of compile-relevant wiki/material path state>"
}
```

The Phase 5.5 fingerprint must cover all bridge inputs:

- `derived/concept_clusters.jsonl`
- material wiki paths implied by current extracted metadata
- concept wiki paths implied by cluster slugs

If any of those change, `arq memory ensure` must rebuild.

Important:

- this phase does **not** hash full wiki markdown content
- it hashes the graph inputs, not the rendered prose

The goal is to make the bridge reflect the semantic structure, not to mirror every byte of the wiki.

---

## Part 5: Query Model

Once Phase 5.5 exists, article-writing and research agents should use this retrieval flow:

### 1. Search for evidence

Use `arq search` against:

- cards
- chunks
- annotations
- figures
- canonical concepts via `concept_clusters_fts`

### 2. Expand through the memory bridge

Use cluster tables to traverse:

- query -> `concept_clusters`
- `concept_clusters` -> `cluster_materials`
- `cluster_materials` -> related materials
- `concept_clusters` -> `cluster_relations`
- any node -> `wiki_pages`

### 3. Read wiki pages for synthesized structure

The agent opens:

- material pages for curated summaries and related sections
- concept pages for canonical concept synthesis

### 4. Verify with evidence

The agent then drills back into:

- chunks
- annotations
- figure sidecars
- page references from `cluster_materials.source_pages`

That is the intended “brain” loop:

- **retrieval memory** -> **semantic memory** -> **evidentiary memory**

---

## Part 6: Search / Related Integration

Phase 5.5 should upgrade existing tools, not only add tables.

### `arq search`

Search should begin using canonical clusters directly:

- search canonical names and aliases through `concept_clusters_fts`
- attach cluster hits to results
- optionally surface:
  - canonical concept name
  - aliases
  - connected materials count

This lets a query for `archival habitat` surface the canonical cluster `archive as architectural space` even if the exact raw per-material concept phrases differ.

### `arq related`

`arq related <material_id>` should prefer canonical clusters over raw `concept_key` overlap.

Ranking priority becomes:

1. shared canonical cluster membership
2. shared normalized keyword
3. shared author
4. shared facet

And the explanation layer should say:

- `shared cluster: archive as architectural space`

not just

- `shared concept key`

### Future `arq memory read`

Phase 5.5 does not require this command yet, but it prepares for it.

A future tool may expose:

```bash
arq memory cluster <cluster_id>
arq memory material <material_id>
arq memory related <cluster_id>
```

These would be thin wrappers over the same tables.

---

## Part 7: What This Phase Does Not Do

Phase 5.5 does **not**:

- run a second LLM clustering pass
- infer new semantic equivalences
- read wiki markdown to extract links heuristically
- replace Phase 6 cluster auditing
- create a separate graph database

Stay simple:

- cluster file is the semantic source
- memory bridge is the deterministic indexed projection

---

## Part 8: CLI Summary

```bash
arq memory rebuild
arq memory ensure
```

Recommended server-maintainer sequence:

```text
arq enrich
arq index rebuild
arq cluster
arq compile
```

`arq compile` auto-runs `arq memory rebuild` on the server path, so the explicit extra command is mainly for debugging or recovery.

For collaborators:

```text
git pull
arq index ensure
```

`arq index ensure` should auto-run `arq memory ensure`, so collaborator workflows do not need to know about the bridge as a separate step.

---

## Part 9: Acceptance Criteria

Phase 5.5 is complete when:

- canonical concept clusters are queryable from SQLite
- aliases are searchable
- cluster -> material evidence is queryable without opening markdown
- concept adjacency is queryable without parsing concept pages
- wiki page paths are resolvable from graph nodes
- `arq search` can surface canonical concept hits
- `arq related` uses canonical clusters as first-class signals

At that point, Arquimedes stops being:

- a search index next to a wiki

and becomes:

- one memory system with both a readable surface and a queryable graph
