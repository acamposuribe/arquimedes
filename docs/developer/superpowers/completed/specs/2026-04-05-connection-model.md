# Arquimedes — Connection Model

> **Status:** Complete
> **Date:** 2026-04-05
> **Related specs:** [Full system design](../../specs/2026-04-04-arquimedes-knowledge-system-design.md), [Phase 3 enrichment](2026-04-04-phase3-enrichment-design.md), [Phase 4 search index](2026-04-04-phase4-search-index-design.md)

## Purpose

Arquimedes is not meant to become only a searchable database. The intended long-term shape is a connected, compounding knowledge system: an artificial memory-brain whose structure grows as materials are ingested, enriched, searched, compiled, and linted.

This document defines how **connections** emerge across the implemented pipeline. The wiki compiler materializes strong connections into pages and links, but connection formation starts earlier.

## Core Principle

Connections should **not** appear for the first time at lint.

Instead:
- **Phase 2** creates structural anchors
- **Phase 3** creates semantic hooks (local concept candidates per material)
- **Phase 4** creates retrieval-time associative links (deterministic, inspectable, cheap)
- **Phase 5** clusters concept candidates into canonical concepts and materializes pages
- **Phase 6** audits, splits, merges, and expands those clusters

Lint is a health-check and improvement pass over a graph that already exists in weaker form. It is not the origin of the graph.

### Phase 4 determinism boundary

Phase 4 stays **deterministic, cheap, and inspectable**. It connects materials through structural signals — shared normalized keywords, shared facets, shared authors, and normalized concept-key matches (case/plural variants of exactly the same phrase). It does not synthesize semantic equivalences across different phrasings.

Two papers may both discuss "the archive as built space" in very different words. Phase 4 cannot find that connection. Phase 5 can.

This boundary is intentional. An `arq index rebuild` must mean "deterministic local rebuild from existing artifacts." It must never trigger an LLM call.

### On concept clustering

The LLM enrichment pipeline (Phase 3) emits concept candidates independently per material. Different materials describing the same intellectual territory will use different phrasings. Exact- and normalized-string matching cannot bridge this gap.

Semantic concept clustering — inferring equivalences across phrasings, choosing canonical names, assigning aliases, grouping contributing materials — is compile-shaped work, not index-shaped. It belongs in Phase 5.

The target artifact is `derived/bridge_concept_clusters.jsonl`. `arq cluster` writes it (first-class command, see [Phase 5 spec](2026-04-05-phase5-wiki-compiler-design.md)); `arq compile` consumes it for concept pages; Phase 6 lint audits and refines it. Each cluster record carries a `slug` (for stable page paths), full `source_concepts` provenance (descriptor, relevance, evidence spans, confidence per material), and LLM grouping confidence.

## Connection Types

Arquimedes should distinguish five kinds of connections.

### 1. Structural connections

Objective, deterministic links created from document structure and extraction.

Examples:
- material -> page
- page -> annotation
- page -> figure
- page -> table
- chunk -> page
- chunk -> annotation overlap
- material -> author
- material -> collection
- material -> raw keyword

These do not require interpretation. They are the parse graph.

### 2. Semantic connections

Meaning-bearing links introduced by enrichment.

Examples:
- material -> concept candidate
- material -> enriched keyword
- material -> facet
- chunk -> content_class
- figure -> visual_type
- figure -> relevance
- annotation -> emphasis signal

These are not yet explicit wiki links, but they create the semantic hooks needed for compile-time materialization and clustering.

### 3. Retrieval connections

Query-time links created by search and ranking.

Examples:
- material matched via card text
- material matched via chunk text
- material matched via annotation
- material matched via figure
- material boosted by emphasized chunk
- material boosted by annotation comment

These are evidence paths. They explain why a material surfaced for a specific question.

### 4. Attention connections

Signals of human importance or system salience.

Examples:
- chunk emphasized because it overlaps reader annotations
- annotation comment exists
- figure marked substantive
- concept repeatedly appears across materials

These are especially important because they make Arquimedes feel like a personal memory system rather than a neutral archive.

### 5. Materialized connections

Explicit, persistent knowledge links written into the wiki layer.

Examples:
- related materials sections
- concept pages
- backlinks
- cross-references between practice and research materials
- index pages and topic maps

These are produced by compile and refined by lint.

## Phase 2: Structural Anchors

Phase 2 should not invent cross-material meaning, but it should already create deterministic connection primitives.

### What Phase 2 should produce

- `material -> page`
- `page -> heading`
- `page -> figure_ref`
- `page -> table_ref`
- `page -> annotation_ids`
- `chunk -> source_pages`
- `chunk -> annotation overlap` via `emphasized`
- `figure -> source_page`
- `table -> source_page`
- `material -> authors`
- `material -> domain`
- `material -> collection`
- `material -> raw_keywords`
- `material -> raw_document_type`

### Role in the system

Phase 2 creates the first memory substrate: a **parse graph**. These edges are deterministic and trustworthy. They make later semantic and retrieval connections explainable.

### Important constraint

Phase 2 should not assert strong cross-material claims such as "related to" or "same concept." It should only expose structural anchors that later phases can build on.

## Phase 3: Semantic Hooks

Phase 3 should enrich materials with local semantic structure. This is where Arquimedes starts becoming associative rather than merely indexed.

### What Phase 3 already supports

- `material -> summary`
- `material -> document_type`
- `material -> enriched keywords`
- `material -> facets`
- `material -> concept candidates`
- `chunk -> summary`
- `chunk -> keywords`
- `chunk -> content_class`
- `figure -> visual_type`
- `figure -> description`
- `figure -> caption`
- `figure -> relevance`
- annotation-aware emphasis in document and chunk enrichment

### Connection interpretation

Phase 3 creates **semantic hooks**, not final knowledge links.

Examples:
- two materials may later be connected because they share concept candidates
- two materials may later be linked because they share jurisdiction, location, or historical period
- a figure may later support a concept page because it is marked substantive and visually typed
- a chunk may later become a supporting citation because it is classed as argument and emphasized

### Important constraint

Phase 3 should avoid overstating cross-material relationships as facts. It should emit rich local metadata and candidates, but not yet formalize them as wiki structure.

### Concept name normalization

Different materials may emit the same concept with surface variation: `Archival Habitat`, `archival habitat`, `archival habitats`. If these are stored and grouped by raw name, the connection graph fragments into false singletons.

**Strategy:** At index time, each concept_name is normalized to a `concept_key` (lowercased, whitespace-collapsed, basic English plural removal on the final word). The display name (`concept_name`) is preserved as-is. All grouping, joining, and deduplication uses `concept_key`, not `concept_name`.

The normalizer handles:
- case folding (`Archival Habitat` → `archival habitat`)
- whitespace collapse
- trailing-s plurals (`habitats` → `habitat`, `archives` → `archive`)
- -ies plurals (`categories` → `category`)
- -ses/-xes/-zes/-ches/-shes plurals (`processes` → `process`)

It intentionally does **not** attempt stemming, lemmatization, or synonym resolution. If those become necessary later, they belong in Phase 6 lint (LLM-assisted concept merging) or a dedicated alias table.

### Concept provenance

Each concept in `concepts.jsonl` already carries rich provenance: `source_pages`, `evidence_spans`, `confidence`. This provenance must be preserved through indexing so that Phase 5 concept pages and "connected because…" explanations can cite grounded evidence.

The concepts table stores these fields alongside `concept_key`, `concept_name`, `relevance`, and `material_id`.

### Keyword and author normalization for relational queries

Keywords and authors are stored in `meta.json` as JSON arrays and indexed as text in the materials table (for FTS). But cross-material queries like "find materials sharing keywords with X" need proper relational joins, not JSON parsing at query time.

**Strategy:** At index time, two helper tables are populated:
- `material_keywords(material_id, keyword)` — one row per keyword, normalized to lowercase
- `material_authors(material_id, author)` — one row per author, normalized to lowercase

`arq related` uses these tables for efficient SQL JOINs instead of scanning all materials and parsing JSON.

## Phase 4: Retrieval Connections

Phase 4 should turn latent hooks into usable, queryable relationships. This is the point where Arquimedes starts to behave like memory rather than storage.

### What Phase 4 should do

- index material-level semantic fields
- index chunk-level summaries/text/keywords
- index annotations directly
- index figure descriptions/captions
- use emphasis and annotation/comment signals in ranking
- expose why a result matched, at least implicitly through returned evidence

### Retrieval connection categories

#### Material-to-evidence connections

Examples:
- material surfaced because of card match
- material surfaced because of chunk match
- material surfaced because of annotation match
- material surfaced because of figure match

#### Attention-weighted retrieval

Examples:
- annotation comment boosts material ranking
- quoted highlight boosts material ranking
- emphasized chunk boosts chunk ranking
- substantive figure can later boost figure-based retrieval

#### Shared-signal retrieval

Examples:
- multiple materials surface for the same concept-oriented query
- materials converge on the same facet or keyword
- search retrieves a cluster of materials tied by shared themes even before a concept page exists

### Key Phase 4 principle

Search should not behave as if only material cards matter.

Arquimedes should support **content-first retrieval**:
- card-layer matches
- chunk-layer matches
- annotation-layer matches
- figure-layer matches

This is essential if the system is to feel like a connected memory rather than a flat catalog.

## What Wiki Compilation Adds

Phase 5 promotes selected strong connections into durable structure.

### Concept clustering (Phase 5 core responsibility)

Phase 3 emits local concept candidates. Phase 5 clusters them into canonical concepts across the collection.

The clustering pass:
1. Reads all `concept_name` values from all `concepts.jsonl` artifacts
2. Groups semantically equivalent concepts regardless of phrasing
3. Chooses a canonical name for each cluster
4. Records aliases, contributing materials, and source evidence
5. Writes `derived/bridge_concept_clusters.jsonl`

**Cluster artifact schema:**
```json
{
  "cluster_id": "concept_0001",
  "canonical_name": "archival habitat",
  "aliases": [
    "archive as architectural space",
    "archival habitat",
    "embodied archives"
  ],
  "material_ids": ["bbf97c1aae06", "b4f8dc3a028c"],
  "source_concepts": [
    {
      "material_id": "bbf97c1aae06",
      "concept_name": "archival habitat",
      "descriptor": "archives understood as built, inhabited environments"
    },
    {
      "material_id": "b4f8dc3a028c",
      "concept_name": "archive as architectural space",
      "descriptor": "the archive framed as spatial and architectural form"
    }
  ],
  "confidence": 0.82
}
```

This file is the single handoff point between Phase 5 clustering and all downstream consumers: concept page generation, semantic `arq related`, Phase 6 auditing.

### Other Phase 5 materialized connections

- related material sections on wiki pages (using cluster membership + structural signals)
- concept pages (one per cluster with `canonical_name`, aliases, contributing materials, evidence, descriptor)
- glossary pages
- backlinks
- master indexes and topic maps
- `wiki/shared/maintenance/graph-health.md` compiled from SQL-backed graph findings

Compile is the first phase that should write explicit narrative links like:
- "related materials"
- "contributes to concept X"
- "see also"
- "contradicts"
- "extends"

But compile should be using earlier connection layers, not inventing everything from scratch.

## What Linting Adds

Phase 6 inspects and improves the connection network. By Phase 6, `derived/bridge_concept_clusters.jsonl` already exists. Lint audits the quality of what compile produced.

### Deterministic lint

- missing wiki pages for extracted materials
- orphan pages
- broken links
- stale enrichment or index drift

### LLM-assisted lint

- **cluster quality:** over-merged concepts that should be split; missed equivalences that should be merged; weak or orphaned clusters with only one material
- **cluster expansion:** materials that belong in a cluster but were not captured by compile
- **cluster naming:** canonical names that are too generic, too narrow, or poorly worded
- missing concept pages
- missing cross-references
- contradictions across materials
- under-connected materials
- unanswered research questions suggested by weakly connected areas

Lint should be understood as a graph-audit and graph-improvement layer, not the origin of the graph.

## Connection Vocabulary For Future Work

Future agents should use the following vocabulary consistently:

- **structural**: deterministic extraction links
- **semantic**: enrichment-derived local metadata and candidates
- **retrieval**: query-time evidence paths and ranking relationships
- **attention**: reader/system importance signals
- **materialized**: explicit wiki links and pages

This vocabulary helps avoid confusion between:
- what is objectively linked
- what is semantically inferred
- what is only query-time evidence
- what has been promoted into durable knowledge

## Practical Guidance For Implementation

When adding features in Phases 2-4:

1. Ask which connection type is being introduced.
2. Prefer deterministic links first, semantic links second.
3. Do not materialize speculative links too early.
4. Do preserve candidate connections so later phases can formalize them.
5. Make retrieval explainable by preserving evidence paths.
6. Treat annotations and emphasis as first-class attention signals.

## Current Project Interpretation

At the current state of the project:

- **Phase 2** has already created structural anchors.
- **Phase 3** has already created semantic hooks and attention signals.
- **Phase 4** has already begun creating retrieval connections through content-first search, chunk ranking, and annotation-aware results.

This means Arquimedes is already on the path toward a connected memory system before the wiki compiler runs.

That is the intended direction.
