# Arquimedes — Connection Model

> **Status:** Working design note
> **Date:** 2026-04-05
> **Related specs:** [Full system design](2026-04-04-arquimedes-knowledge-system-design.md), [Phase 3 enrichment](2026-04-04-phase3-enrichment-design.md), [Phase 4 search index](2026-04-04-phase4-search-index-design.md)

## Purpose

Arquimedes is not meant to become only a searchable database. The intended long-term shape is a connected, compounding knowledge system: an artificial memory-brain whose structure grows as materials are ingested, enriched, searched, compiled, and linted.

This document defines how **connections** should emerge before the wiki compiler exists. The wiki phase will materialize strong connections into pages and links, but connection formation starts earlier.

## Core Principle

Connections should **not** appear for the first time at lint.

Instead:
- **Phase 2** creates structural anchors
- **Phase 3** creates semantic hooks
- **Phase 4** creates retrieval-time associative links
- **Phase 5** materializes selected links into durable wiki structure
- **Phase 6** audits, strengthens, and expands the network

Lint is a health-check and improvement pass over a graph that already exists in weaker form. It is not the origin of the graph.

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

These are not yet explicit wiki links, but they create the semantic hooks needed for later compilation and clustering.

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

These are produced later, primarily by compile and refined by lint.

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

## What Wiki Compilation Will Add Later

Phase 5 will promote selected strong connections into durable structure.

Examples:
- related material sections
- concept pages
- glossary pages
- backlinks
- master indexes and topic maps

Compile is the first phase that should write explicit narrative links like:
- "related materials"
- "contributes to concept X"
- "see also"
- "contradicts"
- "extends"

But compile should be using earlier connection layers, not inventing everything from scratch.

## What Linting Will Add Later

Phase 6 should inspect and improve the connection network.

### Deterministic lint

- missing wiki pages for extracted materials
- orphan pages
- broken links
- stale enrichment or index drift

### LLM-assisted lint

- missing concept pages
- missing cross-references
- contradictions across materials
- under-connected materials
- over-isolated clusters
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

This means Arquimedes is already on the path toward a connected memory system before the wiki compiler exists.

That is the intended direction.
