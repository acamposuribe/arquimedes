# Arquimedes — Concept Graph Improvement Spec

> **Status:** Draft
> **Date:** 2026-04-05
> **Scope:** Improve Phase 3 concept extraction and Phase 5 concept clustering so the resulting graph behaves more like an associative memory than a list of precise but weakly connected concept pages.

## Problem

Current concept pages are often strong locally but weak globally.

Typical failure mode:
- Phase 3 extracts concepts that are still too short or too under-contextualized
- Phase 5 clusters from concept rows only
- the graph ends up with many precise, specific concept pages but too few bridges across related materials

This produces a good archive of ideas, but not yet a strong artificial memory.

## Current Inputs

### Phase 3 concept extraction currently sees
- document metadata
- document/chunk text
- annotations
- prompt instructions asking for richer concept phrases

### Phase 5 clustering currently sees
- `concept_name`
- `concept_key`
- `material_id`
- material title
- `relevance`
- `confidence`
- up to two `evidence_spans`

This is better than before, but still too row-oriented.

## Design Goal

Build a two-layer concept system:

1. **Local concepts**
- strong material-level concepts with provenance
- this stays very close to the current Phase 3 concept output

2. **Bridge concepts**
- broader cross-material umbrellas
- connect related materials even when their local concept wording differs

The graph should support both:
- precision
- association

## Part A — Improve Phase 3 Concept Extraction

### Goal

Produce concept candidates that are:
- specific enough to mean something
- broad enough to connect across materials
- less likely to be one-word abstractions or ultra-local fragments

### Required changes

#### A1. Keep local concepts close to the current implementation

Document enrichment should return:

- `concepts_local`
  - 8–15 strong material-level concepts
  - essentially the current `concepts` output, with modest prompt refinement only

- `concepts_bridge_candidates`
  - 4–8 broader umbrella concepts
  - intended specifically for cross-material connection

These are both grounded in the same document, but serve different graph roles.
The important point is that the new ambition is carried mainly by `concepts_bridge_candidates`, not by a disruptive redesign of local concepts.

#### A2. Stronger extraction criteria, without larger input payloads

`concepts_local` should favor:
- reusable concept phrases
- named mechanisms, typologies, institutional logics, methods, conditions, frameworks
- historical qualifiers only when they clearly improve meaning or disambiguation

Do not force names, dates, or locations into every local concept. Keep the label reusable unless the detail adds clear meaning. Bridge candidates are the place for the broader umbrella.

`concepts_bridge_candidates` should favor:
- broader conceptual umbrellas
- problematics
- fields of inquiry
- larger spatial/institutional/material conditions

Avoid in both:
- vague one-word abstractions
- chapter themes
- incidental topics

Token discipline:
- do not send additional raw document text
- do not increase document-context size for Phase 3
- improve extraction mainly through prompt wording and schema shape
- most extra token cost should be output-side, not input-side

#### A3. Context-bearing names

Prefer phrases with built-in qualification.

Good:
- `archival architecture as institutional authority`
- `colonial archival epistemology and governance`

Weak:
- `authority`
- `space`
- `archive`

### Storage

Persist both in extracted artifacts.

Recommended shape in `concepts.jsonl`:
- add `concept_type: local|bridge_candidate`

Keep provenance the same:
- `source_pages`
- `evidence_spans`
- `confidence`
- `relevance`

## Part B — Improve Phase 5 Clustering

### Goal

Stop clustering from isolated concept rows only.

Current clustering sees too little of each material’s conceptual world. It should become material-aware.

### Required changes

#### B1. Bridge clustering only

Bridge clustering should consume the cleaned local concept signals and produce broader cross-material connections.

- cluster `concepts_bridge_candidates`
- preserve precise local concepts in SQL/search only
- publish only bridge concepts as wiki pages

This is the main ambitious change.

#### B2. Material-aware bridge input

The bridge clustering prompt should not rely on concept rows alone.

For each material, include:
- title
- document summary
- top keywords
- top local concepts
- bridge candidate concepts
- 3–5 strongest evidence snippets

This gives the LLM enough context to decide that two differently worded materials belong to the same broader conceptual territory.

Token discipline:
- no raw full text
- no chunk dumps
- no page dumps
- bridge clustering must operate on compact material packets only

#### B3. Distinct cluster outputs

Write one derived file:

- `derived/bridge_concept_clusters.jsonl`
  - broader associative clusters

Bridge clusters should require:
- at least 2 materials
- meaningful canonical umbrellas
- explicit source concepts from member materials

#### B4. Different validation rules

For bridge clusters:
- still require valid source concept references
- allow multiple source concepts from one material
- prefer broader, cross-material coherence

## Part C — Use in the Graph

### Wiki

Local concepts remain searchable in SQL and the deterministic local concept index.
Bridge concepts become the first-class wiki surface.

### Search / relatedness

Use bridge clusters to strengthen:
- `arq related`
- future article-writing retrieval
- collection understanding
- memory-bridge associations

### Memory bridge

Phase 5.5 mirrors the bridge concept graph into SQLite.

## Migration Strategy

### Step 1
- lightly refine the current local concept prompt
- add `concepts_bridge_candidates`
- start writing `concept_type`

### Step 2
- add separate bridge clustering path
- keep local concepts searchable in SQL

### Step 3
- update compile/search/memory to consume bridge layers and local concept index

## Success Criteria

This improvement is successful when:
- fewer concept pages are trivial one-paper singletons
- related papers surface together more often
- concept graph shows broader intellectual umbrellas
- article-writing agents can traverse both precise evidence and broader associations
- Phase 3 token usage remains close to the current level
- the main extra LLM cost is the new compact bridge-clustering pass

## Non-Goals

Not doing here:
- embeddings
- vector search
- ontology engineering
- human-authored taxonomy

This remains:
- LLM-assisted
- provenance-grounded
- file-based
- deterministic where possible
