# Phase 5 Addendum: Collection Pages — Design Spec

> **Status:** Draft
> **Date:** 2026-04-05
> **Parent spec:** [Phase 5 wiki compiler design](2026-04-05-phase5-wiki-compiler-design.md)
> **Plan:** [Collection pages implementation plan](../plans/2026-04-05-phase5-collection-pages.md)

## Purpose

Phase 5 already generates material pages, concept pages, glossary pages, and directory `_index.md` pages.

This addendum makes collection pages first-class.

Each collection page should give a useful deterministic overview of the materials inside that collection, without requiring a new LLM synthesis pass on every file addition.

## Scope

Collection pages belong to Phase 5 because they are part of semantic publication and should update whenever new materials are published.

They do **not** include:
- main takes
- main learnings
- cross-material interpretation
- tensions / contradictions
- research questions

Those belong to Phase 6 maintenance and collection synthesis.

## Page Location

Collection pages use the existing collection `_index.md` path:

`wiki/{domain}/{collection}/_index.md`

Examples:
- `wiki/research/thermal-mass/_index.md`
- `wiki/practice/fire-codes/_index.md`

Collections named `_general` also get a page:
- `wiki/research/_general/_index.md`
- `wiki/practice/_general/_index.md`

## Inputs

Collection pages are deterministic. They are compiled from existing published/indexed data only:

- material metadata from `extracted/*/meta.json`
- canonical concept clusters from `derived/concept_clusters.jsonl`
- material/concept relations already available through compile/index helpers
- manifest ingestion metadata for recent additions

No new LLM call is allowed for collection pages in Phase 5.

## Content

Each collection page should contain:

1. **Collection title**
- H1 based on collection slug or friendly title formatting

2. **Overview**
- domain
- collection slug
- material count
- optional document-type counts if easy to compute

3. **Recent additions**
- newest materials in the collection, sorted by `ingested_at` descending
- links to material pages

4. **Materials**
- full material list in the collection
- each entry links to the material page
- include short one-line summary when available

5. **Key concepts**
- top canonical concepts by recurrence within the collection
- deterministic ranking:
  - highest material coverage first
  - then stronger relevance summary
  - then canonical name alphabetically
- each concept links to its concept page

6. **Top facets**
- most frequent non-empty facet values across collection materials
- grouped by facet field
- only show fields with meaningful recurrence

## Deterministic Definitions

### Key concepts

Key concepts are not synthesized.

They mean:
- canonical concept clusters that appear across materials in the collection

Recommended ranking inputs:
- `material_count_in_collection`
- relevance summary across member materials
- canonical name as stable tiebreaker

Recommended display:
- concept name
- count of collection materials it appears in

Example:
- `Thermal mass (4 materials)`

### Top facets

Top facets are frequency summaries over enriched facet values already present in material metadata.

Recommended behavior:
- count non-empty values per facet field
- sort by count descending
- optionally require `count >= 2` before showing a value

Example:
- `climate: mediterranean (3)`
- `material_system: masonry (2)`

## Incremental Behavior

Collection pages should rebuild when any of these change:

- collection membership changes
- a member material page input changes
- cluster data changes

At current scale, it is acceptable to rebuild all collection pages during `arq compile`.
If compile later becomes slower, collection-specific stamps can be introduced.

## Compile Ownership

Collection pages are compiler/server-maintainer-owned generated pages.

Manual edits should be treated the same way as other generated wiki pages:
- they may be overwritten on the next compile

## Relationship To Phase 6

Phase 5 collection pages provide structure.

Phase 6 may later add collection synthesis, for example:
- `main_takes`
- `main_questions`
- `emerging themes`
- `design implications`
- `teaching implications`

That later synthesis should rerun only when the collection has changed enough to justify it.

## Implementation Shape

No new CLI command is needed.

This should be implemented as part of `arq compile` by extending the existing collection `_index.md` pages into richer deterministic collection pages.
