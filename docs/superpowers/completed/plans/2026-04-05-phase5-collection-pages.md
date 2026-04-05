# Phase 5 Addendum: Collection Pages — Implementation Plan

> **Status:** Complete
> **Date:** 2026-04-05
> **Spec:** [Collection pages design spec](../specs/2026-04-05-phase5-collection-pages-design.md)

## File Map

| # | File | Action | Responsibility |
|---|------|--------|---------------|
| 1 | `src/arquimedes/compile_pages.py` | Modify | Add collection-page renderer |
| 2 | `src/arquimedes/compile.py` | Modify | Build collection-page inputs and write `wiki/{domain}/{collection}/_index.md` |
| 3 | `tests/test_compile.py` | Modify | Add collection-page coverage |

## Tasks

### C5.C1 — Collection page renderer

**Modify:** `src/arquimedes/compile_pages.py`

Add:
- `render_collection_page(title, domain, collection, materials, key_concepts, top_facets, recent_additions) -> str`

Sections:
- title
- overview
- recent additions
- materials
- key concepts
- top facets

Notes:
- pure renderer only
- no file I/O
- no index access
- standard markdown links only

### C5.C2 — Compile collection inputs

**Modify:** `src/arquimedes/compile.py`

Add compile-time helpers to:
- group materials by `(domain, collection)`
- derive collection material lists
- compute recent additions from `ingested_at`
- compute key concepts from canonical clusters filtered to collection materials
- compute top facets from member material metadata

Then:
- render `wiki/{domain}/{collection}/_index.md` with the new renderer
- include collection pages in the normal index-page rebuild flow

Recommended deterministic rules:
- key concepts ranked by material coverage, then relevance summary, then name
- top facets shown only for recurring non-empty values

### C5.C3 — Incremental behavior

**Modify:** `src/arquimedes/compile.py`

Phase 5 addendum rule:
- rebuilding all collection pages on every compile is acceptable for now

Do not add complex per-collection stamps unless needed.

If later needed, collection page invalidation should depend on:
- collection membership
- member material inputs
- cluster stamp

### C5.C4 — Tests

**Modify:** `tests/test_compile.py`

Add tests for:
- collection page is written at `wiki/{domain}/{collection}/_index.md`
- collection page lists member materials with links
- key concepts are derived from canonical clusters in that collection
- top facets are frequency-based and deterministic
- recent additions are ordered by `ingested_at`
- `_general` collections also render correctly

## Non-Goals

Do not implement here:
- collection summaries written by LLM
- main takes / main learnings
- contradictions / tensions
- teaching or design implications

Those belong to Phase 6.

## Definition of Done

- collection `_index.md` pages are richer than generic directory indexes
- they are fully deterministic
- they require no extra LLM call
- they update through normal `arq compile`
- tests cover the new behavior
