# Phase 4: Search Index & Retrieval — Implementation Plan

**Spec:** [Phase 4 search index design](../specs/2026-04-04-phase4-search-index-design.md)
**Date:** 2026-04-04
**Status:** Not started

---

## File Map

| # | File | Action | Responsibility |
|---|------|--------|---------------|
| 1 | `src/arquimedes/index.py` | Create | SQLite FTS5 index builder: schema creation, material/chunk/figure/annotation ingestion, atomic write, staleness tracking |
| 2 | `src/arquimedes/search.py` | Create | Search interface: multi-depth query, facet filtering, result formatting |
| 3 | `src/arquimedes/cli.py` | Modify | Wire `arq index rebuild`, `arq index ensure`, `arq search` with all flags |

## Dependency Order

```
index.py ─► search.py ─► cli.py
```

`index.py` has no dependency on `search.py`. Both are pure Python + stdlib SQLite — no new dependencies.

## Test Map

| Module | Test file | Strategy |
|--------|-----------|----------|
| `index` | `tests/test_index.py` | Fixture: minimal extracted/ dirs on tmp_path with meta.json, chunks.jsonl, figures/, annotations.jsonl. Test: schema creation, row counts, value extraction from EnrichedField wrappers, staleness detection, atomic write. |
| `search` | `tests/test_search.py` | Fixture: pre-built index on tmp_path. Test: card search, deep search (depth 2, 3), faceted filtering, empty results, limit/chunk-limit, annotation/figure results. |

---

## Tasks

### Task 1: `index.py` — schema + build
- [ ] Create `indexes/` directory if missing
- [ ] Define schema: `materials` + `materials_fts`, `chunks` + `chunks_fts`, `figures` + `figures_fts`, `annotations` + `annotations_fts`, `index_state`
- [ ] `rebuild_index(config) → IndexStats` — full build from extracted/ artifacts
- [ ] Read `materials.jsonl` for material IDs, iterate `extracted/<id>/`
- [ ] Extract `.value` from `EnrichedField` wrappers; empty string for absent fields
- [ ] Keywords list → space-joined string for FTS
- [ ] Facets: read each `facets.<field>.value`, empty string if absent
- [ ] Atomic write: build in temp file, rename to `indexes/search.sqlite`
- [ ] Write `index_state` row: built_at, manifest_hash, material_count, extracted_snapshot
- [ ] Tests
- [ ] Commit

### Task 2: `index.py` — staleness detection
- [ ] `ensure_index(config) → (rebuilt: bool, stats: IndexStats | None)`
- [ ] Fast gate: check file exists, material_count vs manifest line count, max meta.json mtime vs built_at
- [ ] Hash verify (when fast gate inconclusive): manifest_hash + extracted_snapshot comparison
- [ ] `_compute_manifest_hash() → str`
- [ ] `_compute_extracted_snapshot() → str` — sha256 of sorted enrichment stamps
- [ ] Tests: stale detection (new material, re-enriched material, no change)
- [ ] Commit

### Task 3: `search.py` — card search (depth 1)
- [ ] `search(query, config, *, depth=1, facets=None, collection=None, limit=20, chunk_limit=5) → SearchResult`
- [ ] FTS5 query against `materials_fts`
- [ ] Facet filtering via JOIN + WHERE on `materials` table
- [ ] `--collection` as shorthand for `facet collection=<value>`
- [ ] Facet prefix match (LIKE) by default, exact match with `==`
- [ ] Return `SearchResult` with query, depth, total, results list
- [ ] Tests
- [ ] Commit

### Task 4: `search.py` — deep search (depth 2 & 3)
- [ ] For each matching material, query `chunks_fts` with same query
- [ ] At depth 2: return chunk summaries (no full text)
- [ ] At depth 3: include full chunk text
- [ ] Query `annotations_fts` and `figures_fts` for matching materials
- [ ] Include matching annotations and figures alongside chunks
- [ ] Respect `chunk_limit` per material
- [ ] Tests
- [ ] Commit

### Task 5: CLI integration
- [ ] Replace `arq index rebuild` stub → call `index.rebuild_index()`, print stats
- [ ] Replace `arq index ensure` stub → call `index.ensure_index()`, print status
- [ ] Replace `arq search` stub → call `search.search()`, output JSON by default
- [ ] Add `--depth` option (only valid with `--deep`)
- [ ] `--human` flag: format results as readable tables
- [ ] `--limit` and `--chunk-limit` options
- [ ] Exit code 1 when index missing/corrupt
- [ ] Commit

### Task 6: Integration smoke test
- [ ] `arq index rebuild` on sample material → verify index created with correct row counts
- [ ] `arq search "archival habitat"` → verify card result with correct fields
- [ ] `arq search --deep "archival habitat"` → verify chunks included
- [ ] `arq search --deep --depth 3 "archival habitat"` → verify full chunk text included
- [ ] `arq search --facet domain=research "archival"` → verify facet filter works
- [ ] `arq search "nonexistent term"` → verify empty result, exit 0
- [ ] `arq index ensure` → verify skip when current
- [ ] Re-enrich a material, `arq index ensure` → verify rebuild triggers
- [ ] `arq search --human "archival"` → verify readable output
