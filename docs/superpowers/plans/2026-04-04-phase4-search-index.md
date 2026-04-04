# Phase 4: Search Index & Retrieval — Implementation Plan

**Spec:** [Phase 4 search index design](../specs/2026-04-04-phase4-search-index-design.md)
**Date:** 2026-04-04
**Status:** Complete

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
- [x] `ensure_index(config) → (rebuilt: bool, stats: IndexStats | None)`
- [x] Fast gate: file exists, material_count vs manifest line, max mtime of all index-input files (meta.json, chunks.jsonl, annotations.jsonl, fig_*.json) vs built_at
- [x] Hash verify (when fast gate inconclusive): manifest_hash + extracted_snapshot — snapshot covers full file content, not just enrichment stamps
- [x] `_compute_manifest_hash() → str`
- [x] `_compute_extracted_snapshot() → str` — sha256 of file-content hashes for every meta.json, chunks.jsonl, annotations.jsonl, and all fig_*.json
- [x] Tests: snapshot changes on chunks/annotations/figure edits; ensure_index rebuilds on each
- [x] Commit

### Task 3: `search.py` — card search (depth 1)
- [x] `search(query, config, *, depth=1, facets=None, collection=None, limit=20, chunk_limit=5, annotation_limit=10, figure_limit=5) → SearchResult`
- [x] FTS5 query against `materials_fts`
- [x] Facet filtering via JOIN + WHERE on `materials` table
- [x] `--collection` as shorthand for `facet collection=<value>`
- [x] Facet prefix match (LIKE) by default, exact match with `==`
- [x] Return `SearchResult` with query, depth, total, results list
- [x] Tests
- [x] Commit

### Task 4: `search.py` — deep search (depth 2 & 3)
- [x] Content-first at depth 2+: also query `chunks_fts` and `annotations_fts` directly; surface materials with content matches that didn't match at card layer
- [x] For each matching material, query `chunks_fts` with same query
- [x] At depth 2: return chunk summaries (no full text)
- [x] At depth 3: include full chunk text
- [x] Query `annotations_fts` and `figures_fts` for matching materials
- [x] Include matching annotations and figures alongside chunks
- [x] Respect `chunk_limit`, `annotation_limit`, `figure_limit` per material
- [x] Tests: content-only materials surface at depth 2 but not depth 1; facets still filter content-only materials
- [x] Commit

### Task 5: CLI integration
- [x] Replace `arq index rebuild` stub → call `index.rebuild_index()`, print stats
- [x] Replace `arq index ensure` stub → call `index.ensure_index()`, print status
- [x] Replace `arq search` stub → call `search.search()`, output JSON by default
- [x] `--depth` option (overrides `--deep` default of 2)
- [x] `--human` flag: format results as readable tables
- [x] `--limit`, `--chunk-limit`, `--annotation-limit`, `--figure-limit` options
- [x] Exit code 1 when index missing/corrupt
- [x] Commit

### Task 6: Integration smoke test
- [x] `arq index rebuild` on sample material → verify index created with correct row counts
- [x] `arq search "archival habitat"` → verify card result with correct fields
- [x] `arq search --deep "archival habitat"` → verify chunks included
- [x] `arq search --deep --depth 3 "archival habitat"` → verify full chunk text included
- [x] `arq search --facet domain=research "archival"` → verify facet filter works
- [x] `arq search "nonexistent term"` → verify empty result, exit 0
- [x] `arq index ensure` → verify skip when current
- [x] `arq search --human "archival"` → verify readable output
