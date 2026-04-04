# Phase 4: Search Index & Retrieval — Design Spec

> **Status:** Approved design
> **Date:** 2026-04-04
> **Parent spec:** [Full system design](2026-04-04-arquimedes-knowledge-system-design.md)

## Overview

Phase 4 makes the knowledge base queryable. It builds a local SQLite FTS5 index from enriched materials, provides multi-depth retrieval via `arq search`, and exposes faceted filtering. The index is gitignored and rebuilt locally — each device maintains its own copy.

**Primary consumers:** agents (via CLI or future MCP), collaborators (via future web UI). Agents are first-class — output format, token budgets, and depth control are designed for machine consumption.

## Design Principles

1. **JSON by default.** `arq search` outputs machine-parseable JSON. `--human` flag for pretty-printed tables. Agents get stable contracts; humans opt in to nice formatting.
2. **Configurable depth.** `arq search` returns cards (depth 1). `--deep` returns cards + chunk hits (depth 2). `--deep --depth 3` adds full chunk text. Callers control token spend.
3. **Deterministic, no LLM.** Index build and search are pure code — no LLM calls. FTS5 ranking handles relevance.
4. **Cheap staleness.** `arq index ensure` uses a fast mtime/count gate, falling back to full content hash only when ambiguous.

## Index Schema (SQLite FTS5)

Four FTS5 tables, plus a metadata table for staleness tracking.

### `materials` table

Full-text search over document-level metadata. One row per material.

| Column | Source | Type | Notes |
|--------|--------|------|-------|
| `material_id` | meta.json | TEXT PK | Not in FTS — used for joins/lookups |
| `title` | meta.json | TEXT | Raw field |
| `summary` | meta.json → summary.value | TEXT | Enriched; empty if not enriched |
| `keywords` | meta.json → keywords.value | TEXT | Joined with spaces for FTS |
| `raw_keywords` | meta.json | TEXT | Joined with spaces for FTS |
| `domain` | meta.json | TEXT | `practice` or `research` |
| `collection` | meta.json | TEXT | Second-level folder or `_general` |
| `document_type` | meta.json → document_type.value | TEXT | Enriched classification |
| `raw_document_type` | meta.json | TEXT | Deterministic classification |
| `authors` | meta.json | TEXT | Joined with spaces |
| `year` | meta.json | TEXT | |
| `file_type` | meta.json | TEXT | pdf, image, scanned_document |
| `page_count` | meta.json | INTEGER | |

**Facet columns** (from `meta.json → facets.<field>.value`, empty if absent):

| Column | Example values |
|--------|---------------|
| `building_type` | |
| `scale` | detail, building, urban, territorial |
| `location` | |
| `jurisdiction` | |
| `climate` | |
| `program` | |
| `material_system` | |
| `structural_system` | |
| `historical_period` | |
| `course_topic` | |
| `studio_project` | |

**Implementation:** Two SQLite tables — a regular table `materials` with all columns (for exact-match facet filtering and column retrieval) and an FTS5 virtual table `materials_fts` indexing the text-searchable columns (title, summary, keywords, raw_keywords, authors). Facet filtering happens via JOIN against `materials` WHERE clauses.

### `chunks` table

Full-text search over chunk-level data. One row per chunk.

| Column | Source | Type | Notes |
|--------|--------|------|-------|
| `chunk_id` | chunks.jsonl | TEXT PK | |
| `material_id` | derived | TEXT FK | |
| `text` | chunks.jsonl | TEXT | Full chunk text |
| `summary` | chunks.jsonl → summary.value | TEXT | One-line summary |
| `keywords` | chunks.jsonl → keywords.value | TEXT | Joined with spaces |
| `source_pages` | chunks.jsonl | TEXT | JSON array as string |
| `emphasized` | chunks.jsonl | INTEGER | 0 or 1 |
| `content_class` | chunks.jsonl | TEXT | argument, methodology, etc. |

**Implementation:** Regular table `chunks` + FTS5 virtual table `chunks_fts` indexing (summary, text, keywords).

### `figures` table

One row per figure.

| Column | Source | Type | Notes |
|--------|--------|------|-------|
| `figure_id` | fig_NNN.json | TEXT PK | |
| `material_id` | derived | TEXT FK | |
| `description` | fig_NNN.json → description.value | TEXT | LLM-generated |
| `caption` | fig_NNN.json → caption.value | TEXT | Extracted/inferred |
| `visual_type` | fig_NNN.json → visual_type.value | TEXT | plan, section, photo, etc. |
| `source_page` | fig_NNN.json | INTEGER | |
| `relevance` | fig_NNN.json | TEXT | substantive, decorative, front_matter |
| `image_path` | fig_NNN.json | TEXT | Relative path to image file |

**Implementation:** Regular table `figures` + FTS5 virtual table `figures_fts` indexing (description, caption).

### `annotations` table

One row per reader annotation.

| Column | Source | Type | Notes |
|--------|--------|------|-------|
| `annotation_id` | annotations.jsonl | TEXT PK | |
| `material_id` | derived | TEXT FK | |
| `type` | annotations.jsonl | TEXT | highlight, note, underline, etc. |
| `page` | annotations.jsonl | INTEGER | |
| `quoted_text` | annotations.jsonl | TEXT | The highlighted span |
| `comment` | annotations.jsonl | TEXT | Reader's note text |

**Implementation:** Regular table `annotations` + FTS5 virtual table `annotations_fts` indexing (quoted_text, comment).

### `index_state` table

Single-row metadata table for staleness tracking.

| Column | Type | Notes |
|--------|------|-------|
| `built_at` | TEXT | ISO 8601 timestamp of last rebuild |
| `manifest_hash` | TEXT | sha256 of materials.jsonl content |
| `material_count` | INTEGER | Number of materials indexed |
| `extracted_snapshot` | TEXT | sha256 of all index-input file contents (meta.json, chunks.jsonl, annotations.jsonl, all figure sidecars) |

## Index Build (`arq index rebuild`)

Full rebuild from scratch:

1. Read `manifests/materials.jsonl` to get all material IDs
2. For each material in `extracted/`:
   - Read `meta.json` → populate `materials` row
   - Read `chunks.jsonl` → populate `chunks` rows
   - Read `figures/*.json` → populate `figures` rows
   - Read `annotations.jsonl` → populate `annotations` rows
3. Extract `.value` from `EnrichedField` wrappers — only plain values go into the index
4. Build FTS5 virtual tables from content tables
5. Write `index_state` row with current snapshot metadata
6. Write index to `indexes/search.sqlite` (atomic: write to temp, rename)

**Value extraction:** For enriched fields (`summary`, `keywords`, `document_type`, facets, etc.), the indexer reads `field.value`. If the field is `null` (not yet enriched), the column gets an empty string. The index works with partially-enriched materials — unenriched materials are still findable by title, raw_keywords, and raw_document_type.

**Keywords handling:** `keywords.value` is a list of strings. Joined with spaces for FTS indexing: `["thermal mass", "concrete"] → "thermal mass concrete"`.

**Facet handling:** Each facet field in `ArchitectureFacets` may be absent (material has no value for it) or present with a `.value`. Only non-null facets produce non-empty column values.

## Staleness Detection (`arq index ensure`)

Two-tier check:

### Fast gate (cheap, runs first)

1. If `indexes/search.sqlite` doesn't exist → stale
2. Read `index_state` row from the SQLite DB
3. Compare `material_count` against line count of `manifests/materials.jsonl`
4. Compare max mtime of all index-input files (`extracted/*/meta.json`, `*/chunks.jsonl`, `*/annotations.jsonl`, `*/figures/fig_*.json`) against `built_at`
5. If count matches AND no input file is newer → **not stale, skip rebuild**

### Hash verification (runs only if fast gate is inconclusive)

The fast gate can miss in-place re-enrichment where mtime advances but material count stays the same. If the fast gate says "maybe stale" (count matches but any input file mtime is newer):

1. Compute `manifest_hash` = sha256 of `materials.jsonl` content
2. Compute `extracted_snapshot` = sha256 of sorted concatenation of file-content hashes for every `meta.json`, `chunks.jsonl`, `annotations.jsonl`, and `fig_*.json` across all materials
3. Compare both against stored `index_state` values
4. If either differs → stale, rebuild

### Behavior

- `arq index ensure` → runs fast gate, optionally hash verify, rebuilds if stale, prints whether rebuilt or skipped
- Exit code 0 always (staleness is not an error)
- `arq index rebuild` → unconditional full rebuild, ignores staleness

## Search (`arq search`)

### Depth model

| Depth | What's returned | Token cost estimate | When to use |
|-------|----------------|---------------------|-------------|
| 1 (default) | Material cards | ~100 tokens/result | Browsing, filtering, agent triage |
| 2 (`--deep`) | Cards + matching chunk summaries | ~150 tokens/result | Agent retrieval, narrowing down |
| 3 (`--deep --depth 3`) | Cards + chunks with full text | ~600 tokens/result | Full answer context, final retrieval |

### Depth 1: Card search

Query the `materials_fts` table. Return matching material cards:

```json
{
  "query": "thermal mass",
  "depth": 1,
  "total": 3,
  "results": [
    {
      "material_id": "a1b2c3d4e5f6",
      "title": "Thermal Mass in Mediterranean Climate",
      "summary": "A study of thermal mass strategies...",
      "domain": "research",
      "collection": "thermal-mass",
      "document_type": "paper",
      "year": "2021",
      "authors": "García, López",
      "keywords": ["thermal mass", "Mediterranean", "passive design"],
      "rank": 1
    }
  ]
}
```

### Depth 2: Content-first cards + chunk hits

At depth 2, the search is **content-first**: after running card-layer FTS, an additional pass queries `chunks_fts` and `annotations_fts` directly. Materials with strong chunk or annotation matches that did not rank in the card-layer results are fetched by their material_id and appended (after card-layer matches). This ensures that a paper with a pivotal annotation — e.g., "what did I highlight about thermal bridges?" — surfaces even if the material's title/summary is generic.

Run card search, then for each matching material, query `chunks_fts` with the same query. Return cards with their best matching chunk summaries:

```json
{
  "query": "thermal mass",
  "depth": 2,
  "total": 3,
  "results": [
    {
      "material_id": "a1b2c3d4e5f6",
      "title": "Thermal Mass in Mediterranean Climate",
      "summary": "A study of thermal mass strategies...",
      "domain": "research",
      "document_type": "paper",
      "rank": 1,
      "chunks": [
        {
          "chunk_id": "chk_00005",
          "summary": "Concrete walls with 300mm thickness provide optimal thermal lag...",
          "source_pages": [12, 13],
          "emphasized": true,
          "content_class": "argument",
          "rank": 1
        },
        {
          "chunk_id": "chk_00012",
          "summary": "Night ventilation combined with exposed thermal mass reduces...",
          "source_pages": [24],
          "emphasized": false,
          "content_class": "case_study",
          "rank": 2
        }
      ]
    }
  ]
}
```

### Depth 3: Cards + chunks with full text

Same as depth 2, but each chunk includes its full `text` field in addition to `summary`. Higher token cost, but provides complete answer context without separate `arq read` calls.

```json
{
  "chunks": [
    {
      "chunk_id": "chk_00005",
      "summary": "Concrete walls with 300mm thickness...",
      "text": "The experimental results demonstrate that concrete walls...",
      "source_pages": [12, 13],
      "emphasized": true,
      "content_class": "argument",
      "rank": 1
    }
  ]
}
```

### Faceted filtering

Facets are exact-match filters applied as WHERE clauses on the `materials` table before FTS ranking. Multiple `--facet` flags are AND-combined.

```
arq search --facet domain=research "thermal mass"
arq search --facet domain=practice --facet scale=building "fire code"
arq search --collection thermal-mass "Mediterranean climate"
```

`--collection` is a shorthand for `--facet collection=<value>`.

**Filterable fields:** domain, collection, document_type, file_type, year, building_type, scale, location, jurisdiction, climate, program, material_system, structural_system, historical_period, course_topic, studio_project.

**Partial match for facets:** Facet filter uses SQL `LIKE` with prefix matching for flexibility: `--facet location=India` matches "India, London, Cairo...". Exact match available via `--facet location==India` (double equals).

### Result limits

- `--limit N` — max material cards returned (default: 20)
- `--chunk-limit N` — max chunks per material at depth 2/3 (default: 5)
- `--annotation-limit N` — max annotations per material at depth 2/3 (default: 10)
- `--figure-limit N` — max figures per material at depth 2/3 (default: 5)

### Annotation and figure results

At depth 2+, matching annotations and figures are included alongside chunks when they match the query:

```json
{
  "material_id": "a1b2c3d4e5f6",
  "title": "...",
  "chunks": [...],
  "annotations": [
    {
      "annotation_id": "ann_0003",
      "type": "highlight",
      "quoted_text": "thermal mass of the concrete core...",
      "comment": "key finding",
      "page": 12
    }
  ],
  "figures": [
    {
      "figure_id": "fig_0004",
      "description": "Thermal performance graph comparing...",
      "visual_type": "chart",
      "source_page": 15
    }
  ]
}
```

At depth 1, annotations and figures are not included (card-level only).

## CLI Interface

### `arq search <query>`

```
arq search "thermal mass"                           # depth 1, JSON
arq search "thermal mass" --human                   # depth 1, pretty table
arq search --deep "thermal mass"                    # depth 2
arq search --deep --depth 3 "thermal mass"          # depth 3
arq search --facet domain=research "thermal mass"   # faceted
arq search --collection thermal-mass "climate"      # collection filter
arq search --limit 5 "fire code"                    # limit results
arq search --deep --chunk-limit 3 "thermal mass"    # limit chunks per material
```

**Exit codes:**
- 0: search completed (even if 0 results)
- 1: index missing or corrupt (suggests `arq index rebuild`)

### `arq index rebuild`

Unconditional full rebuild.

```
arq index rebuild
```

Output:
```
Building search index...
  materials: 42
  chunks: 1,847
  figures: 312
  annotations: 89
Index built in 2.1s → indexes/search.sqlite
```

### `arq index ensure`

Smart rebuild — skips if current.

```
arq index ensure
```

Output (when current):
```
Index is current (42 materials, built 2026-04-04T15:30:00Z).
```

Output (when stale):
```
Index is stale (3 new materials since last build).
Building search index...
  materials: 45
  chunks: 1,982
  figures: 328
  annotations: 95
Index rebuilt in 2.3s → indexes/search.sqlite
```

## Human-Readable Output (`--human`)

Card search:
```
 #  ID            Title                                    Domain    Type     Year
 1  a1b2c3d4e5f6  Thermal Mass in Mediterranean Climate    research  paper    2021
 2  b2c3d4e5f6a1  Building Code § 4.3 — Thermal Regs      practice  regulation 2023
 3  c3d4e5f6a1b2  Passive House Handbook                   practice  monograph 2019

3 results for "thermal mass"
```

Deep search (depth 2):
```
━━━ 1. Thermal Mass in Mediterranean Climate (a1b2c3d4e5f6) ━━━
    research · paper · 2021
    "A study of thermal mass strategies in Mediterranean..."

    Chunks:
      p.12-13 ★ [argument] Concrete walls with 300mm thickness provide optimal...
      p.24      [case_study] Night ventilation combined with exposed thermal mass...

    Annotations:
      p.12 [highlight] "thermal mass of the concrete core..." → key finding
```

## File Layout

```
indexes/
  search.sqlite            # FTS5 index (gitignored)
```

The `index_state` table lives inside `search.sqlite` itself — no separate file.

## Configuration

No new config keys needed for Phase 4. The index path is derived from the repo structure (`indexes/search.sqlite`). Limits and defaults are hardcoded with sensible values and overridable via CLI flags.

Future config keys (if needed later):
```yaml
search:
  default_limit: 20
  default_chunk_limit: 5
  index_path: "indexes/search.sqlite"
```

## Error Handling

- **No index file:** `arq search` prints error message suggesting `arq index rebuild` and exits 1
- **Corrupt index:** caught by SQLite errors, same suggestion
- **Empty results:** exit 0, return `{"query": "...", "depth": N, "total": 0, "results": []}`
- **Missing enrichment:** unenriched fields are empty strings in the index — materials are still findable by raw fields
- **No extracted materials:** `arq index rebuild` creates an empty index (valid, 0 rows)

## Performance Considerations

- **FTS5 is fast.** SQLite FTS5 handles thousands of documents with sub-millisecond query times. No need for external search engines.
- **Atomic writes.** Index build writes to a temp file, then renames. No partial/corrupt index states.
- **Depth 1 is cheap.** Card search touches only the `materials_fts` table. Depth 2+ adds chunk queries per matching material.
- **Token budgets.** The depth model exists specifically for agent token efficiency. Most agent workflows should use depth 1 for triage, depth 2 for narrowing, and `arq read` for full content.

## Scope Boundaries

**In scope for Phase 4:**
- `arq index rebuild` — full index build
- `arq index ensure` — smart staleness check + rebuild
- `arq search` — card search with JSON output
- `arq search --deep` — multi-depth retrieval (cards + chunks + optional full text)
- `arq search --human` — pretty-printed output
- Faceted filtering (`--facet`, `--collection`)
- Annotation and figure results at depth 2+

**Out of scope (later phases):**
- Semantic/embedding search (future enhancement)
- Reranking (LLM or learned)
- MCP tool wrappers (Phase 7)
- Web UI search (Phase 8)
- Auto-rebuild on sync (Phase 9 — `arq sync` calls `arq index ensure`)
