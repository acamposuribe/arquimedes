# Phase 5: Wiki Compiler — Implementation Plan

> **Status:** Complete
> **Date:** 2026-04-05
> **Spec:** [Phase 5 wiki compiler design](../specs/2026-04-05-phase5-wiki-compiler-design.md)

---

## File Map

| # | File | Action | Responsibility |
|---|------|--------|---------------|
| 1 | `src/arquimedes/cluster.py` | Create | Concept clustering: load concepts from index, build prompt, call LLM, parse clusters, write `derived/concept_clusters.jsonl` + `derived/cluster_stamp.json` |
| 2 | `src/arquimedes/compile.py` | Create | Wiki compiler: load clusters + materials, render material pages, concept pages, index pages, glossary; incremental tracking; orphan removal |
| 3 | `src/arquimedes/compile_pages.py` | Create | Page renderers: `render_material_page()`, `render_concept_page()`, `render_index_page()`, `render_glossary()` — pure functions returning markdown strings |
| 4 | `src/arquimedes/cli.py` | Modify | Add `arq cluster` and `arq compile` commands |
| 5 | `tests/test_compile.py` | Create | All Phase 5 tests (clustering + compilation) |

## Dependency Order

```
cluster.py ─────────────► compile.py ─► cli.py
                              ▲
compile_pages.py ─────────────┘
```

`cluster.py` and `compile_pages.py` are independent of each other — can be built in parallel. `compile.py` depends on both. `cli.py` wraps `compile.py`.

---

## Tasks

### C5.1 — Concept clustering module

**Creates:** `src/arquimedes/cluster.py`

**Functions:**

- `cluster_concepts(config, *, llm_fn=None, force=False) -> dict`
  - Reads all concept records from search index (concept_name, concept_key, material_id, relevance, source_pages, evidence_spans, confidence)
  - Reads material titles from index for prompt context
  - Pre-groups by exact `concept_key` (deterministic dedup)
  - Builds prompt (system + user message with concept keys, material titles, relevance, truncated evidence)
  - Calls LLM, parses JSON response
  - **Validates** every `source_concepts` entry against the indexed concept rows: normalizes each `concept_name` to a key, rejects unknown `(material_id, normalized concept_name)` pairs, drops hallucinated references, logs warnings for repairs
  - Enriches each cluster record: derives `material_ids` from `source_concepts`, generates `slug` from `canonical_name`, attaches full provenance per source_concept from the index data
  - Writes `derived/concept_clusters.jsonl` + `derived/cluster_stamp.json`
  - Returns summary dict: `{"total_concepts": N, "clusters": M, "multi_material": K}`

- `cluster_fingerprint(config) -> str`
  - Fingerprints the full clustering input: concept keys, material IDs, material titles, relevance, evidence_spans, confidence
  - Uses `enrich_stamps.canonical_hash`

- `is_clustering_stale(config) -> bool`
  - Compares current fingerprint against `derived/cluster_stamp.json`
  - Returns True if missing, stale, or force

- `load_clusters(project_root) -> list[dict]`
  - Reads `derived/concept_clusters.jsonl`, returns list of cluster dicts
  - Utility for compile.py and tests

- `slugify(name: str) -> str`
  - Lowercases, replaces spaces with hyphens, strips non-alphanum (except hyphens)
  - Used for `slug` field and concept page paths

**LLM interaction:** Reuses `enrich_llm.get_model_id`, `enrich_llm.make_cli_llm_fn`, `enrich_llm.parse_json_or_repair` — same adapter as Phase 3.

**Index interaction:** Opens `indexes/search.sqlite` read-only. SQL queries:
```sql
SELECT concept_name, concept_key, material_id, relevance,
       source_pages, evidence_spans, confidence
FROM concepts ORDER BY concept_key, material_id;

SELECT material_id, title FROM materials;
```

---

### C5.2 — Page renderers

**Creates:** `src/arquimedes/compile_pages.py`

Pure functions — each takes data, returns a markdown string. No I/O, no side effects.

- `render_material_page(meta, clusters, chunks, annotations, figures, related) -> str`
  - Sections: title, metadata block, summary, key concepts (with links), facets, figures, annotations, related materials, source info
  - `clusters` filtered to those containing this material — used for concept links
  - `related` is a pre-computed list of `{material_id, title, reasons: [str]}`

- `render_concept_page(cluster, material_titles, related_concepts: list[dict]) -> str`
  - Sections: canonical name, aliases, overview ("appears in N materials"), per-material evidence (title + relevance + quoted spans + page refs), related concepts
  - `related_concepts` is pre-computed by `compile.py`: other clusters that share at least one material with this cluster, each as `{canonical_name, slug}`
  - All per-material evidence comes from the cluster dict — no index queries

- `render_index_page(title, entries: list[{name, path, summary}]) -> str`
  - H1 title, count, alphabetical listing with links and one-line summaries

- `render_glossary(clusters) -> str`
  - Alphabetical listing of canonical names linking to concept pages

- `_relative_link(from_path, to_path) -> str`
  - Computes relative path between two wiki pages for markdown links

- `_material_wiki_path(meta) -> str`
  - Returns `wiki/{domain}/{collection}/{material_id}.md` (or `_general/` for uncollected)

- `_concept_wiki_path(slug) -> str`
  - Returns `wiki/shared/concepts/{slug}.md`

---

### C5.3 — Wiki compiler orchestrator

**Creates:** `src/arquimedes/compile.py`

- `compile_wiki(config=None, *, force=False, force_cluster=False) -> dict`
  - Main entry point. Returns summary dict for CLI output.
  - Steps:
    1. Ensure search index exists (`arq index ensure` equivalent)
    2. Run clustering if stale or forced (`cluster.cluster_concepts`)
    3. Load clusters via `cluster.load_clusters`
    4. Load all material metadata from `extracted/*/meta.json`
    5. Determine changed materials (compare per-material stamp vs `derived/compile_stamp.json`)
    6. Determine if clusters changed (compare cluster file hash)
    7. For each changed material: load chunks, annotations, figures; compute related materials; render material page; write to wiki
    8. If clusters changed: for each cluster, compute `related_concepts` (other clusters sharing materials); render **all** concept pages; write to wiki
    9. Render all index pages (always — cheap)
    10. Render glossary
    11. Remove orphan pages (material pages for removed materials, concept pages for removed clusters)
    12. Write `derived/compile_stamp.json`

- `_material_stamp(output_dir: Path) -> str`
  - Hash of meta.json + chunks.jsonl + annotations.jsonl + figures/*.json
  - Reuses `enrich_stamps.canonical_hash`

- `_find_related(material_id, meta, clusters, index_db) -> list[dict]`
  - Shared clusters (strongest), shared keywords, shared authors, shared facets
  - Returns `[{material_id, title, reasons}]`, limit 10
  - Reads from index SQLite (same DB already open for cluster loading)

- `_remove_orphans(wiki_root, current_material_ids, current_slugs) -> list[str]`
  - Walk wiki tree; delete pages whose material_id or slug is no longer current
  - Returns list of removed paths for logging

- `_load_compile_stamp(project_root) -> dict | None`
  - Reads `derived/compile_stamp.json`, returns None if missing

- `_write_compile_stamp(project_root, material_stamps, cluster_stamp)`
  - Writes `derived/compile_stamp.json`

---

### C5.4 — CLI commands

**Modifies:** `src/arquimedes/cli.py`

#### `arq cluster`

```python
@cli.command()
@click.option("--force", is_flag=True)
def cluster(force):
    """Cluster concept candidates across materials."""
```

- Calls `cluster.cluster_concepts(config, force=force)`
- Prints: `"22 concepts → 15 clusters (3 multi-material)"`

#### `arq compile`

```python
@cli.command()
@click.option("--full", is_flag=True)
@click.option("--force-cluster", is_flag=True)
def compile(full, force_cluster):
    """Compile wiki pages from enriched materials and concept clusters."""
```

- Calls `compile.compile_wiki(config, force=full, force_cluster=force_cluster)`
- Prints summary: material pages, concept pages, index pages written

---

## Implementation Order

```
C5.1  cluster.py (concept clustering — LLM pass)
  ↓
C5.2  compile_pages.py (page renderers — pure functions, parallel with C5.1)
  ↓
C5.3  compile.py (orchestrator — depends on C5.1 + C5.2)
  ↓
C5.4  cli.py (commands — depends on C5.3)
  ↓
C5.5  tests (all in one file — validate full pipeline)
```

C5.1 and C5.2 are independent — build whichever comes first, or in parallel.

---

## Test Plan

**One file:** `tests/test_compile.py`

All tests use in-memory fixtures (fake `extracted/` directories, mock index data). LLM is always mocked — the clustering response is a canned JSON fixture.

| # | Test | What it covers |
|---|------|---------------|
| 1 | `test_cluster_parse_and_write` | Mock LLM returns valid JSON → `concept_clusters.jsonl` written with correct fields (cluster_id, canonical_name, slug, aliases, material_ids, source_concepts with full provenance, confidence) |
| 2 | `test_cluster_staleness_skip` | Unchanged concept fingerprint → clustering skipped, no LLM call |
| 3 | `test_material_page_sections` | Given enriched meta + chunks + annotations + figures + clusters → rendered page contains all expected sections (title, summary, concepts with links, facets, figures, annotations, related, source) |
| 4 | `test_concept_page_evidence` | Given cluster with 2 materials → concept page lists both with relevance and quoted evidence spans |
| 5 | `test_index_pages` | Master index lists correct material/concept counts |
| 6 | `test_incremental_skip` | Unchanged material stamp → material page not rewritten; unchanged cluster stamp → concept pages not rewritten |
| 7 | `test_orphan_removal` | Material removed from manifest → wiki page deleted; cluster removed → concept page deleted |

---

## Checklist

- [x] **C5.1** `cluster.py` — concept clustering module with LLM call, staleness, `derived/` output
- [x] **C5.2** `compile_pages.py` — pure page renderers (material, concept, index, glossary)
- [x] **C5.3** `compile.py` — orchestrator (incremental tracking, related materials, orphan removal)
- [x] **C5.4** `cli.py` — `arq cluster` and `arq compile` commands
- [x] **C5.5** `tests/test_compile.py` — 7 tests covering clustering + compilation
