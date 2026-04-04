# Arquimedes — Connection Model: Implementation Plan

> **Status:** Ready to implement
> **Date:** 2026-04-05
> **Spec:** [Connection model](../specs/2026-04-05-connection-model.md)
> **Related plan:** [Phase 4 search index](2026-04-04-phase4-search-index.md)

## Vision

The connection model spec says Arquimedes should be an **artificial memory-brain**, not a searchable database. The difference: a database answers queries; a brain *associates*. Given a material, it knows what else is related and why. Given a concept, it knows which materials contributed. Given a query, it returns not just text matches but converging evidence from multiple layers.

Most of this data already exists — Phase 3 produces concept candidates, keywords, facets per material. The problem is that **none of it is queryable as connections**. `concepts.jsonl` is never indexed. There is no way to ask "what is related to this material?" or "what concepts span the collection?"

This plan adds four capabilities to Phase 4 that turn latent metadata into live connections, all using deterministic SQL queries over existing artifacts — no new LLM calls, no over-engineering.

---

## What Already Works

| Layer | Status | Connection type |
|-------|--------|-----------------|
| `pages.jsonl`: page→heading, page→figure_ref, page→annotation_ids | ✅ | structural |
| `chunks.jsonl`: chunk→source_pages, chunk→emphasized | ✅ | structural + attention |
| `fig_*.json`: figure→source_page, figure→visual_type, figure→relevance | ✅ | structural + semantic |
| `annotations.jsonl`: annotation→page, annotation→quoted_text, annotation→comment | ✅ | structural + attention |
| `meta.json`: material→authors, →domain, →collection, →keywords, →facets | ✅ | structural + semantic |
| `concepts.jsonl`: material→concept candidates with relevance + evidence | ✅ produced | semantic (NOT indexed) |
| `materials_fts`/`chunks_fts`/`annotations_fts`/`figures_fts`: card + content-first search | ✅ | retrieval |
| Emphasis boost, annotation comment boost, content-first multi-layer retrieval | ✅ | retrieval + attention |

**Phase 2 — one minor structural gap:**
`Chunk.emphasized` is boolean. It does not store *which* annotation IDs overlap. Phase 5 needs this for annotation backlinks on wiki pages.

**Phase 3 — complete.** No gaps.

**Phase 4 — the connection layer is missing:**
1. Concepts are never indexed → can't search concepts, can't list shared concepts across materials
2. No way to ask "given material X, what is related?" → no associative memory primitive
3. No way to ask "what concepts exist across the collection?" → Phase 5 can't know which concept pages to compile

---

## Tasks

### C4.1 — Index concepts in FTS5

**Priority:** HIGH (prerequisite for C4.2, C4.3, C4.4)

`concepts.jsonl` exists per material but `rebuild_index()` never reads it. Add a `concepts` base table, `concepts_fts` virtual table, and include `concepts.jsonl` in the staleness scope.

**Schema:**
```sql
CREATE TABLE IF NOT EXISTS concepts (
    concept_name  TEXT NOT NULL,
    material_id   TEXT NOT NULL,
    relevance     TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (material_id, concept_name)
);

CREATE VIRTUAL TABLE IF NOT EXISTS concepts_fts USING fts5(
    concept_name,
    material_id UNINDEXED,
    content='concepts',
    content_rowid='rowid'
);
```

Primary key is `(material_id, concept_name)` — concept names are the natural identifier, not synthetic IDs. This also makes shared-concept queries trivial: `GROUP BY concept_name`.

**Changes:**
- `index.py`: add DDL, populate from `concepts.jsonl` during rebuild, `concepts_fts` rebuild call, `IndexStats.concepts` field
- `index.py`: `_newest_input_mtime()` and `_compute_extracted_snapshot()` include `concepts.jsonl`
- `cli.py`: `arq index rebuild` output shows concept count

**Tests:** concept rows inserted with correct count; FTS match on concept name; staleness when concepts.jsonl changes.

---

### C4.2 — Concepts in search

**Priority:** HIGH (shared-signal retrieval)

Add `concepts_fts` as a fourth content-first source. A search for "archival habitat" at depth ≥ 2 should surface materials where that term is a concept candidate, even if it doesn't appear in the card summary.

**Changes:**
- `search.py`: `ConceptHit` dataclass (concept_name, relevance, rank)
- `search.py`: `_search_concepts(con, query, material_id, limit)` — FTS5 match + join to base table
- `search.py`: `_find_content_material_ids()` adds `concepts_fts` query as fourth source
- `search.py`: `MaterialCard.concepts: list[ConceptHit]`, populated at depth ≥ 2
- `search.py`: `search()` gets `concept_limit: int = 3` parameter
- `search.py`: `_combined_priority()` adds concept-match boost (+0.3 per concept hit — a concept match is a strong semantic signal, stronger than emphasis but weaker than a reader comment)
- `search.py`: `format_human()` renders concept hits
- `cli.py`: `--concept-limit` option on `arq search`

**Tests:** concept-only material surfaces at depth 2 not depth 1; concept hits attached to card; concept boost affects ranking.

---

### C4.3 — `arq related <material_id>`

**Priority:** HIGH (the core connection primitive)

This is the feature that makes Arquimedes feel like a brain. Given a material, return other materials connected through shared concepts, keywords, facets, or authors — ranked by connection strength, with the connection reasons listed.

**No LLM needed.** Pure SQL joins on existing indexed data.

**Algorithm:**

```
For material M:
  1. Find shared concepts: other materials with matching concept_name
     → weight: 1.0 per shared concept (strongest signal — LLM-identified meaning)
  2. Find shared enriched keywords: keyword intersection via JSON arrays
     → weight: 0.3 per shared keyword
  3. Find shared facets: exact match on location, historical_period, scale, etc.
     → weight: 0.5 per shared facet value
  4. Find same author
     → weight: 0.8 per shared author
  5. Score = sum of weighted connections
  6. Return top N related materials with score + connection reasons
```

**Output model:**
```python
@dataclass
class Connection:
    type: str           # "shared_concept" | "shared_keyword" | "shared_facet" | "shared_author"
    value: str          # the concept name / keyword / facet value / author name
    facet: str = ""     # for shared_facet: which facet (location, scale, etc.)
    weight: float = 0.0

@dataclass
class RelatedMaterial:
    material_id: str
    title: str
    score: float
    connections: list[Connection]
```

**Changes:**
- `search.py`: `Connection`, `RelatedMaterial` dataclasses
- `search.py`: `find_related(material_id, config, limit) → list[RelatedMaterial]`
- `search.py`: `format_related_human(material_id, related) → str`
- `cli.py`: `arq related <material_id> [--limit N] [--human]` command

**Tests:** material with shared concept appears in related; shared keyword contributes; scoring sums correctly; material doesn't relate to itself.

---

### C4.4 — `arq concepts`

**Priority:** HIGH (Phase 5 prerequisite — concept page compilation)

List all concept candidates across the collection with material counts. This is the cross-material concept map that tells Phase 5 which concept pages to create.

**Query:**
```sql
SELECT concept_name,
       COUNT(DISTINCT material_id) AS material_count,
       GROUP_CONCAT(DISTINCT material_id) AS material_ids,
       relevance
FROM concepts
GROUP BY concept_name
ORDER BY material_count DESC, concept_name
```

With only one material currently, all counts will be 1. But as the library grows, concepts appearing across 3+ materials are strong candidates for concept wiki pages.

**Changes:**
- `search.py` (or new `connections.py` if search.py gets too large — prefer keeping in search.py for now): `list_concepts(config, min_materials=1, limit=50) → list[ConceptEntry]`
- `ConceptEntry` dataclass: concept_name, material_count, material_ids, relevance
- `format_concepts_human()` for table output
- `cli.py`: `arq concepts [--min-materials N] [--limit N] [--human]`

**Tests:** concepts listed with correct counts; `--min-materials 2` filters single-material concepts.

---

### C2.1 — Chunk annotation overlap IDs

**Priority:** LOW (needed for Phase 5 annotation backlinks, not for Phase 4)

`Chunk.emphasized` is a boolean. Phase 5 will want to say "this passage was highlighted by annotation ann_003, ann_007."

**Changes:**
- `models.py`: add `annotation_overlap_ids: list[str]` to `Chunk`, default `[]`
- `chunking.py`: track `(annotation_id, normalized_text)` pairs per page alongside `emphasized_spans`; when a chunk overlaps, collect the matching annotation IDs
- Re-extraction required (`arq extract-raw`) to populate

**Tests:** chunk overlapping annotation carries the annotation ID; non-overlapping chunk has empty list.

---

## Implementation Order

```
C4.1 (concepts indexed)
  ↓
C4.2 (concepts in search) + C4.4 (arq concepts)   ← can parallelize
  ↓
C4.3 (arq related)                                ← needs all tables populated
  ↓
C2.1 (chunk overlap IDs)                           ← independent, LOW priority
```

C4.1 is the prerequisite for everything.  
C4.2 and C4.4 are independent of each other (both just read the new concepts table).  
C4.3 reads concepts, keywords, facets, and authors — it benefits from C4.1 but also uses existing tables.  
C2.1 can be deferred to just before Phase 5 starts.

---

## What This Unlocks for Phase 5

| Capability | Depends on | Phase 5 use |
|-----------|-----------|-------------|
| Concept page compilation | C4.1 + C4.4 | `arq concepts --min-materials 2` → list of concepts that deserve wiki pages |
| Shared-concept clustering | C4.1 + C4.2 | Search for a concept → all contributing materials surface together |
| Related materials sections | C4.3 | Each material wiki page gets a "related materials" section from `arq related` |
| Annotation backlinks | C2.1 | Material pages show which chunks were highlighted and by which annotations |
| Connection explainability | C4.2 + C4.3 | Every connection has a type, value, and weight — wiki can render "connected because…" |

After these tasks, the connection graph exists in queryable form. Phase 5 materializes it into pages. Phase 6 audits and strengthens it. Nothing is invented at lint time — lint only improves what already exists.

---

## Checkboxes

### Phase 4 — Connection layer

- [x] **C4.1** — `concepts` + `concepts_fts` in `index.py`; staleness scope; `IndexStats.concepts`
- [x] **C4.2** — `ConceptHit` in search; concepts in content-first pass; concept boost; `--concept-limit`
- [x] **C4.3** — `arq related <material_id>`: shared concepts/keywords/facets/authors, scored, explained
- [x] **C4.4** — `arq concepts`: cross-collection concept listing with material counts

### Phase 2 — Retroactive

- [ ] **C2.1** — `Chunk.annotation_overlap_ids: list[str]` in `models.py` and `chunking.py`
