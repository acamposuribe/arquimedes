# Phase 4 Annotation Guidance

This document is an implementation brief for how Phase 4 search/indexing must use annotations.

Annotations are first-class retrieval data in Arquimedes. Phase 4 must use them in two ways:

1. As directly searchable records
2. As ranking signals that improve material and chunk retrieval

The goal is not only "show matching annotations," but also:
- "what did I highlight about X?"
- "surface the chunks/materials I marked as important"
- "make reader intent visible in search results"

## Source Of Truth

Use these existing artifacts:
- `extracted/<material_id>/annotations.jsonl`
- `extracted/<material_id>/chunks.jsonl`
- `docs/superpowers/completed/specs/2026-04-04-phase4-search-index-design.md`
- `docs/superpowers/completed/plans/2026-04-04-phase4-search-index.md`

Important existing signals:
- annotations contain `annotation_id`, `type`, `page`, `quoted_text`, `comment`
- chunks already contain `emphasized: true|false`
- Phase 3 already treats annotations as importance signals; Phase 4 must preserve that in retrieval

## Required Phase 4 Behavior

Implement annotation support at 3 levels.

### 1. Index annotations directly

- Build `annotations` + `annotations_fts`
- Index at least:
  - `annotation_id`
  - `material_id`
  - `type`
  - `page`
  - `quoted_text`
  - `comment`
- FTS fields:
  - `quoted_text`
  - `comment`
- Empty `comment` is valid
- Missing `annotations.jsonl` means zero rows, not an error

### 2. Return annotation hits in deep search

- At depth 1: do not include annotations in result payload
- At depth 2/3: include matching annotations per material
- Returned annotation fields should be:
  - `annotation_id`
  - `type`
  - `page`
  - `quoted_text`
  - `comment`
  - `rank`
- Add a limit for annotations just like chunks
  - default recommendation: `annotation_limit = 3` per material
  - expose as CLI flag later if easy; otherwise hardcode a sane default

### 3. Use annotations as ranking signals

- Annotation matches must be able to improve parent material ranking
- Comment matches should get the strongest boost
- Quoted-text matches should get a moderate boost
- Emphasized chunks should get a modest boost in chunk ranking

Recommended ranking behavior:
- material score =
  - material FTS score
  - plus boost if same material has annotation hits
  - plus boost if same material has emphasized chunk hits
- chunk score =
  - chunk FTS score
  - plus small boost if `emphasized = 1`
  - plus boost if annotation on overlapping page matches query
- annotation score =
  - direct `annotations_fts` rank
  - prefer annotations with non-empty `comment`

## Critical Retrieval Rule

Do not only query annotations inside card-matched materials.

That is the main thing to avoid.

Instead:
- run `materials_fts`
- run `chunks_fts`
- run `annotations_fts`
- optionally run `figures_fts`
- merge results at the material level

Why:
- a material may have weak card text but a very strong annotation hit
- "what did I highlight?" must still find it

## Recommended Search Flow

### For `depth=1`

- query `materials_fts`
- apply facet filters
- return cards only

### For `depth=2`

- query `materials_fts`
- query `chunks_fts`
- query `annotations_fts`
- query `figures_fts`
- merge by `material_id`
- rerank materials using combined evidence
- return:
  - card
  - top chunk summaries
  - top annotation hits
  - top figure hits

### For `depth=3`

- same as depth 2
- include full chunk `text`

## Facet Interaction

Annotations do not introduce new facet dimensions. Facet filters still apply at the material level.

That means:
- filter candidate materials by facet/domain/collection
- then include only annotation hits belonging to those materials

Example:
- `arq search --facet domain=research "thermal mass"`
  should only return annotation hits from research materials

## Suggested Limits

Use bounded deep-search payloads.

Recommended defaults:
- `limit = 20` materials
- `chunk_limit = 5` per material
- `annotation_limit = 3` per material
- `figure_limit = 3` per material

Do not return every matching annotation by default.

## Suggested Ranking Weights

Keep them simple and deterministic. Example starting point:
- material FTS hit: base
- chunk hit: `+0.4`
- emphasized chunk hit: additional `+0.2`
- annotation quoted_text hit: `+0.5`
- annotation comment hit: `+0.8`
- figure hit: `+0.2`

Exact values can be tuned later. The important priority order is:
- comment hit > quoted_text hit > emphasized chunk > ordinary chunk > figure

## Human Output

In `--human`, show annotations clearly.

Recommended format:
- `p.12 [highlight] "thermal mass of the concrete core..." -> key finding`

If `comment` is empty:
- `p.12 [highlight] "thermal mass of the concrete core..."`

## JSON Contract

At depth 2/3, each material result may include:

```json
{
  "material_id": "...",
  "title": "...",
  "rank": 1,
  "chunks": [...],
  "annotations": [
    {
      "annotation_id": "ann_0003",
      "type": "highlight",
      "quoted_text": "...",
      "comment": "key finding",
      "page": 12,
      "rank": 1
    }
  ],
  "figures": [...]
}
```

## Index Staleness

Annotation changes must invalidate the index.

`arq index ensure` must consider:
- `annotations.jsonl` changes
- `chunks.jsonl` changes
- figure sidecar changes
- not only `meta.json`

If annotation content changes and the index is not rebuilt, search quality will be wrong.

## Tests The Agent Must Add

At minimum:

1. Annotation indexing
   - `annotations.jsonl` produces correct row count
   - `comment` and `quoted_text` are indexed

2. Direct annotation retrieval
   - query that appears only in an annotation comment still returns the material at depth 2

3. Ranking boost
   - a material with an annotation hit outranks one with only weak card text

4. Emphasized chunk boost
   - same chunk text, but `emphasized = true` should rank above `false`

5. Facet compatibility
   - annotation hit outside the filtered domain/collection is excluded

6. Bounded output
   - deep search returns at most `annotation_limit` annotation hits per material

7. Staleness
   - modifying `annotations.jsonl` makes `arq index ensure` rebuild

## Non-Goals For Phase 4

Do not add:
- LLM reranking
- semantic embeddings
- annotation summarization
- annotation-specific facets

Keep it lexical, deterministic, and inspectable.

## Short Implementation Summary

Tell the next agent:

- build `annotations` and `annotations_fts`
- query annotations independently, not only inside card hits
- merge annotation evidence into material ranking
- boost `comment` hits most strongly
- boost `emphasized` chunks modestly
- return bounded annotation results at depth 2/3
- ensure annotation file changes trigger index rebuild
