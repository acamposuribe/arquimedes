# Concept Graph Improvement — Implementation Plan

> **Status:** Proposed
> **Date:** 2026-04-05
> **Spec:** [Concept graph improvement spec](2026-04-05-concept-graph-improvement-spec.md)

## Goal

Improve concept quality and cross-material connections by:
- extracting two concept layers in Phase 3
- adding a second, bridge-oriented clustering pass in Phase 5

## Files

| File | Action | Purpose |
|---|---|---|
| `src/arquimedes/enrich_prompts.py` | Modify | Keep local concept prompt close to current behavior; add `concepts_bridge_candidates` instructions/schema |
| `src/arquimedes/enrich_document.py` | Modify | Parse and persist local concepts + bridge candidates |
| `src/arquimedes/models.py` | Modify | Add `concept_type` support if needed in concept model |
| `src/arquimedes/index.py` | Modify | Index concept type and support separate local vs bridge candidate queries |
| `src/arquimedes/cluster.py` | Refactor | Split current clustering into local clustering + bridge clustering |
| `src/arquimedes/compile.py` | Modify later | Consume both cluster files |
| `tests/test_enrich_document.py` | Modify | Cover dual concept output parsing and persistence |
| `tests/test_index.py` | Modify | Cover concept type indexing |
| `tests/test_cluster.py` | Expand | Cover local vs bridge clustering prompts and validation |

## Work Plan

### I1 — Add bridge candidates without blowing up Phase 3

Implement:
- `concepts_local`
- `concepts_bridge_candidates`

Tasks:
- keep current local concept behavior largely intact
- keep local concepts only lightly qualified when historical detail genuinely helps
- update document prompt schema
- add explicit prompt rules for the two concept roles
- parse both fields in `enrich_document.py`
- write both to `concepts.jsonl`
- add `concept_type`

Definition of done:
- extracted concepts can distinguish `local` from `bridge_candidate`
- document enrichment does not require larger raw-text input than today

### I2 — Index support for concept type

Implement:
- index `concept_type`
- make local and bridge candidates queryable separately

Tasks:
- extend `concepts` table if needed
- persist `concept_type`
- keep current `concept_key` normalization

Definition of done:
- index can filter or group by `local` vs `bridge_candidate`

### I3 — Local clustering path

Keep current Phase 5 behavior as the precise layer.

Tasks:
- cluster only `concepts_local`
- keep strict validation
- preserve strong provenance

Definition of done:
- current precise concept pages still work

### I4 — Bridge clustering path

Add a second clustering pass.

Input packet per material should include:
- title
- summary
- keywords
- top local concepts
- bridge candidates
- strongest evidence snippets

Output:
- broader canonical bridge clusters
- only for concepts that connect multiple materials

Definition of done:
- `derived/bridge_concept_clusters.jsonl` exists
- bridge clusters are broader and more cross-material than local clusters
- bridge clustering runs on compact packets, not raw full text

### I5 — Validation rules for bridge clusters

Implement:
- source concept validation against real indexed concepts
- allow multiple source concepts from the same material
- reject hallucinated refs

Definition of done:
- bridge clusters remain provenance-safe without collapsing back to one-concept-per-material rigidity

### I6 — Consumption plan

Do not wire everything at once.

Order:
1. extraction
2. indexing
3. local clustering
4. bridge clustering
5. later: compile/search/memory consumption

## Short-Term Priority

Do first:
1. add `concepts_bridge_candidates` while keeping local concepts close to current behavior
2. concept type indexing
3. bridge clustering artifact

Do later:
4. wiki representation of bridge concepts
5. search/relatedness weighting from bridge graph
6. memory bridge duplication of bridge graph

## Risks

### Risk 1
Bridge concepts become too vague.

Mitigation:
- require cross-material support
- forbid trivial umbrellas

### Risk 2
Two-layer concept system becomes confusing.

Mitigation:
- clear naming:
  - `local`
  - `bridge_candidate`
  - `bridge_cluster`

### Risk 3
Prompt cost increases too much.

Mitigation:
- local concept extraction changes should be prompt/schema-level, not input-size-level
- bridge clustering should be a separate pass
- material-aware packets should be compact summaries, not raw full-text

## Success Checks

- local concepts are richer and less fragmentary
- bridge clusters create better cross-paper links
- concept wiki stops being only a list of narrow pages
- related-material retrieval improves without losing provenance
- Phase 3 cost stays near current levels
- most added cost comes from the explicit bridge pass, which is acceptable
