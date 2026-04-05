# Arquimedes — Phase 5.5: Memory Bridge Implementation Plan

> **Status:** Ready to implement
> **Date:** 2026-04-05
> **Spec:** [Phase 5.5 memory bridge design](../specs/2026-04-05-phase5-5-memory-bridge-design.md)
> **Related:** [Phase 5 wiki compiler](../specs/2026-04-05-phase5-wiki-compiler-design.md), [Connection model](../specs/2026-04-05-connection-model.md)

## Goal

Project the canonical concept graph and wiki page identities into `search.sqlite` so agents can query the same semantic structure that humans read in the wiki.

## Scope

- add bridge tables for canonical clusters, aliases, memberships, relations, and wiki paths
- add `arq memory rebuild` and `arq memory ensure`
- make `arq search` and `arq related` use canonical clusters
- keep the phase deterministic: no new LLM calls
- make server-side `arq compile` publish both wiki and memory bridge together
- make collaborator-side `arq index ensure` refresh both local index and local memory bridge

## Tasks

### M5.5.1 Schema

- [ ] Add tables in `src/arquimedes/index.py` or a dedicated bridge module:
  - [ ] `concept_clusters`
  - [ ] `concept_cluster_aliases`
  - [ ] `concept_clusters_fts`
  - [ ] `cluster_materials`
  - [ ] `cluster_relations`
  - [ ] `wiki_pages`

### M5.5.2 Builder

- [ ] Create `src/arquimedes/memory.py`
- [ ] Implement `memory_rebuild(force=False)`:
  - [ ] load `derived/concept_clusters.jsonl`
  - [ ] compute material wiki paths from extracted metadata
  - [ ] compute concept wiki paths from cluster slugs
  - [ ] populate bridge tables atomically
  - [ ] write `derived/memory_bridge_stamp.json`
- [ ] Implement `memory_ensure()` with fast-check + fingerprint fallback

### M5.5.3 CLI

- [ ] Add `arq memory rebuild`
- [ ] Add `arq memory ensure`
- [ ] Make `arq compile` auto-run `memory_rebuild()` after successful compile
- [ ] Make `arq index ensure` auto-run `memory_ensure()` after the index is current
- [ ] Report counts:
  - [ ] clusters
  - [ ] aliases
  - [ ] cluster-material links
  - [ ] cluster relations
  - [ ] wiki pages

### M5.5.4 Search integration

- [ ] Make `arq search` query `concept_clusters_fts`
- [ ] Surface canonical concept hits in JSON and `--human`
- [ ] Ensure alias queries resolve to canonical names

### M5.5.5 Related integration

- [ ] Update `arq related` to use shared canonical cluster membership as the strongest signal
- [ ] Emit explanations like `shared cluster: <canonical_name>`

### M5.5.6 Tests

- [ ] Bridge rebuild writes expected rows from sample cluster file
- [ ] Alias query hits the canonical cluster
- [ ] Cluster -> material evidence rows preserve `source_pages` / `evidence_spans`
- [ ] Cluster relations reflect shared materials
- [ ] `arq related` prefers canonical-cluster matches
- [ ] `arq memory ensure` skips when current and rebuilds when cluster file changes

## Key files

- `src/arquimedes/memory.py`
- `src/arquimedes/cli.py`
- `src/arquimedes/search.py`
- `src/arquimedes/index.py`
- `tests/test_memory.py`
- `tests/test_search.py`

## Done when

- agents can query canonical clusters from SQLite
- the wiki graph is no longer markdown-only
- canonical concept membership is operational in `search` and `related`
- the system behaves like one memory substrate instead of two loosely coupled layers
- collaborators never need to run `arq cluster` or `arq compile` to get current canonical connections locally
