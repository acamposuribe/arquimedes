# Arquimedes — Phase 5.5: Memory Bridge Implementation Plan

> **Status:** Implemented
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

- [x] Add tables in `src/arquimedes/index.py` or a dedicated bridge module:
  - [x] `concept_clusters`
  - [x] `concept_cluster_aliases`
  - [x] `concept_clusters_fts`
  - [x] `cluster_materials`
  - [x] `cluster_relations`
  - [x] `wiki_pages`

### M5.5.2 Builder

- [x] Create `src/arquimedes/memory.py`
- [x] Implement `memory_rebuild(force=False)`:
  - [x] load `derived/concept_clusters.jsonl`
  - [x] compute material wiki paths from extracted metadata
  - [x] compute concept wiki paths from cluster slugs
  - [x] populate bridge tables atomically
  - [x] write `derived/memory_bridge_stamp.json`
- [x] Implement `memory_ensure()` with fast-check + fingerprint fallback

### M5.5.3 CLI

- [x] Add `arq memory rebuild`
- [x] Add `arq memory ensure`
- [x] Make `arq compile` auto-run `memory_rebuild()` after successful compile
- [x] Make CLI `arq index ensure` auto-run `memory_ensure()` after the index is current
- [x] Report counts:
  - [x] clusters
  - [x] aliases
  - [x] cluster-material links
  - [x] cluster relations
  - [x] wiki pages

### M5.5.4 Search integration

- [x] Make `arq search` query `concept_clusters_fts`
- [x] Surface canonical concept hits in JSON and `--human`
- [x] Ensure alias queries resolve to canonical names

### M5.5.5 Related integration

- [x] Update `arq related` to use shared canonical cluster membership as the strongest signal
- [x] Emit explanations like `shared cluster: <canonical_name>`

### M5.5.6 Tests

- [x] Bridge rebuild writes expected rows from sample cluster file
- [x] Alias query hits the canonical cluster
- [x] Cluster -> material evidence rows preserve `source_pages` / `evidence_spans`
- [x] Cluster relations reflect shared materials
- [x] `arq related` prefers canonical-cluster matches
- [x] `arq memory ensure` skips when current and rebuilds when cluster file changes
- [x] compile integration test verifies full memory rebuild, not only cluster-table sync
- [x] CLI `index ensure` integration test verifies memory ensure is also run

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
- the normal CLI paths, not just standalone memory commands, enforce that guarantee
