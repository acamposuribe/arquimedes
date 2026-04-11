# Arquimedes — Collection Graph Architecture: Implementation Plan

> **Status:** In progress
> **Date:** 2026-04-09
> **Spec:** [Collection graph architecture](../specs/2026-04-09-collection-graph-design.md)
> **Related specs:** [Full system design](../specs/2026-04-04-arquimedes-knowledge-system-design.md), [Phase 5 wiki compiler](../completed/specs/2026-04-05-phase5-wiki-compiler-design.md), [Phase 6 lint](../completed/specs/2026-04-05-phase6-lint-design.md)

## Goal

Make collections first-order semantic homes without losing global associative memory.

The rollout is intentionally split into two implementation steps:

1. **Step 1: Local collection graph**
   Each collection becomes its own semantic neighborhood with local clustering, local cluster pages, and collection-first reflection.
2. **Step 2: Global bridge graph**
   A separate cross-collection bridge layer reconnects those local neighborhoods.

## Rollout Strategy

Do not replace the whole concept graph in one change.

Step 1 should land a complete collection-first semantic system even before Step 2 exists. Step 2 should then restore and improve cross-collection semantic association on top of that foundation.

---

## Step 1 — Local Collection Graph

Step 1 should be standalone and non-breaking. The intent is to keep the current semantic publication loop recognizable, but run it inside collection scope:

- current clustering behavior -> collection-local clustering
- current cluster-audit behavior -> collection-local audit
- current compile/reflection dependencies -> collection-local compile/reflection inputs

Step 2 should only be responsible for bringing those collection-scoped semantic neighborhoods back together at the global bridge level.

### Step 1 Guardrails

These constraints are mandatory for the Step 1 implementation:

- preserve current clustering semantics exactly; only narrow the scope of inputs
- preserve current cluster-audit semantics exactly; only narrow the scope of inputs
- preserve the current clustering prompt and JSON delta contract exactly
- preserve the current audit prompt and work-file contract exactly
- do not add any new membership rule, cleanup heuristic, or validator logic just because clustering is now collection-bound

In particular:

- do **not** invent a “one concept per material” rule
- do **not** auto-split existing cluster memberships during the move to collection scope
- do **not** reinterpret legacy bridge clusters during migration

For a repo whose current data effectively lives inside one collection, the expected migration result is:

- same cluster count
- same cluster memberships
- same prompt behavior
- same audit behavior
- only collection-bound ids, paths, stamps, and scheduling

### S1.1 Data model and artifact layout

- [x] Define canonical local-cluster artifact paths under `derived/collections/`
- [x] Define per-collection stamp files for staleness tracking
- [x] Define stable local cluster IDs scoped by collection
- [x] Define the local concept wiki path under `wiki/<domain>/<collection>/concepts/`
- [x] Define the local cluster JSON schema

### S1.2 Local clustering command

- [x] Reframe Step 1 explicitly as a scoped adaptation of the current `arq cluster` machinery, not a new clustering algorithm
- [x] Make `arq cluster` collection-local
- [x] Add collection-scoping options
- [x] Implement per-collection stale detection
- [x] Build local-cluster prompt packets from one collection only
- [x] Persist local-cluster outputs and stamps per collection
- [x] Extend cluster logging with collection scope
- [x] Add collection-level scheduling and internal gates so only one clustering run per collection is active at a time
- [x] Add config for cross-collection parallelism, e.g. `clustering.parallel_collections`

Notes:

- The intent is to reuse the current `arq cluster` behavior as directly as possible.
- The main engineering work is scope partitioning, per-collection staleness tracking, orchestration, and artifact relocation.
- The expected scale win comes from running many eligible collection jobs in parallel, not from changing the core clustering logic.
- No semantic reinterpretation is allowed in Step 1. The only allowed changes are scope partitioning, orchestration, ids, paths, stamps, and artifact locations.

### S1.3 Local graph projection

- [x] Add SQLite tables for local concept clusters and memberships
- [x] Add alias and relation tables for the local graph
- [x] Register local cluster pages in `wiki_pages`
- [x] Add freshness / rebuild support for the local graph projection

### S1.4 Compile changes

- [x] Compile local cluster pages into each collection tree
- [x] Change collection pages to render local clusters first
- [x] Change material pages to link first to local clusters
- [x] Add deterministic indexes for local concept navigation where useful

### S1.5 Collection reflection changes

- [x] Make collection reflection evidence packets treat local clusters as the primary concept layer
- [x] Keep material and chunk evidence as supporting context
- [x] Preserve incremental reruns per collection

### S1.6 Local audit changes

- [x] Reuse the current cluster-audit pattern for local audit rather than designing a new audit subsystem
- [x] Change audit inputs, work files, findings, and refresh cycles to collection scope
- [x] Add scheduling and internal gates so only one local audit run per collection is active at a time
- [x] Add config for cross-collection audit parallelism, e.g. `lint.parallel_collection_audits`

Notes:

- Local audit belongs in Step 1 because it is part of making current cluster behavior truly collection-local.
- Step 1 should preserve the current clustering-and-audit loop, only scoped differently.
- Local audit must preserve the same cluster-shape assumptions as the current global audit. Scope may change; audit semantics may not.

### S1.7 Search and agent traversal

- [x] Add collection -> local clusters traversal
- [x] Add material -> local clusters traversal
- [x] Preserve global lexical search across the whole corpus
- [x] Preserve deterministic cross-collection relatedness during the transition

### S1.8 Verification

- [x] Test per-collection stale detection
- [x] Test that local clusters never span collections
- [x] Test local cluster page compilation
- [x] Test collection pages using local clusters
- [x] Test local graph projection into SQLite
- [x] Test multi-collection scheduling with one internal gate per collection
- [x] Test collection-local audit scheduling and collection-scoped findings

### Step 1 exit criteria

- [x] Every enriched collection can produce local clusters
- [x] Collection pages render local clusters
- [x] Material pages link to local clusters
- [x] Collection reflections are grounded in local semantic structure
- [x] No local cluster spans collections

---

## Step 2 — Global Bridge Graph

### S2.1 Bridge input contract

- [ ] Define canonical global bridge artifact paths
- [ ] Define a global bridge stamp file
- [ ] Define the bridge-member schema using local clusters as members
- [ ] Define promotion rules from local clusters into the bridge pool

### S2.2 Global bridging command

- [ ] Add `arq bridge-global`
- [ ] Build staged bridge packets from promoted local clusters and collection reflections
- [ ] Load existing global bridge memory independently from local-cluster memory
- [ ] Make stale detection depend on promoted local-cluster changes, not all raw materials

### S2.3 Bridge projection

- [ ] Extend SQLite graph tables so global bridge rows point back to local clusters
- [ ] Add traversal from local concept -> global bridge
- [ ] Add traversal from global bridge -> contributing local clusters
- [ ] Preserve searchable bridge aliases and page identities

### S2.4 Compile changes

- [ ] Keep `wiki/shared/bridge-concepts/` as the bridge publication area
- [ ] Update bridge pages to cite local cluster pages as members
- [ ] Add backlinks from local cluster pages to bridge memberships where useful
- [ ] Distinguish local clusters from bridge concepts in shared indexes

### S2.5 Lint changes

- [ ] Add a distinct global bridge audit layer on top of Step 1 local audit
- [ ] Keep collection reflection focused on collection meaning
- [ ] Move cross-collection synthesis into the global bridge layer
- [ ] Re-evaluate whether graph maintenance should exist at all in Step 2 before adapting it to the two-layer graph

Notes:

- Step 2 should assume collection-local audit already exists.
- The lint work in Step 2 is about the new global bridge layer and graph-wide synthesis, not about making audit local for the first time.
- Graph maintenance is explicitly marked for re-thinking before any Step 2 adaptation work. Removal is allowed if it still does not justify its complexity.

### S2.6 Search and retrieval

- [ ] Add graph traversal helpers:
  material -> local concept -> global bridge -> other local concepts -> materials
- [ ] Keep default search global
- [ ] Distinguish local-home overlap from bridge overlap in relatedness explanations

### S2.7 Verification

- [ ] Test that global bridge clusters contain members from multiple collection scopes
- [ ] Test local concept -> bridge traversal
- [ ] Test bridge-page compilation from local-cluster inputs
- [ ] Test bridge stale detection using promoted local-cluster changes

### Step 2 exit criteria

- [ ] Cross-collection semantic publication no longer depends on raw material-level global clustering
- [ ] Bridge pages are built from local semantic outputs
- [ ] Agents can traverse both local homes and global bridges
- [ ] The system remains globally associative without flattening all concept homes into one layer

---

## Cross-Cutting Work

### Docs

- [ ] Update the main system spec when Step 1 ships
- [ ] Update `docs/PIPELINE.md` with the new semantic publication order
- [ ] Update `docs/PLAN.md` as milestones land

### Migration

- [x] Provide deterministic migration for old bridge-memory tables where possible
- [x] Decide what the existing `derived/bridge_concept_clusters.jsonl` means during Step 1
- [x] Add upgrade notes for existing repos

Migration requirements for Step 1:

- when the current corpus effectively lives inside one collection, `derived/bridge_concept_clusters.jsonl` should rebase into one collection-local cluster file with near-1:1 preservation of cluster count and membership
- `derived/lint/cluster_reviews.jsonl` and `derived/lint/collection_reflections.jsonl` should be rebased to local ids and local paths without semantic reinterpretation
- the migration should be deterministic and LLM-free
- the migration should not require re-enrichment, re-extraction, re-indexing, re-clustering, or re-reflection merely to preserve current semantics

### Operational safety

- [x] Make collection assignment refreshable so moved materials can be rehomed
- [x] Keep logs and stamps auditable by collection scope
- [ ] Avoid any design that forces a full-corpus semantic rebuild after a small collection change

## Immediate Next Order

Recommended execution sequence:

1. make collection assignment rehomable and trustworthy
2. add local-cluster artifacts, stamps, and SQL tables
3. implement collection-local `arq cluster`
4. compile local cluster pages and collection-home pages
5. re-ground collection reflections in local clusters
6. implement global bridging over local semantic outputs
