# Arquimedes — Collection Graph Architecture

> **Status:** Proposed
> **Date:** 2026-04-09
> **Related specs:** [Full system design](2026-04-04-arquimedes-knowledge-system-design.md), [Connection model](../completed/specs/2026-04-05-connection-model.md), [Phase 5 wiki compiler](../completed/specs/2026-04-05-phase5-wiki-compiler-design.md), [Phase 6 lint](../completed/specs/2026-04-05-phase6-lint-design.md)

## Purpose

Arquimedes already treats collections as strong publication and navigation units, but the canonical concept graph is still clustered at the full-corpus level. That works for a small library, but it will eventually flatten semantic neighborhoods and make bridge clustering harder to scale and harder to trust.

This proposal changes the architecture in two deliberate steps:

1. **Local collection graph**
   Collections become first-order semantic homes.
2. **Global bridge graph**
   A separate layer reconnects those local homes across collections.

The goal is not to fragment the corpus into silos. The goal is to let local meaning stabilize before broader cross-collection bridges are synthesized.

## Core Principle

Collections should become **containers of attention**, not containers of truth.

That means:

- collections should own local semantic publication
- the corpus should still remain globally searchable and globally connectable
- the global graph should emerge from compact local semantic outputs, not from one flat pool of raw material-level candidates

This is a better scaling model and a more mind-like one:

- strong local neighborhoods
- compressed intermediate representations
- broader associative bridges between those neighborhoods

## Problems In The Current Model

### Collections are already important, but only halfway

Collections already determine:

- ingest placement
- manifest metadata
- material page paths
- collection page paths
- search filtering
- collection reflection scope

But they do **not** determine:

- canonical concept-home ownership
- cluster audit scope
- semantic clustering boundaries
- the primary graph that explains why a concept belongs where it does

### The current bridge layer does two jobs at once

The existing bridge clustering layer acts as both:

- the semantic home for concepts
- the semantic bridge across collections

Those are different responsibilities.

When a single layer does both, the likely failure modes are:

- over-merged concepts
- vague canonical names
- noisier bridge memory
- weaker collection identity
- more difficult explanation of why something is connected

## Target Shape

Arquimedes should evolve into a two-level semantic graph.

### Layer 1 — Local Collection Graph

The local collection graph is the primary semantic publication layer.

Its scope is exactly one `(domain, collection)` pair.

It owns:

- local clustering of material-level concept candidates
- local concept pages
- collection-local cluster audit
- collection reflection
- material-to-home-concept relationships

### Layer 2 — Global Bridge Graph

The global bridge graph is the cross-collection integration layer.

Its scope spans the whole corpus, but its inputs are **not** raw material-level concept candidates. Its inputs are compact outputs from the local collection graph.

It owns:

- cross-collection bridge concepts
- bridge audit across collections
- bridge pages
- graph-level cross-collection synthesis

## Semantic Units

### Material concept candidate

This remains the Phase 3 local semantic hook.

Artifact:

- `extracted/<material_id>/concepts.jsonl`

It is still the raw evidence layer.

### Local collection cluster

This becomes the canonical concept home inside a collection.

A local cluster is the current bridge-cluster model rebased into one collection scope.

It may connect multiple materials, and it may also retain multiple source concepts from the same material whenever the current global clustering semantics would already allow that. The only new hard boundary in Step 1 is that it may never span collections.

Suggested artifact:

- `derived/collections/<domain>__<collection>/local_concept_clusters.jsonl`

Suggested record fields (basically what bridge concepts have now):

- `cluster_id`
- `domain`
- `collection`
- `canonical_name`
- `slug`
- `aliases[]`
- `descriptor`
- `material_ids[]`
- `source_concepts[]`
- `confidence`
- `wiki_path`

Suggested page path:

- `wiki/<domain>/<collection>/concepts/<slug>.md`

### Global bridge cluster

This becomes the cross-collection concept layer.

A global bridge cluster should connect two or more local collection clusters that express the same broader conceptual territory across collection boundaries.

Suggested artifact:

- `derived/global_bridge_clusters.jsonl`

Suggested record fields:

- `bridge_id`
- `canonical_name`
- `slug`
- `descriptor`
- `aliases[]`
- `member_local_clusters[]`
- `domain_collection_keys[]`
- `supporting_material_ids[]`
- `confidence`
- `wiki_path`

Suggested page path:

- `wiki/shared/bridge-concepts/<slug>.md`

## Ownership Rules

### Local ownership

The local collection graph is the semantic home of a material.

That means:

- material pages should link first to local concept homes
- collection pages should summarize local concept homes first
- local cluster audit should judge concept quality inside the collection

### Global ownership

The global bridge graph does not replace local homes.

Its job is to explain cross-collection connection and synthesis.

That means:

- bridge pages should cite contributing local concept pages
- bridge membership should be explainable in terms of local-cluster evidence
- the global graph should not own all semantic identity by default

## Step 1 — Local Collection Graph

Step 1 introduces collection-local semantic publication.

This step is intentionally **not** a wholly new clustering logic family.

Local collection clustering should reuse the current `arq cluster` pattern as much as possible:

- same basic clustering behavior
- same bridge-memory style incremental reasoning
- same kind of structured cluster output
- same kind of audit/reflection follow-up

The major change is **scope**, not clustering semantics. What is currently a global material-level bridge clustering pass should become the same operation bounded to one `(domain, collection)` at a time.

Step 1 should be a **standalone, non-breaking scoped version of the current semantic publication model**. In other words:

- take the current clustering behavior
- take the current cluster-audit behavior
- take the current compile/reflection flow that depends on them
- limit all of that to collection scope

Only after that works cleanly should Step 2 add a new global bridge layer that reconnects collections.

### Non-Negotiable Step 1 Constraints

Step 1 is a **scope rebinding**, not a semantic redesign.

That means:

- keep the current clustering prompt and JSON response contract exactly as they are
- keep the current cluster-membership semantics exactly as they are
- keep the current cluster-audit prompt and review semantics exactly as they are
- change only which inputs a run sees, where outputs are written, and how runs are scheduled

Step 1 must therefore **not**:

- add a new “one concept per material” rule
- auto-split clusters into narrower local clusters just because they are now collection-bound
- reinterpret old bridge clusters during migration
- introduce new cleanup heuristics, validators, or concept-selection rules that did not exist in the current global behavior

The practical test for whether Step 1 is correct is simple:

- if the current corpus effectively lives in one collection, then the existing bridge clusters should become the same clusters, just collection-bound
- migration should be structurally close to **1:1 rebasing**, not semantic recomputation
- the LLM should not need to know what a collection is; collection scope must be enforced entirely by staged inputs and orchestration

### Pipeline shape

After `arq index rebuild`, the semantic publication path becomes:

`cluster-local -> compile -> memory rebuild`

In Step 1, local collection clustering becomes the first-class semantic layer even if the existing global bridge layer is temporarily retained for continuity. In implementation terms, this is basically what is now called `arq cluster`, but updated to live within collection bounds rather than full-corpus bounds.

This should be understood literally:

- same packet semantics
- same prompt semantics
- same response semantics
- same cluster-membership semantics
- only narrower staged evidence and narrower persistence boundaries

### Command shape

Recommended commands:

- `arq cluster-local`
- `arq cluster-local --domain research --collection papers`
- `arq cluster-local --collection papers`

`arq cluster` can later become an umbrella command, but Step 1 should make collection scope explicit.

### Scheduling and parallelism

The expected scale win in Step 1 should come primarily from **parallelizing across eligible collections**, not from inventing a different clustering algorithm.

That means:

- stale detection should identify which collections actually need reclustering
- each eligible collection should be clustered independently
- multiple eligible collections may be processed in parallel
- each collection should still have an internal gate so only one local clustering run for that collection is active at a time

Suggested config shape:

- `clustering.parallel_collections`

This is the key implementation shift: one global cluster job becomes many bounded cluster jobs orchestrated concurrently.

### Local audit in Step 1

Collection-local audit is part of Step 1, not Step 2.

It should be treated exactly the same way as local clustering:

- reuse the current cluster-audit pattern
- change the audit input packet to one collection-local cluster file at a time
- emit collection-scoped review artifacts and findings
- schedule audits across eligible collections in parallel, with one audit run per collection at a time

Suggested config shape:

- `lint.parallel_collection_audits`

This keeps Step 1 standalone: the current clustering-and-audit loop continues to exist, but now within collection bounds.

The same constraint applies here too:

- local audit must not introduce a stricter or different membership model than the current global audit
- if the current global audit would preserve a cluster shape, Step 1 local audit should preserve that same shape inside collection scope

### Input scope

For one `(domain, collection)`:

- material rows only from that collection
- concept candidates only from materials in that collection
- existing local-cluster memory only for that collection

### Output scope

For one `(domain, collection)`:

- local cluster JSONL
- local cluster stamp
- local concept pages
- collection page concept section
- collection reflection grounded in those local clusters

### Step 1 invariants

- a local cluster may not span collections
- collection reflections should primarily cite local clusters
- material pages should treat local clusters as concept homes
- stale detection should be per collection
- the clustering logic itself should remain as close as practical to the current `arq cluster` behavior
- local clustering must preserve the same membership rules as the current global clustering
- local audit must preserve the same review semantics as the current global audit
- one-scope legacy migrations should preserve cluster count and cluster membership as closely as possible

### Transitional behavior

Until Step 2 exists, cross-collection connection may still rely on:

- deterministic relatedness from authors, keywords, facets, and exact concept-key overlap
- existing bridge artifacts retained temporarily during migration

Step 1 is intentionally collection-first before it is fully bridge-rich again.

## Step 2 — Global Bridge Graph

Step 2 reintroduces large-scale conceptual connection as its own layer.

### Pipeline shape

After Step 2, the semantic publication path becomes:

`cluster-local -> bridge-global -> compile -> memory rebuild`

### Command shape

Recommended commands:

- `arq bridge-global`
- `arq bridge-global --force`

Long-term:

- `arq cluster` should orchestrate stale local clustering followed by stale global bridging

### Bridge inputs

The global bridge graph should consume compact local semantics, not every raw material-level candidate.

Recommended inputs:

- local cluster canonical names
- local cluster descriptors
- local cluster material counts
- collection reflections
- local cluster reflections if added later

### Promotion rule

Not every local cluster needs to enter the global bridge pool.

Promotion should be limited to local clusters that meet one or more of:

- high confidence
- multiple materials
- repeated recurrence
- explicit importance in collection reflection
- strong cross-collection bridgeability

This keeps the global bridge layer sparse and meaningful.

### Step 2 invariants

- a global bridge cluster must include members from at least two collection scopes
- bridge pages should cite local clusters, not only raw materials
- bridge audit should judge cross-collection coherence, not collection-internal quality

## Wiki Changes

### Collection-local concept pages

Step 1 should add:

- `wiki/<domain>/<collection>/concepts/<slug>.md`

These pages are the semantic homes for materials in that collection.

### Collection pages

Collection pages should shift from “materials plus overlapping bridge concepts” to “materials plus local concept homes”.

They should render:

- materials
- local concept homes
- top facets
- recent additions
- collection reflection

### Global bridge pages

Step 2 should keep:

- `wiki/shared/bridge-concepts/<slug>.md`

But these pages should clearly act as cross-collection bridges rather than the default home for all canonical meaning.

## Memory Model Changes

The SQLite graph layer should become two related graph families.

### Local graph tables

Suggested new tables:

- `local_concept_clusters`
- `local_cluster_materials`
- `local_cluster_aliases`
- `local_cluster_relations`

These should carry `domain` and `collection` as first-class scope columns.

### Global bridge tables

The existing bridge-oriented tables can evolve into the global layer or be renamed accordingly.

The important requirement is structural:

- global bridge rows must point back to local cluster members

### Wiki registry

`wiki_pages` should register:

- material pages
- collection pages
- local concept pages
- global bridge pages

## Search And Agent Traversal

Global search should remain the default.

But the memory should support:

- open a collection as a semantic record
- list a collection’s local concept homes
- traverse from a material to its local concept homes
- traverse from a local concept to its global bridges
- traverse from a global bridge back into contributing collections

That preserves one connected knowledge system without flattening everything into one concept namespace.

## Scalability Properties

This architecture scales better because:

- local clustering cost grows with a changed collection, not the whole corpus
- Step 1 can parallelize collection-local clustering jobs while keeping each collection internally serialized
- bridge-global cost grows with promoted local clusters, not with all raw material-level concepts
- audit scope becomes more meaningful
- concept-home quality improves because local neighborhoods are tighter


## Compatibility And Migration

Recommended rollout:

1. add local-cluster artifacts, stamps, and tables
2. compile local concept pages
3. ground collection reflections primarily in local clusters
4. temporarily retain the existing bridge layer for continuity if needed
5. replace raw-material global clustering with global bridging over local semantic outputs

## Non-Goals

This proposal does not mean:

- one separate search index per collection
- one separate repo per collection
- no cross-collection association
- no global memory

Arquimedes remains one shared knowledge system.

Collections become the first-order semantic neighborhoods inside that system.
