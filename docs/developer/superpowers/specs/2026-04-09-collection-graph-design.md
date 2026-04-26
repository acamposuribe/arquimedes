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
- local cluster pages
- collection-local cluster audit
- collection reflection
- material-to-home-concept relationships

### Layer 2 — Domain Bridge Graph

The bridge graph is the cross-collection integration layer **within one domain**.

Each domain owns its own bridge graph. Research and Practice do not bridge into
the same Step 2 graph.

Its scope spans all collections inside one domain, but its inputs are **not**
raw material-level concept candidates. Its inputs are compact outputs from the
local collection graph.

It owns:

- cross-collection bridge concepts inside one domain
- bridge audit across collections inside one domain
- bridge pages
- graph-level synthesis within one domain

## Semantic Units

### Local concept

This remains the Phase 3 material-level semantic hook.

Artifact:

- `extracted/<material_id>/concepts.jsonl`

It is still the raw evidence layer for one material.

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

### Domain bridge cluster

This becomes the cross-collection concept layer inside one domain.

A domain bridge cluster should connect two or more local collection clusters
from the same domain that express the same broader conceptual territory across
collection boundaries.

The atomic members of a global bridge are collection-local clusters only. Raw material-level concepts do not participate directly in Step 2 bridging.

Suggested artifact:

- `derived/domains/<domain>/global_bridge_clusters.jsonl`

Suggested record fields:

- `bridge_id`
- `domain`
- `canonical_name`
- `slug`
- `descriptor`
- `aliases[]`
- `member_local_clusters[]`
- `domain_collection_keys[]`
- `supporting_material_ids[]`
- `bridge_takeaways[]`
- `bridge_tensions[]`
- `bridge_open_questions[]`
- `helpful_new_sources[]`
- `why_this_bridge_matters`
- `supporting_collection_reflections[]`
- `confidence`
- `wiki_path`

The bridge layer should be the place where cross-collection synthesis becomes
visible inside one domain. Collection reflections may still nominate important
local materials and local clusters, but shared takeaways, tensions, and
bridge-level open questions should be materialized on the bridge rows/pages
rather than being left implicit in collection-local prose.

Suggested page path:

- `wiki/<domain>/bridge-concepts/<slug>.md`

## Ownership Rules

### Local ownership

The local collection graph is the semantic home of a material.

That means:

- material pages should link first to local clusters
- collection pages should summarize local clusters first
- local cluster audit should judge concept quality inside the collection

### Bridge ownership

The domain bridge graph does not replace local homes.

Its job is to explain cross-collection connection and synthesis inside one
domain.

That means:

- bridge pages should cite contributing local cluster pages
- bridge membership should be explainable in terms of local-cluster evidence
- the domain bridge graph should not own all semantic identity by default

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

`cluster -> compile -> memory rebuild`

In Step 1, local collection clustering becomes the first-class semantic layer even if the existing global bridge layer is temporarily retained for continuity. In implementation terms, this is basically what is now called `arq cluster`, but updated to live within collection bounds rather than full-corpus bounds.

This should be understood literally:

- same packet semantics
- same prompt semantics
- same response semantics
- same cluster-membership semantics
- only narrower staged evidence and narrower persistence boundaries

### Command shape

Recommended commands:

- `arq cluster`
- `arq cluster --domain research --collection papers`
- `arq cluster --collection papers`

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
- local cluster pages
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

### Legacy shared-bridge migration

When upgrading a pre-domain-scope vault that still has:

- `derived/global_bridge_clusters.jsonl`
- `derived/global_bridge_stamp.json`
- `wiki/shared/bridge-concepts/*.md`

the migration into per-domain bridge publication should be **explicit and operator-run**, not an implicit side effect of `compile`, `lint`, or `serve`.

The migration contract is:

- provide a deterministic CLI migrator: `arq migrate-global-bridges`
- dry-run by default; write only on explicit `--apply`
- backup every file it overwrites before applying changes
- migrate only bridges whose member local clusters can be proven to belong to exactly one domain
- refuse `--apply` when any bridge is ambiguous or mixed-domain
- rewrite migrated bridge ids to the canonical domain-prefixed shape: `global_bridge__<domain>__<slug>`
- write migrated bridge artifacts to `derived/domains/<domain>/global_bridge_clusters.jsonl`
- write migrated bridge stamps to `derived/domains/<domain>/global_bridge_stamp.json`
- copy legacy bridge pages into `wiki/<domain>/bridge-concepts/*.md`
- rewrite `wiki/shared/glossary/_index.md` links from legacy shared bridge paths to the new domain-specific bridge paths
- leave legacy shared bridge files in place as compatibility/backups until a later cleanup step

This migrator is a structural rehoming tool, not a semantic recomputation pass. It must not call the LLM or reinterpret bridge membership.

## Step 2 — Global Bridge Graph

Step 2 reintroduces large-scale conceptual connection as its own layer.

### Pipeline shape

After Step 2, the semantic publication path becomes:

`cluster -> bridge-global -> compile -> memory rebuild`

During the rollout, the first execution boundary may live inside lint rather than as a standalone command.

The intended Step 2 execution slice runs as:

`cluster -> lint(global-bridge) -> compile -> memory rebuild`

That stage should write one artifact set per domain, for example
`derived/domains/research/global_bridge_clusters.jsonl` and
`derived/domains/research/global_bridge_stamp.json`, by running an
incremental LLM clustering pass over collection-local clusters plus compact
hosting-collection context, with existing same-domain bridges provided as
memory. That memory should include the connected local-cluster reflections and
collection signals needed to support substantial cross-collection synthesis
inside the domain. The pass should run independently per domain and skip any
domain that has fewer than two collections in scope. A dedicated
`bridge-global` command can still be added later once compile, memory, and
search consume this layer directly.

Global bridge reflection should be part of this same Step 2 clustering pass. The bridge-clustering output should already include bridge takeaways, tensions, open questions, helpful new sources, and why the bridge matters, so a separate bridge concept-reflection pass is not needed. `why_this_bridge_matters` should be written as the main prose body of the bridge page rather than a short caption.

### Command shape

Current implemented entrypoint:

- `arq lint --stage global-bridge`
- `arq lint --full`

No standalone `arq bridge-global` command is currently required.

The Step 2 bridge layer is owned by lint because it depends on
collection-local semantic outputs, collection reflection state, and existing
same-domain bridge memory rather than on the collection-local clustering
command itself.

Long-term, `arq cluster` may still orchestrate stale local clustering followed by stale global bridging indirectly, but that does not require a separate public bridge command.

### Bridge inputs

The domain bridge graph should consume collection-local clusters, not raw
material-level candidates.

Required inputs for the Step 2 LLM pass:

- pending collection-local clusters: canonical names, descriptors, aliases, hosting collection, and local-cluster reflection fields
- compact hosting-collection context: collection title, main takeaways, main tensions, and why the collection matters
- existing same-domain bridge memory: current bridge canonicals, descriptors,
  aliases, member local clusters with their connected local-cluster
  reflections, supporting collection signals, and a compact subset of bridge
  synthesis fields needed for continuity rather than the full stored bridge row

Incrementality should work exactly like current clustering: only changed or
newly eligible collection-local clusters enter the domain packet, and the LLM
either links them to existing same-domain bridges or creates new same-domain
bridges.

### Exact Step 2 input packet

The Step 2 bridge-clustering pass should read two staged JSON inputs, mirroring the current collection-clustering pattern:

1. `global_bridge.packet.json`

Contains only pending collection-local clusters plus compact hosting-collection context.

Suggested shape:

```json
{
   "kind": "global_bridge_packet",
   "pending_local_clusters": [
      {
         "cluster_id": "research__Archives__local_0001",
         "domain": "research",
         "collection": "Archives",
         "collection_key": "research/Archives",
         "canonical_name": "Archive as Political Institution",
         "descriptor": "...",
         "aliases": ["archontic authority", "archive as force field"],
         "wiki_path": "wiki/research/Archives/concepts/archive-as-political-institution.md",
         "reflection": {
            "main_takeaways": ["..."],
            "main_tensions": ["..."],
            "open_questions": ["..."],
            "helpful_new_sources": ["..."],
            "why_this_concept_matters": "..."
         }
      }
   ],
   "collection_context": [
      {
         "collection_key": "research/Archives",
         "title": "Archives",
         "main_takeaways": ["..."],
         "main_tensions": ["..."],
         "why_this_collection_matters": "..."
      }
   ]
}
```

2. `global_bridge.memory.json`

Contains the current stored global bridge graph in compact form.

Suggested shape:

```json
{
   "kind": "global_bridge_memory",
   "bridges": [
      {
         "bridge_id": "global_bridge__archive-and-power",
         "canonical_name": "Archive and Power",
         "descriptor": "...",
         "aliases": ["..."],
         "member_local_clusters": [
            {
               "cluster_id": "research__Archives__local_0001",
               "collection_key": "research/Archives",
               "canonical_name": "Archive as Political Institution"
            }
         ],
         "bridge_takeaways": ["..."],
         "bridge_open_questions": ["..."],
         "why_this_bridge_matters": "..."
      }
   ]
}
```

The packet should never include raw material-level concepts. Step 2 works entirely at collection-cluster granularity.

### Exact Step 2 delta schema

The LLM response should preserve the same incremental clustering structure as the current cluster pass: attach pending members to existing bridges or create new bridges.

Suggested response schema:

```json
{
   "links_to_existing": [
      {
         "bridge_id": "required existing bridge id",
         "member_local_clusters": [
            {"cluster_id": "required pending local cluster id"}
         ]
      }
   ],
   "new_clusters": [
      {
         "canonical_name": "required string",
         "descriptor": "short bridge description",
         "aliases": ["max 4 strings"],
         "member_local_clusters": [
            {"cluster_id": "required pending local cluster id"}
         ],
         "bridge_takeaways": ["strings"],
         "bridge_tensions": ["strings"],
         "bridge_open_questions": ["strings"],
         "helpful_new_sources": ["strings"],
         "why_this_bridge_matters": "string"
      }
   ],
   "_finished": true
}
```

Rules:

- `links_to_existing` may reference only existing `bridge_id` values from bridge memory
- both `links_to_existing` and `new_clusters` may reference only pending local cluster ids from the packet
- a new bridge with members from one collection must include at least 3 local clusters
- a new bridge spanning multiple collections must include at least 2 local clusters
- cross-collection bridges are preferred, but broad within-collection syntheses are allowed when they produce a genuinely higher-order learning

### Exact Step 2 prompt intent

The prompt should stay close to the current clustering prompt, but the target concept should be different.

Instead of asking for cross-material umbrella clusters, Step 2 should ask for
broad domain bridge concepts that surface:

- the main perspectives that organize multiple collection clusters
- the main positions or debates that recur across the domain
- the main learnings or syntheses that should become first-class domain bridge pages

The prompt should explicitly say:

- you are grouping collection-local clusters into broader same-domain bridge concepts
- prefer bridges that connect multiple collections when the conceptual relation is real
- it is acceptable to create a within-collection domain bridge only when it synthesizes at least three local clusters into a meaningfully broader learning
- do not rely on name similarity alone; use descriptors, local-cluster reflections, and collection context to judge semantic fit
- never bridge local clusters across domain boundaries
- bridge canonicals should be broad, analytically meaningful, and useful as shared conceptual pages across one domain
- the bridge output must already include bridge takeaways, tensions, open questions, suggested new sources, and why the bridge matters

If graph maintenance is retained in Step 2, it should not consume collection-local audit threads directly. It should sit above the local graph and consume only collection reflections plus global bridge rows/pages, acting as a minimal global backlog rather than a second pass over local cluster quality.

### Membership thresholds

Domain bridges do not need to be cross-collection in every case, but
cross-collection bridges are preferred.

Creation thresholds:

- if all member clusters come from the same collection, the bridge must include at least three collection-local clusters
- if the bridge spans multiple collections, the bridge must include at least two collection-local clusters

### Step 2 invariants

- a domain bridge cluster stores collection-local cluster members only
- all member local clusters in one bridge must share the same domain
- cross-collection bridges are preferred, but a strong within-collection synthesis may still become a domain bridge if it generalizes at least three local clusters
- bridge pages should cite local clusters, not only raw materials
- bridge audit should judge cross-collection coherence, not collection-internal quality

## Wiki Changes

### Collection-local cluster pages

Step 1 should add:

- `wiki/<domain>/<collection>/concepts/<slug>.md`

These pages are the semantic homes for materials in that collection.

When Step 2 domain bridges exist, local cluster pages should backlink to the
domain bridge pages they participate in, so readers can move from a local home
into the cross-collection bridge layer without losing scope.

### Collection pages

Collection pages should shift from “materials plus overlapping bridge concepts” to “materials plus local clusters”.

They should render:

- materials
- local clusters
- top facets
- recent additions
- collection reflection, including `main_takeaways`, `main_tensions`, `open_questions`, `helpful_new_sources`, and `why_this_collection_matters`

### Domain bridge pages

Step 2 should keep:

- `wiki/<domain>/bridge-concepts/<slug>.md`

But these pages should clearly act as cross-collection bridges inside one
domain rather than the default home for all canonical meaning.

In the current implementation slice, bridge pages are compiled from `member_local_clusters[]` and render contributing local cluster pages as first-class members, rather than flattening the bridge back into raw material-level source concepts.

These pages should render their bridge-owned synthesis directly from the global-bridge artifact, including `bridge_takeaways`, `bridge_tensions`, `bridge_open_questions`, `helpful_new_sources`, and `why_this_bridge_matters`, rather than depending on the separate concept-reflection pipeline.

## Memory Model Changes

The SQLite graph layer should become two related graph families.

### Local graph tables

Suggested new tables:

- `local_concept_clusters`
- `local_cluster_materials`
- `local_concept_cluster_aliases`
- `local_cluster_relations`

These should carry `domain` and `collection` as first-class scope columns.

### Global bridge tables

The existing bridge-oriented tables can evolve into the global layer or be renamed accordingly.

The important requirement is structural:

- global bridge rows must point back to local cluster members

The current rollout keeps the existing `concept_clusters` family as the global bridge table surface and adds a dedicated membership table that maps each global bridge row back to its contributing local clusters.

### Wiki registry

`wiki_pages` should register:

- material pages
- collection pages
- local cluster pages
- global bridge pages

## Search And Agent Traversal

Global search should remain the default.

But the memory should support:

- open a collection as a semantic record
- list a collection’s local clusters
- traverse from a material to its local clusters
- traverse from a local concept to its global bridges
- traverse from a global bridge back into contributing collections

The current implementation exposes the bridge traversal through SQLite-backed membership rows and search helpers, while keeping default lexical search global.

Relatedness explanations should distinguish clearly between local-home overlap and shared-bridge overlap rather than collapsing both into one generic concept-overlap label.

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
2. compile local cluster pages
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
