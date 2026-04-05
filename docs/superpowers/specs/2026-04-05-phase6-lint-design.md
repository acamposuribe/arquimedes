# Phase 6 — Wiki Linting, Reflection, and Memory Growth

> **Status:** Ready to implement
> **Parent spec:** [Full system design](2026-04-04-arquimedes-knowledge-system-design.md)
> **Related specs:** [Phase 5 wiki compiler](../completed/specs/2026-04-05-phase5-wiki-compiler-design.md), [Phase 5.5 memory bridge](../completed/specs/2026-04-05-phase5-5-memory-bridge-design.md), [Connection model](../completed/specs/2026-04-05-connection-model.md), [Collection pages addendum](../completed/specs/2026-04-05-phase5-collection-pages-design.md)

## Purpose

Phase 6 is not only a health checker.

It is the first reflective maintenance layer: the point where Arquimedes starts improving its own graph, deepening concept pages, deepening collection pages, and turning the knowledge base into a more useful thinking partner.

Phase 5 publishes structure.

Phase 6 reviews that structure, improves it, and feeds the results back into both:
- the wiki
- the machine-queryable memory layer

## Core Principle

Run deterministic checks first.

Only after the graph and files are structurally healthy should the heavier LLM passes run.

This keeps Phase 6 disciplined:
- fast checks first
- reflective synthesis second
- memory update last

## Ordering And Parallelism

Phase 6 should maximize parallelism only where it does not weaken reflective quality.

Recommended order:
1. deterministic lint
2. cluster audit
3. concept-page reflection
4. collection-page reflection
5. graph reflection
6. apply accepted changes
7. memory rebuild

Parallelize when safe:
- deterministic checks may run in parallel
- concept-page reflections may run in parallel across independent stale clusters
- collection reflections may run in parallel across independent stale collections

Keep ordered when quality depends on prior outputs:
- cluster audit should run before concept-page reflection
- concept-page reflection should run before collection reflection when collection packets depend on improved concept understanding
- graph reflection should run last among LLM passes so it sees the best available page/graph state
- memory rebuild must run after applied changes

## Implementation Priority

Phase 6 is broad, so implementation should be staged.

Priority order:
1. deterministic lint core
2. cluster audit
3. concept-page reflection
4. collection-page reflection
5. memory projection of accepted reflections
6. global graph reflection

This keeps the phase realistic.

The most valuable early wins are:
- better concept pages
- better collection pages
- reflective memory records that agents can query

Global graph reflection is important, but it should come after the page-level reflective loop is working.

## Scope

Phase 6 owns 3 kinds of work:

1. deterministic health checks
2. LLM graph audit
3. LLM reflective synthesis for concept and collection pages

## Deterministic Pass

`arq lint --quick` runs only deterministic checks.

Required checks:
- broken links
- orphaned materials
- orphaned wiki pages
- missing required metadata
- stale enrichment
- stale index
- stale memory bridge
- duplicate materials
- missing compiled pages for existing materials / collections / clusters

Output:
- terminal report
- optional machine-readable JSON on `--json`
- structured JSON report in `derived/lint/deterministic_report.json`

CLI exit codes:
- `0` when no deterministic issues are found
- `1` when deterministic issues exist but none are high severity
- `2` when deterministic findings include at least one high-severity issue

`--fix` may auto-apply only deterministic fixes such as:
- rebuild index
- rebuild memory bridge
- regenerate missing pages through compile if safe

`--full` also auto-applies those same safe fixes after the reflective passes, so full lint is a maintenance action rather than a read-only report.

## LLM Passes

`arq lint --full` runs deterministic checks first, then these reflective passes.

All expensive passes should be dirty-set driven.

Do not rerun every reflection on every lint pass.

### 1. Cluster Audit

Review `derived/concept_clusters.jsonl` and current concept pages.

Tasks:
- detect over-merged clusters that should split
- detect missed equivalences that should merge
- detect weak single-material clusters
- detect poor canonical names
- detect missing materials that belong in an existing cluster

Output artifact:
- `derived/lint/cluster_reviews.jsonl`

Each record should include:
- `cluster_id`
- `finding_type`
- `severity`
- `recommendation`
- `affected_material_ids`
- `affected_concept_names`
- `evidence`

Eligibility:
- clusters with `material_count >= 2` should always be eligible when stale
- single-material clusters may be audited more lightly
- unchanged clusters should be skipped

### 2. Concept Page Reflection

For each concept page, synthesize what the connected materials jointly say.

This is the most important new Phase 6 responsibility.

Add or refresh structured sections such as:
- `main_takeaways`
- `main_tensions`
- `open_questions`
- `why_this_concept_matters`

These are not generic summaries.

They are cross-material reflective synthesis grounded in the cluster members and their evidence.

This pass should focus on clusters that are actually worth synthesis.

Eligibility:
- default: only clusters with `material_count >= 2`
- skip unchanged clusters
- optionally skip very weak clusters below a configurable evidence threshold

Output artifact:
- `derived/lint/concept_reflections.jsonl`

Each record should include:
- `cluster_id`
- `slug`
- `main_takeaways[]`
- `main_tensions[]`
- `open_questions[]`
- `why_this_concept_matters`
- `supporting_material_ids[]`
- `supporting_evidence[]`

### 3. Collection Page Reflection

For each collection page, synthesize what the collection teaches.

Examples:
- what is most relevant in `Archives`
- what themes recur
- what tensions or disagreements matter
- what this collection is useful for
- what the database currently knows from this collection

This extends Phase 5 deterministic collection pages with real learning value.

Eligibility:
- default: only collections with at least 2 materials
- skip unchanged collections

Output artifact:
- `derived/lint/collection_reflections.jsonl`

Each record should include:
- `domain`
- `collection`
- `main_takeaways[]`
- `main_tensions[]`
- `important_material_ids[]`
- `important_cluster_ids[]`
- `open_questions[]`

### 4. Graph Reflection

Review the graph globally.

Tasks:
- missing cross-references
- contradictions across materials
- under-connected materials
- under-connected clusters
- unanswered questions from weakly connected areas
- candidate future sources or source-types that would strengthen weak areas
- candidate bridge pages or missing concept links

Output artifact:
- `derived/lint/graph_findings.jsonl`

This pass is intentionally advisory-first.

Unlike concept and collection reflections, graph reflection should initially produce filed findings rather than broad automatic page rewrites.

## Materialization

Phase 6 should not stop at reports.

It must be able to materialize reflective knowledge into the wiki.

`arq lint --full` or `arq lint --fix` should:
- update concept pages with reflection sections
- update collection pages with reflection sections
- optionally update related-links sections when recommendations are accepted

The source of truth for these reflective additions is the structured lint artifacts in `derived/lint/`, not freehand markdown editing.

## Filed Outputs

Like the original LLM-wiki pattern, Phase 6 should leave durable filed outputs behind.

Minimum durable outputs:
- structured artifacts in `derived/lint/`
- human-readable report in `wiki/_lint_report.md`
- updated concept pages
- updated collection pages

Lint findings, takeaways, tensions, and open questions should compound in the system, not disappear into terminal output.

## Review Model

Phase 6 should support a human-in-the-loop review model.

Recommended split:
- deterministic fixes may apply automatically
- reflective page updates may apply through `--full` or `--fix` when accepted or configured as safe
- graph findings and suggested future questions/sources may remain filed for later review

The default safety model should be:
- `--quick`: deterministic only
- `--full`: generate reflective artifacts and apply safe page updates / maintenance fixes
- `--fix`: deterministic fixes + explicitly safe or approved reflective page updates

## Cumulative Reflection Context

Phase 6 reflection is cumulative, not stateless.

When a concept or collection is reflected again:
- keep the previous reflection sections visible in the page
- feed those prior conclusions back into the next LLM pass
- combine them with rich evidence packets: new evidence, changed clusters, newly linked materials, and representative excerpts from supporting materials
- allow the model to revise, extend, or contradict prior conclusions

The system should not clear reflective memory on every rerun.
It should build on existing conclusions the way human memory does.

## Read-Only Search Tool

Reflection passes may need more context than the default packet provides.

Phase 6 should therefore expose a narrow, read-only search tool backed by the SQL index.

Allowed operations:
- search materials
- search concepts
- search collections
- open a result record by id

Constraints:
- query-only
- no repository traversal
- no filesystem writes
- no arbitrary file access
- no code execution

The tool exists so reflection can ask for targeted extra context without requiring full material pages in every prompt.
It is a supplement to the default packet, not a replacement for it.

The default packet should already be rich enough to support good conclusions:
- current page reflection text
- new or changed evidence
- representative excerpts from linked materials
- key chunk summaries / annotations / figure descriptions when relevant

Local concepts stay searchable and useful as raw inputs, but they do **not** receive their own reflection pass in Phase 6.
They feed the bridge/main-concept work and remain available through the index.

The search tool is for filling gaps, not for making the prompt thin by default.

## Memory Integration

This is the key architectural move.

Phase 6 reflections must feed memory.

After accepted reflective outputs are materialized:
- run `arq memory rebuild`
- project the new reflective knowledge into SQLite

The memory layer should gain structured reflective records, not only cluster topology.

Minimum indexed reflection types:
- concept takeaways
- concept tensions
- concept open questions
- collection takeaways
- collection tensions
- collection open questions

These should be added as reflection-oriented tables or record types, not overloaded into existing cluster topology tables.

Topology and reflection should stay distinct.

The exact table design can be finalized in implementation, but the result must be:
- queryable by agents
- linked to concept pages / collection pages
- linked to supporting materials

This is what turns the system from searchable memory into a more useful thinking partner.

## Staleness Rules

Deterministic lint is stale when:
- wiki changed
- extracted/enriched artifacts changed
- index or memory bridge changed

Concept reflection is stale when any of these changed:
- cluster membership
- canonical name
- supporting material summaries / concepts
- compiled concept page content

Collection reflection is stale when any of these changed:
- collection membership
- member material pages
- linked concept pages
- deterministic collection page content

Graph reflection is stale when:
- clusters changed
- memory bridge changed
- wiki changed materially

Because graph reflection is the most expensive and least localized pass, it should also support a coarse schedule gate such as:
- minimum elapsed time since last full graph review
- or minimum number of changed clusters/materials

## Commands

- `arq lint --quick`
  - deterministic only
- `arq lint --full`
  - deterministic + all LLM passes
- `arq lint --report`
  - write reports/artifacts without applying
- `arq lint --fix`
  - apply deterministic fixes and accepted reflective wiki updates
- `arq lint --json`
  - emit machine-readable CLI output

## Scheduling

Phase 6 is not an every-file pipeline step.

Recommended model:
- `--quick` after compile
- `--full` on a periodic schedule or when enough change has accumulated

Within one run, provider exhaustion should be remembered across independent batches so a provider that hit a limit is skipped for later passes instead of being retried repeatedly.

This keeps cost reasonable while still allowing richer reflective growth.

## Non-Goals

Phase 6 should not:
- replace Phase 5 compile
- invent unsupported claims without evidence
- rewrite the whole wiki every time
- become a generic chat-based “think about everything” pass

It should stay grounded, structured, and provenance-aware.
