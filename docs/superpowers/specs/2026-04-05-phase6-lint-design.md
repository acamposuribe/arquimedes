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
- structured JSON report in `derived/lint/deterministic_report.json`

`--fix` may auto-apply only deterministic fixes such as:
- rebuild index
- rebuild memory bridge
- regenerate missing pages through compile if safe

## LLM Passes

`arq lint --full` runs deterministic checks first, then these reflective passes.

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

Output artifact:
- `derived/lint/concept_reflections.jsonl`

Each record should include:
- `cluster_id`
- `slug`
- `main_takeaways[]`
- `main_tensions[]`
- `open_questions[]`
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

## Materialization

Phase 6 should not stop at reports.

It must be able to materialize reflective knowledge into the wiki.

`arq lint --fix` or a later explicit apply step should:
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
- reflective page updates may apply through `--fix` only when accepted or configured as safe
- graph findings and suggested future questions/sources may remain filed for later review

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

## Commands

- `arq lint --quick`
  - deterministic only
- `arq lint --full`
  - deterministic + all LLM passes
- `arq lint --report`
  - write reports/artifacts without applying
- `arq lint --fix`
  - apply deterministic fixes and accepted reflective wiki updates

## Scheduling

Phase 6 is not an every-file pipeline step.

Recommended model:
- `--quick` after compile
- `--full` on a periodic schedule or when enough change has accumulated

This keeps cost reasonable while still allowing richer reflective growth.

## Non-Goals

Phase 6 should not:
- replace Phase 5 compile
- invent unsupported claims without evidence
- rewrite the whole wiki every time
- become a generic chat-based “think about everything” pass

It should stay grounded, structured, and provenance-aware.
