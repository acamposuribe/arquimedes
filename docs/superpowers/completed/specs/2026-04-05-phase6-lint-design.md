# Arquimedes — Phase 6: Wiki Linting, Reflection, and Memory Growth

> **Status:** Complete
> **Date:** 2026-04-06
> **Related specs:** [Full system design](../../specs/2026-04-04-arquimedes-knowledge-system-design.md), [Phase 5 wiki compiler, collection pages, and memory bridge](2026-04-05-phase5-wiki-compiler-design.md), [Connection model](2026-04-05-connection-model.md)
> **Plan:** [PLAN.md](../../../PLAN.md)

## Purpose

Phase 6 is the reflective maintenance layer for the wiki and its machine-queryable memory.

Phase 5 publishes structure. Phase 6 reviews that structure, deepens it, and feeds the results back into both:
- the wiki
- the SQLite memory layer

The phase is intentionally split into:
- **deterministic lint** for mechanical hygiene
- **LLM reflection** for semantic maintenance and growth

Phase 6 is file-driven. The LLM reads staged packets and work files, edits its work file directly, and emits structured artifacts that the rest of the pipeline can project into SQL and compile into wiki pages.

## Core Principle

Run deterministic checks first.

Only after the graph and files are structurally healthy should the heavier reflective passes run.

Phase 6 stays disciplined:
- fast checks first
- reflective synthesis second
- memory / wiki refreshes between reflective stages

## Execution Model

`arq lint --quick`
- deterministic checks only
- no LLM

`arq lint --full`
- deterministic lint first
- cluster audit
- refresh SQL / wiki state
- concept reflections
- refresh SQL / wiki state
- collection reflections
- refresh SQL / wiki state
- graph maintenance
- final SQL / wiki projection

The reflective stages are incremental and do not run blindly on every pass.

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

## 1. Cluster Audit

Cluster audit reviews the current bridge graph against new materials.

It is incremental:
- it runs when there are materials ingested after the last cluster run
- it compares the current bridge memory to the new material packet
- it preserves existing bridge concepts when they still form a coherent idea
- it treats splitting as a last resort

Input files:
- current bridge memory work copy in `derived/tmp/`
- incremental bridge packet for the new materials in `derived/tmp/`

Tasks:
- detect over-merged clusters that should split
- detect missed equivalences that should merge
- detect weak single-material clusters
- detect poor canonical names
- detect missing materials that belong in an existing cluster
- merge / rename / improve bridge clusters where safe and supported

Output artifacts:
- `derived/lint/cluster_reviews.jsonl`
- updated bridge memory work copy in `derived/tmp/`

## 2. Concept Reflections

Concept reflection synthesizes what a bridge concept means across its supporting materials.

It is file-driven and incremental:
- the LLM reads a link-light concept wiki page copy
- the LLM reads a staged evidence file for the supporting materials
- the LLM edits its per-cluster work file directly
- the run is skipped when the linked-concept fingerprint has not changed

Main outputs:
- `main_takeaways`
- `main_tensions`
- `open_questions`
- `why_this_concept_matters`

Input files:
- concept wiki page copy in `derived/tmp/`
- staged evidence file in `derived/tmp/`
- per-cluster work file in `derived/tmp/`

Output artifact:
- `derived/lint/concept_reflections.jsonl`

## 3. Collection Reflections

Collection reflection synthesizes what a collection teaches as a whole.

It is also file-driven and incremental:
- the LLM reads a link-light collection wiki page copy
- the LLM reads a staged evidence file split into `new_materials` and `old_materials`
- the LLM edits its per-collection work file directly
- the run is skipped when the collection key + member material ids have not changed

New materials are treated as the primary evidence.
Old materials are treated as compact continuity context.

Main outputs:
- `main_takeaways`
- `main_tensions`
- `open_questions`
- `why_this_collection_matters`

Input files:
- collection wiki page copy in `derived/tmp/`
- staged evidence file in `derived/tmp/`
- per-collection work file in `derived/tmp/`

Output artifact:
- `derived/lint/collection_reflections.jsonl`

## 4. Graph Maintenance

Graph maintenance is the semantic backlog for unresolved graph-level issues.

It is advisory-first and SQL-first:
- the LLM reads a compact graph-state packet
- the LLM reads the current bridge / concept / collection reflection digests folded into that packet
- the LLM writes structured findings, not freehand wiki markdown

Output artifact:
- `derived/lint/graph_findings.jsonl`

Those findings are then projected into SQLite by `arq memory rebuild`, and the visible maintenance page is compiled from SQL:
- `wiki/shared/maintenance/graph-health.md`

This keeps the source of truth queryable and makes the wiki a rendered view rather than a direct LLM target.

## Materialization

Phase 6 does not stop at JSONL artifacts.

The reflective outputs are projected into SQL and then rendered into the wiki through the normal build pipeline.

`arq lint --full` or `arq lint --fix` should:
- write reflection artifacts under `derived/lint/`
- apply safe bridge-cluster maintenance actions coming from cluster reviews
- keep graph maintenance in SQL-backed storage
- leave wiki rendering to `arq compile`

`arq compile` then renders the SQL-backed reflection outputs into:
- concept pages
- collection pages
- the graph-maintenance page

The source of truth for these reflective additions is the structured lint artifacts and their SQL projections, not freehand markdown editing or direct wiki patching.

## Filed Outputs

Phase 6 leaves durable filed outputs behind.

Minimum durable outputs:
- `derived/lint/deterministic_report.json`
- `derived/lint/cluster_reviews.jsonl`
- `derived/lint/concept_reflections.jsonl`
- `derived/lint/collection_reflections.jsonl`
- `derived/lint/graph_findings.jsonl`
- `wiki/shared/maintenance/graph-health.md`

## Review Model

Phase 6 supports a human-in-the-loop review model.

Recommended split:
- deterministic fixes may apply automatically
- reflective artifacts are rendered into the wiki by `arq compile` after lint
- graph findings and suggested future questions / sources may remain filed for later review

The default safety model is:
- `--quick`: deterministic only
- `--full`: generate reflective artifacts and apply safe maintenance fixes
- `--fix`: deterministic fixes + explicitly safe or approved maintenance fixes

## Cumulative Reflection Context

Phase 6 reflection is cumulative, not stateless.

When a concept or collection is reflected again:
- keep the previous reflection sections visible in the page
- feed those prior conclusions back into the next LLM pass
- combine them with rich evidence packets
- allow the model to revise, extend, or contradict prior conclusions

The system should not clear reflective memory on every rerun.
It should build on existing conclusions the way human memory does.

## Non-Goals

Phase 6 should not:
- replace Phase 5 compile
- invent unsupported claims without evidence
- repeat deterministic lint in the reflective passes
- rewrite the whole wiki every time
- become a generic chat-based “think about everything” pass

It should stay grounded, structured, and provenance-aware.
