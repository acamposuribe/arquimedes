# Current State: Enrich + Cluster Prompt/Output Flow

Date: 2026-04-08

This note is the current handoff for agents working on the LLM-driven enrich and cluster pipeline.

## Core Decision

For enrich-document and bridge-cluster, the LLM should no longer edit output work files in place. The LLM now returns one final structured JSON object in chat, and the application parses, validates, and applies that JSON programmatically.

This keeps the contract deterministic after the model response:

- prompt defines the JSON schema
- model returns JSON only
- `parse_json_or_repair(...)` handles parse/repair
- Python code validates required fields
- Python code writes canonical artifacts atomically

Input staging still exists where useful. Output editing by the LLM does not.

## Document Enrichment: Current Flow

Current implementation lives primarily in [src/arquimedes/enrich_document.py](src/arquimedes/enrich_document.py) and [src/arquimedes/enrich_prompts.py](src/arquimedes/enrich_prompts.py).

### What changed

Document enrichment already used a JSON-return flow and is now the reference pattern.

- The LLM reads staged source files from disk:
  - `meta.json`
  - flattened `document.work.md`
- The LLM returns a single JSON object matching `_DOCUMENT_PATCH_SCHEMA`
- The response must include `_finished: true`
- The code rejects partial/final-missing payloads
- The code normalizes plain values into the internal `{ "value": ... }` shape
- The code writes `meta.json`, `concepts.jsonl`, and optional `toc.json` atomically

### Important contract details

- The output contract is enforced in code, not by trusting the model.
- Required output fields are checked explicitly.
- Refusal-style summaries are rejected.
- `parse_json_or_repair(...)` is the only repair layer; there is no output work-file promotion step.


## Bridge Clustering: Current Flow

Current implementation lives in [src/arquimedes/cluster.py](src/arquimedes/cluster.py).

### What changed

Bridge clustering used to follow an edit-in-place workflow around staged files. That has been replaced.

Current cluster flow:

1. Stage compact JSON inputs:
   - bridge packets from pending concept/material rows
   - bridge memory from existing canonical bridge clusters
2. Send a prompt that tells the model to read those staged input files.
3. Put the compact response schema in the system prompt.
4. Require one final JSON object only, at the end of the full job.
5. Parse via `parse_json_or_repair(...)`.
6. Validate `_finished: true` and required fields.
7. Apply the JSON delta programmatically.
8. Validate cluster integrity and attach provenance.
9. Write canonical `derived/bridge_concept_clusters.jsonl`.

### Current output schema

Cluster output is a compact JSON delta with:

- `links_to_existing`
- `new_clusters`
- `_finished`

The schema is intentionally compact to reduce token use and now lives in `_BRIDGE_SYSTEM_PROMPT`, not in the user prompt.

### Prompt contract changes made

- Schema moved into the system prompt.
- Schema compacted into a single-line JSON description.
- Prompt explicitly says: do all reasoning silently first.
- Prompt explicitly forbids partial JSON, draft JSON, commentary, and markdown fences.
- `_finished` must be true only in the final completed JSON object.


## Important Files

- [src/arquimedes/enrich_document.py](src/arquimedes/enrich_document.py)
- [src/arquimedes/enrich_prompts.py](src/arquimedes/enrich_prompts.py)
- [src/arquimedes/enrich.py](src/arquimedes/enrich.py)
- [src/arquimedes/cluster.py](src/arquimedes/cluster.py)
- [src/arquimedes/lint.py](src/arquimedes/lint.py)
- [tests/test_enrich_document.py](tests/test_enrich_document.py)
- [tests/test_cluster.py](tests/test_cluster.py)
- [tests/test_lint.py](tests/test_lint.py)


## Known Boundary: What Still Uses File-Edit Workflows

This migration is not global.

Phase-6 reflective lint is now mixed:

- cluster audit uses the same final-JSON pattern as document enrich / bridge cluster
- concept reflection now uses the same final-JSON pattern with deterministic post-apply into `concept_reflections.jsonl`, including per-field `null => preserve existing value` handling
- collection reflection now uses the same final-JSON pattern with deterministic post-apply into `collection_reflections.jsonl`, including per-field `null => preserve existing value` handling
- graph maintenance now uses the same final-JSON pattern with deterministic post-apply into `graph_findings.jsonl`, with `findings: null` preserving the stored findings list unchanged

So the current state is:

- document enrich: JSON-return flow
- metadata-fix: JSON-return flow
- bridge cluster: JSON-return flow
- lint cluster audit: JSON-return flow with one canonical persistent review row per bridge cluster
- lint concept reflection: JSON-return flow with deterministic post-compile into the canonical reflection row
- lint collection reflection: JSON-return flow with deterministic post-compile into the canonical reflection row
- lint graph maintenance: JSON-return flow with deterministic post-compile into the canonical findings rows

Reflective lint gating now uses one shared `derived/lint/lint_stamp.json`: `audited_at`, `concept_reflection_at`, `collection_reflection_at`, and `graph_reflection_at` are each compared against the latest `derived/bridge_cluster_stamp.json` as the outer gate. After that outer gate, every reflective stage still applies its own internal conditions: cluster audit narrows work to changed/open bridge clusters plus changed uncovered-local packets, concept reflection skips unchanged eligible clusters by row fingerprint, collection reflection skips unchanged eligible collections by row fingerprint, and graph maintenance still applies its own unchanged/schedule checks. Only cluster audit and graph maintenance need extra persisted internal state files beyond the shared outer lint stamp.

The LLM-edited output work-file pattern is now removed from the current lint reflection surfaces.


## Suggested Next Pickup

If continuing from here, the highest-value follow-on tasks are:

1. Keep schema definitions compact and system-prompt-local for every migrated stage.
2. Preserve the distinction between staged input files and model-owned output files: input staging is fine, output editing is what should be avoided.
3. If continuing this cleanup outside lint, apply the same final-JSON/post-apply contract to any remaining LLM-owned output-edit workflows.