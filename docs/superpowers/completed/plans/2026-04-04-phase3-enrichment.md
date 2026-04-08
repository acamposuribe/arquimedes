# Phase 3: LLM Enrichment — Implementation Plan

**Spec:** [Phase 3 enrichment design](../specs/2026-04-04-phase3-enrichment-design.md)
**Date:** 2026-04-04
**Status:** Complete (2026-04-04)

## Resolved v1 Simplifications

All previously noted simplifications have been implemented:

- ~~**Token-budget batching:**~~ Chunk batching now uses token-budget estimation (`estimate_tokens`) to fill batches proportionally rather than fixed count splitting.
- ~~**Large material context curating:**~~ `build_document_prompt` detects when total tokens exceed `max_context_tokens` (default 80k) and calls `_curate_context_for_large_doc` which scores chunks by position, annotation overlap, and emphasis, then greedily selects within budget.
- ~~**Multi-file stage atomicity:**~~ All three stages use backup-commit-rollback: originals moved to `.bak`, temps renamed to final, with rollback on any failure.

---

## File Map

| # | File | Action | Responsibility |
|---|------|--------|---------------|
| 1 | `config/config.yaml` | Modify | Add `enrichment_schema_version`, `chunk_batch_target`, `figure_batch_size`, `max_retries` |
| 2 | `src/arquimedes/enrich_stamps.py` | Create | Canonical fingerprinting, staleness check, stamp read/write |
| 3 | `src/arquimedes/enrich_prompts.py` | Create | Prompt builders for document, chunk, figure stages |
| 4 | `src/arquimedes/enrich_llm.py` | Create | LLM callable abstraction (`LlmFn`), JSON parse, schema-repair retry. Default adapter via `make_cli_llm_fn` shells out to configurable agent CLI (no API keys). |
| 5 | `src/arquimedes/enrich_document.py` | Create | Document stage: LLM call → write `meta.json` enriched fields + `concepts.jsonl` |
| 6 | `src/arquimedes/enrich_chunks.py` | Create | Chunk stage: batched LLM calls → atomic write to `chunks.jsonl` + `chunk_enrichment_stamps.json` |
| 7 | `src/arquimedes/enrich_figures.py` | Create | Figure stage: vision + text fallback → write figure sidecar JSONs |
| 8 | `src/arquimedes/enrich.py` | Create | Orchestrator: load config, iterate materials, dispatch stages, combined-call logic |
| 9 | `src/arquimedes/cli.py` | Modify | Implement `arq enrich` (with `--force`, `--stage`, `--dry-run`) and `arq extract` wrapper |

## Dependency Order

```
config.yaml ─┐
              ├─► enrich_stamps ─┐
              │                  ├─► enrich_document ─┐
enrich_llm ──┤                  ├─► enrich_chunks   ──├─► enrich.py ─► cli.py
              │                  ├─► enrich_figures  ─┘
enrich_prompts ──────────────────┘
```

`enrich_stamps`, `enrich_llm`, and `enrich_prompts` have no interdependencies — can be built in parallel.

## Test Map

| Module | Test file | Strategy |
|--------|-----------|----------|
| `enrich_stamps` | `tests/test_enrich_stamps.py` | Pure functions — no mocks needed. Fixture: minimal `extracted/<id>/` on `tmp_path` |
| `enrich_llm` | `tests/test_enrich_llm.py` | Test JSON parse/fence-stripping, schema-repair, prompt flattening, CLI adapter (fake shell script) |
| `enrich_document` | `tests/test_enrich_document.py` | Mock `enrich_llm.call_llm` — test JSON→model mapping, meta.json + concepts.jsonl writes |
| `enrich_chunks` | `tests/test_enrich_chunks.py` | Mock LLM — test batching logic, atomic write (all-or-nothing), stamp map |
| `enrich_figures` | `tests/test_enrich_figures.py` | Mock LLM — test vision path, text-fallback path, `analysis_mode` field |
| `enrich` | `tests/test_enrich.py` | Mock stage functions — test orchestrator: combined vs split, `--force`, `--stage` filter, `--dry-run` |

---

## Tasks

### Task 1: Config extension
- [x] Add `enrichment_schema_version`, `chunk_batch_target`, `figure_batch_size`, `max_retries` to `config/config.yaml` under `enrichment:`
- [x] Commit

### Task 2: `enrich_stamps.py` + tests
- [x] `canonical_hash(*parts) → str` — sorted-key JSON serialization → sha256[:16]
- [x] `document_fingerprint(output_dir) → str` — hash raw meta projection + pages + annotations + toc + chunks
- [x] `chunk_fingerprint(output_dir, chunks, doc_context) → str` — hash chunk records + annotations + doc context digest
- [x] `figure_fingerprint(figure, page_text, caption_candidates, doc_context) → str` — hash image file + metadata + context
- [x] `make_stamp(config) → dict` — build stamp dict from config (prompt_version, model, schema_version, fingerprint placeholder)
- [x] `is_stale(existing_stamp, current_stamp) → bool` — compares 3 fields (prompt_version, enrichment_schema_version, input_fingerprint; model is audit-only)
- [x] `read_stamp(path_or_dict) / write_stamp(path, stamp)` — I/O helpers
- [x] Tests: determinism (same input → same hash), staleness detection, round-trip I/O
- [x] Commit

### Task 3: `enrich_llm.py` + tests
- [x] `make_cli_llm_fn(config, stage) → LlmFn` — shells out to stage-specific routes from `enrichment.llm_routes[stage]` (ordered attempts; fallback to legacy `config["llm"]["agent_cmd"]` if absent), relying on process completion outcomes (exit code, timeout, empty output) rather than free-text stderr/stdout heuristics
- [x] `parse_json_or_repair(client, model, text, schema_description) → dict` — JSON parse, one schema-repair retry on failure, raise `EnrichmentError` if still invalid
- [x] Custom `EnrichmentError` exception
- [x] Tests: mock client — success, retry on rate limit, schema-repair path, final failure
- [x] Commit

### Task 4: `enrich_prompts.py` + tests
- [x] `build_document_prompt(meta, toc, chunks, annotations) → (system, messages)` — document-level prompt with annotation markers
- [x] `build_combined_prompt(meta, toc, chunks, annotations) → (system, messages)` — combined doc+chunk prompt
- [x] `build_chunk_batch_prompt(chunk_batch, doc_context, annotations) → (system, messages)` — chunk batch prompt
- [x] `build_figure_batch_prompt(figures_with_context, doc_context) → (system, messages)` — figure batch prompt (vision-ready)
- [x] `inject_annotations(text, annotations, page_number) → str` — wrap highlighted spans in `[HIGHLIGHTED]...[/HIGHLIGHTED]`
- [x] Tests: annotation injection, prompt structure validation (all required sections present)
- [x] Commit

### Task 5: `enrich_document.py` + tests
- [x] `enrich_document_stage(output_dir, config, client, *, force=False) → StageResult`
- [x] Parse LLM response → `MaterialMeta` enriched fields + `ConceptCandidate` list
- [x] Build `EnrichedField` with provenance (LLM returns `source_pages`, `evidence_spans`, `confidence`; we stamp `model`, `prompt_version`, `enriched_at`)
- [x] Write updated `meta.json` (merge enriched fields onto existing raw fields)
- [x] Write `concepts.jsonl`
- [x] Write `_enrichment_stamp` to `meta.json`
- [x] Tests: mock LLM response → verify meta.json fields, concepts.jsonl, stamp written
- [x] Commit

### Task 6: `enrich_chunks.py` + tests
- [x] `enrich_chunks_stage(output_dir, config, client, *, force=False) → StageResult`
- [x] `_compute_batches(chunks, target_per_batch) → list[list[Chunk]]` — token-budget batching
- [x] Accumulate all batch results in memory
- [x] Atomic write: `chunks.jsonl` (merge enriched fields onto existing raw chunk data) + `chunk_enrichment_stamps.json`
- [x] If any batch fails after retries → stage fails, no writes
- [x] Tests: batching logic, atomic write on success, no write on partial failure
- [x] Commit

### Task 7: `enrich_figures.py` + tests
- [x] `enrich_figures_stage(output_dir, config, client, *, force=False) → StageResult`
- [x] Vision path: send image + page text + caption candidates
- [x] Text fallback: if image missing/unreadable, send text-only context, set `analysis_mode: "text_fallback"`
- [x] Write enriched fields + `analysis_mode` + `_enrichment_stamp` to each figure sidecar JSON
- [x] Tests: vision path, text-fallback path, analysis_mode field, stamp per figure
- [x] Commit

### Task 8: `enrich.py` orchestrator + tests
- [x] `enrich(material_id=None, config=None, *, force=False, stages=None, dry_run=False) → dict`
- [x] Load config, iterate materials (all pending or single), dispatch stages
- [x] Combined-call logic: if total chunk text < threshold → one LLM call for doc+chunk, parse independently, commit independently (per spec combined-call failure semantics)
- [x] `--dry-run`: compute staleness for each stage, print report, return without LLM calls
- [x] `--stage` filter: only run requested stages
- [x] Return dict of `{material_id: {stage: result}}`
- [x] Exit code logic: any stage failure → 1
- [x] Parallel stages: document + figure run concurrently, chunk waits for document
- [x] Tests: mock stage functions — combined vs split dispatch, force flag, stage filter, dry-run
- [x] Commit

### Task 9: CLI integration
- [x] Replace `arq enrich` stub: add `--force`, `--stage` (multiple), `--dry-run` options, wire to `enrich.enrich()`
- [x] Replace `arq extract` stub: run `extract_raw()` then `enrich()` sequentially, pass through `--force` and `--stage`
- [x] No API key check needed — agent CLI handles auth; routing falls through only on concrete process failure outcomes
- [x] Print per-material stage results in spec format
- [x] Commit

### Task 10: Integration smoke test
- [x] Run `arq enrich --dry-run` on a real extracted material — verify no API key needed, staleness report printed
- [x] Run `arq enrich <material_id>` on a single material — verify all three stages complete, files written correctly
- [x] Run `arq enrich <material_id>` again — verify all stages skipped (stamps current)
- [x] Run `arq enrich <material_id> --force --stage document` — verify only document re-enriched
- [x] Spec/plan updated to match implementation (staleness 3-field contract, parallel stages, process-outcome fallback, content_class/relevance)
