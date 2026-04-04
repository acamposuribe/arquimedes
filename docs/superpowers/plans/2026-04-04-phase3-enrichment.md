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
- [ ] Add `enrichment_schema_version`, `chunk_batch_target`, `figure_batch_size`, `max_retries` to `config/config.yaml` under `enrichment:`
- [ ] Commit

### Task 2: `enrich_stamps.py` + tests
- [ ] `canonical_hash(*parts) → str` — sorted-key JSON serialization → sha256[:16]
- [ ] `document_fingerprint(output_dir) → str` — hash raw meta projection + pages + annotations + toc + chunks
- [ ] `chunk_fingerprint(output_dir, chunks, doc_context) → str` — hash chunk records + annotations + doc context digest
- [ ] `figure_fingerprint(figure, page_text, caption_candidates, doc_context) → str` — hash image file + metadata + context
- [ ] `make_stamp(config) → dict` — build stamp dict from config (prompt_version, model, schema_version, fingerprint placeholder)
- [ ] `is_stale(existing_stamp, current_stamp) → bool` — exact match on all 4 fields
- [ ] `read_stamp(path_or_dict) / write_stamp(path, stamp)` — I/O helpers
- [ ] Tests: determinism (same input → same hash), staleness detection, round-trip I/O
- [ ] Commit

### Task 3: `enrich_llm.py` + tests
- [ ] `make_cli_llm_fn(config) → LlmFn` — shells out to agent CLI(s) from `config["llm"]["agent_cmd"]` (list: tried in order, first success wins), retries on timeout
- [ ] `parse_json_or_repair(client, model, text, schema_description) → dict` — JSON parse, one schema-repair retry on failure, raise `EnrichmentError` if still invalid
- [ ] Custom `EnrichmentError` exception
- [ ] Tests: mock client — success, retry on rate limit, schema-repair path, final failure
- [ ] Commit

### Task 4: `enrich_prompts.py` + tests
- [ ] `build_document_prompt(meta, toc, chunks, annotations) → (system, messages)` — document-level prompt with annotation markers
- [ ] `build_combined_prompt(meta, toc, chunks, annotations) → (system, messages)` — combined doc+chunk prompt
- [ ] `build_chunk_batch_prompt(chunk_batch, doc_context, annotations) → (system, messages)` — chunk batch prompt
- [ ] `build_figure_batch_prompt(figures_with_context, doc_context) → (system, messages)` — figure batch prompt (vision-ready)
- [ ] `inject_annotations(text, annotations, page_number) → str` — wrap highlighted spans in `[HIGHLIGHTED]...[/HIGHLIGHTED]`
- [ ] Tests: annotation injection, prompt structure validation (all required sections present)
- [ ] Commit

### Task 5: `enrich_document.py` + tests
- [ ] `enrich_document_stage(output_dir, config, client, *, force=False) → StageResult`
- [ ] Parse LLM response → `MaterialMeta` enriched fields + `ConceptCandidate` list
- [ ] Build `EnrichedField` with provenance (LLM returns `source_pages`, `evidence_spans`, `confidence`; we stamp `model`, `prompt_version`, `enriched_at`)
- [ ] Write updated `meta.json` (merge enriched fields onto existing raw fields)
- [ ] Write `concepts.jsonl`
- [ ] Write `_enrichment_stamp` to `meta.json`
- [ ] Tests: mock LLM response → verify meta.json fields, concepts.jsonl, stamp written
- [ ] Commit

### Task 6: `enrich_chunks.py` + tests
- [ ] `enrich_chunks_stage(output_dir, config, client, *, force=False) → StageResult`
- [ ] `_compute_batches(chunks, target_per_batch) → list[list[Chunk]]` — token-budget batching
- [ ] Accumulate all batch results in memory
- [ ] Atomic write: `chunks.jsonl` (merge enriched fields onto existing raw chunk data) + `chunk_enrichment_stamps.json`
- [ ] If any batch fails after retries → stage fails, no writes
- [ ] Tests: batching logic, atomic write on success, no write on partial failure
- [ ] Commit

### Task 7: `enrich_figures.py` + tests
- [ ] `enrich_figures_stage(output_dir, config, client, *, force=False) → StageResult`
- [ ] Vision path: send image + page text + caption candidates
- [ ] Text fallback: if image missing/unreadable, send text-only context, set `analysis_mode: "text_fallback"`
- [ ] Write enriched fields + `analysis_mode` + `_enrichment_stamp` to each figure sidecar JSON
- [ ] Tests: vision path, text-fallback path, analysis_mode field, stamp per figure
- [ ] Commit

### Task 8: `enrich.py` orchestrator + tests
- [ ] `enrich(material_id=None, config=None, *, force=False, stages=None, dry_run=False) → dict`
- [ ] Load config, iterate materials (all pending or single), dispatch stages
- [ ] Combined-call logic: if total chunk text < threshold → one LLM call for doc+chunk, parse independently, commit independently (per spec combined-call failure semantics)
- [ ] `--dry-run`: compute staleness for each stage, print report, return without LLM calls
- [ ] `--stage` filter: only run requested stages
- [ ] Return dict of `{material_id: {stage: result}}`
- [ ] Exit code logic: any stage failure → 1
- [ ] Tests: mock stage functions — combined vs split dispatch, force flag, stage filter, dry-run
- [ ] Commit

### Task 9: CLI integration
- [ ] Replace `arq enrich` stub: add `--force`, `--stage` (multiple), `--dry-run` options, wire to `enrich.enrich()`
- [ ] Replace `arq extract` stub: run `extract_raw()` then `enrich()` sequentially, pass through `--force` and `--stage`
- [ ] API key check: fail fast if missing with clear message
- [ ] Print per-material stage results in spec format
- [ ] Commit

### Task 10: Integration smoke test
- [ ] Run `arq enrich --dry-run` on a real extracted material — verify no API key needed, staleness report printed
- [ ] Run `arq enrich <material_id>` on a single material — verify all three stages complete, files written correctly
- [ ] Run `arq enrich <material_id>` again — verify all stages skipped (stamps current)
- [ ] Run `arq enrich <material_id> --force --stage document` — verify only document re-enriched
- [ ] Update spec/plan if any behavior diverges
