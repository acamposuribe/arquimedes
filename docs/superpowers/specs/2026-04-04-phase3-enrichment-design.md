# Phase 3: LLM Enrichment — Design Spec

> **Status:** Complete
> **Date:** 2026-04-04
> **Parent spec:** [Full system design](2026-04-04-arquimedes-knowledge-system-design.md)

## Overview

`arq enrich` adds LLM-generated semantic metadata to extracted materials. Every enriched field carries full provenance. Enrichment is split into three independent stages per material — document, chunk, and figure — each with its own staleness tracking, so stages can rerun selectively without invalidating each other.

## Enrichment Stages

### Stage 1: Document Enrichment

**Inputs:** `meta.json` (raw fields only), `pages.jsonl`, `annotations.jsonl`, `toc.json`, `chunks.jsonl`

**Outputs added to `meta.json`:**
- `summary` (EnrichedField) — ~200 words, weighted toward annotated sections
- `document_type` (EnrichedField) — from fixed enum: regulation | catalogue | monograph | paper | lecture_note | precedent | technical_spec | site_document. Refines `raw_document_type`.
- `keywords` (EnrichedField) — 5-15 terms, refines `raw_keywords`
- `facets` (ArchitectureFacets) — each sub-field is an EnrichedField or None. Only set where confident. Fields: building_type, scale, location, jurisdiction, climate, program, material_system, structural_system, historical_period, course_topic, studio_project.
- `_enrichment_stamp` — see Staleness section

**Outputs written to `concepts.jsonl`:**
- Concept candidates owned exclusively by this stage
- Each entry: `{concept_name, relevance, provenance}`

**LLM call strategy:**
- For small/moderate materials (all chunk text fits within ~80k tokens alongside the document prompt): combined with chunk enrichment in one call. The LLM response contains both document-level and chunk-level output in a single structured response.
- For large materials: separate document call with curated context (see below), chunk enrichment runs in its own batched calls

**Combined call failure semantics:** When document and chunk enrichment share one LLM call, the response is parsed into document output and chunk output independently. If the document portion is valid but the chunk portion is malformed (or vice versa), the valid portion commits and the invalid portion triggers a schema-repair retry for just that portion. If the retry also fails, only the failed stage is marked as failed. The stages remain independent even when they share a transport call.

**Context sent to LLM:**
- Document header: title, authors, year, raw_keywords, raw_document_type, domain, collection
- TOC / top-level headings for structural overview
- Chunk texts as the canonical text representation (avoids duplicating full page text)
- Annotated spans marked as `[HIGHLIGHTED]...[/HIGHLIGHTED]` with reader notes inline
- For large materials where full chunk text won't fit: TOC/headings + first pages + conclusion pages + pages with annotations + top representative chunks (by emphasis and position)

### Stage 2: Chunk Enrichment

**Inputs:** `chunks.jsonl`, `annotations.jsonl`, document context (title, summary if available from current or prior document enrichment, raw_document_type, top-level headings)

**Outputs added to each chunk in `chunks.jsonl`:**
- `summary` (EnrichedField) — one-line summary
- `keywords` (EnrichedField) — extracted terms
- `content_class` (string) — chunk role classification: `argument` | `methodology` | `case_study` | `bibliography` | `front_matter` | `caption` | `appendix`

**Outputs written to `chunk_enrichment_stamps.json`:**
- `{chunk_id: stamp}` map for per-chunk staleness tracking

**Batching:**
- Token-budget based, targeting ~40-60 chunks per batch as a heuristic
- Adjust batch size based on actual token count of chunk texts
- Document context included in each batch call (compact: title + summary/type + headings)
- Summary used as document context only if it exists from a current document enrichment (same run or prior non-stale run). Otherwise fall back to title, raw_document_type, top-level headings.

**Atomicity:**
- Accumulate batch results in memory
- Write `chunks.jsonl` and `chunk_enrichment_stamps.json` only after all batches for this material succeed
- If any batch fails (after retries), the entire chunk stage fails — no partial writes

### Stage 3: Figure Enrichment

**Inputs:** figure images, figure sidecar JSONs (`figures/*.json`), surrounding page text, document context

**Outputs added to each figure sidecar JSON:**
- `visual_type` (EnrichedField) — plan | section | elevation | detail | photo | diagram | chart | render | sketch
- `description` (EnrichedField) — visual description of what the figure shows
- `caption` (EnrichedField) — extracted or inferred caption
- `relevance` (string) — figure relevance classification: `substantive` | `decorative` | `front_matter`
- `analysis_mode` — "vision" or "text_fallback"
- `_enrichment_stamp`

**Batching:**
- 3-8 figures per multimodal call
- Per-figure context sent: source_page text (near bbox), nearby caption candidates (text fragments adjacent to the figure bbox), figure metadata (source_page, bbox, extraction_method)
- Document context: title, document_type, domain

**Fallback:**
- If image file is missing or unreadable: fall back to text-only context
- Same output fields, but `analysis_mode: "text_fallback"` and typically lower confidence
- `analysis_mode` is stored so downstream consumers can assess trustworthiness

## Staleness & Versioning

### Enrichment Stamp

Each stage writes a stamp alongside its output:

```json
{
  "prompt_version": "enrich-v1.0",
  "model": "claude-sonnet-4-6",
  "enrichment_schema_version": "1",
  "input_fingerprint": "a3b8f1c2d4e5..."
}
```

### Input Fingerprint

A hash of **all inputs that can change the enrichment result**, including context from other artifacts and stages:

- **Document stage:** canonical raw-only projection of `meta.json` (material_id, title, authors, year, raw_keywords, raw_document_type, domain, collection, page_count) + full content of `pages.jsonl` + full content of `annotations.jsonl` + full content of `toc.json` + full content of `chunks.jsonl` (chunk texts are the canonical text representation sent to the LLM)
- **Chunk stage:** full chunk records from `chunks.jsonl` (text, source_pages, emphasized, chunk_id) + full content of `annotations.jsonl` + document context digest (title, raw_document_type, top-level headings, and — if present — the current document-stage summary value and its stamp). This means re-enriching the document stage can make the chunk stage stale if the summary changed.
- **Figure stage:** per-figure fingerprint — image file hash + source_page text + nearby caption candidates + figure sidecar metadata (source_page, bbox, extraction_method) + document context digest (title, document_type value if enriched, domain). Re-enriching document_type can make figure stage stale.

### Staleness Rules

**"Current" = exact match** on three stamp fields: `prompt_version`, `enrichment_schema_version`, `input_fingerprint`. Any difference = stale.

The `model` field is **audit-only** — it records which model actually produced the output (which may vary due to ordered agent fallback) but is not compared for staleness. To force re-enrichment with a different model, use `--force`.

- Stale or missing stamp: re-enrich
- Current stamp: skip
- `--force`: re-enrich regardless

### Where Stamps Live

- **Document stage:** `meta.json` → `_enrichment_stamp` key
- **Chunk stage:** `chunk_enrichment_stamps.json` — `{chunk_id: stamp}` map. Checked per-chunk but written atomically (all or nothing per stage run).
- **Figure stage:** each `figures/*.json` sidecar → `_enrichment_stamp` key

Stamps are independent per stage. Re-enriching chunks does not invalidate document or figure enrichment. Re-extracting raw artifacts (which changes input_fingerprint) triggers re-enrichment of affected stages on next run.

## Provenance

Every enriched field uses the existing `EnrichedField` wrapper:

```json
{
  "value": "A study of thermal mass in Mediterranean climate...",
  "provenance": {
    "source_pages": [1, 2, 3],
    "evidence_spans": ["This paper examines...", "In the Mediterranean..."],
    "model": "claude-sonnet-4-6",
    "prompt_version": "enrich-v1.0",
    "confidence": 0.92,
    "enriched_at": "2026-04-04T14:30:00Z"
  }
}
```

**Evidence spans must be:**
- Page-anchored (source_pages tells you where they come from)
- Short — quoted or paraphrased supporting fragments, not full paragraphs
- For chunk-level outputs: may also reference `chunk_id`

**What the LLM returns:** `source_pages`, `evidence_spans`, `confidence` (0-1).
**What we stamp ourselves:** `model`, `prompt_version`, `enriched_at`.

## Annotation-Aware Enrichment

When `annotations.jsonl` exists:
- Highlighted/noted spans are injected into the prompt as `[HIGHLIGHTED]...[/HIGHLIGHTED]` with reader notes
- The prompt instructs the LLM to weight annotated sections as priority context — "the reader considered these passages important"
- Summaries should reflect annotated content more heavily
- Keywords may be partially derived from annotated spans
- Emphasized chunks (overlapping with annotations) are marked in the chunk batch

## CLI Interface

### `arq enrich [material_id]`

- No argument: enrich all materials with stale or missing enrichment
- With `material_id`: enrich that single material
- `--force`: re-enrich regardless of staleness
- `--stage document|chunk|figure`: repeatable — run only specified stages. Example: `--stage document --stage figure`
- `--dry-run`: report what would be enriched without calling the LLM. Does not require an API key.

**Output:**
```
Enriching bbf97c1aae06 (Engaging the Archival Habitat)
  document: enriched (summary, 8 keywords, 4 facets, 2 concepts)
  chunks:   enriched (47 chunks, 2 batches)
  figures:  enriched (6 figures, 1 vision batch)
```

**Exit codes:**
- 0: all requested stages succeeded
- 1: one or more requested stages failed (partial results still printed)

### `arq extract [material_id]`

Convenience wrapper: runs `extract-raw` then `enrich` sequentially.
Passes through `--force` and `--stage` flags to the enrich step.

## Error Handling

- **Agent CLI errors** (timeouts, non-zero exit): retry up to max_retries per call
- **Invalid LLM output** (malformed JSON, missing fields): one schema-repair retry ("return valid JSON matching the schema"), then fail the stage if still invalid
- **Partial failure:** if one stage fails, save what succeeded in other stages. Stamps track independence.
- **Stage atomicity:** either a stage completes fully or it doesn't write. No partial enrichment within a stage.
- **Missing agent CLI:** fail fast with message listing all tried commands. Falls back to next configured agent on failure.
- **Missing material:** clear error message with material_id

## File Layout After Enrichment

```
extracted/<material_id>/
  meta.json                      # gains enriched fields + _enrichment_stamp
  chunks.jsonl                   # each chunk gains summary + keywords
  chunk_enrichment_stamps.json   # NEW — {chunk_id: stamp} map
  concepts.jsonl                 # NEW — concept candidates from document stage
  figures/
    fig_0001.json                # gains visual_type, description, caption,
                                 #   analysis_mode, _enrichment_stamp
```

## Configuration

From `config.yaml`:

```yaml
llm:
  agent_cmd:                      # legacy fallback; stage routes take precedence
    - "claude --print"
    - "codex exec"

enrichment:
  prompt_version: "enrich-v1.0"
  enrichment_schema_version: "1"
  chunk_batch_target: 50        # target chunks per batch (adjusted by token budget)
  figure_batch_size: 6          # figures per multimodal call
  max_retries: 3                # agent CLI call retries
  parallel: 4                   # concurrent material enrichments (1 = sequential)
  llm_routes:
    document:
      - provider: codex
        command: "codex exec"
        model: gpt-5.4-mini
        effort: high
      - provider: copilot
        command: "copilot"
        model: gpt-5-mini
        effort: high
    chunk:
      - provider: copilot
        command: "copilot"
        model: gpt-5-mini
    figure:
      - provider: codex
        command: "codex exec"
        model: gpt-5.4-mini
        effort: medium
      - provider: copilot
        command: "copilot"
        model: gpt-4o
    cluster:
      - provider: claude
        command: "claude --print"
        model: sonnet
        effort: medium
      - provider: codex
        command: "codex exec"
        model: gpt-5.4-mini
        effort: high
      - provider: copilot
        command: "copilot"
        model: gpt-5-mini
```

### Performance

- **Lazy LLM init:** agent CLI is never constructed if nothing is stale
- **Claude optimizations:** called with `--no-session-persistence --disable-slash-commands --tools "" --model sonnet --system-prompt` to skip session saving, skill resolution, and built-in tools (`--bare` is intentionally avoided — it breaks credential discovery)
- **Codex optimizations:** codex is called with `--ephemeral --skip-git-repo-check` to reduce startup overhead
- **Parallel materials:** when multiple materials need enrichment, they are processed concurrently via `ThreadPoolExecutor(max_workers=parallel)`
- **Parallel stages:** within a single material, document + figure stages run concurrently (independent inputs), while chunk stage waits for document (uses doc summary in prompt context)
- **Early skip:** orchestrator checks staleness before dispatching to stage functions, avoiding unnecessary LLM construction
- **Ordered stage fallback:** routes are tried in order from `enrichment.llm_routes[stage]` (falling back to legacy `llm.agent_cmd` if no stage routes exist). If an agent fails (exit code, auth error, rate limit), the next is tried automatically
- **Fast-fail on auth/rate-limit:** subprocess stderr is monitored in real-time; patterns like "not logged in", "rate limit", "quota exceeded" trigger immediate process kill (via `os.killpg`) instead of waiting for timeout

## Scope Boundaries

**In scope for Phase 3:**
- `arq enrich` with all flags
- `arq extract` convenience wrapper
- Three enrichment stages with staleness tracking
- Provenance on every field
- Annotation-aware prompting

**Out of scope (later phases):**
- Search indexing of enriched fields (Phase 4)
- Wiki compilation from concepts (Phase 5)
- Enrichment as part of the watch/sync pipeline (Phase 9)
