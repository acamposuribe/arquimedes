# Expanded Source Filetypes - Implementation Plan

> **Status:** Implemented (phases 1–8 complete on 2026-05-02). Documented for reference.
> **Date:** 2026-05-02
> **Companion spec:** [Expanded source filetypes design](../specs/2026-05-02-expanded-source-filetypes-design.md)
> **References:** [Operational pipeline](../../PIPELINE.md), [Arquimedes knowledge system design](../specs/2026-04-04-arquimedes-knowledge-system-design.md)

## Implementation summary (2026-05-02)

- Phase 1 — done: `ingest._detect_file_type` recognizes `.txt`/`.md`/`.markdown`/`.docx`/`.pptx`/`.xlsx`; `MaterialMeta.file_type` comment updated; tests added in `tests/test_ingest.py`.
- Phase 2 — done: `src/arquimedes/extract_text.py` provides `write_synthetic_extraction()` and shared text helpers; `extract.extract_raw()` now uses explicit per-type dispatch with a warn-skip else branch.
- Phase 3 — done: `extract_raw_text_file` and `extract_raw_markdown_file`; tests in `tests/test_extract_textlike.py`.
- Phases 4–6 — done: `src/arquimedes/extract_office.py` implements DOCX/PPTX/XLSX extractors using `python-docx`, `python-pptx`, `openpyxl` (added to `pyproject.toml`); tests in `tests/test_extract_office.py` use generated fixtures.
- Phase 7 — done: end-to-end dispatch test in `tests/test_extract.py` plus warn-skip test for unknown `file_type`. Audited downstream: `enrich.py:_allows_figure_enrichment` excludes only image/scanned types; new doc-like types pass through and short-circuit cleanly because no figures dir is produced. `serve.py`'s `file_type == "image"` branch is image-only by design.
- Phase 8 — done: `docs/developer/PIPELINE.md` lists the new supported extensions.

## Goal

Add deterministic ingest and `extract-raw` support for `.txt`, `.md`, `.markdown`, `.docx`, `.pptx`, and `.xlsx` while preserving the existing PDF/image pipeline.

## Non-goals

- No legacy `.doc`, `.ppt`, `.xls` support.
- No OCR of embedded Office images in the first slice.
- No LibreOffice conversion dependency.
- No schema redesign for pages/chunks.
- No enrichment prompt redesign unless tests reveal format assumptions.

## Phase 1: Ingest Recognition

Files:

- `src/arquimedes/ingest.py`
- `src/arquimedes/models.py` (comment update only)
- `tests/test_ingest.py`

Changes:

- Add extension sets:
  - `TEXT_EXTENSIONS = {".txt"}`
  - `MARKDOWN_EXTENSIONS = {".md", ".markdown"}`
  - `OFFICE_EXTENSIONS = {".docx", ".pptx", ".xlsx"}` or per-format sets
- Extend `SUPPORTED_EXTENSIONS` to include all new extensions.
- Extend `_detect_file_type(path)`:
  - `.txt` -> `text`
  - `.md` / `.markdown` -> `markdown`
  - `.docx` -> `docx`
  - `.pptx` -> `pptx`
  - `.xlsx` -> `xlsx`
- Keep unknown extensions skipped.
- Update the inline `file_type` comment on `MaterialMeta` (currently `# pdf | image | scanned_document`) to list the new values.
- Add tests for scan and explicit-path ingest for each extension.
- Add tests that unsupported legacy Office extensions remain skipped.

Acceptance:

- `arq ingest` registers new file types from `Research/`, `Practice/`, and `Proyectos/` folders.
- Manifest `file_type` is stable and format-specific.

## Phase 2: Shared Text-Like Extraction Helpers

Files:

- new `src/arquimedes/extract_text.py` or `src/arquimedes/extract_document.py`
- `src/arquimedes/extract.py`
- `tests/test_extract_textlike.py`

Changes:

- Introduce a helper that writes the common raw artifacts from a list of synthetic pages:
  - `meta.json`
  - `text.md`
  - `pages.jsonl`
  - `chunks.jsonl` via existing `chunk_pages`
  - optional `extraction_warnings.jsonl`
- Reuse existing `Page` model rather than adding a new schema.
- Set `MaterialMeta.page_count` to the synthetic page count when writing `meta.json`.
- Add robust text normalization helpers:
  - line ending normalization
  - control character cleanup if needed
  - paragraph-aware splitting by target character/word count
  - heading-aware Markdown splitting
- Decide whether `_sanitize_strings` remains imported from `extract_pdf.py` or moves to a shared utility. Prefer moving only if import cycles or clarity demand it.
- In `extract.py`, replace the current implicit fallthrough with explicit dispatch for every supported `file_type` and a final `else` that warns and skips. Today an unrecognized `file_type` silently creates `output_dir`, writes nothing, and still appends the id to the returned list — a partial implementation must not regress into that state.

Acceptance:

- A unit test can create synthetic pages and produce valid extraction files without PDF/image dependencies.

## Phase 3: TXT And Markdown Extractors

Files:

- `src/arquimedes/extract_text.py`
- `src/arquimedes/extract.py`
- `tests/test_extract_textlike.py`

Changes:

- Implement `extract_raw_text_file(...)` for `.txt`:
  - read UTF-8
  - fallback to replacement decoding or common encodings with warning
  - split into synthetic pages by paragraph/length
- Implement `extract_raw_markdown_file(...)` for `.md`/`.markdown`:
  - preserve source Markdown in `text.md`
  - split by headings while respecting fenced code blocks where practical
  - fall back to length splitting
- Add dispatch in `extract_raw()`:
  - `file_type == "text"`
  - `file_type == "markdown"`

Acceptance:

- Minimal `.txt` and `.md` fixtures extract to `meta.json`, `text.md`, `pages.jsonl`, `chunks.jsonl`.
- Markdown heading boundaries are represented in page text.

## Phase 4: DOCX Extractor

Files:

- `pyproject.toml`
- `src/arquimedes/extract_office.py` or `src/arquimedes/extract_docx.py`
- `src/arquimedes/extract.py`
- `tests/test_extract_office.py`

Changes:

- Add dependency: `python-docx`.
- Implement DOCX extraction:
  - paragraphs in document order
  - headings mapped to Markdown headings when style names indicate headings
  - tables rendered as Markdown or TSV blocks
  - basic warnings for unsupported features if detectable
- Add dispatch for `file_type == "docx"`.
- Add test fixture generated in test code to avoid binary fixture bloat where possible.

Acceptance:

- A generated DOCX with heading, paragraph, and table extracts searchable text and chunks.

## Phase 5: PPTX Extractor

Files:

- `pyproject.toml`
- `src/arquimedes/extract_office.py` or `src/arquimedes/extract_pptx.py`
- `src/arquimedes/extract.py`
- `tests/test_extract_office.py`

Changes:

- Add dependency: `python-pptx`.
- Implement PPTX extraction:
  - one synthetic page per slide
  - slide heading includes slide number and title if present
  - collect shape text ordered approximately by `(top, left)`
  - extract tables where available
  - include speaker notes if accessible without brittle XML hacks; otherwise document as deferred
- Add dispatch for `file_type == "pptx"`.

Acceptance:

- A generated PPTX with two slides extracts each slide as a synthetic page.

## Phase 6: XLSX Extractor

Files:

- `pyproject.toml`
- `src/arquimedes/extract_office.py` or `src/arquimedes/extract_xlsx.py`
- `src/arquimedes/extract.py`
- `tests/test_extract_office.py`

Changes:

- Add dependency: `openpyxl`.
- Implement XLSX extraction:
  - open workbook with `read_only=True`, `data_only=True`
  - one synthetic page per visible worksheet
  - render non-empty rows as Markdown tables or TSV blocks
  - skip fully empty sheets with warning or minimal page depending on expected UX
  - enforce configurable safety limits for rows/cells/characters per sheet
- Add dispatch for `file_type == "xlsx"`.

Acceptance:

- A generated XLSX with two sheets extracts sheet headings and cell values.
- Large-sheet bounds produce explicit warnings.

## Phase 7: CLI, Index, Compile Smoke Tests

Files:

- `tests/test_search.py` or a new integration-style test
- existing CLI tests if present
- `src/arquimedes/serve.py`, `src/arquimedes/compile_pages.py`, `src/arquimedes/templates/` (review for hardcoded "pdf"/"image" branches)
- `src/arquimedes/enrich.py`, `src/arquimedes/enrich_figures.py`, `src/arquimedes/enrich_document.py` (verify behavior for non-PDF doc-like types — `_allows_figure_enrichment` currently lets them through and should short-circuit on empty figure sets)

Changes:

- Add a smoke test that ingests and extracts one new-format material, rebuilds/uses chunks, and verifies no downstream assumptions reject the new `file_type`.
- Review material page rendering for file-type display. Add labels if the UI currently assumes only PDF/image.
- Review enrichment path for assumptions about PDF pages or figure availability. If figure enrichment hits the new types and produces noisy warnings, extend `_allows_figure_enrichment` to also exclude `text` and `markdown` (and any others without embedded media in this slice).

Acceptance:

- `arq extract <id>` works for at least one text-like and one Office fixture.
- Index rebuild can consume the resulting chunks.

## Phase 8: Documentation Updates

Files:

- `docs/developer/PIPELINE.md`
- `docs/developer/PLAN.md`
- relevant maintainer/collaborator docs if they list supported file types

Changes:

- Update supported source file lists.
- Mention known limitations:
  - no legacy Office formats
  - no OCR of embedded images in Office files
  - spreadsheets are extracted for searchability, not as perfect tables
- Keep this plan and the companion spec synchronized with implementation decisions.

Acceptance:

- Documentation reflects actual implemented support before merge.

## Suggested Implementation Order

1. Ingest recognition and tests.
2. Shared synthetic-page writer.
3. TXT/Markdown extraction.
4. DOCX extraction.
5. XLSX extraction.
6. PPTX extraction.
7. Downstream smoke tests and docs.

TXT/Markdown should land first because they validate the synthetic-page path with minimal dependencies. Office support can then reuse the same writer.

## Risks And Mitigations

| Risk | Mitigation |
| --- | --- |
| Office parser dependencies expose partial content | Record limitations and warnings; prioritize searchable text |
| Large XLSX files exhaust memory | Use read-only mode and configurable limits |
| Markdown splitting breaks fenced blocks | Start simple, add tests for fenced code before making heading splitting aggressive |
| Downstream code assumes PDF/image file types | Add smoke tests through extraction, index, and material rendering |
| Binary test fixtures bloat repo | Generate Office fixtures in tests where practical |

## Test Matrix

| Format | Ingest | Extract | Chunk | Downstream smoke |
| --- | --- | --- | --- | --- |
| `.txt` | required | required | required | required |
| `.md` | required | required | required | required |
| `.markdown` | required | required | required | optional alias test |
| `.docx` | required | required | required | required |
| `.pptx` | required | required | required | required |
| `.xlsx` | required | required | required | required |
| `.doc` | skipped | n/a | n/a | n/a |
| `.ppt` | skipped | n/a | n/a | n/a |
| `.xls` | skipped | n/a | n/a | n/a |
