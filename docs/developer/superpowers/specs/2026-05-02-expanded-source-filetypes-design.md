# Expanded Source Filetypes: Text, Markdown, Office Documents - Design Spec

> **Status:** Draft for discussion
> **Date:** 2026-05-02
> **Companion plan:** [Expanded source filetypes implementation plan](../plans/2026-05-02-expanded-source-filetypes.md)
> **References:** [Arquimedes knowledge system design](2026-04-04-arquimedes-knowledge-system-design.md), [Operational pipeline](../../PIPELINE.md)

## Purpose

Arquimedes currently ingests and extracts PDFs and images only. This spec adds first-class raw extraction support for common non-PDF source files used in architectural practice and research:

- plain text: `.txt`
- Markdown: `.md`, `.markdown`
- Word: `.docx`
- PowerPoint: `.pptx`
- Excel: `.xlsx`

The goal is to let collaborators drop ordinary documents, notes, presentations, and spreadsheets into the same library folders and have the maintainer pipeline produce deterministic `extracted/<material_id>/` artifacts compatible with search, enrichment, compile, and project dossiers.

## Current Behavior

`src/arquimedes/ingest.py` supports only:

- `.pdf` as `file_type="pdf"`
- `.jpg`, `.jpeg`, `.png`, `.tiff`, `.tif`, `.bmp`, `.webp` as `file_type="image"` or `file_type="scanned_document"`

All other extensions are ignored during library scans. If passed explicitly to `arq ingest`, they are detected as `unknown` and skipped.

`src/arquimedes/extract.py` dispatches only to PDF and image extraction handlers. Note the dispatch in `extract_raw()` silently no-ops on unrecognized `file_type` values today: it creates the output directory, never writes `meta.json`, and still appends the id to the returned list. New file types must take their own dispatch branch — partial support cannot rely on this silent fallthrough.

`MaterialMeta.file_type` in `src/arquimedes/models.py` carries an inline comment listing only `pdf | image | scanned_document`. New values must be added there too.

`src/arquimedes/enrich.py:_allows_figure_enrichment` excludes only `image` and `scanned_document`. The new doc-like types will pass that gate; in practice they will produce no figures and should short-circuit cleanly, but the assumption is worth verifying.

## Design Goals

1. Preserve the existing manifest and extraction contract.
2. Keep extraction deterministic and local; no LLM during `extract-raw`.
3. Produce useful text and chunk artifacts even when the source has no page model.
4. Preserve enough source structure for later enrichment and citation.
5. Avoid silent data loss: unsupported embedded content should be noted in metadata or extraction warnings.
6. Keep dependencies conservative and pure-Python where practical.
7. Do not change the behavior of existing PDF/image materials.

## Supported File Types

| Extension | Manifest `file_type` | Extraction model |
| --- | --- | --- |
| `.txt` | `text` | one or more synthetic pages split by length |
| `.md`, `.markdown` | `markdown` | heading-aware synthetic pages/sections |
| `.docx` | `docx` | paragraphs and tables, optionally embedded images later |
| `.pptx` | `pptx` | one synthetic page per slide |
| `.xlsx` | `xlsx` | one synthetic page per sheet, table-oriented text |

Legacy binary Office formats (`.doc`, `.ppt`, `.xls`) are out of scope for this slice. They should remain unsupported unless converted to modern OOXML or PDF.

## Extraction Contract

Every new extractor must write the same core files expected by downstream stages:

```text
extracted/<material_id>/
├── meta.json
├── text.md
├── pages.jsonl
└── chunks.jsonl
```

Optional files may be added when useful:

```text
├── tables.jsonl          # docx/xlsx table records if introduced
├── figures/              # future embedded Office media extraction
└── extraction_warnings.jsonl
```

The minimum viable implementation should not require new downstream readers. It can encode tables and slide/sheet boundaries into `text.md` and `Page.text` first, then add richer sidecars later.

## Page Model For Non-Paged Sources

Arquimedes already uses `Page` records as the unit before chunking. For non-PDF sources, `page_number` becomes a stable synthetic ordinal:

- text: page 1..N based on length windows
- markdown: page 1..N based on top-level sections, falling back to length windows
- docx: page 1..N based on document sections or length windows; table text stays near surrounding paragraphs
- pptx: page N equals slide N
- xlsx: page N equals worksheet N

Synthetic page titles should be included in text, for example:

```markdown
# Slide 3: Design Options
```

or

```markdown
# Sheet: Budget
```

This keeps citations readable without changing the downstream schema.

## Text And Markdown Extraction

Plain text extraction should:

- read UTF-8 by default
- fall back to UTF-8 with `errors="replace"` and emit a warning rather than pulling in an encoding-detection dependency for the first slice
- normalize line endings
- write `text.md` with the source text wrapped in a fenced code block so Markdown control characters don't render as headings/lists; per-page Markdown can prepend a `# Page N` heading above the fence
- split long files into synthetic pages by paragraph boundaries where possible

Markdown extraction should:

- preserve original Markdown text in `text.md`
- split by headings where practical
- avoid rendering Markdown to HTML during raw extraction
- keep fenced code blocks intact when splitting

## DOCX Extraction

DOCX extraction should use `python-docx` unless a better dependency is selected during implementation.

Minimum extraction:

- document paragraphs in order
- headings as Markdown headings when style names indicate headings
- bullet/numbered paragraphs as list-like lines where possible
- tables rendered as Markdown tables or simple TSV-style blocks
- headers/footers may be deferred but should be listed as a known limitation
- comments, tracked changes, and embedded images may be deferred

DOCX output should prefer readable Markdown over exhaustive OOXML fidelity.

## PPTX Extraction

PPTX extraction should use `python-pptx` unless a better dependency is selected during implementation.

Minimum extraction:

- one synthetic page per slide
- slide title, body text, tables, and speaker notes if accessible
- shape text ordered approximately by top/left position
- include slide number headings in `text.md`

Known limitations to record:

- text embedded inside images is not OCRed in the first slice
- SmartArt and charts may expose partial text only
- embedded media extraction is optional/future

## XLSX Extraction

XLSX extraction should use `openpyxl` in read-only/data-only mode.

Minimum extraction:

- one synthetic page per visible worksheet
- sheet title as heading
- non-empty cell ranges rendered as Markdown tables or TSV blocks
- formula cells should use cached values where available (`data_only=True`)
- very large sheets should be bounded by configurable row/cell limits with warnings

Spreadsheets are often semi-structured. Extraction should prioritize searchability and evidence over perfect table reconstruction.

## Metadata

`meta.json` should continue to include existing raw fields. New `file_type` values should flow from the manifest. In particular:

- `page_count` must equal the number of synthetic pages written to `pages.jsonl` (slides for pptx, sheets for xlsx, length/heading windows otherwise) so downstream UIs that show "N pages" stay sensible.
- `raw_document_type` should be left empty for the new types unless a deterministic classifier is added; existing PDF heuristics in `classify.py` should not be force-applied.
- `raw_keywords` may be populated from the extracted text using the same deterministic path as PDFs if it runs cleanly over `Page.text`; otherwise leave empty in this slice.

Recommended additional raw metadata fields, if easy to add without schema churn:

- `source_format`: extension or MIME-like label
- `synthetic_page_model`: `length_window`, `markdown_heading`, `docx_flow`, `pptx_slide`, `xlsx_sheet`
- `extraction_warnings`: summary count or short list

If adding fields to `MaterialMeta` is too invasive, warnings can live in `extraction_warnings.jsonl` initially.

## Error Handling

- Corrupt or encrypted Office files should not crash the whole extraction run.
- The extractor should write a warning and skip the material, or create minimal `meta.json` plus warning artifacts if consistent with existing failure behavior.
- Password-protected Office files are unsupported in this slice.
- Extremely large text/spreadsheet files should be truncated or bounded with explicit warnings rather than exhausting memory.

## Dependencies

Proposed dependencies:

- `python-docx` for `.docx`
- `python-pptx` for `.pptx`
- `openpyxl` for `.xlsx`

These should be added to `pyproject.toml` only when implementation starts.

## Non-goals

- No legacy `.doc`, `.ppt`, `.xls` support.
- No OCR of images embedded in Office files in the first slice.
- No round-trip reconstruction of Office formatting.
- No conversion through LibreOffice as a required dependency.
- No changes to enrichment prompts beyond ensuring new file types are accepted as ordinary materials.

## Acceptance Criteria

1. `arq ingest` registers `.txt`, `.md`, `.markdown`, `.docx`, `.pptx`, and `.xlsx` files in enabled domain folders.
2. Manifest entries use distinct `file_type` values for the new formats.
3. `arq extract-raw <id>` creates valid `meta.json`, `text.md`, `pages.jsonl`, and `chunks.jsonl` for each new format.
4. `arq extract` continues to run raw extraction plus enrichment for the new formats.
5. Search/index rebuild can ingest chunks from the new formats without special cases.
6. Existing PDF/image tests continue to pass.
7. Tests cover ingest detection and one minimal extraction fixture per new format.
