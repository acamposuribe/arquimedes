# Proyectos Domain - Implementation Plan

> **Status:** Draft for discussion
> **Date:** 2026-04-30
> **Companion spec:** [Proyectos domain design](../specs/2026-04-30-proyectos-domain-design.md)
> **References:** [Practice domain Phase 1](2026-04-27-practice-domain-phase1.md), [Operational pipeline](../../PIPELINE.md), [Collection graph architecture](../specs/2026-04-09-collection-graph-design.md)

## Goal

Introduce `proyectos` as a project-centric domain for office productivity and archival memory.

The first implementation should prove the new publication mode without disturbing Research or Practice:

1. Ingest `Proyectos/<project-id>/...` as domain `proyectos`.
2. Enrich project materials with project-operational fields.
3. Skip concept clustering and bridge-concept synthesis for this domain.
4. Maintain one structured project state artifact per project.
5. Maintain editable project section artifacts for prose that Hermes or reflection can improve.
6. Compile a useful project page from state, sections, notes, and materials.
7. Expose a small structured CLI update surface for Hermes.

## Non-goals

- No migration of existing Research or Practice artifacts.
- No full Discord ingestion pipeline or periodic channel export packets in the first slice.
- No budget-table OCR/parser specialization in the first slice.
- No arbitrary markdown editing by Hermes.
- No MCP tool surface for Hermes in the first slice.
- No cross-project concept graph for Proyectos.
- No redesign of the current collection graph for Research or Practice.

## Phase 0: Ratify Decisions (resolved)

These were open during drafting and are now committed in the spec. Listed here so the plan stands alone.

- Internal slug: `proyectos` (folder also `Proyectos/`).
- Project id convention: `<job-number>-<slug>`, e.g. `2407-casa-rio`.
- Stage taxonomy v1: `lead → feasibility → schematic_design → basic_project → execution_project → tender → construction → handover → archived`.
- Prompt-version suffix for Proyectos: `proyectos-es-v1`.
- Project-extraction storage: nested under `project_extraction` inside the existing document/chunk enrichment JSON, not promoted to top-level keys. Top-level `summary`, `keywords`, `facets` are still produced (Spanish, project-flavored) so material pages render with shared templates. Concept-candidate emission is suppressed at the prompt level for Proyectos, not schema-deleted.
- Hermes notes are committed to the vault with provenance.
- Project note kind enum v1: `decision`, `requirement`, `risk`, `deadline`, `coordination`, `learning`, `mistake`, `repair`.
- Section-replacement protocol: schema-enforced via `revision`, `replaces_updated_at`, `justification`, and `references_prior_body` in the delta payload. See spec for merge rules.
- `proyectos/_general` is an intake bucket: ingested but no project page compiled, no project reflection run; surfaced as an ingest warning instead.
- `updated_by` enum: `reflection`, `hermes`, `human`, `cli`. Never `lint`.
- Discord ingestion deferred. Hermes records a persistent note when a discussion captures: a decision, requirement, risk, deadline, coordination issue, learning, mistake, or repair action.

## Phase 1: Domain Profile And Ingest Recognition

Files:

- `src/arquimedes/domain_profiles.py`
- ingest/domain path parsing modules
- domain label tests
- ingest tests

Changes:

- Add a `proyectos` profile with `publication_mode="project_dossier"`, `output_language="Spanish"`, `prompt_version_suffix="proyectos-es-v1"`.
- Extend `DomainProfile` with publication behavior flags:
  - `publication_mode`: `concept_graph` | `project_dossier`
  - `run_local_clustering`
  - `run_concept_reflection`
  - `run_collection_reflection`
  - `run_project_reflection`
  - `run_global_bridge`
  - `run_office_learning`
- Add helper API on `domain_profiles.py` (call sites use these instead of reading flags off the dataclass directly):
  - `get_publication_mode(domain)`
  - `should_run_clustering(domain)`
  - `should_run_concept_reflection(domain)`
  - `should_run_collection_reflection(domain)`
  - `should_run_project_reflection(domain)`
  - `should_run_global_bridge(domain)`
  - `should_run_office_learning(domain)`
  - `is_proyectos_domain(domain)`
- Update `_PROFILES` so `research` and `practice` get `publication_mode="concept_graph"` and the existing run flags default to their current behavior.
- Update `src/arquimedes/ingest.py:34` `DOMAIN_FOLDERS` set to include `proyectos`. `_derive_domain` and `_derive_collection` should require no other change.
- Add Spanish UI/generated labels for project pages (a Proyectos label dict; can clone from `_PRACTICE_LABELS` and override project-specific keys like `estado`, `trabajo_en_curso`, `riesgos`, `proximo_foco`).
- Update `display_domain_name` to return "Proyectos" for the new domain.

Tests:

- `Proyectos/foo/file.pdf` becomes `domain=proyectos`, `collection=foo`.
- `Proyectos/loose.pdf` becomes `domain=proyectos`, `collection=_general` (still ingested; quarantine behavior is enforced in Phase 5/6, not ingest).
- Unknown-domain behavior remains unchanged for Research and Practice.
- `display_domain_name("proyectos")` and generated labels render correctly.
- Helper API returns expected booleans for all three domains.

## Phase 2: Project Prompt Module

Files:

- `src/arquimedes/project_prompts.py` - new
- `src/arquimedes/enrich_prompts.py`
- `tests/test_enrich_prompts.py`

Changes:

- Add dedicated project-focused document, chunk, figure, and table prompt fragments. Proyectos enrichment should not reuse Research or Practice prompts with only language changes.
- Ask for Spanish outputs.
- Ask the model to classify `project_material_type`.
- Ask for operational evidence written into a `project_extraction` sub-block of the document/chunk enrichment JSON (not top-level keys):
  - relevance
  - main points
  - decisions
  - requirements
  - risks/blockers
  - open items
  - actors
  - dates/deadlines
  - spatial/design scope
  - budget signals
  - evidence refs
- Top-level `summary`, `keywords`, `facets` are still produced (Spanish, project-flavored).
- Suppress concept-candidate emission for Proyectos at the prompt level. The schema field stays; the prompt simply does not request candidates.
- Stamp `prompt_version` with `-proyectos-es-v1` via the existing `domain_prompt_version` helper.

Tests:

- Proyectos prompts mention project material types and operational extraction.
- Research and Practice prompts do not change.
- Prompt versioning distinguishes Proyectos if semantic outputs change.

## Phase 3: Skip Concept Graph Publication

Files:

- `src/arquimedes/cluster.py`
- `src/arquimedes/lint.py`
- `src/arquimedes/lint_concept_reflection.py`
- `src/arquimedes/lint_global_bridge.py`
- pipeline/watch orchestration where needed
- tests around cluster/lint routing

Changes:

- Use domain profile flags to skip local clustering for `proyectos`.
- Skip concept reflection for `proyectos`.
- Skip bridge-concept synthesis for `proyectos`.
- Keep indexing, material pages, figure extraction, and material search.
- Ensure `arq cluster` reports that Proyectos is intentionally skipped rather than silently doing nothing.

Tests:

- Running cluster with only Proyectos materials produces no local cluster artifacts and exits successfully.
- Mixed-domain runs still cluster Research and Practice.
- Full lint does not attempt concept or bridge-concept prompts for Proyectos.

## Phase 4a: Project State And Notes

Phase 4 is split because state+notes is the easy half and section artifacts carry the load-bearing replacement protocol. Land 4a first; 4b after the project page can render.

Files:

- `src/arquimedes/project_state.py` - new
- `src/arquimedes/lint_project_reflection.py` - new
- `tests/test_project_state.py` - new
- `tests/test_lint_project_reflection.py` - new

Changes:

- Define load/save/merge helpers for:
  - `derived/projects/<project-id>/project_state.json`
  - `derived/projects/<project-id>/notes.jsonl`
- State schema follows the spec field list, including the v1 stage enum and the three-horizon discipline (`main_objectives` = end-of-project, `current_work_in_progress` = this week, `next_focus` = next 1–2 weeks).
- `updated_by` is constrained to the enum `reflection`, `hermes`, `human`, `cli`. `lint` is rejected.
- Deterministic rules:
  - notes are append-only with `{actor, timestamp, kind, text, source_refs, material_id?, confidence?}`
  - reflection deltas merge field-by-field without dropping unrelated existing fields
  - regressive `stage` transitions require a justification recorded in `mistakes_or_regrets` or `repair_actions`
  - reflection prompts must enforce three-horizon disjointness; tests assert the same item does not appear across `main_objectives`/`current_work_in_progress`/`next_focus`
- Add a project reflection prompt (`lint_project_reflection.py`) that reads recent project evidence + prior state and emits a state delta only (no section deltas in 4a).

Tests:

- Empty project state scaffolds deterministically.
- Notes append with actor/timestamp/source refs.
- Reflection state deltas merge without dropping unrelated existing fields.
- `updated_by="lint"` is rejected.
- Backwards stage transition without justification is rejected.
- Same-item-across-horizons is rejected (or warned, depending on validator strictness).
- Invalid deltas fail with actionable messages.

## Phase 4b: Section Artifacts And Replacement Protocol

Files:

- `src/arquimedes/project_state.py` - extend
- `src/arquimedes/lint_project_reflection.py` - extend
- `tests/test_project_state.py` - extend
- `tests/test_lint_project_reflection.py` - extend

Changes:

- Add load/save/merge helpers for `derived/projects/<project-id>/sections.json`.
- Section record schema per spec: `section_id`, `title`, `body`, `updated_at`, `updated_by`, `source_refs`, `evidence_material_ids`, `confidence`, `protected`, `revision`.
- Implement the section-replacement protocol exactly as specified:
  - reject deltas where `replaces_updated_at` does not match on-disk `updated_at`
  - reject reflection writes against `protected=true` sections unless `justification` is non-empty AND `references_prior_body=true`
  - Hermes/human writes always succeed (subject to revision/replaces_updated_at) and flip `protected=true`
  - reflection writes preserve `protected` as-is, never flip it to false
  - `revision` is the authoritative ordering; `revision` must equal `prior.revision + 1`
- Extend the project reflection prompt to also propose section deltas. Section deltas go through the same validator.

Tests (the load-bearing set):

- Stale `replaces_updated_at` rejected.
- Reflection overwrite of `protected=true` without `justification` rejected.
- Reflection overwrite of `protected=true` without `references_prior_body=true` rejected.
- Hermes overwrite always wins and sets `protected=true`.
- Two reflection writes interleaved correctly (the second sees the first's `updated_at`).
- Reflection write does not flip `protected` from true to false.
- Bad `revision` (skip or repeat) rejected.

## Phase 5: Project Page Compilation

Files:

- `src/arquimedes/compile.py`
- `src/arquimedes/compile_pages.py`
- `src/arquimedes/serve.py`
- `src/arquimedes/read.py`
- project/page templates if separate templates are useful
- compile tests

Changes:

- For `publication_mode=project_dossier`, compile `wiki/proyectos/<project-id>/_index.md` as a project dashboard.
- Skip page compilation for `proyectos/_general` and emit an ingest-style warning listing the loose files so the maintainer can move them.
- Render from `project_state.json`, `sections.json` (when present after 4b), `notes.jsonl`, and material evidence:
  - current status
  - work in progress
  - objectives
  - known conditions
  - decisions and requirements
  - risks/blockers
  - missing information
  - next focus
  - important materials
  - learnings
  - mistakes and repair actions
  - recent notes
- Group the project's "Materiales del proyecto" list by `project_extraction.project_material_type` (Informes de reunión, Planos, Normativa, Fotografías de obra, etc.); fall back to "Sin clasificar" when the type is missing. Pass `project_material_type` from each meta into `render_project_page` from `compile.py`.
- Avoid rendering local concept sections for Proyectos by default.
- Web UI surfacing: extend `_DOMAINS` in `src/arquimedes/serve.py` and `src/arquimedes/read.py` to include `proyectos` so the domain tab, wiki rail, search scoping, and Spanish UI language all activate for the Proyectos vault.

Tests:

- Project page compiles without cluster artifacts.
- Important materials link to material pages.
- Hermes-authored section text appears on the compiled page (after 4b lands).
- Spanish headings render for Proyectos.
- Materials are grouped by project material type with Spanish headings and a "Sin clasificar" fallback.
- `proyectos/_general` produces no page and emits a warning listing affected files.
- Research and Practice collection pages remain unchanged.
- `Proyectos` appears as a third domain tab in the web UI; `/wiki/proyectos`, `/?domain=proyectos`, and `/search?domain=proyectos` all resolve.

## Phase 6: Hermes CLI Write Surface

Files:

- `src/arquimedes/cli.py`
- `src/arquimedes/project_state.py`
- `docs/collaborator/hermes-projects-handbook.md` or another short Hermes-facing handbook path
- CLI tests

Commands (deterministic, no LLM, ship in Phase 6):

```bash
arq project status <project-id>
arq project note <project-id> --kind decision|requirement|risk|deadline|coordination|learning|mistake|repair --text ...
arq project update <project-id> --field next_focus --text ...
arq project append <project-id> --field risks_or_blockers --text ...
arq project resolve <project-id> --item <id> --note ...
arq project section set <project-id> <section-id> --text ...
arq project recompile <project-id>
arq project reflect <project-id>
```

Commands (LLM-backed, deferred — see open question 1 in spec):

```bash
arq project section improve <project-id> <section-id> --instruction ...
```

`section improve` is structurally a single-section reflection invocation, not a deterministic write. It calls the section-improve prompt, produces a section delta, and routes it through the merge protocol from Phase 4b. It depends on enrichment LLM routes being configured and obeys the same precedence rules as nightly reflection. Decide during Phase 6 whether to ship it or defer; do not bundle it into the deterministic CLI surface.

By default, deterministic write commands trigger an immediate per-project recompile so Hermes notes appear on the page right away. `--no-recompile` is available for batched writes followed by `arq project recompile`.

Handbook:

- Add a short Hermes-facing guide for project memory.
- Explain channel-to-project mapping.
- Explain when to record a Discord discussion as a project note.
- Explain when to improve a whole section artifact instead of appending another note.
- Tell Hermes to include Discord channel/message refs when available.
- Tell Hermes to use CLI commands and never edit compiled markdown directly.

Tests:

- CLI writes structured notes with provenance.
- CLI validates project ids and allowed fields.
- Compile after an update reflects the new state.
- Compile after a section update renders the improved section text.
- Invalid field writes are rejected.
- Reflection evidence includes Hermes notes and section edits.
- Reflection treats Hermes warnings as high-priority evidence that must be preserved, resolved, or explicitly challenged with evidence.

## Phase 7: Office Learning Pass

Office learning is implemented as a publication-mode variant inside the existing `lint_global_bridge.py` runner, **not** a fresh module. Proyectos does not produce global bridge concepts or bridge pages; it only reuses the runner's orchestration machinery. The runner already has incremental packet building, route handling, the "<2 collections, skip" guard, and prompt-version stamping, all of which office learning needs identically.

Files:

- `src/arquimedes/lint_global_bridge.py` - modify to dispatch on `DomainProfile.publication_mode`
- `src/arquimedes/project_prompts.py` - add office-learning prompt fragments
- compile support for `wiki/proyectos/_office-learning.md`
- tests

Changes:

- Add a `publication_mode`-keyed dispatch in the bridge runner: `concept_graph` keeps current bridge-concept behavior; `project_dossier` runs office-learning synthesis using packets built from `project_state.json` instead of cluster artifacts.
- Build compact packets from project states. Focus only on:
  - repeated blockers
  - mistakes and repairs
  - positive learnings
  - process improvements
  - reusable templates/checklists
- Write:
  - `derived/domains/proyectos/office_learning.json`
  - `wiki/proyectos/_office-learning.md`

Tests:

- Requires at least two projects unless explicitly forced.
- Does not consume raw material concepts.
- Produces office-level findings with source project ids.
- Mixed-domain run: Research/Practice bridge-concept synthesis still runs with old behavior; Proyectos runs office learning. No cross-contamination.

## File Map

| File | Action | Responsibility |
|---|---|---|
| `src/arquimedes/domain_profiles.py` | Modify | Add Proyectos profile and publication behavior flags |
| ingest path parsing modules | Modify | Recognize `Proyectos/` top-level folder |
| `src/arquimedes/project_prompts.py` | Create | Project-domain prompt text |
| `src/arquimedes/enrich_prompts.py` | Modify | Route Proyectos enrichment prompts |
| `src/arquimedes/cluster.py` | Modify | Skip project-dossier domains by default |
| `src/arquimedes/lint.py` | Modify | Route project reflection and skip concept graph stages |
| `src/arquimedes/project_state.py` | Create | Project state, notes, and section artifact persistence |
| `src/arquimedes/lint_project_reflection.py` | Create | Nightly project reflection stage |
| `src/arquimedes/compile_pages.py` | Modify | Render project dashboard pages |
| `src/arquimedes/cli.py` | Modify | Add `arq project ...` commands |
| Hermes handbook | Create | Short local CLI instructions for Hermes |
| `src/arquimedes/lint_global_bridge.py` | Modify | Dispatch on `publication_mode`; route project_dossier to office-learning synthesis |

## Dependency Order

```text
domain profile flags + helper API
    |
    +--> ingest recognition
    +--> project prompts (project_extraction sub-block)
    +--> cluster/lint skips (publication_mode dispatch)
            |
            +--> project_state.py (state + notes)        [Phase 4a]
                    |
                    +--> lint_project_reflection.py (state delta)
                    +--> compile project page (state + notes only)
                    +--> sections.json + replacement protocol     [Phase 4b]
                            |
                            +--> compile project page (with sections)
                            +--> CLI Hermes writes
                                    |
                                    +--> office learning (lint_global_bridge dispatch)
```

## Suggested First Slice

Start with a narrow but visible vertical slice:

1. Add `proyectos` profile, helper API, and ingest recognition.
2. Add project prompts for enrichment (project_extraction sub-block).
3. Skip clustering and bridge-concept synthesis for Proyectos via `publication_mode` dispatch.
4. Land Phase 4a: `project_state.py` with state + notes (no sections yet).
5. Compile a sparse project page from state, important materials, and notes.

This gives the office a real project page before sections, the section-replacement protocol, the Hermes CLI surface, or office-learning synthesis. Phase 4b (sections) and Phase 6 (CLI) follow once the page renders end to end.

## Verification Commands

Expected focused commands, to be refined during implementation:

```bash
pytest tests/test_enrich_prompts.py
pytest tests/test_project_state.py
pytest tests/test_lint_project_reflection.py
pytest tests/test_compile.py
pytest tests/test_cli.py
```

## Rollout Notes

- Existing Research and Practice outputs should not change.
- Existing Proyectos materials, if any, will need ingest/extract reruns after folder recognition lands.
- **Cross-domain misclassification is a manual move.** Files previously dropped into `Research/<project>/` or `Practice/<project>/` are not relocated automatically. The maintainer moves them into `Proyectos/<project-id>/`, then re-runs ingest/extract; the old domain entries are removed via the existing manifest cleanup path.
- Project pages can start sparse and improve as reflection fields stabilize.
- Hermes uses structured project CLI commands from the start; compiled markdown remains generated output.
- Hermes-curated notes and section edits are treated as high-priority project evidence in reflection packets, not disposable chat metadata.

## Definition Of Done

- Proyectos is recognized as a first-class domain.
- Project pages are useful without concept clusters.
- The pipeline intentionally skips concept-graph stages for Proyectos.
- Project state is structured, versionable, and provenance-aware.
- Hermes has a safe, narrow CLI write API for project memory.
- Hermes can improve project-page sections by editing section artifacts through CLI commands.
- Hermes has a short handbook explaining when to record Discord discussions as persistent project notes.
- The design leaves a clear later path for Discord digest ingestion and office-wide learning.
