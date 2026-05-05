# Proyectos Domain: Project-Centric Office Memory - Design Spec

> **Status:** Draft for discussion
> **Date:** 2026-04-30
> **Companion plan:** [Proyectos domain implementation plan](../plans/2026-04-30-proyectos-domain.md)
> **References:** [Arquimedes knowledge system design](2026-04-04-arquimedes-knowledge-system-design.md), [Collection graph architecture](2026-04-09-collection-graph-design.md), [Practice domain Phase 1](../plans/2026-04-27-practice-domain-phase1.md), [Operational pipeline](../../PIPELINE.md)

## Purpose

Add a third domain, `proyectos`, for an architecture office that wants Arquimedes to maintain project memory rather than research or reusable practice knowledge.

Research and Practice use collections as semantic neighborhoods: materials produce concepts, concepts become local clusters, and clusters can become bridge concepts. Proyectos should treat each collection as one live office project. Its main output is not a concept graph. Its main output is an evolving project page that helps the office and its Hermes agent understand:

- what the project is
- what stage it is in
- what material has arrived
- what decisions, constraints, risks, requests, and obligations are now known
- what is missing
- what the office should focus on next
- what has been learned for future projects
- what mistakes need repair

The design keeps the existing ingest, extraction, enrichment, search, wiki, and server publication machinery, but gives `proyectos` a different domain profile and a different reflective maintenance path.

## Core Principle

In `proyectos`, collections are not containers of concepts. Collections are active project dossiers.

That means:

- collection identity is authoritative: `Proyectos/<project-name>/...` is the project
- material pages remain useful evidence pages
- project visual-material pages may expose an admin-only cleanup mode (`?mode=admin`) for deleting bad extracted figure artifacts directly from the web UI; this removes the extracted image file and its JSON sidecar, and is a maintenance workflow rather than normal reading mode
- project pages become the primary semantic publication
- clustering is disabled by default
- bridge-concept synthesis is replaced by office-learning synthesis
- Hermes should be able to update dedicated project reflection fields through explicit local `arq` CLI commands

The goal is not to make projects isolated. Search can still span the whole domain, and a later global learning pass can compare projects. The difference is that cross-project connection is secondary to project status, next actions, and institutional learning.

## Domain And Folder Model

The library root gains a third top-level folder:

```text
LIBRARY_ROOT/
├── Research/
├── Practice/
└── Proyectos/
    ├── 2407-casa-rio/
    ├── 2501-oficinas-centro/
    └── _general/
```

Rules:

- `Proyectos/` maps to domain slug `proyectos`.
- The second-level folder maps to `collection`, and in this domain the collection is the project id.
- Direct files inside `Proyectos/` go to `_general`, but this should be treated as an intake/error bucket rather than a normal project.
- Project ids should be stable folder slugs, not client-private prose names, because they become paths, URLs, and agent handles.
- A project may have optional project metadata, but folder placement remains the source of truth for domain and project membership.
- `Proyectos/_general` is reserved as an intake/error bucket. Files that land there are still ingested as `domain=proyectos`, `collection=_general`, but compile **must not** generate a project dossier page for `_general`, and project reflection **must not** run on it. Instead it surfaces as an ingest warning so the maintainer can move the file under a real project folder. This is a hard rule, not a "should."

## Mental Model

Research:

```text
material -> concepts -> local clusters -> bridge concepts -> wiki synthesis
```

Practice:

```text
material -> concepts -> practical local clusters -> practical bridge concepts -> wiki synthesis
```

Proyectos:

```text
material -> project evidence -> project state -> project reflections -> office learning
```

This is a parallel publication mode, not a small prompt variation.

## Material Understanding

Enrichment for `proyectos` is a dedicated behavior, not a reuse of Research or Practice prompts. It should first classify what role a material plays inside a project, then extract operational evidence for project status and next actions. The material's folder path is relevant enrichment context: in office project dossiers, folders often encode phase, delivery package, discipline, source actor, or material type. Prompts should expose the relative source path and instruct the LLM to treat folder names as clues, while still grounding claims in content and recording path-based inferences in `evidence_refs`.

Suggested `project_material_type` values:

- `meeting_report`
- `meeting_notes`
- `client_request`
- `authority_request`
- `regulation`
- `drawing_set`
- `technical_report`
- `working_document`
- `budget_table`
- `site_photo`
- `map_or_cartography`
- `contract_or_admin`
- `email_or_message_export`
- `schedule`
- `unknown`

Suggested project-facing extracted fields:

- `project_relevance`: why this material matters for the project
- `main_points`: compact bullets of the material's important content
- `decisions`: decisions recorded or implied by the material
- `requirements`: requirements, obligations, requests, or constraints
- `risks_or_blockers`: problems, conflicts, missing approvals, contradictions, delays
- `open_items`: questions, follow-up tasks, information gaps
- `actors`: client, consultant, authority, contractor, supplier, internal team, when inferable
- `dates_and_deadlines`: dated events, submissions, meetings, due dates
- `spatial_or_design_scope`: zones, drawings, lots, rooms, systems, or design objects affected
- `budget_signals`: costs, line items, allowances, deviations, payment implications
- `evidence_refs`: pages, figures, tables, annotations, filenames, or message timestamps supporting the above

**Storage decision (committed, not deferred):** project-extraction fields are written to a dedicated `project_extraction` sub-block inside the existing document and chunk enrichment JSON, not promoted to top-level keys. This keeps Research/Practice readers untouched and gives Proyectos a single namespaced object to evolve. Top-level `summary`, `keywords`, and `facets` continue to be produced (Spanish, project-flavored) so material pages still render with shared templates. Concept-candidate emission for Proyectos is suppressed at the prompt level rather than schema-deleted, because the storage path is shared.

Prompt versioning for Proyectos uses suffix `proyectos-es-v1` (mirroring the Practice precedent of `practice-es-v1`).

## Project State Artifact

Each project should have a durable machine-readable state artifact.

Suggested path:

```text
derived/projects/<project-id>/project_state.json
```

Suggested fields:

- `domain`: always `proyectos`
- `project_id`
- `project_title`
- `main_strategy`: the project's governing design/living-place strategy; this is the highest-priority project framing and should be checked before subsequent decisions
- `stage`: one of the v1 stage enum below
- `stage_confidence`: float in [0, 1]
- `last_material_ids`: most recent N material ids that informed this state
- `main_objectives`: end-of-project deliverables and commitments (the "what is this project for")
- `current_work_in_progress`: what is being executed *this week* by the office
- `next_focus`: what should happen in the next 1–2 weeks (action-oriented, not deliverable-oriented)
- `known_conditions`: site, regulatory, budgetary, contractual constraints already established
- `decisions`: decisions recorded with provenance
- `requirements`: client / authority / technical requirements
- `risks_or_blockers`: open risks, conflicts, missing approvals, contradictions, delays
- `missing_information`: what we still need to know to move forward
- `positive_learnings`: things that worked, useful for future projects
- `mistakes_or_regrets`: what went wrong, even if already repaired
- `repair_actions`: concrete actions to fix or recover from a mistake on *this* project
- `important_material_ids`
- `updated_at`
- `updated_by`: one of `reflection`, `hermes`, `human`, `cli`. Never `lint` — `lint` is the runner, the actor is the reflection stage.

The three time-horizon fields (`main_objectives`, `current_work_in_progress`, `next_focus`) are explicitly disjoint by horizon. Reflection prompts must enforce this — items that fit in `current_work_in_progress` must not also appear in `next_focus`, and items in `main_objectives` are project-end, not weekly-task scope. This is the only way to keep the LLM from producing the same bullet across all three fields.

### Stage enum (v1, committed)

```text
lead
feasibility
schematic_design
basic_project
execution_project
tender
construction
handover
archived
```

This is the canonical Spanish-architecture-office sequence. `stage` is monotonic in normal operation (a project usually does not regress); reflection that proposes a backwards transition must include a justification under `mistakes_or_regrets` or `repair_actions`. New stages are added by bumping `proyectos-es-v1` to `-v2`.

This artifact is the project equivalent of cluster memory: compact, durable, and suitable for future LLM passes.

Some project-page prose should also be editable as source, not only generated from scalar state fields. Hermes may need to improve a section with the full project picture in mind. That should happen by editing section artifacts, not by hand-editing compiled markdown.

Suggested path:

```text
derived/projects/<project-id>/sections.json
```

Section record schema:

- `section_id`: `estado`, `trabajo_en_curso`, `riesgos`, `proximo_foco`, etc.
- `title`
- `body`
- `updated_at`
- `updated_by`: `reflection`, `hermes`, `human`, or `cli`
- `source_refs`: references to project notes, materials, or earlier sections used to compose this body
- `evidence_material_ids`
- `confidence`: float in [0, 1]
- `protected`: true when this section was authored or last edited by `hermes` or `human`
- `revision`: monotonically increasing integer per section_id

The compiled project page renders these section artifacts directly.

### Section replacement protocol (committed, not narrative)

Reflection may propose new section text, but the merge step is schema-enforced, not advisory.

A section update delta MUST include:

- `section_id`
- `body`
- `updated_by`
- `revision`: must equal `prior.revision + 1`
- `replaces_updated_at`: must equal the prior section's `updated_at`
- `justification`: free-text reason the prior body is being superseded

Merge rules:

- If the on-disk section's `updated_at` does not match `replaces_updated_at`, the delta is **rejected** (someone else wrote in the meantime — reflection must re-read and try again, or escalate the conflict).
- If the on-disk section is `protected=true` (Hermes or human authored) and the incoming delta has `updated_by=reflection`, the delta is **rejected unless** `justification` is non-empty AND the incoming `body` references the prior body explicitly (paraphrase, contradict-with-evidence, or extend). The validator does not judge prose quality, but it does require `references_prior_body: true` in the delta payload — reflection has to assert it actually read the prior text.
- Hermes/human writes always succeed (subject to revision/replaces_updated_at being current). They flip `protected` to true.
- Reflection writes preserve `protected` as it was. They never silently flip it to false.
- `revision` is the only authoritative ordering. `updated_at` is informational and may be inaccurate across clock skew.

This is the load-bearing rule of the whole design. Tests should cover: (a) stale `replaces_updated_at` rejected, (b) reflection overwrite of `protected` without justification rejected, (c) Hermes overwrite always wins, (d) two reflection writes interleaved correctly.

## Project Reflection Page

Each project collection page should become a project dashboard, not a concept index.

Suggested wiki path:

```text
wiki/proyectos/<project-id>/_index.md
```

Suggested sections:

- `Estrategia principal`
- `Estado del proyecto`
- `Trabajo en curso`
- `Objetivos principales`
- `Condiciones y restricciones`
- `Decisiones y requisitos`
- `Problemas, riesgos y bloqueos`
- `Información pendiente`
- `Próximo foco`
- `Materiales importantes`
- `Aprendizajes positivos`
- `Errores y acciones de reparación`
- `Historial reciente`

The page should be compiled deterministically from `project_state.json`, `sections.json`, project notes, and material evidence. LLM reflection should update state, notes, or section artifacts through validated deltas, not hand-edit arbitrary markdown.

## Reflection Stages

The nightly `lint --full` path should run project-specific reflection stages for `proyectos`.

### Stage 1: Material Intake Synthesis

Input:

- new or changed enriched materials in one project
- prior `project_state.json`
- current project page excerpt

Output:

- project state delta
- important new evidence
- possible stage change
- new decisions, constraints, blockers, and open items

### Stage 2: Project Status Reflection

Input:

- all compact project evidence
- existing project state
- recent Hermes/human updates
- current section artifacts

Output:

- current stage
- work in progress
- main objectives
- missing information
- next focus
- risks and blockers
- proposed section improvements when useful

Hermes notes, warnings, and explicit section edits should be treated as high-priority evidence. Reflection should not bury them inside generic synthesis; it should either incorporate them into the relevant state/section or explain why they remain unresolved.

### Stage 3: Project Learning Reflection

Input:

- project history
- mistakes, repairs, successes, positive patterns

Output:

- positive learnings useful for future projects
- mistakes or failure modes
- concrete repair actions for the current project
- reusable process improvements

### Stage 4: Office Learning Pass

Input:

- compact packets from multiple projects
- especially mistakes, repair actions, positive learnings, and stage transitions

Output:

- office-level process improvements
- recurring blockers
- reusable templates/checklists to consider
- warnings about systemic issues

Suggested artifact:

```text
derived/domains/proyectos/office_learning.json
wiki/proyectos/_office-learning.md
```

This replaces bridge-concept synthesis for `proyectos`.

## Hermes Editing Contract

Hermes should not edit compiled markdown directly. Hermes runs on the same device and can call local `arq` CLI commands, so no MCP tool surface is required for the first design. It should write structured project notes, state deltas, and section artifact updates, then Arquimedes compiles the page.

Suggested CLI commands:

Deterministic writes (no LLM, always available):

- `arq project status <project-id>` — read current state, including note ids for edit/delete operations
- `arq project update <project-id> --field next_focus --text ...` — replace a single state field
- `arq project append <project-id> --field risks_or_blockers --text ... --source hermes` — append to a list-valued state field
- `arq project resolve <project-id> --item <id> --note ...` — mark an open item resolved
- `arq project note <project-id> --kind decision|requirement|risk|deadline|coordination|learning|mistake|repair --text ...` — append a project note
- `arq project note-edit <project-id> <note-id> --text ...` — edit an existing project note and preserve update metadata
- `arq project note-delete <project-id> <note-id>` — soft-delete an existing project note
- `arq project section set <project-id> <section-id> --text ... --source hermes` — write/replace section body verbatim
- `arq project recompile <project-id>` — force a per-project compile after a batch of writes
- `arq project reflect <project-id>` — force one project-reflection pass for a specific project and then recompile

LLM-backed writes (require enrichment routes, may be unavailable in offline contexts):

- `arq project section improve <project-id> <section-id> --instruction ...` — calls the section-improve LLM stage. This is **not** a deterministic CLI write; it is a single-section reflection invocation that produces a section-update delta and routes it through the merge protocol above. It is ratelimited and obeys the same precedence rules as nightly reflection.

`arq project note`, `note-edit`, `note-delete`, `update`, `append`, `resolve`, `section set`, and `reflect` trigger an immediate per-project recompile by default so Hermes changes and reflection results appear on the page right away. `--no-recompile` is available for batched writes followed by a single `arq project recompile`.

Hermes needs a short handbook, not a protocol layer. The handbook should tell it:

- how to map Discord channels to project ids
- when to record a note instead of letting a conversation stay ephemeral
- which `arq project ...` command to use for decisions, risks, learnings, mistakes, repairs, and follow-up needs
- when to improve a whole project-page section rather than append another note
- how to include Discord channel/message provenance
- that compiled markdown is generated output and should not be edited directly

All writes should preserve provenance:

- actor
- timestamp
- source channel or Discord message id when available
- optional material id
- optional confidence

Precedence rule:

- Hermes/human notes and warnings are high-priority reflection evidence.
- Hermes/human section edits are page-source artifacts and should be rendered directly.
- Nightly reflection can refine or supersede Hermes-authored section text only by writing an explicit replacement with provenance, never by silently erasing it.
- If reflection disagrees with a Hermes warning, it should keep the warning visible as unresolved or mark it resolved with evidence.

## Discord And Hermes Integration

The Discord channel name can map to a project id when it matches a `Proyectos/<project-id>/` folder.

Discord should not be ingested as periodic channel exports in the first design. Hermes already has Discord access and can judge when something discussed in a project channel is worth preserving. The persistent instruction to Hermes should be:

- when a discussion records a project's governing strategy, decision, requirement, risk, deadline, coordination problem, useful learning, mistake, or repair action, add a project note with `arq project note`
- include the Discord channel and message reference when available
- prefer short, curated notes over dumping chat history
- do not duplicate information already captured in a source material unless the conversation changes its interpretation or priority

These notes should be first-class project evidence:

- stored in `derived/projects/<project-id>/notes.jsonl`
- rendered in the project page, especially in recent notes, decisions, risks, learnings, mistakes, and repair sections
- included in the next project reflection evidence packet
- preserved with provenance so reflection can distinguish a Hermes-curated Discord note from extracted source material
- treated as higher-priority evidence than ordinary low-confidence inferred material summaries

A later `discord_digest` material type can still be added if full channel history becomes valuable, but it should not be the default path.

## Search And Retrieval

Project search should default to collection scope.

Examples:

```bash
arq search --domain proyectos --collection 2407-casa-rio "licencia"
arq project search 2407-casa-rio "presupuesto estructura"
```

The web UI should make the project page feel like the natural entry point. Search across all Proyectos remains useful for office memory, but the everyday Hermes workflow should start from one project.

## Disabled Or Replaced Behaviors

For `proyectos`:

- skip `arq cluster` by default
- skip local concept cluster pages
- skip concept reflection
- skip bridge-concept synthesis
- keep material pages
- keep extraction, enrichment, figures, tables, annotations, full-text search
- replace collection reflection with project reflection
- replace bridge-concept reflection with office learning reflection

If a future project needs concept clustering, it should be an opt-in submode, not the default.

### Office learning reuses the bridge runner

Office learning is implemented as a **publication-mode variant** inside the existing `lint_global_bridge.py` runner, not a fresh module. Proyectos does not produce global bridge concepts or bridge pages; it only reuses the runner's orchestration machinery: incremental packet building, route handling, the "<2 collections, skip" guard, and prompt-version stamping. The variant point is the prompt module (`project_prompts.office_learning_*`) and the output artifact path (`derived/domains/proyectos/office_learning.json` instead of bridge tables). The dispatch is keyed off `DomainProfile.publication_mode`: `concept_graph` runs bridge-concept synthesis; `project_dossier` runs office-learning synthesis. A standalone `lint_office_learning.py` would duplicate ~80% of the runner.

## Configuration Surface

Extend domain profiles from prompt-only differences into publication behavior.

Profile fields:

- `domain`
- `output_language`
- `publication_mode`: `concept_graph` or `project_dossier`
- `run_local_clustering`
- `run_concept_reflection`
- `run_collection_reflection`
- `run_project_reflection`
- `run_global_bridge`
- `run_office_learning`
- `material_type_taxonomy`
- `generated_labels`

Helper API on `domain_profiles.py` (new, used by call sites instead of reading flags off the dataclass directly):

- `get_publication_mode(domain) -> str`
- `should_run_clustering(domain) -> bool`
- `should_run_concept_reflection(domain) -> bool`
- `should_run_collection_reflection(domain) -> bool`
- `should_run_project_reflection(domain) -> bool`
- `should_run_global_bridge(domain) -> bool`
- `should_run_office_learning(domain) -> bool`
- `is_proyectos_domain(domain) -> bool` (mirrors `is_practice_domain`)

Initial values:

| Domain | Publication mode | Local clustering | Collection/project reflection | Global pass |
|---|---|---:|---|---|
| `research` | `concept_graph` | yes | collection reflection | bridge concepts |
| `practice` | `concept_graph` | yes | collection reflection | bridge concepts |
| `proyectos` | `project_dossier` | no | project reflection | office learning |

## Resolved Decisions

1. Internal slug is `proyectos` (matches folder; consistency over English purity).
2. Canonical project id is the office-job-number-prefixed slug: `<job-number>-<slug>`, e.g. `2407-casa-rio`.
3. Stage taxonomy v1 is committed above (`lead → feasibility → schematic_design → basic_project → execution_project → tender → construction → handover → archived`).
4. Phase 1 ships specialized prompts only. Specialized parsers for budgets and drawings are deferred.
5. Hermes notes are committed to the vault by default with provenance. A `--private` flag on `arq project note` is reserved for a later phase if needed.
6. Hermes records a persistent note when a discussion captures: a project's governing strategy, decision, requirement, risk, deadline, coordination issue, learning, mistake, or repair action. The v1 `note.kind` enum is `strategy`, `decision`, `requirement`, `risk`, `deadline`, `coordination`, `learning`, `mistake`, `repair`. `strategy` is special: it is permanent high-priority evidence, lint must never archive/touch it, and only Hermes or humans may create/edit it. The handbook expands each trigger.
7. Section-edit precedence is fully specified by the section replacement protocol above.

## Open Questions

1. Should `arq project section improve` be available in Phase 6, or pushed to a later phase once nightly section reflection has stabilized?
2. Should `office_learning` require a minimum number of *recently-updated* projects, not just a minimum number of total projects, to avoid producing noise on a quiet week?
3. Should `proyectos/_general` files be auto-quarantined to a separate folder on disk, or only flagged?

## Success Criteria

- A `Proyectos/<project-id>/` folder is ingested as domain `proyectos`, collection `<project-id>`.
- Proyectos enrichment identifies material type and extracts project-operational evidence in Spanish.
- Proyectos collections produce project pages, not concept indexes.
- Clustering and bridge passes are skipped for Proyectos unless explicitly requested.
- Nightly reflection updates project state, next focus, missing information, risks, learnings, and repair actions.
- Hermes can update project state through structured CLI commands with provenance.
- Hermes-curated Discord notes appear on project pages and are included in later project reflection passes.
- Hermes can improve project-page section artifacts through CLI commands, and those section artifacts are rendered by compile.
- Reflection treats Hermes notes, warnings, and section edits as high-priority inputs rather than generic evidence.
- An office-learning pass can summarize recurring successes, mistakes, and process improvements across projects.
