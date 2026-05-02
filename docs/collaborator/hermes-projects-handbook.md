---
name: arquimedes-proyectos
description: Use when Hermes helps with Arquimedes Proyectos project memory: identifying a project from Discord context, reviewing status and open notes, adding project notes from conversation or files, updating generated-page sections, linking read-only server folders by symlink, adding files to the library root, or forcing project reflection when explicitly requested.
---

# Arquimedes Proyectos

Use this skill when Hermes is helping with `Proyectos/` project memory in Arquimedes.

## What Proyectos Is

`Proyectos/` is Arquimedes' project-memory domain for architecture practice. Each folder is one project dossier. It combines ingested materials, project notes, curated section text, and structured `project_state` into a generated project page.

Hermes' default job is to preserve useful project memory from conversation and files, not to rewrite the whole dossier.

## Priority Order

When trying to understand where a project stands, prioritize evidence in this order:

1. Explicit human instructions in the current conversation.
2. **Open project notes/comments** from Hermes or humans.
3. Current project status/page sections.
4. Structured state.
5. Source materials found through search/read.

Open notes are high-priority unresolved evidence. If they contradict older conclusions, trust the open notes unless newer evidence says otherwise.

## Normal Workflow

0. **Initialize/link folders only if requested.** If the human asks to link a server/NAS folder, create a Unix symlink inside the library project folder. Do not copy server files.
1. **Identify the project.** Infer it from Discord channel/context only when clear; otherwise ask the human. Confirm against `arq project list`.
2. **Find and review the dossier.** Use `arq project status <project-id>` and read open notes first.
3. **Add notes for new facts.** Record decisions, requirements, risks, deadlines, coordination issues, lessons, mistakes, repair actions, or useful meeting/file summaries.
4. **Update sections only when necessary.** Use section edits for curated prose that should appear directly in the generated project page.
5. **Update structured state only when explicitly requested.** Direct state commands are admin escape hatches; prefer notes and sections.
6. **Force reflection only when requested.** If the human asks to re-run/refresh/force synthesis, run project reflection.

## Core Project Commands

List projects:

```bash
arq project list
```

Overview with material counts:

```bash
arq overview --domain proyectos --human
```

Read current state, sections, and open notes:

```bash
arq project status <project-id>
```

Search within one project dossier:

```bash
arq project search <project-id> "licencia"
```

Add a note with one of the allowed kinds (`decision`, `requirement`, `risk`, `deadline`, `coordination`, `learning`, `mistake`, `repair`):

```bash
arq project note <project-id> --kind decision --text "..." --source-ref "discord://channel/message"
```

Resolve an open item:

```bash
arq project resolve <project-id> --item missing_information:1 --note "..."
```

Replace a generated-page section:

```bash
arq project section set <project-id> proximo_foco --text "..."
```

Force reflection when explicitly requested:

```bash
arq project reflect <project-id>
```

Batch writes without recompiling every time:

```bash
arq project note <project-id> --kind risk --text "..." --no-recompile
arq project section set <project-id> riesgos --text "..." --no-recompile
arq project recompile <project-id>
```

Direct structured-state commands: use only if the human directly asks for this kind of change.

```bash
arq project update <project-id> --field next_focus --text "..."
arq project append <project-id> --field risks_or_blockers --text "..."
```

## Corpus Tools for Project Materials

Use these when project status is not enough and you need source evidence.

- `search`: find materials, passages, figures, annotations, clusters, and bridges.
- `read`: drill into one material; default card first, then `--detail chunks|figures|annotations`, `--page`, `--chunk`, or `--full` only when needed.
- `figures`: list or inspect figures for one material.
- `annotations`: list reader annotations for one material.

CLI equivalents:

```bash
arq search --deep --facet domain=proyectos "consulta estructura"
arq read <material-id> --human
arq read <material-id> --detail annotations --human
arq figures <material-id> --human
arq annotations <material-id> --human
```

Prefer `arq project search <project-id> ...` once the project is known. Use broad `arq search` only when the project is unknown or cross-project context is needed.

## What To Preserve as Notes

Add a short curated note when a conversation or uploaded file records one of these allowed note kinds:

- `decision`: a choice, approval, rejection, design direction, or agreed action.
- `requirement`: a client, authority, technical, contractual, budget, or performance requirement.
- `risk`: a risk, blocker, contradiction, dependency, uncertainty, delay, or potential problem.
- `deadline`: a dated commitment, milestone, submission date, meeting date, or time-sensitive task.
- `coordination`: a coordination issue between people, disciplines, consultants, client, authority, or contractor.
- `learning`: a useful lesson, pattern, insight, precedent, or reusable project knowledge.
- `mistake`: an error, regret, wrong assumption, failed path, or thing to avoid repeating.
- `repair`: a corrective action, mitigation, recovery plan, resolution, or follow-up fix.

Do not dump chat history. Do not duplicate a source material unless the conversation changes its interpretation, urgency, or priority.

Include provenance whenever possible:

- `--source-ref` for Discord refs, meeting dates, filenames, authority references, or material ids.
- `--material-id` when the note is tied to one ingested material.
- `--confidence` only when uncertainty matters.

## Files From Humans

If a human wants Hermes to add meeting reports, PDFs, images, notes, or other project files to Arquimedes:

- Put the files under the **Arquimedes library root**, normally inside `Proyectos/<project-id>/...`.
- Do **not** put new files in the office server/NAS folder.
- Treat server/NAS folders as read-only sources.
- If the human wants Arquimedes to ingest a server folder, create a symlink from the library project folder to that server folder instead of copying it.

## Linking Server/NAS Folders for Ingest

When the human asks to "get", "link", "mount", "alias", or "make an alias" for an office server/NAS folder, interpret it as: create a Unix symlinked folder inside the Arquimedes library root.

Pattern:

```bash
ln -s "<real-server-folder>" "$ARQ_LIBRARY_ROOT/Proyectos/<project-id>/<link-name>"
```

Example:

```bash
ln -s "/Volumes/Server/Clientes/Casa Rio/Entregas" \
  "$ARQ_LIBRARY_ROOT/Proyectos/2407-casa-rio/server-entregas"
```

Rules:

- Use `ln -s`; do not create a macOS Finder alias.
- Do not copy the server folder.
- Do not write new files into the server folder.
- Put the symlink under `Proyectos/<project-id>/`.
- Use a short stable link name, e.g. `server-docs`, `server-entregas`, `cliente`, `consultores`.
- If the target link path already exists, stop and ask before replacing it.

Safety checks:

1. Confirm project id with `arq project list`.
2. Confirm the real server folder exists and is readable.
3. Confirm the symlink will be created inside `$ARQ_LIBRARY_ROOT/Proyectos/<project-id>/`.
4. Confirm the link name does not already exist.
5. Verify with:

```bash
ls -la "$ARQ_LIBRARY_ROOT/Proyectos/<project-id>"
```

You should see:

```text
server-entregas -> /Volumes/Server/Clientes/Casa Rio/Entregas
```

## Notes vs Sections vs State

- **Notes**: atomic facts, decisions, requests, corrections, contradictions, and new evidence. This is Hermes' default write path.
- **Sections**: curated prose for generated project pages when a note is not enough.
- **Structured state**: canonical fields maintained mostly by reflection/lint. Change directly only when explicitly requested by the human.

Never edit compiled markdown directly. Project pages are generated from state, notes, sections, and materials.

## Reflection

Reflection may synthesize open notes into sections/state. It should preserve, resolve, or explicitly challenge Hermes warnings with evidence.

Run reflection only when explicitly requested:

```bash
arq project reflect <project-id>
```

After successful incorporation, notes move automatically out of the open queue into archived statuses such as `incorporated` or `superseded`.
