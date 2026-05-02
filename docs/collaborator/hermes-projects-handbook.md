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
4. **Update sections when necessary.** Use section edits for curated prose that should appear directly in the generated project page.
5. **Force reflection only when requested.** If the human asks to re-run/refresh/force synthesis, run project reflection.

## Core Project Commands

| Task | Command |
| --- | --- |
| List projects | `arq project list` |
| Overview with material counts | `arq overview --domain proyectos --human` |
| Read state, sections, and open notes | `arq project status <project-id>` |
| Search one project dossier | `arq project search <project-id> "licencia"` |
| Add a note | `arq project note <project-id> --kind decision --text "..." --source-ref "discord://channel/message"` |
| Edit a note by `note_id` | `arq project note-edit <project-id> note-0001 --text "..."` |
| Delete a note by `note_id` | `arq project note-delete <project-id> note-0001` |
| Resolve an open item | `arq project resolve <project-id> --item missing_information:1 --note "..."` |
| Replace a page section | `arq project section set <project-id> proximo_foco --text "..."` |
| Force reflection when requested | `arq project reflect <project-id>` |

Batch writes with `--no-recompile`, then run `arq project recompile <project-id>` once.

Direct structured-state commands are admin escape hatches; use only when the human directly asks: `arq project update ...`, `arq project append ...`.

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


## Vault Root vs Library Root

Arquimedes uses two different roots. Do not confuse them.

- **Vault root**: the local Git checkout that contains Arquimedes' generated/indexed knowledge files and config, such as `config/`, `wiki/`, `derived/`, `extracted/`, and docs. This is where `arq` commands normally run. Do not drop source PDFs or project files here.
- **Library root**: the shared source-material folder that Arquimedes scans for ingest. This is where humans' PDFs, images, notes, meeting reports, and project source files belong. Project files usually go under `Proyectos/<project-id>/...` inside this root.

To find the active roots, run:

```bash
arq vault info
```

Use the `library_root:` line for file placement and symlink targets inside Arquimedes. Use the `vault_root:` line only for running `arq` commands or understanding which vault checkout is active.

**Non-negotiable boundary:** Hermes must never directly create, edit, move, delete, or overwrite files under the **vault root**. Vault-root files include configuration, generated wiki/derived/extracted data, indexes, docs, and any other files in the Git checkout. Direct filesystem edits in the vault root are prohibited and out of bounds. If vault state or vault info must change, use the appropriate `arq` command only, or ask the human/maintainer.

Safety rules:

- Before writing or linking files, always resolve the current `library_root` with `arq vault info --human`.
- Do not infer the library root from the current working directory.
- Never write to `vault_root` directly. This remains prohibited even for small fixes, config tweaks, markdown edits, or cleanup.
- Vault info/config/state may only be changed through `arq` commands. If no command exists, ask the human/maintainer instead of editing files.

## Files From Humans

If a human wants Hermes to add meeting reports, PDFs, images, notes, or other project files to Arquimedes:

- Resolve `library_root` with `arq vault info --human`.
- Put files under `library_root/Proyectos/<project-id>/...`.
- Never put source files in `vault_root` or in the office server/NAS folder.
- Treat server/NAS folders as read-only. If they must be ingested, symlink them from the library project folder instead of copying.

## Linking Server/NAS Folders for Ingest

When the human asks to "get", "link", "mount", "alias", or "make an alias" for an office server/NAS folder, create a Unix symlinked folder inside the Arquimedes library root. First resolve `ARQ_LIBRARY_ROOT` from `arq vault info --human` (`library_root:`), then use:

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
2. Resolve `library_root` with `arq vault info --human`; never use `vault_root`.
3. Confirm the server folder is readable and the link path is inside `$ARQ_LIBRARY_ROOT/Proyectos/<project-id>/`.
4. Confirm the link name does not already exist.
5. Verify with:

```bash
ls -la "$ARQ_LIBRARY_ROOT/Proyectos/<project-id>"
```

You should see:

```text
server-entregas -> /Volumes/Server/Clientes/Casa Rio/Entregas
```