# Hermes Project Memory Handbook

Use this guide when you are helping maintain `Proyectos/` project dossiers.

## Project Identity

Always start by listing the available Proyectos dossiers before guessing a project id.

Get the full project list with either:

```bash
arq project list
```

or the domain-filtered overview:

```bash
arq overview --domain proyectos --human
```

Use `arq project list` when you just need the ids. Use `arq overview --domain proyectos` when you also want material counts.

A Discord/project channel maps to a project when its name or context matches a folder under:

```text
Proyectos/<project-id>/
```

Use the folder slug as the project id in CLI commands, for example:

```bash
arq project status 2407-casa-rio
```

`Proyectos/_general` is an intake bucket, not a project. Ask the maintainer to move loose files into a real project folder before recording project memory for them.

## What To Preserve

Add a project note when a conversation records one of these:

- a decision
- a client, authority, technical, or contractual requirement
- a risk, blocker, contradiction, or delay
- a deadline or dated commitment
- a coordination issue
- a useful learning
- a mistake or regret
- a repair action

Prefer short, curated notes over dumping chat history. Do not duplicate a source material unless the conversation changes its interpretation, urgency, or priority.

## Linking External Project Folders For Ingest

When a human asks you to "get", "link", "mount", "alias", or "make an alias" for a folder from the office server/NAS into a Proyectos dossier, interpret that as: **create a Unix symlinked folder inside the library root**, not a Finder alias and not a copy.

The purpose is to let Arquimedes ingest source files that physically live elsewhere without duplicating them.

Use this pattern:

```bash
ln -s "<real-server-folder>" "<library-root>/Proyectos/<project-id>/<link-name>"
```

Example:

```bash
ln -s "/Volumes/Server/Clientes/Casa Rio/Entregas" \
  "$ARQ_LIBRARY_ROOT/Proyectos/2407-casa-rio/server-entregas"
```

Rules:

- Create a **symlink** with `ln -s`. Do not create a macOS Finder alias.
- Do **not** copy the server folder into the library.
- Put the symlink under the correct project folder: `Proyectos/<project-id>/...`.
- Use a short, stable link name such as `server-docs`, `server-entregas`, `cliente`, or `consultores`.
- If the target link path already exists, stop and ask before replacing it.
- Verify after creating it:

```bash
ls -la "$ARQ_LIBRARY_ROOT/Proyectos/<project-id>"
```

You should see a row like:

```text
server-entregas -> /Volumes/Server/Clientes/Casa Rio/Entregas
```

Safety checks before creating the symlink:

1. Confirm the project id with `arq project list`.
2. Confirm the real server folder exists and is readable.
3. Confirm the symlink will be created inside the library root, under `Proyectos/<project-id>/`.
4. Confirm the link name does not already exist.

Arquimedes ingest follows symlinked directories. Files inside the linked server folder are picked up as if they lived under `Proyectos/<project-id>/<link-name>/`, while the bytes remain stored in the original server location.

## Commands

List available projects:

```bash
arq project list
```

Read the current dossier:

```bash
arq project status <project-id>
```

Force a fresh project reflection when a human explicitly asks to re-run synthesis for that project:

```bash
arq project reflect <project-id>
```

Record a note:

```bash
arq project note <project-id> --kind decision --text "..." --source-ref "discord://channel/message"
```

Avoid direct state mutation in normal Hermes workflows. Prefer notes first, then optional section edits, and let project reflection/lint reconcile canonical `project_state` on the next pass.

Direct state commands are maintainer/admin escape hatches, not the default Hermes path:

```bash
arq project update <project-id> --field next_focus --text "..."
arq project append <project-id> --field risks_or_blockers --text "..."
```

Resolve an open item:

```bash
arq project resolve <project-id> --item missing_information:1 --note "..."
```

You can also resolve by exact item text:

```bash
arq project resolve <project-id> --item "Confirmar acometida" --note "Confirmada por ingeniería."
```

Replace a whole page section:

```bash
arq project section set <project-id> proximo_foco --text "..."
```

Search within one project:

```bash
arq project search <project-id> "licencia"
```

If you do not yet know the exact project id, list projects first instead of searching the whole corpus blindly.

Batch writes without recompiling every time:

```bash
arq project note <project-id> --kind risk --text "..." --no-recompile
arq project section set <project-id> riesgos --text "..." --no-recompile
arq project recompile <project-id>
```

If a human asks Hermes to "re-run", "refresh", or "force" the project synthesis after new notes or section edits, use:

```bash
arq project reflect <project-id>
```

That command forces one LLM-backed reflection for the specific project even if the incremental lint logic would otherwise skip it.

## Provenance

Include `--source-ref` whenever you can. Good source refs include Discord channel/message refs, material ids, file names, meeting dates, or authority references.

Use `--material-id` when the note is tied to one ingested material.

Use `--confidence` only when uncertainty matters.

## State Versus Sections

Use notes for atomic facts, decisions, corrections, contradictions, and new evidence.

Use `section set` when the project page needs a better synthesized paragraph or section, not just another bullet.

Treat `project_state.json` as canonical structured memory maintained mostly by reflection/lint. Hermes should usually influence it indirectly through notes and curated sections, not by replacing fields directly.

If a human needs a narrow manual correction in the structured data, prefer editing or deleting a single item from the web UI structured-data block rather than rewriting whole fields.

Never edit compiled markdown directly. Project pages are generated from state, notes, sections, and materials.

## Reflection

Human and Hermes notes are high-priority evidence for later project reflection. Reflection may refine project sections, but it must do so through the section replacement protocol with provenance. It should preserve, resolve, or explicitly challenge Hermes warnings with evidence.

Open notes have the highest weight when they contradict older inferred conclusions. When notes add non-overlapping information, reflection should merge them additively instead of deleting unrelated prior state. After successful incorporation, notes move out of the open queue into archived statuses such as `incorporated` or `superseded`.
