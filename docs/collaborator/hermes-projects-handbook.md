# Hermes Project Memory Handbook

Use this guide when you are helping maintain `Proyectos/` project dossiers.

## Project Identity

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

## Commands

Read the current dossier:

```bash
arq project status <project-id>
```

Record a note:

```bash
arq project note <project-id> --kind decision --text "..." --source-ref "discord://channel/message"
```

Replace a state field:

```bash
arq project update <project-id> --field next_focus --text "..."
```

Append to a list-valued state field:

```bash
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

Batch writes without recompiling every time:

```bash
arq project note <project-id> --kind risk --text "..." --no-recompile
arq project section set <project-id> riesgos --text "..." --no-recompile
arq project recompile <project-id>
```

## Provenance

Include `--source-ref` whenever you can. Good source refs include Discord channel/message refs, material ids, file names, meeting dates, or authority references.

Use `--material-id` when the note is tied to one ingested material.

Use `--confidence` only when uncertainty matters.

## State Versus Sections

Use notes for atomic facts and decisions.

Use `update` or `append` when a structured state field changes, such as `next_focus`, `missing_information`, or `risks_or_blockers`.

Use `section set` when the project page needs a better synthesized paragraph or section, not just another bullet.

Never edit compiled markdown directly. Project pages are generated from state, notes, sections, and materials.

## Reflection

Human and Hermes notes are high-priority evidence for later project reflection. Reflection may refine project sections, but it must do so through the section replacement protocol with provenance. It should preserve, resolve, or explicitly challenge Hermes warnings with evidence.
