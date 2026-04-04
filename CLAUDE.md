# Arquimedes — Claude Code Instructions

## Project

Collaborative LLM knowledge base for architecture (building design) practice and research.

Raw materials live in a shared iCloud folder (`~/Arquimedes-Library`). The repo contains extracted artifacts, wiki, indexes, and tools. Python tooling via `arq` CLI (Click). SQLite FTS5 for search (gitignored, rebuilt locally).

## User

- Architect and architecture professor
- Works in both practice (design, regulations, materials) and research (academic papers, teaching)
- Visual materials are critical — architecture PDFs contain plans, sections, elevations, diagrams
- Collaborates with colleagues via shared iCloud folder; system must be easy for non-technical collaborators
- Values provenance and research integrity (regulations, teaching context)
- Prefers web browser as primary UI

## Key Principles

### Deterministic first, LLM only for ambiguity

Never use an LLM for something a deterministic program can do reliably. LLMs are "system 2" — slower, more expensive, but flexible. Code is "system 1" — fast, cheap, repeatable. Before assigning any task to the LLM enrichment stage, ask: "Can a deterministic program do this reliably?" If yes, build it as code. Only use the LLM for tasks that genuinely require language understanding or ambiguity resolution.

### Always keep spec and plan in sync

When implementation changes affect the spec or plan, update those documents immediately — never let them drift. The spec and plan are the source of truth for collaborators and other LLM agents picking up work. After any code change that alters the contract, update `docs/superpowers/specs/2026-04-04-arquimedes-knowledge-system-design.md` and `docs/PLAN.md` in the same batch of work.

## Key Files

| File | Purpose |
|------|---------|
| `docs/PLAN.md` | Implementation plan with phase checkboxes |
| `docs/superpowers/specs/2026-04-04-arquimedes-knowledge-system-design.md` | Full design spec |
| `config/config.yaml` | Default configuration |
| `src/arquimedes/cli.py` | `arq` CLI entrypoint |
