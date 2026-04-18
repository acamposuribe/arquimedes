# Arquimedes — Claude Code Instructions

> If you are an agent **investigating** the knowledge base (answering questions about its contents), stop here and read [docs/agent-handbook.md](docs/agent-handbook.md) instead. This file is for agents **modifying the Arquimedes code itself**.

## Project

Collaborative LLM knowledge base for architecture (building design) practice and research.

### Deterministic first, LLM only for ambiguity

Never use an LLM for something a deterministic program can do reliably. LLMs are "system 2" — slower, more expensive, but flexible. Code is "system 1" — fast, cheap, repeatable. Before assigning any task to the LLM enrichment stage, ask: "Can a deterministic program do this reliably?" If yes, build it as code. Only use the LLM for tasks that genuinely require language understanding or ambiguity resolution.

### Always keep spec and plan in sync

When implementation changes affect the spec or plan, update those documents immediately — never let them drift. The spec and plan are the source of truth for collaborators and other LLM agents picking up work. 