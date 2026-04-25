# Phase 7: Agent Tools — Implementation Plan

> **Status:** Proposed
> **Date:** 2026-04-16
> **Spec:** [Phase 7 agent tools design](../specs/2026-04-16-phase7-agent-tools-design.md)

## Goal

Make the `arq` CLI the complete token-efficient usage surface for collaborator-side agents, backed by an in-repo agent handbook that teaches investigation flow and the wiki's mental model.

Phase 7 does not add semantic layers, does not run LLMs, and does not mutate knowledge-base artifacts. It wraps existing deterministic helpers (`search.py`, `read.py`, `freshness.py`, `index.py`) into a small, disciplined set of read commands plus an orientation command, a refresh command, and a handbook.

## Prerequisite (landed 2026-04-16)

Phase 7 commands are wrappers over `search.py`. Before wrapping, search itself had to cover every reflection layer. Two gaps are now closed:

- ✅ Step 2 global bridge clusters (bridge_id, canonical_name, aliases, descriptor, wiki_path) — FTS-indexed in `global_bridge_clusters_fts` and surfaced as `SearchResult.global_bridges`
- ✅ Bridge reflection prose (`why_this_bridge_matters`, `bridge_takeaways`, `bridge_tensions`, `bridge_open_questions`, `helpful_new_sources`) — stored in `global_bridge_clusters` and covered by both FTS and a LIKE fallback

Shipped:

- `global_bridge_clusters` table + `global_bridge_clusters_fts` in `memory.py`
- population from `derived/global_bridge_clusters.jsonl` during `arq memory rebuild`
- `GlobalBridgeHit` dataclass + `_search_global_bridges` helper in `search.py`
- `global_bridges` field on `SearchResult` populated alongside `collection_pages` and `canonical_clusters`
- tests in `TestGlobalBridgeSearch` covering FTS name matches and LIKE fallback over bridge reflection prose
- retired legacy `concept_clusters_fts` path removed from active search coverage

Slice 7.1 can now proceed.

## Current Implementation Snapshot

Already in place:

- `arq search`, `arq related`, `arq material-clusters`, `arq collection-clusters`, `arq concepts` — JSON-by-default, `--human` opt-in, `ensure_index_and_memory()` already called
- `arq index ensure` — cheap staleness-aware rebuild
- `src/arquimedes/search.py` — library functions consumed by CLI
- `src/arquimedes/read.py` (Phase 8) — wiki path, material artifact, figure, text helpers
- `src/arquimedes/freshness.py` (Phase 8) — `freshness.refresh()` (pull-if-applicable + ensure)
- `src/arquimedes/index.py` — `ensure_index_and_memory()`

Stubs / missing:

- `arq read` is a stub at [cli.py:317](../../../src/arquimedes/cli.py#L317)
- `arq figures` is a stub at [cli.py:324](../../../src/arquimedes/cli.py#L324)
- no `arq annotations`
- no `arq overview`
- no `arq refresh`
- no transparent `ensure` guard shared across agent-facing commands
- no `ARQ_SKIP_FRESHNESS` support
- no `docs/agent-handbook.md`
- no pointer from `CLAUDE.md` or `docs/developer/PLAN.md` to a handbook

## File Map

| # | File | Action | Responsibility |
|---|------|--------|---------------|
| 1 | `src/arquimedes/agent_cli.py` | Create | Shared helpers for agent-facing commands: `ensure_guard()` decorator reading `ARQ_SKIP_FRESHNESS`; JSON/human dual-output helper; error-formatting helper |
| 2 | `src/arquimedes/read.py` | Modify | Extend with the thin accessors Phase 7 commands need if not already present: card model (`build_material_card`), chunk-by-id, annotations-by-material, compact chunk / figure / annotation indexes, corpus snapshot |
| 3 | `src/arquimedes/cli.py` | Modify | Replace `arq read` and `arq figures` stubs; add `arq annotations`, `arq overview`, `arq refresh`; wire all five through `ensure_guard` |
| 4 | `docs/agent-handbook.md` | Create | Collaborator-agent mental model, investigation recipe, token hygiene, freshness guidance, quick-reference table, explicit maintainer-only command list |
| 5 | `CLAUDE.md` | Modify | Add short pointer section: "If you are investigating the knowledge base, read `docs/agent-handbook.md` first; this file is for agents modifying the Arquimedes code itself" |
| 6 | `docs/developer/PLAN.md` | Modify | Phase 7 bullet list replaced; agent handbook added to supporting-docs table; Phase 7 verification bullets appended |
| 7 | `tests/test_agent_cli.py` | Create | Unit tests for `ensure_guard`, JSON/human output, error formatting |
| 8 | `tests/test_read_command.py` | Create | `arq read` behavior across `--page`, `--chunk`, `--full`, `--detail` |
| 9 | `tests/test_figures_command.py` | Create | `arq figures` default list, `--visual-type`, `--figure` |
| 10 | `tests/test_annotations_command.py` | Create | `arq annotations` default list, `--page`, `--type` |
| 11 | `tests/test_overview_command.py` | Create | `arq overview` snapshot shape against a fixture corpus |
| 12 | `tests/test_refresh_command.py` | Create | `arq refresh` dispatches to `freshness.refresh()` and returns structured status |
| 13 | `tests/test_agent_handbook.py` | Create | Handbook: existence, link-check against current wiki paths, command names referenced match real Click commands |

## Dependency Order

```text
agent_cli.py + tests ──┐
                       ├──► cli.py changes (new + replaced commands)
read.py extensions ────┘

cli.py changes ──► per-command tests (read/figures/annotations/overview/refresh)

agent-handbook.md ──► handbook tests ──► CLAUDE.md + PLAN.md pointer updates
```

`agent_cli.py` and the `read.py` extensions are independent and should land first (parallelizable). CLI wiring depends on both.

The handbook is independent of code changes but its link-check test depends on commands and wiki paths being current, so land it after the CLI changes.

## Key Design Decisions

### Shared `ensure_guard` helper

A single decorator in `agent_cli.py` wraps each new agent-facing command:

- reads `ARQ_SKIP_FRESHNESS`; if truthy, skip
- otherwise call `ensure_index_and_memory()`
- propagate `FileNotFoundError` as a `click.ClickException` with a helpful message
- never swallow other exceptions

This keeps the freshness contract in one place and avoids repeating the same four lines in every command.

`arq refresh` does **not** go through `ensure_guard` because it does its own (heavier) freshness work.

### Output convention

Every new command follows the existing convention:

- default: JSON on stdout via `json.dumps(..., ensure_ascii=False, indent=2)`
- `--human`: human-readable rendering through a per-command formatter
- exit non-zero on unambiguous error (missing material id, bad args)
- exit zero with an empty-but-valid JSON result when the query is valid but matches nothing

### `arq read` layered flags

Flags on `arq read` are ordered by increasing read cost:

1. (no flag) → card, from `meta.json` + `extracted/<id>/` counts
2. `--detail <aspect>` → card plus compact index for one aspect
3. `--chunk <chunk_id>` → one chunk's text + metadata
4. `--page N` → one page's text + metadata
5. `--full` → full `text.md` (documented as the heaviest option)

`--detail` is mutually exclusive with `--chunk`, `--page`, `--full`. Validated in Click; emits `click.UsageError` on conflict.

### `arq overview` reads live state

Not cached. Queries SQLite for counts and reads stamp files under `derived/` for freshness flags. Cost is bounded: a handful of `SELECT COUNT(*)` queries plus small JSON reads.

### `arq refresh` is a thin wrapper

It imports `freshness.refresh()` (Phase 8), calls it, and emits the returned status object as JSON (default) or a short `--human` summary. No new logic.

### Handbook is authoritative, not enforced in code

No runtime guard prevents a collaborator agent from calling `arq cluster` or `arq compile`. The handbook states clearly which commands are maintainer-owned and explains the divergence risk. Enforcement is social/documentation, not technical. This matches the Phase 5 wiki-ownership model, which also relies on documented ownership rather than permission gates.

### Tests use fixture corpora, not the real library

`tests/conftest.py` already provides small corpora for existing CLI tests (see Phase 4 / Phase 8 tests). Phase 7 tests reuse the same pattern: a minimal `extracted/<id>/` tree and a built SQLite index under `tmp_path`.

## Implementation Slices

### Slice 7.1 — shared agent CLI helpers

Ships:

- `src/arquimedes/agent_cli.py` with `ensure_guard`, JSON/human dispatcher, error formatter
- `tests/test_agent_cli.py`
- optional: refactor one existing command (e.g., `arq related`) to route through `ensure_guard` as a reference application, only if the diff stays small

Done when tests pass and `ensure_guard` can be imported from `arquimedes.agent_cli`.

### Slice 7.2 — read accessors in `read.py`

Ships:

- `build_material_card(material_id) -> dict` with `counts`, `wiki_path`, `source_path`
- `get_chunk_by_id(material_id, chunk_id) -> dict`
- `list_chunks_compact(material_id) -> list[dict]`
- `list_figures_compact(material_id, visual_type: str | None) -> list[dict]`
- `get_figure(material_id, figure_id) -> dict`
- `list_annotations(material_id, page: int | None, kind: str | None) -> list[dict]`
- `build_corpus_overview() -> dict`
- extension of existing tests in `tests/test_read.py` to cover the new helpers

Done when the new helpers return the documented shapes and existing `test_read.py` still passes.

### Slice 7.3 — replace `arq read` and `arq figures` stubs

Ships:

- `arq read` replaced with full Click command using `--page`, `--chunk`, `--full`, `--detail`, `--human`; mutual-exclusion validation
- `arq figures` replaced with Click command using `--visual-type`, `--figure`, `--human`
- both wrapped with `ensure_guard`
- `tests/test_read_command.py`, `tests/test_figures_command.py`

Done when tests pass and `arq read --help` / `arq figures --help` show the documented flags.

### Slice 7.4 — `arq annotations` and `arq overview`

Ships:

- `arq annotations <material_id>` with `--page`, `--type`, `--human`
- `arq overview` (no args) with `--human`
- both wrapped with `ensure_guard`
- `tests/test_annotations_command.py`, `tests/test_overview_command.py`

Done when tests pass and both commands appear in `arq --help`.

### Slice 7.5 — `arq refresh`

Ships:

- `arq refresh` wiring to `freshness.refresh()`
- JSON default / `--human` summary
- `tests/test_refresh_command.py` (mocks `freshness.refresh` for determinism)

Done when tests pass.

### Slice 7.6 — agent handbook (minimal)

Ships:

- `docs/agent-handbook.md` containing only: mental model (2-3 lines), path tree, investigation recipe, command quick-reference table, one-line maintainer-only warning. No introduction prose, no examples, no long token-hygiene essay.
- pointer section in `CLAUDE.md`
- supporting-docs row in `docs/developer/PLAN.md`
- `tests/test_agent_handbook.py` verifying:
  - file exists
  - referenced wiki paths resolve to real directories/globs
  - command names referenced exist in `cli.py`
  - body length stays **under ~800 tokens** (hard cap, not aspirational)

Done when tests pass and the handbook's token budget is under the cap. If the handbook cannot teach the investigation flow under the cap, the fix is to make `--help` richer or the command defaults smarter, not to lengthen the handbook.

### Slice 7.7 — PLAN.md update

Ships:

- Phase 7 section rewritten to match the new scope (no MCP, no `arq read`/`arq figures` as separate bullets — subsumed by command-surface bullet)
- verification checklist gains Phase 7 rows
- Supporting docs table gains handbook and Phase 7 spec/plan rows
- Status line updated if Phase 7 is the new active phase

## Testing Strategy

- unit tests for every new `read.py` accessor against fixture corpora
- command-level tests using `CliRunner` from Click
- handbook test asserts living references (command names, wiki paths) match reality at the time of each commit
- `ARQ_SKIP_FRESHNESS=1` tested explicitly: patched `ensure_index_and_memory` must not be called when the flag is set, and must be called when it is unset

Tests do not invoke any LLM route and do not hit the network.

## Cross-cutting Concerns

### Path safety

`arq read`, `arq figures`, and `arq annotations` accept a `material_id` argument only; they never accept raw filesystem paths. All path resolution goes through `read.py` helpers that stay inside `extracted/` and `wiki/`.

### Error messages

Missing material / page / chunk / figure must produce a single-line error naming what was not found and, where useful, suggesting the next command. Example:

```
Error: material "0012ab" has no chunk "xy99".
Try: arq read 0012ab --detail chunks
```

### Backwards compatibility

The existing `arq read` and `arq figures` stubs return a hard-coded "not yet implemented" string. Replacing them is a net improvement with no downstream consumers to break.

No other existing command changes behavior in Phase 7.

### Documentation

The spec is the canonical design reference. `--help` strings on every new command link to the handbook path (`docs/agent-handbook.md`).

## Exit Criteria

Phase 7 is complete when:

- [ ] Slice 7.1 (agent_cli helpers) merged with tests green
- [ ] Slice 7.2 (read accessors) merged with tests green
- [ ] Slice 7.3 (`arq read`, `arq figures`) merged with tests green
- [ ] Slice 7.4 (`arq annotations`, `arq overview`) merged with tests green
- [ ] Slice 7.5 (`arq refresh`) merged with tests green
- [ ] Slice 7.6 (handbook + pointers) merged with tests green
- [ ] Slice 7.7 (PLAN.md refresh) merged
- [ ] Manual verification: a fresh agent session that reads `docs/agent-handbook.md` and is asked an investigation question can answer it using only the documented agent-facing commands, without touching any maintainer command
- [ ] No mutation commands were added to the agent surface

## Out of Scope (deferred)

- MCP server wrapping the same library code — may be added later as a thin adapter if demand appears
- `arq watch` / `arq sync` behavioral changes — Phase 9
- freshness daemon behavior — Phase 9
- maintainer instruction file for the server agent — Phase 9
- any UI changes — Phase 8 owns the web surface
