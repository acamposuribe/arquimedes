# Phase 9 — Server Agent + Sync (Implementation Plan)

> **Status:** Implemented in code/tests (2026-04-25); macOS launchd/iCloud end-to-end operator verification remains
> **Spec:** [Phase 9 design spec](../specs/2026-04-25-phase9-server-agent-design.md)
> **References:** [`docs/developer/PIPELINE.md`](../../PIPELINE.md), [`docs/developer/PLAN.md` Phase 9 block](../../PLAN.md#phase-9-server-agent--sync), [Full design spec](../specs/2026-04-04-arquimedes-knowledge-system-design.md)

## Goal

Stand up `arq watch` (server-side 30-minute scan daemon), `arq sync` (collaborator daemon), launchd installers for both plus a separate daily 02:00 `arq lint --full` job, an automatic + reversible removal cascade, and `docs/maintainer/MAINTAINER.md` — turning the maintainer pipeline defined in `docs/developer/PIPELINE.md` into an always-on system.

## Risks and open questions (resolve early)

Resolved implementation notes:

1. **Daytime pipeline shape** — implemented as `ingest -> extract -> index rebuild -> compile`; reflective/global-bridge work remains nightly lint-only.
2. **Nightly lint publication semantics** — `arq lint --install-full` installs a `lint --full --commit-push` payload.
3. **macOS Full Disk Access** — left as an operator prerequisite in `docs/maintainer/MAINTAINER.md`.
4. **Lint-full CLI shape** — implemented as `arq lint --install-full`.
5. **Compile re-entrancy/removal safety** — covered by `removal.cascade_delete` unit tests plus existing compile behavior; real iCloud removal cycle remains operator verification.

## Pre-implementation reading

Before any code changes, the implementer reviews:

- `src/arquimedes/compile.py` — for the cluster + memory rebuild auto-trigger question above
- `src/arquimedes/freshness.py` — `arq sync` reuses `ensure_index_and_memory()` here
- `src/arquimedes/cli.py` — Click command patterns for the new daemon commands
- `src/arquimedes/agent_cli.py` — JSON / human dispatch convention
- existing `logs/` writers in enrich/cluster/lint — new `watch.log` and `sync.log` should follow the same `START`/`DONE`/`FAILED` convention

## Implementation tasks

### 4.1 Configuration

Add the new config keys defined in the spec to:

- `config/config.yaml` — shared defaults only
- `config/maintainer/config.yaml` — maintainer extraction/enrichment/LLM/daemon profile
- `src/arquimedes/config.py` — schema + validation

New keys:

```yaml
config/config.yaml:
  sync:
    pull_interval: 300
    auto_start: false
    auto_index: true
    reset_tracked: true

config/maintainer/config.yaml:
  extraction:
    chunk_size: 500
  enrichment:
    prompt_version: "enrich-v1.0"
    llm_routes: [...]
watch:
  scan_interval_minutes: 30
  batch_commit: true
  enrich_retries: 1
  commit_message_template: "..."

sync:
  pull_interval: 300
  auto_start: false
  auto_index: true
  reset_tracked: true

lint_full:
  schedule_cron: "0 2 * * *"
```

### 4.2 `src/arquimedes/watch.py`

New module. Components:

- `LibraryScanner` — walks `LIBRARY_ROOT` on a fixed cadence and computes the current file snapshot.
- `ScanDiff` / `BatchPlanner` — compares the current snapshot with manifest/library state and classifies `add_or_modify`, `move`, and `delete` work for one cycle. Pure logic, easy to unit-test.
- `BatchPipeline.run(batch)` — per-cycle dispatcher:
  - calls `arq ingest`, `arq extract` (with retry-once via 4.6 helper), `arq index rebuild`, `arq compile`
  - if pre-implementation verification shows compile does not auto-cluster stale collections, inserts `arq cluster` immediately before compile
  - on delete list, delegates to `removal.cascade_delete(material_ids)` (4.6) before commit
  - composes commit message from `watch.commit_message_template`
  - runs `git add -A && git commit -m … && git push`; push failures are logged but do not roll back the local commit
- `WatchDaemon` — top-level `start()` runs one scan every `watch.scan_interval_minutes`, hands non-empty batches to the pipeline, and exposes a graceful `stop()` for tests.

Each step writes structured `START` / `DONE` / `FAILED` records to `~/.arquimedes/watch.log` and a per-batch `logs/watch-<ts>.log`.

### 4.3 `src/arquimedes/sync.py`

New module. Behaviors:

- `SyncCycle.run()` — one fetch + canonical reset + ensure pass:
  1. `git fetch origin`
  2. compare `HEAD` to `origin/main` and inspect local worktree status
  3. capture current `HEAD` for the log line
  4. `git reset --hard origin/main` after every successful fetch, including when only local tracked edits exist
  5. `git clean -fd` to remove untracked non-ignored scratch files while preserving ignored local runtime files
  6. call `freshness.ensure_index_and_memory()`
  7. log outcome (prior HEAD, new HEAD, ensure result)
- `SyncDaemon.start()` — long-running loop; sleeps `sync.pull_interval` between cycles. SIGTERM/SIGINT trigger clean shutdown.
- All git commands use `subprocess.run` with `check=True` so non-zero exits surface as logged errors rather than silent skips.

### 4.4 `src/arquimedes/launchd.py`

New module providing a small installer surface:

- `render_plist(template_name, substitutions) -> str`
- `install(label, plist_text)` — write to `~/Library/LaunchAgents/<label>.plist` and `launchctl bootstrap gui/<uid>` it; idempotent (re-install replaces).
- `uninstall(label)` — `launchctl bootout` then delete the plist.
- `status(label)` — wraps `launchctl print` and returns a structured dict for `--status` output.

Plist templates live under `ops/launchd/`:

- `com.arquimedes.watch.plist.tmpl`
- `com.arquimedes.sync.plist.tmpl`
- `com.arquimedes.lint-full.plist.tmpl`

The watch plist uses `StartInterval` derived from `watch.scan_interval_minutes` rather than a keepalive loop. The lint-full plist uses `StartCalendarInterval` for daily 02:00 local time.

### 4.5 CLI surface — `src/arquimedes/cli.py`

Add three commands:

- `arq watch` — runs one scan cycle in the foreground or installs the scheduled job. Flags: `--config`, `--install`, `--uninstall`, `--status`, `--once`. `--install/--uninstall/--status` exit immediately after the launchd action. `--once` runs a single scan/publish cycle. Without those flags, the command stays suitable as the launchd payload if the implementation chooses an in-process sleep loop, but the default installer should prefer `StartInterval`.
- `arq sync` — runs the collaborator daemon with the same `--install/--uninstall/--status` flags. `--once` runs a single cycle and exits (useful for manual catch-up and tests).
- `arq lint --install-full` (or new `arq lint-full` — pick per open question 4) — installs only the nightly plist, with `--commit-push` in the launchd payload. Calls into the same `launchd` helper.

All three commands respect the existing JSON/human convention from `agent_cli.py` for `--status`.

### 4.6 Removal cascade — `src/arquimedes/removal.py`

New module (kept separate from `compile.py` so removal logic stays auditable):

- `cascade_delete(material_ids: list[str], *, dry_run: bool = False) -> RemovalReport` performs the spec's steps 1–4 (manifest, extracted/, wiki page, cluster references) and returns a structured report listing every file/path it touched. Step 5 (`arq cluster`), step 6 (`arq compile`), step 7 (`arq index rebuild`) are invoked by the caller (`watch.BatchPipeline`) after `cascade_delete` returns, so a `dry_run=True` mode can preview the diff without running publication.
- Idempotent: re-running on already-deleted ids is a no-op with a clear log entry.
- Threshold for cluster collapse comes from existing cluster validation (two-distinct-materials rule); `removal.py` does not redefine it.

Also extends `freshness.py` only if the implementer determines `ensure_index_and_memory()` does not already detect post-removal staleness; otherwise no change there.

### 4.7 Tests

Implemented unit coverage:

- `tests/test_watch.py` — snapshot diffing, add/modify/move/delete classification, and empty-cycle behavior.
- `tests/test_removal.py` — cascade idempotency, cluster collapse, bulk delete, dry-run report shape.
- `tests/test_sync.py` — fetch + reset + clean + ensure flow; local-edit overwrite scenario.
- `tests/test_launchd.py` — plist rendering with interval/calendar payloads.
- `tests/test_config.py` — role-specific config separation for collaborator vs maintainer.

Still manual/operator verification:

- Real `arq watch --install` / `--status` / `--uninstall` on the macOS maintainer.
- Real iCloud add/update/delete cycle through the scheduled watcher.
- Real push to GitHub and collaborator clone sync.
- Real scheduled `arq lint --full --commit-push` launchd firing at 02:00.

### 4.8 `docs/maintainer/MAINTAINER.md`

Top-level operational handbook, written from scratch using the spec's outline section. Cross-references:

- pointer from `docs/CLAUDE.md` (project file) noting that `MAINTAINER.md` is the operational counterpart for the server agent
- pointer from `docs/collaborator/agent-handbook.md` reinforcing the read-only collaborator contract
- pointer from `docs/developer/PIPELINE.md` linking to MAINTAINER for the operator-facing recovery procedures

### 4.9 `docs/developer/PLAN.md` updates

After each task lands:

- Tick the corresponding Phase 9 checkbox.
- Update **Status** line at the top once the daemon is functional in a test environment.
- Update **Last updated** on every commit that touches PLAN.md.
- Add header references for the spec and this plan (mirroring the Phase 7/8 entries).
- Update the **Verification Checklist** rows for `Watch` and `Sync` from `[ ]` to `[x]` once their integration tests pass.

### 4.10 `docs/collaborator/agent-handbook.md`

Add a one-line reinforcement of the read-only contract under the existing maintainer-only warning if the current wording doesn't already cover it. Keep within the ~800-token cap.

## Verification (end-to-end)

Run on the server first, then on a collaborator clone:

1. `arq watch --install` then `arq watch --status` reports the scheduled scan job as loaded.
2. Drop a PDF into `LIBRARY_ROOT`. After the next scan cycle (or `arq watch --once`), observe:
   - new manifest row in `manifests/materials.jsonl`
   - new directory under `extracted/<material_id>/`
   - new compiled material page under `wiki/<domain>/<collection>/`
   - new commit on `origin/main` with the spec's commit-message format
3. Cause persistent enrichment failure for one material in a 2-material batch (point its provider at `/bin/false`). Confirm:
   - one retry per `watch.log`
   - one structured failure entry in `logs/watch-<ts>.log`
   - other material is committed normally
4. Modify a tracked PDF's contents. New `material_id` flows through ingest + extract; commit reflects an update.
5. Move a file inside `LIBRARY_ROOT`. Manifest `relative_path`/`domain`/`collection` update; no extraction; commit logs the move.
6. Delete a known PDF. After the next scan cycle:
   - manifest row removed
   - `extracted/<id>/` removed
   - wiki page removed
   - cluster references rewritten (or cluster collapsed)
   - `arq index rebuild` reflects the post-removal state
   - single removal commit pushed
   - `git revert <commit>` fully restores the material
7. Delete and re-add the same file before the next scan cycle: no commit, no cascade.
8. On a separate machine: `arq sync --install`. After `sync.pull_interval`, the new commit is pulled and `arq index ensure` runs (visible in `sync.log`).
9. Edit a wiki file on the collaborator clone. Next sync overwrites it; `sync.log` records the prior HEAD so the change is recoverable via reflog.
10. `arq lint --install-full` with `lint_full.schedule_cron` set to ~2 minutes from now. Verify the plist fires once at that time, `arq lint --full` runs to completion, `global-bridge` runs as part of that job, and any tracked changes are committed/pushed.
11. `--uninstall` round-trips for all three plists; `launchctl print` no longer lists them.

## Out of scope (already documented in the spec, restated for plan-readers)

- MCP server, Web UI auth, embeddings, multi-server topology
- Any work on the deprecated raw-material global-bridge publication path outside the nightly full-lint job
- Cron-style scheduling baked into the watcher process (launchd owns scheduling)
