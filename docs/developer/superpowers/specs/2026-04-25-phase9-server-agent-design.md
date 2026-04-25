# Phase 9 — Server Agent + Sync (Design Spec)

> **Status:** Proposed (2026-04-25)
> **Phase:** 9
> **Supersedes:** the Phase 9 stub in `docs/developer/PLAN.md`
> **Companion plan:** [Phase 9 implementation plan](../plans/2026-04-25-phase9-server-agent.md)
> **References:** [Full design spec](2026-04-04-arquimedes-knowledge-system-design.md), [PIPELINE.md](../../PIPELINE.md), [Phase 7 agent tools](2026-04-16-phase7-agent-tools-design.md), [Phase 8 web UI](2026-04-11-phase8-web-ui-design.md)

## Context

Phases 1–8 built the maintainer's repertoire: ingest, deterministic + LLM extraction, search, clustering, wiki compilation, lint, memory bridge, agent CLI, and a web UI. `docs/developer/PIPELINE.md` describes the canonical operational flow. Phase 9 turns that flow into an always-on system.

In Karpathy's pattern this is the moment the wiki gains a maintainer that runs without supervision. In Arquimedes, the server agent (Mac Mini) scans the shared library on a fixed cadence, runs the publication pipeline for any newly detected changes, commits and pushes the results, and keeps collaborators in sync via a lightweight read-only clone protocol. A nightly reflective lint job extends that loop with periodic deep maintenance.

Phase 9 deliverables fall into five buckets:
1. `arq watch` — server-side scheduled scan daemon + batch pipeline + auto-commit/push
2. Removal cascade — automatic, reversible deletion handling
3. `arq sync` — collaborator-side read-only auto-sync daemon
4. launchd integration for `watch`, `sync`, and a separate daily `lint --full`
5. `docs/maintainer/MAINTAINER.md` — the operational handbook for the server agent

## Operating contract

The contract that ties the daemons together (and that `MAINTAINER.md` codifies):

- **The server maintainer is the only semantic publisher.** It is the only role that runs `arq cluster`, `arq compile`, `arq lint --full`, and the global `arq memory rebuild`. Collaborators never run these.
- **The library root is the source of truth for material existence.** When a file disappears from `LIBRARY_ROOT`, the maintainer cascades the deletion through manifest, extracted artifacts, wiki, clusters, and index. Reversible only via `git revert`.
- **Collaborator clones are read-only for tracked paths.** `arq sync` performs `git fetch && git reset --hard origin/main` on every sync. Any local edit to `extracted/`, `wiki/`, `derived/`, `manifests/`, or `indexes/` will be overwritten without warning. Ignored runtime files (e.g., `config/collaborator/config.local.yaml`, SQLite files) are preserved.
- **Global-bridge belongs to nightly full lint, not the daytime publish loop.** The 30-minute publication cycle does not run reflective lint stages. The daily 02:00 `arq lint --full` pass does, and that is where `global-bridge` runs alongside the other reflective maintenance stages. The legacy raw-material bridge publication path and compatibility readers are retired.
- **Per-batch atomicity is best-effort, not transactional.** A batch commit captures a consistent post-pipeline state; partial failures within a batch (e.g., one material fails enrichment after retry) are logged, the rest is committed, and the failed material is retried in the next batch via the existing stamp/checkpoint machinery.

## `arq watch` — server daemon

`arq watch` is a scheduled scanner, not an event-stream watcher. Every `watch.scan_interval_minutes` minutes (default 30), it walks `LIBRARY_ROOT`, compares the current file set to the known manifest/library state, and builds one publication batch.

### Scan model

Each cycle classifies observed changes into four logical categories:

- `created` — a file is present in `LIBRARY_ROOT` but not yet represented in the manifest
- `modified` — a known path now has different contents
- `moved` — the same contents now live at a different path; manifest `relative_path` / `domain` / `collection` are rehomed per Phase 2 rules
- `deleted` — a previously known source path is absent from `LIBRARY_ROOT`

The cycle then coalesces the detected changes into:

- **Add/modify list** — paths to ingest (or re-ingest), extract, and publish
- **Move list** — paths whose only change is a manifest rehome
- **Delete list** — known materials whose source file is gone (see [Removal cascade](#removal-cascade-hard-automatic))

If a cycle fails before commit, nothing is requeued in memory; the next scheduled scan recomputes the durable repo/library delta from disk. Stamps + checkpoints from earlier phases make this idempotent.

### Per-batch pipeline

For non-empty add/modify or delete lists, the watcher runs one publication cycle:

```
arq ingest <changed paths>
arq extract                       # all-pending mode (PIPELINE.md §3); enrichment in parallel
arq index rebuild
arq compile                       # auto-runs lint --quick + memory rebuild per Phase 5/6
git add -A && git commit -m "<batch message>" && git push
```

Implementation note: the daytime publication cycle intentionally stops at publish-time work (`ingest`, `extract`, `index rebuild`, `compile`). Reflective lint stages, including `global-bridge`, are reserved for the nightly full-lint job. If the current implementation still requires an explicit `arq cluster` call immediately before `arq compile` to keep collection-local pages current, the implementation plan must wire that prerequisite in without changing the higher-level cadence contract.

### LLM retry policy

`arq extract`'s enrichment step already performs ordered runtime fallback across configured providers. On top of that, the watcher adds **one batch-level retry** per material that the enrichment phase reports as failed (process exit non-zero, timeout, or unusable empty output across the entire route list). If the second attempt also fails:

- the material's stage stamps and checkpoints are left as-is (so a future run continues, not restarts)
- a structured failure entry is written to `logs/watch-<batch-timestamp>.log`
- the rest of the batch proceeds, and the failed material is retried automatically on the next scheduled scan cycle

This is intentionally narrower than retrying every kind of failure: hard runtime errors (config issues, missing binaries) should fail loudly rather than retry silently.

### Commit semantics

Each successful batch commits with the structure:

```
auto: ingest <N add> / <M update> / <K remove>

added:
  - <material_id>  <relative_path>
updated:
  - <material_id>  <relative_path>
removed:
  - <material_id>  <former relative_path>
```

`watch.commit_message_template` is configurable but defaults to the form above. Commits are pushed immediately after creation; push failures (network or branch conflict) abort the batch, log the error, and let the next batch retry — they do not roll back the local commit, since the pipeline state is already consistent.

### Logs

- `~/.arquimedes/watch.log` — long-running daemon log (rotating, size-bounded). Captures startup, configured scan cadence, scan summaries, batch outcomes, and operator-facing errors.
- `logs/watch-<timestamp>.log` — per-batch detailed log for post-mortems. Mirrors the existing per-stage `logs/` convention used by `arq enrich`, `arq cluster`, and `arq lint`, including explicit `START` / `DONE` / `FAILED` markers.

## Removal cascade (hard, automatic)

Triggered when a scheduled scan shows a previously known material's source file as missing.

### Cascade order

For each removed material id:

1. Delete the row from `manifests/materials.jsonl`.
2. Delete the directory `extracted/<material_id>/` in full.
3. Delete the compiled wiki material page under `wiki/<domain>/<collection>/...`.
4. Drop concept-cluster references that point at this material id from every affected `derived/collections/<domain>__<collection>/local_concept_clusters.jsonl`. If a cluster's member set falls below the local-home threshold (two distinct materials), drop the cluster entirely along with its compiled page.
5. Run `arq cluster` for each affected collection so cluster identity stays internally consistent.
6. Run `arq compile` (which auto-runs `arq lint --quick` and `arq memory rebuild`) so wiki and SQLite memory drop the orphaned references.
7. Run `arq index rebuild` to fully reflect the post-removal state.
8. Stage and commit (single commit, alongside any add/modify changes from the same batch).

### Reversibility

Every cascade step is captured in git. `git revert <removal commit>` restores the manifest, extracted directory, wiki page, and cluster artifacts. The next scan cycle sees the source file present again (or not, if the file was deleted from iCloud) and reconciles.

### Edge cases

- **Bulk deletion**: if N materials are removed between two scans, run cascade steps 1–4 for all of them, then a single pass of steps 5–8.
- **Transient absence**: if iCloud temporarily hides a file between scans, the next scan may classify it as deleted. The system accepts that risk in exchange for a simple 30-minute cadence; operators who need more conservatism can raise `watch.scan_interval_minutes` or temporarily disable the daemon during large iCloud moves.
- **Cluster collapse**: if removing a material reduces a cluster below the local-home threshold, drop the cluster and its compiled page. The cascade does not attempt to re-home those concepts elsewhere; the next normal `arq cluster` run does that on its own evidence.
- **Deletion of a material referenced from collection reflection prose**: the reflection text becomes stale. Step 6 (`arq compile`) runs `arq lint --quick` which flags this as `stale_collection_reflection`; remediation happens during the next daily 02:00 `arq lint --full`. The watcher does not block on this.

## `arq sync` — collaborator daemon

### Behavior

A long-running process that, every `sync.pull_interval` seconds (default 300):

1. Run `git fetch origin`. On network failure, log and skip until next interval.
2. Run `git reset --hard origin/main` after every successful fetch. This resets every tracked file under `extracted/`, `wiki/`, `derived/`, `manifests/`, and `indexes/` to the published state, even when a collaborator agent edited tracked files locally.
3. Run `git clean -fd` to remove untracked non-ignored scratch files created inside the repo. Ignored runtime files such as `config/collaborator/config.local.yaml` and `indexes/search.sqlite` are preserved.
4. After a successful reset/clean, run `arq index ensure` (which auto-runs `arq memory ensure` per Phase 5.5). This rebuilds the local SQLite index and memory bridge if their inputs changed.
4. Log the outcome.

### Read-only collaborator contract

`arq sync` does **not** attempt three-way merges, stash/unstash, conflict markers, or operator prompts. The contract is:

- Collaborators never edit tracked paths in their clone. If they need a private workspace, they put it outside the repo or in untracked files.
- Any local change to a tracked path will be overwritten on the next sync, and untracked non-ignored scratch files will be removed. The sync log records whether local tracked changes were restored, including the prior `HEAD` so committed previous states are recoverable via reflog within the system's reflog horizon.
- Local commits on the collaborator's `main` branch are also overwritten. Collaborators who want to experiment must use a branch other than `main` (and not run `arq sync` while on that branch).

This contract is documented in `docs/maintainer/MAINTAINER.md` and reinforced in `docs/collaborator/agent-handbook.md` for collaborator-side agents.

### Logs

`~/.arquimedes/sync.log` records each cycle: timestamp, fetch outcome, reset outcome, ensure outcome. Sized and rotated like `watch.log`.

## launchd integration

Phase 9 ships first-class launchd installers so admins do not hand-author plists.

### Watch installer

```
arq watch --install   [--config <path>]
arq watch --uninstall
arq watch --status
```

`--install` writes `~/Library/LaunchAgents/com.arquimedes.watch.plist` containing:

- `Label`: `com.arquimedes.watch`
- `ProgramArguments`: `[<arq path>, watch, --config, <config path>, --once]`
- `WorkingDirectory`: repo root
- `RunAtLoad`: `true`
- `StartInterval`: `watch.scan_interval_minutes * 60`
- `KeepAlive`: `false`
- `StandardOutPath` / `StandardErrorPath`: `~/.arquimedes/watch.out.log` / `watch.err.log`
- `EnvironmentVariables`: any required `PATH` extension to find `git` and the LLM CLI binaries

Then it `launchctl bootstrap gui/<uid> <plist>` and confirms via `launchctl print`. Re-running `--install` re-bootstraps after writing.

### Sync installer

`arq sync --install` writes `…sync.plist` analogously, with `StartInterval` set from `sync.pull_interval`. KeepAlive is `false`; launchd respawns on schedule, not on exit.

### Lint --full installer

A separate installer ships the nightly reflective pass:

```
arq lint --install-full
```

(or, equivalently, `arq lint-full --install` — the implementation plan picks the spelling.) This writes `…lint-full.plist` with `StartCalendarInterval` derived from `lint_full.schedule_cron` (default daily 02:00 local time) and `ProgramArguments` of `[<arq>, lint, --full, --commit-push]`. The lint-full job commits and pushes any resulting repo changes after a successful run.

### Templates

For admins who prefer manual setup, the same plist bodies ship under `ops/launchd/` as committed templates with placeholder variables. The installer simply renders these templates and writes them to `~/Library/LaunchAgents/`.

## Configuration additions

`config/config.yaml` defines only collaborator-safe shared defaults (`library_root`, `sync`, `serve`). Maintainer-only extraction, enrichment, LLM/provider routing, watch, lint, and clustering settings live in `config/maintainer/config.yaml`; collaborator setup writes only `config/collaborator/config.local.yaml`.

Shared config:

```yaml
sync:
  pull_interval: 300         # seconds
  auto_start: false
  auto_index: true
  reset_tracked: true        # collaborator clones are read-only
```

Maintainer config:

```yaml
extraction:
  chunk_size: 500
  generate_thumbnails: true
  ocr_fallback: true

enrichment:
  prompt_version: "enrich-v1.0"
  enrichment_schema_version: "1"
  # plus maintainer-only llm_routes

watch:
  scan_interval_minutes: 30  # periodic library scan cadence
  batch_commit: true
  enrich_retries: 1          # batch-level retries per failed material
  commit_message_template: |
    auto: ingest {n_add} add / {n_update} update / {n_remove} remove

lint_full:
  schedule_cron: "0 2 * * *" # daily 02:00 local; advisory, used by the launchd installer
```

`config/collaborator/config.local.yaml` overrides shared defaults for collaborator machines and is gitignored. `config/maintainer/config.yaml` is the tracked maintainer profile; optional `config/maintainer/config.local.yaml` is gitignored for maintainer-local overrides.

## `docs/maintainer/MAINTAINER.md` — operational handbook outline

Maintainer operational doc. Distinct from `CLAUDE.md` (build-system docs), `docs/developer/PLAN.md` (implementation plan), and `docs/collaborator/agent-handbook.md` (collaborator-agent investigation guide). Sections:

1. **Roles and machines** — what runs on the Mac Mini vs. collaborator laptops.
2. **Cadence** — 30-minute publication scans, daily 02:00 `lint --full`, log retention.
3. **Setup** — `arq watch --install`, `arq sync --install`, `arq lint --install-full`, prerequisites (Full Disk Access for the watcher, network access for push).
4. **Recovery procedures**:
   - Daemon stuck or crashed → `launchctl kickstart` / `--uninstall` + `--install`.
   - Failed batch → inspect `logs/watch-<ts>.log`, drop the affected file back into the library to retrigger, or run the pipeline manually.
   - Bad publication → `git revert` the offending commit; the next batch reconciles.
   - Removal-cascade undo → `git revert <removal commit>`; verify with `arq lint --quick`.
5. **Hard contract for collaborators** — read-only tracked paths, no edits, sync overwrites.
6. **Log map** — `~/.arquimedes/*.log`, `logs/`, where each event is recorded.
7. **Pointers** — back to `docs/developer/PIPELINE.md`, `docs/collaborator/agent-handbook.md`, and the Phase 9 spec/plan.

## Out of scope

Phase 9 is bounded. The following are explicitly **not** included:

- MCP server (deferred indefinitely; CLI is the agent surface)
- Any separate daytime bridge-publication path outside nightly `arq lint --full`; compile does not publish the retired legacy raw-material bridge layer
- Web UI auth, push notifications, or any Phase 8 follow-up
- Embeddings / semantic search
- Multi-server topology, sharding, or distributed publication
- Cron-style scheduling baked into `arq watch`'s process; scheduling lives in launchd

## Verification scenarios

The implementation plan's verification section expands these into runnable steps; the spec defines what must work end-to-end.

1. **Add path, single material**: `arq watch --install`, drop a PDF into `LIBRARY_ROOT`, wait for the next 30-minute scan. Manifest, `extracted/<id>/`, wiki page, index, and a single commit on `origin/main` all reflect the new material.
2. **Add path, retry-then-skip**: simulate persistent enrichment failure for one material in a multi-material batch (e.g., point its provider at a broken binary). Confirm one retry, then a logged skip, with the rest of the batch committed.
3. **Modify path**: update a tracked PDF's contents (different `material_id`). Watcher reuses the manifest entry rehome path or registers a new material as appropriate; commit reflects an update.
4. **Move path**: rename a file within the library. Manifest `relative_path`, `domain`, `collection` update; no extraction work; single commit.
5. **Delete path, single**: remove a known PDF from `LIBRARY_ROOT`. After the next scan, the cascade runs; `git revert` of the resulting commit fully restores the material.
6. **Delete path, bulk**: remove three files between two scans. Single commit, all three cascaded together.
7. **Delete path, transient**: remove and re-add a file before the next scan. No cascade, no commit.
8. **Sync happy path**: server pushes; collaborator's `arq sync` (already installed) picks up the change within `sync.pull_interval` and runs `arq index ensure`.
9. **Sync overwrite**: introduce a local edit under `wiki/` on the collaborator. Next sync resets it; sync log records the overwritten `HEAD`.
10. **Lint --full schedule**: install the lint-full plist with a near-future calendar interval; verify it fires once, runs `global-bridge` with the other reflective stages, and commits/pushes any resulting changes.
11. **launchd lifecycle**: `--install`, `--status`, `--uninstall` round-trip cleanly for all three plists.

## Open questions (handed to the implementation plan)

- Does `arq compile` already invoke `arq cluster` for stale collections? If yes, drop the explicit `cluster` step from the watcher's pipeline.
- FSEvents library: `pyobjc-FSEvents` directly vs. `watchdog`'s FSEvents observer. Pick during 4.2 and document trade-offs.
- macOS Sequoia Full Disk Access requirement for the watcher process.
- Whether `arq lint-full` should be a new top-level command or an `--install-full` flag on `arq lint`.
