# Arquimedes Maintainer Operations

This is the operational handbook for the Mac Mini that publishes the shared knowledge system.

## Roles

The maintainer machine is the only semantic publisher. It owns ingest, extraction, clustering, compile, lint, memory rebuild, commits, and pushes.

Collaborator clones consume the published result and rebuild local query artifacts only.

## Cadence

- Every 30 minutes: `arq watch --once` scans the shared library and publishes one batch if anything changed.
- Daily at 02:00: `arq lint --full --commit-push` runs reflective maintenance, including `global-bridge`, then commits and pushes any changed artifacts.
- Every collaborator refresh: `arq refresh` restores the canonical repo state and ensures local index + memory.

## Setup

Install from the repo root on the maintainer machine:

```bash
arq watch --install
arq lint --install-full
arq serve --install --config config/maintainer/config.yaml
```

Use the maintainer config profile for maintainer commands:

```bash
export ARQUIMEDES_CONFIG=config/maintainer/config.yaml
```

Check launchd state:

```bash
arq watch --status
```

Run one publication cycle manually:

```bash
arq watch --once
```

## Publication Cycle

The daytime cycle is intentionally narrow:

```text
scan library -> ingest -> extract -> index rebuild -> compile -> commit/push
```

Reflective lint stages do not run in the daytime cycle. `global-bridge` runs only through the nightly `arq lint --full` job.

## LAN Web UI

The maintainer machine serves the web UI to collaborators on the local network. With `serve.host: 0.0.0.0` and `serve.port: 8420` in `config/maintainer/config.yaml`, `arq serve --install` registers a `KeepAlive` launchd job (`com.arquimedes.serve`) that stays up across reboots and restarts on crash.

Find the hostname collaborators should use:

```bash
scutil --get LocalHostName
```

That name resolves over mDNS as `<name>.local` from any client on the same LAN. Collaborators just open `http://<name>.local:8420` in a browser.

First run: macOS will prompt once to allow incoming connections for the Python binary. Approve it. If `serve` does not appear on the LAN, check System Settings → Network → Firewall.

Manage the job:

```bash
arq serve --status
arq serve --uninstall
```

Caveats:

- The web UI has no authentication. Only expose it on a trusted LAN. Do not port-forward to the public internet.
- Windows 10 build 1803+ resolves `*.local` natively. Older Windows needs Bonjour Print Services (free Apple installer) — see `docs/collaborator/setup.md`.
- The Mac Mini must not sleep. Energy Saver → "Prevent automatic sleeping when the display is off" must be on (also required for `arq watch`).

## Recovery

If a scan fails, inspect:

- `~/.arquimedes/watch.log`
- `logs/watch-<timestamp>.log`

If a publication is bad, revert the publication commit:

```bash
git revert <commit>
arq lint --quick
```

If a deletion cascade needs undoing, revert the removal commit. The next scan reconciles against the shared library state.

## Contracts

- The shared library root is the source of truth for material existence.
- If a known source file disappears, the next scan removes manifest rows, extracted artifacts, wiki pages, cluster references, index entries, and memory references through the removal cascade.
- Collaborator edits to tracked generated paths are not protected by the publication workflow.

## Logs

- `~/.arquimedes/watch.log` records scheduled scan outcomes.
- `~/.arquimedes/sync.log` records sync outcomes when sync is used.
- `logs/` contains per-stage and per-batch run details.

## References

- `docs/developer/PIPELINE.md`
- `docs/developer/superpowers/specs/2026-04-25-phase9-server-agent-design.md`
- `docs/developer/superpowers/plans/2026-04-25-phase9-server-agent.md`
