# Arquimedes Maintainer Operations

This is the operational handbook for the Mac Mini that publishes the shared knowledge system.

## The vault model (Phase 10)

`arq` itself is one piece of code (the public `arquimedes` package, installed via pipx). Each knowledge base lives in its own private git repo called a **vault**. A maintainer machine owns exactly one vault and is the only writer for it.

Three trees, three lifecycles:

- **Code** — the `arquimedes` Python package (this repo). Public, versioned, upgraded via `pipx install --upgrade arquimedes`.
- **Vault** — a private git repo containing `extracted/`, `manifests/`, `derived/`, `wiki/`, and the vault's own `config/`. Maintainer pushes; collaborators pull read-only via deploy keys. **One vault per maintainer machine.**
- **Local cache** — per-machine, regenerable runtime state (`indexes/search.sqlite`, `logs/`). Defaults to the vault root for back-compat; can be moved out of the vault tree by setting `ARQUIMEDES_LOCAL_CACHE` or `local_cache_root` in the active config.

Pointing `arq` at a vault:

- `--config <path>` global flag on every subcommand
- `$ARQUIMEDES_CONFIG` env var
- cwd-walk for `config/config.yaml` (legacy default)

Manual one-off operations on a different vault: `arq --config ~/Vaults/office/config/maintainer/config.yaml watch --once`.

Inspect the active vault any time with `arq vault info --human`.

## Roles

The maintainer machine is the only semantic publisher. It owns ingest, extraction, clustering, compile, lint, memory rebuild, commits, and pushes.

Collaborator clones consume the published result and rebuild local query artifacts only.

The freshness path (`git fetch && git reset --hard && git clean -fd`) only runs on collaborator machines (detected by presence of `config/collaborator/config.local.yaml` and absence of `config/maintainer/config.yaml`). Maintainer and developer machines skip the destructive steps so in-flight work is never wiped.

## Cadence

- Every 30 minutes: `arq watch --once` scans the shared library and publishes one batch if anything changed.
- Daily at 02:00: `arq lint --full --commit-push` runs reflective maintenance, including `global-bridge`, then commits and pushes any changed artifacts.
- Every collaborator refresh: `arq refresh` restores the canonical repo state and ensures local index + memory.

## Setup

Install code globally:

```bash
pipx install arquimedes
# or, until PyPI is set up:
pipx install git+https://github.com/<user>/arquimedes.git
```

Either point at an existing vault or create one:

```bash
# new vault (this machine becomes its maintainer)
arq init ~/Vaults/personal
# or clone a vault you already own
git clone git@github.com:<user>/arq-vault-personal.git ~/Vaults/personal
```

Pin this shell — and every launchd job installed below — to the active vault:

```bash
export ARQUIMEDES_CONFIG=~/Vaults/personal/config/maintainer/config.yaml
```

Install launchd jobs (each one embeds the current `$ARQUIMEDES_CONFIG` into its plist via `_arq_program_args()`, so the job stays pinned to this vault even after reboot):

```bash
arq watch --install
arq lint --install-full
arq serve --install
```

Verify the vault was resolved correctly:

```bash
arq vault info --human
```

Check launchd state:

```bash
arq watch --status
```

Run one publication cycle manually:

```bash
arq watch --once
```

## Onboarding a collaborator

Collaborator onboarding is a standard maintainer task. The maintainer can delegate it to an agent, but the handoff should always contain the same ingredients:

- a per-collaborator read-only deploy key
- the vault clone URL
- the collaborator setup guide
- a small collaborator-facing handoff note that tells the collaborator's agent what to read and which local key file to use

Recommended flow:

1. Generate a dedicated deploy key for that collaborator.
```bash
ssh-keygen -t ed25519 -f ~/Downloads/arq-vault-<name>.key -C "arq-vault <name>"
```
2. Add `~/Downloads/arq-vault-<name>.key.pub` to the private vault repo as a GitHub deploy key, with write access disabled.
3. Create a handoff folder containing:
   `docs/collaborator/setup.md`, the private key file, and a copy of `docs/maintainer/collaborator-handoff-template.md` filled in for that collaborator.
4. Send the handoff folder securely to the collaborator.

The vault clone URL given to collaborators should use the SSH host alias described in `docs/collaborator/setup.md`, for example:

```text
git@arq-vault:<user>/arq-vault-personal.git
```

The collaborator's agent should be told to read the setup guide first and use the private key file from the same handoff folder. The reusable template for that note lives at `docs/maintainer/collaborator-handoff-template.md`.

If you are using an agent to prepare the handoff, the agent should:

1. generate the per-collaborator deploy keypair
2. assemble the handoff folder
3. fill in the handoff template with the vault clone URL and filenames
4. stop before any GitHub-side deploy-key registration unless the maintainer explicitly asks it to continue

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
- `<local-cache-root>/logs/` contains per-stage and per-batch run details (defaults to `<vault-root>/logs/` unless `ARQUIMEDES_LOCAL_CACHE` or `local_cache_root` is set).

## Multi-vault on one human

A maintainer can own multiple vaults across multiple machines (e.g., personal vault on the Mac Mini, office vault on a different host). Constraints:

- Each vault gets its own private GitHub repo.
- Each maintainer machine owns exactly one vault and runs exactly one set of `arq watch / arq lint / arq serve` launchd jobs.
- For one-off operations against a non-resident vault from any machine, point `arq` at it manually: `arq --config /path/to/other-vault/config/maintainer/config.yaml overview`.

Do not try to install two `arq watch` launchd jobs on one machine — the labels collide.

## References

- `docs/developer/PIPELINE.md`
- `docs/developer/superpowers/specs/2026-04-25-phase9-server-agent-design.md`
- `docs/developer/superpowers/plans/2026-04-25-phase9-server-agent.md`
