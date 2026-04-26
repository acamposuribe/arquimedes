# Phase 10 — Vault Model: Code/Data Split (Design Spec)

> **Status:** Proposed (2026-04-26, revised after design discussion)
> **Phase:** 10
> **Companion plan:** _not yet written — pending ratification of this spec_
> **References:** [Phase 9 server agent design](2026-04-25-phase9-server-agent-design.md), [PIPELINE.md](../../PIPELINE.md), [MAINTAINER.md](../../../maintainer/MAINTAINER.md), [collaborator setup](../../../collaborator/setup.md)

## Context

Today the `arquimedes` repository mixes two things that evolve at completely different rates:

- **Code** — `src/`, `tests/`, `docs/`, `pyproject.toml`, schemas, default configs, web templates. Versioned, tagged, released. One source of truth for everyone.
- **Data** — `extracted/`, `manifests/`, `derived/`, `indexes/`, `wiki/`, `logs/`, the local config. Per-instance, machine-generated, refreshed continuously by the publication pipeline.

This served the bootstrap phase. It does not scale to the next step. The maintainer (Alejandro) wants multiple independent knowledge bases off the same code — a personal KB today, an office KB next, possibly a research-lab KB later. **Branches and forks are the wrong abstraction** for this because both couple code release cycles to data lifecycle.

This phase introduces the **vault** model: code is an installable package, each knowledge base is a separate **vault** (a private git repo) that the CLI is pointed at via a config file.

## Goals

1. Decouple code releases from data growth. Improving `arq` is `pipx install --upgrade arquimedes`, not a merge into N data branches.
2. Allow N independent vaults across N maintainer machines. Each vault has exactly one maintainer machine.
3. Preserve the current Phase 9 publication contract (server publishes, collaborators consume read-only) inside the new model.
4. Preserve the full git history of the existing code and data when migrating.
5. **Zero GitHub-account requirement for collaborators.** They install code anonymously and consume vaults via read-only deploy keys.
6. Manual override (drive a different vault from a maintainer's laptop): single `--config <path>` flag.

## Non-goals

- Multi-vault on one maintainer machine. Each maintainer machine owns exactly one vault. (A user can still manually point `arq --config` at any vault for one-off operations from any machine.)
- A central registry of vaults. Discovery is by config-file path / env var, period.
- Cross-vault search or federation.
- Migration tooling beyond a one-time `git filter-repo` recipe.

## Operating contract

- **The code repo (`arquimedes`) is the only source of truth for `arq` itself.** Public. Improvements ship as tagged releases consumed via `pipx install --upgrade`.
- **A vault is a private git repo.** Maintainer pushes; collaborators clone read-only via per-collaborator deploy keys. Distribution is git, not cloud sync.
- **One vault per maintainer machine.** Each maintainer machine runs `arq watch`, `arq lint --full`, and `arq serve` against exactly one vault.
- **Each maintainer machine = one launchd label set.** No label suffixing for multi-vault.
- **The library root (source PDFs) lives in the shared cloud/NAS folder, separately from the vault.** Multiple writers (maintainer + collaborators dropping new materials) is the existing Phase 9 model. The vault never goes in the cloud folder, eliminating the risk of accidental collaborator write to derived artifacts.
- **Local SQLite and runtime state are per-machine and regenerable.** They live outside the vault and are never committed.
- **Per-vault publication contract is unchanged from Phase 9.** Maintainer publishes via `git push`; collaborators consume via `arq refresh` (`git fetch && git reset --hard origin/main`). The change is *what* gets published — a vault repo, not the code repo.

## Naming

| Term | Meaning |
|---|---|
| **vault** | One knowledge base. Lives in a private git repo. |
| **vault root** | The absolute path to the local checkout of a vault on this machine. All `arq` data paths are resolved relative to this. |
| **vault repo** | The remote git repo for a vault, on the maintainer's GitHub account (e.g., `<user>/arq-vault-personal`). |
| **library root** | (unchanged) Path to the shared cloud/NAS folder containing the source PDFs. May or may not be near the vault root on disk. |
| **local cache root** | Per-machine directory holding `indexes/search.sqlite` and runtime state. Defaults to `~/.arquimedes/<vault-name>/`. Never synced, never committed. |
| **code repo** | This repo (`arquimedes`). Public. Source of truth for `arq` itself. |
| **deploy key** | Per-collaborator read-only SSH key registered on a vault repo. Lets a collaborator clone without a GitHub account. |

## Code / vault / local-cache boundary

| Path | Lives in | Notes |
|---|---|---|
| `src/`, `tests/`, `pyproject.toml` | code repo (public) | Released as the `arquimedes` package. |
| `docs/` | code repo | Documentation about the system, not a specific KB. |
| Web templates / static | code repo | Already inside the package (see `serve.py`). |
| `config/maintainer/config.example.yaml` | code repo | Template only. Per-vault overrides live in the vault. |
| `extracted/`, `manifests/`, `derived/`, `wiki/`, `logs/` | vault repo | Published by the maintainer; consumed read-only by collaborators. |
| `config.local.yaml` | vault root (committed) | Names `library_root`, optional `local_cache_root`, maintainer-only sections. The maintainer can keep machine-specific bits in a separate ignored file (`config.machine.yaml`). |
| `indexes/search.sqlite` + WAL/SHM | local cache root (per-machine, never synced) | Regenerated from the vault by `arq refresh`. |
| Freshness stamps, lockfiles | local cache root | Hidden, machine-local. |

The `config/maintainer/config.yaml` we have today splits into:
- **`config/maintainer/config.example.yaml`** in the code repo — template with LLM routing, prompt versions, schema versions.
- **`config.local.yaml`** at each vault root — concrete `library_root`, optional overrides for any of the example fields.

## Vault layout

```
~/Vaults/personal/                   ← vault root (one local checkout of the vault repo)
├── .git/                            ← clone of <user>/arq-vault-personal
├── config.local.yaml                ← library_root + per-vault settings (committed)
├── extracted/<material_id>/...      ← per-material artifacts
├── manifests/                       ← provenance / ingest manifests
├── derived/                         ← clusters / projections
├── wiki/                            ← human-readable pages
└── logs/                            ← per-stage run details

~/.arquimedes/personal/              ← local cache root (per-machine, never synced)
├── indexes/search.sqlite (+ WAL/SHM)
├── memory.bridge
└── state.json                       ← freshness stamps, etc.

<cloud/NAS>/Library-Personal/        ← library root (source PDFs, shared)
├── architecture/...
├── research/...
└── ...
```

## Configuration discovery

`arq` resolves the active vault via a single config file at startup, in this order:

1. `--config <path>` global flag (overrides everything; the manual-switch mechanism).
2. `$ARQUIMEDES_CONFIG` env var (the everyday default — set once per machine).
3. Walk up from cwd until a `config.local.yaml` is found whose parent contains `extracted/` or `wiki/` (convenience for `cd ~/Vaults/personal && arq overview`).
4. Otherwise: error with a one-line pointer to `arq init --help`.

The config file's parent directory is the **vault root**. The config file itself names `library_root` and optionally `local_cache_root` (defaulting to `~/.arquimedes/<basename(vault_root)>/`).

This replaces every current call site that uses `get_project_root()`. The resolver lives in one module; everything else asks the resolver.

## CLI changes

New:

- `arq init <vault-root>` — scaffold an empty vault locally. Creates the canonical subdirectories, optionally `git init` the vault, writes a starter `config.local.yaml` (asking for `library_root`).
- `arq init --from <vault-repo-url> <vault-root>` — clone an existing vault repo to `<vault-root>`. The everyday collaborator onboarding command. Assumes the deploy key is already installed.
- `arq vault info` — report the resolved vault root, library root, local cache root, freshness, config source, vault git remote.
- Global `--config <path>` flag accepted by every subcommand (today only some commands accept it).

Changed:

- `arq refresh` — on **collaborator machines** fetches the vault git remote, runs `git reset --hard origin/main` + `git clean -fd`, then refreshes local-cache derived artifacts. On **maintainer/developer machines** skips the destructive git steps (would otherwise wipe in-flight work) and only refreshes the local cache. The role gate already ships in `freshness._is_collaborator_machine()` and is keyed on overlay presence (`config/collaborator/config.local.yaml` exists AND `config/maintainer/config.yaml` does not). No longer fetches the code repo (code is upgraded via pipx).
- `arq watch` — publishes to the vault git remote, not the code repo.
- `arq sync` — fetches the vault git remote.
- All read commands (`overview`, `search`, `read`, `figures`, `annotations`) — resolve paths via the vault resolver.

## Installation

**Code:** `pipx install arquimedes` (or `pipx install git+https://github.com/<user>/arquimedes.git@v0.x` until PyPI is set up). The code repo is **public** — collaborators install anonymously, no GitHub account required.

**Vault clone (per collaborator):** read-only deploy key + `arq init --from`. See "Deploy key setup" below.

## Maintainer workflow

```
maintainer machine (Mac Mini for personal vault, separate machine for office vault, etc.)
├── pipx install arquimedes==0.x        ← code, installed once globally
└── ~/Vaults/personal/                  ← clone of <user>/arq-vault-personal
    ├── config.local.yaml               ← library_root + maintainer profile
    ├── extracted/, wiki/, ...
    └── .git/                           ← push to publish
```

Setup:

```bash
pipx install arquimedes
mkdir -p ~/Vaults
git clone git@github.com:<user>/arq-vault-personal.git ~/Vaults/personal
export ARQUIMEDES_CONFIG=~/Vaults/personal/config.local.yaml
arq watch --install
arq lint --install-full
arq serve --install
```

`ARQUIMEDES_CONFIG` is set globally in the maintainer's shell profile so all interactive `arq` calls hit the right vault. The launchd installers also embed the resolved config path into the plist `ProgramArguments`, so launchd jobs are independent of the active shell.

A second knowledge base means a second maintainer machine: same setup against `~/Vaults/office` and `<user>/arq-vault-office`. The launchd labels stay `com.arquimedes.{watch,lint,serve}` on each machine — no per-vault label suffixing because each machine owns one vault.

**Manual cross-vault operations from a maintainer's laptop:** `arq --config ~/Vaults/office/config.local.yaml read <id>` works from any machine that has a checkout of that vault and the right deploy key. No env var change required.

**Optional convenience:** the maintainer may `git clone` the vault working copy *inside* the cloud/NAS folder so the source PDFs and the derived artifacts are physically adjacent. Distribution still happens via `git push`, not cloud sync. The cloud sync of the working tree is incidental.

## Collaborator workflow

```
collaborator machine (Windows or macOS, no GitHub account)
├── pipx install arquimedes              ← public code, anonymous install
├── ~/.ssh/arq-vault-personal_deploy_key ← maintainer-issued read-only key
└── ~/Vaults/personal/                   ← clone of <user>/arq-vault-personal
    └── config.local.yaml                ← committed; collaborator does not edit
```

Setup:

```bash
pipx install arquimedes
# ...one-time deploy-key install (see below)...
arq init --from git@github.com-arq-personal:<user>/arq-vault-personal.git ~/Vaults/personal
export ARQUIMEDES_CONFIG=~/Vaults/personal/config.local.yaml
arq refresh
arq overview --human
```

Collaborator's `library_root`: if the source PDFs are in a shared cloud folder the collaborator already has, no extra config — `library_root` in the committed `config.local.yaml` resolves the same on every machine because everyone mounts the cloud folder at a path the maintainer can predict, OR the collaborator overrides it in a per-machine sidecar (`config.machine.yaml`, gitignored). Treatment of this varies by cloud provider; document both patterns in `setup.md`.

## Deploy key setup (per collaborator, one-time)

The maintainer:

1. On the maintainer machine, generate a key per collaborator: `ssh-keygen -t ed25519 -f ~/.ssh/arq-vault-personal_<name>_deploy -N "" -C "arq-vault-personal deploy: <name>"`.
2. Add the **public** key to the vault repo on GitHub: Settings → Deploy keys → Add deploy key. Leave "Allow write access" **unchecked**.
3. Send the **private** key to the collaborator via a secure channel (Signal, password manager share, USB hand-off — not email).

The collaborator (one time, per vault):

- Place the private key at `~/.ssh/arq-vault-personal_deploy` (macOS/Linux) or `%USERPROFILE%\.ssh\arq-vault-personal_deploy` (Windows).
- `chmod 600 ~/.ssh/arq-vault-personal_deploy` (macOS) — Windows ACLs equivalent.
- Add an entry to `~/.ssh/config` (or `%USERPROFILE%\.ssh\config`):
  ```
  Host github.com-arq-personal
      HostName github.com
      User git
      IdentityFile ~/.ssh/arq-vault-personal_deploy
      IdentitiesOnly yes
  ```
- Clone using the aliased host: `git@github.com-arq-personal:<user>/arq-vault-personal.git`.

The aliased host avoids interfering with any other GitHub SSH usage on the collaborator's machine. Per-vault keys mean the maintainer can revoke one collaborator's access without affecting others (delete that one deploy key on GitHub).

## Migration

Goal: split the current `arquimedes` repo into:

1. `arquimedes` (this repo, after split) — code, docs, schemas only. **Made public**. History preserved.
2. `arq-vault-personal` — the existing data. Private. History preserved.

Tooling: [`git filter-repo`](https://github.com/newren/git-filter-repo). One pass per resulting repo.

```bash
# Step 1: mandatory backup
git clone --mirror <current-repo> arquimedes-backup.git

# Step 2: derive the vault-only repo
git clone <current-repo> arq-vault-personal
cd arq-vault-personal
git filter-repo \
  --path extracted/ \
  --path manifests/ \
  --path derived/ \
  --path indexes/ \
  --path wiki/ \
  --path logs/ \
  --path config/maintainer/config.yaml      # the live machine config goes with the vault as config.local.yaml
git mv config/maintainer/config.yaml config.local.yaml
# Push to a new private GitHub repo:
gh repo create <user>/arq-vault-personal --private --source=. --push

# Step 3: derive the code-only repo
git clone <current-repo> arquimedes-code
cd arquimedes-code
git filter-repo \
  --invert-paths \
  --path extracted/ \
  --path manifests/ \
  --path derived/ \
  --path indexes/ \
  --path wiki/ \
  --path logs/
# Force-push to the existing arquimedes repo (after coordinating with current collaborators —
# this rewrites history; existing clones must be re-cloned).
git push --force origin main

# Step 4: flip the existing arquimedes repo to public on GitHub.
gh repo edit <user>/arquimedes --visibility public
```

The history-rewrite of the code repo and the visibility flip are the disruptive steps. Both are mandatory and both happen at the same moment. Existing collaborators must:

- Re-clone (or, equivalently, install via pipx and stop using their old code-repo clone).
- Receive a deploy key and clone the new vault repo.

Communicate the migration date in advance.

## Phasing

A reasonable build order, each step independently shippable:

0. **Freshness role gate (DONE 2026-04-26).** `freshness.update_workspace()` only runs `git fetch && git reset --hard && git clean -fd` when `_is_collaborator_machine(root)` returns True (collaborator overlay present, no maintainer overlay). On maintainer/developer machines the destructive steps are skipped and only `ensure_index_and_memory()` runs. This was extracted out of phase 10 because it was actively destroying maintainer/dev work mid-session; the rest of phase 10 builds on it.
1. **Vault resolver** + global `--config` flag (PARTIALLY DONE 2026-04-26: vault accessors and global `--config` are wired; call-site migration still pending). Add `--config` / `$ARQUIMEDES_CONFIG` / cwd-walk. Default fallback: today's `get_project_root()`. Migrate every internal call site. Zero behavior change for current users; enables everything below. Tests for resolver precedence.
2. **Local cache split (DONE 2026-04-26).** `index.py`, `cluster.py`, `compile.py`, `memory.py`, `enrich.py`, `lint.py`, `watch.py` now resolve `indexes/search.sqlite` and per-run logs through `get_indexes_root(config)` / `get_logs_root(config)`. Default fallback is the vault root, so existing maintainer machines keep working unchanged; collaborators can opt into a separate cache by setting `ARQUIMEDES_LOCAL_CACHE` or `local_cache_root` in their config.
3. **`arq init <path>` (DONE 2026-04-26).** `vault.init_vault()` scaffolds the standard subtree (`extracted/`, `manifests/`, `derived/`, `wiki/`, `config/`), writes a minimal shared config + maintainer profile marker + `.gitignore` (excludes `indexes/`, `logs/`, `*.sqlite-shm`, `*.sqlite-wal`, local config overrides), seeds an empty `manifests/materials.jsonl`, and runs `git init` unless `--no-git` is passed. Refuses non-empty directories. Backed by `tests/test_vault_init.py`.
4. **`arq vault info` (DONE 2026-04-26).** Reports the resolved vault root, library root, local cache root, config source stack, vault git remote, and whether the search index exists. JSON by default, `--human` for short text. Resolver failure now returns an actionable error message listing the four ways to point `arq` at a vault. `MAINTAINER.md`, `docs/collaborator/setup.md`, and `docs/collaborator/agent-handbook.md` rewritten to introduce the vault concept and pipx + deploy-key flow.
5. **launchd installer using resolved config (DONE 2026-04-26).** `_arq_program_args()` in `cli.py` builds plist program args of the form `python -m arquimedes.cli [--config <vault-config>] <subcommand>`. `arq lint --install-full`, `arq watch --install`, `arq sync --install`, and `arq serve --install` all route through it, so each launchd job is pinned to whichever vault config was active when the maintainer ran `--install`. `MAINTAINER.md` updated.
6. **`arq init --from <url>` (DONE 2026-04-26).** `vault.clone_vault()` clones an existing vault repo into the target path; refuses non-empty targets, surfaces git errors as `RuntimeError`. The CLI exposes it via `arq init <path> --from <git-url>`. `docs/collaborator/setup.md` rewritten with the pipx + deploy-key + `arq init --from` flow.
7. **Migration (PENDING).** Run the `git filter-repo` recipe. Flip code repo to public. Issue deploy keys. Coordinate re-clones. Tag the code repo `v0.x`.

Steps 1–6 are reversible and non-disruptive. Step 7 is the point of no return.

## Risks

- **Hidden assumptions in `get_project_root()` callers.** Some code may assume the project root contains both code and data. Audit needed during step 1.
- **`config/maintainer/config.yaml` split.** The transition needs care so existing maintainer machines keep working through the migration. Add a back-compat shim for one release: if the resolver finds the legacy file at the old path and no `config.local.yaml` exists, log a deprecation warning and continue.
- **Wiki relative links.** `[[../shared/concept]]`-style links assume a specific tree shape. Resolver must produce the same shape under any vault root.
- **Migration-time history rewrite + visibility flip.** Force-push of a rewritten `main` is destructive. Visibility flip is one-way (revertible but disruptive). The mirror backup at step 1 of migration is mandatory.
- **Deploy key distribution.** Sending private keys is a security operation. Document a secure channel explicitly. Keys should be unique per collaborator and revocable.
- **Public code repo content.** Before flipping public, audit `docs/`, `config/`, and any committed example data for things you don't want public (institutional names, copyrighted snippets, internal URLs).

## Verification scenarios

To call this phase done:

1. `arq init /tmp/throwaway-vault` produces a vault that `arq overview` runs successfully against (empty corpus).
2. A second vault on a second machine: `ARQUIMEDES_CONFIG=~/Vaults/office/config.local.yaml arq overview` returns the office corpus, distinct from the personal vault on the first machine.
3. The current Mac Mini, after re-pointing to the new vault repo, runs the publication pipeline indistinguishably from before. `arq watch --install` produces a launchd job that publishes to the vault repo.
4. A collaborator on a fresh Windows machine, with no GitHub account: installs arq via `pipx install arquimedes`, follows the deploy-key setup, runs `arq init --from <vault-repo-url>` and `arq search "concrete"` successfully.
5. Code-only and vault-only repos both have intact history for their respective paths.
6. Removing a single deploy key on GitHub immediately denies that one collaborator's `git fetch` without affecting others.

## Out of scope

- Cross-vault federation, search, or links.
- A central registry of known vaults.
- A GUI for vault management.
- PyPI publication (use `pip install git+...` until it's worth setting up).
- Cloud-folder-as-vault. The safety asymmetry rules it out — collaborators having write access to the data tree is unacceptable.

## Decisions captured (from 2026-04-26 conversation)

- Code repo lives in this same GitHub project. **Will be made public** at migration time.
- Each vault is a **private git repo** on the maintainer's GitHub account: `<user>/arq-vault-personal`, `<user>/arq-vault-office`, …
- One vault per maintainer machine. Manual cross-vault operations via `arq --config <path>`.
- Vault data lives in a git repo, **not** in the cloud/NAS folder. (The library root — source PDFs — stays in the cloud/NAS folder, separate from the vault.)
- SQLite + runtime state lives in `~/.arquimedes/<vault-name>/` per machine, never synced.
- Code installation: `pipx install` from the public code repo. No GitHub account needed.
- Vault access for collaborators: **read-only deploy keys only.** GitHub-collaborator invitations are not part of the documented flow.
- Migration must preserve history (`git filter-repo`) for both code and data.
- Naming: "vault".
