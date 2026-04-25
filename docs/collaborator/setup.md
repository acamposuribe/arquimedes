# Arquimedes Collaborator Setup

This guide is for Windows and macOS collaborators who want to use the Arquimedes knowledge base.

The system is built for collaborators' **agents** to read. The primary setup is a local clone of the repo: the agent runs `arq` commands that build a local SQLite index and answer questions from the wiki. A LAN-served web UI exists as an optional convenience for browsing visually, but it is not how the system is meant to be used day to day.

Collaborators only need read access to the GitHub repository. Clone the canonical repo directly and keep it pointed at that upstream; you do not need GitHub write permission, a fork, or maintainer credentials. The maintainer provides the canonical Git URL (referred to as `<REPO_URL>` below) — if the collaborator doesn't have it, ask them or the maintainer.

The setup requires only one collaborator-specific choice:

- the path to the **shared library folder** on this machine (the location where the source PDFs live — could be an iCloud / OneDrive / Dropbox sync folder, a NAS mount, or any other directory that appears to the OS as a normal local path; the maintainer tells you which)

Everything else uses sensible defaults.

> Naming note: the source PDFs are sometimes called the "Library" (the agent handbook refers to PDFs in `Library/`). That is the same folder this guide calls the "shared library folder" — there is no `Library/` directory inside the repo itself.

## What the agent should do

If an agent is helping with setup, it should ask for only this one input:

- the absolute path to the shared library folder on this machine

Then it should do the rest:

1. Verify prerequisites (Python 3.11+, Git, shared library folder reachable). Install missing prerequisites.
2. Clone the repo (sparse-checkout) to a normal local folder.
3. Create a virtual environment.
4. Install Arquimedes into that environment.
5. Write `config/collaborator/config.local.yaml` with the collaborator's `library_root`.
6. Run `arq refresh`.
7. Verify that `arq overview --human` works.

The sections below are in execution order: **Prerequisites → One-time install → Configuration → Verification**. End-to-end on a typical machine this takes 5–15 minutes; the install step downloads ~200 MB of Python wheels.

## Prerequisites

Both platforms need the same three things:

- **Python 3.11 or newer** (Arquimedes requires `>=3.11`; older Python will fail at install time)
- **Git** (for `git clone` and `arq refresh`)
- **The shared library folder reachable as a normal local path** (cloud-synced or network-mounted — both work, as long as the OS shows it as a regular directory)

### Windows

- Python 3.11+: install from <https://www.python.org/downloads/windows/> (check "Add python.exe to PATH"), or `winget install Python.Python.3.11`. The launcher exposes it as `py -3.11`.
- Git: install from <https://git-scm.com/download/win>, or `winget install Git.Git`.
- Verify in a fresh PowerShell:
  ```powershell
  py -3.11 --version
  git --version
  ```

### macOS

- Python 3.11+: macOS only ships an older `python3`. Install a current one with Homebrew (`brew install python@3.11`) or from <https://www.python.org/downloads/macos/>.
- Git: comes with Xcode Command Line Tools — first `git` invocation prompts to install them. Or `brew install git`.
- Verify in a fresh Terminal:
  ```bash
  python3.11 --version
  git --version
  ```

## One-time install

The clone uses Git **sparse-checkout** so only the directories collaborators actually need are materialized on disk. Excluded: `tests/`, `docs/developer/`, `docs/maintainer/`, `.github/`, `ops/`, `logs/`. Included: `src/`, `config/`, `wiki/`, `extracted/`, `derived/`, `indexes/`, `manifests/`, `docs/collaborator/`, plus the root-level files Git always keeps.

Default local paths (use these unless the collaborator asks otherwise):

- Windows — repo clone: `%USERPROFILE%\Sites\arquimedes`, virtualenv: `…\arquimedes\.venv`
- macOS — repo clone: `~/Sites/arquimedes`, virtualenv: `~/Sites/arquimedes/.venv`

Run all `arq` commands from the repo root.

### Windows (PowerShell)

```powershell
git clone --no-checkout <REPO_URL> "$env:USERPROFILE\Sites\arquimedes"
cd "$env:USERPROFILE\Sites\arquimedes"
git sparse-checkout init --cone
git sparse-checkout set src config wiki extracted derived indexes manifests docs/collaborator
git checkout main

py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e .
```

### macOS (Terminal)

```bash
git clone --no-checkout <REPO_URL> ~/Sites/arquimedes
cd ~/Sites/arquimedes
git sparse-checkout init --cone
git sparse-checkout set src config wiki extracted derived indexes manifests docs/collaborator
git checkout main

python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .
```

`git pull` and `arq refresh` continue to work normally — sparse-checkout is transparent to the rest of Git. To later add or remove paths, edit the `git sparse-checkout set ...` line and re-run it.

## Configuration

Create `config/collaborator/config.local.yaml` with the absolute path to the shared library folder on **this** machine. The path is whatever the maintainer's storage layout maps to locally — examples:

Windows (PowerShell, single line):

```yaml
library_root: "C:/Users/<you>/<path-to-shared-folder>"
```

macOS (Terminal, single line):

```yaml
library_root: "/Users/<you>/<path-to-shared-folder>"
```

The actual path depends on how the shared folder is mounted on this machine — could be inside an iCloud / OneDrive / Dropbox sync directory, a SMB / AFP / NFS mount from a NAS, or a plain local directory. Use whatever absolute path the OS file browser shows for it.

Do not add LLM, provider, model, or agent command settings for collaborator search/read setup. Collaborator-facing commands only need the cloud library root plus the committed repo artifacts.

## Verification

Still in the same shell (venv active from the install step), run:

```text
arq refresh
arq overview --human
arq search "concrete" --human
```

All three should succeed. Common failures:

- `arq` reported as **"command not found"** → the venv is not activated, or `pip install -e .` failed. Re-run activation, or re-run install.
- `arq overview` errors with a **missing-library / path-not-found** message → the `library_root` path in `config/collaborator/config.local.yaml` is wrong, or the shared folder is not currently mounted/synced on this machine.

## Everyday use

From the repo root, activate the venv and run an `arq` command:

```powershell
# Windows
.\.venv\Scripts\Activate.ps1
arq refresh
arq search "thermal mass"
```

```bash
# macOS
source .venv/bin/activate
arq refresh
arq search "thermal mass"
```

Useful read-only commands:

- `arq refresh`
- `arq overview`
- `arq search "<query>"`
- `arq read <material_id>`
- `arq figures <material_id>`
- `arq annotations <material_id>`

Do **not** run `arq serve` locally — the web UI is served by the maintainer (see "Optional: web UI" below). Running a second instance just confuses bookmarks.

## About `arq refresh` and freshness

Agent-facing read commands now run the normal freshness path first, which fetches the upstream repo, restores tracked files to that upstream state, and removes untracked non-ignored files created inside the repo.

That means:

- `arq search`, `arq read`, `arq overview`, `arq figures`, and `arq annotations` try to fetch/reset/clean first and then refresh local query artifacts
- `arq refresh` is still the explicit command to use when you want a visible repo/update step before doing anything else
- collaborators can pull from the canonical repo without being able to push to it

If the repo has local tracked changes or scratch files, refresh restores the canonical upstream copy. The per-machine `config/collaborator/config.local.yaml` file and local SQLite index are gitignored and are preserved. If there is no upstream branch, refresh cannot restore from Git; use `arq refresh` to see the status clearly.

## If the agent is driving setup

A good instruction from the collaborator is:

```text
Read docs/collaborator/setup.md first. Set up this machine for Arquimedes. Only ask me for the shared library folder path unless something fails.
```

---

## Optional: web UI

For visual browsing (no agent), the maintainer's Mac Mini also serves the read-only web UI on the local network. If you are on the same LAN, open:

```
http://<maintainer-hostname>.local:8420
```

Ask the maintainer for the exact hostname. On Windows 10 build 1803+ `.local` resolves natively; on older Windows install Apple's free **Bonjour Print Services**, or ask the maintainer for the LAN IP and use `http://<ip>:8420`.

The web UI has no login and is only exposed on the trusted LAN. It is a convenience, not the primary interface — the agent-driven local clone above is the main way to use Arquimedes.
