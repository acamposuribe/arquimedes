# Arquimedes Collaborator Setup

This guide is for Windows and macOS collaborators who want to use the Arquimedes knowledge base.

The system is built for collaborators' **agents** to read. The primary setup is: install `arq` once globally, then clone the **vault** (the maintainer's private knowledge-base repo) using a read-only deploy key. The agent runs `arq` commands that build a local SQLite index and answer questions from the wiki. A LAN-served web UI exists as an optional convenience for browsing visually, but it is not how the system is meant to be used day to day.

You do **not** need a GitHub account. The `arquimedes` code is installed anonymously via pipx; the vault is cloned with a per-collaborator read-only deploy key the maintainer issues to you over a secure channel.

The setup requires three collaborator-specific inputs:

- the **vault git URL** (something like `git@arq-vault:<maintainer>/arq-vault-personal.git`) — the maintainer provides it
- the **deploy key** (an SSH private key file the maintainer issues to you) — also provided by the maintainer
- the path to the **shared library folder** on this machine (the location where the source PDFs live — could be an iCloud / OneDrive / Dropbox sync folder, a NAS mount, or any other directory that appears to the OS as a normal local path; the maintainer tells you which)

Everything else uses sensible defaults.

> Naming note: the source PDFs are sometimes called the "Library" (the agent handbook refers to PDFs in `Library/`). That is the same folder this guide calls the "shared library folder" — there is no `Library/` directory inside the vault itself.

## What the agent should do

If an agent is helping with setup, it should ask for only these three inputs:

- the vault git URL
- the deploy key file (path on disk)
- the absolute path to the shared library folder on this machine

Then it should do the rest:

1. Verify prerequisites (Python 3.11+, Git, pipx, shared library folder reachable). Install missing prerequisites.
2. Install the `arquimedes` package globally with pipx.
3. Run `pipx upgrade arquimedes` so the machine has the latest collaborator tools before using `arq`.
4. Install the deploy key into `~/.ssh/` and add a `Host` alias to `~/.ssh/config` so `git clone` uses it.
5. Run `arq init <vault-path> --from <vault-git-url>` to clone the vault.
6. Write `<vault-path>/config/collaborator/config.local.yaml` with the collaborator's `library_root`.
7. Run `arq refresh`.
8. Verify that `arq overview --human` works.

The sections below are in execution order: **Prerequisites → Code install → Deploy key → Vault clone → Configuration → Verification**. End-to-end on a typical machine this takes 5–15 minutes; the install step downloads ~200 MB of Python wheels.

## Prerequisites

Both platforms need the same four things:

- **Python 3.11 or newer** (Arquimedes requires `>=3.11`; older Python will fail at install time)
- **Git** (for `git clone` and `arq refresh`)
- **pipx** (so `arq` is installed in its own isolated environment, on PATH globally)
- **The shared library folder reachable as a normal local path** (cloud-synced or network-mounted — both work, as long as the OS shows it as a regular directory)

### Windows

- Python 3.11+: install from <https://www.python.org/downloads/windows/> (check "Add python.exe to PATH"), or `winget install Python.Python.3.11`. The launcher exposes it as `py -3.11`.
- Git: install from <https://git-scm.com/download/win>, or `winget install Git.Git`.
- pipx: `py -3.11 -m pip install --user pipx && py -3.11 -m pipx ensurepath` (then close & reopen PowerShell so `pipx` is on PATH).
- Verify in a fresh PowerShell:
  ```powershell
  py -3.11 --version
  git --version
  pipx --version
  ```

### macOS

- Python 3.11+: macOS only ships an older `python3`. Install a current one with Homebrew (`brew install python@3.11`) or from <https://www.python.org/downloads/macos/>.
- Git: comes with Xcode Command Line Tools — first `git` invocation prompts to install them. Or `brew install git`.
- pipx: `brew install pipx && pipx ensurepath` (then close & reopen Terminal).
- Verify in a fresh Terminal:
  ```bash
  python3.11 --version
  git --version
  pipx --version
  ```

## Code install

`arq` is installed once per machine in its own isolated environment via pipx — there is no virtualenv to activate.

```bash
pipx install arquimedes
# or, until PyPI is set up:
pipx install git+https://github.com/<maintainer>/arquimedes.git
```

Verify:

```bash
pipx upgrade arquimedes
arq --version
```

Upgrades later: `pipx upgrade arquimedes`.

## Deploy key

The vault is a private git repo. The maintainer issues you a per-collaborator read-only **deploy key** — an SSH key registered only on the vault repo. You install the private half of the key on this machine; the maintainer keeps it revocable from the GitHub side without affecting other collaborators.

The key arrives as two files (or one private file plus the public half on a sticky note):

- `arq-vault-<name>.key` — the private key
- `arq-vault-<name>.key.pub` — the matching public key (only needed if the maintainer hasn't already added it on GitHub)

Install the private key:

```bash
# macOS / Linux
mkdir -p ~/.ssh
mv ~/Downloads/arq-vault-<name>.key ~/.ssh/arq-vault
chmod 600 ~/.ssh/arq-vault
```

```powershell
# Windows (PowerShell)
mkdir $HOME\.ssh -ErrorAction SilentlyContinue
Move-Item $HOME\Downloads\arq-vault-<name>.key $HOME\.ssh\arq-vault
icacls $HOME\.ssh\arq-vault /inheritance:r /grant:r "$($env:USERNAME):(F)"
```

Add a host alias to `~/.ssh/config` (create the file if it doesn't exist) so `git` uses this key for the vault and only for the vault:

```text
Host arq-vault
  HostName github.com
  User git
  IdentityFile ~/.ssh/arq-vault
  IdentitiesOnly yes
```

The clone URL the maintainer gives you will use `arq-vault` instead of `github.com`, e.g.:

```text
git@arq-vault:<maintainer>/arq-vault-personal.git
```

Test the key without cloning:

```bash
ssh -T git@arq-vault
# expected: "Hi <maintainer>/arq-vault-personal! You've successfully authenticated..."
```

## Vault clone

Default local paths (use these unless the collaborator asks otherwise):

- Windows — vault: `%USERPROFILE%\Vaults\arquimedes`
- macOS — vault: `~/Vaults/arquimedes`

```bash
arq init ~/Vaults/arquimedes --from git@arq-vault:<maintainer>/arq-vault-personal.git
```

```powershell
arq init "$env:USERPROFILE\Vaults\arquimedes" --from "git@arq-vault:<maintainer>/arq-vault-personal.git"
```

`arq refresh` and read commands automatically discover the vault by walking up from the current directory, so all subsequent commands should be run from inside the vault folder.

## Configuration

Inside the vault folder, create `config/collaborator/config.local.yaml` with the absolute path to the shared library folder on **this** machine. The presence of this file marks the machine as a collaborator (and enables the destructive `arq refresh` git restore — see below). The path is whatever the maintainer's storage layout maps to locally — examples:

Windows (PowerShell, single line):

```yaml
library_root: "C:/Users/<you>/<path-to-shared-folder>"
```

macOS (Terminal, single line):

```yaml
library_root: "/Users/<you>/<path-to-shared-folder>"
```

The actual path depends on how the shared folder is mounted on this machine — could be inside an iCloud / OneDrive / Dropbox sync directory, a SMB / AFP / NFS mount from a NAS, or a plain local directory. Use whatever absolute path the OS file browser shows for it.

Do not add LLM, provider, model, or agent command settings for collaborator search/read setup. Collaborator-facing commands only need the cloud library root plus the committed vault artifacts.

## Verification

From inside the vault folder, run:

```text
arq vault info --human
arq refresh
arq overview --human
arq search "concrete" --human
```

All four should succeed. Common failures:

- `arq` reported as **"command not found"** → pipx didn't add itself to PATH. Run `pipx ensurepath` and reopen the shell.
- `arq vault info` reports the **wrong vault** → you're running outside the vault folder. `cd` into it, or set `ARQUIMEDES_CONFIG=<vault>/config/collaborator/config.local.yaml`.
- `arq overview` errors with a **missing-library / path-not-found** message → the `library_root` path in `config/collaborator/config.local.yaml` is wrong, or the shared folder is not currently mounted/synced on this machine.
- `arq refresh` fails with **"Permission denied (publickey)"** → the deploy key is not loaded or the `~/.ssh/config` host alias is wrong. Re-run `ssh -T git@arq-vault` to debug.

## Everyday use

From the vault folder:

```bash
pipx upgrade arquimedes
arq refresh
arq search "thermal mass"
```

There is no virtualenv to activate — `arq` is a global pipx-installed command.

Useful read-only commands:

- `arq refresh`
- `arq overview`
- `arq search "<query>"`
- `arq read <material_id>`
- `arq figures <material_id>`
- `arq annotations <material_id>`

Do **not** run `arq serve` locally — the web UI is served by the maintainer (see "Optional: web UI" below). Running a second instance just confuses bookmarks.

## About `arq refresh` and freshness

Agent-facing read commands run the freshness path first, which fetches the upstream **vault** repo, restores tracked files to that upstream state, and removes untracked non-ignored files created inside the vault.

That means:

- `arq search`, `arq read`, `arq overview`, `arq figures`, and `arq annotations` try to fetch/reset/clean first and then refresh local query artifacts
- `arq refresh` is still the explicit command to use when you want a visible repo/update step before doing anything else
- collaborators can pull from the vault without being able to push to it
- the destructive reset only runs on collaborator machines (detected by `config/collaborator/config.local.yaml` being present and `config/maintainer/config.yaml` being absent); it never wipes maintainer or developer work

If the vault has local tracked changes or scratch files, refresh restores the canonical upstream copy. The per-machine `config/collaborator/config.local.yaml` file and the local SQLite index (under `indexes/` or your `local_cache_root`) are gitignored and are preserved. If there is no upstream branch, refresh cannot restore from Git; use `arq refresh` to see the status clearly.

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
