# Arquimedes Collaborator Setup (Windows)

This guide is for Windows collaborators who want a local clone with the latest wiki and search tools.

Collaborators only need read access to the GitHub repository. They should clone
the canonical repo directly and keep it pointed at that upstream; they do not
need GitHub write permission, a fork, or maintainer credentials to search the
knowledge system.

The setup should require only one collaborator-specific choice:

- the path to the shared cloud library folder

Everything else can use sensible defaults.

## What the agent should do

If an agent is helping with setup, it should ask for only this one input:

- the absolute Windows path to the shared cloud library folder

Then it should do the rest:

1. Clone the repo to a normal local folder.
2. Create a virtual environment.
3. Install Arquimedes into that environment.
4. Write `config/collaborator/config.local.yaml` with the collaborator's `library_root`.
5. Run `arq refresh`.
6. Verify that `arq overview` works.

## Default local paths

Unless the collaborator asks for something else, use:

- repo clone: `%USERPROFILE%\Sites\arquimedes`
- virtualenv: `%USERPROFILE%\Sites\arquimedes\.venv`

Run all `arq` commands from the repo root.

## One-time install

Open PowerShell and run:

```powershell
git clone <REPO_URL> "$env:USERPROFILE\Sites\arquimedes"
cd "$env:USERPROFILE\Sites\arquimedes"
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e .
```

Create `config/collaborator/config.local.yaml` with the collaborator's cloud path:

```yaml
library_root: "C:/PATH/TO/Arquimedes"
```

Do not add LLM, provider, model, or agent command settings for collaborator
search/read setup. Collaborator-facing commands only need the cloud library
root plus the committed repo artifacts.

Then run:

```powershell
.\.venv\Scripts\Activate.ps1
arq refresh
arq overview --human
```

## Everyday use

From the repo root:

```powershell
.\.venv\Scripts\Activate.ps1
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

## Important caveat

Agent-facing read commands now run the normal freshness path first, which fetches the upstream repo, restores tracked files to that upstream state, and removes untracked non-ignored files created inside the repo.

That means:

- `arq search`, `arq read`, `arq overview`, `arq figures`, and `arq annotations` try to fetch/reset/clean first and then refresh local query artifacts
- `arq refresh` is still the explicit command to use when you want a visible repo/update step before doing anything else
- collaborators can pull from the canonical repo without being able to push to it

If the repo has local tracked changes or scratch files, refresh restores the canonical upstream copy. The per-machine `config/collaborator/config.local.yaml` file and local SQLite index are gitignored and are preserved. If there is no upstream branch, refresh cannot restore from Git; use `arq refresh` to see the status clearly.

## Fast verification

After setup, all of these should work:

```powershell
.\.venv\Scripts\Activate.ps1
arq refresh
arq overview --human
arq search "concrete" --human
```

## If the agent is driving setup

A good instruction from the collaborator is:

```text
Read docs/collaborator/setup.md first. Set up this Windows machine for Arquimedes. Only ask me for the cloud library folder path unless something fails.
```
