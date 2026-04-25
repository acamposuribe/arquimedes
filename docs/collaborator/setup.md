# Arquimedes Collaborator Setup (Windows)

This guide is for Windows collaborators who want to use the Arquimedes knowledge base.

There are two paths. **Path A is the default.** Path B is only for collaborators who want offline search or want to drive `arq` from their own agent.

---

## Path A — Browser only (recommended, zero install)

The maintainer's Mac Mini serves the web UI on the local network. If you are on the same LAN, you can use Arquimedes with nothing more than a browser.

1. Open your browser.
2. Go to `http://<maintainer-hostname>.local:8420`.

Ask the maintainer for the exact hostname (they get it with `scutil --get LocalHostName`). Bookmark the URL.

That's the whole setup.

### If `<name>.local` does not resolve

Windows 10 build 1803 and newer resolve `.local` names natively, so most modern Windows installs just work. On older Windows, install **Bonjour Print Services** from Apple (free, one installer, no account). After install and reboot, `<name>.local` resolves.

If you cannot install Bonjour, ask the maintainer for the Mac Mini's LAN IP and use `http://<ip>:8420` instead. Note that DHCP-assigned IPs can change.

### Limitations of Path A

- The web UI has no login. The maintainer only exposes it on a trusted LAN.
- You cannot search while off the LAN. For offline use, follow Path B.
- You cannot drive `arq` from your own LLM agent. For that, follow Path B.

---

## Path B — Local clone (offline / agent-driven use)

This path is for collaborators who want a full local copy: search without the LAN, or run their own agent against the knowledge base.

Collaborators only need read access to the GitHub repository. Clone the canonical repo directly and keep it pointed at that upstream; you do not need GitHub write permission, a fork, or maintainer credentials.

The setup requires only one collaborator-specific choice:

- the path to the shared cloud library folder

Everything else uses sensible defaults.

### What the agent should do

If an agent is helping with setup, it should ask for only this one input:

- the absolute Windows path to the shared cloud library folder

Then it should do the rest:

1. Clone the repo to a normal local folder.
2. Create a virtual environment.
3. Install Arquimedes into that environment.
4. Write `config/collaborator/config.local.yaml` with the collaborator's `library_root`.
5. Run `arq refresh`.
6. Verify that `arq overview` works.

### Default local paths

Unless the collaborator asks for something else, use:

- repo clone: `%USERPROFILE%\Sites\arquimedes`
- virtualenv: `%USERPROFILE%\Sites\arquimedes\.venv`

Run all `arq` commands from the repo root.

### One-time install

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

### Everyday use

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

Do **not** run `arq serve` locally — the web UI is served by the maintainer (Path A). Running a second instance just confuses bookmarks.

### Important caveat

Agent-facing read commands now run the normal freshness path first, which fetches the upstream repo, restores tracked files to that upstream state, and removes untracked non-ignored files created inside the repo.

That means:

- `arq search`, `arq read`, `arq overview`, `arq figures`, and `arq annotations` try to fetch/reset/clean first and then refresh local query artifacts
- `arq refresh` is still the explicit command to use when you want a visible repo/update step before doing anything else
- collaborators can pull from the canonical repo without being able to push to it

If the repo has local tracked changes or scratch files, refresh restores the canonical upstream copy. The per-machine `config/collaborator/config.local.yaml` file and local SQLite index are gitignored and are preserved. If there is no upstream branch, refresh cannot restore from Git; use `arq refresh` to see the status clearly.

### Fast verification

After setup, all of these should work:

```powershell
.\.venv\Scripts\Activate.ps1
arq refresh
arq overview --human
arq search "concrete" --human
```

### If the agent is driving setup

A good instruction from the collaborator is:

```text
Read docs/collaborator/setup.md first. Set up this Windows machine for Arquimedes (Path B). Only ask me for the cloud library folder path unless something fails.
```
