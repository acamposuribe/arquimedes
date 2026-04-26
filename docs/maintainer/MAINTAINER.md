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
arq mcp --install
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
- the collaborator agent handbook for future read/query sessions
- a small collaborator-facing handoff note that tells the collaborator's agent what to read and which local key file to use

Recommended flow:

1. Generate a dedicated deploy key for that collaborator.
```bash
ssh-keygen -t ed25519 -f ~/Downloads/arq-vault-<name>.key -C "arq-vault <name>"
```
2. Add `~/Downloads/arq-vault-<name>.key.pub` to the private vault repo as a GitHub deploy key, with write access disabled.
3. Create a handoff folder containing:
   `docs/collaborator/setup.md`, `docs/collaborator/agent-handbook.md`, the private key file, and a copy of `docs/maintainer/collaborator-handoff-template.md` filled in for that collaborator.
4. Send the handoff folder securely to the collaborator.

The vault clone URL given to collaborators should use the SSH host alias described in `docs/collaborator/setup.md`, for example:

```text
git@arq-vault:<user>/arq-vault-personal.git
```

The collaborator's agent should be told to read the setup guide first, use the private key file from the same handoff folder, and then treat the agent handbook as the default guide for future Arquimedes sessions. If the agent supports persistent memory, it should store a reminder to reopen the handbook at the start of future Arquimedes work. The reusable template for that note lives at `docs/maintainer/collaborator-handoff-template.md`.

If the collaborator uses an agent client without shell permissions but with MCP support, the maintainer should point them at the packaged read-only MCP server:

```text
arq-mcp --config <vault>/config/collaborator/config.local.yaml
```

That server exposes the collaborator-safe read surface without granting Bash access.

## Remote MCP for ChatGPT

If you want ChatGPT developer mode to use Arquimedes without requiring each collaborator to clone a vault locally, run `arq-mcp` as a remote streamable HTTP server and expose it through an HTTPS tunnel.

Recommended shape:

- keep the tool surface read-only
- expose the server through a stable HTTPS URL such as `https://mcp.example.com/mcp`
- use Cloudflare Tunnel for public HTTPS reachability if you already manage the domain there
- let Cloudflare Access manage OAuth for ChatGPT on the protected hostname

Configure the remote MCP in `config/maintainer/config.yaml`:

```yaml
mcp:
  transport: "streamable-http"
  host: "0.0.0.0"
  port: 8000
  streamable_http_path: "/mcp"
  keep_alive: true
```

Run in the foreground for testing:

```bash
arq mcp
```

Install the launchd job once the profile is correct:

```bash
arq mcp --install
arq mcp --status
```

Important:

- `mcp` itself stays read-only; identity and login happen at the Cloudflare Access layer
- the public MCP URL ChatGPT sees is the Cloudflare hostname, not the local bind URL
- if you want a second layer of auth for non-Cloudflare clients later, `arq-mcp` still has optional OIDC/JWT support, but ChatGPT did not need it in the working setup below

### Cloudflare Tunnel + Access runbook (tested on macOS)

This is the operator path we actually validated on the maintainer machine. It publishes the MCP over HTTPS, protects it with a self-hosted Access app, and lets ChatGPT authenticate through Cloudflare Managed OAuth.

1. Start the MCP locally and keep it bound to loopback. For first testing, running it in the foreground is fine:

```bash
arq mcp
```

The default maintainer profile should bind the MCP to `127.0.0.1:8000`.

2. Install `cloudflared` and log in once:

```bash
brew install cloudflared
cloudflared tunnel login
```

3. Create a named tunnel:

```bash
cloudflared tunnel create arquimedes-personal
```

4. Create the DNS route for the public MCP hostname:

```bash
cloudflared tunnel route dns arquimedes-personal mcp-personal.example.com
```

5. Write `~/.cloudflared/config.yml`:

```yaml
tunnel: <TUNNEL-UUID>
credentials-file: /Users/<you>/.cloudflared/<TUNNEL-UUID>.json

ingress:
  - hostname: mcp-personal.example.com
    service: http://127.0.0.1:8000
  - service: http_status:404
```

6. Test the tunnel manually:

```bash
cloudflared tunnel run arquimedes-personal
```

Smoke test from another terminal:

```bash
curl -i https://mcp-personal.example.com/mcp
```

Expected result before Access is added:

- `406 Not Acceptable`
- JSON body complaining that the client must accept `text/event-stream`

That means the public hostname reaches the real MCP server.

7. On this macOS setup, the built-in `cloudflared service install` path did not preserve the `tunnel run <name>` invocation correctly for the named tunnel. The working persistent setup was a custom user LaunchAgent:

`~/Library/LaunchAgents/com.arquimedes.cloudflared-tunnel.plist`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.arquimedes.cloudflared-tunnel</string>
  <key>ProgramArguments</key>
  <array>
    <string>/opt/homebrew/bin/cloudflared</string>
    <string>tunnel</string>
    <string>run</string>
    <string>arquimedes-personal</string>
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>StandardOutPath</key>
  <string>/Users/<you>/Library/Logs/arquimedes-cloudflared.out.log</string>
  <key>StandardErrorPath</key>
  <string>/Users/<you>/Library/Logs/arquimedes-cloudflared.err.log</string>
</dict>
</plist>
```

Load it with:

```bash
launchctl load ~/Library/LaunchAgents/com.arquimedes.cloudflared-tunnel.plist
```

Check it with:

```bash
launchctl list | grep arquimedes.cloudflared-tunnel
```

8. Add Cloudflare Access in front of the MCP hostname:

- Zero Trust -> Access / Applications -> Add an application
- Type: `Self-hosted`
- Destination: `mcp-personal.example.com`
- Policy: `Allow`
- For a personal vault, allow only the specific email addresses you want to use
- Session duration: `24 hours`

9. In that same self-hosted app, enable `Managed OAuth` and set:

- `Allowed redirect URIs`: `https://chatgpt.com/connector/oauth/*`
- keep `Allow localhost clients` on
- keep `Allow loopback clients` on
- keep `Grant session duration` as `Same as session duration`
- keep `Access token lifetime` as `Default`

10. Smoke test after Access is added:

```bash
curl -i https://mcp-personal.example.com/mcp
```

Expected result before browser login:

- `302`
- redirect to `*.cloudflareaccess.com`

Open the MCP URL once in a browser, log in through Access, then test again in that browser tab. Expected result after successful login:

- `406 Not Acceptable`

That proves:

- the tunnel works
- Access is enforcing login
- authenticated traffic reaches the real MCP server

11. Connect ChatGPT:

- ChatGPT web -> Settings -> Apps / Connectors -> add remote MCP server
- MCP URL: `https://mcp-personal.example.com/mcp`
- Authentication: `OAuth`
- ChatGPT should now autodetect OAuth, redirect through Cloudflare login, and finish connected without any manual OAuth field entry

12. Repeat the same shape for an office/org vault on its own maintainer server:

- choose a different tunnel name, such as `arquimedes-office`
- choose a different hostname, such as `mcp-office.example.com`
- point the tunnel ingress to that server's local MCP port
- in the Access policy, allow the office collaborators' identities instead of the maintainer's personal emails
- enable `Managed OAuth` on that office hostname too

Current status:

- HTTPS publication is verified
- Cloudflare Access protection is verified
- browser login through Access is verified
- ChatGPT remote MCP connection through Cloudflare Managed OAuth is verified

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
- Each maintainer machine owns exactly one vault and runs exactly one set of `arq watch / arq lint / arq serve / arq mcp` launchd jobs.
- For one-off operations against a non-resident vault from any machine, point `arq` at it manually: `arq --config /path/to/other-vault/config/maintainer/config.yaml overview`.

Do not try to install two `arq watch` launchd jobs on one machine — the labels collide.

## References

- `docs/developer/PIPELINE.md`
- `docs/developer/superpowers/specs/2026-04-25-phase9-server-agent-design.md`
- `docs/developer/superpowers/plans/2026-04-25-phase9-server-agent.md`
