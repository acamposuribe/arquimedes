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

The maintainer machine is the only semantic publisher and the only host of the published knowledge base. It owns ingest, extraction, clustering, compile, lint, memory rebuild, commits, and pushes. It also publishes the read-only `arq-mcp` server that collaborators consume remotely.

Collaborators do not clone the vault. Their only contact with the system is the remote MCP, gated by Cloudflare Access.

(The collaborator-clone path — destructive `git fetch && git reset --hard && git clean -fd` triggered by the presence of `config/collaborator/config.local.yaml` — is legacy code still present in `freshness.py` for backwards compatibility and will be retired.)

## Cadence

- Every 30 minutes: `arq watch --once` scans the shared library and publishes one batch if anything changed.
- Daily at 02:00: `arq lint --full --commit-push` runs reflective maintenance, including `global-bridge`, then commits and pushes any changed artifacts.
- Continuously: `arq mcp` (launchd) serves the read-only MCP on loopback; `cloudflared` (launchd) tunnels it to the public hostname behind Cloudflare Access.

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

Collaborators no longer clone the vault. The maintainer is the only writer and the only host of the published knowledge base; collaborators consume it as a remote MCP from their agent client (ChatGPT, Claude desktop, etc.). Onboarding is therefore an Access-policy update plus a one-line handoff:

1. In Cloudflare Zero Trust → Access → Applications, open the self-hosted app for `mcp.<your-domain>` and add the collaborator's email to the existing Allow policy. Save.
2. Send the collaborator two things:
   - the public MCP URL (e.g. `https://mcp.example.com/mcp`)
   - a pointer to `docs/collaborator/agent-handbook.md` so their agent knows how to use the read-only tool surface

That's it. There is no key, no clone, no local install, no `config.local.yaml` for collaborators. First connection from their agent triggers Cloudflare's OAuth flow against their email; subsequent calls reuse the session.

If you ever need to revoke a collaborator, remove their email from the Access policy — their existing session expires within the configured `Session duration` (24 h by default).

## Remote MCP for ChatGPT

The remote MCP is now the canonical collaborator surface. Run `arq-mcp` as a streamable HTTP server bound to loopback and expose it over HTTPS through Cloudflare Tunnel + Access.

Recommended shape:

- keep the tool surface read-only
- expose the server through a stable HTTPS URL such as `https://mcp.example.com/mcp`
- use Cloudflare Tunnel for public HTTPS reachability if you already manage the domain there
- let Cloudflare Access manage OAuth for ChatGPT on the protected hostname

Configure the remote MCP in `config/maintainer/config.yaml`:

```yaml
mcp:
  transport: "streamable-http"
  host: "127.0.0.1"
  port: 8000
  streamable_http_path: "/mcp"
  keep_alive: true
  # Required when fronting the MCP with a reverse proxy. The MCP SDK
  # auto-enables DNS-rebinding protection on loopback binds and rejects any
  # Host header other than 127.0.0.1/localhost with `421 Invalid Host header`.
  # List the public hostname here so the tunnel-forwarded request is accepted.
  allowed_hosts:
    - "mcp.example.com"
  allowed_origins:
    - "https://mcp.example.com"
    - "https://chatgpt.com"
  cloudflare_tunnel:
    enabled: true
    tunnel_name: "arquimedes-example"
    binary_path: "/opt/homebrew/bin/cloudflared"
```

If you cannot edit the maintainer config (or want a defense-in-depth fallback), the cloudflared ingress block also accepts `originRequest.httpHostHeader: 127.0.0.1:8000`, which rewrites the inner Host header before it reaches the MCP. Either approach makes the 421 go away; prefer `allowed_hosts` so the server's own config documents which hostnames are legitimate.

Run in the foreground for testing:

```bash
arq mcp
```

Install the launchd job once the profile is correct:

```bash
arq mcp --install
arq mcp --status
```

If `mcp.cloudflare_tunnel.enabled` is true, `arq mcp --install` also installs the `cloudflared tunnel run <name>` LaunchAgent automatically.

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
cloudflared tunnel create arquimedes-example
```

4. Create the DNS route for the public MCP hostname:

```bash
cloudflared tunnel route dns arquimedes-example mcp.example.com
```

5. Write `~/.cloudflared/config.yml`:

```yaml
tunnel: <TUNNEL-UUID>
credentials-file: /Users/<you>/.cloudflared/<TUNNEL-UUID>.json

ingress:
  - hostname: mcp.example.com
    service: http://127.0.0.1:8000
  - service: http_status:404
```

This works as long as `mcp.allowed_hosts` in `config/maintainer/config.yaml` lists `mcp.example.com` (see the MCP block above). If the `allowed_hosts` entry is missing, the MCP SDK's DNS-rebinding protection will reject the tunnel-forwarded request with `421 Invalid Host header` — add `originRequest.httpHostHeader: 127.0.0.1:8000` under the ingress as a quick workaround, but the canonical fix is to keep the allowlist on the server side.

6. Test the tunnel manually:

```bash
cloudflared tunnel run arquimedes-example
```

Smoke test from another terminal:

```bash
curl -i https://mcp.example.com/mcp
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
    <string>arquimedes-example</string>
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
- Destination: `mcp.example.com`
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
curl -i https://mcp.example.com/mcp
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
- MCP URL: `https://mcp.example.com/mcp`
- Authentication: `OAuth`
- ChatGPT should now autodetect OAuth, redirect through Cloudflare login, and finish connected without any manual OAuth field entry

12. Repeat the same shape for an office/org vault on its own maintainer server:

- choose a different tunnel name, such as `arquimedes-office`
- choose a different hostname, such as `mcp-office.example.com` or your real office hostname
- point the tunnel ingress to that server's local MCP port
- in the Access policy, allow the office collaborators' identities instead of the maintainer's personal emails
- enable `Managed OAuth` on that office hostname too

Current status:

- HTTPS publication is verified
- Cloudflare Access protection is verified
- browser login through Access is verified
- ChatGPT remote MCP connection through Cloudflare Managed OAuth is verified

## Publication Cycle

The daytime cycle is intentionally narrow:

```text
scan library -> ingest -> extract -> index rebuild -> compile -> commit/push
```

Reflective lint stages do not run in the daytime cycle. `global-bridge` runs only through the nightly `arq lint --full` job.

## LAN Web UI

Optional local browse UI for the maintainer (and anyone trusted on the same LAN). Collaborators do not use this — they connect via the remote MCP. With `serve.host: 0.0.0.0` and `serve.port: 8420` in `config/maintainer/config.yaml`, `arq serve --install` registers a `KeepAlive` launchd job (`com.arquimedes.serve`) that stays up across reboots and restarts on crash.

Find the hostname:

```bash
scutil --get LocalHostName
```

That name resolves over mDNS as `<name>.local` from any client on the same LAN. Open `http://<name>.local:8420` in a browser.

First run: macOS will prompt once to allow incoming connections for the Python binary. Approve it. If `serve` does not appear on the LAN, check System Settings → Network → Firewall.

Manage the job:

```bash
arq serve --status
arq serve --uninstall
```

Caveats:

- The web UI has no authentication. Only expose it on a trusted LAN. Do not port-forward to the public internet.
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
