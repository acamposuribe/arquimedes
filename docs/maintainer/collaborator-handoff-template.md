# Arquimedes Setup Handoff

Give this folder to your agent and ask it to complete the setup for you.

Use this prompt:

```text
Read Setup.md first. Use the SSH key file in this same folder. Use the vault repo URL below. Run `pipx upgrade arquimedes` before using `arq`. Set up this machine for Arquimedes. After setup is complete, treat Agent Handbook.md as the default guide for future Arquimedes work. If you support persistent memory, store a reminder to reopen Agent Handbook.md at the start of every future Arquimedes session. Only ask me for the shared library folder path unless something fails.
```

Vault repo URL:

```text
git@arq-vault:<maintainer-user-or-org>/arq-vault-personal.git
```

Files in this folder:

- `Setup.md` — collaborator setup instructions
- `Agent Handbook.md` — default guide for future Arquimedes sessions
- `arq-vault-<name>.key` — SSH private key for vault access

Notes for the collaborator:

- Give the whole folder to your agent, not just the key file.
- Your agent should follow `Setup.md` and use the local key file in this folder when configuring SSH.
- After setup, your agent should keep using `Agent Handbook.md` for future Arquimedes work.
- If your local agent app cannot run shell commands but supports MCP, configure it to run `arq-mcp --config <vault>/config/collaborator/config.local.yaml` after setup. That MCP surface also includes `serve_local_ui` for the local browser UI fallback.
- The only machine-specific input your agent should need from you is the absolute path to the shared library folder on this machine, unless something fails.
