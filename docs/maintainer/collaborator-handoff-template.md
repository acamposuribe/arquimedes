# Arquimedes Setup Handoff

Give this folder to your agent and ask it to complete the setup for you.

Use this prompt:

```text
Read Setup.md first. Use the SSH key file in this same folder. Use the vault repo URL below. Set up this machine for Arquimedes. Only ask me for the shared library folder path unless something fails.
```

Vault repo URL:

```text
git@arq-vault:<maintainer-user-or-org>/arq-vault-personal.git
```

Files in this folder:

- `Setup.md` — collaborator setup instructions
- `arq-vault-<name>.key` — SSH private key for vault access

Notes for the collaborator:

- Give the whole folder to your agent, not just the key file.
- Your agent should follow `Setup.md` and use the local key file in this folder when configuring SSH.
- The only machine-specific input your agent should need from you is the absolute path to the shared library folder on this machine, unless something fails.
