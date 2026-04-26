# Arquimedes Agent Handbook

You are investigating a knowledge base you did **not** build. Use the **read-only MCP tools** exposed by the maintainer's remote `arq-mcp` server. Do not modify the vault, index, wiki, or derived artifacts.

## Surface

Default surface: the maintainer's remote `arq-mcp` server. Connect to it from your agent client (ChatGPT developer mode, Claude desktop, etc.) using the URL the maintainer gave you. There is no local install for collaborators.

Tool groups:

- `overview`, `list_domains_and_collections`, `recent_materials`: orient the corpus before searching
- `search`: find materials, passages, figures, annotations, clusters, and bridges
- `read`, `figures`, `annotations`: drill into one material
- `related`, `material_clusters`, `collection_clusters`, `concepts`, `materials_for_collection`, `materials_for_concept`, `list_wiki_dir`, `wiki_page_record`: traverse outward from one hit
- `refresh`: explicit freshness when you need the latest collaborator state

Prefer the smallest tool that answers the question.

## Mental Model

Three layers:

- materials
- published wiki pages
- indexed search / cluster / bridge projections

Start from the index, not from raw files.

## Recipe

1. Orient with `overview`, `list_domains_and_collections`, or `recent_materials`.
2. Search with `search`.
3. Open one material with `read`.
4. Drill down only as needed with `read(detail=...)`, `figures`, or `annotations`.
5. Traverse with relation or navigation tools only after you have a concrete hit.

## Token Hygiene

- Prefer `overview` or `recent_materials` over broad search when orienting.
- Prefer `read` card output before page, chunk, or full text.
- Prefer `read(detail=chunks|figures|annotations)` before requesting bodies.
- Use navigation tools instead of repeated broad searches.
- Avoid full text unless the question truly needs wording-level evidence.

## Optional: visual browse via LAN web UI

If you are on the same local network as the maintainer machine, the maintainer also serves a read-only web UI for visual browsing of the vault — useful when you want to skim the wiki, look at figures, or click around materials without going through an agent. Open `http://<maintainer-hostname>.local:8420` in any browser (the maintainer will tell you the exact hostname; macOS and Windows 10 build 1803+ resolve `*.local` natively, older Windows needs Bonjour Print Services). The UI is unauthenticated, so it only works on the trusted LAN — there is no public version. The remote MCP remains the canonical agent surface; the web UI is just a convenience for human eyes.
