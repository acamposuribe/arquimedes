# Arquimedes Agent Handbook

You are investigating a knowledge base you did **not** build. Use the **read-only MCP tools** when available. Do not modify the vault, index, wiki, or derived artifacts.

If MCP is unavailable but shell access is available, fall back to [agent-handbook-cli.md](agent-handbook-cli.md).

## Surface

Default surface: `arq-mcp`

Tool groups:

- `overview`, `list_domains_and_collections`, `recent_materials`: orient the corpus before searching
- `search`: find materials, passages, figures, annotations, clusters, and bridges
- `read`, `figures`, `annotations`: drill into one material
- `related`, `material_clusters`, `collection_clusters`, `concepts`, `materials_for_collection`, `materials_for_concept`, `list_wiki_dir`, `wiki_page_record`: traverse outward from one hit
- `refresh`: explicit freshness when you need the latest collaborator state

Prefer the smallest tool that answers the question.

CLI is fallback only: use it only if MCP is unavailable.

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

## Boundaries

Do not run maintainer workflows. If a task appears to require publishing, ingest, extraction, clustering, compile, lint, sync, or watch behavior, stop and ask the human maintainer.
