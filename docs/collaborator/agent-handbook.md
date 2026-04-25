# Arquimedes Agent Handbook

You are investigating a knowledge base you did **not** build. Your job is to answer questions using the `arq` CLI; never edit the index, wiki, or `derived/` artifacts.

**Precondition:** this handbook assumes setup is already done and the project virtualenv is activated (Windows: `.\.venv\Scripts\Activate.ps1`, macOS/Linux: `source .venv/bin/activate`) before running any `arq` command — verify with `arq overview --human`; if `arq` is not found or the command fails, stop and follow `docs/collaborator/setup.md`.

## Mental model

Three layers, all derived from PDFs in `Library/`:

- `extracted/<material_id>/` — per-material artifacts: `meta.json`, `pages.jsonl`, `chunks.jsonl`, `annotations.jsonl`, `figures/fig_*.json`, `text.md`
- `wiki/<domain>/<collection>/` — human-readable pages per material, plus `shared/` for cross-collection bridge concepts
- `indexes/search.sqlite` + `derived/*.jsonl` — FTS5 index and cluster/bridge projections

A `material_id` is a 12-char sha256 prefix. Domains are `research` and `practice`.

## Investigation recipe

1. **Orient.** `arq overview` → corpus shape and freshness stamps.
2. **Find.** `arq search "<query>"` → ranked hits across materials, chunks, figures, annotations, local clusters, global bridges.
3. **Locate a material.** `arq read <material_id>` → compact card (meta + counts + wiki path).
4. **Drill in** (in order of cost):
   - `arq read <id> --detail chunks|figures|annotations` — compact index of one aspect
   - `arq read <id> --chunk <chunk_id>` — one chunk's text
   - `arq read <id> --page <N>` — one page's text
   - `arq read <id> --full` — full `text.md` (heavy)
5. **Traverse.** `arq related <id>`, `arq material-clusters <id>`, `arq collection-clusters <domain> <collection>`, `arq concepts`.
6. **Refresh.** `arq refresh` — fetches upstream, restores tracked files to the canonical repo state, removes untracked non-ignored scratch files, and ensures index + memory are current. Other agent commands already run the same freshness path first; set `ARQ_SKIP_FRESHNESS=1` to opt out.

## Command quick reference (read-only)

| Command | Purpose |
|---|---|
| `arq overview` | Corpus counts, collections, derived-artifact stamps |
| `arq search <q>` | FTS ranked hits across all layers |
| `arq read <id>` | Material card; `--page/--chunk/--full/--detail` drill-in |
| `arq figures <id>` | Figure index; `--visual-type`, `--figure <id>` |
| `arq annotations <id>` | Reader highlights/notes; `--page`, `--type` |
| `arq related <id>` | Materials linked via concepts/keywords/authors |
| `arq material-clusters <id>` | Local clusters this material belongs to |
| `arq collection-clusters <domain> <collection>` | Local clusters in a collection |
| `arq concepts` | Concept candidates across the corpus |
| `arq refresh` | Restore canonical repo state + ensure index/memory |

Every command emits JSON by default; add `--human` for short human-readable text. Exit code is non-zero on unambiguous error (missing id, bad flag combo) and zero with an empty-but-valid result when a query matches nothing.

## Token hygiene

- Start with `--detail <aspect>` before asking for full page/chunk text.
- `arq search` already returns compact snippets — you rarely need `--full`.
- Filter figures with `--visual-type` and annotations with `--page`/`--type`.

## Web UI

The maintainer machine serves the web UI on the local network at `http://<maintainer-hostname>.local:8420`. If the human collaborator wants to browse visually, point them there — do not start a local server yourself.

## Maintainer-only commands — do not call - out of bounds!

`arq ingest`, `arq extract`, `arq extract-raw`, `arq enrich`, `arq cluster`, `arq compile`, `arq memory`, `arq lint`, `arq index`, `arq watch`, `arq sync`, `arq serve`. These mutate artifacts or kick off long-running pipelines. If you think one is needed, ask the human maintainer. (`arq serve` is read-only but long-running and is owned by the maintainer's launchd job — never start a second instance.)