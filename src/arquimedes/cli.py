"""Arquimedes CLI — `arq` command entrypoint."""

from __future__ import annotations

import json

import click

from arquimedes import __version__


@click.group()
@click.version_option(version=__version__, prog_name="arquimedes")
def cli():
    """Arquimedes — Collaborative LLM knowledge base for architecture."""
    pass


@cli.command()
@click.argument("path", required=False)
def ingest(path: str | None):
    """Scan library for new materials and register them."""
    from arquimedes.ingest import ingest as do_ingest

    try:
        new_materials = do_ingest(path=path)
    except FileNotFoundError as e:
        raise click.ClickException(str(e))

    if new_materials:
        click.echo(f"Registered {len(new_materials)} new material(s):")
        for m in new_materials:
            click.echo(f"  {m.material_id}  {m.relative_path}  [{m.file_type}] ({m.collection})")
    else:
        click.echo("No new materials found.")


@cli.command("extract-raw")
@click.argument("material_id", required=False)
@click.option("--force", is_flag=True, help="Re-extract even if output already exists.")
def extract_raw(material_id: str | None, force: bool):
    """Deterministic extraction: text, pages, figures, tables, TOC, annotations."""
    from arquimedes.extract import extract_raw as do_extract

    click.echo("Running deterministic extraction...")
    try:
        extracted = do_extract(material_id=material_id, force=force)
    except (ValueError, FileNotFoundError) as e:
        raise click.ClickException(str(e))

    if extracted:
        click.echo(f"Extracted {len(extracted)} material(s):")
        for mid in extracted:
            click.echo(f"  {mid}")
    else:
        click.echo("Nothing to extract (all materials already extracted).")


@cli.command()
@click.argument("material_id", required=False)
@click.option("--force", is_flag=True, help="Re-enrich even if not stale.")
@click.option(
    "--stage",
    "stages",
    multiple=True,
    type=click.Choice(["document", "chunk", "figure"]),
    help="Run only specific stage(s). Repeatable. Default: all stages.",
)
@click.option("--dry-run", is_flag=True, help="Report staleness without calling LLM.")
def enrich(material_id: str | None, force: bool, stages: tuple[str, ...], dry_run: bool):
    """LLM enrichment: summaries, facets, descriptions (with provenance)."""
    from arquimedes.enrich import enrich as do_enrich
    from arquimedes.enrich_llm import EnrichmentError

    try:
        results, all_succeeded = do_enrich(
            material_id=material_id,
            force=force,
            stages=list(stages) if stages else None,
            dry_run=dry_run,
        )
    except EnrichmentError as e:
        raise click.ClickException(str(e))
    except (ValueError, FileNotFoundError) as e:
        raise click.ClickException(str(e))

    if not results:
        click.echo("Nothing to enrich (all materials up to date).")
        return

    for mid, material_result in results.items():
        title = material_result.get("title", mid)
        click.echo(f"\n{mid}  {title}")
        for stage_name in ["document", "chunk", "figure"]:
            if stage_name in material_result:
                r = material_result[stage_name]
                status = r.get("status", "?")
                detail = r.get("detail", "")
                click.echo(f"  [{stage_name}] {status}: {detail}")

    if not all_succeeded:
        raise SystemExit(1)


@cli.command()
@click.argument("material_id", required=False)
@click.option("--force", is_flag=True, help="Re-extract and re-enrich even if not stale.")
@click.option(
    "--stage",
    "stages",
    multiple=True,
    type=click.Choice(["document", "chunk", "figure"]),
    help="Run only specific enrichment stage(s). Repeatable. Default: all stages.",
)
def extract(material_id: str | None, force: bool, stages: tuple[str, ...]):
    """Convenience: runs extract-raw + enrich."""
    from arquimedes.extract import extract_raw as do_extract_raw
    from arquimedes.enrich import enrich as do_enrich
    from arquimedes.enrich_llm import EnrichmentError

    click.echo("Running deterministic extraction...")
    try:
        extracted = do_extract_raw(material_id=material_id)
    except (ValueError, FileNotFoundError) as e:
        raise click.ClickException(str(e))

    if extracted:
        click.echo(f"Extracted {len(extracted)} material(s):")
        for mid in extracted:
            click.echo(f"  {mid}")
    else:
        click.echo("Nothing to extract (all materials already extracted).")

    click.echo("Running LLM enrichment...")
    try:
        results, all_succeeded = do_enrich(
            material_id=material_id,
            force=force,
            stages=list(stages) if stages else None,
        )
    except EnrichmentError as e:
        raise click.ClickException(str(e))
    except (ValueError, FileNotFoundError) as e:
        raise click.ClickException(str(e))

    if not results:
        click.echo("Nothing to enrich (all materials up to date).")
    else:
        for mid, material_result in results.items():
            title = material_result.get("title", mid)
            click.echo(f"\n{mid}  {title}")
            for stage_name in ["document", "chunk", "figure"]:
                if stage_name in material_result:
                    r = material_result[stage_name]
                    status = r.get("status", "?")
                    detail = r.get("detail", "")
                    click.echo(f"  [{stage_name}] {status}: {detail}")

    if not all_succeeded:
        raise SystemExit(1)


@cli.command()
@click.argument("query")
@click.option("--deep", is_flag=True, help="Multi-layer retrieval (depth 2 by default).")
@click.option("--depth", type=click.IntRange(1, 3), default=None, help="Retrieval depth 1-3 (overrides --deep default of 2).")
@click.option("--facet", multiple=True, help="Facet filter: key=value or key==value (exact). Repeatable.")
@click.option("--collection", help="Search within a specific collection.")
@click.option("--limit", default=20, show_default=True, help="Max number of material cards.")
@click.option("--chunk-limit", default=5, show_default=True, help="Max chunks per material at depth 2+.")
@click.option("--annotation-limit", default=3, show_default=True, help="Max annotations per material at depth 2+.")
@click.option("--figure-limit", default=3, show_default=True, help="Max figures per material at depth 2+.")
@click.option("--concept-limit", default=3, show_default=True, help="Max concept hits per material at depth 2+.")
@click.option("--human", is_flag=True, help="Pretty-printed output (default: JSON).")
def search(
    query: str,
    deep: bool,
    depth: int | None,
    facet: tuple[str, ...],
    collection: str | None,
    limit: int,
    chunk_limit: int,
    annotation_limit: int,
    figure_limit: int,
    concept_limit: int,
    human: bool,
):
    """Search the knowledge base (JSON output by default)."""
    from arquimedes.search import search as do_search, format_human

    # Resolve effective depth
    if depth is not None:
        effective_depth = depth
    elif deep:
        effective_depth = 2
    else:
        effective_depth = 1

    try:
        result = do_search(
            query,
            depth=effective_depth,
            facets=list(facet),
            collection=collection,
            limit=limit,
            chunk_limit=chunk_limit,
            annotation_limit=annotation_limit,
            figure_limit=figure_limit,
            concept_limit=concept_limit,
        )
    except FileNotFoundError as e:
        raise click.ClickException(str(e))

    if human:
        click.echo(format_human(result))
    else:
        click.echo(result.to_json())

    if result.total == 0:
        raise SystemExit(0)


@cli.command()
@click.argument("material_id")
@click.option("--limit", default=10, show_default=True, help="Max related materials to return.")
@click.option("--human", is_flag=True, help="Pretty-printed output (default: JSON).")
def related(material_id: str, limit: int, human: bool):
    """Find materials related to MATERIAL_ID via shared concepts, keywords, facets, or authors."""
    import json as _json
    from arquimedes.search import find_related, format_related_human

    try:
        results = find_related(material_id, limit=limit)
    except FileNotFoundError as e:
        raise click.ClickException(str(e))

    if human:
        click.echo(format_related_human(material_id, results))
    else:
        click.echo(_json.dumps(
            {"material_id": material_id, "related": [r.to_dict() for r in results]},
            ensure_ascii=False, indent=2,
        ))


@cli.command()
@click.option("--min-materials", default=1, show_default=True, help="Only show concepts appearing in at least N materials.")
@click.option("--limit", default=100, show_default=True, help="Max concepts to return.")
@click.option("--human", is_flag=True, help="Pretty-printed output (default: JSON).")
def concepts(min_materials: int, limit: int, human: bool):
    """List concept candidates across the collection with material counts."""
    import json as _json
    from arquimedes.search import list_concepts, format_concepts_human

    try:
        entries = list_concepts(min_materials=min_materials, limit=limit)
    except FileNotFoundError as e:
        raise click.ClickException(str(e))

    if human:
        click.echo(format_concepts_human(entries))
    else:
        click.echo(_json.dumps(
            [e.to_dict() for e in entries],
            ensure_ascii=False, indent=2,
        ))


@cli.command()
@click.argument("material_id")
@click.option("--page", type=int, help="Read a specific page")
def read(material_id: str, page: int | None):
    """Read extracted content for a material."""
    click.echo("arq read: not yet implemented")


@cli.command()
@click.argument("material_id")
def figures(material_id: str):
    """List figures with descriptions for a material."""
    click.echo("arq figures: not yet implemented")


@cli.command("cluster")
@click.option("--force", is_flag=True, help="Re-cluster even if input is unchanged.")
def cluster_cmd(force: bool):
    """Cluster bridge concepts across materials into canonical concepts."""
    from arquimedes.cluster import cluster_bridge_concepts
    from arquimedes.enrich_llm import EnrichmentError
    from arquimedes.config import load_config

    llm_state: dict = {}

    try:
        bridge_summary = cluster_bridge_concepts(load_config(), force=force, llm_state=llm_state)
    except EnrichmentError as e:
        raise click.ClickException(str(e))
    except FileNotFoundError as e:
        raise click.ClickException(str(e))

    if bridge_summary and bridge_summary.get("skipped"):
        click.echo("Bridge clustering is up to date — skipped.")
    elif bridge_summary:
        total = bridge_summary["bridge_concepts"]
        n_clusters = bridge_summary["clusters"]
        multi = bridge_summary["multi_material"]
        click.echo(f"Bridge: {total} concepts → {n_clusters} clusters ({multi} multi-material)")


@cli.command()
@click.option("--full", is_flag=True, help="Full rebuild instead of incremental.")
@click.option("--force-cluster", is_flag=True, help="Re-run clustering before compiling.")
@click.option("--recompile-pages", is_flag=True, help="Re-render wiki pages from existing clusters without reclustering.")
def compile(full: bool, force_cluster: bool, recompile_pages: bool):
    """Compile wiki pages from enriched materials and concept clusters."""
    from arquimedes.compile import compile_wiki
    from arquimedes.enrich_llm import EnrichmentError
    from arquimedes.config import load_config

    try:
        summary = compile_wiki(
            load_config(),
            force=full,
            force_cluster=force_cluster,
            recompile_pages=recompile_pages,
        )
    except EnrichmentError as e:
        raise click.ClickException(str(e))
    except FileNotFoundError as e:
        raise click.ClickException(str(e))

    cl = summary.get("clustering", {})
    local = cl.get("local", {}) if isinstance(cl, dict) else {}
    bridge = cl.get("bridge", {}) if isinstance(cl, dict) else {}
    if local:
        if local.get("skipped"):
            click.echo("Local clustering is up to date — skipped.")
        else:
            total = local.get("total_concepts", 0)
            n_clusters = local.get("clusters", 0)
            multi = local.get("multi_material", 0)
            click.echo(f"Local: {total} concepts → {n_clusters} clusters ({multi} multi-material)")
    if bridge:
        if bridge.get("skipped"):
            click.echo("Bridge clustering is up to date — skipped.")
        else:
            total = bridge.get("bridge_concepts", 0)
            n_clusters = bridge.get("clusters", 0)
            multi = bridge.get("multi_material", 0)
            click.echo(f"Bridge: {total} concepts → {n_clusters} clusters ({multi} multi-material)")
    click.echo("Compiling:")
    click.echo(f"  {summary['material_pages']} material page(s) written, {summary['material_pages_skipped']} skipped")
    click.echo(f"  {summary['concept_pages']} concept page(s) written")
    click.echo(f"  {summary['index_pages']} index page(s) written")
    if summary["orphans_removed"]:
        click.echo(f"  {summary['orphans_removed']} orphan page(s) removed")
    click.echo("Done. wiki/ updated.")


@cli.command()
@click.option("--quick", is_flag=True, help="Deterministic checks only (no LLM)")
@click.option("--full", is_flag=True, help="Deterministic checks plus reflective LLM passes")
@click.option("--report", is_flag=True, help="Write report to wiki/_lint_report.md")
@click.option("--fix", is_flag=True, help="Auto-fix deterministic issues, queue LLM suggestions")
@click.option("--json", "as_json", is_flag=True, help="Emit machine-readable JSON")
def lint(quick: bool, full: bool, report: bool, fix: bool, as_json: bool):
    """Run health checks on the knowledge base."""
    from arquimedes.config import load_config
    from arquimedes.lint import lint_exit_code, run_lint

    try:
        result = run_lint(load_config(), quick=quick, full=full, report=report, fix=fix)
    except (ValueError, FileNotFoundError) as e:
        raise click.ClickException(str(e))
    except Exception as e:
        raise click.ClickException(str(e))

    if as_json:
        click.echo(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        det = result.get("deterministic", {})
        summary = det.get("summary", {}) if isinstance(det, dict) else {}
        click.echo(f"Deterministic lint ({result.get('mode', 'quick')}):")
        click.echo(f"  issues: {summary.get('issues', 0)}")
        click.echo(f"  high:   {summary.get('high', 0)}")
        click.echo(f"  medium: {summary.get('medium', 0)}")
        click.echo(f"  low:    {summary.get('low', 0)}")

        fixes = result.get("fixes")
        if isinstance(fixes, dict) and fixes.get("details"):
            click.echo("Fixes:")
            for item in fixes["details"]:
                click.echo(f"  - {item}")

        reflection = result.get("reflection")
        if isinstance(reflection, dict):
            click.echo("Reflective passes:")
            click.echo(f"  cluster reviews:      {reflection.get('cluster_reviews', 0)}")
            click.echo(f"  concept reflections:  {reflection.get('concept_reflections', 0)}")
            click.echo(f"  collection reflections: {reflection.get('collection_reflections', 0)}")
            click.echo(f"  graph maintenance:    {reflection.get('graph_maintenance', 0)}")

        click.echo(f"Lint report: {result.get('report_path')}")

    raise SystemExit(lint_exit_code(result))


@cli.group()
def index():
    """Manage the search index."""
    pass


@index.command("rebuild")
def index_rebuild():
    """Rebuild the search index from scratch."""
    from arquimedes.index import rebuild_index

    click.echo("Building search index...")
    try:
        stats = rebuild_index()
    except Exception as e:
        raise click.ClickException(str(e))

    click.echo(f"  materials:   {stats.materials}")
    click.echo(f"  chunks:      {stats.chunks}")
    click.echo(f"  figures:     {stats.figures}")
    click.echo(f"  annotations: {stats.annotations}")
    click.echo(f"  concepts:    {stats.concepts}")
    click.echo(f"Index built in {stats.elapsed:.1f}s → indexes/search.sqlite")


@index.command("ensure")
def index_ensure():
    """Rebuild search index and memory bridge only if stale."""
    from arquimedes.index import ensure_index_and_memory

    try:
        index_rebuilt, stats, memory_rebuilt, memory_counts = ensure_index_and_memory()
    except Exception as e:
        raise click.ClickException(str(e))

    if index_rebuilt and stats is not None:
        click.echo("Index is stale — rebuilding...")
        click.echo(f"  materials:   {stats.materials}")
        click.echo(f"  chunks:      {stats.chunks}")
        click.echo(f"  figures:     {stats.figures}")
        click.echo(f"  annotations: {stats.annotations}")
        click.echo(f"  concepts:    {stats.concepts}")
        click.echo(f"Index rebuilt in {stats.elapsed:.1f}s → indexes/search.sqlite")
    else:
        click.echo("Index is current.")

    if memory_rebuilt and not memory_counts.get("skipped"):
        click.echo("Memory bridge is stale — rebuilding...")
        click.echo(f"  clusters:              {memory_counts.get('clusters', 0)}")
        click.echo(f"  aliases:               {memory_counts.get('aliases', 0)}")
        click.echo(f"  cluster-material links:{memory_counts.get('cluster_material_links', 0)}")
        click.echo(f"  cluster relations:     {memory_counts.get('cluster_relations', 0)}")
        click.echo(f"  wiki pages:            {memory_counts.get('wiki_pages', 0)}")
        click.echo("Memory bridge rebuilt.")


@cli.command()
def watch():
    """Start file watcher daemon (server mode)."""
    click.echo("arq watch: not yet implemented")


@cli.group()
def memory():
    """Manage the memory bridge (canonical concept graph in SQLite)."""
    pass


@memory.command("rebuild")
def memory_rebuild_cmd():
    """Project canonical concept clusters and wiki paths into search.sqlite."""
    from arquimedes.memory import memory_rebuild

    click.echo("Rebuilding memory bridge...")
    try:
        counts = memory_rebuild()
    except FileNotFoundError as e:
        raise click.ClickException(str(e))

    click.echo(f"  clusters:              {counts['clusters']}")
    click.echo(f"  aliases:               {counts['aliases']}")
    click.echo(f"  cluster-material links:{counts['cluster_material_links']}")
    click.echo(f"  cluster relations:     {counts['cluster_relations']}")
    click.echo(f"  wiki pages:            {counts['wiki_pages']}")
    click.echo("Memory bridge rebuilt → indexes/search.sqlite")


@memory.command("ensure")
def memory_ensure_cmd():
    """Rebuild memory bridge only if cluster or manifest inputs changed."""
    from arquimedes.memory import memory_ensure

    try:
        rebuilt, counts = memory_ensure()
    except FileNotFoundError as e:
        raise click.ClickException(str(e))

    if rebuilt:
        click.echo("Memory bridge is stale — rebuilding...")
        click.echo(f"  clusters:              {counts['clusters']}")
        click.echo(f"  aliases:               {counts['aliases']}")
        click.echo(f"  cluster-material links:{counts['cluster_material_links']}")
        click.echo(f"  cluster relations:     {counts['cluster_relations']}")
        click.echo(f"  wiki pages:            {counts['wiki_pages']}")
        click.echo("Memory bridge rebuilt → indexes/search.sqlite")
    else:
        click.echo("Memory bridge is current.")


@cli.command()
@click.option("--install", is_flag=True, help="Install launchd service for auto-pull")
def sync(install: bool):
    """Start auto-pull daemon (collaborator mode)."""
    click.echo("arq sync: not yet implemented")


@cli.command()
@click.option("--host", default=None, help="Host to bind to")
@click.option("--port", type=int, default=None, help="Port to listen on")
def serve(host: str | None, port: int | None):
    """Start the web UI."""
    click.echo("arq serve: not yet implemented")


@cli.command()
def status():
    """Show system stats and recent additions."""
    click.echo("arq status: not yet implemented")


if __name__ == "__main__":
    cli()
