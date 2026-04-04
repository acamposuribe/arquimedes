"""Arquimedes CLI — `arq` command entrypoint."""

from __future__ import annotations

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
def extract_raw(material_id: str | None):
    """Deterministic extraction: text, pages, figures, tables, TOC, annotations."""
    from arquimedes.extract import extract_raw as do_extract

    click.echo("Running deterministic extraction...")
    try:
        extracted = do_extract(material_id=material_id)
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
@click.option("--annotation-limit", default=10, show_default=True, help="Max annotations per material at depth 2+.")
@click.option("--figure-limit", default=5, show_default=True, help="Max figures per material at depth 2+.")
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
@click.option("--page", type=int, help="Read a specific page")
def read(material_id: str, page: int | None):
    """Read extracted content for a material."""
    click.echo("arq read: not yet implemented")


@cli.command()
@click.argument("material_id")
def figures(material_id: str):
    """List figures with descriptions for a material."""
    click.echo("arq figures: not yet implemented")


@cli.command()
@click.option("--full", is_flag=True, help="Full rebuild instead of incremental")
def compile(full: bool):
    """Generate/update wiki from extracted materials."""
    click.echo("arq compile: not yet implemented")


@cli.command()
@click.option("--quick", is_flag=True, help="Deterministic checks only (no LLM)")
@click.option("--report", is_flag=True, help="Write report to wiki/_lint_report.md")
@click.option("--fix", is_flag=True, help="Auto-fix deterministic issues, queue LLM suggestions")
def lint(quick: bool, report: bool, fix: bool):
    """Run health checks on the knowledge base."""
    click.echo("arq lint: not yet implemented")


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
    click.echo(f"Index built in {stats.elapsed:.1f}s → indexes/search.sqlite")


@index.command("ensure")
def index_ensure():
    """Rebuild search index only if stale."""
    from arquimedes.index import ensure_index

    try:
        rebuilt, stats = ensure_index()
    except Exception as e:
        raise click.ClickException(str(e))

    if rebuilt and stats is not None:
        click.echo("Index is stale — rebuilding...")
        click.echo(f"  materials:   {stats.materials}")
        click.echo(f"  chunks:      {stats.chunks}")
        click.echo(f"  figures:     {stats.figures}")
        click.echo(f"  annotations: {stats.annotations}")
        click.echo(f"Index rebuilt in {stats.elapsed:.1f}s → indexes/search.sqlite")
    else:
        click.echo("Index is current.")


@cli.command()
def watch():
    """Start file watcher daemon (server mode)."""
    click.echo("arq watch: not yet implemented")


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
