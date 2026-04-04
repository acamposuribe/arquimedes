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
def enrich(material_id: str | None):
    """LLM enrichment: summaries, facets, descriptions (with provenance)."""
    click.echo("arq enrich: not yet implemented")


@cli.command()
@click.argument("material_id", required=False)
def extract(material_id: str | None):
    """Convenience: runs extract-raw + enrich."""
    click.echo("arq extract: not yet implemented")


@cli.command()
@click.argument("query")
@click.option("--deep", is_flag=True, help="Multi-layer auto-drill retrieval")
@click.option("--facet", multiple=True, help="Facet filter (e.g., domain=practice)")
@click.option("--collection", help="Search within a specific collection")
def search(query: str, deep: bool, facet: tuple[str, ...], collection: str | None):
    """Search the knowledge base."""
    click.echo("arq search: not yet implemented")


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
    click.echo("arq index rebuild: not yet implemented")


@index.command("ensure")
def index_ensure():
    """Rebuild search index only if stale."""
    click.echo("arq index ensure: not yet implemented")


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
