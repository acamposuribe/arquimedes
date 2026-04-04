"""Search interface for the Arquimedes SQLite FTS5 index.

Multi-depth retrieval:
  depth 1 — material cards (default)
  depth 2 — cards + chunk summaries (--deep)
  depth 3 — cards + chunks with full text (--deep --depth 3)

Output is JSON by default; callers pass human=True for pretty-printed tables.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from arquimedes.config import get_project_root, load_config
from arquimedes.index import get_index_path


# --- Result models ---

@dataclass
class AnnotationHit:
    annotation_id: str
    type: str
    quoted_text: str
    comment: str
    page: int
    rank: int = 0

    def to_dict(self) -> dict:
        return {
            "annotation_id": self.annotation_id,
            "type": self.type,
            "quoted_text": self.quoted_text,
            "comment": self.comment,
            "page": self.page,
            "rank": self.rank,
        }


@dataclass
class FigureHit:
    figure_id: str
    description: str
    visual_type: str
    source_page: int
    image_path: str

    def to_dict(self) -> dict:
        return {
            "figure_id": self.figure_id,
            "description": self.description,
            "visual_type": self.visual_type,
            "source_page": self.source_page,
            "image_path": self.image_path,
        }


@dataclass
class ChunkHit:
    chunk_id: str
    summary: str
    source_pages: list[int]
    emphasized: bool
    content_class: str
    rank: int
    text: str = ""  # only populated at depth 3

    def to_dict(self) -> dict:
        d: dict[str, Any] = {
            "chunk_id": self.chunk_id,
            "summary": self.summary,
            "source_pages": self.source_pages,
            "emphasized": self.emphasized,
            "content_class": self.content_class,
            "rank": self.rank,
        }
        if self.text:
            d["text"] = self.text
        return d


@dataclass
class MaterialCard:
    material_id: str
    title: str
    summary: str
    domain: str
    collection: str
    document_type: str
    year: str
    authors: str
    keywords: list[str]
    rank: int
    chunks: list[ChunkHit] = field(default_factory=list)
    annotations: list[AnnotationHit] = field(default_factory=list)
    figures: list[FigureHit] = field(default_factory=list)

    def to_dict(self) -> dict:
        d: dict[str, Any] = {
            "material_id": self.material_id,
            "title": self.title,
            "summary": self.summary,
            "domain": self.domain,
            "collection": self.collection,
            "document_type": self.document_type,
            "year": self.year,
            "authors": self.authors,
            "keywords": self.keywords,
            "rank": self.rank,
        }
        if self.chunks:
            d["chunks"] = [c.to_dict() for c in self.chunks]
        if self.annotations:
            d["annotations"] = [a.to_dict() for a in self.annotations]
        if self.figures:
            d["figures"] = [f.to_dict() for f in self.figures]
        return d


@dataclass
class SearchResult:
    query: str
    depth: int
    total: int
    results: list[MaterialCard]

    def to_dict(self) -> dict:
        return {
            "query": self.query,
            "depth": self.depth,
            "total": self.total,
            "results": [r.to_dict() for r in self.results],
        }

    def to_json(self, indent: int | None = 2) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=indent)


# --- Facet handling ---

_FACET_COLUMNS = {
    "domain", "collection", "document_type", "file_type", "year",
    "building_type", "scale", "location", "jurisdiction", "climate",
    "program", "material_system", "structural_system", "historical_period",
    "course_topic", "studio_project",
}


def _parse_facet(facet_str: str) -> tuple[str, str, bool]:
    """Parse 'key=value' or 'key==value'. Returns (column, value, exact)."""
    if "==" in facet_str:
        key, _, value = facet_str.partition("==")
        return key.strip(), value.strip(), True
    key, _, value = facet_str.partition("=")
    return key.strip(), value.strip(), False


def _build_facet_where(
    facets: list[str],
    collection: str | None,
) -> tuple[str, list[str]]:
    """Build WHERE clause and params from facet list + optional collection shorthand."""
    conditions: list[str] = []
    params: list[str] = []

    all_facets = list(facets)
    if collection:
        all_facets.append(f"collection={collection}")

    for facet_str in all_facets:
        col, val, exact = _parse_facet(facet_str)
        if col not in _FACET_COLUMNS:
            continue  # silently ignore unknown facets
        if exact:
            conditions.append(f"m.{col} = ?")
            params.append(val)
        else:
            conditions.append(f"m.{col} LIKE ?")
            params.append(f"%{val}%")

    where = " AND ".join(conditions)
    return where, params


# --- Core search ---

def search(
    query: str,
    config: dict | None = None,
    *,
    depth: int = 1,
    facets: list[str] | None = None,
    collection: str | None = None,
    limit: int = 20,
    chunk_limit: int = 5,
    annotation_limit: int = 3,
    figure_limit: int = 3,
) -> SearchResult:
    """Search the index. Returns a SearchResult.

    depth=1: cards only
    depth=2: cards + chunk summaries + annotation/figure hits (content-first)
    depth=3: cards + chunks with full text + annotation/figure hits (content-first)

    At depth >= 2 the search is content-first: materials that match in chunks or
    annotations but not at the card layer are still surfaced (appended after
    card-layer matches).
    """
    if config is None:
        config = load_config()

    index_path = get_index_path(config)
    if not index_path.exists():
        raise FileNotFoundError(
            f"Search index not found at {index_path}. Run `arq index rebuild` first."
        )

    con = sqlite3.connect(f"file:{index_path}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    try:
        return _do_search(
            con, query,
            depth=depth,
            facets=facets or [],
            collection=collection,
            limit=limit,
            chunk_limit=chunk_limit,
            annotation_limit=annotation_limit,
            figure_limit=figure_limit,
        )
    finally:
        con.close()


def _row_to_card(row: sqlite3.Row, rank: int) -> MaterialCard:
    try:
        keywords = json.loads(row["keywords"] or "[]")
        if not isinstance(keywords, list):
            keywords = []
    except (json.JSONDecodeError, TypeError):
        keywords = []
    try:
        authors_list = json.loads(row["authors"] or "[]")
        authors = ", ".join(str(a) for a in authors_list) if isinstance(authors_list, list) else row["authors"]
    except (json.JSONDecodeError, TypeError):
        authors = row["authors"]
    return MaterialCard(
        material_id=row["material_id"],
        title=row["title"],
        summary=row["summary"],
        domain=row["domain"],
        collection=row["collection"],
        document_type=row["document_type"],
        year=row["year"],
        authors=authors,
        keywords=keywords,
        rank=rank,
    )


def _fetch_material_row(
    con: sqlite3.Connection,
    material_id: str,
    facet_where: str,
    facet_params: list[str],
) -> sqlite3.Row | None:
    """Fetch card columns for a single material, respecting any facet filters."""
    base = """
        SELECT m.material_id, m.title, m.summary, m.domain, m.collection,
               m.document_type, m.year, m.authors, m.keywords
        FROM materials m
        WHERE m.material_id = ?
    """
    if facet_where:
        return con.execute(base + f" AND {facet_where}", [material_id] + facet_params).fetchone()
    return con.execute(base, [material_id]).fetchone()


def _find_content_material_ids(con: sqlite3.Connection, query: str, limit: int) -> list[str]:
    """Return ordered distinct material_ids with chunk, annotation, or figure FTS matches."""
    seen: set[str] = set()
    result: list[str] = []
    for sql in (
        """SELECT DISTINCT c.material_id FROM chunks_fts
           JOIN chunks c ON chunks_fts.rowid = c.rowid
           WHERE chunks_fts MATCH ? LIMIT ?""",
        """SELECT DISTINCT a.material_id FROM annotations_fts
           JOIN annotations a ON annotations_fts.rowid = a.rowid
           WHERE annotations_fts MATCH ? LIMIT ?""",
        """SELECT DISTINCT f.material_id FROM figures_fts
           JOIN figures f ON figures_fts.rowid = f.rowid
           WHERE figures_fts MATCH ? LIMIT ?""",
    ):
        for row in con.execute(sql, [query, limit]).fetchall():
            mid = row[0]
            if mid not in seen:
                seen.add(mid)
                result.append(mid)
    return result


def _combined_priority(card: "MaterialCard") -> float:
    """Lower score = better rank. Boosts: comment hit > quoted_text hit > emphasized chunk."""
    annotation_boost = sum(
        0.8 if a.comment else 0.5
        for a in card.annotations
    )
    chunk_boost = sum(0.2 for c in card.chunks if c.emphasized)
    return card.rank - annotation_boost - chunk_boost


def _do_search(
    con: sqlite3.Connection,
    query: str,
    *,
    depth: int,
    facets: list[str],
    collection: str | None,
    limit: int,
    chunk_limit: int,
    annotation_limit: int,
    figure_limit: int,
) -> SearchResult:
    facet_where, facet_params = _build_facet_where(facets, collection)

    # --- Card-layer FTS ---
    if facet_where:
        sql = f"""
            SELECT m.material_id, m.title, m.summary, m.domain, m.collection,
                   m.document_type, m.year, m.authors, m.keywords
            FROM materials_fts
            JOIN materials m ON materials_fts.rowid = m.rowid
            WHERE materials_fts MATCH ? AND {facet_where}
            ORDER BY materials_fts.rank
            LIMIT ?
        """
        rows = con.execute(sql, [query] + facet_params + [limit]).fetchall()
    else:
        sql = """
            SELECT m.material_id, m.title, m.summary, m.domain, m.collection,
                   m.document_type, m.year, m.authors, m.keywords
            FROM materials_fts
            JOIN materials m ON materials_fts.rowid = m.rowid
            WHERE materials_fts MATCH ?
            ORDER BY materials_fts.rank
            LIMIT ?
        """
        rows = con.execute(sql, [query, limit]).fetchall()

    cards_by_id: dict[str, MaterialCard] = {}
    for i, row in enumerate(rows, 1):
        card = _row_to_card(row, i)
        cards_by_id[card.material_id] = card

    # --- Content-first: surface materials with chunk/annotation matches at depth >= 2 ---
    if depth >= 2:
        content_mids = _find_content_material_ids(con, query, limit)
        for mid in content_mids:
            if mid not in cards_by_id:
                row = _fetch_material_row(con, mid, facet_where, facet_params)
                if row:
                    rank = len(cards_by_id) + 1
                    cards_by_id[mid] = _row_to_card(row, rank)

    cards = list(cards_by_id.values())

    # --- Populate content for depth >= 2 ---
    if depth >= 2:
        for card in cards:
            card.chunks = _search_chunks(
                con, query, card.material_id, chunk_limit, include_text=(depth >= 3)
            )
            card.annotations = _search_annotations(
                con, query, card.material_id, annotation_limit
            )
            card.figures = _search_figures(
                con, query, card.material_id, figure_limit
            )

        # Rerank materials using annotation + emphasized-chunk evidence
        cards.sort(key=_combined_priority)
        for i, card in enumerate(cards, 1):
            card.rank = i

    return SearchResult(query=query, depth=depth, total=len(cards), results=cards)


def _search_chunks(
    con: sqlite3.Connection,
    query: str,
    material_id: str,
    limit: int,
    include_text: bool,
) -> list[ChunkHit]:
    # Soft emphasis boost: blend FTS rank with a modest boost for emphasized chunks.
    # FTS5 rank is a negative value (more negative = better match); subtracting 0.2
    # from an emphasized chunk's score shifts it up without overriding a strong
    # text-relevance gap between non-emphasized chunks.
    sql = """
        SELECT c.chunk_id, c.summary, c.source_pages, c.emphasized,
               c.content_class, c.text, chunks_fts.rank
        FROM chunks_fts
        JOIN chunks c ON chunks_fts.rowid = c.rowid
        WHERE chunks_fts MATCH ? AND c.material_id = ?
        ORDER BY (chunks_fts.rank - CASE WHEN c.emphasized = 1 THEN 0.2 ELSE 0.0 END)
        LIMIT ?
    """
    rows = con.execute(sql, [query, material_id, limit]).fetchall()
    hits = []
    for i, row in enumerate(rows, 1):
        try:
            source_pages = json.loads(row["source_pages"])
        except (json.JSONDecodeError, TypeError):
            source_pages = []
        hits.append(ChunkHit(
            chunk_id=row["chunk_id"],
            summary=row["summary"],
            source_pages=source_pages,
            emphasized=bool(row["emphasized"]),
            content_class=row["content_class"],
            rank=i,
            text=row["text"] if include_text else "",
        ))
    return hits


def _search_annotations(
    con: sqlite3.Connection,
    query: str,
    material_id: str,
    limit: int,
) -> list[AnnotationHit]:
    # Prefer annotations with a comment (reader intent strongest signal)
    sql = """
        SELECT a.annotation_id, a.type, a.quoted_text, a.comment, a.page
        FROM annotations_fts
        JOIN annotations a ON annotations_fts.rowid = a.rowid
        WHERE annotations_fts MATCH ? AND a.material_id = ?
        ORDER BY (CASE WHEN a.comment = '' THEN 1 ELSE 0 END), annotations_fts.rank
        LIMIT ?
    """
    rows = con.execute(sql, [query, material_id, limit]).fetchall()
    return [
        AnnotationHit(
            annotation_id=row["annotation_id"],
            type=row["type"],
            quoted_text=row["quoted_text"],
            comment=row["comment"],
            page=row["page"],
            rank=i,
        )
        for i, row in enumerate(rows, 1)
    ]


def _search_figures(
    con: sqlite3.Connection,
    query: str,
    material_id: str,
    limit: int,
) -> list[FigureHit]:
    sql = """
        SELECT f.figure_id, f.description, f.visual_type, f.source_page, f.image_path
        FROM figures_fts
        JOIN figures f ON figures_fts.rowid = f.rowid
        WHERE figures_fts MATCH ? AND f.material_id = ?
        ORDER BY figures_fts.rank
        LIMIT ?
    """
    rows = con.execute(sql, [query, material_id, limit]).fetchall()
    return [
        FigureHit(
            figure_id=row["figure_id"],
            description=row["description"],
            visual_type=row["visual_type"],
            source_page=row["source_page"],
            image_path=row["image_path"],
        )
        for row in rows
    ]


# --- Human-readable formatting ---

def format_human(result: SearchResult) -> str:
    if result.total == 0:
        return f'No results for "{result.query}"'

    lines: list[str] = []

    if result.depth == 1:
        # Table format
        lines.append(f' {"#":>2}  {"ID":<14}  {"Title":<45}  {"Domain":<10}  {"Type":<12}  {"Year"}')
        lines.append(" " + "-" * 95)
        for card in result.results:
            title = card.title[:43] + ".." if len(card.title) > 45 else card.title
            dtype = (card.document_type or "—")[:12]
            lines.append(
                f' {card.rank:>2}  {card.material_id:<14}  {title:<45}  {card.domain:<10}  {dtype:<12}  {card.year}'
            )
        lines.append("")
        lines.append(f'{result.total} result(s) for "{result.query}"')
    else:
        for card in result.results:
            lines.append(f"\n{'━' * 3} {card.rank}. {card.title} ({card.material_id}) {'━' * 3}")
            dtype = card.document_type or card.domain
            lines.append(f"    {card.domain} · {dtype} · {card.year}")
            if card.summary:
                snippet = card.summary[:120] + "…" if len(card.summary) > 120 else card.summary
                lines.append(f'    "{snippet}"')

            if card.chunks:
                lines.append("")
                lines.append("    Chunks:")
                for chunk in card.chunks:
                    pages = ",".join(str(p) for p in chunk.source_pages)
                    star = "★" if chunk.emphasized else " "
                    cls = f"[{chunk.content_class}]" if chunk.content_class else ""
                    summary = chunk.summary[:80] + "…" if len(chunk.summary) > 80 else chunk.summary
                    lines.append(f"      p.{pages:<6} {star} {cls:<18} {summary}")
                    if chunk.text:
                        excerpt = chunk.text[:200] + "…" if len(chunk.text) > 200 else chunk.text
                        lines.append(f"        {excerpt}")

            if card.annotations:
                lines.append("")
                lines.append("    Annotations:")
                for ann in card.annotations:
                    comment = f" → {ann.comment}" if ann.comment else ""
                    qt = ann.quoted_text[:80] + "…" if len(ann.quoted_text) > 80 else ann.quoted_text
                    lines.append(f'      p.{ann.page} [{ann.type}] "{qt}"{comment}')

            if card.figures:
                lines.append("")
                lines.append("    Figures:")
                for fig in card.figures:
                    desc = fig.description[:80] + "…" if len(fig.description) > 80 else fig.description
                    lines.append(f"      p.{fig.source_page} [{fig.visual_type}] {desc}")

        lines.append("")
        lines.append(f'{result.total} result(s) for "{result.query}" (depth {result.depth})')

    return "\n".join(lines)
