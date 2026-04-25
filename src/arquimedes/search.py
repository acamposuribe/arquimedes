"""Search interface for the Arquimedes SQLite FTS5 index.

Multi-depth retrieval:
  depth 1 — material cards (default)
  depth 2 — cards + chunk summaries (--deep)
  depth 3 — cards + chunks with full text (--deep --depth 3)

Output is JSON by default; callers pass human=True for pretty-printed tables.
"""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from arquimedes.classify import STOP_WORDS
from arquimedes.config import get_project_root, load_config
from arquimedes.index import get_index_path


# --- Result models ---

@dataclass
class ConceptHit:
    concept_name: str
    relevance: str
    source_pages: list[int] = field(default_factory=list)
    evidence_spans: list[str] = field(default_factory=list)
    confidence: float = 0.0
    rank: int = 0

    def to_dict(self) -> dict:
        d: dict[str, Any] = {
            "concept_name": self.concept_name,
            "relevance": self.relevance,
            "rank": self.rank,
        }
        if self.source_pages:
            d["source_pages"] = self.source_pages
        if self.evidence_spans:
            d["evidence_spans"] = self.evidence_spans
        if self.confidence:
            d["confidence"] = self.confidence
        return d


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
class CanonicalClusterHit:
    cluster_id: str
    canonical_name: str
    slug: str
    aliases: list[str]
    material_count: int
    wiki_path: str
    summary: str = ""
    domain: str = ""
    collection: str = ""

    def to_dict(self) -> dict:
        d: dict[str, Any] = {
            "cluster_id": self.cluster_id,
            "canonical_name": self.canonical_name,
            "slug": self.slug,
            "material_count": self.material_count,
        }
        if self.aliases:
            d["aliases"] = self.aliases
        if self.wiki_path:
            d["wiki_path"] = self.wiki_path
        if self.summary:
            d["summary"] = self.summary
        if self.domain:
            d["domain"] = self.domain
        if self.collection:
            d["collection"] = self.collection
        return d


@dataclass
class CollectionPageHit:
    domain: str
    collection: str
    title: str
    wiki_path: str
    material_count: int
    summary: str = ""

    def to_dict(self) -> dict:
        d: dict[str, Any] = {
            "domain": self.domain,
            "collection": self.collection,
            "title": self.title,
            "wiki_path": self.wiki_path,
            "material_count": self.material_count,
        }
        if self.summary:
            d["summary"] = self.summary
        return d


@dataclass
class GlobalBridgeHit:
    bridge_id: str
    canonical_name: str
    slug: str
    descriptor: str
    aliases: list[str]
    material_count: int
    wiki_path: str
    summary: str = ""

    def to_dict(self) -> dict:
        d: dict[str, Any] = {
            "bridge_id": self.bridge_id,
            "canonical_name": self.canonical_name,
            "slug": self.slug,
            "material_count": self.material_count,
        }
        if self.descriptor:
            d["descriptor"] = self.descriptor
        if self.aliases:
            d["aliases"] = self.aliases
        if self.wiki_path:
            d["wiki_path"] = self.wiki_path
        if self.summary:
            d["summary"] = self.summary
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
    concepts: list[ConceptHit] = field(default_factory=list)

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
        if self.concepts:
            d["concepts"] = [c.to_dict() for c in self.concepts]
        return d


@dataclass
class SearchResult:
    query: str
    depth: int
    total: int
    results: list[MaterialCard]
    collection_pages: list[CollectionPageHit] = field(default_factory=list)
    canonical_clusters: list[CanonicalClusterHit] = field(default_factory=list)
    global_bridges: list[GlobalBridgeHit] = field(default_factory=list)

    def to_dict(self) -> dict:
        d: dict[str, Any] = {
            "query": self.query,
            "depth": self.depth,
            "total": self.total,
            "results": [r.to_dict() for r in self.results],
        }
        if self.collection_pages:
            d["collection_pages"] = [c.to_dict() for c in self.collection_pages]
        if self.canonical_clusters:
            d["canonical_clusters"] = [c.to_dict() for c in self.canonical_clusters]
        if self.global_bridges:
            d["global_bridges"] = [b.to_dict() for b in self.global_bridges]
        return d

    def to_json(self, indent: int | None = 2) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=indent)


@dataclass
class MaterialEvidence:
    query: str
    material_id: str
    depth: int
    chunks: list[ChunkHit] = field(default_factory=list)
    annotations: list[AnnotationHit] = field(default_factory=list)
    figures: list[FigureHit] = field(default_factory=list)
    concepts: list[ConceptHit] = field(default_factory=list)

    @property
    def has_hits(self) -> bool:
        return bool(self.chunks or self.annotations or self.figures or self.concepts)

    def to_dict(self) -> dict:
        d: dict[str, Any] = {
            "query": self.query,
            "material_id": self.material_id,
            "depth": self.depth,
        }
        if self.chunks:
            d["chunks"] = [c.to_dict() for c in self.chunks]
        if self.annotations:
            d["annotations"] = [a.to_dict() for a in self.annotations]
        if self.figures:
            d["figures"] = [f.to_dict() for f in self.figures]
        if self.concepts:
            d["concepts"] = [c.to_dict() for c in self.concepts]
        return d


# --- FTS query sanitization ---

_FTS5_SPECIAL = re.compile(r'["\(\)\*\:\^]')


def safe_fts_query(query: str) -> str:
    """Sanitize and expand a user query for FTS5 MATCH.

    - Empty → empty phrase match.
    - Contains FTS5 special chars or bare AND/OR/NOT → phrase search (quoted).
    - Single token → passed through as-is.
    - Multiple plain tokens → stop words filtered out, remainder OR-expanded.
      BM25 naturally ranks full-match documents highest.
      e.g. "buildings for the thermal mass" → "buildings OR thermal OR mass"
    - If filtering removes all tokens → fall back to phrase search on original.
    """
    stripped = query.strip()
    if not stripped:
        return '""'
    # Fall back to phrase search when query contains FTS5 syntax chars or operators
    if _FTS5_SPECIAL.search(stripped) or re.search(r'\b(AND|OR|NOT)\b', stripped):
        escaped = stripped.replace('"', '""')
        return f'"{escaped}"'
    tokens = stripped.split()
    if len(tokens) == 1:
        return stripped
    meaningful = [t for t in tokens if t.lower() not in STOP_WORDS]
    if not meaningful:
        # All tokens were stop words — phrase search on the original
        escaped = stripped.replace('"', '""')
        return f'"{escaped}"'
    return " OR ".join(meaningful)


def _safe_fts_query(query: str) -> str:
    return safe_fts_query(query)


def _parse_json_list(value: str | None) -> list[str]:
    if not value:
        return []
    try:
        parsed = json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item).strip() for item in parsed if str(item).strip()]


def _summary_from_reflection(
    why_this_matters: str | None,
    main_takeaways: str | None,
    main_tensions: str | None,
    open_questions: str | None,
) -> str:
    for candidate in (
        str(why_this_matters or "").strip(),
        *(_parse_json_list(main_takeaways)[:1]),
        *(_parse_json_list(main_tensions)[:1]),
        *(_parse_json_list(open_questions)[:1]),
    ):
        if candidate:
            return candidate
    return ""


def _like_pattern(query: str) -> str:
    return f"%{(query or '').strip()}%"


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
    scope: str = "all",
    material_ids: list[str] | None = None,
    facets: list[str] | None = None,
    collection: str | None = None,
    limit: int = 20,
    chunk_limit: int = 5,
    annotation_limit: int = 3,
    figure_limit: int = 3,
    concept_limit: int = 3,
) -> SearchResult:
    """Search the index. Returns a SearchResult.

    depth=1: cards only
    depth=2: cards + chunk summaries + annotation/figure hits (content-first)
    depth=3: cards + chunks with full text + annotation/figure hits (content-first)

    At depth >= 2 the search is content-first: materials that match in chunks,
    annotations, or figures but not at the card layer are still surfaced
    (appended after card-layer matches).
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
            scope=scope,
            material_ids=material_ids or [],
            facets=facets or [],
            collection=collection,
            limit=limit,
            chunk_limit=chunk_limit,
            annotation_limit=annotation_limit,
            figure_limit=figure_limit,
            concept_limit=concept_limit,
        )
    finally:
        con.close()


def search_material_evidence(
    query: str,
    material_id: str,
    config: dict | None = None,
    *,
    depth: int = 3,
    chunk_limit: int = 5,
    annotation_limit: int = 3,
    figure_limit: int = 3,
    concept_limit: int = 3,
) -> MaterialEvidence:
    """Return search evidence scoped to a single material."""
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
        row = _fetch_material_row(con, material_id, "", [], [])
        if row is None:
            raise FileNotFoundError(f"Material {material_id} is not present in the search index.")
        fts_query = _safe_fts_query(query)
        return MaterialEvidence(
            query=query,
            material_id=material_id,
            depth=depth,
            chunks=_search_chunks(con, fts_query, material_id, chunk_limit, include_text=(depth >= 3)),
            annotations=_search_annotations(con, fts_query, material_id, annotation_limit),
            figures=_search_figures(con, fts_query, material_id, figure_limit),
            concepts=_search_concepts(con, fts_query, material_id, concept_limit),
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
    scoped_material_ids: list[str],
) -> sqlite3.Row | None:
    """Fetch card columns for a single material, respecting any facet filters."""
    base = """
        SELECT m.material_id, m.title, m.summary, m.domain, m.collection,
               m.document_type, m.year, m.authors, m.keywords
        FROM materials m
        WHERE m.material_id = ?
    """
    params = [material_id]
    if scoped_material_ids:
        placeholders = ",".join("?" for _ in scoped_material_ids)
        base += f" AND m.material_id IN ({placeholders})"
        params.extend(scoped_material_ids)
    if facet_where:
        return con.execute(base + f" AND {facet_where}", params + facet_params).fetchone()
    return con.execute(base, params).fetchone()


def _find_content_material_ids(con: sqlite3.Connection, query: str, limit: int, scoped_material_ids: list[str]) -> list[str]:
    """Return ordered distinct material_ids with chunk, annotation, figure, or concept FTS matches."""
    seen: set[str] = set()
    result: list[str] = []
    scope_sql = ""
    scope_params: list[str] = []
    if scoped_material_ids:
        placeholders = ",".join("?" for _ in scoped_material_ids)
        scope_sql = f" AND {{alias}}.material_id IN ({placeholders})"
        scope_params = list(scoped_material_ids)
    for sql, alias in (
        (
            """SELECT DISTINCT c.material_id FROM chunks_fts
               JOIN chunks c ON chunks_fts.rowid = c.rowid
               WHERE chunks_fts MATCH ?{scope} LIMIT ?""",
            "c",
        ),
        (
            """SELECT DISTINCT a.material_id FROM annotations_fts
               JOIN annotations a ON annotations_fts.rowid = a.rowid
               WHERE annotations_fts MATCH ?{scope} LIMIT ?""",
            "a",
        ),
        (
            """SELECT DISTINCT f.material_id FROM figures_fts
               JOIN figures f ON figures_fts.rowid = f.rowid
               WHERE figures_fts MATCH ?{scope} LIMIT ?""",
            "f",
        ),
        (
            """SELECT DISTINCT co.material_id FROM concepts_fts
               JOIN concepts co ON concepts_fts.rowid = co.rowid
               WHERE concepts_fts MATCH ?{scope} LIMIT ?""",
            "co",
        ),
    ):
        rendered_sql = sql.format(scope=scope_sql.format(alias=alias) if scope_sql else "")
        for row in con.execute(rendered_sql, [query, *scope_params, limit]).fetchall():
            mid = row[0]
            if mid not in seen:
                seen.add(mid)
                result.append(mid)
    return result


def _combined_priority(card: "MaterialCard") -> float:
    """Lower score = better rank. Boosts: comment hit > quoted_text hit > concept match > emphasized chunk."""
    annotation_boost = sum(
        0.8 if a.comment else 0.5
        for a in card.annotations
    )
    chunk_boost = sum(0.2 for c in card.chunks if c.emphasized)
    concept_boost = sum(0.3 for _ in card.concepts)
    return card.rank - annotation_boost - chunk_boost - concept_boost


def _do_search(
    con: sqlite3.Connection,
    query: str,
    *,
    depth: int,
    scope: str,
    material_ids: list[str],
    facets: list[str],
    collection: str | None,
    limit: int,
    chunk_limit: int,
    annotation_limit: int,
    figure_limit: int,
    concept_limit: int,
) -> SearchResult:
    fts_query = _safe_fts_query(query)
    normalized_scope = (scope or "all").strip().lower()
    if normalized_scope not in {"all", "title", "author"}:
        normalized_scope = "all"
    materials_query = fts_query
    if normalized_scope == "title":
        materials_query = f"title : ({fts_query})"
    elif normalized_scope == "author":
        materials_query = f"authors : ({fts_query})"
    facet_where, facet_params = _build_facet_where(facets, collection)
    scoped_material_ids = [mid for mid in material_ids if str(mid).strip()]
    material_scope_where = ""
    material_scope_params: list[str] = []
    if scoped_material_ids:
        placeholders = ",".join("?" for _ in scoped_material_ids)
        material_scope_where = f"m.material_id IN ({placeholders})"
        material_scope_params = scoped_material_ids
    combined_where_parts = [part for part in (facet_where, material_scope_where) if part]
    combined_where = " AND ".join(combined_where_parts)
    combined_params = facet_params + material_scope_params

    # --- Card-layer FTS ---
    if combined_where:
        sql = f"""
            SELECT m.material_id, m.title, m.summary, m.domain, m.collection,
                   m.document_type, m.year, m.authors, m.keywords
            FROM materials_fts
            JOIN materials m ON materials_fts.rowid = m.rowid
            WHERE materials_fts MATCH ? AND {combined_where}
            ORDER BY materials_fts.rank
            LIMIT ?
        """
        rows = con.execute(sql, [materials_query] + combined_params + [limit]).fetchall()
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
        rows = con.execute(sql, [materials_query, limit]).fetchall()

    cards_by_id: dict[str, MaterialCard] = {}
    for i, row in enumerate(rows, 1):
        card = _row_to_card(row, i)
        cards_by_id[card.material_id] = card

    # --- Content-first: surface materials with chunk/annotation matches at depth >= 2 ---
    if depth >= 2 and normalized_scope == "all":
        content_mids = _find_content_material_ids(con, fts_query, limit, scoped_material_ids)
        for mid in content_mids:
            if mid not in cards_by_id:
                row = _fetch_material_row(con, mid, facet_where, facet_params, scoped_material_ids)
                if row:
                    rank = len(cards_by_id) + 1
                    cards_by_id[mid] = _row_to_card(row, rank)

    cards = list(cards_by_id.values())

    # --- Populate content for depth >= 2 ---
    if depth >= 2 and normalized_scope == "all":
        for card in cards:
            card.chunks = _search_chunks(
                con, fts_query, card.material_id, chunk_limit, include_text=(depth >= 3)
            )
            card.annotations = _search_annotations(
                con, fts_query, card.material_id, annotation_limit
            )
            card.figures = _search_figures(
                con, fts_query, card.material_id, figure_limit
            )
            card.concepts = _search_concepts(
                con, fts_query, card.material_id, concept_limit
            )

        # Rerank materials using annotation + emphasized-chunk evidence
        cards.sort(key=_combined_priority)
        for i, card in enumerate(cards, 1):
            card.rank = i

    # Always query canonical concept clusters — useful at all depths
    collection_pages = _search_collection_pages(con, query, limit=min(limit, 6)) if normalized_scope == "all" else []
    canonical_clusters = _search_canonical_clusters(con, fts_query, query, limit=min(limit, 6)) if normalized_scope == "all" else []
    global_bridges = _search_global_bridges(con, fts_query, query, limit=min(limit, 6)) if normalized_scope == "all" else []

    return SearchResult(
        query=query,
        depth=depth,
        total=len(cards),
        results=cards,
        collection_pages=collection_pages,
        canonical_clusters=canonical_clusters,
        global_bridges=global_bridges,
    )


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


def _search_concepts(
    con: sqlite3.Connection,
    query: str,
    material_id: str,
    limit: int,
    *,
    concept_type: str = "local",
) -> list[ConceptHit]:
    sql = """
        SELECT co.concept_name, co.relevance, co.source_pages, co.evidence_spans, co.confidence
        FROM concepts_fts
        JOIN concepts co ON concepts_fts.rowid = co.rowid
        WHERE concepts_fts MATCH ? AND co.material_id = ? AND co.concept_type = ?
        ORDER BY concepts_fts.rank
        LIMIT ?
    """
    rows = con.execute(sql, [query, material_id, concept_type, limit]).fetchall()
    hits: list[ConceptHit] = []
    for i, row in enumerate(rows, 1):
        try:
            source_pages = json.loads(row["source_pages"] or "[]")
        except (json.JSONDecodeError, TypeError):
            source_pages = []
        try:
            evidence_spans = json.loads(row["evidence_spans"] or "[]")
        except (json.JSONDecodeError, TypeError):
            evidence_spans = []
        hits.append(ConceptHit(
            concept_name=row["concept_name"],
            relevance=row["relevance"],
            source_pages=source_pages,
            evidence_spans=evidence_spans,
            confidence=row["confidence"] or 0.0,
            rank=i,
        ))
    return hits


def _search_canonical_clusters(
    con: sqlite3.Connection,
    fts_query: str,
    raw_query: str,
    limit: int = 5,
) -> list[CanonicalClusterHit]:
    hits: list[CanonicalClusterHit] = []
    seen: set[str] = set()

    try:
        rows = con.execute(
            """SELECT cc.cluster_id, cc.canonical_name, cc.slug,
                      cc.aliases, cc.material_count, cc.wiki_path, cc.domain, cc.collection,
                      cr.main_takeaways, cr.main_tensions, cr.open_questions, cr.why_this_concept_matters
               FROM local_concept_clusters_fts
               JOIN local_concept_clusters cc ON local_concept_clusters_fts.rowid = cc.rowid
               LEFT JOIN concept_reflections cr ON cr.cluster_id = cc.cluster_id
               WHERE local_concept_clusters_fts MATCH ?
               ORDER BY local_concept_clusters_fts.rank
               LIMIT ?""",
            [fts_query, limit],
        ).fetchall()
    except sqlite3.OperationalError:
        rows = []
    for row in rows:
        cluster_id = row["cluster_id"]
        if cluster_id in seen:
            continue
        seen.add(cluster_id)
        try:
            aliases = json.loads(row["aliases"] or "[]")
        except (json.JSONDecodeError, TypeError):
            aliases = []
        hits.append(CanonicalClusterHit(
            cluster_id=cluster_id,
            canonical_name=row["canonical_name"],
            slug=row["slug"],
            aliases=aliases,
            material_count=row["material_count"] or 0,
            wiki_path=row["wiki_path"] or "",
            summary=_summary_from_reflection(
                row["why_this_concept_matters"],
                row["main_takeaways"],
                row["main_tensions"],
                row["open_questions"],
            ),
            domain=row["domain"] or "",
            collection=row["collection"] or "",
        ))
        if len(hits) >= limit:
            return hits

    like = _like_pattern(raw_query)
    try:
        rows = con.execute(
            """
            SELECT cc.cluster_id, cc.canonical_name, cc.slug, cc.aliases, cc.material_count,
                   cc.wiki_path, cc.domain, cc.collection,
                   cr.main_takeaways, cr.main_tensions, cr.open_questions, cr.why_this_concept_matters
            FROM concept_reflections cr
            JOIN local_concept_clusters cc ON cr.cluster_id = cc.cluster_id
            WHERE lower(cr.why_this_concept_matters) LIKE lower(?)
               OR lower(cr.main_takeaways) LIKE lower(?)
               OR lower(cr.main_tensions) LIKE lower(?)
               OR lower(cr.open_questions) LIKE lower(?)
               OR lower(cr.helpful_new_sources) LIKE lower(?)
            ORDER BY cc.material_count DESC, cc.canonical_name
            LIMIT ?
            """,
            [like, like, like, like, like, limit],
        ).fetchall()
    except sqlite3.OperationalError:
        rows = []
    for row in rows:
        cluster_id = row["cluster_id"]
        if cluster_id in seen:
            continue
        seen.add(cluster_id)
        aliases = _parse_json_list(row["aliases"] or "[]")
        hits.append(CanonicalClusterHit(
            cluster_id=cluster_id,
            canonical_name=row["canonical_name"],
            slug=row["slug"],
            aliases=aliases,
            material_count=row["material_count"] or 0,
            wiki_path=row["wiki_path"] or "",
            summary=_summary_from_reflection(
                row["why_this_concept_matters"],
                row["main_takeaways"],
                row["main_tensions"],
                row["open_questions"],
            ),
            domain=row["domain"] or "",
            collection=row["collection"] or "",
        ))
        if len(hits) >= limit:
            return hits
    return hits


def _search_collection_pages(
    con: sqlite3.Connection,
    raw_query: str,
    limit: int = 5,
) -> list[CollectionPageHit]:
    pattern = _like_pattern(raw_query)
    try:
        rows = con.execute(
            """
            WITH collection_counts AS (
                SELECT domain, collection, COUNT(*) AS material_count
                FROM materials
                GROUP BY domain, collection
            )
            SELECT cc.domain, cc.collection,
                   COALESCE(wp.title, cc.domain || ' / ' || cc.collection) AS title,
                   COALESCE(cr.wiki_path, wp.path, 'wiki/' || cc.domain || '/' || cc.collection || '/_index.md') AS wiki_path,
                   cc.material_count,
                   cr.main_takeaways,
                   cr.main_tensions,
                   cr.open_questions,
                   cr.why_this_collection_matters
            FROM collection_counts cc
            LEFT JOIN collection_reflections cr
              ON cr.domain = cc.domain AND cr.collection = cc.collection
            LEFT JOIN wiki_pages wp
              ON wp.page_type = 'collection' AND wp.page_id = cc.domain || '/' || cc.collection
            WHERE lower(cc.domain) LIKE lower(?)
               OR lower(cc.collection) LIKE lower(?)
               OR lower(COALESCE(wp.title, '')) LIKE lower(?)
               OR lower(COALESCE(cr.why_this_collection_matters, '')) LIKE lower(?)
               OR lower(COALESCE(cr.main_takeaways, '')) LIKE lower(?)
               OR lower(COALESCE(cr.main_tensions, '')) LIKE lower(?)
               OR lower(COALESCE(cr.open_questions, '')) LIKE lower(?)
               OR lower(COALESCE(cr.helpful_new_sources, '')) LIKE lower(?)
            ORDER BY cc.material_count DESC, cc.domain, cc.collection
            LIMIT ?
            """,
            [pattern, pattern, pattern, pattern, pattern, pattern, pattern, pattern, limit],
        ).fetchall()
    except sqlite3.OperationalError:
        rows = []
    return [
        CollectionPageHit(
            domain=row["domain"] or "",
            collection=row["collection"] or "",
            title=row["title"] or f"{row['domain']} / {row['collection']}",
            wiki_path=row["wiki_path"] or f"wiki/{row['domain']}/{row['collection']}/_index.md",
            material_count=row["material_count"] or 0,
            summary=_summary_from_reflection(
                row["why_this_collection_matters"],
                row["main_takeaways"],
                row["main_tensions"],
                row["open_questions"],
            ),
        )
        for row in rows
    ]


def _bridge_summary(
    why_this_bridge_matters: str | None,
    descriptor: str | None,
    bridge_takeaways: str | None,
    bridge_tensions: str | None,
    bridge_open_questions: str | None,
) -> str:
    for candidate in (
        str(why_this_bridge_matters or "").strip(),
        str(descriptor or "").strip(),
        *(_parse_json_list(bridge_takeaways)[:1]),
        *(_parse_json_list(bridge_tensions)[:1]),
        *(_parse_json_list(bridge_open_questions)[:1]),
    ):
        if candidate:
            return candidate
    return ""


def _search_global_bridges(
    con: sqlite3.Connection,
    fts_query: str,
    raw_query: str,
    limit: int = 5,
) -> list[GlobalBridgeHit]:
    """Search Step 2 global bridges via FTS over name, aliases, and bridge reflection prose.

    Falls back to LIKE over bridge reflection columns when FTS returns nothing,
    so substring queries (e.g., short terms the porter tokenizer drops) still
    find matches.
    """
    hits: list[GlobalBridgeHit] = []
    seen: set[str] = set()

    # Primary: FTS over bridge_clusters_fts (canonical_name, aliases, descriptor,
    # bridge_takeaways, bridge_tensions, bridge_open_questions,
    # helpful_new_sources, why_this_bridge_matters).
    try:
        rows = con.execute(
            """SELECT gbc.bridge_id, gbc.canonical_name, gbc.slug, gbc.descriptor,
                      gbc.aliases, gbc.material_count, gbc.wiki_path,
                      gbc.bridge_takeaways, gbc.bridge_tensions,
                      gbc.bridge_open_questions, gbc.why_this_bridge_matters
               FROM global_bridge_clusters_fts
               JOIN global_bridge_clusters gbc
                 ON global_bridge_clusters_fts.rowid = gbc.rowid
               WHERE global_bridge_clusters_fts MATCH ?
               ORDER BY global_bridge_clusters_fts.rank
               LIMIT ?""",
            [fts_query, limit],
        ).fetchall()
    except sqlite3.OperationalError:
        rows = []
    for row in rows:
        bridge_id = row["bridge_id"]
        if bridge_id in seen:
            continue
        seen.add(bridge_id)
        hits.append(GlobalBridgeHit(
            bridge_id=bridge_id,
            canonical_name=row["canonical_name"],
            slug=row["slug"] or "",
            descriptor=row["descriptor"] or "",
            aliases=_parse_json_list(row["aliases"] or "[]"),
            material_count=row["material_count"] or 0,
            wiki_path=row["wiki_path"] or "",
            summary=_bridge_summary(
                row["why_this_bridge_matters"],
                row["descriptor"],
                row["bridge_takeaways"],
                row["bridge_tensions"],
                row["bridge_open_questions"],
            ),
        ))
        if len(hits) >= limit:
            return hits

    # Fallback: LIKE over reflection prose columns for substring matches the
    # porter tokenizer might drop.
    pattern = _like_pattern(raw_query)
    try:
        rows = con.execute(
            """SELECT bridge_id, canonical_name, slug, descriptor, aliases,
                      material_count, wiki_path, bridge_takeaways, bridge_tensions,
                      bridge_open_questions, helpful_new_sources, why_this_bridge_matters
               FROM global_bridge_clusters
               WHERE lower(canonical_name) LIKE lower(?)
                  OR lower(descriptor) LIKE lower(?)
                  OR lower(aliases) LIKE lower(?)
                  OR lower(why_this_bridge_matters) LIKE lower(?)
                  OR lower(bridge_takeaways) LIKE lower(?)
                  OR lower(bridge_tensions) LIKE lower(?)
                  OR lower(bridge_open_questions) LIKE lower(?)
                  OR lower(helpful_new_sources) LIKE lower(?)
               ORDER BY material_count DESC, canonical_name
               LIMIT ?""",
            [pattern, pattern, pattern, pattern, pattern, pattern, pattern, pattern, limit],
        ).fetchall()
    except sqlite3.OperationalError:
        rows = []
    for row in rows:
        bridge_id = row["bridge_id"]
        if bridge_id in seen:
            continue
        seen.add(bridge_id)
        hits.append(GlobalBridgeHit(
            bridge_id=bridge_id,
            canonical_name=row["canonical_name"],
            slug=row["slug"] or "",
            descriptor=row["descriptor"] or "",
            aliases=_parse_json_list(row["aliases"] or "[]"),
            material_count=row["material_count"] or 0,
            wiki_path=row["wiki_path"] or "",
            summary=_bridge_summary(
                row["why_this_bridge_matters"],
                row["descriptor"],
                row["bridge_takeaways"],
                row["bridge_tensions"],
                row["bridge_open_questions"],
            ),
        ))
        if len(hits) >= limit:
            return hits
    return hits


def _local_cluster_rows_for_material(con: sqlite3.Connection, material_id: str) -> list[sqlite3.Row]:
    try:
        return con.execute(
            """SELECT lcc.cluster_id, lcc.canonical_name, lcc.slug, lcc.aliases,
                      lcc.material_count, lcc.wiki_path, lcc.domain, lcc.collection
               FROM local_cluster_materials lcm
               JOIN local_concept_clusters lcc ON lcm.cluster_id = lcc.cluster_id
               WHERE lcm.material_id = ?
               ORDER BY lcc.canonical_name, lcc.cluster_id""",
            [material_id],
        ).fetchall()
    except sqlite3.OperationalError:
        return []


def _local_cluster_rows_for_collection(con: sqlite3.Connection, domain: str, collection: str) -> list[sqlite3.Row]:
    try:
        return con.execute(
            """SELECT cluster_id, canonical_name, slug, aliases,
                      material_count, wiki_path, domain, collection
               FROM local_concept_clusters
               WHERE domain = ? AND collection = ?
               ORDER BY canonical_name, cluster_id""",
            [domain, collection],
        ).fetchall()
    except sqlite3.OperationalError:
        return []


def _global_bridge_rows_for_local_cluster(con: sqlite3.Connection, cluster_id: str) -> list[sqlite3.Row]:
    try:
        return con.execute(
            """SELECT gbc.bridge_id AS cluster_id, gbc.canonical_name, gbc.slug, gbc.aliases,
                      gbc.material_count, gbc.wiki_path, '' AS domain, '' AS collection
               FROM global_bridge_members gbm
               JOIN global_bridge_clusters gbc ON gbm.bridge_cluster_id = gbc.bridge_id
               WHERE gbm.local_cluster_id = ?
               ORDER BY gbc.canonical_name, gbc.bridge_id""",
            [cluster_id],
        ).fetchall()
    except sqlite3.OperationalError:
        return []


def _local_cluster_rows_for_global_bridge(con: sqlite3.Connection, bridge_id: str) -> list[sqlite3.Row]:
    try:
        return con.execute(
            """SELECT lcc.cluster_id, lcc.canonical_name, lcc.slug, lcc.aliases,
                      lcc.material_count, lcc.wiki_path, lcc.domain, lcc.collection
               FROM global_bridge_members gbm
               JOIN local_concept_clusters lcc ON gbm.local_cluster_id = lcc.cluster_id
               WHERE gbm.bridge_cluster_id = ?
               ORDER BY lcc.domain, lcc.collection, lcc.canonical_name, lcc.cluster_id""",
            [bridge_id],
        ).fetchall()
    except sqlite3.OperationalError:
        return []


def _rows_to_cluster_hits(rows: list[sqlite3.Row]) -> list[CanonicalClusterHit]:
    hits: list[CanonicalClusterHit] = []
    for row in rows:
        try:
            aliases = json.loads(row["aliases"] or "[]")
        except (json.JSONDecodeError, TypeError):
            aliases = []
        hits.append(CanonicalClusterHit(
            cluster_id=row["cluster_id"],
            canonical_name=row["canonical_name"],
            slug=row["slug"],
            aliases=aliases,
            material_count=row["material_count"] or 0,
            wiki_path=row["wiki_path"] or "",
            domain=row["domain"] or "",
            collection=row["collection"] or "",
        ))
    return hits


# --- Human-readable formatting ---

def format_human(result: SearchResult) -> str:
    if (
        result.total == 0
        and not result.collection_pages
        and not result.canonical_clusters
        and not result.global_bridges
    ):
        return f'No results for "{result.query}"'

    lines: list[str] = []

    if result.collection_pages:
        lines.append("Collection pages:")
        for hit in result.collection_pages:
            summary = hit.summary[:100] + "…" if len(hit.summary) > 100 else hit.summary
            lines.append(f"  • {hit.title} [{hit.material_count} material(s)]")
            if summary:
                lines.append(f"    {summary}")
        lines.append("")

    if result.canonical_clusters:
        lines.append("Concept pages:")
        for cl in result.canonical_clusters:
            alias_str = f"  (aliases: {', '.join(cl.aliases[:3])})" if cl.aliases else ""
            if "/bridge-concepts/" in cl.wiki_path:
                tag = " [global]"
            elif cl.domain and cl.collection:
                tag = f" [local {cl.domain}/{cl.collection}]"
            else:
                tag = ""
            lines.append(f"  • {cl.canonical_name}{tag}{alias_str}  [{cl.material_count} material(s)]")
            if cl.summary:
                summary = cl.summary[:100] + "…" if len(cl.summary) > 100 else cl.summary
                lines.append(f"    {summary}")
        lines.append("")

    if result.global_bridges:
        lines.append("Global bridges:")
        for br in result.global_bridges:
            alias_str = f"  (aliases: {', '.join(br.aliases[:3])})" if br.aliases else ""
            lines.append(f"  • {br.canonical_name}{alias_str}  [{br.material_count} material(s)]")
            if br.summary:
                summary = br.summary[:100] + "…" if len(br.summary) > 100 else br.summary
                lines.append(f"    {summary}")
        lines.append("")

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

            if card.concepts:
                lines.append("")
                lines.append("    Concepts:")
                for con_hit in card.concepts:
                    lines.append(f"      [{con_hit.relevance}] {con_hit.concept_name}")

        lines.append("")
        lines.append(f'{result.total} result(s) for "{result.query}" (depth {result.depth})')

    return "\n".join(lines)


# --- Related materials (C4.3) ---

@dataclass
class Connection:
    type: str   # shared_concept | shared_keyword | shared_facet | shared_author
    value: str  # the shared term
    facet: str = ""   # for shared_facet: which facet column
    weight: float = 0.0

    def to_dict(self) -> dict:
        d: dict[str, Any] = {
            "type": self.type,
            "value": self.value,
            "weight": self.weight,
            "label": _connection_label(self),
        }
        if self.facet:
            d["facet"] = self.facet
        return d


def _connection_label(conn: Connection) -> str:
    if conn.type == "shared_local_cluster":
        return f"shared local home: {conn.value}"
    if conn.type == "shared_global_cluster":
        return f"shared bridge: {conn.value}"
    if conn.type == "shared_concept":
        return f"shared concept: {conn.value}"
    if conn.type == "shared_keyword":
        return f"shared keyword: {conn.value}"
    if conn.type == "shared_author":
        return f"shared author: {conn.value}"
    if conn.type == "shared_facet":
        facet_label = f" [{conn.facet}]" if conn.facet else ""
        return f"shared facet{facet_label}: {conn.value}"
    return f"{conn.type}: {conn.value}"


@dataclass
class RelatedMaterial:
    material_id: str
    title: str
    domain: str
    collection: str
    document_type: str
    year: str
    score: float
    connections: list[Connection]

    def to_dict(self) -> dict:
        return {
            "material_id": self.material_id,
            "title": self.title,
            "domain": self.domain,
            "collection": self.collection,
            "document_type": self.document_type,
            "year": self.year,
            "score": round(self.score, 3),
            "connections": [c.to_dict() for c in self.connections],
        }


_FACET_COLUMNS_FOR_RELATED = (
    "location", "historical_period", "scale", "jurisdiction",
    "climate", "program", "material_system", "structural_system",
    "course_topic", "studio_project", "building_type",
)


def find_related(
    material_id: str,
    config: dict | None = None,
    *,
    limit: int = 10,
) -> list[RelatedMaterial]:
    """Return materials related to material_id via shared concepts, keywords, facets, or authors."""
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
        return _do_find_related(con, material_id, limit)
    finally:
        con.close()


def _do_find_related(
    con: sqlite3.Connection,
    material_id: str,
    limit: int,
) -> list[RelatedMaterial]:
    scores: dict[str, float] = {}
    connections: dict[str, list[Connection]] = {}

    def _add(mid: str, conn: Connection) -> None:
        if mid == material_id:
            return
        scores[mid] = scores.get(mid, 0.0) + conn.weight
        connections.setdefault(mid, []).append(conn)

    # Shared collection-local cluster membership (weight 2.5) — strongest same-collection signal
    try:
        cluster_rows = con.execute(
            """SELECT lcm2.material_id, lcc.canonical_name
               FROM local_cluster_materials lcm1
               JOIN local_cluster_materials lcm2 ON lcm1.cluster_id = lcm2.cluster_id
               JOIN local_concept_clusters lcc ON lcm1.cluster_id = lcc.cluster_id
               WHERE lcm1.material_id = ? AND lcm2.material_id != ?""",
            [material_id, material_id],
        ).fetchall()
        for row in cluster_rows:
            _add(row["material_id"], Connection(
                type="shared_local_cluster", value=row["canonical_name"], weight=2.5
            ))
    except sqlite3.OperationalError:
        pass

    # Shared local concepts via concept_key (weight 1.0 each — normalized matching)
    rows = con.execute(
        """SELECT c2.material_id, c1.concept_name
           FROM concepts c1
           JOIN concepts c2 ON c1.concept_key = c2.concept_key
           WHERE c1.material_id = ? AND c2.material_id != ?
             AND c1.concept_type = 'local' AND c2.concept_type = 'local'""",
        [material_id, material_id],
    ).fetchall()
    for row in rows:
        _add(row["material_id"], Connection(
            type="shared_concept", value=row["concept_name"], weight=1.0
        ))

    # Shared enriched keywords via helper table (weight 0.3 each)
    kw_rows = con.execute(
        """SELECT mk2.material_id, mk1.keyword
           FROM material_keywords mk1
           JOIN material_keywords mk2 ON mk1.keyword = mk2.keyword
           WHERE mk1.material_id = ? AND mk2.material_id != ?""",
        [material_id, material_id],
    ).fetchall()
    for row in kw_rows:
        _add(row["material_id"], Connection(
            type="shared_keyword", value=row["keyword"], weight=0.3
        ))

    # Shared authors via helper table (weight 0.8 each)
    auth_rows = con.execute(
        """SELECT ma2.material_id, ma1.author
           FROM material_authors ma1
           JOIN material_authors ma2 ON ma1.author = ma2.author
           WHERE ma1.material_id = ? AND ma2.material_id != ?""",
        [material_id, material_id],
    ).fetchall()
    for row in auth_rows:
        _add(row["material_id"], Connection(
            type="shared_author", value=row["author"], weight=0.8
        ))

    # Shared facet values (weight 0.5 each, skip empty values)
    facet_row = con.execute(
        f"SELECT {', '.join(_FACET_COLUMNS_FOR_RELATED)} FROM materials WHERE material_id = ?",
        [material_id],
    ).fetchone()
    if facet_row:
        all_others = con.execute(
            f"SELECT material_id, {', '.join(_FACET_COLUMNS_FOR_RELATED)} FROM materials WHERE material_id != ?",
            [material_id],
        ).fetchall()
        for col in _FACET_COLUMNS_FOR_RELATED:
            src_val = (facet_row[col] or "").strip()
            if not src_val:
                continue
            for other in all_others:
                other_val = (other[col] or "").strip()
                if other_val and other_val.lower() == src_val.lower():
                    _add(other["material_id"], Connection(
                        type="shared_facet", value=src_val, facet=col, weight=0.5
                    ))

    if not scores:
        return []

    # Fetch card info for all candidates
    sorted_mids = sorted(scores, key=lambda m: -scores[m])[:limit]
    related: list[RelatedMaterial] = []
    for mid in sorted_mids:
        row = con.execute(
            "SELECT title, domain, collection, document_type, year FROM materials WHERE material_id = ?",
            [mid],
        ).fetchone()
        if not row:
            continue
        related.append(RelatedMaterial(
            material_id=mid,
            title=row["title"],
            domain=row["domain"],
            collection=row["collection"],
            document_type=row["document_type"],
            year=row["year"],
            score=scores[mid],
            connections=connections[mid],
        ))

    return related


def format_related_human(material_id: str, related: list[RelatedMaterial]) -> str:
    if not related:
        return f"No related materials found for {material_id}."
    lines = [f"Related to {material_id}:\n"]
    for i, r in enumerate(related, 1):
        lines.append(f"  {i:>2}. {r.title[:60]} ({r.material_id})  score={r.score:.2f}")
        for conn in r.connections[:5]:
            prefix = "★ " if conn.type in {"shared_local_cluster", "shared_global_cluster"} else "  "
            lines.append(f"      {prefix}{_connection_label(conn)}")
    return "\n".join(lines)


def get_material_clusters(
    material_id: str,
    config: dict | None = None,
) -> list[CanonicalClusterHit]:
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
        return _rows_to_cluster_hits(_local_cluster_rows_for_material(con, material_id))
    finally:
        con.close()


def get_collection_clusters(
    domain: str,
    collection: str,
    config: dict | None = None,
) -> list[CanonicalClusterHit]:
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
        return _rows_to_cluster_hits(_local_cluster_rows_for_collection(con, domain, collection))
    finally:
        con.close()


def get_cluster_global_bridges(
    cluster_id: str,
    config: dict | None = None,
) -> list[CanonicalClusterHit]:
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
        return _rows_to_cluster_hits(_global_bridge_rows_for_local_cluster(con, cluster_id))
    finally:
        con.close()


def get_bridge_member_clusters(
    bridge_id: str,
    config: dict | None = None,
) -> list[CanonicalClusterHit]:
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
        return _rows_to_cluster_hits(_local_cluster_rows_for_global_bridge(con, bridge_id))
    finally:
        con.close()


def format_cluster_hits_human(label: str, hits: list[CanonicalClusterHit]) -> str:
    if not hits:
        return f"No local clusters found for {label}."
    lines = [f"Local clusters for {label}:\n"]
    for idx, hit in enumerate(hits, 1):
        scope = f" [{hit.domain}/{hit.collection}]" if hit.domain and hit.collection else ""
        lines.append(f"  {idx:>2}. {hit.canonical_name}{scope} ({hit.cluster_id})  materials={hit.material_count}")
    return "\n".join(lines)


# --- Concept listing (C4.4) ---

@dataclass
class ConceptEntry:
    concept_name: str
    material_count: int
    material_ids: list[str]
    relevance_summary: str   # e.g. "2×high, 1×medium"

    def to_dict(self) -> dict:
        return {
            "concept_name": self.concept_name,
            "material_count": self.material_count,
            "material_ids": self.material_ids,
            "relevance_summary": self.relevance_summary,
        }


def _build_relevance_summary(csv: str) -> str:
    """Turn a comma-separated relevance list into a counted summary like '2×high, 1×medium'."""
    if not csv:
        return ""
    values = [v.strip() for v in csv.split(",") if v.strip()]
    counts: dict[str, int] = {}
    for v in values:
        counts[v] = counts.get(v, 0) + 1
    # Order by count descending, then alphabetically
    ranked = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
    if len(ranked) == 1 and ranked[0][1] == 1:
        return ranked[0][0]
    return ", ".join(f"{c}×{r}" if c > 1 else r for r, c in ranked)


def list_concepts(
    config: dict | None = None,
    *,
    min_materials: int = 1,
    limit: int = 100,
    concept_type: str = "local",
) -> list[ConceptEntry]:
    """List all concept candidates across the collection, grouped by normalized concept_key."""
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
        rows = con.execute(
            """
            SELECT concept_key,
                   MAX(concept_name) AS display_name,
                   COUNT(DISTINCT material_id) AS material_count,
                   GROUP_CONCAT(DISTINCT material_id) AS material_ids_csv,
                   GROUP_CONCAT(relevance) AS relevance_values
            FROM concepts
            WHERE concept_type = ?
            GROUP BY concept_key
            HAVING COUNT(DISTINCT material_id) >= ?
            ORDER BY material_count DESC, concept_key
            LIMIT ?
            """,
            [concept_type, min_materials, limit],
        ).fetchall()
    finally:
        con.close()

    entries: list[ConceptEntry] = []
    for row in rows:
        mids = [m.strip() for m in (row["material_ids_csv"] or "").split(",") if m.strip()]
        entries.append(ConceptEntry(
            concept_name=row["display_name"] or row["concept_key"],
            material_count=row["material_count"],
            material_ids=mids,
            relevance_summary=_build_relevance_summary(row["relevance_values"] or ""),
        ))
    return entries


def format_concepts_human(entries: list[ConceptEntry]) -> str:
    if not entries:
        return "No concepts found."
    lines = [f' {"#":>3}  {"Concept":<45}  {"Materials":>9}  {"Relevance"}']
    lines.append(" " + "-" * 80)
    for i, e in enumerate(entries, 1):
        name = e.concept_name[:43] + ".." if len(e.concept_name) > 45 else e.concept_name
        lines.append(f" {i:>3}  {name:<45}  {e.material_count:>9}  {e.relevance_summary}")
    lines.append(f"\n{len(entries)} concept(s) across collection.")
    return "\n".join(lines)
