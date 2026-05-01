"""Deterministic read helpers for Phase 8 surfaces."""

from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path, PurePosixPath
from urllib.parse import unquote, urlsplit

from arquimedes.compile_pages import _material_wiki_path
from arquimedes.config import get_library_root, get_project_root
from arquimedes.index import get_index_path

_DOMAINS = ("research", "practice", "proyectos")


def _normalized_domain(domain: str | None) -> str | None:
    value = str(domain or "").strip().lower()
    return value if value in _DOMAINS else None


def _material_dir(material_id: str) -> Path:
    material_id = material_id.strip()
    if not material_id or any(ch in material_id for ch in ("/", "\\", "..")):
        raise FileNotFoundError(material_id or "Invalid material id")
    return get_project_root() / "extracted" / material_id


def _safe_rel_path(rel_path: str) -> PurePosixPath:
    rel_path = (rel_path or "").strip().strip("/")
    if "\\" in rel_path:
        raise FileNotFoundError(rel_path)
    path = PurePosixPath(rel_path)
    if path.is_absolute():
        raise FileNotFoundError(rel_path)
    parts = path.parts[1:] if path.parts[:1] == ("wiki",) else path.parts
    if any(part == ".." for part in parts):
        raise FileNotFoundError(rel_path)
    return PurePosixPath(*parts)


def _resolve(root: Path, path: PurePosixPath) -> Path:
    candidate = (root / Path(*path.parts)).resolve()
    try:
        candidate.relative_to(root.resolve())
    except ValueError as exc:
        raise FileNotFoundError(candidate) from exc
    return candidate


def _read_json(path: Path) -> dict:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return data


def _index_rows(query: str, params: tuple = ()) -> list[sqlite3.Row]:
    index_path = get_index_path()
    if not index_path.exists():
        return []
    con = sqlite3.connect(f"file:{index_path}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    try:
        return list(con.execute(query, params))
    finally:
        con.close()


def load_material_meta(material_id: str) -> dict:
    path = _material_dir(material_id) / "meta.json"
    if not path.exists():
        raise FileNotFoundError(path)
    return _read_json(path)


def material_wiki_path(material_id: str) -> Path:
    return _resolve(get_project_root(), PurePosixPath(_material_wiki_path(load_material_meta(material_id))))


def load_material_wiki(material_id: str) -> str:
    path = material_wiki_path(material_id)
    if not path.exists():
        raise FileNotFoundError(path)
    return path.read_text(encoding="utf-8")


def load_material_figures(material_id: str) -> list[dict]:
    figures_dir = _material_dir(material_id) / "figures"
    if not figures_dir.is_dir():
        return []
    figures: list[dict] = []
    for path in sorted(figures_dir.glob("fig_*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        if isinstance(data, dict):
            figures.append(data)
    return figures


def material_source_path(material_id: str) -> Path | None:
    source_path = str(load_material_meta(material_id).get("source_path") or "").strip()
    if not source_path:
        return None
    try:
        path = _resolve(get_library_root(), PurePosixPath(source_path))
    except (FileNotFoundError, ValueError):
        return None
    return path if path.exists() else None


def material_extracted_text_path(material_id: str) -> Path | None:
    path = _material_dir(material_id) / "text.md"
    return path if path.exists() else None


def material_figure_image_path(material_id: str, filename: str) -> Path | None:
    if "/" in filename or "\\" in filename:
        return None
    path = _material_dir(material_id) / "figures" / filename
    return path if path.exists() else None


def load_material_thumbnails(material_id: str) -> list[dict]:
    material_dir = _material_dir(material_id)
    pages_path = material_dir / "pages.jsonl"
    thumbs: list[dict] = []
    if pages_path.exists():
        for line in pages_path.read_text(encoding="utf-8").splitlines():
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(row, dict):
                continue
            thumb = str(row.get("thumbnail_path") or "").strip()
            if thumb.startswith("thumbnails/"):
                name = PurePosixPath(thumb).name
                path = material_dir / "thumbnails" / name
                if path.exists():
                    thumbs.append({"page_number": int(row.get("page_number") or 0), "filename": name})
    if thumbs:
        return thumbs
    thumbs_dir = material_dir / "thumbnails"
    if not thumbs_dir.is_dir():
        return []
    return [{"page_number": i + 1, "filename": path.name} for i, path in enumerate(sorted(thumbs_dir.glob("page_*.png")))]


def material_thumbnail_path(material_id: str, filename: str) -> Path | None:
    if "/" in filename or "\\" in filename:
        return None
    path = _material_dir(material_id) / "thumbnails" / filename
    return path if path.exists() else None


def _wiki_dir(rel_path: str = "") -> tuple[str, Path]:
    path = _safe_rel_path(rel_path)
    root = get_project_root() / "wiki"
    resolved = _resolve(root, path)
    if not resolved.is_dir():
        raise FileNotFoundError(resolved)
    return path.as_posix().strip("."), resolved


def list_wiki_dir(rel_path: str = "") -> dict:
    rel, directory = _wiki_dir(rel_path)
    dirs = [{"name": path.name, "path": "/".join(filter(None, [rel, path.name]))} for path in sorted(directory.iterdir()) if path.is_dir()]
    pages = [
        {"name": path.stem, "path": "/".join(filter(None, [rel, path.stem]))}
        for path in sorted(directory.glob("*.md"))
        if path.name != "_index.md"
    ]
    return {"path": rel, "dirs": dirs, "pages": pages, "index_exists": (directory / "_index.md").exists()}


def load_wiki_page(rel_path: str) -> tuple[Path, str]:
    path = _safe_rel_path(rel_path)
    root = get_project_root() / "wiki"
    if not path.parts:
        resolved = root / "_index.md"
    else:
        candidate = _resolve(root, path)
        if candidate.is_dir():
            resolved = candidate / "_index.md"
        else:
            resolved = candidate if candidate.suffix == ".md" else candidate.with_suffix(".md")
    if not resolved.exists():
        raise FileNotFoundError(resolved)
    return resolved, resolved.read_text(encoding="utf-8")


def material_id_for_wiki_path(path: Path) -> str | None:
    if path.name == "_index.md" or "concepts" in path.parts or "shared" in path.parts:
        return None
    material_id = path.stem
    try:
        load_material_meta(material_id)
    except FileNotFoundError:
        return None
    return material_id


def wiki_page_record(path: Path) -> dict | None:
    project_root = get_project_root()
    try:
        rel_path = path.relative_to(project_root).as_posix()
    except ValueError:
        return None
    rows = _index_rows(
        "SELECT page_type, page_id, title, path, domain, collection FROM wiki_pages WHERE path = ? LIMIT 1",
        (rel_path,),
    )
    if rows:
        return dict(rows[0])
    try:
        rows = _index_rows(
            """
            SELECT 'concept' AS page_type, cluster_id AS page_id, canonical_name AS title,
                   wiki_path AS path, domain, collection
            FROM local_concept_clusters
            WHERE wiki_path = ?
            LIMIT 1
            """,
            (rel_path,),
        )
        if rows:
            return dict(rows[0])
        rows = _index_rows(
            """
            SELECT 'global_bridge' AS page_type, bridge_id AS page_id, canonical_name AS title,
                   wiki_path AS path, domain, 'bridge-concepts' AS collection
            FROM global_bridge_clusters
            WHERE wiki_path = ?
            LIMIT 1
            """,
            (rel_path,),
        )
        return dict(rows[0]) if rows else None
    except sqlite3.OperationalError:
        return None


def materials_for_concept(cluster_id: str) -> list[dict]:
    rows = _index_rows(
        """
        SELECT DISTINCT m.material_id, m.title
        FROM materials m
        JOIN local_cluster_materials scoped ON scoped.material_id = m.material_id
        WHERE scoped.cluster_id = ?
        ORDER BY m.title
        """,
        (cluster_id,),
    )
    return [dict(row) for row in rows]


def _resolve_wiki_target(base_rel: PurePosixPath, target: str) -> PurePosixPath | None:
    parsed = urlsplit((target or "").strip().strip("<>"))
    raw_path = unquote(parsed.path or "").strip()
    if not raw_path or parsed.scheme in {"http", "https", "mailto", "data", "javascript", "file"}:
        return None
    if raw_path.startswith("/"):
        return PurePosixPath(raw_path.lstrip("/"))
    if raw_path.startswith("wiki/"):
        return PurePosixPath(raw_path)

    parts: list[str] = []
    for part in (*base_rel.parent.parts, *PurePosixPath(raw_path).parts):
        if part in ("", "."):
            continue
        if part == "..":
            if not parts:
                return None
            parts.pop()
            continue
        parts.append(part)
    return PurePosixPath(*parts)


def materials_for_concept_page(path: Path, body: str) -> list[dict]:
    project_root = get_project_root()
    try:
        base_rel = PurePosixPath(path.relative_to(project_root).as_posix())
    except ValueError:
        return []

    seen: set[str] = set()
    materials: list[dict] = []
    for match in re.finditer(r"\[[^\]]+\]\(([^)]+)\)", body):
        resolved = _resolve_wiki_target(base_rel, match.group(1))
        if not resolved or resolved.parts[:1] != ("wiki",):
            continue
        if resolved.suffix != ".md" or resolved.name == "_index.md" or len(resolved.parts) != 4:
            continue
        material_id = resolved.stem
        if material_id in seen:
            continue
        try:
            meta = load_material_meta(material_id)
        except FileNotFoundError:
            continue
        seen.add(material_id)
        materials.append({"material_id": material_id, "title": str(meta.get("title") or material_id)})
    return materials


def list_domains_and_collections(domain: str | None = None) -> list[dict]:
    scoped_domain = _normalized_domain(domain)
    if scoped_domain:
        rows = _index_rows(
            "SELECT DISTINCT domain, collection FROM materials WHERE domain = ? ORDER BY domain, collection",
            (scoped_domain,),
        )
    else:
        rows = _index_rows(
            "SELECT DISTINCT domain, collection FROM materials ORDER BY domain, collection"
        )
    return [{"domain": row["domain"], "collection": row["collection"]} for row in rows]


def list_glossary_concepts(domain: str | None = None) -> list[dict]:
    try:
        _, body = load_wiki_page("shared/glossary")
    except FileNotFoundError:
        return []
    scoped_domain = _normalized_domain(domain)
    rows = [
        {"name": name.removesuffix(" (main)"), "path": path}
        for name, path in re.findall(r"\[([^\]]+)\]\((wiki/[^/]+/bridge-concepts/[^)]+\.md)\)", body)
    ]
    if not scoped_domain:
        return rows
    prefix = f"wiki/{scoped_domain}/bridge-concepts/"
    return [row for row in rows if str(row.get("path") or "").startswith(prefix)]


def materials_for_collection(domain: str, collection: str) -> list[dict]:
    rows = _index_rows(
        "SELECT material_id, title FROM materials WHERE domain=? AND collection=? ORDER BY title",
        (domain, collection),
    )
    return [dict(row) for row in rows]


def recent_materials(limit: int = 10, domain: str | None = None) -> list[dict]:
    scoped_domain = _normalized_domain(domain)
    if scoped_domain:
        rows = _index_rows(
            "SELECT material_id, title, summary, domain, collection, document_type, year FROM materials WHERE domain = ? ORDER BY rowid DESC LIMIT ?",
            (scoped_domain, limit),
        )
    else:
        rows = _index_rows(
            "SELECT material_id, title, summary, domain, collection, document_type, year FROM materials ORDER BY rowid DESC LIMIT ?",
            (limit,),
        )
    return [dict(row) for row in rows]


def random_figures(limit: int = 12, domain: str | None = None) -> list[dict]:
    scoped_domain = _normalized_domain(domain)
    params: tuple = (limit,)
    where = "WHERE f.image_path != ''"
    if scoped_domain:
        where += " AND m.domain = ?"
        params = (scoped_domain, limit)
    try:
        rows = _index_rows(
            f"""
            SELECT f.figure_id, f.material_id, f.image_path, f.caption, f.description,
                   m.title, m.domain, m.collection
            FROM figures f
            JOIN materials m ON m.material_id = f.material_id
            {where}
            ORDER BY RANDOM()
            LIMIT ?
            """,
            params,
        )
    except sqlite3.OperationalError:
        return []
    return [dict(row) for row in rows]


# ---------------------------------------------------------------------------
# Phase 7 agent-facing accessors
# ---------------------------------------------------------------------------


def _load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows: list[dict] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            rows.append(obj)
    return rows


def _wiki_rel_path(material_id: str, meta: dict) -> str:
    rel = _material_wiki_path(meta)
    return rel if rel.startswith("wiki/") else f"wiki/{rel}"


def build_material_card(material_id: str) -> dict:
    """Compact summary of a material: meta + counts + pointers, no bodies."""
    meta = load_material_meta(material_id)
    material_dir = _material_dir(material_id)
    chunks = _load_jsonl(material_dir / "chunks.jsonl")
    pages = _load_jsonl(material_dir / "pages.jsonl")
    annotations = _load_jsonl(material_dir / "annotations.jsonl")
    figures_dir = material_dir / "figures"
    figure_count = len(list(figures_dir.glob("fig_*.json"))) if figures_dir.is_dir() else 0

    source_path = str(meta.get("source_path") or "").strip()

    return {
        "material_id": material_id,
        "title": str(meta.get("title") or ""),
        "authors": list(meta.get("authors") or []),
        "year": str(meta.get("year") or ""),
        "document_type": str(meta.get("document_type") or ""),
        "domain": str(meta.get("domain") or ""),
        "collection": str(meta.get("collection") or ""),
        "summary": str(meta.get("summary") or ""),
        "wiki_path": _wiki_rel_path(material_id, meta),
        "source_path": source_path,
        "counts": {
            "pages": len(pages),
            "chunks": len(chunks),
            "annotations": len(annotations),
            "figures": figure_count,
        },
    }


def list_chunks_compact(material_id: str) -> list[dict]:
    """One entry per chunk: id, pages, emphasized flag, summary (if present)."""
    material_dir = _material_dir(material_id)
    if not (material_dir / "meta.json").exists():
        raise FileNotFoundError(material_dir / "meta.json")
    rows: list[dict] = []
    for chunk in _load_jsonl(material_dir / "chunks.jsonl"):
        summary_value = chunk.get("summary")
        summary_text = ""
        if isinstance(summary_value, dict):
            summary_text = str(summary_value.get("value") or "")
        elif isinstance(summary_value, str):
            summary_text = summary_value
        rows.append({
            "chunk_id": str(chunk.get("chunk_id") or ""),
            "source_pages": list(chunk.get("source_pages") or []),
            "emphasized": bool(chunk.get("emphasized")),
            "content_class": str(chunk.get("content_class") or ""),
            "summary": summary_text,
        })
    return rows


def get_chunk_by_id(material_id: str, chunk_id: str) -> dict:
    """Full chunk record (text + metadata) by id."""
    material_dir = _material_dir(material_id)
    if not (material_dir / "meta.json").exists():
        raise FileNotFoundError(material_dir / "meta.json")
    for chunk in _load_jsonl(material_dir / "chunks.jsonl"):
        if str(chunk.get("chunk_id") or "") == chunk_id:
            return chunk
    raise FileNotFoundError(f"chunk {chunk_id!r} in material {material_id!r}")


def get_page(material_id: str, page_number: int) -> dict:
    """Full page record (text + metadata) by 1-based page number."""
    material_dir = _material_dir(material_id)
    if not (material_dir / "meta.json").exists():
        raise FileNotFoundError(material_dir / "meta.json")
    for page in _load_jsonl(material_dir / "pages.jsonl"):
        if int(page.get("page_number") or 0) == page_number:
            return page
    raise FileNotFoundError(f"page {page_number} in material {material_id!r}")


def _figure_field_value(value: object) -> str:
    if isinstance(value, dict):
        return str(value.get("value") or "")
    if isinstance(value, str):
        return value
    return ""


def list_figures_compact(material_id: str, visual_type: str | None = None) -> list[dict]:
    """Compact figure index: id, page, visual_type, caption (flattened)."""
    figures = load_material_figures(material_id)
    vt_filter = (visual_type or "").strip().lower() or None
    rows: list[dict] = []
    for fig in figures:
        vt = _figure_field_value(fig.get("visual_type")).strip()
        if vt_filter and vt.lower() != vt_filter:
            continue
        rows.append({
            "figure_id": str(fig.get("figure_id") or ""),
            "source_page": int(fig.get("source_page") or 0),
            "visual_type": vt,
            "relevance": str(fig.get("relevance") or ""),
            "caption": _figure_field_value(fig.get("caption")),
            "image_path": str(fig.get("image_path") or ""),
        })
    return rows


def get_figure(material_id: str, figure_id: str) -> dict:
    """Full figure sidecar by id."""
    for fig in load_material_figures(material_id):
        if str(fig.get("figure_id") or "") == figure_id:
            return fig
    raise FileNotFoundError(f"figure {figure_id!r} in material {material_id!r}")


def list_annotations(
    material_id: str,
    page: int | None = None,
    kind: str | None = None,
) -> list[dict]:
    """Annotations for a material, optionally filtered by page or type."""
    material_dir = _material_dir(material_id)
    if not (material_dir / "meta.json").exists():
        raise FileNotFoundError(material_dir / "meta.json")
    kind_filter = (kind or "").strip().lower() or None
    rows: list[dict] = []
    for ann in _load_jsonl(material_dir / "annotations.jsonl"):
        if page is not None and int(ann.get("page") or 0) != page:
            continue
        if kind_filter and str(ann.get("type") or "").lower() != kind_filter:
            continue
        rows.append(ann)
    return rows


def _read_json_if_exists(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        return _read_json(path)
    except (json.JSONDecodeError, ValueError, OSError):
        return None


def _count_one(sql: str, params: tuple = ()) -> int:
    try:
        rows = _index_rows(sql, params)
    except sqlite3.OperationalError:
        return 0
    if not rows:
        return 0
    first = rows[0]
    value = first[0] if len(first.keys()) == 1 else first["c"]
    return int(value or 0)


def build_corpus_overview(*, domain: str | None = None) -> dict:
    """Live snapshot of corpus counts and freshness stamps."""
    project_root = get_project_root()
    derived_dir = project_root / "derived"
    index_path = get_index_path()

    domain = str(domain or "").strip()
    if domain:
        counts = {
            "materials": _count_one("SELECT COUNT(*) FROM materials WHERE domain = ?", (domain,)),
            "chunks": _count_one("SELECT COUNT(*) FROM chunks WHERE material_id IN (SELECT material_id FROM materials WHERE domain = ?)", (domain,)),
            "figures": _count_one("SELECT COUNT(*) FROM figures WHERE material_id IN (SELECT material_id FROM materials WHERE domain = ?)", (domain,)),
            "annotations": _count_one("SELECT COUNT(*) FROM annotations WHERE material_id IN (SELECT material_id FROM materials WHERE domain = ?)", (domain,)),
            "wiki_pages": _count_one("SELECT COUNT(*) FROM wiki_pages WHERE domain = ?", (domain,)),
        }
    else:
        counts = {
            "materials": _count_one("SELECT COUNT(*) FROM materials"),
            "chunks": _count_one("SELECT COUNT(*) FROM chunks"),
            "figures": _count_one("SELECT COUNT(*) FROM figures"),
            "annotations": _count_one("SELECT COUNT(*) FROM annotations"),
            "wiki_pages": _count_one("SELECT COUNT(*) FROM wiki_pages"),
        }
    params: tuple[object, ...] = ()
    sql = "SELECT domain, collection, COUNT(*) AS c FROM materials"
    if domain:
        sql += " WHERE domain = ?"
        params = (domain,)
    sql += " GROUP BY domain, collection ORDER BY domain, collection"
    try:
        domain_rows = _index_rows(sql, params)
    except sqlite3.OperationalError:
        domain_rows = []
    collections = [
        {"domain": row["domain"], "collection": row["collection"], "material_count": int(row["c"])}
        for row in domain_rows
    ]

    stamps = {
        "compile": _read_json_if_exists(derived_dir / "compile_stamp.json"),
        "memory_bridge": None,
        "global_bridge": {},
    }
    domain_stamp_paths = sorted((derived_dir / "domains").glob("*/global_bridge_stamp.json"))
    if domain_stamp_paths:
        stamps["global_bridge"] = {
            path.parent.name: _read_json_if_exists(path)
            for path in domain_stamp_paths
        }
    else:
        legacy_bridge_stamp = _read_json_if_exists(derived_dir / "global_bridge_stamp.json")
        stamps["global_bridge"] = legacy_bridge_stamp
    try:
        from arquimedes.memory import read_memory_bridge_stamp

        stamps["memory_bridge"] = read_memory_bridge_stamp(project_root=project_root) or None
    except Exception:
        stamps["memory_bridge"] = None

    return {
        "project_root": str(project_root),
        "index_path": str(index_path),
        "index_exists": index_path.exists(),
        "counts": counts,
        "collections": collections,
        "domain_filter": domain or None,
        "stamps": stamps,
    }
