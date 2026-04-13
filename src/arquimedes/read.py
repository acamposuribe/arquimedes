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
            SELECT 'concept' AS page_type, cluster_id AS page_id, canonical_name AS title,
                   wiki_path AS path, 'shared' AS domain, 'bridge-concepts' AS collection
            FROM concept_clusters
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
        JOIN (
            SELECT material_id FROM local_cluster_materials WHERE cluster_id = ?
            UNION
            SELECT material_id FROM cluster_materials WHERE cluster_id = ?
        ) scoped ON scoped.material_id = m.material_id
        ORDER BY m.title
        """,
        (cluster_id, cluster_id),
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


def list_domains_and_collections() -> list[dict]:
    rows = _index_rows(
        "SELECT DISTINCT domain, collection FROM materials ORDER BY domain, collection"
    )
    return [{"domain": row["domain"], "collection": row["collection"]} for row in rows]


def list_glossary_concepts() -> list[dict]:
    try:
        _, body = load_wiki_page("shared/glossary")
    except FileNotFoundError:
        return []
    return [{"name": name.removesuffix(" (main)"), "path": path} for name, path in re.findall(r"\[([^\]]+)\]\((wiki/shared/bridge-concepts/[^)]+\.md)\)", body)]


def materials_for_collection(domain: str, collection: str) -> list[dict]:
    rows = _index_rows(
        "SELECT material_id, title FROM materials WHERE domain=? AND collection=? ORDER BY title",
        (domain, collection),
    )
    return [dict(row) for row in rows]


def recent_materials(limit: int = 10) -> list[dict]:
    rows = _index_rows(
        "SELECT material_id, title, summary, domain, collection, document_type, year FROM materials ORDER BY rowid DESC LIMIT ?",
        (limit,),
    )
    return [dict(row) for row in rows]
