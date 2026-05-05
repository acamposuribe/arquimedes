"""Scan the raw materials library and register new materials."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from arquimedes.config import get_enabled_domains, get_library_root, get_project_root, load_config
from arquimedes.extract_image import _is_likely_scanned_document
from arquimedes.models import MaterialManifest, compute_file_hash, compute_material_id

# File extensions we can process
PDF_EXTENSIONS = {".pdf"}
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".tiff", ".tif", ".bmp", ".webp"}
TEXT_EXTENSIONS = {".txt"}
MARKDOWN_EXTENSIONS = {".md", ".markdown"}
DOCX_EXTENSIONS = {".docx"}
PPTX_EXTENSIONS = {".pptx"}
XLSX_EXTENSIONS = {".xlsx"}
SUPPORTED_EXTENSIONS = (
    PDF_EXTENSIONS
    | IMAGE_EXTENSIONS
    | TEXT_EXTENSIONS
    | MARKDOWN_EXTENSIONS
    | DOCX_EXTENSIONS
    | PPTX_EXTENSIONS
    | XLSX_EXTENSIONS
)
IGNORED_FOLDER_NAME_SUBSTRINGS = ("previos",)


def _is_in_ignored_folder(path: Path) -> bool:
    """Return True when any parent folder should be ignored by ingest."""
    return any(
        ignored in part.casefold()
        for part in path.parts[:-1]
        for ignored in IGNORED_FOLDER_NAME_SUBSTRINGS
    )


def _detect_file_type(path: Path) -> str:
    """Detect file type from extension and content heuristics.

    Returns: 'pdf', 'scanned_document', 'image', 'text', 'markdown', 'docx',
    'pptx', 'xlsx', or 'unknown'.
    """
    ext = path.suffix.lower()
    if ext in PDF_EXTENSIONS:
        return "pdf"
    if ext in IMAGE_EXTENSIONS:
        if _is_likely_scanned_document(path):
            return "scanned_document"
        return "image"
    if ext in TEXT_EXTENSIONS:
        return "text"
    if ext in MARKDOWN_EXTENSIONS:
        return "markdown"
    if ext in DOCX_EXTENSIONS:
        return "docx"
    if ext in PPTX_EXTENSIONS:
        return "pptx"
    if ext in XLSX_EXTENSIONS:
        return "xlsx"
    return "unknown"


DOMAIN_FOLDERS = {"research", "practice", "proyectos"}


def _derive_domain(relative_path: Path, enabled_domains: set[str] | None = None) -> str:
    """Derive an enabled domain from the top-level LIBRARY_ROOT folder."""
    parts = relative_path.parts
    if not parts:
        return ""
    top = parts[0].lower()
    domain_folders = enabled_domains if enabled_domains is not None else DOMAIN_FOLDERS
    if top in domain_folders:
        return top
    return ""


def _derive_collection(relative_path: Path, enabled_domains: set[str] | None = None) -> str:
    """Derive collection name from second-level subfolder (within domain folder)."""
    parts = relative_path.parts
    top = parts[0].lower() if parts else ""
    domain_folders = enabled_domains if enabled_domains is not None else DOMAIN_FOLDERS

    if top in domain_folders:
        # Domain folder is the first level; collection is the second
        if len(parts) <= 2:
            return "_general"
        return parts[1]
    else:
        # File outside enabled domain folders — use first-level as collection
        if len(parts) <= 1:
            return "_general"
        return parts[0]


def load_manifest(project_root: Path) -> dict[str, MaterialManifest]:
    """Load existing manifest, keyed by material_id."""
    manifest_path = project_root / "manifests" / "materials.jsonl"
    materials: dict[str, MaterialManifest] = {}
    if manifest_path.exists():
        for line in manifest_path.read_text().strip().splitlines():
            if line.strip():
                m = MaterialManifest.from_json_line(line)
                materials[m.material_id] = m
    return materials


def save_manifest(project_root: Path, materials: dict[str, MaterialManifest]) -> None:
    """Write manifest to disk, sorted by material_id for stable diffs."""
    manifest_path = project_root / "manifests" / "materials.jsonl"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    lines = [m.to_json_line() for m in sorted(materials.values(), key=lambda m: m.material_id)]
    manifest_path.write_text("\n".join(lines) + "\n" if lines else "")


def ignored_materials_path(project_root: Path) -> Path:
    return project_root / "manifests" / "ignored_materials.jsonl"


def load_ignored_material_hashes(project_root: Path) -> set[str]:
    path = ignored_materials_path(project_root)
    hashes: set[str] = set()
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            file_hash = str(row.get("file_hash") or "").strip()
            if file_hash:
                hashes.add(file_hash)
    return hashes


def add_ignored_material(project_root: Path, *, material_id: str, file_hash: str, relative_path: str, reason: str = "admin_delete") -> None:
    path = ignored_materials_path(project_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = []
    if path.exists():
        existing = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    row = {
        "material_id": material_id,
        "file_hash": file_hash,
        "relative_path": relative_path,
        "reason": reason,
        "ignored_at": datetime.now(timezone.utc).isoformat(),
    }
    rows = []
    seen_hashes = set()
    for line in existing:
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError:
            rows.append(line)
            continue
        seen_hashes.add(str(parsed.get("file_hash") or ""))
        rows.append(json.dumps(parsed, ensure_ascii=False))
    if file_hash not in seen_hashes:
        rows.append(json.dumps(row, ensure_ascii=False))
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")


def _ignored_extensions(config: dict | None = None) -> set[str]:
    """Return configured file extensions to ignore during ingest.

    Config shape::

        ingest:
          ignore_extensions: [.docx, pptx]

    Values are normalized case-insensitively and may be written with or without
    a leading dot.
    """
    ingest_config = (config or {}).get("ingest") or {}
    raw = ingest_config.get("ignore_extensions") or ingest_config.get("ignored_extensions") or []
    if isinstance(raw, str):
        raw = [raw]
    ignored: set[str] = set()
    try:
        values = list(raw)
    except TypeError:
        values = []
    for value in values:
        ext = str(value or "").strip().lower()
        if not ext:
            continue
        if not ext.startswith("."):
            ext = f".{ext}"
        ignored.add(ext)
    return ignored


def scan_library(library_root: Path, ignored_extensions: set[str] | None = None) -> list[Path]:
    """Find all supported files in the library, recursively.

    Match extensions case-insensitively so mixed-case names like ``Draft.Docx``
    are treated the same as ``Draft.docx``. Follow symlinked directories so a
    project can link to source folders stored outside the library root without
    duplicating them. Directory realpaths are tracked to avoid symlink loops.
    """
    files: list[Path] = []
    ignored = ignored_extensions or set()
    visited_dirs: set[Path] = set()

    def walk(directory: Path) -> None:
        if any(ignored in directory.name.casefold() for ignored in IGNORED_FOLDER_NAME_SUBSTRINGS):
            return
        try:
            real_dir = directory.resolve(strict=True)
        except OSError:
            return
        if real_dir in visited_dirs:
            return
        visited_dirs.add(real_dir)

        try:
            children = list(directory.iterdir())
        except OSError:
            return

        for child in children:
            try:
                if child.is_dir():
                    walk(child)
                elif (
                    child.is_file()
                    and child.suffix.lower() in SUPPORTED_EXTENSIONS
                    and child.suffix.lower() not in ignored
                ):
                    files.append(child)
            except OSError:
                continue

    walk(library_root)
    return sorted(files, key=lambda p: str(p))


def _relative_to_library(file_path: Path, library_root: Path) -> Path:
    """Return the lexical path of a material relative to the library root.

    Do not resolve symlinks here: for a file reached through
    ``Proyectos/<project>/linked-folder``, the symlink path is the canonical
    project placement even when the bytes live outside ``library_root``.
    """
    try:
        return file_path.relative_to(library_root)
    except ValueError:
        try:
            return file_path.absolute().relative_to(library_root.absolute())
        except ValueError:
            return Path(file_path.name)


def _refresh_extracted_scope(project_root: Path, entry: MaterialManifest) -> None:
    meta_path = project_root / "extracted" / entry.material_id / "meta.json"
    if not meta_path.exists():
        return
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return
    if not isinstance(meta, dict):
        return
    changed = False
    for key, value in {
        "file_hash": entry.file_hash,
        "source_path": entry.relative_path,
        "domain": entry.domain,
        "collection": entry.collection,
        "ingested_at": entry.ingested_at,
    }.items():
        if meta.get(key) != value:
            meta[key] = value
            changed = True
    if changed:
        meta_path.write_text(json.dumps(meta, separators=(",", ":"), ensure_ascii=False), encoding="utf-8")


def ingest(
    path: str | list[str] | tuple[str, ...] | None = None,
    config: dict | None = None,
) -> list[MaterialManifest]:
    """Scan the library for new materials and register them in the manifest.

    Args:
        path: Optional specific file or directory to ingest (relative to library root
              or absolute). May also be a list/tuple of explicit file or directory
              paths. If None, scans the entire library.
        config: Optional config dict. Loaded from disk if not provided.

    Returns:
        List of newly registered MaterialManifest entries.
    """
    if config is None:
        config = load_config()

    library_root = get_library_root(config)
    project_root = get_project_root()
    enabled_domains = get_enabled_domains(config)
    ignored_extensions = _ignored_extensions(config)

    if not library_root.exists():
        raise FileNotFoundError(f"Library root does not exist: {library_root}")

    # Determine what to scan
    if path is None:
        files = scan_library(library_root, ignored_extensions=ignored_extensions)
    else:
        raw_targets = [path] if isinstance(path, str) else list(path)
        files = []
        for raw_target in raw_targets:
            target = Path(raw_target)
            if not target.is_absolute():
                target = library_root / target
            if target.is_file():
                if target.suffix.lower() not in ignored_extensions and not _is_in_ignored_folder(target):
                    files.append(target)
            elif target.is_dir():
                if not any(
                    ignored in part.casefold()
                    for part in target.parts
                    for ignored in IGNORED_FOLDER_NAME_SUBSTRINGS
                ):
                    files.extend(scan_library(target, ignored_extensions=ignored_extensions))
            else:
                raise FileNotFoundError(f"Path does not exist: {target}")
        files = list(dict.fromkeys(files))

    # Load existing manifest
    manifest = load_manifest(project_root)
    ignored_hashes = load_ignored_material_hashes(project_root)
    existing_hashes = {m.file_hash for m in manifest.values()}
    existing_by_hash = {m.file_hash: m for m in manifest.values()}

    new_materials: list[MaterialManifest] = []
    manifest_changed = False

    for file_path in files:
        file_type = _detect_file_type(file_path)
        if file_type == "unknown":
            continue

        # Compute identity
        material_id = compute_material_id(file_path)
        file_hash = compute_file_hash(file_path)

        if file_hash in ignored_hashes:
            continue

        # Refresh path-derived scope for already-registered materials.
        if material_id in manifest:
            existing = manifest[material_id]
            relative = _relative_to_library(file_path, library_root)
            domain = _derive_domain(relative, enabled_domains)
            collection = _derive_collection(relative, enabled_domains)
            if domain and (
                existing.relative_path != str(relative)
                or existing.domain != domain
                or existing.collection != collection
            ):
                manifest[material_id] = MaterialManifest(
                    material_id=existing.material_id,
                    file_hash=file_hash,
                    relative_path=str(relative),
                    file_type=existing.file_type,
                    domain=domain,
                    collection=collection,
                    ingested_at=datetime.now(timezone.utc).isoformat(),
                    ingested_by=existing.ingested_by,
                )
                _refresh_extracted_scope(project_root, manifest[material_id])
                manifest_changed = True
            continue

        # Also check by hash for safety
        if file_hash in existing_hashes:
            existing = existing_by_hash[file_hash]
            relative = _relative_to_library(file_path, library_root)
            domain = _derive_domain(relative, enabled_domains)
            collection = _derive_collection(relative, enabled_domains)
            if domain and (
                existing.material_id == material_id
                or existing.relative_path != str(relative)
                or existing.domain != domain
                or existing.collection != collection
            ):
                manifest[existing.material_id] = MaterialManifest(
                    material_id=existing.material_id,
                    file_hash=file_hash,
                    relative_path=str(relative),
                    file_type=existing.file_type,
                    domain=domain,
                    collection=collection,
                    ingested_at=datetime.now(timezone.utc).isoformat(),
                    ingested_by=existing.ingested_by,
                )
                _refresh_extracted_scope(project_root, manifest[existing.material_id])
                manifest_changed = True
            continue

        # Derive collection from subfolder structure
        relative = _relative_to_library(file_path, library_root)

        domain = _derive_domain(relative, enabled_domains)
        collection = _derive_collection(relative, enabled_domains)

        if not domain:
            print(f"  Warning: {relative} is not inside Research/ or Practice/ — skipping")
            continue

        entry = MaterialManifest(
            material_id=material_id,
            file_hash=file_hash,
            relative_path=str(relative),
            file_type=file_type,
            domain=domain,
            collection=collection,
            ingested_at=datetime.now(timezone.utc).isoformat(),
        )

        manifest[material_id] = entry
        new_materials.append(entry)
        manifest_changed = True

    # Save updated manifest
    if manifest_changed:
        save_manifest(project_root, manifest)

    return new_materials
