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


def scan_library(library_root: Path) -> list[Path]:
    """Find all supported files in the library, recursively.

    Match extensions case-insensitively so mixed-case names like ``Draft.Docx``
    are treated the same as ``Draft.docx``.
    """
    files = [p for p in library_root.rglob("*") if p.is_file() and p.suffix.lower() in SUPPORTED_EXTENSIONS]
    return sorted(files)


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

    if not library_root.exists():
        raise FileNotFoundError(f"Library root does not exist: {library_root}")

    # Determine what to scan
    if path is None:
        files = scan_library(library_root)
    else:
        raw_targets = [path] if isinstance(path, str) else list(path)
        files = []
        for raw_target in raw_targets:
            target = Path(raw_target)
            if not target.is_absolute():
                target = library_root / target
            if target.is_file():
                files.append(target)
            elif target.is_dir():
                files.extend(scan_library(target))
            else:
                raise FileNotFoundError(f"Path does not exist: {target}")
        files = list(dict.fromkeys(files))

    # Load existing manifest
    manifest = load_manifest(project_root)
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

        # Refresh path-derived scope for already-registered materials.
        if material_id in manifest:
            existing = manifest[material_id]
            try:
                relative = file_path.resolve().relative_to(library_root.resolve())
            except ValueError:
                relative = Path(file_path.name)
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
            try:
                relative = file_path.resolve().relative_to(library_root.resolve())
            except ValueError:
                relative = Path(file_path.name)
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
        try:
            relative = file_path.resolve().relative_to(library_root.resolve())
        except ValueError:
            relative = Path(file_path.name)

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
