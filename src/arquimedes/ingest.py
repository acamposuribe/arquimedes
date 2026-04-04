"""Scan the raw materials library and register new materials."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from arquimedes.config import get_library_root, get_project_root, load_config
from arquimedes.extract_image import _is_likely_scanned_document
from arquimedes.models import MaterialManifest, compute_file_hash, compute_material_id

# File extensions we can process
PDF_EXTENSIONS = {".pdf"}
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".tiff", ".tif", ".bmp", ".webp"}
SUPPORTED_EXTENSIONS = PDF_EXTENSIONS | IMAGE_EXTENSIONS


def _detect_file_type(path: Path) -> str:
    """Detect file type from extension and content heuristics.

    Returns: 'pdf', 'scanned_document', 'image', or 'unknown'.
    """
    ext = path.suffix.lower()
    if ext in PDF_EXTENSIONS:
        return "pdf"
    if ext in IMAGE_EXTENSIONS:
        if _is_likely_scanned_document(path):
            return "scanned_document"
        return "image"
    return "unknown"


DOMAIN_FOLDERS = {"research", "practice"}


def _derive_domain(relative_path: Path) -> str:
    """Derive domain from top-level LIBRARY_ROOT folder.

    LIBRARY_ROOT must contain Research/ and Practice/ folders.
    Returns 'research' or 'practice', or '' if file is outside those folders.
    """
    parts = relative_path.parts
    if not parts:
        return ""
    top = parts[0].lower()
    if top in DOMAIN_FOLDERS:
        return top
    return ""


def _derive_collection(relative_path: Path) -> str:
    """Derive collection name from second-level subfolder (within domain folder).

    Structure: LIBRARY_ROOT/Research/<collection>/file.pdf
    Files directly inside the domain folder get collection '_general'.
    """
    parts = relative_path.parts
    top = parts[0].lower() if parts else ""

    if top in DOMAIN_FOLDERS:
        # Domain folder is the first level; collection is the second
        if len(parts) <= 2:
            return "_general"
        return parts[1]
    else:
        # File outside domain folders — use first-level as collection
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
    """Find all supported files in the library, recursively."""
    files = []
    for ext in SUPPORTED_EXTENSIONS:
        files.extend(library_root.rglob(f"*{ext}"))
    # Also catch uppercase extensions
    for ext in SUPPORTED_EXTENSIONS:
        files.extend(library_root.rglob(f"*{ext.upper()}"))
    # Deduplicate (case-insensitive filesystems may double-count)
    seen = set()
    unique = []
    for f in files:
        resolved = f.resolve()
        if resolved not in seen:
            seen.add(resolved)
            unique.append(f)
    return sorted(unique)


def ingest(
    path: str | None = None,
    config: dict | None = None,
) -> list[MaterialManifest]:
    """Scan the library for new materials and register them in the manifest.

    Args:
        path: Optional specific file or directory to ingest (relative to library root
              or absolute). If None, scans the entire library.
        config: Optional config dict. Loaded from disk if not provided.

    Returns:
        List of newly registered MaterialManifest entries.
    """
    if config is None:
        config = load_config()

    library_root = get_library_root(config)
    project_root = get_project_root()

    if not library_root.exists():
        raise FileNotFoundError(f"Library root does not exist: {library_root}")

    # Determine what to scan
    if path:
        target = Path(path)
        if not target.is_absolute():
            target = library_root / target
        if target.is_file():
            files = [target]
        elif target.is_dir():
            files = scan_library(target)
        else:
            raise FileNotFoundError(f"Path does not exist: {target}")
    else:
        files = scan_library(library_root)

    # Load existing manifest
    manifest = load_manifest(project_root)
    existing_hashes = {m.file_hash for m in manifest.values()}

    new_materials: list[MaterialManifest] = []

    for file_path in files:
        file_type = _detect_file_type(file_path)
        if file_type == "unknown":
            continue

        # Compute identity
        material_id = compute_material_id(file_path)

        # Skip if already registered (same content)
        if material_id in manifest:
            continue

        # Also check by hash for safety
        file_hash = compute_file_hash(file_path)
        if file_hash in existing_hashes:
            continue

        # Derive collection from subfolder structure
        try:
            relative = file_path.resolve().relative_to(library_root.resolve())
        except ValueError:
            relative = Path(file_path.name)

        domain = _derive_domain(relative)
        collection = _derive_collection(relative)

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

    # Save updated manifest
    if new_materials:
        save_manifest(project_root, manifest)

    return new_materials
