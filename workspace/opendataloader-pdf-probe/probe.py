"""Run a minimal OpenDataLoader PDF extraction against an Arquimedes vault item."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path


DEFAULT_LIBRARY_ROOT = "~/Library/Mobile Documents/com~apple~CloudDocs/Arquimedes"


def _read_jsonl(path: Path) -> list[dict]:
    rows: list[dict] = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def _select_material(manifest_path: Path, material_id: str | None) -> dict:
    rows = [row for row in _read_jsonl(manifest_path) if row.get("file_type") == "pdf"]
    if not rows:
        raise SystemExit(f"No PDF materials found in {manifest_path}")
    if material_id:
        for row in rows:
            if row.get("material_id") == material_id:
                return row
        raise SystemExit(f"Material {material_id} not found in {manifest_path}")
    return rows[0]


def _load_library_root(vault: Path) -> Path:
    # Keep the probe dependency-free: the shared vault config currently stores
    # library_root as a simple top-level scalar, so this small parser is enough.
    config_path = vault / "config" / "config.yaml"
    if config_path.exists():
        for line in config_path.read_text(encoding="utf-8").splitlines():
            if line.startswith("library_root:"):
                raw = line.split(":", 1)[1].strip().strip("\"'")
                return Path(raw).expanduser()
    return Path(DEFAULT_LIBRARY_ROOT).expanduser()


def _java_status() -> dict:
    java = shutil.which("java")
    if not java:
        return {"available": False, "error": "java not found on PATH"}
    completed = subprocess.run(
        [java, "-version"],
        check=False,
        capture_output=True,
        text=True,
    )
    return {
        "available": completed.returncode == 0,
        "path": java,
        "version": (completed.stderr or completed.stdout).strip(),
    }


def _summarize_existing(vault: Path, material_id: str) -> dict:
    extracted = vault / "extracted" / material_id
    pages_path = extracted / "pages.jsonl"
    text_path = extracted / "text.md"
    tables_path = extracted / "tables.jsonl"
    annotations_path = extracted / "annotations.jsonl"

    pages = _read_jsonl(pages_path) if pages_path.exists() else []
    return {
        "extracted_dir": str(extracted),
        "page_count": len(pages),
        "text_chars": len(text_path.read_text(encoding="utf-8")) if text_path.exists() else 0,
        "tables": len(_read_jsonl(tables_path)) if tables_path.exists() else 0,
        "annotations": len(_read_jsonl(annotations_path)) if annotations_path.exists() else 0,
        "first_page_preview": (pages[0].get("text", "")[:600] if pages else ""),
    }


def _summarize_opendataloader(output_dir: Path) -> dict:
    files = sorted(path for path in output_dir.rglob("*") if path.is_file())
    summary = {
        "output_dir": str(output_dir),
        "files": [str(path.relative_to(output_dir)) for path in files],
        "markdown_chars": 0,
        "json_files": 0,
        "first_markdown_preview": "",
    }
    for path in files:
        if path.suffix.lower() == ".json":
            summary["json_files"] += 1
        if path.suffix.lower() in {".md", ".markdown"}:
            text = path.read_text(encoding="utf-8", errors="replace")
            summary["markdown_chars"] += len(text)
            if not summary["first_markdown_preview"]:
                summary["first_markdown_preview"] = text[:600]
    return summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--vault", type=Path, required=True)
    parser.add_argument("--material-id")
    parser.add_argument("--output-dir", type=Path, default=Path(__file__).parent / "runs")
    args = parser.parse_args()

    vault = args.vault.expanduser().resolve()
    manifest_path = vault / "manifests" / "materials.jsonl"
    material = _select_material(manifest_path, args.material_id)
    library_root = _load_library_root(vault)
    pdf_path = library_root / material["relative_path"]
    if not pdf_path.exists():
        raise SystemExit(f"PDF not found: {pdf_path}")

    run_dir = args.output_dir / material["material_id"]
    od_dir = run_dir / "opendataloader"
    od_dir.mkdir(parents=True, exist_ok=True)

    report = {
        "material": material,
        "pdf_path": str(pdf_path),
        "java": _java_status(),
        "existing_arquimedes": _summarize_existing(vault, material["material_id"]),
    }

    try:
        import opendataloader_pdf
    except ImportError as exc:
        report["opendataloader"] = {"status": "missing_python_package", "error": str(exc)}
    else:
        try:
            opendataloader_pdf.convert(
                input_path=[str(pdf_path)],
                output_dir=str(od_dir),
                format="markdown,json",
            )
            report["opendataloader"] = {
                "status": "ok",
                "summary": _summarize_opendataloader(od_dir),
            }
        except Exception as exc:
            report["opendataloader"] = {
                "status": "failed",
                "error": f"{type(exc).__name__}: {exc}",
                "summary": _summarize_opendataloader(od_dir),
            }

    report_path = run_dir / "report.json"
    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    print(f"\nWrote {report_path}")
    return 0 if report.get("opendataloader", {}).get("status") == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
