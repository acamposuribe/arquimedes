"""Thumbnail-based metadata correction stage."""

from __future__ import annotations

import json
from pathlib import Path

from arquimedes import enrich_prompts, enrich_stamps
from arquimedes.llm import EnrichmentError, get_model_id, parse_json_or_repair

_METADATA_FIX_SCHEMA = {
    "type": "object",
    "properties": {
        "title": {"type": "string"},
        "authors": {"type": "array", "items": {"type": "string"}},
        "year": {"type": "string"},
        "_finished": {"type": "boolean"},
    },
    "required": ["title", "authors", "year", "_finished"],
}


def _load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    records = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            records.append(json.loads(line))
    return records


def _standalone_image_pages(output_dir: Path) -> list[dict]:
    """Build a synthetic page thumbnail from a standalone image figure sidecar."""
    figures_dir = output_dir / "figures"
    if not figures_dir.exists():
        return []
    for sidecar_path in sorted(figures_dir.glob("*.json")):
        try:
            sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        image_rel = str(sidecar.get("image_path", "") or "")
        if image_rel and (output_dir / image_rel).exists():
            return [{"page_number": 1, "text": "", "thumbnail_path": image_rel}]
    return []


def enrich_metadata_stage(
    output_dir: Path,
    config: dict,
    llm_fn,
    *,
    force: bool = False,
) -> dict:
    """Correct title/authors/year from the first page thumbnails."""
    enrichment_config = config.get("enrichment", {})
    model = get_model_id(config, "metadata")
    prompt_version = f"{enrichment_config.get('prompt_version', 'enrich-v1.0')}-metadata"
    schema_version = enrichment_config.get("enrichment_schema_version", "1")

    try:
        fingerprint = enrich_stamps.metadata_fingerprint(output_dir)
    except Exception as exc:
        return {"status": "failed", "detail": f"Fingerprint error: {exc}"}

    stamp = enrich_stamps.make_stamp(prompt_version, model, schema_version, fingerprint)
    existing_stamp = enrich_stamps.read_metadata_fix_stamp(output_dir)
    if not force and not enrich_stamps.is_stale(existing_stamp, stamp):
        return {"status": "skipped", "detail": "up to date"}

    try:
        meta_path = output_dir / "meta.json"
        meta = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.exists() else {}
        pages = _load_jsonl(output_dir / "pages.jsonl") or _standalone_image_pages(output_dir)
    except Exception as exc:
        return {"status": "failed", "detail": f"Load error: {exc}"}

    if not any(str(page.get("thumbnail_path", "") or "").strip() for page in pages[:4]):
        return {"status": "skipped", "detail": "no thumbnails available"}

    try:
        system, messages = enrich_prompts.build_metadata_fix_prompt(meta, pages, output_dir)
        raw_text = llm_fn(system, messages)
        parsed = parse_json_or_repair(llm_fn, raw_text, _METADATA_FIX_SCHEMA)
    except EnrichmentError as exc:
        return {"status": "failed", "detail": str(exc)}
    except Exception as exc:
        return {"status": "failed", "detail": f"LLM error: {exc}"}

    if not isinstance(parsed, dict):
        return {"status": "failed", "detail": "LLM did not return a JSON object"}
    if parsed.get("_finished") is not True:
        return {"status": "failed", "detail": "LLM output missing _finished=true"}

    title = str(parsed.get("title", meta.get("title", "")) or meta.get("title", "")).strip()
    year = str(parsed.get("year", meta.get("year", "")) or meta.get("year", "")).strip()
    authors_raw = parsed.get("authors", meta.get("authors", []))
    if isinstance(authors_raw, str):
        authors = [part.strip() for part in authors_raw.split(",") if part.strip()]
    elif isinstance(authors_raw, list):
        authors = [str(part).strip() for part in authors_raw if str(part).strip()]
    else:
        authors = list(meta.get("authors", []))

    meta_out = dict(meta)
    changed_fields: list[str] = []
    if title and title != meta.get("title", ""):
        meta_out["title"] = title
        changed_fields.append("title")
    if authors and authors != meta.get("authors", []):
        meta_out["authors"] = authors
        changed_fields.append("authors")
    if year and year != meta.get("year", ""):
        meta_out["year"] = year
        changed_fields.append("year")

    actual_model = getattr(llm_fn, "last_model", model)
    stamp["model"] = actual_model
    meta_out["_metadata_fix_stamp"] = stamp

    tmp_meta = (output_dir / "meta.json").with_suffix(".json.tmp")
    try:
        tmp_meta.write_text(
            json.dumps(meta_out, ensure_ascii=False, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
        tmp_meta.replace(output_dir / "meta.json")
    except Exception as exc:
        try:
            tmp_meta.unlink(missing_ok=True)
        except Exception:
            pass
        return {"status": "failed", "detail": f"Write error: {exc}"}

    if changed_fields:
        return {"status": "enriched", "detail": f"updated {', '.join(changed_fields)}"}
    return {"status": "enriched", "detail": "verified current metadata"}