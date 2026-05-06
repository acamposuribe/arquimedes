"""Document enrichment stage — Phase 3.

Enriches document-level metadata. Research/practice materials get summary, document_type,
keywords, reflections, facets, and concepts. Project dossier materials get summary,
document_type, keywords, facets, and project_extraction, but no reflective learning fields
or concept candidates. Ordinary enriched fields keep only the value; stage provenance lives
in stamps. Concept candidates keep source provenance because clustering and search depend on it.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from arquimedes import enrich_prompts, enrich_stamps, llm, project_state
from arquimedes.config import get_project_root
from arquimedes.domain_profiles import domain_prompt_version, is_proyectos_domain
from arquimedes.llm import get_model_id
from arquimedes.models import (
    ArchitectureFacets,
    ConceptCandidate,
    EnrichedField,
    Provenance,
)


# ---------------------------------------------------------------------------
# Helpers — load artifacts
# ---------------------------------------------------------------------------


def _load_jsonl(path: Path) -> list[dict]:
    """Load a .jsonl file into a list of dicts. Returns [] if absent."""
    if not path.exists():
        return []
    records = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            records.append(json.loads(line))
    return records


def _load_json(path: Path, default=None):
    """Load a JSON file. Returns default if absent."""
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _save_failed_response(output_dir: Path, stage: str, raw_text: str, detail: str) -> None:
    debug_dir = output_dir / "debug"
    debug_dir.mkdir(exist_ok=True)
    response_path = debug_dir / f"{stage}.failed.response.txt"
    meta_path = debug_dir / f"{stage}.failed.meta.json"
    response_path.write_text(raw_text or "", encoding="utf-8")
    meta_path.write_text(
        json.dumps(
            {
                "failed_at": datetime.now(timezone.utc).isoformat(),
                "detail": detail,
                "response_chars": len(raw_text or ""),
            },
            separators=(",", ":"),
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


def _failed_with_response(output_dir: Path, raw_text: str, detail: str) -> dict:
    try:
        _save_failed_response(output_dir, "document", raw_text, detail)
    except Exception:
        pass
    return {"status": "failed", "detail": detail}


def _is_project_drawing_set(meta: dict) -> bool:
    """Return True for Proyectos drawing-set/planos materials."""
    if not is_proyectos_domain(str(meta.get("domain", ""))):
        return False
    doc_type = meta.get("document_type")
    if isinstance(doc_type, dict):
        doc_type = doc_type.get("value")
    project_type = (meta.get("project_extraction") or {}).get("project_material_type")
    return "drawing_set" in {str(meta.get("raw_document_type") or ""), str(doc_type or ""), str(project_type or "")}


def _project_page_image_paths(output_dir: Path, *, short_doc_limit: int = 6, long_doc_limit: int = 4) -> list[Path]:
    """Select page thumbnails for Proyectos document enrichment.

    Before document enrichment we may not know whether a PDF is Planos. Attach a
    small visual sample for all Proyectos PDFs so the model can classify visual
    materials from what is actually visible, not only defective extracted text.
    """
    pages = sorted(
        [page for page in _load_jsonl(output_dir / "pages.jsonl") if isinstance(page, dict)],
        key=lambda page: int(page.get("page_number", 0) or 0),
    )
    max_images = short_doc_limit if len(pages) <= short_doc_limit else long_doc_limit
    selected = pages[:max_images]
    paths: list[Path] = []
    for page in selected:
        thumbnail_rel = str(page.get("thumbnail_path") or "")
        if not thumbnail_rel:
            continue
        image_path = output_dir / thumbnail_rel
        if image_path.exists():
            paths.append(image_path)
    return paths


def _project_visual_config(config: dict) -> dict:
    """Return configured visual-page limits for Proyectos document enrichment."""
    enrichment = config.get("enrichment", {}) if isinstance(config, dict) else {}
    visual = dict(enrichment.get("proyectos_visual_pages") or {})
    return {
        "max_images_short": int(visual.get("max_images_short", 6) or 6),
        "max_images_long": int(visual.get("max_images_long", 4) or 4),
        "max_width": int(visual.get("max_width", 900) or 900),
        "max_image_bytes": int(visual.get("max_image_bytes", 1_500_000) or 1_500_000),
        "jpeg_quality": int(visual.get("jpeg_quality", 82) or 82),
    }


def _primary_image_paths(output_dir: Path, meta: dict, config: dict | None = None) -> list[Path]:
    """Return primary image paths for multimodal document enrichment.

    Proyectos PDFs get a small page-thumbnail sample because before enrichment
    we may not yet know whether they are drawings. Plain image materials also
    need visual input. Scanned documents should use OCR text when chunking
    succeeded; attach the image only when there are no chunks.
    """
    file_type = meta.get("file_type")
    if is_proyectos_domain(str(meta.get("domain", ""))) and file_type in {"pdf", "scanned_document"}:
        visual = _project_visual_config(config or {})
        return _project_page_image_paths(
            output_dir,
            short_doc_limit=visual["max_images_short"],
            long_doc_limit=visual["max_images_long"],
        )

    if file_type not in {"image", "scanned_document"}:
        return []
    if file_type == "scanned_document":
        chunks_path = output_dir / "chunks.jsonl"
        if chunks_path.exists() and any(line.strip() for line in chunks_path.read_text(encoding="utf-8").splitlines()):
            return []
    figures_dir = output_dir / "figures"
    sidecar_path = figures_dir / "fig_0001.json"
    if sidecar_path.exists():
        try:
            sidecar = json.loads(sidecar_path.read_text(encoding="utf-8"))
            image_rel = sidecar.get("image_path") or ""
            if image_rel:
                image_path = Path(image_rel)
                if not image_path.is_absolute():
                    image_path = output_dir / image_path
                if image_path.exists():
                    return [image_path]
        except Exception:
            pass
    for pattern in ("fig_0001.*", "*.jpg", "*.jpeg", "*.png", "*.webp", "*.tif", "*.tiff", "*.bmp"):
        matches = sorted(p for p in figures_dir.glob(pattern) if p.is_file() and p.suffix.lower() != ".json")
        if matches:
            return [matches[0]]
    return []


# ---------------------------------------------------------------------------
# Field-level helpers
# ---------------------------------------------------------------------------


def _make_enriched_field(llm_field: dict, model: str, prompt_version: str, *, confidence: float = 1.0) -> EnrichedField:
    """Build a value-only EnrichedField for ordinary document metadata."""
    del model, prompt_version, confidence
    if isinstance(llm_field, dict) and "value" in llm_field:
        return EnrichedField(value=llm_field["value"])
    return EnrichedField(value=llm_field)


def _make_facets(facets_data: dict, model: str, prompt_version: str) -> ArchitectureFacets:
    """Build an ArchitectureFacets object from the LLM facets dict."""
    facet_fields = [
        "building_type", "scale", "location", "jurisdiction", "climate",
        "program", "material_system", "structural_system", "historical_period",
        "course_topic", "studio_project",
    ]
    kwargs = {}
    for name in facet_fields:
        if name in facets_data and facets_data[name] not in (None, "", []):
            kwargs[name] = _make_enriched_field(facets_data[name], model, prompt_version, confidence=0.7)
    return ArchitectureFacets(**kwargs)


_DOCUMENT_PATCH_SCHEMA = """{
    \"summary\": \"required string\",
    \"document_type\": \"required string\",
    \"keywords\": [\"required strings\"],
    \"methodological_conclusions\": [\"required strings, may be empty\"],
    \"main_content_learnings\": [\"required strings, may be empty\"],
    \"bibliography\": {\"optional bibliographic fields\": \"...\"} or null,
    \"facets\": {\"optional facet fields\": \"...\"},
    \"concepts_local\": [{\"concept_name\": \"...\", \"descriptor\": \"...\", \"relevance\": \"high|medium|low\", \"source_pages\": [1], \"evidence_spans\": [\"...\"]}],
    \"concepts_bridge_candidates\": [{\"concept_name\": \"...\", \"descriptor\": \"...\", \"relevance\": \"high|medium|low\", \"source_pages\": [1], \"evidence_spans\": [\"...\"]}],
    \"toc\": [{\"title\": \"...\", \"level\": 0, \"page\": 1}] or [],
    \"_finished\": true
}"""

_PROJECT_DOCUMENT_PATCH_SCHEMA = """{
    \"title\": \"required string\",
    \"summary\": \"required string\",
    \"document_type\": \"required string\",
    \"keywords\": [\"required strings\"],
    \"bibliography\": {\"optional bibliographic fields\": \"...\"} or null,
    \"facets\": {\"optional facet fields\": \"...\"},
    \"concepts_local\": [],
    \"concepts_bridge_candidates\": [],
    \"toc\": [{\"title\": \"...\", \"level\": 0, \"page\": 1}] or [],
    \"project_extraction\": {
        \"project_material_type\": \"meeting_report|meeting_notes|client_request|authority_request|regulation|drawing_set|technical_report|reference_material|working_document|budget_table|site_photo|map_or_cartography|contract_or_admin|email_or_message_export|schedule|unknown\",
        \"project_phase\": \"lead|feasibility|schematic_design|basic_project|execution_project|tender|construction|handover|archived|unknown\",
        \"drawing_scope\": \"string, for drawing_set only: phase/sheet/scope such as Anteproyecto. Planta baja; otherwise empty string\",
        \"material_date\": \"YYYY-MM-DD when known, especially for site_photo/fotografías de obra; otherwise empty string\",
        \"project_relevance\": \"required string\",
        \"main_points\": [\"strings\"],
        \"decisions\": [\"strings\"],
        \"requirements\": [\"strings\"],
        \"risks_or_blockers\": [\"strings\"],
        \"open_items\": [\"strings\"],
        \"actors\": [\"strings\"],
        \"dates_and_deadlines\": [\"strings\"],
        \"spatial_or_design_scope\": [\"strings\"],
        \"budget_signals\": [\"strings\"],
        \"evidence_refs\": [\"strings\"]
    },
    \"_finished\": true
}"""

_REQUIRED_DOCUMENT_OUTPUT_FIELDS = (
    "summary",
    "document_type",
    "keywords",
    "methodological_conclusions",
    "main_content_learnings",
    "bibliography",
    "facets",
    "concepts_local",
    "concepts_bridge_candidates",
    "toc",
)

_REQUIRED_PROJECT_DOCUMENT_OUTPUT_FIELDS = (
    "summary",
    "document_type",
    "keywords",
    "bibliography",
    "facets",
    "concepts_local",
    "concepts_bridge_candidates",
    "toc",
    "project_extraction",
)


_RELEVANCE_CONFIDENCE = {"high": 1.0, "medium": 0.7, "low": 0.4}

_SUMMARY_REFUSAL_MARKERS = (
    "source text is not available",
    "cannot be reliably enriched",
    "without reading the document",
    "cannot be made",
    "avoid introducing unsupported detail",
    "not available in this session",
)


def _make_concept(concept_data: dict, model: str, prompt_version: str) -> ConceptCandidate:
    """Build a ConceptCandidate from the LLM concepts list entry."""
    relevance = concept_data.get("relevance", "medium")
    confidence = _RELEVANCE_CONFIDENCE.get(str(relevance).lower(), 0.7)
    provenance = Provenance.create(
        model=model,
        prompt_version=prompt_version,
        confidence=confidence,
        source_pages=concept_data.get("source_pages", []),
        evidence_spans=concept_data.get("evidence_spans", []),
    )
    return ConceptCandidate(
        concept_name=concept_data.get("concept_name", ""),
        descriptor=concept_data.get("descriptor", ""),
        concept_type=concept_data.get("concept_type", "local"),
        relevance=concept_data.get("relevance", ""),
        provenance=provenance,
    )


def _make_concepts(concepts_data: list[dict], model: str, prompt_version: str, *, concept_type: str) -> list[ConceptCandidate]:
    """Build ConceptCandidate objects from a list of concept dicts."""
    concepts: list[ConceptCandidate] = []
    for concept_data in concepts_data:
        if not isinstance(concept_data, dict) or not concept_data.get("concept_name"):
            continue
        concept = _make_concept(concept_data, model, prompt_version)
        concept.concept_type = concept_type
        concepts.append(concept)
    return concepts


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def enrich_document_stage(
    output_dir: Path,
    config: dict,
    llm_fn,
    *,
    force: bool = False,
) -> dict:
    """Enrich document-level metadata for a single material.

    Args:
        output_dir: Path to extracted/<material_id>/ directory.
        config: Full config dict (enrichment section read from config["enrichment"]).
        llm_fn: Callable (system, messages) -> str. The LLM implementation.
        force: Re-enrich even if not stale.

    Returns:
        {"status": "enriched"|"skipped"|"failed", "detail": str}
    """
    enrichment_config = config.get("enrichment", {})
    model: str = get_model_id(config, "document")
    prompt_version: str = enrichment_config.get("prompt_version", "enrich-v1.0")
    schema_version: str = enrichment_config.get("enrichment_schema_version", "1")

    # 1. Load meta early so we can apply the domain-specific prompt suffix
    #    BEFORE the staleness check; otherwise materials enriched with the base
    #    prompt are spuriously reported "up to date" against domain-suffixed
    #    profiles like proyectos-es-v1.
    try:
        meta = _load_json(output_dir / "meta.json", default={})
    except Exception as exc:
        return {"status": "failed", "detail": f"Load error: {exc}"}

    domain = str(meta.get("domain", ""))
    is_project_domain = is_proyectos_domain(domain)
    prompt_version = domain_prompt_version(prompt_version, domain)
    if is_project_domain and _project_page_image_paths(output_dir):
        prompt_version = f"{prompt_version}-visual-pages-v1"

    # 2. Compute fingerprint and stamp
    try:
        fingerprint = enrich_stamps.document_fingerprint(output_dir)
    except Exception as exc:
        return {"status": "failed", "detail": f"Fingerprint error: {exc}"}

    stamp = enrich_stamps.make_stamp(prompt_version, model, schema_version, fingerprint)

    # 3. Staleness check
    existing_stamp = enrich_stamps.read_document_stamp(output_dir)
    if not force and enrich_stamps.matches_stage_version(existing_stamp, prompt_version, schema_version):
        return {"status": "skipped", "detail": "up to date"}

    # 4. Load remaining artifacts
    try:
        chunks = _load_jsonl(output_dir / "chunks.jsonl")
        annotations = _load_jsonl(output_dir / "annotations.jsonl")
    except Exception as exc:
        return {"status": "failed", "detail": f"Load error: {exc}"}

    # 4. Build prompt and call LLM
    document_text_path = output_dir / "document.work.md"
    try:
        meta_path, document_text_path = enrich_prompts.build_document_input_files(
            output_dir, chunks, annotations
        )
        if is_project_domain:
            project_id = str(meta.get("collection") or "").strip()
            if project_id:
                try:
                    prompt_meta = dict(_load_json(meta_path, default={}))
                    current_stage = str(project_state.load_project_state(project_id, root=get_project_root()).get("stage") or "").strip()
                    if current_stage:
                        prompt_meta["project_context"] = {**dict(prompt_meta.get("project_context") or {}), "current_project_phase": current_stage}
                        meta_path = output_dir / "document.meta.work.json"
                        meta_path.write_text(json.dumps(prompt_meta, ensure_ascii=False, indent=2), encoding="utf-8")
                except Exception:
                    pass

        # LLM reads the source files directly and returns a JSON patch.
        system, messages = enrich_prompts.build_document_file_prompt(
            meta_path,
            document_text_path,
            domain=domain,
            image_paths=_primary_image_paths(output_dir, meta, config),
            visual_config=_project_visual_config(config),
        )
        raw_text = llm_fn(system, messages)
        repair_schema = _PROJECT_DOCUMENT_PATCH_SCHEMA if is_project_domain else _DOCUMENT_PATCH_SCHEMA
        parsed = llm.parse_json_or_repair(llm_fn, raw_text, repair_schema)
        if not isinstance(parsed, dict):
            return _failed_with_response(output_dir, raw_text, "LLM did not return a JSON object")
        if parsed.get("_finished") is not True:
            return _failed_with_response(output_dir, raw_text, "LLM output missing _finished=true")
        parsed = dict(parsed)
        parsed.pop("_finished", None)
        required_output_fields = (
            _REQUIRED_PROJECT_DOCUMENT_OUTPUT_FIELDS if is_project_domain else _REQUIRED_DOCUMENT_OUTPUT_FIELDS
        )
        missing_output_fields = [field for field in required_output_fields if field not in parsed]
        if missing_output_fields:
            return _failed_with_response(
                output_dir,
                raw_text,
                f"LLM output missing required fields: {', '.join(missing_output_fields)}",
            )
    except llm.EnrichmentError as exc:
        if "raw_text" in locals():
            return _failed_with_response(output_dir, raw_text, str(exc))
        return {"status": "failed", "detail": str(exc)}
    except Exception as exc:
        return {"status": "failed", "detail": f"LLM error: {exc}"}
    finally:
        for work_path in (document_text_path, output_dir / "document.meta.work.json"):
            try:
                work_path.unlink(missing_ok=True)
            except Exception:
                pass

    # 5. Normalize flat values into {"value": ...} shape for downstream mapping.
    # The file-based approach has the LLM write plain values; the pre-parsed
    # path still uses the old {"value": ...} wrapper shape.
    def _normalize_field(data: dict, key: str) -> None:
        val = data.get(key)
        if val is not None and not isinstance(val, dict):
            data[key] = {"value": val}

    reflective_fields = () if is_project_domain else ("methodological_conclusions", "main_content_learnings")
    for field in ("summary", "document_type", "keywords", *reflective_fields):
        _normalize_field(parsed, field)

    for field in ("summary", "document_type", "keywords", *reflective_fields):
        value = parsed.get(field)
        if isinstance(value, dict) and "value" not in value:
            return _failed_with_response(output_dir, raw_text, f"LLM output field '{field}' missing 'value'")

    # Normalize each facet value
    facets = parsed.get("facets") or {}
    if isinstance(facets, dict):
        for fkey in list(facets):
            _normalize_field(facets, fkey)
            if isinstance(facets.get(fkey), dict) and "value" not in facets[fkey]:
                return _failed_with_response(output_dir, raw_text, f"LLM output facet '{fkey}' missing 'value'")

    summary_value = parsed.get("summary", {}).get("value") if isinstance(parsed.get("summary"), dict) else None
    if not isinstance(summary_value, str) or not summary_value.strip():
        return _failed_with_response(output_dir, raw_text, "LLM output summary is empty")
    summary_lower = summary_value.lower()
    if any(marker in summary_lower for marker in _SUMMARY_REFUSAL_MARKERS):
        return _failed_with_response(output_dir, raw_text, "LLM output summary is a refusal or no-access response")

    document_type_value = parsed.get("document_type", {}).get("value") if isinstance(parsed.get("document_type"), dict) else None
    if not isinstance(document_type_value, str) or not document_type_value.strip():
        return _failed_with_response(output_dir, raw_text, "LLM output document_type is empty")

    keywords_value = parsed.get("keywords", {}).get("value") if isinstance(parsed.get("keywords"), dict) else None
    if not isinstance(keywords_value, list) or not any(isinstance(item, str) and item.strip() for item in keywords_value):
        return _failed_with_response(output_dir, raw_text, "LLM output keywords are empty")

    # 6. Map parsed JSON to model objects and merge into meta dict
    #    Use actual responding model for provenance (not the config fallback list)
    actual_model: str = getattr(llm_fn, "last_model", model)
    try:
        meta_out = dict(meta)

        enriched_count = {"keywords": 0, "facets": 0, "concepts": 0}

        ef = _make_enriched_field(parsed["summary"], actual_model, prompt_version)
        meta_out["summary"] = ef.to_dict()

        ef = _make_enriched_field(parsed["document_type"], actual_model, prompt_version)
        meta_out["document_type"] = ef.to_dict()

        ef = _make_enriched_field(parsed["keywords"], actual_model, prompt_version)
        meta_out["keywords"] = ef.to_dict()
        enriched_count["keywords"] = len(ef.value) if isinstance(ef.value, list) else 1

        if is_project_domain:
            parsed_title = str(parsed.get("title") or "").strip()
            if parsed_title:
                meta_out["title"] = parsed_title
            # Project dossiers are archival/operational records, not research reflections.
            # Remove stale reflective fields from prior enrichments and keep concepts empty.
            meta_out.pop("methodological_conclusions", None)
            meta_out.pop("main_content_learnings", None)
            project_extraction = parsed.get("project_extraction")
            if not isinstance(project_extraction, dict):
                return _failed_with_response(output_dir, raw_text, "LLM output field 'project_extraction' must be an object")
            meta_out["project_extraction"] = project_extraction
            enriched_count["project_extraction"] = sum(1 for v in project_extraction.values() if v)
        else:
            for field_name in ("methodological_conclusions", "main_content_learnings"):
                field_data = parsed.get(field_name)
                meta_out[field_name] = _make_enriched_field(field_data, actual_model, prompt_version).to_dict()
                enriched_count[field_name] = len(meta_out[field_name]["value"]) if isinstance(meta_out[field_name].get("value"), list) else 1

        facets_data = parsed.get("facets", {})
        if not isinstance(facets_data, dict):
            return _failed_with_response(output_dir, raw_text, "LLM output field 'facets' must be an object")
        if facets_data:
            facets = _make_facets(facets_data, actual_model, prompt_version)
            meta_out["facets"] = facets.to_dict()
            enriched_count["facets"] = sum(1 for v in facets.to_dict().values() if v)
        else:
            meta_out["facets"] = {}

        # bibliography: stored as-is (plain dict of optional strings) plus provenance metadata
        bibliography_data = parsed.get("bibliography")
        if bibliography_data is None:
            meta_out.pop("bibliography", None)
        elif isinstance(bibliography_data, dict):
            # Normalize editors: LLM may return a comma-separated string instead of array
            editors = bibliography_data.get("editors")
            if isinstance(editors, str) and editors.strip():
                bibliography_data["editors"] = [e.strip() for e in editors.split(",") if e.strip()]
            # Strip schema-metadata keys from the actual bib fields; keep everything else
            bib = {
                k: v for k, v in bibliography_data.items()
                if v not in (None, "", []) and k not in ("source_pages", "confidence")
            }
            if bib:
                bib["_source_pages"] = bibliography_data.get("source_pages", [])
                bib["_confidence"] = float(bibliography_data.get("confidence", 0.0))
                bib["_model"] = actual_model
                bib["_prompt_version"] = prompt_version
                meta_out["bibliography"] = bib
            else:
                meta_out.pop("bibliography", None)
        else:
            return _failed_with_response(output_dir, raw_text, "LLM output field 'bibliography' must be an object or null")

        concepts_local_data = parsed.get("concepts_local")
        if not isinstance(concepts_local_data, list):
            return _failed_with_response(output_dir, raw_text, "LLM output field 'concepts_local' must be a list")

        concepts_bridge_data = parsed.get("concepts_bridge_candidates", [])
        if not isinstance(concepts_bridge_data, list):
            return _failed_with_response(output_dir, raw_text, "LLM output field 'concepts_bridge_candidates' must be a list")

        concepts: list[ConceptCandidate] = []
        if not is_project_domain:
            concepts.extend(_make_concepts(concepts_local_data, actual_model, prompt_version, concept_type="local"))
            concepts.extend(_make_concepts(concepts_bridge_data, actual_model, prompt_version, concept_type="bridge_candidate"))
        enriched_count["concepts"] = len(concepts)

        # TOC: promote if LLM extracted one and toc.json is currently empty
        toc_out: list | None = None
        toc_path = output_dir / "toc.json"
        existing_toc = json.loads(toc_path.read_text(encoding="utf-8")) if toc_path.exists() else []
        toc_data = parsed.get("toc")
        if not isinstance(toc_data, list):
            return _failed_with_response(output_dir, raw_text, "LLM output field 'toc' must be a list")
        if not existing_toc:
            if toc_data:
                toc_out = toc_data
                enriched_count["toc"] = len(toc_out)

        required_fields = ("summary", "document_type", "keywords")
        missing = [field for field in required_fields if field not in meta_out or not isinstance(meta_out[field], dict)]
        if missing:
            return _failed_with_response(
                output_dir,
                raw_text,
                f"Document metadata missing required fields after patch apply: {', '.join(missing)}",
            )
        for field in required_fields:
            if "value" not in meta_out[field]:
                return _failed_with_response(
                    output_dir,
                    raw_text,
                    f"Document metadata field '{field}' missing 'value' after patch apply",
                )

    except Exception as exc:
        return _failed_with_response(output_dir, raw_text, f"Mapping error: {exc}")

    # 7. Atomic write: stage all files first, then commit all renames
    try:
        # Update stamp with actual responding model (not the config fallback list)
        stamp["model"] = actual_model
        meta_out["_enrichment_stamp"] = stamp

        meta_path = output_dir / "meta.json"
        concepts_path = output_dir / "concepts.jsonl"

        # Stage: write to temp files
        tmp_meta = meta_path.with_suffix(".json.tmp")
        tmp_concepts = concepts_path.with_suffix(".jsonl.tmp")

        tmp_meta.write_text(
            json.dumps(meta_out, separators=(',', ':'), ensure_ascii=False), encoding="utf-8"
        )
        with open(tmp_concepts, "w", encoding="utf-8") as f:
            for concept in concepts:
                f.write(json.dumps(concept.to_dict(), ensure_ascii=False) + "\n")

        toc_path = output_dir / "toc.json"
        tmp_toc = toc_path.with_suffix(".json.tmp") if toc_out is not None else None
        if tmp_toc is not None:
            tmp_toc.write_text(
                json.dumps(toc_out, separators=(',', ':'), ensure_ascii=False), encoding="utf-8"
            )

        # Backup originals for rollback
        bak_meta = meta_path.with_suffix(".json.bak")
        bak_concepts = concepts_path.with_suffix(".jsonl.bak")
        bak_toc = toc_path.with_suffix(".json.bak") if tmp_toc is not None else None
        if meta_path.exists():
            meta_path.replace(bak_meta)
        if concepts_path.exists():
            concepts_path.replace(bak_concepts)
        if bak_toc is not None and toc_path.exists():
            toc_path.replace(bak_toc)

        # Commit: rename all temps to final
        committed: list[tuple[Path, Path]] = []  # (final, backup)
        try:
            tmp_meta.replace(meta_path)
            committed.append((meta_path, bak_meta))
            tmp_concepts.replace(concepts_path)
            committed.append((concepts_path, bak_concepts))
            if tmp_toc is not None:
                tmp_toc.replace(toc_path)
                committed.append((toc_path, bak_toc))
        except Exception:
            # Rollback: restore backups for any committed files
            for final_path, backup_path in committed:
                try:
                    if backup_path and backup_path.exists():
                        backup_path.replace(final_path)
                except Exception:
                    pass
            # Clean up temps
            for tmp in (tmp_meta, tmp_concepts, tmp_toc):
                if tmp is not None:
                    try:
                        tmp.unlink(missing_ok=True)
                    except Exception:
                        pass
            raise

        # Clean up backups on success
        for bak in (bak_meta, bak_concepts, bak_toc):
            if bak is not None:
                try:
                    bak.unlink(missing_ok=True)
                except Exception:
                    pass
    except Exception as exc:
        return {"status": "failed", "detail": f"Write error: {exc}"}

    # 8. Build detail string
    parts = []
    if "summary" in meta_out:
        parts.append("summary")
    if enriched_count["keywords"]:
        parts.append(f"{enriched_count['keywords']} keywords")
    for field_name, label in (
        ("methodological_conclusions", "methodological conclusions"),
        ("main_content_learnings", "main content learnings"),
    ):
        if enriched_count.get(field_name):
            parts.append(f"{enriched_count[field_name]} {label}")
    if enriched_count.get("project_extraction"):
        parts.append("project extraction")
    if enriched_count["facets"]:
        parts.append(f"{enriched_count['facets']} facets")
    if enriched_count["concepts"]:
        parts.append(f"{enriched_count['concepts']} concepts")
    if enriched_count.get("toc"):
        parts.append(f"toc ({enriched_count['toc']} entries)")
    detail = ", ".join(parts) if parts else "no fields enriched"

    return {"status": "enriched", "detail": detail}
