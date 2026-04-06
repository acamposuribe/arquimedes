"""Document enrichment stage — Phase 3.

Enriches document-level metadata: summary, document_type, keywords, methodological conclusions,
main content learnings, facets, and concepts. All enriched fields carry provenance
(model, prompt_version, confidence, source_pages, evidence_spans).
"""

from __future__ import annotations

import json
from pathlib import Path

from arquimedes import enrich_llm, enrich_prompts, enrich_stamps
from arquimedes.enrich_llm import get_model_id
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


# ---------------------------------------------------------------------------
# Field-level helpers
# ---------------------------------------------------------------------------


def _make_enriched_field(llm_field: dict, model: str, prompt_version: str, *, confidence: float = 1.0) -> EnrichedField:
    """Build an EnrichedField from the LLM response dict for a single field."""
    provenance = Provenance.create(
        model=model,
        prompt_version=prompt_version,
        confidence=confidence,
        source_pages=[],
        evidence_spans=[],
    )
    return EnrichedField(value=llm_field["value"], provenance=provenance)


def _make_facets(facets_data: dict, model: str, prompt_version: str) -> ArchitectureFacets:
    """Build an ArchitectureFacets object from the LLM facets dict."""
    facet_fields = [
        "building_type", "scale", "location", "jurisdiction", "climate",
        "program", "material_system", "structural_system", "historical_period",
        "course_topic", "studio_project",
    ]
    kwargs = {}
    for name in facet_fields:
        if name in facets_data and isinstance(facets_data[name], dict) and "value" in facets_data[name]:
            kwargs[name] = _make_enriched_field(facets_data[name], model, prompt_version, confidence=0.7)
    return ArchitectureFacets(**kwargs)


_RELEVANCE_CONFIDENCE = {"high": 1.0, "medium": 0.7, "low": 0.4}


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

    # 1. Compute fingerprint and stamp
    try:
        fingerprint = enrich_stamps.document_fingerprint(output_dir)
    except Exception as exc:
        return {"status": "failed", "detail": f"Fingerprint error: {exc}"}

    stamp = enrich_stamps.make_stamp(prompt_version, model, schema_version, fingerprint)

    # 2. Staleness check
    existing_stamp = enrich_stamps.read_document_stamp(output_dir)
    if not force and not enrich_stamps.is_stale(existing_stamp, stamp):
        return {"status": "skipped", "detail": "up to date"}

    # 3. Load artifacts
    try:
        meta = _load_json(output_dir / "meta.json", default={})
        toc = _load_json(output_dir / "toc.json", default=None)
        chunks = _load_jsonl(output_dir / "chunks.jsonl")
        annotations = _load_jsonl(output_dir / "annotations.jsonl")
    except Exception as exc:
        return {"status": "failed", "detail": f"Load error: {exc}"}

    # 4. Build prompt and call LLM
    work_meta_path = output_dir / "meta.work.json"
    work_chunks_path = output_dir / "chunks.work.txt"
    try:
        # Pre-step: create work files with enrichment scaffold
        enrich_prompts.build_document_work_files(output_dir, meta, chunks, annotations)

        # LLM reads and edits work files in place — no content in prompt
        system, messages = enrich_prompts.build_document_file_prompt(
            work_meta_path, work_chunks_path
        )
        llm_fn(system, messages)

        # Post-step: read back the edited work file
        work_meta = _load_json(work_meta_path)
        if work_meta is None:
            return {"status": "failed", "detail": "LLM did not produce a valid work file"}
        parsed = work_meta
    except enrich_llm.EnrichmentError as exc:
        return {"status": "failed", "detail": str(exc)}
    except Exception as exc:
        return {"status": "failed", "detail": f"LLM error: {exc}"}
    finally:
        # Clean up work files regardless of outcome
        for work_path in (work_meta_path, work_chunks_path):
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

    for field in ("summary", "document_type", "keywords",
                  "methodological_conclusions", "main_content_learnings"):
        _normalize_field(parsed, field)
    # Normalize each facet value
    facets = parsed.get("facets") or {}
    if isinstance(facets, dict):
        for fkey in list(facets):
            _normalize_field(facets, fkey)

    # Validate required fields are present
    _REQUIRED_FIELDS = ("summary", "document_type", "keywords")
    missing = [f for f in _REQUIRED_FIELDS if f not in parsed or not isinstance(parsed[f], dict)]
    if missing:
        return {"status": "failed", "detail": f"LLM output missing required fields: {', '.join(missing)}"}

    for req_field in _REQUIRED_FIELDS:
        if "value" not in parsed[req_field]:
            return {"status": "failed", "detail": f"LLM output field '{req_field}' missing 'value'"}

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

        for field_name in ("methodological_conclusions", "main_content_learnings"):
            field_data = parsed.get(field_name)
            if isinstance(field_data, dict) and "value" in field_data:
                meta_out[field_name] = _make_enriched_field(field_data, actual_model, prompt_version).to_dict()
                enriched_count[field_name] = len(meta_out[field_name]["value"]) if isinstance(meta_out[field_name].get("value"), list) else 1

        facets_data = parsed.get("facets", {})
        if facets_data and isinstance(facets_data, dict):
            facets = _make_facets(facets_data, actual_model, prompt_version)
            meta_out["facets"] = facets.to_dict()
            enriched_count["facets"] = sum(1 for v in facets.to_dict().values() if v)

        # bibliography: stored as-is (plain dict of optional strings) plus provenance metadata
        bibliography_data = parsed.get("bibliography")
        if bibliography_data and isinstance(bibliography_data, dict):
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

        concepts_local_data = parsed.get("concepts_local")
        if not isinstance(concepts_local_data, list):
            concepts_local_data = parsed.get("concepts", []) if isinstance(parsed.get("concepts"), list) else []

        concepts_bridge_data = parsed.get("concepts_bridge_candidates", [])
        if not isinstance(concepts_bridge_data, list):
            concepts_bridge_data = []

        concepts: list[ConceptCandidate] = []
        concepts.extend(_make_concepts(concepts_local_data, actual_model, prompt_version, concept_type="local"))
        concepts.extend(_make_concepts(concepts_bridge_data, actual_model, prompt_version, concept_type="bridge_candidate"))
        enriched_count["concepts"] = len(concepts)

        # TOC: promote if LLM extracted one and toc.json is currently empty
        toc_out: list | None = None
        toc_path = output_dir / "toc.json"
        existing_toc = json.loads(toc_path.read_text(encoding="utf-8")) if toc_path.exists() else []
        if not existing_toc:
            toc_data = parsed.get("toc")
            if isinstance(toc_data, list) and toc_data:
                toc_out = toc_data
                enriched_count["toc"] = len(toc_out)

    except Exception as exc:
        return {"status": "failed", "detail": f"Mapping error: {exc}"}

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
    if enriched_count["facets"]:
        parts.append(f"{enriched_count['facets']} facets")
    if enriched_count["concepts"]:
        parts.append(f"{enriched_count['concepts']} concepts")
    if enriched_count.get("toc"):
        parts.append(f"toc ({enriched_count['toc']} entries)")
    detail = ", ".join(parts) if parts else "no fields enriched"

    return {"status": "enriched", "detail": detail}
