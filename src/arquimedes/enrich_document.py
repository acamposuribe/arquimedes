"""Document enrichment stage — Phase 3.

Enriches document-level metadata: summary, document_type, keywords, facets, concepts.
All enriched fields carry provenance (model, prompt_version, confidence, source_pages, evidence_spans).
"""

from __future__ import annotations

import json
from pathlib import Path

from arquimedes import enrich_llm, enrich_prompts, enrich_stamps
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


def _make_enriched_field(llm_field: dict, model: str, prompt_version: str) -> EnrichedField:
    """Build an EnrichedField from the LLM response dict for a single field."""
    provenance = Provenance.create(
        model=model,
        prompt_version=prompt_version,
        confidence=float(llm_field.get("confidence", 0.0)),
        source_pages=llm_field.get("source_pages", []),
        evidence_spans=llm_field.get("evidence_spans", []),
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
            kwargs[name] = _make_enriched_field(facets_data[name], model, prompt_version)
    return ArchitectureFacets(**kwargs)


def _make_concept(concept_data: dict, model: str, prompt_version: str) -> ConceptCandidate:
    """Build a ConceptCandidate from the LLM concepts list entry."""
    provenance = Provenance.create(
        model=model,
        prompt_version=prompt_version,
        confidence=concept_data.get("confidence", 1.0),
        source_pages=concept_data.get("source_pages", []),
        evidence_spans=concept_data.get("evidence_spans", []),
    )
    return ConceptCandidate(
        concept_name=concept_data.get("concept_name", ""),
        relevance=concept_data.get("relevance", ""),
        provenance=provenance,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

_DOCUMENT_SCHEMA_DESC = """\
{
  "summary": {"value": "...", "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0},
  "document_type": {"value": "regulation|catalogue|monograph|paper|lecture_note|precedent|technical_spec|site_document", "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0},
  "keywords": {"value": ["term1", ...], "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0},
  "facets": {<facet_name>: {"value": "...", "source_pages": [...], "evidence_spans": ["..."], "confidence": 0.0-1.0}},
  "concepts": [{"concept_name": "...", "relevance": "...", "source_pages": [...], "evidence_spans": ["..."]}]
}"""


def enrich_document_stage(
    output_dir: Path,
    config: dict,
    client,
    *,
    force: bool = False,
) -> dict:
    """Enrich document-level metadata for a single material.

    Args:
        output_dir: Path to extracted/<material_id>/ directory.
        config: Full config dict (enrichment section read from config["enrichment"]).
        client: anthropic.Anthropic client (or mock).
        force: Re-enrich even if not stale.

    Returns:
        {"status": "enriched"|"skipped"|"failed", "detail": str}
    """
    enrichment_config = config.get("enrichment", {})
    model: str = config.get("llm", {}).get("model", "claude-sonnet-4-6")
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
    try:
        system, messages = enrich_prompts.build_document_prompt(meta, toc, chunks, annotations)
        raw_text = enrich_llm.call_llm(client, model, system, messages)
        parsed = enrich_llm.parse_json_or_repair(client, model, raw_text, _DOCUMENT_SCHEMA_DESC)
    except enrich_llm.EnrichmentError as exc:
        return {"status": "failed", "detail": str(exc)}
    except Exception as exc:
        return {"status": "failed", "detail": f"LLM error: {exc}"}

    # 5. Map parsed JSON to model objects and merge into meta dict
    try:
        meta_out = dict(meta)

        enriched_count = {"keywords": 0, "facets": 0, "concepts": 0}

        if "summary" in parsed and isinstance(parsed["summary"], dict):
            ef = _make_enriched_field(parsed["summary"], model, prompt_version)
            meta_out["summary"] = ef.to_dict()

        if "document_type" in parsed and isinstance(parsed["document_type"], dict):
            ef = _make_enriched_field(parsed["document_type"], model, prompt_version)
            meta_out["document_type"] = ef.to_dict()

        if "keywords" in parsed and isinstance(parsed["keywords"], dict):
            ef = _make_enriched_field(parsed["keywords"], model, prompt_version)
            meta_out["keywords"] = ef.to_dict()
            enriched_count["keywords"] = len(ef.value) if isinstance(ef.value, list) else 1

        facets_data = parsed.get("facets", {})
        if facets_data and isinstance(facets_data, dict):
            facets = _make_facets(facets_data, model, prompt_version)
            meta_out["facets"] = facets.to_dict()
            enriched_count["facets"] = sum(1 for v in facets.to_dict().values() if v)

        concepts_data = parsed.get("concepts", [])
        if not isinstance(concepts_data, list):
            concepts_data = []

        concepts: list[ConceptCandidate] = [
            _make_concept(c, model, prompt_version)
            for c in concepts_data
            if isinstance(c, dict) and c.get("concept_name")
        ]
        enriched_count["concepts"] = len(concepts)

    except Exception as exc:
        return {"status": "failed", "detail": f"Mapping error: {exc}"}

    # 6. Write meta.json (with enriched fields merged)
    try:
        meta_path = output_dir / "meta.json"
        meta_path.write_text(json.dumps(meta_out, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception as exc:
        return {"status": "failed", "detail": f"Write meta error: {exc}"}

    # 7. Write concepts.jsonl
    try:
        concepts_path = output_dir / "concepts.jsonl"
        with open(concepts_path, "w", encoding="utf-8") as f:
            for concept in concepts:
                f.write(json.dumps(concept.to_dict(), ensure_ascii=False) + "\n")
    except Exception as exc:
        return {"status": "failed", "detail": f"Write concepts error: {exc}"}

    # 8. Write stamp
    try:
        enrich_stamps.write_document_stamp(output_dir, stamp)
    except Exception as exc:
        return {"status": "failed", "detail": f"Write stamp error: {exc}"}

    # 9. Build detail string
    parts = []
    if "summary" in meta_out:
        parts.append("summary")
    if enriched_count["keywords"]:
        parts.append(f"{enriched_count['keywords']} keywords")
    if enriched_count["facets"]:
        parts.append(f"{enriched_count['facets']} facets")
    if enriched_count["concepts"]:
        parts.append(f"{enriched_count['concepts']} concepts")
    detail = ", ".join(parts) if parts else "no fields enriched"

    return {"status": "enriched", "detail": detail}
