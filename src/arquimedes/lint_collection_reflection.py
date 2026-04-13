from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from arquimedes.config import load_config
from arquimedes.llm import EnrichmentError
from arquimedes.llm import parse_json_or_repair


def _deps():
    import arquimedes.lint as deps

    return deps


def _collection_reflection_due(root: Path) -> tuple[bool, str]:
    deps = _deps()
    return deps._bridge_cluster_stage_due(
        root,
        "collection_reflection_at",
        root / deps.LINT_DIR / "collection_reflections.jsonl",
        "collection reflection",
    )


def _collection_reflection_key(domain: str, collection: str) -> str:
    domain = (domain or "practice").strip() or "practice"
    collection = (collection or "_general").strip() or "_general"
    return f"{domain}/{collection}"


def _collection_reflection_stage_dir(root: Path) -> Path:
    return root / "derived" / "tmp" / "collection_reflections"


def _collection_reflection_page_copy_path(root: Path, domain: str, collection: str) -> Path:
    key = _collection_reflection_key(domain, collection).replace("/", "__")
    return _collection_reflection_stage_dir(root) / f"{key}.page.md"


def _collection_reflection_evidence_path(root: Path, domain: str, collection: str) -> Path:
    key = _collection_reflection_key(domain, collection).replace("/", "__")
    return _collection_reflection_stage_dir(root) / f"{key}.evidence.json"


def _format_collection_material_concept(row: dict) -> str:
    deps = _deps()
    concept_name = str(row.get("concept_name", "")).strip()
    descriptor = str(row.get("descriptor", "")).strip()
    spans = [span for span in deps._safe_list(row.get("evidence_spans", [])) if span]
    if descriptor:
        return f"{concept_name} ({descriptor})".strip()
    if spans:
        return f"{concept_name} ({', '.join(spans[:2])})".strip()
    return concept_name


def _collection_reflection_local_clusters(
    domain: str,
    collection: str,
    metas: list[dict],
    clusters: list[dict],
    tool=None,
) -> list[dict]:
    deps = _deps()
    material_ids = {str(meta.get("material_id", "")).strip() for meta in metas if str(meta.get("material_id", "")).strip()}
    target_scope = deps._collection_scope(domain, collection)
    overlapping = []
    for cluster in clusters:
        if deps._cluster_scope(cluster) != target_scope:
            continue
        overlap = material_ids & {str(mid).strip() for mid in deps._safe_list(cluster.get("material_ids", []))}
        if not overlap:
            continue
        reflection = tool.open_record("concept", cluster.get("cluster_id", "")) if tool else None
        reflection_data = reflection.get("reflection", {}) if isinstance(reflection, dict) else {}
        overlapping.append({
            "cluster_id": cluster.get("cluster_id", ""),
            "concept": cluster.get("canonical_name", ""),
            "material_count": len(overlap),
            "main_takeaways": deps._parse_json_list(reflection_data.get("main_takeaways", ""))[:3],
            "main_tensions": deps._parse_json_list(reflection_data.get("main_tensions", ""))[:2],
            "open_questions": deps._parse_json_list(reflection_data.get("open_questions", ""))[:2],
            "why_this_concept_matters": str(reflection_data.get("why_this_concept_matters", "")).strip(),
        })
    overlapping.sort(key=lambda item: (-item["material_count"], item["concept"].lower(), item["cluster_id"]))
    return overlapping


def _collection_reflection_materials(
    root: Path,
    domain: str,
    collection: str,
    metas: list[dict],
    clusters: list[dict],
    existing_record: dict | None = None,
    tool=None,
) -> dict[str, list[dict]]:
    deps = _deps()
    if not metas:
        return {"new_materials": [], "old_materials": []}

    cluster_lookup: dict[str, list[dict]] = defaultdict(list)
    cluster_by_material: dict[str, list[str]] = defaultdict(list)
    for cluster in clusters:
        cluster_id = str(cluster.get("cluster_id", "")).strip()
        if not cluster_id:
            continue
        for mid in deps._safe_list(cluster.get("material_ids", [])):
            mid = str(mid).strip()
            if mid:
                cluster_by_material[mid].append(cluster_id)
                cluster_lookup[mid].append(cluster)

    scored: list[tuple[float, str, dict, list[dict], list[str]]] = []
    for meta in metas:
        mid = str(meta.get("material_id", "")).strip()
        if not mid:
            continue
        overlap_clusters = cluster_lookup.get(mid, [])
        overlap_ids = cluster_by_material.get(mid, [])
        score = float(len(overlap_clusters))
        title = str(meta.get("title", mid)).strip().lower()
        scored.append((score, title, meta, overlap_clusters, overlap_ids))

    scored.sort(key=lambda item: (-item[0], item[1], item[2].get("material_id", "")))
    selected = scored[:deps.COLLECTION_REFLECTION_MAX_MATERIALS]

    friendly_title = f"{domain.replace('_', ' ').title()} / {collection.replace('_', ' ').title()}"
    previous_ids = {
        str(mid).strip()
        for mid in deps._safe_list((existing_record or {}).get("important_material_ids", []))
        if str(mid).strip()
    }
    new_materials: list[dict] = []
    old_materials: list[dict] = []
    for _score, _title, meta, overlap_clusters, _overlap_ids in selected:
        mid = str(meta.get("material_id", "")).strip()
        title = str(meta.get("title", mid)).strip()
        summary = deps._meta_val(meta.get("summary"))
        methodological_conclusions = deps._meta_list_value(meta, "methodological_conclusions")
        main_content_learnings = deps._meta_list_value(meta, "main_content_learnings")

        query_terms = deps._dedupe_strings([
            friendly_title,
            _collection_reflection_key(domain, collection),
            title,
            summary,
            *[cluster.get("canonical_name", "") for cluster in overlap_clusters if cluster.get("canonical_name", "")],
        ])

        is_new = mid not in previous_ids
        material_entry: dict[str, Any] = {
            "material_id": mid,
            "title": title,
            "methodological_conclusions": methodological_conclusions,
            "main_content_learnings": main_content_learnings,
        }

        if is_new:
            chunk_limit = deps.COLLECTION_REFLECTION_MAX_CHUNKS_PER_MATERIAL
            chunk_evidence = deps._collect_material_chunk_evidence(tool, mid, query_terms, chunk_limit)
            evidence = (
                tool._material_evidence(
                    mid,
                    query_terms,
                    chunk_limit=chunk_limit,
                    annotation_limit=deps.COLLECTION_REFLECTION_MAX_ANNOTATIONS_PER_MATERIAL,
                    figure_limit=deps.COLLECTION_REFLECTION_MAX_FIGURES_PER_MATERIAL,
                    concept_limit=deps.COLLECTION_REFLECTION_MAX_CONCEPTS_PER_MATERIAL,
                )
                if tool
                else {}
            )
            evidence_payload: dict[str, Any] = {
                "chunks": [
                    {
                        "text": str(chunk.get("text", "")).strip(),
                        "source": str(chunk.get("source", "")).strip() or "fallback",
                    }
                    for chunk in chunk_evidence
                    if isinstance(chunk, dict) and str(chunk.get("text", "")).strip()
                ],
                "figures": [
                    {
                        "figure_id": str(fig.get("figure_id", "")).strip(),
                        "description": str(fig.get("description", "")).strip(),
                    }
                    for fig in list(evidence.get("figures", []))
                    if isinstance(fig, dict)
                ],
                "concepts": [
                    _format_collection_material_concept(con)
                    for con in list(evidence.get("concepts", []))
                    if isinstance(con, dict) and str(con.get("concept_name", "")).strip()
                ],
            }
            annotations = [
                line
                for line in (
                    deps._format_material_annotation(ann)
                    for ann in list(evidence.get("annotations", []))
                    if isinstance(ann, dict)
                )
                if line
            ]
            if annotations:
                evidence_payload["annotations"] = "\n".join(annotations)
            material_entry["evidence"] = evidence_payload
            new_materials.append(material_entry)
        else:
            old_materials.append({
                "material_id": material_entry["material_id"],
                "title": material_entry["title"],
                "methodological_conclusions": material_entry["methodological_conclusions"],
                "main_content_learnings": material_entry["main_content_learnings"],
            })

    return {"new_materials": new_materials, "old_materials": old_materials}


def _build_collection_reflection_evidence_payload(
    root: Path,
    domain: str,
    collection: str,
    metas: list[dict],
    clusters: list[dict],
    existing_record: dict | None = None,
    tool=None,
) -> dict:
    materials = _collection_reflection_materials(root, domain, collection, metas, clusters, existing_record, tool)
    return {
        "kind": "collection_reflection",
        "collection_key": _collection_reflection_key(domain, collection),
        "domain": domain,
        "collection": collection,
        "title": f"{domain.replace('_', ' ').title()} / {collection.replace('_', ' ').title()}",
        "local_clusters": _collection_reflection_local_clusters(domain, collection, metas, clusters, tool),
        "new_materials": materials["new_materials"],
        "old_materials": materials["old_materials"],
    }


def _collection_reflection_scaffold(
    domain: str,
    collection: str,
    fingerprint: str,
    root: Path,
) -> dict:
    deps = _deps()
    return {
        "collection_key": _collection_reflection_key(domain, collection),
        "domain": domain,
        "collection": collection,
        "main_takeaways": [],
        "main_tensions": [],
        "important_material_ids": [],
        "important_cluster_ids": [],
        "open_questions": [],
        "helpful_new_sources": [],
        "why_this_collection_matters": "",
        "input_fingerprint": fingerprint,
        "wiki_path": str(deps._collection_page_path(domain, collection)),
    }


def _compile_collection_reflection_response(
    domain: str,
    collection: str,
    scaffold: dict,
    fingerprint: str,
    parsed: Any,
    existing_record: dict | None = None,
) -> dict:
    deps = _deps()
    if not isinstance(parsed, dict):
        raise EnrichmentError("Collection reflection output must be a JSON object")
    if parsed.get("_finished") is not True:
        raise EnrichmentError("Collection reflection output missing _finished=true")

    def _resolve_list_field(field: str) -> list[str]:
        value = parsed.get(field)
        if value is None:
            return deps._dedupe_strings(deps._safe_list((existing_record or {}).get(field, scaffold.get(field, []))))
        if not isinstance(value, list):
            raise EnrichmentError(f"Collection reflection output field '{field}' must be a list or null")
        return deps._dedupe_strings(deps._safe_list(value))

    def _resolve_id_field(field: str) -> list[str]:
        return sorted({item.strip() for item in _resolve_list_field(field) if item.strip()})

    def _resolve_text_field(field: str) -> str:
        value = parsed.get(field)
        if value is None:
            return str((existing_record or {}).get(field, scaffold.get(field, "")) or "").strip()
        if not isinstance(value, str):
            raise EnrichmentError(f"Collection reflection output field '{field}' must be a string or null")
        return value.strip()

    for field in ("main_takeaways", "main_tensions", "important_material_ids", "important_cluster_ids", "open_questions", "helpful_new_sources"):
        if field.startswith("important_"):
            _resolve_id_field(field)
        else:
            _resolve_list_field(field)
    _resolve_text_field("why_this_collection_matters")

    return {
        **scaffold,
        "collection_key": _collection_reflection_key(domain, collection),
        "domain": domain,
        "collection": collection,
        "main_takeaways": _resolve_list_field("main_takeaways"),
        "main_tensions": _resolve_list_field("main_tensions"),
        "important_material_ids": _resolve_id_field("important_material_ids"),
        "important_cluster_ids": _resolve_id_field("important_cluster_ids"),
        "open_questions": _resolve_list_field("open_questions"),
        "helpful_new_sources": _resolve_list_field("helpful_new_sources"),
        "why_this_collection_matters": _resolve_text_field("why_this_collection_matters"),
        "input_fingerprint": fingerprint,
        "wiki_path": str(scaffold.get("wiki_path", "")).strip(),
    }


def _collection_reflection_fingerprint(domain: str, collection: str, metas: list[dict], clusters: list[dict]) -> str:
    deps = _deps()
    material_ids = sorted({
        str(meta.get("material_id", "")).strip()
        for meta in metas
        if str(meta.get("material_id", "")).strip()
    })
    target_scope = deps._collection_scope(domain, collection)
    local_clusters = sorted(
        [
            {
                "cluster_id": str(cluster.get("cluster_id", "")).strip(),
                "canonical_name": str(cluster.get("canonical_name", "")).strip(),
                "material_ids": sorted(deps._safe_list(cluster.get("material_ids", []))),
            }
            for cluster in clusters
            if deps._cluster_scope(cluster) == target_scope
        ],
        key=lambda row: row["cluster_id"],
    )
    return deps.canonical_hash({
        "collection_key": _collection_reflection_key(domain, collection),
        "material_ids": material_ids,
        "local_clusters": local_clusters,
    })


def _collection_reflection_prompt(
    domain: str,
    collection: str,
    page_path: Path,
    evidence_path: Path,
) -> tuple[str, str]:
    deps = _deps()
    system = (
        "You are an architecture research librarian writing reflective synthesis for a collection page.\n"
        "\n"
        "Your job is not to restate the collection page. Your job is to explain what the collection is doing as a whole: "
        "the central through-line, the important materials and local clusters, the tensions it holds, and what remains unresolved.\n"
        "\n"
        "Use the collection wiki page as the current public state of the collection. Use the staged SQL-evidence file for "
        "the supporting materials, methodological conclusions, main content learnings, chunks, annotations, figures, and compact local-concept "
        "summaries that ground the synthesis. Preserve prior "
        "conclusions when they still hold, but revise them when the evidence changes.\n"
        "\n"
        "The material-level methodological conclusions and main content learnings are the primary reusable evidence for each material. "
        "The chunks are only secondary support. Chunks with source=search are the strongest matches to the collection evidence queries. "
        "Chunks with source=fallback are only fill-in evidence for the same material and may be less directly relevant. "
        "Prefer the search-sourced chunks when forming the synthesis, and keep the chunk selection compact.\n"
        "The local_clusters entries are short synthesized cues from the concept reflections, not raw membership ids.\n"
        "Treat the new_materials as the main evidence for this run and the old_materials as compact background continuity.\n"
        "\n"
        "For each reflection, be specific, cumulative, and didactic. Prefer teachable claims over generic summaries. "
        "Write the reflection as a synthesis, not as a list of facts. Preserve the strongest distinctions, material-level evidence, and collection-level stakes, not just the conclusion.\n"
        "If this is the first run, still write a strong first synthesis instead of waiting for prior history. You are writing a wiki article, so be didactic. \n"
        "IMPORTANT: Avoid academic jargon, theoretical buzzwords, or pretentious language. Use clear, direct, and specific language that conveys real analytical meaning. Make conclusions easy to read for audiences that are new to the theme, but keep them nuanced and grounded. Remember, this is for a wiki-style page.\n"
        "\n"
        "Return exactly one final JSON object matching this schema: "
        f"{deps._COLLECTION_REFLECTION_DELTA_SCHEMA}\n"
        "Do all reasoning silently first. Do not return markdown fences, commentary, or partial JSON."
    )
    user = (
        f"Read these files:\n"
        f"- Collection wiki page: {page_path}\n"
        f"- SQL evidence file: {evidence_path}\n"
        "\n"
        "Return only the reflection fields requested by the schema: main_takeaways, main_tensions, important_material_ids, important_cluster_ids, open_questions, helpful_new_sources, why_this_collection_matters, and _finished.\n"
        "Write a strong reflection that includes main takeaways, main tensions, important materials, important local clusters, open questions, helpful new sources, and why this collection matters.\n"
        "Field guidance:\n"
        "- why_this_collection_matters: write one concise didactic paragraph that states the collection's thesis or through-line, why it matters in this corpus, and at least one concrete distinction, example, or stake that keeps the paragraph from becoming generic. Don't start with 'this collection matters becaus...'. Maintain scholarly rigor.\n"
        "- main_takeaways: usually keep 3-5 distinct takeaways when the evidence supports them. Keep the strongest claims, but preserve the key distinction, mechanism, or evidence that makes each one teachable. What can we learn from this collection as architectural researchers?\n"
        "- main_tensions: usually keep 3-5 distinct tensions when the evidence supports them. Identify real unresolved tensions or ambiguities and name the exact terms of the conflict clearly. Do not collapse several tensions into one broad umbrella sentence.\n"
        "- important_material_ids: select the materials that are genuinely load-bearing for the collection's current argument, not just all new_materials.\n"
        "- important_cluster_ids: select the local clusters that best organize the collection's current through-line, not just the most repeated names. Keep these focused and meaningful.\n"
        "- open_questions: usually keep 3-5 non-overlapping questions when the evidence supports them. Ask what the current corpus still cannot explain, not generic future-research prompts and not takeaways rewritten as questions. If the collection already surfaces genuine unresolved tensions, scope limits, or bridge gaps, open_questions should almost never be empty.\n"
        "- helpful_new_sources: usually keep 3-5 targeted, non-duplicative source suggestions when the evidence supports them. Suggest source types, cases, archives, or comparisons that would most directly help resolve the listed gaps. Avoid multiple phrasings of the same source idea.\n"
        "If one reflection field should remain exactly as the current reflection already states it, you may return null for that field and the pipeline will preserve the stored value for that key.\n"
        "Do not return collection metadata, fingerprints, or wiki paths.\n"
        "If prior reflection text still fits the evidence, preserve it; if it no longer fits, revise it.\n"
        "Do not leave the work file as a mere summary of the page. Use the evidence to surface the collection's role, stakes, and unresolved questions.\n"
        "Return final JSON only.\n"
    )
    return system, user


def _run_collection_reflections_impl(
    deps: Any,
    root: Path,
    groups: dict[tuple[str, str], list[dict]],
    clusters: list[dict],
    llm_factory=None,
    tool=None,
    route_signature: str = "",
) -> list[dict]:
    existing = deps._existing_by_key(root / deps.LINT_DIR / "collection_reflections.jsonl", "collection_key")
    output: list[dict] = []
    eligible = [(domain, collection, metas) for (domain, collection), metas in groups.items() if len(metas) >= 2]
    workers = max(1, min(len(eligible), int(load_config().get("enrichment", {}).get("parallel", 4) or 4)))

    def _one(domain: str, collection: str, metas: list[dict]) -> dict | None:
        key = deps._collection_reflection_key(domain, collection)
        page_path = root / deps._collection_page_path(domain, collection)
        fingerprint = deps._collection_reflection_fingerprint(domain, collection, metas, clusters)
        existing_record = existing.get(key)
        if existing_record and existing_record.get("input_fingerprint") == fingerprint:
            return existing_record
        page_copy_path = deps._collection_reflection_page_copy_path(root, domain, collection)
        evidence_path = deps._collection_reflection_evidence_path(root, domain, collection)
        deps._stage_reflection_page_copy(page_path, page_copy_path)
        evidence_path.parent.mkdir(parents=True, exist_ok=True)
        evidence_payload = deps._build_collection_reflection_evidence_payload(root, domain, collection, metas, clusters, existing_record, tool)
        deps._write_json(evidence_path, evidence_payload)
        scaffold = deps._collection_reflection_scaffold(domain, collection, fingerprint, root)
        llm_fn = llm_factory("cluster")
        system, user = deps._collection_reflection_prompt(domain, collection, page_copy_path, evidence_path)
        succeeded = False
        try:
            if tool is not None:
                parsed = deps._run_reflection_prompt_with_context(
                    llm_fn,
                    system,
                    user,
                    deps._COLLECTION_REFLECTION_DELTA_SCHEMA,
                    tool,
                )
            else:
                raw = llm_fn(system, [{"role": "user", "content": user}])
                parsed = parse_json_or_repair(llm_fn, raw, deps._COLLECTION_REFLECTION_DELTA_SCHEMA)
            record = deps._compile_collection_reflection_response(
                domain,
                collection,
                scaffold,
                fingerprint,
                parsed,
                existing_record,
            )
            succeeded = True
            return record
        finally:
            if succeeded:
                deps._cleanup_paths(evidence_path, page_copy_path)

    if len(eligible) > 1 and workers > 1:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(_one, domain, collection, metas) for domain, collection, metas in eligible]
            for fut in as_completed(futures):
                record = fut.result()
                if record:
                    output.append(record)
    else:
        for domain, collection, metas in eligible:
            record = _one(domain, collection, metas)
            if record:
                output.append(record)

    output.sort(key=lambda r: (r.get("domain", ""), r.get("collection", "")))
    run_at = datetime.now(timezone.utc).isoformat()
    deps._attach_run_provenance(output, route_signature, run_at)
    deps._write_jsonl(root / deps.LINT_DIR / "collection_reflections.jsonl", output)
    deps._write_lint_stage_stamp(root, collection_reflection_at=run_at)
    return output