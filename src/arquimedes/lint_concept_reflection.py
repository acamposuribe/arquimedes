from __future__ import annotations

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from arquimedes import practice_prompts
from arquimedes.llm import EnrichmentError
from arquimedes.config import load_config
from arquimedes.domain_profiles import is_practice_domain
from arquimedes.llm import parse_json_or_repair


def _deps():
    import arquimedes.lint as deps

    return deps


def _concept_reflection_due(root: Path) -> tuple[bool, str]:
    deps = _deps()
    artifact_path = root / deps.LINT_DIR / "concept_reflections.jsonl"
    stage_label = "concept reflection"
    current_clustered_at = deps._current_clustered_at(root)
    if not current_clustered_at:
        return False, "concept clustering has not run yet"
    if not artifact_path.exists():
        return True, f"{stage_label} artifact missing"

    stage_at = str(deps._lint_stage_stamp(root).get("concept_reflection_at", "") or "")
    if not stage_at:
        return True, f"{stage_label} stamp missing"

    latest_driver_dt = deps._parse_iso_datetime(current_clustered_at)
    stage_at_dt = deps._parse_iso_datetime(stage_at)
    if latest_driver_dt is None or stage_at_dt is None:
        return True, f"{stage_label} stamp invalid"
    if stage_at_dt >= latest_driver_dt:
        return False, f"{stage_label} already ran after latest concept-graph change"
    return True, f"latest concept-graph change is newer than {stage_label}"


def _concept_reflection_stage_dir(root: Path) -> Path:
    return root / "derived" / "tmp" / "concept_reflections"


def _concept_reflection_page_copy_path(root: Path, cluster_id: str) -> Path:
    safe_cluster_id = (cluster_id or "cluster").strip() or "cluster"
    return _concept_reflection_stage_dir(root) / f"{safe_cluster_id}.page.md"


def _concept_reflection_evidence_path(root: Path, cluster_id: str) -> Path:
    safe_cluster_id = (cluster_id or "cluster").strip() or "cluster"
    return _concept_reflection_stage_dir(root) / f"{safe_cluster_id}.evidence.json"


def _collect_material_chunk_evidence(
    tool,
    material_id: str,
    query_terms: list[str],
    chunk_limit: int,
) -> list[dict]:
    if not tool or chunk_limit <= 0:
        return []
    selected = []
    seen: set[str] = set()
    search_terms = [term for term in (str(t).strip() for t in query_terms) if term]
    for query in search_terms:
        try:
            rows = tool.search_material_evidence("chunk", material_id, query, limit=chunk_limit)
        except Exception:
            rows = []
        for row in rows:
            chunk_id = str(row.get("chunk_id", "")).strip()
            if not chunk_id or chunk_id in seen:
                continue
            item = dict(row)
            item.setdefault("text", item.get("snippet", ""))
            selected.append({
                "chunk_id": chunk_id,
                "text": item.get("text", ""),
                "source": "search",
            })
            seen.add(chunk_id)
            if len(selected) >= chunk_limit:
                return selected
    if selected and len(selected) >= chunk_limit:
        return selected[:chunk_limit]
    fallback = tool._material_evidence(material_id, query_terms, chunk_limit=chunk_limit)
    for row in list(fallback.get("chunks", [])):
        if not isinstance(row, dict):
            continue
        chunk_id = str(row.get("chunk_id", "")).strip()
        if not chunk_id or chunk_id in seen:
            continue
        selected.append({
            "chunk_id": str(row.get("chunk_id", "")).strip(),
            "text": str(row.get("text") or row.get("excerpt") or "").strip(),
            "source": "fallback",
        })
        seen.add(chunk_id)
        if len(selected) >= chunk_limit:
            break
    return selected[:chunk_limit]


def _format_material_annotation(row: dict) -> str:
    parts = []
    page = str(row.get("page", "")).strip()
    quoted_text = str(row.get("quoted_text", "")).strip()
    comment = str(row.get("comment", "")).strip()
    if page:
        parts.append(f"p. {page}")
    if quoted_text:
        parts.append(quoted_text)
    if comment:
        parts.append(comment)
    return " — ".join(parts).strip()


def _format_material_concept(row: dict) -> str:
    deps = _deps()
    concept_name = str(row.get("concept_name", "")).strip()
    spans = [span for span in deps._safe_list(row.get("evidence_spans", [])) if span]
    if spans:
        return f"{concept_name} ({', '.join(spans)})".strip()
    return concept_name


def _build_concept_reflection_evidence_payload(
    cluster: dict,
    material_info: dict[str, dict],
    tool=None,
) -> dict:
    deps = _deps()
    source_concepts = cluster.get("source_concepts", [])
    unique_material_ids = []
    seen: set[str] = set()
    candidate_material_ids = [
        str(material_id).strip()
        for material_id in deps._safe_list(cluster.get("supporting_material_ids", []))
        if str(material_id).strip()
    ]
    if candidate_material_ids:
        for mid in candidate_material_ids:
            if mid not in seen:
                seen.add(mid)
                unique_material_ids.append(mid)
    else:
        for sc in sorted(
            source_concepts,
            key=lambda x: (
                x.get("material_id", ""),
                -float(x.get("confidence", 0.0) or 0.0),
            ),
        ):
            mid = sc.get("material_id", "")
            if mid and mid not in seen:
                seen.add(mid)
                unique_material_ids.append(mid)

    query_terms = [
        cluster.get("canonical_name", ""),
        *(deps._safe_list(cluster.get("aliases", []))),
        *[sc.get("concept_name", "") for sc in source_concepts if sc.get("concept_name")],
    ]

    material_weights: dict[str, float] = defaultdict(float)
    for sc in source_concepts:
        mid = sc.get("material_id", "")
        if not mid:
            continue
        confidence = float(sc.get("confidence", 0.0) or 0.0)
        relevance = str(sc.get("relevance", "")).strip().lower()
        material_weights[mid] += confidence
        material_weights[mid] += {"high": 0.5, "medium": 0.25, "low": 0.1}.get(relevance, 0.0)
        material_weights[mid] += min(len(deps._safe_list(sc.get("evidence_spans", []))), 4) * 0.02
    ordered_material_ids = sorted(unique_material_ids, key=lambda mid: (material_weights.get(mid, 0.0), mid), reverse=True)
    chunk_limits: dict[str, int] = {}
    if ordered_material_ids:
        base = deps.CONCEPT_REFLECTION_TOTAL_CHUNK_BUDGET // len(ordered_material_ids)
        remainder = deps.CONCEPT_REFLECTION_TOTAL_CHUNK_BUDGET % len(ordered_material_ids)
        if base > 0:
            for idx, mid in enumerate(ordered_material_ids):
                chunk_limits[mid] = min(
                    deps.CONCEPT_REFLECTION_MAX_CHUNKS_PER_MATERIAL,
                    base + (1 if idx < remainder else 0),
                )
        else:
            for idx, mid in enumerate(ordered_material_ids):
                chunk_limits[mid] = 1 if idx < deps.CONCEPT_REFLECTION_TOTAL_CHUNK_BUDGET else 0

    materials = []
    for mid in ordered_material_ids:
        info = material_info.get(mid, {})
        chunk_limit = chunk_limits.get(mid, 0)
        chunk_evidence = _collect_material_chunk_evidence(tool, mid, query_terms, chunk_limit)
        evidence = (
            tool._material_evidence(
                mid,
                query_terms,
                chunk_limit=chunk_limit,
                annotation_limit=deps.CONCEPT_REFLECTION_MAX_ANNOTATIONS_PER_MATERIAL,
                figure_limit=deps.concept_reflection_figure_limit(str(cluster.get("domain", ""))),
                concept_limit=deps.CONCEPT_REFLECTION_MAX_CONCEPTS_PER_MATERIAL,
            )
            if tool
            else {}
        )
        if isinstance(evidence, dict):
            annotations = [
                line
                for line in (
                    _format_material_annotation(ann)
                    for ann in list(evidence.get("annotations", []))
                    if isinstance(ann, dict)
                )
                if line
            ]
            evidence_payload = {
                "chunks": [
                    {
                        "chunk_id": str(chunk.get("chunk_id", "")).strip(),
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
                    _format_material_concept(con)
                    for con in list(evidence.get("concepts", []))
                    if isinstance(con, dict) and str(con.get("concept_name", "")).strip()
                ],
            }
            if annotations:
                evidence_payload["annotations"] = "\n".join(annotations)
            evidence = evidence_payload
        materials.append({
            "material_id": mid,
            "title": info.get("title", mid),
            "summary": info.get("summary", ""),
            "keywords": info.get("keywords", []),
            "evidence": evidence,
        })

    return {
        "cluster_id": cluster.get("cluster_id", ""),
        "canonical_name": cluster.get("canonical_name", ""),
        "slug": cluster.get("slug", ""),
        "aliases": cluster.get("aliases", []),
        "materials": materials,
    }


def _concept_reflection_scaffold(
    cluster: dict,
    fingerprint: str,
    root: Path,
) -> dict:
    deps = _deps()
    source_concepts = cluster.get("source_concepts", [])
    supporting_material_ids = sorted({
        str(material_id).strip()
        for material_id in (
            deps._safe_list(cluster.get("supporting_material_ids", []))
            or [sc.get("material_id", "") for sc in source_concepts]
        )
        if str(material_id).strip()
    })
    supporting_evidence = sorted({
        span
        for sc in source_concepts
        for span in deps._safe_list(sc.get("evidence_spans", []))
    })
    return {
        "cluster_id": cluster.get("cluster_id", ""),
        "slug": cluster.get("slug", ""),
        "canonical_name": cluster.get("canonical_name", ""),
        "main_takeaways": [],
        "main_tensions": [],
        "open_questions": [],
        "helpful_new_sources": [],
        "why_this_concept_matters": "",
        "supporting_material_ids": supporting_material_ids,
        "supporting_evidence": supporting_evidence,
        "input_fingerprint": fingerprint,
        "wiki_path": str(deps._concept_page_path(cluster)),
    }


def _compile_concept_reflection_response(
    cluster: dict,
    scaffold: dict,
    fingerprint: str,
    parsed: Any,
    existing_record: dict | None = None,
) -> dict:
    deps = _deps()
    if not isinstance(parsed, dict):
        raise EnrichmentError("Concept reflection output must be a JSON object")
    if parsed.get("_finished") is not True:
        raise EnrichmentError("Concept reflection output missing _finished=true")

    def _resolve_list_field(field: str) -> list[str]:
        value = parsed.get(field)
        if value is None:
            return deps._dedupe_strings(deps._safe_list((existing_record or {}).get(field, scaffold.get(field, []))))
        if not isinstance(value, list):
            raise EnrichmentError(f"Concept reflection output field '{field}' must be a list or null")
        return deps._dedupe_strings(deps._safe_list(value))

    def _resolve_text_field(field: str) -> str:
        value = parsed.get(field)
        if value is None:
            return str((existing_record or {}).get(field, scaffold.get(field, "")) or "").strip()
        if not isinstance(value, str):
            raise EnrichmentError(f"Concept reflection output field '{field}' must be a string or null")
        return value.strip()

    for field in ("main_takeaways", "main_tensions", "open_questions", "helpful_new_sources"):
        _resolve_list_field(field)
    _resolve_text_field("why_this_concept_matters")
    return {
        **scaffold,
        "cluster_id": cluster.get("cluster_id", ""),
        "slug": cluster.get("slug", ""),
        "canonical_name": cluster.get("canonical_name", ""),
        "main_takeaways": _resolve_list_field("main_takeaways"),
        "main_tensions": _resolve_list_field("main_tensions"),
        "open_questions": _resolve_list_field("open_questions"),
        "helpful_new_sources": _resolve_list_field("helpful_new_sources"),
        "why_this_concept_matters": _resolve_text_field("why_this_concept_matters"),
        "input_fingerprint": fingerprint,
        "wiki_path": str(scaffold.get("wiki_path", "")).strip(),
    }


def _concept_reflection_link_fingerprint(cluster: dict) -> str:
    deps = _deps()
    linked: dict[str, Any] = {
        "source_concepts": [],
        "supporting_material_ids": sorted(
            {
                str(material_id).strip()
                for material_id in deps._safe_list(cluster.get("supporting_material_ids", []))
                if str(material_id).strip()
            }
        ),
    }
    seen: set[tuple[str, str]] = set()
    for sc in cluster.get("source_concepts", []):
        if not isinstance(sc, dict):
            continue
        mid = str(sc.get("material_id", "")).strip()
        cname = str(sc.get("concept_name", "")).strip()
        if not mid or not cname:
            continue
        key = (mid, cname)
        if key in seen:
            continue
        seen.add(key)
        linked["source_concepts"].append({"material_id": mid, "concept_name": cname})
    linked["source_concepts"].sort(key=lambda item: (item["material_id"], item["concept_name"]))
    return deps.canonical_hash(linked)


def _concept_reflection_prompt(
    domain: str,
    page_path: Path,
    evidence_path: Path,
) -> tuple[str, str]:
    deps = _deps()
    if is_practice_domain(domain):
        return practice_prompts.concept_reflection_prompt(
            deps._CONCEPT_REFLECTION_DELTA_SCHEMA,
            page_path,
            evidence_path,
        )
    system = (
        "You are an architecture research librarian writing reflective synthesis for a concept page.\n"
        "\n"
        "Your job is not to restate the page. Your job is to explain the concept's role in the corpus: "
        "the central claim it makes, why it matters here, what tensions it holds, and what remains unresolved.\n"
        "\n"
        "Use the wiki page as the current public state of the concept. Use the staged SQL-evidence file for "
        "the supporting materials, chunks, annotations, and figures that ground the synthesis. Preserve prior "
        "conclusions when they still hold, but revise them when the evidence changes.\n"
        "\n"
        "Write for a reader who may be new to the theme. The page should teach quickly without flattening the idea. "
        "Keep the prose concise, but do not drop the distinctions, examples, or internal structure that make the concept understandable.\n"
        "\n"
        "For each reflection, be specific, cumulative, and didactic. Prefer teachable claims over generic summaries. "
        "Write the reflection as a synthesis, not as a list of facts. Preserve the strongest conceptual distinctions and evidence, "
        "not just the conclusion.\n"
        "If this is the first run, still write a strong first synthesis instead of waiting for prior history.\n"
        "IMPORTANT: Avoid academic jargon, theoretical buzzwords, or pretentious language. Use clear, direct, and specific language that conveys real analytical meaning. Make conclusions easy to read for audiences that are new to the theme, but keep them nuanced and grounded. Remember, this is for a wiki-style page.\n"
        "\n"
        "Return exactly one final JSON object matching this schema: "
        f"{deps._CONCEPT_REFLECTION_DELTA_SCHEMA}\n"
        "Do all reasoning silently first. Do not return markdown fences, commentary, or partial JSON."
    )
    user = (
        f"Read these files:\n"
        f"- Concept wiki page: {page_path}\n"
        f"- SQL evidence file: {evidence_path}\n"
        "\n"
        "The concept wiki page is the current public page and may already contain a previous reflection.\n"
        "The SQL evidence file contains the staged evidence for this concept cluster. The chunks inside that file are ordered by usefulness: source=search is the strongest match to the concept query, while source=fallback is only secondary support for the same material.\n"
        "The annotations field, when present, is a single newline-delimited string of annotation notes. The concepts field, when present, is a compact list of concept names only.\n"
        "Return only the reflection fields requested by the schema: main_takeaways, main_tensions, open_questions, helpful_new_sources, why_this_concept_matters, and _finished.\n"
        "Field guidance:\n"
        "- why_this_concept_matters: write one concise didactic paragraph that states the concept's thesis, why it matters in this corpus, and at least one concrete distinction, example, or stake that keeps the paragraph from becoming generic.\n"
        "- main_takeaways: usually keep 3-5 distinct takeaways when the evidence supports them. Keep the strongest claims, but preserve the key distinction, mechanism, or evidence that makes each one teachable. What can we learn from this concept as architectural researchers?\n"
        "- main_tensions: usually keep 3-5 distinct tensions when the evidence supports them. Identify real unresolved tensions or ambiguities and name the exact terms of the conflict clearly. Do not collapse several tensions into one broad umbrella sentence. \n"
        "- open_questions: usually keep 3-5 non-overlapping questions when the evidence supports them. Ask what the current corpus still cannot explain, not generic future-research prompts and not takeaways rewritten as questions. If the concept already surfaces genuine unresolved tensions, scope limits, or compatibility problems, open_questions should almost never be empty.\n"
        "- helpful_new_sources: usually keep 3-5 targeted, non-duplicative source suggestions when the evidence supports them. Suggest source types, cases, archives, or comparisons that would most directly help resolve the listed gaps. Avoid multiple phrasings of the same source idea.\n"
        "If one reflection field should remain exactly as the current reflection already states it, you may return null for that field and the pipeline will preserve the stored value for that key.\n"
        "Do not return cluster metadata, supporting ids, or wiki paths.\n"
        "If prior reflection text still fits the evidence, preserve it; if it no longer fits, revise it.\n"
        "Do not leave the work file as a mere summary of the page. Use the evidence to surface the concept's role, stakes, and unresolved questions.\n"
        f"Return exactly one final JSON object matching this schema: {deps._CONCEPT_REFLECTION_DELTA_SCHEMA}\n"
        "Do not respond until the work is complete. Return one response only, directly as JSON, with _finished set to true. "
        "Do not return markdown fences, commentary, drafts, progress updates, or partial JSON.\n"
    )
    return system, user


def _run_concept_reflections_impl(
    deps: Any,
    root: Path,
    clusters: list[dict],
    material_info: dict[str, dict],
    llm_factory=None,
    tool=None,
    route_signature: str = "",
) -> list[dict]:
    existing = deps._existing_by_key(root / deps.LINT_DIR / "concept_reflections.jsonl", "cluster_id")
    output: list[dict] = []
    eligible = [
        c
        for c in clusters
        if len(
            {
                str(material_id).strip()
                for material_id in (
                    deps._safe_list(c.get("supporting_material_ids", []))
                    or [sc.get("material_id", "") for sc in c.get("source_concepts", [])]
                )
                if str(material_id).strip()
            }
        ) >= 2
    ]
    workers = max(1, min(len(eligible), int(load_config().get("enrichment", {}).get("parallel", 4) or 4)))

    def _one(cluster: dict) -> dict | None:
        page_path = root / deps._concept_page_path(cluster)
        evidence_payload = deps._build_concept_reflection_evidence_payload(cluster, material_info, tool)
        fingerprint = deps._concept_reflection_link_fingerprint(cluster)
        existing_record = existing.get(cluster.get("cluster_id", ""))
        if existing_record and existing_record.get("input_fingerprint") == fingerprint:
            return existing_record
        page_copy_path = deps._concept_reflection_page_copy_path(root, cluster.get("cluster_id", ""))
        evidence_path = deps._concept_reflection_evidence_path(root, cluster.get("cluster_id", ""))
        deps._stage_reflection_page_copy(page_path, page_copy_path)
        evidence_path.parent.mkdir(parents=True, exist_ok=True)
        deps._write_json(evidence_path, evidence_payload)
        scaffold = deps._concept_reflection_scaffold(cluster, fingerprint, root)
        llm_fn = llm_factory("cluster")
        system, user = deps._concept_reflection_prompt(
            str(cluster.get("domain", "")),
            page_copy_path,
            evidence_path,
        )
        succeeded = False
        try:
            if tool is not None:
                parsed = deps._run_reflection_prompt_with_context(
                    llm_fn,
                    system,
                    user,
                    deps._CONCEPT_REFLECTION_DELTA_SCHEMA,
                    tool,
                )
            else:
                raw = llm_fn(system, [{"role": "user", "content": user}])
                parsed = parse_json_or_repair(llm_fn, raw, deps._CONCEPT_REFLECTION_DELTA_SCHEMA)
            record = deps._compile_concept_reflection_response(cluster, scaffold, fingerprint, parsed, existing_record)
            succeeded = True
            return record
        finally:
            if succeeded:
                deps._cleanup_paths(evidence_path, page_copy_path)

    if len(eligible) > 1 and workers > 1:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(_one, c) for c in eligible]
            for fut in as_completed(futures):
                record = fut.result()
                if record:
                    output.append(record)
    else:
        for cluster in eligible:
            record = _one(cluster)
            if record:
                output.append(record)

    output.sort(key=lambda r: r.get("cluster_id", ""))
    run_at = datetime.now(timezone.utc).isoformat()
    deps._attach_run_provenance(output, route_signature, run_at)
    deps._write_jsonl(root / deps.LINT_DIR / "concept_reflections.jsonl", output)
    deps._write_lint_stage_stamp(root, concept_reflection_at=run_at)
    return output
