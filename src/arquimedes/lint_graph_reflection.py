from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from arquimedes.llm import EnrichmentError
from arquimedes.llm import parse_json_or_repair


def _deps():
    import arquimedes.lint as deps

    return deps


def _graph_reflection_due(
    root: Path,
    config: dict,
    clusters: list[dict],
    manifest_records: list[dict],
    concept_refs: list[dict],
    collection_refs: list[dict],
    deterministic_report: dict,
) -> tuple[bool, str]:
    deps = _deps()
    stage_due, stage_reason = deps._bridge_cluster_stage_due(
        root,
        "graph_reflection_at",
        root / deps.LINT_DIR / "graph_findings.jsonl",
        "graph reflection",
    )
    if not stage_due:
        return False, stage_reason

    stamp = deps._read_stamp(root / deps.GRAPH_REFLECTION_STAMP_PATH)
    findings_path = root / deps.LINT_DIR / "graph_findings.jsonl"
    payload = _graph_reflection_packet(
        deterministic_report,
        clusters,
        concept_refs,
        collection_refs,
        manifest_records,
    )
    current_fp = deps.canonical_hash(payload)
    if not findings_path.exists():
        return True, stage_reason
    if not stamp:
        return True, "graph reflection stamp missing"
    if stamp.get("graph_fingerprint") == current_fp:
        return False, "graph reflection unchanged"

    schedule_cfg = deps._lint_schedule_config(config).get("graph_schedule", {})
    min_hours = float(schedule_cfg.get("min_interval_hours", deps.DEFAULT_GRAPH_REFLECTION_INTERVAL_HOURS) or deps.DEFAULT_GRAPH_REFLECTION_INTERVAL_HOURS)
    min_cluster_delta = int(schedule_cfg.get("min_cluster_delta", deps.DEFAULT_GRAPH_REFLECTION_MIN_CLUSTER_DELTA) or deps.DEFAULT_GRAPH_REFLECTION_MIN_CLUSTER_DELTA)
    min_material_delta = int(schedule_cfg.get("min_material_delta", deps.DEFAULT_GRAPH_REFLECTION_MIN_MATERIAL_DELTA) or deps.DEFAULT_GRAPH_REFLECTION_MIN_MATERIAL_DELTA)

    checked_at = deps._parse_iso_datetime(stamp.get("checked_at"))
    if checked_at is None:
        return True, "graph reflection stamp invalid"
    age_hours = (datetime.now(timezone.utc) - checked_at).total_seconds() / 3600.0
    cluster_delta = abs(len(clusters) - int(stamp.get("cluster_count", 0) or 0))
    material_delta = abs(len(manifest_records) - int(stamp.get("material_count", 0) or 0))

    if cluster_delta >= min_cluster_delta or material_delta >= min_material_delta:
        return True, "enough graph change accumulated"
    if age_hours >= min_hours:
        return True, "graph reflection periodic interval reached"
    return False, "graph reflection deferred by schedule"


def _graph_reflection_stage_dir(root: Path) -> Path:
    return root / "derived" / "tmp" / "graph_reflection"


def _graph_reflection_page_path(root: Path) -> Path:
    return root / "wiki" / "shared" / "maintenance" / "graph-health.md"


def _graph_reflection_packet_path(root: Path) -> Path:
    return _graph_reflection_stage_dir(root) / "graph_health.packet.json"


def _graph_reflection_existing_path(root: Path) -> Path:
    return _graph_reflection_stage_dir(root) / "graph_health.current.jsonl"


def _graph_reflection_packet(
    deterministic_report: dict,
    bridge_clusters: list[dict],
    concept_refs: list[dict],
    collection_refs: list[dict],
    manifest_records: list[dict],
) -> dict:
    deps = _deps()
    material_ids = {
        str(record.get("material_id", "")).strip()
        for record in manifest_records
        if str(record.get("material_id", "")).strip()
    }
    bridge_clusters = [c for c in bridge_clusters if isinstance(c, dict)]
    multi_material_clusters = [
        c for c in bridge_clusters
        if len(dict.fromkeys(str(mid).strip() for mid in deps._safe_list(c.get("material_ids", [])) if str(mid).strip())) > 1
    ]
    multi_collection_bridges = [
        c for c in bridge_clusters
        if len(dict.fromkeys(str(key).strip() for key in deps._safe_list(c.get("domain_collection_keys", [])) if str(key).strip())) > 1
    ]
    bridge_ids = {
        str(cluster.get("cluster_id", "")).strip()
        for cluster in bridge_clusters
        if str(cluster.get("cluster_id", "")).strip()
    }
    bridge_reflections = [
        row for row in bridge_clusters
        if isinstance(row, dict)
        and str(row.get("cluster_id", "")).strip() in bridge_ids
        and (
            deps._safe_list(row.get("bridge_takeaways", []))
            or deps._safe_list(row.get("bridge_tensions", []))
            or deps._safe_list(row.get("bridge_open_questions", []))
            or deps._safe_list(row.get("helpful_new_sources", []))
            or str(row.get("why_this_bridge_matters", "")).strip()
        )
    ]

    def _compact_bridge_thread(row: dict) -> dict | None:
        bridge_id = str(row.get("cluster_id", "")).strip()
        canonical_name = str(row.get("canonical_name", "")).strip()
        takeaways = deps._safe_list(row.get("bridge_takeaways", []))[:2]
        tensions = deps._safe_list(row.get("bridge_tensions", []))[:2]
        questions = deps._safe_list(row.get("bridge_open_questions", []))[:3]
        collection_keys = [
            str(key).strip()
            for key in deps._safe_list(row.get("domain_collection_keys", []))
            if str(key).strip()
        ][:4]
        matter = str(row.get("why_this_bridge_matters", "")).strip()
        helpful_new_sources = deps._safe_list(row.get("helpful_new_sources", []))[:3]
        if not (bridge_id or canonical_name or takeaways or tensions or questions or collection_keys or helpful_new_sources or matter):
            return None
        return {
            "bridge_id": bridge_id,
            "canonical_name": canonical_name,
            "domain_collection_keys": collection_keys,
            "bridge_takeaways": takeaways,
            "bridge_tensions": tensions,
            "bridge_open_questions": questions,
            "helpful_new_sources": helpful_new_sources,
            "why_this_bridge_matters": matter,
        }

    def _compact_collection_reflection(row: dict) -> dict | None:
        collection_key = str(row.get("collection_key", "")).strip()
        takeaways = deps._safe_list(row.get("main_takeaways", []))[:2]
        tensions = deps._safe_list(row.get("main_tensions", []))[:2]
        questions = deps._safe_list(row.get("open_questions", []))[:3]
        matter = str(row.get("why_this_collection_matters", "")).strip()
        if not (collection_key or takeaways or tensions or questions or matter):
            return None
        return {
            "collection_key": collection_key,
            "main_takeaways": takeaways,
            "main_tensions": tensions,
            "open_questions": questions,
            "why_this_collection_matters": matter,
        }

    return {
        "kind": "graph_maintenance",
        "summary": deterministic_report.get("summary", {}),
        "graph_state": {
            "materials": len(material_ids),
            "global_bridges": len(bridge_clusters),
            "multi_material_bridges": len(multi_material_clusters),
            "multi_collection_bridges": len(multi_collection_bridges),
            "bridge_reflections": len(bridge_reflections),
            "collection_reflections": len(collection_refs),
        },
        "bridge_threads": [item for row in bridge_clusters for item in [_compact_bridge_thread(row)] if item][:10],
        "collection_threads": [item for row in collection_refs if isinstance(row, dict) for item in [_compact_collection_reflection(row)] if item][:10],
    }


def _graph_reflection_prompt(
    packet_path: Path,
    current_path: Path,
) -> tuple[str, str]:
    deps = _deps()
    system = (
        "You are an architecture research librarian writing structured graph-maintenance findings for SQL-backed storage.\n"
        "\n"
        "This is not a wiki page. It is a semantic maintenance record for the graph: what still feels unresolved, "
        "what bridge areas are too thin or too broad, what collection syntheses still need work, and what should be investigated next.\n"
        "\n"
        "Use the compact graph-state packet for the high-signal inputs. Use the current graph-findings file as the "
        "current stored state when it exists. Preserve useful prior material when it still fits, but revise stale "
        "items instead of copying them forward blindly.\n"
        "\n"
        "Deterministic lint already handles mechanical hygiene such as broken links, orphans, and stale page counts. "
        "Do not repeat that work here. Keep the findings concise, judgment-heavy, and SQL-friendly.\n"
        "\n"
        "Return exactly one final JSON object matching this schema: "
        f"{deps._GRAPH_REFLECTION_DELTA_SCHEMA}\n"
        "Do all reasoning silently first. Do not return markdown fences, commentary, or partial JSON."
    )
    user = (
        f"Read these files:\n"
        f"- Graph-state packet: {packet_path}\n"
        f"- Current graph findings file: {current_path}\n"
        "\n"
        "The packet is ultra-compact graph state: summary counts, global bridge threads, and collection threads.\n"
        "The current graph findings file is the current stored state and may be empty on the first run.\n"
        "Write a prioritized maintenance record, not a raw list of everything. Focus on the few unresolved "
        "semantic problems that matter most: weak bridge areas, collection synthesis gaps, "
        "and the next questions or sources that would move the graph forward.\n"
        "Return only the fields requested by the schema: findings and _finished.\n"
        "If the current findings list still fits the evidence exactly, you may return null for findings and the pipeline will preserve the stored list unchanged.\n"
        "Do not restate deterministic lint results.\n"
        f"Return exactly one final JSON object matching this schema: {deps._GRAPH_REFLECTION_DELTA_SCHEMA}\n"
        "Do not respond until the work is complete. Return one response only, directly as JSON, with _finished set to true. "
        "Do not return markdown fences, commentary, drafts, progress updates, or partial JSON.\n"
    )
    return system, user


def _compile_graph_reflection_response(
    parsed: Any,
    existing_rows: list[dict],
    fingerprint: str,
) -> list[dict]:
    deps = _deps()
    if not isinstance(parsed, dict):
        raise EnrichmentError("Graph maintenance output must be a JSON object")
    if parsed.get("_finished") is not True:
        raise EnrichmentError("Graph maintenance output missing _finished=true")

    findings = parsed.get("findings")
    if findings is None:
        source_rows = [dict(row) for row in existing_rows if isinstance(row, dict)]
    elif isinstance(findings, list):
        source_rows = findings
    else:
        raise EnrichmentError("Graph maintenance output field 'findings' must be a list or null")

    normalized = []
    for idx, finding in enumerate(source_rows):
        if not isinstance(finding, dict):
            continue
        for field in ("finding_type", "severity", "summary", "details"):
            value = finding.get(field, "")
            if not isinstance(value, str):
                raise EnrichmentError(f"Graph maintenance finding field '{field}' must be a string")
        for field in (
            "affected_material_ids",
            "affected_cluster_ids",
            "candidate_future_sources",
            "candidate_bridge_links",
        ):
            value = finding.get(field, [])
            if not isinstance(value, list):
                raise EnrichmentError(f"Graph maintenance finding field '{field}' must be a list")
        finding_id = str(finding.get("finding_id", "")).strip()
        if not finding_id:
            finding_id = f"graph:{idx}"
        normalized.append({
            "finding_id": finding_id,
            "finding_type": str(finding.get("finding_type", "")).strip(),
            "severity": str(finding.get("severity", "")).strip(),
            "summary": str(finding.get("summary", "")).strip(),
            "details": str(finding.get("details", "")).strip(),
            "affected_material_ids": deps._dedupe_strings(deps._safe_list(finding.get("affected_material_ids", []))),
            "affected_cluster_ids": deps._dedupe_strings(deps._safe_list(finding.get("affected_cluster_ids", []))),
            "candidate_future_sources": deps._dedupe_strings(deps._safe_list(finding.get("candidate_future_sources", []))),
            "candidate_bridge_links": deps._dedupe_strings(deps._safe_list(finding.get("candidate_bridge_links", []))),
            "input_fingerprint": fingerprint,
        })
    return normalized


def _run_graph_reflection_impl(
    deps: Any,
    root: Path,
    deterministic_report: dict,
    concept_refs: list[dict],
    collection_refs: list[dict],
    bridge_clusters: list[dict],
    manifest_records: list[dict],
    llm_factory=None,
    tool=None,
    route_signature: str = "",
) -> dict:
    packet_path = deps._graph_reflection_packet_path(root)
    current_path = deps._graph_reflection_existing_path(root)
    payload = deps._graph_reflection_packet(
        deterministic_report,
        bridge_clusters,
        concept_refs,
        collection_refs,
        manifest_records,
    )
    fingerprint = deps.canonical_hash(payload)
    findings_path = root / deps.LINT_DIR / "graph_findings.jsonl"
    existing_rows = deps._load_jsonl(findings_path)
    if findings_path.exists() and fingerprint and deps._read_stamp(root / deps.GRAPH_REFLECTION_STAMP_PATH).get("graph_fingerprint") == fingerprint:
        return {
            "graph_maintenance": 0,
            "graph_skipped": True,
            "graph_skip_reason": "graph maintenance unchanged",
        }

    deps._stage_work_copy(findings_path, current_path)
    deps._write_json(packet_path, payload)

    llm_fn = llm_factory("lint")
    system, user = deps._graph_reflection_prompt(packet_path, current_path)
    succeeded = False
    try:
        raw = llm_fn(system, [{"role": "user", "content": user}])
        parsed = parse_json_or_repair(llm_fn, raw, deps._GRAPH_REFLECTION_DELTA_SCHEMA)
        normalized = deps._compile_graph_reflection_response(parsed, existing_rows, fingerprint)
        deps._attach_run_provenance(normalized, route_signature, datetime.now(timezone.utc).isoformat())
        deps._write_jsonl(findings_path, normalized)
        succeeded = True
    finally:
        if succeeded:
            deps._cleanup_paths(packet_path, current_path)

    deps._write_stamp(
        root / deps.GRAPH_REFLECTION_STAMP_PATH,
        {
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "graph_fingerprint": fingerprint,
            "cluster_count": len({cluster.get("cluster_id", "") for cluster in bridge_clusters if cluster.get("cluster_id", "")}),
            "material_count": len({mid for cluster in bridge_clusters for mid in deps._safe_list(cluster.get("material_ids", [])) if mid}),
            "bridge_cluster_count": len([cluster for cluster in bridge_clusters if len(dict.fromkeys(deps._safe_list(cluster.get("material_ids", [])))) > 1]),
                "bridge_reflection_count": len(
                    [
                        cluster
                        for cluster in bridge_clusters
                        if any(
                            [
                                deps._safe_list(cluster.get("bridge_takeaways", [])),
                                deps._safe_list(cluster.get("bridge_tensions", [])),
                                deps._safe_list(cluster.get("bridge_open_questions", [])),
                                deps._safe_list(cluster.get("helpful_new_sources", [])),
                                str(cluster.get("why_this_bridge_matters", "")).strip(),
                            ]
                        )
                    ]
                ),
            "collection_reflection_count": len(collection_refs),
            "finding_count": len(normalized),
        },
    )
    deps._write_lint_stage_stamp(root, graph_reflection_at=datetime.now(timezone.utc).isoformat())
    return {
        "graph_maintenance": len(normalized),
        "graph_skipped": False,
        "graph_skip_reason": "",
    }
