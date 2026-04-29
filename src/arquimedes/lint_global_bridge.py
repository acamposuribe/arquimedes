from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import shutil
from typing import Any

from arquimedes import practice_prompts
from arquimedes.domain_profiles import is_practice_domain
from arquimedes.llm import EnrichmentError
from arquimedes.llm import parse_json_or_repair

_GLOBAL_BRIDGE_DELTA_SCHEMA = '{"links_to_existing":[{"bridge_id":"required existing bridge id","member_local_clusters":[{"cluster_id":"required pending local cluster id"}],"canonical_name":"string(optional)","descriptor":"string(optional)","aliases":["strings(optional)"],"bridge_takeaways":["strings(optional)"],"bridge_tensions":["strings(optional)"],"bridge_open_questions":["strings(optional)"],"helpful_new_sources":["strings(optional)"],"why_this_bridge_matters":"string(optional)"}],"new_clusters":[{"canonical_name":"required string","descriptor":"short bridge description","aliases":["max 4 strings"],"member_local_clusters":[{"cluster_id":"required pending local cluster id"}],"bridge_takeaways":["strings"],"bridge_tensions":["strings"],"bridge_open_questions":["strings"],"helpful_new_sources":["strings"],"why_this_bridge_matters":"string"}],"_finished":true}'
_REQUIRED_GLOBAL_BRIDGE_FIELDS = ("links_to_existing", "new_clusters")


def _deps():
    import arquimedes.lint as deps

    return deps


def _bridge_domain(domain: str) -> str:
    deps = _deps()
    scope_domain, _ = deps._collection_scope(domain, "_general")
    return scope_domain


def _domain_bridge_dir(root: Path, domain: str) -> Path:
    return root / "derived" / "domains" / _bridge_domain(domain)


def _bridge_wiki_path(domain: str, slug: str) -> str:
    return f"wiki/{_bridge_domain(domain)}/bridge-concepts/{slug}.md"


def _global_bridge_artifact_path(root: Path, domain: str | None = None) -> Path:
    if domain is None:
        return root / "derived" / "global_bridge_clusters.jsonl"
    return _domain_bridge_dir(root, domain) / "global_bridge_clusters.jsonl"


def _global_bridge_stamp_path(root: Path, domain: str | None = None) -> Path:
    if domain is None:
        return root / "derived" / "global_bridge_stamp.json"
    return _domain_bridge_dir(root, domain) / "global_bridge_stamp.json"


def global_bridge_artifact_path(root: Path, domain: str | None = None) -> Path:
    return _global_bridge_artifact_path(root, domain)


def global_bridge_stamp_path(root: Path, domain: str | None = None) -> Path:
    return _global_bridge_stamp_path(root, domain)


def global_bridge_artifact_paths(root: Path) -> list[Path]:
    domain_paths = sorted((root / "derived" / "domains").glob("*/global_bridge_clusters.jsonl"))
    if domain_paths:
        return domain_paths
    legacy = _global_bridge_artifact_path(root)
    return [legacy] if legacy.exists() else []


def global_bridge_stamp_paths(root: Path) -> list[Path]:
    domain_paths = sorted((root / "derived" / "domains").glob("*/global_bridge_stamp.json"))
    if domain_paths:
        return domain_paths
    legacy = _global_bridge_stamp_path(root)
    return [legacy] if legacy.exists() else []


def _global_bridge_fingerprints_from_stamps(root: Path, domain: str | None = None) -> dict[str, str]:
    deps = _deps()
    if domain is not None:
        stamp = deps._read_stamp(_global_bridge_stamp_path(root, domain))
        return stamp.get("local_cluster_fingerprints", {}) if isinstance(stamp, dict) else {}
    merged: dict[str, str] = {}
    for stamp_path in global_bridge_stamp_paths(root):
        stamp = deps._read_stamp(stamp_path)
        if not isinstance(stamp, dict):
            continue
        for cluster_id, fingerprint in stamp.get("local_cluster_fingerprints", {}).items():
            cluster_key = str(cluster_id).strip()
            fingerprint_value = str(fingerprint).strip()
            if cluster_key and fingerprint_value:
                merged[cluster_key] = fingerprint_value
    return merged


def _global_bridge_stage_dir(root: Path, domain: str) -> Path:
    return root / "derived" / "tmp" / "global_bridge" / _bridge_domain(domain)


def _global_bridge_packet_path(root: Path, domain: str) -> Path:
    return _global_bridge_stage_dir(root, domain) / "global_bridge.packet.json"


def _global_bridge_memory_path(root: Path, domain: str) -> Path:
    return _global_bridge_stage_dir(root, domain) / "global_bridge.memory.json"


def _global_bridge_no_progress_path(root: Path, domain: str) -> Path:
    return _global_bridge_stage_dir(root, domain) / "global_bridge.no_progress.json"


def _collection_scope_key(domain: str, collection: str) -> str:
    deps = _deps()
    scope_domain, scope_collection = deps._collection_scope(domain, collection)
    return f"{scope_domain}__{scope_collection}"


def _safe_cluster_id(row: dict) -> str:
    return str(row.get("cluster_id") or row.get("bridge_id", "")).strip()


def _dedupe_cluster_refs(rows: list[dict]) -> list[dict]:
    seen: set[str] = set()
    output = []
    for row in rows:
        cluster_id = str(row.get("cluster_id", "")).strip()
        if not cluster_id or cluster_id in seen:
            continue
        seen.add(cluster_id)
        output.append({"cluster_id": cluster_id})
    return output


def _dict_rows(value: Any) -> list[dict]:
    if not isinstance(value, list):
        return []
    return [row for row in value if isinstance(row, dict)]


def _local_cluster_reflection_map(root: Path) -> dict[str, dict]:
    deps = _deps()
    rows = {}
    for row in deps._load_jsonl(root / deps.LINT_DIR / "concept_reflections.jsonl"):
        if not isinstance(row, dict):
            continue
        cluster_id = str(row.get("cluster_id", "")).strip()
        if not cluster_id or cluster_id.startswith("global_bridge__"):
            continue
        rows[cluster_id] = row
    return rows


def _collection_context_rows(collection_refs: list[dict]) -> list[dict]:
    deps = _deps()
    rows = []
    for row in collection_refs:
        if not isinstance(row, dict):
            continue
        collection_key = str(row.get("collection_key", "")).strip()
        domain = str(row.get("domain", "")).strip() or collection_key.partition("/")[0]
        collection = str(row.get("collection", "")).strip() or collection_key.partition("/")[2]
        if not collection_key:
            if not (domain and collection):
                continue
            collection_key = f"{domain}/{collection}"
        rows.append(
            {
                "collection_key": collection_key,
                "domain": domain,
                "collection": collection,
                "title": collection,
                "main_takeaways": deps._dedupe_strings(deps._safe_list(row.get("main_takeaways", [])))[:4],
                "main_tensions": deps._dedupe_strings(deps._safe_list(row.get("main_tensions", [])))[:4],
                "why_this_collection_matters": str(row.get("why_this_collection_matters", "")).strip(),
            }
        )
    return sorted(rows, key=lambda row: row["collection_key"])


def _reflection_snapshot(reflection: dict) -> dict:
    deps = _deps()
    return {
        "main_takeaways": deps._dedupe_strings(deps._safe_list(reflection.get("main_takeaways", []))),
        "main_tensions": deps._dedupe_strings(deps._safe_list(reflection.get("main_tensions", []))),
        "open_questions": deps._dedupe_strings(deps._safe_list(reflection.get("open_questions", []))),
        "helpful_new_sources": deps._dedupe_strings(deps._safe_list(reflection.get("helpful_new_sources", []))),
        "why_this_concept_matters": str(reflection.get("why_this_concept_matters", "")).strip(),
    }


def _local_cluster_snapshot(cluster: dict, reflection_map: dict[str, dict]) -> dict | None:
    deps = _deps()
    cluster_id = str(cluster.get("cluster_id", "")).strip()
    domain = str(cluster.get("domain", "")).strip()
    collection = str(cluster.get("collection", "")).strip()
    if not (cluster_id and domain and collection):
        return None
    reflection = reflection_map.get(cluster_id, {})
    return {
        "cluster_id": cluster_id,
        "domain": domain,
        "collection": collection,
        "collection_key": f"{domain}/{collection}",
        "canonical_name": str(cluster.get("canonical_name", "")).strip(),
        "slug": str(cluster.get("slug", "")).strip(),
        "descriptor": str(cluster.get("descriptor", "")).strip(),
        "aliases": deps._dedupe_strings(deps._safe_list(cluster.get("aliases", [])))[:8],
        "wiki_path": str(cluster.get("wiki_path", "")).strip(),
        "material_ids": [
            str(material_id).strip()
            for material_id in deps._safe_list(cluster.get("material_ids", []))
            if str(material_id).strip()
        ],
        "confidence": float(cluster.get("confidence", 0.0) or 0.0),
        "reflection": _reflection_snapshot(reflection),
    }


def _bridge_memory_snapshot(
    clusters: list[dict],
    local_cluster_index: dict[str, dict] | None = None,
    collection_context_by_key: dict[str, dict] | None = None,
    *,
    domain: str | None = None,
) -> list[dict]:
    deps = _deps()
    target_domain = _bridge_domain(domain) if domain else None
    rows = []
    for row in clusters:
        if not isinstance(row, dict):
            continue
        bridge_id = _safe_cluster_id(row)
        if not bridge_id:
            continue
        bridge_domain = _bridge_domain(str(row.get("domain", "")).strip()) if str(row.get("domain", "")).strip() else ""
        if target_domain and bridge_domain and bridge_domain != target_domain:
            continue
        member_rows = []
        collection_keys: set[str] = set()
        for member in _dict_rows(row.get("member_local_clusters", [])):
            cluster_id = str(member.get("cluster_id", "")).strip()
            if not cluster_id:
                continue
            current = (local_cluster_index or {}).get(cluster_id, member)
            collection_key = str(current.get("collection_key", "")).strip() or f"{str(current.get('domain', '')).strip()}/{str(current.get('collection', '')).strip()}"
            if collection_key:
                collection_keys.add(collection_key)
            member_row = {
                "cluster_id": cluster_id,
                "collection_key": collection_key,
                "canonical_name": str(current.get("canonical_name", "")).strip(),
                "descriptor": str(current.get("descriptor", "")).strip(),
            }
            reflection = current.get("reflection")
            if isinstance(reflection, dict):
                member_row["reflection"] = _reflection_snapshot(reflection)
            member_rows.append(member_row)
        rows.append(
            {
                "bridge_id": bridge_id,
                "domain": bridge_domain,
                "canonical_name": str(row.get("canonical_name", "")).strip(),
                "descriptor": str(row.get("descriptor", "")).strip(),
                "aliases": deps._dedupe_strings(deps._safe_list(row.get("aliases", [])))[:8],
                "member_local_clusters": member_rows,
                "bridge_takeaways": deps._dedupe_strings(deps._safe_list(row.get("bridge_takeaways", [])))[:6],
                "bridge_open_questions": deps._dedupe_strings(deps._safe_list(row.get("bridge_open_questions", [])))[:6],
                "why_this_bridge_matters": str(row.get("why_this_bridge_matters", "")).strip(),
                "supporting_collection_reflections": [
                    collection_context_by_key[key]
                    for key in sorted(collection_keys)
                    if collection_context_by_key and key in collection_context_by_key
                ],
            }
        )
    return sorted(rows, key=lambda row: (row["canonical_name"].casefold(), row["bridge_id"]))


def _global_bridge_inputs(
    root: Path,
    local_clusters: list[dict],
    collection_refs: list[dict],
    *,
    domain: str | None = None,
    previous_cluster_fingerprints: dict[str, str] | None = None,
    previous_collection_fingerprints: dict[str, str] | None = None,
) -> dict:
    deps = _deps()
    target_domain = _bridge_domain(domain) if domain else None
    reflection_map = _local_cluster_reflection_map(root)
    local_rows = [
        snapshot
        for cluster in local_clusters
        for snapshot in [_local_cluster_snapshot(cluster, reflection_map)]
        if snapshot is not None and (target_domain is None or snapshot["domain"] == target_domain)
    ]
    local_rows = sorted(local_rows, key=lambda row: (row["collection_key"], row["canonical_name"].casefold(), row["cluster_id"]))
    local_cluster_index = {row["cluster_id"]: row for row in local_rows}
    collection_context = [
        row
        for row in _collection_context_rows(collection_refs)
        if target_domain is None or _bridge_domain(str(row.get("domain", "")).strip()) == target_domain
    ]
    collection_context_by_key = {row["collection_key"]: row for row in collection_context}
    collection_scope_count = len({row["collection_key"] for row in local_rows})
    existing_bridges = load_global_bridge_clusters(root, domain=target_domain) if target_domain else load_global_bridge_clusters(root)
    existing_member_ids = {
        str(member.get("cluster_id", "")).strip()
        for bridge in existing_bridges
        for member in _dict_rows(bridge.get("member_local_clusters", []))
        if isinstance(member, dict) and str(member.get("cluster_id", "")).strip()
    }
    previous_cluster_fps = previous_cluster_fingerprints
    if previous_cluster_fps is None:
        previous_cluster_fps = _global_bridge_fingerprints_from_stamps(root, target_domain)
    current_cluster_fps = {
        row["cluster_id"]: deps.canonical_hash(row)
        for row in local_rows
    }
    current_collection_fps = {
        row["collection_key"]: deps.canonical_hash(row)
        for row in collection_context
    }
    pending_local_clusters = []
    for row in local_rows:
        cluster_id = row["cluster_id"]
        if (
            not existing_bridges
            or previous_cluster_fps.get(cluster_id) != current_cluster_fps.get(cluster_id)
            or cluster_id not in existing_member_ids
        ):
            pending_local_clusters.append(row)
    pending_collection_keys = {row["collection_key"] for row in pending_local_clusters}
    packet = {
        "kind": "global_bridge_packet",
        "pending_local_clusters": pending_local_clusters,
        "collection_context": [
            collection_context_by_key[key]
            for key in sorted(pending_collection_keys)
            if key in collection_context_by_key
        ],
    }
    memory = {
        "kind": "global_bridge_memory",
        "bridges": _bridge_memory_snapshot(
            existing_bridges,
            local_cluster_index,
            collection_context_by_key,
            domain=target_domain,
        ),
    }
    fingerprint = deps.canonical_hash({"pending_local_clusters": pending_local_clusters})
    return {
        "collection_scope_count": collection_scope_count,
        "packet": packet,
        "memory": memory,
        "all_local_clusters": local_rows,
        "existing_bridges": existing_bridges,
        "collection_context_by_key": collection_context_by_key,
        "input_fingerprint": fingerprint,
        "local_cluster_fingerprints": current_cluster_fps,
        "collection_context_fingerprints": current_collection_fps,
    }


def _global_bridge_input_snapshot(
    local_clusters: list[dict],
    collection_refs: list[dict],
    root: Path | None = None,
    *,
    domain: str | None = None,
) -> dict:
    if root is None:
        deps = _deps()
        target_domain = _bridge_domain(domain) if domain else None
        reflection_map: dict[str, dict] = {}
        local_rows = [
            snapshot
            for cluster in local_clusters
            for snapshot in [_local_cluster_snapshot(cluster, reflection_map)]
            if snapshot is not None and (target_domain is None or snapshot["domain"] == target_domain)
        ]
        collection_context = [
            row
            for row in _collection_context_rows(collection_refs)
            if target_domain is None or _bridge_domain(str(row.get("domain", "")).strip()) == target_domain
        ]
        return {
            "kind": "global_bridge_packet",
            "pending_local_clusters": sorted(local_rows, key=lambda row: (row["collection_key"], row["canonical_name"].casefold(), row["cluster_id"])),
            "collection_context": collection_context,
        }
    return _global_bridge_inputs(root, local_clusters, collection_refs, domain=domain)["packet"]


def _global_bridge_prompt(packet_path: Path, memory_path: Path, domain: str) -> tuple[str, str]:
    if is_practice_domain(domain):
        return practice_prompts.global_bridge_prompt(
            _GLOBAL_BRIDGE_DELTA_SCHEMA,
            packet_path,
            memory_path,
            domain,
        )
    system = (
        f"You are an architecture research librarian. You are grouping collection-local concept clusters into broader {domain.title()} bridge concepts.\n"
        "\n"
        "Output schema:\n"
        f"{_GLOBAL_BRIDGE_DELTA_SCHEMA}\n"
        "\n"
        "Rules:\n"
        "- Work only with the pending collection-local clusters in the packet. Do not invent members that are not present there.\n"
        "- Use the existing global bridge memory only to decide whether pending local clusters belong to an existing bridge or should create a new bridge.\n"
        f"- Every bridge in this pass must stay inside the {domain.title()} domain. Never connect Research and Practice in the same bridge.\n"
        "- Prefer bridges that connect multiple collections when the conceptual relation is real.\n"
        "- It is acceptable to create a within-collection bridge only when it synthesizes at least three local clusters into a genuinely broader learning, position, or perspective.\n"
        "- Do not rely on name similarity alone. Use descriptors, local-cluster reflections, and collection context to judge semantic fit.\n"
        "- Global bridge canonicals should be broad, analytically meaningful, and useful as shared conceptual pages across the whole knowledge system. Keep the total number of bridges limited to the most significant and widely applicable ones.\n"
        "- Each bridge must also include bridge takeaways, bridge tensions, bridge open questions, helpful new sources, and why the bridge matters.\n"
        "- Treat why_this_bridge_matters as the main prose body of the bridge page. Write it as a grounded mini-essay in 2 to 4 paragraphs, roughly 140 to 260 words, explaining the shared problem, what becomes legible only at the bridge level, and why the connector matters across collections.\n"
        "- Use the full connected local-cluster reflections and collection signals in the packet and memory. Synthesize them into bridge-level insight instead of repeating them as slogans.\n"
        "- Prefer 4 to 6 concrete bridge takeaways and 2 to 4 substantive tensions or questions when the evidence supports them.\n"
        "- Prefer refocusing an existing bridge with links_to_existing when its title, descriptor, aliases, or bridge-level synthesis can be adjusted to honestly include the pending clusters.\n"
        "- links_to_existing may update an existing bridge's name, descriptor, aliases, and bridge-level synthesis if the pending members materially change it.\n"
        "- New bridges with members from one collection must include at least 4 local clusters. New bridges spanning multiple collections must include at least 3 local clusters.\n"
        "- Complete the full clustering pass before you answer. Return structured JSON only once, at the end.\n"
    )
    user = (
        f"Read the pending global bridge packet from {packet_path}.\n"
        f"Read the existing global bridge memory from {memory_path}.\n"
        "Treat both files as source material for the current bridge-clustering pass.\n"
        "Use the connected local-cluster reflections and collection signals to write page-worthy bridge synthesis, not just short labels.\n"
        "Use links_to_existing to attach pending local clusters to existing bridges by bridge_id.\n"
        "Use new_clusters when pending local clusters should form a new global bridge instead.\n"
        "Only reference collection-local cluster ids that appear in the pending packet.\n"
        f"Return exactly one final JSON object matching this schema: {_GLOBAL_BRIDGE_DELTA_SCHEMA}\n"
        "Do not respond until the work is complete. Return one response only, directly as JSON, with _finished set to true. "
        "Do not return markdown fences, commentary, drafts, progress updates, or partial JSON.\n"
    )
    return system, user


def _normalize_bridge_member_refs(member_local_clusters: Any, pending_ids: set[str], *, label: str) -> list[dict]:
    if not isinstance(member_local_clusters, list):
        raise EnrichmentError(f"{label} field 'member_local_clusters' must be a list")
    rows = []
    for idx, item in enumerate(member_local_clusters, start=1):
        if not isinstance(item, dict):
            raise EnrichmentError(f"{label} member_local_clusters[{idx}] must be an object")
        cluster_id = str(item.get("cluster_id", "")).strip()
        if not cluster_id:
            raise EnrichmentError(f"{label} member_local_clusters[{idx}] is missing cluster_id")
        if cluster_id not in pending_ids:
            raise EnrichmentError(f"{label} member_local_clusters[{idx}] references unknown pending cluster_id '{cluster_id}'")
        rows.append({"cluster_id": cluster_id})
    rows = _dedupe_cluster_refs(rows)
    if not rows:
        raise EnrichmentError(f"{label} has no valid member_local_clusters")
    return rows


def _optional_string_list(value: Any, *, label: str) -> list[str]:
    deps = _deps()
    if value is None:
        return []
    if not isinstance(value, list):
        raise EnrichmentError(f"{label} must be a list")
    return deps._dedupe_strings([str(item).strip() for item in value if str(item).strip()])


def _normalize_global_bridge_response(parsed: Any, existing_bridges: list[dict], pending_ids: set[str]) -> dict:
    if not isinstance(parsed, dict):
        raise EnrichmentError("Global bridge output must be a JSON object")
    if parsed.get("_finished") is not True:
        raise EnrichmentError("Global bridge output missing _finished=true")
    missing = [field for field in _REQUIRED_GLOBAL_BRIDGE_FIELDS if field not in parsed]
    if missing:
        raise EnrichmentError(f"Global bridge output missing required fields: {', '.join(missing)}")
    links_to_existing = parsed.get("links_to_existing")
    new_clusters = parsed.get("new_clusters")
    if not isinstance(links_to_existing, list):
        raise EnrichmentError("Global bridge output field 'links_to_existing' must be a list")
    if not isinstance(new_clusters, list):
        raise EnrichmentError("Global bridge output field 'new_clusters' must be a list")
    existing_ids = {_safe_cluster_id(row) for row in existing_bridges if _safe_cluster_id(row)}
    normalized_links = []
    for idx, row in enumerate(links_to_existing, start=1):
        if not isinstance(row, dict):
            raise EnrichmentError(f"links_to_existing[{idx}] must be an object")
        bridge_id = str(row.get("bridge_id", "")).strip()
        if not bridge_id:
            raise EnrichmentError(f"links_to_existing[{idx}] is missing bridge_id")
        if bridge_id not in existing_ids:
            raise EnrichmentError(f"links_to_existing[{idx}] references unknown bridge_id '{bridge_id}'")
        normalized_links.append(
            {
                "bridge_id": bridge_id,
                "member_local_clusters": _normalize_bridge_member_refs(row.get("member_local_clusters"), pending_ids, label=f"links_to_existing[{idx}]"),
                "canonical_name": str(row.get("canonical_name", "")).strip(),
                "descriptor": str(row.get("descriptor", "")).strip(),
                "aliases": _optional_string_list(row.get("aliases"), label=f"links_to_existing[{idx}].aliases"),
                "bridge_takeaways": _optional_string_list(row.get("bridge_takeaways"), label=f"links_to_existing[{idx}].bridge_takeaways"),
                "bridge_tensions": _optional_string_list(row.get("bridge_tensions"), label=f"links_to_existing[{idx}].bridge_tensions"),
                "bridge_open_questions": _optional_string_list(row.get("bridge_open_questions"), label=f"links_to_existing[{idx}].bridge_open_questions"),
                "helpful_new_sources": _optional_string_list(row.get("helpful_new_sources"), label=f"links_to_existing[{idx}].helpful_new_sources"),
                "why_this_bridge_matters": str(row.get("why_this_bridge_matters", "")).strip(),
            }
        )
    normalized_new = []
    for idx, row in enumerate(new_clusters, start=1):
        if not isinstance(row, dict):
            raise EnrichmentError(f"new_clusters[{idx}] must be an object")
        canonical_name = str(row.get("canonical_name", "")).strip()
        if not canonical_name:
            raise EnrichmentError(f"new_clusters[{idx}] is missing canonical_name")
        normalized_new.append(
            {
                "canonical_name": canonical_name,
                "descriptor": str(row.get("descriptor", "")).strip(),
                "aliases": _optional_string_list(row.get("aliases"), label=f"new_clusters[{idx}].aliases"),
                "member_local_clusters": _normalize_bridge_member_refs(row.get("member_local_clusters"), pending_ids, label=f"new_clusters[{idx}]"),
                "bridge_takeaways": _optional_string_list(row.get("bridge_takeaways"), label=f"new_clusters[{idx}].bridge_takeaways"),
                "bridge_tensions": _optional_string_list(row.get("bridge_tensions"), label=f"new_clusters[{idx}].bridge_tensions"),
                "bridge_open_questions": _optional_string_list(row.get("bridge_open_questions"), label=f"new_clusters[{idx}].bridge_open_questions"),
                "helpful_new_sources": _optional_string_list(row.get("helpful_new_sources"), label=f"new_clusters[{idx}].helpful_new_sources"),
                "why_this_bridge_matters": str(row.get("why_this_bridge_matters", "")).strip(),
            }
        )
    return {"links_to_existing": normalized_links, "new_clusters": normalized_new}


def _member_cluster_row(cluster: dict) -> dict:
    row = {
        "cluster_id": str(cluster.get("cluster_id", "")).strip(),
        "domain": str(cluster.get("domain", "")).strip(),
        "collection": str(cluster.get("collection", "")).strip(),
        "collection_key": str(cluster.get("collection_key", "")).strip(),
        "canonical_name": str(cluster.get("canonical_name", "")).strip(),
        "slug": str(cluster.get("slug", "")).strip(),
        "descriptor": str(cluster.get("descriptor", "")).strip(),
        "material_ids": [str(material_id).strip() for material_id in cluster.get("material_ids", []) if str(material_id).strip()],
        "wiki_path": str(cluster.get("wiki_path", "")).strip(),
        "confidence": float(cluster.get("confidence", 0.0) or 0.0),
    }
    reflection = cluster.get("reflection")
    if isinstance(reflection, dict):
        row["reflection"] = _reflection_snapshot(reflection)
    return row


def _bridge_threshold_satisfied(member_rows: list[dict]) -> bool:
    domains = {
        _bridge_domain(str(row.get("domain", "")).strip())
        for row in member_rows
        if str(row.get("domain", "")).strip()
    }
    if len(domains) != 1:
        return False
    collection_keys = {str(row.get("collection_key", "")).strip() for row in member_rows if str(row.get("collection_key", "")).strip()}
    if not collection_keys:
        return False
    if len(collection_keys) >= 2:
        return len(member_rows) >= 2
    return len(member_rows) >= 3


def _finalize_global_bridges(
    existing_bridges: list[dict],
    parsed: dict,
    local_clusters: list[dict],
    collection_context_by_key: dict[str, dict],
    *,
    domain: str | None = None,
) -> list[dict]:
    deps = _deps()
    target_domain = _bridge_domain(domain) if domain else None
    cluster_index = {
        str(cluster.get("cluster_id", "")).strip(): cluster
        for cluster in local_clusters
        if isinstance(cluster, dict) and str(cluster.get("cluster_id", "")).strip()
    }
    pending_ids = {
        str(member.get("cluster_id", "")).strip()
        for row in parsed.get("links_to_existing", [])
        for member in row.get("member_local_clusters", [])
    } | {
        str(member.get("cluster_id", "")).strip()
        for row in parsed.get("new_clusters", [])
        for member in row.get("member_local_clusters", [])
    }
    working = []
    for row in existing_bridges:
        if not isinstance(row, dict):
            continue
        bridge_id = _safe_cluster_id(row)
        if not bridge_id:
            continue
        member_ids = [
            str(member.get("cluster_id", "")).strip()
            for member in _dict_rows(row.get("member_local_clusters", []))
            if isinstance(member, dict) and str(member.get("cluster_id", "")).strip() and str(member.get("cluster_id", "")).strip() not in pending_ids
        ]
        working.append(
            {
                "bridge_id": bridge_id,
                "canonical_name": str(row.get("canonical_name", "")).strip(),
                "slug": str(row.get("slug", "")).strip(),
                "descriptor": str(row.get("descriptor", "")).strip(),
                "aliases": deps._dedupe_strings(deps._safe_list(row.get("aliases", []))),
                "member_cluster_ids": member_ids,
                "bridge_takeaways": deps._dedupe_strings(deps._safe_list(row.get("bridge_takeaways", []))),
                "bridge_tensions": deps._dedupe_strings(deps._safe_list(row.get("bridge_tensions", []))),
                "bridge_open_questions": deps._dedupe_strings(deps._safe_list(row.get("bridge_open_questions", []))),
                "helpful_new_sources": deps._dedupe_strings(deps._safe_list(row.get("helpful_new_sources", []))),
                "why_this_bridge_matters": str(row.get("why_this_bridge_matters", "")).strip(),
            }
        )
    by_id = {row["bridge_id"]: row for row in working}
    for row in parsed.get("links_to_existing", []):
        target = by_id[row["bridge_id"]]
        target["member_cluster_ids"] = [
            *target.get("member_cluster_ids", []),
            *[member["cluster_id"] for member in row.get("member_local_clusters", [])],
        ]
        for field in (
            "canonical_name",
            "descriptor",
            "aliases",
            "bridge_takeaways",
            "bridge_tensions",
            "bridge_open_questions",
            "helpful_new_sources",
            "why_this_bridge_matters",
        ):
            value = row.get(field)
            if value:
                target[field] = value
    new_rows = []
    for row in parsed.get("new_clusters", []):
        new_rows.append(
            {
                "bridge_id": "",
                "canonical_name": row["canonical_name"],
                "slug": "",
                "descriptor": row.get("descriptor", ""),
                "aliases": row.get("aliases", []),
                "member_cluster_ids": [member["cluster_id"] for member in row.get("member_local_clusters", [])],
                "bridge_takeaways": row.get("bridge_takeaways", []),
                "bridge_tensions": row.get("bridge_tensions", []),
                "bridge_open_questions": row.get("bridge_open_questions", []),
                "helpful_new_sources": row.get("helpful_new_sources", []),
                "why_this_bridge_matters": row.get("why_this_bridge_matters", ""),
            }
        )
    rows = [*working, *new_rows]
    used_ids: set[str] = set()
    slug_counts: dict[str, int] = {}
    output = []
    for row in rows:
        member_ids = []
        seen_members: set[str] = set()
        for cluster_id in row.get("member_cluster_ids", []):
            cluster_id = str(cluster_id).strip()
            if not cluster_id or cluster_id in seen_members or cluster_id not in cluster_index:
                continue
            seen_members.add(cluster_id)
            member_ids.append(cluster_id)
        member_rows = [_member_cluster_row(cluster_index[cluster_id]) for cluster_id in member_ids]
        member_domains = {
            _bridge_domain(str(member.get("domain", "")).strip())
            for member in member_rows
            if str(member.get("domain", "")).strip()
        }
        if len(member_domains) != 1:
            raise EnrichmentError("Global bridge members must all belong to the same domain")
        bridge_domain = next(iter(member_domains))
        if target_domain and bridge_domain != target_domain:
            raise EnrichmentError(
                f"Global bridge domain mismatch: expected {target_domain}, got {bridge_domain}"
            )
        if not _bridge_threshold_satisfied(member_rows):
            continue
        canonical_name = str(row.get("canonical_name", "")).strip() or str(member_rows[0].get("canonical_name", "")).strip()
        base_slug = str(row.get("slug", "")).strip() or deps.slugify(canonical_name) or "global-bridge"
        bridge_id = str(row.get("bridge_id", "")).strip()
        preferred_id_prefix = f"global_bridge__{bridge_domain}__"
        if not bridge_id or not bridge_id.startswith(preferred_id_prefix):
            slug_counts[base_slug] = slug_counts.get(base_slug, 0) + 1
            slug = base_slug if slug_counts[base_slug] == 1 else f"{base_slug}-{slug_counts[base_slug]}"
            candidate_id = f"{preferred_id_prefix}{slug}"
            while candidate_id in used_ids:
                slug_counts[base_slug] += 1
                slug = f"{base_slug}-{slug_counts[base_slug]}"
                candidate_id = f"{preferred_id_prefix}{slug}"
            bridge_id = candidate_id
            used_ids.add(bridge_id)
        else:
            slug = str(row.get("slug", "")).strip() or bridge_id.removeprefix(preferred_id_prefix) or base_slug
            used_ids.add(bridge_id)
        collection_keys = sorted({row["collection_key"] for row in member_rows if row.get("collection_key")})
        supporting_material_ids = sorted(
            {
                material_id
                for member in member_rows
                for material_id in deps._safe_list(member.get("material_ids", []))
                if str(material_id).strip()
            }
        )
        supporting_collection_reflections = [
            collection_context_by_key[key]
            for key in collection_keys
            if key in collection_context_by_key
        ]
        confidence_values = [float(member.get("confidence", 0.0) or 0.0) for member in member_rows]
        aliases = deps._dedupe_strings(
            [
                *deps._safe_list(row.get("aliases", [])),
                *[member.get("canonical_name", "") for member in member_rows],
            ]
        )
        aliases = [alias for alias in aliases if alias and alias.casefold() != canonical_name.casefold()][:8]
        output.append(
            {
                "bridge_id": bridge_id,
                "domain": bridge_domain,
                "canonical_name": canonical_name,
                "slug": slug,
                "descriptor": str(row.get("descriptor", "")).strip(),
                "aliases": aliases,
                "member_local_clusters": member_rows,
                "domain_collection_keys": collection_keys,
                "supporting_material_ids": supporting_material_ids,
                "bridge_takeaways": deps._dedupe_strings(deps._safe_list(row.get("bridge_takeaways", [])))[:6],
                "bridge_tensions": deps._dedupe_strings(deps._safe_list(row.get("bridge_tensions", [])))[:6],
                "bridge_open_questions": deps._dedupe_strings(deps._safe_list(row.get("bridge_open_questions", [])))[:6],
                "helpful_new_sources": deps._dedupe_strings(deps._safe_list(row.get("helpful_new_sources", [])))[:6],
                "why_this_bridge_matters": str(row.get("why_this_bridge_matters", "")).strip(),
                "supporting_collection_reflections": supporting_collection_reflections,
                "confidence": round(sum(confidence_values) / len(confidence_values), 4) if confidence_values else 0.0,
                "wiki_path": _bridge_wiki_path(bridge_domain, slug),
            }
        )
    return sorted(output, key=lambda row: (row["canonical_name"].casefold(), row["bridge_id"]))


def _represented_pending_cluster_ids(bridges: list[dict], pending_ids: set[str]) -> set[str]:
    represented = set()
    for bridge in bridges:
        for member in _dict_rows(bridge.get("member_local_clusters", [])):
            cluster_id = str(member.get("cluster_id", "")).strip()
            if cluster_id in pending_ids:
                represented.add(cluster_id)
    return represented


def _write_global_bridge_no_progress_diagnostic(
    root: Path,
    domain: str,
    *,
    raw: str,
    parsed: Any,
    normalized: dict,
    pending_ids: set[str],
    represented_ids: set[str],
    bridge_count: int,
) -> Path:
    deps = _deps()
    path = _global_bridge_no_progress_path(root, domain)
    deps._write_json(
        path,
        {
            "domain": _bridge_domain(domain),
            "diagnostic": "global bridge output made no progress",
            "written_at": datetime.now(timezone.utc).isoformat(),
            "pending_cluster_ids": sorted(pending_ids),
            "represented_pending_cluster_ids": sorted(represented_ids),
            "unrepresented_pending_cluster_ids": sorted(pending_ids - represented_ids),
            "bridge_count": bridge_count,
            "raw_response": raw,
            "parsed_response": parsed,
            "normalized_response": normalized,
        },
    )
    return path


def load_global_bridge_clusters(root: Path, domain: str | None = None) -> list[dict]:
    deps = _deps()
    rows = []
    target_domain = _bridge_domain(domain) if domain else None
    for artifact_path in (
        [_global_bridge_artifact_path(root, target_domain)]
        if target_domain and _global_bridge_artifact_path(root, target_domain).exists()
        else global_bridge_artifact_paths(root)
    ):
        for row in deps._load_jsonl(artifact_path):
            if not isinstance(row, dict):
                continue
            bridge_id = str(row.get("bridge_id", "")).strip()
            if not bridge_id:
                continue
            member_rows = [
                dict(member)
                for member in _dict_rows(row.get("member_local_clusters", []))
                if isinstance(member, dict)
            ]
            bridge_domain = str(row.get("domain", "")).strip()
            if not bridge_domain:
                member_domains = {
                    _bridge_domain(str(member.get("domain", "")).strip())
                    for member in member_rows
                    if str(member.get("domain", "")).strip()
                }
                bridge_domain = next(iter(member_domains)) if len(member_domains) == 1 else ""
            if target_domain and bridge_domain and _bridge_domain(bridge_domain) != target_domain:
                continue
            rows.append(
                {
                    **row,
                    "domain": bridge_domain,
                    "cluster_id": bridge_id,
                    "material_ids": [
                        str(material_id).strip()
                        for material_id in deps._safe_list(row.get("supporting_material_ids", []))
                        if str(material_id).strip()
                    ],
                    "member_local_clusters": member_rows,
                    "source_concepts": [],
                    "wiki_path": str(row.get("wiki_path", "")).strip()
                    or (
                        _bridge_wiki_path(bridge_domain, str(row.get("slug", "")).strip())
                        if bridge_domain
                        else f"wiki/shared/bridge-concepts/{str(row.get('slug', '')).strip()}.md"
                    ),
                }
            )
    return rows


def _legacy_global_bridge_rows(root: Path) -> list[dict]:
    deps = _deps()
    return [
        dict(row)
        for row in deps._load_jsonl(_global_bridge_artifact_path(root))
        if isinstance(row, dict) and str(row.get("bridge_id", "")).strip()
    ]


def _legacy_bridge_page_source(root: Path, row: dict, slug: str) -> Path | None:
    wiki_path = str(row.get("wiki_path", "")).strip()
    if wiki_path:
        candidate = root / wiki_path
        if candidate.exists():
            return candidate
    fallback = root / "wiki" / "shared" / "bridge-concepts" / f"{slug}.md"
    return fallback if fallback.exists() else None


def _backup_file(path: Path, root: Path, backup_root: Path) -> None:
    if not path.exists():
        return
    target = backup_root / path.relative_to(root)
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(path, target)


def _legacy_bridge_migration_plan(
    row: dict,
    *,
    local_cluster_index: dict[str, dict],
    collection_context_by_key: dict[str, dict],
    used_ids: set[str],
    slug_counts: dict[str, int],
) -> tuple[dict | None, dict | None]:
    deps = _deps()
    bridge_id = str(row.get("bridge_id", "")).strip() or "(missing bridge_id)"
    raw_members = _dict_rows(row.get("member_local_clusters", []))
    if not raw_members:
        return None, {"bridge_id": bridge_id, "reason": "no member_local_clusters", "domains": []}

    member_rows: list[dict] = []
    member_domains: set[str] = set()
    missing_cluster_ids: list[str] = []
    missing_domains: list[str] = []
    for raw_member in raw_members:
        cluster_id = str(raw_member.get("cluster_id", "")).strip()
        if not cluster_id:
            continue
        cluster = local_cluster_index.get(cluster_id)
        if cluster is None:
            missing_cluster_ids.append(cluster_id)
            cluster = {}
        raw_domain = str(raw_member.get("domain", "")).strip() or str(cluster.get("domain", "")).strip()
        if not raw_domain:
            missing_domains.append(cluster_id)
            continue
        domain = _bridge_domain(raw_domain)
        collection = str(raw_member.get("collection", "")).strip() or str(cluster.get("collection", "")).strip()
        scope_domain, scope_collection = deps._collection_scope(domain, collection)
        member_domains.add(scope_domain)
        merged_cluster = {
            **cluster,
            **raw_member,
            "cluster_id": cluster_id,
            "domain": scope_domain,
            "collection": scope_collection,
            "collection_key": f"{scope_domain}/{scope_collection}",
            "canonical_name": str(raw_member.get("canonical_name", "")).strip() or str(cluster.get("canonical_name", "")).strip(),
            "slug": str(raw_member.get("slug", "")).strip() or str(cluster.get("slug", "")).strip(),
            "descriptor": str(raw_member.get("descriptor", "")).strip() or str(cluster.get("descriptor", "")).strip(),
            "material_ids": raw_member.get("material_ids") or cluster.get("material_ids") or [],
            "wiki_path": str(raw_member.get("wiki_path", "")).strip() or str(cluster.get("wiki_path", "")).strip(),
            "confidence": raw_member.get("confidence", cluster.get("confidence", 0.0)),
        }
        member_rows.append(_member_cluster_row(merged_cluster))

    if missing_cluster_ids:
        return None, {
            "bridge_id": bridge_id,
            "reason": "unknown local clusters",
            "domains": sorted(member_domains),
            "cluster_ids": missing_cluster_ids,
        }
    if missing_domains:
        return None, {
            "bridge_id": bridge_id,
            "reason": "member domains could not be inferred",
            "domains": sorted(member_domains),
            "cluster_ids": missing_domains,
        }
    if len(member_domains) != 1:
        return None, {
            "bridge_id": bridge_id,
            "reason": "bridge spans multiple domains",
            "domains": sorted(member_domains),
        }

    bridge_domain = next(iter(member_domains))
    canonical_name = str(row.get("canonical_name", "")).strip() or str(member_rows[0].get("canonical_name", "")).strip() or bridge_id
    base_slug = str(row.get("slug", "")).strip() or deps.slugify(canonical_name) or "global-bridge"
    preferred_id_prefix = f"global_bridge__{bridge_domain}__"
    migrated_bridge_id = bridge_id
    if not migrated_bridge_id.startswith(preferred_id_prefix):
        slug_counts[base_slug] = slug_counts.get(base_slug, 0) + 1
        slug = base_slug if slug_counts[base_slug] == 1 else f"{base_slug}-{slug_counts[base_slug]}"
        candidate_id = f"{preferred_id_prefix}{slug}"
        while candidate_id in used_ids:
            slug_counts[base_slug] += 1
            slug = f"{base_slug}-{slug_counts[base_slug]}"
            candidate_id = f"{preferred_id_prefix}{slug}"
        migrated_bridge_id = candidate_id
        used_ids.add(migrated_bridge_id)
    else:
        slug = str(row.get("slug", "")).strip() or migrated_bridge_id.removeprefix(preferred_id_prefix) or base_slug
        used_ids.add(migrated_bridge_id)

    collection_keys = sorted({
        str(member.get("collection_key", "")).strip()
        for member in member_rows
        if str(member.get("collection_key", "")).strip()
    })
    supporting_material_ids = sorted(
        {
            str(material_id).strip()
            for material_id in deps._safe_list(row.get("supporting_material_ids", []))
            if str(material_id).strip()
        }
        or {
            material_id
            for member in member_rows
            for material_id in deps._safe_list(member.get("material_ids", []))
            if str(material_id).strip()
        }
    )
    supporting_collection_reflections = [
        collection_context_by_key[key]
        for key in collection_keys
        if key in collection_context_by_key
    ]
    confidence_values = [float(member.get("confidence", 0.0) or 0.0) for member in member_rows]
    aliases = deps._dedupe_strings(
        [
            *deps._safe_list(row.get("aliases", [])),
            *[str(member.get("canonical_name", "")).strip() for member in member_rows],
        ]
    )
    aliases = [alias for alias in aliases if alias and alias.casefold() != canonical_name.casefold()][:8]
    migrated = {
        "bridge_id": migrated_bridge_id,
        "domain": bridge_domain,
        "canonical_name": canonical_name,
        "slug": slug,
        "descriptor": str(row.get("descriptor", "")).strip(),
        "aliases": aliases,
        "member_local_clusters": member_rows,
        "domain_collection_keys": collection_keys,
        "supporting_material_ids": supporting_material_ids,
        "bridge_takeaways": deps._dedupe_strings(deps._safe_list(row.get("bridge_takeaways", [])))[:6],
        "bridge_tensions": deps._dedupe_strings(deps._safe_list(row.get("bridge_tensions", [])))[:6],
        "bridge_open_questions": deps._dedupe_strings(deps._safe_list(row.get("bridge_open_questions", [])))[:6],
        "helpful_new_sources": deps._dedupe_strings(deps._safe_list(row.get("helpful_new_sources", [])))[:6],
        "why_this_bridge_matters": str(row.get("why_this_bridge_matters", "")).strip(),
        "supporting_collection_reflections": supporting_collection_reflections,
        "confidence": round(sum(confidence_values) / len(confidence_values), 4) if confidence_values else 0.0,
        "wiki_path": _bridge_wiki_path(bridge_domain, slug),
    }
    warning = None
    if len(collection_keys) < 2 and len(member_rows) < 3:
        warning = {
            "bridge_id": migrated_bridge_id,
            "reason": "bridge preserves legacy within-collection shape below current creation threshold",
            "domains": [bridge_domain],
        }
    return migrated, warning


def migrate_legacy_global_bridges(
    root: Path | None = None,
    *,
    apply: bool = False,
    force: bool = False,
) -> dict:
    deps = _deps()
    if root is None:
        root = deps.get_project_root()
    root = Path(root).resolve()
    legacy_artifact = _global_bridge_artifact_path(root)
    legacy_stamp = _global_bridge_stamp_path(root)
    legacy_rows = _legacy_global_bridge_rows(root)
    shared_glossary = root / "wiki" / "shared" / "glossary" / "_index.md"
    local_clusters = deps.load_local_clusters(root)
    local_cluster_index = {
        str(cluster.get("cluster_id", "")).strip(): dict(cluster)
        for cluster in local_clusters
        if str(cluster.get("cluster_id", "")).strip()
    }
    collection_refs = deps._load_jsonl(root / deps.LINT_DIR / "collection_reflections.jsonl")
    collection_context_by_key = {
        row["collection_key"]: row
        for row in _collection_context_rows(collection_refs)
        if str(row.get("collection_key", "")).strip()
    }

    migrated_by_domain: dict[str, list[dict]] = {}
    page_copies: list[dict] = []
    warnings: list[dict] = []
    ambiguous: list[dict] = []
    used_ids: set[str] = set()
    slug_counts: dict[str, int] = {}

    for row in legacy_rows:
        migrated, warning = _legacy_bridge_migration_plan(
            row,
            local_cluster_index=local_cluster_index,
            collection_context_by_key=collection_context_by_key,
            used_ids=used_ids,
            slug_counts=slug_counts,
        )
        if migrated is None:
            ambiguous.append(warning or {"bridge_id": str(row.get("bridge_id", "")).strip(), "reason": "could not migrate", "domains": []})
            continue
        if warning:
            warnings.append(warning)
        migrated_by_domain.setdefault(migrated["domain"], []).append(migrated)
        source_page = _legacy_bridge_page_source(root, row, migrated["slug"])
        if source_page is not None:
            page_copies.append(
                {
                    "bridge_id": migrated["bridge_id"],
                    "domain": migrated["domain"],
                    "source": source_page,
                    "target": root / migrated["wiki_path"],
                }
            )

    for domain_rows in migrated_by_domain.values():
        domain_rows.sort(key=lambda item: (item["canonical_name"].casefold(), item["bridge_id"]))

    glossary_original = shared_glossary.read_text(encoding="utf-8") if shared_glossary.exists() else ""
    glossary_updated = glossary_original
    glossary_replacements = 0
    for domain_rows in migrated_by_domain.values():
        for row in domain_rows:
            legacy_path = f"wiki/shared/bridge-concepts/{row['slug']}.md"
            if legacy_path in glossary_updated:
                glossary_updated = glossary_updated.replace(legacy_path, row["wiki_path"])
                glossary_replacements += 1

    collisions: list[str] = []
    for domain, rows in migrated_by_domain.items():
        artifact_path = _global_bridge_artifact_path(root, domain)
        stamp_path = _global_bridge_stamp_path(root, domain)
        if not force and artifact_path.exists():
            collisions.append(str(artifact_path))
        if not force and stamp_path.exists():
            collisions.append(str(stamp_path))
        for item in page_copies:
            if item["domain"] != domain:
                continue
            target = item["target"]
            if not force and target.exists():
                collisions.append(str(target))
    if not force and shared_glossary.exists() and glossary_updated != glossary_original and shared_glossary.exists():
        collisions = list(dict.fromkeys(collisions))

    can_apply = bool(legacy_rows) and not collisions and not ambiguous
    backup_root: Path | None = None
    if apply and not can_apply:
        return {
            "applied": False,
            "can_apply": False,
            "root": str(root),
            "legacy_artifact_found": legacy_artifact.exists(),
            "legacy_stamp_found": legacy_stamp.exists(),
            "legacy_bridges": len(legacy_rows),
            "migrated_bridges": sum(len(rows) for rows in migrated_by_domain.values()),
            "migrated_domains": {domain: len(rows) for domain, rows in migrated_by_domain.items()},
            "ambiguous_bridges": ambiguous,
            "warnings": warnings,
            "collisions": collisions,
            "page_copies": len(page_copies),
            "glossary_replacements": glossary_replacements,
            "backup_root": None,
            "next_steps": ["Resolve collisions or ambiguous bridges before rerunning with --apply."],
        }

    if apply and legacy_rows:
        backup_root = root / "derived" / "migrations" / "global_bridge_domain_scope" / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        if legacy_artifact.exists():
            _backup_file(legacy_artifact, root, backup_root)
        if legacy_stamp.exists():
            _backup_file(legacy_stamp, root, backup_root)
        if shared_glossary.exists():
            _backup_file(shared_glossary, root, backup_root)
        for item in page_copies:
            target = item["target"]
            if target.exists():
                _backup_file(target, root, backup_root)
        for domain, rows in migrated_by_domain.items():
            artifact_path = _global_bridge_artifact_path(root, domain)
            stamp_path = _global_bridge_stamp_path(root, domain)
            artifact_path.parent.mkdir(parents=True, exist_ok=True)
            deps._write_jsonl(artifact_path, rows)
            bundle = _global_bridge_inputs(root, local_clusters, collection_refs, domain=domain)
            deps._write_stamp(
                stamp_path,
                {
                    "domain": domain,
                    "migrated_at": datetime.now(timezone.utc).isoformat(),
                    "legacy_bridged_at": (deps._read_stamp(legacy_stamp) or {}).get("bridged_at", "") if legacy_stamp.exists() else "",
                    "input_fingerprint": f"migrated:{domain}",
                    "local_cluster_fingerprints": bundle["local_cluster_fingerprints"],
                    "collection_context_fingerprints": bundle["collection_context_fingerprints"],
                    "pending_local_clusters": 0,
                    "global_bridge_count": len(rows),
                    "collection_scope_count": bundle["collection_scope_count"],
                },
            )
        for item in page_copies:
            target = item["target"]
            target.parent.mkdir(parents=True, exist_ok=True)
            if force or not target.exists():
                target.write_text(item["source"].read_text(encoding="utf-8"), encoding="utf-8")
        if shared_glossary.exists() and glossary_updated != glossary_original:
            shared_glossary.write_text(glossary_updated, encoding="utf-8")

    next_steps = []
    if ambiguous:
        next_steps.append("Review ambiguous bridges manually before deleting legacy shared bridge files.")
    if apply and legacy_rows:
        next_steps.append("Run `arq memory rebuild` in the migrated vault before relying on search-backed bridge pages.")
        next_steps.append("Run `arq compile --recompile-pages` in the migrated vault to refresh domain bridge pages and indexes.")
    elif legacy_rows:
        next_steps.append("Rerun with --apply once the preview looks correct.")

    return {
        "applied": bool(apply and can_apply),
        "can_apply": can_apply,
        "root": str(root),
        "legacy_artifact_found": legacy_artifact.exists(),
        "legacy_stamp_found": legacy_stamp.exists(),
        "legacy_bridges": len(legacy_rows),
        "migrated_bridges": sum(len(rows) for rows in migrated_by_domain.values()),
        "migrated_domains": {domain: len(rows) for domain, rows in migrated_by_domain.items()},
        "ambiguous_bridges": ambiguous,
        "warnings": warnings,
        "collisions": collisions,
        "page_copies": len(page_copies),
        "glossary_replacements": glossary_replacements,
        "backup_root": str(backup_root) if backup_root is not None else None,
        "next_steps": next_steps,
    }


def _global_bridge_due(root: Path, local_clusters: list[dict], collection_refs: list[dict]) -> tuple[bool, str]:
    domains = {
        _bridge_domain(str(cluster.get("domain", "")).strip())
        for cluster in local_clusters
        if str(cluster.get("domain", "")).strip()
    }
    domains.update(path.parent.name for path in global_bridge_artifact_paths(root) if path.parent.name != "derived")
    domains = sorted(domain for domain in domains if domain)
    if not domains:
        return False, "no domains"
    deps = _deps()
    reasons: list[str] = []
    any_due = False
    for domain in domains:
        bundle = _global_bridge_inputs(root, local_clusters, collection_refs, domain=domain)
        artifact_path = _global_bridge_artifact_path(root, domain)
        stamp = deps._read_stamp(_global_bridge_stamp_path(root, domain))
        if bundle["collection_scope_count"] < 2:
            if artifact_path.exists() or stamp:
                reasons.append(f"{domain}: global bridge cleanup needed")
                any_due = True
                continue
            reasons.append(f"{domain}: fewer than 2 collections")
            continue
        if not artifact_path.exists():
            reasons.append(f"{domain}: global bridge artifact missing")
            any_due = True
            continue
        if not stamp:
            reasons.append(f"{domain}: global bridge stamp missing")
            any_due = True
            continue
        if bundle["packet"].get("pending_local_clusters", []):
            reasons.append(f"{domain}: new local clusters pending")
            any_due = True
            continue
        reasons.append(f"{domain}: global bridge unchanged")
    return any_due, "; ".join(reasons)


def _run_global_bridge_impl(
    deps: Any,
    root: Path,
    local_clusters: list[dict],
    collection_refs: list[dict],
    llm_factory=None,
    tool=None,
    route_signature: str = "",
) -> dict:
    del tool
    domains = {
        _bridge_domain(str(cluster.get("domain", "")).strip())
        for cluster in local_clusters
        if str(cluster.get("domain", "")).strip()
    }
    domains.update(path.parent.name for path in global_bridge_artifact_paths(root) if path.parent.name != "derived")
    domains = sorted(domain for domain in domains if domain)
    if not domains:
        return {
            "global_bridges": 0,
            "global_bridge_skipped": True,
            "global_bridge_skip_reason": "no domains",
            "domains": {},
        }

    llm_fn = llm_factory("lint")
    run_at_any: str | None = None
    domain_results: dict[str, dict] = {}
    ran_any = False
    reasons: list[str] = []
    failures: list[BaseException] = []

    for domain in domains:
        bundle = _global_bridge_inputs(root, local_clusters, collection_refs, domain=domain)
        artifact_path = deps._global_bridge_artifact_path(root, domain)
        stamp_path = deps._global_bridge_stamp_path(root, domain)
        stamp = deps._read_stamp(stamp_path)
        if bundle["collection_scope_count"] < 2:
            if artifact_path.exists():
                artifact_path.unlink()
                ran_any = True
            if stamp_path.exists():
                stamp_path.unlink()
                ran_any = True
            if ran_any:
                run_at_any = datetime.now(timezone.utc).isoformat()
            if artifact_path.exists() or stamp:
                domain_results[domain] = {
                    "global_bridges": 0,
                    "global_bridge_skipped": False,
                    "global_bridge_skip_reason": "",
                }
                reasons.append(f"{domain}: cleaned up stale bridges")
                continue
            domain_results[domain] = {
                "global_bridges": len(bundle["existing_bridges"]),
                "global_bridge_skipped": True,
                "global_bridge_skip_reason": "fewer than 2 collections",
            }
            reasons.append(f"{domain}: fewer than 2 collections")
            continue

        if artifact_path.exists() and stamp and not bundle["packet"].get("pending_local_clusters", []):
            domain_results[domain] = {
                "global_bridges": len(bundle["existing_bridges"]),
                "global_bridge_skipped": True,
                "global_bridge_skip_reason": "global bridge unchanged",
            }
            reasons.append(f"{domain}: global bridge unchanged")
            continue

        packet_path = _global_bridge_packet_path(root, domain)
        memory_path = _global_bridge_memory_path(root, domain)
        no_progress_path = _global_bridge_no_progress_path(root, domain)
        deps._write_json(packet_path, bundle["packet"])
        deps._write_json(memory_path, bundle["memory"])

        system, user = _global_bridge_prompt(packet_path, memory_path, domain)
        succeeded = False
        no_progress_diagnostic_path: Path | None = None
        try:
            raw = llm_fn(system, [{"role": "user", "content": user}])
            parsed = parse_json_or_repair(llm_fn, raw, _GLOBAL_BRIDGE_DELTA_SCHEMA)
            normalized = _normalize_global_bridge_response(
                parsed,
                bundle["existing_bridges"],
                {
                    row["cluster_id"]
                    for row in bundle["packet"].get("pending_local_clusters", [])
                    if str(row.get("cluster_id", "")).strip()
                },
            )
            pending_ids = {
                row["cluster_id"]
                for row in bundle["packet"].get("pending_local_clusters", [])
                if str(row.get("cluster_id", "")).strip()
            }
            bridges = _finalize_global_bridges(
                bundle["existing_bridges"],
                normalized,
                bundle["all_local_clusters"],
                bundle["collection_context_by_key"],
                domain=domain,
            )
            represented_pending_ids = _represented_pending_cluster_ids(bridges, pending_ids)
            if pending_ids and not represented_pending_ids:
                no_progress_diagnostic_path = _write_global_bridge_no_progress_diagnostic(
                    root,
                    domain,
                    raw=raw,
                    parsed=parsed,
                    normalized=normalized,
                    pending_ids=pending_ids,
                    represented_ids=represented_pending_ids,
                    bridge_count=len(bridges),
                )
            run_at = datetime.now(timezone.utc).isoformat()
            run_at_any = run_at
            deps._attach_run_provenance(bridges, route_signature, run_at)
            deps._write_jsonl(artifact_path, bridges)
            post_run_bundle = _global_bridge_inputs(
                root,
                local_clusters,
                collection_refs,
                domain=domain,
                previous_cluster_fingerprints=bundle["local_cluster_fingerprints"],
                previous_collection_fingerprints=bundle["collection_context_fingerprints"],
            )
            deps._write_stamp(
                stamp_path,
                {
                    "domain": domain,
                    "bridged_at": run_at,
                    "input_fingerprint": post_run_bundle["input_fingerprint"],
                    "local_cluster_fingerprints": bundle["local_cluster_fingerprints"],
                    "collection_context_fingerprints": bundle["collection_context_fingerprints"],
                    "pending_local_clusters": len(post_run_bundle["packet"].get("pending_local_clusters", [])),
                    "global_bridge_count": len(bridges),
                    "collection_scope_count": post_run_bundle["collection_scope_count"],
                },
            )
            succeeded = True
            ran_any = True
            domain_results[domain] = {
                "global_bridges": len(bridges),
                "global_bridge_skipped": False,
                "global_bridge_skip_reason": "",
            }
            if no_progress_diagnostic_path is not None:
                domain_results[domain]["global_bridge_no_progress"] = True
                domain_results[domain]["global_bridge_diagnostic_path"] = str(no_progress_diagnostic_path)
        except BaseException as exc:
            failures.append(exc)
            domain_results[domain] = {
                "global_bridges": len(bundle["existing_bridges"]),
                "global_bridge_skipped": False,
                "global_bridge_skip_reason": f"failed: {exc}",
            }
        finally:
            if succeeded:
                cleanup_paths = [packet_path, memory_path]
                if no_progress_diagnostic_path is None:
                    cleanup_paths.append(no_progress_path)
                deps._cleanup_paths(*cleanup_paths)

    if run_at_any is not None:
        deps._write_lint_stage_stamp(root, global_bridge_at=run_at_any)
    if failures:
        raise failures[0]

    total_bridges = len(load_global_bridge_clusters(root))
    if ran_any:
        return {
            "global_bridges": total_bridges,
            "global_bridge_skipped": False,
            "global_bridge_skip_reason": "",
            "domains": domain_results,
        }
    return {
        "global_bridges": total_bridges,
        "global_bridge_skipped": True,
        "global_bridge_skip_reason": "; ".join(reasons),
        "domains": domain_results,
    }
