from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from arquimedes.llm import EnrichmentError
from arquimedes.llm import parse_json_or_repair

_GLOBAL_BRIDGE_DELTA_SCHEMA = '{"links_to_existing":[{"bridge_id":"required existing bridge id","member_local_clusters":[{"cluster_id":"required pending local cluster id"}],"canonical_name":"string(optional)","descriptor":"string(optional)","aliases":["strings(optional)"],"bridge_takeaways":["strings(optional)"],"bridge_tensions":["strings(optional)"],"bridge_open_questions":["strings(optional)"],"helpful_new_sources":["strings(optional)"],"why_this_bridge_matters":"string(optional)"}],"new_clusters":[{"canonical_name":"required string","descriptor":"short bridge description","aliases":["max 4 strings"],"member_local_clusters":[{"cluster_id":"required pending local cluster id"}],"bridge_takeaways":["strings"],"bridge_tensions":["strings"],"bridge_open_questions":["strings"],"helpful_new_sources":["strings"],"why_this_bridge_matters":"string"}],"_finished":true}'
_REQUIRED_GLOBAL_BRIDGE_FIELDS = ("links_to_existing", "new_clusters")


def _deps():
    import arquimedes.lint as deps

    return deps


def _global_bridge_artifact_path(root: Path) -> Path:
    return root / "derived" / "global_bridge_clusters.jsonl"


def _global_bridge_stamp_path(root: Path) -> Path:
    return root / "derived" / "global_bridge_stamp.json"


def global_bridge_artifact_path(root: Path) -> Path:
    return _global_bridge_artifact_path(root)


def global_bridge_stamp_path(root: Path) -> Path:
    return _global_bridge_stamp_path(root)


def _global_bridge_stage_dir(root: Path) -> Path:
    return root / "derived" / "tmp" / "global_bridge"


def _global_bridge_packet_path(root: Path) -> Path:
    return _global_bridge_stage_dir(root) / "global_bridge.packet.json"


def _global_bridge_memory_path(root: Path) -> Path:
    return _global_bridge_stage_dir(root) / "global_bridge.memory.json"


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
                "title": collection,
                "main_takeaways": deps._dedupe_strings(deps._safe_list(row.get("main_takeaways", [])))[:4],
                "main_tensions": deps._dedupe_strings(deps._safe_list(row.get("main_tensions", [])))[:4],
                "why_this_collection_matters": str(row.get("why_this_collection_matters", "")).strip(),
            }
        )
    return sorted(rows, key=lambda row: row["collection_key"])


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
        "reflection": {
            "main_takeaways": deps._dedupe_strings(deps._safe_list(reflection.get("main_takeaways", [])))[:4],
            "main_tensions": deps._dedupe_strings(deps._safe_list(reflection.get("main_tensions", [])))[:4],
            "open_questions": deps._dedupe_strings(deps._safe_list(reflection.get("open_questions", [])))[:4],
            "helpful_new_sources": deps._dedupe_strings(deps._safe_list(reflection.get("helpful_new_sources", [])))[:4],
            "why_this_concept_matters": str(reflection.get("why_this_concept_matters", "")).strip(),
        },
    }


def _bridge_memory_snapshot(clusters: list[dict]) -> list[dict]:
    deps = _deps()
    rows = []
    for row in clusters:
        if not isinstance(row, dict):
            continue
        bridge_id = _safe_cluster_id(row)
        if not bridge_id:
            continue
        rows.append(
            {
                "bridge_id": bridge_id,
                "canonical_name": str(row.get("canonical_name", "")).strip(),
                "descriptor": str(row.get("descriptor", "")).strip(),
                "aliases": deps._dedupe_strings(deps._safe_list(row.get("aliases", [])))[:8],
                "member_local_clusters": [
                    {
                        "cluster_id": str(member.get("cluster_id", "")).strip(),
                        "collection_key": str(member.get("collection_key", "")).strip() or f"{str(member.get('domain', '')).strip()}/{str(member.get('collection', '')).strip()}",
                        "canonical_name": str(member.get("canonical_name", "")).strip(),
                    }
                    for member in _dict_rows(row.get("member_local_clusters", []))
                    if isinstance(member, dict) and str(member.get("cluster_id", "")).strip()
                ],
                "bridge_takeaways": deps._dedupe_strings(deps._safe_list(row.get("bridge_takeaways", [])))[:6],
                "bridge_open_questions": deps._dedupe_strings(deps._safe_list(row.get("bridge_open_questions", [])))[:6],
                "why_this_bridge_matters": str(row.get("why_this_bridge_matters", "")).strip(),
            }
        )
    return sorted(rows, key=lambda row: (row["canonical_name"].casefold(), row["bridge_id"]))


def _global_bridge_inputs(root: Path, local_clusters: list[dict], collection_refs: list[dict]) -> dict:
    deps = _deps()
    reflection_map = _local_cluster_reflection_map(root)
    local_rows = [
        snapshot
        for cluster in local_clusters
        for snapshot in [_local_cluster_snapshot(cluster, reflection_map)]
        if snapshot is not None
    ]
    local_rows = sorted(local_rows, key=lambda row: (row["collection_key"], row["canonical_name"].casefold(), row["cluster_id"]))
    collection_context = _collection_context_rows(collection_refs)
    collection_context_by_key = {row["collection_key"]: row for row in collection_context}
    collection_scope_count = len({row["collection_key"] for row in local_rows})
    existing_bridges = load_global_bridge_clusters(root)
    existing_member_ids = {
        str(member.get("cluster_id", "")).strip()
        for bridge in existing_bridges
        for member in _dict_rows(bridge.get("member_local_clusters", []))
        if isinstance(member, dict) and str(member.get("cluster_id", "")).strip()
    }
    stamp = deps._read_stamp(_global_bridge_stamp_path(root))
    previous_cluster_fps = stamp.get("local_cluster_fingerprints", {}) if isinstance(stamp, dict) else {}
    previous_collection_fps = stamp.get("collection_context_fingerprints", {}) if isinstance(stamp, dict) else {}
    current_cluster_fps = {
        row["cluster_id"]: deps.canonical_hash(row)
        for row in local_rows
    }
    current_collection_fps = {
        row["collection_key"]: deps.canonical_hash(row)
        for row in collection_context
    }
    changed_collection_keys = {
        key
        for key, fingerprint in current_collection_fps.items()
        if previous_collection_fps.get(key) != fingerprint
    }
    pending_local_clusters = []
    for row in local_rows:
        cluster_id = row["cluster_id"]
        collection_key = row["collection_key"]
        if (
            not existing_bridges
            or previous_cluster_fps.get(cluster_id) != current_cluster_fps.get(cluster_id)
            or collection_key in changed_collection_keys
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
        "bridges": _bridge_memory_snapshot(existing_bridges),
    }
    fingerprint = deps.canonical_hash({"packet": packet, "memory": memory})
    return {
        "collection_scope_count": collection_scope_count,
        "packet": packet,
        "memory": memory,
        "existing_bridges": existing_bridges,
        "collection_context_by_key": collection_context_by_key,
        "input_fingerprint": fingerprint,
        "local_cluster_fingerprints": current_cluster_fps,
        "collection_context_fingerprints": current_collection_fps,
    }


def _global_bridge_input_snapshot(local_clusters: list[dict], collection_refs: list[dict], root: Path | None = None) -> dict:
    if root is None:
        deps = _deps()
        reflection_map: dict[str, dict] = {}
        local_rows = [
            snapshot
            for cluster in local_clusters
            for snapshot in [_local_cluster_snapshot(cluster, reflection_map)]
            if snapshot is not None
        ]
        collection_context = _collection_context_rows(collection_refs)
        return {
            "kind": "global_bridge_packet",
            "pending_local_clusters": sorted(local_rows, key=lambda row: (row["collection_key"], row["canonical_name"].casefold(), row["cluster_id"])),
            "collection_context": collection_context,
        }
    return _global_bridge_inputs(root, local_clusters, collection_refs)["packet"]


def _global_bridge_prompt(packet_path: Path, memory_path: Path) -> tuple[str, str]:
    system = (
        "You are an architecture research librarian. You are grouping collection-local concept clusters into broader global bridge concepts across the knowledge system.\n"
        "\n"
        "Output schema:\n"
        f"{_GLOBAL_BRIDGE_DELTA_SCHEMA}\n"
        "\n"
        "Rules:\n"
        "- Work only with the pending collection-local clusters in the packet. Do not invent members that are not present there.\n"
        "- Use the existing global bridge memory only to decide whether pending local clusters belong to an existing bridge or should create a new bridge.\n"
        "- Prefer bridges that connect multiple collections when the conceptual relation is real.\n"
        "- It is acceptable to create a within-collection bridge only when it synthesizes at least three local clusters into a genuinely broader learning, position, or perspective.\n"
        "- Do not rely on name similarity alone. Use descriptors, local-cluster reflections, and collection context to judge semantic fit.\n"
        "- Global bridge canonicals should be broad, analytically meaningful, and useful as shared conceptual pages across the whole knowledge system.\n"
        "- Each bridge must also include bridge takeaways, bridge tensions, bridge open questions, helpful new sources, and why the bridge matters.\n"
        "- links_to_existing may update an existing bridge's name, descriptor, aliases, and bridge-level synthesis if the pending members materially change it.\n"
        "- New bridges with members from one collection must include at least 4 local clusters. New bridges spanning multiple collections must include at least 3 local clusters.\n"
        "- Complete the full clustering pass before you answer. Return structured JSON only once, at the end.\n"
    )
    user = (
        f"Read the pending global bridge packet from {packet_path}.\n"
        f"Read the existing global bridge memory from {memory_path}.\n"
        "Treat both files as source material for the current bridge-clustering pass.\n"
        "Use links_to_existing to attach pending local clusters to existing bridges by bridge_id.\n"
        "Use new_clusters when pending local clusters should form a new global bridge instead.\n"
        "Only reference collection-local cluster ids that appear in the pending packet.\n"
        "Return final JSON only.\n"
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
    return {
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


def _bridge_threshold_satisfied(member_rows: list[dict]) -> bool:
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
) -> list[dict]:
    deps = _deps()
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
    used_ids = {row["bridge_id"] for row in rows if row.get("bridge_id")}
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
        if not _bridge_threshold_satisfied(member_rows):
            continue
        canonical_name = str(row.get("canonical_name", "")).strip() or str(member_rows[0].get("canonical_name", "")).strip()
        base_slug = str(row.get("slug", "")).strip() or deps.slugify(canonical_name) or "global-bridge"
        bridge_id = str(row.get("bridge_id", "")).strip()
        if not bridge_id:
            slug_counts[base_slug] = slug_counts.get(base_slug, 0) + 1
            slug = base_slug if slug_counts[base_slug] == 1 else f"{base_slug}-{slug_counts[base_slug]}"
            candidate_id = f"global_bridge__{slug}"
            while candidate_id in used_ids:
                slug_counts[base_slug] += 1
                slug = f"{base_slug}-{slug_counts[base_slug]}"
                candidate_id = f"global_bridge__{slug}"
            bridge_id = candidate_id
            used_ids.add(bridge_id)
        else:
            slug = str(row.get("slug", "")).strip() or bridge_id.removeprefix("global_bridge__") or base_slug
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
                "wiki_path": f"wiki/shared/bridge-concepts/{slug}.md",
            }
        )
    return sorted(output, key=lambda row: (row["canonical_name"].casefold(), row["bridge_id"]))


def load_global_bridge_clusters(root: Path) -> list[dict]:
    if not _global_bridge_artifact_path(root).exists():
        return []
    deps = _deps()
    rows = []
    for row in deps._load_jsonl(_global_bridge_artifact_path(root)):
        if not isinstance(row, dict):
            continue
        bridge_id = str(row.get("bridge_id", "")).strip()
        if not bridge_id:
            continue
        rows.append(
            {
                **row,
                "cluster_id": bridge_id,
                "material_ids": [
                    str(material_id).strip()
                    for material_id in deps._safe_list(row.get("supporting_material_ids", []))
                    if str(material_id).strip()
                ],
                "member_local_clusters": [
                    dict(member)
                    for member in _dict_rows(row.get("member_local_clusters", []))
                    if isinstance(member, dict)
                ],
                "source_concepts": [],
                "wiki_path": str(row.get("wiki_path", "")).strip() or f"wiki/shared/bridge-concepts/{str(row.get('slug', '')).strip()}.md",
            }
        )
    return rows


def _global_bridge_due(root: Path, local_clusters: list[dict], collection_refs: list[dict]) -> tuple[bool, str]:
    deps = _deps()
    bundle = _global_bridge_inputs(root, local_clusters, collection_refs)
    if bundle["collection_scope_count"] < 2:
        return False, "fewer than 2 collections"
    if not bundle["packet"].get("pending_local_clusters", []):
        return False, "global bridge unchanged"
    artifact_path = _global_bridge_artifact_path(root)
    stamp = deps._read_stamp(_global_bridge_stamp_path(root))
    if not artifact_path.exists():
        return True, "global bridge artifact missing"
    if not stamp:
        return True, "global bridge stamp missing"
    if str(stamp.get("input_fingerprint", "") or "") == bundle["input_fingerprint"]:
        return False, "global bridge unchanged"
    return True, "pending local clusters or collection context changed"


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
    bundle = _global_bridge_inputs(root, local_clusters, collection_refs)
    if bundle["collection_scope_count"] < 2:
        return {
            "global_bridges": len(bundle["existing_bridges"]),
            "global_bridge_skipped": True,
            "global_bridge_skip_reason": "fewer than 2 collections",
        }
    if not bundle["packet"].get("pending_local_clusters", []):
        return {
            "global_bridges": len(bundle["existing_bridges"]),
            "global_bridge_skipped": True,
            "global_bridge_skip_reason": "global bridge unchanged",
        }

    artifact_path = deps._global_bridge_artifact_path(root)
    stamp_path = deps._global_bridge_stamp_path(root)
    stamp = deps._read_stamp(stamp_path)
    if artifact_path.exists() and str(stamp.get("input_fingerprint", "") or "") == bundle["input_fingerprint"]:
        return {
            "global_bridges": len(deps._load_jsonl(artifact_path)),
            "global_bridge_skipped": True,
            "global_bridge_skip_reason": "global bridge unchanged",
        }

    packet_path = _global_bridge_packet_path(root)
    memory_path = _global_bridge_memory_path(root)
    deps._write_json(packet_path, bundle["packet"])
    deps._write_json(memory_path, bundle["memory"])

    llm_fn = llm_factory("lint")
    system, user = _global_bridge_prompt(packet_path, memory_path)
    succeeded = False
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
        bridges = _finalize_global_bridges(
            bundle["existing_bridges"],
            normalized,
            bundle["packet"].get("pending_local_clusters", []),
            bundle["collection_context_by_key"],
        )
        run_at = datetime.now(timezone.utc).isoformat()
        deps._attach_run_provenance(bridges, route_signature, run_at)
        deps._write_jsonl(artifact_path, bridges)
        deps._write_stamp(
            stamp_path,
            {
                "bridged_at": run_at,
                "input_fingerprint": bundle["input_fingerprint"],
                "local_cluster_fingerprints": bundle["local_cluster_fingerprints"],
                "collection_context_fingerprints": bundle["collection_context_fingerprints"],
                "pending_local_clusters": len(bundle["packet"].get("pending_local_clusters", [])),
                "global_bridge_count": len(bridges),
                "collection_scope_count": bundle["collection_scope_count"],
            },
        )
        deps._write_lint_stage_stamp(root, global_bridge_at=run_at)
        succeeded = True
    finally:
        if succeeded:
            deps._cleanup_paths(packet_path, memory_path)

    return {
        "global_bridges": len(bridges),
        "global_bridge_skipped": False,
        "global_bridge_skip_reason": "",
    }
