from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path


def _read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, separators=(",", ":"), ensure_ascii=False), encoding="utf-8")


def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        data = json.loads(line)
        if isinstance(data, dict):
            rows.append(data)
    return rows


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows)
    if rows:
        text += "\n"
    path.write_text(text, encoding="utf-8")


def _manifest_index(root: Path) -> dict[str, dict]:
    rows = _read_jsonl(root / "manifests" / "materials.jsonl")
    return {
        str(row.get("material_id", "")).strip(): row
        for row in rows
        if str(row.get("material_id", "")).strip()
    }


def _scope_of(row: dict) -> tuple[str, str]:
    domain = str(row.get("domain", "practice") or "practice").strip() or "practice"
    collection = str(row.get("collection", "_general") or "_general").strip() or "_general"
    return domain, collection


def _collection_records(manifest_index: dict[str, dict], domain: str, collection: str) -> list[dict]:
    target = (domain, collection)
    return [row for row in manifest_index.values() if _scope_of(row) == target]


def _determine_scope(manifest_index: dict[str, dict], domain: str | None, collection: str | None) -> tuple[str, str, int]:
    scopes = sorted({_scope_of(row) for row in manifest_index.values() if isinstance(row, dict)})
    if domain or collection:
        if not domain or not collection:
            raise SystemExit("Pass both --domain and --collection for migration.")
        target = (domain.strip() or "practice", collection.strip() or "_general")
        if target not in scopes:
            raise SystemExit(f"Scope not found in manifest: {target[0]}/{target[1]}")
        return target[0], target[1], len(scopes)
    if len(scopes) != 1:
        rendered = ", ".join(f"{item_domain}/{item_collection}" for item_domain, item_collection in scopes)
        raise SystemExit(f"Multiple manifest scopes found: {rendered}. Pass --domain and --collection.")
    return scopes[0][0], scopes[0][1], 1


def _select_bridge_clusters(bridge_clusters: list[dict], manifest_index: dict[str, dict], domain: str, collection: str) -> list[dict]:
    target = (domain, collection)
    selected = []
    for cluster in bridge_clusters:
        cluster_id = str(cluster.get("cluster_id", "")).strip()
        material_ids = [str(mid).strip() for mid in cluster.get("material_ids", []) if str(mid).strip()]
        if not material_ids:
            continue
        unknown = [mid for mid in material_ids if mid not in manifest_index]
        if unknown:
            raise SystemExit(f"Bridge cluster {cluster_id or '<unknown>'} references unknown material ids: {', '.join(sorted(unknown))}")
        member_scopes = {_scope_of(manifest_index[mid]) for mid in material_ids}
        if target in member_scopes and len(member_scopes) > 1:
            rendered = ", ".join(f"{item_domain}/{item_collection}" for item_domain, item_collection in sorted(member_scopes))
            raise SystemExit(f"Bridge cluster {cluster_id} spans multiple scopes ({rendered}); refusing non-deterministic migration.")
        if member_scopes == {target}:
            selected.append(dict(cluster))
    return selected


def _fallback_cluster_fingerprint(clusters: list[dict]):
    from arquimedes.enrich_stamps import canonical_hash

    return canonical_hash([
        {
            "cluster_id": str(cluster.get("cluster_id", "")).strip(),
            "canonical_name": str(cluster.get("canonical_name", "")).strip(),
            "material_ids": sorted(str(mid).strip() for mid in cluster.get("material_ids", []) if str(mid).strip()),
        }
        for cluster in clusters
    ])


def migrate_step1_local_graph(
    project_root: str | Path,
    *,
    domain: str | None = None,
    collection: str | None = None,
    refresh: bool = True,
) -> dict:
    root = Path(project_root).resolve()
    src_path = root / "src"
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))

    from arquimedes.cluster import (
        local_cluster_dir,
        local_cluster_fingerprint,
        local_cluster_path,
        local_cluster_stamp_path,
        normalize_local_clusters,
    )
    from arquimedes.compile import compile_wiki
    from arquimedes.config import load_config
    from arquimedes.lint import (
        _cluster_audit_cluster_fingerprints,
        _collection_reflection_fingerprint,
        _concept_reflection_link_fingerprint,
    )
    from arquimedes.memory import memory_rebuild

    old_cwd = Path.cwd()
    os.chdir(root)
    try:
        manifest_index = _manifest_index(root)
        if not manifest_index:
            raise SystemExit("Manifest is empty; nothing to migrate.")
        domain, collection, scope_count = _determine_scope(manifest_index, domain, collection)
        bridge_clusters = _read_jsonl(root / "derived" / "bridge_concept_clusters.jsonl")
        if not bridge_clusters:
            raise SystemExit("No legacy bridge clusters found at derived/bridge_concept_clusters.jsonl")

        selected = _select_bridge_clusters(bridge_clusters, manifest_index, domain, collection)
        if not selected:
            raise SystemExit(f"No bridge clusters found for {domain}/{collection}")

        local_clusters = normalize_local_clusters(domain, collection, selected)
        cluster_map = {
            str(old.get("cluster_id", "")).strip(): migrated
            for old, migrated in zip(selected, local_clusters)
            if str(old.get("cluster_id", "")).strip()
        }
        local_path = local_cluster_path(root, domain, collection)
        _write_jsonl(local_path, local_clusters)

        bridge_stamp = _read_json(root / "derived" / "bridge_cluster_stamp.json")
        clustered_at = str(bridge_stamp.get("clustered_at", "")).strip()
        route_signature = str(bridge_stamp.get("route_signature", "")).strip()
        fingerprint = ""
        try:
            fingerprint = str(local_cluster_fingerprint(domain, collection, load_config()) or "").strip()
        except Exception:
            fingerprint = ""
        if not fingerprint:
            fingerprint = _fallback_cluster_fingerprint(local_clusters)
        _write_json(
            local_cluster_stamp_path(root, domain, collection),
            {
                "clustered_at": clustered_at,
                "fingerprint": fingerprint,
                "route_signature": route_signature,
                "total_concepts": sum(len(cluster.get("source_concepts", [])) for cluster in local_clusters),
                "clusters": len(local_clusters),
                "domain": domain,
                "collection": collection,
            },
        )

        concept_reflections_path = root / "derived" / "lint" / "concept_reflections.jsonl"
        concept_reflections = _read_jsonl(concept_reflections_path)
        concept_rows = {}
        for row in concept_reflections:
            cluster_id = str(row.get("cluster_id", "")).strip()
            if cluster_id in cluster_map:
                cluster = cluster_map[cluster_id]
                updated = dict(row)
                updated["cluster_id"] = cluster["cluster_id"]
                updated["slug"] = cluster.get("slug", "")
                updated["canonical_name"] = cluster.get("canonical_name", "")
                updated["wiki_path"] = cluster.get("wiki_path", "")
                updated["input_fingerprint"] = _concept_reflection_link_fingerprint(cluster)
                concept_rows[updated["cluster_id"]] = updated
            elif cluster_id:
                concept_rows[cluster_id] = dict(row)
        if concept_rows:
            _write_jsonl(concept_reflections_path, [concept_rows[key] for key in sorted(concept_rows)])

        review_fingerprints = _cluster_audit_cluster_fingerprints(local_clusters, route_signature)
        cluster_reviews_path = root / "derived" / "lint" / "cluster_reviews.jsonl"
        cluster_reviews = _read_jsonl(cluster_reviews_path)
        review_rows = {}
        migrated_review_count = 0
        for row in cluster_reviews:
            cluster_id = str(row.get("cluster_id", "")).strip()
            if cluster_id in cluster_map:
                cluster = cluster_map[cluster_id]
                updated = dict(row)
                updated["review_id"] = cluster["cluster_id"]
                updated["cluster_id"] = cluster["cluster_id"]
                updated["wiki_path"] = cluster.get("wiki_path", "")
                updated["input_fingerprint"] = review_fingerprints.get(cluster["cluster_id"], str(updated.get("input_fingerprint", "")).strip())
                review_rows[updated["cluster_id"]] = updated
                migrated_review_count += 1
            elif cluster_id:
                review_rows[cluster_id] = dict(row)
        if review_rows:
            _write_jsonl(cluster_reviews_path, [review_rows[key] for key in sorted(review_rows)])

        collection_reflections_path = root / "derived" / "lint" / "collection_reflections.jsonl"
        collection_reflections = _read_jsonl(collection_reflections_path)
        collection_rows = {}
        target_key = f"{domain}/{collection}"
        target_metas = _collection_records(manifest_index, domain, collection)
        target_fingerprint = _collection_reflection_fingerprint(domain, collection, target_metas, local_clusters)
        for row in collection_reflections:
            collection_key = str(row.get("collection_key", "")).strip()
            if collection_key == target_key:
                updated = dict(row)
                updated["collection_key"] = target_key
                updated["domain"] = domain
                updated["collection"] = collection
                updated["important_cluster_ids"] = sorted({
                    cluster_map[str(cluster_id).strip()]["cluster_id"]
                    for cluster_id in row.get("important_cluster_ids", [])
                    if str(cluster_id).strip() in cluster_map
                })
                updated["input_fingerprint"] = target_fingerprint
                updated["wiki_path"] = f"wiki/{domain}/{collection}/_index.md"
                collection_rows[target_key] = updated
            elif collection_key:
                collection_rows[collection_key] = dict(row)
        if collection_rows:
            _write_jsonl(collection_reflections_path, [collection_rows[key] for key in sorted(collection_rows)])

        lint_stamp = _read_json(root / "derived" / "lint" / "lint_stamp.json")
        if scope_count == 1:
            audit_state = _read_json(root / "derived" / "lint" / "cluster_audit_state.json")
            if audit_state:
                _write_json(
                    local_cluster_dir(root, domain, collection) / "local_audit_state.json",
                    {
                        "pending_local_fingerprint": str(audit_state.get("pending_local_fingerprint", "")).strip(),
                        "pending_local_concepts": int(audit_state.get("pending_local_concepts", 0) or 0),
                    },
                )
        audited_at = str(lint_stamp.get("audited_at", "")).strip()
        if audited_at and migrated_review_count:
            _write_json(
                local_cluster_dir(root, domain, collection) / "local_audit_stamp.json",
                {
                    "audited_at": audited_at,
                    "cluster_reviews": migrated_review_count,
                },
            )

        if refresh:
            config = load_config()
            compile_wiki(config, skip_cluster=True, run_quick_lint=False)
            memory_rebuild(config)

        return {
            "domain": domain,
            "collection": collection,
            "local_clusters": len(local_clusters),
            "concept_reflections": len([key for key in concept_rows if key.startswith(f"{domain}__{collection}__local_")]),
            "cluster_reviews": migrated_review_count,
            "collection_reflections": int(target_key in collection_rows),
            "refresh": refresh,
        }
    finally:
        os.chdir(old_cwd)


def main() -> None:
    parser = argparse.ArgumentParser(description="One-time deterministic migration from legacy bridge artifacts into Step 1 local clusters.")
    parser.add_argument("--project-root", default=".", help="Repo root to migrate (default: current directory).")
    parser.add_argument("--domain", help="Explicit domain to migrate when the manifest has multiple scopes.")
    parser.add_argument("--collection", help="Explicit collection to migrate when the manifest has multiple scopes.")
    args = parser.parse_args()
    result = migrate_step1_local_graph(
        args.project_root,
        domain=args.domain,
        collection=args.collection,
        refresh=True,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()