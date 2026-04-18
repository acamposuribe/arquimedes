from __future__ import annotations

import json
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from arquimedes.config import load_config
from arquimedes.llm import EnrichmentError


def _deps():
    import arquimedes.lint as deps

    return deps


def _cluster_audit_due(root: Path, _bridge_clusters: list[dict]) -> tuple[bool, str]:
    deps = _deps()
    local_groups = deps._group_clusters_by_scope(deps._current_concepts(root))
    if local_groups:
        for (domain, collection), scope_clusters in sorted(local_groups.items()):
            if not scope_clusters:
                continue
            cluster_stamp = deps._read_stamp(deps.local_cluster_stamp_path(root, domain, collection))
            clustered_at = deps._parse_iso_datetime(cluster_stamp.get("clustered_at"))
            audit_stamp = deps._read_stamp(deps._local_audit_stamp_path(root, domain, collection))
            audited_at = deps._parse_iso_datetime(audit_stamp.get("audited_at"))
            if audited_at is None:
                return True, f"local audit stamp missing for {domain}/{collection}"
            if clustered_at is None:
                return True, f"local cluster stamp missing for {domain}/{collection}"
            if audited_at < clustered_at:
                return True, f"latest clustering is newer than local audit for {domain}/{collection}"
        return False, "local audit already ran after latest clustering"
    artifact_path = root / deps.LINT_DIR / "cluster_reviews.jsonl"
    return deps._bridge_cluster_stage_due(
        root,
        "audited_at",
        artifact_path,
        "cluster audit",
    )


def _cluster_audit_input_path(root: Path) -> Path:
    deps = _deps()
    return root / deps.LINT_DIR / "cluster_audit_input.json"


def _cluster_audit_raw_response_path(root: Path, phase: str = "final") -> Path:
    deps = _deps()
    suffix = phase.strip() or "final"
    return root / deps.LINT_DIR / f"cluster_audit_last_response.{suffix}.txt"


def _cluster_audit_parsed_response_path(root: Path) -> Path:
    deps = _deps()
    return root / deps.LINT_DIR / "cluster_audit_last_response.parsed.json"


def _cleanup_cluster_audit_debug_artifacts(root: Path) -> None:
    for path in (
        _cluster_audit_raw_response_path(root, "initial"),
        _cluster_audit_raw_response_path(root, "final"),
        _cluster_audit_parsed_response_path(root),
    ):
        try:
            path.unlink(missing_ok=True)
        except OSError:
            continue


def _cluster_audit_input_paths(root: Path) -> tuple[Path, Path]:
    tmp_root = root / "derived" / "tmp"
    return (
        tmp_root / "bridge_concept_clusters.audit.input.jsonl",
        tmp_root / "cluster_reviews.audit.input.jsonl",
    )


def _cluster_audit_lock_pid(gate_path: Path) -> int | None:
    try:
        raw = gate_path.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    if not raw:
        return None
    try:
        pid = int(raw)
    except ValueError:
        return None
    return pid if pid > 0 else None


def _cluster_audit_pid_is_running(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _acquire_cluster_audit_gate(gate_path: Path) -> bool:
    while True:
        try:
            gate_fd = os.open(gate_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            pid = _cluster_audit_lock_pid(gate_path)
            if pid is not None and _cluster_audit_pid_is_running(pid):
                return False
            try:
                gate_path.unlink()
            except FileNotFoundError:
                continue
            except OSError:
                return False
            continue
        try:
            os.write(gate_fd, f"{os.getpid()}\n".encode())
        finally:
            os.close(gate_fd)
        return True


def _cluster_audit_cluster_snapshot(cluster: dict) -> dict:
    deps = _deps()
    return {
        "cluster_id": str(cluster.get("cluster_id", "")).strip(),
        "canonical_name": str(cluster.get("canonical_name", "")).strip(),
        "aliases": deps._safe_list(cluster.get("aliases", [])),
        "material_ids": deps._safe_list(cluster.get("material_ids", [])),
        "source_concepts": [
            {
                "material_id": str(source.get("material_id", "")).strip(),
                "concept_name": str(source.get("concept_name", "")).strip(),
                "concept_key": str(source.get("concept_key", "")).strip() or deps._normalize_concept_name(str(source.get("concept_name", "")).strip()),
            }
            for source in cluster.get("source_concepts", [])
            if isinstance(source, dict)
        ],
    }


def _cluster_audit_cluster_fingerprint(cluster: dict, route_signature: str) -> str:
    deps = _deps()
    del route_signature
    return deps.canonical_hash({
        "material_ids": sorted({
            str(material_id).strip()
            for material_id in cluster.get("material_ids", [])
            if str(material_id).strip()
        }),
    })


def _cluster_audit_cluster_fingerprints(clusters: list[dict], route_signature: str) -> dict[str, str]:
    fingerprints: dict[str, str] = {}
    for cluster in clusters:
        cluster_id = str(cluster.get("cluster_id", "")).strip()
        if not cluster_id:
            continue
        fingerprints[cluster_id] = _cluster_audit_cluster_fingerprint(cluster, route_signature)
    return fingerprints


def _cluster_audit_pending_local_fingerprint(local_rows: list[tuple], route_signature: str) -> str:
    deps = _deps()
    pending_local = []
    for row in local_rows:
        concept_name, concept_key, material_id, relevance, source_pages, evidence_spans, confidence, concept_type, descriptor = deps._split_concept_row(row)
        pending_local.append({
            "material_id": material_id,
            "concept_name": concept_name,
            "concept_key": concept_key,
            "relevance": relevance,
            "source_pages": source_pages,
            "evidence_spans": evidence_spans,
            "confidence": confidence,
            "concept_type": concept_type,
            "descriptor": descriptor,
        })
    pending_local.sort(key=lambda item: (item["material_id"], item["concept_key"], item["concept_name"]))
    return deps.canonical_hash({
        "route_signature": route_signature,
        "pending_local_concepts": pending_local,
    })


def _cluster_audit_target_clusters(
    clusters: list[dict],
    canonical_reviews: dict[str, dict],
    route_signature: str,
) -> tuple[list[dict], dict[str, str]]:
    cluster_fingerprints = _cluster_audit_cluster_fingerprints(clusters, route_signature)
    targets = []
    for cluster in sorted(clusters, key=lambda item: str(item.get("cluster_id", ""))):
        cluster_id = str(cluster.get("cluster_id", "")).strip()
        if not cluster_id:
            continue
        review = canonical_reviews.get(cluster_id)
        reasons: list[str] = []
        if review is None:
            reasons.append("missing_review")
        else:
            if str(review.get("status", "")).strip().lower() == "open":
                reasons.append("open_review")
            if str(review.get("input_fingerprint", "")).strip() != cluster_fingerprints.get(cluster_id, ""):
                reasons.append("changed_since_last_audit")
        if reasons:
            targets.append({
                "cluster_id": cluster_id,
                "canonical_name": str(cluster.get("canonical_name", "")).strip(),
                "material_ids": _deps()._safe_list(cluster.get("material_ids", [])),
                "current_review_status": str(review.get("status", "")).strip() if isinstance(review, dict) else "",
                "reasons": reasons,
            })
    return targets, cluster_fingerprints


def _cluster_audit_finalize_reviews(
    reviews: list[dict],
    cluster_fingerprints: dict[str, str],
    *,
    context_requested: bool | None = None,
    context_request_count: int | None = None,
) -> list[dict]:
    finalized = []
    for row in sorted(reviews, key=lambda item: str(item.get("cluster_id", ""))):
        if not isinstance(row, dict):
            continue
        cluster_id = str(row.get("cluster_id", "")).strip()
        if not cluster_id:
            continue
        normalized = dict(row)
        normalized["review_id"] = _cluster_audit_review_id(cluster_id)
        normalized["input_fingerprint"] = cluster_fingerprints.get(cluster_id, str(normalized.get("input_fingerprint", "")).strip())
        if context_requested is not None:
            normalized["context_requested"] = context_requested
        else:
            normalized["context_requested"] = bool(normalized.get("context_requested", False))
        if context_request_count is not None:
            normalized["context_request_count"] = context_request_count
        else:
            normalized["context_request_count"] = int(normalized.get("context_request_count", 0) or 0)
        finalized.append(normalized)
    return finalized


def _cluster_audit_mutable_concept_index(
    local_rows: list[tuple],
) -> dict[tuple[str, str], dict]:
    deps = _deps()
    concept_index: dict[tuple[str, str], dict] = {}
    for row in local_rows:
        concept_name, concept_key, material_id, relevance, source_pages, evidence_spans, confidence, concept_type, descriptor = deps._split_concept_row(row)
        concept_index[(material_id, concept_key)] = {
            "concept_name": concept_name,
            "concept_key": concept_key,
            "material_id": material_id,
            "relevance": relevance,
            "source_pages": source_pages,
            "evidence_spans": evidence_spans,
            "confidence": confidence,
            "concept_type": concept_type,
            "descriptor": descriptor,
        }
    return concept_index


def _audit_optional_text(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _cluster_audit_is_nonfatal_bridge_update_error(exc: Exception) -> bool:
    return "must leave the bridge connected to at least two distinct materials" in str(exc)


def _cluster_audit_skipped_bridge_update_review_fields() -> dict[str, str]:
    return {
        "finding_type": "validation_error",
        "severity": "medium",
        "status": "open",
        "note": "A proposed bridge update was ignored because it would have left the cluster connected to fewer than two distinct materials.",
        "recommendation": "Keep the current cluster unchanged and only retry the refinement with a cross-material update that preserves at least two distinct materials.",
    }


def _cluster_audit_validate_bridge_candidate(
    candidate: dict,
    concept_index: dict[tuple[str, str], dict],
    assigned_pairs: set[tuple[str, str]],
    *,
    label: str,
) -> dict | None:
    deps = _deps()
    if not isinstance(candidate, dict):
        raise EnrichmentError(f"{label} must be an object")
    canonical_name = _audit_optional_text(candidate.get("canonical_name", ""))
    if not canonical_name:
        raise EnrichmentError(f"{label} is missing canonical_name")
    aliases = candidate.get("aliases", [])
    if not isinstance(aliases, list):
        raise EnrichmentError(f"{label} field 'aliases' must be a list")
    raw_sources = candidate.get("source_concepts", [])
    if not isinstance(raw_sources, list):
        raise EnrichmentError(f"{label} field 'source_concepts' must be a list")

    validated_source = []
    for idx, entry in enumerate(raw_sources, start=1):
        if not isinstance(entry, dict):
            raise EnrichmentError(f"{label}.source_concepts[{idx}] must be an object")
        material_id = str(entry.get("material_id", "")).strip()
        concept_name = str(entry.get("concept_name", "")).strip()
        if not material_id or not concept_name:
            raise EnrichmentError(f"{label}.source_concepts[{idx}] must include material_id and concept_name")
        indexed = deps._resolve_concept_reference(material_id, concept_name, concept_index)
        if indexed is None:
            continue
        validated_source.append(deps._build_source_concept(indexed))

    deduped_source = []
    seen_local: set[tuple[str, str]] = set()
    for source in validated_source:
        key = (source["material_id"], source["concept_key"])
        if key in seen_local:
            continue
        seen_local.add(key)
        deduped_source.append(source)

    candidate_source = []
    candidate_pairs: list[tuple[str, str]] = []
    for source in deduped_source:
        key = (source["material_id"], source["concept_key"])
        if key in assigned_pairs:
            continue
        candidate_source.append(source)
        candidate_pairs.append(key)

    material_ids = list(dict.fromkeys(source["material_id"] for source in candidate_source))
    claimed_material_ids = deps._dedupe_strings(deps._safe_list(candidate.get("material_ids", [])))
    if claimed_material_ids and claimed_material_ids != material_ids:
        raise EnrichmentError(f"{label} field 'material_ids' must match the source_concepts material ids")
    if len(material_ids) < 2:
        return None

    assigned_pairs.update(candidate_pairs)
    alias_values = deps._dedupe_strings([
        canonical_name,
        *[str(value).strip() for value in aliases if str(value).strip()],
        *[source.get("concept_name", "") for source in candidate_source if source.get("concept_name", "")],
    ])
    return {
        "canonical_name": canonical_name,
        "slug": deps.slugify(canonical_name),
        "aliases": alias_values,
        "material_ids": material_ids,
        "source_concepts": [
            {
                "material_id": source["material_id"],
                "concept_name": source["concept_name"],
                "descriptor": source.get("descriptor", ""),
                "relevance": source["relevance"],
                "source_pages": source["source_pages"],
                "evidence_spans": source["evidence_spans"],
                "confidence": source["confidence"],
            }
            for source in candidate_source
        ],
        "confidence": deps._derive_cluster_confidence(candidate_source),
    }


def _cluster_audit_apply_bridge_update(
    candidate: dict,
    existing_cluster: dict,
    concept_index: dict[tuple[str, str], dict],
    assigned_pairs: set[tuple[str, str]],
    *,
    label: str,
) -> dict:
    deps = _deps()
    if not isinstance(candidate, dict):
        raise EnrichmentError(f"{label} must be an object")

    if "new_aliases" in candidate and not isinstance(candidate.get("new_aliases"), list):
        raise EnrichmentError(f"{label} field 'new_aliases' must be a list when provided")
    if "new_materials" in candidate and not isinstance(candidate.get("new_materials"), list):
        raise EnrichmentError(f"{label} field 'new_materials' must be a list when provided")
    if "removed_materials" in candidate and not isinstance(candidate.get("removed_materials"), list):
        raise EnrichmentError(f"{label} field 'removed_materials' must be a list when provided")
    raw_new_sources = candidate.get("new_source_concepts", []) or []
    if not isinstance(raw_new_sources, list):
        raise EnrichmentError(f"{label} field 'new_source_concepts' must be a list when provided")

    removed_materials = set(deps._dedupe_strings(deps._safe_list(candidate.get("removed_materials", []))))
    retained_sources = []
    retained_pairs: set[tuple[str, str]] = set()
    for source in existing_cluster.get("source_concepts", []):
        if not isinstance(source, dict):
            continue
        material_id = str(source.get("material_id", "")).strip()
        concept_name = str(source.get("concept_name", "")).strip()
        concept_key = str(source.get("concept_key", "")).strip() or deps._normalize_concept_name(concept_name)
        if not material_id or not concept_name or material_id in removed_materials:
            continue
        retained_pairs.add((material_id, concept_key))
        retained_sources.append({
            "material_id": material_id,
            "concept_name": concept_name,
            "descriptor": str(source.get("descriptor", "")).strip(),
            "relevance": str(source.get("relevance", "")).strip(),
            "source_pages": source.get("source_pages", []),
            "evidence_spans": source.get("evidence_spans", []),
            "confidence": float(source.get("confidence", 0.0) or 0.0),
            "concept_key": concept_key,
        })

    seen_pairs = assigned_pairs | retained_pairs
    added_sources = []
    added_pairs: set[tuple[str, str]] = set()
    for idx, entry in enumerate(raw_new_sources, start=1):
        if not isinstance(entry, dict):
            raise EnrichmentError(f"{label}.new_source_concepts[{idx}] must be an object")
        material_id = str(entry.get("material_id", "")).strip()
        concept_name = str(entry.get("concept_name", "")).strip()
        if not material_id or not concept_name:
            raise EnrichmentError(f"{label}.new_source_concepts[{idx}] must include material_id and concept_name")
        indexed = deps._resolve_concept_reference(material_id, concept_name, concept_index)
        if indexed is None:
            continue
        normalized = deps._build_source_concept(indexed)
        pair = (normalized["material_id"], normalized["concept_key"])
        if pair in seen_pairs or pair in added_pairs:
            continue
        added_pairs.add(pair)
        added_sources.append(normalized)

    final_sources = [*retained_sources, *added_sources]
    final_material_ids = list(dict.fromkeys(source["material_id"] for source in final_sources))
    if len(final_material_ids) < 2:
        raise EnrichmentError(f"{label} must leave the bridge connected to at least two distinct materials")

    assigned_pairs.update(retained_pairs)
    assigned_pairs.update(added_pairs)

    canonical_name = _audit_optional_text(candidate.get("new_name", "")) or _audit_optional_text(existing_cluster.get("canonical_name", ""))
    if not canonical_name:
        raise EnrichmentError(f"{label} produced an empty canonical name")
    alias_values = candidate.get("new_aliases") if "new_aliases" in candidate else existing_cluster.get("aliases", [])
    aliases = deps._dedupe_strings([canonical_name, *[str(value).strip() for value in (alias_values or []) if str(value).strip()]])

    return {
        "cluster_id": str(existing_cluster.get("cluster_id", "")).strip(),
        "canonical_name": canonical_name,
        "slug": deps.slugify(canonical_name),
        "aliases": aliases,
        "material_ids": final_material_ids,
        "source_concepts": [
            {
                "material_id": source["material_id"],
                "concept_name": source["concept_name"],
                "descriptor": source.get("descriptor", ""),
                "relevance": source["relevance"],
                "source_pages": source["source_pages"],
                "evidence_spans": source["evidence_spans"],
                "confidence": source["confidence"],
            }
            for source in final_sources
        ],
        "confidence": deps._derive_cluster_confidence(final_sources, fallback=float(existing_cluster.get("confidence", 0.0) or 0.0)),
    }


def _cluster_audit_review_id(cluster_id: str) -> str:
    return cluster_id


def _cluster_audit_existing_review_key(row: dict, position: int) -> tuple[float, int]:
    deps = _deps()
    provenance = row.get("_provenance", {}) if isinstance(row.get("_provenance", {}), dict) else {}
    run_at = deps._parse_iso_datetime(provenance.get("run_at"))
    return (run_at.timestamp() if run_at else float("-inf"), position)


def _cluster_audit_review_ref(review: dict) -> str:
    if not isinstance(review, dict):
        return ""
    return (
        str(review.get("cluster_ref", "")).strip()
        or str(review.get("cluster_id", "")).strip()
        or str(review.get("bridge_ref", "")).strip()
    )


def _cluster_audit_canonicalize_existing_reviews(
    existing_rows: list[dict],
    cluster_refs: dict[str, dict],
) -> dict[str, dict]:
    canonical: dict[str, tuple[tuple[float, int], dict]] = {}
    for position, row in enumerate(existing_rows):
        if not isinstance(row, dict):
            continue
        cluster_id = str(row.get("cluster_id", "")).strip()
        if not cluster_id or cluster_id not in cluster_refs:
            continue
        score = _cluster_audit_existing_review_key(row, position)
        current = canonical.get(cluster_id)
        if current is None or score >= current[0]:
            normalized = _cluster_audit_normalize_review_row(
                row,
                cluster_refs,
                review_id=_cluster_audit_review_id(cluster_id),
                default_cluster_ref=cluster_id,
            )
            for key in ("input_fingerprint", "context_requested", "context_request_count"):
                if key in row:
                    normalized[key] = row.get(key)
            provenance = row.get("_provenance")
            if isinstance(provenance, dict):
                normalized["_provenance"] = dict(provenance)
            canonical[cluster_id] = (score, normalized)
    return {cluster_id: row for cluster_id, (_score, row) in canonical.items()}


def _cluster_audit_normalize_review_row(
    review: dict,
    cluster_refs: dict[str, dict],
    *,
    review_id: str,
    default_cluster_ref: str = "",
) -> dict:
    deps = _deps()
    if not isinstance(review, dict):
        raise EnrichmentError("Cluster audit review entries must be objects")
    cluster_ref = _cluster_audit_review_ref(review) or default_cluster_ref
    if not cluster_ref:
        raise EnrichmentError(f"Cluster audit review '{review_id}' is missing cluster_ref/cluster_id")
    target_cluster = cluster_refs.get(cluster_ref)
    if not isinstance(target_cluster, dict):
        raise EnrichmentError(f"Cluster audit review '{review_id}' references unknown bridge '{cluster_ref}'")
    finding_type = str(review.get("finding_type", "")).strip()
    severity = str(review.get("severity", "")).strip().lower()
    status = str(review.get("status", "")).strip().lower()
    if status == "resolved":
        status = "validated"
    if status == "improved":
        status = "open"
    note = str(review.get("note", "")).strip()
    recommendation = str(review.get("recommendation", "")).strip()
    if not finding_type:
        raise EnrichmentError(f"Cluster audit review '{review_id}' is missing finding_type")
    if severity not in {"high", "medium", "low"}:
        raise EnrichmentError(f"Cluster audit review '{review_id}' has invalid severity '{severity}'")
    valid_statuses = {"open", "validated"}
    if status not in valid_statuses:
        raise EnrichmentError(f"Cluster audit review '{review_id}' has invalid status '{status}'")
    if not note:
        raise EnrichmentError(f"Cluster audit review '{review_id}' is missing note")
    if not recommendation:
        raise EnrichmentError(f"Cluster audit review '{review_id}' is missing recommendation")
    return {
        "review_id": review_id,
        "cluster_id": str(target_cluster.get("cluster_id", "")).strip(),
        "finding_type": finding_type,
        "severity": severity,
        "status": status,
        "note": note,
        "recommendation": recommendation,
        "wiki_path": str(deps._concept_page_path(target_cluster)),
    }


def _cluster_audit_apply_review_delta(
    existing_rows: list[dict],
    review_updates: list[dict],
    new_reviews: list[dict],
    cluster_refs: dict[str, dict],
) -> tuple[list[dict], set[str]]:
    rows_by_cluster = _cluster_audit_canonicalize_existing_reviews(existing_rows, cluster_refs)
    touched_clusters: set[str] = set()

    for idx, review in enumerate(review_updates, start=1):
        cluster_id = _cluster_audit_review_ref(review)
        if not cluster_id:
            raise EnrichmentError(f"review_updates[{idx}] is missing cluster_id")
        existing = rows_by_cluster.get(cluster_id)
        if existing is None:
            raise EnrichmentError(f"review_updates[{idx}] references bridge '{cluster_id}' without an existing canonical review row; use new_reviews instead")
        if cluster_id in touched_clusters:
            raise EnrichmentError(f"review_updates[{idx}] duplicates cluster_id '{cluster_id}'")
        normalized = _cluster_audit_normalize_review_row(
            review,
            cluster_refs,
            review_id=_cluster_audit_review_id(cluster_id),
            default_cluster_ref=cluster_id,
        )
        rows_by_cluster[cluster_id] = {
            **existing,
            **normalized,
        }
        touched_clusters.add(cluster_id)

    for idx, review in enumerate(new_reviews, start=1):
        cluster_ref = _cluster_audit_review_ref(review)
        if not cluster_ref:
            raise EnrichmentError(f"new_reviews[{idx}] is missing cluster_ref")
        target_cluster = cluster_refs.get(cluster_ref)
        if not isinstance(target_cluster, dict):
            raise EnrichmentError(f"new_reviews[{idx}] references unknown bridge '{cluster_ref}'")
        canonical_cluster_id = str(target_cluster.get("cluster_id", "")).strip()
        if canonical_cluster_id in rows_by_cluster:
            raise EnrichmentError(f"new_reviews[{idx}] references bridge '{canonical_cluster_id}' which already has a canonical review row; use review_updates instead")
        if canonical_cluster_id in touched_clusters:
            raise EnrichmentError(f"new_reviews[{idx}] duplicates cluster_id '{canonical_cluster_id}'")
        normalized = _cluster_audit_normalize_review_row(
            review,
            cluster_refs,
            review_id=_cluster_audit_review_id(canonical_cluster_id),
            default_cluster_ref=cluster_ref,
        )
        rows_by_cluster[canonical_cluster_id] = normalized
        touched_clusters.add(canonical_cluster_id)

    return [rows_by_cluster[key] for key in sorted(rows_by_cluster)], touched_clusters


def _ensure_cluster_audit_review_coverage(
    reviews: list[dict],
    bridge_clusters: list[dict],
) -> list[dict]:
    deps = _deps()
    rows = [dict(row) for row in reviews if isinstance(row, dict)]
    covered_clusters = {
        str(row.get("cluster_id", "")).strip()
        for row in rows
        if str(row.get("cluster_id", "")).strip()
    }
    for cluster in bridge_clusters:
        cluster_id = str(cluster.get("cluster_id", "")).strip()
        if not cluster_id or cluster_id in covered_clusters:
            continue
        rows.append({
            "review_id": _cluster_audit_review_id(cluster_id),
            "cluster_id": cluster_id,
            "finding_type": "validated",
            "severity": "low",
            "status": "validated",
            "note": "Current bridge remains coherent in this audit pass.",
            "recommendation": "Keep the current bridge as is unless stronger new evidence appears.",
            "wiki_path": str(deps._concept_page_path(cluster)),
        })
        covered_clusters.add(cluster_id)
    return rows


def _cluster_audit_prompt(root: Path, input_path: Path, bridge_input_path: Path, reviews_input_path: Path) -> tuple[str, str]:
    deps = _deps()
    system = (
        "You are an architecture research librarian auditing the bridge concept graph.\n"
        "\n"
        "You will receive exactly three read-only inputs: the uncovered local bridge packet, the staged bridge memory file with the existing clusters under review, and the current cluster-review audit log. "
        "If you need more context for your decisions, do not guess. Return a JSON object that includes a context_requests array with up to 4 read-only SQL-index lookups. "
        "Use context requests only for targeted material evidence queries or a collection open_record."
        " Each request should look like {\"tool\":\"search_material_evidence\",\"kind\":\"chunk|annotation|figure\",\"material_id\":\"...\",\"query\":\"...\",\"limit\":5} "
        "or {\"tool\":\"open_record\",\"kind\":\"collection\",\"id\":\"...\"}. "
        "You will get only one read-only context round, so request everything you need at once.\n"
        "\n"
        f"Return exactly one final JSON object matching this schema: {deps._CLUSTER_AUDIT_DELTA_SCHEMA}\n"
        "- Use bridge_updates for existing clusters in the staged bridge memory file. You may rename if strictly necessary, replace aliases, attach uncovered local concepts and remove materials that no longer belong.\n"
        "- Use new_bridges for genuinely new cross-material bridges built from the uncovered local concepts in the bridge packet.\n"
        "- Every new_bridges entry that you keep must have exactly one matching new_reviews row using the same temporary bridge_ref. If you decide a candidate is not a real bridge, omit both that new_bridges entry and its review row.\n"
        "- new_source_concepts is the authoritative way to attach uncovered local concepts to a reviewed bridge. If you include new_materials, it is only a convenience hint; the pipeline will derive the actual added materials from new_source_concepts.\n"
        "The cluster_reviews file is an audit log for the next round and contains only the current review rows for the staged bridge clusters under review: what changed, why it changed, what still seems doubtful, and what is validated for now. There must be exactly one canonical audit-log row per bridge cluster when the audit is done. Status must always be open or validated.\n"
        "- Use review_updates for clusters that already have a canonical audit-log row, keyed by cluster_id.\n"
        "- Use new_reviews for clusters that do not yet have a canonical audit-log row. When the row is for a newly proposed bridge, cluster_ref must repeat the exact bridge_ref from new_bridges.\n"
        "There must be exactly one canonical audit-log row per bridge cluster, and review status must be open or validated.\n\n"
        "ABOUT CLUSTER NAMES: Bridge concepts are ambitious cross-material ideas. "
        "Only change existing names for clear improvement, and only if strictly necessary."
        "For new clustrers: Cluster names may be theoretically dense and multi-word. Avoid near-duplicate concepts, incidental topics, and generic labels like history, power, space, or memory unless sharply qualified. Prefer cluster names that carry analytical charge and group local and bridge concepts together, like spatial justice, racial capitalism, architecture as care, counter-mapping methods, or collecting as spatial practice, and many others. IMP: Avoid academic jargon, theoretical buzzwords, or pretentious language. Use clear, direct, and specific language that conveys real analytical meaning.\n\n"
        "Set _finished to true only in the final completed JSON object. Return JSON only.\n"
        "\n"
        "## TODO\n"
        "- [ ] Read the existing review rows and recommendations.\n"
        "- [ ] Audit the staged bridge-memory clusters: improve names or aliases when strictly necessary, attach uncovered local concepts when they clearly belong, and remove materials that clearly do not belong.\n"
        "- [ ] Create genuinely new bridges only for uncovered local concepts that do not fit one of the reviewed bridges.\n"
        "- [ ] Return explicit bridge and review deltas in the final JSON object.\n"
        "- [ ] Finish only when the final JSON object is complete and _finished is true.\n"
    )
    user = (
        f"Read these files:\n"
        f"- {input_path}\n"
        "- If the packet points to a bridge packet file, read that too.\n"
        f"- {reviews_input_path}\n"
        f"- {bridge_input_path}\n"
        "\n"
        "The bridge input file is JSONL and contains only the existing bridge clusters you are allowed to review. Treat it as read-only input.\n"
        "The cluster_reviews input file contains only the current audit rows for the staged bridge clusters under review. Treat it as read-only input.\n"
        "Do not invent concepts that are not present in the bridge packet.\n"
        "Return final JSON only.\n"
    )
    return system, user


def _next_bridge_cluster_index(clusters: list[dict]) -> int:
    max_idx = 0
    for cluster in clusters:
        cid = str(cluster.get("cluster_id", "")).strip()
        match = re.fullmatch(r"bridge_(\d{4})", cid)
        if match:
            max_idx = max(max_idx, int(match.group(1)))
    return max_idx + 1


def _assign_new_bridge_ids(clusters: list[dict], existing_clusters: list[dict]) -> list[dict]:
    existing_ids = {str(cluster.get("cluster_id", "")).strip() for cluster in existing_clusters if cluster.get("cluster_id", "")}
    next_idx = _next_bridge_cluster_index(existing_clusters)
    reassigned: list[dict] = []
    used_ids = set(existing_ids)
    for cluster in clusters:
        cid = str(cluster.get("cluster_id", "")).strip()
        if cid and cid not in used_ids and re.fullmatch(r"bridge_\d{4}", cid):
            reassigned.append(cluster)
            used_ids.add(cid)
            continue
        while f"bridge_{next_idx:04d}" in used_ids:
            next_idx += 1
        new_id = f"bridge_{next_idx:04d}"
        used_ids.add(new_id)
        next_idx += 1
        reassigned.append({
            **cluster,
            "cluster_id": new_id,
            "wiki_path": f"wiki/shared/bridge-concepts/{cluster.get('slug', '')}.md" if cluster.get("slug", "") else "",
        })
    return reassigned


def _cover_pairs_from_clusters(clusters: list[dict]) -> set[tuple[str, str]]:
    covered: set[tuple[str, str]] = set()
    for cluster in clusters:
        for source in cluster.get("source_concepts", []) or []:
            if not isinstance(source, dict):
                continue
            material_id = str(source.get("material_id", "")).strip()
            concept_name = str(source.get("concept_name", "")).strip()
            concept_key = str(source.get("concept_key", "")).strip() or concept_name.lower().strip()
            if material_id and concept_key:
                covered.add((material_id, concept_key))
    return covered


def _run_cluster_audit_impl(
    deps: Any,
    root: Path,
    clusters: list[dict],
    material_info: dict[str, dict],
    route_signature: str = "",
    llm_factory=None,
    tool=None,
) -> tuple[list[dict], int]:
    if deps._clusters_are_local(clusters):
        return deps._run_local_cluster_audit(root, clusters, material_info, route_signature, llm_factory, tool)

    existing_path = root / deps.LINT_DIR / "cluster_reviews.jsonl"
    existing_rows = deps._load_jsonl(existing_path)
    local_rows, material_rows = deps._local_rows_not_in_bridge(root, clusters)
    existing_cluster_by_id = {
        str(cluster.get("cluster_id", "")).strip(): cluster
        for cluster in clusters
        if str(cluster.get("cluster_id", "")).strip()
    }
    canonical_existing_reviews = deps._cluster_audit_canonicalize_existing_reviews(existing_rows, existing_cluster_by_id)
    target_clusters, cluster_fingerprints = deps._cluster_audit_target_clusters(clusters, canonical_existing_reviews, route_signature)
    target_cluster_ids = {
        str(target.get("cluster_id", "")).strip()
        for target in target_clusters
        if str(target.get("cluster_id", "")).strip()
    }
    current_pending_local_fingerprint = deps._cluster_audit_pending_local_fingerprint(local_rows, route_signature)
    audit_state = deps._read_stamp(root / deps.CLUSTER_AUDIT_STATE_PATH)
    prior_pending_local_fingerprint = str(audit_state.get("pending_local_fingerprint", "") or "")
    pending_local_changed = bool(local_rows and material_rows) and current_pending_local_fingerprint != prior_pending_local_fingerprint

    normalized_reviews = [canonical_existing_reviews[key] for key in sorted(canonical_existing_reviews)]
    normalized_reviews = deps._ensure_cluster_audit_review_coverage(normalized_reviews, clusters)
    normalized_reviews = deps._cluster_audit_finalize_reviews(normalized_reviews, cluster_fingerprints)

    if not target_cluster_ids and not pending_local_changed:
        run_at = datetime.now(timezone.utc).isoformat()
        reviews_changed = normalized_reviews != existing_rows
        if reviews_changed:
            deps._attach_run_provenance(normalized_reviews, route_signature, run_at)
            deps._write_jsonl(existing_path, normalized_reviews)
        deps._write_lint_stage_stamp(root, audited_at=run_at)
        deps._write_stamp(
            root / deps.CLUSTER_AUDIT_STATE_PATH,
            {
                "pending_local_fingerprint": current_pending_local_fingerprint,
                "pending_local_concepts": len(local_rows),
            },
        )


        deps._cleanup_cluster_audit_debug_artifacts(root)
        return normalized_reviews, 0

    bridge_packets_path = None
    if local_rows and material_rows:
        bridge_packets_path = deps._stage_bridge_packet_input(
            root,
            local_rows,
            material_rows,
            max_local_concepts_per_material=None,
            max_bridge_candidates_per_material=None,
            max_evidence_snippets_per_material=None,
        )

    bridge_input_path, reviews_input_path = deps._cluster_audit_input_paths(root)
    reviewable_clusters = [
        cluster
        for cluster in clusters
        if str(cluster.get("cluster_id", "")).strip() in target_cluster_ids
    ]
    packet = {
        "bridge_memory": str(bridge_input_path),
        "cluster_reviews": str(reviews_input_path),
        "bridge_packets": str(bridge_packets_path) if bridge_packets_path else "",
    }
    input_path = deps._cluster_audit_input_path(root)

    deps._write_json(input_path, packet)
    deps._write_jsonl(bridge_input_path, reviewable_clusters)
    deps._stage_cluster_audit_reviews_input(reviews_input_path, canonical_existing_reviews, target_cluster_ids)
    llm_fn = llm_factory("lint")
    system, user = deps._cluster_audit_prompt(root, input_path, bridge_input_path, reviews_input_path)

    def _record_cluster_audit_raw_response(raw_text: str, phase: str) -> None:
        path = deps._cluster_audit_raw_response_path(root, phase)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(str(raw_text), encoding="utf-8")

    parsed = deps._run_reflection_prompt_with_context(
        llm_fn,
        system,
        user,
        deps._CLUSTER_AUDIT_DELTA_SCHEMA,
        tool,
        raw_response_recorder=_record_cluster_audit_raw_response,
    )
    deps._write_json(deps._cluster_audit_parsed_response_path(root), parsed if isinstance(parsed, dict) else {"raw": parsed})

    if not isinstance(parsed, dict):
        raise EnrichmentError("Cluster audit output must be a JSON object")
    if parsed.get("_finished") is not True:
        raise EnrichmentError("Cluster audit output missing _finished=true")
    parsed.pop("_finished", None)

    bridge_updates = parsed.get("bridge_updates", [])
    new_bridges = parsed.get("new_bridges", [])
    review_updates = parsed.get("review_updates", [])
    new_reviews = parsed.get("new_reviews", [])
    if not isinstance(bridge_updates, list):
        raise EnrichmentError("Cluster audit output field 'bridge_updates' must be a list")
    if not isinstance(new_bridges, list):
        raise EnrichmentError("Cluster audit output field 'new_bridges' must be a list")
    if not isinstance(review_updates, list):
        raise EnrichmentError("Cluster audit output field 'review_updates' must be a list")
    if not isinstance(new_reviews, list):
        raise EnrichmentError("Cluster audit output field 'new_reviews' must be a list")

    context_requested = bool(parsed.get("context_requested"))
    context_request_count = int(parsed.get("context_request_count", 0) or 0)
    seen_update_cluster_ids: set[str] = set()
    for idx, candidate in enumerate(bridge_updates, start=1):
        if not isinstance(candidate, dict):
            raise EnrichmentError(f"bridge_updates[{idx}] must be an object")
        cluster_id = str(candidate.get("cluster_id", "")).strip()
        if not cluster_id:
            raise EnrichmentError(f"bridge_updates[{idx}] is missing cluster_id")
        if cluster_id not in existing_cluster_by_id:
            raise EnrichmentError(f"bridge_updates[{idx}] references unknown cluster_id '{cluster_id}'")
        if cluster_id not in target_cluster_ids:
            raise EnrichmentError(f"bridge_updates[{idx}] references cluster_id '{cluster_id}' outside audit_targets")
        if cluster_id in seen_update_cluster_ids:
            raise EnrichmentError(f"bridge_updates[{idx}] duplicates cluster_id '{cluster_id}'")
        seen_update_cluster_ids.add(cluster_id)

    for idx, review in enumerate(review_updates, start=1):
        if not isinstance(review, dict):
            raise EnrichmentError(f"review_updates[{idx}] must be an object")
        cluster_id = str(review.get("cluster_id", "")).strip() or str(review.get("cluster_ref", "")).strip()
        if not cluster_id:
            raise EnrichmentError(f"review_updates[{idx}] is missing cluster_id")
        if cluster_id not in target_cluster_ids:
            raise EnrichmentError(f"review_updates[{idx}] references cluster_id '{cluster_id}' outside audit_targets")

    concept_index = deps._cluster_audit_mutable_concept_index(local_rows)
    assigned_pairs = deps._cover_pairs_from_clusters(clusters)

    applied_update_cluster_ids: set[str] = set()
    skipped_update_cluster_ids: set[str] = set()
    updated_bridge_rows = []
    for idx, candidate in enumerate(bridge_updates, start=1):
        cluster_id = str(candidate.get("cluster_id", "")).strip()
        try:
            validated = deps._cluster_audit_apply_bridge_update(
                candidate,
                existing_cluster_by_id[cluster_id],
                concept_index,
                assigned_pairs,
                label=f"bridge_updates[{idx}]",
            )
        except EnrichmentError as exc:
            if _cluster_audit_is_nonfatal_bridge_update_error(exc):
                skipped_update_cluster_ids.add(cluster_id)
                continue
            raise
        updated_bridge_rows.append(validated)
        applied_update_cluster_ids.add(cluster_id)

    filtered_review_updates = [
        review
        for review in review_updates
        if _cluster_audit_review_ref(review) not in skipped_update_cluster_ids
    ]
    filtered_new_reviews = [
        review
        for review in new_reviews
        if _cluster_audit_review_ref(review) not in skipped_update_cluster_ids
    ]
    synthetic_review_updates = []
    synthetic_new_reviews = []
    for cluster_id in sorted(skipped_update_cluster_ids):
        review_fields = _cluster_audit_skipped_bridge_update_review_fields()
        if cluster_id in canonical_existing_reviews:
            synthetic_review_updates.append({
                "cluster_id": cluster_id,
                **review_fields,
            })
        else:
            synthetic_new_reviews.append({
                "cluster_ref": cluster_id,
                **review_fields,
            })

    new_bridge_candidates = []
    new_bridge_refs: list[tuple[str, dict]] = []
    dropped_new_bridge_refs: set[str] = set()
    for idx, candidate in enumerate(new_bridges, start=1):
        if not isinstance(candidate, dict):
            raise EnrichmentError(f"new_bridges[{idx}] must be an object")
        bridge_ref = str(candidate.get("bridge_ref", "")).strip()
        if not bridge_ref:
            raise EnrichmentError(f"new_bridges[{idx}] is missing bridge_ref")
        validated = deps._cluster_audit_validate_bridge_candidate(
            candidate,
            concept_index,
            assigned_pairs,
            label=f"new_bridges[{idx}]",
        )
        if validated is None:
            dropped_new_bridge_refs.add(bridge_ref)
            continue
        new_bridge_refs.append((bridge_ref, validated))
        new_bridge_candidates.append(validated)

    assigned_new_bridges = deps._assign_new_bridge_ids(new_bridge_candidates, clusters) if new_bridge_candidates else []
    bridge_ref_map = {
        bridge_ref: assigned_new_bridges[idx]
        for idx, (bridge_ref, _candidate) in enumerate(new_bridge_refs)
        if idx < len(assigned_new_bridges)
    }

    normalized_new_reviews = []
    for idx, review in enumerate([*filtered_new_reviews, *synthetic_new_reviews], start=1):
        if not isinstance(review, dict):
            raise EnrichmentError(f"new_reviews[{idx}] must be an object")
        cluster_ref = deps._cluster_audit_review_ref(review)
        if not cluster_ref:
            raise EnrichmentError(f"new_reviews[{idx}] is missing cluster_ref")
        if cluster_ref in bridge_ref_map:
            normalized_new_reviews.append(review)
            continue
        if cluster_ref in dropped_new_bridge_refs:
            continue
        if cluster_ref in existing_cluster_by_id and cluster_ref not in target_cluster_ids:
            raise EnrichmentError(f"new_reviews[{idx}] references cluster_id '{cluster_ref}' outside audit_targets")
        normalized_new_reviews.append(review)
    bridge_refs = {**existing_cluster_by_id, **bridge_ref_map}

    bridge_work = sorted([
        *[cluster for cluster in clusters if str(cluster.get("cluster_id", "")).strip() not in applied_update_cluster_ids],
        *updated_bridge_rows,
        *assigned_new_bridges,
    ], key=lambda c: c.get("cluster_id", ""))
    reviews_work, reviewed_cluster_ids = deps._cluster_audit_apply_review_delta(
        existing_rows,
        [*filtered_review_updates, *synthetic_review_updates],
        normalized_new_reviews,
        bridge_refs,
    )
    required_review_ids = set(target_cluster_ids) | {
        str(cluster.get("cluster_id", "")).strip()
        for cluster in assigned_new_bridges
        if str(cluster.get("cluster_id", "")).strip()
    }
    missing_review_ids = sorted(cluster_id for cluster_id in required_review_ids if cluster_id not in reviewed_cluster_ids)
    if missing_review_ids:
        raise EnrichmentError(
            "Cluster audit must update review rows for every target or new bridge cluster: "
            + ", ".join(missing_review_ids)
        )
    reviews_work = deps._ensure_cluster_audit_review_coverage(reviews_work, bridge_work)

    final_cluster_fingerprints = deps._cluster_audit_cluster_fingerprints(bridge_work, route_signature)
    reviews_work = deps._cluster_audit_finalize_reviews(
        reviews_work,
        final_cluster_fingerprints,
        context_requested=context_requested,
        context_request_count=context_request_count,
    )

    if not isinstance(bridge_work, list) or any(not isinstance(cluster, dict) for cluster in bridge_work):
        raise EnrichmentError("Cluster audit output produced invalid bridge clusters")
    if not isinstance(reviews_work, list) or any(not isinstance(row, dict) for row in reviews_work):
        raise EnrichmentError("Cluster audit output produced invalid cluster reviews")

    bridge_changed = bridge_work != clusters

    run_at = datetime.now(timezone.utc).isoformat()
    deps._attach_run_provenance(reviews_work, route_signature, run_at)
    deps._attach_run_provenance(bridge_work, route_signature, run_at)
    deps._write_jsonl(root / deps.LINT_DIR / "cluster_reviews.jsonl", reviews_work)
    deps._write_lint_stage_stamp(root, audited_at=run_at)
    remaining_local_rows = deps._filter_local_rows_not_in_bridge(local_rows, bridge_work)
    deps._write_stamp(
        root / deps.CLUSTER_AUDIT_STATE_PATH,
        {
            "pending_local_fingerprint": deps._cluster_audit_pending_local_fingerprint(remaining_local_rows, route_signature),
            "pending_local_concepts": len(remaining_local_rows),
        },
    )
    deps._write_jsonl(root / "derived" / "bridge_concept_clusters.jsonl", bridge_work)
    stamp_path = root / "derived" / "bridge_cluster_stamp.json"
    stamp_path.write_text(
        json.dumps({
            "clustered_at": run_at,
            "fingerprint": deps.bridge_cluster_fingerprint(None),
            "bridge_concepts": sum(len(c.get("source_concepts", [])) for c in bridge_work),
            "clusters": len(bridge_work),
        }, separators=(",", ":")),
        encoding="utf-8",
    )
    deps._cleanup_paths(input_path, bridge_input_path, reviews_input_path, bridge_packets_path or Path())
    deps._cleanup_cluster_audit_debug_artifacts(root)
    return reviews_work, int(bridge_changed)


def _run_local_cluster_audit_impl(
    deps: Any,
    root: Path,
    clusters: list[dict],
    material_info: dict[str, dict],
    route_signature: str = "",
    llm_factory=None,
    tool=None,
) -> tuple[list[dict], int]:
    existing_path = root / deps.LINT_DIR / "cluster_reviews.jsonl"
    existing_rows = deps._load_jsonl(existing_path)
    local_groups = deps._group_clusters_by_scope(clusters)
    local_cluster_ids = {
        str(cluster.get("cluster_id", "")).strip()
        for cluster in clusters
        if str(cluster.get("cluster_id", "")).strip()
    }
    preserved_rows = [
        row for row in existing_rows
        if str(row.get("cluster_id", "")).strip() not in local_cluster_ids
    ]
    workers = max(1, min(len(local_groups), deps._parallel_collection_audit_workers(load_config())))

    def _one(domain: str, collection: str, scope_clusters: list[dict]) -> tuple[list[dict], int]:
        gate_path = deps._local_audit_gate_path(root, domain, collection)
        gate_path.parent.mkdir(parents=True, exist_ok=True)
        scope_cluster_ids = {
            str(cluster.get("cluster_id", "")).strip()
            for cluster in scope_clusters
            if str(cluster.get("cluster_id", "")).strip()
        }
        scope_existing_rows = [
            row for row in existing_rows
            if str(row.get("cluster_id", "")).strip() in scope_cluster_ids
        ]
        if not _acquire_cluster_audit_gate(gate_path):
            return scope_existing_rows, 0
        try:
            scope_root = deps.local_cluster_dir(root, domain, collection)
            local_rows, material_rows = deps._local_rows_in_scope_not_in_clusters(root, domain, collection, scope_clusters, material_info)
            existing_cluster_by_id = {
                str(cluster.get("cluster_id", "")).strip(): cluster
                for cluster in scope_clusters
                if str(cluster.get("cluster_id", "")).strip()
            }
            canonical_existing_reviews = deps._cluster_audit_canonicalize_existing_reviews(scope_existing_rows, existing_cluster_by_id)
            target_clusters, cluster_fingerprints = deps._cluster_audit_target_clusters(scope_clusters, canonical_existing_reviews, route_signature)
            target_cluster_ids = {
                str(target.get("cluster_id", "")).strip()
                for target in target_clusters
                if str(target.get("cluster_id", "")).strip()
            }
            current_pending_local_fingerprint = deps._cluster_audit_pending_local_fingerprint(local_rows, route_signature)
            audit_state = deps._read_stamp(deps._local_audit_state_path(root, domain, collection))
            prior_pending_local_fingerprint = str(audit_state.get("pending_local_fingerprint", "") or "")
            pending_local_changed = bool(local_rows and material_rows) and current_pending_local_fingerprint != prior_pending_local_fingerprint

            normalized_reviews = [canonical_existing_reviews[key] for key in sorted(canonical_existing_reviews)]
            normalized_reviews = deps._ensure_cluster_audit_review_coverage(normalized_reviews, scope_clusters)
            normalized_reviews = deps._cluster_audit_finalize_reviews(normalized_reviews, cluster_fingerprints)

            if not target_cluster_ids and not pending_local_changed:
                run_at = datetime.now(timezone.utc).isoformat()
                if normalized_reviews != scope_existing_rows:
                    deps._attach_run_provenance(normalized_reviews, route_signature, run_at)
                deps._write_stamp(
                    deps._local_audit_state_path(root, domain, collection),
                    {
                        "pending_local_fingerprint": current_pending_local_fingerprint,
                        "pending_local_concepts": len(local_rows),
                    },
                )
                deps._write_stamp(
                    deps._local_audit_stamp_path(root, domain, collection),
                    {
                        "audited_at": run_at,
                        "cluster_reviews": len(normalized_reviews),
                    },
                )
                return normalized_reviews, 0

            bridge_packets_path = None
            if local_rows and material_rows:
                bridge_packets_path = deps._stage_bridge_packet_input(
                    scope_root,
                    local_rows,
                    material_rows,
                    max_local_concepts_per_material=None,
                    max_bridge_candidates_per_material=None,
                    max_evidence_snippets_per_material=None,
                )

            bridge_input_path, reviews_input_path = deps._cluster_audit_input_paths(scope_root)
            reviewable_clusters = [
                cluster
                for cluster in scope_clusters
                if str(cluster.get("cluster_id", "")).strip() in target_cluster_ids
            ]
            packet = {
                "bridge_memory": str(bridge_input_path),
                "cluster_reviews": str(reviews_input_path),
                "bridge_packets": str(bridge_packets_path) if bridge_packets_path else "",
            }
            input_path = deps._cluster_audit_input_path(scope_root)
            deps._write_json(input_path, packet)
            deps._write_jsonl(bridge_input_path, reviewable_clusters)
            deps._stage_cluster_audit_reviews_input(reviews_input_path, canonical_existing_reviews, target_cluster_ids)
            llm_fn = llm_factory("lint")
            system, user = deps._cluster_audit_prompt(scope_root, input_path, bridge_input_path, reviews_input_path)

            def _record_cluster_audit_raw_response(raw_text: str, phase: str) -> None:
                path = deps._cluster_audit_raw_response_path(scope_root, phase)
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(str(raw_text), encoding="utf-8")

            parsed = deps._run_reflection_prompt_with_context(
                llm_fn,
                system,
                user,
                deps._CLUSTER_AUDIT_DELTA_SCHEMA,
                tool,
                raw_response_recorder=_record_cluster_audit_raw_response,
            )
            deps._write_json(deps._cluster_audit_parsed_response_path(scope_root), parsed if isinstance(parsed, dict) else {"raw": parsed})
            if not isinstance(parsed, dict):
                raise EnrichmentError("Cluster audit output must be a JSON object")
            if parsed.get("_finished") is not True:
                raise EnrichmentError("Cluster audit output missing _finished=true")
            parsed.pop("_finished", None)

            bridge_updates = parsed.get("bridge_updates", [])
            new_bridges = parsed.get("new_bridges", [])
            review_updates = parsed.get("review_updates", [])
            new_reviews = parsed.get("new_reviews", [])
            if not isinstance(bridge_updates, list):
                raise EnrichmentError("Cluster audit output field 'bridge_updates' must be a list")
            if not isinstance(new_bridges, list):
                raise EnrichmentError("Cluster audit output field 'new_bridges' must be a list")
            if not isinstance(review_updates, list):
                raise EnrichmentError("Cluster audit output field 'review_updates' must be a list")
            if not isinstance(new_reviews, list):
                raise EnrichmentError("Cluster audit output field 'new_reviews' must be a list")

            context_requested = bool(parsed.get("context_requested"))
            context_request_count = int(parsed.get("context_request_count", 0) or 0)
            seen_update_cluster_ids: set[str] = set()
            for idx, candidate in enumerate(bridge_updates, start=1):
                if not isinstance(candidate, dict):
                    raise EnrichmentError(f"bridge_updates[{idx}] must be an object")
                cluster_id = str(candidate.get("cluster_id", "")).strip()
                if not cluster_id:
                    raise EnrichmentError(f"bridge_updates[{idx}] is missing cluster_id")
                if cluster_id not in existing_cluster_by_id:
                    raise EnrichmentError(f"bridge_updates[{idx}] references unknown cluster_id '{cluster_id}'")
                if cluster_id not in target_cluster_ids:
                    raise EnrichmentError(f"bridge_updates[{idx}] references cluster_id '{cluster_id}' outside audit_targets")
                if cluster_id in seen_update_cluster_ids:
                    raise EnrichmentError(f"bridge_updates[{idx}] duplicates cluster_id '{cluster_id}'")
                seen_update_cluster_ids.add(cluster_id)

            for idx, review in enumerate(review_updates, start=1):
                if not isinstance(review, dict):
                    raise EnrichmentError(f"review_updates[{idx}] must be an object")
                cluster_id = str(review.get("cluster_id", "")).strip() or str(review.get("cluster_ref", "")).strip()
                if not cluster_id:
                    raise EnrichmentError(f"review_updates[{idx}] is missing cluster_id")
                if cluster_id not in target_cluster_ids:
                    raise EnrichmentError(f"review_updates[{idx}] references cluster_id '{cluster_id}' outside audit_targets")

            concept_index = deps._cluster_audit_mutable_concept_index(local_rows)
            assigned_pairs = deps._cover_pairs_from_clusters(scope_clusters)
            applied_update_cluster_ids: set[str] = set()
            skipped_update_cluster_ids: set[str] = set()
            updated_scope_rows = []
            for idx, candidate in enumerate(bridge_updates, start=1):
                cluster_id = str(candidate.get("cluster_id", "")).strip()
                try:
                    validated = deps._cluster_audit_apply_bridge_update(
                        candidate,
                        existing_cluster_by_id[cluster_id],
                        concept_index,
                        assigned_pairs,
                        label=f"bridge_updates[{idx}]",
                    )
                except EnrichmentError as exc:
                    if _cluster_audit_is_nonfatal_bridge_update_error(exc):
                        skipped_update_cluster_ids.add(cluster_id)
                        continue
                    raise
                updated_scope_rows.append(validated)
                applied_update_cluster_ids.add(cluster_id)

            filtered_review_updates = [
                review
                for review in review_updates
                if _cluster_audit_review_ref(review) not in skipped_update_cluster_ids
            ]
            filtered_new_reviews = [
                review
                for review in new_reviews
                if _cluster_audit_review_ref(review) not in skipped_update_cluster_ids
            ]
            synthetic_review_updates = []
            synthetic_new_reviews = []
            for cluster_id in sorted(skipped_update_cluster_ids):
                review_fields = _cluster_audit_skipped_bridge_update_review_fields()
                if cluster_id in canonical_existing_reviews:
                    synthetic_review_updates.append({
                        "cluster_id": cluster_id,
                        **review_fields,
                    })
                else:
                    synthetic_new_reviews.append({
                        "cluster_ref": cluster_id,
                        **review_fields,
                    })

            new_cluster_candidates = []
            new_cluster_refs: list[tuple[str, dict]] = []
            dropped_new_cluster_refs: set[str] = set()
            for idx, candidate in enumerate(new_bridges, start=1):
                if not isinstance(candidate, dict):
                    raise EnrichmentError(f"new_bridges[{idx}] must be an object")
                bridge_ref = str(candidate.get("bridge_ref", "")).strip()
                if not bridge_ref:
                    raise EnrichmentError(f"new_bridges[{idx}] is missing bridge_ref")
                validated = deps._cluster_audit_validate_bridge_candidate(
                    candidate,
                    concept_index,
                    assigned_pairs,
                    label=f"new_bridges[{idx}]",
                )
                if validated is None:
                    dropped_new_cluster_refs.add(bridge_ref)
                    continue
                new_cluster_refs.append((bridge_ref, validated))
                new_cluster_candidates.append(validated)

            assigned_new_clusters = []
            if new_cluster_candidates:
                normalized_scope = deps.normalize_local_clusters(
                    domain,
                    collection,
                    [*scope_clusters, *new_cluster_candidates],
                )
                assigned_new_clusters = normalized_scope[len(scope_clusters):]
            bridge_ref_map = {
                bridge_ref: assigned_new_clusters[idx]
                for idx, (bridge_ref, _candidate) in enumerate(new_cluster_refs)
                if idx < len(assigned_new_clusters)
            }

            normalized_new_reviews = []
            for idx, review in enumerate([*filtered_new_reviews, *synthetic_new_reviews], start=1):
                if not isinstance(review, dict):
                    raise EnrichmentError(f"new_reviews[{idx}] must be an object")
                cluster_ref = deps._cluster_audit_review_ref(review)
                if not cluster_ref:
                    raise EnrichmentError(f"new_reviews[{idx}] is missing cluster_ref")
                if cluster_ref in bridge_ref_map:
                    normalized_new_reviews.append(review)
                    continue
                if cluster_ref in dropped_new_cluster_refs:
                    continue
                if cluster_ref in existing_cluster_by_id and cluster_ref not in target_cluster_ids:
                    raise EnrichmentError(f"new_reviews[{idx}] references cluster_id '{cluster_ref}' outside audit_targets")
                normalized_new_reviews.append(review)
            bridge_refs = {**existing_cluster_by_id, **bridge_ref_map}

            scope_work = sorted([
                *[cluster for cluster in scope_clusters if str(cluster.get("cluster_id", "")).strip() not in applied_update_cluster_ids],
                *updated_scope_rows,
                *assigned_new_clusters,
            ], key=lambda c: c.get("cluster_id", ""))
            reviews_work, reviewed_cluster_ids = deps._cluster_audit_apply_review_delta(
                scope_existing_rows,
                [*filtered_review_updates, *synthetic_review_updates],
                normalized_new_reviews,
                bridge_refs,
            )
            required_review_ids = set(target_cluster_ids) | {
                str(cluster.get("cluster_id", "")).strip()
                for cluster in assigned_new_clusters
                if str(cluster.get("cluster_id", "")).strip()
            }
            missing_review_ids = sorted(cluster_id for cluster_id in required_review_ids if cluster_id not in reviewed_cluster_ids)
            if missing_review_ids:
                raise EnrichmentError(
                    "Cluster audit must update review rows for every target or new bridge cluster: "
                    + ", ".join(missing_review_ids)
                )
            reviews_work = deps._ensure_cluster_audit_review_coverage(reviews_work, scope_work)
            final_cluster_fingerprints = deps._cluster_audit_cluster_fingerprints(scope_work, route_signature)
            reviews_work = deps._cluster_audit_finalize_reviews(
                reviews_work,
                final_cluster_fingerprints,
                context_requested=context_requested,
                context_request_count=context_request_count,
            )

            cluster_changed = scope_work != scope_clusters
            run_at = datetime.now(timezone.utc).isoformat()
            deps._attach_run_provenance(reviews_work, route_signature, run_at)
            deps._attach_run_provenance(scope_work, route_signature, run_at)
            deps._write_jsonl(deps.local_cluster_dir(root, domain, collection) / "local_concept_clusters.jsonl", scope_work)
            deps.local_cluster_stamp_path(root, domain, collection).write_text(
                json.dumps(
                    {
                        "clustered_at": run_at,
                        "fingerprint": deps.local_cluster_fingerprint(domain, collection, load_config()),
                        "clusters": len(scope_work),
                        "bridge_concepts": sum(len(cluster.get("source_concepts", [])) for cluster in scope_work),
                    },
                    separators=(",", ":"),
                ),
                encoding="utf-8",
            )
            remaining_local_rows = deps._filter_local_rows_not_in_bridge(local_rows, scope_work)
            deps._write_stamp(
                deps._local_audit_state_path(root, domain, collection),
                {
                    "pending_local_fingerprint": deps._cluster_audit_pending_local_fingerprint(remaining_local_rows, route_signature),
                    "pending_local_concepts": len(remaining_local_rows),
                },
            )
            deps._write_stamp(
                deps._local_audit_stamp_path(root, domain, collection),
                {
                    "audited_at": run_at,
                    "cluster_reviews": len(reviews_work),
                },
            )
            deps._cleanup_paths(input_path, bridge_input_path, reviews_input_path, bridge_packets_path or Path())
            deps._cleanup_cluster_audit_debug_artifacts(scope_root)
            return reviews_work, int(cluster_changed)
        finally:
            try:
                gate_path.unlink()
            except OSError:
                pass

    results: list[tuple[list[dict], int]] = []
    scope_items = sorted(local_groups.items())
    if len(scope_items) > 1 and workers > 1:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(_one, domain, collection, scope_clusters) for (domain, collection), scope_clusters in scope_items]
            for fut in as_completed(futures):
                results.append(fut.result())
    else:
        for (domain, collection), scope_clusters in scope_items:
            results.append(_one(domain, collection, scope_clusters))

    merged_reviews = preserved_rows + [row for reviews, _changed in results for row in reviews]
    merged_reviews.sort(key=lambda row: str(row.get("cluster_id", "")))
    deps._write_jsonl(existing_path, merged_reviews)
    deps._write_lint_stage_stamp(root, audited_at=datetime.now(timezone.utc).isoformat())
    return merged_reviews, sum(changed for _reviews, changed in results)