"""Persistence helpers for Proyectos project dossiers."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from arquimedes.config import get_project_root

PROJECT_STAGES = (
    "lead",
    "feasibility",
    "schematic_design",
    "basic_project",
    "execution_project",
    "tender",
    "construction",
    "handover",
    "archived",
)
UPDATED_BY = {"reflection", "hermes", "human", "cli"}
NOTE_KINDS = {"decision", "requirement", "risk", "deadline", "coordination", "learning", "mistake", "repair"}
LIST_FIELDS = {
    "last_material_ids",
    "main_objectives",
    "current_work_in_progress",
    "next_focus",
    "known_conditions",
    "decisions",
    "requirements",
    "risks_or_blockers",
    "missing_information",
    "positive_learnings",
    "mistakes_or_regrets",
    "repair_actions",
    "important_material_ids",
}
STATE_FIELDS = LIST_FIELDS | {
    "project_title",
    "stage",
    "stage_confidence",
    "updated_by",
}
SECTION_IDS = {
    "estado": "Estado del proyecto",
    "trabajo_en_curso": "Trabajo en curso",
    "objetivos_principales": "Objetivos principales",
    "condiciones_restricciones": "Condiciones y restricciones",
    "decisiones": "Decisiones",
    "requisitos": "Requisitos",
    "riesgos": "Problemas, riesgos y bloqueos",
    "informacion_pendiente": "Información pendiente",
    "proximo_foco": "Próximo foco",
    "aprendizajes": "Aprendizajes positivos",
    "errores_reparaciones": "Errores y acciones de reparación",
}


class ProjectStateError(ValueError):
    """Raised when a project state, note, or delta is invalid."""


def project_dir(root: Path, project_id: str) -> Path:
    return root / "derived" / "projects" / project_id


def project_state_path(root: Path, project_id: str) -> Path:
    return project_dir(root, project_id) / "project_state.json"


def project_notes_path(root: Path, project_id: str) -> Path:
    return project_dir(root, project_id) / "notes.jsonl"


def project_sections_path(root: Path, project_id: str) -> Path:
    return project_dir(root, project_id) / "sections.json"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def empty_project_state(project_id: str) -> dict[str, Any]:
    return {
        "domain": "proyectos",
        "project_id": project_id,
        "project_title": project_id.replace("-", " ").title(),
        "stage": "lead",
        "stage_confidence": 0.0,
        "last_material_ids": [],
        "main_objectives": [],
        "current_work_in_progress": [],
        "next_focus": [],
        "known_conditions": [],
        "decisions": [],
        "requirements": [],
        "risks_or_blockers": [],
        "missing_information": [],
        "positive_learnings": [],
        "mistakes_or_regrets": [],
        "repair_actions": [],
        "important_material_ids": [],
        "updated_at": "",
        "updated_by": "cli",
    }


def _as_list(value: Any, field: str) -> list:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ProjectStateError(f"{field} must be a list")
    return value


def validate_project_state(state: dict[str, Any]) -> dict[str, Any]:
    state = dict(state)
    if state.get("domain") != "proyectos":
        raise ProjectStateError("project state domain must be proyectos")
    project_id = str(state.get("project_id", "")).strip()
    if not project_id or project_id == "_general":
        raise ProjectStateError("project_id must be a real Proyectos collection id")
    stage = str(state.get("stage", "")).strip()
    if stage not in PROJECT_STAGES:
        raise ProjectStateError(f"stage must be one of: {', '.join(PROJECT_STAGES)}")
    try:
        confidence = float(state.get("stage_confidence", 0.0))
    except (TypeError, ValueError) as exc:
        raise ProjectStateError("stage_confidence must be a number") from exc
    if not 0.0 <= confidence <= 1.0:
        raise ProjectStateError("stage_confidence must be between 0 and 1")
    state["stage_confidence"] = confidence
    updated_by = str(state.get("updated_by", "")).strip()
    if updated_by not in UPDATED_BY:
        raise ProjectStateError(f"updated_by must be one of: {', '.join(sorted(UPDATED_BY))}")
    for field in LIST_FIELDS:
        state[field] = _as_list(state.get(field), field)
    _validate_horizon_disjointness(state)
    return state


def _normalized_items(values: list) -> set[str]:
    return {" ".join(str(item).casefold().split()) for item in values if str(item).strip()}


def _validate_horizon_disjointness(state: dict[str, Any]) -> None:
    horizons = [
        ("main_objectives", _normalized_items(state.get("main_objectives", []))),
        ("current_work_in_progress", _normalized_items(state.get("current_work_in_progress", []))),
        ("next_focus", _normalized_items(state.get("next_focus", []))),
    ]
    for idx, (left_name, left_values) in enumerate(horizons):
        for right_name, right_values in horizons[idx + 1:]:
            overlap = left_values & right_values
            if overlap:
                item = sorted(overlap)[0]
                raise ProjectStateError(f"{item!r} appears in both {left_name} and {right_name}")


def load_project_state(project_id: str, *, root: Path | None = None) -> dict[str, Any]:
    root = root or get_project_root()
    path = project_state_path(root, project_id)
    if not path.exists():
        return empty_project_state(project_id)
    return validate_project_state(json.loads(path.read_text(encoding="utf-8")))


def save_project_state(project_id: str, state: dict[str, Any], *, root: Path | None = None) -> dict[str, Any]:
    root = root or get_project_root()
    state = validate_project_state(state)
    path = project_state_path(root, project_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return state


def merge_project_state_delta(project_id: str, delta: dict[str, Any], *, root: Path | None = None) -> dict[str, Any]:
    root = root or get_project_root()
    state = load_project_state(project_id, root=root)
    updated_by = str(delta.get("updated_by", "")).strip()
    if updated_by not in UPDATED_BY:
        raise ProjectStateError(f"updated_by must be one of: {', '.join(sorted(UPDATED_BY))}")
    if updated_by == "lint":
        raise ProjectStateError("updated_by must name the actor, not lint")

    next_state = dict(state)
    next_stage = str(delta.get("stage", next_state["stage"])).strip()
    if next_stage != next_state["stage"]:
        old_idx = PROJECT_STAGES.index(next_state["stage"])
        new_idx = PROJECT_STAGES.index(next_stage) if next_stage in PROJECT_STAGES else -1
        if new_idx < old_idx:
            has_justification = bool(delta.get("mistakes_or_regrets") or delta.get("repair_actions"))
            if not has_justification:
                raise ProjectStateError("backwards stage transitions require mistakes_or_regrets or repair_actions")

    for key, value in delta.items():
        if key in {"domain", "project_id", "updated_at"}:
            continue
        if key in LIST_FIELDS:
            next_state[key] = _as_list(value, key)
        elif key in next_state:
            next_state[key] = value
    next_state["updated_by"] = updated_by
    next_state["updated_at"] = str(delta.get("updated_at") or now_iso())
    return save_project_state(project_id, next_state, root=root)


def append_project_note(
    project_id: str,
    *,
    kind: str,
    text: str,
    actor: str,
    source_refs: list | None = None,
    material_id: str | None = None,
    confidence: float | None = None,
    root: Path | None = None,
    timestamp: str | None = None,
) -> dict[str, Any]:
    if kind not in NOTE_KINDS:
        raise ProjectStateError(f"note kind must be one of: {', '.join(sorted(NOTE_KINDS))}")
    if actor not in UPDATED_BY:
        raise ProjectStateError(f"actor must be one of: {', '.join(sorted(UPDATED_BY))}")
    if not text.strip():
        raise ProjectStateError("note text is required")
    note: dict[str, Any] = {
        "actor": actor,
        "timestamp": timestamp or now_iso(),
        "kind": kind,
        "text": text.strip(),
        "source_refs": source_refs or [],
    }
    if material_id:
        note["material_id"] = material_id
    if confidence is not None:
        note["confidence"] = float(confidence)
    root = root or get_project_root()
    path = project_notes_path(root, project_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(note, ensure_ascii=False, separators=(",", ":")) + "\n")
    return note


def load_project_notes(project_id: str, *, root: Path | None = None) -> list[dict[str, Any]]:
    root = root or get_project_root()
    path = project_notes_path(root, project_id)
    if not path.exists():
        return []
    notes = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            notes.append(json.loads(line))
    return notes


def validate_project_id(project_id: str) -> str:
    project_id = str(project_id or "").strip()
    if not project_id:
        raise ProjectStateError("project id is required")
    if project_id == "_general":
        raise ProjectStateError("Proyectos/_general is an intake bucket, not a project")
    if "/" in project_id or "\\" in project_id:
        raise ProjectStateError("project id must be a collection slug, not a path")
    return project_id


def validate_state_field(field: str) -> str:
    field = str(field or "").strip()
    if field not in STATE_FIELDS:
        allowed = ", ".join(sorted(STATE_FIELDS - {"updated_by"}))
        raise ProjectStateError(f"field must be one of: {allowed}")
    if field == "updated_by":
        raise ProjectStateError("updated_by cannot be written directly")
    return field


def validate_section_id(section_id: str) -> str:
    section_id = str(section_id or "").strip()
    if not section_id:
        raise ProjectStateError("section_id is required")
    if "/" in section_id or "\\" in section_id:
        raise ProjectStateError("section_id must be a slug, not a path")
    return section_id


def _normalize_section_record(section_id: str, record: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(record, dict):
        raise ProjectStateError(f"section {section_id} must be an object")
    section_id = validate_section_id(record.get("section_id") or section_id)
    updated_by = str(record.get("updated_by", "")).strip()
    if updated_by not in UPDATED_BY:
        raise ProjectStateError(f"section {section_id} updated_by must be one of: {', '.join(sorted(UPDATED_BY))}")
    revision = int(record.get("revision", 0))
    if revision < 0:
        raise ProjectStateError(f"section {section_id} revision must be non-negative")
    confidence = float(record.get("confidence", 0.0))
    if not 0.0 <= confidence <= 1.0:
        raise ProjectStateError(f"section {section_id} confidence must be between 0 and 1")
    return {
        "section_id": section_id,
        "title": str(record.get("title") or SECTION_IDS.get(section_id) or section_id.replace("_", " ").title()),
        "body": str(record.get("body", "")),
        "updated_at": str(record.get("updated_at", "")),
        "updated_by": updated_by,
        "source_refs": _as_list(record.get("source_refs"), f"{section_id}.source_refs"),
        "evidence_material_ids": _as_list(record.get("evidence_material_ids"), f"{section_id}.evidence_material_ids"),
        "confidence": confidence,
        "protected": bool(record.get("protected", False)),
        "revision": revision,
    }


def load_project_sections(project_id: str, *, root: Path | None = None) -> dict[str, dict[str, Any]]:
    root = root or get_project_root()
    path = project_sections_path(root, project_id)
    if not path.exists():
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(raw, list):
        records = raw
    elif isinstance(raw, dict):
        records = raw.get("sections", raw)
        if isinstance(records, dict):
            records = list(records.values())
    else:
        raise ProjectStateError("sections.json must be an object or list")
    sections: dict[str, dict[str, Any]] = {}
    for record in records:
        section_id = validate_section_id(record.get("section_id", "") if isinstance(record, dict) else "")
        sections[section_id] = _normalize_section_record(section_id, record)
    return sections


def save_project_sections(
    project_id: str,
    sections: dict[str, dict[str, Any]],
    *,
    root: Path | None = None,
) -> dict[str, dict[str, Any]]:
    root = root or get_project_root()
    normalized = {
        section_id: _normalize_section_record(section_id, record)
        for section_id, record in sorted(sections.items())
    }
    path = project_sections_path(root, project_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"sections": [normalized[key] for key in sorted(normalized)]}
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return normalized


def empty_project_section(section_id: str) -> dict[str, Any]:
    section_id = validate_section_id(section_id)
    return {
        "section_id": section_id,
        "title": SECTION_IDS.get(section_id) or section_id.replace("_", " ").title(),
        "body": "",
        "updated_at": "",
        "updated_by": "cli",
        "source_refs": [],
        "evidence_material_ids": [],
        "confidence": 0.0,
        "protected": False,
        "revision": 0,
    }


def merge_project_section_delta(
    project_id: str,
    delta: dict[str, Any],
    *,
    root: Path | None = None,
) -> dict[str, Any]:
    root = root or get_project_root()
    section_id = validate_section_id(delta.get("section_id", ""))
    updated_by = str(delta.get("updated_by", "")).strip()
    if updated_by not in UPDATED_BY:
        raise ProjectStateError(f"updated_by must be one of: {', '.join(sorted(UPDATED_BY))}")
    body = str(delta.get("body", ""))
    if not body.strip():
        raise ProjectStateError("section body is required")

    sections = load_project_sections(project_id, root=root)
    prior = sections.get(section_id, empty_project_section(section_id))
    if str(delta.get("replaces_updated_at", "")) != str(prior.get("updated_at", "")):
        raise ProjectStateError("stale section update: replaces_updated_at does not match current updated_at")
    expected_revision = int(prior.get("revision", 0)) + 1
    if int(delta.get("revision", -1)) != expected_revision:
        raise ProjectStateError(f"section revision must be {expected_revision}")

    if prior.get("protected") and updated_by == "reflection":
        if not str(delta.get("justification", "")).strip():
            raise ProjectStateError("reflection overwrite of protected section requires justification")
        if delta.get("references_prior_body") is not True:
            raise ProjectStateError("reflection overwrite of protected section requires references_prior_body=true")

    next_record = dict(prior)
    next_record.update({
        "section_id": section_id,
        "title": str(delta.get("title") or prior.get("title") or SECTION_IDS.get(section_id) or section_id),
        "body": body,
        "updated_at": str(delta.get("updated_at") or now_iso()),
        "updated_by": updated_by,
        "source_refs": _as_list(delta.get("source_refs"), "source_refs"),
        "evidence_material_ids": _as_list(delta.get("evidence_material_ids"), "evidence_material_ids"),
        "confidence": float(delta.get("confidence", prior.get("confidence", 0.0))),
        "revision": expected_revision,
    })
    if updated_by in {"hermes", "human"}:
        next_record["protected"] = True
    elif updated_by == "reflection":
        next_record["protected"] = bool(prior.get("protected", False))
    else:
        next_record["protected"] = bool(delta.get("protected", prior.get("protected", False)))

    sections[section_id] = _normalize_section_record(section_id, next_record)
    save_project_sections(project_id, sections, root=root)
    return sections[section_id]


def set_project_section(
    project_id: str,
    section_id: str,
    *,
    body: str,
    actor: str = "hermes",
    source_refs: list | None = None,
    evidence_material_ids: list | None = None,
    confidence: float = 1.0,
    root: Path | None = None,
) -> dict[str, Any]:
    root = root or get_project_root()
    section_id = validate_section_id(section_id)
    prior = load_project_sections(project_id, root=root).get(section_id, empty_project_section(section_id))
    return merge_project_section_delta(
        project_id,
        {
            "section_id": section_id,
            "body": body,
            "updated_by": actor,
            "revision": int(prior.get("revision", 0)) + 1,
            "replaces_updated_at": prior.get("updated_at", ""),
            "source_refs": source_refs or [],
            "evidence_material_ids": evidence_material_ids or [],
            "confidence": confidence,
        },
        root=root,
    )
