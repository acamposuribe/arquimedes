"""Project reflection stage for the Proyectos domain."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from arquimedes.enrich_stamps import canonical_hash
from arquimedes.llm import parse_json_or_repair
from arquimedes.project_state import (
    load_project_notes,
    load_project_sections,
    load_project_state,
    merge_project_section_delta,
    merge_project_state_delta,
)

PROJECT_REFLECTION_SCHEMA = '{"state_delta":{"stage":"optional stage","stage_confidence":0.0,"main_objectives":["strings"],"current_work_in_progress":["strings"],"next_focus":["strings"],"known_conditions":["strings"],"decisions":["strings"],"requirements":["strings"],"risks_or_blockers":["strings"],"missing_information":["strings"],"positive_learnings":["strings"],"mistakes_or_regrets":["strings"],"repair_actions":["strings"],"important_material_ids":["ids"],"last_material_ids":["ids"]},"section_deltas":[{"section_id":"estado|trabajo_en_curso|riesgos|proximo_foco|...","title":"string optional","body":"string","revision":1,"replaces_updated_at":"exact prior updated_at","justification":"string","references_prior_body":true|false,"source_refs":["strings"],"evidence_material_ids":["ids"],"confidence":0.0}],"_finished":true}'


def _project_reflection_records_path(root: Path) -> Path:
    return root / "derived" / "lint" / "project_reflections.jsonl"


def _project_reflection_stage_dir(root: Path) -> Path:
    return root / "derived" / "tmp" / "project_reflections"


def _project_reflection_evidence_path(root: Path, project_id: str) -> Path:
    return _project_reflection_stage_dir(root) / f"{project_id}.evidence.json"


def _project_reflection_failure_path(root: Path, project_id: str) -> Path:
    return _project_reflection_stage_dir(root) / f"{project_id}.failure.json"


def _write_project_reflection_failure(
    root: Path,
    project_id: str,
    *,
    error: BaseException,
    raw_response: str,
    evidence_path: Path,
    parsed: Any = None,
) -> None:
    path = _project_reflection_failure_path(root, project_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "project_id": project_id,
        "failed_at": datetime.now(timezone.utc).isoformat(),
        "error_type": type(error).__name__,
        "error": str(error),
        "raw_response": raw_response,
        "parsed_response": parsed,
        "evidence_path": str(evidence_path),
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _project_material_packet(meta: dict) -> dict:
    project_extraction = meta.get("project_extraction") if isinstance(meta.get("project_extraction"), dict) else {}
    return {
        "material_id": meta.get("material_id", ""),
        "title": meta.get("title", ""),
        "summary": _meta_text(meta.get("summary")),
        "keywords": _meta_list(meta.get("keywords")),
        "project_extraction": project_extraction,
    }


def _meta_text(value: Any) -> str:
    if isinstance(value, dict):
        return str(value.get("value", "") or "")
    return str(value or "")


def _meta_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    if isinstance(value, dict):
        return _meta_list(value.get("value"))
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []


def _project_reflection_payload(root: Path, project_id: str, metas: list[dict]) -> dict:
    state = load_project_state(project_id, root=root)
    notes = load_project_notes(project_id, root=root)
    sections = load_project_sections(project_id, root=root)
    return {
        "kind": "project_reflection",
        "domain": "proyectos",
        "project_id": project_id,
        "incremental": True,
        "materials": [_project_material_packet(meta) for meta in sorted(metas, key=lambda row: row.get("material_id", ""))],
        "state": state,
        "notes": notes,
        "sections": sections,
    }


def _project_reflection_fingerprint(root: Path, project_id: str, metas: list[dict]) -> str:
    payload = _project_reflection_payload(root, project_id, metas)
    return canonical_hash(payload)


def _material_ids(metas: list[dict]) -> list[str]:
    return sorted(str(meta.get("material_id", "")).strip() for meta in metas if str(meta.get("material_id", "")).strip())


def _reflected_material_ids(record: dict | None) -> set[str]:
    if not record:
        return set()
    value = record.get("reflected_material_ids") or []
    if not isinstance(value, list):
        return set()
    return {str(item).strip() for item in value if str(item).strip()}


def _new_materials_for_reflection(metas: list[dict], existing_record: dict | None) -> list[dict]:
    reflected = _reflected_material_ids(existing_record)
    if not reflected:
        return list(metas)
    return [meta for meta in metas if str(meta.get("material_id", "")).strip() not in reflected]


def _notes_signature(notes: list[dict]) -> str:
    return canonical_hash([
        {
            "actor": note.get("actor", ""),
            "timestamp": note.get("timestamp", ""),
            "kind": note.get("kind", ""),
            "text": note.get("text", ""),
            "source_refs": note.get("source_refs", []),
            "material_id": note.get("material_id", ""),
        }
        for note in notes
    ])


def _sections_signature(sections: dict[str, dict]) -> str:
    return canonical_hash({
        section_id: {
            "revision": section.get("revision", 0),
            "updated_at": section.get("updated_at", ""),
            "updated_by": section.get("updated_by", ""),
            "protected": section.get("protected", False),
        }
        for section_id, section in sorted(sections.items())
    })


def _state_external_signature(state: dict) -> str:
    return canonical_hash({
        "updated_at": state.get("updated_at", ""),
        "updated_by": state.get("updated_by", ""),
    })


def _evidence_signatures(root: Path, project_id: str) -> dict[str, str]:
    return {
        "notes_signature": _notes_signature(load_project_notes(project_id, root=root)),
        "sections_signature": _sections_signature(load_project_sections(project_id, root=root)),
        "state_external_signature": _state_external_signature(load_project_state(project_id, root=root)),
    }


def _has_new_human_evidence(root: Path, project_id: str, existing_record: dict | None) -> bool:
    if not existing_record:
        return True
    signatures = _evidence_signatures(root, project_id)
    return any(existing_record.get(key) != value for key, value in signatures.items())


def _project_reflection_prompt(evidence_path: Path) -> tuple[str, str]:
    system = """\
Eres la reflexión operativa de un estudio de arquitectura. Mantienes memoria viva de un proyecto Proyectos.

Devuelve JSON válido y nada más.

Tareas:
- Actualiza project_state con el estado actual, notas/secciones actuales y los materiales incrementales del paquete.
- En la primera ejecución de un proyecto, el paquete contiene todos los materiales existentes; después contiene solo materiales nuevos.
- Si el paquete no contiene materiales, la reflexión está causada por notas, secciones o estado editados por Hermes/humanos.
- Distingue horizontes:
  - main_objectives: objetivos finales del proyecto.
  - current_work_in_progress: trabajo activo esta semana.
  - next_focus: foco de las próximas 1-2 semanas, no lo que ya está en curso.
- Trata notas y secciones Hermes/humanas como evidencia de alta prioridad.
- Propón section_deltas solo cuando mejoren una sección de página.
- Nunca borres texto protegido: si reemplazas una sección protegida como reflection, incluye justification no vacía y references_prior_body=true.
- Copia revision como prior.revision + 1 y replaces_updated_at como el updated_at exacto de la sección previa.
- Usa updated_by=reflection implícitamente; no lo incluyas salvo que el esquema lo pida.
"""
    user = f"""\
Lee el paquete de evidencia del proyecto en:
{evidence_path}

Responde con este esquema exacto:
{PROJECT_REFLECTION_SCHEMA}
"""
    return system, user


def _normalize_state_delta(parsed: dict) -> dict:
    delta = parsed.get("state_delta") or {}
    if not isinstance(delta, dict):
        delta = {}
    delta = dict(delta)
    delta["updated_by"] = "reflection"
    return delta


def _normalize_section_deltas(parsed: dict) -> list[dict]:
    raw = parsed.get("section_deltas") or []
    if not isinstance(raw, list):
        return []
    deltas = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        delta = dict(item)
        delta["updated_by"] = "reflection"
        deltas.append(delta)
    return deltas


def _eligible_project_groups(groups: dict[tuple[str, str], list[dict]]) -> list[tuple[str, list[dict]]]:
    eligible = []
    for (domain, collection), metas in groups.items():
        if domain == "proyectos" and collection != "_general" and metas:
            eligible.append((collection, metas))
    return sorted(eligible, key=lambda row: row[0])


def _run_project_reflections_impl(
    deps: Any,
    root: Path,
    groups: dict[tuple[str, str], list[dict]],
    llm_factory=None,
    tool=None,
    route_signature: str = "",
) -> dict:
    del tool
    existing = deps._existing_by_key(_project_reflection_records_path(root), "project_id")
    output: list[dict] = []
    changed = 0
    failures: list[BaseException] = []
    eligible = _eligible_project_groups(groups)
    if not eligible:
        deps._write_jsonl(_project_reflection_records_path(root), list(existing.values()))
        return {
            "project_reflections": 0,
            "project_reflection_projects": 0,
            "project_reflection_skipped": True,
        }
    llm_fn = llm_factory("project-reflection")

    for project_id, metas in eligible:
        existing_record = existing.get(project_id)
        new_metas = _new_materials_for_reflection(metas, existing_record)
        if not new_metas and not _has_new_human_evidence(root, project_id, existing_record):
            output.append(existing_record)
            continue

        evidence_path = _project_reflection_evidence_path(root, project_id)
        fingerprint = _project_reflection_fingerprint(root, project_id, new_metas)
        deps._write_json(evidence_path, _project_reflection_payload(root, project_id, new_metas))
        system, user = _project_reflection_prompt(evidence_path)
        raw = ""
        parsed: Any = None
        try:
            raw = llm_fn(system, [{"role": "user", "content": user}])
            parsed = parse_json_or_repair(llm_fn, raw, PROJECT_REFLECTION_SCHEMA)
            if not isinstance(parsed, dict) or parsed.get("_finished") is not True:
                raise ValueError("project reflection output must be a JSON object with _finished=true")

            state = merge_project_state_delta(project_id, _normalize_state_delta(parsed), root=root)
            sections_changed = []
            for delta in _normalize_section_deltas(parsed):
                section = merge_project_section_delta(project_id, delta, root=root)
                sections_changed.append(section["section_id"])

            run_at = datetime.now(timezone.utc).isoformat()
            reflected_material_ids = sorted(_reflected_material_ids(existing_record) | set(_material_ids(new_metas)))
            record = {
                "project_id": project_id,
                "domain": "proyectos",
                "input_fingerprint": fingerprint,
                "reflected_material_ids": reflected_material_ids,
                "new_material_ids": _material_ids(new_metas),
                **_evidence_signatures(root, project_id),
                "state_updated_at": state.get("updated_at", ""),
                "sections_changed": sections_changed,
                "updated_at": run_at,
                "route_signature": route_signature,
            }
            output.append(record)
            changed += 1
            try:
                evidence_path.unlink()
            except OSError:
                pass
            try:
                _project_reflection_failure_path(root, project_id).unlink()
            except OSError:
                pass
        except BaseException as exc:
            _write_project_reflection_failure(
                root,
                project_id,
                error=exc,
                raw_response=raw,
                parsed=parsed,
                evidence_path=evidence_path,
            )
            failures.append(exc)

    output_keys = {row["project_id"] for row in output if row.get("project_id")}
    output.extend(row for key, row in existing.items() if key not in output_keys)
    output.sort(key=lambda row: row.get("project_id", ""))
    deps._write_jsonl(_project_reflection_records_path(root), output)
    if failures:
        raise failures[0]
    return {
        "project_reflections": changed,
        "project_reflection_projects": len(eligible),
        "project_reflection_skipped": changed == 0,
    }
