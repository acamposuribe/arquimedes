from __future__ import annotations

import pytest

from arquimedes.project_state import (
    ProjectStateError,
    append_project_note,
    empty_project_section,
    load_project_notes,
    load_project_sections,
    load_project_state,
    mark_project_notes_incorporated,
    merge_project_section_delta,
    merge_project_state_delta,
    resolve_project_item,
    save_project_state,
    set_project_note_status,
    set_project_section,
    update_project_note,
)


def test_empty_project_state_scaffolds_deterministically(tmp_path):
    state = load_project_state("2407-casa-rio", root=tmp_path)

    assert state["domain"] == "proyectos"
    assert state["project_id"] == "2407-casa-rio"
    assert state["main_strategy"] == ""
    assert state["stage"] == "lead"
    assert state["main_objectives"] == []


def test_project_state_delta_merges_without_dropping_fields(tmp_path):
    state = load_project_state("2407-casa-rio", root=tmp_path)
    state["known_conditions"] = ["Parcela en esquina"]
    save_project_state("2407-casa-rio", state, root=tmp_path)

    merged = merge_project_state_delta(
        "2407-casa-rio",
        {
            "updated_by": "reflection",
            "stage": "feasibility",
            "next_focus": ["Confirmar servidumbres"],
        },
        root=tmp_path,
    )

    assert merged["known_conditions"] == ["Parcela en esquina"]
    assert merged["stage"] == "feasibility"
    assert merged["next_focus"] == ["Confirmar servidumbres"]


def test_project_state_rejects_lint_actor(tmp_path):
    with pytest.raises(ProjectStateError, match="updated_by"):
        merge_project_state_delta("2407-casa-rio", {"updated_by": "lint"}, root=tmp_path)


def test_project_state_rejects_backwards_stage_without_justification(tmp_path):
    state = load_project_state("2407-casa-rio", root=tmp_path)
    state["stage"] = "basic_project"
    save_project_state("2407-casa-rio", state, root=tmp_path)

    with pytest.raises(ProjectStateError, match="backwards"):
        merge_project_state_delta(
            "2407-casa-rio",
            {"updated_by": "reflection", "stage": "schematic_design"},
            root=tmp_path,
        )


def test_project_state_rejects_duplicate_horizon_items(tmp_path):
    with pytest.raises(ProjectStateError, match="appears in both"):
        merge_project_state_delta(
            "2407-casa-rio",
            {
                "updated_by": "reflection",
                "main_objectives": ["Entregar proyecto básico"],
                "next_focus": ["Entregar proyecto básico"],
            },
            root=tmp_path,
        )


def test_project_notes_append_with_provenance(tmp_path):
    append_project_note(
        "2407-casa-rio",
        kind="strategy",
        text="Abrir la casa al paisaje fluvial y organizar la vida cotidiana en torno al porche.",
        actor="hermes",
        source_refs=["discord://2407/123"],
        root=tmp_path,
        timestamp="2026-04-30T10:00:00+00:00",
    )

    notes = load_project_notes("2407-casa-rio", root=tmp_path)
    assert notes == [
        {
            "actor": "hermes",
            "timestamp": "2026-04-30T10:00:00+00:00",
            "kind": "strategy",
            "text": "Abrir la casa al paisaje fluvial y organizar la vida cotidiana en torno al porche.",
            "source_refs": ["discord://2407/123"],
        }
    ]


def test_main_strategy_field_merges_as_scalar(tmp_path):
    state = merge_project_state_delta(
        "2407-casa-rio",
        {
            "updated_by": "hermes",
            "main_strategy": "Usar el patio como regulador climático y social del proyecto.",
        },
        root=tmp_path,
    )

    assert state["main_strategy"] == "Usar el patio como regulador climático y social del proyecto."


def test_strategy_note_cannot_be_archived_and_only_human_or_hermes_can_edit(tmp_path):
    note = append_project_note(
        "2407-casa-rio",
        kind="strategy",
        text="Organizar la casa desde el umbral entre paisaje y vida doméstica.",
        actor="hermes",
        root=tmp_path,
    )

    with pytest.raises(ProjectStateError, match="only be edited by hermes or human"):
        update_project_note("2407-casa-rio", note_id=note["note_id"], text="Cambio.", actor="reflection", root=tmp_path)
    with pytest.raises(ProjectStateError, match="always stay open"):
        set_project_note_status("2407-casa-rio", note_id=note["note_id"], status="incorporated", actor="human", root=tmp_path)

    edited = update_project_note(
        "2407-casa-rio",
        note_id=note["note_id"],
        text="Organizar la casa desde el umbral entre paisaje, clima y vida doméstica.",
        actor="human",
        root=tmp_path,
    )
    assert edited["updated_by"] == "human"
    assert edited["status"] == "open"


def test_mark_project_notes_incorporated_skips_strategy_notes(tmp_path):
    append_project_note("2407-casa-rio", kind="strategy", text="Estrategia permanente.", actor="hermes", root=tmp_path)
    append_project_note("2407-casa-rio", kind="risk", text="Licencia pendiente.", actor="hermes", root=tmp_path)

    changed = mark_project_notes_incorporated("2407-casa-rio", root=tmp_path)
    notes = load_project_notes("2407-casa-rio", root=tmp_path, include_deleted=True, include_metadata=True)

    assert changed == ["note-0002"]
    assert notes[0]["kind"] == "strategy"
    assert notes[0]["status"] == "open"
    assert notes[1]["kind"] == "risk"
    assert notes[1]["status"] == "incorporated"


def test_section_set_writes_protected_record(tmp_path):
    section = set_project_section(
        "2407-casa-rio",
        "riesgos",
        body="La licencia sigue pendiente.",
        actor="hermes",
        source_refs=["discord://2407/456"],
        root=tmp_path,
    )

    assert section["revision"] == 1
    assert section["protected"] is True
    assert section["updated_by"] == "hermes"
    assert load_project_sections("2407-casa-rio", root=tmp_path)["riesgos"]["body"] == "La licencia sigue pendiente."


def test_section_stale_replaces_updated_at_rejected(tmp_path):
    set_project_section("2407-casa-rio", "proximo_foco", body="Primera versión.", actor="hermes", root=tmp_path)

    with pytest.raises(ProjectStateError, match="stale section update"):
        merge_project_section_delta(
            "2407-casa-rio",
            {
                "section_id": "proximo_foco",
                "body": "Nueva versión.",
                "updated_by": "hermes",
                "revision": 2,
                "replaces_updated_at": "old-timestamp",
            },
            root=tmp_path,
        )


def test_reflection_cannot_overwrite_protected_without_justification_and_prior_reference(tmp_path):
    prior = set_project_section("2407-casa-rio", "estado", body="Texto Hermes.", actor="hermes", root=tmp_path)

    base_delta = {
        "section_id": "estado",
        "body": "Texto reflexión.",
        "updated_by": "reflection",
        "revision": 2,
        "replaces_updated_at": prior["updated_at"],
    }
    with pytest.raises(ProjectStateError, match="justification"):
        merge_project_section_delta("2407-casa-rio", dict(base_delta), root=tmp_path)
    with pytest.raises(ProjectStateError, match="references_prior_body"):
        merge_project_section_delta(
            "2407-casa-rio",
            {**base_delta, "justification": "Nueva evidencia."},
            root=tmp_path,
        )


def test_reflection_overwrite_preserves_protected_flag(tmp_path):
    prior = set_project_section("2407-casa-rio", "estado", body="Texto Hermes.", actor="hermes", root=tmp_path)

    section = merge_project_section_delta(
        "2407-casa-rio",
        {
            "section_id": "estado",
            "body": "Texto reflexión que actualiza Texto Hermes con evidencia nueva.",
            "updated_by": "reflection",
            "revision": 2,
            "replaces_updated_at": prior["updated_at"],
            "justification": "Incorpora el acta nueva.",
            "references_prior_body": True,
        },
        root=tmp_path,
    )

    assert section["protected"] is True
    assert section["updated_by"] == "reflection"
    assert section["revision"] == 2


def test_section_revision_skip_or_repeat_rejected(tmp_path):
    prior = empty_project_section("riesgos")

    with pytest.raises(ProjectStateError, match="revision must be 1"):
        merge_project_section_delta(
            "2407-casa-rio",
            {
                "section_id": "riesgos",
                "body": "Texto.",
                "updated_by": "hermes",
                "revision": 2,
                "replaces_updated_at": prior["updated_at"],
            },
            root=tmp_path,
        )


def test_two_reflection_writes_interleave_with_current_timestamp(tmp_path):
    first = merge_project_section_delta(
        "2407-casa-rio",
        {
            "section_id": "trabajo_en_curso",
            "body": "Primera reflexión.",
            "updated_by": "reflection",
            "revision": 1,
            "replaces_updated_at": "",
        },
        root=tmp_path,
    )
    second = merge_project_section_delta(
        "2407-casa-rio",
        {
            "section_id": "trabajo_en_curso",
            "body": "Segunda reflexión.",
            "updated_by": "reflection",
            "revision": 2,
            "replaces_updated_at": first["updated_at"],
        },
        root=tmp_path,
    )

    assert second["revision"] == 2
    assert second["body"] == "Segunda reflexión."


def test_resolve_project_item_removes_exact_text_and_writes_note(tmp_path):
    state = load_project_state("2407-casa-rio", root=tmp_path)
    state["missing_information"] = ["Confirmar acometida electrica", "Pedir catastro"]
    save_project_state("2407-casa-rio", state, root=tmp_path)

    result = resolve_project_item(
        "2407-casa-rio",
        item="Confirmar acometida electrica",
        note="La acometida queda confirmada por el tecnico.",
        actor="hermes",
        source_refs=["discord://2407/321"],
        root=tmp_path,
    )

    assert result["field"] == "missing_information"
    assert result["item"] == "Confirmar acometida electrica"
    assert result["state"]["missing_information"] == ["Pedir catastro"]
    notes = load_project_notes("2407-casa-rio", root=tmp_path)
    assert notes[-1]["kind"] == "coordination"
    assert "La acometida queda confirmada" in notes[-1]["text"]
    assert notes[-1]["source_refs"] == ["discord://2407/321"]


def test_resolve_project_item_accepts_field_index(tmp_path):
    state = load_project_state("2407-casa-rio", root=tmp_path)
    state["risks_or_blockers"] = ["Licencia pendiente", "Coste estructura"]
    save_project_state("2407-casa-rio", state, root=tmp_path)

    result = resolve_project_item(
        "2407-casa-rio",
        item="risks_or_blockers:2",
        note="Se ajusta la solucion estructural.",
        actor="human",
        root=tmp_path,
    )

    assert result["item"] == "Coste estructura"
    assert result["state"]["risks_or_blockers"] == ["Licencia pendiente"]
