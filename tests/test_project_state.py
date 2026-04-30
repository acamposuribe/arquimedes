from __future__ import annotations

import pytest

from arquimedes.project_state import (
    ProjectStateError,
    append_project_note,
    load_project_notes,
    load_project_state,
    merge_project_state_delta,
    save_project_state,
)


def test_empty_project_state_scaffolds_deterministically(tmp_path):
    state = load_project_state("2407-casa-rio", root=tmp_path)

    assert state["domain"] == "proyectos"
    assert state["project_id"] == "2407-casa-rio"
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
        kind="decision",
        text="Se mantiene la escalera existente.",
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
            "kind": "decision",
            "text": "Se mantiene la escalera existente.",
            "source_refs": ["discord://2407/123"],
        }
    ]
