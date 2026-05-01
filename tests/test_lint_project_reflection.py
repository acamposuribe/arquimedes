from __future__ import annotations

import json
from pathlib import Path

import arquimedes.lint as lint_mod


def _write_project_material(root: Path, material_id: str = "mat_project") -> None:
    (root / "manifests").mkdir(parents=True, exist_ok=True)
    (root / "extracted" / material_id).mkdir(parents=True, exist_ok=True)
    manifest_path = root / "manifests" / "materials.jsonl"
    existing = manifest_path.read_text(encoding="utf-8") if manifest_path.exists() else ""
    manifest_path.write_text(
        existing + json.dumps({
            "material_id": material_id,
            "file_hash": material_id,
            "relative_path": f"Proyectos/2407-casa-rio/{material_id}.pdf",
            "file_type": "pdf",
            "domain": "proyectos",
            "collection": "2407-casa-rio",
            "ingested_at": "2026-04-30T10:00:00+00:00",
        }) + "\n",
        encoding="utf-8",
    )
    (root / "extracted" / material_id / "meta.json").write_text(
        json.dumps({
            "material_id": material_id,
            "title": "Acta de seguimiento",
            "domain": "proyectos",
            "collection": "2407-casa-rio",
            "summary": "El acta recoge tareas pendientes de licencia.",
            "project_extraction": {
                "project_material_type": "meeting_notes",
                "risks_or_blockers": ["Licencia pendiente"],
                "open_items": ["Confirmar acometida eléctrica"],
            },
        }),
        encoding="utf-8",
    )


def test_run_reflective_lint_project_reflection_updates_state_and_sections(tmp_path, monkeypatch):
    import arquimedes.project_state as project_state_mod

    _write_project_material(tmp_path)
    (tmp_path / "indexes").mkdir()
    (tmp_path / "indexes" / "search.sqlite").write_text("", encoding="utf-8")

    class DummyTool:
        def __init__(self, _root: Path):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def llm_factory(stage: str):
        assert stage == "project-reflection"

        def llm(_system, _messages):
            return json.dumps({
                "state_delta": {
                    "stage": "schematic_design",
                    "stage_confidence": 0.8,
                    "current_work_in_progress": ["Preparar documentación de licencia"],
                    "next_focus": ["Confirmar acometida eléctrica"],
                    "risks_or_blockers": ["Licencia pendiente"],
                    "important_material_ids": ["mat_project"],
                    "last_material_ids": ["mat_project"],
                },
                "section_deltas": [{
                    "section_id": "proximo_foco",
                    "body": "Confirmar acometida eléctrica y cerrar documentación de licencia.",
                    "revision": 1,
                    "replaces_updated_at": "",
                    "justification": "Primer resumen desde acta.",
                    "references_prior_body": False,
                    "source_refs": ["mat_project"],
                    "evidence_material_ids": ["mat_project"],
                    "confidence": 0.8,
                }],
                "_finished": True,
            })

        return llm

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(lint_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(lint_mod, "get_index_path", lambda: tmp_path / "indexes" / "search.sqlite")
    monkeypatch.setattr(lint_mod, "ReflectionIndexTool", DummyTool)
    monkeypatch.setattr(lint_mod, "compile_wiki", lambda *args, **kwargs: None)
    monkeypatch.setattr(lint_mod, "memory_rebuild", lambda _config: None)
    monkeypatch.setattr(project_state_mod, "get_project_root", lambda: tmp_path)

    result = lint_mod.run_reflective_lint(
        {"llm": {"agent_cmd": "echo"}},
        {"summary": {"issues": 0}, "issues": []},
        stages=["project-reflection"],
        llm_factory=llm_factory,
    )

    state = project_state_mod.load_project_state("2407-casa-rio", root=tmp_path)
    sections = project_state_mod.load_project_sections("2407-casa-rio", root=tmp_path)
    records = lint_mod._load_jsonl(tmp_path / "derived" / "lint" / "project_reflections.jsonl")

    assert result["project_reflections"] == 1
    assert state["stage"] == "schematic_design"
    assert state["current_work_in_progress"] == ["Preparar documentación de licencia"]
    assert sections["proximo_foco"]["body"] == "Confirmar acometida eléctrica y cerrar documentación de licencia."
    assert records[0]["project_id"] == "2407-casa-rio"
    assert records[0]["reflected_material_ids"] == ["mat_project"]


def test_project_reflection_failure_writes_raw_response_debug(tmp_path, monkeypatch):
    import pytest
    import arquimedes.project_state as project_state_mod

    _write_project_material(tmp_path)
    (tmp_path / "indexes").mkdir()
    (tmp_path / "indexes" / "search.sqlite").write_text("", encoding="utf-8")

    class DummyTool:
        def __init__(self, _root: Path):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    raw_response = json.dumps({
        "state_delta": {"stage": "anteproyecto"},
        "section_deltas": [],
        "_finished": True,
    })

    def llm_factory(_stage: str):
        def llm(_system, _messages):
            return raw_response

        return llm

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(lint_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(lint_mod, "get_index_path", lambda: tmp_path / "indexes" / "search.sqlite")
    monkeypatch.setattr(lint_mod, "ReflectionIndexTool", DummyTool)
    monkeypatch.setattr(lint_mod, "memory_rebuild", lambda _config: None)
    monkeypatch.setattr(project_state_mod, "get_project_root", lambda: tmp_path)

    with pytest.raises(project_state_mod.ProjectStateError):
        lint_mod.run_reflective_lint(
            {"llm": {"agent_cmd": "echo"}},
            {"summary": {"issues": 0}, "issues": []},
            stages=["project-reflection"],
            llm_factory=llm_factory,
        )

    debug_path = tmp_path / "derived" / "tmp" / "project_reflections" / "2407-casa-rio.failure.json"
    debug = json.loads(debug_path.read_text(encoding="utf-8"))
    assert debug["raw_response"] == raw_response
    assert debug["parsed_response"]["state_delta"]["stage"] == "anteproyecto"
    assert "stage must be one of" in debug["error"]


def test_project_reflection_stage_skips_general_bucket(tmp_path, monkeypatch):
    _write_project_material(tmp_path)
    meta_path = tmp_path / "extracted" / "mat_project" / "meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    meta["collection"] = "_general"
    meta_path.write_text(json.dumps(meta), encoding="utf-8")

    (tmp_path / "indexes").mkdir()
    (tmp_path / "indexes" / "search.sqlite").write_text("", encoding="utf-8")

    class DummyTool:
        def __init__(self, _root: Path):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def llm_factory(_stage: str):
        raise AssertionError("project reflection should skip _general")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(lint_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(lint_mod, "get_index_path", lambda: tmp_path / "indexes" / "search.sqlite")
    monkeypatch.setattr(lint_mod, "ReflectionIndexTool", DummyTool)
    monkeypatch.setattr(lint_mod, "memory_rebuild", lambda _config: None)

    result = lint_mod.run_reflective_lint(
        {"llm": {"agent_cmd": "echo"}},
        {"summary": {"issues": 0}, "issues": []},
        stages=["project-reflection"],
        llm_factory=llm_factory,
    )

    assert result["project_reflections"] == 0


def test_project_reflection_is_incremental_after_first_run(tmp_path, monkeypatch):
    import arquimedes.project_state as project_state_mod

    _write_project_material(tmp_path, "mat_initial")
    _write_project_material(tmp_path, "mat_new")
    (tmp_path / "derived" / "lint").mkdir(parents=True, exist_ok=True)
    (tmp_path / "derived" / "lint" / "project_reflections.jsonl").write_text(
        json.dumps({
            "project_id": "2407-casa-rio",
            "domain": "proyectos",
            "reflected_material_ids": ["mat_initial"],
            "input_fingerprint": "old",
        }) + "\n",
        encoding="utf-8",
    )
    (tmp_path / "indexes").mkdir()
    (tmp_path / "indexes" / "search.sqlite").write_text("", encoding="utf-8")

    seen_material_ids: list[list[str]] = []

    class DummyTool:
        def __init__(self, _root: Path):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def llm_factory(_stage: str):
        def llm(_system, messages):
            evidence_path = Path(messages[0]["content"].split("Lee el paquete de evidencia del proyecto en:\n", 1)[1].split("\n", 1)[0])
            payload = json.loads(evidence_path.read_text(encoding="utf-8"))
            seen_material_ids.append([row["material_id"] for row in payload["materials"]])
            return json.dumps({
                "state_delta": {
                    "last_material_ids": ["mat_new"],
                    "next_focus": ["Revisar nuevo material"],
                },
                "section_deltas": [],
                "_finished": True,
            })

        return llm

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(lint_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(lint_mod, "get_index_path", lambda: tmp_path / "indexes" / "search.sqlite")
    monkeypatch.setattr(lint_mod, "ReflectionIndexTool", DummyTool)
    monkeypatch.setattr(lint_mod, "compile_wiki", lambda *args, **kwargs: None)
    monkeypatch.setattr(lint_mod, "memory_rebuild", lambda _config: None)
    monkeypatch.setattr(project_state_mod, "get_project_root", lambda: tmp_path)

    result = lint_mod.run_reflective_lint(
        {"llm": {"agent_cmd": "echo"}},
        {"summary": {"issues": 0}, "issues": []},
        stages=["project-reflection"],
        llm_factory=llm_factory,
    )

    records = lint_mod._load_jsonl(tmp_path / "derived" / "lint" / "project_reflections.jsonl")
    assert result["project_reflections"] == 1
    assert seen_material_ids == [["mat_new"]]
    assert records[0]["reflected_material_ids"] == ["mat_initial", "mat_new"]


def test_project_reflection_runs_for_new_notes_without_new_materials(tmp_path, monkeypatch):
    import arquimedes.project_state as project_state_mod
    from arquimedes.lint_project_reflection import _evidence_signatures

    _write_project_material(tmp_path, "mat_initial")
    project_state_mod.append_project_note(
        "2407-casa-rio",
        kind="coordination",
        text="Esta semana se revisa la acometida eléctrica.",
        actor="hermes",
        root=tmp_path,
        timestamp="2026-04-30T12:00:00+00:00",
    )
    signatures = _evidence_signatures(tmp_path, "2407-casa-rio")
    project_state_mod.append_project_note(
        "2407-casa-rio",
        kind="risk",
        text="La acometida sigue sin confirmar.",
        actor="hermes",
        root=tmp_path,
        timestamp="2026-04-30T13:00:00+00:00",
    )
    (tmp_path / "derived" / "lint").mkdir(parents=True, exist_ok=True)
    (tmp_path / "derived" / "lint" / "project_reflections.jsonl").write_text(
        json.dumps({
            "project_id": "2407-casa-rio",
            "domain": "proyectos",
            "reflected_material_ids": ["mat_initial"],
            **signatures,
        }) + "\n",
        encoding="utf-8",
    )
    (tmp_path / "indexes").mkdir()
    (tmp_path / "indexes" / "search.sqlite").write_text("", encoding="utf-8")

    seen_material_ids: list[list[str]] = []

    class DummyTool:
        def __init__(self, _root: Path):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def llm_factory(_stage: str):
        def llm(_system, messages):
            evidence_path = Path(messages[0]["content"].split("Lee el paquete de evidencia del proyecto en:\n", 1)[1].split("\n", 1)[0])
            payload = json.loads(evidence_path.read_text(encoding="utf-8"))
            seen_material_ids.append([row["material_id"] for row in payload["materials"]])
            assert len(payload["notes"]) == 2
            return json.dumps({
                "state_delta": {
                    "current_work_in_progress": ["Revisar acometida eléctrica"],
                },
                "section_deltas": [],
                "_finished": True,
            })

        return llm

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(lint_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(lint_mod, "get_index_path", lambda: tmp_path / "indexes" / "search.sqlite")
    monkeypatch.setattr(lint_mod, "ReflectionIndexTool", DummyTool)
    monkeypatch.setattr(lint_mod, "compile_wiki", lambda *args, **kwargs: None)
    monkeypatch.setattr(lint_mod, "memory_rebuild", lambda _config: None)
    monkeypatch.setattr(project_state_mod, "get_project_root", lambda: tmp_path)

    result = lint_mod.run_reflective_lint(
        {"llm": {"agent_cmd": "echo"}},
        {"summary": {"issues": 0}, "issues": []},
        stages=["project-reflection"],
        llm_factory=llm_factory,
    )

    assert result["project_reflections"] == 1
    assert seen_material_ids == [[]]
