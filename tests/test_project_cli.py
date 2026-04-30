from __future__ import annotations

import json

from click.testing import CliRunner

from arquimedes.cli import cli


def test_project_status_reads_state_and_notes(tmp_path, monkeypatch):
    import arquimedes.project_state as project_state_mod

    monkeypatch.setattr(project_state_mod, "get_project_root", lambda: tmp_path)
    project_state_mod.append_project_note(
        "2407-casa-rio",
        kind="decision",
        text="Se mantiene la escalera.",
        actor="hermes",
        root=tmp_path,
        timestamp="2026-04-30T10:00:00+00:00",
    )

    result = CliRunner().invoke(cli, ["project", "status", "2407-casa-rio"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["project_id"] == "2407-casa-rio"
    assert payload["state"]["stage"] == "lead"
    assert payload["notes"][0]["text"] == "Se mantiene la escalera."


def test_project_note_writes_provenance_without_recompile(tmp_path, monkeypatch):
    import arquimedes.project_state as project_state_mod

    monkeypatch.setattr(project_state_mod, "get_project_root", lambda: tmp_path)

    result = CliRunner().invoke(
        cli,
        [
            "project",
            "note",
            "2407-casa-rio",
            "--kind",
            "risk",
            "--text",
            "Falta confirmar acometida.",
            "--source-ref",
            "discord://2407/123",
            "--no-recompile",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["compile"] is None
    assert payload["note"]["actor"] == "hermes"
    assert project_state_mod.load_project_notes("2407-casa-rio", root=tmp_path)[0]["kind"] == "risk"


def test_project_update_and_append_state_fields(tmp_path, monkeypatch):
    import arquimedes.project_state as project_state_mod

    monkeypatch.setattr(project_state_mod, "get_project_root", lambda: tmp_path)

    update = CliRunner().invoke(
        cli,
        [
            "project",
            "update",
            "2407-casa-rio",
            "--field",
            "current_work_in_progress",
            "--text",
            "Preparar mediciones",
            "--no-recompile",
        ],
    )
    append = CliRunner().invoke(
        cli,
        [
            "project",
            "append",
            "2407-casa-rio",
            "--field",
            "risks_or_blockers",
            "--text",
            "Licencia pendiente",
            "--no-recompile",
        ],
    )

    assert update.exit_code == 0
    assert append.exit_code == 0
    state = project_state_mod.load_project_state("2407-casa-rio", root=tmp_path)
    assert state["current_work_in_progress"] == ["Preparar mediciones"]
    assert state["risks_or_blockers"] == ["Licencia pendiente"]


def test_project_update_rejects_invalid_field(tmp_path, monkeypatch):
    import arquimedes.project_state as project_state_mod

    monkeypatch.setattr(project_state_mod, "get_project_root", lambda: tmp_path)

    result = CliRunner().invoke(
        cli,
        ["project", "update", "2407-casa-rio", "--field", "bogus", "--text", "x", "--no-recompile"],
    )

    assert result.exit_code != 0
    assert "field must be one of" in result.output


def test_project_recompile_calls_compile_skip_cluster(monkeypatch):
    import arquimedes.compile as compile_mod

    calls = []

    def fake_compile(config, *, skip_cluster=False, run_quick_lint=True, **_kwargs):
        calls.append((skip_cluster, run_quick_lint))
        return {"ok": True}

    monkeypatch.setattr(compile_mod, "compile_wiki", fake_compile)
    monkeypatch.setattr("arquimedes.config.load_config", lambda: {"llm": {"agent_cmd": "echo"}})

    result = CliRunner().invoke(cli, ["project", "recompile", "2407-casa-rio"])

    assert result.exit_code == 0
    assert calls == [(True, False)]
    assert json.loads(result.output)["compile"] == {"ok": True}


def test_project_section_set_writes_artifact_without_recompile(tmp_path, monkeypatch):
    import arquimedes.project_state as project_state_mod

    monkeypatch.setattr(project_state_mod, "get_project_root", lambda: tmp_path)

    result = CliRunner().invoke(
        cli,
        [
            "project",
            "section",
            "set",
            "2407-casa-rio",
            "proximo_foco",
            "--text",
            "Cerrar mediciones antes del viernes.",
            "--source-ref",
            "discord://2407/999",
            "--no-recompile",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["compile"] is None
    assert payload["section"]["protected"] is True
    assert project_state_mod.load_project_sections("2407-casa-rio", root=tmp_path)["proximo_foco"]["body"] == "Cerrar mediciones antes del viernes."


def test_project_resolve_removes_item_and_writes_note(tmp_path, monkeypatch):
    import arquimedes.project_state as project_state_mod

    monkeypatch.setattr(project_state_mod, "get_project_root", lambda: tmp_path)
    state = project_state_mod.load_project_state("2407-casa-rio", root=tmp_path)
    state["missing_information"] = ["Confirmar acometida"]
    project_state_mod.save_project_state("2407-casa-rio", state, root=tmp_path)

    result = CliRunner().invoke(
        cli,
        [
            "project",
            "resolve",
            "2407-casa-rio",
            "--item",
            "missing_information:1",
            "--note",
            "Confirmada por la ingenieria.",
            "--source-ref",
            "discord://2407/444",
            "--no-recompile",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["compile"] is None
    assert payload["resolved"]["field"] == "missing_information"
    assert payload["resolved"]["state"]["missing_information"] == []
    notes = project_state_mod.load_project_notes("2407-casa-rio", root=tmp_path)
    assert notes[-1]["kind"] == "coordination"
    assert notes[-1]["source_refs"] == ["discord://2407/444"]


def test_project_search_scopes_to_proyectos_collection(monkeypatch):
    calls = []

    class FakeResult:
        total = 1

        def to_json(self):
            return '{"total": 1}'

    def fake_search(query, **kwargs):
        calls.append((query, kwargs))
        return FakeResult()

    monkeypatch.setattr("arquimedes.search.search", fake_search)

    result = CliRunner().invoke(
        cli,
        ["project", "search", "2407-casa-rio", "licencia", "--deep", "--facet", "year=2026"],
    )

    assert result.exit_code == 0
    assert json.loads(result.output)["total"] == 1
    assert calls == [
        (
            "licencia",
            {
                "depth": 2,
                "facets": ["domain=proyectos", "year=2026"],
                "collection": "2407-casa-rio",
                "limit": 20,
                "chunk_limit": 5,
                "annotation_limit": 3,
                "figure_limit": 3,
                "concept_limit": 3,
            },
        )
    ]
