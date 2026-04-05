"""Tests for clustering prompt construction."""

from __future__ import annotations

import json
import sqlite3

import pytest
from click.testing import CliRunner

from arquimedes.cluster import _build_bridge_prompt, _build_prompt


def test_cluster_prompt_includes_semantic_merge_guidance():
    rows = [
        (
            "archival habitat",
            "archival habitat",
            "m1",
            "high",
            "[1]",
            '["Archives are spatial infrastructures.", "Habitation gives archives social form."]',
            0.9,
            "local",
        )
    ]
    prompt = _build_prompt(rows, {"m1": "Archival Habitat"})
    assert "Merge only when the concepts are semantically equivalent" in prompt
    assert "Do not group multiple distinct concepts from the same material into one umbrella cluster." in prompt
    assert 'concept_name="archival habitat"' in prompt
    assert "confidence=0.9" in prompt
    assert "Archives are spatial infrastructures." in prompt
    assert "Habitation gives archives social form." in prompt


def test_bridge_prompt_includes_material_packets():
    prompt = _build_bridge_prompt([
        {
            "material_id": "m1",
            "title": "Archival Habitat",
            "summary": "A material about archival space.",
            "keywords": ["archive", "space"],
            "local_concepts": [{"concept_name": "archival habitat", "relevance": "high"}],
            "bridge_candidates": [{"concept_name": "archival spatiality", "relevance": "high"}],
            "evidence_snippets": ["Archives are spatial infrastructures."],
        },
        {
            "material_id": "m2",
            "title": "Archival Landscapes",
            "summary": "Another material about archives.",
            "keywords": ["archive"],
            "local_concepts": [{"concept_name": "archive as space", "relevance": "high"}],
            "bridge_candidates": [{"concept_name": "archival spatiality", "relevance": "high"}],
            "evidence_snippets": ["Habitation gives archives social form."],
        },
    ])
    assert 'material="Archival Habitat" [m1]' in prompt
    assert 'material="Archival Landscapes" [m2]' in prompt
    assert "bridge candidate" in prompt or "bridge_candidates" in prompt
    assert "Bridge clusters must connect at least two materials." in prompt


def test_bridge_clustering_skips_without_candidates(tmp_path, monkeypatch):
    import arquimedes.cluster as cluster_mod
    import arquimedes.config as config_mod

    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "config.yaml").write_text("library_root: ~/dummy\n", encoding="utf-8")
    (tmp_path / "indexes").mkdir()
    db_path = tmp_path / "indexes" / "search.sqlite"
    con = sqlite3.connect(str(db_path))
    con.execute("CREATE TABLE materials (material_id TEXT PRIMARY KEY, title TEXT, summary TEXT, keywords TEXT)")
    con.execute(
        "CREATE TABLE concepts (concept_name TEXT, concept_key TEXT, material_id TEXT, concept_type TEXT DEFAULT 'local', relevance TEXT, source_pages TEXT, evidence_spans TEXT, confidence REAL, PRIMARY KEY (material_id, concept_type, concept_key))"
    )
    con.execute("INSERT INTO materials VALUES (?, ?, ?, ?)", ("m1", "Material One", "Summary", "[]"))
    con.execute(
        "INSERT INTO concepts VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ("archival habitat", "archival habitat", "m1", "local", "high", "[1]", '["evidence"]', 0.9),
    )
    con.commit()
    con.close()

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(config_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(config_mod, "load_config", lambda: {"llm": {"agent_cmd": "echo"}})
    monkeypatch.setattr(cluster_mod, "get_project_root", lambda: tmp_path)

    llm_called = []

    def mock_llm(system, messages):
        llm_called.append(True)
        return "[]"

    result = cluster_mod.cluster_bridge_concepts({"llm": {"agent_cmd": "echo"}}, llm_fn=mock_llm, force=True)

    assert result["skipped"] is True
    assert llm_called == []
    bridge_path = tmp_path / "derived" / "bridge_concept_clusters.jsonl"
    assert bridge_path.exists()
    assert bridge_path.read_text(encoding="utf-8") == ""


def test_cluster_cli_defaults_to_local_only(tmp_path, monkeypatch):
    import arquimedes.cli as cli_mod
    import arquimedes.cluster as cluster_mod
    import arquimedes.config as config_mod

    calls = []

    def mock_local(config, *, force=False, llm_fn=None, llm_state=None):
        calls.append(("local", force, llm_state is not None))
        return {"total_concepts": 1, "clusters": 1, "multi_material": 0}

    def mock_bridge(config, *, force=False, llm_fn=None, llm_state=None):
        calls.append(("bridge", force, llm_state is not None))
        return {"bridge_concepts": 0, "clusters": 0, "multi_material": 0}

    monkeypatch.setattr(config_mod, "load_config", lambda: {"llm": {"agent_cmd": "echo"}})
    monkeypatch.setattr(cluster_mod, "cluster_concepts", mock_local)
    monkeypatch.setattr(cluster_mod, "cluster_bridge_concepts", mock_bridge)

    runner = CliRunner()
    result = runner.invoke(cli_mod.cli, ["cluster"])

    assert result.exit_code == 0
    assert calls == [("local", False, True)]
    assert "Local:" in result.output
    assert "Bridge:" not in result.output
