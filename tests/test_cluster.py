"""Tests for clustering prompt construction."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest
from click.testing import CliRunner

from arquimedes.cluster import (
    _BRIDGE_SYSTEM_PROMPT,
    _LOCAL_SYSTEM_PROMPT,
    _build_bridge_prompt,
    _build_prompt,
    _cluster_input_path,
    _stage_local_cluster_input,
)


def test_cluster_prompt_includes_semantic_merge_guidance():
    input_path = Path("/tmp/local_cluster_input.json")
    prompt = _build_prompt(input_path, 1)
    assert "Merge only when the concepts are semantically equivalent" in prompt
    assert "Do not group multiple distinct concepts from the same material into one umbrella cluster." in prompt
    assert str(input_path) in prompt
    assert "Read the local clustering input JSON" in prompt
    assert "confidence=0.9" not in prompt
    assert "relevance=high" not in prompt
    assert "Do not emit singleton clusters" in _LOCAL_SYSTEM_PROMPT
    assert "backfilled deterministically" in _LOCAL_SYSTEM_PROMPT


def test_bridge_prompt_includes_material_packets():
    local_path = Path("/tmp/local_clusters.json")
    bridge_path = Path("/tmp/bridge_concept_clusters.jsonl")
    output_path = Path("/tmp/bridge_clusters.json")
    prompt = _build_bridge_prompt(
        local_path,
        bridge_path,
        output_path,
    )
    assert str(local_path) in prompt
    assert str(bridge_path) in prompt
    assert str(output_path) in prompt
    assert "Read the local cluster file" in prompt
    assert "Read the current bridge memory file" in prompt
    assert "preserve the existing bridge concepts" in prompt.lower()
    assert "Bridge clusters must connect at least two materials." in prompt


def test_local_cluster_prompt_requests_file_write():
    from arquimedes.cluster import _LOCAL_SYSTEM_PROMPT, _build_prompt, _cluster_output_path

    prompt = _build_prompt(Path("/tmp/local_cluster_input.json"), 1)
    assert "Write the output JSON directly to" not in prompt
    assert "Do not emit singleton clusters" in _LOCAL_SYSTEM_PROMPT
    assert _cluster_output_path(Path("/tmp"), "local").name == "local_clusters.json"


def test_bridge_cluster_prompt_requests_file_write():
    prompt = _build_bridge_prompt(
        Path("/tmp/local_clusters.json"),
        Path("/tmp/bridge_concept_clusters.jsonl"),
        Path("/tmp/bridge_clusters.json"),
    )
    assert "Write the updated bridge clusters" in prompt


def test_cluster_input_files_are_staged(tmp_path):
    local_path = _stage_local_cluster_input(
        tmp_path,
        [
            (
                "archival habitat",
                "archival habitat",
                "m1",
                "high",
                "[1]",
                '["Archives are spatial infrastructures."]',
                0.9,
                "local",
            )
        ],
        {"m1": "Archival Habitat"},
    )
    assert local_path == _cluster_input_path(tmp_path, "local")
    local_payload = json.loads(local_path.read_text(encoding="utf-8"))
    assert local_payload["kind"] == "local"
    assert local_payload["concept_count"] == 1
    assert local_payload["concepts"][0]["material_title"] == "Archival Habitat"


def test_bridge_clustering_uses_local_clusters(tmp_path, monkeypatch):
    import arquimedes.cluster as cluster_mod
    import arquimedes.config as config_mod

    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "config.yaml").write_text("library_root: ~/dummy\n", encoding="utf-8")
    (tmp_path / "derived").mkdir()
    (tmp_path / "derived" / "concept_clusters.jsonl").write_text(
        json.dumps({
            "cluster_id": "concept_0001",
            "canonical_name": "archival habitat",
            "slug": "archival-habitat",
            "aliases": ["archival habitat"],
            "material_ids": ["m1"],
            "source_concepts": [{
                "material_id": "m1",
                "concept_name": "archival habitat",
                "relevance": "high",
                "source_pages": [1],
                "evidence_spans": ["evidence"],
                "confidence": 0.9,
            }],
            "confidence": 0.9,
        }) + "\n",
        encoding="utf-8",
    )
    (tmp_path / "derived" / "bridge_concept_clusters.jsonl").write_text("", encoding="utf-8")
    (tmp_path / "indexes").mkdir()
    db_path = tmp_path / "indexes" / "search.sqlite"
    con = sqlite3.connect(str(db_path))
    con.execute("CREATE TABLE materials (material_id TEXT PRIMARY KEY, title TEXT, summary TEXT, keywords TEXT)")
    con.commit()
    con.close()

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(config_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(config_mod, "load_config", lambda: {"llm": {"agent_cmd": "echo"}})
    monkeypatch.setattr(cluster_mod, "get_project_root", lambda: tmp_path)

    llm_called = []

    def mock_llm(system, messages):
        llm_called.append((system, messages))
        return "[]"

    result = cluster_mod.cluster_bridge_concepts({"llm": {"agent_cmd": "echo"}}, llm_fn=mock_llm, force=True)

    assert result["skipped"] is False
    assert llm_called
    bridge_path = tmp_path / "derived" / "bridge_concept_clusters.jsonl"
    assert bridge_path.exists()


def test_cluster_cli_defaults_to_both_passes(tmp_path, monkeypatch):
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
    assert calls == [("local", False, True), ("bridge", False, True)]
    assert "Local:" in result.output
    assert "Bridge:" in result.output


def test_cluster_cli_local_only_skips_bridge(tmp_path, monkeypatch):
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
    result = runner.invoke(cli_mod.cli, ["cluster", "--local-only"])

    assert result.exit_code == 0
    assert calls == [("local", False, True)]
    assert "Local:" in result.output
    assert "Bridge:" not in result.output
