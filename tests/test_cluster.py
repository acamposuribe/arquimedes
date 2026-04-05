"""Tests for clustering prompt construction."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest
from click.testing import CliRunner

from arquimedes.cluster import (
    _BRIDGE_SYSTEM_PROMPT,
    _build_bridge_prompt,
    _stage_bridge_packet_input,
    _pending_bridge_concept_rows,
    _pending_bridge_material_rows,
)


def test_bridge_prompt_includes_material_packets():
    packets_path = Path("/tmp/bridge_cluster_input.json")
    bridge_path = Path("/tmp/bridge_concept_clusters.jsonl")
    output_path = Path("/tmp/bridge_clusters.json")
    prompt = _build_bridge_prompt(
        packets_path,
        bridge_path,
        output_path,
    )
    assert str(packets_path) in prompt
    assert str(bridge_path) in prompt
    assert str(output_path) in prompt
    assert "Read the bridge packet file" in prompt
    assert "Read the current bridge memory file" in prompt
    assert "preserve the existing bridge concepts" in prompt.lower()
    assert "Bridge clusters must connect at least two materials." in prompt


def test_bridge_cluster_prompt_requests_file_write():
    prompt = _build_bridge_prompt(
        Path("/tmp/bridge_cluster_input.json"),
        Path("/tmp/bridge_concept_clusters.jsonl"),
        Path("/tmp/bridge_clusters.json"),
    )
    assert "Write the updated bridge clusters" in prompt


def test_bridge_packet_file_is_staged(tmp_path):
    (tmp_path / "derived").mkdir()
    (tmp_path / "indexes").mkdir()
    db_path = tmp_path / "indexes" / "search.sqlite"
    con = sqlite3.connect(str(db_path))
    con.execute("CREATE TABLE materials (material_id TEXT PRIMARY KEY, title TEXT, summary TEXT, keywords TEXT)")
    con.execute(
        "CREATE TABLE concepts (concept_name TEXT, concept_key TEXT, material_id TEXT, relevance TEXT, source_pages TEXT, evidence_spans TEXT, confidence REAL, concept_type TEXT DEFAULT 'local', PRIMARY KEY (material_id, concept_type, concept_key))"
    )
    con.execute("INSERT INTO materials VALUES (?, ?, ?, ?)", ("m1", "Archival Habitat", "Summary", '["archive"]'))
    con.execute(
        "INSERT INTO concepts VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ("archival habitat", "archival habitat", "m1", "high", "[1]", '["evidence"]', 0.9, "local"),
    )
    con.commit()
    con.close()

    con = sqlite3.connect(str(db_path))
    try:
        concept_rows = con.execute("SELECT concept_name, concept_key, material_id, relevance, source_pages, evidence_spans, confidence, concept_type FROM concepts").fetchall()
        material_rows = con.execute("SELECT material_id, title, summary, keywords FROM materials").fetchall()
    finally:
        con.close()

    path = _stage_bridge_packet_input(tmp_path, concept_rows, material_rows)
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["kind"] == "bridge_packets"
    assert payload["material_packet_count"] == 1
    assert payload["material_packets"][0]["title"] == "Archival Habitat"


def test_bridge_clustering_only_stages_uncovered_materials(tmp_path):
    existing = [
        {
            "cluster_id": "bridge_0001",
            "canonical_name": "archival habitat",
            "aliases": ["archival habitat"],
            "material_ids": ["m1"],
            "source_concepts": [{"material_id": "m1", "concept_name": "archival habitat"}],
            "confidence": 0.9,
        }
    ]
    material_rows = [
        ("m1", "Archival Habitat", "Summary", '["archive"]'),
        ("m2", "Counterarchive", "Summary", '["archive"]'),
    ]
    concept_rows = [
        ("archival habitat", "archival habitat", "m1", "high", "[1]", '["evidence"]', 0.9, "bridge_candidate"),
        ("counterarchive", "counterarchive", "m2", "high", "[2]", '["evidence"]', 0.9, "bridge_candidate"),
    ]

    pending_materials = _pending_bridge_material_rows(material_rows, existing)
    pending_concepts = _pending_bridge_concept_rows(concept_rows, existing)

    assert pending_materials == [("m2", "Counterarchive", "Summary", '["archive"]')]
    assert pending_concepts == [("counterarchive", "counterarchive", "m2", "high", "[2]", '["evidence"]', 0.9, "bridge_candidate")]


def test_bridge_clustering_uses_bridge_packets(tmp_path, monkeypatch):
    import arquimedes.cluster as cluster_mod
    import arquimedes.config as config_mod

    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "config.yaml").write_text("library_root: ~/dummy\n", encoding="utf-8")
    (tmp_path / "derived").mkdir()
    (tmp_path / "derived" / "bridge_concept_clusters.jsonl").write_text("", encoding="utf-8")
    (tmp_path / "indexes").mkdir()
    db_path = tmp_path / "indexes" / "search.sqlite"
    con = sqlite3.connect(str(db_path))
    con.execute("CREATE TABLE materials (material_id TEXT PRIMARY KEY, title TEXT, summary TEXT, keywords TEXT)")
    con.execute(
        "CREATE TABLE concepts (concept_name TEXT, concept_key TEXT, material_id TEXT, relevance TEXT, source_pages TEXT, evidence_spans TEXT, confidence REAL, concept_type TEXT DEFAULT 'local', PRIMARY KEY (material_id, concept_type, concept_key))"
    )
    con.execute("INSERT INTO materials VALUES (?, ?, ?, ?)", ("m1", "Archival Habitat", "Summary", '["archive"]'))
    con.execute(
        "INSERT INTO concepts VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ("archival habitat", "archival habitat", "m1", "high", "[1]", '["evidence"]', 0.9, "local"),
    )
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


def test_cluster_cli_runs_bridge_pass(tmp_path, monkeypatch):
    import arquimedes.cli as cli_mod
    import arquimedes.cluster as cluster_mod
    import arquimedes.config as config_mod

    calls = []

    def mock_bridge(config, *, force=False, llm_fn=None, llm_state=None):
        calls.append(("bridge", force, llm_state is not None))
        return {"bridge_concepts": 0, "clusters": 0, "multi_material": 0}

    monkeypatch.setattr(config_mod, "load_config", lambda: {"llm": {"agent_cmd": "echo"}})
    monkeypatch.setattr(cluster_mod, "cluster_bridge_concepts", mock_bridge)

    runner = CliRunner()
    result = runner.invoke(cli_mod.cli, ["cluster"])

    assert result.exit_code == 0
    assert calls == [("bridge", False, True)]
    assert "Bridge:" in result.output


def test_cluster_cli_force_sets_bridge_force(tmp_path, monkeypatch):
    import arquimedes.cli as cli_mod
    import arquimedes.cluster as cluster_mod
    import arquimedes.config as config_mod

    calls = []

    def mock_bridge(config, *, force=False, llm_fn=None, llm_state=None):
        calls.append(("bridge", force, llm_state is not None))
        return {"bridge_concepts": 0, "clusters": 0, "multi_material": 0}

    monkeypatch.setattr(config_mod, "load_config", lambda: {"llm": {"agent_cmd": "echo"}})
    monkeypatch.setattr(cluster_mod, "cluster_bridge_concepts", mock_bridge)

    runner = CliRunner()
    result = runner.invoke(cli_mod.cli, ["cluster", "--force"])

    assert result.exit_code == 0
    assert calls == [("bridge", True, True)]
    assert "Bridge:" in result.output
