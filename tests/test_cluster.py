"""Tests for clustering prompt construction."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from datetime import datetime, timezone

import pytest
from click.testing import CliRunner

from arquimedes.cluster import (
    _BRIDGE_SYSTEM_PROMPT,
    _build_bridge_prompt,
    cluster_concepts,
    _pending_local_concept_rows,
    _pending_local_material_rows,
    bridge_cluster_fingerprint,
    is_local_clustering_stale,
    load_local_clusters,
    local_cluster_path,
    local_cluster_stamp_path,
    local_cluster_stamp_path,
    local_concept_wiki_path,
    local_cluster_fingerprint,
    normalize_local_clusters,
    _stage_bridge_packet_input,
    _pending_bridge_concept_rows,
    _pending_bridge_material_rows,
)


def test_local_cluster_paths_and_wiki_path():
    root = Path("/tmp/arq")
    assert local_cluster_path(root, "research", "papers") == root / "derived" / "collections" / "research__papers" / "local_concept_clusters.jsonl"
    assert local_cluster_stamp_path(root, "research", "papers") == root / "derived" / "collections" / "research__papers" / "local_cluster_stamp.json"
    assert local_concept_wiki_path("research", "papers", "archival-space") == "wiki/research/papers/concepts/archival-space.md"


def test_normalize_local_clusters_assigns_scoped_ids_and_schema():
    rows = normalize_local_clusters("research", "papers", [{
        "canonical_name": "Archival Space",
        "aliases": ["Archival Space", "Archive as Space"],
        "material_ids": ["m2", "m1", "m1"],
        "source_concepts": [{"material_id": "m1", "concept_name": "archival space"}],
        "confidence": 0.9,
    }])

    assert rows[0] == {
        "cluster_id": "research__papers__local_0001",
        "domain": "research",
        "collection": "papers",
        "canonical_name": "Archival Space",
        "slug": "archival-space",
        "aliases": ["Archival Space", "Archive as Space"],
        "descriptor": "",
        "material_ids": ["m1", "m2"],
        "source_concepts": [{"material_id": "m1", "concept_name": "archival space"}],
        "confidence": 0.9,
        "wiki_path": "wiki/research/papers/concepts/archival-space.md",
    }


def test_load_local_clusters_normalizes_scope_and_wiki_path(tmp_path):
    path = local_cluster_path(tmp_path, "research", "papers")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"canonical_name": "Archival Space", "aliases": [], "material_ids": ["m1"], "source_concepts": []}) + "\n", encoding="utf-8")

    rows = load_local_clusters(tmp_path)

    assert len(rows) == 1
    assert rows[0]["cluster_id"] == "research__papers__local_0001"
    assert rows[0]["domain"] == "research"
    assert rows[0]["collection"] == "papers"
    assert rows[0]["wiki_path"] == "wiki/research/papers/concepts/archival-space.md"


def test_pending_local_rows_only_include_target_collection_and_newer_materials():
    material_rows = [
        ("m1", "One", "", "[]"),
        ("m2", "Two", "", "[]"),
        ("m3", "Three", "", "[]"),
    ]
    concept_rows = [
        ("c1", "c1", "m1", "high", "[]", "[]", 0.9, "local", ""),
        ("c2", "c2", "m2", "high", "[]", "[]", 0.9, "local", ""),
        ("c3", "c3", "m3", "high", "[]", "[]", 0.9, "local", ""),
    ]
    manifest_index = {
        "m1": {"domain": "research", "collection": "papers", "ingested_at": "2026-01-01T00:00:00+00:00"},
        "m2": {"domain": "research", "collection": "papers", "ingested_at": "2026-03-01T00:00:00+00:00"},
        "m3": {"domain": "research", "collection": "books", "ingested_at": "2026-03-01T00:00:00+00:00"},
    }
    clustered_at = datetime(2026, 2, 1, tzinfo=timezone.utc)

    assert _pending_local_material_rows(material_rows, manifest_index, "research", "papers", clustered_at) == [("m2", "Two", "", "[]")]
    assert _pending_local_concept_rows(concept_rows, manifest_index, "research", "papers", clustered_at) == [("c2", "c2", "m2", "high", "[]", "[]", 0.9, "local", "")]


def test_local_cluster_staleness_and_fingerprint_are_collection_scoped(tmp_path, monkeypatch):
    import arquimedes.cluster as cluster_mod

    (tmp_path / "indexes").mkdir()
    con = sqlite3.connect(str(tmp_path / "indexes" / "search.sqlite"))
    con.execute("CREATE TABLE materials (material_id TEXT PRIMARY KEY, title TEXT, summary TEXT, keywords TEXT)")
    con.execute("CREATE TABLE concepts (concept_name TEXT, descriptor TEXT DEFAULT '', concept_key TEXT, material_id TEXT, relevance TEXT, source_pages TEXT, evidence_spans TEXT, confidence REAL, concept_type TEXT DEFAULT 'local', PRIMARY KEY (material_id, concept_type, concept_key))")
    con.execute("INSERT INTO materials VALUES ('m1', 'One', '', '[]')")
    con.execute("INSERT INTO materials VALUES ('m2', 'Two', '', '[]')")
    con.execute("INSERT INTO concepts VALUES ('c1', '', 'c1', 'm1', 'high', '[]', '[]', 0.9, 'local')")
    con.execute("INSERT INTO concepts VALUES ('c2', '', 'c2', 'm2', 'high', '[]', '[]', 0.9, 'local')")
    con.commit()
    con.close()

    (tmp_path / "manifests").mkdir()
    (tmp_path / "manifests" / "materials.jsonl").write_text(
        "\n".join([
            json.dumps({"material_id": "m1", "domain": "research", "collection": "papers", "ingested_at": "2026-01-01T00:00:00+00:00"}),
            json.dumps({"material_id": "m2", "domain": "research", "collection": "books", "ingested_at": "2026-01-01T00:00:00+00:00"}),
        ]) + "\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(cluster_mod, "get_project_root", lambda: tmp_path)

    assert is_local_clustering_stale("research", "papers") is True
    assert local_cluster_fingerprint("research", "papers")

    local_cluster_path(tmp_path, "research", "papers").parent.mkdir(parents=True, exist_ok=True)
    local_cluster_path(tmp_path, "research", "papers").write_text(json.dumps({"cluster_id": "research__papers__local_0001"}) + "\n", encoding="utf-8")
    local_cluster_stamp_path(tmp_path, "research", "papers").write_text(json.dumps({"clustered_at": "2026-02-01T00:00:00+00:00", "total_concepts": 1, "clusters": 1}), encoding="utf-8")

    assert is_local_clustering_stale("research", "papers") is False


def test_cluster_concepts_writes_collection_local_artifacts(tmp_path, monkeypatch):
    import arquimedes.cluster as cluster_mod
    import arquimedes.config as config_mod

    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "config.yaml").write_text("library_root: ~/dummy\n", encoding="utf-8")
    (tmp_path / "indexes").mkdir()
    con = sqlite3.connect(str(tmp_path / "indexes" / "search.sqlite"))
    con.execute("CREATE TABLE materials (material_id TEXT PRIMARY KEY, title TEXT, summary TEXT, keywords TEXT)")
    con.execute("CREATE TABLE concepts (concept_name TEXT, descriptor TEXT DEFAULT '', concept_key TEXT, material_id TEXT, relevance TEXT, source_pages TEXT, evidence_spans TEXT, confidence REAL, concept_type TEXT DEFAULT 'local', PRIMARY KEY (material_id, concept_type, concept_key))")
    con.execute("INSERT INTO materials VALUES ('m1', 'One', 'Summary', '[\"archive\"]')")
    con.execute("INSERT INTO materials VALUES ('m2', 'Two', 'Summary', '[\"archive\"]')")
    con.execute("INSERT INTO materials VALUES ('m3', 'Three', 'Summary', '[\"archive\"]')")
    con.execute("INSERT INTO concepts VALUES ('archival habitat', '', 'archival habitat', 'm1', 'high', '[1]', '[\"evidence\"]', 0.9, 'local')")
    con.execute("INSERT INTO concepts VALUES ('archive as space', '', 'archive as space', 'm2', 'high', '[2]', '[\"evidence\"]', 0.8, 'local')")
    con.execute("INSERT INTO concepts VALUES ('other concept', '', 'other concept', 'm3', 'high', '[3]', '[\"evidence\"]', 0.8, 'local')")
    con.commit()
    con.close()

    (tmp_path / "manifests").mkdir()
    (tmp_path / "manifests" / "materials.jsonl").write_text(
        "\n".join([
            json.dumps({"material_id": "m1", "domain": "research", "collection": "papers", "ingested_at": "2026-01-01T00:00:00+00:00"}),
            json.dumps({"material_id": "m2", "domain": "research", "collection": "papers", "ingested_at": "2026-01-01T00:00:00+00:00"}),
            json.dumps({"material_id": "m3", "domain": "research", "collection": "books", "ingested_at": "2026-01-01T00:00:00+00:00"}),
        ]) + "\n",
        encoding="utf-8",
    )

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(config_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(cluster_mod, "get_project_root", lambda: tmp_path)

    def mock_llm(_system, _messages):
        return json.dumps({
            "links_to_existing": [],
            "new_clusters": [{
                "canonical_name": "archival space",
                "descriptor": "Shared archival spatial idea.",
                "aliases": ["archival space"],
                "source_concepts": [
                    {"material_id": "m1", "concept_name": "archival habitat"},
                    {"material_id": "m2", "concept_name": "archive as space"},
                ],
            }],
            "_finished": True,
        })

    result = cluster_concepts({"llm": {"agent_cmd": "echo"}}, llm_fn=mock_llm, force=True, domain="research", collection="papers")

    assert result["collections"] == 1
    assert result["clusters"] == 1
    output = load_local_clusters(tmp_path, domain="research", collection="papers")
    assert len(output) == 1
    assert output[0]["cluster_id"] == "research__papers__local_0001"
    assert output[0]["wiki_path"] == "wiki/research/papers/concepts/archival-space.md"
    assert not local_cluster_path(tmp_path, "research", "books").exists()


def test_cluster_concepts_drops_cross_collection_membership(tmp_path, monkeypatch):
    import arquimedes.cluster as cluster_mod
    import arquimedes.config as config_mod

    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "config.yaml").write_text("library_root: ~/dummy\n", encoding="utf-8")
    (tmp_path / "indexes").mkdir()
    con = sqlite3.connect(str(tmp_path / "indexes" / "search.sqlite"))
    con.execute("CREATE TABLE materials (material_id TEXT PRIMARY KEY, title TEXT, summary TEXT, keywords TEXT)")
    con.execute("CREATE TABLE concepts (concept_name TEXT, descriptor TEXT DEFAULT '', concept_key TEXT, material_id TEXT, relevance TEXT, source_pages TEXT, evidence_spans TEXT, confidence REAL, concept_type TEXT DEFAULT 'local', PRIMARY KEY (material_id, concept_type, concept_key))")
    con.execute("INSERT INTO materials VALUES ('m1', 'One', 'Summary', '[\"archive\"]')")
    con.execute("INSERT INTO materials VALUES ('m2', 'Two', 'Summary', '[\"memory\"]')")
    con.execute("INSERT INTO concepts VALUES ('archival habitat', '', 'archival habitat', 'm1', 'high', '[1]', '[\"evidence\"]', 0.9, 'local')")
    con.execute("INSERT INTO concepts VALUES ('memory and place', '', 'memory and place', 'm2', 'high', '[2]', '[\"evidence\"]', 0.8, 'local')")
    con.commit()
    con.close()

    (tmp_path / "manifests").mkdir()
    (tmp_path / "manifests" / "materials.jsonl").write_text(
        "\n".join([
            json.dumps({"material_id": "m1", "domain": "research", "collection": "papers", "ingested_at": "2026-01-01T00:00:00+00:00"}),
            json.dumps({"material_id": "m2", "domain": "research", "collection": "books", "ingested_at": "2026-01-01T00:00:00+00:00"}),
        ]) + "\n",
        encoding="utf-8",
    )

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(config_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(cluster_mod, "get_project_root", lambda: tmp_path)

    def mock_llm(_system, _messages):
        return json.dumps({
            "links_to_existing": [],
            "new_clusters": [{
                "canonical_name": "invalid cross collection cluster",
                "aliases": ["invalid cross collection cluster"],
                "source_concepts": [
                    {"material_id": "m1", "concept_name": "archival habitat"},
                    {"material_id": "m2", "concept_name": "memory and place"},
                ],
            }],
            "_finished": True,
        })

    result = cluster_concepts({"llm": {"agent_cmd": "echo"}}, llm_fn=mock_llm, force=True, domain="research", collection="papers")

    output = load_local_clusters(tmp_path, domain="research", collection="papers")
    assert result["clusters"] == 0
    assert output == []


def test_cluster_concepts_skips_busy_collection_scope(tmp_path, monkeypatch):
    import arquimedes.cluster as cluster_mod
    import arquimedes.config as config_mod

    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "config.yaml").write_text("library_root: ~/dummy\n", encoding="utf-8")
    (tmp_path / "indexes").mkdir()
    con = sqlite3.connect(str(tmp_path / "indexes" / "search.sqlite"))
    con.execute("CREATE TABLE materials (material_id TEXT PRIMARY KEY, title TEXT, summary TEXT, keywords TEXT)")
    con.execute("CREATE TABLE concepts (concept_name TEXT, descriptor TEXT DEFAULT '', concept_key TEXT, material_id TEXT, relevance TEXT, source_pages TEXT, evidence_spans TEXT, confidence REAL, concept_type TEXT DEFAULT 'local', PRIMARY KEY (material_id, concept_type, concept_key))")
    con.execute("INSERT INTO materials VALUES ('m1', 'One', 'Summary', '[\"archive\"]')")
    con.execute("INSERT INTO concepts VALUES ('archival habitat', '', 'archival habitat', 'm1', 'high', '[1]', '[\"evidence\"]', 0.9, 'local')")
    con.commit()
    con.close()

    (tmp_path / "manifests").mkdir()
    (tmp_path / "manifests" / "materials.jsonl").write_text(
        json.dumps({"material_id": "m1", "domain": "research", "collection": "papers", "ingested_at": "2026-01-01T00:00:00+00:00"}) + "\n",
        encoding="utf-8",
    )

    gate_path = local_cluster_path(tmp_path, "research", "papers").parent / ".cluster.lock"
    gate_path.parent.mkdir(parents=True, exist_ok=True)
    gate_path.write_text("busy\n", encoding="utf-8")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(config_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(cluster_mod, "get_project_root", lambda: tmp_path)

    result = cluster_concepts({"llm": {"agent_cmd": "echo"}}, llm_fn=lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("llm should not run")), force=True, domain="research", collection="papers")

    assert result["skipped"] is True
    assert result["changed"] == 0


def test_bridge_prompt_includes_material_packets():
    packets_path = Path("/tmp/bridge_cluster_input.json")
    bridge_path = Path("/tmp/bridge_memory_input.json")
    prompt = _build_bridge_prompt(
        packets_path,
        bridge_path,
    )
    assert str(packets_path) in prompt
    assert str(bridge_path) in prompt
    assert "Read the new concepts packet file" in prompt
    assert "Read the existing bridge cluster memory file" in prompt
    assert "New clusters must connect at least two materials." in prompt
    assert "return exactly one final JSON object only when the full clustering job is complete" in prompt
    assert "Set _finished to true only in that final completed JSON object." in prompt
    assert '"_finished":true' in _BRIDGE_SYSTEM_PROMPT
    assert '"links_to_existing"' in _BRIDGE_SYSTEM_PROMPT
    assert '"new_clusters"' in _BRIDGE_SYSTEM_PROMPT
    assert '"descriptor"' in _BRIDGE_SYSTEM_PROMPT
    assert '"confidence"' not in _BRIDGE_SYSTEM_PROMPT
    assert "short descriptor" in _BRIDGE_SYSTEM_PROMPT


def test_bridge_cluster_prompt_requests_json_response():
    prompt = _build_bridge_prompt(
        Path("/tmp/bridge_cluster_input.json"),
        Path("/tmp/bridge_memory_input.json"),
    )
    assert "existing bridge cluster memory file" in prompt
    assert "JSON only" in prompt
    assert "Do not output partial JSON" in prompt


def test_bridge_packet_file_is_staged(tmp_path):
    (tmp_path / "derived").mkdir()
    (tmp_path / "indexes").mkdir()
    db_path = tmp_path / "indexes" / "search.sqlite"
    con = sqlite3.connect(str(db_path))
    con.execute("CREATE TABLE materials (material_id TEXT PRIMARY KEY, title TEXT, summary TEXT, keywords TEXT)")
    con.execute(
        "CREATE TABLE concepts (concept_name TEXT, descriptor TEXT DEFAULT '', concept_key TEXT, material_id TEXT, relevance TEXT, source_pages TEXT, evidence_spans TEXT, confidence REAL, concept_type TEXT DEFAULT 'local', PRIMARY KEY (material_id, concept_type, concept_key))"
    )
    con.execute("INSERT INTO materials VALUES (?, ?, ?, ?)", ("m1", "Archival Habitat", "Summary", '["archive"]'))
    con.execute(
        "INSERT INTO concepts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("archival habitat", "A habitat for archives.", "archival habitat", "m1", "high", "[1]", '["evidence"]', 0.9, "local"),
    )
    con.commit()
    con.close()

    con = sqlite3.connect(str(db_path))
    try:
        concept_rows = con.execute("SELECT concept_name, concept_key, material_id, relevance, source_pages, evidence_spans, confidence, concept_type, descriptor FROM concepts").fetchall()
        material_rows = con.execute("SELECT material_id, title, summary, keywords FROM materials").fetchall()
    finally:
        con.close()

    path = _stage_bridge_packet_input(tmp_path, concept_rows, material_rows)
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["kind"] == "bridge_packets"
    assert len(payload["materials"]) == 1
    packet = payload["materials"][0]
    assert packet["title"] == "Archival Habitat"
    assert set(packet["concepts"][0].keys()) == {"concept", "descriptor"}
    assert packet["concepts"][0]["descriptor"] == "A habitat for archives."
    assert packet["bridge"] == []


def test_bridge_clustering_only_stages_materials_ingested_after_cutoff(tmp_path):
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
    manifest_index = {
        "m1": {"material_id": "m1", "ingested_at": "2026-01-01T00:00:00+00:00"},
        "m2": {"material_id": "m2", "ingested_at": "2026-02-01T00:00:00+00:00"},
    }
    clustered_at = datetime(2026, 1, 15, tzinfo=timezone.utc)

    pending_materials = _pending_bridge_material_rows(material_rows, manifest_index, clustered_at)
    pending_concepts = _pending_bridge_concept_rows(concept_rows, manifest_index, clustered_at)

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
        "CREATE TABLE concepts (concept_name TEXT, descriptor TEXT DEFAULT '', concept_key TEXT, material_id TEXT, relevance TEXT, source_pages TEXT, evidence_spans TEXT, confidence REAL, concept_type TEXT DEFAULT 'local', PRIMARY KEY (material_id, concept_type, concept_key))"
    )
    con.execute("INSERT INTO materials VALUES (?, ?, ?, ?)", ("m1", "Archival Habitat", "Summary", '["archive"]'))
    con.execute("INSERT INTO materials VALUES (?, ?, ?, ?)", ("m2", "Counterarchive Practice", "Summary", '["archive"]'))
    con.execute(
        "INSERT INTO concepts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("archival habitat", "A habitat for archives.", "archival habitat", "m1", "high", "[1]", '["evidence"]', 0.9, "local"),
    )
    con.execute(
        "INSERT INTO concepts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("counterarchive practice", "An archival practice against official memory.", "counterarchive practice", "m2", "high", "[2]", '["evidence"]', 0.8, "local"),
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
        return json.dumps({
            "links_to_existing": [],
            "new_clusters": [
                {
                    "canonical_name": "archival counterpublics",
                    "descriptor": "How archival practices create shared publics across institutions and media.",
                    "aliases": ["archival counterpublics"],
                    "source_concepts": [
                        {"material_id": "m1", "concept_name": "archival habitat"},
                        {"material_id": "m2", "concept_name": "counterarchive practice"},
                    ],
                }
            ],
            "_finished": True,
        })

    result = cluster_mod.cluster_bridge_concepts({"llm": {"agent_cmd": "echo"}}, llm_fn=mock_llm, force=True)

    assert result["skipped"] is False
    assert result["clusters"] == 1
    assert llm_called
    bridge_path = tmp_path / "derived" / "bridge_concept_clusters.jsonl"
    assert bridge_path.exists()
    cluster_lines = bridge_path.read_text(encoding="utf-8").splitlines()
    assert len(cluster_lines) == 1
    payload = json.loads(cluster_lines[0])
    assert payload["canonical_name"] == "archival counterpublics"
    assert payload["descriptor"] == "How archival practices create shared publics across institutions and media."
    assert sorted(payload["material_ids"]) == ["m1", "m2"]


def test_bridge_clustering_links_packet_concepts_to_existing_cluster(tmp_path, monkeypatch):
    import arquimedes.cluster as cluster_mod
    import arquimedes.config as config_mod

    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "config.yaml").write_text("library_root: ~/dummy\n", encoding="utf-8")
    (tmp_path / "derived").mkdir()
    (tmp_path / "derived" / "bridge_concept_clusters.jsonl").write_text(
        json.dumps({
            "cluster_id": "bridge_0001",
            "canonical_name": "archival publics",
            "slug": "archival-publics",
            "aliases": ["archival publics"],
            "material_ids": ["m1"],
            "source_concepts": [{"material_id": "m1", "concept_name": "archival habitat"}],
            "confidence": 0.9,
        }) + "\n",
        encoding="utf-8",
    )
    (tmp_path / "indexes").mkdir()
    db_path = tmp_path / "indexes" / "search.sqlite"
    con = sqlite3.connect(str(db_path))
    con.execute("CREATE TABLE materials (material_id TEXT PRIMARY KEY, title TEXT, summary TEXT, keywords TEXT)")
    con.execute(
        "CREATE TABLE concepts (concept_name TEXT, descriptor TEXT DEFAULT '', concept_key TEXT, material_id TEXT, relevance TEXT, source_pages TEXT, evidence_spans TEXT, confidence REAL, concept_type TEXT DEFAULT 'local', PRIMARY KEY (material_id, concept_type, concept_key))"
    )
    con.execute("INSERT INTO materials VALUES (?, ?, ?, ?)", ("m1", "Archival Habitat", "Summary", '["archive"]'))
    con.execute("INSERT INTO materials VALUES (?, ?, ?, ?)", ("m2", "Counterarchive Practice", "Summary", '["archive"]'))
    con.execute(
        "INSERT INTO concepts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("archival habitat", "A habitat for archives.", "archival habitat", "m1", "high", "[1]", '["evidence"]', 0.9, "local"),
    )
    con.execute(
        "INSERT INTO concepts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("counterarchive practice", "An archival practice against official memory.", "counterarchive practice", "m2", "high", "[2]", '["evidence"]', 0.8, "local"),
    )
    con.commit()
    con.close()

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(config_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(config_mod, "load_config", lambda: {"llm": {"agent_cmd": "echo"}})
    monkeypatch.setattr(cluster_mod, "get_project_root", lambda: tmp_path)

    def mock_llm(system, messages):
        return json.dumps({
            "links_to_existing": [
                {
                    "cluster_id": "bridge_0001",
                    "source_concepts": [
                        {"material_id": "m2", "concept_name": "counterarchive practice"}
                    ],
                }
            ],
            "new_clusters": [],
            "_finished": True,
        })

    result = cluster_mod.cluster_bridge_concepts({"llm": {"agent_cmd": "echo"}}, llm_fn=mock_llm, force=True)

    assert result["clusters"] == 1
    cluster_lines = (tmp_path / "derived" / "bridge_concept_clusters.jsonl").read_text(encoding="utf-8").splitlines()
    payload = json.loads(cluster_lines[0])
    assert payload["canonical_name"] == "archival publics"
    assert sorted(payload["material_ids"]) == ["m1", "m2"]


def test_bridge_clustering_force_ignores_incremental_cutoff(tmp_path, monkeypatch):
    import arquimedes.cluster as cluster_mod
    import arquimedes.config as config_mod

    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "config.yaml").write_text("library_root: ~/dummy\n", encoding="utf-8")
    (tmp_path / "derived").mkdir()
    (tmp_path / "derived" / "bridge_concept_clusters.jsonl").write_text("", encoding="utf-8")
    (tmp_path / "derived" / "bridge_cluster_stamp.json").write_text(
        json.dumps({
            "clustered_at": "2026-03-01T00:00:00+00:00",
            "fingerprint": "abc123",
            "bridge_concepts": 2,
            "clusters": 1,
        }),
        encoding="utf-8",
    )
    (tmp_path / "manifests").mkdir()
    (tmp_path / "manifests" / "materials.jsonl").write_text(
        "\n".join(
            [
                json.dumps({"material_id": "m1", "ingested_at": "2026-01-01T00:00:00+00:00"}),
                json.dumps({"material_id": "m2", "ingested_at": "2026-01-02T00:00:00+00:00"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (tmp_path / "indexes").mkdir()
    db_path = tmp_path / "indexes" / "search.sqlite"
    con = sqlite3.connect(str(db_path))
    con.execute("CREATE TABLE materials (material_id TEXT PRIMARY KEY, title TEXT, summary TEXT, keywords TEXT)")
    con.execute(
        "CREATE TABLE concepts (concept_name TEXT, descriptor TEXT DEFAULT '', concept_key TEXT, material_id TEXT, relevance TEXT, source_pages TEXT, evidence_spans TEXT, confidence REAL, concept_type TEXT DEFAULT 'local', PRIMARY KEY (material_id, concept_type, concept_key))"
    )
    con.execute("INSERT INTO materials VALUES (?, ?, ?, ?)", ("m1", "Archival Habitat", "Summary", '[\"archive\"]'))
    con.execute("INSERT INTO materials VALUES (?, ?, ?, ?)", ("m2", "Counterarchive Practice", "Summary", '[\"archive\"]'))
    con.execute(
        "INSERT INTO concepts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("archival habitat", "A habitat for archives.", "archival habitat", "m1", "high", "[1]", '[\"evidence\"]', 0.9, "local"),
    )
    con.execute(
        "INSERT INTO concepts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("counterarchive practice", "An archival practice against official memory.", "counterarchive practice", "m2", "high", "[2]", '[\"evidence\"]', 0.8, "local"),
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
        return json.dumps({
            "links_to_existing": [],
            "new_clusters": [
                {
                    "canonical_name": "archival counterpublics",
                    "aliases": ["archival counterpublics"],
                    "source_concepts": [
                        {"material_id": "m1", "concept_name": "archival habitat"},
                        {"material_id": "m2", "concept_name": "counterarchive practice"},
                    ],
                }
            ],
            "_finished": True,
        })

    result = cluster_mod.cluster_bridge_concepts({"llm": {"agent_cmd": "echo"}}, llm_fn=mock_llm, force=True)

    assert result["skipped"] is False
    assert result["clusters"] == 1
    assert llm_called


def test_bridge_fingerprint_only_tracks_uncovered_materials(tmp_path, monkeypatch):
    import arquimedes.cluster as cluster_mod
    import arquimedes.config as config_mod

    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "config.yaml").write_text("library_root: ~/dummy\n", encoding="utf-8")
    (tmp_path / "derived").mkdir()
    (tmp_path / "manifests").mkdir()
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

    (tmp_path / "manifests" / "materials.jsonl").write_text(
        "\n".join(
            [
                json.dumps({
                    "material_id": "m1",
                    "ingested_at": "2026-02-01T00:00:00+00:00",
                }),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (tmp_path / "derived" / "bridge_cluster_stamp.json").write_text(
        json.dumps({
            "clustered_at": "2026-01-01T00:00:00+00:00",
            "fingerprint": "abc123",
            "bridge_concepts": 1,
            "clusters": 1,
        }),
        encoding="utf-8",
    )

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(config_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(config_mod, "load_config", lambda: {"llm": {"agent_cmd": "echo"}})
    monkeypatch.setattr(cluster_mod, "get_project_root", lambda: tmp_path)

    fp1 = bridge_cluster_fingerprint()

    (tmp_path / "derived" / "bridge_concept_clusters.jsonl").write_text(
        "\n".join(
            [
                json.dumps({
                    "cluster_id": "bridge_0001",
                    "canonical_name": "Counterarchive",
                    "slug": "counterarchive",
                    "material_ids": ["m2"],
                    "source_concepts": [{"material_id": "m2", "concept_name": "counterarchive"}],
                })
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    fp2 = bridge_cluster_fingerprint()
    assert fp1 == fp2


def test_cluster_cli_runs_local_collection_pass(tmp_path, monkeypatch):
    import arquimedes.cli as cli_mod
    import arquimedes.cluster as cluster_mod
    import arquimedes.config as config_mod

    calls = []

    def mock_cluster(config, *, force=False, llm_fn=None, llm_state=None, domain=None, collection=None):
        calls.append(("local", force, llm_state is not None, domain, collection))
        return {"total_concepts": 0, "clusters": 0, "multi_material": 0, "collections": 0}

    monkeypatch.setattr(config_mod, "load_config", lambda: {"llm": {"agent_cmd": "echo"}})
    monkeypatch.setattr(cluster_mod, "cluster_concepts", mock_cluster)

    runner = CliRunner()
    result = runner.invoke(cli_mod.cli, ["cluster"])

    assert result.exit_code == 0
    assert calls == [("local", False, True, None, None)]
    assert "Local:" in result.output


def test_cluster_cli_force_sets_local_force(tmp_path, monkeypatch):
    import arquimedes.cli as cli_mod
    import arquimedes.cluster as cluster_mod
    import arquimedes.config as config_mod

    calls = []

    def mock_cluster(config, *, force=False, llm_fn=None, llm_state=None, domain=None, collection=None):
        calls.append(("local", force, llm_state is not None, domain, collection))
        return {"total_concepts": 0, "clusters": 0, "multi_material": 0, "collections": 0}

    monkeypatch.setattr(config_mod, "load_config", lambda: {"llm": {"agent_cmd": "echo"}})
    monkeypatch.setattr(cluster_mod, "cluster_concepts", mock_cluster)

    runner = CliRunner()
    result = runner.invoke(cli_mod.cli, ["cluster", "--force"])

    assert result.exit_code == 0
    assert calls == [("local", True, True, None, None)]
    assert "Local:" in result.output


def test_cluster_logs_failed_outcome(tmp_path, monkeypatch):
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
        "CREATE TABLE concepts (concept_name TEXT, descriptor TEXT DEFAULT '', concept_key TEXT, material_id TEXT, relevance TEXT, source_pages TEXT, evidence_spans TEXT, confidence REAL, concept_type TEXT DEFAULT 'local', PRIMARY KEY (material_id, concept_type, concept_key))"
    )
    con.execute("INSERT INTO materials VALUES (?, ?, ?, ?)", ("m1", "Archival Habitat", "Summary", '[\"archive\"]'))
    con.execute(
        "INSERT INTO concepts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("archival habitat", "A habitat for archives.", "archival habitat", "m1", "high", "[1]", '[\"evidence\"]', 0.9, "local"),
    )
    con.commit()
    con.close()

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(config_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(config_mod, "load_config", lambda: {"llm": {"agent_cmd": "echo"}})
    monkeypatch.setattr(cluster_mod, "get_project_root", lambda: tmp_path)

    def failing_llm(system, messages):
        raise cluster_mod.EnrichmentError("boom")

    with pytest.raises(cluster_mod.EnrichmentError, match="boom"):
        cluster_mod.cluster_bridge_concepts({"llm": {"agent_cmd": "echo"}}, llm_fn=failing_llm, force=True)

    log_lines = (tmp_path / "logs" / "cluster.log").read_text(encoding="utf-8").splitlines()
    assert len(log_lines) == 2
    assert "\tSTART\tbridge\tTrue" in log_lines[0]
    assert "\tFAILED\tboom" in log_lines[1]
    assert "DONE" not in log_lines[1]
