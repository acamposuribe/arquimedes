from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from fastapi.testclient import TestClient

import arquimedes.read as read_mod
import arquimedes.serve as serve_mod
from arquimedes.search import ChunkHit, ConceptHit, MaterialEvidence


def _write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")


def _repo(tmp_path, monkeypatch):
    (tmp_path / "Library" / "Research").mkdir(parents=True)
    (tmp_path / "Library" / "Practice").mkdir(parents=True)
    (tmp_path / "wiki" / "research").mkdir(parents=True)
    (tmp_path / "wiki" / "research" / "papers").mkdir(parents=True)
    (tmp_path / "wiki" / "practice").mkdir(parents=True)
    (tmp_path / "wiki" / "practice" / "projects").mkdir(parents=True)
    (tmp_path / "extracted" / "mat_001" / "figures").mkdir(parents=True)
    (tmp_path / "indexes").mkdir()
    monkeypatch.setattr(read_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(read_mod, "get_library_root", lambda: tmp_path / "Library")
    monkeypatch.setattr(read_mod, "get_index_path", lambda: tmp_path / "indexes" / "search.sqlite")
    monkeypatch.setattr(serve_mod.read_mod, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(serve_mod.read_mod, "get_library_root", lambda: tmp_path / "Library")
    monkeypatch.setattr(serve_mod.read_mod, "get_index_path", lambda: tmp_path / "indexes" / "search.sqlite")
    monkeypatch.setattr(serve_mod, "get_index_path", lambda: tmp_path / "indexes" / "search.sqlite")
    return tmp_path


def test_health_endpoint(tmp_path, monkeypatch):
    _repo(tmp_path, monkeypatch)
    client = TestClient(serve_mod.create_app())
    assert client.get("/health").json() == {"ok": True}


def test_home_handles_missing_index(tmp_path, monkeypatch):
    root = _repo(tmp_path, monkeypatch)
    (root / "wiki" / "shared" / "glossary").mkdir(parents=True)
    (root / "wiki" / "shared" / "glossary" / "_index.md").write_text("- [Alpha (main)](wiki/research/bridge-concepts/alpha.md)\n", encoding="utf-8")
    client = TestClient(serve_mod.create_app())
    response = client.get("/")
    assert response.status_code == 200
    assert "arq index rebuild" in response.text
    assert "/wiki/research/bridge-concepts/alpha" in response.text
    assert "Alpha" in response.text


def test_home_page_only_shows_enabled_domain_tabs(tmp_path, monkeypatch):
    _repo(tmp_path, monkeypatch)
    monkeypatch.setattr(
        serve_mod.read_mod,
        "list_domains_and_collections",
        lambda domain=None: [{"domain": domain or "research", "collection": "papers"}],
    )
    monkeypatch.setattr(serve_mod.read_mod, "recent_materials", lambda limit=10, domain=None: [])

    client = TestClient(serve_mod.create_app({"domains": {"enabled": ["research", "proyectos"]}}))
    response = client.get("/")

    assert response.status_code == 200
    assert 'href="/?domain=research"' in response.text
    assert 'href="/?domain=proyectos"' in response.text
    assert 'href="/?domain=practice"' not in response.text


def test_home_page_scopes_domain_navigation(tmp_path, monkeypatch):
    root = _repo(tmp_path, monkeypatch)
    (root / "wiki" / "shared" / "glossary").mkdir(parents=True)
    (root / "wiki" / "shared" / "glossary" / "_index.md").write_text(
        "- [Research Alpha (main)](wiki/research/bridge-concepts/research-alpha.md)\n"
        "- [Practice Beta (main)](wiki/practice/bridge-concepts/practice-beta.md)\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        serve_mod.read_mod,
        "list_domains_and_collections",
        lambda domain=None: (
            [{"domain": "research", "collection": "papers"}]
            if domain == "research"
            else [{"domain": "practice", "collection": "projects"}]
            if domain == "practice"
            else [
                {"domain": "practice", "collection": "projects"},
                {"domain": "research", "collection": "papers"},
            ]
        ),
    )
    monkeypatch.setattr(
        serve_mod.read_mod,
        "recent_materials",
        lambda limit=10, domain=None: [
            {
                "material_id": f"{domain or 'research'}_001",
                "title": f"{(domain or 'research').title()} Material",
                "summary": f"{domain or 'research'} summary",
                "domain": domain or "research",
                "collection": "projects" if domain == "practice" else "papers",
                "document_type": "note",
                "year": "2026",
            }
        ],
    )
    client = TestClient(serve_mod.create_app())
    response = client.get("/?domain=practice")
    assert response.status_code == 200
    assert 'href="/?domain=practice" class="active"' in response.text
    assert "Práctica Materiales recientes" in response.text
    assert '<div class="home-grid">' not in response.text
    assert '<ul class="wiki-list">' not in response.text
    assert "/wiki/practice/projects" in response.text
    assert "/wiki/research/papers" not in response.text
    assert "/wiki/practice/bridge-concepts/practice-beta" in response.text
    assert "/wiki/research/bridge-concepts/research-alpha" not in response.text
    assert 'name="domain" type="hidden" value="practice"' in response.text


def test_home_page_shows_random_figure_discovery_before_recent_materials(tmp_path, monkeypatch):
    root = _repo(tmp_path, monkeypatch)
    con = sqlite3.connect(str(root / "indexes" / "search.sqlite"))
    con.execute("CREATE TABLE materials (material_id TEXT, title TEXT, summary TEXT, domain TEXT, collection TEXT, document_type TEXT, year TEXT)")
    con.execute("CREATE TABLE figures (figure_id TEXT, material_id TEXT, description TEXT, caption TEXT, visual_type TEXT, source_page INTEGER, relevance TEXT, image_path TEXT)")
    con.execute("INSERT INTO materials VALUES (?,?,?,?,?,?,?)", ("mat_001", "Material One", "Summary", "research", "papers", "paper", "2024"))
    con.execute("INSERT INTO figures VALUES (?,?,?,?,?,?,?,?)", ("fig_0001", "mat_001", "Plan description", "Plan caption", "plan", 1, "substantive", "figures/fig_0001.png"))
    con.commit()
    con.close()
    (root / "extracted" / "mat_001" / "figures" / "fig_0001.png").write_bytes(b"png")

    client = TestClient(serve_mod.create_app())
    response = client.get("/?domain=research")
    assert response.status_code == 200
    assert 'class="home-figure-grid"' in response.text
    assert 'href="/materials/mat_001"' in response.text
    assert 'src="/figures-low/mat_001/fig_0001.png"' in response.text
    assert response.text.index('class="home-figure-grid"') < response.text.index("Research Recent Materials")


def test_proyectos_home_renders_project_dashboard_not_recent_materials(tmp_path, monkeypatch):
    root = _repo(tmp_path, monkeypatch)
    monkeypatch.setattr(
        serve_mod.read_mod,
        "list_domains_and_collections",
        lambda domain=None: [{"domain": "proyectos", "collection": "2511-gandia"}] if domain == "proyectos" else [],
    )
    monkeypatch.setattr(
        serve_mod.read_mod,
        "materials_for_collection",
        lambda domain, collection: [
            {"material_id": "mat_001", "title": "Acta"},
            {"material_id": "mat_002", "title": "Foto"},
        ],
    )
    monkeypatch.setattr(
        serve_mod.read_mod,
        "recent_materials",
        lambda limit=10, domain=None: [
            {
                "material_id": "mat_001",
                "title": "WhatsApp Image",
                "summary": "Confusing recent material",
                "domain": "proyectos",
                "collection": "2511-gandia",
                "document_type": "site_photo",
                "year": "2026",
            }
        ],
    )
    _write_json(root / "derived" / "projects" / "2511-gandia" / "project_state.json", {
        "domain": "proyectos",
        "project_id": "2511-gandia",
        "project_title": "2511 (35) Edificio Gandia",
        "stage": "basic_project",
        "stage_confidence": 0.8,
        "last_material_ids": [],
        "main_objectives": [],
        "current_work_in_progress": ["Cerrar levantamiento del estado actual."],
        "next_focus": ["Preparar consulta municipal."],
        "known_conditions": [],
        "decisions": [],
        "requirements": [],
        "risks_or_blockers": ["Medianera pendiente de revisión."],
        "missing_information": [],
        "positive_learnings": [],
        "mistakes_or_regrets": [],
        "repair_actions": [],
        "important_material_ids": [],
        "updated_at": "2026-05-01T08:00:00+00:00",
        "updated_by": "human",
    })

    client = TestClient(serve_mod.create_app())
    response = client.get("/?domain=proyectos")
    assert response.status_code == 200
    assert "2511 (35) Edificio Gandia" in response.text
    assert "Cerrar levantamiento del estado actual." in response.text
    assert "Preparar consulta municipal." in response.text
    assert "Medianera pendiente de revisión." in response.text
    assert "2 materiales" in response.text
    assert "WhatsApp Image" not in response.text
    assert "Materiales recientes" not in response.text


def test_material_route_rewrites_links(tmp_path, monkeypatch):
    root = _repo(tmp_path, monkeypatch)
    _write_json(root / "extracted" / "mat_001" / "meta.json", {
        "material_id": "mat_001",
        "title": "Material One",
        "domain": "research",
        "collection": "papers",
        "source_path": "Research/mat_001.pdf",
    })
    (root / "Library" / "Research" / "mat_001.pdf").write_text("pdf", encoding="utf-8")
    (root / "extracted" / "mat_001" / "text.md").write_text("Extracted", encoding="utf-8")
    (root / "extracted" / "mat_001" / "figures" / "fig_0001.png").write_bytes(b"png")
    (root / "extracted" / "mat_001" / "thumbnails").mkdir()
    (root / "extracted" / "mat_001" / "thumbnails" / "page_0001.png").write_bytes(b"png")
    (root / "extracted" / "mat_001" / "pages.jsonl").write_text(json.dumps({"page_number": 1, "thumbnail_path": "thumbnails/page_0001.png"}) + "\n", encoding="utf-8")
    _write_json(root / "extracted" / "mat_001" / "figures" / "fig_0001.json", {"figure_id": "fig_0001", "image_path": "figures/fig_0001.png", "caption": "Gallery caption"})
    (root / "wiki" / "research" / "papers" / "mat_001.md").write_text(
        "[Concept](../../../wiki/shared/concepts/archive.md)\n\n"
        "[Open original file](file:///tmp/source.pdf)\n\n"
        "[Full extracted text](../../../extracted/mat_001/text.md)\n\n"
        "## Figures\n\n"
        "**fig_0001**\n"
        "![Fig](figures/fig_0001.png)\n\n"
        "> Gallery caption\n",
        encoding="utf-8",
    )
    client = TestClient(serve_mod.create_app())
    response = client.get("/materials/mat_001")
    assert response.status_code == 200
    assert '/wiki/shared/concepts/archive' in response.text
    assert response.text.count('/source/mat_001') == 1
    assert response.text.count('/extracted/mat_001/text') == 1
    assert '/figures-low/mat_001/fig_0001.png' in response.text
    assert 'data-zoom-src="/thumbnails/mat_001/page_0001.png"' in response.text
    assert 'data-zoom-group="figures"' in response.text
    assert '<blockquote>\n<p>Gallery caption</p>\n</blockquote>' not in response.text
    assert 'aria-label="Search within page"' in response.text


def test_material_route_moves_related_materials_to_end(tmp_path, monkeypatch):
    root = _repo(tmp_path, monkeypatch)
    _write_json(root / "extracted" / "mat_001" / "meta.json", {
        "material_id": "mat_001",
        "title": "Material One",
        "domain": "research",
        "collection": "papers",
    })
    (root / "wiki" / "research" / "papers" / "mat_001.md").write_text(
        "# Material One\n\n"
        "## Related Materials\n\n"
        "- [Other](other.md)\n\n"
        "## Source\n\n"
        "**Pages:** 10\n",
        encoding="utf-8",
    )
    client = TestClient(serve_mod.create_app())
    response = client.get("/materials/mat_001")
    assert response.status_code == 200
    assert response.text.index(">Source</h2>") < response.text.index(">Related Materials</h2>")
    assert 'class="related-materials-list"' in response.text


def test_material_route_renders_figures_before_related_materials(tmp_path, monkeypatch):
    root = _repo(tmp_path, monkeypatch)
    _write_json(root / "extracted" / "mat_001" / "meta.json", {
        "material_id": "mat_001",
        "title": "Material One",
        "domain": "research",
        "collection": "papers",
    })
    _write_json(root / "extracted" / "mat_001" / "figures" / "fig_0001.json", {
        "figure_id": "fig_0001",
        "image_path": "figures/fig_0001.png",
        "caption": "Caption",
    })
    (root / "extracted" / "mat_001" / "figures" / "fig_0001.png").write_bytes(b"png")
    (root / "wiki" / "research" / "papers" / "mat_001.md").write_text(
        "# Material One\n\n"
        "## Figures\n\n"
        "**fig_0001**\n"
        "![Fig](figures/fig_0001.png)\n\n"
        "## Related Materials\n\n"
        "- [Other](other.md)\n\n"
        "## Source\n\n"
        "**Pages:** 10\n",
        encoding="utf-8",
    )
    client = TestClient(serve_mod.create_app())
    response = client.get("/materials/mat_001")
    assert response.status_code == 200
    assert response.text.index(">Figures</h2>") < response.text.index(">Related Materials</h2>")


def test_material_route_marks_metadata_table_for_friendly_ui(tmp_path, monkeypatch):
    root = _repo(tmp_path, monkeypatch)
    _write_json(root / "extracted" / "mat_001" / "meta.json", {
        "material_id": "mat_001",
        "title": "Material One",
        "domain": "research",
        "collection": "papers",
    })
    (root / "wiki" / "research" / "papers" / "mat_001.md").write_text(
        "# Material One\n\n"
        "## Metadata\n\n"
        "| Field | Value |\n"
        "| --- | --- |\n"
        "| Authors | Someone |\n",
        encoding="utf-8",
    )
    client = TestClient(serve_mod.create_app())
    response = client.get("/materials/mat_001")
    assert response.status_code == 200
    assert 'class="metadata-table"' in response.text


def test_wiki_directory_listing_route(tmp_path, monkeypatch):
    root = _repo(tmp_path, monkeypatch)
    (root / "wiki" / "research" / "papers" / "concepts").mkdir()
    (root / "wiki" / "research" / "papers" / "mat_001.md").write_text("Page", encoding="utf-8")
    client = TestClient(serve_mod.create_app())
    response = client.get("/wiki/research/papers")
    assert response.status_code == 200
    assert "Directories" in response.text
    assert "/wiki/research/papers/concepts" in response.text


def test_wiki_root_scopes_active_domain(tmp_path, monkeypatch):
    root = _repo(tmp_path, monkeypatch)
    (root / "wiki" / "practice" / "_index.md").write_text("# Practice Wiki\n\nPractice root body.\n", encoding="utf-8")
    (root / "wiki" / "research" / "_index.md").write_text("# Research Wiki\n\nResearch root body.\n", encoding="utf-8")
    client = TestClient(serve_mod.create_app())
    response = client.get("/wiki?domain=practice")
    assert response.status_code == 200
    assert "Practice root body." in response.text
    assert "Research root body." not in response.text
    assert 'href="/wiki/practice" class="active"' in response.text


def test_wiki_material_page_rewrites_figure_links(tmp_path, monkeypatch):
    root = _repo(tmp_path, monkeypatch)
    _write_json(root / "extracted" / "mat_001" / "meta.json", {
        "material_id": "mat_001",
        "title": "Material One",
        "domain": "research",
        "collection": "papers",
        "source_path": "Research/mat_001.pdf",
    })
    (root / "wiki" / "research" / "papers" / "mat_001.md").write_text("![Fig](figures/fig_0001.png)\n", encoding="utf-8")
    (root / "extracted" / "mat_001" / "figures" / "fig_0001.png").write_bytes(b"png")
    (root / "extracted" / "mat_001" / "thumbnails").mkdir()
    (root / "extracted" / "mat_001" / "thumbnails" / "page_0001.png").write_bytes(b"png")
    (root / "extracted" / "mat_001" / "pages.jsonl").write_text(json.dumps({"page_number": 1, "thumbnail_path": "thumbnails/page_0001.png"}) + "\n", encoding="utf-8")
    client = TestClient(serve_mod.create_app())
    response = client.get("/wiki/research/papers/mat_001")
    assert response.status_code == 200
    assert '/figures-low/mat_001/fig_0001.png' in response.text
    assert 'data-zoom-src="/thumbnails/mat_001/page_0001.png"' in response.text


def test_material_page_collapses_long_page_thumbnail_strip(tmp_path, monkeypatch):
    root = _repo(tmp_path, monkeypatch)
    _write_json(root / "extracted" / "mat_001" / "meta.json", {
        "material_id": "mat_001",
        "title": "Material One",
        "domain": "research",
        "collection": "papers",
    })
    thumbs_dir = root / "extracted" / "mat_001" / "thumbnails"
    thumbs_dir.mkdir()
    pages = []
    for page in range(1, 22):
        filename = f"page_{page:04d}.png"
        (thumbs_dir / filename).write_bytes(b"png")
        pages.append(json.dumps({"page_number": page, "thumbnail_path": f"thumbnails/{filename}"}))
    (root / "extracted" / "mat_001" / "pages.jsonl").write_text("\n".join(pages) + "\n", encoding="utf-8")
    (root / "wiki" / "research" / "papers" / "mat_001.md").write_text("# Material One\n", encoding="utf-8")

    client = TestClient(serve_mod.create_app())
    response = client.get("/materials/mat_001")
    assert response.status_code == 200
    assert '<details class="thumb-details">' in response.text
    assert "<span>Pages</span>" in response.text
    assert '<span class="meta-line">(21)</span>' in response.text
    assert 'data-zoom-src="/thumbnails/mat_001/page_0021.png"' in response.text


def test_figure_and_source_routes(tmp_path, monkeypatch):
    root = _repo(tmp_path, monkeypatch)
    _write_json(root / "extracted" / "mat_001" / "meta.json", {
        "material_id": "mat_001",
        "title": "Material One",
        "domain": "research",
        "collection": "papers",
        "source_path": "Research/mat_001.pdf",
    })
    (root / "Library" / "Research" / "mat_001.pdf").write_text("pdf", encoding="utf-8")
    (root / "extracted" / "mat_001" / "figures" / "fig_0001.png").write_bytes(b"png")
    (root / "extracted" / "mat_001" / "thumbnails").mkdir()
    (root / "extracted" / "mat_001" / "thumbnails" / "page_0001.png").write_bytes(b"png")
    client = TestClient(serve_mod.create_app())
    assert client.get("/source/mat_001").status_code == 200
    assert client.get("/figures/mat_001/fig_0001.png").status_code == 200
    assert client.get("/figures-low/mat_001/fig_0001.png").status_code == 200
    assert client.get("/thumbnails/mat_001/page_0001.png").status_code == 200


def test_figures_page_exposes_zoom_targets(tmp_path, monkeypatch):
    root = _repo(tmp_path, monkeypatch)
    _write_json(root / "extracted" / "mat_001" / "meta.json", {"material_id": "mat_001", "title": "Material One"})
    _write_json(root / "extracted" / "mat_001" / "figures" / "fig_0001.json", {"figure_id": "fig_0001", "image_path": "figures/fig_0001.png", "caption": "Caption", "description": "Description", "source_page": 4, "visual_type": "plan"})
    (root / "extracted" / "mat_001" / "figures" / "fig_0001.png").write_bytes(b"png")
    client = TestClient(serve_mod.create_app())
    response = client.get("/materials/mat_001/figures")
    assert response.status_code == 200
    assert 'data-zoom-group="figures"' in response.text
    assert 'data-zoom-src="/figures/mat_001/fig_0001.png"' in response.text
    assert 'src="/figures-low/mat_001/fig_0001.png"' in response.text
    assert 'data-zoom-caption="Caption"' in response.text
    assert 'data-zoom-meta="p. 4 · plan"' in response.text


def test_search_route_renders_results(tmp_path, monkeypatch):
    root = _repo(tmp_path, monkeypatch)
    con = sqlite3.connect(str(root / "indexes" / "search.sqlite"))
    con.execute("CREATE TABLE materials (material_id TEXT, title TEXT, summary TEXT, domain TEXT, collection TEXT, document_type TEXT, year TEXT)")
    con.execute("INSERT INTO materials VALUES (?,?,?,?,?,?,?)", ("mat_001", "Material One", "Summary", "research", "papers", "paper", "2024"))
    con.commit()
    con.close()

    captured = {}

    class Result:
        query = "material"
        total = 1
        canonical_clusters = []
        results = [type("Card", (), {
            "material_id": "mat_001",
            "title": "Material One",
            "summary": "Summary",
            "domain": "research",
            "collection": "papers",
            "document_type": "paper",
            "year": "2024",
            "concepts": [type("Concept", (), {"concept_name": "Musee Imaginaire"})()],
            "chunks": [type("Chunk", (), {"source_pages": [7], "summary": "The Musee Imaginaire frames architecture as a portable archive."})()],
        })()]

    def _fake_search(*args, **kwargs):
        captured["depth"] = kwargs.get("depth")
        captured["scope"] = kwargs.get("scope")
        return Result()

    monkeypatch.setattr(serve_mod.search_mod, "search", _fake_search)
    client = TestClient(serve_mod.create_app())
    response = client.get("/search?q=material&scope=author")
    assert response.status_code == 200
    assert captured["depth"] == 3
    assert captured["scope"] == "author"
    assert "Material One" in response.text
    assert "Matching passages" not in response.text
    assert "Musee Imaginaire" in response.text
    assert "/materials/mat_001?q=material&amp;depth=3&amp;scope=author" in response.text
    assert "/wiki/research/papers" in response.text


def test_search_scopes_active_domain(tmp_path, monkeypatch):
    _repo(tmp_path, monkeypatch)
    captured = {}

    class Result:
        query = "material"
        total = 0
        collection_pages = []
        canonical_clusters = []
        results = []

    def _fake_search(*args, **kwargs):
        captured["facets"] = kwargs.get("facets")
        return Result()

    monkeypatch.setattr(serve_mod.search_mod, "search", _fake_search)
    client = TestClient(serve_mod.create_app())
    response = client.get("/search?q=material&domain=practice&facet=year%3D%3D2024")
    assert response.status_code == 200
    assert captured["facets"] == ["year==2024", "domain==practice"]
    assert "Práctica dominio" in response.text
    assert 'name="domain" type="hidden" value="practice"' in response.text


def test_search_route_renders_collection_and_concept_sections(tmp_path, monkeypatch):
    _repo(tmp_path, monkeypatch)

    class Result:
        query = "archive"
        total = 1
        collection_pages = [
            type("Collection", (), {
                "domain": "research",
                "collection": "papers",
                "title": "Research / Papers",
                "wiki_path": "wiki/research/papers/_index.md",
                "material_count": 4,
                "summary": "This collection treats architecture as archival imagination.",
            })()
        ]
        canonical_clusters = [
            type("Cluster", (), {
                "canonical_name": "Archival Habitat Cluster",
                "wiki_path": "wiki/research/papers/concepts/archival-habitat-cluster.md",
                "material_count": 2,
                "summary": "Architecture can act as a living archive.",
                "domain": "research",
                "collection": "papers",
            })()
        ]
        results = []

    monkeypatch.setattr(serve_mod.search_mod, "search", lambda *args, **kwargs: Result())
    client = TestClient(serve_mod.create_app())
    response = client.get("/search?q=archive&scope=all")
    assert response.status_code == 200
    assert "Collections" in response.text
    assert "Concepts" in response.text
    assert "Research / Papers" in response.text
    assert "Archival Habitat Cluster" in response.text


def test_update_and_freshness_endpoints(tmp_path, monkeypatch):
    _repo(tmp_path, monkeypatch)
    monkeypatch.setattr(serve_mod.freshness_mod, "workspace_freshness_status", lambda: {"message": "Up to date"})
    monkeypatch.setattr(serve_mod.freshness_mod, "update_workspace", lambda: {"message": "Updated", "pull_result": "ok"})
    client = TestClient(serve_mod.create_app())
    assert client.get("/api/freshness").json()["message"] == "Up to date"
    assert client.post("/update").json()["message"] == "Updated"


def test_public_exposure_unregisters_mutating_routes(tmp_path, monkeypatch):
    _repo(tmp_path, monkeypatch)
    config = {"serve": {"public_exposure": True, "allowed_hosts": ["vault.example.com"]}}
    client = TestClient(serve_mod.create_app(config), base_url="http://vault.example.com")
    assert client.get("/api/freshness").status_code == 404
    assert client.post("/update").status_code == 404
    assert client.get("/health").json() == {"ok": True}


def test_public_exposure_enforces_trusted_host(tmp_path, monkeypatch):
    _repo(tmp_path, monkeypatch)
    config = {"serve": {"public_exposure": True, "allowed_hosts": ["vault.example.com"]}}
    client = TestClient(serve_mod.create_app(config), base_url="http://attacker.example.com")
    assert client.get("/health").status_code == 400


def test_public_exposure_requires_allowed_hosts(tmp_path, monkeypatch):
    _repo(tmp_path, monkeypatch)
    import pytest
    with pytest.raises(RuntimeError, match="allowed_hosts"):
        serve_mod.create_app({"serve": {"public_exposure": True}})


def test_public_exposure_hides_freshness_banner(tmp_path, monkeypatch):
    root = _repo(tmp_path, monkeypatch)
    (root / "wiki" / "shared" / "glossary").mkdir(parents=True)
    (root / "wiki" / "shared" / "glossary" / "_index.md").write_text("", encoding="utf-8")
    config = {"serve": {"public_exposure": True, "allowed_hosts": ["vault.example.com"]}}
    client = TestClient(serve_mod.create_app(config), base_url="http://vault.example.com")
    response = client.get("/")
    assert response.status_code == 200
    assert "freshness-banner" not in response.text


def test_default_serve_keeps_freshness_banner(tmp_path, monkeypatch):
    root = _repo(tmp_path, monkeypatch)
    (root / "wiki" / "shared" / "glossary").mkdir(parents=True)
    (root / "wiki" / "shared" / "glossary" / "_index.md").write_text("", encoding="utf-8")
    client = TestClient(serve_mod.create_app())
    response = client.get("/")
    assert "freshness-banner" in response.text


def test_material_route_renders_search_hits(tmp_path, monkeypatch):
    root = _repo(tmp_path, monkeypatch)
    _write_json(root / "extracted" / "mat_001" / "meta.json", {
        "material_id": "mat_001",
        "title": "Material One",
        "domain": "research",
        "collection": "papers",
        "source_path": "Research/mat_001.pdf",
    })
    (root / "Library" / "Research" / "mat_001.pdf").write_text("pdf", encoding="utf-8")
    (root / "extracted" / "mat_001" / "text.md").write_text("Extracted", encoding="utf-8")
    (root / "wiki" / "research" / "papers" / "mat_001.md").write_text("Material body", encoding="utf-8")
    monkeypatch.setattr(
        serve_mod.search_mod,
        "search_material_evidence",
        lambda *args, **kwargs: MaterialEvidence(
            query="Musee Imaginaire",
            material_id="mat_001",
            depth=3,
            chunks=[
                ChunkHit(
                    chunk_id="chk_001",
                    summary="Portable archive framing.",
                    source_pages=[7],
                    emphasized=False,
                    content_class="argument",
                    rank=1,
                    text="The Musee Imaginaire frames architecture as a portable archive of images.",
                )
            ],
            concepts=[
                ConceptHit(
                    concept_name="Musee Imaginaire",
                    relevance="high",
                    source_pages=[7],
                    evidence_spans=["Musee Imaginaire"],
                    confidence=1.0,
                    rank=1,
                )
            ],
        ),
    )
    client = TestClient(serve_mod.create_app())
    response = client.get("/materials/mat_001?q=Musee%20Imaginaire&depth=3")
    assert response.status_code == 200
    assert "Search hits for" in response.text
    assert "Portable archive framing." in response.text
    assert "<mark>Musee</mark> <mark>Imaginaire</mark>" in response.text
    assert "Concept matches" in response.text
    assert 'aria-label="Clear search"' in response.text
    assert 'href="/materials/mat_001"' in response.text


def test_collection_page_uses_collection_search_label(tmp_path, monkeypatch):
    root = _repo(tmp_path, monkeypatch)
    (root / "wiki" / "research" / "papers" / "_index.md").write_text("Collection body", encoding="utf-8")
    captured = {}

    class Result:
        query = "archive"
        total = 1
        collection_pages = []
        canonical_clusters = []
        results = [type("Card", (), {
            "material_id": "mat_001",
            "title": "Material One",
            "summary": "Summary",
            "domain": "research",
            "collection": "papers",
            "document_type": "paper",
            "year": "2024",
            "concepts": [],
            "chunks": [type("Chunk", (), {"source_pages": [3], "summary": "Archival framing."})()],
        })()]

    def _fake_search(*args, **kwargs):
        captured["facet"] = kwargs.get("facets")
        captured["collection"] = kwargs.get("collection")
        return Result()

    monkeypatch.setattr(serve_mod.search_mod, "search", _fake_search)
    client = TestClient(serve_mod.create_app())
    response = client.get("/wiki/research/papers?q=archive")
    assert response.status_code == 200
    assert "Search This Collection" in response.text
    assert "Search This Material" not in response.text
    assert 'aria-label="Clear search"' in response.text
    assert 'href="/wiki/research/papers"' in response.text
    assert captured["collection"] == "papers"
    assert captured["facet"] == ["domain==research"]


def test_practice_material_page_localizes_ui_and_parses_spanish_sections(tmp_path, monkeypatch):
    root = _repo(tmp_path, monkeypatch)
    _write_json(root / "extracted" / "mat_001" / "meta.json", {
        "material_id": "mat_001",
        "title": "Material Uno",
        "domain": "practice",
        "collection": "projects",
        "source_path": "Practice/mat_001.pdf",
    })
    (root / "Library" / "Practice" / "mat_001.pdf").write_text("pdf", encoding="utf-8")
    (root / "extracted" / "mat_001" / "text.md").write_text("Extracted", encoding="utf-8")
    _write_json(root / "extracted" / "mat_001" / "figures" / "fig_0001.json", {
        "figure_id": "fig_0001",
        "image_path": "figures/fig_0001.png",
        "caption": "Detalle útil",
    })
    (root / "extracted" / "mat_001" / "figures" / "fig_0001.png").write_bytes(b"png")
    (root / "wiki" / "practice" / "projects" / "mat_001.md").write_text(
        "# Material Uno\n\n"
        "## Metadatos\n\n"
        "| Campo | Valor |\n"
        "| --- | --- |\n"
        "| Autores | Alguien |\n\n"
        "## Materiales relacionados\n\n"
        "- [Otro](otro.md)\n\n"
        "## Fuente\n\n"
        "[Abrir archivo original](file:///tmp/source.pdf)\n\n"
        "[Texto extraído completo](../../../extracted/mat_001/text.md)\n\n"
        "## Figuras\n\n"
        "**fig_0001**\n"
        "![Fig](figures/fig_0001.png)\n\n",
        encoding="utf-8",
    )
    client = TestClient(serve_mod.create_app())
    response = client.get("/materials/mat_001")
    assert response.status_code == 200
    assert 'lang="es"' in response.text
    assert "Navegación" in response.text
    assert "Inicio" in response.text
    assert "Buscar" in response.text
    assert "Acciones" in response.text
    assert "Abrir archivo fuente" in response.text
    assert "Texto extraído" in response.text
    assert "Figuras" in response.text
    assert "Materiales relacionados" in response.text
    assert 'class="metadata-table"' in response.text
    assert 'class="related-materials-list"' in response.text
    assert response.text.index(">Fuente</h2>") < response.text.index(">Materiales relacionados</h2>")


def test_proyectos_material_page_surfaces_project_extraction(tmp_path, monkeypatch):
    root = _repo(tmp_path, monkeypatch)
    (root / "wiki" / "proyectos" / "2511-gandia").mkdir(parents=True)
    _write_json(root / "extracted" / "mat_project" / "meta.json", {
        "material_id": "mat_project",
        "title": "Acta de seguimiento",
        "domain": "proyectos",
        "collection": "2511-gandia",
        "year": "2026",
        "authors": ["Alejandro Campos"],
        "project_extraction": {
            "project_material_type": "meeting_report",
            "project_relevance": "Registra acuerdos y próximos pasos del expediente.",
            "main_points": ["Se revisa el estado de licencia."],
            "decisions": ["Enviar documentación actualizada."],
            "requirements": ["Incorporar medición de medianera."],
            "risks_or_blockers": ["Licencia pendiente."],
            "open_items": ["Confirmar respuesta municipal."],
            "actors": ["Ayuntamiento"],
            "dates_and_deadlines": ["2026-05-15"],
            "spatial_or_design_scope": ["Medianera norte"],
            "budget_signals": ["Revisar coste de demolición."],
            "evidence_refs": ["p. 1"],
        },
    })
    (root / "wiki" / "proyectos" / "2511-gandia" / "mat_project.md").write_text(
        "# Acta de seguimiento\n\n"
        "## Metadatos\n\n"
        "| Campo | Valor |\n| --- | --- |\n| Año | 2026 |\n\n"
        "## Resumen\n\n"
        "Resumen duplicado.\n\n"
        "Cuerpo del acta.\n\n"
        "## Fuente\n\n"
        "**Páginas:** 2\n",
        encoding="utf-8",
    )

    client = TestClient(serve_mod.create_app())
    response = client.get("/materials/mat_project")
    assert response.status_code == 200
    assert "Resumen operativo" in response.text
    assert "Acta de seguimiento" in response.text
    assert "Informes de reunión" in response.text
    assert "Año" in response.text
    assert "2026" in response.text
    assert "Tipo" in response.text
    assert "Proyecto" in response.text
    assert "2511-gandia" in response.text
    assert "Autor" in response.text
    assert "Alejandro Campos" in response.text
    assert "Lectura de proyecto" not in response.text
    assert "Resumen duplicado." not in response.text
    assert "Cuerpo del acta." not in response.text
    assert "Páginas" not in response.text
    assert response.text.index("Metadatos") < response.text.index("Buscar en este material")
    assert "Registra acuerdos y próximos pasos del expediente." in response.text
    assert "Puntos principales" in response.text
    assert "Se revisa el estado de licencia." in response.text
    assert "Decisiones" in response.text
    assert "Enviar documentación actualizada." in response.text
    assert "Requisitos" in response.text
    assert "Riesgos y bloqueos" in response.text
    assert "Licencia pendiente." in response.text
    assert "Pendientes" in response.text
    assert "Confirmar respuesta municipal." in response.text
    assert "Referencias de evidencia" in response.text
    assert "p. 1" in response.text


def test_proyectos_standalone_image_renders_before_operational_summary(tmp_path, monkeypatch):
    root = _repo(tmp_path, monkeypatch)
    (root / "wiki" / "proyectos" / "2511-gandia").mkdir(parents=True)
    _write_json(root / "extracted" / "mat_image" / "meta.json", {
        "material_id": "mat_image",
        "title": "Foto de fachada",
        "domain": "proyectos",
        "collection": "2511-gandia",
        "file_type": "image",
        "project_extraction": {
            "project_material_type": "site_photo",
            "project_relevance": "Documenta el estado actual de fachada.",
        },
    })
    _write_json(root / "extracted" / "mat_image" / "figures" / "fig_0001.json", {
        "figure_id": "fig_0001",
        "image_path": "figures/fig_0001.jpg",
        "caption": "Fachada principal",
    })
    (root / "extracted" / "mat_image" / "figures" / "fig_0001.jpg").write_bytes(b"fake jpg")
    (root / "wiki" / "proyectos" / "2511-gandia" / "mat_image.md").write_text(
        "# Foto de fachada\n",
        encoding="utf-8",
    )

    client = TestClient(serve_mod.create_app())
    response = client.get("/materials/mat_image")
    assert response.status_code == 200
    assert 'class="project-primary-image-block"' in response.text
    assert '/figures-low/mat_image/fig_0001.jpg' in response.text
    assert 'Fachada principal' in response.text
    assert response.text.index('class="project-primary-image-block"') < response.text.index("Resumen operativo")


def test_collection_page_renders_material_cards_with_word_truncation_and_previews(tmp_path, monkeypatch):
    root = _repo(tmp_path, monkeypatch)
    _write_json(root / "extracted" / "mat_001" / "meta.json", {
        "material_id": "mat_001",
        "title": "Material One",
        "domain": "research",
        "collection": "papers",
        "summary": {"value": " ".join(f"word{i:02d}" for i in range(1, 51))},
        "document_type": {"value": "paper"},
        "year": "2024",
    })
    _write_json(root / "extracted" / "mat_001" / "figures" / "fig_0001.json", {
        "figure_id": "fig_0001",
        "image_path": "figures/fig_0001.png",
        "caption": "Preview image",
    })
    (root / "extracted" / "mat_001" / "figures" / "fig_0001.png").write_bytes(b"png")
    (root / "wiki" / "research" / "papers" / "_index.md").write_text(
        "# Research / Papers\n\n"
        "## Overview\n\n"
        "Intro text.\n\n"
        "## Materials\n\n"
        "- [Material One](mat_001.md) — old summary\n\n"
        "## Key Concepts\n\n"
        "- [Archive](concepts/archive.md)\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        serve_mod.read_mod,
        "materials_for_collection",
        lambda domain, collection: [{"material_id": "mat_001", "title": "Material One"}] if (domain, collection) == ("research", "papers") else [],
    )
    client = TestClient(serve_mod.create_app())
    response = client.get("/wiki/research/papers")
    assert response.status_code == 200
    assert 'class="material-card"' in response.text
    assert "word44..." in response.text
    assert "word45" not in response.text
    assert '/figures-low/mat_001/fig_0001.png' in response.text
    assert response.text.index(">Materials</h2>") < response.text.index(">Key Concepts</h2>")


def test_proyectos_project_page_handles_resolved_wiki_paths(tmp_path, monkeypatch):
    real_root = tmp_path / "real"
    alias_root = tmp_path / "alias"
    real_root.mkdir()
    try:
        alias_root.symlink_to(real_root, target_is_directory=True)
    except OSError:
        alias_root = real_root

    _repo(alias_root, monkeypatch)
    (real_root / "wiki" / "proyectos" / "2407-casa-rio").mkdir(parents=True)
    (real_root / "wiki" / "proyectos" / "2407-casa-rio" / "_index.md").write_text(
        "# Casa Río\n\n"
        "## Estado del proyecto\n\n"
        "En marcha.\n\n"
        "## Materiales importantes\n\n"
        "- [Acta de arranque](mat_proyecto.md)\n\n"
        "## Historial reciente\n\n"
        "- [Acta de arranque](mat_proyecto.md) (2026-04-30)\n",
        encoding="utf-8",
    )
    (real_root / "wiki" / "proyectos" / "2407-casa-rio" / "mat_proyecto.md").write_text(
        "# Acta de arranque\n",
        encoding="utf-8",
    )
    _write_json(real_root / "extracted" / "mat_proyecto" / "meta.json", {
        "material_id": "mat_proyecto",
        "title": "Acta de arranque",
        "domain": "proyectos",
        "collection": "2407-casa-rio",
        "summary": {"value": "Reunión inicial del proyecto."},
        "project_extraction": {"project_material_type": {"value": "meeting_report"}},
    })
    con = sqlite3.connect(str(real_root / "indexes" / "search.sqlite"))
    con.execute("CREATE TABLE materials (material_id TEXT, title TEXT, summary TEXT, domain TEXT, collection TEXT, document_type TEXT, year TEXT)")
    con.execute("INSERT INTO materials VALUES (?,?,?,?,?,?,?)", ("mat_proyecto", "Acta de arranque", "Reunión inicial.", "proyectos", "2407-casa-rio", "acta", "2026"))
    con.commit()
    con.close()

    client = TestClient(serve_mod.create_app())
    response = client.get("/wiki/proyectos/2407-casa-rio")
    assert response.status_code == 200
    assert "Casa Río" in response.text
    assert "Informes de reunión" in response.text
    assert "/materials/mat_proyecto" in response.text
    assert response.text.index(">Materiales del proyecto</h2>") < response.text.index(">Historial reciente</h2>")


def test_concept_page_uses_concept_search_label_and_linked_material_scope(tmp_path, monkeypatch):
    root = _repo(tmp_path, monkeypatch)
    (root / "wiki" / "research" / "papers" / "concepts").mkdir(parents=True, exist_ok=True)
    concept_path = root / "wiki" / "research" / "papers" / "concepts" / "archival-habitat.md"
    concept_path.write_text("Concept body", encoding="utf-8")
    captured = {}

    class Result:
        query = "archive"
        total = 0
        collection_pages = []
        canonical_clusters = []
        results = []

    monkeypatch.setattr(
        serve_mod.read_mod,
        "wiki_page_record",
        lambda path: {"page_type": "concept", "page_id": "cluster_001", "path": "wiki/research/papers/concepts/archival-habitat.md"},
    )
    monkeypatch.setattr(
        serve_mod.read_mod,
        "materials_for_concept",
        lambda cluster_id: [{"material_id": "mat_001", "title": "Material One"}] if cluster_id == "cluster_001" else [],
    )

    def _fake_search(*args, **kwargs):
        captured["material_ids"] = kwargs.get("material_ids")
        return Result()

    monkeypatch.setattr(serve_mod.search_mod, "search", _fake_search)
    client = TestClient(serve_mod.create_app())
    response = client.get("/wiki/research/papers/concepts/archival-habitat?q=archive")
    assert response.status_code == 200
    assert "Search This Concept" in response.text
    assert "Search This Material" not in response.text
    assert captured["material_ids"] == ["mat_001"]


def test_concept_page_falls_back_to_markdown_linked_materials(tmp_path, monkeypatch):
    root = _repo(tmp_path, monkeypatch)
    _write_json(root / "extracted" / "mat_001" / "meta.json", {
        "material_id": "mat_001",
        "title": "Material One",
        "domain": "research",
        "collection": "papers",
    })
    (root / "wiki" / "research" / "papers" / "mat_001.md").write_text("Material body", encoding="utf-8")
    concept_path = root / "wiki" / "research" / "papers" / "concepts" / "archival-habitat.md"
    concept_path.parent.mkdir(parents=True, exist_ok=True)
    concept_path.write_text(
        "# Archival Habitat\n\n## By Material\n\n- [Material One](../mat_001.md)\n",
        encoding="utf-8",
    )
    captured = {}

    class Result:
        query = "archive"
        total = 0
        collection_pages = []
        canonical_clusters = []
        results = []

    monkeypatch.setattr(serve_mod.read_mod, "wiki_page_record", lambda path: None)

    def _fake_search(*args, **kwargs):
        captured["material_ids"] = kwargs.get("material_ids")
        return Result()

    monkeypatch.setattr(serve_mod.search_mod, "search", _fake_search)
    client = TestClient(serve_mod.create_app())
    response = client.get("/wiki/research/papers/concepts/archival-habitat?q=archive")
    assert response.status_code == 200
    assert "Search This Concept" in response.text
    assert captured["material_ids"] == ["mat_001"]
