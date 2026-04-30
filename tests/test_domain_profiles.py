from __future__ import annotations

from arquimedes.domain_profiles import (
    display_domain_name,
    domain_prompt_version,
    generated_label,
    get_publication_mode,
    is_proyectos_domain,
    should_run_clustering,
    should_run_collection_reflection,
    should_run_global_bridge,
    should_run_office_learning,
    should_run_project_reflection,
)


def test_proyectos_profile_flags_and_labels():
    assert display_domain_name("proyectos") == "Proyectos"
    assert generated_label("riesgos", "proyectos") == "Problemas, riesgos y bloqueos"
    assert get_publication_mode("proyectos") == "project_dossier"
    assert is_proyectos_domain("proyectos") is True
    assert should_run_clustering("proyectos") is False
    assert should_run_collection_reflection("proyectos") is False
    assert should_run_project_reflection("proyectos") is True
    assert should_run_global_bridge("proyectos") is False
    assert should_run_office_learning("proyectos") is True
    assert domain_prompt_version("document-v1", "proyectos") == "document-v1-proyectos-es-v1"


def test_research_and_practice_keep_concept_graph_defaults():
    assert get_publication_mode("research") == "concept_graph"
    assert should_run_clustering("research") is True
    assert should_run_global_bridge("practice") is True
    assert should_run_project_reflection("practice") is False
