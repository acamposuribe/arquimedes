"""Domain-specific behavior profiles.

This module keeps non-schema differences between ``research`` and ``practice``
small, explicit, and reusable across enrichment, lint, clustering, and page
rendering.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class DomainProfile:
    domain: str
    output_language: str
    prompt_version_suffix: str = ""
    concept_reflection_figure_limit: int = 2
    collection_reflection_figure_limit: int = 2
    generated_labels: dict[str, str] = field(default_factory=dict)


_RESEARCH_LABELS = {
    "metadata": "Metadata",
    "field": "Field",
    "value": "Value",
    "authors": "Authors",
    "year": "Year",
    "type": "Type",
    "domain": "Domain",
    "collection": "Collection",
    "pages": "Pages",
    "summary": "Summary",
    "material_conclusions": "Material Conclusions",
    "methodological_conclusions": "Methodological conclusions",
    "main_content_learnings": "Main content learnings",
    "key_concepts": "Key Concepts",
    "architecture_facets": "Architecture Facets",
    "building_type": "Building type",
    "scale": "Scale",
    "location": "Location",
    "jurisdiction": "Jurisdiction",
    "climate": "Climate",
    "program": "Program",
    "material_system": "Material system",
    "structural_system": "Structural system",
    "historical_period": "Historical period",
    "course_topic": "Course topic",
    "studio_project": "Studio project",
    "figures": "Figures",
    "reader_annotations": "Reader Annotations",
    "note": "Note",
    "source": "Source",
    "citation": "Citation",
    "open_original_file": "Open original file",
    "full_extracted_text": "Full extracted text",
    "related_materials": "Related Materials",
    "reflections": "Reflections",
    "main_takeaways": "Main takeaways",
    "main_tensions": "Main tensions",
    "open_questions": "Open questions",
    "helpful_new_sources": "Helpful new sources",
    "recent_changes": "Recent Changes",
    "status": "Status",
    "severity": "Severity",
    "recommendation": "Recommendation",
    "bridge_cluster": "Bridge cluster",
    "also_known_as": "Also known as",
    "cross_collection_synthesis": "Cross-Collection Synthesis",
    "why_this_bridge_matters": "Why This Bridge Matters",
    "shared_takeaways": "Shared Takeaways",
    "shared_tensions": "Shared Tensions",
    "collection_signals": "Collection Signals",
    "global_bridges": "Global Bridges",
    "contributing_local_clusters": "Contributing Local Clusters",
    "local_cluster": "Local cluster",
    "promotion": "Promotion",
    "by_material": "By Material",
    "related_concepts": "Related Concepts",
    "overview": "Overview",
    "recent_additions": "Recent Additions",
    "materials": "Materials",
    "top_facets": "Top Facets",
    "bridge_suffix": "bridge",
    "main_suffix": "main",
    "concepts": "Concepts",
    "bridge_concepts": "Bridge Concepts",
    "local_concepts": "Local Concepts",
    "main_concepts": "Main Concepts",
}

_PRACTICE_LABELS = {
    "metadata": "Metadatos",
    "field": "Campo",
    "value": "Valor",
    "authors": "Autores",
    "year": "Año",
    "type": "Tipo",
    "domain": "Dominio",
    "collection": "Colección",
    "pages": "Páginas",
    "summary": "Resumen",
    "material_conclusions": "Conclusiones del material",
    "methodological_conclusions": "Conclusiones metodológicas",
    "main_content_learnings": "Aprendizajes principales",
    "key_concepts": "Conceptos clave",
    "architecture_facets": "Facetas arquitectónicas",
    "building_type": "Tipo de edificio",
    "scale": "Escala",
    "location": "Ubicación",
    "jurisdiction": "Jurisdicción",
    "climate": "Clima",
    "program": "Programa",
    "material_system": "Sistema material",
    "structural_system": "Sistema estructural",
    "historical_period": "Período histórico",
    "course_topic": "Tema del curso",
    "studio_project": "Proyecto de taller",
    "figures": "Figuras",
    "reader_annotations": "Anotaciones del lector",
    "note": "Nota",
    "source": "Fuente",
    "citation": "Cita",
    "open_original_file": "Abrir archivo original",
    "full_extracted_text": "Texto extraído completo",
    "related_materials": "Materiales relacionados",
    "reflections": "Reflexiones",
    "main_takeaways": "Aprendizajes principales",
    "main_tensions": "Tensiones principales",
    "open_questions": "Preguntas abiertas",
    "helpful_new_sources": "Fuentes nuevas útiles",
    "recent_changes": "Cambios recientes",
    "status": "Estado",
    "severity": "Severidad",
    "recommendation": "Recomendación",
    "bridge_cluster": "Cluster puente",
    "also_known_as": "También conocido como",
    "cross_collection_synthesis": "Síntesis entre colecciones",
    "why_this_bridge_matters": "Por qué importa este puente",
    "shared_takeaways": "Aprendizajes compartidos",
    "shared_tensions": "Tensiones compartidas",
    "collection_signals": "Señales de colección",
    "global_bridges": "Puentes globales",
    "contributing_local_clusters": "Clusters locales contribuyentes",
    "local_cluster": "Cluster local",
    "promotion": "Motivos de promoción",
    "by_material": "Por material",
    "related_concepts": "Conceptos relacionados",
    "overview": "Resumen general",
    "recent_additions": "Incorporaciones recientes",
    "materials": "Materiales",
    "top_facets": "Facetas principales",
    "bridge_suffix": "puente",
    "main_suffix": "principal",
    "concepts": "Conceptos",
    "bridge_concepts": "Conceptos puente",
    "local_concepts": "Conceptos locales",
    "main_concepts": "Conceptos principales",
}

_PROFILES = {
    "research": DomainProfile(
        domain="research",
        output_language="English",
        generated_labels=_RESEARCH_LABELS,
    ),
    "practice": DomainProfile(
        domain="practice",
        output_language="Spanish",
        prompt_version_suffix="practice-es-v1",
        concept_reflection_figure_limit=4,
        collection_reflection_figure_limit=4,
        generated_labels=_PRACTICE_LABELS,
    ),
}


def normalize_domain(domain: str, *, default: str = "practice") -> str:
    """Normalize a domain slug with an explicit fallback."""
    normalized = (domain or default).strip().lower() or default
    return normalized


def get_domain_profile(domain: str, *, default: str = "research") -> DomainProfile:
    """Return the built-in profile for *domain* or a safe default."""
    normalized = normalize_domain(domain, default=default)
    return _PROFILES.get(normalized, _PROFILES[default])


def is_practice_domain(domain: str, *, default: str = "research") -> bool:
    return normalize_domain(domain, default=default) == "practice"


def domain_prompt_version(prompt_version: str, domain: str) -> str:
    """Return the stage prompt version adjusted for domain-specific behavior."""
    profile = get_domain_profile(domain, default="research")
    if not profile.prompt_version_suffix:
        return prompt_version
    suffix = f"-{profile.prompt_version_suffix}"
    return prompt_version if prompt_version.endswith(suffix) else f"{prompt_version}{suffix}"


def generated_label(key: str, domain: str, *, default: str | None = None) -> str:
    """Return a generated UI label for *domain* with a plain fallback."""
    profile = get_domain_profile(domain, default="research")
    if key in profile.generated_labels:
        return profile.generated_labels[key]
    return default if default is not None else key.replace("_", " ").title()


def display_domain_name(domain: str) -> str:
    """Return the user-facing domain name."""
    return "Práctica" if is_practice_domain(domain) else "Research"
