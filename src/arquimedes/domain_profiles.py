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
    publication_mode: str = "concept_graph"
    run_local_clustering: bool = True
    run_concept_reflection: bool = True
    run_collection_reflection: bool = True
    run_project_reflection: bool = False
    run_global_bridge: bool = True
    run_office_learning: bool = False
    concept_reflection_figure_limit: int = 2
    collection_reflection_figure_limit: int = 2
    generated_labels: dict[str, str] = field(default_factory=dict)


_RESEARCH_LABELS = {
    "home": "Home",
    "wiki": "Wiki",
    "search": "Search",
    "navigation": "Navigation",
    "directories": "Directories",
    "pages_nav": "Pages",
    "actions": "Actions",
    "open_source_file": "Open source file",
    "extracted_text": "Extracted text",
    "no_extracted_figures": "No extracted figures",
    "linked_materials": "Linked Materials",
    "search_scope": "Search scope",
    "all": "All",
    "title_label": "Title",
    "author": "Author",
    "search_the_knowledge_base": "Search the knowledge base…",
    "search_materials_concepts_annotations": "Search materials, concepts, annotations…",
    "search_materials_button": "Search",
    "search_materials_placeholder": "thermal mass, archive, courtyard…",
    "collection_placeholder": "Collection",
    "architecture_knowledge_base_desc": "Architecture knowledge base — browse the compiled wiki, search materials, and explore extracted figures.",
    "index_not_ready": "Index not ready.",
    "index_not_ready_help": "Run arq index rebuild to unlock search and collection navigation.",
    "search_index_not_found": "Search index not found. Run arq index rebuild first.",
    "no_indexed_collections_yet": "No indexed collections yet.",
    "no_indexed_materials_yet": "No indexed materials yet.",
    "search_hits_for": "Search hits for",
    "within_this_material": "Within This Material",
    "scoped_search": "Scoped Search",
    "matching_passages": "Matching passages",
    "full_text": "Full text",
    "annotations_label": "Annotations",
    "concept_matches": "Concept matches",
    "figure_matches": "Figure matches",
    "no_matches_inside_material": "No indexed chunk, annotation, concept, or figure matches were found for this query inside the material.",
    "no_matches_inside_scope": "No matching materials were found inside this page.",
    "search_this_material": "Search This Material",
    "search_this_collection": "Search This Collection",
    "search_this_concept": "Search This Concept",
    "clear_search": "Clear search",
    "search_within_page": "Search within page",
    "recent_materials": "Recent Materials",
    "domain_suffix": "domain",
    "figures_for": "Figures",
    "extracted_text_for": "Extracted text",
    "no_extracted_figures_for_material": "No extracted figures were found for this material.",
    "collections_summary": "collections",
    "concepts_summary": "concepts",
    "materials_summary": "materials",
    "results_for": "for",
    "no_results_for": "No results for",
    "enter_query_to_search": "Enter a query above to search collections, concepts, materials, and passages inside materials.",
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
    "home": "Inicio",
    "wiki": "Wiki",
    "search": "Buscar",
    "navigation": "Navegación",
    "directories": "Directorios",
    "pages_nav": "Páginas",
    "actions": "Acciones",
    "open_source_file": "Abrir archivo fuente",
    "extracted_text": "Texto extraído",
    "no_extracted_figures": "No hay figuras extraídas",
    "linked_materials": "Materiales vinculados",
    "search_scope": "Alcance de búsqueda",
    "all": "Todo",
    "title_label": "Título",
    "author": "Autor",
    "search_the_knowledge_base": "Buscar en la base de conocimiento…",
    "search_materials_concepts_annotations": "Buscar materiales, conceptos y anotaciones…",
    "search_materials_button": "Buscar",
    "search_materials_placeholder": "masa térmica, archivo, patio…",
    "collection_placeholder": "Colección",
    "architecture_knowledge_base_desc": "Base de conocimiento de arquitectura: recorre la wiki compilada, busca materiales y explora figuras extraídas.",
    "index_not_ready": "Índice no disponible.",
    "index_not_ready_help": "Ejecuta arq index rebuild para habilitar la búsqueda y la navegación por colecciones.",
    "search_index_not_found": "No se encontró el índice de búsqueda. Ejecuta arq index rebuild primero.",
    "no_indexed_collections_yet": "Todavía no hay colecciones indexadas.",
    "no_indexed_materials_yet": "Todavía no hay materiales indexados.",
    "search_hits_for": "Resultados de búsqueda para",
    "within_this_material": "Dentro de este material",
    "scoped_search": "Búsqueda acotada",
    "matching_passages": "Pasajes coincidentes",
    "full_text": "Texto completo",
    "annotations_label": "Anotaciones",
    "concept_matches": "Coincidencias de conceptos",
    "figure_matches": "Coincidencias de figuras",
    "no_matches_inside_material": "No se encontraron coincidencias indexadas de fragmentos, anotaciones, conceptos o figuras para esta consulta dentro del material.",
    "no_matches_inside_scope": "No se encontraron materiales coincidentes dentro de esta página.",
    "search_this_material": "Buscar en este material",
    "search_this_collection": "Buscar en esta colección",
    "search_this_concept": "Buscar en este concepto",
    "clear_search": "Limpiar búsqueda",
    "search_within_page": "Buscar en la página",
    "recent_materials": "Materiales recientes",
    "domain_suffix": "dominio",
    "figures_for": "Figuras",
    "extracted_text_for": "Texto extraído",
    "no_extracted_figures_for_material": "No se encontraron figuras extraídas para este material.",
    "collections_summary": "colecciones",
    "concepts_summary": "conceptos",
    "materials_summary": "materiales",
    "results_for": "para",
    "no_results_for": "No hay resultados para",
    "enter_query_to_search": "Escribe una consulta arriba para buscar en colecciones, conceptos, materiales y pasajes dentro de los materiales.",
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

_PROYECTOS_LABELS = {
    **_PRACTICE_LABELS,
    "collection": "Proyecto",
    "collection_placeholder": "Proyecto",
    "collections_summary": "proyectos",
    "architecture_knowledge_base_desc": "Memoria de proyectos de arquitectura: recorre expedientes compilados, busca materiales y revisa el estado de cada proyecto.",
    "no_indexed_collections_yet": "Todavía no hay proyectos indexados.",
    "search_this_collection": "Buscar en este proyecto",
    "overview": "Estado del proyecto",
    "materials": "Materiales del proyecto",
    "recent_additions": "Historial reciente",
    "estado": "Estado del proyecto",
    "trabajo_en_curso": "Trabajo en curso",
    "objetivos_principales": "Objetivos principales",
    "condiciones_restricciones": "Condiciones y restricciones",
    "decisiones_requisitos": "Decisiones y requisitos",
    "riesgos": "Problemas, riesgos y bloqueos",
    "informacion_pendiente": "Información pendiente",
    "proximo_foco": "Próximo foco",
    "materiales_importantes": "Materiales importantes",
    "aprendizajes": "Aprendizajes positivos",
    "errores_reparaciones": "Errores y acciones de reparación",
    "notas_recientes": "Notas recientes",
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
    "proyectos": DomainProfile(
        domain="proyectos",
        output_language="Spanish",
        prompt_version_suffix="proyectos-es-v1",
        publication_mode="project_dossier",
        run_local_clustering=False,
        run_concept_reflection=False,
        run_collection_reflection=False,
        run_project_reflection=True,
        run_global_bridge=False,
        run_office_learning=True,
        concept_reflection_figure_limit=4,
        collection_reflection_figure_limit=4,
        generated_labels=_PROYECTOS_LABELS,
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


def is_proyectos_domain(domain: str, *, default: str = "research") -> bool:
    return normalize_domain(domain, default=default) == "proyectos"


def get_publication_mode(domain: str) -> str:
    return get_domain_profile(domain, default="research").publication_mode


def should_run_clustering(domain: str) -> bool:
    return get_domain_profile(domain, default="research").run_local_clustering


def should_run_concept_reflection(domain: str) -> bool:
    return get_domain_profile(domain, default="research").run_concept_reflection


def should_run_collection_reflection(domain: str) -> bool:
    return get_domain_profile(domain, default="research").run_collection_reflection


def should_run_project_reflection(domain: str) -> bool:
    return get_domain_profile(domain, default="research").run_project_reflection


def should_run_global_bridge(domain: str) -> bool:
    return get_domain_profile(domain, default="research").run_global_bridge


def should_run_office_learning(domain: str) -> bool:
    return get_domain_profile(domain, default="research").run_office_learning


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
    if is_proyectos_domain(domain):
        return "Proyectos"
    return "Práctica" if is_practice_domain(domain) else "Research"
