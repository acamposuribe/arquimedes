---
name: arquimedes-proyectos
description: "Use when Hermes helps with Arquimedes Proyectos project memory: identify project, review open notes/status, add notes, update sections, link project files, or force reflection only when requested. Fast card; load references only for details."
version: 1.4.0
author: Hermes Agent
license: MIT
metadata:
  hermes:
    tags: [arquimedes, proyectos, architecture, notes, dossiers]
    related_skills: []
---

# Arquimedes Proyectos

Índice de decisión para trabajo con proyectos en Arquimedes. Carga las referencias según el árbol; no contienen todo aquí.

## Qué es Proyectos

`Proyectos/` es el dominio de memoria de proyectos de Arquimedes para la práctica de arquitectura. Cada carpeta es el dossier de un proyecto. Combina materiales ingeridos, notas de proyecto, texto curado de secciones y `project_state` estructurado en una página de proyecto generada.

El trabajo por defecto de Hermes es preservar memoria útil del proyecto a partir de conversaciones y archivos, no reescribir el dossier completo.

## Árbol de decisión — carga la referencia según lo que pida el usuario

| Si el usuario pide... | Carga esta referencia |
|---|---|
| Consultar estado, buscar/leer materiales, listar figuras/anotaciones | `references/read.md` |
| Añadir/editar/borrar notas, editar secciones, ejecutar reflection | `references/write.md` |
| Enviar, copiar, dejar o extraer un archivo del proyecto al escritorio, /tmp, etc. | `references/send-files.md` |
| Añadir archivos nuevos al proyecto, enlazar servidor/NAS, iniciar proyecto desde carpeta | `references/add-files.md` |
| Crear contenido para redes sociales (Instagram, etc.) a partir de materiales del proyecto | `references/social-media-post.md` |

Carga solo la referencia necesaria. Si la petición toca varias, carga la más prioritaria primero.

## Prioridad de evidencia

Cuando Hermes intente entender en qué punto está un proyecto, prioriza en este orden:

1. Instrucciones humanas explícitas en la conversación actual.
2. **Notas abiertas del proyecto** de Hermes o del equipo.
3. **Notas de tipo `strategy` / Estrategia principal.** Son evidencia prioritaria persistente y marco rector del proyecto.
4. Secciones actuales de la página del proyecto.
5. Estado estructurado.
6. Materiales fuente encontrados mediante `search`/`read`.

Las notas `strategy` no se archivan automáticamente, no las toca lint y deben seguir condicionando las decisiones posteriores. No deben reescribirse libremente: Hermes solo debe editarlas si la persona lo pide explícitamente.

## Flujo de trabajo normal

1. Identifica el proyecto. Infiérelo del canal/contexto de Discord solo cuando sea claro; si no, pregunta al usuario.
2. Localiza y revisa el dossier. Lee primero las notas abiertas.
3. Captura la **estrategia principal** en cuanto quede clara. Registra el marco rector del proyecto —cómo responde al lugar, al contexto, al clima o a una forma de habitar— como nota `strategy` antes de actualizaciones de menor prioridad.
   - Si ya existe una estrategia, no la edites por iniciativa propia. Solo corrígela o reescríbela si la persona lo pide explícitamente.
4. Añade notas con los nuevos hechos. Registra estrategia, decisiones, requisitos, riesgos, plazos, temas de coordinación, lecciones, errores, reparaciones o resúmenes útiles de reuniones/archivos.
5. Actualiza secciones cuando sea necesario. Usa ediciones de sección para prosa curada que deba aparecer directamente en la página generada del proyecto.
6. Fuerza reflection solo cuando se solicite. Si el usuario pide re-ejecutar/refrescar/forzar la síntesis, ejecuta reflection del proyecto.

## Non-negotiables

- Usa comandos `arq` oficiales. Nunca edites archivos del vault directamente.
- Identifica el proyecto primero. Si el contexto no es claro, pregunta.
- Para localizar o enviar materiales del proyecto, usa siempre comandos `arq`. Nunca uses búsquedas de sistema de archivos (`search_files`, `ls`, `find`). El corpus es la fuente de verdad.
- Nunca escribas archivos fuente/proyecto en `vault_root`.
- **Cuando copies o extraigas archivos del proyecto, carga siempre `references/send-files.md`.** Contiene la regla crítica: PDFs originales, nunca figuras extraídas. Las figuras pierden cotas, textos y diagramas.
- **Cuando crees contenido para redes sociales desde materiales del proyecto, carga siempre `references/social-media-post.md`.** Contiene el orden de prioridad correcto de imágenes y el tono adecuado.

## Referencias

- `references/read.md`: comandos CLI de solo lectura, búsqueda en corpus, estado del proyecto.
- `references/write.md`: flujo de trabajo, tipos de nota, secciones, reflection.
- `references/send-files.md`: enviar/copiar/dejar archivos del proyecto al usuario.
- `references/add-files.md`: añadir archivos nuevos, enlaces symlink a servidor/NAS.
- `references/social-media-post.md`: crear posts de Instagram/redes sociales desde materiales, figuras y referencias arquitectónicas del proyecto.
- `references/pdf-to-image.md`: convertir PDFs de planos a imágenes PNG cropeadas (300 DPI, recorte de bordes blancos).
