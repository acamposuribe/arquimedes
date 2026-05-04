# Arquimedes Proyectos — Lectura (principal)

Usa solo comandos oficiales de Arquimedes. No edites archivos del vault directamente.

## Orden de prioridad

Al tratar de entender el estado de un proyecto, prioriza la evidencia en este orden:

1. Instrucciones explícitas del usuario en la conversación actual.
2. Notas/comentarios abiertos del proyecto (de Hermes o de humanos).
3. Estado actual / secciones de página del proyecto.
4. Estado estructurado.
5. Materiales fuente encontrados mediante búsqueda/lectura.

Las notas abiertas son evidencia no resuelta de alta prioridad. Si contradicen conclusiones anteriores, confía en las notas abiertas salvo que evidencia más reciente diga lo contrario.

## Comandos principales del proyecto (lectura)

| Tarea | Comando |
| --- | --- |
| Listar proyectos | `arq project list` |
| Resumen con conteo de materiales | `arq overview --domain proyectos` |
| Leer estado, secciones y notas abiertas | `arq project status <project-id>` |
| Buscar en el dossier de un proyecto | `arq project search <project-id> "licencia"` |

## Herramientas de corpus para materiales del proyecto

Úsalas cuando el estado del proyecto no sea suficiente y necesites evidencia fuente.

- `search`: encuentra materiales, pasajes, figuras, anotaciones, clusters y bridges.
- `read`: explora un material; primero la ficha por defecto, luego `--detail chunks|figures|annotations`, `--page`, `--chunk` o `--full` solo cuando sea necesario.
- `figures`: lista o inspecciona figuras de un material.
- `annotations`: lista anotaciones del lector para un material.

Ejemplos CLI:

```bash
arq search --deep --facet domain=proyectos "consulta estructura"
arq project search <project-id> "planta baja anteproyecto"
arq read <material-id>
arq read <material-id> --detail annotations
arq figures <material-id>
arq figures <material-id> --figure fig_0001
arq annotations <material-id>
```

Prefiere `arq project search <project-id> ...` una vez conocido el proyecto. Usa `arq search` general solo cuando el proyecto sea desconocido o necesites contexto entre proyectos.
