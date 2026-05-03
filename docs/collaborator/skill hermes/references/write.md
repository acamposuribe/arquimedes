# Arquimedes Proyectos — Escritura (principal)

## Comandos de escritura

| Tarea | Comando |
| --- | --- |
| Añadir una nota | `arq project note <project-id> --kind strategy --text "..." --source-ref "discord://channel/message"` |
| Editar una nota por `note_id` | `arq project note-edit <project-id> note-0001 --text "..."` |
| Eliminar una nota por `note_id` | `arq project note-delete <project-id> note-0001` |
| Resolver un ítem abierto | `arq project resolve <project-id> --item missing_information:1 --note "..."` |
| Reemplazar una sección de página | `arq project section set <project-id> proximo_foco --text "..."` |
| Forzar reflection cuando se solicite | `arq project reflect <project-id>` |

Agrupa escrituras con `--no-recompile` y luego ejecuta `arq project recompile <project-id>` una sola vez.

`note-delete` es un borrado lógico: marca la nota con `status: deleted`, `deleted: true`, `deleted_at` y `deleted_by`, y recompila salvo que se esté agrupando. Verifica con `arq project status <project-id>`; las notas eliminadas no deben aparecer en la lista de notas activas.

Los comandos directos de estado estructurado son escapes de administrador; úsalos solo cuando el usuario los pida explícitamente: `arq project update ...`, `arq project append ...`.

## Estrategia principal

Antes de registrar notas menores, verifica si la conversación o el archivo está definiendo la **estrategia principal** del proyecto.

`strategy` significa: el marco rector del proyecto; la idea que organiza cómo el edificio responde al lugar, al contexto, al clima o a una forma de habitar. Todas las decisiones posteriores deben contrastarse contra esta estrategia.

Reglas:

- Regístrala en cuanto quede clara.
- Trátala como evidencia prioritaria persistente.
- No se archiva automáticamente.
- Lint/reflection no la toca.
- Puede editarla Hermes o una persona desde la web, pero no debe borrarse como una nota ordinaria.
- No la reescribas libremente. Si ya existe una estrategia, solo corrígela, refínala o sustitúyela cuando la persona lo pida explícitamente.
- Si existe una estrategia previa y la conversación la matiza, no abras una duplicada si en realidad se trata de la misma estrategia.

## Qué preservar como notas

Añade una nota breve cuando una conversación o archivo aporte uno de estos tipos permitidos:

- `strategy`: el marco rector del proyecto —la manera principal en que debe responder al lugar, contexto, clima o modo de habitar—; las decisiones posteriores deben contrastarse contra ella.
- `decision`: una elección, aprobación, rechazo, dirección de diseño o acción acordada.
- `requirement`: un requisito del cliente, autoridad, técnico, contractual, presupuestario o de prestaciones.
- `risk`: un riesgo, bloqueo, contradicción, dependencia, incertidumbre, retraso o problema potencial.
- `deadline`: un compromiso con fecha, hito, fecha de entrega, reunión o tarea con plazo.
- `coordination`: un tema de coordinación entre personas, disciplinas, consultores, cliente, autoridad o contratista.
- `learning`: una lección útil, patrón, idea, precedente o conocimiento reutilizable.
- `mistake`: un error, arrepentimiento, suposición equivocada, camino fallido o algo que evitar repetir.
- `repair`: una acción correctiva, mitigación, plan de recuperación, resolución o solución de seguimiento.

No vuelques el historial del chat. No dupliques un material fuente salvo que la conversación cambie su interpretación, urgencia o prioridad.

Incluye procedencia siempre que sea posible:

- `--source-ref` para referencias de Discord, fechas de reunión, nombres de archivo, referencias de autoridad o ids de material.
- `--material-id` cuando la nota esté vinculada a un material ingerido.
- `--confidence` solo cuando la incertidumbre sea relevante.

## Notas vs secciones vs estado

- Notas: hechos atómicos, decisiones, peticiones, correcciones, contradicciones y nueva evidencia. Es la vía de escritura por defecto de Hermes.
- Secciones: prosa curada para las páginas generadas del proyecto, cuando una nota no basta.
- Estado estructurado: campos canónicos mantenidos principalmente por reflection/lint. Modifícalos directamente solo cuando el usuario lo pida explícitamente.

Nunca edites markdown compilado directamente. Las páginas de proyecto se generan a partir del estado, notas, secciones y materiales.

## Reflection

Reflection puede sintetizar notas abiertas en secciones/estado. Debe preservar, resolver o discutir explícitamente las advertencias de Hermes con evidencia.

Ejecuta reflection solo cuando se solicite explícitamente:

```bash
arq project reflect <project-id>
```

Tras una incorporación exitosa, las notas ordinarias pasan automáticamente de la cola abierta a estados archivados como `incorporated` o `superseded`. Las notas `strategy` son la excepción: permanecen abiertas y visibles como evidencia prioritaria persistente.
