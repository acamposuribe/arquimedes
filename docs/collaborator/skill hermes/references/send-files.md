# Arquimedes Proyectos — Enviar archivos al usuario (especial lectura)

Cuando el usuario pida un material de un dossier de Proyectos, envía el archivo fuente original por defecto, no una figura extraída. Usa figuras solo si se piden explícitamente o si el archivo original no está disponible y lo indicas.

Flujo:

1. `arq project search` para identificar el material.
2. `arq read <material-id>` (sin `--human`) para obtener `source_path`. El flag `--human` oculta ese campo; solo sin flags se ve en la salida JSON.
3. `arq vault info` para obtener `library_root`.
4. Copia el archivo fuente a la ruta de destino que pidió el usuario (escritorio, /tmp, etc.) con un nombre claro.
5. En plataformas de mensajería, devuelve `MEDIA:<ruta>`; en CLI, indica la ruta copiada.

No confundas una figura extraída con el material en sí; las láminas de planos normalmente significan el PDF completo original. Si varios materiales podrían ser el objetivo, pregunta al usuario cuál quiere, pero intenta inferirlo de la conversación primero.
