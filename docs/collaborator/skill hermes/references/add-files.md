# Arquimedes Proyectos — Añadir archivos y enlaces simbólicos (especial escritura)

## Vault root vs library root

Arquimedes usa dos raíces distintas. No las confundas.

- Vault root: el checkout local de Git que contiene los archivos de conocimiento generados/indexados por Arquimedes y su configuración, como `config/`, `wiki/`, `derived/`, `extracted/` y docs. Aquí se ejecutan normalmente los comandos `arq`. No deposites PDFs fuente ni archivos de proyecto aquí.
- Library root: la carpeta compartida de materiales fuente que Arquimedes escanea para ingerir. Aquí pertenecen los PDFs, imágenes, notas, informes de reunión y archivos fuente de proyecto de los humanos. Los archivos de proyecto suelen ir bajo `Proyectos/<project-id>/...` dentro de esta raíz.

Para encontrar las raíces activas:

```bash
arq vault info
```

Usa la línea `library_root:` para colocar archivos y destinos de enlaces simbólicos dentro de Arquimedes. Usa la línea `vault_root:` solo para ejecutar comandos `arq` o entender qué checkout del vault está activo.

Límite no negociable: Hermes nunca debe crear, editar, mover, eliminar ni sobrescribir archivos bajo el vault root directamente. Los archivos del vault root incluyen configuración, datos generados wiki/derived/extracted, índices, docs y cualquier otro archivo del checkout de Git. Las ediciones directas en el sistema de archivos del vault root están prohibidas. Si el estado o la información del vault debe cambiar, usa solo un comando `arq`, o pregunta al humano/mantenedor.

Reglas de seguridad:

- Antes de escribir o enlazar archivos, resuelve siempre el `library_root` actual con `arq vault info`.
- No infieras el library root desde el directorio de trabajo actual.
- Nunca escribas en `vault_root` directamente. Esto sigue prohibido incluso para pequeñas correcciones, ajustes de configuración, ediciones de markdown o limpieza.
- La información/configuración/estado del vault solo puede cambiarse mediante comandos `arq`. Si no existe un comando, pregunta al humano/mantenedor en lugar de editar archivos.

## Archivos de humanos

Si un usuario quiere que Hermes añada informes de reunión, PDFs, imágenes, notas u otros archivos de proyecto a Arquimedes:

- Resuelve `library_root` con `arq vault info`.
- Coloca los archivos bajo `library_root/Proyectos/<project-id>/...`.
- Nunca pongas archivos fuente en `vault_root` ni en la carpeta del servidor/NAS de la oficina.
- Trata las carpetas del servidor/NAS como de solo lectura. Si deben ingerirse, crea un enlace simbólico desde la carpeta del proyecto en la biblioteca en lugar de copiarlas.

## Enlazar carpetas del servidor/NAS para ingesta

Cuando el usuario pida "traer", "enlazar", "montar", "hacer un alias" o "crear un alias" de una carpeta del servidor/NAS de la oficina, crea una carpeta con enlace simbólico Unix dentro del library root de Arquimedes.

Primero resuelve `ARQ_LIBRARY_ROOT` desde `arq vault info` (`library_root:`), luego usa:

```bash
ln -s "<carpeta-real-del-servidor>" "$ARQ_LIBRARY_ROOT/Proyectos/<project-id>/<nombre-del-enlace>"
```

Ejemplo:

```bash
ln -s "/Volumes/Server/Clientes/Casa Rio/Entregas" \
  "$ARQ_LIBRARY_ROOT/Proyectos/2407-casa-rio/server-entregas"
```

Reglas:

- Usa `ln -s`; no crees un alias de macOS Finder.
- No copies la carpeta del servidor.
- No escribas archivos nuevos dentro de la carpeta del servidor.
- Coloca el enlace simbólico bajo `Proyectos/<project-id>/`.
- Usa un nombre de enlace corto y estable, p. ej. `server-docs`, `server-entregas`, `cliente`, `consultores`.
- Si la ruta del enlace de destino ya existe, detente y pregunta antes de reemplazarla.

Verificaciones de seguridad:

1. Confirma el id del proyecto con `arq project list`.
2. Resuelve `library_root` con `arq vault info`; nunca uses `vault_root`.
3. Confirma que la carpeta del servidor es legible y que la ruta del enlace está dentro de `$ARQ_LIBRARY_ROOT/Proyectos/<project-id>/`.
4. Confirma que el nombre del enlace no existe ya.
5. Verifica que el enlace existe y apunta a la ruta del servidor prevista.
