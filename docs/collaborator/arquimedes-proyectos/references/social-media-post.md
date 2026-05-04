# Arquimedes Proyectos — Crear posts para redes sociales

Flujo para generar contenido de Instagram u otras redes a partir de materiales de un proyecto en Arquimedes.

## Principio rector

El post debe construir una **genealogía visual**, no un texto decorativo. Lo importante son las plantas, los diagramas espaciales y los referentes. El texto es auxiliar, mínimo y factual.

## Flujo de trabajo

### 1. Identificar el proyecto y cargar estado

```bash
arq project status "<project-id>"
```

Obtener: objetivos, decisiones, condiciones, `important_material_ids`, y entender la **estrategia espacial principal** del proyecto (ej: "introducir el patio de manzana hacia el interior para que todas las estancias tengan vistas lejanas").

### 2. Buscar los materiales clave (NO figuras extraídas)

Buscar los **planos originales** (PDFs), no renders ni figuras extraídas:

```bash
arq project search "<project-id>" "emplazamiento planta seccion"
```

**Prioridad de selección para el carrusel:**
1. **Plano de emplazamiento** — el más importante. Muestra la relación con la manzana, el interior de bloque, edificios colindantes (ej: colegio). Es el plano que explica la estrategia.
2. **Plantas tipo** — donde se lee cómo el espacio se organiza alrededor de la estrategia principal (patio, vacío, vistas).
3. **Sección longitudinal** — la relación vertical con el entorno.
4. **Referentes** — imágenes de obras de referencia (fotos, planos o dibujos) que dialogan con la estrategia del proyecto.

**No uses:**
- Figuras extraídas de PDFs (`.jpeg`/`.png` en `extracted/<id>/figures/`) — pierden textos, cotas, diagramas, anotaciones.
- Renders de fachada o interiores como portada — son lo menos relevante arquitectónicamente.
- `search_files`, `ls`, `find` para localizar archivos. Usa siempre `arq`.

### 3. Copiar los PDFs originales

Para cada material seleccionado, obtener el `source_path` y copiar el PDF:

```bash
arq read <material-id>           # sin --human, para ver source_path
arq vault info                   # obtener library_root
cp "<library_root>/<source_path>" /tmp/ig_<proyecto>/pdfs/<nombre>.pdf
```

Usar `rtk cp` para resolver permisos. Todos los PDFs quedan en `/tmp/ig_<proyecto>/pdfs/`.

### 3.5. Convertir PDFs a imágenes cropeadas

El usuario NO quiere PDFs para redes sociales. Quiere imágenes PNG/JPG renderizadas desde los PDFs originales, con bordes blancos eliminados.

Herramientas necesarias (instalar si faltan):
```bash
pip3 install PyMuPDF Pillow numpy
```

Script de conversión (ver `references/pdf-to-image.md` para el script completo). Resumen:
- Renderizar a 300 DPI con PyMuPDF (`fitz`)
- Convertir a array numpy para detectar píxeles no-blancos (umbral 250)
- Cropear con margen de 20px
- Guardar como PNG en `~/Desktop/ig_<proyecto>/`

Las imágenes de referentes (Bofill, Coderch, etc.) se copian directamente como JPG sin convertir.

La carpeta final en el escritorio contiene las imágenes numeradas en orden de carrusel:
```
01_emplazamiento.png
02_planta_primera.png
03_seccion.png
04_bofill_bach.jpg     (referente)
05_girasol.jpg         (referente)
06_alzados.png
07_interiores.png
```

### 4. Investigar referencias arquitectónicas

Si el usuario pide relacionar el proyecto con referentes (Coderch, Bofill, etc.):

- **NO usar `delegate_task`** — se enreda en bucles de terminal y no produce resultados útiles.
- **NO usar Wikipedia API** — devuelve HTTP 403 o JSON con caracteres de control ilegibles.
- **Usar `browser_navigate`** a fuentes especializadas:
  - Docomomo Ibérico: `https://docomomoiberico.com/edificios/`
  - Para extraer URLs de imágenes: `browser_get_images`
  - Para descargar las imágenes a máxima resolución: `rtk curl -sL -o <destino> "<url sin .webp>"`
- Si `vision_analyze` y `browser_vision` están rate-limited (429), no reintentar. Las imágenes se entregan al usuario sin verificar visualmente; él las revisa.

### 5. Redactar el texto del post

Principios de tono (preferencia del usuario):
- **Mínimo.** El texto no es el contenido principal; la secuencia visual sí.
- **Espacial, no técnico.** Hablar de vacío, luz, vistas lejanas, relación con la calle. No de normativa, superficies ni sistemas.
- **No comercial ni poético.** Nada de "obra maestra", metáforas florales, ni adjetivos decorativos. La arquitectura se explica con planos, no con palabras.
- **Arquitectónico pero sobrio.** Las referencias a otros arquitectos se muestran visualmente (imagen del referente al lado del plano del proyecto), no se describen con texto.

Estructura máxima:
1. Una frase de contexto (lugar + programa)
2. Una frase sobre la estrategia espacial
3. Ficha mínima: autores, emplazamiento, fase

Nada más. Si el texto ocupa más de 4 líneas, sobra.

### 6. Preparar la entrega

- PDFs del proyecto en `/tmp/ig_<proyecto>/pdfs/`
- Imágenes de referentes en `/tmp/ig_<proyecto>/referentes/`
- Proponer una secuencia de carrusel numerada
- Texto del post (4 líneas máximo)
- Preguntar al usuario si las láminas seleccionadas son las correctas

## Pitfalls

- **No usar figuras extraídas para planos.** Las figuras pierden cotas, textos y esquemas. Usar siempre los PDFs originales. Las figuras solo valen para renders interiores si el usuario los pide explícitamente.
- **La fachada NO es la imagen principal.** El plano de emplazamiento y las plantas son lo que importa. La fachada es secundaria.
- **El texto no es poesía.** Si suena a "poesía barata" o "vendehumos", se borra y se deja solo la ficha técnica.
- **No usar `search_files` para nada.** Solo comandos `arq`.
- **No entregar PDFs para redes sociales.** El usuario quiere imágenes renderizadas y cropeadas en una carpeta del escritorio, no PDFs. Ver paso 3.5 y `references/pdf-to-image.md`.

## Ejemplo de sesión

Conversación del 3 de mayo 2026: post de Instagram del Edificio Gandia. Lecciones principales:
- El usuario corrigió el uso de figuras extraídas y `search_files`. Solo arq, solo PDFs originales.
- Corrección del referente Bofill: no es Bach 2-4 (1962-66, Ricardo Bofill), sino **Bach 28 (1960-63, Emili + Ricardo Bofill)**. La memoria de Docomomo describe explícitamente: *"un patio central en diagonal, que se convierte en una prolongación del patio interior de manzana"*. Esa estrategia es la que resuena con Gandia.
- El texto propuesto inicialmente fue rechazado por "poesía barata y vendehumos". Corrección: texto factual, mínimo, centrado en la operación espacial.
- El usuario quiere las imágenes renderizadas desde PDFs y cropeadas en una carpeta del escritorio, no los PDFs mismos.

## Referentes arquitectónicos conocidos (para genealogía visual)

| Referente | Estrategia | Usar cuando el proyecto... |
|---|---|---|
| Bofill, Bach 28 (Emili+Ricardo Bofill, 1960-63) | Patio central en diagonal que prolonga el interior de manzana | ...introduce el patio de manzana hacia dentro para dar vistas a todas las estancias |
| Bofill, Bach 2-4 (Ricardo Bofill, 1962-66) | Eliminación de patiecillos → patio posterior unificado | ...unifica patios pequeños en uno grande |
| Coderch, Edificio Girasol (1964-66) | Retranqueos escalonados de fachada; el vacío construye | ...usa retranqueos, terrazas, el vacío como elemento compositivo |
