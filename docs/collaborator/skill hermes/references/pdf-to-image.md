# PDF a imagen cropeada

Conversión de PDFs de planos arquitectónicos a imágenes PNG con recorte de bordes blancos. Usado para preparar contenido visual (Instagram, presentaciones) desde materiales de Proyectos.

## Dependencias

```bash
pip3 install PyMuPDF Pillow numpy
```

## Script completo

```python
import fitz  # PyMuPDF
from pathlib import Path
from PIL import Image
import numpy as np

pdf_dir = Path("/tmp/ig_proyecto/pdfs")
output_dir = Path.home() / "Desktop" / "ig_proyecto"
output_dir.mkdir(parents=True, exist_ok=True)

# Lista de PDFs: (nombre_entrada, nombre_salida)
pdfs = [
    ("01_Emplazamiento.pdf", "01_emplazamiento.png"),
    ("05_Planta_primera.pdf", "02_planta_primera.png"),
    # ...
]

for pdf_name, out_name in pdfs:
    pdf_path = pdf_dir / pdf_name
    if not pdf_path.exists():
        print(f"MISSING: {pdf_name}")
        continue
    
    doc = fitz.open(str(pdf_path))
    page = doc[0]
    
    # Render a 300 DPI
    mat = fitz.Matrix(300/72, 300/72)
    pix = page.get_pixmap(matrix=mat)
    
    # Convertir a PIL
    img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
    
    # Detectar bounding box de contenido (no-blanco: cualquier canal < 250)
    arr = np.array(img)
    mask = np.any(arr < 250, axis=2)
    
    if mask.any():
        rows = np.any(mask, axis=1)
        cols = np.any(mask, axis=0)
        ymin, ymax = np.where(rows)[0][[0, -1]]
        xmin, xmax = np.where(cols)[0][[0, -1]]
        margin = 20
        ymin = max(0, ymin - margin)
        ymax = min(img.height, ymax + margin)
        xmin = max(0, xmin - margin)
        xmax = min(img.width, xmax + margin)
        img = img.crop((xmin, ymin, xmax, ymax))
    
    out_path = output_dir / out_name
    img.save(str(out_path), "PNG", optimize=True)
    doc.close()
    print(f"OK: {out_name} ({img.width}x{img.height})")

print("DONE")
```

## Notas

- 300 DPI da buena resolución para Instagram y presentaciones. Para impresión, usar 600.
- El umbral 250 en la máscara permite píxeles casi-blancos (fondos sucios de escaneos). Ajustar si se corta contenido.
- Las imágenes de referentes externos (Docomomo, etc.) se copian directamente, no se convierten.
- Guardar el script en `/tmp/convert_pdfs.py` y ejecutar con `rtk python3 /tmp/convert_pdfs.py` (el sandbox de `execute_code` no tiene acceso a los paquetes instalados por pip).
