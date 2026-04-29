# OpenDataLoader PDF Probe

Minimal local experiment for testing `opendataloader-pdf` against a material
from an Arquimedes vault.

```bash
python3.11 -m venv workspace/opendataloader-pdf-probe/.venv
workspace/opendataloader-pdf-probe/.venv/bin/python -m pip install -U pip opendataloader-pdf
workspace/opendataloader-pdf-probe/.venv/bin/python workspace/opendataloader-pdf-probe/probe.py \
  --vault /Users/alejandrocampos/Vaults/personal \
  --material-id 015a8ff37424
```

OpenDataLoader requires Java 11+ on `PATH`.

With Homebrew OpenJDK, run the probe with:

```bash
PATH="/opt/homebrew/opt/openjdk/bin:$PATH" \
  workspace/opendataloader-pdf-probe/.venv/bin/python workspace/opendataloader-pdf-probe/probe.py \
  --vault /Users/alejandrocampos/Vaults/personal \
  --material-id 015a8ff37424
```
