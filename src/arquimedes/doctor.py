"""Environment diagnostics for maintainer machines."""

from __future__ import annotations

import importlib.util
import shutil
import subprocess
import sys
from dataclasses import dataclass


@dataclass
class Check:
    name: str
    ok: bool
    detail: str
    fix: str = ""
    required: bool = True


def _module_check(name: str, module: str, *, fix: str = "", required: bool = True) -> Check:
    spec = importlib.util.find_spec(module)
    if spec is None:
        return Check(name, False, f"missing Python module: {module}", fix, required)
    return Check(name, True, f"found Python module: {module}", required=required)


def _command_check(name: str, command: str, *, fix: str = "", required: bool = True) -> Check:
    path = shutil.which(command)
    if not path:
        return Check(name, False, f"missing command: {command}", fix, required)
    try:
        result = subprocess.run([command, "--version"], text=True, capture_output=True, timeout=5, check=False)
        first = (result.stdout or result.stderr or "").splitlines()[0].strip()
    except Exception:
        first = "version check failed"
    detail = f"{path}" + (f" ({first})" if first else "")
    return Check(name, True, detail, required=required)


def run_checks() -> list[Check]:
    """Return maintainer environment checks."""
    checks = [
        Check("Python", True, sys.executable),
        _module_check(
            "pyexpat XML support",
            "pyexpat",
            fix="Install/reinstall a Python build with expat support, e.g. `brew install expat && brew reinstall python`.",
        ),
        _module_check("PyMuPDF PDF support", "fitz", fix="Run `pip install -e .` or reinstall Arquimedes dependencies."),
        _module_check("Pillow image support", "PIL", fix="Run `pip install -e .` or reinstall Arquimedes dependencies."),
        _module_check("openpyxl XLSX support", "openpyxl", fix="Run `pip install openpyxl` or reinstall Arquimedes dependencies."),
        _module_check("pytesseract Python wrapper", "pytesseract", fix="Run `pip install pytesseract` or reinstall Arquimedes dependencies."),
        _command_check(
            "Tesseract OCR engine",
            "tesseract",
            fix="Install native OCR engine: `brew install tesseract tesseract-lang`.",
        ),
    ]
    return checks


def format_checks(checks: list[Check]) -> str:
    lines = ["Arquimedes doctor", ""]
    failed_required = False
    for check in checks:
        marker = "OK" if check.ok else ("MISSING" if check.required else "OPTIONAL")
        lines.append(f"[{marker}] {check.name}: {check.detail}")
        if not check.ok and check.fix:
            lines.append(f"      fix: {check.fix}")
        if check.required and not check.ok:
            failed_required = True
    lines.append("")
    lines.append("Result: " + ("FAILED — fix required items above" if failed_required else "OK"))
    return "\n".join(lines)


def has_required_failures(checks: list[Check]) -> bool:
    return any(check.required and not check.ok for check in checks)
