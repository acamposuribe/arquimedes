from pathlib import Path

from PIL import Image

from arquimedes.extract_image import _is_likely_scanned_document


def _write_image(path: Path, size=(1200, 1800)) -> None:
    Image.new("RGB", size, color="white").save(path)


def test_project_photo_folder_is_not_treated_as_scanned_document(tmp_path):
    photo_dir = tmp_path / "Proyectos" / "2410-casa" / "01_INFO" / "02_ESTADO ACTUAL" / "01_FOTOS"
    photo_dir.mkdir(parents=True)
    image_path = photo_dir / "IMG_1625.JPG"
    _write_image(image_path)

    assert _is_likely_scanned_document(image_path) is False


def test_paper_like_aspect_ratio_alone_is_not_scanned_document(tmp_path):
    image_path = tmp_path / "random-name.jpg"
    _write_image(image_path, size=(1200, 1800))

    assert _is_likely_scanned_document(image_path) is False


def test_scan_folder_is_treated_as_scanned_document(tmp_path):
    scan_dir = tmp_path / "Research" / "archive" / "scans"
    scan_dir.mkdir(parents=True)
    image_path = scan_dir / "page_001.jpg"
    _write_image(image_path)

    assert _is_likely_scanned_document(image_path) is True
