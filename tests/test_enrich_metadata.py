"""Tests for enrich_metadata: thumbnail-based metadata correction stage."""

from __future__ import annotations

import base64
import json
from pathlib import Path
from unittest.mock import MagicMock

from arquimedes.enrich_metadata import enrich_metadata_stage


def _make_png(path: Path) -> Path:
    png_bytes = base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwADhQGAWjR9awAAAABJRU5ErkJggg=="
    )
    path.write_bytes(png_bytes)
    return path


def _make_extracted_dir(tmp_path: Path) -> Path:
    output_dir = tmp_path / "extracted" / "test123"
    output_dir.mkdir(parents=True)
    thumbs_dir = output_dir / "thumbnails"
    thumbs_dir.mkdir()
    _make_png(thumbs_dir / "page_0001.png")
    _make_png(thumbs_dir / "page_0002.png")

    meta = {
        "material_id": "test123",
        "title": "Microsoft Word - Draft.docx",
        "authors": ["Wrong Author"],
        "year": "",
        "page_count": 2,
    }
    (output_dir / "meta.json").write_text(json.dumps(meta), encoding="utf-8")

    pages = [
        {
            "page_number": 1,
            "text": "Decolonizing Knowledge and the Question of the Archive Achille Mbembe 2015",
            "thumbnail_path": "thumbnails/page_0001.png",
        },
        {
            "page_number": 2,
            "text": "Second page text",
            "thumbnail_path": "thumbnails/page_0002.png",
        },
    ]
    with open(output_dir / "pages.jsonl", "w", encoding="utf-8") as handle:
        for page in pages:
            handle.write(json.dumps(page) + "\n")

    return output_dir


def _make_config() -> dict:
    return {
        "llm": {"agent_cmd": "test-agent --print"},
        "enrichment": {
            "prompt_version": "enrich-v1.0",
            "enrichment_schema_version": "1",
            "max_retries": 3,
            "llm_routes": {
                "metadata": [
                    {"provider": "copilot", "command": "copilot", "model": "gpt-5.4-mini"}
                ]
            },
        },
    }


class TestEnrichMetadataStage:
    def test_updates_title_authors_and_year(self, tmp_path):
        output_dir = _make_extracted_dir(tmp_path)
        config = _make_config()

        def _side_effect(system, messages):
            return json.dumps(
                {
                    "title": "Decolonizing Knowledge and the Question of the Archive",
                    "authors": ["Achille Mbembe"],
                    "year": "2015",
                    "_finished": True,
                }
            )

        llm_fn = MagicMock(side_effect=_side_effect)
        llm_fn.last_model = "copilot:gpt-5.4-mini"

        result = enrich_metadata_stage(output_dir, config, llm_fn, force=True)

        assert result["status"] == "enriched"
        raw_text = (output_dir / "meta.json").read_text(encoding="utf-8")
        meta = json.loads(raw_text)
        assert meta["title"] == "Decolonizing Knowledge and the Question of the Archive"
        assert meta["authors"] == ["Achille Mbembe"]
        assert meta["year"] == "2015"
        assert meta["_metadata_fix_stamp"]["model"] == "copilot:gpt-5.4-mini"
        assert raw_text.count("\n") == 1

    def test_skips_when_not_stale(self, tmp_path):
        output_dir = _make_extracted_dir(tmp_path)
        config = _make_config()
        llm_fn = MagicMock(
            return_value=json.dumps(
                {
                    "title": "Decolonizing Knowledge and the Question of the Archive",
                    "authors": ["Achille Mbembe"],
                    "year": "2015",
                    "_finished": True,
                }
            )
        )
        llm_fn.last_model = "copilot:gpt-5.4-mini"

        first = enrich_metadata_stage(output_dir, config, llm_fn, force=True)
        second = enrich_metadata_stage(output_dir, config, llm_fn, force=False)

        assert first["status"] == "enriched"
        assert second["status"] == "skipped"
        assert llm_fn.call_count == 1