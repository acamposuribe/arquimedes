"""Data models for Arquimedes knowledge base."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any


# --- Enums ---


class DocumentType(str, Enum):
    REGULATION = "regulation"
    CATALOGUE = "catalogue"
    MONOGRAPH = "monograph"
    PAPER = "paper"
    LECTURE_NOTE = "lecture_note"
    PRECEDENT = "precedent"
    TECHNICAL_SPEC = "technical_spec"
    SITE_DOCUMENT = "site_document"


class Scale(str, Enum):
    DETAIL = "detail"
    BUILDING = "building"
    URBAN = "urban"
    TERRITORIAL = "territorial"


class VisualType(str, Enum):
    PLAN = "plan"
    SECTION = "section"
    ELEVATION = "elevation"
    DETAIL = "detail"
    PHOTO = "photo"
    DIAGRAM = "diagram"
    CHART = "chart"
    RENDER = "render"
    SKETCH = "sketch"


class FigureExtractionMethod(str, Enum):
    EMBEDDED = "embedded"
    RASTERIZED_REGION = "rasterized_region"


# --- Provenance ---


@dataclass
class Provenance:
    """Tracks the origin and confidence of every LLM-enriched field."""

    source_pages: list[int] = field(default_factory=list)
    evidence_spans: list[str] = field(default_factory=list)
    model: str = ""
    prompt_version: str = ""
    confidence: float = 0.0
    enriched_at: str = ""

    @classmethod
    def create(
        cls,
        model: str,
        prompt_version: str,
        confidence: float,
        source_pages: list[int] | None = None,
        evidence_spans: list[str] | None = None,
    ) -> Provenance:
        return cls(
            source_pages=source_pages or [],
            evidence_spans=evidence_spans or [],
            model=model,
            prompt_version=prompt_version,
            confidence=confidence,
            enriched_at=datetime.now(timezone.utc).isoformat(),
        )


@dataclass
class EnrichedField:
    """A value with provenance tracking."""

    value: Any
    provenance: Provenance

    def to_dict(self) -> dict:
        prov = asdict(self.provenance)
        if not prov.get("source_pages"):
            prov.pop("source_pages", None)
        if not prov.get("evidence_spans"):
            prov.pop("evidence_spans", None)
        return {"value": self.value, "provenance": prov}

    @classmethod
    def from_dict(cls, data: dict) -> EnrichedField:
        return cls(
            value=data["value"],
            provenance=Provenance(**data["provenance"]),
        )


# --- Architecture Facets ---


@dataclass
class ArchitectureFacets:
    """Domain-specific metadata for architecture materials."""

    building_type: EnrichedField | None = None
    scale: EnrichedField | None = None
    location: EnrichedField | None = None
    jurisdiction: EnrichedField | None = None
    climate: EnrichedField | None = None
    program: EnrichedField | None = None
    material_system: EnrichedField | None = None
    structural_system: EnrichedField | None = None
    historical_period: EnrichedField | None = None
    course_topic: EnrichedField | None = None
    studio_project: EnrichedField | None = None

    def to_dict(self) -> dict:
        result = {}
        for f in self.__dataclass_fields__:
            val = getattr(self, f)
            if val is not None:
                result[f] = val.to_dict()
        return result

    @classmethod
    def from_dict(cls, data: dict) -> ArchitectureFacets:
        kwargs = {}
        for f in cls.__dataclass_fields__:
            if f in data:
                kwargs[f] = EnrichedField.from_dict(data[f])
        return cls(**kwargs)


# --- Core Models ---


@dataclass
class MaterialManifest:
    """Entry in manifests/materials.jsonl — the registry of all known materials."""

    material_id: str
    file_hash: str
    relative_path: str
    file_type: str
    domain: str  # practice | research — derived from top-level LIBRARY_ROOT folder
    collection: str  # derived from second-level subfolder within domain folder
    ingested_at: str
    ingested_by: str = ""

    def to_json_line(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)

    @classmethod
    def from_json_line(cls, line: str) -> MaterialManifest:
        return cls(**json.loads(line))


@dataclass
class MaterialMeta:
    """Document-level metadata stored in extracted/<id>/meta.json.

    Raw fields are set by extract-raw. Enriched fields are added by enrich.
    """

    # Raw fields (set by extract-raw)
    material_id: str = ""
    file_hash: str = ""
    source_path: str = ""
    title: str = ""
    authors: list[str] = field(default_factory=list)
    year: str = ""
    language: str = ""
    source_url: str = ""
    page_count: int = 0
    file_type: str = ""  # pdf | image | scanned_document
    collection: str = ""
    ingested_at: str = ""  # ISO 8601 timestamp

    # Domain — derived from top-level LIBRARY_ROOT folder (Research/ or Practice/)
    domain: str = ""  # practice | research

    # Deterministic classification (set by extract-raw, no provenance needed)
    raw_keywords: list[str] = field(default_factory=list)
    raw_document_type: str = ""  # regulation | paper | catalogue | ... | "" (ambiguous)

    # Enriched fields (set by enrich, with provenance)
    document_type: EnrichedField | None = None
    summary: EnrichedField | None = None
    keywords: EnrichedField | None = None
    methodological_conclusions: EnrichedField | None = None
    main_content_learnings: EnrichedField | None = None
    facets: ArchitectureFacets | None = None

    # Enrichment stamp (set by enrich stage, used for staleness checks)
    _enrichment_stamp: dict | None = None

    def to_dict(self) -> dict:
        result = {
            "material_id": self.material_id,
            "file_hash": self.file_hash,
            "source_path": self.source_path,
            "title": self.title,
            "authors": self.authors,
            "year": self.year,
            "language": self.language,
            "source_url": self.source_url,
            "page_count": self.page_count,
            "file_type": self.file_type,
            "domain": self.domain,
            "collection": self.collection,
            "ingested_at": self.ingested_at,
            "raw_keywords": self.raw_keywords,
            "raw_document_type": self.raw_document_type,
        }
        if self.document_type is not None:
            result["document_type"] = self.document_type.to_dict()
        if self.summary is not None:
            result["summary"] = self.summary.to_dict()
        if self.keywords is not None:
            result["keywords"] = self.keywords.to_dict()
        if self.methodological_conclusions is not None:
            result["methodological_conclusions"] = self.methodological_conclusions.to_dict()
        if self.main_content_learnings is not None:
            result["main_content_learnings"] = self.main_content_learnings.to_dict()
        if self.facets is not None:
            result["facets"] = self.facets.to_dict()
        if self._enrichment_stamp is not None:
            result["_enrichment_stamp"] = self._enrichment_stamp
        return result

    @classmethod
    def from_dict(cls, data: dict) -> MaterialMeta:
        meta = cls(
            material_id=data.get("material_id", ""),
            file_hash=data.get("file_hash", ""),
            source_path=data.get("source_path", ""),
            title=data.get("title", ""),
            authors=data.get("authors", []),
            year=data.get("year", ""),
            language=data.get("language", ""),
            source_url=data.get("source_url", ""),
            page_count=data.get("page_count", 0),
            file_type=data.get("file_type", ""),
            domain=data.get("domain", ""),
            collection=data.get("collection", ""),
            ingested_at=data.get("ingested_at", ""),
            raw_keywords=data.get("raw_keywords", []),
            raw_document_type=data.get("raw_document_type", ""),
        )
        if "document_type" in data:
            meta.document_type = EnrichedField.from_dict(data["document_type"])
        if "summary" in data:
            meta.summary = EnrichedField.from_dict(data["summary"])
        if "keywords" in data:
            meta.keywords = EnrichedField.from_dict(data["keywords"])
        if "methodological_conclusions" in data:
            meta.methodological_conclusions = EnrichedField.from_dict(data["methodological_conclusions"])
        if "main_content_learnings" in data:
            meta.main_content_learnings = EnrichedField.from_dict(data["main_content_learnings"])
        if "facets" in data:
            meta.facets = ArchitectureFacets.from_dict(data["facets"])
        if "_enrichment_stamp" in data:
            meta._enrichment_stamp = data["_enrichment_stamp"]
        return meta

    def save(self, extracted_dir: Path) -> None:
        path = extracted_dir / self.material_id / "meta.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, separators=(',', ':'), ensure_ascii=False)

    @classmethod
    def load(cls, extracted_dir: Path, material_id: str) -> MaterialMeta:
        path = extracted_dir / material_id / "meta.json"
        with open(path) as f:
            return cls.from_dict(json.load(f))


@dataclass
class Page:
    """Page-level extraction stored in pages.jsonl."""

    page_number: int
    text: str
    headings: list[str] = field(default_factory=list)
    section_boundaries: list[str] = field(default_factory=list)
    figure_refs: list[str] = field(default_factory=list)
    table_refs: list[str] = field(default_factory=list)
    thumbnail_path: str = ""
    has_annotations: bool = False  # true if page contains reader highlights/notes
    annotation_ids: list[str] = field(default_factory=list)  # refs to annotations.jsonl

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> Page:
        return cls(**data)


@dataclass
class Annotation:
    """Reader annotation extracted from PDF (highlights, notes, marks).

    Stored in annotations.jsonl. These represent reader-assigned importance.
    """

    annotation_id: str
    type: str  # highlight | note | underline | strikeout | freetext
    page: int
    quoted_text: str = ""  # the highlighted/annotated text span
    comment: str = ""  # user's note text (if any)
    color: str = ""
    rect: list[float] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> Annotation:
        return cls(**data)


@dataclass
class Chunk:
    """Retrieval-sized text chunk stored in chunks.jsonl."""

    chunk_id: str
    text: str
    source_pages: list[int] = field(default_factory=list)
    emphasized: bool = False  # true if chunk overlaps annotated/highlighted spans
    annotation_overlap_ids: list[str] = field(default_factory=list)  # IDs of overlapping annotations

    # Enriched (added by enrich step)
    summary: EnrichedField | None = None
    keywords: EnrichedField | None = None
    content_class: str = ""  # argument|methodology|case_study|bibliography|front_matter|caption|appendix

    def to_dict(self) -> dict:
        result = {
            "chunk_id": self.chunk_id,
            "text": self.text,
            "source_pages": self.source_pages,
            "emphasized": self.emphasized,
            "annotation_overlap_ids": self.annotation_overlap_ids,
        }
        if self.summary is not None:
            result["summary"] = self.summary.to_dict()
        if self.keywords is not None:
            result["keywords"] = self.keywords.to_dict()
        if self.content_class:
            result["content_class"] = self.content_class
        return result

    @classmethod
    def from_dict(cls, data: dict) -> Chunk:
        chunk = cls(
            chunk_id=data["chunk_id"],
            text=data["text"],
            source_pages=data.get("source_pages", []),
            emphasized=data.get("emphasized", False),
            annotation_overlap_ids=data.get("annotation_overlap_ids", []),
        )
        if "summary" in data:
            chunk.summary = EnrichedField.from_dict(data["summary"])
        if "keywords" in data:
            chunk.keywords = EnrichedField.from_dict(data["keywords"])
        if "content_class" in data:
            chunk.content_class = data["content_class"]
        return chunk


@dataclass
class Figure:
    """Extracted figure stored as figures/fig_NNN.json sidecar."""

    figure_id: str
    source_page: int
    image_path: str
    bbox: list[float] = field(default_factory=list)
    extraction_method: str = ""  # embedded | rasterized_region

    # Enriched (added by enrich step)
    visual_type: EnrichedField | None = None
    description: EnrichedField | None = None
    caption: EnrichedField | None = None
    relevance: str = ""  # substantive|decorative|front_matter
    analysis_mode: str = ""  # vision | text_fallback
    _enrichment_stamp: dict | None = None

    def to_dict(self) -> dict:
        result = {
            "figure_id": self.figure_id,
            "source_page": self.source_page,
            "image_path": self.image_path,
            "bbox": self.bbox,
            "extraction_method": self.extraction_method,
        }
        if self.visual_type is not None:
            result["visual_type"] = self.visual_type.to_dict()
        if self.description is not None:
            result["description"] = self.description.to_dict()
        if self.caption is not None:
            result["caption"] = self.caption.to_dict()
        if self.relevance:
            result["relevance"] = self.relevance
        if self.analysis_mode:
            result["analysis_mode"] = self.analysis_mode
        if self._enrichment_stamp is not None:
            result["_enrichment_stamp"] = self._enrichment_stamp
        return result

    @classmethod
    def from_dict(cls, data: dict) -> Figure:
        fig = cls(
            figure_id=data["figure_id"],
            source_page=data["source_page"],
            image_path=data["image_path"],
            bbox=data.get("bbox", []),
            extraction_method=data.get("extraction_method", ""),
        )
        if "visual_type" in data:
            fig.visual_type = EnrichedField.from_dict(data["visual_type"])
        if "description" in data:
            fig.description = EnrichedField.from_dict(data["description"])
        if "caption" in data:
            fig.caption = EnrichedField.from_dict(data["caption"])
        if "relevance" in data:
            fig.relevance = data["relevance"]
        if "analysis_mode" in data:
            fig.analysis_mode = data["analysis_mode"]
        if "_enrichment_stamp" in data:
            fig._enrichment_stamp = data["_enrichment_stamp"]
        return fig


@dataclass
class Table:
    """Extracted table stored in tables.jsonl."""

    table_id: str
    source_page: int
    headers: list[str] = field(default_factory=list)
    rows: list[list[str]] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> Table:
        return cls(**data)


@dataclass
class ConceptCandidate:
    """LLM-identified concept candidate stored in concepts.jsonl."""

    concept_name: str
    descriptor: str = ""
    concept_type: str = "local"
    relevance: str = ""
    provenance: Provenance | None = None

    def to_dict(self) -> dict:
        result = {
            "concept_name": self.concept_name,
            "descriptor": self.descriptor,
            "concept_type": self.concept_type,
            "relevance": self.relevance,
        }
        if self.provenance is not None:
            result["provenance"] = asdict(self.provenance)
        return result

    @classmethod
    def from_dict(cls, data: dict) -> ConceptCandidate:
        prov = None
        if "provenance" in data:
            prov = Provenance(**data["provenance"])
        return cls(
            concept_name=data["concept_name"],
            descriptor=data.get("descriptor", ""),
            concept_type=data.get("concept_type", "local"),
            relevance=data.get("relevance", ""),
            provenance=prov,
        )


# --- Utility ---


def compute_material_id(file_path: Path) -> str:
    """Compute deterministic material_id from file contents: sha256[:12]."""
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        for block in iter(lambda: f.read(65536), b""):
            h.update(block)
    return h.hexdigest()[:12]


def compute_file_hash(file_path: Path) -> str:
    """Compute full sha256 hash of file contents."""
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        for block in iter(lambda: f.read(65536), b""):
            h.update(block)
    return h.hexdigest()
