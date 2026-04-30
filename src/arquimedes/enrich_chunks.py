"""Chunk enrichment stage — Phase 3.

Enriches per-chunk metadata: summary and keywords for each text chunk.
Uses batched LLM calls with a durable work file so completed batches survive
interrupted runs. Canonical chunk files are updated only after the full stage
finishes successfully.
"""

from __future__ import annotations

import json
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime, timezone
from pathlib import Path

from arquimedes import enrich_prompts, enrich_stamps, llm
from arquimedes.domain_profiles import domain_prompt_version
from arquimedes.llm import get_model_id
from arquimedes.enrich_prompts import estimate_tokens


# ---------------------------------------------------------------------------
# Helpers — load artifacts
# ---------------------------------------------------------------------------


def _load_jsonl(path: Path) -> list[dict]:
    """Load a .jsonl file into a list of dicts. Returns [] if absent."""
    if not path.exists():
        return []
    records = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            records.append(json.loads(line))
    return records


def _load_json(path: Path, default=None):
    """Load a JSON file. Returns default if absent."""
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _toc_headings(toc: object) -> list[str]:
    """Return title strings from permissive TOC shapes."""
    if not isinstance(toc, list):
        return []
    headings: list[str] = []
    for entry in toc:
        if isinstance(entry, str):
            title = entry.strip()
        elif isinstance(entry, dict):
            title = str(entry.get("title", "")).strip()
        else:
            title = ""
        if title:
            headings.append(title)
    return headings


def _chunk_work_path(output_dir: Path) -> Path:
    return output_dir / "chunk_enrichment.work.json"


def _load_chunk_work(output_dir: Path) -> dict[str, dict]:
    work_path = _chunk_work_path(output_dir)
    if not work_path.exists():
        return {}
    try:
        payload = json.loads(work_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    chunks = payload.get("chunks", {}) if isinstance(payload, dict) else {}
    return chunks if isinstance(chunks, dict) else {}


def _write_chunk_work(output_dir: Path, work_chunks: dict[str, dict]) -> None:
    work_path = _chunk_work_path(output_dir)
    tmp_path = work_path.with_suffix(".json.tmp")
    payload = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "chunks": work_chunks,
    }
    tmp_path.write_text(
        json.dumps(payload, separators=(",", ":"), ensure_ascii=False),
        encoding="utf-8",
    )
    tmp_path.replace(work_path)


def _delete_chunk_work(output_dir: Path) -> None:
    try:
        _chunk_work_path(output_dir).unlink(missing_ok=True)
    except OSError:
        pass


def _save_failed_response(output_dir: Path, raw_text: str, detail: str) -> None:
    debug_dir = output_dir / "debug"
    debug_dir.mkdir(exist_ok=True)
    response_path = debug_dir / "chunks.failed.response.txt"
    meta_path = debug_dir / "chunks.failed.meta.json"
    response_path.write_text(raw_text or "", encoding="utf-8")
    meta_path.write_text(
        json.dumps(
            {
                "failed_at": datetime.now(timezone.utc).isoformat(),
                "detail": detail,
                "response_chars": len(raw_text or ""),
            },
            separators=(",", ":"),
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


def _failed_with_response(output_dir: Path, raw_text: str, detail: str) -> dict:
    try:
        _save_failed_response(output_dir, raw_text, detail)
    except Exception:
        pass
    return {"status": "failed", "detail": detail}


def _format_batch_responses(batch_responses: dict[int, str]) -> str:
    parts: list[str] = []
    for batch_idx in sorted(batch_responses):
        parts.append(f"--- batch {batch_idx + 1} raw response ---\n{batch_responses[batch_idx] or ''}")
    return "\n\n".join(parts)


def _has_complete_work_entry(entry: dict | None, target_stamp: dict) -> bool:
    if not isinstance(entry, dict):
        return False
    enrichment = entry.get("enrichment")
    stamp = entry.get("stamp")
    if not isinstance(enrichment, dict) or "summary" not in enrichment:
        return False
    if not isinstance(stamp, dict):
        return False
    return not enrich_stamps.is_stale(stamp, target_stamp)


def _promote_chunks(
    chunks: list[dict],
    enriched_chunks_map: dict[str, dict],
    chunks_path: Path,
    output_dir: Path,
    stamps: dict[str, dict],
) -> None:
    tmp_chunks = chunks_path.with_suffix(".jsonl.tmp")
    with open(tmp_chunks, "w", encoding="utf-8") as f:
        for chunk in chunks:
            cid = chunk.get("chunk_id", "")
            f.write(json.dumps(enriched_chunks_map.get(cid, chunk), ensure_ascii=False) + "\n")
    tmp_chunks.replace(chunks_path)
    enrich_stamps.write_chunk_stamps(output_dir, stamps)
    _delete_chunk_work(output_dir)


def _debug_enabled() -> bool:
    value = os.getenv("ARQ_LLM_DEBUG", "")
    return value.strip().lower() not in {"", "0", "false", "no", "off"}


def _debug(message: str) -> None:
    if _debug_enabled():
        print(f"[chunk-debug] {message}", file=sys.stderr)


def _configured_parallel_requests(config: dict, key: str) -> int:
    value = config.get("enrichment", {}).get(key, 1)
    try:
        return max(1, int(value))
    except (TypeError, ValueError):
        return 1


def _actual_model_name(llm_candidate, default: str) -> str:
    value = getattr(llm_candidate, "last_model", default)
    return value if isinstance(value, str) and value.strip() else default


# ---------------------------------------------------------------------------
# Schema description for repair fallback
# ---------------------------------------------------------------------------

_VALID_CONTENT_CLASSES = frozenset(
    {"argument", "methodology", "case_study", "bibliography", "front_matter", "caption", "appendix"}
)

_FENCE_RE = re.compile(r"```(?:json|jsonl)?\s*(.*?)```", re.DOTALL | re.IGNORECASE)
_JSON_OBJECT_RE = re.compile(r"\{[^{}]*\}", re.DOTALL)


def _normalize_chunk_response_obj(obj: dict) -> tuple[str, dict] | None:
    chunk_id = str(obj.get("id") or obj.get("chunk_id") or "").strip()
    if not chunk_id:
        return None

    enrichment: dict = {}
    s = str(obj.get("s") or obj.get("summary") or "").strip()
    if s:
        enrichment["summary"] = " ".join(s.split())

    kw = obj.get("kw") or obj.get("keywords")
    if isinstance(kw, list):
        keywords = [" ".join(str(k).split()) for k in kw if str(k).strip()]
        if keywords:
            enrichment["keywords"] = keywords[:3]

    cls = str(obj.get("cls") or obj.get("content_class") or "").strip()
    if cls in _VALID_CONTENT_CLASSES:
        enrichment["content_class"] = cls

    if not enrichment:
        return None
    return chunk_id, enrichment


def _extract_chunk_response_objects(raw_text: str) -> list[dict]:
    """Recover chunk response objects from strict or lightly malformed JSONL.

    Some agent CLIs wrap JSONL in bullets, line-wrap inside strings, or place
    adjacent objects on one physical line. Keep strict JSON parsing first, then
    fall back to extracting object-looking spans and replacing accidental line
    breaks inside each span with spaces.
    """
    text = raw_text.strip()
    if not text:
        return []

    match = _FENCE_RE.search(text)
    if match:
        text = match.group(1).strip()

    objects: list[dict] = []

    def _append_value(value) -> bool:
        if isinstance(value, dict):
            items = value.get("chunks") or value.get("items")
            if isinstance(items, list):
                appended = False
                for item in items:
                    if isinstance(item, dict):
                        objects.append(item)
                        appended = True
                return appended
            if "id" in value or "chunk_id" in value:
                objects.append(value)
                return True
            return False
        if isinstance(value, list):
            appended = False
            for item in value:
                if isinstance(item, dict):
                    objects.append(item)
                    appended = True
            return appended
        return False

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        parsed = None
    if parsed is not None and _append_value(parsed):
        return objects

    for line in text.splitlines():
        line = line.strip().removeprefix("●").removeprefix("•").strip().rstrip(",")
        if not line or not line.startswith("{"):
            continue
        try:
            parsed_line = json.loads(line)
        except json.JSONDecodeError:
            continue
        _append_value(parsed_line)

    if objects:
        return objects

    for match in _JSON_OBJECT_RE.finditer(text):
        candidate = " ".join(match.group(0).split())
        try:
            parsed_obj = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed_obj, dict):
            objects.append(parsed_obj)

    return objects


def _parse_chunk_jsonl(raw_text: str) -> dict[str, dict]:
    """Parse compact JSONL chunk response into chunk_id → enrichment dict.

    Expected line: {"id":"chk_XXXXX","cls":"...","kw":["..."],"s":"..."}
    Also tolerates common agent formatting glitches such as bullets, wrapped
    strings, fenced output, arrays, or adjacent objects on one line.
    """
    result: dict[str, dict] = {}
    for obj in _extract_chunk_response_objects(raw_text):
        normalized = _normalize_chunk_response_obj(obj)
        if normalized is None:
            continue
        chunk_id, enrichment = normalized
        result[chunk_id] = enrichment
    return result


# ---------------------------------------------------------------------------
# Field-level helper
# ---------------------------------------------------------------------------


def _make_enrichment(parsed: dict, model: str, prompt_version: str) -> dict:
    """Build chunk enrichment dict from parsed JSONL entry.

    Wraps summary and keywords in value-only shape for compatibility with
    index._val() and index._kw_json() which expect {"value": ...}.
    """
    del model, prompt_version
    enrichment: dict = {}
    s = parsed.get("summary", "")
    if s:
        enrichment["summary"] = {"value": s}
    kw = parsed.get("keywords", [])
    if kw:
        enrichment["keywords"] = {"value": kw}
    cls = parsed.get("content_class", "")
    if cls:
        enrichment["content_class"] = cls
    return enrichment


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def enrich_chunks_stage(
    output_dir: Path,
    config: dict,
    llm_fn,
    *,
    force: bool = False,
) -> dict:
    """Enrich chunk-level metadata for a single material.

    Args:
        output_dir: Path to extracted/<material_id>/ directory.
        config: Full config dict.
        llm_fn: Callable (system, messages) -> str. The LLM implementation.
        force: Re-enrich even if not stale.

    Returns:
        {"status": "enriched"|"skipped"|"failed", "detail": str}
    """
    enrichment_config = config.get("enrichment", {})
    model: str = get_model_id(config, "chunk")
    prompt_version: str = enrichment_config.get("prompt_version", "enrich-v1.0")
    schema_version: str = enrichment_config.get("enrichment_schema_version", "1")
    batch_target: int = enrichment_config.get("chunk_batch_target", 50)

    chunks_path = output_dir / "chunks.jsonl"

    if force:
        _delete_chunk_work(output_dir)

    # 1. Load artifacts
    try:
        chunks = _load_jsonl(chunks_path)
        meta = _load_json(output_dir / "meta.json", default={})
        toc = _load_json(output_dir / "toc.json", default=None)
    except Exception as exc:
        return {"status": "failed", "detail": f"Load error: {exc}"}

    prompt_version = domain_prompt_version(prompt_version, str(meta.get("domain", "")))

    if not chunks:
        return {"status": "skipped", "detail": "no chunks"}

    # 2. Build doc_context for fingerprint and prompt
    # Collect headings from toc or from pages
    headings: list[str] = []
    if toc and isinstance(toc, list):
        headings = _toc_headings(toc)
    else:
        pages = _load_jsonl(output_dir / "pages.jsonl")
        for page in pages:
            headings.extend(page.get("headings", []))

    # Include current doc summary + stamp if present, to detect doc-level re-enrichment
    doc_context: dict = {
        "title": meta.get("title", ""),
        "raw_document_type": meta.get("raw_document_type", ""),
        "headings": headings,
    }
    if "summary" in meta:
        doc_context["summary"] = meta["summary"]
    existing_doc_stamp = enrich_stamps.read_document_stamp(output_dir)
    if existing_doc_stamp:
        doc_context["doc_stamp"] = existing_doc_stamp

    # 3. Compute per-chunk fingerprints and staleness
    try:
        annotations = _load_jsonl(output_dir / "annotations.jsonl")
    except Exception as exc:
        return {"status": "failed", "detail": f"Load annotations error: {exc}"}

    existing_stamps = enrich_stamps.read_chunk_stamps(output_dir)
    work_chunks = {} if force else _load_chunk_work(output_dir)

    # Build an in-memory map of all chunks (preserves existing enrichment on fresh ones).
    enriched_chunks_map: dict[str, dict] = {
        c.get("chunk_id", ""): dict(c) for c in chunks
    }

    stale_chunks: list[dict] = []
    stale_stamps: dict[str, dict] = {}  # chunk_id → target stamp for stale chunks
    resumed_chunk_ids: set[str] = set()
    for chunk in chunks:
        chunk_id = chunk.get("chunk_id", "")
        fp = enrich_stamps.single_chunk_fingerprint(chunk, annotations, doc_context)
        target_stamp = enrich_stamps.make_stamp(prompt_version, model, schema_version, fp)
        existing = existing_stamps.get(chunk_id)
        canonical_is_stale = force or enrich_stamps.is_stale(existing, target_stamp)
        if not canonical_is_stale:
            continue

        work_entry = work_chunks.get(chunk_id)
        if not force and _has_complete_work_entry(work_entry, target_stamp):
            enrichment = work_entry.get("enrichment", {})
            stamp = work_entry.get("stamp", {})
            if chunk_id in enriched_chunks_map:
                enriched_chunks_map[chunk_id].update(enrichment)
            existing_stamps[chunk_id] = stamp
            resumed_chunk_ids.add(chunk_id)
            continue

        stale_chunks.append(chunk)
        stale_stamps[chunk_id] = target_stamp

    if not stale_chunks:
        if resumed_chunk_ids:
            try:
                _promote_chunks(chunks, enriched_chunks_map, chunks_path, output_dir, existing_stamps)
            except Exception as exc:
                return {"status": "failed", "detail": f"Write error: {exc}"}
            detail = f"resumed {len(resumed_chunk_ids)} chunk{'s' if len(resumed_chunk_ids) != 1 else ''} from work file"
            return {"status": "enriched", "detail": detail}
        _delete_chunk_work(output_dir)
        return {"status": "skipped", "detail": "up to date"}

    # 4. Build doc_context_str for prompts
    doc_context_str = enrich_prompts.build_document_context(meta, toc, headings if headings else None)

    # 5. Batch stale chunks and checkpoint completed batches to a work file.
    # Key: chunk_id → enrichment dict
    chunk_enrichments: dict[str, dict] = {}

    avg_chunk_tokens = max(
        sum(estimate_tokens(c.get("text", "")) for c in stale_chunks) // len(stale_chunks), 1
    )
    batch_token_budget = batch_target * avg_chunk_tokens

    batches: list[list[dict]] = []
    current_batch: list[dict] = []
    current_tokens = 0
    for chunk in stale_chunks:
        tok = estimate_tokens(chunk.get("text", ""))
        if current_batch and current_tokens + tok > batch_token_budget:
            batches.append(current_batch)
            current_batch = [chunk]
            current_tokens = tok
        else:
            current_batch.append(chunk)
            current_tokens += tok
    if current_batch:
        batches.append(current_batch)

    n_batches = len(batches)
    if _debug_enabled():
        _debug(
            f"material={output_dir.name} stale_chunks={len(stale_chunks)} "
            f"batch_target={batch_target} avg_chunk_tokens={avg_chunk_tokens} "
            f"batch_token_budget={batch_token_budget} batches={n_batches}"
        )

    llm_factory = llm_fn.__dict__.get("_arq_factory") if hasattr(llm_fn, "__dict__") else None
    requested_parallelism = _configured_parallel_requests(config, "chunk_parallel_requests")
    can_parallelize = callable(llm_factory) and len(batches) > 1
    worker_count = min(requested_parallelism, len(batches)) if can_parallelize else 1

    if _debug_enabled():
        _debug(
            f"material={output_dir.name} chunk_parallel_requests={requested_parallelism} "
            f"worker_count={worker_count} clone_factory={'yes' if callable(llm_factory) else 'no'}"
        )

    def _run_batch(batch_idx: int, batch: list[dict]) -> tuple[int, dict[str, dict], str, str]:
        batch_llm_fn = llm_factory() if worker_count > 1 else llm_fn
        if _debug_enabled():
            batch_ids = ",".join(c.get("chunk_id", "") for c in batch)
            _debug(f"batch={batch_idx + 1}/{n_batches} size={len(batch)} chunk_ids={batch_ids}")
        system, messages = enrich_prompts.build_chunk_batch_prompt(
            batch,
            doc_context_str,
            annotations,
            domain=str(meta.get("domain", "")),
        )
        raw_text = batch_llm_fn(system, messages)
        actual_model = _actual_model_name(batch_llm_fn, model)
        parsed_batch = _parse_chunk_jsonl(raw_text)
        batch_chunk_ids = {chunk.get("chunk_id", "") for chunk in batch}
        batch_enrichments: dict[str, dict] = {
            chunk_id: _make_enrichment(parsed, actual_model, prompt_version)
            for chunk_id, parsed in parsed_batch.items()
            if parsed and chunk_id in batch_chunk_ids
        }
        return batch_idx, batch_enrichments, actual_model, raw_text

    def _checkpoint_batch(batch_enrichments: dict[str, dict], actual_model: str) -> dict[str, dict]:
        enriched_at = datetime.now(timezone.utc).isoformat()
        batch_stamps: dict[str, dict] = {}
        for chunk_id, enrichment in batch_enrichments.items():
            if "summary" not in enrichment:
                raise ValueError(f"Chunk '{chunk_id}' missing required field: summary")
            target_stamp = stale_stamps.get(chunk_id)
            if target_stamp is None:
                continue
            stamp = target_stamp.copy()
            stamp["model"] = actual_model
            stamp["enriched_at"] = enriched_at
            batch_stamps[chunk_id] = stamp

        for chunk_id, enrichment in batch_enrichments.items():
            work_chunks[chunk_id] = {
                "enrichment": enrichment,
                "stamp": batch_stamps.get(chunk_id, {}),
            }
        _write_chunk_work(output_dir, work_chunks)
        return batch_stamps

    batch_results: dict[int, tuple[dict[str, dict], dict[str, dict]]] = {}
    batch_raw_responses: dict[int, str] = {}
    if worker_count > 1:
        with ThreadPoolExecutor(max_workers=worker_count) as pool:
            next_batch_idx = 0
            first_failure: tuple[int, str] | None = None

            while next_batch_idx < len(batches):
                wave_end = min(next_batch_idx + worker_count, len(batches))
                wave_futures = {
                    pool.submit(_run_batch, batch_idx, batches[batch_idx]): batch_idx
                    for batch_idx in range(next_batch_idx, wave_end)
                }
                next_batch_idx = wave_end

                done, _ = wait(set(wave_futures.keys()))
                successful_results: list[tuple[int, dict[str, dict], str, str]] = []
                for future in done:
                    batch_idx = wave_futures[future]
                    try:
                        successful_results.append(future.result())
                    except llm.EnrichmentError as exc:
                        if first_failure is None:
                            first_failure = (
                                batch_idx,
                                f"Batch {batch_idx + 1}/{n_batches} LLM error: {exc}",
                            )
                    except Exception as exc:
                        if first_failure is None:
                            first_failure = (
                                batch_idx,
                                f"Batch {batch_idx + 1}/{n_batches} error: {exc}",
                            )

                for result_idx, batch_enrichments, actual_model, raw_text in successful_results:
                    batch_idx = result_idx
                    batch_raw_responses[result_idx] = raw_text
                    try:
                        batch_stamps = _checkpoint_batch(batch_enrichments, actual_model)
                    except Exception as exc:
                        if first_failure is None:
                            first_failure = (
                                batch_idx,
                                f"Batch {batch_idx + 1}/{n_batches} error: {exc}",
                            )
                        continue
                    batch_results[result_idx] = (batch_enrichments, batch_stamps)

                if first_failure is not None:
                    raw_text = batch_raw_responses.get(first_failure[0], _format_batch_responses(batch_raw_responses))
                    return _failed_with_response(output_dir, raw_text, first_failure[1])
    else:
        for batch_idx, batch in enumerate(batches):
            try:
                result_idx, batch_enrichments, actual_model, raw_text = _run_batch(batch_idx, batch)
                batch_raw_responses[result_idx] = raw_text
            except llm.EnrichmentError as exc:
                return {"status": "failed", "detail": f"Batch {batch_idx + 1}/{n_batches} LLM error: {exc}"}
            except Exception as exc:
                return {"status": "failed", "detail": f"Batch {batch_idx + 1}/{n_batches} error: {exc}"}
            try:
                batch_stamps = _checkpoint_batch(batch_enrichments, actual_model)
            except Exception as exc:
                detail = f"Batch {batch_idx + 1}/{n_batches} error: {exc}"
                return _failed_with_response(output_dir, raw_text, detail)
            batch_results[result_idx] = (batch_enrichments, batch_stamps)

    for batch_idx in range(n_batches):
        batch_enrichments, batch_stamps = batch_results[batch_idx]
        for chunk_id, enrichment in batch_enrichments.items():
            if chunk_id in enriched_chunks_map:
                enriched_chunks_map[chunk_id].update(enrichment)
            chunk_enrichments[chunk_id] = enrichment
            if chunk_id in batch_stamps:
                existing_stamps[chunk_id] = batch_stamps[chunk_id]

    for chunk_id in resumed_chunk_ids:
        work_entry = work_chunks.get(chunk_id, {})
        enrichment = work_entry.get("enrichment", {})
        if chunk_id in enriched_chunks_map:
            enriched_chunks_map[chunk_id].update(enrichment)
        if isinstance(enrichment, dict):
            chunk_enrichments[chunk_id] = enrichment

    # 6. Validate completeness. Allow up to 3 missing chunks (LLM occasionally skips one).
    stale_ids = {c.get("chunk_id", "") for c in stale_chunks if c.get("chunk_id")}
    missing_ids = stale_ids - set(chunk_enrichments.keys())
    if len(missing_ids) > 3:
        detail = f"LLM output missing {len(missing_ids)} chunk(s): {sorted(missing_ids)[:5]}"
        return _failed_with_response(output_dir, _format_batch_responses(batch_raw_responses), detail)
    if missing_ids:
        print(
            f"  [chunk] warning: {len(missing_ids)} chunk(s) not returned by LLM "
            f"({sorted(missing_ids)}), proceeding without them",
            file=sys.stderr,
        )

    # Validate each resumed or newly enriched chunk has required summary field.
    for cid, enrichment in chunk_enrichments.items():
        if "summary" not in enrichment:
            detail = f"Chunk '{cid}' missing required field: summary"
            return _failed_with_response(output_dir, _format_batch_responses(batch_raw_responses), detail)

    try:
        _promote_chunks(chunks, enriched_chunks_map, chunks_path, output_dir, existing_stamps)
    except Exception as exc:
        return {"status": "failed", "detail": f"Write error: {exc}"}

    n_enriched = len(chunk_enrichments)
    detail = f"{n_enriched} chunks, {n_batches} batch{'es' if n_batches != 1 else ''}"
    return {"status": "enriched", "detail": detail}
