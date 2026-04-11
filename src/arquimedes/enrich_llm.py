"""Backward-compatible shim for legacy `arquimedes.enrich_llm` imports.

Shared LLM and agent-CLI infrastructure now lives in `arquimedes.llm` because
the same factory and routing layer is used across enrichment, clustering,
lint, and other AI-assisted flows.
"""

from __future__ import annotations

from arquimedes.llm import (
    EnrichmentError,
    LlmFn,
    _build_agent_cmd,
    _build_prompt_text,
    _build_stage_request,
    _command_to_parts,
    _parse_json_prefix,
    _provider_from_parts,
    _route_flag,
    _route_signature,
    _run_agent_subprocess,
    _stage_route_config,
    _strip_fences,
    check_claude_oauth_usage,
    get_agent_model_name,
    get_model_id,
    make_cli_llm_fn,
    parse_json_or_repair,
)
