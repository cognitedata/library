"""Index query functions."""

from __future__ import annotations

import logging
from typing import Any

from inverted_index.config import INDEX_STORAGE_CONFIG, SCOPE_CONFIG
from inverted_index.normalize import normalize_query_terms
from inverted_index.raw_ops import list_registered_scope_keys
from inverted_index.scope import build_scope_key
from inverted_index.storage import get_storage_adapter
from inverted_index.storage.raw_keys import merge_postings

logger = logging.getLogger(__name__)


def resolve_query_scope_keys(
    client: Any,
    storage_config: dict,
    *,
    match_scope_key: str | None = None,
    match_scope_keys: list[str] | None = None,
    all_scopes: bool = False,
    storage_adapter: Any = None,
) -> list[str]:
    """Resolve scope keys for multi-scope analytics queries."""
    if match_scope_key and match_scope_keys:
        raise ValueError("Provide match_scope_key or match_scope_keys, not both")

    if all_scopes:
        local_registry = getattr(storage_adapter, "_local_registry", None)
        scopes = list_registered_scope_keys(
            client,
            storage_config,
            local_registry=local_registry,
        )
        if not scopes:
            if not SCOPE_CONFIG.get("enabled", False):
                fallback = str(SCOPE_CONFIG.get("fallback_scope_key") or "global").strip()
                if fallback:
                    logger.info(
                        "Partition registry empty; using fallback scope %s", fallback
                    )
                    return [fallback]
            raise ValueError(
                "No scopes in partition registry; run build-metadata/build-annotations "
                "(without dry run) first or pass explicit scope keys"
            )
        return scopes

    if match_scope_keys:
        scopes = [str(s).strip() for s in match_scope_keys if str(s).strip()]
        if not scopes:
            raise ValueError("match_scope_keys must contain at least one non-empty scope")
        return scopes

    if match_scope_key:
        scope = str(match_scope_key).strip()
        if not scope:
            raise ValueError("match_scope_key must be non-empty")
        return [scope]

    raise ValueError(
        "Scope required: pass match_scope_key, match_scope_keys, or all_scopes=True"
    )


def query_index_by_terms(
    client: Any,
    terms: list[str],
    match_scope_key: str | None = None,
    match_scope_keys: list[str] | None = None,
    match_scope: dict | None = None,
    source_types: list[str] | None = None,
    min_confidence: float = 0.0,
    limit: int = 5000,
    include_additional_metadata: bool = True,
    strict_scope: bool = True,
    storage_config: dict | None = None,
    storage_adapter: Any = None,
) -> list[dict]:
    """Primary lookup — normalize terms and return matching index entries."""
    del include_additional_metadata
    normalized_terms = normalize_query_terms(terms)
    if not normalized_terms:
        return []

    cfg = storage_config or INDEX_STORAGE_CONFIG
    adapter = storage_adapter or get_storage_adapter(cfg, client)

    scope_keys: list[str] | None = None
    if match_scope_keys is not None:
        scope_keys = resolve_query_scope_keys(
            client,
            cfg,
            match_scope_keys=match_scope_keys,
            storage_adapter=adapter,
        )
    elif match_scope_key:
        scope_keys = [match_scope_key]
    elif match_scope:
        built = build_scope_key(match_scope, SCOPE_CONFIG)
        if built:
            scope_keys = [built]

    if strict_scope and SCOPE_CONFIG.get("enabled", False) and SCOPE_CONFIG.get("levels"):
        if not scope_keys:
            logger.warning("query_index_by_terms: scope required but not provided")
            return []

    if scope_keys and len(scope_keys) == 1:
        return adapter.query_by_terms(
            normalized_terms,
            match_scope_key=scope_keys[0],
            source_types=source_types,
            min_confidence=min_confidence,
            limit=limit,
        )

    if scope_keys:
        return adapter.query_by_terms(
            normalized_terms,
            match_scope_keys=scope_keys,
            source_types=source_types,
            min_confidence=min_confidence,
            limit=limit,
        )

    return adapter.query_by_terms(
        normalized_terms,
        match_scope_key=None,
        source_types=source_types,
        min_confidence=min_confidence,
        limit=limit,
    )


def query_references_for_aliases(
    client: Any,
    aliases: list[str],
    match_scope_key: str | None = None,
    match_scope: dict | None = None,
    **kwargs: Any,
) -> list[dict]:
    """Thin wrapper around query_index_by_terms for target-driven flows."""
    return query_index_by_terms(
        client,
        terms=aliases,
        match_scope_key=match_scope_key,
        match_scope=match_scope,
        **kwargs,
    )


def list_index_entries_by_file(
    client: Any,
    file_external_id: str,
    *,
    file_space: str = "cdf_cdm",
    match_scope_key: str | None = None,
    source_types: list[str] | None = None,
    storage_config: dict | None = None,
    storage_adapter: Any = None,
    limit: int = 5000,
) -> list[dict]:
    """List index entries whose reference is the given CogniteFile."""
    cfg = storage_config or INDEX_STORAGE_CONFIG
    adapter = storage_adapter or get_storage_adapter(cfg, client)
    if not hasattr(adapter, "list_by_file"):
        raise TypeError(f"Storage adapter {type(adapter)} has no list_by_file method")

    entries = adapter.list_by_file(
        file_external_id,
        source_types=source_types,
        file_space=file_space,
        match_scope_key=match_scope_key,
        limit=limit,
    )
    merged = merge_postings([], entries)
    if match_scope_key:
        merged = [e for e in merged if e.get("match_scope_key") == match_scope_key]
    return merged[:limit]
