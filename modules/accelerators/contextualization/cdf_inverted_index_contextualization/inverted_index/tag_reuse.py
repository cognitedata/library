"""Cross-scope tag reuse analytics (admin workload)."""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from collections.abc import Callable
from typing import Any

from inverted_index.cancellation import raise_if_cancelled

from inverted_index.config import INDEX_STORAGE_CONFIG, TAG_REUSE_AUDIT_POLICY
from inverted_index.normalize import normalize_query_terms
from inverted_index.query import query_index_by_terms, resolve_query_scope_keys
from inverted_index.raw_ops import iter_partition_terms
from inverted_index.storage import get_storage_adapter

logger = logging.getLogger(__name__)


def _hit_dedupe_key(hit: dict) -> tuple:
    return (
        hit.get("normalized_term"),
        hit.get("match_scope_key"),
        hit.get("reference_external_id"),
        hit.get("source_property"),
        hit.get("source_type"),
    )


def _term_summary_from_hits(normalized_term: str, hits: list[dict]) -> dict:
    scopes: set[str] = set()
    reference_count_by_scope: dict[str, int] = defaultdict(int)
    display_term = normalized_term
    seen_keys: set[tuple] = set()
    hit_count = 0

    for hit in hits:
        if hit.get("normalized_term") != normalized_term:
            continue
        scope = str(hit.get("match_scope_key") or "").strip()
        if scope:
            scopes.add(scope)
        dedupe = _hit_dedupe_key(hit)
        if dedupe in seen_keys:
            continue
        seen_keys.add(dedupe)
        hit_count += 1
        if scope:
            reference_count_by_scope[scope] += 1
        if hit.get("term"):
            display_term = str(hit["term"])

    scope_list = sorted(scopes)
    return {
        "normalized_term": normalized_term,
        "term": display_term,
        "scope_count": len(scope_list),
        "scopes": scope_list,
        "hit_count": hit_count,
        "reference_count_by_scope": dict(reference_count_by_scope),
        "cross_scope_duplicate": len(scope_list) > 1,
    }


def summarize_tag_scope_reuse(
    hits: list[dict],
    *,
    terms_queried: list[str] | None = None,
    scopes_queried: list[str] | None = None,
    reuse_only: bool = False,
) -> dict:
    """Aggregate per-term cross-scope reuse metrics from index query hits."""
    terms_with_data: set[str] = set()
    for hit in hits:
        term = hit.get("normalized_term")
        if term:
            terms_with_data.add(str(term))

    if terms_queried:
        ordered_terms = [t for t in terms_queried if t in terms_with_data]
        for term in sorted(terms_with_data):
            if term not in ordered_terms:
                ordered_terms.append(term)
    else:
        ordered_terms = sorted(terms_with_data)

    by_term = [_term_summary_from_hits(term, hits) for term in ordered_terms]
    if reuse_only:
        by_term = [row for row in by_term if row["cross_scope_duplicate"]]

    cross_scope_duplicate_count = sum(1 for row in by_term if row["cross_scope_duplicate"])
    terms_with_hits = len(ordered_terms)
    rate = cross_scope_duplicate_count / terms_with_hits if terms_with_hits else 0.0

    return {
        "scopes_queried": scopes_queried or [],
        "terms_queried": terms_queried or [],
        "terms_with_hits": terms_with_hits,
        "cross_scope_duplicate_count": cross_scope_duplicate_count,
        "cross_scope_duplicate_rate": rate,
        "by_term": by_term,
    }


def audit_cross_scope_tags(
    client: Any,
    storage_config: dict | None = None,
    *,
    scope_keys: list[str],
    min_scope_count: int = 2,
    limit: int = 5000,
    storage_adapter: Any = None,
    progress_interval: int = 1000,
    on_progress: Callable[[str], None] | None = None,
    should_cancel: Callable[[], bool] | None = None,
) -> dict:
    """Scan scope partitions and report tags present in multiple scopes."""
    cfg = storage_config or INDEX_STORAGE_CONFIG
    adapter = storage_adapter or get_storage_adapter(cfg, client)
    warn_threshold = int(TAG_REUSE_AUDIT_POLICY.get("warn_scope_count", 50))
    if len(scope_keys) > warn_threshold:
        logger.warning(
            "tag-reuse-audit scanning %s scopes (warn threshold %s)",
            len(scope_keys),
            warn_threshold,
        )

    local_cache = getattr(adapter, "_local_partitions", None)
    term_scopes: dict[str, set[str]] = defaultdict(set)
    lookup_keys_scanned = 0
    t0 = time.perf_counter()
    emit = on_progress if progress_interval > 0 else None
    scope_count = len(scope_keys)
    if emit:
        emit(
            "[tag-reuse-audit] starting "
            f"scopes={scope_count} min_scope_count={min_scope_count} limit={limit} "
            f"progress_interval={progress_interval}"
        )

    for scope_index, scope in enumerate(scope_keys, start=1):
        raise_if_cancelled(should_cancel)
        scope_keys_at_start = lookup_keys_scanned
        if emit:
            emit(
                "[tag-reuse-audit] scanning "
                f"scope {scope_index}/{scope_count} scope={scope}"
            )
        for term in iter_partition_terms(
            client,
            cfg,
            scope,
            local_cache=local_cache,
        ):
            raise_if_cancelled(should_cancel)
            lookup_keys_scanned += 1
            term_scopes[term].add(scope)
            if emit and lookup_keys_scanned % progress_interval == 0:
                emit(
                    "[tag-reuse-audit] "
                    f"lookup_keys_scanned={lookup_keys_scanned} "
                    f"unique_terms={len(term_scopes)} "
                    f"scope {scope_index}/{scope_count} scope={scope}"
                )
        if emit:
            scope_terms = lookup_keys_scanned - scope_keys_at_start
            emit(
                "[tag-reuse-audit] "
                f"scope {scope_index}/{scope_count} complete "
                f"terms={scope_terms} lookup_keys_scanned={lookup_keys_scanned} "
                f"unique_terms={len(term_scopes)}"
            )

    if emit:
        emit(
            "[tag-reuse-audit] aggregating duplicates "
            f"min_scope_count={min_scope_count} limit={limit}"
        )

    duplicates: list[dict] = []
    for normalized_term in sorted(term_scopes.keys()):
        scopes = sorted(term_scopes[normalized_term])
        if len(scopes) < min_scope_count:
            continue
        duplicates.append(
            {
                "normalized_term": normalized_term,
                "term": normalized_term,
                "scope_count": len(scopes),
                "scopes": scopes,
                "hit_count": len(scopes),
                "reference_count_by_scope": {s: 1 for s in scopes},
                "cross_scope_duplicate": True,
            }
        )

    duplicates.sort(key=lambda row: (-row["scope_count"], row["normalized_term"]))
    if limit > 0:
        duplicates = duplicates[:limit]

    duration_sec = round(time.perf_counter() - t0, 6)
    cross_scope_count = len(duplicates)
    unique_terms = len(term_scopes)

    if emit:
        emit(
            "[tag-reuse-audit] complete "
            f"lookup_keys_scanned={lookup_keys_scanned} "
            f"unique_terms={unique_terms} "
            f"cross_scope_duplicates={cross_scope_count} "
            f"duration_sec={duration_sec:.1f}"
        )

    return {
        "scopes_queried": scope_keys,
        "scopes_scanned": len(scope_keys),
        "lookup_keys_scanned": lookup_keys_scanned,
        "unique_terms_scanned": unique_terms,
        "min_scope_count": min_scope_count,
        "duration_sec": duration_sec,
        "reuse_metrics": {
            "scopes_queried": scope_keys,
            "terms_queried": [],
            "terms_with_hits": unique_terms,
            "cross_scope_duplicate_count": cross_scope_count,
            "cross_scope_duplicate_rate": (
                cross_scope_count / unique_terms if unique_terms else 0.0
            ),
            "by_term": duplicates,
        },
    }


def query_with_reuse_metrics(
    client: Any,
    terms: list[str],
    *,
    all_scopes: bool = False,
    match_scope_keys: list[str] | None = None,
    match_scope_key: str | None = None,
    source_types: list[str] | None = None,
    min_confidence: float = 0.0,
    limit: int = 5000,
    reuse_only: bool = False,
    storage_config: dict | None = None,
    storage_adapter: Any = None,
) -> dict:
    """Run a multi-scope term query and attach reuse summary."""
    cfg = storage_config or INDEX_STORAGE_CONFIG
    adapter = storage_adapter or get_storage_adapter(cfg, client)
    scopes = resolve_query_scope_keys(
        client,
        cfg,
        match_scope_key=match_scope_key,
        match_scope_keys=match_scope_keys,
        all_scopes=all_scopes,
        storage_adapter=adapter,
    )
    normalized_terms = normalize_query_terms(terms)
    hits = query_index_by_terms(
        client,
        terms,
        match_scope_keys=scopes,
        source_types=source_types,
        min_confidence=min_confidence,
        limit=limit,
        strict_scope=False,
        storage_config=cfg,
        storage_adapter=adapter,
    )
    reuse_metrics = summarize_tag_scope_reuse(
        hits,
        terms_queried=normalized_terms,
        scopes_queried=scopes,
        reuse_only=reuse_only,
    )
    return {
        "scopes_queried": scopes,
        "terms_queried": normalized_terms,
        "hits": hits,
        "reuse_metrics": reuse_metrics,
    }
