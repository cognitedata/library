"""Target-driven contextualization."""

from __future__ import annotations

import logging
import sys
import time
from datetime import datetime
from typing import Any, Callable

from cognite.client import data_modeling as dm

from inverted_index.aliases import is_self_reference_hit, read_instance_query_terms
from inverted_index.cancellation import raise_if_cancelled
from inverted_index.cdm_relations import (
    file_reference_types,
    hit_link_gate_reason,
    resolve_node_from_config,
    resolve_view_key,
    view_external_id,
)
from inverted_index.config import SCOPE_CONFIG, SUBSCRIPTION_CONFIG, TARGET_DRIVEN_CONFIG
from inverted_index.config_loader import load_direct_relation_preset
from inverted_index.dm_apply import apply_direct_relations_batched
from inverted_index.dm_query import query_all_nodes, top_level_property_names
from inverted_index.edge_links import build_custom_edge_apply, upsert_diagram_annotation
from inverted_index.query import query_index_by_terms
from inverted_index.scope import resolve_match_scope
from inverted_index.sources.annotations import _view_properties
from inverted_index.storage.raw_keys import posting_dedupe_key
from inverted_index.target_driven_dedupe import (
    record_target_driven_run,
    should_skip_target_driven,
)

logger = logging.getLogger(__name__)


def _default_direct_relation_config() -> dict:
    return load_direct_relation_preset()


def require_incoming_view_key(
    *,
    incoming_view_key: str | None = None,
    view_external_id_param: str | None = None,
    view_space: str | None = None,
    direct_relation_config: dict | None = None,
) -> str:
    if incoming_view_key and str(incoming_view_key).strip():
        return str(incoming_view_key).strip()
    dr_cfg = direct_relation_config or _default_direct_relation_config()
    views = dr_cfg.get("views") or {}
    if view_external_id_param:
        resolved = resolve_view_key(
            views,
            space=view_space,
            external_id=str(view_external_id_param),
        )
        if resolved:
            return resolved
    raise ValueError(
        "incoming_view_key or a resolvable view_external_id is required"
    )


def _default_batch_progress(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def resolve_query_property(
    query_property: str | None = None,
    target_driven_config: dict | None = None,
) -> str:
    if query_property and str(query_property).strip():
        return str(query_property).strip()
    cfg = target_driven_config or TARGET_DRIVEN_CONFIG
    return str(cfg.get("query_property") or "aliases")


def _query_property_fallbacks(target_driven_config: dict | None = None) -> tuple[str, ...]:
    cfg = target_driven_config or TARGET_DRIVEN_CONFIG
    raw = cfg.get("query_property_fallbacks") or []
    return tuple(str(item) for item in raw if str(item).strip())


def exclude_empty_aliases(target_driven_config: dict | None = None) -> bool:
    cfg = target_driven_config or TARGET_DRIVEN_CONFIG
    return bool(cfg.get("exclude_empty_aliases", False))


def effective_query_fallbacks(target_driven_config: dict | None = None) -> tuple[str, ...]:
    if exclude_empty_aliases(target_driven_config):
        return ()
    return _query_property_fallbacks(target_driven_config)


def batch_scan_top_level_properties(
    resolved_query_property: str,
    target_driven_config: dict | None = None,
) -> list[str]:
    td_cfg = target_driven_config or TARGET_DRIVEN_CONFIG
    top_level = _top_level_property(resolved_query_property)
    if exclude_empty_aliases(td_cfg):
        return top_level_property_names([top_level])
    paths = [top_level, *effective_query_fallbacks(td_cfg)]
    return top_level_property_names(paths) or [top_level]


def batch_exists_filter(
    view_id: dm.ViewId,
    resolved_query_property: str,
    target_driven_config: dict | None = None,
) -> Any:
    props = batch_scan_top_level_properties(resolved_query_property, target_driven_config)
    exists_filters = [dm.filters.Exists(view_id.as_property_ref(prop)) for prop in props]
    if len(exists_filters) == 1:
        return exists_filters[0]
    return dm.filters.Or(*exists_filters)


def _top_level_property(property_path: str) -> str:
    return str(property_path).split(".")[0].strip() or "aliases"


def _references_found_by_type(hits: list[dict]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for hit in hits:
        ref_type = str(hit.get("reference_type") or "unknown")
        counts[ref_type] = counts.get(ref_type, 0) + 1
    return counts


def _merge_references_found_by_type(
    total: dict[str, int],
    partial: dict[str, int] | None,
) -> dict[str, int]:
    merged = dict(total)
    for ref_type, count in (partial or {}).items():
        merged[ref_type] = merged.get(ref_type, 0) + int(count)
    return merged


def _format_batch_progress(
    *,
    processed: int,
    skipped: int,
    references_found: int,
    links_created: int,
    errors: int,
    elapsed_sec: float,
    query_filtered_by_confidence: int = 0,
    links_filtered_by_confidence: int = 0,
    skipped_already_linked: int = 0,
    label: str = "target-driven-all",
) -> str:
    return (
        f"[{label}] processed={processed} skipped={skipped} "
        f"references_found={references_found} links_created={links_created} "
        f"query_filtered_by_confidence={query_filtered_by_confidence} "
        f"links_filtered_by_confidence={links_filtered_by_confidence} "
        f"skipped_already_linked={skipped_already_linked} "
        f"errors={errors} elapsed={elapsed_sec:.1f}s"
    )


def _dedupe_index_hits(hits: list[dict]) -> list[dict]:
    merged: list[dict] = []
    seen: set[tuple] = set()
    for hit in hits:
        key = posting_dedupe_key(hit)
        if key in seen:
            continue
        seen.add(key)
        merged.append(hit)
    return merged


def _query_terms_across_scopes(
    client: Any,
    query_terms: list[str],
    lookup_scope_keys: list[str],
    *,
    source_types: list[str] | None,
    min_confidence: float,
    storage_adapter: Any,
) -> tuple[list[dict], int]:
    hits: list[dict] = []
    filtered_by_confidence = 0
    query_min_confidence = 0.0 if min_confidence > 0 else min_confidence
    for scope_key in lookup_scope_keys:
        scope_hits = query_index_by_terms(
            client,
            terms=query_terms,
            match_scope_key=scope_key,
            source_types=source_types,
            min_confidence=query_min_confidence,
            storage_adapter=storage_adapter,
        )
        if min_confidence > 0:
            for hit in scope_hits:
                conf = (hit.get("additional_metadata") or {}).get("confidence")
                if conf is not None and float(conf) < float(min_confidence):
                    filtered_by_confidence += 1
                else:
                    hits.append(hit)
        else:
            hits.extend(scope_hits)
    return _dedupe_index_hits(hits), filtered_by_confidence


def _flush_direct_relation_buffer(
    client: Any,
    buffer: list[dict],
    *,
    chunk_size: int = 500,
) -> dict:
    if not buffer:
        return {"direct_relations_updated": 0, "already_linked": 0, "errors": []}
    result = apply_direct_relations_batched(client, buffer, chunk_size=chunk_size)
    buffer.clear()
    return result


def _merge_link_result(result: dict, link_result: dict) -> None:
    result["direct_relations_updated"] = int(
        result.get("direct_relations_updated") or 0
    ) + int(link_result.get("direct_relations_updated") or 0)
    for link_key, count in (link_result.get("direct_relations_by_link") or {}).items():
        result["direct_relations_by_link"][link_key] = int(
            result["direct_relations_by_link"].get(link_key) or 0
        ) + int(count)
    result["edges_created"] = int(result.get("edges_created") or 0) + int(
        link_result.get("edges_created") or 0
    )
    for link_key, count in (link_result.get("edges_by_link") or {}).items():
        result["edges_by_link"][link_key] = int(
            result["edges_by_link"].get(link_key) or 0
        ) + int(count)
    result["annotations_created"] = int(
        result.get("annotations_created") or 0
    ) + int(link_result.get("annotations_created") or 0)
    result["annotations_updated"] = int(
        result.get("annotations_updated") or 0
    ) + int(link_result.get("annotations_updated") or 0)
    result["annotations_skipped"] = int(
        result.get("annotations_skipped") or 0
    ) + int(link_result.get("annotations_skipped") or 0)
    result["skipped_already_linked"] = int(
        result.get("skipped_already_linked") or 0
    ) + int(link_result.get("already_linked") or 0)
    result["links_filtered_by_confidence"] = int(
        result.get("links_filtered_by_confidence") or 0
    ) + int(link_result.get("filtered_by_confidence") or 0)
    if link_result.get("errors"):
        result.setdefault("errors", []).extend(link_result["errors"])
    result["links_created"] = (
        int(result.get("direct_relations_updated") or 0)
        + int(result.get("edges_created") or 0)
        + int(result.get("annotations_created") or 0)
        + int(result.get("annotations_updated") or 0)
    )


def process_target_driven_contextualization(
    client: Any,
    instance_external_id: str,
    *,
    incoming_view_key: str | None = None,
    view_external_id_param: str | None = None,
    view_space: str | None = None,
    instance_space: str = "cdf_cdm",
    scope_config: dict | None = None,
    source_types_to_consider: list[str] | None = None,
    min_confidence: float = 0.6,
    auto_create_links: bool = True,
    direct_relation_config: dict | None = None,
    dry_run: bool = False,
    instance: dict | None = None,
    storage_adapter: Any = None,
    match_scope_keys: list[str] | None = None,
    scope_lookup_override: bool = False,
    query_property: str | None = None,
    target_driven_config: dict | None = None,
    direct_relation_buffer: list[dict] | None = None,
) -> dict:
    """
    Target-driven contextualization using configurable instance query terms.

    ``instance`` may be passed for local/demo runs; otherwise retrieved from CDF.
    """
    scope_cfg = scope_config or SCOPE_CONFIG
    dr_cfg = direct_relation_config or _default_direct_relation_config()
    td_cfg = target_driven_config or TARGET_DRIVEN_CONFIG
    try:
        resolved_incoming_view_key = require_incoming_view_key(
            incoming_view_key=incoming_view_key,
            view_external_id_param=view_external_id_param,
            view_space=view_space or instance_space,
            direct_relation_config=dr_cfg,
        )
    except ValueError as exc:
        return {"error": str(exc)}
    views = dr_cfg.get("views") or {}
    view_id = view_external_id(views, resolved_incoming_view_key)
    resolved_query_property = resolve_query_property(query_property, td_cfg)
    fallbacks = effective_query_fallbacks(td_cfg)
    source_types = source_types_to_consider or list(
        dr_cfg.get("source_types")
        or [
            "diagram_annotation_pattern",
            "diagram_annotation_standard",
            "file_metadata",
            "asset_metadata",
        ]
    )

    target = instance
    if target is None and client is not None:
        try:
            nodes = client.data_modeling.instances.retrieve_nodes(
                [(instance_space, instance_external_id)]
            )
            if nodes:
                node = nodes[0]
                target = {
                    "externalId": node.external_id,
                    "space": instance_space,
                    "properties": dict(node.properties or {}),
                }
        except Exception as exc:
            return {"error": f"Failed to retrieve instance: {exc}"}

    if not target:
        return {"error": "Instance not found"}

    query_terms = read_instance_query_terms(
        target, resolved_query_property, fallbacks=fallbacks
    )
    if not query_terms:
        return {
            "references_found": 0,
            "references_found_by_type": {},
            "links_created": 0,
            "skipped": 1,
            "reason": "no_query_terms",
            "query_property": resolved_query_property,
            "query_terms": [],
        }

    match_scope_key, match_scope = resolve_match_scope(target, view_id, scope_cfg)
    if match_scope_keys and not scope_lookup_override:
        if not match_scope_key or match_scope_key not in match_scope_keys:
            return {
                "instance_external_id": instance_external_id,
                "incoming_view_key": resolved_incoming_view_key,
                "match_scope_key": match_scope_key,
                "match_scope": match_scope,
                "query_property": resolved_query_property,
                "query_terms": query_terms,
                "references_found": 0,
                "references_found_by_type": {},
                "links_created": 0,
                "skipped": 1,
                "reason": "scope_filtered",
                "match_scope_keys_filter": match_scope_keys,
            }
        lookup_scope_keys = [match_scope_key]
    elif match_scope_keys and scope_lookup_override:
        lookup_scope_keys = match_scope_keys
    elif match_scope_key:
        lookup_scope_keys = [match_scope_key]
    else:
        return {
            "references_found": 0,
            "references_found_by_type": {},
            "links_created": 0,
            "skipped": 1,
            "reason": "scope_unresolved",
            "query_property": resolved_query_property,
            "query_terms": query_terms,
        }

    hits, query_filtered_by_confidence = _query_terms_across_scopes(
        client,
        query_terms,
        lookup_scope_keys,
        source_types=source_types,
        min_confidence=min_confidence,
        storage_adapter=storage_adapter,
    )
    self_filtered = sum(
        1
        for hit in hits
        if is_self_reference_hit(hit, instance_external_id, instance_space)
    )
    hits = [
        hit
        for hit in hits
        if not is_self_reference_hit(hit, instance_external_id, instance_space)
    ]

    result: dict[str, Any] = {
        "instance_external_id": instance_external_id,
        "incoming_view_key": resolved_incoming_view_key,
        "match_scope_key": match_scope_key,
        "match_scope": match_scope,
        "lookup_scope_keys": lookup_scope_keys,
        "query_property": resolved_query_property,
        "query_terms": query_terms,
        "references_found": len(hits),
        "references_found_by_type": _references_found_by_type(hits),
        "self_reference_filtered": self_filtered,
        "query_filtered_by_confidence": query_filtered_by_confidence,
        "links_created": 0,
        "annotations_created": 0,
        "annotations_updated": 0,
        "annotations_skipped": 0,
        "edges_created": 0,
        "edges_by_link": {},
        "direct_relations_updated": 0,
        "direct_relations_by_link": {},
        "skipped_already_linked": 0,
        "links_filtered_by_confidence": 0,
        "dry_run": dry_run,
        "hits": hits if dry_run else None,
    }

    if auto_create_links and hits:
        link_result = apply_configured_links(
            client,
            instance_external_id,
            instance_space,
            resolved_incoming_view_key,
            hits,
            direct_relation_config=dr_cfg,
            dry_run=dry_run,
            direct_relation_buffer=direct_relation_buffer,
        )
        _merge_link_result(result, link_result)
        if dry_run:
            result["pending_applies"] = link_result.get("pending_applies")

    return result


def run_target_driven_for_instance_ids(
    client: Any,
    instance_external_ids: list[str],
    *,
    incoming_view_key: str | None = None,
    view_external_id_param: str | None = None,
    view_space: str | None = None,
    instance_space: str = "cdf_cdm",
    scope_config: dict | None = None,
    direct_relation_config: dict | None = None,
    target_driven_config: dict | None = None,
    min_confidence: float = 0.6,
    dry_run: bool = False,
    storage_adapter: Any = None,
    progress_interval: int = 100,
    on_progress: Callable[[str], None] | None = _default_batch_progress,
    match_scope_keys: list[str] | None = None,
    scope_lookup_override: bool = False,
    should_cancel: Callable[[], bool] | None = None,
    query_property: str | None = None,
    force: bool = False,
    write_buffer_size: int = 0,
) -> dict:
    """Run target-driven contextualization for an explicit list of instance external IDs."""
    scope_cfg = scope_config or SCOPE_CONFIG
    dr_cfg = direct_relation_config or _default_direct_relation_config()
    td_cfg = target_driven_config or TARGET_DRIVEN_CONFIG
    resolved_query_property = resolve_query_property(query_property, td_cfg)

    processed = 0
    skipped = 0
    scope_filtered = 0
    dedupe_skipped = 0
    references_found = 0
    references_found_by_type: dict[str, int] = {}
    links_created = 0
    query_filtered_by_confidence = 0
    links_filtered_by_confidence = 0
    skipped_already_linked = 0
    errors: list[dict] = []
    results: list[dict] = []
    started_at = time.monotonic()
    emit = on_progress if progress_interval > 0 else None
    direct_buffer: list[dict] | None = [] if write_buffer_size > 0 and not dry_run else None

    resolved_incoming_view_key = require_incoming_view_key(
        incoming_view_key=incoming_view_key,
        view_external_id_param=view_external_id_param,
        view_space=view_space or instance_space,
        direct_relation_config=dr_cfg,
    )
    views_registry = dr_cfg.get("views") or {}
    scope_view_id = view_external_id(views_registry, resolved_incoming_view_key)

    if emit:
        scope_mode = "override" if scope_lookup_override else "filter"
        scope_keys_label = (
            ",".join(match_scope_keys) if match_scope_keys else "resolved-per-instance"
        )
        emit(
            "[target-driven-selected] Starting "
            f"count={len(instance_external_ids)} space={instance_space} "
            f"incoming_view_key={resolved_incoming_view_key} "
            f"query_property={resolved_query_property} "
            f"scope_keys={scope_keys_label} scope_mode={scope_mode} dry_run={dry_run} "
            f"progress_interval={progress_interval}"
        )

    for external_id in instance_external_ids:
        raise_if_cancelled(should_cancel)
        try:
            preloaded_instance = None
            if client is not None and not dry_run:
                nodes = client.data_modeling.instances.retrieve_nodes(
                    [(instance_space, external_id)]
                )
                if nodes:
                    preloaded_instance = {
                        "externalId": nodes[0].external_id,
                        "space": instance_space,
                        "properties": dict(nodes[0].properties or {}),
                    }
                    query_terms = read_instance_query_terms(
                        preloaded_instance,
                        resolved_query_property,
                        fallbacks=effective_query_fallbacks(td_cfg),
                    )
                    scope_key, _ = resolve_match_scope(
                        preloaded_instance, scope_view_id, scope_cfg
                    )
                    if query_terms and should_skip_target_driven(
                        client,
                        instance_space,
                        external_id,
                        query_terms,
                        scope_key or "",
                        force=force,
                    ):
                        dedupe_skipped += 1
                        skipped += 1
                        continue

            summary = process_target_driven_contextualization(
                client,
                instance_external_id=external_id,
                incoming_view_key=resolved_incoming_view_key,
                instance_space=instance_space,
                scope_config=scope_cfg,
                direct_relation_config=dr_cfg,
                target_driven_config=td_cfg,
                min_confidence=min_confidence,
                dry_run=dry_run,
                instance=preloaded_instance,
                storage_adapter=storage_adapter,
                match_scope_keys=match_scope_keys,
                scope_lookup_override=scope_lookup_override,
                query_property=resolved_query_property,
                direct_relation_buffer=direct_buffer,
            )
        except Exception as exc:
            logger.exception(
                "Target-driven failed for %s/%s", instance_space, external_id
            )
            errors.append(
                {
                    "instance_external_id": external_id,
                    "instance_space": instance_space,
                    "error": str(exc),
                }
            )
            if emit:
                emit(
                    f"[target-driven-selected] error {instance_space}/{external_id}: {exc}"
                )
            continue

        if summary.get("skipped") and summary.get("reason") == "scope_filtered":
            scope_filtered += 1
            continue

        processed += 1
        if summary.get("skipped"):
            skipped += 1
        if summary.get("error"):
            errors.append(
                {
                    "instance_external_id": external_id,
                    "instance_space": instance_space,
                    "error": summary["error"],
                }
            )
            if emit:
                emit(
                    f"[target-driven-selected] error {instance_space}/{external_id}: "
                    f"{summary['error']}"
                )
            continue

        if (
            direct_buffer is not None
            and write_buffer_size > 0
            and len(direct_buffer) >= write_buffer_size
        ):
            flush_result = _flush_direct_relation_buffer(client, direct_buffer)
            summary["direct_relations_updated"] = int(
                summary.get("direct_relations_updated") or 0
            ) + int(flush_result.get("direct_relations_updated") or 0)
            summary["skipped_already_linked"] = int(
                summary.get("skipped_already_linked") or 0
            ) + int(flush_result.get("already_linked") or 0)

        if not dry_run and (summary.get("query_terms") or []) and not summary.get("skipped"):
            record_target_driven_run(
                client,
                instance_space,
                external_id,
                summary.get("query_terms") or [],
                summary.get("match_scope_key") or "",
                summary,
            )

        references_found += int(summary.get("references_found") or 0)
        references_found_by_type = _merge_references_found_by_type(
            references_found_by_type,
            summary.get("references_found_by_type"),
        )
        links_created += int(summary.get("links_created") or 0)
        for link_err in summary.get("errors") or []:
            errors.append(
                {
                    "instance_external_id": external_id,
                    "instance_space": instance_space,
                    "error": str(link_err),
                }
            )
        query_filtered_by_confidence += int(
            summary.get("query_filtered_by_confidence") or 0
        )
        links_filtered_by_confidence += int(
            summary.get("links_filtered_by_confidence") or 0
        )
        skipped_already_linked += int(summary.get("skipped_already_linked") or 0)
        if dry_run and (
            summary.get("references_found")
            or summary.get("skipped")
            or summary.get("reason")
        ):
            results.append(summary)
        if emit and processed % progress_interval == 0:
            emit(
                _format_batch_progress(
                    processed=processed,
                    skipped=skipped,
                    references_found=references_found,
                    links_created=links_created,
                    errors=len(errors),
                    elapsed_sec=time.monotonic() - started_at,
                    query_filtered_by_confidence=query_filtered_by_confidence,
                    links_filtered_by_confidence=links_filtered_by_confidence,
                    skipped_already_linked=skipped_already_linked,
                    label="target-driven-selected",
                )
            )

    if direct_buffer is not None and direct_buffer:
        flush_result = _flush_direct_relation_buffer(client, direct_buffer)
        links_created += int(flush_result.get("direct_relations_updated") or 0)
        skipped_already_linked += int(flush_result.get("already_linked") or 0)
        for link_err in flush_result.get("errors") or []:
            errors.append({"error": str(link_err)})

    if emit:
        emit(
            _format_batch_progress(
                processed=processed,
                skipped=skipped,
                references_found=references_found,
                links_created=links_created,
                errors=len(errors),
                elapsed_sec=time.monotonic() - started_at,
                query_filtered_by_confidence=query_filtered_by_confidence,
                links_filtered_by_confidence=links_filtered_by_confidence,
                skipped_already_linked=skipped_already_linked,
                label="target-driven-selected",
            )
            + " — complete"
        )

    return {
        "processed": processed,
        "skipped": skipped,
        "scope_filtered": scope_filtered,
        "dedupe_skipped": dedupe_skipped,
        "references_found": references_found,
        "references_found_by_type": references_found_by_type,
        "links_created": links_created,
        "query_filtered_by_confidence": query_filtered_by_confidence,
        "links_filtered_by_confidence": links_filtered_by_confidence,
        "skipped_already_linked": skipped_already_linked,
        "errors": errors,
        "dry_run": dry_run,
        "instance_external_ids": instance_external_ids,
        "instance_space": instance_space,
        "incoming_view_key": resolved_incoming_view_key,
        "query_property": resolved_query_property,
        "match_scope_keys": match_scope_keys,
        "scope_lookup_override": scope_lookup_override,
        "results": results if dry_run else None,
    }


def _resolve_backfill_view_keys(
    *,
    direct_relation_config: dict,
    subscription_config: dict,
    watch_view_keys: list[str] | None = None,
) -> list[str]:
    if watch_view_keys:
        return watch_view_keys
    sub_keys = subscription_config.get("watch_view_keys")
    if isinstance(sub_keys, list) and sub_keys:
        return [str(k) for k in sub_keys]
    return list((direct_relation_config.get("views") or {}).keys())


def _aggregate_backfill_summary(
    *,
    processed: int,
    skipped: int,
    scope_filtered: int,
    dedupe_skipped: int,
    references_found: int,
    references_found_by_type: dict[str, int],
    links_created: int,
    query_filtered_by_confidence: int,
    links_filtered_by_confidence: int,
    skipped_already_linked: int,
    errors: list[dict],
    dry_run: bool,
    watch_view_keys: list[str],
    view_keys_scanned: list[str],
    spaces: list[str | None],
    match_scope_keys: list[str] | None,
    scope_lookup_override: bool,
    query_property: str,
    results: list[dict] | None,
) -> dict:
    return {
        "processed": processed,
        "skipped": skipped,
        "scope_filtered": scope_filtered,
        "dedupe_skipped": dedupe_skipped,
        "references_found": references_found,
        "references_found_by_type": references_found_by_type,
        "links_created": links_created,
        "query_filtered_by_confidence": query_filtered_by_confidence,
        "links_filtered_by_confidence": links_filtered_by_confidence,
        "skipped_already_linked": skipped_already_linked,
        "errors": errors,
        "dry_run": dry_run,
        "watch_view_keys": watch_view_keys,
        "view_keys_scanned": view_keys_scanned,
        "instance_spaces": [s for s in spaces if s is not None] or None,
        "match_scope_keys": match_scope_keys,
        "scope_lookup_override": scope_lookup_override,
        "query_property": query_property,
        "results": results if dry_run else None,
    }


def run_target_driven_backfill(
    client: Any,
    *,
    watch_view_keys: list[str] | None = None,
    instance_spaces: list[str] | None = None,
    subscription_config: dict | None = None,
    target_driven_config: dict | None = None,
    view_space: str = "cdf_cdm",
    version: str = "v1",
    scope_config: dict | None = None,
    direct_relation_config: dict | None = None,
    min_confidence: float = 0.6,
    dry_run: bool = False,
    storage_adapter: Any = None,
    batch_size: int = 1000,
    max_assets: int | None = None,
    progress_interval: int = 100,
    on_progress: Callable[[str], None] | None = _default_batch_progress,
    match_scope_keys: list[str] | None = None,
    scope_lookup_override: bool = False,
    should_cancel: Callable[[], bool] | None = None,
    query_property: str | None = None,
    force: bool = False,
    write_buffer_size: int = 500,
    filter_updated_after: datetime | None = None,
) -> dict:
    """Fleet backfill: enumerate instances with query property set, then contextualize."""
    scope_cfg = scope_config or SCOPE_CONFIG
    dr_cfg = direct_relation_config or _default_direct_relation_config()
    sub_cfg = subscription_config or SUBSCRIPTION_CONFIG
    td_cfg = target_driven_config or TARGET_DRIVEN_CONFIG
    resolved_query_property = resolve_query_property(query_property, td_cfg)
    property_names = batch_scan_top_level_properties(resolved_query_property, td_cfg)

    view_keys = _resolve_backfill_view_keys(
        direct_relation_config=dr_cfg,
        subscription_config=sub_cfg,
        watch_view_keys=watch_view_keys,
    )
    views_registry = dr_cfg.get("views") or {}
    if instance_spaces:
        spaces: list[str | None] = list(instance_spaces)
    else:
        sub_spaces = sub_cfg.get("instance_spaces")
        if isinstance(sub_spaces, list) and sub_spaces:
            spaces = list(sub_spaces)
        else:
            spaces = [None]

    processed = 0
    skipped = 0
    scope_filtered = 0
    dedupe_skipped = 0
    references_found = 0
    references_found_by_type: dict[str, int] = {}
    links_created = 0
    query_filtered_by_confidence = 0
    links_filtered_by_confidence = 0
    skipped_already_linked = 0
    errors: list[dict] = []
    results: list[dict] = []
    started_at = time.monotonic()
    scope_label = ", ".join(str(s) for s in spaces if s) or "all"
    emit = on_progress if progress_interval > 0 else None
    direct_buffer: list[dict] | None = [] if write_buffer_size > 0 and not dry_run else None

    if emit:
        scope_mode = "override" if scope_lookup_override else "filter"
        scope_keys_label = (
            ",".join(match_scope_keys) if match_scope_keys else "resolved-per-instance"
        )
        emit(
            "[target-driven-backfill] Starting "
            f"view_keys={','.join(view_keys)} spaces={scope_label} "
            f"query_property={resolved_query_property} scope_keys={scope_keys_label} "
            f"scope_mode={scope_mode} dry_run={dry_run} "
            f"progress_interval={progress_interval}"
        )

    for view_key in view_keys:
        raise_if_cancelled(should_cancel)
        view_ref = views_registry.get(view_key) or {}
        view_ext = str(view_ref.get("external_id", view_key))
        view_sp = str(view_ref.get("space", view_space))
        view_ver = str(view_ref.get("version", version))
        view_id = dm.ViewId(space=view_sp, external_id=view_ext, version=view_ver)
        exists_filter = batch_exists_filter(view_id, resolved_query_property, td_cfg)
        for space in spaces:
            raise_if_cancelled(should_cancel)
            for inst in query_all_nodes(
                client,
                view_id=view_id,
                property_names=property_names,
                instance_space=space,
                user_filters=[exists_filter],
                filter_updated_after=filter_updated_after,
                page_size=batch_size,
                max_items=max_assets or 0,
            ):
                raise_if_cancelled(should_cancel)
                external_id = inst.external_id
                resolved_space = getattr(inst, "space", None) or space or view_sp
                instance = {
                    "externalId": external_id,
                    "space": resolved_space,
                    "properties": _view_properties(
                        inst,
                        view_space=view_sp,
                        view=view_ext,
                        version=view_ver,
                    ),
                }
                try:
                    query_terms = read_instance_query_terms(
                        instance,
                        resolved_query_property,
                        fallbacks=effective_query_fallbacks(td_cfg),
                    )
                    view_name = view
                    scope_key, _ = resolve_match_scope(instance, view_name, scope_cfg)
                    if (
                        not dry_run
                        and query_terms
                        and should_skip_target_driven(
                            client,
                            resolved_space,
                            external_id,
                            query_terms,
                            scope_key or "",
                            force=force,
                        )
                    ):
                        dedupe_skipped += 1
                        skipped += 1
                        continue

                    summary = process_target_driven_contextualization(
                        client,
                        instance_external_id=external_id,
                        incoming_view_key=view_key,
                        instance_space=instance["space"],
                        scope_config=scope_cfg,
                        direct_relation_config=dr_cfg,
                        target_driven_config=td_cfg,
                        min_confidence=min_confidence,
                        dry_run=dry_run,
                        instance=instance,
                        storage_adapter=storage_adapter,
                        match_scope_keys=match_scope_keys,
                        scope_lookup_override=scope_lookup_override,
                        query_property=resolved_query_property,
                        direct_relation_buffer=direct_buffer,
                    )
                except Exception as exc:
                    logger.exception(
                        "Target-driven backfill failed for %s/%s", space, external_id
                    )
                    errors.append(
                        {
                            "instance_external_id": external_id,
                            "instance_space": instance["space"],
                            "error": str(exc),
                        }
                    )
                    if emit:
                        emit(
                            f"[target-driven-backfill] error {instance['space']}/{external_id}: {exc}"
                        )
                    continue

                if summary.get("skipped") and summary.get("reason") == "scope_filtered":
                    scope_filtered += 1
                    continue

                processed += 1
                if summary.get("skipped"):
                    skipped += 1
                if summary.get("error"):
                    errors.append(
                        {
                            "instance_external_id": external_id,
                            "instance_space": instance["space"],
                            "error": summary["error"],
                        }
                    )
                    if emit:
                        emit(
                            f"[target-driven-backfill] error {instance['space']}/{external_id}: "
                            f"{summary['error']}"
                        )
                    continue

                if (
                    direct_buffer is not None
                    and write_buffer_size > 0
                    and len(direct_buffer) >= write_buffer_size
                ):
                    flush_result = _flush_direct_relation_buffer(client, direct_buffer)
                    links_created += int(flush_result.get("direct_relations_updated") or 0)
                    skipped_already_linked += int(
                        flush_result.get("already_linked") or 0
                    )

                query_terms = summary.get("query_terms") or []
                if not dry_run and query_terms and not summary.get("skipped"):
                    record_target_driven_run(
                        client,
                        instance["space"],
                        external_id,
                        query_terms,
                        summary.get("match_scope_key") or "",
                        summary,
                    )

                references_found += int(summary.get("references_found") or 0)
                references_found_by_type = _merge_references_found_by_type(
                    references_found_by_type,
                    summary.get("references_found_by_type"),
                )
                links_created += int(summary.get("links_created") or 0)
                for link_err in summary.get("errors") or []:
                    errors.append(
                        {
                            "instance_external_id": external_id,
                            "instance_space": instance["space"],
                            "error": str(link_err),
                        }
                    )
                query_filtered_by_confidence += int(
                    summary.get("query_filtered_by_confidence") or 0
                )
                links_filtered_by_confidence += int(
                    summary.get("links_filtered_by_confidence") or 0
                )
                skipped_already_linked += int(summary.get("skipped_already_linked") or 0)
                if dry_run and (
                    summary.get("references_found")
                    or summary.get("skipped")
                    or summary.get("reason")
                ):
                    results.append(summary)
                if emit and processed % progress_interval == 0:
                    emit(
                        _format_batch_progress(
                            processed=processed,
                            skipped=skipped,
                            references_found=references_found,
                            links_created=links_created,
                            errors=len(errors),
                            elapsed_sec=time.monotonic() - started_at,
                            query_filtered_by_confidence=query_filtered_by_confidence,
                            links_filtered_by_confidence=links_filtered_by_confidence,
                            skipped_already_linked=skipped_already_linked,
                            label="target-driven-backfill",
                        )
                    )
                if max_assets and processed >= max_assets:
                    break
            if max_assets and processed >= max_assets:
                break
        if max_assets and processed >= max_assets:
            break

    if direct_buffer is not None and direct_buffer:
        flush_result = _flush_direct_relation_buffer(client, direct_buffer)
        links_created += int(flush_result.get("direct_relations_updated") or 0)
        skipped_already_linked += int(flush_result.get("already_linked") or 0)
        for link_err in flush_result.get("errors") or []:
            errors.append({"error": str(link_err)})

    if emit:
        emit(
            _format_batch_progress(
                processed=processed,
                skipped=skipped,
                references_found=references_found,
                links_created=links_created,
                errors=len(errors),
                elapsed_sec=time.monotonic() - started_at,
                query_filtered_by_confidence=query_filtered_by_confidence,
                links_filtered_by_confidence=links_filtered_by_confidence,
                skipped_already_linked=skipped_already_linked,
                label="target-driven-backfill",
            )
            + " — complete"
        )

    return _aggregate_backfill_summary(
        processed=processed,
        skipped=skipped,
        scope_filtered=scope_filtered,
        dedupe_skipped=dedupe_skipped,
        references_found=references_found,
        references_found_by_type=references_found_by_type,
        links_created=links_created,
        query_filtered_by_confidence=query_filtered_by_confidence,
        links_filtered_by_confidence=links_filtered_by_confidence,
        skipped_already_linked=skipped_already_linked,
        errors=errors,
        dry_run=dry_run,
        watch_view_keys=view_keys,
        view_keys_scanned=view_keys,
        spaces=spaces,
        match_scope_keys=match_scope_keys,
        scope_lookup_override=scope_lookup_override,
        query_property=resolved_query_property,
        results=results if dry_run else None,
    )


def run_target_driven_for_all_assets(
    client: Any,
    **kwargs: Any,
) -> dict:
    """Deprecated alias for fleet backfill."""
    return run_target_driven_backfill(client, **kwargs)


def build_configured_link_pending(
    instance_external_id: str,
    instance_space: str,
    incoming_view_key: str,
    index_hits: list[dict],
    direct_relation_config: dict | None = None,
) -> dict:
    """Build pending link applies without writing."""
    dr_cfg = direct_relation_config or _default_direct_relation_config()
    views = dr_cfg.get("views") or {}
    edge_views = dr_cfg.get("edge_views") or {}
    max_list_size = int(dr_cfg.get("max_list_size", 1000))

    pending_applies: list[dict] = []
    pending_edges: list[dict] = []
    pending_annotations: list[dict] = []
    direct_by_link: dict[str, int] = {}
    edges_by_link: dict[str, int] = {}
    ann_by_link: dict[str, int] = {}
    skipped = 0
    filtered_by_confidence = 0

    for link_key, link_cfg in (dr_cfg.get("links") or {}).items():
        if not link_cfg.get("enabled"):
            continue
        if incoming_view_key not in link_cfg.get("incoming_views", []):
            continue

        write_modes = link_cfg.get("write_modes") or ["direct_relation"]
        resolve_cfg = (link_cfg.get("resolve_by_incoming_view") or {}).get(
            incoming_view_key
        )
        if not resolve_cfg:
            continue

        dr_count = 0
        edge_count = 0
        ann_count = 0
        forward_view_key = link_cfg.get("forward_view", "")
        forward_view = views.get(forward_view_key) or {}

        for hit in index_hits:
            gate_reason = hit_link_gate_reason(hit, link_cfg, dr_cfg)
            if gate_reason is not None:
                skipped += 1
                if gate_reason == "confidence":
                    filtered_by_confidence += 1
                continue

            forward_ref = resolve_node_from_config(
                hit,
                resolve_cfg.get("forward"),
                incoming_space=instance_space,
                incoming_external_id=instance_external_id,
            )
            target_ref = resolve_node_from_config(
                hit,
                resolve_cfg.get("target"),
                incoming_space=instance_space,
                incoming_external_id=instance_external_id,
            )
            if not forward_ref or not target_ref:
                skipped += 1
                continue

            fwd_space, fwd_ext = forward_ref
            tgt_space, tgt_ext = target_ref

            if "direct_relation" in write_modes:
                pending_applies.append(
                    {
                        "link": link_key,
                        "forward_space": fwd_space,
                        "forward_external_id": fwd_ext,
                        "forward_view_space": forward_view.get("space", fwd_space),
                        "forward_view_external_id": forward_view.get(
                            "external_id", forward_view_key
                        ),
                        "forward_view_version": forward_view.get("version", "v1"),
                        "property": link_cfg.get("property"),
                        "target_space": tgt_space,
                        "target_external_id": tgt_ext,
                        "cardinality": link_cfg.get("cardinality", "list"),
                        "overwrite_existing": link_cfg.get("overwrite_existing", False),
                        "max_list_size": max_list_size,
                    }
                )
                dr_count += 1

            if "edge" in write_modes:
                edge_cfg = link_cfg.get("edge") or {}
                edge_view_key = edge_cfg.get("edge_view")
                edge_view = edge_views.get(edge_view_key) if edge_view_key else None
                if edge_view:
                    allowed = edge_cfg.get("when_source_types") or link_cfg.get(
                        "source_types"
                    )
                    if not allowed or hit.get("source_type") in allowed:
                        pending_edges.append(
                            {
                                "link": link_key,
                                "edge_view": edge_view,
                                "start_space": fwd_space,
                                "start_external_id": fwd_ext,
                                "end_space": tgt_space,
                                "end_external_id": tgt_ext,
                                "edge_space": edge_cfg.get("edge_space"),
                                "external_id_template": edge_cfg.get(
                                    "external_id_template"
                                ),
                            }
                        )
                        edge_count += 1

            if "diagram_annotation" in write_modes:
                ann_cfg = link_cfg.get("diagram_annotation") or {}
                allowed = ann_cfg.get("when_source_types") or [
                    "diagram_annotation_pattern",
                    "diagram_annotation_standard",
                ]
                if hit.get("source_type") in allowed:
                    start_space = fwd_space
                    start_ext = fwd_ext
                    file_refs = file_reference_types(dr_cfg, link_cfg)
                    if ann_cfg.get("file_from_reference") and hit.get(
                        "reference_type"
                    ) in file_refs:
                        start_space = hit.get("reference_space") or fwd_space
                        start_ext = hit.get("reference_external_id") or fwd_ext
                    pending_annotations.append(
                        {
                            "link": link_key,
                            "hit": hit,
                            "start_space": start_space,
                            "start_external_id": start_ext,
                            "end_space": tgt_space,
                            "end_external_id": tgt_ext,
                            "diagram_annotation_cfg": ann_cfg,
                        }
                    )
                    ann_count += 1

        direct_by_link[link_key] = dr_count
        edges_by_link[link_key] = edge_count
        ann_by_link[link_key] = ann_count

    return {
        "pending_applies": pending_applies,
        "pending_edges": pending_edges,
        "pending_annotations": pending_annotations,
        "direct_relations_by_link": direct_by_link,
        "edges_by_link": edges_by_link,
        "annotations_by_link": ann_by_link,
        "skipped": skipped,
        "filtered_by_confidence": filtered_by_confidence,
    }


def apply_configured_links(
    client: Any,
    instance_external_id: str,
    instance_space: str,
    incoming_view_key: str,
    index_hits: list[dict],
    direct_relation_config: dict | None = None,
    dry_run: bool = False,
    direct_relation_buffer: list[dict] | None = None,
) -> dict:
    """Dispatch direct_relation, edge, and diagram_annotation write modes per link."""
    dr_cfg = direct_relation_config or _default_direct_relation_config()
    if not dr_cfg.get("enabled"):
        return {
            "direct_relations_updated": 0,
            "direct_relations_by_link": {},
            "edges_created": 0,
            "edges_by_link": {},
            "annotations_created": 0,
            "annotations_updated": 0,
            "annotations_skipped": 0,
        }

    built = build_configured_link_pending(
        instance_external_id,
        instance_space,
        incoming_view_key,
        index_hits,
        direct_relation_config=dr_cfg,
    )
    pending_applies = built["pending_applies"]
    pending_edges = built["pending_edges"]
    pending_annotations = built["pending_annotations"]

    direct_updated = 0
    edges_created = 0
    annotations_created = 0
    annotations_updated = 0
    annotations_skipped = 0
    already_linked = 0
    errors: list[str] = []

    if dry_run:
        direct_updated = len(pending_applies)
        edges_created = len(pending_edges)
        annotations_created = len(pending_annotations)
    elif client is not None:
        if direct_relation_buffer is not None:
            direct_relation_buffer.extend(pending_applies)
        else:
            batch_result = apply_direct_relations_batched(client, pending_applies)
            direct_updated = int(batch_result.get("direct_relations_updated") or 0)
            already_linked = int(batch_result.get("already_linked") or 0)
            errors.extend(batch_result.get("errors") or [])

        for edge in pending_edges:
            try:
                edge_apply = build_custom_edge_apply(
                    edge_view_cfg=edge["edge_view"],
                    start_space=edge["start_space"],
                    start_external_id=edge["start_external_id"],
                    end_space=edge["end_space"],
                    end_external_id=edge["end_external_id"],
                    edge_space=edge.get("edge_space"),
                    external_id_template=edge.get("external_id_template"),
                )
                client.data_modeling.instances.apply([edge_apply])
                edges_created += 1
            except Exception as exc:
                errors.append(str(exc))

        for ann in pending_annotations:
            try:
                outcome = upsert_diagram_annotation(
                    client,
                    ann["hit"],
                    start_space=ann["start_space"],
                    start_external_id=ann["start_external_id"],
                    end_space=ann["end_space"],
                    end_external_id=ann["end_external_id"],
                    diagram_annotation_cfg=ann["diagram_annotation_cfg"],
                    dr_cfg=dr_cfg,
                    dry_run=False,
                )
                if outcome == "created":
                    annotations_created += 1
                elif outcome == "updated":
                    annotations_updated += 1
                else:
                    annotations_skipped += 1
            except Exception as exc:
                errors.append(str(exc))

    return {
        "direct_relations_updated": direct_updated,
        "direct_relations_by_link": built["direct_relations_by_link"],
        "edges_created": edges_created,
        "edges_by_link": built["edges_by_link"],
        "annotations_created": annotations_created,
        "annotations_updated": annotations_updated,
        "annotations_skipped": annotations_skipped,
        "annotations_by_link": built["annotations_by_link"],
        "already_linked": already_linked,
        "skipped": built["skipped"],
        "filtered_by_confidence": built["filtered_by_confidence"],
        "errors": errors,
        "pending_applies": pending_applies if dry_run else None,
        "pending_edges": pending_edges if dry_run else None,
        "pending_annotations": pending_annotations if dry_run else None,
    }


def apply_cdm_direct_relations(
    client: Any,
    instance_external_id: str,
    instance_space: str,
    incoming_view_key: str,
    index_hits: list[dict],
    direct_relation_config: dict | None = None,
    dry_run: bool = False,
) -> dict:
    """Apply direct_relation write mode only (legacy entry point)."""
    result = apply_configured_links(
        client,
        instance_external_id,
        instance_space,
        incoming_view_key,
        index_hits,
        direct_relation_config=direct_relation_config,
        dry_run=dry_run,
    )
    return {
        "direct_relations_updated": result.get("direct_relations_updated", 0),
        "direct_relations_by_link": result.get("direct_relations_by_link", {}),
        "already_linked": result.get("already_linked", 0),
        "skipped": result.get("skipped", 0),
        "errors": result.get("errors", []),
        "pending_applies": result.get("pending_applies"),
    }
