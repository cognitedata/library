"""Target-driven contextualization."""

from __future__ import annotations

import logging
import sys
import time
from typing import Any, Callable, Literal

from inverted_index.cdm_relations import (
    hit_link_gate_reason,
    merge_direct_relation_list,
    merge_direct_relation_single,
    resolve_node_from_config,
)
from inverted_index.config import DIRECT_RELATION_CONFIG, SCOPE_CONFIG, SUBSCRIPTION_CONFIG
from inverted_index.dm_query import query_all_nodes
from inverted_index.edge_links import build_custom_edge_apply, upsert_diagram_annotation
from inverted_index.query import query_references_for_aliases
from inverted_index.scope import resolve_match_scope
from inverted_index.sources.annotations import _view_properties
from inverted_index.storage.raw_keys import posting_dedupe_key

from inverted_index.aliases import is_self_reference_hit, read_instance_aliases
from inverted_index.cancellation import raise_if_cancelled

logger = logging.getLogger(__name__)


def _default_batch_progress(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


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


def _query_aliases_across_scopes(
    client: Any,
    aliases: list[str],
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
        scope_hits = query_references_for_aliases(
            client,
            aliases,
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


def process_target_driven_contextualization(
    client: Any,
    instance_external_id: str,
    instance_type: Literal["asset", "file", "equipment", "timeseries"],
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
) -> dict:
    """
    Target-driven contextualization after external aliasing populates aliases.

  ``instance`` may be passed for local/demo runs; otherwise retrieved from CDF.
    """
    scope_cfg = scope_config or SCOPE_CONFIG
    dr_cfg = direct_relation_config or DIRECT_RELATION_CONFIG
    source_types = source_types_to_consider or [
        "diagram_annotation_pattern",
        "diagram_annotation_standard",
        "file_metadata",
        "asset_metadata",
    ]

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

    view_id = {
        "asset": "CogniteAsset",
        "file": "CogniteFile",
        "equipment": "CogniteEquipment",
        "timeseries": "CogniteTimeSeries",
    }.get(instance_type, "CogniteAsset")

    aliases = read_instance_aliases(target)
    if not aliases:
        return {
            "references_found": 0,
            "links_created": 0,
            "skipped": 1,
            "reason": "no_aliases",
        }

    match_scope_key, match_scope = resolve_match_scope(target, view_id, scope_cfg)
    if match_scope_keys and not scope_lookup_override:
        if not match_scope_key or match_scope_key not in match_scope_keys:
            return {
                "instance_external_id": instance_external_id,
                "instance_type": instance_type,
                "match_scope_key": match_scope_key,
                "match_scope": match_scope,
                "aliases": aliases,
                "references_found": 0,
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
            "links_created": 0,
            "skipped": 1,
            "reason": "scope_unresolved",
        }

    hits, query_filtered_by_confidence = _query_aliases_across_scopes(
        client,
        aliases,
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

    result = {
        "instance_external_id": instance_external_id,
        "instance_type": instance_type,
        "match_scope_key": match_scope_key,
        "match_scope": match_scope,
        "lookup_scope_keys": lookup_scope_keys,
        "aliases": aliases,
        "references_found": len(hits),
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
            instance_type,
            hits,
            direct_relation_config=dr_cfg,
            dry_run=dry_run,
        )
        result["direct_relations_updated"] = link_result.get("direct_relations_updated", 0)
        result["direct_relations_by_link"] = link_result.get("direct_relations_by_link", {})
        result["edges_created"] = link_result.get("edges_created", 0)
        result["edges_by_link"] = link_result.get("edges_by_link", {})
        result["annotations_created"] = link_result.get("annotations_created", 0)
        result["annotations_updated"] = link_result.get("annotations_updated", 0)
        result["annotations_skipped"] = link_result.get("annotations_skipped", 0)
        result["skipped_already_linked"] = link_result.get("already_linked", 0)
        result["links_filtered_by_confidence"] = link_result.get("filtered_by_confidence", 0)
        if link_result.get("errors"):
            result["errors"] = link_result["errors"]
        result["links_created"] = (
            int(link_result.get("direct_relations_updated") or 0)
            + int(link_result.get("edges_created") or 0)
            + int(link_result.get("annotations_created") or 0)
            + int(link_result.get("annotations_updated") or 0)
        )
        if dry_run:
            result["pending_applies"] = link_result.get("pending_applies")

    return result


def run_target_driven_for_instance_ids(
    client: Any,
    instance_external_ids: list[str],
    *,
    instance_type: Literal["asset", "file", "equipment", "timeseries"] = "asset",
    instance_space: str = "cdf_cdm",
    scope_config: dict | None = None,
    direct_relation_config: dict | None = None,
    min_confidence: float = 0.6,
    dry_run: bool = False,
    storage_adapter: Any = None,
    progress_interval: int = 100,
    on_progress: Callable[[str], None] | None = _default_batch_progress,
    match_scope_keys: list[str] | None = None,
    scope_lookup_override: bool = False,
    should_cancel: Callable[[], bool] | None = None,
) -> dict:
    """Run target-driven contextualization for an explicit list of instance external IDs."""
    scope_cfg = scope_config or SCOPE_CONFIG
    dr_cfg = direct_relation_config or DIRECT_RELATION_CONFIG

    processed = 0
    skipped = 0
    scope_filtered = 0
    references_found = 0
    links_created = 0
    query_filtered_by_confidence = 0
    links_filtered_by_confidence = 0
    skipped_already_linked = 0
    errors: list[dict] = []
    results: list[dict] = []
    started_at = time.monotonic()
    emit = on_progress if progress_interval > 0 else None
    if emit:
        scope_mode = "override" if scope_lookup_override else "filter"
        scope_keys_label = (
            ",".join(match_scope_keys) if match_scope_keys else "resolved-per-instance"
        )
        emit(
            "[target-driven-selected] Starting "
            f"count={len(instance_external_ids)} space={instance_space} "
            f"type={instance_type} scope_keys={scope_keys_label} "
            f"scope_mode={scope_mode} dry_run={dry_run} "
            f"progress_interval={progress_interval}"
        )

    for external_id in instance_external_ids:
        raise_if_cancelled(should_cancel)
        try:
            summary = process_target_driven_contextualization(
                client,
                instance_external_id=external_id,
                instance_type=instance_type,
                instance_space=instance_space,
                scope_config=scope_cfg,
                direct_relation_config=dr_cfg,
                min_confidence=min_confidence,
                dry_run=dry_run,
                storage_adapter=storage_adapter,
                match_scope_keys=match_scope_keys,
                scope_lookup_override=scope_lookup_override,
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
        references_found += int(summary.get("references_found") or 0)
        links_created += int(summary.get("links_created") or 0)
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
        "references_found": references_found,
        "links_created": links_created,
        "query_filtered_by_confidence": query_filtered_by_confidence,
        "links_filtered_by_confidence": links_filtered_by_confidence,
        "skipped_already_linked": skipped_already_linked,
        "errors": errors,
        "dry_run": dry_run,
        "instance_external_ids": instance_external_ids,
        "instance_space": instance_space,
        "instance_type": instance_type,
        "match_scope_keys": match_scope_keys,
        "scope_lookup_override": scope_lookup_override,
        "results": results if dry_run else None,
    }


def _resolve_batch_views(
    *,
    instance_type: Literal["asset", "file", "equipment", "timeseries"],
    subscription_config: dict,
    instance_views: list[str] | None = None,
    asset_views: list[str] | None = None,
) -> list[str]:
    if instance_views:
        return instance_views
    if asset_views:
        return asset_views
    if instance_type == "file":
        return subscription_config.get("file_views") or ["CogniteFile"]
    if instance_type == "equipment":
        return subscription_config.get("equipment_views") or ["CogniteEquipment"]
    if instance_type == "timeseries":
        return subscription_config.get("timeseries_views") or ["CogniteTimeSeries"]
    return subscription_config.get("asset_views") or ["CogniteAsset"]


def run_target_driven_for_all_assets(
    client: Any,
    *,
    instance_type: Literal["asset", "file", "equipment", "timeseries"] = "asset",
    instance_spaces: list[str] | None = None,
    instance_views: list[str] | None = None,
    asset_views: list[str] | None = None,
    subscription_config: dict | None = None,
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
) -> dict:
    """Run target-driven contextualization for every configured view instance."""
    from cognite.client import data_modeling as dm

    scope_cfg = scope_config or SCOPE_CONFIG
    dr_cfg = direct_relation_config or DIRECT_RELATION_CONFIG
    sub_cfg = subscription_config or SUBSCRIPTION_CONFIG
    views = _resolve_batch_views(
        instance_type=instance_type,
        subscription_config=sub_cfg,
        instance_views=instance_views,
        asset_views=asset_views,
    )
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
    references_found = 0
    links_created = 0
    query_filtered_by_confidence = 0
    links_filtered_by_confidence = 0
    skipped_already_linked = 0
    errors: list[dict] = []
    results: list[dict] = []
    started_at = time.monotonic()
    scope_label = ", ".join(str(s) for s in spaces if s) or "all"
    emit = on_progress if progress_interval > 0 else None
    if emit:
        scope_mode = "override" if scope_lookup_override else "filter"
        scope_keys_label = (
            ",".join(match_scope_keys) if match_scope_keys else "resolved-per-instance"
        )
        emit(
            "[target-driven-all] Starting "
            f"type={instance_type} views={','.join(views)} spaces={scope_label} "
            f"scope_keys={scope_keys_label} scope_mode={scope_mode} "
            f"dry_run={dry_run} progress_interval={progress_interval}"
        )

    for view in views:
        raise_if_cancelled(should_cancel)
        view_id = dm.ViewId(space=view_space, external_id=view, version=version)
        for space in spaces:
            raise_if_cancelled(should_cancel)
            for inst in query_all_nodes(
                client,
                view_id=view_id,
                property_names=["aliases", "name"],
                instance_space=space,
                page_size=batch_size,
                max_items=max_assets or 0,
            ):
                raise_if_cancelled(should_cancel)
                external_id = inst.external_id
                resolved_space = getattr(inst, "space", None) or space or view_space
                instance = {
                    "externalId": external_id,
                    "space": resolved_space,
                    "properties": _view_properties(
                        inst,
                        view_space=view_space,
                        view=view,
                        version=version,
                    ),
                }
                try:
                    summary = process_target_driven_contextualization(
                        client,
                        instance_external_id=external_id,
                        instance_type=instance_type,
                        instance_space=instance["space"],
                        scope_config=scope_cfg,
                        direct_relation_config=dr_cfg,
                        min_confidence=min_confidence,
                        dry_run=dry_run,
                        instance=instance,
                        storage_adapter=storage_adapter,
                        match_scope_keys=match_scope_keys,
                        scope_lookup_override=scope_lookup_override,
                    )
                except Exception as exc:
                    logger.exception(
                        "Target-driven failed for %s/%s", space, external_id
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
                            f"[target-driven-all] error {instance['space']}/{external_id}: {exc}"
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
                            f"[target-driven-all] error {instance['space']}/{external_id}: "
                            f"{summary['error']}"
                        )
                    continue
                references_found += int(summary.get("references_found") or 0)
                links_created += int(summary.get("links_created") or 0)
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
                        )
                    )
                if max_assets and processed >= max_assets:
                    break
            if max_assets and processed >= max_assets:
                break
        if max_assets and processed >= max_assets:
            break

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
            )
            + " — complete"
        )

    return {
        "processed": processed,
        "skipped": skipped,
        "scope_filtered": scope_filtered,
        "references_found": references_found,
        "links_created": links_created,
        "query_filtered_by_confidence": query_filtered_by_confidence,
        "links_filtered_by_confidence": links_filtered_by_confidence,
        "skipped_already_linked": skipped_already_linked,
        "errors": errors,
        "dry_run": dry_run,
        "instance_type": instance_type,
        "instance_views": views,
        "asset_views": views,
        "instance_spaces": [s for s in spaces if s is not None] or None,
        "match_scope_keys": match_scope_keys,
        "scope_lookup_override": scope_lookup_override,
        "results": results if dry_run else None,
    }


def apply_configured_links(
    client: Any,
    instance_external_id: str,
    instance_space: str,
    instance_type: Literal["asset", "file", "equipment", "timeseries"],
    index_hits: list[dict],
    direct_relation_config: dict | None = None,
    dry_run: bool = False,
) -> dict:
    """Dispatch direct_relation, edge, and diagram_annotation write modes per link."""
    dr_cfg = direct_relation_config or DIRECT_RELATION_CONFIG
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

    views = dr_cfg.get("views") or {}
    edge_views = dr_cfg.get("edge_views") or {}
    max_list_size = int(dr_cfg.get("max_list_size", 1000))

    pending_applies: list[dict] = []
    pending_edges: list[dict] = []
    pending_annotations: list[dict] = []
    direct_by_link: dict[str, int] = {}
    edges_by_link: dict[str, int] = {}
    ann_by_link: dict[str, int] = {}
    already_linked = 0
    skipped = 0
    filtered_by_confidence = 0
    errors: list[str] = []

    for link_key, link_cfg in (dr_cfg.get("links") or {}).items():
        if not link_cfg.get("enabled"):
            continue
        if instance_type not in link_cfg.get("instance_types", []):
            continue

        write_modes = link_cfg.get("write_modes") or ["direct_relation"]
        resolve_cfg = (link_cfg.get("resolve_by_instance_type") or {}).get(instance_type)
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
                    if ann_cfg.get("file_from_reference") and hit.get(
                        "reference_type"
                    ) == "CogniteFile":
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

    direct_updated = 0
    edges_created = 0
    annotations_created = 0
    annotations_updated = 0
    annotations_skipped = 0

    if dry_run:
        direct_updated = len(pending_applies)
        edges_created = len(pending_edges)
        annotations_created = len(pending_annotations)
    elif client is not None:
        for apply in pending_applies:
            try:
                changed = _apply_single_relation(client, apply)
                if changed:
                    direct_updated += 1
                else:
                    already_linked += 1
            except Exception as exc:
                errors.append(str(exc))

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
        "direct_relations_by_link": direct_by_link,
        "edges_created": edges_created,
        "edges_by_link": edges_by_link,
        "annotations_created": annotations_created,
        "annotations_updated": annotations_updated,
        "annotations_skipped": annotations_skipped,
        "annotations_by_link": ann_by_link,
        "already_linked": already_linked,
        "skipped": skipped,
        "filtered_by_confidence": filtered_by_confidence,
        "errors": errors,
        "pending_applies": pending_applies if dry_run else None,
        "pending_edges": pending_edges if dry_run else None,
        "pending_annotations": pending_annotations if dry_run else None,
    }


def apply_cdm_direct_relations(
    client: Any,
    instance_external_id: str,
    instance_space: str,
    instance_type: Literal["asset", "file", "equipment", "timeseries"],
    index_hits: list[dict],
    direct_relation_config: dict | None = None,
    dry_run: bool = False,
) -> dict:
    """Apply direct_relation write mode only (legacy entry point)."""
    result = apply_configured_links(
        client,
        instance_external_id,
        instance_space,
        instance_type,
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


def _apply_single_relation(client: Any, apply: dict) -> bool:
    """Read-merge-write a single forward direct relation. Returns True when changed."""
    space = apply["forward_space"]
    ext_id = apply["forward_external_id"]
    prop = apply["property"]
    nodes = client.data_modeling.instances.retrieve_nodes([(space, ext_id)])
    if not nodes:
        raise ValueError(f"Forward node not found: {space}/{ext_id}")
    current_props = dict(nodes[0].properties or {})
    existing = current_props.get(prop)
    if apply.get("cardinality") == "list":
        merged, changed = merge_direct_relation_list(
            existing or [],
            apply["target_space"],
            apply["target_external_id"],
            max_list_size=apply.get("max_list_size", 1000),
        )
        if not changed:
            return False
        new_val = merged
    else:
        new_val, status = merge_direct_relation_single(
            existing if isinstance(existing, dict) else None,
            apply["target_space"],
            apply["target_external_id"],
            overwrite=apply.get("overwrite_existing", False),
        )
        if status == "already_linked":
            return False
    from cognite.client.data_classes.data_modeling import NodeApply, NodeOrEdgeData

    view_space = apply.get("forward_view_space", space)
    view_external_id = apply.get("forward_view_external_id", "CogniteFile")
    view_version = apply.get("forward_view_version", "v1")
    client.data_modeling.instances.apply(
        [
            NodeApply(
                space=space,
                external_id=ext_id,
                sources=[
                    NodeOrEdgeData(
                        source=(view_space, view_external_id, view_version),
                        properties={prop: new_val},
                    )
                ],
            )
        ]
    )
    return True
