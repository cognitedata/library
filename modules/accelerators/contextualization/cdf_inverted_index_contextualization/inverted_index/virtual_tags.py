"""Virtual CogniteAsset tag creation from scoped inverted-index terms (UC4)."""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from collections.abc import Callable
from typing import Any

from inverted_index.cancellation import raise_if_cancelled
from inverted_index.config import INDEX_STORAGE_CONFIG, SCOPE_CONFIG, VIRTUAL_TAG_CREATION_CONFIG
from inverted_index.dm_query import cognite_asset_exists_for_term
from inverted_index.normalize import normalize_term
from inverted_index.query import query_index_by_terms, resolve_query_scope_keys
from inverted_index.raw_ops import (
    iter_partition_terms,
    load_postings_row,
    resolve_scope_partition_table,
)
from inverted_index.scope import parse_scope_key, slugify_scope_code
from inverted_index.storage import get_storage_adapter
from inverted_index.storage.raw_keys import build_raw_postings_row_key, flatten_postings_to_entries

logger = logging.getLogger(__name__)

_TERM_SELECTION_ALL = "all"
_TERM_SELECTION_MISSING = "missing_tags_only"


def effective_hierarchy_levels(
    virtual_tag_config: dict, scope_config: dict
) -> list[str]:
    levels = virtual_tag_config.get("hierarchy_levels") or []
    return list(levels or scope_config.get("levels") or [])


def effective_source_types(virtual_tag_config: dict) -> list[str]:
    return list(
        virtual_tag_config.get("source_types")
        or VIRTUAL_TAG_CREATION_CONFIG["source_types"]
    )


def effective_term_selection_mode(
    virtual_tag_config: dict, *, override: str | None = None
) -> str:
    mode = str(override or virtual_tag_config.get("term_selection_mode") or "").strip()
    if mode in (_TERM_SELECTION_ALL, _TERM_SELECTION_MISSING):
        return mode
    return _TERM_SELECTION_MISSING


def _min_confidence(virtual_tag_config: dict) -> float:
    return float(virtual_tag_config.get("min_confidence", 0.0))


def _filter_hits_by_source_and_confidence(
    hits: list[dict],
    *,
    source_types: list[str],
    min_confidence: float,
) -> list[dict]:
    allowed = set(source_types)
    out: list[dict] = []
    for hit in hits:
        if hit.get("source_type") not in allowed:
            continue
        conf = (hit.get("additional_metadata") or {}).get("confidence")
        if conf is not None and float(conf) < min_confidence:
            continue
        out.append(hit)
    return out


def _display_term(hits: list[dict], normalized_term: str) -> str:
    for hit in hits:
        if hit.get("term"):
            return str(hit["term"])
    return normalized_term


def create_asset_dict(
    external_id: str,
    name: str,
    *,
    space: str,
    description: str | None = None,
    parent_external_id: str | None = None,
    level: str | None = None,
    extra_properties: dict[str, Any] | None = None,
) -> dict[str, Any]:
    asset: dict[str, Any] = {
        "externalId": external_id,
        "space": space,
        "properties": {"name": name},
    }
    if description:
        asset["properties"]["description"] = description
    if parent_external_id:
        asset["properties"]["parent"] = {
            "space": space,
            "externalId": parent_external_id,
        }
    if level:
        asset["properties"]["tags"] = [level]
    for key, value in (extra_properties or {}).items():
        if value is not None and value != "":
            asset["properties"][key] = value
    return asset


def build_structural_assets(
    scope_values: dict[str, str],
    hierarchy_levels: list[str],
    virtual_tag_config: dict,
) -> tuple[list[dict[str, Any]], str | None]:
    """Build parent-chain structural CogniteAsset dicts; return assets and deepest external_id."""
    space = str(virtual_tag_config.get("instance_space") or "inst_virtual_tags")
    level_codes = {
        level: slugify_scope_code(scope_values.get(level, ""))
        for level in hierarchy_levels
    }
    assets: list[dict[str, Any]] = []
    descriptions: dict[str, str] = {}
    parent_external_id: str | None = None
    deepest_external_id: str | None = None

    for level_index, level in enumerate(hierarchy_levels):
        code = level_codes.get(level, "")
        if not code:
            continue
        display = str(scope_values.get(level, code))
        parts = [level]
        for prev in hierarchy_levels[: level_index + 1]:
            prev_code = level_codes.get(prev, "")
            if prev_code:
                parts.append(prev_code)
        external_id = "_".join(parts)
        if level_index == 0:
            description = display
        else:
            parent_desc = descriptions.get(parent_external_id or "", "")
            description = f"{parent_desc} > {display}" if parent_desc else display
        descriptions[external_id] = description
        assets.append(
            create_asset_dict(
                external_id,
                code,
                space=space,
                description=description,
                parent_external_id=parent_external_id,
                level=level,
            )
        )
        parent_external_id = external_id
        deepest_external_id = external_id

    return assets, deepest_external_id


def build_leaf_external_id(
    scope_values: dict[str, str],
    hierarchy_levels: list[str],
    sanitized_term: str,
    *,
    leaf_level: str,
) -> str:
    codes = [
        slugify_scope_code(scope_values[level])
        for level in hierarchy_levels
        if scope_values.get(level)
    ]
    scope_path = "_".join(codes) if codes else "global"
    return f"{leaf_level}_{scope_path}_{sanitized_term}"


def build_virtual_tag_asset(
    scope_values: dict[str, str],
    term: str,
    hits: list[dict],
    virtual_tag_config: dict,
    *,
    hierarchy_levels: list[str],
    parent_external_id: str,
) -> dict[str, Any]:
    space = str(virtual_tag_config.get("instance_space") or "inst_virtual_tags")
    leaf_level = str(virtual_tag_config.get("leaf_level") or "asset_tag")
    sanitized = slugify_scope_code(term)
    external_id = build_leaf_external_id(
        scope_values,
        hierarchy_levels,
        sanitized,
        leaf_level=leaf_level,
    )
    extra: dict[str, Any] = {}
    if virtual_tag_config.get("populate_aliases", True):
        extra["aliases"] = [term]
    mapping = virtual_tag_config.get("scope_property_mapping") or {}
    for level, prop in mapping.items():
        if level in scope_values and prop:
            extra[prop] = scope_values[level]

    source_files: set[str] = set()
    for hit in hits:
        ref = hit.get("reference_external_id")
        if hit.get("source_type") == "diagram_annotation_pattern" and ref:
            source_files.add(str(ref))
    if source_files:
        extra["sourceFile"] = ", ".join(sorted(source_files))

    return create_asset_dict(
        external_id,
        term,
        space=space,
        parent_external_id=parent_external_id,
        level=leaf_level,
        extra_properties=extra,
    )


def is_missing_tag_term(
    client: Any,
    match_scope_key: str,
    normalized_term: str,
    hits: list[dict],
    *,
    virtual_tag_config: dict,
    scope_config: dict,
) -> bool:
    """True when term is diagram-detected and not mappable to an existing CogniteAsset."""
    criteria = virtual_tag_config.get("missing_tag_criteria") or {}
    if criteria.get("require_pattern_detection", True):
        if not any(h.get("source_type") == "diagram_annotation_pattern" for h in hits):
            return False
    if criteria.get("exclude_with_cognite_asset_metadata", True):
        for hit in hits:
            if (
                hit.get("source_type") == "asset_metadata"
                and hit.get("reference_type") == "CogniteAsset"
            ):
                return False
    if criteria.get("check_existing_cognite_asset", True):
        scope_values = parse_scope_key(match_scope_key, scope_config) or {}
        display = _display_term(hits, normalized_term)
        if cognite_asset_exists_for_term(
            client,
            normalized_term=normalized_term,
            display_term=display,
            virtual_tag_config=virtual_tag_config,
            scope_values=scope_values,
        ):
            return False
    return True


def term_passes_selection_mode(
    client: Any,
    match_scope_key: str,
    normalized_term: str,
    hits: list[dict],
    *,
    virtual_tag_config: dict,
    scope_config: dict,
    term_selection_mode: str,
) -> bool:
    if term_selection_mode == _TERM_SELECTION_ALL:
        return bool(hits)
    return is_missing_tag_term(
        client,
        match_scope_key,
        normalized_term,
        hits,
        virtual_tag_config=virtual_tag_config,
        scope_config=scope_config,
    )


def _hits_for_term_raw(
    client: Any,
    storage_config: dict,
    match_scope_key: str,
    normalized_term: str,
    *,
    local_cache: dict[str, dict[str, dict]] | None = None,
    local_registry: dict[str, dict] | None = None,
) -> list[dict]:
    raw_db = storage_config.get("raw", {}).get("database")
    table = resolve_scope_partition_table(
        client,
        storage_config,
        match_scope_key,
        normalized_term=normalized_term,
        local_registry=local_registry,
    )
    row_key = build_raw_postings_row_key(match_scope_key, normalized_term)
    postings, _cols = load_postings_row(
        client,
        raw_db,
        table,
        row_key,
        local_cache=local_cache,
    )
    return flatten_postings_to_entries(
        postings,
        match_scope_key=match_scope_key,
        normalized_term=normalized_term,
    )


def load_term_hits(
    client: Any,
    match_scope_key: str,
    normalized_term: str,
    *,
    storage_config: dict | None = None,
    storage_adapter: Any = None,
    source_types: list[str] | None = None,
    min_confidence: float = 0.0,
) -> list[dict]:
    cfg = storage_config or INDEX_STORAGE_CONFIG
    adapter = storage_adapter or get_storage_adapter(cfg, client)
    if cfg.get("backend") == "raw":
        local_cache = getattr(adapter, "_local_partitions", None)
        local_registry = getattr(adapter, "_local_registry", None)
        hits = _hits_for_term_raw(
            client,
            cfg,
            match_scope_key,
            normalized_term,
            local_cache=local_cache,
            local_registry=local_registry,
        )
    else:
        hits = query_index_by_terms(
            client,
            [normalized_term],
            match_scope_key=match_scope_key,
            storage_config=cfg,
            storage_adapter=adapter,
            limit=5000,
        )
    return _filter_hits_by_source_and_confidence(
        hits,
        source_types=source_types or effective_source_types(VIRTUAL_TAG_CREATION_CONFIG),
        min_confidence=min_confidence,
    )


def _asset_apply_from_dict(asset: dict[str, Any], virtual_tag_config: dict) -> Any:
    from cognite.client.data_classes.data_modeling import DirectRelationReference
    from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteAssetApply

    props = dict(asset.get("properties") or {})
    parent_ref = None
    parent_data = props.pop("parent", None)
    if isinstance(parent_data, dict):
        parent_ref = DirectRelationReference(
            space=str(parent_data.get("space") or asset.get("space")),
            external_id=str(parent_data["externalId"]),
        )
    aliases = props.pop("aliases", None)
    source_file = props.pop("sourceFile", None) or props.pop("source_file", None)
    source_context = props.pop("sourceContext", None) or props.pop("source_context", None)
    source_id = props.pop("sourceId", None) or props.pop("source_id", None)
    tags = props.pop("tags", None)
    description = props.pop("description", None)

    kwargs: dict[str, Any] = {}
    if description is not None:
        kwargs["description"] = description
    if parent_ref is not None:
        kwargs["parent"] = parent_ref
    if tags is not None:
        kwargs["tags"] = tags if isinstance(tags, list) else [tags]
    if aliases is not None:
        kwargs["aliases"] = aliases if isinstance(aliases, list) else [aliases]
    if source_file is not None:
        kwargs["source_file"] = source_file
    if source_context is not None:
        kwargs["source_context"] = source_context
    if source_id is not None:
        kwargs["source_id"] = source_id

    view_space = str(virtual_tag_config.get("view_space") or "cdf_cdm")
    view_external_id = str(virtual_tag_config.get("view_external_id") or "CogniteAsset")
    view_version = str(virtual_tag_config.get("view_version") or "v1")

    return CogniteAssetApply(
        space=str(asset["space"]),
        external_id=str(asset["externalId"]),
        name=str(props.get("name") or asset["externalId"]),
        **kwargs,
    )


def apply_virtual_assets(
    client: Any,
    assets: list[dict[str, Any]],
    virtual_tag_config: dict,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    if not assets:
        return {"assets_applied": 0, "dry_run": dry_run}
    if dry_run or client is None:
        return {"assets_applied": len(assets), "dry_run": True}

    chunk_size = int(virtual_tag_config.get("apply_chunk_size", 500))
    applies = [_asset_apply_from_dict(a, virtual_tag_config) for a in assets]
    applied = 0
    for i in range(0, len(applies), chunk_size):
        chunk = applies[i : i + chunk_size]
        client.data_modeling.instances.apply(chunk)
        applied += len(chunk)
    return {"assets_applied": applied, "dry_run": False}


def upsert_virtual_tags_for_terms(
    client: Any,
    match_scope_key: str,
    terms_with_hits: dict[str, list[dict]],
    *,
    virtual_tag_config: dict,
    scope_config: dict,
    dry_run: bool = False,
) -> dict[str, Any]:
    scope_values = parse_scope_key(match_scope_key, scope_config)
    if not scope_values:
        return {
            "skipped": True,
            "reason": "unparseable_scope_key",
            "match_scope_key": match_scope_key,
        }

    hierarchy_levels = effective_hierarchy_levels(virtual_tag_config, scope_config)
    if not hierarchy_levels:
        return {
            "skipped": True,
            "reason": "no_hierarchy_levels",
            "match_scope_key": match_scope_key,
        }

    structural, parent_id = build_structural_assets(
        scope_values, hierarchy_levels, virtual_tag_config
    )
    if not parent_id:
        return {
            "skipped": True,
            "reason": "no_structural_parent",
            "match_scope_key": match_scope_key,
        }

    leaf_assets: list[dict[str, Any]] = []
    for normalized_term, hits in sorted(terms_with_hits.items()):
        term = _display_term(hits, normalized_term)
        leaf_assets.append(
            build_virtual_tag_asset(
                scope_values,
                term,
                hits,
                virtual_tag_config,
                hierarchy_levels=hierarchy_levels,
                parent_external_id=parent_id,
            )
        )

    all_assets = structural + leaf_assets
    apply_result = apply_virtual_assets(
        client, all_assets, virtual_tag_config, dry_run=dry_run
    )
    return {
        "match_scope_key": match_scope_key,
        "structural_assets": len(structural),
        "leaf_assets": len(leaf_assets),
        "terms": len(terms_with_hits),
        **apply_result,
    }


def collect_eligible_terms_for_scope(
    client: Any,
    match_scope_key: str,
    *,
    virtual_tag_config: dict,
    scope_config: dict,
    storage_config: dict | None = None,
    storage_adapter: Any = None,
    term_selection_mode: str | None = None,
    limit: int = 0,
    progress_interval: int = 1000,
    on_progress: Callable[[str], None] | None = None,
    should_cancel: Callable[[], bool] | None = None,
) -> dict[str, list[dict]]:
    cfg = storage_config or INDEX_STORAGE_CONFIG
    adapter = storage_adapter or get_storage_adapter(cfg, client)
    source_types = effective_source_types(virtual_tag_config)
    min_conf = _min_confidence(virtual_tag_config)
    mode = effective_term_selection_mode(
        virtual_tag_config, override=term_selection_mode
    )
    local_cache = getattr(adapter, "_local_partitions", None)
    local_registry = getattr(adapter, "_local_registry", None)

    eligible: dict[str, list[dict]] = {}
    scanned = 0
    for term in iter_partition_terms(
        client,
        cfg,
        match_scope_key,
        local_cache=local_cache,
        local_registry=local_registry,
    ):
        raise_if_cancelled(should_cancel)
        scanned += 1
        hits = load_term_hits(
            client,
            match_scope_key,
            term,
            storage_config=cfg,
            storage_adapter=adapter,
            source_types=source_types,
            min_confidence=min_conf,
        )
        if not hits:
            continue
        if not term_passes_selection_mode(
            client,
            match_scope_key,
            term,
            hits,
            virtual_tag_config=virtual_tag_config,
            scope_config=scope_config,
            term_selection_mode=mode,
        ):
            continue
        eligible[term] = hits
        if limit > 0 and len(eligible) >= limit:
            break
        if on_progress and progress_interval > 0 and scanned % progress_interval == 0:
            on_progress(
                f"[virtual-tags] scope={match_scope_key} "
                f"scanned={scanned} eligible={len(eligible)}"
            )
    return eligible


def process_virtual_tags_for_index_entries(
    client: Any,
    entries: list[dict],
    *,
    virtual_tag_config: dict | None = None,
    scope_config: dict | None = None,
    storage_config: dict | None = None,
    storage_adapter: Any = None,
    dry_run: bool = False,
    term_selection_mode: str | None = None,
) -> dict[str, Any]:
    vtc = virtual_tag_config or VIRTUAL_TAG_CREATION_CONFIG
    scope_cfg = scope_config or SCOPE_CONFIG
    if not vtc.get("enabled"):
        return {"skipped": True, "reason": "disabled"}

    source_types = set(effective_source_types(vtc))
    min_conf = _min_confidence(vtc)
    mode = effective_term_selection_mode(vtc, override=term_selection_mode)

    pairs: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for entry in entries:
        st = entry.get("source_type")
        if st not in source_types:
            continue
        scope = str(entry.get("match_scope_key") or "").strip()
        term = str(entry.get("normalized_term") or "").strip()
        if not scope or not term:
            continue
        conf = (entry.get("additional_metadata") or {}).get("confidence")
        if conf is not None and float(conf) < min_conf:
            continue
        pairs[(scope, term)].append(entry)

    if not pairs:
        return {"scopes_processed": 0, "terms_processed": 0, "leaf_assets": 0}

    cfg = storage_config or INDEX_STORAGE_CONFIG
    adapter = storage_adapter or get_storage_adapter(cfg, client)
    by_scope: dict[str, dict[str, list[dict]]] = defaultdict(dict)
    skipped_terms = 0

    for (scope, term), partial_hits in pairs.items():
        hits = load_term_hits(
            client,
            scope,
            term,
            storage_config=cfg,
            storage_adapter=adapter,
            source_types=list(source_types),
            min_confidence=min_conf,
        )
        if not hits:
            hits = partial_hits
        if not term_passes_selection_mode(
            client,
            scope,
            term,
            hits,
            virtual_tag_config=vtc,
            scope_config=scope_cfg,
            term_selection_mode=mode,
        ):
            skipped_terms += 1
            continue
        by_scope[scope][term] = hits

    scope_results: list[dict] = []
    total_leaves = 0
    for scope, terms_map in by_scope.items():
        result = upsert_virtual_tags_for_terms(
            client,
            scope,
            terms_map,
            virtual_tag_config=vtc,
            scope_config=scope_cfg,
            dry_run=dry_run,
        )
        scope_results.append(result)
        total_leaves += int(result.get("leaf_assets") or 0)

    return {
        "scopes_processed": len(by_scope),
        "terms_processed": sum(len(m) for m in by_scope.values()),
        "terms_skipped": skipped_terms,
        "leaf_assets": total_leaves,
        "scope_results": scope_results,
        "dry_run": dry_run,
    }


def run_virtual_tag_creation(
    client: Any,
    *,
    virtual_tag_config: dict | None = None,
    scope_config: dict | None = None,
    storage_config: dict | None = None,
    storage_adapter: Any = None,
    all_scopes: bool = False,
    match_scope_keys: list[str] | None = None,
    dry_run: bool = False,
    limit: int = 0,
    term_selection_mode: str | None = None,
    progress_interval: int = 1000,
    on_progress: Callable[[str], None] | None = None,
    should_cancel: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    vtc = virtual_tag_config or VIRTUAL_TAG_CREATION_CONFIG
    scope_cfg = scope_config or SCOPE_CONFIG
    if not vtc.get("enabled"):
        raise ValueError("virtual_tag_creation.enabled must be true to run UC4")
    if not scope_cfg.get("enabled"):
        raise ValueError("scope.enabled must be true for virtual tag hierarchy placement")
    if not effective_hierarchy_levels(vtc, scope_cfg):
        raise ValueError("scope.levels (or virtual_tag_creation.hierarchy_levels) required")

    cfg = storage_config or INDEX_STORAGE_CONFIG
    adapter = storage_adapter or get_storage_adapter(cfg, client)
    batch_limit = int(limit or vtc.get("batch_limit") or 0)

    scopes = resolve_query_scope_keys(
        client,
        cfg,
        match_scope_keys=match_scope_keys,
        all_scopes=all_scopes,
        storage_adapter=adapter,
    )

    t0 = time.perf_counter()
    emit = on_progress
    if emit:
        emit(
            f"[virtual-tags] starting scopes={len(scopes)} "
            f"mode={effective_term_selection_mode(vtc, override=term_selection_mode)} "
            f"dry_run={dry_run}"
        )

    scope_results: list[dict] = []
    total_leaves = 0
    total_terms = 0
    remaining = batch_limit if batch_limit > 0 else 0

    for scope_index, scope in enumerate(scopes, start=1):
        raise_if_cancelled(should_cancel)
        scope_limit = remaining if remaining > 0 else 0
        eligible = collect_eligible_terms_for_scope(
            client,
            scope,
            virtual_tag_config=vtc,
            scope_config=scope_cfg,
            storage_config=cfg,
            storage_adapter=adapter,
            term_selection_mode=term_selection_mode,
            limit=scope_limit,
            progress_interval=progress_interval,
            on_progress=on_progress,
            should_cancel=should_cancel,
        )
        if not eligible:
            if emit:
                emit(f"[virtual-tags] scope {scope_index}/{len(scopes)} {scope} eligible=0")
            continue
        result = upsert_virtual_tags_for_terms(
            client,
            scope,
            eligible,
            virtual_tag_config=vtc,
            scope_config=scope_cfg,
            dry_run=dry_run,
        )
        scope_results.append(result)
        total_leaves += int(result.get("leaf_assets") or 0)
        total_terms += len(eligible)
        if remaining > 0:
            remaining -= len(eligible)
            if remaining <= 0:
                break
        if emit:
            emit(
                f"[virtual-tags] scope {scope_index}/{len(scopes)} complete "
                f"scope={scope} terms={len(eligible)} leaf_assets={result.get('leaf_assets', 0)}"
            )

    duration_sec = round(time.perf_counter() - t0, 6)
    if emit:
        emit(
            f"[virtual-tags] complete scopes={len(scope_results)} "
            f"terms={total_terms} leaf_assets={total_leaves} duration_sec={duration_sec:.1f}"
        )

    return {
        "scopes_queried": scopes,
        "scopes_processed": len(scope_results),
        "terms_processed": total_terms,
        "leaf_assets": total_leaves,
        "duration_sec": duration_sec,
        "dry_run": dry_run,
        "term_selection_mode": effective_term_selection_mode(
            vtc, override=term_selection_mode
        ),
        "scope_results": scope_results,
    }
