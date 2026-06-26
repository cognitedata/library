"""CLI command implementations against live CDF (RAW index backend)."""

from __future__ import annotations

import json
from typing import Any, Callable

from inverted_index.build import build_diagram_annotation_index, build_metadata_index
from inverted_index.config_loader import build_runtime_config
from inverted_index.normalize import normalize_query_terms
from inverted_index.query import (
    list_index_entries_by_file,
    query_index_by_terms,
    resolve_query_scope_keys,
)
from inverted_index.scoring import (
    calculate_contextualization_score,
    get_pattern_not_in_standard_delta,
    get_standard_not_in_pattern_delta,
)
from inverted_index.storage import get_storage_adapter
from inverted_index.storage.raw_adapter import validate_raw_scope_config
from inverted_index.tag_reuse import audit_cross_scope_tags, query_with_reuse_metrics
from inverted_index.target_driven import (
    process_target_driven_contextualization,
    run_target_driven_for_all_assets,
    run_target_driven_for_instance_ids,
)
from inverted_index.cancellation import raise_if_cancelled
from local_runner.client import auth_mode_from_env, create_cognite_client


def _query_summary(result: Any) -> str:
    if isinstance(result, list):
        return f"hits={len(result)}"
    if not isinstance(result, dict):
        return "done"
    hits = result.get("hits") or []
    reuse = result.get("reuse_metrics") or {}
    return (
        f"hits={len(hits)} "
        f"scopes={len(result.get('scopes_queried') or [])} "
        f"cross_scope_duplicates={reuse.get('cross_scope_duplicate_count', 0)}"
    )


def _scope_keys_label(scopes: list[str], *, max_names: int = 5) -> str:
    if not scopes:
        return "none"
    if len(scopes) <= max_names:
        return ",".join(scopes)
    head = ",".join(scopes[:max_names])
    return f"{head},… (+{len(scopes) - max_names} more)"


def _tag_reuse_audit_summary(result: Any) -> str:
    if not isinstance(result, dict):
        return "done"
    reuse = result.get("reuse_metrics") or {}
    return (
        f"scopes_scanned={result.get('scopes_scanned', 0)} "
        f"lookup_keys_scanned={result.get('lookup_keys_scanned', 0)} "
        f"unique_terms={result.get('unique_terms_scanned', 0)} "
        f"cross_scope_duplicates={reuse.get('cross_scope_duplicate_count', 0)} "
        f"duration_sec={result.get('duration_sec', 0)}"
    )


def _runtime() -> dict[str, Any]:
    cfg = build_runtime_config()
    validate_raw_scope_config(cfg["scope_config"])
    return cfg


def cmd_build_metadata(
    *,
    dry_run: bool = False,
    on_log: Callable[[str], None] | None = None,
    should_cancel: Callable[[], bool] | None = None,
) -> dict:
    cfg = _runtime()
    client = None if dry_run else create_cognite_client()
    adapter = (
        get_storage_adapter(cfg["storage_config"], client)
        if not dry_run
        else None
    )
    return build_metadata_index(
        client,
        index_field_config=cfg["index_field_config"],
        scope_config=cfg["scope_config"],
        storage_config=cfg["storage_config"],
        instance_spaces=cfg.get("instance_spaces"),
        dry_run=dry_run,
        storage_adapter=adapter,
        on_progress=on_log,
        should_cancel=should_cancel,
    )


def cmd_build_annotations(
    *,
    file_external_id: str | None = None,
    detection_mode: str = "all",
    dry_run: bool = False,
    on_log: Callable[[str], None] | None = None,
    should_cancel: Callable[[], bool] | None = None,
) -> dict:
    cfg = _runtime()
    client = None if dry_run else create_cognite_client()
    storage = cfg["storage_config"]
    adapter = get_storage_adapter(storage, client) if not dry_run else None

    from inverted_index.sources.annotations import list_diagram_annotations

    annotations = None
    if client is not None:
        if on_log:
            on_log(
                "[build-annotations] fetching annotations "
                f"file_external_id={file_external_id or 'all'}"
            )
        annotations = list_diagram_annotations(
            client,
            file_external_id=file_external_id,
            instance_spaces=cfg.get("instance_spaces"),
            detection_mode=detection_mode,  # type: ignore[arg-type]
        )
    result = build_diagram_annotation_index(
        client,
        detection_mode=detection_mode,  # type: ignore[arg-type]
        scope_config=cfg["scope_config"],
        storage_config=storage,
        annotations=annotations,
        instance_spaces=cfg.get("instance_spaces"),
        dry_run=dry_run,
        storage_adapter=adapter,
        should_cancel=should_cancel,
        on_progress=on_log,
    )
    return result


def parse_scope_key_args(scope_keys: list[str] | None) -> list[str]:
    """Flatten repeatable --scope-key values (comma-separated allowed)."""
    if not scope_keys:
        return []
    out: list[str] = []
    for raw in scope_keys:
        for part in str(raw).split(","):
            key = part.strip()
            if key:
                out.append(key)
    return out


def parse_instance_id_args(
    instance_external_id: str | None,
    instance_external_ids: list[str] | None,
) -> list[str]:
    """Merge single and repeatable instance id args (comma-separated allowed)."""
    seen: set[str] = set()
    out: list[str] = []
    for raw in ([instance_external_id] if instance_external_id else []) + (
        instance_external_ids or []
    ):
        for part in str(raw).split(","):
            key = part.strip()
            if key and key not in seen:
                seen.add(key)
                out.append(key)
    return out


def cmd_query(
    terms: list[str],
    *,
    all_scopes: bool = False,
    match_scope_keys: list[str] | None = None,
    source_types: list[str] | None = None,
    min_confidence: float = 0.0,
    reuse_only: bool = False,
    hits_only: bool = False,
    on_log: Callable[[str], None] | None = None,
    should_cancel: Callable[[], bool] | None = None,
) -> Any:
    if on_log:
        on_log(
            "[query] starting "
            f"terms={','.join(terms)} all_scopes={all_scopes} "
            f"min_confidence={min_confidence} reuse_only={reuse_only} hits_only={hits_only}"
        )
    raise_if_cancelled(should_cancel)
    cfg = _runtime()
    client = create_cognite_client()
    adapter = get_storage_adapter(cfg["storage_config"], client)
    scopes = parse_scope_key_args(match_scope_keys)

    if all_scopes:
        result = query_with_reuse_metrics(
            client,
            terms,
            all_scopes=True,
            source_types=source_types,
            min_confidence=min_confidence,
            reuse_only=reuse_only,
            storage_config=cfg["storage_config"],
            storage_adapter=adapter,
        )
    else:
        resolved = resolve_query_scope_keys(
            client,
            cfg["storage_config"],
            match_scope_keys=scopes,
            storage_adapter=adapter,
        )
        if len(resolved) == 1:
            normalized = normalize_query_terms(terms)
            hits = query_index_by_terms(
                client,
                terms,
                match_scope_key=resolved[0],
                source_types=source_types,
                min_confidence=min_confidence,
                strict_scope=False,
                storage_config=cfg["storage_config"],
                storage_adapter=adapter,
            )
            from inverted_index.tag_reuse import summarize_tag_scope_reuse

            reuse_metrics = summarize_tag_scope_reuse(
                hits,
                terms_queried=normalized,
                scopes_queried=resolved,
                reuse_only=reuse_only,
            )
            result = {
                "scopes_queried": resolved,
                "terms_queried": normalized,
                "hits": hits,
                "reuse_metrics": reuse_metrics,
            }
        else:
            result = query_with_reuse_metrics(
                client,
                terms,
                match_scope_keys=scopes,
                source_types=source_types,
                min_confidence=min_confidence,
                reuse_only=reuse_only,
                storage_config=cfg["storage_config"],
                storage_adapter=adapter,
            )

    raise_if_cancelled(should_cancel)
    if on_log:
        on_log(f"[query] complete {_query_summary(result)}")

    if hits_only:
        return result["hits"]
    return result


def cmd_tag_reuse_audit(
    *,
    all_scopes: bool = False,
    match_scope_keys: list[str] | None = None,
    min_scope_count: int = 2,
    limit: int = 5000,
    on_log: Callable[[str], None] | None = None,
    should_cancel: Callable[[], bool] | None = None,
) -> dict:
    explicit_keys = parse_scope_key_args(match_scope_keys)
    if on_log:
        on_log(
            "[tag-reuse-audit] starting "
            f"all_scopes={all_scopes} min_scope_count={min_scope_count} limit={limit} "
            f"explicit_keys={explicit_keys or 'none'}"
        )
    raise_if_cancelled(should_cancel)
    cfg = _runtime()
    client = create_cognite_client()
    adapter = get_storage_adapter(cfg["storage_config"], client)
    scopes = explicit_keys
    if on_log:
        on_log(
            "[tag-reuse-audit] resolving scopes "
            f"all_scopes={all_scopes} explicit_keys={scopes or 'none'}"
        )
    if all_scopes:
        scopes = resolve_query_scope_keys(
            client,
            cfg["storage_config"],
            all_scopes=True,
            storage_adapter=adapter,
        )
    elif not scopes:
        raise ValueError("tag-reuse-audit requires --all-scopes or at least one --scope-key")
    else:
        scopes = resolve_query_scope_keys(
            client,
            cfg["storage_config"],
            match_scope_keys=scopes,
            storage_adapter=adapter,
        )
    if on_log:
        on_log(
            "[tag-reuse-audit] resolved "
            f"scope_count={len(scopes)} scopes={_scope_keys_label(scopes)}"
        )
    result = audit_cross_scope_tags(
        client,
        cfg["storage_config"],
        scope_keys=scopes,
        min_scope_count=min_scope_count,
        limit=limit,
        storage_adapter=adapter,
        on_progress=on_log,
        should_cancel=should_cancel,
    )
    raise_if_cancelled(should_cancel)
    if on_log:
        on_log(f"[tag-reuse-audit] finished {_tag_reuse_audit_summary(result)}")
    return result


def cmd_target_driven(
    *,
    instance_external_id: str | None = None,
    instance_external_ids: list[str] | None = None,
    instance_type: str = "asset",
    instance_space: str = "cdf_cdm",
    dry_run: bool = False,
    min_confidence: float = 0.6,
    match_scope_keys: list[str] | None = None,
    scope_lookup_override: bool = False,
    max_assets: int | None = None,
    progress_interval: int = 100,
    on_log: Callable[[str], None] | None = None,
    should_cancel: Callable[[], bool] | None = None,
) -> dict:
    cfg = _runtime()
    client = create_cognite_client()
    adapter = get_storage_adapter(cfg["storage_config"], client)
    scopes = match_scope_keys
    if scopes:
        scopes = resolve_query_scope_keys(
            client,
            cfg["storage_config"],
            match_scope_keys=scopes,
            storage_adapter=adapter,
        )

    instance_ids = parse_instance_id_args(instance_external_id, instance_external_ids)
    if instance_ids:
        if len(instance_ids) == 1:
            only_id = instance_ids[0]
            if on_log:
                on_log(
                    f"[target-driven] asset {only_id} "
                    f"space={instance_space} dry_run={dry_run}"
                )
            result = process_target_driven_contextualization(
                client,
                instance_external_id=only_id,
                instance_type=instance_type,  # type: ignore[arg-type]
                instance_space=instance_space,
                scope_config=cfg["scope_config"],
                direct_relation_config=cfg["direct_relation_config"],
                min_confidence=min_confidence,
                dry_run=dry_run,
                storage_adapter=adapter,
                match_scope_keys=scopes,
                scope_lookup_override=bool(scopes),
            )
            if on_log:
                on_log(
                    f"[target-driven] complete references_found={result.get('references_found', 0)}"
                )
            return result

        return run_target_driven_for_instance_ids(
            client,
            instance_ids,
            instance_type=instance_type,  # type: ignore[arg-type]
            instance_space=instance_space,
            scope_config=cfg["scope_config"],
            direct_relation_config=cfg["direct_relation_config"],
            min_confidence=min_confidence,
            dry_run=dry_run,
            storage_adapter=adapter,
            progress_interval=progress_interval,
            on_progress=on_log,
            match_scope_keys=scopes,
            scope_lookup_override=bool(scopes) or scope_lookup_override,
            should_cancel=should_cancel,
        )

    return run_target_driven_for_all_assets(
        client,
        instance_type=instance_type,  # type: ignore[arg-type]
        instance_spaces=cfg.get("instance_spaces"),
        subscription_config=cfg.get("subscription_config"),
        scope_config=cfg["scope_config"],
        direct_relation_config=cfg["direct_relation_config"],
        min_confidence=min_confidence,
        dry_run=dry_run,
        storage_adapter=adapter,
        max_assets=max_assets,
        progress_interval=progress_interval,
        match_scope_keys=scopes,
        scope_lookup_override=scope_lookup_override,
        on_progress=on_log,
        should_cancel=should_cancel,
    )


def cmd_migrate(
    *,
    dry_run: bool = False,
    purge: bool = True,
    match_scope_keys: list[str] | None = None,
) -> dict:
    from inverted_index.migrate import migrate_index

    cfg = _runtime()
    client = None if dry_run else create_cognite_client()
    adapter = get_storage_adapter(cfg["storage_config"], client) if client else None
    scopes = parse_scope_key_args(match_scope_keys) if match_scope_keys else None
    if scopes and client is not None:
        scopes = resolve_query_scope_keys(
            client,
            cfg["storage_config"],
            match_scope_keys=scopes,
            storage_adapter=adapter,
        )
    return migrate_index(
        client,
        storage_config=cfg["storage_config"],
        scope_config=cfg["scope_config"],
        index_field_config=cfg["index_field_config"],
        instance_spaces=cfg.get("instance_spaces"),
        match_scope_keys=scopes,
        purge=purge,
        dry_run=dry_run,
        storage_adapter=adapter,
    )


def cmd_score(
    file_external_id: str,
    match_scope_key: str | None = None,
    file_space: str = "cdf_cdm",
    *,
    on_log: Callable[[str], None] | None = None,
    should_cancel: Callable[[], bool] | None = None,
) -> dict:
    if on_log:
        on_log(
            f"[score] starting file={file_external_id} space={file_space} "
            f"scope={match_scope_key or 'default'}"
        )
    raise_if_cancelled(should_cancel)
    cfg = _runtime()
    client = create_cognite_client()
    adapter = get_storage_adapter(cfg["storage_config"], client)
    result = calculate_contextualization_score(
        client,
        file_external_id=file_external_id,
        file_space=file_space,
        match_scope_key=match_scope_key,
        storage_adapter=adapter,
    )
    if on_log:
        on_log(f"[score] complete overall_score={result.get('overall_score')}")
    return result


def cmd_list_by_file(
    file_external_id: str,
    *,
    match_scope_key: str | None = None,
    file_space: str = "cdf_cdm",
    source_types: list[str] | None = None,
    limit: int = 5000,
    on_log: Callable[[str], None] | None = None,
    should_cancel: Callable[[], bool] | None = None,
) -> list[dict]:
    if on_log:
        on_log(
            f"[list-by-file] starting file={file_external_id} space={file_space} "
            f"scope={match_scope_key or 'all'} limit={limit}"
        )
    raise_if_cancelled(should_cancel)
    cfg = _runtime()
    client = create_cognite_client()
    adapter = get_storage_adapter(cfg["storage_config"], client)
    entries = list_index_entries_by_file(
        client,
        file_external_id,
        file_space=file_space,
        match_scope_key=match_scope_key,
        source_types=source_types,
        storage_config=cfg["storage_config"],
        storage_adapter=adapter,
        limit=limit,
    )
    if on_log:
        on_log(f"[list-by-file] complete entries={len(entries)}")
    return entries


def cmd_deltas(
    file_external_id: str,
    *,
    match_scope_key: str | None = None,
    file_space: str = "cdf_cdm",
    on_log: Callable[[str], None] | None = None,
    should_cancel: Callable[[], bool] | None = None,
) -> dict:
    if on_log:
        on_log(
            f"[deltas] starting file={file_external_id} space={file_space} "
            f"scope={match_scope_key or 'default'}"
        )
    raise_if_cancelled(should_cancel)
    cfg = _runtime()
    client = create_cognite_client()
    adapter = get_storage_adapter(cfg["storage_config"], client)
    missing_tags = get_pattern_not_in_standard_delta(
        client,
        file_external_id,
        file_space=file_space,
        match_scope_key=match_scope_key,
        storage_adapter=adapter,
    )
    raise_if_cancelled(should_cancel)
    pattern_feedback = get_standard_not_in_pattern_delta(
        client,
        file_external_id,
        file_space=file_space,
        storage_adapter=adapter,
    )
    result = {
        "missing_tags": missing_tags,
        "pattern_feedback": pattern_feedback,
    }
    if on_log:
        on_log(
            "[deltas] complete "
            f"missing_tags={len(missing_tags)} "
            f"pattern_feedback={len(pattern_feedback)}"
        )
    return result


def print_json(data: Any) -> None:
    print(json.dumps(data, indent=2, default=str))


def print_auth_banner() -> None:
    from local_runner.client import create_cognite_client

    client = create_cognite_client()
    print(f"CDF project: {client.config.project} (auth: {auth_mode_from_env()})")
