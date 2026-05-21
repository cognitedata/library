"""DM view query: list instances and write cohort rows."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, MutableMapping, Optional

from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.ids import ViewId

from cdf_fn_common.cohort_storage import canvas_node_id_for_task, require_run_id
from cdf_fn_common.discovery_query_shared import (
    RECORD_KIND_COLUMN,
    RUN_ID_COLUMN,
    SCOPE_KEY_COLUMN,
    WORKFLOW_STATUS_UPDATED_AT_COLUMN,
    build_entity_cohort_row,
    resolve_query_sink,
    resolve_task_config,
    _flush_rows,
)
from cdf_fn_common.extraction_input_hash import compute_extraction_inputs_hash_from_entity_row
from cdf_fn_common.incremental_listing import (
    flush_key_discovery_processing_states,
    load_hash_by_node_for_scope,
    read_listing_watermark_ms,
    try_resolve_key_discovery_backend,
    write_listing_watermark_ms,
)
from cdf_fn_common.incremental_scope import (
    HIGH_WATERMARK_MS_COLUMN,
    RECORD_KIND_WATERMARK,
    list_all_instances,
    node_instance_id_str,
    node_last_updated_time_ms,
    scope_key_from_view_dict,
    scope_watermark_row_key,
)
from cdf_fn_common.raw_upload import RawRowsUploadQueue
from cdf_fn_common.source_view_filter_build import build_source_view_query_filter
from cdf_fn_common.task_runtime import merge_compiled_task_into_data

from .base import AbstractDiscoveryQueryHandler


def _incremental_enabled(data: Mapping[str, Any]) -> bool:
    if bool(data.get("run_all")):
        return False
    configuration = dict(data.get("configuration") or {}) if isinstance(data.get("configuration"), dict) else {}
    ke_params = dict(
        dict(dict(configuration.get("key_extraction") or {}).get("config") or {}).get("parameters") or {}
    )
    if bool(ke_params.get("incremental_change_processing")):
        return True
    # Canvas / IR task config (no root key_extraction on scope document).
    cfg = resolve_task_config(data)
    return bool(cfg.get("incremental_change_processing"))


def _key_extraction_parameters(configuration: Mapping[str, Any]) -> Dict[str, Any]:
    ke = configuration.get("key_extraction")
    if not isinstance(ke, dict):
        return {}
    kcfg = ke.get("config")
    if not isinstance(kcfg, dict):
        return {}
    params = kcfg.get("parameters")
    return dict(params) if isinstance(params, dict) else {}


def _source_view_index_for_view(
    configuration: Mapping[str, Any],
    *,
    view_space: str,
    view_external_id: str,
    view_version: str,
) -> Optional[int]:
    svs = configuration.get("source_views")
    if not isinstance(svs, list):
        return None
    for i, sv in enumerate(svs):
        if not isinstance(sv, dict):
            continue
        if (
            str(sv.get("view_space") or "").strip() == str(view_space or "").strip()
            and str(sv.get("view_external_id") or "").strip() == str(view_external_id or "").strip()
            and str(sv.get("view_version") or "").strip() == str(view_version or "").strip()
        ):
            return i
    return None


def _incremental_skip_unchanged_source_inputs(
    data: Mapping[str, Any],
    configuration: Mapping[str, Any],
    cfg: Mapping[str, Any],
    *,
    incremental: bool,
) -> bool:
    """Match workflow semantics: default True when incremental and not run_all."""
    if not incremental or bool(data.get("run_all")):
        return False
    if "incremental_skip_unchanged_source_inputs" in cfg:
        return bool(cfg.get("incremental_skip_unchanged_source_inputs"))
    params = _key_extraction_parameters(configuration)
    return bool(params.get("incremental_skip_unchanged_source_inputs", True))


def _extract_view_properties(instance: Any, view_id: ViewId) -> Dict[str, Any]:
    dumped = instance.dump() if hasattr(instance, "dump") else {}
    if not isinstance(dumped, dict):
        return {}
    props = (
        dumped.get("properties", {})
        .get(view_id.space, {})
        .get(f"{view_id.external_id}/{view_id.version}", {})
        or {}
    )
    return dict(props) if isinstance(props, dict) else {}


def _watermark_filter(high_ms: int) -> dm.filters.Filter:
    return dm.filters.Range(
        ("node", "lastUpdatedTime"),
        gt=int(high_ms),
    )


def _combine_filters(base: Any, extra: Any) -> Any:
    if extra is None:
        return base
    return dm.filters.And(base, extra)


def _write_watermark(
    client: Any,
    *,
    raw_db: str,
    raw_table: str,
    scope_key: str,
    high_ms: int,
    run_id: str,
) -> None:
    wm_key = scope_watermark_row_key(scope_key)
    cols = {
        RECORD_KIND_COLUMN: RECORD_KIND_WATERMARK,
        SCOPE_KEY_COLUMN: scope_key,
        HIGH_WATERMARK_MS_COLUMN: int(high_ms),
        RUN_ID_COLUMN: run_id,
        WORKFLOW_STATUS_UPDATED_AT_COLUMN: datetime.now(timezone.utc).isoformat(
            timespec="milliseconds"
        ),
    }
    client.raw.rows.insert(db_name=raw_db, table_name=raw_table, row={wm_key: cols})


class ViewQueryHandler(AbstractDiscoveryQueryHandler):
    @staticmethod
    def _entity_type_for_view(data: Mapping[str, Any], view_cfg: Mapping[str, Any]) -> str:
        et = ViewQueryHandler.first_nonempty(view_cfg.get("entity_type"))
        if et:
            return et
        ve = ViewQueryHandler.first_nonempty(view_cfg.get("view_external_id"))
        configuration = dict(data.get("configuration") or {}) if isinstance(data.get("configuration"), dict) else {}
        for sv in configuration.get("source_views") or []:
            if not isinstance(sv, dict):
                continue
            if ViewQueryHandler.first_nonempty(sv.get("view_external_id")) == ve:
                return ViewQueryHandler.first_nonempty(sv.get("entity_type"), "asset")
        return "asset"

    @staticmethod
    def _pick_properties(
        props: Mapping[str, Any],
        include_properties: Optional[List[Any]],
    ) -> Dict[str, Any]:
        if not include_properties:
            return dict(props)
        out: Dict[str, Any] = {}
        for name in include_properties:
            key = str(name).strip()
            if key and key in props:
                out[key] = props[key]
        return out

    @staticmethod
    def _inject_instance_space_on_properties(props: Dict[str, Any], inst: Any) -> Dict[str, Any]:
        """Always set ``instance_space`` from the DM node (not from ``include_properties``)."""
        out = dict(props)
        space = getattr(inst, "space", None)
        if space is not None and str(space).strip():
            out["instance_space"] = str(space).strip()
        return out

    @classmethod
    def run(
        cls,
        fn_external_id: str,
        data: MutableMapping[str, Any],
        client: Any,
        log: Any,
    ) -> Dict[str, Any]:
        merge_compiled_task_into_data(data)
        cfg = resolve_task_config(data)
        view_space = cls.first_nonempty(cfg.get("view_space"), "cdf_cdm")
        view_external_id = cls.first_nonempty(cfg.get("view_external_id"))
        view_version = cls.first_nonempty(cfg.get("view_version"), "v1")
        if not view_external_id:
            raise ValueError("config.view_external_id is required for fn_dm_view_query")

        instance_space = cls.first_nonempty(cfg.get("instance_space"), data.get("instance_space"))
        include_properties = cfg.get("include_properties") or []
        if not isinstance(include_properties, list):
            include_properties = []
        batch_size = int(cfg.get("batch_size") or cfg.get("limit") or 1000)
        limit_per_page = min(1000, batch_size) if batch_size > 0 else 1000

        view_id = ViewId(space=view_space, external_id=view_external_id, version=view_version)
        scope_view = {
            "view_space": view_space,
            "view_external_id": view_external_id,
            "view_version": view_version,
            "instance_space": instance_space or None,
            "filters": cfg.get("filters") or [],
        }
        scope_key = scope_key_from_view_dict(scope_view)
        entity_type = cls._entity_type_for_view(data, scope_view)
        run_id = require_run_id(data)
        data["run_id"] = run_id
        task_id = cls.first_nonempty(data.get("task_id"), fn_external_id)
        canvas_node_id = canvas_node_id_for_task(data, task_id)
        raw_db, raw_table = resolve_query_sink(data)
        configuration = (
            dict(data.get("configuration") or {}) if isinstance(data.get("configuration"), dict) else {}
        )
        ke_params = _key_extraction_parameters(configuration)

        base_filter = build_source_view_query_filter(view_id, scope_view.get("filters") or [])
        incremental = _incremental_enabled(data)
        hash_skip = _incremental_skip_unchanged_source_inputs(
            data, configuration, cfg, incremental=incremental
        )
        kd_backend = (
            try_resolve_key_discovery_backend(client, ke_params, log=log)
            if incremental and client is not None
            else None
        )
        wm_before: Optional[int] = None
        if incremental:
            try:
                wm_before = read_listing_watermark_ms(
                    client,
                    backend=kd_backend,
                    raw_db=raw_db,
                    raw_table=raw_table,
                    scope_key=scope_key,
                )
            except Exception:
                wm_before = None
            if wm_before is not None:
                base_filter = _combine_filters(base_filter, _watermark_filter(wm_before))

        latest_by_node: Dict[str, str] = {}
        if hash_skip and client is not None:
            try:
                latest_by_node = load_hash_by_node_for_scope(
                    client,
                    backend=kd_backend,
                    raw_db=raw_db,
                    raw_table=raw_table,
                    scope_key=scope_key,
                    hash_index_cache=data.get("discovery_raw_hash_index_cache"),
                )
            except Exception:
                latest_by_node = {}

        source_view_index = _source_view_index_for_view(
            configuration,
            view_space=view_space,
            view_external_id=view_external_id,
            view_version=view_version,
        )
        workflow_scope_opt = str(ke_params.get("workflow_scope") or "").strip() or None
        source_view_fp_opt = str(ke_params.get("source_view_fingerprint") or "").strip() or None
        scope_view_for_hash = {**scope_view, "include_properties": include_properties}

        if log and hasattr(log, "info"):
            log.info(
                "%s listing view=%s/%s/%s space=%s incremental=%s hash_skip=%s "
                "state_backend=%s prior_hash_nodes=%s watermark_before=%s",
                fn_external_id,
                view_space,
                view_external_id,
                view_version,
                instance_space or "(any)",
                incremental,
                hash_skip,
                "key_discovery_fdm" if kd_backend is not None else "raw",
                len(latest_by_node),
                wm_before,
            )

        # Empty / omitted instance space means any DM space. ``instances.list(space=...)`` must receive
        # ``None`` to search across spaces — never a non-existent space name. Legacy configs may still
        # use the removed sentinel ``all_spaces``; treat it like empty.
        _ins = str(instance_space or "").strip()
        if not _ins or _ins.lower() == "all_spaces":
            list_space_arg = None
        else:
            list_space_arg = _ins

        queue = RawRowsUploadQueue(client)
        pending: List[Dict[str, Any]] = []
        kd_processing_pending: List[Dict[str, Any]] = []
        n_written = 0
        n_listed = 0
        n_skipped_hash = 0
        max_last_updated: Optional[int] = wm_before
        raw_write_error: Optional[str] = None

        try:
            for inst in list_all_instances(
                client,
                instance_type="node",
                space=list_space_arg,
                sources=[view_id],
                filter=base_filter,
                limit_per_page=limit_per_page,
                logger=log,
                progress_context=f"task={task_id}",
            ):
                ext_id = cls.first_nonempty(getattr(inst, "external_id", None))
                if not ext_id:
                    continue
                nid = node_instance_id_str(inst)
                if not nid:
                    continue
                n_listed += 1
                props = cls._inject_instance_space_on_properties(
                    cls._pick_properties(_extract_view_properties(inst, view_id), include_properties),
                    inst,
                )
                lu = node_last_updated_time_ms(inst)
                if lu is not None:
                    max_last_updated = lu if max_last_updated is None else max(max_last_updated, lu)

                inputs_hash: Optional[str] = None
                if hash_skip:
                    try:
                        entity_metadata = {
                            "view_space": view_space,
                            "view_external_id": view_external_id,
                            "view_version": view_version,
                            "source_view_index": source_view_index,
                            **props,
                        }
                        inputs_hash = compute_extraction_inputs_hash_from_entity_row(
                            entity_metadata,
                            scope_view_for_hash,
                            workflow_scope=workflow_scope_opt,
                            source_view_fingerprint=source_view_fp_opt,
                            logger=log,
                        )
                    except Exception as hash_ex:
                        if log and hasattr(log, "warning"):
                            log.warning(
                                "%s extraction input hash failed for node=%s (cohort row still written): %s",
                                fn_external_id,
                                nid,
                                hash_ex,
                            )
                        inputs_hash = None

                    if inputs_hash and latest_by_node.get(nid) == inputs_hash:
                        n_skipped_hash += 1
                        continue

                pending.append(
                    build_entity_cohort_row(
                        run_id=run_id,
                        scope_key=scope_key,
                        canvas_node_id=canvas_node_id,
                        query_source="view",
                        node_instance_id=nid,
                        external_id=ext_id,
                        entity_type=entity_type,
                        view_space=view_space,
                        view_external_id=view_external_id,
                        view_version=view_version,
                        properties=props,
                        last_updated_ms=lu,
                        extraction_inputs_hash=(
                            None
                            if kd_backend is not None
                            else (inputs_hash if hash_skip and inputs_hash else None)
                        ),
                    )
                )
                n_written += 1
                if (
                    kd_backend is not None
                    and hash_skip
                    and inputs_hash
                    and workflow_scope_opt
                ):
                    kd_processing_pending.append(
                        {
                            "workflow_scope": workflow_scope_opt,
                            "source_view_fingerprint": source_view_fp_opt or "",
                            "record_instance_key": nid,
                            "record_external_id": ext_id,
                            "last_seen_hash": inputs_hash,
                            "last_watermark_value_ms": lu,
                        }
                    )
                if len(pending) >= 500:
                    _flush_rows(queue, raw_db, raw_table, pending, client=client)
                if len(kd_processing_pending) >= 500:
                    flush_key_discovery_processing_states(
                        client, kd_backend, kd_processing_pending, log=log
                    )
                    kd_processing_pending.clear()

            _flush_rows(queue, raw_db, raw_table, pending, client=client)
            flush_key_discovery_processing_states(
                client, kd_backend, kd_processing_pending, log=log
            )

            if incremental and max_last_updated is not None and (
                wm_before is None or max_last_updated > wm_before
            ):
                if kd_backend is not None:
                    write_listing_watermark_ms(
                        client,
                        backend=kd_backend,
                        high_ms=max_last_updated,
                        log=log,
                    )
                else:
                    _write_watermark(
                        client,
                        raw_db=raw_db,
                        raw_table=raw_table,
                        scope_key=scope_key,
                        high_ms=max_last_updated,
                        run_id=run_id,
                    )
        except Exception as ex:
            raw_write_error = f"{type(ex).__name__}: {ex!s}"
            if log and hasattr(log, "warning"):
                log.warning(
                    "%s cohort RAW write failed (instances_read=%s): %s",
                    fn_external_id,
                    n_written,
                    raw_write_error,
                )

        summary = {
            "function_external_id": fn_external_id,
            "task_id": task_id,
            "query_source": "view",
            "instances_written": n_written,
            "instances_listed": n_listed,
            "instances_skipped_unchanged_hash": n_skipped_hash,
            "incremental_skip_unchanged_source_inputs": hash_skip,
            "run_id": run_id,
            "scope_key": scope_key,
            "raw_db": raw_db,
            "raw_table": raw_table,
            "view": f"{view_space}/{view_external_id}/{view_version}",
            "incremental": incremental,
            "watermark_before_ms": wm_before,
            "watermark_after_ms": max_last_updated if incremental else None,
        }
        if raw_write_error:
            summary["raw_write_error"] = raw_write_error
        data["run_id"] = run_id
        return summary
