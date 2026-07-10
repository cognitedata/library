"""Apply CogniteDiagramAnnotation edges from diagram_detect_to_dm cohort rows."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Tuple

from cognite.client.data_classes.data_modeling import (
    DirectRelationReference,
    EdgeApply,
    NodeOrEdgeData,
)
from cognite.client.data_classes.data_modeling.ids import ViewId

from cdf_fn_common.etl_cohort_storage import require_pipeline_run_key
from cdf_fn_common.etl_discovery_query_shared import (
    EXTERNAL_ID_COLUMN,
    INSTANCE_SPACE_COLUMN,
    NODE_INSTANCE_ID_COLUMN,
    _first_nonempty,
    resolve_task_config,
)
from cdf_fn_common.etl_save_apply import (
    DEFAULT_SAVE_BATCH_SIZE,
    _iter_entity_rows_for_save,
    _prepare_save_cfg,
    _resolve_dry_run,
    _resolve_save_batch_size,
    validate_save_config,
)
from cdf_fn_common.etl_task_runtime import merge_compiled_task_into_data
from cdf_fn_common.etl_ui_progress import emit_handler_progress


def _file_start_node_from_props(
    props: Mapping[str, Any],
    cols: Mapping[str, Any],
) -> Tuple[str, str]:
    start_space = _first_nonempty(
        props.get("start_node_space"),
        props.get("file_instance_space"),
        cols.get(INSTANCE_SPACE_COLUMN),
    )
    start_ext = _first_nonempty(
        props.get("start_node_external_id"),
        props.get("file_external_id"),
        cols.get(EXTERNAL_ID_COLUMN),
    )
    if not start_space:
        nid = str(cols.get(NODE_INSTANCE_ID_COLUMN) or "").strip()
        if ":" in nid:
            head, _, tail = nid.partition(":")
            if head.strip() and tail.strip():
                start_space = head.strip()
                if not start_ext:
                    start_ext = tail.strip()
    return start_space, start_ext


def _edge_properties_from_staging(props: Mapping[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    text = _first_nonempty(props.get("text"))
    if text:
        out["startNodeText"] = text
    page = props.get("page")
    if page is not None:
        try:
            out["startNodePageNumber"] = int(page)
        except (TypeError, ValueError):
            pass
    if props.get("confidence") is not None:
        out["confidence"] = props["confidence"]
    status = _first_nonempty(props.get("status"))
    if status:
        out["status"] = status
    for src, dest in (
        ("x_min", "startNodeXMin"),
        ("y_min", "startNodeYMin"),
        ("x_max", "startNodeXMax"),
        ("y_max", "startNodeYMax"),
    ):
        if props.get(src) is not None:
            out[dest] = props[src]
    return out


def edge_apply_from_staging_row(
    cols: Mapping[str, Any],
    props: Mapping[str, Any],
    *,
    view_id: ViewId,
    default_annotation_space: str,
) -> Optional[EdgeApply]:
    ann_space = _first_nonempty(
        props.get("annotation_space"),
        cols.get(INSTANCE_SPACE_COLUMN),
        default_annotation_space,
    )
    ann_ext = _first_nonempty(props.get("annotation_external_id"), cols.get(EXTERNAL_ID_COLUMN))
    start_space, start_ext = _file_start_node_from_props(props, cols)
    end_space = _first_nonempty(props.get("end_node_space"))
    end_ext = _first_nonempty(props.get("end_node_external_id"))
    if not all([ann_space, ann_ext, start_space, start_ext, end_space, end_ext]):
        return None

    properties = _edge_properties_from_staging(props)
    sources = [NodeOrEdgeData(view_id, properties)] if properties else []
    return EdgeApply(
        space=ann_space,
        external_id=ann_ext,
        type=DirectRelationReference(space=view_id.space, external_id=view_id.external_id),
        start_node=DirectRelationReference(space=start_space, external_id=start_ext),
        end_node=DirectRelationReference(space=end_space, external_id=end_ext),
        sources=sources,
    )


def etl_apply_diagram_annotation_save(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    cfg = _prepare_save_cfg(data)
    validate_save_config(cfg, save_kind="view")

    view_space = _first_nonempty(cfg.get("view_space"), "cdf_cdm")
    view_external_id = _first_nonempty(cfg.get("view_external_id"), "CogniteDiagramAnnotation")
    view_version = _first_nonempty(cfg.get("view_version"), "v1")
    default_annotation_space = _first_nonempty(
        cfg.get("annotation_space"),
        cfg.get("instance_space"),
        data.get("instance_space"),
        "discovery-annotations",
    )

    view_id = ViewId(space=view_space, external_id=view_external_id, version=view_version)
    run_id = require_pipeline_run_key(data)
    data["run_id"] = run_id
    task_id = _first_nonempty(data.get("task_id"), fn_external_id)
    dry_run = _resolve_dry_run(data, client, cfg)
    batch_size = _resolve_save_batch_size(cfg)

    all_rows = _iter_entity_rows_for_save(client, data, task_id)
    rows_read = len(all_rows)
    edges_applied = 0
    skipped = 0
    batch: List[EdgeApply] = []

    def flush_batch() -> None:
        nonlocal edges_applied, batch
        if not batch:
            return
        if not dry_run and client is not None:
            client.data_modeling.instances.apply(edges=batch)
        edges_applied += len(batch)
        batch.clear()

    for _pred_index, cols, props in all_rows:
        edge = edge_apply_from_staging_row(
            cols,
            props,
            view_id=view_id,
            default_annotation_space=default_annotation_space,
        )
        if edge is None:
            skipped += 1
            continue
        batch.append(edge)
        if len(batch) >= batch_size:
            flush_batch()
            emit_handler_progress(edges_applied, total=rows_read, label="diagram annotations")

    flush_batch()

    if log and hasattr(log, "info"):
        log.info(
            "%s diagram_annotation_save rows_read=%s edges_applied=%s skipped=%s dry_run=%s",
            fn_external_id,
            rows_read,
            edges_applied,
            skipped,
            dry_run,
        )

    return {
        "function_external_id": fn_external_id,
        "task_id": task_id,
        "status": "ok",
        "run_id": run_id,
        "rows_read": rows_read,
        "edges_applied": edges_applied,
        "instances_applied": edges_applied,
        "skipped": skipped,
        "dry_run": dry_run,
        "save_kind": "diagram_annotation",
        "view_space": view_space,
        "view_external_id": view_external_id,
        "view_version": view_version,
        "batch_size": batch_size or DEFAULT_SAVE_BATCH_SIZE,
    }
