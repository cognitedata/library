"""Deployed CDF handler for diagram annotation jsonMapping stages."""

from __future__ import annotations

from typing import Any, Dict, MutableMapping

from cdf_fn_common.etl_annotation_map.kuiper_templates import (
    prepare_local_json_mapping_input,
    resolve_cohort_source_task_id_from_json_mapping_config,
    run_json_mapping_kuiper,
)
from cdf_fn_common.etl_cohort_storage import require_pipeline_run_key
from cdf_fn_common.etl_discovery_query_shared import resolve_task_config
from cdf_fn_common.etl_json_mapping_sink import (
    materialize_json_mapping_output_to_cohort,
    should_materialize_cohort_after_json_mapping,
)
from cdf_fn_common.etl_task_runtime import merge_compiled_task_into_data


def etl_handle_json_mapping(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    if client is None:
        raise ValueError("json_mapping requires a CDF client")

    merge_compiled_task_into_data(data)
    cfg = resolve_task_config(data)
    task_id = str(data.get("task_id") or fn_external_id)
    run_id = require_pipeline_run_key(data)
    data["run_id"] = run_id

    payload = data.get("payload") if isinstance(data.get("payload"), dict) else {}
    fallback_source = str(
        data.get("source_task_id") or payload.get("source_task_id") or ""
    ).strip()
    source_tid = resolve_cohort_source_task_id_from_json_mapping_config(
        cfg,
        fallback=fallback_source,
    )
    if source_tid:
        data["source_task_id"] = source_tid

    raw_input = cfg.get("input") if isinstance(cfg.get("input"), dict) else {}
    kuiper_input = prepare_local_json_mapping_input(
        cfg,
        raw_input,
        client=client,
        data=data,
        source_task_id=source_tid,
    )
    output = run_json_mapping_kuiper(cfg, kuiper_input)

    rows_materialized = 0
    if should_materialize_cohort_after_json_mapping(cfg):
        rows_materialized = materialize_json_mapping_output_to_cohort(
            client,
            data,
            task_id=task_id,
            cfg=cfg,
            output=output,
            log=log,
        )
        if rows_materialized == 0 and log and hasattr(log, "warning"):
            log.warning(
                "%s json_mapping cohort materialize produced 0 rows mapper_kind=%s source_task_id=%s",
                fn_external_id,
                cfg.get("mapper_kind"),
                source_tid,
            )

    return {
        "function_external_id": fn_external_id,
        "task_id": task_id,
        "status": "ok",
        "run_id": run_id,
        "mapper_kind": str(cfg.get("mapper_kind") or "custom"),
        "source_task_id": source_tid,
        "cohort_rows_written": rows_materialized,
        "output": output,
    }
