"""Orchestration for fn_etl_workflow_fanout_plan (profile-driven dynamic task generator)."""

from __future__ import annotations

import uuid
from typing import Any, Dict, MutableMapping

from cdf_fn_common.etl_discovery_query_shared import resolve_task_config
from cdf_fn_common.etl_fanout_plan.registry import get_fanout_profile
from cdf_fn_common.etl_file_processing_state import (
    optional_positive_int,
    resolve_file_workflow_params,
)
from cdf_fn_common.etl_task_runtime import merge_compiled_task_into_data


def etl_handle_workflow_fanout_plan(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    if client is None:
        raise ValueError("workflow_fanout_plan requires a CDF client")

    merge_compiled_task_into_data(data)
    params = resolve_file_workflow_params(data)
    cfg = resolve_task_config(data)
    if cfg.get("batch_size") is not None:
        params["batch_size"] = int(cfg.get("batch_size"))
    if cfg.get("max_pattern_samples") is not None:
        params["max_pattern_samples"] = int(cfg.get("max_pattern_samples"))
    if cfg.get("pattern_normalization"):
        params["pattern_normalization"] = str(cfg.get("pattern_normalization"))
    if cfg.get("child_function_external_id"):
        params["child_function_external_id"] = str(cfg.get("child_function_external_id"))
    if cfg.get("max_files_per_run") is not None:
        params["max_files_per_run"] = optional_positive_int(cfg.get("max_files_per_run"))
    if cfg.get("max_attempts") is not None:
        params["max_attempts"] = int(cfg.get("max_attempts"))
    task_id = str(data.get("task_id") or fn_external_id)

    run_id = str(data.get("run_id") or "").strip()
    if not run_id:
        run_id = str(uuid.uuid4())
        data["run_id"] = run_id

    profile_name = str(cfg.get("fanout_profile") or "file_annotation").strip().lower()
    profile = get_fanout_profile(profile_name)
    result = profile.build_tasks(
        client=client,
        data=dict(data),
        cfg=dict(cfg),
        params=params,
        log=log,
    )

    return {
        "function_external_id": fn_external_id,
        "task_id": task_id,
        "fanout_profile": profile_name,
        **result,
    }
