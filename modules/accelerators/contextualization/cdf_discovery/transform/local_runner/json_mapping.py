"""Local execution for compiled CDF ``jsonMapping`` workflow tasks (``cognite-kuiper``)."""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, Mapping, MutableMapping

from cdf_fn_common.etl_annotation_map.kuiper_templates import (
    prepare_local_json_mapping_input,
    run_json_mapping_kuiper,
)
from cdf_fn_common.etl_json_mapping_sink import (
    materialize_json_mapping_output_to_cohort,
    should_materialize_cohort_after_json_mapping,
)

_OUTPUT_REF_RE = re.compile(r"^\$\{([A-Za-z0-9_]+)\.output(?:\.(.+))?\}$")


def _task_config(task: Mapping[str, Any]) -> Dict[str, Any]:
    payload = task.get("payload")
    if isinstance(payload, dict):
        cfg = payload.get("config")
        if isinstance(cfg, dict):
            return dict(cfg)
    return {}


def _task_output_root(summary: Any) -> Any:
    if not isinstance(summary, dict):
        return summary
    raw = summary.get("output")
    return raw if raw is not None else summary


def _resolve_output_ref(ref: str, summaries: Mapping[str, Any]) -> Any:
    m = _OUTPUT_REF_RE.match(ref.strip())
    if not m:
        return ref
    task_id, path = m.group(1), m.group(2)
    cur = _task_output_root(summaries.get(task_id))
    if not path:
        return cur
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def resolve_json_mapping_input(
    value: Any,
    summaries: Mapping[str, Any],
) -> Any:
    """Resolve ``${task_id.output}`` placeholders in jsonMapping input."""
    if isinstance(value, str):
        if value.strip().startswith("${") and value.strip().endswith("}"):
            return _resolve_output_ref(value, summaries)
        return value
    if isinstance(value, list):
        return [resolve_json_mapping_input(v, summaries) for v in value]
    if isinstance(value, dict):
        return {k: resolve_json_mapping_input(v, summaries) for k, v in value.items()}
    return value


def _source_task_id(task: Mapping[str, Any], cfg: Mapping[str, Any]) -> str:
    deps = [str(d) for d in (task.get("depends_on") or []) if str(d).strip()]
    if deps:
        return deps[0]
    raw = cfg.get("source_task_id")
    return str(raw).strip() if raw is not None else ""


def run_local_json_mapping_task(
    task: Mapping[str, Any],
    *,
    summaries: Mapping[str, Any],
    shared_data: MutableMapping[str, Any],
    client: Any,
    logger: logging.Logger,
    dry_run: bool,
) -> Dict[str, Any]:
    task_id = str(task.get("id") or "").strip()
    cfg = _task_config(task)

    if dry_run or client is None:
        return {
            "status": "skipped",
            "reason": "dry_run",
            "task_id": task_id,
            "task_type": "jsonMapping",
        }

    raw_input = cfg.get("input") if isinstance(cfg.get("input"), dict) else {}
    resolved_refs = resolve_json_mapping_input(raw_input, summaries)
    if not isinstance(resolved_refs, dict):
        raise ValueError("jsonMapping input must resolve to a JSON object")

    source_tid = _source_task_id(task, cfg)
    data: Dict[str, Any] = dict(shared_data)
    data["task_id"] = task_id
    data["compiled_task"] = task
    if source_tid:
        data["source_task_id"] = source_tid

    kuiper_input = prepare_local_json_mapping_input(
        cfg,
        resolved_refs,
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
            log=logger,
        )
        if rows_materialized == 0 and logger and hasattr(logger, "warning"):
            logger.warning(
                "jsonMapping task %s: cohort materialize produced 0 rows (mapper_kind=%s)",
                task_id,
                cfg.get("mapper_kind"),
            )

    if data.get("run_id"):
        shared_data["run_id"] = data["run_id"]

    shared_data.setdefault("task_outputs", {})[task_id] = output
    return {
        "status": "ok",
        "task_id": task_id,
        "task_type": "jsonMapping",
        "output": output,
        "cohort_rows_written": rows_materialized,
    }
