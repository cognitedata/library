"""Persist jsonMapping Kuiper output into the task cohort RAW sink (local / save_raw handoff)."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, MutableMapping

from cdf_fn_common.etl_annotation_map.kuiper_templates import is_diagram_mapper_kind
from cdf_fn_common.etl_cohort_handoff import write_entity_rows_to_cohort_sink
from cdf_fn_common.etl_cohort_storage import require_pipeline_run_key


def cohort_rows_from_staging_props(
    staging_rows: List[Mapping[str, Any]],
    *,
    mapper_kind: str,
    run_id: str,
    scope_key: str,
    default_space: str,
) -> List[Dict[str, Any]]:
    """Turn jsonMapping staging property dicts into entity cohort rows for RAW save."""
    out: List[Dict[str, Any]] = []
    for props in staging_rows:
        if not isinstance(props, Mapping):
            continue
        p = dict(props)
        if mapper_kind == "diagram_detect_to_dm":
            space = str(p.get("annotation_space") or default_space).strip()
            ext = str(p.get("annotation_external_id") or "").strip()
            node_id = f"{space}:{ext}" if space and ext else ext or space
            entity_type = "CogniteDiagramAnnotation"
            view_space = "cdf_discovery"
            view_external_id = "AnnotationStaging"
        else:
            space = ""
            ext = str(p.get("file_external_id") or p.get("annotation_external_id") or "").strip()
            node_id = ext or str(p.get("file_id") or "")
            entity_type = "ClassicAnnotationStaging"
            view_space = "cdf_discovery"
            view_external_id = "ClassicAnnotationStaging"
        if not ext and not node_id:
            continue
        out.append(
            {
                "columns": {
                    "node_instance_id": node_id,
                    "external_id": ext or node_id,
                    "view_space": view_space,
                    "view_external_id": view_external_id,
                    "view_version": "v1",
                    "entity_type": entity_type,
                    "run_id": run_id,
                    "scope_key": scope_key,
                    "instance_space": space,
                },
                "properties": p,
            }
        )
    return out


def should_materialize_cohort_after_json_mapping(cfg: Mapping[str, Any]) -> bool:
    if cfg.get("materialize_cohort") is True:
        return True
    return is_diagram_mapper_kind(str(cfg.get("mapper_kind") or ""))


def materialize_json_mapping_output_to_cohort(
    client: Any,
    data: MutableMapping[str, Any],
    *,
    task_id: str,
    cfg: Mapping[str, Any],
    output: Any,
    log: Any,
) -> int:
    """Write diagram/staging Kuiper output rows to this task's cohort table."""
    mapper_kind = str(cfg.get("mapper_kind") or "custom").strip().lower()
    if not should_materialize_cohort_after_json_mapping(cfg):
        return 0

    run_id = require_pipeline_run_key(data)
    scope_key = str(
        cfg.get("workflow_scope") or data.get("workflow_scope") or "file_pattern_extract"
    ).strip()

    staging_rows: List[Mapping[str, Any]] = []
    if isinstance(output, list):
        staging_rows = [r for r in output if isinstance(r, dict)]
    elif isinstance(output, dict):
        staging_rows = [output]

    if not staging_rows:
        return 0

    default_space = (
        str(cfg.get("annotation_space") or "discovery-annotations").strip()
        if mapper_kind == "diagram_detect_to_dm"
        else ""
    )
    mapped = cohort_rows_from_staging_props(
        staging_rows,
        mapper_kind=mapper_kind,
        run_id=run_id,
        scope_key=scope_key,
        default_space=default_space,
    )
    if not mapped:
        return 0

    write_entity_rows_to_cohort_sink(
        client,
        data,
        run_id=run_id,
        scope_key=scope_key,
        task_id=task_id,
        query_source="json_mapping",
        entity_type="CogniteFile",
        view_space="cdf_cdm",
        view_external_id="CogniteFile",
        view_version="v1",
        rows=mapped,
        log=log,
    )
    return len(mapped)
