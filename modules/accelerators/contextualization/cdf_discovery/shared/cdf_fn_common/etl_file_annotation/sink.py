"""Write file annotation hits to cohort RAW sink."""

from __future__ import annotations

from typing import Any, List, Mapping

from cdf_fn_common.etl_cohort_handoff import write_entity_rows_to_cohort_sink


def write_file_annotation_cohort_rows(
    client: Any,
    data: Mapping[str, Any],
    rows: List[Mapping[str, Any]],
    *,
    task_id: str,
    run_id: str,
    scope: str,
    log: Any = None,
) -> None:
    if not rows:
        return
    write_entity_rows_to_cohort_sink(
        client,
        data,
        run_id=run_id,
        scope_key=scope,
        task_id=task_id,
        query_source="file_annotation",
        entity_type="CogniteFile",
        view_space="cdf_cdm",
        view_external_id="CogniteFile",
        view_version="v1",
        rows=rows,
        log=log,
    )
