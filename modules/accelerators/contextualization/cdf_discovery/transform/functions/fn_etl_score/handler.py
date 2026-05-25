"""CDF handler: ETL scoring stage."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, MutableMapping

_staging_root = Path(__file__).resolve().parent.parent
if str(_staging_root) not in sys.path:
    sys.path.insert(0, str(_staging_root))

from cdf_fn_common.etl_common import (
    _first_nonempty,
    iter_predecessor_rows,
    merge_compiled_task_into_data,
    resolve_run_id,
    resolve_task_config,
)
from cdf_fn_common.etl_score_validate import (
    materialize_scoring_rules,
    score_row_properties,
    validate_scoring_config,
)


def etl_handle_score(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    merge_compiled_task_into_data(data)
    cfg = resolve_task_config(data)
    validate_scoring_config(cfg)
    rules = materialize_scoring_rules(cfg)
    run_id = resolve_run_id(data)
    data["run_id"] = run_id
    task_id = _first_nonempty(data.get("task_id"), fn_external_id)

    rows_read = 0
    rows_written = 0
    out_rows: list[dict[str, Any]] = []

    for cols, props in iter_predecessor_rows(data):
        rows_read += 1
        scored = score_row_properties(props, cfg, rules)
        out_rows.append({"columns": cols, "properties": scored})
        rows_written += 1

    data["_predecessor_rows"] = out_rows
    return {
        "function_external_id": fn_external_id,
        "task_id": task_id,
        "rows_read": rows_read,
        "rows_written": rows_written,
        "run_id": run_id,
    }


def handle(data: Dict[str, Any], client: Any = None) -> Dict[str, Any]:
    return etl_handle_score("fn_etl_score", data, client, log=None)
