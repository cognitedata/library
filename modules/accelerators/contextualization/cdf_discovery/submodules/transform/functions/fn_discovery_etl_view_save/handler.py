"""CDF handler: ETL DM view save stage."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, MutableMapping

_staging_root = Path(__file__).resolve().parent.parent
if str(_staging_root) not in sys.path:
    sys.path.insert(0, str(_staging_root))

from cdf_fn_common.etl_diagram_annotation_save import etl_apply_diagram_annotation_save
from cdf_fn_common.etl_save_apply import etl_apply_view_save
from cdf_fn_common.etl_discovery_query_shared import resolve_task_config
from cdf_fn_common.etl_task_runtime import merge_compiled_task_into_data


def etl_handle_save_view(
    fn_external_id: str,
    data: MutableMapping[str, Any],
    client: Any,
    log: Any,
) -> Dict[str, Any]:
    merge_compiled_task_into_data(data)
    cfg = resolve_task_config(data)
    view_external_id = str(cfg.get("view_external_id") or "").strip()
    if view_external_id == "CogniteDiagramAnnotation":
        return etl_apply_diagram_annotation_save(fn_external_id, data, client, log)
    return etl_apply_view_save(fn_external_id, data, client, log)


def handle(data: Dict[str, Any], client: Any = None) -> Dict[str, Any]:
    return etl_handle_save_view("fn_discovery_etl_view_save", data, client, log=None)
