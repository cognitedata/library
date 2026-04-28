"""CDF handler for incremental state update (cohort + watermarks)."""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Any, Dict

try:
    from cognite.client import CogniteClient

    CDF_AVAILABLE = True
except ImportError:
    CDF_AVAILABLE = False

from cdf_fn_common.function_logging import resolve_function_logger
from cdf_fn_common.scope_document_dm import (
    ensure_key_extraction_config_from_scope_dm,
    incremental_change_processing_in_task_configuration,
)
from cdf_fn_common.task_runtime import merge_compiled_task_into_data
from pipeline import incremental_state_update


def handle(
    data: Dict[str, Any],
    client: CogniteClient = None,
) -> Dict[str, Any]:
    """Run incremental state detection; returns compact JSON with ``run_id``."""
    log: Any = None
    try:
        log = resolve_function_logger(data, None)
        log.info("Starting CNT-INCREMENTAL-STATE-UPDATE")

        if not client:
            raise ValueError("CogniteClient is required")

        merge_compiled_task_into_data(data)

        if not incremental_change_processing_in_task_configuration(data):
            log.info(
                "incremental_change_processing is false — skipping cohort/state RAW writes "
                "(v5 workflow keeps this task for a stable DAG)."
            )
            rid = data.get("run_id") or str(uuid.uuid4())
            data["run_id"] = rid
            return {
                "status": "succeeded",
                "run_id": rid,
                "message": '{"cohort_rows_written": 0, "skipped_incremental_disabled": true}',
            }

        ensure_key_extraction_config_from_scope_dm(
            data, client, incremental_change_processing=True
        )

        if "config" not in data:
            raise ValueError("Missing required key 'config' in input data")

        provided_config = data["config"]
        if provided_config.get("externalId"):
            data["workflow_config_external_id"] = str(provided_config.get("externalId"))

        unwrapped = provided_config.get("config", provided_config)
        if not isinstance(unwrapped, dict):
            raise ValueError("Expected dict config under data['config']")
        params = unwrapped.get("parameters") if isinstance(unwrapped.get("parameters"), dict) else {}
        data_section = unwrapped.get("data") if isinstance(unwrapped.get("data"), dict) else {}
        cdf_config = SimpleNamespace(
            parameters=SimpleNamespace(**params),
            data=SimpleNamespace(
                source_views=data_section.get("source_views", []) or [],
                source_view=data_section.get("source_view"),
                extraction_rules=data_section.get("extraction_rules", []) or [],
                associations=data_section.get("associations") or [],
            ),
        )

        incremental_state_update(client, log, data, cdf_config)

        return {
            "status": "succeeded",
            "run_id": data.get("run_id"),
            "message": data.get("message"),
        }
    except Exception as ex:
        message = f"incremental_state_update failed: {ex!s}"
        if log:
            log.error(message)
        return {"status": "failure", "message": message}


def run_locally() -> Dict[str, Any]:
    raise NotImplementedError("Use KEY_EXTRACTION_CONFIG_PATH with cdf-tk deploy")


if __name__ == "__main__":
    run_locally()
