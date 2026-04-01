"""CDF handler for incremental state update (cohort + watermarks)."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Dict, Optional

try:
    from cognite.client import CogniteClient

    CDF_AVAILABLE = True
except ImportError:
    CDF_AVAILABLE = False

from ..cdf_fn_common.function_logging import resolve_function_logger
from ..cdf_fn_common.scope_document_dm import ensure_key_extraction_config_from_scope_dm
from ..fn_dm_key_extraction.config import Config
from .pipeline import incremental_state_update


def handle(
    data: Dict[str, Any],
    client: CogniteClient = None,
    logger: Optional[Any] = None,
) -> Dict[str, Any]:
    """Run incremental state detection; returns compact JSON with ``run_id``."""
    log: Any = None
    try:
        log = resolve_function_logger(data, logger)
        log.info("Starting fn_dm_incremental_state_update")

        if not client:
            raise ValueError("CogniteClient is required")

        ensure_key_extraction_config_from_scope_dm(
            data, client, incremental_change_processing=True
        )

        if "config" not in data:
            raise ValueError("Missing required key 'config' in input data")

        provided_config = data["config"]
        if provided_config.get("externalId"):
            data["workflow_config_external_id"] = str(provided_config.get("externalId"))

        unwrapped = provided_config.get("config", provided_config)
        cdf_config = Config.model_validate(unwrapped)

        if not cdf_config.parameters.incremental_change_processing:
            raise ValueError(
                "parameters.incremental_change_processing must be true for incremental state update"
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
