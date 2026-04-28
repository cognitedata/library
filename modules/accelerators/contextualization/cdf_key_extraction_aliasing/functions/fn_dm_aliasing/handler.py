"""
CDF Handler for Aliasing

This module provides a CDF-compatible handler function that can be used in
CDF Functions or called directly, maintaining compatibility with the CDF
workflow format while using the existing AliasingEngine.
"""

from typing import Any, Dict

try:
    from cognite.client import CogniteClient

    CDF_AVAILABLE = True
except ImportError:
    CDF_AVAILABLE = False

from cdf_fn_common.function_logging import resolve_function_logger
from cdf_fn_common.scope_document_dm import ensure_aliasing_config_from_scope_dm
from cdf_fn_common.task_runtime import merge_compiled_task_into_data
from cdf_adapter import (
    _DEFAULT_ALIASING_VALIDATION,
    _convert_yaml_direct_to_aliasing_config,
    scope_has_key_extraction_rules,
)
from dependencies import create_client, get_env_variables
from .engine.tag_aliasing_engine import AliasingEngine


def handle(
    data: Dict[str, Any],
    client: CogniteClient = None,
) -> Dict[str, Any]:
    """
    CDF-compatible handler function for tag aliasing.

    This function follows the CDF function handler pattern and can be used
    in CDF Functions or called directly.

    Args:
        data: Dictionary containing:
            - config: Workflow-provided config payload
            - logLevel: Optional log level (DEBUG, INFO, WARNING, ERROR)
            - tags: Optional list of tags to generate aliases for
            - entities: Optional list of entities with tags to alias
        client: CogniteClient instance (required for RAW read/write workflow mode)

    Returns:
        JSON-serializable dict with status + summary.
    """
    log: Any = None

    try:
        loglevel = data.get("logLevel", "INFO")
        verbose = data.get("verbose", False)
        log = resolve_function_logger(data, None)

        log.info(f"Starting tag aliasing with loglevel = {loglevel} with verbose set to {verbose}")

        if not client:
            raise ValueError("CogniteClient is required for tag aliasing in CDF")

        merge_compiled_task_into_data(data)

        if data.get("configuration") or data.get("scope_document"):
            ensure_aliasing_config_from_scope_dm(data, client)

        # Load configuration from workflow payload (required)
        if "config" in data:
            # Workflow/direct config provided
            provided_config = data["config"]
            if isinstance(provided_config, dict):
                unwrapped = provided_config.get("config", provided_config)
                # CDF/workflow-shaped aliasing config
                al_data = (
                    unwrapped.get("data", {})
                    if isinstance(unwrapped, dict) and isinstance(unwrapped.get("data"), dict)
                    else {}
                )
                scope_doc = data.get("configuration") or data.get("scope_document")
                use_workflow_yaml = isinstance(unwrapped, dict) and isinstance(
                    unwrapped.get("data"), dict
                ) and (
                    "aliasing_rules" in al_data
                    or "pathways" in al_data
                    or scope_has_key_extraction_rules(
                        scope_doc if isinstance(scope_doc, dict) else None
                    )
                )
                if use_workflow_yaml:
                    aliasing_config = _convert_yaml_direct_to_aliasing_config(
                        {"config": unwrapped},
                        scope_document=scope_doc if isinstance(scope_doc, dict) else None,
                    )
                    log.info("Using workflow-provided aliasing config")

                    # Enable RAW upload from config parameters unless explicitly overridden
                    params = unwrapped.get("parameters", {})
                    if isinstance(params, dict):
                        if "raw_db" in params:
                            data.setdefault("raw_db", params["raw_db"])
                        if "raw_table_aliases" in params:
                            data.setdefault("raw_table", params["raw_table_aliases"])
                        if "raw_table_state" in params:
                            data.setdefault("raw_table_state", params["raw_table_state"])
                        data.setdefault("upload_to_raw", True)
                else:
                    # Direct engine-ready config
                    aliasing_config = provided_config
                    log.info("Using provided config directly")
            else:
                aliasing_config = provided_config
                log.info("Using provided config directly")
        else:
            log.warning(
                "No config in request; using identity passthrough (zero aliasing rules). "
                "CDF functions must pass aliasing config in the workflow payload."
            )
            aliasing_config = {
                "rules": [],
                "validation": dict(_DEFAULT_ALIASING_VALIDATION),
            }

        # Initialize engine with logger
        engine = AliasingEngine(aliasing_config, log)

        from pipeline import tag_aliasing

        tag_aliasing(client=client, logger=log, data=data, engine=engine)

        # CDF Functions requires the returned object to be JSON-serializable.
        # The pipeline stores rich objects in `data` (engines, mappings, etc),
        # so only return a compact summary.
        return {
            "status": "succeeded",
            "summary": {
                "total_tags_processed": int(data.get("total_tags_processed", 0)),
                "total_aliases_generated": int(data.get("total_aliases_generated", 0)),
                "raw_written": bool(data.get("upload_to_raw", False)),
                "raw_db": data.get("raw_db"),
                "raw_table_aliases": data.get("raw_table"),
            },
        }

    except Exception as e:
        message = f"Tag aliasing pipeline failed: {e!s}"

        if log:
            log.error(message)
        else:
            print(f"[ERROR] {message}")

        return {"status": "failure", "message": message}


def run_locally():
    """Run handler locally for testing (requires .env file and CDF config)."""
    # Use dependencies helper to get environment variables and create client
    env_config = get_env_variables()
    client = create_client(env_config, debug=False)

    # Test data
    data = {
        "logLevel": "DEBUG",
        "verbose": False,
        "tags": ["P-101", "FCV-2001A", "T-201"],
        "entities": [
            {
                "tag": "P-101",
                "entity_type": "asset",
                "context": {"site": "Plant_A", "equipment_type": "pump"},
            }
        ],
    }

    # Run handler
    print("Starting tag aliasing pipeline...")
    result = handle(data, client)

    if result["status"] == "succeeded":
        print("Pipeline completed successfully!")
    else:
        print(f"Pipeline failed: {result.get('message', 'Unknown error')}")

    return result


if __name__ == "__main__":
    try:
        result = run_locally()
        print(f"Final result: {result}")
    except Exception as e:
        print(f"Failed: {e}")
        import traceback

        traceback.print_exc()
