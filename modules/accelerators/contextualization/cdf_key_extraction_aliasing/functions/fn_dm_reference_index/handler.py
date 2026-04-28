"""
CDF handler: build/update RAW reference index from key-extraction FK + document JSON.
"""

from copy import deepcopy
from typing import Any, Dict

try:
    from cognite.client import CogniteClient

    CDF_AVAILABLE = True
except ImportError:
    CDF_AVAILABLE = False

from cdf_fn_common.function_logging import resolve_function_logger
from cdf_fn_common.scope_document_dm import apply_reference_index_scope_document
from cdf_fn_common.task_runtime import merge_compiled_task_into_data
from .dependencies import create_client, get_env_variables
from .pipeline import persist_reference_index


_DEFAULT_ALIASING_VALIDATION: Dict[str, Any] = {
    "max_aliases_per_tag": 50,
    "min_confidence": 0.01,
    "validation_rules": [
        {
            "name": "alias_length_check",
            "match": {
                "expressions": [
                    {},
                    {
                        "pattern": r"^.{101,}$",
                        "description": "Alias exceeds maximum length 100",
                    },
                ]
            },
        }
    ],
}


def handle(
    data: Dict[str, Any],
    client: CogniteClient = None,
) -> Dict[str, Any]:
    """
    Workflow payload (``data``) should include:

    - ``source_raw_db``, ``source_raw_table_key``: key-extraction state table
    - ``reference_index_raw_table``: target RAW table for inverted index (same or other db)
    - ``reference_index_raw_db``: optional (defaults to ``source_raw_db``)
    - ``config``: same shape as fn_dm_aliasing (``aliasing_rules`` for inline AliasingEngine)
    - ``source_instance_space``, ``source_view_space``, ``source_view_external_id``,
      ``source_view_version``: fallbacks when cohort columns are absent
    - ``reference_index_fk_entity_type`` / ``reference_index_document_entity_type``: optional
    - ``incremental_auto_run_id``, ``source_run_id``, ``source_workflow_status``: optional filters
    - ``source_raw_list_page_size``: RAW list chunk size for source table (default 10000)
    - ``source_raw_read_limit`` / ``raw_read_limit``: max source rows (default 10000); ``0`` = unlimited
    - ``reference_index_prefetch_table``: if true, list entire index RAW table once (fewer retrieves)
    - ``reference_index_retrieve_concurrency``: parallel ``retrieve`` for cold keys (default 1)
    - ``skip_reference_index_ddl``: skip ``create_table_if_not_exists`` when tables exist
    - ``enable_reference_index``: when false or omitted, no-op (return success without RAW writes)

    Args:
        logger: Optional injected logger; default built from ``data`` (``logLevel`` / ``verbose``).
    """
    log: Any = None
    try:
        log = resolve_function_logger(data, None)
        log.info("Starting reference index persistence")

        if not client:
            raise ValueError("CogniteClient is required")

        merge_compiled_task_into_data(data)

        apply_reference_index_scope_document(data, client)

        if not data.get("enable_reference_index"):
            log.info(
                "Reference index skipped: enable_reference_index is false or omitted "
                "(set true in workflow task data to persist)"
            )
            return {
                "status": "succeeded",
                "summary": {
                    "reference_index_skipped": True,
                    "reference_index_skip_reason": "enable_reference_index_false",
                    "reference_index_entities_processed": 0,
                    "reference_index_inverted_writes": 0,
                    "reference_index_posting_events": 0,
                    "reference_index_fk_posting_events": 0,
                    "reference_index_document_posting_events": 0,
                    "reference_index_raw_db": data.get("reference_index_raw_db"),
                    "reference_index_raw_table": data.get("reference_index_raw_table"),
                    "reference_index_insert_batches": 0,
                    "reference_index_source_list_chunks": 0,
                },
            }

        persist_reference_index(client=client, logger=log, data=data)

        return {
            "status": "succeeded",
            "summary": {
                "reference_index_entities_processed": int(
                    data.get("reference_index_entities_processed", 0)
                ),
                "reference_index_inverted_writes": int(
                    data.get("reference_index_inverted_writes", 0)
                ),
                "reference_index_posting_events": int(
                    data.get("reference_index_posting_events", 0)
                ),
                "reference_index_fk_posting_events": int(
                    data.get("reference_index_fk_posting_events", 0)
                ),
                "reference_index_document_posting_events": int(
                    data.get("reference_index_document_posting_events", 0)
                ),
                "reference_index_raw_db": data.get("reference_index_raw_db"),
                "reference_index_raw_table": data.get("reference_index_raw_table"),
                "reference_index_insert_batches": int(
                    data.get("reference_index_insert_batches", 0)
                ),
                "reference_index_source_list_chunks": int(
                    data.get("reference_index_source_list_chunks", 0)
                ),
            },
        }
    except Exception as e:
        msg = f"Reference index failed: {e!s}"
        if log:
            log.error(msg)
        else:
            print(f"[ERROR] {msg}")
        return {"status": "failure", "message": msg}


def run_locally() -> Dict[str, Any]:
    import os

    _ref_validation = deepcopy(_DEFAULT_ALIASING_VALIDATION)
    _ref_validation["max_aliases_per_tag"] = 100

    env = get_env_variables()
    client = create_client(env, debug=False)
    site = os.getenv("SITE_ABBREVIATION", "SITE")
    data = {
        "logLevel": "DEBUG",
        "enable_reference_index": True,
        "source_raw_db": "db_key_extraction",
        "source_raw_table_key": f"{site}_key_extraction_state",
        "reference_index_raw_db": "db_key_extraction",
        "reference_index_raw_table": f"{site}_reference_index",
        "source_raw_read_limit": 500,
        "source_instance_space": os.getenv("CDF_INSTANCE_SPACE", "space"),
        "source_view_space": "cdf_cdm",
        "source_view_external_id": "CogniteFile",
        "source_view_version": "v1",
        "config": {
            "config": {
                "parameters": {"debug": True},
                "data": {
                    "aliasing_rules": [],
                    "validation": _ref_validation,
                },
            }
        },
    }
    return handle(data, client)


if __name__ == "__main__":
    print(run_locally())
