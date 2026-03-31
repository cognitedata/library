"""
CDF handler: build/update RAW reference index from key-extraction FK + document JSON.
"""

from typing import Any, Dict

try:
    from cognite.client import CogniteClient

    CDF_AVAILABLE = True
except ImportError:
    CDF_AVAILABLE = False

from .dependencies import create_client, create_logger_service, get_env_variables
from .pipeline import persist_reference_index


def handle(data: Dict[str, Any], client: CogniteClient = None) -> Dict[str, Any]:
    """
    Workflow payload (data) should include:
      - source_raw_db, source_raw_table_key: key-extraction state table
      - reference_index_raw_table: target RAW table for inverted index (same or other db)
      - reference_index_raw_db: optional (defaults to source_raw_db)
      - config: same shape as fn_dm_aliasing (aliasing_rules for inline AliasingEngine)
      - source_instance_space, source_view_space, source_view_external_id, source_view_version:
        fallbacks when cohort columns are absent
      - reference_index_fk_entity_type / reference_index_document_entity_type: optional
      - incremental_auto_run_id, source_run_id, source_workflow_status: optional filters
    """
    logger = None
    try:
        loglevel = data.get("logLevel", "INFO")
        verbose = bool(data.get("verbose", False))
        logger = create_logger_service(loglevel, verbose)
        logger.info("Starting reference index persistence")

        if not client:
            raise ValueError("CogniteClient is required")

        persist_reference_index(client=client, logger=logger, data=data)

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
                "reference_index_raw_db": data.get("reference_index_raw_db"),
                "reference_index_raw_table": data.get("reference_index_raw_table"),
            },
        }
    except Exception as e:
        msg = f"Reference index failed: {e!s}"
        if logger:
            logger.error(msg)
        else:
            print(f"[ERROR] {msg}")
        return {"status": "failure", "message": msg}


def run_locally() -> Dict[str, Any]:
    import os

    env = get_env_variables()
    client = create_client(env, debug=False)
    site = os.getenv("SITE_ABBREVIATION", "SITE")
    data = {
        "logLevel": "DEBUG",
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
                    "validation": {
                        "max_aliases_per_tag": 50,
                        "min_alias_length": 2,
                        "max_alias_length": 100,
                        "allowed_characters": r"A-Za-z0-9-_/. ",
                    },
                },
            }
        },
    }
    return handle(data, client)


if __name__ == "__main__":
    print(run_locally())
