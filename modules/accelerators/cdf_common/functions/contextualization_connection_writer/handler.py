import traceback
from typing import Literal

from cognite.client import CogniteClient
from cognite.client.config import global_config
from cognite.client.data_classes import ExtractionPipelineRunWrite

from core.logger import CogniteFunctionLogger
from core.models import FUNCTION_ID, State
from core.utils import (
    iterate_new_approved_annotations,
    load_config,
    to_direct_relations_by_source_by_node,
    write_connections,
)

# Do not warn the user about feature previews from the Cognite-SDK we use in Toolkit
# ruff: noqa: E402
global_config.disable_pypi_version_check = True
global_config.silence_feature_preview_warnings = True

EXTRACTION_PIPELINE_EXTERNAL_ID = "ctx_files_direct_relation_write"
EXTERNAL_ID_LIMIT = 256
EXTRACTION_RUN_MESSAGE_LIMIT = 1000


def handle(data: dict, client: CogniteClient) -> dict:
    try:
        connection_count = execute(data, client)
    except Exception as e:
        tb = traceback.extract_tb(e.__traceback__)
        last_entry_this_file = next((entry for entry in reversed(tb) if entry.filename == __file__), None)
        suffix = ""
        if last_entry_this_file:
            suffix = f" in function {last_entry_this_file.name} on line {last_entry_this_file.lineno}: {last_entry_this_file.line}"

        status: Literal["failure", "success"] = "failure"
        # Truncate the error message to 1000 characters the maximum allowed by the API
        prefix = f"ERROR {FUNCTION_ID}: "
        error_msg = f'"{e!s}"'
        message = prefix + error_msg + suffix
        if len(message) >= EXTRACTION_RUN_MESSAGE_LIMIT:
            error_msg = error_msg[: EXTRACTION_RUN_MESSAGE_LIMIT - len(prefix) - len(suffix) - 3 - 1]
            message = prefix + error_msg + '..."' + suffix
    else:
        status = "success"
        message = f"{FUNCTION_ID} executed successfully. Created {connection_count} connections"

    client.extraction_pipelines.runs.create(
        ExtractionPipelineRunWrite(extpipe_external_id=EXTRACTION_PIPELINE_EXTERNAL_ID, status=status, message=message)
    )
    # Need to run at least daily or the sync endpoint will forget the cursors
    # (max time is 3 days).
    return {"status": status, "message": message}


def execute(data: dict, client: CogniteClient) -> int:
    logger = CogniteFunctionLogger(data.get("logLevel", "INFO"))  # type: ignore[arg-type]
    logger.debug("Starting connection write")
    config = load_config(client, EXTRACTION_PIPELINE_EXTERNAL_ID, logger)
    logger.debug("Loaded config successfully")

    state = State.from_cdf(client, config.state)
    connection_count = 0
    for annotation_list in iterate_new_approved_annotations(state, client, config.data.annotation_space, logger):
        annotation_by_source_by_node = to_direct_relations_by_source_by_node(
            annotation_list, config.data.direct_relation_mappings, logger
        )
        connections = write_connections(annotation_by_source_by_node, client, logger)
        connection_count += connections

    state.to_cdf(client, config.state)
    logger.info(f"Created {connection_count} connections")
    return connection_count
