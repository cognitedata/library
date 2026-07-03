from __future__ import annotations

import json
import time
import traceback
from datetime import UTC, datetime
from enum import Enum
from hashlib import sha256
from typing import TYPE_CHECKING, Any

from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes import (
    ExtractionPipelineRun,
    Row,
)
from cognite.client.data_classes.contextualization import DiagramDetectConfig, DiagramDetectResults
from cognite.client.data_classes.data_modeling import (
    DirectRelationReference,
    EdgeApply,
    EdgeId,
    Node,
    NodeId,
    NodeList,
    NodeOrEdgeData,
    ViewId,
)
from cognite.client.data_classes.data_modeling.query import NodeResultSetExpression, Query, Select, SourceSelector
from cognite.client.exceptions import CogniteAPIError
from config import Config, ViewPropertyConfig

# RawUploadQueue is only constructed at runtime in annotate_p_and_id; importing
# it lazily lets pipeline.py be imported (e.g. for unit tests) without the
# cognite-extractor-utils package installed. The type hint below stays valid
# because of `from __future__ import annotations`.
if TYPE_CHECKING:  # pragma: no cover
    from cognite.extractorutils.uploader import RawUploadQueue
from constants import (
    ANNOTATE_BATCH_SIZE,
    BATCH_SIZE,
    EXTERNAL_ID_LIMIT,
    FILE_LINK_EXTERNAL_ID,
    FUNCTION_ID,
    STAT_STORE_CURSOR,
    STAT_STORE_NUM_IN_BATCH,
    STAT_STORE_VALUE,
)
from logger import CogniteFunctionLogger


class DiagramAnnotationStatus(Enum):
    SUGGESTED = "Suggested"
    APPROVED = "Approved"


def _truncate(msg: str, max_len: int) -> str:
    """Trim a string to max_len characters with an ellipsis if truncated."""
    return msg if len(msg) <= max_len else msg[: max_len - 3] + "..."


# Define a custom exception
class DiagramDetectError(Exception):
    def __init__(self, message="Error processing Diagram Detect"):
        super().__init__(message)


def annotate_p_and_id(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    data: dict[str, Any],
    config: Config
) -> None:
    """
    Read configuration and start P&ID annotation process by
    1. Reading files to annotate
    2. Get file entities to be matched against files in P&ID
    3. Read existing annotations for the found files
    4. Get assets and put it into the list of entities to be found in the P&ID
    5. Process file:
        - detecting entities
        - creation annotations.
        - remove duplicate annotations

    Args:
        client: An instantiated CogniteClient
        config: A dataclass containing the configuration for the annotation process
    """
    pipeline_ext_id = data["ExtractionPipelineExtId"]
    error_count, annotated_count = 0, 0
    # Local copy so failure-mode adjustments (e.g. drop to size 1) don't leak
    # across function invocations in a warm container.
    annotate_batch_size = ANNOTATE_BATCH_SIZE
    try:
        file_cursor = None
        file_num = 0
        if config.parameters.debug:
            logger = CogniteFunctionLogger("DEBUG")
            logger.debug("**** Write debug messages and only process one file *****")
            annotate_batch_size = 1

        logger.debug("Initiate RAW upload queue used to store output from Diagram parsing")
        from cognite.extractorutils.uploader import RawUploadQueue
        raw_uploader = RawUploadQueue(cdf_client=client, max_queue_size=500000, trigger_log_level="INFO")

        # Check if we should run all files (then delete state content in RAW) or just new files
        if config.parameters.run_all:
            logger.debug("Run all files, delete state content in RAW since we are rerunning based on all input")
            delete_table(client, config.parameters.raw_db, config.parameters.raw_table_doc_tag)
            delete_table(client, config.parameters.raw_db, config.parameters.raw_table_doc_doc)
            delete_table(client, config.parameters.raw_db, config.parameters.raw_table_state)
        elif config.parameters.debug_file and config.parameters.debug:
            logger.debug(f"Since debugging with one file name: {config.parameters.debug_file} - ignore cursor and batch number")

        # Ensure RAW database/tables exist exactly once per invocation, before any
        # state reads/writes or per-batch RAW uploads (M3). The create_table helper
        # is idempotent.
        raw_db = config.parameters.raw_db
        create_table(client, raw_db, config.parameters.raw_table_state)
        create_table(client, raw_db, config.parameters.raw_table_doc_tag)
        create_table(client, raw_db, config.parameters.raw_table_doc_doc)

        if not config.parameters.run_all and not (
            config.parameters.debug_file and config.parameters.debug
        ):
            logger.debug("Get file cursor and batch number from RAW, to continue processing from last run")
            file_cursor = read_state_cursor(client, logger, raw_db, config.parameters.raw_table_state)
            file_num = read_state_batch_num(client, logger, raw_db, config.parameters.raw_table_state)

        logger.debug("Create entities for files, assets, equipment and more if in configuration")
        entities = get_all_entities(client, logger, config)

        logger.debug("Get files that has been updated since last run")
        files_view_id = config.data.annotation_job.file_view.as_view_id()
        new_files = get_new_files(client, logger, file_cursor, files_view_id, config)

        logger.debug(f"Number of new files to process are: {len(new_files['files'])}")
        if len(new_files["files"]) == 0:
            logger.debug("No new files to process, we are done - just update pipeline run")
            update_pipeline_run(client, logger, pipeline_ext_id, "success", annotated_count, error_count, None)
            return

        original_batch_size = annotate_batch_size
        while len(new_files["files"]) > 0:
            doc_doc = []
            doc_tag = []
            annotation_view_id = config.data.annotation_view.as_view_id()
            search_property = config.data.annotation_job.file_view.search_property
            file_ids = new_files["files"].as_ids()
            # Size of the most recently attempted batch; consulted by the
            # exception handlers below so they don't double-count or
            # mis-count using a post-mutated annotate_batch_size.
            last_batch_size = 0
            try:
                for num in range(file_num, len(file_ids), annotate_batch_size):
                    batch = file_ids[num:num+annotate_batch_size]
                    last_batch_size = len(batch)
                    result = run_diagram_detect(client, logger, entities, batch, search_property)
                    if result is None:
                        error_count += last_batch_size
                    else:
                        errors_before = error_count
                        error_count = push_result_to_annotations(
                            client,
                            config,
                            logger,
                            annotation_view_id,
                            files_view_id,
                            result,
                            new_files,
                            error_count,
                            doc_doc,
                            doc_tag
                        )
                        errors_added = error_count - errors_before
                        annotated_count += last_batch_size - errors_added

                        # Update raw with new annotations
                        write_mapping_to_raw(client, config, raw_uploader, doc_doc, doc_tag, logger)

                    logger.debug("Update state store with doc num in batch - in case timeout to set water mark")
                    update_state_store(client, logger, file_cursor, num+annotate_batch_size, config, None, STAT_STORE_NUM_IN_BATCH)

                    if config.parameters.debug:
                        break

                if not config.parameters.debug:
                    logger.debug("Update state store with new cursor - in case timeout on next loop to set water mark")
                    file_num = 0
                    file_cursor = new_files.cursors["files"]
                    update_state_store(client, logger, file_cursor, file_num, config, STAT_STORE_CURSOR, STAT_STORE_NUM_IN_BATCH)

                logger.debug("Update pipeline run with success")
                update_pipeline_run(client, logger, pipeline_ext_id, "success", annotated_count, error_count, None)
                error_count, annotated_count = 0, 0

                logger.debug("look for more files to process...")
                new_files = get_new_files(client, logger, file_cursor, files_view_id, config)

                if config.parameters.debug:
                    break

            except DiagramDetectError as e:
                # DiagramDetectError is raised by run_diagram_detect only for single-file
                # batches that exhausted retries, so last_batch_size is 1 here.
                error_count += last_batch_size
                msg = f"Skipping file - diagram detect failed with error: {e!s}"
                logger.error(msg)
                file_num += last_batch_size
                annotate_batch_size = original_batch_size

            except Exception as e:
                if annotate_batch_size > 1:
                    msg = f"Failed to push: {last_batch_size} annotations to data model, setting Batch Size = 1 and retry error: {e!s}"
                    logger.error(msg)
                    annotate_batch_size = 1
                else:
                    error_count += last_batch_size
                    msg = f"Failed to push: {last_batch_size} annotations to data model, error: {e!s}"
                    logger.error(msg)
                    raise Exception(msg) from e

    except Exception as e:
        msg = f"failed, Message: {e!s}"
        update_pipeline_run(client, logger, pipeline_ext_id, "failure", annotated_count, error_count, msg)
        raise Exception(msg) from e



def update_pipeline_run(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    xid: str,
    status: str,
    annotated_count: int,
    error_count: int,
    error: str | None = None
) -> None:

    if status == "success":
        msg = (
            f"Annotated P&ID file(s) annotated: {annotated_count}, "
            f" - NOT annotated due to errors: {error_count}"
        )
        logger.info(msg)
    else:
        msg = (
            f"Annotated P&ID files, OK: {annotated_count}, errors : {error_count}, "
            f"{error}, traceback:\n{traceback.format_exc()}"
        )
        logger.error(msg)

    client.extraction_pipelines.runs.create(
        ExtractionPipelineRun(
            extpipe_external_id=xid,
            status=status,
            message=_truncate(msg, 1000)
        )
    )

def _read_state_value(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    key: str,
    db: str,
    table: str,
) -> Any:
    """Look up a single state value from RAW. Returns None if not found.

    The state table is expected to exist already; callers should ensure
    it via a one-time `create_table` call at pipeline start (M3).
    """
    logger.info(f"Read state from DB: {db} Table: {table} Key: {key}")
    row_list = client.raw.rows.list(
        db_name=db, table_name=table, columns=[STAT_STORE_VALUE], limit=-1
    )
    for row in row_list:
        if row.key == key:
            return row.columns[STAT_STORE_VALUE]
    logger.debug(f"State not found for key: {key}")
    return None


def read_state_cursor(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    db: str,
    table: str,
) -> str | None:
    """Read the persisted sync cursor; None if no prior state."""
    raw_value = _read_state_value(client, logger, STAT_STORE_CURSOR, db, table)
    if raw_value is None:
        return None
    return str(raw_value)


def read_state_batch_num(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    db: str,
    table: str,
) -> int:
    """Read the persisted in-batch index, coercing missing/non-int values to 0."""
    raw_value = _read_state_value(client, logger, STAT_STORE_NUM_IN_BATCH, db, table)
    if raw_value is None:
        return 0
    try:
        return int(raw_value)
    except (TypeError, ValueError):
        logger.warning(
            f"Stored {STAT_STORE_NUM_IN_BATCH} is not an int ({raw_value!r}); resetting to 0."
        )
        return 0


def update_state_store(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    file_cursor: str,
    file_num: int,
    config: Config,
    cursor: str,
    batch_num: str,
) -> None:
    """Persist cursor and/or batch_num into the state table.

    The state table is expected to exist already; callers should ensure
    it via a one-time `create_table` call at pipeline start (M3).
    """
    if cursor:
        state_row = Row(cursor, {STAT_STORE_VALUE: file_cursor})
        client.raw.rows.insert(config.parameters.raw_db, config.parameters.raw_table_state, state_row)

    if batch_num:
        state_row = Row(batch_num, {STAT_STORE_VALUE: file_num})
        client.raw.rows.insert(config.parameters.raw_db, config.parameters.raw_table_state, state_row)

    logger.debug(f"Update state store DB: {config.parameters.raw_db} Table: {config.parameters.raw_table_state}")


def get_all_entities(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    config: Config,
) -> NodeList[Node]:
    """
    Read all entities from space

    Every entity dict stores its searchable value under the file view's
    search_property name. That key must match the diagram-detect
    `search_field` argument used in run_diagram_detect, otherwise the API
    silently returns no matches for the entity.

    :returns: Nodelist of entities nodes
    """
    entities = []
    job_config = config.data.annotation_job
    all_files = get_all_files(client, logger, job_config.file_view)
    file_search_property = job_config.file_view.search_property
    file_view_id = job_config.file_view.as_view_id()

    for file in all_files:
        entities.append(
            {
                "external_id": file.external_id,
                "name": file.properties[file_view_id].get("name", ""),
                "space": file.space,
                file_search_property: file.properties[file_view_id].get(file_search_property, ""),
                "annotation_type_external_id": job_config.file_view.type,
            }
        )
    logger.debug(f"Number of files added as entities: {len(entities)}")

    for entity_view in job_config.entity_views:

        entity_type = entity_view.type
        view_search_property = entity_view.search_property
        view_id = entity_view.as_view_id()
        logger.debug(
            f"Get all entities from view: {view_id} with search property: "
            f"{view_search_property} and type: {entity_type}"
        )

        if view_search_property != file_search_property:
            logger.warning(
                f"View {view_id} declares search_property '{view_search_property}' but "
                f"the file view's search_property '{file_search_property}' is used as the "
                f"diagram-detect search_field. Entities from this view will be stored "
                f"under '{file_search_property}' to remain matchable."
            )

        is_selected = get_entity_filter(entity_view, logger)

        entity_list = client.data_modeling.instances.list(
            space=entity_view.instance_space,
            sources=[view_id],
            filter=is_selected,
            limit=-1,
        )

        warning_logged = False
        for entity in entity_list:
            if view_search_property in entity.properties[view_id]:
                if not warning_logged:
                    logger.debug(f"View {view_id} contains {view_search_property} property")
                    warning_logged = True
                value = entity.properties[view_id][view_search_property]
            else:
                if not warning_logged:
                    logger.warning(
                        f"View {view_id} does not contain {view_search_property} property, "
                        f"using name instead"
                    )
                    warning_logged = True
                value = entity.properties[view_id]["name"]

            entities.append(
                {
                    "external_id": entity.external_id,
                    "name": entity.properties[view_id]["name"],
                    "space": entity.space,
                    file_search_property: value,
                    "annotation_type_external_id": entity_type,
                }
            )
        logger.info(
            f"Total number of entities: {len(entities)} including elements from "
            f"view: {view_id} and type: {entity_type}"
        )

    # Two views (or a file view + an entity view) referencing the same node
    # would otherwise produce duplicate entities, which the diagram-detect API
    # turns into duplicate edges (different external_ids because
    # create_annotation_id is detect-result-content-aware, but semantically
    # duplicate). De-dup on (space, external_id) and warn if anything was
    # dropped (H8).
    seen: set[tuple[str, str]] = set()
    deduped: list[dict] = []
    for entity in entities:
        key = (entity["space"], entity["external_id"])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(entity)

    dropped = len(entities) - len(deduped)
    if dropped:
        logger.warning(
            f"Dropped {dropped} duplicate entities (same (space, external_id)) "
            f"that appeared in more than one configured view. Total unique "
            f"entities: {len(deduped)}."
        )

    return deduped



def get_all_files(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    file_view_config: ViewPropertyConfig,
) -> NodeList[Node]:
    """
    Read all files matching the configured filter (uploaded, supported mime
    type, plus any caller-defined extra filter).

    :returns: NodeList of file nodes
    """
    logger.debug(f"Get all files from view: {file_view_config.as_view_id()}")
    is_selected = get_file_filter(file_view_config, None, logger)

    files = client.data_modeling.instances.list(
        space=file_view_config.instance_space,
        sources=[file_view_config.as_view_id()],
        filter=is_selected,
        limit=-1
    )

    logger.info(f"Num files: {len(files)} from view: {file_view_config.as_view_id()}")

    return files

def get_new_files(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    file_cursor: str,
    files_view_id: ViewId,
    config: Config
) -> NodeList[Node]:
    """
    Read new (or changed) files since the last persisted sync cursor, using
    the configured file filter.

    :returns: NodeList of file nodes
    """

    file_view_config = config.data.annotation_job.file_view
    debug_file = config.parameters.debug_file if config.parameters.debug else None

    logger.debug(f"Get new files from view: {files_view_id}, based on config: {file_view_config}")
    is_selected = get_file_filter(file_view_config, debug_file, logger)
    property_list = ["name", "sourceId", file_view_config.search_property]

    sync_query = Query(
        with_={
            "files": NodeResultSetExpression(
                filter=is_selected, limit=BATCH_SIZE
            ),
        },
        select={
            "files": Select([SourceSelector(files_view_id, property_list)]),
        },
    )

    num_retry = 0
    retry = True
    sync_result = None

    max_retries = 3
    while retry:
        sync_query.cursors["files"] = file_cursor
        try:
            sync_result = client.data_modeling.instances.sync(sync_query)
            retry = False
        except CogniteAPIError as e:
            msg = f"failed, Message: {e!s}"
            if e.code != 400:
                raise Exception(msg) from e

            num_retry += 1
            if num_retry > max_retries:
                # Persistent 400 with this cursor is operator-actionable
                # (e.g. the data model was rebuilt and the cursor is no longer
                # valid). Bail out without silently restarting from scratch and
                # overwriting the persisted cursor; if the cursor really is
                # stale, the operator can set runAll: true once to reset.
                logger.error(
                    f"400 error after {max_retries} retries with the existing cursor. "
                    "If the underlying data model was rebuilt, set runAll: true once "
                    f"to reset the cursor, then resume normal operation. Original error: {msg}"
                )
                raise Exception(msg) from e

            sleep_for = min(2 ** num_retry, 30)
            logger.warning(
                f"Got 400 error (attempt {num_retry}/{max_retries}); retrying with the same "
                f"cursor in {sleep_for}s: {msg}"
            )
            time.sleep(sleep_for)
            

    new_cursor_value = sync_result.cursors["files"]
    update_state_store(client, logger, new_cursor_value, None, config, STAT_STORE_CURSOR, None)

    logger.info(f"Num new files: {len(sync_result['files'])} from view: {files_view_id}")
    return sync_result

def get_file_filter(
    file_view_config: ViewPropertyConfig,
    debug_file: str,
    logger: CogniteFunctionLogger,
) -> dm.filters.Filter:

    is_view = dm.filters.HasData(views=[file_view_config.as_view_id()])
    is_uploaded = dm.filters.Equals(file_view_config.as_property_ref("isUploaded"), True)

    is_file_type = dm.filters.In(
        file_view_config.as_property_ref("mimeType"), ["application/pdf", "image/jpeg", "image/png", "image/tiff"]
    )
    is_selected = dm.filters.And(is_view, is_uploaded, is_file_type)
    dbg_msg = "File filter: isUploaded=True, mimeType IN [application/pdf, image/jpeg, image/png, image/tiff]"

    if debug_file:
        is_debug_file = dm.filters.Equals(file_view_config.as_property_ref("name"), debug_file)
        dbg_msg = f"{dbg_msg} filtering on file name: {debug_file}"
        is_selected = dm.filters.And(is_selected, is_debug_file)


    if file_view_config.filter_property and file_view_config.filter_values:
        is_filter_param = dm.filters.In(
            file_view_config.as_property_ref(
                file_view_config.filter_property),
                file_view_config.filter_values
        )
        is_selected = dm.filters.And(is_selected, is_filter_param)
        dbg_msg = f"{dbg_msg} File filtering on: '{file_view_config.filter_values}' IN: '{file_view_config.filter_property}'"

    logger.debug(dbg_msg)

    return is_selected


def get_entity_filter(
    view_config: ViewPropertyConfig,
    logger: CogniteFunctionLogger,
) -> dm.filters.Filter:

    is_view = dm.filters.HasData(views=[view_config.as_view_id()])
    is_selected = dm.filters.And(is_view)

    dbg_msg = f"For view: {view_config.as_view_id()} - Entity filter: HasData = True"


    if view_config.filter_property and view_config.filter_values:
        is_filter_param = dm.filters.In(
            view_config.as_property_ref(
                view_config.filter_property),
                view_config.filter_values
        )
        is_selected = dm.filters.And(is_selected, is_filter_param)
        dbg_msg = f"{dbg_msg} Entity filtering on: '{view_config.filter_values}' IN: '{view_config.filter_property}'"

    logger.debug(dbg_msg)

    return is_selected




def run_diagram_detect(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    entities: list[dict[str, Any]],
    file_ids: list[NodeId],
    search_property: str,
) -> DiagramDetectResults:
    """
    Run diagram detect job on a batch of documents.

    Retries transient failures with bounded exponential backoff. If retries are
    exhausted on a multi-file batch, raises a generic Exception so the caller
    can drop to single-file mode. If retries are exhausted on a single file,
    raises DiagramDetectError so the caller can skip the file.
    """
    logger.info(
        f"Run diagram detect on {len(file_ids)} files, num entities: {len(entities)}, "
        f"partial match: True, search field: {search_property}"
    )

    max_retries = 3
    num_retry = 0

    while True:
        try:
            job = client.diagrams.detect(
                file_instance_ids=file_ids,
                entities=entities,
                partial_match=True,
                search_field=search_property,
                configuration=DiagramDetectConfig(read_embedded_text=True),
            )
            logger.debug("Diagram detect job started...   waiting for job to finish")
            return job.result
        except Exception as e:
            num_retry += 1
            if num_retry > max_retries:
                if len(file_ids) > 1:
                    raise Exception(
                        f"Batch diagram detect failed on {len(file_ids)} files after "
                        f"{max_retries} retries - falling back to one file at a time"
                    ) from e
                raise DiagramDetectError(
                    f"Diagram detect job failed for {file_ids} after {max_retries} retries: "
                    f"{e!s} - skipping file"
                ) from e

            sleep_for = min(2 ** num_retry, 30)
            logger.warning(
                f"Diagram detect attempt {num_retry}/{max_retries} failed: {e!s}; "
                f"retrying in {sleep_for}s"
            )
            time.sleep(sleep_for)



def push_result_to_annotations(
    client: CogniteClient,
    config: Config,
    logger: CogniteFunctionLogger,
    annotation_view_id: ViewId,
    files_view_id: ViewId,
    result: DiagramDetectResults,
    new_files: dict[str, Any],
    error_count: int,
    doc_doc: list[dict],
    doc_tag: list[dict],
) -> int:

    edge_applies: list[EdgeApply] = []
    files_to_clean: list[NodeId] = []
    logger.debug(f"Pushing annotations to data model, number of items: {len(result['items'])}")

    # Build a (space, external_id) -> sourceId index once per call instead of
    # rescanning new_files["files"] for every annotation (M4).
    source_id_by_node: dict[tuple[str, str], str | None] = {}
    for file_node in new_files["files"]:
        node_props = file_node.properties.get(files_view_id, {})
        source_id_by_node[(file_node.space, file_node.external_id)] = node_props.get("sourceId")

    for result_item in result["items"]:

        file_instance_id_dict = result_item.get("fileInstanceId")
        if file_instance_id_dict is None:
            error_count += 1
            logger.error(f"File instance id not found in result item: {result_item.get('fileId')}")
            continue

        logger.debug(f"File instance id: {file_instance_id_dict}")
        file_instance_id = NodeId.load(file_instance_id_dict)

        if config.parameters.clean_old_annotations:
            files_to_clean.append(file_instance_id)

        source_id = source_id_by_node.get((file_instance_id.space, file_instance_id.external_id))

        edge_apply, error_count = _result_item_to_edge_applies(
            config,
            logger,
            annotation_view_id,
            result_item,
            file_instance_id,
            source_id,
            error_count,
            doc_doc,
            doc_tag,
        )
        edge_applies.extend(edge_apply)
        logger.debug(
            f"Number of annotations for file: "
            f"{result_item['fileInstanceId']['externalId']} to apply: {len(edge_apply)}"
        )

    # Clean up old annotations for the whole page in a single batched call (M6),
    # before applying the new ones so we don't accidentally delete fresh edges.
    if config.parameters.clean_old_annotations and files_to_clean:
        delete_annotations_for_files(client, logger, annotation_view_id, files_to_clean)

    max_retries = 3
    num_retry = 0
    while True:
        try:
            client.data_modeling.instances.apply(edge_applies)
            logger.debug(f"Total number of annotations added/updated: {len(edge_applies)}")
            return error_count
        except Exception as e:
            num_retry += 1
            if num_retry > max_retries:
                msg = f"Annotations add/update of: {len(edge_applies)} failed, error: {e!s}"
                logger.error(msg)
                raise Exception(msg) from e
            sleep_for = min(2 ** num_retry, 30)
            logger.warning(
                f"Apply attempt {num_retry}/{max_retries} failed: {e!s}; retrying in {sleep_for}s"
            )
            time.sleep(sleep_for)


def _result_item_to_edge_applies(
    config: Config,
    logger: CogniteFunctionLogger,
    annotation_view_id: ViewId,
    result_item: dict[str, Any],
    file_instance_id: NodeId,
    source_id: str,
    error_count: int,
    doc_doc: list[dict],
    doc_tag: list[dict],
) -> tuple[list[EdgeApply], int]:

    edge_annotations = []

    if "annotations" in result_item:
        for detect_annotation in result_item["annotations"]:
            edge_apply, error_count = _detect_annotation_to_edge_applies(
                config,
                logger,
                error_count,
                doc_doc,
                doc_tag,
                annotation_view_id,
                file_instance_id,
                source_id,
                detect_annotation
            )
            edge_annotations.extend(edge_apply)

        return edge_annotations, error_count
    else:
        return [], error_count

def _detect_annotation_to_edge_applies(
    config: Config,
    logger: CogniteFunctionLogger,
    error_count: int,
    doc_doc: list[dict],
    doc_tag: list[dict],
    annotation_view_id: ViewId,
    file_instance_id: NodeId,
    source_id: str,
    detect_annotation: dict[str, Any],
) -> tuple[list[EdgeApply], int]:

    diagram_annotations = []
    file_instance_space=config.data.annotation_job.file_view.instance_space
    annotation_schema_space=config.data.annotation_view.schema_space
    approve_threshold=config.parameters.auto_approval_threshold
    suggest_threshold=config.parameters.auto_suggest_threshold
    try:
        #logger.debug(f"Detected annotation: {detect_annotation}")
        for entity in detect_annotation["entities"]:
            if detect_annotation["confidence"] >= approve_threshold:
                annotation_status = DiagramAnnotationStatus.APPROVED.value
            elif detect_annotation["confidence"] >= suggest_threshold:
                annotation_status = DiagramAnnotationStatus.SUGGESTED.value
            else:
                continue

            external_id=create_annotation_id(file_instance_id, entity, detect_annotation["text"], detect_annotation)

            # start collection properties to log to RAW
            doc_log = {
                "external_id": external_id,
                "start_source_id": source_id,
                "start_node": file_instance_id.external_id,
                "end_node": entity["external_id"],
                "end_node_space":entity["space"],
                "view_id":annotation_view_id.external_id,
                "view_space":annotation_view_id.space,
                "view_version":annotation_view_id.version,
            }
            now = datetime.now(UTC).replace(microsecond=0)

            annotation_properties = {
                "name": file_instance_id.external_id,
                "confidence": detect_annotation["confidence"],
                "status": annotation_status,
                "startNodePageNumber": detect_annotation["region"]["page"],
                "startNodeXMin": min(v["x"] for v in detect_annotation["region"]["vertices"]),
                "startNodeYMin": min(v["y"] for v in detect_annotation["region"]["vertices"]),
                "startNodeXMax": max(v["x"] for v in detect_annotation["region"]["vertices"]),
                "startNodeYMax": max(v["y"] for v in detect_annotation["region"]["vertices"]),
                "startNodeText": detect_annotation["text"],
                "sourceCreatedUser":FUNCTION_ID,
                "sourceUpdatedUser":FUNCTION_ID,
            }

            doc_log.update(annotation_properties)
            annotation_properties["sourceCreatedTime"] = now.isoformat()
            annotation_properties["sourceUpdatedTime"] = now.isoformat()

            diagram_annotations.append(
                EdgeApply(
                    space=file_instance_space,
                    external_id=external_id,
                    type=DirectRelationReference(
                        space=annotation_schema_space,
                        external_id=entity["annotation_type_external_id"]
                    ),
                    start_node=DirectRelationReference(
                        space=file_instance_id.space,
                        external_id=file_instance_id.external_id
                    ),
                    end_node=DirectRelationReference(
                        space=entity["space"],
                        external_id=entity["external_id"]
                    ),
                    sources=[
                        NodeOrEdgeData(
                            source=annotation_view_id,
                            properties=annotation_properties,
                        )
                    ],
                )
            )

            if entity["annotation_type_external_id"] == FILE_LINK_EXTERNAL_ID:
                doc_doc.append(doc_log)
            else:
                doc_tag.append(doc_log)

        return diagram_annotations, error_count
    except (KeyError, TypeError, ValueError) as e:
        # These are the parse-shape errors we expect from a malformed detect
        # response: missing dict keys, non-numeric coordinates, bad types in
        # entity payloads. Bugs (NameError, ImportError, etc.) propagate so
        # they surface in CI/log review rather than being silently absorbed
        # as a single error_count increment.
        error_count += 1
        logger.error(
            f"Failed to create annotation for file: {file_instance_id.external_id} error: {e!s}"
        )
        return [], error_count

def create_annotation_id(
    file_id: dm.NodeId,
    entity: dict[str, Any],
    text: str,
    raw_annotation: dict[str, Any]
) -> str:
    hash_ = sha256(json.dumps(raw_annotation, sort_keys=True).encode()).hexdigest()[:10]
    naive = f"{file_id.space}:{file_id.external_id}:{entity['space']}:{entity['external_id']}:{text}:{hash_}"
    if len(naive) < EXTERNAL_ID_LIMIT:
        return naive

    prefix = f"{file_id.external_id}:{entity['external_id']}:{text}"
    short_id = f"{prefix}:{hash_}"
    if len(short_id) < EXTERNAL_ID_LIMIT:
        return short_id

    return prefix[: EXTERNAL_ID_LIMIT - 10] + hash_

def write_mapping_to_raw(
    client: CogniteClient,
    config: Config,
    raw_uploader: RawUploadQueue,
    doc_doc: list[dict],
    doc_tag: list[dict],
    logger: CogniteFunctionLogger,
) -> None:
    """
    Write matching results to RAW DB.

    The destination tables are expected to exist already; callers should
    ensure them via a one-time `create_table` call at pipeline start (M3).

    Args:
        config: Instance of ContextConfig
        raw_uploader : Instance of RawUploadQueue
        doc_doc: list of doc doc matches
        doc_tag: list of doc tag matches
        logger: Instance of CogniteFunctionLogger
    """
    raw_db = config.parameters.raw_db
    tag_tbl = config.parameters.raw_table_doc_tag
    doc_tbl = config.parameters.raw_table_doc_doc

    for tag in doc_tag:
        raw_uploader.add_to_upload_queue(raw_db, tag_tbl, Row(str(tag['external_id']), tag))

    logger.info(f"Added {len(doc_tag)} rows to {raw_db}/{tag_tbl}")

    for doc in doc_doc:
        raw_uploader.add_to_upload_queue(raw_db, doc_tbl, Row(str(doc['external_id']), doc))

    logger.info(f"Added {len(doc_doc)} rows to {raw_db}/{doc_tbl}")

    # Upload any remaining RAW rows in queue
    raw_uploader.upload()


def create_table(client: CogniteClient, raw_db: str, tbl: str) -> None:
    try:
        client.raw.databases.create(raw_db)
    except Exception:
        # Database may already exist when the pipeline is re-run.
        # Expected failure; continue without affecting the caller.
        pass

    try:
        client.raw.tables.create(raw_db, tbl)
    except Exception:
        # Table may already exist when the pipeline is re-run.
        # Expected failure; continue without affecting the caller.
        pass

def delete_table(client: CogniteClient, db: str, tbl: str) -> None:
    try:
        client.raw.tables.delete(db, [tbl])
    except CogniteAPIError as e:
        # Any other error than table not found, and we re-raise
        if e.code != 404:
            raise


def get_property(
    view_id: ViewId,
    property: str
) -> list[str]:

    return [view_id.space, f"{view_id.external_id}/{view_id.version}", property]


def list_annotations_for_files(
    client: CogniteClient,
    annotation_view_id: ViewId,
    nodes: list[NodeId],
) -> list:
    """
    List all annotation edges for the given file nodes.

    The instances.list API takes a single `space`, so when the input nodes
    span multiple spaces we issue one list call per space. Typical pages
    are single-space, so this is one call.

    Args:
        client (CogniteClient): The Cognite client instance.
        annotation_view_id (ViewId): The ViewId of the annotation view.
        nodes (list[NodeId]): The file nodes whose annotations to list.

    Returns:
        list: A list of edges (annotations) linked to the input file nodes.
    """
    if not nodes:
        return []

    external_ids = [n.external_id for n in nodes]
    is_function = dm.filters.Equals(get_property(annotation_view_id, "sourceCreatedUser"), FUNCTION_ID)
    is_file = dm.filters.In(get_property(annotation_view_id, "name"), external_ids)
    is_selected = dm.filters.And(is_function, is_file)

    annotations: list = []
    for space in {n.space for n in nodes}:
        annotations.extend(
            client.data_modeling.instances.list(
                space=space,
                sources=[annotation_view_id],
                instance_type="edge",
                filter=is_selected,
                limit=-1,
            )
        )
    return annotations


def delete_annotations_for_files(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    annotation_view_id: ViewId,
    nodes: list[NodeId],
) -> None:
    """
    Delete all annotation edges for the given file nodes in a single batched
    list+delete pair (M6).

    Args:
        client (CogniteClient): The Cognite client instance.
        annotation_view_id (ViewId): The ViewId of the annotation view.
        nodes (list[NodeId]): The file nodes whose annotations to clean up.
    """
    if not nodes:
        return

    annotations = list_annotations_for_files(client, annotation_view_id, nodes)
    if not annotations:
        logger.debug(f"No old annotations to clean for {len(nodes)} files")
        return

    edge_ids = [EdgeId(space=edge.space, external_id=edge.external_id) for edge in annotations]
    client.data_modeling.instances.delete(edge_ids)
    logger.info(f"Deleted {len(edge_ids)} old annotations across {len(nodes)} files")
