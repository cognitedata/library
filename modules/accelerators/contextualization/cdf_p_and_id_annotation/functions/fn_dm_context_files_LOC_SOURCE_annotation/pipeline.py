from enum import Enum

from cognite.client.data_classes.data_modeling import (
    DirectRelationReference,
    EdgeApply,
    EdgeId,
    NodeId,
    NodeOrEdgeData,
    ViewId,
    NodeList,
    Node,
)
from cognite.client.data_classes.filters import In, Or

import sys
import json
from hashlib import sha256
from datetime import datetime, timezone
import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes import (
    ExtractionPipelineRun,
)
from cognite.client.data_classes.contextualization import DiagramDetectResults, DiagramDetectConfig
from cognite.client import data_modeling as dm
from cognite.client.utils._auxiliary import split_into_chunks
from cognite.client.utils._text import shorten
from cognite.extractorutils.uploader import RawUploadQueue
from cognite.client.data_classes import Row
from cognite.client.exceptions import CogniteAPIError

from cognite.client.data_classes.data_modeling.query import Query, NodeResultSetExpression, Select, SourceSelector

from config import Config, ViewPropertyConfig
from logger import CogniteFunctionLogger

from constants import (
    FILE_LINK_EXTERNAL_ID,
    STAT_STORE_CURSOR,
    STAT_STORE_NUM_IN_BATCH,
    STAT_STORE_VALUE,
    BATCH_SIZE,
    ANNOTATE_BATCH_SIZE,
    EXTERNAL_ID_LIMIT,
    FUNCTION_ID
)

sys.path.append(str(Path(__file__).parent))


@dataclass
class DiagramAnnotationStatus(Enum):
    SUGGESTED = "Suggested"
    APPROVED = "Approved"

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
    try:
        global ANNOTATE_BATCH_SIZE
        pipeline_ext_id = data["ExtractionPipelineExtId"]

        error_count, annotated_count = 0, 0
        file_cursor = None
        file_num = 0
        if config.parameters.debug:
            logger = CogniteFunctionLogger("DEBUG")
            logger.debug(f"**** Write debug messages and only process one file *****")
            ANNOTATE_BATCH_SIZE = 1

        logger.debug(f"Initiate RAW upload queue used to store output from Diagram parsing")
        raw_uploader = RawUploadQueue(cdf_client=client, max_queue_size=500000, trigger_log_level="INFO")

        # Check if we should run all files (then delete state content in RAW) or just new files
        if config.parameters.run_all:
            logger.debug(f"Run all files, delete state content in RAW since we are rerunning based on all input")
            delete_table(client, config.parameters.raw_db, config.parameters.raw_table_doc_tag)
            delete_table(client, config.parameters.raw_db, config.parameters.raw_table_doc_doc)
            delete_table(client, config.parameters.raw_db, config.parameters.raw_table_state)
        elif config.parameters.debug_file and config.parameters.debug:
            logger.debug(f"Since debugging with one file name: {config.parameters.debug_file} - ignore cursor and batch number")
        else:
            logger.debug(f"Get file cursor and batch number from RAW, to continue processing from last run")
            file_cursor = read_state_store(client, logger, STAT_STORE_CURSOR, config.parameters.raw_db, config.parameters.raw_table_state)
            file_num = read_state_store(client, logger, STAT_STORE_NUM_IN_BATCH, config.parameters.raw_db, config.parameters.raw_table_state)

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

        original_batch_size = ANNOTATE_BATCH_SIZE
        while len(new_files["files"]) > 0:
            doc_doc = []
            doc_tag = []
            annotation_view_id = config.data.annotation_view.as_view_id()
            search_property = config.data.annotation_job.file_view.search_property
            file_ids = new_files["files"].as_ids()
            try:
                for num in range(file_num, len(file_ids), ANNOTATE_BATCH_SIZE):
                    result = run_diagram_detect(client, logger, entities, file_ids[num:num+ANNOTATE_BATCH_SIZE], search_property)
                    if result is None:
                        error_count += len(file_ids)
                    else:
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
                        annotated_count += len(file_ids) - error_count

                        # Update raw with new annotations
                        write_mapping_to_raw(client, config, raw_uploader, doc_doc, doc_tag, logger)

                    logger.debug("Update state store with doc num in batch - in case timeout to set water mark")
                    update_state_store(client, logger, file_cursor, num+ANNOTATE_BATCH_SIZE, config, None, STAT_STORE_NUM_IN_BATCH)

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
                error_count += ANNOTATE_BATCH_SIZE
                msg = f"Skipping file - diagram detect failed with error: {e!s}"
                logger.error(msg)
                file_num += ANNOTATE_BATCH_SIZE
                ANNOTATE_BATCH_SIZE = original_batch_size
                pass
            
            except Exception as e:
                if ANNOTATE_BATCH_SIZE > 1:
                    msg = f"Failed to push: {ANNOTATE_BATCH_SIZE} annotations to data model, setting Batch Size = 1 and retry error: {e!s}"
                    logger.error(msg)
                    ANNOTATE_BATCH_SIZE = 1
                    pass
                else:
                    error_count += ANNOTATE_BATCH_SIZE
                    msg = f"Failed to push: {ANNOTATE_BATCH_SIZE} annotations to data model, error: {e!s}"
                    logger.error(msg)
                    raise Exception(msg) from e

    except Exception as e:
        msg = f"failed, Message: {e!s}"
        update_pipeline_run(client, logger, pipeline_ext_id, "failure", annotated_count, error_count, msg)
        raise Exception("msg")



def update_pipeline_run(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    xid: str,
    status: str,
    annotated_count: int,
    error_count: int,
    error: str = None
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
            message=shorten(msg, 1000)
        )
    )

def read_state_store(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    key: str,
    db: str,
    table: str
) -> str:

    if key == STAT_STORE_NUM_IN_BATCH:
        value = 0
    else:
        value = None

    logger.info(f"Read state from DB: {db} Table: {table} Key: {key}")

    logger.debug("Create DB / Table for state if it does not exist")
    create_table(client, db, table)

    row_list = client.raw.rows.list(db_name=db, table_name=table, columns=[STAT_STORE_VALUE], limit=-1)
    for row in row_list:
        if row.key == key:
            value = row.columns[STAT_STORE_VALUE]
    if value == 0 or value == None:
        logger.debug(f"State not found for key: {key} -> read all values")

    return value

def update_state_store(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    file_cursor: str,
    file_num: int,
    config: Config,
    cursor: str,
    batch_num: str
) -> None:

    state_row = None

    # Create DB / Table for state if it does not exist
    create_table(client, config.parameters.raw_db, config.parameters.raw_table_state)

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

    :returns: Nodelist of entities nodes
    """
    entities = []
    job_config = config.data.annotation_job
    all_files = get_all_files(client, logger, job_config.file_view)
    search_property = job_config.file_view.search_property

    for file in all_files:
        entities.append(
        {
            "external_id": file.external_id,
            "name": file.properties[job_config.file_view.as_view_id()]["name"],
            "space": file.space,
            search_property: file.properties[job_config.file_view.as_view_id()][search_property],
            "annotation_type_external_id": job_config.file_view.type,
        }
    )
    logger.debug(f"Number files added as entities: {len(entities)}")

    for entity_view in job_config.entity_views:

        type = entity_view.type
        search_property = entity_view.search_property
        view_id = entity_view.as_view_id()
        logger.debug(f"Get all entities from view: {view_id} with search property: {search_property} and type: {type}")

        is_selected = get_entity_filter(entity_view, logger)

        entity_list = client.data_modeling.instances.list(
            space=entity_view.instance_space,
            sources=[entity_view.as_view_id()], 
            filter=is_selected,
            limit=-1
        )

        warningLogged = False
        for entity in entity_list:
            if search_property in entity.properties[view_id]:
                if not warningLogged:
                    logger.debug(f"View {view_id} contains {search_property} property")
                    warningLogged = True

                entities.append(
                    {
                        "external_id": entity.external_id,
                        "name": entity.properties[view_id]["name"],
                        "space": entity.space,
                        search_property: entity.properties[view_id][search_property],
                        "annotation_type_external_id": type,
                    }
                )
            else:
                if not warningLogged:
                    logger.warning(f"View {view_id} don't contains {search_property} property, using name instead")
                    warningLogged = True
                entities.append(
                    {
                        "external_id": entity.external_id,
                        "name": entity.properties[view_id]["name"],
                        "space": entity.space,
                        search_property: entity.properties[view_id]["name"],
                        "annotation_type_external_id": type,
                    }
                )
        logger.info(f"Total number of entities: {len(entities)} including elements from view: {view_id} and type: {type}")


    return entities



def get_all_files(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    file_view_config: ViewPropertyConfig,
) -> NodeList[Node]:
    """
    Read files based on tag PID

    :returns: Nodelist of files nodes
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
    Read new files based on TAG PID

    :returns: Nodelist of files nodes
    """

    file_view_config= config.data.annotation_job.file_view
    if config.parameters.debug:
        debug_file = config.parameters.debug_file
    else:
        debug_file = None

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

    while retry:
        sync_query.cursors["files"] = file_cursor
        try:
            sync_result = client.data_modeling.instances.sync(sync_query)
            retry = False
        except Exception as e:
            msg = f"failed, Message: {e!s}"
            if e.code == 400:
                logger.warning(f"Got 400 error, Resetting cursor and trying again : {msg}")
                file_cursor = None
                num_retry += 1
                if num_retry > 3:
                    retry = False
                    raise Exception(msg) from e
            else:
                retry = False
                raise Exception(msg) from e
            

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
    dbg_msg = f"File filter: isUploaded=True, mimeType IN [application/pdf, image/jpeg, image/png, image/tiff]"

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

    dbg_msg = f"For for view: {view_config.as_view_id()} - Entity filter: HasData = True"


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
    entities: list[dict[str, any]],
    file_ids: list[NodeId],
    search_property: str,
) -> DiagramDetectResults:
    """
    Run diagram detect job
    on bach of documents
    """
    logger.info(f"Run diagram detect on {len(file_ids)} files, num entities: {len(entities)}, partial match: True, search field: {search_property}")

    num_retry = 0
    retry = True

    while retry:
        try:

            job = client.diagrams.detect(
                file_instance_ids=file_ids,
                entities=entities,
                partial_match=True,
                search_field=search_property,
                configuration=DiagramDetectConfig(read_embedded_text=True)
            )
            logger.debug("Diagram detect job started...   waiting for job to finish")
            return job.result
        except Exception as e:
            num_retry += 1
            if num_retry > 3:
                retry = False
                if len(file_ids) > 1:
                    raise Exception(f"Batch diagram detect batch failed on {len(file_ids)} files - rerunning with one file at a time")
                else:             
                    raise DiagramDetectError(f"Diagram detect job failed for {file_ids}, error: {e} - skipping file")



def push_result_to_annotations(
    client: CogniteClient,
    config: Config,
    logger: CogniteFunctionLogger,
    annotation_view_id: ViewId,
    files_view_id: ViewId,
    result: DiagramDetectResults,
    new_files: dict[str, any],
    error_count: int,
    doc_doc: list[dict],
    doc_tag: list[dict]
) -> None:

    edge_applies = []
    logger.debug(f"Pushing annotations to data model, number of items: {len(result['items'])}")

    for result_item in result["items"]:

        file_instance_id_dict = result_item.get("fileInstanceId")
        if file_instance_id_dict is not None:
            logger.debug(f"File instance id: {file_instance_id_dict}")
            file_instance_id = NodeId.load(file_instance_id_dict)

            if config.parameters.clean_old_annotations:
                delete_annotations_for_file(client, logger, annotation_view_id, file_instance_id)

            source_id = get_file_source_id(new_files, file_instance_id, files_view_id)

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
            logger.info(f"Number of annotations for file: {result_item['fileInstanceId']['externalId']} to apply: {len(edge_apply)}")
        else:
            error_count += 1
            logger.error(f"File instance id not found in result item: {result_item['fileId']}")
            return [], error_count

    num_retry = 0
    retry = True

    while retry:
        try:
            client.data_modeling.instances.apply(edge_applies)
            logger.debug(f"Total number of annotations added/updated: {len(edge_applies)}")
            return error_count
        except Exception as e:
            num_retry += 1
            if num_retry > 3:
                retry = False
                msg = f"Annotations add/update of: {len(edge_applies)} failed, error: {e.message}"
                logger.error(msg)
                raise Exception(msg) from e


def _result_item_to_edge_applies(
    config: Config,
    logger: CogniteFunctionLogger,
    annotation_view_id: ViewId,
    result_item: dict[str, any],
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
    detect_annotation: dict[str, any],
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
            now = datetime.now(timezone.utc).replace(microsecond=0)

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
    except Exception as e:
        error_count += 1
        logger.error(f"Failed to create annotation for file: {file_instance_id.external_id} error: {e}")
        return [],error_count

def create_annotation_id(
    file_id: dm.NodeId,
    entity: dict[str, any],
    text: str,
    raw_annotation: dict[str, Any]
) -> str:
    hash_ = sha256(json.dumps(raw_annotation, sort_keys=True).encode()).hexdigest()[:10]
    naive = f"{file_id.space}:{file_id.external_id}:{entity['space']}:{entity['external_id']}:{text}:{hash_}"
    if len(naive) < EXTERNAL_ID_LIMIT:
        return naive

    prefix = f"{file_id.external_id}:{entity['external_id']}:{text}"
    shorten = f"{prefix}:{hash_}"
    if len(shorten) < EXTERNAL_ID_LIMIT:
        return shorten

    return prefix[: EXTERNAL_ID_LIMIT - 10] + hash_

def get_file_source_id(
    new_files: dict[str, any],
    file_instance_id: NodeId,
    files_view_id: ViewId
) -> str:
    file_source_id = None

    # Find the matching file and extract the sourceId
    for file_node in new_files["files"]:
        if file_node.external_id == file_instance_id.external_id and \
           file_node.space == file_instance_id.space:
            if "sourceId" in file_node.properties[files_view_id]:
                file_source_id = file_node.properties[files_view_id]["sourceId"]
            break

    return file_source_id


def write_mapping_to_raw(
    client: CogniteClient,
    config: Config,
    raw_uploader: RawUploadQueue,
    doc_doc: list[dict],
    doc_tag: list[dict],
    logger: CogniteFunctionLogger
) -> None:
    """
    Write matching results to RAW DB

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

    logger.debug("Create DB / Table for DB: {raw_db}  Tables: {doc_tag} and {doc_doc} if it does not exist")
    create_table(client, raw_db, tag_tbl)
    create_table(client, raw_db, doc_tbl)

    for tag in doc_tag:
        raw_uploader.add_to_upload_queue(raw_db, tag_tbl, Row(str(tag['external_id']), tag))

    logger.info(f"Added {len(doc_tag)} rows to {raw_db}/{tag_tbl}")

    for doc in doc_doc:
        raw_uploader.add_to_upload_queue(raw_db, doc_tbl, Row(str(doc['external_id']), doc))

    logger.info(f"Added {len(doc_doc)} rows to {raw_db}/{doc_tbl}")

    # Upload any remaining RAW cols in queue
    raw_uploader.upload()


def create_table(client: CogniteClient, raw_db: str, tbl: str) -> None:
    try:
        client.raw.databases.create(raw_db)
    except Exception:
        pass

    try:
        client.raw.tables.create(raw_db, tbl)
    except Exception:
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


def list_annotations_for_file(
    client: CogniteClient,
    annotation_view_id: ViewId,
    node: NodeId
) -> list:
    """
    List all annotation edges for a file node.

    Args:
        client (CogniteClient): The Cognite client instance.
        annotation_view_id (ViewId): The ViewId of the annotation view.
        node (NodeId): The NodeId of the file node.

    Returns:
        list: A list of edges (annotations) linked to the file node.
    """

    is_function = dm.filters.Equals(get_property(annotation_view_id, "sourceCreatedUser"), FUNCTION_ID)
    is_file = dm.filters.Equals(get_property(annotation_view_id, "name"), node.external_id)
    is_selected = dm.filters.And(is_function, is_file)

    # Query for edges (annotations) connected to the file node
    annotations = client.data_modeling.instances.list(
        space=node.space,
        sources=[annotation_view_id],
        instance_type="edge",
        filter=is_selected,
        limit=-1,
    )
    return annotations


def delete_annotations_for_file(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    annotation_view_id: ViewId,
    node: NodeId
) -> None:
    """
    Delete all annotation edges for a file node.

    Args:
        client (CogniteClient): The Cognite client instance.
        annotation_view_id (ViewId): The ViewId of the annotation view.
        node (NodeId): The NodeId of the file node.
    """
    # List annotations for the file node
    annotations = list_annotations_for_file(client, annotation_view_id, node)
    if not annotations:
        logger.debug(f"No annotations found for file with NodeId: {node}")
        return

    # Extract edge IDs for deletion
    edge_ids = [EdgeId(space=node.space, external_id=edge.external_id) for edge in annotations]

    # Delete edges
    client.data_modeling.instances.delete(edge_ids)
    logger.info(f"Deleted {len(edge_ids)} annotations for file with NodeId: {node}")
