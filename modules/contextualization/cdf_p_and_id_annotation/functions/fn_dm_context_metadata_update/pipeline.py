import sys
import traceback
from pathlib import Path
import re
from typing import Any


from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import (
    NodeApply,
    NodeOrEdgeData,
    ViewId,
    NodeList,
    Node,
)
from cognite.client.data_classes.filters import In, HasData, Equals
from cognite.client.utils._text import shorten
from cognite.client.data_classes import ExtractionPipelineRun, Row
from cognite.client import data_modeling as dm

from cognite.client.data_classes.data_modeling.query import (
    Query,
    NodeResultSetExpression,
    Select,
    SourceSelector,
)

from config import Config, ViewPropertyConfig
from logger import CogniteFunctionLogger

from constants import (
    STAT_STORE_VALUE,
    STAT_STORE_FILE_CURSOR,
    STAT_STORE_ASSET_CURSOR,
    BATCH_SIZE,
    FILE_NODE,
    ASSET_NODE,
)

sys.path.append(str(Path(__file__).parent))


def file_metadata_update(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    data: dict[str, Any],
    config: Config,
) -> None:
    """
    Main function for the alias update process. This function will read new files and assets from Cognite Data Fusion and create aliases for them in the CDM.

    Raises:
        Exception: If any error occurs during the alias update process, it logs the error message and traceback, and updates the pipeline run status to "failure".

    Args:
        client: An instantiated CogniteClient
        config: A dataclass containing the configuration for the annotation process

    """

    global BATCH_SIZE

    try:
        pipeline_ext_id = data["ExtractionPipelineExtId"]

        file_cursor = None
        asset_cursor = None

        if config.parameters.debug:
            logger = CogniteFunctionLogger("DEBUG")
            logger.debug(f"**** Write debug messages and only process one file *****")
            BATCH_SIZE = 100  # Minimum batch size for debug
        else:
            logger.debug("Get files that has been updated since last run")

            # Check if we should run all files (then delete state content in RAW) or just new files
            if not config.parameters.run_all:
                file_cursor = read_state_store(
                    client,
                    logger,
                    STAT_STORE_FILE_CURSOR,
                    config.parameters.raw_db,
                    config.parameters.raw_table_state,
                )
                asset_cursor = read_state_store(
                    client,
                    logger,
                    STAT_STORE_ASSET_CURSOR,
                    config.parameters.raw_db,
                    config.parameters.raw_table_state,
                )

        # Get files that has been updated since last run to create aliases
        files_view_id = config.data.job.file_view.as_view_id()
        new_files = get_new_items(
            client,
            logger,
            file_cursor,
            files_view_id,
            config,
            FILE_NODE,
            STAT_STORE_FILE_CURSOR,
        )

        if len(new_files[FILE_NODE]) > 0:
            while len(new_files[FILE_NODE]) > 0:
                num_file_aliases = update_metadata(
                    client,
                    logger,
                    new_files,
                    files_view_id,
                    config.data.job.file_view.instance_space,
                    FILE_NODE,
                )

                msg = f"[INFO]: Updating {num_file_aliases} aliases for {len(new_files[FILE_NODE])} files"
                update_pipeline_run(client, logger, pipeline_ext_id, "success", msg)

                if config.parameters.debug:
                    break

                file_cursor = new_files.cursors[FILE_NODE]

                # look for more files to process
                new_files = get_new_items(
                    client,
                    logger,
                    file_cursor,
                    files_view_id,
                    config,
                    FILE_NODE,
                    STAT_STORE_FILE_CURSOR,
                )

        else:
            msg = f"[INFO]: No new file to process"
            update_pipeline_run(client, logger, pipeline_ext_id, "success", msg)

        # Get assets that has been updated since last run to create aliases
        asset_view_id = config.data.job.asset_view.as_view_id()
        new_assets = get_new_items(
            client,
            logger,
            asset_cursor,
            asset_view_id,
            config,
            ASSET_NODE,
            STAT_STORE_ASSET_CURSOR,
        )
        if len(new_assets[ASSET_NODE]) > 0:
            while len(new_assets[ASSET_NODE]) > 0:
                num_asset_aliases = update_metadata(
                    client,
                    logger,
                    new_assets,
                    asset_view_id,
                    config.data.job.asset_view.instance_space,
                    ASSET_NODE,
                )

                msg = f"[INFO]: Updating {num_asset_aliases} aliases for {len(new_assets[ASSET_NODE])} assets"
                update_pipeline_run(client, logger, pipeline_ext_id, "success", msg)

                if config.parameters.debug:
                    break

                asset_cursor = new_assets.cursors[ASSET_NODE]

                # look for more assets to process
                new_assets = get_new_items(
                    client,
                    logger,
                    asset_cursor,
                    asset_view_id,
                    config,
                    ASSET_NODE,
                    STAT_STORE_ASSET_CURSOR,
                )

        else:
            msg = f"[INFO]: No new assets to process"
            update_pipeline_run(client, logger, pipeline_ext_id, "success", msg)

    except Exception as e:
        msg = f"Failed, Message: {e!s}, traceback:\n{traceback.format_exc()}"
        update_pipeline_run(client, logger, pipeline_ext_id, "failure", msg)


def update_pipeline_run(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    xid: str,
    status: str,
    msg: str = None,
) -> None:
    if status == "success":
        logger.info(msg)
    else:
        logger.error(msg)

    client.extraction_pipelines.runs.create(
        ExtractionPipelineRun(
            extpipe_external_id=xid, status=status, message=shorten(msg, 1000)
        )
    )


def read_state_store(
    client: CogniteClient, logger: CogniteFunctionLogger, key: str, db: str, table: str
) -> str:
    """
    Reads a value from a state store in a specified database and table.
    This function attempts to read a value associated with a given key from a specified
    database and table. If the database or table does not exist, it will create them.
    Args:
        client (CogniteClient): The Cognite client used to interact with the database.
        key (str): The key for which the value needs to be read.
        db (str): The name of the database.
        table (str): The name of the table.
    Returns:
        str: The value associated with the given key, or None if the key does not exist.
    """

    value = None

    logger.info(f"Read state from DB: {db} Table: {table} Key: {key}")

    # Create DB / Table for state if it does not exist
    try:
        client.raw.databases.create(db)
    except Exception:
        pass

    try:
        client.raw.tables.create(db, table)
    except Exception:
        pass

    row_list = client.raw.rows.list(
        db_name=db, table_name=table, columns=[STAT_STORE_VALUE], limit=-1
    )
    for row in row_list:
        if row.key == key:
            value = row.columns[STAT_STORE_VALUE]

    return value


def update_state_store(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    cursor: str,
    config: Config,
    cursor_name: str,
) -> None:
    """
    Updates the state store with the provided cursor value.

    Args:
        client (CogniteClient): The Cognite client used to interact with the Cognite Data Fusion.
        cursor (str): The cursor value to be stored.
        config (Config): Configuration object containing database and table information.
        cursor_name (str): The name of the cursor to be updated in the state store.

    Returns:
        None
    """
    logger.info(f"Updating state store for cursor: {cursor_name}")
    state_row = Row(cursor_name, {STAT_STORE_VALUE: cursor})
    client.raw.rows.insert(
        config.parameters.raw_db, config.parameters.raw_table_state, state_row
    )


def get_new_items(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    cursor: str,
    view_id: ViewId,
    config: Config,
    node_type: str,
    cursor_name: str,
) -> NodeList[Node]:
    """
    Read new files based on TAG PID

    :returns: Nodelist of files nodes
    """
    if node_type == FILE_NODE:
        view_config = config.data.job.file_view
        if config.parameters.debug:
            debug_file = config.parameters.debug_file
        else:
            debug_file = None
        is_selected = get_file_filter(view_config, debug_file, logger)
        property_list = ["name", "aliases", "tags", "description"]

    else:
        view_config = config.data.job.asset_view
        is_selected = get_asset_filter(view_config, logger)
        property_list = ["name", "aliases"]

    logger.debug(f"Get new items from view: {view_id}, based on config: {view_config}")

    sync_query = Query(
        with_={
            node_type: NodeResultSetExpression(filter=is_selected, limit=BATCH_SIZE),
        },
        select={
            node_type: Select([SourceSelector(view_id, property_list)]),
        },
    )

    num_retry = 0
    retry = True

    while retry:
        sync_query.cursors[node_type] = cursor
        try:
            sync_result = client.data_modeling.instances.sync(sync_query)
            retry = False
        except Exception as e:
            msg = f"failed, Message: {e!s}"
            if e.code == 400:
                logger.warning(
                    f"Got 400 error, Resetting cursor and trying again : {msg}"
                )
                cursor = None
                num_retry += 1
                if num_retry > 3:
                    retry = False
                    raise Exception(msg) from e
            else:
                retry = False
                raise Exception(msg) from e

    new_cursor_value = sync_result.cursors[node_type]
    # Update state store with new cursor - in case timeout on next loop to set water mark
    update_state_store(client, logger, new_cursor_value, config, cursor_name)

    logger.info(f"Num new items: {len(sync_result[node_type])} from view: {view_id}")
    return sync_result


def get_file_filter(
    file_view_config: ViewPropertyConfig,
    debug_file: str,
    logger: CogniteFunctionLogger,
) -> dm.filters.Filter:
    is_view = dm.filters.HasData(views=[file_view_config.as_view_id()])
    is_uploaded = dm.filters.Equals(
        file_view_config.as_property_ref("isUploaded"), True
    )

    is_file_type = dm.filters.In(
        file_view_config.as_property_ref("mimeType"),
        ["application/pdf", "image/jpeg", "image/png", "image/tiff"],
    )
    is_selected = dm.filters.And(is_view, is_uploaded, is_file_type)
    dbg_msg = f"File filter: hasData = True, isUploaded=True, mimeType IN [application/pdf, image/jpeg, image/png, image/tiff]"

    if debug_file:
        is_debug_file = dm.filters.Equals(
            file_view_config.as_property_ref("name"), debug_file
        )
        dbg_msg = f"{dbg_msg} filtering on file name: {debug_file}"
        is_selected = dm.filters.And(is_selected, is_debug_file)

    logger.debug(dbg_msg)

    return is_selected


def get_asset_filter(
    view_config: ViewPropertyConfig,
    logger: CogniteFunctionLogger,
) -> dm.filters.Filter:
    is_view = dm.filters.HasData(views=[view_config.as_view_id()])
    dbg_msg = f"Asset filter: hasData = True"
    is_selected = dm.filters.And(is_view)

    logger.debug(dbg_msg)

    return is_selected


def extract_tags_simple(text: str, tags: list[str]) -> list[str]:
    # Convert to lowercase and split by non-alphanumeric characters

    if text:
        use_text = re.split(r"[,.]", text, maxsplit=1)[0].strip()
        words = re.findall(r"\b\w+\b", use_text.lower())
        if "P&ID" in use_text:
            if "PID" not in tags:
                tags.append("PID")

        # Define a simple set of stopwords (expand this as needed)
        stopwords = {
            "apologize",
            "this",
            "appears",
            "document",
            "outlines",
            "cognite",
            "data",
            "fusion",
            "system",
            "industrial",
            "process",
        }

        # Filter out stopwords
        for word in words:
            if word not in stopwords and len(word) > 4 and word not in tags:
                tags.append(word)

    return tags


def update_metadata(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    new_nodes: NodeList[Node],
    view_id: ViewId,
    node_space: str,
    node_type: str,
) -> int:
    """
    Create and update aliases for a list of new nodes in a data model.
    Args:
        client (CogniteClient): The Cognite client used to interact with the data model.
        config (Config): Configuration object containing settings for alias creation.
        new_nodes (NodeList[Node]): List of new nodes to create aliases for.
        view_id (ViewId): The view ID used to access node properties.
        node_space (str): The space in which the nodes exist.
        node_type (str): The type of nodes being processed (e.g., "file" or "asset").
    Returns:
        int : The number of aliases created or updated.
    """

    node_num = 0
    item_update = []
    tags = []
    node_ids = new_nodes[node_type].as_ids()
    for num in range(node_num, len(node_ids), 1):
        aliases = []
        description = None
        tags = []
        update_metadata = False
        ext_id = new_nodes[node_type][num].external_id
        name = new_nodes[node_type][num].properties[view_id]["name"]
        if "description" in new_nodes[node_type][num].properties[view_id]:
            description = new_nodes[node_type][num].properties[view_id]["description"]
        if "aliases" in new_nodes[node_type][num].properties[view_id]:
            aliases = new_nodes[node_type][num].properties[view_id]["aliases"]
        if "tags" in new_nodes[node_type][num].properties[view_id]:
            tags = new_nodes[node_type][num].properties[view_id]["tags"]

        if node_type == FILE_NODE:
            if description in [None, ""] or description.find("I apologize") > -1:
                summary = get_doc_summary(client, logger, ext_id, node_space)
                if (
                    summary and summary.find("I apologize") < 0
                ):  # test if we found a summary with any value
                    tags = extract_tags_simple(summary, tags)
                    update_metadata = True
                else:
                    summary = ""
            else:
                summary = description

            aliases_upd = get_file_alias_list(name, aliases)
            properties_dict = {
                "name": name,
                "aliases": aliases_upd,
                "tags": tags,
                "description": summary,
            }
            dbg_msg = f"Updating file : {ext_id} with aliases: {aliases_upd} - tags : {tags} - summary: {summary}"
        else:
            aliases_upd = get_asset_alias_list(name, aliases)
            properties_dict = {"aliases": aliases_upd}
            dbg_msg = f"Updating asset : {ext_id} with aliases: {aliases_upd}"

        if aliases_upd != aliases:
            update_metadata = True

        if update_metadata:
            item_update.append(
                NodeApply(
                    space=node_space,
                    external_id=ext_id,
                    sources=[
                        NodeOrEdgeData(
                            source=view_id,
                            properties=properties_dict,
                        )
                    ],
                )
            )
            logger.debug(dbg_msg)

    client.data_modeling.instances.apply(item_update)
    return len(item_update)


def get_doc_summary(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    ext_id: str,
    node_space: str,
) -> str:
    endpoint = f"/api/v1/projects/{client._config.project}/ai/tools/documents/summarize"

    payload = {
        "ignoreUnknownIds": False,
        "items": [{"instanceId": {"space": node_space, "externalId": ext_id}}],
    }

    try:
        response = client.post(url=endpoint, json=payload)

    except Exception as e:
        msg = f"Failed to get summary for file externalId: {ext_id}, Message: {e!s}"
        logger.warning(msg)
        return None

    if response.status_code != 200:
        return None
    elif response.json().get("items") is None:
        return None
    elif response.json().get("items")[0]["summary"] is None:
        return None

    return response.json().get("items")[0]["summary"]


def get_file_alias_list(name: str, aliases: list[str]) -> list[str]:
    """
    Generate a list of file aliases based on the provided file name.

    Args:
        name (str): The original file name.
        directory (str): The directory path of the file.
    Returns:
        list[str]: A list of aliases including the original file name and the generated aliases.
    """
    aliases_upd = []
    aliases_upd.extend(aliases)

    alias_1 = name.split(".")[:-1][0]  # Remove last part of file name
    if alias_1 not in aliases_upd:
        aliases_upd.append(alias_1)

    alias_2 = name[: name.rfind("-")]  # Remove last part of file name
    if alias_2 not in aliases_upd:
        aliases_upd.append(alias_2)

    return aliases_upd


def get_asset_alias_list(name: str, aliases: list[str]) -> list[str]:
    """
    Generate a list of asset aliases based on the provided name and external ID.
    Args:
        name (str): The name of the asset.
        ext_id (str): The external ID of the asset.
    Returns:
        list[str]: An updated list of aliases including the name, external ID,
                   and a modified version of the name with the first part removed.
    """
    aliases_upd = []
    aliases_upd.extend(aliases)

    if name not in aliases_upd:
        aliases_upd.append(name)

    alias_1 = "-".join(name.split("-")[1:])  # Remove first part of asset name

    if alias_1 not in aliases_upd and alias_1 != "":
        aliases_upd.append(alias_1)

    return aliases_upd
