"""
AI Property Extractor - Cognite Function Handler

This function extracts structured property values from unstructured text fields
in data modeling instances using LLM agents.
"""

import os
import sys
import time
import traceback
from pathlib import Path
from typing import Literal

from cognite.client import CogniteClient, ClientConfig
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import ExtractionPipelineRunWrite

# Add current directory to path for local imports
sys.path.append(str(Path(__file__).parent))

from config import load_config, Config, WriteMode
from extractor import LLMPropertyExtractor
from state_store import StateStoreHandler


FUNCTION_ID = "ai_property_extractor"
EXTRACTION_RUN_MESSAGE_LIMIT = 1000
DEFAULT_EXTRACTION_PIPELINE_EXT_ID = "ep_ai_property_extractor"
MAX_RUNTIME_SECONDS = 9 * 60  # 9 minutes


class CogniteFunctionLogger:
    """Logger for Cognite Functions using print statements."""
    
    def __init__(self, log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"):
        self.log_level = log_level.upper()

    def _print(self, prefix: str, message: str) -> None:
        if "\n" not in message:
            print(f"{prefix} {message}")
            return
        lines = message.split("\n")
        print(f"{prefix} {lines[0]}")
        prefix_len = len(prefix)
        for line in lines[1:]:
            print(f"{' ' * prefix_len} {line}")

    def debug(self, message: str) -> None:
        if self.log_level == "DEBUG":
            self._print("[DEBUG]", message)

    def info(self, message: str) -> None:
        if self.log_level in ("DEBUG", "INFO"):
            self._print("[INFO]", message)

    def warning(self, message: str) -> None:
        if self.log_level in ("DEBUG", "INFO", "WARNING"):
            self._print("[WARNING]", message)

    def error(self, message: str) -> None:
        self._print("[ERROR]", message)


def handle(data: dict, client: CogniteClient) -> dict:
    """
    Main entry point for the Cognite Function.
    
    Args:
        data: Function input data containing:
            - logLevel: Optional log level (DEBUG, INFO, WARNING, ERROR)
            - ExtractionPipelineExtId: External ID of the extraction pipeline with config
        client: Authenticated CogniteClient
    
    Returns:
        Dictionary with status and message
    """
    extraction_pipeline_ext_id = data.get("ExtractionPipelineExtId") or DEFAULT_EXTRACTION_PIPELINE_EXT_ID
    data["ExtractionPipelineExtId"] = extraction_pipeline_ext_id  # Ensure it's set for downstream use
    
    try:
        result = execute(data, client)
        status: Literal["failure", "success"] = "success"
        message = f"{FUNCTION_ID} executed successfully. Processed {result['processed']} instances, updated {result['updated']} instances."
        
    except Exception as e:
        tb = traceback.extract_tb(e.__traceback__)
        last_entry_this_file = next(
            (entry for entry in reversed(tb) if entry.filename == __file__), 
            None
        )
        suffix = ""
        if last_entry_this_file:
            suffix = f" in function {last_entry_this_file.name} on line {last_entry_this_file.lineno}: {last_entry_this_file.line}"

        status = "failure"
        prefix = f"ERROR {FUNCTION_ID}: "
        error_msg = f'"{e!s}"'
        message = prefix + error_msg + suffix
        
        if len(message) >= EXTRACTION_RUN_MESSAGE_LIMIT:
            error_msg = error_msg[: EXTRACTION_RUN_MESSAGE_LIMIT - len(prefix) - len(suffix) - 3 - 1]
            message = prefix + error_msg + '..."' + suffix

    # Write extraction pipeline run status
    if extraction_pipeline_ext_id:
        client.extraction_pipelines.runs.create(
            ExtractionPipelineRunWrite(
                extpipe_external_id=extraction_pipeline_ext_id, 
                status=status, 
                message=message
            )
        )
    
    return {"status": status, "message": message}


def execute(data: dict, client: CogniteClient) -> dict:
    """
    Execute the property extraction pipeline.
    
    Runs in a loop until either:
    - 9 minutes have elapsed (to stay within function timeout limits)
    - No more instances need processing
    
    Supports:
    - State store for incremental processing via cursor
    - Per-property write modes (add_new_only, append, overwrite)
    - Force reset via function parameter
    
    Args:
        data: Function input data with optional keys:
            - resetState: bool - Reset cursor and re-process all instances
            - logLevel: str - Log level (DEBUG, INFO, WARNING, ERROR)
        client: Authenticated CogniteClient
    
    Returns:
        Dictionary with processed and updated counts
    """
    start_time = time.time()
    log_level = data.get("logLevel", "INFO")
    logger = CogniteFunctionLogger(log_level)
    
    logger.info(f"Starting {FUNCTION_ID}")
    logger.info(f"Reading config from extraction pipeline: {data.get('ExtractionPipelineExtId')}")
    
    # Load configuration from extraction pipeline
    config = load_config(client, data, logger)
    logger.debug("Loaded configuration successfully")
    
    # Log prompt configuration
    if config.prompt.custom_instructions:
        logger.debug(f"Using custom prompt instructions: {config.prompt.custom_instructions[:100]}...")
    if config.prompt.template:
        logger.debug("Using custom prompt template")
    
    # Initialize state store if enabled
    state_store = None
    cursor = None
    if config.state_store.enabled:
        logger.info(f"State store enabled: {config.state_store.raw_database}/{config.state_store.raw_table}")
        state_store = StateStoreHandler(
            client=client,
            database=config.state_store.raw_database,
            table=config.state_store.raw_table,
            logger=logger
        )
        
        # Initialize run and get cursor
        force_reset = data.get("resetState", False)
        cursor = state_store.initialize_run(
            config_version=config.state_store.config_version,
            force_reset=force_reset
        )
        
        if cursor:
            logger.info(f"Resuming from cursor: {cursor}")
        else:
            logger.info("Starting fresh - will process all matching instances")
    else:
        logger.debug("State store disabled - processing without cursor")
    
    # Retrieve the view
    logger.info(f"Retrieving view: {config.view.space}/{config.view.external_id}/{config.view.version}")
    view_id = config.view.as_view_id()
    views = client.data_modeling.views.retrieve(view_id)
    if not views:
        raise ValueError(f"View not found: {view_id}")
    view = views[0]
    
    # Initialize the extractor with prompt configuration
    extractor = LLMPropertyExtractor(
        client=client,
        agent_external_id=config.agent.external_id,
        logger=logger,
        custom_prompt_instructions=config.prompt.get_custom_instructions(),
        prompt_template=config.prompt.get_template()
    )
    
    # Get property configs with write modes
    property_configs = config.extraction.get_property_configs()
    if property_configs:
        write_modes = {pc.property: pc.write_mode.value for pc in property_configs}
        logger.debug(f"Property write modes: {write_modes}")
    
    batch_size = config.processing.batch_size
    total_processed = 0
    total_updated = 0
    batch_number = 0
    
    # Run in a loop until timeout or no more instances
    while True:
        elapsed_time = time.time() - start_time
        remaining_time = MAX_RUNTIME_SECONDS - elapsed_time
        
        # Check if we're approaching the timeout
        if remaining_time < 30:  # Stop if less than 30 seconds remaining
            logger.info(f"Approaching timeout limit ({elapsed_time:.1f}s elapsed), stopping")
            break
        
        batch_number += 1
        logger.info(f"Batch {batch_number}: Querying up to {batch_size} instances ready for extraction")
        instances = query_instances(
            client, view_id, config, logger, 
            limit=batch_size, 
            cursor=cursor,
            property_configs=property_configs
        )
        
        if not instances:
            logger.info("No more instances need processing")
            break
        
        logger.info(f"Batch {batch_number}: Processing {len(instances)} instances")
        node_applies = extractor.extract_properties_from_instances(
            instances=instances,
            view=view,
            text_property=config.extraction.text_property,
            properties_to_extract=config.extraction.get_properties_to_extract() or None,
            ai_property_mapping=config.extraction.get_ai_property_mapping() or None,
            property_configs=property_configs or None,
            dry_run=True  # Always dry run first, we apply separately
        )
        
        # Apply batch updates
        if node_applies:
            logger.info(f"Batch {batch_number}: Applying {len(node_applies)} node updates to CDF")
            client.data_modeling.instances.apply(node_applies)
            total_updated += len(node_applies)
        
        total_processed += len(instances)
        
        # Update cursor to the latest lastUpdatedTime in this batch
        if state_store and instances:
            # Get the max lastUpdatedTime from this batch
            batch_max_time = max(
                inst.last_updated_time for inst in instances 
                if hasattr(inst, 'last_updated_time') and inst.last_updated_time
            )
            if batch_max_time:
                cursor = batch_max_time.isoformat().replace('+00:00', 'Z')
                state_store.update_cursor(cursor, increment_processed=len(instances))
                logger.debug(f"Updated cursor to: {cursor}")
        
        elapsed_time = time.time() - start_time
        logger.info(f"Batch {batch_number} complete: {len(instances)} processed, {len(node_applies) if node_applies else 0} updated. "
                   f"Running totals: {total_processed} processed, {total_updated} updated. "
                   f"Elapsed time: {elapsed_time:.1f}s")
    
    elapsed_time = time.time() - start_time
    logger.info(f"Completed: processed {total_processed} instances, updated {total_updated} in {elapsed_time:.1f}s ({batch_number} batches)")
    
    # Log final state store stats
    if state_store:
        stats = state_store.get_stats()
        logger.info(f"State store stats: total_processed={stats['total_processed']}, cursor={stats['cursor']}")
    
    return {"processed": total_processed, "updated": total_updated}


def query_instances(
    client: CogniteClient, 
    view_id, 
    config: Config, 
    logger: CogniteFunctionLogger,
    limit: int = 10,
    cursor: str = None,
    property_configs: list = None
) -> list:
    """
    Query instances from the view that are ready for extraction.
    
    Only returns instances where:
    - The text property exists (has content to extract from)
    - lastUpdatedTime >= cursor (if cursor provided, for incremental processing)
    - For add_new_only properties: target property is empty
    - For append/overwrite: always included (handled in extractor)
    
    Args:
        client: Authenticated CogniteClient
        view_id: The view ID to query
        config: Configuration object
        logger: Logger instance
        limit: Maximum number of instances to return (default: 10)
        cursor: Optional cursor (ISO timestamp) to filter by lastUpdatedTime
        property_configs: Optional list of PropertyConfig objects
    
    Returns:
        List of instances ready for extraction, sorted by lastUpdatedTime ascending
    """
    from cognite.client import data_modeling as dm
    from cognite.client.data_classes.data_modeling import InstanceSort
    from config import PropertyConfig, WriteMode
    
    filters: list[dm.filters.Filter] = []
    
    # Filter 1: Must have the text property populated
    text_property_ref = [view_id.space, f"{view_id.external_id}/{view_id.version}", config.extraction.text_property]
    text_exists_filter = dm.filters.Exists(text_property_ref)
    filters.append(text_exists_filter)
    logger.debug(f"Filter: text property '{config.extraction.text_property}' must exist")
    
    # Filter 2: High-water mark cursor filter (for incremental processing)
    if cursor:
        logger.debug(f"Filter: lastUpdatedTime >= {cursor}")
        filters.append(dm.filters.Range(
            ["node", "lastUpdatedTime"],
            gte={"value": cursor}
        ))
    
    # Filter 3: Property filters based on write modes
    # For add_new_only: target must be empty
    # For append/overwrite: no filter needed (they process regardless)
    
    if property_configs:
        # New-style: use PropertyConfig objects
        add_new_only_props = [
            pc for pc in property_configs 
            if pc.write_mode == WriteMode.ADD_NEW_ONLY
        ]
        other_props = [
            pc for pc in property_configs 
            if pc.write_mode != WriteMode.ADD_NEW_ONLY
        ]
        
        if add_new_only_props and not other_props:
            # ALL properties are add_new_only: require at least one target to be empty
            empty_property_filters = []
            for pc in add_new_only_props:
                target_prop = pc.get_target_property()
                target_property_ref = [view_id.space, f"{view_id.external_id}/{view_id.version}", target_prop]
                prop_not_exists = dm.filters.Not(dm.filters.Exists(target_property_ref))
                empty_property_filters.append(prop_not_exists)
                logger.debug(f"Filter: target property '{target_prop}' should not exist (add_new_only)")
            
            if empty_property_filters:
                if len(empty_property_filters) == 1:
                    filters.append(empty_property_filters[0])
                else:
                    filters.append(dm.filters.Or(*empty_property_filters))
        
        elif add_new_only_props and other_props:
            # Mixed modes: build OR filter
            # (any add_new_only target empty) OR (we have append/overwrite props)
            # Since we have append/overwrite props, we can't filter them out at query level
            # Just add a filter for add_new_only properties OR always include
            # In practice, for mixed modes we include all and let extractor handle it
            logger.debug("Mixed write modes detected - filtering relaxed, extractor handles modes")
        
        # else: all props are append/overwrite, no target filter needed
    else:
        # Legacy mode: use properties_to_extract and ai_property_mapping
        properties_to_check = config.extraction.properties_to_extract or []
        ai_mapping = config.extraction.ai_property_mapping or {}
        
        if properties_to_check:
            # Build a filter that checks if ANY target property is empty
            empty_property_filters = []
            for source_prop in properties_to_check:
                target_prop = ai_mapping.get(source_prop, source_prop)
                target_property_ref = [view_id.space, f"{view_id.external_id}/{view_id.version}", target_prop]
                prop_not_exists = dm.filters.Not(dm.filters.Exists(target_property_ref))
                empty_property_filters.append(prop_not_exists)
                logger.debug(f"Filter: target property '{target_prop}' should not exist (empty)")
            
            if empty_property_filters:
                if len(empty_property_filters) == 1:
                    filters.append(empty_property_filters[0])
                else:
                    filters.append(dm.filters.Or(*empty_property_filters))
    
    # Filter 4: Add user-defined filters from config
    if config.processing.filters:
        logger.debug(f"Adding user-defined filters: {config.processing.filters}")
        for f in config.processing.filters:
            filters.append(build_filter(f, view_id))
    
    # Combine all filters with AND
    combined_filter = dm.filters.And(*filters) if len(filters) > 1 else filters[0] if filters else None
    
    # Query instances with limit, sorted by lastUpdatedTime ascending for proper cursor progression
    logger.debug(f"Querying instances with limit={limit}")
    instances = client.data_modeling.instances.list(
        instance_type="node",
        sources=[view_id],
        filter=combined_filter,
        limit=limit,
        sort=[InstanceSort(["node", "lastUpdatedTime"], "ascending")]
    )
    
    return list(instances)


def build_filter(filter_config: dict, view_id=None):
    """
    Build a DM filter from configuration.
    
    Args:
        filter_config: Filter configuration dict with keys:
            - type: Filter type (equals, in, prefix, exists, not_exists)
            - property: Property name or path
            - value: Value to filter by (not needed for exists/not_exists)
        view_id: Optional ViewId to build full property references
    
    Returns:
        A dm.filters.Filter object
    """
    from cognite.client import data_modeling as dm
    
    filter_type = filter_config.get("type", "equals")
    property_path = filter_config.get("property")
    value = filter_config.get("value")
    
    # Build full property reference if view_id provided and property is a simple string
    if view_id and isinstance(property_path, str):
        property_path = [view_id.space, f"{view_id.external_id}/{view_id.version}", property_path]
    elif view_id and isinstance(property_path, list) and len(property_path) == 1:
        property_path = [view_id.space, f"{view_id.external_id}/{view_id.version}", property_path[0]]
    
    if filter_type == "equals":
        return dm.filters.Equals(property_path, value)
    elif filter_type == "in":
        return dm.filters.In(property_path, value)
    elif filter_type == "prefix":
        return dm.filters.Prefix(property_path, value)
    elif filter_type == "exists":
        return dm.filters.Exists(property_path)
    elif filter_type == "not_exists":
        return dm.filters.Not(dm.filters.Exists(property_path))
    else:
        raise ValueError(f"Unsupported filter type: {filter_type}")


def run_locally():
    """Run the function locally for development/testing."""
    
    required_envvars = ("CDF_PROJECT", "CDF_CLUSTER", "IDP_CLIENT_ID", "IDP_CLIENT_SECRET", "IDP_TOKEN_URL")
    if missing := [envvar for envvar in required_envvars if envvar not in os.environ]:
        raise ValueError(f"Missing required environment variables: {missing}")

    cdf_project_name = os.environ["CDF_PROJECT"]
    cdf_cluster = os.environ["CDF_CLUSTER"]
    client_id = os.environ["IDP_CLIENT_ID"]
    client_secret = os.environ["IDP_CLIENT_SECRET"]
    token_uri = os.environ["IDP_TOKEN_URL"]
    base_url = f"https://{cdf_cluster}.cognitedata.com"

    client = CogniteClient(
        ClientConfig(
            client_name="AI Property Extractor Local",
            base_url=base_url,
            project=cdf_project_name,
            credentials=OAuthClientCredentials(
                token_url=token_uri,
                client_id=client_id,
                client_secret=client_secret,
                scopes=[f"{base_url}/.default"],
            ),
        )
    )
    
    data = {
        "logLevel": "DEBUG", 
        "ExtractionPipelineExtId": "ep_ai_property_extractor"
    }
    
    print("Starting AI Property Extractor locally...")
    result = handle(data, client)
    print(f"Result: {result}")
    return result


if __name__ == "__main__":
    run_locally()


