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
from cognite.client.data_classes.data_modeling import NodeApply, NodeOrEdgeData

# Add current directory to path for local imports
sys.path.append(str(Path(__file__).parent))

from datetime import datetime, timezone

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
    - State store for incremental processing
    - Per-property write modes (add_new_only, append, overwrite)
    - Epoch-based AI timestamp for append/overwrite modes
    - Force reset via function parameter
    
    Args:
        data: Function input data with optional keys:
            - resetState: bool - Reset state and re-process all instances
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
    
    # Get property configs with write modes
    property_configs = config.extraction.get_property_configs()
    if property_configs:
        write_modes = {pc.property: pc.write_mode.value for pc in property_configs}
        logger.debug(f"Property write modes: {write_modes}")
    
    # Determine if AI timestamp is needed (only for append/overwrite modes)
    use_ai_timestamp = config.has_reprocess_modes() and bool(config.extraction.ai_timestamp_property)
    ai_ts_property = config.extraction.ai_timestamp_property
    
    if use_ai_timestamp:
        logger.info(f"AI timestamp enabled: '{ai_ts_property}' (append/overwrite modes active)")
    else:
        logger.debug("Pure add_new_only mode — AI timestamp not used")
    
    # Initialize state store if enabled
    state_store = None
    epoch_start = None
    force_reset = data.get("resetState", False)
    
    if config.state_store.enabled:
        logger.info(f"State store enabled: {config.state_store.raw_database}/{config.state_store.raw_table}")
        state_store = StateStoreHandler(
            client=client,
            database=config.state_store.raw_database,
            table=config.state_store.raw_table,
            logger=logger
        )
        
        if use_ai_timestamp:
            # Epoch-based state: the epoch_start is a fixed timestamp that marks the
            # beginning of the current processing generation. All nodes processed since
            # epoch_start have ai_ts >= epoch_start. On reset, a new epoch starts.
            epoch_start = state_store.initialize_epoch(
                config_version=config.state_store.config_version,
                force_reset=force_reset
            )
            logger.info(f"Processing epoch start: {epoch_start}")
        else:
            # For add_new_only: cursor is informational only (not used in query filter)
            state_store.initialize_run(
                config_version=config.state_store.config_version,
                force_reset=force_reset
            )
            logger.info("State store initialized (add_new_only mode, cursor is informational)")
    elif use_ai_timestamp:
        # No state store but AI timestamp needed — generate ephemeral epoch
        # This prevents within-run reprocessing but can't persist across runs
        epoch_start = datetime.now(timezone.utc).isoformat()
        logger.warning("State store disabled but append/overwrite modes active. "
                       "AI timestamp will prevent within-run reprocessing, but every "
                       "run will re-process all nodes. Enable state store for incremental processing.")
    
    # Retrieve the source view
    logger.info(f"Retrieving source view: {config.view.space}/{config.view.external_id}/{config.view.version}")
    view_id = config.view.as_view_id()
    views = client.data_modeling.views.retrieve(view_id)
    if not views:
        raise ValueError(f"Source view not found: {view_id}")
    view = views[0]
    
    # Retrieve the target view if configured (optional, falls back to source view)
    target_view = None
    if config.target_view:
        target_view_config = config.get_target_view_config()
        target_view_id = target_view_config.as_view_id()
        logger.info(f"Retrieving target view: {target_view_config.space}/{target_view_config.external_id}/{target_view_config.version}")
        target_views = client.data_modeling.views.retrieve(target_view_id)
        if not target_views:
            raise ValueError(f"Target view not found: {target_view_id}")
        target_view = target_views[0]
    else:
        logger.debug("No target view configured, writing to source view")
    
    # Initialize the extractor with prompt configuration
    extractor = LLMPropertyExtractor(
        client=client,
        agent_external_id=config.agent.external_id,
        logger=logger,
        custom_prompt_instructions=config.prompt.get_custom_instructions(),
        prompt_template=config.prompt.get_template()
    )
    
    # Determine the write view for stamping the AI timestamp
    write_view_config = config.get_target_view_config()
    write_view_id = write_view_config.as_view_id()
    
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
            epoch_start=epoch_start,
            property_configs=property_configs,
            target_view_id=target_view.as_id() if target_view else None
        )
        
        if not instances:
            logger.info("No more instances need processing")
            break
        
        logger.info(f"Batch {batch_number}: Processing {len(instances)} instances (llm_batch_size={config.processing.llm_batch_size})")
        node_applies = extractor.extract_properties_from_instances(
            instances=instances,
            view=view,
            text_property=config.extraction.text_property,
            properties_to_extract=config.extraction.get_properties_to_extract() or None,
            ai_property_mapping=config.extraction.get_ai_property_mapping() or None,
            property_configs=property_configs or None,
            target_view=target_view,
            dry_run=True,  # Always dry run first, we apply separately
            llm_batch_size=config.processing.llm_batch_size
        )
        
        # --- AI Timestamp stamping (only for append/overwrite modes) ---
        timestamp_only_applies = []
        if use_ai_timestamp:
            batch_timestamp = datetime.now(timezone.utc).isoformat()
            
            # Build a set of external_ids that already have a NodeApply from extraction
            updated_ext_ids = set()
            if node_applies:
                for na in node_applies:
                    updated_ext_ids.add(na.external_id)
                    # Add the AI timestamp to the existing NodeApply's sources
                    _add_ai_timestamp_to_node_apply(na, write_view_id, ai_ts_property, batch_timestamp)
            
            # For instances that were processed but had no extracted values,
            # still stamp them with the AI timestamp so they aren't re-queried
            for inst in instances:
                if inst.external_id not in updated_ext_ids:
                    ts_node = NodeApply(
                        space=inst.space,
                        external_id=inst.external_id,
                        sources=[NodeOrEdgeData(
                            write_view_id,
                            properties={ai_ts_property: batch_timestamp}
                        )]
                    )
                    timestamp_only_applies.append(ts_node)
        
        # Apply all updates (extracted values + optional timestamps)
        all_applies = (node_applies or []) + timestamp_only_applies
        if all_applies:
            if use_ai_timestamp:
                logger.info(f"Batch {batch_number}: Applying {len(node_applies or [])} extraction updates + "
                           f"{len(timestamp_only_applies)} timestamp-only updates to CDF")
            else:
                logger.info(f"Batch {batch_number}: Applying {len(node_applies or [])} extraction updates to CDF")
            client.data_modeling.instances.apply(all_applies)
            total_updated += len(node_applies or [])
        
        total_processed += len(instances)
        
        # Update state store (for monitoring and epoch persistence)
        if state_store:
            state_store.update_cursor(
                cursor=datetime.now(timezone.utc).isoformat(),
                increment_processed=len(instances)
            )
        
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
    epoch_start: str = None,
    property_configs: list = None,
    target_view_id = None
) -> list:
    """
    Query instances from the view that are ready for extraction.
    
    Only returns instances where:
    - The text property exists (has content to extract from)
    - For add_new_only properties: target property is empty (natural idempotency)
    - For append/overwrite: AI timestamp property is missing or older than epoch_start
    
    **Epoch-based filtering** (append/overwrite modes):
    An epoch is a fixed timestamp stored in the state store that marks the
    beginning of the current processing generation. All nodes processed within
    the current epoch are stamped with a timestamp >= epoch_start. This filter
    therefore excludes nodes already processed in the current epoch.
    
    This avoids the infinite reprocessing bug of cursor-based approaches:
    since epoch_start is fixed across batches within a run and across runs,
    nodes stamped in batch 1 will still satisfy (ai_ts >= epoch_start) and
    will NOT be re-queried.
    
    Args:
        client: Authenticated CogniteClient
        view_id: The source view ID to query
        config: Configuration object
        logger: Logger instance
        limit: Maximum number of instances to return (default: 10)
        epoch_start: Fixed epoch timestamp (ISO string). Nodes with
                     ai_timestamp >= epoch_start are excluded from results.
        property_configs: Optional list of PropertyConfig objects
        target_view_id: Optional target view ID for checking target property values.
                       If not specified, checks properties in the source view.
    
    Returns:
        List of instances ready for extraction
    """
    from cognite.client import data_modeling as dm
    from config import PropertyConfig, WriteMode
    
    filters: list[dm.filters.Filter] = []
    
    # Filter 1: Must have the text property populated
    text_property_ref = [view_id.space, f"{view_id.external_id}/{view_id.version}", config.extraction.text_property]
    text_exists_filter = dm.filters.Exists(text_property_ref)
    filters.append(text_exists_filter)
    logger.debug(f"Filter: text property '{config.extraction.text_property}' must exist")
    
    # Determine which view to use for checking target property values
    target_check_view_id = target_view_id if target_view_id else view_id
    
    # AI timestamp property reference (on the target view)
    ai_ts_prop = config.extraction.ai_timestamp_property
    use_ai_timestamp = bool(ai_ts_prop) and config.has_reprocess_modes()
    ai_ts_ref = None
    if use_ai_timestamp:
        ai_ts_ref = [target_check_view_id.space, f"{target_check_view_id.external_id}/{target_check_view_id.version}", ai_ts_prop]
    
    # ── Classify properties by write mode ──
    all_add_new_only = True
    has_reprocess_modes = False
    add_new_only_props: list = []
    reprocess_props: list = []
    
    if property_configs:
        add_new_only_props = [pc for pc in property_configs if pc.write_mode == WriteMode.ADD_NEW_ONLY]
        reprocess_props = [pc for pc in property_configs if pc.write_mode != WriteMode.ADD_NEW_ONLY]
        has_reprocess_modes = len(reprocess_props) > 0
        all_add_new_only = not has_reprocess_modes
    
    # ── Build write-mode-aware filters ──
    #
    # Three cases:
    #   1. Pure add_new_only  → AND(all targets empty)
    #   2. Pure append/overwrite → epoch filter only
    #   3. Mixed              → OR(any add_new_only target empty, epoch filter)
    #
    # In all cases the extractor's _apply_write_mode handles per-property
    # logic, so the query filter just needs to avoid *over-filtering* nodes
    # that still have work to do.
    
    def _build_epoch_filter():
        """Build the epoch-based timestamp filter for append/overwrite modes."""
        if use_ai_timestamp and epoch_start:
            not_yet_processed = dm.filters.Not(dm.filters.Exists(ai_ts_ref))
            processed_before_epoch = dm.filters.Range(ai_ts_ref, lt=epoch_start)
            logger.debug(f"Epoch filter: AI timestamp '{ai_ts_prop}' not exist OR < {epoch_start}")
            return dm.filters.Or(not_yet_processed, processed_before_epoch)
        if has_reprocess_modes and not use_ai_timestamp:
            logger.warning("append/overwrite modes without AI timestamp — all matching nodes will be processed every run")
        return None
    
    if property_configs:
        if all_add_new_only:
            # Case 1: ALL properties are add_new_only — require ALL targets empty (AND).
            for pc in add_new_only_props:
                target_prop = pc.get_target_property()
                ref = [target_check_view_id.space, f"{target_check_view_id.external_id}/{target_check_view_id.version}", target_prop]
                filters.append(dm.filters.Not(dm.filters.Exists(ref)))
                logger.debug(f"Filter: target '{target_prop}' must not exist (add_new_only)")
            logger.debug("Pure add_new_only mode — property existence filters prevent reprocessing")
        
        elif not add_new_only_props:
            # Case 2: ALL properties are append/overwrite — epoch filter only.
            epoch_f = _build_epoch_filter()
            if epoch_f:
                filters.append(epoch_f)
        
        else:
            # Case 3: Mixed modes — include node if ANY of these is true:
            #   (a) Any add_new_only target is still empty, OR
            #   (b) Epoch says the node needs reprocessing (for append/overwrite)
            logger.debug("Mixed write modes — building combined OR filter")
            
            or_clauses: list[dm.filters.Filter] = []
            
            # (a) add_new_only: any empty target means work to do
            for pc in add_new_only_props:
                target_prop = pc.get_target_property()
                ref = [target_check_view_id.space, f"{target_check_view_id.external_id}/{target_check_view_id.version}", target_prop]
                or_clauses.append(dm.filters.Not(dm.filters.Exists(ref)))
                logger.debug(f"Filter (mixed/add_new_only): target '{target_prop}' empty")
            
            # (b) epoch filter for append/overwrite properties
            epoch_f = _build_epoch_filter()
            if epoch_f:
                or_clauses.append(epoch_f)
            
            if or_clauses:
                filters.append(dm.filters.Or(*or_clauses) if len(or_clauses) > 1 else or_clauses[0])
    else:
        # Legacy mode: use properties_to_extract and ai_property_mapping (always add_new_only)
        properties_to_check = config.extraction.properties_to_extract or []
        ai_mapping = config.extraction.ai_property_mapping or {}
        
        if properties_to_check:
            for source_prop in properties_to_check:
                target_prop = ai_mapping.get(source_prop, source_prop)
                ref = [target_check_view_id.space, f"{target_check_view_id.external_id}/{target_check_view_id.version}", target_prop]
                filters.append(dm.filters.Not(dm.filters.Exists(ref)))
                logger.debug(f"Filter: target '{target_prop}' must not exist (legacy mode)")
    
    # Filter: Add user-defined filters from config
    if config.processing.filters:
        logger.debug(f"Adding user-defined filters: {config.processing.filters}")
        for f in config.processing.filters:
            filters.append(build_filter(f, view_id))
    
    # Combine all filters with AND
    combined_filter = dm.filters.And(*filters) if len(filters) > 1 else filters[0] if filters else None
    
    # Query instances with limit
    logger.debug(f"Querying instances with limit={limit}")
    
    # Include both source and target views as sources to get properties from both
    sources = [view_id]
    if target_view_id and target_view_id != view_id:
        sources.append(target_view_id)
        logger.debug(f"Including target view in query sources: {target_view_id}")
    
    instances = client.data_modeling.instances.list(
        instance_type="node",
        sources=sources,
        filter=combined_filter,
        limit=limit
    )
    
    return list(instances)


def _add_ai_timestamp_to_node_apply(node_apply, write_view_id, ai_ts_property: str, timestamp: str):
    """
    Add the AI timestamp property to an existing NodeApply.
    
    If the NodeApply already has a source matching the write_view_id, the timestamp
    property is added to that source's properties. Otherwise, a new source is added.
    
    Args:
        node_apply: The NodeApply object to modify in-place
        write_view_id: The view ID to write the timestamp to
        ai_ts_property: The property name for the AI timestamp
        timestamp: The ISO timestamp string to write
    """
    # Check if there's already a source for this view
    for source in node_apply.sources:
        if source.source == write_view_id:
            source.properties[ai_ts_property] = timestamp
            return
    
    # No existing source for this view - add a new one
    node_apply.sources.append(
        NodeOrEdgeData(write_view_id, properties={ai_ts_property: timestamp})
    )


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


