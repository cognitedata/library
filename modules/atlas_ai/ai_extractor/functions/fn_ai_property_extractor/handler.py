"""
AI Property Extractor - Cognite Function Handler

This function extracts structured property values from unstructured text fields
in data modeling instances using LLM agents.
"""

import os
import sys
import traceback
from pathlib import Path
from typing import Literal

from cognite.client import CogniteClient, ClientConfig
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import ExtractionPipelineRunWrite

# Add current directory to path for local imports
sys.path.append(str(Path(__file__).parent))

from config import load_config, Config
from extractor import LLMPropertyExtractor


FUNCTION_ID = "ai_property_extractor"
EXTRACTION_RUN_MESSAGE_LIMIT = 1000
DEFAULT_EXTRACTION_PIPELINE_EXT_ID = "ep_ai_property_extractor"


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
    
    Args:
        data: Function input data
        client: Authenticated CogniteClient
    
    Returns:
        Dictionary with processed and updated counts
    """
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
    
    # Query instances from the view (limited to batch_size, pre-filtered for extraction readiness)
    batch_size = config.processing.batch_size
    logger.info(f"Querying up to {batch_size} instances ready for extraction")
    instances = query_instances(client, view_id, config, logger, limit=batch_size)
    logger.info(f"Found {len(instances)} instances to process")
    
    if not instances:
        logger.info("No instances need processing")
        return {"processed": 0, "updated": 0}
    
    # Process all instances (already limited by query)
    logger.info(f"Processing {len(instances)} instances")
    node_applies = extractor.extract_properties_from_instances(
        instances=instances,
        view=view,
        text_property=config.extraction.text_property,
        properties_to_extract=config.extraction.properties_to_extract or None,
        ai_property_mapping=config.extraction.ai_property_mapping or None,
        dry_run=True  # Always dry run first, we apply separately
    )
    
    # Apply all updates
    updated_count = 0
    if node_applies:
        logger.info(f"Applying {len(node_applies)} node updates to CDF")
        client.data_modeling.instances.apply(node_applies)
        updated_count = len(node_applies)
    
    logger.info(f"Completed: processed {len(instances)} instances, updated {updated_count}")
    return {"processed": len(instances), "updated": updated_count}


def query_instances(
    client: CogniteClient, 
    view_id, 
    config: Config, 
    logger: CogniteFunctionLogger,
    limit: int = 10
) -> list:
    """
    Query instances from the view that are ready for extraction.
    
    Only returns instances where:
    - The text property exists (has content to extract from)
    - At least one target property is empty (needs to be filled)
    
    Args:
        client: Authenticated CogniteClient
        view_id: The view ID to query
        config: Configuration object
        logger: Logger instance
        limit: Maximum number of instances to return (default: 10)
    
    Returns:
        List of instances ready for extraction
    """
    from cognite.client import data_modeling as dm
    
    filters: list[dm.filters.Filter] = []
    
    # Filter 1: Must have the text property populated
    text_property_ref = [view_id.space, f"{view_id.external_id}/{view_id.version}", config.extraction.text_property]
    text_exists_filter = dm.filters.Exists(text_property_ref)
    filters.append(text_exists_filter)
    logger.debug(f"Filter: text property '{config.extraction.text_property}' must exist")
    
    # Filter 2: At least one target property must be empty (not filled yet)
    # Determine which properties we're targeting
    properties_to_check = config.extraction.properties_to_extract or []
    ai_mapping = config.extraction.ai_property_mapping or {}
    
    if properties_to_check:
        # Build a filter that checks if ANY target property is empty
        # Target property = mapped property if mapping exists, otherwise the source property
        empty_property_filters = []
        for source_prop in properties_to_check:
            target_prop = ai_mapping.get(source_prop, source_prop)
            target_property_ref = [view_id.space, f"{view_id.external_id}/{view_id.version}", target_prop]
            # Property is empty if it does NOT exist
            prop_not_exists = dm.filters.Not(dm.filters.Exists(target_property_ref))
            empty_property_filters.append(prop_not_exists)
            logger.debug(f"Filter: target property '{target_prop}' should not exist (empty)")
        
        if empty_property_filters:
            # Use OR: at least one property needs to be empty
            if len(empty_property_filters) == 1:
                filters.append(empty_property_filters[0])
            else:
                filters.append(dm.filters.Or(*empty_property_filters))
    
    # Filter 3: Add user-defined filters from config
    if config.processing.filters:
        logger.debug(f"Adding user-defined filters: {config.processing.filters}")
        for f in config.processing.filters:
            filters.append(build_filter(f, view_id))
    
    # Combine all filters with AND
    combined_filter = dm.filters.And(*filters) if len(filters) > 1 else filters[0] if filters else None
    
    # Query instances with limit
    logger.debug(f"Querying instances with limit={limit}")
    instances = client.data_modeling.instances.list(
        instance_type="node",
        sources=[view_id],
        filter=combined_filter,
        limit=limit
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


