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
    
    # Retrieve the view
    logger.info(f"Retrieving view: {config.view.space}/{config.view.external_id}/{config.view.version}")
    view_id = config.view.as_view_id()
    views = client.data_modeling.views.retrieve(view_id)
    if not views:
        raise ValueError(f"View not found: {view_id}")
    view = views[0]
    
    # Initialize the extractor
    extractor = LLMPropertyExtractor(
        client=client,
        agent_external_id=config.agent.external_id,
        logger=logger
    )
    
    # Query instances from the view
    logger.info("Querying instances from view")
    instances = query_instances(client, view_id, config, logger)
    logger.info(f"Found {len(instances)} instances to process")
    
    if not instances:
        return {"processed": 0, "updated": 0}
    
    # Process instances in batches
    batch_size = config.processing.batch_size
    all_node_applies = []
    
    for i in range(0, len(instances), batch_size):
        batch = instances[i:i + batch_size]
        logger.info(f"Processing batch {i // batch_size + 1} ({len(batch)} instances)")
        
        node_applies = extractor.extract_properties_from_instances(
            instances=batch,
            view=view,
            text_property=config.extraction.text_property,
            properties_to_extract=config.extraction.properties_to_extract or None,
            ai_property_mapping=config.extraction.ai_property_mapping or None,
            dry_run=True  # Always dry run first, we apply separately
        )
        all_node_applies.extend(node_applies)
    
    # Apply all updates
    updated_count = 0
    if all_node_applies:
        logger.info(f"Applying {len(all_node_applies)} node updates to CDF")
        client.data_modeling.instances.apply(all_node_applies)
        updated_count = len(all_node_applies)
    
    logger.info(f"Completed: processed {len(instances)} instances, updated {updated_count}")
    return {"processed": len(instances), "updated": updated_count}


def query_instances(client: CogniteClient, view_id, config: Config, logger: CogniteFunctionLogger) -> list:
    """
    Query instances from the view, optionally applying filters.
    
    Args:
        client: Authenticated CogniteClient
        view_id: The view ID to query
        config: Configuration object
        logger: Logger instance
    
    Returns:
        List of instances
    """
    from cognite.client import data_modeling as dm
    
    # Build filter if provided
    filter_obj = None
    if config.processing.filters:
        logger.debug(f"Applying filters: {config.processing.filters}")
        filter_obj = dm.filters.And(*[
            build_filter(f) for f in config.processing.filters
        ]) if isinstance(config.processing.filters, list) else None
    
    # Query instances
    instances = client.data_modeling.instances.list(
        instance_type="node",
        sources=[view_id],
        filter=filter_obj,
        limit=None  # Get all matching instances
    )
    
    return list(instances)


def build_filter(filter_config: dict):
    """Build a DM filter from configuration."""
    from cognite.client import data_modeling as dm
    
    filter_type = filter_config.get("type", "equals")
    property_path = filter_config.get("property")
    value = filter_config.get("value")
    
    if filter_type == "equals":
        return dm.filters.Equals(property_path, value)
    elif filter_type == "in":
        return dm.filters.In(property_path, value)
    elif filter_type == "prefix":
        return dm.filters.Prefix(property_path, value)
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


