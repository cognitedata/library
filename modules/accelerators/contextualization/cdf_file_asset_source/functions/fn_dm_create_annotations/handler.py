"""
CDF Handler for Create Annotations

This module provides a CDF-compatible handler function that can be used in
CDF Functions or called directly.
"""

from typing import Any, Dict

try:
    from cognite.client import CogniteClient

    CDF_AVAILABLE = True
except ImportError:
    CDF_AVAILABLE = False

# Try to import CDF config loader
try:
    from .config import Config, load_config_parameters

    CDF_CONFIG_AVAILABLE = True
except ImportError:
    CDF_CONFIG_AVAILABLE = False
    load_config_parameters = None
    Config = None


def handle(
    data: Dict[str, Any], client: CogniteClient = None
) -> Dict[str, Any]:  # noqa: ARG001
    """
    CDF-compatible handler function for create annotations.

    This function follows the CDF function handler pattern and can be used
    in CDF Functions or called directly.

    Args:
        data: Dictionary containing:
            - ExtractionPipelineExtId: External ID of the extraction pipeline (optional)
            - results_store: Dictionary mapping file_id to results (optional if loading from RAW)
            - space: Data model space for annotations (default: cdf_cdm)
            - view_external_id: View external ID (default: CogniteDiagramAnnotation)
            - view_version: View version (default: v1)
            - log_level: Optional log level (DEBUG, INFO, WARNING, ERROR)
        client: CogniteClient instance (required if using CDF config loading)

    Returns:
        Dictionary with status and result information
    """
    logger = None

    try:
        # Initialize logging
        loglevel = data.get("log_level", "INFO")

        # Use logger from dependencies
        from .dependencies import create_logger_service

        logger = create_logger_service(loglevel)

        logger.info(f"Starting create annotations with loglevel = {loglevel}")

        # Load configuration from CDF extraction pipeline
        # Check if _cdf_config is already provided (e.g., from run_locally)
        cdf_config = data.get("_cdf_config")
        if (
            cdf_config is None
            and CDF_CONFIG_AVAILABLE
            and client
            and "ExtractionPipelineExtId" in data
        ):
            pipeline_ext_id = data["ExtractionPipelineExtId"]
            logger.info(f"Loading config from extraction pipeline: {pipeline_ext_id}")

            cdf_config = load_config_parameters(client, data)
            logger.debug(f"Loaded CDF config: {cdf_config}")

            # Store CDF config for use in pipeline
            data["_cdf_config"] = cdf_config

        # Extract parameters from CDF config and merge into data if config is available
        if cdf_config is not None:
            config_data = cdf_config.data
            config_params = cdf_config.parameters

            # Read from data section
            if "space" not in data:
                data["space"] = config_data.get("space", "cdf_cdm")
            if "view_external_id" not in data:
                data["view_external_id"] = config_data.get(
                    "view_external_id", "CogniteDiagramAnnotation"
                )
            if "view_version" not in data:
                data["view_version"] = config_data.get("view_version", "v1")
            if "log_level" not in data:
                data["log_level"] = config_params.log_level

        # Get client if available (needed for loading results from RAW)
        if client is None and CDF_AVAILABLE:
            from .dependencies import create_client, get_env_variables

            try:
                env_vars = get_env_variables()
                client = create_client(env_vars)
            except Exception as e:
                logger.warning(f"Could not create client: {e}")

        # Call pipeline function
        from .pipeline import create_annotations

        create_annotations(
            client=client,
            logger=logger,
            data=data,
        )

        return {"status": "succeeded", "data": data}

    except Exception as e:
        message = f"Create annotations pipeline failed: {e!s}"

        if logger:
            logger.error(message)
        else:
            print(f"[ERROR] {message}")

        return {"status": "failure", "message": message}


def run_locally():
    """Run handler locally for testing."""
    from pathlib import Path

    import yaml

    # Test data - load config file
    # handler.py is at: modules/create_asset_hierarchy_from_files/functions/fn_dm_create_annotations/handler.py
    # Need to go up 3 levels to get to create_asset_hierarchy_from_files module root
    script_dir = Path(__file__).parent.parent.parent
    config_file = (
        script_dir / "pipelines" / "ctx_create_annotations_default.config.yaml"
    )

    # Create client for loading assets from RAW
    if not CDF_AVAILABLE:
        raise ImportError("CogniteClient not available. Install cognite-sdk.")

    from .dependencies import create_client, get_env_variables

    env_vars = get_env_variables()
    client = create_client(env_vars)

    # Load config from local YAML file for local testing
    if config_file.exists():
        with open(config_file, "r") as f:
            config = yaml.safe_load(f)
            config_data = config.get("config", {})
            parameters = config_data.get("parameters", {})
            data_section = config_data.get("data", {})

        # Create a mock CDF config structure
        class MockCDFConfig:
            def __init__(self):
                self.parameters = type("obj", (object,), parameters)()
                self.data = data_section

        # Use CDF format - results will be loaded from RAW
        data = {
            "log_level": parameters.get("log_level", "DEBUG"),
            "ExtractionPipelineExtId": "ctx_create_annotations_default",
            "space": data_section.get("space", "cdf_cdm"),
            "view_external_id": data_section.get(
                "view_external_id", "CogniteDiagramAnnotation"
            ),
            "view_version": data_section.get("view_version", "v1"),
            "_cdf_config": MockCDFConfig(),  # Mock config for local testing
            "_local_mode": True,  # Flag to indicate local execution
        }
    else:
        raise FileNotFoundError(f"Config file not found: {config_file}")

    # Run handler
    print("Starting create annotations pipeline...")
    result = handle(data, client=client)

    if result["status"] == "succeeded":
        print("Pipeline completed successfully!")
    else:
        print(f"Pipeline failed: {result.get('message', 'Unknown error')}")

    return result


if __name__ == "__main__":
    try:
        result = run_locally()
        print(f"Final result: {result}")
    except Exception as e:
        print(f"Failed: {e}")
        import traceback

        traceback.print_exc()
