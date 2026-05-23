"""
CDF Handler for Extract Assets By Pattern

This module provides a CDF-compatible handler function that can be used in
CDF Functions or called directly, maintaining compatibility with the CDF
workflow format.
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
    try:
        from config import Config, load_config_parameters

        CDF_CONFIG_AVAILABLE = True
    except ImportError:
        CDF_CONFIG_AVAILABLE = False
        load_config_parameters = None
        Config = None


def handle(data: Dict[str, Any], client: CogniteClient = None) -> Dict[str, Any]:
    """
    CDF-compatible handler function for extract assets by pattern.

    This function follows the CDF function handler pattern and can be used
    in CDF Functions or called directly.

    Args:
        data: Dictionary containing:
            - ExtractionPipelineExtId: External ID of the extraction pipeline (optional)
            - patterns: List of pattern dictionaries (required, or provided via config)
            - files: List of file info dictionaries (optional, will query CDF if not provided)
            - limit: Optional limit on number of files to retrieve
            - mime_type: Optional MIME type filter
            - instance_space: Optional data model space to filter files by
            - partial_match: Whether to enable partial matching (default: True)
            - min_tokens: Minimum number of tokens required (default: 2)
            - batch_size: Number of files to process in each batch (default: 20)
            - state_store: Optional state store dictionary to update with results
            - logLevel: Optional log level (DEBUG, INFO, WARNING, ERROR)
        client: CogniteClient instance (required if using CDF config loading or querying files from CDF)

    Returns:
        Dictionary with status and result information
    """
    logger = None

    try:
        # Initialize logging
        loglevel = data.get("logLevel", "INFO")

        # Use logger from dependencies
        try:
            from .dependencies import create_logger_service
        except ImportError:
            from dependencies import create_logger_service

        logger = create_logger_service(loglevel)

        logger.info(f"Starting extract assets by pattern with loglevel = {loglevel}")

        # Load configuration from CDF extraction pipeline (or use provided mock config)
        cdf_config = None
        if "_cdf_config" in data:
            # Use provided mock config (for local testing)
            cdf_config = data["_cdf_config"]
            logger.info("Using provided config (local mode)")
        elif CDF_CONFIG_AVAILABLE and (
            data.get("configuration") is not None or data.get("step")
        ):
            step = data.get("step", "extract")
            logger.info(f"Loading config for step: {step}")

            cdf_config = load_config_parameters(client, data)
            logger.debug(f"Loaded CDF config: {cdf_config}")

        if cdf_config:
            # Extract parameters from CDF config and merge into data
            config_data = cdf_config.data
            # Handle patterns - must be provided directly in config
            if "patterns" not in data and "patterns" in config_data:
                data["patterns"] = config_data["patterns"]
            if "limit" not in data and "limit" in config_data:
                data["limit"] = config_data["limit"]
            if "mime_type" not in data and "mime_type" in config_data:
                data["mime_type"] = config_data["mime_type"]
            if "instance_space" not in data and "instance_space" in config_data:
                data["instance_space"] = config_data["instance_space"]
            if "partial_match" not in data and "partial_match" in config_data:
                data["partial_match"] = config_data["partial_match"]
            if "min_tokens" not in data and "min_tokens" in config_data:
                data["min_tokens"] = config_data["min_tokens"]
            if "batch_size" not in data and "batch_size" in config_data:
                data["batch_size"] = config_data["batch_size"]
            if "max_attempts" not in data and "max_attempts" in config_data:
                data["max_attempts"] = config_data.get("max_attempts", 3)
            if (
                "max_pages_per_chunk" not in data
                and "max_pages_per_chunk" in config_data
            ):
                data["max_pages_per_chunk"] = config_data.get("max_pages_per_chunk", 50)
            if (
                "diagram_detect_config" not in data
                and "diagram_detect_config" in config_data
            ):
                data["diagram_detect_config"] = config_data.get(
                    "diagram_detect_config", {}
                )
            if "logLevel" not in data:
                data["logLevel"] = "DEBUG" if cdf_config.parameters.debug else "INFO"

            # Store CDF config for use in pipeline
            data["_cdf_config"] = cdf_config

        # Call pipeline function
        try:
            from .pipeline import extract_assets_by_pattern
        except ImportError:
            from pipeline import extract_assets_by_pattern

        extract_assets_by_pattern(
            client=client,
            logger=logger,
            data=data,
        )

        return {"status": "succeeded", "data": data}

    except Exception as e:
        message = f"Extract assets by pattern pipeline failed: {e!s}"

        if logger:
            logger.error(message)
        else:
            print(f"[ERROR] {message}")

        return {"status": "failure", "message": message}


def run_locally():
    """Run handler locally for testing (requires .env file)."""
    import os

    from dotenv import load_dotenv

    load_dotenv()

    try:
        from .dependencies import create_client, get_env_variables
    except ImportError:
        from dependencies import create_client, get_env_variables

    # Get environment variables
    env_config = get_env_variables()

    # Create client
    if not CDF_AVAILABLE:
        raise ImportError("CogniteClient not available. Install cognite-sdk.")

    client = create_client(env_config)

    data = {"step": "extract", "logLevel": "INFO"}
    if CDF_CONFIG_AVAILABLE and load_config_parameters is not None:
        cdf_config = load_config_parameters(client, data)
        data["_cdf_config"] = cdf_config
        patterns_data = cdf_config.data.get("patterns")
        print(
            f"Loaded {len(patterns_data) if patterns_data else 0} pattern groups from default.config.yaml"
        )
        if cdf_config.parameters.initialize_state:
            print("Running in initialize_state mode (will skip processing)")

    # Run handler
    print("Starting extract assets by pattern pipeline...")
    result = handle(data, client)

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
