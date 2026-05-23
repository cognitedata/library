"""
CDF Handler for Create Asset Hierarchy

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
    CDF-compatible handler function for create asset hierarchy.

    This function follows the CDF function handler pattern and can be used
    in CDF Functions or called directly.

    Args:
        data: Dictionary containing:
            - ExtractionPipelineExtId: External ID of the extraction pipeline (optional)
            - locations: List of location dictionaries (required, or provided via config)
            - tags_file: Path to extracted tags CSV file (optional if tags provided)
            - tags: List of tag dictionaries (optional if tags_file provided)
            - output_file: Path to output YAML file (optional)
            - space: Instance space for assets (default: inst_enterprise_file_assets)
            - include_resource_type: Include resourceType as intermediate level (default: False)
            - include_resource_subtype: Include resourceSubType as intermediate level (default: False)
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

        logger.info(f"Starting create asset hierarchy with loglevel = {loglevel}")

        # Load configuration from CDF extraction pipeline
        # Check if _cdf_config is already provided (e.g., from run_locally)
        cdf_config = data.get("_cdf_config")
        if cdf_config is None and CDF_CONFIG_AVAILABLE and (
            data.get("configuration") is not None or data.get("step")
        ):
            step = data.get("step", "create")
            logger.info(f"Loading config for step: {step}")

            cdf_config = load_config_parameters(client, data)
            logger.debug(f"Loaded CDF config: {cdf_config}")

            # Store CDF config for use in pipeline
            data["_cdf_config"] = cdf_config

        # Extract parameters from CDF config and merge into data if config is available
        if cdf_config is not None:
            config_data = cdf_config.data
            config_params = cdf_config.parameters

            scope_yaml = config_data.get("scope")
            if "locations" not in data and scope_yaml is not None:
                hierarchy_levels = config_data.get("hierarchy_levels")
                from .utils.location_utils import convert_locations_dict_to_flat_list

                locations = convert_locations_dict_to_flat_list(
                    scope_yaml, hierarchy_levels
                )
                data["locations"] = locations
                # Also store hierarchy_levels in data for use in pipeline
                if hierarchy_levels:
                    data["hierarchy_levels"] = hierarchy_levels
            if "tags_file" not in data and "tags_file" in config_data:
                data["tags_file"] = config_data["tags_file"]

            # Read from parameters section (moved from data section)
            if "output_file" not in data and config_params.output_file:
                data["output_file"] = config_params.output_file
            if (
                "pattern_config_path" not in data
                and hasattr(config_params, "pattern_config_path")
                and config_params.pattern_config_path
            ):
                data["pattern_config_path"] = config_params.pattern_config_path
            if "space" not in data:
                data["space"] = config_params.space
            if "include_resource_type" not in data:
                data["include_resource_type"] = config_params.include_resource_type
            if "include_resource_subtype" not in data:
                data[
                    "include_resource_subtype"
                ] = config_params.include_resource_subtype
            if "include_resource_subsubtype" not in data:
                data[
                    "include_resource_subsubtype"
                ] = config_params.include_resource_subsubtype
            if "include_resource_variant" not in data:
                data[
                    "include_resource_variant"
                ] = config_params.include_resource_variant
            if "log_level" not in data:
                data["log_level"] = config_params.log_level

        # Get client if available (needed for loading assets from RAW)
        if client is None and CDF_AVAILABLE:
            from .dependencies import create_client, get_env_variables

            try:
                env_vars = get_env_variables()
                client = create_client(env_vars)
            except Exception as e:
                logger.warning(f"Could not create client: {e}")

        # Call pipeline function
        from .pipeline import create_asset_hierarchy

        create_asset_hierarchy(
            client=client,
            logger=logger,
            data=data,
        )

        return {"status": "succeeded", "data": data}

    except Exception as e:
        message = f"Create asset hierarchy pipeline failed: {e!s}"

        if logger:
            logger.error(message)
        else:
            print(f"[ERROR] {message}")

        return {"status": "failure", "message": message}


def run_locally():
    """Run handler locally for testing."""
    from pathlib import Path

    script_dir = Path(__file__).parent.parent.parent
    output_file = script_dir / "results" / "asset_hierarchy.yaml"

    if not CDF_AVAILABLE:
        raise ImportError("CogniteClient not available. Install cognite-sdk.")

    from .dependencies import create_client, get_env_variables

    env_vars = get_env_variables()
    client = create_client(env_vars)

    data = {"step": "create", "log_level": "INFO", "_local_mode": True}
    if CDF_CONFIG_AVAILABLE and load_config_parameters is not None:
        cdf_config = load_config_parameters(client, data)
        data["_cdf_config"] = cdf_config
        params = cdf_config.parameters
        if params.output_file:
            data["output_file"] = params.output_file
        else:
            data["output_file"] = str(output_file)

    # Run handler
    print("Starting create asset hierarchy pipeline...")
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
