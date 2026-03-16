"""
CDF Handler for Aliasing

This module provides a CDF-compatible handler function that can be used in
CDF Functions or called directly, maintaining compatibility with the CDF
workflow format while using the existing AliasingEngine.
"""

import sys
from pathlib import Path
from typing import Any, Dict

# Add parent path for imports
sys.path.append(str(Path(__file__).parent.parent))

try:
    from cognite.client import CogniteClient

    CDF_AVAILABLE = True
except ImportError:
    CDF_AVAILABLE = False

from .cdf_adapter import convert_cdf_config_to_aliasing_config
from .dependencies import create_client, create_logger_service, get_env_variables
from .engine.tag_aliasing_engine import AliasingEngine

try:
    from .config import Config, load_config_parameters

    CDF_CONFIG_AVAILABLE = True
except ImportError:
    CDF_CONFIG_AVAILABLE = False
    load_config_parameters = None
    Config = None


def handle(data: Dict[str, Any], client: CogniteClient = None) -> Dict[str, Any]:
    """
    CDF-compatible handler function for tag aliasing.

    This function follows the CDF function handler pattern and can be used
    in CDF Functions or called directly.

    Args:
        data: Dictionary containing:
            - ExtractionPipelineExtId: Optional external ID of extraction pipeline
            - logLevel: Optional log level (DEBUG, INFO, WARNING, ERROR)
            - tags: List of tags to generate aliases for (required)
            - entities: Optional list of entities with tags to alias
        client: CogniteClient instance (required if using CDF config loading)

    Returns:
        Dictionary with}")
    """
    logger = None

    try:
        # Initialize logging
        loglevel = data.get("logLevel", "INFO")
        verbose = data.get("verbose", False)

        # Use dependencies helper to create logger
        logger = create_logger_service(loglevel, verbose)

        logger.info(f"Starting tag aliasing with loglevel = {loglevel} with verbose set to {verbose}")

        # Load configuration from CDF extraction pipeline or use provided config
        if CDF_CONFIG_AVAILABLE and client and "ExtractionPipelineExtId" in data:
            pipeline_ext_id = data["ExtractionPipelineExtId"]
            logger.info(f"Loading config from extraction pipeline: {pipeline_ext_id}")

            cdf_config = load_config_parameters(client, data)

            # Convert CDF config to aliasing config
            aliasing_config = convert_cdf_config_to_aliasing_config(cdf_config)
            data["_cdf_config"] = cdf_config
            data["_aliasing_config"] = aliasing_config

        elif "config" in data:
            # Direct config provided (for standalone usage)
            aliasing_config = data["config"]
            logger.info("Using provided config directly")
        else:
            # Load from aliasing pipeline configs
            try:
                import yaml

                from .cdf_adapter import _convert_yaml_direct_to_aliasing_config

                # Go up to key_extraction_aliasing root, then to pipelines
                pipelines_dir = Path(__file__).parent.parent.parent / "pipelines"
                all_aliasing_rules = []

                for config_file in sorted(pipelines_dir.glob("*aliasing*.config.yaml")):
                    try:
                        with open(config_file, "r") as f:
                            pipeline_config = yaml.safe_load(f)

                        config_data = pipeline_config.get("config", {}).get("data", {})

                        # Use adapter to convert aliasing rules to engine format
                        pipeline_aliasing_config = (
                            _convert_yaml_direct_to_aliasing_config(
                                {"config": {"data": config_data}}
                            )
                        )
                        converted_rules = pipeline_aliasing_config.get("rules", [])
                        all_aliasing_rules.extend(converted_rules)
                        logger.info(
                            f"Loaded {len(converted_rules)} aliasing rules from {config_file.name}"
                        )
                    except Exception as e:
                        logger.warning(
                            f"Failed to load aliasing pipeline config {config_file.name}: {e}"
                        )
                        continue

                if all_aliasing_rules:
                    aliasing_config = {
                        "rules": all_aliasing_rules,
                        "validation": {
                            "max_aliases_per_tag": 50,
                            "min_alias_length": 2,
                            "max_alias_length": 50,
                            "allowed_characters": r"A-Za-z0-9-_/. ",
                        },
                    }
                    logger.info(
                        f"Using aliasing pipeline configs: {len(all_aliasing_rules)} rules loaded"
                    )
                else:
                    # Minimal default config if no pipeline configs found
                    aliasing_config = {
                        "rules": [
                            {
                                "name": "normalize_separators",
                                "type": "character_substitution",
                                "enabled": True,
                                "priority": 10,
                                "preserve_original": True,
                                "config": {"substitutions": {"_": "-", " ": "-"}},
                            }
                        ],
                        "validation": {
                            "max_aliases_per_tag": 50,
                            "min_alias_length": 1,
                            "max_alias_length": 100,
                        },
                    }
                    logger.warning(
                        "No aliasing pipeline configs found, using minimal default config"
                    )
            except Exception as e:
                logger.error(f"Failed to load aliasing pipeline configs: {e}")
                # Minimal default config
                aliasing_config = {
                    "rules": [
                        {
                            "name": "normalize_separators",
                            "type": "character_substitution",
                            "enabled": True,
                            "priority": 10,
                            "preserve_original": True,
                            "config": {"substitutions": {"_": "-", " ": "-"}},
                        }
                    ],
                    "validation": {
                        "max_aliases_per_tag": 50,
                        "min_alias_length": 1,
                        "max_alias_length": 100,
                    },
                }
                logger.info("Using minimal default aliasing config")

        # Initialize engine with logger
        engine = AliasingEngine(aliasing_config, logger)
        data["_engine"] = engine

        # Call pipeline function
        from .pipeline import tag_aliasing

        tag_aliasing(client=client, logger=logger, data=data, engine=engine)

        return {"status": "succeeded", "data": data}

    except Exception as e:
        message = f"Tag aliasing pipeline failed: {e!s}"

        if logger:
            logger.error(message)
        else:
            print(f"[ERROR] {message}")

        return {"status": "failure", "message": message}


def run_locally():
    """Run handler locally for testing (requires .env file and CDF config)."""
    # Use dependencies helper to get environment variables and create client
    env_config = get_env_variables()
    client = create_client(env_config, debug=False)

    # Test data
    data = {
        "logLevel": "DEBUG",
        "verbose": False,
        "tags": ["P-101", "FCV-2001A", "T-201"],
        "entities": [
            {
                "tag": "P-101",
                "entity_type": "asset",
                "context": {"site": "Plant_A", "equipment_type": "pump"},
            }
        ],
    }

    # Run handler
    print("Starting tag aliasing pipeline...")
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
