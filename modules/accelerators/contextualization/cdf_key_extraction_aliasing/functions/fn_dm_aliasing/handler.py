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

# CDF Functions loads this file as a top-level module (`handler`), so relative imports
# like `from .x import y` will fail with "no known parent package".
from cdf_adapter import _convert_yaml_direct_to_aliasing_config
from dependencies import create_client, create_logger_service, get_env_variables
from engine.tag_aliasing_engine import AliasingEngine

def handle(data: Dict[str, Any], client: CogniteClient = None) -> Dict[str, Any]:
    """
    CDF-compatible handler function for tag aliasing.

    This function follows the CDF function handler pattern and can be used
    in CDF Functions or called directly.

    Args:
        data: Dictionary containing:
            - config: Workflow-provided config payload
            - logLevel: Optional log level (DEBUG, INFO, WARNING, ERROR)
            - tags: Optional list of tags to generate aliases for
            - entities: Optional list of entities with tags to alias
        client: CogniteClient instance (required for RAW read/write workflow mode)

    Returns:
        JSON-serializable dict with status + summary.
    """
    logger = None

    try:
        # Initialize logging
        loglevel = data.get("logLevel", "INFO")
        verbose = data.get("verbose", False)

        # Use dependencies helper to create logger
        logger = create_logger_service(loglevel, verbose)

        logger.info(f"Starting tag aliasing with loglevel = {loglevel} with verbose set to {verbose}")

        # Load configuration from workflow payload (required)
        if "config" in data:
            # Workflow/direct config provided
            provided_config = data["config"]
            if isinstance(provided_config, dict):
                unwrapped = provided_config.get("config", provided_config)
                # CDF/workflow-shaped aliasing config
                if (
                    isinstance(unwrapped, dict)
                    and "data" in unwrapped
                    and isinstance(unwrapped.get("data"), dict)
                    and "aliasing_rules" in unwrapped.get("data", {})
                ):
                    aliasing_config = _convert_yaml_direct_to_aliasing_config(
                        {"config": unwrapped}
                    )
                    logger.info("Using workflow-provided aliasing config")

                    # Enable RAW upload from config parameters unless explicitly overridden
                    params = unwrapped.get("parameters", {})
                    if isinstance(params, dict):
                        if "raw_db" in params:
                            data.setdefault("raw_db", params["raw_db"])
                        if "raw_table_aliases" in params:
                            data.setdefault("raw_table", params["raw_table_aliases"])
                        if "raw_table_state" in params:
                            data.setdefault("raw_table_state", params["raw_table_state"])
                        data.setdefault("upload_to_raw", True)
                else:
                    # Direct engine-ready config
                    aliasing_config = provided_config
                    logger.info("Using provided config directly")
            else:
                aliasing_config = provided_config
                logger.info("Using provided config directly")
        else:
            # Load from aliasing pipeline configs
            try:
                import yaml

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

        # Call pipeline function (CDF Functions loads this as top-level module)
        from pipeline import tag_aliasing

        tag_aliasing(client=client, logger=logger, data=data, engine=engine)

        # CDF Functions requires the returned object to be JSON-serializable.
        # The pipeline stores rich objects in `data` (engines, mappings, etc),
        # so only return a compact summary.
        return {
            "status": "succeeded",
            "summary": {
                "total_tags_processed": int(data.get("total_tags_processed", 0)),
                "total_aliases_generated": int(data.get("total_aliases_generated", 0)),
                "raw_written": bool(data.get("upload_to_raw", False)),
                "raw_db": data.get("raw_db"),
                "raw_table_aliases": data.get("raw_table"),
            },
        }

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
