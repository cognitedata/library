"""
CDF Handler for Key Extraction

This module provides a CDF-compatible handler function that can be used in
CDF Functions or called directly, maintaining compatibility with the CDF
workflow format while using the existing KeyExtractionEngine.
"""

from typing import Any, Dict

try:
    from cognite.client import CogniteClient

    CDF_AVAILABLE = True
except ImportError:
    CDF_AVAILABLE = False

from .cdf_adapter import convert_cdf_config_to_engine_config, load_config_from_yaml
from .engine.key_extraction_engine import KeyExtractionEngine

# Try to import CDF config loader - fallback if not available
try:
    from .config import Config

    CDF_CONFIG_AVAILABLE = True
except ImportError:
    CDF_CONFIG_AVAILABLE = False
    Config = None


def handle(data: Dict[str, Any], client: CogniteClient = None) -> Dict[str, Any]:
    """
    CDF-compatible handler function for key extraction.

    This function follows the CDF function handler pattern and can be used
    in CDF Functions or called directly.

    Args:
        data: Dictionary containing:
            - config: Workflow-provided config payload
            - logLevel: Optional log level (DEBUG, INFO, WARNING, ERROR)
        client: CogniteClient instance (required for CDF querying/writing mode)

    Returns:
        Dictionary with status and result information
    """
    logger = None
    cdf_config = None

    try:
        # Initialize logging
        loglevel = data.get("logLevel", "INFO")
        verbose = data.get("verbose", False)

        # Use CDF logger from common module
        from .common.logger import CogniteFunctionLogger

        logger = CogniteFunctionLogger(loglevel, verbose=verbose)
        logger.info(f"Starting key extraction with loglevel = {loglevel} and verbose {"ON" if verbose else "OFF"}")

        # Load config from workflow payload
        if "config" in data:
            provided_config = data["config"]

            # Workflow-provided config can be either:
            # - full pipeline-shaped payload {externalId, config: {parameters, data}}
            # - config-only payload {parameters, data}
            # - already-converted engine config
            if CDF_CONFIG_AVAILABLE and isinstance(provided_config, dict):
                unwrapped = provided_config.get("config", provided_config)
                try:
                    cdf_config = Config.model_validate(unwrapped)
                    engine_config = convert_cdf_config_to_engine_config(cdf_config)
                    logger.info("Using workflow-provided config")
                    data["_cdf_config"] = cdf_config
                    data["_engine_config"] = engine_config
                except Exception:
                    # Fall back to direct engine config mode
                    engine_config = provided_config
                    cdf_config = None
                    logger.info("Using provided engine config directly")
            else:
                # Direct config provided (for standalone usage)
                engine_config = provided_config
                cdf_config = None
                logger.info("Using provided config directly")
        else:
            raise ValueError(
                "Missing required key 'config' in input data"
            )

        # Initialize engine with typed Config (pass cdf_config directly)
        engine = KeyExtractionEngine(cdf_config, logger=logger)
        data["_engine"] = engine
        data["_cdf_config"] = cdf_config

        # Call pipeline function (support package and script execution)
        try:
            from .pipeline import key_extraction
        except ImportError:
            from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.pipeline import (
                key_extraction,
            )

        key_extraction(
            client=client,
            logger=logger,
            data=data,
            engine=engine,
            cdf_config=cdf_config if (CDF_CONFIG_AVAILABLE and client) else None,
        )

        return {"status": "succeeded", "data": data}

    except Exception as eoti:
        message = f"Key extraction pipeline failed: {eoti!s}"

        if logger:
            logger.error(message)
        else:
            print(f"[ERROR] {message}")

        return {"status": "failure", "message": message}


def run_locally():
    """Run handler locally for testing using a config file."""
    import os

    from dotenv import load_dotenv

    load_dotenv()

    required_envvars = (
        "CDF_PROJECT",
        "CDF_CLUSTER",
        "IDP_TENANT_ID",
        "IDP_CLIENT_ID",
        "IDP_CLIENT_SECRET",
    )

    if missing := [envvar for envvar in required_envvars if envvar not in os.environ]:
        raise ValueError(f"Missing required environment variables: {missing}")

    # Create client
    if not CDF_AVAILABLE:
        raise ImportError("CogniteClient not available. Install cognite-sdk.")

    from cognite.client import ClientConfig
    from cognite.client.credentials import OAuthClientCredentials

    cdf_project = os.getenv("CDF_PROJECT")
    cdf_cluster = os.getenv("CDF_CLUSTER")
    tenant_id = os.getenv("IDP_TENANT_ID")
    client_id = os.getenv("IDP_CLIENT_ID")
    client_secret = os.getenv("IDP_CLIENT_SECRET")

    scopes = [f"https://{cdf_cluster}.cognitedata.com/.default"]
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

    creds = OAuthClientCredentials(
        token_url=token_url,
        client_id=client_id,
        client_secret=client_secret,
        scopes=scopes,
    )

    cnf = ClientConfig(
        client_name="KeyExtraction_Local",
        project=cdf_project,
        base_url=f"https://{cdf_cluster}.cognitedata.com",
        credentials=creds,
    )

    client = CogniteClient(cnf)

    # Test data (workflow-style config payload)
    config_path = os.getenv(
        "KEY_EXTRACTION_CONFIG_PATH",
        str(
            Path(__file__).resolve().parents[2]
            / "extraction_pipelines"
            / "ctx_key_extraction_GEL_prod.config.yaml"
        ),
    )
    workflow_config = load_config_from_yaml(config_path, validate=False)
    data = {
        "logLevel": "DEBUG",
        "config": workflow_config,
    }

    # Run handler
    print("Starting key extraction pipeline...")
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
