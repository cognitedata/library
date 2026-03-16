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

from .cdf_adapter import convert_cdf_config_to_engine_config
from .engine.key_extraction_engine import KeyExtractionEngine

# Try to import CDF config loader - fallback if not available
try:
    from .config import Config, load_config_parameters

    CDF_CONFIG_AVAILABLE = True
except ImportError:
    CDF_CONFIG_AVAILABLE = False
    load_config_parameters = None
    Config = None


def handle(data: Dict[str, Any], client: CogniteClient = None) -> Dict[str, Any]:
    """
    CDF-compatible handler function for key extraction.

    This function follows the CDF function handler pattern and can be used
    in CDF Functions or called directly.

    Args:
        data: Dictionary containing:
            - ExtractionPipelineExtId: External ID of the extraction pipeline
            - logLevel: Optional log level (DEBUG, INFO, WARNING, ERROR)
        client: CogniteClient instance (required if using CDF config loading)

    Returns:
        Dictionary with status and result information
    """
    logger = None

    try:
        # Initialize logging
        loglevel = data.get("logLevel", "INFO")
        verbose = data.get("verbose", False)

        # Use CDF logger from common module
        from .common.logger import CogniteFunctionLogger

        logger = CogniteFunctionLogger(loglevel, verbose=verbose)
        logger.info(f"Starting key extraction with loglevel = {loglevel} and verbose {"ON" if verbose else "OFF"}")

        # Load configuration from CDF extraction pipeline
        if CDF_CONFIG_AVAILABLE and client and "ExtractionPipelineExtId" in data:
            pipeline_ext_id = data["ExtractionPipelineExtId"]
            logger.info(f"Loading config from extraction pipeline: {pipeline_ext_id}")

            cdf_config = load_config_parameters(client, data)
            logger.debug(f"Loaded CDF config: {cdf_config}")

            logger = CogniteFunctionLogger(loglevel, cdf_config.parameters.verbose)

            # Convert CDF config to engine format
            # engine_config = convert_cdf_config_to_engine_config(cdf_config)
            # logger.debug(f"Converted to engine config format")

            # Store CDF config for use in pipeline
            # data["_cdf_config"] = cdf_config
            # data["_engine_config"] = engine_config

        elif "config" in data:
            # Direct config provided (for standalone usage)
            # engine_config = data["config"]
            logger.info("Using provided config directly")
        else:
            raise ValueError(
                "Either ExtractionPipelineExtId (with client) or config must be provided in data"
            )

        # Initialize engine
        engine = KeyExtractionEngine(cdf_config, logger=logger)
        data["_engine"] = engine

        # Call pipeline function
        from .pipeline import key_extraction

        key_extraction(
            client=client,
            logger=logger,
            data=data,
            engine=engine,
            cdf_config=cdf_config if CDF_CONFIG_AVAILABLE and client else None,
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
    """Run handler locally for testing (requires .env file and CDF config)."""
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
        client_name="poweruser",
        project=cdf_project,
        base_url=f"https://p001.plink.{cdf_cluster}.cognitedata.com",
        credentials=creds,
    )

    client = CogniteClient(cnf)

    # Test data
    data = {
        "logLevel": "DEBUG",
        "ExtractionPipelineExtId": "ctx_key_extraction_files_pid_aliases_rob"
        # "ExtractionPipelineExtId": "ep_ctx_refining_alias_extraction_mtz",  # Update with your pipeline ID
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
        # print(f"Final result: {result}")
    except Exception as e:
        print(f"Failed: {e}")
        import traceback

        traceback.print_exc()
