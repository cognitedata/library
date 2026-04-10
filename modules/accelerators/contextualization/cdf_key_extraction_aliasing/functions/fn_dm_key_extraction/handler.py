"""
CDF Handler for Key Extraction

This module provides a CDF-compatible handler function that can be used in
CDF Functions or called directly, maintaining compatibility with the CDF
workflow format while using the existing KeyExtractionEngine.
"""

import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict

try:
    from cognite.client import CogniteClient

    CDF_AVAILABLE = True
except ImportError:
    CDF_AVAILABLE = False

from cdf_fn_common.function_logging import resolve_function_logger
from cdf_fn_common.scope_document_dm import ensure_key_extraction_config_from_scope_dm
from cdf_adapter import convert_cdf_config_to_engine_config, load_config_from_yaml
from .engine.key_extraction_engine import KeyExtractionEngine

# Try to import CDF config loader - fallback if not available
try:
    from config import Config

    CDF_CONFIG_AVAILABLE = True
except ImportError:
    CDF_CONFIG_AVAILABLE = False
    Config = None


def handle(
    data: Dict[str, Any],
    client: CogniteClient = None,
) -> Dict[str, Any]:
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
    log: Any = None
    cdf_config = None

    try:
        loglevel = data.get("logLevel", "INFO")
        verbose = data.get("verbose", False)
        log = resolve_function_logger(data, None, strict_level_names=True)
        verbose_label = "ON" if verbose else "OFF"
        log.info(
            f"Starting key extraction with loglevel = {loglevel} and verbose {verbose_label}"
        )

        if not client:
            raise ValueError("CogniteClient is required for CDF key extraction")

        ensure_key_extraction_config_from_scope_dm(
            data, client, incremental_change_processing=True
        )

        # Load config from workflow payload
        if "config" in data:
            provided_config = data["config"]

            # Workflow-provided config can be either:
            # - full pipeline-shaped payload {externalId, config: {parameters, data}}
            # - config-only payload {parameters, data}
            # - already-converted engine config
            if CDF_CONFIG_AVAILABLE and isinstance(provided_config, dict):
                # If the workflow provided a named config, keep it for logging/auditing.
                # NOTE: This is NOT an Extraction Pipeline external id.
                if provided_config.get("externalId"):
                    data["workflow_config_external_id"] = str(provided_config.get("externalId"))

                unwrapped = provided_config.get("config", provided_config)
                try:
                    cdf_config = Config.model_validate(unwrapped)
                    engine_config = convert_cdf_config_to_engine_config(cdf_config)
                    log.info("Using workflow-provided config")
                except Exception as e:
                    # Fall back to dict-based workflow config parsing (no pydantic dependency on the
                    # exact schema of extraction rules). This is the common case for workflow-provided
                    # YAML payloads.
                    log.warning(
                        "Failed to parse workflow config with pydantic; "
                        f"falling back to dict-based parsing: {e!s}"
                    )

                    if not isinstance(unwrapped, dict):
                        raise

                    from cdf_adapter import _convert_yaml_direct_to_engine_config
                    from config import SourceViewConfig

                    engine_config = _convert_yaml_direct_to_engine_config(unwrapped)

                    params = dict((unwrapped.get("parameters") or {}) if isinstance(unwrapped.get("parameters"), dict) else {})
                    data_section = dict((unwrapped.get("data") or {}) if isinstance(unwrapped.get("data"), dict) else {})

                    # Build a minimal object with the attributes the pipeline expects.
                    source_views_raw = data_section.get("source_views", []) or []
                    source_views = [SourceViewConfig.model_validate(v) for v in source_views_raw]
                    cdf_config = SimpleNamespace(
                        parameters=SimpleNamespace(**params),
                        data=SimpleNamespace(
                            source_views=source_views,
                            extraction_rules=data_section.get("extraction_rules", []) or [],
                        ),
                    )

                    log.info("Using workflow-provided config (dict-based)")
            else:
                # Direct config provided (for standalone usage)
                engine_config = provided_config
                cdf_config = None
                log.info("Using provided config directly")
        else:
            raise ValueError(
                "Missing required key 'config' in input data"
            )

        # Initialize engine (do not store in return payload)
        engine = KeyExtractionEngine(engine_config)

        from pipeline import key_extraction

        key_extraction(
            client=client,
            logger=log,
            data=data,
            engine=engine,
            cdf_config=cdf_config if (CDF_CONFIG_AVAILABLE and client) else None,
        )

        # CDF Functions requires the returned object to be JSON-serializable.
        # The pipeline may store non-serializable objects (Enums, engines, pydantic models) in `data`,
        # so only return a small summary here.
        entities_keys_extracted = data.get("entities_keys_extracted", {}) or {}
        return {
            "status": "succeeded",
            "summary": {
                "entities_processed": len(entities_keys_extracted)
                if isinstance(entities_keys_extracted, dict)
                else None,
                "workflow_config_external_id": data.get("workflow_config_external_id"),
            },
        }

    except Exception as eoti:
        message = f"Key extraction pipeline failed: {eoti!s}"

        if log:
            log.error(message)
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
    config_path = os.getenv("KEY_EXTRACTION_CONFIG_PATH")
    if not config_path:
        raise ValueError(
            "Missing KEY_EXTRACTION_CONFIG_PATH env var for local run. "
            "Provide a path to a workflow-shaped YAML config file."
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
