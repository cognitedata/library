"""Handler for the metadata update function, which writes aliases ahead of entity matching."""

import os
import sys
import time
from pathlib import Path
from typing import Any

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials

sys.path.append(str(Path(__file__).parent))

from alias_optimizations import time_operation
from config import FunctionInput, format_config_for_log, load_config_parameters
from logger import CogniteFunctionLogger
from pipeline import metadata_update

# ---------------------------------------------------------------------------
# Usage tracking
# ---------------------------------------------------------------------------
_SOURCE = "dp:contextualization:cdf_entity_matching"
_DP_VERSION = "1"
_TRACKER_VERSION = "1"


def _report_usage(client: CogniteClient) -> None:
    try:
        import threading

        from mixpanel import Consumer, Mixpanel

        mp = Mixpanel("8f28374a6614237dd49877a0d27daa78", consumer=Consumer(api_host="api-eu.mixpanel.com"))
        distinct_id = f"{client.config.project}:{client.config.cdf_cluster}"

        def _send() -> None:
            mp.track(
                distinct_id,
                "fn-handle",
                {
                    "source": _SOURCE,
                    "tracker_version": _TRACKER_VERSION,
                    "dp_version": _DP_VERSION,
                    "type": "py-function",
                    "cdf_cluster": client.config.cdf_cluster,
                    "cdf_project": client.config.project,
                },
            )

        threading.Thread(target=_send, daemon=True).start()
    except Exception:
        # Usage tracking is best-effort; must not affect the handler.
        pass


def handle(data: dict[str, Any], client: CogniteClient) -> dict[str, Any]:
    """Update aliases as configured by the extraction pipeline named in the data.

    Args:
        data: Function data containing extraction pipeline configuration
        client: Authenticated CogniteClient instance

    Returns:
        Status of the run, and the input data.

    Raises:
        pydantic.ValidationError: When the input is invalid.
        Exception: Whatever the run raised, so CDF records the call as failed.
    """
    function_input = FunctionInput.model_validate(data)
    _report_usage(client)
    logger = None

    try:
        logger = CogniteFunctionLogger(function_input.log_level)

        logger.info(f"Starting Aliases Update with loglevel = {function_input.log_level}")
        logger.info(f"Reading parameters from extraction pipeline config: {function_input.extraction_pipeline_ext_id}")

        load_start = time.time()
        config = load_config_parameters(client, data)
        load_duration = time.time() - load_start
        logger.info(f"Configuration loading took {load_duration:.2f}s")
        logger.info(format_config_for_log(config))

        with time_operation("Complete metadata update pipeline", logger):
            metadata_update(client, logger, data, config)

        logger.info("Aliases Update completed successfully!")
        return {"status": "succeeded", "data": data}

    except Exception as e:
        message = f"Aliases Update failed: {e!s}"

        if logger:
            logger.error(message)
        else:
            print(f"[ERROR] {message}")

        # A returned value is a succeeded call to CDF, so the workflow would go on to match on stale aliases.
        raise


def run_locally() -> dict[str, Any]:
    """
    Run the optimized metadata update locally with enhanced error handling.
    """

    print("🚀 Optimized Metadata Update Pipeline")
    print("=" * 50)

    try:
        # Validate environment variables
        required_envvars = ("CDF_PROJECT", "CDF_CLUSTER", "IDP_CLIENT_ID", "IDP_CLIENT_SECRET", "IDP_TOKEN_URL")
        if missing := [envvar for envvar in required_envvars if envvar not in os.environ]:
            raise ValueError(f"Missing required environment variables: {missing}")

        # Extract configuration
        cdf_project_name = os.environ["CDF_PROJECT"]
        cdf_cluster = os.environ["CDF_CLUSTER"]
        client_id = os.environ["IDP_CLIENT_ID"]
        client_secret = os.environ["IDP_CLIENT_SECRET"]
        token_uri = os.environ["IDP_TOKEN_URL"]
        base_url = f"https://{cdf_cluster}.cognitedata.com"

        # Initialize client
        client = CogniteClient(
            ClientConfig(
                client_name="Optimized Metadata Update Pipeline",
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

        # Test data
        data = {"logLevel": "INFO", "ExtractionPipelineExtId": "ep_ctx_aliases_update"}

        print("🔄 Starting optimized metadata update...")
        result = handle(data, client)

        if result["status"] == "succeeded":
            print("✅ Optimized metadata update completed successfully!")
        else:
            print(f"❌ Metadata update failed: {result.get('message', 'Unknown error')}")

        return result

    except Exception as e:
        print(f"❌ Failed to run metadata update: {e}")
        import traceback

        traceback.print_exc()
        return {"status": "failure", "message": str(e)}


if __name__ == "__main__":
    result = run_locally()
    print(f"\n📊 Final result: {result}")
