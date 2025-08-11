"""
Optimized Metadata Update Handler

This handler provides optimized metadata update functionality with enhanced
performance monitoring, error handling, and logging as the default implementation.
"""

import os
import sys
from pathlib import Path
from typing import Dict, Any

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials

sys.path.append(str(Path(__file__).parent))

from logger import CogniteFunctionLogger
from config import load_config_parameters
from pipeline import metadata_update
from metadata_optimizations import (
    time_operation,
    monitor_memory_usage,
    cleanup_memory,
    PerformanceBenchmark,
    optimize_metadata_processing,
)


def handle(data: Dict[str, Any], client: CogniteClient) -> Dict[str, Any]:
    """
    Optimized metadata update handler with enhanced performance and monitoring.

    This is now the default implementation, providing all optimizations automatically.

    Args:
        data: Function data containing extraction pipeline configuration
        client: Authenticated CogniteClient instance

    Returns:
        Dict containing status and result information
    """

    logger = None
    benchmark = None

    try:
        # Apply global optimizations
        optimize_metadata_processing()

        # Initialize enhanced logging and monitoring
        loglevel = data.get("logLevel", "INFO")
        logger = CogniteFunctionLogger(loglevel)
        benchmark = PerformanceBenchmark(logger)

        logger.info(f"🚀 Starting OPTIMIZED metadata update with loglevel = {loglevel}")
        logger.info(
            f"📝 Reading parameters from extraction pipeline config: {data.get('ExtractionPipelineExtId')}"
        )

        # Monitor initial memory usage
        monitor_memory_usage(logger, "Handler start")

        # Load configuration with timing
        config = benchmark.benchmark_function(
            "Configuration loading", load_config_parameters, client, data
        )
        logger.debug("✅ Configuration loaded successfully")

        # Execute optimized metadata update pipeline
        with time_operation("Complete metadata update pipeline", logger):
            benchmark.benchmark_function(
                "Metadata update pipeline",
                metadata_update,
                client,
                logger,
                data,
                config,
            )

        # Final cleanup and monitoring
        cleanup_memory()
        monitor_memory_usage(logger, "Handler end")

        # Log performance summary
        if benchmark:
            benchmark.log_summary()

        logger.info("🎉 Optimized metadata update completed successfully!")
        return {"status": "succeeded", "data": data}

    except Exception as e:
        message = f"Optimized metadata update failed: {e!s}"

        if logger:
            logger.error(message)
            if benchmark:
                benchmark.log_summary()
        else:
            print(f"[ERROR] {message}")

        return {"status": "failure", "message": message}


def run_locally():
    """
    Run the optimized metadata update locally with enhanced error handling.
    """

    print("🚀 Optimized Metadata Update Pipeline")
    print("=" * 50)

    try:
        # Apply optimizations
        optimize_metadata_processing()

        # Validate environment variables
        required_envvars = (
            "CDF_PROJECT",
            "CDF_CLUSTER",
            "IDP_CLIENT_ID",
            "IDP_CLIENT_SECRET",
            "IDP_TOKEN_URL",
        )
        if missing := [
            envvar for envvar in required_envvars if envvar not in os.environ
        ]:
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
        data = {
            "logLevel": "INFO",
            "ExtractionPipelineExtId": "ep_ctx_entity_matching_metadata_update",
        }

        print("🔄 Starting optimized metadata update...")
        result = handle(data, client)

        if result["status"] == "succeeded":
            print("✅ Optimized metadata update completed successfully!")
        else:
            print(
                f"❌ Metadata update failed: {result.get('message', 'Unknown error')}"
            )

        return result

    except Exception as e:
        print(f"❌ Failed to run metadata update: {e}")
        import traceback

        traceback.print_exc()
        return {"status": "failure", "message": str(e)}


if __name__ == "__main__":
    result = run_locally()
    print(f"\n📊 Final result: {result}")
