import os
import sys
from pathlib import Path

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import OAuthClientCredentials

sys.path.append(str(Path(__file__).parent))

from logger import CogniteFunctionLogger
from config import load_config_parameters
from pipeline import asset_entity_matching

# IMPORT OPTIMIZATIONS
from pipeline_optimizations import (
    time_operation,
    monitor_memory_usage,
    cleanup_memory,
    patch_existing_pipeline,
    PerformanceBenchmark
)


def handle(data: dict, client: CogniteClient) -> dict:
    """
    OPTIMIZED Entity Matching Handler
    
    This function includes performance optimizations for improved speed and monitoring.
    """
    logger = None
    benchmark = None
    
    try:
        # Apply global optimizations
        patch_existing_pipeline()
        
        # Initialize logger and performance monitoring
        loglevel = data.get("logLevel", "INFO")
        logger = CogniteFunctionLogger(loglevel)
        benchmark = PerformanceBenchmark(logger)
        
        logger.info(f"Starting OPTIMIZED entity matching with loglevel = {loglevel}")
        logger.info(f"Reading parameters from extraction pipeline config: {data.get('ExtractionPipelineExtId')}")
        
        # Monitor initial memory usage
        monitor_memory_usage(logger, "Handler start")
        
        # Load configuration with timing
        config = benchmark.benchmark_function(
            "Config loading",
            load_config_parameters,
            client, data
        )
        logger.debug("Loaded config successfully")
        
        # Run the main pipeline with optimizations and timing
        with time_operation("Complete pipeline execution", logger):
            benchmark.benchmark_function(
                "Pipeline execution",
                asset_entity_matching,
                client, logger, data, config
            )
        
        # Final cleanup and monitoring
        cleanup_memory()
        monitor_memory_usage(logger, "Handler end")
        
        # Log performance summary
        if benchmark:
            benchmark.log_summary()
        
        logger.info("Optimized entity matching completed successfully!")
        return {"status": "succeeded", "data": data}
        
    except Exception as e:
        message = f"Optimized pipeline failed: {e!s}"
        
        if logger:
            logger.error(message)
            if benchmark:
                benchmark.log_summary()
        else:
            print(f"[ERROR] {message}")  # Fallback logging if logger creation failed
            
        return {"status": "failure", "message": message}



def run_locally():
    """
    OPTIMIZED Local Runner
    
    Enhanced with better error handling and performance monitoring.
    """
    
    # Apply optimizations first
    patch_existing_pipeline()
    
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
            client_name="Optimized Toolkit Entity Matching Pipeline",
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
        "ExtractionPipelineExtId": "ep_ctx_timeseries_Springfield_springfield_entity_matching"
    }
    
    # Run optimized handler
    print("Starting optimized entity matching pipeline...")
    result = handle(data, client)
    
    if result["status"] == "succeeded":
        print("Optimized pipeline completed successfully!")
    else:
        print(f"Pipeline failed: {result.get('message', 'Unknown error')}")
    
    return result


if __name__ == "__main__":
    print("Entity Matching Pipeline with Optimizations")
    print("=" * 50)
    try:
        result = run_locally()
        print(f"Final result: {result}")
    except Exception as e:
        print(f"Failed: {e}")
        import traceback
        traceback.print_exc() 