"""
Open Industrial Data Sync Handler

Synchronizes time series data from Cognite's Open Industrial Data (publicdata project)
to the target CDF project with time offset to simulate real-time data.

Replicates functionality from original fn_open_id_feeder
"""

from cognite.client import CogniteClient
from dependencies import (
    create_config_and_client,
    create_data_sync_service,
    create_logger,
    create_oid_client,
)


def handle(client: CogniteClient, secrets: dict) -> dict:
    """
    Main handler for Open Industrial Data synchronization
    
    Args:
        client: Authenticated CogniteClient for target project
        secrets: Secrets dict containing openIdClientSecret
        
    Returns:
        dict: Execution summary with statistics
    """
    try:
        # Load configuration from module's config file
        config, client = create_config_and_client(client, secrets)
        
        # Create logger
        logger = create_logger(config)
        
        # Create OID client (connects to publicdata, not target project)
        oid_client = create_oid_client(config, logger)
        
        # Create data sync service
        sync_service = create_data_sync_service(client, oid_client, config, logger)
        
        # Run synchronization (matches original function behavior)
        sync_service.run_full_sync()
        
        # Return success with stats
        return {
            "success": True,
            "time_series_synced": logger.stats["ts_synced"],
            "datapoints_inserted": logger.stats["dps_inserted"],
            "errors": logger.stats["errors"]
        }
        
    except Exception as e:
        error_msg = f"Function failed: {str(e)}"
        print(f"ERROR: {error_msg}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "error": error_msg
        }

