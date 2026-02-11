"""
Test script to validate the Open Industrial Data sync function locally
This mirrors the logic from ReadFromOpenIndustrialData.ipynb
"""

import os
import sys

# Add function directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "functions", "fn_oid_sync"))

from datetime import datetime, timedelta

from services.DataSyncService import OIDPublicClient  # pyright: ignore[reportMissingImports]
from services.LoggerService import CompactLogger  # pyright: ignore[reportMissingImports]
from utils.DataStructures import OIDConfig  # pyright: ignore[reportMissingImports]


def test_oid_connection():
    """Test connection to Open Industrial Data"""
    print("=" * 60)
    print("TESTING OPEN INDUSTRIAL DATA CONNECTION")
    print("=" * 60)
    
    # Create config
    config = OIDConfig(
        instance_space="springfield_instances",
        oid_tenant_id="",
        oid_client_id="",
        oid_client_secret="",
        timeseries_view_space="sp_enterprise_process_industry",
        timeseries_view_external_id="Enterprise_TimeSeries",
        timeseries_view_version="v1",
    )
    
    # Create logger
    logger = CompactLogger(name="OID-Test", log_level="INFO")
    
    try:
        # Create OID client
        logger.info("Creating OID client...")
        oid_client = OIDPublicClient(config, logger)
        
        # Test 1: List time series
        logger.info("Test 1: Fetching PI time series...")
        ts_list = oid_client.client.time_series.list(
            limit=10,
            advanced_filter={
                "prefix": {
                    "property": ["externalId"],
                    "value": "pi:"
                }
            }
        )
        logger.info(f"✓ Found {len(ts_list)} time series")
        for i, ts in enumerate(ts_list[:3]):
            logger.info(f"  {i+1}. {ts.external_id}")
        
        # Test 2: Check datapoints availability
        logger.info("\nTest 2: Checking datapoints availability...")
        if len(ts_list) > 0:
            test_ts = ts_list[0]
            end_time = datetime.now()
            start_time = end_time - timedelta(days=7)
            
            dps = oid_client.client.time_series.data.retrieve(
                external_id=test_ts.external_id,
                start=start_time,
                end=end_time,
                limit=10
            )
            
            if dps and len(dps) > 0:
                logger.info(f"✓ Found {len(dps)} datapoints for {test_ts.external_id}")
            else:
                logger.warning(f"⚠ No recent datapoints for {test_ts.external_id}")
        
        # Test 3: List assets
        logger.info("\nTest 3: Fetching assets...")
        assets = oid_client.client.assets.list(limit=5)
        logger.info(f"✓ Found {len(assets)} assets")
        for i, asset in enumerate(assets[:3]):
            logger.info(f"  {i+1}. {asset.name}")
        
        # Test 4: List files
        logger.info("\nTest 4: Fetching files...")
        files = oid_client.client.files.list(limit=5)
        logger.info(f"✓ Found {len(files)} files")
        for i, file in enumerate(files[:3]):
            logger.info(f"  {i+1}. {file.name} ({file.mime_type})")
        
        logger.info("\n" + "=" * 60)
        logger.info("✓ ALL TESTS PASSED!")
        logger.info("=" * 60)
        
        return True
        
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_oid_connection()
    sys.exit(0 if success else 1)

