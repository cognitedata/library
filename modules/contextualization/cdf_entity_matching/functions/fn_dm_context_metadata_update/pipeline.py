"""
Optimized Metadata Update Pipeline

This module provides optimized metadata update functionality for timeseries and assets
with improved performance, caching, batch processing, and error handling.
"""

import sys
import traceback
from pathlib import Path
from typing import Any, Optional, List, Dict, Tuple, Union
import gc

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import (
    NodeApply,
    NodeOrEdgeData,
    ViewId,
    NodeList,
    Node,
)
from cognite.client.data_classes.filters import In, HasData, Equals
from cognite.client.utils._text import shorten
from cognite.client.data_classes import ExtractionPipelineRun, Row
from cognite.client import data_modeling as dm
from cognite.client.exceptions import CogniteAPIError

from config import Config, ViewPropertyConfig
from logger import CogniteFunctionLogger
from constants import (
    BATCH_SIZE,
    TS_NODE,
    ASSET_NODE,
)

# Import optimizations
from metadata_optimizations import (
    time_operation,
    monitor_memory_usage,
    cleanup_memory,
    OptimizedMetadataProcessor,
    BatchProcessor,
    PerformanceBenchmark,
    optimize_metadata_processing
)

sys.path.append(str(Path(__file__).parent))


def metadata_update(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    data: Dict[str, Any],
    config: Config
) -> None:
    """
    Optimized main function for metadata update process.
    
    Includes performance optimizations, better error handling, and monitoring.
    """
    
    # Apply global optimizations
    optimize_metadata_processing()
    
    # Initialize performance monitoring
    benchmark = PerformanceBenchmark(logger)
    
    try:
        pipeline_ext_id = data["ExtractionPipelineExtId"]
        
        # Monitor initial memory usage
        monitor_memory_usage(logger, "Pipeline start")
        
        # Process configuration
        with time_operation("Configuration processing", logger):
            batch_size = BATCH_SIZE
            if config.parameters.debug:
                logger.debug("Debug mode enabled - processing limited data")
                batch_size = 100
            
            # Initialize processors
            metadata_processor = OptimizedMetadataProcessor(logger)
            batch_processor = BatchProcessor(batch_size=batch_size)
        
        # Process timeseries
        with time_operation("Timeseries processing", logger):
            ts_updates = benchmark.benchmark_function(
                "Process timeseries metadata",
                _process_timeseries_optimized,
                client, logger, config, metadata_processor, batch_processor
            )
            
            if ts_updates > 0:
                msg = f"Successfully updated {ts_updates} timeseries metadata entries"
                update_pipeline_run(client, logger, pipeline_ext_id, "success", msg)
            else:
                msg = "No timeseries metadata updates needed"
                update_pipeline_run(client, logger, pipeline_ext_id, "success", msg)
        
        # Process assets
        with time_operation("Asset processing", logger):
            asset_updates = benchmark.benchmark_function(
                "Process asset metadata",
                _process_assets_optimized,
                client, logger, config, metadata_processor, batch_processor
            )
            
            if asset_updates > 0:
                msg = f"Successfully updated {asset_updates} asset metadata entries"
                update_pipeline_run(client, logger, pipeline_ext_id, "success", msg)
            else:
                msg = "No asset metadata updates needed"
                update_pipeline_run(client, logger, pipeline_ext_id, "success", msg)
        
        # Log performance statistics
        processor_stats = metadata_processor.get_stats()
        logger.info(f"📊 Processing Stats: {processor_stats['processed']} processed, "
                   f"{processor_stats['updated']} updated, "
                   f"{processor_stats['update_rate']:.2%} update rate, "
                   f"{processor_stats['cache_hit_rate']:.2%} cache hit rate")
        
        # Final cleanup and monitoring
        cleanup_memory()
        monitor_memory_usage(logger, "Pipeline end")
        benchmark.log_summary()
        
    except Exception as e:
        msg = f"Optimized metadata update failed: {e!s}, traceback:\n{traceback.format_exc()}"
        logger.error(msg)
        update_pipeline_run(client, logger, pipeline_ext_id, "failure", msg)
        raise


def _process_timeseries_optimized(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    config: Config,
    metadata_processor: OptimizedMetadataProcessor,
    batch_processor: BatchProcessor
) -> int:
    """Process timeseries metadata with optimizations"""
    
    total_updates = 0
    
    ts_view_id = config.data.job.timeseries_view.as_view_id()
    
    while True:
        # Get new timeseries
        with time_operation("Fetch timeseries batch", logger):
            new_timeseries = get_new_items(
                client, logger, ts_view_id, config, TS_NODE
            )
        
        if not new_timeseries or len(new_timeseries) == 0:
            logger.info("No more timeseries to process")
            break
        
        # Process timeseries in batches
        with time_operation(f"Process {len(new_timeseries)} timeseries", logger):
            updates = []
            
            for node in new_timeseries:
                update = metadata_processor.process_timeseries_metadata(
                    node, ts_view_id, config.data.job.timeseries_view.instance_space
                )
                if update:
                    updates.append(update)
            
            # Apply updates in batches
            if updates:
                batch_updates = batch_processor.apply_updates_in_batches(
                    client, updates, logger
                )
                total_updates += batch_updates
                logger.info(f"Applied {batch_updates} timeseries updates")
            else:
                logger.info("No timeseries updates needed") 
                break
        
        
        # Break if debug mode
        if config.parameters.debug:
            break
        
        # Memory cleanup
        cleanup_memory()
    
    return total_updates


def _process_assets_optimized(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    config: Config,
    metadata_processor: OptimizedMetadataProcessor,
    batch_processor: BatchProcessor
) -> int:
    """Process asset metadata with optimizations"""
    
    total_updates = 0    
    asset_view_id = config.data.job.asset_view.as_view_id()
    
    while True:
        # Get new assets
        with time_operation("Fetch assets batch", logger):
            new_assets = get_new_items(
                client, logger, asset_view_id, config, ASSET_NODE
            )
        
        if not new_assets or len(new_assets) == 0:
            logger.info("No more assets to process")
            break
        
        # Process assets in batches
        with time_operation(f"Process {len(new_assets)} assets", logger):
            updates = []
            
            for node in new_assets:
                update = metadata_processor.process_asset_metadata(
                    node, asset_view_id, config.data.job.asset_view.instance_space
                )
                if update:
                    updates.append(update)
            
            # Apply updates in batches
            if updates:
                batch_updates = batch_processor.apply_updates_in_batches(
                    client, updates, logger
                )
                total_updates += batch_updates
                logger.info(f"Applied {batch_updates} asset updates")
            else:
                logger.info("No asset updates needed") 
                break
        
        
        # Break if debug mode
        if config.parameters.debug:
            break
        
        # Memory cleanup
        cleanup_memory()
    
    return total_updates


def update_pipeline_run(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    xid: str,
    status: str,
    msg: Optional[str] = None
) -> None:
    """
    Update extraction pipeline run status with enhanced error handling
    """
    
    try:
        if status == "success":
            logger.info(msg or "Success")
        else:
            logger.error(msg or "Error")
        
        # Truncate message to avoid API limits
        truncated_msg = shorten(msg, 1000) if msg else ""
        
        client.extraction_pipelines.runs.create(
            ExtractionPipelineRun(
                extpipe_external_id=xid,
                status=status,
                message=truncated_msg
            )
        )
        
    except Exception as e:
        logger.warning(f"Failed to update pipeline run: {e}")



def get_new_items(
    client: CogniteClient,
    logger: CogniteFunctionLogger,
    view_id: ViewId,
    config: Config,
    node_type: str,
) -> Optional[NodeList[Node]]:
    """
    Get new items with enhanced error handling and retry logic
    """
    
    try:
        logger.debug(f"Getting new {node_type} from view: {view_id} ")
        
        # Set the filter for the query
        if node_type == TS_NODE:
            view_config = config.data.job.timeseries_view
            debug_item = config.parameters.debug_timeseries if config.parameters.debug else None
            filter_query = get_ts_filter(view_config, debug_item, config.parameters.run_all, logger)
        else:  # ASSET_NODE
            view_config = config.data.job.asset_view
            filter_query = get_asset_filter(view_config, logger, config.parameters.run_all)
        
        # Query with retry logic
        max_retries = 3
        for attempt in range(max_retries):
            try:
                result = client.data_modeling.instances.list(
                    instance_type="node",
                    sources=[view_id],
                    filter=filter_query,
                    limit=BATCH_SIZE
                )
                
                logger.info(f"Retrieved {len(result)} ")
                return result
                
            except CogniteAPIError as e:
                if e.code == 400 and attempt < max_retries - 1:
                    logger.warning(f"API error (attempt {attempt + 1}): {e}")
                    continue
                else:
                    raise
        
        return None
        
    except Exception as e:
        logger.error(f"Failed to get new items: {e}")
        return None


def get_ts_filter(
    view_config: ViewPropertyConfig,
    debug_ts: Optional[str],
    run_all: bool,
    logger: CogniteFunctionLogger,
) -> dm.filters.Filter:
    """
    Create timeseries filter with enhanced logic
    """
    
    filters: List[dm.filters.Filter] = [HasData(views=[view_config.as_view_id()])]
    

        # Check if the view entity already is matched or not
    if not run_all:  
        has_alias = dm.filters.Exists(view_config.as_property_ref("aliases"))
        not_alias = dm.filters.Not(has_alias)
        filters.append(not_alias)
        dbg_msg = f"Entity filtering on: 'aliases' - NOT EXISTS"
    
    if debug_ts:
        logger.debug(f"Debug timeseries filter: {dbg_msg} {debug_ts}")
        filters.append(Equals(view_config.external_id, debug_ts))
    
    return dm.filters.And(*filters) if len(filters) > 1 else filters[0]



def get_asset_filter(
    view_config: ViewPropertyConfig,
    logger: CogniteFunctionLogger,
    run_all: bool,
) -> dm.filters.Filter:
    """
    Create asset filter with enhanced logic
    """
    
    logger.debug("Creating asset filter")

    filters: List[dm.filters.Filter] = [HasData(views=[view_config.as_view_id()])]
    
 
    if not run_all:  
        has_alias = dm.filters.Exists(view_config.as_property_ref("aliases"))
        not_alias = dm.filters.Not(has_alias)
        filters.append(not_alias)

    return dm.filters.And(*filters) if len(filters) > 1 else filters[0]


# Export all functions for backward compatibility
__all__ = [
    'metadata_update',
    'update_pipeline_run',
    'get_new_items',
    'get_ts_filter',
    'get_asset_filter'
]