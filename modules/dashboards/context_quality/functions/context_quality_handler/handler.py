"""
Cognite Function: Contextualization Quality Metrics

Computes all quality metrics in a single function:

1. TIME SERIES METRICS:
   - Asset TS Association Rate
   - Critical Asset Coverage  
   - Unit Consistency
   - Data Freshness
   - Processing Lag

2. ASSET HIERARCHY METRICS:
   - Hierarchy Completion Rate
   - Orphan Count/Rate
   - Depth Statistics (Avg, Max)
   - Breadth Statistics (Avg Children, Std Dev)
   - Depth/Breadth Distributions

3. EQUIPMENT-ASSET METRICS:
   - Equipment Association Rate
   - Asset Equipment Coverage
   - Serial Number Completeness
   - Manufacturer Completeness
   - Type Consistency
   - Critical Equipment Contextualization

4. MAINTENANCE WORKFLOW METRICS (RMDM v1):
   - Notification → Work Order Linkage
   - Notification → Asset Linkage
   - Notification → Equipment Linkage
   - Work Order → Asset Coverage
   - Work Order → Equipment Coverage
   - Work Order Completion Rate
   - Failure Mode Documentation Rate
   - Failure Mechanism Documentation Rate
   - Failure Cause Documentation Rate
   - Asset Maintenance Coverage
   - Equipment Maintenance Coverage

5. FILE ANNOTATION METRICS (CDM CogniteDiagramAnnotation):
   - Total Annotations
   - Asset Tag Annotations
   - File Link Annotations
   - Status Distribution (approved/suggested/rejected)
   - Confidence Distribution (high/medium/low)
   - Average Confidence Score

Results are saved to a Cognite File as JSON for persistence.
"""

import logging
import time

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import ViewId

# Import from metrics modules
# Note: Using absolute import for compatibility with manual zip deployment to CDF
from metrics import (
    # Config and constants
    DEFAULT_CONFIG,
    LOG_EVERY_N_BATCHES,
    # Utilities
    format_elapsed,
    # Accumulator
    CombinedAccumulator,
    # Batch processors
    process_timeseries_batch,
    process_asset_batch,
    process_equipment_batch,
    process_notification_batch,
    process_maintenance_order_batch,
    process_failure_notification_batch,
    compute_historical_gaps_batch,
    # File annotation processors
    FileAnnotationAccumulator,
    process_annotation_batch,
    # 3D model processors
    Model3DAccumulator,
    process_asset_3d_batch,
    process_3d_object_batch,
    # Metric computers
    compute_ts_metrics,
    compute_asset_hierarchy_metrics,
    compute_equipment_metrics,
    compute_maintenance_metrics,
    compute_file_annotation_metrics,
    compute_3d_metrics,
    # Storage
    save_metrics_to_file,
    # Batch processing
    save_batch_file,
    load_and_merge_all_batches,
    delete_batch_files,
    list_batch_files,
)


# ----------------------------------------------------
# LOGGING SETUP
# ----------------------------------------------------
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(
        '%(asctime)s | %(levelname)s | %(message)s',
        datefmt='%H:%M:%S'
    ))
    logger.addHandler(handler)


# ----------------------------------------------------
# BATCH PROCESSING HELPERS
# ----------------------------------------------------

def _handle_aggregation(client: CogniteClient, config: dict, start_time: float) -> dict:
    """
    Aggregation mode: Load all batch files, merge, and compute final metrics.
    """
    logger.info("=" * 70)
    logger.info("AGGREGATION MODE: Merging batch files")
    logger.info("=" * 70)
    
    # List batch files
    batch_files = list_batch_files(client)
    if not batch_files:
        logger.error("No batch files found for aggregation")
        return {"error": "No batch files found", "status": "failed"}
    
    logger.info(f"Found {len(batch_files)} batch files to merge")
    
    # Load and merge all batches
    merged_acc = load_and_merge_all_batches(client)
    
    if not merged_acc:
        logger.error("Failed to merge batch files")
        return {"error": "Failed to merge batch files", "status": "failed"}
    
    logger.info(f"Merged accumulator: assets={merged_acc.total_assets:,}, "
               f"ts={merged_acc.total_ts:,}, equipment={merged_acc.total_equipment:,}")
    
    # Compute all metrics from merged accumulator
    logger.info("-" * 50)
    logger.info("Computing final metrics from merged data")
    logger.info("-" * 50)
    
    ts_metrics = compute_ts_metrics(merged_acc)
    hierarchy_metrics = compute_asset_hierarchy_metrics(merged_acc)
    equipment_metrics = compute_equipment_metrics(merged_acc)
    
    # Maintenance metrics
    enable_maintenance = config.get("enable_maintenance_metrics", True)
    maintenance_metrics = {}
    if enable_maintenance and merged_acc.total_notifications > 0:
        maintenance_metrics = compute_maintenance_metrics(merged_acc)
    
    # File annotation metrics (not batched currently - would need similar treatment)
    file_annotation_metrics = {}
    
    total_elapsed = time.time() - start_time
    
    # Compile final metrics
    all_metrics = {
        "metadata": {
            "computed_at": merged_acc.now.isoformat(),
            "execution_time_seconds": round(total_elapsed, 2),
            "batch_mode": True,
            "batches_merged": len(batch_files),
            "instance_counts": {
                "timeseries": {
                    "total_instances": merged_acc.total_ts_instances,
                    "unique": merged_acc.total_ts,
                    "duplicates": merged_acc.ts_duplicates,
                },
                "assets": {
                    "total_instances": merged_acc.total_asset_instances,
                    "unique": merged_acc.total_assets,
                    "duplicates": merged_acc.asset_duplicates,
                },
                "equipment": {
                    "total_instances": merged_acc.total_equipment_instances,
                    "unique": merged_acc.total_equipment,
                    "duplicates": merged_acc.equipment_duplicates,
                },
                "notifications": {
                    "total_instances": merged_acc.total_notification_instances,
                    "unique": merged_acc.total_notifications,
                    "duplicates": merged_acc.notification_duplicates,
                },
                "maintenance_orders": {
                    "total_instances": merged_acc.total_order_instances,
                    "unique": merged_acc.total_orders,
                    "duplicates": merged_acc.order_duplicates,
                },
            },
            "config": config,
        },
        "timeseries_metrics": ts_metrics,
        "hierarchy_metrics": hierarchy_metrics,
        "equipment_metrics": equipment_metrics,
        "maintenance_metrics": maintenance_metrics,
        "file_annotation_metrics": file_annotation_metrics,
    }
    
    # Save final metrics
    file_external_id = config.get("file_external_id", "contextualization_quality_metrics")
    file_name = config.get("file_name", "contextualization_quality_metrics.json")
    save_metrics_to_file(client, all_metrics, file_external_id, file_name)
    
    # Clean up batch files
    logger.info("Cleaning up batch files...")
    deleted = delete_batch_files(client)
    logger.info(f"Deleted {deleted} batch files")
    
    logger.info("=" * 70)
    logger.info(f"AGGREGATION COMPLETE in {format_elapsed(total_elapsed)}")
    logger.info("=" * 70)
    
    return all_metrics


def _handle_batch_collection(client: CogniteClient, config: dict, start_time: float) -> dict:
    """
    Batch collection mode: Process a subset of data and save intermediate results.
    """
    batch_index = config.get("batch_index", 0)
    batch_size = config.get("batch_size", 200000)
    
    logger.info("=" * 70)
    logger.info(f"BATCH COLLECTION MODE: Batch {batch_index}")
    logger.info(f"Processing up to {batch_size:,} instances per entity type")
    logger.info("=" * 70)
    
    # Calculate offset based on batch index
    offset = batch_index * batch_size
    
    chunk_size = config["chunk_size"]
    freshness_days = config["freshness_days"]
    enable_gaps = config["enable_historical_gaps"]
    enable_maintenance = config["enable_maintenance_metrics"]
    gap_sample_rate = config["gap_sample_rate"]
    gap_threshold_days = config["gap_threshold_days"]
    gap_lookback = config["gap_lookback"]
    
    # Build view IDs
    ts_view = ViewId(
        config["ts_view_space"],
        config["ts_view_external_id"],
        config["ts_view_version"]
    )
    asset_view = ViewId(
        config["asset_view_space"],
        config["asset_view_external_id"],
        config["asset_view_version"]
    )
    equipment_view = ViewId(
        config["equipment_view_space"],
        config["equipment_view_external_id"],
        config["equipment_view_version"]
    )
    notification_view = ViewId(
        config["notification_view_space"],
        config["notification_view_external_id"],
        config["notification_view_version"]
    )
    order_view = ViewId(
        config["maintenance_order_view_space"],
        config["maintenance_order_view_external_id"],
        config["maintenance_order_view_version"]
    )
    failure_notification_view = ViewId(
        config["failure_notification_view_space"],
        config["failure_notification_view_external_id"],
        config["failure_notification_view_version"]
    )
    
    # Initialize accumulator
    acc = CombinedAccumulator(freshness_days=freshness_days)
    batch_counts = {"ts": 0, "assets": 0, "equipment": 0, "notifications": 0, "orders": 0}
    
    # Track how many instances we've skipped (for offset) and processed
    instances_skipped = 0
    instances_processed = 0
    
    # ============================================================
    # Process Time Series (with offset)
    # ============================================================
    logger.info(f"[Batch {batch_index}] Processing Time Series...")
    
    for ts_batch in client.data_modeling.instances(
        chunk_size=chunk_size,
        instance_type="node",
        sources=ts_view,
    ):
        # Skip batches until we reach our offset
        batch_instance_count = len(ts_batch.data)
        if instances_skipped + batch_instance_count <= offset:
            instances_skipped += batch_instance_count
            continue
        
        batch_counts["ts"] += 1
        process_timeseries_batch(ts_batch, ts_view, acc)
        instances_processed += batch_instance_count
        
        # Historical gap analysis
        if enable_gaps and batch_counts["ts"] % gap_sample_rate == 0:
            compute_historical_gaps_batch(
                ts_batch, client, acc,
                gap_threshold_days=gap_threshold_days,
                lookback=gap_lookback
            )
        
        if batch_counts["ts"] % LOG_EVERY_N_BATCHES == 0:
            logger.info(f"[TS] Batch {batch_counts['ts']:,} | Total: {acc.total_ts:,}")
        
        if instances_processed >= batch_size:
            logger.info(f"[TS] Reached batch limit ({batch_size:,})")
            break
    
    logger.info(f"[Batch {batch_index}] TS collected: {acc.total_ts:,}")
    
    # Reset counters for assets
    instances_skipped = 0
    instances_processed = 0
    
    # ============================================================
    # Process Assets (with offset)
    # ============================================================
    logger.info(f"[Batch {batch_index}] Processing Assets...")
    
    for asset_batch in client.data_modeling.instances(
        chunk_size=chunk_size,
        instance_type="node",
        sources=asset_view,
    ):
        batch_instance_count = len(asset_batch.data)
        if instances_skipped + batch_instance_count <= offset:
            instances_skipped += batch_instance_count
            continue
        
        batch_counts["assets"] += 1
        process_asset_batch(asset_batch, asset_view, acc)
        instances_processed += batch_instance_count
        
        if batch_counts["assets"] % LOG_EVERY_N_BATCHES == 0:
            logger.info(f"[Assets] Batch {batch_counts['assets']:,} | Total: {acc.total_assets:,}")
        
        if instances_processed >= batch_size:
            logger.info(f"[Assets] Reached batch limit ({batch_size:,})")
            break
    
    logger.info(f"[Batch {batch_index}] Assets collected: {acc.total_assets:,}")
    
    # Reset counters for equipment
    instances_skipped = 0
    instances_processed = 0
    
    # ============================================================
    # Process Equipment (with offset)
    # ============================================================
    logger.info(f"[Batch {batch_index}] Processing Equipment...")
    
    for eq_batch in client.data_modeling.instances(
        chunk_size=chunk_size,
        instance_type="node",
        sources=equipment_view,
    ):
        batch_instance_count = len(eq_batch.data)
        if instances_skipped + batch_instance_count <= offset:
            instances_skipped += batch_instance_count
            continue
        
        batch_counts["equipment"] += 1
        process_equipment_batch(eq_batch, equipment_view, acc)
        instances_processed += batch_instance_count
        
        if batch_counts["equipment"] % LOG_EVERY_N_BATCHES == 0:
            logger.info(f"[Equipment] Batch {batch_counts['equipment']:,} | Total: {acc.total_equipment:,}")
        
        if instances_processed >= batch_size:
            logger.info(f"[Equipment] Reached batch limit ({batch_size:,})")
            break
    
    logger.info(f"[Batch {batch_index}] Equipment collected: {acc.total_equipment:,}")
    
    # ============================================================
    # Process Maintenance Data (with offset)
    # ============================================================
    if enable_maintenance:
        # Reset counters
        instances_skipped = 0
        instances_processed = 0
        
        logger.info(f"[Batch {batch_index}] Processing Notifications...")
        try:
            for notif_batch in client.data_modeling.instances(
                chunk_size=chunk_size,
                instance_type="node",
                sources=notification_view,
            ):
                batch_instance_count = len(notif_batch.data)
                if instances_skipped + batch_instance_count <= offset:
                    instances_skipped += batch_instance_count
                    continue
                
                batch_counts["notifications"] += 1
                process_notification_batch(notif_batch, notification_view, acc)
                instances_processed += batch_instance_count
                
                if instances_processed >= batch_size:
                    break
        except Exception as e:
            logger.warning(f"Could not process Notifications: {e}")
        
        logger.info(f"[Batch {batch_index}] Notifications collected: {acc.total_notifications:,}")
        
        # Reset counters for orders
        instances_skipped = 0
        instances_processed = 0
        
        logger.info(f"[Batch {batch_index}] Processing Orders...")
        try:
            for order_batch in client.data_modeling.instances(
                chunk_size=chunk_size,
                instance_type="node",
                sources=order_view,
            ):
                batch_instance_count = len(order_batch.data)
                if instances_skipped + batch_instance_count <= offset:
                    instances_skipped += batch_instance_count
                    continue
                
                batch_counts["orders"] += 1
                process_maintenance_order_batch(order_batch, order_view, acc)
                instances_processed += batch_instance_count
                
                if instances_processed >= batch_size:
                    break
        except Exception as e:
            logger.warning(f"Could not process Orders: {e}")
        
        logger.info(f"[Batch {batch_index}] Orders collected: {acc.total_orders:,}")
        
        # Failure notifications (no offset - typically small)
        try:
            for fn_batch in client.data_modeling.instances(
                chunk_size=chunk_size,
                instance_type="node",
                sources=failure_notification_view,
            ):
                process_failure_notification_batch(fn_batch, failure_notification_view, acc)
        except Exception as e:
            logger.warning(f"Could not process Failure Notifications: {e}")
    
    # ============================================================
    # Save batch file
    # ============================================================
    total_elapsed = time.time() - start_time
    
    batch_metadata = {
        "batch_index": batch_index,
        "batch_size": batch_size,
        "offset": offset,
        "batches_processed": batch_counts,
        "execution_time_seconds": round(total_elapsed, 2),
    }
    
    save_batch_file(client, acc, batch_index, batch_metadata)
    
    logger.info("=" * 70)
    logger.info(f"BATCH {batch_index} COMPLETE in {format_elapsed(total_elapsed)}")
    logger.info(f"  Assets: {acc.total_assets:,}")
    logger.info(f"  TS: {acc.total_ts:,}")
    logger.info(f"  Equipment: {acc.total_equipment:,}")
    logger.info("=" * 70)
    
    return {
        "status": "batch_complete",
        "batch_index": batch_index,
        "batch_size": batch_size,
        "counts": {
            "assets": acc.total_assets,
            "timeseries": acc.total_ts,
            "equipment": acc.total_equipment,
            "notifications": acc.total_notifications,
            "orders": acc.total_orders,
        },
        "execution_time_seconds": round(total_elapsed, 2),
    }


# ----------------------------------------------------
# MAIN HANDLER
# ----------------------------------------------------

def handle(data: dict, client: CogniteClient) -> dict:
    """
    Cognite Function entry point - Combined metrics computation.

    Supports three modes:
    1. Normal mode: Process all data in single run (default)
    2. Batch collection mode: Process a subset and save intermediate data
    3. Aggregation mode: Merge all batch files and compute final metrics

    Args:
        data: Configuration overrides (optional)
            - batch_mode: bool - Enable batch processing
            - batch_index: int - Current batch index (0, 1, 2, ...)
            - batch_size: int - Instances per batch
            - is_aggregation: bool - Run aggregation phase
        client: CogniteClient instance

    Returns:
        dict: All computed metrics (or batch status in batch mode)
    """
    start_time = time.time()
    
    # Merge config
    config = {**DEFAULT_CONFIG, **(data or {})}
    
    # Check for batch/aggregation mode
    batch_mode = config.get("batch_mode", False)
    is_aggregation = config.get("is_aggregation", False)
    
    # ============================================================
    # AGGREGATION MODE: Merge all batches and compute final metrics
    # ============================================================
    if is_aggregation:
        return _handle_aggregation(client, config, start_time)
    
    # ============================================================
    # BATCH MODE: Process subset and save intermediate data
    # ============================================================
    if batch_mode:
        return _handle_batch_collection(client, config, start_time)
    
    # ============================================================
    # NORMAL MODE: Process all data in single run
    # ============================================================
    logger.info("=" * 70)
    logger.info("STARTING Contextualization Quality Metrics Function")
    logger.info("=" * 70)
    
    chunk_size = config["chunk_size"]
    max_ts = config["max_timeseries"]
    max_assets = config["max_assets"]
    max_eq = config["max_equipment"]
    max_notif = config["max_notifications"]
    max_orders = config["max_maintenance_orders"]
    max_annotations = config["max_annotations"]
    freshness_days = config["freshness_days"]
    enable_gaps = config["enable_historical_gaps"]
    enable_maintenance = config["enable_maintenance_metrics"]
    enable_file_annotations = config["enable_file_annotation_metrics"]
    gap_sample_rate = config["gap_sample_rate"]
    gap_threshold_days = config["gap_threshold_days"]
    gap_lookback = config["gap_lookback"]
    file_external_id = config["file_external_id"]
    file_name = config["file_name"]
    
    logger.info(f"Limits: TS={max_ts:,}, Assets={max_assets:,}, Equipment={max_eq:,}")
    if enable_maintenance:
        logger.info(f"Maintenance Limits: Notifications={max_notif:,}, Orders={max_orders:,}")
    
    # Build view IDs
    ts_view = ViewId(
        config["ts_view_space"],
        config["ts_view_external_id"],
        config["ts_view_version"]
    )
    asset_view = ViewId(
        config["asset_view_space"],
        config["asset_view_external_id"],
        config["asset_view_version"]
    )
    equipment_view = ViewId(
        config["equipment_view_space"],
        config["equipment_view_external_id"],
        config["equipment_view_version"]
    )
    
    # RMDM v1 views for maintenance workflow
    notification_view = ViewId(
        config["notification_view_space"],
        config["notification_view_external_id"],
        config["notification_view_version"]
    )
    order_view = ViewId(
        config["maintenance_order_view_space"],
        config["maintenance_order_view_external_id"],
        config["maintenance_order_view_version"]
    )
    failure_notification_view = ViewId(
        config["failure_notification_view_space"],
        config["failure_notification_view_external_id"],
        config["failure_notification_view_version"]
    )
    
    # CDM File Annotation view
    annotation_view = ViewId(
        config["annotation_view_space"],
        config["annotation_view_external_id"],
        config["annotation_view_version"]
    )
    
    # Initialize accumulator
    acc = CombinedAccumulator(freshness_days=freshness_days)
    
    batch_counts = {"ts": 0, "assets": 0, "equipment": 0, "notifications": 0, "orders": 0, "failure_notifications": 0, "annotations": 0}
    
    # ============================================================
    # PHASE 1: Process Time Series
    # ============================================================
    logger.info("-" * 50)
    logger.info("PHASE 1: Processing Time Series")
    logger.info("-" * 50)
    
    phase1_start = time.time()
    
    for ts_batch in client.data_modeling.instances(
        chunk_size=chunk_size,
        instance_type="node",
        sources=ts_view,
    ):
        batch_counts["ts"] += 1
        process_timeseries_batch(ts_batch, ts_view, acc)
        
        # Debug: Log unit info after first batch
        if batch_counts["ts"] == 1:
            logger.info(f"[DEBUG] After 1st batch: sourceUnit={acc.has_source_unit}, targetUnit={acc.has_target_unit}, checked={acc.unit_checks}")
        
        # Historical gap analysis: always analyze first batch + every Nth batch
        if enable_gaps and (batch_counts["ts"] == 1 or batch_counts["ts"] % gap_sample_rate == 0):
            logger.info(f"[Gaps] Analyzing batch {batch_counts['ts']} for historical data completeness...")
            compute_historical_gaps_batch(
                ts_batch, client, acc,
                gap_threshold_days=gap_threshold_days,
                lookback=gap_lookback
            )
            logger.info(f"[Gaps] Analyzed: {acc.ts_analyzed_for_gaps} TS, gaps found: {acc.gap_count}")
        
        if batch_counts["ts"] % LOG_EVERY_N_BATCHES == 0:
            elapsed = time.time() - start_time
            logger.info(
                f"[TS] Batch {batch_counts['ts']:,} | "
                f"Total: {acc.total_ts:,} | "
                f"Elapsed: {format_elapsed(elapsed)}"
            )
        
        if acc.total_ts >= max_ts:
            logger.info(f"🛑 Reached TS limit ({max_ts:,})")
            break
    
    logger.info(f"✅ PHASE 1: {acc.total_ts:,} TS in {format_elapsed(time.time() - phase1_start)}")
    
    # ============================================================
    # PHASE 2: Process Assets (shared data for TS, Hierarchy, EQ)
    # ============================================================
    logger.info("-" * 50)
    logger.info("PHASE 2: Processing Assets (shared data)")
    logger.info("-" * 50)
    
    phase2_start = time.time()
    
    for asset_batch in client.data_modeling.instances(
        chunk_size=chunk_size,
        instance_type="node",
        sources=asset_view,
    ):
        batch_counts["assets"] += 1
        process_asset_batch(asset_batch, asset_view, acc)
        
        if batch_counts["assets"] % LOG_EVERY_N_BATCHES == 0:
            elapsed = time.time() - start_time
            logger.info(
                f"[Assets] Batch {batch_counts['assets']:,} | "
                f"Total: {acc.total_assets:,} | "
                f"Elapsed: {format_elapsed(elapsed)}"
            )
        
        if acc.total_assets >= max_assets:
            logger.info(f"🛑 Reached Asset limit ({max_assets:,})")
            break
    
    logger.info(f"✅ PHASE 2: {acc.total_assets:,} Assets in {format_elapsed(time.time() - phase2_start)}")
    
    # ============================================================
    # PHASE 3: Process Equipment
    # ============================================================
    logger.info("-" * 50)
    logger.info("PHASE 3: Processing Equipment")
    logger.info("-" * 50)
    
    phase3_start = time.time()
    
    for eq_batch in client.data_modeling.instances(
        chunk_size=chunk_size,
        instance_type="node",
        sources=equipment_view,
    ):
        batch_counts["equipment"] += 1
        process_equipment_batch(eq_batch, equipment_view, acc)
        
        if batch_counts["equipment"] % LOG_EVERY_N_BATCHES == 0:
            elapsed = time.time() - start_time
            logger.info(
                f"[Equipment] Batch {batch_counts['equipment']:,} | "
                f"Total: {acc.total_equipment:,} | "
                f"Elapsed: {format_elapsed(elapsed)}"
            )
        
        if acc.total_equipment >= max_eq:
            logger.info(f"🛑 Reached Equipment limit ({max_eq:,})")
            break
    
    logger.info(f"✅ PHASE 3: {acc.total_equipment:,} Equipment in {format_elapsed(time.time() - phase3_start)}")
    
    # ============================================================
    # PHASE 4: Process Maintenance Workflow Data (RMDM v1)
    # ============================================================
    maintenance_metrics = {}
    
    if enable_maintenance:
        logger.info("-" * 50)
        logger.info("PHASE 4: Processing Maintenance Workflow Data (RMDM v1)")
        logger.info("-" * 50)
        
        phase4_start = time.time()
        
        # 4a. Process Notifications
        logger.info("[Maintenance] Processing Notifications...")
        try:
            for notif_batch in client.data_modeling.instances(
                chunk_size=chunk_size,
                instance_type="node",
                sources=notification_view,
            ):
                batch_counts["notifications"] += 1
                process_notification_batch(notif_batch, notification_view, acc)
                
                if batch_counts["notifications"] % LOG_EVERY_N_BATCHES == 0:
                    logger.info(f"[Notifications] Batch {batch_counts['notifications']:,} | Total: {acc.total_notifications:,}")
                
                if acc.total_notifications >= max_notif:
                    logger.info(f"🛑 Reached Notification limit ({max_notif:,})")
                    break
        except Exception as e:
            logger.warning(f"[Maintenance] Could not process Notifications: {e}")
        
        logger.info(f"[Maintenance] Notifications: {acc.total_notifications:,}")
        
        # 4b. Process Maintenance Orders
        logger.info("[Maintenance] Processing Maintenance Orders...")
        try:
            for order_batch in client.data_modeling.instances(
                chunk_size=chunk_size,
                instance_type="node",
                sources=order_view,
            ):
                batch_counts["orders"] += 1
                process_maintenance_order_batch(order_batch, order_view, acc)
                
                if batch_counts["orders"] % LOG_EVERY_N_BATCHES == 0:
                    logger.info(f"[Orders] Batch {batch_counts['orders']:,} | Total: {acc.total_orders:,}")
                
                if acc.total_orders >= max_orders:
                    logger.info(f"🛑 Reached Order limit ({max_orders:,})")
                    break
        except Exception as e:
            logger.warning(f"[Maintenance] Could not process Maintenance Orders: {e}")
        
        logger.info(f"[Maintenance] Orders: {acc.total_orders:,}")
        
        # 4c. Process Failure Notifications
        logger.info("[Maintenance] Processing Failure Notifications...")
        try:
            for fn_batch in client.data_modeling.instances(
                chunk_size=chunk_size,
                instance_type="node",
                sources=failure_notification_view,
            ):
                batch_counts["failure_notifications"] += 1
                process_failure_notification_batch(fn_batch, failure_notification_view, acc)
                
                if batch_counts["failure_notifications"] % LOG_EVERY_N_BATCHES == 0:
                    logger.info(f"[FailureNotif] Batch {batch_counts['failure_notifications']:,} | Total: {acc.total_failure_notifications:,}")
        except Exception as e:
            logger.warning(f"[Maintenance] Could not process Failure Notifications: {e}")
        
        logger.info(f"[Maintenance] Failure Notifications: {acc.total_failure_notifications:,}")
        logger.info(f"✅ PHASE 4: Maintenance data processed in {format_elapsed(time.time() - phase4_start)}")
        
        # Compute maintenance metrics
        maintenance_metrics = compute_maintenance_metrics(acc)
    else:
        logger.info("[Maintenance] Skipped - disabled in config")
    
    # ============================================================
    # PHASE 5: Process File Annotations (CDM CogniteDiagramAnnotation)
    # ============================================================
    file_annotation_metrics = {}
    annotation_acc = FileAnnotationAccumulator()
    
    if enable_file_annotations:
        logger.info("-" * 50)
        logger.info("PHASE 5: Processing File Annotations (CDM)")
        logger.info("-" * 50)
        
        phase5_start = time.time()
        
        try:
            for annot_batch in client.data_modeling.instances(
                chunk_size=chunk_size,
                instance_type="edge",
                sources=annotation_view,
            ):
                batch_counts["annotations"] += 1
                process_annotation_batch(annot_batch, annotation_view, annotation_acc)
                
                if batch_counts["annotations"] % LOG_EVERY_N_BATCHES == 0:
                    logger.info(f"[Annotations] Batch {batch_counts['annotations']:,} | Total: {annotation_acc.unique_annotations:,}")
                
                if annotation_acc.unique_annotations >= max_annotations:
                    logger.info(f"🛑 Reached Annotation limit ({max_annotations:,})")
                    break
        except Exception as e:
            logger.warning(f"[Annotations] Could not process annotations: {e}")
        
        logger.info(f"[Annotations] Total: {annotation_acc.unique_annotations:,}")
        logger.info(f"✅ PHASE 5: Annotations processed in {format_elapsed(time.time() - phase5_start)}")
        
        # Compute file annotation metrics
        file_annotation_metrics = compute_file_annotation_metrics(annotation_acc)
    else:
        logger.info("[Annotations] Skipped - disabled in config")
    
    # ============================================================
    # PHASE 6: Process 3D Model Contextualization
    # ============================================================
    model3d_metrics = {}
    model3d_acc = Model3DAccumulator()
    enable_3d = config.get("enable_3d_metrics", True)
    max_3d_objects = config.get("max_3d_objects", 150000)
    
    if enable_3d:
        logger.info("-" * 50)
        logger.info("PHASE 6: Processing 3D Model Contextualization")
        logger.info("-" * 50)
        
        phase6_start = time.time()
        
        # Build 3D object view
        object3d_view = ViewId(
            config["object3d_view_space"],
            config["object3d_view_external_id"],
            config["object3d_view_version"]
        )
        
        # 6a. Process assets for 3D links (reuse asset data already loaded)
        logger.info("[3D] Checking assets for 3D object associations...")
        try:
            for asset_batch in client.data_modeling.instances(
                chunk_size=chunk_size,
                instance_type="node",
                sources=asset_view,
            ):
                process_asset_3d_batch(asset_batch, asset_view, model3d_acc)
                
                if model3d_acc.total_assets_checked >= max_assets:
                    break
        except Exception as e:
            logger.warning(f"[3D] Could not process asset 3D links: {e}")
        
        logger.info(f"[3D] Assets with 3D: {model3d_acc.assets_with_3d:,} / {model3d_acc.total_assets_checked:,}")
        
        # 6b. Process 3D objects for bounding box completeness
        logger.info("[3D] Processing 3D objects...")
        try:
            for obj_batch in client.data_modeling.instances(
                chunk_size=chunk_size,
                instance_type="node",
                sources=object3d_view,
            ):
                batch_counts["3d_objects"] = batch_counts.get("3d_objects", 0) + 1
                process_3d_object_batch(obj_batch, object3d_view, model3d_acc)
                
                if model3d_acc.total_3d_objects >= max_3d_objects:
                    logger.info(f"🛑 Reached 3D Object limit ({max_3d_objects:,})")
                    break
        except Exception as e:
            logger.warning(f"[3D] Could not process 3D objects: {e}")
        
        logger.info(f"[3D] Total 3D objects: {model3d_acc.total_3d_objects:,}")
        logger.info(f"✅ PHASE 6: 3D processing in {format_elapsed(time.time() - phase6_start)}")
        
        # Compute 3D metrics
        model3d_metrics = compute_3d_metrics(model3d_acc)
    else:
        logger.info("[3D] Skipped - disabled in config")
    
    # ============================================================
    # PHASE 7: Compute All Metrics
    # ============================================================
    logger.info("-" * 50)
    logger.info("PHASE 7: Computing All Metrics")
    logger.info("-" * 50)
    
    phase7_start = time.time()
    
    ts_metrics = compute_ts_metrics(acc)
    hierarchy_metrics = compute_asset_hierarchy_metrics(acc)
    equipment_metrics = compute_equipment_metrics(acc)
    
    logger.info(f"✅ PHASE 7: Metrics computed in {format_elapsed(time.time() - phase7_start)}")
    
    # ============================================================
    # PHASE 8: Compile and Save Results
    # ============================================================
    logger.info("-" * 50)
    logger.info("PHASE 8: Saving Results to Cognite Files")
    logger.info("-" * 50)
    
    total_elapsed = time.time() - start_time
    
    all_metrics = {
        "metadata": {
            "computed_at": acc.now.isoformat(),
            "execution_time_seconds": round(total_elapsed, 2),
            "batches_processed": batch_counts,
            "instance_counts": {
                "timeseries": {
                    "total_instances": acc.total_ts_instances,
                    "unique": acc.total_ts,
                    "duplicates": acc.ts_duplicates,
                },
                "assets": {
                    "total_instances": acc.total_asset_instances,
                    "unique": acc.total_assets,
                    "duplicates": acc.asset_duplicates,
                },
                "equipment": {
                    "total_instances": acc.total_equipment_instances,
                    "unique": acc.total_equipment,
                    "duplicates": acc.equipment_duplicates,
                },
                "notifications": {
                    "total_instances": acc.total_notification_instances,
                    "unique": acc.total_notifications,
                    "duplicates": acc.notification_duplicates,
                },
                "maintenance_orders": {
                    "total_instances": acc.total_order_instances,
                    "unique": acc.total_orders,
                    "duplicates": acc.order_duplicates,
                },
                "failure_notifications": {
                    "unique": acc.total_failure_notifications,
                },
                "annotations": {
                    "unique": annotation_acc.unique_annotations if enable_file_annotations else 0,
                },
                "3d_objects": {
                    "unique": model3d_acc.total_3d_objects if enable_3d else 0,
                    "assets_with_3d": model3d_acc.assets_with_3d if enable_3d else 0,
                },
            },
            "limits_reached": {
                "timeseries": acc.total_ts >= max_ts,
                "assets": acc.total_assets >= max_assets,
                "equipment": acc.total_equipment >= max_eq,
                "notifications": acc.total_notifications >= max_notif if enable_maintenance else False,
                "maintenance_orders": acc.total_orders >= max_orders if enable_maintenance else False,
                "annotations": annotation_acc.unique_annotations >= max_annotations if enable_file_annotations else False,
                "3d_objects": model3d_acc.total_3d_objects >= max_3d_objects if enable_3d else False,
            },
            "config": {
                "chunk_size": chunk_size,
                "max_timeseries": max_ts,
                "max_assets": max_assets,
                "max_equipment": max_eq,
                "max_notifications": max_notif,
                "max_maintenance_orders": max_orders,
                "max_annotations": max_annotations,
                "max_3d_objects": max_3d_objects,
                "freshness_days": freshness_days,
                "enable_historical_gaps": enable_gaps,
                "enable_maintenance_metrics": enable_maintenance,
                "enable_file_annotation_metrics": enable_file_annotations,
                "enable_3d_metrics": enable_3d,
            },
        },
        "timeseries_metrics": ts_metrics,
        "hierarchy_metrics": hierarchy_metrics,
        "equipment_metrics": equipment_metrics,
        "maintenance_metrics": maintenance_metrics if enable_maintenance else {},
        "file_annotation_metrics": file_annotation_metrics if enable_file_annotations else {},
        "model3d_metrics": model3d_metrics if enable_3d else {},
    }
    
    # Save to Cognite Files
    save_metrics_to_file(client, all_metrics, file_external_id, file_name)
    
    # Final summary
    logger.info("=" * 70)
    logger.info("EXECUTION SUMMARY")
    logger.info("=" * 70)
    logger.info("Instance Counts (Total / Unique / Duplicates):")
    logger.info(f"  Time Series:  {acc.total_ts_instances:,} / {acc.total_ts:,} / {acc.ts_duplicates:,}")
    logger.info(f"  Assets:       {acc.total_asset_instances:,} / {acc.total_assets:,} / {acc.asset_duplicates:,}")
    logger.info(f"  Equipment:    {acc.total_equipment_instances:,} / {acc.total_equipment:,} / {acc.equipment_duplicates:,}")
    if enable_maintenance:
        logger.info(f"  Notifications:{acc.total_notification_instances:,} / {acc.total_notifications:,} / {acc.notification_duplicates:,}")
        logger.info(f"  Orders:       {acc.total_order_instances:,} / {acc.total_orders:,} / {acc.order_duplicates:,}")
        logger.info(f"  FailureNotif: {acc.total_failure_notifications:,}")
    if enable_file_annotations:
        logger.info(f"  Annotations:  {annotation_acc.total_annotations:,} / {annotation_acc.unique_annotations:,}")
    logger.info("-" * 50)
    logger.info(f"TS to Asset Rate:        {ts_metrics['ts_to_asset_rate']}%")
    logger.info(f"Asset Monitoring:        {ts_metrics['ts_asset_monitoring_coverage']}%")
    logger.info(f"TS Critical Coverage:    {ts_metrics['ts_critical_coverage']}%")
    logger.info(f"Hierarchy Completion:    {hierarchy_metrics['hierarchy_completion_rate']}%")
    logger.info(f"Hierarchy Orphan Rate:   {hierarchy_metrics['hierarchy_orphan_rate']}%")
    logger.info(f"EQ Association:          {equipment_metrics['eq_association_rate']}%")
    logger.info(f"EQ Type Consistency:     {equipment_metrics['eq_type_consistency_rate']}%")
    if enable_maintenance and maintenance_metrics:
        logger.info("-" * 50)
        logger.info("Maintenance Workflow Metrics:")
        logger.info(f"  Notif→Order Rate:      {maintenance_metrics.get('maint_notif_to_order_rate', 'N/A')}%")
        logger.info(f"  Notif→Asset Rate:      {maintenance_metrics.get('maint_notif_to_asset_rate', 'N/A')}%")
        logger.info(f"  Order Completion:      {maintenance_metrics.get('maint_order_completion_rate', 'N/A')}%")
        logger.info(f"  Failure Mode Rate:     {maintenance_metrics.get('maint_failure_mode_rate', 'N/A')}%")
    if enable_file_annotations and file_annotation_metrics:
        logger.info("-" * 50)
        logger.info("File Annotation Metrics:")
        logger.info(f"  Total Annotations:     {file_annotation_metrics.get('annot_total', 0):,}")
        logger.info(f"  Files with Annot:      {file_annotation_metrics.get('annot_unique_files_with_annotations', 0):,}")
        logger.info(f"  Avg Confidence:        {file_annotation_metrics.get('annot_avg_confidence', 'N/A')}%")
        logger.info(f"  Approved Rate:         {file_annotation_metrics.get('annot_approved_pct', 0):.1f}%")
    if enable_3d and model3d_metrics:
        logger.info("-" * 50)
        logger.info("3D Model Contextualization Metrics:")
        logger.info(f"  Asset 3D Coverage:     {model3d_metrics.get('model3d_asset_coverage', 0)}%")
        logger.info(f"  Critical Asset 3D:     {model3d_metrics.get('model3d_critical_asset_rate', 0)}%")
        logger.info(f"  BBox Completeness:     {model3d_metrics.get('model3d_bbox_completeness', 0)}%")
        logger.info(f"  Total 3D Objects:      {model3d_metrics.get('model3d_total_objects', 0):,}")
    logger.info("-" * 50)
    logger.info(f"Total Execution Time:    {format_elapsed(total_elapsed)}")
    logger.info(f"File saved:              {file_external_id}")
    logger.info("=" * 70)
    
    return all_metrics
