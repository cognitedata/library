# -*- coding: utf-8 -*-
"""
Staging vs Data Model comparison metrics.

Compares row counts between CDF Raw tables (staging) and Data Model instances
to identify data pipeline discrepancies.
"""

import logging
from typing import Dict, List, Tuple, Any
from dataclasses import dataclass, field

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import ViewId
from cognite.client.data_classes.aggregations import Count

logger = logging.getLogger(__name__)


# ============================================================
# DEFAULT CONFIGURATION
# ============================================================

DEFAULT_STAGING_CONFIG = {
    # Raw database containing source tables
    "staging_raw_database": "oracle:db",
    
    # DM Space and version for target views
    "staging_dm_space": "rmdm",
    "staging_dm_version": "v1",
    
    # Feature flag
    "enable_staging_metrics": True,
}

# Mapping: DM View -> List of source Raw tables
# Format: {view_external_id: [table1, table2, ...]}
DEFAULT_VIEW_TO_TABLES_MAPPING = {
    "Asset": [
        "AGPDW.W_AGHUB_FUNCTIONAL_LOCATION_D"
    ],
    "Equipment": [
        "AGPDW.W_AGHUB_EQUIPMENT_TIME_SEGMENT_D"
    ],
    "IDI_DownGradeSituation": [
        "AGPDW.W_AGHUB_DOWNGRADE_SITUATION_D"
    ],
    "IDI_MOC": [
        "AGPDW.W_AGHUB_MANAGEMENT_OF_CHANGE_D"
    ],
    "IDI_BOM": [
        "AGPDW.W_AGHUB_BILL_OF_MATERIALS_EQUIPMENT_D",
        "AGPDW.W_AGHUB_BILL_OF_MATERIALS_ITEM_D",
        "AGPDW.W_AGHUB_MATERIAL_PLANT_DATA_D"
    ],
    "IDI_MaintenancePlanItem": [
        "MAINTENANCE_PLANQUERY"
    ],
    "IDI_MaintenancePlan": [
        "MAINTENANCE_PLANQUERY"
    ],
    "IDI_Notifications": [
        "AGPDW.W_AGHUB_NOTIFICATION_DETAILS_D",
        "AGPDW.W_AGHUB_NOTIFICATION_HEADER_D",
        "AGPDW.W_AGHUB_NOTIFICATIONS_FULL"
    ],
    "IDI_Permits": [
        "PERMIT_QUERY"
    ],
    "IDI_Maintenance_Order": [
        "AGPDW.W_AGHUB_MAINTENANCE_ORDER_HEADER_D"
    ],
    "IDI_Operations": [
        "AGPDW.W_AGHUB_ORDER_OPERATION_D"
    ],
    "IDI_InspectionTask": [
        "AGPDW.W_IDI_INSP_TASK_D"
    ],
    "IDI_InspectionRec": [
        "AGPDW.W_IDI_INSP_REC_D"
    ],
    "IDI_InspectionRepo": [
        "AGPDW.W_IDI_INSP_REPO_D"
    ],
    "IDI_PRD_ANAL_RBI": [
        "AGPDW.W_IDI_PRD_ANAL_RBI_D"
    ],
    "IDI_RBI_PIPI": [
        "AGPDW.W_IDI_RBI_PIPI_D"
    ],
    "IDI_RCA_Legacy": [
        "AGPDW.W_IDI_RCA_LEGACY_D"
    ],
    "IDI_MaximoWorkorder": [
        "AGPDW.W_AGHUB_MAXIMO_WORKORDER_D"
    ],
    "IDI_RCA_Analysis": [
        "AGPDW.W_IDI_RCA_ANAL_D"
    ],
    "IDI_LIMS": [
        "AGPDW.W_IDI_LIMS_V2"
    ],
    "IDI_Dynamo": [
        "AGPDW.W_ALARM_RWS_EVENT_F",
        "AGPDW.W_ALARM_OPN_EVENT_F"
    ],
    "IDI_Runlog": [
        "RLOG_NERP.RUNLOG",
        "RLOG_NERP.RL_LONGDESCRIPTION",
        "RLOG_NERP.RL_EQUIPMENT_MASTER"
    ],
    "IDI_ELOG_Val": [
        "ELOG_NERP.ELOG_ASSIGNMENT",
        "ELOG_NERP.ELOG_ASSIGNMENT_HEADER",
        "ELOG_NERP.ELOG_LONG_TEXT",
        "ELOG_NERP.ELOG_META",
        "ELOG_NERP.ELOG_PLANT",
        "ELOG_NERP.ELOG_VAL"
    ],
}


# ============================================================
# DATA CLASSES
# ============================================================

@dataclass
class StagingAccumulator:
    """Accumulates staging vs DM comparison data."""
    
    # Per-view comparison results
    view_comparisons: List[Dict[str, Any]] = field(default_factory=list)
    
    # Summary counts
    total_raw_rows: int = 0
    total_dm_instances: int = 0
    views_matched: int = 0
    views_with_gaps: int = 0
    views_not_found: int = 0
    views_with_errors: int = 0
    
    # Raw table counts cache (to avoid re-querying)
    raw_table_counts: Dict[str, int] = field(default_factory=dict)
    
    # DM view counts cache
    dm_view_counts: Dict[str, int] = field(default_factory=dict)


# ============================================================
# DATA FETCHING FUNCTIONS
# ============================================================

def get_raw_table_row_count(
    client: CogniteClient, 
    database: str, 
    table: str
) -> Tuple[int, str]:
    """
    Get the row count for a Raw table.
    
    Returns:
        Tuple of (count, status) where status is 'ok', 'not_found', or 'error: <message>'
    """
    try:
        # Use list and count rows
        rows = client.raw.rows.list(db_name=database, table_name=table, limit=None)
        count = sum(1 for _ in rows)
        return count, "ok"
    except Exception as e:
        error_msg = str(e).lower()
        if "not found" in error_msg or "404" in error_msg:
            return 0, "not_found"
        return 0, f"error: {str(e)[:100]}"


def get_dm_view_instance_count(
    client: CogniteClient, 
    space: str, 
    view_id: str, 
    version: str
) -> Tuple[int, str]:
    """
    Get the instance count for a Data Model view using aggregate.
    
    Returns:
        Tuple of (count, status) where status is 'ok', 'not_found', or 'error: <message>'
    """
    try:
        view = ViewId(space=space, external_id=view_id, version=version)
        
        # Use aggregate to get count efficiently
        result = client.data_modeling.instances.aggregate(
            view=view,
            aggregates=Count("externalId"),
            group_by=[]
        )
        
        if result and len(result) > 0:
            count = result[0].aggregates[0].value
            return count, "ok"
        return 0, "ok"
        
    except Exception as e:
        error_msg = str(e).lower()
        if "not found" in error_msg or "404" in error_msg or "does not exist" in error_msg:
            return 0, "not_found"
        return 0, f"error: {str(e)[:100]}"


# ============================================================
# METRICS COMPUTATION
# ============================================================

def compute_staging_metrics(
    client: CogniteClient,
    config: Dict[str, Any] = None,
    view_to_tables_mapping: Dict[str, List[str]] = None
) -> Dict[str, Any]:
    """
    Compute staging vs DM comparison metrics.
    
    Args:
        client: CogniteClient instance
        config: Configuration overrides (optional)
        view_to_tables_mapping: Custom mapping of views to raw tables (optional)
    
    Returns:
        Dictionary containing staging comparison metrics
    """
    # Merge config
    cfg = {**DEFAULT_STAGING_CONFIG, **(config or {})}
    
    database = cfg.get("staging_raw_database", "oracle:db")
    space = cfg.get("staging_dm_space", "rmdm")
    version = cfg.get("staging_dm_version", "v1")
    
    mapping = view_to_tables_mapping or DEFAULT_VIEW_TO_TABLES_MAPPING
    
    logger.info(f"Computing staging metrics: database={database}, space={space}, version={version}")
    logger.info(f"Processing {len(mapping)} view mappings")
    
    acc = StagingAccumulator()
    
    # Process each view mapping
    for view_id, tables in mapping.items():
        logger.debug(f"Processing view: {view_id} <- {tables}")
        
        # Get DM view count
        dm_count, dm_status = get_dm_view_instance_count(client, space, view_id, version)
        acc.dm_view_counts[view_id] = dm_count
        
        # Get Raw table counts
        raw_breakdown = {}
        raw_total = 0
        raw_errors = []
        
        for table in tables:
            # Check cache first
            if table in acc.raw_table_counts:
                count = acc.raw_table_counts[table]
                status = "ok"
            else:
                count, status = get_raw_table_row_count(client, database, table)
                acc.raw_table_counts[table] = count
            
            raw_breakdown[table] = count
            raw_total += count
            
            if status != "ok":
                raw_errors.append(f"{table}: {status}")
        
        # Calculate metrics - use symmetric comparison to catch gaps in both directions
        # Match rate = min/max * 100 (0-100%, works regardless of which is larger)
        if raw_total > 0 or dm_count > 0:
            match_rate = round((min(raw_total, dm_count) / max(raw_total, dm_count)) * 100, 2)
        else:
            match_rate = 100.0  # Both zero = perfect match
        
        # Absolute difference and direction indicator
        difference = abs(raw_total - dm_count)
        if dm_count > raw_total:
            gap_direction = "dm_exceeds_raw"
        elif raw_total > dm_count:
            gap_direction = "raw_exceeds_dm"
        else:
            gap_direction = "equal"
        
        # Determine status
        if dm_status == "not_found":
            status = "view_not_found"
            acc.views_not_found += 1
        elif raw_errors:
            status = "raw_error"
            acc.views_with_errors += 1
        elif match_rate >= 99:
            status = "matched"
            acc.views_matched += 1
        elif match_rate >= 90:
            status = "minor_gap"
            acc.views_with_gaps += 1
        elif match_rate >= 50:
            status = "significant_gap"
            acc.views_with_gaps += 1
        else:
            status = "major_gap"
            acc.views_with_gaps += 1
        
        # Update totals
        acc.total_raw_rows += raw_total
        acc.total_dm_instances += dm_count
        
        # Store comparison result
        comparison = {
            "dm_view": view_id,
            "dm_space": space,
            "dm_version": version,
            "raw_tables": tables,
            "raw_database": database,
            "raw_breakdown": raw_breakdown,
            "raw_total": raw_total,
            "dm_count": dm_count,
            "difference": difference,
            "gap_direction": gap_direction,
            "match_rate": match_rate,
            "status": status,
            "errors": raw_errors,
        }
        acc.view_comparisons.append(comparison)
        
        logger.debug(f"  {view_id}: raw={raw_total:,} dm={dm_count:,} match={match_rate}% status={status} direction={gap_direction}")
    
    # Calculate overall metrics - use symmetric comparison
    if acc.total_raw_rows > 0 or acc.total_dm_instances > 0:
        overall_match_rate = round(
            (min(acc.total_raw_rows, acc.total_dm_instances) / max(acc.total_raw_rows, acc.total_dm_instances)) * 100, 2
        )
    else:
        overall_match_rate = 0.0
    
    # Build result
    result = {
        "staging_has_data": len(acc.view_comparisons) > 0,
        
        # Summary
        "staging_total_mappings": len(mapping),
        "staging_views_matched": acc.views_matched,
        "staging_views_with_gaps": acc.views_with_gaps,
        "staging_views_not_found": acc.views_not_found,
        "staging_views_with_errors": acc.views_with_errors,
        
        # Totals
        "staging_total_raw_rows": acc.total_raw_rows,
        "staging_total_dm_instances": acc.total_dm_instances,
        "staging_total_difference": abs(acc.total_raw_rows - acc.total_dm_instances),
        "staging_overall_gap_direction": "dm_exceeds_raw" if acc.total_dm_instances > acc.total_raw_rows else ("raw_exceeds_dm" if acc.total_raw_rows > acc.total_dm_instances else "equal"),
        "staging_overall_match_rate": overall_match_rate,
        
        # Configuration used
        "staging_config": {
            "raw_database": database,
            "dm_space": space,
            "dm_version": version,
        },
        
        # Detailed comparisons
        "staging_comparisons": acc.view_comparisons,
    }
    
    logger.info(f"Staging metrics complete: {acc.views_matched} matched, {acc.views_with_gaps} with gaps, overall match: {overall_match_rate}%")
    
    return result
