"""
Time Series processing and metrics computation.
"""

from typing import Optional
from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import ViewId, NodeId

from .common import (
    get_props,
    get_external_id,
    normalize_timestamp,
    CombinedAccumulator,
)


def process_timeseries_batch(
    ts_batch,
    ts_view: ViewId,
    acc: CombinedAccumulator
):
    """Process time series batch - collect TS metrics data."""
    for ts in ts_batch:
        ts_id = get_external_id(ts)
        if not ts_id:
            continue
        
        acc.total_ts_instances += 1
        
        # Skip if already processed (duplicate)
        if ts_id in acc.ts_ids_seen:
            acc.ts_duplicate_ids.append(ts_id)
            continue
        acc.ts_ids_seen.add(ts_id)
        
        # Track assets with TS and TS with asset links
        props = get_props(ts, ts_view)
        assets_ref = props.get("assets") or []
        if isinstance(assets_ref, dict):
            assets_ref = [assets_ref]
        
        has_asset_link = False
        for a in assets_ref:
            aid = a.get("externalId")
            if aid:
                acc.assets_with_ts.add(aid)
                has_asset_link = True
        
        if has_asset_link:
            acc.ts_with_asset_link += 1
        
        # Unit Metrics - get from view properties first, fallback to dump
        unit = props.get("unit")
        src_unit = props.get("sourceUnit")
        
        # If not in view props, try the dump (some SDKs expose it differently)
        if unit is None or src_unit is None:
            try:
                props_dump = ts.dump() if hasattr(ts, 'dump') else {}
                # Check nested properties structure
                if "properties" in props_dump:
                    for view_key, view_props in props_dump.get("properties", {}).items():
                        if isinstance(view_props, dict):
                            if unit is None:
                                unit = view_props.get("unit")
                            if src_unit is None:
                                src_unit = view_props.get("sourceUnit")
                else:
                    if unit is None:
                        unit = props_dump.get("unit")
                    if src_unit is None:
                        src_unit = props_dump.get("sourceUnit")
            except Exception:
                pass
        
        acc.unit_checks += 1
        
        # Clean up unit strings
        unit_str = unit.strip() if isinstance(unit, str) and unit.strip() else None
        src_unit_str = src_unit.strip() if isinstance(src_unit, str) and src_unit.strip() else None
        
        # Track source unit completeness
        if src_unit_str:
            acc.has_source_unit += 1
            acc.source_units_seen.add(src_unit_str)
        
        # Track target (standardized) unit completeness
        if unit_str:
            acc.has_target_unit += 1
        
        # Track if any unit is defined
        if unit_str or src_unit_str:
            acc.has_any_unit += 1
        
        # Track if units match (when both present)
        if unit_str and src_unit_str and unit_str == src_unit_str:
            acc.units_match += 1
        
        # Data Freshness
        raw_ts = getattr(ts, "last_updated_time", None)
        ts_last_update = normalize_timestamp(raw_ts)
        if ts_last_update and ts_last_update >= acc.fresh_limit:
            acc.fresh_count += 1
        
        # Processing Lag
        if ts_last_update:
            lag_seconds = (acc.now - ts_last_update).total_seconds()
            lag_hours = lag_seconds / 3600
            if 0 < lag_hours < 8760:
                acc.lag_sum += lag_hours
                acc.lag_count += 1


def compute_historical_gaps_batch(
    ts_batch,
    client: CogniteClient,
    acc: CombinedAccumulator,
    gap_threshold_days: int = 7,
    lookback: str = "1000d-ago",
):
    """
    Compute historical data completeness for a batch of timeseries.
    
    For each time series:
    1. Calculate total time span (first to last datapoint)
    2. Find gaps larger than threshold (e.g., 7 days without data)
    3. Sum gap durations to calculate data completeness
    
    Example: 1 year of data with a 1-month gap = (365-30)/365 = 91.8% complete
    """
    ts_instance_ids = []
    for n in ts_batch:
        if getattr(n, "space", None) and getattr(n, "external_id", None):
            ts_instance_ids.append(NodeId(n.space, n.external_id))

    if not ts_instance_ids:
        return

    try:
        df = client.time_series.data.retrieve_dataframe(
            instance_id=ts_instance_ids,
            start=lookback,
            end="now",
            column_names="external_id",
            ignore_unknown_ids=True,
        )
    except Exception:
        return

    if df.empty:
        return

    gap_threshold_ms = gap_threshold_days * 24 * 3600 * 1000
    ms_per_day = 24 * 3600 * 1000

    for col in df.columns:
        series = df[col].dropna()
        if len(series) < 2:
            continue
        
        acc.ts_analyzed_for_gaps += 1
        acc.ts_with_data += 1
        
        timestamps = series.index.astype("int64") // 1_000_000  # Convert to ms
        
        # Calculate total time span (first to last datapoint)
        first_ts = timestamps.min()
        last_ts = timestamps.max()
        time_span_ms = last_ts - first_ts
        time_span_days = time_span_ms / ms_per_day
        acc.total_time_span_days += time_span_days
        
        # Find gaps between consecutive datapoints
        diffs = timestamps[1:] - timestamps[:-1]
        
        # Identify significant gaps (longer than threshold)
        significant_gaps = diffs[diffs > gap_threshold_ms]
        
        if len(significant_gaps) > 0:
            # Sum total gap duration for this TS
            gap_duration_ms = significant_gaps.sum()
            gap_duration_days = gap_duration_ms / ms_per_day
            acc.total_gap_duration_days += gap_duration_days
            acc.gap_count += len(significant_gaps)
            
            # Track longest gap
            longest_gap_ms = significant_gaps.max()
            longest_gap_days = longest_gap_ms / ms_per_day
            if longest_gap_days > acc.longest_gap_days:
                acc.longest_gap_days = longest_gap_days


def compute_ts_metrics(acc: CombinedAccumulator) -> dict:
    """Compute all time series contextualization metrics."""
    associated_assets = len(acc.assets_with_ts)
    
    # TS to Asset Rate: % of time series that are linked to at least one asset
    # This is the PRIMARY metric - orphaned TS (not linked to asset) are a problem
    ts_to_asset_rate = (
        (acc.ts_with_asset_link / acc.total_ts * 100)
        if acc.total_ts else 0.0
    )
    
    # Asset Monitoring Coverage: % of assets that have at least one TS linked
    # This is SECONDARY - it's OK for some assets to not have TS
    asset_monitoring_coverage = (
        (associated_assets / acc.total_assets * 100)
        if acc.total_assets else 0.0
    )
    
    # Return None if no critical assets exist (N/A case)
    critical_coverage = (
        (acc.critical_assets_with_ts / acc.critical_assets_total * 100)
        if acc.critical_assets_total > 0 else None
    )
    
    # Unit metrics - multiple perspectives on unit quality
    source_unit_rate = (
        (acc.has_source_unit / acc.unit_checks * 100)
        if acc.unit_checks else 0.0
    )
    target_unit_rate = (
        (acc.has_target_unit / acc.unit_checks * 100)
        if acc.unit_checks else 0.0
    )
    any_unit_rate = (
        (acc.has_any_unit / acc.unit_checks * 100)
        if acc.unit_checks else 0.0
    )
    # Unit mapping rate: when both are present, do they match?
    both_units_present = min(acc.has_source_unit, acc.has_target_unit)
    unit_mapping_rate = (
        (acc.units_match / both_units_present * 100)
        if both_units_present > 0 else None
    )
    
    # Historical Data Completeness
    # = (total_time_span - gap_duration) / total_time_span × 100%
    # Example: 1 year span with 1 month gap = (365-30)/365 = 91.8%
    if acc.total_time_span_days > 0:
        data_with_gaps = acc.total_time_span_days - acc.total_gap_duration_days
        historical_data_completeness = (data_with_gaps / acc.total_time_span_days) * 100
    else:
        historical_data_completeness = None  # No data analyzed
    
    # Average gap duration
    avg_gap_days = (
        acc.total_gap_duration_days / acc.gap_count 
        if acc.gap_count > 0 else 0.0
    )
    
    data_freshness = (
        (acc.fresh_count / acc.total_ts * 100)
        if acc.total_ts else 0.0
    )
    
    processing_lag_hours = (
        (acc.lag_sum / acc.lag_count)
        if acc.lag_count else None
    )
    
    return {
        # PRIMARY: TS to Asset Contextualization (orphaned TS are a problem)
        "ts_to_asset_rate": round(ts_to_asset_rate, 2),
        "ts_with_asset_link": acc.ts_with_asset_link,
        "ts_without_asset_link": acc.total_ts - acc.ts_with_asset_link,
        # SECONDARY: Asset Monitoring Coverage (OK for some assets to lack TS)
        "ts_asset_monitoring_coverage": round(asset_monitoring_coverage, 2),
        "ts_associated_assets": associated_assets,
        "ts_critical_coverage": round(critical_coverage, 2) if critical_coverage is not None else None,
        "ts_critical_with_ts": acc.critical_assets_with_ts,
        "ts_critical_total": acc.critical_assets_total,
        "ts_has_critical_assets": acc.critical_assets_total > 0,
        # Unit metrics
        "ts_source_unit_completeness": round(source_unit_rate, 2),
        "ts_target_unit_completeness": round(target_unit_rate, 2),
        "ts_any_unit_completeness": round(any_unit_rate, 2),
        "ts_unit_mapping_rate": round(unit_mapping_rate, 2) if unit_mapping_rate is not None else None,
        "ts_has_source_unit": acc.has_source_unit,
        "ts_has_target_unit": acc.has_target_unit,
        "ts_has_any_unit": acc.has_any_unit,
        "ts_units_match": acc.units_match,
        "ts_unit_checks": acc.unit_checks,
        "ts_unique_source_units": len(acc.source_units_seen),
        # Historical Data Completeness - time-based coverage
        "ts_historical_data_completeness": round(historical_data_completeness, 2) if historical_data_completeness is not None else None,
        "ts_analyzed_for_gaps": acc.ts_analyzed_for_gaps,
        "ts_total_time_span_days": round(acc.total_time_span_days, 1),
        "ts_total_gap_duration_days": round(acc.total_gap_duration_days, 1),
        "ts_gap_count": acc.gap_count,
        "ts_longest_gap_days": round(acc.longest_gap_days, 1),
        "ts_avg_gap_days": round(avg_gap_days, 1),
        "ts_data_freshness": round(data_freshness, 2),
        "ts_fresh_count": acc.fresh_count,
        "ts_processing_lag_hours": round(processing_lag_hours, 2) if processing_lag_hours else None,
        "ts_total": acc.total_ts,
    }
