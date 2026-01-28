# -*- coding: utf-8 -*-
"""
Time Series Contextualization Dashboard.
"""

import streamlit as st

from .common import (
    get_status_color_ts,
    metric_card,
    gauge,
    gauge_na,
)
from .ai_summary import (
    render_ai_summary_section,
    get_timeseries_prompt,
    format_timeseries_metrics,
)


def render_time_series_dashboard(metrics: dict):
    """Render the Time Series Contextualization dashboard tab."""
    st.title("Time Series Contextualization Quality Dashboard")
    
    ts_metrics = metrics.get("timeseries_metrics", {})
    hierarchy = metrics.get("hierarchy_metrics", {})
    metadata = metrics.get("metadata", {})
    
    if not ts_metrics:
        st.warning("No time series metrics found in the data file.")
        return
    
    # Show data info
    computed_at = metadata.get("computed_at", "Unknown")
    st.info(f"[Date] Metrics computed at: {computed_at}")
    
    # Important note about data sources
    with st.expander("**Understanding Metric Sources** - Click to learn more", expanded=False):
        st.markdown("""
        **Metrics calculated from Time Series METADATA** (fast, no datapoint access):
        - TS to Asset Contextualization - from `assets` property
        - Asset Monitoring Coverage - from `assets` property  
        - Critical Asset Coverage - from `assets` property + asset criticality
        - Source/Target Unit Completeness - from `sourceUnit` and `unit` properties
        - Data Freshness - from `lastUpdatedTime` *[Note: This is when the TS definition was last modified, NOT when data was last ingested]*
        - Processing Lag - from `lastUpdatedTime` *[Same limitation as above]*
        
        **Metrics calculated from ACTUAL DATA POINTS** (requires datapoint retrieval):
        - Historical Data Completeness - analyzes actual datapoint timestamps
        - Gap Count/Duration - detects gaps between consecutive datapoints
        - Total Time Span - first to last datapoint timestamp
        
        *Tip: Metadata-based metrics are fast but may not reflect actual data ingestion status. For true data freshness, check the Historical Data Completeness section.*
        """)
    
    st.markdown("---")
    
    # Extract metrics
    # PRIMARY: TS to Asset Contextualization (orphaned TS are a problem)
    ts_to_asset_rate = ts_metrics.get("ts_to_asset_rate", 0)
    ts_with_asset_link = ts_metrics.get("ts_with_asset_link", 0)
    ts_without_asset_link = ts_metrics.get("ts_without_asset_link", 0)
    # SECONDARY: Asset Monitoring Coverage (OK for some assets to lack TS)
    asset_monitoring_coverage = ts_metrics.get("ts_asset_monitoring_coverage", 0)
    associated_assets = ts_metrics.get("ts_associated_assets", 0)
    critical_coverage = ts_metrics.get("ts_critical_coverage")  # Can be None
    critical_with_ts = ts_metrics.get("ts_critical_with_ts", 0)
    critical_total = ts_metrics.get("ts_critical_total", 0)
    has_critical_assets = ts_metrics.get("ts_has_critical_assets", False)
    # Unit metrics
    source_unit_completeness = ts_metrics.get("ts_source_unit_completeness", 0)
    target_unit_completeness = ts_metrics.get("ts_target_unit_completeness", 0)
    any_unit_completeness = ts_metrics.get("ts_any_unit_completeness", 0)
    unit_mapping_rate = ts_metrics.get("ts_unit_mapping_rate")  # Can be None
    has_source_unit = ts_metrics.get("ts_has_source_unit", 0)
    has_target_unit = ts_metrics.get("ts_has_target_unit", 0)
    has_any_unit = ts_metrics.get("ts_has_any_unit", 0)
    units_match = ts_metrics.get("ts_units_match", 0)
    unit_checks = ts_metrics.get("ts_unit_checks", 0)
    unique_source_units = ts_metrics.get("ts_unique_source_units", 0)
    # Historical Data Completeness metrics
    historical_data_completeness = ts_metrics.get("ts_historical_data_completeness")  # Can be None
    ts_analyzed_for_gaps = ts_metrics.get("ts_analyzed_for_gaps", 0)
    total_time_span_days = ts_metrics.get("ts_total_time_span_days", 0)
    total_gap_duration_days = ts_metrics.get("ts_total_gap_duration_days", 0)
    gap_count = ts_metrics.get("ts_gap_count", 0)
    longest_gap_days = ts_metrics.get("ts_longest_gap_days", 0)
    avg_gap_days = ts_metrics.get("ts_avg_gap_days", 0)
    # Other metrics
    data_freshness = ts_metrics.get("ts_data_freshness", 0)
    fresh_count = ts_metrics.get("ts_fresh_count", 0)
    processing_lag_hours = ts_metrics.get("ts_processing_lag_hours")
    total_ts = ts_metrics.get("ts_total", 0)
    total_assets = hierarchy.get("hierarchy_total_assets", 0)
    
    # Summary cards
    col1, col2, col3, col4 = st.columns(4)
    metric_card(col1, "Total Time Series", f"{total_ts:,}",
                help_text="Total number of unique time series")
    metric_card(col2, "TS with Asset Link", f"{ts_with_asset_link:,}",
                help_text="Time series linked to at least one asset (contextualized)")
    metric_card(col3, "Orphaned TS", f"{ts_without_asset_link:,}",
                help_text="Time series NOT linked to any asset (need contextualization)")
    metric_card(col4, "Assets with TS", f"{associated_assets:,}",
                help_text="Number of assets that have at least one time series linked")
    
    st.markdown("---")
    
    st.header("Data Quality Metrics")
    
    # METRIC CARDS (GAUGES) - Row 1: Contextualization
    g1, g2, g3 = st.columns(3)
    
    # CRITICAL: TS to Asset Contextualization (orphaned TS are a problem)
    gauge(g1, "TS to Asset Contextualization", ts_to_asset_rate, "ts_to_asset", 
          get_status_color_ts, [0, 100], "%", key="ts_to_asset",
          help_text="CRITICAL: % of TS with asset link. Orphaned TS are a problem.")
    
    # INFORMATIONAL: Asset Monitoring Coverage (not all assets need TS)
    gauge(g2, "Asset Monitoring Coverage", asset_monitoring_coverage, "asset_monitoring", 
          get_status_color_ts, [0, 100], "%", key="ts_asset_coverage",
          help_text="INFORMATIONAL: % of assets with TS. Low values OK - not all assets need monitoring.")
    
    # Critical Asset Coverage - show N/A if no critical assets defined
    if has_critical_assets and critical_coverage is not None:
        gauge(g3, "Critical Asset Coverage", critical_coverage, "critical_coverage", 
              get_status_color_ts, [0, 100], "%", key="ts_critical",
              help_text="% of critical assets with time series (should be 100%)")
    else:
        gauge_na(g3, "Critical Asset Coverage", "No critical assets defined", key="ts_critical_na",
                 help_text="% of critical assets with time series linked")
    
    st.markdown("---")
    
    # Row 2: Data Quality - Freshness, Historical Completeness, Source Unit
    g4, g5, g6 = st.columns(3)
    gauge(g4, "Data Freshness (Last 30 Days)", data_freshness, "freshness", 
          get_status_color_ts, [0, 100], "%", key="ts_fresh",
          help_text="% of time series updated within the last 30 days")
    
    # Historical Data Completeness - check config and show appropriate message
    config = metadata.get("config", {})
    gaps_enabled = config.get("enable_historical_gaps", True)  # Default True for newer versions
    
    if historical_data_completeness is not None and ts_analyzed_for_gaps > 0:
        gauge(g5, "Historical Data Completeness", historical_data_completeness, "gap", 
              get_status_color_ts, [0, 100], "%", key="ts_gap",
              help_text="% of time span with actual data (100% - gap duration)")
    elif gaps_enabled and ts_analyzed_for_gaps == 0:
        # Feature enabled but no TS were analyzed (possibly no TS data available)
        gauge_na(g5, "Historical Data Completeness", "No time series data to analyze", key="ts_gap_na",
                 help_text="Detects gaps >7 days in historical data")
    else:
        gauge_na(g5, "Historical Data Completeness", "Disabled in config", key="ts_gap_na",
                 help_text="Enable 'enable_historical_gaps' in function config")
    
    # Source Unit Completeness
    gauge(g6, "Source Unit Completeness", source_unit_completeness, "unit_consistency", 
          get_status_color_ts, [0, 100], "%", key="ts_src_unit",
          help_text="% of time series with sourceUnit populated (e.g., C, mm, %)")
    
    st.write("")
    
    # Row 3: Target Unit (if available)
    g7, g8, g9 = st.columns(3)
    # Target Unit Completeness (standardized unit)
    if target_unit_completeness > 0:
        gauge(g7, "Target Unit (Standardized)", target_unit_completeness, "unit_consistency", 
              get_status_color_ts, [0, 100], "%", key="ts_tgt_unit",
              help_text="% of time series with standardized unit populated")
    else:
        gauge_na(g7, "Target Unit (Standardized)", "No standardized units defined", key="ts_tgt_unit_na",
                 help_text="% of time series with standardized unit populated")
    
    st.markdown("---")
    
    # DIAGNOSTICS & LAG
    st.subheader("Diagnostics")
    c1, c2 = st.columns(2)
    
    if processing_lag_hours is not None:
        c1.metric(
            "Avg Time Since Last TS Update",
            f"{processing_lag_hours:.2f} hours",
            help="Average time since the Time Series metadata was last modified (lastUpdatedTime). Note: This is metadata update time, not the timestamp of the latest datapoint."
        )
    else:
        c1.metric("Avg Time Since Last TS Update", "N/A")
    
    # Data Breakdown
    c2.markdown("### Data Breakdown")
    c2.markdown(f"""
    | Metric | Value |
    |--------|-------|
    | Total Time Series | {total_ts:,} |
    | TS with Asset Link | {ts_with_asset_link:,} |
    | Orphaned TS (no asset) | {ts_without_asset_link:,} |
    | Total Assets | {total_assets:,} |
    | Assets with TS | {associated_assets:,} |
    | Total Critical Assets | {critical_total:,} |
    | Critical Assets with TS | {critical_with_ts:,} |
    | TS with Source Unit | {has_source_unit:,} |
    | TS with Target Unit | {has_target_unit:,} |
    | Unique Source Units | {unique_source_units:,} |
    | Fresh TS Count | {fresh_count:,} |
    """)
    
    # Historical Data Completeness Details
    if ts_analyzed_for_gaps > 0:
        st.markdown("---")
        st.subheader("Historical Data Completeness Analysis")
        st.markdown(f"""
        Analyzed **{ts_analyzed_for_gaps:,}** time series for significant data gaps (>{7} days without data).
        """)
        
        d1, d2, d3, d4 = st.columns(4)
        d1.metric("Total Time Span", f"{total_time_span_days:,.0f} days", 
                  help="Sum of time spans (first to last datapoint) across all analyzed TS")
        d2.metric("Gaps Found", f"{gap_count:,}",
                  help="Number of periods > 7 days without data")
        d3.metric("Total Gap Duration", f"{total_gap_duration_days:,.0f} days",
                  help="Sum of all gap durations")
        d4.metric("Longest Gap", f"{longest_gap_days:,.0f} days",
                  help="Duration of the longest gap found")
        
        if gap_count > 0:
            st.warning(f"""
            **Gap Summary:** Found {gap_count:,} significant gaps totaling {total_gap_duration_days:,.0f} days. 
            Average gap duration: {avg_gap_days:.1f} days. Longest gap: {longest_gap_days:.0f} days.
            """)
        else:
            st.success("No significant data gaps (>7 days) detected in the analyzed time series.")
    
    # AI SUMMARY SECTION
    render_ai_summary_section(
        dashboard_type="Time Series Contextualization",
        metrics_data=format_timeseries_metrics(ts_metrics, hierarchy),
        system_prompt=get_timeseries_prompt(),
        key_prefix="timeseries"
    )
    
    st.markdown("---")
    st.success("Time Series Contextualization dashboard loaded from pre-computed metrics.")
