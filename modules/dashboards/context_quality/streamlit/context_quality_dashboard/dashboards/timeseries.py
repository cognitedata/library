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
from .reports import generate_timeseries_report


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
        **All metrics are calculated from Time Series METADATA** (fast, no datapoint access):
        - TS to Asset Contextualization - from `assets` property
        - Asset Monitoring Coverage - from `assets` property  
        - Critical Asset Coverage - from `assets` property + asset criticality
        - Source/Target Unit Completeness - from `sourceUnit` and `unit` properties
        - Data Freshness - from `lastUpdatedTime` *[Note: This is when the TS definition was last modified, NOT when data was last ingested]*
        - Processing Lag - from `lastUpdatedTime` *[Same limitation as above]*
        
        *Tip: Metadata-based metrics are fast but may not reflect actual data ingestion status.*
        """)
    
    # Download Report Button
    st.download_button(
        label="Download Time Series Report (PDF)",
        data=generate_timeseries_report(metrics),
        file_name="timeseries_report.pdf",
        mime="application/pdf",
        use_container_width=True,
        type="primary",
        key="download_timeseries_report"
    )
    
    st.markdown("---")
    
    # Extract metrics
    ts_to_asset_rate = ts_metrics.get("ts_to_asset_rate", 0)
    ts_with_asset_link = ts_metrics.get("ts_with_asset_link", 0)
    ts_without_asset_link = ts_metrics.get("ts_without_asset_link", 0)
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
    # Other metrics
    data_freshness = ts_metrics.get("ts_data_freshness", 0)
    fresh_count = ts_metrics.get("ts_fresh_count", 0)
    processing_lag_hours = ts_metrics.get("ts_processing_lag_hours")
    total_ts = ts_metrics.get("ts_total", 0)
    total_assets = hierarchy.get("hierarchy_total_assets", 0)
    
    # =====================================================
    # MAIN METRIC - BIG ON TOP
    # =====================================================
    st.header("Time Series-to-Asset Contextualization")
    
    col_main, col_info = st.columns([2, 1])
    
    with col_main:
        gauge(col_main, "TS Linked to Assets", ts_to_asset_rate, "ts_to_asset", 
              get_status_color_ts, [0, 100], "%", key="ts_to_asset_main",
              help_text="% of time series linked to at least one asset.")
    
    with col_info:
        st.markdown("### Why This Matters")
        st.markdown("""
        Time series without asset links are **orphaned** - they exist
        but lack business context. This prevents:
        - Viewing sensor data on the asset tree
        - Asset-level monitoring dashboards
        - Condition-based maintenance triggers
        """)
        
        if ts_to_asset_rate >= 95:
            st.success(f"Excellent! Only {ts_without_asset_link:,} TS orphaned.")
        elif ts_to_asset_rate >= 80:
            st.warning(f"{ts_without_asset_link:,} time series need asset links.")
        else:
            st.error(f"{ts_without_asset_link:,} time series are orphaned!")
    
    st.markdown("---")
    
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
    
    # CSV Download for Orphaned TS
    orphaned_ids = ts_metrics.get("ts_orphaned_ids", [])
    if orphaned_ids:
        csv_data = "external_id\n" + "\n".join(orphaned_ids)
        st.download_button(
            label=f"Download Orphaned TS IDs ({len(orphaned_ids):,} items)",
            data=csv_data,
            file_name="orphaned_timeseries.csv",
            mime="text/csv",
            key="download_orphaned_ts"
        )
    
    st.markdown("---")
    
    # =====================================================
    # TS TO EQUIPMENT METRIC
    # =====================================================
    st.header("Time Series-to-Equipment Contextualization")
    
    ts_to_equipment_rate = ts_metrics.get("ts_to_equipment_rate", 0)
    ts_with_equipment_link = ts_metrics.get("ts_with_equipment_link", 0)
    ts_without_equipment_link = ts_metrics.get("ts_without_equipment_link", 0)
    associated_equipment = ts_metrics.get("ts_associated_equipment", 0)
    
    col_eq_main, col_eq_info = st.columns([2, 1])
    
    with col_eq_main:
        gauge(col_eq_main, "TS Linked to Equipment", ts_to_equipment_rate, "ts_to_equipment", 
              get_status_color_ts, [0, 100], "%", key="ts_to_equipment_main",
              help_text="% of time series linked to at least one equipment.")
    
    with col_eq_info:
        st.markdown("### Why This Matters")
        st.markdown("""
        Time series linked to equipment enable:
        - Equipment-level monitoring dashboards
        - Equipment health analytics
        - Predictive maintenance insights
        """)
        
        if ts_to_equipment_rate >= 80:
            st.success(f"Good! {ts_with_equipment_link:,} TS linked to equipment.")
        elif ts_to_equipment_rate >= 50:
            st.warning(f"{ts_without_equipment_link:,} time series lack equipment links.")
        else:
            st.info(f"{ts_without_equipment_link:,} time series without equipment links.")
    
    # Equipment summary cards
    col_eq1, col_eq2, col_eq3 = st.columns(3)
    metric_card(col_eq1, "TS with Equipment Link", f"{ts_with_equipment_link:,}",
                help_text="Time series linked to at least one equipment")
    metric_card(col_eq2, "TS without Equipment", f"{ts_without_equipment_link:,}",
                help_text="Time series NOT linked to any equipment")
    metric_card(col_eq3, "Equipment with TS", f"{associated_equipment:,}",
                help_text="Number of equipment that have at least one time series linked")
    
    # CSV Download for TS without Equipment
    ts_no_equip_ids = ts_metrics.get("ts_without_equipment_ids", [])
    if ts_no_equip_ids:
        csv_data_eq = "external_id\n" + "\n".join(ts_no_equip_ids)
        st.download_button(
            label=f"Download TS without Equipment ({len(ts_no_equip_ids):,} items)",
            data=csv_data_eq,
            file_name="ts_without_equipment.csv",
            mime="text/csv",
            key="download_ts_no_equipment"
        )
    
    st.markdown("---")
    
    st.header("Additional Quality Metrics")
    
    # METRIC CARDS (GAUGES) - Row 1: Coverage and Freshness
    g1, g2, g3 = st.columns(3)
    
    # Asset Monitoring Coverage (not all assets need TS)
    gauge(g1, "Asset Monitoring Coverage", asset_monitoring_coverage, "asset_monitoring", 
          get_status_color_ts, [0, 100], "%", key="ts_asset_coverage",
          help_text="INFORMATIONAL: % of assets with TS. Low values OK - not all assets need monitoring.")
    
    # Critical Asset Coverage - show N/A if no critical assets defined
    if has_critical_assets and critical_coverage is not None:
        gauge(g2, "Critical Asset Coverage", critical_coverage, "critical_coverage", 
              get_status_color_ts, [0, 100], "%", key="ts_critical",
              help_text="% of critical assets with time series (should be 100%)")
    else:
        gauge_na(g2, "Critical Asset Coverage", "No critical assets defined", key="ts_critical_na",
                 help_text="% of critical assets with time series linked")
    
    gauge(g3, "Data Freshness (Last 30 Days)", data_freshness, "freshness", 
          get_status_color_ts, [0, 100], "%", key="ts_fresh",
          help_text="% of time series updated within the last 30 days")
    
    st.markdown("")
    
    # Row 2: Data Quality - Units
    g4, g5 = st.columns(2)
    
    # Source Unit Completeness
    gauge(g4, "Source Unit Completeness", source_unit_completeness, "unit_consistency", 
          get_status_color_ts, [0, 100], "%", key="ts_src_unit",
          help_text="% of time series with sourceUnit populated (e.g., C, mm, %)")
    
    # Target Unit Completeness (standardized unit) - in same row
    if target_unit_completeness > 0:
        gauge(g5, "Target Unit (Standardized)", target_unit_completeness, "unit_consistency", 
              get_status_color_ts, [0, 100], "%", key="ts_tgt_unit",
              help_text="% of time series with standardized unit populated")
    else:
        gauge_na(g5, "Target Unit (Standardized)", "No standardized units defined", key="ts_tgt_unit_na",
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
    | TS with Equipment Link | {ts_with_equipment_link:,} |
    | Equipment with TS | {associated_equipment:,} |
    | Total Assets | {total_assets:,} |
    | Assets with TS | {associated_assets:,} |
    | Total Critical Assets | {critical_total:,} |
    | Critical Assets with TS | {critical_with_ts:,} |
    | TS with Source Unit | {has_source_unit:,} |
    | TS with Target Unit | {has_target_unit:,} |
    | Unique Source Units | {unique_source_units:,} |
    | Fresh TS Count | {fresh_count:,} |
    """)
    
    # AI SUMMARY SECTION
    render_ai_summary_section(
        dashboard_type="Time Series Contextualization",
        metrics_data=format_timeseries_metrics(ts_metrics, hierarchy),
        system_prompt=get_timeseries_prompt(),
        key_prefix="timeseries"
    )
    
    st.markdown("---")
    st.success("Time Series Contextualization dashboard loaded from pre-computed metrics.")
