# -*- coding: utf-8 -*-
"""
Maintenance IDI Views Dashboard.

Displays metrics for maintenance-related IDI views:
- IDI_MaintenancePlan (asset, work order)
- IDI_MaintenancePlanItem (asset, maintenance plan, work order)
- IDI_Maintenance_Order (asset, maintenance plan, maintenance plan item, operations)
- IDI_Operations (work order)
- IDI_MaximoWorkorder (asset)
- IDI_Notifications (asset, work order)
"""

import streamlit as st
import pandas as pd

from .common import (
    get_status_color_maintenance,
    metric_card,
    gauge,
    gauge_na,
)
from .ai_summary import (
    render_ai_summary_section,
    get_maintenance_prompt,
)
from .reports import generate_maintenance_idi_report


def get_status_icon(status: str) -> str:
    """Get icon for view status."""
    icons = {
        "ok": "✅",
        "empty": "⚪",
        "not_found": "❌",
        "error": "⚠️",
    }
    return icons.get(status, "❓")


def format_maintenance_idi_metrics(maint: dict) -> str:
    """Format maintenance IDI metrics for AI summary."""
    if not maint:
        return "No maintenance IDI metrics available."
    
    lines = []
    lines.append("## Maintenance IDI Views Summary")
    lines.append(f"- Total Views: {maint.get('maint_idi_total_views', 0)}")
    lines.append(f"- Views with Data: {maint.get('maint_idi_views_with_data', 0)}")
    lines.append(f"- Total Instances: {maint.get('maint_idi_total_instances', 0):,}")
    lines.append(f"- Assets with Maintenance: {maint.get('maint_idi_assets_with_maintenance', 0):,}")
    lines.append(f"- Asset Coverage Rate: {maint.get('maint_idi_asset_coverage_rate', 'N/A')}%")
    
    views = maint.get("maint_idi_views", [])
    if views:
        lines.append("\n### Per-View Breakdown:")
        for view in views:
            if view.get("status") == "ok":
                lines.append(f"\n**{view.get('display_name')}** ({view.get('unique_instances', 0):,} records)")
                for rel_name, rel_data in view.get("relations", {}).items():
                    rate = rel_data.get("rate", "N/A")
                    lines.append(f"  - {rel_name}: {rate}% ({rel_data.get('count', 0):,} linked)")
    
    return "\n".join(lines)


def render_maintenance_dashboard(metrics: dict):
    """Render the Maintenance IDI Views dashboard tab."""
    st.title("Maintenance Data Quality Dashboard")
    st.markdown("*Metrics for IDI Maintenance Views*")
    
    # Try new IDI metrics first, fall back to legacy metrics
    maint_idi = metrics.get("maintenance_idi_metrics", {})
    maint_legacy = metrics.get("maintenance_metrics", {})
    metadata = metrics.get("metadata", {})
    
    # Determine which metrics to use
    use_idi = maint_idi and maint_idi.get("maint_idi_has_data", False)
    use_legacy = not use_idi and maint_legacy and (
        maint_legacy.get("maint_has_notifications", False) or 
        maint_legacy.get("maint_has_orders", False)
    )
    
    if not use_idi and not use_legacy:
        st.warning("""
        **No Maintenance Data Found**
        
        No maintenance metrics found. This could mean:
        1. Maintenance IDI views are not deployed in your space
        2. The views contain no data
        3. Metrics haven't been computed yet
        
        **Expected Views:**
        - IDI_MaintenancePlan
        - IDI_MaintenancePlanItem
        - IDI_Maintenance_Order
        - IDI_Operations
        - IDI_MaximoWorkorder
        - IDI_Notifications
        """)
        return
    
    # Show data info
    computed_at = metadata.get("computed_at", "Unknown")
    st.info(f"Metrics computed at: {computed_at}")
    
    if use_idi:
        _render_maintenance_idi_dashboard(maint_idi, metadata, metrics)
    else:
        _render_legacy_maintenance_info()


def _render_legacy_maintenance_info():
    """Show info about legacy maintenance metrics."""
    st.info("""
    **Legacy Maintenance Metrics Detected**
    
    The current metrics use the old RMDM v1 format. Please re-run the metrics 
    function to generate the new IDI maintenance metrics.
    """)


def _render_maintenance_idi_dashboard(maint: dict, metadata: dict, full_metrics: dict = None):
    """Render the new IDI maintenance dashboard."""
    
    # Understanding the metrics
    with st.expander("**Understanding the Metrics** - Click to learn more", expanded=False):
        st.markdown("""
        **Maintenance Views:**
        - **IDI_MaintenancePlan** - Maintenance plans linked to assets and work orders
        - **IDI_MaintenancePlanItem** - Individual items in maintenance plans
        - **IDI_Maintenance_Order** - Maintenance work orders with multi-relation tracking
        - **IDI_Operations** - Operational activities linked to work orders
        - **IDI_MaximoWorkorder** - Work orders from Maximo system
        - **IDI_Notifications** - Maintenance notifications linked to assets/work orders
        
        **Key Metrics per View:**
        - **Asset Link Rate** - % of records linked to an asset (should be high)
        - **Work Order Link Rate** - % of records linked to work orders
        - **Plan Link Rate** - % of items linked to maintenance plans
        
        *Tip: High asset linkage is critical for maintenance traceability!*
        """)
    
    # Download Report Button
    if full_metrics:
        st.download_button(
            label="Download Maintenance IDI Report (PDF)",
            data=generate_maintenance_idi_report(full_metrics),
            file_name="maintenance_idi_report.pdf",
            mime="application/pdf",
            use_container_width=True,
            type="primary",
            key="download_maintenance_idi_report"
        )
    
    st.markdown("---")
    
    # =====================================================
    # SUMMARY SECTION
    # =====================================================
    st.header("Maintenance Summary")
    
    col1, col2, col3, col4 = st.columns(4)
    
    total_views = maint.get("maint_idi_total_views", 0)
    views_with_data = maint.get("maint_idi_views_with_data", 0)
    total_instances = maint.get("maint_idi_total_instances", 0)
    assets_with_maint = maint.get("maint_idi_assets_with_maintenance", 0)
    asset_coverage = maint.get("maint_idi_asset_coverage_rate")
    
    metric_card(col1, "Total Views", f"{total_views}",
                help_text="Number of maintenance views monitored")
    metric_card(col2, "Views with Data", f"{views_with_data}",
                help_text="Views that have records")
    metric_card(col3, "Total Records", f"{total_instances:,}",
                help_text="Sum of all maintenance records")
    metric_card(col4, "Assets with Maintenance", f"{assets_with_maint:,}",
                help_text="Unique assets linked to maintenance data")
    
    st.markdown("---")
    
    # =====================================================
    # MAIN METRIC - ASSET COVERAGE
    # =====================================================
    st.header("Asset Maintenance Coverage")
    
    col_main, col_info = st.columns([2, 1])
    
    with col_main:
        if asset_coverage is not None:
            gauge(col_main, "Assets with Maintenance Data", asset_coverage, "maint_coverage",
                  get_status_color_maintenance, [0, 100], "%", key="maint_idi_coverage",
                  help_text="% of assets that have at least one maintenance record")
        else:
            gauge_na(col_main, "Assets with Maintenance Data", "No asset data",
                     key="maint_idi_coverage_na")
    
    with col_info:
        st.markdown("### Why This Matters")
        st.markdown("""
        Assets without maintenance data may indicate:
        - Missing data integration
        - New assets not yet in maintenance planning
        - Data quality issues in source systems
        
        Not all assets need maintenance - focus on critical equipment.
        """)
    
    st.markdown("---")
    
    # =====================================================
    # PER-VIEW METRICS
    # =====================================================
    st.header("Per-View Metrics")
    
    views = maint.get("maint_idi_views", [])
    
    if not views:
        st.info("No view data available.")
        return
    
    # Create tabs for each view with data
    ok_views = [v for v in views if v.get("status") == "ok" and v.get("unique_instances", 0) > 0]
    
    if ok_views:
        # Create a tab for each view
        tab_names = [v.get("display_name", v.get("view_id", "?")) for v in ok_views]
        tabs = st.tabs(tab_names)
        
        for tab, view in zip(tabs, ok_views):
            with tab:
                _render_view_details(view)
    
    # Show views without data
    empty_views = [v for v in views if v.get("status") in ["empty", "not_found", "error"]]
    if empty_views:
        with st.expander(f"Views without Data ({len(empty_views)})", expanded=False):
            for view in empty_views:
                status = view.get("status", "unknown")
                icon = get_status_icon(status)
                error = view.get("error", "")
                st.markdown(
                    f"{icon} **{view.get('display_name')}** "
                    f"(`{view.get('view_id')}`) - {status}"
                    f"{': ' + error if error else ''}"
                )
    
    st.markdown("---")
    
    # AI Summary
    render_ai_summary_section(
        dashboard_type="Maintenance Data Quality",
        metrics_data=format_maintenance_idi_metrics(maint),
        system_prompt=get_maintenance_prompt(),
        key_prefix="maintenance_idi"
    )
    
    st.markdown("---")
    st.success("Maintenance dashboard loaded from pre-computed metrics.")


def _render_view_details(view: dict):
    """Render detailed metrics for a single view."""
    st.subheader(f"{view.get('display_name')} Details")
    
    # Summary row
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Records", f"{view.get('unique_instances', 0):,}")
    col2.metric("Duplicates", f"{view.get('duplicates', 0):,}")
    col3.metric("Unique Assets Linked", f"{view.get('relations', {}).get('asset', {}).get('unique_links', 0):,}")
    
    st.markdown("### Relation Metrics")
    
    # Get relations
    relations = view.get("relations", {})
    
    if not relations:
        st.info("No relation metrics available for this view.")
        return
    
    # Create gauges for each relation
    relation_cols = st.columns(min(len(relations), 3))
    
    for i, (rel_name, rel_data) in enumerate(relations.items()):
        col_idx = i % 3
        with relation_cols[col_idx]:
            rate = rel_data.get("rate")
            count = rel_data.get("count", 0)
            without = rel_data.get("without", 0)
            unique_links = rel_data.get("unique_links", 0)
            
            # Friendly name for relation
            friendly_name = rel_name.replace("_", " ").title()
            
            if rate is not None:
                gauge(
                    relation_cols[col_idx],
                    f"→ {friendly_name}",
                    rate,
                    f"{view.get('view_id')}_{rel_name}",
                    get_status_color_maintenance,
                    [0, 100],
                    "%",
                    key=f"maint_gauge_{view.get('view_id')}_{rel_name}",
                    help_text=f"Linked: {count:,}, Without: {without:,}, Unique: {unique_links:,}"
                )
            else:
                st.metric(f"→ {friendly_name}", "N/A")
    
    # Table breakdown
    st.markdown("### Breakdown Table")
    
    table_data = []
    for rel_name, rel_data in relations.items():
        table_data.append({
            "Relation": rel_name,
            "Linked": rel_data.get("count", 0),
            "Not Linked": rel_data.get("without", 0),
            "Rate %": rel_data.get("rate", "N/A"),
            "Unique Targets": rel_data.get("unique_links", 0),
        })
    
    df = pd.DataFrame(table_data)
    st.dataframe(df, use_container_width=True, hide_index=True)
    
    # CSV Download for records without asset link
    unlinked_ids = view.get("unlinked_asset_ids", [])
    asset_rel = relations.get("asset", {})
    without_asset = asset_rel.get("without", 0)
    if without_asset > 0 and unlinked_ids:
        csv_data = "external_id\n" + "\n".join(unlinked_ids)
        st.download_button(
            label=f"Download Unlinked to Asset ({len(unlinked_ids):,} items)",
            data=csv_data,
            file_name=f"{view.get('view_id', 'view')}_unlinked_asset.csv",
            mime="text/csv",
            key=f"download_maint_unlinked_{view.get('view_id')}"
        )