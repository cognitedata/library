# -*- coding: utf-8 -*-
"""
"Others" Dashboard - IDI Views Asset Linkage Quality.

Displays metrics for miscellaneous IDI views that don't fit into
the main dashboard categories:
- IDI_LIMS, IDI_Dynamo, IDI_Runlog, IDI_ELOG_Val
- IDI_DownGradeSituation, IDI_MOC, IDI_BOM, IDI_Permits
- IDI_InspectionTask, IDI_InspectionRec, IDI_InspectionRepo
- IDI_PRD_ANAL_RBI, IDI_RBI_PIPI, IDI_RCA_Analysis, IDI_RCA_Legacy
"""

import streamlit as st
import pandas as pd

from .common import (
    get_status_color_equipment,
    metric_card,
    gauge,
    gauge_na,
)
from .reports import generate_others_report


def get_status_icon(status: str) -> str:
    """Get icon for view status."""
    icons = {
        "ok": "✅",
        "empty": "⚪",
        "not_found": "❌",
        "error": "⚠️",
    }
    return icons.get(status, "❓")


def get_rate_color(rate: float) -> str:
    """Get color based on rate value."""
    if rate is None:
        return "gray"
    if rate >= 90:
        return "green"
    if rate >= 70:
        return "orange"
    return "red"


def render_others_dashboard(metrics: dict):
    """Render the Others IDI Views dashboard tab."""
    st.title("Other IDI Views - Asset Linkage Quality")
    st.markdown("*Metrics for miscellaneous IDI data sources linked to assets*")
    
    others = metrics.get("others_metrics", {})
    metadata = metrics.get("metadata", {})
    
    if not others or not others.get("others_has_data", False):
        st.warning("""
        **No 'Others' Metrics Data Found**
        
        This could mean:
        1. The 'Others' metrics collection is not enabled in the function, or
        2. The metrics were run without the **Others** section (e.g. `--only 3d`), or
        3. None of the IDI views contain data.
        
        **To get Others metrics:**
        - **Function:** Ensure Others is enabled in the function configuration.
        - **Local script:** Run with the Others section included, e.g. `python run_metrics.py` (full) or `python run_metrics.py --only others`.
        """)
        return
    
    # Show data info
    computed_at = metadata.get("computed_at", "Unknown")
    st.info(f"Metrics computed at: {computed_at}")
    
    # Understanding the metrics
    with st.expander("**Understanding the Metrics** - Click to learn more", expanded=False):
        st.markdown("""
        **Asset Linkage Rate:**
        - Shows what percentage of records in each view are linked to an asset
        - High rates (>90%) indicate good data contextualization
        - Low rates may indicate data quality issues or missing relationships
        
        **Views Covered:**
        - **LIMS**: Lab Information Management System data
        - **Dynamo**: Alarm and event data
        - **Runlog**: Operational run log entries
        - **ELOG_Val**: Electronic log values
        - **DownGradeSituation**: Downgrade situations
        - **MOC**: Management of Change records
        - **BOM**: Bill of Materials
        - **Permits**: Work permits
        - **Inspection***: Inspection tasks, records, and reports
        - **RBI***: Risk-Based Inspection data
        - **RCA***: Root Cause Analysis data
        
        *Tip: Focus on views critical to your operations - not all views need 100% asset linkage.*
        """)
    
    # Download Report Button
    st.download_button(
        label="Download Others Report (PDF)",
        data=generate_others_report(metrics),
        file_name="others_idi_report.pdf",
        mime="application/pdf",
        use_container_width=True,
        type="primary",
        key="download_others_report"
    )
    
    st.markdown("---")
    
    # =====================================================
    # SUMMARY SECTION
    # =====================================================
    st.header("Summary")
    
    col1, col2, col3, col4 = st.columns(4)
    
    total_views = others.get("others_total_views", 0)
    views_with_data = others.get("others_views_with_data", 0)
    views_not_found = others.get("others_views_not_found", 0)
    total_instances = others.get("others_total_instances", 0)
    overall_rate = others.get("others_overall_asset_rate")
    
    metric_card(col1, "Total Views", f"{total_views}",
                help_text="Number of IDI views configured for monitoring")
    metric_card(col2, "Views with Data", f"{views_with_data}",
                help_text="Views that have at least one record")
    metric_card(col3, "Total Records", f"{total_instances:,}",
                help_text="Sum of all records across all views")
    
    # Overall rate as gauge
    with col4:
        if overall_rate is not None:
            st.metric("Overall Asset Link Rate", f"{overall_rate}%",
                      help="Average asset linkage rate across all views")
        else:
            st.metric("Overall Asset Link Rate", "N/A")
    
    st.markdown("---")
    
    # =====================================================
    # PER-VIEW METRICS TABLE
    # =====================================================
    st.header("Per-View Metrics")
    
    views = others.get("others_views", [])
    
    if not views:
        st.info("No view data available.")
        return
    
    # Create DataFrame for display
    table_data = []
    for view in views:
        status = view.get("status", "unknown")
        rate = view.get("asset_link_rate")
        
        table_data.append({
            "Status": get_status_icon(status),
            "View": view.get("display_name", view.get("view_id", "?")),
            "View ID": view.get("view_id", ""),
            "Total": view.get("unique_instances", 0),
            "With Asset": view.get("instances_with_asset", 0),
            "Without Asset": view.get("instances_without_asset", 0),
            "Asset Rate %": rate if rate is not None else "N/A",
            "Unique Assets": view.get("unique_assets_linked", 0),
        })
    
    df = pd.DataFrame(table_data)
    
    # Style the dataframe
    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Status": st.column_config.TextColumn("", width="small"),
            "View": st.column_config.TextColumn("View Name", width="medium"),
            "View ID": st.column_config.TextColumn("External ID", width="medium"),
            "Total": st.column_config.NumberColumn("Total Records", format="%d"),
            "With Asset": st.column_config.NumberColumn("With Asset", format="%d"),
            "Without Asset": st.column_config.NumberColumn("Without Asset", format="%d"),
            "Asset Rate %": st.column_config.NumberColumn("Asset Rate", format="%.1f%%"),
            "Unique Assets": st.column_config.NumberColumn("Unique Assets", format="%d"),
        }
    )
    
    # CSV Download for unlinked (without asset) IDs per view
    with st.expander("Download unlinked IDs (CSV)", expanded=False):
        for view in views:
            unlinked_ids = view.get("unlinked_asset_ids", [])
            without = view.get("instances_without_asset", 0)
            if without > 0 and unlinked_ids:
                csv_data = "external_id\n" + "\n".join(unlinked_ids)
                st.download_button(
                    label=f"{view.get('display_name', view.get('view_id'))}: {len(unlinked_ids):,} unlinked",
                    data=csv_data,
                    file_name=f"{view.get('view_id', 'view')}_unlinked_asset.csv",
                    mime="text/csv",
                    key=f"download_others_unlinked_{view.get('view_id')}"
                )
    
    st.markdown("---")
    
    # =====================================================
    # DETAILED VIEW CARDS
    # =====================================================
    st.header("Detailed View Breakdown")
    
    # Group views by status
    ok_views = [v for v in views if v.get("status") == "ok"]
    empty_views = [v for v in views if v.get("status") == "empty"]
    error_views = [v for v in views if v.get("status") in ["not_found", "error"]]
    
    if ok_views:
        st.subheader("✅ Views with Data")
        
        # Create gauges in rows of 3
        for i in range(0, len(ok_views), 3):
            cols = st.columns(3)
            for j, view in enumerate(ok_views[i:i+3]):
                with cols[j]:
                    rate = view.get("asset_link_rate") or 0
                    gauge(
                        cols[j],
                        view.get("display_name", view.get("view_id")),
                        rate,
                        f"others_{view.get('view_id')}",
                        get_status_color_equipment,
                        [0, 100],
                        "%",
                        key=f"others_gauge_{view.get('view_id')}",
                        help_text=f"Total: {view.get('unique_instances', 0):,}, "
                                  f"Linked: {view.get('instances_with_asset', 0):,}"
                    )
        st.write("")
    
    if empty_views:
        with st.expander(f"⚪ Empty Views ({len(empty_views)})", expanded=False):
            st.markdown("The following views exist but contain no data:")
            for view in empty_views:
                st.markdown(f"- **{view.get('display_name')}** (`{view.get('view_id')}`)")
    
    if error_views:
        with st.expander(f"❌ Views with Errors ({len(error_views)})", expanded=False):
            st.markdown("The following views could not be processed:")
            for view in error_views:
                error_msg = view.get("error", "Unknown error")
                st.markdown(f"- **{view.get('display_name')}** (`{view.get('view_id')}`): {error_msg}")
    
    st.markdown("---")
    st.success("Others dashboard loaded from pre-computed metrics.")
