# -*- coding: utf-8 -*-
"""
Equipment-Asset Quality Dashboard.
"""

import streamlit as st

from .common import (
    get_status_color_equipment,
    metric_card,
    gauge,
    gauge_na,
)
from .ai_summary import (
    render_ai_summary_section,
    get_equipment_prompt,
    format_equipment_metrics,
)
from .reports import generate_equipment_report


def render_equipment_dashboard(metrics: dict):
    """Render the Equipment-Asset Quality dashboard tab."""
    st.title("Equipment-Asset Relationship Quality Dashboard")
    
    equipment = metrics.get("equipment_metrics", {})
    metadata = metrics.get("metadata", {})
    
    if not equipment:
        st.warning("No equipment metrics found in the data file.")
        return
    
    # Show data info
    computed_at = metadata.get("computed_at", "Unknown")
    st.info(f"[Date] Metrics computed at: {computed_at}")
    
    # Understanding the metrics
    with st.expander("**Understanding the Metrics** - Click to learn more", expanded=False):
        st.markdown("""
        **Contextualization Metrics:**
        - **Equipment Association** - % of equipment linked to an asset (should be ~100%)
        - **Asset Equipment Coverage** - % of assets with equipment (low values OK - not all assets have equipment)
        - **Critical Equipment** - % of critical equipment linked to assets (should be 100%)
        
        **Metadata Quality Metrics:**
        - **Serial Number Completeness** - % of equipment with serial number populated
        - **Manufacturer Completeness** - % of equipment with manufacturer info
        - **Type Consistency** - % of equipment where equipment type matches linked asset type
        
        **Distribution Metrics:**
        - **Avg Equipment per Asset** - Mean number of equipment items per asset
        - **Max Equipment per Asset** - Highest equipment count on a single asset
        
        *Tip: Focus on Equipment Association first - unlinked equipment cannot be found in the asset tree.*
        """)
    
    # Download Report Button
    st.download_button(
        label="Download Equipment Report (PDF)",
        data=generate_equipment_report(metrics),
        file_name="equipment_report.pdf",
        mime="application/pdf",
        use_container_width=True,
        type="primary",
        key="download_equipment_report"
    )
    
    # Extract metrics
    total_equipment = equipment.get("eq_total", 0)
    association_rate = equipment.get("eq_association_rate", 0)
    linked_eq = equipment.get("eq_linked", 0)
    unlinked_eq = equipment.get("eq_unlinked", 0)
    asset_coverage = equipment.get("eq_asset_coverage", 0)
    assets_with_eq = equipment.get("eq_assets_with_equipment", 0)
    serial_rate = equipment.get("eq_serial_completeness", 0)
    manufacturer_rate = equipment.get("eq_manufacturer_completeness", 0)
    type_consistency_rate = equipment.get("eq_type_consistency_rate", 0)
    critical_rate = equipment.get("eq_critical_contextualization")  # Can be None
    critical_total = equipment.get("eq_critical_total", 0)
    critical_linked = equipment.get("eq_critical_linked", 0)
    has_critical_equipment = equipment.get("eq_has_critical_equipment", False)
    avg_eq_per_asset = equipment.get("eq_avg_per_asset", 0)
    max_eq_per_asset = equipment.get("eq_max_per_asset", 0)
    
    st.markdown("---")
    
    # =====================================================
    # MAIN METRIC - BIG ON TOP
    # =====================================================
    st.header("Equipment-to-Asset Contextualization")
    
    col_main, col_info = st.columns([2, 1])
    
    with col_main:
        gauge(col_main, "Equipment Linked to Assets", association_rate, "association", 
              get_status_color_equipment, [0, 100], "%", key="eq_assoc_main",
              help_text="% of equipment items linked to an asset. Should be high.")
    
    with col_info:
        st.markdown("### Why This Matters")
        st.markdown("""
        Equipment without asset links are **orphaned** - they exist 
        but lack physical location context. This prevents:
        - Finding equipment on an asset
        - Maintenance planning by location
        - Impact analysis during failures
        """)
        
        if association_rate >= 95:
            st.success(f"Excellent! Only {unlinked_eq:,} equipment unlinked.")
        elif association_rate >= 80:
            st.warning(f"{unlinked_eq:,} equipment items need asset links.")
        else:
            st.error(f"{unlinked_eq:,} equipment items are orphaned!")
    
    st.markdown("---")
    
    # Summary cards
    col1, col2, col3, col4 = st.columns(4)
    metric_card(col1, "Total Equipment", f"{total_equipment:,}",
                help_text="Total number of unique equipment items")
    metric_card(col2, "Linked Equipment", f"{linked_eq:,}",
                help_text="Equipment items that have a valid asset link")
    metric_card(col3, "Unlinked Equipment", f"{unlinked_eq:,}",
                help_text="Equipment items missing an asset link (need contextualization)")
    metric_card(col4, "Assets with Equipment", f"{assets_with_eq:,}",
                help_text="Number of assets that have at least one equipment linked")
    
    # CSV Download for Orphaned Equipment
    orphaned_eq_ids = equipment.get("eq_orphaned_ids", [])
    if orphaned_eq_ids:
        csv_data = "external_id\n" + "\n".join(orphaned_eq_ids)
        st.download_button(
            label=f"Download Unlinked Equipment IDs ({len(orphaned_eq_ids):,} items)",
            data=csv_data,
            file_name="unlinked_equipment.csv",
            mime="text/csv",
            key="download_orphaned_equipment"
        )
    
    st.markdown("---")
    
    # GAUGES - Additional metrics
    st.subheader("Additional Quality Metrics")
    g1, g2, g3 = st.columns(3)
    gauge(g1, "Asset Equipment Coverage", asset_coverage, "coverage", 
          get_status_color_equipment, [0, 100], "%", key="eq_coverage",
          help_text="INFORMATIONAL: % of assets with equipment. Low values OK - not all assets have equipment.")
    gauge(g2, "Serial Number Completeness", serial_rate, "serial", 
          get_status_color_equipment, [0, 100], "%", key="eq_serial",
          help_text="% of equipment with serial number populated")
    gauge(g3, "Manufacturer Data Quality", manufacturer_rate, "manufacturer", 
          get_status_color_equipment, [0, 100], "%", key="eq_manu",
          help_text="% of equipment with manufacturer info populated")
    
    st.write("")
    
    g4, g5, g6 = st.columns(3)
    gauge(g4, "Type Consistency", type_consistency_rate, "type_consistency", 
          get_status_color_equipment, [0, 100], "%", key="eq_type",
          help_text="% of equipment where type matches linked asset type")
    
    # Critical Equipment - show N/A if no critical equipment is defined
    if has_critical_equipment and critical_rate is not None:
        gauge(g5, "Critical Equipment", critical_rate, "critical", 
              get_status_color_equipment, [0, 100], "%", key="eq_critical",
              help_text="% of critical equipment linked to an asset (should be 100%)")
    else:
        gauge_na(g5, "Critical Equipment", "No critical equipment defined", key="eq_critical_na",
                 help_text="% of critical equipment linked to an asset")
    
    st.markdown("---")
    
    # Additional Stats
    st.subheader("Equipment Distribution")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Avg Equipment per Asset", f"{avg_eq_per_asset:.2f}",
              help="Average number of equipment items per asset")
    c2.metric("Max Equipment per Asset", f"{max_eq_per_asset:,}",
              help="Maximum equipment linked to a single asset")
    c3.metric("Critical Equipment Total", f"{critical_total:,}",
              help="Equipment items marked as critical")
    c4.metric("Critical Equipment Linked", f"{critical_linked:,}",
              help="Critical equipment with valid asset links")
    

    st.markdown("---")
    
    # =====================================================
    # COGNITE ACTIVITY METRICS
    # =====================================================
    st.header("Equipment → CogniteActivity")
    st.markdown("*How many equipment items are linked to activities?*")
    
    # Extract activity metrics
    has_activities = equipment.get("eq_has_activities", False)
    total_activities = equipment.get("eq_total_activities", 0)
    eq_with_activities = equipment.get("eq_with_activities", 0)
    eq_activity_rate = equipment.get("eq_activity_rate")
    activities_with_eq = equipment.get("eq_activities_with_equipment", 0)
    activity_eq_rate = equipment.get("eq_activity_equipment_rate")
    assets_with_activities = equipment.get("eq_assets_with_activities", 0)
    asset_activity_rate = equipment.get("eq_asset_activity_rate")
    
    if has_activities:
        # Summary row
        a1, a2, a3, a4 = st.columns(4)
        metric_card(a1, "Total Activities", f"{total_activities:,}",
                    help_text="Total CogniteActivity instances processed")
        metric_card(a2, "Equipment with Activities", f"{eq_with_activities:,}",
                    help_text="Equipment items linked to at least one activity")
        metric_card(a3, "Assets with Activities", f"{assets_with_activities:,}",
                    help_text="Assets linked to at least one activity")
        metric_card(a4, "Activities with Equipment", f"{activities_with_eq:,}",
                    help_text="Activities that have equipment links")
        
        st.write("")
        
        # Gauges
        ag1, ag2, ag3 = st.columns(3)
        
        if eq_activity_rate is not None:
            gauge(ag1, "Equipment → Activity Rate", eq_activity_rate, "eq_activity",
                  get_status_color_equipment, [0, 100], "%", key="eq_activity_rate",
                  help_text="% of equipment with at least one activity")
        else:
            gauge_na(ag1, "Equipment → Activity Rate", "No data", key="eq_activity_rate_na")
        
        if activity_eq_rate is not None:
            gauge(ag2, "Activity → Equipment Rate", activity_eq_rate, "activity_eq",
                  get_status_color_equipment, [0, 100], "%", key="activity_eq_rate",
                  help_text="% of activities linked to equipment")
        else:
            gauge_na(ag2, "Activity → Equipment Rate", "No data", key="activity_eq_rate_na")
        
        if asset_activity_rate is not None:
            gauge(ag3, "Asset → Activity Rate", asset_activity_rate, "asset_activity",
                  get_status_color_equipment, [0, 100], "%", key="asset_activity_rate",
                  help_text="% of assets with at least one activity")
        else:
            gauge_na(ag3, "Asset → Activity Rate", "No data", key="asset_activity_rate_na")
    else:
        st.info("""
        **No CogniteActivity Data**
        
        No activity data was found. This could mean:
        - CogniteActivity view is not deployed
        - No activities exist in the system
        - Activity metrics collection is not enabled
        """)

    st.markdown("---")
    
    # Summary
    if unlinked_eq == 0:
        st.success("[!] All equipment is properly linked to assets!")
    else:
        st.warning(f"{unlinked_eq:,} equipment items are missing asset links.")
    
    # AI SUMMARY SECTION
    hierarchy = metrics.get("hierarchy_metrics", {})
    render_ai_summary_section(
        dashboard_type="Equipment-Asset Quality",
        metrics_data=format_equipment_metrics(equipment, hierarchy),
        system_prompt=get_equipment_prompt(),
        key_prefix="equipment"
    )
    
    st.markdown("---")
    st.success("Equipment-Asset dashboard loaded from pre-computed metrics.")
