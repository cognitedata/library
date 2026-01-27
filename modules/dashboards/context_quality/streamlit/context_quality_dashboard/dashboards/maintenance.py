# -*- coding: utf-8 -*-
"""
Maintenance Workflow Quality Dashboard (RMDM v1).
"""

import streamlit as st

from .common import (
    get_status_color_maintenance,
    metric_card,
    gauge,
    gauge_na,
)
from .ai_summary import (
    render_ai_summary_section,
    get_maintenance_prompt,
    format_maintenance_metrics,
)


def render_maintenance_dashboard(metrics: dict):
    """Render the Maintenance Workflow Quality dashboard tab."""
    st.title("Maintenance Workflow Quality Dashboard")
    st.markdown("*Based on RMDM v1 Data Model*")
    
    maintenance = metrics.get("maintenance_metrics", {})
    metadata = metrics.get("metadata", {})
    
    # Check if maintenance metrics are enabled
    config = metadata.get("config", {})
    maintenance_enabled = config.get("enable_maintenance_metrics", True)
    
    if not maintenance_enabled:
        st.warning("""
        **Maintenance Metrics Disabled**
        
        Maintenance workflow metrics are disabled in the function configuration.
        To enable, set `enable_maintenance_metrics: true` in the function input.
        """)
        return
    
    if not maintenance or not maintenance.get("maint_has_notifications", False) and not maintenance.get("maint_has_orders", False):
        st.warning("""
        **No Maintenance Data Found**
        
        No notifications or maintenance orders found in the RMDM v1 data model.
        """)
        
        st.info("""
        **Prerequisites:**
        
        1. **RMDM v1 must be deployed** - Ensure the RMDM v1 data model is available in your CDF project
        2. **Views must be populated** - The following views need data in the `rmdm` space:
           - `Notification`
           - `MaintenanceOrder`
           - `FailureNotification`
        
        **Using a different RMDM model?**
        
        If your RMDM model uses a different space name or view names, you can configure them in:
        
        [Folder] `metrics/common.py` -> Lines **40-48** (DEFAULT_CONFIG section)
        
        ```python
        "notification_view_space": "your_space",
        "notification_view_external_id": "YourNotificationView",
        "maintenance_order_view_space": "your_space",
        "maintenance_order_view_external_id": "YourMaintenanceOrderView",
        "failure_notification_view_space": "your_space",
        "failure_notification_view_external_id": "YourFailureNotificationView",
        ```
        
        Or pass these as function input when calling the function.
        """)
        return
    
    # Show data info
    computed_at = metadata.get("computed_at", "Unknown")
    st.info(f"[Date] Metrics computed at: {computed_at}")
    
    # Extract metrics
    total_notifications = maintenance.get("maint_total_notifications", 0)
    total_orders = maintenance.get("maint_total_orders", 0)
    total_failure_notif = maintenance.get("maint_total_failure_notifications", 0)
    
    # Summary cards
    col1, col2, col3, col4 = st.columns(4)
    metric_card(col1, "Total Notifications", f"{total_notifications:,}",
                help_text="Total maintenance notifications in RMDM")
    metric_card(col2, "Total Work Orders", f"{total_orders:,}",
                help_text="Total maintenance orders (work orders) in RMDM")
    metric_card(col3, "Failure Notifications", f"{total_failure_notif:,}",
                help_text="Notifications with failure analysis data")
    metric_card(col4, "Orders Completed", f"{maintenance.get('maint_orders_completed', 0):,}",
                help_text="Work orders with actualEndTime (completed)")
    
    st.markdown("---")
    
    # =========================================
    # SECTION 1: Notification Linkage Metrics
    # =========================================
    st.header("Notification Linkage")
    st.markdown("*How well are notifications linked to work orders, assets, and equipment?*")
    st.caption("Note: Not all notifications require a work order - low values are acceptable.")
    
    g1, g2, g3 = st.columns(3)
    
    # 1. Notification -> Work Order Linkage (INFORMATIONAL)
    notif_order_rate = maintenance.get("maint_notif_to_order_rate")
    if notif_order_rate is not None and total_notifications > 0:
        gauge(g1, "Notification -> Work Order", notif_order_rate, "notif_order", 
              get_status_color_maintenance, [0, 100], "%", key="maint_notif_order",
              help_text="INFORMATIONAL: % with WO. Low values OK - not all notifications need a work order.")
    else:
        gauge_na(g1, "Notification -> Work Order", "No notifications", key="maint_notif_order_na",
                 help_text="% of notifications linked to a maintenance order")
    
    # 2. Notification -> Asset Linkage
    notif_asset_rate = maintenance.get("maint_notif_to_asset_rate")
    if notif_asset_rate is not None and total_notifications > 0:
        gauge(g2, "Notification -> Asset", notif_asset_rate, "notif_asset", 
              get_status_color_maintenance, [0, 100], "%", key="maint_notif_asset",
              help_text="% of notifications linked to an asset")
    else:
        gauge_na(g2, "Notification -> Asset", "No notifications", key="maint_notif_asset_na",
                 help_text="% of notifications linked to an asset")
    
    # 3. Notification -> Equipment Linkage
    notif_eq_rate = maintenance.get("maint_notif_to_equipment_rate")
    if notif_eq_rate is not None and total_notifications > 0:
        gauge(g3, "Notification -> Equipment", notif_eq_rate, "notif_equipment", 
              get_status_color_maintenance, [0, 100], "%", key="maint_notif_eq",
              help_text="% of notifications linked to equipment")
    else:
        gauge_na(g3, "Notification -> Equipment", "No notifications", key="maint_notif_eq_na",
                 help_text="% of notifications linked to equipment")
    
    st.markdown("---")
    
    # =========================================
    # SECTION 2: Work Order Quality (CRITICAL METRICS)
    # =========================================
    st.header("[Note] Work Order Quality")
    st.markdown("*Critical metrics - work orders should be linked to notifications and assets.*")
    st.caption("All work orders should originate from a notification and be linked to an asset.")
    
    # Row 1: Critical work order metrics
    g4, g5 = st.columns(2)
    
    # Work Order -> Notification Linkage (CRITICAL - should be ~100%)
    order_notif_rate = maintenance.get("maint_order_to_notif_rate")
    if order_notif_rate is not None and total_orders > 0:
        gauge(g4, "Work Order -> Notification", order_notif_rate, "order_notif", 
              get_status_color_maintenance, [0, 100], "%", key="maint_order_notif",
              help_text="CRITICAL: Should be ~100%. All WOs should originate from a notification.")
    else:
        gauge_na(g4, "Work Order -> Notification", "No orders", key="maint_order_notif_na",
                 help_text="% of work orders that have a linked notification")
    
    # Work Order -> Asset Coverage (CRITICAL - should be ~100%)
    order_asset_rate = maintenance.get("maint_order_to_asset_rate")
    if order_asset_rate is not None and total_orders > 0:
        gauge(g5, "Work Order -> Asset", order_asset_rate, "order_asset", 
              get_status_color_maintenance, [0, 100], "%", key="maint_order_asset",
              help_text="CRITICAL: Should be ~100%. All WOs should be linked to an asset.")
    else:
        gauge_na(g5, "Work Order -> Asset", "No orders", key="maint_order_asset_na",
                 help_text="% of work orders linked to an asset")
    
    st.write("")
    
    # Row 2: Other work order metrics
    g6, g7 = st.columns(2)
    
    # Work Order -> Equipment Coverage
    order_eq_rate = maintenance.get("maint_order_to_equipment_rate")
    if order_eq_rate is not None and total_orders > 0:
        gauge(g6, "Work Order -> Equipment", order_eq_rate, "order_equipment", 
              get_status_color_maintenance, [0, 100], "%", key="maint_order_eq",
              help_text="% of work orders linked to equipment")
    else:
        gauge_na(g6, "Work Order -> Equipment", "No orders", key="maint_order_eq_na",
                 help_text="% of work orders linked to equipment")
    
    # Work Order Completion Rate
    order_completion_rate = maintenance.get("maint_order_completion_rate")
    if order_completion_rate is not None and total_orders > 0:
        gauge(g7, "Order Completion Rate", order_completion_rate, "order_completion", 
              get_status_color_maintenance, [0, 100], "%", key="maint_order_complete",
              help_text="% of work orders with actualEndTime (completed)")
    else:
        gauge_na(g7, "Order Completion Rate", "No orders", key="maint_order_complete_na",
                 help_text="% of work orders with actualEndTime")
    
    st.markdown("---")
    
    # =========================================
    # SECTION 3: Failure Analysis Documentation
    # =========================================
    st.header("Failure Analysis Documentation")
    st.markdown("*How well is failure data documented in failure notifications?*")
    
    f1, f2, f3 = st.columns(3)
    
    # Failure Mode Documentation Rate
    failure_mode_rate = maintenance.get("maint_failure_mode_rate")
    if failure_mode_rate is not None and total_failure_notif > 0:
        gauge(f1, "Failure Mode Documentation", failure_mode_rate, "failure_mode", 
              get_status_color_maintenance, [0, 100], "%", key="maint_fail_mode",
              help_text="% of failure notifications with failure mode")
    else:
        gauge_na(f1, "Failure Mode Documentation", "No failure notifications", key="maint_fail_mode_na",
                 help_text="% of failure notifications with failure mode")
    
    # Failure Mechanism Documentation Rate
    failure_mech_rate = maintenance.get("maint_failure_mechanism_rate")
    if failure_mech_rate is not None and total_failure_notif > 0:
        gauge(f2, "Failure Mechanism Doc", failure_mech_rate, "failure_mechanism", 
              get_status_color_maintenance, [0, 100], "%", key="maint_fail_mech",
              help_text="% of failure notifications with failure mechanism")
    else:
        gauge_na(f2, "Failure Mechanism Doc", "No failure notifications", key="maint_fail_mech_na",
                 help_text="% of failure notifications with failure mechanism")
    
    # Failure Cause Documentation Rate
    failure_cause_rate = maintenance.get("maint_failure_cause_rate")
    if failure_cause_rate is not None and total_failure_notif > 0:
        gauge(f3, "Failure Cause Documentation", failure_cause_rate, "failure_cause", 
              get_status_color_maintenance, [0, 100], "%", key="maint_fail_cause",
              help_text="% of failure notifications with failure cause")
    else:
        gauge_na(f3, "Failure Cause Documentation", "No failure notifications", key="maint_fail_cause_na",
                 help_text="% of failure notifications with failure cause")
    
    st.markdown("---")
    
    # =========================================
    # SECTION 4: Maintenance Coverage (INFORMATIONAL)
    # =========================================
    st.header("Maintenance Coverage")
    st.markdown("*What percentage of assets/equipment have maintenance records?*")
    st.caption("Note: Not all assets/equipment require maintenance records - low values are normal.")
    
    m1, m2, _ = st.columns(3)
    
    # Asset Maintenance Coverage (INFORMATIONAL)
    asset_maint_rate = maintenance.get("maint_asset_coverage_rate")
    if asset_maint_rate is not None:
        gauge(m1, "Asset Maintenance Coverage", asset_maint_rate, "asset_maint_coverage", 
              get_status_color_maintenance, [0, 100], "%", key="maint_asset_cov",
              help_text="INFORMATIONAL: % of assets with maintenance records. Low values are normal.")
    else:
        gauge_na(m1, "Asset Maintenance Coverage", "No asset data", key="maint_asset_cov_na",
                 help_text="% of assets with maintenance records")
    
    # Equipment Maintenance Coverage (INFORMATIONAL)
    eq_maint_rate = maintenance.get("maint_equipment_coverage_rate")
    if eq_maint_rate is not None:
        gauge(m2, "Equipment Maintenance Coverage", eq_maint_rate, "equipment_maint_coverage", 
              get_status_color_maintenance, [0, 100], "%", key="maint_eq_cov",
              help_text="INFORMATIONAL: % of equipment with maintenance records. Low values are normal.")
    else:
        gauge_na(m2, "Equipment Maintenance Coverage", "No equipment data", key="maint_eq_cov_na",
                 help_text="% of equipment with maintenance records")
    
    st.markdown("---")
    
    # =========================================
    # SECTION 5: Data Breakdown Table
    # =========================================
    st.subheader("Detailed Breakdown")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Notification Data")
        st.markdown(f"""
        | Metric | Value |
        |--------|-------|
        | Total Notifications | {total_notifications:,} |
        | With Work Order | {maintenance.get('maint_notif_with_order', 0):,} |
        | Without Work Order | {maintenance.get('maint_notif_without_order', 0):,} |
        | With Asset | {maintenance.get('maint_notif_with_asset', 0):,} |
        | With Equipment | {maintenance.get('maint_notif_with_equipment', 0):,} |
        """)
    
    with col2:
        st.markdown("### Work Order Data")
        st.markdown(f"""
        | Metric | Value |
        |--------|-------|
        | Total Work Orders | {total_orders:,} |
        | With Notification | {maintenance.get('maint_orders_with_notification', 0):,} |
        | Without Notification | {maintenance.get('maint_orders_without_notification', 0):,} |
        | With Asset | {maintenance.get('maint_order_with_asset', 0):,} |
        | With Equipment | {maintenance.get('maint_order_with_equipment', 0):,} |
        | Completed | {maintenance.get('maint_orders_completed', 0):,} |
        """)
    
    st.markdown("---")
    
    col3, col4 = st.columns(2)
    
    with col3:
        st.markdown("### Failure Analysis")
        st.markdown(f"""
        | Metric | Value |
        |--------|-------|
        | Total Failure Notifications | {total_failure_notif:,} |
        | With Failure Mode | {maintenance.get('maint_failure_notif_with_mode', 0):,} |
        | With Failure Mechanism | {maintenance.get('maint_failure_notif_with_mechanism', 0):,} |
        | With Failure Cause | {maintenance.get('maint_failure_notif_with_cause', 0):,} |
        """)
    
    with col4:
        st.markdown("### Maintenance Coverage")
        st.markdown(f"""
        | Metric | Value |
        |--------|-------|
        | Assets with Notifications | {maintenance.get('maint_unique_assets_in_notifications', 0):,} |
        | Assets with Work Orders | {maintenance.get('maint_unique_assets_in_orders', 0):,} |
        | Equipment with Notifications | {maintenance.get('maint_unique_equipment_in_notifications', 0):,} |
        | Equipment with Work Orders | {maintenance.get('maint_unique_equipment_in_orders', 0):,} |
        | Assets with Any Maintenance | {maintenance.get('maint_assets_with_maintenance', 0):,} |
        | Equipment with Any Maintenance | {maintenance.get('maint_equipment_with_maintenance', 0):,} |
        """)
    
    # AI SUMMARY SECTION
    render_ai_summary_section(
        dashboard_type="Maintenance Workflow Quality",
        metrics_data=format_maintenance_metrics(maintenance),
        system_prompt=get_maintenance_prompt(),
        key_prefix="maintenance"
    )
    
    st.markdown("---")
    st.success("Maintenance Workflow dashboard loaded from pre-computed metrics.")
