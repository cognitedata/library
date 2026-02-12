# -*- coding: utf-8 -*-
"""
Sidebar metadata display component.
"""

import streamlit as st
from datetime import datetime

from .reports import generate_full_report


def render_metadata_sidebar(metrics: dict):
    """Display metadata in the sidebar."""
    metadata = metrics.get("metadata") or {}
    
    # =====================================================
    # DOWNLOAD REPORTS SECTION - AT THE TOP
    # =====================================================
    st.sidebar.title("Download Reports")
    st.sidebar.caption("Export dashboard data as PDF reports")
    
    # Full Report Button
    try:
        full_report = generate_full_report(metrics)
        st.sidebar.download_button(
            label="Full Report (All Dashboards)",
            data=full_report,
            file_name=f"context_quality_full_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf",
            mime="application/pdf",
            use_container_width=True
        )
    except Exception as e:
        st.sidebar.error(f"Error generating report: {e}")
    
    st.sidebar.caption("Individual reports available on each dashboard tab")
    
    st.sidebar.markdown("---")
    
    # =====================================================
    # METRICS INFO SECTION
    # =====================================================
    st.sidebar.title("Metrics Info")
    
    computed_at = metadata.get("computed_at", "Unknown")
    if computed_at != "Unknown":
        try:
            dt = datetime.fromisoformat(computed_at.replace("Z", "+00:00"))
            computed_at = dt.strftime("%Y-%m-%d %H:%M:%S UTC")
        except:
            pass
    
    st.sidebar.markdown(f"**Computed At:** {computed_at}")
    
    execution_time = metadata.get("execution_time_seconds", 0)
    st.sidebar.markdown(f"**Execution Time:** {execution_time:.1f}s")
    
    st.sidebar.markdown("---")
    
    # Instance Counts Section
    st.sidebar.markdown("### Instance Counts")
    instance_counts = metadata.get("instance_counts") or {}
    config = metadata.get("config") or {}
    limits = metadata.get("limits_reached") or {}
    
    # Helper function to generate duplicate IDs CSV content
    def generate_duplicate_csv(duplicate_ids: list, view_name: str) -> str:
        """Generate CSV content for duplicate IDs."""
        lines = [f"# Duplicate External IDs for {view_name}", "external_id"]
        lines.extend(duplicate_ids)
        return "\n".join(lines)
    
    # Helper function to display counts with optional duplicate download
    def display_counts(label: str, counts: dict, limit_key: str = None, view_key: str = None):
        counts = counts or {}
        total = counts.get("total_instances", counts.get("unique", 0))
        unique = counts.get("unique", total)
        dups = counts.get("duplicates", 0)
        duplicate_ids = counts.get("duplicate_ids", [])
        
        # Check if limit was reached
        limit_reached = (limits.get(limit_key) or False) if limit_key else False
        limit_indicator = " [LIMIT]" if limit_reached else ""
        
        st.sidebar.markdown(f"**{label}:** {unique:,}{limit_indicator}")
        if dups > 0:
            if duplicate_ids and len(duplicate_ids) > 0:
                # Create a download button for duplicate IDs
                csv_content = generate_duplicate_csv(duplicate_ids, label)
                col1, col2 = st.sidebar.columns([2, 1])
                with col1:
                    st.caption(f"   ({dups:,} duplicates)")
                with col2:
                    st.download_button(
                        label="CSV",
                        data=csv_content,
                        file_name=f"{view_key or label.lower()}_duplicates.csv",
                        mime="text/csv",
                        key=f"dl_dup_{view_key or label.lower()}",
                        help=f"Download {dups:,} duplicate external IDs"
                    )
            else:
                st.sidebar.caption(f"   ({dups:,} duplicates)")
    
    # Core views (always present)
    display_counts("Assets", instance_counts.get("assets") or {}, "assets", "assets")
    display_counts("Equipment", instance_counts.get("equipment") or {}, "equipment", "equipment")
    display_counts("Time Series", instance_counts.get("timeseries") or {}, "timeseries", "timeseries")
    
    # Maintenance views (if enabled)
    notif_counts = instance_counts.get("notifications") or {}
    if notif_counts.get("unique", 0) > 0:
        display_counts("Notifications", notif_counts, "notifications", "notifications")
    
    order_counts = instance_counts.get("maintenance_orders") or {}
    if order_counts.get("unique", 0) > 0:
        display_counts("Work Orders", order_counts, "maintenance_orders", "work_orders")
    
    failure_counts = instance_counts.get("failure_notifications") or {}
    if failure_counts.get("unique", 0) > 0:
        st.sidebar.markdown(f"**Failure Notifications:** {failure_counts.get('unique', 0):,}")
    
    # Annotations (if enabled)
    # Get annotation duplicate info from file_annotation_metrics
    annot_metrics = metrics.get("file_annotation_metrics") or {}
    annot_counts = instance_counts.get("annotations") or {}
    if annot_counts.get("unique", 0) > 0:
        # Enrich counts with duplicate_ids from metrics
        enriched_annot = dict(annot_counts)
        enriched_annot["duplicates"] = annot_metrics.get("annot_duplicates", 0)
        enriched_annot["duplicate_ids"] = annot_metrics.get("annot_duplicate_ids", [])
        display_counts("Annotations", enriched_annot, "annotations", "annotations")
    
    # 3D Objects (if enabled)
    # Get 3D duplicate info from model3d_metrics
    model3d_metrics = metrics.get("model3d_metrics") or {}
    obj3d_counts = instance_counts.get("3d_objects") or {}
    if obj3d_counts.get("unique", 0) > 0:
        assets_with_3d = obj3d_counts.get("assets_with_3d", 0)
        dups_3d = model3d_metrics.get("model3d_duplicates", 0)
        dup_ids_3d = model3d_metrics.get("model3d_duplicate_ids", [])
        
        st.sidebar.markdown(f"**3D Objects:** {obj3d_counts.get('unique', 0):,}")
        if assets_with_3d > 0:
            st.sidebar.caption(f"   ({assets_with_3d:,} assets with 3D)")
        if dups_3d > 0:
            if dup_ids_3d and len(dup_ids_3d) > 0:
                csv_content = generate_duplicate_csv(dup_ids_3d, "3D Objects")
                col1, col2 = st.sidebar.columns([2, 1])
                with col1:
                    st.caption(f"   ({dups_3d:,} duplicates)")
                with col2:
                    st.download_button(
                        label="CSV",
                        data=csv_content,
                        file_name="3d_objects_duplicates.csv",
                        mime="text/csv",
                        key="dl_dup_3d_objects",
                        help=f"Download {dups_3d:,} duplicate external IDs"
                    )
            else:
                st.sidebar.caption(f"   ({dups_3d:,} duplicates)")
    
    # Files (if enabled)
    file_counts = instance_counts.get("files") or {}
    if file_counts.get("unique", 0) > 0:
        display_counts("Files", file_counts, "files", "files")
    
    st.sidebar.markdown("---")
    
    # =====================================================
    # PROCESSING COVERAGE
    # =====================================================
    st.sidebar.markdown("### Processing Status")
    
    # Check if any limits were reached
    any_limit_reached = any(limits.values()) if limits else False
    
    if any_limit_reached:
        st.sidebar.warning("""
        **Partial Data**
        
        Processing limits reached. Metrics represent trends only.
        
        Increase limits in Configure & Run tab for complete analysis.
        """)
    else:
        st.sidebar.success("All data processed successfully.")
    
    st.sidebar.markdown("---")
    
    # Refresh button
    if st.sidebar.button("Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
