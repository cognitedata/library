# -*- coding: utf-8 -*-
"""
Sidebar metadata display component.
"""

import streamlit as st
from datetime import datetime

from .reports import (
    generate_full_report,
    generate_asset_hierarchy_report,
    generate_equipment_report,
    generate_timeseries_report,
    generate_maintenance_report,
    generate_file_annotation_report,
    generate_3d_model_report,
    generate_files_report,
)


def _instance_counts_from_metrics(metrics: dict) -> dict:
    """Build instance counts from metric sections (totals). Used when metadata.instance_counts is partial/zeros (e.g. after single-metric rerun)."""
    hierarchy = metrics.get("hierarchy_metrics", {})
    equipment = metrics.get("equipment_metrics", {})
    ts = metrics.get("timeseries_metrics", {})
    maint = metrics.get("maintenance_metrics", {})
    annot = metrics.get("file_annotation_metrics", {})
    model3d = metrics.get("model3d_metrics", {})
    file_metrics = metrics.get("file_metrics", {})
    return {
        "assets": {"unique": hierarchy.get("hierarchy_total_assets", 0), "total_instances": hierarchy.get("hierarchy_total_assets", 0)},
        "equipment": {"unique": equipment.get("eq_total", 0), "total_instances": equipment.get("eq_total", 0)},
        "timeseries": {"unique": ts.get("ts_total", 0), "total_instances": ts.get("ts_total", 0)},
        "notifications": {"unique": maint.get("maint_total_notifications", 0), "total_instances": maint.get("maint_total_notifications", 0)},
        "maintenance_orders": {"unique": maint.get("maint_total_orders", 0), "total_instances": maint.get("maint_total_orders", 0)},
        "failure_notifications": {"unique": maint.get("maint_total_failure_notifications", 0)},
        "annotations": {"unique": annot.get("annot_total_instances", annot.get("annot_total", 0)), "total_instances": annot.get("annot_total_instances", annot.get("annot_total", 0))},
        "3d_objects": {
            "unique": model3d.get("model3d_total_objects", 0),
            "assets_with_3d": model3d.get("model3d_assets_with_3d", 0),
        },
        "files": {"unique": file_metrics.get("file_total", 0), "total_instances": file_metrics.get("file_total", 0)},
    }


def _merge_instance_counts(metadata_counts: dict, metrics_counts: dict) -> dict:
    """Use metadata instance_counts when non-zero; otherwise use totals from metrics."""
    merged = {}
    for key, fallback in metrics_counts.items():
        meta = metadata_counts.get(key, {}) or {}
        unique_meta = meta.get("unique", 0) or meta.get("total_instances", 0)
        unique_fallback = fallback.get("unique", 0) or fallback.get("total_instances", 0)
        if unique_meta > 0:
            merged[key] = dict(meta)
        elif unique_fallback > 0:
            merged[key] = dict(fallback)
        else:
            merged[key] = dict(meta) if meta else dict(fallback)
    return merged


def render_metadata_sidebar(metrics: dict):
    """Display metadata in the sidebar."""
    metadata = metrics.get("metadata", {})
    
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
    
    # Individual dashboard reports in an expander
    with st.sidebar.expander("Individual Reports", expanded=False):
        # Asset Hierarchy
        try:
            asset_report = generate_asset_hierarchy_report(metrics)
            st.download_button(
                label="Asset Hierarchy",
                data=asset_report,
                file_name=f"asset_hierarchy_report_{datetime.now().strftime('%Y%m%d')}.pdf",
                mime="application/pdf",
                key="dl_asset",
                use_container_width=True
            )
        except:
            pass
        
        # Equipment
        try:
            eq_report = generate_equipment_report(metrics)
            st.download_button(
                label="Equipment-Asset",
                data=eq_report,
                file_name=f"equipment_report_{datetime.now().strftime('%Y%m%d')}.pdf",
                mime="application/pdf",
                key="dl_equipment",
                use_container_width=True
            )
        except:
            pass
        
        # Time Series
        try:
            ts_report = generate_timeseries_report(metrics)
            st.download_button(
                label="Time Series",
                data=ts_report,
                file_name=f"timeseries_report_{datetime.now().strftime('%Y%m%d')}.pdf",
                mime="application/pdf",
                key="dl_timeseries",
                use_container_width=True
            )
        except:
            pass
        
        # Maintenance (if available)
        if metrics.get("maintenance_metrics"):
            try:
                maint_report = generate_maintenance_report(metrics)
                st.download_button(
                    label="Maintenance",
                    data=maint_report,
                    file_name=f"maintenance_report_{datetime.now().strftime('%Y%m%d')}.pdf",
                    mime="application/pdf",
                    key="dl_maintenance",
                    use_container_width=True
                )
            except:
                pass
        
        # File Annotation (if available)
        if metrics.get("file_annotation_metrics"):
            try:
                annot_report = generate_file_annotation_report(metrics)
                st.download_button(
                    label="File Annotation",
                    data=annot_report,
                    file_name=f"file_annotation_report_{datetime.now().strftime('%Y%m%d')}.pdf",
                    mime="application/pdf",
                    key="dl_annotation",
                    use_container_width=True
                )
            except:
                pass
        
        # 3D Model (if available)
        if metrics.get("model3d_metrics"):
            try:
                model3d_report = generate_3d_model_report(metrics)
                st.download_button(
                    label="3D Model",
                    data=model3d_report,
                    file_name=f"3d_model_report_{datetime.now().strftime('%Y%m%d')}.pdf",
                    mime="application/pdf",
                    key="dl_3d",
                    use_container_width=True
                )
            except:
                pass
        
        # Files (if available)
        if metrics.get("file_metrics"):
            try:
                files_report = generate_files_report(metrics)
                st.download_button(
                    label="Files",
                    data=files_report,
                    file_name=f"files_report_{datetime.now().strftime('%Y%m%d')}.pdf",
                    mime="application/pdf",
                    key="dl_files",
                    use_container_width=True
                )
            except:
                pass
    
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
    
    # Instance Counts Section (use totals from metrics when metadata counts are zero, e.g. after single-metric rerun)
    st.sidebar.markdown("### Instance Counts")
    instance_counts_raw = metadata.get("instance_counts", {})
    instance_counts_from_metrics = _instance_counts_from_metrics(metrics)
    instance_counts = _merge_instance_counts(instance_counts_raw, instance_counts_from_metrics)
    config = metadata.get("config", {})
    limits = metadata.get("limits_reached", {})
    
    # Helper function to generate duplicate IDs CSV content
    def generate_duplicate_csv(duplicate_ids: list, view_name: str) -> str:
        """Generate CSV content for duplicate IDs."""
        lines = [f"# Duplicate External IDs for {view_name}", "external_id"]
        lines.extend(duplicate_ids)
        return "\n".join(lines)
    
    # Helper function to display counts with optional duplicate download
    def display_counts(label: str, counts: dict, limit_key: str = None, view_key: str = None):
        total = counts.get("total_instances", counts.get("unique", 0))
        unique = counts.get("unique", total)
        dups = counts.get("duplicates", 0)
        duplicate_ids = counts.get("duplicate_ids", [])
        
        # Check if limit was reached
        limit_reached = limits.get(limit_key, False) if limit_key else False
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
    display_counts("Assets", instance_counts.get("assets", {}), "assets", "assets")
    display_counts("Equipment", instance_counts.get("equipment", {}), "equipment", "equipment")
    display_counts("Time Series", instance_counts.get("timeseries", {}), "timeseries", "timeseries")
    
    # Maintenance views (if enabled)
    notif_counts = instance_counts.get("notifications", {})
    if notif_counts.get("unique", 0) > 0:
        display_counts("Notifications", notif_counts, "notifications", "notifications")
    
    order_counts = instance_counts.get("maintenance_orders", {})
    if order_counts.get("unique", 0) > 0:
        display_counts("Work Orders", order_counts, "maintenance_orders", "work_orders")
    
    failure_counts = instance_counts.get("failure_notifications", {})
    if failure_counts.get("unique", 0) > 0:
        st.sidebar.markdown(f"**Failure Notifications:** {failure_counts.get('unique', 0):,}")
    
    # Annotations (if enabled)
    # Get annotation duplicate info from file_annotation_metrics
    annot_metrics = metrics.get("file_annotation_metrics", {})
    annot_counts = instance_counts.get("annotations", {})
    if annot_counts.get("unique", 0) > 0:
        # Enrich counts with duplicate_ids from metrics
        enriched_annot = dict(annot_counts)
        enriched_annot["duplicates"] = annot_metrics.get("annot_duplicates", 0)
        enriched_annot["duplicate_ids"] = annot_metrics.get("annot_duplicate_ids", [])
        display_counts("Annotations", enriched_annot, "annotations", "annotations")
    
    # 3D Objects (if enabled)
    # Get 3D duplicate info from model_3d_metrics
    model3d_metrics = metrics.get("model_3d_metrics", {})
    obj3d_counts = instance_counts.get("3d_objects", {})
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
    file_counts = instance_counts.get("files", {})
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
