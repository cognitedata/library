# -*- coding: utf-8 -*-
"""
Report Generator for Context Quality Dashboards.

Generates downloadable PDF reports for each dashboard tab.
Uses fpdf2 for lightweight, pure-Python PDF generation.
"""

from fpdf import FPDF
from datetime import datetime
from typing import Optional
import io


class ContextQualityReport(FPDF):
    """Custom PDF class with header/footer branding."""
    
    def __init__(self, title: str):
        super().__init__()
        self.report_title = title
        self.set_auto_page_break(auto=True, margin=15)
    
    def header(self):
        self.set_font("Helvetica", "B", 12)
        self.cell(0, 10, self.report_title, border=0, ln=True, align="C")
        self.set_font("Helvetica", "", 8)
        self.set_text_color(128, 128, 128)
        self.cell(0, 5, f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", ln=True, align="C")
        self.set_text_color(0, 0, 0)
        self.ln(5)
    
    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(128, 128, 128)
        self.cell(0, 10, f"Page {self.page_no()}/{{nb}} | Context Quality Dashboard Report", align="C")
    
    def add_section_header(self, title: str):
        """Add a section header with styling."""
        self.set_font("Helvetica", "B", 14)
        self.set_fill_color(240, 240, 240)
        self.cell(0, 10, title, ln=True, fill=True)
        self.ln(3)
    
    def add_metric_row(self, label: str, value: str, status: Optional[str] = None):
        """Add a metric row with optional status indicator."""
        self.set_font("Helvetica", "", 10)
        
        # Status color
        if status == "good":
            self.set_text_color(76, 175, 80)  # Green
        elif status == "warning":
            self.set_text_color(255, 193, 7)  # Yellow
        elif status == "error":
            self.set_text_color(244, 67, 54)  # Red
        else:
            self.set_text_color(0, 0, 0)
        
        self.cell(100, 8, label, border=0)
        self.set_font("Helvetica", "B", 10)
        self.cell(0, 8, value, ln=True, align="R")
        self.set_text_color(0, 0, 0)
        self.set_font("Helvetica", "", 10)
    
    def add_key_metric(self, label: str, value: float, suffix: str = "%", threshold_good: float = 90, threshold_warn: float = 70):
        """Add a key metric with automatic status coloring."""
        status = "good" if value >= threshold_good else "warning" if value >= threshold_warn else "error"
        self.add_metric_row(label, f"{value:.1f}{suffix}", status)
    
    def add_table(self, headers: list, rows: list):
        """Add a simple table."""
        self.set_font("Helvetica", "B", 9)
        self.set_fill_color(220, 220, 220)
        
        col_width = 190 / len(headers)
        for header in headers:
            self.cell(col_width, 8, header, border=1, fill=True, align="C")
        self.ln()
        
        self.set_font("Helvetica", "", 9)
        for row in rows:
            for cell in row:
                self.cell(col_width, 7, str(cell), border=1, align="C")
            self.ln()
        self.ln(3)
    
    def add_paragraph(self, text: str):
        """Add a paragraph of text."""
        self.set_font("Helvetica", "", 10)
        self.multi_cell(0, 6, text)
        self.ln(3)


def get_status(value: float, good: float = 90, warn: float = 70) -> str:
    """Get status string based on value thresholds."""
    if value >= good:
        return "good"
    elif value >= warn:
        return "warning"
    return "error"


def generate_asset_hierarchy_report(metrics: dict) -> bytes:
    """Generate PDF report for Asset Hierarchy dashboard."""
    hierarchy = metrics.get("hierarchy_metrics", {})
    metadata = metrics.get("metadata", {})
    
    pdf = ContextQualityReport("Asset Hierarchy Quality Report")
    pdf.alias_nb_pages()
    pdf.add_page()
    
    # Executive Summary
    pdf.add_section_header("Executive Summary")
    completion_rate = hierarchy.get("hierarchy_completion_rate", 0)
    orphan_rate = hierarchy.get("hierarchy_orphan_rate", 0)
    
    pdf.add_key_metric("Hierarchy Completion Rate", completion_rate)
    pdf.add_key_metric("Orphan Rate", orphan_rate, threshold_good=5, threshold_warn=15)
    pdf.ln(5)
    
    # Key Metrics
    pdf.add_section_header("Key Metrics")
    pdf.add_metric_row("Total Assets", f"{hierarchy.get('hierarchy_total_assets', 0):,}")
    pdf.add_metric_row("Root Assets", f"{hierarchy.get('hierarchy_root_assets', 0):,}")
    pdf.add_metric_row("Orphan Assets", f"{hierarchy.get('hierarchy_orphan_count', 0):,}")
    pdf.add_metric_row("Average Depth", f"{hierarchy.get('hierarchy_avg_depth', 0):.2f}")
    pdf.add_metric_row("Max Depth", f"{hierarchy.get('hierarchy_max_depth', 0)}")
    pdf.add_metric_row("Average Children per Parent", f"{hierarchy.get('hierarchy_avg_children', 0):.2f}")
    pdf.add_metric_row("Max Children", f"{hierarchy.get('hierarchy_max_children', 0):,}")
    pdf.ln(5)
    
    # Insights
    pdf.add_section_header("Insights")
    insights = []
    if completion_rate >= 95:
        insights.append("Excellent hierarchy structure - almost all assets have proper parent links.")
    elif completion_rate >= 80:
        insights.append("Good hierarchy structure, but some assets may have missing parent links.")
    else:
        insights.append("Hierarchy needs attention - many assets are missing parent links.")
    
    if orphan_rate > 10:
        insights.append(f"High orphan rate ({orphan_rate:.1f}%) - review disconnected assets.")
    
    for insight in insights:
        pdf.add_paragraph(f"- {insight}")
    
    # Metadata
    pdf.add_section_header("Report Metadata")
    pdf.add_metric_row("Computed At", metadata.get("computed_at", "Unknown"))
    pdf.add_metric_row("Execution Time", f"{metadata.get('execution_time_seconds', 0):.1f} seconds")
    
    return bytes(pdf.output())


def generate_equipment_report(metrics: dict) -> bytes:
    """Generate PDF report for Equipment-Asset dashboard."""
    equipment = metrics.get("equipment_metrics", {})
    metadata = metrics.get("metadata", {})
    
    pdf = ContextQualityReport("Equipment-Asset Quality Report")
    pdf.alias_nb_pages()
    pdf.add_page()
    
    # Executive Summary
    pdf.add_section_header("Executive Summary")
    association_rate = equipment.get("eq_association_rate", 0)
    
    pdf.add_key_metric("Equipment Association Rate", association_rate)
    pdf.add_key_metric("Serial Number Completeness", equipment.get("eq_serial_completeness", 0))
    pdf.add_key_metric("Type Consistency Rate", equipment.get("eq_type_consistency_rate", 0))
    pdf.ln(5)
    
    # Key Metrics
    pdf.add_section_header("Key Metrics")
    pdf.add_metric_row("Total Equipment", f"{equipment.get('eq_total', 0):,}")
    pdf.add_metric_row("Linked Equipment", f"{equipment.get('eq_linked', 0):,}")
    pdf.add_metric_row("Unlinked Equipment", f"{equipment.get('eq_unlinked', 0):,}")
    pdf.add_metric_row("Assets with Equipment", f"{equipment.get('eq_assets_with_equipment', 0):,}")
    pdf.add_metric_row("Critical Equipment Total", f"{equipment.get('eq_critical_total', 0):,}")
    pdf.add_metric_row("Critical Equipment Linked", f"{equipment.get('eq_critical_linked', 0):,}")
    pdf.add_metric_row("Avg Equipment per Asset", f"{equipment.get('eq_avg_per_asset', 0):.2f}")
    pdf.ln(5)
    
    # Insights
    pdf.add_section_header("Insights")
    unlinked = equipment.get('eq_unlinked', 0)
    if unlinked == 0:
        pdf.add_paragraph("- Excellent! All equipment is properly linked to assets.")
    else:
        pdf.add_paragraph(f"- {unlinked:,} equipment items need asset links.")
    
    critical_rate = equipment.get("eq_critical_contextualization")
    if critical_rate is not None and critical_rate < 100:
        pdf.add_paragraph(f"- Critical equipment contextualization is {critical_rate:.1f}% - should be 100%.")
    
    # Metadata
    pdf.add_section_header("Report Metadata")
    pdf.add_metric_row("Computed At", metadata.get("computed_at", "Unknown"))
    
    return bytes(pdf.output())


def generate_timeseries_report(metrics: dict) -> bytes:
    """Generate PDF report for Time Series dashboard."""
    ts = metrics.get("timeseries_metrics", {})
    hierarchy = metrics.get("hierarchy_metrics", {})
    metadata = metrics.get("metadata", {})
    
    pdf = ContextQualityReport("Time Series Contextualization Report")
    pdf.alias_nb_pages()
    pdf.add_page()
    
    # Executive Summary
    pdf.add_section_header("Executive Summary")
    ts_to_asset = ts.get("ts_to_asset_rate", 0)
    
    pdf.add_key_metric("TS to Asset Contextualization", ts_to_asset)
    pdf.add_key_metric("TS to Equipment Contextualization", ts.get("ts_to_equipment_rate", 0), threshold_good=70, threshold_warn=40)
    pdf.add_key_metric("Asset Monitoring Coverage", ts.get("ts_asset_monitoring_coverage", 0), threshold_good=70, threshold_warn=40)
    pdf.add_key_metric("Data Freshness (30 days)", ts.get("ts_data_freshness", 0))
    pdf.add_key_metric("Source Unit Completeness", ts.get("ts_source_unit_completeness", 0))
    pdf.ln(5)
    
    # Key Metrics
    pdf.add_section_header("Key Metrics")
    pdf.add_metric_row("Total Time Series", f"{ts.get('ts_total', 0):,}")
    pdf.add_metric_row("TS with Asset Link", f"{ts.get('ts_with_asset_link', 0):,}")
    pdf.add_metric_row("Orphaned TS", f"{ts.get('ts_without_asset_link', 0):,}")
    pdf.add_metric_row("TS with Equipment Link", f"{ts.get('ts_with_equipment_link', 0):,}")
    pdf.add_metric_row("Equipment with TS", f"{ts.get('ts_associated_equipment', 0):,}")
    pdf.add_metric_row("Assets with TS", f"{ts.get('ts_associated_assets', 0):,}")
    pdf.add_metric_row("Total Assets", f"{hierarchy.get('hierarchy_total_assets', 0):,}")
    pdf.add_metric_row("Fresh TS Count", f"{ts.get('ts_fresh_count', 0):,}")
    pdf.add_metric_row("Unique Source Units", f"{ts.get('ts_unique_source_units', 0):,}")
    pdf.ln(5)
    
    # Historical Data Analysis
    if ts.get("ts_analyzed_for_gaps", 0) > 0:
        pdf.add_section_header("Historical Data Analysis")
        pdf.add_metric_row("TS Analyzed for Gaps", f"{ts.get('ts_analyzed_for_gaps', 0):,}")
        pdf.add_metric_row("Total Time Span", f"{ts.get('ts_total_time_span_days', 0):,.0f} days")
        pdf.add_metric_row("Gaps Found", f"{ts.get('ts_gap_count', 0):,}")
        pdf.add_metric_row("Total Gap Duration", f"{ts.get('ts_total_gap_duration_days', 0):,.0f} days")
        pdf.ln(5)
    
    # Insights
    pdf.add_section_header("Insights")
    orphaned = ts.get('ts_without_asset_link', 0)
    if orphaned == 0:
        pdf.add_paragraph("- Excellent! All time series are linked to assets.")
    else:
        pdf.add_paragraph(f"- {orphaned:,} time series need asset links for proper contextualization.")
    
    # Metadata
    pdf.add_section_header("Report Metadata")
    pdf.add_metric_row("Computed At", metadata.get("computed_at", "Unknown"))
    
    return bytes(pdf.output())


def generate_maintenance_report(metrics: dict) -> bytes:
    """Generate PDF report for Maintenance Workflow dashboard."""
    maint = metrics.get("maintenance_metrics", {})
    metadata = metrics.get("metadata", {})
    
    pdf = ContextQualityReport("Maintenance Workflow Quality Report")
    pdf.alias_nb_pages()
    pdf.add_page()
    
    # Executive Summary
    pdf.add_section_header("Executive Summary")
    order_asset = maint.get("maint_order_to_asset_rate", 0) or 0
    order_notif = maint.get("maint_order_to_notif_rate", 0) or 0
    
    pdf.add_key_metric("Work Order to Asset Rate", order_asset)
    pdf.add_key_metric("Work Order to Notification Rate", order_notif)
    pdf.add_key_metric("Order Completion Rate", maint.get("maint_order_completion_rate", 0) or 0)
    pdf.ln(5)
    
    # Volume Metrics
    pdf.add_section_header("Volume Metrics")
    pdf.add_metric_row("Total Notifications", f"{maint.get('maint_total_notifications', 0):,}")
    pdf.add_metric_row("Total Work Orders", f"{maint.get('maint_total_orders', 0):,}")
    pdf.add_metric_row("Failure Notifications", f"{maint.get('maint_total_failure_notifications', 0):,}")
    pdf.add_metric_row("Completed Orders", f"{maint.get('maint_orders_completed', 0):,}")
    pdf.ln(5)
    
    # Linkage Metrics
    pdf.add_section_header("Notification Linkage")
    pdf.add_key_metric("Notification to Work Order", maint.get("maint_notif_to_order_rate", 0) or 0, threshold_good=50, threshold_warn=20)
    pdf.add_key_metric("Notification to Asset", maint.get("maint_notif_to_asset_rate", 0) or 0)
    pdf.add_key_metric("Notification to Equipment", maint.get("maint_notif_to_equipment_rate", 0) or 0)
    pdf.ln(5)
    
    # Failure Analysis
    pdf.add_section_header("Failure Analysis Documentation")
    pdf.add_key_metric("Failure Mode Rate", maint.get("maint_failure_mode_rate", 0) or 0)
    pdf.add_key_metric("Failure Mechanism Rate", maint.get("maint_failure_mechanism_rate", 0) or 0)
    pdf.add_key_metric("Failure Cause Rate", maint.get("maint_failure_cause_rate", 0) or 0)
    
    # Metadata
    pdf.add_section_header("Report Metadata")
    pdf.add_metric_row("Computed At", metadata.get("computed_at", "Unknown"))
    
    return bytes(pdf.output())


def generate_file_annotation_report(metrics: dict) -> bytes:
    """Generate PDF report for P&ID Annotation dashboard."""
    annot = metrics.get("file_annotation_metrics", {})
    metadata = metrics.get("metadata", {})
    
    pdf = ContextQualityReport("P&ID Annotation Quality Report")
    pdf.alias_nb_pages()
    pdf.add_page()
    
    # Executive Summary
    pdf.add_section_header("Executive Summary")
    avg_conf = annot.get("annot_avg_confidence")
    if avg_conf:
        pdf.add_key_metric("Average Confidence", avg_conf)
    pdf.add_key_metric("Approved Rate", annot.get("annot_approved_pct", 0))
    pdf.add_key_metric("High Confidence Rate", annot.get("annot_confidence_high_pct", 0), threshold_good=70, threshold_warn=50)
    pdf.ln(5)
    
    # Volume Metrics
    pdf.add_section_header("Volume Metrics")
    pdf.add_metric_row("Total Annotations", f"{annot.get('annot_total', 0):,}")
    pdf.add_metric_row("Files with Annotations", f"{annot.get('annot_unique_files_with_annotations', 0):,}")
    pdf.add_metric_row("Assets Linked", f"{annot.get('annot_unique_assets_linked', 0):,}")
    pdf.add_metric_row("Pages Annotated", f"{annot.get('annot_unique_pages', 0):,}")
    pdf.ln(5)
    
    # Confidence Distribution
    pdf.add_section_header("Confidence Distribution")
    pdf.add_metric_row("High Confidence (>=90%)", f"{annot.get('annot_confidence_high', 0):,}")
    pdf.add_metric_row("Medium Confidence (50-90%)", f"{annot.get('annot_confidence_medium', 0):,}")
    pdf.add_metric_row("Low Confidence (<50%)", f"{annot.get('annot_confidence_low', 0):,}")
    pdf.ln(5)
    
    # Status Distribution
    pdf.add_section_header("Status Distribution")
    pdf.add_metric_row("Approved", f"{annot.get('annot_approved', 0):,}")
    pdf.add_metric_row("Suggested (Pending)", f"{annot.get('annot_suggested', 0):,}")
    pdf.add_metric_row("Rejected", f"{annot.get('annot_rejected', 0):,}")
    
    # Metadata
    pdf.add_section_header("Report Metadata")
    pdf.add_metric_row("Computed At", metadata.get("computed_at", "Unknown"))
    
    return bytes(pdf.output())


def generate_3d_model_report(metrics: dict) -> bytes:
    """Generate PDF report for 3D Model dashboard."""
    model3d = metrics.get("model3d_metrics", {})
    metadata = metrics.get("metadata", {})
    
    pdf = ContextQualityReport("3D Model Contextualization Report")
    pdf.alias_nb_pages()
    pdf.add_page()
    
    # Executive Summary
    pdf.add_section_header("Executive Summary")
    ctx_rate = model3d.get("model3d_contextualization_rate", 0)
    
    pdf.add_key_metric("3D to Asset Contextualization", ctx_rate)
    pdf.add_key_metric("Asset 3D Coverage", model3d.get("model3d_asset_coverage", 0), threshold_good=70, threshold_warn=40)
    pdf.add_key_metric("Critical Asset 3D Rate", model3d.get("model3d_critical_asset_rate", 0), threshold_good=100, threshold_warn=80)
    pdf.add_key_metric("BBox Completeness", model3d.get("model3d_bbox_completeness", 0))
    pdf.ln(5)
    
    # Volume Metrics
    pdf.add_section_header("Volume Metrics")
    pdf.add_metric_row("Total 3D Objects", f"{model3d.get('model3d_total_objects', 0):,}")
    pdf.add_metric_row("Objects with Asset Link", f"{model3d.get('model3d_objects_with_asset', 0):,}")
    pdf.add_metric_row("Total Assets", f"{model3d.get('model3d_total_assets', 0):,}")
    pdf.add_metric_row("Assets with 3D", f"{model3d.get('model3d_assets_with_3d', 0):,}")
    pdf.add_metric_row("Critical Assets Total", f"{model3d.get('model3d_critical_total', 0):,}")
    pdf.add_metric_row("Critical Assets with 3D", f"{model3d.get('model3d_critical_with_3d', 0):,}")
    pdf.ln(5)
    
    # Bounding Box Distribution
    pdf.add_section_header("Bounding Box Distribution")
    pdf.add_metric_row("Complete BBox", f"{model3d.get('model3d_bbox_complete_count', 0):,}")
    pdf.add_metric_row("Partial BBox", f"{model3d.get('model3d_bbox_partial_count', 0):,}")
    pdf.add_metric_row("Missing BBox", f"{model3d.get('model3d_bbox_none_count', 0):,}")
    pdf.ln(5)
    
    # Model Type Distribution
    pdf.add_section_header("Model Type Distribution")
    pdf.add_metric_row("CAD Models", f"{model3d.get('model3d_cad_count', 0):,}")
    pdf.add_metric_row("360 Images", f"{model3d.get('model3d_360_count', 0):,}")
    pdf.add_metric_row("Point Clouds", f"{model3d.get('model3d_pointcloud_count', 0):,}")
    pdf.add_metric_row("Multi-Model", f"{model3d.get('model3d_multi_model_count', 0):,}")
    
    # Metadata
    pdf.add_section_header("Report Metadata")
    pdf.add_metric_row("Computed At", metadata.get("computed_at", "Unknown") if metadata else "Unknown")
    
    return bytes(pdf.output())


def generate_files_report(metrics: dict) -> bytes:
    """Generate PDF report for Files dashboard."""
    files = metrics.get("file_metrics", {})
    hierarchy = metrics.get("hierarchy_metrics", {})
    metadata = metrics.get("metadata", {})
    
    pdf = ContextQualityReport("Files Contextualization Report")
    pdf.alias_nb_pages()
    pdf.add_page()
    
    # Executive Summary
    pdf.add_section_header("Executive Summary")
    file_to_asset = files.get("file_to_asset_rate", 0)
    
    pdf.add_key_metric("File to Asset Rate", file_to_asset)
    pdf.add_key_metric("Asset File Coverage", files.get("file_asset_coverage", 0), threshold_good=50, threshold_warn=20)
    pdf.add_key_metric("Category Rate", files.get("file_category_rate", 0))
    pdf.add_key_metric("Content Uploaded", files.get("file_upload_rate", 0))
    pdf.ln(5)
    
    # Volume Metrics
    pdf.add_section_header("Volume Metrics")
    pdf.add_metric_row("Total Files", f"{files.get('file_total', 0):,}")
    pdf.add_metric_row("Files with Assets", f"{files.get('files_with_assets', 0):,}")
    pdf.add_metric_row("Orphaned Files", f"{files.get('files_without_assets', 0):,}")
    pdf.add_metric_row("Assets with Files", f"{files.get('assets_with_files', 0):,}")
    pdf.add_metric_row("Total Assets", f"{hierarchy.get('hierarchy_total_assets', 0):,}")
    pdf.ln(5)
    
    # Metadata Quality
    pdf.add_section_header("Metadata Quality")
    pdf.add_key_metric("Name Completeness", files.get("file_name_rate", 0))
    pdf.add_key_metric("Description Rate", files.get("file_description_rate", 0), threshold_good=50, threshold_warn=20)
    pdf.add_key_metric("Source ID Rate", files.get("file_source_id_rate", 0))
    pdf.ln(5)
    
    # Distribution
    pdf.add_section_header("Distribution Statistics")
    pdf.add_metric_row("Avg Files per Asset", f"{files.get('avg_files_per_asset', 0):.2f}")
    pdf.add_metric_row("Max Files per Asset", f"{files.get('max_files_per_asset', 0):,}")
    pdf.add_metric_row("Unique MIME Types", f"{files.get('unique_mime_types', 0):,}")
    pdf.add_metric_row("Unique Categories", f"{files.get('unique_categories', 0):,}")
    
    # Insights
    pdf.add_section_header("Insights")
    orphaned = files.get('files_without_assets', 0)
    if orphaned == 0:
        pdf.add_paragraph("- Excellent! All files are linked to assets.")
    else:
        pdf.add_paragraph(f"- {orphaned:,} files are orphaned (not linked to any asset).")
    
    # Metadata
    pdf.add_section_header("Report Metadata")
    pdf.add_metric_row("Computed At", metadata.get("computed_at", "Unknown"))
    
    return bytes(pdf.output())


def generate_others_report(metrics: dict) -> bytes:
    """Generate PDF report for Others IDI Views dashboard."""
    others = metrics.get("others_metrics", {})
    metadata = metrics.get("metadata", {})
    
    pdf = ContextQualityReport("Others IDI Views Quality Report")
    pdf.alias_nb_pages()
    pdf.add_page()
    
    # Executive Summary
    pdf.add_section_header("Executive Summary")
    overall_rate = others.get("others_overall_asset_rate")
    
    if overall_rate is not None:
        pdf.add_key_metric("Overall Asset Link Rate", overall_rate)
    else:
        pdf.add_metric_row("Overall Asset Link Rate", "N/A")
    
    pdf.add_metric_row("Total Views", f"{others.get('others_total_views', 0)}")
    pdf.add_metric_row("Views with Data", f"{others.get('others_views_with_data', 0)}")
    pdf.add_metric_row("Total Records", f"{others.get('others_total_instances', 0):,}")
    pdf.ln(5)
    
    # Per-View Metrics Table
    pdf.add_section_header("Per-View Metrics")
    
    views = others.get("others_views", [])
    if views:
        # Filter to views with data
        ok_views = [v for v in views if v.get("status") == "ok"]
        
        if ok_views:
            headers = ["View", "Total", "With Asset", "Rate %"]
            rows = []
            for view in ok_views:
                rate = view.get("asset_link_rate")
                rate_str = f"{rate:.1f}" if rate is not None else "N/A"
                rows.append([
                    view.get("display_name", view.get("view_id", "?"))[:20],
                    str(view.get("unique_instances", 0)),
                    str(view.get("instances_with_asset", 0)),
                    rate_str
                ])
            pdf.add_table(headers, rows)
        else:
            pdf.add_paragraph("No views with data found.")
    else:
        pdf.add_paragraph("No view data available.")
    
    pdf.ln(5)
    
    # View Status Summary
    pdf.add_section_header("View Status Summary")
    
    if views:
        ok_count = len([v for v in views if v.get("status") == "ok"])
        empty_count = len([v for v in views if v.get("status") == "empty"])
        error_count = len([v for v in views if v.get("status") in ["not_found", "error"]])
        
        pdf.add_metric_row("Views with Data", f"{ok_count}", "good" if ok_count > 0 else None)
        pdf.add_metric_row("Empty Views", f"{empty_count}", "warning" if empty_count > 0 else None)
        pdf.add_metric_row("Views with Errors", f"{error_count}", "error" if error_count > 0 else None)
        pdf.ln(5)
        
        # List empty views if any
        empty_views = [v for v in views if v.get("status") == "empty"]
        if empty_views:
            pdf.add_section_header("Empty Views")
            for view in empty_views:
                pdf.add_paragraph(f"- {view.get('display_name', view.get('view_id'))}")
        
        # List error views if any
        error_views = [v for v in views if v.get("status") in ["not_found", "error"]]
        if error_views:
            pdf.add_section_header("Views with Errors")
            for view in error_views:
                error_msg = view.get("error", "Unknown error")
                pdf.add_paragraph(f"- {view.get('display_name', view.get('view_id'))}: {error_msg}")
    
    # Insights
    pdf.add_section_header("Insights")
    insights = []
    
    if overall_rate is not None:
        if overall_rate >= 90:
            insights.append("Excellent asset linkage across IDI views - data is well contextualized.")
        elif overall_rate >= 70:
            insights.append("Good asset linkage, but some views may need attention.")
        else:
            insights.append("Asset linkage needs improvement - review views with low rates.")
    
    views_with_data = others.get("others_views_with_data", 0)
    total_views = others.get("others_total_views", 0)
    if total_views > 0 and views_with_data < total_views:
        insights.append(f"{total_views - views_with_data} views are empty or have errors.")
    
    if not insights:
        insights.append("No specific insights available.")
    
    for insight in insights:
        pdf.add_paragraph(f"- {insight}")
    
    # Metadata
    pdf.add_section_header("Report Metadata")
    pdf.add_metric_row("Computed At", metadata.get("computed_at", "Unknown"))
    
    return bytes(pdf.output())


def generate_maintenance_idi_report(metrics: dict) -> bytes:
    """Generate PDF report for Maintenance IDI Views dashboard."""
    maint = metrics.get("maintenance_idi_metrics", {})
    metadata = metrics.get("metadata", {})
    
    pdf = ContextQualityReport("Maintenance IDI Views Quality Report")
    pdf.alias_nb_pages()
    pdf.add_page()
    
    # Executive Summary
    pdf.add_section_header("Executive Summary")
    asset_coverage = maint.get("maint_idi_asset_coverage_rate")
    
    if asset_coverage is not None:
        pdf.add_key_metric("Asset Maintenance Coverage", asset_coverage, threshold_good=50, threshold_warn=20)
    else:
        pdf.add_metric_row("Asset Maintenance Coverage", "N/A")
    
    pdf.add_metric_row("Total Views", f"{maint.get('maint_idi_total_views', 0)}")
    pdf.add_metric_row("Views with Data", f"{maint.get('maint_idi_views_with_data', 0)}")
    pdf.add_metric_row("Total Instances", f"{maint.get('maint_idi_total_instances', 0):,}")
    pdf.add_metric_row("Assets with Maintenance", f"{maint.get('maint_idi_assets_with_maintenance', 0):,}")
    pdf.ln(5)
    
    # Per-View Metrics
    pdf.add_section_header("Per-View Metrics")
    
    views = maint.get("maint_idi_views", [])
    if views:
        ok_views = [v for v in views if v.get("status") == "ok"]
        
        if ok_views:
            for view in ok_views:
                pdf.set_font("Helvetica", "B", 11)
                pdf.cell(0, 8, view.get("display_name", view.get("view_id", "?")), ln=True)
                pdf.set_font("Helvetica", "", 10)
                
                pdf.add_metric_row("  Total Records", f"{view.get('unique_instances', 0):,}")
                pdf.add_metric_row("  Duplicates", f"{view.get('duplicates', 0):,}")
                
                # Relations
                relations = view.get("relations", {})
                for rel_name, rel_data in relations.items():
                    rate = rel_data.get("rate")
                    rate_str = f"{rate:.1f}%" if rate is not None else "N/A"
                    pdf.add_metric_row(f"  - {rel_name.title()}", f"{rate_str} ({rel_data.get('count', 0):,} linked)")
                pdf.ln(2)
        else:
            pdf.add_paragraph("No views with data found.")
    else:
        pdf.add_paragraph("No view data available.")
    
    pdf.ln(5)
    
    # View Status Summary
    pdf.add_section_header("View Status Summary")
    
    if views:
        ok_count = len([v for v in views if v.get("status") == "ok"])
        empty_count = len([v for v in views if v.get("status") == "empty"])
        error_count = len([v for v in views if v.get("status") in ["not_found", "error"]])
        
        pdf.add_metric_row("Views with Data", f"{ok_count}", "good" if ok_count > 0 else None)
        pdf.add_metric_row("Empty Views", f"{empty_count}", "warning" if empty_count > 0 else None)
        pdf.add_metric_row("Views with Errors", f"{error_count}", "error" if error_count > 0 else None)
    
    # Insights
    pdf.add_section_header("Insights")
    insights = []
    
    if asset_coverage is not None:
        if asset_coverage >= 50:
            insights.append("Good asset maintenance coverage - half or more of assets have maintenance data.")
        elif asset_coverage >= 20:
            insights.append("Moderate asset coverage - consider reviewing assets without maintenance data.")
        else:
            insights.append("Low asset coverage - many assets lack maintenance records.")
    
    views_with_data = maint.get("maint_idi_views_with_data", 0)
    total_views = maint.get("maint_idi_total_views", 0)
    if total_views > 0 and views_with_data < total_views:
        insights.append(f"{total_views - views_with_data} maintenance views are empty or have errors.")
    
    if not insights:
        insights.append("No specific insights available.")
    
    for insight in insights:
        pdf.add_paragraph(f"- {insight}")
    
    # Metadata
    pdf.add_section_header("Report Metadata")
    pdf.add_metric_row("Computed At", metadata.get("computed_at", "Unknown"))
    
    return bytes(pdf.output())


def generate_staging_report(metrics: dict) -> bytes:
    """Generate PDF report for Staging vs DM Comparison dashboard."""
    staging = metrics.get("staging_metrics", {})
    metadata = metrics.get("metadata", {})
    
    pdf = ContextQualityReport("Staging vs Data Model Comparison Report")
    pdf.alias_nb_pages()
    pdf.add_page()
    
    # Executive Summary
    pdf.add_section_header("Executive Summary")
    overall_rate = staging.get("staging_overall_match_rate", 0)
    
    pdf.add_key_metric("Overall Match Rate", overall_rate)
    pdf.add_metric_row("Total Mappings", f"{staging.get('staging_total_mappings', 0)}")
    pdf.add_metric_row("Views Matched (>=99%)", f"{staging.get('staging_views_matched', 0)}")
    pdf.add_metric_row("Views with Gaps", f"{staging.get('staging_views_with_gaps', 0)}")
    pdf.add_metric_row("Views Not Found", f"{staging.get('staging_views_not_found', 0)}")
    pdf.add_metric_row("Views with Errors", f"{staging.get('staging_views_with_errors', 0)}")
    pdf.ln(5)
    
    # Data Volume
    pdf.add_section_header("Data Volume")
    pdf.add_metric_row("Total Raw Rows", f"{staging.get('staging_total_raw_rows', 0):,}")
    pdf.add_metric_row("Total DM Instances", f"{staging.get('staging_total_dm_instances', 0):,}")
    pdf.add_metric_row("Total Difference", f"{staging.get('staging_total_difference', 0):,}")
    pdf.ln(5)
    
    # Per-View Comparison
    pdf.add_section_header("Per-View Comparison")
    
    comparisons = staging.get("staging_comparisons", [])
    if comparisons:
        headers = ["View", "Raw", "DM", "Match %", "Status"]
        rows = []
        for comp in comparisons[:15]:  # Limit to 15 rows
            status = comp.get("status", "unknown")
            rate = comp.get("match_rate", 0)
            rows.append([
                comp.get("dm_view", "?")[:18],
                str(comp.get("raw_total", 0)),
                str(comp.get("dm_count", 0)),
                f"{rate:.1f}",
                status[:12]
            ])
        pdf.add_table(headers, rows)
        
        if len(comparisons) > 15:
            pdf.add_paragraph(f"... and {len(comparisons) - 15} more views")
    else:
        pdf.add_paragraph("No comparison data available.")
    
    pdf.ln(5)
    
    # Configuration
    pdf.add_section_header("Configuration Used")
    config = staging.get("staging_config", {})
    pdf.add_metric_row("Raw Database", config.get("raw_database", "Unknown"))
    pdf.add_metric_row("DM Space", config.get("dm_space", "Unknown"))
    pdf.add_metric_row("DM Version", config.get("dm_version", "Unknown"))
    pdf.ln(5)
    
    # Insights
    pdf.add_section_header("Insights")
    insights = []
    
    if overall_rate >= 99:
        insights.append("Excellent data pipeline health - staging and DM are well synchronized.")
    elif overall_rate >= 90:
        insights.append("Good data pipeline - minor discrepancies exist between staging and DM.")
    elif overall_rate >= 70:
        insights.append("Data pipeline needs attention - significant gaps between staging and DM.")
    else:
        insights.append("Critical: Large data loss between staging and DM - investigate immediately.")
    
    views_with_gaps = staging.get("staging_views_with_gaps", 0)
    if views_with_gaps > 0:
        insights.append(f"{views_with_gaps} views have data gaps that may need investigation.")
    
    views_not_found = staging.get("staging_views_not_found", 0)
    if views_not_found > 0:
        insights.append(f"{views_not_found} views were not found in the Data Model.")
    
    for insight in insights:
        pdf.add_paragraph(f"- {insight}")
    
    # Metadata
    pdf.add_section_header("Report Metadata")
    pdf.add_metric_row("Computed At", metadata.get("computed_at", "Unknown"))
    
    return bytes(pdf.output())


def generate_full_report(metrics: dict) -> bytes:
    """Generate a comprehensive PDF report covering all dashboards."""
    metadata = metrics.get("metadata") or {}
    
    pdf = ContextQualityReport("Contextualization Quality - Full Report")
    pdf.alias_nb_pages()
    pdf.add_page()
    
    # Title page content
    pdf.set_font("Helvetica", "B", 20)
    pdf.ln(30)
    pdf.cell(0, 15, "Contextualization Quality Report", ln=True, align="C")
    pdf.set_font("Helvetica", "", 12)
    pdf.ln(10)
    pdf.cell(0, 8, f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", ln=True, align="C")
    pdf.cell(0, 8, f"Metrics Computed: {metadata.get('computed_at', 'Unknown')}", ln=True, align="C")
    
    # Executive Overview
    pdf.add_page()
    pdf.add_section_header("Executive Overview")
    
    # Key metrics from each area
    hierarchy = metrics.get("hierarchy_metrics", {})
    equipment = metrics.get("equipment_metrics", {})
    ts = metrics.get("timeseries_metrics", {})
    maint = metrics.get("maintenance_metrics", {})
    files = metrics.get("file_metrics", {})
    others = metrics.get("others_metrics", {})
    
    pdf.add_key_metric("Asset Hierarchy Completion", hierarchy.get("hierarchy_completion_rate", 0))
    pdf.add_key_metric("Equipment Association", equipment.get("eq_association_rate", 0))
    pdf.add_key_metric("Time Series Contextualization", ts.get("ts_to_asset_rate", 0))
    
    if maint:
        pdf.add_key_metric("Work Order to Asset", maint.get("maint_order_to_asset_rate", 0) or 0)
    
    if files:
        pdf.add_key_metric("File to Asset", files.get("file_to_asset_rate", 0))
    
    if others and others.get("others_overall_asset_rate") is not None:
        pdf.add_key_metric("Others Asset Linkage", others.get("others_overall_asset_rate", 0))
    
    pdf.ln(10)
    
    # Instance Counts
    pdf.add_section_header("Data Volume Summary")
    instance_counts = metadata.get("instance_counts") or {}
    
    pdf.add_metric_row("Assets", f"{(instance_counts.get('assets') or {}).get('unique', 0):,}")
    pdf.add_metric_row("Time Series", f"{(instance_counts.get('timeseries') or {}).get('unique', 0):,}")
    pdf.add_metric_row("Equipment", f"{(instance_counts.get('equipment') or {}).get('unique', 0):,}")
    
    if "notifications" in instance_counts:
        pdf.add_metric_row("Notifications", f"{(instance_counts.get('notifications') or {}).get('unique', 0):,}")
        pdf.add_metric_row("Work Orders", f"{(instance_counts.get('maintenance_orders') or {}).get('unique', 0):,}")
    
    if "files" in instance_counts:
        pdf.add_metric_row("Files", f"{(instance_counts.get('files') or {}).get('unique', 0):,}")
    
    if others and others.get("others_total_instances", 0) > 0:
        pdf.add_metric_row("Others (IDI Views)", f"{others.get('others_total_instances', 0):,}")
    
    # Recommendations
    pdf.add_page()
    pdf.add_section_header("Recommendations")
    
    recommendations = []
    
    if hierarchy.get("hierarchy_completion_rate", 100) < 95:
        recommendations.append("Review and fix assets with missing parent links to improve hierarchy completion.")
    
    if equipment.get("eq_association_rate", 100) < 90:
        recommendations.append("Link unassociated equipment to their respective assets.")
    
    if ts.get("ts_to_asset_rate", 100) < 90:
        recommendations.append("Contextualize orphaned time series by linking them to assets.")
    
    if maint and (maint.get("maint_order_to_asset_rate") or 0) < 90:
        recommendations.append("Ensure all work orders are linked to assets for proper tracking.")
    
    if files and files.get("file_to_asset_rate", 100) < 80:
        recommendations.append("Link orphaned files to relevant assets for better document management.")
    
    if others and (others.get("others_overall_asset_rate") or 0) < 80:
        recommendations.append("Improve asset linkage for IDI views (Others category) to ensure proper data contextualization.")
    
    if not recommendations:
        recommendations.append("Excellent data quality! Continue monitoring to maintain standards.")
    
    for rec in recommendations:
        pdf.add_paragraph(f"- {rec}")
    
    return bytes(pdf.output())
