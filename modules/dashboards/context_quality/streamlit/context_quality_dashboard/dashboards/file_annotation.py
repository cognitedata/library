# -*- coding: utf-8 -*-
"""
File Annotation Quality Dashboard (CDM CogniteDiagramAnnotation).

Features user input boxes for reference numbers (files in scope, expected tags)
to calculate metrics that require baseline data not available in the data model.
"""

import streamlit as st
import plotly.graph_objects as go

from .common import (
    get_status_color_hierarchy,
    metric_card,
    gauge,
    gauge_na,
)
from .ai_summary import (
    render_ai_summary_section,
    get_file_annotation_prompt,
    format_file_annotation_metrics,
)
from .reports import generate_file_annotation_report


def get_status_color_annotation(metric_key, value):
    """Color function for file annotation metrics."""
    # Confidence-based metrics
    if metric_key == "avg_confidence":
        if value >= 90: return "#4CAF50"
        if value >= 70: return "#FFC107"
        return "#F44336"
    
    # Status metrics (approved rate)
    if metric_key == "approved_rate":
        if value >= 80: return "#4CAF50"
        if value >= 50: return "#FFC107"
        return "#F44336"
    
    # Processing rates (file coverage, etc.)
    if metric_key in ["file_processing", "asset_tag", "file_link"]:
        if value >= 90: return "#4CAF50"
        if value >= 70: return "#FFC107"
        return "#F44336"
    
    # High confidence rate
    if metric_key == "high_confidence":
        if value >= 70: return "#4CAF50"
        if value >= 50: return "#FFC107"
        return "#F44336"
    
    return "#0068C9"


def render_file_annotation_dashboard(metrics: dict):
    """Render the File Annotation Quality dashboard tab."""
    st.title("P&ID Annotation Quality Dashboard")
    st.markdown("*Based on CDM CogniteDiagramAnnotation*")
    
    annot = metrics.get("file_annotation_metrics", {})
    metadata = metrics.get("metadata", {})
    
    # Check if annotations are enabled
    config = metadata.get("config", {})
    annotations_enabled = config.get("enable_file_annotation_metrics", True)
    
    if not annotations_enabled:
        st.warning("""
        **P&ID Annotation Metrics Disabled**

        P&ID annotation metrics are disabled in the function configuration.
        To enable, set `enable_file_annotation_metrics: true` in the function input.
        """)
        return
    
    if not annot or not annot.get("annot_has_data", False):
        st.warning("""
        **No P&ID Annotation Data Found**
        
        No CogniteDiagramAnnotation edges found in the CDM.
        """)
        
        st.info("""
        **Prerequisites:**
        
        1. **Files must be processed** - Run diagram parsing on your P&ID files
        2. **Annotations must exist** - CogniteDiagramAnnotation edges should link files to assets/equipment
        
        **Common causes:**
        - Diagram parsing has not been run
        - No P&ID files in the project
        - Annotation data is in a different space
        
        **Configuration:**
        
        You can configure the annotation view in `metrics/common.py` -> Lines **40-42**:
        
        ```python
        "annotation_view_space": "cdf_cdm",
        "annotation_view_external_id": "CogniteDiagramAnnotation",
        "annotation_view_version": "v1",
        ```
        """)
        return
    
    # Show data info
    computed_at = metadata.get("computed_at", "Unknown")
    st.info(f"[Date] Metrics computed at: {computed_at}")
    
    # Understanding the metrics
    with st.expander("**Understanding the Metrics** - Click to learn more", expanded=False):
        st.markdown("""
        **Confidence Metrics:**
        - **Average Confidence** - Mean confidence score across all annotations (higher = more reliable)
        - **High Confidence (≥90%)** - Annotations reliable enough for auto-approval
        - **Medium Confidence (50-90%)** - May need spot-checking
        - **Low Confidence (<50%)** - Likely needs manual review
        
        **Status Metrics:**
        - **Approved Rate** - % of annotations that have been manually approved
        - **Suggested** - Annotations pending human review
        - **Rejected** - Annotations marked as incorrect
        
        **Coverage Metrics (Requires User Input):**
        - **File Processing Rate** - Files with annotations / Total files in scope
        - **Asset Tag Detection Rate** - Detected tags / Expected tags
        
        **Annotation Types:**
        - **Asset Tags** - Annotations linking to asset nodes
        - **File Links** - Annotations linking to other files (cross-references)
        
        *Tip: High confidence + high approved rate indicates good annotation quality. Enter reference numbers above to calculate coverage rates.*
        """)
    
    # Download Report Button
    st.download_button(
        label="Download P&ID Annotation Report (PDF)",
        data=generate_file_annotation_report(metrics),
        file_name="pnid_annotation_report.pdf",
        mime="application/pdf",
        use_container_width=True,
        type="primary",
        key="download_file_annotation_report"
    )
    
    # Extract base metrics
    total_annotations = annot.get("annot_total", 0)
    files_with_annotations = annot.get("annot_unique_files_with_annotations", 0)
    assets_linked = annot.get("annot_unique_assets_linked", 0)
    avg_confidence = annot.get("annot_avg_confidence")
    approved_pct = annot.get("annot_approved_pct", 0)
    
    st.markdown("---")
    
    # =====================================================
    # MAIN METRIC - BIG ON TOP
    # =====================================================
    st.header("Annotation Quality")
    
    col_main, col_info = st.columns([2, 1])
    
    with col_main:
        if avg_confidence is not None:
            gauge(col_main, "Average Annotation Confidence", avg_confidence, "avg_confidence",
                  get_status_color_annotation, [0, 100], "%", key="annot_avg_conf_main",
                  help_text="Average confidence score across all annotations")
        else:
            gauge_na(col_main, "Average Annotation Confidence", "No confidence data", key="annot_avg_conf_main_na")
    
    with col_info:
        st.markdown("### Why This Matters")
        st.markdown("""
        Annotation confidence indicates how reliable the 
        detected tags and links are:
        - **High confidence (>90%)**: Auto-approve safe
        - **Medium (50-90%)**: May need review
        - **Low (<50%)**: Likely needs manual review
        """)
        
        high_conf_pct = annot.get("annot_confidence_high_pct", 0)
        if avg_confidence is None:
            st.info("No confidence data available.")
        elif avg_confidence >= 90:
            st.success(f"Excellent! {high_conf_pct:.0f}% are high confidence.")
        elif avg_confidence >= 70:
            st.warning(f"Good quality. Review medium/low confidence annotations.")
        else:
            st.error(f"Many annotations may need manual review.")
    
    st.markdown("---")
    
    # =========================================
    # USER INPUT SECTION: Reference Numbers
    # =========================================
    st.header("Reference Numbers (User Input)")
    st.markdown("""
    *Enter reference numbers to calculate completion rates. These values are not stored in the data model,
    so you need to provide them manually for accurate rate calculations.*
    """)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("[Folder] Files in Scope")
        files_in_scope = st.number_input(
            "Total files that should be annotated",
            min_value=0,
            value=st.session_state.get("annot_files_in_scope", 0),
            step=1,
            help="Total number of P&ID files or documents that should be processed for annotation",
            key="files_in_scope_input"
        )
        st.session_state["annot_files_in_scope"] = files_in_scope
    
    with col2:
        st.subheader("[Tag] Expected Asset Tags")
        expected_tags = st.number_input(
            "Expected asset tags to detect",
            min_value=0,
            value=st.session_state.get("annot_expected_tags", 0),
            step=1,
            help="Estimated number of asset tags that should be detected across all files",
            key="expected_tags_input"
        )
        st.session_state["annot_expected_tags"] = expected_tags
    
    # Calculate dynamic rates based on user input
    file_processing_rate = None
    if files_in_scope > 0:
        file_processing_rate = round(files_with_annotations / files_in_scope * 100, 1)
    
    asset_tag_rate = None
    if expected_tags > 0:
        asset_tag_rate = round(annot.get("annot_asset_tags", 0) / expected_tags * 100, 1)
    
    # =========================================
    # SECTION 1: Summary Cards
    # =========================================
    st.markdown("---")
    st.header("[Chart] Summary")
    
    col1, col2, col3, col4 = st.columns(4)
    metric_card(col1, "Total Annotations", f"{total_annotations:,}",
                help_text="Total CogniteDiagramAnnotation edges")
    metric_card(col2, "Files with Annotations", f"{files_with_annotations:,}",
                help_text="Unique files that have at least one annotation")
    metric_card(col3, "Assets Linked", f"{assets_linked:,}",
                help_text="Unique assets linked via annotations")
    metric_card(col4, "Pages Annotated", f"{annot.get('annot_unique_pages', 0):,}",
                help_text="Unique file pages with annotations")
    
    st.markdown("---")
    
    # =========================================
    # SECTION 2: Calculated Rates (from user input)
    # =========================================
    st.header("Calculated Rates")
    st.caption("*Rates calculated using reference numbers you provided above*")
    
    g1, g2 = st.columns(2)
    
    # File Processing Rate (requires user input)
    if file_processing_rate is not None:
        gauge(g1, "File Processing Rate", file_processing_rate, "file_processing",
              get_status_color_annotation, [0, 100], "%", key="annot_file_rate",
              help_text=f"Files with annotations ({files_with_annotations}) / Files in scope ({files_in_scope})")
    else:
        gauge_na(g1, "File Processing Rate", "Enter 'Files in Scope'", key="annot_file_rate_na",
                 help_text="Enter the total files in scope above to calculate this rate")
    
    # Asset Tag Rate (requires user input)
    if asset_tag_rate is not None:
        gauge(g2, "Asset Tag Detection Rate", asset_tag_rate, "asset_tag",
              get_status_color_annotation, [0, 100], "%", key="annot_tag_rate",
              help_text=f"Asset annotations ({annot.get('annot_asset_tags', 0)}) / Expected tags ({expected_tags})")
    else:
        gauge_na(g2, "Asset Tag Detection Rate", "Enter 'Expected Tags'", key="annot_tag_rate_na",
                 help_text="Enter the expected asset tags above to calculate this rate")
    
    st.markdown("---")
    
    # =========================================
    # SECTION 3: Confidence Metrics
    # =========================================
    st.header("Confidence Distribution")
    st.markdown("*How confident is the annotation model in its predictions?*")
    
    high_conf_pct = annot.get("annot_confidence_high_pct", 0)
    medium_conf_pct = annot.get("annot_confidence_medium_pct", 0)
    low_conf_pct = annot.get("annot_confidence_low_pct", 0)
    
    c1, c2, c3, c4 = st.columns(4)
    
    # Approved rate gauge (since avg_confidence is now on top)
    gauge(c1, "Approved Rate", approved_pct, "approved_rate",
          get_status_color_annotation, [0, 100], "%", key="annot_approved_conf",
          help_text="% of annotations that have been approved")
    
    # High confidence gauge
    gauge(c2, "High Confidence", high_conf_pct, "high_confidence",
          get_status_color_annotation, [0, 100], "%", key="annot_high_conf",
          help_text="% of annotations with confidence >= 90%")
    
    # Confidence distribution chart
    with c3:
        fig = go.Figure(data=[go.Pie(
            labels=["High (>=90%)", "Medium (50-90%)", "Low (<50%)"],
            values=[
                annot.get("annot_confidence_high", 0),
                annot.get("annot_confidence_medium", 0),
                annot.get("annot_confidence_low", 0)
            ],
            marker_colors=["#4CAF50", "#FFC107", "#F44336"],
            hole=0.4,
            textinfo="percent",
            textposition="inside",
            hoverinfo="label+percent+value"
        )])
        fig.update_layout(
            title="Confidence Distribution",
            height=300,
            margin=dict(l=20, r=20, t=40, b=20),
            showlegend=True,
            legend=dict(orientation="h", y=-0.1, x=0.5, xanchor="center")
        )
        st.plotly_chart(fig, use_container_width=True, key="conf_pie")
    
    with c4:
        st.markdown("### Confidence Counts")
        st.markdown(f"""
        | Level | Count |
        |-------|-------|
        | High (>=90%) | {annot.get('annot_confidence_high', 0):,} |
        | Medium (50-90%) | {annot.get('annot_confidence_medium', 0):,} |
        | Low (<50%) | {annot.get('annot_confidence_low', 0):,} |
        | Missing | {annot.get('annot_confidence_missing', 0):,} |
        """)
    
    st.markdown("---")
    
    # =========================================
    # SECTION 4: Status Distribution
    # =========================================
    st.header("Annotation Status")
    st.markdown("*Review status of annotations (approved, suggested, rejected)*")
    
    suggested_pct = annot.get("annot_suggested_pct", 0)
    rejected_pct = annot.get("annot_rejected_pct", 0)
    
    s1, s2, s3 = st.columns(3)
    
    # Summary metrics
    s1.metric("Approved", f"{annot.get('annot_approved', 0):,}", 
              help=f"{approved_pct:.1f}% of total annotations")
    
    # Status distribution chart
    with s2:
        fig = go.Figure(data=[go.Pie(
            labels=["Approved", "Suggested", "Rejected"],
            values=[
                annot.get("annot_approved", 0),
                annot.get("annot_suggested", 0),
                annot.get("annot_rejected", 0)
            ],
            marker_colors=["#4CAF50", "#0068C9", "#F44336"],
            hole=0.4,
            textinfo="percent+label"
        )])
        fig.update_layout(
            title="Status Distribution",
            height=300,
            margin=dict(l=20, r=20, t=40, b=20),
            showlegend=False
        )
        st.plotly_chart(fig, use_container_width=True, key="status_pie")
    
    with s3:
        st.markdown("### Status Counts")
        st.markdown(f"""
        | Status | Count |
        |--------|-------|
        | Approved | {annot.get('annot_approved', 0):,} |
        | Suggested | {annot.get('annot_suggested', 0):,} |
        | Rejected | {annot.get('annot_rejected', 0):,} |
        """)
        
        # Manual review indicator
        suggested = annot.get('annot_suggested', 0)
        if suggested > 0:
            st.warning(f"{suggested:,} annotations pending review")
    
    st.markdown("---")
    
    # =========================================
    # SECTION 5: Annotation Types
    # =========================================
    st.header("Annotation Types")
    st.markdown("*What types of entities are being linked?*")
    
    t1, t2 = st.columns(2)
    
    with t1:
        # Annotation type distribution chart
        fig = go.Figure(data=[go.Bar(
            x=["Asset Tags", "File Links", "Other"],
            y=[
                annot.get("annot_asset_tags", 0),
                annot.get("annot_file_links", 0),
                annot.get("annot_other", 0)
            ],
            marker_color=["#0068C9", "#4CAF50", "#888888"]
        )])
        fig.update_layout(
            title="Annotations by Target Type",
            height=300,
            margin=dict(l=20, r=20, t=40, b=20),
            xaxis_title="Type",
            yaxis_title="Count"
        )
        st.plotly_chart(fig, use_container_width=True, key="type_bar")
    
    with t2:
        st.markdown("### Type Breakdown")
        st.markdown(f"""
        | Type | Count | % |
        |------|-------|---|
        | [Tag] Asset Tags | {annot.get('annot_asset_tags', 0):,} | {annot.get('annot_asset_tag_pct', 0):.1f}% |
        | File Links | {annot.get('annot_file_links', 0):,} | {annot.get('annot_file_link_pct', 0):.1f}% |
        | Other | {annot.get('annot_other', 0):,} | - |
        """)
        
        st.markdown("---")
        
        st.markdown("### Unique Entities")
        st.markdown(f"""
        | Entity | Count |
        |--------|-------|
        | Unique Assets Linked | {assets_linked:,} |
        | Unique Files Linked | {annot.get('annot_unique_files_linked', 0):,} |
        """)
    
    # =========================================
    # SECTION 6: Detailed Metrics Table
    # =========================================
    st.markdown("---")
    st.subheader("Detailed Metrics")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Core Metrics")
        st.markdown(f"""
        | Metric | Value |
        |--------|-------|
        | Total Annotations | {total_annotations:,} |
        | Files with Annotations | {files_with_annotations:,} |
        | Pages with Annotations | {annot.get('annot_unique_pages', 0):,} |
        | Unique Assets Linked | {assets_linked:,} |
        | Unique Files Linked | {annot.get('annot_unique_files_linked', 0):,} |
        """)
    
    with col2:
        st.markdown("### Calculated Rates")
        file_rate_display = f"{file_processing_rate:.1f}%" if file_processing_rate is not None else "N/A (enter files in scope)"
        tag_rate_display = f"{asset_tag_rate:.1f}%" if asset_tag_rate is not None else "N/A (enter expected tags)"
        
        st.markdown(f"""
        | Metric | Value |
        |--------|-------|
        | File Processing Rate | {file_rate_display} |
        | Asset Tag Detection Rate | {tag_rate_display} |
        | Average Confidence | {annot.get('annot_avg_confidence', 'N/A')}% |
        | Approved Rate | {approved_pct:.1f}% |
        | High Confidence Rate | {high_conf_pct:.1f}% |
        """)
    
    # AI SUMMARY SECTION
    # Pass user-provided reference numbers to the AI
    render_ai_summary_section(
        dashboard_type="P&ID Annotation Quality",
        metrics_data=format_file_annotation_metrics(annot, files_in_scope, expected_tags),
        system_prompt=get_file_annotation_prompt(),
        key_prefix="file_annotation"
    )
    
    st.markdown("---")
    st.success("P&ID Annotation dashboard loaded from pre-computed metrics.")
