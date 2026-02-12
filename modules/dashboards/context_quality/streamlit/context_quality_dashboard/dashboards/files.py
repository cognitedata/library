# -*- coding: utf-8 -*-
"""
File Contextualization Dashboard.

Displays metrics about file-to-asset relationships and file metadata quality.
Primary metric: Files linked to Assets (contextualization coverage)
"""

import streamlit as st

from .common import (
    get_status_color_files,
    metric_card,
    gauge,
    gauge_na,
)
from .ai_summary import (
    render_ai_summary_section,
)
from .reports import generate_files_report


def get_file_prompt() -> str:
    """System prompt for file metrics AI summary."""
    return """You are an expert data quality analyst. Analyze file contextualization metrics 
and provide actionable insights. Focus on:
1. File-to-Asset coverage - are files properly linked to assets?
2. Metadata completeness - do files have names, descriptions, categories?
3. Upload status - are file contents actually uploaded?
4. Distribution patterns - any unusual MIME type or category patterns?
Keep response concise (3-5 bullet points)."""


def format_file_metrics(metrics: dict) -> str:
    """Format file metrics for AI summary."""
    return f"""
File Contextualization Metrics:
- Total Files: {metrics.get('file_total', 0):,}
- File→Asset Rate: {metrics.get('file_to_asset_rate', 0)}%
- Files with Assets: {metrics.get('files_with_assets', 0):,}
- Files without Assets: {metrics.get('files_without_assets', 0):,}
- Asset File Coverage: {metrics.get('file_asset_coverage', 0)}%
- Assets with Files: {metrics.get('assets_with_files', 0):,}
- Category Rate: {metrics.get('file_category_rate', 0)}%
- Upload Rate: {metrics.get('file_upload_rate', 0)}%
- Name Completeness: {metrics.get('file_name_rate', 0)}%
- Description Rate: {metrics.get('file_description_rate', 0)}%
- Unique MIME Types: {metrics.get('unique_mime_types', 0)}
- Unique Categories: {metrics.get('unique_categories', 0)}
"""


def render_files_dashboard(metrics: dict):
    """Render the File Contextualization dashboard tab."""
    st.title("File Contextualization Quality Dashboard")
    
    file_metrics = metrics.get("file_metrics", {})
    hierarchy = metrics.get("hierarchy_metrics", {})
    metadata = metrics.get("metadata", {})
    
    if not file_metrics or not file_metrics.get("file_has_data", False):
        st.warning("No file metrics found. Enable 'enable_file_metrics' in the configuration and run the function.")
        return
    
    # Show data info
    computed_at = metadata.get("computed_at", "Unknown")
    st.info(f"[Date] Metrics computed at: {computed_at}")
    
    # Understanding the metrics
    with st.expander("**Understanding the Metrics** - Click to learn more", expanded=False):
        st.markdown("""
        **Contextualization Metrics:**
        - **File → Asset Rate** - % of files linked to at least one asset (should be high)
        - **Asset File Coverage** - % of assets with files linked (informational - not all assets need files)
        
        **Metadata Quality Metrics:**
        - **Category Rate** - % of files with a category (document type) assigned
        - **Content Uploaded** - % of files with actual content uploaded (not just metadata)
        - **Name Completeness** - % of files with a name populated
        - **Description Rate** - % of files with a description (helps searchability)
        - **Source ID Rate** - % of files with source system identifier
        
        **Distribution Metrics:**
        - **Avg/Max Files per Asset** - File distribution across assets
        - **MIME Types** - Distribution of file formats (PDF, DWG, etc.)
        - **Categories** - Distribution of document types
        
        *Tip: File → Asset Rate is the key metric - orphaned files cannot be found via the asset tree.*
        """)
    
    # Download Report Button
    st.download_button(
        label="Download Files Report (PDF)",
        data=generate_files_report(metrics),
        file_name="files_report.pdf",
        mime="application/pdf",
        use_container_width=True,
        type="primary",
        key="download_files_report"
    )
    
    st.markdown("---")
    
    # Extract key metrics
    total_files = file_metrics.get("file_total", 0)
    file_to_asset_rate = file_metrics.get("file_to_asset_rate", 0)
    files_with_assets = file_metrics.get("files_with_assets", 0)
    files_without_assets = file_metrics.get("files_without_assets", 0)
    asset_file_coverage = file_metrics.get("file_asset_coverage", 0)
    assets_with_files = file_metrics.get("assets_with_files", 0)
    total_assets = hierarchy.get("hierarchy_total_assets", 0)
    
    # =====================================================
    # MAIN METRIC - BIG ON TOP
    # =====================================================
    st.header("File-to-Asset Contextualization")
    
    # Large gauge for primary metric
    col_main, col_info = st.columns([2, 1])
    
    with col_main:
        gauge(
            col_main, 
            "Files Linked to Assets", 
            file_to_asset_rate, 
            "file_to_asset",
            get_status_color_files, 
            [0, 100], 
            "%", 
            key="file_to_asset_primary",
            help_text="PRIMARY: Percentage of files that are linked to at least one asset. Higher is better - orphaned files lack context."
        )
    
    with col_info:
        st.markdown("### Why This Matters")
        st.markdown("""
        Files without asset links are **orphaned** - they exist in the system 
        but lack business context. This makes it difficult to:
        - Find relevant documentation for assets
        - Understand which equipment a P&ID diagram relates to
        - Associate inspection reports with physical assets
        """)
        
        if file_to_asset_rate < 50:
            st.error(f"{files_without_assets:,} files are not linked to any asset!")
        elif file_to_asset_rate < 80:
            st.warning(f"{files_without_assets:,} files could benefit from asset linkage.")
        else:
            st.success(f"Good coverage! Only {files_without_assets:,} files are orphaned.")
    
    st.markdown("---")
    
    # =====================================================
    # SUMMARY CARDS
    # =====================================================
    col1, col2, col3, col4 = st.columns(4)
    metric_card(col1, "Total Files", f"{total_files:,}",
                help_text="Total number of unique files in the system")
    metric_card(col2, "Files with Assets", f"{files_with_assets:,}",
                help_text="Files linked to at least one asset")
    metric_card(col3, "Orphaned Files", f"{files_without_assets:,}",
                help_text="Files NOT linked to any asset")
    metric_card(col4, "Assets with Files", f"{assets_with_files:,}",
                help_text="Number of assets that have at least one file linked")
    
    st.markdown("---")
    
    # =====================================================
    # SECONDARY METRICS
    # =====================================================
    st.header("Additional Quality Metrics")
    
    # Row 1: Coverage and Category
    g1, g2, g3 = st.columns(3)
    
    # Asset File Coverage (informational - not all assets need files)
    gauge(g1, "Asset File Coverage", asset_file_coverage, "asset_coverage",
          get_status_color_files, [0, 100], "%", key="file_asset_cov",
          help_text="INFORMATIONAL: % of assets with files. Low values OK - not all assets need documentation.")
    
    # Category completeness
    category_rate = file_metrics.get("file_category_rate", 0)
    files_uncategorized = file_metrics.get("files_uncategorized", 0)
    gauge(g2, "Category Assigned", category_rate, "category",
          get_status_color_files, [0, 100], "%", key="file_category",
          help_text=f"% of files with a category (document type) assigned. {files_uncategorized:,} uncategorized.")
    
    # Upload status
    upload_rate = file_metrics.get("file_upload_rate", 0)
    gauge(g3, "Content Uploaded", upload_rate, "upload",
          get_status_color_files, [0, 100], "%", key="file_upload",
          help_text="% of files that have actual content uploaded (not just metadata)")
    
    st.markdown("")
    
    # Row 2: Metadata Completeness
    g4, g5, g6 = st.columns(3)
    
    name_rate = file_metrics.get("file_name_rate", 0)
    gauge(g4, "Name Completeness", name_rate, "name",
          get_status_color_files, [0, 100], "%", key="file_name",
          help_text="% of files with a name property populated")
    
    description_rate = file_metrics.get("file_description_rate", 0)
    gauge(g5, "Description Rate", description_rate, "description",
          get_status_color_files, [0, 100], "%", key="file_desc",
          help_text="% of files with a description (helpful for searchability)")
    
    source_id_rate = file_metrics.get("file_source_id_rate", 0)
    gauge(g6, "Source ID Rate", source_id_rate, "source_id",
          get_status_color_files, [0, 100], "%", key="file_source",
          help_text="% of files with a source system identifier")
    
    st.markdown("---")
    
    # =====================================================
    # DIAGNOSTICS
    # =====================================================
    st.subheader("Diagnostics & Distribution")
    
    c1, c2 = st.columns(2)
    
    with c1:
        st.markdown("### File Statistics")
        avg_files = file_metrics.get("avg_files_per_asset", 0)
        max_files = file_metrics.get("max_files_per_asset", 0)
        unique_mime = file_metrics.get("unique_mime_types", 0)
        unique_cats = file_metrics.get("unique_categories", 0)
        files_uploaded = file_metrics.get("files_uploaded", 0)
        files_with_name = file_metrics.get("files_with_name", 0)
        files_with_desc = file_metrics.get("files_with_description", 0)
        files_with_cat = file_metrics.get("files_with_category", 0)
        
        files_without_cat = file_metrics.get("files_uncategorized", 0)
        
        st.markdown(f"""
        | Metric | Value |
        |--------|-------|
        | Total Files | {total_files:,} |
        | Files with Assets | {files_with_assets:,} |
        | Orphaned Files | {files_without_assets:,} |
        | Total Assets | {total_assets:,} |
        | Assets with Files | {assets_with_files:,} |
        | Avg Files per Asset | {avg_files:.1f} |
        | Max Files per Asset | {max_files:,} |
        | Files with Content | {files_uploaded:,} |
        | Files with Name | {files_with_name:,} |
        | Files with Description | {files_with_desc:,} |
        | Files with Category | {files_with_cat:,} |
        | Files without Category | {files_without_cat:,} |
        | Unique MIME Types | {unique_mime:,} |
        | Unique Categories | {unique_cats:,} |
        """)
    
    with c2:
        # Top MIME Types
        st.markdown("### Top MIME Types")
        top_mime = file_metrics.get("top_mime_types", {})
        if top_mime:
            for mime, count in list(top_mime.items())[:8]:
                pct = (count / total_files * 100) if total_files > 0 else 0
                st.markdown(f"- **{mime}**: {count:,} ({pct:.1f}%)")
        else:
            st.info("No MIME type data available")
        
        st.markdown("### Top Categories")
        top_cats = file_metrics.get("top_categories", {})
        if top_cats:
            for cat, count in list(top_cats.items())[:5]:
                pct = (count / total_files * 100) if total_files > 0 else 0
                st.markdown(f"- **{cat}**: {count:,} ({pct:.1f}%)")
        else:
            st.info("No category data available")
        
        # Uncategorized files download
        uncategorized_ids = file_metrics.get("files_uncategorized_ids", [])
        files_uncategorized_count = file_metrics.get("files_uncategorized", 0)
        if files_uncategorized_count > 0:
            st.markdown(f"**Uncategorized Files:** {files_uncategorized_count:,}")
            if uncategorized_ids:
                csv_data = "external_id\n" + "\n".join(uncategorized_ids)
                st.download_button(
                    label=f"Download Uncategorized File IDs ({len(uncategorized_ids):,} items)",
                    data=csv_data,
                    file_name="uncategorized_files.csv",
                    mime="text/csv",
                    key="download_uncategorized_files"
                )
    
    # =====================================================
    # AI SUMMARY
    # =====================================================
    render_ai_summary_section(
        dashboard_type="File Contextualization",
        metrics_data=format_file_metrics(file_metrics),
        system_prompt=get_file_prompt(),
        key_prefix="files"
    )
    
    st.markdown("---")
    st.success("File Contextualization dashboard loaded from pre-computed metrics.")
