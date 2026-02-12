# -*- coding: utf-8 -*-
"""
Staging vs Data Model Dashboard.

Displays pre-computed comparison between CDF Raw tables (staging) and 
Data Model instances to identify data pipeline discrepancies.
"""

import streamlit as st
import pandas as pd
from typing import Dict, Any, Optional
from datetime import datetime, timezone
from cognite.client import CogniteClient

from .reports import generate_staging_report


# ============================================================
# REASONS STORAGE (CDF Raw Table)
# ============================================================

REASONS_DB = "context_quality"
REASONS_TABLE = "staging_comparison_reasons"


def get_client() -> CogniteClient:
    """Get the Cognite client from session state."""
    if 'cognite_client' not in st.session_state:
        st.session_state['cognite_client'] = CogniteClient()
    return st.session_state['cognite_client']


def load_reasons() -> Dict[str, str]:
    """Load saved reasons from CDF Raw table."""
    try:
        client = get_client()
        
        # Check if table exists first
        try:
            tables = client.raw.tables.list(REASONS_DB, limit=100)
            table_names = [t.name for t in tables]
            if REASONS_TABLE not in table_names:
                return {}  # Table doesn't exist yet, return empty
        except Exception:
            return {}  # Database doesn't exist yet
        
        # Load all reasons
        reasons = {}
        try:
            rows = client.raw.rows.list(REASONS_DB, REASONS_TABLE, limit=None)
            for row in rows:
                reasons[row.key] = row.columns.get("reason", "")
        except Exception:
            pass
        return reasons
    except Exception as e:
        # Don't show warning on first load when table doesn't exist
        return {}


def save_reasons(reasons: Dict[str, str]) -> bool:
    """Save reasons to CDF Raw table."""
    try:
        client = get_client()
        
        # Ensure database exists
        try:
            client.raw.databases.create(REASONS_DB)
        except Exception:
            pass  # Already exists
        
        # Ensure table exists
        try:
            client.raw.tables.create(REASONS_DB, REASONS_TABLE)
        except Exception:
            pass  # Already exists
        
        # Prepare rows as dict format: {key: columns_dict}
        row_data = {}
        for dm_view, reason in reasons.items():
            if reason and reason.strip():  # Only save non-empty reasons
                row_data[dm_view] = {
                    "reason": reason.strip(),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
        
        if row_data:
            # Use the dict format for insert
            client.raw.rows.insert(REASONS_DB, REASONS_TABLE, row=row_data, ensure_parent=True)
            return True
        return False
    except Exception as e:
        st.error(f"Could not save reasons: {e}")
        return False


# ============================================================
# UI HELPER FUNCTIONS
# ============================================================

def get_status_color(status: str) -> str:
    """Return color based on status."""
    colors = {
        "matched": "#4CAF50",           # Green
        "minor_gap": "#FFC107",         # Yellow
        "significant_gap": "#FF9800",   # Orange
        "major_gap": "#F44336",         # Red
        "view_not_found": "#9E9E9E",    # Gray
        "raw_error": "#9E9E9E",         # Gray
    }
    return colors.get(status, "#0068C9")


def get_status_emoji(status: str) -> str:
    """Return emoji based on status."""
    emojis = {
        "matched": "OK",
        "minor_gap": "(!)",
        "significant_gap": "(!)",
        "major_gap": "X",
        "view_not_found": "?",
        "raw_error": "(!)",
    }
    return emojis.get(status, "-")


def get_status_label(status: str) -> str:
    """Return human-readable status label."""
    labels = {
        "matched": "Matched",
        "minor_gap": "Minor Gap",
        "significant_gap": "Significant Gap",
        "major_gap": "Major Gap",
        "view_not_found": "View Not Found",
        "raw_error": "Raw Error",
    }
    return labels.get(status, status)


def render_summary_cards(staging_metrics: Dict[str, Any]):
    """Render summary metric cards."""
    total_mappings = staging_metrics.get("staging_total_mappings", 0)
    matched = staging_metrics.get("staging_views_matched", 0)
    with_gaps = staging_metrics.get("staging_views_with_gaps", 0)
    not_found = staging_metrics.get("staging_views_not_found", 0)
    with_errors = staging_metrics.get("staging_views_with_errors", 0)
    
    total_raw = staging_metrics.get("staging_total_raw_rows", 0)
    total_dm = staging_metrics.get("staging_total_dm_instances", 0)
    overall_match = staging_metrics.get("staging_overall_match_rate", 0)
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Total Mappings", total_mappings)
    with col2:
        st.metric("Matched (>=99%)", matched)
    with col3:
        st.metric("With Gaps", with_gaps)
    with col4:
        st.metric("Not Found", not_found)
    with col5:
        st.metric("Errors", with_errors)
    
    st.markdown("---")
    
    # Overall totals
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Raw Rows", f"{total_raw:,}")
    with col2:
        st.metric("Total DM Instances", f"{total_dm:,}")
    with col3:
        difference = staging_metrics.get("staging_total_difference", abs(total_raw - total_dm))
        gap_direction = staging_metrics.get("staging_overall_gap_direction", "equal")
        if gap_direction == "dm_exceeds_raw":
            delta_text = f"+{difference:,} more in DM"
        elif gap_direction == "raw_exceeds_dm":
            delta_text = f"-{difference:,} missing in DM"
        else:
            delta_text = "0 difference"
        st.metric("Overall Match Rate", f"{overall_match}%", delta=delta_text)


def render_comparison_table(comparisons: list):
    """Render the main comparison table with editable reasons."""
    st.subheader("Detailed Comparison")
    
    if not comparisons:
        st.info("No comparison data available.")
        return
    
    # Load existing reasons
    saved_reasons = load_reasons()
    
    # Build display data: add Raw Table(s) column; omit Status and Match %
    rows = []
    for comp in comparisons:
        diff = comp.get('difference', 0)
        gap_dir = comp.get('gap_direction', 'equal')
        if gap_dir == "dm_exceeds_raw":
            diff_str = f"+{diff:,} (DM)"
        elif gap_dir == "raw_exceeds_dm":
            diff_str = f"-{diff:,} (Raw)"
        else:
            diff_str = "0"
        raw_tables = comp.get("raw_tables", []) or list(comp.get("raw_breakdown", {}).keys())
        raw_tables_str = ", ".join(raw_tables) if raw_tables else "—"
        dm_view = comp.get("dm_view", "")
        rows.append({
            "Raw Table(s)": raw_tables_str,
            "DM View": dm_view,
            "Raw Rows": comp.get('raw_total', 0),
            "DM Instances": comp.get('dm_count', 0),
            "Difference": diff_str,
            "Reason": saved_reasons.get(dm_view, ""),
        })
    
    df = pd.DataFrame(rows)
    
    # Use data_editor for editable Reason column
    edited_df = st.data_editor(
        df,
        use_container_width=True,
        hide_index=True,
        disabled=["Raw Table(s)", "DM View", "Raw Rows", "DM Instances", "Difference"],
        column_config={
            "Raw Table(s)": st.column_config.TextColumn(
                "Raw Table(s)",
                width="large",
                help="Staging / Raw table(s) compared to the DM view"
            ),
            "DM View": st.column_config.TextColumn("DM View", width="medium"),
            "Raw Rows": st.column_config.NumberColumn("Raw Rows", width="small", format="%d"),
            "DM Instances": st.column_config.NumberColumn("DM Instances", width="small", format="%d"),
            "Difference": st.column_config.TextColumn("Difference", width="small"),
            "Reason": st.column_config.TextColumn(
                "Reason for Difference",
                width="large",
                help="Enter explanation for the count difference"
            ),
        },
        key="staging_comparison_table"
    )
    
    # Save button for reasons
    col_save, col_info = st.columns([1, 3])
    with col_save:
        if st.button("Save Reasons", type="primary", use_container_width=True):
            # Extract edited reasons
            new_reasons = {}
            for _, row in edited_df.iterrows():
                dm_view = row["DM View"]
                reason = row.get("Reason", "")
                if reason and str(reason).strip():
                    new_reasons[dm_view] = str(reason).strip()
            
            if new_reasons:
                if save_reasons(new_reasons):
                    st.success(f"Saved {len(new_reasons)} reason(s) successfully!")
                    st.rerun()  # Refresh to show saved data
                else:
                    st.error("Failed to save reasons. Check console for errors.")
            else:
                st.info("No reasons to save. Enter text in the 'Reason for Difference' column first.")
    
    with col_info:
        st.caption("Edit the 'Reason for Difference' column above, then click Save to persist your explanations.")


def render_detailed_breakdown(comparisons: list):
    """Render detailed breakdown for each view."""
    st.subheader("Detailed Breakdown by View")
    
    for comp in comparisons:
        status = comp.get("status", "")
        status_emoji = get_status_emoji(status)
        status_label = get_status_label(status)
        match_rate = comp.get("match_rate", 0)
        view_id = comp.get("dm_view", "Unknown")
        
        with st.expander(f"{status_emoji} **{view_id}** - {status_label} ({match_rate:.1f}%)"):
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**Source Raw Tables:**")
                raw_breakdown = comp.get("raw_breakdown", {})
                if raw_breakdown:
                    for table, count in raw_breakdown.items():
                        st.markdown(f"- `{table}`: {count:,} rows")
                    st.markdown(f"**Total Raw Rows: {comp.get('raw_total', 0):,}**")
                else:
                    tables = comp.get("raw_tables", [])
                    for table in tables:
                        st.markdown(f"- `{table}`")
                    st.markdown(f"**Total Raw Rows: {comp.get('raw_total', 0):,}**")
            
            with col2:
                st.markdown("**Data Model View:**")
                st.markdown(f"- Space: `{comp.get('dm_space', 'rmdm')}`")
                st.markdown(f"- View: `{view_id}`")
                st.markdown(f"- Version: `{comp.get('dm_version', 'v1')}`")
                st.markdown(f"**Total Instances: {comp.get('dm_count', 0):,}**")
            
            # Show errors if any
            errors = comp.get("errors", [])
            if errors:
                st.warning("Errors encountered:")
                for err in errors:
                    st.markdown(f"- {err}")
            
            # Show difference analysis
            diff = comp.get("difference", 0)
            gap_dir = comp.get("gap_direction", "equal")
            if gap_dir == "raw_exceeds_dm":
                st.error(f"**{diff:,} rows** in staging are missing from the Data Model")
            elif gap_dir == "dm_exceeds_raw":
                st.warning(f"**{diff:,} more instances** in DM than in staging (possible duplicates or derived data)")
            else:
                st.success("Row counts match exactly!")


# ============================================================
# MAIN RENDER FUNCTION
# ============================================================

def render_staging_vs_dm_dashboard(metrics: dict):
    """
    Render the Staging vs Data Model comparison dashboard.
    
    Args:
        metrics: The full metrics dictionary containing staging_metrics
    """
    st.title("Staging vs Data Model Comparison")
    
    staging_metrics = metrics.get("staging_metrics", {})
    metadata = metrics.get("metadata", {})
    
    # Check if staging metrics are available
    if not staging_metrics or not staging_metrics.get("staging_has_data", False):
        st.warning("""
        **Staging comparison metrics not available.**
        
        To enable staging vs DM comparison:
        
        **Via Dashboard (Configure & Run tab):**
        - Enable "Staging Metrics" in the configuration
        - Run the metrics function
        
        **Via Local Script:**
        ```bash
        python run_metrics.py --staging
        ```
        
        **Via SDK:**
        ```python
        client.functions.call(
            external_id="context_quality_handler",
            data={"enable_staging_metrics": True}
        )
        ```
        """)
        return
    
    # Show data info
    computed_at = metadata.get("computed_at", "Unknown")
    st.info(f"Metrics computed at: {computed_at}")
    
    # Configuration section
    staging_config = staging_metrics.get("staging_config", {})
    with st.expander("**Configuration**", expanded=False):
        st.markdown(f"""
        **Current Settings:**
        - Raw Database: `{staging_config.get('raw_database', 'oracle:db')}`
        - DM Space: `{staging_config.get('dm_space', 'rmdm')}`
        - DM Version: `{staging_config.get('dm_version', 'v1')}`
        - Total Mappings: {staging_metrics.get('staging_total_mappings', 0)}
        
        *To modify the mapping, update the configuration in the function or local script.*
        """)
    
    # Understanding the metrics
    with st.expander("**Understanding the Metrics** - Click to learn more", expanded=False):
        st.markdown("""
        **Match Rate Categories:**
        - **Matched (≥99%)** - Staging and DM are in sync; minimal or no data loss
        - **Minor Gap (90-99%)** - Small discrepancy; may be due to filtering or timing
        - **Significant Gap (50-90%)** - Noticeable data loss; investigate pipeline
        - **Major Gap (<50%)** - Critical data loss; pipeline issue likely
        
        **Status Indicators:**
        - **View Not Found** - The DM view doesn't exist in the configured space
        - **Raw Error** - Error accessing the Raw table (permissions, table not found)
        
        **Metrics Explained:**
        - **Raw Rows** - Total rows in the source Raw table(s)
        - **DM Instances** - Total instances in the Data Model view
        - **Difference** - Gap between Raw and DM (positive = data loss)
        
        *Tip: A healthy pipeline should have ≥99% match rate for all mappings.*
        """)
    
    # Download Report Button
    st.download_button(
        label="Download Staging Report (PDF)",
        data=generate_staging_report(metrics),
        file_name="staging_vs_dm_report.pdf",
        mime="application/pdf",
        use_container_width=True,
        type="primary",
        key="download_staging_report"
    )
    
    st.markdown("---")
    
    # Summary cards
    st.subheader("Summary")
    render_summary_cards(staging_metrics)
    
    st.markdown("---")
    
    # Main comparison table
    comparisons = staging_metrics.get("staging_comparisons", [])
    render_comparison_table(comparisons)
    
    st.markdown("---")
    
    # Detailed breakdown
    render_detailed_breakdown(comparisons)
    
    st.markdown("---")
    
    # Legend
    with st.expander("**Status Legend**"):
        st.markdown("""
        | Status | Match Rate | Description |
        |--------|------------|-------------|
        | OK Matched | >=99% | Raw and DM counts are nearly identical |
        | (!) Minor Gap | 90-99% | Small discrepancy, may be acceptable |
        | (!) Significant Gap | 50-90% | Notable data loss in pipeline |
        | X Major Gap | <50% | Severe data loss, investigate immediately |
        | ? View Not Found | - | DM view does not exist in the specified space |
        | (!) Raw Error | - | Error accessing Raw table (permissions or missing) |
        """)
    
    st.success("Staging vs DM comparison loaded from pre-computed metrics.")
