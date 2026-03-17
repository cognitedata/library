# -*- coding: utf-8 -*-
"""
Asset Hierarchy Quality Dashboard.
"""

import streamlit as st
import pandas as pd
import plotly.express as px

from .common import (
    get_status_color_hierarchy,
    metric_card,
    gauge,
)
from .ai_summary import (
    render_ai_summary_section,
    get_hierarchy_prompt,
    format_hierarchy_metrics,
)


def render_asset_hierarchy_dashboard(metrics: dict):
    """Render the Asset Hierarchy Quality dashboard tab."""
    st.title("Asset Hierarchy Quality Dashboard")
    
    hierarchy = metrics.get("hierarchy_metrics", {})
    metadata = metrics.get("metadata", {})
    
    if not hierarchy:
        st.warning("No hierarchy metrics found in the data file.")
        return
    
    # Show data info
    computed_at = metadata.get("computed_at", "Unknown")
    st.info(f"[Date] Metrics computed at: {computed_at}")
    
    # Understanding the metrics
    with st.expander("**Understanding the Metrics** - Click to learn more", expanded=False):
        st.markdown("""
        **Hierarchy Structure Metrics:**
        - **Hierarchy Completion** - % of non-root assets with a valid parent link (should be ~100%)
        - **Root Assets** - Assets at the top level with no parent (entry points to your hierarchy)
        - **Orphan Assets** - Assets with no parent AND no children (disconnected from hierarchy)
        - **Orphan Rate** - % of total assets that are orphans
        
        **Depth Metrics:**
        - **Average Depth** - Mean number of levels from root to each asset
        - **Max Depth** - Deepest level in the hierarchy (very deep may indicate data issues)
        - **Depth Distribution** - How many assets exist at each level
        
        **Breadth Metrics:**
        - **Average Children** - Mean number of direct children per parent asset
        - **Max Children** - Largest number of children under a single parent (very high may need review)
        - **Breadth Distribution** - How many parents have X number of children
        
        *Tip: A healthy hierarchy has high completion rate, reasonable depth (usually <10 levels), and balanced breadth.*
        """)
    
    # Extract metrics
    total_assets = hierarchy.get("hierarchy_total_assets", 0)
    root_assets = hierarchy.get("hierarchy_root_assets", 0)
    orphan_count = hierarchy.get("hierarchy_orphan_count", 0)
    orphan_rate = hierarchy.get("hierarchy_orphan_rate", 0)
    hierarchy_completion_rate = hierarchy.get("hierarchy_completion_rate", 0)
    avg_depth = hierarchy.get("hierarchy_avg_depth", 0)
    max_depth = hierarchy.get("hierarchy_max_depth", 0)
    avg_children = hierarchy.get("hierarchy_avg_children", 0)
    std_children = hierarchy.get("hierarchy_std_children", 0)
    max_children = hierarchy.get("hierarchy_max_children", 0)
    depth_distribution = hierarchy.get("hierarchy_depth_distribution", {})
    breadth_distribution = hierarchy.get("hierarchy_breadth_distribution", {})
    
    st.markdown("---")
    
    # =====================================================
    # MAIN METRIC - BIG ON TOP
    # =====================================================
    st.header("Hierarchy Completion")
    
    col_main, col_info = st.columns([2, 1])
    
    with col_main:
        gauge(col_main, "Parent Link Completion", hierarchy_completion_rate, "completion", 
              get_status_color_hierarchy, [0, 100], "%", key="h_completion_main",
              help_text="% of non-root assets that have a valid parent link")
    
    with col_info:
        st.markdown("### Why This Matters")
        st.markdown("""
        Assets without parent links break the hierarchy structure.
        A complete hierarchy enables:
        - Navigation from root to leaf assets
        - Rollup calculations and aggregations
        - Proper asset context and location
        """)
        
        non_root = total_assets - root_assets
        incomplete = non_root - int(non_root * hierarchy_completion_rate / 100) if non_root > 0 else 0
        
        if hierarchy_completion_rate >= 95:
            st.success(f"Excellent hierarchy structure!")
        elif hierarchy_completion_rate >= 80:
            st.warning(f"~{incomplete:,} assets may have missing parent links.")
        else:
            st.error(f"~{incomplete:,} assets need parent links.")
    
    st.markdown("---")
    
    # METRIC CARDS
    col1, col2, col3, col4 = st.columns(4)
    metric_card(col1, "Total Assets", f"{total_assets:,}",
                help_text="Total number of unique assets in the hierarchy")
    metric_card(col2, "Root Assets", f"{root_assets:,}",
                help_text="Assets at the top level with no parent (hierarchy entry points)")
    metric_card(col3, "Orphans", f"{orphan_count:,}", metric_key="orphans", 
                color_func=get_status_color_hierarchy, raw_value=orphan_count,
                help_text="Assets with no parent AND no children (disconnected from hierarchy)")
    metric_card(col4, "Orphan Rate", f"{orphan_rate:.2f}", suffix="%", 
                metric_key="orphans", color_func=get_status_color_hierarchy, raw_value=orphan_rate,
                help_text="Percentage of assets that are orphans")
    
    st.markdown("---")
    
    # GAUGE SECTION
    st.subheader("Additional Structure Indicators")
    max_depth_range = max(max_depth, 10)
    g1, g2, g3 = st.columns(3)
    
    gauge(g1, "Average Depth", avg_depth, "depth", 
          get_status_color_hierarchy, [0, max_depth_range], "", key="h_avg_depth",
          help_text="Mean number of levels from root to each asset")
    gauge(g2, "Max Depth", max_depth, "depth", 
          get_status_color_hierarchy, [0, max_depth_range], "", key="h_max_depth",
          help_text="Deepest level in the hierarchy (too deep may indicate issues)")
    gauge(g3, "Orphan Rate", orphan_rate, "orphans", 
          get_status_color_hierarchy, [0, 100], "%", key="h_orphan_rate",
          help_text="% of assets that are disconnected (no parent, no children)")
    
    st.markdown("---")
    
    # Additional Stats
    st.subheader("Breadth Statistics")
    c1, c2, c3 = st.columns(3)
    c1.metric("Average Children per Parent", f"{avg_children:.2f}",
              help="Mean number of direct children per parent asset")
    c2.metric("Std Dev Children", f"{std_children:.2f}",
              help="Standard deviation of children count (high = uneven distribution)")
    c3.metric("Max Children", f"{max_children:,}",
              help="Maximum children under a single parent")
    
    st.markdown("---")
    
    # DISTRIBUTIONS
    st.subheader("Depth Distribution")
    st.markdown("<span style='color:#666;font-size:13px;'>(i) Shows how many assets exist at each level of the hierarchy. Level 0 = root assets, Level 1 = direct children of roots, etc.</span>", unsafe_allow_html=True)
    if depth_distribution:
        # Convert string keys to int for proper sorting
        depth_dist_clean = {int(k): v for k, v in depth_distribution.items()}
        depth_df = pd.DataFrame(list(depth_dist_clean.items()), columns=["Depth", "Count"])
        depth_df = depth_df.sort_values("Depth")
        fig_depth = px.bar(depth_df, x="Depth", y="Count", text="Count")
        fig_depth.update_traces(textposition='outside')
        st.plotly_chart(fig_depth, use_container_width=True)
    else:
        st.info("No depth distribution data available.")
    
    st.subheader("Breadth Distribution (Children per Parent)")
    st.markdown("<span style='color:#666;font-size:13px;'>(i) Shows how many parent assets have X number of children. Helps identify imbalanced hierarchies (e.g., one parent with 1000+ children).</span>", unsafe_allow_html=True)
    if breadth_distribution:
        # Convert and limit to top 20 for readability
        breadth_dist_clean = {int(k): v for k, v in breadth_distribution.items()}
        breadth_df = pd.DataFrame(list(breadth_dist_clean.items()), columns=["Children Count", "Number of Parents"])
        breadth_df = breadth_df.sort_values("Children Count").head(30)
        fig_breadth = px.bar(breadth_df, x="Children Count", y="Number of Parents", text="Number of Parents")
        fig_breadth.update_traces(textposition='outside')
        st.plotly_chart(fig_breadth, use_container_width=True)
    else:
        st.info("No breadth distribution data available.")
    
    # AI SUMMARY SECTION
    render_ai_summary_section(
        dashboard_type="Asset Hierarchy Quality",
        metrics_data=format_hierarchy_metrics(hierarchy),
        system_prompt=get_hierarchy_prompt(),
        key_prefix="hierarchy"
    )
    
    st.markdown("---")
    st.success("Asset Hierarchy dashboard loaded from pre-computed metrics.")
