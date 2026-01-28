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
    st.subheader("Key Structural Quality Indicators")
    g1, g2, g3 = st.columns(3)
    max_depth_range = max(max_depth, 10)
    
    gauge(g1, "Hierarchy Completion", hierarchy_completion_rate, "completion", 
          get_status_color_hierarchy, [0, 100], "%", key="h_completion",
          help_text="% of non-root assets that have a valid parent link")
    gauge(g2, "Average Depth", avg_depth, "depth", 
          get_status_color_hierarchy, [0, max_depth_range], "", key="h_avg_depth",
          help_text="Mean number of levels from root to each asset")
    gauge(g3, "Max Depth", max_depth, "depth", 
          get_status_color_hierarchy, [0, max_depth_range], "", key="h_max_depth",
          help_text="Deepest level in the hierarchy (too deep may indicate issues)")
    
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
