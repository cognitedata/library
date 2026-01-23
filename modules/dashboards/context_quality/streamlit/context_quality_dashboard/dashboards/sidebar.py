"""
Sidebar metadata display component.
"""

import streamlit as st
from datetime import datetime


def render_metadata_sidebar(metrics: dict):
    """Display metadata in the sidebar."""
    metadata = metrics.get("metadata", {})
    
    st.sidebar.title("📋 Metrics Info")
    
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
    st.sidebar.markdown("### 📊 Instance Counts")
    instance_counts = metadata.get("instance_counts", {})
    
    # Assets
    asset_counts = instance_counts.get("assets", {})
    asset_total = asset_counts.get("total_instances", 0)
    asset_unique = asset_counts.get("unique", 0)
    asset_dups = asset_counts.get("duplicates", 0)
    
    st.sidebar.markdown("**Assets:**")
    st.sidebar.markdown(f"- Total Instances: {asset_total:,}")
    st.sidebar.markdown(f"- Unique: {asset_unique:,}")
    if asset_dups > 0:
        st.sidebar.markdown(f"- ⚠️ Duplicates: {asset_dups:,}")
    else:
        st.sidebar.markdown(f"- Duplicates: {asset_dups:,}")
    
    # Equipment
    eq_counts = instance_counts.get("equipment", {})
    eq_total = eq_counts.get("total_instances", 0)
    eq_unique = eq_counts.get("unique", 0)
    eq_dups = eq_counts.get("duplicates", 0)
    
    st.sidebar.markdown("**Equipment:**")
    st.sidebar.markdown(f"- Total Instances: {eq_total:,}")
    st.sidebar.markdown(f"- Unique: {eq_unique:,}")
    if eq_dups > 0:
        st.sidebar.markdown(f"- ⚠️ Duplicates: {eq_dups:,}")
    else:
        st.sidebar.markdown(f"- Duplicates: {eq_dups:,}")
    
    # Time Series
    ts_counts = instance_counts.get("timeseries", {})
    ts_total = ts_counts.get("total_instances", 0)
    ts_unique = ts_counts.get("unique", 0)
    ts_dups = ts_counts.get("duplicates", 0)
    
    st.sidebar.markdown("**Time Series:**")
    st.sidebar.markdown(f"- Total Instances: {ts_total:,}")
    st.sidebar.markdown(f"- Unique: {ts_unique:,}")
    if ts_dups > 0:
        st.sidebar.markdown(f"- ⚠️ Duplicates: {ts_dups:,}")
    else:
        st.sidebar.markdown(f"- Duplicates: {ts_dups:,}")
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### Batches Processed")
    batches = metadata.get("batches_processed", {})
    st.sidebar.markdown(f"- Assets: {batches.get('assets', 0):,}")
    st.sidebar.markdown(f"- Equipment: {batches.get('equipment', 0):,}")
    st.sidebar.markdown(f"- Time Series: {batches.get('ts', 0):,}")
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📊 Processing Coverage")
    config = metadata.get("config", {})
    limits = metadata.get("limits_reached", {})
    
    # Get actual processed counts
    ts_processed = ts_unique
    asset_processed = asset_unique
    eq_processed = eq_unique
    
    ts_limit = config.get("max_timeseries", 150000)
    asset_limit = config.get("max_assets", 150000)
    eq_limit = config.get("max_equipment", 150000)
    
    ts_limit_reached = limits.get("timeseries", False)
    asset_limit_reached = limits.get("assets", False)
    eq_limit_reached = limits.get("equipment", False)
    
    any_limit_reached = ts_limit_reached or asset_limit_reached or eq_limit_reached
    
    # Assets
    if asset_limit_reached:
        st.sidebar.markdown(f"**Assets:** {asset_processed:,} of {asset_limit:,} limit ⚠️ LIMIT REACHED")
    else:
        st.sidebar.markdown(f"**Assets:** {asset_processed:,} ✅ All processed")
    
    # Equipment
    if eq_limit_reached:
        st.sidebar.markdown(f"**Equipment:** {eq_processed:,} of {eq_limit:,} limit ⚠️ LIMIT REACHED")
    else:
        st.sidebar.markdown(f"**Equipment:** {eq_processed:,} ✅ All processed")
    
    # Time Series
    if ts_limit_reached:
        st.sidebar.markdown(f"**Time Series:** {ts_processed:,} of {ts_limit:,} limit ⚠️ LIMIT REACHED")
    else:
        st.sidebar.markdown(f"**Time Series:** {ts_processed:,} ✅ All processed")
    
    # Warning if any limit was reached
    if any_limit_reached:
        st.sidebar.markdown("---")
        st.sidebar.warning("""
        ⚠️ **Partial Data Warning**
        
        One or more processing limits were reached. The metrics shown represent **trends only** and do not reflect the complete dataset.
        
        To analyze all data, increase the limits in the function configuration. 

        Note: Higher limits may cause function timeouts (max 10 min).
        """)
    
    st.sidebar.markdown("---")
    if st.sidebar.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()
