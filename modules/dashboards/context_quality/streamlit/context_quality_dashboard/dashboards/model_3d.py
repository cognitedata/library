# -*- coding: utf-8 -*-
"""
3D Model Contextualization Dashboard.

Displays metrics for 3D model associations:
- 3D Object Contextualization (3D -> Asset) - PRIMARY
- Asset 3D Coverage (Asset -> 3D)
- Critical Asset 3D Association
- Bounding Box Completeness

Features creative visualizations since there are only a few primary metrics.
"""

import streamlit as st
import plotly.graph_objects as go
from typing import Optional

from .ai_summary import (
    render_ai_summary_section,
    get_3d_model_prompt,
    format_3d_model_metrics,
)
from .reports import generate_3d_model_report


def _create_3d_cube_indicator(
    value: float,
    title: str,
    target: float = 70.0,
    subtitle: str = ""
) -> go.Figure:
    """
    Create a creative 3D-themed indicator using a stylized cube representation.
    Uses color fills to show progress.
    """
    # Determine color based on value vs target (using standard dashboard colors)
    if value >= target:
        color = "#4CAF50"  # Green (Material Design)
        status = "[OK] Target Met"
    elif value >= target * 0.7:
        color = "#FFC107"  # Yellow/Amber (Material Design)
        status = "[~] Approaching"
    else:
        color = "#F44336"  # Red (Material Design)
        status = "[X] Below Target"
    
    fig = go.Figure()
    
    # Create a modern card-style indicator with gradient effect
    fig.add_trace(go.Indicator(
        mode="gauge+number",
        value=value,
        title={
            "text": f"<b>{title}</b><br><span style='font-size:12px;color:gray'>{subtitle}</span>",
            "font": {"size": 16}
        },
        number={"suffix": "%", "font": {"size": 40, "color": color}},
        gauge={
            "axis": {"range": [0, 100], "tickwidth": 1, "tickcolor": "#ddd"},
            "bar": {"color": color, "thickness": 0.7},
            "bgcolor": "white",
            "borderwidth": 2,
            "bordercolor": "#eee",
            "steps": [
                {"range": [0, target * 0.7], "color": "#FFEBEE"},  # Light red
                {"range": [target * 0.7, target], "color": "#FFF8E1"},  # Light yellow
                {"range": [target, 100], "color": "#E8F5E9"},  # Light green
            ],
            "threshold": {
                "line": {"color": "#333", "width": 3},
                "thickness": 0.8,
                "value": target
            }
        }
    ))
    
    # Add target annotation
    fig.add_annotation(
        x=0.5, y=-0.15,
        text=f"Target: {target}% | {status}",
        showarrow=False,
        font={"size": 11, "color": "gray"},
        xref="paper", yref="paper"
    )
    
    fig.update_layout(
        height=280,
        margin={"t": 80, "b": 50, "l": 30, "r": 30},
        paper_bgcolor="rgba(0,0,0,0)",
        font={"family": "Arial, sans-serif"}
    )
    
    return fig


def _create_bbox_donut(
    complete: int,
    partial: int,
    none: int,
    total: int
) -> go.Figure:
    """Create a donut chart for bounding box distribution."""
    
    labels = ["Complete", "Partial", "Missing"]
    values = [complete, partial, none]
    colors = ["#4CAF50", "#FFC107", "#F44336"]  # Standard dashboard colors
    
    fig = go.Figure(data=[go.Pie(
        labels=labels,
        values=values,
        hole=0.6,
        marker={"colors": colors, "line": {"color": "#fff", "width": 2}},
        textinfo="percent",
        textfont={"size": 14},
        hovertemplate="<b>%{label}</b><br>%{value:,} objects<br>%{percent}<extra></extra>"
    )])
    
    # Add center annotation
    fig.add_annotation(
        text=f"<b>{total:,}</b><br><span style='font-size:12px'>Total</span>",
        x=0.5, y=0.5,
        font={"size": 20},
        showarrow=False
    )
    
    fig.update_layout(
        title={"text": "Bounding Box Distribution", "font": {"size": 16}},
        height=300,
        margin={"t": 60, "b": 30, "l": 30, "r": 30},
        showlegend=True,
        legend={"orientation": "h", "y": -0.1, "x": 0.5, "xanchor": "center"}
    )
    
    return fig


def _create_model_type_bars(
    cad_count: int,
    img360_count: int,
    pointcloud_count: int,
    multi_count: int,
    total: int
) -> go.Figure:
    """Create horizontal bar chart for model type distribution."""
    
    categories = ["CAD Models", "360 Images", "Point Clouds", "Multi-Model"]
    values = [cad_count, img360_count, pointcloud_count, multi_count]
    colors = ["#3498db", "#9b59b6", "#1abc9c", "#e67e22"]
    
    # Calculate percentages
    pcts = [(v / total * 100) if total > 0 else 0 for v in values]
    
    fig = go.Figure()
    
    for i, (cat, val, pct, color) in enumerate(zip(categories, values, pcts, colors)):
        fig.add_trace(go.Bar(
            y=[cat],
            x=[pct],
            orientation="h",
            marker={"color": color, "line": {"width": 0}},
            text=f"{val:,} ({pct:.1f}%)",
            textposition="auto",
            textfont={"color": "white", "size": 12},
            hovertemplate=f"<b>{cat}</b><br>{val:,} objects ({pct:.1f}%)<extra></extra>",
            showlegend=False
        ))
    
    fig.update_layout(
        title={"text": "3D Model Type Coverage", "font": {"size": 16}},
        xaxis={"title": "Percentage of 3D Objects", "range": [0, 100]},
        yaxis={"title": ""},
        height=250,
        margin={"t": 60, "b": 50, "l": 120, "r": 30},
        bargap=0.3
    )
    
    return fig


def _create_coverage_comparison(
    contextualization_rate: float,
    asset_coverage: float,
    critical_coverage: float,
    bbox_completeness: float
) -> go.Figure:
    """Create a radar/spider chart comparing the 4 main metrics."""
    
    categories = ["3D->Asset<br>Contextualization", "Asset->3D<br>Coverage", "Critical Asset<br>3D Rate", "BBox<br>Completeness"]
    values = [contextualization_rate, asset_coverage, critical_coverage, bbox_completeness]
    
    # Close the polygon
    categories_closed = categories + [categories[0]]
    values_closed = values + [values[0]]
    
    fig = go.Figure()
    
    # Add the metric values
    fig.add_trace(go.Scatterpolar(
        r=values_closed,
        theta=categories_closed,
        fill="toself",
        fillcolor="rgba(52, 152, 219, 0.3)",
        line={"color": "#3498db", "width": 3},
        marker={"size": 10, "color": "#3498db"},
        name="Current",
        hovertemplate="<b>%{theta}</b><br>%{r:.1f}%<extra></extra>"
    ))
    
    # Add target reference
    target_values = [90, 70, 100, 90, 90]  # Different targets per metric (closed)
    fig.add_trace(go.Scatterpolar(
        r=target_values,
        theta=categories_closed,
        fill="none",
        line={"color": "#F44336", "width": 2, "dash": "dash"},
        marker={"size": 0},
        name="Target",
        hovertemplate="Target: %{r}%<extra></extra>"
    ))
    
    fig.update_layout(
        polar={
            "radialaxis": {
                "visible": True,
                "range": [0, 100],
                "ticksuffix": "%"
            }
        },
        title={"text": "3D Contextualization Overview", "font": {"size": 16}},
        height=380,
        margin={"t": 80, "b": 60, "l": 80, "r": 80},
        showlegend=True,
        legend={"orientation": "h", "y": -0.25, "x": 0.5, "xanchor": "center"}
    )
    
    return fig


def render_3d_model_dashboard(metrics: dict, metadata: Optional[dict] = None):
    """
    Render the 3D Model Contextualization dashboard.
    
    Args:
        metrics: The model3d_metrics dict from the function output
        metadata: Optional metadata dict
    """
    st.header("3D Model Contextualization")
    
    if not metrics:
        st.warning("""
        **No 3D metrics available**
        
        This could mean:
        - 3D metrics are disabled in configuration
        - No `Cognite3DObject` view data is available
        - The function hasn't been run yet
        
        Enable 3D metrics in the **Configure & Run** tab and ensure the 3D Object view is configured.
        """)
        return
    
    # Understanding the metrics
    with st.expander("**Understanding the Metrics** - Click to learn more", expanded=False):
        st.markdown("""
        **Contextualization Metrics:**
        - **3D → Asset Contextualization** - % of 3D objects linked to assets (should be high)
        - **Asset → 3D Coverage** - % of assets with 3D representations (informational - not all assets need 3D)
        - **Critical Asset 3D Rate** - % of critical assets with 3D links (should be 100%)
        
        **Bounding Box Metrics:**
        - **BBox Completeness** - % of 3D objects with complete spatial definitions
        - **Complete** - Objects with full bounding box data
        - **Partial** - Objects with incomplete spatial data
        - **Missing** - Objects without any bounding box
        
        **Model Type Distribution:**
        - **CAD Models** - Objects from CAD model sources
        - **360 Images** - Objects from 360° image captures
        - **Point Clouds** - Objects from laser scanning/LiDAR
        - **Multi-Model** - Objects appearing in multiple 3D sources
        
        *Tip: Focus on 3D → Asset Contextualization first - unlinked 3D objects cannot be navigated from the asset tree.*
        """)
    
    # Download Report Button
    st.download_button(
        label="Download 3D Model Report (PDF)",
        data=generate_3d_model_report(metrics),
        file_name="3d_model_report.pdf",
        mime="application/pdf",
        use_container_width=True,
        type="primary",
        key="download_3d_model_report"
    )
    
    # Extract metrics
    # PRIMARY METRIC: 3D -> Asset Contextualization
    contextualization_rate = metrics.get("model3d_contextualization_rate", 0)
    objects_with_asset = metrics.get("model3d_objects_with_asset", 0)
    
    # Secondary metrics
    asset_coverage = metrics.get("model3d_asset_coverage", 0)
    critical_rate = metrics.get("model3d_critical_asset_rate", 0)
    bbox_completeness = metrics.get("model3d_bbox_completeness", 0)
    
    total_assets = metrics.get("model3d_total_assets", 0)
    assets_with_3d = metrics.get("model3d_assets_with_3d", 0)
    critical_total = metrics.get("model3d_critical_total", 0)
    critical_with_3d = metrics.get("model3d_critical_with_3d", 0)
    total_objects = metrics.get("model3d_total_objects", 0)
    
    # Bounding box distribution
    bbox_complete = metrics.get("model3d_bbox_complete_count", 0)
    bbox_partial = metrics.get("model3d_bbox_partial_count", 0)
    bbox_none = metrics.get("model3d_bbox_none_count", 0)
    
    # Model type distribution
    cad_count = metrics.get("model3d_cad_count", 0)
    img360_count = metrics.get("model3d_360_count", 0)
    pointcloud_count = metrics.get("model3d_pointcloud_count", 0)
    multi_count = metrics.get("model3d_multi_model_count", 0)
    
    # ===== HERO SECTION =====
    st.subheader("3D Object Contextualization")
    st.caption("Shows what percentage of 3D objects are linked to assets - the key indicator of 3D contextualization quality.")
    
    # Large hero gauge for the primary metric
    col_hero, col_overview = st.columns([2, 1])
    
    with col_hero:
        st.plotly_chart(
            _create_3d_cube_indicator(
                contextualization_rate,
                "3D -> Asset Contextualization",
                target=90.0,
                subtitle=f"{objects_with_asset:,} / {total_objects:,} 3D objects linked to assets"
            ),
            use_container_width=True
        )
    
    with col_overview:
        # Quick stats
        st.metric("Total 3D Objects", f"{total_objects:,}")
        st.metric("Linked to Assets", f"{objects_with_asset:,}")
        unlinked = total_objects - objects_with_asset
        if total_objects == 0:
            st.info("No 3D objects found in the configured view")
        elif unlinked > 0:
            st.metric("Not Linked", f"{unlinked:,}", delta=f"-{unlinked:,}", delta_color="inverse")
        else:
            st.success("All 3D objects are contextualized!")
    
    # ===== OVERVIEW RADAR =====
    st.markdown("---")
    st.plotly_chart(
        _create_coverage_comparison(contextualization_rate, asset_coverage, critical_rate, bbox_completeness),
        use_container_width=True
    )
    
    # ===== ADDITIONAL METRICS ROW =====
    st.markdown("---")
    st.subheader("Additional Metrics")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.plotly_chart(
            _create_3d_cube_indicator(
                asset_coverage,
                "Asset -> 3D Coverage",
                target=70.0,
                subtitle=f"{assets_with_3d:,} / {total_assets:,} assets have 3D"
            ),
            use_container_width=True
        )
    
    with col2:
        st.plotly_chart(
            _create_3d_cube_indicator(
                critical_rate,
                "Critical Asset 3D",
                target=100.0,
                subtitle=f"{critical_with_3d:,} / {critical_total:,} critical"
            ),
            use_container_width=True
        )
    
    with col3:
        st.plotly_chart(
            _create_3d_cube_indicator(
                bbox_completeness,
                "BBox Completeness",
                target=90.0,
                subtitle=f"{bbox_complete:,} / {total_objects:,} objects"
            ),
            use_container_width=True
        )
    
    # ===== DISTRIBUTION SECTION =====
    st.markdown("---")
    st.subheader("[Chart] Distribution Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if total_objects > 0:
            st.plotly_chart(
                _create_bbox_donut(bbox_complete, bbox_partial, bbox_none, total_objects),
                use_container_width=True
            )
        else:
            st.info("No 3D objects to analyze")
    
    with col2:
        if total_objects > 0:
            st.plotly_chart(
                _create_model_type_bars(
                    cad_count, img360_count, pointcloud_count, multi_count, total_objects
                ),
                use_container_width=True
            )
        else:
            st.info("No model type data available")
    
    # ===== INSIGHTS SECTION =====
    st.markdown("---")
    st.subheader("Insights")
    
    insights = []
    
    # Handle case when no 3D objects exist
    if total_objects == 0:
        insights.append(("[i]", "**No 3D objects found** - Configure the Cognite3DObject view in the Configuration tab to see 3D metrics."))
    else:
        # PRIMARY: 3D -> Asset Contextualization insight (MOST IMPORTANT)
        if contextualization_rate >= 90:
            insights.append(("[OK]", f"**Excellent 3D contextualization** - {contextualization_rate}% of 3D objects are linked to assets ({objects_with_asset:,} / {total_objects:,})"))
        elif contextualization_rate >= 70:
            insights.append(("[!]", f"**Good 3D contextualization** - {contextualization_rate}% of 3D objects are linked. {total_objects - objects_with_asset:,} objects still need linking."))
        elif contextualization_rate >= 50:
            insights.append(("[!]", f"**Moderate 3D contextualization** - {contextualization_rate}% of 3D objects are linked. Consider reviewing unlinked objects."))
        else:
            insights.append(("[X]", f"**Low 3D contextualization** - Only {contextualization_rate}% of 3D objects are linked to assets. This is a priority area for improvement."))
        
        # Bounding box insight
        if bbox_completeness >= 90:
            insights.append(("[OK]", f"**Excellent bounding boxes** - {bbox_completeness}% of 3D objects have complete spatial definitions"))
        elif bbox_none > bbox_complete:
            insights.append(("[X]", f"**Missing bounding boxes** - {bbox_none:,} objects lack spatial definitions"))
        elif bbox_partial > 0:
            insights.append(("[!]", f"**Partial bounding boxes** - {bbox_partial:,} objects have incomplete spatial data"))
        
        # Multi-model insight
        if multi_count > 0:
            insights.append(("[+]", f"**Multi-model coverage** - {multi_count:,} objects appear in multiple 3D sources (CAD, 360, Point Cloud)"))
    
    # Asset 3D coverage insight (always show if assets exist)
    if total_assets > 0:
        if asset_coverage >= 70:
            insights.append(("[OK]", f"**Good 3D coverage** - {asset_coverage}% of assets have 3D representations"))
        elif asset_coverage >= 40:
            insights.append(("[!]", f"**Moderate 3D coverage** - {asset_coverage}% of assets have 3D representations. Consider prioritizing critical assets."))
        elif asset_coverage > 0:
            insights.append(("[!]", f"**Low 3D coverage** - Only {asset_coverage}% of assets have 3D links."))
    
    # Critical asset insight
    if critical_total > 0:
        if critical_rate == 100:
            insights.append(("[OK]", f"**All critical assets** have 3D representations ({critical_with_3d:,} assets)"))
        elif critical_rate >= 80:
            insights.append(("[!]", f"**Most critical assets** have 3D ({critical_rate}%). {critical_total - critical_with_3d:,} still need linking."))
        else:
            insights.append(("[X]", f"**Critical asset gap** - Only {critical_rate}% of critical assets have 3D. This is a priority area."))
    
    for icon, text in insights:
        st.markdown(f"{icon} {text}")
    
    # ===== AI INSIGHTS SECTION =====
    st.markdown("---")
    render_ai_summary_section(
        dashboard_type="3D Model Contextualization",
        metrics_data=format_3d_model_metrics(metrics),
        system_prompt=get_3d_model_prompt(),
        key_prefix="model3d"
    )
    
    # ===== STATS FOOTER =====
    st.markdown("---")
    
    with st.expander("Detailed Statistics", expanded=False):
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("**Asset Statistics**")
            st.write(f"- Total assets checked: {total_assets:,}")
            st.write(f"- Assets with 3D: {assets_with_3d:,}")
            st.write(f"- Coverage rate: {asset_coverage}%")
        
        with col2:
            st.markdown("**Critical Assets**")
            st.write(f"- Total critical: {critical_total:,}")
            st.write(f"- With 3D: {critical_with_3d:,}")
            st.write(f"- Association rate: {critical_rate}%")
        
        with col3:
            st.markdown("**3D Objects**")
            st.write(f"- Total objects: {total_objects:,}")
            st.write(f"- **Linked to assets: {objects_with_asset:,}**")
            st.write(f"- Contextualization rate: **{contextualization_rate}%**")
            st.write(f"- Complete bbox: {bbox_complete:,}")
            st.write(f"- Partial bbox: {bbox_partial:,}")
            st.write(f"- No bbox: {bbox_none:,}")
