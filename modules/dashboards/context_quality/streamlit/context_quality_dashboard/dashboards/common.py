"""
Common UI components and color functions for the dashboard.
"""

import streamlit as st
import plotly.graph_objects as go


# ----------------------------------------------------
# COLOR FUNCTIONS
# ----------------------------------------------------

def get_status_color_hierarchy(metric_name, value):
    if metric_name == "completion":
        if value >= 98: return "#4CAF50"
        elif value >= 95: return "#FFC107"
        else: return "#F44336"
    if metric_name == "orphans":
        if value == 0: return "#4CAF50"
        elif value <= 5: return "#FFC107"
        else: return "#F44336"
    if metric_name == "depth":
        if value <= 6: return "#4CAF50"
        elif value <= 8: return "#FFC107"
        else: return "#F44336"
    return "#0068C9"


def get_status_color_ts(metric_key, value):
    """Color function for time series metrics.
    
    Direction considerations:
    - ts_to_asset: CRITICAL - orphaned TS are a problem
    - asset_monitoring: INFORMATIONAL - not all assets need TS monitoring
    - critical_coverage: CRITICAL - should be 100%
    """
    if metric_key == "ts_to_asset":
        # CRITICAL: TS should be linked to assets (orphaned TS are a problem)
        if value >= 95: return "#4CAF50"
        if value >= 90: return "#FFC107"
        return "#F44336"
    if metric_key == "asset_monitoring":
        # INFORMATIONAL: Not all assets need TS monitoring - low values are OK
        return "#0068C9"  # Blue - informational
    if metric_key == "critical_coverage":
        # CRITICAL: Should be 100%
        if value == 100: return "#4CAF50"
        if value >= 95: return "#FFC107"
        return "#F44336"
    if metric_key == "unit_consistency":
        if value > 95: return "#4CAF50"
        if value >= 90: return "#FFC107"
        return "#F44336"
    if metric_key == "gap":
        if value >= 95: return "#4CAF50"  # Higher is better (% without gaps)
        if value >= 85: return "#FFC107"
        return "#F44336"
    if metric_key == "freshness":
        if value >= 90: return "#4CAF50"
        if value >= 70: return "#FFC107"
        return "#F44336"
    return "#0068C9"


def get_status_color_equipment(metric_key, value):
    """Color function for equipment metrics.
    
    Direction considerations:
    - association: CRITICAL - equipment should be linked to assets
    - coverage: INFORMATIONAL - not all assets have equipment attached
    - critical: CRITICAL - should be 100%
    """
    if metric_key == "association":
        # CRITICAL: Equipment should be linked to assets
        if value >= 90: return "#4CAF50"
        if value >= 70: return "#FFC107"
        return "#F44336"
    if metric_key == "coverage":
        # INFORMATIONAL: Not all assets have equipment - low values are OK
        return "#0068C9"  # Blue - informational
    if metric_key == "serial":
        if value >= 90: return "#4CAF50"
        if value >= 70: return "#FFC107"
        return "#F44336"
    if metric_key == "manufacturer":
        if value >= 80: return "#4CAF50"
        if value >= 60: return "#FFC107"
        return "#F44336"
    if metric_key == "type_consistency":
        if value >= 95: return "#4CAF50"
        if value >= 85: return "#FFC107"
        return "#F44336"
    if metric_key == "critical":
        # CRITICAL: Should be 100%
        if value == 100: return "#4CAF50"
        if value >= 90: return "#FFC107"
        return "#F44336"
    return "#0068C9"


def get_status_color_maintenance(metric_key, value):
    """Color function for maintenance workflow metrics.
    
    Important direction considerations (per Jan Inge's guidance):
    - notif_order: INFORMATIONAL - low values are OK (not all notifs need WO)
    - order_notif: CRITICAL - should be ~100% (all WOs should have a notification)
    - order_asset: CRITICAL - should be ~100% (all WOs should link to an asset)
    - asset/equipment maintenance coverage: INFORMATIONAL - low values OK
    """
    
    # INFORMATIONAL: Notification → Work Order (low values are acceptable)
    if metric_key == "notif_order":
        # Any value is OK - this is just informational
        return "#0068C9"  # Blue - informational
    
    # CRITICAL: Work Order → Notification (should be ~100%)
    if metric_key == "order_notif":
        if value >= 95: return "#4CAF50"
        if value >= 80: return "#FFC107"
        return "#F44336"
    
    # CRITICAL: Work Order → Asset (should be ~100%)
    if metric_key == "order_asset":
        if value >= 95: return "#4CAF50"
        if value >= 80: return "#FFC107"
        return "#F44336"
    
    # Standard metrics (moderate thresholds)
    if metric_key in ["notif_asset", "notif_equipment", "order_equipment"]:
        if value >= 80: return "#4CAF50"
        if value >= 60: return "#FFC107"
        return "#F44336"
    
    if metric_key == "order_completion":
        if value >= 80: return "#4CAF50"
        if value >= 60: return "#FFC107"
        return "#F44336"
    
    # Failure documentation metrics
    if metric_key in ["failure_mode", "failure_mechanism", "failure_cause"]:
        if value >= 80: return "#4CAF50"
        if value >= 50: return "#FFC107"
        return "#F44336"
    
    # INFORMATIONAL: Maintenance coverage (low values are acceptable)
    if metric_key in ["asset_maint_coverage", "equipment_maint_coverage"]:
        # Any value is OK - this is just informational
        return "#0068C9"  # Blue - informational
    
    return "#0068C9"


# ----------------------------------------------------
# UI COMPONENTS
# ----------------------------------------------------

def metric_card(col, title, value, metric_key=None, suffix="", color_func=get_status_color_hierarchy, raw_value=None, help_text=None):
    """
    Display a styled metric card with optional help tooltip.
    
    Args:
        col: Streamlit column to render in
        title: Card title
        value: Display value (can be formatted string)
        metric_key: Key for color function lookup
        suffix: Suffix to append to value (e.g., "%")
        color_func: Function to determine color based on metric
        raw_value: Raw numeric value for color calculation (if different from display value)
        help_text: Optional tooltip text shown on hover over question mark
    """
    # Use raw_value for color calculation if provided, otherwise try to use value
    color_value = raw_value if raw_value is not None else value
    color = color_func(metric_key, color_value) if metric_key else "#0068C9"
    
    # Build title with optional help icon
    help_icon = ""
    if help_text:
        # Escape quotes in help text for HTML attribute
        escaped_help = help_text.replace('"', '&quot;').replace("'", "&#39;")
        help_icon = f"""<span title="{escaped_help}" style='cursor:help;margin-left:4px;color:#888;font-size:12px;'>ⓘ</span>"""
    
    col.markdown(f"""
    <div style='background:#F7F9FB;padding:14px;border-radius:12px;box-shadow:0 1px 4px rgba(0,0,0,0.08);border-left:6px solid {color};text-align:center;'>
        <div style='font-size:14px;color:#666;'>{title}{help_icon}</div>
        <div style='font-size:28px;font-weight:600;color:{color};'>{value}{suffix}</div>
    </div>
    """, unsafe_allow_html=True)


def gauge(col, title, value, metric_key, color_func, axis_range, suffix="%", key=None, help_text=None):
    """Display a gauge with optional help text."""
    color = color_func(metric_key, value)
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=value,
        title={'text': title, 'font': {'size': 14}},
        number={'suffix': suffix, 'font': {'size': 28}},
        gauge={
            'axis': {'range': axis_range},
            'bar': {'color': color}
        }
    ))
    fig.update_layout(
        width=300,
        height=250,
        margin=dict(l=30, r=30, t=80, b=30)
    )
    col.plotly_chart(fig, use_container_width=False, key=key)
    
    # Show help text below the gauge if provided
    if help_text:
        col.markdown(f"<div style='width:300px;text-align:center;font-size:12px;color:#666;margin-top:-15px;'>ⓘ {help_text}</div>", unsafe_allow_html=True)


def gauge_na(col, title, message="N/A", key=None, help_text=None):
    """Display a grayed-out gauge for metrics that are not applicable."""
    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=0,
        title={'text': title, 'font': {'size': 14}},
        number={'font': {'color': '#999999', 'size': 28}, 'suffix': ''},
        gauge={
            'axis': {'range': [0, 100], 'tickcolor': '#cccccc'},
            'bar': {'color': '#e0e0e0'},
            'bgcolor': '#f5f5f5',
            'bordercolor': '#cccccc',
        }
    ))
    fig.update_layout(
        width=300,
        height=250, 
        margin=dict(l=30, r=30, t=80, b=30),
        annotations=[
            dict(
                text=message,
                x=0.5,
                y=0.25,
                showarrow=False,
                font=dict(size=14, color='#888888'),
                xref='paper',
                yref='paper'
            )
        ]
    )
    col.plotly_chart(fig, use_container_width=False, key=key)
    
    # Show help text below the gauge if provided
    if help_text:
        col.markdown(f"<div style='width:300px;text-align:center;font-size:12px;color:#666;margin-top:-15px;'>ⓘ {help_text}</div>", unsafe_allow_html=True)
