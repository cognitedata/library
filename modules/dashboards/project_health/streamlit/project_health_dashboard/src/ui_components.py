"""
UI components for the CDF Project Health Dashboard.
"""

import streamlit as st
from .utils import (
    get_status_color,
    get_status_emoji,
    get_time_ago,
    format_timestamp,
    build_cdf_link,
    is_failed_status,
)
from .config import COLORS


def _render_status_cell(status: str) -> None:
    if is_failed_status(status):
        st.markdown(f"<span style='color:{COLORS['failed']}'>❌ {status}</span>", unsafe_allow_html=True)
    else:
        color = get_status_color(status)
        emoji = get_status_emoji(status)
        st.markdown(f"<span style='color:{color}'>{emoji} {status}</span>", unsafe_allow_html=True)


def _render_uptime_cell(uptime_pct: float, has_runs: bool, uptime_threshold: int) -> None:
    if not has_runs:
        st.markdown(f"<span style='color:{COLORS['neutral']}'>No runs</span>", unsafe_allow_html=True)
        return
    is_healthy = uptime_pct >= uptime_threshold
    color = COLORS["success"] if is_healthy else COLORS["failed"]
    emoji = "✅" if is_healthy else "❌"
    st.markdown(f"<span style='color:{color}'>{emoji} {uptime_pct:.1f}%</span>", unsafe_allow_html=True)


def _render_link_cell(config: dict, resource_type: str, resource_id) -> None:
    link = build_cdf_link(
        config.get("cdf_base_url") or "",
        config.get("cdf_project") or "",
        resource_type,
        resource_id,
        config.get("cdf_cluster"),
    )
    st.markdown(f"[View ↗]({link})" if link else "—")


def _get_resource_runs_count(resource: dict) -> int:
    for key in ("runs_in_window", "jobs_in_window", "executions_in_window", "calls_in_window"):
        if resource.get(key):
            return resource[key]
    return 0


def _render_table_header(columns: list, widths: list = None) -> None:
    widths = widths or [3, 2, 2, 2, 1]
    cols = st.columns(widths)
    for col, header in zip(cols, columns):
        with col:
            st.markdown(f"**{header}**")
    st.markdown("---")


def render_resource_table(
    resources: list,
    resource_type: str,
    config: dict,
    time_range_label: str = "",
    uptime_threshold: int = 75,
) -> None:
    if not resources:
        st.info(f"No {resource_type.replace('_', ' ')} found for this dataset.")
        return
    _render_table_header(["Name", "Latest Run Status", "Last Run", "Uptime", "Link"])
    for resource in resources:
        name = resource.get("name") or resource.get("external_id", "Unknown")
        status = resource.get("last_status") or resource.get("status") or "Unknown"
        last_run = (
            resource.get("last_run")
            or resource.get("last_execution")
            or resource.get("last_seen")
            or resource.get("last_call")
        )
        runs_in_window = _get_resource_runs_count(resource)
        uptime_pct = resource.get("uptime_percentage", 100.0)
        resource_id = resource.get("id") or resource.get("external_id")
        col1, col2, col3, col4, col5 = st.columns([3, 2, 2, 2, 1])
        with col1:
            st.write(name)
        with col2:
            _render_status_cell(status)
        with col3:
            st.write(f"🕐 {get_time_ago(last_run)}")
        with col4:
            _render_uptime_cell(uptime_pct, runs_in_window > 0, uptime_threshold)
        with col5:
            _render_link_cell(config, resource_type, resource_id)


def render_function_details(
    function_data: dict,
    config: dict,
    time_range_label: str,
    uptime_threshold: int = 75,
) -> None:
    functions = function_data.get("functions", [])
    if not functions:
        st.info("No functions found for this dataset.")
        return
    _render_table_header(["Name", "Deployment Status", "Uptime", "Last Call", "Link"])
    for func in functions:
        name = func.get("name") or func.get("external_id", "Unknown")
        status = func.get("status", "Unknown")
        calls_in_window = func.get("calls_in_window", 0)
        uptime_pct = func.get("uptime_percentage", 100.0)
        last_call = func.get("last_call")
        col1, col2, col3, col4, col5 = st.columns([3, 2, 2, 2, 1])
        with col1:
            st.write(name)
        with col2:
            _render_status_cell(status)
        with col3:
            _render_uptime_cell(uptime_pct, calls_in_window > 0, uptime_threshold)
        with col4:
            time_ago = get_time_ago(last_call) if last_call else "Never"
            st.write(f"🕐 {time_ago}")
        with col5:
            _render_link_cell(config, "function", func.get("id") or func.get("external_id"))


def render_errors_section(all_errors: list, max_display: int = 20) -> None:
    if not all_errors:
        st.success("No errors detected in recent runs.")
        return
    st.error(f"Found {len(all_errors)} error(s) in recent runs")
    for error in all_errors[:max_display]:
        with st.expander(f"❌ {error['resource']} - {error['status']}", expanded=False):
            if error.get("time"):
                st.write(f"**Time:** {format_timestamp(error['time'])}")
            if error.get("message"):
                st.write(f"**Message:** {error['message']}")
