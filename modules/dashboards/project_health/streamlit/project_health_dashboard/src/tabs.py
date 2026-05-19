"""
Tab rendering for the CDF Project Health Dashboard.
"""

import streamlit as st
from .charts import create_health_gauge, create_status_donut
from .utils import get_status_emoji, format_timestamp, build_cdf_link
from .ui_components import render_resource_table, render_errors_section, render_function_details


def render_health_charts(
    summary: dict,
    title: str,
    emoji: str,
    time_range_label: str,
    gauge_key: str,
    donut_key: str,
    health_key: str = "healthy",
    no_runs_key: str = "no_runs",
) -> None:
    col1, col2 = st.columns(2)
    total_with_runs = summary["total"] - summary.get(no_runs_key, 0)
    with col1:
        fig = create_health_gauge(summary[health_key], total_with_runs, "Overall Health", emoji)
        st.plotly_chart(fig, use_container_width=True, key=gauge_key)
    with col2:
        fig = create_status_donut(summary, "Status Distribution", time_range_label)
        st.plotly_chart(fig, use_container_width=True, key=donut_key)


def render_runs_expanders(
    resources: list,
    resource_name: str,
    emoji: str,
    runs_key: str,
    status_key: str = "status",
    time_key: str = "created_time",
    error_key: str = None,
) -> None:
    for resource in resources:
        runs = resource.get(runs_key, [])
        if not runs:
            continue
        name = resource.get("name", resource.get("external_id", "Unknown"))
        with st.expander(f"{emoji} {name} - {len(runs)} run(s) in window"):
            for run in runs:
                status = run.get(status_key, "Unknown")
                run_emoji = get_status_emoji(status)
                if "start_time" in run and "end_time" in run:
                    start_str = format_timestamp(run.get("start_time"))
                    end_val = run.get("end_time")
                    end_str = format_timestamp(end_val) if end_val else "Running..."
                    st.write(f"{run_emoji} **{status}** | Started: {start_str} | Ended: {end_str}")
                elif "started_time" in run and "finished_time" in run:
                    start_str = format_timestamp(run.get("started_time"))
                    end_val = run.get("finished_time")
                    end_str = format_timestamp(end_val) if end_val else "Running..."
                    st.write(f"{run_emoji} **{status}** | Started: {start_str} | Finished: {end_str}")
                else:
                    time_str = format_timestamp(run.get(time_key))
                    st.write(f"{run_emoji} **{status}** at {time_str}")
                if error_key and run.get(error_key):
                    if error_key == "error":
                        st.error(f"Error: {run[error_key]}")
                    else:
                        st.caption(f"{error_key.replace('_', ' ').title()}: {run[error_key]}")


def render_overview_tab(
    ep_summary: dict,
    wf_summary: dict,
    tr_summary: dict,
    fn_summary: dict,
    dataset_external_id: str,
    time_range_label: str,
    config: dict,
    all_errors: list,
) -> None:
    st.markdown("Monitor the health of your CDF resources scoped by dataset.")
    st.info(f"📊 Monitoring dataset: **{dataset_external_id}** | Time Range: **{time_range_label}**")
    st.header("Health Overview")
    ep_total = ep_summary.get("total", 0)
    wf_total = wf_summary.get("total", 0)
    tr_total = tr_summary.get("total", 0)
    fn_total = fn_summary.get("total", 0)
    ep_with_runs = ep_total - ep_summary.get("no_runs", 0)
    wf_with_runs = wf_total - wf_summary.get("no_runs", 0)
    tr_with_runs = tr_total - tr_summary.get("no_runs", 0)
    fn_with_calls = fn_total - fn_summary.get("no_calls", 0)
    if ep_total == 0 and wf_total == 0 and tr_total == 0 and fn_total == 0:
        st.info("No resources found for this dataset in the selected time range. Run the function from the **Configuration** tab, or pick another dataset in the sidebar.")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        fig = create_health_gauge(ep_summary.get("healthy", 0), ep_with_runs, "Extraction Pipelines", "📥")
        st.plotly_chart(fig, use_container_width=True, key="gauge_overview_ep")
    with col2:
        fig = create_health_gauge(wf_summary.get("healthy", 0), wf_with_runs, "Workflows", "🔄")
        st.plotly_chart(fig, use_container_width=True, key="gauge_overview_wf")
    with col3:
        fig = create_health_gauge(tr_summary.get("healthy", 0), tr_with_runs, "Transformations", "⚡")
        st.plotly_chart(fig, use_container_width=True, key="gauge_overview_tr")
    with col4:
        fig = create_health_gauge(fn_summary.get("healthy", 0), fn_with_calls, "Functions", "🔧")
        st.plotly_chart(fig, use_container_width=True, key="gauge_overview_fn")
    st.markdown("---")
    st.subheader("📈 Status Distribution")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.plotly_chart(create_status_donut(ep_summary, "Extraction Pipelines", time_range_label), use_container_width=True, key="donut_overview_ep")
    with col2:
        st.plotly_chart(create_status_donut(wf_summary, "Workflows", time_range_label), use_container_width=True, key="donut_overview_wf")
    with col3:
        st.plotly_chart(create_status_donut(tr_summary, "Transformations", time_range_label), use_container_width=True, key="donut_overview_tr")
    with col4:
        st.plotly_chart(create_status_donut(fn_summary, "Functions", time_range_label), use_container_width=True, key="donut_overview_fn")
    st.markdown("---")
    st.subheader("🔗 Quick Links")
    link_cols = st.columns(4)
    base_url = config.get("cdf_base_url") or ""
    project = config.get("cdf_project") or ""
    cluster = config.get("cdf_cluster")
    with link_cols[0]:
        link = build_cdf_link(base_url, project, "extraction_pipelines", cluster=cluster)
        st.markdown(f"[View Extraction Pipelines ↗]({link})" if link else "—")
    with link_cols[1]:
        link = build_cdf_link(base_url, project, "workflows", cluster=cluster)
        st.markdown(f"[View Workflows ↗]({link})" if link else "—")
    with link_cols[2]:
        link = build_cdf_link(base_url, project, "transformations", cluster=cluster)
        st.markdown(f"[View Transformations ↗]({link})" if link else "—")
    with link_cols[3]:
        link = build_cdf_link(base_url, project, "functions", cluster=cluster)
        st.markdown(f"[View Functions ↗]({link})" if link else "—")
    st.markdown("---")
    st.subheader("❌ Recent Errors")
    render_errors_section(all_errors)


def render_extraction_pipelines_tab(
    extraction_data: dict,
    dataset_external_id: str,
    time_range_label: str,
    config: dict,
    uptime_threshold: int,
) -> None:
    st.info(f"📥 **Extraction Pipelines Health** | Dataset: **{dataset_external_id}** | Time Range: **{time_range_label}** | Uptime Threshold: **{uptime_threshold}%**")
    render_health_charts(extraction_data["summary"], "Extraction Pipelines", "📥", time_range_label, "gauge_tab_ep", "donut_tab_ep")
    st.markdown("---")
    st.subheader("Pipeline Details")
    render_resource_table(extraction_data["pipelines"], "extraction_pipeline", config, time_range_label, uptime_threshold)
    if extraction_data["pipelines"]:
        st.markdown("---")
        st.subheader(f"Runs in {time_range_label}")
        render_runs_expanders(extraction_data["pipelines"], "Pipeline", "📥", "recent_runs", "status", "created_time", "message")


def render_workflows_tab(
    workflow_data: dict,
    dataset_external_id: str,
    time_range_label: str,
    config: dict,
    uptime_threshold: int,
) -> None:
    st.info(f"🔄 **Workflows Health** | Dataset: **{dataset_external_id}** | Time Range: **{time_range_label}** | Uptime Threshold: **{uptime_threshold}%**")
    render_health_charts(workflow_data["summary"], "Workflows", "🔄", time_range_label, "gauge_tab_wf", "donut_tab_wf")
    st.markdown("---")
    st.subheader("Workflow Details")
    render_resource_table(workflow_data["workflows"], "workflow", config, time_range_label, uptime_threshold)
    if workflow_data["workflows"]:
        st.markdown("---")
        st.subheader(f"Executions in {time_range_label}")
        render_runs_expanders(workflow_data["workflows"], "Workflow", "🔄", "recent_executions", "status", "start_time", "reason_for_incompletion")


def render_transformations_tab(
    transformation_data: dict,
    dataset_external_id: str,
    time_range_label: str,
    config: dict,
    uptime_threshold: int,
) -> None:
    st.info(f"⚡ **Transformations Health** | Dataset: **{dataset_external_id}** | Time Range: **{time_range_label}** | Uptime Threshold: **{uptime_threshold}%**")
    render_health_charts(transformation_data["summary"], "Transformations", "⚡", time_range_label, "gauge_tab_tr", "donut_tab_tr")
    st.markdown("---")
    st.subheader("Transformation Details")
    render_resource_table(transformation_data["transformations"], "transformation", config, time_range_label, uptime_threshold)
    if transformation_data["transformations"]:
        st.markdown("---")
        st.subheader(f"Jobs in {time_range_label}")
        render_runs_expanders(transformation_data["transformations"], "Transformation", "⚡", "recent_jobs", "status", "started_time", "error")


def render_functions_tab(
    function_data: dict,
    dataset_external_id: str,
    time_range_label: str,
    config: dict,
    uptime_threshold: int,
) -> None:
    st.info(f"🔧 **Functions Health** | Dataset: **{dataset_external_id}** | Time Range: **{time_range_label}** | Uptime Threshold: **{uptime_threshold}%**")
    render_health_charts(function_data["summary"], "Functions", "🔧", time_range_label, "gauge_tab_fn", "donut_tab_fn", health_key="healthy", no_runs_key="no_calls")
    st.markdown("---")
    st.subheader("Function Details")
    render_function_details(function_data, config, time_range_label, uptime_threshold)
    if function_data["functions"]:
        st.markdown("---")
        st.subheader(f"Calls in {time_range_label}")
        render_runs_expanders(function_data["functions"], "Function", "🔧", "recent_calls", "status", "start_time", "error")
