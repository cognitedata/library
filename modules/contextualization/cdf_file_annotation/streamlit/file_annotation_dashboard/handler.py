import streamlit as st
import pandas as pd
import altair as alt
from cognite.client import CogniteClient
from datetime import datetime, timedelta
from helper import (
    fetch_annotation_states,
    fetch_pipeline_run_history,
    process_runs_for_graphing,
    fetch_extraction_pipeline_config,
    calculate_success_failure_stats,
    get_failed_run_details,
    fetch_function_logs,
)


# --- Page Configuration ---
st.set_page_config(
    page_title="File Annotation Status Dashboard",
    page_icon="📄",
    layout="wide",
)

# --- Data Fetching ---
ep_config, annotation_state_view, file_view = fetch_extraction_pipeline_config()
df_raw = fetch_annotation_states(annotation_state_view)
pipeline_runs = fetch_pipeline_run_history()

# --- Sidebar for Inputs ---
with st.sidebar:
    st.header("Data Model Identifiers")
    if ep_config["launchFunction"].get("primaryScopeProperty") != "None":
        st.text_input(
            ep_config["launchFunction"].get("primaryScopeProperty"), value="All"
        )
    if ep_config["launchFunction"].get("secondaryScopeProperty"):
        st.text_input(
            ep_config["launchFunction"].get("secondaryScopeProperty"), value="All"
        )
    if st.button("Refresh Data"):
        st.cache_data.clear()
        st.rerun()

# --- Main Application ---
st.title("📄 File Annotation Status Dashboard")
st.markdown(
    "This application provides an audit trail and overview of the file annotation process."
)

if not df_raw.empty:
    st.sidebar.header("Filters")

    # Status Filter
    all_statuses = ["All"] + sorted(df_raw["status"].unique().tolist())
    selected_status = st.sidebar.selectbox("Filter by Status", options=all_statuses)

    # Date Range Filter
    min_date = df_raw["lastUpdatedTime"].min().date()
    max_date = df_raw["lastUpdatedTime"].max().date()
    date_range = st.sidebar.date_input(
        "Filter by Last Updated Date",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )

    # Apply filters
    df_filtered = df_raw.copy()
    if selected_status != "All":
        df_filtered = df_filtered[df_filtered["status"] == selected_status]

    if len(date_range) == 2:
        start_date, end_date = date_range
        df_filtered = df_filtered[
            (df_filtered["lastUpdatedTime"].dt.date >= start_date)
            & (df_filtered["lastUpdatedTime"].dt.date <= end_date)
        ]

    # --- Pipeline Statistics Section ---
    st.header("Pipeline Statistics (from Run History)")

    if pipeline_runs:
        # Time window selection
        time_window_map = {
            "Last 24 Hours": 24,
            "Last 7 Days": 7 * 24,
            "Last 30 Days": 30 * 24,
        }
        time_window_option = st.selectbox(
            "Select time window for recent performance:",
            options=list(time_window_map.keys()),
        )
        window_hours = time_window_map[time_window_option]
        now = pd.Timestamp.now(tz="UTC")
        filter_start_time = now - timedelta(hours=window_hours)

        # Filter runs based on the time window
        recent_pipeline_runs = [
            run
            for run in pipeline_runs
            if pd.to_datetime(run.created_time, unit="ms").tz_localize("UTC")
            > filter_start_time
        ]

        # Calculate stats for the selected time window
        success_count, failure_count = calculate_success_failure_stats(
            recent_pipeline_runs
        )
        df_runs_for_graphing = process_runs_for_graphing(recent_pipeline_runs)

        total_launched_recent = int(
            df_runs_for_graphing[df_runs_for_graphing["type"] == "Launch"][
                "count"
            ].sum()
        )
        total_finalized_recent = int(
            df_runs_for_graphing[df_runs_for_graphing["type"] == "Finalize"][
                "count"
            ].sum()
        )

        # Display Metrics
        sf_col1, sf_col2 = st.columns(2)
        with sf_col1:
            st.metric(
                f"Successful Runs ({time_window_option})",
                f"{success_count:,}",
                help="Total successful runs in the period.",
            )
        with sf_col2:
            st.metric(
                f"Failed Runs ({time_window_option})",
                f"{failure_count:,}",
                delta=f"{failure_count:,}" if failure_count > 0 else "0",
                delta_color="inverse",
                help="Total failed runs in the period.",
            )

        failed_runs_details = get_failed_run_details(recent_pipeline_runs)
        if failed_runs_details:
            with st.expander("View recent failures and fetch logs", expanded=False):
                for i, run in enumerate(failed_runs_details):
                    st.error(
                        f"**Timestamp:** {run['timestamp'].strftime('%Y-%m-%d %H:%M:%S %Z')}"
                    )
                    st.code(run["message"], language="text")

                    # Button to fetch logs if IDs are available
                    if run.get("function_id") and run.get("call_id"):
                        button_key = f"log_btn_{i}"
                        if st.button("Fetch Function Logs", key=button_key):
                            with st.spinner("Fetching logs..."):
                                logs = fetch_function_logs(
                                    function_id=int(run["function_id"]),
                                    call_id=int(run["call_id"]),
                                )
                                if logs:
                                    st.text_area(
                                        "Function Logs",
                                        "".join(logs),
                                        height=300,
                                    )
                                else:
                                    st.warning("No logs found for this run.")
                    st.divider()
        else:
            st.success("No failed pipeline runs in the selected time window. ✅")

        # Graphs
        base_chart = (
            alt.Chart(df_runs_for_graphing)
            .mark_circle(size=60, opacity=0.7)
            .encode(
                x=alt.X("timestamp:T", title="Time of Run"),
                y=alt.Y("count:Q", title="Files Processed"),
                tooltip=["timestamp:T", "count:Q", "type:N"],
            )
            .interactive()
        )
        g_col1, g_col2 = st.columns(2)
        with g_col1:
            st.metric(
                f"Total Launched ({time_window_option})",
                f"{total_launched_recent:,}",
            )
            launch_chart = base_chart.transform_filter(
                alt.datum.type == "Launch"
            ).properties(title="Files Processed per Launch Run")
            st.altair_chart(launch_chart, use_container_width=True)
        with g_col2:
            st.metric(
                f"Total Finalized ({time_window_option})",
                f"{total_finalized_recent:,}",
            )
            finalize_chart = base_chart.transform_filter(
                alt.datum.type == "Finalize"
            ).properties(title="Files Processed per Finalize Run")
            st.altair_chart(finalize_chart, use_container_width=True)

    # --- Dashboard Metrics ---
    st.header("Status Overview")

    status_counts = df_filtered["status"].value_counts()

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Files", len(df_filtered))
    with col2:
        st.metric("Annotated", status_counts.get("Annotated", 0))
    with col3:
        st.metric("New", status_counts.get("New", 0))
        st.metric("Processing", status_counts.get("Processing", 0))
    with col4:
        st.metric("Finalizing", status_counts.get("Finalizing", 0))
        st.metric("Failed", status_counts.get("Failed", 0))

    # --- Visualizations ---
    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader("Status Distribution")
        if not status_counts.empty:
            st.bar_chart(status_counts)
        else:
            st.info("No data to display for the selected filters.")

    with col_b:
        st.subheader("Annotation Activity Over Time")
        if not df_filtered.empty:
            time_series_data = (
                df_filtered.set_index("lastUpdatedTime").resample("D").size()
            )
            st.line_chart(time_series_data)
        else:
            st.info("No data to display for the selected filters.")

    # --- Detailed Data View ---
    st.subheader("Detailed Annotation State Instances")

    # Select columns to display
    columns_to_display = [
        "externalId",
        "status",
        "fileExternalId",
        "jobId",
        "retries",
        "annotationMessage",
        "lastUpdatedTime",
        "createdTime",
    ]
    # Filter out columns that don't exist in the dataframe
    displayable_cols = [col for col in columns_to_display if col in df_filtered.columns]

    st.dataframe(df_filtered[displayable_cols], use_container_width=True)

else:
    st.info(
        "No data returned from Cognite Data Fusion. Please check your settings and data model."
    )
