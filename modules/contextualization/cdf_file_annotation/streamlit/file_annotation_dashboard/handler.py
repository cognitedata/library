import streamlit as st
import pandas as pd
import altair as alt
from cognite.client import CogniteClient
from datetime import datetime, timedelta
from helper import (
    fetch_annotation_states,
    fetch_pipeline_run_history,
    process_runs_for_graphing,
)

# --- Page Configuration ---
st.set_page_config(
    page_title="File Annotation Status Dashboard",
    page_icon="📄",
    layout="wide",
)

# --- Sidebar for Inputs ---
with st.sidebar:
    st.header("CDF Connection Settings")
    st.header("Data Model Identifiers")
    project = st.text_input("Project", value="Refining")
    site = st.text_input("Site", value="All")
    unit = st.text_input("Unit", value="All")
    if st.button("Refresh Data"):
        st.cache_data.clear()

# --- Main Application ---
st.title("📄 File Annotation Status Dashboard")
st.markdown(
    "This application provides an audit trail and overview of the file annotation process."
)

df_raw = fetch_annotation_states(project)
pipeline_runs = fetch_pipeline_run_history(project)

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
        df_runs = process_runs_for_graphing(pipeline_runs)

        # --- Recent Performance ---
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

        recent_runs_df = df_runs[df_runs["timestamp"] > filter_start_time]

        # --- Stats ---
        total_launched_recent = int(
            recent_runs_df[recent_runs_df["type"] == "Launch"]["count"].sum()
        )
        total_finalized_recent = int(
            recent_runs_df[recent_runs_df["type"] == "Finalize"]["count"].sum()
        )

        recent_failed_df = df_raw[
            (df_raw["lastUpdatedTime"] > filter_start_time)
            & (df_raw["status"] == "Failed")
        ]
        avg_retries_recent = (
            recent_failed_df["retries"].mean() if not recent_failed_df.empty else 0
        )

        # Base chart for common properties
        base_chart = (
            alt.Chart(recent_runs_df)
            .mark_circle(size=60, opacity=0.7)
            .encode(
                x=alt.X("timestamp:T", title="Time of Run"),
                y=alt.Y("count:Q", title="Files Processed"),
                tooltip=[
                    alt.Tooltip("timestamp:T", title="Time", format="%Y-%m-%d %H:%M"),
                    alt.Tooltip("count:Q", title="Files Processed"),
                    alt.Tooltip("type:N", title="Type"),
                ],
            )
            .interactive()
        )

        # Create two columns for the graphs
        g_col1, g_col2 = st.columns(2)
        with g_col1:
            st.metric(
                f"Total Launched ({time_window_option})",
                f"{total_launched_recent:,}",
                help=f"Total files launched in the selected period.",
            )
            launch_chart = base_chart.transform_filter(
                alt.datum.type == "Launch"
            ).properties(title="Files Processed per Launch Run")
            st.altair_chart(launch_chart, use_container_width=True)

        with g_col2:
            st.metric(
                f"Total Finalized (Aggregated) ({time_window_option})",
                f"{total_finalized_recent:,}",
                help=f"Total files finalized in the selected period.",
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
    with col3:
        st.metric("Processing", status_counts.get("Processing", 0))
    with col4:
        st.metric("Finalizing", status_counts.get("Finalizing", 0))
    with col4:
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
