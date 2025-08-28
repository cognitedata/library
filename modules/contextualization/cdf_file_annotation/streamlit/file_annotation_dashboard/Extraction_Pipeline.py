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
    fetch_function_logs,
    parse_run_message,
    find_pipelines,
)

st.set_page_config(
    page_title="Pipeline Run History",
    page_icon="📈",
    layout="wide",
)

# --- Sidebar for Pipeline Selection ---
st.sidebar.title("Pipeline Selection")
# The helper function now returns a pre-filtered list
pipeline_ids = find_pipelines()

if not pipeline_ids:
    st.info("No active file annotation pipelines found to monitor.")
    st.stop()

# Use session_state to remember the selection across pages
if "selected_pipeline" not in st.session_state or st.session_state.selected_pipeline not in pipeline_ids:
    st.session_state.selected_pipeline = pipeline_ids[0]

# The selectbox displays the filtered list for the user
selected_pipeline = st.sidebar.selectbox("Select a pipeline to monitor:", options=pipeline_ids, key="selected_pipeline")

# --- Main Application ---
st.title("Pipeline Run History")
st.markdown("This page provides statistics and detailed history for the selected extraction pipeline run.")

# Fetch data using the user's selection
pipeline_runs = fetch_pipeline_run_history(selected_pipeline)


# --- Pipeline Statistics Section ---
if pipeline_runs:
    # Time window selection
    time_window_map = {
        "All": None,
        "Last 24 Hours": 24,
        "Last 7 Days": 7 * 24,
        "Last 30 Days": 30 * 24,
    }
    time_window_option = st.sidebar.selectbox(
        "Filter by Time Window:",
        options=list(time_window_map.keys()),
    )
    window_hours = time_window_map[time_window_option]

    if window_hours is not None:
        now = pd.Timestamp.now(tz="UTC")
        filter_start_time = now - timedelta(hours=window_hours)
        # Filter runs based on the time window
        recent_pipeline_runs = [
            run
            for run in pipeline_runs
            if pd.to_datetime(run.created_time, unit="ms").tz_localize("UTC") > filter_start_time
        ]
    else:
        # If 'All' is selected, use the original unfiltered list of runs
        recent_pipeline_runs = pipeline_runs

    # MODIFICATION: Check if 'recent_pipeline_runs' has data BEFORE processing.
    # If it's empty, display a message. Otherwise, proceed with stats and graphs.
    if not recent_pipeline_runs:
        st.warning("No pipeline runs found in the selected time window.")
    else:
        # --- Calculate detailed stats for the selected time window ---
        df_runs_for_graphing = process_runs_for_graphing(recent_pipeline_runs)

        launch_success = 0
        launch_failure = 0
        finalize_success = 0
        finalize_failure = 0

        for run in recent_pipeline_runs:
            # We need to parse the message to determine the caller type
            parsed_message = parse_run_message(run.message)
            caller = parsed_message.get("caller")

            if caller == "Launch":
                if run.status == "success":
                    launch_success += 1
                elif run.status == "failure":
                    launch_failure += 1
            elif caller == "Finalize":
                if run.status == "success":
                    finalize_success += 1
                elif run.status == "failure":
                    finalize_failure += 1

        total_launched_recent = int(df_runs_for_graphing[df_runs_for_graphing["type"] == "Launch"]["count"].sum())
        total_finalized_recent = int(df_runs_for_graphing[df_runs_for_graphing["type"] == "Finalize"]["count"].sum())

        # --- Display Metrics and Graphs in two columns ---
        g_col1, g_col2 = st.columns(2)

        with g_col1:
            st.subheader("Launch Runs")
            m_col1, m_col2, m_col3 = st.columns(3)
            m_col1.metric(
                f"Files Launched",
                f"{total_launched_recent:,}",
            )
            m_col2.metric(
                "Successful Runs",
                f"{launch_success:,}",
            )
            m_col3.metric(
                "Failed Runs",
                f"{launch_failure:,}",
                delta=f"{launch_failure:,}" if launch_failure > 0 else "0",
                delta_color="inverse",
            )

        with g_col2:
            st.subheader("Finalize Runs")
            m_col4, m_col5, m_col6 = st.columns(3)
            m_col4.metric(
                f"Files Finalized",
                f"{total_finalized_recent:,}",
            )
            m_col5.metric(
                "Successful Runs",
                f"{finalize_success:,}",
            )
            m_col6.metric(
                "Failed Runs",
                f"{finalize_failure:,}",
                delta=f"{finalize_failure:,}" if finalize_failure > 0 else "0",
                delta_color="inverse",
            )

        # --- Graphs ---
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

        chart_col1, chart_col2 = st.columns(2)
        with chart_col1:
            launch_chart = base_chart.transform_filter(alt.datum.type == "Launch").properties(
                title="Files Processed per Launch Run"
            )
            st.altair_chart(launch_chart, use_container_width=True)
        with chart_col2:
            finalize_chart = base_chart.transform_filter(alt.datum.type == "Finalize").properties(
                title="Files Processed per Finalize Run"
            )
            st.altair_chart(finalize_chart, use_container_width=True)

    # --- UNIFIED DETAILED RUN HISTORY ---
    with st.expander("View recent runs and fetch logs", expanded=True):
        if not recent_pipeline_runs:
            st.info("No runs in the selected time window.")
        else:
            f_col1, f_col2 = st.columns(2)
            with f_col1:
                run_status_filter = st.radio(
                    "Filter by run status:",
                    ("All", "Success", "Failure"),
                    horizontal=True,
                    key="run_status_filter",
                )
            with f_col2:
                caller_type_filter = st.radio(
                    "Filter by caller type:",
                    ("All", "Launch", "Finalize"),
                    horizontal=True,
                    key="caller_type_filter",
                )

            st.divider()

            filtered_runs = recent_pipeline_runs
            if run_status_filter != "All":
                filtered_runs = [run for run in filtered_runs if run.status.lower() == run_status_filter.lower()]

            if caller_type_filter != "All":
                filtered_runs = [
                    run for run in filtered_runs if parse_run_message(run.message).get("caller") == caller_type_filter
                ]

            if not filtered_runs:
                st.warning(f"No runs match the selected filters.")
            else:
                # Pagination state
                if "page_num" not in st.session_state:
                    st.session_state.page_num = 0

                items_per_page = 3
                start_idx = st.session_state.page_num * items_per_page
                end_idx = start_idx + items_per_page
                paginated_runs = filtered_runs[start_idx:end_idx]

                # Display logic for each run
                for run in paginated_runs:

                    if run.status == "success":
                        st.markdown(f"**Status:** Success")
                        st.success(
                            f"Timestamp: {pd.to_datetime(run.created_time, unit='ms').tz_localize('UTC').strftime('%Y-%m-%d %H:%M:%S %Z')}"
                        )
                    else:
                        st.markdown(f"**Status:** Failure")
                        st.error(
                            f"Timestamp: {pd.to_datetime(run.created_time, unit='ms').tz_localize('UTC').strftime('%Y-%m-%d %H:%M:%S %Z')}"
                        )

                    parsed_message = parse_run_message(run.message)
                    if run.message:
                        st.code(run.message, language="text")

                    function_id = int(parsed_message.get("function_id"))
                    call_id = int(parsed_message.get("call_id"))

                    if function_id and call_id:
                        button_key = f"log_btn_all_{call_id}"
                        if st.button("Fetch Function Logs", key=button_key):
                            with st.spinner("Fetching logs..."):
                                logs = fetch_function_logs(function_id=function_id, call_id=call_id)
                                if logs:
                                    st.text_area(
                                        "Function Logs",
                                        "".join(logs),
                                        height=300,
                                        key=f"log_area_all_{call_id}",
                                    )
                                else:
                                    st.warning("No logs found for this run.")
                    st.divider()

                # Pagination controls
                total_pages = (len(filtered_runs) + items_per_page - 1) // items_per_page
                if total_pages > 1:
                    p_col1, p_col2, p_col3 = st.columns([1, 2, 1])
                    with p_col1:
                        if st.button(
                            "Previous",
                            disabled=(st.session_state.page_num == 0),
                            use_container_width=True,
                        ):
                            st.session_state.page_num -= 1
                            st.rerun()
                    with p_col2:
                        st.markdown(
                            f"<div style='text-align: center;'>Page {st.session_state.page_num + 1} of {total_pages}</div>",
                            unsafe_allow_html=True,
                        )
                    with p_col3:
                        if st.button(
                            "Next",
                            disabled=(st.session_state.page_num >= total_pages - 1),
                            use_container_width=True,
                        ):
                            st.session_state.page_num += 1
                            st.rerun()
else:
    st.info("No data returned from Cognite Data Fusion. Please check your settings and data model.")
