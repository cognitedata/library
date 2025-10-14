import streamlit as st
import pandas as pd
import altair as alt
from datetime import timedelta
from helper import (
    fetch_annotation_states,
    fetch_pipeline_run_history,
    process_runs_for_graphing,
    fetch_extraction_pipeline_config,
    fetch_function_logs,
    parse_run_message,
    find_pipelines,
    get_files_by_call_id,
    calculate_overview_kpis,
    filter_log_lines,
)

# --- Page Configuration ---
st.set_page_config(
    page_title="Pipeline Health",
    page_icon="🩺",
    layout="wide",
)

# --- Session State and Callbacks ---
if "selected_pipeline" not in st.session_state:
    st.session_state.selected_pipeline = None
if "selected_status_file_index" not in st.session_state:
    st.session_state.selected_status_file_index = None
if "page_num" not in st.session_state:  # For run history pagination
    st.session_state.page_num = 0


def reset_table_selection():
    st.session_state.selected_status_file_index = None


# --- Sidebar ---
st.sidebar.title("Pipeline Selection")
pipeline_ids = find_pipelines()

if not pipeline_ids:
    st.info("No active file annotation pipelines found to monitor.")
    st.stop()

if st.session_state.selected_pipeline not in pipeline_ids:
    st.session_state.selected_pipeline = pipeline_ids[0]

selected_pipeline = st.sidebar.selectbox("Select a pipeline to monitor:", options=pipeline_ids, key="selected_pipeline")

# --- Main Application ---
st.title("Pipeline Health Dashboard")

# --- Data Fetching ---
config_result = fetch_extraction_pipeline_config(selected_pipeline)
if not config_result:
    st.error(f"Could not fetch configuration for pipeline: {selected_pipeline}")
    st.stop()

ep_config, view_config = config_result

annotation_state_view = view_config["annotation_state"]
file_view = view_config["file"]

df_annotation_states = fetch_annotation_states(annotation_state_view, file_view)
pipeline_runs = fetch_pipeline_run_history(selected_pipeline)

# --- Create Tabs ---
overview_tab, explorer_tab, history_tab = st.tabs(["Overview", "File Explorer", "Run History"])

# ==========================================
#               OVERVIEW TAB
# ==========================================
with overview_tab:
    st.subheader(
        "Live Pipeline KPIs",
        help="Provides a high-level summary of the pipeline's current state and historical throughput. The KPIs are calculated directly from the AnnotationState data model for real-time accuracy.",
    )

    kpis = calculate_overview_kpis(df_annotation_states)

    kpi_col1, kpi_col2, kpi_col3 = st.columns(3)
    kpi_col1.metric("Files Awaiting Processing", f"{kpis['awaiting_processing']:,}")
    kpi_col2.metric("Total Files Processed", f"{kpis['processed_total']:,}")
    kpi_col3.metric(
        "Overall Failure Rate",
        f"{kpis['failure_rate_total']:.2f}%",
        delta=f"{kpis['failed_total']:,} failed files",
        delta_color="inverse" if kpis["failed_total"] > 0 else "off",
    )

    st.divider()
    st.subheader("Pipeline Throughput")
    time_agg = st.radio("Aggregate by:", options=["Daily", "Hourly", "Weekly"], horizontal=True, key="time_agg_radio")

    if not df_annotation_states.empty:
        df_finalized = df_annotation_states[df_annotation_states["status"].isin(["Annotated", "Failed"])].copy()
        if not df_finalized.empty:
            if time_agg == "Hourly":
                df_finalized["time_bucket"] = df_finalized["lastUpdatedTime"].dt.floor("H")
            elif time_agg == "Weekly":
                df_finalized["time_bucket"] = (
                    df_finalized["lastUpdatedTime"].dt.to_period("W").apply(lambda p: p.start_time)
                )
            else:  # Daily
                df_finalized["time_bucket"] = df_finalized["lastUpdatedTime"].dt.date

            daily_counts = df_finalized.groupby("time_bucket").size().reset_index(name="count")

            throughput_chart = (
                alt.Chart(daily_counts)
                .mark_bar()
                .encode(
                    x=alt.X("time_bucket:T", title=f"Time ({time_agg})"),
                    y=alt.Y("count:Q", title="Number of Files Finalized"),
                    tooltip=["time_bucket:T", "count:Q"],
                )
                .properties(title=f"Files Finalized {time_agg}")
                .interactive()
            )
            st.altair_chart(throughput_chart, use_container_width=True)
        else:
            st.info("No files have been finalized yet.")

# ==========================================
#              FILE EXPLORER TAB
# ==========================================
with explorer_tab:
    st.subheader(
        "File-Centric Debugging",
        help="A file-centric debugging tool for deep-dive analysis. Filter and select any file to view its current status, metadata, and the specific Launch and Finalize function logs associated with it.",
    )
    if df_annotation_states.empty:
        st.info("No annotation state data found for this pipeline.")
    else:
        with st.expander("Filter and Slice Data"):
            # ... (your existing filter logic remains unchanged here) ...
            excluded_columns = [
                "externalId",
                "space",
                "annotationMessage",
                "fileAliases",
                "fileAssets",
                "fileIsuploaded",
                "diagramDetectJobId",
                "linkedFile",
                "patternModeJobId",
                "sourceCreatedUser",
                "sourceCreatedTime",
                "sourceUpdatedTime",
                "sourceUpdatedUser",
                "fileSpace",
                "fileSourceupdateduser",
                "fileSourcecreatedUser",
                "fileSourceId",
                "createdTime",
                "fileSourcecreateduser",
                "patternModeMessage",
                "fileSourceupdatedtime",
                "fileSourcecreatedtime",
                "fileUploadedtime",
            ]
            potential_columns = [col for col in df_annotation_states.columns if col not in excluded_columns]
            filterable_columns = []
            for col in potential_columns:
                # Skip empty columns or columns where the first item is a list/dict
                if df_annotation_states[col].dropna().empty or isinstance(
                    df_annotation_states[col].dropna().iloc[0], (list, dict)
                ):
                    continue

                # Final check to ensure the column is suitable for filtering
                if df_annotation_states[col].nunique() < 100:
                    filterable_columns.append(col)

            filterable_columns = sorted(filterable_columns)

            filter_col1, filter_col2 = st.columns(2)
            selected_column = filter_col1.selectbox(
                "Filter by Metadata Property",
                ["None"] + filterable_columns,
                on_change=reset_table_selection,
                key="meta_filter",
            )

            selected_values = []
            if selected_column != "None":
                unique_values = sorted(df_annotation_states[selected_column].dropna().unique().tolist())
                selected_values = filter_col2.multiselect(
                    f"Select Value(s) for {selected_column}",
                    unique_values,
                    on_change=reset_table_selection,
                    key="value_filter",
                )

        df_display = df_annotation_states.copy()
        if selected_column != "None" and selected_values:
            df_display = df_display[df_display[selected_column].isin(selected_values)]

        df_display = df_display.sort_values(by="lastUpdatedTime", ascending=False).reset_index(drop=True)
        df_display.insert(0, "Select", False)

        if (
            st.session_state.selected_status_file_index is not None
            and st.session_state.selected_status_file_index < len(df_display)
        ):
            df_display.at[st.session_state.selected_status_file_index, "Select"] = True

        # --- START: New additions for customizable and readable columns ---
        default_columns = [
            "Select",
            "fileName",
            "fileExternalId",
            "fileSourceid",
            "status",
            "fileMimetype",
            "pageCount",
            "annotatedPageCount",
        ]
        all_columns = df_display.columns.tolist()

        with st.popover("Customize Table Columns"):
            selected_columns = st.multiselect(
                "Select columns to display:",
                options=all_columns,
                default=[col for col in default_columns if col in all_columns],
            )

        if not selected_columns:
            st.warning("Please select at least one column to display.")
            st.stop()

        edited_df = st.data_editor(
            df_display[selected_columns],  # Display only selected columns
            key="status_table_editor",
            column_config={
                "Select": st.column_config.CheckboxColumn(required=True),
                "fileName": "File Name",
                "fileExternalId": "File External ID",
                "status": "Annotation Status",
                "retries": "Retries",
                "fileSourceid": "Source ID",
                "fileMimetype": "Mime Type",
                "annotationMessage": "Annotation Message",
                "patternModeMessage": "Pattern Mode Message",
                "pageCount": "Page Count",
                "annotatedPageCount": "Annotated Page Count",
            },
            use_container_width=True,
            hide_index=True,
            disabled=df_display.columns.difference(["Select"]),
        )

        selected_indices = edited_df[edited_df.Select].index.tolist()
        if len(selected_indices) > 1:
            new_selection = [
                idx for idx in selected_indices if idx != st.session_state.get("selected_status_file_index")
            ]
            st.session_state.selected_status_file_index = new_selection[0] if new_selection else None
            st.rerun()
        elif len(selected_indices) == 1:
            st.session_state.selected_status_file_index = selected_indices[0]
        elif len(selected_indices) == 0 and st.session_state.selected_status_file_index is not None:
            st.session_state.selected_status_file_index = None
            st.rerun()

        if (
            st.session_state.selected_status_file_index is not None
            and st.session_state.selected_status_file_index < len(df_display)
        ):
            st.divider()
            st.subheader("Function Log Viewer")
            selected_row = df_display.iloc[st.session_state.selected_status_file_index]
            file_ext_id = selected_row.get("fileExternalId", "")

            finalize_tab, launch_tab = st.tabs(["Finalize Log", "Launch Log"])
            with finalize_tab:
                finalize_func_id = selected_row.get("finalizeFunctionId")
                finalize_call_id = selected_row.get("finalizeFunctionCallId")
                if pd.notna(finalize_func_id) and pd.notna(finalize_call_id):
                    with st.spinner("Fetching finalize log..."):
                        finalize_logs_raw = "".join(
                            fetch_function_logs(function_id=int(finalize_func_id), call_id=int(finalize_call_id))
                        )
                        if finalize_logs_raw:
                            st.download_button(
                                "Download Full Log", finalize_logs_raw, f"{file_ext_id}_finalize_log.txt"
                            )
                            filtered_log = filter_log_lines(finalize_logs_raw, file_ext_id)
                            st.write("**Relevant Log Entries:**")
                            st.code(
                                filtered_log if filtered_log else "No log entries found for this specific file.",
                                language="log",
                            )
                            with st.expander("View Full Log"):
                                st.code(finalize_logs_raw, language="log")
                        else:
                            st.warning("No finalize logs found.")
                else:
                    st.info("No Finalize Function call information available for this file.")

            with launch_tab:
                launch_func_id = selected_row.get("launchFunctionId")
                launch_call_id = selected_row.get("launchFunctionCallId")
                if pd.notna(launch_func_id) and pd.notna(launch_call_id):
                    with st.spinner("Fetching launch log..."):
                        launch_logs_raw = "".join(
                            fetch_function_logs(function_id=int(launch_func_id), call_id=int(launch_call_id))
                        )
                        # NOTE: launch log doesn't provide log lines with individual Node Id's of files processed
                        if launch_logs_raw:
                            st.download_button("Download Full Log", launch_logs_raw, f"{file_ext_id}_launch_log.txt")
                            with st.expander("View Full Log"):
                                st.code(launch_logs_raw, language="log")
                        else:
                            st.warning("No launch logs found.")
                else:
                    st.info("No Launch Function call information available for this file.")

# ==========================================
#              RUN HISTORY TAB
# ==========================================
with history_tab:
    st.subheader(
        "Run-Centric Analysis",
        help="A run-centric view for analyzing the execution history of the pipeline functions. Review the status, logs, and a list of files processed for each individual pipeline run.",
    )
    if not pipeline_runs:
        st.info("No pipeline runs found for this pipeline.")
    else:
        time_window_map = {"All": None, "Last 24 Hours": 24, "Last 7 Days": 7 * 24, "Last 30 Days": 30 * 24}
        time_window_option = st.selectbox(
            "Filter by Time Window:", options=list(time_window_map.keys()), key="time_window_history"
        )
        window_hours = time_window_map[time_window_option]

        if window_hours is not None:
            now = pd.Timestamp.now(tz="UTC")
            filter_start_time = now - timedelta(hours=window_hours)
            recent_pipeline_runs = [
                run
                for run in pipeline_runs
                if pd.to_datetime(run.created_time, unit="ms").tz_localize("UTC") > filter_start_time
            ]
        else:
            recent_pipeline_runs = pipeline_runs

        if not recent_pipeline_runs:
            st.warning("No pipeline runs found in the selected time window.")
        else:
            df_runs_for_graphing = process_runs_for_graphing(recent_pipeline_runs)
            launch_success, launch_failure, finalize_success, finalize_failure = 0, 0, 0, 0
            for run in recent_pipeline_runs:
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
            total_finalized_recent = int(
                df_runs_for_graphing[df_runs_for_graphing["type"] == "Finalize"]["count"].sum()
            )

            g_col1, g_col2 = st.columns(2)
            with g_col1:
                st.subheader("Launch Runs")
                m_col1, m_col2, m_col3 = st.columns(3)
                m_col1.metric("Files Launched", f"{total_launched_recent:,}")
                m_col2.metric("Successful Runs", f"{launch_success:,}")
                m_col3.metric(
                    "Failed Runs",
                    f"{launch_failure:,}",
                    delta=f"{launch_failure:,}" if launch_failure > 0 else "0",
                    delta_color="inverse",
                )
            with g_col2:
                st.subheader("Finalize Runs")
                m_col4, m_col5, m_col6 = st.columns(3)
                m_col4.metric("Files Finalized", f"{total_finalized_recent:,}")
                m_col5.metric("Successful Runs", f"{finalize_success:,}")
                m_col6.metric(
                    "Failed Runs",
                    f"{finalize_failure:,}",
                    delta=f"{finalize_failure:,}" if finalize_failure > 0 else "0",
                    delta_color="inverse",
                )

            st.divider()

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
                st.altair_chart(
                    base_chart.transform_filter(alt.datum.type == "Launch").properties(
                        title="Files Processed per Launch Run"
                    ),
                    use_container_width=True,
                )
            with chart_col2:
                st.altair_chart(
                    base_chart.transform_filter(alt.datum.type == "Finalize").properties(
                        title="Files Processed per Finalize Run"
                    ),
                    use_container_width=True,
                )

            st.divider()
            st.subheader("Detailed Run History")

            f_col1, f_col2 = st.columns(2)
            run_status_filter = f_col1.radio(
                "Filter by run status:", ("All", "Success", "Failure"), horizontal=True, key="run_status_filter"
            )
            caller_type_filter = f_col2.radio(
                "Filter by caller type:", ("All", "Launch", "Finalize"), horizontal=True, key="caller_type_filter"
            )

            filtered_runs = recent_pipeline_runs
            if run_status_filter != "All":
                filtered_runs = [run for run in filtered_runs if run.status.lower() == run_status_filter.lower()]
            if caller_type_filter != "All":
                filtered_runs = [
                    run for run in filtered_runs if parse_run_message(run.message).get("caller") == caller_type_filter
                ]

            if not filtered_runs:
                st.warning("No runs match the selected filters.")
            else:
                items_per_page = 5
                start_idx = st.session_state.page_num * items_per_page
                end_idx = start_idx + items_per_page
                paginated_runs = filtered_runs[start_idx:end_idx]

                if not paginated_runs:
                    st.warning("No runs match the selected filters.")
                else:
                    for run in paginated_runs:
                        st.markdown(
                            f"**Status:** {run.status.capitalize()} at {pd.to_datetime(run.created_time, unit='ms').tz_localize('UTC').strftime('%Y-%m-%d %H:%M:%S')}"
                        )
                        st.code(run.message, language="text")

                        parsed_message = parse_run_message(run.message)
                        function_id_str = parsed_message.get("function_id")
                        call_id_str = parsed_message.get("call_id")

                        expander_col1, expander_col2 = st.columns(2)

                        with expander_col1:
                            with st.expander("View Function Log"):
                                st.write("**Function Log**")
                                log_key = f"log_{run.id}"

                                if function_id_str and call_id_str:
                                    # Show the log if it has been fetched, otherwise show the load button
                                    if log_key in st.session_state:
                                        st.download_button(
                                            "Download Log", st.session_state[log_key], f"run_{run.id}_log.txt"
                                        )
                                        st.code(st.session_state[log_key], language="log")
                                    else:
                                        if st.button("Load Log", key=f"load_btn_{run.id}"):
                                            with st.spinner("Fetching logs..."):
                                                logs = "".join(
                                                    fetch_function_logs(
                                                        function_id=int(function_id_str), call_id=int(call_id_str)
                                                    )
                                                )
                                                st.session_state[log_key] = (
                                                    logs if logs else "No logs found for this run."
                                                )
                                                st.rerun()
                                else:
                                    st.info("No log information in run message.")

                        with expander_col2:
                            with st.expander("View Files Processed"):
                                st.write("External ID(s):")
                                if call_id_str:
                                    df_files_in_run = get_files_by_call_id(int(call_id_str), annotation_state_view)
                                    if not df_files_in_run.empty:
                                        file_list = df_files_in_run["File External ID"].tolist()
                                        st.text("\n".join(file_list))
                                    else:
                                        st.write("No associated files found.")
                                else:
                                    st.info("No call_id found in run message.")
                        st.divider()

                total_pages = (len(filtered_runs) + items_per_page - 1) // items_per_page
                if total_pages > 1:
                    p_col1, p_col2, p_col3 = st.columns([1, 2, 1])
                    if p_col1.button("Previous", disabled=(st.session_state.page_num == 0), use_container_width=True):
                        st.session_state.page_num -= 1
                        st.rerun()
                    p_col2.markdown(
                        f"<div style='text-align: center; margin-top: 5px;'>Page {st.session_state.page_num + 1} of {total_pages}</div>",
                        unsafe_allow_html=True,
                    )
                    if p_col3.button(
                        "Next", disabled=(st.session_state.page_num >= total_pages - 1), use_container_width=True
                    ):
                        st.session_state.page_num += 1
                        st.rerun()
