from config_registries import FunctionRunConfigRegistry
from data_fetcher import DataFetcher
from data_processor import DataProcessor
import streamlit as st
import pandas as pd
import altair as alt
from factories import FactoryHandler
from typing import Optional, List, Tuple, Callable, Union
from constants import FieldNames

from data_structures import KPI
from abc import ABC, abstractmethod


class Component(ABC):
    @abstractmethod
    def render(self) -> None:
        pass

class KPIsComponent(Component):
    def __init__(self, annotation_states: pd.DataFrame):
        self.annotation_states = annotation_states

    def calculate_kpis(self) -> KPI:
        df = self.annotation_states
        status_field = FieldNames.STATUS_LOWER_CASE

        awaiting = int((df[status_field] == "Awaiting").sum()) if not df.empty else 0
        processed_total = int(df[df[status_field].isin(["Annotated", "Failed"])].shape[0]) if not df.empty else 0
        failed_total = int((df[status_field] == "Failed").sum()) if not df.empty else 0
        failure_rate = (failed_total / processed_total * 100) if processed_total > 0 else 0.0

        return KPI(awaiting_processing=awaiting, processed_total=processed_total, failed_total=failed_total, failure_rate_total=failure_rate)

    def render(self) -> None:
        st.subheader("Live Pipeline KPIs")
        kpis = self.calculate_kpis()
        c1, c2, c3 = st.columns(3)

        c1.metric("Files Awaiting Processing", f"{kpis.awaiting_processing:,}")
        c2.metric("Total Files Processed", f"{kpis.processed_total:,}")
        c3.metric(
            "Overall Failure Rate",
            f"{kpis.failure_rate_total:.2f}%",
            delta=f"{kpis.failed_total:,} failed files",
            delta_color="inverse" if kpis.failed_total > 0 else "off",
        )


class ThroughputComponent(Component):
    def __init__(self, annotation_states: pd.DataFrame):
        self.annotation_states = annotation_states

    def render(self) -> None:
        st.subheader("Pipeline Throughput")
        time_agg = st.radio("Aggregate by:", options=["Daily", "Hourly", "Weekly"], horizontal=True, key="ph_time_agg")
        df = self.annotation_states

        if df.empty:
            st.info("No annotation state data to show throughput.")
            return

        status_field = FieldNames.STATUS_LOWER_CASE
        df_finalized = df[df[status_field].isin(["Annotated", "Failed"])].copy()

        if df_finalized.empty:
            st.info("No files have been finalized yet.")
            return

        time_bucket_field = FieldNames.TIME_BUCKET_SNAKE_CASE
        last_updated_field = FieldNames.LAST_UPDATED_TIME_CAMEL_CASE

        if time_agg == "Hourly":
            df_finalized[time_bucket_field] = df_finalized[last_updated_field].dt.floor("H")
        elif time_agg == "Weekly":
            df_finalized[time_bucket_field] = df_finalized[last_updated_field].dt.to_period("W").apply(lambda p: p.start_time)
        else:
            df_finalized[time_bucket_field] = df_finalized[last_updated_field].dt.date

        counts = df_finalized.groupby(time_bucket_field).size().reset_index(name="count")
        chart = alt.Chart(counts).mark_bar().encode(
            x=alt.X(f"{time_bucket_field}:T", title=f"Time ({time_agg})"),
            y=alt.Y("count:Q", title="Number of Files Finalized"),
            tooltip=[f"{time_bucket_field}:T", "count:Q"],
        ).properties(title=f"Files Finalized {time_agg}").interactive()

        st.altair_chart(chart, width="stretch")


class FileTableComponent(Component):
    def __init__(self, annotation_states: pd.DataFrame):
        self.annotation_states = annotation_states.copy()

    def render(self) -> Optional[pd.DataFrame]:
        st.subheader("File-Centric Debugging")

        df = self.annotation_states

        if df.empty:
            st.info("No annotation state data found for this pipeline.")
            return pd.DataFrame()

        df_display = df.sort_values(by=FieldNames.LAST_UPDATED_TIME_CAMEL_CASE, ascending=False).reset_index(drop=True)
        df_display.insert(0, FieldNames.SELECT_TITLE_CASE, False)

        previously_selected_idx = st.session_state.get("selected_status_file_index")
        if previously_selected_idx is not None and 0 <= previously_selected_idx < len(df_display):
            df_display.at[previously_selected_idx, FieldNames.SELECT_TITLE_CASE] = True

        default_columns = [
            FieldNames.SELECT_TITLE_CASE,
            FieldNames.FILE_NAME_CAMEL_CASE,
            FieldNames.LAST_UPDATED_TIME_CAMEL_CASE,
            FieldNames.FILE_EXTERNAL_ID_CAMEL_CASE,
            FieldNames.FILE_SOURCE_ID_CAMEL_CASE,
            FieldNames.STATUS_LOWER_CASE,
            FieldNames.FILE_MIME_TYPE_CAMEL_CASE,
            FieldNames.PAGE_COUNT_CAMEL_CASE,
            FieldNames.ANNOTATED_PAGE_COUNT_CAMEL_CASE,
        ]

        all_columns = df_display.columns.tolist()

        with st.popover("Customize Table Columns"):
            selected_columns = st.multiselect(
                "Select columns to display:", options=all_columns, default=[col for col in default_columns if col in all_columns]
            )

        if not selected_columns:
            st.warning("Please select at least one column to display.")
            st.stop()

        if FieldNames.SELECT_TITLE_CASE not in selected_columns:
            selected_columns = [FieldNames.SELECT_TITLE_CASE] + selected_columns

        editor_key = "status_table_editor"

        handler = FactoryHandler.make_single_selection_handler(editor_key=editor_key, selected_index_state_key="selected_status_file_index")
            
        st.data_editor(
            df_display[selected_columns],
            key=editor_key,
            column_config={
                FieldNames.SELECT_TITLE_CASE: st.column_config.CheckboxColumn(required=True)
            },
            width="stretch",
            hide_index=True,
            disabled=df_display.columns.difference([FieldNames.SELECT_TITLE_CASE]),
            on_change=handler,
        )

        return df_display


class TabbedComponent(Component):
    def __init__(self, tabs: List[Tuple[str, Union[Component, Callable[[], Component]]]]):
        self.tabs = tabs

    def render(self) -> None:
        labels = [label for label, _ in self.tabs]
        tab_widgets = st.tabs(labels)

        for widget, (label, comp_or_factory) in zip(tab_widgets, self.tabs):
            with widget:
                component = comp_or_factory() if callable(comp_or_factory) else comp_or_factory
                if component is None:
                    continue
                component.render()


class LogTabComponent(Component):
    def __init__(self, client, selected_row: dict, function_run_config):
        self.client = client
        self.selected_row = selected_row
        self.function_run_config = function_run_config

    def render(self):
        file_ext_id = self.selected_row.get(FieldNames.FILE_EXTERNAL_ID_CAMEL_CASE, "")
        func_id_raw = self.selected_row.get(self.function_run_config.function_id_field)
        call_id_raw = self.selected_row.get(self.function_run_config.function_call_id_field)

        if pd.notna(func_id_raw) and pd.notna(call_id_raw):
            with st.spinner(f"Fetching {self.function_run_config.log_title} {file_ext_id}..."):
                try:
                    func_id = int(func_id_raw)
                    call_id = int(call_id_raw)
                except Exception:
                    st.warning("Could not parse function id / call id for this file.")
                    return
                logs_raw = DataFetcher.fetch_function_logs(self.client, function_id=func_id, call_id=call_id)
            if logs_raw:
                relevant = ""
                try:
                    relevant = DataProcessor.filter_log_lines(logs_raw, file_ext_id)
                except Exception:
                    relevant = ""

                if relevant:
                    st.write("**Relevant Log Entries:**")
                    st.code(relevant, language="log")
                else:
                    st.info("No relevant log entries found for this specific file.")

                st.download_button("Download Full Log", logs_raw, f"{file_ext_id}_{self.function_run_config.log_snake_case}.txt")
                with st.expander("View Full Log"):
                    st.code(logs_raw, language="log")
            else:
                st.warning("No logs found for this function call.")
        else:
            st.warning("No function call id found for this file.")


class FunctionLogViewerComponent(Component):
    def __init__(self, client, selected_row: dict):
        self.client = client
        self.selected_row = selected_row

    def render(self) -> None:
        st.subheader("Function Log Viewer")
        configs = FunctionRunConfigRegistry.get_available_function_configs_for_row(self.selected_row)
        if not configs:
            st.info("No function runs available for this file.")
            return

        tabs = [(cfg.log_title, LogTabComponent(self.client, self.selected_row, cfg)) for cfg in configs]
        TabbedComponent(tabs).render()


class RunHistoryComponent(Component):
    def __init__(self, client, pipeline_runs: list, annotation_states: pd.DataFrame, extraction_pipeline_cfg=None):
        self.client = client
        self.pipeline_runs = pipeline_runs
        self.annotation_states = annotation_states
        self.extraction_pipeline_cfg = extraction_pipeline_cfg

    def render(self) -> None:
        st.subheader("Run-Centric Analysis")
        if not self.pipeline_runs:
            st.info("No pipeline runs found for this pipeline.")
            return

        RunSummaryMetricsComponent(self.pipeline_runs).render()
        st.divider()
        RunChartsComponent(self.pipeline_runs).render()
        st.divider()
        DetailedRunHistoryComponent(self.client, self.pipeline_runs, self.annotation_states, self.extraction_pipeline_cfg).render()


class RunSummaryMetricsComponent(Component):
    def __init__(self, pipeline_runs: list):
        self.pipeline_runs = pipeline_runs

    def render(self) -> None:
        df_runs_for_graphing = DataProcessor.process_runs_for_graphing(self.pipeline_runs)
        totals_by_type = {}
        success_counts = {}
        failure_counts = {}

        configs = FunctionRunConfigRegistry.get_all_configs()

        for cfg in configs:
            typ = cfg.caller_type.value
            totals_by_type[typ] = int(df_runs_for_graphing[df_runs_for_graphing["type"] == typ]["count"].sum()) if not df_runs_for_graphing.empty else 0
            success_counts[typ] = 0
            failure_counts[typ] = 0

        for run in self.pipeline_runs:
            try:
                parsed = DataProcessor.parse_run_message(getattr(run, FieldNames.MESSAGE_LOWER_CASE, ""))
                caller = parsed.get(FieldNames.CALLER_LOWER_CASE)
            except Exception:
                caller = None

            if not caller:
                continue
            if caller not in success_counts:
                continue
            if run.status == FieldNames.SUCCESS_LOWER_CASE:
                success_counts[caller] += 1
            elif run.status == FieldNames.FAILURE_LOWER_CASE:
                failure_counts[caller] += 1

        configs_to_show = [cfg.caller_type.value for cfg in configs]
        pairs = [configs_to_show[i : i + 2] for i in range(0, len(configs_to_show), 2)]
        for pair in pairs:
            cols = st.columns(len(pair))
            for col, typ in zip(cols, pair):
                with col:
                    st.subheader(f"{typ} Runs")
                    m1, m2, m3 = st.columns(3)
                    m1.metric(FieldNames.FILES_PROCESSED_TITLE_CASE, f"{totals_by_type.get(typ, 0):,}")
                    m2.metric(FieldNames.SUCCESSFULL_RUNS_TITLE_CASE, f"{success_counts.get(typ, 0):,}")
                    m3.metric(FieldNames.FAILED_RUNS_TITLE_CASE, f"{failure_counts.get(typ, 0):,}", delta=f"{failure_counts.get(typ, 0):,}" if failure_counts.get(typ, 0) > 0 else "0", delta_color="inverse")

class RunChartsComponent(Component):
    def __init__(self, pipeline_runs: list):
        self.pipeline_runs = pipeline_runs

    def render(self) -> None:
        df_runs_for_graphing = DataProcessor.process_runs_for_graphing(self.pipeline_runs)
        if df_runs_for_graphing.empty:
            st.info("No run metrics available to plot.")
            return

        base_chart = (
            alt.Chart(df_runs_for_graphing)
            .mark_circle(size=60, opacity=0.7)
            .encode(
                x=alt.X(f"{FieldNames.TIMESTAMP_LOWER_CASE}:T", title="Time of Run"),
                y=alt.Y(f"{FieldNames.COUNT_LOWER_CASE}:Q", title=FieldNames.FILES_PROCESSED_TITLE_CASE),
                tooltip=[f"{FieldNames.TIMESTAMP_LOWER_CASE}:T", f"{FieldNames.COUNT_LOWER_CASE}:Q", f"{FieldNames.TYPE_LOWER_CASE}:N"],
            )
            .interactive()
        )

        configs = FunctionRunConfigRegistry.get_all_configs()
        chart_titles = [cfg.caller_type.value for cfg in configs]
        charts = [base_chart.transform_filter(alt.datum.type == title).properties(title=f"Files Processed per {title} Run") for title in chart_titles]

        if charts:
            cols = st.columns(2)
            for i, ch in enumerate(charts):
                with cols[i % 2]:
                    st.altair_chart(ch, width="stretch")


class DetailedRunHistoryComponent(Component):
    def __init__(self, client, pipeline_runs: list, annotation_states: pd.DataFrame, extraction_pipeline_cfg=None):
        self.pipeline_runs = pipeline_runs
        self.annotation_states = annotation_states
        self.client = client
        self.extraction_pipeline_cfg = extraction_pipeline_cfg

    def render(self) -> None:
        st.subheader("Detailed Run History")

        if not self.pipeline_runs:
            st.info("No runs available.")
            return

        time_window_map = {"All": None, "Last 24 Hours": 24, "Last 7 Days": 7 * 24, "Last 30 Days": 30 * 24}
        time_window_option = st.selectbox("Filter by Time Window:", options=list(time_window_map.keys()), key="run_time_window")
        window_hours = time_window_map[time_window_option]

        if window_hours is not None:
            now = pd.Timestamp.now(tz="UTC")
            start = now - pd.Timedelta(hours=window_hours)
            recent_runs = [r for r in self.pipeline_runs if pd.to_datetime(r.created_time, unit="ms").tz_localize("UTC") > start]
        else:
            recent_runs = self.pipeline_runs

        if not recent_runs:
            st.warning("No pipeline runs found in the selected time window.")
            return

        f_col1, f_col2 = st.columns(2)
        run_status_filter = f_col1.radio("Filter by run status:", ("All", "Success", "Failure"), horizontal=True, key="run_status_filter")
        caller_options = ["All"] + [cfg.caller_type.value for cfg in FunctionRunConfigRegistry.get_all_configs()]
        caller_type_filter = f_col2.radio("Filter by caller type:", tuple(caller_options), horizontal=True, key="caller_type_filter")

        filtered_runs = recent_runs

        if run_status_filter != "All":
            filtered_runs = [run for run in filtered_runs if run.status.lower() == run_status_filter.lower()]
        if caller_type_filter != "All":
            filtered_runs = [
                run
                for run in filtered_runs
                if DataProcessor.parse_run_message(getattr(run, FieldNames.MESSAGE_LOWER_CASE, "")).get(FieldNames.CALLER_LOWER_CASE) == caller_type_filter
            ]

        if not filtered_runs:
            st.warning("No runs match the selected filters.")
            return

        items_per_page = 5
        start_idx = st.session_state.page_num * items_per_page
        end_idx = start_idx + items_per_page
        paginated_runs = filtered_runs[start_idx:end_idx]

        if not paginated_runs:
            st.warning("No runs match the selected filters.")
            return

        for run in paginated_runs:
            st.markdown(f"**Status:** {run.status.capitalize()} at {pd.to_datetime(run.created_time, unit='ms').tz_localize('UTC').strftime('%Y-%m-%d %H:%M:%S')}")
            st.code(getattr(run, FieldNames.MESSAGE_LOWER_CASE, ""), language="text")

            parsed_message = DataProcessor.parse_run_message(getattr(run, FieldNames.MESSAGE_LOWER_CASE, ""))

            function_id_str = parsed_message.get(FieldNames.FUNCTION_ID_SNAKE_CASE)
            call_id_str = parsed_message.get(FieldNames.CALL_ID_SNAKE_CASE)

            expander_col1, expander_col2 = st.columns(2)

            with expander_col1:
                with st.expander("View Function Log"):
                    st.write("**Function Log**")

                    if st.button("Load Log", key=f"load_btn_{run.id}"):
                        with st.spinner("Fetching logs..."):
                            func_id = int(function_id_str)
                            cid = int(call_id_str)
                            logs = DataFetcher.fetch_function_logs(self.client, function_id=func_id, call_id=cid)

                        if logs:
                            st.download_button("Download Log", logs, f"run_{run.id}_log.txt")
                            st.code(logs, language="log")
                        else:
                            st.info("No logs found for this run.")

            with expander_col2:
                with st.expander("View Files Processed"):
                    st.write("External ID(s):")
                    files_from_service = []

                    if call_id_str:
                        cid = int(call_id_str)
                    else:
                        cid = None

                    if cid is None:
                        st.write("No call_id present for this run.")
                    else:
                        caller = parsed_message.get(FieldNames.CALLER_LOWER_CASE)
                        ann_view = None

                        if hasattr(self, "extraction_pipeline_cfg") and self.extraction_pipeline_cfg:
                            ann_view = getattr(self.extraction_pipeline_cfg, "annotation_state_view_cfg", None)
                        if ann_view is None and hasattr(self, "annotation_state_view"):
                            ann_view = getattr(self, "annotation_state_view")

                        files_from_service = DataFetcher.fetch_files_by_function_call_id(self.client, cid, ann_view, caller_type=caller)

                    if files_from_service:
                        st.text("\n".join(files_from_service))
                    else:
                        st.write("No associated files found.")

            st.divider()

        total_pages = (len(filtered_runs) + items_per_page - 1) // items_per_page
        if total_pages > 1:
            p_col1, p_col2, p_col3 = st.columns([1, 2, 1])
            if p_col1.button("Previous", disabled=(st.session_state.page_num == 0), width="stretch"):
                st.session_state.page_num -= 1
                st.rerun()
            p_col2.markdown(
                f"<div style='text-align: center; margin-top: 5px;'>Page {st.session_state.page_num + 1} of {total_pages}</div>",
                unsafe_allow_html=True,
            )
            if p_col3.button("Next", disabled=(st.session_state.page_num >= total_pages - 1), width="stretch"):
                st.session_state.page_num += 1
                st.rerun()