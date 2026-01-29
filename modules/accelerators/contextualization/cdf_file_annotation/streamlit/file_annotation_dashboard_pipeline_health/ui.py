import streamlit as st
from cognite.client import CogniteClient
from data_fetcher import DataFetcher
from data_processor import DataProcessor
from tabs import OverviewTab, FileExplorerTab, RunHistoryTab
from data_structures import ExtractionPipelineConfig, ViewPropertyConfig
from constants import FieldNames


class PipelineHealthUI:
    def __init__(self, client: CogniteClient | None = None):
        self.client = client or CogniteClient()

    def render(self):
        if "selected_pipeline" not in st.session_state:
            st.session_state.selected_pipeline = None
        if "selected_status_file_index" not in st.session_state:
            st.session_state.selected_status_file_index = None
        if "page_num" not in st.session_state:
            st.session_state.page_num = 0

        st.title("Pipeline Health")
        st.caption("Monitor pipeline throughput, recent runs and function logs. Choose a pipeline to begin.")

        st.write(f"Project selected: {self.client.config.project if self.client else 'No CDF client available yet'}")

        pipeline_ids = DataFetcher.find_pipelines(self.client)

        if not pipeline_ids:
            st.info("No active file annotation pipelines found to monitor.")
            return

        placeholder = "-- Select a Pipeline --"
        options = [placeholder] + pipeline_ids
        selected_pipeline = st.selectbox("Select a pipeline:", options=options, index=0, key="ph_pipeline")

        if not selected_pipeline or selected_pipeline == placeholder:
            st.session_state.selected_pipeline = None
            st.info("Please select a pipeline from the dropdown above to load its data.")
            return

        last = st.session_state.get("last_loaded_pipeline")

        if last != selected_pipeline:
            st.session_state["last_loaded_pipeline"] = selected_pipeline
            st.session_state["selected_status_file_index"] = None

        with st.spinner(f"Loading pipeline configuration for '{selected_pipeline}'..."):
            pipeline_config = DataFetcher.load_pipeline_config(self.client, selected_pipeline)

        if not pipeline_config:
            st.error(f"Could not fetch configuration for pipeline: {selected_pipeline}")
            return

        extraction_pipeline_cfg = ExtractionPipelineConfig.from_dict(pipeline_config)

        with st.spinner(f"Fetching annotation states from view '{selected_pipeline}'..."):
            df_annotation_states = DataFetcher.fetch_annotation_states(self.client, extraction_pipeline_cfg)

        with st.spinner(f"Fetching pipeline run history for '{selected_pipeline}'..."):
            pipeline_runs = DataFetcher.fetch_pipeline_run_history(self.client, selected_pipeline)

        overview_tab, file_explorer_tab, run_history_tab = st.tabs(["Overview", "File Explorer", "Run History"])

        with overview_tab:
            OverviewTab(self.client, df_annotation_states).render()
        with file_explorer_tab:
            FileExplorerTab(self.client, df_annotation_states, extraction_pipeline_cfg).render()
        with run_history_tab:
            RunHistoryTab(self.client, pipeline_runs, df_annotation_states, extraction_pipeline_cfg).render()

        if "initial_rerun_done" not in st.session_state:
            st.session_state.initial_rerun_done = True
            st.rerun()