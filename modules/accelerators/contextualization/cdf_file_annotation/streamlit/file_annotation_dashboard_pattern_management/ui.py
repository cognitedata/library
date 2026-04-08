import uuid
from typing import Optional

import streamlit as st
from cognite.client import CogniteClient
from constants import FieldNames
from data_fetcher import DataFetcher
from data_structures import ExtractionPipelineConfig
from tabs import PatternManagementTab


class PatternManagementUI:
    def __init__(self, client: Optional[CogniteClient] = None):
        self.client = client or CogniteClient()

    def render(self):
        if "selected_pipeline" not in st.session_state:
            st.session_state.selected_pipeline = None
        if FieldNames.SESSION_MANUAL_PATTERNS_EDITOR_KEY_SNAKE_CASE not in st.session_state:
            st.session_state[FieldNames.SESSION_MANUAL_PATTERNS_EDITOR_KEY_SNAKE_CASE] = str(uuid.uuid4())
        if FieldNames.SESSION_MANUAL_PATTERNS_CHANGES_SNAKE_CASE not in st.session_state:
            st.session_state[FieldNames.SESSION_MANUAL_PATTERNS_CHANGES_SNAKE_CASE] = set()
        
        st.title("Pattern Management")
        st.write(st.__version__)
        st.caption("Manage patterns used in the File Annotation Pipeline. Choose a pipeline to begin.")

        st.write(f"Project selected: {self.client.config.project if self.client else 'No CDF client available yet'}")

        pipeline_ids = DataFetcher.find_pipelines(self.client)

        if not pipeline_ids:
            st.info("No active file annotation pipelines found to monitor.")
            return

        placeholder = "-- Select a Pipeline --"
        options = [placeholder] + pipeline_ids
        selected_pipeline = st.selectbox("Select a pipeline:", options=options, index=0, key="ph_pipeline")

        if not selected_pipeline or selected_pipeline == placeholder:
            st.info("Please select a pipeline from the dropdown above to load its data.")
            return

        last = st.session_state.get(FieldNames.SESSION_LAST_LOADED_PIPELINE_SNAKE_CASE)

        if last != selected_pipeline:
            st.session_state[FieldNames.SESSION_LAST_LOADED_PIPELINE_SNAKE_CASE] = selected_pipeline
            st.session_state.pop(FieldNames.SESSION_MANUAL_PATTERNS_LAST_UPDATE_TS_SNAKE_CASE, None)
            st.session_state.pop(FieldNames.SESSION_MANUAL_PATTERNS_LAST_UPDATE_UPSERTED_SNAKE_CASE, None)
            st.session_state.pop(FieldNames.SESSION_MANUAL_PATTERNS_LAST_UPDATE_DELETED_SNAKE_CASE, None)
            st.session_state.pop(FieldNames.SESSION_MANUAL_PATTERNS_LAST_UPDATE_SYNCED_SNAKE_CASE, None)
            st.session_state.pop(FieldNames.SESSION_MANUAL_PATTERNS_LAST_UPDATE_BY_SNAKE_CASE, None)
            st.session_state.pop(FieldNames.SESSION_PATTERN_FORGE_PREVIEW_DF_SNAKE_CASE, None)
            st.session_state.pop(FieldNames.SESSION_PATTERN_FORGE_GENERATED_SCOPES_SNAKE_CASE, None)
            st.session_state.pop(FieldNames.SESSION_PATTERN_FORGE_FINAL_PREVIEW_DF_SNAKE_CASE, None)
            st.session_state.pop(FieldNames.SESSION_PATTERN_FORGE_FINAL_PREVIEW_ROWS_SNAKE_CASE, None)
            st.session_state.pop(FieldNames.SESSION_MANUAL_PATTERNS_IMPORT_PREVIEW_DF_SNAKE_CASE, None)
            st.session_state.pop(FieldNames.SESSION_MANUAL_PATTERNS_IMPORT_PREVIEW_BACKUP_DF_SNAKE_CASE, None)

        with st.spinner(f"Loading pipeline configuration for '{selected_pipeline}'..."):
            pipeline_config = DataFetcher.load_pipeline_config(self.client, selected_pipeline)

        if not pipeline_config:
            st.error(f"Could not fetch configuration for pipeline: {selected_pipeline}")
            return

        extraction_pipeline_cfg = ExtractionPipelineConfig.from_dict(pipeline_config)

        tab_options = ["Pattern Management"]

        if FieldNames.SESSION_PATTERN_MANAGEMENT_TAB_SELECTOR_SNAKE_CASE not in st.session_state:
            st.session_state[FieldNames.SESSION_PATTERN_MANAGEMENT_TAB_SELECTOR_SNAKE_CASE] = tab_options[0]

        try:
            index = tab_options.index(st.session_state.get(FieldNames.SESSION_PATTERN_MANAGEMENT_TAB_SELECTOR_SNAKE_CASE, tab_options[0]))
        except ValueError:
            index = 0

        selected_tab = st.radio("Tabs", tab_options, index=index, horizontal=True, key=FieldNames.SESSION_PATTERN_MANAGEMENT_TAB_SELECTOR_SNAKE_CASE)

        if selected_tab == "Pattern Management":
            PatternManagementTab().render(self.client, extraction_pipeline_cfg)
