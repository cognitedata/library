from typing import Optional
import streamlit as st
import pandas as pd
from cognite.client import CogniteClient
from data_fetcher import DataFetcher
from data_processor import DataProcessor
from tabs import PerFileTab, OverallTab, PatternManagementTab
from data_structures import ExtractionPipelineConfig
from constants import FieldNames
import uuid


class AnnotationQualityUI:
    def __init__(self, client: Optional[CogniteClient] = None):
        self.client = client or CogniteClient()

    def render(self):
        if "selected_pipeline" not in st.session_state:
            st.session_state.selected_pipeline = None
        if "manual_patterns_editor_key" not in st.session_state:
            st.session_state.manual_patterns_editor_key = str(uuid.uuid4())
        if "manual_patterns_changes" not in st.session_state:
            st.session_state.manual_patterns_changes = set()
        
        st.title("Annotation Quality")
        st.caption("See the quality metrics, manage patterns and promote annotations manually. Choose a pipeline to begin.")

        st.write(f"Project selected: {self.client.config.project if self.client else 'No CDF client available yet'}")

        pipeline_ids = DataFetcher.find_pipelines(self.client, "files_annotation")

        if not pipeline_ids:
            st.info("No active file annotation pipelines found to monitor.")
            return

        placeholder = "-- Select a Pipeline --"
        options = [placeholder] + pipeline_ids
        selected_pipeline = st.selectbox("Select a pipeline:", options=options, index=0, key="ph_pipeline")

        if not selected_pipeline or selected_pipeline == placeholder:
            st.info("Please select a pipeline from the dropdown above to load its data.")
            return

        last = st.session_state.get("last_loaded_pipeline")

        if last != selected_pipeline:
            st.session_state["last_loaded_pipeline"] = selected_pipeline

        with st.spinner(f"Loading pipeline configuration for '{selected_pipeline}'..."):
            pipeline_config = DataFetcher.load_pipeline_config(self.client, selected_pipeline)

        if not pipeline_config:
            st.error(f"Could not fetch configuration for pipeline: {selected_pipeline}")
            return

        extraction_pipeline_cfg = ExtractionPipelineConfig.from_dict(pipeline_config)

        with st.spinner(f"Loading annotations and metadata for pipeline '{selected_pipeline}'..."):
            annotation_frames = DataFetcher.fetch_annotations(self.client, extraction_pipeline_cfg)
            files_metadata = DataFetcher.fetch_entities_metadata(self.client, extraction_pipeline_cfg=extraction_pipeline_cfg, entity_type=FieldNames.FILE_TITLE_CASE)
            annotation_frames = DataProcessor.enrich_annotation_frames_with_files_metadata(annotation_frames, files_metadata)

        overall_tab, per_file_tab, pattern_management_tab = st.tabs(["Overall Quality Metrics", "Per-File Analysis", "Pattern Management"])

        with overall_tab:
            OverallTab().render(self.client, extraction_pipeline_cfg, actual_df=annotation_frames.actual_df, potential_df=annotation_frames.potential_df)
        with per_file_tab:
            PerFileTab().render(self.client, extraction_pipeline_cfg, actual_df=annotation_frames.actual_df, potential_df=annotation_frames.potential_df)
        with pattern_management_tab:
            PatternManagementTab().render(self.client, extraction_pipeline_cfg)

        if "initial_rerun_done" not in st.session_state:
            st.session_state.initial_rerun_done = True
            st.rerun()