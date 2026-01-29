import streamlit as st
from components import KPIsComponent, ThroughputComponent, FileTableComponent, FunctionLogViewerComponent, RunHistoryComponent
import pandas as pd


class OverviewTab:
    def __init__(self, client, annotation_states: pd.DataFrame):
        self.client = client
        self.annotation_states = annotation_states

    def render(self) -> None:
        KPIsComponent(self.annotation_states).render()
        st.divider()
        ThroughputComponent(self.annotation_states).render()

class FileExplorerTab:
    def __init__(self, client, annotation_states: pd.DataFrame, extraction_pipeline_cfg=None):
        self.client = client
        self.annotation_states = annotation_states
        self.extraction_pipeline_cfg = extraction_pipeline_cfg

    def render(self) -> None:
        edited = FileTableComponent(self.annotation_states).render()

        if edited is None or edited.empty:
            return

        selected_indices = edited[edited.Select].index.tolist()
        if not selected_indices:
            return

        selected_index = selected_indices[0]
        selected_row = self.annotation_states.sort_values(by="lastUpdatedTime", ascending=False).reset_index(drop=True).iloc[selected_index].to_dict()
        FunctionLogViewerComponent(self.client, selected_row).render()


class RunHistoryTab:
    def __init__(self, client, pipeline_runs: list, annotation_states, extraction_pipeline_cfg=None):
        self.client = client
        self.pipeline_runs = pipeline_runs
        self.annotation_states = annotation_states
        self.extraction_pipeline_cfg = extraction_pipeline_cfg

    def render(self) -> None:
        RunHistoryComponent(self.client, self.pipeline_runs, self.annotation_states, self.extraction_pipeline_cfg).render()
