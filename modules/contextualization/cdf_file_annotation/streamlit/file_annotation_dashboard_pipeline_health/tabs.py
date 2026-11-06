import pandas as pd
import streamlit as st
from components import (
    FileTableComponent,
    FunctionLogViewerComponent,
    KPIsComponent,
    RunHistoryComponent,
    ThroughputComponent,
)


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
        selected = FileTableComponent(self.annotation_states).render()

        if selected is None or selected.empty:
            return

        FunctionLogViewerComponent(self.client, selected.iloc[0].to_dict()).render()


class RunHistoryTab:
    def __init__(self, client, pipeline_runs: list, annotation_states, extraction_pipeline_cfg=None):
        self.client = client
        self.pipeline_runs = pipeline_runs
        self.annotation_states = annotation_states
        self.extraction_pipeline_cfg = extraction_pipeline_cfg

    def render(self) -> None:
        RunHistoryComponent(
            self.client, self.pipeline_runs, self.annotation_states, self.extraction_pipeline_cfg
        ).render()
