import streamlit as st
import pandas as pd
from components import (
    OverallAnnotationCoverageComponent,
    PatternCatalogComponent,
    AnnotationComparisonComponent,
    ManualPromotingComponent,
    TagEntityResourceTypeCoverageComponent,
    FileResourceTypeCoverageComponent,
    SecondaryScopeCoverageComponent,
    PerFileFiltersComponent,
    FileAggregationComponent,
) 

class OverallTab:
    def render(self, client, extraction_pipeline_cfg, actual_df: pd.DataFrame, potential_df: pd.DataFrame) -> None:
        OverallAnnotationCoverageComponent(actual_df=actual_df, potential_df=potential_df).render()
        st.divider()
        TagEntityResourceTypeCoverageComponent(actual_df=actual_df, potential_df=potential_df).render()
        FileResourceTypeCoverageComponent(extraction_pipeline_cfg, actual_df=actual_df, potential_df=potential_df).render()
        SecondaryScopeCoverageComponent(extraction_pipeline_cfg, actual_df=actual_df, potential_df=potential_df).render()

class PerFileTab:
    def render(self, client, extraction_pipeline_cfg, actual_df: pd.DataFrame, potential_df: pd.DataFrame) -> None:
        PerFileFiltersComponent(extraction_pipeline_cfg, actual_df=actual_df, potential_df=potential_df).render()
        FileAggregationComponent(extraction_pipeline_cfg, actual_df=actual_df, potential_df=potential_df).render()
        AnnotationComparisonComponent(extraction_pipeline_cfg, actual_df=actual_df, potential_df=potential_df).render()
        ManualPromotingComponent(client, extraction_pipeline_cfg, actual_df, potential_df).render()

class PatternManagementTab:
    def render(self, client, extraction_pipeline_cfg) -> None:
        PatternCatalogComponent(client, extraction_pipeline_cfg).render()
        st.divider()