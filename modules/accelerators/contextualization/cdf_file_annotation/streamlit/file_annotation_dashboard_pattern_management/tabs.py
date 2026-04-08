import streamlit as st
from components import (
    PatternCatalogComponent,
)


class PatternManagementTab:
    def render(self, client, extraction_pipeline_cfg) -> None:
        PatternCatalogComponent(client, extraction_pipeline_cfg).render()
        st.divider()

