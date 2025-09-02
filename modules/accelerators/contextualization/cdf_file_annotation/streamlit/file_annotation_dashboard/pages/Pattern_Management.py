import streamlit as st
import pandas as pd
from datetime import datetime, timezone

from helper import (
    fetch_extraction_pipeline_config,
    find_pipelines,
    fetch_manual_patterns,
    save_manual_patterns,
)

st.set_page_config(page_title="Pattern Management", page_icon="✏️", layout="wide")

st.title("Pattern Management")
st.markdown("Add, edit, or delete manual patterns to improve the quality of the pattern detection job.")

# --- Sidebar for Pipeline Selection ---
st.sidebar.title("Pipeline Selection")
pipeline_ids = find_pipelines()

if not pipeline_ids:
    st.info("No active file annotation pipelines found to monitor.")
    st.stop()

selected_pipeline = st.sidebar.selectbox(
    "Select a pipeline to manage patterns for:", options=pipeline_ids, key="pattern_pipeline_selector"
)

# --- Data Fetching ---
config_result = fetch_extraction_pipeline_config(selected_pipeline)
if not config_result:
    st.error(f"Could not fetch configuration for pipeline: {selected_pipeline}")
    st.stop()

ep_config, _, _ = config_result
cache_config = ep_config.get("launchFunction", {}).get("cacheService", {})
db_name = cache_config.get("rawDb")
manual_patterns_table = cache_config.get("rawManualPatternsCatalog")
primary_scope_prop = ep_config.get("launchFunction", {}).get("primaryScopeProperty")
secondary_scope_prop = ep_config.get("launchFunction", {}).get("secondaryScopeProperty")


if not all([db_name, manual_patterns_table]):
    st.error("RAW DB name or manual patterns table name is not configured in the extraction pipeline.")
    st.stop()

# --- Load and Display Existing Patterns ---
st.subheader("Existing Manual Patterns")

df_patterns = fetch_manual_patterns(db_name, manual_patterns_table)

edited_df = st.data_editor(
    df_patterns,
    num_rows="dynamic",
    use_container_width=True,
    column_config={
        "key": st.column_config.TextColumn("Scope Key", disabled=True),
        "sample": st.column_config.TextColumn("Pattern String", required=True),
        "resource_type": st.column_config.TextColumn("Resource Type", required=True),
        "scope_level": st.column_config.TextColumn("Scope Level", required=True),
        "primary_scope": st.column_config.TextColumn("Primary Scope", required=False),
        "secondary_scope": st.column_config.TextColumn("Secondary Scope", required=False),
        "created_by": st.column_config.TextColumn("Created By", required=True),
    },
)

if st.button("Save Changes", type="primary"):
    with st.spinner("Saving changes to RAW..."):
        try:
            save_manual_patterns(edited_df, db_name, manual_patterns_table)
            st.success("Changes saved successfully!")
            st.cache_data.clear()
            st.rerun()
        except Exception as e:
            st.error(f"Failed to save changes: {e}")


st.divider()

# --- Add New Pattern Form ---
st.subheader("Add a New Pattern")

scope_level = st.selectbox(
    "1. Select Scope Level", ["Global", "Primary Scope", "Secondary Scope"], key="scope_level_selector"
)

with st.form(key="new_pattern_form", clear_on_submit=True):
    st.write("2. Enter Pattern Details")
    new_pattern = st.text_input("Pattern String", placeholder="e.g., [PI]-00000")
    new_resource_type = st.text_input("Resource Type", placeholder="e.g., Asset")

    primary_scope_value = ""
    if scope_level in ["Primary Scope", "Secondary Scope"]:
        primary_scope_value = st.text_input(f"Primary Scope Value ({primary_scope_prop or 'not configured'})")

    secondary_scope_value = ""
    if scope_level == "Secondary Scope":
        secondary_scope_value = st.text_input(f"Secondary Scope Value ({secondary_scope_prop or 'not configured'})")

    submit_button = st.form_submit_button(label="Add New Pattern")

    if submit_button:
        if not all([new_pattern, new_resource_type]):
            st.warning("Pattern String and Resource Type are required.")
        else:
            with st.spinner("Adding new pattern..."):
                try:
                    updated_df = pd.concat(
                        [
                            edited_df,
                            pd.DataFrame(
                                [
                                    {
                                        "sample": new_pattern,
                                        "resource_type": new_resource_type,
                                        "scope_level": scope_level,
                                        "primary_scope": primary_scope_value,
                                        "secondary_scope": secondary_scope_value,
                                        "created_by": "streamlit",
                                    }
                                ]
                            ),
                        ],
                        ignore_index=True,
                    )

                    save_manual_patterns(updated_df, db_name, manual_patterns_table)
                    st.success("New pattern added successfully!")
                    st.cache_data.clear()
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to add pattern: {e}")
