import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime, timezone
from helper import (
    fetch_extraction_pipeline_config,
    fetch_raw_table_data,
    find_pipelines,
    generate_file_canvas,
    fetch_pattern_catalog,
    fetch_manual_patterns,
    fetch_annotation_states,
    save_manual_patterns,
    normalize,
)
from cognite.client.data_classes.data_modeling import NodeId

# --- Page Configuration ---
st.set_page_config(
    page_title="Annotation Quality",
    page_icon="🎯",
    layout="wide",
)


# --- Callback function to reset selection ---
def reset_selection():
    st.session_state.selected_row_index = None


# --- Initialize Session State ---
if "selected_row_index" not in st.session_state:
    st.session_state.selected_row_index = None

# --- Sidebar for Pipeline Selection ---
st.sidebar.title("Pipeline Selection")
pipeline_ids = find_pipelines()

if not pipeline_ids:
    st.info("No active file annotation pipelines found to monitor.")
    st.stop()

selected_pipeline = st.sidebar.selectbox("Select a pipeline:", options=pipeline_ids, key="quality_pipeline_selector")

# --- Data Fetching & Processing ---
config_result = fetch_extraction_pipeline_config(selected_pipeline)
if not config_result:
    st.error(f"Could not fetch configuration for pipeline: {selected_pipeline}")
    st.stop()

ep_config, view_config = config_result

annotation_state_view = view_config["annotation_state"]
file_view = view_config["file"]
target_entities_view = view_config["target_entities"]

report_config = ep_config.get("finalizeFunction", {}).get("reportService", {})
cache_config = ep_config.get("launchFunction", {}).get("cacheService", {})
db_name = report_config.get("rawDb")
pattern_table = report_config.get("rawTableDocPattern")
tag_table = report_config.get("rawTableDocTag")
doc_table = report_config.get("rawTableDocDoc")
cache_table = cache_config.get("rawTableCache")
manual_patterns_table = cache_config.get("rawManualPatternsCatalog")
file_resource_property = ep_config.get("launchFunction", {}).get("fileResourceProperty", "")
target_entities_resource_property = ep_config.get("launchFunction", {}).get("targetEntitiesResourceProperty", "")

if not all([db_name, pattern_table, tag_table, doc_table, cache_table, manual_patterns_table]):
    st.error("Could not find all required RAW table names in the pipeline configuration.")
    st.stop()

# --- Main Application ---
st.title("Annotation Quality Dashboard")

# --- Create Tabs ---
overall_tab, per_file_tab, management_tab = st.tabs(
    ["Overall Quality Metrics", "Per-File Analysis", "Pattern Management"]
)

# ==========================================
#         OVERALL QUALITY METRICS TAB
# ==========================================
with overall_tab:
    df_patterns = fetch_raw_table_data(db_name, pattern_table)
    df_tags = fetch_raw_table_data(db_name, tag_table)
    df_docs = fetch_raw_table_data(db_name, doc_table)

    if df_patterns.empty:
        st.info("The pattern catalog is empty. Run the pipeline with patternMode enabled to generate data.")
    else:
        df_annotations = pd.concat([df_tags, df_docs], ignore_index=True)

        st.subheader(
            "Overall Annotation Quality",
            help="Provides a high-level summary of pattern performance across all files. Use these aggregate metrics, charts, and tag lists to understand the big picture and identify systemic trends or gaps in the pattern catalog.",
        )
        all_resource_types = ["All"] + sorted(df_patterns["endNodeResourceType"].unique().tolist())
        selected_resource_type = st.selectbox(
            "Filter by Resource Type:",
            options=all_resource_types,
            on_change=reset_selection,
            key="resource_type_filter",
        )

        if selected_resource_type == "All":
            df_metrics_input = df_patterns
            df_annotations_input = df_annotations
        else:
            df_metrics_input = df_patterns[df_patterns["endNodeResourceType"] == selected_resource_type]
            if not df_annotations.empty and "endNodeResourceType" in df_annotations.columns:
                df_annotations_input = df_annotations[df_annotations["endNodeResourceType"] == selected_resource_type]
            else:
                df_annotations_input = pd.DataFrame()

        # 1. Get the original, un-normalized sets of strings
        potential_tags_original = set(df_metrics_input["startNodeText"])
        actual_annotations_original = (
            set(df_annotations_input["startNodeText"])
            if not df_annotations_input.empty and "startNodeText" in df_annotations_input.columns
            else set()
        )

        # 2. Create a mapping from normalized text back to an original version for display
        text_map = {
            normalize(text): text for text in potential_tags_original.union(actual_annotations_original) if text
        }

        # 3. Create fully normalized sets for logical comparison
        normalized_potential_set = {normalize(t) for t in potential_tags_original}
        normalized_actual_set = {normalize(t) for t in actual_annotations_original}

        # 4. Perform all set operations on the normalized data for accurate logic
        normalized_unmatched = normalized_potential_set - normalized_actual_set

        # 5. Use the map to get the final sets with original text for display
        actual_annotations_set = {text_map[t] for t in normalized_actual_set if t in text_map}
        potential_new_annotations_set = {text_map[t] for t in normalized_unmatched if t in text_map}

        total_actual = len(actual_annotations_set)
        total_potential = len(potential_new_annotations_set)

        overall_coverage = (
            (total_actual / (total_actual + total_potential)) * 100 if (total_actual + total_potential) > 0 else 0
        )

        st.metric(
            "Overall Annotation Coverage",
            f"{overall_coverage:.2f}%",
            help="The percentage of all unique tags (both actual and potential) that have been successfully annotated. Formula: Total Actual Annotations / (Total Actual Annotations + Total Potential New Annotations)",
        )

        st.divider()
        chart_data = []
        for resource_type in all_resource_types[1:]:
            df_patterns_filtered = df_patterns[df_patterns["endNodeResourceType"] == resource_type]
            df_annotations_filtered = (
                df_annotations[df_annotations["endNodeResourceType"] == resource_type]
                if not df_annotations.empty and "endNodeResourceType" in df_annotations.columns
                else pd.DataFrame()
            )
            potential = set(df_patterns_filtered["startNodeText"])
            actual = (
                set(df_annotations_filtered["startNodeText"])
                if not df_annotations_filtered.empty and "startNodeText" in df_annotations_filtered.columns
                else set()
            )
            # Use normalized comparison for chart data as well
            norm_potential = {normalize(p) for p in potential}
            norm_actual = {normalize(a) for a in actual}

            total_actual_rt = len(norm_actual)
            total_potential_rt = len(norm_potential - norm_actual)

            coverage = (
                (total_actual_rt / (total_actual_rt + total_potential_rt)) * 100
                if (total_actual_rt + total_potential_rt) > 0
                else 0
            )
            chart_data.append(
                {
                    "resourceType": resource_type,
                    "coverageRate": coverage,
                    "actualAnnotations": total_actual_rt,
                    "potentialNewAnnotations": total_potential_rt,
                }
            )

        df_chart_data = pd.DataFrame(chart_data)
        df_chart_display = (
            df_chart_data[df_chart_data["resourceType"] == selected_resource_type]
            if selected_resource_type != "All"
            else df_chart_data
        )

        if not df_chart_display.empty:
            coverage_chart = (
                alt.Chart(df_chart_display)
                .mark_bar()
                .encode(
                    x=alt.X("resourceType:N", title="Resource Type", sort="-y"),
                    y=alt.Y("coverageRate:Q", title="Annotation Coverage (%)", scale=alt.Scale(domain=[0, 100])),
                    tooltip=["resourceType", "coverageRate", "actualAnnotations", "potentialNewAnnotations"],
                )
                .properties(title="Annotation Coverage by Resource Type")
            )
            st.altair_chart(coverage_chart, use_container_width=True)

        else:
            st.info("No data available for the selected resource type to generate charts.")

        st.divider()
        # --- Pattern Catalog ---
        with st.expander("View Full Pattern Catalog"):
            df_auto_patterns = fetch_pattern_catalog(db_name, cache_table)
            df_manual_patterns = fetch_manual_patterns(db_name, manual_patterns_table)

            df_auto_patterns.rename(columns={"resourceType": "resource_type", "pattern": "sample"}, inplace=True)
            df_combined_patterns = (
                pd.concat(
                    [df_auto_patterns[["resource_type", "sample"]], df_manual_patterns[["resource_type", "sample"]]]
                )
                .drop_duplicates()
                .sort_values(by=["resource_type", "sample"])
            )

            if df_combined_patterns.empty:
                st.info("Pattern catalog is empty or could not be loaded.")
            else:
                resource_types = sorted(df_combined_patterns["resource_type"].unique())
                tabs = st.tabs(resource_types)
                for i, resource_type in enumerate(resource_types):
                    with tabs[i]:
                        df_filtered_patterns = df_combined_patterns[
                            df_combined_patterns["resource_type"] == resource_type
                        ]
                        st.dataframe(
                            df_filtered_patterns[["sample"]],
                            use_container_width=True,
                            hide_index=True,
                            column_config={"sample": "Pattern"},
                        )
        tag_col1, tag_col2 = st.columns(2)
        with tag_col1:
            st.metric(
                "✅ Actual Annotations",
                f"{total_actual}",
                help="A list of all unique tags that have been successfully created. This is our ground truth.",
            )
            st.dataframe(
                pd.DataFrame(sorted(list(actual_annotations_set)), columns=["Tag"]),
                use_container_width=True,
                hide_index=True,
            )
        with tag_col2:
            st.metric(
                "💡 Potential New Annotations",
                f"{total_potential}",
                help="A list of all unique tags found by the pattern-mode job that do not yet exist as actual annotations. This is now a clean 'to-do list' of tags that could be promoted or used to create new patterns.",
            )
            st.dataframe(
                pd.DataFrame(sorted(list(potential_new_annotations_set)), columns=["Tag"]),
                use_container_width=True,
                hide_index=True,
            )

# ==========================================
#           PER-FILE ANALYSIS TAB
# ==========================================
with per_file_tab:
    st.subheader(
        "Per-File Annotation Quality",
        help="A deep-dive tool for investigating the quality scores of individual files. Filter the table to find specific examples of high or low performance, then select a file to see a detailed breakdown of its specific matched, unmatched, and missed tags.",
    )

    df_patterns_file = fetch_raw_table_data(db_name, pattern_table)
    df_tags_file = fetch_raw_table_data(db_name, tag_table)
    df_docs_file = fetch_raw_table_data(db_name, doc_table)

    if df_patterns_file.empty:
        st.info("The pattern catalog is empty. Run the pipeline with patternMode enabled to generate data.")
    else:
        df_annotations_file = pd.concat([df_tags_file, df_docs_file], ignore_index=True)
        df_patterns_agg_file = (
            df_patterns_file.groupby("startNode")["startNodeText"].apply(set).reset_index(name="potentialTags")
        )
        df_annotations_agg_file = (
            df_annotations_file.groupby("startNode")["startNodeText"].apply(set).reset_index(name="actualAnnotations")
            if not df_annotations_file.empty
            else pd.DataFrame(columns=["startNode", "actualAnnotations"])
        )

        df_quality_file = pd.merge(df_patterns_agg_file, df_annotations_agg_file, on="startNode", how="left")
        df_quality_file["actualAnnotations"] = df_quality_file["actualAnnotations"].apply(
            lambda x: x if isinstance(x, set) else set()
        )

        # Apply normalized comparison for per-file metrics
        def calculate_metrics(row):
            potential = row["potentialTags"]
            actual = row["actualAnnotations"]
            norm_potential = {normalize(p) for p in potential}
            norm_actual = {normalize(a) for a in actual}

            total_actual_pf = len(norm_actual)
            total_potential_pf = len(norm_potential - norm_actual)

            return total_actual_pf, total_potential_pf

        metrics = df_quality_file.apply(calculate_metrics, axis=1, result_type="expand")
        df_quality_file[["actualAnnotationsCount", "potentialNewAnnotationsCount"]] = metrics

        df_quality_file["coverageRate"] = (
            (
                df_quality_file["actualAnnotationsCount"]
                / (df_quality_file["actualAnnotationsCount"] + df_quality_file["potentialNewAnnotationsCount"])
            )
            * 100
        ).fillna(0)

        df_file_meta = fetch_annotation_states(annotation_state_view, file_view)
        df_display_unfiltered = (
            pd.merge(df_quality_file, df_file_meta, left_on="startNode", right_on="fileExternalId", how="left")
            if not df_file_meta.empty
            else df_quality_file
        )

        with st.expander("Filter Per-File Quality Table"):
            excluded_columns = [
                "Select",
                "startNode",
                "potentialTags",
                "actualAnnotations",
                "actualAnnotationsCount",
                "potentialNewAnnotationsCount",
                "coverageRate",
                "externalId",
                "space",
                "annotatedPageCount",
                "annotationMessage",
                "fileAliases",
                "fileAssets",
                "fileIsuploaded",
                "jobId",
                "linkedFile",
                "pageCount",
                "patternModeJobId",
                "sourceCreatedUser",
                "sourceCreatedTime",
                "sourceUpdatedTime",
                "sourceUpdatedUser",
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
            filterable_columns = sorted([col for col in df_display_unfiltered.columns if col not in excluded_columns])
            filter_col1, filter_col2 = st.columns(2)
            with filter_col1:
                selected_column = st.selectbox(
                    "Filter by Metadata Property",
                    options=["None"] + filterable_columns,
                    on_change=reset_selection,
                    key="per_file_filter",
                )
            selected_values = []
            if selected_column != "None":
                unique_values = sorted(df_display_unfiltered[selected_column].dropna().unique().tolist())
                with filter_col2:
                    selected_values = st.multiselect(
                        f"Select Value(s) for {selected_column}", options=unique_values, on_change=reset_selection
                    )
            coverage_range = st.slider(
                "Filter by Annotation Coverage (%)", 0, 100, (0, 100), on_change=reset_selection, key="coverage_slider"
            )

        df_display = df_display_unfiltered.copy()
        if selected_column != "None" and selected_values:
            df_display = df_display[df_display[selected_column].isin(selected_values)]
        df_display = df_display[
            (df_display["coverageRate"] >= coverage_range[0]) & (df_display["coverageRate"] <= coverage_range[1])
        ]
        df_display = df_display.reset_index(drop=True)
        df_display.insert(0, "Select", False)

        default_columns = [
            "Select",
            "fileName",
            "fileSourceid",
            "fileMimetype",
            "coverageRate",
            "annotationMessage",
            "patternModeMessage",
            "lastUpdatedTime",
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

        if st.session_state.get("selected_row_index") is not None and st.session_state.selected_row_index < len(
            df_display
        ):
            df_display.at[st.session_state.selected_row_index, "Select"] = True

        edited_df = st.data_editor(
            df_display[selected_columns],
            key="quality_table_editor",
            column_config={
                "Select": st.column_config.CheckboxColumn(required=True),
                "fileName": "File Name",
                "fileSourceid": "Source ID",
                "fileMimetype": "Mime Type",
                "fileExternalId": "File External ID",
                "coverageRate": st.column_config.ProgressColumn(
                    "Annotation Coverage ℹ️",
                    help="The percentage of all unique tags (both actual and potential) that have been successfully annotated. Formula: Total Actual Annotations / (Total Actual Annotations + Total Potential New Annotations)",
                    format="%.2f%%",
                    min_value=0,
                    max_value=100,
                ),
                "annotationMessage": "Annotation Message",
                "patternModeMessage": "Pattern Mode Message",
                "lastUpdatedTime": "Last Updated Time",
            },
            use_container_width=True,
            hide_index=True,
            disabled=df_display.columns.difference(["Select"]),
        )

        selected_indices = edited_df[edited_df.Select].index.tolist()
        if len(selected_indices) > 1:
            new_selection = [idx for idx in selected_indices if idx != st.session_state.get("selected_row_index")]
            st.session_state.selected_row_index = new_selection[0] if new_selection else None
            st.rerun()
        elif len(selected_indices) == 1:
            st.session_state.selected_row_index = selected_indices[0]
        elif len(selected_indices) == 0 and st.session_state.get("selected_row_index") is not None:
            st.session_state.selected_row_index = None
            st.rerun()

        st.divider()
        if st.session_state.get("selected_row_index") is not None and st.session_state.selected_row_index < len(
            df_display
        ):
            selected_file_data = df_display.iloc[st.session_state.selected_row_index]
            selected_file = selected_file_data["startNode"]
            st.markdown(f"**Displaying Tag Comparison for file:** `{selected_file}`")
            file_space_series = df_patterns_file[df_patterns_file["startNode"] == selected_file]["startNodeSpace"]
            if not file_space_series.empty:
                file_space = file_space_series.iloc[0]
                file_node_id = NodeId(space=file_space, external_id=selected_file)
                df_potential_tags_details = df_patterns_file[df_patterns_file["startNode"] == selected_file][
                    ["startNodeText", "endNodeResourceType"]
                ]
                df_actual_annotations_details = (
                    df_annotations_file[df_annotations_file["startNode"] == selected_file][
                        ["startNodeText", "endNodeResourceType"]
                    ]
                    if not df_annotations_file.empty
                    else pd.DataFrame(columns=["startNodeText", "endNodeResourceType"])
                )

                # Use normalized comparison for per-file detail view
                potential_set = set(df_potential_tags_details["startNodeText"])
                actual_set = set(df_actual_annotations_details["startNodeText"])
                norm_potential = {normalize(p) for p in potential_set}
                norm_actual = {normalize(a) for a in actual_set}

                # We need a map from normalized text back to original for accurate filtering
                potential_map = {normalize(text): text for text in potential_set}

                norm_unmatched = norm_potential - norm_actual

                actual_df = df_actual_annotations_details.drop_duplicates()
                potential_df = df_potential_tags_details[
                    df_potential_tags_details["startNodeText"].isin({potential_map[t] for t in norm_unmatched})
                ].drop_duplicates(subset=["startNodeText", "endNodeResourceType"])

                if st.button("Create in Canvas", key=f"canvas_btn_{selected_file}"):
                    with st.spinner("Generating Industrial Canvas with bounding boxes..."):
                        # The 'regions' column is no longer available in the RAW table.
                        # You will need to adjust the canvas generation logic to handle this.
                        # For now, we will pass an empty list.
                        potential_tags_for_canvas = []
                        canvas_url = generate_file_canvas(
                            file_id=file_node_id,
                            file_view=file_view,
                            ep_config=ep_config,
                            unmatched_tags_with_regions=potential_tags_for_canvas,
                        )
                        if canvas_url:
                            st.session_state["generated_canvas_url"] = canvas_url
                        else:
                            st.session_state.pop("generated_canvas_url", None)

                if "generated_canvas_url" in st.session_state and st.session_state.generated_canvas_url:
                    st.markdown(
                        f"**[Open Last Generated Canvas]({st.session_state.generated_canvas_url})**",
                        unsafe_allow_html=True,
                    )

                st.divider()
                col1, col2 = st.columns(2)
                with col1:
                    st.metric(
                        "✅ Actual Annotations in this File",
                        len(actual_df),
                    )
                    st.dataframe(
                        actual_df,
                        column_config={"startNodeText": "Tag", "endNodeResourceType": "Resource Type"},
                        use_container_width=True,
                        hide_index=True,
                    )
                with col2:
                    st.metric(
                        "💡 Potential New Annotations in this File",
                        len(potential_df),
                    )
                    st.dataframe(
                        potential_df,
                        column_config={"startNodeText": "Tag", "endNodeResourceType": "Resource Type"},
                        use_container_width=True,
                        hide_index=True,
                    )
        else:
            st.info("✔️ Select a file in the table above to see a detailed breakdown of its tags.")

# ==========================================
#           PATTERN MANAGEMENT TAB
# ==========================================
with management_tab:
    primary_scope_prop = ep_config.get("launchFunction", {}).get("primaryScopeProperty")
    secondary_scope_prop = ep_config.get("launchFunction", {}).get("secondaryScopeProperty")

    st.subheader(
        "Existing Manual Patterns",
        help="An action-oriented tool for improving pattern quality. After identifying missed tags in the other tabs, come here to add new manual patterns or edit existing ones to enhance the detection logic for future pipeline runs.",
    )
    df_manual_patterns_manage = fetch_manual_patterns(db_name, manual_patterns_table)

    edited_df_manage = st.data_editor(
        df_manual_patterns_manage,
        num_rows="dynamic",
        use_container_width=True,
        column_config={
            "key": st.column_config.TextColumn("Scope Key", disabled=True),
            "sample": st.column_config.TextColumn("Pattern String", required=True),
            "resource_type": st.column_config.TextColumn("Resource Type", required=True),
            "scope_level": st.column_config.SelectboxColumn(
                "Scope Level",
                options=["Global", "Primary Scope", "Secondary Scope"],
                required=True,
            ),
            "primary_scope": st.column_config.TextColumn("Primary Scope"),
            "secondary_scope": st.column_config.TextColumn("Secondary Scope"),
            "created_by": st.column_config.TextColumn("Created By", required=True),
        },
    )

    if st.button("Save Changes", type="primary", key="save_patterns"):
        with st.spinner("Saving changes to RAW..."):
            try:
                save_manual_patterns(edited_df_manage, db_name, manual_patterns_table)
                st.success("Changes saved successfully!")
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"Failed to save changes: {e}")

    st.divider()

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
                        new_row = pd.DataFrame(
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
                        )
                        updated_df = pd.concat([edited_df_manage, new_row], ignore_index=True)

                        save_manual_patterns(updated_df, db_name, manual_patterns_table)
                        st.success("New pattern added successfully!")
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed to add pattern: {e}")
