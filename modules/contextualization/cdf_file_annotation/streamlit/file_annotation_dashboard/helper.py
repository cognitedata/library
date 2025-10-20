import os
import re
import yaml
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from cognite.client import CogniteClient
from cognite.client.data_classes import RowWrite, Asset, AssetFilter
from cognite.client.data_classes.data_modeling import (
    ViewId,
    NodeId,
    Node,
    filters,
    EdgeApply,
    NodeOrEdgeData,
    DirectRelationReference,
)
from cognite.client.data_classes.functions import FunctionCallLog
from data_structures import ViewPropertyConfig
from canvas import dm_generate

client = CogniteClient()


@st.cache_data(ttl=3600)
def get_file_node(file_id: NodeId, file_view: ViewPropertyConfig) -> Node | None:
    """Fetches a single file node from CDF."""
    try:
        node = client.data_modeling.instances.retrieve_nodes(nodes=file_id, sources=file_view.as_view_id())
        return node
    except Exception as e:
        st.error(f"Failed to retrieve file node {file_id}: {e}")
    return None


def generate_file_canvas(
    file_id: NodeId, file_view: ViewPropertyConfig, ep_config: dict, unmatched_tags_with_regions: list = []
):
    """
    Generates an Industrial Canvas, including bounding boxes for unmatched tags,
    and returns the canvas URL.
    """
    file_node = get_file_node(file_id, file_view)
    if not file_node:
        st.error("Could not generate canvas because the file node could not be retrieved.")
        return None

    canvas_name = f"Annotation Quality Analysis - {file_node.external_id}"

    try:
        domain = os.getenv("COGNITE_ORGANIZATION")
        project = client.config.project
        cluster = client.config.cdf_cluster

        canvas_id = dm_generate(
            name=canvas_name,
            file_node=file_node,
            file_view_id=file_view.as_view_id(),
            client=client,
            unmatched_tags_with_regions=unmatched_tags_with_regions,
        )
        st.success(f"Successfully generated canvas: {canvas_name}")

        canvas_url = f"https://{domain}.fusion.cognite.com/{project}/industrial-canvas/canvas?canvasId={canvas_id}&cluster={cluster}.cognitedata.com&env={cluster}&workspace=industrial-tools"
        return canvas_url

    except Exception as e:
        st.error(f"Failed to generate canvas: {e}")
        return None


@st.cache_data(ttl=600)
def find_pipelines(name_filter: str = "file_annotation") -> list[str]:
    """
    Finds the external IDs of all extraction pipelines in the project,
    filtered by a substring in their external ID.
    """
    try:
        all_pipelines = client.extraction_pipelines.list(limit=-1)
        if not all_pipelines:
            st.warning(f"No extraction pipelines found in the project.")
            return []

        filtered_ids = [p.external_id for p in all_pipelines if name_filter in p.external_id]

        if not filtered_ids:
            st.warning(f"No pipelines matching the filter '*{name_filter}*' found in the project.")
            return []

        return sorted(filtered_ids)
    except Exception as e:
        st.error(f"An error occurred while searching for extraction pipelines: {e}")
        return []


@st.cache_data(ttl=3600)
def fetch_raw_table_data(db_name: str, table_name: str) -> pd.DataFrame:
    """Fetches all rows from a specified RAW table and returns as a DataFrame."""
    try:
        rows = client.raw.rows.list(db_name=db_name, table_name=table_name, limit=-1)
        if not rows:
            return pd.DataFrame()
        data = [row.columns for row in rows]
        return pd.DataFrame(data)
    except Exception as e:
        st.error(f"Failed to fetch data from RAW table '{table_name}': {e}")
        return pd.DataFrame()


def parse_run_message(message: str) -> dict:
    """Parses the structured run message and returns a dictionary of its components."""
    if not message:
        return {}

    pattern = re.compile(
        r"\(caller:(?P<caller>\w+), function_id:(?P<function_id>[\w\.-]+), call_id:(?P<call_id>[\w\.-]+)\) - "
        r"total files processed: (?P<total>\d+) - "
        r"successful files: (?P<success>\d+) - "
        r"failed files: (?P<failed>\d+)"
    )
    match = pattern.search(message)
    if match:
        data = match.groupdict()
        for key in ["total", "success", "failed"]:
            if key in data:
                data[key] = int(data[key])
        return data
    return {}


@st.cache_data(ttl=3600)
def fetch_extraction_pipeline_config(pipeline_ext_id: str) -> tuple[dict, ViewPropertyConfig, ViewPropertyConfig]:
    """
    Fetch configurations from the latest extraction
    """
    ep_configuration = client.extraction_pipelines.config.retrieve(external_id=pipeline_ext_id)
    config_dict = yaml.safe_load(ep_configuration.config)

    local_annotation_state_view = config_dict["dataModelViews"]["annotationStateView"]
    annotation_state_view = ViewPropertyConfig(
        local_annotation_state_view["schemaSpace"],
        local_annotation_state_view["externalId"],
        local_annotation_state_view["version"],
        local_annotation_state_view["instanceSpace"],
    )

    local_file_view = config_dict["dataModelViews"]["fileView"]
    file_view = ViewPropertyConfig(
        local_file_view["schemaSpace"],
        local_file_view["externalId"],
        local_file_view["version"],
        local_file_view.get("instanceSpace"),
    )

    local_target_entities_view = config_dict["dataModelViews"]["targetEntitiesView"]
    target_entities_view = ViewPropertyConfig(
        local_target_entities_view["schemaSpace"],
        local_target_entities_view["externalId"],
        local_target_entities_view["version"],
        local_target_entities_view.get("instanceSpace"),
    )

    views_dict = {
        "annotation_state": annotation_state_view,
        "file": file_view,
        "target_entities": target_entities_view,
    }

    return (config_dict, views_dict)


@st.cache_data(ttl=3600)
def fetch_annotation_states(annotation_state_view: ViewPropertyConfig, file_view: ViewPropertyConfig):
    """
    Fetches annotation state instances from the specified data model view
    and joins them with their corresponding file instances.
    """
    annotation_instances = client.data_modeling.instances.list(
        instance_type="node",
        space=annotation_state_view.instance_space,
        sources=annotation_state_view.as_view_id(),
        limit=-1,
    )
    if not annotation_instances:
        return pd.DataFrame()

    annotation_data = []
    nodes_to_fetch = []
    for instance in annotation_instances:
        node_data = {
            "externalId": instance.external_id,
            "space": instance.space,
            "createdTime": pd.to_datetime(instance.created_time, unit="ms"),
            "lastUpdatedTime": pd.to_datetime(instance.last_updated_time, unit="ms"),
        }
        for prop_key, prop_value in instance.properties[annotation_state_view.as_view_id()].items():
            if prop_key == "linkedFile" and prop_value:
                file_external_id = prop_value.get("externalId")
                file_space = prop_value.get("space")
                node_data["fileExternalId"] = file_external_id
                node_data["fileSpace"] = file_space
                if file_external_id and file_space:
                    nodes_to_fetch.append(NodeId(space=file_space, external_id=file_external_id))
            node_data[prop_key] = prop_value
        annotation_data.append(node_data)

    df_annotations = pd.DataFrame(annotation_data)
    if df_annotations.empty or not nodes_to_fetch:
        return df_annotations

    unique_nodes_to_fetch = list(set(nodes_to_fetch))
    file_instances = client.data_modeling.instances.retrieve_nodes(
        nodes=unique_nodes_to_fetch, sources=file_view.as_view_id()
    )

    file_data = []
    for instance in file_instances:
        node_data = {"fileExternalId": instance.external_id, "fileSpace": instance.space}
        properties = instance.properties[file_view.as_view_id()]

        for prop_key, prop_value in properties.items():
            node_data[f"file{prop_key.capitalize()}"] = (
                ", ".join(map(str, prop_value)) if isinstance(prop_value, list) else prop_value
            )
        file_data.append(node_data)

    if not file_data:
        return df_annotations

    df_files = pd.DataFrame(file_data)
    df_merged = pd.merge(df_annotations, df_files, on=["fileExternalId", "fileSpace"], how="left")

    for col in ["createdTime", "lastUpdatedTime"]:
        if col in df_merged.columns:
            df_merged[col] = df_merged[col].dt.tz_localize("UTC")

    df_merged.rename(columns={"annotationStatus": "status", "attemptCount": "retries"}, inplace=True)

    return df_merged


@st.cache_data(ttl=3600)
def fetch_pipeline_run_history(pipeline_ext_id: str):
    """Fetches the full run history for a given extraction pipeline."""
    return client.extraction_pipelines.runs.list(external_id=pipeline_ext_id, limit=-1)


@st.cache_data(ttl=3600)
def fetch_function_logs(function_id: int, call_id: int):
    """Fetches the logs for a specific function call."""
    try:
        log: FunctionCallLog = client.functions.calls.get_logs(call_id, function_id)
        return log.to_text(with_timestamps=False)
    except Exception as e:
        return [f"Could not retrieve logs: {e}"]


def process_runs_for_graphing(runs):
    """Transforms pipeline run data into a DataFrame for graphing."""
    launch_data, finalize_runs_to_agg = [], []
    for run in runs:
        if run.status != "success":
            continue
        parsed = parse_run_message(run.message)
        if not parsed:
            continue
        timestamp, count, caller = (
            pd.to_datetime(run.created_time, unit="ms").tz_localize("UTC"),
            parsed.get("total", 0),
            parsed.get("caller"),
        )
        if caller == "Launch":
            launch_data.append({"timestamp": timestamp, "count": count, "type": "Launch"})
        elif caller == "Finalize":
            finalize_runs_to_agg.append({"timestamp": timestamp, "count": count})

    aggregated_finalize_data = []
    if finalize_runs_to_agg:
        finalize_runs_to_agg.sort(key=lambda x: x["timestamp"])
        current_group_start_time, current_group_count = finalize_runs_to_agg[0]["timestamp"], 0
        for run in finalize_runs_to_agg:
            if run["timestamp"] < current_group_start_time + timedelta(minutes=10):
                current_group_count += run["count"]
            else:
                aggregated_finalize_data.append(
                    {"timestamp": current_group_start_time, "count": current_group_count, "type": "Finalize"}
                )
                current_group_start_time, current_group_count = run["timestamp"], run["count"]
        if current_group_count > 0:
            aggregated_finalize_data.append(
                {"timestamp": current_group_start_time, "count": current_group_count, "type": "Finalize"}
            )

    return pd.concat([pd.DataFrame(launch_data), pd.DataFrame(aggregated_finalize_data)], ignore_index=True)


@st.cache_data(ttl=3600)
def fetch_pattern_catalog(db_name: str, table_name: str) -> pd.DataFrame:
    """
    Fetches the entity cache and explodes it to create a complete
    catalog of all generated patterns, indexed by resourceType.
    """
    try:
        rows = client.raw.rows.list(db_name=db_name, table_name=table_name, limit=-1)
        if not rows:
            return pd.DataFrame()
        all_patterns = []
        for row in pd.DataFrame([row.columns for row in rows]).itertuples():
            for sample_list in ["AssetPatternSamples", "FilePatternSamples"]:
                if hasattr(row, sample_list) and isinstance(getattr(row, sample_list), list):
                    for item in getattr(row, sample_list):
                        if item.get("sample") and item.get("resource_type"):
                            all_patterns.extend(
                                [
                                    {"resourceType": item["resource_type"], "pattern": pattern}
                                    for pattern in item["sample"]
                                ]
                            )
        return pd.DataFrame(all_patterns)
    except Exception as e:
        st.error(f"Failed to fetch pattern catalog from '{table_name}': {e}")
        return pd.DataFrame()


def fetch_manual_patterns(db_name: str, table_name: str) -> pd.DataFrame:
    """
    Fetches all manual patterns from the RAW table and explodes them
    into a tidy DataFrame for display and editing.
    """
    all_patterns = []
    try:
        for row in client.raw.rows.list(db_name=db_name, table_name=table_name, limit=-1):
            key, patterns_list = row.key, row.columns.get("patterns", [])
            scope_level, primary_scope, secondary_scope = "Global", "", ""
            if key != "GLOBAL":
                parts = key.split("_")
                if len(parts) == 2:
                    scope_level, primary_scope, secondary_scope = "Secondary Scope", parts[0], parts[1]
                else:
                    scope_level, primary_scope = "Primary Scope", key
            all_patterns.extend(
                [
                    {
                        "key": key,
                        "scope_level": scope_level,
                        "annotation_type": p.get("annotation_type"),
                        "primary_scope": primary_scope,
                        "secondary_scope": secondary_scope,
                        "sample": p.get("sample"),
                        "resource_type": p.get("resource_type"),
                        "created_by": p.get("created_by"),
                    }
                    for p in patterns_list
                ]
            )

        df = (
            pd.DataFrame(all_patterns)
            if all_patterns
            else pd.DataFrame(
                columns=[
                    "key",
                    "scope_level",
                    "annotation_type",
                    "primary_scope",
                    "secondary_scope",
                    "sample",
                    "resource_type",
                    "created_by",
                ]
            )
        )
        return df.fillna("").astype(str)
    except Exception as e:
        if "NotFoundError" not in str(type(e)):
            st.error(f"Failed to fetch manual patterns: {e}")
        return pd.DataFrame(
            columns=["key", "scope_level", "annotation_type", "primary_scope", "secondary_scope", "sample", "resource_type", "created_by"]
        )


def save_manual_patterns(df: pd.DataFrame, db_name: str, table_name: str):
    """
    Takes a tidy DataFrame of patterns, groups them by scope key,
    and writes them back to the RAW table.
    """

    def create_key(row):
        if row["scope_level"] == "Global":
            return "GLOBAL"
        if row["scope_level"] == "Primary Scope" and row["primary_scope"]:
            return row["primary_scope"]
        if row["scope_level"] == "Secondary Scope" and row["primary_scope"] and row["secondary_scope"]:
            return f"{row['primary_scope']}_{row['secondary_scope']}"
        return None

    df["key"] = df.apply(create_key, axis=1)
    df.dropna(subset=["key"], inplace=True)
    rows_to_write = [
        RowWrite(key=key, columns={"patterns": group[["sample", "resource_type", "annotation_type", "created_by"]].to_dict("records")})
        for key, group in df.groupby("key")
    ]

    existing_keys = {r.key for r in client.raw.rows.list(db_name, table_name, limit=-1)}
    keys_to_delete = list(existing_keys - {r.key for r in rows_to_write})
    if keys_to_delete:
        client.raw.rows.delete(db_name=db_name, table_name=table_name, key=keys_to_delete)
    if rows_to_write:
        client.raw.rows.insert(db_name=db_name, table_name=table_name, row=rows_to_write, ensure_parent=True)


@st.cache_data(ttl=600)
def get_files_by_call_id(call_id: int, annotation_state_view: ViewPropertyConfig) -> pd.DataFrame:
    """
    Finds all files associated with a specific function call ID by querying
    the AnnotationState data model.
    """
    if not call_id:
        return pd.DataFrame()
    try:
        call_id_filter = filters.Or(
            filters.Equals(annotation_state_view.as_property_ref("launchFunctionCallId"), call_id),
            filters.Equals(annotation_state_view.as_property_ref("finalizeFunctionCallId"), call_id),
        )
        instances = client.data_modeling.instances.list(
            instance_type="node", sources=annotation_state_view.as_view_id(), filter=call_id_filter, limit=-1
        )
        if not instances:
            return pd.DataFrame()

        view_id_tuple = annotation_state_view.as_view_id()
        file_ids = [
            instance.properties.get(view_id_tuple, {}).get("linkedFile", {}).get("externalId")
            for instance in instances
            if instance.properties.get(view_id_tuple, {}).get("linkedFile", {}).get("externalId")
        ]
        return pd.DataFrame(file_ids, columns=["File External ID"])
    except Exception as e:
        st.error(f"Failed to query files by call ID: {e}")
        return pd.DataFrame()


def calculate_overview_kpis(df: pd.DataFrame) -> dict:
    """Calculates high-level KPIs from the AnnotationState dataframe."""
    kpis = {"awaiting_processing": 0, "processed_total": 0, "failed_total": 0, "failure_rate_total": 0}
    if df.empty:
        return kpis
    kpis["awaiting_processing"] = len(df[df["status"].isin(["New", "Retry", "Processing", "Finalizing"])])
    finalized_all_time = df[df["status"].isin(["Annotated", "Failed"])]
    kpis["processed_total"] = len(finalized_all_time)
    kpis["failed_total"] = len(finalized_all_time[finalized_all_time["status"] == "Failed"])
    if kpis["processed_total"] > 0:
        kpis["failure_rate_total"] = (kpis["failed_total"] / kpis["processed_total"]) * 100
    return kpis


def filter_log_lines(log_text: str, search_string: str) -> str:
    """
    Takes a block of log text and a search string, returning a new string
    containing the lines that include the search string, plus the subsequent
    indented lines that provide context.
    """
    if not log_text or not isinstance(log_text, str):
        return "Log content is not available or in an invalid format."
    relevant_blocks, lines = [], log_text.splitlines()
    for i, line in enumerate(lines):
        if search_string in line:
            current_block = [line]
            next_line_index = i + 1
            while next_line_index < len(lines):
                next_line = lines[next_line_index]
                if next_line.strip().startswith("-") or "\t" in next_line or "  " in next_line:
                    current_block.append(next_line)
                    next_line_index += 1
                else:
                    break
            relevant_blocks.append("\n".join(current_block))
    return "\n\n".join(relevant_blocks)


# --- Remove all non-alphanumeric characters, convert to lowercase, and strip leading zeros from numbers ---
def normalize(s):
    """
    Normalizes a string by:
    1. Ensuring it's a string.
    2. Removing all non-alphanumeric characters.
    3. Converting to lowercase.
    4. Removing leading zeros from any sequence of digits found within the string.
    """
    if not isinstance(s, str):
        return ""

    # Step 1: Basic cleaning (e.g., "V-0912" -> "v0912")
    s = re.sub(r"[^a-zA-Z0-9]", "", s).lower()

    # Step 2: Define a replacer function that converts any matched number to an int and back to a string
    def strip_leading_zeros(match):
        # match.group(0) is the matched string (e.g., "0912")
        return str(int(match.group(0)))

    # Step 3: Apply the replacer function to all sequences of digits (\d+) in the string
    # This turns "v0912" into "v912"
    return re.sub(r"\d+", strip_leading_zeros, s)


def derive_annotation_status(tags, row_status=None, default_status="Pattern found"):
    if tags is None:
        tags = []

    if "PromotedAuto" in tags:
        return "Automatically Promoted"

    if "PromotedManually" in tags:
        return "Manually Promoted"

    if "AmbiguousMatch" in tags:
        return "Ambiguous"

    if "PromoteAttempted" in tags:
        return "No match found"

    return default_status


def build_potential_status_map(df_candidates: pd.DataFrame) -> dict:
    status_map = {}
    if df_candidates is None or df_candidates.empty:
        return status_map

    for _, r in df_candidates.iterrows():
        text = r.get("startNodeText")
        if not text:
            continue
        status_label = derive_annotation_status(r.get("tags"), row_status=r.get("status"))
        status_map.setdefault(text, status_label)

    return status_map


@st.cache_data(ttl=600)
def fetch_potential_annotations(db_name: str, table_name: str, file_external_id: str) -> pd.DataFrame:
    """Fetches potential annotations for a specific file from the patterns RAW table."""
    try:
        rows = client.raw.rows.list(
            db_name=db_name, table_name=table_name, limit=-1, filter={"startNode": file_external_id}
        )
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame([row.columns for row in rows])
    except Exception as e:
        st.error(f"Failed to fetch potential annotations: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def fetch_entities(entity_view: ViewPropertyConfig, resource_property: str, secondary_scope_prop: str | None = None) -> pd.DataFrame:
    """
    Fetches entity instances from the specified data model view and returns a tidy DataFrame.
    """
    instances = client.data_modeling.instances.list(
        instance_type="node",
        space=entity_view.instance_space,
        sources=entity_view.as_view_id(),
        limit=-1
    )

    if not instances:
        return pd.DataFrame()

    data = []

    for instance in instances:
        props = instance.properties.get(entity_view.as_view_id(), {}) or {}
        row = {"externalId": instance.external_id, "space": instance.space}

        row["name"] = props.get("name")
        row["resourceType"] = props.get(resource_property)

        if secondary_scope_prop:
            row[secondary_scope_prop] = props.get(secondary_scope_prop)
        
        for k, v in props.items():
            if k not in row:
                row[k] = v

        data.append(row)

    return pd.DataFrame(data)


def show_connect_unmatched_ui(
    tag_text,
    file_view,
    target_entities_view,
    file_resource_property,
    target_entities_resource_property,
    associated_files,
    tab,
    db_name,
    pattern_table,
    apply_config,
    annotation_state_view,
    secondary_scope_prop = None,
):
    """
    Displays the UI to connect a single unmatched tag to either an Asset or a File.
    """
    st.markdown(f"### Tag to Connect: `{tag_text}`")
    df_states = fetch_annotation_states(annotation_state_view, file_view)

    file_count = len(associated_files)
    expander_label = f"Associated Files ({file_count})"

    with st.expander(expander_label, expanded=False):
        for associated_file in associated_files:
            row = df_states[df_states["fileExternalId"] == associated_file]
            name = row.iloc[0]["fileName"]
            st.markdown(f"- `{name} ({associated_file})`")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Retrieve Assets", key=f"btn_retrieve_assets_{tab}"):
            st.session_state.selected_entity_type_to_connect = "asset"
            st.session_state.selected_entity_to_connect_index = None
    with col2:
        if st.button("Retrieve Files", key=f"btn_retrieve_files_{tab}"):
            st.session_state.selected_entity_type_to_connect = "file"
            st.session_state.selected_entity_to_connect_index = None

    entity_type = st.session_state.selected_entity_type_to_connect

    if not entity_type:
        return

    if entity_type == "file":
        entity_view = file_view
        resource_property = file_resource_property
        annotation_type = "diagrams.FileLink"
    else:
        entity_view = target_entities_view
        resource_property = target_entities_resource_property
        annotation_type = "diagrams.AssetLink"

    df_entities = fetch_entities(entity_view, resource_property, secondary_scope_prop)

    if df_entities.empty:
        st.warning(f"No {entity_type}s found.")
        return

    df_entities_display = df_entities.copy()
    df_entities_display.insert(0, "Select", False)

    if st.session_state.selected_entity_to_connect_index is not None:
        idx = st.session_state.selected_entity_to_connect_index

        if idx in df_entities_display.index:
            df_entities_display.loc[:, "Select"] = False
            df_entities_display.at[idx, "Select"] = True

    secondary_col = secondary_scope_prop if secondary_scope_prop else None
    filterable_columns = [
        col for col in (([secondary_col, "resourceType"] if secondary_col else ["resourceType"])) if col in df_entities_display.columns
    ]

    for filterable_column in filterable_columns:
        unique_values = sorted(df_entities_display[filterable_column].dropna().unique().tolist())

        selected_value = st.selectbox(
            f"Filter by {filterable_column}",
            key=f"sb_filterable_column_{filterable_column}_{tab}",
            options=[None] + unique_values,
            index=0
        )

        if selected_value:
            df_entities_display = df_entities_display[df_entities_display[filterable_column] == selected_value]
    
    all_columns = df_entities_display.columns.tolist()
    default_columns = ["Select", "name", "resourceType", "externalId"]
    if secondary_col and secondary_col in df_entities_display.columns:
        default_columns.insert(-1, secondary_col)

    with st.popover("Customize Table Columns"):
        selected_columns = st.multiselect(
            f"Select columns to display ({entity_type}s)",
            options=all_columns,
            default=[col for col in default_columns if col in all_columns],
            key=f"ms_selected_columns_{tab}_{entity_type}"
        )

    entity_editor_key = f"{entity_type}_editor_{tag_text}_{tab}"
    column_config = {
        "Select": st.column_config.CheckboxColumn(required=True),
        "name": "Name",
        "externalId": "External ID",
        "resourceType": "Resource Type",
    }
    if secondary_col and secondary_col in df_entities_display.columns:
        column_config[secondary_col] = secondary_col

    edited_entities = st.data_editor(
        df_entities_display[selected_columns],
        key=entity_editor_key,
        column_config=column_config,
        use_container_width=True,
        hide_index=True,
        disabled=df_entities_display.columns.difference(["Select"]),
    )

    selected_indices = edited_entities[edited_entities.Select].index.tolist()

    if len(selected_indices) > 1:
        new_selection = [idx for idx in selected_indices if idx != st.session_state.selected_entity_to_connect_index]
        st.session_state.selected_entity_to_connect_index = new_selection[0] if new_selection else None
        st.rerun()
    elif len(selected_indices) == 1:
        st.session_state.selected_entity_to_connect_index = selected_indices[0]
    elif len(selected_indices) == 0 and st.session_state.selected_entity_to_connect_index is not None:
        st.session_state.selected_entity_to_connect_index = None
        st.rerun()

    if st.session_state.selected_entity_to_connect_index is not None:
        selected_entity = df_entities.loc[st.session_state.selected_entity_to_connect_index]
        if st.button(
            f"Connect '{tag_text}' to '{selected_entity['name']}' in {str(len(associated_files)) + ' files' if len(associated_files) > 1 else str(len(associated_files)) + ' file'}",
            key=f"btn_connect_tag_to_entities_{tab}"
        ):
            success, count, error = create_tag_connection(
                client,
                db_name,
                pattern_table,
                tag_text,
                associated_files,
                selected_entity,
                annotation_type,
                apply_config,
                entity_view,
            )

            if success:
                st.toast(
                    f"{count} annotation{'s' if count > 1 else ''} created from tag '{tag_text}' to {entity_type} '{selected_entity['name']}' "
                    f"in {len(associated_files)} file{'s' if len(associated_files) > 1 else ''}!",
                    icon=":material/check_small:"
                )
                st.cache_data.clear()
            else:
                st.toast(
                    body=f"Failed to connect tag '{tag_text}': {error}",
                    icon=":material/error:"
                )


def create_tag_connection(
    client: CogniteClient,
    db_name: str,
    table_name: str,
    tag_text: str,
    associated_files: list[str],
    selected_entity: pd.Series,
    annotation_type: str,
    apply_config: dict,
    entity_view: ViewPropertyConfig,
):
    updated_rows = []
    updated_edges = []

    try:
        rows = client.raw.rows.list(
            db_name=db_name,
            table_name=table_name,
            limit=-1
        )

        sink_node_space = apply_config["sinkNode"]["space"]

        for row in rows:
            row_data = row.columns

            if row_data.get("startNodeText") == tag_text and row_data.get("startNode") in associated_files:
                edge_external_id = row.key
                file_id = row_data.get("startNode")

                updated_edges.append(
                    EdgeApply(
                        space=sink_node_space,
                        external_id=edge_external_id,
                        type=DirectRelationReference(space=row_data.get("viewSpace"), external_id=annotation_type),
                        start_node=DirectRelationReference(space=row_data.get("startNodeSpace"), external_id=file_id),
                        end_node=DirectRelationReference(space=selected_entity.get("space"), external_id=selected_entity.get("externalId"))
                    )
                )
                
                row_data["endNode"] = selected_entity["externalId"]
                row_data["endNodeSpace"] = selected_entity["space"]

                resource_type = selected_entity["resourceType"] if selected_entity["resourceType"] else entity_view.external_id

                row_data["endNodeResourceType"] = resource_type

                existing_tags = row_data.get("tags")
                if existing_tags is None:
                    row_data["tags"] = ["PromotedManually"]
                else:
                    if "PromotedManually" not in existing_tags:
                        existing_tags.append("PromotedManually")
                        row_data["tags"] = existing_tags

                row_data["status"] = "Approved"

                updated_rows.append(
                    RowWrite(
                        key=edge_external_id,
                        columns=row_data
                    )
                )

        if updated_rows:
            client.raw.rows.insert(
                db_name=db_name,
                table_name=table_name,
                row=updated_rows,
                ensure_parent=True
            )

        if updated_edges:
            client.data_modeling.instances.apply(edges=updated_edges, replace=False)

        return True, len(updated_rows), None
    except Exception as e:
        return False, 0, str(e)


def build_unmatched_tags_with_regions(
    df: pd.DataFrame,
    file_id: str,
    potential_new_annotations: list[str]
):
    df_filtered = df[
        (df["startNode"] == file_id) &
        (df["startNodeText"].isin(potential_new_annotations))
    ]

    unmatched_tags_with_regions = []

    for _, row in df_filtered.iterrows():
        region = {
            "vertices": [
                {"x": row["startNodeXMin"], "y": row["startNodeYMin"]},
                {"x": row["startNodeXMax"], "y": row["startNodeYMin"]},
                {"x": row["startNodeXMax"], "y": row["startNodeYMax"]},
                {"x": row["startNodeXMin"], "y": row["startNodeYMax"]},
            ]
        }

        unmatched_tags_with_regions.append({
            "text": row["startNodeText"],
            "regions": [region]
        })

    return unmatched_tags_with_regions