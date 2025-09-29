import os
import re
import yaml
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from cognite.client import CogniteClient
from cognite.client.data_classes import RowWrite
from cognite.client.data_classes.data_modeling import ViewId, NodeId, Node, filters
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

    return (config_dict, annotation_state_view, file_view)


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
            columns=["key", "scope_level", "primary_scope", "secondary_scope", "sample", "resource_type", "created_by"]
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
        RowWrite(key=key, columns={"patterns": group[["sample", "resource_type", "created_by"]].to_dict("records")})
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


# --- Remove all non-alphanumeric characters and convert to lowercase ---
def normalize(s):
    # Ensure input is a string before applying regex
    if not isinstance(s, str):
        return ""
    return re.sub(r"[^a-zA-Z0-9]", "", s).lower()
