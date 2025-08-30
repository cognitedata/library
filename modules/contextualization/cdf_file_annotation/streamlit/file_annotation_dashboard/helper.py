import os
import re
import yaml
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import ViewId, NodeId, Node
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
        domain = ep_config.get("streamlitDashboard", {}).get("industrialCanvasDomain", "cog-shadow-projects")
        project = client.config.project
        cluster = client.config.cdf_cluster

        # Pass the unmatched tags data to dm_generate
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
    # ... (This function remains unchanged)
    annotation_instances = client.data_modeling.instances.list(
        instance_type="node",
        space=annotation_state_view.instance_space,
        sources=annotation_state_view.as_view_id(),
        limit=-1,
    )
    if not annotation_instances:
        st.info("No annotation state instances found in the specified view.")
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
        node_data = {
            "fileExternalId": instance.external_id,
            "fileSpace": instance.space,
        }
        properties = instance.properties[file_view.as_view_id()]

        for prop_key, prop_value in properties.items():
            if isinstance(prop_value, list):
                string_values = []
                for value in prop_value:
                    string_values.append(str(value))
                node_data[f"file{prop_key.capitalize()}"] = ", ".join(filter(None, string_values))
            else:
                node_data[f"file{prop_key.capitalize()}"] = prop_value
        file_data.append(node_data)

    if not file_data:
        return df_annotations

    df_files = pd.DataFrame(file_data)

    df_merged = pd.merge(df_annotations, df_files, on=["fileExternalId", "fileSpace"], how="left")

    if "createdTime" in df_merged.columns:
        df_merged["createdTime"] = df_merged["createdTime"].dt.tz_localize("UTC")
    if "lastUpdatedTime" in df_merged.columns:
        df_merged["lastUpdatedTime"] = df_merged["lastUpdatedTime"].dt.tz_localize("UTC")

    df_merged.rename(
        columns={
            "annotationStatus": "status",
            "attemptCount": "retries",
            "diagramDetectJobId": "jobId",
        },
        inplace=True,
    )

    for col in ["status", "fileExternalId", "retries", "jobId"]:
        if col not in df_merged.columns:
            df_merged[col] = None

    return df_merged


@st.cache_data(ttl=3600)
def fetch_pipeline_run_history(pipeline_ext_id: str):
    """Fetches the full run history for a given extraction pipeline."""
    return client.extraction_pipelines.runs.list(external_id=pipeline_ext_id, limit=-1)


def calculate_success_failure_stats(runs):
    """Calculates success and failure counts from a list of pipeline runs."""
    # ... (This function remains unchanged)
    success_count = sum(1 for run in runs if run.status == "success")
    failure_count = sum(1 for run in runs if run.status == "failure")
    return success_count, failure_count


def get_failed_run_details(runs):
    """Filters for failed runs and extracts their details, including IDs."""
    # ... (This function remains unchanged)
    failed_runs = []
    for run in runs:
        if run.status == "failure":
            parsed_message = parse_run_message(run.message)
            failed_runs.append(
                {
                    "timestamp": pd.to_datetime(run.created_time, unit="ms").tz_localize("UTC"),
                    "message": run.message,
                    "status": run.status,
                    "function_id": parsed_message.get("function_id"),
                    "call_id": parsed_message.get("call_id"),
                }
            )
    return sorted(failed_runs, key=lambda x: x["timestamp"], reverse=True)


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
    # ... (This function remains unchanged)
    launch_data = []
    finalize_runs_to_agg = []

    for run in runs:
        if run.status != "success":
            continue

        parsed = parse_run_message(run.message)
        if not parsed:
            continue

        timestamp = pd.to_datetime(run.created_time, unit="ms").tz_localize("UTC")
        count = parsed.get("total", 0)
        caller = parsed.get("caller")

        if caller == "Launch":
            launch_data.append({"timestamp": timestamp, "count": count, "type": "Launch"})
        elif caller == "Finalize":
            finalize_runs_to_agg.append({"timestamp": timestamp, "count": count})

    aggregated_finalize_data = []
    if finalize_runs_to_agg:
        finalize_runs_to_agg.sort(key=lambda x: x["timestamp"])
        current_group_start_time = finalize_runs_to_agg[0]["timestamp"]
        current_group_count = 0

        for run in finalize_runs_to_agg:
            if run["timestamp"] < current_group_start_time + timedelta(minutes=10):
                current_group_count += run["count"]
            else:
                aggregated_finalize_data.append(
                    {
                        "timestamp": current_group_start_time,
                        "count": current_group_count,
                        "type": "Finalize",
                    }
                )
                current_group_start_time = run["timestamp"]
                current_group_count = run["count"]

        if current_group_count > 0:
            aggregated_finalize_data.append(
                {
                    "timestamp": current_group_start_time,
                    "count": current_group_count,
                    "type": "Finalize",
                }
            )

    df_launch = pd.DataFrame(launch_data)
    df_finalize = pd.DataFrame(aggregated_finalize_data)

    return pd.concat([df_launch, df_finalize], ignore_index=True)


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

        data = [row.columns for row in rows]
        df_cache = pd.DataFrame(data)

        all_patterns = []
        for _, row in df_cache.iterrows():
            for sample_list in ["AssetPatternSamples", "FilePatternSamples"]:
                if row.get(sample_list) and isinstance(row[sample_list], list):
                    for item in row[sample_list]:
                        if item.get("sample") and item.get("resource_type"):
                            for pattern in item["sample"]:
                                all_patterns.append({"resourceType": item["resource_type"], "pattern": pattern})

        return pd.DataFrame(all_patterns)
    except Exception as e:
        st.error(f"Failed to fetch pattern catalog from '{table_name}': {e}")
        return pd.DataFrame()
