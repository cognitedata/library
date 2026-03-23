import streamlit as st
import pandas as pd
import yaml
import time
from typing import Optional
from typing import Callable, Any
from data_structures import ViewPropertyConfig
from constants import FieldNames
from cognite.client import CogniteClient
from cognite.client.data_classes import RowList
from cognite.client.data_classes.data_modeling import NodeId, filters
from cognite.client.exceptions import CogniteAPIError
from data_structures import CallerType


class DataFetcher:
    @staticmethod
    def _call_with_retries(func: Callable[..., Any], *args, max_attempts: int = 100, delay_seconds: float = 10.0, **kwargs) -> Any:
        attempt = 0
        while True:
            attempt += 1
            try:
                return func(*args, **kwargs)
            except CogniteAPIError as e:
                if e.code != 408 and e.code != 429:
                    raise
                if attempt >= max_attempts:
                    raise
                sleep_time = delay_seconds * (2 ** (attempt - 1))
                time.sleep(sleep_time)

    @staticmethod
    @st.cache_data(ttl=3600)
    def find_pipelines(_client: CogniteClient, name_filter: str = "file_annotation") -> list[str]:
        all_pipelines = DataFetcher._call_with_retries(_client.extraction_pipelines.list, limit=-1)
        if not all_pipelines:
            return []
        filtered_ids = [p.external_id for p in all_pipelines if name_filter in p.external_id]
        return sorted(filtered_ids)

    @staticmethod
    @st.cache_data(ttl=3600)
    def load_pipeline_config(_client: CogniteClient, pipeline_external_id: str) -> Optional[dict]:
        ep_configuration = DataFetcher._call_with_retries(_client.extraction_pipelines.config.retrieve, external_id=pipeline_external_id)
        if not ep_configuration:
            return None
        return yaml.safe_load(ep_configuration.config)

    @staticmethod
    def _list_raw_rows(_client: CogniteClient, db_name: str, table_name: str, filter: dict | None = None, chunk_size: int = 1000):
        if filter:
            return _client.raw.rows.list(db_name=db_name, table_name=table_name, filter=filter, limit=-1)
        all_rows = RowList([])
        for chunk in _client.raw.rows(db_name=db_name, table_name=table_name, chunk_size=chunk_size, limit=None):
            all_rows.extend(chunk)
        return all_rows

    @staticmethod
    def fetch_raw_table_as_dataframe(_client: CogniteClient, db_name: str, table_name: str) -> pd.DataFrame:
        try:
            rows = DataFetcher._list_raw_rows(_client=_client, db_name=db_name, table_name=table_name)
        except Exception:
            return pd.DataFrame()
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame([r.columns for r in rows])

    @staticmethod
    @st.cache_data(ttl=3600)
    def fetch_annotation_states(_client: CogniteClient, extraction_pipeline_cfg) -> pd.DataFrame:
        annotation_state_view_cfg = extraction_pipeline_cfg.annotation_state_view_cfg
        file_view_cfg = extraction_pipeline_cfg.file_view_cfg

        annotation_space = annotation_state_view_cfg.instance_space if annotation_state_view_cfg is not None else None
        annotation_sources = [annotation_state_view_cfg.as_view_id()] if annotation_state_view_cfg is not None else None

        annotation_instances = DataFetcher._call_with_retries(
            _client.data_modeling.instances.list,
            instance_type="node",
            space=annotation_space,
            sources=annotation_sources,
            limit=-1,
        )

        if not annotation_instances:
            return pd.DataFrame()
        

        annotation_data: list[dict] = []
        nodes_to_fetch: list[NodeId] = []

        ann_view_obj = annotation_state_view_cfg.as_view_id() if annotation_state_view_cfg is not None else None
        file_view_obj = file_view_cfg.as_view_id() if file_view_cfg is not None else None

        for instance in annotation_instances:
            node_row = {
                FieldNames.EXTERNAL_ID_CAMEL_CASE: instance.external_id,
                FieldNames.FILE_SPACE_CAMEL_CASE: instance.space,
                FieldNames.CREATED_TIME_CAMEL_CASE: pd.to_datetime(instance.created_time, unit="ms"),
                FieldNames.LAST_UPDATED_TIME_CAMEL_CASE: pd.to_datetime(instance.last_updated_time, unit="ms"),
            }

            props = {}

            if ann_view_obj is not None and ann_view_obj in instance.properties:
                props = instance.properties.get(ann_view_obj, {})

            for prop_key, prop_value in props.items():
                if prop_key == FieldNames.LINKED_FILE_CAMEL_CASE and prop_value:
                    file_external_id = prop_value.get(FieldNames.EXTERNAL_ID_CAMEL_CASE)
                    file_space = prop_value.get(FieldNames.SPACE_LOWER_CASE)

                    node_row[FieldNames.FILE_EXTERNAL_ID_CAMEL_CASE] = file_external_id
                    node_row[FieldNames.FILE_SPACE_CAMEL_CASE] = file_space

                    if file_external_id and file_space:
                        nodes_to_fetch.append(NodeId(space=file_space, external_id=file_external_id))

                node_row[prop_key] = prop_value

            annotation_data.append(node_row)

        df_annotations = pd.DataFrame(annotation_data)

        if df_annotations.empty or not nodes_to_fetch:
            return df_annotations

        unique_nodes_to_fetch = list({(n.space, n.external_id): n for n in nodes_to_fetch}.values())
        file_sources = [file_view_cfg.as_view_id()] if file_view_cfg is not None else None
        file_instances = DataFetcher._call_with_retries(_client.data_modeling.instances.retrieve_nodes, nodes=unique_nodes_to_fetch, sources=file_sources)

        file_data: list[dict] = []

        for instance in file_instances:
            file_row = {
                FieldNames.FILE_EXTERNAL_ID_CAMEL_CASE: instance.external_id,
                FieldNames.FILE_SPACE_CAMEL_CASE: instance.space
            }

            properties = {}

            if file_view_obj is not None and file_view_obj in instance.properties:
                properties = instance.properties.get(file_view_obj, {})

            for prop_key, prop_value in properties.items():
                file_row[f"file{prop_key.capitalize()}"] = ", ".join(map(str, prop_value)) if isinstance(prop_value, list) else prop_value

            file_data.append(file_row)

        if not file_data:
            return df_annotations

        df_files = pd.DataFrame(file_data)
        df_merged = pd.merge(df_annotations, df_files, on=[FieldNames.FILE_EXTERNAL_ID_CAMEL_CASE, FieldNames.FILE_SPACE_CAMEL_CASE], how="left")

        for col in [FieldNames.CREATED_TIME_CAMEL_CASE, FieldNames.LAST_UPDATED_TIME_CAMEL_CASE]:
            if col in df_merged.columns:
                df_merged[col] = df_merged[col].dt.tz_localize("UTC")

        df_merged.rename(columns={FieldNames.ANNOTATION_STATUS_CAMEL_CASE: FieldNames.STATUS_LOWER_CASE, FieldNames.ATTEMPT_COUNT_CAMEL_CASE: "retries"}, inplace=True)
        return df_merged

    @staticmethod
    @st.cache_data(ttl=3600)
    def fetch_pipeline_run_history(_client: CogniteClient, pipeline_external_id: str) -> list:
        if not pipeline_external_id:
            return []

        runs = DataFetcher._call_with_retries(_client.extraction_pipelines.runs.list, external_id=pipeline_external_id, limit=-1)

        return list(runs) if runs else []

    @staticmethod
    def fetch_function_logs(_client: CogniteClient, function_id: int, call_id: int) -> str:
        try:
            log_obj = DataFetcher._call_with_retries(_client.functions.calls.get_logs, call_id, function_id)
        except Exception:
            return ""

        try:
            text = log_obj.to_text(with_timestamps=False) if hasattr(log_obj, FieldNames.TO_TEXT_SNAKE_CASE) else str(log_obj)
            return text or ""
        except Exception:
            return str(log_obj) if log_obj is not None else ""

    @staticmethod
    @st.cache_data(ttl=3600)
    def fetch_files_by_function_call_id(_client: CogniteClient, call_id: int, annotation_state_view: ViewPropertyConfig, caller_type: str | None = None) -> list:
        if not call_id or annotation_state_view is None:
            return []

        mapping = {
            CallerType.LAUNCH: FieldNames.LAUNCH_FUNCTION_CALL_ID_CAMEL_CASE,
            CallerType.FINALIZE: FieldNames.FINALIZE_FUNCTION_CALL_ID_CAMEL_CASE,
            CallerType.PREPARE: FieldNames.PREPARE_FUNCTION_CALL_ID_CAMEL_CASE,
            CallerType.PROMOTE: FieldNames.PROMOTE_FUNCTION_CALL_ID_CAMEL_CASE,
        }

        view_id = annotation_state_view.as_view_id()

        if caller_type and caller_type in mapping:
            prop = mapping[caller_type]
            call_id_filter = filters.Equals(annotation_state_view.as_property_ref(prop), call_id)
        else:
            return []

        try:
            instances = DataFetcher._call_with_retries(
                _client.data_modeling.instances.list,
                instance_type="node",
                sources=[view_id],
                filter=call_id_filter,
                limit=-1,
            )

            if not instances:
                return []

            file_external_ids = []

            for instance in instances:
                props = instance.properties.get(view_id, {}) if view_id in instance.properties else {}
                linked_file = props.get(FieldNames.LINKED_FILE_CAMEL_CASE, {}) or {}
                file_external_id = linked_file.get(FieldNames.EXTERNAL_ID_CAMEL_CASE)
                if file_external_id:
                    file_external_ids.append(str(file_external_id))

            return file_external_ids
        except Exception:
            return []
