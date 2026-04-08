import time
from typing import Any, Callable, Optional

import pandas as pd
import streamlit as st
import yaml
from cognite.client import CogniteClient
from cognite.client.data_classes import RowList
from cognite.client.exceptions import CogniteAPIError, CogniteConnectionError
from constants import FieldNames
from data_structures import ExtractionPipelineConfig


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
                time.sleep(delay_seconds)
            except CogniteConnectionError:
                if attempt >= max_attempts:
                    raise
                time.sleep(delay_seconds)

    @staticmethod
    @st.cache_data(ttl=7200)
    def find_pipelines(_client: CogniteClient, name_filter: str = "file_annotation") -> list[str]:
        all_pipelines = DataFetcher._call_with_retries(func=_client.extraction_pipelines.list, limit=-1)

        if not all_pipelines:
            return []

        filtered_ids = [p.external_id for p in all_pipelines if name_filter in p.external_id]

        return sorted(filtered_ids)

    @staticmethod
    @st.cache_data(ttl=7200)
    def load_pipeline_config(_client: CogniteClient, pipeline_external_id: str) -> Optional[dict]:
        ep_configuration = DataFetcher._call_with_retries(func=_client.extraction_pipelines.config.retrieve, external_id=pipeline_external_id)

        if not ep_configuration:
            return None

        return yaml.safe_load(ep_configuration.config)

    @staticmethod
    def fetch_raw_table_as_dataframe(_client: CogniteClient, db_name: str, table_name: str, columns: list[str] | None = None, chunk_size: int = 1000) -> pd.DataFrame:
        rows = []

        for chunk_rows in DataFetcher._call_with_retries(func=_client.raw.rows,
            db_name=db_name,
            table_name=table_name,
            columns=columns,
            chunk_size=chunk_size,
            limit=-1
        ):
            rows.extend(chunk_rows)

        row_list = RowList(rows)

        return row_list.to_pandas() if rows else None


    @staticmethod
    @st.cache_data(ttl=7200)
    def fetch_manual_patterns(_client: CogniteClient, extraction_pipeline_cfg: ExtractionPipelineConfig) -> pd.DataFrame:
        if extraction_pipeline_cfg is None:
            return pd.DataFrame()

        raw_db = extraction_pipeline_cfg.raw_db
        manual_patterns_table = extraction_pipeline_cfg.raw_manual_patterns_catalog

        if not raw_db or not manual_patterns_table:
            return pd.DataFrame()

        df = DataFetcher.fetch_raw_table_as_dataframe(_client, db_name=raw_db, table_name=manual_patterns_table)

        if df is None or df.empty:
            return pd.DataFrame()
        
        rows: list[dict] = []

        for key, r in df.iterrows():
            pattern_scope = key
            patterns = r.get(FieldNames.PATTERNS_LOWER_CASE) or []

            for pattern in patterns:
                sample_val = pattern.get(FieldNames.SAMPLE_LOWER_CASE)
                resource_type = pattern.get(FieldNames.RESOURCE_TYPE_SNAKE_CASE)
                annotation_type = pattern.get(FieldNames.ANNOTATION_TYPE_SNAKE_CASE)

                if isinstance(sample_val, (list, tuple, set)):
                    for s in sample_val:
                        rows.append({
                            FieldNames.SAMPLE_LOWER_CASE: s,
                            FieldNames.RESOURCE_TYPE_SNAKE_CASE: resource_type,
                            FieldNames.ANNOTATION_TYPE_SNAKE_CASE: annotation_type,
                            FieldNames.PATTERN_SCOPE_SNAKE_CASE: pattern_scope,
                        })
                else:
                    rows.append({
                        FieldNames.SAMPLE_LOWER_CASE: sample_val,
                        FieldNames.RESOURCE_TYPE_SNAKE_CASE: resource_type,
                        FieldNames.ANNOTATION_TYPE_SNAKE_CASE: annotation_type,
                        FieldNames.PATTERN_SCOPE_SNAKE_CASE: pattern_scope,
                    })

        out = pd.DataFrame(rows)

        if out.empty:
            return out

        out[FieldNames.ANNOTATION_TYPE_SNAKE_CASE] = out[FieldNames.ANNOTATION_TYPE_SNAKE_CASE].apply(
            lambda x:
                FieldNames.ASSET_TITLE_CASE if x == FieldNames.DIAGRAMS_ASSET_LINK_CUSTOM_CASE
                else (FieldNames.FILE_TITLE_CASE if x == FieldNames.DIAGRAMS_FILE_LINK_CUSTOM_CASE else None)
        )

        out = out.drop_duplicates().reset_index(drop=True)

        return out

    @staticmethod
    @st.cache_data(ttl=7200)
    def fetch_automatic_patterns(_client: CogniteClient, extraction_pipeline_cfg: ExtractionPipelineConfig) -> pd.DataFrame:
        if extraction_pipeline_cfg is None:
            return pd.DataFrame()

        raw_db = extraction_pipeline_cfg.raw_db
        pattern_cache_table = extraction_pipeline_cfg.raw_table_pattern_cache

        if not raw_db or not pattern_cache_table:
            return pd.DataFrame()

        df = DataFetcher.fetch_raw_table_as_dataframe(_client, db_name=raw_db, table_name=pattern_cache_table, chunk_size=10)

        if df is None or df.empty:
            return pd.DataFrame()

        rows: list[dict] = []
 
        for key, r in df.iterrows():
            pattern_scope = key

            file_samples = r.get(FieldNames.FILE_PATTERN_SAMPLES_PASCAL_CASE) or []
            asset_samples = r.get(FieldNames.ASSET_PATTERN_SAMPLES_PASCAL_CASE) or []

            for file_sample in file_samples:
                sample_val = file_sample.get(FieldNames.SAMPLE_LOWER_CASE)
                resource_type = file_sample.get(FieldNames.RESOURCE_TYPE_SNAKE_CASE)
                annotation_type = file_sample.get(FieldNames.ANNOTATION_TYPE_SNAKE_CASE)

                if isinstance(sample_val, (list, tuple, set)):
                    for s in sample_val:
                        rows.append({
                            FieldNames.SAMPLE_LOWER_CASE: s,
                            FieldNames.RESOURCE_TYPE_SNAKE_CASE: resource_type,
                            FieldNames.ANNOTATION_TYPE_SNAKE_CASE: annotation_type,
                            FieldNames.PATTERN_SCOPE_SNAKE_CASE: pattern_scope,
                            FieldNames.ENTITY_TYPE_SNAKE_CASE: FieldNames.FILE_TITLE_CASE,
                        })
                else:
                    rows.append({
                        FieldNames.SAMPLE_LOWER_CASE: sample_val,
                        FieldNames.RESOURCE_TYPE_SNAKE_CASE: resource_type,
                        FieldNames.ANNOTATION_TYPE_SNAKE_CASE: annotation_type,
                        FieldNames.PATTERN_SCOPE_SNAKE_CASE: pattern_scope,
                        FieldNames.ENTITY_TYPE_SNAKE_CASE: FieldNames.FILE_TITLE_CASE,
                    })

            for asset_sample in asset_samples:
                sample_val = asset_sample.get(FieldNames.SAMPLE_LOWER_CASE)
                resource_type = asset_sample.get(FieldNames.RESOURCE_TYPE_SNAKE_CASE)
                annotation_type = asset_sample.get(FieldNames.ANNOTATION_TYPE_SNAKE_CASE)

                if isinstance(sample_val, (list, tuple, set)):
                    for s in sample_val:
                        rows.append({
                            FieldNames.SAMPLE_LOWER_CASE: s,
                            FieldNames.RESOURCE_TYPE_SNAKE_CASE: resource_type,
                            FieldNames.ANNOTATION_TYPE_SNAKE_CASE: annotation_type,
                            FieldNames.PATTERN_SCOPE_SNAKE_CASE: pattern_scope,
                            FieldNames.ENTITY_TYPE_SNAKE_CASE: FieldNames.ASSET_TITLE_CASE,
                        })
                else:
                    rows.append({
                        FieldNames.SAMPLE_LOWER_CASE: sample_val,
                        FieldNames.RESOURCE_TYPE_SNAKE_CASE: resource_type,
                        FieldNames.ANNOTATION_TYPE_SNAKE_CASE: annotation_type,
                        FieldNames.PATTERN_SCOPE_SNAKE_CASE: pattern_scope,
                        FieldNames.ENTITY_TYPE_SNAKE_CASE: FieldNames.ASSET_TITLE_CASE,
                    })

        out = pd.DataFrame(rows)

        if out.empty:
            return out

        out[FieldNames.ANNOTATION_TYPE_SNAKE_CASE] = out[FieldNames.ANNOTATION_TYPE_SNAKE_CASE].apply(
            lambda x:
                FieldNames.ASSET_TITLE_CASE if x == FieldNames.DIAGRAMS_ASSET_LINK_CUSTOM_CASE
                else (FieldNames.FILE_TITLE_CASE if x == FieldNames.DIAGRAMS_FILE_LINK_CUSTOM_CASE else None)
        )

        out = out.drop_duplicates().reset_index(drop=True)

        return out


    @staticmethod
    @st.cache_data(ttl=7200)
    def fetch_data_model_instances_as_list(_client: CogniteClient, view_cfg, _filter_obj=None) -> list:
        if _client is None or view_cfg is None:
            return []

        try:
            kwargs = dict(
                instance_type="node",
                sources=view_cfg.as_view_id(),
                space=view_cfg.instance_space,
                limit=-1,
            )
            if _filter_obj is not None:
                kwargs["filter"] = _filter_obj

            nodes = DataFetcher._call_with_retries(
                func=_client.data_modeling.instances.list,
                **kwargs,
            )
        except Exception as e:
            st.error(f"Could not list instances for view: {e}")
            return []

        return list(nodes) if nodes else []
