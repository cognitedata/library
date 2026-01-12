import streamlit as st
import pandas as pd
import yaml
import time
from typing import Optional, Callable, Any
from data_structures import ViewPropertyConfig, CallerType, AnnotationFrames, ExtractionPipelineConfig, AnnotationStatus
from constants import FieldNames
from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import filters
from cognite.client.exceptions import CogniteAPIError
from data_processor import DataProcessor


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
    def fetch_raw_table_as_dataframe(_client: CogniteClient, db_name: str, table_name: str, columns: list[str] | None = None) -> pd.DataFrame:
        rows = DataFetcher._call_with_retries(func=_client.raw.rows.list,
            db_name=db_name,
            table_name=table_name,
            columns=columns,
            limit=-1
        )

        return rows.to_pandas() if rows else None

    @staticmethod
    @st.cache_data(ttl=7200)
    def fetch_annotations(_client: CogniteClient, extraction_pipeline_cfg: ExtractionPipelineConfig) -> AnnotationFrames:
        db_name = extraction_pipeline_cfg.raw_db
        pattern_tags_tbl_name = extraction_pipeline_cfg.raw_table_pattern_tags
        asset_tags_tbl_name = extraction_pipeline_cfg.raw_table_asset_tags
        file_tags_tbl_name = extraction_pipeline_cfg.raw_table_file_tags

        annotation_columns = [
            FieldNames.EXTERNAL_ID_CAMEL_CASE,
            FieldNames.START_NODE_RESOURCE_CAMEL_CASE,
            FieldNames.END_NODE_RESOURCE_CAMEL_CASE,
            FieldNames.STATUS_LOWER_CASE,
            FieldNames.START_NODE_TEXT_CAMEL_CASE,
            FieldNames.START_NODE_CAMEL_CASE,
            FieldNames.END_NODE_RESOURCE_TYPE_CAMEL_CASE,
            FieldNames.START_SOURCE_ID_CAMEL_CASE,
            FieldNames.END_NODE_CAMEL_CASE,
            FieldNames.END_NODE_SPACE_CAMEL_CASE,
            FieldNames.TAGS_LOWER_CASE,
        ]

        actual_df: pd.DataFrame = pd.DataFrame(columns=annotation_columns)
        potential_df: pd.DataFrame = pd.DataFrame(columns=annotation_columns)

        if db_name and asset_tags_tbl_name:
            actual_df = DataFetcher.fetch_raw_table_as_dataframe(
                _client,
                db_name=db_name,
                table_name=asset_tags_tbl_name,
                columns=annotation_columns
            )

        if db_name and file_tags_tbl_name:
            docs_df = DataFetcher.fetch_raw_table_as_dataframe(
                _client,
                db_name=db_name,
                table_name=file_tags_tbl_name,
                columns=annotation_columns
            )

            if not docs_df.empty:
                if actual_df is None or actual_df.empty:
                    actual_df = docs_df
                else:
                    actual_df = pd.concat([actual_df, docs_df], ignore_index=True)
            del docs_df

        actual_df = DataFetcher._filter_empty_rows(actual_df)
        if not actual_df.empty:
            actual_df[FieldNames.IS_FROM_PATTERN_TABLE_CAMEL_CASE] = False

        if db_name and pattern_tags_tbl_name:
            potential_df = DataFetcher.fetch_raw_table_as_dataframe(
                _client,
                db_name=db_name,
                table_name=pattern_tags_tbl_name,
                columns=annotation_columns
            )
            potential_df = DataFetcher._filter_empty_rows(potential_df)

        if not potential_df.empty:
            potential_df[FieldNames.IS_FROM_PATTERN_TABLE_CAMEL_CASE] = True

        if not potential_df.empty and FieldNames.STATUS_LOWER_CASE in potential_df.columns:
            approved_mask = potential_df[FieldNames.STATUS_LOWER_CASE].astype(str) == AnnotationStatus.APPROVED.value
            if approved_mask.any():
                approved_rows = potential_df[approved_mask]
                if not approved_rows.empty:
                    if not actual_df.empty:
                        for col in approved_rows.columns:
                            if col not in actual_df.columns:
                                actual_df[col] = None
                        for col in actual_df.columns:
                            if col not in approved_rows.columns:
                                approved_rows[col] = None
                        actual_df = pd.concat([actual_df, approved_rows[actual_df.columns]], ignore_index=True, sort=False)
                    else:
                        actual_df = approved_rows.reset_index(drop=True)
                    potential_df = potential_df[~approved_mask].reset_index(drop=True)

        norm_col = FieldNames.NORMALIZED_STATUS_CAMEL_CASE

        try:
            if actual_df is not None and not actual_df.empty:
                actual_df[norm_col] = actual_df.apply(lambda r: DataProcessor.derive_normalized_status(r), axis=1)
        except Exception:
            pass

        try:
            if potential_df is not None and not potential_df.empty:
                potential_df[norm_col] = potential_df.apply(lambda r: DataProcessor.derive_normalized_status(r), axis=1)
        except Exception:
            pass

        return AnnotationFrames(actual_df=actual_df, potential_df=potential_df)

    @staticmethod
    def _filter_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return df

        column = FieldNames.END_NODE_CAMEL_CASE

        if column not in df.columns:
            return df

        mask = df[column].isna()

        try:
            mask = mask | df[column].astype(str).str.strip().eq("")
        except Exception:
            mask = mask | (df[column] == "")

        return df.loc[~mask].reset_index(drop=True)

    @staticmethod
    @st.cache_data(ttl=7200)
    def fetch_entities_metadata(_client: CogniteClient, extraction_pipeline_cfg: ExtractionPipelineConfig | None = None, entity_type: str | None = None, _filter_expression: object | None = None):
        if extraction_pipeline_cfg is not None:
            if entity_type == FieldNames.ASSET_TITLE_CASE:
                entity_view_cfg = extraction_pipeline_cfg.asset_view_cfg
                entity_resource_type_property = extraction_pipeline_cfg.asset_resource_property
            elif entity_type == FieldNames.FILE_TITLE_CASE:
                entity_view_cfg = extraction_pipeline_cfg.file_view_cfg
                entity_resource_type_property = extraction_pipeline_cfg.file_resource_property
            else:
                entity_view_cfg = extraction_pipeline_cfg.file_view_cfg
                entity_resource_type_property = extraction_pipeline_cfg.file_resource_property
            secondary_scope_property = extraction_pipeline_cfg.secondary_scope_property

        entity_space = entity_view_cfg.instance_space if entity_view_cfg is not None else None
        entity_sources = [entity_view_cfg.as_view_id()] if entity_view_cfg is not None else None

        metadata_columns = [
            FieldNames.SOURCE_ID_CAMEL_CASE,
            FieldNames.NAME_LOWER_CASE,
        ]

        if secondary_scope_property:
            metadata_columns.append(secondary_scope_property)

        if entity_resource_type_property:
            metadata_columns.append(entity_resource_type_property)

        entities_df = pd.DataFrame(columns=[FieldNames.EXTERNAL_ID_CAMEL_CASE] + metadata_columns)

        for nodes in DataFetcher._call_with_retries(
            func=_client.data_modeling.instances,
            instance_type="node",
            space=entity_space,
            sources=entity_sources,
            filter=_filter_expression,
            chunk_size=1000,
            limit=-1,
        ):
            chunk_rows: list[dict] = []
            for node in nodes:
                props = node.properties[entity_view_cfg.as_view_id()]

                external_id = node.external_id

                row_dict = {
                    FieldNames.EXTERNAL_ID_CAMEL_CASE: external_id,
                }

                for metadata_column in metadata_columns:
                    if metadata_column in props:
                        row_dict[metadata_column] = props[metadata_column]

                chunk_rows.append(row_dict)

            if chunk_rows:
                df_chunk = pd.DataFrame(chunk_rows, columns=[FieldNames.EXTERNAL_ID_CAMEL_CASE] + metadata_columns)
                entities_df = pd.concat([entities_df, df_chunk], ignore_index=True, sort=False)

        return entities_df

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

        df = DataFetcher.fetch_raw_table_as_dataframe(_client, db_name=raw_db, table_name=pattern_cache_table)

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
