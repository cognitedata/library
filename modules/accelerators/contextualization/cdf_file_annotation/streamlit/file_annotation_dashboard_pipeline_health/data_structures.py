from dataclasses import dataclass
from enum import Enum
from typing import Optional, List, Tuple
from cognite.client.data_classes.data_modeling.ids import ViewId
import pandas as pd
from constants import FieldNames


@dataclass
class KPI:
    awaiting_processing: int = 0
    processed_total: int = 0
    failed_total: int = 0
    failure_rate_total: float = 0.0


@dataclass
class RunRecord:
    timestamp: object = None
    count: int = 0
    type: str | None = None


@dataclass
class ViewPropertyConfig:
    schema_space: str
    external_id: str
    version: str
    instance_space: Optional[str] = None

    def as_view_id(self) -> ViewId:
        return ViewId(space=self.schema_space, external_id=self.external_id, version=self.version)

    def as_property_ref(self, property_name: str) -> List[str]:
        return [self.schema_space, f"{self.external_id}/{self.version}", property_name]


class CallerType(str, Enum):
    PREPARE = "Prepare"
    LAUNCH = "Launch"
    FINALIZE = "Finalize"
    PROMOTE = "Promote"


@dataclass
class FunctionRunConfig:
    caller_type: CallerType
    function_id_field: str
    function_call_id_field: str
    log_title: str
    log_snake_case: str


@dataclass
class ApplyServiceConfig:
    raw_db: str
    raw_table_pattern_tags: str
    raw_table_asset_tags: str
    raw_table_file_tags: str

    @classmethod
    def from_dict(cls, d: dict | None):
        if not isinstance(d, dict):
            return cls(raw_db=None, raw_table_pattern_tags=None, raw_table_asset_tags=None, raw_table_file_tags=None)
        return cls(
            raw_db=d.get(FieldNames.RAW_DATABASE_CAMEL_CASE),
            raw_table_pattern_tags=d.get(FieldNames.RAW_TABLE_DOC_PATTERN_CAMEL_CASE),
            raw_table_asset_tags=d.get(FieldNames.RAW_TABLE_DOC_TAG_CAMEL_CASE),
            raw_table_file_tags=d.get(FieldNames.RAW_TABLE_DOC_DOC_CAMEL_CASE),
        )

@dataclass
class CacheServiceConfig:
    raw_db: str
    raw_table_pattern_tags: str
    raw_table_asset_tags: str
    raw_table_file_tags: str

    @classmethod
    def from_dict(cls, d: dict | None):
        if not isinstance(d, dict):
            return cls(
                raw_db=None,
                raw_table_pattern_tags=None,
                raw_table_asset_tags=None,
                raw_table_file_tags=None
            )

        return cls(
            raw_db=d.get(FieldNames.RAW_DATABASE_CAMEL_CASE),
            raw_table_pattern_tags=d.get(FieldNames.RAW_TABLE_DOC_PATTERN_CAMEL_CASE),
            raw_table_asset_tags=d.get(FieldNames.RAW_TABLE_DOC_TAG_CAMEL_CASE),
            raw_table_file_tags=d.get(FieldNames.RAW_TABLE_DOC_DOC_CAMEL_CASE),
        )

@dataclass
class LaunchFunctionConfig:
    secondary_scope_property: str
    asset_resource_property: str
    file_resource_property: str
    cache_service: CacheServiceConfig

    @classmethod
    def from_dict(cls, d: dict | None):
        if not isinstance(d, dict):
            return cls(cache_service=CacheServiceConfig.from_dict(None))

        cache_service = d.get(FieldNames.CACHE_SERVICE_CAMEL_CASE)

        return cls(
            secondary_scope_property=d.get(FieldNames.SECONDARY_SCOPE_PROPERTY_CAMEL_CASE),
            asset_resource_property=d.get(FieldNames.ASSET_RESOURCE_PROPERTY_CAMEL_CASE),
            file_resource_property=d.get(FieldNames.FILE_RESOURCE_PROPERTY_CAMEL_CASE),
            cache_service=CacheServiceConfig.from_dict(cache_service),
        )
    

@dataclass
class FinalizeFunctionConfig:
    apply_service: ApplyServiceConfig

    @classmethod
    def from_dict(cls, d: dict | None):
        if not isinstance(d, dict):
            return cls(
                apply_service=ApplyServiceConfig.from_dict(None)
            )

        apply_service = d.get(FieldNames.APPLY_SERVICE_CAMEL_CASE)

        return cls(apply_service=ApplyServiceConfig.from_dict(apply_service))


@dataclass
class LaunchFunctionConfig:
    secondary_scope_property: str
    asset_resource_property: str
    file_resource_property: str
    cache_service: CacheServiceConfig

    @classmethod
    def from_dict(cls, d: dict | None):
        if not isinstance(d, dict):
            return cls(cache_service=CacheServiceConfig.from_dict(None))

        cache_service = d.get(FieldNames.CACHE_SERVICE_CAMEL_CASE)

        return cls(
            secondary_scope_property=d.get(FieldNames.SECONDARY_SCOPE_PROPERTY_CAMEL_CASE),
            asset_resource_property=d.get(FieldNames.ASSET_RESOURCE_PROPERTY_CAMEL_CASE),
            file_resource_property=d.get(FieldNames.FILE_RESOURCE_PROPERTY_CAMEL_CASE),
            cache_service=CacheServiceConfig.from_dict(cache_service),
        )
    

@dataclass
class FinalizeFunctionConfig:
    apply_service: ApplyServiceConfig

    @classmethod
    def from_dict(cls, d: dict | None):
        if not isinstance(d, dict):
            return cls(
                apply_service=ApplyServiceConfig.from_dict(None)
            )

        apply_service = d.get(FieldNames.APPLY_SERVICE_CAMEL_CASE)

        return cls(apply_service=ApplyServiceConfig.from_dict(apply_service))


@dataclass
class ExtractionPipelineConfig:
    launch_function: LaunchFunctionConfig
    finalize_function: FinalizeFunctionConfig
    file_view_cfg: ViewPropertyConfig | None = None
    asset_view_cfg: ViewPropertyConfig | None = None
    annotation_state_view_cfg: ViewPropertyConfig | None = None

    @classmethod
    def from_dict(cls, d: dict | None):
        if not isinstance(d, dict):
            return cls(
                launch_function=LaunchFunctionConfig.from_dict(None),
                finalize_function=FinalizeFunctionConfig.from_dict(None)
            )

        launch_function = d.get(FieldNames.LAUNCH_FUNCTION_CAMEL_CASE)
        finalize_function = d.get(FieldNames.FINALIZE_FUNCTION_CAMEL_CASE)

        data_model_views = d.get(FieldNames.DATA_MODEL_VIEWS_CAMEL_CASE, {}) or {}

        def _build_view(cfg_dict):
            if not cfg_dict:
                return None
            return ViewPropertyConfig(
                schema_space=cfg_dict.get(FieldNames.SCHEMA_SPACE_CAMEL_CASE),
                external_id=cfg_dict.get(FieldNames.EXTERNAL_ID_CAMEL_CASE),
                version=str(cfg_dict.get(FieldNames.VERSION_CAMEL_CASE)),
                instance_space=cfg_dict.get(FieldNames.INSTANCE_SPACE_CAMEL_CASE),
            )

        annotation_state_view = _build_view(data_model_views.get(FieldNames.ANNOTATION_STATE_VIEW_CAMEL_CASE))
        file_view = _build_view(data_model_views.get(FieldNames.FILE_VIEW_CAMEL_CASE))
        asset_view = _build_view(data_model_views.get(FieldNames.ASSET_VIEW_CAMEL_CASE))

        return cls(
            launch_function=LaunchFunctionConfig.from_dict(launch_function),
            finalize_function=FinalizeFunctionConfig.from_dict(finalize_function),
            file_view_cfg=file_view,
            asset_view_cfg=asset_view,
            annotation_state_view_cfg=annotation_state_view,
        )

    @property
    def file_resource_property(self) -> str | None:
        return getattr(self.launch_function, FieldNames.FILE_RESOURCE_PROPERTY_SNAKE_CASE, None)

    @property
    def asset_resource_property(self) -> str | None:
        return getattr(self.launch_function, FieldNames.ASSET_RESOURCE_PROPERTY_SNAKE_CASE, None)

    @property
    def secondary_scope_property(self) -> str | None:
        return getattr(self.launch_function, FieldNames.SECONDARY_SCOPE_PROPERTY_SNAKE_CASE, None)

    @property
    def db_name(self) -> str | None:
        cache = getattr(self.launch_function, FieldNames.CACHE_SERVICE_SNAKE_CASE, None)
        apply = getattr(self.finalize_function, FieldNames.APPLY_SERVICE_SNAKE_CASE, None)

        if cache and getattr(cache, FieldNames.RAW_DB_SNAKE_CASE, None):
            return cache.raw_db
        if apply and getattr(apply, FieldNames.RAW_DB_SNAKE_CASE, None):
            return apply.raw_db
        return None

    @property
    def pattern_table_name(self) -> str | None:
        cache = getattr(self.launch_function, FieldNames.CACHE_SERVICE_SNAKE_CASE, None)
        apply = getattr(self.finalize_function, FieldNames.APPLY_SERVICE_SNAKE_CASE, None)
        return (getattr(cache, FieldNames.RAW_TABLE_PATTERN_TAGS_SNAKE_CASE, None) or getattr(apply, FieldNames.RAW_TABLE_PATTERN_TAGS_SNAKE_CASE, None))

    @property
    def asset_table_name(self) -> str | None:
        cache = getattr(self.launch_function, FieldNames.CACHE_SERVICE_SNAKE_CASE, None)
        apply = getattr(self.finalize_function, FieldNames.APPLY_SERVICE_SNAKE_CASE, None)
        return (getattr(cache, FieldNames.RAW_TABLE_ASSET_TAGS_SNAKE_CASE, None) or getattr(apply, FieldNames.RAW_TABLE_ASSET_TAGS_SNAKE_CASE, None))

    @property
    def file_table_name(self) -> str | None:
        cache = getattr(self.launch_function, FieldNames.CACHE_SERVICE_SNAKE_CASE, None)
        apply = getattr(self.finalize_function, FieldNames.APPLY_SERVICE_SNAKE_CASE, None)
        return (getattr(cache, FieldNames.RAW_TABLE_FILE_TAGS_SNAKE_CASE, None) or getattr(apply, FieldNames.RAW_TABLE_FILE_TAGS_SNAKE_CASE, None))
        
    


