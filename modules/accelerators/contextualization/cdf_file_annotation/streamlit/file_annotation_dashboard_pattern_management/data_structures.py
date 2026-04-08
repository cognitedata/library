from dataclasses import dataclass
from enum import Enum
from typing import Optional, List, Any
from cognite.client.data_classes.data_modeling.ids import ViewId
from cognite.client.data_classes.filters import Filter
from cognite.client import data_modeling as dm
import pandas as pd
from constants import FieldNames
import logging
import streamlit as st

logger = logging.getLogger(__name__)
 
class FilterOperator(str, Enum):
    EQUALS = "Equals"
    EXISTS = "Exists"
    CONTAINSALL = "ContainsAll"
    IN = "In"
    SEARCH = "Search"

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
class AnnotationTag:
    tag_text: str
    resource_type: str
    secondary_scope: str
    status: str
    annotation_type: str | None = None

class NormalizedStatus(str, Enum):
    REGULARLY_ANNOTATED = "Regularly Annotated"
    AUTOMATICALLY_PROMOTED = "Automatically Promoted"
    MANUALLY_PROMOTED = "Manually Promoted"
    PATTERN_FOUND = "Pattern Found"
    NO_MATCH = "No Match"
    AMBIGUOUS = "Ambiguous"

class TagsStatus(str, Enum):
    PROMOTED_AUTO = "PromotedAuto"
    PROMOTED_MANUALLY = "PromotedManually"
    PROMOTE_ATTEMPTED = "PromoteAttempted"
    AMBIGUOUS_MATCH = "AmbiguousMatch"  

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


@dataclass
class FilterConfig:
    values: Optional[list[Any] | Any] = None
    negate: bool = False
    operator: Optional[FilterOperator] = None
    target_property: str = ""

    @classmethod
    def from_dict(cls, d: dict | None):
        if not isinstance(d, dict):
            return None

        target = d.get(FieldNames.TARGET_PROPERTY_CAMEL_CASE) or d.get(FieldNames.TARGET_PROPERTY_SNAKE_CASE)
        op_raw = d.get(FieldNames.OPERATOR_CAMEL_CASE) or d.get(FieldNames.OPERATOR_SNAKE_CASE) or d.get("op")

        operator = None
        if op_raw is not None:
            operator = FilterOperator(op_raw)

        values = d.get(FieldNames.VALUES_CAMEL_CASE) if d.get(FieldNames.VALUES_CAMEL_CASE) is not None else d.get(FieldNames.VALUES_SNAKE_CASE, d.get("value"))
        negate = bool(d.get(FieldNames.NEGATE_CAMEL_CASE, d.get(FieldNames.NEGATE_SNAKE_CASE, False)))

        return cls(target_property=target, operator=operator, values=values, negate=negate)

    def matches(self, node_properties: dict) -> bool:
        if not isinstance(node_properties, dict):
            return False
        prop = self.target_property
        val = node_properties.get(prop) if prop else None

        find_values = self.values
        if isinstance(find_values, list):
            find_values = [v.value if hasattr(v, "value") else v for v in find_values]
        elif hasattr(find_values, "value"):
            find_values = find_values.value

        op = self.operator if self.operator is not None else FilterOperator.EXISTS

        passed = False
        if op == FilterOperator.EXISTS:
            passed = val is not None and not (isinstance(val, str) and str(val).strip() == "")
        elif op == FilterOperator.EQUALS:
            if isinstance(find_values, (list, tuple)):
                passed = any(str(val) == str(v) for v in find_values)
            else:
                passed = str(val) == str(find_values)
        elif op == FilterOperator.IN:
            if isinstance(find_values, (list, tuple)):
                passed = any(str(val) == str(v) for v in find_values)
            else:
                passed = str(val) in str(find_values)
        elif op == FilterOperator.CONTAINSALL:
            if val is None:
                passed = False
            else:
                if isinstance(val, (list, tuple)):
                    if isinstance(find_values, (list, tuple)):
                        passed = all(any(str(x) == str(fv) for x in val) for fv in find_values)
                    else:
                        passed = any(str(x) == str(find_values) for x in val)
                else:
                    if isinstance(find_values, (list, tuple)):
                        passed = all(str(fv) in str(val) for fv in find_values)
                    else:
                        passed = str(find_values) in str(val)
        elif op == FilterOperator.SEARCH:
            if val is None:
                passed = False
            else:
                if isinstance(find_values, (list, tuple)):
                    passed = any(str(fv) in str(val) for fv in find_values)
                else:
                    passed = str(find_values) in str(val)
        else:
            if find_values is None:
                passed = val is not None
            else:
                if isinstance(find_values, (list, tuple)):
                    passed = any(str(val) == str(v) for v in find_values)
                else:
                    passed = str(val) == str(find_values)

        return (not passed) if self.negate else passed

    def as_filter(self, view_properties: ViewPropertyConfig) -> Filter:
        property_reference = view_properties.as_property_ref(self.target_property)

        if isinstance(self.values, list):
            find_values = [v.value if hasattr(v, "value") else v for v in self.values]
        elif hasattr(self.values, "value"):
            find_values = self.values.value
        else:
            find_values = self.values

        if find_values is None:
            if self.operator == FilterOperator.EXISTS:
                return dm.filters.Exists(property=property_reference)
            else:
                raise ValueError(f"Operator {self.operator} requires a value")

        if self.operator == FilterOperator.IN:
            if not isinstance(find_values, list):
                raise ValueError(f"Operator 'IN' requires a list of values for property {self.target_property}")
            filt = dm.filters.In(property=property_reference, values=find_values)
        elif self.operator == FilterOperator.EQUALS:
            filt = dm.filters.Equals(property=property_reference, value=find_values)
        elif self.operator == FilterOperator.CONTAINSALL:
            filt = dm.filters.ContainsAll(property=property_reference, values=find_values)
        elif self.operator == FilterOperator.SEARCH:
            filt = dm.filters.Search(property=property_reference, value=find_values)
        else:
            raise NotImplementedError(f"Operator {self.operator} is not implemented.")

        if self.negate:
            return dm.filters.Not(filt)
        else:
            return filt


@dataclass
class QueryConfig:
    target_view: ViewPropertyConfig | None = None
    filters: List[FilterConfig] | None = None
    limit: int | None = -1

    @classmethod
    def from_dict(cls, d: dict | None, view_from_query_fn=None):
        if not isinstance(d, dict):
            return None
        if view_from_query_fn is None:
            def _vf(x):
                if not isinstance(x, dict):
                    return None
                tv = x.get(FieldNames.TARGET_VIEW_CAMEL_CASE) or x.get(FieldNames.TARGET_VIEW_SNAKE_CASE)
                if not isinstance(tv, dict):
                    return None
                return ViewPropertyConfig(
                    schema_space=tv.get(FieldNames.SCHEMA_SPACE_CAMEL_CASE),
                    external_id=tv.get(FieldNames.EXTERNAL_ID_CAMEL_CASE),
                    version=str(tv.get(FieldNames.VERSION_CAMEL_CASE)),
                    instance_space=tv.get(FieldNames.INSTANCE_SPACE_CAMEL_CASE),
                )
            view_from_query_fn = _vf

        view = view_from_query_fn(d)

        raw_filters = d.get(FieldNames.FILTERS_CAMEL_CASE) or d.get(FieldNames.FILTERS_SNAKE_CASE) or d.get(FieldNames.FILTER_CAMEL_CASE) or d.get(FieldNames.FILTER_SNAKE_CASE) or None
        filters_parsed: List[FilterConfig] | None = None
        if raw_filters is not None:
            if isinstance(raw_filters, list):
                filters_parsed = [f for f in (FilterConfig.from_dict(x) for x in raw_filters) if f is not None]
            elif isinstance(raw_filters, dict):
                single = FilterConfig.from_dict(raw_filters)
                filters_parsed = [single] if single is not None else None

        limit_val = d.get(FieldNames.LIMIT_CAMEL_CASE) if d.get(FieldNames.LIMIT_CAMEL_CASE) is not None else d.get(FieldNames.LIMIT_SNAKE_CASE, -1)

        return cls(target_view=view, filters=filters_parsed, limit=limit_val)

class CallerType(str, Enum):
    PREPARE = "Prepare"
    LAUNCH = "Launch"
    FINALIZE = "Finalize"
    PROMOTE = "Promote"

@dataclass
class AnnotationStatus(str, Enum):
    APPROVED = "Approved"
    SUGGESTED = "Suggested"
    REJECTED = "Rejected"

@dataclass
class AnnotationCoverageData:
    coverage_pct: float
    actual_count: int
    potential_count: int
    total_possible: int

@dataclass
class ActualAnnotationStatus(str, Enum):
    REGULARLY_ANNOTATED = "Regularly Annotated"
    AUTOMATICALLY_PROMOTED = "Automatically Promoted"
    MANUALLY_PROMOTED = "Manually Promoted"

@dataclass
class PotentialAnnotationStatus(str, Enum):
    PATTERN_FOUND = "Pattern Found"
    AMBIGUOUS = "Ambiguous"
    NO_MATCH = "No Match"

@dataclass
class FunctionRunConfig:
    caller_type: CallerType
    function_id_field: str
    function_call_id_field: str
    log_title: str
    log_snake_case: str


@dataclass
class AnnotationFrames:
    actual_df: pd.DataFrame
    potential_df: pd.DataFrame

@dataclass
class RawTablesConfig:
    raw_db: str
    raw_table_pattern_tags: str
    raw_table_asset_tags: str
    raw_table_file_tags: str
    raw_table_pattern_cache: str
    raw_manual_patterns_catalog: str


    @classmethod
    def from_dict(cls, d: dict | None):
        if not isinstance(d, dict):
            return cls(
                raw_db=None,
                raw_table_pattern_tags=None,
                raw_table_asset_tags=None,
                raw_table_file_tags=None,
                raw_table_pattern_cache=None,
                raw_manual_patterns_catalog=None
            )
        return cls(
            raw_db=d.get(FieldNames.RAW_DATABASE_CAMEL_CASE),
            raw_table_pattern_tags=d.get(FieldNames.RAW_TABLE_DOC_PATTERN_CAMEL_CASE),
            raw_table_asset_tags=d.get(FieldNames.RAW_TABLE_DOC_TAG_CAMEL_CASE),
            raw_table_file_tags=d.get(FieldNames.RAW_TABLE_DOC_DOC_CAMEL_CASE),
            raw_table_pattern_cache=d.get(FieldNames.RAW_TABLE_CACHE_CAMEL_CASE),
            raw_manual_patterns_catalog=d.get(FieldNames.RAW_TABLE_MANUAL_PATTERNS_CATALOG_CAMEL_CASE),
        )

@dataclass
class ApplyServiceConfig:
    raw_db: str
    raw_table_pattern_tags: str
    raw_table_asset_tags: str
    raw_table_file_tags: str
    raw_manual_patterns_catalog: str
    raw_table_pattern_cache: str

    @classmethod
    def from_dict(cls, d: dict | None):
        if not isinstance(d, dict):
            return cls(
                raw_db=None,
                raw_table_pattern_tags=None,
                raw_table_asset_tags=None,
                raw_table_file_tags=None,
                raw_manual_patterns_catalog=None,
                raw_table_pattern_cache=None
            )
        return cls(
            raw_db=d.get(FieldNames.RAW_DATABASE_CAMEL_CASE),
            raw_table_pattern_tags=d.get(FieldNames.RAW_TABLE_DOC_PATTERN_CAMEL_CASE),
            raw_table_asset_tags=d.get(FieldNames.RAW_TABLE_DOC_TAG_CAMEL_CASE),
            raw_table_file_tags=d.get(FieldNames.RAW_TABLE_DOC_DOC_CAMEL_CASE),
            raw_manual_patterns_catalog=d.get(FieldNames.RAW_TABLE_MANUAL_PATTERNS_CATALOG_CAMEL_CASE),
            raw_table_pattern_cache=d.get(FieldNames.RAW_TABLE_CACHE_CAMEL_CASE),
        )

@dataclass
class CacheServiceConfig:
    raw_db: str
    raw_table_pattern_tags: str
    raw_table_asset_tags: str
    raw_table_file_tags: str
    raw_manual_patterns_catalog: str
    raw_table_pattern_cache: str

    @classmethod
    def from_dict(cls, d: dict | None):
        if not isinstance(d, dict):
            return cls(
                raw_db=None,
                raw_table_pattern_tags=None,
                raw_table_asset_tags=None,
                raw_table_file_tags=None,
                raw_manual_patterns_catalog=None,
                raw_table_pattern_cache=None
            )

        return cls(
            raw_db=d.get(FieldNames.RAW_DATABASE_CAMEL_CASE),
            raw_table_pattern_tags=d.get(FieldNames.RAW_TABLE_DOC_PATTERN_CAMEL_CASE),
            raw_table_asset_tags=d.get(FieldNames.RAW_TABLE_DOC_TAG_CAMEL_CASE),
            raw_table_file_tags=d.get(FieldNames.RAW_TABLE_DOC_DOC_CAMEL_CASE),
            raw_manual_patterns_catalog=d.get(FieldNames.RAW_TABLE_MANUAL_PATTERNS_CATALOG_CAMEL_CASE),
            raw_table_pattern_cache=d.get(FieldNames.RAW_TABLE_CACHE_CAMEL_CASE),
        )

@dataclass
class LaunchFunctionConfig:
    primary_scope_property: str | None = None
    secondary_scope_property: str | None = None
    asset_resource_property: str | None = None
    file_resource_property: str | None = None
    cache_service: CacheServiceConfig | None = None
    target_entities_view_cfg: ViewPropertyConfig | None = None
    file_entities_view_cfg: ViewPropertyConfig | None = None
    target_entities_query_filters: list[dict] | None = None
    file_entities_query_filters: list[dict] | None = None
    target_entities_query: QueryConfig | list[QueryConfig] | None = None
    file_entities_query: QueryConfig | list[QueryConfig] | None = None

    @classmethod
    def from_dict(cls, d: dict | None):
        if not isinstance(d, dict):
            return cls(cache_service=CacheServiceConfig.from_dict(None))

        cache_service = d.get(FieldNames.CACHE_SERVICE_CAMEL_CASE)

        def _view_from_query(query_obj) -> ViewPropertyConfig | None:
            if not isinstance(query_obj, dict):
                return None
            target_view = query_obj.get(FieldNames.TARGET_VIEW_CAMEL_CASE) or query_obj.get(FieldNames.TARGET_VIEW_SNAKE_CASE)
            if not isinstance(target_view, dict):
                return None
            schema_space = target_view.get(FieldNames.SCHEMA_SPACE_CAMEL_CASE)
            external_id = target_view.get(FieldNames.EXTERNAL_ID_CAMEL_CASE)
            version = target_view.get(FieldNames.VERSION_CAMEL_CASE)
            if not schema_space or not external_id or version is None:
                return None
            return ViewPropertyConfig(
                schema_space=schema_space,
                external_id=external_id,
                version=str(version),
                instance_space=target_view.get(FieldNames.INSTANCE_SPACE_CAMEL_CASE),
            )

        data_model_service = d.get(FieldNames.DATA_MODEL_SERVICE_CAMEL_CASE) or d.get(FieldNames.DATA_MODEL_SERVICE_SNAKE_CASE)
        target_entities_view = None
        file_entities_view = None
        target_entities_filters = None
        file_entities_filters = None
        target_entities_query = None
        file_entities_query = None
        if isinstance(data_model_service, dict):
            gt = data_model_service.get(FieldNames.GET_TARGET_ENTITIES_QUERY_CAMEL_CASE) or data_model_service.get(FieldNames.GET_TARGET_ENTITIES_QUERY_SNAKE_CASE)
            gf = data_model_service.get(FieldNames.GET_FILE_ENTITIES_QUERY_CAMEL_CASE) or data_model_service.get(FieldNames.GET_FILE_ENTITIES_QUERY_SNAKE_CASE)

            def _parse_query_obj(obj):
                if obj is None:
                    return None
                if isinstance(obj, list):
                    parsed = [QueryConfig.from_dict(x, view_from_query_fn=_view_from_query) for x in obj]
                    return [p for p in parsed if p is not None]
                if isinstance(obj, dict):
                    qc = QueryConfig.from_dict(obj, view_from_query_fn=_view_from_query)
                    return qc
                return None

            gt_obj = gt if (isinstance(gt, (dict, list)) and gt) else None
            gf_obj = gf if (isinstance(gf, (dict, list)) and gf) else None

            target_entities_query = _parse_query_obj(gt_obj)
            file_entities_query = _parse_query_obj(gf_obj)

            first_gt = gt_obj[0] if isinstance(gt_obj, list) and gt_obj else (gt_obj if isinstance(gt_obj, dict) else None)
            first_gf = gf_obj[0] if isinstance(gf_obj, list) and gf_obj else (gf_obj if isinstance(gf_obj, dict) else None)

            target_entities_view = _view_from_query(first_gt) if first_gt else None
            file_entities_view = _view_from_query(first_gf) if first_gf else None

            if isinstance(first_gt, dict):
                target_entities_filters = first_gt.get("filters") or None
            if isinstance(first_gf, dict):
                file_entities_filters = first_gf.get("filters") or None

        return cls(
            primary_scope_property=d.get(FieldNames.PRIMARY_SCOPE_PROPERTY_CAMEL_CASE) or d.get(FieldNames.PRIMARY_SCOPE_PROPERTY_SNAKE_CASE),
            secondary_scope_property=d.get(FieldNames.SECONDARY_SCOPE_PROPERTY_CAMEL_CASE),
            asset_resource_property=d.get(FieldNames.ASSET_RESOURCE_PROPERTY_CAMEL_CASE),
            file_resource_property=d.get(FieldNames.FILE_RESOURCE_PROPERTY_CAMEL_CASE),
            cache_service=CacheServiceConfig.from_dict(cache_service),
            target_entities_view_cfg=target_entities_view,
            file_entities_view_cfg=file_entities_view,
            target_entities_query_filters=target_entities_filters,
            file_entities_query_filters=file_entities_filters,
            target_entities_query=target_entities_query,
            file_entities_query=file_entities_query,
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
    raw_tables: RawTablesConfig
    file_view_cfg: ViewPropertyConfig | None = None
    asset_view_cfg: ViewPropertyConfig | None = None
    annotation_state_view_cfg: ViewPropertyConfig | None = None

    @classmethod
    def from_dict(cls, d: dict | None):
        if not isinstance(d, dict):
            return cls(
                launch_function=LaunchFunctionConfig.from_dict(None),
                finalize_function=FinalizeFunctionConfig.from_dict(None),
                raw_tables=RawTablesConfig.from_dict(None),
            )

        launch_function = d.get(FieldNames.LAUNCH_FUNCTION_CAMEL_CASE)
        finalize_function = d.get(FieldNames.FINALIZE_FUNCTION_CAMEL_CASE)
        raw_tables = d.get(FieldNames.RAW_TABLES_CAMEL_CASE)

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

        launch_fn_obj = LaunchFunctionConfig.from_dict(launch_function)

        try:
            if launch_fn_obj is not None:
                if getattr(launch_fn_obj, "target_entities_view_cfg", None) is not None:
                    tev = launch_fn_obj.target_entities_view_cfg
                    if tev.instance_space is None and asset_view is not None:
                        tev.instance_space = asset_view.instance_space

                if getattr(launch_fn_obj, "file_entities_view_cfg", None) is not None:
                    fev = launch_fn_obj.file_entities_view_cfg
                    if fev.instance_space is None and file_view is not None:
                        fev.instance_space = file_view.instance_space

                teq = getattr(launch_fn_obj, "target_entities_query", None)
                if teq:
                    if isinstance(teq, list):
                        for q in teq:
                            if q and getattr(q, "target_view", None) is not None and q.target_view.instance_space is None and asset_view is not None:
                                q.target_view.instance_space = asset_view.instance_space
                    else:
                        q = teq
                        if getattr(q, "target_view", None) is not None and q.target_view.instance_space is None and asset_view is not None:
                            q.target_view.instance_space = asset_view.instance_space

                feq = getattr(launch_fn_obj, "file_entities_query", None)
                if feq:
                    if isinstance(feq, list):
                        for q in feq:
                            if q and getattr(q, "target_view", None) is not None and q.target_view.instance_space is None and file_view is not None:
                                q.target_view.instance_space = file_view.instance_space
                    else:
                        q = feq
                        if getattr(q, "target_view", None) is not None and q.target_view.instance_space is None and file_view is not None:
                            q.target_view.instance_space = file_view.instance_space
        except Exception:
            pass

        return cls(
            launch_function=launch_fn_obj,
            finalize_function=FinalizeFunctionConfig.from_dict(finalize_function),
            raw_tables=RawTablesConfig.from_dict(raw_tables),
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
    def raw_table_asset_tags(self) -> str | None:
        cache = getattr(self.launch_function, FieldNames.CACHE_SERVICE_SNAKE_CASE, None)
        apply = getattr(self.finalize_function, FieldNames.APPLY_SERVICE_SNAKE_CASE, None)

        return (
            getattr(self.raw_tables, FieldNames.RAW_TABLE_ASSET_TAGS_SNAKE_CASE, None)
            or getattr(cache, FieldNames.RAW_TABLE_ASSET_TAGS_SNAKE_CASE, None) 
            or getattr(apply, FieldNames.RAW_TABLE_ASSET_TAGS_SNAKE_CASE, None)
        )

    @property
    def raw_table_file_tags(self) -> str | None:
        cache = getattr(self.launch_function, FieldNames.CACHE_SERVICE_SNAKE_CASE, None)
        apply = getattr(self.finalize_function, FieldNames.APPLY_SERVICE_SNAKE_CASE, None)

        return (
            getattr(self.raw_tables, FieldNames.RAW_TABLE_FILE_TAGS_SNAKE_CASE, None)
            or getattr(cache, FieldNames.RAW_TABLE_FILE_TAGS_SNAKE_CASE, None)
            or getattr(apply, FieldNames.RAW_TABLE_FILE_TAGS_SNAKE_CASE, None)
        )
    
    @property
    def raw_manual_patterns_catalog(self) -> str | None:
        cache = getattr(self.launch_function, FieldNames.CACHE_SERVICE_SNAKE_CASE, None)
        apply = getattr(self.finalize_function, FieldNames.APPLY_SERVICE_SNAKE_CASE, None)

        return (
            getattr(self.raw_tables, FieldNames.RAW_TABLE_MANUAL_PATTERNS_CATALOG_SNAKE_CASE, None)
            or getattr(cache, FieldNames.RAW_TABLE_MANUAL_PATTERNS_CATALOG_SNAKE_CASE, None)
            or getattr(apply, FieldNames.RAW_TABLE_MANUAL_PATTERNS_CATALOG_SNAKE_CASE, None)
        )
    
    @property
    def raw_table_pattern_tags(self) -> str | None:
        cache = getattr(self.launch_function, FieldNames.CACHE_SERVICE_SNAKE_CASE, None)
        apply = getattr(self.finalize_function, FieldNames.APPLY_SERVICE_SNAKE_CASE, None)

        return (
            getattr(self.raw_tables, FieldNames.RAW_TABLE_PATTERN_TAGS_SNAKE_CASE, None)
            or getattr(cache, FieldNames.RAW_TABLE_PATTERN_TAGS_SNAKE_CASE, None)
            or getattr(apply, FieldNames.RAW_TABLE_PATTERN_TAGS_SNAKE_CASE, None)
        )

    @property
    def raw_db(self) -> str | None:
        cache = getattr(self.launch_function, FieldNames.CACHE_SERVICE_SNAKE_CASE, None)
        apply = getattr(self.finalize_function, FieldNames.APPLY_SERVICE_SNAKE_CASE, None)

        return (
            getattr(self.raw_tables, FieldNames.RAW_DB_SNAKE_CASE, None)
            or (getattr(cache, FieldNames.RAW_DB_SNAKE_CASE, None) if cache else None)
            or (getattr(apply, FieldNames.RAW_DB_SNAKE_CASE, None) if apply else None)
        )

    @property
    def raw_table_pattern_cache(self) -> str | None:
        cache = getattr(self.launch_function, FieldNames.CACHE_SERVICE_SNAKE_CASE, None)
        apply = getattr(self.finalize_function, FieldNames.APPLY_SERVICE_SNAKE_CASE, None)

        return (
            getattr(self.raw_tables, FieldNames.RAW_TABLE_PATTERN_CACHE_SNAKE_CASE, None)
            or (getattr(cache, FieldNames.RAW_TABLE_PATTERN_CACHE_SNAKE_CASE, None) if cache else None)
            or (getattr(apply, FieldNames.RAW_TABLE_PATTERN_CACHE_SNAKE_CASE, None) if apply else None)
        )


class UIReporter:
    def __init__(self, log_box=None, max_lines: int = 15, show_progress: bool = True):
        self.log_box = log_box or st.empty()
        self.max_lines = max_lines
        self.logs: List[str] = []
        self.show_progress = show_progress
        self._progress_bar = None
        self._status_box = None

    def _render(self) -> None:
        try:
            if not self.log_box:
                return
            text = "\n".join(self.logs[-self.max_lines:])
            try:
                self.log_box.text_area("Logs", value=text, height=200)
            except Exception:
                try:
                    self.log_box.write(text)
                except Exception:
                    pass
        except Exception:
            pass

    def log(self, msg: str) -> None:
        try:
            self.logs.append(str(msg))
            self._render()
        except Exception:
            pass

    def print(self, msg: str) -> None:
        self.log(msg)

    def callback(self, msg: str) -> None:
        self.log(msg)

    def progress(self, idx: int, total: int, name: str | None = None) -> None:
        if not self.show_progress:
            return
        try:
            if self._progress_bar is None:
                self._progress_bar = st.progress(0)
            pct = 0
            try:
                if not total:
                    pct = 0
                else:
                    displayed = max(1, min(int(idx), int(total)))
                    pct = int((float(displayed) / float(total)) * 100)
            except Exception:
                pct = 0
            pct = max(0, min(100, pct))
            try:
                self._progress_bar.progress(pct)
            except Exception:
                pass

            if self._status_box is None:
                self._status_box = st.empty()
            try:
                displayed = max(1, min(int(idx), int(total))) if total else int(idx)
                status = f"{name or ''}: {displayed}/{total if total else displayed} ({pct}%)"
                self._status_box.text(status)
            except Exception:
                pass
        except Exception:
            pass

