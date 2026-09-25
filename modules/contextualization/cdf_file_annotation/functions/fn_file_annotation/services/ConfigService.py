import re
from enum import Enum
from typing import Literal, Self

import yaml
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.contextualization import (
    ConnectionFlags,
    CustomizeFuzziness,
    DiagramDetectConfig,
    DirectionWeights,
)
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.data_classes.filters import Filter
from cognite.client.exceptions import CogniteAPIError
from fa_constants import (
    ANNOTATION_EXTRACT,
    BATCH_SIZE,
    CASE_SENSITIVE,
    CORE_ANNOTATION_EXTERNAL_ID,
    CORE_ANNOTATION_SCHEMA_SPACE,
    CORE_ANNOTATION_VERSION,
    DELETE_REJECTED_EDGES,
    DELETE_SUGGESTED_EDGES,
    DIRECTION_DELTA,
    DIRECTION_WEIGHTS,
    EXCLUDED_PREPARE_TAGS,
    FILE_ANNOTATION_TYPE,
    FUZZINESS_FUZZY_SCORE,
    FUZZINESS_MAX_BOXES,
    FUZZINESS_MIN_CHARS,
    LAUNCH_STATE_LIMIT,
    LAUNCH_STATUSES,
    MAX_ENTITY_SEARCH_LIMIT,
    MAX_RETRY_ATTEMPTS,
    MIN_FUZZY_SCORE,
    MIN_TOKENS,
    NATURAL_READING_ORDER,
    NO_TEXT_INBETWEEN,
    PAGE_RANGE,
    PREPARE_FILE_LIMIT,
    PROCESSING_STATUS,
    PROMOTE_CANDIDATE_LIMIT,
    PROMOTE_FILE_ENTITIES,
    PROMOTE_TARGET_ENTITIES,
    RAW_TABLE_CACHE,
    RAW_TABLE_DOC_DOC,
    RAW_TABLE_DOC_PATTERN,
    RAW_TABLE_DOC_TAG,
    RAW_TABLE_MANUAL_PATTERNS,
    RAW_TABLE_PROMOTE_CACHE,
    READ_EMBEDDED_TEXT,
    REMOVE_LEADING_ZEROS,
    SUBSTITUTIONS,
    SUGGESTED_STATUS,
    TAG_DETECT_IN_DIAGRAMS,
    TAG_PROMOTE_ATTEMPTED,
    TAG_TO_ANNOTATE,
    TARGET_ANNOTATION_TYPE,
)
from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic.alias_generators import to_camel
from utils.DataStructures import AnnotationStatus, FilterOperator


# Configuration Classes
class ViewPropertyConfig(BaseModel, alias_generator=to_camel):
    schema_space: str
    instance_space: str | None = None
    external_id: str
    version: str
    annotation_type: Literal["diagrams.FileLink", "diagrams.AssetLink"] | None = None
    search_property: str = "aliases"
    resource_property: str | None = None

    def as_view_id(self) -> dm.ViewId:
        return dm.ViewId(space=self.schema_space, external_id=self.external_id, version=self.version)

    def as_property_ref(self, property_name: str) -> list[str]:
        return [self.schema_space, f"{self.external_id}/{self.version}", property_name]


class FilterConfig(BaseModel, alias_generator=to_camel):
    values: list[AnnotationStatus | str] | AnnotationStatus | str | None = None
    negate: bool = False
    operator: FilterOperator
    target_property: str

    def as_filter(self, view_properties: ViewPropertyConfig) -> Filter:
        property_reference = view_properties.as_property_ref(self.target_property)

        # Converts enum value into string -> i.e.) in the case of AnnotationStatus
        if isinstance(self.values, list):
            find_values = [v.value if isinstance(v, Enum) else v for v in self.values]
        elif isinstance(self.values, Enum):
            find_values = self.values.value
        else:
            find_values = self.values

        filter: Filter
        if find_values is None:
            if self.operator == FilterOperator.EXISTS:
                filter = dm.filters.Exists(property=property_reference)
            else:
                raise ValueError(f"Operator {self.operator} requires a value")
        elif self.operator == FilterOperator.IN:
            if not isinstance(find_values, list):
                raise ValueError(f"Operator 'IN' requires a list of values for property {self.target_property}")
            filter = dm.filters.In(property=property_reference, values=find_values)
        elif self.operator == FilterOperator.EQUALS:
            filter = dm.filters.Equals(property=property_reference, value=find_values)
        elif self.operator == FilterOperator.CONTAINSALL:
            filter = dm.filters.ContainsAll(property=property_reference, values=find_values)
        elif self.operator == FilterOperator.SEARCH:
            filter = dm.filters.Search(property=property_reference, value=find_values)
        else:
            raise NotImplementedError(f"Operator {self.operator} is not implemented.")

        if self.negate:
            return dm.filters.Not(filter)
        else:
            return filter


class QueryConfig(BaseModel, alias_generator=to_camel):
    target_view: ViewPropertyConfig
    filters: list[FilterConfig]
    limit: int | None = -1

    def build_filter(self) -> Filter:
        list_filters: list[Filter] = [f.as_filter(self.target_view) for f in self.filters]

        if len(list_filters) == 1:
            return list_filters[0]
        else:
            return dm.filters.And(*list_filters)  # NOTE: '*' Unpacks each filter in the list


class ConnectionFlagsConfig(BaseModel, alias_generator=to_camel):
    no_text_inbetween: bool | None = None
    natural_reading_order: bool | None = None

    def as_connection_flag(self) -> ConnectionFlags:
        params = {key: value for key, value in self.model_dump().items() if value is not None}
        return ConnectionFlags(**params)


class CustomizeFuzzinessConfig(BaseModel, alias_generator=to_camel):
    fuzzy_score: float | None = None
    max_boxes: int | None = None
    min_chars: int | None = None

    def as_customize_fuzziness(self) -> CustomizeFuzziness:
        params = {key: value for key, value in self.model_dump().items() if value is not None}
        return CustomizeFuzziness(**params)


class DirectionWeightsConfig(BaseModel, alias_generator=to_camel):
    left: float | None = None
    right: float | None = None
    up: float | None = None
    down: float | None = None

    def as_direction_weights(self) -> DirectionWeights:
        params = {key: value for key, value in self.model_dump().items() if value is not None}
        return DirectionWeights(**params)


class DiagramDetectConfigModel(BaseModel, alias_generator=to_camel):
    # NOTE: configs come from V7 of the cognite python sdk cognite SDK
    annotation_extract: bool | None = None
    case_sensitive: bool | None = None
    connection_flags: ConnectionFlagsConfig | None = None
    customize_fuzziness: CustomizeFuzzinessConfig | None = None
    direction_delta: float | None = None
    direction_weights: DirectionWeightsConfig | None = None
    min_fuzzy_score: float | None = None
    read_embedded_text: bool | None = None
    remove_leading_zeros: bool | None = None
    substitutions: dict[str, list[str]] | None = None

    def as_config(self) -> DiagramDetectConfig:
        params = {}
        if self.annotation_extract is not None:
            params["annotation_extract"] = self.annotation_extract
        if self.case_sensitive is not None:
            params["case_sensitive"] = self.case_sensitive
        if self.connection_flags is not None:
            params["connection_flags"] = self.connection_flags.as_connection_flag()
        if self.customize_fuzziness is not None:
            params["customize_fuzziness"] = self.customize_fuzziness.as_customize_fuzziness()
        if self.direction_delta is not None:
            params["direction_delta"] = self.direction_delta
        if self.direction_weights is not None:
            params["direction_weights"] = self.direction_weights.as_direction_weights()
        if self.min_fuzzy_score is not None:
            params["min_fuzzy_score"] = self.min_fuzzy_score
        if self.read_embedded_text is not None:
            params["read_embedded_text"] = self.read_embedded_text
        if self.remove_leading_zeros is not None:
            params["remove_leading_zeros"] = self.remove_leading_zeros
        if self.substitutions is not None:
            params["substitutions"] = self.substitutions

        return DiagramDetectConfig(**params)


# Launch Related Configs
class DataModelServiceConfig(BaseModel, alias_generator=to_camel):
    get_files_to_process_query: QueryConfig | list[QueryConfig]
    get_target_entities_query: QueryConfig | list[QueryConfig]
    get_file_entities_query: QueryConfig | list[QueryConfig]


class RawTablesConfig(BaseModel, alias_generator=to_camel):
    """
    Consolidated configuration for RAW database and tables used across all functions.
    This section centralizes all RAW storage configuration to avoid duplication
    and ensure consistency across prepare, launch, finalize, and promote functions.
    """

    raw_db: str
    raw_table_cache: str
    raw_table_doc_tag: str
    raw_table_doc_doc: str
    raw_table_doc_pattern: str
    raw_table_promote_cache: str
    raw_manual_patterns_catalog: str


class AnnotationServiceConfig(BaseModel, alias_generator=to_camel):
    page_range: int = Field(gt=0, le=50)
    partial_match: bool = True
    min_tokens: int = 1
    diagram_detect_config: DiagramDetectConfigModel | None = None


class PrepareFunction(BaseModel, alias_generator=to_camel):
    get_files_for_annotation_reset_query: QueryConfig | list[QueryConfig] | None = None
    get_files_to_annotate_query: QueryConfig | list[QueryConfig]


class LaunchFunction(BaseModel, alias_generator=to_camel):
    batch_size: int = Field(gt=0, le=50)
    primary_scope_property: str | None = None
    secondary_scope_property: str | None = None
    file_search_property: str = "aliases"
    target_entities_search_property: str = "aliases"
    pattern_mode: bool
    structural_auto_patterns: bool = False
    file_resource_property: str | None = None
    target_entities_resource_property: str | None = None
    data_model_service: DataModelServiceConfig
    annotation_service: AnnotationServiceConfig


# Finalize Related Configs
class RetrieveServiceConfig(BaseModel, alias_generator=to_camel):
    get_job_id_query: QueryConfig | list[QueryConfig]


class ApplyServiceConfig(BaseModel, alias_generator=to_camel):
    # Asset links (diagrams.AssetLink) use these; file links use the file_* pair below.
    asset_auto_approval_threshold: float = Field(gt=0.0, le=1.0)
    asset_auto_suggest_threshold: float = Field(gt=0.0, le=1.0)
    file_auto_approval_threshold: float = Field(gt=0.0, le=1.0)
    file_auto_suggest_threshold: float = Field(gt=0.0, le=1.0)
    sink_node: NodeId


class FinalizeFunction(BaseModel, alias_generator=to_camel):
    clean_old_annotations: bool
    max_retry_attempts: int
    retrieve_service: RetrieveServiceConfig
    apply_service: ApplyServiceConfig


# Promote Related Configs
def _coerce_pattern_list(value: object) -> object:
    if value is None:
        return []
    return [value] if isinstance(value, str) else value


def _validate_capture_group_patterns(value: list[str], *, field_name: str) -> list[str]:
    for pattern in value:
        try:
            compiled = re.compile(pattern)
        except re.error as e:
            raise ValueError(f"{field_name} entry {pattern!r} is not a valid regular expression: {e}") from e
        if not compiled.groups:
            raise ValueError(
                f"{field_name} entry {pattern!r} must have at least one capture group - "
                "the normalized form is the groups joined by '_'"
            )
    return value


class TextNormalizationConfig(BaseModel, alias_generator=to_camel):
    """
    Configuration for text normalization during promote and auto pattern generation.

    Separate pattern lists avoid false positives when asset and file aliases differ:

    - entityNormalizationPatterns: assets / targetEntitiesView (diagrams.AssetLink)
    - fileNormalizationPatterns: files / fileView (diagrams.FileLink)

    Same capture-group semantics as cdf_entity_matching aliases_update:
    each match yields capture groups joined by "_"; longest match wins.
    Empty list disables filtering for that source.
    """

    entity_normalization_patterns: list[str] = Field(
        alias="entityNormalizationPatterns",
        default_factory=list,
    )
    file_normalization_patterns: list[str] = Field(
        alias="fileNormalizationPatterns",
        default_factory=list,
    )

    @field_validator("entity_normalization_patterns", "file_normalization_patterns", mode="before")
    @classmethod
    def wrap_single_pattern(cls, value: object) -> object:
        return _coerce_pattern_list(value)

    @field_validator("entity_normalization_patterns")
    @classmethod
    def validate_entity_patterns(cls, value: list[str]) -> list[str]:
        return _validate_capture_group_patterns(value, field_name="entityNormalizationPatterns")

    @field_validator("file_normalization_patterns")
    @classmethod
    def validate_file_patterns(cls, value: list[str]) -> list[str]:
        return _validate_capture_group_patterns(value, field_name="fileNormalizationPatterns")

    def patterns_for_annotation_type(self, annotation_type: str) -> list[str]:
        """Return entity or file patterns based on annotation type."""
        if annotation_type == "diagrams.FileLink":
            return self.file_normalization_patterns
        return self.entity_normalization_patterns


class EntitySearchServiceConfig(BaseModel, alias_generator=to_camel):
    """
    Configuration for the EntitySearchService in the promote function.

    Controls entity search and text normalization behavior:
    - Queries entities directly (server-side IN filter on entity/file aliases)
    - Text normalization for generating search variations

    Uses efficient server-side filtering on the smaller entity dataset rather than
    the larger annotation edge dataset for better performance at scale.
    """

    max_entity_search_limit: int = Field(default=1000, gt=0, le=10000)
    text_normalization: TextNormalizationConfig


class PromoteFunctionConfig(BaseModel, alias_generator=to_camel):
    """
    Configuration for the promote function.

    The promote function resolves pattern-mode annotations by finding matching entities
    and updating annotation edges from pointing to a sink node to pointing to actual entities.

    Configuration is organized by service interface:
    - entitySearchService: Controls entity search strategies

    Batch size is controlled via getCandidatesQuery.limit field.
    """

    get_candidates_query: QueryConfig | list[QueryConfig]
    delete_rejected_edges: bool
    delete_suggested_edges: bool
    promote_file_entities: bool = True
    promote_target_entities: bool = True
    entity_search_service: EntitySearchServiceConfig


class DataModelViews(BaseModel, alias_generator=to_camel):
    core_annotation_view: ViewPropertyConfig
    annotation_state_view: ViewPropertyConfig
    file_view: ViewPropertyConfig
    target_entities_view: ViewPropertyConfig


class PatternPromoteParameters(BaseModel, alias_generator=to_camel):
    text_normalization: TextNormalizationConfig = Field(default_factory=TextNormalizationConfig)


class Parameters(BaseModel, alias_generator=to_camel):
    pattern_mode: bool = True
    structural_auto_patterns: bool = True
    clean_old_annotations: bool = True
    asset_auto_approval_threshold: float = Field(default=1.0, gt=0.0, le=1.0)
    asset_auto_suggest_threshold: float = Field(default=1.0, gt=0.0, le=1.0)
    # Thresholds for diagrams.FileLink; unset falls back to the two above.
    file_auto_approval_threshold: float | None = Field(default=None, gt=0.0, le=1.0)
    file_auto_suggest_threshold: float | None = Field(default=None, gt=0.0, le=1.0)
    primary_scope_property: str | None = None
    secondary_scope_property: str | None = None
    raw_db: str
    raw_table_cache: str = RAW_TABLE_CACHE
    raw_table_doc_tag: str = RAW_TABLE_DOC_TAG
    raw_table_doc_doc: str = RAW_TABLE_DOC_DOC
    raw_table_doc_pattern: str = RAW_TABLE_DOC_PATTERN
    raw_table_promote_cache: str = RAW_TABLE_PROMOTE_CACHE
    raw_manual_patterns_catalog: str = RAW_TABLE_MANUAL_PATTERNS
    pattern_promote: PatternPromoteParameters = Field(default_factory=PatternPromoteParameters)
    files_to_annotate_tags: list[str] = Field(default_factory=lambda: [TAG_TO_ANNOTATE])
    files_to_annotate_exclude_tags: list[str] = Field(default_factory=lambda: list(EXCLUDED_PREPARE_TAGS))
    file_entities_tags: list[str] = Field(default_factory=lambda: [TAG_DETECT_IN_DIAGRAMS])
    target_entities_tags: list[str] = Field(default_factory=lambda: [TAG_DETECT_IN_DIAGRAMS])
    debug_file_external_id: str | None = None

    @field_validator("debug_file_external_id", mode="before")
    @classmethod
    def blank_debug_file_to_none(cls, value: object) -> object:
        if isinstance(value, str):
            return value.strip() or None
        return value


def _tag_list(value: object, default: list[str], *, allow_empty: bool = False) -> list[str]:
    """Coerce a config tag value to a list of non-empty strings."""
    if value is None:
        return list(default)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped:
            return [stripped]
        return [] if allow_empty else list(default)
    if isinstance(value, list):
        tags = [str(item).strip() for item in value if str(item).strip()]
        if tags or allow_empty:
            return tags
        return list(default)
    return list(default)


class ConfigData(BaseModel, alias_generator=to_camel):
    file_view: ViewPropertyConfig
    target_entities_view: ViewPropertyConfig
    annotation_state_view: ViewPropertyConfig
    sink_node: NodeId


class Config(BaseModel, alias_generator=to_camel):
    parameters: Parameters
    data: ConfigData
    raw_tables: RawTablesConfig
    data_model_views: DataModelViews
    prepare_function: PrepareFunction
    launch_function: LaunchFunction
    finalize_function: FinalizeFunction
    promote_function: PromoteFunctionConfig

    @model_validator(mode="before")
    @classmethod
    def build_internal_stage_config(cls, value: object) -> object:
        """Build fixed stage queries from the concise public parameters/data config."""
        if not isinstance(value, dict) or "parameters" not in value or "data" not in value:
            return value

        config = dict(value)
        parameters = config.get("parameters")
        data = config.get("data")
        if not isinstance(parameters, dict) or not isinstance(data, dict):
            return value

        file_view = data.get("fileView")
        target_view = data.get("targetEntitiesView")
        state_view = data.get("annotationStateView")
        sink_node = data.get("sinkNode")
        if (
            not isinstance(file_view, dict)
            or not isinstance(target_view, dict)
            or not isinstance(state_view, dict)
            or not isinstance(sink_node, dict)
        ):
            return value

        parameters = dict(parameters)
        file_view = dict(file_view)
        target_view = dict(target_view)
        state_view = dict(state_view)
        sink_node = dict(sink_node)
        file_view["annotationType"] = FILE_ANNOTATION_TYPE
        target_view["annotationType"] = TARGET_ANNOTATION_TYPE
        core_view = {
            "schemaSpace": CORE_ANNOTATION_SCHEMA_SPACE,
            "externalId": CORE_ANNOTATION_EXTERNAL_ID,
            "version": CORE_ANNOTATION_VERSION,
        }
        raw_db = parameters.get("rawDb")
        if not raw_db:
            return value
        asset_approval_threshold = parameters.get("assetAutoApprovalThreshold", 1.0)
        asset_suggest_threshold = parameters.get("assetAutoSuggestThreshold", 1.0)
        pattern_promote = parameters.get("patternPromote")
        if not isinstance(pattern_promote, dict):
            pattern_promote = {}
        text_normalization = pattern_promote.get("textNormalization")
        if not isinstance(text_normalization, dict):
            text_normalization = {}

        files_to_annotate_tags = _tag_list(parameters.get("filesToAnnotateTags"), [TAG_TO_ANNOTATE])
        files_to_annotate_exclude_tags = [
            tag
            for tag in _tag_list(
                parameters.get("filesToAnnotateExcludeTags"),
                list(EXCLUDED_PREPARE_TAGS),
                allow_empty=True,
            )
            if tag not in files_to_annotate_tags
        ]
        file_entities_tags = _tag_list(parameters.get("fileEntitiesTags"), [TAG_DETECT_IN_DIAGRAMS])
        target_entities_tags = _tag_list(parameters.get("targetEntitiesTags"), [TAG_DETECT_IN_DIAGRAMS])
        prepare_filters: list[dict[str, object]] = [
            {"values": files_to_annotate_tags, "operator": "In", "targetProperty": "tags"}
        ]
        if files_to_annotate_exclude_tags:
            prepare_filters.append(
                {
                    "values": files_to_annotate_exclude_tags,
                    "negate": True,
                    "operator": "In",
                    "targetProperty": "tags",
                }
            )

        config.update(
            {
                "rawTables": {
                    "rawDb": raw_db,
                    "rawTableCache": parameters.get("rawTableCache", RAW_TABLE_CACHE),
                    "rawTableDocTag": parameters.get("rawTableDocTag", RAW_TABLE_DOC_TAG),
                    "rawTableDocDoc": parameters.get("rawTableDocDoc", RAW_TABLE_DOC_DOC),
                    "rawTableDocPattern": parameters.get("rawTableDocPattern", RAW_TABLE_DOC_PATTERN),
                    "rawTablePromoteCache": parameters.get("rawTablePromoteCache", RAW_TABLE_PROMOTE_CACHE),
                    "rawManualPatternsCatalog": parameters.get("rawManualPatternsCatalog", RAW_TABLE_MANUAL_PATTERNS),
                },
                "dataModelViews": {
                    "coreAnnotationView": core_view,
                    "annotationStateView": state_view,
                    "fileView": file_view,
                    "targetEntitiesView": target_view,
                },
                "prepareFunction": {
                    "getFilesToAnnotateQuery": {
                        "targetView": file_view,
                        "filters": prepare_filters,
                        "limit": PREPARE_FILE_LIMIT,
                    }
                },
                "launchFunction": {
                    "batchSize": BATCH_SIZE,
                    "fileSearchProperty": file_view.get("searchProperty", "aliases"),
                    "targetEntitiesSearchProperty": target_view.get("searchProperty", "aliases"),
                    "primaryScopeProperty": parameters.get("primaryScopeProperty"),
                    "secondaryScopeProperty": parameters.get("secondaryScopeProperty"),
                    "patternMode": parameters.get("patternMode", True),
                    "structuralAutoPatterns": parameters.get("structuralAutoPatterns", True),
                    "fileResourceProperty": file_view.get("resourceProperty"),
                    "targetEntitiesResourceProperty": target_view.get("resourceProperty"),
                    "dataModelService": {
                        "getFilesToProcessQuery": {
                            "targetView": state_view,
                            "filters": [
                                {
                                    "values": LAUNCH_STATUSES,
                                    "operator": "In",
                                    "targetProperty": "annotationStatus",
                                },
                                {"operator": "Exists", "targetProperty": "linkedFile"},
                            ],
                            "limit": LAUNCH_STATE_LIMIT,
                        },
                        "getTargetEntitiesQuery": {
                            "targetView": target_view,
                            "filters": [
                                {
                                    "values": target_entities_tags,
                                    "operator": "In",
                                    "targetProperty": "tags",
                                }
                            ],
                        },
                        "getFileEntitiesQuery": {
                            "targetView": file_view,
                            "filters": [
                                {
                                    "values": file_entities_tags,
                                    "operator": "In",
                                    "targetProperty": "tags",
                                }
                            ],
                        },
                    },
                    "annotationService": {
                        "pageRange": PAGE_RANGE,
                        "partialMatch": True,
                        "minTokens": MIN_TOKENS,
                        "diagramDetectConfig": {
                            "annotationExtract": ANNOTATION_EXTRACT,
                            "caseSensitive": CASE_SENSITIVE,
                            "connectionFlags": {
                                "noTextInbetween": NO_TEXT_INBETWEEN,
                                "naturalReadingOrder": NATURAL_READING_ORDER,
                            },
                            "customizeFuzziness": {
                                "fuzzyScore": FUZZINESS_FUZZY_SCORE,
                                "maxBoxes": FUZZINESS_MAX_BOXES,
                                "minChars": FUZZINESS_MIN_CHARS,
                            },
                            "directionDelta": DIRECTION_DELTA,
                            "directionWeights": DIRECTION_WEIGHTS,
                            "minFuzzyScore": MIN_FUZZY_SCORE,
                            "readEmbeddedText": READ_EMBEDDED_TEXT,
                            "removeLeadingZeros": REMOVE_LEADING_ZEROS,
                            "substitutions": SUBSTITUTIONS,
                        },
                    },
                },
                "finalizeFunction": {
                    "cleanOldAnnotations": parameters.get("cleanOldAnnotations", True),
                    "maxRetryAttempts": MAX_RETRY_ATTEMPTS,
                    "retrieveService": {
                        "getJobIdQuery": {
                            "targetView": state_view,
                            "filters": [
                                {
                                    "values": PROCESSING_STATUS,
                                    "operator": "Equals",
                                    "targetProperty": "annotationStatus",
                                },
                                {"operator": "Exists", "targetProperty": "diagramDetectJobId"},
                            ],
                        }
                    },
                    "applyService": {
                        "assetAutoApprovalThreshold": asset_approval_threshold,
                        "assetAutoSuggestThreshold": asset_suggest_threshold,
                        "fileAutoApprovalThreshold": parameters.get("fileAutoApprovalThreshold")
                        or asset_approval_threshold,
                        "fileAutoSuggestThreshold": parameters.get("fileAutoSuggestThreshold")
                        or asset_suggest_threshold,
                        "sinkNode": sink_node,
                    },
                },
                "promoteFunction": {
                    "getCandidatesQuery": {
                        "targetView": core_view,
                        "filters": [
                            {"values": SUGGESTED_STATUS, "operator": "Equals", "targetProperty": "status"},
                            {
                                "values": [TAG_PROMOTE_ATTEMPTED],
                                "negate": True,
                                "operator": "In",
                                "targetProperty": "tags",
                            },
                        ],
                        "limit": PROMOTE_CANDIDATE_LIMIT,
                    },
                    "deleteRejectedEdges": DELETE_REJECTED_EDGES,
                    "deleteSuggestedEdges": DELETE_SUGGESTED_EDGES,
                    "promoteFileEntities": PROMOTE_FILE_ENTITIES,
                    "promoteTargetEntities": PROMOTE_TARGET_ENTITIES,
                    "entitySearchService": {
                        "maxEntitySearchLimit": MAX_ENTITY_SEARCH_LIMIT,
                        "textNormalization": text_normalization,
                    },
                },
            }
        )
        config["data"] = data
        return config

    @model_validator(mode="after")
    def validate_debug_file(self) -> Self:
        if self.parameters.debug_file_external_id and not self.data_model_views.file_view.instance_space:
            raise ValueError("debugFileExternalId requires data.fileView.instanceSpace to be set")
        return self

    @property
    def debug_file(self) -> NodeId | None:
        """The single file to process when debugFileExternalId is set, otherwise None."""
        external_id = self.parameters.debug_file_external_id
        space = self.data_model_views.file_view.instance_space
        if not external_id or not space:
            return None
        return NodeId(space, external_id)


# Functions to construct queries
def get_limit_from_query(query: QueryConfig | list[QueryConfig]) -> int:
    """
    Determines the retrieval limit from a query configuration.
    Handles 'None' by treating it as the default -1 (unlimited).
    """
    default_limit = -1
    if isinstance(query, list):
        if not query:
            return default_limit
        limits = [q.limit if q.limit is not None else default_limit for q in query]
        return max(limits)
    else:
        return query.limit if query.limit is not None else default_limit


def build_filter_from_query(query: QueryConfig | list[QueryConfig]) -> Filter:
    """
    Builds a Cognite Filter from a query configuration.

    If the query is a list, it builds a filter for each item and combines them with a logical OR.
    If the query is a single object, it builds the filter directly from it.
    """
    if isinstance(query, list):
        list_filters: list[Filter] = [q.build_filter() for q in query]
        if not list_filters:
            raise ValueError("Query list cannot be empty.")
        return dm.filters.Or(*list_filters) if len(list_filters) > 1 else list_filters[0]
    else:
        return query.build_filter()


# Helper functions for config logging
def _format_config_header(function_name: str, pipeline_ext_id: str, debug_file: NodeId | None) -> list[str]:
    """Build the header lines naming the stage and the pipeline the config was read from."""
    separator = "=" * 80
    lines = [
        separator,
        f"FUNCTION: {function_name}",
        f"CONFIG SOURCE: extraction pipeline '{pipeline_ext_id}'",
    ]
    if debug_file:
        lines.append(f"DEBUG MODE: only processing file {debug_file.space}/{debug_file.external_id}")
    lines.append(separator)
    return lines


def _format_query_summary(query: QueryConfig | list[QueryConfig], query_name: str) -> str:
    """Format a query configuration into a readable summary string."""
    lines = [f"  {query_name}:"]

    queries = query if isinstance(query, list) else [query]

    for i, q in enumerate(queries):
        if len(queries) > 1:
            lines.append(f"    Query {i + 1}:")
            indent = "      "
        else:
            indent = "    "

        # View information
        view = q.target_view
        view_str = f"{view.schema_space}/{view.external_id}/{view.version}"
        lines.append(f"{indent}- Target view: {view_str}")

        # Filter information
        filter_parts = []
        for f in q.filters:
            if f.operator == FilterOperator.EXISTS:
                filter_str = f"{f.target_property} EXISTS"
            elif f.operator == FilterOperator.IN:
                values_str = str(f.values) if isinstance(f.values, list) else f"[{f.values}]"
                filter_str = f"{f.target_property} IN {values_str}"
            elif f.operator == FilterOperator.EQUALS:
                filter_str = f"{f.target_property} = {f.values}"
            else:
                filter_str = f"{f.target_property} {f.operator.value} {f.values}"

            if f.negate:
                filter_str = f"NOT ({filter_str})"
            filter_parts.append(filter_str)

        filter_combined = " AND ".join(filter_parts)
        lines.append(f"{indent}- Filter: {filter_combined}")

        # Limit information
        if q.limit is not None and q.limit != -1:
            lines.append(f"{indent}- Limit: {q.limit}")

    return "\n".join(lines)


def _format_diagram_detect_config(config: DiagramDetectConfigModel | None) -> str:
    """Format diagram detect configuration into a readable string."""
    if config is None:
        return "    - Diagram detect config: None"

    lines = ["    - Diagram detect config:"]

    if config.annotation_extract is not None:
        lines.append(f"      • Annotation extract: {config.annotation_extract}")
    if config.case_sensitive is not None:
        lines.append(f"      • Case sensitive: {config.case_sensitive}")
    if config.connection_flags is not None:
        flags = config.connection_flags
        flag_parts = []
        if flags.no_text_inbetween is not None:
            flag_parts.append(f"noTextInbetween={flags.no_text_inbetween}")
        if flags.natural_reading_order is not None:
            flag_parts.append(f"naturalReadingOrder={flags.natural_reading_order}")
        if flag_parts:
            lines.append(f"      • Connection flags: {', '.join(flag_parts)}")
    if config.customize_fuzziness is not None:
        fuzz = config.customize_fuzziness
        fuzz_parts = []
        if fuzz.fuzzy_score is not None:
            fuzz_parts.append(f"score={fuzz.fuzzy_score}")
        if fuzz.max_boxes is not None:
            fuzz_parts.append(f"maxBoxes={fuzz.max_boxes}")
        if fuzz.min_chars is not None:
            fuzz_parts.append(f"minChars={fuzz.min_chars}")
        if fuzz_parts:
            lines.append(f"      • Customize fuzziness: {', '.join(fuzz_parts)}")
    if config.direction_delta is not None:
        lines.append(f"      • Direction delta: {config.direction_delta}")
    if config.direction_weights is not None:
        lines.append(f"      • Direction weights: {config.direction_weights}")
    if config.min_fuzzy_score is not None:
        lines.append(f"      • Min fuzzy score: {config.min_fuzzy_score}")
    if config.read_embedded_text is not None:
        lines.append(f"      • Read embedded text: {config.read_embedded_text}")
    if config.remove_leading_zeros is not None:
        lines.append(f"      • Remove leading zeros: {config.remove_leading_zeros}")
    if config.substitutions is not None:
        lines.append(f"      • Substitutions: {len(config.substitutions)} patterns")

    return "\n".join(lines)


def format_prepare_config(config: Config, pipeline_ext_id: str) -> str:
    """
    Format the prepare function configuration for logging.

    Args:
        config: The configuration object
        pipeline_ext_id: The extraction pipeline external ID

    Returns:
        Formatted configuration string ready for logging
    """
    lines = [*_format_config_header("Prepare", pipeline_ext_id, config.debug_file), "PREPARE SERVICE CONFIG"]

    # Files to Annotate Query
    lines.append(_format_query_summary(config.prepare_function.get_files_to_annotate_query, "Files to Annotate Query"))

    # Files for Annotation Reset Query (if configured)
    if config.prepare_function.get_files_for_annotation_reset_query is not None:
        lines.append(
            _format_query_summary(
                config.prepare_function.get_files_for_annotation_reset_query, "Files for Annotation Reset Query"
            )
        )

    lines.append("=" * 80)
    return "\n".join(lines)


def format_launch_config(config: Config, pipeline_ext_id: str) -> str:
    """
    Format the launch function configuration for logging.

    Args:
        config: The configuration object
        pipeline_ext_id: The extraction pipeline external ID

    Returns:
        Formatted configuration string ready for logging
    """
    launch = config.launch_function

    lines = [
        *_format_config_header("Launch", pipeline_ext_id, config.debug_file),
        "LAUNCH SERVICE CONFIG",
        f"  • Batch size: {launch.batch_size}",
        f"  • Pattern mode: {launch.pattern_mode}",
        f"  • Structural auto patterns: {launch.structural_auto_patterns}",
        f"  • Primary scope property: {launch.primary_scope_property}",
        f"  • Secondary scope property: {launch.secondary_scope_property}",
        f"  • File search property: {launch.file_search_property}",
        f"  • Target entities search property: {launch.target_entities_search_property}",
        "DATA MODEL SERVICE",
    ]

    # Add queries
    lines.append(_format_query_summary(launch.data_model_service.get_files_to_process_query, "Files to Process Query"))
    lines.append(_format_query_summary(launch.data_model_service.get_target_entities_query, "Target Entities Query"))
    lines.append(_format_query_summary(launch.data_model_service.get_file_entities_query, "File Entities Query"))

    raw = config.raw_tables
    lines.extend(
        [
            "ENTITY CACHE",
            f"  • RAW DB: {raw.raw_db}",
            f"  • Sync state table: {raw.raw_table_cache}",
            f"  • Manual patterns catalog: {raw.raw_manual_patterns_catalog}",
        ]
    )

    # Annotation service
    annot = launch.annotation_service
    lines.extend(
        [
            "ANNOTATION SERVICE",
            f"  • Page range: {annot.page_range} pages",
            f"  • Partial match: {annot.partial_match}",
            f"  • Min tokens: {annot.min_tokens}",
        ]
    )

    lines.append(_format_diagram_detect_config(annot.diagram_detect_config))

    lines.append("=" * 80)
    return "\n".join(lines)


def format_finalize_config(config: Config, pipeline_ext_id: str) -> str:
    """
    Format the finalize function configuration for logging.

    Args:
        config: The configuration object
        pipeline_ext_id: The extraction pipeline external ID

    Returns:
        Formatted configuration string ready for logging
    """
    finalize = config.finalize_function

    lines = [
        *_format_config_header("Finalize", pipeline_ext_id, config.debug_file),
        "FINALIZE SERVICE CONFIG",
        f"  • Clean old annotations: {finalize.clean_old_annotations}",
        f"  • Max retry attempts: {finalize.max_retry_attempts}",
        "RETRIEVE SERVICE",
    ]

    lines.append(_format_query_summary(finalize.retrieve_service.get_job_id_query, "Job ID Query"))

    # Apply service
    apply = finalize.apply_service
    raw = config.raw_tables
    lines.extend(
        [
            "APPLY SERVICE",
            f"  • Asset link approval / suggest threshold: "
            f"{apply.asset_auto_approval_threshold} / {apply.asset_auto_suggest_threshold}",
            f"  • File link approval / suggest threshold: "
            f"{apply.file_auto_approval_threshold} / {apply.file_auto_suggest_threshold}",
            f"  • Sink node: {apply.sink_node.space}/{apply.sink_node.external_id}",
            f"  • RAW DB: {raw.raw_db}",
            f"  • Doc-Tag table: {raw.raw_table_doc_tag}",
            f"  • Doc-Doc table: {raw.raw_table_doc_doc}",
            f"  • Doc-Pattern table: {raw.raw_table_doc_pattern}",
        ]
    )

    lines.append("=" * 80)
    return "\n".join(lines)


def format_promote_config(config: Config, pipeline_ext_id: str) -> str:
    """
    Format the promote function configuration for logging.

    Args:
        config: The configuration object
        pipeline_ext_id: The extraction pipeline external ID

    Returns:
        Formatted configuration string ready for logging
    """
    promote = config.promote_function
    raw = config.raw_tables
    lines = [
        *_format_config_header("Promote", pipeline_ext_id, config.debug_file),
        "PROMOTE SERVICE CONFIG",
        f"  • Delete rejected edges: {promote.delete_rejected_edges}",
        f"  • Delete suggested edges: {promote.delete_suggested_edges}",
        f"  • Promote file entities: {promote.promote_file_entities}",
        f"  • Promote target entities: {promote.promote_target_entities}",
        f"  • RAW DB: {raw.raw_db}",
        f"  • Doc-Tag table: {raw.raw_table_doc_tag}",
        f"  • Doc-Doc table: {raw.raw_table_doc_doc}",
        f"  • Doc-Pattern table: {raw.raw_table_doc_pattern}",
        f"  • Promote cache table: {raw.raw_table_promote_cache}",
    ]

    lines.append(_format_query_summary(promote.get_candidates_query, "Candidates Query"))

    # Entity search service
    entity_search = promote.entity_search_service
    text_norm = entity_search.text_normalization
    lines.extend(
        [
            "ENTITY SEARCH SERVICE",
            f"  • Max entity search limit: {entity_search.max_entity_search_limit}",
            "  • Text normalization:",
            f"    - Entity normalize patterns: {text_norm.entity_normalization_patterns}",
            f"    - File normalize patterns: {text_norm.file_normalization_patterns}",
            "    - Selection: longest matching form (always)",
            "    - Empty list disables filtering for that source",
            "    - Non-matching text is not searched when patterns are set",
            "    - Built-in: remove non-alphanumeric characters and strip leading zeros",
            "    - Casing preserved (DMS alias match is case-sensitive)",
        ]
    )

    lines.append("=" * 80)
    return "\n".join(lines)


def load_config_parameters(
    client: CogniteClient,
    function_data: dict[str, object],
) -> Config:
    """
    Retrieves the configuration parameters from the function data and loads the configuration from CDF.
    """
    if "ExtractionPipelineExtId" not in function_data:
        raise ValueError("Missing key 'ExtractionPipelineExtId' in input data to the function")

    pipeline_ext_id = function_data["ExtractionPipelineExtId"]
    try:
        raw_config = client.extraction_pipelines.config.retrieve(external_id=pipeline_ext_id)
        if raw_config.config is None:
            raise ValueError(f"No config found for extraction pipeline: {pipeline_ext_id!r}")
    except CogniteAPIError as e:
        raise RuntimeError(f"Not able to retrieve pipeline config for extraction pipeline: {pipeline_ext_id!r}") from e

    loaded_yaml_data = yaml.safe_load(raw_config.config)

    if isinstance(loaded_yaml_data, dict):
        return Config.model_validate(loaded_yaml_data)
    else:
        raise ValueError(
            "Invalid configuration structure from CDF: \nExpected a YAML dictionary with a top-level 'config' key."
        )
