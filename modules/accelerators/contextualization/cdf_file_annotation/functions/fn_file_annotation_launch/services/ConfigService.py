from enum import Enum
from typing import Any, Literal, Optional

import yaml
from cognite.client.data_classes.contextualization import (
    DiagramDetectConfig,
    ConnectionFlags,
    CustomizeFuzziness,
    DirectionWeights,
)
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.data_classes.filters import Filter
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.exceptions import CogniteAPIError
from pydantic import BaseModel, Field
from pydantic.alias_generators import to_camel
from utils.DataStructures import AnnotationStatus, FilterOperator


# Configuration Classes
class ViewPropertyConfig(BaseModel, alias_generator=to_camel):
    schema_space: str
    instance_space: Optional[str] = None
    external_id: str
    version: str
    annotation_type: Optional[Literal["diagrams.FileLink", "diagrams.AssetLink"]] = None

    def as_view_id(self) -> dm.ViewId:
        return dm.ViewId(space=self.schema_space, external_id=self.external_id, version=self.version)

    def as_property_ref(self, property) -> list[str]:
        return [self.schema_space, f"{self.external_id}/{self.version}", property]


class FilterConfig(BaseModel, alias_generator=to_camel):
    values: Optional[list[AnnotationStatus | str] | AnnotationStatus | str] = None
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
    limit: Optional[int] = -1

    def build_filter(self) -> Filter:
        list_filters: list[Filter] = [f.as_filter(self.target_view) for f in self.filters]

        if len(list_filters) == 1:
            return list_filters[0]
        else:
            return dm.filters.And(*list_filters)  # NOTE: '*' Unpacks each filter in the list


class ConnectionFlagsConfig(BaseModel, alias_generator=to_camel):
    no_text_inbetween: Optional[bool] = None
    natural_reading_order: Optional[bool] = None

    def as_connection_flag(self) -> ConnectionFlags:
        params = {key: value for key, value in self.model_dump().items() if value is not None}
        return ConnectionFlags(**params)


class CustomizeFuzzinessConfig(BaseModel, alias_generator=to_camel):
    fuzzy_score: Optional[float] = None
    max_boxes: Optional[int] = None
    min_chars: Optional[int] = None

    def as_customize_fuzziness(self) -> CustomizeFuzziness:
        params = {key: value for key, value in self.model_dump().items() if value is not None}
        return CustomizeFuzziness(**params)


class DirectionWeightsConfig(BaseModel, alias_generator=to_camel):
    left: Optional[float] = None
    right: Optional[float] = None
    up: Optional[float] = None
    down: Optional[float] = None

    def as_direction_weights(self) -> DirectionWeights:
        params = {key: value for key, value in self.model_dump().items() if value is not None}
        return DirectionWeights(**params)


class DiagramDetectConfigModel(BaseModel, alias_generator=to_camel):
    # NOTE: configs come from V7 of the cognite python sdk cognite SDK
    annotation_extract: Optional[bool] = None
    case_sensitive: Optional[bool] = None
    connection_flags: Optional[ConnectionFlagsConfig] = None
    customize_fuzziness: Optional[CustomizeFuzzinessConfig] = None
    direction_delta: Optional[float] = None
    direction_weights: Optional[DirectionWeightsConfig] = None
    min_fuzzy_score: Optional[float] = None
    read_embedded_text: Optional[bool] = None
    remove_leading_zeros: Optional[bool] = None
    substitutions: Optional[dict[str, list[str]]] = None

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


class CacheServiceConfig(BaseModel, alias_generator=to_camel):
    cache_time_limit: int


class AnnotationServiceConfig(BaseModel, alias_generator=to_camel):
    page_range: int = Field(gt=0, le=50)
    partial_match: bool = True
    min_tokens: int = 1
    diagram_detect_config: Optional[DiagramDetectConfigModel] = None


class PrepareFunction(BaseModel, alias_generator=to_camel):
    get_files_for_annotation_reset_query: Optional[QueryConfig | list[QueryConfig]] = None
    get_files_to_annotate_query: QueryConfig | list[QueryConfig]


class LaunchFunction(BaseModel, alias_generator=to_camel):
    batch_size: int = Field(gt=0, le=50)
    primary_scope_property: str
    secondary_scope_property: Optional[str] = None
    file_search_property: str = "aliases"
    target_entities_search_property: str = "aliases"
    pattern_mode: bool
    file_resource_property: Optional[str] = None
    target_entities_resource_property: Optional[str] = None
    data_model_service: DataModelServiceConfig
    cache_service: CacheServiceConfig
    annotation_service: AnnotationServiceConfig


# Finalize Related Configs
class RetrieveServiceConfig(BaseModel, alias_generator=to_camel):
    get_job_id_query: QueryConfig | list[QueryConfig]


class ApplyServiceConfig(BaseModel, alias_generator=to_camel):
    auto_approval_threshold: float = Field(gt=0.0, le=1.0)
    auto_suggest_threshold: float = Field(gt=0.0, le=1.0)
    sink_node: NodeId


class FinalizeFunction(BaseModel, alias_generator=to_camel):
    clean_old_annotations: bool
    max_retry_attempts: int
    retrieve_service: RetrieveServiceConfig
    apply_service: ApplyServiceConfig


# Promote Related Configs
class TextNormalizationConfig(BaseModel, alias_generator=to_camel):
    """
    Configuration for text normalization and variation generation.

    Controls how text is normalized for matching and what variations are generated
    to improve match rates across different naming conventions.

    These flags affect both the normalize() function (for cache keys and direct matching)
    and generate_text_variations() function (for query-based matching).
    """

    remove_special_characters: bool = True
    convert_to_lowercase: bool = True
    strip_leading_zeros: bool = True


class EntitySearchServiceConfig(BaseModel, alias_generator=to_camel):
    """
    Configuration for the EntitySearchService in the promote function.

    Controls entity search and text normalization behavior:
    - Queries entities directly (server-side IN filter on entity/file aliases)
    - Text normalization for generating search variations

    Uses efficient server-side filtering on the smaller entity dataset rather than
    the larger annotation edge dataset for better performance at scale.
    """

    # enable_existing_annotations_search: bool = True # NOTE: Could be useful in the future - currently unused
    enable_global_entity_search: bool = True
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
    RAW database and table configuration is centralized in rawTables section.
    """

    get_candidates_query: QueryConfig | list[QueryConfig]
    delete_rejected_edges: bool
    delete_suggested_edges: bool
    entity_search_service: EntitySearchServiceConfig


class DataModelViews(BaseModel, alias_generator=to_camel):
    core_annotation_view: ViewPropertyConfig
    annotation_state_view: ViewPropertyConfig
    file_view: ViewPropertyConfig
    target_entities_view: ViewPropertyConfig


class RawTables(BaseModel, alias_generator=to_camel):
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


class Config(BaseModel, alias_generator=to_camel):
    data_model_views: DataModelViews
    raw_tables: RawTables
    prepare_function: PrepareFunction
    launch_function: LaunchFunction
    finalize_function: FinalizeFunction
    promote_function: PromoteFunctionConfig

    @classmethod
    def parse_direct_relation(cls, value: Any) -> Any:
        if isinstance(value, dict):
            return dm.DirectRelationReference.load(value)
        return value


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
    lines = ["=" * 80, f"FUNCTION: Prepare ({pipeline_ext_id})", "=" * 80, "", "PREPARE SERVICE CONFIG"]

    # Files to Annotate Query
    lines.append(_format_query_summary(config.prepare_function.get_files_to_annotate_query, "Files to Annotate Query"))

    # Files for Annotation Reset Query (if configured)
    if config.prepare_function.get_files_for_annotation_reset_query is not None:
        lines.append("")
        lines.append(
            _format_query_summary(
                config.prepare_function.get_files_for_annotation_reset_query, "Files for Annotation Reset Query"
            )
        )

    lines.extend(["", "=" * 80])
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
        "=" * 80,
        f"FUNCTION: Launch ({pipeline_ext_id})",
        "=" * 80,
        "",
        "LAUNCH SERVICE CONFIG",
        f"  • Batch size: {launch.batch_size}",
        f"  • Pattern mode: {launch.pattern_mode}",
        f"  • Primary scope property: {launch.primary_scope_property}",
        f"  • Secondary scope property: {launch.secondary_scope_property}",
        f"  • File search property: {launch.file_search_property}",
        f"  • Target entities search property: {launch.target_entities_search_property}",
        "",
        "DATA MODEL SERVICE",
    ]

    # Add queries
    lines.append(_format_query_summary(launch.data_model_service.get_files_to_process_query, "Files to Process Query"))
    lines.append("")
    lines.append(_format_query_summary(launch.data_model_service.get_target_entities_query, "Target Entities Query"))
    lines.append("")
    lines.append(_format_query_summary(launch.data_model_service.get_file_entities_query, "File Entities Query"))

    # Cache service
    cache = launch.cache_service
    raw = config.raw_tables
    lines.extend(
        [
            "",
            "CACHE SERVICE",
            f"  • Cache time limit: {cache.cache_time_limit} hours",
            "",
            "RAW TABLES (from consolidated config)",
            f"  • RAW DB: {raw.raw_db}",
            f"  • Cache table: {raw.raw_table_cache}",
            f"  • Manual patterns catalog: {raw.raw_manual_patterns_catalog}",
        ]
    )

    # Annotation service
    annot = launch.annotation_service
    lines.extend(
        [
            "",
            "ANNOTATION SERVICE",
            f"  • Page range: {annot.page_range} pages",
            f"  • Partial match: {annot.partial_match}",
            f"  • Min tokens: {annot.min_tokens}",
        ]
    )

    lines.append(_format_diagram_detect_config(annot.diagram_detect_config))

    lines.extend(["", "=" * 80])
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
        "=" * 80,
        f"FUNCTION: Finalize ({pipeline_ext_id})",
        "=" * 80,
        "",
        "FINALIZE SERVICE CONFIG",
        f"  • Clean old annotations: {finalize.clean_old_annotations}",
        f"  • Max retry attempts: {finalize.max_retry_attempts}",
        "",
        "RETRIEVE SERVICE",
    ]

    lines.append(_format_query_summary(finalize.retrieve_service.get_job_id_query, "Job ID Query"))

    # Apply service
    apply = finalize.apply_service
    raw = config.raw_tables
    lines.extend(
        [
            "",
            "APPLY SERVICE",
            f"  • Auto approval threshold: {apply.auto_approval_threshold}",
            f"  • Auto suggest threshold: {apply.auto_suggest_threshold}",
            f"  • Sink node: {apply.sink_node.space}/{apply.sink_node.external_id}",
            "",
            "RAW TABLES (from consolidated config)",
            f"  • RAW DB: {raw.raw_db}",
            f"  • Doc-Tag table: {raw.raw_table_doc_tag}",
            f"  • Doc-Doc table: {raw.raw_table_doc_doc}",
            f"  • Doc-Pattern table: {raw.raw_table_doc_pattern}",
        ]
    )

    lines.extend(["", "=" * 80])
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
        "=" * 80,
        f"FUNCTION: Promote ({pipeline_ext_id})",
        "=" * 80,
        "",
        "PROMOTE SERVICE CONFIG",
        f"  • Delete rejected edges: {promote.delete_rejected_edges}",
        f"  • Delete suggested edges: {promote.delete_suggested_edges}",
        "",
        "RAW TABLES (from consolidated config)",
        f"  • RAW DB: {raw.raw_db}",
        f"  • Doc-Tag table: {raw.raw_table_doc_tag}",
        f"  • Doc-Doc table: {raw.raw_table_doc_doc}",
        f"  • Doc-Pattern table: {raw.raw_table_doc_pattern}",
        f"  • Promote cache table: {raw.raw_table_promote_cache}",
        "",
    ]

    lines.append(_format_query_summary(promote.get_candidates_query, "Candidates Query"))

    # Entity search service
    entity_search = promote.entity_search_service
    text_norm = entity_search.text_normalization
    lines.extend(
        [
            "",
            "ENTITY SEARCH SERVICE",
            f"  • Enable global entity search: {entity_search.enable_global_entity_search}",
            f"  • Max entity search limit: {entity_search.max_entity_search_limit}",
            "  • Text normalization:",
            f"    - Remove special characters: {text_norm.remove_special_characters}",
            f"    - Convert to lowercase: {text_norm.convert_to_lowercase}",
            f"    - Strip leading zeros: {text_norm.strip_leading_zeros}",
        ]
    )

    lines.extend(["", "=" * 80])
    return "\n".join(lines)


def load_config_parameters(
    client: CogniteClient,
    function_data: dict[str, Any],
) -> Config:
    """
    Retrieves the configuration parameters from the function data and loads the configuration from CDF.
    """
    if "ExtractionPipelineExtId" not in function_data:
        raise ValueError("Missing key 'ExtractionPipelineExtId' in input data to the function")

    pipeline_ext_id = function_data["ExtractionPipelineExtId"]
    try:
        raw_config = client.extraction_pipelines.config.retrieve(pipeline_ext_id)
        if raw_config.config is None:
            raise ValueError(f"No config found for extraction pipeline: {pipeline_ext_id!r}")
    except CogniteAPIError:
        raise RuntimeError(f"Not able to retrieve pipeline config for extraction pipeline: {pipeline_ext_id!r}")

    loaded_yaml_data = yaml.safe_load(raw_config.config)

    if isinstance(loaded_yaml_data, dict):
        return Config.model_validate(loaded_yaml_data)
    else:
        raise ValueError(
            "Invalid configuration structure from CDF: \nExpected a YAML dictionary with a top-level 'config' key."
        )
