from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Union

import yaml
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.ids import ViewId
from cognite.client.data_classes.filters import Filter
from pydantic import BaseModel, Field, field_validator, model_validator

from .utils.DataStructures import ExtractionType, FilterOperator, SourceFieldParameter
from .utils.RegexMethodParameter import RegexOptions

SkipEntityPolicy = Literal["successful_only", "none"]

ExtractionHandlerId = Literal["regex_handler", "heuristic"]
FieldResultsMode = Literal["merge_all"]


# Configuration classes
class Parameters(BaseModel):
    debug: bool = Field(False, description="Enable debug mode")
    verbose: bool = Field(False, description="Enable verbose output")
    run_all: bool = Field(
        False,
        description=(
            "When true, instance listing is not filtered by existing RAW rows; incremental "
            "detection processes the full scope; apply replaces keys from RAW only. When false, "
            "see skip_entity_policy for RAW-based exclusion and merge behavior on apply."
        ),
    )
    max_files: Optional[int] = Field(
        None,
        description="Optional limit for how many file entities to process (across all source_views).",
    )
    raw_db: str = Field(..., description="ID of the raw database")
    raw_table_key: str = Field(
        ...,
        description=(
            "RAW table for extraction payloads, per-entity processing state (RECORD_KIND=entity, "
            "EXTRACTION_STATUS), and run-summary rows (RECORD_KIND=run). "
            "Convention: `key_extraction_state` (default scope) or `{site}_key_extraction_state` (workflows)."
        ),
    )
    skip_entity_policy: SkipEntityPolicy = Field(
        "successful_only",
        description=(
            "When run_all is false: successful_only excludes instances that have a RAW entity row "
            "with EXTRACTION_STATUS success or empty; failed or rows without that column are listed "
            "again. none matches run_all true for listing (no RAW-based exclusion)."
        ),
    )
    write_empty_extraction_rows: bool = Field(
        False,
        description=(
            "If true, write a RAW entity row with EXTRACTION_STATUS=empty when extraction yields "
            "no candidate keys and no FK refs (stops re-querying those instances when using "
            "successful_only)."
        ),
    )
    raw_skip_scan_chunk_size: int = Field(
        5000,
        ge=100,
        le=10000,
        description="Chunk size when scanning raw_table_key for skip_entity_policy (RAW rows iterator).",
    )
    incremental_change_processing: bool = Field(
        True,
        description=(
            "Must be true for CDF/workflow runs: use WORKFLOW_STATUS-driven RAW cohort handoff "
            "(run fn_dm_incremental_state_update first, then key extraction reads cohort rows "
            "RUN_ID + WORKFLOW_STATUS=detected). Direct Data Modeling view listing is not supported."
        ),
    )
    enable_reference_index: bool = Field(
        False,
        description=(
            "When true, workflow/local parity should run fn_dm_reference_index (RAW inverted index "
            "for FK and document references). Align with the same flag in the workflow task payload "
            "for fn_dm_reference_index."
        ),
    )
    incremental_skip_unchanged_source_inputs: bool = Field(
        True,
        description=(
            "When true (default) together with incremental_change_processing: fn_dm_incremental_state_update "
            "skips emitting cohort rows when the content hash matches the last completed state "
            "for that node (RAW EXTRACTION_INPUTS_HASH when key_discovery_instance_space is unset; "
            "otherwise Key Discovery FDM state). lastUpdatedTime watermarks still advance. "
            "fn_dm_key_extraction writes EXTRACTION_INPUTS_HASH to RAW only when key_discovery_instance_space is unset."
        ),
    )
    workflow_scope: Optional[str] = Field(
        None,
        description=(
            "Leaf deployment scope id (same as scope.id); set by module build / scope_build. "
            "Required for Key Discovery FDM state when incremental_change_processing is true."
        ),
    )
    key_discovery_instance_space: Optional[str] = Field(
        None,
        description=(
            "When set, incremental watermark/hash/prior state use Key Discovery FDM views in this "
            "instance space instead of RAW scans."
        ),
    )
    key_discovery_schema_space: str = Field(
        "dm_key_discovery",
        description="DMS space for KeyDiscoveryProcessingState / KeyDiscoveryScopeCheckpoint views.",
    )
    key_discovery_dm_version: str = Field(
        "v1",
        description="Version of Key Discovery views/containers.",
    )
    key_discovery_processing_state_view_external_id: str = Field(
        "KeyDiscoveryProcessingState",
        description="External id of the KeyDiscoveryProcessingState view.",
    )
    key_discovery_checkpoint_view_external_id: str = Field(
        "KeyDiscoveryScopeCheckpoint",
        description="External id of the KeyDiscoveryScopeCheckpoint view.",
    )
    cdm_view_version: str = Field(
        "v1",
        description="cdf_cdm CogniteDescribable view version used for state node upserts.",
    )
    exclude_self_referencing_keys: Union[bool, Dict[str, Any]] = Field(
        True,
        description=(
            "If True, drop foreign_key_reference values that match a candidate_key on the same "
            "instance. If a dict, keys are entity_type (e.g. timeseries) and optional 'default'; "
            "per-type value overrides default."
        ),
    )


class EntityType(Enum):
    """Represents the possible types of data."""

    ASSET = "asset"
    FILE = "file"
    TIMESERIES = "timeseries"
    EQUIPMENT = "equipment"


class ViewPropertyConfig(BaseModel):
    schema_space: str
    instance_space: Optional[str] = None
    external_id: str
    version: str
    search_property: str

    def as_view_id(self) -> dm.ViewId:
        """Convert this config to a `ViewId`."""
        return dm.ViewId(
            space=self.schema_space, external_id=self.external_id, version=self.version
        )

    def as_property_ref(self, property) -> list[str]:
        """Build a data modeling property reference for filters/search."""
        return [self.schema_space, f"{self.external_id}/{self.version}", property]


class FilterConfig(BaseModel):
    values: Optional[list[str] | str] = None
    negate: bool = False
    operator: FilterOperator
    target_property: str
    property_scope: Literal["view", "node"] = Field(
        "view",
        description='Use "view" for view properties (default) or "node" for node metadata (e.g. space, externalId).',
    )

    def as_filter(self, view_properties: ViewPropertyConfig) -> Filter:
        """Convert this config to a Cognite Data Modeling filter."""
        if self.property_scope == "node":
            property_reference = ("node", self.target_property)
        else:
            property_reference = view_properties.as_property_ref(self.target_property)
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
                raise ValueError(
                    f"Operator 'IN' requires a list of values for property {self.target_property}"
                )
            filter = dm.filters.In(property=property_reference, values=find_values)
        elif self.operator == FilterOperator.EQUALS:
            filter = dm.filters.Equals(property=property_reference, value=find_values)
        elif self.operator == FilterOperator.CONTAINSALL:
            filter = dm.filters.ContainsAll(
                property=property_reference, values=find_values
            )
        elif self.operator == FilterOperator.CONTAINSANY:
            if not isinstance(find_values, list):
                raise ValueError(
                    f"Operator 'CONTAINSANY' requires a list of values for property {self.target_property}"
                )
            filter = dm.filters.ContainsAny(
                property=property_reference, values=find_values
            )
        elif self.operator == FilterOperator.SEARCH:
            filter = dm.filters.Search(property=property_reference, value=find_values)
        else:
            raise NotImplementedError(f"Operator {self.operator} is not implemented.")

        if self.negate:
            return dm.filters.Not(filter)
        return filter


class ViewConfig(BaseModel):
    view_external_id: str = Field(
        ..., description="External ID of the data model view (e.g., 'txEquipment')."
    )
    view_space: str = Field(
        ...,
        description="Space where the view is defined (e.g., 'sp_enterprise_process_industry').",
    )
    view_version: str = Field(
        ..., description="Version of the view schema (e.g., 'v1')."
    )
    instance_space: Optional[str] = Field(
        None,
        description=(
            "Optional instance space passed to instances.list(space=...). "
            "Omit to query across spaces (combine with filters, e.g. property_scope: node "
            "and target_property: space)."
        ),
    )
    entity_type: EntityType = Field(
        ..., description="Type of entity for processing context (e.g., DataType.ASSET)."
    )

    def as_view_id(self) -> dm.ViewId:
        return dm.ViewId(
            space=self.view_space,
            external_id=self.view_external_id,
            version=self.view_version,
        )


class TargetViewConfig(ViewConfig):
    target_prop: str = Field(
        "aliases",
        description="The target property to update in the target view (e.g., 'aliases').",
    )


class SourceViewConfig(TargetViewConfig):
    """Configuration and scope for querying data views."""

    batch_size: int = Field(
        1000,
        description="Number of entities to process per batch (default 1000).",
    )
    filters: Optional[List[Any]] = Field(
        None,
        description=(
            "CDF Data Modeling filters (optional). Each entry may be a leaf "
            "(operator, target_property, property_scope, values, negate) or a boolean group "
            "with keys ``and``, ``or``, or ``not`` (see Cognite filter grammar). "
            "RANGE may set gt, gte, lt, lte on the leaf object."
        ),
    )
    include_properties: List[str] = Field(
        default_factory=[],
        description="List of properties to retrieve (optional).",
    )
    resource_property: str = Field(
        "externalId",
        description="The resource property that adds granularity to the instances.",
    )
    exclude_self_referencing_keys: Optional[bool] = Field(
        None,
        description=(
            "If set, overrides parameters.exclude_self_referencing_keys for entities listed "
            "from this source view. None inherits global parameters (bool or per-entity_type map)."
        ),
    )
    validation: Optional[ValidationConfig] = Field(
        None,
        description=(
            "Optional validation overlay merged with global data.validation for entities "
            "from this view (scalar overrides; confidence_match_rules concatenated then sorted)."
        ),
    )
    key_discovery_hash_property_paths: Optional[List[str]] = Field(
        None,
        description=(
            "When non-empty, incremental content hash uses only these view property paths "
            "(same syntax as rule source_field.field_name). When omitted, hash fields match "
            "iter_wanted_fields / extraction rules."
        ),
    )

    def as_view_id(self) -> ViewId:
        """Convert this source view config to a `ViewId`."""
        return ViewId(
            space=self.view_space,
            external_id=self.view_external_id,
            version=self.view_version,
        )

    def build_filter(self) -> Filter:
        """Build user filter nodes (ANDed); extraction pipeline adds ``HasData`` separately."""
        from .utils.source_view_filter_build import build_source_view_user_filters

        return build_source_view_user_filters(self.as_view_id(), self.filters or [])


class SourceTableConfig(BaseModel):
    table_name: str = Field(..., description="Name of the source table in RAW.")
    database_name: str = Field(..., description="Name of the RAW database.")
    join_fields: Dict[str, str] = Field(
        ...,
        description="Join keys: view_field (column on the instance dataframe) and table_field (column in RAW).",
    )
    columns: Optional[List[str]] = Field(
        None,
        description="Columns to read from RAW (None = all). Non-join columns are prefixed as {table_id}__{column} after join.",
    )
    table_id: str = Field(
        ...,
        description="Stable id for this table; use the same value in extraction rule source_fields.table_id.",
    )


class ConfidenceModifier(BaseModel):
    mode: Literal["explicit", "offset"] = Field(
        ...,
        description="explicit: set confidence to value (clamped). offset: add value to current confidence (clamped).",
    )
    value: float = Field(..., description="Target confidence (explicit) or delta (offset).")


class ConfidenceMatchExpression(BaseModel):
    pattern: str = Field(..., description="Regex pattern; match if re.search succeeds on the key value.")
    description: Optional[str] = Field(
        None,
        description="Human-readable label for authors (ignored by the engine).",
    )


class ConfidenceMatchSpec(BaseModel):
    expressions: List[Union[str, ConfidenceMatchExpression]] = Field(
        default_factory=list,
        description=(
            "Regex patterns as plain strings, or {pattern, description} objects; "
            "matched using the parent rule's expression_match (or validation default): "
            "search -> re.search, fullmatch -> re.fullmatch."
        ),
    )
    keywords: List[str] = Field(
        default_factory=list,
        description="Substrings; match if any keyword is contained in the key value (case-insensitive).",
    )

    @model_validator(mode="after")
    def at_least_one_matcher(self) -> ConfidenceMatchSpec:
        def _expr_nonempty(item: Union[str, ConfidenceMatchExpression]) -> bool:
            if isinstance(item, str):
                return bool(str(item).strip())
            return bool(str(item.pattern).strip())

        ex_ok = any(_expr_nonempty(x) for x in self.expressions)
        kw_ok = any(str(x).strip() for x in self.keywords)
        if not ex_ok and not kw_ok:
            raise ValueError("match requires at least one non-empty expression or keyword")
        return self


ExpressionMatchMode = Literal["search", "fullmatch"]


class ConfidenceMatchRule(BaseModel):
    name: Optional[str] = Field(None, description="Optional label for logs.")
    enabled: bool = True
    priority: Optional[int] = Field(
        None,
        description="Lower runs first. If omitted, order is list_index * 10.",
    )
    expression_match: Optional[ExpressionMatchMode] = Field(
        None,
        description=(
            "How match.expressions are applied for this rule: search (re.search) or "
            "fullmatch (re.fullmatch). Omit to use validation.expression_match, then search."
        ),
    )
    match: ConfidenceMatchSpec
    confidence_modifier: ConfidenceModifier


class FieldExtractionSpec(BaseModel):
    """One extractable field within a regex_handler rule."""

    field_name: str = Field(
        ...,
        description="Property name or dotted path (e.g. name, metadata.code).",
    )
    table_id: Optional[str] = Field(
        None,
        description="When using RAW source_tables, join key matching SourceTableConfig.table_id.",
    )
    variable: Optional[str] = Field(
        None,
        description="Name for result_template placeholders; defaults to field_name.",
    )
    regex: Optional[str] = Field(
        None,
        description="If set, find all matches (group 1 if present). If None, trimmed passthrough.",
    )
    regex_options: RegexOptions = Field(
        default_factory=RegexOptions,
        description="Per-field regex flags.",
    )
    max_matches_per_field: int = Field(
        100,
        ge=1,
        le=10000,
        description="Cap regex matches per field.",
    )
    preprocessing: Union[List[str], str, None] = Field(
        None,
        description="Optional steps before extraction: trim, lowercase, uppercase, remove_special_chars.",
    )
    max_length: int = Field(
        1000,
        ge=1,
        le=100000,
        description="Max processed field length (performance).",
    )
    required: bool = Field(
        False,
        description="If true, skip rule when this field is missing (engine may log).",
    )
    priority: int = Field(1, description="Order hint for field lists (informational).")


class HeuristicStrategySpec(BaseModel):
    id: str = Field(..., description="Strategy id, e.g. delimiter_split, sliding_token")
    weight: float = Field(1.0, description="Weight for combining scores")


class HeuristicRuleParameters(BaseModel):
    """Parameters for handler: heuristic only."""

    strategies: List[HeuristicStrategySpec] = Field(
        default_factory=list,
        description="Ordered heuristic strategies with weights",
    )
    max_candidates_per_field: int = Field(
        20,
        ge=1,
        le=5000,
        description="Max candidate substrings emitted per source field",
    )


class ValidationConfig(BaseModel):
    min_confidence: float = Field(
        0.1,
        description="Minimum confidence for validated keys.",
    )
    expression_match: Optional[ExpressionMatchMode] = Field(
        None,
        description=(
            "Default expression match mode for confidence_match_rules that omit expression_match "
            "(search or fullmatch). If omitted, rules default to search."
        ),
    )
    regexp_match: Union[str, List[str], None] = Field(
        default=None,
        description=(
            "Deprecated: prefer confidence_match_rules with expressions. If set, engine may still "
            "accept config but post-processing should use rules only."
        ),
    )
    confidence_match_rules: List[ConfidenceMatchRule] = Field(
        default_factory=list,
        description=(
            "Ordered confidence adjustments per key (priority, then list order). "
            "For each key, each rule: if match succeeds, apply modifier — offset chains with "
            "later rules; explicit sets confidence and stops further rules for that key."
        ),
    )


class ExtractionRuleConfig(BaseModel):
    """Configuration for a single extraction rule (regex_handler or heuristic)."""

    rule_id: Optional[str] = None
    description: str = Field("", description="Human-readable description for operators")
    handler: ExtractionHandlerId = Field(
        "regex_handler",
        description="regex_handler | heuristic",
    )
    parameters: Optional[HeuristicRuleParameters] = Field(
        None,
        description="Required when handler is heuristic; strategy configuration",
    )
    fields: List[FieldExtractionSpec] = Field(
        default_factory=list,
        description="Fields to read for this rule (all handlers)",
    )
    entity_types: List[str] = Field(
        default_factory=list,
        description="If non-empty, rule applies only to these entity types (lowercased match). Empty = all",
    )
    field_results_mode: FieldResultsMode = Field(
        "merge_all",
        description="merge_all only: use all field specs (legacy first_match is coerced to merge_all).",
    )
    result_template: Optional[str] = Field(
        None,
        description="Optional template e.g. {unit}-{tag}; Cartesian product of variable lists when set",
    )
    max_template_combinations: int = Field(
        10000,
        ge=1,
        le=1_000_000,
        description="Cap Cartesian combinations for result_template",
    )
    enabled: bool = True
    priority: int = Field(
        100,
        description="Lower runs first among rules (convention: sort ascending)",
    )
    scope_filters: Dict[str, Any] = Field(
        default_factory=dict,
        description="Optional filters to limit the scope of this extraction rule.",
    )
    extraction_type: ExtractionType = Field(
        ExtractionType.CANDIDATE_KEY,
        description="Type of extraction (e.g., 'candidate_key', 'foreign_key_reference').",
    )
    validation: Optional[ValidationConfig] = Field(
        None,
        description="Validation configuration for the extraction rule.",
    )

    @model_validator(mode="before")
    @classmethod
    def _fill_rule_id_from_name(cls, data: Any) -> Any:
        if isinstance(data, dict):
            data = dict(data)
            if not data.get("rule_id") and data.get("name"):
                data["rule_id"] = str(data["name"])
            # Legacy handler id (pre regex_handler rename)
            if data.get("handler") == "field_rule":
                data["handler"] = "regex_handler"
            # Removed field_rule_fixed_width — same engine path as regex_handler
            if data.get("handler") == "field_rule_fixed_width":
                data["handler"] = "regex_handler"
            if str(data.get("field_results_mode") or "").strip().lower() == "first_match":
                data["field_results_mode"] = "merge_all"
        return data

    @field_validator("parameters", mode="before")
    @classmethod
    def _coerce_heuristic_parameters(cls, v: Any) -> Any:
        if v is None or v == {}:
            return None
        if isinstance(v, HeuristicRuleParameters):
            return v
        if isinstance(v, dict):
            return HeuristicRuleParameters.model_validate(v)
        return v

    @model_validator(mode="after")
    def _validate_handler_payload(self) -> "ExtractionRuleConfig":
        if self.handler == "heuristic":
            if self.parameters is None:
                raise ValueError("handler heuristic requires non-empty parameters")
            if not self.fields:
                raise ValueError("handler heuristic requires at least one field")
        return self

    @property
    def name(self) -> str:
        """Alias for rule_id for backward compatibility."""
        return self.rule_id or "unnamed_rule"

    @property
    def min_confidence(self) -> float:
        """Minimum confidence from validation or default."""
        return self.validation.min_confidence if self.validation else 0.1

    @property
    def get_source_field_string(self) -> str:
        """Returns a string of all the source fields used for this rule."""
        return ", ".join(f.field_name for f in self.fields)


class ConfigData(BaseModel):
    source_view: Optional[SourceViewConfig] = Field(
        None,
        description="Single source view (legacy). Prefer source_views for multi-view scopes.",
    )
    source_views: Optional[List[SourceViewConfig]] = Field(
        None,
        description="Source views to query. When set, used by pipeline and engine (with source_view as fallback).",
    )
    source_tables: Optional[List[SourceTableConfig]] = None
    extraction_rules: List[ExtractionRuleConfig]
    validation: Optional[ValidationConfig] = Field(
        None,
        description="Global validation (min_confidence, regexp_match, confidence_match_rules) merged in engine.",
    )

    @model_validator(mode="after")
    def _require_source_view_or_views(self) -> "ConfigData":
        if self.source_view is None and not self.source_views:
            raise ValueError(
                "data must include source_view or a non-empty source_views list"
            )
        return self


class Config(BaseModel):
    parameters: Parameters
    data: ConfigData

    @classmethod
    def pares_direct_relation(cls, value: Any) -> Any:
        """Parse direct relation references from dict input."""
        if isinstance(value, dict):
            return dm.DirectRelationReference.load(value)
        return value


__all__ = [
    "Parameters",
    "EntityType",
    "ViewPropertyConfig",
    "FilterConfig",
    "SourceViewConfig",
    "FieldExtractionSpec",
    "HeuristicStrategySpec",
    "HeuristicRuleParameters",
    "ExtractionRuleConfig",
    "ConfidenceModifier",
    "ConfidenceMatchExpression",
    "ConfidenceMatchSpec",
    "ConfidenceMatchRule",
    "ValidationConfig",
    "ConfigData",
    "Config",
]
