from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Union

import yaml
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.ids import ViewId
from cognite.client.data_classes.filters import Filter
from pydantic import BaseModel, Field, model_validator

from .utils.DataStructures import ExtractionType, FilterOperator, SourceFieldParameter
from .utils.FixedWidthMethodParameter import FixedWidthMethodParameter
from .utils.HeuristicMethodParameter import HeuristicMethodParameter
from .utils.PassthroughMethodParameter import PassthroughMethodParameter
from .utils.RegexMethodParameter import RegexMethodParameter
from .utils.TokenReassemblyMethodParameter import TokenReassemblyMethodParameter

SkipEntityPolicy = Literal["successful_only", "none"]


# Configuration classes
class Parameters(BaseModel):
    debug: bool = Field(False, description="Enable debug mode")
    verbose: bool = Field(False, description="Enable verbose output")
    full_rescan: bool = Field(
        False,
        description=(
            "When true, instance listing is not filtered by existing RAW rows; incremental "
            "detection uses a full scope rescan; apply replaces keys from RAW only. When false, "
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
            "When full_rescan is false: successful_only excludes instances that have a RAW entity row "
            "with EXTRACTION_STATUS success or empty; failed or rows without that column are listed "
            "again. none matches full_rescan true for listing (no RAW-based exclusion)."
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
        False,
        description=(
            "When true, use WORKFLOW_STATUS-driven RAW cohort handoff: run "
            "fn_dm_incremental_state_update first, then key extraction reads cohort rows "
            "(RUN_ID + WORKFLOW_STATUS) instead of listing the full view."
        ),
    )
    incremental_skip_unchanged_source_inputs: bool = Field(
        True,
        description=(
            "When true (default) together with incremental_change_processing: fn_dm_incremental_state_update "
            "skips emitting cohort rows when EXTRACTION_INPUTS_HASH matches the last completed "
            "entity row for that node in RAW (lastUpdatedTime watermarks still advance). "
            "fn_dm_key_extraction writes EXTRACTION_INPUTS_HASH on incremental entity rows."
        ),
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
    filters: Optional[List[FilterConfig]] = Field(
        None,
        description="CDF DMS filter to limit query scope (optional).",
    )
    include_properties: List[str] = Field(
        default_factory=[],
        description="List of properties to retrieve (optional).",
    )
    resource_property: str = Field(
        ...,
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

    def as_view_id(self) -> ViewId:
        """Convert this source view config to a `ViewId`."""
        return ViewId(
            space=self.view_space,
            external_id=self.view_external_id,
            version=self.view_version,
        )

    def build_filter(self) -> Filter:
        """Build a combined filter from `filters` (ANDed), or return a single filter."""
        target_view = ViewPropertyConfig(
            schema_space=self.view_space,
            instance_space=self.instance_space,
            external_id=self.view_external_id,
            version=self.view_version,
            search_property="",
        )
        list_filters: list[Filter] = (
            [f.as_filter(target_view) for f in (self.filters or [])]
        )
        if len(list_filters) == 1:
            return list_filters[0]
        if not list_filters:
            return dm.filters.HasData(views=[self.as_view_id()])
        return dm.filters.And(*list_filters)


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


# Union for dynamic parameter object in ExtractionRuleConfig
ExtractionMethod = Union[
    FixedWidthMethodParameter,
    HeuristicMethodParameter,
    PassthroughMethodParameter,
    RegexMethodParameter,
    TokenReassemblyMethodParameter,
]


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
            "match if any re.search succeeds on the key value."
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


class ConfidenceMatchRule(BaseModel):
    name: Optional[str] = Field(None, description="Optional label for logs.")
    enabled: bool = True
    priority: Optional[int] = Field(
        None,
        description="Lower runs first. If omitted, order is list_index * 10.",
    )
    match: ConfidenceMatchSpec
    confidence_modifier: ConfidenceModifier


class ValidationConfig(BaseModel):
    min_confidence: float = Field(
        0.1,
        description="Minimum confidence for validated keys.",
    )
    regexp_match: Union[str, List[str], None] = Field(
        default=None,
        description="Regular expression(s) that the extracted key must match to be considered valid.",
    )
    confidence_match_rules: List[ConfidenceMatchRule] = Field(
        default_factory=list,
        description=(
            "Ordered confidence adjustments per key: first matching rule wins (by priority, then list order)."
        ),
    )


class ExtractionRuleConfig(BaseModel):
    """Configuration for a single extraction rule."""

    rule_id: str
    method: Literal[
        "passthrough", "regex", "fixed width", "token reassembly", "heuristic"
    ] = "passthrough"
    config: ExtractionMethod = Field(
        ...,
        description="Method-specific configuration parameters.",
        alias="parameters",
    )
    source_fields: Union[List[SourceFieldParameter], SourceFieldParameter, None] = Field(
        None,
        description="List of source field paths to extract data from.",
    )
    priority: int = Field(
        100,
        description="The priority of the rule in the order of rules applied to the target field",
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

    @property
    def name(self) -> str:
        """Alias for rule_id for backward compatibility."""
        return self.rule_id

    @property
    def min_confidence(self) -> float:
        """Minimum confidence from validation or default."""
        return self.validation.min_confidence if self.validation else 0.1

    @property
    def get_source_field_string(self) -> str:
        """Returns a string of all the source fields used for this rule."""
        if isinstance(self.source_fields, list):
            return ", ".join([sf.field_name for sf in self.source_fields])
        if isinstance(self.source_fields, SourceFieldParameter):
            return self.source_fields.field_name
        return ""


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
    field_selection_strategy: Literal["first_match", "merge_all"]
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
    "SourceFieldConfig",
    "ExtractionRuleConfig",
    "ConfidenceModifier",
    "ConfidenceMatchExpression",
    "ConfidenceMatchSpec",
    "ConfidenceMatchRule",
    "ValidationConfig",
    "ConfigData",
    "Config",
]
