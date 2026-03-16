from __future__ import annotations

from enum import Enum
from typing import Any, List, Literal, Optional, Union, Dict

import yaml
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.data_modeling.ids import ViewId
from cognite.client.data_classes.filters import Filter
from cognite.client.exceptions import CogniteAPIError
from pydantic import BaseModel, Field

from .utils.DataStructures import ExtractionType, FilterOperator, SourceFieldParameter
from .utils.FixedWidthMethodParameter import FixedWidthMethodParameter
from .utils.HeuristicMethodParameter import HeuristicMethodParameter
from .utils.RegexMethodParameter import RegexMethodParameter
from .utils.TokenReassemblyMethodParameter import TokenReassemblyMethodParameter


# Configuration classes
class Parameters(BaseModel):
    debug: bool = Field(False, description="Enable debug mode")
    verbose: bool = Field(False, description="Enable verbose output")
    run_all: bool = Field(True, description="Run all extraction rules")
    overwrite: bool = Field(False, description="Overwrite existing results")
    apply: bool = Field(True, description="Apply results to instances")
    min_key_length: int = Field(3, description="Minimum key length to apply")
    raw_db: str = Field(..., description="ID of the raw database")
    raw_table_state: str = Field(..., description="ID of the state table in RAW")
    raw_table_key: str = Field(..., description="ID of the key table in RAW")


class EntityType(Enum):
    """
    Represents the possible types of data.
    """

    ASSET = "asset"
    FILE = "file"
    TIMESERIES = "timeseries"


class ViewPropertyConfig(BaseModel):
    schema_space: str
    instance_space: str
    external_id: str
    version: str
    search_property: str

    def as_view_id(self) -> dm.ViewId:
        return dm.ViewId(
            space=self.schema_space, external_id=self.external_id, version=self.version
        )

    def as_property_ref(self, property) -> list[str]:
        return [self.schema_space, f"{self.external_id}/{self.version}", property]


class FilterConfig(BaseModel):
    values: Optional[list[str] | str] = None  # Maybe we add a 'match status' here?
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
        else:
            return filter

class ViewConfig(BaseModel):
        # 1. Mandatory Fields
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
    instance_space: str = Field(
        ..., description="The instance space that holds the target instances."
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
        'aliases',
        description="The target property to update in the target view (e.g., 'aliases')."
    )

class SourceViewConfig(ViewConfig):
    """
    A class to define the configuration and scope for querying data views.
    """
    batch_size: int = Field(
        ..., description="Number of entities to process per batch (e.g., 100)."
    )

    # 2. Optional Fields with Defaults/Mutable Defaults
    filters: Optional[List[FilterConfig]] = Field(
        None,  # Use alias if input JSON/YAML uses 'filter' instead of 'filters'
        description="CDF DMS filter to limit query scope (optional).",
    )

    # FIX: Use default_factory for the mutable default (List)
    include_properties: List[str] = Field(
        default_factory=[],
        description="List of properties to retrieve (optional, e.g., ['name', 'description']).",
    )

    resource_property: str = Field(
        ...,
        description="The resource property that adds granularity to the instances."
    )

    def as_view_id(self) -> ViewId:
        return ViewId(
            space=self.view_space,
            external_id=self.view_external_id,
            version=self.view_version,
        )

    def build_filter(self) -> Filter:
        target_view = ViewPropertyConfig(
            schema_space=self.view_space,
            instance_space=self.instance_space,
            external_id=self.view_external_id,
            version=self.view_version,
            search_property="",
        )

        list_filters: list[Filter] = [f.as_filter(target_view) for f in self.filters]

        if len(list_filters) == 1:
            return list_filters[0]
        else:
            return dm.filters.And(
                *list_filters
            )  # NOTE: '*' Unpacks each filter in the list

class SourceTableConfig(BaseModel):
    table_name: str = Field(..., description="Name of the source table in RAW (e.g., 'raw_table_key').")
    database_name: str = Field(..., description="Name of the RAW database (e.g., 'raw_db').")
    join_fields: Dict[str, str] = Field(..., description="Mapping of join fields between view and table (e.g., {'view_field': 'sourceId', 'table_field': 'sourceId'}).")
    columns: Optional[List[str]] = Field(..., description="Comma-separated list of columns to read from the source table (e.g., 'col1, col2, col3').")
    table_id: str = Field(..., description="Local identifier for the source table to be used in this function.")

# Union for dynamic parameter object in ExtractionRuleConfig
ExtractionMethod = Union[
    FixedWidthMethodParameter,
    HeuristicMethodParameter,
    RegexMethodParameter,
    TokenReassemblyMethodParameter,
]


class ExtractionRuleConfig(BaseModel):
    """
    Configuration for a single extraction rule, dynamically instantiating the
    correct method parameter class based on the 'method' field.
    """

    rule_id: str

    # This field defines which specific model (Regex, FixedWidth, etc.) to use.
    # We enforce that the incoming data must have a 'method' key that matches one
    # of the required 'method' fields defined in the Union members.
    method: Literal["regex", "fixed width", "token reassembly", "heuristic"]

    # This field MUST be renamed to hold all the method-specific parameters.
    # Pydantic will perform the dynamic dispatch using the 'method' field as the tag.
    config: ExtractionMethod = Field(..., description="Method-specific configuration parameters.")

    source_fields: Union[List[SourceFieldParameter], SourceFieldParameter] = Field(
        None, description="List of source field paths to extract data from."
    )

    priority: int = Field(
        100,
        description="The priority of the rule in the order of rules applied to the target field",
    )

    scope_filters: Dict[str, Any] = Field(
        default_factory=dict,
        description="Optional filters to limit the scope of this extraction rule."
    )
    
    extraction_type: ExtractionType = Field(
        ExtractionType.CANDIDATE_KEY,
        description="Type of extraction (e.g., 'candidate_key', 'foreign_key_reference')."
    )

    validation: Optional[ValidationConfig] = Field(
        None,
        description="Validation configuration for the extraction rule."
    )

    @property
    def name(self) -> str:
        """Alias for rule_id for backward compatibility."""
        return self.rule_id
    
    @property
    def get_source_field_string(self) -> str:
        """Returns a string of all the source fields used for this rule"""
        if isinstance(self.source_fields, list):
            return ", ".join([sf.field_name for sf in self.source_fields])
        elif isinstance(self.source_fields, SourceFieldParameter):
            return self.source_fields.field_name
        else:
            return ""


class ValidationConfig(BaseModel):
    min_confidence: float = Field(
        0.1, 
        description="Minimum confidence for validated keys."
    )

    blacklist_keywords: List[str] = Field(
        default_factory=[],
        description="List of keywords that, if present in the extracted key, invalidate it."
    )

    regexp_match: Union[str, List[str]] = Field(
        default=None,
        description="Regular expression(s) that the extracted key must match to be considered valid."
    )


class ConfigData(BaseModel):
    source_view: SourceViewConfig
    source_tables: Optional[List[SourceTableConfig]]
    # target_view: Optional[TargetViewConfig]
    extraction_rules: List[ExtractionRuleConfig]
    field_selection_strategy: Literal["first_match", "merge_all"]


class Config(BaseModel):
    parameters: Parameters
    data: ConfigData

    @classmethod
    def pares_direct_relation(cls, value: Any) -> Any:
        if isinstance(value, dict):
            return dm.DirectRelationReference.load(value)
        return value


def load_config_parameters(
    client: CogniteClient, function_data: dict[str, Any]
) -> Config:
    """Retrieves the configuration parameters from the function data and loads the configuration from CDF."""
    if "ExtractionPipelineExtId" not in function_data:
        raise ValueError(
            "Missing key 'ExtractionPipelineExtId' in input data to the function"
        )

    pipeline_ext_id = function_data["ExtractionPipelineExtId"]
    try:
        raw_config = client.extraction_pipelines.config.retrieve(pipeline_ext_id)
        if raw_config.config is None:
            raise ValueError(
                f"No config found for extraction pipeline: {pipeline_ext_id!r}"
            )
    except CogniteAPIError as e:
        raise RuntimeError(
            f"Not able to retrieve pipeline config for extraction pipeline: {pipeline_ext_id!r} Due to: {e}"
        )

    config_dict = yaml.safe_load(raw_config.config)
    return Config(**config_dict)
