from __future__ import annotations
from enum import Enum

from dataclasses import dataclass
from typing import Any, Literal, Union, Optional, List, Dict

from utils.FixedWidthMethodParameter import FixedWidthMethodParameter
from utils.HeuristicMethodParameter import HeuristicMethodParameter
from utils.RegexMethodParameter import RegexMethodParameter
from utils.TokenReassemblyMethodParameter import TokenReassemblyMethodParameter

import yaml
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.filters import Filter
from cognite.client.exceptions import CogniteAPIError
from cognite.client.data_classes.data_modeling.ids import ViewId
from pydantic import BaseModel, Field, field_validator
from pydantic.alias_generators import to_camel

from utils.DataStructures import \
    FilterOperator

# Configuration classes
class Parameters(BaseModel):
    debug: bool
    run_all: bool
    remove_old_keys: bool
    raw_db: str
    raw_table_state: str
    raw_table_key: str

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
        return dm.ViewId(space=self.schema_space, external_id=self.external_id, version=self.version)

    def as_property_ref(self, property) -> list[str]:
        return [self.schema_space, f"{self.external_id}/{self.version}", property]

class FilterConfig(BaseModel):
    values: Optional[list[str] | str] = None # Maybe we add a 'match status' here?
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


class SourceViewConfig(BaseModel):
    """
    A class to define the configuration and scope for querying data views.
    """
    # 1. Mandatory Fields
    view_external_id: str = Field(..., description="External ID of the data model view (e.g., 'txEquipment').")
    view_space: str = Field(...,
                            description="Space where the view is defined (e.g., 'sp_enterprise_process_industry').")
    view_version: str = Field(..., description="Version of the view schema (e.g., 'v1').")
    entity_type: EntityType = Field(..., description="Type of entity for processing context (e.g., DataType.ASSET).")
    batch_size: int = Field(..., description="Number of entities to process per batch (e.g., 100).")

    # 2. Optional Fields with Defaults/Mutable Defaults
    filters: Optional[List[FilterConfig]] = Field(
        None, alias="filter",  # Use alias if input JSON/YAML uses 'filter' instead of 'filters'
        description="CDF DMS filter to limit query scope (optional)."
    )

    # FIX: Use default_factory for the mutable default (List)
    include_properties: List[str] = Field(
        default_factory=[],
        description="List of properties to retrieve (optional, e.g., ['name', 'description']).",
    )

    def as_view_id(self) -> ViewId:
        return ViewId(
            schema_space=self.view_space,
            external_id=self.view_external_id,
            version=self.view_version
        )

    def build_filter(self) -> Filter:
        target_view = ViewPropertyConfig(
            schema_space=self.view_space,
            instance_space=self.view_space,
            external_id=self.view_external_id,
            version=self.view_version,
            search_property=""
        )

        list_filters: list[Filter] = [f.as_filter(target_view) for f in self.filters]

        if len(list_filters) == 1:
            return list_filters[0]
        else:
            return dm.filters.And(*list_filters)  # NOTE: '*' Unpacks each filter in the list

# Union for dynamic parameter object in ExtractionRuleConfig
ExtractionMethod = Union[
    FixedWidthMethodParameter,
    HeuristicMethodParameter,
    RegexMethodParameter,
    TokenReassemblyMethodParameter
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
    method: Literal[
        "regex",
        "fixed width",
        "token reassembly",
        "heuristic"
    ]

    # This field MUST be renamed to hold all the method-specific parameters.
    # Pydantic will perform the dynamic dispatch using the 'method' field as the tag.
    method_parameters: ExtractionMethod = Field(
        ..., alias='parameters'
        # Optional: Use alias if your input JSON/YAML uses a different field name (e.g., 'parameters')
    )

class ConfigData(BaseModel):
    source_views: List[SourceViewConfig]
    extraction_rules: List[ExtractionRuleConfig]
    field_selection_strategy: Literal["first_match", "highest_priority", "merge_all", "highest_confidence"]

class Config(BaseModel):
    parameters: Parameters
    data: ConfigData

    @classmethod
    def pares_direct_relation(cls, value: Any) -> Any:
        if isinstance(value, dict):
            return dm.DirectRelationReference.load(value)
        return value

def load_config_parameters(client: CogniteClient, function_data: dict[str, Any]) -> Config:
    """Retrieves the configuration parameters from the function data and loads the configuration from CDF."""
    if "ExtractionPipelineExtId" not in function_data:
        raise ValueError("Missing key 'ExtractionPipelineExtId' in input data to the function")

    pipeline_ext_id = function_data["ExtractionPipelineExtId"]
    try:
        raw_config = client.extraction_pipelines.config.retrieve(pipeline_ext_id)
        if raw_config.config is None:
            raise ValueError(f"No config found for extraction pipeline: {pipeline_ext_id!r}")
    except CogniteAPIError:
        raise RuntimeError(f"Not able to retrieve pipeline config for extraction pipeline: {pipeline_ext_id!r}")

    return Config.model_validate(yaml.safe_load(raw_config.config))