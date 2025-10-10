from __future__ import annotations
from enum import Enum

from dataclasses import dataclass
from typing import Any, Literal, cast, Optional

import yaml
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.data_classes.filters import Filter
from cognite.client.exceptions import CogniteAPIError
from pydantic import BaseModel, Field, field_validator
from pydantic.alias_generators import to_camel

from utils.DataStructures import \
    FilterOperator


# Configuration classes
class Parameters(BaseModel, alias_generator=to_camel):
    debug: bool
    run_all: bool
    remove_old_asset_links: bool
    raw_db: str
    raw_table_state: str
    raw_tale_ctx_good: str
    raw_tale_ctx_bad: str
    raw_tale_ctx_manual: str = None
    raw_tale_ctx_rule: str = None
    auto_approval_threshold: float = Field(gt=0.0, le=1.0)

class FilterConfig(BaseModel, alias_generator=to_camel):
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

class ViewPropertyConfig(BaseModel, alias_generator=to_camel):
    schema_space: str
    instance_space: str
    external_id: str
    version: str
    search_property: str = "alias"

    def as_view_id(self) -> dm.ViewId:
        return dm.ViewId(space=self.schema_space, external_id=self.external_id, version=self.version)

    def as_property_ref(self, property) -> list[str]:
        return [self.schema_space, f"{self.external_id}/{self.version}", property]

class ConfigData(BaseModel, alias_generator=to_camel):
    source_query: QueryConfig
    target_query: QueryConfig

class Config(BaseModel, alias_generator=to_camel):
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
