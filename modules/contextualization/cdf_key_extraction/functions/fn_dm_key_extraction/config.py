from __future__ import annotations
from enum import Enum

from dataclasses import dataclass
from typing import Any, Literal, cast, Optional, List

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
class Parameters(BaseModel):
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

class SourceView:
    """
    A class to define the configuration and scope for querying data views.
    """
    def __init__(
        self,
        view_external_id: str,
        view_space: str,
        view_version: str,
        entity_type: EntityType,
        batch_size: int,
        filters: Optional[List[FilterConfig]] = None,
        include_properties: Optional[List[str]] = None
    ):
        """
        Initializes the SourceViews configuration.

        :param view_external_id: External ID of the data model view (e.g., "txEquipment").
        :param view_space: Space where the view is defined (e.g., "sp_enterprise_process_industry").
        :param view_version: Version of the view schema (e.g., "v1").
        :param entity_type: Type of entity for processing context (e.g., DataType.ASSET).
        :param batch_size: Number of entities to process per batch (e.g., 100).
        :param filter: CDF DMS filter to limit query scope (optional).
        :param include_properties: List of properties to retrieve (optional, e.g., ["name", "description"]).
        """
        self.view_external_id = view_external_id
        self.view_space = view_space
        self.view_version = view_version
        self.entity_type = entity_type
        self.batch_size = batch_size
        self.filters = filters
        self.include_properties = include_properties if include_properties is not None else []

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

class FieldRole(Enum):
    """Defines the role of the field in the data extraction process."""
    TARGET = "target"
    CONTEXT = "context"
    VALIDATION = "validation"

class SourceFieldParameter:
    """
    A class to define the configuration parameters for a single source field
    during data extraction.
    """
    def __init__(
        self,
        field_name: str,
        field_type: str,
        required: bool,
        priority: int,
        role: FieldRole,
        separator: Optional[str] = None,
        max_length: Optional[int] = None,
        preprocessing: Optional[List[str]] = None
    ):
        """
        Initializes the SourceFieldParameters configuration.

        :param field_name: Name or path to the metadata field (e.g., "description", "metadata.tagIds").
        :param field_type: Data type of the field (e.g., "string", "array", "object").
        :param required: Whether the field must exist (skip entity if missing) (e.g., false).
        :param priority: Order of precedence when multiple fields match (e.g., 1).
        :param role: Role in extraction: "target", "context", "validation" (e.g., FieldRole.TARGET).
        :param separator: Delimiter for list-type fields (optional, e.g., ",", ";", "|").
        :param max_length: Maximum field length to process (performance) (optional, e.g., 1000).
        :param preprocessing: Preprocessing steps before extraction (optional, e.g., ["trim", "lowercase"]).
        """
        self.field_name = field_name
        self.field_type = field_type
        self.required = required
        self.priority = priority
        self.separator = separator
        self.role = role
        self.max_length = max_length
        self.preprocessing = preprocessing if preprocessing is not None else []

class ConfigData(BaseModel, alias_generator=to_camel):
    source_view: SourceView
    source_field_parameters: List[SourceFieldParameter]
    field_selection_strategy: Literal["first_match", "highest_priority", "merge_all", "highest_confidence"]

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