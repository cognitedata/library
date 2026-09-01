
from typing import Any

import yaml
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.exceptions import CogniteAPIError
from pydantic import BaseModel, field_validator
from pydantic.alias_generators import to_camel


# Configuration classes
class Parameters(BaseModel, alias_generator=to_camel):
    debug: bool
    run_all: bool
    raw_db: str
    raw_table_state: str
    debug_timeseries: str | None = None
    update_all: bool = False
    view_filter_property: str = "tags"


class ViewPropertyConfig(BaseModel, alias_generator=to_camel):
    schema_space: str
    instance_space: str | list[str]
    external_id: str
    version: str

    @field_validator("instance_space")
    @classmethod
    def validate_instance_space(cls, value: str | list[str]) -> str | list[str]:
        if isinstance(value, list) and not value:
            raise ValueError("instanceSpace must name at least one space")
        return value

    @property
    def instance_spaces(self) -> list[str]:
        """All configured instance spaces, as a list even when a single space is configured."""
        return [self.instance_space] if isinstance(self.instance_space, str) else list(self.instance_space)

    def as_view_id(self) -> dm.ViewId:
        return dm.ViewId(space=self.schema_space, external_id=self.external_id, version=self.version)

    def as_property_ref(self, property) -> list[str]:
        return [self.schema_space, f"{self.external_id}/{self.version}", property]


class JobConfig(BaseModel, alias_generator=to_camel):
    timeseries_view: ViewPropertyConfig
    asset_view: ViewPropertyConfig

class ConfigData(BaseModel, alias_generator=to_camel):
    job: JobConfig

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
        raw_config = client.extraction_pipelines.config.retrieve(external_id=pipeline_ext_id)
        if raw_config.config is None:
            raise ValueError(f"No config found for extraction pipeline: {pipeline_ext_id!r}")
    except CogniteAPIError as e:
        raise RuntimeError(f"Not able to retrieve pipeline config for extraction pipeline: {pipeline_ext_id!r}") from e

    return Config.model_validate(yaml.safe_load(raw_config.config))
