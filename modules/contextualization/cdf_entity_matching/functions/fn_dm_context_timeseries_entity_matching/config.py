
import yaml
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.exceptions import CogniteAPIError
from pipeline_types import FunctionInputData
from pydantic import BaseModel, Field, field_validator
from pydantic.alias_generators import to_camel


# Configuration classes
class Parameters(BaseModel, alias_generator=to_camel):
    debug: bool
    dm_update: bool
    run_all: bool
    remove_old_links: bool
    raw_db: str
    raw_table_state: str
    raw_table_ctx_good: str
    raw_table_ctx_bad: str
    raw_table_ctx_manual: str | None = None
    raw_table_ctx_rule: str | None = None
    auto_approval_threshold: float = Field(gt=0.0, le=1.0)


class ViewPropertyConfig(BaseModel, alias_generator=to_camel):
    schema_space: str
    # Configured as "instanceSpace", either one space or a list of them, and normalised to
    # a list here so callers never have to tell the two apart.
    instance_spaces: list[str] = Field(alias="instanceSpace", min_length=1)
    external_id: str
    version: str
    search_property: str = "alias"
    filter_property: str | None = None
    filter_values: list[str] | None = None

    @field_validator("instance_spaces", mode="before")
    @classmethod
    def wrap_single_space(cls, value: object) -> object:
        return [value] if isinstance(value, str) else value

    @property
    def default_instance_space(self) -> str:
        """Space to write an instance to when its own space is unknown - the first configured space."""
        return self.instance_spaces[0]

    def as_view_id(self) -> dm.ViewId:
        return dm.ViewId(space=self.schema_space, external_id=self.external_id, version=self.version)

    def as_property_ref(self, property_name: str) -> list[str]:
        return [self.schema_space, f"{self.external_id}/{self.version}", property_name]

class ConfigData(BaseModel, alias_generator=to_camel):
    entity_view: ViewPropertyConfig
    target_view: ViewPropertyConfig

class Config(BaseModel, alias_generator=to_camel):
    parameters: Parameters
    data: ConfigData

    @classmethod
    def pares_direct_relation(cls, value: object) -> object:
        if isinstance(value, dict):
            return dm.DirectRelationReference.load(value)
        return value


def load_config_parameters(client: CogniteClient, function_data: FunctionInputData) -> Config:
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
