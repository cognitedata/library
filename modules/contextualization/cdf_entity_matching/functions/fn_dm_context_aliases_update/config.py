
import re
from typing import Any, Literal

import yaml
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.exceptions import CogniteAPIError
from constants import DEFAULT_ALIAS_PATTERN
from pydantic import BaseModel, Field, field_validator
from pydantic.alias_generators import to_camel


# Configuration classes
class Parameters(BaseModel, alias_generator=to_camel):
    debug: bool
    run_all: bool
    raw_db: str
    raw_table_state: str
    update_all: bool = False


class ViewPropertyConfig(BaseModel, alias_generator=to_camel):
    schema_space: str
    # Configured as "instanceSpace", either one space or a list of them, and normalised to
    # a list here so callers never have to tell the two apart.
    instance_spaces: list[str] = Field(alias="instanceSpace", min_length=1)
    external_id: str
    version: str
    # Configured as "aliasPattern", one regular expression or a list of them, normalised
    # to a list here. Each pattern finds the tag inside a name; the alias it yields is
    # that pattern's capture groups joined by "_", so the groups decide the alias rather
    # than the whole match.
    alias_patterns: list[str] = Field(
        alias="aliasPattern",
        default_factory=lambda: [DEFAULT_ALIAS_PATTERN],
        min_length=1,
    )
    # What to keep when several patterns match one name: every alias they yield, or only
    # the longest - the most specific reading of the name.
    alias_selection: Literal["all", "longest"] = "all"

    @field_validator("instance_spaces", mode="before")
    @classmethod
    def wrap_single_space(cls, value: object) -> object:
        return [value] if isinstance(value, str) else value

    @field_validator("alias_patterns", mode="before")
    @classmethod
    def wrap_single_pattern(cls, value: object) -> object:
        return [value] if isinstance(value, str) else value

    @field_validator("alias_patterns")
    @classmethod
    def validate_alias_patterns(cls, value: list[str]) -> list[str]:
        for pattern in value:
            try:
                compiled = re.compile(pattern)
            except re.error as e:
                raise ValueError(f"aliasPattern {pattern!r} is not a valid regular expression: {e}") from e
            if not compiled.groups:
                raise ValueError(
                    f"aliasPattern {pattern!r} must have at least one capture group - "
                    "the alias is the groups joined by '_'"
                )
        return value

    def as_view_id(self) -> dm.ViewId:
        return dm.ViewId(space=self.schema_space, external_id=self.external_id, version=self.version)

    def as_property_ref(self, property) -> list[str]:
        return [self.schema_space, f"{self.external_id}/{self.version}", property]


class JobConfig(BaseModel, alias_generator=to_camel):
    timeseries_view: ViewPropertyConfig
    asset_view: ViewPropertyConfig
    # Optional so a configuration written before file support existed still loads; file
    # metadata is then skipped rather than failing the run.
    file_view: ViewPropertyConfig | None = None

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
