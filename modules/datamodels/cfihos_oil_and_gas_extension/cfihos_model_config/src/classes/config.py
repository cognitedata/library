from pathlib import Path

from cognite.client.data_classes.data_modeling import ViewId
from pydantic import BaseModel, ConfigDict, Field


class ConfigCFIHOS(BaseModel):
    """Configuration class for the CFIHOS extension."""

    model_config = ConfigDict(extra="ignore", frozen=True)
    include: bool = Field(alias="include", default=False)
    source: str | Path = Field(alias="source_input")
    filter_: dict | None = Field(alias="filter", default=None)
    implements: list[ViewId] = Field(default_factory=list)
    use_presence: bool = Field(alias="use_presence", default=False)
    constraints: dict | None = None
    view_filter: dict[str, str | dict[str, str]] | None = None


class Config(BaseModel):
    """Configuration class for the CDM extension."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    space_name: str
    data_model_name: str
    data_model_version: str
    data_model_external_id: str
    data_model_description: str | None = None
    cfihos: ConfigCFIHOS
