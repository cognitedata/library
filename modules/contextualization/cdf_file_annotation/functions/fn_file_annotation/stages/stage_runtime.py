"""Shared helpers for stage entrypoints."""

from typing import Literal

from cognite.client.exceptions import CogniteAPIError
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

# Logged, then re-raised so the CDF function call fails and the workflow stops.
# ValidationError is included because Pydantic v2 does not inherit it from ValueError.
STAGE_REPORTABLE_ERRORS: tuple[type[BaseException], ...] = (
    CogniteAPIError,
    ValueError,
    RuntimeError,
    ValidationError,
)

LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR"]
StageName = Literal["prepare", "launch", "finalize", "promote"]


class StageInput(BaseModel):
    """The function input a stage reads."""

    model_config = ConfigDict(populate_by_name=True)

    extraction_pipeline_ext_id: str = Field(alias="ExtractionPipelineExtId", min_length=1)
    log_level: LogLevel = Field(default="INFO", alias="logLevel")
    log_path: str | None = Field(default=None, alias="logPath")

    @field_validator("log_level", mode="before")
    @classmethod
    def _upper_log_level(cls, value: object) -> object:
        return value.upper() if isinstance(value, str) else value


class FunctionInput(StageInput):
    """The function input the handler dispatches on."""

    stage: StageName
