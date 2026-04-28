"""Pydantic model for passthrough extraction method (no pattern or parsing)."""

from typing import Literal

from pydantic import BaseModel, Field


class PassthroughMethodParameter(BaseModel):
    """Parameters for passthrough extraction: use entire field value as key."""

    method: Literal["passthrough"] = "passthrough"
    min_confidence: float = Field(
        1.0,
        description="Confidence for the emitted key (default 1.0).",
    )
