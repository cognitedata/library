"""
Configuration models for write asset hierarchy.

This module provides Pydantic models for validating and loading
extraction pipeline configuration.
"""

from typing import Any, Dict, Optional

from typing import TYPE_CHECKING

from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class Parameters(BaseModel):
    """Parameters for write asset hierarchy pipeline."""

    debug: bool = Field(False, description="Enable debug mode")
    run_all: bool = Field(True, description="Run all files")
    overwrite: bool = Field(False, description="Overwrite existing results")
    raw_db: str = Field(..., description="ID of the RAW database")
    raw_table_assets: str = Field(..., description="ID of the assets table in RAW")


class Config(BaseModel):
    """Configuration model for write asset hierarchy extraction pipeline."""

    externalId: str
    config: Dict[str, Any]

    @property
    def parameters(self) -> Parameters:
        """Get parameters from config."""
        params = self.config.get("parameters", {})
        return Parameters.model_validate(params)

    @property
    def data(self) -> Dict[str, Any]:
        """Get data section from config."""
        return self.config.get("data", {})


def load_config_parameters(
    client: "CogniteClient | None", function_data: Dict[str, Any]
) -> Config:
    """Load step config from workflow ``configuration`` + ``step`` or ``default.config.yaml``."""
    from functions.shared.utils.module_config import resolve_cdf_config

    return resolve_cdf_config(function_data, "write", Config, client=client)
