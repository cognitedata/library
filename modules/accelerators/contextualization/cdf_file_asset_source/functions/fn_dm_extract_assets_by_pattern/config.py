"""
Configuration models for extract annotation tags.

This module provides Pydantic models for validating and loading
extraction pipeline configuration.
"""

from typing import Any, Dict, List, Literal, Optional

from typing import TYPE_CHECKING

from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class Parameters(BaseModel):
    """Parameters for extract annotation tags pipeline."""

    debug: bool = Field(False, description="Enable debug mode")
    run_all: bool = Field(True, description="Run all files")
    overwrite: bool = Field(False, description="Overwrite existing results")
    initialize_state: bool = Field(
        False,
        description="Only query for new files and initialize state, skip processing",
    )
    raw_db: str = Field(..., description="ID of the raw database")
    raw_table_state: str = Field(..., description="ID of the state table in RAW")
    raw_table_results: Optional[str] = Field(
        None,
        description="ID of the results table in RAW (optional, defaults to state table)",
    )
    results_field: str = Field(
        "results", description="Field name for results in the state/results table"
    )


class Config(BaseModel):
    """Configuration model for extract annotation tags extraction pipeline."""

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

    return resolve_cdf_config(
        function_data, "extract", Config, client=client
    )
