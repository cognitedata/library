"""
Configuration models for create asset hierarchy.

This module provides Pydantic models for validating and loading
extraction pipeline configuration.
"""

from typing import Any, Dict, Optional

from typing import TYPE_CHECKING

from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from cognite.client import CogniteClient


class Parameters(BaseModel):
    """Parameters for create asset hierarchy pipeline."""

    debug: bool = Field(False, description="Enable debug mode")
    run_all: bool = Field(True, description="Run all files")
    overwrite: bool = Field(False, description="Overwrite existing results")
    raw_db: str = Field(..., description="ID of the RAW database")
    raw_table_state: str = Field(
        ..., description="ID of the state table in RAW (contains results)"
    )
    results_field: str = Field(
        "results", description="Field name for results in state table"
    )
    raw_table_assets: str = Field(
        ..., description="ID of the assets table in RAW for storing generated hierarchy"
    )
    output_file: Optional[str] = Field(
        None,
        description="Output file path for asset hierarchy YAML (only used when running locally)",
    )
    space: str = Field("sp_enterprise_schema", description="Instance space for assets")
    include_resource_type: bool = Field(
        False, description="Include resourceType (tag_class_name) as intermediate level"
    )
    include_resource_subtype: bool = Field(
        False,
        description="Include resourceSubType (equipment_class_name) as intermediate level",
    )
    include_resource_subsubtype: bool = Field(
        False,
        description="Include resourceSubSubType (equipment_subclass_name) as intermediate level",
    )
    include_resource_variant: bool = Field(
        False,
        description="Include resourceVariant (equipment_variant_name) as intermediate level",
    )
    logLevel: str = Field("INFO", description="Log level (DEBUG, INFO, WARNING, ERROR)")
    limit: Optional[int] = Field(
        -1, description="Maximum number of files to process (-1 for no limit)"
    )
    batch_size: Optional[int] = Field(
        None, description="Number of files to process per batch (None for no batching)"
    )
    pattern_config_path: Optional[str] = Field(
        None,
        description="Optional asset tag classifier YAML path (relative to module root)",
    )


class Config(BaseModel):
    """Configuration model for create asset hierarchy extraction pipeline."""

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

    return resolve_cdf_config(function_data, "create", Config, client=client)
