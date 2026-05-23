"""
Configuration models for create annotations.

This module provides Pydantic models for validating and loading
extraction pipeline configuration.
"""

from typing import Any, Dict, Optional

import yaml
from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError
from pydantic import BaseModel, Field


class Parameters(BaseModel):
    """Parameters for create annotations pipeline."""

    debug: bool = Field(False, description="Enable debug mode")
    raw_db: str = Field(..., description="ID of the RAW database")
    raw_table_results: str = Field(
        ...,
        description="ID of the results table in RAW (source for diagram detection results)",
    )
    raw_table_annotations: str = Field(
        ...,
        description="ID of the annotations table in RAW for storing CogniteDiagramAnnotations",
    )
    logLevel: str = Field("INFO", description="Log level (DEBUG, INFO, WARNING, ERROR)")


class Config(BaseModel):
    """Configuration model for create annotations extraction pipeline."""

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
    client: CogniteClient, function_data: Dict[str, Any]
) -> Config:
    """Retrieves the configuration parameters from the function data and loads the configuration from CDF."""
    if "ExtractionPipelineExtId" not in function_data:
        raise ValueError(
            "Missing key 'ExtractionPipelineExtId' in input data to the function"
        )

    pipeline_ext_id = function_data["ExtractionPipelineExtId"]
    try:
        raw_config = client.extraction_pipelines.config.retrieve(pipeline_ext_id)
        if raw_config.config is None:
            raise ValueError(
                f"No config found for extraction pipeline: {pipeline_ext_id!r}"
            )
    except CogniteAPIError:
        raise RuntimeError(
            f"Not able to retrieve pipeline config for extraction pipeline: {pipeline_ext_id!r}"
        )

    config_dict = yaml.safe_load(raw_config.config)
    config_dict["externalId"] = pipeline_ext_id
    return Config.model_validate(config_dict)
