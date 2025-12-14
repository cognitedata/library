"""
Configuration models for the AI Property Extractor.

Uses Pydantic for validation and parsing of extraction pipeline configuration.
"""

from typing import Any, Dict, List, Optional

import yaml
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.exceptions import CogniteAPIError
from pydantic import BaseModel, Field
from pydantic.alias_generators import to_camel


class AgentConfig(BaseModel, alias_generator=to_camel):
    """Configuration for the LLM agent."""
    external_id: str


class ViewConfig(BaseModel, alias_generator=to_camel):
    """Configuration for the data modeling view."""
    space: str
    external_id: str
    version: str = "v1"

    def as_view_id(self) -> dm.ViewId:
        return dm.ViewId(
            space=self.space, 
            external_id=self.external_id, 
            version=self.version
        )


class ExtractionConfig(BaseModel, alias_generator=to_camel):
    """Configuration for property extraction."""
    text_property: str = Field(
        ..., 
        description="The property containing text to extract from"
    )
    properties_to_extract: Optional[List[str]] = Field(
        default=None,
        description="List of property IDs to extract. If None, all non-filled properties are extracted."
    )
    ai_property_mapping: Optional[Dict[str, str]] = Field(
        default=None,
        description="Mapping from source property to target property (e.g., {'description': 'ai_description'})"
    )


class ProcessingConfig(BaseModel, alias_generator=to_camel):
    """Configuration for processing behavior."""
    batch_size: int = Field(
        default=10, 
        ge=1, 
        le=100,
        description="Number of instances to process per batch"
    )
    filters: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Optional filters for instance selection"
    )


class Config(BaseModel, alias_generator=to_camel):
    """Root configuration model for the AI Property Extractor."""
    agent: AgentConfig
    view: ViewConfig
    extraction: ExtractionConfig
    processing: ProcessingConfig = Field(default_factory=ProcessingConfig)


def load_config(client: CogniteClient, function_data: Dict[str, Any], logger=None) -> Config:
    """
    Load configuration from the extraction pipeline.
    
    Args:
        client: Authenticated CogniteClient
        function_data: Function input data containing ExtractionPipelineExtId
        logger: Optional logger instance
    
    Returns:
        Parsed Config object
    
    Raises:
        ValueError: If ExtractionPipelineExtId is missing or config is empty
        RuntimeError: If unable to retrieve pipeline config from CDF
    """
    if "ExtractionPipelineExtId" not in function_data:
        raise ValueError("Missing key 'ExtractionPipelineExtId' in input data to the function")

    pipeline_ext_id = function_data["ExtractionPipelineExtId"]
    
    try:
        raw_config = client.extraction_pipelines.config.retrieve(pipeline_ext_id)
        if raw_config.config is None:
            raise ValueError(f"No config found for extraction pipeline: {pipeline_ext_id!r}")
    except CogniteAPIError as e:
        raise RuntimeError(
            f"Unable to retrieve pipeline config for extraction pipeline: {pipeline_ext_id!r}. Error: {e}"
        )

    if logger:
        logger.debug(f"Raw config: {raw_config.config}")

    config_dict = yaml.safe_load(raw_config.config)
    return Config.model_validate(config_dict)


