"""
Configuration models for the AI Property Extractor.

Uses Pydantic for validation and parsing of extraction pipeline configuration.
"""

import json
from typing import Any, Dict, List, Optional

import yaml
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.exceptions import CogniteAPIError
from pydantic import BaseModel, Field, field_validator
from pydantic.alias_generators import to_camel


def parse_json_string(v: Any, expected_type: type) -> Any:
    """
    Parse a JSON string to dict/list, or return value as-is if already parsed.
    Also handles empty values by returning None.
    
    Args:
        v: The value to parse (could be str, dict, list, or None)
        expected_type: Expected type (dict or list)
    
    Returns:
        Parsed value or None if empty
    """
    if v is None:
        return None
    
    # If it's a string, try to parse as JSON
    if isinstance(v, str):
        v = v.strip()
        if not v:
            return None
        try:
            v = json.loads(v)
        except json.JSONDecodeError:
            # If it can't be parsed, return None for safety
            return None
    
    # Normalize empty collections to None
    if isinstance(v, (list, dict)) and len(v) == 0:
        return None
    
    return v


# Default prompt template used when none is configured
DEFAULT_PROMPT_TEMPLATE = """You are an expert data analyst. You will receive a free text. Your task is to extract the relevant values for the following structured properties, as best as possible, from that text.

For each property, you will be given:
- externalId: A unique identifier for the property.
- name: The display name.
- description: A detailed explanation of what should be filled into this property.

For each property, return the best-matching value you can extract from the text, or null if no relevant information is found. Output a dictionary in JSON with property externalId as key and the extracted value (or null) as value.
{custom_instructions}

Here is the text to analyze:
{text}

Here are the properties to fill:
{properties}

Remember:
- Return only parsable JSON with property externalId keys.
- Use null for missing fields.
- If a property is a list, return a JSON array.

Example output:
{{
  "Property_XYZ": "value 1",
  "Property_ABC": null,
  "Property_List": ["value1", "value2"]
}}"""


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
    
    @field_validator('properties_to_extract', mode='before')
    @classmethod
    def parse_properties_to_extract(cls, v):
        """Parse JSON string to list, handle empty values."""
        return parse_json_string(v, list)
    
    @field_validator('ai_property_mapping', mode='before')
    @classmethod
    def parse_ai_property_mapping(cls, v):
        """Parse JSON string to dict, handle empty values."""
        return parse_json_string(v, dict)


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
    
    @field_validator('filters', mode='before')
    @classmethod
    def parse_filters(cls, v):
        """Parse JSON string to list, handle empty values."""
        return parse_json_string(v, list)


class PromptConfig(BaseModel, alias_generator=to_camel):
    """Configuration for the LLM prompt."""
    custom_instructions: Optional[str] = Field(
        default=None,
        description="Additional instructions appended to the base prompt to customize LLM behavior"
    )
    template: Optional[str] = Field(
        default=None,
        description="Custom prompt template. Available placeholders: {text}, {properties}, {custom_instructions}"
    )
    
    def get_template(self) -> str:
        """Get the prompt template, falling back to default if not configured."""
        if self.template and self.template.strip():
            return self.template
        return DEFAULT_PROMPT_TEMPLATE
    
    def get_custom_instructions(self) -> str:
        """Get custom instructions, returning empty string if not configured."""
        if self.custom_instructions and self.custom_instructions.strip():
            return f"\n\n{self.custom_instructions}"
        return ""


class Config(BaseModel, alias_generator=to_camel):
    """Root configuration model for the AI Property Extractor."""
    agent: AgentConfig
    view: ViewConfig
    extraction: ExtractionConfig
    processing: ProcessingConfig = Field(default_factory=ProcessingConfig)
    prompt: PromptConfig = Field(default_factory=PromptConfig)


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


