"""
Configuration models for the AI Property Extractor.

Uses Pydantic for validation and parsing of extraction pipeline configuration.
"""

import json
from enum import Enum
from typing import Any, Dict, List, Optional, Union

import yaml
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client.exceptions import CogniteAPIError
from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic.alias_generators import to_camel


class WriteMode(str, Enum):
    """
    Write mode for property extraction.
    
    - add_new_only: Only write if target property is empty (default, safest)
    - append: Append new values to existing lists (deduplicates)
    - overwrite: Always overwrite with new value
    """
    ADD_NEW_ONLY = "add_new_only"
    APPEND = "append"
    OVERWRITE = "overwrite"


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


class PropertyConfig(BaseModel, alias_generator=to_camel):
    """
    Configuration for a single property extraction.
    
    Supports per-property write modes and optional target property mapping.
    """
    property: str = Field(
        ...,
        description="The source property ID to extract from the view"
    )
    target_property: Optional[str] = Field(
        default=None,
        description="Target property ID to write to. Defaults to source property if not specified."
    )
    write_mode: WriteMode = Field(
        default=WriteMode.ADD_NEW_ONLY,
        description="How to handle existing values: add_new_only, append, or overwrite"
    )
    
    def get_target_property(self) -> str:
        """Get the target property, defaulting to source property if not specified."""
        return self.target_property or self.property


class StateStoreConfig(BaseModel, alias_generator=to_camel):
    """
    Configuration for the state store.
    
    The state store uses CDF RAW to track extraction progress via a cursor.
    This enables efficient incremental processing of large datasets.
    """
    enabled: bool = Field(
        default=True,
        description="Whether to enable state tracking"
    )
    raw_database: str = Field(
        default="ai_extractor_state",
        description="RAW database name for state storage"
    )
    raw_table: str = Field(
        default="extraction_state",
        description="RAW table name for state storage"
    )
    config_version: Optional[str] = Field(
        default=None,
        description="Config version string. Changing this triggers a full re-run."
    )


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
    """
    Configuration for property extraction.
    
    Supports two configuration styles:
    1. New style (recommended): Use 'properties' list with per-property write modes
    2. Legacy style: Use 'propertiesToExtract' list and 'aiPropertyMapping' dict
    
    If 'properties' is provided, it takes precedence over legacy fields.
    """
    text_property: str = Field(
        ..., 
        description="The property containing text to extract from"
    )
    
    # New-style per-property configuration with write modes
    properties: Optional[List[PropertyConfig]] = Field(
        default=None,
        description="List of property configurations with per-property write modes"
    )
    
    # Legacy fields (maintained for backward compatibility)
    properties_to_extract: Optional[List[str]] = Field(
        default=None,
        description="[Legacy] List of property IDs to extract. Use 'properties' instead for write mode support."
    )
    ai_property_mapping: Optional[Dict[str, str]] = Field(
        default=None,
        description="[Legacy] Mapping from source property to target property. Use 'properties' instead."
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
    
    @field_validator('properties', mode='before')
    @classmethod
    def parse_properties(cls, v):
        """Parse JSON string to list of property configs."""
        return parse_json_string(v, list)
    
    def get_property_configs(self) -> List[PropertyConfig]:
        """
        Get list of property configurations.
        
        Converts legacy format to PropertyConfig objects if needed.
        Returns empty list if no properties are configured.
        """
        # If new-style properties are configured, use them
        if self.properties:
            return self.properties
        
        # Convert legacy format to PropertyConfig objects
        if self.properties_to_extract:
            ai_mapping = self.ai_property_mapping or {}
            return [
                PropertyConfig(
                    property=prop,
                    target_property=ai_mapping.get(prop),
                    write_mode=WriteMode.ADD_NEW_ONLY  # Legacy behavior
                )
                for prop in self.properties_to_extract
            ]
        
        return []
    
    def get_properties_to_extract(self) -> Optional[List[str]]:
        """Get list of source property IDs to extract (for backward compatibility)."""
        if self.properties:
            return [p.property for p in self.properties]
        return self.properties_to_extract
    
    def get_ai_property_mapping(self) -> Optional[Dict[str, str]]:
        """Get source->target property mapping (for backward compatibility)."""
        if self.properties:
            mapping = {}
            for p in self.properties:
                if p.target_property and p.target_property != p.property:
                    mapping[p.property] = p.target_property
            return mapping if mapping else None
        return self.ai_property_mapping


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
    state_store: StateStoreConfig = Field(default_factory=StateStoreConfig)


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


