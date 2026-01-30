"""
LLM Property Extractor for Cognite Data Fusion

This module provides functionality to extract structured property values from unstructured text
using LLM agents in Cognite Data Fusion. It's designed to work with data modeling views and 
instances, automatically filling in missing properties based on text content.

Main Features:
- Extract structured data from free text using LLM
- Automatic type coercion for different property types
- Support for both single values and lists
- AI property mapping: write AI-generated values to separate properties (e.g., description -> ai_description)
- Write modes: add_new_only (default), append (for lists), overwrite
"""

import json
from typing import Optional, List, Dict, Any

from cognite.client import CogniteClient
from cognite.client.data_classes.agents import Message
from cognite.client.data_classes.data_modeling import View, NodeApply, NodeOrEdgeData
import cognite.client.data_classes.data_modeling.data_types as dtypes
from cognite.client.data_classes.data_modeling.views import MultiReverseDirectRelation, SingleReverseDirectRelation

from config import PropertyConfig, WriteMode


class LLMPropertyExtractor:
    """
    Extract structured properties from text using LLM agents.
    
    This class handles the entire pipeline of:
    1. Analyzing text with LLM to extract property values
    2. Type coercion to match view property types
    3. Creating NodeApply objects ready for CDF ingestion
    
    Supports AI property mapping to write AI-generated values to separate properties,
    keeping source system values separate from AI-generated values.
    """
    
    # Property types that cannot be set directly
    REVERSE_RELATION_TYPES = (MultiReverseDirectRelation, SingleReverseDirectRelation)
    
    # Default prompt template used when none is configured (single instance)
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

    # Default prompt template for batch processing (multiple instances)
    DEFAULT_BATCH_PROMPT_TEMPLATE = """You are an expert data analyst. You will receive multiple items, each containing an ID and a free text. Your task is to extract the relevant values for the following structured properties from each item's text.

**Individual approach**: Each item should be analyzed individually and independently - do not mix information between items.

For each property, you will be given:
- externalId: A unique identifier for the property.
- name: The display name.
- description: A detailed explanation of what should be filled into this property.

For each item, return a JSON object with the item's ID and the extracted property values.
{custom_instructions}

Here are the items to analyze:
{items}

Here are the properties to fill for EACH item:
{properties}

Remember:
- Return a JSON array with one object per item.
- Each object must have "id" (matching the item ID) and the property externalIds as keys.
- Use null for missing fields.
- If a property is a list, return a JSON array.

Example output for 2 items:
[
  {{
    "id": "item_1",
    "Property_XYZ": "value 1",
    "Property_ABC": null,
    "Property_List": ["value1", "value2"]
  }},
  {{
    "id": "item_2",
    "Property_XYZ": "value 2",
    "Property_ABC": "some value",
    "Property_List": []
  }}
]"""
    
    def __init__(
        self, 
        client: CogniteClient, 
        agent_external_id: str,
        logger=None,
        custom_prompt_instructions: Optional[str] = None,
        prompt_template: Optional[str] = None
    ):
        """
        Initialize the LLM Property Extractor.
        
        Args:
            client: Authenticated CogniteClient instance
            agent_external_id: External ID of the agent to use for text parsing
            logger: Optional logger instance
            custom_prompt_instructions: Optional custom instructions to add to the prompt
            prompt_template: Optional custom prompt template with placeholders {text}, {properties}, {custom_instructions}
        """
        self.client = client
        self.agent_external_id = agent_external_id
        self.custom_prompt_instructions = custom_prompt_instructions or ""
        self.prompt_template = prompt_template if prompt_template and prompt_template.strip() else self.DEFAULT_PROMPT_TEMPLATE
        self.logger = logger
        self._agent = None
    
    def _log(self, level: str, message: str) -> None:
        """Log a message if logger is available."""
        if self.logger:
            getattr(self.logger, level.lower(), self.logger.info)(message)
    
    @property
    def agent(self):
        """Lazy load and cache the agent using retrieve by external_id."""
        if self._agent is None:
            self._log("debug", f"Retrieving agent: {self.agent_external_id}")
            self._agent = self.client.agents.retrieve(self.agent_external_id)
            if self._agent is None:
                raise ValueError(f"Agent with external_id '{self.agent_external_id}' not found")
        return self._agent
    
    def _build_prompt(self, text: str, properties_to_fill: Dict[str, Dict[str, str]]) -> str:
        """
        Build the LLM prompt for property extraction using the configured template.
        
        Args:
            text: The text to extract properties from
            properties_to_fill: Dictionary mapping property external_id to {name, description}
        
        Returns:
            Formatted prompt string
        """
        # Format custom instructions (add newlines if present)
        custom_instructions = ""
        if self.custom_prompt_instructions and self.custom_prompt_instructions.strip():
            custom_instructions = f"\n\n{self.custom_prompt_instructions}"
        
        # Build the prompt using the template
        prompt = self.prompt_template.format(
            text=text,
            properties=json.dumps(properties_to_fill, indent=2),
            custom_instructions=custom_instructions
        )
        
        return prompt.strip()
    
    def _build_batch_prompt(
        self, 
        items: List[Dict[str, str]], 
        properties_to_fill: Dict[str, Dict[str, str]],
        batch_prompt_template: Optional[str] = None
    ) -> str:
        """
        Build the LLM prompt for batch property extraction.
        
        Args:
            items: List of dicts with 'id' and 'text' keys for each instance
            properties_to_fill: Dictionary mapping property external_id to {name, description}
            batch_prompt_template: Optional custom batch prompt template
        
        Returns:
            Formatted prompt string for batch processing
        """
        # Format custom instructions (add newlines if present)
        custom_instructions = ""
        if self.custom_prompt_instructions and self.custom_prompt_instructions.strip():
            custom_instructions = f"\n\n{self.custom_prompt_instructions}"
        
        # Use provided template or default batch template
        template = batch_prompt_template if batch_prompt_template else self.DEFAULT_BATCH_PROMPT_TEMPLATE
        
        # Format items for the prompt
        formatted_items = []
        for item in items:
            formatted_items.append({
                "id": item["id"],
                "text": item["text"]
            })
        
        # Build the prompt using the batch template
        prompt = template.format(
            items=json.dumps(formatted_items, indent=2),
            properties=json.dumps(properties_to_fill, indent=2),
            custom_instructions=custom_instructions
        )
        
        return prompt.strip()
    
    def _parse_text_with_llm(self, text: str, properties_to_fill: Dict[str, Dict[str, str]]) -> Dict[str, Any]:
        """
        Parse text using LLM and return extracted properties.
        
        Args:
            text: The text to parse
            properties_to_fill: Dictionary mapping property external_id to {name, description}
        
        Returns:
            Dictionary mapping property external_id to extracted value
        
        Raises:
            ValueError: If the LLM response cannot be parsed as JSON
        """
        prompt = self._build_prompt(text, properties_to_fill)
        self._log("debug", f"Sending prompt to agent (length: {len(prompt)})")
        
        response = self.client.agents.chat(self.agent.external_id, Message(content=prompt))
        returned_json = response.messages[0].content.text
        
        # Try to extract JSON from the response (handle markdown code blocks)
        json_str = self._extract_json_from_response(returned_json)
        
        try:
            return json.loads(json_str)
        except json.JSONDecodeError:
            raise ValueError(f"Failed to parse JSON from agent response: {returned_json}")
    
    def _parse_batch_with_llm(
        self, 
        items: List[Dict[str, str]], 
        properties_to_fill: Dict[str, Dict[str, str]],
        batch_prompt_template: Optional[str] = None
    ) -> Dict[str, Dict[str, Any]]:
        """
        Parse multiple texts using LLM in a single call and return extracted properties for each.
        
        Args:
            items: List of dicts with 'id' and 'text' keys for each instance
            properties_to_fill: Dictionary mapping property external_id to {name, description}
            batch_prompt_template: Optional custom batch prompt template
        
        Returns:
            Dictionary mapping item ID to extracted properties dict
        
        Raises:
            ValueError: If the LLM response cannot be parsed as JSON
        """
        prompt = self._build_batch_prompt(items, properties_to_fill, batch_prompt_template)
        self._log("debug", f"Sending batch prompt to agent (length: {len(prompt)}, items: {len(items)})")
        
        response = self.client.agents.chat(self.agent.external_id, Message(content=prompt))
        returned_json = response.messages[0].content.text
        
        # Try to extract JSON from the response (handle markdown code blocks)
        json_str = self._extract_json_from_response(returned_json)
        
        try:
            parsed_list = json.loads(json_str)
        except json.JSONDecodeError:
            raise ValueError(f"Failed to parse JSON from agent batch response: {returned_json}")
        
        # Convert list response to dict keyed by item ID
        if not isinstance(parsed_list, list):
            raise ValueError(f"Expected list response for batch processing, got: {type(parsed_list)}")
        
        results = {}
        for item_result in parsed_list:
            if not isinstance(item_result, dict):
                self._log("warning", f"Skipping non-dict item in batch response: {item_result}")
                continue
            
            item_id = item_result.get("id")
            if not item_id:
                self._log("warning", f"Skipping item without 'id' in batch response: {item_result}")
                continue
            
            # Extract properties (exclude 'id' from the properties dict)
            properties = {k: v for k, v in item_result.items() if k != "id"}
            results[item_id] = properties
        
        return results
    
    @staticmethod
    def _extract_json_from_response(response: str) -> str:
        """
        Extract JSON from LLM response, handling markdown code blocks.
        
        Args:
            response: Raw LLM response string
        
        Returns:
            Cleaned JSON string
        """
        response = response.strip()
        
        # Handle markdown code blocks
        if response.startswith("```"):
            lines = response.split("\n")
            # Remove first line (```json or ```) and last line (```)
            if lines[-1].strip() == "```":
                lines = lines[1:-1]
            else:
                lines = lines[1:]
            response = "\n".join(lines)
        
        return response.strip()
    
    def _is_reverse_relation(self, prop) -> bool:
        """Check if a property is a reverse relation type."""
        return isinstance(prop, self.REVERSE_RELATION_TYPES)
    
    def _get_property_dict_from_view(
        self, 
        view: View, 
        property_ids: List[str]
    ) -> Dict[str, Dict[str, str]]:
        """
        Extract property metadata from a view.
        
        Args:
            view: The view containing property definitions
            property_ids: List of property external_ids to extract
        
        Returns:
            Dictionary mapping property external_id to {name, description}
        """
        return {
            external_id: {
                "name": prop.name or external_id,
                "description": prop.description or ""
            }
            for external_id, prop in view.properties.items()
            if external_id in property_ids 
            and not self._is_reverse_relation(prop)
        }
    
    def _get_non_filled_properties(
        self, 
        instance, 
        view: View,
        property_ids: Optional[List[str]] = None
    ) -> List[str]:
        """
        Get list of property external_ids that are not filled in an instance.
        Skips reverse relation properties as they cannot be set directly.
        
        Args:
            instance: The instance to check
            view: The view defining the properties
            property_ids: Optional list to filter which properties to check
        
        Returns:
            List of property external_ids that are missing in the instance
        """
        instance_properties = instance.properties.get(view.as_id(), {})
        
        properties_to_check = property_ids if property_ids else list(view.properties.keys())
        
        return [
            prop_id 
            for prop_id in properties_to_check
            if prop_id in view.properties
            and not self._is_reverse_relation(view.properties[prop_id])
            and (prop_id not in instance_properties or instance_properties[prop_id] is None)
        ]
    
    def _coerce_value_to_type(self, value: Any, prop_type: type, prop_is_list: bool) -> Any:
        """
        Coerce a value to match the expected property type.
        
        Args:
            value: The value to coerce
            prop_type: The target property type class
            prop_is_list: Whether the property is a list type
        
        Returns:
            Coerced value
        """
        def coerce_single_value(val):
            if val is None:
                return None
            
            try:
                if prop_type is dtypes.Text:
                    return str(val)
                elif prop_type in (dtypes.Int32, dtypes.Int64):
                    return int(float(val))  # Handle "123.0" strings
                elif prop_type in (dtypes.Float32, dtypes.Float64):
                    return float(val)
                elif prop_type is dtypes.Boolean:
                    if isinstance(val, str):
                        return val.lower() in ("true", "1", "yes", "ja")
                    return bool(val)
            except (ValueError, TypeError) as e:
                self._log("warning", f"Failed to coerce value '{val}' to {prop_type}: {e}")
                return val
            
            return val  # fallback for unhandled types
        
        if prop_is_list:
            if value is None:
                return []
            elif isinstance(value, str) and value.startswith("[") and value.endswith("]"):
                import ast
                try:
                    value_list = ast.literal_eval(value)
                    return [coerce_single_value(v) for v in value_list]
                except (ValueError, SyntaxError):
                    return [coerce_single_value(value)]
            elif isinstance(value, list):
                return [coerce_single_value(v) for v in value]
            else:  # single value, wrap in list
                return [coerce_single_value(value)]
        else:
            return coerce_single_value(value)
    
    def _apply_write_mode(
        self,
        new_value: Any,
        current_value: Any,
        write_mode: WriteMode,
        prop_is_list: bool
    ) -> tuple[Any, bool]:
        """
        Apply write mode logic to determine the final value to write.
        
        Args:
            new_value: The new value extracted by LLM
            current_value: The current value in the instance (may be None)
            write_mode: The write mode to apply
            prop_is_list: Whether the property is a list type
        
        Returns:
            Tuple of (final_value, should_write):
            - final_value: The value to write (merged, new, or unchanged)
            - should_write: Whether to include this property in the update
        """
        if new_value is None:
            return None, False
        
        if write_mode == WriteMode.OVERWRITE:
            # Always overwrite with new value
            return new_value, True
        
        if write_mode == WriteMode.ADD_NEW_ONLY:
            # Only write if current value is empty/None
            if current_value is None or current_value == "" or current_value == []:
                return new_value, True
            else:
                self._log("debug", f"Skipping (add_new_only): target already has value")
                return current_value, False
        
        if write_mode == WriteMode.APPEND:
            # Append mode - only valid for lists
            if not prop_is_list:
                self._log("warning", f"Append mode not supported for non-list properties, treating as add_new_only")
                if current_value is None or current_value == "":
                    return new_value, True
                else:
                    return current_value, False
            
            # Handle list append with deduplication
            current_list = current_value if isinstance(current_value, list) else []
            new_list = new_value if isinstance(new_value, list) else [new_value]
            
            # Deduplicate: add only items not already in current list
            merged = list(current_list)  # Copy current
            for item in new_list:
                if item not in merged:
                    merged.append(item)
            
            # Only write if there are new items
            if len(merged) > len(current_list):
                return merged, True
            else:
                self._log("debug", f"Skipping (append): no new items to add")
                return current_list, False
        
        # Default fallback (should not reach here)
        return new_value, True
    
    def _create_node_apply(
        self, 
        instance, 
        parsed_dict: Dict[str, Any], 
        source_view: View,
        property_mapping: Optional[Dict[str, str]] = None,
        property_configs: Optional[Dict[str, PropertyConfig]] = None,
        target_view: Optional[View] = None
    ) -> Optional[NodeApply]:
        """
        Create a NodeApply object from parsed properties.
        
        Args:
            instance: The original instance
            parsed_dict: Dictionary of parsed property values (keyed by source property)
            source_view: The view defining the source properties (used for property metadata)
            property_mapping: Optional mapping from source property to target property
                             e.g., {"description": "ai_description"}
            property_configs: Optional dict of PropertyConfig objects keyed by source property
                             Used to apply write modes (append, overwrite, add_new_only)
            target_view: Optional separate view for writing properties. If not specified, 
                        writes to the source view.
        
        Returns:
            NodeApply object ready for CDF ingestion, or None if no properties to update
        """
        new_properties = {}
        property_mapping = property_mapping or {}
        property_configs = property_configs or {}
        
        # Use target view for writing, fall back to source view
        write_view = target_view if target_view else source_view
        
        # Get instance properties from both views for checking current values
        source_instance_properties = instance.properties.get(source_view.as_id(), {})
        target_instance_properties = instance.properties.get(write_view.as_id(), {}) if target_view else source_instance_properties
        
        for source_prop_id, value in parsed_dict.items():
            if value is None:
                continue
            
            # Determine target property (mapped or same as source)
            target_prop_id = property_mapping.get(source_prop_id, source_prop_id)
            
            # Get the target property definition for type coercion from the write view
            if target_prop_id not in write_view.properties:
                self._log("warning", f"Target property '{target_prop_id}' not found in target view, skipping")
                continue
            
            prop_definition = write_view.properties[target_prop_id]
            
            # Skip reverse relations as they can't be set directly
            if self._is_reverse_relation(prop_definition):
                continue
            
            prop_type = type(prop_definition.type)
            prop_is_list = getattr(prop_definition.type, "is_list", False)
            
            # Coerce value to proper type
            coerced_value = self._coerce_value_to_type(value, prop_type, prop_is_list)
            
            # Get current value from target view's instance properties
            current_value = target_instance_properties.get(target_prop_id)
            
            # Get write mode from property config (default to ADD_NEW_ONLY for safety)
            prop_config = property_configs.get(source_prop_id)
            write_mode = prop_config.write_mode if prop_config else WriteMode.ADD_NEW_ONLY
            
            # Apply write mode logic
            final_value, should_write = self._apply_write_mode(
                new_value=coerced_value,
                current_value=current_value,
                write_mode=write_mode,
                prop_is_list=prop_is_list
            )
            
            if should_write:
                new_properties[target_prop_id] = final_value
        
        if not new_properties:
            return None
        
        return NodeApply(
            space=instance.space,
            external_id=instance.external_id,
            sources=[NodeOrEdgeData(write_view.as_id(), properties=new_properties)]
        )
    
    def extract_properties_from_instance(
        self,
        instance,
        view: View,
        text_property: str,
        properties_to_extract: Optional[List[str]] = None,
        ai_property_mapping: Optional[Dict[str, str]] = None,
        property_configs: Optional[List[PropertyConfig]] = None,
        target_view: Optional[View] = None
    ) -> Optional[NodeApply]:
        """
        Extract and fill properties from an instance's text field.
        
        This is the main method to use for extracting properties from a single instance.
        
        Args:
            instance: The CDF instance to process
            view: The source view defining the properties to read from
            text_property: The property name containing the text to parse
            properties_to_extract: Optional list of property IDs to extract. 
                                   If None, all non-filled properties will be extracted.
                                   Only properties in this list will be extracted.
                                   Ignored if property_configs is provided.
            ai_property_mapping: Optional mapping from source property to target property.
                                e.g., {"description": "ai_description"}
                                Ignored if property_configs is provided.
            property_configs: Optional list of PropertyConfig objects with per-property write modes.
                             If provided, takes precedence over properties_to_extract and ai_property_mapping.
            target_view: Optional separate view for writing extracted properties.
                        If not specified, writes to the source view.
        
        Returns:
            NodeApply object with extracted properties, or None if no extraction needed/possible
        """
        # Build property mapping and config dict from PropertyConfig objects
        if property_configs:
            ai_property_mapping = {}
            property_configs_dict = {}
            properties_to_extract = []
            for pc in property_configs:
                properties_to_extract.append(pc.property)
                if pc.target_property and pc.target_property != pc.property:
                    ai_property_mapping[pc.property] = pc.target_property
                property_configs_dict[pc.property] = pc
        else:
            ai_property_mapping = ai_property_mapping or {}
            property_configs_dict = {}
        
        # Get the text to parse (query already filters for text existence)
        instance_properties = instance.properties.get(view.as_id(), {})
        text = instance_properties.get(text_property)
        
        # Use target view for writing, fall back to source view
        write_view = target_view if target_view else view
        
        # Get target view instance properties for checking current values
        target_instance_properties = instance.properties.get(write_view.as_id(), {}) if target_view else instance_properties
        
        # Determine which properties to extract
        if properties_to_extract is not None:
            candidate_properties = properties_to_extract
        else:
            # If not specified, use all non-reverse-relation properties from source view
            candidate_properties = [
                prop_id for prop_id, prop in view.properties.items()
                if not self._is_reverse_relation(prop)
            ]
        
        # For each candidate property, determine:
        # 1. The source property (for LLM extraction metadata)
        # 2. The target property (where to write the value)
        # 3. Whether we should process based on write mode
        
        properties_to_process = []  # Source properties to send to LLM
        property_mapping = {}       # source -> target mapping
        
        for source_prop in candidate_properties:
            # Check that source property exists in source view (for metadata)
            if source_prop not in view.properties:
                self._log("warning", f"Source property '{source_prop}' not found in source view, skipping")
                continue
            
            # Skip if source is a reverse relation
            if self._is_reverse_relation(view.properties[source_prop]):
                continue
            
            # Determine target property (mapped or same as source)
            target_prop = ai_property_mapping.get(source_prop, source_prop)
            
            # Check that target property exists in the write view
            if target_prop not in write_view.properties:
                self._log("warning", f"Target property '{target_prop}' not found in target view, skipping")
                continue
            
            # Check if target property is reverse relation
            if self._is_reverse_relation(write_view.properties[target_prop]):
                self._log("warning", f"Target property '{target_prop}' is a reverse relation, skipping")
                continue
            
            # Get write mode from property config
            prop_config = property_configs_dict.get(source_prop)
            write_mode = prop_config.write_mode if prop_config else WriteMode.ADD_NEW_ONLY
            
            # Determine if we should process this property based on write mode
            # Check current value from target view's instance properties
            current_value = target_instance_properties.get(target_prop)
            has_value = current_value is not None and current_value != "" and current_value != []
            
            if write_mode == WriteMode.ADD_NEW_ONLY and has_value:
                # Skip if target already has value
                self._log("debug", f"Skipping property '{source_prop}' (add_new_only mode, target already filled)")
                continue
            
            # For overwrite and append modes, always process
            # The _apply_write_mode method will handle the merge logic
            
            properties_to_process.append(source_prop)
            property_mapping[source_prop] = target_prop
        
        if not properties_to_process:
            self._log("debug", f"No properties to process for instance {instance.external_id}")
            return None
        
        # Extract property metadata using source property names from source view
        property_metadata = self._get_property_dict_from_view(view, properties_to_process)
        
        if not property_metadata:
            return None
        
        self._log("debug", f"Extracting properties {list(property_metadata.keys())} from instance {instance.external_id}")
        
        # Parse with LLM
        parsed_values = self._parse_text_with_llm(text, property_metadata)
        
        # Filter parsed values to only include properties we intended to extract
        filtered_parsed_values = {
            k: v for k, v in parsed_values.items() 
            if k in properties_to_process
        }
        
        # Create and return NodeApply with property configs for write mode handling
        return self._create_node_apply(
            instance, 
            filtered_parsed_values, 
            view,  # source view for property metadata
            property_mapping,
            property_configs_dict,
            target_view  # optional target view for writing
        )
    
    def extract_properties_from_instances(
        self,
        instances: List,
        view: View,
        text_property: str,
        properties_to_extract: Optional[List[str]] = None,
        ai_property_mapping: Optional[Dict[str, str]] = None,
        property_configs: Optional[List[PropertyConfig]] = None,
        target_view: Optional[View] = None,
        dry_run: bool = True,
        llm_batch_size: int = 1
    ) -> List[NodeApply]:
        """
        Extract and fill properties from multiple instances.
        
        Args:
            instances: List of CDF instances to process
            view: The source view defining the properties to read from
            text_property: The property name containing the text to parse
            properties_to_extract: Optional list of property IDs to extract.
                                   If None, all non-filled properties will be extracted.
                                   Ignored if property_configs is provided.
            ai_property_mapping: Optional mapping from source to target properties.
                                e.g., {"description": "ai_description"}
                                Ignored if property_configs is provided.
            property_configs: Optional list of PropertyConfig objects with per-property write modes.
                             If provided, takes precedence over properties_to_extract and ai_property_mapping.
            target_view: Optional separate view for writing extracted properties.
                        If not specified, writes to the source view.
            dry_run: If True, return NodeApply objects without applying to CDF (default: True)
            llm_batch_size: Number of instances to send to LLM in a single prompt (default: 1).
                           Set to 1 for individual processing. Higher values reduce API calls.
        
        Returns:
            List of NodeApply objects with extracted properties
        """
        results = []
        total = len(instances)
        
        if llm_batch_size <= 1:
            # Original single-instance processing
            for i, instance in enumerate(instances):
                try:
                    self._log("debug", f"Processing instance {i+1}/{total}: {instance.external_id}")
                    
                    node_apply = self.extract_properties_from_instance(
                        instance=instance,
                        view=view,
                        text_property=text_property,
                        properties_to_extract=properties_to_extract,
                        ai_property_mapping=ai_property_mapping,
                        property_configs=property_configs,
                        target_view=target_view
                    )
                    
                    if node_apply:
                        results.append(node_apply)
                        
                except Exception as e:
                    self._log("error", f"Error processing instance {instance.external_id}: {e}")
                    print(f"Error processing instance {instance.external_id}: {e}")
                    continue
        else:
            # Batch LLM processing - send multiple instances in a single prompt
            results = self._extract_properties_batched(
                instances=instances,
                view=view,
                text_property=text_property,
                properties_to_extract=properties_to_extract,
                ai_property_mapping=ai_property_mapping,
                property_configs=property_configs,
                target_view=target_view,
                llm_batch_size=llm_batch_size
            )
        
        if not dry_run and results:
            self._log("info", f"Applying {len(results)} node updates to CDF")
            self.client.data_modeling.instances.apply(results)
        
        return results
    
    def _extract_properties_batched(
        self,
        instances: List,
        view: View,
        text_property: str,
        properties_to_extract: Optional[List[str]],
        ai_property_mapping: Optional[Dict[str, str]],
        property_configs: Optional[List[PropertyConfig]],
        target_view: Optional[View],
        llm_batch_size: int
    ) -> List[NodeApply]:
        """
        Extract properties from multiple instances using batched LLM calls.
        
        Sends multiple instances to the LLM in a single prompt and parses the 
        list response to create NodeApply objects for each instance.
        
        Args:
            instances: List of CDF instances to process
            view: The source view defining the properties to read from
            text_property: The property name containing the text to parse
            properties_to_extract: Optional list of property IDs to extract
            ai_property_mapping: Optional mapping from source to target properties
            property_configs: Optional list of PropertyConfig objects
            target_view: Optional separate view for writing extracted properties
            llm_batch_size: Number of instances to send to LLM in a single prompt
        
        Returns:
            List of NodeApply objects with extracted properties
        """
        results = []
        total = len(instances)
        
        # Build property mapping and config dict from PropertyConfig objects
        if property_configs:
            ai_property_mapping = {}
            property_configs_dict = {}
            properties_to_extract = []
            for pc in property_configs:
                properties_to_extract.append(pc.property)
                if pc.target_property and pc.target_property != pc.property:
                    ai_property_mapping[pc.property] = pc.target_property
                property_configs_dict[pc.property] = pc
        else:
            ai_property_mapping = ai_property_mapping or {}
            property_configs_dict = {}
        
        # Use target view for writing, fall back to source view
        write_view = target_view if target_view else view
        
        # Determine which properties to extract (same logic as single-instance)
        if properties_to_extract is not None:
            candidate_properties = properties_to_extract
        else:
            candidate_properties = [
                prop_id for prop_id, prop in view.properties.items()
                if not self._is_reverse_relation(prop)
            ]
        
        # Build property metadata once (shared across all instances)
        property_metadata = self._get_property_dict_from_view(view, candidate_properties)
        if not property_metadata:
            self._log("debug", "No extractable properties found")
            return []
        
        # Build property mapping
        property_mapping = {}
        for source_prop in candidate_properties:
            if source_prop in view.properties and not self._is_reverse_relation(view.properties[source_prop]):
                target_prop = ai_property_mapping.get(source_prop, source_prop)
                if target_prop in write_view.properties:
                    property_mapping[source_prop] = target_prop
        
        # Process instances in batches
        for batch_start in range(0, total, llm_batch_size):
            batch_end = min(batch_start + llm_batch_size, total)
            batch_instances = instances[batch_start:batch_end]
            
            self._log("debug", f"Processing batch {batch_start+1}-{batch_end}/{total}")
            
            # Prepare items for batch prompt
            items = []
            instance_map = {}  # Map ID to instance for later lookup
            
            for instance in batch_instances:
                instance_properties = instance.properties.get(view.as_id(), {})
                text = instance_properties.get(text_property)
                
                if not text:
                    self._log("debug", f"Skipping instance {instance.external_id} - no text property")
                    continue
                
                items.append({
                    "id": instance.external_id,
                    "text": text
                })
                instance_map[instance.external_id] = instance
            
            if not items:
                continue
            
            try:
                # Call LLM with batch prompt
                batch_results = self._parse_batch_with_llm(items, property_metadata)
                
                # Create NodeApply for each result
                for instance_id, parsed_values in batch_results.items():
                    instance = instance_map.get(instance_id)
                    if not instance:
                        self._log("warning", f"Instance ID '{instance_id}' from LLM not found in batch")
                        continue
                    
                    # Filter parsed values to only include properties we intended to extract
                    filtered_parsed_values = {
                        k: v for k, v in parsed_values.items() 
                        if k in candidate_properties
                    }
                    
                    if not filtered_parsed_values:
                        continue
                    
                    # Create NodeApply with property configs for write mode handling
                    node_apply = self._create_node_apply(
                        instance, 
                        filtered_parsed_values, 
                        view,
                        property_mapping,
                        property_configs_dict,
                        target_view
                    )
                    
                    if node_apply:
                        results.append(node_apply)
                        
            except Exception as e:
                self._log("error", f"Error processing batch {batch_start+1}-{batch_end}: {e}")
                print(f"Error processing batch {batch_start+1}-{batch_end}: {e}")
                
                # Fallback to individual processing for this batch
                self._log("info", f"Falling back to individual processing for batch {batch_start+1}-{batch_end}")
                for instance in batch_instances:
                    try:
                        node_apply = self.extract_properties_from_instance(
                            instance=instance,
                            view=view,
                            text_property=text_property,
                            properties_to_extract=properties_to_extract,
                            ai_property_mapping=ai_property_mapping,
                            property_configs=property_configs,
                            target_view=target_view
                        )
                        if node_apply:
                            results.append(node_apply)
                    except Exception as inner_e:
                        self._log("error", f"Error processing instance {instance.external_id}: {inner_e}")
                        continue
        
        return results
    
    def get_extractable_properties(self, view: View) -> Dict[str, Dict[str, Any]]:
        """
        Get all extractable properties from a view (excluding reverse relations).
        
        Useful for inspecting which properties can be extracted.
        
        Args:
            view: The view to inspect
        
        Returns:
            Dictionary mapping property_id to property info
        """
        extractable = {}
        skipped = {}
        
        for prop_id, prop in view.properties.items():
            prop_info = {
                "name": prop.name or prop_id,
                "description": prop.description or "",
                "type": type(prop.type).__name__ if hasattr(prop, 'type') else type(prop).__name__,
                "is_list": getattr(prop.type, "is_list", False) if hasattr(prop, 'type') else False
            }
            
            if self._is_reverse_relation(prop):
                prop_info["skip_reason"] = "Reverse relation - cannot be set directly"
                skipped[prop_id] = prop_info
            else:
                extractable[prop_id] = prop_info
        
        return {
            "extractable": extractable,
            "skipped": skipped
        }


