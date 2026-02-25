"""
Key Extraction Engine for Cognite Data Fusion (CDF)

This module implements the core key extraction functionality as described in the
key_extraction.md specification. It provides methods for extracting candidate keys
and foreign key references from entity metadata using regex, fixed width parsing,
token reassembly, and heuristic approaches.

Features:
- 4 extraction methods: regex, fixed width, token reassembly, heuristic
- Support for candidate keys and foreign key references
- Configurable extraction rules with priority ordering
- Integration with CDF data model views
- Comprehensive validation and error handling

Author: Darren Downtain
Version: 1.0.1
"""

import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from modules.accelerators.contextualization.cdf_key_extraction_aliasing.functions.fn_dm_key_extraction.utils.TokenReassemblyMethodParameter import (
    AssemblyRule,
    TokenReassemblyMethodParameter,
)

from ..common.logger import CogniteFunctionLogger
from ..config import ConfigData, ValidationConfig
from ..utils.DataStructures import *
from .handlers import (
    ExtractionMethodHandler,
    FixedWidthExtractionHandler,
    HeuristicExtractionHandler,
    RegexExtractionHandler,
    TokenReassemblyExtractionHandler,
)


class KeyExtractionEngine:
    """Main engine for key extraction operations."""

    def __init__(
        self,
        config: ConfigData,
        logger: CogniteFunctionLogger = CogniteFunctionLogger("INFO", True),
    ):
        """Initialize the key extraction engine with configuration."""
        self.config = config
        self.logger = logger
        self.rules = self._load_rules()
        self.method_handlers = self._initialize_method_handlers()
        self.validation_config = ValidationConfig(**config.get("validation", {}))

    def _load_rules(self) -> List[ExtractionRule]:
        """Load and parse extraction rules from configuration."""
        rules = []
        rules_config = self.config.get("extraction_rules", [])

        for rule_config in rules_config:
            try:
                # Parse source fields
                source_fields = []
                source_fields_config = rule_config.get("source_fields", [])
                for field_config in source_fields_config:
                    if isinstance(field_config, dict):
                        source_field = SourceField(
                            field_name=field_config["field_name"],
                            field_type=field_config.get("field_type", "string"),
                            required=field_config.get("required", False),
                            priority=field_config.get("priority", 1),
                            separator=field_config.get("separator"),
                            role=field_config.get("role", "target"),
                            max_length=field_config.get("max_length", 1000),
                            preprocessing=field_config.get("preprocessing", []),
                        )
                    else:
                        # Simple string format
                        source_field = SourceField(field_name=field_config)
                    source_fields.append(source_field)

                # Normalize method name (convert "fixed width" -> "fixed_width", etc.)
                method_name = rule_config.get("method", "regex")
                if isinstance(method_name, str):
                    method_name = method_name.replace(" ", "_")

                rule = ExtractionRule(
                    name=rule_config["name"],
                    description=rule_config.get("description", ""),
                    extraction_type=ExtractionType(
                        rule_config.get("extraction_type", "candidate_key")
                    ),
                    method=ExtractionMethod(method_name),
                    pattern=rule_config.get("pattern", ""),
                    priority=rule_config.get("priority", 50),
                    enabled=rule_config.get("enabled", True),
                    scope_filters=rule_config.get("scope_filters", {}),
                    min_confidence=rule_config.get("min_confidence", 0.3),
                    case_sensitive=rule_config.get("case_sensitive", False),
                    aliasing_rules=rule_config.get("aliasing_rules", []),
                    source_fields=source_fields,
                    config=rule_config.get("config", {}),
                    composite_strategy=rule_config.get("composite_strategy"),
                )
                rules.append(rule)

            except (KeyError, ValueError) as e:
                self.logger.error(f"Invalid rule configuration: {e}")

        return sorted(rules, key=lambda r: r.priority)

    def _initialize_method_handlers(
        self,
    ) -> Dict[ExtractionMethod, ExtractionMethodHandler]:
        """Initialize method handler instances."""
        return {
            ExtractionMethod.REGEX: RegexExtractionHandler(self.logger),
            ExtractionMethod.FIXED_WIDTH: FixedWidthExtractionHandler(self.logger),
            ExtractionMethod.TOKEN_REASSEMBLY: TokenReassemblyExtractionHandler(
                self.logger
            ),
            ExtractionMethod.HEURISTIC: HeuristicExtractionHandler(self.logger),
        }

    def extract_keys(
        self, entity: Dict[str, Any], entity_type: str = "asset"
    ) -> ExtractionResult:
        """
        Extract keys from entity metadata.

        Args:
            entity: Entity data with metadata fields
            entity_type: Type of entity (asset, file, timeseries, etc.)

        Returns:
            ExtractionResult with extracted keys and metadata
        """
        result = ExtractionResult(
            entity_id=entity.get("id", entity.get("externalId", "unknown")),
            entity_type=entity_type,
        )

        # Build context for extraction
        context = self._build_context(entity, entity_type)

        # Apply each rule
        for rule in self.rules:
            if not rule.enabled:
                continue

            # Check scope filters
            if not self._check_scope_filters(rule, context):
                continue

            # Handle composite field extraction (cross-field merging)
            if rule.composite_strategy:
                extracted_keys = self._extract_from_composite_fields(
                    entity, rule, context, result
                )
                # Categorize extracted keys
                for key in extracted_keys:
                    if key.extraction_type == ExtractionType.CANDIDATE_KEY:
                        result.candidate_keys.append(key)
                    elif key.extraction_type == ExtractionType.FOREIGN_KEY_REFERENCE:
                        result.foreign_key_references.append(key)
                    elif key.extraction_type == ExtractionType.DOCUMENT_REFERENCE:
                        result.document_references.append(key)
                continue

            # Extract from source fields honoring field_selection_strategy
            strategy = (
                rule.field_selection_strategy
                or (rule.config or {}).get("field_selection_strategy")
                or "merge_all"
            )

            # Sort fields by priority (lower number = higher priority)
            fields = sorted(rule.source_fields, key=lambda f: getattr(f, "priority", 1))

            collected_for_rule: List[ExtractedKey] = []

            for source_field in fields:
                field_value = self._get_field_value(entity, source_field)
                if not field_value:
                    if source_field.required:
                        self.logger.verbose(
                            "DEBUG",
                            f"Required field '{source_field.field_name}' missing for entity {result.entity_id}",
                        )
                        continue
                    else:
                        continue

                # Apply preprocessing
                processed_value = self._preprocess_field_value(
                    field_value, source_field
                )

                # Extract keys using appropriate method
                method_handler = self.method_handlers.get(rule.method)
                if not method_handler:
                    self.logger.warning(
                        "WARNING", f"No handler for method {rule.method}"
                    )
                    continue

                try:
                    extracted_keys = method_handler.extract(
                        processed_value, rule, context
                    )

                    collected_for_rule.extend(extracted_keys)

                    # field selection strategies short-circuiting
                    # first_match: stop at first field (in priority order) that produces results
                    if strategy == "first_match" and extracted_keys:
                        break

                except Exception as e:
                    self.logger.verbose(
                        "ERROR", f"Error extracting keys with rule '{rule.name}': {e}"
                    )

            # Post-process collected keys per strategy
            if collected_for_rule:
                # merge_all: process all fields, deduplication will keep highest confidence for duplicates
                # Categorize into result
                for key in collected_for_rule:
                    if rule.extraction_type == ExtractionType.CANDIDATE_KEY:
                        result.candidate_keys.append(key)
                    elif rule.extraction_type == ExtractionType.FOREIGN_KEY_REFERENCE:
                        result.foreign_key_references.append(key)
                    elif rule.extraction_type == ExtractionType.DOCUMENT_REFERENCE:
                        result.document_references.append(key)

        # Apply validation
        result = self._validate_extraction_result(result)

        return result

    def _build_context(
        self, entity: Dict[str, Any], entity_type: str
    ) -> Dict[str, Any]:
        """Build context information for extraction."""
        context = {
            "entity_type": entity_type,
            "entity_id": entity.get("id", entity.get("externalId", "unknown")),
        }

        # Add common context fields
        if "metadata" in entity and entity["metadata"] is not None:
            metadata = entity["metadata"]
            context.update(
                {
                    "site": metadata.get("site"),
                    "unit": metadata.get("unit"),
                    "equipment_type": metadata.get("equipmentType"),
                    "document_type": metadata.get("documentType"),
                }
            )

        view_id = entity.get(entity.get("externalId", "unknown"), {})
        if view_id:
            context.update(
                {
                    "view_space": view_id.get("view_space", ""),
                    "view_external_id": view_id.get("view_external_id"),
                    "view_version": view_id.get("view_version"),
                    "instance_space": view_id.get("instance_space"),
                }
            )

        # Pass blacklist so extraction handlers can set confidence to 0.0 for blacklisted keys
        context["blacklist_keywords"] = (
            getattr(self.validation_config, "blacklist_keywords", []) or []
        )

        return context

    def _extract_from_composite_fields(
        self,
        entity: Dict[str, Any],
        rule: ExtractionRule,
        context: Dict[str, Any],
        result: ExtractionResult,
    ) -> List[ExtractedKey]:
        """
        Extract keys by merging multiple source fields based on composite_strategy.

        Args:
            entity: Entity data with metadata fields
            rule: Extraction rule with composite_strategy configured
            context: Extraction context
            result: Current extraction result (for metadata)

        Returns:
            List of extracted keys from merged fields
        """
        extracted_keys = []

        # Separate fields by role (target/context used by composite strategies below)
        target_fields = [f for f in rule.source_fields if f.role == "target"]
        context_fields = [f for f in rule.source_fields if f.role == "context"]

        # Collect field values
        field_values = {}
        for source_field in rule.source_fields:
            field_value = self._get_field_value(entity, source_field)
            if field_value:
                # Apply preprocessing
                processed_value = self._preprocess_field_value(
                    field_value, source_field
                )
                field_values[source_field.field_name] = {
                    "value": processed_value,
                    "field": source_field,
                }
            elif source_field.required:
                # Required field missing, skip this rule
                self.logger.verbose(
                    "WARNING",
                    f"Required field '{source_field.field_name}' missing for composite extraction in rule '{rule.name}'",
                )
                return []

        # Apply composite strategy
        if rule.composite_strategy == "concatenate":
            # Simple concatenation of fields
            separator = rule.config.get("field_separator", "-")
            field_order = rule.config.get(
                "field_order", [f.field_name for f in target_fields]
            )

            composite_value_parts = []
            for field_name in field_order:
                if field_name in field_values:
                    composite_value_parts.append(field_values[field_name]["value"])

            if composite_value_parts:
                composite_value = separator.join(composite_value_parts)

                # Extract using the configured method
                method_handler = self.method_handlers.get(rule.method)
                if method_handler:
                    try:
                        extracted_keys = method_handler.extract(
                            composite_value, rule, context
                        )
                        # Update source_field metadata to indicate composite extraction
                        for key in extracted_keys:
                            key.source_field = "+".join(field_order)
                            key.metadata["composite_extraction"] = True
                            key.metadata["composite_fields"] = field_order
                            key.metadata["composite_strategy"] = "concatenate"
                    except Exception as e:
                        self.logger.verbose(
                            "ERROR",
                            f"Error in composite extraction for rule '{rule.name}': {e}",
                        )

        elif rule.composite_strategy == "token_reassembly":
            # Cross-field token reassembly
            extracted_keys = self._extract_cross_field_token_reassembly(
                field_values, rule, context
            )

        elif rule.composite_strategy == "context_aware":
            # Use context fields to inform extraction from target fields
            # Merge context into context dict and extract from target fields
            enhanced_context = context.copy()
            for context_field in context_fields:
                if context_field.field_name in field_values:
                    enhanced_context[context_field.field_name] = field_values[
                        context_field.field_name
                    ]["value"]

            # Extract from target fields with enhanced context
            method_handler = self.method_handlers.get(rule.method)
            if method_handler:
                for target_field in target_fields:
                    if target_field.field_name in field_values:
                        try:
                            field_keys = method_handler.extract(
                                field_values[target_field.field_name]["value"],
                                rule,
                                enhanced_context,
                            )
                            for key in field_keys:
                                key.metadata["context_aware"] = True
                                key.metadata["context_fields"] = [
                                    f.field_name for f in context_fields
                                ]
                            extracted_keys.extend(field_keys)
                        except Exception as e:
                            self.logger.verbose(
                                "ERROR",
                                f"Error in context-aware extraction for rule '{rule.name}': {e}",
                            )

        return extracted_keys

    def _extract_cross_field_token_reassembly(
        self,
        field_values: Dict[str, Dict[str, Any]],
        rule: ExtractionRule,
        context: Dict[str, Any],
    ) -> List[ExtractedKey]:
        """
        Extract tokens from multiple fields and reassemble them into complete tags.

        Args:
            field_values: Dictionary mapping field names to their processed values
            rule: Extraction rule with token_reassembly method
            context: Extraction context

        Returns:
            List of extracted and reassembled keys
        """
        extracted_keys = []
        config = rule.config
        tokenization = config.get("tokenization", {})

        extract_from_multiple_fields = tokenization.get(
            "extract_from_multiple_fields", []
        )

        if not extract_from_multiple_fields:
            # Fallback: tokenize each field separately and combine
            # Use first token from each field as potential components
            separator_patterns = tokenization.get(
                "separator_patterns", ["-", "_", "/", " "]
            )
            all_tokens = {}

            for field_name, field_data in field_values.items():
                tokens = self._tokenize_field_value(
                    field_data["value"], separator_patterns
                )
                # Store tokens keyed by field name (for assembly format like {siteCode}-{unitNumber})
                if tokens:
                    all_tokens[field_name] = [
                        {
                            "value": tokens[0],  # Use first token from field
                            "field": field_name,
                            "position": 0,
                            "required": True,
                        }
                    ]
        else:
            # Extract tokens from specific fields based on configuration
            all_tokens = {}
            separator_patterns = tokenization.get(
                "separator_patterns", ["-", "_", "/", " "]
            )
            token_patterns = tokenization.get("token_patterns", [])

            # Map token types to fields
            for field_config in rule.source_fields:
                field_name = field_config.field_name
                token_type = field_config.role

                if field_name in field_values:
                    field_value = field_values[field_name]["value"]
                    tokens = self._tokenize_field_value(field_value, separator_patterns)

                    # Match tokens against patterns if specified
                    for token_pattern in token_patterns:
                        pattern_name = token_pattern.get("name")
                        pattern_regex = token_pattern.get("pattern")
                        component_type = token_pattern.get("component_type", "")

                        if component_type == token_type and pattern_regex:
                            try:
                                compiled_pattern = re.compile(pattern_regex)
                                for i, token in enumerate(tokens):
                                    if compiled_pattern.match(token):
                                        if pattern_name not in all_tokens:
                                            all_tokens[pattern_name] = []
                                        all_tokens[pattern_name].append(
                                            {
                                                "value": token,
                                                "field": field_name,
                                                "position": i,
                                                "component_type": component_type,
                                                "required": token_pattern.get(
                                                    "required", False
                                                ),
                                            }
                                        )
                            except re.error as e:
                                self.logger.verbose(
                                    "ERROR",
                                    f"Invalid token pattern '{pattern_regex}': {e}",
                                )

        # Apply assembly rules
        assembly_rules = config.get("assembly_rules", [])
        handler = TokenReassemblyExtractionHandler()
        tkr_param = TokenReassemblyMethodParameter(**rule.config)

        for assembly_rule in assembly_rules:
            assembled_keys = handler._assemble_tokens(
                all_tokens,
                AssemblyRule(**assembly_rule),
                tkr_param,
                config.get("extraction_type", rule.extraction_type),
            )
            if assembled_keys:
                for assembled_key in assembled_keys:
                    assembled_key.metadata["cross_field_extraction"] = True
                    assembled_key.metadata["source_fields"] = list(field_values.keys())
                    assembled_key.source_field = "+".join(field_values.keys())
                    assembled_key.extraction_type = config.get(
                        "extraction_type", rule.extraction_type
                    )
                extracted_keys.append(assembled_key)

        return extracted_keys

    def _tokenize_field_value(
        self, text: str, separator_patterns: List[str]
    ) -> List[str]:
        """Tokenize text using separator patterns."""
        separator_regex = "|".join(re.escape(sep) for sep in separator_patterns)
        tokens = re.split(separator_regex, text)
        return [token.strip() for token in tokens if token.strip()]

    def _check_scope_filters(
        self, rule: ExtractionRule, context: Dict[str, Any]
    ) -> bool:
        """Check if rule scope filters are satisfied."""
        if not rule.scope_filters:
            return True

        for filter_key, filter_values in rule.scope_filters.items():
            context_value = context.get(filter_key)
            if isinstance(filter_values, list):
                if context_value not in filter_values:
                    return False
            else:
                if context_value != filter_values:
                    return False

        return True

    def _get_field_value(
        self, entity: Dict[str, Any], source_field: SourceField
    ) -> Optional[str]:
        """Get field value from entity data."""
        field_name = source_field.field_name

        # Handle nested field paths
        if "." in field_name:
            parts = field_name.split(".")
            value = entity
            for part in parts:
                if isinstance(value, dict) and part in value:
                    value = value[part]
                else:
                    return None
            return str(value) if value is not None else None

        # Handle array fields
        if source_field.field_type == "array" and field_name in entity:
            field_value = entity[field_name]
            if isinstance(field_value, list):
                separator = source_field.separator or ","
                return separator.join(str(item) for item in field_value)

        # Simple field access
        if field_name in entity:
            value = entity[field_name]
            return str(value) if value is not None else None

        return None

    def _preprocess_field_value(
        self, field_value: str, source_field: SourceField
    ) -> str:
        """Apply preprocessing to field value."""
        processed = field_value

        for preprocessing_step in source_field.preprocessing:
            if preprocessing_step == "trim":
                processed = processed.strip()
            elif preprocessing_step == "lowercase":
                processed = processed.lower()
            elif preprocessing_step == "uppercase":
                processed = processed.upper()
            elif preprocessing_step == "remove_special_chars":
                processed = re.sub(r"[^\w\s-]", "", processed)

        # Apply max length limit
        if len(processed) > source_field.max_length:
            processed = processed[: source_field.max_length]

        return processed

    def _validate_extraction_result(self, result: ExtractionResult) -> ExtractionResult:
        """Apply validation to extraction result."""
        # Remove duplicate keys
        result.candidate_keys = self._remove_duplicate_keys(result.candidate_keys)
        result.foreign_key_references = self._remove_duplicate_keys(
            result.foreign_key_references
        )
        result.document_references = self._remove_duplicate_keys(
            result.document_references
        )

        # Apply min_confidence to overall results (only place keys are dropped).
        # Blacklisted items already have confidence 0.0 from extraction handlers;
        # they are dropped here when 0.0 < min_confidence.
        min_confidence = self.validation_config.min_confidence
        result.candidate_keys = [
            k for k in result.candidate_keys if k.confidence >= min_confidence
        ]
        result.foreign_key_references = [
            k for k in result.foreign_key_references if k.confidence >= min_confidence
        ]
        result.document_references = [
            k for k in result.document_references if k.confidence >= min_confidence
        ]

        # Add metadata (validation_config as dict for JSON serialization)
        validation_config_dict = (
            self.validation_config.model_dump()
            if hasattr(self.validation_config, "model_dump")
            else dict(self.validation_config)
        )
        result.metadata = {
            "extraction_timestamp": datetime.now().isoformat(),
            "total_candidate_keys": len(result.candidate_keys),
            "total_foreign_keys": len(result.foreign_key_references),
            "total_document_references": len(result.document_references),
            "validation_config": validation_config_dict,
        }

        return result

    def _remove_duplicate_keys(self, keys: List[ExtractedKey]) -> List[ExtractedKey]:
        """Remove duplicate keys, keeping the one with highest confidence."""
        seen = {}
        for key in keys:
            if key.value not in seen or key.confidence > seen[key.value].confidence:
                seen[key.value] = key
        return list(seen.values())


# TODO this method is never called?
# def load_config_from_yaml(file_path: str) -> Dict[str, Any]:
#     """Load configuration from YAML file."""
#     try:
#         with open(file_path, "r") as f:
#             return yaml.safe_load(f)
#     except FileNotFoundError:
#         logger.verbose("ERROR", f"Configuration file not found: {file_path}")
#         return {}
#     except yaml.YAMLError as e:
#         logger.verbose("ERROR", f"Error parsing YAML configuration: {e}")
#         return {}


def main():
    """Example usage of the KeyExtractionEngine."""

    # Example configuration
    config = {
        "extraction_rules": [
            {
                "name": "standard_pump_tag",
                "description": "Extracts standard pump tags from equipment descriptions",
                "extraction_type": "candidate_key",
                "method": "regex",
                "pattern": r"\bP[-_]?\d{2,4}[A-Z]?\b",
                "priority": 50,
                "enabled": True,
                "min_confidence": 0.7,
                "case_sensitive": False,
                "source_fields": [
                    {
                        "field_name": "name",
                        "field_type": "string",
                        "required": True,
                        "priority": 1,
                    },
                    {
                        "field_name": "description",
                        "field_type": "string",
                        "required": False,
                        "priority": 2,
                    },
                ],
            },
            {
                "name": "flow_instrument_tag",
                "description": "Extracts ISA flow instrument tags",
                "extraction_type": "foreign_key_reference",
                "method": "regex",
                "pattern": r"\bFIC[-_]?\d{4}[A-Z]?\b",
                "priority": 30,
                "enabled": True,
                "min_confidence": 0.8,
                "source_fields": [
                    {
                        "field_name": "description",
                        "field_type": "string",
                        "required": False,
                    }
                ],
            },
        ],
        "validation": {"min_confidence": 0.5, "max_keys_per_type": 10},
    }

    # Initialize engine
    engine = KeyExtractionEngine(config)

    # Test cases
    test_entities = [
        {
            "id": "asset_001",
            "name": "P-101",
            "description": "Main feed pump for Tank T-301, controlled by FIC-2001",
            "metadata": {"site": "Plant_A", "equipmentType": "pump"},
        },
        {
            "id": "asset_002",
            "name": "FCV-2001A",
            "description": "Flow control valve for reactor feed system",
            "metadata": {"site": "Plant_B", "equipmentType": "valve"},
        },
    ]

    print("Key Extraction Engine - Test Results")
    print("=" * 50)

    for i, entity in enumerate(test_entities, 1):
        print(f"\nTest Case {i}:")
        print(f"Entity: {entity['name']}")
        print(f"Description: {entity['description']}")

        result = engine.extract_keys(entity, "asset")

        print(f"\nCandidate Keys ({len(result.candidate_keys)}):")
        for key in result.candidate_keys:
            print(
                f"  - {key.value} (confidence: {key.confidence:.2f}, method: {key.method.value})"
            )

        print(f"\nForeign Key References ({len(result.foreign_key_references)}):")
        for key in result.foreign_key_references:
            print(
                f"  - {key.value} (confidence: {key.confidence:.2f}, method: {key.method.value})"
            )

        print(f"\nDocument References ({len(result.document_references)}):")
        for key in result.document_references:
            print(
                f"  - {key.value} (confidence: {key.confidence:.2f}, method: {key.method.value})"
            )


if __name__ == "__main__":
    main()
