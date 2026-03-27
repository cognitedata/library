"""
Key Extraction Engine for Cognite Data Fusion (CDF)

This module implements the core key extraction functionality as described in the
key_extraction.md specification. It provides methods for extracting candidate keys
and foreign key references from entity metadata using regex, fixed width parsing,
token reassembly, and heuristic approaches.

Features:
- 5 extraction methods: passthrough (default when omitted), regex, fixed width, token reassembly, heuristic
- Support for candidate keys and foreign key references
- Configurable extraction rules with priority ordering
- Integration with CDF data model views
- Comprehensive validation and error handling

Author: Darren Downtain
Version: 1.0.1
"""

import re
from datetime import datetime
from types import SimpleNamespace
from typing import Any, Dict, List, Optional, Union

from ..utils.TokenReassemblyMethodParameter import (
    AssemblyRule,
    TokenReassemblyMethodParameter,
)

from ..common.logger import CogniteFunctionLogger
from ..config import Config, ExtractionRuleConfig
from ..utils.DataStructures import *
from ..utils.rule_utils import (
    get_extraction_type_from_rule,
    get_method_from_rule,
    normalize_extraction_type,
    normalize_method,
)
from .handlers import (
    ExtractionMethodHandler,
    FixedWidthExtractionHandler,
    HeuristicExtractionHandler,
    PassthroughExtractionHandler,
    RegexExtractionHandler,
    TokenReassemblyExtractionHandler,
)


class KeyExtractionEngine:
    """Main engine for key extraction operations."""

    def __init__(
        self,
        config: Union[Config, Dict[str, Any]],
        logger: CogniteFunctionLogger = CogniteFunctionLogger("INFO", False),
    ):
        """Initialize the key extraction engine with configuration (Config or dict from main.py)."""
        self.logger = logger
        if isinstance(config, dict):
            params = config.get("parameters", {})
            if isinstance(params, dict):
                params = SimpleNamespace(min_key_length=params.get("min_key_length", 3), **params)
            data = SimpleNamespace(
                extraction_rules=config.get("extraction_rules", []),
                field_selection_strategy=config.get("field_selection_strategy", "first_match"),
            )
            self.config = SimpleNamespace(parameters=params, data=data)
            validation_default = config.get("validation") or {}
            if isinstance(validation_default, dict):
                validation_default = SimpleNamespace(
                    blacklist_keywords=validation_default.get("blacklist_keywords", []),
                    min_confidence=validation_default.get("min_confidence", 0.1),
                    regexp_match=validation_default.get("regexp_match"),
                )
            _defaults = {"scope_filters": {}, "validation": validation_default}
            _sf_defaults = {"table_id": None, "role": "target", "field_name": "", "required": False, "priority": 1}
            self.rules = []
            for r in config.get("extraction_rules", []):
                if isinstance(r, dict):
                    r = dict(_defaults, **r)
                    r.setdefault("validation", validation_default)
                    # Normalize method name for handler lookup (e.g. fixed_width -> fixed width)
                    method = r.get("method", "")
                    if method == "fixed_width":
                        r["method"] = ExtractionMethod.FIXED_WIDTH.value
                    elif method == "token_reassembly":
                        r["method"] = ExtractionMethod.TOKEN_REASSEMBLY.value
                    sf = r.get("source_fields", [])
                    if sf and isinstance(sf[0], dict):
                        r["source_fields"] = [SimpleNamespace(**{**_sf_defaults, **f}) for f in sf]
                    self.rules.append(SimpleNamespace(**r))
                else:
                    self.rules.append(r)
        else:
            self.config = config
            self.rules = config.data.extraction_rules
        self.method_handlers = self._initialize_method_handlers()
        self.field_selection_strategy = self.config.data.field_selection_strategy

    def _categorize_keys_into_result(
        self,
        keys: List[ExtractedKey],
        rule: Any,
        result: ExtractionResult,
    ) -> None:
        """Append keys to candidate_keys / foreign_key_references / document_references by rule type."""
        rtype = normalize_extraction_type(get_extraction_type_from_rule(rule))
        for key in keys:
            if rtype == ExtractionType.CANDIDATE_KEY:
                result.candidate_keys.append(key)
            elif rtype == ExtractionType.FOREIGN_KEY_REFERENCE:
                result.foreign_key_references.append(key)
            elif rtype == ExtractionType.DOCUMENT_REFERENCE:
                result.document_references.append(key)

    def _initialize_method_handlers(
        self,
    ) -> Dict[ExtractionMethod, ExtractionMethodHandler]:
        """Initialize method handler instances."""
        return {
            ExtractionMethod.PASSTHROUGH.value: PassthroughExtractionHandler(self.logger),
            ExtractionMethod.REGEX.value: RegexExtractionHandler(self.logger),
            ExtractionMethod.FIXED_WIDTH.value: FixedWidthExtractionHandler(self.logger),
            ExtractionMethod.TOKEN_REASSEMBLY.value: TokenReassemblyExtractionHandler(
                self.logger
            ),
            ExtractionMethod.HEURISTIC.value: HeuristicExtractionHandler(self.logger),
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
            if not getattr(rule, "enabled", True):
                continue

            # Handle composite field extraction (cross-field merging)
            if getattr(rule, "composite_strategy", None):
                extracted_keys = self._extract_from_composite_fields(
                    entity, rule, context, result
                )
                if extracted_keys:
                    self._categorize_keys_into_result(extracted_keys, rule, result)
                continue

            # Check scope filters
            if not self._check_scope_filters(rule, context):
                continue

            # Extract from source fields honoring field_selection_strategy
            strategy = self.field_selection_strategy

            # Normalize source_fields to list (can be single SourceFieldParameter or list)
            source_fields_list = (
                list(rule.source_fields)
                if isinstance(rule.source_fields, list)
                else ([rule.source_fields] if rule.source_fields else [])
            )
            fields = sorted(
                source_fields_list, key=lambda f: getattr(f, "priority", 1)
            )

            collected_for_rule: List[ExtractedKey] = []

            for source_field in fields:
                field_value = self._get_field_value(entity, source_field, rule.name)
                if not isinstance(field_value, str) or field_value == "":
                    if getattr(source_field, "required", False):
                        self.logger.verbose(
                            "DEBUG",
                            f"Required field '{source_field.field_name}' missing for entity {result.entity_id}",
                        )
                        continue
                    else:
                        continue

                if getattr(source_field, "preprocessing", None):
                    processed_value = self._preprocess_field_value(
                        field_value, source_field
                    )
                else:
                    processed_value = field_value

                # Extract keys using appropriate method (canonical method name from one place)
                method_key = normalize_method(
                    getattr(rule, "method", None)
                ).value
                method_handler = self.method_handlers.get(method_key)
                if not method_handler:
                    self.logger.warning(f"No handler for method {method_key}")
                    continue

                try:
                    extracted_keys = method_handler.extract(
                        processed_value, rule, context
                    )

                    # Attribute extracted keys to the specific field we extracted from.
                    # Handlers may not know which field produced the match.
                    for k in extracted_keys:
                        if getattr(source_field, "field_name", None):
                            k.source_field = source_field.field_name

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
                self._categorize_keys_into_result(collected_for_rule, rule, result)

        # Apply validation using last rule's validation config (per-rule would require refactor)
        if self.rules:
            result = self._validate_extraction_result(self.rules[-1], result)
        else:
            result = self._validate_extraction_result(None, result)

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

        # Blacklist is enforced only in _validate_extraction_result (see _apply_blacklist).

        return context

    def _extract_from_composite_fields(
        self,
        entity: Dict[str, Any],
        rule: ExtractionRuleConfig,
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
        _rnm = getattr(rule, "name", None) or getattr(rule, "rule_id", None)
        for source_field in rule.source_fields:
            field_value = self._get_field_value(entity, source_field, _rnm)
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

                # Extract using the configured method (canonical method name)
                method_key = normalize_method(getattr(rule, "method", None)).value
                method_handler = self.method_handlers.get(method_key)
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
            method_key = normalize_method(getattr(rule, "method", None)).value
            method_handler = self.method_handlers.get(method_key)
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
                get_extraction_type_from_rule(rule),
                get_method_from_rule(rule),
                context,
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
        self, rule: ExtractionRuleConfig, context: Dict[str, Any]
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
        self,
        entity: Dict[str, Any],
        source_field: Any,
        rule_name: Optional[str] = None,
    ) -> Optional[str]:
        """Get field value from entity data. Uses rule_name for pipeline-style keys (rule_name_field_name)."""
        field_name = getattr(source_field, "field_name", source_field)
        if isinstance(field_name, str) and "." in field_name:
            parts = field_name.split(".")
            value = entity
            for part in parts:
                if isinstance(value, dict) and part in value:
                    value = value[part]
                else:
                    return None
            return str(value) if value is not None else None
        if rule_name and getattr(source_field, "table_id", None):
            lookup = "_".join([rule_name, source_field.table_id, field_name])
            return entity.get("table_data", {}).get(lookup) or entity.get(lookup)
        if rule_name:
            lookup = "_".join([rule_name, field_name])
            if lookup in entity:
                value = entity[lookup]
                return str(value) if value is not None else None
        if field_name in entity:
            value = entity[field_name]
            return str(value) if value is not None else None
        return None

    def _preprocess_field_value(
        self, field_value: str, source_field: Any
    ) -> str:
        """Apply preprocessing to field value."""
        processed = field_value
        preprocessing = getattr(source_field, "preprocessing", None) or []
        if isinstance(preprocessing, str):
            preprocessing = [preprocessing]
        for step in preprocessing:
            if step == "trim":
                processed = processed.strip()
            elif step == "lowercase":
                processed = processed.lower()
            elif step == "uppercase":
                processed = processed.upper()
            elif step == "remove_special_chars":
                processed = re.sub(r"[^\w\s-]", "", processed)
        max_len = getattr(source_field, "max_length", None) or 1000
        if len(processed) > max_len:
            processed = processed[:max_len]
        return processed

    def _validate_extraction_result(
        self,
        rule: Optional[ExtractionRuleConfig],
        result: ExtractionResult,
    ) -> ExtractionResult:
        """Apply validation to extraction result (uses rule.validation and config.parameters)."""
        min_key_length = getattr(
            self.config.parameters, "min_key_length", 3
        ) or 3
        result.candidate_keys = [
            k for k in result.candidate_keys if len(k.value) >= min_key_length
        ]
        result.foreign_key_references = [
            k for k in result.foreign_key_references if len(k.value) >= min_key_length
        ]
        result.document_references = [
            k for k in result.document_references if len(k.value) >= min_key_length
        ]
        if rule is None or rule.validation is None:
            result.candidate_keys = self._remove_duplicate_keys(result.candidate_keys)
            result.foreign_key_references = self._remove_duplicate_keys(
                result.foreign_key_references
            )
            result.document_references = self._remove_duplicate_keys(
                result.document_references
            )
            result.metadata = {
                "extraction_timestamp": datetime.now().isoformat(),
                "total_candidate_keys": len(result.candidate_keys),
                "total_foreign_keys": len(result.foreign_key_references),
                "total_document_references": len(result.document_references),
            }
            return result
        result.candidate_keys = self._remove_duplicate_keys(result.candidate_keys)
        result.foreign_key_references = self._remove_duplicate_keys(
            result.foreign_key_references
        )
        result.document_references = self._remove_duplicate_keys(
            result.document_references
        )
        blacklist_keywords = getattr(rule.validation, "blacklist_keywords", []) or []
        if blacklist_keywords:
            result.candidate_keys = self._apply_blacklist(
                result.candidate_keys, blacklist_keywords
            )
            result.foreign_key_references = self._apply_blacklist(
                result.foreign_key_references, blacklist_keywords
            )
            result.document_references = self._apply_blacklist(
                result.document_references, blacklist_keywords
            )
        min_confidence = getattr(rule.validation, "min_confidence", 0) or 0
        result.candidate_keys = [
            k for k in result.candidate_keys if k.confidence >= min_confidence
        ]
        result.foreign_key_references = [
            k for k in result.foreign_key_references if k.confidence >= min_confidence
        ]
        result.document_references = [
            k for k in result.document_references if k.confidence >= min_confidence
        ]
        rgx_patterns = None
        regexp_match = getattr(rule.validation, "regexp_match", None)
        if isinstance(regexp_match, list):
            rgx_patterns = regexp_match
        elif isinstance(regexp_match, str):
            rgx_patterns = [regexp_match]
        if rgx_patterns:
            result.candidate_keys = [
                k for k in result.candidate_keys
                if any(re.search(p, k.value) for p in rgx_patterns)
            ]
            result.foreign_key_references = [
                k for k in result.foreign_key_references
                if any(re.search(p, k.value) for p in rgx_patterns)
            ]
            result.document_references = [
                k for k in result.document_references
                if any(re.search(p, k.value) for p in rgx_patterns)
            ]
        validation_config_dict = (
            rule.validation.model_dump()
            if hasattr(rule.validation, "model_dump")
            else (vars(rule.validation) if isinstance(rule.validation, SimpleNamespace) else {})
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

    def _apply_blacklist(
        self, keys: List[ExtractedKey], blacklist_keywords: List[str]
    ) -> List[ExtractedKey]:
        """Filter out keys that contain any blacklisted keywords."""
        if not blacklist_keywords:
            return keys
        filtered_keys = []
        for key in keys:
            key_value_lower = key.value.lower()
            contains_blacklisted = any(
                keyword.lower() in key_value_lower for keyword in blacklist_keywords
            )
            if not contains_blacklisted:
                filtered_keys.append(key)
            else:
                self.logger.verbose(
                    "DEBUG",
                    f"Excluded key '{key.value}' (contains blacklisted keyword)",
                )
        return filtered_keys


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
