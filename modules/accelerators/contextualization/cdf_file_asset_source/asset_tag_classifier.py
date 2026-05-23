#!/usr/bin/env python3
"""
Asset Tag Classifier

This module provides functionality to classify asset tags based on pattern matching
against industry-standard tag patterns. It can process assets from JSON or YAML files
and classify them with classification metadata including resourceDescription, resourceType,
resourceSubType, and standard information.
"""

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml


class AssetTagClassifier:
    """Classifies asset tags based on pattern matching against configuration patterns."""

    def __init__(
        self,
        config_path: Union[str, Path],
        document_patterns_path: Optional[Union[str, Path]] = None,
    ):
        """
        Initialize the AssetTagClassifier with a configuration file.

        Args:
            config_path: Path to the YAML configuration file containing pattern definitions
            document_patterns_path: Optional path to document patterns YAML file.
                                    If provided, tags matching document patterns will be ignored.
        """
        self.config_path = Path(config_path)
        self.config = self._load_full_config()
        self.patterns = self._load_patterns()
        self.compiled_patterns = self._compile_patterns()

        # Load classification mappings from config (with ISA 5.1 defaults as fallback)
        self.classification_mappings = self._load_classification_mappings()

        # Load document patterns if provided (to classify documents)
        self.document_patterns_path = (
            Path(document_patterns_path) if document_patterns_path else None
        )
        self.compiled_document_patterns = self._load_and_compile_document_patterns()

    def _load_full_config(self) -> Dict[str, Any]:
        """Load full configuration from YAML file."""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")

        with open(self.config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        return config or {}

    def _load_patterns(self) -> Dict[str, Any]:
        """Load pattern configuration from YAML file."""
        # Extract patterns from config structure
        patterns = {}

        # Handle asset_tag_patterns structure
        if "asset_tag_patterns" in self.config:
            for category, pattern_list in self.config["asset_tag_patterns"].items():
                if isinstance(pattern_list, list):
                    patterns[category] = pattern_list

        return patterns

    def _load_classification_mappings(self) -> Dict[str, Any]:
        """
        Load tag classification mappings from config with ISA 5.1 defaults as fallback.

        Returns:
            Dictionary containing process_variables, instrument_functions, and equipment_qualifiers
        """
        # ISA 5.1 defaults
        default_mappings = {
            "process_variables": {
                "F": "Flow",
                "P": "Pressure",
                "T": "Temperature",
                "L": "Level",
                "A": "Analysis",
                "X": "Unclassified",
            },
            "instrument_functions": {
                "I": "Indicator",
                "C": "Controller",
                "T": "Transmitter",
                "V": "Valve",
                "E": "Element",
                "A": "Alarm",
                "S": "Switch",
                "Y": "Computing",
            },
            "equipment_qualifiers": {
                "pumps": {
                    "FWP": "Feed Water Pump",
                    "CWP": "Cooling Water Pump",
                    "BWP": "Boiler Water Pump",
                    "HWP": "Hot Water Pump",
                    "CHWP": "Chilled Water Pump",
                    "CP": "Centrifugal Pump",
                    "DP": "Dosing Pump",
                    "VP": "Vacuum Pump",
                },
                "compressors": {
                    "AC": "Air Compressor",
                    "RC": "Reciprocating Compressor",
                    "CC": "Centrifugal Compressor",
                    "SC": "Screw Compressor",
                    "PC": "Positive Displacement Compressor",
                },
                "heat_exchangers": {
                    "HE": "Heat Exchanger",
                    "CE": "Condenser",
                    "RE": "Reboiler",
                    "AE": "Air Cooler",
                    "SE": "Steam Heater",
                    "CO": "Cooler",
                },
            },
        }

        # Load from config if available, otherwise use defaults
        config_mappings = self.config.get("tag_classification_mappings", {})

        mappings = {
            "process_variables": config_mappings.get(
                "process_variables", default_mappings["process_variables"]
            ),
            "instrument_functions": config_mappings.get(
                "instrument_functions", default_mappings["instrument_functions"]
            ),
            "equipment_qualifiers": config_mappings.get(
                "equipment_qualifiers", default_mappings["equipment_qualifiers"]
            ),
        }

        # Merge equipment qualifiers (allow partial overrides)
        if "equipment_qualifiers" in config_mappings:
            for eq_type in ["pumps", "compressors", "heat_exchangers"]:
                if eq_type in config_mappings["equipment_qualifiers"]:
                    # Merge: config overrides defaults
                    mappings["equipment_qualifiers"][eq_type] = {
                        **default_mappings["equipment_qualifiers"].get(eq_type, {}),
                        **config_mappings["equipment_qualifiers"][eq_type],
                    }

        return mappings

    def _compile_patterns(self) -> List[Dict[str, Any]]:
        """
        Compile all patterns into a flat list with compiled regex for efficient matching.

        Returns:
            List of pattern dictionaries with compiled regex objects
        """
        compiled = []

        for category, pattern_list in self.patterns.items():
            if isinstance(pattern_list, list):
                for pattern in pattern_list:
                    if "pattern" in pattern:
                        try:
                            # Fix pattern escaping - YAML may have over-escaped patterns
                            # When YAML loads patterns, backslashes get doubled
                            # Pattern loaded: '\\\\bP[-_]?\\\\\\d{1,6}[A-Z]?\\\\b'
                            # We need: '\bP[-_]?\d{1,6}[A-Z]?\b'
                            pattern_str = pattern["pattern"]
                            # Use regex to replace any sequence of 2+ backslashes followed by
                            # a regex metacharacter (b, d, w, s, etc.) with a single backslash
                            # This handles all cases: \\b, \\\\b, \\\\\\b, etc. -> \b
                            import re as re_module

                            # Match 2 or more backslashes followed by a letter
                            # Replace with single backslash + letter
                            pattern_str = re_module.sub(
                                r"\\{2,}([bdwsDW])", r"\\\1", pattern_str
                            )
                            # Also handle numeric escape sequences like \d
                            pattern_str = re_module.sub(r"\\{2,}d", r"\\d", pattern_str)
                            # Compile the regex pattern
                            regex = re.compile(pattern_str)
                            pattern_copy = pattern.copy()
                            pattern_copy["compiled_regex"] = regex
                            pattern_copy["category"] = category
                            compiled.append(pattern_copy)
                        except re.error as e:
                            print(
                                f"Warning: Invalid regex pattern '{pattern.get('name', 'unknown')}': {e}"
                            )
                            continue

        # Sort by priority (lower priority number = higher priority)
        compiled.sort(key=lambda p: p.get("priority", 999))

        return compiled

    def _load_and_compile_document_patterns(self) -> List[Dict[str, Any]]:
        """
        Load and compile document patterns for classification.

        Returns:
            List of pattern dictionaries with compiled regex objects (similar to asset patterns)
        """
        if not self.document_patterns_path or not self.document_patterns_path.exists():
            return []

        try:
            with open(self.document_patterns_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)

            compiled = []

            # Load document_patterns from config
            if "document_patterns" in config:
                for pattern in config["document_patterns"]:
                    if "pattern" in pattern:
                        try:
                            pattern_str = pattern["pattern"]
                            # Fix pattern escaping (same as asset patterns)
                            import re as re_module

                            pattern_str = re_module.sub(
                                r"\\{2,}([bdwsDW])", r"\\\1", pattern_str
                            )
                            pattern_str = re_module.sub(r"\\{2,}d", r"\\d", pattern_str)
                            regex = re.compile(pattern_str)
                            pattern_copy = pattern.copy()
                            pattern_copy["compiled_regex"] = regex
                            pattern_copy["category"] = "document"
                            compiled.append(pattern_copy)
                        except re.error as e:
                            print(
                                f"Warning: Invalid document regex pattern '{pattern.get('name', 'unknown')}': {e}"
                            )
                            continue

            # Sort by priority (lower priority number = higher priority)
            compiled.sort(key=lambda p: p.get("priority", 999))

            return compiled
        except Exception as e:
            print(
                f"Warning: Could not load document patterns from {self.document_patterns_path}: {e}"
            )
            return []

    def _classify_document_tag(
        self, tag: str, validate: bool = True
    ) -> Optional[Dict[str, Any]]:
        """
        Classify a tag using document patterns.

        Args:
            tag: The tag string to classify
            validate: If True, apply validation rules after pattern match

        Returns:
            Dictionary with classification metadata, or None if no document pattern matches
        """
        if not self.compiled_document_patterns or not tag:
            return None

        # Try to match against all document patterns (sorted by priority)
        for pattern in self.compiled_document_patterns:
            regex = pattern["compiled_regex"]
            match = regex.search(tag)
            if match:
                # Apply validation rules if enabled
                if validate:
                    validation_result = self._validate_tag(tag, pattern, match)
                    if not validation_result["valid"]:
                        # Skip this pattern if validation fails
                        continue

                # Build classification result for document
                # Map document pattern fields to classification fields
                resource_type = pattern.get("type", "DOCUMENT")
                resource_subtype = pattern.get(
                    "subtype", pattern.get("document_type", "")
                )
                resource_type_pascal = self._to_camel_case(resource_type)
                classification = {
                    "resourceDescription": pattern.get(
                        "verbose_description", pattern.get("description", "")
                    ),
                    "resourceType": resource_type_pascal,
                    "resourceSubType": self._to_camel_case(resource_subtype),
                    "category": resource_type_pascal,  # Set category to pattern "type" (DOCUMENT -> Document)
                    "standard": pattern.get(
                        "standard", pattern.get("industry_standard", "")
                    ),
                    "standard_section": pattern.get("standard_section", ""),
                    "standard_subsection": pattern.get("standard_subsection", ""),
                    "matched_pattern": pattern.get("name", ""),
                    "pattern_category": "document",
                }
                return classification

        return None

    def classify_tag(self, tag: str, validate: bool = True) -> Optional[Dict[str, Any]]:
        """
        Classify a single tag by matching against all patterns.

        Args:
            tag: The tag string to classify
            validate: If True, apply validation rules after pattern match

        Returns:
            Dictionary with classification metadata, or None if no match found or validation fails
        """
        if not tag or not isinstance(tag, str):
            return None

        # Check document patterns first (they have higher priority)
        document_classification = self._classify_document_tag(tag, validate=validate)
        if document_classification:
            return document_classification

        # Try to match against all asset patterns (sorted by priority)
        for pattern in self.compiled_patterns:
            regex = pattern["compiled_regex"]
            match = regex.search(tag)
            if match:
                # Apply validation rules if enabled
                if validate:
                    validation_result = self._validate_tag(tag, pattern, match)
                    if not validation_result["valid"]:
                        # Skip this pattern if validation fails
                        continue

                # Build classification result with 4-level CFIHOS hierarchy support
                resource_type = self._extract_resource_type(pattern)
                resource_subtype = self._extract_resource_subtype(pattern, tag, match)
                resource_subsubtype = self._extract_resource_subsubtype(
                    pattern, tag, match
                )
                resource_variant = self._extract_resource_variant(pattern, tag, match)

                pattern_type = pattern.get(
                    "type", ""
                )  # Get "type" field (EQUIPMENT, INSTRUMENT, etc.)
                classification = {
                    "resourceDescription": pattern.get(
                        "verbose_description", pattern.get("description", "")
                    ),
                    "resourceType": resource_type,  # Level 1: Equipment Category
                    "resourceSubType": resource_subtype,  # Level 2: Equipment Type
                    "resourceSubSubType": resource_subsubtype,  # Level 3: Equipment Subtype (CFIHOS)
                    "resourceVariant": resource_variant,  # Level 4: Equipment Variant/Service (CFIHOS)
                    "category": self._to_camel_case(
                        pattern_type
                    ),  # Set category to pattern "type" (Pascal Case)
                    "standard": pattern.get(
                        "standard", pattern.get("industry_standard", "")
                    ),
                    "standard_section": pattern.get("standard_section", ""),
                    "standard_subsection": pattern.get("standard_subsection", ""),
                    "matched_pattern": pattern.get("name", ""),
                    "pattern_category": pattern.get("category", ""),
                }
                return classification

        return None

    def _to_camel_case(self, value: str) -> str:
        """
        Convert a string to Pascal Case with spaces (delimiters replaced with spaces).

        Examples:
            "PIPING_INSTRUMENTATION_DIAGRAM" -> "Piping Instrumentation Diagram"
            "HEAT_EXCHANGER" -> "Heat Exchanger"
            "CENTRIFUGAL_PUMP" -> "Centrifugal Pump"
            "DOCUMENT" -> "Document"

        Args:
            value: String to convert (can be UPPER_CASE, lower_case, or mixed)

        Returns:
            Pascal Case string with spaces
        """
        if not value:
            return ""

        # Split by common delimiters (underscore, hyphen, space)
        parts = re.split(r"[_\-\s]+", str(value))

        # Convert to Pascal Case: all words capitalized, joined with spaces
        pascal_case_parts = []
        for part in parts:
            if part:
                pascal_case_parts.append(part.capitalize())

        return " ".join(pascal_case_parts)

    def _validate_tag(
        self, tag: str, pattern: Dict[str, Any], match: re.Match
    ) -> Dict[str, Any]:
        """
        Validate a tag against the pattern's validation rules.

        Args:
            tag: The tag string to validate
            pattern: The pattern dictionary containing validation_rules
            match: The regex match object from pattern matching

        Returns:
            Dictionary with 'valid' (bool) and 'errors' (list of error messages)
        """
        validation_rules = pattern.get("validation_rules", [])
        if not validation_rules:
            return {"valid": True, "errors": []}

        errors = []
        match_groups = match.groups() if match else []

        for rule in validation_rules:
            rule_type = rule.get("type")
            if not rule_type:
                continue

            try:
                if rule_type == "length":
                    min_len = rule.get("min", 0)
                    max_len = rule.get("max", float("inf"))
                    if not (min_len <= len(tag) <= max_len):
                        errors.append(
                            rule.get(
                                "message",
                                f"Length must be between {min_len} and {max_len}",
                            )
                        )

                elif rule_type == "starts_with":
                    value = rule.get("value")
                    case_sensitive = rule.get("case_sensitive", True)
                    if value:
                        tag_start = tag[: len(value)]
                        if case_sensitive:
                            if tag_start != value:
                                errors.append(
                                    rule.get("message", f"Must start with '{value}'")
                                )
                        else:
                            if tag_start.upper() != value.upper():
                                errors.append(
                                    rule.get("message", f"Must start with '{value}'")
                                )

                elif rule_type == "ends_with":
                    allowed_values = rule.get("value", [])
                    if not isinstance(allowed_values, list):
                        allowed_values = [allowed_values]
                    case_sensitive = rule.get("case_sensitive", True)
                    matched = False
                    for value in allowed_values:
                        if value == "" and tag == "":
                            matched = True
                            break
                        if value and tag.endswith(value):
                            if (
                                case_sensitive
                                or tag[-len(value) :].upper() == value.upper()
                            ):
                                matched = True
                                break
                    if not matched:
                        errors.append(
                            rule.get(
                                "message", f"Must end with one of: {allowed_values}"
                            )
                        )

                elif rule_type == "allowed_characters":
                    pattern_str = rule.get("pattern")
                    if pattern_str:
                        if not re.match(pattern_str, tag):
                            errors.append(
                                rule.get("message", "Contains invalid characters")
                            )

                elif rule_type == "forbidden_characters":
                    forbidden = rule.get("characters", [])
                    for char in forbidden:
                        if char in tag:
                            errors.append(
                                rule.get("message", f"Cannot contain '{char}'")
                            )

                elif rule_type == "case":
                    requirement = rule.get("requirement", "").lower()
                    if requirement == "uppercase":
                        if tag != tag.upper():
                            errors.append(rule.get("message", "Must be uppercase"))
                    elif requirement == "lowercase":
                        if tag != tag.lower():
                            errors.append(rule.get("message", "Must be lowercase"))
                    elif requirement == "mixed":
                        if not tag[0].isupper() if tag else False:
                            errors.append(
                                rule.get("message", "Must start with uppercase letter")
                            )

                elif rule_type == "contains":
                    value = rule.get("value")
                    if value and value not in tag:
                        errors.append(rule.get("message", f"Must contain '{value}'"))

                elif rule_type == "not_contains":
                    value = rule.get("value")
                    if value and value in tag:
                        errors.append(rule.get("message", f"Cannot contain '{value}'"))

                elif rule_type == "contains_any":
                    values = rule.get("values", [])
                    if values and not any(v in tag for v in values):
                        errors.append(
                            rule.get("message", f"Must contain one of: {values}")
                        )

                elif rule_type == "numeric_range":
                    # Extract numeric portion from tag
                    extract_pattern = rule.get("extract_pattern", r"\d+")
                    numeric_match = re.search(extract_pattern, tag)
                    if numeric_match:
                        try:
                            num_value = int(numeric_match.group())
                            min_val = rule.get("min")
                            max_val = rule.get("max")
                            if min_val is not None and num_value < min_val:
                                errors.append(
                                    rule.get(
                                        "message", f"Numeric value must be >= {min_val}"
                                    )
                                )
                            if max_val is not None and num_value > max_val:
                                errors.append(
                                    rule.get(
                                        "message", f"Numeric value must be <= {max_val}"
                                    )
                                )
                        except ValueError:
                            errors.append(
                                rule.get("message", "Could not extract numeric value")
                            )
                    else:
                        errors.append(rule.get("message", "No numeric value found"))

                elif rule_type == "group_validation":
                    # Validate specific regex capture groups
                    groups_config = rule.get("groups", [])
                    for group_config in groups_config:
                        group_index = group_config.get("index", 0)
                        if 0 <= group_index < len(match_groups):
                            group_value = match_groups[group_index]
                            group_type = group_config.get("type")
                            if group_type == "prefix":
                                allowed = group_config.get("allowed_values", [])
                                if group_value not in allowed:
                                    errors.append(
                                        group_config.get(
                                            "message",
                                            f"Group {group_index} must be one of {allowed}",
                                        )
                                    )
                            elif group_type == "numeric":
                                try:
                                    num_value = int(group_value)
                                    min_val = group_config.get("min")
                                    max_val = group_config.get("max")
                                    if min_val is not None and num_value < min_val:
                                        errors.append(
                                            group_config.get(
                                                "message",
                                                f"Group {group_index} must be >= {min_val}",
                                            )
                                        )
                                    if max_val is not None and num_value > max_val:
                                        errors.append(
                                            group_config.get(
                                                "message",
                                                f"Group {group_index} must be <= {max_val}",
                                            )
                                        )
                                except (ValueError, TypeError):
                                    errors.append(
                                        group_config.get(
                                            "message",
                                            f"Group {group_index} must be numeric",
                                        )
                                    )

            except Exception as e:
                # Log validation rule error but don't fail the entire validation
                errors.append(f"Validation rule error ({rule_type}): {str(e)}")

        return {"valid": len(errors) == 0, "errors": errors}

    def _extract_resource_type(self, pattern: Dict[str, Any]) -> str:
        """Extract resourceType from pattern and convert to camelCase.

        CFIHOS Level 1: Tag Class Name (e.g., "Rotating Equipment", "Static Equipment")
        """
        value = ""
        if "tag_class_name" in pattern:
            value = pattern["tag_class_name"]
        elif "type" in pattern:
            value = pattern["type"]

        return self._to_camel_case(value) if value else ""

    def _extract_resource_subtype(
        self,
        pattern: Dict[str, Any],
        tag: Optional[str] = None,
        match: Optional[re.Match] = None,
    ) -> str:
        """
        Extract resourceSubType from pattern and convert to camelCase.
        Extracts ISA 5.1 process variables, instrument functions, and equipment qualifiers from tag prefix
        for more granular classification across all equipment and instrument types.

        Args:
            pattern: Pattern dictionary
            tag: Optional tag string to extract information from
            match: Optional regex match object

        Returns:
            Resource subtype string in Pascal Case
        """
        base_subtype = ""
        pattern_type = pattern.get("type", "").upper()

        if "equipment_class_name" in pattern:
            base_subtype = pattern["equipment_class_name"]
        elif "subtype" in pattern:
            base_subtype = pattern["subtype"]
        elif "instrument_type" in pattern:
            base_subtype = pattern["instrument_type"]

        if not tag:
            return self._to_camel_case(base_subtype) if base_subtype else ""

        base_subtype_upper = base_subtype.upper()

        # For INSTRUMENTS: Extract process variable and instrument function
        if pattern_type == "INSTRUMENT":
            process_var = self._extract_process_variable_from_tag(tag)
            instrument_func = self._extract_instrument_function_from_tag(tag)

            if process_var and instrument_func:
                # Create granular subtype: "Flow Controller", "Pressure Transmitter", etc.
                return f"{process_var} {instrument_func}"
            elif process_var:
                # Fallback to just process variable if function not found
                return f"{process_var} Instrument"

        # For VALVES: Extract process variable
        elif base_subtype_upper in ("CONTROL_VALVE", "SAFETY_VALVE"):
            process_var = self._extract_process_variable_from_tag(tag)
            if process_var:
                if base_subtype_upper == "CONTROL_VALVE":
                    return f"{process_var} Control Valve"
                elif base_subtype_upper == "SAFETY_VALVE":
                    return f"{process_var} Safety Valve"

        # For 4-level CFIHOS hierarchy:
        # Level 2 (resourceSubType) should be the base equipment type (e.g., "Pump", "Compressor")
        # Level 3 (resourceSubSubType) will be extracted separately for specific subtypes
        # Level 4 (resourceVariant) will be extracted separately for service variants

        # For PUMPS: Return base type "Pump" for Level 2
        # More specific types (Centrifugal, Positive Displacement) go to Level 3
        # Service variants (Feed Water, Cooling Water) go to Level 4
        if base_subtype_upper in (
            "PUMP",
            "CENTRIFUGAL_PUMP",
            "POSITIVE_DISPLACEMENT_PUMP",
        ):
            return "Pump"

        # For COMPRESSORS: Return base type "Compressor" for Level 2
        if base_subtype_upper in (
            "COMPRESSOR",
            "CENTRIFUGAL_COMPRESSOR",
            "RECIPROCATING_COMPRESSOR",
            "SCREW_COMPRESSOR",
            "AIR_COMPRESSOR",
        ):
            return "Compressor"

        # For HEAT EXCHANGERS: Return base type "Heat Exchanger" for Level 2
        if base_subtype_upper in (
            "HEAT_EXCHANGER",
            "SHELL_TUBE_HEAT_EXCHANGER",
            "PLATE_HEAT_EXCHANGER",
            "CONDENSER",
            "REBOILER",
            "AIR_COOLER",
        ):
            return "Heat Exchanger"

        # For VESSELS: Return base type "Vessel" for Level 2
        if base_subtype_upper in (
            "VESSEL",
            "PRESSURE_VESSEL",
            "STORAGE_VESSEL",
            "REACTOR_VESSEL",
            "SEPARATOR_VESSEL",
            "TANK",
        ):
            return "Vessel"

        # For VALVES: Return base type "Valve" for Level 2
        if base_subtype_upper in (
            "VALVE",
            "CONTROL_VALVE",
            "SAFETY_VALVE",
            "ISOLATION_VALVE",
            "CHECK_VALVE",
            "MANUAL_VALVE",
        ):
            return "Valve"

        # For TURBINES: Return base type "Turbine" for Level 2
        if base_subtype_upper in ("TURBINE", "STEAM_TURBINE", "GAS_TURBINE"):
            return "Turbine"

        # For FANS: Return base type "Fan" for Level 2
        if base_subtype_upper in ("FAN",):
            return "Fan"

        # For MOTORS: Return base type "Motor" for Level 2
        if base_subtype_upper in ("MOTOR",):
            return "Motor"

        # For COLUMNS: Return base type "Column" for Level 2
        if base_subtype_upper in ("COLUMN", "DISTILLATION_COLUMN", "ABSORPTION_COLUMN"):
            return "Column"

        # For REACTORS: Return base type "Reactor" for Level 2
        if base_subtype_upper in ("REACTOR",):
            return "Reactor"

        # For ELECTRICAL EQUIPMENT: Return base type for Level 2
        if base_subtype_upper in ("GENERATOR",):
            return "Generator"
        if base_subtype_upper in ("TRANSFORMER",):
            return "Transformer"
        if base_subtype_upper in ("SWITCHGEAR",):
            return "Switchgear"

        # Default: return the base subtype converted to Pascal Case
        return self._to_camel_case(base_subtype) if base_subtype else ""

    def _extract_resource_subsubtype(
        self,
        pattern: Dict[str, Any],
        tag: Optional[str] = None,
        match: Optional[re.Match] = None,
    ) -> str:
        """
        Extract resourceSubSubType (Level 3) from pattern and tag.
        This represents the equipment subtype in CFIHOS hierarchy.

        Examples:
        - For pumps: "Centrifugal Pump", "Positive Displacement Pump"
        - For compressors: "Centrifugal Compressor", "Reciprocating Compressor"
        - For vessels: "Pressure Vessel", "Storage Vessel"

        Args:
            pattern: Pattern dictionary
            tag: Optional tag string to extract information from
            match: Optional regex match object

        Returns:
            Resource sub-subtype string in Pascal Case
        """
        # Check if pattern explicitly defines subsubtype (CFIHOS Level 3: Equipment Subclass Name)
        if "equipment_subclass_name" in pattern:
            return self._to_camel_case(pattern["equipment_subclass_name"])
        if "subsubtype" in pattern:
            return self._to_camel_case(pattern["subsubtype"])

        # Extract from equipment_class_name if it's specific enough
        base_subtype = pattern.get("equipment_class_name", "") or pattern.get(
            "subtype", ""
        )
        if not base_subtype or not tag:
            return ""

        base_subtype_upper = base_subtype.upper()

        # For specific subtypes, return them as Level 3
        if base_subtype_upper in (
            "CENTRIFUGAL_PUMP",
            "POSITIVE_DISPLACEMENT_PUMP",
            "CENTRIFUGAL_COMPRESSOR",
            "RECIPROCATING_COMPRESSOR",
            "SCREW_COMPRESSOR",
            "AIR_COMPRESSOR",
            "PRESSURE_VESSEL",
            "STORAGE_VESSEL",
            "REACTOR_VESSEL",
            "SEPARATOR_VESSEL",
            "SHELL_TUBE_HEAT_EXCHANGER",
            "PLATE_HEAT_EXCHANGER",
            "CONDENSER",
            "REBOILER",
            "AIR_COOLER",
            "DISTILLATION_COLUMN",
            "ABSORPTION_COLUMN",
            "CONTROL_VALVE",
            "SAFETY_VALVE",
            "ISOLATION_VALVE",
            "CHECK_VALVE",
            "MANUAL_VALVE",
            "STEAM_TURBINE",
            "GAS_TURBINE",
            "GENERATOR",
            "TRANSFORMER",
            "SWITCHGEAR",
        ):
            return self._to_camel_case(base_subtype)

        # For generic types, try to extract more specific subtype from tag
        if base_subtype_upper == "PUMP":
            qualifier = self._extract_pump_qualifier_from_tag(tag, base_subtype_upper)
            if qualifier and qualifier != "Pump":
                # Extract the type part (e.g., "Centrifugal" from "Centrifugal Pump")
                parts = qualifier.split()
                if len(parts) > 1:
                    return parts[0]  # Return "Centrifugal", "Feed Water", etc.

        if base_subtype_upper == "COMPRESSOR":
            qualifier = self._extract_compressor_qualifier_from_tag(tag)
            if qualifier and qualifier != "Compressor":
                parts = qualifier.split()
                if len(parts) > 1:
                    return parts[0]  # Return "Centrifugal", "Reciprocating", etc.

        if base_subtype_upper == "HEAT_EXCHANGER":
            qualifier = self._extract_heat_exchanger_qualifier_from_tag(tag)
            if qualifier and qualifier != "Heat Exchanger":
                # For heat exchangers, the qualifier is often the full type
                return qualifier

        return ""

    def _extract_resource_variant(
        self,
        pattern: Dict[str, Any],
        tag: Optional[str] = None,
        match: Optional[re.Match] = None,
    ) -> str:
        """
        Extract resourceVariant (Level 4) from pattern and tag.
        This represents the service/application variant in CFIHOS hierarchy.

        Examples:
        - For pumps: "Feed Water Pump", "Cooling Water Pump", "Boiler Water Pump"
        - For compressors: "Instrument Air Compressor", "Gas Compressor"
        - For vessels: Service-specific variants

        Args:
            pattern: Pattern dictionary
            tag: Optional tag string to extract information from
            match: Optional regex match object

        Returns:
            Resource variant string in Pascal Case
        """
        # Check if pattern explicitly defines variant (CFIHOS Level 4: Equipment Variant Name)
        if "equipment_variant_name" in pattern:
            return self._to_camel_case(pattern["equipment_variant_name"])
        if "variant" in pattern:
            return self._to_camel_case(pattern["variant"])

        if not tag:
            return ""

        base_subtype = pattern.get("equipment_class_name", "") or pattern.get(
            "subtype", ""
        )
        base_subtype_upper = base_subtype.upper()

        # Extract service/application qualifiers for Level 4
        if base_subtype_upper in (
            "PUMP",
            "CENTRIFUGAL_PUMP",
            "POSITIVE_DISPLACEMENT_PUMP",
        ):
            qualifier = self._extract_pump_qualifier_from_tag(tag, base_subtype_upper)
            if qualifier:
                # For service-specific pumps (e.g., "Feed Water Pump"), return full qualifier
                # For type-specific pumps (e.g., "Centrifugal Pump"), return empty
                if any(
                    service in qualifier
                    for service in [
                        "Feed Water",
                        "Cooling Water",
                        "Boiler Water",
                        "Hot Water",
                        "Chilled Water",
                        "Dosing",
                        "Vacuum",
                        "Submersible",
                        "Metering",
                    ]
                ):
                    return qualifier
                # For type-only qualifiers, variant is empty (type is already in Level 3)
                return ""

        if base_subtype_upper in (
            "COMPRESSOR",
            "CENTRIFUGAL_COMPRESSOR",
            "RECIPROCATING_COMPRESSOR",
            "SCREW_COMPRESSOR",
        ):
            qualifier = self._extract_compressor_qualifier_from_tag(tag)
            if qualifier:
                # For service-specific compressors (e.g., "Instrument Air Compressor"), return full qualifier
                if any(service in qualifier for service in ["Instrument Air", "Gas"]):
                    return qualifier
                # For type-only qualifiers, variant is empty
                return ""

        # For other equipment types, check if there are service-specific qualifiers
        # This can be extended for other equipment types as needed

        return ""

    def _extract_process_variable_from_tag(self, tag: str) -> Optional[str]:
        """
        Extract process variable name from tag prefix using configured mappings.

        Args:
            tag: Tag string (e.g., "FCV-101", "PCV-201", "TCV-301", "LCV-401", "FIC-101")

        Returns:
            Process variable name (e.g., "Flow", "Pressure", "Temperature", "Level") or None
        """
        if not tag:
            return None

        # Get process variable mappings from config
        process_var_map = self.classification_mappings.get("process_variables", {})

        # Extract prefix letters, handling optional numeric unit/system prefix
        # Pattern: Optional numeric prefix (e.g., "24-") followed by 1-4 letters
        # Examples: FCV, PCV, 12-FIC-101, 24-P-1234, 5-PCV-201
        match = re.search(r"(?:^\d+[-_]?)?([A-Z]{1,4})", tag.upper())
        if match:
            prefix = match.group(1)
            # Check if first letter is a process variable code
            first_char = prefix[0]
            if first_char in process_var_map:
                return process_var_map[first_char]

        return None

    def _extract_instrument_function_from_tag(self, tag: str) -> Optional[str]:
        """
        Extract instrument function from tag prefix using configured mappings.

        Note: Multiple function letters can appear (e.g., IC = Indicator + Controller = Controller)
        The last function letter typically takes precedence.

        Args:
            tag: Tag string (e.g., "FIC-101", "PIT-201", "TCV-301", "LE-401")

        Returns:
            Instrument function name (e.g., "Controller", "Transmitter", "Indicator") or None
        """
        if not tag:
            return None

        # Get instrument function mappings from config
        function_map = self.classification_mappings.get("instrument_functions", {})

        # Extract prefix letters, handling optional numeric unit/system prefix
        # Pattern: Optional numeric prefix (e.g., "12-") followed by process variable (1 letter) + Function letters (1-3 letters)
        # Examples: FIC, 12-FIC-101 (F=Flow, I=Indicator, C=Controller -> Controller),
        #           PIT, 5-PIT-201 (P=Pressure, I=Indicator, T=Transmitter -> Transmitter),
        #           FCV, 8-FCV-301 (F=Flow, C=Controller, V=Valve -> Valve),
        #           FE, 24-FE-401 (F=Flow, E=Element -> Element)
        match = re.search(r"(?:^\d+[-_]?)?([A-Z]{2,4})", tag.upper())
        if match:
            prefix = match.group(1)
            if len(prefix) >= 2:
                # Check all letters after the process variable (first letter)
                # Priority: Last function letter typically takes precedence
                # But for common combinations, use specific logic
                function_chars = prefix[1:]  # All letters after process variable

                # Special handling for common combinations
                if "CV" in function_chars or "V" in function_chars:
                    return "Valve"  # Control valve
                elif "IC" in function_chars or function_chars.endswith("C"):
                    return "Controller"  # IC = Indicator + Controller = Controller
                elif "IT" in function_chars or function_chars.endswith("T"):
                    return "Transmitter"  # IT = Indicator + Transmitter = Transmitter
                elif function_chars.endswith("I"):
                    return "Indicator"
                elif function_chars.endswith("E"):
                    return "Element"
                elif function_chars.endswith("A"):
                    return "Alarm"
                elif function_chars.endswith("S"):
                    return "Switch"
                elif function_chars.endswith("Y"):
                    return "Computing"
                else:
                    # Check second letter as fallback
                    second_char = prefix[1]
                    if second_char in function_map:
                        return function_map[second_char]

        return None

    def _extract_pump_qualifier_from_tag(
        self, tag: str, base_subtype: str
    ) -> Optional[str]:
        """
        Extract pump service/type qualifier from tag prefix using configured mappings.

        Args:
            tag: Tag string (e.g., "FWP-101", "CWP-201", "CP-301")
            base_subtype: Base subtype from pattern

        Returns:
            Qualified pump type (e.g., "Feed Water Pump", "Centrifugal Pump") or None
        """
        if not tag:
            return None

        # Get pump qualifier mappings from config
        qualifier_map = self.classification_mappings.get(
            "equipment_qualifiers", {}
        ).get("pumps", {})

        # Extract prefix, handling optional numeric unit/system prefix
        # Pattern: Optional numeric prefix (e.g., "24-") followed by 1-4 letters
        # Examples: P-1234, 24-P-1234, FWP-101, 12-FWP-201, CP-301, 5-CP-401
        match = re.search(r"(?:^\d+[-_]?)?([A-Z]{1,4})", tag.upper())
        if match:
            prefix = match.group(1)
            if prefix in qualifier_map:
                return qualifier_map[prefix]
            # If it's just "P", return standard pump
            elif prefix == "P" and base_subtype == "PUMP":
                return "Pump"
            elif prefix == "CP" and base_subtype == "CENTRIFUGAL_PUMP":
                return "Centrifugal Pump"

        return None

    def _extract_compressor_qualifier_from_tag(self, tag: str) -> Optional[str]:
        """
        Extract compressor type qualifier from tag prefix using configured mappings.

        Args:
            tag: Tag string (e.g., "AC-101", "RC-201", "CC-301")

        Returns:
            Qualified compressor type (e.g., "Air Compressor", "Reciprocating Compressor") or None
        """
        if not tag:
            return None

        # Get compressor qualifier mappings from config
        qualifier_map = self.classification_mappings.get(
            "equipment_qualifiers", {}
        ).get("compressors", {})

        # Extract prefix, handling optional numeric unit/system prefix
        # Pattern: Optional numeric prefix (e.g., "100-") followed by 1-3 letters
        # Examples: C-101, 100-C-301, AC-201, 12-AC-401
        match = re.search(r"(?:^\d+[-_]?)?([A-Z]{1,3})", tag.upper())
        if match:
            prefix = match.group(1)
            if prefix in qualifier_map:
                return qualifier_map[prefix]
            # If it's just "C", return standard compressor
            elif prefix == "C":
                return "Compressor"

        return None

    def _extract_heat_exchanger_qualifier_from_tag(self, tag: str) -> Optional[str]:
        """
        Extract heat exchanger type qualifier from tag prefix using configured mappings.

        Args:
            tag: Tag string (e.g., "HE-101", "CE-201", "RE-301", "E-401")

        Returns:
            Qualified heat exchanger type (e.g., "Condenser", "Reboiler", "Heat Exchanger") or None
        """
        if not tag:
            return None

        # Get heat exchanger qualifier mappings from config
        qualifier_map = self.classification_mappings.get(
            "equipment_qualifiers", {}
        ).get("heat_exchangers", {})

        # Extract prefix, handling optional numeric unit/system prefix
        # Pattern: Optional numeric prefix (e.g., "8-") followed by 0-2 letters + E
        # Examples: E-101, 8-E-401, HE-201, 12-HE-301, CE-301, 5-CE-401
        # Note: For heat exchangers, we need to match the "E" at the end, so we look for patterns ending in E
        match = re.search(r"(?:^\d+[-_]?)?([A-Z]{0,2})E", tag.upper())
        if match:
            prefix = match.group(1) + "E"  # Include the E in the prefix
            if prefix in qualifier_map:
                return qualifier_map[prefix]
            # If it's just "E", return standard heat exchanger
            elif prefix == "E":
                return "Heat Exchanger"

        return None

    def classify_assets(
        self,
        assets: Union[List[Dict[str, Any]], Dict[str, Any]],
        tag_field: str = "externalId",
        skip_classified: bool = False,
    ) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Classify assets by matching tags against patterns.

        Args:
            assets: List of asset dictionaries or a single asset dictionary
            tag_field: Field name in asset dictionary that contains the tag to classify
            skip_classified: If True, skip assets that already have classification fields populated

        Returns:
            Classified assets with classification metadata added
        """
        # Handle single asset dictionary
        if isinstance(assets, dict) and not isinstance(assets, list):
            return self._classify_single_asset(assets, tag_field, skip_classified)

        # Handle list of assets
        if isinstance(assets, list):
            return [
                self._classify_single_asset(asset, tag_field, skip_classified)
                for asset in assets
            ]

        raise ValueError("Assets must be a dictionary or list of dictionaries")

    def _is_already_classified(self, asset: Dict[str, Any]) -> bool:
        """
        Check if an asset already has classification fields populated.

        Args:
            asset: Asset dictionary to check

        Returns:
            True if asset appears to be already classified, False otherwise
        """
        # Check if any of the main classification fields are populated
        classification_fields = [
            "resourceDescription",
            "resourceType",
            "resourceSubType",
            "standard",
        ]

        # Values that should be treated as "not classified"
        unclassified_values = ["undefined", "null", "none", ""]

        for field in classification_fields:
            value = asset.get(field)
            # Consider it classified if the field exists and has a non-empty value
            # that is not one of the unclassified placeholder values
            if value:
                value_str = str(value).strip().lower()
                if value_str and value_str not in unclassified_values:
                    return True

        return False

    def _classify_single_asset(
        self, asset: Dict[str, Any], tag_field: str, skip_classified: bool = False
    ) -> Dict[str, Any]:
        """
        Classify a single asset and add classification metadata.

        Args:
            asset: Single asset dictionary
            tag_field: Field name containing the tag
            skip_classified: If True, skip assets that are already classified

        Returns:
            Classified asset dictionary
        """
        # Create a copy to avoid modifying the original
        classified = asset.copy()

        # Check if already classified and should be skipped
        if skip_classified and self._is_already_classified(asset):
            # Return asset as-is without re-classifying
            return classified

        # Extract tag value
        tag_value = asset.get(tag_field, "")
        if not tag_value:
            # If tag field is empty, add empty classification fields only if they don't exist
            if "resourceDescription" not in classified:
                classified["resourceDescription"] = ""
            if "resourceType" not in classified:
                classified["resourceType"] = ""
            if "resourceSubType" not in classified:
                classified["resourceSubType"] = ""
            if "standard" not in classified:
                classified["standard"] = ""
            return classified

        # Classify the tag
        classification = self.classify_tag(str(tag_value))

        if classification:
            # Add classification metadata to asset
            classified["resourceDescription"] = classification["resourceDescription"]
            classified["resourceType"] = classification["resourceType"]
            classified["resourceSubType"] = classification["resourceSubType"]
            classified["category"] = classification.get(
                "category", ""
            )  # Set category from classification
            classified["standard"] = classification["standard"]
            # Optionally add additional metadata
            classified["standard_section"] = classification.get("standard_section", "")
            classified["standard_subsection"] = classification.get(
                "standard_subsection", ""
            )
            classified["matched_pattern"] = classification.get("matched_pattern", "")
        else:
            # No match found - only set empty values if fields don't already exist
            if "resourceDescription" not in classified:
                classified["resourceDescription"] = ""
            if "resourceType" not in classified:
                classified["resourceType"] = ""
            if "resourceSubType" not in classified:
                classified["resourceSubType"] = ""
            if "standard" not in classified:
                classified["standard"] = ""

        return classified

    def load_assets(
        self, assets_path: Union[str, Path]
    ) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Load assets from JSON or YAML file.

        Args:
            assets_path: Path to JSON or YAML file containing assets

        Returns:
            Loaded assets (list or dictionary)
        """
        assets_path = Path(assets_path)
        if not assets_path.exists():
            raise FileNotFoundError(f"Assets file not found: {assets_path}")

        suffix = assets_path.suffix.lower()

        if suffix == ".json":
            with open(assets_path, "r", encoding="utf-8") as f:
                return json.load(f)
        elif suffix in [".yaml", ".yml"]:
            with open(assets_path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f)
        else:
            raise ValueError(
                f"Unsupported file format: {suffix}. Use .json, .yaml, or .yml"
            )

    def save_assets(
        self,
        assets: Union[List[Dict[str, Any]], Dict[str, Any]],
        output_path: Union[str, Path],
        format: str = "yaml",
    ) -> None:
        """
        Save classified assets to a file.

        Args:
            assets: Classified assets to save
            output_path: Path to output file
            format: Output format ("yaml" or "json")
        """
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if format.lower() == "json":
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(assets, f, indent=2, ensure_ascii=False)
        elif format.lower() in ["yaml", "yml"]:
            with open(output_path, "w", encoding="utf-8") as f:
                yaml.dump(
                    assets,
                    f,
                    default_flow_style=False,
                    sort_keys=False,
                    allow_unicode=True,
                )
        else:
            raise ValueError(
                f"Unsupported output format: {format}. Use 'yaml' or 'json'"
            )


def classify_assets_from_file(
    assets_path: Union[str, Path],
    config_path: Union[str, Path],
    tag_field: str = "name",
    output_path: Optional[Union[str, Path]] = None,
    output_format: str = "yaml",
    skip_classified: bool = False,
) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Convenience function to classify assets from a file.

    Args:
        assets_path: Path to JSON or YAML file containing assets
        config_path: Path to YAML configuration file with pattern definitions
        tag_field: Field name in asset dictionary that contains the tag to classify
        output_path: Optional path to save classified assets
        output_format: Output format for saved file ("yaml" or "json")
        skip_classified: If True, skip assets that already have classification fields populated

    Returns:
        Classified assets with classification metadata
    """
    classifier = AssetTagClassifier(config_path)
    assets = classifier.load_assets(assets_path)
    classified = classifier.classify_assets(assets, tag_field, skip_classified)

    if output_path:
        classifier.save_assets(classified, output_path, output_format)

    return classified


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Classify asset tags based on pattern matching"
    )
    parser.add_argument(
        "assets",
        type=str,
        help="Path to JSON or YAML file containing assets",
    )
    parser.add_argument(
        "config",
        type=str,
        help="Path to YAML configuration file with pattern definitions",
    )
    parser.add_argument(
        "--tag-field",
        type=str,
        default="externalId",
        help="Field name containing the tag to classify (default: externalId)",
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Path to output file for classified assets",
    )
    parser.add_argument(
        "--format",
        type=str,
        choices=["yaml", "json"],
        default="yaml",
        help="Output format (default: yaml)",
    )
    parser.add_argument(
        "--skip-classified",
        action="store_true",
        help="Skip assets that already have classification fields populated",
    )

    args = parser.parse_args()

    classified = classify_assets_from_file(
        args.assets,
        args.config,
        args.tag_field,
        args.output,
        args.format,
        args.skip_classified,
    )

    print(
        f"✓ Classified {len(classified) if isinstance(classified, list) else 1} asset(s)"
    )
