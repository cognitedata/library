import re
from typing import Any, Dict, Optional

from ...utils.DataStructures import *
from ...utils.confidence import compute_fixed_width_confidence
from ...utils.fixed_width_utils import (
    convert_fixed_width_pattern_to_regex,
    validate_field_type,
)
from ...utils.rule_utils import (
    common_extracted_key_attrs,
    get_config,
    get_rule_attr,
    get_rule_id,
)
from .ExtractionMethodHandler import ExtractionMethodHandler


class FixedWidthExtractionHandler(ExtractionMethodHandler):
    """Handles fixed width parsing extraction."""

    def extract(
        self, text: str, rule: ExtractionRule, context: Dict[str, Any] = None
    ) -> List[ExtractedKey]:
        """Extract keys using fixed width parsing."""
        if not text:
            return []

        self.logger.verbose(
            "DEBUG", f"Fixed width extraction for rule '{get_rule_id(rule)}' on text '{text}'"
        )

        extracted_keys = []
        config = get_config(rule)

        # Get field definitions (legacy format) or positions (new format)
        field_definitions = config.get("field_definitions", [])
        positions = config.get("positions", [])

        self.logger.verbose("DEBUG", f"Field definitions: {field_definitions}")
        self.logger.verbose("DEBUG", f"Positions: {positions}")

        if not field_definitions and not positions:
            self.logger.verbose(
                "WARNING",
                f"No field definitions or positions found for fixed width rule '{get_rule_id(rule)}'",
            )
            return []

        # Convert positions to field_definitions if using new format
        if positions and not field_definitions:
            field_definitions = self._convert_positions_to_field_definitions(
                positions, config
            )
            self.logger.verbose(
                "DEBUG",
                f"Converted positions to field definitions: {field_definitions}",
            )

        # Optional: decode encoded byte strings
        try:
            encoding = config.get("encoding")
            if encoding and isinstance(text, str) and text.startswith("b'"):
                import ast

                actual_bytes: bytes = ast.literal_eval(text)
                text = actual_bytes.decode(encoding)
        except Exception:
            pass

        # Split text into records/lines
        record_delimiter = config.get("record_delimiter")
        lines = text.split(record_delimiter) if record_delimiter else text.split("\n")

        # Apply line filtering
        line_pattern = config.get("line_pattern")
        skip_lines = config.get("skip_lines", 0)

        processed_lines = []
        for i, line in enumerate(lines):
            if i < skip_lines:
                continue

            if line_pattern:
                if not re.match(line_pattern, line):
                    continue

            if config.get("stop_on_empty") and not line.strip():
                break

            processed_lines.append(line)

        # Process each line
        for line in processed_lines:
            line_keys = self._extract_from_line(line, field_definitions, rule, context)
            extracted_keys.extend(line_keys)

        # Reconstruct complete tags from individual fields
        reconstructed_keys = self._reconstruct_complete_tags(extracted_keys, rule)

        return reconstructed_keys

    def _convert_positions_to_field_definitions(
        self, positions: List[Dict], config: Dict
    ) -> List[Dict]:
        """Convert position-based configuration to field definitions."""
        field_definitions = []

        for i, position in enumerate(positions):
            start = position.get("start", 0)
            end = position.get("end", start + 1)
            field_type = position.get("type", "unknown")
            optional = position.get("optional", False)

            field_def = {
                "name": f"field_{i}",
                "start_position": start,
                "end_position": end,
                "field_type": field_type,
                "required": not optional,
                "trim": True,
                "padding": config.get("padding", "none"),
            }

            field_definitions.append(field_def)

        return field_definitions

    def _extract_from_line(
        self,
        line: str,
        field_definitions: List[Dict],
        rule: ExtractionRule,
        context: Optional[Dict[str, Any]] = None,
    ) -> List[ExtractedKey]:
        """Extract keys from a single line using field definitions."""
        extracted_keys = []

        # First, try to match the pattern if it exists (top-level or under config)
        cfg_line = get_config(rule)
        pattern = cfg_line.get("pattern") or get_rule_attr(rule, "pattern")
        # Only validate pattern if it exists and field_definitions are provided
        if pattern:
            # Convert pattern to regex for validation
            regex_pattern = convert_fixed_width_pattern_to_regex(pattern)
            # If conversion fails or returns None, skip pattern validation
            if regex_pattern:
                match = re.match(regex_pattern, line)
                if not match:
                    return []  # Pattern doesn't match, skip this line
            else:
                # Pattern conversion failed - warn and continue without pattern validation
                self.logger.verbose(
                    "WARNING",
                    f"Failed to convert pattern '{pattern}' to regex, skipping pattern validation",
                )

        # Extract individual fields
        for field_def in field_definitions:
            start_pos = field_def.get("start_position", 0)
            end_pos = field_def.get("end_position", len(line))
            field_name = field_def.get("name", "unknown")
            field_type = field_def.get("field_type", "unknown")
            trim = field_def.get("trim", True)
            required = field_def.get("required", False)
            padding = field_def.get("padding", "none")

            # Extract field value
            if end_pos > len(line):
                if required:
                    continue  # Required field extends beyond line length
                end_pos = len(line)

            field_value = line[start_pos:end_pos]

            # Apply padding handling
            if padding == "zero" and field_value:
                field_value = field_value.lstrip("0") or "0"
            elif padding == "space" and field_value:
                field_value = field_value.strip()

            if trim:
                field_value = field_value.strip()

            # Skip if required field is empty
            if required and not field_value:
                continue

            # Validate field type if specified
            if field_value and field_type != "unknown":
                if not validate_field_type(field_value, field_type):
                    continue

            if field_value:
                confidence = compute_fixed_width_confidence(
                    field_value, field_type, required, validate_fn=validate_field_type
                )

                common = common_extracted_key_attrs(rule)
                extracted_key = ExtractedKey(
                    value=field_value,
                    extraction_type=common["extraction_type"],
                    source_field=field_name,
                    confidence=confidence,
                    method=common["method"],
                    rule_id=common["rule_id"],
                    metadata={
                        "start_position": start_pos,
                        "end_position": end_pos,
                        "field_definition": field_def,
                        "field_type": field_type,
                        "source_text": line,  # Add source text for reconstruction
                    },
                )
                extracted_keys.append(extracted_key)

        return extracted_keys

    def _reconstruct_complete_tags(
        self, extracted_keys: List[ExtractedKey], rule: ExtractionRule
    ) -> List[ExtractedKey]:
        """Reconstruct complete tags from individual field extractions."""
        if not extracted_keys:
            return []

        # Group keys by their source line (using metadata if available)
        grouped_keys = {}
        for key in extracted_keys:
            # Use the original text as grouping key
            source_text = key.metadata.get("source_text", "")
            if source_text not in grouped_keys:
                grouped_keys[source_text] = []
            grouped_keys[source_text].append(key)

        reconstructed_keys = []

        for source_text, keys in grouped_keys.items():
            if len(keys) < 2:  # Need at least 2 fields to reconstruct
                reconstructed_keys.extend(keys)
                continue

            # Sort keys by their position in the text
            sorted_keys = sorted(
                keys, key=lambda k: k.metadata.get("start_position", 0)
            )

            # Reconstruct the complete tag
            complete_tag = ""
            for key in sorted_keys:
                complete_tag += key.value

            # Create a new extracted key for the complete tag
            if complete_tag:
                common = common_extracted_key_attrs(rule)
                reconstructed_key = ExtractedKey(
                    value=complete_tag,
                    extraction_type=common["extraction_type"],
                    source_field=sorted_keys[0].source_field,
                    confidence=min(
                        k.confidence for k in sorted_keys
                    ),  # Use minimum confidence
                    method=common["method"],
                    rule_id=common["rule_id"],
                    metadata={
                        "reconstructed": True,
                        "component_keys": [k.value for k in sorted_keys],
                        "source_text": source_text,
                        "field_count": len(sorted_keys),
                    },
                )
                reconstructed_keys.append(reconstructed_key)

        return reconstructed_keys
