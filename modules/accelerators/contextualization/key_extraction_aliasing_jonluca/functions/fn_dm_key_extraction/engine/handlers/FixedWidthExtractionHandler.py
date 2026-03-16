import re

from ...utils.DataStructures import *
from ...config import ExtractionRuleConfig
from .ExtractionMethodHandler import ExtractionMethodHandler


class FixedWidthExtractionHandler(ExtractionMethodHandler):
    """Handles fixed width parsing extraction."""

    def extract(
        self, text: str, rule: ExtractionRuleConfig, context: Dict[str, Any] = None
    ) -> List[ExtractedKey]:
        """Extract keys using fixed width parsing."""
        if not text:
            return []

        self.logger.verbose(
            "DEBUG", f"Fixed width extraction for rule '{rule.name}' on text '{text}'"
        )

        extracted_keys = []
        config = rule.config

        # Get field definitions (legacy format) or positions (new format)
        field_definitions = config.get("field_definitions", [])
        positions = config.get("positions", [])

        self.logger.verbose("DEBUG", f"Field definitions: {field_definitions}")
        self.logger.verbose("DEBUG", f"Positions: {positions}")

        if not field_definitions and not positions:
            self.logger.verbose(
                "WARNING",
                f"No field definitions or positions found for fixed width rule '{rule.name}'",
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
            line_keys = self._extract_from_line(line, field_definitions, rule)
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
        self, line: str, field_definitions: List[Dict], rule: ExtractionRuleConfig
    ) -> List[ExtractedKey]:
        """Extract keys from a single line using field definitions."""
        extracted_keys = []

        # First, try to match the pattern if it exists
        pattern = rule.pattern
        # Only validate pattern if it exists and field_definitions are provided
        if pattern:
            # Convert pattern to regex for validation
            regex_pattern = self._convert_fixed_width_pattern_to_regex(pattern)
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
                if not self._validate_field_type(field_value, field_type):
                    continue

            if field_value:
                confidence = self._calculate_fixed_width_confidence(
                    field_value, field_type, required
                )

                extracted_key = ExtractedKey(
                    value=field_value,
                    extraction_type=rule.extraction_type,
                    source_field=field_name,
                    confidence=confidence,
                    method=rule.method,
                    rule_id=rule.name,
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

    def _convert_fixed_width_pattern_to_regex(self, pattern: str) -> Optional[str]:
        """Convert fixed width pattern to regex for validation."""
        try:
            # Handle patterns like "P{position:0,length:1}\d{position:1,length:3}[A-Z]{position:4,length:1}"
            # Convert to regex like "P\d{3}[A-Z]?"

            self.logger.verbose("DEBUG", f"Converting fixed width pattern: {pattern}")

            # Extract position and length information
            position_matches = re.findall(r"\{position:(\d+),length:(\d+)\}", pattern)

            # Build regex pattern by replacing position specifications with proper regex
            regex_pattern = pattern

            # Replace each position specification with appropriate regex
            for i, (pos, length) in enumerate(position_matches):
                pos_int = int(pos)
                length_int = int(length)

                # Find the character class before this position spec
                before_pos = pattern[
                    : pattern.find(f"{{position:{pos},length:{length}}}")
                ]
                char_class = before_pos[-1] if before_pos else ""

                if char_class == "d":
                    # Replace \d{position:x,length:y} with \d{y}
                    regex_pattern = regex_pattern.replace(
                        f"\\d{{position:{pos},length:{length}}}", f"\\d{{{length}}}"
                    )
                elif char_class in "[]":
                    # Handle character classes like [A-Z]{position:x,length:y}
                    char_class_start = before_pos.rfind("[")
                    if char_class_start != -1:
                        char_class_content = before_pos[char_class_start:]
                        regex_pattern = regex_pattern.replace(
                            f"{char_class_content}{{position:{pos},length:{length}}}",
                            f"{char_class_content}{{{length}}}",
                        )
                else:
                    # Handle literal characters like P{position:x,length:y}
                    # For literal characters, just use the character repeated if length > 1
                    if length_int == 1:
                        regex_pattern = regex_pattern.replace(
                            f"{char_class}{{position:{pos},length:{length}}}",
                            char_class,
                        )
                    else:
                        regex_pattern = regex_pattern.replace(
                            f"{char_class}{{position:{pos},length:{length}}}",
                            f"{char_class}{{{length}}}",
                        )

            # Clean up any remaining position specifications
            regex_pattern = re.sub(r"\{position:\d+,length:\d+\}", "", regex_pattern)

            # Convert escaped characters
            regex_pattern = regex_pattern.replace("\\d", r"\d")

            self.logger.verbose("DEBUG", f"Converted to regex pattern: {regex_pattern}")

            return regex_pattern
        except Exception as e:
            self.logger.verbose(
                "WARNING", f"Failed to convert pattern '{pattern}' to regex: {e}"
            )
            return None

    def _validate_field_type(self, value: str, field_type: str) -> bool:
        """Validate field value against expected type."""
        if field_type == "equipment_type":
            return value.isalpha() and len(value) <= 3
        elif field_type == "number":
            return value.isdigit()
        elif field_type == "suffix":
            return value.isalnum() and len(value) <= 2
        elif field_type == "instrument_type":
            return value.isalpha() and len(value) <= 4
        return True

    def _calculate_fixed_width_confidence(
        self, value: str, field_type: str, required: bool
    ) -> float:
        """Calculate confidence score for fixed width extraction."""
        base_confidence = 0.9  # Fixed width parsing has high base confidence

        # Adjust based on field type validation
        if self._validate_field_type(value, field_type):
            base_confidence += 0.05

        # Adjust based on required field
        if required:
            base_confidence += 0.05

        return min(base_confidence, 1.0)

    def _reconstruct_complete_tags(
        self, extracted_keys: List[ExtractedKey], rule: ExtractionRuleConfig
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
                reconstructed_key = ExtractedKey(
                    value=complete_tag,
                    extraction_type=rule.extraction_type,
                    source_field=sorted_keys[0].source_field,
                    confidence=min(
                        k.confidence for k in sorted_keys
                    ),  # Use minimum confidence
                    method=rule.method,
                    rule_id=rule.name,
                    metadata={
                        "reconstructed": True,
                        "component_keys": [k.value for k in sorted_keys],
                        "source_text": source_text,
                        "field_count": len(sorted_keys),
                    },
                )
                reconstructed_keys.append(reconstructed_key)

        return reconstructed_keys
