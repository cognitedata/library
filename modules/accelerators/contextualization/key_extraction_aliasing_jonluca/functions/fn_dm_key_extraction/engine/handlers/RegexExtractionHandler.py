import re

from ...utils.DataStructures import *
from ...config import ExtractionRuleConfig
from .ExtractionMethodHandler import ExtractionMethodHandler


class RegexExtractionHandler(ExtractionMethodHandler):
    """Handles regex-based key extraction."""

    def extract(
        self, text: str, rule: ExtractionRuleConfig, context: Dict[str, Any] = None
    ) -> List[ExtractedKey]:
        """Extract keys using regex patterns."""
        rgx_rule = rule.config

        if not text or not rgx_rule.pattern:
            return []

        extracted_keys = []

        try:
            # Compile regex pattern with appropriate flags
            pattern = re.compile(rgx_rule.pattern, rgx_rule.regex_options.to_regex_flags())

            # Find all matches
            matches = pattern.finditer(text)

            for m in matches:
                key_value = m.group(1)

                # Optional: named capture groups with reassembly   <-- we don't need this, token reassembly does this
                # capture_groups = (rgx_rule or {}).get("capture_groups")
                # reassemble_format = (rgx_rule or {}).get("reassemble_format")
                # if capture_groups and reassemble_format and m.groupdict():
                #     group_data = m.groupdict()
                #     try:
                #         key_value = reassemble_format.format(**group_data)
                #     except Exception:
                #         # Fall back to full match if formatting fails
                #         key_value = m.group(0)

                if key_value:
                    # Calculate confidence based on pattern specificity
                    confidence = self._calculate_confidence(key_value, text)

                    if confidence >= rule.min_confidence:
                        # Handle both dict and SourceField object for source_field
                        source_field = "unknown"
                        if rule.source_fields:
                            first_field = rule.source_fields[0]
                            if isinstance(first_field, dict):
                                source_field = first_field.get("field_name", "unknown")
                            elif isinstance(first_field, SourceFieldParameter):
                                source_field = first_field.field_name
                            else:
                                source_field = first_field.field_name

                        extracted_key = ExtractedKey(
                            value=key_value,
                            extraction_type=rule.extraction_type,
                            source_field=source_field,
                            confidence=confidence,
                            method=rule.method,
                            rule_id=rule.name,
                            metadata={
                                "pattern": rgx_rule.pattern,
                                "match_position": text.find(key_value),
                                "context": context or {},
                            },
                        )
                        extracted_keys.append(extracted_key)

        except re.error as e:
            self.logger.error(
                f"Invalid regex pattern '{rgx_rule.pattern}' in rule '{rule.name}': {e}"
            )

        return extracted_keys

    def _calculate_confidence(
        self, key_value: str, text: str
    ) -> float:
        """Calculate confidence score for extracted key."""
        base_confidence = 0.4

        # Adjust based on key characteristics
        if len(key_value) >= 3:
            base_confidence += 0.1

        # Check if key appears at start of field (higher confidence)
        if text.strip().startswith(key_value):
            base_confidence += 0.1

        # Check for word boundaries
        if re.search(r"\b" + re.escape(key_value) + r"\b", text):
            base_confidence += 0.05

        return min(base_confidence, 1.0)
